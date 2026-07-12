"""Tests for TradFi stock split-adjustment functions."""
from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

import pytest

# Ensure project root is importable
_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from hyperliquid_best_1m import (
    apply_split_adjustment_to_bars,
    compute_cumulative_split_adjustment,
    _save_split_factors_to_cache,
    _load_split_factors_from_cache,
    _SPLIT_FACTOR_MEM,
    _SPLIT_FACTORS_PATH,
)


class TestCumulativeSplitAdjustment:
    """Tests for compute_cumulative_split_adjustment."""

    def test_no_splits(self):
        """No splits → factor is always 1.0."""
        assert compute_cumulative_split_adjustment([], date(2024, 1, 1)) == 1.0

    def test_single_split_before(self):
        """Date before a 4:1 split → factor = 4.0."""
        splits = [(date(2020, 8, 31), 4.0)]
        assert compute_cumulative_split_adjustment(splits, date(2020, 8, 30)) == 4.0

    def test_single_split_on_date(self):
        """Date on split day → no adjustment (split already happened)."""
        splits = [(date(2020, 8, 31), 4.0)]
        assert compute_cumulative_split_adjustment(splits, date(2020, 8, 31)) == 1.0

    def test_single_split_after(self):
        """Date after split → no adjustment."""
        splits = [(date(2020, 8, 31), 4.0)]
        assert compute_cumulative_split_adjustment(splits, date(2021, 1, 1)) == 1.0

    def test_multiple_splits(self):
        """Two splits: 7:1 (2014-06-09) and 4:1 (2020-08-31).
        Date before both → factor = 7 * 4 = 28.
        Date between → factor = 4.
        Date after both → factor = 1.
        """
        splits = [(date(2014, 6, 9), 7.0), (date(2020, 8, 31), 4.0)]
        assert compute_cumulative_split_adjustment(splits, date(2014, 6, 8)) == 28.0
        assert compute_cumulative_split_adjustment(splits, date(2014, 6, 9)) == 4.0
        assert compute_cumulative_split_adjustment(splits, date(2020, 8, 30)) == 4.0
        assert compute_cumulative_split_adjustment(splits, date(2020, 8, 31)) == 1.0
        assert compute_cumulative_split_adjustment(splits, date(2025, 1, 1)) == 1.0

    def test_reverse_split(self):
        """Reverse split with factor < 1 (e.g. 1:10 reverse = 0.1)."""
        splits = [(date(2023, 5, 1), 0.1)]
        # Before: factor = 0.1 → divide OHLC by 0.1 = multiply by 10
        assert compute_cumulative_split_adjustment(splits, date(2023, 4, 30)) == pytest.approx(0.1)
        assert compute_cumulative_split_adjustment(splits, date(2023, 5, 1)) == 1.0


class TestApplySplitAdjustmentToBars:
    """Tests for apply_split_adjustment_to_bars."""

    def test_empty_splits(self):
        """No splits → bars unchanged."""
        bars = [{"t": 1598832000000, "o": 500.0, "h": 510.0, "l": 490.0, "c": 505.0, "v": 1000.0}]
        result = apply_split_adjustment_to_bars(bars, [])
        assert result[0]["o"] == 500.0
        assert result[0]["v"] == 1000.0

    def test_empty_bars(self):
        """No bars → returns empty."""
        result = apply_split_adjustment_to_bars([], [(date(2020, 8, 31), 4.0)])
        assert result == []

    def test_aapl_4_to_1_split(self):
        """AAPL 4:1 split on 2020-08-31.
        Bar from 2020-08-28 (Friday before split):
        - Original: o=500, h=510, l=490, c=505, v=1000
        - Adjusted: o=125, h=127.5, l=122.5, c=126.25, v=4000
        """
        splits = [(date(2020, 8, 31), 4.0)]
        # 2020-08-28 00:00 UTC in ms
        bar_ts = int(date(2020, 8, 28).strftime("%s")) * 1000  # Use explicit
        bar_ts = 1598572800000  # 2020-08-28 00:00 UTC
        bars = [{"t": bar_ts, "o": 500.0, "h": 510.0, "l": 490.0, "c": 505.0, "v": 1000.0}]
        apply_split_adjustment_to_bars(bars, splits)
        assert bars[0]["o"] == pytest.approx(125.0)
        assert bars[0]["h"] == pytest.approx(127.5)
        assert bars[0]["l"] == pytest.approx(122.5)
        assert bars[0]["c"] == pytest.approx(126.25)
        assert bars[0]["v"] == pytest.approx(4000.0)

    def test_bar_after_split_unchanged(self):
        """Bar after split date → no adjustment."""
        splits = [(date(2020, 8, 31), 4.0)]
        bar_ts = 1598918400000  # 2020-09-01 00:00 UTC
        bars = [{"t": bar_ts, "o": 130.0, "h": 132.0, "l": 128.0, "c": 131.0, "v": 5000.0}]
        apply_split_adjustment_to_bars(bars, splits)
        assert bars[0]["o"] == pytest.approx(130.0)
        assert bars[0]["v"] == pytest.approx(5000.0)

    def test_multiple_bars_mixed_days(self):
        """Bars spanning split date: pre-split adjusted, post-split not."""
        splits = [(date(2020, 8, 31), 4.0)]
        bars = [
            {"t": 1598572800000, "o": 400.0, "h": 400.0, "l": 400.0, "c": 400.0, "v": 100.0},  # 2020-08-28
            {"t": 1598918400000, "o": 100.0, "h": 100.0, "l": 100.0, "c": 100.0, "v": 400.0},  # 2020-09-01
        ]
        apply_split_adjustment_to_bars(bars, splits)
        # Pre-split bar: 400/4=100, vol*4=400
        assert bars[0]["o"] == pytest.approx(100.0)
        assert bars[0]["v"] == pytest.approx(400.0)
        # Post-split bar: unchanged
        assert bars[1]["o"] == pytest.approx(100.0)
        assert bars[1]["v"] == pytest.approx(400.0)


class TestSplitFactorCache:
    """Tests for split factor disk cache."""

    def test_save_and_load(self, tmp_path, monkeypatch):
        """Round-trip: save → load returns same data."""
        monkeypatch.setattr("hyperliquid_best_1m._SPLIT_FACTORS_PATH", tmp_path / "split_factors.json")
        monkeypatch.setattr("hyperliquid_best_1m._SPLIT_FILE_LOADED", False)
        _SPLIT_FACTOR_MEM.clear()

        splits = [(date(2020, 8, 31), 4.0), (date(2014, 6, 9), 7.0)]
        _save_split_factors_to_cache("AAPL", splits)

        # Clear mem cache to force disk read
        _SPLIT_FACTOR_MEM.clear()
        monkeypatch.setattr("hyperliquid_best_1m._SPLIT_FILE_LOADED", False)
        loaded = _load_split_factors_from_cache("AAPL")

        assert loaded is not None
        assert len(loaded) == 2
        assert loaded[0] == (date(2020, 8, 31), 4.0)
        assert loaded[1] == (date(2014, 6, 9), 7.0)

    def test_mem_cache_hit(self, tmp_path, monkeypatch):
        """Second load comes from memory."""
        monkeypatch.setattr("hyperliquid_best_1m._SPLIT_FACTORS_PATH", tmp_path / "split_factors.json")
        monkeypatch.setattr("hyperliquid_best_1m._SPLIT_FILE_LOADED", False)
        _SPLIT_FACTOR_MEM.clear()

        splits = [(date(2023, 1, 1), 2.0)]
        _save_split_factors_to_cache("TEST", splits)
        loaded1 = _load_split_factors_from_cache("TEST")
        loaded2 = _load_split_factors_from_cache("TEST")
        assert loaded1 is loaded2  # Same object from mem

    def test_missing_cache_returns_none(self, tmp_path, monkeypatch):
        """Non-existent ticker → None."""
        monkeypatch.setattr("hyperliquid_best_1m._SPLIT_FACTORS_PATH", tmp_path / "split_factors.json")
        monkeypatch.setattr("hyperliquid_best_1m._SPLIT_FILE_LOADED", False)
        _SPLIT_FACTOR_MEM.clear()
        assert _load_split_factors_from_cache("NONEXIST") is None
