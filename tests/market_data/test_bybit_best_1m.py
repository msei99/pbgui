"""Regression tests for safe Bybit latest-1m refreshes."""

from __future__ import annotations

import sys
from datetime import date, datetime, timezone
from types import SimpleNamespace

import bybit_best_1m as bybit


class _FixedDateTime(datetime):
    """Freeze the latest refresh window for deterministic pagination tests."""

    @classmethod
    def now(cls, tz=None):
        value = cls(2026, 1, 3, 0, 30, tzinfo=timezone.utc)
        return value if tz is not None else value.replace(tzinfo=None)


def _row(ts_ms: int) -> list[float]:
    """Return one CCXT-shaped OHLCV row."""
    return [ts_ms, 1.0, 2.0, 0.5, 1.5, 3.0]


def _install_fake_ccxt(monkeypatch, fetch_ohlcv) -> None:
    """Install a minimal CCXT Bybit client around the supplied fetch method."""
    exchange = SimpleNamespace(fetch_ohlcv=fetch_ohlcv)
    monkeypatch.setitem(sys.modules, "ccxt", SimpleNamespace(bybit=lambda _config: exchange))


def _prepare_latest_test(monkeypatch, tmp_path) -> list[tuple[str, int, bool]]:
    """Patch storage and logging while recording day writes."""
    writes: list[tuple[str, int, bool]] = []
    monkeypatch.setattr(bybit, "datetime", _FixedDateTime)
    monkeypatch.setattr(bybit, "append_exchange_download_log", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(bybit, "_list_existing_days", lambda _coin: [date(2025, 12, 31)])
    monkeypatch.setattr(bybit, "_bybit_day_path", lambda _coin, day: tmp_path / f"{day}.npz")
    monkeypatch.setattr(
        bybit,
        "_write_candles_for_day",
        lambda _coin, day, candles, overwrite=False: writes.append(
            (day, len(candles), overwrite)
        )
        or len(candles),
    )
    return writes


def test_latest_refresh_discards_pages_after_fetch_error(monkeypatch, tmp_path) -> None:
    """A later CCXT page failure must not overwrite days with earlier pages."""
    writes = _prepare_latest_test(monkeypatch, tmp_path)
    start_ms = int(datetime(2026, 1, 2, tzinfo=timezone.utc).timestamp() * 1000)
    calls = 0

    def fetch_ohlcv(_symbol, timeframe, since, limit):
        nonlocal calls
        calls += 1
        if calls == 1:
            return [_row(start_ms + idx * 60_000) for idx in range(limit)]
        raise TimeoutError("temporary Bybit timeout")

    _install_fake_ccxt(monkeypatch, fetch_ohlcv)
    result = bybit.update_latest_bybit_1m_for_coin(coin="BTC", lookback_days=1)

    assert result["result"] == "error"
    assert result["minutes_written"] == 0
    assert writes == []


def test_latest_refresh_rejects_incomplete_closed_day(monkeypatch, tmp_path) -> None:
    """An empty follow-up page must not make a 1000-minute closed day valid."""
    writes = _prepare_latest_test(monkeypatch, tmp_path)
    start_ms = int(datetime(2026, 1, 2, tzinfo=timezone.utc).timestamp() * 1000)
    calls = 0

    def fetch_ohlcv(_symbol, timeframe, since, limit):
        nonlocal calls
        calls += 1
        if calls == 1:
            return [_row(start_ms + idx * 60_000) for idx in range(limit)]
        return []

    _install_fake_ccxt(monkeypatch, fetch_ohlcv)
    result = bybit.update_latest_bybit_1m_for_coin(coin="BTC", lookback_days=1)

    assert result["result"] == "error"
    assert "ends at minute 999" in result["error"]
    assert result["minutes_written"] == 0
    assert writes == []


def test_latest_refresh_verifies_closed_day_before_writing(monkeypatch, tmp_path) -> None:
    """A complete prior day and partial current day are written after validation."""
    writes = _prepare_latest_test(monkeypatch, tmp_path)
    last_available_ms = int(datetime(2026, 1, 3, 0, 30, tzinfo=timezone.utc).timestamp() * 1000)

    def fetch_ohlcv(_symbol, timeframe, since, limit):
        if since > last_available_ms:
            return []
        count = min(limit, ((last_available_ms - since) // 60_000) + 1)
        return [_row(since + idx * 60_000) for idx in range(count)]

    _install_fake_ccxt(monkeypatch, fetch_ohlcv)
    result = bybit.update_latest_bybit_1m_for_coin(coin="BTC", lookback_days=1)

    assert result["result"] == "ok"
    assert result["closed_days_checked"] == 1
    assert writes == [
        ("2026-01-02", 1440, True),
        ("2026-01-03", 31, True),
    ]


def test_overwrite_replaces_source_index(monkeypatch, tmp_path) -> None:
    """An NPZ overwrite must replace rather than merge source coverage."""
    replace_calls = []
    update_calls = []
    monkeypatch.setattr(bybit, "_bybit_day_path", lambda _coin, _day: tmp_path / "day.npz")
    monkeypatch.setattr(bybit, "_write_day_npz", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        bybit, "replace_source_index_for_day", lambda **kwargs: replace_calls.append(kwargs)
    )
    monkeypatch.setattr(
        bybit, "update_source_index_for_day", lambda **kwargs: update_calls.append(kwargs)
    )

    candle = {"t": 1, "o": 1.0, "h": 1.0, "l": 1.0, "c": 1.0, "v": 1.0}
    bybit._write_candles_for_day("BTC", "2026-01-02", {0: candle}, overwrite=True)

    assert len(replace_calls) == 1
    assert replace_calls[0]["minute_indices"] == [0]
    assert update_calls == []
