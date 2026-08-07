"""Regression tests for OKX market-data heatmap views."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from api import heatmap


def test_build_ohlcv_info_reports_coins_with_downloaded_history(tmp_path: Path, monkeypatch) -> None:
    """The Hyperliquid build picker should receive read-only downloaded-history metadata."""
    exchange_root = tmp_path / "hyperliquid"
    btc_dir = exchange_root / "1m" / "BTC_DIR"
    btc_dir.mkdir(parents=True)
    (btc_dir / "2026-01-01.npz").write_bytes(b"test")
    monkeypatch.setattr("market_data.load_market_data_config", lambda: {})
    monkeypatch.setattr(
        "market_data.get_effective_enabled_coins",
        lambda _exchange, cfg=None: (["BTC", "ETH"], [], False),
    )
    monkeypatch.setattr("market_data.get_exchange_raw_root_dir", lambda _exchange: exchange_root)
    monkeypatch.setattr(
        "market_data.normalize_market_data_coin_dir",
        lambda _exchange, coin: f"{coin}_DIR",
    )

    payload = heatmap.build_ohlcv_info(session=None)

    assert payload["eligible_coins"] == ["BTC", "ETH"]
    assert payload["coins_with_downloaded_history"] == ["BTC"]


def test_build_ohlcv_info_excludes_tradfi_without_downloadable_mapping(tmp_path: Path, monkeypatch) -> None:
    """XYZ build options should match the canonical Tiingo downloader eligibility rules."""
    monkeypatch.setattr("market_data.load_market_data_config", lambda: {})
    monkeypatch.setattr(
        "market_data.get_effective_enabled_coins",
        lambda _exchange, cfg=None: (["BTC", "xyz:AAPL", "xyz:EUR", "xyz:MSFT", "xyz:NONE"], [], False),
    )
    monkeypatch.setattr("market_data.get_exchange_raw_root_dir", lambda _exchange: tmp_path)
    monkeypatch.setattr("market_data.normalize_market_data_coin_dir", lambda _exchange, coin: coin)
    monkeypatch.setattr(
        "market_data_tradfi.load_tradfi_map",
        lambda: [
            {"xyz_coin": "AAPL", "status": "ok", "tiingo_ticker": "AAPL"},
            {"xyz_coin": "EUR", "status": "alias", "tiingo_fx_ticker": "eurusd"},
            {"xyz_coin": "MSFT", "status": "pending", "tiingo_ticker": "MSFT"},
            {"xyz_coin": "NONE", "status": "no_provider"},
        ],
    )
    monkeypatch.setattr("market_data_sources.source_index_contains_code", lambda **_kwargs: True)

    payload = heatmap.build_ohlcv_info(session=None)

    assert payload["eligible_coins"] == ["BTC", "xyz:AAPL", "xyz:EUR"]


def test_okx_1m_overview_uses_source_index(monkeypatch) -> None:
    """OKX 1m overview should render from source-index day counts."""

    calls: list[dict[str, Any]] = []
    ini_calls: list[tuple[str, str]] = []

    def fake_load_ini(section: str, key: str) -> None:
        """Return no configured interval while recording the OKX lookup."""

        ini_calls.append((section, key))
        return None

    def fake_counts(**kwargs: Any) -> dict[str, dict[str, int]]:
        """Return a tiny OKX source-index coverage sample."""

        calls.append(dict(kwargs))
        return {
            "20260601": {"api": 1440},
            "20260602": {"api": 720, "other_exchange": 0},
        }

    monkeypatch.setattr(
        "market_data_sources.get_daily_source_counts_for_range",
        fake_counts,
    )
    monkeypatch.setattr("pbgui_purefunc.load_ini", fake_load_ini)

    payload = heatmap.get_heatmap_overview(exchange="okx", dataset="1m", coin="BTC")

    assert payload["error"] is None
    assert payload["figure"]
    assert "api" in payload["legend_html"]
    assert calls == [
        {
            "exchange": "okx",
            "coin": "BTC",
            "start_day": None,
            "end_day": None,
            "lag_minutes": 60,
            "cutoff_ts_ms": None,
        }
    ]
    assert ini_calls == [("okx_data", "latest_1m_interval_seconds")]


def test_okx_missing_lag_uses_custom_interval(monkeypatch) -> None:
    """OKX missing-data lag should honor its configured one-minute interval."""

    calls: list[tuple[str, str]] = []

    def fake_load_ini(section: str, key: str) -> str:
        """Return a custom OKX interval while recording the lookup."""

        calls.append((section, key))
        return "900"

    monkeypatch.setattr("pbgui_purefunc.load_ini", fake_load_ini)

    assert heatmap._get_missing_lag_minutes("okx") == 15
    assert calls == [("okx_data", "latest_1m_interval_seconds")]
