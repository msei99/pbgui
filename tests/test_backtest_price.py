"""Regression tests for backtest PBGui MarketData price overlays."""

from __future__ import annotations

import sys

import numpy as np
import pandas as pd

from api import backtest_price
from api import market_data as api_market_data
import market_data
import PBCoinData


def test_price_payload_resolves_normalized_coin_to_storage_directory(tmp_path, monkeypatch) -> None:
    """A result coin such as ADA must load its quoted PBGui OHLCV directory."""
    exchange_root = tmp_path / "bybit"
    (exchange_root / "1m" / "ADA_USDT:USDT").mkdir(parents=True)
    monkeypatch.setattr(market_data, "get_exchange_raw_root_dir", lambda _exchange: exchange_root)
    monkeypatch.setattr(PBCoinData, "get_symbol_for_coin", lambda _coin, _exchange: "ADAUSDT")
    captured: dict[str, str] = {}

    def fake_load(**kwargs):
        captured.update({key: str(value) for key, value in kwargs.items()})
        return pd.DataFrame(
            {
                "ts": np.array([1784678400000, 1784678460000, 1784678520000], dtype=np.int64),
                "c": np.array([0.61, 0.62, 0.63], dtype=np.float64),
            }
        )

    monkeypatch.setattr(api_market_data, "_load_ohlcv_from_npz_range", fake_load)

    payload = backtest_price.build_market_price_payload(
        {"backtest": {"exchanges": ["bybit"], "start_date": "2026-07-22", "end_date": "2026-07-22"}},
        exchange="bybit",
        coin="ADA",
        max_points=2,
    )

    assert captured["coin"] == "ADA_USDT:USDT"
    assert payload["available"] is True
    assert payload["original_points"] == 3
    assert payload["close"] == [0.61, 0.63]


def test_storage_coin_resolution_uses_exchange_symbol_mapping(tmp_path, monkeypatch) -> None:
    """Multiplier symbols must resolve to the mapped PBGui directory instead of a guessed base name."""
    exchange_root = tmp_path / "binanceusdm"
    (exchange_root / "1m" / "1000PEPE_USDT:USDT").mkdir(parents=True)
    monkeypatch.setattr(market_data, "get_exchange_raw_root_dir", lambda _exchange: exchange_root)
    monkeypatch.setattr(sys.modules["PBCoinData"], "get_symbol_for_coin", lambda _coin, _exchange: "1000PEPEUSDT")

    assert backtest_price._storage_coin_dir("binanceusdm", "PEPE") == "1000PEPE_USDT:USDT"
