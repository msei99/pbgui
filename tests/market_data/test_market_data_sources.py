"""Tests for market-data source index label handling."""

from __future__ import annotations

import market_data_sources as sources


def test_non_hyperliquid_other_code_is_reported_as_api(monkeypatch, tmp_path) -> None:
    """Legacy OKX archive source code 3 displays as official API data."""

    monkeypatch.setattr(sources, "get_source_index_path", lambda exchange, coin: tmp_path / str(exchange) / str(coin) / "sources.idx")

    sources.update_source_index_for_day(
        exchange="okx",
        coin="BTC_USDT:USDT",
        day="2024-01-01",
        minute_indices=[0, 1],
        code=sources.SOURCE_CODE_OTHER,
    )

    minutes = sources.get_source_minutes_for_range(exchange="okx", coin="BTC_USDT:USDT")
    counts = sources.get_daily_source_counts_for_range(exchange="okx", coin="BTC_USDT:USDT")

    assert minutes["20240101"]["00"] == {0: "api", 1: "api"}
    assert counts["20240101"]["api"] == 2
    assert counts["20240101"]["other_exchange"] == 0


def test_hyperliquid_other_code_stays_other_exchange(monkeypatch, tmp_path) -> None:
    """Hyperliquid fallback source code 3 keeps the other_exchange label."""

    monkeypatch.setattr(sources, "get_source_index_path", lambda exchange, coin: tmp_path / str(exchange) / str(coin) / "sources.idx")

    sources.update_source_index_for_day(
        exchange="hyperliquid",
        coin="BTC_USDC:USDC",
        day="2024-01-01",
        minute_indices=[0],
        code=sources.SOURCE_CODE_OTHER,
    )

    minutes = sources.get_source_minutes_for_range(exchange="hyperliquid", coin="BTC_USDC:USDC")
    counts = sources.get_daily_source_counts_for_range(exchange="hyperliquid", coin="BTC_USDC:USDC")

    assert minutes["20240101"]["00"] == {0: "other_exchange"}
    assert counts["20240101"]["api"] == 0
    assert counts["20240101"]["other_exchange"] == 1
