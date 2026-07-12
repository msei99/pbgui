"""Tests for market-data source index label handling."""

from __future__ import annotations

import json

import market_data_sources as sources
import market_data_tradfi as tradfi


class _JsonResponse:
    """Minimal urllib response context manager for Tiingo tests."""

    def __init__(self, payload) -> None:
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        return None

    def read(self) -> bytes:
        return json.dumps(self.payload).encode("utf-8")


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


def test_tiingo_refresh_preserves_valid_cache_when_requests_fail(monkeypatch, tmp_path) -> None:
    """Transient Tiingo failures must not replace valid mapped quotes with an empty cache."""
    cache_path = tmp_path / "tradfi_quote_cache.json"
    cache_path.write_text(
        json.dumps(
            {
                "fetched_at": "2026-07-10T12:00:00+00:00",
                "quotes": {
                    "AAPL": {"price": 200.0, "source": "iex_all"},
                    "eurusd": {"price": 1.17, "source": "fx_top"},
                    "REMOVED": {"price": 5.0, "source": "iex_all"},
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(tradfi, "tradfi_quote_cache_path", lambda: cache_path)

    def fail_request(*args, **kwargs):
        raise OSError("Tiingo unavailable")

    monkeypatch.setattr("urllib.request.urlopen", fail_request)

    result = tradfi.refresh_tradfi_quote_cache(
        api_key="test-key",
        records=[{"tiingo_ticker": "AAPL"}, {"tiingo_fx_ticker": "EURUSD"}],
    )

    saved = json.loads(cache_path.read_text(encoding="utf-8"))
    assert saved["fetched_at"] == "2026-07-10T12:00:00+00:00"
    assert saved["quotes"] == {
        "AAPL": {"price": 200.0, "source": "iex_all"},
        "eurusd": {"price": 1.17, "source": "fx_top"},
    }
    assert result["quotes_saved"] == 2


def test_tiingo_refresh_merges_fresh_quotes_with_cached_failed_provider(monkeypatch, tmp_path) -> None:
    """A successful equity refresh must retain cached FX quotes when only FX fails."""
    cache_path = tmp_path / "tradfi_quote_cache.json"
    cache_path.write_text(
        json.dumps(
            {
                "fetched_at": "2026-07-10T12:00:00+00:00",
                "quotes": {
                    "AAPL": {"price": 190.0, "source": "iex_all"},
                    "eurusd": {"price": 1.17, "source": "fx_top"},
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(tradfi, "tradfi_quote_cache_path", lambda: cache_path)

    def partial_response(request, timeout):
        if request.full_url.startswith("https://api.tiingo.com/iex?"):
            return _JsonResponse([{"ticker": "AAPL", "tngoLast": 205.0, "timestamp": "fresh"}])
        raise OSError("Tiingo FX unavailable")

    monkeypatch.setattr("urllib.request.urlopen", partial_response)

    result = tradfi.refresh_tradfi_quote_cache(
        api_key="test-key",
        records=[{"tiingo_ticker": "AAPL"}, {"tiingo_fx_ticker": "EURUSD"}],
    )

    saved = json.loads(cache_path.read_text(encoding="utf-8"))
    assert saved["quotes"]["AAPL"]["price"] == 205.0
    assert saved["quotes"]["AAPL"]["quote_timestamp"] == "fresh"
    assert saved["quotes"]["eurusd"] == {"price": 1.17, "source": "fx_top"}
    assert saved["fetched_at"] != "2026-07-10T12:00:00+00:00"
    assert result["quotes_saved"] == 2
