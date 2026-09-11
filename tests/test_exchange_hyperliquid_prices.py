"""Hyperliquid ticker normalization without network or runtime data access."""

from unittest.mock import Mock

import pytest

from Exchange import Exchange


@pytest.fixture
def exchange():
    """Provide a Hyperliquid client with in-memory mids and metadata."""
    result = Exchange("hyperliquid")
    result.instance = Mock(markets={"loaded": {}})
    result.instance.fetch.return_value = {}
    result.instance.market.return_value = {"info": {}}
    return result


@pytest.mark.parametrize("symbol,key", [
    ("BTC/USDC:USDC", "BTC"), ("KPEPE/USDC:USDC", "kPEPE"),
    ("XYZ-TSLA/USDC:USDC", "xyz:TSLA"), ("FLX-XMR/USDC:USDC", "XMR"),
])
@pytest.mark.parametrize("raw", ["64123.5", 64123.5, 0])
def test_all_mids_prices_are_numeric(exchange, symbol, key, raw):
    """Raw strings and existing numbers support float position-size arithmetic."""
    exchange.instance.fetch.return_value = {key: raw}
    ticker = exchange.fetch_prices([symbol], "swap")[symbol]
    assert isinstance(ticker["last"], float)
    assert 1.5 * ticker["last"] == 1.5 * float(raw)
    assert isinstance(ticker["timestamp"], int)
    exchange.instance.market.assert_not_called()


@pytest.mark.parametrize("field", ["markPx", "midPx", "oraclePx"])
@pytest.mark.parametrize("raw_mid", [None, "invalid", {}])
def test_missing_or_invalid_mids_use_numeric_metadata(exchange, field, raw_mid):
    """Missing and malformed allMids values still permit the metadata fallback."""
    symbol = "XYZ-TSLA/USDC:USDC"
    exchange.instance.fetch.return_value = {"xyz:TSLA": raw_mid}
    exchange.instance.market.return_value = {"info": {field: "123.5"}}
    ticker = exchange.fetch_prices([symbol], "swap")[symbol]
    assert isinstance(ticker["last"], float)
    assert 1.5 * ticker["last"] == 185.25


@pytest.mark.parametrize("raw", [None, "invalid", {"invalid": True}])
def test_invalid_metadata_does_not_discard_other_tickers(exchange, raw):
    """Unusable symbols are omitted while valid symbols remain available."""
    exchange.instance.fetch.return_value = {"BTC": "42.5"}
    exchange.instance.market.return_value = {"info": {"markPx": raw}}
    prices = exchange.fetch_prices(["BAD/USDC:USDC", "BTC/USDC:USDC"], "swap")
    assert list(prices) == ["BTC/USDC:USDC"]
    assert prices["BTC/USDC:USDC"]["last"] == 42.5


def test_other_exchanges_keep_ccxt_ticker_response():
    """The Hyperliquid conversion leaves ordinary CCXT ticker handling intact."""
    exchange = Exchange("binance")
    exchange.instance = Mock()
    expected = {"BTC/USDT:USDT": {"last": 42.5}}
    exchange.instance.fetch_tickers.return_value = expected
    assert exchange.fetch_prices(list(expected), "swap") is expected
    exchange.instance.fetch_tickers.assert_called_once_with(symbols=list(expected))
