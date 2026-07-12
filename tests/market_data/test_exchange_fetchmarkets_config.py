"""Tests for Exchange fetchMarkets HIP-3 configuration."""

import Exchange as ExchangeModule


class DummySyncExchange:
    def __init__(self, *args, **kwargs):
        self.options = {}
        self.timeout = None
        self.enableRateLimit = None

    def checkRequiredCredentials(self):
        return True


class DummyCCXT:
    hyperliquid = DummySyncExchange
    binance = DummySyncExchange


def test_connect_sets_hyperliquid_hip3_fetchmarkets(monkeypatch):
    monkeypatch.setattr(ExchangeModule, "ccxt", DummyCCXT)

    exchange = ExchangeModule.Exchange("hyperliquid")
    exchange.connect()

    fetch_markets = exchange.instance.options.get("fetchMarkets")
    assert fetch_markets is not None, "Hyperliquid fetchMarkets options should be set"
    assert fetch_markets.get("types") == ["swap", "hip3"]
    assert fetch_markets.get("hip3", {}).get("dexes") == []


def test_connect_does_not_set_hip3_for_other_exchanges(monkeypatch):
    monkeypatch.setattr(ExchangeModule, "ccxt", DummyCCXT)

    exchange = ExchangeModule.Exchange("binance")
    exchange.connect()

    assert exchange.instance.options.get("fetchMarkets") is None
