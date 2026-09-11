"""Offline regressions for Hyperliquid DEX metadata alias resolution."""

from unittest.mock import Mock

import pytest

import hyperliquid_api as api


@pytest.fixture
def metadata(monkeypatch):
    """Isolate the metadata cache and prevent mapping or network reads."""
    monkeypatch.setattr(api, "_HYPERLIQUID_META_CACHE", {"ts": 0, "names": set(), "names_upper": {}})
    monkeypatch.setattr(api, "get_symbol_for_coin", Mock(return_value=""))
    request = Mock(side_effect=[{"universe": [{"name": "BTC"}, {"name": "kPEPE"}]},
                                {"universe": [{"name": "AAPL"}]}])
    monkeypatch.setattr(api, "hyperliquid_info_post", request)
    return request


@pytest.mark.parametrize("coin", ["XYZ-AAPL", "xyz-aapl", "xyz:AAPL", "XYZ:AAPL", "XYZ-AAPL/USDC:USDC"])
@pytest.mark.parametrize("meta_name", ["AAPL", "xyz:AAPL"])
def test_dex_alias_resolves_from_bare_or_qualified_metadata(metadata, coin, meta_name):
    """Every supported stock-perp spelling resolves to the canonical DEX name."""
    metadata.side_effect = [{"universe": [{"name": "BTC"}]}, {"universe": [{"name": meta_name}]}]
    assert api.resolve_hyperliquid_coin_name(coin=coin) == "xyz:AAPL"
    names, upper = api._load_hyperliquid_meta_names()
    assert "xyz:xyz:AAPL" not in names
    assert upper["XYZ:AAPL"] == "xyz:AAPL"
    assert metadata.call_count == 2


@pytest.mark.parametrize("coin, expected", [("BTC", "BTC"), ("btcusdc", "BTC"), ("KPEPE", "kPEPE")])
def test_standard_coins_keep_canonical_names(metadata, coin, expected):
    """Standard perpetuals and case-sensitive k-prefix names remain resolvable."""
    assert api.resolve_hyperliquid_coin_name(coin=coin) == expected


def test_cache_retains_aliases_and_returns_copies(metadata):
    """Warm-cache reads preserve both lookup forms and cannot mutate cached names."""
    names, upper = api._load_hyperliquid_meta_names()
    names.clear()
    upper.clear()
    assert api.resolve_hyperliquid_coin_name(coin="XYZ-AAPL") == "xyz:AAPL"
    assert metadata.call_count == 2


def test_standard_universe_does_not_gain_dex_alias(metadata):
    """A standard coin is not falsely accepted as a DEX-listed instrument."""
    with pytest.raises(ValueError, match="does not contain"):
        api.resolve_hyperliquid_coin_name(coin="XYZ-BTC")


def test_candle_request_uses_dex_qualified_coin(metadata):
    """Candle retrieval sends the resolved alias to the info endpoint."""
    metadata.side_effect = [{"universe": []}, {"universe": [{"name": "AAPL"}]}, []]
    assert api.fetch_candle_snapshot(coin="XYZ-AAPL", interval="1m", start_ms=1000, end_ms=61000) == []
    assert metadata.call_args.args[0]["req"]["coin"] == "xyz:AAPL"
