"""Offline regressions for Hyperliquid DEX metadata alias resolution."""

from unittest.mock import Mock
import json

import pytest

import hyperliquid_api as api


@pytest.fixture
def metadata(monkeypatch, tmp_path):
    """Isolate the metadata cache and prevent mapping or network reads."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(api, "_HYPERLIQUID_META_CACHE", {})
    monkeypatch.setattr(api, "get_symbol_for_coin", Mock(return_value=""))
    request = Mock(side_effect=lambda payload, **_: {"universe": [{"name": "AAPL"}]} if payload.get("dex") else
                   {"universe": [{"name": "BTC"}, {"name": "kPEPE"}]})
    monkeypatch.setattr(api, "hyperliquid_info_post", request)
    return request


@pytest.mark.parametrize("coin", ["XYZ-AAPL", "xyz-aapl", "xyz:AAPL", "XYZ:AAPL", "XYZ-AAPL/USDC:USDC"])
@pytest.mark.parametrize("meta_name", ["AAPL", "xyz:AAPL"])
def test_dex_alias_resolves_from_bare_or_qualified_metadata(metadata, coin, meta_name):
    """Every supported stock-perp spelling resolves to the canonical DEX name."""
    metadata.side_effect = [{"universe": [{"name": meta_name}]}]
    assert api.resolve_hyperliquid_coin_name(coin=coin) == "xyz:AAPL"
    names, upper = api._load_hyperliquid_meta_names(dex="xyz")
    assert "xyz:xyz:AAPL" not in names
    assert upper["XYZ:AAPL"] == "xyz:AAPL"
    assert metadata.call_count == 1
    assert metadata.call_args.args[0] == {"type": "meta", "dex": "xyz"}


@pytest.mark.parametrize("coin, expected", [("BTC", "BTC"), ("btcusdc", "BTC"), ("KPEPE", "kPEPE")])
def test_standard_coins_keep_canonical_names(metadata, coin, expected):
    """Standard perpetuals and case-sensitive k-prefix names remain resolvable."""
    assert api.resolve_hyperliquid_coin_name(coin=coin) == expected


def test_cache_retains_aliases_and_returns_copies(metadata):
    """Warm-cache reads preserve both lookup forms and cannot mutate cached names."""
    names, upper = api._load_hyperliquid_meta_names(dex="xyz")
    names.clear()
    upper.clear()
    assert api.resolve_hyperliquid_coin_name(coin="XYZ-AAPL") == "xyz:AAPL"
    assert metadata.call_count == 1


def test_standard_universe_does_not_gain_dex_alias(metadata):
    """A standard coin is not falsely accepted as a DEX-listed instrument."""
    with pytest.raises(ValueError, match="does not contain"):
        api.resolve_hyperliquid_coin_name(coin="XYZ-BTC")


def test_candle_request_uses_dex_qualified_coin(metadata):
    """Candle retrieval sends the resolved alias to the info endpoint."""
    metadata.side_effect = [{"universe": [{"name": "AAPL"}]}, []]
    assert api.fetch_candle_snapshot(coin="XYZ-AAPL", interval="1m", start_ms=1000, end_ms=61000) == []
    assert metadata.call_args.args[0]["req"]["coin"] == "xyz:AAPL"


@pytest.mark.parametrize("dex", ["xyz", "para", "mkts", "flx", "newdex"])
@pytest.mark.parametrize("ticker", ["FOOUSD", "FOOUSDC", "FOOEUR", "FOODAI"])
@pytest.mark.parametrize("separator", ["-", ":"])
def test_all_dexes_resolve_only_requested_namespace(metadata, tmp_path, dex, ticker, separator):
    """Local mapping confirms extra DEXes; quote endings survive the candle payload."""
    path = tmp_path / "data/coindata/hyperliquid/mapping.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps([{"is_hip3": True, "dex": dex, "coin": f"{dex.upper()}-{ticker}",
                                 "ccxt_symbol": f"{dex.upper()}-{ticker}/USDC:USDC"}]))
    metadata.side_effect = [{"universe": [{"name": ticker}]}, []]
    assert api.fetch_candle_snapshot(coin=f"{dex}{separator}{ticker}/USDC:USDC", interval="1m",
                                     start_ms=1000, end_ms=61000, timeout_s=7.25) == []
    assert metadata.call_args_list[0].args[0] == {"type": "meta", "dex": dex}
    assert metadata.call_args_list[0].kwargs == {"timeout_s": 7.25}
    assert metadata.call_args.args[0]["req"]["coin"] == f"{dex}:{ticker}"
    assert metadata.call_args.kwargs == {"timeout_s": 7.25}
    assert metadata.call_count == 2


@pytest.mark.parametrize("dex", ["xyz", "para", "mkts", "flx", "newdex"])
@pytest.mark.parametrize("ticker", ["FOOUSD", "FOOUSDC", "FOOEUR", "FOODAI", "FOO_BARUSD"])
def test_pure_normalizer_preserves_full_ticker(dex, ticker, monkeypatch):
    """Explicit context normalizes API names without reading files or the network."""
    row = {"is_hip3": True, "dex": dex, "coin": f"{dex.upper()}-{ticker}"}
    forbidden = Mock(side_effect=AssertionError("Unexpected IO"))
    monkeypatch.setattr(api.Path, "read_text", forbidden)
    monkeypatch.setattr(api.requests, "post", forbidden)
    for alias in [f"{dex}:{ticker}", f"{dex.upper()}-{ticker}/USDC:USDC", f"{dex.upper()}-{ticker}_USDC:USDC"]:
        assert api.normalize_hyperliquid_coin(alias, mapping_record=row) == f"{dex}:{ticker}"
    forbidden.assert_not_called()


@pytest.mark.parametrize("coin", ["newdex:FOO", "NEWDEx-FOOUSD", "bad.dex:FOO", "../bad:FOO"])
def test_unconfirmed_dex_never_queries_metadata(metadata, coin):
    """Arbitrary prefixes cannot trigger DEX metadata requests."""
    with pytest.raises(ValueError):
        api.resolve_hyperliquid_coin_name(coin=coin)
    metadata.assert_not_called()


@pytest.mark.parametrize("dex", ["xyz", "", "bad.dex", "../newdex", "newdex\ninvalid", "a" * 17])
def test_conflicting_mapping_does_not_confirm_extra_dex(metadata, tmp_path, dex):
    """A wrong, default or malformed DEX cannot authorize another namespace."""
    path = tmp_path / "data/coindata/hyperliquid/mapping.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps([{"is_hip3": True, "dex": dex, "coin": "NEWDEX-FOOUSD"}]))
    with pytest.raises(ValueError, match="Unconfirmed"):
        api.resolve_hyperliquid_coin_name(coin="newdex:FOOUSD")
    metadata.assert_not_called()


def test_dex_and_standard_collisions_are_isolated(metadata):
    """Identical tickers in multiple namespaces never satisfy one another."""
    metadata.side_effect = lambda payload, **_: {"universe": [{"name": "SAME"}]} if payload.get("dex") != "mkts" else {"universe": [{"name": "para:SAME"}]}
    assert api.resolve_hyperliquid_coin_name(coin="SAME") == "SAME"
    assert api.resolve_hyperliquid_coin_name(coin="para:SAME") == "para:SAME"
    assert api.resolve_hyperliquid_coin_name(coin="xyz:SAME") == "xyz:SAME"
    with pytest.raises(ValueError):
        api.resolve_hyperliquid_coin_name(coin="mkts:SAME")
    assert set(api._HYPERLIQUID_META_CACHE) == {"", "para", "xyz", "mkts"}


def test_cache_expiry_limit_and_failure_isolation(metadata, monkeypatch):
    """A 300-second TTL and 64-entry cap bound independent DEX caches."""
    now = [1000.0]
    monkeypatch.setattr(api.time, "monotonic", lambda: now[0])
    metadata.side_effect = lambda *args, **kwargs: {"universe": [{"name": "SAME"}]}
    for i in range(65):
        api._load_hyperliquid_meta_names(dex=f"test{i}", timeout_s=2)
    assert len(api._HYPERLIQUID_META_CACHE) == 64
    assert "test0" not in api._HYPERLIQUID_META_CACHE
    assert api.resolve_hyperliquid_coin_name(coin="xyz:SAME") == "xyz:SAME"
    calls = metadata.call_count
    now[0] += 299
    assert api.resolve_hyperliquid_coin_name(coin="xyz:SAME") == "xyz:SAME"
    assert metadata.call_count == calls
    metadata.side_effect = TimeoutError("synthetic")
    with pytest.raises(ValueError, match="unavailable"):
        api.resolve_hyperliquid_coin_name(coin="para:SAME", timeout_s=2)
    assert api.resolve_hyperliquid_coin_name(coin="xyz:SAME") == "xyz:SAME"
    now[0] += 1
    metadata.side_effect = [{"universe": [{"name": "NEW"}]}]
    assert api.resolve_hyperliquid_coin_name(coin="xyz:NEW") == "xyz:NEW"
    assert set(api._HYPERLIQUID_META_CACHE) == {"xyz"}


@pytest.mark.parametrize("failure", [None, "status", "json"])
def test_http_response_is_closed_on_success_and_failure(monkeypatch, failure):
    """Every obtained response is closed, including HTTP and JSON exceptions."""
    response = Mock()
    response.json.return_value = {"universe": []}
    if failure == "status":
        response.raise_for_status.side_effect = RuntimeError("synthetic status")
    if failure == "json":
        response.json.side_effect = ValueError("synthetic JSON")
    request = Mock(return_value=response)
    monkeypatch.setattr(api.requests, "post", request)
    if failure:
        with pytest.raises((RuntimeError, ValueError)):
            api.hyperliquid_info_post({"type": "meta"}, timeout_s=4.5)
    else:
        assert api.hyperliquid_info_post({"type": "meta"}, timeout_s=4.5) == {"universe": []}
    response.close.assert_called_once_with()
    assert request.call_args.kwargs["timeout"] == 4.5
