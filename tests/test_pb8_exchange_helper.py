"""Offline unit contracts for the PB8-native exchange helper."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

import pb8_exchange_helper


def test_weex_wallet_balance_excludes_unrealized_pnl() -> None:
    """WEEX equity must be normalized back to realised wallet balance."""
    fetched = {"info": [{"asset": "USDT", "balance": "100.25", "unrealizePnl": "-1.75"}]}

    assert pb8_exchange_helper._weex_wallet_balance(fetched) == 102.0


@pytest.mark.parametrize(
    "fetched",
    [
        {"info": []},
        {"info": [{"asset": "USDT", "balance": "nan", "unrealizePnl": "0"}]},
        {"info": [{"asset": "USDC", "balance": "10", "unrealizePnl": "0"}]},
    ],
)
def test_weex_wallet_balance_fails_closed(fetched) -> None:
    """Malformed or non-finite WEEX account rows must not enter the Dashboard."""
    with pytest.raises(ValueError):
        pb8_exchange_helper._weex_wallet_balance(fetched)


def test_credentials_require_weex_passphrase() -> None:
    """The isolated helper independently enforces WEEX's credential contract."""
    with pytest.raises(ValueError, match="passphrase"):
        pb8_exchange_helper._credentials(
            {"credentials": {"key": "key", "secret": "secret"}},
            "weex",
        )


def test_load_modules_rejects_runtime_version_mismatch(monkeypatch, tmp_path) -> None:
    """A future PB8 connector contract must be reviewed before this bridge runs it."""
    src = tmp_path / "src"
    src.mkdir()
    fake_modules = {
        "exchanges.bitunix": SimpleNamespace(BitunixClient=object),
        "exchanges.weex": SimpleNamespace(AsyncWeex=object),
        "fill_events_manager": SimpleNamespace(BitunixFetcher=object, WeexFetcher=object),
        "passivbot_version": SimpleNamespace(__version__="8.2.0"),
    }
    monkeypatch.setitem(__import__("sys").modules, "exchanges.bitunix", fake_modules["exchanges.bitunix"])
    monkeypatch.setitem(__import__("sys").modules, "exchanges.weex", fake_modules["exchanges.weex"])
    monkeypatch.setitem(__import__("sys").modules, "fill_events_manager", fake_modules["fill_events_manager"])
    monkeypatch.setitem(__import__("sys").modules, "passivbot_version", fake_modules["passivbot_version"])

    with pytest.raises(RuntimeError, match="requires 8.1.0"):
        pb8_exchange_helper._load_modules(tmp_path)


def test_oversized_fill_result_requests_smaller_window(monkeypatch, tmp_path) -> None:
    """The helper signals bounded pagination without returning or dropping an oversized event set."""
    class Client:
        async def load_markets(self):
            return None

        async def close(self):
            return None

    class Fetcher:
        def __init__(self, _client):
            pass

        async def fetch(self, _since, _until, _state):
            return [{}] * (pb8_exchange_helper.MAX_FILLS + 1)

    modules = {
        "version": "8.1.0",
        "BitunixFetcher": Fetcher,
        "WeexFetcher": Fetcher,
    }
    monkeypatch.setattr(pb8_exchange_helper, "_load_modules", lambda _path: modules)

    async def build_client(*_args):
        return Client()

    monkeypatch.setattr(pb8_exchange_helper, "_build_client", build_client)

    result = asyncio.run(pb8_exchange_helper._dispatch({
        "pb8_dir": str(tmp_path),
        "exchange": "bitunix",
        "operation": "fills",
        "credentials": {"key": "key", "secret": "secret"},
        "since": 0,
        "until": 10,
    }))

    assert result == {"version": "8.1.0", "fills": [], "too_many": True}


def test_bitunix_saturated_trade_page_requests_smaller_window(monkeypatch, tmp_path) -> None:
    """A full Bitunix page is ambiguous and must be paged instead of accepted as complete."""
    class Client:
        async def load_markets(self):
            return None

        async def close(self):
            return None

    class Fetcher:
        trade_limit = 100

        def __init__(self, _client):
            pass

        async def fetch(self, _since, _until, _state):
            return [{"id": str(index)} for index in range(self.trade_limit)]

    monkeypatch.setattr(pb8_exchange_helper, "_load_modules", lambda _path: {
        "version": "8.1.0",
        "BitunixFetcher": Fetcher,
        "WeexFetcher": Fetcher,
    })

    async def build_client(*_args):
        return Client()

    monkeypatch.setattr(pb8_exchange_helper, "_build_client", build_client)

    result = asyncio.run(pb8_exchange_helper._dispatch({
        "pb8_dir": str(tmp_path),
        "exchange": "bitunix",
        "operation": "fills",
        "credentials": {"key": "key", "secret": "secret"},
        "since": 0,
        "until": 10,
    }))

    assert result == {"version": "8.1.0", "fills": [], "too_many": True}
