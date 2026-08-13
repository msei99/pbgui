"""Offline contracts for the PB8-native Bitunix and WEEX read bridge."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

import pb8_exchange_bridge


def _user(exchange: str = "weex") -> SimpleNamespace:
    """Return one in-memory credential object for bridge tests."""
    return SimpleNamespace(name="alice", exchange=exchange, key="api-key", secret="api-secret", passphrase="api-pass")


def test_snapshot_reuses_one_helper_call_and_filters_orders(monkeypatch) -> None:
    """Sequential dashboard reads should share one bounded account snapshot."""
    instance = pb8_exchange_bridge.PB8ExchangeInstance("weex", _user())
    calls = []
    snapshot = {
        "balance": 100.0,
        "positions": [{"symbol": "BTC/USDT:USDT"}],
        "orders": [
            {"id": "btc", "symbol": "BTC/USDT:USDT"},
            {"id": "eth", "symbol": "ETH/USDT:USDT"},
        ],
        "tickers": {"BTC/USDT:USDT": {"last": 50000.0}},
    }
    monkeypatch.setattr(instance, "_call", lambda operation, **_payload: calls.append(operation) or snapshot)

    assert instance.fetch_balance()["total"]["USDT"] == 100.0
    assert instance.fetch_positions() == snapshot["positions"]
    assert instance.fetch_open_orders("BTC/USDT:USDT") == [snapshot["orders"][0]]
    assert instance.fetch_ticker("BTC/USDT:USDT")["last"] == 50000.0
    assert calls == ["account_snapshot"]


def test_fill_normalization_produces_income_and_execution_contracts(monkeypatch) -> None:
    """PB8 canonical fills must feed both dashboard history and executions."""
    instance = pb8_exchange_bridge.PB8ExchangeInstance("bitunix", _user("bitunix"))
    event = {
        "id": "trade-1",
        "order_id": "order-1",
        "timestamp": 123,
        "symbol": "BTC/USDT:USDT",
        "side": "sell",
        "qty": 0.1,
        "price": 50000.0,
        "pnl": 2.0,
        "fees": {"cost": 0.25},
        "raw": [{"source": "test"}],
    }
    monkeypatch.setattr(instance, "fetch_fills", lambda **_kwargs: [event])

    assert instance.fetch_income() == [{
        "symbol": "BTCUSDT",
        "timestamp": 123,
        "income": 1.75,
        "uniqueid": "bitunix:fill:trade-1",
    }]
    execution = instance.fetch_executions()[0]
    assert execution["trade_id"] == "trade-1"
    assert execution["realized_pnl"] == 2.0
    assert execution["fee"] == 0.25
    assert json.loads(execution["raw_json"]) == [{"source": "test"}]


def test_fill_fetch_splits_oversized_windows_and_deduplicates(monkeypatch) -> None:
    """Oversized PB8 fill windows are bisected until bounded responses make progress."""
    instance = pb8_exchange_bridge.PB8ExchangeInstance("bitunix", _user("bitunix"))
    calls = []

    def fake_call(_operation, **payload):
        calls.append((payload["since"], payload["until"]))
        if payload["until"] - payload["since"] > 4:
            return {"too_many": True, "fills": []}
        event_id = f"{payload['since']}-{payload['until']}"
        return {"fills": [{"id": event_id, "timestamp": payload["since"]}, {"id": "shared", "timestamp": 1}]}

    monkeypatch.setattr(instance, "_call", fake_call)

    fills = instance.fetch_fills(since=0, until=9)

    assert calls[0] == (0, 9)
    assert {event["id"] for event in fills} == {"0-4", "5-9", "shared"}


def test_helper_error_redacts_submitted_credentials(monkeypatch) -> None:
    """Credential values returned by an exchange error must not reach diagnostics."""
    instance = pb8_exchange_bridge.PB8ExchangeInstance("weex", _user())
    monkeypatch.setattr(pb8_exchange_bridge, "acquire_master_runtime_lock", lambda _root: SimpleNamespace(release=lambda: None))
    monkeypatch.setattr(pb8_exchange_bridge, "pb8_runtime_status", lambda: {
        "ready": True,
        "pb8dir": "/runtime/pb8",
        "pb8venv": "/runtime/python",
    })
    class FakePopen:
        """Write one helper error into the bounded output file."""

        returncode = 1

        def __init__(self, *_args, **kwargs):
            self.output = kwargs["stdout"]

        def communicate(self, input=None, timeout=None):
            del input, timeout
            self.output.write(b'{"ok":false,"detail":"api-secret api-pass"}')

        def kill(self):
            return None

        def wait(self):
            return self.returncode

    monkeypatch.setattr(pb8_exchange_bridge.subprocess, "Popen", FakePopen)

    with pytest.raises(pb8_exchange_bridge.PB8ExchangeError) as exc_info:
        instance._call("account_snapshot")

    assert "api-secret" not in str(exc_info.value)
    assert "api-pass" not in str(exc_info.value)
    assert "[redacted]" in str(exc_info.value)
