"""Fail-closed persistence contracts for PB8-native exchange polling."""

from __future__ import annotations

from types import SimpleNamespace
import threading

import Database as database_module


def test_order_fetch_failure_aborts_before_database_mutation(monkeypatch) -> None:
    """A partial open-order snapshot must not delete previously persisted rows."""
    db = object.__new__(database_module.Database)
    db._write_lock = threading.Lock()
    user = SimpleNamespace(name="alice", exchange="weex")
    monkeypatch.setattr(db, "fetch_positions", lambda _user: [
        [1, "BTCUSDT", 0, 1, 0, 1, "alice", "long"],
        [2, "ETHUSDT", 0, 1, 0, 1, "alice", "long"],
    ])
    monkeypatch.setattr(db, "fetch_orders", lambda _user: [[1, 0, 0, 1, 1, "buy", "stored", "BTCUSDT", "alice"]])
    monkeypatch.setattr(db, "_connect", lambda: (_ for _ in ()).throw(AssertionError("DB mutation must not start")))

    class Exchange:
        calls = 0

        def fetch_all_open_orders(self, _symbol):
            self.calls += 1
            if self.calls == 1:
                return [{"id": "fresh", "timestamp": 1, "amount": 1, "price": 1, "side": "buy", "symbol": "BTC/USDT:USDT"}]
            raise RuntimeError("temporary failure")

    db.update_orders(user, Exchange())
