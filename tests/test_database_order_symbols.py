"""Order synchronization resolves mapped symbols and preserves skipped snapshots."""

from contextlib import nullcontext
import json
import threading
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import Database as database_module
import Exchange as exchange_module
from ccxt.base.errors import BadSymbol


@pytest.mark.parametrize("failure", ["missing", "unsupported", "transient", "none"])
def test_symbol_failure_does_not_discard_cached_orders(monkeypatch, tmp_path, failure):
    """A failed symbol preserves its cache while later symbols are still polled."""
    monkeypatch.setattr(database_module, "PBGDIR", tmp_path)
    monkeypatch.setattr(exchange_module, "PBGDIR", tmp_path)
    mapping = tmp_path / "data/coindata/bybit/mapping.json"
    mapping.parent.mkdir(parents=True)
    rows = [{"symbol": "BTCUSD", "ccxt_symbol": "BTC/USD:BTC"}]
    if failure != "missing":
        rows.append({"symbol": "ETHUSDT", "ccxt_symbol": "ETH/USDT:USDT"})
    mapping.write_text(json.dumps(rows))
    db = object.__new__(database_module.Database)
    db._write_lock = threading.Lock()
    db.fetch_positions = Mock(return_value=[[1, "ETHUSDT"], [2, "BTCUSD"]])
    db.fetch_orders = Mock(return_value=[
        [1, 0, 0, 1, 1, "buy", "cached-eth", "ETHUSDT", "alice"],
        [2, 0, 0, 1, 1, "buy", "cached-btc", "BTCUSD", "alice"],
    ])
    db._connect = Mock(return_value=nullcontext(object()))
    db.remove_order = Mock()
    db.add_order = Mock()
    db.update_order = Mock()

    def fetch(symbol):
        """Simulate independent exchange results for the two positions."""
        if symbol == "ETH/USDT:USDT":
            if failure == "none":
                return None
            if failure == "unsupported":
                raise BadSymbol("unsupported")
            raise RuntimeError("temporary failure")
        assert symbol == "BTC/USD:BTC"
        return [{"id": "fresh-btc", "timestamp": 1, "amount": 2,
                 "price": 3, "side": "buy", "symbol": symbol}]

    exchange = SimpleNamespace(fetch_all_open_orders=Mock(side_effect=fetch))
    db.update_orders(SimpleNamespace(name="alice", exchange="bybit"), exchange)
    assert exchange.fetch_all_open_orders.call_args.args == ("BTC/USD:BTC",)
    if failure in {"transient", "none"}:
        db._connect.assert_not_called()
        db.remove_order.assert_not_called()
        db.add_order.assert_not_called()
    else:
        assert [call.args[1] for call in db.remove_order.call_args_list] == [2]
        assert db.add_order.call_args.args[1] == [1, 2, 3, "buy", "fresh-btc", "BTCUSD", "alice"]
