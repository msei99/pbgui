"""Offline Dashboard regressions for the Bitget UTA read-only rollout."""

import asyncio
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from api import dashboard


@pytest.fixture
def account(monkeypatch):
    """Provide an isolated Bitget account without private calls or runtime data."""
    user = SimpleNamespace(name="bitget-test", exchange="bitget")
    exchange = SimpleNamespace(
        instance=Mock(),
        ensure_bitget_account_mode=Mock(return_value=True),
        fetch_all_open_orders=Mock(return_value=[]),
        fetch_positions=Mock(return_value=[]),
    )
    db = SimpleNamespace(
        fetch_positions=Mock(return_value=[[0, "BTCUSDT", 0, 2, 10, 100, user.name, "long"]]),
        fetch_prices=Mock(return_value=[[0, "BTCUSDT", 0, 110]]),
        fetch_orders_by_symbol=Mock(return_value=[]),
    )
    monkeypatch.setattr(dashboard, "_get_users", lambda: SimpleNamespace(
        find_user=lambda name: user if name == user.name else None, list=lambda: [user.name],
    ))
    monkeypatch.setattr(dashboard, "_get_exchange", lambda _user: exchange)
    monkeypatch.setattr(dashboard, "_get_db", lambda: db)
    monkeypatch.setattr(dashboard, "_log", Mock())
    return user, exchange, db


@pytest.mark.parametrize("mode", [True, None, "false", RuntimeError("detection failed"), HTTPException(503, "unavailable")])
@pytest.mark.parametrize("endpoint", ["preview", "execute"])
def test_market_close_api_fails_closed(account, mode, endpoint):
    """UTA and unverified accounts return HTTP 409 before state lookup or orders."""
    user, exchange, db = account
    if isinstance(mode, Exception):
        exchange.ensure_bitget_account_mode.side_effect = mode
    else:
        exchange.ensure_bitget_account_mode.return_value = mode
    app = FastAPI()
    app.include_router(dashboard.router)
    app.dependency_overrides[dashboard.require_auth] = lambda: None
    with TestClient(app) as client:
        if endpoint == "preview":
            response = client.get("/positions/close_price", params={"user": user.name, "symbol": "BTCUSDT"})
        else:
            response = client.post("/positions/manage", json={
                "user": user.name, "symbol": "BTCUSDT", "side": "long", "amount": 1,
            })
    assert response.status_code == 409
    reason = "read-only rollout" if mode is True else "could not be verified"
    assert reason in response.json()["detail"]
    exchange.instance.create_order.assert_not_called()
    exchange.fetch_positions.assert_not_called()
    db.fetch_positions.assert_not_called()


@pytest.mark.parametrize("side,order_side", [("long", "sell"), ("short", "buy")])
def test_classic_market_close_parameters_unchanged(account, monkeypatch, side, order_side):
    """Verified Classic retains the shipped reduce-only hedge close parameters."""
    user, exchange, _db = account
    exchange.ensure_bitget_account_mode.return_value = False
    exchange.instance.amount_to_precision.return_value = "1"
    exchange.instance.create_order.return_value = {"id": "closed"}
    monkeypatch.setattr(dashboard, "_live_positions_for_user", lambda *_args: [{
        "user": user.name, "symbol": "BTCUSDT", "side": side, "size": 2, "price": 110,
    }])
    result = dashboard._execute_market_close(dashboard.PositionManagePayload(
        user=user.name, symbol="BTCUSDT", side=side, amount=1,
    ))
    assert result["ok"]
    exchange.instance.create_order.assert_called_once_with(
        "BTC/USDT:USDT", "market", order_side, 1.0, None,
        {"reduceOnly": True, "holdSide": side, "oneWayMode": False},
    )
    assert dashboard.get_position_close_price(user.name, "BTCUSDT", side, None)["ok"]


def test_close_rechecks_actual_order_client(account, monkeypatch):
    """A changed client or mode cannot pass the early capability check and submit."""
    user, exchange, _db = account
    exchange.ensure_bitget_account_mode.side_effect = [False, True]
    monkeypatch.setattr(dashboard, "_live_positions_for_user", lambda *_args: [{
        "user": user.name, "symbol": "BTCUSDT", "side": "long", "size": 2,
    }])
    with pytest.raises(HTTPException) as error:
        dashboard._execute_market_close(dashboard.PositionManagePayload(user=user.name, symbol="BTCUSDT", amount=1))
    assert error.value.status_code == 409
    exchange.instance.create_order.assert_not_called()


@pytest.mark.parametrize("mode,supported", [(False, True), (True, False), (None, False)])
def test_position_rows_expose_account_capability(account, mode, supported):
    """Existing Dashboard capability fields describe the actual Bitget account."""
    user, exchange, _db = account
    exchange.ensure_bitget_account_mode.return_value = mode
    result = dashboard.get_positions_data(user.name, False, None)
    row = result["positions"][0]
    assert row["market_close_supported"] is supported
    assert bool(row["market_close_reason"]) is not supported
    assert result["source"] == "db"


@pytest.mark.parametrize("key", ["posSide", "holdSide"])
@pytest.mark.parametrize("side", ["long", "short"])
def test_live_bitget_rows_and_orders(account, key, side):
    """Raw UTA/Classic side fields feed normalized rows and per-leg DCA/TP."""
    user, exchange, db = account
    entry_side, close_side = ("buy", "sell") if side == "long" else ("sell", "buy")
    exchange.fetch_positions.return_value = [{
        "symbol": "BTC/USDT:USDT", "side": None, "contracts": 2, "contractSize": 0.1,
        "entryPrice": 100, "markPrice": 110, "unrealizedPnl": 2, "info": {key: side},
    }]
    exchange.fetch_all_open_orders.return_value = [
        {"status": "open", "side": entry_side, "amount": 1, "price": 90, "info": {key: side}},
        {"status": "open", "side": close_side, "amount": 1, "price": 120, "info": {key: side}},
        {"status": "open", "side": entry_side, "amount": 1, "price": 95,
         "info": {key: "short" if side == "long" else "long"}},
    ]
    row = dashboard._live_positions_for_user(user, db)[0]
    assert (row["symbol"], row["side"], row["size"], row["upnl"]) == ("BTCUSDT", side, 0.2, 2)
    assert (row["dca"], row["next_dca"], row["next_tp"]) == (1, 90, 120)
    exchange.fetch_all_open_orders.assert_called_once_with("BTC/USDT:USDT")
    exchange.instance.fetch_open_orders.assert_not_called()


def test_opposite_leg_orders_do_not_become_dca(account):
    """An explicit opposite leg is not an ambiguous order fallback."""
    user, exchange, db = account
    exchange.fetch_all_open_orders.return_value = [{
        "side": "buy", "amount": 1, "price": 90, "info": {"posSide": "short"},
    }]
    assert dashboard._classify_orders_for_position(user, db, "BTCUSDT", "long") == (0, 0, 0)


def test_bitget_order_read_error_is_not_an_empty_live_snapshot(account):
    """Failed routed reads propagate or explicitly use the existing DB fallback."""
    user, exchange, db = account
    exchange.fetch_all_open_orders.side_effect = RuntimeError("mode unavailable")
    with pytest.raises(RuntimeError, match="mode unavailable"):
        dashboard._live_open_orders_for_symbol(user, "BTCUSDT")
    db.fetch_orders_by_symbol.return_value = [[0, 0, 0, 1, 90, "buy"]]
    orders, _unknown, source = dashboard._dashboard_orders_for_position(user, db, "BTCUSDT", "long")
    assert orders == [{"price": 90, "amount": 1, "side": "buy"}]
    assert source == "db"


@pytest.mark.parametrize("uta", [False, True])
@pytest.mark.parametrize("stream", ["positions", "orders"])
def test_bitget_rest_poll_updates_preserves_errors_and_clears_empty(account, monkeypatch, uta, stream):
    """Classic and UTA poll full snapshots, refresh credentials and never use WS."""
    from Exchange import Exchange

    user, exchange, _db = account
    exchange.ensure_bitget_account_mode.return_value = uta
    rotated = SimpleNamespace(name=user.name, exchange="bitget", key="rotated-test-key")
    current_user = [user]
    monkeypatch.setattr(dashboard, "_get_users", lambda: SimpleNamespace(find_user=lambda _name: current_user[0]))
    subscribers = {
        (user.name, "BTCUSDT", "long"): {object()},
        (user.name, "ETHUSDC", "short"): {object()},
        ("other-user", "BTCUSDT", "long"): {object()},
    }
    subscriber_attr = "_order_subscribers" if stream == "orders" else "_position_subscribers"
    monkeypatch.setattr(dashboard, subscriber_attr, subscribers)
    acquire = AsyncMock(side_effect=AssertionError("Bitget must not acquire WS"))
    release = AsyncMock()
    monkeypatch.setattr(Exchange, "get_private_ws_client", acquire)
    monkeypatch.setattr(Exchange, "release_private_ws_client", release)
    notify = Mock()
    monkeypatch.setattr(dashboard, "_notify_order_update" if stream == "orders" else "_notify_position_update", notify)
    iteration = [0]

    def snapshot(read_user, *args):
        """Supply populated, failed, then empty account snapshots without RPC."""
        assert read_user is (user if iteration[0] == 0 else rotated)
        if iteration[0] == 1:
            raise RuntimeError("private read failed")
        if iteration[0] == 2:
            return []
        if stream == "orders":
            assert args[0] in {"BTCUSDT", "ETHUSDC"}
            return [{"side": "buy", "amount": 1, "price": 90, "info": {"posSide": "long"}},
                    {"side": "sell", "amount": 2, "price": 120, "info": {"posSide": "short"}}]
        return [{"symbol": "BTCUSDT", "side": "long", "entry": 90, "size": 1, "upnl": 2},
                {"symbol": "ETHUSDC", "side": "short", "entry": 120, "size": 2, "upnl": 3},
                {"symbol": "SOLUSDT", "side": "long", "entry": 10, "size": 3, "upnl": 4}]

    async def next_poll(delay):
        """Advance bounded polling without wall-clock waits; remove the final owner."""
        assert delay == 5
        assert notify.call_count == (4 if iteration[0] == 2 else 2)
        current_user[0] = rotated
        iteration[0] += 1
        if iteration[0] == 3:
            for key in list(subscribers):
                if key[0] == user.name:
                    del subscribers[key]

    fetch = Mock(side_effect=snapshot)
    monkeypatch.setattr(dashboard, "_live_open_orders_for_symbol" if stream == "orders" else "_live_positions_for_user", fetch)
    monkeypatch.setattr(dashboard._asyncio, "sleep", next_poll)
    asyncio.run(getattr(dashboard, f"_watch_{stream}_stream")(user.name))
    acquire.assert_not_called()
    release.assert_not_called()
    if stream == "orders":
        notify.assert_any_call(user.name, "BTCUSDT", "long", [{"price": 90, "amount": 1, "side": "buy"}], unknown=False)
        notify.assert_any_call(user.name, "ETHUSDC", "short", [{"price": 120, "amount": 2, "side": "sell"}], unknown=False)
        notify.assert_any_call(user.name, "BTCUSDT", "long", [], unknown=False)
        notify.assert_any_call(user.name, "ETHUSDC", "short", [], unknown=False)
    else:
        notify.assert_any_call(user.name, "BTCUSDT", "long", {"entry": 90, "size": 1, "upnl": 2, "side": "long"})
        notify.assert_any_call(user.name, "ETHUSDC", "short", {"entry": 120, "size": 2, "upnl": 3, "side": "short"})
        notify.assert_any_call(user.name, "BTCUSDT", "long", None)
        notify.assert_any_call(user.name, "ETHUSDC", "short", None)


@pytest.mark.parametrize("stream", ["positions", "orders"])
def test_bitget_unsubscribe_drains_inflight_read_without_orphans(account, monkeypatch, stream):
    """Unregister awaits an in-flight read, publishes nothing stale and removes tasks."""
    user, _exchange, _db = account
    notify = Mock()
    monkeypatch.setattr(dashboard, "_notify_order_update", notify)
    monkeypatch.setattr(dashboard, "_notify_position_update", notify)
    for name in ("_candle_subscribers", "_position_subscribers", "_order_subscribers",
                 "_ws_ohlcv_tasks", "_ws_position_tasks", "_ws_order_tasks"):
        monkeypatch.setattr(dashboard, name, {})
    monkeypatch.setattr(dashboard, "_stream_task_lock", None)

    async def run():
        """Exercise actual task cancellation while the owned worker is blocked."""
        baseline = asyncio.all_tasks()
        loop = asyncio.get_running_loop()
        started = asyncio.Event()
        finish = threading.Event()
        finished = threading.Event()
        queue = asyncio.Queue()

        def read(*_args):
            """Keep one isolated read running until cancellation has been requested."""
            loop.call_soon_threadsafe(started.set)
            assert finish.wait(5)
            finished.set()
            return []

        monkeypatch.setattr(dashboard, "_live_open_orders_for_symbol" if stream == "orders" else "_live_positions_for_user", read)
        subscribers = dashboard._order_subscribers if stream == "orders" else dashboard._position_subscribers
        registry = dashboard._ws_order_tasks if stream == "orders" else dashboard._ws_position_tasks
        subscribers[(user.name, "BTCUSDT", "long")] = {queue}
        task = asyncio.create_task(getattr(dashboard, f"_watch_{stream}_stream")(user.name))
        registry[user.name] = task
        await asyncio.wait_for(started.wait(), 2)
        stop = asyncio.create_task(dashboard.unregister_chart_client(user.name, "BTCUSDT", "1m", "long", queue))
        try:
            await asyncio.sleep(0.01)
            assert not stop.done()
            assert not finished.is_set()
        finally:
            finish.set()
            await asyncio.wait_for(stop, 2)
        assert finished.is_set()
        assert task.cancelled()
        assert registry == {}
        assert asyncio.all_tasks() == baseline
        notify.assert_not_called()

    asyncio.run(run())
