"""Regression tests for live dashboard exchange call counts (#386, #397)."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from api import dashboard


def _ccxt_position(symbol, side="long"):
    return {"symbol": symbol, "contracts": 1, "side": side, "entryPrice": 100, "unrealizedPnl": 1, "markPrice": 101}


def _order(symbol, side, price):
    return {"symbol": symbol, "status": "open", "side": side, "amount": 1, "price": price, "info": {"positionSide": "long"}}


@pytest.fixture
def ccxt_account(monkeypatch):
    """Provide a Bybit-like account whose private reads are counted mocks."""
    user = SimpleNamespace(name="alice", exchange="bybit")
    exchange = SimpleNamespace(instance=Mock(), fetch_positions=Mock(), fetch_balance=Mock(return_value=1000.0))
    exchange.fetch_positions.return_value = [_ccxt_position("BTC/USDT:USDT"), _ccxt_position("ETH/USDT:USDT")]
    exchange.instance.fetch_open_orders.return_value = [
        _order("BTC/USDT:USDT", "buy", 90), _order("BTC/USDT:USDT", "sell", 120),
        _order("ETH/USDT:USDT", "buy", 80),
    ]
    exchange.fetch_all_open_orders = Mock(return_value=exchange.instance.fetch_open_orders.return_value)
    monkeypatch.setattr(dashboard, "_get_exchange", lambda _user: exchange)
    monkeypatch.setattr(dashboard, "_log", Mock())
    return user, exchange


def test_failed_account_snapshot_keeps_per_symbol_fallback(ccxt_account):
    """An incomplete account read falls back instead of hiding DCA/TP orders (#386)."""
    user, exchange = ccxt_account
    exchange.fetch_all_open_orders.side_effect = RuntimeError("incomplete page")
    orders = exchange.instance.fetch_open_orders.return_value
    exchange.instance.fetch_open_orders.side_effect = lambda symbol=None: (
        orders[:1] if symbol is None else [order for order in orders if order["symbol"] == symbol]
    )
    rows = {row["symbol"]: row for row in dashboard._live_positions_for_user(user, Mock())}
    assert [call.kwargs for call in exchange.instance.fetch_open_orders.call_args_list] == [
        {"symbol": "BTC/USDT:USDT"}, {"symbol": "ETH/USDT:USDT"},
    ]
    assert (rows["BTCUSDT"]["dca"], rows["BTCUSDT"]["next_dca"], rows["BTCUSDT"]["next_tp"]) == (1, 90, 120)
    assert (rows["ETHUSDT"]["dca"], rows["ETHUSDT"]["next_dca"]) == (1, 80)
    exchange.instance.fetch_ticker.assert_not_called()


def test_bitget_uta_reads_all_open_orders_once(ccxt_account):
    """Bitget UTA reads every cursor page, so one account-wide read serves all positions."""
    user, exchange = ccxt_account
    user.exchange = "bitget"
    exchange.ensure_bitget_account_mode = Mock(return_value=True)
    exchange.fetch_all_open_orders = Mock(return_value=exchange.instance.fetch_open_orders.return_value)
    exchange.fetch_positions.return_value.append(_ccxt_position("SOL/USDT:USDT"))
    rows = {row["symbol"]: row for row in dashboard._live_positions_for_user(user, Mock())}
    exchange.fetch_all_open_orders.assert_called_once_with(None)
    exchange.instance.fetch_open_orders.assert_not_called()
    assert rows["BTCUSDT"]["next_tp"] == 120
    assert rows["SOLUSDT"]["dca"] == 0


def test_bitget_classic_uses_complete_account_reader(ccxt_account):
    """Bitget Classic uses the paginated reader once for all positions."""
    user, exchange = ccxt_account
    user.exchange = "bitget"
    exchange.ensure_bitget_account_mode = Mock(return_value=False)
    dashboard._live_positions_for_user(user, Mock())
    exchange.fetch_all_open_orders.assert_called_once_with(None, settle_coins=("USDT",))
    exchange.instance.fetch_open_orders.assert_not_called()


def test_live_balance_skips_order_and_ticker_reads(ccxt_account):
    """Balance only needs size, entry and uPnL from the positions payload (#397)."""
    user, exchange = ccxt_account
    for position in exchange.fetch_positions.return_value:
        position.pop("markPrice")
    assert dashboard._live_balance_for_user(user, Mock()) == (1000.0, 2.0, 200.0)
    exchange.instance.fetch_open_orders.assert_not_called()
    exchange.instance.fetch_ticker.assert_not_called()


def test_hyperliquid_positions_and_balance_share_requests(monkeypatch):
    """Hyperliquid reads one account state for balance and one order list for positions."""
    user = SimpleNamespace(name="hl", exchange="hyperliquid", wallet_address="0xabc")
    state = {
        "marginSummary": {"accountValue": "1010"},
        "assetPositions": [
            {"position": {"coin": "BTC", "szi": "1", "entryPx": "100", "unrealizedPnl": "10", "positionValue": "110"}},
            {"position": {"coin": "ETH", "szi": "-2", "entryPx": "50", "unrealizedPnl": "0", "positionValue": "100"}},
        ],
    }
    state_reads = Mock(return_value=state)
    order_reads = Mock(return_value=[
        {"price": 90.0, "amount": 1.0, "side": "buy", "info": {"coin": "BTC"}},
        {"price": 120.0, "amount": 1.0, "side": "sell", "info": {"coin": "BTC"}},
    ])
    monkeypatch.setattr(dashboard, "_hyperliquid_user_state", state_reads)
    monkeypatch.setattr(dashboard, "_hyperliquid_open_orders", order_reads)

    rows = {row["symbol"]: row for row in dashboard._live_positions_for_user(user, Mock())}
    assert order_reads.call_count == 1
    assert (rows["BTCUSDC"]["dca"], rows["BTCUSDC"]["next_dca"], rows["BTCUSDC"]["next_tp"]) == (1, 90.0, 120.0)
    assert rows["ETHUSDC"]["dca"] == 0

    state_reads.reset_mock()
    order_reads.reset_mock()
    assert dashboard._live_balance_for_user(user, Mock()) == (1000.0, 10.0, 200.0)
    assert state_reads.call_count == 1
    order_reads.assert_not_called()


def test_db_positions_read_orders_once_per_user(monkeypatch):
    """DB fallback groups one orders query instead of one query per position (#386)."""
    user = SimpleNamespace(name="alice", exchange="bybit")
    db = SimpleNamespace(
        fetch_positions=Mock(return_value=[
            [0, "BTCUSDT", 0, 1, 0, 100, "alice", "long"],
            [1, "ETHUSDT", 0, 1, 0, 50, "alice", "long"],
        ]),
        fetch_prices=Mock(return_value=[[0, "BTCUSDT", 0, 110], [1, "ETHUSDT", 0, 55]]),
        fetch_orders=Mock(return_value=[
            [0, "BTCUSDT", 0, 1, 90, "buy"], [1, "BTCUSDT", 0, 1, 120, "sell"], [2, "ETHUSDT", 0, 1, 40, "buy"],
        ]),
        fetch_orders_by_symbol=Mock(side_effect=AssertionError("per-symbol query")),
    )
    monkeypatch.setattr(dashboard, "_get_db", lambda: db)
    monkeypatch.setattr(dashboard, "_get_users", lambda: SimpleNamespace(
        list=lambda: ["alice"], find_user=lambda name: user if name == "alice" else None,
    ))
    monkeypatch.setattr(dashboard, "_market_close_capability", lambda *_: {})
    rows = {row["symbol"]: row for row in dashboard.get_positions_data(users="alice", live=False, session=None)["positions"]}
    db.fetch_orders.assert_called_once_with(user)
    assert (rows["BTCUSDT"]["price"], rows["BTCUSDT"]["dca"], rows["BTCUSDT"]["next_tp"]) == (110, 1, 120)
    assert (rows["ETHUSDT"]["price"], rows["ETHUSDT"]["next_dca"]) == (55, 40)
