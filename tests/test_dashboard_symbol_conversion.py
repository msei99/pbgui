"""Dashboard symbol conversion and mocked market-close regressions."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from api import dashboard


@pytest.mark.parametrize("symbol,expected", [
    ("BTCUSDT", "BTC/USDT:USDT"), ("ETHUSDC", "ETH/USDC:USDC"),
    ("BTC/USDT:USDT", "BTC/USDT:USDT"), ("ETH/USDC:USDC", "ETH/USDC:USDC"),
    ("BTC/USDT", "BTC/USDT"), ("XYZ-AAPL/USDC:USDC", "XYZ-AAPL/USDC:USDC"),
    ("BTCUSD", "BTCUSD"), ("", ""), (None, ""),
])
def test_symbol_conversion_is_idempotent(symbol, expected):
    """Raw symbols convert once while CCXT and empty inputs remain stable."""
    assert dashboard._symbol_to_ccxt(symbol) == expected
    assert dashboard._symbol_to_ccxt(expected) == expected


@pytest.mark.parametrize("side,order_side", [("long", "sell"), ("short", "buy")])
def test_market_close_preserves_formatted_symbol(monkeypatch, side, order_side):
    """The real close helper passes a valid CCXT symbol to the mocked order client."""
    symbol = "BTC/USDT:USDT"
    user = SimpleNamespace(name="test-user", exchange="bitget")
    exchange = SimpleNamespace(instance=Mock(), ensure_bitget_account_mode=Mock(return_value=False))
    exchange.instance.amount_to_precision.return_value = "1"
    exchange.instance.create_order.return_value = {"id": "mock-close"}
    monkeypatch.setattr(dashboard, "_get_users", lambda: SimpleNamespace(find_user=lambda _: user))
    monkeypatch.setattr(dashboard, "_get_exchange", lambda _: exchange)
    monkeypatch.setattr(dashboard, "_get_db", lambda: Mock())
    monkeypatch.setattr(dashboard, "_log", Mock())
    monkeypatch.setattr(dashboard, "_live_positions_for_user", lambda *_: [{
        "user": user.name, "symbol": symbol, "side": side, "size": 2, "price": 110,
    }])
    result = dashboard._execute_market_close(dashboard.PositionManagePayload(
        user=user.name, symbol=symbol, side=side, amount=1,
    ))
    assert result["ok"]
    exchange.instance.create_order.assert_called_once_with(
        symbol, "market", order_side, 1.0, None,
        {"reduceOnly": True, "holdSide": side, "oneWayMode": False},
    )
