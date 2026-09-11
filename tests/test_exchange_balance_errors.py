"""Balance failures must propagate without being persisted as account data."""

from types import SimpleNamespace
from unittest.mock import Mock

from ccxt.base.errors import AuthenticationError, ExchangeNotAvailable, RequestTimeout
import pytest

from Exchange import Exchange
from Database import Database


@pytest.mark.parametrize("exchange_id", ["binance", "bybit", "bitget", "hyperliquid", "okx", "gateio", "bitunix", "weex"])
@pytest.mark.parametrize("error_type", [AuthenticationError, ExchangeNotAvailable, RequestTimeout])
def test_balance_raises_original_exchange_failure(exchange_id, error_type, monkeypatch):
    """Authentication and transient failures retain their original type and identity."""
    exchange = Exchange(exchange_id)
    error = error_type("test failure")
    exchange.instance = Mock()
    exchange.instance.fetch_balance.side_effect = error
    monkeypatch.setattr(exchange, "ensure_bitget_account_mode", lambda: False)
    with pytest.raises(error_type) as caught:
        exchange.fetch_balance("swap")
    assert caught.value is error
    exchange.instance.fetch_balance.assert_called_once_with(params={"type": "swap"})


@pytest.mark.parametrize("exchange_id, payload", [
    ("binance", {"info": {"totalWalletBalance": "42.5"}}),
    ("bybit", {"info": {"result": {"list": [{"accountType": "UNIFIED", "totalWalletBalance": "42.5"}]}}}),
    ("bitget", {"info": [{"available": "42.5"}]}),
    ("hyperliquid", {"total": {"USDC": "42.5"}}),
    ("okx", {"total": {"USDT": "42.5"}}),
    ("bitunix", {"total": {"USDT": "42.5"}}),
    ("weex", {"total": {"USDT": "42.5"}}),
])
def test_successful_balance_still_returns_float(exchange_id, payload, monkeypatch):
    """Successful responses retain exchange-specific normalization."""
    exchange = Exchange(exchange_id)
    exchange.instance = Mock()
    exchange.instance.fetch_balance.return_value = payload
    monkeypatch.setattr(exchange, "ensure_bitget_account_mode", lambda: False)
    assert exchange.fetch_balance("swap") == 42.5


def test_database_skips_persistence_after_exchange_failure(monkeypatch, tmp_path):
    """A real Exchange failure reaches the database guard before opening a connection."""
    monkeypatch.setattr("Database.PBGDIR", tmp_path)
    exchange = Exchange("binance")
    exchange.instance = Mock()
    exchange.instance.fetch_balance.side_effect = AuthenticationError("test failure")
    database = object.__new__(Database)
    database._connect = Mock(side_effect=AssertionError("must not access database"))
    database.update_balance = Mock()
    logger = Mock()
    monkeypatch.setattr("Database._human_log", logger)
    Database.update_balances(database, SimpleNamespace(name="test-user"), _exchange=exchange)
    database._connect.assert_not_called()
    database.update_balance.assert_not_called()
    assert logger.call_args.kwargs["level"] == "WARNING"
