"""Bybit history pagination and retries with mocked private API responses."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from ccxt.base.errors import AuthenticationError, RequestTimeout, RateLimitExceeded

from Exchange import Exchange


@pytest.fixture(params=[True, False], ids=["unified", "classic"])
def history_client(request, monkeypatch):
    """Provide both Bybit account modes without network requests or retry delays."""
    exchange = Exchange("bybit", SimpleNamespace(key="test", name="test-user"))
    exchange.instance = Mock()
    exchange.instance.milliseconds.return_value = 2000
    exchange.instance.is_unified_enabled.return_value = (False, request.param)
    method = (exchange.instance.privateGetV5AccountTransactionLog if request.param
              else exchange.instance.privateGetV5AccountContractTransactionLog)
    delay = Mock()
    monkeypatch.setattr("Exchange.sleep", delay)
    return exchange, method, delay


def page(cursor="", rows=None):
    """Create one cursor page containing valid transaction records."""
    return {"result": {"nextPageCursor": cursor, "list": rows or []}}


def row(identifier):
    """Create a trade transaction with the fields required by normalization."""
    return {"type": "TRADE", "symbol": "BTCUSDT", "transactionTime": "1500",
            "change": "12.5", "id": identifier}


def test_success_requests_each_cursor_once(history_client):
    """Successful pages make one call each and normalize every transaction once."""
    exchange, method, delay = history_client
    method.side_effect = [page("next", [row("a")]), page(rows=[row("b")])]
    result = exchange.fetch_history(1000)
    assert {item["uniqueid"] for item in result} == {"a", "b"}
    assert len(result) == 2
    assert [call.kwargs["params"]["cursor"] for call in method.call_args_list] == [None, "next"]
    delay.assert_not_called()


def test_empty_cursor_page_continues_same_window(history_client):
    """An empty first page with a cursor must neither crash nor skip its next page."""
    exchange, method, delay = history_client
    method.side_effect = [page("next"), page(rows=[row("a")])]
    assert exchange.fetch_history(1000)[0]["uniqueid"] == "a"
    assert [call.kwargs["params"]["startTime"] for call in method.call_args_list] == [1000, 1000]
    delay.assert_not_called()


def test_empty_terminal_page_finishes(history_client):
    """A terminal empty page completes the window after one request."""
    exchange, method, delay = history_client
    method.return_value = page()
    assert exchange.fetch_history(1000) == []
    method.assert_called_once()
    delay.assert_not_called()


@pytest.mark.parametrize("error_type", [RequestTimeout, RateLimitExceeded])
def test_transient_failure_retries_until_success(history_client, error_type):
    """Transient failures retry the same page and stop immediately on success."""
    exchange, method, delay = history_client
    method.side_effect = [error_type("test"), page(rows=[row("a")])]
    assert len(exchange.fetch_history(1000)) == 1
    assert method.call_count == 2
    delay.assert_called_once_with(5)


@pytest.mark.parametrize("previous_page", [False, True])
def test_exhaustion_raises_without_partial_history(history_client, previous_page):
    """Exhaustion propagates the original error, even after an earlier successful page."""
    exchange, method, delay = history_client
    error = RequestTimeout("test")
    method.side_effect = ([page("next", [row("a")])] if previous_page else []) + [error] * 5
    with pytest.raises(RequestTimeout) as caught:
        exchange.fetch_history(1000)
    assert caught.value is error
    assert method.call_count == 5 + int(previous_page)
    assert delay.call_count == 4


def test_authentication_failure_is_not_retried(history_client):
    """Permanent failures propagate immediately without redundant private requests."""
    exchange, method, delay = history_client
    error = AuthenticationError("test")
    method.side_effect = error
    with pytest.raises(AuthenticationError) as caught:
        exchange.fetch_history(1000)
    assert caught.value is error
    method.assert_called_once()
    delay.assert_not_called()
