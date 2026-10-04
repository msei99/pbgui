"""Offline account pagination tests using real CCXT parsers and native page mocks."""

from types import SimpleNamespace
from unittest.mock import Mock

import ccxt
import pytest

from Exchange import Exchange
from api import dashboard
import exchange_open_orders as reader


def _native_order(exchange_id, order_id, symbol, side="buy", price=90, status="live"):
    """Build realistic regular-order fields understood by the installed CCXT parser."""
    coin, quote = symbol.split("/")
    quote = quote.split(":")[0]
    market_id = f"{coin}{quote}"
    if exchange_id == "okx":
        return {"ordId": str(order_id), "instId": f"{coin}-{quote}-SWAP", "instType": "SWAP",
                "state": status, "side": side, "px": str(price), "sz": "1", "accFillSz": "0",
                "ordType": "limit", "posSide": "long", "cTime": str(order_id + 1700000000000)}
    if exchange_id == "bybit":
        return {"orderId": str(order_id), "symbol": market_id, "orderStatus": "New",
                "side": side.title(), "price": str(price), "qty": "1", "cumExecQty": "0",
                "orderType": "Limit", "positionIdx": 1, "createdTime": str(order_id + 1700000000000)}
    if exchange_id == "bitget":
        return {"orderId": str(order_id), "symbol": market_id, "status": status,
                "side": side, "price": str(price), "size": "1", "baseVolume": "0",
                "orderType": "limit", "posSide": "long", "posMode": "hedge_mode",
                "tradeSide": "open" if side == "buy" else "close", "marginCoin": quote,
                "cTime": str(order_id + 1700000000000)}
    return {"orderId": order_id, "symbol": market_id, "status": "NEW",
            "side": side.upper(), "price": str(price), "origQty": "1", "executedQty": "0",
            "type": "LIMIT", "positionSide": "LONG", "time": order_id + 1700000000000}


@pytest.fixture(params=["bybit", "okx", "binance", "bitget"])
def account(request, monkeypatch):
    """Provide a real CCXT client with local markets and authenticated reads mocked."""
    exchange_id = request.param
    client = getattr(ccxt, exchange_id)()
    symbols = ["BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT", "BTC/USDC:USDC"]
    markets = []
    for symbol in symbols:
        coin, quote = symbol.split("/")
        quote = quote.split(":")[0]
        market_id = f"{coin}-{quote}-SWAP" if exchange_id == "okx" else f"{coin}{quote}"
        markets.append({"id": market_id, "symbol": symbol, "base": coin, "quote": quote,
                        "settle": quote, "type": "swap", "swap": True, "spot": False,
                        "linear": True, "inverse": False, "contract": True, "contractSize": 1})
    client.set_markets(markets)
    monkeypatch.setattr(client, "load_markets", lambda: client.markets)
    exchange = Exchange(exchange_id)
    exchange.instance = client
    if exchange_id == "bitget":
        monkeypatch.setattr(exchange, "ensure_bitget_account_mode", lambda: False)
    return exchange_id, exchange, client


def _install_pages(exchange_id, client, rows, monkeypatch):
    """Return deterministic native pages for the exact cursor sent by production code."""
    method_name = {"bybit": "privateGetV5OrderRealtime", "okx": "privateGetTradeOrdersPending",
                   "binance": "fapiPrivateGetOpenOrders", "bitget": "privateMixGetV2MixOrderOrdersPending"}[exchange_id]

    def respond(params):
        """Filter settlement/status, then emulate the native endpoint's cursor format."""
        if exchange_id == "binance":
            return rows
        filtered = rows
        if exchange_id in {"bybit", "bitget"}:
            quote = params.get("settleCoin") or params["productType"].split("-")[0]
            filtered = [row for row in rows if row["symbol"].endswith(quote)]
        if exchange_id == "bitget":
            filtered = [row for row in filtered if row["status"] == params["status"]]
        cursor = params.get({"bybit": "cursor", "okx": "after", "bitget": "idLessThan"}[exchange_id])
        id_key = "ordId" if exchange_id == "okx" else "orderId"
        start = 0 if not cursor else next(i + 1 for i, row in enumerate(filtered) if str(row[id_key]) == cursor)
        page = filtered[start:start + params["limit"]]
        end = str(page[-1][id_key]) if page else ""
        if exchange_id == "bybit":
            return {"retCode": 0, "result": {"list": page, "nextPageCursor": end if start + len(page) < len(filtered) else ""}}
        if exchange_id == "okx":
            return {"code": "0", "data": page}
        return {"code": "00000", "data": {"entrustedList": page, "endId": end}}

    method = Mock(side_effect=respond)
    monkeypatch.setattr(client, method_name, method)
    return method


def test_dashboard_classifies_orders_beyond_first_page(account, monkeypatch):
    """TP orders on later pages and no-order symbols remain correct without per-symbol calls."""
    exchange_id, exchange, client = account
    rows = [_native_order(exchange_id, 1000 - i, "BTC/USDT:USDT" if i % 2 == 0 else "ETH/USDT:USDT")
            for i in range(101)]
    rows += [_native_order(exchange_id, 899, "BTC/USDT:USDT", "sell", 120),
             _native_order(exchange_id, 898, "ETH/USDT:USDT", "sell", 130)]
    method = _install_pages(exchange_id, client, rows, monkeypatch)
    symbol_reader = Mock(side_effect=AssertionError("No per-symbol order read expected"))
    monkeypatch.setattr(client, "fetch_open_orders", symbol_reader)
    monkeypatch.setattr(exchange, "fetch_positions", lambda: [
        {"symbol": f"{coin}/USDT:USDT", "contracts": 1, "side": "long", "entryPrice": 100, "markPrice": 110}
        for coin in ("BTC", "ETH", "SOL")])
    monkeypatch.setattr(dashboard, "_get_exchange", lambda user: exchange)
    user = SimpleNamespace(name="test-account", exchange=exchange_id)
    positions = {row["symbol"]: row for row in dashboard._live_positions_for_user(user, Mock())}
    assert positions["BTCUSDT"]["next_tp"] == 120
    assert positions["ETHUSDT"]["next_tp"] == 130
    assert positions["BTCUSDT"]["dca"] == 51
    assert positions["ETHUSDT"]["dca"] == 50
    assert positions["SOLUSDT"]["dca"] == positions["SOLUSDT"]["next_tp"] == 0
    assert method.call_count == {"bybit": 3, "okx": 2, "binance": 1, "bitget": 4}[exchange_id]
    symbol_reader.assert_not_called()


def test_usdc_positions_and_partially_filled_orders_are_included(account, monkeypatch):
    """USDC order snapshots include partially filled regular orders on Classic Bitget."""
    exchange_id, exchange, client = account
    rows = [_native_order(exchange_id, 3, "BTC/USDC:USDC", status="partially_filled")]
    _install_pages(exchange_id, client, rows, monkeypatch)
    orders = exchange.fetch_all_open_orders(None, settle_coins=("USDC",))
    assert len(orders) == 1
    assert orders[0]["symbol"] == "BTC/USDC:USDC"
    assert orders[0]["price"] == 90


def test_per_symbol_exchange_reads_are_unchanged(account, monkeypatch):
    """Database sync and action reads keep the existing CCXT per-symbol path."""
    _, exchange, client = account
    method = Mock(return_value=[{"id": "one"}])
    monkeypatch.setattr(client, "fetch_open_orders", method)
    assert exchange.fetch_all_open_orders("BTC/USDT:USDT") == [{"id": "one"}]
    method.assert_called_once_with(symbol="BTC/USDT:USDT")


@pytest.mark.parametrize("account", ["bybit", "okx", "bitget"], indirect=True)
def test_dashboard_discards_snapshot_after_later_page_failure(account, monkeypatch):
    """A successful first page followed by failure uses complete symbol fallback data."""
    exchange_id, exchange, client = account
    rows = [_native_order(exchange_id, 1000 - i, "BTC/USDT:USDT") for i in range(101)]
    rows.append(_native_order(exchange_id, 899, "BTC/USDT:USDT", "sell", 120))
    method = _install_pages(exchange_id, client, rows, monkeypatch)
    original_response = method.side_effect

    def fail_second_page(params):
        """Allow the first read and fail only when advancing its native cursor."""
        if any(key in params for key in ("cursor", "after", "idLessThan")):
            raise OSError("temporary failure with sensitive response details")
        return original_response(params)

    method.side_effect = fail_second_page
    symbol_reader = Mock(return_value=client.parse_orders(rows))
    monkeypatch.setattr(client, "fetch_open_orders", symbol_reader)
    monkeypatch.setattr(exchange, "fetch_positions", lambda: [{
        "symbol": "BTC/USDT:USDT", "contracts": 1, "side": "long", "entryPrice": 100, "markPrice": 110,
    }])
    monkeypatch.setattr(dashboard, "_get_exchange", lambda user: exchange)
    log = Mock()
    monkeypatch.setattr(dashboard, "_log", log)
    user = SimpleNamespace(name="test-account", exchange=exchange_id)
    position = dashboard._live_positions_for_user(user, Mock())[0]
    assert position["dca"] == 101
    assert position["next_tp"] == 120
    assert method.call_count == 2
    symbol_reader.assert_called_once_with(symbol="BTC/USDT:USDT")
    log.assert_called_once()
    assert "sensitive response" not in str(log.call_args)


@pytest.mark.parametrize("account", ["bybit"], indirect=True)
def test_bybit_short_page_cursor_and_overlap_are_respected(account, monkeypatch):
    """An explicit cursor wins over short-page heuristics; overlap never doubles DCA counts."""
    exchange_id, exchange, client = account
    first = _native_order(exchange_id, 2, "BTC/USDT:USDT")
    last = _native_order(exchange_id, 1, "BTC/USDT:USDT", "sell", 120)
    method = Mock(side_effect=[
        {"retCode": 0, "result": {"list": [first], "nextPageCursor": "older"}},
        {"retCode": 0, "result": {"list": [first, last], "nextPageCursor": ""}},
    ])
    monkeypatch.setattr(client, "privateGetV5OrderRealtime", method)
    orders = exchange.fetch_all_open_orders(None, settle_coins=("USDT",))
    assert len(orders) == 2
    assert method.call_args_list[1].args[0]["cursor"] == "older"


@pytest.mark.parametrize("account", ["bybit", "okx", "bitget"], indirect=True)
@pytest.mark.parametrize("failure", ["request", "cycle", "missing_cursor", "malformed", "limit", "native_error"])
def test_failed_pagination_never_returns_partial_snapshot(account, monkeypatch, failure):
    """Failures after a successful first page cannot silently become complete results."""
    exchange_id, exchange, client = account
    size = 100 if exchange_id == "okx" else 1
    rows = [_native_order(exchange_id, 1000 - i, "BTC/USDT:USDT") for i in range(size)]
    if exchange_id == "bybit":
        method_name = "privateGetV5OrderRealtime"
        first = {"retCode": 0, "result": {"list": rows, "nextPageCursor": "older"}}
        second = {"retCode": 0, "result": {"list": rows, "nextPageCursor": "older"}}
        missing = {"retCode": 0, "result": {"list": rows}}
        malformed = {"retCode": 0, "result": {"list": None, "nextPageCursor": ""}}
        native_error = {"retCode": 10001}
    elif exchange_id == "okx":
        method_name = "privateGetTradeOrdersPending"
        first = {"code": "0", "data": rows}
        second = first
        missing = {"code": "0", "data": [dict(row, ordId="") for row in rows]}
        malformed = {"code": "0", "data": None}
        native_error = {"code": "50011"}
    else:
        method_name = "privateMixGetV2MixOrderOrdersPending"
        first = {"code": "00000", "data": {"entrustedList": rows, "endId": "1000"}}
        second = first
        missing = {"code": "00000", "data": {"entrustedList": rows}}
        malformed = {"code": "00000", "data": {"entrustedList": None}}
        native_error = {"code": "40084"}
    if failure == "request":
        second = OSError("transient read failure")
    elif failure == "missing_cursor":
        second = missing
    elif failure == "malformed":
        second = malformed
    elif failure == "native_error":
        second = native_error
    elif failure == "limit":
        monkeypatch.setattr(reader, "MAX_ORDER_PAGES", 1)
    method = Mock(side_effect=[first, second])
    monkeypatch.setattr(client, method_name, method)
    with pytest.raises((reader.OpenOrdersError, OSError)):
        exchange.fetch_all_open_orders(None, settle_coins=("USDT",))
