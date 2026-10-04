"""Complete account-wide perpetual order reads using native CCXT endpoints.

Bybit: /v5/order/realtime (nextPageCursor).
OKX: /api/v5/trade/orders-pending (after the last ordId).
Bitget Classic: /api/v2/mix/order/orders-pending (idLessThan/endId).
Binance USD-M: /fapi/v1/openOrders (all symbols, no pagination).
CCXT parses the collected native rows without a limit that could truncate them.
"""

SERVICE = "Exchange"
MAX_ORDER_PAGES = 100


class OpenOrdersError(RuntimeError):
    """An incomplete or malformed account read must never become a snapshot."""


def _data(response, code_key, success_code, data_key):
    """Validate a native success envelope without exposing its contents."""
    if not isinstance(response, dict) or str(response.get(code_key)) != success_code:
        raise OpenOrdersError("Open orders request did not return a successful response")
    return response.get(data_key)


def _collect_pages(fetch_page, params, id_key, cursor_param):
    """Collect every page, deduplicate overlap and reject non-progressing cursors."""
    orders = {}
    seen_cursors = set()
    for _ in range(MAX_ORDER_PAGES):
        rows, cursor = fetch_page(dict(params))
        if not isinstance(rows, list) or len(rows) > params["limit"]:
            raise OpenOrdersError("Open orders returned a malformed page")
        for row in rows:
            if not isinstance(row, dict) or not row.get(id_key):
                raise OpenOrdersError("Open orders returned an invalid order ID")
            # Binance IDs are symbol-scoped; other endpoints use account-wide IDs.
            identity = (str(row.get("symbol") or row.get("instId") or ""), str(row[id_key]))
            orders[identity] = row
        if not cursor:
            return list(orders.values())
        if not rows or not isinstance(cursor, str) or cursor in seen_cursors:
            raise OpenOrdersError("Open orders pagination did not advance")
        seen_cursors.add(cursor)
        params[cursor_param] = cursor
    raise OpenOrdersError("Open orders pagination exceeded the safety limit")


def _bybit_orders(client, settle_coin):
    """Follow the explicit cursor even when a page contains fewer than 50 rows."""
    def fetch_page(params):
        """Read one validated linear open-order page."""
        data = _data(client.privateGetV5OrderRealtime(params), "retCode", "0", "result")
        if not isinstance(data, dict) or not isinstance(data.get("nextPageCursor"), str):
            raise OpenOrdersError("Open orders returned no pagination metadata")
        return data.get("list"), data["nextPageCursor"]

    return _collect_pages(fetch_page, {
        "category": "linear", "settleCoin": settle_coin, "openOnly": 0, "limit": 50,
    }, "orderId", "cursor")


def _okx_orders(client):
    """Read all SWAP order pages, moving backwards using the last native order ID."""
    def fetch_page(params):
        """Read one validated regular-order page, including partially filled orders."""
        rows = _data(client.privateGetTradeOrdersPending(params), "code", "0", "data")
        cursor = None
        if isinstance(rows, list) and len(rows) == params["limit"]:
            cursor = str(rows[-1].get("ordId") or "") if isinstance(rows[-1], dict) else None
        return rows, cursor

    return _collect_pages(fetch_page, {"instType": "SWAP", "limit": 100}, "ordId", "after")


def _bitget_orders(client, settle_coin, status):
    """Read Classic product pages for one open status using the native endId."""
    def fetch_page(params):
        """Read one validated Classic order page."""
        data = _data(client.privateMixGetV2MixOrderOrdersPending(params), "code", "00000", "data")
        if not isinstance(data, dict):
            raise OpenOrdersError("Open orders returned malformed data")
        rows = data.get("entrustedList")
        cursor = data.get("endId") if rows else None
        if rows and not cursor:
            raise OpenOrdersError("Open orders returned no pagination metadata")
        return rows, cursor

    return _collect_pages(fetch_page, {
        "productType": f"{settle_coin}-FUTURES", "status": status, "limit": 100,
    }, "orderId", "idLessThan")


def fetch_account_open_orders(client, exchange_id, settle_coins=("USDT", "USDC")):
    """Return complete regular perpetual orders for supported account readers.

Settlement filters come from the active positions, never from a market lookup.
Other exchanges and unsupported settlement assets fail closed so the caller can
use its existing per-symbol fallback. No partial result is returned on failure.
"""
    if exchange_id not in {"bybit", "okx", "binance", "bitget"}:
        raise OpenOrdersError("Account-wide order reads are not supported for this exchange")
    settle_coins = tuple(sorted(set(settle_coins)))
    if not settle_coins or not set(settle_coins) <= {"USDT", "USDC"}:
        raise OpenOrdersError("Account-wide order reads require a supported settlement asset")
    client.load_markets()
    if exchange_id == "bybit":
        rows = [row for coin in settle_coins for row in _bybit_orders(client, coin)]
    elif exchange_id == "okx":
        rows = _okx_orders(client)
    elif exchange_id == "bitget":
        # The native default only guarantees live orders; include partially filled
        # orders explicitly. An order may change status during the two reads.
        rows = [row for coin in settle_coins for status in ("live", "partially_filled")
                for row in _bitget_orders(client, coin, status)]
        rows = list({(str(row["symbol"]), str(row["orderId"])): row for row in rows}.values())
    else:
        rows = client.fapiPrivateGetOpenOrders({})
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
            raise OpenOrdersError("Open orders returned malformed data")
    orders = client.parse_orders(rows)
    if len(orders) != len(rows) or any(not order.get("symbol") for order in orders):
        raise OpenOrdersError("Open orders could not be completely normalized")
    return orders
