"""Read-only Bitget v3 USDT futures support, independent of CCXT's UTA parsers.

Schemas: bitget.com/legacy-docs/uta/{account/Get-Financial-Records,
account/Get-Account,trade/Get-Order-Fills,enum}. Ledger and fills retain 90
days; each query covers at most 30 days. Never infer profit from transfers.
"""

import hashlib
import json
import math
import re
import time

from ccxt.base.errors import NetworkError, RateLimitExceeded
from logging_helpers import human_log as _log

SERVICE = "BitgetUTA"
MODE_TTL_SECONDS = 300
DAY_MS = 86_400_000
CATEGORY = "USDT-FUTURES"


class BitgetUTAError(RuntimeError):
    """A sanitized failure that must not advance a history checkpoint."""


def is_classic_account_error(exc: Exception) -> bool:
    """Recognize only explicit native classic-account error codes, not prose."""
    text = str(exc)
    start = text.find("{")
    if start < 0:
        return False
    try:
        payload, _ = json.JSONDecoder().raw_decode(text[start:])
    except (ValueError, TypeError):
        return False
    return isinstance(payload, dict) and str(payload.get("code")) in {"40084", "25245"}


def _data(response):
    """Validate the native success envelope without exposing response content."""
    if not isinstance(response, dict) or response.get("code") != "00000":
        raise BitgetUTAError("Bitget UTA returned an unsuccessful response")
    data = response.get("data")
    if not isinstance(data, dict):
        raise BitgetUTAError("Bitget UTA returned malformed data")
    return data


def parse_account_settings(response) -> dict:
    """Return allowlisted settings; transitional/unknown modes fail closed.

    Native settings document unified/hybrid, not a successful classic value.
    Classic is recognized only by the two explicit native errors.
    """
    data = _data(response)
    if not isinstance(data.get("accountMode"), str) or data["accountMode"] not in {"unified", "hybrid"}:
        raise BitgetUTAError("Bitget account mode is unknown or transitioning")
    return {key: data[key] for key in ("accountMode", "uid", "holdMode", "accountLevel", "assetMode")
            if key in data and isinstance(data[key], str)}


def resolve_account_mode(client) -> dict:
    """Detect mode on a synchronous authenticated client; never guess Classic."""
    try:
        response = client.privateUtaGetV3AccountSettings()
    except Exception as exc:
        if is_classic_account_error(exc):
            return {"accountMode": "classic"}
        _log(SERVICE, "Account mode detection failed", level="ERROR")
        raise BitgetUTAError("Bitget account mode detection failed; check credentials and UTA read permission") from None
    try:
        return parse_account_settings(response)
    except BitgetUTAError:
        _log(SERVICE, "Account settings could not be resolved", level="ERROR")
        raise


def request(method, params=None):
    """Bounded retry for transient read failures with sanitized diagnostics."""
    for attempt in range(3):
        try:
            return method(params or {})
        except (NetworkError, RateLimitExceeded):
            if attempt < 2:
                _log(SERVICE, "Transient read failure; retrying", level="WARNING")
                time.sleep(2 ** attempt)
                continue
        except Exception:
            pass  # The fixed diagnostic below deliberately discards raw CCXT text.
        _log(SERVICE, "Private read failed; no partial data returned", level="ERROR")
        raise BitgetUTAError("Bitget UTA private read failed") from None


def number(value):
    """Require an explicit finite number, including zero and signed rebates."""
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise BitgetUTAError("Bitget UTA contains an invalid number")
    try:
        result = float(value)
    except (ValueError, OverflowError):
        raise BitgetUTAError("Bitget UTA contains an invalid number") from None
    if not math.isfinite(result):
        raise BitgetUTAError("Bitget UTA contains a non-finite number")
    return result


def _text(row, key):
    """Validate an identifier without including its value in errors."""
    value = row.get(key)
    if not isinstance(value, str) or not value or not re.fullmatch(r"[A-Za-z0-9_.:-]+", value):
        raise BitgetUTAError("Bitget UTA contains an invalid identifier")
    return value


def _timestamp(row, key):
    """Require an integral, positive millisecond timestamp."""
    value = number(row.get(key))
    if value <= 0 or not value.is_integer():
        raise BitgetUTAError("Bitget UTA contains an invalid timestamp")
    return int(value)


def parse_wallet_balance(response):
    """Read USDT wallet balance, not equity, free collateral or USD valuation."""
    data = _data(response)
    assets = data.get("assets")
    if not isinstance(assets, list):
        raise BitgetUTAError("Bitget UTA assets list is missing")
    coins = set()
    balance = 0.0
    for asset in assets:
        if not isinstance(asset, dict):
            raise BitgetUTAError("Bitget UTA asset is malformed")
        coin = _text(asset, "coin")
        if coin in coins:
            raise BitgetUTAError("Bitget UTA asset is duplicated")
        coins.add(coin)
        value = number(asset.get("balance"))
        if coin == "USDT":
            balance = value
    return balance


def fetch_balance(client):
    """Fetch native USDT wallet balance with strict asset-list validation."""
    return parse_wallet_balance(request(client.privateUtaGetV3AccountAssets))


def cursor_rows(method, params, id_field):
    """Fetch every native cursor page; abort on incomplete or cyclic paging."""
    rows, seen, cursors = [], {}, set()
    params = {**params, "limit": "100"}
    for _ in range(10000):
        data = _data(request(method, params))
        page = data.get("list")
        # Empty native fills can return explicit nulls instead of [] and "".
        if "list" in data and page is None and "cursor" in data and data["cursor"] in (None, ""):
            page = []
        if not isinstance(page, list) or len(page) > 100:
            raise BitgetUTAError("Bitget UTA page is malformed")
        for row in page:
            if not isinstance(row, dict):
                raise BitgetUTAError("Bitget UTA row is malformed")
            row_id = _text(row, id_field)
            if row_id in seen and seen[row_id] != row:
                raise BitgetUTAError("Bitget UTA returned conflicting duplicate records")
            if row_id not in seen:
                seen[row_id] = row
                rows.append(row)
        cursor = data.get("cursor")
        if cursor is not None and not isinstance(cursor, str):
            raise BitgetUTAError("Bitget UTA cursor is malformed")
        if not cursor:
            if len(page) == 100:
                raise BitgetUTAError("Bitget UTA full page has no continuation cursor")
            return rows
        if cursor in cursors:
            raise BitgetUTAError("Bitget UTA cursor repeated; history is incomplete")
        cursors.add(cursor)
        params = {**params, "cursor": cursor}
    raise BitgetUTAError("Bitget UTA pagination safety bound exceeded")


def history_rows(client, since, method, id_field):
    """Collect retained history in non-overlapping inclusive 30-day windows."""
    now = client.milliseconds()
    earliest = now - 90 * DAY_MS
    if since is None or since < earliest:
        _log(SERVICE, "Only the last 90 days of UTA history are available; older and pre-migration Classic history cannot be recovered by v3", level="WARNING")
    start = max(earliest, int(since) if since is not None else earliest)
    result = {}
    while start <= now:
        end = min(now, start + 30 * DAY_MS - 1)
        params = {"category": CATEGORY, "startTime": str(start), "endTime": str(end)}
        if id_field == "id":
            params["coin"] = "USDT"
        for row in cursor_rows(method, params, id_field):
            timestamp = _timestamp(row, "ts" if id_field == "id" else "createdTime")
            if not start <= timestamp <= end:
                raise BitgetUTAError("Bitget UTA record lies outside the requested window")
            row_id = _text(row, id_field)
            if row_id in result and result[row_id] != row:
                raise BitgetUTAError("Bitget UTA returned conflicting history records")
            result[row_id] = row
        start = end + 1
    return list(result.values())


# Explicit native financial-record enums. ORDER_DEALT_* are spot/margin
# principal flows, not futures PnL, and intentionally are not trade aliases.
TRADE_TYPES = frozenset({
    "OPEN_LONG", "OPEN_SHORT", "BUY_DEAL", "SELL_DEAL", "CLOSE_LONG", "CLOSE_SHORT",
    "FORCE_CLOSE_LONG", "FORCE_CLOSE_SHORT", "BURST_CLOSE_LONG", "BURST_CLOSE_SHORT",
    "OFFSET_REDUCE_CLOSE_LONG", "OFFSET_REDUCE_CLOSE_SHORT", "FORCE_BUY_SSM",
    "FORCE_SELL_SSM", "BURST_BUY_SSM", "BURST_SELL_SSM", "DELIVERY_LONG", "DELIVERY_SHORT",
})
FUNDING_TYPES = frozenset({
    "CONTRACT_MAIN_SETTLE_FEE_USER_IN", "CONTRACT_MAIN_SETTLE_FEE_USER_OUT",
    "MARGIN_SETTLE_FEE_USER_IN", "MARGIN_SETTLE_FEE_USER_OUT",
})
PRINCIPAL_TYPES = frozenset({
    "TRANSFER_IN", "TRANSFER_OUT", "RESERVE_TRANSFER_IN", "RESERVE_TRANSFER_OUT",
    "BORROW", "REPAYMENT", "INCREASE_MARGIN", "REDUCE_MARGIN", "MARGIN_BACK",
    "MARGIN_LEVER_ORDER_REFROZEN", "MARGIN_LEVER_ORDER_FROZEN", "MARGIN_LEVER_POS_IN",
    "ORDER_DEALT_FROZEN_OUT", "ORDER_DEALT_IN",
})


def parse_income(row, account_scope):
    """Normalize known futures ledger events; reject ambiguous principal/PnL."""
    if not isinstance(row, dict) or row.get("category") != CATEGORY or row.get("coin") != "USDT":
        raise BitgetUTAError("Bitget UTA income is not a USDT futures record")
    kind = _text(row, "type")
    timestamp = _timestamp(row, "ts")
    row_id = _text(row, "id")
    amount, fee = number(row.get("amount")), number(row.get("fee"))
    if row.get("feeCoin", "USDT") != "USDT":
        raise BitgetUTAError("Bitget UTA non-USDT fees cannot be converted")
    if kind in PRINCIPAL_TYPES:
        return None
    if kind in TRADE_TYPES:
        if row.get("positionType", "crossed") != "crossed":
            raise BitgetUTAError("Bitget UTA isolated ledger principal/PnL is unsupported")
        income = amount + fee  # Ledger fees are signed cash changes, unlike fills.
    elif kind in FUNDING_TYPES:
        income = (abs(amount) if kind.endswith("_IN") else -abs(amount)) + fee
    elif kind == "ORDER_PLF_FEE_OUT":
        # A separate token-fee bill carries the debit in amount, not a trade PnL.
        if fee != 0:
            raise BitgetUTAError("Bitget UTA separate fee bill has ambiguous fee fields")
        income = -abs(amount)
    else:
        raise BitgetUTAError("Bitget UTA financial record type is unsupported; history not imported")
    scope = hashlib.sha256(str(account_scope).encode("utf-8")).hexdigest()
    return {"symbol": _text(row, "symbol"), "timestamp": timestamp,
            "income": number(income), "uniqueid": f"bitget:uta:{scope}:{row_id}"}


def fetch_history(client, since, account_scope):
    """Return all normalized ledger income, or fail without a partial result."""
    rows = history_rows(client, since, client.privateUtaGetV3AccountFinancialRecords, "id")
    result = [parse_income(row, account_scope) for row in rows]
    return sorted((row for row in result if row is not None), key=lambda row: (row["timestamp"], row["uniqueid"]))


def _fee_cost(details, unfilled=False):
    """Sum native fee costs; only unfilled orders allow null fee placeholders."""
    if not isinstance(details, list):
        raise BitgetUTAError("Bitget UTA fee detail is missing")
    cost = 0.0
    for detail in details:
        if not isinstance(detail, dict):
            raise BitgetUTAError("Bitget UTA fee detail is malformed")
        if unfilled and "feeCoin" in detail and "fee" in detail and detail["feeCoin"] is None and detail["fee"] is None:
            continue
        if detail.get("feeCoin") != "USDT":
            raise BitgetUTAError("Bitget UTA non-USDT fees cannot be converted")
        cost += number(detail.get("fee"))
    return number(cost)


def parse_execution(row):
    """Normalize native fills with true execution sides and positive fee costs."""
    if not isinstance(row, dict) or row.get("category") != CATEGORY:
        raise BitgetUTAError("Bitget UTA fill is not a USDT futures record")
    side = row.get("side")
    if side not in ("buy", "sell"):
        raise BitgetUTAError("Bitget UTA fill side is invalid")
    price, qty = number(row.get("execPrice")), number(row.get("execQty"))
    if price <= 0 or qty <= 0:
        raise BitgetUTAError("Bitget UTA fill price or quantity is invalid")
    fee = _fee_cost(row.get("feeDetail"))
    result = {"symbol": _text(row, "symbol"), "timestamp": _timestamp(row, "createdTime"),
              "side": side, "price": price, "qty": qty, "fee": number(fee),
              "realized_pnl": number(row.get("execPnl")), "order_id": _text(row, "orderId"),
              "trade_id": "bitget:uta:" + _text(row, "execId")}
    try:
        result["raw_json"] = json.dumps({"info": row, "pbgui_account_mode": "uta",
                                         "side": side, "fee": {"cost": fee, "currency": "USDT"}}, allow_nan=False)
    except (ValueError, TypeError):
        raise BitgetUTAError("Bitget UTA fill cannot be serialized safely") from None
    return result


def fetch_executions(client, since):
    """Fetch account-wide native fills, including symbols absent from income."""
    rows = history_rows(client, since, client.privateUtaGetV3TradeFills, "execId")
    return sorted((parse_execution(row) for row in rows), key=lambda row: (row["timestamp"], row["trade_id"]))


def _perpetual_market(client, symbol):
    """Resolve a native ID from loaded metadata; skip only known non-perpetuals.

    CCXT safe_market can fabricate unresolved markets. Never use that fallback
    when a full snapshot may delete previously persisted positions or orders.
    """
    markets_by_id = getattr(client, "markets_by_id", None)
    candidates = markets_by_id.get(symbol) if isinstance(markets_by_id, dict) else None
    if not isinstance(candidates, list) or not candidates:
        raise BitgetUTAError("Bitget UTA market is unresolved")
    matches = []
    known_contract = False
    for market in candidates:
        if not isinstance(market, dict) or market.get("id") != symbol:
            raise BitgetUTAError("Bitget UTA market metadata is malformed")
        if market.get("spot") is True:
            continue
        if market.get("future") is True and market.get("swap") is False:
            known_contract = True
            continue
        if market.get("swap") is not True:
            raise BitgetUTAError("Bitget UTA market type is unresolved")
        known_contract = True
        if market.get("linear") is False or market.get("quote") not in (None, "USDT") or market.get("settle") not in (None, "USDT"):
            continue
        if market.get("linear") is not True or market.get("quote") != "USDT" or market.get("settle") != "USDT" or not market.get("symbol"):
            raise BitgetUTAError("Bitget UTA perpetual market metadata is incomplete")
        if number(market.get("contractSize")) <= 0:
            raise BitgetUTAError("Bitget UTA market contract size is invalid")
        matches.append(market)
    if len(matches) > 1:
        raise BitgetUTAError("Bitget UTA perpetual market is ambiguous")
    if not known_contract:
        raise BitgetUTAError("Bitget UTA contract market is unresolved")
    return matches[0] if matches else None


def fetch_open_orders(client, symbol):
    """Read every open-order cursor page and use CCXT only for normalization."""
    request(lambda params: client.load_markets())
    params = {"category": CATEGORY}
    market = None
    if symbol:
        requested_market = client.market(symbol)
        market = _perpetual_market(client, requested_market["id"])
        if market is None or market["symbol"] != requested_market["symbol"]:
            raise BitgetUTAError("Bitget UTA requires a USDT linear perpetual market")
        params["symbol"] = market["id"]
    rows = cursor_rows(client.privateUtaGetV3TradeUnfilledOrders, params, "orderId")
    result = []
    for row in rows:
        if row.get("category") != CATEGORY:
            raise BitgetUTAError("Bitget UTA open order is malformed")
        row_market = _perpetual_market(client, _text(row, "symbol"))
        if row_market is None:
            continue
        if market is not None and market["symbol"] != row_market["symbol"]:
            raise BitgetUTAError("Bitget UTA open order does not match the requested market")
        if row.get("side") not in ("buy", "sell"):
            raise BitgetUTAError("Bitget UTA open order side is invalid")
        if number(row.get("qty")) <= 0 or number(row.get("price")) < 0:
            raise BitgetUTAError("Bitget UTA open order price or quantity is invalid")
        filled = number(row.get("cumExecQty"))
        if not 0 <= filled <= number(row["qty"]):
            raise BitgetUTAError("Bitget UTA open order filled quantity is invalid")
        fee = _fee_cost(row.get("feeDetail"), unfilled=filled == 0)
        order = client.parse_order(row, row_market)
        amount = number(order.get("amount"))
        normalized_filled = number(order.get("filled"))
        remaining = number(order.get("remaining"))
        if amount <= 0 or not 0 <= normalized_filled <= amount or not 0 <= remaining <= amount:
            raise BitgetUTAError("Bitget UTA normalized order amount is invalid")
        if number(order.get("price")) < 0:
            raise BitgetUTAError("Bitget UTA normalized order price is invalid")
        order["side"] = row["side"]
        order["fee"] = {"cost": fee, "currency": "USDT"}
        order["fees"] = [order["fee"]]
        order["pbgui_account_mode"] = "uta"
        result.append(order)
    return result


def fetch_positions(client):
    """Validate native position data before using CCXT's position parser."""
    request(lambda params: client.load_markets())
    data = _data(request(client.privateUtaGetV3PositionCurrentPosition, {"category": CATEGORY}))
    rows = data.get("list")
    # Bitget returns {"list": null} for an account without positions.
    if "list" in data and rows is None:
        rows = []
    if not isinstance(rows, list):
        raise BitgetUTAError("Bitget UTA positions list is missing")
    result = []
    for row in rows:
        if not isinstance(row, dict) or row.get("category") != CATEGORY or row.get("marginCoin") != "USDT":
            raise BitgetUTAError("Bitget UTA position is malformed")
        market = _perpetual_market(client, _text(row, "symbol"))
        if market is None:
            continue
        side = row.get("posSide") or row.get("holdSide")
        if side not in ("long", "short"):
            raise BitgetUTAError("Bitget UTA position side is invalid")
        for key in ("total", "avgPrice", "unrealisedPnl"):
            number(row.get(key))
        if number(row["total"]) < 0 or number(row["avgPrice"]) < 0:
            raise BitgetUTAError("Bitget UTA position quantity or entry price is invalid")
        for key in ("positionBalance", "markPrice", "liquidationPrice", "leverage", "curRealisedPnl", "mmr"):
            if key in row:
                number(row[key])
        position = client.parse_position(row, market)
        contracts = number(position.get("contracts"))
        contract_size = number(position.get("contractSize"))
        if contracts < 0 or contract_size <= 0 or position.get("symbol") != market["symbol"]:
            raise BitgetUTAError("Bitget UTA normalized position is invalid")
        number(contracts * contract_size)
        position["side"] = side
        position["pbgui_account_mode"] = "uta"
        result.append(position)
    return result
