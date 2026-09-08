"""Isolated Bitget UTA runtime tests; no credentials, network or runtime files."""

import asyncio
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from ccxt.base.errors import AuthenticationError, ExchangeError, RequestTimeout

import bitget_uta as uta
import Exchange as exchange_module
from Exchange import Exchange


@pytest.fixture(autouse=True)
def isolated_logs(monkeypatch):
    """Keep diagnostic writes and retry sleeps away from the real workspace."""
    monkeypatch.setattr(uta, "_log", Mock())
    monkeypatch.setattr(exchange_module, "_log", Mock())
    monkeypatch.setattr(uta.time, "sleep", Mock())


def response(data):
    """Build a native success envelope."""
    return {"code": "00000", "data": data}


def ledger(record_id="1", **changes):
    """Build a documented cross-margin financial event."""
    return {"id": record_id, "category": uta.CATEGORY, "coin": "USDT", "symbol": "BTCUSDT",
            "type": "CLOSE_LONG", "amount": "4", "fee": "-0.1", "ts": "1000",
            "positionType": "crossed", **changes}


def fill(record_id="1", **changes):
    """Build a native UTA fill with positive fee expense."""
    return {"execId": record_id, "orderId": "order1", "category": uta.CATEGORY,
            "symbol": "BTCUSDT", "side": "sell", "execPrice": "100", "execQty": "2",
            "execPnl": "4", "feeDetail": [{"feeCoin": "USDT", "fee": "0.1"}],
            "createdTime": "1000", **changes}


def user():
    """Create a value-only fake user, never load the credential store."""
    return SimpleNamespace(name="test-user", exchange="bitget", key="fake-key",
                           secret="fake-secret", passphrase="fake-pass")


@pytest.mark.parametrize("mode", ["unified", "hybrid"])
def test_settings_modes(mode):
    """Both documented active modes use UTA; arbitrary payload keys are removed."""
    client = Mock()
    client.privateUtaGetV3AccountSettings.return_value = response({"accountMode": mode, "uid": "123", "secret": "hidden"})
    assert uta.resolve_account_mode(client) == {"accountMode": mode, "uid": "123"}


@pytest.mark.parametrize("code", ["40084", "25245"])
def test_classic_errors(code):
    """Only the native explicit unsupported-account codes choose Classic."""
    client = Mock()
    client.privateUtaGetV3AccountSettings.side_effect = ExchangeError('bitget ' + json.dumps({"code": code, "msg": "hidden"}))
    assert uta.resolve_account_mode(client) == {"accountMode": "classic"}


@pytest.mark.parametrize("payload", [None, {}, {"data": {}}, response({}), response({"accountMode": "classic"}),
                                     response({"accountMode": "switching"}), response({"accountMode": "upgrading"}),
                                     response({"accountMode": "unknown"}), {"code": "40009", "data": {"accountMode": "unified"}}])
def test_settings_fail_closed(payload):
    """Missing, undocumented and transitional settings never route Classic."""
    with pytest.raises(uta.BitgetUTAError):
        uta.parse_account_settings(payload)


@pytest.mark.parametrize("exc", [AuthenticationError('secret 40084'), RequestTimeout('hidden'),
                                 ExchangeError('bitget {"code":"40009","msg":"secret 25245"}')])
def test_detection_sanitized(exc):
    """Secrets and numeric substrings cannot select Classic or escape errors."""
    client = Mock()
    client.privateUtaGetV3AccountSettings.side_effect = exc
    with pytest.raises(uta.BitgetUTAError) as caught:
        uta.resolve_account_mode(client)
    assert "secret" not in str(caught.value)
    assert "hidden" not in str(caught.value)
    assert caught.value.__suppress_context__


@pytest.mark.parametrize("assets,expected", [([], 0), ([{"coin": "BTC", "balance": "2"}], 0),
    ([{"coin": "USDT", "balance": "12", "equity": "99", "available": "3"}], 12)])
def test_wallet_balance(assets, expected):
    """Only wallet balance contributes; valid absent USDT means genuine zero."""
    assert uta.parse_wallet_balance(response({"assets": assets})) == expected


@pytest.mark.parametrize("data", [{}, {"assets": None}, {"assets": [None]},
    {"assets": [{"coin": "USDT", "equity": "1"}]},
    {"assets": [{"coin": "USDT", "balance": "NaN"}]},
    {"assets": [{"coin": "USDT", "balance": "1"}] * 2}])
def test_wallet_rejects_malformed(data):
    """Malformed assets never masquerade as a zero wallet."""
    with pytest.raises(uta.BitgetUTAError):
        uta.parse_wallet_balance(response(data))


def test_cursor_same_timestamp_dedup():
    """More than 100 same-timestamp events survive pagination with ID dedup."""
    first = [ledger(str(i)) for i in range(100)]
    second = [first[-1], *[ledger(str(i)) for i in range(100, 125)]]
    method = Mock(side_effect=[response({"list": first, "cursor": "next"}),
                               response({"list": second, "cursor": "last"}), response({"list": [], "cursor": ""})])
    rows = uta.cursor_rows(method, {"category": uta.CATEGORY}, "id")
    assert len(rows) == 125
    assert method.call_args_list[1].args[0]["cursor"] == "next"
    assert method.call_args_list[2].args[0]["cursor"] == "last"


@pytest.mark.parametrize("second", [response({"list": [ledger()], "cursor": "next"}),
    response({"list": [ledger(amount="5")], "cursor": ""}),
    response({"list": None}), AuthenticationError("hidden")])
def test_cursor_aborts_partial_failure(second):
    """Cycles, conflicting duplicates, malformed pages and failures abort all."""
    method = Mock(side_effect=[response({"list": [ledger()], "cursor": "next"}), second])
    with pytest.raises(uta.BitgetUTAError):
        uta.cursor_rows(method, {}, "id")


def test_full_page_requires_cursor():
    """A missing continuation on a full page must not silently truncate."""
    method = Mock(return_value=response({"list": [ledger(str(i)) for i in range(100)]}))
    with pytest.raises(uta.BitgetUTAError, match="continuation"):
        uta.cursor_rows(method, {}, "id")


@pytest.mark.parametrize("cursor", [None, ""])
def test_native_null_fill_page_is_empty(cursor):
    """Replay Bitget's successful empty-fill shape without weakening envelopes."""
    client = Mock()
    client.milliseconds.return_value = 2000
    client.privateUtaGetV3TradeFills.return_value = response({"list": None, "cursor": cursor})
    assert uta.fetch_executions(client, 1000) == []
    client.privateUtaGetV3TradeFills.assert_called_once()


def test_native_null_terminal_page_preserves_previous_records():
    """An explicit empty terminal page completes rather than discards prior data."""
    method = Mock(side_effect=[response({"list": [fill()], "cursor": "next"}),
                               response({"list": None, "cursor": None})])
    assert uta.cursor_rows(method, {}, "execId") == [fill()]


@pytest.mark.parametrize("payload", [
    response({"cursor": None}), response({"list": None}),
    response({"list": None, "cursor": "next"}),
    response({"list": None, "cursor": False}),
    {"code": "40014", "data": {"list": None, "cursor": None}},
])
def test_null_page_does_not_hide_missing_fields_or_errors(payload):
    """Only a successful explicit empty terminal response is accepted as empty."""
    with pytest.raises(uta.BitgetUTAError):
        uta.cursor_rows(Mock(return_value=payload), {}, "execId")


def test_windows_retention_and_boundaries():
    """Clamp to 90 days and cover exact inclusive boundaries without gaps."""
    now = 100 * uta.DAY_MS
    client = Mock()
    client.milliseconds.return_value = now
    method = Mock(return_value=response({"list": [], "cursor": ""}))
    assert uta.history_rows(client, 0, method, "execId") == []
    windows = [call.args[0] for call in method.call_args_list]
    assert int(windows[0]["startTime"]) == 10 * uta.DAY_MS
    assert int(windows[-1]["endTime"]) == now
    for previous, following in zip(windows, windows[1:]):
        assert int(previous["endTime"]) + 1 == int(following["startTime"])
    assert all(int(w["endTime"]) - int(w["startTime"]) < 30 * uta.DAY_MS for w in windows)
    assert all("cursor" not in w and "symbol" not in w for w in windows)
    uta._log.assert_called_once()


@pytest.mark.parametrize("kind,amount,fee,expected", [
    ("CLOSE_LONG", "4", "-0.1", 3.9), ("OPEN_SHORT", "0", "-0.1", -0.1),
    ("BUY_DEAL", "1", "0.1", 1.1), ("CONTRACT_MAIN_SETTLE_FEE_USER_IN", "2", "0", 2),
    ("CONTRACT_MAIN_SETTLE_FEE_USER_OUT", "2", "0", -2),
    ("MARGIN_SETTLE_FEE_USER_OUT", "-2", "0", -2), ("ORDER_PLF_FEE_OUT", "0.2", "0", -0.2)])
def test_income_signs(kind, amount, fee, expected):
    """Trade cash changes include fees/rebates; funding has explicit direction."""
    assert uta.parse_income(ledger(type=kind, amount=amount, fee=fee), "123")["income"] == pytest.approx(expected)


@pytest.mark.parametrize("kind", ["TRANSFER_IN", "TRANSFER_OUT", "INCREASE_MARGIN", "BORROW", "ORDER_DEALT_IN"])
def test_no_principal_income(kind):
    """Transfers and spot-like dealt principal are never treated as futures PnL."""
    assert uta.parse_income(ledger(type=kind, amount="10000"), "123") is None


@pytest.mark.parametrize("changes", [{"type": "NEW_TYPE"}, {"type": "FIXED_CLOSE_LONG"},
    {"amount": "NaN"}, {"fee": "inf"}, {"coin": "BTC"}, {"feeCoin": "BGB"},
    {"category": "MARGIN"}, {"ts": "1.5"}, {"positionType": "isolated"}])
def test_income_rejects_uncertain_values(changes):
    """Unknown types, non-USDT accounting and principal ambiguity fail closed."""
    with pytest.raises(uta.BitgetUTAError):
        uta.parse_income(ledger(**changes), "123")


def test_scoped_income_ids():
    """Identical native IDs differ across accounts and from Classic IDs."""
    first = uta.parse_income(ledger(), "123")
    assert first["uniqueid"] != uta.parse_income(ledger(), "456")["uniqueid"]
    assert first == uta.parse_income(ledger(), "123")
    assert first["uniqueid"].startswith("bitget:uta:")


@pytest.mark.parametrize("side", ["buy", "sell"])
@pytest.mark.parametrize("fee", ["0.2", "-0.2", "0"])
def test_fill_sides_fees_provenance(side, fee):
    """UTA never applies Classic hedge-close inversion or fee negation."""
    native = fill(side=side, tradeSide="close", holdMode="hedge_mode", feeDetail=[{"feeCoin": "USDT", "fee": fee}])
    result = uta.parse_execution(native)
    assert result["side"] == side
    assert result["fee"] == float(fee)
    assert result["realized_pnl"] == 4
    raw = json.loads(result["raw_json"])
    assert raw["info"] == native
    assert raw["pbgui_account_mode"] == "uta"


@pytest.mark.parametrize("changes", [{"execPnl": None}, {"execPrice": "NaN"}, {"execQty": "0"},
    {"feeDetail": None}, {"feeDetail": [{"feeCoin": "BGB", "fee": "0.2"}]},
    {"feeDetail": [{"feeCoin": "USDT", "fee": "inf"}]}, {"side": "close_long"}])
def test_fill_invalid(changes):
    """Malformed executions and unconvertible fees abort normalization."""
    with pytest.raises(uta.BitgetUTAError):
        uta.parse_execution(fill(**changes))


def test_account_wide_executions():
    """Executions do not require symbols discovered from income or market scans."""
    client = Mock()
    client.milliseconds.return_value = 2000
    client.privateUtaGetV3TradeFills.return_value = response({"list": [fill(symbol="NEWUSDT")], "cursor": ""})
    assert uta.fetch_executions(client, 1000)[0]["symbol"] == "NEWUSDT"
    assert "symbol" not in client.privateUtaGetV3TradeFills.call_args.args[0]
    client.load_markets.assert_not_called()


def test_transient_retry_only():
    """Read retries are bounded and never retry auth failures."""
    method = Mock(side_effect=[RequestTimeout("hidden"), response({})])
    assert uta.request(method) == response({})
    assert method.call_count == 2
    method = Mock(side_effect=AuthenticationError("hidden"))
    with pytest.raises(uta.BitgetUTAError):
        uta.request(method)
    assert method.call_count == 1


def test_lazy_mode_refresh_and_credentials(monkeypatch):
    """Public connect is private-call-free; private mode has a bounded lifetime."""
    client = Mock(options={})
    client.privateUtaGetV3AccountSettings.return_value = response({"accountMode": "hybrid", "uid": "123"})
    monkeypatch.setattr(exchange_module.ccxt, "bitget", Mock(return_value=client))
    ex = Exchange("bitget", user())
    ex.connect()
    client.privateUtaGetV3AccountSettings.assert_not_called()
    assert ex._bitget_uta is None
    assert ex.ensure_bitget_account_mode() is True
    assert client.options["uta"] is True
    ex.ensure_bitget_account_mode()
    assert client.privateUtaGetV3AccountSettings.call_count == 1
    ex._bitget_mode_checked -= uta.MODE_TTL_SECONDS
    ex.ensure_bitget_account_mode()
    assert client.privateUtaGetV3AccountSettings.call_count == 2
    ex.user.secret = "replacement"
    ex.ensure_bitget_account_mode()
    assert client.privateUtaGetV3AccountSettings.call_count == 3
    assert client.secret == "replacement"
    client.close.assert_called_once()


def test_classic_balance_preserved(monkeypatch):
    """The existing Classic available-balance result is unchanged."""
    client = Mock(options={})
    client.privateUtaGetV3AccountSettings.side_effect = ExchangeError('bitget {"code":"40084"}')
    client.fetch_balance.return_value = {"info": [{"available": "42"}]}
    ex = Exchange("bitget", user())
    ex.instance = client
    assert ex.fetch_balance("swap") == 42
    assert client.options["uta"] is False


@pytest.fixture
def private_cache(monkeypatch):
    """Replace every private cache/lock registry without touching real clients."""
    for name in ("_private_ws_clients", "_private_ws_owners", "_private_ws_locks", "_private_creation_locks"):
        monkeypatch.setattr(Exchange, name, {})
    monkeypatch.setattr(exchange_module, "_CREATION_INFLIGHT", set())
    monkeypatch.setattr(exchange_module, "_notify_private_client_closed", Mock())


@pytest.mark.parametrize("mode", ["classic", "unified", "hybrid"])
def test_bitget_private_ws_uses_rest_without_allocation(monkeypatch, private_cache, mode):
    """All Bitget account modes opt out before private probing or allocation."""
    client = SimpleNamespace(privateUtaGetV3AccountSettings=AsyncMock(return_value=response({"accountMode": mode})))
    factory = Mock(return_value=client)
    monkeypatch.setattr(exchange_module.ccxt_pro, "bitget", factory)
    account = user()
    assert asyncio.run(Exchange.get_private_ws_client("bitget", account, "dashboard_orders")) is None
    account.secret = "changed"
    assert asyncio.run(Exchange.get_private_ws_client("bitget", account, "live_session.balance")) is None
    factory.assert_not_called()
    client.privateUtaGetV3AccountSettings.assert_not_awaited()
    assert not Exchange._private_ws_clients
    assert not Exchange._private_ws_owners
    assert not Exchange._private_ws_locks
    assert not Exchange._private_creation_locks
    assert not exchange_module._CREATION_INFLIGHT


@pytest.fixture
def parser_client(monkeypatch):
    """Use installed CCXT parsers with markets supplied entirely in memory."""
    client = exchange_module.ccxt.bitget()
    client.set_markets([{"id": "BTCUSDT", "symbol": "BTC/USDT:USDT", "base": "BTC", "quote": "USDT",
                         "settle": "USDT", "type": "swap", "swap": True, "spot": False,
                         "future": False, "contract": True, "linear": True, "inverse": False,
                         "contractSize": 1, "precision": {"amount": 0.001, "price": 0.1}}])
    monkeypatch.setattr(client, "load_markets", Mock(return_value=client.markets))
    return client


def test_open_orders_native_pages(parser_client, monkeypatch):
    """Every order page uses native cursors, with true UTA close sides."""
    native = {"category": uta.CATEGORY, "symbol": "BTCUSDT", "orderId": "1", "qty": "1",
              "price": "100", "side": "sell", "posSide": "long", "holdMode": "hedge_mode",
              "tradeSide": "close", "orderType": "limit", "orderStatus": "live", "reduceOnly": "NO",
              "cumExecQty": "0", "createdTime": "1000", "feeDetail": [{"feeCoin": None, "fee": None}]}
    first = [{**native, "orderId": str(i)} for i in range(100)]
    second = [{**native, "orderId": str(i)} for i in range(100, 140)]
    method = Mock(side_effect=[response({"list": first, "cursor": "100"}), response({"list": second, "cursor": ""})])
    monkeypatch.setattr(parser_client, "privateUtaGetV3TradeUnfilledOrders", method)
    orders = uta.fetch_open_orders(parser_client, None)
    assert len(orders) == 140
    assert all(order["side"] == "sell" and order["symbol"] == "BTC/USDT:USDT" for order in orders)
    assert method.call_args_list[1].args[0]["cursor"] == "100"
    parser_client.load_markets.assert_called_once()


@pytest.mark.parametrize("side_key,side", [("posSide", "long"), ("holdSide", "short")])
def test_native_position_parser(parser_client, monkeypatch, side_key, side):
    """Installed CCXT position parsing preserves native hedge direction."""
    native = {"category": uta.CATEGORY, "symbol": "BTCUSDT", "marginCoin": "USDT", side_key: side,
              "total": "2", "avgPrice": "100", "unrealisedPnl": "3", "markPrice": "101.5",
              "positionBalance": "10", "leverage": "20", "marginMode": "crossed", "holdMode": "hedge_mode",
              "createdTime": "1000"}
    method = Mock(return_value=response({"list": [native]}))
    monkeypatch.setattr(parser_client, "privateUtaGetV3PositionCurrentPosition", method)
    positions = uta.fetch_positions(parser_client)
    assert positions[0]["side"] == side
    assert positions[0]["contracts"] == 2
    assert positions[0]["entryPrice"] == 100
    assert positions[0]["unrealizedPnl"] == 3
    method.assert_called_once_with({"category": uta.CATEGORY})


def test_native_null_positions_are_empty(parser_client, monkeypatch):
    """Replay the successful no-position shape observed in read-only acceptance."""
    method = Mock(return_value=response({"list": None}))
    monkeypatch.setattr(parser_client, "privateUtaGetV3PositionCurrentPosition", method)
    assert uta.fetch_positions(parser_client) == []


@pytest.mark.parametrize("payload", [response({}), {"code": "40014", "data": {"list": None}}])
def test_missing_or_failed_positions_are_not_empty(parser_client, monkeypatch, payload):
    """Missing position lists and permission failures cannot clear saved state."""
    monkeypatch.setattr(parser_client, "privateUtaGetV3PositionCurrentPosition", Mock(return_value=payload))
    with pytest.raises(uta.BitgetUTAError):
        uta.fetch_positions(parser_client)


def test_exchange_private_routing_and_public_independence(monkeypatch):
    """Public quotes never probe settings; each private wrapper routes to v3."""
    client = Mock(options={})
    client.privateUtaGetV3AccountSettings.return_value = response({"accountMode": "unified", "uid": "123"})
    ex = Exchange("bitget", user())
    ex.instance = client
    ex.fetch_price("BTC/USDT:USDT", "swap")
    client.privateUtaGetV3AccountSettings.assert_not_called()
    for name, args in [("fetch_balance", ("swap",)), ("fetch_positions", ()),
                       ("fetch_all_open_orders", (None,)), ("fetch_history", (1000,)), ("fetch_executions", (1000, []))]:
        helper = {"fetch_all_open_orders": "fetch_open_orders"}.get(name, name)
        operation = Mock(return_value=[])
        monkeypatch.setattr(uta, helper, operation)
        assert getattr(ex, name)(*args) == []
        assert operation.call_args.args[0] is client
    assert client.options["uta"] is True
    assert client.privateUtaGetV3AccountSettings.call_count == 1


def test_history_normalization_failure_is_atomic():
    """One unknown event prevents returning earlier valid events to persistence."""
    client = Mock()
    client.milliseconds.return_value = 2000
    client.privateUtaGetV3AccountFinancialRecords.return_value = response({"list": [ledger(), ledger("2", type="UNSUPPORTED")], "cursor": ""})
    with pytest.raises(uta.BitgetUTAError):
        uta.fetch_history(client, 1000, "123")


def test_history_second_window_failure():
    """A later window failure cannot advance a checkpoint past partial history."""
    client = Mock()
    client.milliseconds.return_value = 100 * uta.DAY_MS
    client.privateUtaGetV3AccountFinancialRecords.side_effect = [
        response({"list": [ledger(ts=str(10 * uta.DAY_MS))], "cursor": ""}), AuthenticationError("hidden")]
    with pytest.raises(uta.BitgetUTAError):
        uta.fetch_history(client, 0, "123")


def test_mode_failure_after_refresh_does_not_reuse_stale_route():
    """A formerly unified account must fail closed when re-detection fails."""
    client = Mock(options={})
    client.privateUtaGetV3AccountSettings.side_effect = [response({"accountMode": "unified"}), RequestTimeout("hidden")]
    ex = Exchange("bitget", user())
    ex.instance = client
    assert ex.ensure_bitget_account_mode()
    ex._bitget_mode_checked -= uta.MODE_TTL_SECONDS
    with pytest.raises(uta.BitgetUTAError):
        ex.fetch_balance("swap")
    assert ex._bitget_uta is None
    client.privateUtaGetV3AccountAssets.assert_not_called()


def test_private_ws_caps(monkeypatch, private_cache):
    """Bitget REST fallback does not allocate even when WS capacity is zero."""
    monkeypatch.setattr(exchange_module, "_RUNTIME_MAX_PRIVATE_WS_GLOBAL", 0)
    factory = Mock()
    monkeypatch.setattr(exchange_module.ccxt_pro, "bitget", factory)
    assert asyncio.run(Exchange.get_private_ws_client("bitget", user())) is None
    factory.assert_not_called()


@pytest.mark.parametrize("fee,expected", [("0.2", 0.2), ("-0.1", -0.1)])
def test_open_order_fee_cost(parser_client, monkeypatch, fee, expected):
    """Partial orders correct CCXT 4.5.38's native UTA fee sign inversion."""
    row = {"category": uta.CATEGORY, "symbol": "BTCUSDT", "orderId": "1", "qty": "2",
           "price": "100", "side": "buy", "posSide": "short", "orderType": "limit",
           "orderStatus": "partially_filled", "cumExecQty": "1",
           "feeDetail": [{"feeCoin": "USDT", "fee": fee}]}
    monkeypatch.setattr(parser_client, "privateUtaGetV3TradeUnfilledOrders", Mock(return_value=response({"list": [row], "cursor": ""})))
    order = uta.fetch_open_orders(parser_client, "BTC/USDT:USDT")[0]
    assert order["fee"] == {"cost": expected, "currency": "USDT"}


def test_classic_rest_data_after_ws_opt_out(monkeypatch, private_cache):
    """Classic callers retain correct REST results after private WS is declined."""
    account = user()
    assert asyncio.run(Exchange.get_private_ws_client("bitget", account)) is None
    client = Mock(options={})
    client.privateUtaGetV3AccountSettings.side_effect = ExchangeError('bitget {"code":"40084"}')
    client.fetch_balance.return_value = {"info": [{"available": "42"}]}
    client.fetch_positions.return_value = [{"symbol": "BTC/USDT:USDT", "contracts": 2}]
    client.fetch_open_orders.return_value = [{"id": "1", "amount": 1}]
    ex = Exchange("bitget", account)
    ex.instance = client
    assert ex.fetch_balance("swap") == 42
    assert ex.fetch_positions() == client.fetch_positions.return_value
    assert ex.fetch_all_open_orders("BTC/USDT:USDT") == client.fetch_open_orders.return_value
    assert client.options["uta"] is False
    client.privateUtaGetV3AccountSettings.assert_called_once()


def position_row(**changes):
    """Return one native perpetual position without loading account data."""
    return {"category": uta.CATEGORY, "symbol": "BTCUSDT", "marginCoin": "USDT", "posSide": "long",
            "total": "2", "avgPrice": "100", "unrealisedPnl": "3", "markPrice": "101.5",
            "positionBalance": "10", "leverage": "20", "marginMode": "crossed", "holdMode": "hedge_mode",
            "createdTime": "1000", **changes}


@pytest.mark.parametrize("kind", ["positions", "orders"])
def test_snapshot_skips_known_delivery(parser_client, monkeypatch, kind):
    """Account-wide snapshots skip known dated contracts without hiding perps."""
    delivery = {**parser_client.markets["BTC/USDT:USDT"], "id": "BTCUSDT_260925",
                "symbol": "BTC/USDT:USDT-260925", "swap": False, "future": True, "type": "future"}
    parser_client.set_markets([parser_client.markets["BTC/USDT:USDT"], delivery])
    rows = [position_row(), position_row(symbol=delivery["id"])]
    if kind == "positions":
        monkeypatch.setattr(parser_client, "privateUtaGetV3PositionCurrentPosition", Mock(return_value=response({"list": rows})))
        result = uta.fetch_positions(parser_client)
    else:
        for i, row in enumerate(rows):
            row.update(orderId=str(i), qty="2", price="100", side="buy", orderType="limit",
                       cumExecQty="0", feeDetail=[], orderStatus="live")
        monkeypatch.setattr(parser_client, "privateUtaGetV3TradeUnfilledOrders", Mock(return_value=response({"list": rows})))
        result = uta.fetch_open_orders(parser_client, None)
    assert len(result) == 1
    assert result[0]["symbol"] == "BTC/USDT:USDT"


@pytest.mark.parametrize("kind", ["positions", "orders"])
def test_snapshot_unknown_market_aborts(parser_client, monkeypatch, kind):
    """Unknown native IDs cannot turn into a fabricated market or silent deletion."""
    row = position_row(symbol="UNKNOWNUSDT")
    if kind == "positions":
        monkeypatch.setattr(parser_client, "privateUtaGetV3PositionCurrentPosition", Mock(return_value=response({"list": [row]})))
        operation = lambda: uta.fetch_positions(parser_client)
    else:
        row.update(orderId="1", qty="2", price="100", side="buy", cumExecQty="0", feeDetail=[])
        monkeypatch.setattr(parser_client, "privateUtaGetV3TradeUnfilledOrders", Mock(return_value=response({"list": [row]})))
        operation = lambda: uta.fetch_open_orders(parser_client, None)
    with pytest.raises(uta.BitgetUTAError, match="unresolved"):
        operation()


@pytest.mark.parametrize("field,value", [("total", "-1"), ("total", "NaN"), ("avgPrice", "-1")])
def test_position_invalid_native_quantity(parser_client, monkeypatch, field, value):
    """Negative native quantities and non-finite sizes fail before CCXT parsing."""
    monkeypatch.setattr(parser_client, "privateUtaGetV3PositionCurrentPosition",
                        Mock(return_value=response({"list": [position_row(**{field: value})]})))
    with pytest.raises(uta.BitgetUTAError):
        uta.fetch_positions(parser_client)


@pytest.mark.parametrize("field,value", [("contracts", None), ("contracts", float("nan")),
    ("contracts", -1), ("contractSize", 0), ("contractSize", float("inf"))])
def test_position_invalid_normalized_size(parser_client, monkeypatch, field, value):
    """Parser output must be finite and usable before a database update."""
    monkeypatch.setattr(parser_client, "privateUtaGetV3PositionCurrentPosition",
                        Mock(return_value=response({"list": [position_row()]})))
    parsed = {"symbol": "BTC/USDT:USDT", "contracts": 2, "contractSize": 1, field: value}
    monkeypatch.setattr(parser_client, "parse_position", Mock(return_value=parsed))
    with pytest.raises(uta.BitgetUTAError):
        uta.fetch_positions(parser_client)


@pytest.mark.parametrize("changes", [{"linear": None}, {"contractSize": None}, {"contractSize": 0},
                                     {"contractSize": float("nan")}, {"settle": None}])
def test_perpetual_metadata_incomplete(parser_client, changes):
    """Known in-scope perpetuals cannot be skipped due to incomplete metadata."""
    parser_client.markets_by_id["BTCUSDT"][0].update(changes)
    with pytest.raises(uta.BitgetUTAError):
        uta._perpetual_market(parser_client, "BTCUSDT")


def test_history_scope_stable_when_uid_disappears():
    """A settings refresh cannot change history IDs for the persisted user key."""
    client = Mock(options={})
    client.milliseconds.return_value = 2000
    client.privateUtaGetV3AccountSettings.side_effect = [response({"accountMode": "unified", "uid": "123"}),
                                                       response({"accountMode": "unified"})]
    client.privateUtaGetV3AccountFinancialRecords.return_value = response({"list": [ledger()], "cursor": ""})
    ex = Exchange("bitget", user())
    ex.instance = client
    first = ex.fetch_history(1000)
    ex._bitget_mode_checked -= uta.MODE_TTL_SECONDS
    assert ex.fetch_history(1000) == first


def test_spot_only_metadata_does_not_resolve_native_future(parser_client):
    """A matching spot ID alone cannot justify dropping a native futures row."""
    parser_client.markets_by_id["BTCUSDT"] = [{"id": "BTCUSDT", "spot": True, "swap": False}]
    with pytest.raises(uta.BitgetUTAError, match="unresolved"):
        uta._perpetual_market(parser_client, "BTCUSDT")


@pytest.mark.parametrize("field,value", [("amount", None), ("amount", float("nan")), ("amount", -1),
    ("filled", float("inf")), ("remaining", -1), ("price", float("nan"))])
def test_order_normalized_amounts_finite(parser_client, monkeypatch, field, value):
    """Normalized order amounts cannot bypass native finite-number checks."""
    row = {"category": uta.CATEGORY, "symbol": "BTCUSDT", "orderId": "1", "qty": "2",
           "price": "100", "side": "buy", "cumExecQty": "0", "feeDetail": []}
    monkeypatch.setattr(parser_client, "privateUtaGetV3TradeUnfilledOrders",
                        Mock(return_value=response({"list": [row], "cursor": ""})))
    parsed = {"amount": 2, "filled": 0, "remaining": 2, "price": 100, field: value}
    monkeypatch.setattr(parser_client, "parse_order", Mock(return_value=parsed))
    with pytest.raises(uta.BitgetUTAError):
        uta.fetch_open_orders(parser_client, None)


def test_bitget_public_ws_still_available(monkeypatch):
    """Private opt-out does not affect shared public clients or ownership."""
    async def run():
        """Use only fake clients and isolated shared registries."""
        for name in ("_shared_ws_clients", "_shared_ws_owners", "_shared_ws_locks"):
            monkeypatch.setattr(Exchange, name, {})
        monkeypatch.setattr(Exchange, "_shared_ws_markets_loaded", set())
        client = SimpleNamespace(load_markets=AsyncMock(), close=AsyncMock(),
                                 privateUtaGetV3AccountSettings=AsyncMock())
        factory = Mock(return_value=client)
        monkeypatch.setattr(exchange_module.ccxt_pro, "bitget", factory)
        assert await Exchange.get_shared_ws_client("bitget", caller="candles") is client
        assert await Exchange.get_shared_ws_client("bitget", caller="prices") is client
        factory.assert_called_once()
        client.privateUtaGetV3AccountSettings.assert_not_awaited()
        assert not await Exchange.release_shared_ws_client("bitget", caller="candles")
        client.close.assert_not_awaited()
        assert await Exchange.release_shared_ws_client("bitget", caller="prices")
        client.close.assert_awaited_once()
    asyncio.run(run())


class ObservedRLock:
    """Expose contention while retaining the Exchange instance's real RLock."""

    def __init__(self, lock):
        """Wrap the production lock, without changing its reentrant behavior."""
        self.lock = lock
        self.blocked = threading.Event()

    def __enter__(self):
        """Signal only when another thread owns the lock."""
        if not self.lock.acquire(blocking=False):
            self.blocked.set()
            assert self.lock.acquire(timeout=5), "client operation never released its lock"
        return self

    def __exit__(self, *args):
        """Always release, including when the guarded operation fails."""
        self.lock.release()


@pytest.mark.parametrize("second_operation", ["positions", "ensure", "balance", "history", "executions"])
def test_concurrent_ttl_refresh_keeps_one_uta_generation(second_operation):
    """Actual mode resolution plus the native read form one atomic operation."""
    ex = Exchange("bitget", user())
    client = Mock(options={"uta": True})
    ex.instance = client
    ex._bitget_uta = True
    ex._bitget_mode_checked = exchange_module.time.monotonic() - uta.MODE_TTL_SECONDS - 1
    observed = ObservedRLock(ex._bitget_lock)
    ex._bitget_lock = observed
    probe_barrier = threading.Barrier(2)
    release_probe = threading.Event()
    read_started = threading.Event()
    release_read = threading.Event()

    def settings():
        """Hold the real resolver at the native settings response boundary."""
        probe_barrier.wait(timeout=5)
        assert release_probe.wait(5)
        return response({"accountMode": "unified"})

    def orders(params):
        """Hold the first native read after mode publication."""
        assert client.options["uta"] is True
        read_started.set()
        assert release_read.wait(5)
        return response({"list": [], "cursor": ""})

    client.privateUtaGetV3AccountSettings.side_effect = settings
    client.privateUtaGetV3TradeUnfilledOrders.side_effect = orders
    client.privateUtaGetV3PositionCurrentPosition.return_value = response({"list": []})
    client.privateUtaGetV3AccountAssets.return_value = response({"assets": []})
    client.privateUtaGetV3AccountFinancialRecords.return_value = response({"list": [], "cursor": ""})
    client.privateUtaGetV3TradeFills.return_value = response({"list": [], "cursor": ""})
    client.milliseconds.return_value = 2000
    operation, args, expected = {
        "positions": (ex.fetch_positions, (), []),
        "ensure": (ex.ensure_bitget_account_mode, (), True),
        "balance": (ex.fetch_balance, ("swap",), 0),
        "history": (ex.fetch_history, (1000,), []),
        "executions": (ex.fetch_executions, (1000,), []),
    }[second_operation]
    with ThreadPoolExecutor(max_workers=2) as workers:
        first = workers.submit(ex.fetch_all_open_orders, None)
        try:
            probe_barrier.wait(timeout=5)
            second = workers.submit(operation, *args)
            assert observed.blocked.wait(5)
            assert client.privateUtaGetV3AccountSettings.call_count == 1
            release_probe.set()
            assert read_started.wait(5)
            assert not second.done()
            assert ex._bitget_uta is True and client.options["uta"] is True
            client.privateUtaGetV3PositionCurrentPosition.assert_not_called()
        finally:
            release_probe.set()
            release_read.set()
        assert first.result(timeout=5) == []
        assert second.result(timeout=5) == expected
    assert client.privateUtaGetV3AccountSettings.call_count == 1
    client.fetch_positions.assert_not_called()
    client.fetch_open_orders.assert_not_called()


@pytest.mark.parametrize("action", ["close", "connect", "credentials", "markets", "quote"])
def test_client_lifecycle_waits_for_private_read(monkeypatch, action):
    """Close, replacement and public client use wait for the complete REST read."""
    account = user()
    ex = Exchange("bitget", account)
    client = Mock(options={})
    client.privateUtaGetV3AccountSettings.return_value = response({"accountMode": "unified"})
    ex.instance = client
    assert ex.ensure_bitget_account_mode()
    observed = ObservedRLock(ex._bitget_lock)
    ex._bitget_lock = observed
    read_barrier = threading.Barrier(2)
    release_read = threading.Event()
    replacement_client = Mock(options={})
    replacement_client.privateUtaGetV3AccountSettings.return_value = response({"accountMode": "hybrid"})
    factory = Mock(return_value=replacement_client)
    monkeypatch.setattr(exchange_module.ccxt, "bitget", factory)

    def assets(params):
        """Pause within an authenticated native request."""
        read_barrier.wait(timeout=5)
        assert release_read.wait(5)
        client.close.assert_not_called()
        return response({"assets": [{"coin": "USDT", "balance": "42"}]})

    def change_client():
        """Exercise real nested connect/close during credential replacement."""
        if action == "credentials":
            replacement = user()
            replacement.secret = "replacement"
            ex.user = replacement
            return ex.ensure_bitget_account_mode()
        if action == "markets":
            return ex.load_market()
        if action == "quote":
            return ex.fetch_price("BTC/USDT:USDT", "swap")
        return getattr(ex, action)()

    client.privateUtaGetV3AccountAssets.side_effect = assets
    with ThreadPoolExecutor(max_workers=2) as workers:
        first = workers.submit(ex.fetch_balance, "swap")
        try:
            read_barrier.wait(timeout=5)
            second = workers.submit(change_client)
            assert observed.blocked.wait(5)
            assert not second.done()
            assert ex.user is account and ex.instance is client
            client.close.assert_not_called()
            client.load_markets.assert_not_called()
            client.fetch_ticker.assert_not_called()
            factory.assert_not_called()
        finally:
            release_read.set()
        assert first.result(timeout=5) == 42
        second.result(timeout=5)
    if action in {"close", "credentials"}:
        client.close.assert_called_once()
    if action in {"connect", "credentials"}:
        factory.assert_called_once()
        assert ex.instance is replacement_client
    if action == "credentials":
        assert replacement_client.options["uta"] is True


def test_client_lock_is_not_global():
    """Independent Bitget instances and other exchanges never share the lock."""
    first = Exchange("bitget", user())
    second = Exchange("bitget", user())
    second.instance = Mock(options={})
    second.instance.privateUtaGetV3AccountSettings.return_value = response({"accountMode": "unified"})
    other = Exchange("binance", user())
    other.instance = Mock()
    other.instance.fetch_balance.return_value = {"info": {"totalWalletBalance": "7"}}
    with ThreadPoolExecutor(max_workers=2) as workers:
        with first._bitget_lock, other._bitget_lock:
            assert workers.submit(second.ensure_bitget_account_mode).result(timeout=5) is True
            assert workers.submit(other.fetch_balance, "swap").result(timeout=5) == 7


def test_failed_private_read_releases_lock():
    """A failed request cannot strand future callers behind the instance lock."""
    ex = Exchange("bitget", user())
    ex.instance = Mock(options={})
    ex.instance.privateUtaGetV3AccountSettings.return_value = response({"accountMode": "unified"})
    ex.instance.privateUtaGetV3AccountAssets.side_effect = [AuthenticationError("hidden"),
        response({"assets": [{"coin": "USDT", "balance": "3"}]})]
    with pytest.raises(uta.BitgetUTAError):
        ex.fetch_balance("swap")
    with ThreadPoolExecutor(max_workers=1) as workers:
        assert workers.submit(ex.fetch_balance, "swap").result(timeout=5) == 3
