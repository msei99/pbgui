"""Regression tests for dashboard position order classification.

These tests lock down the long/short-specific mapping used by the dashboard
snapshot and live API paths for DCA counting plus nearest DCA/TP prices.
"""

import asyncio

import pytest

from api import dashboard, live


class _TickerExchange:
    """Minimal exchange stub returning a fixed ticker."""

    def __init__(self, ticker):
        """Store the ticker payload returned by fetch_ticker."""
        self.ticker = ticker

    def fetch_ticker(self, symbol):
        """Return the configured ticker regardless of symbol."""
        return self.ticker

    def market(self, symbol):
        """Return minimal market metadata with Hyperliquid min cost."""
        return {"limits": {"cost": {"min": 10.0}}}


class _UserStub:
    """Minimal user object for dashboard order parameter tests."""

    def __init__(self, is_vault=False, wallet_address="", name="alice", exchange="hyperliquid"):
        """Store fields used by dashboard helper tests."""
        self.name = name
        self.exchange = exchange
        self.is_vault = is_vault
        self.wallet_address = wallet_address


class _LivePositionsExchange:
    """Minimal Exchange wrapper stub for live dashboard positions."""

    def __init__(self, positions):
        """Store raw positions returned by fetch_positions."""
        self.positions = positions
        self.instance = _TickerExchange({"last": 0.0})

    def fetch_positions(self):
        """Return configured CCXT-style positions."""
        return self.positions


class _DbStub:
    """Minimal DB stub returning no dashboard orders."""

    def fetch_orders_by_symbol(self, user, symbol):
        """Return no open orders for DCA/TP classification."""
        return []


def _order(price: float, side: str) -> list:
    """Build a minimal DB-style order row for classification tests."""
    return [0, 0, 0, 0, price, side]


@pytest.mark.parametrize(
    ("helper", "label"),
    [
        (dashboard._classify_position_orders, "dashboard"),
        (live._classify_position_orders, "live"),
    ],
)
def test_classify_position_orders_uses_nearest_prices_for_long_and_short(helper, label):
    """Classify long/short orders with the correct nearest DCA and TP prices."""
    long_orders = [
        _order(97.0, "buy"),
        _order(99.0, "buy"),
        _order(104.0, "sell"),
        _order(106.0, "sell"),
    ]
    short_orders = [
        _order(103.0, "sell"),
        _order(101.0, "sell"),
        _order(99.0, "buy"),
        _order(97.0, "buy"),
    ]

    long_dca, long_next_dca, long_next_tp = helper(long_orders, "long")
    short_dca, short_next_dca, short_next_tp = helper(short_orders, "short")

    assert long_dca == 2, f"{label}: expected two long DCA orders"
    assert long_next_dca == 99.0, f"{label}: expected nearest long DCA at 99.0"
    assert long_next_tp == 104.0, f"{label}: expected nearest long TP at 104.0"

    assert short_dca == 2, f"{label}: expected two short DCA orders"
    assert short_next_dca == 101.0, f"{label}: expected nearest short DCA at 101.0"
    assert short_next_tp == 99.0, f"{label}: expected nearest short TP at 99.0"


@pytest.mark.parametrize(
    ("helper", "label"),
    [
        (dashboard._classify_position_orders, "dashboard"),
        (live._classify_position_orders, "live"),
    ],
)
def test_classify_position_orders_normalizes_side_casing(helper, label):
    """Treat upper-case side values the same as lower-case inputs."""
    orders = [
        _order(103.0, "SELL"),
        _order(101.0, "sell"),
        _order(99.0, "BUY"),
        _order(97.0, "buy"),
    ]

    dca, next_dca, next_tp = helper(orders, "SHORT")

    assert dca == 2, f"{label}: expected short DCA count to stay case-insensitive"
    assert next_dca == 101.0, f"{label}: expected nearest short DCA at 101.0"
    assert next_tp == 99.0, f"{label}: expected nearest short TP at 99.0"


def test_extract_order_position_side_prefers_exchange_fields_then_pb_ids_then_reduce_only():
    """Detect live order legs from exchange fields, PB ids, then reduceOnly fallback."""
    assert dashboard._extract_order_position_side({"info": {"positionIdx": "1"}}) == "long"
    assert dashboard._extract_order_position_side({"info": {"positionSide": "SHORT"}}) == "short"
    assert dashboard._extract_order_position_side({"clientOrderId": "entry_close_short_0x1234"}) == "short"
    assert dashboard._extract_order_position_side({"side": "buy", "reduceOnly": True}) == "short"
    assert dashboard._extract_order_position_side({"side": "sell", "reduceOnly": False}) == "short"


def test_filter_live_orders_for_side_marks_ambiguous_orders_unknown():
    """Keep matching live leg orders and flag ambiguous leftovers."""
    orders = [
        {"price": 101.0, "amount": 1.0, "side": "sell", "info": {"positionIdx": "2"}},
        {"price": 99.0, "amount": 1.0, "side": "buy", "info": {"positionIdx": "2"}},
        {"price": 105.0, "amount": 1.0, "side": "sell"},
    ]

    filtered, unknown = dashboard._filter_live_orders_for_side(orders, "short")

    assert filtered == [
        {"price": 101.0, "amount": 1.0, "side": "sell"},
        {"price": 99.0, "amount": 1.0, "side": "buy"},
    ]
    assert unknown is True


def test_live_order_classification_uses_ambiguous_symbol_orders(monkeypatch):
    """Use live symbol orders for DCA/TP even when the exchange omits leg metadata."""
    live_orders = [
        {"price": 0.08028, "amount": 10.0, "side": "buy"},
        {"price": 0.09714, "amount": 10.0, "side": "sell"},
    ]
    monkeypatch.setattr(dashboard, "_live_open_orders_for_symbol", lambda user, symbol: live_orders)

    user = _UserStub(name="hl_manicptpro", exchange="hyperliquid")
    dca, next_dca, next_tp = dashboard._classify_orders_for_position(user, _DbStub(), "DOGEUSDC", "long", live=True)
    orders, unknown, source = dashboard._dashboard_orders_for_position(user, _DbStub(), "DOGEUSDC", "long", live=True)

    assert dca == 1
    assert next_dca == 0.08028
    assert next_tp == 0.09714
    assert orders == live_orders
    assert unknown is True
    assert source == "live"


def test_has_hedged_symbol_positions_detects_dual_legs_only_for_same_symbol():
    """Treat a symbol as hedged only when both long and short rows are open."""
    positions = [
        [0, "BTCUSDT", 0, 1.0, 0.0, 100.0, "alice", "long"],
        [0, "BTCUSDT", 0, 1.0, 0.0, 100.0, "alice", "short"],
        [0, "ETHUSDT", 0, 1.0, 0.0, 100.0, "alice", "long"],
    ]

    assert dashboard._has_hedged_symbol_positions(positions, "BTCUSDT") is True
    assert dashboard._has_hedged_symbol_positions(positions, "ETHUSDT") is False


def test_position_close_order_side_reduces_long_and_short():
    """Map position side to the opposite reduce-only market order side."""
    assert dashboard._position_close_order_side("long") == "sell"
    assert dashboard._position_close_order_side("short") == "buy"


def test_resolve_close_amount_supports_amount_and_percent():
    """Resolve explicit and percent close amounts without exceeding position size."""
    assert dashboard._resolve_close_amount(100.0, 25.0, None) == 25.0
    assert dashboard._resolve_close_amount(100.0, None, 50.0) == 50.0


def test_market_close_param_candidates_try_bybit_hedge_before_one_way():
    """Close Bybit hedge-mode legs even when only one side is currently open."""
    assert dashboard._market_close_param_candidates("bybit", "long", False) == [
        {"reduceOnly": True, "positionIdx": 1},
        {"reduceOnly": True},
    ]
    assert dashboard._market_close_param_candidates("bybit", "short", False) == [
        {"reduceOnly": True, "positionIdx": 2},
        {"reduceOnly": True},
    ]


def test_market_close_param_candidates_try_binance_position_side_before_one_way():
    """Close Binance hedge-mode legs even when only one side is currently open."""
    assert dashboard._market_close_param_candidates("binance", "long", False) == [
        {"positionSide": "LONG"},
        {"reduceOnly": True},
    ]
    assert dashboard._market_close_param_candidates("binance", "short", False) == [
        {"positionSide": "SHORT"},
        {"reduceOnly": True},
    ]


def test_market_close_params_match_exchange_specific_reduce_only_fields():
    """Build close params matching Passivbot's exchange-specific field names."""
    assert dashboard._market_close_params("bitget", "short", False) == {
        "reduceOnly": True,
        "holdSide": "short",
        "oneWayMode": False,
    }
    assert dashboard._market_close_params("gateio", "long", False) == {
        "reduceOnly": True,
        "reduce_only": True,
    }
    assert dashboard._market_close_params("okx", "long", False) == {
        "reduceOnly": True,
        "hedged": True,
        "posSide": "long",
    }


def test_is_position_mode_mismatch_detects_bybit_error():
    """Retry market closes only for exchange position mode mismatch errors."""
    assert dashboard._is_position_mode_mismatch(Exception("position idx not match position mode")) is True
    assert dashboard._is_position_mode_mismatch(Exception("Order's position side does not match user's setting.")) is True
    assert dashboard._is_position_mode_mismatch(Exception("insufficient balance")) is False


def test_is_amount_precision_error_detects_user_correctable_close_amount():
    """Treat exchange amount precision failures as validation errors."""
    msg = "binance amount of ICP/USDT:USDT must be greater than minimum amount precision of 1"

    assert dashboard._is_amount_precision_error(Exception(msg)) is True
    assert dashboard._is_amount_precision_error(Exception("exchange unavailable")) is False


def test_dashboard_price_from_rows_returns_positive_symbol_price():
    """Read the dashboard market price used for Hyperliquid market closes."""
    rows = [
        [1, "BTCUSDT", 0, 50000.0, "alice"],
        [2, "ETHUSDT", 0, 2500.0, "alice"],
    ]

    assert dashboard._dashboard_price_from_rows(rows, "ETHUSDT") == 2500.0
    assert dashboard._dashboard_price_from_rows(rows, "SOLUSDT") == 0.0


def test_market_close_price_arg_only_required_for_hyperliquid():
    """Pass a price to Hyperliquid market orders for CCXT slippage handling."""
    exchange = _TickerExchange({"bid": 49990.0, "ask": 50010.0, "last": 50000.0})

    assert dashboard._market_close_price_arg(None, "bybit", "BTC/USDT:USDT", "buy", 50000.0) is None
    assert dashboard._market_close_price_arg(exchange, "hyperliquid", "BTC/USDC:USDC", "buy", 40000.0) == 50010.0
    assert dashboard._market_close_price_arg(exchange, "hyperliquid", "BTC/USDC:USDC", "sell", 40000.0) == 49990.0


def test_apply_market_close_user_params_adds_hyperliquid_vault_address():
    """Send Hyperliquid vaultAddress for vault users, matching Passivbot."""
    params = {"reduceOnly": True}

    assert dashboard._apply_market_close_user_params(params, "hyperliquid", _UserStub(True, "0xabc")) == {
        "reduceOnly": True,
        "vaultAddress": "0xabc",
    }
    assert dashboard._apply_market_close_user_params(params, "hyperliquid", _UserStub(False, "0xabc")) == params


def test_validate_market_close_min_cost_rejects_hyperliquid_dust_order():
    """Reject Hyperliquid market closes below the exchange minimum order value."""
    exchange = _TickerExchange({})

    assert dashboard._market_close_min_cost(exchange, "hyperliquid", "DOGE/USDC:USDC") == 10.0
    with pytest.raises(Exception) as exc_info:
        dashboard._validate_market_close_min_cost("hyperliquid", 1.0, 0.15, 10.0)
    assert "minimum order value" in str(exc_info.value)
    dashboard._validate_market_close_min_cost("hyperliquid", 100.0, 0.15, 10.0)


def test_live_positions_for_user_normalizes_exchange_payload(monkeypatch):
    """Build dashboard position rows directly from live exchange positions."""
    raw_positions = [
        {
            "symbol": "DOGE/USDC:USDC",
            "contracts": 1062,
            "contractSize": 1,
            "side": "long",
            "entryPrice": 0.08467,
            "unrealizedPnl": 0.026,
            "markPrice": 0.08511,
        },
        {"symbol": "BTC/USDT:USDT", "contracts": 0, "side": "long"},
    ]
    monkeypatch.setattr(dashboard, "_get_exchange", lambda user: _LivePositionsExchange(raw_positions))

    rows = dashboard._live_positions_for_user(_UserStub(name="hl_manicptpro", exchange="bybit"), _DbStub())

    assert rows == [
        {
            "user": "hl_manicptpro",
            "exchange": "bybit",
            "symbol": "DOGEUSDC",
            "side": "long",
            "size": 1062.0,
            "upnl": 0.026,
            "entry": 0.08467,
            "price": 0.08511,
            "dca": 0,
            "next_dca": 0.0,
            "next_tp": 0.0,
            "pos_value": 90.39,
        }
    ]


def test_hyperliquid_live_state_builds_balance_and_positions(monkeypatch):
    """Parse Hyperliquid clearinghouse state for live dashboard balance/positions."""
    state = {
        "marginSummary": {"accountValue": "90.406"},
        "assetPositions": [
            {
                "position": {
                    "coin": "DOGE",
                    "szi": "1062.0",
                    "entryPx": "0.08467",
                    "unrealizedPnl": "0.026",
                    "positionValue": "90.38",
                }
            },
            {"position": {"coin": "BTC", "szi": "0"}},
        ],
    }
    monkeypatch.setattr(dashboard, "_hyperliquid_user_state", lambda user: state)
    monkeypatch.setattr(dashboard, "_live_open_orders_for_symbol", lambda user, symbol: [])

    user = _UserStub(name="hl_manicptpro", exchange="hyperliquid", wallet_address="0xvault")

    balance, upnl = dashboard._hyperliquid_live_balance_for_user(user)
    assert balance == pytest.approx(90.38)
    assert upnl == pytest.approx(0.026)
    assert dashboard._hyperliquid_live_positions_for_user(user, _DbStub()) == [
        {
            "user": "hl_manicptpro",
            "exchange": "hyperliquid",
            "symbol": "DOGEUSDC",
            "side": "long",
            "size": 1062.0,
            "upnl": 0.026,
            "entry": 0.08467,
            "price": pytest.approx(90.38 / 1062.0),
            "dca": 0,
            "next_dca": 0.0,
            "next_tp": 0.0,
            "pos_value": 90.38,
        }
    ]


def test_dashboard_coin_key_matches_coin_override_normalization():
    """Normalize dashboard symbols to coin_overrides keys used by PBGui."""
    assert dashboard._dashboard_coin_key("DOGEUSDT") == "DOGE"
    assert dashboard._dashboard_coin_key("1000BONKUSDT") == "BONK"
    assert dashboard._dashboard_coin_key("kSHIBUSDT") == "SHIB"


def test_apply_panic_symbol_sets_only_selected_side_override():
    """Set per-symbol panic on the selected side without global forced modes."""
    cfg = {
        "live": {},
        "coin_overrides": {
            "DOGE": {"bot": {"short": {"forced_mode_short": "panic"}}},
        },
    }

    coin = dashboard._apply_panic_symbol(cfg, "DOGEUSDT", "short")

    assert coin == "DOGE"
    assert cfg["coin_overrides"]["DOGE"]["live"]["forced_mode_short"] == "panic"
    assert cfg["coin_overrides"]["DOGE"]["bot"]["short"]["forced_mode_short"] == "panic"
    assert "forced_mode_long" not in cfg["live"]


def test_apply_panic_all_sets_global_long_and_short_modes():
    """Set global Passivbot panic modes for all positions of a user."""
    cfg = {"live": {}}

    dashboard._apply_panic_all(cfg)

    assert cfg["live"]["forced_mode_long"] == "p"
    assert cfg["live"]["forced_mode_short"] == "p"


def test_apply_graceful_stop_symbol_sets_selected_side_override():
    """Set per-symbol graceful stop on the selected side without global forced modes."""
    cfg = {"live": {}, "coin_overrides": {}}

    coin = dashboard._apply_graceful_stop_symbol(cfg, "DOGEUSDT", "long")

    assert coin == "DOGE"
    assert cfg["coin_overrides"]["DOGE"]["live"]["forced_mode_long"] == "graceful_stop"
    assert "forced_mode_short" not in cfg["coin_overrides"]["DOGE"]["live"]
    assert "forced_mode_long" not in cfg["live"]


def test_apply_graceful_stop_all_sets_global_long_and_short_modes():
    """Set global Passivbot graceful stop modes for all positions of a user."""
    cfg = {"live": {}}

    dashboard._apply_graceful_stop_all(cfg)

    assert cfg["live"]["forced_mode_long"] == "graceful_stop"
    assert cfg["live"]["forced_mode_short"] == "graceful_stop"


def test_apply_tp_only_symbol_sets_selected_side_override():
    """Set per-symbol take-profit-only on the selected side without global forced modes."""
    cfg = {"live": {}, "coin_overrides": {}}

    coin = dashboard._apply_tp_only_symbol(cfg, "DOGEUSDT", "short")

    assert coin == "DOGE"
    assert cfg["coin_overrides"]["DOGE"]["live"]["forced_mode_short"] == "tp_only"
    assert "forced_mode_long" not in cfg["coin_overrides"]["DOGE"]["live"]
    assert "forced_mode_short" not in cfg["live"]


def test_apply_tp_only_all_sets_global_long_and_short_modes():
    """Set global Passivbot take-profit-only modes for all positions of a user."""
    cfg = {"live": {}}

    dashboard._apply_tp_only_all(cfg)

    assert cfg["live"]["forced_mode_long"] == "tp_only"
    assert cfg["live"]["forced_mode_short"] == "tp_only"


def test_manage_position_panic_all_dry_run_does_not_save(monkeypatch, tmp_path):
    """Preview panic-all config without saving or starting SSH sync."""
    cfg = {"live": {"user": "alice"}, "pbgui": {"version": 7}}
    config_path = tmp_path / "config.json"
    called = {"save": False}

    def fake_find_instance(user):
        """Return a mutable instance config for the dry-run request."""
        assert user == "alice"
        return "alice_instance", config_path, cfg

    async def fake_save(*args, **kwargs):
        """Fail the test if dry-run reaches the save/sync path."""
        called["save"] = True
        raise AssertionError("dry_run must not save or sync")

    monkeypatch.setattr(dashboard, "_find_instance_config_for_user", fake_find_instance)
    monkeypatch.setattr(dashboard, "_save_dashboard_panic_config", fake_save)

    payload = dashboard.PositionManagePayload(user="alice", action="panic_all", dry_run=True)
    result = asyncio.run(dashboard.manage_position(payload, session=None))

    assert called["save"] is False
    assert result["dry_run"] is True
    assert result["version"] == 8
    assert result["config"]["live"]["forced_mode_long"] == "p"
    assert result["config"]["live"]["forced_mode_short"] == "p"
    assert cfg["pbgui"]["version"] == 7
    assert "forced_mode_long" not in cfg["live"]
    assert "forced_mode_short" not in cfg["live"]


def test_manage_position_graceful_stop_all_dry_run_does_not_save(monkeypatch, tmp_path):
    """Preview graceful-stop-all config without saving or starting SSH sync."""
    cfg = {"live": {"user": "alice"}, "pbgui": {"version": 11}}
    config_path = tmp_path / "config.json"
    called = {"save": False}

    def fake_find_instance(user):
        """Return a mutable instance config for the dry-run request."""
        assert user == "alice"
        return "alice_instance", config_path, cfg

    async def fake_save(*args, **kwargs):
        """Fail the test if dry-run reaches the save/sync path."""
        called["save"] = True
        raise AssertionError("dry_run must not save or sync")

    monkeypatch.setattr(dashboard, "_find_instance_config_for_user", fake_find_instance)
    monkeypatch.setattr(dashboard, "_save_dashboard_panic_config", fake_save)

    payload = dashboard.PositionManagePayload(user="alice", action="graceful_stop_all", dry_run=True)
    result = asyncio.run(dashboard.manage_position(payload, session=None))

    assert called["save"] is False
    assert result["dry_run"] is True
    assert result["version"] == 12
    assert result["config"]["live"]["forced_mode_long"] == "graceful_stop"
    assert result["config"]["live"]["forced_mode_short"] == "graceful_stop"
    assert cfg["pbgui"]["version"] == 11
    assert "forced_mode_long" not in cfg["live"]
    assert "forced_mode_short" not in cfg["live"]
