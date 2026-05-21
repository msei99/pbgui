"""Regression tests for dashboard position order classification.

These tests lock down the long/short-specific mapping used by the dashboard
snapshot and live API paths for DCA counting plus nearest DCA/TP prices.
"""

import pytest

from api import dashboard, live


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


def test_has_hedged_symbol_positions_detects_dual_legs_only_for_same_symbol():
    """Treat a symbol as hedged only when both long and short rows are open."""
    positions = [
        [0, "BTCUSDT", 0, 1.0, 0.0, 100.0, "alice", "long"],
        [0, "BTCUSDT", 0, 1.0, 0.0, 100.0, "alice", "short"],
        [0, "ETHUSDT", 0, 1.0, 0.0, 100.0, "alice", "long"],
    ]

    assert dashboard._has_hedged_symbol_positions(positions, "BTCUSDT") is True
    assert dashboard._has_hedged_symbol_positions(positions, "ETHUSDT") is False
