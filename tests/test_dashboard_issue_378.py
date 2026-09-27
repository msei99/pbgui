"""Offline dashboard regressions for gross exposure and historical position rows."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from api import dashboard


def _position(user, size, entry=100.0):
    """Build a DB position row with a nullable side column."""
    return [1, "ETHUSDT", 0, size, 0.0, entry, user, None]


@pytest.fixture
def dashboard_data(monkeypatch):
    """Supply only in-memory users and DB responses to dashboard routes."""
    users = {
        name: SimpleNamespace(name=name, exchange="binance")
        for name in ("alice", "bob")
    }
    positions = {"alice": [_position("alice", 1.0)], "bob": [_position("bob", -1.0)]}
    db = Mock()
    balances = [
        [1, 0, 1000.0, "alice"],
        [2, 0, 1000.0, "bob"],
    ]
    db.fetch_balances.side_effect = lambda selected: [row for row in balances if row[3] in selected]
    db.fetch_positions.side_effect = lambda user: positions[user.name]
    db.fetch_prices.return_value = [[1, "ETHUSDT", 0, 110.0]]
    db.fetch_orders_by_symbol.return_value = []
    all_users = SimpleNamespace(
        list=lambda: list(users),
        find_user=lambda name: users.get(name),
    )
    monkeypatch.setattr(dashboard, "_get_db", lambda: db)
    monkeypatch.setattr(dashboard, "_get_users", lambda: all_users)
    monkeypatch.setattr(dashboard, "_market_close_capability", lambda *_: {})
    monkeypatch.setattr(dashboard, "_log", Mock())
    return db, users, positions


def test_gross_exposure_in_db_and_live_fallback(dashboard_data, monkeypatch):
    """Short exposure must add to portfolio TWE in both DB-backed paths."""
    db, _, _ = dashboard_data
    result = dashboard.get_balance(users="ALL", live=False, session=None)
    assert [row["we"] for row in result["rows"]] == [10.0, 10.0]
    assert result["totals"]["we"] == 10.0

    monkeypatch.setattr(dashboard, "_live_balance_for_user", Mock(side_effect=RuntimeError("offline")))
    result = dashboard.get_balance(users="ALL", live=True, session=None)
    assert result["source"] == "db"
    assert [row["we"] for row in result["rows"]] == [10.0, 10.0]
    assert result["totals"]["we"] == 10.0
    assert db.fetch_positions.call_count == 4


def test_live_balance_exposure_uses_absolute_notional(dashboard_data, monkeypatch):
    """Signed live position sizes must still produce gross exposure."""
    _, users, _ = dashboard_data
    monkeypatch.setattr(dashboard, "_get_exchange", lambda _: SimpleNamespace(fetch_balance=lambda _: 1000.0))
    monkeypatch.setattr(dashboard, "_live_positions_for_user", lambda *_: [
        {"size": 1.0, "entry": 100.0, "upnl": 0.0},
        {"size": -1.0, "entry": 100.0, "upnl": 0.0},
    ])
    assert dashboard._live_balance_for_user(users["alice"], None) == (1000.0, 0.0, 200.0)


def test_missing_db_side_is_inferred_for_positions_and_actions(dashboard_data):
    """A signed historical short row must keep its side and positive value."""
    _, _, positions = dashboard_data
    result = dashboard.get_positions_data(users="bob", live=False, session=None)
    row = result["positions"][0]
    assert row["side"] == "short"
    assert row["size"] == -1.0
    assert row["pos_value"] == 110.0
    assert dashboard._find_dashboard_position(positions["bob"], "ETHUSDT", "short") == positions["bob"][0]
    assert dashboard._find_dashboard_position(positions["bob"], "ETHUSDT", "long") is None


@pytest.mark.parametrize("from_date,dates", [
    ("2026-09-01", ["2026-09-01", "2026-09-02", "2026-09-03"]),
    ("", ["2026-09-03"]),
])
def test_adg_fills_requested_start_and_all_time_fallback(dashboard_data, monkeypatch, from_date, dates):
    """ADG starts at the requested date, or the first trade for ALL_TIME."""
    db, _, _ = dashboard_data
    db.select_pnl.return_value = [["2026-09-03", 10.0]]
    monkeypatch.setattr(dashboard, "_period_to_range", lambda _: (0, 0, from_date, "2026-09-03"))
    result = dashboard.get_adg_data(users="alice", period="THIS_MONTH", mode="bar", session=None)
    assert [bar["date"] for bar in result["bars"]] == dates
    assert [bar["adg"] for bar in result["bars"][:-1]] == [0.0] * (len(dates) - 1)
    assert result["bars"][-1]["adg"] == pytest.approx(100 * 10 / 990, abs=0.0001)


def test_dashboard_chart_regressions():
    """Render charts with an offline Plotly stub to check data and axes."""
    import subprocess
    from pathlib import Path

    script = Path(__file__).with_name("dashboard_issue_378.cjs")
    subprocess.run(["node", str(script)], check=True, capture_output=True, text=True)
