"""Offline P+L aggregation, API and AI persistence regressions for issue #440."""

import asyncio
from contextlib import closing, contextmanager
from datetime import datetime, timezone
import sqlite3
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from Database import Database
from ai_capabilities import AICapabilityError, AICapabilityService
from api import dashboard, dashboards


def _timestamp(value):
    """Convert explicit UTC fixture dates to milliseconds."""
    return int(datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp() * 1000)


@pytest.fixture
def ppl_history(tmp_path, monkeypatch):
    """Run the real aggregation SQL against a private file with boundary records."""
    start = _timestamp("2023-12-31T23:59:59")
    end = _timestamp("2025-01-01T00:00:00")
    path = tmp_path / "history.db"
    # Deliberately insert out of date order to exercise the observed year ordering.
    rows = [
        (end, "bob", 30),
        (start - 1, "alice", 999),
        (_timestamp("2024-06-01T12:00:00"), "bob", 20),
        (start, "alice", 10),
        (end + 1, "alice", -999),
        (_timestamp("2024-01-01T00:00:00"), "alice", -4),
        (start + 500, "bob", -2),
        (end, "alice", -6),
    ]
    with closing(sqlite3.connect(path)) as conn:
        conn.execute('CREATE TABLE history (timestamp INTEGER, user TEXT, income REAL)')
        conn.executemany('INSERT INTO history VALUES (?, ?, ?)', rows)
        conn.commit()

    @contextmanager
    def connect():
        """Close every test connection, including those opened in API threads."""
        with closing(sqlite3.connect(path)) as conn:
            yield conn

    db = object.__new__(Database)
    monkeypatch.setattr(db, "_connect", connect)
    return db, start, end


@pytest.mark.parametrize("users,expected", [
    (["ALL"], [("2023", 10, -2), ("2024", 20, -4), ("2025", 30, -6)]),
    (["alice"], [("2023", 10, 0), ("2024", 0, -4), ("2025", 0, -6)]),
    (["alice", "bob"], [("2023", 10, -2), ("2024", 20, -4), ("2025", 30, -6)]),
])
def test_ppl_year_utc_boundaries_and_users(ppl_history, users, expected):
    """Group two UTC year transitions without changing date or account filters."""
    db, start, end = ppl_history
    # This checks SQLite's observed output, not an SQL ordering guarantee.
    assert db.select_ppl(users, start, end, "YEAR") == expected


@pytest.mark.parametrize("users,expected", [
    (["ALL"], [("ALL_TIME", 60, -12)]),
    (["alice"], [("ALL_TIME", 10, -10)]),
    (["bob"], [("ALL_TIME", 50, -2)]),
    (["alice", "bob"], [("ALL_TIME", 60, -12)]),
])
def test_ppl_all_time_keeps_inclusive_range_and_users(ppl_history, users, expected):
    """Include both boundary instants and exclude their immediate neighbours."""
    db, start, end = ppl_history
    assert db.select_ppl(users, start, end, "ALL_TIME") == expected


@pytest.mark.parametrize("period,labels", [
    ("DAY", ["2023-12-31", "2024-01-01", "2024-06-01", "2025-01-01"]),
    ("WEEK", ["2023-52", "2024-01", "2024-22", "2025-00"]),
    ("MONTH", ["2023-12", "2024-01", "2024-06", "2025-01"]),
])
def test_ppl_existing_aggregations(ppl_history, period, labels):
    """Keep the existing daily, weekly and monthly totals and labels."""
    db, start, end = ppl_history
    assert db.select_ppl(["ALL"], start, end, period) == [
        (labels[0], 10, -2), (labels[1], 0, -4),
        (labels[2], 20, 0), (labels[3], 30, -6),
    ]


@pytest.mark.parametrize("sum_period,users,expected", [
    ("YEAR", "alice", [("2023", 10, 0), ("2024", 0, -4), ("2025", 0, -6)]),
    ("ALL_TIME", "alice,bob", [("ALL_TIME", 60, -12)]),
    ("ALL_TIME", "missing", [("ALL_TIME", 0, 0)]),
])
def test_ppl_route_aggregates_and_empty_result(ppl_history, monkeypatch, sum_period, users, expected):
    """Use real SQL through the route, including NULL-to-zero empty aggregates."""
    db, start, end = ppl_history
    requested_periods = []

    def bounds(period):
        """Supply fixed millisecond boundaries without a local-time dependency."""
        requested_periods.append(period)
        return start, end, "2023-12-31", "2025-01-01"

    monkeypatch.setattr(dashboard, "_get_db", lambda: db)
    monkeypatch.setattr(dashboard, "_period_to_range", bounds)
    app = FastAPI()
    app.include_router(dashboard.router, prefix="/api/dashboard")
    app.dependency_overrides[dashboard.require_auth] = lambda: object()
    with TestClient(app) as client:
        response = client.get("/api/dashboard/ppl_data", params={
            "period": "CUSTOM:2023-12-31:2024-12-31", "users": users, "sum_period": sum_period,
        })
    assert response.status_code == 200
    assert requested_periods == ["CUSTOM:2023-12-31:2024-12-31"]
    assert response.json() == {
        "sum_period": sum_period, "from_date": "2023-12-31", "to_date": "2025-01-01",
        "bars": [{"period": label, "profits": profits, "losses": losses}
                 for label, profits, losses in expected],
    }


@pytest.fixture
def ppl_layout(tmp_path, monkeypatch):
    """Isolate the actual dashboard persistence and AI layout contract."""
    directory = tmp_path / "dashboards"
    directory.mkdir()
    monkeypatch.setattr(dashboards, "_dashboards_dir", lambda: directory)
    monkeypatch.setattr(dashboards, "list_users", lambda session=None: {"users": ["alice"]})
    dashboards.save_dashboard("portfolio", {
        "name": "portfolio", "rows": 1, "cols": 1,
        "dashboard_type_1_1": "P+L", "dashboard_ppl_users_1_1": ["alice"],
        "dashboard_ppl_period_1_1": "THIS_YEAR", "dashboard_ppl_sum_period_1_1": "MONTH",
    }, session=object())
    service = AICapabilityService(tmp_path / "capabilities")
    yield service
    asyncio.run(service.shutdown())


@pytest.mark.parametrize("value", ["DAY", "WEEK", "MONTH", "YEAR", "ALL_TIME", "year", "all_time"])
def test_ppl_ai_layout_save_get_roundtrip(ppl_layout, value):
    """Normalize accepted inputs and return the saved aggregation unchanged."""
    service = ppl_layout
    name, current, prepared, digest = service._prepare_dashboard_layout({
        "name": "portfolio", "cells": [{"row": 1, "column": 1, "sum_period": value}],
    })
    assert digest == service._digest(current)
    assert prepared["dashboard_ppl_period_1_1"] == "THIS_YEAR"
    assert prepared["dashboard_ppl_users_1_1"] == ["alice"]
    proposal = SimpleNamespace(
        id="test", name=name, config={"dashboard": prepared}, create_only=False, expected_digest=digest,
    )
    assert service._execute_dashboard_layout_save(proposal)["status"] == "executed"
    saved = dashboards.get_dashboard(name, session=object())["config"]
    assert saved["dashboard_ppl_sum_period_1_1"] == value.upper()
    assert service._get_dashboard_layout({"name": name})["cells"][0]["sum_period"] == value.upper()
    # Loading an existing YEAR/ALL_TIME config must not normalize it to DAY.
    _, _, reloaded, _ = service._prepare_dashboard_layout({"name": name, "cells": []})
    assert reloaded == saved


@pytest.mark.parametrize("widget,value", [
    ("P+L", "QUARTER"), ("P+L", ""), ("P+L", "YEARLY"),
    ("PNL", "YEAR"), ("ADG", "ALL_TIME"), ("INCOME", "YEAR"),
    ("BALANCE", "ALL_TIME"), ("TOP", "YEAR"), ("POSITIONS", "ALL_TIME"),
])
def test_ppl_ai_rejects_invalid_aggregation_or_widget(ppl_layout, widget, value):
    """Keep aggregation validation limited to the P+L widget and five values."""
    with pytest.raises(AICapabilityError, match="P\\+L sum period is invalid"):
        ppl_layout._prepare_dashboard_layout({"name": "portfolio", "cells": [
            {"row": 1, "column": 1, "type": widget, "sum_period": value},
        ]})


def test_ppl_ai_schema_matches_supported_aggregations(ppl_layout):
    """Expose exactly the same five aggregation values in the model schema."""
    tool = next(tool for tool in ppl_layout.responses_tools() if tool["name"] == "propose_dashboard_layout")
    schema = tool["parameters"]["properties"]["cells"]["items"]["properties"]["sum_period"]
    assert schema["enum"] == ["DAY", "WEEK", "MONTH", "YEAR", "ALL_TIME"]
