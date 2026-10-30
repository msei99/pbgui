"""Offline PNL gap, conversion and strict HTTP JSON regressions for issue #427."""

from javascript_checks import run_javascript

import json
from pathlib import Path
from unittest.mock import Mock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api import dashboard


@pytest.fixture
def pnl_client(monkeypatch):
    """Expose only the dashboard router with synthetic authenticated data."""
    db = Mock()
    log = Mock()
    monkeypatch.setattr(dashboard, "_get_db", lambda: db)
    monkeypatch.setattr(dashboard, "_log", log)
    monkeypatch.setattr(dashboard, "_period_to_range", lambda _: (0, 1, "2026-09-01", "2026-09-30"))
    app = FastAPI()
    app.include_router(dashboard.router, prefix="/api/dashboard")
    app.dependency_overrides[dashboard.require_auth] = lambda: object()
    with TestClient(app) as client:
        yield client, db, log


@pytest.mark.parametrize("invalid", [
    None, float("nan"), float("inf"), -float("inf"), "NaN", "Infinity",
    "", "  ", "bad", True, False, {}, [], 10 ** 1000,
])
def test_pnl_invalid_day_is_gap_and_later_sum_resumes(pnl_client, invalid):
    """Missing or unconvertible daily aggregates cannot break HTTP JSON or the next day."""
    client, db, log = pnl_client
    db.select_pnl.return_value = [
        ["2026-09-01", 5.0], ["2026-09-02", invalid], ["2026-09-03", 3.0],
    ]
    response = client.get("/api/dashboard/pnl_data", params={"users": "alice", "mode": "Cumulative"})
    assert response.status_code == 200
    payload = response.json()
    json.dumps(payload, allow_nan=False)
    assert payload["bars"] == [
        {"date": "2026-09-01", "income": 5.0, "cum": 5.0},
        {"date": "2026-09-02", "income": None, "cum": None},
        {"date": "2026-09-03", "income": 3.0, "cum": 8.0},
    ]
    db.select_pnl.assert_called_once_with(["alice"], 0, 1)
    log.assert_called_once_with(
        dashboard.SERVICE, "PNL data contains unavailable values", level="WARNING",
        meta={"invalid_days": 1, "cumulative_overflow": False},
    )


@pytest.mark.parametrize("values,incomes,cumulative", [
    ([" 5.125 ", "-2.25", 0, "0"], [5.12, -2.25, 0.0, 0.0], [5.12, 2.87, 2.87, 2.87]),
    ([1e308, 1e308, -1e308, 3], [1e308, 1e308, -1e308, 3.0], [1e308, None, None, None]),
    ([None, "bad", 0], [None, None, 0.0], [None, None, 0.0]),
    ([None, "bad"], [None, None], [None, None]),
    ([], [], []),
])
@pytest.mark.parametrize("mode", ["Daily", "Cumulative", "bar", "line"])
def test_pnl_rounding_overflow_and_empty_http_response(pnl_client, values, incomes, cumulative, mode):
    """Overflow affects only cumulative values; valid zeros and rounding remain intact."""
    client, db, log = pnl_client
    dates = [f"2026-09-{index + 1:02}" for index in range(len(values))]
    db.select_pnl.return_value = list(zip(dates, values))
    response = client.get("/api/dashboard/pnl_data", params={"mode": mode})
    assert response.status_code == 200
    payload = response.json()
    json.dumps(payload, allow_nan=False)
    assert payload["mode"] == mode
    assert [bar["date"] for bar in payload["bars"]] == dates
    assert [bar["income"] for bar in payload["bars"]] == incomes
    assert [bar["cum"] for bar in payload["bars"]] == cumulative
    invalid_days = sum(value is None for value in incomes)
    overflow = any(income is not None and cum is None for income, cum in zip(incomes, cumulative))
    if invalid_days or overflow:
        log.assert_called_once_with(
            dashboard.SERVICE, "PNL data contains unavailable values", level="WARNING",
            meta={"invalid_days": invalid_days, "cumulative_overflow": overflow},
        )
    else:
        log.assert_not_called()


@pytest.mark.parametrize("mode", ["Daily", "Cumulative", "bar", "line"])
@pytest.mark.parametrize("scenario", ["mixed", "invalid", "empty", "missing"])
def test_pnl_renderer_values_and_build_paths(mode, scenario):
    """Exercise real renderer and both builder paths with isolated DOM/Plotly doubles."""
    script = Path(__file__).with_name("dashboard_frontend_regressions.browser.js")
    result = run_javascript(
        [str(script), "pnl", mode, scenario],
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
