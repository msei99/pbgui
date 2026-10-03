"""Offline tests for authoritative Dashboard candle reconciliation."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from api import dashboard


@pytest.fixture
def candle_client(monkeypatch):
    """Create an isolated authenticated route with fake exchange/cache state."""
    user = SimpleNamespace(name="alice", exchange="hyperliquid")
    monkeypatch.setattr(dashboard, "_get_users", lambda: SimpleNamespace(find_user=lambda name: user if name == "alice" else None))
    monkeypatch.setattr(dashboard, "_ohlcv_cache", {})
    monkeypatch.setattr(dashboard, "_get_db", Mock(side_effect=AssertionError("No private snapshot needed")))
    exchange = Mock()
    exchange.fetch_ohlcv.return_value = [[300000, 5, 6, 4, 5.2, 12], [600000, 5, 6, 4, 5.3, 20]]
    monkeypatch.setattr(dashboard, "_get_exchange", Mock(return_value=exchange))
    monkeypatch.setattr(dashboard, "_log", Mock())
    app = FastAPI()
    app.include_router(dashboard.router, prefix="/api/dashboard")
    app.dependency_overrides[dashboard.require_auth] = lambda: object()
    with TestClient(app) as client:
        yield client, exchange


def test_fresh_snapshot_repairs_closed_cache_preserving_current_ws(candle_client):
    """A final closed value replaces stale history without rolling back live data."""
    client, exchange = candle_client
    dashboard._ohlcv_cache_put("alice", "NEARUSDC", "5m", [[300000, 5, 6, 4, 5, 10], [600000, 5, 7, 4, 6, 30]])
    response = client.get("/api/dashboard/candles_data", params={"user": "alice", "symbol": "NEARUSDC", "timeframe": "5m"})
    assert response.status_code == 200
    assert response.json()["candles"][0]["c"] == 5.2
    exchange.fetch_ohlcv.assert_called_once_with("NEAR/USDC:USDC", "futures", timeframe="5m", limit=500)
    cached = dashboard._ohlcv_cache[("alice", "NEARUSDC", "5m")]["candles"]
    assert cached[0][-2:] == [5.2, 12]
    assert cached[-1][-2:] == [6, 30]
    dashboard._get_db.assert_not_called()


@pytest.mark.parametrize("params,status", [
    ({"user": "missing"}, 404),
    ({"timeframe": "invalid"}, 422),
    ({"limit": 1501}, 422),
    ({"limit": 0}, 422),
])
def test_snapshot_invalid_request_does_not_fetch(candle_client, params, status):
    """Unknown accounts and invalid candle dimensions fail before exchange access."""
    client, exchange = candle_client
    response = client.get("/api/dashboard/candles_data", params={"user": "alice", "symbol": "NEARUSDC", **params})
    assert response.status_code == status
    exchange.fetch_ohlcv.assert_not_called()


@pytest.mark.parametrize("error,status,detail", [
    (TimeoutError("private diagnostic"), 502, "Candle data unavailable"),
    (HTTPException(409, "Blocked"), 409, "Blocked"),
])
def test_snapshot_error_status_and_redaction(candle_client, error, status, detail):
    """Expected HTTP statuses survive; provider diagnostics are not exposed."""
    client, exchange = candle_client
    exchange.fetch_ohlcv.side_effect = error
    response = client.get("/api/dashboard/candles_data", params={"user": "alice", "symbol": "NEARUSDC"})
    assert response.status_code == status
    assert response.json()["detail"] == detail
    assert "private diagnostic" not in str(dashboard._log.call_args)


def test_snapshot_requires_authentication(monkeypatch):
    """Public market candles still require a PBGui authenticated session."""
    app = FastAPI()
    app.include_router(dashboard.router, prefix="/api/dashboard")
    monkeypatch.setattr(dashboard, "_get_users", Mock(side_effect=AssertionError("Unauthorized access")))
    with TestClient(app) as client:
        response = client.get("/api/dashboard/candles_data", params={"user": "alice", "symbol": "NEARUSDC"})
    assert response.status_code in {401, 403}
