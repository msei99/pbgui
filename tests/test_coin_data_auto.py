"""Automatic Coin Data work uses isolated files, clocks and producer doubles."""

import importlib.util
import json
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import api.coin_data as api
from file_lock import advisory_file_lock


@pytest.fixture
def auto_env(monkeypatch, tmp_path):
    """Never touch production files or use a running daemon/exchange."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(api, "COINDATA_DIR", tmp_path / "data/coindata")
    monkeypatch.setattr(api, "_REFRESH_JOBS", {})
    monkeypatch.setattr(api, "_REFRESH_THREADS", {})
    monkeypatch.setattr(api, "_REFRESH_ACCEPTING", True)
    fake = SimpleNamespace(exchange="bybit", mapping_interval=24, fetch_interval=24,
                           metadata_interval=1, cmc_pool_status=lambda: {"active_credentials": 0})
    fake.refresh_exchange_mapping = lambda *_a, **_k: (publish("exchange:bybit") or {"ok": True})
    monkeypatch.setattr(api, "_new_coindata", lambda **_kw: fake)
    yield fake
    for thread in list(api._REFRESH_THREADS.values()):
        thread.join(5)
        assert not thread.is_alive()


def publish(resource, age=0):
    """Publish minimal synthetic data with deterministic freshness."""
    names = ([resource[9:] + "/" + name for name in ("mapping.json", "ccxt_markets.json")]
             if resource.startswith("exchange:") else
             ["coindata.json" if resource == "cmc:listings" else "metadata.json"])
    for name in names:
        path = api.COINDATA_DIR / name
        path.parent.mkdir(parents=True, exist_ok=True)
        value = ([{"symbol": "GUI406USDT"}] if name.endswith("mapping.json") else
                 {"GUI406": {}} if name.endswith("ccxt_markets.json") else {"data": [{"id": 1}]})
        path.write_text(json.dumps(value))
        os.utime(path, (api.time.time() - age, api.time.time() - age))


def wait_jobs(ids):
    """Join only workers owned by this fixture and inspect their terminal records."""
    for job_id in ids:
        thread = api._REFRESH_THREADS.get(job_id)
        if thread:
            thread.join(5)
            assert not thread.is_alive()
    return [api._get_refresh_job(job_id) for job_id in ids]


@pytest.mark.parametrize("age,expected", [(0, False), (90000, True)])
def test_ensure_uses_configured_freshness(auto_env, age, expected):
    """Fresh data starts nothing; expired data starts exactly one exchange worker."""
    publish("exchange:bybit", age)
    response = api.ensure_current(api.CoinDataRefreshRequest(exchange="bybit"))
    assert bool(response["job_ids"]) is expected
    wait_jobs(response["job_ids"])


def test_parallel_filters_share_one_resource_job(auto_env, monkeypatch):
    """Different UI filters share one worker even when called concurrently."""
    entered, release = Event(), Event()

    def refresh(*_args, **_kwargs):
        """Hold the real worker until all concurrent Ensure calls return."""
        entered.set()
        assert release.wait(5)
        publish("exchange:bybit")
        return {"ok": True}

    monkeypatch.setattr(api, "_refresh_single_exchange", refresh)
    try:
        with ThreadPoolExecutor(max_workers=8) as pool:
            responses = list(pool.map(lambda value: api.ensure_current(api.CoinDataRefreshRequest(
                exchange="bybit", market_cap=value, tags=[str(value)], quotes=["USDT"])), range(8)))
        assert entered.wait(2)
        ids = {tuple(response["job_ids"]) for response in responses}
        assert len(ids) == 1
        assert len(next(iter(ids))) == 1
        assert len(api._REFRESH_THREADS) == 1
    finally:
        release.set()
    assert wait_jobs(responses[0]["job_ids"])[0]["status"] == "completed"
    assert api.ensure_current(api.CoinDataRefreshRequest(exchange="bybit"))["job_ids"] == []


@pytest.mark.parametrize("resource", ["exchange:bybit", "cmc:listings", "cmc:metadata"])
def test_daemon_publication_is_rechecked_inside_resource_lock(auto_env, monkeypatch, resource):
    """Work queued behind a daemon publication rechecks timestamps before fetching."""
    monkeypatch.setattr(api, "_refresh_single_exchange", lambda *_a, **_k: pytest.fail("Duplicate fetch"))
    auto_env.cmc_pool_status = lambda: {"active_credentials": 1}
    for item in ("cmc:listings", "cmc:metadata", "exchange:bybit"):
        publish(item)
    publish(resource, 90000)
    with advisory_file_lock(api._auto_lock_target(resource)):
        response = api.ensure_current(api.CoinDataRefreshRequest(exchange="bybit"))
        publish(resource)
    assert wait_jobs(response["job_ids"])[0]["status"] == "completed"


def test_ensure_adopts_legacy_batch_jobs(auto_env):
    """An existing full refresh is observed instead of spawning overlapping work."""
    job_id = api._create_refresh_job("Batch", "Running", 1,
                                    resources=("exchange:bybit", "cmc:listings", "cmc:metadata"))
    assert api.ensure_current(api.CoinDataRefreshRequest(exchange="bybit"))["job_ids"] == [job_id]
    assert api._REFRESH_THREADS == {}


def test_failure_backoff_is_shared_and_no_key_does_not_block_exchange(auto_env, monkeypatch):
    """Failure publishes a private retry deadline and never starts CMC without a key."""
    calls = []

    def fail(*_a, **_k):
        """Return a producer failure without touching a real endpoint."""
        calls.append(1)
        return {"ok": False, "error": "Synthetic unavailable exchange"}

    monkeypatch.setattr(api, "_refresh_single_exchange", fail)
    first = api.ensure_current(api.CoinDataRefreshRequest(exchange="bybit"))
    assert wait_jobs(first["job_ids"])[0]["status"] == "error"
    second = api.ensure_current(api.CoinDataRefreshRequest(exchange="bybit"))
    assert second["job_ids"] == []
    assert second["retry_at"] > api.time.time()
    assert calls == [1]
    assert len(list((api.COINDATA_DIR / ".locks").glob("*.retry.json"))) == 1
    retry_file = api._auto_lock_target("exchange:bybit").with_suffix(".retry.json")
    retry = json.loads(retry_file.read_text())
    retry["next_retry_at"] = api.time.time() - 1
    retry_file.write_text(json.dumps(retry))
    third = api.ensure_current(api.CoinDataRefreshRequest(exchange="bybit"))
    wait_jobs(third["job_ids"])
    assert calls == [1, 1]


def test_ensure_route_requires_auth_and_validates_exchange(auto_env):
    """The new mutation contract uses normal authentication and rejects traversal."""
    app = FastAPI()
    app.include_router(api.router, prefix="/api/coin-data")

    def deny():
        """Avoid inspecting any real session storage."""
        raise api.HTTPException(status_code=401, detail="Synthetic unauthenticated session")

    app.dependency_overrides[api.require_auth] = deny
    with TestClient(app) as client:
        assert client.post("/api/coin-data/ensure", json={"exchange": "bybit"}).status_code == 401
        app.dependency_overrides[api.require_auth] = lambda: SimpleNamespace()
        assert client.post("/api/coin-data/ensure", json={"exchange": "../bybit"}).status_code == 422
    assert api._REFRESH_JOBS == {}


@pytest.mark.parametrize("resource", ["cmc:listings", "cmc:metadata"])
def test_cmc_due_work_uses_local_key_and_stage_lock(auto_env, monkeypatch, resource):
    """Automatic CMC stages run without a daemon and skip fresh stages."""
    publish("exchange:bybit")
    other = "cmc:metadata" if resource == "cmc:listings" else "cmc:listings"
    publish(other)
    calls = []
    auto_env.cmc_pool_status = lambda: {"active_credentials": 1, "ready": True}
    auto_env.load_data = lambda: None
    auto_env.load_metadata = lambda: None

    def fetch():
        """Publish one isolated CMC stage."""
        calls.append(resource)
        publish(resource)
        return True

    auto_env.fetch_data = fetch if resource == "cmc:listings" else lambda: pytest.fail("Fresh listings fetched")
    auto_env.fetch_metadata = fetch if resource == "cmc:metadata" else lambda: pytest.fail("Fresh metadata fetched")
    response = api.ensure_current(api.CoinDataRefreshRequest(exchange="bybit"))
    assert wait_jobs(response["job_ids"])[0]["status"] == "completed"
    assert calls == [resource]


@pytest.mark.parametrize("exchange,rows", [("bybit", []), ("hyperliquid", [{"symbol": "BTCUSDC", "quote": "USDC"}])])
def test_real_constructor_and_snapshot_never_invoke_producers(monkeypatch, tmp_path, exchange, rows):
    """Missing mappings and absent HIP-3 records stay read-only through real setters."""
    import pbgui_purefunc

    spec = importlib.util.spec_from_file_location("PBCoinData_auto_snapshot", Path(__file__).resolve().parents[1] / "PBCoinData.py")
    real = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(real)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("PBGUI_DIR", str(tmp_path))
    monkeypatch.setattr(pbgui_purefunc, "pbgui_ini_path", lambda: tmp_path / "pbgui.ini")
    monkeypatch.setattr(api, "COINDATA_DIR", tmp_path / "data/coindata")
    monkeypatch.setattr(api, "CoinData", real.CoinData)
    monkeypatch.setattr(real, "build_cmc_pool_client", lambda _root: SimpleNamespace(status=lambda: {"active_credentials": 0}))
    for name in ("fetch_data", "fetch_metadata", "fetch_ccxt_markets", "refresh_exchange_mapping", "build_mapping"):
        monkeypatch.setattr(real.CoinData, name, lambda *_a, **_k: pytest.fail("Snapshot invoked producer"))
    directory = api.COINDATA_DIR / exchange
    directory.mkdir(parents=True)
    (directory / "mapping.json").write_text(json.dumps(rows))
    app = FastAPI()
    app.include_router(api.router, prefix="/api/coin-data")
    app.dependency_overrides[api.require_auth] = lambda: SimpleNamespace()
    with TestClient(app) as client:
        explicit = client.get("/api/coin-data/state", params={"exchange": exchange, "quotes": "USDT"})
        assert explicit.status_code == 422
        assert explicit.json()["detail"].startswith("Quote not available for ")
        for _ in range(3):
            response = client.get("/api/coin-data/state", params={"exchange": exchange, "market_cap": 1})
            assert response.status_code == 200
            assert response.json()["filters"]["exchange"] == exchange
