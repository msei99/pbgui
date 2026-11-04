"""Tests for the storage controller, its worker lifecycle and the Storage API."""

from __future__ import annotations

import asyncio
import json
import os
import sys
import textwrap
import time
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import storage_controller
import storage_registry as registry
from storage_controller import StorageController, StorageError

ROOT = Path(__file__).resolve().parents[1]
DAY = 86400


def _file(path: Path, size: int = 4096, age_days: float = 0) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x" * size)
    stamp = time.time() - age_days * DAY
    os.utime(path, (stamp, stamp))
    return path


def _wrapper(tmp_path: Path) -> Path:
    """Write a worker entry point that only touches the temporary tree."""
    script = tmp_path / "isolated_worker.py"
    script.write_text(textwrap.dedent(f"""
        import sys
        from pathlib import Path
        sys.path.insert(0, {str(ROOT)!r})
        import logging_helpers
        logging_helpers.LOG_ROOT = Path({str(tmp_path / 'worker-logs')!r})
        import storage_registry as registry
        import storage_worker
        import signal
        registry._active_task_job_ids = lambda: set()
        registry._pb_running = lambda ctx, base, scripts: False
        signal.signal(signal.SIGTERM, storage_worker._handle_stop)
        pbgdir = Path({str(tmp_path / 'pbgui')!r})
        factory = lambda base, **kw: registry.StorageContext(pbgui=Path(base), **kw)
        sys.exit(storage_worker.run(Path(sys.argv[1]), pbgdir, factory))
    """))
    return script


def _sleeper(tmp_path: Path) -> Path:
    """A fake worker that runs until it receives SIGTERM."""
    script = tmp_path / "sleeper.py"
    marker = tmp_path / "terminated"
    script.write_text(textwrap.dedent(f"""
        import signal, sys, time
        from pathlib import Path
        def stop(*_):
            Path({str(marker)!r}).write_text("yes")
            sys.exit(0)
        signal.signal(signal.SIGTERM, stop)
        time.sleep(120)
    """))
    return script


@pytest.fixture()
def tree(tmp_path):
    pbgui = tmp_path / "pbgui"
    for name in ("a", "b"):
        _file(pbgui / "data" / "logs" / "jobs" / f"{name}.log", age_days=40)
    _file(pbgui / "data" / "pbgui.db")
    return pbgui


async def _wait_idle(controller: StorageController, timeout: float = 30) -> None:
    deadline = time.monotonic() + timeout
    while controller.busy or (controller._monitor_task and not controller._monitor_task.done()):
        if time.monotonic() > deadline:
            raise AssertionError("storage worker did not finish")
        await asyncio.sleep(0.05)


def test_scan_preview_cleanup_end_to_end(tmp_path, tree):
    """Scan, preview and cleanup run through a real worker subprocess."""

    async def scenario():
        controller = StorageController(tree, worker_script=_wrapper(tmp_path))
        try:
            await controller.ensure_loaded()
            assert await controller.request_scan()
            await _wait_idle(controller)
            view = controller.snapshot_view()
            assert view["ready"] and view["generation"] == 1
            jobs = next(c for c in view["categories"] if c["id"] == "logs_jobs")
            assert jobs["count"] == 2 and jobs["default_count"] == 2
            plan = controller.preview([{"category": "logs_jobs", "days": 30, "keep": 0}])
            assert plan["count"] == 2 and "selections" not in plan
            await controller.start_cleanup(plan["id"])
            await _wait_idle(controller)
            assert not list((tree / "data" / "logs" / "jobs").iterdir())
            assert (tree / "data" / "pbgui.db").exists()
            status = controller.status()
            assert status["last_result"]["status"] == "done" and status["last_result"]["done_items"] == 2
            assert controller.snapshot_view()["generation"] == 2
            history = controller.history()
            assert len(history) == 1 and history[0]["roots"]["pbgui"] > 0
            with pytest.raises(StorageError) as exc:
                await controller.start_cleanup(plan["id"])
            assert exc.value.status == 410
        finally:
            await controller.shutdown()

    asyncio.run(scenario())


def test_shutdown_terminates_running_worker(tmp_path, tree):
    """Shutdown stops the owned worker deterministically and is idempotent."""

    async def scenario():
        controller = StorageController(tree, worker_script=_sleeper(tmp_path))
        controller.start()
        assert await controller.request_scan()
        assert controller.busy
        await asyncio.sleep(0.5)
        started = time.monotonic()
        await controller.shutdown()
        assert time.monotonic() - started < storage_controller.STOP_GRACE_S
        assert not controller.busy
        await controller.shutdown()
        with pytest.raises(StorageError):
            await controller._start_job("scan", {})

    asyncio.run(scenario())
    assert (tmp_path / "terminated").read_text() == "yes"


def test_second_job_is_rejected_while_busy(tmp_path, tree):
    """Only one storage job runs at a time."""

    async def scenario():
        controller = StorageController(tree, worker_script=_sleeper(tmp_path))
        try:
            assert await controller.request_scan()
            assert await controller.request_scan() is False
            with pytest.raises(storage_controller.StorageBusy):
                await controller._start_job("scan", {})
        finally:
            await controller.shutdown()

    asyncio.run(scenario())


def _loaded(tree: Path, categories: dict) -> StorageController:
    controller = StorageController(tree)
    controller._snapshot = {"version": registry.SNAPSHOT_VERSION, "finished_at": time.time(), "roots": [],
                            "mounts": [], "categories": categories}
    controller._loaded = True
    controller._generation = 1
    return controller


def _item(item_id, size=100, age_days=40, blocked=None):
    stamp = time.time() - age_days * DAY
    return {"id": item_id, "label": item_id, "paths": [f"/x/{item_id}"], "size": size, "files": 1,
            "mtime": stamp, "sort_mtime": stamp, "blocked": blocked, "meta": {}}


def test_preview_validation(tree):
    """Previews reject protected, unknown, duplicate and unselected review categories."""
    controller = _loaded(tree, {
        "logs_jobs": {"items": [_item("a"), _item("b", age_days=1), _item("c", blocked="Job is pending or running")]},
        "pb7_optimize_results": {"items": [_item("run")]},
        "protected_core": {"items": [_item("PBGui database", blocked="Protected")]},
    })
    plan = controller.preview([{"category": "logs_jobs", "days": 30}])
    assert plan["count"] == 1 and plan["categories"][0]["items"][0]["id"] == "a"
    cases = [
        ([{"category": "protected_core", "item_ids": ["PBGui database"]}], 403),
        ([{"category": "nope"}], 404),
        ([{"category": "logs_jobs"}, {"category": "logs_jobs"}], 400),
        ([{"category": "pb7_optimize_results"}], 400),
        ([{"category": "logs_jobs", "days": -1}], 400),
        ([{"category": "logs_jobs", "days": "5"}], 400),
        ([{"category": "logs_jobs", "item_ids": "a"}], 400),
        ([], 400),
    ]
    for selections, status in cases:
        with pytest.raises(StorageError) as exc:
            controller.preview(selections)
        assert exc.value.status == status, selections
    review = controller.preview([{"category": "pb7_optimize_results", "item_ids": ["run", "missing"]}])
    assert review["count"] == 1


def test_preview_requires_snapshot(tree):
    """Without a finished scan, previews and item lists report 409."""
    controller = StorageController(tree)
    with pytest.raises(StorageError) as exc:
        controller.preview([{"category": "logs_jobs"}])
    assert exc.value.status == 409


def test_plans_are_bounded_and_expire(tree, monkeypatch):
    """Plans expire after their TTL and only a bounded number is kept."""
    controller = _loaded(tree, {"logs_jobs": {"items": [_item("a")]}})
    first = controller.preview([{"category": "logs_jobs", "days": 30}])
    for _ in range(storage_controller.MAX_PLANS):
        controller.preview([{"category": "logs_jobs", "days": 30}])
    assert first["id"] not in controller._plans and len(controller._plans) == storage_controller.MAX_PLANS
    newest = next(reversed(controller._plans))
    controller._plans[newest]["created_at"] -= storage_controller.PLAN_TTL_S + 1

    async def scenario():
        with pytest.raises(StorageError) as exc:
            await controller.start_cleanup(newest)
        assert exc.value.status == 410

    asyncio.run(scenario())


def test_snapshot_view_tiers_and_category_items(tree):
    """Summaries use default filters; item lists flag the requested filter."""
    controller = _loaded(tree, {
        "logs_jobs": {"items": [_item("a", size=10), _item("b", size=20, age_days=1)]},
        "pb7_optimize_results": {"items": [_item("run", size=1000), _item("new", size=5, blocked="Changed within the last 24 hours")]},
        "protected_core": {"items": [_item("PBGui database", size=7, blocked="Protected")]},
    })
    view = controller.snapshot_view()
    assert view["tiers"]["safe"]["bytes"] == 10
    assert view["tiers"]["review"]["bytes"] == 1000
    assert view["tiers"]["protected"]["bytes"] == 7
    items = controller.category_items("logs_jobs", days=0)
    assert items["match_count"] == 2 and [row["id"] for row in items["items"]] == ["b", "a"]
    counts_only = controller.category_items("logs_jobs", days=30, limit=0)
    assert counts_only["items"] == [] and counts_only["match_count"] == 1
    with pytest.raises(StorageError):
        controller.category_items("missing")


def test_settings_validation_and_persistence(tree):
    """Settings are validated, persisted and auto rules limited to allowed categories."""
    controller = StorageController(tree)
    settings = controller.load_settings()
    assert settings["auto"]["enabled"] is False
    settings["auto"]["enabled"] = True
    settings["auto"]["rules"]["logs_jobs"] = {"enabled": True, "days": 10, "keep": 0}
    saved = controller.save_settings(settings)
    assert controller.load_settings() == saved
    bad = [
        {**settings, "scan_interval_hours": 0},
        {**settings, "auto": {**settings["auto"], "hour": 24}},
        {**settings, "auto": {**settings["auto"], "rules": {"pb7_optimize_results": {"enabled": True, "days": 1}}}},
        {**settings, "auto": {**settings["auto"], "rules": {"logs_jobs": {"enabled": True, "days": 0, "keep": 0}}}},
    ]
    for value in bad:
        with pytest.raises(StorageError):
            controller.save_settings(value)
    assert controller.load_settings() == saved


def test_auto_cleanup_runs_once_per_day(tmp_path, tree, monkeypatch):
    """The scheduler starts the automatic cleanup once per day after its hour."""
    controller = StorageController(tree)
    settings = controller.load_settings()
    settings["auto"].update({"enabled": True, "hour": 0})
    controller.save_settings(settings)
    started = []

    async def fake_start(mode, payload):
        started.append(mode)
        return {}

    monkeypatch.setattr(controller, "_start_job", fake_start)

    async def scenario():
        await controller._tick()
        await controller._tick()

    asyncio.run(scenario())
    assert started[0] == "auto" and started.count("auto") == 1


def test_warnings(tree):
    """Disk and memory thresholds produce Welcome warnings."""
    controller = StorageController(tree)
    warnings = controller.warnings(
        [{"mount": "/", "percent": 96, "free": 1}, {"mount": "/data", "percent": 86, "free": 1}, {"mount": "/x", "percent": 50}],
        {"memory": {"total": 100, "available": 5}, "swap": {"total": 10, "percent": 60}},
    )
    assert [w["level"] for w in warnings] == ["critical", "warning", "warning", "warning"]


@pytest.fixture()
def client(tree, monkeypatch):
    """API client with an isolated controller and authentication bypassed."""
    import api.storage as storage_api

    controller = _loaded(tree, {"logs_jobs": {"items": [_item("a")]}})
    monkeypatch.setattr(storage_api, "_controller", controller)
    storage_api._cache.clear()
    app = FastAPI()
    app.include_router(storage_api.router, prefix="/api/storage")
    app.dependency_overrides[storage_api.require_auth] = lambda: object()
    with TestClient(app, raise_server_exceptions=False) as test_client:
        yield test_client, controller
    storage_api._cache.clear()


def test_api_routes(client):
    """The API maps controller errors and validates bodies."""
    test_client, controller = client
    assert test_client.get("/api/storage/snapshot").json()["ready"] is True
    status = test_client.get("/api/storage/status").json()
    assert status["busy"] is False and isinstance(status["mounts"], list)
    response = test_client.post("/api/storage/preview", json={"selections": [{"category": "protected_core"}]})
    assert response.status_code == 403
    response = test_client.post("/api/storage/preview", json={"selections": [{"category": "logs_jobs", "days": 30}]})
    assert response.status_code == 200 and response.json()["count"] == 1
    assert test_client.post("/api/storage/cleanup", json={"plan_id": "unknown"}).status_code == 410
    assert test_client.get("/api/storage/categories/logs_jobs/items?days=30").json()["match_count"] == 1
    assert test_client.get("/api/storage/categories/nope/items").status_code == 404
    assert test_client.put("/api/storage/settings", json={"scan_interval_hours": 99}).status_code == 400
    assert test_client.post("/api/storage/cancel").json() == {"stopping": False}
    assert test_client.get("/api/storage/history").json() == {"history": []}


def test_api_system_and_summary(client):
    """System and summary endpoints return live values without secrets."""
    test_client, _ = client
    system = test_client.get("/api/storage/system").json()
    assert system["system"]["cpu_logical"] >= 1 and "processes" in system and "pythons" in system
    summary = test_client.get("/api/storage/summary").json()
    assert set(summary) >= {"system", "processes", "disk", "reclaimable", "warnings", "scanned_at"}
    assert "items" not in summary["processes"]


def test_api_requires_running_controller(client, monkeypatch):
    """Requests fail clearly when the controller is not running."""
    import api.storage as storage_api

    test_client, _ = client
    monkeypatch.setattr(storage_api, "_controller", None)
    assert test_client.get("/api/storage/status").status_code == 503


def test_main_page_renders_placeholders(client):
    """The page is rendered with the API base and navigation hash."""
    test_client, _ = client
    html = test_client.get("/api/storage/main_page").text
    assert "%%" not in html
    assert "current: 'system_storage'" in html and '"/api/storage"' in html


def test_api_auth_is_required(tree, monkeypatch):
    """Without a session every storage route is rejected."""
    import api.storage as storage_api

    monkeypatch.setattr(storage_api, "_controller", _loaded(tree, {}))
    app = FastAPI()
    app.include_router(storage_api.router, prefix="/api/storage")
    with TestClient(app, raise_server_exceptions=False) as test_client:
        for method, path in (("get", "/status"), ("get", "/snapshot"), ("post", "/cancel"), ("get", "/summary"),
                             ("get", "/system"), ("get", "/main_page"), ("put", "/settings")):
            response = getattr(test_client, method)("/api/storage" + path)
            assert response.status_code in (401, 403), path


def test_worker_runs_with_same_interpreter():
    """The controller spawns the worker with the API interpreter by default."""
    assert StorageController(Path(".")).python == sys.executable
    assert json.loads(json.dumps(storage_controller.default_settings()))["scan_interval_hours"] == 6


def test_busy_auto_cleanup_is_retried_the_same_day(tree):
    """An automatic run that found another worker busy does not count for today."""
    controller = StorageController(tree)
    state = tree / "data" / "storage" / "auto_state.json"
    state.parent.mkdir(parents=True, exist_ok=True)
    state.write_text(json.dumps({"last_run": time.time()}))

    async def scenario():
        await controller._finish(75, {"id": "job", "mode": "auto"}, tree / "data" / "storage" / "missing.json")

    asyncio.run(scenario())
    assert json.loads(state.read_text())["last_run"] == 0
    assert controller._busy_elsewhere_until > time.time()
    assert controller.status()["last_result"]["errors"] == ["Another storage worker is running"]


def test_cleanup_exclude_only_removes_entries(tree, monkeypatch):
    """Deselected preview entries are dropped; unknown ids cannot add entries."""
    controller = _loaded(tree, {"logs_jobs": {"items": [_item("a"), _item("b")]}})
    started = []

    async def fake_start(mode, payload):
        started.append(payload)
        return {"id": "job"}

    monkeypatch.setattr(controller, "_start_job", fake_start)

    async def scenario():
        plan = controller.preview([{"category": "logs_jobs", "days": 30}])
        await controller.start_cleanup(plan["id"], {"logs_jobs": ["a", "not-in-plan"]})
        assert started[-1]["selections"][0]["item_ids"] == ["b"]
        plan = controller.preview([{"category": "logs_jobs", "days": 30}])
        for bad in ({"logs_jobs": "a"}, {"../x": ["a"]}, ["a"], {"logs_jobs": [1]}):
            with pytest.raises(StorageError) as exc:
                await controller.start_cleanup(plan["id"], bad)
            assert exc.value.status == 400
        with pytest.raises(StorageError) as exc:
            await controller.start_cleanup(plan["id"], {"logs_jobs": ["a", "b"]})
        assert exc.value.status == 400 and plan["id"] in controller._plans

    asyncio.run(scenario())
