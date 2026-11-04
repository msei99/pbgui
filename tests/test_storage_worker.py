"""Tests for the storage worker process logic (run in-process on isolated data)."""

from __future__ import annotations

import json
import os
import time
import uuid
from pathlib import Path

import pytest

import storage_registry as registry
import storage_worker

DAY = 86400


def _file(path: Path, size: int = 4096, age_days: float = 0) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x" * size)
    stamp = time.time() - age_days * DAY
    os.utime(path, (stamp, stamp))
    return path


def _age(path: Path, age_days: float) -> None:
    stamp = time.time() - age_days * DAY
    os.utime(path, (stamp, stamp))


@pytest.fixture()
def env(tmp_path, monkeypatch):
    """Isolated PBGui tree plus a context factory for the worker."""
    monkeypatch.setattr(registry, "_active_task_job_ids", lambda: set())
    monkeypatch.setattr(registry, "_pb_running", lambda ctx, base, scripts: False)
    monkeypatch.setattr(storage_worker, "_STOP", False)
    pbgui = tmp_path / "pbgui"
    pb7 = tmp_path / "pb7"
    for name in ("a", "b", "c"):
        _file(pbgui / "data" / "logs" / "jobs" / f"{name}.log", age_days=40)
    _file(pb7 / "optimize_results" / "run1" / "x.bin", size=8192, age_days=50)
    _file(pb7 / "optimize_results" / "run2" / "x.bin", size=8192, age_days=50)
    for name in ("run1", "run2"):
        _age(pb7 / "optimize_results" / name, 50)

    def factory(pbgdir, **kwargs):
        return registry.StorageContext(pbgui=Path(pbgdir), pb7=pb7, **kwargs)

    return {"pbgui": pbgui, "pb7": pb7, "factory": factory, "state": pbgui / "data" / "storage"}


def _job(env, mode, **payload):
    path = env["state"] / "jobs" / "job.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    job = {"id": uuid.uuid4().hex, "mode": mode, **payload}
    path.write_text(json.dumps(job))
    return path, job


def _progress(env):
    return json.loads((env["state"] / "progress.json").read_text())


def test_scan_writes_snapshot_and_progress(env):
    """A scan job stores the snapshot and reports done."""
    path, job = _job(env, "scan")
    assert storage_worker.run(path, env["pbgui"], env["factory"]) == 0
    snapshot = json.loads((env["state"] / "snapshot.json").read_text())
    assert snapshot["job_id"] == job["id"] and snapshot["version"] == registry.SNAPSHOT_VERSION
    assert _progress(env)["status"] == "done"


def test_cleanup_deletes_only_planned_items_and_updates_snapshot(env):
    """Only the plan's item ids are deleted; the snapshot loses them without a rescan."""
    storage_worker.run(_job(env, "scan")[0], env["pbgui"], env["factory"])
    path, _job_data = _job(env, "cleanup", selections=[
        {"category": "logs_jobs", "days": 30, "keep": 0, "item_ids": ["a.log", "b.log"]},
        {"category": "pb7_optimize_results", "days": 0, "keep": 0, "item_ids": ["run1"]},
    ])
    assert storage_worker.run(path, env["pbgui"], env["factory"]) == 0
    jobs = env["pbgui"] / "data" / "logs" / "jobs"
    assert sorted(p.name for p in jobs.iterdir()) == ["c.log"]
    assert not (env["pb7"] / "optimize_results" / "run1").exists()
    assert (env["pb7"] / "optimize_results" / "run2").exists()
    progress = _progress(env)
    assert progress["status"] == "done" and progress["done_items"] == 3 and progress["freed_bytes"] >= 8192
    snapshot = json.loads((env["state"] / "snapshot.json").read_text())
    remaining = {item["id"] for item in snapshot["categories"]["logs_jobs"]["items"]}
    assert remaining == {"c.log"}


def test_cleanup_rechecks_items_on_disk(env):
    """An item that changed after the preview no longer matches its age filter."""
    path, _ = _job(env, "cleanup", selections=[
        {"category": "logs_jobs", "days": 30, "keep": 0, "item_ids": ["a.log", "b.log"]},
    ])
    _file(env["pbgui"] / "data" / "logs" / "jobs" / "a.log")  # rewritten now
    assert storage_worker.run(path, env["pbgui"], env["factory"]) == 0
    jobs = env["pbgui"] / "data" / "logs" / "jobs"
    assert (jobs / "a.log").exists() and not (jobs / "b.log").exists()
    progress = _progress(env)
    assert progress["skipped_items"] == 1 and progress["results"]["logs_jobs"]["skipped"] == 1


def test_cleanup_rechecks_each_item_right_before_deletion(env, monkeypatch):
    """A folder that gets new results while an earlier folder is deleted is kept."""
    path, _ = _job(env, "cleanup", selections=[
        {"category": "pb7_optimize_results", "days": 0, "keep": 0, "item_ids": ["run1", "run2"]},
    ])
    results = env["pb7"] / "optimize_results"
    original = registry.delete_item
    order = []

    def slow_delete(ctx, category, item, trash):
        if not order:
            other = "run2" if item["id"] == "run1" else "run1"
            _file(results / other / "new_result.bin")  # written while the first folder is deleted
        order.append(item["id"])
        return original(ctx, category, item, trash)

    monkeypatch.setattr(registry, "delete_item", slow_delete)
    assert storage_worker.run(path, env["pbgui"], env["factory"]) == 0
    assert len(order) == 1 and not (results / order[0]).exists()
    other = "run2" if order[0] == "run1" else "run1"
    assert (results / other / "new_result.bin").exists() and (results / other / "x.bin").exists()
    progress = _progress(env)
    assert progress["done_items"] == 1 and progress["skipped_items"] == 1
    assert progress["results"]["pb7_optimize_results"]["skipped"] == 1


def test_cleanup_rechecks_blockers_that_do_not_touch_the_item(env, monkeypatch):
    """A job queued during the cleanup keeps its unchanged log."""
    path, _ = _job(env, "cleanup", selections=[
        {"category": "logs_jobs", "days": 30, "keep": 0, "item_ids": ["a.log", "b.log"]},
    ])
    original = registry.delete_item

    def queue_job(ctx, category, item, trash):
        if item["id"] == "a.log":
            monkeypatch.setattr(registry, "_active_task_job_ids", lambda: {"b"})
        return original(ctx, category, item, trash)

    monkeypatch.setattr(registry, "delete_item", queue_job)
    assert storage_worker.run(path, env["pbgui"], env["factory"]) == 0
    jobs = env["pbgui"] / "data" / "logs" / "jobs"
    assert not (jobs / "a.log").exists() and (jobs / "b.log").exists()
    progress = _progress(env)
    assert progress["skipped_items"] == 1 and progress["results"]["logs_jobs"]["failed"] == 0


def test_cleanup_stops_at_pb7_backtest_started_between_deletions(env, monkeypatch):
    """Unchanged PB7 datasets are kept once a backtest starts during the cleanup."""
    hlcvs = env["pb7"] / "caches" / "hlcvs_data"
    for name in ("set1", "set2", "set3"):
        _file(hlcvs / name / "hlcvs.npy", age_days=20)
        _age(hlcvs / name, 20)
    path, _ = _job(env, "cleanup", selections=[
        {"category": "pb7_hlcvs", "days": 7, "keep": 0, "item_ids": None},
    ])
    original = registry.delete_item
    order = []

    def start_backtest(ctx, category, item, trash):
        freed = original(ctx, category, item, trash)
        order.append(item["id"])
        monkeypatch.setattr(registry, "_pb_running", lambda ctx, base, scripts: True)
        return freed

    monkeypatch.setattr(registry, "delete_item", start_backtest)
    assert storage_worker.run(path, env["pbgui"], env["factory"]) == 0
    assert len(order) == 1 and sorted(p.name for p in hlcvs.iterdir()) == sorted({"set1", "set2", "set3"} - set(order))
    progress = _progress(env)
    assert progress["done_items"] == 1 and progress["skipped_items"] == 2
    assert progress["results"]["pb7_hlcvs"]["failed"] == 0 and not progress["errors"]


def test_cleanup_ignores_protected_and_unknown_categories(env):
    """Hand-written jobs cannot target protected or unknown categories."""
    _file(env["pbgui"] / "data" / "pbgui.db")
    path, _ = _job(env, "cleanup", selections=[
        {"category": "protected_core", "item_ids": ["PBGui database"]},
        {"category": "../etc", "item_ids": ["x"]},
    ])
    assert storage_worker.run(path, env["pbgui"], env["factory"]) == 0
    assert (env["pbgui"] / "data" / "pbgui.db").exists()


def test_auto_mode_applies_only_enabled_auto_rules(env):
    """Automatic cleanup scans first and applies enabled auto-allowed rules only."""
    path, _ = _job(env, "auto", rules={
        "logs_jobs": {"enabled": True, "days": 30, "keep": 0},
        "pb7_optimize_results": {"enabled": True, "days": 1, "keep": 0},
    })
    assert storage_worker.run(path, env["pbgui"], env["factory"]) == 0
    assert list((env["pbgui"] / "data" / "logs" / "jobs").iterdir()) == []
    assert (env["pb7"] / "optimize_results" / "run1").exists()
    progress = _progress(env)
    assert progress["mode"] == "auto" and progress["done_items"] == 3


def test_second_worker_reports_busy(env):
    """Only one worker may run at a time across processes."""
    fcntl = pytest.importorskip("fcntl")
    env["state"].mkdir(parents=True, exist_ok=True)
    with (env["state"] / "worker.lock").open("a+b") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert storage_worker.run(_job(env, "scan")[0], env["pbgui"], env["factory"]) == storage_worker.EXIT_BUSY


def test_stop_request_cancels_cleanup(env, monkeypatch):
    """A SIGTERM-style stop ends the cleanup before the next item."""
    monkeypatch.setattr(storage_worker, "_STOP", True)
    path, _ = _job(env, "cleanup", selections=[{"category": "logs_jobs", "days": 30, "keep": 0, "item_ids": ["a.log"]}])
    assert storage_worker.run(path, env["pbgui"], env["factory"]) == 0
    assert _progress(env)["status"] == "cancelled"
    assert (env["pbgui"] / "data" / "logs" / "jobs" / "a.log").exists()


def _vast_job(root: Path, status: str, *, requeue_from: str | None = None, age_days: float = 30) -> str:
    identifier = uuid.uuid4().hex
    folder = root / "jobs" / identifier
    folder.mkdir(parents=True)
    os.chmod(root, 0o700)
    os.chmod(root / "jobs", 0o700)
    os.chmod(folder, 0o700)
    stamp = time.time() - age_days * DAY
    state = {"id": identifier, "status": status, "created_at": stamp, "finished_at": stamp, "rental_state": "none",
             "config_name": "cfg"}
    if requeue_from:
        state["requeue_from"] = requeue_from
    (folder / "state.json").write_text(json.dumps(state))
    os.chmod(folder / "state.json", 0o600)
    _file(folder / "input.tar.gz", size=8192, age_days=age_days)
    _age(folder, age_days)
    return identifier


def test_vast_lineages_are_listed_and_purged_together(env):
    """Retry lineages are grouped; inactive ones are purged through the Vast queue."""
    root = env["pbgui"] / "data" / "vast"
    first = _vast_job(root, "failed")
    retry = _vast_job(root, "cancelled", requeue_from=first)
    done = _vast_job(root, "completed")
    ctx = env["factory"](env["pbgui"])
    inactive = registry.list_category(ctx, registry.CATEGORY_BY_ID["vast_inactive"])
    completed = registry.list_category(ctx, registry.CATEGORY_BY_ID["vast_completed"])
    assert not inactive["blocked"], inactive
    assert [sorted(item["meta"]["job_ids"]) for item in inactive["items"]] == [sorted([first, retry])]
    assert [item["id"] for item in completed["items"]] == [done]
    item_id = inactive["items"][0]["id"]
    path, _ = _job(env, "cleanup", selections=[{"category": "vast_inactive", "days": 14, "keep": 0, "item_ids": [item_id]}])
    assert storage_worker.run(path, env["pbgui"], env["factory"]) == 0
    assert _progress(env)["errors"] == []
    assert not (root / "jobs" / first).exists() and not (root / "jobs" / retry).exists()
    assert (root / "jobs" / done).exists()
