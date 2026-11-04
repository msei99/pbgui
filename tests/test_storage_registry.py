"""Tests for storage categories, scanning, filters and safe deletion."""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import psutil
import pytest

import storage_registry as registry

DAY = 86400


def _file(path: Path, size: int = 4096, age_days: float = 0) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x" * size)
    stamp = time.time() - age_days * DAY
    os.utime(path, (stamp, stamp))
    return path


def _age_dir(path: Path, age_days: float) -> None:
    stamp = time.time() - age_days * DAY
    for root, dirs, files in os.walk(path):
        for name in files + dirs:
            os.utime(os.path.join(root, name), (stamp, stamp), follow_symlinks=False)
    os.utime(path, (stamp, stamp))


@pytest.fixture()
def layout(tmp_path, monkeypatch):
    """Build an isolated PBGui/PB7/PB8 tree and a context pointing to it."""
    monkeypatch.setattr(registry, "_active_task_job_ids", lambda: {"pending-job"})
    monkeypatch.setattr(registry, "_pb_running", lambda ctx, base, scripts: False)
    home = tmp_path / "home"
    (home / ".config" / "systemd" / "user").mkdir(parents=True)
    monkeypatch.setattr(Path, "home", lambda: home)
    root = tmp_path / "software"
    pbgui = root / "pbgui"
    data = pbgui / "data"
    _file(data / "logs" / "PBGui.log")
    _file(data / "logs" / "PBGui.log.1", age_days=10)
    _file(data / "logs" / "PBGui.log.2", age_days=1)
    _file(data / "logs" / "jobs" / "old-job.log", age_days=40)
    _file(data / "logs" / "jobs" / "pending-job.log", age_days=40)
    _file(data / "logs" / "jobs" / "new-job.log", age_days=1)
    _file(data / "logs" / "optimizes_v8" / "run.log", age_days=90)
    for index in range(7):
        _file(data / "backup" / "db" / f"pbgui-2026010{index}.db", age_days=40 + index)
    _file(data / "backup" / "db-tools" / "db-tools-1-pbgui.db", age_days=50)
    _file(data / "backup" / "db-tools" / "db-tools-1-pbgui.db-wal", age_days=50)
    _file(data / "backup" / "db-repair-20260101" / "candidate.db", age_days=30)
    _file(data / "release_worktrees" / "v1" / "README.md")
    _file(data / "instances (Kopie)" / "a.json")
    _file(data / "instances" / "a.json")
    _file(data / "pbgui.db")
    _file(data / "ohlcv" / "binance" / "BTC.npy", size=8192)
    pb7 = root / "pb7"
    _file(pb7 / "historical_data" / "ohlcvs_bybit" / "BTC" / "2024-01-01.npy", size=16384)
    _file(pb7 / "caches" / "ohlcv" / "bybit" / "1m" / "BTC" / "a.npy")
    _file(pb7 / "caches" / "ohlcv" / "bybit_old" / "1m" / "x.npy")
    _file(pb7 / "caches" / "hlcvs_data" / "old" / "hlcvs.npy.gz")
    _age_dir(pb7 / "caches" / "hlcvs_data" / "old", 20)
    _file(pb7 / "optimize_results" / "old_run" / "all_results.bin", size=32768)
    _age_dir(pb7 / "optimize_results" / "old_run", 60)
    _file(pb7 / "optimize_results" / "active_run" / "all_results.bin")
    _file(pb7 / "backtests" / "pbgui" / "cfg" / "result.json")
    pb8 = root / "pb8"
    _file(pb8 / "optimize_results" / "run" / "pareto.json")
    for name in ("venv_pb6", "venv_other"):
        _file(root / name / "pyvenv.cfg")
    ctx = registry.StorageContext(pbgui=pbgui, pb7=pb7, pb8=pb8)
    return ctx


def _listing(ctx, category_id):
    return registry.list_category(ctx, registry.CATEGORY_BY_ID[category_id])


def _ids(listing):
    return {item["id"] for item in listing["items"]}


def test_scan_builds_roots_trees_and_categories(layout):
    """A scan measures every root and lists every category."""
    snapshot = registry.scan(layout)
    roots = {root["id"]: root for root in snapshot["roots"]}
    assert {"pbgui", "pb7", "pb8", "venvs"} <= roots.keys()
    assert roots["pb7"]["tree"]["size"] >= 16384 + 32768
    assert set(snapshot["categories"]) == {category.id for category in registry.CATEGORIES}
    assert snapshot["mounts"] and snapshot["mounts"][0]["total"] > 0
    venv_names = {child["name"] for child in roots["venvs"]["tree"]["children"]}
    assert "venv_pb6" in venv_names and "venv_other" not in venv_names


def test_rotated_and_job_logs(layout):
    """Rotated generations and job logs are listed; active job logs are kept."""
    assert _ids(_listing(layout, "logs_rotated")) == {"PBGui.log.1", "PBGui.log.2"}
    jobs = _listing(layout, "logs_jobs")
    blocked = {item["id"]: item["blocked"] for item in jobs["items"]}
    assert blocked["pending-job.log"] and not blocked["old-job.log"]
    selected = registry.select_items(jobs["items"], now=time.time(), days=30)
    assert [item["id"] for item in selected] == ["old-job.log"]


def test_keep_newest_and_age_filter(layout):
    """Keep-newest protects the newest backups even when they are old."""
    items = _listing(layout, "db_backups")["items"]
    selected = registry.select_items(items, now=time.time(), days=30, keep=5)
    assert len(items) == 7 and len(selected) == 2
    newest = sorted(items, key=lambda item: item["sort_mtime"], reverse=True)[:5]
    assert not {item["id"] for item in newest} & {item["id"] for item in selected}


def test_db_tools_companion_files_are_one_entry(layout):
    """SQLite -wal/-shm companions are deleted together with their database."""
    items = _listing(layout, "db_tools_backups")["items"]
    assert len(items) == 1 and len(items[0]["paths"]) == 2


def test_results_changed_recently_are_blocked(layout):
    """Folders changed within 24 hours cannot be selected."""
    items = {item["id"]: item for item in _listing(layout, "pb7_optimize_results")["items"]}
    assert items["active_run"]["blocked"]
    assert not items["old_run"]["blocked"]
    assert registry.select_items(list(items.values()), now=time.time(), item_ids=["active_run", "old_run"]) == [items["old_run"]]


def test_pb7_legacy_requires_finished_migration(layout):
    """historical_data stays blocked until PB7 marked its migration done."""
    assert _listing(layout, "pb7_legacy_history")["blocked"]
    _file(layout.pb7 / "caches" / "ohlcv" / ".migration_done", size=10)
    listing = _listing(layout, "pb7_legacy_history")
    assert not listing["blocked"] and _ids(listing) == {"ohlcvs_bybit"}


def test_pb7_cache_leftovers_and_hlcvs_guard(layout, monkeypatch):
    """Only renamed exchange folders are leftovers; hlcvs is blocked while PB7 runs."""
    assert _ids(_listing(layout, "pb7_cache_leftovers")) == {"bybit_old"}
    monkeypatch.setattr(registry, "_pb_running", lambda ctx, base, scripts: True)
    assert _listing(layout, "pb7_hlcvs")["blocked"]


def test_copies_worktrees_and_protected(layout):
    """Manual copies are detected; protected categories expose no deletable items."""
    assert _ids(_listing(layout, "manual_copies")) == {"instances (Kopie)"}
    assert _ids(_listing(layout, "release_worktrees")) == {"v1"}
    for category_id in ("protected_core", "protected_market"):
        items = _listing(layout, category_id)["items"]
        assert items and all(item["blocked"] for item in items)
        assert registry.select_items(items, now=time.time()) == []


def test_venvs_in_use_are_blocked(layout, monkeypatch):
    """Only PB/PBGui environments are offered; used ones are blocked."""
    pb6 = layout.pbgui.parent / "venv_pb6"
    monkeypatch.setattr(registry, "venv_usage", lambda ctx: {os.path.realpath(pb6): "Used by PBGui"})
    items = _listing(layout, "old_venvs")["items"]
    assert [(item["id"], bool(item["blocked"])) for item in items] == [("venv_pb6", True)]


def _start_venv_process(venv: Path):
    """Run a sleeping interpreter through a venv-style ``bin/python`` symlink."""
    (venv / "bin").mkdir(exist_ok=True)
    python = venv / "bin" / "python"
    python.symlink_to(os.path.realpath(sys.executable))
    env = {key: value for key, value in os.environ.items() if key != "VIRTUAL_ENV"}
    proc = subprocess.Popen([str(python), "-c", "import time; time.sleep(60)"], env=env)
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            if psutil.Process(proc.pid).cmdline():
                break
        except psutil.Error:
            pass
        time.sleep(0.05)
    return proc


def test_running_process_through_venv_symlink_blocks_venv(layout):
    """A process started via a venv's symlinked python keeps that venv in use.

    Its executable resolves to the system interpreter, so the venv is only
    visible in the command line.
    """
    pb6 = layout.pbgui.parent / "venv_pb6"
    proc = _start_venv_process(pb6)
    try:
        assert registry.venv_usage(layout).get(os.path.realpath(pb6)) == "Used by a running process"
        item = _listing(layout, "old_venvs")["items"][0]
        assert item["id"] == "venv_pb6" and item["blocked"]
        with pytest.raises(registry.DeleteError):
            registry.delete_item(layout, registry.CATEGORY_BY_ID["old_venvs"], dict(item, blocked=None),
                                 registry.TrashRegistry(layout.pbgui / "state"))
        assert pb6.exists()
    finally:
        proc.kill()
        proc.wait()


class _FakeProc:
    def __init__(self, exe="", cmdline=None, cwd="", environ=None):
        self.info = {"exe": exe, "cmdline": cmdline or [], "cwd": cwd}
        self._environ = environ

    def environ(self):
        if self._environ is None:
            raise psutil.AccessDenied()
        return self._environ


def test_process_venvs_uses_argv_cwd_and_virtual_env(tmp_path):
    """Interpreter paths keep their last symlink; relative argv and VIRTUAL_ENV count."""
    venv = tmp_path / "venv_pb6"
    (venv / "bin").mkdir(parents=True)
    root = os.path.realpath(venv)
    roots = [root]
    assert registry._process_venvs(_FakeProc(exe="/usr/bin/python3", cmdline=[str(venv / "bin" / "python")]), roots) == roots
    assert registry._process_venvs(_FakeProc(cmdline=["bin/python", "x.py"], cwd=str(venv)), roots) == roots
    assert registry._process_venvs(_FakeProc(cmdline=["python"], environ={"VIRTUAL_ENV": str(venv)}), roots) == roots
    assert registry._process_venvs(_FakeProc(exe="/usr/bin/python3", cmdline=["python", "x.py"], cwd=str(venv)), roots) == []
    assert registry._process_venvs(_FakeProc(cmdline=[str(tmp_path / "venv_pb6_other" / "bin" / "python")]), roots) == []


def test_vast_listing_without_jobs_is_unavailable(layout):
    """A missing cloud job folder is reported as not present, not as an error."""
    listing = _listing(layout, "vast_inactive")
    assert listing["items"] == [] and listing["available"] is False and not listing["blocked"]


def test_delete_path_refuses_outside_and_root(layout, tmp_path):
    """Deletion only works strictly below an approved root."""
    trash = registry.TrashRegistry(tmp_path / "state")
    root = layout.data / "backup" / "db"
    outside = _file(tmp_path / "outside.txt")
    with pytest.raises(registry.DeleteError):
        registry.delete_path(outside, [root], trash)
    with pytest.raises(registry.DeleteError):
        registry.delete_path(root, [root], trash)
    assert outside.exists() and root.exists()


def test_delete_path_unlinks_symlink_without_following(layout, tmp_path):
    """A symlink inside a root is removed as a link; its target survives."""
    trash = registry.TrashRegistry(tmp_path / "state")
    target_dir = tmp_path / "precious"
    _file(target_dir / "keep.txt")
    link = layout.data / "release_worktrees" / "link"
    link.symlink_to(target_dir, target_is_directory=True)
    registry.delete_path(link, [layout.data / "release_worktrees"], trash)
    assert not link.is_symlink() and (target_dir / "keep.txt").exists()


def test_directory_link_inside_tree_is_not_followed(layout, tmp_path):
    """Links inside a deleted folder are unlinked, never traversed."""
    trash = registry.TrashRegistry(tmp_path / "state")
    target_dir = tmp_path / "precious"
    _file(target_dir / "keep.txt")
    folder = layout.data / "release_worktrees" / "v1"
    (folder / "inner").symlink_to(target_dir, target_is_directory=True)
    freed, files = registry.delete_path(folder, [layout.data / "release_worktrees"], trash)
    assert not folder.exists() and (target_dir / "keep.txt").exists()
    assert freed > 0 and files >= 2
    assert trash.pending() == []


def test_interrupted_directory_deletion_finishes_later(layout, tmp_path):
    """A stop during deletion leaves registered trash that the next run removes."""
    trash = registry.TrashRegistry(tmp_path / "state")
    folder = layout.pb7 / "optimize_results" / "old_run"
    calls = {"n": 0}

    def stop_soon():
        calls["n"] += 1
        return True

    with pytest.raises(registry.ScanCancelled):
        registry.delete_path(folder, [layout.pb7 / "optimize_results"], trash, stop_soon)
    pending = trash.pending()
    assert not folder.exists() and len(pending) == 1 and pending[0].exists()
    listing = _listing(layout, "pb7_optimize_results")
    assert all(not item["id"].startswith(registry.TRASH_PREFIX) for item in listing["items"])
    registry.finish_pending_trash(trash)
    assert not pending[0].exists() and trash.pending() == []


def test_delete_item_rotated_log_and_protected(layout, tmp_path):
    """Rotated logs are deleted under the log lock; protected entries never."""
    trash = registry.TrashRegistry(tmp_path / "state")
    category = registry.CATEGORY_BY_ID["logs_rotated"]
    item = next(item for item in _listing(layout, "logs_rotated")["items"] if item["id"] == "PBGui.log.1")
    registry.delete_item(layout, category, item, trash)
    assert not (layout.data / "logs" / "PBGui.log.1").exists()
    assert (layout.data / "logs" / "PBGui.log").exists()
    protected = registry.CATEGORY_BY_ID["protected_core"]
    db_item = next(item for item in _listing(layout, "protected_core")["items"] if item["id"] == "PBGui database")
    with pytest.raises(registry.DeleteError):
        registry.delete_item(layout, protected, db_item, trash)
    assert (layout.data / "pbgui.db").exists()


def test_venv_deleter_rechecks_usage(layout, tmp_path, monkeypatch):
    """An environment that became used after the preview is not deleted."""
    trash = registry.TrashRegistry(tmp_path / "state")
    pb6 = layout.pbgui.parent / "venv_pb6"
    monkeypatch.setattr(registry, "venv_usage", lambda ctx: {})
    item = _listing(layout, "old_venvs")["items"][0]
    monkeypatch.setattr(registry, "venv_usage", lambda ctx: {os.path.realpath(pb6): "Used by PBGui"})
    with pytest.raises(registry.DeleteError):
        registry.delete_item(layout, registry.CATEGORY_BY_ID["old_venvs"], item, trash)
    assert pb6.exists()


def test_apply_deletions_updates_tree_and_items(layout):
    """Deleted items disappear from the snapshot and their size from the tree."""
    snapshot = registry.scan(layout)
    roots = {root["id"]: root for root in snapshot["roots"]}
    before = roots["pb7"]["tree"]["size"]
    item = next(item for item in snapshot["categories"]["pb7_optimize_results"]["items"] if item["id"] == "old_run")
    registry.apply_deletions(snapshot, {"pb7_optimize_results": [item]})
    assert roots["pb7"]["tree"]["size"] == before - item["size"]
    assert "old_run" not in {i["id"] for i in snapshot["categories"]["pb7_optimize_results"]["items"]}


def test_only_ids_limits_deep_statistics(layout):
    """Fresh listings walk only the selected folders recursively."""
    layout.fresh_stats = True
    layout.only_ids = {"old_run"}
    items = {item["id"]: item for item in _listing(layout, "pb7_optimize_results")["items"]}
    assert items["old_run"]["files"] == 1 and items["old_run"]["size"] >= 32768
    assert items["active_run"]["files"] == 0


def test_scan_can_be_cancelled(layout):
    """A requested stop aborts a scan without a partial result."""
    layout.stop = lambda: True
    with pytest.raises(registry.ScanCancelled):
        registry.scan(layout)


def test_apply_deletions_with_multiple_paths(layout):
    """Grouped entries subtract each path's own size from the tree."""
    snapshot = registry.scan(layout)
    roots = {root["id"]: root for root in snapshot["roots"]}
    before = roots["pbgui"]["tree"]["size"]
    item = snapshot["categories"]["db_tools_backups"]["items"][0]
    assert len(item["paths"]) == 2 and set(item["path_sizes"]) == set(item["paths"])
    registry.apply_deletions(snapshot, {"db_tools_backups": [item]})
    assert roots["pbgui"]["tree"]["size"] == before - item["size"]
