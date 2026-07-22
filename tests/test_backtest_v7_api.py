"""Regression tests for backtest_v7 runtime launch preparation."""

import asyncio
import copy
import json
import threading
from pathlib import Path

import pytest

import pbgui_purefunc
from api import backtest_v7


def test_launch_backtest_applies_pbgui_market_data_override(tmp_path, monkeypatch):
    """Backtest launch should rewrite ohlcv_source_dir to the PBGui market data root when the setting is enabled."""
    queue_dir = tmp_path / "bt_v7_queue"
    log_dir = tmp_path / "logs" / "backtests"
    queue_dir.mkdir(parents=True)
    log_dir.mkdir(parents=True)
    config_path = tmp_path / "backtest.json"
    config_path.write_text("{}", encoding="utf-8")
    config = {
        "backtest": {"ohlcv_source_dir": "/manual/data"},
    }
    captured = {}

    monkeypatch.setattr(backtest_v7, "_bt_queue_dir", lambda: queue_dir)
    monkeypatch.setattr(backtest_v7, "_bt_log_dir", lambda: log_dir)
    monkeypatch.setattr(backtest_v7, "_read_ini_section", lambda section="backtest_v7": {"use_pbgui_market_data": "True"})
    monkeypatch.setattr(backtest_v7, "load_pb7_config", lambda path: copy.deepcopy(config))
    monkeypatch.setattr(backtest_v7, "_get_pbgui_market_data_path", lambda: "/pbgui/data/ohlcv")
    monkeypatch.setattr(backtest_v7, "pb7venv", lambda: "/venv/bin/python")
    monkeypatch.setattr(backtest_v7, "pb7dir", lambda: "/tmp/pb7")

    def fake_save(cfg, path):
        captured["saved"] = (copy.deepcopy(cfg), Path(path))

    class FakePopen:
        def __init__(self, *args, **kwargs):
            captured["cmd"] = args[0]
            self.pid = 4242

    monkeypatch.setattr(backtest_v7, "save_pb7_config", fake_save)
    monkeypatch.setattr(backtest_v7.subprocess, "Popen", FakePopen)
    monkeypatch.setattr(backtest_v7.psutil, "Process", lambda _pid: type("Proc", (), {"create_time": lambda self: 123.0})())

    item = {
        "filename": "queue-btc",
        "name": "BTC",
        "json": str(config_path),
    }

    backtest_v7.BacktestWorker(backtest_v7._store)._launch_backtest(item)

    saved_cfg, saved_path = captured["saved"]
    assert saved_path == config_path, f"Expected backtest config save at {config_path}, got {saved_path}"
    assert saved_cfg["backtest"]["ohlcv_source_dir"] == "/pbgui/data/ohlcv", "Expected backtest launch to rewrite ohlcv_source_dir to the PBGui path"
    assert captured["cmd"][-1] == str(config_path), "Expected backtest launch to keep using the config path after rewriting it"
    assert (queue_dir / "queue-btc.pid").read_text(encoding="utf-8").strip() == "4242"


def test_launch_backtest_uses_queued_config_snapshot(tmp_path, monkeypatch):
    """Queued backtests should launch with the config captured at queue time, not a later editor save."""
    queue_dir = tmp_path / "bt_v7_queue"
    log_dir = tmp_path / "logs" / "backtests"
    cfg_dir = tmp_path / "saved_config"
    queue_dir.mkdir(parents=True)
    log_dir.mkdir(parents=True)
    cfg_dir.mkdir(parents=True)
    config_path = cfg_dir / "backtest.json"
    config_path.write_text("{}", encoding="utf-8")
    override_path = cfg_dir / "HYPE.json"
    override_path.write_text('{"bot": {"long": {"entry_initial_qty_pct": 0.01}}, "live": {}}\n', encoding="utf-8")
    queued_snapshot = {
        "backtest": {"starting_balance": 1000, "ohlcv_source_dir": "/manual/data"},
        "bot": {"long": {"wallet_exposure_limit": 0.1}},
        "coin_overrides": {"HYPE": {"override_config_path": "HYPE.json"}},
    }
    edited_config = {
        "backtest": {"starting_balance": 9999, "ohlcv_source_dir": "/edited/data"},
        "bot": {"long": {"wallet_exposure_limit": 0.9}},
    }
    captured = {}

    monkeypatch.setattr(backtest_v7, "_bt_queue_dir", lambda: queue_dir)
    monkeypatch.setattr(backtest_v7, "_bt_log_dir", lambda: log_dir)
    monkeypatch.setattr(backtest_v7, "_read_ini_section", lambda section="backtest_v7": {"use_pbgui_market_data": "False"})
    monkeypatch.setattr(backtest_v7, "load_pb7_config", lambda path: copy.deepcopy(edited_config))
    monkeypatch.setattr(backtest_v7, "pb7venv", lambda: "/venv/bin/python")
    monkeypatch.setattr(backtest_v7, "pb7dir", lambda: "/tmp/pb7")

    def fake_save(cfg, path):
        captured["saved"] = (copy.deepcopy(cfg), Path(path))

    class FakePopen:
        def __init__(self, *args, **kwargs):
            captured["cmd"] = args[0]
            self.pid = 4243

    monkeypatch.setattr(backtest_v7, "save_pb7_config", fake_save)
    monkeypatch.setattr(backtest_v7.subprocess, "Popen", FakePopen)
    monkeypatch.setattr(backtest_v7.psutil, "Process", lambda _pid: type("Proc", (), {"create_time": lambda self: 123.0})())

    item = {
        "filename": "queue-hype",
        "name": "HYPE",
        "json": str(config_path),
        "config_snapshot": copy.deepcopy(queued_snapshot),
    }

    backtest_v7.BacktestWorker(backtest_v7._store)._launch_backtest(item)

    saved_cfg, saved_path = captured["saved"]
    expected_path = queue_dir / "configs" / "queue-hype" / "backtest.json"
    assert saved_path == expected_path
    assert saved_cfg == queued_snapshot
    assert captured["cmd"][-1] == str(expected_path)
    assert (expected_path.parent / "HYPE.json").read_text(encoding="utf-8") == override_path.read_text(encoding="utf-8")
    assert (queue_dir / "queue-hype.pid").read_text(encoding="utf-8").strip() == "4243"


def test_read_ini_section_uses_pbgui_ini_path_when_cwd_differs(tmp_path, monkeypatch):
    """Backtest settings should be read from the configured PBGui INI path, not the current working directory."""
    ini_path = tmp_path / "pbgui.ini"
    ini_path.write_text(
        "[backtest_v7]\n"
        "autostart = True\n"
        "cpu = 7\n"
        "use_pbgui_market_data = True\n"
        "hlcvs_cleanup_days = 11\n",
        encoding="utf-8",
    )
    other_cwd = tmp_path / "other"
    other_cwd.mkdir()
    monkeypatch.setattr(pbgui_purefunc, "pbgui_ini_path", lambda: ini_path)
    monkeypatch.chdir(other_cwd)

    settings = backtest_v7._read_ini_section()

    assert settings["autostart"] == "True"
    assert settings["cpu"] == "7"
    assert settings["use_pbgui_market_data"] == "True"
    assert settings["hlcvs_cleanup_days"] == "11"


def test_add_to_queue_snapshots_provided_config_without_reloading_saved_path(tmp_path, monkeypatch):
    """Queueing an inline config should snapshot that request config, avoiding same-name exchange races."""
    config_dir = tmp_path / "bt_v7"
    queue_dir = tmp_path / "bt_v7_queue"
    config = {"backtest": {"exchanges": ["bybit"], "starting_balance": 1000}}

    monkeypatch.setattr(backtest_v7, "_bt_configs_dir", lambda: config_dir)
    monkeypatch.setattr(backtest_v7, "_bt_queue_dir", lambda: queue_dir)
    monkeypatch.setattr(
        backtest_v7,
        "_load_and_repair_backtest_config",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should not reload provided config")),
    )
    monkeypatch.setattr(backtest_v7._store, "notify", lambda: None)

    response = backtest_v7.add_to_queue({"name": "demo_config", "config": copy.deepcopy(config)}, session=None)

    queue_file = queue_dir / f"{response['filename']}.json"
    queue_data = json.loads(queue_file.read_text(encoding="utf-8"))
    assert queue_data["exchange"] == ["bybit"]
    assert queue_data["config_snapshot"]["backtest"]["exchanges"] == ["bybit"]
    assert queue_data["config_snapshot"]["backtest"]["base_dir"] == "backtests/pbgui/demo_config"


def test_worker_rereads_cpu_limit_while_waiting_for_slot(monkeypatch):
    """Increasing Backtest CPU slots should unblock queued work without restarting the worker."""
    class FakeStore:
        """Minimal BacktestStore double for worker-loop scheduling."""

        def __init__(self):
            self.items = {
                "running-a": {"filename": "running-a", "name": "Running A", "status": "backtesting"},
                "running-b": {"filename": "running-b", "name": "Running B", "status": "backtesting"},
                "queued-c": {"filename": "queued-c", "name": "Queued C", "status": "queued"},
            }

        async def refresh_from_disk(self):
            """Keep the in-memory queue stable during the test."""
            return None

    store = FakeStore()
    worker = backtest_v7.BacktestWorker(store)
    worker._running = True
    launched = []
    settings_calls = {"count": 0}
    sleep_calls = {"count": 0}

    def fake_read_ini_section(section="backtest_v7"):
        settings_calls["count"] += 1
        return {"autostart": "True", "cpu": "2" if settings_calls["count"] == 1 else "8"}

    async def fake_sleep(delay):
        sleep_calls["count"] += 1
        if sleep_calls["count"] > 5:
            worker._running = False
            raise RuntimeError("worker did not unblock after CPU limit changed")

    def fake_launch(item):
        launched.append(item["filename"])
        store.items[item["filename"]]["status"] = "backtesting"
        worker._running = False
        return {"pid": 42, "create_time": 123.0, "command_markers": ["backtest.py"]}

    monkeypatch.setattr(backtest_v7, "_read_ini_section", fake_read_ini_section)
    monkeypatch.setattr(backtest_v7.multiprocessing, "cpu_count", lambda: 16)
    monkeypatch.setattr(backtest_v7.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(worker, "_launch_backtest", fake_launch)
    monkeypatch.setattr(backtest_v7, "claim_backtest_slot", lambda *_args: True)
    monkeypatch.setattr(backtest_v7, "publish_backtest_process", lambda *_args: None)
    monkeypatch.setattr(backtest_v7, "_log", lambda *args, **kwargs: None)

    asyncio.run(worker._loop())

    assert launched == ["queued-c"]
    assert settings_calls["count"] >= 2


def test_delete_result_is_idempotent_for_missing_local_result(tmp_path, monkeypatch):
    """Deleting a stale local result row should succeed after background cleanup already removed it."""
    results_root = tmp_path / "pb7" / "backtests" / "pbgui"
    stale_result = results_root / "archive_retest_demo_12345678" / "combined" / "2026-06-21T20_34_35"
    monkeypatch.setattr(backtest_v7, "_bt_results_base", lambda: str(results_root))

    response = backtest_v7.delete_result(str(stale_result), session=None)

    assert response == {"ok": True, "missing": True}


def test_add_optimize_config_to_archive_uses_worker_thread(tmp_path, monkeypatch):
    """Archive exports should not block the API event loop while file/git work runs."""

    calls = []

    async def fake_to_thread(fn, *args):
        calls.append((fn, args))
        return {"ok": True, "relative_path": "pbgui/v1/optimize/demo.json"}

    archives_root = tmp_path / "archives"
    (archives_root / "demo_archive").mkdir(parents=True)
    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archives_root)
    monkeypatch.setattr(backtest_v7, "_own_archive_name", lambda: "demo_archive")
    monkeypatch.setattr(backtest_v7.asyncio, "to_thread", fake_to_thread)

    result = asyncio.run(backtest_v7.add_optimize_config_to_archive("demo_archive", {"config_name": "demo_config"}, session=None))

    assert result == {"ok": True, "relative_path": "pbgui/v1/optimize/demo.json"}
    assert calls == [(backtest_v7._add_optimize_config_to_archive_sync, ("demo_archive", "demo_config"))]


def test_add_config_to_archive_uses_worker_thread(tmp_path, monkeypatch):
    """Backtest result archive exports should not block the API event loop."""

    calls = []

    async def fake_to_thread(fn, *args):
        calls.append((fn, args))
        return {"ok": True, "relative_path": "pbgui/v1/backtests/demo"}

    archives_root = tmp_path / "archives"
    (archives_root / "demo_archive").mkdir(parents=True)
    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archives_root)
    monkeypatch.setattr(backtest_v7, "_own_archive_name", lambda: "demo_archive")
    monkeypatch.setattr(backtest_v7.asyncio, "to_thread", fake_to_thread)

    result = asyncio.run(backtest_v7.add_config_to_archive("demo_archive", {"source_path": "/tmp/result"}, session=None))

    assert result == {"ok": True, "relative_path": "pbgui/v1/backtests/demo"}
    assert calls == [(backtest_v7._add_config_to_archive_sync, ("demo_archive", "/tmp/result"))]


def test_add_config_archive_workflow_holds_one_outer_transaction(tmp_path, monkeypatch):
    """Migration, copy, status, and manifest generation share one archive transaction."""
    archive = tmp_path / "archives" / "mine"
    source = tmp_path / "results" / "run"
    archive.mkdir(parents=True)
    source.mkdir(parents=True)
    state = {"active": False, "entries": 0}

    class Transaction:
        """Track the outer archive transaction used by the workflow."""

        def __enter__(self):
            state["active"] = True
            state["entries"] += 1

        def __exit__(self, *_args):
            state["active"] = False

    def require_active(value):
        """Assert archive work remains inside the tracked transaction."""
        assert state["active"] is True
        return value

    monkeypatch.setattr(backtest_v7, "_require_own_archive", lambda *_args: archive)
    monkeypatch.setattr(
        backtest_v7,
        "_resolve_managed_backtest_result",
        lambda path, declared_version=None: (Path(path), declared_version or "v7"),
    )
    monkeypatch.setattr(backtest_v7, "archive_transaction", lambda _root: Transaction())
    monkeypatch.setattr(
        backtest_v7,
        "maybe_migrate_own_archive",
        lambda *_args, **_kwargs: require_active({"status": {"status": "current"}}),
    )
    monkeypatch.setattr(
        backtest_v7,
        "copy_backtest_result_to_archive",
        lambda *_args: require_active({"ok": True, "relative_path": "result"}),
    )
    monkeypatch.setattr(backtest_v7, "archive_migration_status", lambda _root: require_active({"status": "current"}))
    monkeypatch.setattr(backtest_v7, "rebuild_archive_manifest", lambda _root: require_active({"items": []}))
    monkeypatch.setattr(backtest_v7, "_invalidate_archive_cache", lambda _name: require_active(None))
    monkeypatch.setattr(backtest_v7, "_log", lambda *_args, **_kwargs: None)

    response = backtest_v7._add_config_to_archive_sync("mine", str(source))

    assert response["manifest"] == {"items": []}
    assert state == {"active": False, "entries": 1}


def test_delete_archive_waits_for_archive_transaction(tmp_path, monkeypatch):
    """Archive deletion cannot remove a clone while another archive transaction is active."""
    archives_root = tmp_path / "archives"
    archive = archives_root / "mine"
    archive.mkdir(parents=True)
    started = threading.Event()
    finished = threading.Event()
    errors = []
    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archives_root)

    def run_delete() -> None:
        """Attempt deletion from a competing thread."""
        started.set()
        try:
            backtest_v7.delete_archive("mine", session=None)
        except Exception as exc:
            errors.append(exc)
        finally:
            finished.set()

    with backtest_v7.archive_transaction(archive):
        thread = threading.Thread(target=run_delete)
        thread.start()
        assert started.wait(timeout=5)
        assert not finished.wait(timeout=0.1)
        assert archive.exists()

    thread.join(timeout=5)
    assert errors == []
    assert finished.is_set()
    assert not archive.exists()


def test_delete_archive_wins_before_waiting_add_without_recreating_root(tmp_path, monkeypatch):
    """A queued archive add revalidates after deletion and cannot recreate the removed root."""
    archives_root = tmp_path / "archives"
    archive = archives_root / "mine"
    source = tmp_path / "results" / "run"
    archive.mkdir(parents=True)
    source.mkdir(parents=True)
    delete_locked = threading.Event()
    release_delete = threading.Event()
    add_started = threading.Event()
    add_finished = threading.Event()
    delete_errors = []
    add_errors = []
    real_rmtree = backtest_v7.rmtree

    def blocking_rmtree(path, *args, **kwargs):
        """Hold deletion after it owns the archive transaction."""
        if Path(path) == archive:
            delete_locked.set()
            assert release_delete.wait(timeout=5)
        return real_rmtree(path, *args, **kwargs)

    def run_delete() -> None:
        """Delete the archive while holding its transaction first."""
        try:
            backtest_v7.delete_archive("mine", session=None)
        except Exception as exc:
            delete_errors.append(exc)

    def run_add() -> None:
        """Attempt an add that must wait and then fail revalidation."""
        add_started.set()
        try:
            backtest_v7._add_config_to_archive_sync("mine", str(source))
        except Exception as exc:
            add_errors.append(exc)
        finally:
            add_finished.set()

    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archives_root)
    monkeypatch.setattr(backtest_v7, "_own_archive_name", lambda: "mine")
    monkeypatch.setattr(backtest_v7, "rmtree", blocking_rmtree)
    monkeypatch.setattr(
        backtest_v7,
        "copy_backtest_result_to_archive",
        lambda *_args: (_ for _ in ()).throw(AssertionError("deleted archive must not be recreated")),
    )

    delete_thread = threading.Thread(target=run_delete)
    delete_thread.start()
    assert delete_locked.wait(timeout=5)
    add_thread = threading.Thread(target=run_add)
    add_thread.start()
    assert add_started.wait(timeout=5)
    assert not add_finished.wait(timeout=0.1)

    release_delete.set()
    delete_thread.join(timeout=5)
    add_thread.join(timeout=5)

    assert delete_errors == []
    assert len(add_errors) == 1
    assert isinstance(add_errors[0], backtest_v7.HTTPException)
    assert add_errors[0].status_code == 404
    assert not delete_thread.is_alive()
    assert not add_thread.is_alive()
    assert not archive.exists()


def test_delete_archive_wins_before_waiting_readme_without_saving_ini(tmp_path, monkeypatch):
    """A queued README save returns 404 after deletion without persisting archive settings."""
    archives_root = tmp_path / "archives"
    archive = archives_root / "mine"
    archive.mkdir(parents=True)
    delete_locked = threading.Event()
    release_delete = threading.Event()
    save_started = threading.Event()
    save_finished = threading.Event()
    delete_errors = []
    save_errors = []
    ini_saves = []
    real_rmtree = backtest_v7.rmtree

    def blocking_rmtree(path, *args, **kwargs):
        """Hold deletion after it owns the archive transaction."""
        if Path(path) == archive:
            delete_locked.set()
            assert release_delete.wait(timeout=5)
        return real_rmtree(path, *args, **kwargs)

    def run_delete() -> None:
        """Delete the archive while holding its transaction first."""
        try:
            backtest_v7.delete_archive("mine", session=None)
        except Exception as exc:
            delete_errors.append(exc)

    def run_save() -> None:
        """Attempt a README-bearing settings save after deletion begins."""
        save_started.set()
        try:
            backtest_v7.save_archive_settings(
                {"my_archive": "mine", "username": "new-user", "readme_title": "Mine"},
                session=None,
            )
        except Exception as exc:
            save_errors.append(exc)
        finally:
            save_finished.set()

    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archives_root)
    monkeypatch.setattr(backtest_v7, "rmtree", blocking_rmtree)
    monkeypatch.setattr(backtest_v7, "save_ini_section", lambda *args: ini_saves.append(args))

    delete_thread = threading.Thread(target=run_delete)
    delete_thread.start()
    assert delete_locked.wait(timeout=5)
    save_thread = threading.Thread(target=run_save)
    save_thread.start()
    assert save_started.wait(timeout=5)
    assert not save_finished.wait(timeout=0.1)

    release_delete.set()
    delete_thread.join(timeout=5)
    save_thread.join(timeout=5)

    assert delete_errors == []
    assert len(save_errors) == 1
    assert isinstance(save_errors[0], backtest_v7.HTTPException)
    assert save_errors[0].status_code == 404
    assert ini_saves == []
    assert not delete_thread.is_alive()
    assert not save_thread.is_alive()
    assert not archive.exists()


def test_optimize_import_waits_for_archive_transaction_before_parsing(tmp_path, monkeypatch):
    """Optimize import source parsing cannot race a pull that owns the archive lock."""
    archives_root = tmp_path / "archives"
    archive = archives_root / "other"
    source = archive / "pbgui/configs/v7.12.0/optimize/demo.json"
    source.parent.mkdir(parents=True)
    source.write_text('{"config_version":"v7.12.0","backtest":{},"optimize":{}}', encoding="utf-8")
    local_configs = tmp_path / "local-optimize"
    parsed = threading.Event()
    finished = threading.Event()
    errors = []
    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: archives_root)
    monkeypatch.setattr(backtest_v7, "_opt_archive_configs_dir", lambda: local_configs)
    monkeypatch.setattr(backtest_v7, "get_template_config", lambda: {"config_version": "v7.12.0"})

    def fake_load(path, **_kwargs):
        """Record when parsing begins and load the private source snapshot."""
        parsed.set()
        return json.loads(Path(path).read_text(encoding="utf-8"))

    monkeypatch.setattr(backtest_v7, "load_pb7_config", fake_load)
    monkeypatch.setattr(
        backtest_v7,
        "save_pb7_config",
        lambda config, path: Path(path).parent.mkdir(parents=True, exist_ok=True)
        or Path(path).write_text(json.dumps(config), encoding="utf-8"),
    )

    def run_import() -> None:
        """Attempt an import from a competing thread."""
        try:
            backtest_v7.import_archive_optimize_config(
                "other", {"path": str(source), "name": "demo", "collision": "error"}, session=None
            )
        except Exception as exc:
            errors.append(exc)
        finally:
            finished.set()

    with backtest_v7.archive_transaction(archive):
        thread = threading.Thread(target=run_import)
        thread.start()
        assert not parsed.wait(timeout=0.1)
        assert not finished.is_set()

    thread.join(timeout=5)
    assert errors == []
    assert parsed.is_set()
    assert finished.is_set()
    assert (local_configs / "demo.json").exists()


def test_explicit_failure_only_migration_does_not_rebuild_manifest(tmp_path, monkeypatch):
    """An explicit migration failure leaves archive-generated files untouched for retry."""
    archive = tmp_path / "archives" / "mine"
    archive.mkdir(parents=True)
    rebuilt = []
    monkeypatch.setattr(backtest_v7, "_require_own_archive", lambda *_args: archive)
    monkeypatch.setattr(
        backtest_v7,
        "migrate_archive_layout",
        lambda _root: {
            "ok": False,
            "skipped": False,
            "migrated": 0,
            "removed_duplicates": 0,
            "failed": 1,
            "remaining_legacy": True,
        },
    )
    monkeypatch.setattr(backtest_v7, "rebuild_archive_manifest", lambda root: rebuilt.append(root))
    monkeypatch.setattr(backtest_v7, "_invalidate_archive_cache", lambda _name: None)

    response = backtest_v7.migrate_archive("mine", session=None)

    assert response["failed"] == 1
    assert "manifest" not in response
    assert rebuilt == []


def test_archive_rebacktest_queues_one_backtest_per_selected_exchange(tmp_path, monkeypatch):
    """Archive rebacktest parameter selection should split multiple exchanges into separate queue items."""
    archive_dir = tmp_path / "archives" / "demo"
    result_dir = archive_dir / "pbgui" / "v1" / "backtests" / "demo_config" / "bybit" / "2026-07-08T00_00_00"
    result_dir.mkdir(parents=True)
    (result_dir / "config.json").write_text("{}", encoding="utf-8")
    base_config = {
        "backtest": {
            "base_dir": "backtests/pbgui/demo_config",
            "exchanges": ["combined"],
            "starting_balance": 1000,
        }
    }
    queued = []

    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: tmp_path / "archives")
    monkeypatch.setattr(backtest_v7, "load_pb7_config", lambda *args, **kwargs: copy.deepcopy(base_config))

    def fake_add_to_queue(body, session=None):
        queued.append(copy.deepcopy(body))
        return {"ok": True, "filename": f"queue-{len(queued)}"}

    monkeypatch.setattr(backtest_v7, "add_to_queue", fake_add_to_queue)

    response = backtest_v7.rebacktest_archive_results(
        "demo",
        {
            "paths": [str(result_dir)],
            "overrides": {
                "start_date": "2020-01-01",
                "end_date": "2026-07-08",
                "starting_balance": 2000,
                "exchanges": ["bybit", "hyperliquid"],
            },
        },
        session=None,
    )

    assert response["queued"] == 2
    assert [item["config"]["backtest"]["exchanges"] for item in queued] == [["bybit"], ["hyperliquid"]]
    assert [item["name"] for item in queued] == ["demo_config", "demo_config"]
    assert all(item["config"]["backtest"]["starting_balance"] == 2000 for item in queued)


def test_backtest_worker_stop_awaits_task_and_allows_restart() -> None:
    """Controller shutdown must clear its task so startup can create a new controller."""

    async def scenario() -> None:
        worker = backtest_v7.BacktestWorker(backtest_v7._store)
        worker._running = True
        worker._task = asyncio.create_task(asyncio.sleep(60))
        old_task = worker._task

        await worker.stop()

        assert old_task.done()
        assert worker._task is None
        worker.start()
        assert worker._task is not None
        await worker.stop()

    asyncio.run(scenario())


def test_archive_sync_stop_waits_for_active_file_work(monkeypatch) -> None:
    """Archive shutdown must not abandon an executor pull before restarting."""
    import threading

    started = threading.Event()
    release = threading.Event()

    def fake_pull() -> list:
        started.set()
        release.wait(timeout=5)
        return []

    async def scenario() -> None:
        worker = backtest_v7.ArchiveSyncWorker()
        monkeypatch.setattr(backtest_v7, "_read_auto_pull_interval", lambda: 1)
        monkeypatch.setattr(backtest_v7, "_pull_all_archives_sync", fake_pull)
        worker.start()
        assert await asyncio.to_thread(started.wait, 2)
        stop_task = asyncio.create_task(worker.stop())
        await asyncio.sleep(0)
        assert not stop_task.done()
        release.set()
        await stop_task
        assert worker._task is None

    asyncio.run(scenario())


@pytest.mark.parametrize("use_pbgui_market_data", [True, False])
def test_archive_rebacktest_forwards_explicit_market_data_choice(
    use_pbgui_market_data,
    tmp_path,
    monkeypatch,
) -> None:
    """Archive rebacktest forwards either explicit market-data boolean to every queue item."""
    archive_dir = tmp_path / "archives" / "demo"
    result_dir = archive_dir / "pbgui/configs/v7.12.0/backtests/demo_config/bybit/result"
    result_dir.mkdir(parents=True)
    (result_dir / "config.json").write_text("{}", encoding="utf-8")
    config = {
        "backtest": {
            "base_dir": "backtests/pbgui/demo_config",
            "exchanges": ["bybit"],
            "ohlcv_source_dir": "/archive/data",
        }
    }
    queued = []
    monkeypatch.setattr(backtest_v7, "_archives_dir", lambda: tmp_path / "archives")
    monkeypatch.setattr(backtest_v7, "load_pb7_config", lambda *args, **kwargs: copy.deepcopy(config))
    monkeypatch.setattr(
        backtest_v7,
        "_apply_pbgui_market_data_override",
        lambda cfg, enabled: (False, "/pbgui/data") if enabled else (False, ""),
    )
    monkeypatch.setattr(
        backtest_v7,
        "add_to_queue",
        lambda body, session=None: queued.append(copy.deepcopy(body)) or {"ok": True, "filename": "queued"},
    )

    response = backtest_v7.rebacktest_archive_results(
        "demo",
        {
            "paths": [str(result_dir)],
            "overrides": {"use_pbgui_market_data": use_pbgui_market_data},
        },
        session=None,
    )

    assert response["queued"] == 1
    assert queued[0]["use_pbgui_market_data"] is use_pbgui_market_data


@pytest.mark.parametrize(
    ("source_path", "expected_path"),
    [("/pbgui/data/ohlcv", None), ("/custom/ohlcv", "/custom/ohlcv")],
    ids=["clears-managed-path", "preserves-custom-path"],
)
def test_backtest_worker_explicit_false_only_clears_managed_market_data_path(
    source_path,
    expected_path,
    tmp_path,
    monkeypatch,
) -> None:
    """An explicit false removes PBGui's path while preserving a custom OHLCV source."""
    queue_dir = tmp_path / "queue"
    log_dir = tmp_path / "logs"
    source_config = tmp_path / "source" / "backtest.json"
    source_config.parent.mkdir(parents=True)
    source_config.write_text("{}", encoding="utf-8")
    launched = {}
    saved = {}
    monkeypatch.setattr(backtest_v7, "_bt_queue_dir", lambda: queue_dir)
    monkeypatch.setattr(backtest_v7, "_bt_log_dir", lambda: log_dir)
    monkeypatch.setattr(backtest_v7, "_read_ini_section", lambda section="backtest_v7": {"use_pbgui_market_data": "True"})
    monkeypatch.setattr(backtest_v7, "pb7venv", lambda: "/venv/bin/python")
    monkeypatch.setattr(backtest_v7, "pb7dir", lambda: "/tmp/pb7")
    monkeypatch.setattr(backtest_v7, "_get_pbgui_market_data_path", lambda: "/pbgui/data/ohlcv")
    monkeypatch.setattr(
        backtest_v7,
        "save_pb7_config",
        lambda config, path: saved.update(config=copy.deepcopy(config), path=Path(path)),
    )

    class FakePopen:
        """Capture a fully mocked worker launch."""

        def __init__(self, *args, **kwargs):
            launched["cmd"] = args[0]
            self.pid = 5150

    monkeypatch.setattr(backtest_v7.subprocess, "Popen", FakePopen)
    monkeypatch.setattr(backtest_v7.psutil, "Process", lambda _pid: type("Proc", (), {"create_time": lambda self: 123.0})())

    backtest_v7.BacktestWorker(backtest_v7._store)._launch_backtest(
        {
            "filename": "explicit-false",
            "name": "demo",
            "json": str(source_config),
            "config_snapshot": {"backtest": {"ohlcv_source_dir": source_path}},
            "use_pbgui_market_data": False,
        }
    )

    if expected_path is None:
        assert "ohlcv_source_dir" not in saved["config"]["backtest"]
    else:
        assert saved["config"]["backtest"]["ohlcv_source_dir"] == expected_path
    assert saved["path"] == queue_dir / "configs/explicit-false/backtest.json"
    assert launched["cmd"][-1] == str(queue_dir / "configs/explicit-false/backtest.json")
    assert (queue_dir / "explicit-false.pid").read_text(encoding="utf-8").strip() == "5150"
