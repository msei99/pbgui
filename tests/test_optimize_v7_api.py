"""Regression tests for optimize_v7 queue runtime tracking.

Tests cover:
- Recovering a live optimize PID from the running process list when the stored
  queue PID has already gone stale
- Stopping a recovered optimize process and clearing the stale queue PID file
- Queue config snapshot fallback when the original config path is missing
- Updating queued config references when a config is renamed/saved

Background:
PBGui launches optimize jobs as detached processes. In the reported failure
mode, the tracked PID file could point at a dead or zombie launcher while the
actual optimize.py process continued under a different PID. The queue then
showed no running job and the Stop action no longer targeted the live process.
These tests lock down the recovery and stop fallback paths.
"""

import asyncio
import copy
from pathlib import Path
import json

import pytest

import pbgui_purefunc
from api import optimize_v7


class FakeProcess:
    """Minimal psutil.Process stand-in for optimize process ownership tests."""

    def __init__(self, pid: int, *, ppid: int, cmdline: list[str], create_time: float = 1.0):
        self.pid = pid
        self._ppid = ppid
        self.info = {
            "pid": pid,
            "ppid": ppid,
            "cmdline": cmdline,
            "create_time": create_time,
        }

    def ppid(self) -> int:
        """Return the configured parent PID."""
        return self._ppid


def test_read_ini_section_uses_pbgui_ini_path_when_cwd_differs(tmp_path, monkeypatch):
    """Optimize settings should be read from the configured PBGui INI path, not the current working directory."""
    ini_path = tmp_path / "pbgui.ini"
    ini_path.write_text(
        "[optimize_v7]\n"
        "autostart = True\n"
        "cpu = 5\n",
        encoding="utf-8",
    )
    other_cwd = tmp_path / "other"
    other_cwd.mkdir()
    monkeypatch.setattr(pbgui_purefunc, "pbgui_ini_path", lambda: ini_path)
    monkeypatch.chdir(other_cwd)

    settings = optimize_v7._read_ini_section()

    assert settings["autostart"] == "True"
    assert settings["cpu"] == "5"
    assert settings["cpu_override"] == "True"


def test_update_settings_is_strict_and_uses_one_atomic_section_save(monkeypatch):
    """Shared Optimize settings reject coercion and persist all accepted keys in one transaction."""
    saved = []
    notified = []
    monkeypatch.setattr(optimize_v7.multiprocessing, "cpu_count", lambda: 8)
    monkeypatch.setattr(optimize_v7, "save_ini_section", lambda section, values: saved.append((section, values)))
    monkeypatch.setattr(optimize_v7._store, "notify", lambda: notified.append(True))

    result = optimize_v7.update_settings(
        {"autostart": True, "cpu": 99, "cpu_override": False, "use_pbgui_market_data": True},
        None,
    )

    assert result == {"ok": True}
    assert saved == [
        (
            "optimize_v7",
            {"autostart": "True", "cpu_override": "False", "use_pbgui_market_data": "True", "cpu": "8"},
        )
    ]
    assert notified == [True]

    for invalid in ({"autostart": 1}, {"cpu": True}, {"cpu": "4"}):
        with pytest.raises(optimize_v7.HTTPException) as exc_info:
            optimize_v7.update_settings(invalid, None)
        assert exc_info.value.status_code == 422
    assert len(saved) == 1


def build_process_descriptor(proc: FakeProcess, config_path: Path, log_path: Path | None = None) -> dict:
    """Build a minimal optimize process index entry for tests."""
    log_paths = set()
    if log_path is not None:
        log_paths.add(optimize_v7._normalize_process_arg_path(log_path))
    return {
        "pid": proc.pid,
        "proc": proc,
        "args": {optimize_v7._normalize_process_arg_path(config_path)},
        "log_paths": log_paths,
        "create_time": float(proc.info.get("create_time") or 0.0),
    }


@pytest.fixture
def optimize_queue_dirs(tmp_path, monkeypatch):
    """Redirect optimize queue/log paths to an isolated temporary directory."""
    queue_dir = tmp_path / "opt_v7_queue"
    log_dir = tmp_path / "logs" / "optimizes"
    queue_dir.mkdir(parents=True)
    log_dir.mkdir(parents=True)
    monkeypatch.setattr(optimize_v7, "_opt_queue_dir", lambda: queue_dir)
    monkeypatch.setattr(optimize_v7, "_opt_log_dir", lambda: log_dir)
    return queue_dir, log_dir


def test_load_queue_recovers_live_optimize_pid_from_config_path(optimize_queue_dirs, monkeypatch):
    """Queue loading should adopt a live optimize.py PID when the stored PID is stale."""
    queue_dir, _ = optimize_queue_dirs
    filename = "stale-pid-item"
    config_path = queue_dir.parent / "example_config.json"
    config_path.write_text("{}", encoding="utf-8")
    (queue_dir / f"{filename}.json").write_text(
        json.dumps({"filename": filename, "name": "example", "json": str(config_path)}),
        encoding="utf-8",
    )
    (queue_dir / f"{filename}.pid").write_text("9999", encoding="utf-8")

    live_proc = FakeProcess(
        4242,
        ppid=1,
        cmdline=["/venv/bin/python", "-u", "/pb7/src/optimize.py", str(config_path)],
        create_time=10.0,
    )
    monkeypatch.setattr(
        optimize_v7,
        "_build_optimize_process_index",
        lambda: [build_process_descriptor(live_proc, config_path)],
    )
    monkeypatch.setattr(optimize_v7._store, "_is_process_running", lambda pid: pid == 4242)

    items = optimize_v7._load_queue_sync()

    assert len(items) == 1, "Expected exactly one queue item"
    assert items[0]["pid"] == 4242, f"Expected recovered live PID 4242, got {items[0]['pid']}"
    assert items[0]["status"] == "running", f"Expected running status, got {items[0]['status']}"
    assert (queue_dir / f"{filename}.pid").read_text(encoding="utf-8").strip() == "4242"


def test_stop_queue_item_kills_recovered_live_process(optimize_queue_dirs, monkeypatch):
    """Stop should kill a recovered live optimize PID and clear the stale pidfile."""
    queue_dir, _ = optimize_queue_dirs
    filename = "stale-stop-item"
    config_path = queue_dir.parent / "example_config.json"
    config_path.write_text("{}", encoding="utf-8")
    (queue_dir / f"{filename}.json").write_text(
        json.dumps({"filename": filename, "name": "example", "json": str(config_path)}),
        encoding="utf-8",
    )
    (queue_dir / f"{filename}.pid").write_text("9999", encoding="utf-8")

    log_path = queue_dir.parent / "logs" / "optimizes" / f"{filename}.log"
    log_path.write_text("running\n", encoding="utf-8")

    live_state = {"alive": True}
    killed_pids: list[int] = []

    live_proc = FakeProcess(
        4242,
        ppid=1,
        cmdline=["/venv/bin/python", "-u", "/pb7/src/optimize.py", str(config_path)],
        create_time=10.0,
    )

    def fake_index():
        if not live_state["alive"]:
            return []
        return [build_process_descriptor(live_proc, config_path, log_path)]

    def fake_kill(pid: int):
        killed_pids.append(pid)
        live_state["alive"] = False
        return {pid}

    monkeypatch.setattr(optimize_v7, "_build_optimize_process_index", fake_index)
    monkeypatch.setattr(optimize_v7, "_kill_process_tree", fake_kill)
    monkeypatch.setattr(optimize_v7._store, "_is_process_running", lambda pid: pid == 4242)

    result = optimize_v7.stop_queue_item(filename, None)

    assert result == {"ok": True}
    assert killed_pids == [4242], f"Expected recovered pid 4242 to be killed, got {killed_pids}"
    assert not (queue_dir / f"{filename}.pid").exists(), "Expected stale pidfile to be removed after stop"


def test_duplicate_queue_items_only_owner_log_keeps_running_pid(optimize_queue_dirs, monkeypatch):
    """Only the queue row whose log file is owned by the live process may stay running."""
    queue_dir, log_dir = optimize_queue_dirs
    config_path = queue_dir.parent / "shared_config.json"
    config_path.write_text("{}", encoding="utf-8")

    owner_filename = "owner-item"
    shadow_filename = "shadow-item"
    owner_log = log_dir / f"{owner_filename}.log"
    shadow_log = log_dir / f"{shadow_filename}.log"
    owner_log.write_text("running\n", encoding="utf-8")
    shadow_log.write_text("old failure\n", encoding="utf-8")

    for filename in (owner_filename, shadow_filename):
        (queue_dir / f"{filename}.json").write_text(
            json.dumps({"filename": filename, "name": "shared", "json": str(config_path)}),
            encoding="utf-8",
        )
        (queue_dir / f"{filename}.pid").write_text("4242", encoding="utf-8")

    live_proc = FakeProcess(
        4242,
        ppid=1,
        cmdline=["/venv/bin/python", "-u", "/pb7/src/optimize.py", str(config_path)],
        create_time=10.0,
    )
    monkeypatch.setattr(
        optimize_v7,
        "_build_optimize_process_index",
        lambda: [build_process_descriptor(live_proc, config_path, owner_log)],
    )
    monkeypatch.setattr(optimize_v7._store, "_is_process_running", lambda pid: pid == 4242)

    items = {item["filename"]: item for item in optimize_v7._load_queue_sync()}

    assert items[owner_filename]["status"] == "running", f"Owner row should stay running, got {items[owner_filename]['status']}"
    assert items[owner_filename]["pid"] == 4242, f"Owner row should keep pid 4242, got {items[owner_filename]['pid']}"
    assert items[shadow_filename]["status"] == "error", f"Shadow row should fall back to error, got {items[shadow_filename]['status']}"
    assert items[shadow_filename]["pid"] is None, f"Shadow row should drop the shared pid, got {items[shadow_filename]['pid']}"
    assert not (queue_dir / f"{shadow_filename}.pid").exists(), "Shadow row pidfile should be removed"


def test_load_queue_respects_persisted_order(optimize_queue_dirs, monkeypatch):
    """Queue listing should respect persisted order instead of falling back to file name order."""
    queue_dir, _ = optimize_queue_dirs
    config_path = queue_dir.parent / "ordered.json"
    config_path.write_text("{}", encoding="utf-8")
    (queue_dir / "z-last.json").write_text(
        json.dumps({"filename": "z-last", "name": "last", "json": str(config_path), "order": 5}),
        encoding="utf-8",
    )
    (queue_dir / "a-first.json").write_text(
        json.dumps({"filename": "a-first", "name": "first", "json": str(config_path), "order": 1}),
        encoding="utf-8",
    )

    monkeypatch.setattr(optimize_v7, "_build_optimize_process_index", lambda: [])
    monkeypatch.setattr(optimize_v7, "_build_queue_config_counts", lambda qd: {})

    items = optimize_v7._load_queue_sync()

    assert [item["filename"] for item in items] == ["a-first", "z-last"], f"Unexpected queue order: {[item['filename'] for item in items]}"


def test_store_refresh_from_disk_uses_persisted_queue_order(optimize_queue_dirs, monkeypatch):
    """In-memory queue order should match persisted queue order so autostart sees the same priority."""
    queue_dir, _ = optimize_queue_dirs
    config_path = queue_dir.parent / "ordered.json"
    config_path.write_text("{}", encoding="utf-8")
    (queue_dir / "later.json").write_text(
        json.dumps({"filename": "later", "name": "later", "json": str(config_path), "order": 9}),
        encoding="utf-8",
    )
    (queue_dir / "earlier.json").write_text(
        json.dumps({"filename": "earlier", "name": "earlier", "json": str(config_path), "order": 2}),
        encoding="utf-8",
    )

    monkeypatch.setattr(optimize_v7, "_build_optimize_process_index", lambda: [])
    monkeypatch.setattr(optimize_v7, "_build_queue_config_counts", lambda qd: {})

    asyncio.run(optimize_v7._store.refresh_from_disk())

    assert list(optimize_v7._store.items) == ["earlier", "later"], f"Unexpected in-memory queue order: {list(optimize_v7._store.items)}"


def test_reorder_queue_updates_persisted_order(optimize_queue_dirs):
    """Queue reorder endpoint should rewrite persisted order for the dragged queue sequence."""
    queue_dir, _ = optimize_queue_dirs
    config_path = queue_dir.parent / "ordered.json"
    config_path.write_text("{}", encoding="utf-8")
    for index, filename in enumerate(["one", "two", "three"]):
        (queue_dir / f"{filename}.json").write_text(
            json.dumps({"filename": filename, "name": filename, "json": str(config_path), "order": index}),
            encoding="utf-8",
        )

    result = optimize_v7.reorder_queue({"filenames": ["three", "one", "two"]}, None)

    assert result == {"ok": True, "count": 3}, f"Unexpected reorder result: {result}"
    persisted = {}
    for filename in ["one", "two", "three"]:
        with open(queue_dir / f"{filename}.json", "r", encoding="utf-8") as f:
            persisted[filename] = json.load(f).get("order")
    assert persisted == {"one": 1, "two": 2, "three": 0}, f"Unexpected persisted order map: {persisted}"


def test_list_results_includes_pareto_only_result_dirs(tmp_path, monkeypatch):
    """Results listing should include completed optimize directories even without all_results.bin."""
    results_base = tmp_path / "optimize_results"
    result_dir = results_base / "2026-05-11T12_00_00_test"
    pareto_dir = result_dir / "pareto"
    pareto_dir.mkdir(parents=True)
    pareto_payload = {
        "backtest": {"base_dir": "backtests/pbgui/ParetoOnly"},
        "optimize": {},
        "metrics": {"adg_weighted_per_exposure": {"mean": 0.123}},
    }
    (pareto_dir / "seed.json").write_text(json.dumps(pareto_payload), encoding="utf-8")

    monkeypatch.setattr(optimize_v7, "_opt_results_base", lambda: results_base)

    payload = optimize_v7.list_results(None)

    assert len(payload["results"]) == 1, f"Expected pareto-only result to be listed, got {payload['results']}"
    result = payload["results"][0]
    assert result["path"] == str(result_dir), f"Expected result path {result_dir}, got {result['path']}"
    assert result["result"] == result_dir.name, f"Expected result directory name {result_dir.name}, got {result['result']}"
    assert result["name"] == "ParetoOnly", f"Expected name from pareto payload, got {result['name']}"
    assert result["pareto_count"] == 1, f"Expected one pareto config, got {result['pareto_count']}"


def test_get_queue_item_config_uses_stored_config_path(optimize_queue_dirs, monkeypatch):
    """Queue edit should load the config referenced by the queue item, not the queue display name."""
    queue_dir, _ = optimize_queue_dirs
    filename = "queue-btc"
    config_path = queue_dir.parent / "btc_real_config.json"
    config_path.write_text("{}", encoding="utf-8")
    (queue_dir / f"{filename}.json").write_text(
        json.dumps({"filename": filename, "name": "BTC", "json": str(config_path)}),
        encoding="utf-8",
    )

    captured = {}

    def fake_load(path, neutralize_added=True):
        captured["path"] = Path(path)
        return {"optimize": {}, "backtest": {}}

    monkeypatch.setattr(optimize_v7, "load_pb7_config", fake_load)
    monkeypatch.setattr(optimize_v7, "_restore_optimize_editor_backend_semantics", lambda cfg, raw_cfg: cfg)
    monkeypatch.setattr(optimize_v7, "_infer_optimize_backend_hint", lambda raw_cfg: None)

    payload = optimize_v7.get_queue_item_config(filename, None)

    assert captured["path"] == config_path, f"Expected queue config path {config_path}, got {captured['path']}"
    assert payload["name"] == "btc_real_config", f"Expected config stem name, got {payload['name']}"


def test_get_queue_item_config_falls_back_to_snapshot_when_path_missing(optimize_queue_dirs, monkeypatch):
    """Queue edit should use the embedded snapshot when the stored config path is gone."""
    queue_dir, _ = optimize_queue_dirs
    filename = "queue-btc"
    missing_path = queue_dir.parent / "BTC.json"
    snapshot = {"backtest": {"base_dir": "backtests/pbgui/BTC"}, "optimize": {"n_cpus": 4}}
    (queue_dir / f"{filename}.json").write_text(
        json.dumps(
            {
                "filename": filename,
                "name": "BTC",
                "json": str(missing_path),
                "config_snapshot": snapshot,
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(optimize_v7, "_restore_optimize_editor_backend_semantics", lambda cfg, raw_cfg: cfg)
    monkeypatch.setattr(optimize_v7, "_infer_optimize_backend_hint", lambda raw_cfg: None)

    payload = optimize_v7.get_queue_item_config(filename, None)

    assert payload["name"] == "BTC", f"Expected queue snapshot name BTC, got {payload['name']}"
    assert payload["config"]["optimize"]["n_cpus"] == 4, "Expected embedded queue snapshot to be returned"


def test_get_queue_item_config_returns_ambiguous_candidate_details(optimize_queue_dirs):
    """Ambiguous old queue items should return structured candidate details for the UI modal."""
    queue_dir, _ = optimize_queue_dirs
    config_dir = queue_dir.parent
    optimize_dir = config_dir / "opt_v7"
    optimize_dir.mkdir(parents=True, exist_ok=True)
    missing_path = optimize_dir / "BTC.json"
    (optimize_dir / "BTC_retest.json").write_text("{}", encoding="utf-8")
    (optimize_dir / "BTC_test.json").write_text("{}", encoding="utf-8")
    filename = "queue-btc"
    (queue_dir / f"{filename}.json").write_text(
        json.dumps({
            "filename": filename,
            "name": "BTC",
            "json": str(missing_path),
        }),
        encoding="utf-8",
    )

    try:
        optimize_v7.get_queue_item_config(filename, None)
        pytest.fail("Expected ambiguous queue config to raise HTTPException")
    except optimize_v7.HTTPException as exc:
        assert exc.status_code == 409, f"Expected 409 for ambiguous candidates, got {exc.status_code}"
        assert isinstance(exc.detail, dict), f"Expected structured detail payload, got {type(exc.detail).__name__}"
        assert exc.detail.get("code") == "queue_config_ambiguous", f"Unexpected detail code: {exc.detail!r}"
        assert exc.detail.get("queue_filename") == filename, f"Expected queue filename {filename}, got {exc.detail.get('queue_filename')}"
        candidate_names = [entry.get("name") for entry in exc.detail.get("candidates") or []]
        assert candidate_names == ["BTC_retest", "BTC_test"], f"Unexpected candidate names: {candidate_names}"


def test_requeue_rejects_invalid_queue_config_and_keeps_error_log(optimize_queue_dirs, monkeypatch):
    """Invalid queue configs should keep their error state instead of being reset to queued."""
    queue_dir, log_dir = optimize_queue_dirs
    filename = "queue-bad"
    config_path = queue_dir.parent / "broken.json"
    config_path.write_text("{}", encoding="utf-8")
    (queue_dir / f"{filename}.json").write_text(
        json.dumps({"filename": filename, "name": "broken", "json": str(config_path)}),
        encoding="utf-8",
    )
    log_path = log_dir / f"{filename}.log"
    log_path.write_text("Failed to load optimize config: boom\n", encoding="utf-8")

    monkeypatch.setattr(optimize_v7, "load_pb7_config", lambda *args, **kwargs: (_ for _ in ()).throw(Exception("boom")))

    try:
        optimize_v7.requeue_queue_item(filename, None)
        pytest.fail("Expected invalid queue config requeue to raise HTTPException")
    except optimize_v7.HTTPException as exc:
        assert exc.status_code == 422, f"Expected 422 for invalid queue config, got {exc.status_code}"
        assert "cannot be requeued" in str(exc.detail), f"Unexpected requeue error detail: {exc.detail!r}"

    assert log_path.exists(), "Expected invalid requeue to keep the existing error log"
    assert "boom" in log_path.read_text(encoding="utf-8"), "Expected original error log contents to survive failed requeue"


def test_repair_queue_item_config_rebinds_legacy_queue_entry(optimize_queue_dirs, monkeypatch):
    """Legacy queue rows with ambiguous matching configs should be repairable in place."""
    queue_dir, _ = optimize_queue_dirs
    optimize_dir = queue_dir.parent / "opt_v7"
    optimize_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(optimize_v7, "_opt_configs_dir", lambda: optimize_dir)
    filename = "queue-btc"
    missing_path = optimize_dir / "BTC.json"
    target_path = optimize_dir / "BTC_test.json"
    target_path.write_text("{}", encoding="utf-8")
    (optimize_dir / "BTC_retest.json").write_text("{}", encoding="utf-8")
    (queue_dir / f"{filename}.json").write_text(
        json.dumps({
            "filename": filename,
            "name": "BTC",
            "json": str(missing_path),
        }),
        encoding="utf-8",
    )

    monkeypatch.setattr(optimize_v7, "load_pb7_config", lambda path, *args, **kwargs: {"backtest": {"exchanges": ["bybit"]}, "optimize": {"n_cpus": 2}})
    monkeypatch.setattr(
        optimize_v7,
        "_load_editor_payload_from_config_path",
        lambda path, name=None: {
            "name": name or Path(path).stem,
            "config": {
                "backtest": {"exchanges": ["bybit", "binance"]},
                "optimize": {"n_cpus": 3},
            },
        },
    )

    response = optimize_v7.repair_queue_item_config(filename, {"name": "BTC_test"}, None)

    with open(queue_dir / f"{filename}.json", "r", encoding="utf-8") as f:
        repaired = json.load(f)

    assert response["ok"] is True, "Expected repair endpoint to report success"
    assert repaired["name"] == "BTC_test", f"Expected repaired queue name BTC_test, got {repaired['name']}"
    assert repaired["json"] == str(target_path), f"Expected repaired queue path {target_path}, got {repaired['json']}"
    assert repaired["config_snapshot"]["optimize"]["n_cpus"] == 3, "Expected fresh snapshot from selected config"
    assert repaired["exchange"] == ["bybit", "binance"], f"Unexpected repaired exchange list: {repaired['exchange']}"


def test_load_editor_payload_falls_back_to_template_merge_for_minimal_config(optimize_queue_dirs, monkeypatch):
    """Minimal optimize stubs should load by merging onto the optimize template before PB7 prepare."""
    queue_dir, _ = optimize_queue_dirs
    cfg_file = queue_dir.parent / "BTC_retest.json"
    cfg_file.write_text(
        json.dumps({
            "backtest": {"base_dir": "backtests/pbgui/BTC_retest"},
            "optimize": {"n_cpus": 2},
        }),
        encoding="utf-8",
    )

    captured = {}

    monkeypatch.setattr(
        optimize_v7,
        "load_pb7_config",
        lambda *args, **kwargs: (_ for _ in ()).throw(Exception("failed to format config: unknown flavor")),
    )
    monkeypatch.setattr(
        optimize_v7,
        "get_template_config",
        lambda: {
            "backtest": {"start_date": "2020-01-01", "end_date": "2024-01-01"},
            "bot": {"long": {"wallet_exposure_limit": 1.0}, "short": {"wallet_exposure_limit": 1.0}},
            "live": {"approved_coins": []},
            "optimize": {"n_cpus": 8},
        },
    )

    def fake_prepare(cfg, **kwargs):
        captured["cfg"] = copy.deepcopy(cfg)
        return copy.deepcopy(cfg)

    monkeypatch.setattr(optimize_v7, "prepare_pb7_config_dict", fake_prepare)
    monkeypatch.setattr(optimize_v7, "_restore_optimize_editor_backend_semantics", lambda cfg, raw_cfg: cfg)
    monkeypatch.setattr(optimize_v7, "_infer_optimize_backend_hint", lambda raw_cfg: None)

    payload = optimize_v7._load_editor_payload_from_config_path(cfg_file)

    assert payload["name"] == "BTC_retest", f"Expected fallback payload name BTC_retest, got {payload['name']}"
    assert captured["cfg"]["bot"]["long"]["wallet_exposure_limit"] == 1.0, "Expected template bot section to be merged in"
    assert captured["cfg"]["optimize"]["n_cpus"] == 2, "Expected minimal config values to override template defaults"
    assert payload["config"]["live"]["approved_coins"] == [], "Expected template live section to survive fallback merge"


def test_save_config_with_new_name_keeps_source_and_its_queue_reference(optimize_queue_dirs, monkeypatch):
    """Saving under a new name creates a copy without renaming the opened config."""
    queue_dir, _ = optimize_queue_dirs
    config_dir = queue_dir.parent
    monkeypatch.setattr(optimize_v7, "_opt_configs_dir", lambda: config_dir)
    old_name = "BTC"
    new_name = "BTC_retest"
    old_path = config_dir / f"{old_name}.json"
    old_path.write_text("{}", encoding="utf-8")
    (queue_dir / "queue-btc.json").write_text(
        json.dumps({"filename": "queue-btc", "name": old_name, "json": str(old_path)}),
        encoding="utf-8",
    )

    def fake_save(cfg, path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_text(json.dumps(cfg), encoding="utf-8")

    monkeypatch.setattr(optimize_v7, "save_pb7_config", fake_save)

    body = {"backtest": {}, "optimize": {"n_cpus": 2}}
    optimize_v7.save_config(new_name, body, session=None)

    with open(queue_dir / "queue-btc.json", "r", encoding="utf-8") as f:
        queue_data = json.load(f)

    new_path = config_dir / f"{new_name}.json"
    assert old_path.exists(), "Expected the opened config to remain after Save As"
    assert new_path.exists(), "Expected Save As to create the new config"
    assert queue_data["json"] == str(old_path), "Expected the source queue entry to keep its config path"
    assert queue_data["name"] == old_name, "Expected the source queue entry to keep its config name"
    assert "config_snapshot" not in queue_data, "Expected Save As not to rewrite the source queue snapshot"


def test_save_config_same_name_refreshes_queue_snapshot(optimize_queue_dirs, monkeypatch):
    """Saving in place still refreshes queue entries using that exact config."""
    queue_dir, _ = optimize_queue_dirs
    config_dir = queue_dir.parent
    monkeypatch.setattr(optimize_v7, "_opt_configs_dir", lambda: config_dir)
    name = "BTC"
    config_path = config_dir / f"{name}.json"
    config_path.write_text("{}", encoding="utf-8")
    (queue_dir / "queue-btc.json").write_text(
        json.dumps({"filename": "queue-btc", "name": name, "json": str(config_path)}),
        encoding="utf-8",
    )

    def fake_save(cfg, path):
        Path(path).write_text(json.dumps(cfg), encoding="utf-8")

    monkeypatch.setattr(optimize_v7, "save_pb7_config", fake_save)
    optimize_v7.save_config(name, {"backtest": {}, "optimize": {"n_cpus": 3}}, session=None)

    with open(queue_dir / "queue-btc.json", "r", encoding="utf-8") as f:
        queue_data = json.load(f)

    assert queue_data["json"] == str(config_path)
    assert queue_data["name"] == name
    assert queue_data["config_snapshot"]["optimize"]["n_cpus"] == 3


def test_launch_optimize_restores_missing_config_from_queue_snapshot(optimize_queue_dirs, monkeypatch):
    """Queue start should recreate a missing config file from the embedded snapshot before launch."""
    queue_dir, _ = optimize_queue_dirs
    config_path = queue_dir.parent / "BTC.json"
    snapshot = {"backtest": {"base_dir": "backtests/pbgui/BTC"}, "optimize": {"n_cpus": 3}}
    captured = {}

    monkeypatch.setattr(optimize_v7, "pb7_suite_preflight_errors", lambda cfg: [])
    monkeypatch.setattr(optimize_v7, "_resolve_optimize_seed", lambda cfg: ("none", ""))
    monkeypatch.setattr(optimize_v7, "pb7venv", lambda: "/venv/bin/python")
    monkeypatch.setattr(optimize_v7, "pb7dir", lambda: "/tmp/pb7")

    def fake_save(cfg, path):
        captured["cfg"] = copy.deepcopy(cfg)
        captured["path"] = Path(path)
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_text(json.dumps(cfg), encoding="utf-8")

    class FakePopen:
        def __init__(self, *args, **kwargs):
            captured["cmd"] = args[0]
            self.pid = 4242

    monkeypatch.setattr(optimize_v7, "save_pb7_config", fake_save)
    monkeypatch.setattr(optimize_v7.subprocess, "Popen", FakePopen)

    item = {
        "filename": "queue-btc",
        "name": "BTC",
        "json": str(config_path),
        "config_snapshot": snapshot,
    }

    optimize_v7.OptimizeWorker(optimize_v7._store)._launch_optimize(item)

    assert captured["path"] == config_path, f"Expected restored config at {config_path}, got {captured['path']}"
    assert captured["cfg"]["optimize"]["n_cpus"] == 3, "Expected queue snapshot config to be restored before launch"
    assert config_path.exists(), "Expected missing config file to be recreated from queue snapshot"


def test_launch_optimize_applies_pbgui_market_data_override(optimize_queue_dirs, monkeypatch):
    """Optimize launch should rewrite ohlcv_source_dir to the PBGui market data root when the setting is enabled."""
    queue_dir, _ = optimize_queue_dirs
    config_path = queue_dir.parent / "ETH.json"
    config_path.write_text("{}", encoding="utf-8")
    config = {
        "backtest": {"ohlcv_source_dir": "/manual/data"},
        "optimize": {"n_cpus": 2},
    }
    captured = {}

    monkeypatch.setattr(optimize_v7, "_read_ini_section", lambda section="optimize_v7": {"use_pbgui_market_data": "True"})
    monkeypatch.setattr(optimize_v7, "load_pb7_config", lambda path: copy.deepcopy(config))
    monkeypatch.setattr(optimize_v7, "_get_pbgui_market_data_path", lambda: "/pbgui/data/ohlcv")
    monkeypatch.setattr(optimize_v7, "pb7_suite_preflight_errors", lambda cfg: [])
    monkeypatch.setattr(optimize_v7, "_resolve_optimize_seed", lambda cfg: ("none", ""))
    monkeypatch.setattr(optimize_v7, "pb7venv", lambda: "/venv/bin/python")
    monkeypatch.setattr(optimize_v7, "pb7dir", lambda: "/tmp/pb7")

    def fake_save(cfg, path):
        captured.setdefault("saved", []).append((copy.deepcopy(cfg), Path(path)))

    class FakePopen:
        def __init__(self, *args, **kwargs):
            captured["cmd"] = args[0]
            self.pid = 4343

    monkeypatch.setattr(optimize_v7, "save_pb7_config", fake_save)
    monkeypatch.setattr(optimize_v7.subprocess, "Popen", FakePopen)

    item = {
        "filename": "queue-eth",
        "name": "ETH",
        "json": str(config_path),
    }

    optimize_v7.OptimizeWorker(optimize_v7._store)._launch_optimize(item)

    assert captured["saved"], "Expected optimize launch to persist the PBGui market data override"
    saved_cfg, saved_path = captured["saved"][0]
    assert saved_path == config_path, f"Expected optimize config save at {config_path}, got {saved_path}"
    assert saved_cfg["backtest"]["ohlcv_source_dir"] == "/pbgui/data/ohlcv", "Expected optimize launch to rewrite ohlcv_source_dir to the PBGui path"
    assert captured["cmd"][-1] == str(config_path), "Expected optimize launch to keep using the config path after rewriting it"


def test_launch_pareto_dash_session_omits_unsupported_pathname_prefix(tmp_path, monkeypatch):
    """PB7 pareto_dash.py does not accept --pathname-prefix, so PBGui must proxy paths itself."""

    result_dir = tmp_path / "result"
    (result_dir / "pareto").mkdir(parents=True)
    pb7_root = tmp_path / "pb7"
    script_path = pb7_root / "src" / "tools" / "pareto_dash.py"
    script_path.parent.mkdir(parents=True)
    script_path.write_text("", encoding="utf-8")
    cache_root = tmp_path / "cache"
    log_path = tmp_path / "logs" / "pareto_dash.log"
    captured: dict[str, list[str]] = {}

    class FakePopen:
        """Minimal subprocess stand-in for Pareto Dash launch tests."""

        def __init__(self, cmd, **kwargs):
            captured["cmd"] = list(cmd)
            captured["kwargs"] = kwargs

        def poll(self):
            return None

    monkeypatch.setattr(optimize_v7, "pb7dir", lambda: str(pb7_root))
    monkeypatch.setattr(optimize_v7, "pb7venv", lambda: "/venv/bin/python")
    monkeypatch.setattr(optimize_v7, "_pareto_dash_cache_root", lambda: cache_root)
    monkeypatch.setattr(optimize_v7, "_pareto_dash_log_path", lambda: log_path)
    monkeypatch.setattr(optimize_v7, "_find_free_local_port", lambda: 43210)
    monkeypatch.setattr(optimize_v7, "_wait_for_pareto_dash_ready", lambda proc, port, target_path="/": (True, ""))
    monkeypatch.setattr(optimize_v7.subprocess, "Popen", FakePopen)

    proxy_root = "/api/optimize-v7/results/pareto-dash/abc123/"
    session = optimize_v7._launch_pareto_dash_session("abc123", result_dir, proxy_root)

    assert session["port"] == 43210
    assert "--pathname-prefix" not in captured["cmd"]
    assert proxy_root not in captured["cmd"]
    assert captured["cmd"][:3] == ["/venv/bin/python", "-u", str(script_path)]


def test_pareto_dash_proxy_rewrites_root_relative_dash_urls() -> None:
    """Proxy responses should point Dash root-relative assets back through the PBGui proxy."""

    proxy_root = "/api/optimize-v7/results/pareto-dash/session-id/"
    content = (
        '<script id="_dash-config">{'
        '"requests_pathname_prefix":"\\u002f",'
        '"routes_pathname_prefix": "/",'
        '"url_base_pathname": null'
        '}</script>'
        '<script src="/_dash-component-suites/dash/deps.js"></script>'
        '<link href="/assets/style.css">'
    ).encode("utf-8")

    rewritten = optimize_v7._rewrite_pareto_dash_body(content, "text/html; charset=utf-8", proxy_root).decode("utf-8")

    assert '"requests_pathname_prefix":"/api/optimize-v7/results/pareto-dash/session-id/"' in rewritten
    assert '"routes_pathname_prefix": "/api/optimize-v7/results/pareto-dash/session-id/"' in rewritten
    assert '"url_base_pathname": "/api/optimize-v7/results/pareto-dash/session-id/"' in rewritten
    assert 'src="/api/optimize-v7/results/pareto-dash/session-id/_dash-component-suites/dash/deps.js"' in rewritten
    assert 'href="/api/optimize-v7/results/pareto-dash/session-id/assets/style.css"' in rewritten
    assert optimize_v7._rewrite_pareto_dash_location(
        "http://127.0.0.1:43210/_dash-layout",
        upstream_base="http://127.0.0.1:43210",
        proxy_root=proxy_root,
    ) == "/api/optimize-v7/results/pareto-dash/session-id/_dash-layout"


def test_optimize_shutdown_stops_dash_but_not_queue_processes(tmp_path) -> None:
    """API-owned Dash processes stop without touching detached optimize queue PIDs."""
    calls: list[str] = []

    class FakeProcess:
        def poll(self):
            return None

        def terminate(self):
            calls.append("terminate")

        def wait(self, timeout=None):
            calls.append("wait")
            return 0

    stage_root = tmp_path / "stage"
    stage_root.mkdir()
    optimize_v7._pareto_dash_sessions["session"] = {
        "process": FakeProcess(),
        "stage_root": stage_root,
    }

    asyncio.run(optimize_v7.shutdown())

    assert calls == ["terminate", "wait"]
    assert not stage_root.exists()
    assert optimize_v7._pareto_dash_sessions == {}
    optimize_v7._pareto_dash_accepting = True
