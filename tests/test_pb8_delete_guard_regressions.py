"""Offline PB8 deletion regressions using the real matcher and fake processes."""

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from fastapi import HTTPException

import PBRun
import pb8_config
from api import v8_instances
from master.async_monitor import VPSMonitor
from master.async_store import VPSStore


@pytest.fixture
def guarded_instance(tmp_path, monkeypatch):
    """Isolate all state and prohibit config preparation, rebuilds, and real processes."""
    target = tmp_path / "data" / "run_v8" / "alice"
    target.mkdir(parents=True)
    (target / "config.json").write_text('{"pbgui":{"enabled_on":"disabled"}}', encoding="utf-8")
    stage = target.parent / ".pbgui-v8-stage-test"
    stage.mkdir()
    (stage / "untouched").write_text("pending", encoding="utf-8")
    monkeypatch.setattr(v8_instances, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(v8_instances, "_master_hostname", lambda: "local")
    monkeypatch.setattr(v8_instances.pbgui_purefunc, "pb8dir", lambda: str(tmp_path / "pb8"))
    monkeypatch.setattr(v8_instances.pbgui_purefunc, "pb8venv", lambda: str(tmp_path / "venv" / "bin" / "python"))
    monkeypatch.setattr(v8_instances.pbgui_purefunc, "pb8_runtime_status", Mock(side_effect=AssertionError("No readiness check")))
    def load_config(operation, **payload):
        """Replace only the external canonicalization process, not the public loader."""
        assert operation == "load"
        path = Path(payload["config_path"])
        assert path == target / "config.json"
        return {"config": json.loads(path.read_text(encoding="utf-8"))}

    monkeypatch.setattr(pb8_config, "_runtime_fingerprint", lambda: (str(tmp_path),))
    monkeypatch.setattr(pb8_config, "_call_helper", load_config)
    monkeypatch.setattr(PBRun.RunV8, "load", Mock(side_effect=AssertionError("No credentials/start validation")))
    monkeypatch.setattr(v8_instances, "read_materialized_state", Mock(side_effect=AssertionError("No state rebuild")))
    processes = []
    monkeypatch.setattr(PBRun.psutil, "process_iter", lambda: iter(processes))
    store = VPSStore()
    monkeypatch.setattr(v8_instances, "_monitor", SimpleNamespace(store=store, pool=SimpleNamespace(connected_hosts=lambda: ["remote"])))
    snapshot = Mock(return_value="1")
    tombstone = Mock(return_value={"op": "DELETE_PB8_INSTANCE", "op_id": "test"})
    remove = Mock()
    monkeypatch.setattr(v8_instances, "_snapshot_v8_bundle", snapshot)
    monkeypatch.setattr(v8_instances, "_record_delete", tombstone)
    monkeypatch.setattr(v8_instances.shutil, "rmtree", remove)
    monkeypatch.setattr(v8_instances, "_current_version", lambda _name: 1)
    monkeypatch.setattr(v8_instances, "_highest_cluster_version", lambda _name: 1)
    monkeypatch.setattr(v8_instances.time, "time", lambda: 1000.0)
    return SimpleNamespace(root=tmp_path, target=target, stage=stage, store=store,
                           processes=processes, snapshot=snapshot, tombstone=tombstone, remove=remove)


def _assert_blocked(state):
    """A refused delete must leave bundles, transaction stages and cluster state intact."""
    with pytest.raises(HTTPException) as error:
        v8_instances.delete_v8_instance("alice", None)
    assert error.value.status_code == 409
    state.snapshot.assert_not_called()
    state.tombstone.assert_not_called()
    state.remove.assert_not_called()
    assert (state.target / "config.json").is_file()
    assert (state.stage / "untouched").read_text(encoding="utf-8") == "pending"
    return error.value


@pytest.mark.parametrize("assignment", ["disabled", "remote", "local"])
@pytest.mark.parametrize("python_prefix", [False, True])
def test_disabled_reassigned_and_notready_live_process_blocks_delete(guarded_instance, assignment, python_prefix):
    """RunV8 exact matching detects orphan processes without loading/start-validating configs."""
    state = guarded_instance
    (state.target / "config.json").write_text(json.dumps({"pbgui": {"enabled_on": assignment}}), encoding="utf-8")
    command = [str(state.root / "venv" / "bin" / "passivbot"), "live", str(state.target / "config.json"), "--fail-on-stale-rust"]
    if python_prefix:
        command.insert(0, str(state.root / "venv" / "bin" / "python"))
    state.processes.append(SimpleNamespace(cmdline=lambda: command, cwd=lambda: str(state.root / "pb8"),
                                          create_time=lambda: 10, memory_full_info=lambda: None, cpu_percent=lambda: 0))
    assert "running locally" in _assert_blocked(state).detail


@pytest.mark.parametrize("problem", ["absent", "stale", "unknown", "connected_stale", "missing_running", "absent_instance", "nan_time", "future_time", "collector_error"])
def test_remote_assignment_requires_fresh_explicit_stopped_observation(guarded_instance, problem):
    """A connected host and cached false value are not proof that a remote bot stopped."""
    state = guarded_instance
    (state.target / "config.json").write_text('{"pbgui":{"enabled_on":"remote"}}', encoding="utf-8")
    state.store.update_v8_instances("remote", [{"name": "alice", "running": False}],
                                    snapshot_generated_at=999, snapshot_checked_at=1000)
    status = {"state": "ok", "generated_at": 999, "checked_at": 1000}
    state.store.streams["remote"] = {"monitor_agent": {"files": {"instance_snapshot.json": status}}}
    if problem == "absent":
        state.store.streams.clear()
    elif problem == "stale":
        status["state"] = "stale"
    elif problem == "unknown":
        status["state"] = "unknown"
    elif problem == "connected_stale":
        state.store.v8_instances["remote"][0]["snapshot_generated_at"] = 1
    elif problem == "missing_running":
        del state.store.v8_instances["remote"][0]["running"]
    elif problem == "absent_instance":
        state.store.v8_instances["remote"] = []
    elif problem == "nan_time":
        state.store.v8_instances["remote"][0]["snapshot_generated_at"] = float("nan")
    elif problem == "future_time":
        state.store.v8_instances["remote"][0]["snapshot_checked_at"] = 1100
    else:
        status["collector_error"] = True
    assert "unverified" in _assert_blocked(state).detail


@pytest.mark.parametrize("source", ["observation", "desired_assignment"])
def test_disabled_config_still_checks_relevant_remote_runtime(guarded_instance, source):
    """Disabled configs may still have a previous deployment awaiting observation."""
    state = guarded_instance
    if source == "observation":
        state.store.update_v8_instances("remote", [{"name": "alice", "running": False}],
                                        snapshot_generated_at=990, snapshot_checked_at=995)
    else:
        cluster = state.root / "data" / "cluster"
        cluster.mkdir()
        (cluster / "desired_state.json").write_text(json.dumps({"pb8_instances": {"alice": {"assigned_host": "node-remote", "desired_state": "running"}}}), encoding="utf-8")
        (cluster / "cluster_nodes.json").write_text(json.dumps({"nodes": {"node-remote": {"pbname": "remote"}}}), encoding="utf-8")
    _assert_blocked(state)


@pytest.mark.parametrize("remote", [False, True])
def test_never_deployed_disabled_or_verified_stopped_can_delete(guarded_instance, remote):
    """Unknown unrelated hosts do not permanently lock a never-deployed disabled config."""
    state = guarded_instance
    state.store.v8_instances["unrelated"] = [{"name": "bob", "running": True}]
    if remote:
        (state.target / "config.json").write_text('{"pbgui":{"enabled_on":"remote"}}', encoding="utf-8")
        state.store.update_v8_instances("remote", [{"name": "alice", "running": False}],
                                        snapshot_generated_at=990, snapshot_checked_at=995)
        state.store.streams["remote"] = {"monitor_agent": {"files": {"instance_snapshot.json": {
            "state": "ok", "generated_at": 990, "checked_at": 995,
        }}}}
    assert v8_instances.delete_v8_instance("alice", None)["ok"] is True
    state.snapshot.assert_called_once()
    state.tombstone.assert_called_once_with("alice", 1)
    state.remove.assert_called_once_with(state.target)


def test_remote_running_blocks_even_when_observation_is_stale(guarded_instance):
    """A positive observation is a blocker until a verified stopped update arrives."""
    state = guarded_instance
    state.store.v8_instances["remote"] = [{"name": "alice", "running": True}]
    assert "running on remote" in _assert_blocked(state).detail


def test_other_local_instance_is_not_a_process_match(guarded_instance):
    """An overlapping name is not the exact RunV8 config identity being deleted."""
    state = guarded_instance
    command = [str(state.root / "venv" / "bin" / "passivbot"), "live",
               str(state.target.parent / "alice-extra" / "config.json"), "--fail-on-stale-rust"]
    state.processes.append(SimpleNamespace(cmdline=lambda: command, cwd=lambda: str(state.root / "pb8")))
    assert v8_instances.delete_v8_instance("alice", None)["ok"] is True


def test_failed_local_process_inspection_blocks_before_mutation(guarded_instance, monkeypatch):
    """An inspection failure cannot turn into a false stopped observation."""
    monkeypatch.setattr(PBRun.psutil, "process_iter", Mock(side_effect=OSError("test denied")))
    _assert_blocked(guarded_instance)


@pytest.mark.parametrize("field", ["cmdline", "cwd"])
@pytest.mark.parametrize("error", [PBRun.psutil.AccessDenied(pid=77), OSError("test unreadable")])
def test_swallowed_matcher_errors_cannot_allow_delete(guarded_instance, field, error):
    """Denied command/cwd reads must not inherit RunV8's boolean non-match behavior."""
    state = guarded_instance
    command = [str(state.root / "venv" / "bin" / "passivbot"), "live", str(state.target / "config.json"), "--fail-on-stale-rust"]
    process = SimpleNamespace(cmdline=Mock(return_value=command), cwd=Mock(return_value=str(state.root / "pb8")))
    getattr(process, field).side_effect = error
    state.processes.append(process)
    _assert_blocked(state)


@pytest.mark.parametrize("error", [PBRun.psutil.NoSuchProcess(pid=77), PBRun.psutil.ZombieProcess(pid=77)])
def test_disappeared_process_is_not_a_live_owner(guarded_instance, error):
    """Gone/zombie process races do not make verified stopped bundles undeletable."""
    state = guarded_instance
    state.processes.append(SimpleNamespace(cmdline=Mock(side_effect=error)))
    assert v8_instances.delete_v8_instance("alice", None)["ok"] is True


@pytest.mark.parametrize("failure", ["corrupt", "unavailable"])
def test_loader_failure_is_closed_without_backup_or_tombstone(guarded_instance, monkeypatch, failure):
    """Malformed config or unavailable canonicalization blocks deletion after local checking."""
    state = guarded_instance
    if failure == "corrupt":
        (state.target / "config.json").write_text("{broken", encoding="utf-8")
    else:
        monkeypatch.setattr(pb8_config, "_call_helper", Mock(side_effect=pb8_config.PB8ConfigurationError("test unavailable")))
    _assert_blocked(state)


@pytest.mark.parametrize("snapshot", ["stopped", "stale", "missing", "absent_instance"])
def test_real_monitor_store_contract_controls_delete(guarded_instance, snapshot):
    """Run real monitor collection and metadata updates using an in-memory SSH response."""
    state = guarded_instance
    (state.target / "config.json").write_text('{"pbgui":{"enabled_on":"remote"}}', encoding="utf-8")
    payload = {
        "schema_version": 1, "source": "monitor-agent", "generated_at": 800 if snapshot == "stale" else 990,
        "monitors": [], "v7": [], "cache": {}, "bot_logs": {},
        "v8": [] if snapshot == "absent_instance" else [{
            "name": "alice", "running": False, "cv": 1, "eo": "remote", "rv": 0,
            "di": False, "blocked": False, "blocked_reason": "", "cluster_gate": "allowed",
        }],
    }
    monitor = VPSMonitor.__new__(VPSMonitor)
    monitor.store = state.store
    monitor.pool = SimpleNamespace(
        get_remote_pbgui_dir=lambda _host: str(state.root),
        run=AsyncMock(return_value=SimpleNamespace(exit_status=1 if snapshot == "missing" else 0, stdout=json.dumps(payload))),
    )
    monitor._import_agent_bot_history = Mock()
    monitor._cache_host_snapshot = Mock()
    monitor.debug_logging = False
    asyncio.run(monitor._collect_instances("remote"))
    status = state.store.streams["remote"]["monitor_agent"]["files"]["instance_snapshot.json"]
    assert status["checked_at"] == 1000
    assert status["source"] == "monitor-agent"
    if snapshot == "stopped":
        assert status["state"] == "ok" and status["generated_at"] == 990
        assert state.store.v8_instances["remote"] == [
            {**row, "snapshot_generated_at": 990, "snapshot_checked_at": 1000} for row in payload["v8"]
        ]
        assert v8_instances.delete_v8_instance("alice", None)["ok"] is True
    else:
        _assert_blocked(state)


@pytest.mark.parametrize("processing_error", [False, True])
@pytest.mark.parametrize("old_generated_at", [800, 990])
def test_diagnostics_cannot_authorize_old_rows_during_collection(guarded_instance, processing_error, old_generated_at):
    """Pause after real diagnostic publication, before rows, including a history failure."""
    state = guarded_instance
    state.store.update_v8_instances("remote", [{"name": "alice", "running": False}],
                                    snapshot_generated_at=old_generated_at, snapshot_checked_at=old_generated_at)
    old_published = state.store.v8_instances["remote"]
    payload = {
        "schema_version": 1, "source": "monitor-agent", "generated_at": 999,
        "monitors": [], "v7": [], "cache": {}, "bot_logs": {},
        "v8": [{"name": "alice", "running": True}],
    }
    monitor = VPSMonitor.__new__(VPSMonitor)
    monitor.store = state.store
    monitor.pool = SimpleNamespace(
        get_remote_pbgui_dir=lambda _host: str(state.root),
        run=AsyncMock(return_value=SimpleNamespace(exit_status=0, stdout=json.dumps(payload))),
    )
    monitor._cache_host_snapshot = Mock()
    monitor.debug_logging = False

    def paused_history(*_args):
        """Exercise deletion synchronously at the exact publication gap."""
        status = state.store.streams["remote"]["monitor_agent"]["files"]["instance_snapshot.json"]
        assert status["state"] == "ok" and status["generated_at"] == 999
        assert state.store.v8_instances["remote"] == [{"name": "alice", "running": False}]
        assert "unverified" in _assert_blocked(state).detail
        if processing_error:
            raise RuntimeError("test history failure")

    monitor._import_agent_bot_history = Mock(side_effect=paused_history)
    if processing_error:
        with pytest.raises(RuntimeError, match="history failure"):
            asyncio.run(monitor._collect_instances("remote"))
        assert "unverified" in _assert_blocked(state).detail
    else:
        asyncio.run(monitor._collect_instances("remote"))
        assert "running on remote" in _assert_blocked(state).detail
    monitor._import_agent_bot_history.assert_called_once()
    # Publication never mutates a list already handed to a consumer.
    assert old_published == [{"name": "alice", "running": False,
                              "snapshot_generated_at": old_generated_at, "snapshot_checked_at": old_generated_at}]


@pytest.mark.parametrize("variant", ["legacy", "different_generation", "different_check", "stale", "unknown_running", "all_fresh"])
def test_every_matching_row_requires_same_fresh_generation(guarded_instance, variant):
    """Duplicate matching observations cannot hide a stale, unknown or unstamped row."""
    state = guarded_instance
    state.store.update_v8_instances("remote", [{"name": "alice", "running": False}] * 2,
                                    snapshot_generated_at=990, snapshot_checked_at=995)
    # Copy to model a legacy/malformed transport packet rather than a store mutation.
    rows = [dict(row) for row in state.store.v8_instances["remote"]]
    if variant == "legacy":
        rows[1].pop("snapshot_checked_at")
    elif variant == "different_generation":
        rows[1]["snapshot_generated_at"] = 991
    elif variant == "different_check":
        rows[1]["snapshot_checked_at"] = 996
    elif variant == "stale":
        rows[1]["snapshot_generated_at"] = 800
    elif variant == "unknown_running":
        rows[1].pop("running")
    state.store.v8_instances["remote"] = rows
    state.store.streams["remote"] = {"monitor_agent": {"files": {"instance_snapshot.json": {
        "state": "ok", "generated_at": 1000, "checked_at": 1000,
    }}}}
    if variant == "all_fresh":
        assert v8_instances.delete_v8_instance("alice", None)["ok"] is True
    else:
        assert "unverified" in _assert_blocked(state).detail
