"""Offline tests for PB8-native OHLCV planning and preload ownership."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from api import pb8_ohlcv_tools
from api import pb8_ohlcv_runtime_helper
from master_update_lock import MasterUpdateBusyError


class _Lease:
    """Minimal runtime lease used by isolated tests."""

    def __init__(self, released: list[bool] | None = None) -> None:
        self.released = released

    def release(self) -> None:
        if self.released is not None:
            self.released.append(True)


@pytest.fixture
def pb8_runtime(tmp_path, monkeypatch):
    """Build a fake PB8 source, virtualenv, and PBGui market-data root."""
    pb8_dir = tmp_path / "pb8"
    (pb8_dir / "src").mkdir(parents=True)
    python = tmp_path / "venv_pb8" / "bin" / "python"
    cli = python.parent / "passivbot"
    python.parent.mkdir(parents=True)
    for executable in (python, cli):
        executable.write_text("#!/bin/sh\n", encoding="utf-8")
        executable.chmod(0o700)
    market_root = tmp_path / "pbgui" / "data" / "ohlcv"
    market_root.mkdir(parents=True)
    status = {
        "ready": True,
        "pb8dir": str(pb8_dir),
        "pb8venv": str(python),
        "cli_file": str(cli),
        "errors": [],
    }
    monkeypatch.setattr(pb8_ohlcv_tools, "pb8_runtime_status", lambda: status)
    monkeypatch.setattr(pb8_ohlcv_tools, "PBGDIR", str(tmp_path / "pbgui"))
    monkeypatch.setattr(pb8_ohlcv_tools, "_pbgui_market_data_root", lambda: market_root)
    monkeypatch.setattr(pb8_ohlcv_tools, "acquire_master_runtime_lock", lambda _root: _Lease())
    with pb8_ohlcv_tools._LOCK:
        pb8_ohlcv_tools._JOBS.clear()
        pb8_ohlcv_tools._REAPERS.clear()
        monkeypatch.setattr(pb8_ohlcv_tools, "_RESTORED", False)
    return status, pb8_dir, python, cli, market_root


def test_relative_default_and_pbgui_absolute_sources_resolve_to_approved_roots(pb8_runtime) -> None:
    """PB8 relative/default paths stay under PB8 while PBGui absolute roots remain valid."""
    status, pb8_dir, _python, _cli, market_root = pb8_runtime

    default_config, default_source, catalog = pb8_ohlcv_tools.resolve_pb8_ohlcv_paths(
        {"backtest": {}}, status
    )
    relative_config, relative_source, _catalog = pb8_ohlcv_tools.resolve_pb8_ohlcv_paths(
        {"backtest": {"ohlcv_source_dir": "market-cache"}}, status
    )
    absolute_config, absolute_source, _catalog = pb8_ohlcv_tools.resolve_pb8_ohlcv_paths(
        {"backtest": {"ohlcv_source_dir": str(market_root)}}, status
    )

    assert default_source is None
    assert "ohlcv_source_dir" not in default_config["backtest"]
    assert catalog == pb8_dir / "caches" / "ohlcvs" / "catalog.sqlite"
    assert relative_source == pb8_dir / "market-cache"
    assert relative_config["backtest"]["ohlcv_source_dir"] == str(relative_source)
    assert absolute_source == market_root
    assert absolute_config["backtest"]["ohlcv_source_dir"] == str(market_root)


def test_preflight_uses_pb8_python_cwd_and_never_pb7(pb8_runtime, monkeypatch) -> None:
    """Read-only planning must execute only the configured PB8 helper runtime."""
    _status, pb8_dir, python, _cli, _market_root = pb8_runtime
    captured = {}

    class Proc:
        returncode = 0
        stdout = json.dumps({"ok": True, "result": {"summary": {"overall_status": "ready"}}})
        stderr = ""

    def fake_run(command, **kwargs):
        captured.update(command=command, kwargs=kwargs)
        return Proc()

    monkeypatch.setattr(pb8_ohlcv_tools.subprocess, "run", fake_run)

    result = asyncio.run(pb8_ohlcv_tools.build_pb8_ohlcv_preflight({"backtest": {}}))

    assert result["summary"]["overall_status"] == "ready"
    assert captured["command"][0] == str(python)
    assert captured["kwargs"]["cwd"] == str(pb8_dir)
    assert captured["command"][1].endswith("pb8_ohlcv_runtime_helper.py")
    assert "pb7" not in " ".join(captured["command"]).lower()
    assert json.loads(captured["kwargs"]["input"])["pb8_dir"] == str(pb8_dir)


def test_v8_routes_do_not_import_pb7_ohlcv_runtime() -> None:
    """Both PB8 editors must remain disconnected from the PB7 OHLCV helper."""
    api_dir = Path(pb8_ohlcv_tools.__file__).resolve().parent

    for filename in ("optimize_v8.py", "backtest_v8.py"):
        source = (api_dir / filename).read_text(encoding="utf-8")
        assert "api.pb7_ohlcv_tools" not in source


def test_preload_uses_native_pb8_download_command_and_runtime_cwd(pb8_runtime, monkeypatch) -> None:
    """Persistent preparation jobs must call PB8's installed passivbot download command."""
    _status, pb8_dir, _python, cli, _market_root = pb8_runtime
    captured = {}
    released = []

    def fake_spawn(job_id: str) -> None:
        captured.update(pb8_ohlcv_tools._JOBS[job_id])

    monkeypatch.setattr(pb8_ohlcv_tools, "_spawn_worker", fake_spawn)
    monkeypatch.setattr(
        pb8_ohlcv_tools,
        "acquire_master_runtime_lock",
        lambda _root: _Lease(released),
    )
    (pb8_dir / "caches" / "ohlcv").mkdir(parents=True)

    payload = pb8_ohlcv_tools.start_pb8_ohlcv_preload_job(
        {"backtest": {"ohlcv_source_dir": "caches/ohlcv"}, "pbgui": {"private": "not-runtime"}}
    )

    assert payload["status"] == "queued"
    assert captured["command"] == [str(cli), "download", captured["config_path"]]
    assert captured["cwd"] == str(pb8_dir)
    assert "pb7" not in " ".join(captured["command"]).lower()
    persisted = json.loads(Path(captured["config_path"]).read_text(encoding="utf-8"))
    assert persisted["backtest"]["ohlcv_source_dir"] == str(pb8_dir / "caches" / "ohlcv")
    assert "pbgui" not in persisted
    assert released == [True]


def test_linux_preload_supervisor_uses_independent_user_systemd_unit(pb8_runtime, monkeypatch, tmp_path) -> None:
    """Linux preloads must leave the API cgroup through a private transient user unit."""
    captured = {}
    log_path = tmp_path / "preload.log"

    class Result:
        returncode = 0
        stdout = ""
        stderr = ""

    def fake_run(command, **kwargs):
        captured.update(command=command, kwargs=kwargs)
        return Result()

    monkeypatch.setattr(pb8_ohlcv_tools, "_systemd_user_manager_available", lambda: True)
    monkeypatch.setattr(pb8_ohlcv_tools, "which", lambda name: f"/usr/bin/{name}")
    monkeypatch.setattr(pb8_ohlcv_tools.subprocess, "run", fake_run)
    monkeypatch.setattr(
        pb8_ohlcv_tools.subprocess,
        "Popen",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("fallback Popen used")),
    )

    proc, unit = pb8_ohlcv_tools._launch_supervisor("a" * 12, ["/venv/bin/python", "worker.py"], log_path)

    assert proc is None
    assert unit.startswith(f"pbgui-pb8-ohlcv-{'a' * 12}-")
    assert "--user" in captured["command"]
    assert "--collect" in captured["command"]
    assert "--property=Type=exec" in captured["command"]
    assert "--property=UMask=0077" in captured["command"]
    assert f"--property=WorkingDirectory={pb8_ohlcv_tools.PBGDIR}" in captured["command"]
    assert captured["command"][-3:] == ["--", "/venv/bin/python", "worker.py"]
    assert log_path.read_text(encoding="utf-8") == ""


def test_spawn_persists_systemd_ownership_before_supervisor_launch(pb8_runtime, monkeypatch) -> None:
    """A restarted API must know the transient unit even before the worker publishes its PID."""
    job_id = "b" * 12
    state_path = pb8_ohlcv_tools._state_path(job_id)
    job = {
        "job_id": job_id,
        "status": "queued",
        "state_path": str(state_path),
        "log_path": str(pb8_ohlcv_tools._log_path(job_id)),
    }
    with pb8_ohlcv_tools._LOCK:
        pb8_ohlcv_tools._JOBS[job_id] = job
        pb8_ohlcv_tools._persist_locked(job)

    def fake_launch(_job_id, command, _log_path, *, systemd_unit, allow_systemd):
        persisted = json.loads(state_path.read_text(encoding="utf-8"))
        assert allow_systemd is True
        assert persisted["systemd_unit"] == systemd_unit
        persisted.update(status="completed", returncode=0, finished_at=1)
        state_path.write_text(json.dumps(persisted), encoding="utf-8")
        return None, systemd_unit

    monkeypatch.setattr(pb8_ohlcv_tools, "_systemd_user_manager_available", lambda: True)
    monkeypatch.setattr(pb8_ohlcv_tools, "_launch_supervisor", fake_launch)

    pb8_ohlcv_tools._spawn_worker(job_id)

    assert pb8_ohlcv_tools._JOBS[job_id]["status"] == "completed"
    assert pb8_ohlcv_tools._systemd_unit_for_job(pb8_ohlcv_tools._JOBS[job_id]) is not None


def test_systemd_unit_validation_rejects_foreign_numeric_name() -> None:
    """Persisted state must never authorize control of an unrelated user unit."""
    assert pb8_ohlcv_tools._systemd_unit_for_job({"job_id": "c" * 12, "systemd_unit": "123"}) is None


def test_reconcile_does_not_overwrite_newer_worker_completion(pb8_runtime, monkeypatch) -> None:
    """A completion published during process probing must win over stale reconciliation."""
    job_id = "d" * 12
    state_path = pb8_ohlcv_tools._state_path(job_id)
    job = {"job_id": job_id, "status": "running", "state_path": str(state_path), "stop_requested": False}
    pb8_ohlcv_tools._JOBS[job_id] = job
    pb8_ohlcv_tools._persist_locked(job)

    def finish_during_probe(_job):
        persisted = json.loads(state_path.read_text(encoding="utf-8"))
        persisted.update(status="completed", returncode=0, finished_at=1)
        state_path.write_text(json.dumps(persisted), encoding="utf-8")
        return False

    monkeypatch.setattr(pb8_ohlcv_tools, "_process_identity", finish_during_probe)
    monkeypatch.setattr(pb8_ohlcv_tools, "_systemd_unit_active", lambda _job: False)

    pb8_ohlcv_tools._reconcile_locked(job)

    persisted = json.loads(state_path.read_text(encoding="utf-8"))
    assert persisted["status"] == "completed"
    assert persisted["returncode"] == 0


@pytest.mark.parametrize(("control_ok", "expected_status"), [(True, "stopped"), (False, "launching")])
def test_stop_launching_systemd_job_requires_successful_unit_control(
    pb8_runtime, monkeypatch, control_ok: bool, expected_status: str
) -> None:
    """A PID-less launching unit is stopped by ownership, never by an assumed process state."""
    job_id = "e" * 12
    state_path = pb8_ohlcv_tools._state_path(job_id)
    job = {
        "job_id": job_id,
        "status": "launching",
        "pid": None,
        "systemd_unit": f"pbgui-pb8-ohlcv-{job_id}-123",
        "state_path": str(state_path),
        "log_path": str(pb8_ohlcv_tools._log_path(job_id)),
        "stop_requested": False,
    }
    pb8_ohlcv_tools._JOBS[job_id] = job
    pb8_ohlcv_tools._persist_locked(job)
    controls = []

    def control(_job, action):
        controls.append(action)
        return control_ok

    monkeypatch.setattr(pb8_ohlcv_tools, "_cleanup_jobs", lambda **_kwargs: None)
    monkeypatch.setattr(pb8_ohlcv_tools, "_control_systemd_unit", control)
    monkeypatch.setattr(pb8_ohlcv_tools, "_systemd_unit_active", lambda _job: not control_ok)

    payload = pb8_ohlcv_tools.stop_pb8_ohlcv_preload_job(job_id)

    assert controls == ["stop"]
    assert payload["status"] == expected_status


@pytest.mark.parametrize("operation", ["preflight", "preload"])
def test_update_lock_blocks_pb8_ohlcv_without_starting_subprocess(
    pb8_runtime, monkeypatch, operation: str
) -> None:
    """An active update must fail safely before any PB8 helper or worker starts."""
    monkeypatch.setattr(
        pb8_ohlcv_tools,
        "acquire_master_runtime_lock",
        lambda _root: (_ for _ in ()).throw(MasterUpdateBusyError("update active")),
    )
    monkeypatch.setattr(
        pb8_ohlcv_tools.subprocess,
        "run",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("PB8 helper started")),
    )
    monkeypatch.setattr(
        pb8_ohlcv_tools,
        "_spawn_worker",
        lambda *_args: (_ for _ in ()).throw(AssertionError("PB8 worker started")),
    )

    with pytest.raises(pb8_ohlcv_tools.PB8OhlcvBusyError, match="Retry"):
        if operation == "preflight":
            pb8_ohlcv_tools._run_preflight_helper({"backtest": {}})
        else:
            pb8_ohlcv_tools.start_pb8_ohlcv_preload_job({"backtest": {}})


def test_missing_explicit_source_returns_actionable_unsupported_status(pb8_runtime, monkeypatch) -> None:
    """PB8 must not pretend it can download into a missing caller-managed source."""
    monkeypatch.setattr(
        pb8_ohlcv_tools,
        "_spawn_worker",
        lambda *_args: (_ for _ in ()).throw(AssertionError("worker started")),
    )

    with pytest.raises(pb8_ohlcv_tools.PB8OhlcvUnsupportedError, match="Populate it"):
        pb8_ohlcv_tools.start_pb8_ohlcv_preload_job(
            {"backtest": {"ohlcv_source_dir": "missing-source"}}
        )


@pytest.mark.parametrize(("identity", "expected_status", "signal_count"), [(True, "stopped", 1), (None, "running", 0)])
def test_stop_preload_signals_only_verified_supervisor(
    pb8_runtime, monkeypatch, identity: bool | None, expected_status: str, signal_count: int
) -> None:
    """Stop requests must never signal or falsely stop an unverified process."""
    _status, _pb8_dir, _python, _cli, _market_root = pb8_runtime
    job_id = "a" * 12
    signals = []
    job = {
        "job_id": job_id,
        "status": "running",
        "pid": 4242,
        "process_created_at": 10.0,
        "started_at": 1,
        "finished_at": None,
        "returncode": None,
        "error": None,
        "log_path": str(Path(pb8_ohlcv_tools.PBGDIR) / "preload.log"),
        "state_path": str(Path(pb8_ohlcv_tools.PBGDIR) / "preload.state.json"),
        "stop_requested": False,
    }
    pb8_ohlcv_tools._JOBS[job_id] = job
    monkeypatch.setattr(pb8_ohlcv_tools, "_cleanup_jobs", lambda **_kwargs: None)
    monkeypatch.setattr(pb8_ohlcv_tools, "_refresh_locked", lambda _job: None)
    monkeypatch.setattr(pb8_ohlcv_tools, "_persist_locked", lambda _job: None)
    monkeypatch.setattr(pb8_ohlcv_tools, "_process_identity", lambda _job: identity)
    monkeypatch.setattr(pb8_ohlcv_tools.psutil, "pid_exists", lambda _pid: False)
    monkeypatch.setattr(pb8_ohlcv_tools.os, "killpg", lambda pid, sig: signals.append((pid, sig)))

    payload = pb8_ohlcv_tools.stop_pb8_ohlcv_preload_job(job_id)

    assert payload["status"] == expected_status
    assert len(signals) == signal_count
    assert job["stop_requested"] is True


def test_explicit_source_gaps_disable_remote_preload() -> None:
    """PB8's read-only explicit source mode must not advertise a remote warmup."""
    counts = pb8_ohlcv_runtime_helper._counts()
    counts["missing_local"] = 2

    explicit = pb8_ohlcv_runtime_helper._summary(counts, 2, explicit_source=True)
    default = pb8_ohlcv_runtime_helper._summary(counts, 2, explicit_source=False)

    assert explicit["overall_status"] == "blocked"
    assert explicit["preload_supported"] is False
    assert "read-only" in explicit["preload_detail"]
    assert default["overall_status"] == "preload"
    assert default["preload_supported"] is True
