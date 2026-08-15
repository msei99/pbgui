"""Offline regression tests for PBRun's PB8 live-process supervisor."""

from __future__ import annotations

import configparser
import importlib.util
import json
import os
import signal
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


ROOT_DIR = Path(__file__).parent.parent.resolve()
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

pbrun_spec = importlib.util.spec_from_file_location("PBRun_pb8_tests", ROOT_DIR / "PBRun.py")
pbrun = importlib.util.module_from_spec(pbrun_spec)
assert pbrun_spec.loader is not None
pbrun_spec.loader.exec_module(pbrun)


def _snapshot(**values: str):
    """Build an INI snapshot double with selected main settings."""

    parser = configparser.ConfigParser()
    parser["main"] = values
    return SimpleNamespace(
        parser=parser,
        has_option=parser.has_option,
        get=parser.get,
    )


def _write_pb8_instance(
    root: Path,
    *,
    name: str = "bot-a",
    enabled_on: str = "node-a",
    runtime: str = "pb8",
    version: int = 3,
    live_user: str = "alice",
) -> Path:
    """Create one minimal PB8 live config and its API-key user catalog."""

    instance = root / "data" / "run_v8" / name
    instance.mkdir(parents=True)
    (instance / "config.json").write_text(
        json.dumps(
            {
                "pbgui": {"runtime": runtime, "enabled_on": enabled_on, "version": version},
                "live": {"user": live_user},
            }
        ),
        encoding="utf-8",
    )
    pb8_dir = root / "pb8"
    pb8_dir.mkdir(exist_ok=True)
    (pb8_dir / "api-keys.json").write_text(json.dumps({live_user: {}}), encoding="utf-8")
    return instance


def _runner(root: Path, instance: Path) -> pbrun.RunV8:
    """Build a configured RunV8 without touching an external runtime."""

    runner = pbrun.RunV8()
    runner.path = str(instance)
    runner.user = instance.name
    runner.name = "node-a"
    runner.pb8dir = str(root / "pb8")
    runner.pb8venv = str(root / "venv_pb8" / "bin" / "python")
    runner.pbgdir = root
    return runner


@pytest.mark.parametrize(
    ("settings", "pb7_ready", "pb8_ready"),
    [
        ({"pb7dir": "/pb7", "pb7venv": "/venv7/bin/python"}, True, False),
        ({"pb8dir": "/pb8", "pb8venv": "/venv8/bin/python"}, False, True),
        (
            {
                "pb7dir": "/pb7",
                "pb7venv": "/venv7/bin/python",
                "pb8dir": "/pb8",
                "pb8venv": "/venv8/bin/python",
            },
            True,
            True,
        ),
        ({}, False, False),
    ],
)
def test_pbrun_initializes_for_every_runtime_profile(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    settings: dict[str, str],
    pb7_ready: bool,
    pb8_ready: bool,
) -> None:
    """PBRun remains usable with PB7, PB8, both, or neither configured."""

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(pbrun, "CoinData", lambda: SimpleNamespace())
    monkeypatch.setattr(pbrun.pbgui_purefunc, "load_ini_snapshot", lambda: _snapshot(pbname="node-a", **settings))

    run = pbrun.PBRun()

    assert run.pb7_ready is pb7_ready
    assert run.pb8_ready is pb8_ready
    assert run.run_v7 == []
    assert run.run_v8 == []
    assert Path(run.v8_path) == tmp_path / "data" / "run_v8"


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({"runtime": "pb7"}, "pbgui.runtime"),
        ({"enabled_on": "node-b"}, "pbgui.enabled_on"),
        ({"version": 0}, "pbgui.version"),
        ({"live_user": "missing"}, "api-keys.json"),
    ],
)
def test_runv8_rejects_invalid_metadata_or_missing_api_user(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    overrides: dict[str, object],
    expected: str,
) -> None:
    """PB8 launch metadata and API-key user presence fail closed."""

    logs: list[str] = []
    instance = _write_pb8_instance(
        tmp_path,
        runtime=str(overrides.get("runtime", "pb8")),
        enabled_on=str(overrides.get("enabled_on", "node-a")),
        version=int(overrides.get("version", 3)),
        live_user=str(overrides.get("live_user", "alice")),
    )
    if overrides.get("live_user") == "missing":
        (tmp_path / "pb8" / "api-keys.json").write_text(json.dumps({"alice": {}}), encoding="utf-8")
    runner = _runner(tmp_path, instance)
    monkeypatch.setattr(pbrun, "_log", lambda _service, message, **_kwargs: logs.append(message))

    assert runner.load() is False
    assert expected in logs[-1]


def test_runv8_rejects_symlinked_api_keys_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Credential presence checks never follow a PB8 api-keys symlink."""

    instance = _write_pb8_instance(tmp_path)
    api_keys = tmp_path / "pb8" / "api-keys.json"
    target = tmp_path / "outside-api-keys.json"
    target.write_text(json.dumps({"alice": {}}), encoding="utf-8")
    api_keys.unlink()
    api_keys.symlink_to(target)
    runner = _runner(tmp_path, instance)
    monkeypatch.setattr(pbrun, "_log", lambda *_args, **_kwargs: None)

    assert runner.load() is False


def test_runv8_launches_exact_cli_with_isolated_virtualenv(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """RunV8 uses the PB8 console script, exact arguments, cwd, and isolated PATH."""

    instance = _write_pb8_instance(tmp_path)
    runner = _runner(tmp_path, instance)
    captured: dict[str, object] = {}
    events: list[str] = []
    running = iter((False, False, True))

    class Lease:
        """Record release of the shared PB8 launch boundary."""

        def release(self) -> None:
            events.append("release")

    def fake_popen(command, **kwargs):
        events.append("popen")
        captured.update(command=command, **kwargs)
        return SimpleNamespace(stdout=iter(()))

    monkeypatch.setattr(runner, "is_running", lambda: next(running))
    monkeypatch.setattr(
        pbrun,
        "acquire_master_runtime_lock",
        lambda _root: events.append("acquire") or Lease(),
    )
    monkeypatch.setattr(runner, "_runtime_ready", lambda: events.append("ready") or True)
    monkeypatch.setattr(pbrun.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(pbrun.threading, "Thread", lambda **_kwargs: SimpleNamespace(start=lambda: None))
    monkeypatch.setattr(pbrun, "sleep", lambda _seconds: None)
    monkeypatch.setattr(pbrun, "_log", lambda *_args, **_kwargs: None)

    assert runner.start() is True
    expected = [
        str(tmp_path / "venv_pb8" / "bin" / "passivbot"),
        "live",
        str((instance / "config.json").resolve()),
        "--fail-on-stale-rust",
    ]
    assert captured["command"] == expected
    assert captured["cwd"] == str((tmp_path / "pb8").resolve())
    assert captured["env"]["VIRTUAL_ENV"] == str((tmp_path / "venv_pb8").resolve())
    assert captured["env"]["PATH"] == str(tmp_path / "venv_pb8" / "bin") + os.pathsep + os.defpath
    assert captured["start_new_session"] is True
    assert captured["stdout"] is pbrun.subprocess.PIPE
    assert captured["stderr"] is pbrun.subprocess.STDOUT
    assert events == ["acquire", "ready", "popen", "release"]


def test_runv8_command_keeps_cli_in_symlinked_virtualenv(tmp_path: Path) -> None:
    """Resolving a venv Python symlink must not redirect the PB8 CLI to /usr/bin."""

    instance = _write_pb8_instance(tmp_path)
    python_path = tmp_path / "venv_pb8" / "bin" / "python"
    python_path.parent.mkdir(parents=True)
    python_path.symlink_to(Path(sys.executable).resolve())
    runner = _runner(tmp_path, instance)

    assert runner.command[0] == str(python_path.parent / "passivbot")
    process = SimpleNamespace(
        cmdline=lambda: [str(python_path), *runner.command],
        cwd=lambda: runner.pb8dir,
    )
    assert runner._matches_process(process) is True


def test_runv8_launch_defers_while_pb8_update_owns_runtime_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A PB8 writer blocks launch before readiness checks or process creation."""

    instance = _write_pb8_instance(tmp_path)
    runner = _runner(tmp_path, instance)
    monkeypatch.setattr(runner, "is_running", lambda: False)
    monkeypatch.setattr(
        pbrun,
        "acquire_master_runtime_lock",
        lambda _root: (_ for _ in ()).throw(pbrun.MasterUpdateBusyError("busy")),
    )
    monkeypatch.setattr(runner, "_runtime_ready", lambda: pytest.fail("readiness checked without the lock"))
    monkeypatch.setattr(pbrun.subprocess, "Popen", lambda *_args, **_kwargs: pytest.fail("launched while update was active"))
    monkeypatch.setattr(pbrun, "_log", lambda *_args, **_kwargs: None)

    assert runner.start() is False


def test_runv8_runtime_ready_rejects_rust_probe_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PBRun blocks before launch when PB8 reports an unstamped Rust extension."""

    instance = _write_pb8_instance(tmp_path)
    runner = _runner(tmp_path, instance)
    reasons: list[tuple[str, str]] = []
    monkeypatch.setattr(
        pbrun.pbgui_purefunc,
        "pb8_runtime_status",
        lambda: {"ready": True, "pb8dir": runner.pb8dir, "pb8venv": runner.pb8venv},
    )
    monkeypatch.setattr(
        pbrun.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=0,
            stdout=json.dumps({"path": "src/passivbot_rust.so", "stamped": False, "needs_rebuild": True}) + "\n",
            stderr="",
        ),
    )
    monkeypatch.setattr(runner, "_log_block", lambda key, reason: reasons.append((key, reason)))

    assert runner._runtime_ready() is False
    assert reasons == [("rust_stale", "PB8 Rust extension has no source fingerprint stamp; rerun Update PB8 on this host")]


def test_runv8_runtime_ready_accepts_verified_rust_probe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PBRun permits launch when PB8's own Rust check reports the stamped build current."""

    instance = _write_pb8_instance(tmp_path)
    runner = _runner(tmp_path, instance)
    monkeypatch.setattr(
        pbrun.pbgui_purefunc,
        "pb8_runtime_status",
        lambda: {"ready": True, "pb8dir": runner.pb8dir, "pb8venv": runner.pb8venv},
    )
    monkeypatch.setattr(
        pbrun.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=0,
            stdout=json.dumps({"path": "src/passivbot_rust.so", "stamped": True, "needs_rebuild": False}) + "\n",
            stderr="",
        ),
    )

    assert runner._runtime_ready() is True


def test_runv8_process_identity_rejects_suffix_and_cwd_decoys(tmp_path: Path) -> None:
    """Only the complete command and configured PB8 cwd identify a managed process."""

    instance = _write_pb8_instance(tmp_path)
    runner = _runner(tmp_path, instance)
    exact = runner.command
    process = lambda command, cwd: SimpleNamespace(cmdline=lambda: command, cwd=lambda: cwd)

    assert runner._matches_process(process(exact, runner.pb8dir)) is True
    assert runner._matches_process(process(["python", *exact, "--extra"], runner.pb8dir)) is False
    assert runner._matches_process(process(exact, str(tmp_path / "other"))) is False


def test_watch_v8_stops_only_exact_pb8_process_with_disappeared_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A PBRun restart cleans a missing-config PB8 bot without touching decoys."""

    run_root = tmp_path / "data" / "run_v8"
    run_root.mkdir(parents=True)
    pb8dir = tmp_path / "pb8"
    pb8dir.mkdir()
    pb8venv = tmp_path / "venv_pb8" / "bin" / "python"
    passivbot = pb8venv.parent / "passivbot"
    missing_config = (run_root / "removed-bot" / "config.json").resolve()
    signals: list[tuple[int, signal.Signals]] = []

    class Process:
        """Minimal psutil process double with an exact command identity."""

        def __init__(self, pid: int, config_path: Path):
            self.pid = pid
            self._config_path = config_path

        def cmdline(self):
            return [str(passivbot), "live", str(self._config_path), "--fail-on-stale-rust"]

        def create_time(self):
            return 100.0

        def cwd(self):
            return str(pb8dir)

        def wait(self, timeout):
            del timeout

        def send_signal(self, sig):
            signals.append((self.pid, sig))

    orphan = Process(4101, missing_config)
    outside_decoy = Process(4102, (tmp_path / "unmanaged" / "config.json").resolve())
    run = pbrun.PBRun.__new__(pbrun.PBRun)
    run.pbgdir = tmp_path
    run.v8_path = str(run_root)
    run.name = "node-a"
    run.pb8dir = str(pb8dir)
    run.pb8venv = str(pb8venv)
    run.pb8_ready = True
    run.run_v8 = []
    monkeypatch.setattr(pbrun.psutil, "process_iter", lambda: iter((orphan, outside_decoy)))
    monkeypatch.setattr(pbrun.os, "getpgid", lambda pid: pid)
    monkeypatch.setattr(pbrun.os, "killpg", lambda pid, sig: signals.append((pid, sig)))
    monkeypatch.setattr(pbrun, "_log", lambda *_args, **_kwargs: None)

    run.watch_v8([])

    assert signals == [(4101, signal.SIGINT)]
    assert run.run_v8 == []


def test_watch_v8_never_starts_internal_staging_configs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Atomic-save staging directories are never treated as PB8 instances."""

    run_root = tmp_path / "data" / "run_v8"
    stage = run_root / ".pbgui-v8-stage-0123456789abcdef"
    instance = run_root / "bybit_BTCUSDT"
    for directory in (stage, instance):
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "config.json").write_text("{}", encoding="utf-8")
    watched: list[str] = []
    run = pbrun.PBRun.__new__(pbrun.PBRun)
    run.pbgdir = tmp_path
    run.v8_path = str(run_root)
    run.name = "node-a"
    run.pb8dir = str(tmp_path / "pb8")
    run.pb8venv = str(tmp_path / "venv_pb8" / "bin" / "python")
    run.pb8_ready = True
    run.run_v8 = []
    monkeypatch.setattr(pbrun.psutil, "process_iter", lambda: iter(()))
    monkeypatch.setattr(pbrun.RunV8, "watch", lambda self: watched.append(self.user))

    run.watch_v8()

    assert watched == ["bybit_BTCUSDT"]
    assert [runner.user for runner in run.run_v8] == ["bybit_BTCUSDT"]


def test_pb8_config_change_triggers_immediate_rescan(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The one-second main-loop poll notices a materialized PB8 config immediately."""

    run_root = tmp_path / "data" / "run_v8"
    instance = run_root / "pb8_bot"
    instance.mkdir(parents=True)
    config = instance / "config.json"
    config.write_text('{"pbgui":{"version":1}}', encoding="utf-8")
    run = pbrun.PBRun.__new__(pbrun.PBRun)
    run.pbgdir = tmp_path
    run.v8_path = str(run_root)
    run._v8_runtime_signature = run._current_v8_runtime_signature()
    rescans: list[bool] = []
    monkeypatch.setattr(run, "watch_v8", lambda: rescans.append(True))
    monkeypatch.setattr(pbrun, "_log", lambda *_args, **_kwargs: None)
    config.write_text('{"pbgui":{"version":2}}', encoding="utf-8")

    changed = run.has_v8_runtime_changed()

    assert changed is True
    assert rescans == [True]


def test_runv8_stop_escalates_process_group_sigint_term_kill(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Graceful PB8 stop is SIGINT-first and has bounded TERM/KILL escalation."""

    instance = _write_pb8_instance(tmp_path)
    runner = _runner(tmp_path, instance)
    waits = iter((False, False, True))
    process = SimpleNamespace(pid=4321)
    process.cmdline = lambda: runner.command
    process.cwd = lambda: runner.pb8dir
    process.create_time = lambda: 100.0
    signals: list[tuple[int, signal.Signals]] = []
    monkeypatch.setattr(runner, "pid", lambda: process)
    monkeypatch.setattr(runner, "_wait_stopped", lambda _process, _timeout: next(waits))
    monkeypatch.setattr(pbrun.os, "getpgid", lambda _pid: 4321)
    monkeypatch.setattr(pbrun.os, "killpg", lambda group, sig: signals.append((group, sig)))
    monkeypatch.setattr(pbrun, "_log", lambda *_args, **_kwargs: None)

    runner.stop()

    assert signals == [
        (4321, signal.SIGINT),
        (4321, signal.SIGTERM),
        (4321, signal.SIGKILL),
    ]


@pytest.mark.parametrize("changed_field", ["create_time", "argv", "cwd"])
def test_runv8_stop_revalidates_complete_identity_before_every_signal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    changed_field: str,
) -> None:
    """PID reuse or argv/cwd replacement aborts escalation before another signal."""

    instance = _write_pb8_instance(tmp_path)
    runner = _runner(tmp_path, instance)
    state = {"create_time": 100.0, "argv": runner.command, "cwd": runner.pb8dir}
    process = SimpleNamespace(
        pid=4321,
        create_time=lambda: state["create_time"],
        cmdline=lambda: state["argv"],
        cwd=lambda: state["cwd"],
    )
    signals: list[signal.Signals] = []

    def signal_group(_group: int, sig: signal.Signals) -> None:
        signals.append(sig)
        if changed_field == "create_time":
            state["create_time"] = 101.0
        elif changed_field == "argv":
            state["argv"] = ["unrelated"]
        else:
            state["cwd"] = str(tmp_path / "other")

    monkeypatch.setattr(runner, "_wait_stopped", lambda _process, _timeout: False)
    monkeypatch.setattr(pbrun.os, "getpgid", lambda _pid: 4321)
    monkeypatch.setattr(pbrun.os, "killpg", signal_group)
    monkeypatch.setattr(pbrun, "_log", lambda *_args, **_kwargs: None)

    runner.stop(process)

    assert signals == [signal.SIGINT]


def test_runv8_update_marker_does_not_stop_an_existing_bot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Runtime update readiness gates starts but does not terminate a live bot."""

    instance = _write_pb8_instance(tmp_path)
    runner = _runner(tmp_path, instance)
    marker = tmp_path / "data" / "locks" / "pb8-runtime-invalid"
    marker.parent.mkdir(parents=True)
    marker.write_text("updating\n", encoding="utf-8")
    process = SimpleNamespace(create_time=lambda: instance.stat().st_mtime + 100)
    stopped: list[object] = []
    monkeypatch.setattr(runner, "pid", lambda: process)
    monkeypatch.setattr(runner, "load", lambda: True)
    monkeypatch.setattr(runner, "_cluster_gate_result", lambda: {"ok": True, "status": "allowed"})
    monkeypatch.setattr(runner, "_runtime_ready", lambda: pytest.fail("live bot was subjected to a start-only gate"))
    monkeypatch.setattr(runner, "stop", lambda selected=None: stopped.append(selected))

    runner.watch()

    assert stopped == []


def test_runv8_desired_stop_remains_authoritative_during_update(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An update marker never masks a desired-state stop or tombstone."""

    instance = _write_pb8_instance(tmp_path)
    runner = _runner(tmp_path, instance)
    marker = tmp_path / "data" / "locks" / "pb8-runtime-invalid"
    marker.parent.mkdir(parents=True)
    marker.write_text("updating\n", encoding="utf-8")
    process = SimpleNamespace()
    stopped: list[object] = []
    monkeypatch.setattr(runner, "pid", lambda: process)
    monkeypatch.setattr(runner, "load", lambda: True)
    monkeypatch.setattr(
        runner,
        "_cluster_gate_result",
        lambda: {"ok": False, "status": "tombstoned", "reason": "removed"},
    )
    monkeypatch.setattr(runner, "stop", lambda selected=None: stopped.append(selected))
    monkeypatch.setattr(runner, "_log_block", lambda *_args: None)

    runner.watch()

    assert stopped == [process]


def test_runv8_crash_backoff_is_exponential_and_bounded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repeated early exits cannot create an unbounded or tight restart loop."""

    instance = _write_pb8_instance(tmp_path)
    runner = _runner(tmp_path, instance)
    clock = {"now": 1000.0}
    monkeypatch.setattr(pbrun, "time", lambda: clock["now"])
    monkeypatch.setattr(pbrun, "_log", lambda *_args, **_kwargs: None)

    delays = []
    for _ in range(12):
        runner._record_crash()
        delays.append(runner._next_start_at - clock["now"])

    assert delays[:4] == [5, 10, 20, 40]
    assert delays[-1] == pbrun.PB8_BACKOFF_MAX_SECONDS
    assert all(delay <= pbrun.PB8_BACKOFF_MAX_SECONDS for delay in delays)


def test_runv8_launch_failure_enters_backoff(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A missing or unexecutable PB8 CLI cannot create a tight launch loop."""

    instance = _write_pb8_instance(tmp_path)
    runner = _runner(tmp_path, instance)
    monkeypatch.setattr(runner, "is_running", lambda: False)
    monkeypatch.setattr(runner, "_runtime_ready", lambda: True)
    monkeypatch.setattr(pbrun.subprocess, "Popen", lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("blocked")))
    monkeypatch.setattr(pbrun, "_log", lambda *_args, **_kwargs: None)

    assert runner.start() is False
    assert runner._crash_count == 1
    assert runner._next_start_at > pbrun.time()


def test_memory_restart_uses_runv8_graceful_stop(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Low-memory selection includes PB8 and restarts through its stop method."""

    run = pbrun.PBRun.__new__(pbrun.PBRun)
    events: list[str] = []
    run.run_v7 = [SimpleNamespace(user="v7", memory=SimpleNamespace(rss=10, uss=0))]
    run.run_v8 = [
        SimpleNamespace(
            user="v8",
            memory=SimpleNamespace(rss=100, uss=50),
            stop=lambda: events.append("stop"),
            start=lambda: events.append("start"),
        )
    ]
    monkeypatch.setattr(pbrun.psutil, "virtual_memory", lambda: SimpleNamespace(available=0))
    monkeypatch.setattr(pbrun.psutil, "swap_memory", lambda: SimpleNamespace(free=0))
    monkeypatch.setattr(pbrun, "_log", lambda *_args, **_kwargs: None)

    run.watch_memory()

    assert events == ["stop", "start"]


def test_pbrun_refreshes_added_and_removed_runtime_profiles(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """INI changes activate fresh PB8 and stop runners for removed profiles."""

    settings: dict[str, str] = {
        "pbname": "node-a",
        "pb7dir": "/pb7",
        "pb7venv": "/venv7/bin/python",
    }
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(pbrun, "CoinData", lambda: SimpleNamespace())
    monkeypatch.setattr(pbrun.pbgui_purefunc, "load_ini_snapshot", lambda: _snapshot(**settings))
    run = pbrun.PBRun()
    stopped: list[str] = []
    run.run_v7 = [SimpleNamespace(stop=lambda: stopped.append("v7"))]

    settings.clear()
    settings.update(
        pbname="node-a",
        pb8dir="/pb8",
        pb8venv="/venv8/bin/python",
    )
    v7_changed, v8_changed = run.refresh_runtime_config()

    assert (v7_changed, v8_changed) == (True, True)
    assert stopped == ["v7"]
    assert run.pb7_ready is False
    assert run.pb8_ready is True
    assert (run.pb8dir, run.pb8venv) == ("/pb8", "/venv8/bin/python")

    run.run_v8 = [SimpleNamespace(stop=lambda: stopped.append("v8"))]
    settings.clear()
    _v7_changed, v8_changed = run.refresh_runtime_config()

    assert v8_changed is True
    assert stopped == ["v7", "v8"]
    assert run.pb8_ready is False
    assert run.run_v8 == []


def test_runv8_optional_cluster_map_applies_assignment_version_and_manifest_gate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PB8-specific desired state is enforced, while its absence remains standalone."""

    from master.cluster_state import build_config_manifest, compute_config_manifest_hash, ensure_local_identity

    instance = _write_pb8_instance(tmp_path)
    runner = _runner(tmp_path, instance)
    monkeypatch.setattr(pbrun, "_log", lambda *_args, **_kwargs: None)
    assert runner.load() is True
    cluster_root = tmp_path / "data" / "cluster"
    identity = ensure_local_identity(cluster_root, pbname="node-a")
    standalone = {
        "cluster_id": identity["cluster_id"],
        "instances": {},
        "tombstones": {},
    }
    (cluster_root / "desired_state.json").write_text(json.dumps(standalone), encoding="utf-8")
    assert runner._cluster_gate_result()["ok"] is True

    standalone["pb8_tombstones"] = {instance.name: {"version": "3"}}
    (cluster_root / "desired_state.json").write_text(json.dumps(standalone), encoding="utf-8")
    assert runner._cluster_gate_result()["status"] == "tombstoned"
    standalone["pb8_tombstones"] = {}

    standalone["pb8_instances"] = {
        instance.name: {
            "desired_state": "running",
            "assigned_host": identity["node_id"],
            "version": "3",
            "config_manifest_hash": compute_config_manifest_hash(build_config_manifest(instance)),
            "conflicted": False,
        }
    }
    (cluster_root / "desired_state.json").write_text(json.dumps(standalone), encoding="utf-8")
    assert runner._cluster_gate_result()["status"] == "allowed"
    standalone["pb8_instances"][instance.name]["version"] = "4"
    (cluster_root / "desired_state.json").write_text(json.dumps(standalone), encoding="utf-8")
    assert runner._cluster_gate_result()["status"] == "version_mismatch"


def test_monitor_agent_requires_pbrun_for_local_pb8_assignment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PB8-only hosts keep the shared PBRun systemd service expected."""

    import monitor_agent

    instance = _write_pb8_instance(tmp_path)
    monkeypatch.setattr(monitor_agent, "PBGDIR", tmp_path)

    assert monitor_agent._pbrun_required_for_host("node-a") is True
    config = json.loads((instance / "config.json").read_text(encoding="utf-8"))
    config["pbgui"]["enabled_on"] = "node-b"
    (instance / "config.json").write_text(json.dumps(config), encoding="utf-8")
    assert monitor_agent._pbrun_required_for_host("node-a") is False


def test_pb8_only_setup_enables_pbrun_and_unit_allows_managed_shutdown() -> None:
    """PB8-only provisioning enables PBRun and systemd never kills its bot children."""

    playbook = (ROOT_DIR / "vps-setup.yml").read_text(encoding="utf-8")
    pb8_update = (ROOT_DIR / "vps-update-pb8.yml").read_text(encoding="utf-8")
    systemd_setup = (ROOT_DIR / "setup" / "setup_systemd.sh").read_text(encoding="utf-8")

    assert "(['pbrun'] if ((install_pb7 | bool) or (install_pb8 | bool)) else [])" in playbook
    assert "(['pbgui-pbrun.service'] if ((install_pb7 | bool) or (install_pb8 | bool)) else [])" in playbook
    assert "KillSignal=SIGTERM" in systemd_setup
    assert 'write_unit "pbgui-pbrun.service" "PBGui PBRun Service" "PBRun.py" "" "process"' in systemd_setup
    assert "TimeoutStopSec=30" in systemd_setup
    assert "Detect PB8-only support in installed PBRun" in pb8_update
    assert "Disable incompatible PBRun on PB8-only hosts" in pb8_update
    assert pb8_update.index("Mark PB8 runtime unavailable") < pb8_update.index(
        "Wait for already-starting PB8 runners to cross the launch boundary"
    )
    assert pb8_update.index("Wait for already-starting PB8 runners to cross the launch boundary") < pb8_update.index(
        "Checkout verified official Passivbot v8 commit"
    )


def test_pbrun_controller_shutdown_does_not_stop_managed_bots(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The API Cluster join stop contract terminates PBRun without stopping bots."""

    stopped: list[str] = []

    class FakeRun:
        """Provide an already-idle controller with managed bot sentinels."""

        pbgdir = tmp_path
        pb7_ready = False
        pb8_ready = False
        pidfile = tmp_path / "missing.pid"
        run_v7 = [SimpleNamespace(stop=lambda: stopped.append("v7"))]
        run_v8 = [SimpleNamespace(stop=lambda: stopped.append("v8"))]

        @staticmethod
        def is_running() -> bool:
            return False

        @staticmethod
        def save_pid() -> None:
            return None

        @staticmethod
        def refresh_runtime_config() -> tuple[bool, bool]:
            return False, False

    class AlreadyStoppedEvent:
        """Skip the daemon loop while exercising orderly cleanup."""

        @staticmethod
        def set() -> None:
            return None

        @staticmethod
        def is_set() -> bool:
            return True

    class Capability:
        """Avoid writing a real process-capability heartbeat."""

        def __init__(self, *_args, **_kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

    import credential_process_registry

    monkeypatch.setattr(pbrun, "PBRun", FakeRun)
    monkeypatch.setattr(pbrun.threading, "Event", AlreadyStoppedEvent)
    monkeypatch.setattr(pbrun, "_wait_for_cluster_boot_sync", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(pbrun, "_log", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(credential_process_registry, "ProcessCapabilityHeartbeat", Capability)

    pbrun.main()

    assert stopped == []
