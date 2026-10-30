"""Offline lifecycle regressions with isolated files and process/systemd doubles."""

import asyncio
import ast
import importlib.util
import configparser
import json
from pathlib import Path
import subprocess
import time
from types import SimpleNamespace

import psutil
import pytest

import monitor_agent as agent
import pbcoindata_lifecycle as lifecycle
import pbcoindata_controller as legacy_controller
import pbcoindata_process as legacy_process
from test_pbcoindata_process import retained_zombie


class Process:
    """Small process snapshot double that never inspects real processes."""

    def __init__(self, argv, cwd):
        self.argv, self.directory = argv, cwd

    def cmdline(self):
        """Return argv or simulate access failure."""
        if isinstance(self.argv, Exception):
            raise self.argv
        return self.argv

    def cwd(self):
        """Return the synthetic process working directory."""
        return str(self.directory)


@pytest.fixture
def consumer(tmp_path, monkeypatch):
    """Create only synthetic consumer configs, using an injectable loader."""
    monkeypatch.setattr(lifecycle, "_log", lambda *a, **k: None)
    instance = tmp_path / "data" / "run_v7" / "test-bot"
    instance.mkdir(parents=True)
    script = tmp_path / "pb7" / "src" / "main.py"
    runtime = instance / "config_run.json"
    desired = instance / "config.json"
    desired.write_text('{"pbgui": {"dynamic_ignore": false}}')
    runtime.write_text(desired.read_text())
    process = Process(["python", "-u", str(script), str(runtime)], tmp_path)
    return SimpleNamespace(root=tmp_path, instance=instance, script=script, runtime=runtime, desired=desired, process=process)


def demand(consumer, processes=None):
    """Evaluate demand using one entirely synthetic process snapshot."""
    return lifecycle.consumer_demand(
        consumer.root, str(consumer.root / "pb7"),
        process_iter=lambda: processes if processes is not None else [consumer.process],
        config_loader=lambda path: json.loads(path.read_text()),
    )


@pytest.mark.parametrize("desired,runtime,expected", [
    (False, False, "IDLE"), (True, False, "ACTIVE"), (False, True, "ACTIVE"),
    (True, True, "ACTIVE"), (None, False, "UNKNOWN"), (False, None, "UNKNOWN"),
    ("broken", False, "UNKNOWN"), (False, "broken", "UNKNOWN"),
    (False, "false", "UNKNOWN"), ("broken", True, "ACTIVE"), (True, None, "ACTIVE"),
])
def test_desired_and_materialized_configs(consumer, desired, runtime, expected):
    """Either positive config wins; missing, corrupt or ambiguous state is unknown."""
    for path, value in ((consumer.desired, desired), (consumer.runtime, runtime)):
        if value is None:
            path.unlink()
        else:
            path.write_text("{" if value == "broken" else json.dumps({"pbgui": {"dynamic_ignore": value}}))
    assert demand(consumer) == expected


@pytest.mark.parametrize("kind,expected", [
    ("empty", "IDLE"), ("pb8", "IDLE"), ("relative", "ACTIVE"),
    ("other-main", "IDLE"), ("other-root", "IDLE"), ("denied", "UNKNOWN"),
    ("gone", "IDLE"), ("no-runtime-arg", "UNKNOWN"), ("symlink", "UNKNOWN"),
    ("bad-metadata", "UNKNOWN"),
])
def test_process_identity_and_errors(consumer, kind, expected):
    """Exact local paths, cwd resolution and process failures control demand."""
    consumer.desired.write_text('{"pbgui": {"dynamic_ignore": true}}')
    processes = [consumer.process]
    if kind == "empty":
        processes = []
    elif kind == "pb8":
        consumer.testArguments = ["passivbot", "live", str(consumer.runtime)]
    elif kind == "relative":
        consumer.testArguments = ["python", "pb7/src/main.py", "data/run_v7/test-bot/config_run.json"]
    elif kind == "other-main":
        consumer.testArguments[2] = str(consumer.root / "foreign" / "main.py")
    elif kind == "other-root":
        consumer.testArguments[-1] = str(consumer.root / "other" / "data/run_v7/test-bot/config_run.json")
    elif kind in {"denied", "gone"}:
        consumer.testArguments = psutil.AccessDenied(1) if kind == "denied" else psutil.NoSuchProcess(1)
    elif kind == "no-runtime-arg":
        consumer.testArguments.pop()
    elif kind == "symlink":
        consumer.runtime.unlink()
        target = consumer.root / "external.json"
        target.write_text('{}')
        consumer.runtime.symlink_to(target)
    elif kind == "bad-metadata":
        consumer.desired.write_text('{"pbgui": []}')
    assert demand(consumer, processes) == expected


@pytest.mark.parametrize("role", ["master", "", "unexpected", "slave"])
@pytest.mark.parametrize("credential", [True, False, None])
@pytest.mark.parametrize("state", ["ACTIVE", "IDLE", "UNKNOWN"])
def test_role_credential_demand_matrix(tmp_path, monkeypatch, role, credential, state):
    """Only an explicit slave applies consumer demand; UNKNOWN never acts."""
    cfg = configparser.ConfigParser()
    cfg.read_dict({"main": {"role": role}})
    monkeypatch.setattr(lifecycle, "consumer_demand", lambda *a, **k: state)
    result = lifecycle.lifecycle_decision(tmp_path, credential, config=cfg)
    expected = credential if role != "slave" or state == "ACTIVE" else (False if state == "IDLE" else None)
    assert result["expected"] is expected


def test_snapshot_only_once_and_failure_is_unknown(consumer):
    """A decision uses one snapshot; enumeration failure never proves idle."""
    calls = []

    def snapshot():
        """Record the single process enumeration."""
        calls.append(1)
        return [consumer.process, consumer.process]

    assert lifecycle.consumer_demand(consumer.root, str(consumer.root / "pb7"), process_iter=snapshot, config_loader=lambda p: json.loads(p.read_text())) == "IDLE"
    assert calls == [1]
    assert lifecycle.consumer_demand(consumer.root, process_iter=lambda: (_ for _ in ()).throw(OSError())) == "UNKNOWN"


@pytest.mark.parametrize("exchange_names", [("bybit", "bybit"), ("bybit", "okx")])
def test_complete_snapshot_collects_every_running_consumer(consumer, exchange_names):
    """All running local PB7 consumers contribute, excluding stopped and foreign bots."""
    processes = []
    accounts = {}
    for index, exchange in enumerate((*exchange_names, "hyperliquid")):
        instance = consumer.instance.parent / f"bot-{index}"
        instance.mkdir()
        account = f"account-{index}"
        accounts[account] = {"exchange": exchange, "key": "synthetic-secret"}
        for filename in ("config.json", "config_run.json"):
            (instance / filename).write_text(json.dumps({"pbgui": {"dynamic_ignore": True}, "live": {"user": account}}))
        if index < len(exchange_names):
            processes.append(Process(["python", str(consumer.script), str(instance / "config_run.json")], consumer.root))
    processes += [Process(["python", str(consumer.root / "foreign/main.py"), str(consumer.runtime)], consumer.root),
                  Process(["passivbot", "live", str(consumer.runtime)], consumer.root)]
    account_path = consumer.root / "pb7/api-keys.json"
    account_path.parent.mkdir()
    account_path.write_text(json.dumps(accounts))
    snapshot = lifecycle.consumer_snapshot(consumer.root, str(account_path.parent), process_iter=lambda: processes,
                                           config_loader=lambda p: json.loads(p.read_text()))
    assert snapshot == lifecycle.ConsumerSnapshot("ACTIVE", True, frozenset(exchange_names))
    assert "synthetic-secret" not in repr(snapshot)


@pytest.mark.parametrize("fault", ["missing-file", "invalid-file", "unreadable", "missing-account", "different-user", "different-demand", "unknown-process"])
def test_incomplete_snapshot_cannot_limit_active_consumers(consumer, monkeypatch, fault):
    """Known demand survives incomplete account/config/process coverage without secrets."""
    payload = {"pbgui": {"dynamic_ignore": True}, "live": {"user": "account"}}
    for path in (consumer.desired, consumer.runtime):
        path.write_text(json.dumps(payload))
    account_path = consumer.root / "pb7/api-keys.json"
    account_path.parent.mkdir()
    account_path.write_text(json.dumps({"account": {"exchange": "bybit", "key": "synthetic-secret"}}))
    processes = [consumer.process]
    logs = []
    monkeypatch.setattr(lifecycle, "_log", lambda *a, **k: logs.append((a, k)))
    if fault == "missing-file":
        account_path.unlink()
    elif fault == "invalid-file":
        account_path.write_text("{synthetic-secret")
    elif fault == "unreadable":
        original_read = Path.read_text

        def read(path, *args, **kwargs):
            """Deny only the synthetic account projection."""
            if path == account_path:
                raise PermissionError("synthetic-secret")
            return original_read(path, *args, **kwargs)

        monkeypatch.setattr(Path, "read_text", read)
    elif fault == "missing-account":
        account_path.write_text("{}")
    elif fault == "different-user":
        payload["live"]["user"] = "other-account"
        consumer.runtime.write_text(json.dumps(payload))
    elif fault == "different-demand":
        payload["pbgui"]["dynamic_ignore"] = False
        consumer.runtime.write_text(json.dumps(payload))
    else:
        processes.append(Process(psutil.AccessDenied(99), consumer.root))
    snapshot = lifecycle.consumer_snapshot(consumer.root, str(account_path.parent), process_iter=lambda: processes,
                                           config_loader=lambda p: json.loads(p.read_text()))
    assert snapshot.demand == "ACTIVE" and not snapshot.complete
    assert "synthetic-secret" not in repr(snapshot) + str(logs)


@pytest.mark.parametrize("location", ["root", "runtime"])
@pytest.mark.parametrize("failure", ["symlink-loop", "permission"])
def test_root_resolution_errors_are_unknown_and_redacted(tmp_path, monkeypatch, location, failure):
    """Root/runtime resolution errors return UNKNOWN before any process access."""
    marker = "test-only-sensitive-path"
    root = tmp_path / marker if location == "root" else tmp_path
    target = root if location == "root" else root / "data" / "run_v7"
    target.parent.mkdir(parents=True, exist_ok=True)
    logs = []
    monkeypatch.setattr(lifecycle, "_log", lambda *a, **k: logs.append((a, k)))
    if failure == "symlink-loop":
        target.symlink_to(target.name, target_is_directory=True)
    else:
        original_resolve = Path.resolve

        def resolve(path, *args, **kwargs):
            """Deny only the path under review without accessing external state."""
            if path == target:
                raise PermissionError(marker)
            return original_resolve(path, *args, **kwargs)

        monkeypatch.setattr(Path, "resolve", resolve)
    assert lifecycle.consumer_demand(root, process_iter=lambda: pytest.fail("No process scan with unknown roots")) == "UNKNOWN"
    assert len(logs) == 1 and logs[0][1]["level"] == "WARNING"
    assert marker not in str(logs)
    cfg = configparser.ConfigParser()
    cfg.read_dict({"main": {"role": "slave"}})
    decision = lifecycle.lifecycle_decision(root, True, config=cfg)
    assert decision["demand"] == "UNKNOWN" and decision["expected"] is None


def policy(state="IDLE", expected=False):
    """Build a fresh secret-free new-policy payload."""
    return {"policy_version": 1, "generated_at": time.time(), "role": "slave", "demand": state, "expected": expected, "reason": "test demand"}


def status(state="running", pid=23, manager="systemd"):
    """Build actual service state independently of expected state."""
    return {"status": state, "pid": pid, "manager": manager, "unit": agent.PBGUI_SERVICES["PBCoinData"][0],
            "systemd_state": "active" if state == "running" else "inactive", "main_pid": pid or 0, "error": None, "was_restarted": False}


@pytest.fixture
def controller(tmp_path, monkeypatch):
    """Replace all process and systemctl access and keep real locks in tmp_path."""
    state = SimpleNamespace(decision=policy(), actual=status(), heal=True, calls=[], post=status("stopped", None), owned=True)
    monkeypatch.setattr(agent, "PBGDIR", tmp_path)
    monkeypatch.setattr(agent, "_log", lambda *a, **k: None)
    monkeypatch.setattr(agent, "_pbcoindata_lifecycle", lambda: dict(state.decision))
    monkeypatch.setattr(agent, "_auto_heal_enabled", lambda: state.heal)
    monkeypatch.setattr(agent, "_service_status", lambda *a: dict(state.actual))
    monkeypatch.setattr(agent, "_owned_coindata_process", lambda pid: state.owned)
    import pbcoindata_process
    state.real_preflight = pbcoindata_process.unload_preflight
    monkeypatch.setattr(pbcoindata_process, "manager_state", lambda root: {"manager": "unknown", "reason": "synthetic unknown"})
    monkeypatch.setattr(pbcoindata_process, "transition_status", lambda root: {"basis": "synthetic verified operator transition"})
    monkeypatch.setattr(pbcoindata_process, "unload_preflight", lambda *a, **k: {"allowed": True, "reason": "synthetic structural proof"})
    monkeypatch.setattr(pbcoindata_process, "inspect_pid", lambda root, pid: {"status": "running", "pid": pid})

    def run(argv, **kwargs):
        """Capture a synthetic service action, never invoke systemctl."""
        state.calls.append((argv, kwargs["timeout"]))
        return SimpleNamespace(returncode=0)

    def read(unit, *, timeout):
        """Capture the bounded post-stop status query."""
        state.calls.append(("read", timeout))
        return dict(state.post) if state.post else None

    monkeypatch.setattr(agent.subprocess, "run", run)
    monkeypatch.setattr(agent, "_systemd_service_status", read)
    monkeypatch.setattr(agent, "_pid_file_service_status", lambda *a: status("stopped", None, "legacy"))
    return state


def test_idle_stop_and_bounded_status(controller):
    """Stop has 40 seconds and one status read at most five seconds; idle is explicit."""
    result = agent._coindata_status()
    assert result["status"] == "idle"
    assert result["expected"] is False
    assert controller.calls[0] == (["systemctl", "--user", "stop", "pbgui-pbcoindata.service"], 40)
    assert controller.calls[1][0] == "read"
    assert 0 < controller.calls[1][1] <= 5


@pytest.mark.parametrize("case", ["unknown", "disabled-heal", "legacy", "foreign", "master", "stale-exec-pid"])
def test_observe_without_automatic_stop(controller, case):
    """Unknown, disabled healing, master and non-owned processes are observation only."""
    if case == "unknown":
        controller.decision = policy("UNKNOWN", None)
    elif case == "disabled-heal":
        controller.heal = False
    elif case == "legacy":
        controller.actual["manager"] = "legacy"
    elif case == "foreign":
        controller.owned = False
    elif case == "stale-exec-pid":
        controller.actual["main_pid"] = 0
    else:
        controller.decision.update(role="master", expected=True)
    assert agent._coindata_status()["status"] == "running"
    assert not controller.calls


@pytest.mark.parametrize("case", ["error", "timeout", "still-running", "transition", "unavailable"])
def test_stop_failures_never_claim_idle(controller, monkeypatch, case):
    """Failed stop clients and unconfirmed final states remain visible."""
    if case in {"error", "timeout"}:
        def fail(*args, **kwargs):
            """Simulate a failed or timed-out systemctl client."""
            if case == "timeout":
                raise subprocess.TimeoutExpired("systemctl", 40)
            return SimpleNamespace(returncode=1)
        monkeypatch.setattr(agent.subprocess, "run", fail)
    elif case == "still-running":
        controller.post = status()
    elif case == "transition":
        controller.post.update(systemd_state="deactivating")
    else:
        controller.post = None
    result = agent._coindata_status()
    assert result["status"] != "idle"
    if case in {"error", "timeout"}:
        assert result["stop_error"]


def test_lock_order_recheck_and_release(controller, monkeypatch, tmp_path):
    """Locks follow all-services/pbcoindata, share five seconds and recheck demand."""
    acquired, released = [], []

    def acquire(root, service, action, *, timeout):
        """Change demand after locking to simulate a concurrent bot start."""
        assert root == tmp_path and 0 <= timeout <= 5
        acquired.append(service)
        if service == "pbcoindata":
            controller.decision = policy("ACTIVE", True)
        return SimpleNamespace(release=lambda: released.append(service))

    monkeypatch.setattr(agent, "acquire_service_lifecycle_lock", acquire)
    assert agent._coindata_status()["status"] == "running"
    assert not controller.calls
    assert acquired == ["all-services", "pbcoindata"]
    assert released == acquired[::-1]


def test_lock_conflict_is_observation_only(controller, monkeypatch):
    """A held lifecycle lock never triggers an unprotected action."""
    def busy(*a, **k):
        """Simulate another lifecycle owner."""
        raise agent.ServiceLifecycleBusyError("busy")
    monkeypatch.setattr(agent, "acquire_service_lifecycle_lock", busy)
    assert agent._coindata_status()["lifecycle_error"]
    assert not controller.calls


def test_simulated_start_stop_recovery_cycles(controller, monkeypatch):
    """Idle unload, consumer start, unknown observation and last stop converge."""
    restarts = []
    monkeypatch.setattr(agent, "_restart_systemd_service", lambda *a: (restarts.append(a) is None, ""))
    assert agent._coindata_status()["status"] == "idle"
    controller.actual = status("stopped", None)
    controller.decision = policy("ACTIVE", True)
    assert agent._coindata_status()["status"] == "restarting"
    controller.decision = policy("UNKNOWN", None)
    assert agent._coindata_status()["status"] == "stopped"
    assert len(restarts) == 1
    controller.actual = status()
    controller.decision = policy()
    assert agent._coindata_status()["status"] == "idle"


@pytest.mark.parametrize("argv,owned", [(["python", "PBCoinData.py"], True), (["python", "-u", "PBCoinData.py"], True), (["python", "foreign.py", "PBCoinData.py"], False), (["node", "PBCoinData.py"], False)])
def test_owned_process_requires_script_position(tmp_path, monkeypatch, argv, owned):
    """A script name supplied merely as another program's argument is not ownership."""
    monkeypatch.setattr(agent, "PBGDIR", tmp_path)
    monkeypatch.setattr(psutil, "Process", lambda pid: Process(argv, tmp_path))
    assert agent._owned_coindata_process(23) is owned


def test_same_pid_is_systemd_and_different_pid_is_legacy(monkeypatch):
    """The PID file does not turn the systemd MainPID into a legacy process."""
    monkeypatch.setattr(agent, "_systemd_service_status", lambda unit: status())
    monkeypatch.setattr(agent, "_pid_file_service_status", lambda *a: status())
    assert agent._service_status("unit", "pid", "script")["manager"] == "systemd"
    monkeypatch.setattr(agent, "_pid_file_service_status", lambda *a: status(pid=99))
    assert agent._service_status("unit", "pid", "script")["manager"] == "legacy"


def test_daemon_wrapper_keeps_unknown_active(tmp_path, monkeypatch):
    """The compatibility wrapper cannot turn failed scans into idle."""
    spec = importlib.util.spec_from_file_location("PBCoinData_lifecycle_test", Path(__file__).resolve().parents[1] / "PBCoinData.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(lifecycle, "consumer_demand", lambda *a, **k: "UNKNOWN")
    assert module.CoinData._has_dynamic_ignore_bots() is True


@pytest.mark.parametrize("error_type", [PermissionError, UnicodeDecodeError, configparser.MissingSectionHeaderError])
def test_daemon_wrapper_ini_errors_are_unknown_and_redacted(tmp_path, monkeypatch, error_type):
    """INI read failures keep demand active and never log exception contents."""
    spec = importlib.util.spec_from_file_location("PBCoinData_ini_error_test", Path(__file__).resolve().parents[1] / "PBCoinData.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.chdir(tmp_path)
    marker = "test-only-sensitive-value"
    errors = {
        PermissionError: PermissionError(marker),
        UnicodeDecodeError: UnicodeDecodeError("utf-8", b"\xff", 0, 1, marker),
        configparser.MissingSectionHeaderError: configparser.MissingSectionHeaderError("test.ini", 1, marker),
    }
    logs = []

    def fail_snapshot(*args, **kwargs):
        """Simulate an unreadable INI without accessing runtime configuration."""
        raise errors[error_type]

    monkeypatch.setattr(module, "load_ini_snapshot", fail_snapshot)
    monkeypatch.setattr(module, "_log", lambda *a, **k: logs.append((a, k)))
    monkeypatch.setattr(lifecycle, "consumer_demand", lambda *a, **k: pytest.fail("No scan with unknown configuration"))
    assert module.CoinData._has_dynamic_ignore_bots() is True
    assert len(logs) == 1 and logs[0][1]["level"] == "WARNING"
    assert error_type.__name__ in logs[0][0][1]
    assert marker not in str(logs)


def test_real_daemon_corrupt_ini_waits_with_backoff_and_recovers(tmp_path, monkeypatch):
    """Run actual reload/role/wrapper paths through corrupt INI retries and repair."""
    import credential_process_registry

    spec = importlib.util.spec_from_file_location("PBCoinData_corrupt_ini_test", Path(__file__).resolve().parents[1] / "PBCoinData.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    coin_data_class = module.CoinData
    ini_path = tmp_path / "pbgui.ini"
    marker = "test-only-sensitive-value"
    ini_path.write_text(marker + "=invalid-header\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(module.pbgui_purefunc, "pbgui_ini_path", lambda: ini_path)
    waits, calls, logs = [], [], []

    class EndCycles(BaseException):
        """Finish after the recovered cycle without waiting or starting threads."""

    class UnexpectedDaemonError(BaseException):
        """Fail immediately on the old busy-loop path instead of hanging the test."""

    def log(*args, **kwargs):
        """Capture diagnostics; abort immediately if the no-wait error path fires."""
        logs.append((args, kwargs))
        if kwargs.get("level") == "ERROR":
            raise UnexpectedDaemonError

    def wait(*, timeout):
        """Simulate three failed reload cycles, then an atomic operator repair."""
        waits.append(timeout)
        calls.append("wait")
        if len(waits) == 3:
            repaired = tmp_path / "repaired.ini"
            repaired.write_text("[main]\nrole=slave\n", encoding="utf-8")
            repaired.replace(ini_path)
        elif len(waits) == 4:
            raise EndCycles

    def idle_scan(*args, **kwargs):
        """Only a repaired INI may reach the synthetic consumer scan."""
        assert len(waits) == 3
        return "IDLE"

    watcher = SimpleNamespace(
        _ini_path=ini_path, start=lambda: None, stop=lambda: calls.append("watcher-stop"),
        changed=SimpleNamespace(clear=lambda: None, wait=wait),
    )
    daemon = SimpleNamespace(
        _ini_watcher=watcher, _config_load_failed=False, _logged_idle=False,
        _apply_config_snapshot=lambda snapshot: True,
        is_running=lambda: False, save_pid=lambda: None,
        _is_master=coin_data_class._is_master,
        _has_dynamic_ignore_bots=coin_data_class._has_dynamic_ignore_bots,
        load_data=lambda: calls.append("data"), load_metadata=lambda: calls.append("metadata"),
        update_mappings=lambda **kwargs: calls.append("mappings"),
    )
    daemon.load_config = lambda: coin_data_class.load_config(daemon)
    heartbeat = SimpleNamespace(__enter__=lambda: None, close=lambda: calls.append("heartbeat-close"))
    monkeypatch.setattr(module, "_log", log)
    monkeypatch.setattr(module, "CoinData", lambda **kwargs: daemon)
    monkeypatch.setattr(lifecycle, "consumer_demand", idle_scan)
    monkeypatch.setattr(credential_process_registry, "ProcessCapabilityHeartbeat", lambda *a: heartbeat)
    with pytest.raises(EndCycles):
        module.main()
    assert waits == [2, 4, 8, 60]
    assert calls[:12] == ["data", "metadata", "mappings", "wait"] * 3
    assert calls[12:] == ["wait", "watcher-stop", "heartbeat-close"]
    assert daemon._config_load_failed is False and daemon._logged_idle is True
    assert marker not in str(logs)


def test_real_daemon_runtime_symlink_loop_waits_and_recovers(tmp_path, monkeypatch):
    """A broken runtime path reaches main's wait and can recover without restart."""
    import credential_process_registry

    spec = importlib.util.spec_from_file_location("PBCoinData_runtime_path_test", Path(__file__).resolve().parents[1] / "PBCoinData.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    coin_data_class = module.CoinData
    ini_path = tmp_path / "pbgui.ini"
    ini_path.write_text("[main]\nrole=slave\n", encoding="utf-8")
    run_root = tmp_path / "data" / "run_v7"
    run_root.parent.mkdir()
    run_root.symlink_to(run_root.name, target_is_directory=True)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(module.pbgui_purefunc, "pbgui_ini_path", lambda: ini_path)
    monkeypatch.setattr(lifecycle.psutil, "process_iter", lambda: [])
    waits, calls, logs = [], [], []

    class EndCycles(BaseException):
        """Stop after observing the repaired idle cycle, without a real wait."""

    class UnexpectedDaemonError(BaseException):
        """Make the previous tight-error-loop path fail immediately in this test."""

    def log(*args, **kwargs):
        """Capture only synthetic diagnostics and fail on an escaped helper error."""
        logs.append((args, kwargs))
        if kwargs.get("level") == "ERROR":
            raise UnexpectedDaemonError

    def wait(*, timeout):
        """Repair only the temporary symlink after the first real main cycle."""
        waits.append(timeout)
        calls.append("wait")
        if len(waits) == 1:
            assert daemon._logged_idle is False
            run_root.unlink()
            run_root.mkdir()
        else:
            raise EndCycles

    watcher = SimpleNamespace(
        _ini_path=ini_path, start=lambda: None, stop=lambda: calls.append("watcher-stop"),
        changed=SimpleNamespace(clear=lambda: None, wait=wait),
    )
    daemon = SimpleNamespace(
        _ini_watcher=watcher, _config_load_failed=False, _logged_idle=False,
        _apply_config_snapshot=lambda snapshot: True,
        is_running=lambda: False, save_pid=lambda: None,
        _is_master=coin_data_class._is_master,
        _has_dynamic_ignore_bots=coin_data_class._has_dynamic_ignore_bots,
        load_data=lambda: calls.append("data"), load_metadata=lambda: calls.append("metadata"),
        update_mappings=lambda **kwargs: calls.append("mappings"),
    )
    daemon.load_config = lambda: coin_data_class.load_config(daemon)
    heartbeat = SimpleNamespace(__enter__=lambda: None, close=lambda: calls.append("heartbeat-close"))
    monkeypatch.setattr(module, "_log", log)
    monkeypatch.setattr(lifecycle, "_log", log)
    monkeypatch.setattr(module, "CoinData", lambda **kwargs: daemon)
    monkeypatch.setattr(credential_process_registry, "ProcessCapabilityHeartbeat", lambda *a: heartbeat)
    with pytest.raises(EndCycles):
        module.main()
    assert waits == [60, 60]
    assert calls == ["data", "metadata", "mappings", "wait", "wait", "watcher-stop", "heartbeat-close"]
    assert daemon._logged_idle is True
    assert any("Consumer runtime path unavailable" in args[1] for args, _kwargs in logs)


@pytest.mark.parametrize("role", [None, "unexpected", "slave", "master"])
@pytest.mark.parametrize("demand_state", ["IDLE", "ACTIVE", "UNKNOWN"])
def test_real_daemon_cycle_preserves_non_master_idle(tmp_path, monkeypatch, role, demand_state):
    """Exercise main's cycle: non-masters idle only on proof, masters always work."""
    import credential_process_registry

    spec = importlib.util.spec_from_file_location("PBCoinData_cycle_test", Path(__file__).resolve().parents[1] / "PBCoinData.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    coin_data_class = module.CoinData
    cfg = configparser.ConfigParser()
    cfg.read_dict({"main": {} if role is None else {"role": role}})
    with (tmp_path / "pbgui.ini").open("w", encoding="utf-8") as handle:
        cfg.write(handle)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(module.pbgui_purefunc, "load_ini", lambda section, key: cfg.get(section, key, fallback=None))
    monkeypatch.setattr(module, "_log", lambda *a, **k: None)
    calls, scans = [], []

    class EndCycle(BaseException):
        """Exit the real infinite loop after its first wait, without a real delay."""

    def wait(*, timeout):
        """Record the scheduled wait and leave main through its cleanup block."""
        calls.append(("wait", timeout))
        raise EndCycle

    def scan(*args, **kwargs):
        """Return synthetic demand without any process or bot configuration access."""
        scans.append(1)
        return demand_state

    monkeypatch.setattr(lifecycle, "consumer_demand", scan)
    monkeypatch.setattr(lifecycle, "consumer_snapshot", lambda *a, **k: lifecycle.ConsumerSnapshot(
        demand_state, demand_state != "UNKNOWN", frozenset({"bybit"}) if demand_state == "ACTIVE" else frozenset()))
    watcher = SimpleNamespace(
        start=lambda: calls.append("watcher-start"), stop=lambda: calls.append("watcher-stop"),
        changed=SimpleNamespace(clear=lambda: None, wait=wait),
    )
    daemon = SimpleNamespace(
        _ini_watcher=watcher, _config_load_failed=False, _logged_idle=False,
        is_running=lambda: False, save_pid=lambda: calls.append("save-pid"),
        load_config=lambda: calls.append("config"),
        _is_master=coin_data_class._is_master,
        _has_dynamic_ignore_bots=coin_data_class._has_dynamic_ignore_bots,
        load_data=lambda: calls.append("data"), load_metadata=lambda: calls.append("metadata"),
        update_mappings=lambda **kwargs: calls.append(("selected", kwargs)) or calls.append("mappings"),
    )
    heartbeat = SimpleNamespace(
        __enter__=lambda: calls.append("heartbeat-start"), close=lambda: calls.append("heartbeat-close"),
    )
    monkeypatch.setattr(module, "CoinData", lambda **kwargs: daemon)
    monkeypatch.setattr(credential_process_registry, "ProcessCapabilityHeartbeat", lambda *a: heartbeat)
    with pytest.raises(EndCycle):
        module.main()
    idle = role != "master" and demand_state == "IDLE"
    assert daemon._logged_idle is idle
    assert [name for name in calls if name in ("data", "metadata", "mappings")] == ([] if idle else ["data", "metadata", "mappings"])
    assert len(scans) == (0 if role == "master" else 1)
    if not idle:
        expected_selection = {"exchanges": frozenset({"bybit"})} if role == "slave" and demand_state == "ACTIVE" else {}
        assert ("selected", expected_selection) in calls
    assert ("wait", 60) in calls
    assert calls[-2:] == ["watcher-stop", "heartbeat-close"]


@pytest.mark.parametrize("age,expected", [(0, False), (121, None), (-1, None)])
def test_metadata_freshness(age, expected):
    """Stale or future consumer metadata cannot authorize actions."""
    meta = policy()
    meta["generated_at"] = 1000 - age
    assert lifecycle.metadata_expected(meta, now=1000) is expected


@pytest.mark.parametrize("new_policy", [True, False])
def test_master_preserves_agent_policy_with_old_host_metadata(monkeypatch, new_policy):
    """New agents own healing even if host metadata still says CMC is required."""
    from master.async_monitor import VPSMonitor
    monitor = object.__new__(VPSMonitor)
    monitor.store = SimpleNamespace(host_meta={"local": {"credential_active": True}})
    monitor._auto_restart = True
    monitor._local_master_hostname = lambda: "local"
    calls = []

    async def payload(*a, **k):
        """Return synthetic agent data without any SSH or runtime reads."""
        check = status("stopped", None)
        if new_policy:
            check.update(pbcoindata_lifecycle=policy(), expected=False, status="idle")
        return {"services": {"PBCoinData": check}}

    async def restart(*a):
        """Capture attempted automatic restarts."""
        calls.append(a)
        return True

    monkeypatch.setattr(monitor, "_read_monitor_agent_json", payload)
    monkeypatch.setattr(monitor, "_restart_service", restart)
    result = asyncio.run(monitor._check_and_heal_services(["local"]))
    assert result["local"]["PBCoinData"]["status"] == ("idle" if new_policy else "restarting")
    assert len(calls) == (0 if new_policy else 1)


def test_migration_installed_idle_unit_not_required(tmp_path, monkeypatch):
    """Migration keeps the idle unit installed but excludes it from running requirements."""
    import api.services as services
    monkeypatch.setattr(services, "PBGDIR", tmp_path)
    monkeypatch.setattr(services, "_pbcoindata_lifecycle", lambda: policy())
    monkeypatch.setattr(services, "_pbcluster_required", lambda root: False)
    monkeypatch.setattr(services, "_pbrun_required_for_host", lambda root: False)
    monkeypatch.setattr(services, "_pbdata_required", lambda: False)
    required = services._migration_required_services(tmp_path)
    assert "pbcoindata" not in required
    assert "pbcoindata" in services._MIGRATION_DEFAULT_SERVICES
    assert services._SYSTEMD_SERVICE_UNITS["pbcoindata"] == "pbgui-pbcoindata.service"


def test_embedded_migration_excludes_intentionally_idle_unit(tmp_path):
    """Execute only the embedded pure migration function with systemd doubles."""
    from master.async_monitor import HOST_META_SCRIPT
    tree = ast.parse(HOST_META_SCRIPT.split("\n", 1)[1].rsplit("\nPY", 1)[0])
    function = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "build_systemd_migration_status")
    root = tmp_path / "pbgui"
    root.mkdir()
    python = tmp_path / "venv_pbgui/bin/python"
    python.parent.mkdir(parents=True, exist_ok=True)
    python.touch()
    namespace = {"Path": Path, "time": time, "run": lambda *a, **k: "systemctl", "systemd_user_manager_ok": lambda value: (True, ""),
                 "systemd_unit_status": lambda unit, exists: {"unit": unit, "exists": "yes", "enabled": "enabled", "active": "inactive" if "pbcoindata" in unit else "active"},
                 "collect_legacy_pbgui_processes": lambda root: [], "collect_legacy_cron_count": lambda root: 0}
    exec(compile(ast.Module(body=[function], type_ignores=[]), "embedded migration", "exec"), namespace)
    result = namespace[function.name](str(root), False, False, True, policy())
    assert result["units_ready"] is True
    assert all("pbcoindata" not in row["unit"] for row in result["required_units"])
    assert any("pbcoindata" in row["unit"] for row in result["units"])


def test_unknown_migration_blocks_before_system_changes(monkeypatch):
    """Unknown demand cannot become an excluded service that setup would stop."""
    import api.services as services
    from fastapi import HTTPException
    monkeypatch.setattr(services.subprocess, "run", lambda *a, **k: pytest.fail("No service action allowed"))
    with pytest.raises(HTTPException) as raised:
        services._migration_setup_command({"pbcoindata_lifecycle": policy("UNKNOWN", None), "required_services": ["api-server"]})
    assert raised.value.status_code == 409


def test_legacy_process_appearing_during_stop_remains_visible(controller, monkeypatch):
    """An independent legacy process is never hidden by an inactive unit result."""
    monkeypatch.setattr(agent, "_pid_file_service_status", lambda *a: status(pid=99, manager="legacy"))
    result = agent._coindata_status()
    assert result["status"] == "running"
    assert result["pid"] == 99 and result["manager"] == "legacy"


def test_migration_uses_one_decision_for_required_units_and_metadata(tmp_path, monkeypatch):
    """A second, different consumer snapshot cannot contradict the migration plan."""
    import api.services as services
    monkeypatch.setattr(services, "PBGDIR", tmp_path)
    monkeypatch.setattr(services, "_read_legacy_crontab", lambda: {"entries": []})
    monkeypatch.setattr(services, "_collect_pbgui_daemon_processes", lambda: [])
    monkeypatch.setattr(services, "_migration_systemd_units", lambda: [])
    monkeypatch.setattr(services, "load_ini", lambda *a: "")
    monkeypatch.setattr(services, "_detect_pbgui_python", lambda: "python")
    monkeypatch.setattr(services, "_pbcluster_required", lambda root: False)
    monkeypatch.setattr(services, "_pbrun_required_for_host", lambda root: False)
    monkeypatch.setattr(services, "_pbdata_required", lambda: False)
    decisions = iter([policy("ACTIVE", True)])
    monkeypatch.setattr(services, "_pbcoindata_lifecycle", lambda: next(decisions))
    result = services._migration_status_payload()
    assert result["pbcoindata_lifecycle"]["expected"] is True
    assert "pbcoindata" in result["required_services"]


@pytest.mark.parametrize("owner", [None, "monitor-agent", "pbrun"])
def test_master_healing_owner_requires_agent_confirmation(owner):
    """A verified local controller suppresses competing master recovery."""
    from master.async_monitor import VPSMonitor
    monitor = object.__new__(VPSMonitor)
    decision = policy("ACTIVE", True)
    if owner:
        decision["controller"] = owner
    monitor.store = SimpleNamespace(host_meta={"local": {"credential_active": True, "pbcoindata_lifecycle": decision}})
    monitor._local_master_hostname = lambda: "local"
    assert monitor._auto_restart_allowed_for_host("local", "PBCoinData") is (owner is None)


def test_master_stale_demand_remains_unknown_and_no_remote_heal():
    """Stale new-policy metadata never becomes credential-driven auto-restart."""
    from master.async_monitor import VPSMonitor
    monitor = object.__new__(VPSMonitor)
    decision = policy("ACTIVE", True)
    decision.update(generated_at=time.time() - 121, controller="monitor-agent")
    monitor.store = SimpleNamespace(host_meta={"local": {"credential_active": True, "pbcoindata_lifecycle": decision}})
    monitor._local_master_hostname = lambda: "local"
    assert monitor._optional_service_expected("local", "PBCoinData") is None
    assert monitor._auto_restart_allowed_for_host("local", "PBCoinData") is False


def test_post_stop_queries_share_five_second_budget(controller, monkeypatch):
    """Time spent reading legacy PID state reduces the systemctl query timeout."""
    clock = [100.0]
    monkeypatch.setattr(agent.time, "monotonic", lambda: clock[0])

    def legacy(*args):
        """Simulate two seconds of post-stop PID inspection."""
        clock[0] += 2
        return status("stopped", None, "legacy")

    monkeypatch.setattr(agent, "_pid_file_service_status", legacy)
    assert agent._coindata_status()["status"] == "idle"
    assert controller.calls[1] == ("read", 3.0)


def test_disabled_healing_never_starts_active_consumer_service(controller, monkeypatch):
    """An ACTIVE consumer does not bypass disabled automatic healing."""
    controller.actual = status("stopped", None)
    controller.decision = policy("ACTIVE", True)
    controller.heal = False
    monkeypatch.setattr(agent, "_restart_systemd_service", lambda *a: pytest.fail("Healing disabled"))
    assert agent._coindata_status()["status"] == "stopped"


@pytest.mark.parametrize("state", ["deactivating", "activating", "reloading", "unknown"])
def test_new_demand_observes_unfinished_systemd_job(controller, monkeypatch, state):
    """Demand returning after an unconfirmed stop must not compete with its pending job."""
    controller.decision = policy("ACTIVE", True)
    controller.actual = dict(status("stopped", None), systemd_state=state)
    monkeypatch.setattr(agent, "_restart_systemd_service", lambda *a: pytest.fail("Observe pending or unknown state"))
    assert agent._coindata_status()["status"] == "stopped"
    assert controller.calls == []


def test_timeout_then_returning_demand_waits_for_stop_job(controller, monkeypatch):
    """A timed-out stop remains observation-only until the next cycle confirms exit."""
    controller.post = dict(status("stopped", None), systemd_state="deactivating", main_pid=23)

    def timeout(*args, **kwargs):
        """Leave a synthetic stop job in progress without executing systemctl."""
        raise subprocess.TimeoutExpired("systemctl", 40)

    monkeypatch.setattr(agent.subprocess, "run", timeout)
    first = agent._coindata_status()
    assert first["status"] != "idle" and first["stop_error"]
    controller.actual = controller.post
    controller.decision = policy("ACTIVE", True)
    restarts = []
    monkeypatch.setattr(agent, "_restart_systemd_service", lambda *a: restarts.append(a) or (True, ""))
    assert agent._coindata_status()["status"] == "stopped"
    assert restarts == []
    controller.actual = status("stopped", None)
    assert agent._coindata_status()["status"] == "restarting"
    assert len(restarts) == 1


@pytest.fixture
def legacy(tmp_path, monkeypatch):
    """Use real election/action locks with synthetic consumers, managers and processes."""
    (tmp_path / "pbgui.ini").write_text("[main]\nrole=slave\n[vps_monitor]\nauto_restart=true\n")
    state = SimpleNamespace(demand="IDLE", actual="running", manager="legacy", calls=[], owner="pbrun", ready=True)
    monkeypatch.setattr(legacy_process, "transition_status", lambda root: {"basis": "synthetic verified operator transition"})
    monkeypatch.setattr(legacy_process, "unload_preflight", lambda *a, **k: {"allowed": True, "reason": "synthetic structural proof"})
    state.real_start = legacy_process.start
    monkeypatch.setattr(legacy_process, "manager_state", lambda root: {"manager": state.manager, "reason": "synthetic manager"})
    monkeypatch.setattr(legacy_process, "snapshot", lambda root: {"status": state.actual, "pid": 23 if state.actual == "running" else None})
    monkeypatch.setattr(legacy_controller, "controller_metadata", lambda root: {"controller": state.owner, "recovery_ready": state.ready, "recovery_reason": "synthetic verified lease"})
    monkeypatch.setattr(legacy_controller, "lifecycle_decision", lambda *a, **k: dict(policy(state.demand, True if state.demand == "ACTIVE" else (False if state.demand == "IDLE" else None))))

    def start(root, **kwargs):
        """Capture a bounded start without launching a real daemon."""
        state.calls.append("start")
        state.actual = "running"

    def stop(root):
        """Capture a verified stop without signalling any process."""
        state.calls.append("stop")
        state.actual = "stopped"

    monkeypatch.setattr(legacy_process, "start", start)
    monkeypatch.setattr(legacy_process, "stop", stop)
    state.root = tmp_path
    return state


def test_retained_zombie_active_controller_recovery(retained_zombie,monkeypatch):
    """Real snapshot/start recover ACTIVE before another parent reaps the old ready/Z child."""
    import select
    import psutil
    state=retained_zombie
    state.parent.stdin.write('release-controller\n');state.parent.stdin.flush()
    assert select.select([state.parent.stdout],[],[],5)[0]
    assert state.parent.stdout.readline().strip()=='released'
    (state.root/'pbgui.ini').write_text('[main]\nrole=slave\n[vps_monitor]\nauto_restart=true\n')
    monkeypatch.setattr(legacy_controller,'controller_metadata',lambda *a:{'controller':'pbrun','recovery_ready':True,'recovery_reason':'isolated elected owner'})
    monkeypatch.setattr(legacy_controller,'lifecycle_decision',lambda *a,**k:{'demand':'ACTIVE','expected':True,'reason':'synthetic consumer'})
    controller=legacy_controller.LegacyController(state.root)
    try:
        decision,current=controller.reconcile(True)
        assert decision['expected'] is True and current['status']=='running'
        assert current['pid']!=state.identity.pid
        assert psutil.Process(state.identity.pid).status()==psutil.STATUS_ZOMBIE
        assert not legacy_process.guard_protocol.read_journal(state.root)['transition']
    finally:controller.close()


def test_legacy_pbrun_recovery_without_agent(legacy):
    """PBRun alone unloads and recovers over IDLE/ACTIVE/UNKNOWN/IDLE cycles."""
    controller = legacy_controller.LegacyController(legacy.root)
    try:
        assert controller.reconcile(True)[1]["status"] == "stopped"
        assert controller.lease is not None
        legacy.demand = "ACTIVE"
        assert controller.reconcile(True)[1]["status"] == "running"
        legacy.demand = "UNKNOWN"
        controller.reconcile(True)
        assert legacy.calls == ["stop", "start"]
        legacy.demand = "IDLE"
        controller.reconcile(True)
        assert legacy.calls == ["stop", "start", "stop"]
    finally:
        controller.close()


def test_legacy_manager_loss_and_positive_recovery(legacy):
    """An unloaded daemon waits through UNKNOWN and starts after fresh positive evidence."""
    controller = legacy_controller.LegacyController(legacy.root)
    try:
        assert controller.reconcile(True)[1]["status"] == "stopped"
        legacy.manager, legacy.demand = "unknown", "ACTIVE"
        assert controller.reconcile(True) is None
        assert legacy.calls == ["stop"] and controller.lease is None
        legacy.manager = "legacy"  # Complete offline evidence, not a cached former state.
        assert controller.reconcile(True)[1]["status"] == "running"
        assert legacy.calls == ["stop", "start"]
    finally:
        controller.close()


def test_start_backoff_survives_controller_replacement_and_agent_fallback(legacy, monkeypatch):
    """All elected owners share persisted wall deadlines rather than retrying each tick."""
    clock, attempts = [10000.0], []
    legacy.actual, legacy.demand = "stopped", "ACTIVE"
    monkeypatch.setattr(legacy_process, "start", legacy.real_start)
    monkeypatch.setattr(legacy_process.time, "time", lambda: clock[0])
    monkeypatch.setattr(legacy_process, "_log", lambda *a, **kw: None)
    def fail(*a, **kw):
        """Record a real attempted spawn error without creating any child."""
        attempts.append(clock[0])
        raise OSError("synthetic spawn failure")
    monkeypatch.setattr(legacy_process.subprocess, "Popen", fail)
    first = legacy_controller.LegacyController(legacy.root)
    assert first.reconcile(True) is None
    first.close()
    for tick in (10005, 10030, 10059):
        clock[0] = tick
        replacement = legacy_controller.LegacyController(legacy.root)
        try:
            result = replacement.reconcile(True)
            assert "retry deferred" in result[0]["reason"]
        finally:
            replacement.close()
    assert attempts == [10000]
    legacy.owner = "monitor-agent"
    clock[0] = 10060
    assert legacy_controller.agent_fallback(legacy.root, True) is None
    assert attempts == [10000, 10060]
    clock[0] = 10065
    assert "retry deferred" in legacy_controller.agent_fallback(legacy.root, True)[0]["reason"]
    legacy.manager = "unknown"
    clock[0] = 20000
    assert legacy_controller.agent_fallback(legacy.root, True) is None
    assert len(attempts) == 2  # Expiry never authorizes an unknown manager.


@pytest.mark.parametrize("cause", ["master", "disabled", "systemd", "unknown-manager", "unknown-demand", "foreign", "unknown-pid", "no-recovery"])
def test_legacy_conservative_guards(legacy, cause):
    """Every uncertain boundary prevents legacy signals and starts."""
    if cause == "master":
        (legacy.root / "pbgui.ini").write_text("[main]\nrole=master\n")
    elif cause == "disabled":
        (legacy.root / "pbgui.ini").write_text("[main]\nrole=slave\n[vps_monitor]\nauto_restart=false\n")
    elif cause in {"systemd", "unknown-manager"}:
        legacy.manager = "systemd" if cause == "systemd" else "unknown"
    elif cause == "unknown-demand":
        legacy.demand = "UNKNOWN"
    elif cause in {"foreign", "unknown-pid"}:
        legacy.actual = "foreign" if cause == "foreign" else "unknown"
    else:
        legacy.ready = False
    controller = legacy_controller.LegacyController(legacy.root)
    try:
        controller.reconcile(True)
        assert legacy.calls == []
    finally:
        controller.close()


def test_agent_fallback_requires_free_os_lease_not_pbrun_pid(legacy):
    """An old PBRun PID has no election authority; a real held lease does."""
    piddir = legacy.root / "data/pid"
    piddir.mkdir(parents=True)
    (piddir / "pbrun.pid").write_text("123")
    owner = legacy_controller.LegacyController(legacy.root)
    owner.reconcile(True)
    legacy.calls.clear()
    legacy.owner = "monitor-agent"
    try:
        assert legacy_controller.agent_fallback(legacy.root, True) is None
        assert legacy.calls == []
    finally:
        owner.close()
    legacy.demand = "ACTIVE"
    assert legacy_controller.agent_fallback(legacy.root, True)[1]["status"] == "running"
    assert legacy.calls == ["start"]


@pytest.mark.parametrize("change", ["master", "disabled", "systemd"])
def test_pbrun_releases_lease_on_role_control_or_manager_change(legacy, change):
    """A previous controller cannot retain election ownership after losing its role."""
    controller = legacy_controller.LegacyController(legacy.root)
    try:
        controller.reconcile(True)
        assert controller.lease is not None
        if change == "systemd":
            legacy.manager = "systemd"
        else:
            (legacy.root / "pbgui.ini").write_text("[main]\nrole=master\n" if change == "master" else "[main]\nrole=slave\n[vps_monitor]\nauto_restart=false\n")
        controller.reconcile(True)
        assert controller.lease is None
        replacement = legacy_process.FileLease(legacy.root, "controller", "replacement")
        replacement.release()
    finally:
        controller.close()


def test_legacy_rechecks_manager_after_action_lock(legacy, monkeypatch):
    """Migration winning the action lock prevents a stale legacy start."""
    legacy.actual, legacy.demand = "stopped", "ACTIVE"
    managers = iter(["legacy", "systemd"])
    monkeypatch.setattr(legacy_process, "manager_state", lambda root: {"manager": next(managers), "reason": "synthetic change"})
    controller = legacy_controller.LegacyController(legacy.root)
    assert controller.reconcile(True) is None
    assert legacy.calls == [] and controller.lease is None


def test_legacy_unconfirmed_stop_never_starts_replacement(legacy, monkeypatch):
    """A failed exact stop remains an error/observation rather than permission to spawn."""
    def stop(root):
        """Simulate denied process inspection without signalling."""
        raise legacy_process.ProcessStateError("unconfirmed")
    monkeypatch.setattr(legacy_process, "stop", stop)
    controller = legacy_controller.LegacyController(legacy.root)
    assert controller.reconcile(True) is None
    assert controller.lease is None and legacy.calls == []


def test_manual_success_cannot_authorize_automatic_unknown_recovery(legacy, monkeypatch):
    """A manually running daemon never promotes manager, election or recovery metadata."""
    legacy.manager, legacy.demand, legacy.actual = 'unknown', 'ACTIVE', 'running'
    original = {'version': 1, 'failures': 2, 'last_failure': 10000, 'next_retry': 10120}
    with legacy_process.actions(legacy.root, 'test'):
        legacy_process._write_retry(legacy.root, original)
    before = legacy_process._retry_path(legacy.root).read_bytes()
    controller = legacy_controller.LegacyController(legacy.root)
    try:
        assert controller.reconcile(True) is None
        assert controller.lease is None and legacy.calls == []
        assert legacy_process.manager_state(legacy.root)['manager'] == 'unknown'
        assert legacy_process._retry_path(legacy.root).read_bytes() == before
    finally:
        controller.close()


@pytest.fixture
def guarded_idle(tmp_path, monkeypatch):
    """Use real temporary guard/journal/retry files with synthetic daemon identity checks."""
    import pbcoindata_guard as guard
    import pbcoindata_process as process
    from process_identity import ProcessIdentity
    (tmp_path / 'pbgui.ini').write_text('[main]\nrole=slave\n[vps_monitor]\nauto_restart=true\n')
    (tmp_path / 'PBCoinData.py').write_text('# synthetic guarded service\n')
    (tmp_path / 'pbcoindata_guard.py').write_text('# isolated guarded entry dependency\n')
    (tmp_path / 'secure_files.py').write_text('# isolated stdlib file helper\n')
    lease = guard.acquire_daemon(tmp_path)
    guard.publish(lease, 'ready')
    identity = ProcessIdentity(lease.identity['pid'], lease.identity['create_time'])
    actual = {'status': 'running', 'pid': identity.pid, 'identity': identity}
    monkeypatch.setattr(process, 'inspect_pid', lambda *a, **k: dict(actual))
    monkeypatch.setattr(process, 'manager_state', lambda root: {'manager': 'legacy', 'reason': 'synthetic'})
    document = guard.read_journal(tmp_path)
    document['transition'] = {**guard.scope(tmp_path), 'basis': 'legacy-cleanup', 'autostarts_updated': True, 'controllers_restarted': True, 'confirmed_at': 1000.0}
    guard.write_journal(tmp_path, document)
    yield SimpleNamespace(root=tmp_path, process=process, guard=guard, actual=actual, lease=lease)
    lease.release()
    guard._LIFETIME_GUARDS.remove(lease)


class TestUnloadPreflight:
    """Structural recovery checks are read-only and independent of credentials."""

    @pytest.mark.parametrize('manager', ['legacy', 'systemd'])
    @pytest.mark.parametrize('credential', [False, None])
    @pytest.mark.parametrize('content', [None, '{broken', '{"version":99}', '{"version":1,"failures":1,"last_failure":9999999999999,"next_retry":99999999999999}'])
    def test_idle_accepts_repairable_retry_without_writes(self, guarded_idle, systemd_owner, monkeypatch, manager, credential, content):
        """Clear IDLE unloads with invalid credentials/retry; no metadata repair or authorization occurs."""
        state, process = guarded_idle, guarded_idle.process
        monkeypatch.setattr(process, 'manager_state', lambda root: {'manager': manager})
        retry = process._retry_path(state.root)
        if content is not None: retry.write_text(content)
        journal = process._launch_path(state.root).read_bytes()
        before = retry.read_bytes() if retry.exists() else None
        monkeypatch.setattr(process, '_write_retry', lambda *a: pytest.fail('No retry repair in IDLE'))
        monkeypatch.setattr(process, '_write_launch', lambda *a: pytest.fail('No journal write in preflight'))
        monkeypatch.setattr(process, 'start', lambda *a, **k: pytest.fail('No speculative start'))
        monkeypatch.setattr(process, 'manual_action', lambda *a: pytest.fail('No manual authorization'))
        if manager == 'systemd':
            monkeypatch.setattr(process, 'inspect_retry', lambda *a: pytest.fail('No legacy retry dependency for systemd'))
        decision = lifecycle.lifecycle_decision(state.root, credential, process_iter=lambda: [])
        assert decision['demand'] == 'IDLE' and decision['expected'] is False
        with process.actions(state.root, 'preflight-test'):
            result = process.unload_preflight(state.root, decision, state.actual, manager=manager, recovery={'recovery_ready': True}, restart_check=lambda: (True, 'synthetic systemd policy'))
        assert result['allowed'], result
        assert journal == process._launch_path(state.root).read_bytes()
        assert before == (retry.read_bytes() if retry.exists() else None)

    @pytest.mark.parametrize('fault', ['transition', 'pending', 'corrupt', 'registration', 'controller', 'manager', 'clock', 'io'])
    def test_structural_uncertainty_blocks_without_mutation(self, guarded_idle, monkeypatch, fault):
        """No IDLE stop is allowed when the known future start path is structurally blocked."""
        state, process = guarded_idle, guarded_idle.process
        recovery = {'recovery_ready': fault != 'controller'}
        document = state.guard.read_journal(state.root)
        if fault == 'transition': document['transition'] = None
        if fault == 'pending': document['pending_launch'] = {'version': 2, 'token': 'c'*32, 'pid': None, 'create_time': None}
        state.guard.write_journal(state.root, document)
        if fault == 'corrupt': process._launch_path(state.root).write_text('{broken')
        if fault == 'registration': state.guard.publish(state.lease, 'starting')
        if fault == 'manager': monkeypatch.setattr(process, 'manager_state', lambda root: {'manager': 'unknown'})
        if fault == 'clock': monkeypatch.setattr(process, '_wall_now', lambda: (_ for _ in ()).throw(process.ProcessStateError('clock unusable')))
        if fault == 'io': monkeypatch.setattr(process, 'inspect_retry', lambda *a: (_ for _ in ()).throw(process.ProcessStateError('retry unreadable')))
        before = process._launch_path(state.root).read_bytes()
        result = process.unload_preflight(state.root, policy(), state.actual, manager='legacy', recovery=recovery)
        assert not result['allowed'] and result['reason']
        assert before == process._launch_path(state.root).read_bytes()

    def test_deadline_expires_and_active_repairs_only_then(self, guarded_idle, monkeypatch):
        """A real deadline is temporary; corrupt content is repaired only at ACTIVE start."""
        state, process = guarded_idle, guarded_idle.process
        wall = [1000.0]
        monkeypatch.setattr(process, '_wall_now', lambda: wall[0])
        process._write_retry(state.root, {'version': 1, 'failures': 1, 'last_failure': 990, 'next_retry': 1050})
        assert not process.unload_preflight(state.root, policy(), state.actual, manager='legacy', recovery={'recovery_ready': True})['allowed']
        wall[0] = 1051
        assert process.unload_preflight(state.root, policy(), state.actual, manager='legacy', recovery={'recovery_ready': True})['allowed']
        process._retry_path(state.root).write_text('{broken')
        assert process.inspect_retry(state.root)['repair']
        assert process._retry_path(state.root).read_text() == '{broken'
        with process.actions(state.root, 'active-start'):
            repaired = process.retry_status(state.root)
        assert repaired['next_retry'] == wall[0] + 60


@pytest.mark.parametrize('manager', ['legacy', 'systemd'])
def test_controller_preserves_running_idle_when_preflight_refuses(legacy, controller, monkeypatch, manager):
    """Both actual controller dispatch paths consult the same guard before stop."""
    import pbcoindata_process as process
    calls = []
    def refuse(root, decision, state, **kwargs):
        """Expose an unresolved launch without any action side effects."""
        calls.append(kwargs['manager'])
        return {'allowed': False, 'reason': 'Unresolved isolated launch'}
    monkeypatch.setattr(process, 'unload_preflight', refuse)
    if manager == 'legacy':
        monkeypatch.setattr(process, 'manager_state', lambda root: {'manager': 'legacy'})
        instance = legacy_controller.LegacyController(legacy.root)
        try:
            decision, state = instance.reconcile(False)
            assert state['status'] == 'running' and legacy.calls == []
        finally: instance.close()
    else:
        state = agent._coindata_status()
        assert state['status'] == 'running' and controller.calls == []
    assert calls == [manager]


@pytest.mark.parametrize('history', [[9990.0], [7000.0, 8000.0, 9000.0]])
def test_systemd_idle_respects_own_restart_limits_without_pruning(controller, monkeypatch, history):
    """Known cooldown/rate limits veto unload until automatic reevaluation permits restart."""
    import pbcoindata_process as process
    wall = [10000.0]
    monkeypatch.setattr(agent.time, 'time', lambda: wall[0])
    monkeypatch.setattr(agent, '_SERVICE_RESTART_HISTORY', {'PBCoinData': history[:]})
    def preflight(*args, restart_check, **kwargs):
        """Use the real read-only systemd policy in the controller dispatch."""
        allowed, reason = restart_check()
        return {'allowed': allowed, 'reason': reason}
    monkeypatch.setattr(process, 'unload_preflight', preflight)
    assert agent._coindata_status()['status'] == 'running'
    assert not controller.calls and agent._SERVICE_RESTART_HISTORY['PBCoinData'] == history
    wall[0] = 14000.0
    assert agent._coindata_status()['status'] == 'idle'
    assert agent._SERVICE_RESTART_HISTORY['PBCoinData'] == history


@pytest.mark.parametrize('fault', ['interpreter', 'script', 'helper', 'missing-helper'])
def test_structural_start_permissions_veto_unload(guarded_idle, monkeypatch, fault):
    """Current permissions and missing entry dependencies are observable structural vetoes."""
    state, process = guarded_idle, guarded_idle.process
    original = process.os.access
    blocked = {'interpreter': str(process.sys.executable), 'script': str(state.root / 'PBCoinData.py'), 'helper': str(state.root / 'pbcoindata_guard.py')}.get(fault)
    if fault == 'missing-helper': (state.root / 'pbcoindata_guard.py').unlink()
    monkeypatch.setattr(process.os, 'access', lambda path, mode: False if str(path) == blocked else original(path, mode))
    assert not process.unload_preflight(state.root, policy(), state.actual, manager='legacy', recovery={'recovery_ready': True})['allowed']


def test_default_without_transition_keeps_idle_resident(guarded_idle, monkeypatch):
    """The approved operator transition remains an explicit prerequisite for voluntary IDLE release."""
    state, process = guarded_idle, guarded_idle.process
    document=state.guard.read_journal(state.root);document['transition']=None
    state.guard.write_journal(state.root,document)
    monkeypatch.setattr(process,'manager_state',lambda root:{'manager':'legacy'})
    before=process._launch_path(state.root).read_bytes()
    result=process.unload_preflight(state.root,policy(),state.actual,manager='legacy',recovery={'recovery_ready':True},restart_check=lambda:(True,''))
    assert not result['allowed'] and 'transition' in result['reason']
    assert before==process._launch_path(state.root).read_bytes()


def test_systemd_active_healing_does_not_require_idle_transition(controller, monkeypatch):
    """A stopped systemd unit retains existing ACTIVE healing without legacy attestation."""
    import pbcoindata_process as process
    controller.decision=policy('ACTIVE',True)
    controller.actual=status('stopped',None)
    calls=[]
    monkeypatch.setattr(process,'transition_status',lambda *a:pytest.fail('No extra ACTIVE transition gate'))
    monkeypatch.setattr(process,'snapshot',lambda *a:{'status':'stopped'})
    monkeypatch.setattr(process,'ownership_conflicts',lambda *a:calls.append('ownership'))
    monkeypatch.setattr(agent,'_restart_systemd_service',lambda *a:(calls.append('restart') or True,''))
    result=agent._coindata_status()
    assert result['status']=='restarting' and calls==['ownership','restart']


def test_systemd_idle_without_legacy_transition_uses_positive_unit_evidence(guarded_idle,systemd_owner,monkeypatch):
    """Confirmed systemd ownership replaces only the unrelated legacy transition attestation."""
    import pbcoindata_manager as manager
    state,process=guarded_idle,guarded_idle.process
    document=state.guard.read_journal(state.root);document['transition']=None
    state.guard.write_journal(state.root,document)
    monkeypatch.setattr(process,'manager_state',lambda root:{'manager':'systemd'})
    before=process._launch_path(state.root).read_bytes()
    result=process.unload_preflight(state.root,policy(),state.actual,manager='systemd',recovery={'recovery_ready':True,'controller':'systemd'},restart_check=lambda:(True,''))
    assert result['allowed'],result
    assert process._launch_path(state.root).read_bytes()==before


@pytest.fixture
def systemd_owner(guarded_idle,monkeypatch):
    """Bind real temporary guard registration to fully synthetic current unit/proc evidence."""
    import pbcoindata_manager as manager
    import sys
    state=guarded_idle
    group='/user.slice/user@1000.service/app.slice/pbgui-pbcoindata.service'
    value={'Id':manager.UNIT,'LoadState':'loaded','ActiveState':'active','SubState':'running','MainPID':str(state.actual['pid']),'ControlGroup':group,'Job':'','ExecStart':'{ path='+sys.executable+' ; argv[]='+sys.executable+' -u '+str(state.root/'PBCoinData.py')+' ; ignore_errors=no ; start_time=[n/a] ; }','WorkingDirectory':str(state.root),'Result':'success','StartLimitIntervalUSec':'10s','StartLimitBurst':'5'}
    monkeypatch.setattr(manager,'_target_groups',lambda *a:group)
    original_query=manager._query_unit
    monkeypatch.setattr(manager,'_query_unit',lambda *a,**k:dict(value))
    return SimpleNamespace(value=value,group=group,manager=manager,real_query_unit=original_query)


@pytest.mark.parametrize('fault',['mainpid','historical-pid','changed-mainpid','group','checkout','guard','entry','interpreter','start-limit','cooldown','unreadable','publications','pending','corrupt','identity','empty-directory','relative-directory'])
def test_systemd_idle_requires_fresh_complete_ownership(guarded_idle,systemd_owner,monkeypatch,fault):
    """Each missing/contradictory unit-process fact vetoes stop without legacy certification."""
    state,process,owner=guarded_idle,guarded_idle.process,systemd_owner
    monkeypatch.setattr(process,'manager_state',lambda root:{'manager':'systemd'})
    document=state.guard.read_journal(state.root);document['transition']=None
    if fault=='pending':document['pending_launch']={'version':1,'token':'a'*32,'pid':None,'create_time':None}
    state.guard.write_journal(state.root,document)
    if fault=='corrupt':process._launch_path(state.root).write_text('{broken')
    if fault=='mainpid':owner.value['MainPID']=str(state.actual['pid']+1)
    if fault=='historical-pid':owner.value.update(MainPID='0',ExecMainPID=str(state.actual['pid']))
    if fault=='group':owner.value['ControlGroup']='/system.slice/another.service'
    if fault=='checkout':owner.value['WorkingDirectory']=str(state.root.parent)
    if fault=='empty-directory':
        monkeypatch.chdir(state.root);owner.value['WorkingDirectory']=''
    if fault=='relative-directory':
        monkeypatch.chdir(state.root);owner.value['WorkingDirectory']='.'
    if fault=='guard':state.guard.publish(state.lease,'starting')
    if fault=='entry':owner.value['ExecStart']='{ path=/bin/false ; argv[]=/bin/false ; ignore_errors=no ; }'
    if fault=='interpreter':owner.value['ExecStart']=owner.value['ExecStart'].replace('path=','path=/missing')
    if fault=='start-limit':owner.value['Result']='start-limit-hit'
    if fault=='unreadable':monkeypatch.setattr(owner.manager,'_query_unit',lambda *a,**k:(_ for _ in ()).throw(PermissionError('isolated denial')))
    if fault=='changed-mainpid':
        calls=[]
        def change(*a,**k):
            """Change only the second observation of actual MainPID."""
            calls.append(1);return {**owner.value,'MainPID':str(state.actual['pid']+(len(calls)>1))}
        monkeypatch.setattr(owner.manager,'_query_unit',change)
    if fault=='publications':
        process.pid_path(state.root).write_text(str(state.actual['pid']+10))
        process.write_process_identity(process.pid_path(state.root),state.actual['identity'])
    if fault=='identity':monkeypatch.setattr(process,'inspect_pid',lambda *a,**k:{'status':'unknown'})
    before=process._launch_path(state.root).read_bytes()
    monkeypatch.setattr(process,'transition_status',lambda *a:pytest.fail('No legacy transition dependency'))
    result=process.unload_preflight(state.root,policy(),state.actual,manager='systemd',recovery={'recovery_ready':True,'controller':'systemd'},restart_check=lambda:(fault!='cooldown','isolated cooldown'))
    assert not result['allowed'],fault
    assert process._launch_path(state.root).read_bytes()==before


@pytest.mark.parametrize('fault',['none','mainpid','pending'])
def test_agent_systemd_idle_uses_real_unit_preflight(controller,guarded_idle,systemd_owner,monkeypatch,fault):
    """The actual agent dispatch stops only after complete route-specific read-only evidence."""
    state,process=guarded_idle,guarded_idle.process
    monkeypatch.setattr(agent,'PBGDIR',state.root)
    monkeypatch.setattr(process,'unload_preflight',controller.real_preflight)
    monkeypatch.setattr(process,'manager_state',lambda root:{'manager':'systemd'})
    controller.actual=status('running',state.actual['pid'])
    document=state.guard.read_journal(state.root);document['transition']=None
    if fault=='pending':document['pending_launch']={'version':1,'token':'b'*32,'pid':None,'create_time':None}
    state.guard.write_journal(state.root,document)
    if fault=='mainpid':systemd_owner.value['MainPID']=str(state.actual['pid']+1)
    before=process._launch_path(state.root).read_bytes()
    result=agent._coindata_status()
    stops=[call for call in controller.calls if isinstance(call[0],list) and 'stop' in call[0]]
    assert result['status']==('idle' if fault=='none' else 'running')
    assert len(stops)==(1 if fault=='none' else 0)
    assert process._launch_path(state.root).read_bytes()==before


@pytest.mark.parametrize('job',['','271'])
def test_systemd_idle_queries_real_empty_property_format(guarded_idle,systemd_owner,monkeypatch,job):
    """Exercise full systemd IDLE evidence through the real argv/output unit reader."""
    state,process,owner=guarded_idle,guarded_idle.process,systemd_owner
    owner.value['Job']=job
    monkeypatch.setattr(owner.manager,'_query_unit',owner.real_query_unit)
    monkeypatch.setattr(process,'manager_state',lambda root:{'manager':'systemd'})
    def show(argv,**kwargs):
        """Match systemctl's suppression of unset Job unless --all is specified."""
        names=[argv[index+1] for index,arg in enumerate(argv) if arg=='-p']
        text='\n'.join(name+'='+owner.value[name] for name in names if owner.value[name] or '--all' in argv)
        return SimpleNamespace(returncode=0,stdout=text)
    monkeypatch.setattr(owner.manager.subprocess,'run',show)
    before=process._launch_path(state.root).read_bytes()
    result=process.unload_preflight(state.root,policy(),state.actual,manager='systemd',recovery={'recovery_ready':True,'controller':'systemd'},restart_check=lambda:(True,''))
    assert result['allowed']==(not job),result
    assert process._launch_path(state.root).read_bytes()==before
