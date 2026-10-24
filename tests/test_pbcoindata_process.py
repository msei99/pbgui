"""Exact ownership, election, singleton and bounded cleanup with isolated doubles."""

import importlib.util
import hashlib
import fcntl
import json
import os
from pathlib import Path
import signal
import stat
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import psutil
import pytest

import pbcoindata_process as subject
from pbcoindata_controller import controller_metadata
from process_identity import ProcessIdentity, identity_path, write_process_identity

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def offline_manager(tmp_path, monkeypatch):
    """Provide complete synthetic procfs/runtime evidence, never inspecting host processes."""
    import pbcoindata_manager as manager
    state = SimpleNamespace(root=tmp_path, query="bus-error", process="python", denied=False, nested=False)
    units = tmp_path / "units"
    units.mkdir()
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    runtime.chmod(0o700)
    state.units, state.runtime = units, runtime
    monkeypatch.setattr(manager, "_unit_paths", lambda *a: ([units], {runtime}))
    original_lstat = manager._lstat
    markers = {"/.dockerenv", "/run/.containerenv", "/run/systemd/container"}
    monkeypatch.setattr(manager, "_lstat", lambda path, deadline: None if str(path) in markers else original_lstat(path, deadline))
    original_stat = Path.stat
    monkeypatch.setattr(Path, "stat", lambda path, **kw: SimpleNamespace(st_dev=1, st_ino=2) if str(path) in {"/proc/self/ns/pid", "/proc/1/ns/pid"} else original_stat(path, **kw))
    data = {"/proc/mounts": "proc /proc proc rw 0 0\n", "/proc/self/status": "NSpid:\t42\n",
            "/proc/self/uid_map": "0 0 4294967295\n", "/proc/self/cgroup": "0::/user.slice/session.scope\n",
            "/proc/1/cgroup": "0::/init.scope\n", "/proc/42/cgroup": "0::/user.slice/session.scope\n"}
    state.data = data
    def read(path, deadline):
        """Read only the explicit synthetic evidence map."""
        manager._check(deadline)
        if state.nested and path == "/proc/self/status":
            return "NSpid:\t100 42\n"
        return data[str(path)]
    monkeypatch.setattr(manager, "_read", read)
    monkeypatch.delenv("container", raising=False)
    monkeypatch.delenv("SYSTEMD_NSPAWN_CONTAINER", raising=False)
    monkeypatch.setattr(manager.psutil, "pids", lambda: [42])
    class Process:
        """Represent an inspectable own process or a denied process."""
        def __init__(self, pid):
            if state.denied:
                raise psutil.AccessDenied(pid)
        def uids(self):
            """Use this test UID without inspecting credentials."""
            return SimpleNamespace(real=os.getuid(), effective=os.getuid())
        def status(self):
            """The synthetic process is alive."""
            return psutil.STATUS_RUNNING
        def cmdline(self):
            """Return a synthetic manager or interpreter argv."""
            return [state.process, "--user"]
        def name(self):
            """Return the same synthetic executable name."""
            return state.process
        def exe(self):
            """Return a synthetic executable identity without reading host files."""
            return '/synthetic/' + state.process
    monkeypatch.setattr(manager.psutil, "Process", Process)
    def query(*args, **kwargs):
        """Never execute systemctl in this fixture."""
        if state.query == "missing-command":
            raise FileNotFoundError("synthetic systemctl")
        return SimpleNamespace(returncode=1, stdout="", stderr="synthetic unavailable bus")
    monkeypatch.setattr(manager.subprocess, "run", query)
    state.module = manager
    return state


@pytest.mark.parametrize("query", ["bus-error", "missing-command"])
def test_complete_offline_absence_authorizes_legacy(offline_manager, query):
    """A bus failure is supplemented by complete paths, process and namespace evidence."""
    offline_manager.query = query
    finding = subject.manager_state(offline_manager.root)
    assert finding["manager"] == "legacy"
    assert "offline" in finding["reason"]


@pytest.mark.parametrize("uid,stat_text,expected", [
    (0, "2 (kthreadd) S 0 0 0 0 -1 2097152", "legacy"),
    (1000, "2 (kthreadd) S 0 0 0 0 -1 2097152", "legacy"),
    (0, "2 (worker ) (x)) I 0 0 0 0 -1 2097152", "legacy"),
    (0, "2 (empty user process) S 0 0 0 0 -1 0", "unknown"),
    (0, "2 (kthreadd) S 0 0 0 0 -1 -1", "unknown"),
    (0, "2 (kthreadd) S 0 0 0", "unknown"),
    (0, "3 (kthreadd) S 0 0 0 0 -1 2097152", "unknown"),
    (0, "malformed", "unknown"),
    (0, PermissionError("synthetic stat denial"), "unknown"),
])
def test_offline_root_skips_only_proven_kernel_threads(offline_manager, monkeypatch, uid, stat_text, expected):
    """Root's empty kernel argv is safe only with positive PF_KTHREAD procfs evidence."""
    state = offline_manager
    manager = state.module
    monkeypatch.setattr(manager.os, "getuid", lambda: uid)
    original_lstat = manager._lstat
    def lstat(path, deadline):
        """Supply synthetic runtime ownership without requiring root or touching host data."""
        info = original_lstat(path, deadline)
        return SimpleNamespace(st_uid=uid, st_mode=info.st_mode) if path == state.runtime else info
    monkeypatch.setattr(manager, "_lstat", lstat)
    original_read = manager._read
    def read(path, deadline):
        """Expose only a synthetic kernel stat record; denial remains explicit."""
        if str(path) == "/proc/2/stat":
            manager._check(deadline)
            if isinstance(stat_text, Exception):
                raise stat_text
            return stat_text
        return original_read(path, deadline)
    monkeypatch.setattr(manager, "_read", read)
    class Process:
        """Place a UID-0 kernel thread beside an inspectable owned userspace process."""
        def __init__(self, pid):
            """Keep the synthetic PID without creating or inspecting a real process."""
            self.pid = pid
        def uids(self):
            """Kernel threads belong to root even for a non-root installation."""
            owner = 0 if self.pid == 2 else uid
            return SimpleNamespace(real=owner, effective=owner)
        def status(self):
            """Neither synthetic process is a zombie."""
            return psutil.STATUS_RUNNING
        def cmdline(self):
            """Empty argv alone must never authorize skipping a userspace process."""
            return [] if self.pid == 2 else ["python", "PBRun.py"]
        def name(self):
            """Names are intentionally insufficient to prove kernel ownership."""
            return "kthreadd" if self.pid == 2 else "python"
    monkeypatch.setattr(manager.psutil, "pids", lambda: [2, 42])
    monkeypatch.setattr(manager.psutil, "Process", Process)
    assert subject.manager_state(state.root)["manager"] == expected


@pytest.mark.parametrize("active,pid,job,code,expected", [
    ("inactive", "0", "", 0, "legacy"), ("inactive", "0", "", 4, "legacy"),
    ("active", "42", "", 0, "systemd"), ("activating", "0", "7", 0, "systemd"),
    ("inactive", "42", "", 0, "systemd"), ("inactive", "0", "", 1, "unknown"),
])
def test_online_absence_requires_inactive_unit_without_job(offline_manager, monkeypatch, active, pid, job, code, expected):
    """A removed unit can still have an active PID or pending systemd transition."""
    text = f"LoadState=not-found\nActiveState={active}\nMainPID={pid}\nJob={job}\n"
    monkeypatch.setattr(offline_manager.module.subprocess, "run", lambda *a, **kw: SimpleNamespace(returncode=code, stdout=text))
    assert subject.manager_state(offline_manager.root)["manager"] == expected


def test_probe_shares_budget_and_limits_systemctl(offline_manager, monkeypatch):
    """Time consumed by path discovery reduces the same remaining query deadline."""
    clock, limits = [0.0], []
    monkeypatch.setattr(offline_manager.module.time, "monotonic", lambda: clock[0])
    def paths(*args):
        """Spend part of the shared budget discovering synthetic paths."""
        clock[0] = 3.5
        return [offline_manager.units], {offline_manager.runtime}
    def query(*args, **kwargs):
        """Cross the total deadline without invoking any external command."""
        limits.append(kwargs["timeout"])
        clock[0] = 5.1
        return SimpleNamespace(returncode=1, stdout="")
    monkeypatch.setattr(offline_manager.module, "_unit_paths", paths)
    monkeypatch.setattr(offline_manager.module.subprocess, "run", query)
    assert subject.manager_state(offline_manager.root)["manager"] == "unknown"
    assert limits == [1.5]


@pytest.mark.parametrize("fault", ["manager", "denied", "namespace", "socket", "cgroup", "hidepid", "directory-denied", "budget"])
def test_incomplete_offline_evidence_never_authorizes_start(offline_manager, monkeypatch, fault):
    """Every incomplete local view remains unknown even if the user bus is unavailable."""
    state = offline_manager
    if fault == "manager":
        state.process = "systemd"
    elif fault == "denied":
        state.denied = True
    elif fault == "namespace":
        state.nested = True
    elif fault == "socket":
        (state.runtime / "bus").touch()
    elif fault == "cgroup":
        state.data["/proc/42/cgroup"] = f"0::/user@{os.getuid()}.service/app.slice\n"
    elif fault == "hidepid":
        state.data["/proc/mounts"] = "proc /proc proc rw,hidepid=2 0 0\n"
    elif fault == "directory-denied":
        monkeypatch.setattr(state.module.os, "scandir", lambda path: (_ for _ in ()).throw(PermissionError("synthetic denied")))
    else:
        clock = iter([0, 6])
        monkeypatch.setattr(state.module.time, "monotonic", lambda: next(clock, 6))
    assert subject.manager_state(state.root)["manager"] == "unknown"


@pytest.mark.parametrize("artifact", ["unit", "mask", "dangling-unit", "alias", "dropin", "wanted"])
def test_unit_artifacts_prevent_parallel_legacy(offline_manager, artifact):
    """Aliases, dangling unit links and masks count as installed artifacts."""
    root = offline_manager.units
    if artifact == "unit":
        (root / subject.UNIT).touch()
    elif artifact == "mask":
        (root / subject.UNIT).symlink_to("/dev/null")
    elif artifact == "dangling-unit":
        (root / subject.UNIT).symlink_to(root / "missing")
    elif artifact == "alias":
        (root / "alias.service").symlink_to(root / subject.UNIT)
    elif artifact == "dropin":
        (root / (subject.UNIT + ".d")).mkdir()
    else:
        (root / "default.target.wants").mkdir()
        (root / "default.target.wants" / subject.UNIT).symlink_to(root / "missing")
    assert subject.manager_state(offline_manager.root)["manager"] == "systemd"


def test_load_paths_include_environment_compiled_and_runtime_sources(tmp_path, monkeypatch):
    """Resolve overrides and all runtime priorities in addition to compiled paths."""
    import pbcoindata_manager as manager
    env = {"XDG_CONFIG_HOME": str(tmp_path / "config"), "XDG_CONFIG_DIRS": str(tmp_path / "config2"),
           "XDG_DATA_HOME": str(tmp_path / "data"), "XDG_DATA_DIRS": str(tmp_path / "data2"),
           "XDG_RUNTIME_DIR": str(tmp_path / "run"), "SYSTEMD_UNIT_PATH": str(tmp_path / "override") + ":"}
    monkeypatch.setattr(manager.subprocess, "run", lambda *a, **kw: SimpleNamespace(returncode=0, stdout=str(tmp_path / "compiled") + "\n"))
    paths, runtime = manager._unit_paths(env, tmp_path / "home", os.getuid(), manager.time.monotonic() + 5)
    for relative in ("config/systemd/user.control", "config/systemd/user", "config2/systemd/user", "data/systemd/user", "data2/systemd/user", "run/systemd/transient", "run/systemd/generator.early", "run/systemd/generator", "run/systemd/generator.late", "override", "compiled"):
        assert tmp_path / relative in paths
    assert Path(f"/run/user/{os.getuid()}") in runtime
    env["SYSTEMD_UNIT_PATH"] = "relative-path"
    with pytest.raises(ValueError):
        manager._unit_paths(env, tmp_path, os.getuid(), manager.time.monotonic() + 5)


def test_symlink_loop_and_dangling_ancestor_are_unknown(offline_manager):
    """ENOENT below a symlink cannot hide an unresolved search space."""
    root = offline_manager.units
    (root / "loop").symlink_to("loop")
    assert subject.manager_state(offline_manager.root)["manager"] == "unknown"
    (root / "loop").unlink()
    (root / "dangling").symlink_to("missing")
    with pytest.raises(FileNotFoundError):
        offline_manager.module._lstat(root / "dangling" / subject.UNIT, time.monotonic() + 5)


def test_start_retry_is_shared_and_manual_attempts_update_same_state(tmp_path, monkeypatch):
    """Failures respect persisted wall deadlines; manual actions bypass only the wait."""
    monkeypatch.setattr(subject, "transition_status", lambda root: {"basis": "synthetic verified transition"})
    clock, attempts = [10000.0], []
    monkeypatch.setattr(subject.time, "time", lambda: clock[0])
    monkeypatch.setattr(subject, "snapshot", lambda *a: {"status": "stopped"})
    monkeypatch.setattr(subject, "manager_state", lambda *a: {"manager": "legacy", "reason": "synthetic offline proof"})
    def spawn(*a, **kw):
        """Fail an actual synthetic spawn attempt before creating any process."""
        attempts.append(clock[0])
        raise OSError("synthetic spawn failure")
    monkeypatch.setattr(subject.subprocess, "Popen", spawn)
    with pytest.raises(OSError):
        subject.start(tmp_path, automatic=True)
    assert subject._retry_path(tmp_path).stat().st_mode & 0o777 == 0o600
    clock[0] += 5
    with pytest.raises(subject.RetryDeferred):
        subject.start(tmp_path, automatic=True)
    assert attempts == [10000]
    with pytest.raises(OSError):
        subject.start(tmp_path)  # Explicit action bypasses the first deadline.
    with subject.actions(tmp_path, "test"):
        retry = subject.retry_status(tmp_path)
    assert retry["failures"] == 2 and retry["next_retry"] == 10125
    for index in range(7):
        clock[0] = retry["next_retry"]
        with pytest.raises(OSError):
            subject.start(tmp_path, automatic=True)
        with subject.actions(tmp_path, "test"):
            retry = subject.retry_status(tmp_path)
        assert 60 <= retry["next_retry"] - clock[0] <= 1800
    assert retry["next_retry"] - clock[0] == 1800


@pytest.mark.parametrize("bad", ["{", '{"version":99}', '{"version":1,"failures":1,"last_failure":20000,"next_retry":20060}'])
def test_retry_repair_waits_once_and_clock_correction_is_bounded(tmp_path, monkeypatch, bad):
    """Corruption/future dates repair once; backwards clocks cannot create unbounded waits."""
    clock = [10000.0]
    monkeypatch.setattr(subject.time, "time", lambda: clock[0])
    path = subject._retry_path(tmp_path)
    path.parent.mkdir(parents=True)
    path.write_text(bad)
    with subject.actions(tmp_path, "test"):
        first = subject.retry_status(tmp_path)
        assert first["next_retry"] == 10060 and first["failures"] == 0
        assert subject.retry_status(tmp_path) == first
        clock[0] = 9000  # A wall-clock correction persists one new minimum wait.
        assert subject.retry_status(tmp_path)["next_retry"] == 9060
        clock[0] = 50000
        assert subject.retry_status(tmp_path)["next_retry"] == 9060


def test_successful_exact_start_clears_retry_and_unknown_does_not_count(tmp_path, monkeypatch):
    """Only an exact confirmed child clears failures; manager/identity uncertainty cannot count."""
    monkeypatch.setattr(subject, "registration_ready", lambda *a, **k: True)
    monkeypatch.setattr(subject, "transition_status", lambda root: {"basis": "synthetic verified transition"})
    identity = ProcessIdentity(23, 123)
    state = {"running": False, "manager": "unknown"}
    monkeypatch.setattr(subject, "manager_state", lambda *a: {"manager": state["manager"], "reason": "synthetic evidence"})
    monkeypatch.setattr(subject, "snapshot", lambda *a: {"status": "running", "identity": identity} if state["running"] else {"status": "stopped"})
    with pytest.raises(subject.ProcessStateError):
        subject.start(tmp_path, automatic=True)
    assert not subject._retry_path(tmp_path).exists()
    state["manager"] = "legacy"
    with subject.actions(tmp_path, "test"):
        subject._start_failed(tmp_path)
    def spawn(*a, **kw):
        """Publish only a synthetic exact child, without launching a process."""
        state["running"] = True
        return SimpleNamespace(pid=23, poll=lambda: None)
    monkeypatch.setattr(subject.subprocess, "Popen", spawn)
    monkeypatch.setattr(subject, "inspect_pid", lambda *a: {"status": "running", "identity": identity})
    monkeypatch.setattr(subject, "_CHILDREN", {})
    subject.start(tmp_path)  # Manual bypass of the existing wait.
    with subject.actions(tmp_path, "test"):
        assert subject.retry_status(tmp_path) == {"version": 1, "failures": 0, "last_failure": 0, "next_retry": 0}


def test_unwritable_retry_state_prevents_spawn(tmp_path, monkeypatch):
    """Automatic recovery cannot spawn when its shared deadline cannot be persisted."""
    monkeypatch.setattr(subject, "transition_status", lambda root: {"basis": "synthetic verified transition"})
    monkeypatch.setattr(subject, "snapshot", lambda *a: {"status": "stopped"})
    monkeypatch.setattr(subject, "manager_state", lambda *a: {"manager": "legacy", "reason": "synthetic proof"})
    monkeypatch.setattr(subject, "_write_retry", lambda *a: (_ for _ in ()).throw(PermissionError("synthetic denied")))
    monkeypatch.setattr(subject.subprocess, "Popen", lambda *a, **kw: pytest.fail("No spawn without persisted control"))
    with pytest.raises(subject.ProcessStateError):
        subject.start(tmp_path, automatic=True)


def test_reset_write_failure_does_not_kill_confirmed_child(tmp_path, monkeypatch):
    """Persistence failure after exact readiness is not a spawn or readiness failure."""
    monkeypatch.setattr(subject, "registration_ready", lambda *a, **k: True)
    monkeypatch.setattr(subject, "transition_status", lambda root: {"basis": "synthetic verified transition"})
    identity = ProcessIdentity(23, 123)
    state = {"running": False}
    monkeypatch.setattr(subject, "snapshot", lambda *a: {"status": "running", "identity": identity} if state["running"] else {"status": "stopped"})
    monkeypatch.setattr(subject, "manager_state", lambda *a: {"manager": "legacy", "reason": "synthetic proof"})
    monkeypatch.setattr(subject, "inspect_pid", lambda *a: {"status": "running", "identity": identity})
    monkeypatch.setattr(subject, "_CHILDREN", {})
    with subject.actions(tmp_path, "test"):
        subject._start_failed(tmp_path)
    original = subject._write_retry
    def write(root, value):
        """Fail only the final successful reset, leaving the pending attempt intact."""
        if state["running"] and value["next_retry"] == 0:
            raise PermissionError("synthetic reset failure")
        original(root, value)
    def spawn(*a, **kw):
        """Publish an exact synthetic healthy child."""
        state["running"] = True
        return SimpleNamespace(pid=23, poll=lambda: None)
    monkeypatch.setattr(subject, "_write_retry", write)
    monkeypatch.setattr(subject.subprocess, "Popen", spawn)
    monkeypatch.setattr(subject, "_terminate", lambda *a: pytest.fail("Healthy child must survive"))
    monkeypatch.setattr(subject, "_start_failed", lambda *a: pytest.fail("No spawn failure occurred"))
    with pytest.raises(PermissionError):
        subject.start(tmp_path)
    assert state["running"]
    persisted = json.loads(subject._retry_path(tmp_path).read_text())
    assert persisted["failures"] == 1 and persisted["attempt_at"] > 0


@pytest.fixture
def owned(tmp_path, monkeypatch):
    """Inspect only a synthetic process; never read production PID or /proc data."""
    state = SimpleNamespace(argv=["python", "-u", str(tmp_path / "PBCoinData.py")], cwd=tmp_path,
                            uid=os.getuid(), created=123.0, error=None, signals=[])

    class Process:
        """Provide allowlisted exact process fields or an explicit inspection error."""
        def __init__(self, pid):
            if state.error:
                raise state.error
        def create_time(self):
            """Return the synthetic kernel start time."""
            return state.created
        def status(self):
            """The synthetic owner is a live process."""
            return psutil.STATUS_RUNNING
        def uids(self):
            """Return real and effective ownership."""
            return SimpleNamespace(real=state.uid, effective=state.uid)
        def cmdline(self):
            """Return exact Python argv."""
            return state.argv
        def cwd(self):
            """Return the checkout working directory."""
            return str(state.cwd)

    monkeypatch.setattr(subject.psutil, "Process", Process)
    monkeypatch.setattr(subject, "manager_state", lambda root: {"manager": "legacy", "reason": "synthetic manager"})
    monkeypatch.setattr(subject, "_log", lambda *a, **k: None)
    state.root = tmp_path
    return state


@pytest.mark.parametrize("fault", ["valid", "uid", "cwd", "script", "argument", "creation", "denied", "gone"])
def test_exact_ownership_and_denial_are_distinct(owned, fault):
    """No suffix, argument-position or PID-reuse shortcut can authorize a signal."""
    if fault == "uid":
        owned.uid += 1
    elif fault == "cwd":
        owned.cwd = owned.root / "other"
    elif fault == "script":
        owned.argv[-1] = str(owned.root / "other/PBCoinData.py")
    elif fault == "argument":
        owned.argv.insert(2, "foreign.py")
    elif fault == "creation":
        owned.created += 1
    elif fault == "denied":
        owned.error = psutil.AccessDenied(23)
    elif fault == "gone":
        owned.error = psutil.NoSuchProcess(23)
    result = subject.inspect_pid(owned.root, 23, ProcessIdentity(23, 123))
    expected = "running" if fault == "valid" else ("unknown" if fault in {"creation", "denied"} else ("stopped" if fault == "gone" else "foreign"))
    assert result["status"] == expected


def test_denied_identity_never_stops_starts_or_cleans_files(owned, monkeypatch):
    """AccessDenied is not a process exit even when the old identity is persisted."""
    path = subject.pid_path(owned.root)
    path.parent.mkdir(parents=True)
    path.write_text("23")
    write_process_identity(path, ProcessIdentity(23, 123))
    before = path.read_bytes(), identity_path(path).read_bytes()
    owned.error = psutil.AccessDenied(23)
    monkeypatch.setattr(subject, "signal_exact_process", lambda *a: pytest.fail("No signal allowed"))
    monkeypatch.setattr(subject.subprocess, "Popen", lambda *a, **k: pytest.fail("No replacement allowed"))
    for action in (subject.stop, subject.start):
        with pytest.raises(subject.ProcessStateError):
            action(owned.root)
    assert (path.read_bytes(), identity_path(path).read_bytes()) == before


@pytest.mark.parametrize("result,expected", [("not-found", "legacy"), ("loaded", "systemd"), ("", "unknown"), ("timeout", "unknown"), ("missing-command", "unknown")])
def test_manager_requires_positive_unit_absence(tmp_path, monkeypatch, result, expected):
    """Missing command, bus errors and timeouts cannot authorize a legacy start."""
    import pbcoindata_manager as manager
    monkeypatch.setattr(manager, "_unit_paths", lambda *a: ([tmp_path], set()))
    monkeypatch.setattr(manager, "_offline_manager_absent", lambda *a: (_ for _ in ()).throw(ValueError("synthetic unknown")))
    def run(*args, **kwargs):
        """Return a synthetic manager response without accessing systemd."""
        if result == "timeout":
            raise subprocess.TimeoutExpired("systemctl", 3)
        if result == "missing-command":
            raise FileNotFoundError("systemctl")
        output = f"LoadState={result}\nActiveState=inactive\nMainPID=0\nJob=\n" if result else ""
        return SimpleNamespace(stdout=output, returncode=4 if result == "not-found" else (1 if not result else 0))
    monkeypatch.setattr(subject.subprocess, "run", run)
    monkeypatch.setattr(subject, "_log", lambda *a, **k: None)
    assert subject.manager_state(tmp_path)["manager"] == expected


def test_stop_escalates_only_same_owned_identity(owned, monkeypatch):
    """TERM and KILL use the same start time with bounded 10/2 second waits."""
    waits = []
    monkeypatch.setattr(subject, "signal_exact_process", lambda identity, signum: owned.signals.append((identity, signum)))
    monkeypatch.setattr(subject, "_wait_exit", lambda root, identity, timeout: waits.append(timeout) or timeout == 2)
    expected = ProcessIdentity(23, 123)
    subject._terminate(owned.root, expected)
    assert owned.signals == [(expected, signal.SIGTERM), (expected, signal.SIGKILL)]
    assert waits == [10, 2]


def test_identity_change_between_signals_aborts_escalation(owned, monkeypatch):
    """PID reuse after TERM can never authorize KILL of a replacement."""
    monkeypatch.setattr(subject, "signal_exact_process", lambda identity, signum: owned.signals.append(signum))

    def wait(*args):
        """Replace the synthetic process lifetime after its first signal."""
        owned.created += 1
        return False

    monkeypatch.setattr(subject, "_wait_exit", wait)
    with pytest.raises(subject.ProcessStateError):
        subject._terminate(owned.root, ProcessIdentity(23, 123))
    assert owned.signals == [signal.SIGTERM]


@pytest.mark.parametrize("kind", ["controller", "singleton"])
def test_os_leases_are_exclusive_and_cloexec(tmp_path, kind):
    """A live unrelated synthetic child cannot retain a parent's lease across exec."""
    lease = subject.FileLease(tmp_path, kind, "test")
    assert not os.get_inheritable(lease.fd)
    child = subprocess.Popen([sys.executable, "-c", "import sys; print('ready', flush=True); sys.stdin.readline()"],
                             stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, close_fds=False)
    try:
        assert child.stdout.readline().strip() == "ready"
        with pytest.raises(BlockingIOError):
            subject.FileLease(tmp_path, kind, "competing")
        lease.release()
        replacement = subject.FileLease(tmp_path, kind, "replacement")
        replacement.release()
    finally:
        lease.release()
        child.communicate("exit\n", timeout=3)
        child.stdout.close()


def test_controller_crash_releases_authority_and_metadata_is_not_a_lease(tmp_path):
    """Only a held live exact PBRun lease proves recovery; stale JSON cannot."""
    script = tmp_path / "PBRun.py"
    script.write_text(f"import sys\nsys.path.insert(0, {str(ROOT)!r})\nfrom pbcoindata_process import FileLease\nlease = FileLease('.', 'controller', 'pbrun')\nprint('ready', flush=True)\nsys.stdin.readline()\n")
    child = subprocess.Popen([sys.executable, str(script)], cwd=tmp_path, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    try:
        assert child.stdout.readline().strip() == "ready"
        assert controller_metadata(tmp_path)["controller"] == "pbrun"
        assert controller_metadata(tmp_path)["recovery_ready"] is True
        child.kill()  # This synthetic child owns only temporary test files.
        child.wait(timeout=3)
        assert controller_metadata(tmp_path)["recovery_ready"] is False
    finally:
        if child.poll() is None:
            child.kill()
        child.communicate(timeout=3)


def test_concurrent_starts_stop_and_reaping_use_only_synthetic_daemon(tmp_path, monkeypatch):
    """Two requests create one temporary daemon; TERM cleans files and reaps the child."""
    import pbcoindata_manager as manager

    children, identities = [], {}
    real_popen = subprocess.Popen

    def spawn(*args, **kwargs):
        """Track real test-created handles even if startup fails before registration."""
        child = real_popen(*args, **kwargs)
        children.append(child)
        identities[child.pid] = ProcessIdentity(child.pid, psutil.Process(child.pid).create_time())
        return child

    def target_groups(pid, deadline):
        """Replace only the hidden outer cgroup view for this exact live test child."""
        manager._check(deadline)
        child = next(child for child in children if child.pid == pid)
        assert child.poll() is None
        state = subject.inspect_pid(tmp_path, pid, identities[pid])
        assert state["status"] == "running"
        assert state["identity"] == identities[pid]
        return "/isolated-test.scope"

    script = tmp_path / "PBCoinData.py"
    script.write_text(f"import sys, signal, threading\nsys.path.insert(0, {str(ROOT)!r})\nfrom pbcoindata_process import register_current, clear_owned_files\nfrom pbcoindata_guard import acquire_daemon\nlease = acquire_daemon('.')\nstop = threading.Event()\nsignal.signal(signal.SIGTERM, lambda *args: stop.set())\nidentity = register_current('.')\ntry:\n stop.wait(30)\nfinally:\n clear_owned_files('.', identity)\n lease.release()\n")
    monkeypatch.setattr(subject, "manager_state", lambda root: {"manager": "legacy", "reason": "synthetic legacy"})
    monkeypatch.setattr(subject.subprocess, "Popen", spawn)
    monkeypatch.setattr(manager, "_target_groups", target_groups)
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(subject.start, [tmp_path, tmp_path]))
        state = subject.snapshot(tmp_path)
        assert state["status"] == "running"
        assert len(children) == 1
        assert subject._CHILDREN.get(children[0].pid) is children[0]
        assert identity_path(subject.pid_path(tmp_path)).stat().st_mode & 0o777 == 0o600
        subject.stop(tmp_path)
        assert subject.snapshot(tmp_path)["status"] == "stopped"
        assert not subject.pid_path(tmp_path).exists()
        assert all(subject._CHILDREN.get(child.pid) is not child for child in children)
    finally:
        for child in children:
            if child.poll() is None:
                child.kill()
            child.wait(timeout=3)
            with subject._CHILDREN_LOCK:
                if subject._CHILDREN.get(child.pid) is child:
                    subject._CHILDREN.pop(child.pid)


def test_start_timeout_cleans_only_verified_child(tmp_path, monkeypatch):
    """A ten-second readiness timeout invokes bounded cleanup for only its created PID."""
    identity = ProcessIdentity(23, 123)
    child = SimpleNamespace(pid=23, poll=lambda: None)
    calls = []
    clock = iter([0, 0, 11])
    monkeypatch.setattr(subject.time, "monotonic", lambda: next(clock, 11))
    monkeypatch.setattr(subject.time, "sleep", lambda delay: None)
    monkeypatch.setattr(subject, "actions", lambda *a: __import__("contextlib").nullcontext())
    monkeypatch.setattr(subject, "manager_state", lambda root: {"manager": "legacy", "reason": "synthetic legacy"})
    monkeypatch.setattr(subject, "snapshot", lambda root: {"status": "stopped"})
    monkeypatch.setattr(subject, "inspect_pid", lambda *a: {"status": "running", "identity": identity})
    monkeypatch.setattr(subject.subprocess, "Popen", lambda *a, **k: child)
    monkeypatch.setattr(subject, "_terminate", lambda root, current: calls.append(current))
    monkeypatch.setattr(subject, "clear_owned_files", lambda *a: None)
    monkeypatch.setattr(subject, "reap_children", lambda: None)
    try:
        with pytest.raises(subject.ProcessStateError, match="timed out"):
            subject.start(tmp_path)
        assert calls == [identity]
    finally:
        subject._CHILDREN.pop(23, None)


@pytest.mark.parametrize("failure", ["construct", "heartbeat", "watcher", "pid", None])
@pytest.mark.parametrize("signum", [signal.SIGTERM, signal.SIGINT])
def test_daemon_partial_initialization_and_signals_retain_lifetime_guard(tmp_path, monkeypatch, failure, signum):
    """The real main cleans resources even if initialization fails, without real signals."""
    import credential_process_registry
    spec = importlib.util.spec_from_file_location("coindata_daemon_cleanup", ROOT / "PBCoinData.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.chdir(tmp_path)
    handlers, calls = {}, []

    def install(number, callback):
        """Capture handlers instead of changing the test runner's signal disposition."""
        old = handlers.get(number)
        handlers[number] = callback
        return old

    def maybe_fail(stage):
        """Inject a bounded initialization failure."""
        calls.append(stage)
        if failure == stage:
            raise RuntimeError(stage)

    def wait(*, timeout):
        """Deliver a synthetic stop to the installed real daemon handler."""
        handlers[signum](signum, None)

    watcher = SimpleNamespace(start=lambda: maybe_fail("watcher"), stop=lambda: calls.append("watcher-stop"),
                              changed=SimpleNamespace(clear=lambda: None, set=lambda: calls.append("wake"), wait=wait))
    daemon = SimpleNamespace(_ini_watcher=watcher, _config_load_failed=False, _logged_idle=False,
                             save_pid=lambda: maybe_fail("pid"), load_config=lambda: None, _is_master=lambda: True,
                             load_data=lambda: None, load_metadata=lambda: None, update_mappings=lambda: None)

    def construct(**kwargs):
        """Return the synthetic daemon after the singleton has been acquired."""
        maybe_fail("construct")
        return daemon

    capability = SimpleNamespace(__enter__=lambda: maybe_fail("heartbeat"), close=lambda: calls.append("heartbeat-close"))
    monkeypatch.setattr(module, "CoinData", construct)
    monkeypatch.setattr(module, "_log", lambda *a, **k: None)
    monkeypatch.setattr(module, "_daemon_mapping_exchanges", lambda: None)
    monkeypatch.setattr(signal, "signal", install)
    monkeypatch.setattr(credential_process_registry, "ProcessCapabilityHeartbeat", lambda *a: capability)
    if failure:
        with pytest.raises(RuntimeError, match=failure):
            module.main()
    else:
        module.main()
        assert "wake" in calls
    if failure != "construct":
        assert "watcher-stop" in calls
    if failure not in {"construct"}:
        assert "heartbeat-close" in calls
    import pbcoindata_guard as guard
    with pytest.raises(BlockingIOError):
        subject.FileLease(tmp_path, "singleton", "test")
    # This unit test calls main inside pytest; real process-lifetime release is
    # separately proved in TestGuardProtocol's isolated children.
    for held in list(guard._LIFETIME_GUARDS):
        if held.root == tmp_path:
            held.release()
            guard._LIFETIME_GUARDS.remove(held)


@pytest.fixture
def manual_view(offline_manager, monkeypatch):
    """Provide complete synthetic manual evidence without reading host namespaces."""
    state = offline_manager
    manager = state.module
    state.process = 'sleep'  # A visible unrelated process with no Python script operand.
    (state.root / 'PBCoinData.py').write_text('# synthetic service\n')
    state.data['/proc/self/mountinfo'] = '1 0 0:1 / / rw - ext4 synthetic rw\n2 1 0:2 / /proc rw - proc proc rw\n'
    state.data['/proc/1/mountinfo'] = state.data['/proc/self/mountinfo']
    state.data['/proc/self/status'] = f'NSpid:\t{os.getpid()}\n'
    state.fds, state.closed, state.opens = {}, [], []
    state.ns_inodes = {'pid': manager.PID_NS_INIT_INO, 'user': 0xEFFFFFFD,
                       'cgroup': manager.CGROUP_NS_INIT_INO, 'mnt': 4026532999}
    state.fd_fault = None
    state.proc_device, state.proc_mount_id = (0, 2), 2
    original_open, original_close = os.open, os.close
    original_fstat, original_read, original_ioctl = os.fstat, os.read, fcntl.ioctl
    original_evidence_read = manager._read

    def open_evidence(path, flags, *args, **kwargs):
        """Model kernel descriptors; keep parsing and all FD validation in production code."""
        path = str(path)
        if path.startswith('/proc/') and ('/ns/' in path or path.endswith('/mountinfo')):
            if path.startswith('/proc/1/ns/') or state.fd_fault == 'denied':
                raise PermissionError('Masked namespace reference')
            if state.fd_fault == 'mount-denied' and path.endswith('/mountinfo'):
                raise PermissionError('Denied mountinfo')
            mount_opens = sum(p.endswith('/mountinfo') for p in state.opens)
            if path.endswith('/mountinfo') and state.fd_fault == f'open-{mount_opens + 1}':
                raise PermissionError('Partial mount pair failure')
            if path.endswith('/mountinfo') and mount_opens % 4 >= 2:
                assert sum(row[0].endswith('/mountinfo') for row in state.fds.values()) >= 2
            fd = 50000 + len(state.opens)
            state.opens.append(path)
            if state.fd_fault == 'ns-change' and mount_opens == 3 and path.endswith('/mountinfo'):
                state.ns_inodes['mnt'] += 1
            payload = state.data.get(path, '').encode()
            if state.fd_fault == 'fresh-pid1' and path == '/proc/1/mountinfo' and state.opens.count(path) == 2:
                payload = payload.replace(b'1 0 0:1', b'9 0 0:1').replace(b'2 1 0:2', b'2 9 0:2')
            state.fds[fd] = [path, payload, 0]
            return fd
        return original_open(path, flags, *args, **kwargs)

    def fstat_evidence(fd):
        """Namespace files are regular nsfs inodes; mountinfo belongs to the procfs mount."""
        if fd not in state.fds:
            return original_fstat(fd)
        path = state.fds[fd][0]
        namespace = '/ns/' in path
        return SimpleNamespace(st_dev=5 if namespace else os.makedev(*state.proc_device),
                               st_ino=state.ns_inodes[path.rsplit('/', 1)[1]] if namespace else 123,
                               st_mode=(stat.S_IFDIR if state.fd_fault == 'directory' else stat.S_IFREG) | 0o444)

    def read_evidence_fd(fd, count):
        """Return bounded bytes from the payload captured at open time."""
        if fd not in state.fds:
            return original_read(fd, count)
        row = state.fds[fd]
        result = row[1][row[2]:row[2] + count]
        row[2] += len(result)
        return result

    def ioctl_evidence(fd, operation, *args):
        """Expose the actual requested NS_GET_NSTYPE contract, without a positive helper stub."""
        if fd not in state.fds:
            return original_ioctl(fd, operation, *args)
        assert operation == 0xb703
        if state.fd_fault == 'ioctl':
            raise OSError('Namespace type unavailable')
        kind = state.fds[fd][0].rsplit('/', 1)[1]
        return 0 if state.fd_fault == 'wrong-type' else {'pid': 0x20000000, 'mnt': 0x20000,
                                                       'user': 0x10000000, 'cgroup': 0x2000000}[kind]

    def close_evidence(fd):
        """Track every descriptor release, including partially opened pairs."""
        if fd not in state.fds:
            return original_close(fd)
        state.closed.append(fd)
        del state.fds[fd]

    def read_evidence(path, deadline):
        """Provide fdinfo's authoritative mount attachment, never a namespace symlink stat."""
        if str(path).startswith('/proc/self/fdinfo/'):
            manager._check(deadline)
            return f'mnt_id:\t{999 if state.fd_fault == "fd-mount" else state.proc_mount_id}\n'
        return original_evidence_read(path, deadline)

    def fstatfs_evidence(fd, result):
        """Model the independent kernel filesystem-type syscall, including its failures."""
        assert fd in state.fds
        result[0] = 0 if state.fd_fault == 'fake-procfs' else 0x9fa0
        return -1 if state.fd_fault == 'fstatfs-denied' else 0

    monkeypatch.setattr(manager.os, 'open', open_evidence)
    monkeypatch.setattr(manager.os, 'fstat', fstat_evidence)
    monkeypatch.setattr(manager.os, 'read', read_evidence_fd)
    monkeypatch.setattr(manager.os, 'close', close_evidence)
    monkeypatch.setattr(fcntl, 'ioctl', ioctl_evidence)
    monkeypatch.setattr(manager, '_read', read_evidence)
    monkeypatch.setattr(manager.ctypes, 'CDLL', lambda *a, **k: SimpleNamespace(fstatfs=fstatfs_evidence))
    monkeypatch.setattr(manager, '_unit_paths_without_helper', lambda *a: ([state.units], {state.runtime}))
    monkeypatch.setattr(subject, 'manager_state', lambda root: {'manager': 'unknown', 'reason': 'synthetic missing tools'})
    state.process_state = {'status': 'stopped', 'pid': None}
    monkeypatch.setattr(subject, 'snapshot', lambda root: dict(state.process_state))
    return state


def _publish_conflicting_reference(root, pid):
    """Model a known checkout/alias reference without globally classifying host services."""
    import pbcoindata_guard as guard
    path = guard.registration_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({**guard.scope(root), "pid": pid, "create_time": 123.0, "phase": "starting"}))


@pytest.fixture
def native_nsfs_view(manual_view):
    """Use the whole supplied native table with controlled, matching procfs descriptors."""
    state = manual_view
    payload = (ROOT / 'tests/fixtures/pbcoindata_mountinfo_3411.txt').read_bytes()
    assert hashlib.sha256(payload).hexdigest() == '44838a252e318301794d172eb5c6067e6ef884d0f2f3791582ffd61a6f496da3'
    state.native_mountinfo = payload.decode('utf-8')
    state.native_label = 'mnt:[4026532668]'
    state.data['/proc/self/mountinfo'] = state.data['/proc/1/mountinfo'] = state.native_mountinfo
    state.proc_device, state.proc_mount_id = (0, 26), 29
    return state


class TestManualAuthorization:
    """Action proofs preserve safe manual controls without relaxing automatic UNKNOWN."""

    def test_complete_native_nsfs_mountinfo_authorization(self, manual_view):
        """The complete byte-bound 55-row host shape passes every real validation stage."""
        state, manager = manual_view, manual_view.module
        payload = (ROOT / 'tests/fixtures/pbcoindata_mountinfo_3411.txt').read_bytes()
        assert hashlib.sha256(payload).hexdigest() == '44838a252e318301794d172eb5c6067e6ef884d0f2f3791582ffd61a6f496da3'
        text = payload.decode('utf-8')
        expected = ('mnt:[4026532668]', 'mnt:[4026532400]', 'mnt:[4026532421]', 'mnt:[4026532401]')
        state.data['/proc/self/mountinfo'] = state.data['/proc/1/mountinfo'] = text
        state.proc_device, state.proc_mount_id = (0, 26), 29
        rows = manager._mountinfo(text, time.monotonic() + 5)
        assert len(rows) == len(text.splitlines()) == 55
        assert {row.root for row in rows if row.filesystem == 'nsfs'} == set(expected)
        raw = {int(line.split()[0]): line.split() for line in text.splitlines()}
        for row in rows:
            fields = raw[row.ident]
            assert row.source == fields[-2]
            assert row.options == tuple(fields[5].split(','))
            assert row.super_options == tuple(fields[-1].split(','))
        proc, checkout = manager._mount_view(rows, state.root)
        assert proc.ident == 29 and proc.device == (0, 26)
        assert checkout.ident == 33 and checkout.filesystem == 'ext4'
        with manager.ExitStack() as stack:
            assert manager._mountinfo_fd('self', stack, state.root, time.monotonic() + 5) == rows
        assert not state.fds
        # Reset only the observation counter for the independent four-open checks.
        state.opens.clear()
        view = manager._manual_start_view(state.root, time.monotonic() + 5)
        assert view[1] == rows and not state.fds
        with subject.manual_action(state.root, 'start') as proof:
            assert subject._consume_authorization(state.root, 'start', proof)['status'] == 'stopped'
            assert proof.namespaces[-1][1] == rows
        assert not state.fds
        assert state.opens.count('/proc/self/mountinfo') == state.opens.count('/proc/1/mountinfo') == 6

    @pytest.mark.parametrize('kind', ['cgroup', 'ipc', 'mnt', 'net', 'pid', 'pid_for_children',
                                     'time', 'time_for_children', 'user', 'uts'])
    def test_nsfs_supported_namespace_labels(self, native_nsfs_view, kind):
        """Each documented type remains an opaque root label throughout complete authorization."""
        state = native_nsfs_view
        label = f'{kind}:[4026532668]'
        text = state.native_mountinfo.replace(state.native_label, label)
        state.data['/proc/self/mountinfo'] = state.data['/proc/1/mountinfo'] = text
        with subject.manual_action(state.root, 'start') as proof:
            subject._consume_authorization(state.root, 'start', proof)
            assert next(row.root for row in proof.namespaces[-1][1] if row.ident == 2441) == label
        assert not state.fds

    @pytest.mark.parametrize('inode', ['1', '18446744073709551615'])
    def test_nsfs_inode_bounds(self, native_nsfs_view, inode):
        """Positive ASCII unsigned-long boundary values are retained without normalization."""
        state = native_nsfs_view
        label = f'mnt:[{inode}]'
        text = state.native_mountinfo.replace(state.native_label, label)
        state.data['/proc/self/mountinfo'] = state.data['/proc/1/mountinfo'] = text
        with subject.manual_action(state.root, 'start') as proof:
            subject._consume_authorization(state.root, 'start', proof)
            assert next(row.root for row in proof.namespaces[-1][1] if row.ident == 2441) == label
        assert not state.fds

    @pytest.mark.parametrize('label', ['other:[1]', 'mnt:[0]', 'mnt:[-1]', 'mnt:[01]', 'mnt:[+1]',
                                      'mnt:[١]', 'mnt:[1١]', 'mnt:[18446744073709551616]',
                                      'mnt:[' + '9' * 100 + ']', 'mnt:[1]suffix', 'mnt:1', 'mnt:[1',
                                      'mnt:1]', 'mnt:[]', 'mnt:[1]]', '/absolute',
                                      r'mnt:[1\040]', r'mnt:[1\134]', r'mnt:[1\777]', 'mnt:[1\x07]'])
    def test_invalid_nsfs_labels_remain_blocking(self, native_nsfs_view, label):
        """Relative paths, malformed labels, escapes and non-ASCII or out-of-range numbers cannot authorize."""
        state = native_nsfs_view
        text = state.native_mountinfo.replace(state.native_label, label)
        state.data['/proc/self/mountinfo'] = state.data['/proc/1/mountinfo'] = text
        with pytest.raises(subject.ProcessStateError):
            with subject.manual_action(state.root, 'start'):
                pytest.fail('Invalid namespace label must block')
        assert not state.fds

    @pytest.mark.parametrize('fault', ['ext4', 'tmpfs', 'proc', 'mountpoint', 'root', 'procfs',
                                     'checkout', 'proc-child', 'source-empty', 'source-escape',
                                     'options-empty', 'options-comma', 'super-empty', 'super-comma'])
    def test_nsfs_exception_cannot_bypass_mount_validation(self, native_nsfs_view, fault):
        """The full host table still rejects label misuse and malformed source/option fields."""
        state = native_nsfs_view
        lines = state.native_mountinfo.splitlines()
        index = next(i for i, line in enumerate(lines) if line.startswith('2441 '))
        fields = lines[index].split()
        if fault in {'ext4', 'tmpfs', 'proc'}:
            fields[-3] = fault
        elif fault == 'mountpoint':
            fields[4] = state.native_label
        elif fault in {'root', 'procfs'}:
            target = '33 ' if fault == 'root' else '29 '
            index = next(i for i, line in enumerate(lines) if line.startswith(target))
            fields = lines[index].split()
            fields[3], fields[-3] = state.native_label, 'nsfs'
        elif fault in {'checkout', 'proc-child'}:
            fields[1], fields[4] = ('33', str(state.root)) if fault == 'checkout' else ('29', '/proc/42')
        elif fault == 'source-empty': fields[-2] = ''
        elif fault == 'source-escape': fields[-2] = r'bad\777'
        elif fault == 'options-empty': fields[5] = ''
        elif fault == 'options-comma': fields[5] = 'rw,'
        elif fault == 'super-empty': fields[-1] = ''
        else: fields[-1] = 'rw,'
        lines[index] = ' '.join(fields)
        text = '\n'.join(lines) + '\n'
        state.data['/proc/self/mountinfo'] = state.data['/proc/1/mountinfo'] = text
        with pytest.raises(subject.ProcessStateError):
            with subject.manual_action(state.root, 'start'):
                pytest.fail('The nsfs decoder must not waive later validation')
        assert not state.fds

    @pytest.mark.parametrize('stage', ['pid1', 'fresh-open', 'use'])
    def test_nsfs_label_change_invalidates_evidence(self, native_nsfs_view, monkeypatch, stage):
        """A changed historical mount label is detected at comparison, reopen and proof consumption."""
        state, manager = native_nsfs_view, native_nsfs_view.module
        changed = state.native_mountinfo.replace(state.native_label, 'mnt:[4026532998]')
        if stage == 'pid1':
            state.data['/proc/1/mountinfo'] = changed
        elif stage == 'fresh-open':
            original_open = manager.os.open
            def changed_open(path, flags, *args, **kwargs):
                """Change PID 1's observation only at the fresh fourth mountinfo open."""
                if str(path) == '/proc/1/mountinfo' and state.opens.count(str(path)) == 1:
                    state.data[str(path)] = changed
                return original_open(path, flags, *args, **kwargs)
            monkeypatch.setattr(manager.os, 'open', changed_open)
        if stage == 'use':
            with subject.manual_action(state.root, 'start') as proof:
                state.data['/proc/self/mountinfo'] = state.data['/proc/1/mountinfo'] = changed
                with pytest.raises(subject.ProcessStateError, match='evidence changed'):
                    subject._consume_authorization(state.root, 'start', proof)
        else:
            with pytest.raises(subject.ProcessStateError):
                with subject.manual_action(state.root, 'start'):
                    pytest.fail('Changed nsfs label must block')
        assert not state.fds

    def test_native_nsfs_budget_closes_all_pinned_descriptors(self, native_nsfs_view, monkeypatch):
        """Full host observations retain the existing shared five-second limit and deterministic cleanup."""
        state, manager = native_nsfs_view, native_nsfs_view.module
        original_read = manager.os.read
        clock = [0.0]
        monkeypatch.setattr(manager.time, 'monotonic', lambda: clock[0])
        def timed_read(fd, count):
            """Expire the shared budget while the fresh PID-1 table is being read."""
            value = original_read(fd, count)
            if state.opens.count('/proc/1/mountinfo') == 2:
                clock[0] = 5.0
            return value
        monkeypatch.setattr(manager.os, 'read', timed_read)
        with pytest.raises(TimeoutError):
            manager._manual_start_view(state.root, 5.0)
        assert not state.fds

    @pytest.mark.parametrize('kernel', ['6.8', '6.14', '7.0'])
    def test_masked_pid1_with_stacked_binfmt(self, manual_view, kernel):
        """Dynamic mount namespace inodes and masked PID-1 links use pinned mountinfo on every kernel profile."""
        state = manual_view
        state.ns_inodes['mnt'] += ['6.8', '6.14', '7.0'].index(kernel)
        state.data['/proc/self/mountinfo'] += (
            '3 2 0:3 / /proc/sys/fs/binfmt_misc rw,relatime - autofs systemd-1 rw,fd=32,pgrp=1,direct\n'
            '4 3 0:4 / /proc/sys/fs/binfmt_misc rw - binfmt_misc binfmt_misc rw\n')
        state.data['/proc/1/mountinfo'] = state.data['/proc/self/mountinfo']
        with subject.manual_action(state.root, 'start') as proof:
            subject._consume_authorization(state.root, 'start', proof)
        assert state.opens.count('/proc/self/mountinfo') >= 2
        assert state.opens.count('/proc/1/mountinfo') >= 2
        assert not any(path.startswith('/proc/1/ns/') for path in state.opens)
        assert not state.fds

    @pytest.mark.parametrize('fault', ['denied', 'directory', 'wrong-type', 'ioctl', 'mount-denied',
                                     'fake-procfs', 'fstatfs-denied', 'fd-mount', 'fresh-pid1', 'ns-change',
                                     'open-1', 'open-2', 'open-3', 'open-4'])
    def test_descriptor_failures_close_every_fd(self, manual_view, fault):
        """Type, attachment, filesystem and fresh-open failures never authorize or retain descriptors."""
        state = manual_view
        state.fd_fault = fault
        with pytest.raises(subject.ProcessStateError):
            with subject.manual_action(state.root, 'start'):
                pytest.fail('Incomplete descriptor evidence must block')
        assert not state.fds

    @pytest.mark.parametrize('fault', ['clone', 'chroot', 'checkout', 'root-missing', 'duplicate-id',
                                     'parent-missing', 'cycle', 'separator', 'truncated', 'escape',
                                     'similar-path', 'pid-cover', 'self-cover', 'thread-cover', 'ns-cover',
                                     'kernel-cover', 'bind-binfmt', 'third-stack', 'hidepid', 'subset'])
    def test_mount_parser_rejects_hidden_or_ambiguous_views(self, manual_view, fault):
        """Actual parser and FD checks reject namespace clones, overlays and malformed topology."""
        state = manual_view
        text = state.data['/proc/self/mountinfo']
        if fault == 'clone':
            state.data['/proc/1/mountinfo'] = text.replace('1 0 0:1', '9 0 0:1').replace('2 1 0:2', '2 9 0:2')
        elif fault == 'chroot': text = text.replace('0:1 / /', '0:1 /private /')
        elif fault == 'checkout':
            state.data['/proc/1/mountinfo'] += f'3 1 0:3 / {state.root} rw - ext4 other rw\n'
        elif fault == 'root-missing': text = text.splitlines(keepends=True)[1]
        elif fault == 'duplicate-id': text += '2 1 0:4 / /other rw - tmpfs tmpfs rw\n'
        elif fault == 'parent-missing': text = text.replace('2 1 0:2', '2 99 0:2')
        elif fault == 'cycle': text += '3 4 0:3 / /a rw - tmpfs x rw\n4 3 0:4 / /a/b rw - tmpfs x rw\n'
        elif fault == 'separator': text = text.replace(' - proc ', ' proc ')
        elif fault == 'truncated': text = text.rstrip('\n')
        elif fault == 'escape': text = text.replace('/proc ', '/proc\\777 ')
        elif fault == 'hidepid': text = text.replace('- proc proc rw', '- proc proc rw,hidepid=invisible')
        elif fault == 'subset': text = text.replace('- proc proc rw', '- proc proc rw,subset=pid')
        elif fault == 'third-stack':
            text += ''.join(f'{i} 2 0:{i} / /proc/sys/fs/binfmt_misc rw - autofs x rw\n' for i in (3, 4, 5))
        else:
            path = {'similar-path': '/proc/sys/fs/binfmt_misc_extra', 'pid-cover': '/proc/42',
                    'self-cover': '/proc/self', 'thread-cover': '/proc/thread-self', 'ns-cover': '/proc/1/ns',
                    'kernel-cover': '/proc/sys/kernel', 'bind-binfmt': '/proc/sys/fs/binfmt_misc'}[fault]
            text += f'3 2 0:3 / {path} rw - tmpfs x rw\n'
        state.data['/proc/self/mountinfo'] = text
        with pytest.raises(subject.ProcessStateError):
            with subject.manual_action(state.root, 'start'):
                pytest.fail('Uncertain mount view must block')
        assert not state.fds

    @pytest.mark.parametrize('mapping', ['root', 'proc', 'checkout'])
    def test_changed_mount_identity_invalidates_action_proof(self, manual_view, mapping):
        """Even two mutually consistent fresh tables cannot reuse authorization for a changed mount."""
        state = manual_view
        with subject.manual_action(state.root, 'start') as proof:
            text = state.data['/proc/self/mountinfo']
            if mapping == 'root':
                text = text.replace('1 0 0:1', '9 0 0:1').replace('2 1 0:2', '2 9 0:2')
            elif mapping == 'proc':
                text = text.replace('2 1 0:2', '9 1 0:2')
                state.proc_mount_id = 9
            else:
                text += f'3 1 0:3 / {state.root} rw - ext4 other rw\n'
            state.data['/proc/self/mountinfo'] = state.data['/proc/1/mountinfo'] = text
            with pytest.raises(subject.ProcessStateError):
                subject._consume_authorization(state.root, 'start', proof)
        assert not state.fds

    def test_mount_read_budget_and_fd_cleanup(self, manual_view, monkeypatch):
        """A bounded read which exhausts the shared five seconds releases its original descriptor."""
        state, manager = manual_view, manual_view.module
        original = manager.os.read
        clock = [0.0]
        monkeypatch.setattr(manager.time, 'monotonic', lambda: clock[0])
        def slow_read(fd, size):
            """Consume the remaining shared budget during the first procfs read."""
            result = original(fd, size)
            clock[0] = 5.0
            return result
        monkeypatch.setattr(manager.os, 'read', slow_read)
        with pytest.raises(TimeoutError):
            manager._manual_start_view(state.root, 5.0)
        assert not state.fds

    def test_mount_read_limit_and_escaped_paths(self, manual_view):
        """Valid escaped mount names parse; oversized metadata fails within the existing read limit."""
        state, manager = manual_view, manual_view.module
        text = state.data['/proc/self/mountinfo'] + '3 1 0:3 / /space\\040hash\\043paren( rw - tmpfs none rw\n'
        state.data['/proc/self/mountinfo'] = state.data['/proc/1/mountinfo'] = text
        manager._manual_start_view(state.root, time.monotonic() + 5)
        state.data['/proc/self/mountinfo'] += 'x' * 262145
        with pytest.raises(ValueError, match='read limit'):
            manager._manual_start_view(state.root, time.monotonic() + 5)
        assert not state.fds

    @pytest.mark.parametrize('filesystem,backing,parent', [('btrfs', '/@', '0'), ('ext4', '/', '1')])
    def test_matching_native_root_forms(self, manual_view, filesystem, backing, parent):
        """A Btrfs root subvolume or a kernel self-parent root still has an exact pinned identity."""
        state = manual_view
        text = state.data['/proc/self/mountinfo'].replace('1 0 0:1 / / rw - ext4',
                                                        f'1 {parent} 0:1 {backing} / rw - {filesystem}')
        state.data['/proc/self/mountinfo'] = state.data['/proc/1/mountinfo'] = text
        with subject.manual_action(state.root, 'start') as proof:
            subject._consume_authorization(state.root, 'start', proof)
        assert not state.fds

    def test_mount_source_may_be_dash(self, manual_view):
        """The kernel separator is distinct from an otherwise valid source named dash."""
        state = manual_view
        text = state.data['/proc/self/mountinfo'] + '3 1 0:3 / /mnt rw - tmpfs - rw\n'
        state.data['/proc/self/mountinfo'] = state.data['/proc/1/mountinfo'] = text
        with subject.manual_action(state.root, 'start') as proof:
            subject._consume_authorization(state.root, 'start', proof)
        assert not state.fds


    @pytest.mark.parametrize('environment', ['no-tools', 'container', 'WSL'])
    def test_positive_initial_namespace_without_systemd_helpers(self, manual_view, monkeypatch, environment):
        """Markers alone cannot reject a fully inspectable initial namespace."""
        state = manual_view
        if environment == 'container':
            monkeypatch.setenv('container', 'synthetic')
        elif environment == 'WSL':
            monkeypatch.setenv('WSL_DISTRO_NAME', 'synthetic')
        with subject.manual_action(state.root, 'start') as proof:
            assert proof.route == 'manual' and proof.finding['manager'] == 'unknown'
            assert subject._consume_authorization(state.root, 'start', proof)['status'] == 'stopped'
            with pytest.raises(subject.ProcessStateError):
                subject._consume_authorization(state.root, 'stop', proof)
            with pytest.raises(subject.ProcessStateError):
                subject.start(state.root, automatic=True, authorization=proof)
        with pytest.raises(subject.ProcessStateError):
            subject._consume_authorization(state.root, 'start', proof)
        with pytest.raises(subject.ProcessStateError):
            subject.start(state.root)
        with pytest.raises(subject.ProcessStateError):
            subject.start(state.root, automatic=True)

    @pytest.mark.parametrize('fault', ['hidepid', 'hidepid-superblock', 'proc-shadow', 'nested', 'namespace-denied', 'network-checkout', 'live-manager', 'manager-argv-alias', 'manager-exe-denied', 'competitor', 'competitor-other-uid', 'script-alias', 'foreign-executable-script-alias', 'foreign-exe-denied', 'unit', 'socket', 'process-denied'])
    def test_incomplete_start_evidence_is_concrete_refusal(self, manual_view, monkeypatch, fault):
        """Neither missing tools nor manual intent bypasses incomplete ownership evidence."""
        state, manager = manual_view, manual_view.module
        if fault == 'hidepid':
            state.data['/proc/self/mountinfo'] = state.data['/proc/self/mountinfo'].replace('/proc rw', '/proc rw,hidepid=2')
        elif fault == 'hidepid-superblock':
            state.data['/proc/self/mountinfo'] = state.data['/proc/self/mountinfo'].replace('- proc proc rw', '- proc proc rw,hidepid=2')
        elif fault == 'proc-shadow':
            state.data['/proc/self/mountinfo'] += '3 2 0:3 / /proc/42 rw - tmpfs tmpfs rw\n'
        elif fault == 'nested':
            monkeypatch.setattr(manager, '_namespace', lambda *a: (1, 99))
        elif fault == 'namespace-denied':
            monkeypatch.setattr(manager, '_namespace', lambda *a: (_ for _ in ()).throw(PermissionError('synthetic denial')))
        elif fault == 'network-checkout':
            state.data['/proc/self/mountinfo'] = state.data['/proc/self/mountinfo'].replace('- ext4', '- 9p')
        elif fault == 'live-manager':
            state.process = 'systemd'
        elif fault == 'manager-argv-alias':
            process_type = manager.psutil.Process
            class AliasedManager(process_type):
                """Hide systemd in argv only; kernel name/executable remain authoritative."""
                def cmdline(self):
                    """Use an argv0 alias with the real user-manager argument."""
                    return ['init-alias', '--user']
                def name(self):
                    """Expose the current kernel name."""
                    return 'systemd'
                def exe(self):
                    """Expose the current executable identity."""
                    return '/synthetic/systemd'
            monkeypatch.setattr(manager.psutil, 'Process', AliasedManager)
        elif fault == 'manager-exe-denied':
            process_type = manager.psutil.Process
            class DeniedExecutable(process_type):
                """Represent relevant executable evidence that cannot be read."""
                def exe(self):
                    """Deny the executable rather than treating it as absent."""
                    raise psutil.AccessDenied(42)
            monkeypatch.setattr(manager.psutil, 'Process', DeniedExecutable)
        elif fault in {'competitor', 'competitor-other-uid', 'script-alias', 'foreign-executable-script-alias', 'foreign-exe-denied'}:
            _publish_conflicting_reference(state.root, 42)
            process_type = manager.psutil.Process
            class Competitor(process_type):
                """Represent an unpublished daemon using this exact checkout inode."""
                def cmdline(self):
                    """Use the real synthetic script path at the script position."""
                    return ['py_alias' if fault == 'foreign-executable-script-alias' else 'python', '-u', str(state.root / ('coin_alias.py' if fault in {'script-alias', 'foreign-executable-script-alias'} else 'PBCoinData.py'))]
                def name(self):
                    """A foreign executable may also have an aliased kernel name."""
                    return 'py_alias' if fault == 'foreign-executable-script-alias' else 'python'
                def exe(self):
                    """The executable is authoritative even for a different UID."""
                    if fault == 'foreign-exe-denied':
                        raise psutil.AccessDenied(42)
                    return '/synthetic/python3.12'
                def uids(self):
                    """Foreign ownership does not make a competing daemon disappear."""
                    uid = os.getuid() + (fault in {'competitor-other-uid', 'foreign-executable-script-alias', 'foreign-exe-denied'})
                    return SimpleNamespace(real=uid, effective=uid)
            monkeypatch.setattr(manager.psutil, 'Process', Competitor)
            info = (state.root / 'PBCoinData.py').stat()
            original_stat = Path.stat
            candidate = Path('/proc/42/root') / str(state.root / ('coin_alias.py' if fault in {'script-alias', 'foreign-executable-script-alias'} else 'PBCoinData.py')).lstrip('/')
            monkeypatch.setattr(Path, 'stat', lambda path, **kw: info if path == candidate else original_stat(path, **kw))
        elif fault == 'unit':
            (state.units / subject.UNIT).symlink_to('/dev/null')
        elif fault == 'socket':
            (state.runtime / 'bus').touch()
        else:
            state.denied = True
        with pytest.raises(subject.ProcessStateError):
            with subject.manual_action(state.root, 'start'):
                pytest.fail('No authorization for incomplete evidence')

    @pytest.mark.parametrize('fault', ['none', 'systemd', 'namespace', 'opaque-cgroup', 'changed-identity', 'foreign', 'unknown'])
    def test_target_only_stop_is_exact_and_fresh(self, manual_view, monkeypatch, fault):
        """Stop does not need a global start proof but cannot signal another target."""
        state, manager = manual_view, manual_view.module
        state.process_state = {'status': 'running', 'pid': 42, 'identity': ProcessIdentity(42, 123)}
        if fault == 'systemd':
            state.data['/proc/42/cgroup'] = '0::/user.slice/' + subject.UNIT
        elif fault == 'namespace':
            monkeypatch.setattr(manager, '_namespace', lambda pid, *a: (1, 99 if pid == 42 else 2))
        elif fault == 'opaque-cgroup':
            original = manager._namespace
            monkeypatch.setattr(manager, '_namespace', lambda pid, kind, deadline: (1, 99) if kind == 'cgroup' else original(pid, kind, deadline))
        elif fault in {'foreign', 'unknown'}:
            state.process_state['status'] = fault
        calls = []
        monkeypatch.setattr(subject, '_terminate', lambda root, identity, **kwargs: calls.append(identity))
        monkeypatch.setattr(subject, 'clear_owned_files', lambda *a: None)
        monkeypatch.setattr(subject, 'write_process_identity', lambda *a: None)
        if fault in {'none', 'changed-identity'}:
            with subject.manual_action(state.root, 'stop') as proof:
                if fault == 'changed-identity':
                    state.process_state['identity'] = ProcessIdentity(42, 124)
                    with pytest.raises(subject.ProcessStateError, match='target changed'):
                        subject.stop(state.root, authorization=proof)
                else:
                    subject.stop(state.root, authorization=proof)
            assert calls == ([ProcessIdentity(42, 123)] if fault == 'none' else [])
        else:
            with pytest.raises(subject.ProcessStateError):
                with subject.manual_action(state.root, 'stop'):
                    pytest.fail('Unsafe stop authorized')
            assert not calls


class TestLaunchJournal:
    """Durable reservations fence uncertain children across retries and controller changes."""

    @pytest.fixture(autouse=True)
    def approved_transition(self, monkeypatch):
        """These launch-only cases start with separately verified operator evidence."""
        monkeypatch.setattr(subject, "transition_status", lambda root: {"basis": "synthetic verified transition"})

    @pytest.mark.parametrize('failure', ['file-fsync', 'directory-fsync', 'directory-open'])
    def test_durability_failure_never_spawns(self, tmp_path, monkeypatch, failure):
        """Both file and directory persistence must finish before Popen is allowed."""
        monkeypatch.setattr(subject, 'manager_state', lambda root: {'manager': 'legacy', 'reason': 'synthetic'})
        monkeypatch.setattr(subject, 'snapshot', lambda root: {'status': 'stopped'})
        original_write = subject.atomic_write_private_text
        original_open, original_fsync = subject.os.open, subject.os.fsync
        active, syncing = [False], [False]
        original_sync_directory = subject._sync_launch_directory
        def sync_directory(root):
            """Target only directory persistence, not the preceding no-follow read."""
            syncing[0] = True
            try: return original_sync_directory(root)
            finally: syncing[0] = False
        monkeypatch.setattr(subject, "_sync_launch_directory", sync_directory)
        def write(path, value):
            """Inject failures only into journal writes, preserving retry persistence."""
            active[0] = Path(path) == subject._launch_path(tmp_path)
            try:
                return original_write(path, value)
            finally:
                active[0] = False
        def sync(fd):
            """Fail the actual requested journal sync stage."""
            import stat
            directory = stat.S_ISDIR(os.fstat(fd).st_mode)
            if (failure == 'file-fsync' and active[0] and not directory) or (failure == 'directory-fsync' and directory):
                raise OSError('synthetic sync failure')
            return original_fsync(fd)
        def open_fd(path, flags, *args, **kwargs):
            """Fail only the directory open used by journal synchronization."""
            if failure == 'directory-open' and syncing[0] and flags & os.O_DIRECTORY:
                raise PermissionError('synthetic directory denial')
            return original_open(path, flags, *args, **kwargs)
        monkeypatch.setattr(subject, 'atomic_write_private_text', write)
        monkeypatch.setattr(subject.os, 'fsync', sync)
        monkeypatch.setattr(subject.os, 'open', open_fd)
        monkeypatch.setattr(subject.subprocess, 'Popen', lambda *a, **k: pytest.fail('No spawn without durable reservation'))
        with pytest.raises(OSError):
            subject.start(tmp_path, automatic=True)
        assert json.loads(subject._retry_path(tmp_path).read_text())['failures'] == 0

    def test_unconfirmed_child_never_gets_replacement(self, tmp_path, monkeypatch):
        """Reproduce the former two-Popen bug with real locks and an isolated live-child double."""
        clock, wall, spawned = [0.0], [10000.0], []
        monkeypatch.setattr(subject.time, 'monotonic', lambda: clock[0])
        monkeypatch.setattr(subject.time, 'time', lambda: wall[0])
        monkeypatch.setattr(subject.time, 'sleep', lambda delay: clock.__setitem__(0, clock[0] + 1))
        monkeypatch.setattr(subject, 'manager_state', lambda root: {'manager': 'legacy', 'reason': 'synthetic'})
        monkeypatch.setattr(subject, 'inspect_pid', lambda *a, **k: {'status': 'unknown', 'pid': 23, 'error': 'synthetic inaccessible child'})
        monkeypatch.setattr(subject, '_CHILDREN', {})
        child = SimpleNamespace(pid=23, poll=lambda: None)
        def spawn(*args, **kwargs):
            """Assert that a durable reservation already exists at the spawn boundary."""
            assert subject._read_launch(tmp_path)['pid'] is None
            assert subject._launch_path(tmp_path).stat().st_mode & 0o777 == 0o600
            spawned.append(child)
            return child
        monkeypatch.setattr(subject.subprocess, 'Popen', spawn)
        with pytest.raises(subject.ProcessStateError, match='timed out'):
            subject.start(tmp_path, automatic=True)
        for correction in (20000, 50, 100000):
            wall[0] = correction
            # Even corrupt retry metadata and a full deadline cannot repair the launch fence.
            subject._retry_path(tmp_path).write_text('{broken')
            with subject.actions(tmp_path, 'test'):
                subject.retry_status(tmp_path)
            with pytest.raises(subject.ProcessStateError, match='unconfirmed'):
                subject.start(tmp_path, automatic=True)
            with pytest.raises(subject.ProcessStateError, match='unconfirmed'):
                with subject.manual_action(tmp_path, 'start'):
                    pytest.fail('Unconfirmed launch cannot authorize a manual retry')
        # A replacement controller has no parent-owned registry, but still sees the journal.
        monkeypatch.setattr(subject, '_CHILDREN', {})
        real_process = subject.psutil.Process
        monkeypatch.setattr(subject.psutil, 'Process', lambda pid=None: SimpleNamespace(status=lambda: psutil.STATUS_RUNNING, create_time=lambda: 123) if pid == 23 else real_process(pid))
        with pytest.raises(subject.ProcessStateError, match='unconfirmed'):
            subject.start(tmp_path, automatic=True)
        assert len(spawned) == 1

    @pytest.mark.parametrize('fault', ['reserved', 'corrupt', 'reused', 'denied', 'exited'])
    def test_journal_crash_windows_never_use_ttl(self, tmp_path, monkeypatch, fault):
        """Only confirmed exit releases a reservation; malformed data is never repaired."""
        launch = {'version': 1, 'token': 'a' * 32, 'pid': None if fault == 'reserved' else 23, 'create_time': None if fault == 'reserved' else 123}
        with subject.actions(tmp_path, 'test'):
            subject._write_launch(tmp_path, launch)
        monkeypatch.setattr(subject, '_CHILDREN', {})
        if fault == 'corrupt':
            subject._launch_path(tmp_path).write_text('{broken')
        before = subject._launch_path(tmp_path).read_bytes()
        def process(pid):
            """Represent only the requested synthetic pending PID."""
            if fault == 'denied':
                raise psutil.AccessDenied(pid)
            if fault == 'exited':
                raise psutil.NoSuchProcess(pid)
            return SimpleNamespace(create_time=lambda: 124, status=lambda: psutil.STATUS_RUNNING)
        monkeypatch.setattr(subject.psutil, 'Process', process)
        if fault == 'exited':
            subject._resolve_launch(tmp_path)
            assert subject._read_launch(tmp_path) is None
        else:
            with pytest.raises(subject.ProcessStateError):
                subject._resolve_launch(tmp_path)
            assert subject._launch_path(tmp_path).read_bytes() == before


def test_manual_success_preserves_retry_bytes(manual_view, monkeypatch):
    """A manual UNKNOWN start changes actual state but never grants automatic retry rights."""
    monkeypatch.setattr(subject, "registration_ready", lambda *a, **k: True)
    state = manual_view
    identity = ProcessIdentity(23, 123)
    now = time.time()
    with subject.actions(state.root, 'test'):
        subject._write_retry(state.root, {'version': 1, 'failures': 2, 'last_failure': now, 'next_retry': now + 120})
    original = subject._retry_path(state.root).read_bytes()
    monkeypatch.setattr(subject, 'FileLease', lambda *a: SimpleNamespace(release=lambda: None))
    monkeypatch.setattr(subject, '_CHILDREN', {})
    def spawn(*a, **kw):
        """Publish one exact synthetic child only after journal reservation."""
        assert subject._read_launch(state.root)['pid'] is None
        state.process_state = {'status': 'running', 'pid': 23, 'identity': identity}
        return SimpleNamespace(pid=23, poll=lambda: None)
    monkeypatch.setattr(subject.subprocess, 'Popen', spawn)
    monkeypatch.setattr(subject, 'inspect_pid', lambda *a: {'status': 'running', 'pid': 23, 'identity': identity})
    with subject.manual_action(state.root, 'start') as proof:
        subject.start(state.root, authorization=proof)
    assert subject._retry_path(state.root).read_bytes() == original
    assert subject._read_launch(state.root) is None
    assert subject.manager_state(state.root)['manager'] == 'unknown'


def test_launch_reservation_syncs_new_directory_ancestors_before_spawn(tmp_path, monkeypatch):
    """A new data/state directory must survive a crash along with the journal entry."""
    import stat
    directories = []
    original = subject.os.fsync
    def fsync(fd):
        """Record only the isolated temporary directory descriptors being synchronized."""
        if stat.S_ISDIR(os.fstat(fd).st_mode):
            directories.append(Path(os.readlink(f'/proc/self/fd/{fd}')))
        original(fd)
    monkeypatch.setattr(subject.os, 'fsync', fsync)
    monkeypatch.setattr(subject, 'manager_state', lambda root: {'manager': 'legacy', 'reason': 'synthetic'})
    monkeypatch.setattr(subject, 'snapshot', lambda root: {'status': 'stopped'})
    def spawn(*a, **kw):
        """Check the complete durability boundary without creating any child."""
        assert directories == [tmp_path / 'data/state', tmp_path / 'data', tmp_path]
        raise OSError('synthetic spawn failure after durable reservation')
    monkeypatch.setattr(subject.subprocess, 'Popen', spawn)
    with pytest.raises(OSError, match='after durable'):
        subject.start(tmp_path)
    assert subject._read_launch(tmp_path) is None


def test_manual_proof_cannot_cross_uid_or_namespace(manual_view, monkeypatch):
    """Request-local proof cannot survive changed kernel ownership evidence."""
    state, manager = manual_view, manual_view.module
    with subject.manual_action(state.root, 'start') as proof:
        original_uid = os.getuid()
        monkeypatch.setattr(subject.os, 'getuid', lambda: original_uid + 1)
        with pytest.raises(subject.ProcessStateError, match='action-specific'):
            subject._consume_authorization(state.root, 'start', proof)
        monkeypatch.setattr(subject.os, 'getuid', lambda: original_uid)
        original = manager._namespace
        monkeypatch.setattr(manager, '_namespace', lambda pid, kind, deadline: (2, 4) if kind == 'cgroup' else original(pid, kind, deadline))
        with pytest.raises(subject.ProcessStateError, match='namespace evidence changed'):
            subject._consume_authorization(state.root, 'start', proof)


@pytest.mark.parametrize('arguments,expected', [
    (['alias.py', 'PBCoinData.py'], 'alias.py'),
    (['-u', 'alias.py'], 'alias.py'),
    (['-I', '-X', 'dev', '-Wignore', '--', 'alias.py'], 'alias.py'),
    (['-Xdev', '--check-hash-based-pycs=always', 'alias.py'], 'alias.py'),
    (['-V'], None),
])
def test_manual_scan_uses_only_actual_python_script_operand(arguments, expected):
    """Alias matching must not confuse script payload arguments with the Python script."""
    import pbcoindata_manager as manager
    assert manager._python_script_argument(['python', *arguments], {'python'}) == expected


@pytest.mark.parametrize('arguments', [[], ['--'], ['--', '-'], ['-'], ['-i'], ['-m', 'PBCoinData'], ['-c', 'pass'], ['--unknown', 'alias.py'], ['-X', 'dev']])
def test_unverifiable_python_invocation_blocks_manual_start(arguments):
    """A missing script operand is diagnosed rather than treated as daemon absence."""
    import pbcoindata_manager as manager
    with pytest.raises(ValueError, match='Python'):
        manager._python_script_argument(['python', *arguments], {'python'})


@pytest.fixture
def foreign_services(manual_view, tmp_path_factory, monkeypatch):
    """Model a non-root host with inaccessible foreign sshd and Python system services."""
    state, manager = manual_view, manual_view.module
    state.actor_uid, state.foreign_uid = 1000, 0
    state.foreign_mode = 'unrelated'
    public = tmp_path_factory.mktemp('foreign-system') / 'networkd-dispatcher'
    public.write_text('# isolated unrelated system script\n')
    state.public_script = public
    # Exercise the installed psutil parser on synthetic procfs bytes. Rewritten
    # titles use spaces even when terminated by NUL; they are not one argv item.
    import psutil._pslinux as linux
    procfs = tmp_path_factory.mktemp('foreign-proc')
    (procfs / '800').mkdir()
    def parsed_cmdline(data):
        """Read only an isolated cmdline file through the real Linux psutil method."""
        (procfs / '800' / 'cmdline').write_bytes(data)
        process = linux.Process(800)
        process._procfs_path = str(procfs)
        return process.cmdline()
    state.sshd_argv = parsed_cmdline(b'sshd: /usr/sbin/sshd -D [listener]\x00')
    state.parse_cmdline = parsed_cmdline
    assert state.sshd_argv == ['sshd:', '/usr/sbin/sshd', '-D', '[listener]']
    state.native_argv = {
        'agetty': parsed_cmdline(b'/sbin/agetty\x00-o\x00-p -- \\u\x00--noclear\x00tty1\x00linux\x00'),
        'chronyd': parsed_cmdline(b'/usr/sbin/chronyd\x00-F\x001\x00'),
        'cron': parsed_cmdline(b'/usr/sbin/cron\x00-f\x00-P\x00'),
    }
    state.data['/proc/801/mountinfo'] = state.data['/proc/self/mountinfo']
    state.data['/proc/900/cgroup'] = '0::/user.slice/session.scope\n'
    monkeypatch.setattr(manager.os, 'getuid', lambda: state.actor_uid)
    monkeypatch.setattr(manager.psutil, 'pids', lambda: [800, 1 if state.foreign_mode == 'init' else 801])
    class Process:
        """Expose only conventional procfs fields; privileged executable/root reads are denied."""
        def __init__(self, pid=None):
            self.pid = pid
        def status(self):
            """Both foreign services are live."""
            return psutil.STATUS_RUNNING
        def uids(self):
            """Preserve a distinct root owner unless testing own-user access denial."""
            uid = state.actor_uid if state.foreign_mode == 'own-denied' else state.foreign_uid
            return SimpleNamespace(real=uid, effective=uid)
        def cmdline(self):
            """Match a rewritten sshd title and an ordinary absolute Python system script."""
            if self.pid == 800:
                return list(state.sshd_argv)
            if hasattr(state, 'argv_override'):
                return list(state.argv_override)
            if state.foreign_mode in state.native_argv:
                return list(state.native_argv[state.foreign_mode])
            if state.foreign_mode == 'native-options':
                return ['/usr/sbin/cron', '-f']
            if state.foreign_mode == 'native-truncated':
                return ['/usr/lib/systemd/systemd-journald']
            if state.foreign_mode in {'init', 'init-other-pid'}:
                return ['/sbin/init']
            if state.foreign_mode == 'opaque-title':
                return ['unconfirmed-title']
            script = state.public_script.name if state.foreign_mode == 'relative' else str(state.public_script)
            return ['py_alias' if state.foreign_mode.startswith('alias-') else 'python3', script]
        def name(self):
            """Return the kernel process name, independent of argv0."""
            if self.pid == 800:
                return 'sshd'
            if hasattr(state, 'name_override'):
                return state.name_override
            if state.foreign_mode in state.native_argv:
                return state.foreign_mode
            if state.foreign_mode == 'native-options':
                return 'cron'
            if state.foreign_mode == 'native-truncated':
                return 'systemd-journal'
            if state.foreign_mode in {'init', 'init-other-pid'}:
                return 'systemd'
            return 'py_alias' if state.foreign_mode.startswith('alias-') else 'python3'
        def exe(self):
            """A normal non-root caller cannot inspect these foreign executables."""
            if self.pid == 801 and (state.foreign_mode == 'alias-readable' or getattr(state, 'python_exe', False)):
                return '/usr/bin/python3'
            raise psutil.AccessDenied(self.pid)
        def create_time(self):
            """Supply synthetic lock metadata without inspecting a real process."""
            return 123.0
    monkeypatch.setattr(manager.psutil, 'Process', Process)
    original_stat = Path.stat
    def stat_path(path, **kwargs):
        """Deny only foreign proc-root/cwd; use temporary local files for inode evidence."""
        if str(path).startswith(('/proc/801/root/', '/proc/801/cwd/')):
            raise PermissionError('synthetic foreign procfs restriction')
        return original_stat(path, **kwargs)
    monkeypatch.setattr(Path, 'stat', stat_path)
    state.manager = 'unknown'
    monkeypatch.setattr(subject, 'manager_state', lambda root: {'manager': state.manager, 'reason': 'synthetic current manager'})
    return state


@pytest.mark.parametrize('manager_state', ['legacy', 'unknown'])
@pytest.mark.parametrize('launcher', ['unrelated', 'alias-readable', 'native-options', 'native-truncated', 'init', 'agetty', 'chronyd', 'cron'])
def test_nonroot_manual_start_allows_unrelated_foreign_services(foreign_services, manager_state, launcher):
    """An inaccessible sshd or positively mapped unrelated Python script is not a competitor."""
    state = foreign_services
    state.manager = manager_state
    state.foreign_mode = launcher
    with subject.manual_action(state.root, 'start') as proof:
        assert proof.route == 'manual'
        assert subject._consume_authorization(state.root, 'start', proof)['status'] == 'stopped'


@pytest.mark.parametrize('fault', ['checkout-alias', 'alias-denied', 'alias-readable-checkout', 'opaque-title', 'init-other-pid', 'mount-namespace', 'chroot', 'bind-root', 'unreadable-root', 'relative', 'own-denied', 'root-installation'])
def test_foreign_service_exclusion_requires_positive_mapping(foreign_services, monkeypatch, fault):
    """A known checkout reference stays blocking across foreign aliases and opaque identities."""
    state, manager = foreign_services, foreign_services.module
    _publish_conflicting_reference(state.root, 801)
    if fault in {'checkout-alias', 'alias-readable-checkout'}:
        state.public_script.unlink()
        state.public_script.symlink_to(state.root / 'PBCoinData.py')
        if fault == 'alias-readable-checkout':
            state.foreign_mode = 'alias-readable'
    elif fault == 'mount-namespace':
        original = manager._namespace_metadata
        monkeypatch.setattr(manager, '_namespace_metadata', lambda pid, kind, deadline: (2, 99) if pid == 801 else original(pid, kind, deadline))
    elif fault == 'chroot':
        state.data['/proc/801/mountinfo'] = state.data['/proc/801/mountinfo'].replace('0:1 / /', '0:1 /private /')
    elif fault == 'bind-root':
        rows = state.data['/proc/801/mountinfo'].splitlines()
        state.data['/proc/801/mountinfo'] = '\n'.join(
            '999 ' + row.split(' ', 1)[1] if row.split()[4] == '/' else row for row in rows
        ) + '\n'
    elif fault == 'unreadable-root':
        original = manager._read
        def read(path, deadline):
            """An inaccessible root mapping is not replaced by a path-name assumption."""
            if str(path) == '/proc/801/mountinfo':
                raise PermissionError('synthetic root mapping denial')
            return original(path, deadline)
        monkeypatch.setattr(manager, '_read', read)
    elif fault == 'root-installation':
        state.actor_uid = 0
    else:
        state.foreign_mode = fault
    with pytest.raises(subject.ProcessStateError):
        with subject.manual_action(state.root, 'start'):
            pytest.fail('An unconfirmed or actual competitor cannot be excluded')


@pytest.fixture
def restart_modules():
    """Import callers before synthetic foreign-process fields replace psutil discovery."""
    import api.services as services
    import starter
    return services, starter


@pytest.mark.parametrize('entry', ['api', 'starter'])
@pytest.mark.parametrize('launcher', ['unrelated', 'agetty', 'chronyd'])
def test_nonroot_manual_restart_finishes_running(restart_modules, foreign_services, monkeypatch, entry, launcher):
    """The ordinary foreign-service environment cannot strand PBCoinData after stop."""
    monkeypatch.setattr(subject, "registration_ready", lambda *a, **k: True)
    services, starter = restart_modules
    state = foreign_services
    state.foreign_mode = launcher
    (state.root / 'pbcoindata_guard.py').write_text('# synthetic entry helper\n')
    (state.root / 'secure_files.py').write_text('# synthetic file helper\n')
    state.manager = 'legacy'
    state.process_state = {'status': 'running', 'pid': 900, 'identity': ProcessIdentity(900, 123)}
    spawned = []
    monkeypatch.setattr(subject, 'FileLease', lambda *a: SimpleNamespace(release=lambda: None))
    monkeypatch.setattr(subject, '_CHILDREN', {})
    def terminate(root, identity, **kwargs):
        """Confirm only the synthetic owned target's exit."""
        assert identity == ProcessIdentity(900, 123)
        state.process_state = {'status': 'stopped', 'pid': None}
    def spawn(*a, **kw):
        """Publish one replacement without any daemon or provider execution."""
        spawned.append(901)
        state.process_state = {'status': 'running', 'pid': 901, 'identity': ProcessIdentity(901, 124)}
        return SimpleNamespace(pid=901, poll=lambda: None)
    monkeypatch.setattr(subject, '_terminate', terminate)
    monkeypatch.setattr(subject.subprocess, 'Popen', spawn)
    monkeypatch.setattr(subject, 'inspect_pid', lambda *a: dict(state.process_state))
    if entry == 'api':
        monkeypatch.setattr(services, 'PBGDIR', state.root)
        monkeypatch.setattr(services, '_systemd_enable_status', lambda *a: {})
        result = services._service_action('pbcoindata', 'restart')
        assert result['running'] is True and result['manager'] == 'legacy'
    else:
        monkeypatch.chdir(state.root)
        monkeypatch.setattr(sys, 'argv', ['starter.py', '--restart', 'PBCoinData'])
        starter.main()
    assert spawned == [901] and state.process_state['status'] == 'running'


@pytest.mark.parametrize('payload', [
    b'sshd: /usr/sbin/sshd -D [listener]\x00',
    b'sshd: /usr/sbin/sshd -D [listener]',
    b'sshd: /usr/sbin/sshd -D [listener] 0 of 10-100 startups\x00',
])
def test_installed_psutil_listener_title_is_not_a_python_candidate(foreign_services, payload):
    """Real psutil tokenization, including rewritten NUL titles, stays authorized."""
    state = foreign_services
    state.sshd_argv = state.parse_cmdline(payload)
    assert state.sshd_argv[0] == 'sshd:' and len(state.sshd_argv) >= 4
    with subject.manual_action(state.root, 'start') as proof:
        subject._consume_authorization(state.root, 'start', proof)


@pytest.mark.parametrize('arguments', [
    ['relative-script'], ['-mPBCoinData'], ['-m', 'module'], ['-cCODE'],
    ['-u'], ['-i'], ['-iu'], ['-OO'], ['-Xdev'], ['-Wignore'], ['-'], ['--', 'extensionless-script'],
    ['-P'], ['-bb'], ['-B'], ['-d'], ['-E'], ['-I'], ['-q'], ['-R'], ['-s'], ['-S'],
    ['-v'], ['-x'], ['--check-hash-based-pycs=default'],
])
def test_unknown_foreign_launcher_stays_opaque(foreign_services, arguments):
    """A concrete checkout reference blocks even when its process uses opaque argv."""
    state = foreign_services
    _publish_conflicting_reference(state.root, 801)
    state.argv_override, state.name_override = ['py_alias', *arguments], 'py_alias'
    with pytest.raises(subject.ProcessStateError):
        with subject.manual_action(state.root, 'start'):
            pytest.fail('Opaque foreign Python/script forms cannot authorize a start')


@pytest.mark.parametrize('fault', ['path-alias', 'python-exe', 'script-operand', 'checkout-operand', 'listener-payload'])
def test_native_profiles_do_not_hide_script_launchers(foreign_services, fault):
    """An ordinary service title cannot override a concrete conflicting checkout reference."""
    state = foreign_services
    _publish_conflicting_reference(state.root, 801)
    state.foreign_mode = 'agetty'
    if fault == 'path-alias':
        state.argv_override = ['/tmp/agetty', '-o', '-p -- \\u', '--noclear', 'tty1', 'linux']
    elif fault == 'python-exe':
        state.python_exe = True
    elif fault == 'script-operand':
        state.argv_override = ['/sbin/agetty', '-o', '-p -- \\u', '--noclear', 'tty1', 'alias.py']
    elif fault == 'checkout-operand':
        state.argv_override = ['/sbin/agetty', str(state.root / 'PBCoinData.py')]
    else:
        state.sshd_argv += ['unexpected-payload']
    with pytest.raises(subject.ProcessStateError):
        with subject.manual_action(state.root, 'start'):
            pytest.fail('A native profile must not hide script or contradictory evidence')


@pytest.mark.parametrize('payload', [
    b'sshd: mani [priv]\x00', b'sshd: mani [net]\x00', b'sshd: mani@pts/0\x00',
])
def test_installed_psutil_session_title_preserves_manual_start(foreign_services, payload):
    """Ordinary live SSH sessions use the real rewritten-title tokenizer too."""
    state = foreign_services
    state.sshd_argv = state.parse_cmdline(payload)
    assert state.sshd_argv[0] == 'sshd:'
    with subject.manual_action(state.root, 'start') as proof:
        subject._consume_authorization(state.root, 'start', proof)


def test_namespace_metadata_does_not_require_namespace_open(manual_view, monkeypatch):
    """Masked foreign symlink stats cannot authorize; manual proof never needs PID-1 namespace opens."""
    import pbcoindata_manager as manager
    monkeypatch.setattr(Path, 'stat', lambda *a, **k: SimpleNamespace(st_dev=2, st_ino=manager.PID_NS_INIT_INO))
    with pytest.raises(PermissionError):
        manager._namespace_metadata(1, 'pid', time.monotonic() + 5)
    assert manager._namespace_metadata('self', 'pid', time.monotonic() + 5) == (5, manager.PID_NS_INIT_INO)
    assert not manual_view.fds


def test_mount_descriptor_filesystem_type_is_kernel_observed(tmp_path):
    """Exercise the native fstatfs ABI on only our descriptor and an isolated ordinary file."""
    import pbcoindata_manager as manager
    with open('/proc/self/mountinfo', 'rb') as proc:
        manager._procfs_fd(proc.fileno())
    fake = tmp_path / 'fake-mountinfo'
    fake.write_text('1 0 0:1 / / rw - proc proc rw\n')
    with fake.open('rb') as ordinary:
        with pytest.raises(ValueError, match='not procfs'):
            manager._procfs_fd(ordinary.fileno())


def test_mixed_ubuntu_services_use_shared_ownership_not_native_profiles(foreign_services, monkeypatch):
    """Real psutil tokenization of a whole ordinary host must not deny explicit starts."""
    state, manager = foreign_services, foreign_services.module
    payloads = [b'/usr/sbin/rsyslogd\0-n\0-iNONE\0', b'/sbin/multipathd\0-d\0-s\0',
                b'/sbin/agetty\0-o\0-p -- \\u\0--keep-baud\0' + b'115200,38400,9600\0ttyS0\0vt220\0',
                b'@dbus-daemon\0--system\0--address=systemd:\0--nofork\0--nopidfile\0',
                b'/usr/bin/dockerd\0-H\0fd://\0', b'sshd: /usr/sbin/sshd -D [listener]\0',
                b'/usr/bin/python3\0-c\0pass\0', b'/usr/bin/python3\0']
    argv = [state.parse_cmdline(payload) for payload in payloads]
    assert argv[5] == ['sshd:', '/usr/sbin/sshd', '-D', '[listener]']
    class Process:
        """Ordinary root-owned services whose protected exe/cwd need not be opened."""
        def __init__(self, pid): self.pid = pid
        def status(self): return psutil.STATUS_RUNNING
        def cmdline(self): return argv[self.pid - 800]
        def uids(self): return SimpleNamespace(real=0, effective=0)
        def name(self): return Path(self.cmdline()[0]).name[:15]
        def exe(self): raise psutil.AccessDenied(self.pid)
    monkeypatch.setattr(manager.psutil, 'pids', lambda: list(range(800, 800 + len(argv))))
    monkeypatch.setattr(manager.psutil, 'Process', Process)
    with subject.manual_action(state.root, 'start') as proof:
        assert proof.route == 'manual'


class TestGuardProtocol:
    """Exercise real stdlib guards in isolated child processes, never a real daemon."""

    def test_stdlib_creation_time_matches_psutil(self):
        """Early registration uses exactly the process lifetime later verified by psutil."""
        import pbcoindata_guard as guard
        assert abs(guard.creation_time() - psutil.Process().create_time()) < 0.01

    def test_early_entry_library_import_and_import_failure(self, tmp_path):
        """The actual entry blocks before psutil; a library import remains guard-free."""
        import ast
        tree = ast.parse((ROOT / 'PBCoinData.py').read_text())
        early = ast.unparse(tree.body[0])
        # The import hook is installed before executing the actual unchanged entry.
        program = f'''import sys, importlib.abc
sys.path.insert(0, {str(ROOT)!r})
class Hook(importlib.abc.MetaPathFinder):
 def find_spec(self, fullname, path=None, target=None):
  if fullname == 'psutil':
   from pbcoindata_guard import FileLease
   try:
    FileLease('.', 'singleton', 'probe')
   except BlockingIOError:
    print('guard-before-psutil', flush=True)
   else:
    raise RuntimeError('guard not held')
   raise ImportError('synthetic import failure')
sys.meta_path.insert(0, Hook())
exec({early!r}, {{'__name__': '__main__'}})
import psutil
'''
        result = subprocess.run([sys.executable, '-c', program], cwd=tmp_path, capture_output=True, text=True, timeout=10)
        assert result.returncode != 0 and 'guard-before-psutil' in result.stdout
        lease = subject.FileLease(tmp_path, 'singleton', 'after-exit')
        lease.release()
        library = subprocess.run([sys.executable, '-c', f"import sys;sys.path.insert(0,{str(ROOT)!r});import PBCoinData;from pbcoindata_guard import FileLease;FileLease('.', 'singleton', 'library-probe').release()"], cwd=tmp_path, capture_output=True, text=True, timeout=20)
        assert library.returncode == 0, library.stderr

    def test_guard_survives_main_return_and_fork_child_closes_copy(self, tmp_path):
        """Only OS exit frees a parent guard; a fork child cannot retain or unlock it."""
        program = f'''import sys, os, time
sys.path.insert(0,{str(ROOT)!r})
from pbcoindata_guard import acquire_daemon, _LIFETIME_GUARDS
acquire_daemon('.')
child = os.fork()
if child == 0:
 assert not _LIFETIME_GUARDS
 time.sleep(3)
 os._exit(0)
def main(): pass
main()
print(child, flush=True)
sys.stdin.readline()
os._exit(0)
'''
        child = subprocess.Popen([sys.executable, '-c', program], cwd=tmp_path, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            fork_pid = int(child.stdout.readline())
            with pytest.raises(BlockingIOError):
                subject.FileLease(tmp_path, 'singleton', 'parent-still-alive')
            child.stdin.write('exit\n'); child.stdin.flush(); child.wait(timeout=5)
            # The fork child remains alive, but no longer owns the parent's descriptor.
            lease = subject.FileLease(tmp_path, 'singleton', 'after-parent-exit')
            lease.release()
        finally:
            if child.poll() is None: child.kill()
            child.communicate(timeout=5)

    def test_old_new_lease_same_inode_and_concurrent_direct_start(self, tmp_path):
        """The former FileLease algorithm and the new early guard mutually exclude."""
        import fcntl
        from pbcoindata_guard import acquire_daemon, _LIFETIME_GUARDS
        old = subject.FileLease(tmp_path, 'singleton', 'old-daemon')
        inode = os.fstat(old.fd).st_ino
        try:
            with pytest.raises(BlockingIOError): acquire_daemon(tmp_path)
        finally:
            old.release()
        guard = acquire_daemon(tmp_path)
        try:
            assert os.fstat(guard.fd).st_ino == inode
            assert not os.get_inheritable(guard.fd)
            with pytest.raises(BlockingIOError): subject.FileLease(tmp_path, 'singleton', 'old-probe')
            result = subprocess.run([sys.executable, '-c', f"import sys;sys.path.insert(0,{str(ROOT)!r});from pbcoindata_guard import acquire_daemon;acquire_daemon('.');raise AssertionError('second initialization')"], cwd=tmp_path, capture_output=True, timeout=10)
            assert result.returncode != 0 and b'BlockingIOError' in result.stderr
        finally:
            guard.release(); _LIFETIME_GUARDS.remove(guard)

    def test_transition_is_separate_and_pending_clear_preserves_it(self, tmp_path, monkeypatch):
        """Neither ready nor v1 data certifies migration; launch resolution retains evidence."""
        import pbcoindata_guard as guard
        (tmp_path / 'PBCoinData.py').write_text('# synthetic\n')
        lease = guard.acquire_daemon(tmp_path)
        try:
            guard.publish(lease, 'ready')
            with pytest.raises(subject.ProcessStateError, match='transition'):
                subject.transition_status(tmp_path)
        finally:
            lease.release(); guard._LIFETIME_GUARDS.remove(lease)
        launch = {'version': 1, 'token': 'b' * 32, 'pid': None, 'create_time': None}
        subject._write_launch(tmp_path, launch)
        with pytest.raises(subject.ProcessStateError): subject.transition_status(tmp_path)
        doc = guard.read_journal(tmp_path)
        transition = {**guard.scope(tmp_path), 'basis': 'legacy-cleanup', 'autostarts_updated': True, 'controllers_restarted': True, 'confirmed_at': 1000.0}
        doc['transition'] = transition
        guard.write_journal(tmp_path, doc)
        subject._clear_launch(tmp_path, launch['token'])
        assert guard.read_journal(tmp_path)['transition'] == transition
        assert subject._read_launch(tmp_path) is None
        assert subject.transition_status(tmp_path) == transition

    def test_guarded_popen_ready_and_direct_reservation_conflict(self, tmp_path, monkeypatch):
        """A launched child passes only its own reservation and publishes matching readiness."""
        script = f'''import sys, signal, threading
sys.path.insert(0,{str(ROOT)!r})
from pbcoindata_guard import acquire_daemon
lease = acquire_daemon('.')
from pbcoindata_process import register_current, clear_owned_files
stop = threading.Event()
signal.signal(signal.SIGTERM, lambda *a: stop.set())
identity = register_current('.')
try: stop.wait(20)
finally: clear_owned_files('.',identity)
'''
        (tmp_path / 'PBCoinData.py').write_text(script)
        monkeypatch.setattr(subject, 'manager_state', lambda root: {'manager': 'legacy', 'reason': 'synthetic'})
        try:
            subject.start(tmp_path)
            state = subject.snapshot(tmp_path)
            assert state['status'] == 'running'
            assert subject.registration_ready(tmp_path, state['identity'])
            assert subject._read_launch(tmp_path) is None
            with pytest.raises(subject.ProcessStateError): subject.transition_status(tmp_path)
            # The offline runner hides the outer cgroup namespace. Ownership
            # parsing has its own regressions; supply only that environment proof.
            import pbcoindata_manager as manager
            monkeypatch.setattr(manager,'_target_groups',lambda *a:'/isolated-test.scope')
            subject.stop(tmp_path)
            assert subject.snapshot(tmp_path)['status'] == 'stopped'
        finally:
            for child in list(subject._CHILDREN.values()):
                if child.poll() is None: child.kill()
                child.wait(timeout=5)
            subject.reap_children()
        subject._write_launch(tmp_path, {'version': 1, 'token': 'a'*32, 'pid': None, 'create_time': None})
        result = subprocess.run([sys.executable, str(tmp_path / 'PBCoinData.py')], cwd=tmp_path, capture_output=True, text=True, timeout=10)
        assert result.returncode != 0 and 'Unresolved PBCoinData launch' in result.stderr


@pytest.mark.parametrize('parent', [None, {}, {'pid': True, 'create_time': 123}, {'pid': 23, 'create_time': float('nan')}, {'pid': 23, 'create_time': '123'}, {'pid': 23, 'create_time': -1}])
def test_corrupt_or_missing_parent_cannot_authorize_early_launch(tmp_path, parent):
    """Malformed JSON identities cannot exploit floating-point comparisons at the early entry."""
    import pbcoindata_guard as guard
    launch = {'version': 2, 'token': 'd'*32, 'pid': None, 'create_time': None, 'parent': parent}
    path = guard.journal_path(tmp_path)
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({'version': 2, 'transition': None, 'pending_launch': launch}))
    before = path.read_bytes()
    result = subprocess.run([sys.executable, '-c', f"import sys;sys.path.insert(0,{str(ROOT)!r});from pbcoindata_guard import acquire_daemon;acquire_daemon('.')"], cwd=tmp_path, capture_output=True, timeout=10)
    assert result.returncode != 0 and path.read_bytes() == before


def test_missing_transition_limits_idle_not_manual_authorization(manual_view, monkeypatch):
    """Operator transition is a recovery prerequisite, never a manual override flag."""
    state = manual_view
    with subject.manual_action(state.root, 'start') as proof:
        assert subject._consume_authorization(state.root, 'start', proof)['status'] == 'stopped'
    monkeypatch.setattr(subject, 'manager_state', lambda root: {'manager': 'legacy'})
    with pytest.raises(subject.ProcessStateError, match='transition'):
        subject.transition_status(state.root)


@pytest.mark.parametrize('basis', ['legacy-cleanup', 'clean-boot'])
def test_operator_transition_is_explicit_local_and_scope_bound(tmp_path, monkeypatch, basis):
    """Only the dedicated operator command records an attested completed transition."""
    import starter
    import pbcoindata_guard as guard
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, 'argv', ['starter.py', '--confirm-coindata-guard-transition', '--transition-basis', basis, 'PBCoinData'])
    monkeypatch.setattr(subject.subprocess, 'Popen', lambda *a, **k: pytest.fail('Confirmation must not start anything'))
    starter.main()
    evidence = subject.transition_status(tmp_path)
    assert evidence['basis'] == basis and evidence['uid'] == os.getuid()
    assert evidence['autostarts_updated'] and evidence['controllers_restarted']
    before = subject._launch_path(tmp_path).read_bytes()
    lease = subject.FileLease(tmp_path, 'singleton', 'existing-owner')
    try:
        with pytest.raises(subject.ProcessStateError): subject.confirm_guard_transition(tmp_path, basis)
    finally: lease.release()
    assert subject._launch_path(tmp_path).read_bytes() == before
    moved = tmp_path / 'another-checkout'
    moved.mkdir()
    destination = guard.journal_path(moved)
    destination.parent.mkdir(parents=True)
    destination.write_bytes(before)
    with pytest.raises(subject.ProcessStateError, match='transition'): subject.transition_status(moved)


class TestRetiredPublication:
    """Old publication lifetimes never grant signals to a recycled PID owner."""

    @pytest.mark.parametrize('entry', ['snapshot', 'direct', 'start'])
    @pytest.mark.parametrize('history', ['reused', 'previous-boot', 'numeric-only'])
    def test_recycled_foreign_pid_allows_safe_new_start(self, tmp_path, monkeypatch, entry, history):
        """A private sleep child models PID reuse; only temporary publications are changed."""
        import pbcoindata_guard as guard
        child = subprocess.Popen(['/bin/sleep', '30'], cwd=tmp_path)
        held = None
        try:
            current = psutil.Process(child.pid).create_time()
            path = subject.pid_path(tmp_path)
            path.parent.mkdir(parents=True)
            path.write_text(str(child.pid))
            if history != 'numeric-only':
                old = current - 100 if history == 'reused' else current
                write_process_identity(path, ProcessIdentity(child.pid, old))
                registration = {**guard.scope(tmp_path), **guard.runtime_scope(), 'pid': child.pid, 'create_time': old,
                                'phase': 'ready', 'guard_device': 1, 'guard_inode': 1, 'launch_token': None}
                if history == 'previous-boot': registration['boot_id'] = '00000000-0000-4000-8000-000000000001'
                guard.registration_path(tmp_path).write_text(json.dumps(registration))
            if entry == 'snapshot':
                assert subject.snapshot(tmp_path)['status'] == 'stopped'
            elif entry == 'direct':
                held = guard.acquire_daemon(tmp_path)
                assert held.fd is not None
            else:
                (tmp_path / 'PBCoinData.py').write_text(f"import sys,signal,threading\nsys.path.insert(0,{str(ROOT)!r})\nfrom pbcoindata_guard import acquire_daemon\nguard=acquire_daemon('.')\nfrom pbcoindata_process import register_current,clear_owned_files\nevent=threading.Event()\nsignal.signal(signal.SIGTERM,lambda *a:event.set())\nidentity=register_current('.')\ntry:event.wait(20)\nfinally:clear_owned_files('.',identity)\n")
                monkeypatch.setattr(subject, 'manager_state', lambda root: {'manager': 'legacy'})
                subject.start(tmp_path)
                assert subject.snapshot(tmp_path)['status'] == 'running'
                # This test owns the real child but models initial-namespace visibility;
                # the execution sandbox intentionally hides the outer cgroup namespace.
                import pbcoindata_manager as manager
                monkeypatch.setattr(manager, '_target_groups', lambda *a: '/isolated-test.scope')
                subject.stop(tmp_path)
            assert child.poll() is None
        finally:
            if held is not None:
                held.release(); guard._LIFETIME_GUARDS.remove(held)
            child.terminate(); child.wait(timeout=5)
            for spawned in list(subject._CHILDREN.values()):
                if spawned.poll() is None: spawned.kill()
                spawned.wait(timeout=5)
            subject.reap_children()


def test_active_start_without_transition_preserves_existing_healing(tmp_path, monkeypatch):
    """Missing transition limits voluntary IDLE unloading, not ordinary ACTIVE repair."""
    calls = []
    monkeypatch.setattr(subject, 'manager_state', lambda root: {'manager': 'legacy'})
    def spawn(*a, **k):
        """Reach the actual spawn boundary without creating a daemon."""
        calls.append('spawn')
        raise OSError('synthetic spawn reached')
    monkeypatch.setattr(subject.subprocess, 'Popen', spawn)
    with pytest.raises(OSError, match='spawn reached'):
        subject.start(tmp_path, automatic=True)
    assert calls == ['spawn']


@pytest.mark.parametrize('fault', ['same-lifetime', 'denied', 'nan', 'uid', 'namespace', 'checkout', 'contradiction', 'malformed'])
def test_uncertain_historical_publications_remain_blocking(tmp_path, monkeypatch, fault):
    """Only confirmed retired lifetimes are recoverable; no metadata or foreign process is changed."""
    import pbcoindata_guard as guard
    child = subprocess.Popen(['/bin/sleep', '30'], cwd=tmp_path)
    held_before = list(guard._LIFETIME_GUARDS)
    try:
        created = psutil.Process(child.pid).create_time()
        path = subject.pid_path(tmp_path); path.parent.mkdir(parents=True)
        path.write_text(str(child.pid))
        write_process_identity(path, ProcessIdentity(child.pid, created))
        value = {**guard.scope(tmp_path), **guard.runtime_scope(), 'pid': child.pid, 'create_time': created,
                 'phase': 'ready', 'guard_device': 1, 'guard_inode': 1, 'launch_token': None}
        if fault == 'nan': value['create_time'] = float('nan')
        if fault == 'uid': value['uid'] += 1
        if fault == 'checkout': value['checkout'] += '-other'
        if fault == 'namespace': value['namespaces']['pid'] = 'pid:[123]'
        if fault == 'contradiction': value['create_time'] -= 100
        guard.registration_path(tmp_path).write_text('{broken' if fault == 'malformed' else json.dumps(value))
        before = {p:p.read_bytes() for p in path.parent.iterdir()}
        if fault == 'denied':
            original = Path.read_text
            def read(target, *a, **k):
                """Deny only the referenced child stat, not unrelated host processes."""
                if target == Path(f'/proc/{child.pid}/stat'): raise PermissionError('synthetic stat denial')
                return original(target, *a, **k)
            monkeypatch.setattr(Path, 'read_text', read)
        assert subject.snapshot(tmp_path)['status'] in {'unknown', 'foreign'}
        with pytest.raises((RuntimeError,ValueError,OSError)): guard.acquire_daemon(tmp_path)
        assert child.poll() is None
        assert all(p.read_bytes() == data for p,data in before.items())
    finally:
        child.terminate();child.wait(timeout=5)
        for held in list(guard._LIFETIME_GUARDS):
            if held not in held_before:
                held.release();guard._LIFETIME_GUARDS.remove(held)


def test_boot_retirement_cannot_release_pending_launch(tmp_path):
    """Even a valid old-boot publication cannot turn a pid-less launch fence into absence."""
    import pbcoindata_guard as guard
    child = subprocess.Popen(['/bin/sleep', '30'], cwd=tmp_path)
    held_before = list(guard._LIFETIME_GUARDS)
    try:
        path = subject.pid_path(tmp_path);path.parent.mkdir(parents=True)
        path.write_text(str(child.pid))
        created = psutil.Process(child.pid).create_time()
        value = {**guard.scope(tmp_path), **guard.runtime_scope(), 'pid': child.pid, 'create_time': created,
                 'phase':'ready','boot_id':'00000000-0000-4000-8000-000000000001'}
        guard.registration_path(tmp_path).write_text(json.dumps(value))
        subject._write_launch(tmp_path, {'version':1,'token':'e'*32,'pid':None,'create_time':None})
        before = subject._launch_path(tmp_path).read_bytes()
        assert subject.snapshot(tmp_path)['status'] == 'unknown'
        with pytest.raises(RuntimeError,match='Unresolved'): guard.acquire_daemon(tmp_path)
        assert subject._launch_path(tmp_path).read_bytes() == before
    finally:
        child.terminate();child.wait(timeout=5)
        for held in list(guard._LIFETIME_GUARDS):
            if held not in held_before: held.release();guard._LIFETIME_GUARDS.remove(held)


@pytest.mark.parametrize('fault', ['alias', 'hardlink', 'unreadable'])
def test_numeric_only_does_not_hide_checkout_alias_or_denial(tmp_path, monkeypatch, fault):
    """A free guard and a noncanonical argv0 are never enough to exclude a referenced daemon."""
    import pbcoindata_guard as guard
    script = tmp_path/'PBCoinData.py';script.write_text('import time\ntime.sleep(30)\n')
    alias = tmp_path/'other.py'
    if fault == 'hardlink': os.link(script, alias)
    else: alias.symlink_to(script)
    child = subprocess.Popen([sys.executable,str(alias)],cwd=tmp_path) if fault in {'alias','hardlink'} else subprocess.Popen(['/bin/sleep','30'],cwd=tmp_path)
    held_before = list(guard._LIFETIME_GUARDS)
    try:
        path=subject.pid_path(tmp_path);path.parent.mkdir(parents=True);path.write_text(str(child.pid))
        if fault=='unreadable':
            original = Path.read_bytes
            def read(target):
                """Deny only the known referenced command line."""
                if target==Path(f'/proc/{child.pid}/cmdline'):raise PermissionError('synthetic denial')
                return original(target)
            monkeypatch.setattr(Path,'read_bytes',read)
        with pytest.raises(RuntimeError):guard.acquire_daemon(tmp_path)
        assert path.read_text()==str(child.pid) and child.poll() is None
    finally:
        child.terminate();child.wait(timeout=5)
        for held in list(guard._LIFETIME_GUARDS):
            if held not in held_before:held.release();guard._LIFETIME_GUARDS.remove(held)


def test_new_publication_is_preserved_when_registration_check_races(tmp_path, monkeypatch):
    """A changed reference is never deleted on the basis of an earlier retired observation."""
    import pbcoindata_guard as guard
    path = subject.pid_path(tmp_path);path.parent.mkdir(parents=True)
    original = guard._check_existing_publications
    def changed(root):
        """Insert a newer publication after initial absence was checked."""
        files = original(root)
        path.write_text(str(os.getpid()))
        return files
    monkeypatch.setattr(guard,'_check_existing_publications',changed)
    before = list(guard._LIFETIME_GUARDS)
    try:
        with pytest.raises(RuntimeError,match='changed'): guard.acquire_daemon(tmp_path)
        assert path.read_text()==str(os.getpid())
    finally:
        for held in list(guard._LIFETIME_GUARDS):
            if held not in before:held.release();guard._LIFETIME_GUARDS.remove(held)


def test_post_exit_cleanup_cannot_unlink_a_new_direct_publication(tmp_path, monkeypatch):
    """A barrier after the old numeric read cannot let a direct start replace then lose its files."""
    import threading
    path=subject.pid_path(tmp_path);path.parent.mkdir(parents=True)
    old=ProcessIdentity(23,100);new=ProcessIdentity(24,200)
    path.write_text('23');write_process_identity(path,old)
    original=Path.read_text
    calls=[]
    def compete():
        """Model a supported direct entry trying to publish while cleanup is in progress."""
        try: lease=subject.FileLease(tmp_path,'singleton','new-daemon')
        except BlockingIOError:
            calls.append('blocked');return
        try:
            calls.append('published')
            path.write_text('24');write_process_identity(path,new)
        finally:lease.release()
    def read(target,*a,**k):
        """Release the competing entry exactly after the stale numeric data was read."""
        value=original(target,*a,**k)
        if target==path:
            thread=threading.Thread(target=compete);thread.start();thread.join(timeout=2)
            assert not thread.is_alive()
        return value
    monkeypatch.setattr(Path,'read_text',read)
    subject.clear_owned_files(tmp_path,old)
    assert calls==['blocked']
    monkeypatch.setattr(Path,'read_text',original)
    current=subject.FileLease(tmp_path,'singleton','new-daemon')
    try:
        path.write_text('24');write_process_identity(path,new)
        before={p:p.read_bytes() for p in (path,identity_path(path))}
        subject.clear_owned_files(tmp_path,old)
        assert all(p.read_bytes()==value for p,value in before.items())
    finally:current.release()


def test_boot_and_namespace_conflict_cannot_be_retired(tmp_path):
    """A local bind mount does not certify an old boot's namespace was this controller's domain."""
    import pbcoindata_guard as guard
    child=subprocess.Popen(['/bin/sleep','30'],cwd=tmp_path)
    try:
        record={**guard.runtime_scope(),'kind':'registration','pid':child.pid,'create_time':1.0}
        record['boot_id']='00000000-0000-4000-8000-000000000001'
        record['namespaces']['pid']='pid:[123]'
        assert guard.publication_state(tmp_path,record)=='unknown'
    finally:child.terminate();child.wait(timeout=5)

@pytest.fixture
def parent_cgroup_target(manual_view, monkeypatch):
    """Supply complete isolated target/guard/unit evidence without host queries or signals."""
    state=manual_view
    state.process_state={'status':'running','pid':42,'identity':ProcessIdentity(42,123)}
    monkeypatch.setattr(subject,'inspect_pid',lambda *a,**k:dict(state.process_state))
    monkeypatch.setattr(subject,'registration_ready',lambda *a,**k:True)
    state.unit={'Id':'parent.service','LoadState':'loaded','ActiveState':'active','SubState':'running','MainPID':'99','ControlGroup':'/user.slice/user@1000.service/app.slice/parent.service','Job':'','Result':'success','ExecStart':'','WorkingDirectory':str(state.root),'StartLimitIntervalUSec':'10s','StartLimitBurst':'5'}
    state.parent_script='PBRun.py'
    state.parent_reuse=False
    state.parent_reads=0
    state.orphan=False
    process_type=state.module.psutil.Process
    class Ancestry(process_type):
        """A verified live PBRun parent, with a wrapper negative variant."""
        def __init__(self,pid):
            self.pid=pid
            if pid==99:state.parent_reads+=1
            self.created=123.0 if pid==42 else 456.0 + (state.parent_reuse and pid==99 and state.parent_reads>1)
        def ppid(self):
            """Only the unit MainPID is the target's parent."""
            return 99 if self.pid==42 and not state.orphan else 1
        def create_time(self):
            """Bind each observation to the same parent lifetime."""
            return self.created
        def cmdline(self):
            """Use the supported parent script or an explicit unrelated wrapper."""
            if self.pid==42:return ['python',str(state.root/'PBCoinData.py')]
            return ['python',str(state.root/state.parent_script)] if state.parent_script else ['bash','-c','python PBCoinData.py & wait']
        def exe(self):
            """Resolve Python from kernel executable evidence."""
            return '/synthetic/python3.12' if state.parent_script else '/bin/bash'
        def cwd(self):
            """The controller uses this exact synthetic checkout."""
            return str(state.root)
    monkeypatch.setattr(state.module.psutil,'Process',Ancestry)
    state.queries=[]
    state.real_query_unit=state.module._query_unit
    def query(unit,deadline,*,user=True):
        """Return fresh concrete metadata only for the unit being queried."""
        state.queries.append((unit,user))
        if unit.startswith('user@'):
            return {**state.unit,'Id':unit,'MainPID':'99','ControlGroup':'/user.slice/'+unit}
        return {**state.unit,'Id':unit}
    monkeypatch.setattr(state.module,'_query_unit',query,raising=False)
    return state


@pytest.mark.parametrize('entry',['terminal','pbrun'])
@pytest.mark.parametrize('hierarchy',['v1','v2'])
def test_normal_parent_service_does_not_own_guarded_legacy_child(parent_cgroup_target,entry,hierarchy):
    """An ancestor user manager or PBRun unit is not ownership of its exact legacy child."""
    state=parent_cgroup_target
    base='/user.slice/user@1000.service'
    group=base+'/app.slice/app-terminal.scope' if entry=='terminal' else base+'/app.slice/pbgui-pbrun.service'
    state.unit['ControlGroup']=base if entry=='terminal' else group
    state.data['/proc/42/cgroup']=('0::'+group+'\n' if hierarchy=='v2' else '2:cpu,cpuacct:'+group+'\n1:name=systemd:'+group+'\n')
    with subject.manual_action(state.root,'stop') as proof:
        assert subject._consume_authorization(state.root,'stop',proof)['pid']==42
    assert state.queries


@pytest.mark.parametrize('text,expected',[
 ('0::/user.slice/user@1000.service/app.slice/app-terminal.scope\n','/user.slice/user@1000.service/app.slice/app-terminal.scope'),
 ('2:cpu,cpuacct:/x\n1:name=systemd:/x\n','/x'),
 ('0::/x\n2:cpu,cpuacct:/x\n','/x'),
 ('1:cpu,cpuacct:/x\n',None),('0::/x\n0::/y\n',None),('0::/x\n1:name=systemd:/y\n',None),
 ('1:cpu,cpu:/x\n2:name=systemd:/x\n',None),('1:cpu:/x\n2:cpu:/x\n3:name=systemd:/x\n',None),
 ('0::/x/../y\n',None),('0::/x//y\n',None),('0::relative\n',None),('bad',None),
 ('1:name=systemd:/a.service\n2:cpu,cpuacct:/b.service\n',None)])
def test_cgroup_hierarchies_are_structural(text,expected):
    """Controller lists, hierarchy ambiguity and canonical path boundaries are explicit."""
    import pbcoindata_manager as manager
    if expected is None:
        with pytest.raises(ValueError):manager.cgroup_membership(text)
    else:assert manager.cgroup_membership(text)==expected


@pytest.mark.parametrize('fault',['mainpid','pbcoindata-child','controlgroup','no-guard','namespace','identity','changed-assignment','wrapper','parent-reuse','orphan'])
def test_parent_service_does_not_bypass_target_safety(parent_cgroup_target,monkeypatch,fault):
    """MainPID inequality alone never licenses signalling an unverified service child."""
    state=parent_cgroup_target
    group='/user.slice/user@1000.service/app.slice/parent.service'
    state.data['/proc/42/cgroup']='0::'+group+'\n'
    if fault=='mainpid':state.unit['MainPID']='42'
    if fault=='pbcoindata-child':state.data['/proc/42/cgroup']='0::/user.slice/'+subject.UNIT+'/child\n'
    if fault=='controlgroup':state.unit['ControlGroup']=group+'-similar'
    if fault=='no-guard':monkeypatch.setattr(subject,'registration_ready',lambda *a:False)
    if fault=='namespace':monkeypatch.setattr(state.module,'_namespace',lambda *a:(1,99))
    if fault=='identity':monkeypatch.setattr(subject,'inspect_pid',lambda *a,**k:{'status':'unknown'})
    if fault=='wrapper':state.parent_script=None
    if fault=='parent-reuse':state.parent_reuse=True
    if fault=='orphan':state.orphan=True
    if fault=='changed-assignment':
        with subject.manual_action(state.root,'stop') as proof:
            state.unit['MainPID']='100'
            with pytest.raises(subject.ProcessStateError):subject._consume_authorization(state.root,'stop',proof)
    else:
        with pytest.raises(subject.ProcessStateError):
            with subject.manual_action(state.root,'stop'):pytest.fail('Unsafe target accepted')


def test_manual_assignment_is_rechecked_before_escalation(parent_cgroup_target,monkeypatch):
    """A unit ownership change after TERM invalidates authorization before KILL."""
    state=parent_cgroup_target
    state.data['/proc/42/cgroup']='0::/user.slice/user@1000.service/app.slice/parent.service\n'
    signals=[]
    monkeypatch.setattr(subject,'signal_exact_process',lambda identity,sig:signals.append(sig))
    def wait(*a):
        """Model a timed-out TERM and subsequent takeover of exactly this PID by its unit."""
        state.unit['MainPID']='42';return False
    monkeypatch.setattr(subject,'_wait_exit',wait)
    with subject.manual_action(state.root,'stop') as proof:
        with pytest.raises(subject.ProcessStateError):subject._terminate(state.root,state.process_state['identity'],authorization=proof)
    assert signals==[signal.SIGTERM]


@pytest.mark.parametrize('remaining',[5.0,0.25])
def test_unit_queries_use_validated_argv_and_remaining_budget(parent_cgroup_target,monkeypatch,remaining):
    """One unit query cannot exceed three seconds or the shared remaining budget."""
    import pbcoindata_manager as manager
    # Use the production reader rather than this fixture's ownership double.
    from importlib.util import module_from_spec,spec_from_file_location
    spec=spec_from_file_location('isolated_cgroup_reader',ROOT/'pbcoindata_manager.py')
    reader=module_from_spec(spec)
    monkeypatch.setitem(sys.modules,spec.name,reader)
    spec.loader.exec_module(reader)
    clock=[100.0];calls=[]
    monkeypatch.setattr(reader.time,'monotonic',lambda:clock[0])
    def run(argv,**kw):
        """Return complete isolated unit properties; capture argv and timeout."""
        calls.append((argv,kw['timeout']))
        return SimpleNamespace(returncode=0,stdout='\n'.join(k+'='+v for k,v in parent_cgroup_target.unit.items()))
    monkeypatch.setattr(reader.subprocess,'run',run)
    reader._query_unit('parent.service',100+remaining)
    assert calls[0][1]==min(3,remaining)
    assert calls[0][0][:4]==['systemctl','--user','show','parent.service']
    with pytest.raises(ValueError):reader._query_unit('--bad.service',105)
    clock[0]=106
    with pytest.raises(TimeoutError):reader._query_unit('parent.service',105)
    assert len(calls)==1


def test_unit_query_time_cannot_overrun_shared_budget(parent_cgroup_target,monkeypatch):
    """A completed command after the deadline provides no usable evidence."""
    from importlib.util import module_from_spec,spec_from_file_location
    spec=spec_from_file_location('isolated_late_cgroup_reader',ROOT/'pbcoindata_manager.py')
    reader=module_from_spec(spec)
    monkeypatch.setitem(sys.modules,spec.name,reader)
    spec.loader.exec_module(reader)
    clock=[100.0];monkeypatch.setattr(reader.time,'monotonic',lambda:clock[0])
    def run(*a,**kw):
        """Consume the full remaining synthetic evidence interval."""
        clock[0]=106
        return SimpleNamespace(returncode=0,stdout='\n'.join(k+'='+v for k,v in parent_cgroup_target.unit.items()))
    monkeypatch.setattr(reader.subprocess,'run',run)
    with pytest.raises(TimeoutError):reader._query_unit('parent.service',105)


def test_automatic_escalation_rejects_changed_valid_assignment(owned,monkeypatch):
    """A second positive but different unit assignment never authorizes KILL."""
    import pbcoindata_manager as manager
    assignments=iter([('/scope-a',()),('/scope-b',())])
    monkeypatch.setattr(manager,'_manual_target',lambda *a,**k:next(assignments))
    monkeypatch.setattr(subject,'_wait_exit',lambda *a:False)
    monkeypatch.setattr(subject,'signal_exact_process',lambda identity,sig:owned.signals.append(sig))
    with pytest.raises(subject.ProcessStateError,match='assignment changed'):
        subject._terminate(owned.root,ProcessIdentity(23,123),check_assignment=True)
    assert owned.signals==[signal.SIGTERM]


@pytest.mark.parametrize('fault',['identity','assignment'])
def test_manual_signal_rechecks_first_authorized_binding(parent_cgroup_target,monkeypatch,fault):
    """Changes after authorization are refused before even the first signal."""
    state=parent_cgroup_target
    state.data['/proc/42/cgroup']='0::/user.slice/user@1000.service/app.slice/parent.service\n'
    monkeypatch.setattr(subject,'signal_exact_process',lambda *a:pytest.fail('No signal after a change'))
    with subject.manual_action(state.root,'stop') as proof:
        if fault=='identity':state.process_state['identity']=ProcessIdentity(42,124)
        else:state.unit['MainPID']='100'
        with pytest.raises(subject.ProcessStateError):
            subject._terminate(state.root,ProcessIdentity(42,123),authorization=proof)


@pytest.mark.parametrize('fault',['parent-reuse','first-edge'])
def test_complete_legacy_ancestry_is_rechecked(parent_cgroup_target,monkeypatch,fault):
    """A previously supported parent cannot certify a later replacement/reparented chain."""
    state=parent_cgroup_target
    state.unit['MainPID']='100'
    state.data['/proc/42/cgroup']='0::/user.slice/user@1000.service/app.slice/parent.service\n'
    base=state.module.psutil.Process
    changed=[False]
    class Chain(base):
        """Cache CT per object and replace an earlier edge while inspecting the next parent."""
        def __init__(self,pid):
            self.pid=pid
            if pid==100:changed[0]=True
            self.created={42:123.0,99:456.0,100:789.0}[pid]+(pid==99 and changed[0] and fault=='parent-reuse')
        def ppid(self):
            """Change the original target edge only after the old controller was observed."""
            return (100 if changed[0] and fault=='first-edge' else 99) if self.pid==42 else (100 if self.pid==99 else 1)
        def cmdline(self):
            """Only the middle parent is a supported controller."""
            return ['bash'] if self.pid==100 else ['python',str(state.root/('PBCoinData.py' if self.pid==42 else 'PBRun.py'))]
        def exe(self):
            """The unit MainPID is a wrapper around a separately verified controller."""
            return '/bin/bash' if self.pid==100 else '/synthetic/python3.12'
    monkeypatch.setattr(state.module.psutil,'Process',Chain)
    with pytest.raises(subject.ProcessStateError):
        with subject.manual_action(state.root,'stop'):pytest.fail('Changed ancestry must not authorize a signal')


def _systemctl_property_output(argv,values):
    """Model systemctl show: empty properties are omitted unless --all is requested."""
    names=[argv[index+1] for index,arg in enumerate(argv) if arg=='-p']
    return '\n'.join(name+'='+values[name] for name in names if name in values and (values[name] or '--all' in argv))


@pytest.mark.parametrize('job',['','271'])
def test_unit_query_requests_empty_properties(parent_cgroup_target,monkeypatch,job):
    """No-job uses real omission semantics; a real pending job still blocks ownership."""
    state=parent_cgroup_target
    state.unit['Job']=job
    calls=[]
    def show(argv,**kwargs):
        """Expose only explicitly requested properties in the real empty-value format."""
        calls.append(argv)
        return SimpleNamespace(returncode=0,stdout=_systemctl_property_output(argv,state.unit))
    monkeypatch.setattr(state.module.subprocess,'run',show)
    if job:
        with pytest.raises(ValueError):state.real_query_unit('parent.service',time.monotonic()+5)
    else:
        assert state.real_query_unit('parent.service',time.monotonic()+5)['Job']==''
    assert '--all' in calls[0]


@pytest.mark.parametrize('entry',['terminal','pbrun'])
@pytest.mark.parametrize('job',['','271'])
def test_manual_parent_queries_use_real_empty_property_format(parent_cgroup_target,monkeypatch,entry,job):
    """The manual action reaches the actual unit reader instead of a prevalidated dict."""
    state=parent_cgroup_target
    base='/user.slice/user@1000.service'
    group=base+'/app.slice/app-terminal.scope' if entry=='terminal' else base+'/app.slice/parent.service'
    state.data['/proc/42/cgroup']='0::'+group+'\n'
    monkeypatch.setattr(state.module,'_query_unit',state.real_query_unit)
    def show(argv,**kwargs):
        """Render current ancestor unit properties exactly as systemctl show does."""
        unit=argv[argv.index('show')+1]
        values={**state.unit,'Id':unit,'Job':job,'ControlGroup':base if unit.startswith('user@') else group}
        return SimpleNamespace(returncode=0,stdout=_systemctl_property_output(argv,values))
    monkeypatch.setattr(state.module.subprocess,'run',show)
    if job:
        with pytest.raises(subject.ProcessStateError):
            with subject.manual_action(state.root,'stop'):pytest.fail('Pending unit job must remain blocked')
    else:
        with subject.manual_action(state.root,'stop') as proof:
            assert subject._consume_authorization(state.root,'stop',proof)['pid']==42


@pytest.mark.parametrize('installed',[False,True])
@pytest.mark.parametrize('job',['','271'])
def test_online_manager_requests_empty_job_property(offline_manager,monkeypatch,installed,job):
    """An online absent unit is legacy only with a complete explicit empty Job value."""
    state=offline_manager
    values={'LoadState':'loaded' if installed else 'not-found','ActiveState':'active' if installed else 'inactive','MainPID':'42' if installed else '0','Job':job}
    calls=[]
    def show(argv,**kwargs):
        """Do not manufacture an empty Job line for a command lacking --all."""
        calls.append(argv)
        return SimpleNamespace(returncode=0,stdout=_systemctl_property_output(argv,values))
    monkeypatch.setattr(state.module.subprocess,'run',show)
    result=state.module.probe_manager(state.root)
    assert result['manager']==('systemd' if installed or job else 'legacy'),result
    assert '--all' in calls[0]


@pytest.mark.parametrize('missing',['Job','ControlGroup'])
def test_unit_query_still_requires_returned_properties(parent_cgroup_target,monkeypatch,missing):
    """Requesting all properties never authorizes incomplete successful responses."""
    state=parent_cgroup_target
    values={key:value for key,value in state.unit.items() if key!=missing}
    monkeypatch.setattr(state.module.subprocess,'run',lambda argv,**kwargs:
                        SimpleNamespace(returncode=0,stdout=_systemctl_property_output(argv,values)))
    with pytest.raises(ValueError,match='unconfirmed'):
        state.real_query_unit('parent.service',time.monotonic()+5)


def test_online_manager_still_requires_returned_job(offline_manager,monkeypatch):
    """A missing Job line remains UNKNOWN rather than being inferred to be idle."""
    values={'LoadState':'not-found','ActiveState':'inactive','MainPID':'0'}
    monkeypatch.setattr(offline_manager.module.subprocess,'run',lambda argv,**kwargs:
                        SimpleNamespace(returncode=0,stdout=_systemctl_property_output(argv,values)))
    result=offline_manager.module.probe_manager(offline_manager.root)
    assert result['manager']=='unknown'
    assert 'Incomplete user-manager unit state' in result['reason']


@pytest.fixture
def exit_transition(owned, monkeypatch):
    """Model kernel exit observations without signalling or inspecting host processes."""
    state = owned
    state.phase, state.finish, state.clock, state.assignment = 'running', 'gone', 0.0, 'original'
    state.cleaned, state.starts = [], []
    base = subject.psutil.Process

    class Process(base):
        """Keep full ownership checks real while argv disappears during shutdown."""
        def __init__(self, pid):
            if state.phase == 'gone':
                raise psutil.NoSuchProcess(pid)
            if state.phase == 'denied':
                raise psutil.AccessDenied(pid)
            super().__init__(pid)

        def status(self):
            """A matching zombie/dead lifetime is terminal; empty argv is not."""
            return {'zombie': psutil.STATUS_ZOMBIE, 'dead': psutil.STATUS_DEAD}.get(state.phase, psutil.STATUS_RUNNING)

        def cmdline(self):
            """Reproduce the native observation after a successfully authorized TERM."""
            return [] if state.phase == 'empty' else state.argv

    def signal_target(identity, signum):
        """Record only synthetic signals and enter the observed exit window."""
        state.signals.append((identity, signum))
        state.phase = 'empty' if signum == signal.SIGTERM else ('running' if state.finish == 'never' else 'gone')

    def advance(delay):
        """Advance a bounded monotonic clock and the configured kernel transition."""
        state.clock += delay
        if state.finish == 'reuse':
            state.created = 124.0
        elif state.finish == 'assignment':
            state.assignment = 'changed'
            state.phase = 'running'
        elif state.finish in {'alive', 'never'}:
            state.phase = 'running'
        elif state.finish != 'empty':
            state.phase = state.finish

    monkeypatch.setattr(subject.psutil, 'Process', Process)
    monkeypatch.setattr(subject.time, 'monotonic', lambda: state.clock)
    monkeypatch.setattr(subject.time, 'sleep', advance)
    monkeypatch.setattr(subject, 'signal_exact_process', signal_target)
    monkeypatch.setattr(subject, 'reap_children', lambda: None)
    monkeypatch.setattr(subject, 'snapshot', lambda root: subject.inspect_pid(root, 23, ProcessIdentity(23, state.created)))
    monkeypatch.setattr(subject, 'clear_owned_files', lambda root, identity: state.cleaned.append(identity))
    monkeypatch.setattr('pbcoindata_manager._manual_target', lambda *a, **k: state.assignment)
    monkeypatch.setattr('pbcoindata_manager.manual_process_evidence', lambda *a: state.assignment)
    monkeypatch.setattr(subject, '_start_path_ready', lambda *a, **k: True)
    return state


@pytest.mark.parametrize('finish', ['gone', 'zombie', 'dead'])
@pytest.mark.parametrize('action', ['stop', 'restart'])
def test_exact_stop_waits_through_empty_argv(exit_transition, monkeypatch, finish, action):
    """An already signalled exact daemon may lose argv before confirmed kernel exit."""
    state = exit_transition
    state.finish = finish
    def start(root, *, authorization):
        """Restart may obtain new start authorization only after the old lifetime ended."""
        assert state.phase == finish
        subject._consume_authorization(root, 'start', authorization)
        state.starts.append('start')
    monkeypatch.setattr(subject, 'start', start)
    getattr(subject, action)(state.root)
    assert state.signals == [(ProcessIdentity(23, 123), signal.SIGTERM)]
    assert state.cleaned == [ProcessIdentity(23, 123)]
    assert state.starts == (['start'] if action == 'restart' else [])
    assert 0 < state.clock <= 0.05


@pytest.mark.parametrize('finish', ['empty', 'reuse', 'assignment', 'denied'])
def test_exit_uncertainty_never_licenses_another_signal(exit_transition, finish):
    """No cleanup or KILL follows unresolved argv, reused PID, changed assignment or denied evidence."""
    state = exit_transition
    state.finish = finish
    with pytest.raises(subject.ProcessStateError):
        subject.stop(state.root)
    assert state.signals == [(ProcessIdentity(23, 123), signal.SIGTERM)]
    assert not state.cleaned and not state.starts
    assert 0 < state.clock <= 10.000001


@pytest.mark.parametrize('finish', ['alive', 'never'])
def test_exit_wait_keeps_existing_term_kill_budgets(exit_transition, finish):
    """A still verified live owner may escalate once, within the existing ten/two seconds."""
    state = exit_transition
    state.finish = finish
    if finish == 'never':
        with pytest.raises(subject.ProcessStateError, match='unconfirmed'):
            subject.stop(state.root)
    else:
        subject.stop(state.root)
    assert [sig for _, sig in state.signals] == [signal.SIGTERM, signal.SIGKILL]
    assert state.clock <= (12.000001 if finish == 'never' else 10.000001)


def test_empty_argv_before_authorization_is_still_foreign(exit_transition):
    """The exit wait must not weaken the ordinary service ownership boundary."""
    state = exit_transition
    state.phase = 'empty'
    assert subject.inspect_pid(state.root, 23, ProcessIdentity(23,123))['status'] == 'foreign'
    with pytest.raises(subject.ProcessStateError):
        subject.stop(state.root)
    assert not state.signals


@pytest.mark.parametrize('terminal', [psutil.STATUS_ZOMBIE, psutil.STATUS_DEAD])
def test_terminal_exit_rechecks_fresh_lifetime(exit_transition, monkeypatch, terminal):
    """A terminal observation cannot certify a PID replaced during the status read."""
    state = exit_transition
    base = subject.psutil.Process
    class Replaced(base):
        """Replace the lifetime between the first identity and terminal confirmation."""
        def status(self):
            state.created = 124.0
            return terminal
    monkeypatch.setattr(subject.psutil, 'Process', Replaced)
    with pytest.raises(subject.ProcessStateError, match='identity changed'):
        subject._wait_exit(state.root, ProcessIdentity(23,123), 10)
    assert not state.signals


@pytest.mark.parametrize('terminal', [psutil.STATUS_ZOMBIE, psutil.STATUS_DEAD])
def test_terminal_status_preserves_expected_identity_check(exit_transition, terminal):
    """Even a kernel terminal state never retires a different expected lifetime."""
    state = exit_transition
    state.phase = 'zombie' if terminal == psutil.STATUS_ZOMBIE else 'dead'
    state.created = 124.0
    assert subject.inspect_pid(state.root, 23, ProcessIdentity(23,123))['status'] == 'unknown'


@pytest.mark.parametrize('terminal', [psutil.STATUS_ZOMBIE, psutil.STATUS_DEAD])
def test_inspection_terminal_status_rechecks_fresh_lifetime(exit_transition, monkeypatch, terminal):
    """The pre-signal terminal shortcut cannot retire a PID replaced during its status read."""
    state = exit_transition
    base = subject.psutil.Process
    class Replaced(base):
        """Change the synthetic lifetime between initial identity and terminal status."""
        def status(self):
            state.created = 124.0
            return terminal
    monkeypatch.setattr(subject.psutil, 'Process', Replaced)
    assert subject.inspect_pid(state.root, 23, ProcessIdentity(23,123))['status'] == 'unknown'
    assert not state.signals


@pytest.fixture
def retained_zombie(tmp_path, monkeypatch, request):
    """Keep a real guarded zombie under another verified parent until fixture cleanup."""
    import select
    import pbcoindata_guard as guard
    for name in ('pbcoindata_guard.py','secure_files.py'):
        (tmp_path/name).write_bytes((ROOT/name).read_bytes())
    script = f'''import sys,os,signal,threading
sys.path.insert(0,{str(ROOT)!r})
from pbcoindata_guard import acquire_daemon
lease=acquire_daemon('.')
from pbcoindata_process import register_current,clear_owned_files
identity=register_current('.')
if 'PBGUI_COINDATA_LAUNCH_TOKEN' not in os.environ:
 clear_owned_files('.',identity)
 if os.environ.get('PBGUI_TEST_RETAIN_THREAD'):
  import ctypes,time
  from pathlib import Path
  def worker():
   deadline=time.monotonic()+12
   while not Path('release-thread').exists() and time.monotonic()<deadline:time.sleep(0.01)
   os._exit(0)
  threading.Thread(target=worker).start()
  ctypes.CDLL(None).pthread_exit(None)
 os._exit(0)
event=threading.Event()
signal.signal(signal.SIGTERM,lambda *a:event.set())
try:event.wait(15)
finally:clear_owned_files('.',identity)
'''
    (tmp_path/'PBCoinData.py').write_text(script)
    parent_script = f'''import os,sys,json
sys.path.insert(0,{str(ROOT)!r})
from pbcoindata_guard import FileLease
lease=FileLease('.','controller','pbrun')
child=os.fork()
if child==0:
 if {bool(getattr(request,'param',False))!r}:os.environ['PBGUI_TEST_RETAIN_THREAD']='1'
 os.execl(sys.executable,sys.executable,'-u','PBCoinData.py')
print(json.dumps({{'parent':os.getpid(),'child':child}}),flush=True)
try:
 for command in sys.stdin:
  if command.strip()=='release-controller':
   lease.release()
   print('released',flush=True)
  else:break
finally:
 os.waitpid(child,0)
 lease.release()
'''
    (tmp_path/'PBRun.py').write_text(parent_script)
    parent=subprocess.Popen([sys.executable,'-u','PBRun.py'],cwd=tmp_path,stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
    before_children=set(subject._CHILDREN)
    try:
        assert select.select([parent.stdout],[],[],8)[0], 'Synthetic parent did not become ready'
        ids=json.loads(parent.stdout.readline())
        deadline=time.monotonic()+8
        while psutil.Process(ids['child']).status()!=psutil.STATUS_ZOMBIE:
            assert time.monotonic()<deadline, 'Synthetic child did not reach kernel exit'
            time.sleep(0.02)
        expected=ProcessIdentity(ids['child'],psutil.Process(ids['child']).create_time())
        assert subject.inspect_pid(tmp_path,parent.pid,script='PBRun.py')['status']=='running'
        assert controller_metadata(tmp_path)['controller']=='pbrun'
        assert psutil.Process(expected.pid).ppid()==parent.pid
        assert not subject.pid_path(tmp_path).exists() and not identity_path(subject.pid_path(tmp_path)).exists()
        record=guard.read_publications(tmp_path)['records'][0]
        assert record['phase']=='ready' and record['pid']==expected.pid
        assert abs(record['create_time']-expected.create_time)<0.01
        monkeypatch.setattr(subject,'manager_state',lambda *a:{'manager':'legacy','reason':'isolated unit absence'})
        monkeypatch.setattr('pbcoindata_manager.manual_process_evidence',lambda *a:'isolated-manager-proof')
        monkeypatch.setattr('pbcoindata_manager._manual_target',lambda *a,**k:'isolated-target-proof')
        yield SimpleNamespace(root=tmp_path,parent=parent,identity=expected,record=record)
    except Exception as exc:
        pytest.fail(f'Controlled zombie setup failed: {type(exc).__name__}: {exc}', pytrace=False)
    finally:
        (tmp_path/'release-thread').touch()
        for pid,child in list(subject._CHILDREN.items()):
            if pid not in before_children:
                if child.poll() is None: child.terminate()
                child.wait(timeout=8)
        subject.reap_children()
        if parent.poll() is None:
            parent.stdin.write('reap\n');parent.stdin.flush()
        parent.communicate(timeout=10)
        assert parent.returncode==0


def test_retained_ready_zombie_uses_shared_classification(retained_zombie):
    """Actual snapshot and the stdlib early reader agree while another parent retains Z."""
    import pbcoindata_guard as guard
    state=retained_zombie
    before=guard.read_publications(state.root)['files']
    assert subject.snapshot(state.root)['status']=='stopped'
    assert subject._publication_state(state.root,state.record)=='retired'
    assert guard.publication_state(state.root,state.record)=='retired'
    subject.ownership_conflicts(state.root,subject.snapshot(state.root))
    assert guard._check_existing_publications(state.root)==before
    assert guard.read_publications(state.root)['files']==before
    assert psutil.Process(state.identity.pid).status()==psutil.STATUS_ZOMBIE
    assert state.parent.poll() is None


@pytest.mark.parametrize('entry',['early','manual','automatic'])
def test_retained_zombie_allows_guarded_replacement(retained_zombie,monkeypatch,entry):
    """The old zombie remains under its own parent while only a new guarded child starts."""
    state=retained_zombie
    signals=[]
    original=subject.signal_exact_process
    def send(identity,signum):
        """Record actual signals, rejecting any attempt to signal the retained zombie."""
        assert identity.pid!=state.identity.pid
        signals.append((identity,signum));return original(identity,signum)
    monkeypatch.setattr(subject,'signal_exact_process',send)
    if entry=='early':
        program=f"import sys,os;sys.path.insert(0,{str(ROOT)!r});from pbcoindata_guard import acquire_daemon;acquire_daemon('.');assert 'psutil' not in sys.modules;os._exit(0)"
        done=subprocess.run([sys.executable,'-c',program],cwd=state.root,capture_output=True,text=True,timeout=8)
        assert done.returncode==0,done.stderr
    elif entry=='manual':
        with subject.manual_action(state.root,'start') as proof:subject.start(state.root,authorization=proof)
    else:subject.start(state.root,automatic=True)
    if entry!='early':
        current=subject.snapshot(state.root)
        assert current['status']=='running' and current['pid']!=state.identity.pid
        subject.stop(state.root)
        assert signals==[(current['identity'],signal.SIGTERM)]
    assert psutil.Process(state.identity.pid).status()==psutil.STATUS_ZOMBIE
    assert state.parent.poll() is None


@pytest.mark.parametrize('blocker',['singleton','pending','corrupt','contradiction'])
def test_retired_zombie_preserves_start_fences(retained_zombie,monkeypatch,blocker):
    """A retired registration cannot release another lease or repair a launch journal."""
    import pbcoindata_guard as guard
    state=retained_zombie;lease=None
    if blocker=='singleton':lease=guard.FileLease(state.root,'singleton','other-owner')
    if blocker=='pending':subject._write_launch(state.root,{'version':1,'token':'a'*32,'pid':None,'create_time':None})
    if blocker=='corrupt':
        path=guard.journal_path(state.root);path.parent.mkdir(exist_ok=True,parents=True);path.write_text('{broken')
    if blocker=='contradiction':
        path=subject.pid_path(state.root);path.write_text(str(state.identity.pid))
        write_process_identity(path,ProcessIdentity(state.identity.pid,state.identity.create_time-1))
    before={p:p.read_bytes() for p in (state.root/'data/pid').iterdir()}
    journal=guard.journal_path(state.root)
    old=journal.read_bytes() if journal.exists() else None
    monkeypatch.setattr(subject,'signal_exact_process',lambda *a:pytest.fail('No signals permitted'))
    try:
        assert subject._publication_state(state.root,state.record)=='retired'
        assert subject.snapshot(state.root)['status']=='unknown'
        with pytest.raises((RuntimeError,ValueError,BlockingIOError)):
            guard.acquire_daemon(state.root)
        with pytest.raises(subject.ProcessStateError):subject.start(state.root,automatic=True)
        assert {p:p.read_bytes() for p in (state.root/'data/pid').iterdir()}==before
        assert (journal.read_bytes() if journal.exists() else None)==old
    finally:
        if lease:lease.release()
        for held in list(guard._LIFETIME_GUARDS):
            if held.root==state.root:held.release();guard._LIFETIME_GUARDS.remove(held)


@pytest.mark.parametrize('retained_zombie',[True],indirect=True)
def test_zombie_leader_live_thread_keeps_singleton(retained_zombie,monkeypatch):
    """A native Z group leader can still have a live thread retaining the lifetime lease."""
    import pbcoindata_guard as guard
    state=retained_zombie
    assert psutil.Process(state.identity.pid).num_threads()>1
    before=guard.read_publications(state.root)['files']
    assert guard.publication_state(state.root,state.record)=='retired'
    assert subject.snapshot(state.root)['status']=='unknown'
    monkeypatch.setattr(subject,'signal_exact_process',lambda *a:pytest.fail('No zombie signal'))
    with pytest.raises(subject.ProcessStateError):subject.start(state.root,automatic=True)
    with pytest.raises(BlockingIOError):guard.acquire_daemon(state.root)
    assert guard.read_publications(state.root)['files']==before


@pytest.fixture
def publication_procfs(tmp_path,monkeypatch):
    """Supply narrow synthetic procfs observations to both original readers."""
    import pbcoindata_guard as guard
    state=SimpleNamespace(mode='Z',ticks='23',comm='coin (worker) name',fault=None,reads=0,state_pair=None)
    context=guard.runtime_scope()
    record={**guard.scope(tmp_path),**context,'pid':42,'create_time':100.23,'kind':'registration','phase':'ready'}
    original=Path.read_text
    def read(path,*a,**k):
        """Only the named process and boot-time rows are synthetic."""
        if path==Path('/proc/42/stat'):
            state.reads+=1
            if state.fault=='denied':raise PermissionError('isolated stat denial')
            if state.fault=='missing':raise FileNotFoundError('isolated missing stat')
            fields=['0']*50;fields[0]=state.mode;fields[19]=state.ticks
            if state.state_pair is not None:fields[0]=state.state_pair[(state.reads-1)%2]
            if state.fault=='flip-state' and state.reads%2==0:fields[0]='S'
            if state.fault=='flip-time' and state.reads%2==0:fields[19]='24'
            if state.fault=='truncated':fields=fields[:20]
            if state.fault=='cpu-time':fields[11]='-1'
            return f"{43 if state.fault=='pid' else 42} ({state.comm}) "+' '.join(fields)
        if path==Path('/proc/stat'):
            return 'btime invalid\n' if state.fault=='boot-time' else 'btime 100\n'
        return original(path,*a,**k)
    monkeypatch.setattr(Path,'read_text',read)
    monkeypatch.setattr(guard.os,'sysconf',lambda *a:100)
    monkeypatch.setattr(guard,'runtime_scope',lambda:context)
    def probe(pid,signum):
        """Synthetic procfs still has this PID; missing stat is not kernel-confirmed exit."""
        assert pid==42 and signum==0
    monkeypatch.setattr(guard.os,'kill',probe)
    class Process:
        """The adapter still obtains a lifetime; terminal policy remains in the guard."""
        def __init__(self,pid):assert pid==42
        def create_time(self):return 100.23
        def cmdline(self):return []
        def cwd(self):raise FileNotFoundError('No verifiable invocation')
    monkeypatch.setattr(subject.psutil,'Process',Process)
    monkeypatch.setattr(guard,'foreign_publication',lambda *a,**k:(_ for _ in ()).throw(ValueError('Unbound invocation')))
    state.record=record;state.root=tmp_path;state.context=context
    return state


@pytest.mark.parametrize('reader',['guard','adapter'])
@pytest.mark.parametrize('first,fresh,expected',[
    ('R','S','active'),('S','R','active'),
    ('Z','X','retired'),('X','Z','retired'),
    ('Z','S','unknown'),('S','Z','unknown'),('R','Z','unknown'),
])
def test_publication_state_transitions_preserve_lifetime(publication_procfs,reader,first,fresh,expected):
    """Scheduler changes preserve identity; only two terminal samples prove retirement."""
    import pbcoindata_guard as guard
    state=publication_procfs
    state.state_pair=(first,fresh)
    # The stdlib reader first samples creation_time; the adapter uses psutil instead.
    state.reads=-1 if reader=='guard' else 0
    read=guard.publication_state if reader=='guard' else subject._publication_state
    assert read(state.root,state.record)==expected
    assert state.reads==2


@pytest.mark.parametrize('reader',['guard','adapter'])
@pytest.mark.parametrize('field',[0,2])
def test_publication_identity_fields_must_match_exactly(publication_procfs,monkeypatch,reader,field):
    """Neither near-equal birth times nor changed ticks may certify one stable lifetime."""
    import pbcoindata_guard as guard
    state=publication_procfs
    original=guard.process_observation
    calls=[]
    def observe(pid):
        """Change only the second bound sample, after any initial birth-time read."""
        calls.append(pid)
        value=list(original(pid))
        if len(calls)==(3 if reader=='guard' else 2):
            value[field]+=.001 if field==0 else 1
        return tuple(value)
    monkeypatch.setattr(guard,'process_observation',observe)
    read=guard.publication_state if reader=='guard' else subject._publication_state
    assert read(state.root,state.record)=='unknown'


@pytest.mark.parametrize('mode',['Z','X'])
@pytest.mark.parametrize('comm',['coin','coin worker','coin (inner)) worker'])
def test_terminal_stat_parsing_uses_last_parenthesis(publication_procfs,mode,comm):
    """Both terminal modes share validated stat parsing, including complex comm values."""
    import pbcoindata_guard as guard
    state=publication_procfs;state.mode=mode;state.comm=comm
    assert guard.process_observation(42)==(100.23,mode,23)
    assert guard.publication_state(state.root,state.record)=='retired'
    assert subject._publication_state(state.root,state.record)=='retired'


@pytest.mark.parametrize('mode',['Z','X'])
@pytest.mark.parametrize('fault',['truncated','pid','cpu-time','boot-time','denied','missing','bad-state','nan','negative','overflow','clock-rate'])
def test_terminal_stat_errors_remain_unknown(publication_procfs,monkeypatch,mode,fault):
    """Missing, denied or malformed stat evidence never proves the bound process ended."""
    state=publication_procfs;state.mode=mode;state.fault=fault
    if fault=='bad-state':state.mode='?'
    if fault=='nan':state.ticks='NaN'
    if fault=='negative':state.ticks='-1'
    if fault=='overflow':state.ticks='9'*400
    if fault=='clock-rate':monkeypatch.setattr(subject.guard_protocol.os,'sysconf',lambda *a:0)
    # The adapter independently still sees this PID/lifetime, including when stat is missing.
    assert subject._publication_state(state.root,state.record)=='unknown'
    import pbcoindata_guard as guard
    assert guard.publication_state(state.root,state.record)=='unknown'


@pytest.mark.parametrize('fault',['no-identity','no-context','namespace','boot','reuse','live','empty-argv','flip-state','flip-time','context-change'])
def test_terminal_retirement_requires_bound_stable_evidence(publication_procfs,monkeypatch,fault):
    """Terminal relaxation is limited to matching boot/namespace/lifetime evidence."""
    import pbcoindata_guard as guard
    state=publication_procfs
    if fault=='no-identity':state.record.pop('create_time')
    if fault=='no-context':state.record.pop('boot_id')
    if fault=='namespace':state.record['namespaces']={**state.context['namespaces'],'pid':'pid:[1]'}
    if fault=='boot':state.record['boot_id']='00000000-0000-4000-8000-000000000001'
    if fault=='reuse':state.record['create_time']-=1
    if fault in {'live','empty-argv'}:state.mode='S'
    if fault in {'flip-state','flip-time'}:state.fault=fault
    if fault=='context-change':
        calls=[]
        def context():
            """Change the observing namespace after its initial verification."""
            calls.append(1)
            return state.context if len(calls)==1 else {**state.context,'boot_id':'changed'}
        monkeypatch.setattr(guard,'runtime_scope',context)
    expected='active' if fault in {'live','empty-argv'} else 'unknown'
    assert subject._publication_state(state.root,state.record)==expected
    if fault=='context-change':calls.clear()
    assert guard.publication_state(state.root,state.record)==expected


@pytest.mark.parametrize('kernel',['absent','present','denied'])
def test_missing_stat_requires_kernel_confirmed_exit(publication_procfs,monkeypatch,kernel):
    """Only an ESRCH kernel result confirms that repeated missing stat means PID exit."""
    import pbcoindata_guard as guard
    state=publication_procfs;state.fault='missing'
    def probe(pid,signum):
        """No actual signal is sent by this synthetic existence probe."""
        assert pid==42 and signum==0
        if kernel=='absent':raise ProcessLookupError('No such PID')
        if kernel=='denied':raise PermissionError('Unconfirmed PID')
    monkeypatch.setattr(guard.os,'kill',probe)
    assert guard.publication_state(state.root,state.record)==('retired' if kernel=='absent' else 'unknown')
