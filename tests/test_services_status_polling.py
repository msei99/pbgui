"""Regression tests for the cost of the polled Services status endpoint (#393)."""

import threading
from pathlib import Path
from types import SimpleNamespace

import psutil
import pytest

import api.services as services


@pytest.fixture
def script_cache():
    """Keep path-cache regressions independent of test execution order."""
    services._resolved_service_script.cache_clear()
    try:
        yield
    finally:
        services._resolved_service_script.cache_clear()


def test_irrelevant_process_scan_avoids_filesystem(monkeypatch, script_cache) -> None:
    """A cold scan of 300 unrelated processes performs no cwd or resolve calls."""
    calls = {"cwd": 0, "resolve": 0}

    def forbidden_cwd():
        """Count even cwd errors that the matcher deliberately catches."""
        calls["cwd"] += 1
        raise PermissionError("unrelated process")

    def forbidden_resolve(*args, **kwargs):
        """Reject path resolution even before the cache has any entries."""
        calls["resolve"] += 1
        raise AssertionError("irrelevant process must not resolve paths")

    processes = [SimpleNamespace(info={"pid": pid, "cmdline": ["python", "unrelated.py"],
                                      "username": "test"}, cwd=forbidden_cwd)
                 for pid in range(1, 301)]
    monkeypatch.setattr(psutil, "process_iter", lambda attrs: iter(processes))
    monkeypatch.setattr(Path, "resolve", forbidden_resolve)
    assert len(services._SERVICE_SCRIPT_NAMES) == 7
    assert services._collect_pbgui_daemon_processes() == []
    assert calls == {"cwd": 0, "resolve": 0}
    assert services._resolved_service_script.cache_info().currsize == 0


@pytest.mark.parametrize("kind", ["relative", "absolute", "symlink", "foreign"])
def test_daemon_script_path_identity(monkeypatch, tmp_path, script_cache, kind) -> None:
    """Only paths resolving to this installation's canonical script match."""
    root = tmp_path / "install"
    root.mkdir()
    script = root / "PBRun.py"
    script.write_text("# isolated daemon fixture\n", encoding="utf-8")
    other = tmp_path / "other"
    other.mkdir()
    if kind == "relative":
        argument = "./PBRun.py"
    elif kind == "absolute":
        argument = str(script)
    elif kind == "symlink":
        argument = str(other / "PBRun.py")
        Path(argument).symlink_to(script)
    else:
        argument = str(other / "PBRun.py")
        Path(argument).write_text("# foreign fixture\n", encoding="utf-8")
    monkeypatch.setattr(services, "PBGDIR", root)
    process = SimpleNamespace(cwd=lambda: str(root))
    assert services._process_matches_service_script(
        process, ["python", "-u", argument], "pbrun.py"
    ) is (kind != "foreign")


def test_script_cache_reuses_paths_and_tracks_installation(monkeypatch, tmp_path, script_cache) -> None:
    """Repeated checks reuse the target, while PBGDIR changes select a new target."""
    roots = [tmp_path / "first", tmp_path / "second"]
    for root in roots:
        root.mkdir()
        (root / "PBRun.py").write_text("# fixture\n", encoding="utf-8")
    original_resolve = Path.resolve
    resolutions = []

    def counted_resolve(path, *args, **kwargs):
        """Track target resolution separately from relative candidate resolution."""
        resolutions.append(path)
        return original_resolve(path, *args, **kwargs)

    monkeypatch.setattr(Path, "resolve", counted_resolve)
    for root in roots:
        monkeypatch.setattr(services, "PBGDIR", root)
        process = SimpleNamespace(cwd=lambda: str(root / "subdir"))
        for _ in range(2):
            assert services._process_matches_service_script(
                process, ["python", "../PBRun.py"], "PBRun.py"
            )
        assert resolutions.count(root / "PBRun.py") == 1
    assert services._resolved_service_script.cache_info().misses == 2
    assert services._resolved_service_script.cache_info().hits == 2
    process = SimpleNamespace(cwd=lambda: str(roots[0]))
    assert not services._process_matches_service_script(process, ["PBRun.py"], "PBRun.py")
    assert services._resolved_service_script.cache_info().maxsize == 64


@pytest.mark.parametrize("unknown", [False, True])
def test_unreadable_cwd_and_unknown_scripts(monkeypatch, script_cache, unknown) -> None:
    """Unknown names bypass cwd; unreadable cwd still rejects a known daemon."""
    calls = []

    def unreadable_cwd():
        """Simulate access denial without inspecting a real process."""
        calls.append(1)
        raise PermissionError("isolated access denial")

    name = "unknown.py" if unknown else "PBRun.py"
    assert not services._process_matches_service_script(
        SimpleNamespace(cwd=unreadable_cwd), ["python", name], name
    )
    assert len(calls) == (0 if unknown else 1)
    assert services._resolved_service_script.cache_info().currsize == 0


def test_pid_and_scan_preserve_daemon_identity(monkeypatch, tmp_path, script_cache) -> None:
    """PID checks and scans agree on local versus foreign daemon identities."""
    monkeypatch.setattr(services, "PBGDIR", tmp_path)
    local = SimpleNamespace(
        info={"pid": 123, "cmdline": ["python", "PBRun.py"], "username": "test"},
        cwd=lambda: str(tmp_path), cmdline=lambda: ["python", "PBRun.py"],
        create_time=lambda: 42.0,
    )
    foreign = SimpleNamespace(
        info={"pid": 124, "cmdline": ["python", "PBRun.py"], "username": "test"},
        cwd=lambda: str(tmp_path / "foreign"), cmdline=lambda: ["python", "PBRun.py"],
        create_time=lambda: 43.0,
    )
    processes = {123: local, 124: foreign}
    monkeypatch.setattr(psutil, "pid_exists", lambda pid: pid in processes)
    monkeypatch.setattr(psutil, "Process", lambda pid: processes[pid])
    monkeypatch.setattr(psutil, "process_iter", lambda attrs: iter(processes.values()))
    monkeypatch.setattr(services.os, "getpid", lambda: 123)
    assert services._pid_matches_service(123, "pbrun")
    assert not services._pid_matches_service(124, "pbrun")
    assert not services._pid_matches_service(125, "pbrun")
    assert services._collect_pbgui_daemon_processes() == [{
        "pid": 123, "create_time": 42.0, "service": "pbrun", "script": "PBRun.py",
        "current": True, "cmdline": "python PBRun.py", "username": "test",
    }]


def test_status_scans_daemon_processes_once_per_request(monkeypatch) -> None:
    """All stopped legacy services share one OS process scan."""
    scans = []
    monkeypatch.setattr(services, "_status_reconcile_last_ok", float("inf"))
    monkeypatch.setattr(services, "_systemd_service_status", lambda name: None)
    monkeypatch.setattr(services, "_optional_service_blocker", lambda name: None)
    monkeypatch.setattr(services, "_systemd_enable_status", lambda *args: {})
    monkeypatch.setattr(services, "_read_service_pid", lambda name: None)

    def scan():
        scans.append(1)
        return [{"service": "pbrun"}]

    monkeypatch.setattr(services, "_collect_pbgui_daemon_processes", scan)
    result = services.get_status(session=None)
    assert len(scans) == 1
    assert result["pbrun"]["running"] is True
    assert result["pbdata"]["running"] is False


def test_status_throttles_credential_reconciliation(monkeypatch) -> None:
    """Pending credential reconciliation runs at most once per interval after success."""
    calls = []
    clock = [1000.0]
    monkeypatch.setattr(services, "_status_reconcile_last_ok", float("-inf"))
    monkeypatch.setattr(services.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(services, "reconcile_pending_credentials", lambda root: calls.append(root))
    monkeypatch.setattr(services, "_collect_pbgui_daemon_processes", lambda: [])
    monkeypatch.setattr(services, "_service_status", lambda name, daemons=None: {"running": False})

    services.get_status(session=None)
    services.get_status(session=None)
    assert len(calls) == 1
    clock[0] += services._STATUS_RECONCILE_INTERVAL_S
    services.get_status(session=None)
    assert len(calls) == 2


def test_status_retries_failed_credential_reconciliation(monkeypatch) -> None:
    """A failed reconciliation is not treated as done and retries on the next poll."""
    calls = []

    def failing(root):
        calls.append(root)
        raise RuntimeError("locked")

    monkeypatch.setattr(services, "_status_reconcile_last_ok", float("-inf"))
    monkeypatch.setattr(services, "reconcile_pending_credentials", failing)
    monkeypatch.setattr(services, "_collect_pbgui_daemon_processes", lambda: [])
    monkeypatch.setattr(services, "_service_status", lambda name, daemons=None: {"running": False})
    monkeypatch.setattr(services, "_log", lambda *args, **kwargs: None)

    services.get_status(session=None)
    services.get_status(session=None)
    assert len(calls) == 2


def test_concurrent_status_requests_reconcile_once(monkeypatch) -> None:
    """Parallel polls must not both run the credential reconciliation."""
    calls = []
    entered = threading.Event()
    release = threading.Event()

    def slow_reconcile(root):
        calls.append(root)
        entered.set()
        release.wait(5)

    monkeypatch.setattr(services, "_status_reconcile_last_ok", float("-inf"))
    monkeypatch.setattr(services, "reconcile_pending_credentials", slow_reconcile)
    monkeypatch.setattr(services, "_collect_pbgui_daemon_processes", lambda: [])
    monkeypatch.setattr(services, "_service_status", lambda name, daemons=None: {"running": False})

    first = threading.Thread(target=services.get_status, kwargs={"session": None})
    first.start()
    assert entered.wait(5)
    services.get_status(session=None)
    release.set()
    first.join(5)
    assert len(calls) == 1
