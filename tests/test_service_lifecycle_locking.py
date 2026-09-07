"""Isolated regression tests for PBGui service lifecycle serialization."""

from __future__ import annotations

import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

import api.services as services
import PBApiServer
from PBCluster import PBCluster
from process_identity import ExactProcessSignalError
from process_identity import ProcessIdentity, write_process_identity
from service_lifecycle_lock import ServiceLifecycleBusyError, acquire_service_lifecycle_lock


def test_service_lifecycle_lock_is_reentrant(tmp_path: Path) -> None:
    """One thread may safely nest the same per-service lifecycle lease."""
    outer = acquire_service_lifecycle_lock(tmp_path, "pbdata", "restart", timeout=0.2)
    inner = acquire_service_lifecycle_lock(tmp_path, "pbdata", "stop", timeout=0.2)

    inner.release()
    outer.release()

    replacement = acquire_service_lifecycle_lock(tmp_path, "pbdata", "start", timeout=0.2)
    replacement.release()


def test_service_lifecycle_lease_can_release_from_handoff_thread(tmp_path: Path) -> None:
    """A restart watchdog may release a lease acquired by the request thread."""
    lease = acquire_service_lifecycle_lock(tmp_path, "api-server", "restart", timeout=0.2)
    releaser = threading.Thread(target=lease.release)
    releaser.start()
    releaser.join(timeout=2)

    assert not releaser.is_alive()
    replacement = acquire_service_lifecycle_lock(tmp_path, "api-server", "restart", timeout=0.2)
    replacement.release()


def test_detached_lifecycle_lease_disables_request_thread_reentry(tmp_path: Path) -> None:
    """A reused request thread cannot reenter a lease handed to async restart work."""
    lease = acquire_service_lifecycle_lock(tmp_path, "api-server", "restart", timeout=0.2)
    lease.detach()
    try:
        with pytest.raises(ServiceLifecycleBusyError):
            acquire_service_lifecycle_lock(tmp_path, "api-server", "restart", timeout=0.05)
    finally:
        lease.release()


def test_service_lifecycle_lock_reports_cross_process_contention(tmp_path: Path) -> None:
    """A competing process receives a bounded error with owner context."""
    script = """
import sys
from pathlib import Path
from service_lifecycle_lock import acquire_service_lifecycle_lock
lease = acquire_service_lifecycle_lock(Path(sys.argv[1]), 'pbdata', 'restart', timeout=1)
print('locked', flush=True)
sys.stdin.readline()
lease.release()
"""
    child = subprocess.Popen(
        [sys.executable, "-c", script, str(tmp_path)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=str(Path(__file__).resolve().parents[1]),
    )
    try:
        assert child.stdout is not None
        assert child.stdout.readline().strip() == "locked"
        with pytest.raises(ServiceLifecycleBusyError) as error:
            acquire_service_lifecycle_lock(tmp_path, "pbdata", "start", timeout=0.05)
        assert "pbdata" in str(error.value)
        assert "pid=" in str(error.value)
        assert "0.1s" in str(error.value)
    finally:
        if child.stdin is not None:
            child.stdin.write("release\n")
            child.stdin.flush()
        child.wait(timeout=5)


def test_legacy_start_is_serialized_and_revalidated(monkeypatch, tmp_path: Path) -> None:
    """Two starts serialize and the second observes the first daemon instead of spawning."""
    state = {"running": False, "pid": 0, "runs": 0}
    run_entered = threading.Event()
    allow_run = threading.Event()

    class Service:
        """Controllable legacy service double."""

        def run(self) -> None:
            state["runs"] += 1
            run_entered.set()
            assert allow_run.wait(timeout=2)
            state.update(running=True, pid=101)

        def stop(self) -> None:
            state.update(running=False, pid=0)

    monkeypatch.setattr(services, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(services, "_optional_service_blocker", lambda _name: "")
    monkeypatch.setattr(services, "_systemd_service_action", lambda _name, _action: None)
    monkeypatch.setattr(services, "_service_status", lambda _name: {"running": state["running"], "manager": "legacy"})
    monkeypatch.setattr(services, "_service_identities", lambda _name, status=None: ((state["pid"], float(state["pid"])),) if state["pid"] else ())
    monkeypatch.setattr(services, "_get_service", lambda _name: Service())

    results: list[dict[str, object]] = []
    first = threading.Thread(target=lambda: results.append(services._service_action("pbdata", "start")))
    second = threading.Thread(target=lambda: results.append(services._service_action("pbdata", "start")))
    first.start()
    assert run_entered.wait(timeout=2)
    second.start()
    time.sleep(0.05)
    assert state["runs"] == 1
    allow_run.set()
    first.join(timeout=2)
    second.join(timeout=2)

    assert not first.is_alive()
    assert not second.is_alive()
    assert state["runs"] == 1
    assert len(results) == 2
    assert all(result["running"] is True for result in results)


def test_restart_waits_for_old_identity_before_start(monkeypatch, tmp_path: Path) -> None:
    """Legacy restart verifies shutdown and a distinct replacement without a fixed delay."""
    state = {"running": True, "pid": 10}
    calls: list[str] = []

    class Service:
        """Legacy service double with explicit process identity changes."""

        def stop(self) -> None:
            calls.append("stop")
            state.update(running=False, pid=0)

        def run(self) -> None:
            calls.append("run")
            state.update(running=True, pid=20)

    monkeypatch.setattr(services, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(services, "_optional_service_blocker", lambda _name: "")
    monkeypatch.setattr(services, "_systemd_service_action", lambda _name, _action: None)
    monkeypatch.setattr(services, "_service_status", lambda _name: {"running": state["running"], "manager": "legacy"})
    monkeypatch.setattr(services, "_service_identities", lambda _name, status=None: ((state["pid"], float(state["pid"])),) if state["pid"] else ())
    monkeypatch.setattr(services, "_get_service", lambda _name: Service())

    result = services._service_action("pbdata", "restart")

    assert calls == ["stop", "run"]
    assert result["running"] is True
    assert state["pid"] == 20


def test_systemd_action_remains_inside_lifecycle_lock(monkeypatch, tmp_path: Path) -> None:
    """The per-service lease covers the complete systemd command and verification."""
    state = {"running": False}
    contention: list[bool] = []

    def systemd_action(_name: str, _action: str) -> dict[str, object]:
        def contend() -> None:
            try:
                acquire_service_lifecycle_lock(tmp_path, "pbdata", "stop", timeout=0.05)
            except ServiceLifecycleBusyError:
                contention.append(True)

        competitor = threading.Thread(target=contend)
        competitor.start()
        competitor.join(timeout=2)
        state["running"] = True
        return {"running": True, "manager": "systemd", "unit": "pbgui-pbdata.service"}

    monkeypatch.setattr(services, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(services, "_optional_service_blocker", lambda _name: "")
    monkeypatch.setattr(
        services,
        "_service_status",
        lambda _name: {
            "running": state["running"],
            "manager": "systemd",
            "unit": "pbgui-pbdata.service",
        },
    )
    monkeypatch.setattr(
        services,
        "_service_identities",
        lambda _name, status=None: ((200, 2.0),) if state["running"] else (),
    )
    monkeypatch.setattr(services, "_systemd_service_action", systemd_action)

    result = services._service_action("pbdata", "start")

    assert result["running"] is True
    assert contention == [True]


def test_lifecycle_contention_remains_http_409(monkeypatch, tmp_path: Path) -> None:
    """Route error handling preserves lifecycle lock conflicts as HTTP 409."""
    locked = threading.Event()
    release = threading.Event()

    def hold_lock() -> None:
        with acquire_service_lifecycle_lock(tmp_path, "pbdata", "restart", timeout=1):
            locked.set()
            assert release.wait(timeout=2)

    holder = threading.Thread(target=hold_lock)
    holder.start()
    assert locked.wait(timeout=2)
    monkeypatch.setattr(services, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(services, "_SERVICE_LIFECYCLE_LOCK_TIMEOUT_S", 0.05)
    try:
        with pytest.raises(services.HTTPException) as error:
            services.start_service("pbdata", session=None)
        assert error.value.status_code == 409
        assert "Another lifecycle action" in str(error.value.detail)
    finally:
        release.set()
        holder.join(timeout=2)


def test_api_restart_lifecycle_contention_remains_http_409(monkeypatch) -> None:
    """The dedicated API restart route preserves lifecycle contention as HTTP 409."""
    monkeypatch.setattr(
        PBApiServer,
        "_acquire_api_restart_leases",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(ServiceLifecycleBusyError("restart busy")),
    )

    with pytest.raises(services.HTTPException) as error:
        services.restart_api_server(session=None)

    assert error.value.status_code == 409
    assert "restart busy" in str(error.value.detail)


def test_pbcluster_stop_rejects_unrelated_pid(monkeypatch, tmp_path: Path) -> None:
    """PBCluster never signals a PID that fails exact script identity validation."""
    service = PBCluster(tmp_path)
    service.pidfile.parent.mkdir(parents=True)
    service.pidfile.write_text("424242\n", encoding="utf-8")
    monkeypatch.setattr(service, "_process_identity", lambda _pid: None)
    monkeypatch.setattr("PBCluster.signal_exact_process", lambda *_args: (_ for _ in ()).throw(AssertionError("must not signal")))

    service.stop()

    assert service.pidfile.read_text(encoding="utf-8") == "424242\n"


def test_pbcluster_stop_rejects_same_pid_with_reused_creation_time(monkeypatch, tmp_path: Path) -> None:
    """PBCluster sidecar identity prevents signalling a same-script recycled PID."""
    service = PBCluster(tmp_path)
    service.pidfile.parent.mkdir(parents=True)
    service.pidfile.write_text("4242\n", encoding="utf-8")
    write_process_identity(service.pidfile, ProcessIdentity(4242, 10.0))
    monkeypatch.setattr(service, "_process_identity", lambda _pid: ProcessIdentity(4242, 11.0))
    monkeypatch.setattr("PBCluster.signal_exact_process", lambda *_args: (_ for _ in ()).throw(AssertionError("must not signal")))

    service.stop()

    assert service.pidfile.read_text(encoding="utf-8") == "4242\n"


def test_restart_accepts_same_pid_with_new_creation_time(monkeypatch) -> None:
    """A recycled numeric PID with a new create time is a valid replacement."""
    identities = iter([((42, 10.0),), ((42, 11.0),)])
    monkeypatch.setattr(services, "_service_status", lambda _name: {"running": True, "manager": "systemd"})
    monkeypatch.setattr(services, "_service_identities", lambda _name, status=None: next(identities))

    result = services._wait_for_service_transition(
        "pbdata",
        running=True,
        previous_identities=((42, 10.0),),
    )

    assert result["running"] is True


def test_global_transition_lock_blocks_per_service_action(monkeypatch, tmp_path: Path) -> None:
    """A root restart reservation excludes a concurrent individual transition."""
    lease = acquire_service_lifecycle_lock(tmp_path, "all-services", "root-restart", timeout=0.2)
    monkeypatch.setattr(services, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(services, "_SERVICE_LIFECYCLE_LOCK_TIMEOUT_S", 0.05)
    outcome: list[int] = []

    def contend() -> None:
        try:
            services._service_action("pbdata", "start")
        except services.HTTPException as exc:
            outcome.append(exc.status_code)

    competitor = threading.Thread(target=contend)
    competitor.start()
    competitor.join(timeout=2)
    lease.release()

    assert outcome == [409]


def test_exact_owner_signal_failure_maps_to_http_409(monkeypatch, tmp_path: Path) -> None:
    """A live exact daemon after signal failure is a conflict, never stop success."""
    class Service:
        """Legacy service double that cannot deliver its exact stop signal."""

        def stop(self) -> None:
            raise ExactProcessSignalError("exact owner remains alive")

    monkeypatch.setattr(services, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(services, "_service_status", lambda _name: {"running": True, "manager": "legacy"})
    monkeypatch.setattr(services, "_service_identities", lambda _name, status=None: ((42, 10.0),))
    monkeypatch.setattr(services, "_get_service", lambda _name: Service())

    with pytest.raises(services.HTTPException) as error:
        services._service_action("pbcluster", "stop")

    assert error.value.status_code == 409
    assert "remains alive" in str(error.value.detail)


def test_pbcluster_foreground_suppresses_duplicate(monkeypatch, tmp_path: Path) -> None:
    """A second legacy PBCluster process leaves the authoritative PID untouched."""
    service = PBCluster(tmp_path)
    service.pidfile.parent.mkdir(parents=True)
    service.pidfile.write_text("12345\n", encoding="utf-8")
    monkeypatch.setattr("PBCluster._log", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(service, "_pid_matches", lambda pid: pid == 12345)
    monkeypatch.setattr(
        service.worker,
        "run_forever",
        lambda: (_ for _ in ()).throw(AssertionError("duplicate must not run")),
    )

    service.run_foreground()

    assert service.pidfile.read_text(encoding="utf-8") == "12345\n"


def test_systemd_migration_holds_global_then_update_lease_for_full_transaction(
    monkeypatch, tmp_path: Path,
) -> None:
    """Migration reserves all services before master update and releases in reverse."""
    events: list[str] = []

    class Lease:
        """Record acquisition-specific release ordering."""

        def __init__(self, name: str) -> None:
            self.name = name

        def release(self) -> None:
            events.append(f"release:{self.name}")

    monkeypatch.setattr(services, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(
        services,
        "acquire_service_lifecycle_lock",
        lambda _root, service, action, timeout: (
            events.append(f"acquire:{service}:{action}") or Lease("all-services")
        ),
    )
    monkeypatch.setattr(
        services,
        "acquire_master_update_lock",
        lambda _root: events.append("acquire:master-update") or Lease("master-update"),
    )

    def migrate() -> dict[str, bool]:
        events.append("migration")
        assert events == ["acquire:all-services:migration", "acquire:master-update", "migration"]
        return {"ok": True}

    monkeypatch.setattr(services, "_run_systemd_migration", migrate)

    assert services.run_migration(session=None) == {"ok": True}
    assert events == [
        "acquire:all-services:migration",
        "acquire:master-update",
        "migration",
        "release:master-update",
        "release:all-services",
    ]


@pytest.mark.parametrize("busy_kind", ["lifecycle", "master-update"])
def test_systemd_migration_lock_contention_is_http_409(
    monkeypatch, tmp_path: Path, busy_kind: str,
) -> None:
    """Migration lock contention remains a retryable conflict and releases partial leases."""
    released: list[str] = []

    class Lease:
        """Track release of an acquired global lease."""

        def release(self) -> None:
            released.append("all-services")

    monkeypatch.setattr(services, "PBGDIR", str(tmp_path))
    if busy_kind == "lifecycle":
        monkeypatch.setattr(
            services,
            "acquire_service_lifecycle_lock",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(ServiceLifecycleBusyError("busy")),
        )
    else:
        monkeypatch.setattr(services, "acquire_service_lifecycle_lock", lambda *_args, **_kwargs: Lease())
        monkeypatch.setattr(
            services,
            "acquire_master_update_lock",
            lambda _root: (_ for _ in ()).throw(services.MasterUpdateBusyError("busy")),
        )
    monkeypatch.setattr(
        services,
        "_run_systemd_migration",
        lambda: (_ for _ in ()).throw(AssertionError("migration must not run")),
    )

    with pytest.raises(services.HTTPException) as error:
        services.run_migration(session=None)

    assert error.value.status_code == 409
    assert released == ([] if busy_kind == "lifecycle" else ["all-services"])
