"""Isolated regression tests for direct PBApiServer PID handoff."""

from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace
import threading

import pytest

import api_pid_handoff
import api.services as services
import PBApiServer
from process_identity import ProcessIdentity


class _Lease:
    """Track releases for isolated restart route tests."""

    def __init__(self) -> None:
        self.releases = 0
        self.detached = False

    def detach(self) -> None:
        """Record transfer to asynchronous restart work."""
        self.detached = True

    def release(self) -> None:
        """Record one idempotent test release request."""
        self.releases += 1


class _ImmediateThread:
    """Execute a restart target synchronously without creating a real thread."""

    def __init__(self, *, target, daemon: bool) -> None:
        del daemon
        self.target = target

    def start(self) -> None:
        """Run the target immediately."""
        self.target()


def test_spawn_failure_preserves_authoritative_pid(monkeypatch, tmp_path: Path) -> None:
    """Immediate replacement spawn failure does not remove the old API PID state."""
    pid_file = tmp_path / "data" / "pid" / "api_server.pid"
    pid_file.parent.mkdir(parents=True)
    pid_file.write_text("111\n", encoding="utf-8")
    monkeypatch.setattr(
        api_pid_handoff,
        "snapshot_api_owner",
        lambda *_args, **_kwargs: ProcessIdentity(111, 1.0),
    )
    monkeypatch.setattr(
        api_pid_handoff.subprocess,
        "Popen",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("spawn failed")),
    )

    with pytest.raises(OSError, match="spawn failed"):
        api_pid_handoff.spawn_api_replacement("python", tmp_path, 111)

    assert pid_file.read_text(encoding="utf-8") == "111\n"


def test_successful_spawn_declares_expected_old_pid(monkeypatch, tmp_path: Path) -> None:
    """A replacement receives the explicit old PID and leaves ownership unchanged initially."""
    pid_file = tmp_path / "data" / "pid" / "api_server.pid"
    pid_file.parent.mkdir(parents=True)
    pid_file.write_text("111\n", encoding="utf-8")
    calls: list[tuple[list[str], dict]] = []
    monkeypatch.setattr(
        api_pid_handoff,
        "snapshot_api_owner",
        lambda *_args, **_kwargs: ProcessIdentity(111, 1.0),
    )

    def fake_popen(args, **kwargs):
        calls.append((list(args), kwargs))
        return SimpleNamespace(pid=222, returncode=None, poll=lambda: None)

    monkeypatch.setattr(api_pid_handoff.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(api_pid_handoff, "process_identity", lambda pid: ProcessIdentity(pid, 2.0))

    replacement = api_pid_handoff.spawn_api_replacement("python", tmp_path, 111)

    assert calls[0][0] == ["python", str(tmp_path / "PBApiServer.py")]
    assert calls[0][1]["env"][api_pid_handoff.API_RESTART_EXPECTED_PID_ENV] == "111"
    assert calls[0][1]["env"][api_pid_handoff.API_RESTART_EXPECTED_CREATE_TIME_ENV] == "1.000000000"
    assert calls[0][1]["env"][api_pid_handoff.API_RESTART_ACK_TOKEN_ENV] == replacement.token
    assert calls[0][1]["pass_fds"]
    assert calls[0][1]["close_fds"] is True
    assert pid_file.read_text(encoding="utf-8") == "111\n"
    os.close(replacement.ack_fd)


def test_restart_route_spawn_failure_keeps_pid_and_current_process(monkeypatch, tmp_path: Path) -> None:
    """A failed direct restart preparation returns an error without signalling the API."""
    pid_file = tmp_path / "data" / "pid" / "api_server.pid"
    pid_file.parent.mkdir(parents=True)
    pid_file.write_text("111\n", encoding="utf-8")
    leases = (_Lease(), _Lease(), _Lease())
    killed: list[int] = []

    async def unblocked() -> tuple[bool, str]:
        return False, ""

    monkeypatch.setattr(services, "_systemd_unit_for_service", lambda _name: None)
    monkeypatch.setattr(services, "_log", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(PBApiServer, "_restart_block_state", unblocked)
    monkeypatch.setattr(PBApiServer, "_api_restart_lease", None)
    monkeypatch.setattr(PBApiServer, "_acquire_api_restart_leases", lambda: leases)
    monkeypatch.setattr(
        PBApiServer,
        "_prepare_direct_api_replacement",
        lambda **_kwargs: (_ for _ in ()).throw(OSError("spawn failed")),
    )
    monkeypatch.setattr(PBApiServer, "signal_exact_process", lambda *_args: killed.append(111))

    with pytest.raises(services.HTTPException) as error:
        services.restart_api_server(session=None)

    assert error.value.status_code == 500
    assert "spawn failed" in str(error.value.detail)
    assert pid_file.read_text(encoding="utf-8") == "111\n"
    assert killed == []
    assert [lease.releases for lease in leases] == [1, 1, 1]
    assert PBApiServer._api_restart_lease is None


def test_restart_route_spawns_handoff_before_signalling(monkeypatch, tmp_path: Path) -> None:
    """A successful direct restart acknowledges the child before signalling the old API."""
    pid_file = tmp_path / "data" / "pid" / "api_server.pid"
    pid_file.parent.mkdir(parents=True)
    pid_file.write_text("111\n", encoding="utf-8")
    leases = (_Lease(), _Lease(), _Lease())
    events: list[str] = []

    async def unblocked() -> tuple[bool, str]:
        return False, ""

    monkeypatch.setattr(services, "_systemd_unit_for_service", lambda _name: None)
    monkeypatch.setattr(services, "_log", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(PBApiServer, "_restart_block_state", unblocked)
    monkeypatch.setattr(PBApiServer, "_api_restart_lease", None)
    monkeypatch.setattr(PBApiServer, "_acquire_api_restart_leases", lambda: leases)
    monkeypatch.setattr(
        PBApiServer,
        "_prepare_direct_api_replacement",
        lambda **_kwargs: events.append("ack") or SimpleNamespace(old_identity=ProcessIdentity(111, 1.0)),
    )
    monkeypatch.setattr(PBApiServer, "signal_exact_process", lambda *_args: events.append("signal") or True)
    monkeypatch.setattr(services.time, "sleep", lambda _delay: None)
    monkeypatch.setattr(threading, "Thread", _ImmediateThread)

    result = services.restart_api_server(session=None)

    assert result["ok"] is True
    assert events == ["ack", "signal"]
    assert pid_file.read_text(encoding="utf-8") == "111\n"
    assert [lease.detached for lease in leases] == [True, True, False]
    PBApiServer._api_restart_lease = None
    for lease in leases:
        lease.release()


def test_handoff_waits_then_atomically_takes_pid(monkeypatch, tmp_path: Path) -> None:
    """The replacement waits for the exact old process before taking PID ownership."""
    pid_file = tmp_path / "api_server.pid"
    pid_file.write_text("111\n", encoding="utf-8")
    old = ProcessIdentity(111, 1.0)
    checks = iter([True, False, False])
    monkeypatch.setattr(api_pid_handoff, "process_identity", lambda pid: ProcessIdentity(pid, 2.0))
    monkeypatch.setattr(api_pid_handoff, "snapshot_api_owner", lambda *_args, **_kwargs: old)
    monkeypatch.setattr(api_pid_handoff, "exact_process_alive", lambda _identity: next(checks))
    monkeypatch.setattr(api_pid_handoff.time, "sleep", lambda _delay: None)

    claimed = api_pid_handoff.claim_api_pid(
        pid_file,
        tmp_path / "PBApiServer.py",
        expected_old_pid=111,
        current_pid=222,
        timeout=1,
    )

    assert claimed is True
    assert pid_file.read_text(encoding="utf-8") == "222\n"
    assert (tmp_path / "api_server.pid.lock").is_file()


def test_handoff_refuses_changed_pid_ownership(monkeypatch, tmp_path: Path) -> None:
    """A replacement cannot overwrite ownership changed by another process."""
    pid_file = tmp_path / "api_server.pid"
    pid_file.write_text("333\n", encoding="utf-8")
    monkeypatch.setattr(api_pid_handoff, "process_identity", lambda pid: ProcessIdentity(pid, 2.0))

    with pytest.raises(api_pid_handoff.ApiPidOwnershipError, match="ownership changed"):
        api_pid_handoff.claim_api_pid(
            pid_file,
            tmp_path / "PBApiServer.py",
            expected_old_pid=111,
            current_pid=222,
            timeout=0,
        )

    assert pid_file.read_text(encoding="utf-8") == "333\n"


def test_normal_claim_suppresses_duplicate_api(monkeypatch, tmp_path: Path) -> None:
    """Concurrent normal startup cannot replace a live API PID owner."""
    pid_file = tmp_path / "api_server.pid"
    pid_file.write_text("111\n", encoding="utf-8")
    monkeypatch.setattr(api_pid_handoff, "api_process_matches", lambda pid, _script: pid == 111)
    monkeypatch.setattr(api_pid_handoff, "process_identity", lambda pid: ProcessIdentity(pid, 2.0))

    claimed = api_pid_handoff.claim_api_pid(
        pid_file,
        tmp_path / "PBApiServer.py",
        current_pid=222,
    )

    assert claimed is False
    assert pid_file.read_text(encoding="utf-8") == "111\n"


def test_pid_cleanup_cannot_remove_replacement_ownership(tmp_path: Path) -> None:
    """Late cleanup by the old API cannot unlink a replacement's PID file."""
    pid_file = tmp_path / "api_server.pid"
    pid_file.write_text("222\n", encoding="utf-8")

    removed = api_pid_handoff.release_api_pid(pid_file, 111)

    assert removed is False
    assert pid_file.read_text(encoding="utf-8") == "222\n"


def test_claim_identity_persistence_failure_restores_old_pid(monkeypatch, tmp_path: Path) -> None:
    """A sidecar write failure cannot leave partial replacement PID ownership."""
    pid_file = tmp_path / "api_server.pid"
    pid_file.write_text("111\n", encoding="utf-8")
    monkeypatch.setattr(api_pid_handoff, "process_identity", lambda pid: ProcessIdentity(pid, 2.0))
    monkeypatch.setattr(api_pid_handoff, "api_process_matches", lambda *_args: False)
    monkeypatch.setattr(
        api_pid_handoff,
        "write_process_identity",
        lambda *_args: (_ for _ in ()).throw(OSError("sidecar failed")),
    )

    with pytest.raises(OSError, match="sidecar failed"):
        api_pid_handoff.claim_api_pid(
            pid_file,
            tmp_path / "PBApiServer.py",
            current_pid=222,
        )

    assert pid_file.read_text(encoding="utf-8") == "111\n"


def test_child_acknowledges_only_the_verified_old_identity(monkeypatch, tmp_path: Path) -> None:
    """The handoff child echoes its random token only after exact owner verification."""
    pid_file = tmp_path / "api_server.pid"
    pid_file.write_text("111\n", encoding="utf-8")
    old = ProcessIdentity(111, 1.25)
    read_fd, write_fd = os.pipe()
    token = "x" * 43
    monkeypatch.setenv(api_pid_handoff.API_RESTART_EXPECTED_PID_ENV, "111")
    monkeypatch.setenv(api_pid_handoff.API_RESTART_EXPECTED_CREATE_TIME_ENV, "1.25")
    monkeypatch.setenv(api_pid_handoff.API_RESTART_ACK_FD_ENV, str(write_fd))
    monkeypatch.setenv(api_pid_handoff.API_RESTART_ACK_TOKEN_ENV, token)
    monkeypatch.setattr(api_pid_handoff, "snapshot_api_owner", lambda *_args, **_kwargs: old)

    acknowledged = api_pid_handoff.acknowledge_api_handoff_from_environment(
        pid_file,
        tmp_path / "PBApiServer.py",
    )

    assert acknowledged == old
    assert os.read(read_fd, 100) == f"{token}\n".encode("ascii")
    os.close(read_fd)


def test_ack_timeout_terminates_exact_child(monkeypatch) -> None:
    """Missing acknowledgement never reports success and invokes exact-child cleanup."""
    read_fd, write_fd = os.pipe()
    process = SimpleNamespace(pid=222, returncode=None, poll=lambda: None)
    replacement = api_pid_handoff.ApiReplacement(process, read_fd, "y" * 43, ProcessIdentity(111, 1.0))
    cleaned: list[object] = []
    monkeypatch.setattr(api_pid_handoff, "_terminate_replacement", lambda child: cleaned.append(child))
    try:
        with pytest.raises(api_pid_handoff.ApiReplacementStartupError, match="did not acknowledge"):
            api_pid_handoff.wait_for_api_replacement_ack(replacement, timeout=0.01)
    finally:
        os.close(write_fd)

    assert cleaned == [replacement]


def test_cancel_replacement_signals_and_reaps_captured_child_identity(monkeypatch) -> None:
    """Cancellation uses the child creation identity and waits on its Popen handle."""
    child = ProcessIdentity(222, 2.0)
    calls: list[tuple[str, object]] = []

    class Process:
        """Popen double that exits after exact SIGTERM delivery."""

        pid = 222
        returncode = None

        def poll(self):
            return self.returncode

        def wait(self, timeout=0):
            calls.append(("wait", timeout))
            self.returncode = 0
            return 0

    process = Process()

    def signal_child(identity, signum):
        calls.append(("signal", (identity, signum)))
        process.returncode = 0
        return True

    monkeypatch.setattr(api_pid_handoff, "signal_exact_process", signal_child)
    replacement = api_pid_handoff.ApiReplacement(
        process,
        -1,
        "z" * 43,
        ProcessIdentity(111, 1.0),
        child,
    )

    api_pid_handoff.cancel_api_replacement(replacement)

    assert calls[0] == ("signal", (child, api_pid_handoff.signal.SIGTERM))
    assert calls[1] == ("wait", 2.0)


def test_api_stop_signals_and_cleans_only_exact_owner(monkeypatch, tmp_path: Path) -> None:
    """PBApiServer.stop carries one persisted creation identity through cleanup."""
    pid_file = tmp_path / "api_server.pid"
    pid_file.write_text("111\n", encoding="utf-8")
    owner = ProcessIdentity(111, 1.0)
    server = PBApiServer.PBApiServer.__new__(PBApiServer.PBApiServer)
    server.pidfile = pid_file
    server.my_pid = None
    signals: list[ProcessIdentity] = []
    releases: list[ProcessIdentity] = []
    monkeypatch.setattr(api_pid_handoff, "read_api_pid", lambda _path: 111)
    monkeypatch.setattr(PBApiServer, "snapshot_api_owner", lambda *_args, **_kwargs: owner)
    monkeypatch.setattr(PBApiServer, "signal_exact_process", lambda identity, _sig: signals.append(identity) or True)
    monkeypatch.setattr(PBApiServer, "wait_for_exact_process_exit", lambda identity, _timeout: identity == owner)
    monkeypatch.setattr(PBApiServer, "exact_process_alive", lambda _identity: False)
    monkeypatch.setattr(PBApiServer, "release_api_pid", lambda _path, identity: releases.append(identity) or True)

    server.stop()

    assert signals == [owner]
    assert releases == [owner]
