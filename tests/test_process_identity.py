"""Regression tests for exact Linux lifecycle process signalling."""

from __future__ import annotations

import errno
import os
from pathlib import Path
import signal

import psutil
import pytest

import process_identity as lifecycle


class _Process:
    """Minimal psutil process double with a stable creation identity."""

    def __init__(self, identity: lifecycle.ProcessIdentity, calls: list[int], *, fail: bool = False) -> None:
        self.identity = identity
        self.calls = calls
        self.fail = fail

    def create_time(self) -> float:
        """Return the configured process creation timestamp."""
        return self.identity.create_time

    def send_signal(self, signum: int) -> None:
        """Record fallback delivery or emulate a permission failure."""
        if self.fail:
            raise psutil.AccessDenied(self.identity.pid)
        self.calls.append(signum)


def test_pidfd_open_seccomp_failure_falls_back_to_identity_checked_psutil(monkeypatch) -> None:
    """EPERM from pidfd_open does not disable safe lifecycle signalling."""
    identity = lifecycle.ProcessIdentity(42, 10.0)
    calls: list[int] = []
    monkeypatch.setattr(lifecycle, "process_identity", lambda _pid: identity)
    monkeypatch.setattr(lifecycle.os, "pidfd_open", lambda *_args: (_ for _ in ()).throw(OSError(errno.EPERM, "blocked")))
    monkeypatch.setattr(lifecycle.psutil, "Process", lambda _pid: _Process(identity, calls))

    assert lifecycle.signal_exact_process(identity, signal.SIGTERM) is True
    assert calls == [signal.SIGTERM]


def test_pidfd_send_unsupported_falls_back_after_immediate_revalidation(monkeypatch) -> None:
    """An unsupported pidfd send retries only after creation-time revalidation."""
    identity = lifecycle.ProcessIdentity(42, 10.0)
    calls: list[int] = []
    fd = os.open(os.devnull, os.O_RDONLY)
    monkeypatch.setattr(lifecycle, "process_identity", lambda _pid: identity)
    monkeypatch.setattr(lifecycle.os, "pidfd_open", lambda *_args: fd)
    monkeypatch.setattr(
        lifecycle.signal,
        "pidfd_send_signal",
        lambda *_args: (_ for _ in ()).throw(OSError(errno.ENOSYS, "unsupported")),
    )
    monkeypatch.setattr(lifecycle.psutil, "Process", lambda _pid: _Process(identity, calls))

    assert lifecycle.signal_exact_process(identity, signal.SIGTERM) is True
    assert calls == [signal.SIGTERM]


def test_fallback_refuses_reused_pid_without_signalling(monkeypatch) -> None:
    """A changed creation time closes the final validate-then-signal race window."""
    expected = lifecycle.ProcessIdentity(42, 10.0)
    reused = lifecycle.ProcessIdentity(42, 11.0)
    calls: list[int] = []
    snapshots = iter([expected, reused])
    monkeypatch.setattr(lifecycle, "process_identity", lambda _pid: next(snapshots))
    monkeypatch.setattr(lifecycle.os, "pidfd_open", None, raising=False)
    monkeypatch.setattr(lifecycle.psutil, "Process", lambda _pid: _Process(reused, calls))

    assert lifecycle.signal_exact_process(expected, signal.SIGTERM) is False
    assert calls == []


def test_signal_failure_while_exact_owner_lives_raises_conflict(monkeypatch) -> None:
    """A live exact owner after fallback failure is never reported as success."""
    identity = lifecycle.ProcessIdentity(42, 10.0)
    monkeypatch.setattr(lifecycle, "process_identity", lambda _pid: identity)
    monkeypatch.setattr(lifecycle.os, "pidfd_open", None, raising=False)
    monkeypatch.setattr(lifecycle.psutil, "Process", lambda _pid: _Process(identity, [], fail=True))

    with pytest.raises(lifecycle.ExactProcessSignalError, match="remains alive"):
        lifecycle.signal_exact_process(identity, signal.SIGTERM)


def test_identity_sidecar_is_private_atomic_json(tmp_path: Path) -> None:
    """Lifecycle identity persists beside, not inside, compatibility PID state."""
    pid_file = tmp_path / "service.pid"
    pid_file.write_text("42\n", encoding="utf-8")
    identity = lifecycle.ProcessIdentity(42, 10.0)

    lifecycle.write_process_identity(pid_file, identity)

    sidecar = lifecycle.identity_path(pid_file)
    assert lifecycle.read_process_identity(pid_file) == identity
    assert sidecar.stat().st_mode & 0o777 == 0o600
    assert pid_file.read_text(encoding="utf-8") == "42\n"
