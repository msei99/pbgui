"""Exact Linux process identities and race-resistant signalling helpers."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import errno
import json
import os
from pathlib import Path
import signal
import time

import psutil

from secure_files import atomic_write_private_text


_CREATE_TIME_TOLERANCE = 0.01
_PIDFD_FALLBACK_ERRNOS = {
    errno.EACCES,
    errno.EINVAL,
    errno.ENOSYS,
    errno.EPERM,
    getattr(errno, "ENOTSUP", errno.EINVAL),
    getattr(errno, "EOPNOTSUPP", errno.EINVAL),
}


class ExactProcessSignalError(RuntimeError):
    """Raised when an exact live process could not be signalled safely."""


@dataclass(frozen=True, order=True)
class ProcessIdentity:
    """PID plus kernel process creation time."""

    pid: int
    create_time: float


def process_identity(pid: int) -> ProcessIdentity | None:
    """Return the current identity for a positive PID, or None after exit."""
    try:
        process = psutil.Process(int(pid))
        return ProcessIdentity(pid=int(pid), create_time=float(process.create_time()))
    except (psutil.NoSuchProcess, ProcessLookupError, ValueError):
        return None
    except (psutil.Error, OSError):
        return None


def identities_match(left: ProcessIdentity | None, right: ProcessIdentity | None) -> bool:
    """Return whether two snapshots identify the same process lifetime."""
    return bool(
        left
        and right
        and left.pid == right.pid
        and abs(left.create_time - right.create_time) < _CREATE_TIME_TOLERANCE
    )


def exact_process_alive(identity: ProcessIdentity) -> bool:
    """Return whether the exact process lifetime still owns its PID."""
    return identities_match(process_identity(identity.pid), identity)


def identity_path(pid_file: Path) -> Path:
    """Return the private sidecar used beside a compatibility numeric PID file."""
    pid_file = Path(pid_file)
    return pid_file.with_name(f"{pid_file.name}.identity.json")


def write_process_identity(pid_file: Path, identity: ProcessIdentity) -> None:
    """Atomically persist an exact identity without changing numeric PID consumers."""
    atomic_write_private_text(
        identity_path(pid_file),
        json.dumps(asdict(identity), indent=4, sort_keys=True) + "\n",
    )


def read_process_identity(pid_file: Path) -> ProcessIdentity | None:
    """Read an exact persisted identity sidecar."""
    try:
        value = json.loads(identity_path(pid_file).read_text(encoding="utf-8"))
        identity = ProcessIdentity(pid=int(value["pid"]), create_time=float(value["create_time"]))
        if identity.pid <= 1 or identity.create_time <= 0:
            return None
        return identity
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError):
        return None


def clear_process_identity(pid_file: Path, expected: ProcessIdentity | None = None) -> bool:
    """Remove a sidecar only when it still names the expected process lifetime."""
    path = identity_path(pid_file)
    if expected is not None and not identities_match(read_process_identity(pid_file), expected):
        return False
    path.unlink(missing_ok=True)
    return True


def signal_exact_process(identity: ProcessIdentity, signum: int) -> bool:
    """Signal only one process lifetime, preferring pidfd and safely falling back."""
    if not exact_process_alive(identity):
        return False

    pidfd_open = getattr(os, "pidfd_open", None)
    pidfd_send_signal = getattr(signal, "pidfd_send_signal", None)
    if pidfd_open is not None and pidfd_send_signal is not None:
        try:
            pidfd = pidfd_open(identity.pid, 0)
        except OSError as exc:
            if exc.errno == errno.ESRCH:
                return False
            if exc.errno not in _PIDFD_FALLBACK_ERRNOS:
                _raise_if_still_alive(identity, exc)
                return False
        else:
            try:
                if not exact_process_alive(identity):
                    return False
                try:
                    pidfd_send_signal(pidfd, signum, None, 0)
                    return True
                except OSError as exc:
                    if exc.errno == errno.ESRCH:
                        return False
                    if exc.errno not in _PIDFD_FALLBACK_ERRNOS:
                        _raise_if_still_alive(identity, exc)
                        return False
            finally:
                os.close(pidfd)

    try:
        process = psutil.Process(identity.pid)
        current = ProcessIdentity(identity.pid, float(process.create_time()))
        if not identities_match(current, identity):
            return False
        process.send_signal(signum)
        return True
    except (psutil.NoSuchProcess, ProcessLookupError):
        return False
    except (psutil.Error, OSError) as exc:
        _raise_if_still_alive(identity, exc)
        return False


def wait_for_exact_process_exit(
    identity: ProcessIdentity,
    timeout: float,
    *,
    poll_interval: float = 0.05,
) -> bool:
    """Wait a bounded interval for one exact process lifetime to end."""
    deadline = time.monotonic() + max(0.0, float(timeout))
    while exact_process_alive(identity):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        time.sleep(min(max(0.01, poll_interval), remaining))
    return True


def _raise_if_still_alive(identity: ProcessIdentity, cause: BaseException) -> None:
    """Turn a failed signal against a still-live exact owner into a conflict."""
    try:
        process = psutil.Process(identity.pid)
        still_alive = identities_match(
            ProcessIdentity(identity.pid, float(process.create_time())),
            identity,
        )
    except (psutil.NoSuchProcess, ProcessLookupError):
        still_alive = False
    except (psutil.Error, OSError, ValueError):
        still_alive = psutil.pid_exists(identity.pid)
    if still_alive:
        raise ExactProcessSignalError(
            f"Exact process PID {identity.pid} (created {identity.create_time:.6f}) remains alive after signal failure: {cause}"
        ) from cause
