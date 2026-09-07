"""Atomic PID ownership handoff for direct PBApiServer restarts."""

from __future__ import annotations

import os
from pathlib import Path
import secrets
import select
import signal
import subprocess
import time

import psutil

from file_lock import advisory_file_lock
from process_identity import (
    ProcessIdentity,
    clear_process_identity,
    exact_process_alive,
    identities_match,
    process_identity,
    read_process_identity,
    signal_exact_process,
    write_process_identity,
)
from secure_files import atomic_write_private_text


API_RESTART_EXPECTED_PID_ENV = "PBGUI_RESTART_EXPECTED_PID"
API_RESTART_EXPECTED_CREATE_TIME_ENV = "PBGUI_RESTART_EXPECTED_CREATE_TIME"
API_RESTART_ACK_FD_ENV = "PBGUI_RESTART_ACK_FD"
API_RESTART_ACK_TOKEN_ENV = "PBGUI_RESTART_ACK_TOKEN"
API_HANDOFF_TIMEOUT_SECONDS = 30.0
API_ACK_TIMEOUT_SECONDS = 5.0


class ApiPidOwnershipError(RuntimeError):
    """Raised when a PBApiServer process cannot safely claim PID ownership."""


class ApiReplacementStartupError(RuntimeError):
    """Raised when a direct API replacement does not acknowledge handoff readiness."""


class ApiReplacement:
    """Parent-owned direct child and its private acknowledgement pipe."""

    def __init__(
        self,
        process: subprocess.Popen,
        ack_fd: int,
        token: str,
        old_identity: ProcessIdentity,
        child_identity: ProcessIdentity | None = None,
    ) -> None:
        self.process = process
        self.ack_fd = ack_fd
        self.token = token
        self.old_identity = old_identity
        self.child_identity = child_identity


def read_api_pid(pid_file: Path) -> int | None:
    """Read a positive API PID, returning None for missing or malformed state."""
    try:
        value = Path(pid_file).read_text(encoding="utf-8").strip()
        pid = int(value)
    except (OSError, ValueError):
        return None
    return pid if pid > 0 else None


def api_process_identity(pid: int, api_script: Path) -> ProcessIdentity | None:
    """Return an exact identity when PID executes this installation's API script."""
    try:
        process = psutil.Process(int(pid))
        identity = ProcessIdentity(int(pid), float(process.create_time()))
        cwd = Path(process.cwd())
        expected = Path(api_script).resolve(strict=False)
        for raw_arg in process.cmdline():
            arg = Path(str(raw_arg))
            if arg.name.lower() != expected.name.lower():
                continue
            candidate = arg if arg.is_absolute() else cwd / arg
            if candidate.resolve(strict=False) == expected:
                return identity
    except (OSError, psutil.Error, ValueError):
        return None
    return None


def api_process_matches(pid: int, api_script: Path) -> bool:
    """Return whether PID executes this installation's PBApiServer script."""
    return api_process_identity(pid, api_script) is not None


def snapshot_api_owner(pid_file: Path, api_script: Path, expected_pid: int | None = None) -> ProcessIdentity:
    """Validate and persist the exact API owner named by the numeric PID file."""
    pid_file = Path(pid_file)
    with advisory_file_lock(pid_file):
        pid = read_api_pid(pid_file)
        if pid is None or (expected_pid is not None and pid != int(expected_pid)):
            raise ApiPidOwnershipError("API PID ownership changed")
        current = api_process_identity(pid, api_script)
        if current is None:
            raise ApiPidOwnershipError(f"API PID {pid} is not the expected API process")
        persisted = read_process_identity(pid_file)
        if persisted is not None and not identities_match(persisted, current):
            raise ApiPidOwnershipError(f"API PID {pid} creation identity changed")
        if persisted is None:
            write_process_identity(pid_file, current)
        return current


def claim_api_pid(
    pid_file: Path,
    api_script: Path,
    *,
    expected_old_pid: int | None = None,
    expected_old_create_time: float | None = None,
    current_pid: int | None = None,
    timeout: float = API_HANDOFF_TIMEOUT_SECONDS,
    poll_interval: float = 0.1,
) -> bool:
    """Atomically claim the API PID file, optionally after an explicit handoff."""
    pid_file = Path(pid_file)
    api_script = Path(api_script)
    owner_pid = int(current_pid or os.getpid())
    owner_identity = process_identity(owner_pid)
    if owner_identity is None:
        raise ApiPidOwnershipError("Could not read current API process identity")
    if expected_old_pid is not None:
        expected_old_pid = int(expected_old_pid)
        if expected_old_pid <= 0 or expected_old_pid == owner_pid:
            raise ApiPidOwnershipError("Invalid expected old API PID")
        old_identity = snapshot_api_owner(pid_file, api_script, expected_old_pid)
        if expected_old_create_time is not None:
            requested = ProcessIdentity(expected_old_pid, float(expected_old_create_time))
            if not identities_match(old_identity, requested):
                raise ApiPidOwnershipError(f"API PID {expected_old_pid} creation identity changed")
        deadline = time.monotonic() + max(0.0, timeout)
        while exact_process_alive(old_identity):
            if read_api_pid(pid_file) != expected_old_pid:
                raise ApiPidOwnershipError(
                    f"API PID handoff expected {expected_old_pid}, but ownership changed"
                )
            if time.monotonic() >= deadline:
                raise ApiPidOwnershipError(
                    f"Old API PID {expected_old_pid} did not stop within {max(0.0, timeout):.1f}s"
                )
            time.sleep(min(max(0.01, poll_interval), max(0.0, deadline - time.monotonic())))

    with advisory_file_lock(pid_file):
        existing_pid = read_api_pid(pid_file)
        existing_identity = read_process_identity(pid_file)
        if expected_old_pid is not None:
            if existing_pid != expected_old_pid:
                raise ApiPidOwnershipError(
                    f"API PID handoff expected {expected_old_pid}, but ownership changed"
                )
            if exact_process_alive(old_identity):
                raise ApiPidOwnershipError(f"Old API PID {expected_old_pid} is still running")
        elif existing_pid and existing_pid != owner_pid and api_process_matches(existing_pid, api_script):
            return False
        try:
            atomic_write_private_text(pid_file, f"{owner_pid}\n")
            write_process_identity(pid_file, owner_identity)
        except Exception:
            if existing_pid is None:
                pid_file.unlink(missing_ok=True)
            else:
                atomic_write_private_text(pid_file, f"{existing_pid}\n")
            if existing_identity is None:
                clear_process_identity(pid_file)
            else:
                write_process_identity(pid_file, existing_identity)
            raise
    return True


def release_api_pid(pid_file: Path, owner: int | ProcessIdentity) -> bool:
    """Remove the API PID file only while it still belongs to the caller."""
    pid_file = Path(pid_file)
    owner_pid = owner.pid if isinstance(owner, ProcessIdentity) else int(owner)
    with advisory_file_lock(pid_file):
        if read_api_pid(pid_file) != owner_pid:
            return False
        persisted = read_process_identity(pid_file)
        if isinstance(owner, ProcessIdentity) and persisted is not None and not identities_match(persisted, owner):
            return False
        pid_file.unlink(missing_ok=True)
        clear_process_identity(pid_file, persisted)
    return True


def acknowledge_api_handoff_from_environment(pid_file: Path, api_script: Path) -> ProcessIdentity | None:
    """Verify expected old ownership and acknowledge that this child reached the wait."""
    pid_text = os.environ.pop(API_RESTART_EXPECTED_PID_ENV, "").strip()
    create_time_text = os.environ.pop(API_RESTART_EXPECTED_CREATE_TIME_ENV, "").strip()
    ack_fd_text = os.environ.pop(API_RESTART_ACK_FD_ENV, "").strip()
    token = os.environ.pop(API_RESTART_ACK_TOKEN_ENV, "").strip()
    if not any((pid_text, create_time_text, ack_fd_text, token)):
        return None
    try:
        expected = ProcessIdentity(int(pid_text), float(create_time_text))
        ack_fd = int(ack_fd_text)
    except ValueError as exc:
        raise ApiPidOwnershipError("Invalid API restart handoff state") from exc
    if expected.pid <= 1 or expected.create_time <= 0 or ack_fd < 0 or len(token) < 32:
        raise ApiPidOwnershipError("Invalid API restart handoff state")
    current = snapshot_api_owner(pid_file, api_script, expected.pid)
    if not identities_match(current, expected):
        raise ApiPidOwnershipError("Expected old API ownership changed before acknowledgement")
    try:
        os.write(ack_fd, f"{token}\n".encode("ascii"))
    finally:
        os.close(ack_fd)
    return expected


def spawn_api_replacement(
    python: str | Path,
    pbgui_dir: Path,
    expected_old_pid: int,
    *,
    stdin=None,
    stdout=None,
    stderr=None,
) -> ApiReplacement:
    """Spawn a direct API replacement without altering current PID ownership."""
    pbgui_dir = Path(pbgui_dir)
    pid_file = pbgui_dir / "data" / "pid" / "api_server.pid"
    api_script = pbgui_dir / "PBApiServer.py"
    old_identity = snapshot_api_owner(pid_file, api_script, expected_old_pid)
    token = secrets.token_urlsafe(32)
    read_fd, write_fd = os.pipe()
    os.set_inheritable(read_fd, False)
    os.set_inheritable(write_fd, True)
    env = os.environ.copy()
    env[API_RESTART_EXPECTED_PID_ENV] = str(old_identity.pid)
    env[API_RESTART_EXPECTED_CREATE_TIME_ENV] = f"{old_identity.create_time:.9f}"
    env[API_RESTART_ACK_FD_ENV] = str(write_fd)
    env[API_RESTART_ACK_TOKEN_ENV] = token
    try:
        process = subprocess.Popen(
            [str(python), str(api_script)],
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            close_fds=True,
            pass_fds=(write_fd,),
            cwd=str(pbgui_dir),
            env=env,
        )
    except Exception:
        os.close(read_fd)
        raise
    finally:
        os.close(write_fd)
    child_identity = process_identity(process.pid)
    if child_identity is None:
        replacement = ApiReplacement(process, read_fd, token, old_identity)
        try:
            _terminate_replacement(replacement)
        finally:
            os.close(read_fd)
        raise ApiReplacementStartupError("Could not capture replacement API child identity")
    return ApiReplacement(process, read_fd, token, old_identity, child_identity)


def wait_for_api_replacement_ack(
    replacement: ApiReplacement,
    timeout: float = API_ACK_TIMEOUT_SECONDS,
) -> None:
    """Wait for the exact child token, terminating and reaping it on failure."""
    deadline = time.monotonic() + max(0.0, float(timeout))
    received = bytearray()
    try:
        while time.monotonic() < deadline and len(received) <= 256:
            if replacement.process.poll() is not None:
                raise ApiReplacementStartupError(
                    f"Replacement API exited before acknowledgement with code {replacement.process.returncode}"
                )
            readable, _, _ = select.select([replacement.ack_fd], [], [], min(0.05, deadline - time.monotonic()))
            if not readable:
                continue
            chunk = os.read(replacement.ack_fd, 257 - len(received))
            if not chunk:
                break
            received.extend(chunk)
            if b"\n" in received:
                break
        if received.decode("ascii", errors="replace").strip() != replacement.token:
            raise ApiReplacementStartupError(
                f"Replacement API did not acknowledge handoff within {max(0.0, float(timeout)):g}s"
            )
    except Exception:
        _terminate_replacement(replacement)
        raise
    finally:
        os.close(replacement.ack_fd)


def cancel_api_replacement(replacement: ApiReplacement) -> None:
    """Terminate and reap an acknowledged child while the old API remains owner."""
    _terminate_replacement(replacement)


def _terminate_replacement(replacement: ApiReplacement) -> None:
    """Terminate and reap only the direct child represented by this Popen handle."""
    replacement.process.poll()
    if replacement.process.returncode is not None:
        replacement.process.wait(timeout=0)
        return
    child_identity = replacement.child_identity or process_identity(replacement.process.pid)
    if child_identity is not None:
        signal_exact_process(child_identity, signal.SIGTERM)
    else:
        replacement.process.terminate()
    try:
        replacement.process.wait(timeout=2.0)
        return
    except subprocess.TimeoutExpired:
        pass
    if child_identity is not None and exact_process_alive(child_identity):
        signal_exact_process(child_identity, signal.SIGKILL)
    elif child_identity is None:
        replacement.process.kill()
    try:
        replacement.process.wait(timeout=2.0)
    except subprocess.TimeoutExpired as exc:
        raise ApiReplacementStartupError(
            f"Replacement API child PID {replacement.process.pid} could not be reaped"
        ) from exc
