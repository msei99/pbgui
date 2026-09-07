"""Timed reentrant advisory locks for PBGui service lifecycle actions."""

from __future__ import annotations

import os
from pathlib import Path
import re
import threading
import time

try:
    import fcntl
except ImportError:  # pragma: no cover - PBGui production targets Linux
    fcntl = None

from secure_files import ensure_private_directory


_SERVICE_NAME_RE = re.compile(r"^[a-z0-9-]+$")
_LOCKS_GUARD = threading.Lock()
_THREAD_LOCKS: dict[str, threading.Lock] = {}
_HELD_LOCKS: dict[str, dict] = {}


class ServiceLifecycleBusyError(RuntimeError):
    """Raised when another action retains a service lifecycle lease."""


class ServiceLifecycleLease:
    """Own one reentrant thread and cross-process service lifecycle lease."""

    def __init__(self, key: str, thread_lock: threading.Lock) -> None:
        self._key = key
        self._thread_lock = thread_lock
        self._released = False

    def release(self) -> None:
        """Release this lease once; repeated calls are harmless."""
        if self._released:
            return
        self._released = True
        lock_file = None
        with _LOCKS_GUARD:
            entry = _HELD_LOCKS.get(self._key)
            if entry is None:
                return
            entry["depth"] -= 1
            if entry["depth"] == 0:
                _HELD_LOCKS.pop(self._key, None)
                lock_file = entry["file"]
        if lock_file is not None:
            if fcntl is not None:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
            lock_file.close()
            self._thread_lock.release()

    def __enter__(self) -> "ServiceLifecycleLease":
        return self

    def detach(self) -> None:
        """Prevent further owner-thread reentry before asynchronous handoff."""
        with _LOCKS_GUARD:
            entry = _HELD_LOCKS.get(self._key)
            if entry is not None:
                entry["owner_thread"] = None

    def __exit__(self, exc_type, exc, traceback) -> None:
        del exc_type, exc, traceback
        self.release()

    def __del__(self) -> None:  # pragma: no cover - deterministic callers use release
        self.release()


def _thread_lock(key: str) -> threading.Lock:
    """Return the process-local mutex for one service lock path."""
    with _LOCKS_GUARD:
        return _THREAD_LOCKS.setdefault(key, threading.Lock())


def _busy_reason(lock_path: Path, service: str, timeout: float) -> str:
    """Build a useful contention reason without trusting lock metadata."""
    owner = ""
    try:
        owner = lock_path.read_text(encoding="utf-8").strip()
    except OSError:
        pass
    suffix = f" Current owner: {owner}." if owner else ""
    return (
        f"Another lifecycle action for {service} is still running after "
        f"{max(0.0, timeout):.1f}s.{suffix} Wait for it to finish and retry."
    )


def acquire_service_lifecycle_lock(
    pbgui_dir: Path,
    service: str,
    action: str,
    *,
    timeout: float = 5.0,
) -> ServiceLifecycleLease:
    """Acquire a bounded reentrant lifecycle lease for one PBGui service."""
    if fcntl is None:  # pragma: no cover - PBGui production targets Linux
        raise RuntimeError("Service lifecycle locking requires fcntl support.")
    if not _SERVICE_NAME_RE.fullmatch(service):
        raise ValueError(f"Invalid service name: {service}")

    lock_dir = ensure_private_directory(Path(pbgui_dir).expanduser().resolve(strict=False) / "data" / "locks")
    lock_path = lock_dir / f"service-{service}.lock"
    key = str(lock_path)
    deadline = time.monotonic() + max(0.0, timeout)
    thread_lock = _thread_lock(key)
    thread_id = threading.get_ident()
    with _LOCKS_GUARD:
        entry = _HELD_LOCKS.get(key)
        if entry is not None and entry["owner_thread"] == thread_id:
            entry["depth"] += 1
            return ServiceLifecycleLease(key, thread_lock)
    if not thread_lock.acquire(timeout=max(0.0, deadline - time.monotonic())):
        raise ServiceLifecycleBusyError(_busy_reason(lock_path, service, timeout))

    lock_fd = None
    lock_file = None
    try:
        lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        lock_file = os.fdopen(lock_fd, "r+", encoding="utf-8")
        lock_fd = None
        os.chmod(lock_path, 0o600)
        while True:
            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError as exc:
                if time.monotonic() >= deadline:
                    raise ServiceLifecycleBusyError(_busy_reason(lock_path, service, timeout)) from exc
                time.sleep(min(0.05, max(0.0, deadline - time.monotonic())))

        lock_file.seek(0)
        lock_file.truncate()
        lock_file.write(f"pid={os.getpid()} action={action}")
        lock_file.flush()
        os.fsync(lock_file.fileno())
    except Exception:
        if lock_file is not None:
            lock_file.close()
        elif lock_fd is not None:
            os.close(lock_fd)
        thread_lock.release()
        raise
    with _LOCKS_GUARD:
        _HELD_LOCKS[key] = {"depth": 1, "file": lock_file, "owner_thread": thread_id}
    return ServiceLifecycleLease(key, thread_lock)
