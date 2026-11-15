"""Reentrant thread- and process-safe advisory file locks."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import os
import math
import stat
import threading
import time
from typing import Iterator

try:
    import fcntl
except ImportError:  # pragma: no cover - PBGui production targets Linux
    fcntl = None


_LOCKS_GUARD = threading.Lock()
_THREAD_LOCKS: dict[str, threading.RLock] = {}
_HELD_LOCKS = threading.local()
_HELD_DIRECTORY_LOCKS = threading.local()


@contextmanager
def advisory_directory_lock(directory_fd: int, *, cancel_requested=None,
                            timeout: float | None = None) -> Iterator[None]:
    """Lock a directory inode without a pathname, reentrantly within one thread.

    Open a separate file description even when threads share the input FD, so
    flock serializes them too. Only active ownership is retained; forked children
    cannot inherit the parent's reentrancy marker. The caller keeps its own FD.
    All cooperating writers must use this lock; same-UID external editors need
    not honor it and are outside that guarantee.
    """
    if fcntl is None or type(directory_fd) is not int or directory_fd < 0:
        raise RuntimeError("Directory locking requires a POSIX directory descriptor")
    if cancel_requested is not None and not callable(cancel_requested):
        raise ValueError("Directory cancellation must be callable")
    if timeout is not None and (isinstance(timeout, bool) or not isinstance(timeout, (int, float))
                                or not 0 < timeout <= 60 or not math.isfinite(timeout)):
        raise ValueError("Directory timeout must be positive and at most 60 seconds")
    owned_fd = os.open(".", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory_fd)
    try:
        info = os.fstat(owned_fd)
        if not stat.S_ISDIR(info.st_mode):
            raise ValueError("Directory locking requires a directory")
        pid = os.getpid()
        if getattr(_HELD_DIRECTORY_LOCKS, "pid", None) != pid:
            _HELD_DIRECTORY_LOCKS.pid = pid
            _HELD_DIRECTORY_LOCKS.entries = set()
        held = _HELD_DIRECTORY_LOCKS.entries
        key = (info.st_dev, info.st_ino)
        if key in held:
            if cancel_requested is not None and cancel_requested():
                raise InterruptedError("Directory lock cancelled")
            yield
            return
        if cancel_requested is None and timeout is None:
            fcntl.flock(owned_fd, fcntl.LOCK_EX)
        else:
            deadline = time.monotonic() + timeout if timeout is not None else None
            while True:
                if cancel_requested is not None and cancel_requested():
                    raise InterruptedError("Directory lock cancelled")
                try:
                    fcntl.flock(owned_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if deadline is not None and time.monotonic() >= deadline:
                        raise TimeoutError("Directory lock is busy")
                    time.sleep(0.02)
        try:
            if cancel_requested is not None and cancel_requested():
                raise InterruptedError("Directory lock cancelled")
            held.add(key)
            yield
        finally:
            held.discard(key)
            fcntl.flock(owned_fd, fcntl.LOCK_UN)
    finally:
        os.close(owned_fd)


def _thread_lock(key: str) -> threading.RLock:
    """Return the process-local reentrant lock for one target path."""
    with _LOCKS_GUARD:
        return _THREAD_LOCKS.setdefault(key, threading.RLock())


@contextmanager
def advisory_file_lock(target: Path) -> Iterator[None]:
    """Serialize a file transaction across threads and PBGui processes."""
    target_path = Path(target).expanduser().resolve(strict=False)
    key = str(target_path)
    lock_path = target_path.with_name(f"{target_path.name}.lock")
    parent = lock_path.parent
    if not parent.is_dir():
        parent.mkdir(parents=True, exist_ok=True)

    with _thread_lock(key):
        held = getattr(_HELD_LOCKS, "entries", None)
        if held is None:
            held = {}
            _HELD_LOCKS.entries = held
        if key in held:
            held[key]["depth"] += 1
            try:
                yield
            finally:
                held[key]["depth"] -= 1
            return

        lock_file = lock_path.open("a+b")
        try:
            os.chmod(lock_path, 0o600)
            if fcntl is not None:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            held[key] = {"depth": 1, "file": lock_file}
            yield
        finally:
            held.pop(key, None)
            if fcntl is not None:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
            lock_file.close()
