"""Reentrant thread- and process-safe advisory file locks."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import os
import threading
from typing import Iterator

try:
    import fcntl
except ImportError:  # pragma: no cover - PBGui production targets Linux
    fcntl = None


_LOCKS_GUARD = threading.Lock()
_THREAD_LOCKS: dict[str, threading.RLock] = {}
_HELD_LOCKS = threading.local()


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
    lock_path.parent.mkdir(parents=True, exist_ok=True)

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
