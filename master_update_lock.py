"""Cross-process lease preventing concurrent local master updates."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import time

try:
    import fcntl
except ImportError:  # pragma: no cover - PBGui production targets Linux
    fcntl = None


class MasterUpdateBusyError(RuntimeError):
    """Raised when another local master update already owns the lease."""


class MasterUpdateLease:
    """Owner for one nonblocking local master update file lock."""

    def __init__(self, lock_file, path: Path) -> None:
        self._lock_file = lock_file
        self.path = path

    def release(self) -> None:
        """Release the lease once; repeated calls are harmless."""
        lock_file = self._lock_file
        if lock_file is None:
            return
        self._lock_file = None
        if fcntl is not None:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        lock_file.close()

    def __enter__(self) -> "MasterUpdateLease":
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        del exc_type, exc, traceback
        self.release()


def _ensure_private_lock_directory(path: Path) -> Path:
    """Create one owner-only lock directory without external module dependencies."""
    target = Path(path)
    if target.is_symlink():
        raise RuntimeError(f"Master update lock directory must not be a symlink: {target}")
    target.mkdir(parents=True, exist_ok=True)
    if not target.is_dir():
        raise RuntimeError(f"Master update lock path is not a directory: {target}")
    if os.name == "posix":
        os.chmod(target, 0o700)
    return target


def _acquire_master_lock(pbgui_dir: Path, mode: int, busy_message: str) -> MasterUpdateLease:
    """Acquire one nonblocking lease on the shared local-master lock file."""
    if fcntl is None:  # pragma: no cover - PBGui production targets Linux
        raise RuntimeError("Local master update locking requires fcntl support.")
    lock_dir = Path(pbgui_dir).expanduser().resolve(strict=False) / "data" / "locks"
    _ensure_private_lock_directory(lock_dir)
    lock_path = lock_dir / "master-update.lock"
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    lock_file = os.fdopen(lock_fd, "r+", encoding="utf-8")
    os.chmod(lock_path, 0o600)
    try:
        fcntl.flock(lock_file.fileno(), mode | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        lock_file.close()
        raise MasterUpdateBusyError(busy_message) from exc
    if mode == fcntl.LOCK_EX:
        lock_file.seek(0)
        lock_file.truncate()
        lock_file.write(f"pid={os.getpid()}\n")
        lock_file.flush()
        os.fsync(lock_file.fileno())
    return MasterUpdateLease(lock_file, lock_path)


def acquire_master_update_lock(pbgui_dir: Path) -> MasterUpdateLease:
    """Acquire the exclusive nonblocking lease for a local PBGui master update."""
    return _acquire_master_lock(
        pbgui_dir,
        fcntl.LOCK_EX if fcntl is not None else 0,
        "Another local master update is already running. Wait for it to finish and retry.",
    )


def acquire_master_runtime_lock(pbgui_dir: Path) -> MasterUpdateLease:
    """Protect a short PB8 runtime launch from a concurrent local-master update."""
    return _acquire_master_lock(
        pbgui_dir,
        fcntl.LOCK_SH if fcntl is not None else 0,
        "PB8 is being installed or updated. The backtest remains queued until the update finishes.",
    )


def wait_for_master_update_barrier(pbgui_dir: Path, timeout: float = 120.0) -> None:
    """Wait until all short runtime-launch readers have crossed the update boundary."""
    deadline = time.monotonic() + max(0.0, timeout)
    while True:
        try:
            lease = acquire_master_update_lock(pbgui_dir)
        except MasterUpdateBusyError:
            if time.monotonic() >= deadline:
                raise MasterUpdateBusyError("Timed out waiting for PB8 runtime launches to finish.")
            time.sleep(0.05)
            continue
        lease.release()
        return


def acquire_pb8_update_writer(install_dir: Path, stale_after: float = 14_400.0) -> Path:
    """Atomically own one PB8 writer directory, recovering only old empty crash remnants."""
    owner_dir = Path(install_dir).expanduser().resolve(strict=False) / ".pbgui-pb8-update-active"
    if owner_dir.is_symlink():
        raise RuntimeError(f"PB8 update writer path must not be a symlink: {owner_dir}")
    try:
        owner_dir.mkdir(mode=0o700)
        return owner_dir
    except FileExistsError:
        try:
            age = max(0.0, time.time() - owner_dir.stat().st_mtime)
            if not owner_dir.is_dir() or any(owner_dir.iterdir()) or age < max(0.0, stale_after):
                raise RuntimeError("Another PB8 installation or update is already active.")
            owner_dir.rmdir()
            owner_dir.mkdir(mode=0o700)
            return owner_dir
        except OSError as exc:
            raise RuntimeError("Another PB8 installation or update is already active.") from exc


def release_pb8_update_writer(install_dir: Path) -> None:
    """Release one empty PB8 writer directory; repeated cleanup is harmless."""
    owner_dir = Path(install_dir).expanduser().resolve(strict=False) / ".pbgui-pb8-update-active"
    try:
        owner_dir.rmdir()
    except FileNotFoundError:
        pass


def main() -> int:
    """Expose the runtime-launch barrier to remote update playbooks."""
    parser = argparse.ArgumentParser(description=__doc__)
    operations = parser.add_mutually_exclusive_group(required=True)
    operations.add_argument("--barrier", type=Path)
    operations.add_argument("--acquire-writer", type=Path)
    operations.add_argument("--release-writer", type=Path)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--stale-after", type=float, default=14_400.0)
    args = parser.parse_args()
    if args.barrier is not None:
        wait_for_master_update_barrier(args.barrier, args.timeout)
    elif args.acquire_writer is not None:
        acquire_pb8_update_writer(args.acquire_writer, args.stale_after)
    else:
        release_pb8_update_writer(args.release_writer)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
