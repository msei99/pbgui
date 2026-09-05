"""Nonblocking cross-process maintenance leases for the local SQLite databases."""

import fcntl
import os
from pathlib import Path

from master_update_lock import MasterUpdateLease
from secure_files import PRIVATE_FILE_MODE, ensure_private_directory

SERVICE = "DatabaseLock"


class DatabaseBusyError(RuntimeError):
    """A restore/install conflicts with a database operation already in progress."""


def acquire_database_lock(root: Path, *, exclusive: bool = False) -> MasterUpdateLease:
    """Lease DB maintenance independently of API restart or master-update admission.

    Readers here are complete application operations, not SQLite transactions.
    Independent scans/syncs hold shared leases; restore and file installs require
    exclusive admission. Nested shared leases are safe on separate descriptors.
    """
    directory = ensure_private_directory(Path(root) / "data" / "locks")
    path = directory / "database-maintenance.lock"
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, PRIVATE_FILE_MODE)
    handle = os.fdopen(fd, "r+b")
    try:
        os.fchmod(fd, PRIVATE_FILE_MODE)
        mode = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
        fcntl.flock(fd, mode | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        handle.close()
        raise DatabaseBusyError("A database scan, sync, restore or install is already running") from exc
    except BaseException:
        handle.close()
        raise
    return MasterUpdateLease(handle, path)
