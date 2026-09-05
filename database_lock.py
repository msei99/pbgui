"""Nonblocking cross-process maintenance leases for the local SQLite databases."""

import fcntl
import json
import os
import re
from pathlib import Path

from master_update_lock import MasterUpdateLease
from secure_files import PRIVATE_FILE_MODE, ensure_private_directory, read_regular_file_nofollow

SERVICE = "DatabaseLock"


class DatabaseBusyError(RuntimeError):
    """A restore/install conflicts with a database operation already in progress."""


def recovery_record(root: Path) -> dict | None:
    """Read the small fail-closed maintenance journal, never following a symlink."""
    path = Path(root) / "data" / "locks" / "db-tools-recovery.json"
    if not path.exists() and not path.is_symlink():
        return None
    try:
        data = read_regular_file_nofollow(path, path.parent)
        if len(data) > 65536:
            raise ValueError("Oversized recovery journal")
        record = json.loads(data)
        if (not isinstance(record, dict) or record.get("version") != 1
                or not isinstance(record.get("id"), str) or not re.fullmatch(r"[0-9a-f]{32}", record["id"])
                or record.get("phase") not in {"preparing", "mutating", "consistent"}
                or record.get("pbdata") not in {"none", "systemd", "legacy"}
                or not isinstance(record.get("databases"), dict)
                or set(record["databases"]) - {"pbgui.db", "pbgui_trades.db"}
                or any(type(value) is not bool for value in record["databases"].values())
                or not isinstance(record.get("touched"), list)
                or any(not isinstance(name, str) or name not in record["databases"] for name in record["touched"])):
            raise ValueError("Unsupported recovery journal")
        return record
    except Exception as exc:
        raise DatabaseBusyError("Invalid DB Tools recovery journal; preserve it and repair recovery before writing") from exc


def acquire_database_lock(root: Path, *, exclusive: bool = False, recovery: bool = False) -> MasterUpdateLease:
    """Lease DB maintenance independently of API restart or master-update admission.

    Readers here are complete application operations, not SQLite transactions.
    Independent scans/syncs hold shared leases; restore and file installs require
    exclusive admission. Nested shared leases are safe on separate descriptors.
    Only the recovery owner may bypass a pending journal, and only with EX.
    """
    if (Path(root) / "data").is_symlink():
        raise DatabaseBusyError("Database runtime directory must not be a symlink")
    directory = ensure_private_directory(Path(root) / "data" / "locks")
    path = directory / "database-maintenance.lock"
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, PRIVATE_FILE_MODE)
    handle = os.fdopen(fd, "r+b")
    try:
        os.fchmod(fd, PRIVATE_FILE_MODE)
        mode = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
        fcntl.flock(fd, mode | fcntl.LOCK_NB)
        if recovery and not exclusive:
            raise ValueError("Recovery admission requires an exclusive lease")
        if not recovery:
            pending = recovery_record(root)
            if pending and (exclusive or pending.get("phase") != "consistent"):
                raise DatabaseBusyError("DB Tools recovery pending; run DB Tools maintenance recovery before writing or restarting")
    except BlockingIOError as exc:
        handle.close()
        raise DatabaseBusyError("A database scan, sync, restore or install is already running") from exc
    except BaseException:
        handle.close()
        raise
    return MasterUpdateLease(handle, path)
