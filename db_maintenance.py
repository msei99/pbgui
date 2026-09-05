"""DB Tools target-owned maintenance and durable, compensating bundle recovery.

Only the two PBGui databases are covered. Existing SQLite files are restored in
place. A journal survives process death; ordinary writer admission fails closed
until explicit recovery completes. This is not cross-file SQLite crash atomicity.
"""

from __future__ import annotations

import json
import fcntl
import os
import re
import signal
import sqlite3
import stat
import subprocess
import threading
from contextlib import ExitStack, closing
from pathlib import Path

from database_lock import DatabaseBusyError, acquire_database_lock, recovery_record
from master_update_lock import MasterUpdateLease, acquire_master_runtime_lock
from secure_files import atomic_write_private_text, ensure_private_directory, read_regular_file_nofollow
from sqlite_backup import backup_sqlite_database, restore_sqlite_backup, validate_sqlite_snapshot

SERVICE = "DbTools"
PROTOCOL = 1
DB_NAMES = ("pbgui.db", "pbgui_trades.db")


class MaintenanceCancelled(RuntimeError):
    """An owned cancellation request requires rollback before releasing admission."""


def _identifier(value: str) -> str:
    """Validate persisted operation identifiers before deriving filesystem paths."""
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{32}", value):
        raise ValueError("Invalid maintenance operation identifier")
    return value


def _sync_directory(path: Path) -> None:
    """Persist journal publication/deletion ordering through a crash."""
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _private_directory(root: Path, parts) -> Path:
    """Reject ancestor symlinks and make newly created recovery directories durable."""
    current = root
    for part in parts:
        child = current / part
        created = not child.exists()
        ensure_private_directory(child)
        if created:
            _sync_directory(current)
        current = child
    return current


def _owner_lock(root: Path, name="db-tools-owner.lock"):
    """Serialize recovery owners through the post-EX PBData restart phase as well."""
    directory = _private_directory(root, ("data", "locks"))
    path = directory / name
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    handle = os.fdopen(fd, "r+b")
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BaseException as exc:
        handle.close()
        if isinstance(exc, BlockingIOError):
            raise DatabaseBusyError("A DB Tools maintenance/recovery owner is already active") from exc
        raise
    return MasterUpdateLease(handle, path)


def _cleanup_sqlite_staging(directory: Path) -> None:
    """Remove only private SQLite-helper remnants inside the pinned recovery directory.

    A process can die inside TemporaryDirectory, before its normal cleanup runs.
    Do not recursively delete arbitrary contents or follow a replaced directory.
    """
    from sqlite_backup import _pin

    allowed = {"snapshot.db", "snapshot.db-journal", "snapshot.db-wal", "snapshot.db-shm"}
    with _pin(directory, directory=True) as (parent_fd, _):
        for name in os.listdir(parent_fd):
            if not re.fullmatch(r"\.sqlite-(?:backup|restore)-[a-z0-9_]{8}", name):
                continue
            before = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            if (not stat.S_ISDIR(before.st_mode) or before.st_uid != os.geteuid()
                    or stat.S_IMODE(before.st_mode) != 0o700):
                raise DatabaseBusyError("Untrusted SQLite staging remnant; preserve it for inspection")
            fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent_fd)
            try:
                if not os.path.samestat(before, os.fstat(fd)):
                    raise DatabaseBusyError("SQLite staging ownership changed during cleanup")
                entries = os.listdir(fd)
                for entry in entries:
                    info = os.stat(entry, dir_fd=fd, follow_symlinks=False)
                    if (entry not in allowed or not stat.S_ISREG(info.st_mode)
                            or info.st_uid != os.geteuid() or info.st_mode & 0o077):
                        raise DatabaseBusyError("Unexpected SQLite staging contents; preserve them for inspection")
                for entry in entries:
                    os.unlink(entry, dir_fd=fd)
                if not os.path.samestat(before, os.stat(name, dir_fd=parent_fd, follow_symlinks=False)):
                    raise DatabaseBusyError("SQLite staging directory changed during cleanup")
                os.rmdir(name, dir_fd=parent_fd)
            finally:
                os.close(fd)
        os.fsync(parent_fd)


class PBDataControl:
    """Control only the local PBData service or verified legacy PBData process."""

    def __init__(self, root: Path):
        """Record the approved installation root; do not construct PBData/credentials."""
        self.root = Path(root).resolve()

    def _service_state(self) -> str:
        """Distinguish a stopped service from a failed status query."""
        result = self._service_command("show", "--property=LoadState,ActiveState", timeout=15)
        state = dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)
        if result.returncode in {0, 4} and state.get("LoadState") == "not-found":
            return "inactive"
        if result.returncode:
            raise DatabaseBusyError("Cannot verify PBData service state; repair the user service manager first")
        active = state.get("ActiveState")
        if active not in {"active", "inactive", "failed"}:
            raise DatabaseBusyError("PBData service is changing state; retry after it settles")
        return active

    def _service_command(self, action: str, *options: str, timeout=60):
        """Translate unavailable/timed-out service control into failed admission."""
        try:
            return subprocess.run(["systemctl", "--user", action, "pbgui-pbdata.service", *options],
                                  capture_output=True, text=True, timeout=timeout)
        except (OSError, subprocess.SubprocessError) as exc:
            raise DatabaseBusyError(f"Cannot confirm PBData service {action}; repair service control before maintenance") from exc

    def _legacy(self):
        """Resolve a PID only after checking exact script identity and installation."""
        import psutil

        path = self.root / "data" / "pid" / "pbdata.pid"
        if not path.exists() and not path.is_symlink():
            return None
        raw = read_regular_file_nofollow(path, path.parent).decode("ascii").strip()
        if not raw.isdecimal() or int(raw) <= 1:
            raise DatabaseBusyError("Invalid PBData PID file; cannot confirm writer shutdown")
        try:
            process = psutil.Process(int(raw))
            argv = process.cmdline()
            cwd = Path(process.cwd()).resolve()
            scripts = [Path(arg) if Path(arg).is_absolute() else cwd / arg for arg in argv[1:] if arg.endswith("PBData.py")]
            if not scripts or not any(path.resolve() == self.root / "PBData.py" for path in scripts):
                raise DatabaseBusyError("PBData PID identity does not match this installation")
            return process
        except psutil.NoSuchProcess:
            return None
        except psutil.AccessDenied as exc:
            raise DatabaseBusyError("Cannot verify PBData process identity; no process will be signalled") from exc

    def inspect(self) -> str:
        """Return the restart ownership marker before any stop side effect."""
        if self._service_state() == "active":
            return "systemd"
        return "legacy" if self._legacy() is not None else "none"

    def stop(self, marker: str) -> None:
        """Stop and positively verify termination; never silently treat failure as stopped."""
        if marker == "systemd":
            result = self._service_command("stop")
            if result.returncode:
                raise DatabaseBusyError("PBData service stop failed; no database mutation was admitted")
        process = self._legacy()
        if process is not None:
            import psutil

            try:
                process.terminate()
                process.wait(timeout=60)
            except psutil.NoSuchProcess:
                pass
            except psutil.TimeoutExpired as exc:
                raise DatabaseBusyError("PBData did not terminate; database maintenance remains blocked") from exc
            except psutil.AccessDenied as exc:
                raise DatabaseBusyError("PBData termination was denied; database maintenance remains blocked") from exc
        if self._service_state() == "active" or self._legacy() is not None:
            raise DatabaseBusyError("PBData is still running; database maintenance remains blocked")

    def start(self, marker: str) -> None:
        """Resume only the previously running writer after consistent-state publication."""
        if marker == "none":
            return
        if self.inspect() != "none":
            return
        if marker == "systemd":
            result = self._service_command("start")
            if result.returncode or self._service_state() != "active":
                raise DatabaseBusyError("Databases are consistent but PBData restart failed; retry recovery")
        elif marker == "legacy":
            # The existing launcher owns detached legacy daemons and validates its PID.
            from PBData import PBData

            # Use the existing PID-checked launcher, not the resource-heavy constructor.
            pbdata = PBData.__new__(PBData)
            pbdata.piddir = self.root / "data" / "pid"
            pbdata.pidfile = pbdata.piddir / "pbdata.pid"
            pbdata.my_pid = None
            pbdata.run()
            if not pbdata.is_running():
                raise DatabaseBusyError("Databases are consistent but legacy PBData restart failed; retry recovery")
        else:
            raise ValueError("Invalid PBData recovery marker")


class Maintenance:
    """One target operation owns admission, recovery material, and writer restart."""

    def __init__(self, root: Path, operation_id: str, *, control=None, cancel=None):
        """Bind explicit lifecycle dependencies for production and isolated tests."""
        self.root = Path(root).resolve()
        self.id = _identifier(operation_id)
        self.control = control if control is not None else PBDataControl(self.root)
        self.cancel = cancel if cancel is not None else threading.Event()
        self.record = None
        self.lease = None
        self.resources = ExitStack()
        self.thread_id = None
        self.journal = self.root / "data" / "locks" / "db-tools-recovery.json"
        self.directory = self.root / "data" / "backup" / "db-tools" / f"recovery-{self.id}"

    def _save(self) -> None:
        """Durably record intent before stopping writers or touching any live database."""
        atomic_write_private_text(self.journal, json.dumps(self.record, indent=4))
        _sync_directory(self.journal.parent)

    def check(self) -> None:
        """Observe local cancellation or a validated remote owner's cancellation file."""
        path = self.root / "data" / "tmp" / "db-tools" / self.id / "cancel"
        if self.cancel.is_set() or path.exists() or path.is_symlink():
            raise MaintenanceCancelled("Database maintenance cancelled")

    def __enter__(self):
        """Reserve the target before inspecting/stopping PBData or creating backups."""
        stopped = False
        try:
            self.resources.enter_context(_owner_lock(self.root))
            self.resources.enter_context(acquire_master_runtime_lock(self.root))
            self.lease = self.resources.enter_context(acquire_database_lock(self.root, exclusive=True, recovery=True))
            self.thread_id = threading.get_ident()
            previous = recovery_record(self.root)
            if previous:
                raise DatabaseBusyError("DB Tools recovery pending; call /api/db-tools/maintenance/recover for this target")
            self.check()
            marker = self.control.inspect()
            self.check()
            self.record = {"version": PROTOCOL, "id": self.id, "phase": "preparing", "pbdata": marker,
                           "databases": {}, "touched": []}
            self._save()
            self.control.stop(marker)
            stopped = True
            self.check()
            _private_directory(self.root, ("data", "backup", "db-tools", self.directory.name))
            return self
        except BaseException:
            try:
                if stopped:
                    self.finish()
            finally:
                self.resources.close()
            raise

    def assert_target(self, paths: dict[str, Path]) -> None:
        """An internal SQL helper may bypass admission only for this live, prepared owner."""
        if (self.thread_id != threading.get_ident() or self.lease is None
                or self.lease._lock_file is None or self.record is None
                or self.record["phase"] != "mutating" or set(paths) != set(DB_NAMES)
                or not set(paths).issubset(self.record["touched"])
                or any(Path(path).resolve() != self.root / "data" / name for name, path in paths.items())):
            raise DatabaseBusyError("SQL mutation requires its active, prepared target maintenance owner")
        self.check()

    def prepare(self, staged: dict[str, Path]) -> dict[str, Path]:
        """Make all undo/redo snapshots durable before any destructive operation."""
        if set(staged) - set(DB_NAMES):
            raise ValueError("Unsupported bundle database")
        prepared = {}
        for name in DB_NAMES:
            self.check()
            live = self.root / "data" / name
            if live.is_symlink():
                raise ValueError("Live database must not be a symlink")
            exists = live.exists()
            if exists:
                backup_sqlite_database(live, self.directory / f"original-{name}")
                os.link(self.directory / f"original-{name}", self.directory.parent / f"db-tools-{self.id}-before-{name}")
                _sync_directory(self.directory.parent)
            self.record["databases"][name] = exists
        for name, source in staged.items():
            self.check()
            snapshot = self.directory / f"prepared-{name}"
            backup_sqlite_database(Path(source), snapshot)
            validate_sqlite_snapshot(snapshot, main=name == "pbgui.db")
            prepared[name] = snapshot
        self._save()
        return prepared

    def touch(self, names) -> None:
        """Persist the complete possible write set before entering SQLite code."""
        self.check()
        names = list(names)
        if set(names) - set(self.record["databases"]):
            raise ValueError("Database was not prepared")
        self.record["phase"] = "mutating"
        self.record["touched"] = list(dict.fromkeys(self.record["touched"] + names))
        self._save()

    def install(self, staged: dict[str, Path]) -> None:
        """Install the prepared bundle in place, including originally absent DB files."""
        prepared = self.prepare(staged)
        for name, snapshot in prepared.items():
            self.touch([name])
            live = self.root / "data" / name
            if not live.exists():
                fd = os.open(live, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
                os.close(fd)
                with closing(sqlite3.connect(live)) as conn:
                    conn.execute("PRAGMA user_version=0")
                _sync_directory(live.parent)
            restore_sqlite_backup(snapshot, live, self.directory)
            self.check()

    def rollback(self) -> None:
        """Try every undo entry, retaining the journal if even one cannot be restored."""
        failures = []
        for name in reversed(self.record["touched"]):
            try:
                live = self.root / "data" / name
                if live.is_symlink():
                    raise ValueError("Live database became a symlink")
                if self.record["databases"][name]:
                    restore_sqlite_backup(self.directory / f"original-{name}", live, self.directory,
                                          preserve_live_schema=False)
                else:
                    for suffix in ("-wal", "-shm", "-journal", ""):
                        live.with_name(name + suffix).unlink(missing_ok=True)
                    _sync_directory(live.parent)
            except Exception as exc:
                failures.append(f"{name}: {type(exc).__name__}: {exc}")
        if failures:
            raise DatabaseBusyError("Bundle rollback incomplete; writers remain blocked. " + "; ".join(failures))

    def finish(self) -> None:
        """Publish consistency before releasing DB EX and resuming the previous writer."""
        self.record["phase"] = "consistent"
        self._save()
        self.lease.release()
        # A pending consistent record admits SH startup, but denies new EX work.
        self.control.start(self.record["pbdata"])
        current = recovery_record(self.root)
        if current != self.record:
            raise DatabaseBusyError("Recovery ownership changed; refusing to clear journal")
        current_path = self.root
        for part in self.directory.relative_to(self.root).parts:
            current_path /= part
            if current_path.is_symlink():
                raise DatabaseBusyError("Recovery path became a symlink; refusing cleanup")
        if self.directory.exists():
            if self.directory.is_symlink():
                raise DatabaseBusyError("Recovery directory became a symlink; refusing cleanup")
            _cleanup_sqlite_staging(self.directory)
            for name in DB_NAMES:
                for prefix in ("original-", "prepared-"):
                    (self.directory / f"{prefix}{name}").unlink(missing_ok=True)
            self.directory.rmdir()
            _sync_directory(self.directory.parent)
        self.journal.unlink()
        _sync_directory(self.journal.parent)

    def __exit__(self, exc_type, error, tb):
        """Rollback on exceptions/cancellation, never restart an unresolved mixed bundle."""
        try:
            if error is not None:
                self.rollback()
            self.finish()
        finally:
            self.resources.close()

    def backups(self) -> list[str]:
        """Return retained recovery snapshots indexed by their actual database name."""
        return [str(self.directory.parent / f"db-tools-{self.id}-before-{name}")
                for name, exists in self.record["databases"].items() if exists]


def recover(root: Path, *, control=None) -> dict:
    """Explicitly resume a validated interrupted operation; no requested new mutation."""
    root = Path(root).resolve()
    with ExitStack() as resources:
        resources.enter_context(_owner_lock(root))
        resources.enter_context(acquire_master_runtime_lock(root))
        lease = resources.enter_context(acquire_database_lock(root, exclusive=True, recovery=True))
        record = recovery_record(root)
        if not record:
            return {"ok": True, "recovered": False}
        operation = Maintenance(root, _identifier(record.get("id")), control=control)
        operation.record, operation.lease = record, lease
        if (record.get("phase") not in {"preparing", "mutating", "consistent"}
                or record.get("pbdata") not in {"none", "systemd", "legacy"}
                or not isinstance(record.get("databases"), dict)
                or set(record["databases"]) - set(DB_NAMES)
                or any(type(value) is not bool for value in record["databases"].values())
                or not isinstance(record.get("touched"), list)
                or any(name not in record["databases"] for name in record["touched"])):
            raise DatabaseBusyError("Invalid DB Tools recovery state; preserve snapshots and journal")
        if record["phase"] != "consistent":
            operation.control.stop(record["pbdata"])
            operation.rollback()
        operation.finish()
        return {"ok": True, "recovered": True, "backups": operation.backups()}


def probe(root: Path, *, recovery=False) -> None:
    """Check admission without service changes; the actual worker must reacquire it."""
    root = Path(root).resolve()
    with _owner_lock(root), acquire_master_runtime_lock(root), acquire_database_lock(root, exclusive=True, recovery=recovery):
        recovery_record(root)


def request_cancel(root: Path, operation_id: str) -> None:
    """Signal one remote owner without taking its database lease or changing DB state."""
    path = _private_directory(Path(root).resolve(), ("data", "tmp", "db-tools", _identifier(operation_id)))
    atomic_write_private_text(path / "cancel", "cancel\n")


def clear_cancel(root: Path, operation_id: str) -> None:
    """Remove only a settled owner's cancellation flag, never arbitrary staged data."""
    directory = Path(root) / "data" / "tmp" / "db-tools" / _identifier(operation_id)
    if directory.is_symlink():
        raise ValueError("Cancellation directory must not be a symlink")
    if directory.exists():
        (directory / "cancel").unlink(missing_ok=True)
        directory.rmdir()


def run(root: Path, request: dict, *, cancel=None, control=None, progress=None) -> dict:
    """Execute one complete local or remote DB Tools operation under target admission."""
    if request.get("kind") == "recover":
        return recover(root, control=control)
    kind = request.get("kind")
    if kind not in {"install", "cleanup", "copy"}:
        raise ValueError("Unsupported database maintenance operation")
    staged = {name: Path(path) for name, path in request.get("staged", {}).items()}
    if kind == "copy" and (set(staged) != set(DB_NAMES) or request.get("mode") not in {"replace", "add_missing"}):
        raise ValueError("User copy requires both source databases and a supported copy mode")
    if kind in {"copy", "cleanup"}:
        users = request.get("users")
        if not isinstance(users, list) or not users or any(
            not isinstance(user, str) or not user.strip() or user in {".", ".."}
            or any(char in user for char in ("/", "\\", "\x00")) for user in users
        ):
            raise ValueError("Select valid users before database maintenance")
    with Maintenance(root, request["id"], control=control, cancel=cancel) as owner:
        if kind == "install":
            owner.install(staged)
            result = {"restored": list(staged)}
        else:
            from api.db_tools import copy_user_rows, delete_user_rows

            prepared = owner.prepare(staged)
            owner.touch(DB_NAMES)
            paths = {name: owner.root / "data" / name for name in DB_NAMES}
            if kind == "cleanup":
                result = {"deleted": delete_user_rows(paths, request["users"], request.get("cutoff_ms"), progress, _maintenance=owner)}
            else:
                result = {"copied": copy_user_rows(prepared, paths, request["users"], request["mode"], progress, _maintenance=owner)}
            owner.check()
        result.update(ok=True, backups=owner.backups(), pbdata_was_running=owner.record["pbdata"] != "none")
    return result


def remote_main(request: dict) -> dict:
    """Fail closed on old guards and own rollback when an SSH channel is terminated."""
    check_remote_capability()
    staged = request.get("staged", {})
    approved = Path.cwd() / "data" / "tmp" / "db-tools"
    for name, raw_path in staged.items():
        path = Path(raw_path)
        if not path.is_absolute():
            # SSH paths are relative to HOME, whereas this helper runs in PBGui.
            path = Path.home() / path
        path = path.resolve()
        if name not in DB_NAMES or path.name != name or not path.is_relative_to(approved.resolve()):
            raise ValueError("Remote maintenance source must be an uploaded DB Tools snapshot")
        staged[name] = str(path)
    cancelled = threading.Event()
    previous = {}
    try:
        for signum in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT):
            previous[signum] = signal.signal(signum, lambda *_: cancelled.set())
        return run(Path.cwd(), request, cancel=cancelled)
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


def check_remote_capability() -> None:
    """Reject old checkouts and API processes started before the installed guards."""
    try:
        from Database import DB_MAINTENANCE_PROTOCOL as database_protocol
        from PBData import DB_MAINTENANCE_PROTOCOL as pbdata_protocol
    except ImportError as exc:
        raise DatabaseBusyError("Update and restart remote PBGui/PBData before database maintenance; writer guards are missing") from exc

    if database_protocol != PROTOCOL or pbdata_protocol != PROTOCOL:
        raise DatabaseBusyError("Update and restart the remote PBGui/PBData installation before database maintenance")
    import psutil

    root = Path.cwd().resolve()
    installed = max((root / name).stat().st_mtime for name in ("Database.py", "database_lock.py", "db_maintenance.py"))
    for process in psutil.process_iter(["pid", "uids"]):
        try:
            uids = process.info.get("uids")
            if process.pid == os.getpid() or (uids is not None and uids.real != os.getuid()):
                continue
            cwd = Path(process.cwd()).resolve()
            args = process.cmdline()[1:]
            scripts = [Path(arg) if Path(arg).is_absolute() else cwd / arg
                       for arg in args if Path(arg).name == "PBApiServer.py"]
            is_api = any(path.resolve() == root / "PBApiServer.py" for path in scripts)
            is_api = is_api or (cwd == root and any(arg in {"PBApiServer", "PBApiServer:app"} for arg in args))
            if is_api and process.create_time() < installed:
                raise DatabaseBusyError("Restart the updated remote PBGui API before database maintenance; its writer guards are not loaded")
        except psutil.NoSuchProcess:
            continue
        except psutil.AccessDenied as exc:
            raise DatabaseBusyError("Cannot verify remote API process guards; check process inspection permissions") from exc


def main() -> int:
    """Provide offline operator recovery when the pending journal blocks API startup."""
    import argparse
    import traceback

    parser = argparse.ArgumentParser(description="Recover interrupted PBGui database maintenance; never deletes the safety backups.")
    parser.add_argument("action", choices=["recover"])
    parser.parse_args()
    from logging_helpers import human_log

    try:
        result = recover(Path(__file__).resolve().parent)
    except Exception as exc:
        human_log(SERVICE, f"Database recovery failed: {exc}", level="ERROR", meta={"traceback": traceback.format_exc()})
        return 1
    human_log(SERVICE, f"Database recovery completed; recovered={result['recovered']}", level="INFO")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
