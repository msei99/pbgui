"""FastAPI router: database cleanup and migration tools."""
from __future__ import annotations

import asyncio
import json
import os
import shlex
import shutil
import sqlite3
import tempfile
import traceback
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from api.auth import SessionToken, require_auth
from logging_helpers import human_log as _log
from master.async_pool import remote_path_join
from pbgui_purefunc import PBGDIR

SERVICE = "DbTools"

router = APIRouter()

_monitor = None
_operations: dict[str, "OperationProgress"] = {}
_sync_jobs: dict[str, dict[str, Any]] = {}
_sync_scheduler_task: asyncio.Task | None = None
_sync_job_locks: set[str] = set()

MAIN_DB_NAME = "pbgui.db"
TRADES_DB_NAME = "pbgui_trades.db"
DB_FILE_NAMES = (MAIN_DB_NAME, TRADES_DB_NAME)


@dataclass(frozen=True)
class TableSpec:
    """Describes a user-owned SQLite table handled by the DB tools."""

    db_name: str
    table: str
    user_col: str = "user"
    timestamp_col: str | None = "timestamp"
    key_cols: tuple[str, ...] = ()


@dataclass
class OperationProgress:
    """Progress state for a running DB tools operation."""

    id: str
    kind: str
    total: int
    status: str = "running"
    completed: int = 0
    current: str = "Queued"
    result: dict[str, Any] | None = None
    error: str = ""
    steps: list[dict[str, Any]] | None = None
    created_at: str = ""
    updated_at: str = ""

    def __post_init__(self) -> None:
        now = datetime.now(timezone.utc).isoformat()
        self.created_at = now
        self.updated_at = now
        if self.steps is None:
            self.steps = []

    def set_current(self, label: str) -> None:
        self.current = label
        self.updated_at = datetime.now(timezone.utc).isoformat()

    def advance(self, label: str, detail: Any = None) -> None:
        self.completed = min(self.total, self.completed + 1)
        self.current = label
        self.updated_at = datetime.now(timezone.utc).isoformat()
        assert self.steps is not None
        self.steps.append({"label": label, "detail": detail, "completed": self.completed, "total": self.total})

    def finish(self, result: dict[str, Any]) -> None:
        self.status = "done"
        self.completed = self.total
        self.current = "Completed"
        self.result = result
        self.updated_at = datetime.now(timezone.utc).isoformat()

    def fail(self, error: str) -> None:
        self.status = "error"
        self.error = error
        self.current = "Failed"
        self.updated_at = datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "kind": self.kind,
            "status": self.status,
            "completed": self.completed,
            "total": self.total,
            "percent": int((self.completed / self.total) * 100) if self.total else 0,
            "current": self.current,
            "steps": list(self.steps or []),
            "result": self.result,
            "error": self.error,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


MAIN_TABLES: tuple[TableSpec, ...] = (
    TableSpec(MAIN_DB_NAME, "history", key_cols=("uniqueid",)),
    TableSpec(MAIN_DB_NAME, "position", key_cols=("user", "symbol", "side", "timestamp")),
    TableSpec(MAIN_DB_NAME, "orders", key_cols=("uniqueid",)),
    TableSpec(MAIN_DB_NAME, "prices", key_cols=("user", "symbol")),
    TableSpec(MAIN_DB_NAME, "balances", key_cols=("user",)),
    TableSpec(MAIN_DB_NAME, "history_scan_meta", timestamp_col="last_scan_ts", key_cols=("user", "exchange")),
)
TRADES_TABLES: tuple[TableSpec, ...] = (
    TableSpec(TRADES_DB_NAME, "executions", key_cols=("user", "exchange", "trade_id")),
)
TABLE_SPECS: tuple[TableSpec, ...] = MAIN_TABLES + TRADES_TABLES
APPEND_SYNC_TABLES = {(MAIN_DB_NAME, "history"), (TRADES_DB_NAME, "executions")}
SYNC_OVERLAP_MS = 5 * 60 * 1000


MAIN_SCHEMA: tuple[str, ...] = (
    """CREATE TABLE IF NOT EXISTS history (
            id INTEGER PRIMARY KEY,
            symbol TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            income REAL NOT NULL,
            uniqueid text NOT NULL UNIQUE,
            user TEXT NOT NULL
    );""",
    """CREATE TABLE IF NOT EXISTS position (
            id INTEGER PRIMARY KEY,
            symbol TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            psize REAL NOT NULL,
            upnl REAL NOT NULL,
            entry REAL NOT NULL,
            user TEXT NOT NULL,
            side TEXT
    );""",
    """CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY,
            symbol TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            amount REAL NOT NULL,
            price REAL NOT NULL,
            side TEXT NOT NULL,
            uniqueid text NOT NULL UNIQUE,
            user TEXT NOT NULL
    );""",
    """CREATE TABLE IF NOT EXISTS prices (
            id INTEGER PRIMARY KEY,
            symbol TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            price REAL NOT NULL,
            user TEXT NOT NULL
    );""",
    """CREATE TABLE IF NOT EXISTS balances (
            id INTEGER PRIMARY KEY,
            timestamp INTEGER NOT NULL,
            balance REAL NOT NULL,
            user TEXT NOT NULL UNIQUE
    );""",
    """CREATE TABLE IF NOT EXISTS history_scan_meta (
            user TEXT NOT NULL,
            exchange TEXT NOT NULL,
            last_scan_ts INTEGER NOT NULL,
            PRIMARY KEY (user, exchange)
    );""",
)
TRADES_SCHEMA: tuple[str, ...] = (
    """CREATE TABLE IF NOT EXISTS executions (
            id INTEGER PRIMARY KEY,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            side TEXT,
            price REAL,
            qty REAL,
            fee REAL,
            realized_pnl REAL,
            order_id TEXT,
            trade_id TEXT NOT NULL,
            user TEXT NOT NULL,
            raw_json TEXT,
            UNIQUE(user, exchange, trade_id)
    );""",
)


class TargetRef(BaseModel):
    """A DB tools target; `local` or a known remote master hostname."""

    target: str = "local"


class CleanupPreviewRequest(BaseModel):
    """Preview user-data cleanup."""

    target: str = "local"
    users: list[str] = Field(default_factory=list)
    cutoff_ms: int | None = None


class CleanupRunRequest(CleanupPreviewRequest):
    """Run user-data cleanup."""

    mode: Literal["all", "older"] = "all"


class CopyUsersRequest(BaseModel):
    """Copy selected users between known master targets."""

    source: str = "local"
    target: str = "local"
    users: list[str] = Field(default_factory=list)
    mode: Literal["replace", "add_missing"] = "add_missing"


class CopyDatabaseRequest(BaseModel):
    """Copy the complete DB files between known master targets."""

    source: str = "local"
    target: str = "local"


class DashboardCopyRequest(BaseModel):
    """Copy selected dashboard and template JSON files."""

    source: str = "local"
    target: str = "local"
    dashboards: list[str] = Field(default_factory=list)
    templates: list[str] = Field(default_factory=list)
    mode: Literal["replace_all", "add_missing"] = "add_missing"


class BackupActionRequest(BaseModel):
    """Restore or delete DB backup files for a target."""

    target: str = "local"
    backups: list[str] = Field(default_factory=list)


class SyncJobRequest(BaseModel):
    """Configure a one-way row-level user sync job."""

    id: str | None = None
    name: str = ""
    source: str = "local"
    targets: list[str] = Field(default_factory=list)
    users: list[str] = Field(default_factory=list)
    interval_seconds: int = 60
    enabled: bool = False


def init(monitor) -> None:
    """Inject the VPS monitor so this router can reuse its AsyncSSHPool."""

    global _monitor
    _monitor = monitor
    _load_sync_jobs()
    _ensure_sync_scheduler()


def _operation_total(kind: str, item_count: int = 0, *, replace: bool = False, remote_target: bool = False) -> int:
    """Return the number of concrete progress steps for an operation type."""

    db_install_steps = 2 + 1 + 2 + 1
    if kind == "cleanup":
        return 2 + len(TABLE_SPECS)
    if kind == "copy-users":
        replace_steps = len(TABLE_SPECS) if replace else 0
        upload_steps = 2 if remote_target else 0
        return 2 + upload_steps + 2 + replace_steps + len(TABLE_SPECS)
    if kind == "copy-database":
        return 2 + db_install_steps
    if kind == "copy-dashboards":
        return 1 + max(item_count, 1)
    if kind == "restore-backups":
        return 2 + db_install_steps + max(item_count, 1)
    if kind == "sync-job":
        return 2 + item_count
    return 1


def _start_operation(kind: str, total: int, runner) -> OperationProgress:
    """Start an async operation and return its progress object."""

    _cleanup_operations()
    operation = OperationProgress(id=uuid.uuid4().hex, kind=kind, total=max(int(total), 1))
    _operations[operation.id] = operation

    async def _run() -> None:
        try:
            result = await runner(operation)
            operation.finish(result)
        except HTTPException as exc:
            operation.fail(str(exc.detail))
            _log(SERVICE, f"operation {operation.kind} failed: {exc.detail}", level="WARNING")
        except Exception as exc:
            operation.fail(str(exc))
            _log(SERVICE, f"operation {operation.kind} failed: {exc}", level="ERROR", meta={"traceback": traceback.format_exc()})

    asyncio.create_task(_run(), name=f"db-tools-{kind}-{operation.id[:8]}")
    return operation


def _cleanup_operations() -> None:
    """Keep only recent operation records in memory."""

    if len(_operations) <= 50:
        return
    removable = sorted(_operations.values(), key=lambda item: item.updated_at)[: len(_operations) - 50]
    for item in removable:
        if item.status != "running":
            _operations.pop(item.id, None)


def _data_dir() -> Path:
    return Path(PBGDIR) / "data"


def _backup_dir() -> Path:
    return _data_dir() / "backup" / "db-tools"


def _backup_remote_dir(target: str) -> str:
    return _remote_path(target, "data", "backup", "db-tools")


def _sync_dir() -> Path:
    return _data_dir() / "db_sync"


def _sync_jobs_file() -> Path:
    return _sync_dir() / "jobs.json"


def _sync_job_log_slug(name: str) -> str:
    value = str(name or "").strip()
    chars: list[str] = []
    last_was_separator = False
    for char in value:
        if char.isascii() and (char.isalnum() or char in {"_", "-"}):
            chars.append(char)
            last_was_separator = False
        else:
            if not last_was_separator:
                chars.append("_")
            last_was_separator = True
    slug = "".join(chars).strip("_-")[:80]
    return slug or "sync-job"


def _sync_job_log_relative(job_id: str, job_name: str | None = None) -> str:
    job = _sync_jobs.get(str(job_id), {})
    name = str(job_name or job.get("name") or "sync-job")
    return f"jobs/db-tools-sync-{_sync_job_log_slug(name)}.log"


def _sync_job_log_path(job_id: str, job_name: str | None = None) -> Path:
    return _data_dir() / "logs" / _sync_job_log_relative(job_id, job_name)


def _monitor_snapshot_file() -> Path:
    return _data_dir() / "state" / "vps_monitor" / "snapshot.json"


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")


def _clean_users(users: list[str]) -> list[str]:
    result: list[str] = []
    for user in users or []:
        value = str(user or "").strip()
        if not value or value in result:
            continue
        if "/" in value or "\\" in value or "\x00" in value or value in {".", ".."}:
            raise HTTPException(status_code=400, detail=f"Invalid user name: {value}")
        result.append(value)
    return result


def _validate_item_name(name: str, kind: str) -> str:
    value = str(name or "").strip()
    if not value or "/" in value or "\\" in value or "\x00" in value or value in {".", ".."}:
        raise HTTPException(status_code=400, detail=f"Invalid {kind} name: {value}")
    return value


def _validate_backup_name(name: str) -> str:
    """Validate a DB tools backup filename and return it unchanged."""

    value = str(name or "").strip()
    if not value or "/" in value or "\\" in value or "\x00" in value or value in {".", ".."}:
        raise HTTPException(status_code=400, detail=f"Invalid backup name: {value}")
    if not any(value.endswith(db_name) for db_name in DB_FILE_NAMES):
        raise HTTPException(status_code=400, detail=f"Unsupported backup file: {value}")
    return value


def _backup_db_name(filename: str) -> str:
    for db_name in DB_FILE_NAMES:
        if filename.endswith(db_name):
            return db_name
    raise HTTPException(status_code=400, detail=f"Unsupported backup file: {filename}")


def _backup_label(filename: str, db_name: str) -> str:
    stem = filename[: -len(db_name)].rstrip("-")
    if stem.startswith("db-tools-"):
        parts = stem.split("-", 3)
        if len(parts) == 4:
            return parts[3]
    return stem


def _target_id(value: str) -> str:
    target = str(value or "local").strip()
    return "local" if target in {"", "local", "master"} else target


def _monitor_host_meta() -> dict[str, Any]:
    meta = getattr(getattr(_monitor, "store", None), "host_meta", {}) if _monitor is not None else {}
    if isinstance(meta, dict) and meta:
        return meta
    path = _monitor_snapshot_file()
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        _log(SERVICE, f"failed to read VPS monitor snapshot: {exc}", level="WARNING")
        return {}
    snapshot_meta = data.get("host_meta") if isinstance(data, dict) else {}
    return snapshot_meta if isinstance(snapshot_meta, dict) else {}


def _clean_targets(targets: list[str]) -> list[str]:
    result: list[str] = []
    for item in targets or []:
        target = _target_id(item)
        if target not in result:
            result.append(target)
    if not result:
        raise HTTPException(status_code=422, detail="Select at least one target")
    return result


def _pool():
    if _monitor is None or not getattr(_monitor, "pool", None):
        raise HTTPException(status_code=503, detail="VPS monitor SSH pool is not ready")
    return _monitor.pool


def _local_db_path(db_name: str) -> Path:
    return _data_dir() / db_name


def _sqlite_backup_file(src: Path, dst: Path) -> None:
    """Create a consistent SQLite copy, including any active WAL content."""

    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.unlink(missing_ok=True)
    src_uri = f"file:{src}?mode=ro"
    with sqlite3.connect(src_uri, uri=True) as src_conn:
        with sqlite3.connect(str(dst)) as dst_conn:
            src_conn.backup(dst_conn)


def _remove_sqlite_sidecars(db_path: Path) -> None:
    """Remove WAL/SHM files that belong to a replaced SQLite database."""

    for suffix in ("-wal", "-shm"):
        try:
            Path(str(db_path) + suffix).unlink(missing_ok=True)
        except Exception as exc:
            _log(SERVICE, f"failed to remove SQLite sidecar {db_path}{suffix}: {exc}", level="WARNING")


def _remote_pbgui_dir(target: str) -> str:
    entry = _pool().get_connection(target)
    if not entry:
        raise HTTPException(status_code=404, detail=f"Unknown master target: {target}")
    return str((getattr(entry, "data", {}) or {}).get("remote_pbgui_dir") or getattr(entry.config, "remote_pbgui_dir", None) or "software/pbgui").strip().rstrip("/") or "software/pbgui"


def _remote_path(target: str, *parts: str) -> str:
    return remote_path_join(_remote_pbgui_dir(target), *parts)


def _remote_sqlite_backup_command(src: str, dst: str) -> str:
    script = (
        "import sqlite3, sys, pathlib; "
        "src=sys.argv[1]; dst=sys.argv[2]; pathlib.Path(dst).parent.mkdir(parents=True, exist_ok=True); "
        "pathlib.Path(dst).unlink(missing_ok=True); "
        "s=sqlite3.connect('file:'+src+'?mode=ro', uri=True); "
        "d=sqlite3.connect(dst); s.backup(d); d.close(); s.close()"
    )
    return f"python3 -c {shlex.quote(script)} {shlex.quote(src)} {shlex.quote(dst)}"


def _target_db_paths_local() -> dict[str, Path]:
    return {db_name: _local_db_path(db_name) for db_name in DB_FILE_NAMES}


async def _backup_target_dbs(target: str, label: str, operation: OperationProgress | None = None) -> list[str]:
    """Create WAL-safe backups for both target DB files without replacing them."""

    backups: list[str] = []
    for db_name in DB_FILE_NAMES:
        if operation:
            operation.set_current(f"Back up {db_name} on {target}")
        if target == "local":
            backups.append(_backup_local_file(_local_db_path(db_name), label))
        else:
            backups.append(await _backup_remote_file(target, _remote_path(target, "data", db_name), label))
        if operation:
            operation.advance(f"Backed up {db_name} on {target}", {"target": target, "db": db_name})
    return [item for item in backups if item]


async def _run_remote_python(target: str, script: str, args: list[str], timeout: int = 60) -> dict[str, Any]:
    """Run a small Python helper on a remote target and parse its JSON stdout."""

    pbgui_dir = _remote_pbgui_dir(target)
    command = (
        f"cd {shlex.quote(pbgui_dir)} && "
        f"python3 -c {shlex.quote(script)} "
        + " ".join(shlex.quote(str(arg)) for arg in args)
    )
    result = await _pool().run(target, command, timeout=timeout)
    if not result or result.returncode != 0:
        detail = str(getattr(result, "stderr", "") or getattr(result, "stdout", "") or "remote command failed").strip()
        raise HTTPException(status_code=500, detail=detail or "Remote DB operation failed")
    try:
        return json.loads(str(result.stdout or "").strip().splitlines()[-1])
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Invalid remote DB response: {exc}") from exc


_REMOTE_DELETE_SCRIPT = r"""
import json, sqlite3, sys
db_path, table, user_col, ts_col, cutoff_raw, users_raw = sys.argv[1:7]
users = json.loads(users_raw)
cutoff = None if cutoff_raw == '' else int(cutoff_raw)
conn = sqlite3.connect(db_path, timeout=30)
try:
    conn.execute('PRAGMA busy_timeout=30000')
    exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    if not exists:
        print(json.dumps({'deleted': 0, 'missing': True}))
        sys.exit(0)
    placeholders = ','.join('?' for _ in users)
    params = list(users)
    sql = f'DELETE FROM "{table}" WHERE "{user_col}" IN ({placeholders})'
    if cutoff is not None and ts_col:
        sql += f' AND "{ts_col}" <= ?'
        params.append(cutoff)
    cur = conn.execute(sql, params)
    conn.commit()
    print(json.dumps({'deleted': int(cur.rowcount if cur.rowcount is not None else 0)}))
finally:
    conn.close()
""".strip()


_REMOTE_COPY_SCRIPT = r"""
import json, sqlite3, sys
dst_db, src_db, table, user_col, mode, key_cols_raw, users_raw = sys.argv[1:8]
users = json.loads(users_raw)
key_cols = json.loads(key_cols_raw)
conn = sqlite3.connect(dst_db, timeout=30)
try:
    conn.execute('PRAGMA busy_timeout=30000')
    conn.execute('ATTACH DATABASE ? AS srcdb', (src_db,))
    dst_exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    src_exists = conn.execute("SELECT 1 FROM srcdb.sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    if not dst_exists or not src_exists:
        print(json.dumps({'source': 0, 'inserted': 0, 'skipped': 0}))
        sys.exit(0)
    src_cols = [str(row[1]) for row in conn.execute(f'PRAGMA srcdb.table_info("{table}")')]
    dst_cols = [str(row[1]) for row in conn.execute(f'PRAGMA table_info("{table}")')]
    cols = [col for col in src_cols if col != 'id' and col in dst_cols]
    if user_col not in cols:
        print(json.dumps({'source': 0, 'inserted': 0, 'skipped': 0}))
        sys.exit(0)
    placeholders = ','.join('?' for _ in users)
    col_expr = ', '.join(f'"{col}"' for col in cols)
    rows = conn.execute(f'SELECT {col_expr} FROM srcdb."{table}" WHERE "{user_col}" IN ({placeholders})', users).fetchall()
    key_cols = [col for col in key_cols if col in cols and col in dst_cols]
    if mode == 'add_missing' and not key_cols:
        key_cols = cols
    insert_sql = f'INSERT OR IGNORE INTO "{table}" ({col_expr}) VALUES ({", ".join("?" for _ in cols)})'
    inserted = 0
    skipped = 0
    for row in rows:
        values = dict(zip(cols, row))
        if mode == 'add_missing':
            where = ' AND '.join(f'"{col}" IS ?' for col in key_cols)
            found = conn.execute(f'SELECT 1 FROM "{table}" WHERE {where} LIMIT 1', [values.get(col) for col in key_cols]).fetchone()
            if found:
                skipped += 1
                continue
        cur = conn.execute(insert_sql, [values.get(col) for col in cols])
        if cur.rowcount and cur.rowcount > 0:
            inserted += 1
        else:
            skipped += 1
    conn.commit()
    print(json.dumps({'source': len(rows), 'inserted': inserted, 'skipped': skipped}))
finally:
    conn.close()
""".strip()


_REMOTE_LIST_USERS_SCRIPT = r"""
import json, pathlib, sqlite3, sys
specs = json.loads(sys.argv[1])
users = {}
for spec in specs:
    db_path = pathlib.Path('data') / spec['db_name']
    if not db_path.exists():
        continue
    conn = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True, timeout=30)
    try:
        conn.execute('PRAGMA busy_timeout=30000')
        table = spec['table']
        user_col = spec['user_col']
        exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
        if not exists:
            continue
        table_key = f"{spec['db_name']}:{table}"
        for user, count in conn.execute(f'SELECT "{user_col}", COUNT(*) FROM "{table}" WHERE "{user_col}" IS NOT NULL AND "{user_col}" != \'\' GROUP BY "{user_col}"'):
            name = str(user)
            entry = users.setdefault(name, {'user': name, 'total': 0, 'tables': {}})
            value = int(count or 0)
            entry['tables'][table_key] = value
            entry['total'] += value
    finally:
        conn.close()
print(json.dumps({'users': sorted(users.values(), key=lambda item: item['user'].lower())}))
""".strip()


_REMOTE_COUNT_USERS_SCRIPT = r"""
import json, pathlib, sqlite3, sys
specs = json.loads(sys.argv[1])
users = json.loads(sys.argv[2])
cutoff_raw = sys.argv[3]
cutoff = None if cutoff_raw == '' else int(cutoff_raw)
tables = {}
total = 0
if users:
    placeholders = ','.join('?' for _ in users)
    for spec in specs:
        db_path = pathlib.Path('data') / spec['db_name']
        table_key = f"{spec['db_name']}:{spec['table']}"
        if not db_path.exists():
            tables[table_key] = 0
            continue
        conn = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True, timeout=30)
        try:
            conn.execute('PRAGMA busy_timeout=30000')
            table = spec['table']
            user_col = spec['user_col']
            ts_col = spec.get('timestamp_col') or ''
            exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
            if not exists:
                tables[table_key] = 0
                continue
            params = list(users)
            sql = f'SELECT COUNT(*) FROM "{table}" WHERE "{user_col}" IN ({placeholders})'
            if cutoff is not None and ts_col:
                sql += f' AND "{ts_col}" <= ?'
                params.append(cutoff)
            row = conn.execute(sql, params).fetchone()
            count = int(row[0] if row else 0)
            tables[table_key] = count
            total += count
        finally:
            conn.close()
print(json.dumps({'total': total, 'tables': tables}))
""".strip()


_REMOTE_SYNC_FETCH_SCRIPT = r"""
import json, pathlib, sqlite3, sys
db_name, table, user_col, timestamp_col, mode, users_raw, cutoffs_raw = sys.argv[1:8]
users = json.loads(users_raw)
cutoffs = json.loads(cutoffs_raw or '{}')
db_path = pathlib.Path('data') / db_name
if not users or not db_path.exists():
    print(json.dumps({'columns': [], 'rows': []}))
    sys.exit(0)
conn = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True, timeout=30)
try:
    conn.execute('PRAGMA busy_timeout=30000')
    exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    if not exists:
        print(json.dumps({'columns': [], 'rows': []}))
        sys.exit(0)
    columns = [str(row[1]) for row in conn.execute(f'PRAGMA table_info("{table}")') if str(row[1]) != 'id']
    if user_col not in columns:
        print(json.dumps({'columns': [], 'rows': []}))
        sys.exit(0)
    col_expr = ', '.join(f'"{col}"' for col in columns)
    rows = []
    if mode == 'append' and timestamp_col:
        order_cols = [timestamp_col]
        for key in ('uniqueid', 'exchange', 'trade_id'):
            if key in columns and key not in order_cols:
                order_cols.append(key)
        order_expr = ', '.join(f'"{col}"' for col in order_cols)
        for user in users:
            cutoff = int(cutoffs.get(str(user), 0) or 0)
            sql = f'SELECT {col_expr} FROM "{table}" WHERE "{user_col}" = ? AND "{timestamp_col}" >= ? ORDER BY {order_expr}'
            rows.extend([list(row) for row in conn.execute(sql, (user, cutoff))])
    else:
        placeholders = ','.join('?' for _ in users)
        sql = f'SELECT {col_expr} FROM "{table}" WHERE "{user_col}" IN ({placeholders}) ORDER BY "{user_col}"'
        rows = [list(row) for row in conn.execute(sql, users)]
    print(json.dumps({'columns': columns, 'rows': rows}))
finally:
    conn.close()
""".strip()


_REMOTE_SYNC_APPLY_SCRIPT = r"""
import json, pathlib, sqlite3, sys
db_name, table, user_col, mode, users_raw, payload_path = sys.argv[1:7]
users = json.loads(users_raw)
payload = json.loads(pathlib.Path(payload_path).read_text(encoding='utf-8'))
source_columns = [str(col) for col in payload.get('columns') or []]
source_rows = payload.get('rows') or []
db_path = pathlib.Path('data') / db_name
conn = sqlite3.connect(db_path, timeout=30)
try:
    conn.execute('PRAGMA busy_timeout=30000')
    exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    if not exists:
        print(json.dumps({'fetched': len(source_rows), 'inserted': 0, 'skipped': len(source_rows), 'deleted': 0}))
        sys.exit(0)
    target_columns = [str(row[1]) for row in conn.execute(f'PRAGMA table_info("{table}")') if str(row[1]) != 'id']
    columns = [col for col in source_columns if col in target_columns]
    if user_col not in columns:
        print(json.dumps({'fetched': len(source_rows), 'inserted': 0, 'skipped': len(source_rows), 'deleted': 0}))
        sys.exit(0)
    deleted = 0
    if mode == 'state' and users:
        placeholders = ','.join('?' for _ in users)
        cur = conn.execute(f'DELETE FROM "{table}" WHERE "{user_col}" IN ({placeholders})', users)
        deleted = int(cur.rowcount if cur.rowcount is not None else 0)
    quoted_cols = ', '.join('"' + col + '"' for col in columns)
    insert_sql = f'INSERT OR IGNORE INTO "{table}" ({quoted_cols}) VALUES ({", ".join("?" for _ in columns)})'
    inserted = 0
    skipped = 0
    for row in source_rows:
        values = dict(zip(source_columns, row))
        cur = conn.execute(insert_sql, [values.get(col) for col in columns])
        if cur.rowcount and cur.rowcount > 0:
            inserted += 1
        else:
            skipped += 1
    conn.commit()
    print(json.dumps({'fetched': len(source_rows), 'inserted': inserted, 'skipped': skipped, 'deleted': deleted}))
finally:
    conn.close()
""".strip()


_REMOTE_SYNC_STATS_SCRIPT = r"""
import json, pathlib, sqlite3, sys
specs = json.loads(sys.argv[1])
users = json.loads(sys.argv[2])
tables = {}
if users:
    placeholders = ','.join('?' for _ in users)
    for spec in specs:
        db_path = pathlib.Path('data') / spec['db_name']
        table_key = f"{spec['db_name']}:{spec['table']}"
        entry = {'count': 0, 'max_timestamp': 0, 'users': {}}
        if not db_path.exists():
            tables[table_key] = entry
            continue
        conn = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True, timeout=30)
        try:
            conn.execute('PRAGMA busy_timeout=30000')
            table = spec['table']
            user_col = spec['user_col']
            ts_col = spec.get('timestamp_col') or ''
            exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
            if not exists:
                tables[table_key] = entry
                continue
            row = conn.execute(f'SELECT COUNT(*) FROM "{table}" WHERE "{user_col}" IN ({placeholders})', users).fetchone()
            entry['count'] = int(row[0] if row else 0)
            if ts_col:
                row = conn.execute(f'SELECT MAX("{ts_col}") FROM "{table}" WHERE "{user_col}" IN ({placeholders})', users).fetchone()
                entry['max_timestamp'] = int(row[0] if row and row[0] is not None else 0)
                for user, count, max_ts in conn.execute(
                    f'SELECT "{user_col}", COUNT(*), MAX("{ts_col}") FROM "{table}" WHERE "{user_col}" IN ({placeholders}) GROUP BY "{user_col}"',
                    users,
                ):
                    entry['users'][str(user)] = {'count': int(count or 0), 'max_timestamp': int(max_ts or 0)}
            else:
                for user, count in conn.execute(
                    f'SELECT "{user_col}", COUNT(*) FROM "{table}" WHERE "{user_col}" IN ({placeholders}) GROUP BY "{user_col}"',
                    users,
                ):
                    entry['users'][str(user)] = {'count': int(count or 0), 'max_timestamp': 0}
            tables[table_key] = entry
        finally:
            conn.close()
print(json.dumps({'tables': tables}))
""".strip()


async def _read_remote_text(target: str, remote_path: str) -> str:
    data = await _pool().read_remote_file(target, remote_path)
    if data is None:
        return ""
    return data.decode("utf-8", errors="replace")


async def _probe_remote_master(target: str) -> dict[str, Any] | None:
    pbgui_dir = _remote_pbgui_dir(target)
    script = """
import configparser, json
from pathlib import Path
cfg = configparser.ConfigParser()
path = Path('pbgui.ini')
if path.exists():
    cfg.read(path)
print(json.dumps({
    'pbname': cfg.get('main', 'pbname', fallback=''),
    'role': cfg.get('main', 'role', fallback=''),
}))
""".strip()
    cmd = f"cd {shlex.quote(pbgui_dir)} && python3 - <<'PY'\n{script}\nPY"
    result = await _pool().run(target, cmd, timeout=12)
    if not result or result.returncode != 0:
        return None
    try:
        data = json.loads(str(result.stdout or "").strip().splitlines()[-1])
    except Exception:
        return None
    role = str(data.get("role") or "").strip().lower()
    if role != "master":
        return None
    return {
        "id": target,
        "label": str(data.get("pbname") or target),
        "kind": "remote",
        "role": role,
        "connected": target in _pool().connected_hosts(),
        "remote_pbgui_dir": pbgui_dir,
    }


async def _known_targets() -> list[dict[str, Any]]:
    try:
        from pbgui_purefunc import load_ini

        local_name = str(load_ini("main", "pbname") or "local").strip() or "local"
    except Exception:
        local_name = "local"
    targets = [{"id": "local", "label": local_name, "kind": "local", "role": "master", "connected": True}]
    if _monitor is None or not getattr(_monitor, "pool", None):
        return targets
    host_meta = _monitor_host_meta()
    connected = set(_pool().connected_hosts())
    for hostname in _pool().hostnames():
        meta = host_meta.get(hostname) if isinstance(host_meta, dict) else {}
        role = str((meta or {}).get("role") or "").strip().lower()
        if role != "master":
            continue
        entry = _pool().get_connection(hostname)
        remote_dir = "software/pbgui"
        if entry:
            remote_dir = str(
                (getattr(entry, "data", {}) or {}).get("remote_pbgui_dir")
                or getattr(entry.config, "remote_pbgui_dir", None)
                or "software/pbgui"
            ).strip().rstrip("/") or "software/pbgui"
        targets.append(
            {
                "id": hostname,
                "label": str((meta or {}).get("pbname") or hostname),
                "kind": "remote",
                "role": role,
                "connected": hostname in connected,
                "remote_pbgui_dir": remote_dir,
            }
        )
    return targets


async def _assert_known_target(target: str) -> None:
    if target == "local":
        return
    if _monitor is None or not getattr(_monitor, "pool", None):
        raise HTTPException(status_code=503, detail="VPS monitor SSH pool is not ready")
    entry = _pool().get_connection(target)
    if not entry:
        raise HTTPException(status_code=400, detail=f"Target is not a known VPS host: {target}")
    host_meta = _monitor_host_meta()
    meta = host_meta.get(target) if isinstance(host_meta, dict) else {}
    role = str((meta or {}).get("role") or "").strip().lower()
    if role != "master":
        raise HTTPException(status_code=400, detail=f"Target is not a known master: {target}")
    if target not in set(_pool().connected_hosts()):
        raise HTTPException(status_code=503, detail=f"Target master is not connected: {target}")


def _load_sync_jobs() -> None:
    """Load persisted sync jobs into memory."""

    _sync_jobs.clear()
    path = _sync_jobs_file()
    if not path.exists():
        return
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        _log(SERVICE, f"failed to load sync jobs: {exc}", level="WARNING")
        return
    for item in data if isinstance(data, list) else []:
        if not isinstance(item, dict):
            continue
        job_id = str(item.get("id") or "").strip()
        if job_id:
            _sync_jobs[job_id] = item


def _save_sync_jobs() -> None:
    """Persist sync jobs atomically."""

    path = _sync_jobs_file()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(list(_sync_jobs.values()), indent=4), encoding="utf-8")
    os.replace(tmp, path)


def _sync_job_public(job: dict[str, Any]) -> dict[str, Any]:
    result = dict(job)
    job_id = str(result.get("id") or "").strip()
    if job_id:
        result["log_file"] = _sync_job_log_relative(job_id, str(result.get("name") or ""))
    return result


def _sync_log_list(value: Any) -> str:
    if isinstance(value, (list, tuple, set)):
        return ", ".join(str(item) for item in value) or "none"
    if value is None or value == "":
        return "none"
    return str(value)


def _sync_log_message(job_name: str, event: str, data: dict[str, Any]) -> str:
    name = job_name or "sync job"
    if event == "job_saved":
        enabled = "enabled" if data.get("enabled") else "disabled"
        return (
            f'Sync job "{name}" saved ({enabled}): source {data.get("source")}; '
            f'targets {_sync_log_list(data.get("targets"))}; users {_sync_log_list(data.get("users"))}.'
        )
    if event == "job_deleted":
        return f'Sync job "{name}" deleted.'
    if event == "run_start":
        run_type = "manual" if data.get("manual") else "scheduled"
        return (
            f'Sync job "{name}" started ({run_type}): source {data.get("source")} -> '
            f'{_sync_log_list(data.get("targets"))}; users {_sync_log_list(data.get("users"))}.'
        )
    if event == "safety_ok":
        return (
            f'Sync job "{name}" safety check passed: targets {_sync_log_list(data.get("targets"))}; '
            f'users {_sync_log_list(data.get("users"))}.'
        )
    if event == "target_start":
        return f'Sync job "{name}" started target {data.get("target")}.'
    if event == "target_done":
        verify = data.get("verify") if isinstance(data.get("verify"), dict) else {}
        verify_text = "verified" if verify.get("ok") else "verify mismatch"
        return (
            f'Sync job "{name}" finished target {data.get("target")}: '
            f'fetched {data.get("fetched", data.get("source_total", 0))}, inserted {data.get("inserted", 0)}, '
            f'already present {data.get("skipped", 0)}, state replaced {data.get("deleted", 0)}; '
            f'{verify_text}. No backup.'
        )
    if event == "run_success":
        return (
            f'Sync job "{name}" finished successfully: targets {_sync_log_list(data.get("targets"))}; '
            f'users {_sync_log_list(data.get("users"))}.'
        )
    if event == "run_error":
        return f'Sync job "{name}" failed: {data.get("error") or "unknown error"}.'
    details = "; ".join(f"{key}: {value}" for key, value in data.items() if value is not None)
    return f'Sync job "{name}" {event.replace("_", " ")}' + (f": {details}." if details else ".")


def _log_sync_job(job_id: str, event: str, **data: Any) -> None:
    """Write a sync job event through the standard logging system to its job log."""

    event_data = dict(data)
    job_name = str(
        event_data.pop("job_name", "")
        or event_data.pop("name", "")
        or _sync_jobs.get(job_id, {}).get("name")
        or "sync job"
    )
    level = "WARNING" if "error" in str(event).lower() else "INFO"
    log_path = _sync_job_log_path(job_id, job_name)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    _log(
        SERVICE,
        _sync_log_message(job_name, event, event_data),
        level=level,
        logfile=str(log_path),
    )


def _local_pbdata_active_users() -> dict[str, Any]:
    """Return PBData running state and configured writer users for the local master."""

    running = False
    try:
        pid_path = _data_dir() / "pid" / "pbdata.pid"
        pid = int(pid_path.read_text(encoding="utf-8").strip()) if pid_path.exists() else 0
        if pid > 0:
            cmdline_path = Path("/proc") / str(pid) / "cmdline"
            cmdline = cmdline_path.read_text(encoding="utf-8", errors="ignore") if cmdline_path.exists() else ""
            running = "PBData.py" in cmdline or "pbdata.py" in cmdline.lower()
    except Exception as exc:
        _log(SERVICE, f"local PBData active check failed: {exc}", level="WARNING")
    fetch_users: list[str] = []
    trades_users: list[str] = []
    try:
        import ast as _ast
        import configparser as _configparser

        cfg = _configparser.ConfigParser()
        cfg.read(Path(PBGDIR) / "pbgui.ini")
        if cfg.has_option("pbdata", "fetch_users"):
            parsed = _ast.literal_eval(cfg.get("pbdata", "fetch_users"))
            fetch_users = [str(item) for item in parsed] if isinstance(parsed, list) else []
        if cfg.has_option("pbdata", "trades_users"):
            raw = str(cfg.get("pbdata", "trades_users") or "").strip()
            parsed = _ast.literal_eval(raw) if raw else []
            trades_users = [str(item) for item in parsed] if isinstance(parsed, list) else []
    except Exception as exc:
        _log(SERVICE, f"local PBData user config check failed: {exc}", level="WARNING")
    active = sorted(set(fetch_users + trades_users), key=str.lower) if running else []
    return {"running": running, "fetch_users": fetch_users, "trades_users": trades_users, "active_users": active}


_REMOTE_PBDATA_ACTIVE_SCRIPT = r"""
import ast, configparser, json, pathlib, subprocess
running = False
try:
    svc = subprocess.run(['systemctl', '--user', 'is-active', '--quiet', 'pbgui-pbdata.service'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    running = svc.returncode == 0
except Exception:
    running = False
pid_path = pathlib.Path('data/pid/pbdata.pid')
if not running and pid_path.exists():
    try:
        pid = int(pid_path.read_text().strip())
        cmdline_path = pathlib.Path('/proc') / str(pid) / 'cmdline'
        cmdline = cmdline_path.read_text(errors='ignore') if cmdline_path.exists() else ''
        running = 'PBData.py' in cmdline or 'pbdata.py' in cmdline.lower()
    except Exception:
        running = False
cfg = configparser.ConfigParser()
cfg.read('pbgui.ini')
def read_list(name):
    if not cfg.has_option('pbdata', name):
        return []
    raw = str(cfg.get('pbdata', name) or '').strip()
    if not raw:
        return []
    try:
        value = ast.literal_eval(raw)
        return [str(item) for item in value] if isinstance(value, list) else []
    except Exception:
        return []
fetch_users = read_list('fetch_users')
trades_users = read_list('trades_users')
active_users = sorted(set(fetch_users + trades_users), key=str.lower) if running else []
print(json.dumps({'running': running, 'fetch_users': fetch_users, 'trades_users': trades_users, 'active_users': active_users}))
"""


async def _pbdata_active_users(target: str) -> dict[str, Any]:
    target = _target_id(target)
    if target == "local":
        return _local_pbdata_active_users()
    await _assert_known_target(target)
    return await _run_remote_python(target, _REMOTE_PBDATA_ACTIVE_SCRIPT, [], timeout=30)


async def _assert_target_users_not_active(target: str, users: list[str]) -> None:
    status = await _pbdata_active_users(target)
    active = {str(item) for item in status.get("active_users") or []}
    blocked = sorted(active.intersection(users), key=str.lower)
    if blocked:
        raise HTTPException(
            status_code=409,
            detail=f"PBData is active for user(s) on target {target}: {', '.join(blocked)}",
        )


def _validate_sync_job_payload(payload: SyncJobRequest) -> dict[str, Any]:
    users = _clean_users(payload.users)
    if not users:
        raise HTTPException(status_code=422, detail="Select at least one user")
    source = _target_id(payload.source)
    targets = _clean_targets(payload.targets)
    if source in targets:
        raise HTTPException(status_code=400, detail="Source cannot also be a sync target")
    interval = max(30, int(payload.interval_seconds or 60))
    name = str(payload.name or "").strip() or f"{source} sync"
    job_id = str(payload.id or "").strip() or uuid.uuid4().hex
    now = datetime.now(timezone.utc).isoformat()
    existing = _sync_jobs.get(job_id, {})
    job = {
        "id": job_id,
        "name": name,
        "source": source,
        "targets": targets,
        "users": users,
        "interval_seconds": interval,
        "enabled": bool(payload.enabled),
        "created_at": existing.get("created_at") or now,
        "updated_at": now,
        "last_run": existing.get("last_run") or "",
        "last_ok": existing.get("last_ok") or "",
        "last_error": existing.get("last_error") or "",
        "last_result": existing.get("last_result") or None,
        "next_run": existing.get("next_run") or "",
        "running": bool(existing.get("running") or False),
    }
    return job


async def _validate_sync_job_safety(job: dict[str, Any]) -> None:
    await _assert_known_target(str(job.get("source") or "local"))
    for target in job.get("targets") or []:
        await _assert_known_target(str(target))
        await _assert_target_users_not_active(str(target), list(job.get("users") or []))
    if not job.get("enabled"):
        return
    for other in _sync_jobs.values():
        if other.get("id") == job.get("id") or not other.get("enabled"):
            continue
        other_users = set(other.get("users") or [])
        shared_users = other_users.intersection(set(job.get("users") or []))
        if not shared_users:
            continue
        if str(other.get("source")) != str(job.get("source")):
            raise HTTPException(
                status_code=409,
                detail=f"User(s) already have another sync source: {', '.join(sorted(shared_users))}",
            )


def _next_sync_run_iso(interval_seconds: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=max(30, int(interval_seconds or 60)))).isoformat()


async def _run_sync_job(job_id: str, *, manual: bool = False, operation: OperationProgress | None = None) -> dict[str, Any]:
    job = _sync_jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Sync job not found")
    if job_id in _sync_job_locks:
        raise HTTPException(status_code=409, detail="Sync job is already running")
    _sync_job_locks.add(job_id)
    job["running"] = True
    job["last_run"] = datetime.now(timezone.utc).isoformat()
    job["updated_at"] = job["last_run"]
    _save_sync_jobs()
    try:
        users = _clean_users(list(job.get("users") or []))
        source = _target_id(str(job.get("source") or "local"))
        targets = _clean_targets(list(job.get("targets") or []))
        _log_sync_job(job_id, "run_start", manual=manual, source=source, targets=targets, users=users)
        if operation:
            operation.advance("Validated sync job", {"source": source, "targets": targets, "users": users})
        await _validate_sync_job_safety(job)
        _log_sync_job(job_id, "safety_ok", targets=targets, users=users)
        if operation:
            operation.advance("Safety check passed", {"targets": targets, "users": users})
        results: dict[str, Any] = {}
        with tempfile.TemporaryDirectory(prefix="pbgui-db-tools-sync-") as tmp:
            temp_dir = Path(tmp)
            for target in targets:
                _log_sync_job(job_id, "target_start", target=target)
                results[target] = await sync_user_rows_incremental(source, target, users, temp_dir, operation)
                target_result = results.get(target) or {}
                _log_sync_job(
                    job_id,
                    "target_done",
                    target=target,
                    source_total=target_result.get("source_total"),
                    fetched=target_result.get("fetched"),
                    inserted=target_result.get("inserted"),
                    skipped=target_result.get("skipped"),
                    deleted=target_result.get("deleted"),
                    verify=target_result.get("verify"),
                )
        verify_failed = [
            target
            for target, target_result in results.items()
            if not ((target_result.get("verify") or {}).get("ok"))
        ]
        result = {
            "ok": not verify_failed,
            "manual": manual,
            "source": source,
            "targets": targets,
            "users": users,
            "results": results,
            "verify_failed": verify_failed,
        }
        now = datetime.now(timezone.utc).isoformat()
        if verify_failed:
            err = f"Sync verify failed for target(s): {', '.join(verify_failed)}"
            job.update({"last_error": err, "last_result": result, "next_run": _next_sync_run_iso(int(job.get("interval_seconds") or 60))})
            _log_sync_job(job_id, "run_error", error=err)
            _log(SERVICE, f"sync job {job.get('name')} failed verify: {err}", level="WARNING")
            return result
        job.update({"last_ok": now, "last_error": "", "last_result": result, "next_run": _next_sync_run_iso(int(job.get("interval_seconds") or 60))})
        _log_sync_job(job_id, "run_success", targets=targets, users=users)
        _log(SERVICE, f"sync job {job.get('name')} source={source} targets={targets} users={users}", level="INFO")
        return result
    except Exception as exc:
        err = str(exc.detail) if isinstance(exc, HTTPException) else str(exc)
        job.update({"last_error": err, "next_run": _next_sync_run_iso(int(job.get("interval_seconds") or 60))})
        _log_sync_job(job_id, "run_error", error=err)
        _log(SERVICE, f"sync job {job.get('name')} failed: {err}", level="WARNING")
        if manual:
            raise
        return {"ok": False, "error": err}
    finally:
        job["running"] = False
        job["updated_at"] = datetime.now(timezone.utc).isoformat()
        _sync_job_locks.discard(job_id)
        _save_sync_jobs()


def _ensure_sync_scheduler() -> None:
    global _sync_scheduler_task
    if _sync_scheduler_task and not _sync_scheduler_task.done():
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    _sync_scheduler_task = loop.create_task(_sync_scheduler_loop(), name="db-tools-sync-scheduler")


async def _sync_scheduler_loop() -> None:
    while True:
        try:
            now_ts = datetime.now(timezone.utc).timestamp()
            changed = False
            for job_id, job in list(_sync_jobs.items()):
                if not job.get("enabled") or job.get("running") or job_id in _sync_job_locks:
                    continue
                next_raw = str(job.get("next_run") or "")
                if next_raw:
                    try:
                        next_ts = datetime.fromisoformat(next_raw).timestamp()
                    except Exception:
                        next_ts = 0
                else:
                    next_ts = 0
                if next_ts and next_ts > now_ts:
                    continue
                asyncio.create_task(_run_sync_job(job_id), name=f"db-tools-sync-{job_id[:8]}")
                job["next_run"] = _next_sync_run_iso(int(job.get("interval_seconds") or 60))
                changed = True
            if changed:
                _save_sync_jobs()
        except Exception as exc:
            _log(SERVICE, f"sync scheduler failed: {exc}", level="WARNING", meta={"traceback": traceback.format_exc()})
        await asyncio.sleep(10)


def _ensure_schema(db_path: Path, db_name: str) -> None:
    schema = MAIN_SCHEMA if db_name == MAIN_DB_NAME else TRADES_SCHEMA
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(str(db_path)) as conn:
        for statement in schema:
            conn.execute(statement)
        conn.commit()


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    return row is not None


def _table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [str(row[1]) for row in conn.execute(f'PRAGMA table_info("{table}")').fetchall()]


def _placeholders(items: list[str]) -> str:
    return ",".join("?" for _ in items)


def _table_specs_json() -> str:
    """Serialize table specs for remote read helpers."""

    return json.dumps(
        [
            {
                "db_name": spec.db_name,
                "table": spec.table,
                "user_col": spec.user_col,
                "timestamp_col": spec.timestamp_col or "",
            }
            for spec in TABLE_SPECS
        ]
    )


def _connect_bundle(db_paths: dict[str, Path]) -> dict[str, sqlite3.Connection]:
    conns = {name: sqlite3.connect(str(path), timeout=30) for name, path in db_paths.items()}
    for conn in conns.values():
        try:
            conn.execute("PRAGMA busy_timeout=30000")
        except Exception:
            pass
    return conns


def _close_bundle(conns: dict[str, sqlite3.Connection]) -> None:
    for conn in conns.values():
        try:
            conn.close()
        except Exception:
            pass


def _count_for_spec(conn: sqlite3.Connection, spec: TableSpec, users: list[str], cutoff_ms: int | None = None) -> int:
    if not _table_exists(conn, spec.table):
        return 0
    params: list[Any] = list(users)
    sql = f'SELECT COUNT(*) FROM "{spec.table}" WHERE "{spec.user_col}" IN ({_placeholders(users)})'
    if cutoff_ms is not None and spec.timestamp_col:
        sql += f' AND "{spec.timestamp_col}" <= ?'
        params.append(int(cutoff_ms))
    row = conn.execute(sql, params).fetchone()
    return int(row[0] if row else 0)


def count_user_rows(db_paths: dict[str, Path], users: list[str], cutoff_ms: int | None = None) -> dict[str, Any]:
    """Return row counts for selected users across all DB tool tables."""

    users = _clean_users(users)
    if not users:
        return {"total": 0, "tables": {}}
    conns = _connect_bundle(db_paths)
    try:
        tables: dict[str, int] = {}
        total = 0
        for spec in TABLE_SPECS:
            count = _count_for_spec(conns[spec.db_name], spec, users, cutoff_ms)
            tables[f"{spec.db_name}:{spec.table}"] = count
            total += count
        return {"total": total, "tables": tables}
    finally:
        _close_bundle(conns)


def list_users_with_counts(db_paths: dict[str, Path]) -> list[dict[str, Any]]:
    """List all users found in handled tables with per-user row counts."""

    conns = _connect_bundle(db_paths)
    try:
        users: set[str] = set()
        for spec in TABLE_SPECS:
            conn = conns[spec.db_name]
            if not _table_exists(conn, spec.table):
                continue
            for row in conn.execute(f'SELECT DISTINCT "{spec.user_col}" FROM "{spec.table}"'):
                if row and row[0]:
                    users.add(str(row[0]))
        result: list[dict[str, Any]] = []
        for user in sorted(users, key=str.lower):
            counts = count_user_rows(db_paths, [user])
            result.append({"user": user, "total": counts["total"], "tables": counts["tables"]})
        return result
    finally:
        _close_bundle(conns)


async def list_users_for_target(target: str) -> list[dict[str, Any]]:
    """List users on a target without transferring remote DB files."""

    target = _target_id(target)
    await _assert_known_target(target)
    if target == "local":
        return list_users_with_counts(_target_db_paths_local())
    data = await _run_remote_python(target, _REMOTE_LIST_USERS_SCRIPT, [_table_specs_json()], timeout=90)
    users = data.get("users") or []
    return users if isinstance(users, list) else []


async def count_user_rows_for_target(target: str, users: list[str], cutoff_ms: int | None = None) -> dict[str, Any]:
    """Count selected user rows on a target without transferring remote DB files."""

    target = _target_id(target)
    await _assert_known_target(target)
    users = _clean_users(users)
    if target == "local":
        return count_user_rows(_target_db_paths_local(), users, cutoff_ms)
    if not users:
        return {"total": 0, "tables": {}}
    data = await _run_remote_python(
        target,
        _REMOTE_COUNT_USERS_SCRIPT,
        [_table_specs_json(), json.dumps(users), "" if cutoff_ms is None else str(int(cutoff_ms))],
        timeout=90,
    )
    tables = data.get("tables") if isinstance(data, dict) else {}
    return {"total": int(data.get("total") or 0), "tables": tables if isinstance(tables, dict) else {}}


def _sync_table_key(spec: TableSpec) -> str:
    return f"{spec.db_name}:{spec.table}"


def _sync_table_mode(spec: TableSpec) -> Literal["append", "state"]:
    return "append" if (spec.db_name, spec.table) in APPEND_SYNC_TABLES else "state"


def _sync_select_columns(conn: sqlite3.Connection, spec: TableSpec) -> list[str]:
    if not _table_exists(conn, spec.table):
        return []
    columns = [col for col in _table_columns(conn, spec.table) if col != "id"]
    return columns if spec.user_col in columns else []


def _fetch_sync_rows_from_paths(
    db_paths: dict[str, Path],
    spec: TableSpec,
    users: list[str],
    mode: Literal["append", "state"],
    cutoffs: dict[str, int] | None = None,
) -> dict[str, Any]:
    path = db_paths[spec.db_name]
    if not path.exists():
        return {"columns": [], "rows": []}
    conn = sqlite3.connect(str(path), timeout=30)
    try:
        conn.execute("PRAGMA busy_timeout=30000")
        columns = _sync_select_columns(conn, spec)
        if not columns:
            return {"columns": [], "rows": []}
        col_expr = ", ".join(f'"{col}"' for col in columns)
        rows: list[list[Any]] = []
        if mode == "append" and spec.timestamp_col:
            order_cols = [spec.timestamp_col]
            for key in spec.key_cols:
                if key in columns and key not in order_cols:
                    order_cols.append(key)
            order_expr = ", ".join(f'"{col}"' for col in order_cols)
            for user in users:
                cutoff = int((cutoffs or {}).get(user, 0) or 0)
                rows.extend(
                    [
                        list(row)
                        for row in conn.execute(
                            f'SELECT {col_expr} FROM "{spec.table}" '
                            f'WHERE "{spec.user_col}" = ? AND "{spec.timestamp_col}" >= ? '
                            f"ORDER BY {order_expr}",
                            (user, cutoff),
                        )
                    ]
                )
        else:
            rows = [
                list(row)
                for row in conn.execute(
                    f'SELECT {col_expr} FROM "{spec.table}" '
                    f'WHERE "{spec.user_col}" IN ({_placeholders(users)}) ORDER BY "{spec.user_col}"',
                    users,
                )
            ]
        return {"columns": columns, "rows": rows}
    finally:
        conn.close()


def _apply_sync_rows_to_paths(
    db_paths: dict[str, Path],
    spec: TableSpec,
    users: list[str],
    mode: Literal["append", "state"],
    payload: dict[str, Any],
) -> dict[str, int]:
    path = db_paths[spec.db_name]
    _ensure_schema(path, spec.db_name)
    source_columns = [str(col) for col in payload.get("columns") or []]
    source_rows = list(payload.get("rows") or [])
    conn = sqlite3.connect(str(path), timeout=30)
    try:
        conn.execute("PRAGMA busy_timeout=30000")
        columns = [col for col in source_columns if col in _sync_select_columns(conn, spec)]
        fetched = len(source_rows)
        if spec.user_col not in columns:
            return {"fetched": fetched, "inserted": 0, "skipped": fetched, "deleted": 0}
        deleted = 0
        if mode == "state":
            cur = conn.execute(
                f'DELETE FROM "{spec.table}" WHERE "{spec.user_col}" IN ({_placeholders(users)})',
                users,
            )
            deleted = int(cur.rowcount if cur.rowcount is not None else 0)
        quoted_cols = ", ".join(f'"{col}"' for col in columns)
        insert_sql = f'INSERT OR IGNORE INTO "{spec.table}" ({quoted_cols}) VALUES ({", ".join("?" for _ in columns)})'
        inserted = 0
        skipped = 0
        for row in source_rows:
            values = dict(zip(source_columns, row))
            cur = conn.execute(insert_sql, [values.get(col) for col in columns])
            if cur.rowcount and cur.rowcount > 0:
                inserted += 1
            else:
                skipped += 1
        conn.commit()
        return {"fetched": fetched, "inserted": inserted, "skipped": skipped, "deleted": deleted}
    finally:
        conn.close()


def _sync_table_stats_from_paths(db_paths: dict[str, Path], users: list[str]) -> dict[str, Any]:
    users = _clean_users(users)
    if not users:
        return {"tables": {}}
    conns = _connect_bundle(db_paths)
    try:
        tables: dict[str, Any] = {}
        for spec in TABLE_SPECS:
            entry: dict[str, Any] = {"count": 0, "max_timestamp": 0, "users": {}}
            conn = conns[spec.db_name]
            if not _table_exists(conn, spec.table):
                tables[_sync_table_key(spec)] = entry
                continue
            row = conn.execute(
                f'SELECT COUNT(*) FROM "{spec.table}" WHERE "{spec.user_col}" IN ({_placeholders(users)})',
                users,
            ).fetchone()
            entry["count"] = int(row[0] if row else 0)
            if spec.timestamp_col:
                row = conn.execute(
                    f'SELECT MAX("{spec.timestamp_col}") FROM "{spec.table}" '
                    f'WHERE "{spec.user_col}" IN ({_placeholders(users)})',
                    users,
                ).fetchone()
                entry["max_timestamp"] = int(row[0] if row and row[0] is not None else 0)
                for user, count, max_ts in conn.execute(
                    f'SELECT "{spec.user_col}", COUNT(*), MAX("{spec.timestamp_col}") FROM "{spec.table}" '
                    f'WHERE "{spec.user_col}" IN ({_placeholders(users)}) GROUP BY "{spec.user_col}"',
                    users,
                ):
                    entry["users"][str(user)] = {"count": int(count or 0), "max_timestamp": int(max_ts or 0)}
            else:
                for user, count in conn.execute(
                    f'SELECT "{spec.user_col}", COUNT(*) FROM "{spec.table}" '
                    f'WHERE "{spec.user_col}" IN ({_placeholders(users)}) GROUP BY "{spec.user_col}"',
                    users,
                ):
                    entry["users"][str(user)] = {"count": int(count or 0), "max_timestamp": 0}
            tables[_sync_table_key(spec)] = entry
        return {"tables": tables}
    finally:
        _close_bundle(conns)


async def _sync_table_stats_for_target(target: str, users: list[str]) -> dict[str, Any]:
    target = _target_id(target)
    await _assert_known_target(target)
    users = _clean_users(users)
    if target == "local":
        return _sync_table_stats_from_paths(_target_db_paths_local(), users)
    if not users:
        return {"tables": {}}
    data = await _run_remote_python(target, _REMOTE_SYNC_STATS_SCRIPT, [_table_specs_json(), json.dumps(users)], timeout=90)
    tables = data.get("tables") if isinstance(data, dict) else {}
    return {"tables": tables if isinstance(tables, dict) else {}}


async def _fetch_sync_rows_for_target(
    target: str,
    spec: TableSpec,
    users: list[str],
    mode: Literal["append", "state"],
    cutoffs: dict[str, int] | None = None,
) -> dict[str, Any]:
    target = _target_id(target)
    await _assert_known_target(target)
    if target == "local":
        return _fetch_sync_rows_from_paths(_target_db_paths_local(), spec, users, mode, cutoffs)
    data = await _run_remote_python(
        target,
        _REMOTE_SYNC_FETCH_SCRIPT,
        [
            spec.db_name,
            spec.table,
            spec.user_col,
            spec.timestamp_col or "",
            mode,
            json.dumps(users),
            json.dumps(cutoffs or {}),
        ],
        timeout=120,
    )
    columns = data.get("columns") if isinstance(data, dict) else []
    rows = data.get("rows") if isinstance(data, dict) else []
    return {"columns": columns if isinstance(columns, list) else [], "rows": rows if isinstance(rows, list) else []}


async def _apply_sync_rows_for_target(
    target: str,
    spec: TableSpec,
    users: list[str],
    mode: Literal["append", "state"],
    payload: dict[str, Any],
    temp_dir: Path,
) -> dict[str, int]:
    target = _target_id(target)
    await _assert_known_target(target)
    if target == "local":
        return _apply_sync_rows_to_paths(_target_db_paths_local(), spec, users, mode, payload)
    payload_key = uuid.uuid4().hex
    remote_relative_dir = f"data/tmp/db-tools/{payload_key}"
    remote_dir = _remote_path(target, "data", "tmp", "db-tools", payload_key)
    created = await _pool().run(target, f"mkdir -p {shlex.quote(remote_dir)}", timeout=15)
    if not created or created.returncode != 0:
        raise HTTPException(status_code=500, detail=f"Failed to create remote temp directory on {target}")
    local_payload = temp_dir / f"sync-{uuid.uuid4().hex}.json"
    local_payload.write_text(json.dumps(payload), encoding="utf-8")
    remote_payload = f"{remote_dir}/payload.json"
    script_payload = f"{remote_relative_dir}/payload.json"
    try:
        ok = await _pool().push_file(target, local_payload, remote_payload)
        if not ok:
            raise HTTPException(status_code=500, detail=f"Failed to upload sync payload to {target}")
        data = await _run_remote_python(
            target,
            _REMOTE_SYNC_APPLY_SCRIPT,
            [spec.db_name, spec.table, spec.user_col, mode, json.dumps(users), script_payload],
            timeout=120,
        )
        return {
            "fetched": int(data.get("fetched") or 0),
            "inserted": int(data.get("inserted") or 0),
            "skipped": int(data.get("skipped") or 0),
            "deleted": int(data.get("deleted") or 0),
        }
    finally:
        await _pool().run(target, f"rm -rf {shlex.quote(remote_dir)}", timeout=15)
        local_payload.unlink(missing_ok=True)


async def _verify_sync_target(source: str, target: str, users: list[str]) -> dict[str, Any]:
    source_stats = await _sync_table_stats_for_target(source, users)
    target_stats = await _sync_table_stats_for_target(target, users)
    source_tables = source_stats.get("tables") if isinstance(source_stats, dict) else {}
    target_tables = target_stats.get("tables") if isinstance(target_stats, dict) else {}
    tables: dict[str, Any] = {}
    ok = True
    for spec in TABLE_SPECS:
        key = _sync_table_key(spec)
        source_entry = source_tables.get(key, {}) if isinstance(source_tables, dict) else {}
        target_entry = target_tables.get(key, {}) if isinstance(target_tables, dict) else {}
        source_count = int(source_entry.get("count") or 0)
        target_count = int(target_entry.get("count") or 0)
        source_max = int(source_entry.get("max_timestamp") or 0)
        target_max = int(target_entry.get("max_timestamp") or 0)
        table_ok = source_count == target_count and (not spec.timestamp_col or source_max == target_max)
        ok = ok and table_ok
        tables[key] = {
            "ok": table_ok,
            "source_count": source_count,
            "target_count": target_count,
            "source_max_timestamp": source_max,
            "target_max_timestamp": target_max,
        }
    return {"ok": ok, "tables": tables}


async def sync_user_rows_incremental(
    source: str,
    target: str,
    users: list[str],
    temp_dir: Path,
    operation: OperationProgress | None = None,
) -> dict[str, Any]:
    """Synchronize selected users without backups using incremental SQL for append tables."""

    source = _target_id(source)
    target = _target_id(target)
    await _assert_known_target(source)
    await _assert_known_target(target)
    users = _clean_users(users)
    if not users:
        raise HTTPException(status_code=422, detail="Select at least one user")
    target_stats = await _sync_table_stats_for_target(target, users)
    target_tables = target_stats.get("tables") if isinstance(target_stats, dict) else {}
    tables: dict[str, Any] = {}
    for spec in TABLE_SPECS:
        key = _sync_table_key(spec)
        mode = _sync_table_mode(spec)
        if operation:
            operation.set_current(f"Sync {key} to {target}")
        cutoffs: dict[str, int] = {}
        if mode == "append":
            table_stats = target_tables.get(key, {}) if isinstance(target_tables, dict) else {}
            user_stats = table_stats.get("users", {}) if isinstance(table_stats, dict) else {}
            for user in users:
                max_ts = int((user_stats.get(user, {}) if isinstance(user_stats, dict) else {}).get("max_timestamp") or 0)
                cutoffs[user] = max(0, max_ts - SYNC_OVERLAP_MS)
        payload = await _fetch_sync_rows_for_target(source, spec, users, mode, cutoffs)
        stats = await _apply_sync_rows_for_target(target, spec, users, mode, payload, temp_dir)
        stats["mode"] = mode
        tables[key] = stats
        if operation:
            operation.advance(f"Synced {key} to {target}", stats)
    verify = await _verify_sync_target(source, target, users)
    if operation:
        operation.advance(f"Verified sync target {target}", {"ok": verify.get("ok")})
    return {
        "source_total": sum(int(item.get("fetched") or 0) for item in tables.values()),
        "fetched": sum(int(item.get("fetched") or 0) for item in tables.values()),
        "inserted": sum(int(item.get("inserted") or 0) for item in tables.values()),
        "skipped": sum(int(item.get("skipped") or 0) for item in tables.values()),
        "deleted": sum(int(item.get("deleted") or 0) for item in tables.values()),
        "tables": tables,
        "verify": verify,
    }


def delete_user_rows(
    db_paths: dict[str, Path],
    users: list[str],
    cutoff_ms: int | None = None,
    operation: OperationProgress | None = None,
) -> dict[str, Any]:
    """Delete selected users' rows; optionally only rows older than cutoff."""

    users = _clean_users(users)
    if not users:
        raise HTTPException(status_code=422, detail="Select at least one user")
    conns = _connect_bundle(db_paths)
    deleted: dict[str, int] = {}
    try:
        for spec in TABLE_SPECS:
            table_key = f"{spec.db_name}:{spec.table}"
            if operation:
                operation.set_current(f"Delete rows from {table_key}")
            conn = conns[spec.db_name]
            if not _table_exists(conn, spec.table):
                deleted[table_key] = 0
                if operation:
                    operation.advance(f"Skipped missing table {table_key}", {"deleted": 0})
                continue
            params: list[Any] = list(users)
            sql = f'DELETE FROM "{spec.table}" WHERE "{spec.user_col}" IN ({_placeholders(users)})'
            if cutoff_ms is not None:
                if not spec.timestamp_col:
                    deleted[table_key] = 0
                    if operation:
                        operation.advance(f"Skipped table without timestamp {table_key}", {"deleted": 0})
                    continue
                sql += f' AND "{spec.timestamp_col}" <= ?'
                params.append(int(cutoff_ms))
            cur = conn.execute(sql, params)
            conn.commit()
            deleted_count = int(cur.rowcount if cur.rowcount is not None else 0)
            deleted[table_key] = deleted_count
            if operation:
                operation.advance(f"Deleted rows from {table_key}", {"deleted": deleted_count})
        return {"total": sum(deleted.values()), "tables": deleted}
    finally:
        _close_bundle(conns)


def _row_exists(conn: sqlite3.Connection, table: str, cols: list[str], values: dict[str, Any]) -> bool:
    clauses = [f'"{col}" IS ?' for col in cols]
    params = [values.get(col) for col in cols]
    row = conn.execute(f'SELECT 1 FROM "{table}" WHERE {" AND ".join(clauses)} LIMIT 1', params).fetchone()
    return row is not None


def _copy_spec_rows(
    src: sqlite3.Connection,
    dst: sqlite3.Connection,
    spec: TableSpec,
    users: list[str],
    mode: Literal["replace", "add_missing"],
) -> dict[str, int]:
    if not _table_exists(src, spec.table) or not _table_exists(dst, spec.table):
        return {"source": 0, "inserted": 0, "skipped": 0}
    src_cols = _table_columns(src, spec.table)
    dst_cols = _table_columns(dst, spec.table)
    cols = [col for col in src_cols if col != "id" and col in dst_cols]
    if spec.user_col not in cols:
        return {"source": 0, "inserted": 0, "skipped": 0}
    rows = src.execute(
        f'SELECT {", ".join(f"\"{col}\"" for col in cols)} FROM "{spec.table}" '
        f'WHERE "{spec.user_col}" IN ({_placeholders(users)})',
        users,
    ).fetchall()
    if not rows:
        return {"source": 0, "inserted": 0, "skipped": 0}

    key_cols = [col for col in spec.key_cols if col in cols and col in dst_cols]
    if mode == "add_missing" and not key_cols:
        key_cols = cols
    insert_sql = (
        f'INSERT OR IGNORE INTO "{spec.table}" ({", ".join(f"\"{col}\"" for col in cols)}) '
        f'VALUES ({", ".join("?" for _ in cols)})'
    )
    inserted = 0
    skipped = 0
    for row in rows:
        values = dict(zip(cols, row))
        if mode == "add_missing" and _row_exists(dst, spec.table, key_cols, values):
            skipped += 1
            continue
        cur = dst.execute(insert_sql, [values.get(col) for col in cols])
        if cur.rowcount and cur.rowcount > 0:
            inserted += 1
        else:
            skipped += 1
    dst.commit()
    return {"source": len(rows), "inserted": inserted, "skipped": skipped}


def copy_user_rows(
    source_paths: dict[str, Path],
    target_paths: dict[str, Path],
    users: list[str],
    mode: Literal["replace", "add_missing"],
    operation: OperationProgress | None = None,
) -> dict[str, Any]:
    """Copy selected users from source DB bundle into target DB bundle."""

    users = _clean_users(users)
    if not users:
        raise HTTPException(status_code=422, detail="Select at least one user")
    if mode == "replace":
        delete_user_rows(target_paths, users, operation=operation)

    source_conns = _connect_bundle(source_paths)
    target_conns = _connect_bundle(target_paths)
    try:
        tables: dict[str, dict[str, int]] = {}
        for spec in TABLE_SPECS:
            table_key = f"{spec.db_name}:{spec.table}"
            if operation:
                operation.set_current(f"Copy rows for {table_key}")
            stats = _copy_spec_rows(source_conns[spec.db_name], target_conns[spec.db_name], spec, users, mode)
            tables[table_key] = stats
            if operation:
                operation.advance(f"Copied rows for {table_key}", stats)
        return {
            "source_total": sum(item["source"] for item in tables.values()),
            "inserted": sum(item["inserted"] for item in tables.values()),
            "skipped": sum(item["skipped"] for item in tables.values()),
            "tables": tables,
        }
    finally:
        _close_bundle(source_conns)
        _close_bundle(target_conns)


async def remote_delete_user_rows(
    target: str,
    users: list[str],
    cutoff_ms: int | None,
    operation: OperationProgress | None = None,
) -> dict[str, Any]:
    """Delete selected users directly on a remote target's live SQLite DBs."""

    users = _clean_users(users)
    deleted: dict[str, int] = {}
    users_json = json.dumps(users)
    for spec in TABLE_SPECS:
        table_key = f"{spec.db_name}:{spec.table}"
        if operation:
            operation.set_current(f"Delete rows from {table_key} on {target}")
        data = await _run_remote_python(
            target,
            _REMOTE_DELETE_SCRIPT,
            [
                _remote_path(target, "data", spec.db_name),
                spec.table,
                spec.user_col,
                spec.timestamp_col or "",
                "" if cutoff_ms is None else str(int(cutoff_ms)),
                users_json,
            ],
        )
        count = int(data.get("deleted") or 0)
        deleted[table_key] = count
        if operation:
            operation.advance(f"Deleted rows from {table_key} on {target}", {"deleted": count})
    return {"total": sum(deleted.values()), "tables": deleted}


async def _upload_source_snapshots(target: str, source_paths: dict[str, Path], operation: OperationProgress | None = None) -> dict[str, str]:
    """Upload source DB snapshots to a remote target for direct SQL import."""

    remote_dir = _remote_path(target, "data", "tmp", "db-tools", uuid.uuid4().hex)
    created = await _pool().run(target, f"mkdir -p {shlex.quote(remote_dir)}", timeout=15)
    if not created or created.returncode != 0:
        raise HTTPException(status_code=500, detail=f"Failed to create remote temp directory on {target}")
    remote_paths: dict[str, str] = {}
    for db_name, local_path in source_paths.items():
        if operation:
            operation.set_current(f"Upload source snapshot {db_name} to {target}")
        remote_path = f"{remote_dir}/{db_name}"
        ok = await _pool().push_file(target, local_path, remote_path)
        if not ok:
            raise HTTPException(status_code=500, detail=f"Failed to upload source snapshot {db_name} to {target}")
        remote_paths[db_name] = remote_path
        if operation:
            operation.advance(f"Uploaded source snapshot {db_name} to {target}", {"db": db_name})
    return remote_paths


async def _remove_remote_snapshots(target: str, remote_paths: dict[str, str]) -> None:
    """Remove uploaded remote source snapshots."""

    if not remote_paths:
        return
    parents = sorted({str(Path(path).parent) for path in remote_paths.values()})
    for parent in parents:
        await _pool().run(target, f"rm -rf {shlex.quote(parent)}", timeout=15)


async def remote_copy_user_rows(
    target: str,
    remote_source_paths: dict[str, str],
    users: list[str],
    mode: Literal["replace", "add_missing"],
    operation: OperationProgress | None = None,
) -> dict[str, Any]:
    """Copy selected users directly into a remote target's live SQLite DBs."""

    users = _clean_users(users)
    if mode == "replace":
        await remote_delete_user_rows(target, users, None, operation)
    users_json = json.dumps(users)
    tables: dict[str, dict[str, int]] = {}
    for spec in TABLE_SPECS:
        table_key = f"{spec.db_name}:{spec.table}"
        if operation:
            operation.set_current(f"Copy rows for {table_key} on {target}")
        stats = await _run_remote_python(
            target,
            _REMOTE_COPY_SCRIPT,
            [
                _remote_path(target, "data", spec.db_name),
                remote_source_paths[spec.db_name],
                spec.table,
                spec.user_col,
                mode,
                json.dumps(list(spec.key_cols)),
                users_json,
            ],
            timeout=120,
        )
        normalized = {
            "source": int(stats.get("source") or 0),
            "inserted": int(stats.get("inserted") or 0),
            "skipped": int(stats.get("skipped") or 0),
        }
        tables[table_key] = normalized
        if operation:
            operation.advance(f"Copied rows for {table_key} on {target}", normalized)
    return {
        "source_total": sum(item["source"] for item in tables.values()),
        "inserted": sum(item["inserted"] for item in tables.values()),
        "skipped": sum(item["skipped"] for item in tables.values()),
        "tables": tables,
    }


async def _stage_db_bundle(target: str, temp_dir: Path, prefix: str, operation: OperationProgress | None = None) -> dict[str, Path]:
    await _assert_known_target(target)
    result: dict[str, Path] = {}
    for db_name in DB_FILE_NAMES:
        if operation:
            operation.set_current(f"Load {db_name} from {target}")
        local_path = temp_dir / f"{prefix}-{db_name}"
        if target == "local":
            src = _local_db_path(db_name)
            if src.exists():
                _sqlite_backup_file(src, local_path)
        else:
            remote = _remote_path(target, "data", db_name)
            snapshot = _remote_path(target, "data", "backup", "db-tools", f"stage-{_timestamp()}-{uuid.uuid4().hex}-{db_name}")
            backup_cmd = _remote_sqlite_backup_command(remote, snapshot)
            backup_result = await _pool().run(target, backup_cmd, timeout=60)
            if backup_result and backup_result.returncode == 0:
                ok = await _pool().pull_file(target, snapshot, local_path)
                await _pool().run(target, f"rm -f {shlex.quote(snapshot)}", timeout=10)
                if not ok:
                    local_path.unlink(missing_ok=True)
            else:
                local_path.unlink(missing_ok=True)
        if db_name == MAIN_DB_NAME and not local_path.exists():
            raise HTTPException(status_code=404, detail=f"Required database file not found on {target}: {db_name}")
        _ensure_schema(local_path, db_name)
        result[db_name] = local_path
        if operation:
            operation.advance(f"Loaded {db_name} from {target}", {"target": target, "db": db_name})
    return result


def _backup_local_file(path: Path, label: str) -> str:
    if not path.exists():
        return ""
    target_dir = _backup_dir()
    target_dir.mkdir(parents=True, exist_ok=True)
    backup = target_dir / f"db-tools-{_timestamp()}-{label}-{path.name}"
    if path.name in DB_FILE_NAMES:
        _sqlite_backup_file(path, backup)
    else:
        shutil.copy2(path, backup)
    return str(backup)


async def _backup_remote_file(target: str, remote_path: str, label: str) -> str:
    backup_remote = _remote_path(target, "data", "backup", "db-tools", f"db-tools-{_timestamp()}-{label}-{Path(remote_path).name}")
    if Path(remote_path).name in DB_FILE_NAMES:
        cmd = f"if [ -f {shlex.quote(remote_path)} ]; then {_remote_sqlite_backup_command(remote_path, backup_remote)}; fi"
    else:
        cmd = (
            f"mkdir -p {shlex.quote(str(Path(backup_remote).parent))} && "
            f"if [ -f {shlex.quote(remote_path)} ]; then cp {shlex.quote(remote_path)} {shlex.quote(backup_remote)}; fi"
        )
    result = await _pool().run(target, cmd, timeout=30)
    if not result or result.returncode != 0:
        raise HTTPException(status_code=500, detail=f"Failed to create remote backup on {target}")
    return f"{target}:{backup_remote}"


def _backup_file_info(path: Path) -> dict[str, Any]:
    name = _validate_backup_name(path.name)
    db_name = _backup_db_name(name)
    stat = path.stat()
    return {
        "name": name,
        "db_name": db_name,
        "label": _backup_label(name, db_name),
        "size": stat.st_size,
        "mtime": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(),
    }


async def _list_backup_files(target: str) -> list[dict[str, Any]]:
    """List direct DB backup files for a target's DB tools backup folder."""

    if target == "local":
        base = _backup_dir()
        if not base.exists():
            return []
        items = [_backup_file_info(path) for path in base.iterdir() if path.is_file() and any(path.name.endswith(db_name) for db_name in DB_FILE_NAMES)]
        return sorted(items, key=lambda item: str(item.get("mtime") or ""), reverse=True)

    script = r"""
import json, pathlib
base = pathlib.Path('data/backup/db-tools')
allowed = ('pbgui.db', 'pbgui_trades.db')
items = []
if base.exists():
    for path in base.iterdir():
        if not path.is_file() or not path.name.endswith(allowed):
            continue
        stat = path.stat()
        db_name = next(db for db in allowed if path.name.endswith(db))
        stem = path.name[:-len(db_name)].rstrip('-')
        label = stem
        if stem.startswith('db-tools-'):
            parts = stem.split('-', 3)
            if len(parts) == 4:
                label = parts[3]
        items.append({'name': path.name, 'db_name': db_name, 'label': label, 'size': stat.st_size, 'mtime': stat.st_mtime})
items.sort(key=lambda item: item['mtime'], reverse=True)
print(json.dumps(items))
"""
    data = await _run_remote_python(target, script, [], timeout=30)
    result: list[dict[str, Any]] = []
    for item in data if isinstance(data, list) else []:
        name = _validate_backup_name(str(item.get("name") or ""))
        db_name = _backup_db_name(name)
        mtime = float(item.get("mtime") or 0)
        result.append(
            {
                "name": name,
                "db_name": db_name,
                "label": str(item.get("label") or _backup_label(name, db_name)),
                "size": int(item.get("size") or 0),
                "mtime": datetime.fromtimestamp(mtime, timezone.utc).isoformat() if mtime else "",
            }
        )
    return result


def _assert_sqlite_integrity(path: Path) -> None:
    try:
        uri = f"file:{path}?mode=ro"
        with sqlite3.connect(uri, uri=True) as conn:
            row = conn.execute("PRAGMA integrity_check").fetchone()
    except sqlite3.Error as exc:
        raise HTTPException(status_code=400, detail=f"Backup is not a readable SQLite database: {path.name}") from exc
    if not row or str(row[0]).lower() != "ok":
        raise HTTPException(status_code=400, detail=f"Backup integrity check failed: {path.name}")


async def _stage_backup_files(target: str, backups: list[str], temp_dir: Path, operation: OperationProgress | None = None) -> dict[str, Path]:
    staged: dict[str, Path] = {}
    for raw_name in backups:
        name = _validate_backup_name(raw_name)
        db_name = _backup_db_name(name)
        if db_name in staged:
            raise HTTPException(status_code=400, detail=f"Only one backup can be restored per DB file: {db_name}")
        local_path = temp_dir / f"restore-{db_name}"
        if operation:
            operation.set_current(f"Load backup {name}")
        if target == "local":
            src = _backup_dir() / name
            if not src.exists() or not src.is_file():
                raise HTTPException(status_code=404, detail=f"Backup not found: {name}")
            shutil.copy2(src, local_path)
        else:
            remote = _remote_path(target, "data", "backup", "db-tools", name)
            ok = await _pool().pull_file(target, remote, local_path)
            if not ok:
                raise HTTPException(status_code=404, detail=f"Backup not found on {target}: {name}")
        _assert_sqlite_integrity(local_path)
        staged[db_name] = local_path
        if operation:
            operation.advance(f"Loaded backup {name}", {"target": target, "db": db_name})
    if not staged:
        raise HTTPException(status_code=400, detail="Select at least one backup")
    return staged


async def _delete_backup_files(target: str, backups: list[str]) -> list[str]:
    deleted: list[str] = []
    names = [_validate_backup_name(name) for name in backups]
    if not names:
        raise HTTPException(status_code=400, detail="Select at least one backup")
    if target == "local":
        base = _backup_dir()
        for name in names:
            path = base / name
            if path.exists() and path.is_file():
                path.unlink()
                deleted.append(name)
        return deleted
    script = r"""
import json, pathlib, sys
base = pathlib.Path('data/backup/db-tools')
deleted = []
for name in sys.argv[1:]:
    path = base / name
    if path.exists() and path.is_file():
        path.unlink()
        deleted.append(name)
print(json.dumps(deleted))
"""
    data = await _run_remote_python(target, script, names, timeout=30)
    return [str(item) for item in data if isinstance(item, str)] if isinstance(data, list) else []


def _pbdata_stop_local() -> bool:
    try:
        from PBData import PBData

        pb = PBData()
        if pb.is_running():
            pb.stop()
            return True
    except Exception as exc:
        _log(SERVICE, f"local PBData stop failed: {exc}", level="WARNING", meta={"traceback": traceback.format_exc()})
    return False


def _pbdata_start_local() -> None:
    try:
        from PBData import PBData

        PBData().run()
    except Exception as exc:
        _log(SERVICE, f"local PBData start failed: {exc}", level="WARNING", meta={"traceback": traceback.format_exc()})


async def _pbdata_stop_remote(target: str) -> str:
    pbgui_dir = _remote_pbgui_dir(target)
    cmd = f"""
cd {shlex.quote(pbgui_dir)} || exit 1
if command -v systemctl >/dev/null 2>&1 && systemctl --user is-active --quiet pbgui-pbdata.service; then
  systemctl --user stop pbgui-pbdata.service && printf systemd
elif [ -f data/pid/pbdata.pid ] && kill -0 "$(cat data/pid/pbdata.pid 2>/dev/null)" 2>/dev/null; then
  python3 - <<'PY'
from PBData import PBData
PBData().stop()
PY
  printf legacy
else
  printf none
fi
""".strip()
    result = await _pool().run(target, cmd, timeout=30)
    if not result or result.returncode != 0:
        _log(SERVICE, f"remote PBData stop failed on {target}", level="WARNING")
        return "none"
    marker = str(result.stdout or "").strip().splitlines()[-1] if str(result.stdout or "").strip() else "none"
    return marker if marker in {"systemd", "legacy"} else "none"


async def _pbdata_start_remote(target: str, marker: str) -> None:
    if marker == "none":
        return
    pbgui_dir = _remote_pbgui_dir(target)
    if marker == "systemd":
        cmd = "systemctl --user start pbgui-pbdata.service"
    else:
        cmd = f"cd {shlex.quote(pbgui_dir)} && nohup python3 -u PBData.py >/dev/null 2>&1 &"
    result = await _pool().run(target, cmd, timeout=15)
    if not result or result.returncode != 0:
        _log(SERVICE, f"remote PBData start failed on {target}", level="WARNING")


async def _stop_target_pbdata(target: str, operation: OperationProgress | None = None) -> bool | str:
    """Stop PBData on a DB write target before staging its database files."""

    if operation:
        operation.set_current(f"Stop PBData on {target}")
    marker: bool | str
    if target == "local":
        marker = _pbdata_stop_local()
        detail = {"was_running": bool(marker)}
    else:
        marker = await _pbdata_stop_remote(target)
        detail = {"was_running": bool(marker and marker != "none")}
    if operation:
        operation.advance(f"Stopped PBData on {target}", detail)
    return marker


async def _start_target_pbdata(target: str, marker: bool | str, operation: OperationProgress | None = None) -> None:
    """Restart PBData after a DB write operation if it was running before."""

    was_running = bool(marker and marker != "none")
    if operation:
        operation.set_current(f"Restart PBData on {target}")
    if target == "local":
        if was_running:
            _pbdata_start_local()
            label = f"Restarted PBData on {target}"
        else:
            label = f"PBData restart not needed on {target}"
    else:
        await _pbdata_start_remote(target, str(marker or "none"))
        label = f"PBData restart checked on {target}"
    if operation:
        operation.advance(label, {"was_running": was_running})


async def _install_db_bundle(
    target: str,
    staged_paths: dict[str, Path],
    label: str,
    operation: OperationProgress | None = None,
    manage_pbdata: bool = True,
) -> dict[str, Any]:
    backups: list[str] = []
    pbdata_marker: bool | str = False
    try:
        if target == "local":
            for db_name in DB_FILE_NAMES:
                if operation:
                    operation.set_current(f"Back up {db_name} on {target}")
                backups.append(_backup_local_file(_local_db_path(db_name), label))
                if operation:
                    operation.advance(f"Backed up {db_name} on {target}", {"target": target, "db": db_name})
            if manage_pbdata:
                pbdata_marker = await _stop_target_pbdata(target, operation)
            for db_name, src in staged_paths.items():
                if operation:
                    operation.set_current(f"Install {db_name} on {target}")
                dst = _local_db_path(db_name)
                dst.parent.mkdir(parents=True, exist_ok=True)
                tmp = dst.with_suffix(dst.suffix + ".tmp")
                shutil.copy2(src, tmp)
                _remove_sqlite_sidecars(dst)
                os.replace(tmp, dst)
                _remove_sqlite_sidecars(dst)
                if operation:
                    operation.advance(f"Installed {db_name} on {target}", {"target": target, "db": db_name})
        else:
            for db_name in DB_FILE_NAMES:
                if operation:
                    operation.set_current(f"Back up {db_name} on {target}")
                backups.append(await _backup_remote_file(target, _remote_path(target, "data", db_name), label))
                if operation:
                    operation.advance(f"Backed up {db_name} on {target}", {"target": target, "db": db_name})
            if manage_pbdata:
                pbdata_marker = await _stop_target_pbdata(target, operation)
            await _pool().run(target, f"mkdir -p {shlex.quote(_remote_path(target, 'data'))}", timeout=15)
            for db_name, src in staged_paths.items():
                if operation:
                    operation.set_current(f"Upload {db_name} to {target}")
                remote = _remote_path(target, "data", db_name)
                tmp_remote = remote + ".tmp"
                ok = await _pool().push_file(target, src, tmp_remote)
                if not ok:
                    raise HTTPException(status_code=500, detail=f"Failed to upload {db_name} to {target}")
                move = await _pool().run(
                    target,
                    f"rm -f {shlex.quote(remote + '-wal')} {shlex.quote(remote + '-shm')} && mv {shlex.quote(tmp_remote)} {shlex.quote(remote)} && rm -f {shlex.quote(remote + '-wal')} {shlex.quote(remote + '-shm')}",
                    timeout=30,
                )
                if not move or move.returncode != 0:
                    raise HTTPException(status_code=500, detail=f"Failed to install {db_name} on {target}")
                if operation:
                    operation.advance(f"Installed {db_name} on {target}", {"target": target, "db": db_name})
        return {"backups": [item for item in backups if item], "pbdata_was_running": bool(pbdata_marker and pbdata_marker != "none")}
    finally:
        if manage_pbdata:
            await _start_target_pbdata(target, pbdata_marker, operation)


def _dashboard_base_path(template: bool) -> Path:
    base = _data_dir() / "dashboards"
    return base / "templates" if template else base


async def _list_json_items(target: str, template: bool) -> list[str]:
    await _assert_known_target(target)
    if target == "local":
        base = _dashboard_base_path(template)
        return sorted(path.stem for path in base.glob("*.json") if path.is_file())
    remote_dir = _remote_path(target, "data", "dashboards", "templates" if template else "")
    entries = await _pool().list_remote_dir(target, remote_dir.rstrip("/"))
    return sorted(Path(name).stem for name in entries if str(name).endswith(".json"))


async def _read_json_item(target: str, name: str, template: bool) -> bytes | None:
    filename = f"{_validate_item_name(name, 'dashboard/template')}.json"
    if target == "local":
        path = _dashboard_base_path(template) / filename
        return path.read_bytes() if path.exists() else None
    remote = _remote_path(target, "data", "dashboards", "templates" if template else "", filename)
    return await _pool().read_remote_file(target, remote)


async def _write_json_item(target: str, name: str, template: bool, content: bytes, overwrite: bool, label: str) -> str:
    filename = f"{_validate_item_name(name, 'dashboard/template')}.json"
    if target == "local":
        path = _dashboard_base_path(template) / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        existed = path.exists()
        if existed and not overwrite:
            return "skipped"
        if existed:
            _backup_local_file(path, label)
        tmp = path.with_suffix(".tmp")
        tmp.write_bytes(content)
        tmp.replace(path)
        return "replaced" if existed else "created"
    remote_dir = _remote_path(target, "data", "dashboards", "templates" if template else "").rstrip("/")
    remote = f"{remote_dir}/{filename}"
    existing = set(await _list_json_items(target, template))
    if name in existing and not overwrite:
        return "skipped"
    if name in existing:
        await _backup_remote_file(target, remote, label)
    with tempfile.TemporaryDirectory(prefix="pbgui-db-tools-json-") as tmpdir:
        local = Path(tmpdir) / filename
        local.write_bytes(content)
        await _pool().run(target, f"mkdir -p {shlex.quote(remote_dir)}", timeout=15)
        ok = await _pool().push_file(target, local, remote + ".tmp")
        if not ok:
            raise HTTPException(status_code=500, detail=f"Failed to upload {filename} to {target}")
        move = await _pool().run(target, f"mv {shlex.quote(remote + '.tmp')} {shlex.quote(remote)}", timeout=15)
        if not move or move.returncode != 0:
            raise HTTPException(status_code=500, detail=f"Failed to install {filename} on {target}")
    return "replaced" if name in existing else "created"


@router.get("/main_page", response_class=HTMLResponse)
def get_main_page(request: Request, session: SessionToken = Depends(require_auth)) -> HTMLResponse:
    """Serve the standalone DB Tools page."""

    html_path = Path(__file__).parent.parent / "frontend" / "db_tools.html"
    html = html_path.read_text(encoding="utf-8")
    scheme = request.url.scheme
    host = request.url.hostname or "127.0.0.1"
    port = request.url.port
    origin = f"{scheme}://{host}" + (f":{port}" if port else "")
    api_base = origin + "/api/db-tools"
    ws_base = origin.replace("http://", "ws://").replace("https://", "wss://")

    html = html.replace('"%%TOKEN%%"', json.dumps(session.token))
    html = html.replace('"%%API_BASE%%"', json.dumps(api_base))
    html = html.replace('"%%WS_BASE%%"', json.dumps(ws_base))
    from pbgui_purefunc import PBGUI_SERIAL, PBGUI_VERSION

    html = html.replace('"%%VERSION%%"', json.dumps(PBGUI_VERSION))
    html = html.replace("%%VERSION%%", PBGUI_VERSION)
    html = html.replace('"%%SERIAL%%"', json.dumps(PBGUI_SERIAL))
    html = html.replace("%%SERIAL%%", PBGUI_SERIAL)
    nav_js = Path(__file__).parent.parent / "frontend" / "pbgui_nav.js"
    nav_hash = str(int(nav_js.stat().st_mtime)) if nav_js.exists() else PBGUI_VERSION
    html = html.replace("%%NAV_HASH%%", nav_hash)
    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})


@router.get("/targets")
async def get_targets(session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Return local and known remote master targets."""

    del session
    return {"targets": await _known_targets()}


@router.get("/operations/{operation_id}")
def get_operation(operation_id: str, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Return progress for a running or recently finished DB tools operation."""

    del session
    operation = _operations.get(operation_id)
    if operation is None:
        raise HTTPException(status_code=404, detail="Operation not found")
    return {"operation": operation.to_dict()}


@router.get("/backups")
async def get_backups(target: str = "local", session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """List DB backup files for a target."""

    del session
    target = _target_id(target)
    await _assert_known_target(target)
    return {"target": target, "backups": await _list_backup_files(target)}


@router.post("/backups/restore/run")
async def restore_backups_run(payload: BackupActionRequest, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Restore selected DB backup files to a target."""

    del session
    target = _target_id(payload.target)
    backups = [_validate_backup_name(name) for name in payload.backups]
    if not backups:
        raise HTTPException(status_code=400, detail="Select at least one backup")
    await _assert_known_target(target)

    async def runner(operation: OperationProgress) -> dict[str, Any]:
        with tempfile.TemporaryDirectory(prefix="pbgui-db-tools-restore-") as tmp:
            staged = await _stage_backup_files(target, backups, Path(tmp), operation)
            install = await _install_db_bundle(target, staged, "restore-backup", operation)
            _log(SERVICE, f"restore backups target={target} backups={backups}", level="INFO")
            return {"ok": True, "target": target, "restored": list(staged.keys()), "source_backups": backups, **install}

    operation = _start_operation("restore-backups", _operation_total("restore-backups", len(backups)), runner)
    return {"operation": operation.to_dict()}


@router.post("/backups/delete")
async def delete_backups(payload: BackupActionRequest, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Delete selected DB backup files from a target."""

    del session
    target = _target_id(payload.target)
    await _assert_known_target(target)
    deleted = await _delete_backup_files(target, payload.backups)
    _log(SERVICE, f"delete backups target={target} backups={deleted}", level="INFO")
    return {"ok": True, "target": target, "deleted": deleted}


@router.get("/sync/jobs")
async def get_sync_jobs(session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """List configured one-way user sync jobs."""

    del session
    _ensure_sync_scheduler()
    return {"jobs": [_sync_job_public(job) for job in sorted(_sync_jobs.values(), key=lambda item: str(item.get("name") or "").lower())]}


@router.post("/sync/safety")
async def sync_safety(payload: SyncJobRequest, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Preview PBData writer conflicts for a sync job."""

    del session
    job = _validate_sync_job_payload(payload)
    await _assert_known_target(str(job.get("source") or "local"))
    users = set(job.get("users") or [])
    target_status: dict[str, Any] = {}
    blocked: dict[str, list[str]] = {}
    for target in job.get("targets") or []:
        await _assert_known_target(str(target))
        status = await _pbdata_active_users(str(target))
        active = {str(item) for item in status.get("active_users") or []}
        hits = sorted(active.intersection(users), key=str.lower)
        target_status[str(target)] = status
        if hits:
            blocked[str(target)] = hits
    conflicts: list[dict[str, Any]] = []
    if job.get("enabled"):
        for other in _sync_jobs.values():
            if other.get("id") == job.get("id") or not other.get("enabled"):
                continue
            shared_users = sorted(set(other.get("users") or []).intersection(users), key=str.lower)
            if shared_users and str(other.get("source")) != str(job.get("source")):
                conflicts.append({"job_id": other.get("id"), "job_name": other.get("name"), "source": other.get("source"), "users": shared_users})
    return {"ok": not blocked and not conflicts, "job": job, "targets": target_status, "blocked": blocked, "conflicts": conflicts}


@router.post("/sync/jobs")
async def save_sync_job(payload: SyncJobRequest, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Create or update a one-way row-level user sync job."""

    del session
    job = _validate_sync_job_payload(payload)
    await _validate_sync_job_safety(job)
    if job.get("enabled") and not job.get("next_run"):
        job["next_run"] = _next_sync_run_iso(int(job.get("interval_seconds") or 60))
    _sync_jobs[str(job["id"])] = job
    _save_sync_jobs()
    _ensure_sync_scheduler()
    _log_sync_job(str(job["id"]), "job_saved", name=job.get("name"), source=job.get("source"), targets=job.get("targets"), users=job.get("users"), enabled=job.get("enabled"))
    _log(SERVICE, f"save sync job name={job.get('name')} source={job.get('source')} targets={job.get('targets')} users={job.get('users')} enabled={job.get('enabled')}", level="INFO")
    return {"ok": True, "job": _sync_job_public(job)}


@router.delete("/sync/jobs/{job_id}")
async def delete_sync_job(job_id: str, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Delete a configured sync job."""

    del session
    if job_id in _sync_job_locks:
        raise HTTPException(status_code=409, detail="Sync job is running")
    removed = _sync_jobs.pop(job_id, None)
    if not removed:
        raise HTTPException(status_code=404, detail="Sync job not found")
    _log_sync_job(job_id, "job_deleted", name=removed.get("name"))
    _save_sync_jobs()
    _log(SERVICE, f"delete sync job name={removed.get('name')}", level="INFO")
    return {"ok": True, "deleted": job_id}


@router.post("/sync/jobs/{job_id}/run")
async def run_sync_job(job_id: str, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Run a configured sync job immediately and return operation progress."""

    del session
    job = _sync_jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Sync job not found")
    target_steps = 0
    for _target in job.get("targets") or []:
        target_steps += len(TABLE_SPECS) + 1

    async def runner(operation: OperationProgress) -> dict[str, Any]:
        return await _run_sync_job(job_id, manual=True, operation=operation)

    operation = _start_operation("sync-job", _operation_total("sync-job", target_steps), runner)
    return {"operation": operation.to_dict()}


@router.get("/users")
async def get_users(target: str = "local", session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Return users present in a target DB bundle with row counts."""

    del session
    target = _target_id(target)
    return {"target": target, "users": await list_users_for_target(target)}


@router.post("/cleanup/preview")
async def cleanup_preview(payload: CleanupPreviewRequest, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Preview user data cleanup."""

    del session
    users = _clean_users(payload.users)
    target = _target_id(payload.target)
    return {
        "target": target,
        "users": users,
        "cutoff_ms": payload.cutoff_ms,
        "counts": await count_user_rows_for_target(target, users, payload.cutoff_ms),
    }


@router.post("/cleanup/run")
async def cleanup_run(payload: CleanupRunRequest, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Delete user data from a target, with target DB backups."""

    del session
    target = _target_id(payload.target)
    users = _clean_users(payload.users)
    if not users:
        raise HTTPException(status_code=422, detail="Select at least one user")
    cutoff_ms = int(payload.cutoff_ms) if payload.mode == "older" and payload.cutoff_ms is not None else None
    if payload.mode == "older" and cutoff_ms is None:
        raise HTTPException(status_code=422, detail="cutoff_ms is required for older cleanup")
    await _assert_known_target(target)

    async def runner(operation: OperationProgress) -> dict[str, Any]:
        backups = await _backup_target_dbs(target, "cleanup", operation)
        if target == "local":
            deleted = delete_user_rows(_target_db_paths_local(), users, cutoff_ms, operation)
        else:
            deleted = await remote_delete_user_rows(target, users, cutoff_ms, operation)
        _log(SERVICE, f"cleanup target={target} users={users} mode={payload.mode} deleted={deleted['total']}", level="INFO")
        return {"ok": True, "target": target, "users": users, "deleted": deleted, "backups": backups, "pbdata_was_running": False}

    operation = _start_operation("cleanup", _operation_total("cleanup"), runner)
    return {"operation": operation.to_dict()}


@router.post("/users/copy/preview")
async def copy_users_preview(payload: CopyUsersRequest, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Preview copying selected users between masters."""

    del session
    source = _target_id(payload.source)
    target = _target_id(payload.target)
    users = _clean_users(payload.users)
    if not users:
        raise HTTPException(status_code=422, detail="Select at least one user")
    if source == target:
        raise HTTPException(status_code=400, detail="Source and target must differ")
    return {
        "source": source,
        "target": target,
        "users": users,
        "mode": payload.mode,
        "source_counts": await count_user_rows_for_target(source, users),
        "target_counts": await count_user_rows_for_target(target, users),
    }


@router.post("/users/copy/run")
async def copy_users_run(payload: CopyUsersRequest, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Copy selected users between masters."""

    del session
    source = _target_id(payload.source)
    target = _target_id(payload.target)
    users = _clean_users(payload.users)
    if source == target:
        raise HTTPException(status_code=400, detail="Source and target must differ")
    await _assert_known_target(source)
    await _assert_known_target(target)

    async def runner(operation: OperationProgress) -> dict[str, Any]:
        with tempfile.TemporaryDirectory(prefix="pbgui-db-tools-copy-") as tmp:
            tmp_path = Path(tmp)
            source_paths = await _stage_db_bundle(source, tmp_path, "source", operation)
            backups = await _backup_target_dbs(target, "copy-users", operation)
            remote_source_paths: dict[str, str] = {}
            try:
                if target == "local":
                    copied = copy_user_rows(source_paths, _target_db_paths_local(), users, payload.mode, operation)
                else:
                    remote_source_paths = await _upload_source_snapshots(target, source_paths, operation)
                    copied = await remote_copy_user_rows(target, remote_source_paths, users, payload.mode, operation)
                _log(SERVICE, f"copy users source={source} target={target} users={users} mode={payload.mode} inserted={copied['inserted']}", level="INFO")
                return {"ok": True, "source": source, "target": target, "users": users, "copied": copied, "backups": backups, "pbdata_was_running": False}
            finally:
                if remote_source_paths:
                    await _remove_remote_snapshots(target, remote_source_paths)

    operation = _start_operation(
        "copy-users",
        _operation_total("copy-users", replace=payload.mode == "replace", remote_target=target != "local"),
        runner,
    )
    return {"operation": operation.to_dict()}


@router.post("/database/copy/preview")
async def copy_database_preview(payload: CopyDatabaseRequest, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Preview full DB copy between masters."""

    del session
    source = _target_id(payload.source)
    target = _target_id(payload.target)
    if source == target:
        raise HTTPException(status_code=400, detail="Source and target must differ")
    with tempfile.TemporaryDirectory(prefix="pbgui-db-tools-full-preview-") as tmp:
        source_paths = await _stage_db_bundle(source, Path(tmp), "source")
        files = []
        for db_name in DB_FILE_NAMES:
            files.append({"name": db_name, "size": source_paths[db_name].stat().st_size})
        return {"source": source, "target": target, "files": files, "backup_required": True}


@router.post("/database/copy/run")
async def copy_database_run(payload: CopyDatabaseRequest, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Copy both PBGui DB files from source to target."""

    del session
    source = _target_id(payload.source)
    target = _target_id(payload.target)
    if source == target:
        raise HTTPException(status_code=400, detail="Source and target must differ")
    await _assert_known_target(source)
    await _assert_known_target(target)

    async def runner(operation: OperationProgress) -> dict[str, Any]:
        with tempfile.TemporaryDirectory(prefix="pbgui-db-tools-full-") as tmp:
            source_paths = await _stage_db_bundle(source, Path(tmp), "source", operation)
            pbdata_marker = await _stop_target_pbdata(target, operation)
            try:
                install = await _install_db_bundle(target, source_paths, "full-db-copy", operation, manage_pbdata=False)
                _log(SERVICE, f"copy full database source={source} target={target}", level="INFO")
                return {"ok": True, "source": source, "target": target, **install, "pbdata_was_running": bool(pbdata_marker and pbdata_marker != "none")}
            finally:
                await _start_target_pbdata(target, pbdata_marker, operation)

    operation = _start_operation("copy-database", _operation_total("copy-database"), runner)
    return {"operation": operation.to_dict()}


@router.get("/dashboards")
async def get_dashboards(target: str = "local", session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """List dashboards and templates for a master target."""

    del session
    target = _target_id(target)
    return {
        "target": target,
        "dashboards": await _list_json_items(target, False),
        "templates": await _list_json_items(target, True),
    }


@router.post("/dashboards/copy/preview")
async def copy_dashboards_preview(payload: DashboardCopyRequest, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Preview dashboard/template copy."""

    del session
    source = _target_id(payload.source)
    target = _target_id(payload.target)
    if source == target:
        raise HTTPException(status_code=400, detail="Source and target must differ")
    dashboards = [_validate_item_name(name, "dashboard") for name in payload.dashboards]
    templates = [_validate_item_name(name, "template") for name in payload.templates]
    target_dashboards = set(await _list_json_items(target, False))
    target_templates = set(await _list_json_items(target, True))
    return {
        "source": source,
        "target": target,
        "mode": payload.mode,
        "dashboards": {"selected": dashboards, "existing": sorted(set(dashboards) & target_dashboards)},
        "templates": {"selected": templates, "existing": sorted(set(templates) & target_templates)},
    }


@router.post("/dashboards/copy/run")
async def copy_dashboards_run(payload: DashboardCopyRequest, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Copy selected dashboards and templates between masters."""

    del session
    source = _target_id(payload.source)
    target = _target_id(payload.target)
    if source == target:
        raise HTTPException(status_code=400, detail="Source and target must differ")
    dashboards = [_validate_item_name(item, "dashboard") for item in payload.dashboards]
    templates = [_validate_item_name(item, "template") for item in payload.templates]
    await _assert_known_target(source)
    await _assert_known_target(target)

    async def runner(operation: OperationProgress) -> dict[str, Any]:
        overwrite = payload.mode == "replace_all"
        results: dict[str, dict[str, str]] = {"dashboards": {}, "templates": {}}
        operation.advance("Validated dashboard copy targets", {"source": source, "target": target})
        for name in dashboards:
            operation.set_current(f"Copy dashboard {name}")
            content = await _read_json_item(source, name, False)
            if content is None:
                results["dashboards"][name] = "missing_source"
                operation.advance(f"Dashboard missing on source: {name}", {"status": "missing_source"})
                continue
            status = await _write_json_item(target, name, False, content, overwrite, "dashboard")
            results["dashboards"][name] = status
            operation.advance(f"Copied dashboard {name}", {"status": status})
        for name in templates:
            operation.set_current(f"Copy template {name}")
            content = await _read_json_item(source, name, True)
            if content is None:
                results["templates"][name] = "missing_source"
                operation.advance(f"Template missing on source: {name}", {"status": "missing_source"})
                continue
            status = await _write_json_item(target, name, True, content, overwrite, "template")
            results["templates"][name] = status
            operation.advance(f"Copied template {name}", {"status": status})
        _log(SERVICE, f"copy dashboards source={source} target={target} mode={payload.mode}", level="INFO")
        return {"ok": True, "source": source, "target": target, "results": results}

    operation = _start_operation("copy-dashboards", _operation_total("copy-dashboards", len(dashboards) + len(templates)), runner)
    return {"operation": operation.to_dict()}
