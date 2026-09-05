"""Bounded SQLite snapshots and in-place restore; callers own the live DB lease.

Published restore sources must be immutable standalone snapshots. Live database
names must remain stable under the caller's lease. Linux descriptor paths keep
staging beneath the pinned approved directory even if its pathname is renamed.
"""

from __future__ import annotations

import math
import os
import re
import sqlite3
import stat
import time
from contextlib import ExitStack, closing, contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory

from secure_files import PRIVATE_DIR_MODE, PRIVATE_FILE_MODE

SERVICE = "SQLiteBackup"
# Ordered (name, declared type, NOT NULL, primary-key position) from Database.py.
_CORE_SCHEMA = {
    "history": [
        ("id", "INTEGER", 0, 1), ("symbol", "TEXT", 1, 0),
        ("timestamp", "INTEGER", 1, 0), ("income", "REAL", 1, 0),
        ("uniqueid", "TEXT", 1, 0), ("user", "TEXT", 1, 0),
    ],
    "position": [
        ("id", "INTEGER", 0, 1), ("symbol", "TEXT", 1, 0),
        ("timestamp", "INTEGER", 1, 0), ("psize", "REAL", 1, 0),
        ("upnl", "REAL", 1, 0), ("entry", "REAL", 1, 0),
        ("user", "TEXT", 1, 0), ("side", "TEXT", 0, 0),
    ],
    "orders": [
        ("id", "INTEGER", 0, 1), ("symbol", "TEXT", 1, 0),
        ("timestamp", "INTEGER", 1, 0), ("amount", "REAL", 1, 0),
        ("price", "REAL", 1, 0), ("side", "TEXT", 1, 0),
        ("uniqueid", "TEXT", 1, 0), ("user", "TEXT", 1, 0),
    ],
    "prices": [
        ("id", "INTEGER", 0, 1), ("symbol", "TEXT", 1, 0),
        ("timestamp", "INTEGER", 1, 0), ("price", "REAL", 1, 0),
        ("user", "TEXT", 1, 0),
    ],
    "balances": [
        ("id", "INTEGER", 0, 1), ("timestamp", "INTEGER", 1, 0),
        ("balance", "REAL", 1, 0), ("user", "TEXT", 1, 0),
    ],
    "history_scan_meta": [
        ("user", "TEXT", 1, 1), ("exchange", "TEXT", 1, 2),
        ("last_scan_ts", "INTEGER", 1, 0),
    ],
}

# Bound allocations made by a single VM opcode as well as schema compilation.
# Ordinary PBGui rows are small; the larger row budget also accommodates raw_json.
_SQLITE_LIMITS = {
    sqlite3.SQLITE_LIMIT_LENGTH: 8 * 1024 * 1024,
    sqlite3.SQLITE_LIMIT_SQL_LENGTH: 64 * 1024,
    sqlite3.SQLITE_LIMIT_COLUMN: 64,
    sqlite3.SQLITE_LIMIT_EXPR_DEPTH: 32,
    sqlite3.SQLITE_LIMIT_COMPOUND_SELECT: 16,
    sqlite3.SQLITE_LIMIT_VDBE_OP: 250_000,
    sqlite3.SQLITE_LIMIT_FUNCTION_ARG: 32,
    sqlite3.SQLITE_LIMIT_ATTACHED: 0,
    sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER: 128,
    sqlite3.SQLITE_LIMIT_TRIGGER_DEPTH: 0,
    sqlite3.SQLITE_LIMIT_WORKER_THREADS: 0,
}
_DDL_TOKEN = re.compile(r'\s+|--[^\n]*(?:\n|$)|/\*.*?\*/|"(?:[^"]|"")*"|`(?:[^`]|``)*`|\[[^\]]*\]|[A-Za-z_][A-Za-z_0-9]*|[(),;]', re.DOTALL)
_TRADES_DDL = """CREATE TABLE "executions" (
    "id" INTEGER PRIMARY KEY, "exchange" TEXT NOT NULL, "symbol" TEXT NOT NULL,
    "timestamp" INTEGER NOT NULL, "side" TEXT, "price" REAL, "qty" REAL, "fee" REAL,
    "realized_pnl" REAL, "order_id" TEXT, "trade_id" TEXT NOT NULL, "user" TEXT NOT NULL,
    "raw_json" TEXT, UNIQUE("user", "exchange", "trade_id"))"""


def _ddl_tokens(sql: str) -> list[str | tuple[str]]:
    """Consume closed DDL syntax, retaining quoted identifiers as distinct tokens."""
    if not isinstance(sql, str) or len(sql) > _SQLITE_LIMITS[sqlite3.SQLITE_LIMIT_SQL_LENGTH]:
        raise InvalidBackupError("SQLite schema SQL is too large")
    tokens = []
    end = 0
    while end < len(sql):
        match = _DDL_TOKEN.match(sql, end)
        if match is None:
            raise InvalidBackupError("Unsupported SQLite schema syntax")
        end = match.end()
        token = match.group()
        if token.isspace() or token.startswith(("--", "/*")):
            continue
        if token[0] in ('"', '`', '['):
            quote = token[0]
            token = token[1:-1].replace(quote * 2, quote)
            tokens.append((token.lower(),))
        else:
            tokens.append(token.lower())
    if end != len(sql):
        raise InvalidBackupError("Unsupported SQLite schema syntax")
    if tokens[-1:] == [";"]:
        tokens.pop()
    return tokens


def _ddl_identifier(token: str | tuple[str]) -> str:
    """Extract a name only at a grammar position that explicitly permits identifiers."""
    return token[0] if isinstance(token, tuple) else token


def _ddl_matches(tokens, expected) -> bool:
    """Match keywords exactly; canonical quoted tokens mark identifier positions.

    Quoted PRIMARY/KEY/NOT/NULL are type-name text in SQLite, not constraints.
    Never discard that distinction when matching types or grammar keywords.
    """
    return len(tokens) == len(expected) and all(
        _ddl_identifier(actual) == wanted[0] if isinstance(wanted, tuple) else actual == wanted
        for actual, wanted in zip(tokens, expected)
    )


def _allow_snapshot_schema(source, check, *, main: bool) -> None:
    """Allow only canonical tables and ordinary column indexes, never expressions.

    This must run before integrity_check: CHECK and expression indexes execute
    during that pragma, even on immutable connections with trusted_schema off.
    Non-main utility databases retain support for plain, expression-free tables.
    """
    tables = {}
    indexes = []
    for number, (kind, name, table, sql) in enumerate(source.execute(
        "SELECT type, name, tbl_name, sql FROM sqlite_schema"
    )):
        check()
        if number >= 256:
            raise InvalidBackupError("Too many SQLite schema objects")
        if kind == "index":
            indexes.append((name, table, sql))
            continue
        error = f"Incompatible main database table: {name} (unsupported schema)"
        if kind != "table" or (main and name not in _CORE_SCHEMA and name not in {"sqlite_stat1", "sqlite_stat4"}):
            raise InvalidBackupError(error)
        try:
            tokens = _ddl_tokens(sql or "")
            if name in _CORE_SCHEMA:
                variants = []
                for side in (False, True):
                    for unique in (False, True):
                        columns = []
                        for col, dtype, required, pk in _CORE_SCHEMA[name]:
                            if name == "position" and col == "side" and not side:
                                continue
                            definition = f'"{col}" {dtype}'
                            if pk and name != "history_scan_meta":
                                definition += " PRIMARY KEY"
                            if required:
                                definition += " NOT NULL"
                            if unique and ((name in {"history", "orders"} and col == "uniqueid") or (name == "balances" and col == "user")):
                                definition += " UNIQUE"
                            columns.append(definition)
                        if name == "history_scan_meta":
                            columns.append('PRIMARY KEY ("user", "exchange")')
                        variants.append(_ddl_tokens(f'CREATE TABLE "{name}" ({", ".join(columns)})'))
                if not any(_ddl_matches(tokens, variant) for variant in variants):
                    raise InvalidBackupError(error)
            elif name == "executions":
                if not _ddl_matches(tokens, _ddl_tokens(_TRADES_DDL)):
                    raise InvalidBackupError(error)
            elif name in {"sqlite_stat1", "sqlite_stat4"}:
                cols = '"tbl","idx","stat"' if name == "sqlite_stat1" else '"tbl","idx","neq","nlt","ndlt","sample"'
                if not _ddl_matches(tokens, _ddl_tokens(f'CREATE TABLE "{name}"({cols})')):
                    raise InvalidBackupError(error)
            else:
                # Generic helper compatibility, not an SQL expression parser.
                if not _ddl_matches(tokens[:3], ["create", "table", (name.lower(),)]) or tokens[3:4] != ["("] or tokens[-1:] != [")"]:
                    raise InvalidBackupError(error)
                clauses = [[]]
                for token in tokens[4:-1]:
                    if token == ",":
                        clauses.append([])
                    else:
                        clauses[-1].append(token)
                for clause in clauses:
                    if len(clause) < 2 or clause[1] not in {"integer", "real", "text", "blob"} or clause[2:] not in ([], ["not", "null"], ["primary", "key"], ["not", "null", "unique"]):
                        raise InvalidBackupError(error)
            tables[name] = {row[1].lower() for row in source.execute("SELECT * FROM pragma_table_xinfo(?)", (name,))}
        except InvalidBackupError as exc:
            raise InvalidBackupError(error) from exc
    for name, table, sql in indexes:
        check()
        if table not in tables or table.startswith("sqlite_"):
            raise InvalidBackupError("Unsupported SQLite index table")
        if sql is None:
            # SQLite synthesizes these only for the already-validated constraints.
            if not name.startswith(f"sqlite_autoindex_{table}_"):
                raise InvalidBackupError("Unsupported SQLite automatic index")
            continue
        error = "Unsupported SQLite index (required UNIQUE key must be an ordinary column index)"
        try:
            tokens = _ddl_tokens(sql)
        except InvalidBackupError as exc:
            raise InvalidBackupError(error) from exc
        if tokens[:2] == ["create", "unique"]:
            tokens.pop(1)
        if not _ddl_matches(tokens[:5], ["create", "index", (name.lower(),), "on", (table.lower(),)]) or tokens[5:6] != ["("] or tokens[-1:] != [")"]:
            raise InvalidBackupError(error)
        clauses = [[]]
        for token in tokens[6:-1]:
            if token == ",":
                clauses.append([])
            else:
                clauses[-1].append(token)
        for clause in clauses:
            if not clause or _ddl_identifier(clause.pop(0)) not in tables[table]:
                raise InvalidBackupError(error)
            if len(clause) >= 2 and clause[0] == "collate" and _ddl_identifier(clause[1]) in {"binary", "nocase", "rtrim"}:
                del clause[:2]
            if clause not in ([], ["asc"], ["desc"]):
                raise InvalidBackupError(error)


def _validate_snapshot(source, check, *, main: bool = False) -> None:
    """Bound parsing and VM allocations before inspecting any untrusted schema."""
    for category, limit in _SQLITE_LIMITS.items():
        source.setlimit(category, limit)
    interrupted = False

    def progress():
        """Translate a deadline into SQLite's progress-interrupt protocol."""
        nonlocal interrupted
        try:
            check()
        except RestoreBusyError:
            interrupted = True
            return 1
        return 0

    source.set_progress_handler(progress, 1000)
    try:
        check()
        source.execute("PRAGMA trusted_schema=OFF")
        _allow_snapshot_schema(source, check, main=main)
        if main:
            _validate_main_schema(source)
        check()
        result_seen = False
        for row in source.execute("PRAGMA integrity_check"):
            check()
            result_seen = True
            if row != ("ok",):
                raise InvalidBackupError("SQLite integrity check failed")
        if not result_seen:
            raise InvalidBackupError("SQLite integrity check returned no results")
    except sqlite3.Error as exc:
        if interrupted or getattr(exc, "sqlite_errorcode", 0) & 255 in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
            raise RestoreBusyError("SQLite validation is busy or timed out") from exc
        raise InvalidBackupError("SQLite snapshot validation failed") from exc
    except MemoryError as exc:
        # SQLITE_LIMIT_VDBE_OP reports SQLITE_NOMEM when compilation hits its cap.
        raise InvalidBackupError("SQLite snapshot exceeds validation limits") from exc
    finally:
        source.set_progress_handler(None, 0)


def validate_sqlite_snapshot(path: Path, *, main: bool = False, timeout: float = 30.0) -> None:
    """Validate an already privately staged, standalone, immutable snapshot.

    Unlike restore_sqlite_backup this does not stage arbitrary published inputs;
    callers must retain ownership of the private directory until this returns.
    """
    check = _deadline(timeout)
    path = _path(path)
    with _pin(path, path_only=True) as (_, parent):
        _standalone(parent, path.name)
        with closing(_connect(path, "ro", immutable=True)) as source:
            _validate_snapshot(source, check, main=main)


class InvalidBackupError(ValueError):
    """The snapshot or filesystem boundary is not safe for restore."""


class RestoreBusyError(RuntimeError):
    """The SQLite operation could not complete within its time budget."""


def _path(path: Path) -> Path:
    """Reject unsafe lexical paths before making them absolute."""
    path = Path(path)
    if ".." in path.parts or any(ord(c) < 32 or 127 <= ord(c) <= 159 for c in str(path)):
        raise InvalidBackupError("Control characters and traversal are not allowed")
    return path.absolute()


@contextmanager
def _pin(path: Path, *, directory: bool = False, root_fd: int | None = None, path_only: bool = False):
    """Pin every component without following links or blocking on special files."""
    with ExitStack() as stack:
        try:
            fd = os.dup(root_fd) if root_fd is not None else os.open("/", os.O_RDONLY | os.O_DIRECTORY)
            stack.callback(os.close, fd)
            parent = fd
            parts = path.parts if root_fd is not None else path.parts[1:]
            for index, part in enumerate(parts):
                parent = fd
                is_dir = directory or index < len(parts) - 1
                flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
                if path_only and not is_dir:
                    # Closing an ordinary fd can release other SQLite connections'
                    # POSIX locks on this inode. O_PATH does not release those locks.
                    flags = os.O_PATH | os.O_NOFOLLOW
                fd = os.open(part, flags | (os.O_DIRECTORY if is_dir else 0), dir_fd=parent)
                stack.callback(os.close, fd)
            expected = stat.S_ISDIR if directory else stat.S_ISREG
            if not expected(os.fstat(fd).st_mode):
                raise InvalidBackupError("Expected a regular file or directory")
        except OSError as exc:
            raise InvalidBackupError("Missing, linked, or inaccessible SQLite path") from exc
        yield fd, parent


def _deadline(timeout: float):
    """Return a backup callback that never fails after SQLite commits."""
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("timeout must be finite and positive")
    end = time.monotonic() + timeout

    def check(status=sqlite3.SQLITE_OK, remaining=0, total=0):
        """Abort unfinished work only; SQLITE_DONE already committed."""
        if status != sqlite3.SQLITE_DONE and time.monotonic() >= end:
            raise RestoreBusyError("SQLite backup/restore timed out")

    return check


def _standalone(parent: int, name: str) -> None:
    """Reject sidecars that could make copying only the main file incomplete."""
    for suffix in ("-wal", "-journal"):
        try:
            info = os.stat(name + suffix, dir_fd=parent, follow_symlinks=False)
        except FileNotFoundError:
            continue
        if not stat.S_ISREG(info.st_mode) or info.st_size:
            raise InvalidBackupError("Restore requires a standalone snapshot without WAL/journal data")


def _connect(path: Path, mode: str, *, immutable: bool = False):
    """Open an existing database with a short busy wait, never creating it."""
    return sqlite3.connect(
        path.as_uri() + f"?mode={mode}" + ("&immutable=1" if immutable else ""),
        uri=True, timeout=0.01,
    )


def _copy_database(source, destination, check) -> None:
    """Let SQLite own the atomic transaction, including rollback on timeout."""
    check()
    try:
        source.backup(destination, pages=256, sleep=0.01, progress=check)
    except sqlite3.Error as exc:
        if getattr(exc, "sqlite_errorcode", 0) & 255 in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
            raise RestoreBusyError("SQLite database is busy") from exc
        raise


def _unique_keys(connection, table: str) -> set[tuple[str, ...]]:
    """Find unconditional unique keys, not partial, expression or wider indexes."""
    return {
        tuple(row[0] for row in connection.execute(
            "SELECT name FROM pragma_index_info(?) ORDER BY seqno", (index[0],),
        ))
        for index in connection.execute(
            'SELECT name FROM pragma_index_list(?) WHERE "unique" = 1 AND partial = 0', (table,),
        )
    }


def _validate_main_schema(source, live=None) -> None:
    """Check parsed production signatures, optionally preserving live-only requirements.

    Without a live connection, accept the explicit historical side/metadata and
    balances variants, but still require core layouts, rowid PKs and unique IDs.
    """
    for table, expected in _CORE_SCHEMA.items():
        actual = list(source.execute("SELECT * FROM pragma_table_xinfo(?) ORDER BY cid", (table,)))
        live_columns = list(live.execute("SELECT name FROM pragma_table_xinfo(?)", (table,))) if live is not None else []
        if table == "history_scan_meta" and not actual and not live_columns:
            continue
        if table == "position" and ("side",) not in live_columns and not any(row[1] == "side" for row in actual):
            expected = expected[:-1]
        signature = [(row[1], row[2].upper(), row[3], row[5]) for row in actual]
        is_table = source.execute(
            "SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?", (table,),
        ).fetchone()
        if not is_table or signature != expected or any(row[4] is not None or row[6] for row in actual):
            raise InvalidBackupError(f"Incompatible main database table: {table} (column order/type/NOT NULL/PK)")
        # INTEGER PRIMARY KEY DESC has an index, not the auto-assigned rowid
        # required by core inserts; table_info alone cannot distinguish it.
        if table != "history_scan_meta" and source.execute(
            "SELECT 1 FROM pragma_index_list(?) WHERE origin='pk'", (table,),
        ).fetchone():
            raise InvalidBackupError(f"Incompatible main database table: {table} (id must alias rowid)")
        required = {("uniqueid",)} if table in {"history", "orders"} else set()
        if table == "balances" and live is not None and ("user",) in _unique_keys(live, table):
            required.add(("user",))
        if not required <= _unique_keys(source, table):
            raise InvalidBackupError(f"Incompatible main database table: {table} (required UNIQUE key)")


def restore_sqlite_backup(
    backup_path: Path, destination: Path, approved_root: Path, *, timeout: float = 30.0,
) -> None:
    """Validate a pinned snapshot, then atomically overwrite live SQLite contents.

    No files are replaced, no journal settings changed, and no other connections
    closed. A legacy snapshot without position.side is accepted only when the
    destination also lacks it. Core row layouts and write constraints must match
    production DDL; live balances uniqueness and scan metadata cannot be removed.
    Schema migration belongs to the caller.
    """
    check = _deadline(timeout)
    source_path, destination, root = map(_path, (backup_path, destination, approved_root))
    if not source_path.is_relative_to(root) or source_path == root:
        raise InvalidBackupError("Backup must be below its approved root")
    with _pin(root, directory=True) as (root_fd, _), _pin(
        source_path.relative_to(root), root_fd=root_fd, path_only=True,
    ) as (source_fd, parent), _pin(destination, path_only=True) as (destination_fd, _):
        _standalone(parent, source_path.name)
        before = os.fstat(source_fd)
        if os.path.samestat(before, os.fstat(destination_fd)):
            raise InvalidBackupError("Backup and destination must be distinct files")
        with TemporaryDirectory(prefix=".sqlite-restore-", dir=f"/proc/self/fd/{root_fd}") as temporary:
            os.chmod(temporary, PRIVATE_DIR_MODE)
            staged = Path(temporary) / "snapshot.db"
            fd = os.open(staged, os.O_WRONLY | os.O_CREAT | os.O_EXCL, PRIVATE_FILE_MODE)
            with os.fdopen(fd, "wb") as output:
                # Reopen the pinned descriptor, never the replaceable source name.
                with open(f"/proc/self/fd/{source_fd}", "rb", buffering=0) as pinned:
                    while True:
                        check()
                        chunk = os.read(pinned.fileno(), 1024 * 1024)
                        if not chunk:
                            break
                        output.write(chunk)
            after = os.fstat(source_fd)
            if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
                raise InvalidBackupError("Backup changed while staging")
            _standalone(parent, source_path.name)
            with staged.open("rb") as header:
                if header.read(16) != b"SQLite format 3\x00":
                    raise InvalidBackupError("Invalid SQLite header")
            with closing(_connect(staged, "ro", immutable=True)) as source:
                _validate_snapshot(source, check, main=destination.name == "pbgui.db")
                try:
                    with closing(_connect(destination, "rw")) as live:
                        if destination.name == "pbgui.db":
                            _validate_main_schema(source, live)
                        if live.execute("PRAGMA journal_mode").fetchone()[0] == "wal" and (
                            source.execute("PRAGMA page_size").fetchone()
                            != live.execute("PRAGMA page_size").fetchone()
                        ):
                            raise InvalidBackupError("Snapshot page size is incompatible with live WAL")
                        _copy_database(source, live, check)
                except sqlite3.Error as exc:
                    if getattr(exc, "sqlite_errorcode", 0) & 255 in (
                        sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED,
                    ):
                        raise RestoreBusyError("SQLite validation/restore is busy or timed out") from exc
                    raise InvalidBackupError("SQLite snapshot validation/restore failed") from exc


def backup_sqlite_database(source: Path, destination: Path, *, timeout: float = 30.0) -> None:
    """Publish a coherent WAL-aware snapshot at a NEW path without clobbering it."""
    check = _deadline(timeout)
    source, destination = map(_path, (source, destination))
    with _pin(source, path_only=True), _pin(destination.parent, directory=True) as (parent_fd, _):
        with TemporaryDirectory(prefix=".sqlite-backup-", dir=f"/proc/self/fd/{parent_fd}") as temporary:
            os.chmod(temporary, PRIVATE_DIR_MODE)
            staged = Path(temporary) / "snapshot.db"
            fd = os.open(staged, os.O_RDWR | os.O_CREAT | os.O_EXCL, PRIVATE_FILE_MODE)
            os.close(fd)
            with closing(_connect(source, "ro")) as live, closing(_connect(staged, "rw")) as snapshot:
                _copy_database(live, snapshot, check)
            with staged.open("rb") as completed:
                os.fsync(completed.fileno())
            # SQLITE_DONE completed the copy: do not turn late success into a
            # timeout. Only publication/durability failures may still fail here.
            _standalone(parent_fd, destination.name)
            # link is atomic and fails if the requested publication name exists.
            os.link(staged, destination.name, dst_dir_fd=parent_fd, follow_symlinks=False)
            os.fsync(parent_fd)
