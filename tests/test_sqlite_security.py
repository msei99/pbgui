"""Bounded hostile-schema and publication regressions using isolated SQLite files."""

import ast
import os
import re
import sqlite3
import stat
from contextlib import closing
from pathlib import Path

import pytest

import sqlite_backup as backup
from test_sqlite_backup_restore import _main_schema


QUOTED_SCHEMA_CHANGES = [
    ("id INTEGER PRIMARY KEY", 'id INTEGER "PRIMARY" "KEY"'),
    ("id INTEGER PRIMARY KEY", 'id INTEGER `PRIMARY` `KEY`'),
    ("id INTEGER PRIMARY KEY", 'id INTEGER [PRIMARY] [KEY]'),
    ("symbol TEXT NOT NULL", 'symbol TEXT "NOT" "NULL"'),
    ("uniqueid text NOT NULL UNIQUE", 'uniqueid text "NOT" "NULL" "UNIQUE"'),
    ("uniqueid text NOT NULL UNIQUE", 'uniqueid text "UNIQUE" NOT NULL'),
    ("id INTEGER PRIMARY KEY", 'id "INTEGER" PRIMARY KEY'),
    ("symbol TEXT NOT NULL", 'symbol "TEXT" NOT NULL'),
    ("income REAL NOT NULL", 'income "REAL" NOT NULL'),
    ("income REAL NOT NULL", 'income REAL "CHECK" NOT NULL'),
]


@pytest.mark.parametrize("old,new", QUOTED_SCHEMA_CHANGES)
def test_quoted_schema_keywords_rejected_before_integrity(tmp_path, monkeypatch, old, new):
    """Quoted constraint/type words cannot match canonical unquoted keywords."""
    source = tmp_path / "pbgui.db"
    _main_schema(source, change=("history", old, new))
    integrity_calls = []
    original = backup._connect

    def connect(path, mode, **kwargs):
        """Observe whether the rejected schema ever reaches integrity execution."""
        conn = original(path, mode, **kwargs)
        conn.set_trace_callback(lambda sql: integrity_calls.append(sql) if sql == "PRAGMA integrity_check" else None)
        return conn

    monkeypatch.setattr(backup, "_connect", connect)
    before = source.read_bytes()
    with pytest.raises(backup.InvalidBackupError, match="history"):
        backup.validate_sqlite_snapshot(source, main=True)
    assert not integrity_calls
    assert source.read_bytes() == before


@pytest.mark.parametrize("change", [
    ("history", "id INTEGER PRIMARY KEY", 'id INTEGER "PRIMARY" "KEY"'),
    ("history", "symbol TEXT NOT NULL", 'symbol TEXT "NOT" "NULL"'),
    ("history", "uniqueid text NOT NULL UNIQUE", "uniqueid text NOT NULL"),
    ("history_scan_meta", "PRIMARY KEY (user, exchange)", "PRIMARY KEY (exchange, user)"),
])
def test_parsed_main_signature_is_independent_of_sql_allowlist(tmp_path, monkeypatch, change):
    """A lexical false positive still fails parsed PK/NOT NULL/UNIQUE/meta checks."""
    source = tmp_path / "pbgui.db"
    _main_schema(source, change=change)
    monkeypatch.setattr(backup, "_allow_snapshot_schema", lambda *args, **kwargs: None)
    with closing(sqlite3.connect(source)) as conn:
        integrity_calls = []
        conn.set_trace_callback(lambda sql: integrity_calls.append(sql) if sql == "PRAGMA integrity_check" else None)
        with pytest.raises(backup.InvalidBackupError, match=change[0]):
            backup._validate_snapshot(conn, lambda: None, main=True)
        assert not integrity_calls


@pytest.mark.parametrize("side", [False, True])
@pytest.mark.parametrize("meta", [False, True])
@pytest.mark.parametrize("balance_unique", [False, True])
def test_standalone_main_validation_preserves_legacy_variants(tmp_path, side, meta, balance_unique):
    """DBTools validates real constraints without requiring modern-only features."""
    source = tmp_path / "pbgui.db"
    _main_schema(source, side=side, meta=meta, balance_unique=balance_unique)
    backup.validate_sqlite_snapshot(source, main=True)


@pytest.mark.parametrize("opening,ending", [('"', '"'), ('`', '`'), ('[', ']')])
def test_quoted_table_column_and_index_identifiers_are_allowed(tmp_path, opening, ending):
    """Identifier quoting remains legal, including keyword-shaped index names."""
    source, destination = tmp_path / "snapshot.db", tmp_path / "pbgui.db"
    _main_schema(destination)
    identifiers = set(backup._CORE_SCHEMA) | {col[0] for columns in backup._CORE_SCHEMA.values() for col in columns}
    with closing(sqlite3.connect(destination)) as canonical, closing(sqlite3.connect(source)) as conn:
        for (sql,) in canonical.execute("SELECT sql FROM sqlite_schema WHERE type='table'"):
            sql = re.sub(r"\b[A-Za-z_]+\b", lambda match: opening + match[0] + ending if match[0] in identifiers else match[0], sql)
            conn.execute(sql)
        conn.execute(f'CREATE INDEX {opening}PRIMARY{ending} ON {opening}history{ending} ({opening}user{ending})')
    backup.restore_sqlite_backup(source, destination, tmp_path)
    with closing(sqlite3.connect(destination)) as conn:
        assert conn.execute("SELECT name FROM pragma_index_list('history') WHERE name='PRIMARY'").fetchone() == ("PRIMARY",)


@pytest.mark.parametrize("attack", [
    "trigger", "view", "virtual", "expression", "partial", "extra",
    "check", "named_check", "default", "generated", "foreign_key",
])
def test_schema_rejected_before_integrity(tmp_path, monkeypatch, attack):
    """Even innocuous builtins in CHECK must not reach the integrity VM."""
    source, destination = tmp_path / "snapshot.db", tmp_path / "pbgui.db"
    expression = "length(hex(zeroblob(1048576)))"
    changes = {
        "check": f"income REAL NOT NULL CHECK ({expression})",
        "named_check": f"income REAL NOT NULL CONSTRAINT hidden CHECK ({expression})",
        "default": f"income REAL NOT NULL DEFAULT ({expression})",
        "generated": f"income REAL GENERATED ALWAYS AS ({expression}) VIRTUAL NOT NULL",
        "foreign_key": "income REAL NOT NULL REFERENCES prices(id)",
    }
    change = ("history", "income REAL NOT NULL", changes[attack]) if attack in changes else None
    _main_schema(source, change=change)
    _main_schema(destination)
    extra_sql = {
        "trigger": "CREATE TRIGGER hidden AFTER INSERT ON history BEGIN SELECT zeroblob(1048576); END",
        "view": f"CREATE VIEW hidden AS SELECT {expression}",
        "virtual": "CREATE VIRTUAL TABLE hidden USING fts5(value)",
        "expression": f"CREATE INDEX hidden ON history(({expression}))",
        "partial": f"CREATE INDEX hidden ON history(user) WHERE {expression}",
        "extra": "CREATE TABLE hidden (value TEXT)",
    }
    if attack in extra_sql:
        with closing(sqlite3.connect(source)) as conn:
            conn.execute(extra_sql[attack])
    before = destination.read_bytes()
    opened = []

    class Guarded(sqlite3.Connection):
        """Prove allocation limits precede all SQL, and integrity never runs."""

        def execute(self, sql, *args):
            """Reject an attempt to run integrity on the malicious schema."""
            assert self.getlimit(sqlite3.SQLITE_LIMIT_LENGTH) == 8 * 1024 * 1024
            assert self.getlimit(sqlite3.SQLITE_LIMIT_SQL_LENGTH) == 64 * 1024
            assert sql != "PRAGMA integrity_check"
            return super().execute(sql, *args)

    def connect(path, mode, **kwargs):
        """Only the private immutable source may be opened before rejection."""
        assert mode == "ro" and kwargs == {"immutable": True}
        conn = sqlite3.connect(path.as_uri() + "?mode=ro&immutable=1", uri=True, factory=Guarded)
        opened.append(conn)
        return conn

    monkeypatch.setattr(backup, "_connect", connect)
    with pytest.raises(backup.InvalidBackupError):
        backup.restore_sqlite_backup(source, destination, tmp_path)
    assert destination.read_bytes() == before
    assert not list(tmp_path.glob(".sqlite-restore-*"))
    for conn in opened:
        with pytest.raises(sqlite3.ProgrammingError):
            conn.cursor()


def test_single_opcode_allocation_limit_without_progress_callback(monkeypatch):
    """A one-MiB cap stops hex allocation before a 1000-opcode callback can run."""
    monkeypatch.setitem(backup._SQLITE_LIMITS, sqlite3.SQLITE_LIMIT_LENGTH, 1024 * 1024)
    with closing(sqlite3.connect(":memory:")) as conn:
        conn.execute("CREATE TABLE sample (value TEXT)")
        backup._validate_snapshot(conn, lambda: None)
        calls = []
        conn.set_progress_handler(lambda: calls.append(True) or 0, 1000)
        with pytest.raises(sqlite3.DataError, match="too big"):
            conn.execute("SELECT hex(zeroblob(524289))").fetchone()
        assert not calls


def test_oversized_schema_literal_rejected_without_integrity(tmp_path, monkeypatch):
    """A one-MiB schema literal is rejected during bounded parsing/tokenization."""
    source, destination = tmp_path / "snapshot.db", tmp_path / "pbgui.db"
    _main_schema(source)
    _main_schema(destination)
    with closing(sqlite3.connect(source)) as conn:
        conn.execute("PRAGMA writable_schema=ON")
        ddl = conn.execute("SELECT sql FROM sqlite_schema WHERE name='history'").fetchone()[0]
        ddl = ddl.replace("income REAL NOT NULL", "income REAL NOT NULL CHECK (length(x'" + "00" * 524288 + "'))")
        conn.execute("UPDATE sqlite_schema SET sql=? WHERE name='history'", (ddl,))
        conn.commit()
    original = backup._connect
    integrity_calls = []

    def authorize(action, name, *rest):
        """Record rather than silently accepting a denied integrity attempt."""
        if action == sqlite3.SQLITE_PRAGMA and name == "integrity_check":
            integrity_calls.append(True)
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    def connect(path, mode, **kwargs):
        """Observe actual SQLite statements without executing a hostile check."""
        conn = original(path, mode, **kwargs)
        conn.set_authorizer(authorize)
        return conn

    monkeypatch.setattr(backup, "_connect", connect)
    before = destination.read_bytes()
    with pytest.raises(backup.InvalidBackupError):
        backup.restore_sqlite_backup(source, destination, tmp_path)
    assert destination.read_bytes() == before
    assert not integrity_calls


@pytest.mark.parametrize("trades", [False, True])
def test_actual_database_ddl_and_custom_ordinary_indexes(tmp_path, trades):
    """Production DDL, ALTER-added side, ANALYZE, and custom indexes remain valid."""
    tree = ast.parse(Path("Database.py").read_text())
    method = "create_trades_tables" if trades else "create_tables"
    function = next(node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name == method)
    statements = ast.literal_eval(function.body[0].value)
    source = tmp_path / "snapshot.db"
    destination = tmp_path / ("pbgui_trades.db" if trades else "pbgui.db")
    for path in (source, destination):
        with closing(sqlite3.connect(path)) as conn:
            for sql in statements:
                conn.execute(sql)
            if not trades:
                conn.execute("ALTER TABLE position ADD COLUMN side TEXT")
    with closing(sqlite3.connect(source)) as conn:
        for node in ast.walk(function):
            if isinstance(node, ast.Constant) and isinstance(node.value, str) and node.value.startswith("CREATE INDEX"):
                conn.execute(node.value)
        table = "executions" if trades else "history"
        conn.execute(f'CREATE INDEX "custom performance index" ON "{table}" ("user" COLLATE NOCASE ASC, [timestamp] DESC)')
        conn.execute("ANALYZE")
    backup.restore_sqlite_backup(source, destination, tmp_path)
    with closing(sqlite3.connect(destination)) as conn:
        assert conn.execute("SELECT name FROM sqlite_schema WHERE name='custom performance index'").fetchone()


def test_snapshot_publication_fsyncs_directory(tmp_path, monkeypatch):
    """The new directory entry is synced after linking the fully synced snapshot."""
    source, destination = tmp_path / "source.db", tmp_path / "snapshot.db"
    _main_schema(source)
    original = os.fsync
    synced = []

    def fsync(fd):
        """Record durability order without bypassing the real filesystem call."""
        is_dir = stat.S_ISDIR(os.fstat(fd).st_mode)
        synced.append(is_dir)
        if is_dir:
            assert destination.exists()
        original(fd)

    monkeypatch.setattr(backup.os, "fsync", fsync)
    backup.backup_sqlite_database(source, destination)
    assert synced == [False, True]


@pytest.mark.parametrize("side", ["source", "destination", "parent"])
def test_backup_rejects_symlinks(tmp_path, side):
    """Reusing the snapshot helper must not follow source or publication links."""
    source, destination = tmp_path / "source.db", tmp_path / "snapshot.db"
    _main_schema(source)
    before = source.read_bytes()
    if side == "source":
        linked = tmp_path / "linked.db"
        linked.symlink_to(source)
        source = linked
    elif side == "destination":
        destination.symlink_to(source)
    else:
        linked = tmp_path / "linked"
        linked.symlink_to(tmp_path, target_is_directory=True)
        destination = linked / destination.name
    with pytest.raises((backup.InvalidBackupError, FileExistsError)):
        backup.backup_sqlite_database(source, destination)
    assert source.read_bytes() == before
