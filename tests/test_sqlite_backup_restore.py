"""Isolated, real-SQLite snapshot/restore regressions; never use runtime data."""

import multiprocessing
import os
import sqlite3
import stat
from contextlib import closing
from pathlib import Path

import pytest

import sqlite_backup as backup


def _database(path, value="old", *, wal=False, large=False, page_size=4096):
    """Create an isolated database and retain its connection when requested."""
    conn = sqlite3.connect(path)
    conn.execute(f"PRAGMA page_size={page_size}")
    if wal:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA wal_autocheckpoint=0")
    conn.execute("CREATE TABLE sample (value TEXT, payload BLOB)")
    conn.execute("INSERT INTO sample VALUES (?, ?)", (value, b"x" * (3_000_000 if large else 10)))
    conn.commit()
    return conn


def _value(conn):
    """Read a value without retaining a cursor or transaction."""
    return conn.execute("SELECT value FROM sample").fetchone()[0]


def _writer(path, ready, release):
    """Hold a real independent-process write transaction until released."""
    with closing(sqlite3.connect(path)) as conn:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("UPDATE sample SET value='writer'")
        ready.set()
        if release.wait(10):
            conn.commit()
        else:
            conn.rollback()


@pytest.fixture
def pair(tmp_path):
    """Provide a published snapshot and a WAL destination with a cached reader."""
    root = tmp_path / "backups"
    root.mkdir()
    source = root / "snapshot.db"
    with closing(_database(source, "restored", large=True)):
        pass
    destination = tmp_path / "live.db"
    with closing(_database(destination, wal=True)) as cached:
        yield root, source, destination, cached


def test_wal_readers_keep_snapshot_then_observe_restore(pair):
    """Restore preserves the inode, WAL mode and an existing reader snapshot."""
    root, source, destination, cached = pair
    before = source.read_bytes()
    inode = destination.stat().st_ino
    with closing(sqlite3.connect(destination)) as reader:
        reader.execute("BEGIN")
        assert _value(reader) == "old"
        backup.restore_sqlite_backup(source, destination, root)
        assert _value(reader) == "old"
        assert _value(cached) == "restored"
        reader.rollback()
        assert _value(reader) == "restored"
        assert cached.execute("PRAGMA journal_mode").fetchone() == ("wal",)
    assert destination.stat().st_ino == inode
    assert source.read_bytes() == before
    assert set(root.iterdir()) == {source}


def test_independent_writer_busy_rollback_then_restore(pair):
    """A separate writer blocks restore; timeout leaves committed data intact."""
    root, source, destination, cached = pair
    ctx = multiprocessing.get_context("spawn")
    ready, release = ctx.Event(), ctx.Event()
    process = ctx.Process(target=_writer, args=(str(destination), ready, release))
    # Keep the raw comparison descriptor open: closing it during this test would
    # itself release the cached connection's POSIX locks and invalidate the test.
    with destination.open("rb", buffering=0) as raw:
        process.start()
        try:
            assert ready.wait(10)
            main_before = raw.read()
            with pytest.raises(backup.RestoreBusyError):
                backup.restore_sqlite_backup(source, destination, root, timeout=0.1)
            assert _value(cached) == "old"
            raw.seek(0)
            assert raw.read() == main_before
            assert set(root.iterdir()) == {source}
        finally:
            release.set()
            process.join(10)
            if process.is_alive():
                process.terminate()
                process.join()
        assert process.exitcode == 0
        assert _value(cached) == "writer"
        backup.restore_sqlite_backup(source, destination, root)
        assert _value(cached) == "restored"


@pytest.mark.parametrize("late_status", [sqlite3.SQLITE_OK, sqlite3.SQLITE_DONE])
@pytest.mark.parametrize("journal_mode", ["wal", "delete"])
def test_backup_progress_timeout_and_late_done(pair, monkeypatch, late_status, journal_mode):
    """Timeout midway rolls back, but a late DONE must report committed success."""
    root, source, destination, cached = pair
    assert cached.execute(f"PRAGMA journal_mode={journal_mode}").fetchone() == (journal_mode,)
    clock = [0.0]
    monkeypatch.setattr(backup.time, "monotonic", lambda: clock[0])
    original = backup._copy_database
    seen = []

    def instrumented(src, dest, check):
        """Wrap real backup progress rather than simulating SQLite rollback."""
        def progress(status=None, remaining=0, total=0):
            """Expire only at the requested real SQLite status."""
            if status is None:
                return check()
            seen.append(status)
            if status == late_status:
                clock[0] = 100.0
            check(status, remaining, total)

        original(src, dest, progress)

    monkeypatch.setattr(backup, "_copy_database", instrumented)
    before = destination.read_bytes()
    if late_status == sqlite3.SQLITE_OK:
        with pytest.raises(backup.RestoreBusyError):
            backup.restore_sqlite_backup(source, destination, root, timeout=1)
        assert _value(cached) == "old"
        assert destination.read_bytes() == before
    else:
        backup.restore_sqlite_backup(source, destination, root, timeout=1)
        assert _value(cached) == "restored"
    assert late_status in seen
    assert cached.execute("PRAGMA journal_mode").fetchone() == (journal_mode,)
    assert set(root.iterdir()) == {source}


@pytest.mark.parametrize("kind", [
    "missing", "outside", "traversal", "control", "symlink", "directory-link",
    "directory", "fifo", "header", "corrupt", "wal", "wal-link", "journal",
])
def test_reject_invalid_sources_without_touching_live(pair, tmp_path, kind):
    """Reject unsafe source paths and incomplete/corrupt snapshots before writes."""
    root, good, destination, cached = pair
    source = root / "bad.db"
    if kind == "outside":
        source = tmp_path / "outside.db"
        source.write_bytes(good.read_bytes())
    elif kind == "traversal":
        source = root / ".." / "backups" / good.name
    elif kind == "control":
        source = root / "bad\n.db"
        source.write_bytes(good.read_bytes())
    elif kind == "symlink":
        source.symlink_to(good)
    elif kind == "directory-link":
        (root / "linked").symlink_to(root, target_is_directory=True)
        source = root / "linked" / good.name
    elif kind == "directory":
        source.mkdir()
    elif kind == "fifo":
        os.mkfifo(source)
    elif kind == "header":
        source.write_bytes(b"not sqlite")
    elif kind == "corrupt":
        source.write_bytes(b"SQLite format 3\x00" + b"\x00" * 4096)
    elif kind in {"wal", "wal-link", "journal"}:
        source = good
        sidecar = Path(str(source) + ("-journal" if kind == "journal" else "-wal"))
        if kind == "wal-link":
            sidecar.symlink_to(good)
        else:
            sidecar.write_bytes(b"meaningful sidecar")
    before, original = destination.read_bytes(), good.read_bytes()
    with pytest.raises(backup.InvalidBackupError):
        backup.restore_sqlite_backup(source, destination, root, timeout=0.3)
    assert _value(cached) == "old"
    assert destination.read_bytes() == before
    assert good.read_bytes() == original
    assert not list(root.glob(".sqlite-restore-*"))


@pytest.mark.parametrize("kind", ["missing", "symlink", "fifo", "same"])
def test_reject_invalid_destination(pair, tmp_path, kind):
    """Opening mode=rw must never create a destination or follow a link."""
    root, source, destination, cached = pair
    target = tmp_path / "target.db"
    if kind == "symlink":
        target.symlink_to(destination)
    elif kind == "fifo":
        os.mkfifo(target)
    elif kind == "same":
        target = source
    before = source.read_bytes()
    with pytest.raises(backup.InvalidBackupError):
        backup.restore_sqlite_backup(source, target, root)
    assert _value(cached) == "old"
    assert source.read_bytes() == before
    if kind == "missing":
        assert not target.exists()


def test_reject_incompatible_wal_page_size(pair):
    """A WAL destination cannot accept a differently sized source page."""
    root, _, destination, cached = pair
    source = root / "different.db"
    with closing(_database(source, page_size=8192)):
        pass
    with pytest.raises(backup.InvalidBackupError, match="page size"):
        backup.restore_sqlite_backup(source, destination, root)
    assert _value(cached) == "old"


def _main_schema(path, *, side=True, omit=None, balance_unique=True, meta=True, change=None):
    """Build real production DDL, optionally removing explicit legacy features."""
    tables = {
        "history": [
            "id INTEGER PRIMARY KEY", "symbol TEXT NOT NULL", "timestamp INTEGER NOT NULL",
            "income REAL NOT NULL", "uniqueid text NOT NULL UNIQUE", "user TEXT NOT NULL",
        ],
        "position": [
            "id INTEGER PRIMARY KEY", "symbol TEXT NOT NULL", "timestamp INTEGER NOT NULL",
            "psize REAL NOT NULL", "upnl REAL NOT NULL", "entry REAL NOT NULL", "user TEXT NOT NULL",
        ] + (["side TEXT"] if side else []),
        "orders": [
            "id INTEGER PRIMARY KEY", "symbol TEXT NOT NULL", "timestamp INTEGER NOT NULL",
            "amount REAL NOT NULL", "price REAL NOT NULL", "side TEXT NOT NULL",
            "uniqueid text NOT NULL UNIQUE", "user TEXT NOT NULL",
        ],
        "prices": [
            "id INTEGER PRIMARY KEY", "symbol TEXT NOT NULL", "timestamp INTEGER NOT NULL",
            "price REAL NOT NULL", "user TEXT NOT NULL",
        ],
        "balances": [
            "id INTEGER PRIMARY KEY", "timestamp INTEGER NOT NULL", "balance REAL NOT NULL",
            "user TEXT NOT NULL" + (" UNIQUE" if balance_unique else ""),
        ],
    }
    if meta:
        tables["history_scan_meta"] = [
            "user TEXT NOT NULL", "exchange TEXT NOT NULL", "last_scan_ts INTEGER NOT NULL",
            "PRIMARY KEY (user, exchange)",
        ]
    with closing(sqlite3.connect(path)) as conn:
        for table, columns in tables.items():
            definitions = [column for column in columns if (table, column.split()[0]) != omit]
            ddl = f"CREATE TABLE {table} ({', '.join(definitions)})"
            if change and table == change[0]:
                assert change[1] in ddl
                ddl = ddl.replace(change[1], change[2])
            conn.execute(ddl)


@pytest.mark.parametrize("omit", [None, ("history", "income"), ("position", "entry"),
                                     ("orders", "side"), ("prices", "price"), ("balances", "balance")])
def test_main_database_schema_validation(pair, tmp_path, omit):
    """An unrelated DB or a core schema missing required columns cannot restore."""
    root, unrelated, _, _ = pair
    destination = tmp_path / "pbgui.db"
    _main_schema(destination, side=True)
    source = unrelated
    if omit:
        source = root / "main.db"
        _main_schema(source, side=True, omit=omit)
    before = destination.read_bytes()
    with pytest.raises(backup.InvalidBackupError, match="main database table"):
        backup.restore_sqlite_backup(source, destination, root)
    assert destination.read_bytes() == before


@pytest.mark.parametrize("source_side,live_side", [(False, False), (True, True), (False, True), (True, False)])
def test_legacy_side_compatibility(tmp_path, source_side, live_side):
    """Accept legacy schema only when it does not remove a live side column."""
    source, destination = tmp_path / "snapshot.db", tmp_path / "pbgui.db"
    _main_schema(source, side=source_side)
    _main_schema(destination, side=live_side)
    if live_side and not source_side:
        with pytest.raises(backup.InvalidBackupError):
            backup.restore_sqlite_backup(source, destination, tmp_path)
    else:
        backup.restore_sqlite_backup(source, destination, tmp_path)


@pytest.mark.parametrize("table", ["history", "position", "orders", "prices", "balances"])
@pytest.mark.parametrize("fault", ["missing_pk", "pk_desc", "type", "nullable", "order", "extra", "generated"])
def test_reject_core_schema_signature(tmp_path, table, fault):
    """Reject each malformed core signature before copying any live pages."""
    source, destination = tmp_path / "snapshot.db", tmp_path / "pbgui.db"
    second = "timestamp INTEGER NOT NULL" if table == "balances" else "symbol TEXT NOT NULL"
    mutations = {
        "missing_pk": ("id INTEGER PRIMARY KEY", "id INTEGER"),
        "pk_desc": ("id INTEGER PRIMARY KEY", "id INTEGER PRIMARY KEY DESC"),
        "type": ("timestamp INTEGER NOT NULL", "timestamp TEXT NOT NULL"),
        "nullable": ("timestamp INTEGER NOT NULL", "timestamp INTEGER"),
        "order": (f"id INTEGER PRIMARY KEY, {second}", f"{second}, id INTEGER PRIMARY KEY"),
        "extra": ("id INTEGER PRIMARY KEY", "id INTEGER PRIMARY KEY, extra TEXT"),
        "generated": ("timestamp INTEGER NOT NULL", "timestamp INTEGER GENERATED ALWAYS AS (1) VIRTUAL NOT NULL"),
    }
    _main_schema(source, change=(table, *mutations[fault]))
    _main_schema(destination)
    original, before = source.read_bytes(), destination.read_bytes()
    with pytest.raises(backup.InvalidBackupError, match=f"main database table: {table}"):
        backup.restore_sqlite_backup(source, destination, tmp_path)
    assert source.read_bytes() == original
    assert destination.read_bytes() == before
    assert not list(tmp_path.glob(".sqlite-restore-*"))


@pytest.mark.parametrize("table", ["history", "orders"])
@pytest.mark.parametrize("index_kind", ["missing", "partial", "composite", "expression", "valid"])
def test_required_unique_keys(tmp_path, table, index_kind):
    """Require a full uniqueid key, allowing an equivalent separately named index."""
    source, destination = tmp_path / "snapshot.db", tmp_path / "pbgui.db"
    _main_schema(source, change=(table, "uniqueid text NOT NULL UNIQUE", "uniqueid text NOT NULL"))
    _main_schema(destination)
    index_sql = {
        "partial": "(uniqueid) WHERE uniqueid != ''",
        "composite": "(uniqueid, user)",
        "expression": "(lower(uniqueid))",
        "valid": "(uniqueid)",
    }
    if index_kind != "missing":
        with closing(sqlite3.connect(source)) as conn:
            conn.execute(f'CREATE UNIQUE INDEX "quoted index" ON {table} {index_sql[index_kind]}')
    before = destination.read_bytes()
    if index_kind == "valid":
        backup.restore_sqlite_backup(source, destination, tmp_path)
        with closing(sqlite3.connect(destination)) as restored:
            assert restored.execute(f"PRAGMA index_list({table})").fetchone()[1] == "quoted index"
    else:
        with pytest.raises(backup.InvalidBackupError, match="required UNIQUE key"):
            backup.restore_sqlite_backup(source, destination, tmp_path)
        assert destination.read_bytes() == before


@pytest.mark.parametrize("source_unique,live_unique", [(False, False), (True, True), (False, True), (True, False)])
def test_legacy_balance_uniqueness(tmp_path, source_unique, live_unique):
    """Keep supported duplicate-balance legacy schemas without removing a live key."""
    source, destination = tmp_path / "snapshot.db", tmp_path / "pbgui.db"
    _main_schema(source, balance_unique=source_unique)
    _main_schema(destination, balance_unique=live_unique)
    before = destination.read_bytes()
    if live_unique and not source_unique:
        with pytest.raises(backup.InvalidBackupError, match="balances.*UNIQUE"):
            backup.restore_sqlite_backup(source, destination, tmp_path)
        assert destination.read_bytes() == before
    else:
        backup.restore_sqlite_backup(source, destination, tmp_path)


@pytest.mark.parametrize("source_meta,live_meta", [(False, False), (True, True), (False, True), (True, False)])
def test_history_scan_metadata_presence(tmp_path, source_meta, live_meta):
    """A restore cannot remove metadata required by already-running consumers."""
    source, destination = tmp_path / "snapshot.db", tmp_path / "pbgui.db"
    _main_schema(source, meta=source_meta)
    _main_schema(destination, meta=live_meta)
    before = destination.read_bytes()
    if live_meta and not source_meta:
        with pytest.raises(backup.InvalidBackupError, match="history_scan_meta"):
            backup.restore_sqlite_backup(source, destination, tmp_path)
        assert destination.read_bytes() == before
    else:
        backup.restore_sqlite_backup(source, destination, tmp_path)


@pytest.mark.parametrize("old,new", [
    (", PRIMARY KEY (user, exchange)", ""),
    ("PRIMARY KEY (user, exchange)", "PRIMARY KEY (exchange, user)"),
    ("last_scan_ts INTEGER NOT NULL", "last_scan_ts TEXT NOT NULL"),
    ("user TEXT NOT NULL", "user TEXT"),
])
def test_history_scan_metadata_signature(tmp_path, old, new):
    """The metadata upsert requires its production types and composite primary key."""
    source, destination = tmp_path / "snapshot.db", tmp_path / "pbgui.db"
    _main_schema(source, change=("history_scan_meta", old, new))
    _main_schema(destination)
    before = destination.read_bytes()
    with pytest.raises(backup.InvalidBackupError, match="history_scan_meta"):
        backup.restore_sqlite_backup(source, destination, tmp_path)
    assert destination.read_bytes() == before


def test_corrupt_live_schema_is_not_the_expected_signature(tmp_path):
    """Matching a damaged live schema is not sufficient to validate a snapshot."""
    source, destination = tmp_path / "snapshot.db", tmp_path / "pbgui.db"
    for path in (source, destination):
        _main_schema(path, change=("history", "id INTEGER PRIMARY KEY", "id INTEGER"))
    before = destination.read_bytes()
    with pytest.raises(backup.InvalidBackupError, match="history"):
        backup.restore_sqlite_backup(source, destination, tmp_path)
    assert destination.read_bytes() == before


def test_source_is_pinned_and_staging_is_private(pair, monkeypatch):
    """Replacing the source pathname after open cannot replace the staged bytes."""
    root, source, destination, cached = pair
    replacement = root / "replacement.db"
    with closing(_database(replacement, "wrong")):
        pass
    original_read, original_connect = os.read, backup._connect
    swapped = False
    connections = []

    def read(fd, size):
        """Swap names only after the original source descriptor was acquired."""
        nonlocal swapped
        assert size <= 1024 * 1024
        if not swapped:
            swapped = True
            os.replace(replacement, source)
        return original_read(fd, size)

    def connect(path, mode, **kwargs):
        """Inspect private staging before SQLite opens it."""
        if mode == "ro":
            assert path.name == "snapshot.db"
            assert stat.S_IMODE(path.stat().st_mode) == 0o600
            assert stat.S_IMODE(path.parent.stat().st_mode) == 0o700
            assert path.resolve().is_relative_to(root)
        conn = original_connect(path, mode, **kwargs)
        connections.append(conn)
        return conn

    monkeypatch.setattr(backup.os, "read", read)
    monkeypatch.setattr(backup, "_connect", connect)
    backup.restore_sqlite_backup(source, destination, root)
    assert swapped and _value(cached) == "restored"
    assert len(connections) == 2
    for conn in connections:
        with pytest.raises(sqlite3.ProgrammingError):
            conn.execute("SELECT 1")
    assert set(root.iterdir()) == {source}


def test_integrity_progress_deadline(pair, monkeypatch):
    """Long-running integrity SQL is interrupted and all staging is removed."""
    root, source, destination, cached = pair
    original_connect = backup._connect
    clock = [0.0]
    monkeypatch.setattr(backup.time, "monotonic", lambda: clock[0])

    class SlowIntegrity(sqlite3.Connection):
        """Use actual SQLite VM work to exercise the installed progress handler."""

        def execute(self, sql, *args):
            """Expire during integrity execution rather than before validation."""
            if sql == "PRAGMA integrity_check":
                clock[0] = 100
                return super().execute(
                    "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<10000) SELECT sum(x) FROM n"
                )
            return super().execute(sql, *args)

    def connect(path, mode, **kwargs):
        """Replace only the staged connection with a real instrumented SQLite DB."""
        if mode == "ro":
            return sqlite3.connect(path.as_uri() + "?mode=ro&immutable=1", uri=True, factory=SlowIntegrity)
        return original_connect(path, mode, **kwargs)

    monkeypatch.setattr(backup, "_connect", connect)
    with pytest.raises(backup.RestoreBusyError):
        backup.restore_sqlite_backup(source, destination, root, timeout=1)
    assert _value(cached) == "old"
    assert set(root.iterdir()) == {source}


def test_backup_includes_wal_and_publishes_private_standalone(pair, tmp_path):
    """Backup includes uncheckpointed WAL rows and can itself be safely restored."""
    root, _, source, cached = pair
    cached.execute("UPDATE sample SET value='wal only'")
    cached.commit()
    destination = root / "new.db"
    backup.backup_sqlite_database(source, destination)
    assert stat.S_IMODE(destination.stat().st_mode) == 0o600
    assert not Path(str(destination) + "-wal").exists()
    with closing(sqlite3.connect(destination)) as snapshot:
        assert _value(snapshot) == "wal only"
        assert snapshot.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
    target = tmp_path / "restored.db"
    with closing(_database(target)) as reader:
        backup.restore_sqlite_backup(destination, target, root)
        assert _value(reader) == "wal only"
    assert not list(root.glob(".sqlite-backup-*"))


@pytest.mark.parametrize("fail", ["existing", "timeout"])
def test_backup_failure_never_clobbers_existing_file(pair, monkeypatch, fail):
    """Failed publication or backup leaves an earlier snapshot byte-identical."""
    root, prior, live, cached = pair
    before = prior.read_bytes()
    if fail == "timeout":
        def abort(source, destination, check):
            """Inject failure after a private output was opened."""
            raise backup.RestoreBusyError("injected")

        monkeypatch.setattr(backup, "_copy_database", abort)
    with pytest.raises(FileExistsError if fail == "existing" else backup.RestoreBusyError):
        backup.backup_sqlite_database(live, prior)
    assert prior.read_bytes() == before
    assert _value(cached) == "old"
    assert set(root.iterdir()) == {prior}


@pytest.mark.parametrize("results", [[], [("ok",), ("corrupt later result",)]])
def test_integrity_requires_all_results_and_closes_source(pair, monkeypatch, results):
    """Do not accept an empty check or trust only its first row."""
    root, source, destination, cached = pair
    opened = []

    class IntegrityResults(sqlite3.Connection):
        """Inject unusual check results while retaining a real SQLite connection."""

        def execute(self, sql, *args):
            """Return the complete injected integrity result stream."""
            if sql == "PRAGMA integrity_check":
                return iter(results)
            return super().execute(sql, *args)

    def connect(path, mode, **kwargs):
        """Ensure validation failure never opens the live database."""
        assert mode == "ro"
        conn = sqlite3.connect(path.as_uri() + "?mode=ro&immutable=1", uri=True, factory=IntegrityResults)
        opened.append(conn)
        return conn

    monkeypatch.setattr(backup, "_connect", connect)
    with pytest.raises(backup.InvalidBackupError):
        backup.restore_sqlite_backup(source, destination, root)
    assert _value(cached) == "old"
    assert set(root.iterdir()) == {source}
    assert len(opened) == 1
    with pytest.raises(sqlite3.ProgrammingError):
        opened[0].execute("SELECT 1")


def test_backup_partial_timeout_leaves_no_published_file(pair, monkeypatch):
    """Abort a real multi-page backup and discard all partial private output."""
    root, source, _, _ = pair

    def deadline(timeout):
        """Expire only after SQLite has copied its first batch of pages."""
        def check(status=sqlite3.SQLITE_OK, remaining=0, total=0):
            """Leave initial checks alone, then interrupt an unfinished backup."""
            if total and status != sqlite3.SQLITE_DONE:
                raise backup.RestoreBusyError("injected partial timeout")

        return check

    monkeypatch.setattr(backup, "_deadline", deadline)
    with pytest.raises(backup.RestoreBusyError):
        backup.backup_sqlite_database(source, root / "new.db")
    assert set(root.iterdir()) == {source}


def test_approved_root_cannot_be_a_symlink(pair, tmp_path):
    """Containment is descriptor-based, not just lexical prefix matching."""
    root, source, destination, cached = pair
    linked = tmp_path / "linked"
    linked.symlink_to(root, target_is_directory=True)
    with pytest.raises(backup.InvalidBackupError):
        backup.restore_sqlite_backup(linked / source.name, destination, linked)
    assert _value(cached) == "old"


@pytest.mark.parametrize("timeout", [0, -1, float("inf"), float("nan")])
@pytest.mark.parametrize("operation", ["restore", "backup"])
def test_timeout_must_be_bounded(pair, timeout, operation):
    """Reject invalid budgets before any staging or database work."""
    root, source, destination, _ = pair
    with pytest.raises(ValueError, match="timeout"):
        if operation == "restore":
            backup.restore_sqlite_backup(source, destination, root, timeout=timeout)
        else:
            backup.backup_sqlite_database(destination, root / "new.db", timeout=timeout)
    assert set(root.iterdir()) == {source}
