"""Exercise DBTools backup code without importing unrelated API/exchange startup."""

import ast
import asyncio
import sqlite3
import threading
import time
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace

import pytest

import database_lock
import master_update_lock
import sqlite_backup as backup


@pytest.fixture
def tools(tmp_path):
    """Compile the actual helper bodies with isolated, explicit application globals.

    These helpers need no FastAPI or exchange clients. Keeping startup out of this
    harness also permits real-SQLite tests on a minimal Python 3.12 installation.
    """
    names = {"_sqlite_backup_file", "_run_backup_worker", "_install_db_bundle", "_assert_sqlite_integrity", "_track_background_task", "shutdown"}
    tree = ast.parse(Path("api/db_tools.py").read_text())
    body = [node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names]
    assert {node.name for node in body} == names
    future = ast.parse("from __future__ import annotations").body
    namespace = {
        "asyncio": asyncio, "Path": Path, "sqlite3": sqlite3,
        "PBGDIR": str(tmp_path), "DB_FILE_NAMES": ("pbgui.db", "pbgui_trades.db"),
        "MAIN_DB_NAME": "pbgui.db", "SERVICE": "DbTools",
        "_log": lambda *args, **kwargs: None,
        "_local_db_path": lambda name: tmp_path / name,
        "_background_tasks": set(), "_operations": {}, "_sync_scheduler_task": None,
        "_sync_job_locks": set(), "_SYNC_STATE_LOCK": threading.RLock(),
    }
    exec(compile(ast.Module(body=future + body, type_ignores=[]), "api/db_tools.py", "exec"), namespace)
    return SimpleNamespace(**{name: namespace[name] for name in names}, namespace=namespace)


def _database(path):
    """Create a small committed database and return its owned connection."""
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE sample (value TEXT)")
    conn.execute("INSERT INTO sample VALUES ('original')")
    conn.commit()
    return conn


@pytest.mark.parametrize("existing", [False, True])
def test_dbtools_source_busy_has_overall_deadline_and_closes(tmp_path, monkeypatch, tools, existing):
    """An exclusive source lock must not leave backup() sleeping indefinitely."""
    source, destination = tmp_path / "source.db", tmp_path / "snapshot.db"
    if existing:
        destination.write_bytes(b"previous snapshot")
    opened = []
    original = backup._connect

    def connect(path, mode, **kwargs):
        """Keep references so leaked context-manager connections cannot pass."""
        conn = original(path, mode, **kwargs)
        assert conn.execute("PRAGMA busy_timeout").fetchone() == (10,)
        opened.append(conn)
        return conn

    monkeypatch.setattr(backup, "_connect", connect)
    with closing(_database(source)) as writer:
        writer.execute("BEGIN EXCLUSIVE")
        writer.execute("UPDATE sample SET value='uncommitted'")
        start = time.monotonic()
        with pytest.raises(backup.RestoreBusyError):
            tools._sqlite_backup_file(source, destination, timeout=0.05)
        assert time.monotonic() - start < 2
        writer.rollback()
        assert writer.execute("SELECT value FROM sample").fetchone() == ("original",)
    assert len(opened) == 2
    for conn in opened:
        with pytest.raises(sqlite3.ProgrammingError):
            conn.execute("SELECT 1")
    if existing:
        assert destination.read_bytes() == b"previous snapshot"
    else:
        assert not destination.exists()
    assert not list(tmp_path.glob(".sqlite-backup-*"))


def test_dbtools_wal_and_existing_publication(tmp_path, tools):
    """WAL rows are included, and a name collision never unlinks earlier output."""
    source, destination = tmp_path / "source.db", tmp_path / "snapshot.db"
    with closing(_database(source)) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA wal_autocheckpoint=0")
        conn.execute("UPDATE sample SET value='wal'")
        conn.commit()
        tools._sqlite_backup_file(source, destination)
        before = destination.read_bytes()
        with pytest.raises(FileExistsError):
            tools._sqlite_backup_file(source, destination)
        assert destination.read_bytes() == before
    with closing(sqlite3.connect(destination)) as conn:
        assert conn.execute("SELECT value FROM sample").fetchone() == ("wal",)


def test_backup_late_done_is_success(tmp_path, monkeypatch, tools):
    """No post-copy deadline check may turn committed SQLITE_DONE into an error."""
    source, destination = tmp_path / "source.db", tmp_path / "snapshot.db"
    with closing(_database(source)):
        pass
    clock = [0.0]
    monkeypatch.setattr(backup.time, "monotonic", lambda: clock[0])
    original = backup._copy_database

    def copy(src, dst, check):
        """Advance the clock on a real SQLite DONE callback, not before it."""
        def progress(status=sqlite3.SQLITE_OK, remaining=0, total=0):
            """Keep successful completion immune to a late clock tick."""
            if status == sqlite3.SQLITE_DONE:
                clock[0] = 100
            check(status, remaining, total)

        original(src, dst, progress)

    monkeypatch.setattr(backup, "_copy_database", copy)
    tools._sqlite_backup_file(source, destination, timeout=1)
    assert destination.exists()


@pytest.mark.parametrize("fail", [False, True])
def test_install_cancellation_drains_worker_before_releasing_leases(tmp_path, tools, fail):
    """Cancellation keeps real DB/master leases until the off-loop worker exits."""
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()

    def slow_backup(*args):
        """Hold a bounded worker without accessing any runtime files or services."""
        entered.set()
        try:
            assert release.wait(3)
            if fail:
                raise backup.RestoreBusyError("isolated failure")
            return ""
        finally:
            finished.set()

    tools.namespace["_backup_local_file"] = slow_backup

    async def exercise():
        """The loop stays responsive and repeated cancellation cannot orphan work."""
        task = asyncio.create_task(tools._install_db_bundle("local", {}, "test", manage_pbdata=False))
        try:
            for _ in range(200):
                if entered.is_set():
                    break
                await asyncio.sleep(0.005)
            assert entered.is_set()
            for _ in range(2):
                task.cancel()
                await asyncio.sleep(0.01)
                assert not task.done()
                with pytest.raises(database_lock.DatabaseBusyError):
                    database_lock.acquire_database_lock(tmp_path)
                with pytest.raises(master_update_lock.MasterUpdateBusyError):
                    master_update_lock.acquire_master_update_lock(tmp_path)
        finally:
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await task
        assert finished.is_set()
        with database_lock.acquire_database_lock(tmp_path, exclusive=True):
            pass
        with master_update_lock.acquire_master_update_lock(tmp_path):
            pass

    asyncio.run(exercise())


def test_shutdown_waits_for_registered_backup_worker(tools):
    """Shutdown cannot clear its registry while a cancelled backup still owns files."""
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()

    def worker():
        """Represent bounded SQLite work that will close before its owner exits."""
        entered.set()
        assert release.wait(3)
        finished.set()

    async def exercise():
        """Drive the production task registry and shutdown implementation."""
        task = tools._track_background_task(tools._run_backup_worker(worker), name="isolated-backup")
        for _ in range(200):
            if entered.is_set():
                break
            await asyncio.sleep(0.005)
        shutdown = asyncio.create_task(tools.shutdown())
        try:
            assert entered.is_set()
            await asyncio.sleep(0.01)
            assert not shutdown.done() and not finished.is_set()
            assert task in tools.namespace["_background_tasks"]
        finally:
            release.set()
            await shutdown
        assert task.cancelled() and finished.is_set()
        assert not tools.namespace["_background_tasks"]

    asyncio.run(exercise())


def test_dbtools_integrity_uses_schema_guard_and_timeout_translation(tmp_path, monkeypatch, tools):
    """The staged DBTools integrity path shares restore's guard and deadline."""
    class ApiError(Exception):
        """A minimal boundary exception avoids importing unrelated API startup."""

        def __init__(self, *, status_code, detail):
            """Retain exactly the FastAPI fields emitted by this helper."""
            super().__init__(detail)
            self.status_code = status_code

    tools.namespace["HTTPException"] = ApiError
    source = tmp_path / "snapshot.db"
    with closing(_database(source)) as conn:
        conn.execute("CREATE TRIGGER hidden AFTER INSERT ON sample BEGIN SELECT zeroblob(1048576); END")
    with pytest.raises(ApiError) as invalid:
        tools._assert_sqlite_integrity(source)
    assert invalid.value.status_code == 400

    def deadline(timeout):
        """Expire before opening or parsing a staged source."""
        def check(*args):
            """Report the configured overall budget as exhausted."""
            raise backup.RestoreBusyError("expired")

        return check

    monkeypatch.setattr(backup, "_deadline", deadline)
    with pytest.raises(ApiError) as busy:
        tools._assert_sqlite_integrity(source)
    assert busy.value.status_code == 409
