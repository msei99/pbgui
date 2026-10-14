"""Inventory cache setup: serialized first access, WAL fallback, fork safety and cleanup (#410)."""

import multiprocessing as mp
import os
import sqlite3
import stat
import subprocess
import sys
import threading
import time
import warnings
from pathlib import Path

import pytest

import inventory_cache


class _TrackingConnection(sqlite3.Connection):
    """Record statements, closes and commits; optionally fail or fake one statement."""

    instances: list["_TrackingConnection"] = []
    statements: list[str] = []
    fail_on: str | None = None
    fail_error: Exception | None = None
    fake_wal_result: str | None = None

    def __init__(self, *args, **kwargs):
        """Register every connection opened through the cache."""
        super().__init__(*args, **kwargs)
        self.closed = False
        self.commits = 0
        _TrackingConnection.instances.append(self)

    def execute(self, sql, *args):
        """Fail the configured statement once, or report a fixed WAL switch result."""
        _TrackingConnection.statements.append(sql)
        marker = _TrackingConnection.fail_on
        if marker and marker in sql:
            _TrackingConnection.fail_on = None
            raise _TrackingConnection.fail_error or sqlite3.OperationalError("simulated setup failure")
        if sql == "PRAGMA journal_mode=WAL" and _TrackingConnection.fake_wal_result:
            return super().execute("SELECT ?", (_TrackingConnection.fake_wal_result,))
        return super().execute(sql, *args)

    def commit(self):
        """Count explicit commits."""
        self.commits += 1
        return super().commit()

    def close(self):
        """Mark the connection as closed."""
        self.closed = True
        return super().close()


def _reset_tracking() -> None:
    """Clear the tracking class state."""
    _TrackingConnection.instances = []
    _TrackingConnection.statements = []
    _TrackingConnection.fail_on = None
    _TrackingConnection.fail_error = None
    _TrackingConnection.fake_wal_result = None


@pytest.fixture(autouse=True)
def fresh_setup_state(monkeypatch):
    """Isolate the process-wide setup state of every test."""
    monkeypatch.setattr(inventory_cache, "_wal_attempted", set())
    monkeypatch.setattr(inventory_cache, "_open_lock_fds", set())
    monkeypatch.setattr(inventory_cache, "_setup_thread_lock", threading.Lock())
    monkeypatch.setattr(inventory_cache, "_state_lock", threading.Lock())


@pytest.fixture
def warnings_logged(monkeypatch) -> list[str]:
    """Collect cache log messages instead of writing PBGui logs."""
    messages: list[str] = []
    monkeypatch.setattr(inventory_cache, "_log", lambda service, message, **kwargs: messages.append(message))
    return messages


@pytest.fixture
def tracked_db(tmp_path: Path, monkeypatch, warnings_logged) -> Path:
    """Route the cache to an isolated database opened through _TrackingConnection."""
    db = tmp_path / "inventory_cache.db"
    real_connect = sqlite3.connect
    _reset_tracking()
    monkeypatch.setattr(inventory_cache, "_db_path", lambda: db)
    monkeypatch.setattr(inventory_cache.sqlite3, "connect",
                        lambda *args, **kwargs: real_connect(*args, factory=_TrackingConnection, **kwargs))
    return db


def _count_rows() -> int:
    """Read the inventory table through the normal cache connection."""
    with inventory_cache._get_conn() as conn:
        return conn.execute("SELECT count(*) FROM inventory").fetchone()[0]


def _journal_mode(db: Path) -> str:
    """Read the persistent journal mode through an independent connection."""
    conn = sqlite3.connect(str(db))
    try:
        return conn.execute("PRAGMA journal_mode").fetchone()[0]
    finally:
        conn.close()


def _key(db: Path) -> str:
    """Return the state key the cache uses for a database path."""
    return str(db.resolve())


def _busy_error() -> sqlite3.OperationalError:
    """Build the SQLITE_BUSY error a concurrent WAL switch reports."""
    error = sqlite3.OperationalError("database is locked")
    error.sqlite_errorcode = sqlite3.SQLITE_BUSY
    return error


def _assert_setup_lock_released() -> None:
    """Neither the thread lock nor a lock descriptor stays held."""
    assert inventory_cache._setup_thread_lock.acquire(blocking=False)
    inventory_cache._setup_thread_lock.release()
    assert inventory_cache._open_lock_fds == set()


@pytest.mark.parametrize("failing_statement", [
    "sqlite_master", "PRAGMA journal_mode", "journal_mode=WAL", "CREATE TABLE", "synchronous",
])
def test_setup_failure_closes_connection_and_next_access_succeeds(tracked_db: Path, failing_statement: str) -> None:
    """A failing setup statement leaks neither the connection nor the setup lock."""
    _TrackingConnection.fail_on = failing_statement

    with pytest.raises(sqlite3.OperationalError, match="simulated setup failure"):
        _count_rows()

    assert len(_TrackingConnection.instances) == 1
    assert _TrackingConnection.instances[0].closed is True
    _assert_setup_lock_released()
    assert _count_rows() == 0
    assert all(conn.closed for conn in _TrackingConnection.instances)


def test_new_database_is_set_up_once_and_warm_connections_only_read(tracked_db: Path, monkeypatch) -> None:
    """Setup runs under the lock once; warm connections run no DDL, WAL switch or lock."""
    assert _count_rows() == 0
    assert _journal_mode(tracked_db) == "wal"
    assert _key(tracked_db) in inventory_cache._wal_attempted

    def no_lock(_path):
        """Fail if a warm connection asks for the setup lock."""
        raise AssertionError("warm connection must not take the setup lock")

    monkeypatch.setattr(inventory_cache, "_setup_lock", no_lock)
    _TrackingConnection.statements = []
    assert _count_rows() == 0

    assert not any("CREATE TABLE" in sql for sql in _TrackingConnection.statements)
    assert "PRAGMA journal_mode=WAL" not in _TrackingConnection.statements


def test_busy_wal_switch_keeps_cache_usable_and_retries_later(tracked_db: Path, warnings_logged: list[str]) -> None:
    """A WAL switch blocked by outside readers is not marked as done and is retried."""
    _TrackingConnection.fail_on = "journal_mode=WAL"
    _TrackingConnection.fail_error = _busy_error()

    assert _count_rows() == 0
    assert _key(tracked_db) not in inventory_cache._wal_attempted
    assert _journal_mode(tracked_db) != "wal"

    assert _count_rows() == 0
    assert _journal_mode(tracked_db) == "wal"
    assert _key(tracked_db) in inventory_cache._wal_attempted
    assert warnings_logged == []


def test_existing_rollback_database_is_switched_once(tracked_db: Path) -> None:
    """A real pre-existing rollback-journal database with its table gets one WAL attempt."""
    legacy = sqlite3.connect(str(tracked_db))
    legacy.execute("PRAGMA journal_mode=DELETE")
    inventory_cache._init_db(legacy)
    legacy.execute("INSERT INTO inventory (exchange, dataset, coin) VALUES ('binance', 'l2book', 'BTC')")
    legacy.commit()
    legacy.close()
    assert _journal_mode(tracked_db) == "delete"

    assert _count_rows() == 1
    assert _journal_mode(tracked_db) == "wal"
    _TrackingConnection.statements = []
    assert _count_rows() == 1
    assert "PRAGMA journal_mode=WAL" not in _TrackingConnection.statements


def test_unsupported_wal_falls_back_once_without_repeated_warnings(tracked_db: Path, warnings_logged: list[str], monkeypatch) -> None:
    """If SQLite keeps rollback mode, the cache continues and warns once per process."""
    _TrackingConnection.fake_wal_result = "delete"

    for _ in range(3):
        assert _count_rows() == 0

    assert len(warnings_logged) == 1
    assert "'delete'" in warnings_logged[0]
    assert _TrackingConnection.statements.count("PRAGMA journal_mode=WAL") == 1
    monkeypatch.setattr(inventory_cache, "_setup_lock", lambda _path: (_ for _ in ()).throw(AssertionError("locked")))
    assert _count_rows() == 0


def test_failed_setup_does_not_mark_wal_attempt(tracked_db: Path, warnings_logged: list[str]) -> None:
    """A WAL attempt counts as done only after the whole setup completed."""
    _TrackingConnection.fake_wal_result = "delete"
    _TrackingConnection.fail_on = "CREATE TABLE"

    with pytest.raises(sqlite3.OperationalError):
        _count_rows()
    assert _key(tracked_db) not in inventory_cache._wal_attempted

    assert _count_rows() == 0
    assert _key(tracked_db) in inventory_cache._wal_attempted
    assert len(warnings_logged) == 1


def test_wal_attempt_state_is_bounded() -> None:
    """The per-process marker set never grows beyond its limit."""
    for index in range(inventory_cache._WAL_ATTEMPT_LIMIT + 5):
        inventory_cache._remember_wal_attempt(f"/tmp/db-{index}")
    assert 0 < len(inventory_cache._wal_attempted) <= inventory_cache._WAL_ATTEMPT_LIMIT


def test_setup_commits_nothing_and_schema_is_durable(tracked_db: Path) -> None:
    """The schema and WAL mode persist without a setup commit."""
    with inventory_cache._get_conn() as conn:
        assert conn.commits == 0

    independent = sqlite3.connect(str(tracked_db))
    try:
        tables = {row[0] for row in independent.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    finally:
        independent.close()
    assert "inventory" in tables
    assert _journal_mode(tracked_db) == "wal"


def test_existing_writes_still_commit(tracked_db: Path) -> None:
    """Writers keep their own commits; no data is lost."""
    with inventory_cache._get_conn() as conn:
        conn.execute("INSERT INTO inventory (exchange, dataset, coin) VALUES ('binance', 'l2book', 'BTC')")
        conn.commit()
    assert _count_rows() == 1

    inventory_cache.invalidate_coin("binance", "l2book", "BTC")
    assert _count_rows() == 0


def test_removed_database_is_recreated_on_next_access(tracked_db: Path) -> None:
    """The per-connection schema check sets up a database deleted between accesses."""
    assert _count_rows() == 0
    for suffix in ("", "-wal", "-shm"):
        Path(str(tracked_db) + suffix).unlink(missing_ok=True)

    assert _count_rows() == 0
    assert _journal_mode(tracked_db) == "wal"


def test_warm_reader_does_not_wait_for_wal_writer(tracked_db: Path) -> None:
    """Readers of a set-up database proceed while another connection holds a write transaction."""
    assert _count_rows() == 0
    writer = sqlite3.connect(str(tracked_db), isolation_level=None)
    try:
        writer.execute("BEGIN IMMEDIATE")
        writer.execute("INSERT INTO inventory (exchange, dataset, coin) VALUES ('binance', 'l2book', 'ETH')")
        started = time.monotonic()
        assert _count_rows() == 0
        assert time.monotonic() - started < 2
    finally:
        writer.execute("ROLLBACK")
        writer.close()


def test_setup_lock_times_out_when_thread_lock_is_held(tracked_db: Path, monkeypatch) -> None:
    """A held setup lock fails within the bounded wait and closes the connection."""
    monkeypatch.setattr(inventory_cache, "_SETUP_LOCK_TIMEOUT_SECONDS", 0.2)
    inventory_cache._setup_thread_lock.acquire()
    try:
        with pytest.raises(sqlite3.OperationalError, match="setup lock timed out"):
            _count_rows()
    finally:
        inventory_cache._setup_thread_lock.release()
    assert _TrackingConnection.instances[0].closed is True
    _assert_setup_lock_released()


def test_setup_lock_times_out_when_another_process_holds_it(tracked_db: Path, monkeypatch) -> None:
    """The file lock is bounded too, and is released for the next access."""
    monkeypatch.setattr(inventory_cache, "_SETUP_LOCK_TIMEOUT_SECONDS", 0.3)
    lock_path = Path(str(tracked_db) + ".lock")
    holder = subprocess.Popen(
        [sys.executable, "-c",
         "import fcntl, os, sys\n"
         "fd = os.open(sys.argv[1], os.O_RDWR | os.O_CREAT, 0o600)\n"
         "fcntl.lockf(fd, fcntl.LOCK_EX)\n"
         "print('locked', flush=True)\n"
         "sys.stdin.readline()\n",
         str(lock_path)],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    try:
        assert holder.stdout.readline().strip() == "locked"
        with pytest.raises(sqlite3.OperationalError, match="setup lock timed out"):
            _count_rows()
        _assert_setup_lock_released()
    finally:
        holder.communicate("\n", timeout=10)
    assert _count_rows() == 0


def test_lock_file_permissions_are_repaired(tracked_db: Path) -> None:
    """An existing lock file is made owner-only through its descriptor."""
    lock_path = Path(str(tracked_db) + ".lock")
    lock_path.write_text("")
    os.chmod(lock_path, 0o644)

    assert _count_rows() == 0

    assert stat.S_IMODE(lock_path.stat().st_mode) == 0o600


@pytest.mark.parametrize("unsafe", ["symlink", "fifo", "foreign_owner"])
def test_unsafe_lock_file_is_rejected_and_descriptor_closed(tracked_db: Path, tmp_path: Path, monkeypatch, unsafe: str) -> None:
    """Links, non-regular files and foreign owners are refused without leaking descriptors."""
    lock_path = Path(str(tracked_db) + ".lock")
    closed: list[int] = []
    real_close = os.close
    monkeypatch.setattr(inventory_cache.os, "close", lambda fd: (closed.append(fd), real_close(fd))[1])
    if unsafe == "symlink":
        target = tmp_path / "elsewhere"
        target.write_text("")
        lock_path.symlink_to(target)
        expected = OSError
    elif unsafe == "fifo":
        os.mkfifo(lock_path)
        expected = sqlite3.OperationalError
    else:
        lock_path.write_text("")
        monkeypatch.setattr(inventory_cache.os, "geteuid", lambda: os.getuid() + 1)
        expected = sqlite3.OperationalError

    with pytest.raises(expected):
        _count_rows()

    if expected is sqlite3.OperationalError:
        assert len(closed) == 1
    assert _TrackingConnection.instances[0].closed is True
    _assert_setup_lock_released()


def _hold_setup_lock(path: Path, held: threading.Event, release: threading.Event) -> None:
    """Hold the setup lock (thread and file) until released."""
    with inventory_cache._setup_lock(path):
        held.set()
        release.wait(30)


def test_forked_child_proceeds_while_parent_thread_holds_setup_lock(tmp_path: Path, monkeypatch) -> None:
    """A child forked during setup gets fresh locks and does not release the parent's lock."""
    parent_db = tmp_path / "parent.db"
    child_db = tmp_path / "child.db"
    monkeypatch.setattr(inventory_cache, "_db_path", lambda: child_db)
    held, release = threading.Event(), threading.Event()
    holder = threading.Thread(target=_hold_setup_lock, args=(parent_db, held, release))
    holder.start()
    try:
        assert held.wait(10)
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message=".*fork.*", category=DeprecationWarning)
            pid = os.fork()
        if pid == 0:  # child: never return into pytest
            code = 1
            try:
                code = 0 if _count_rows() == 0 else 2
            finally:
                os._exit(code)
        deadline = time.monotonic() + 20
        status = None
        while time.monotonic() < deadline:
            finished, status = os.waitpid(pid, os.WNOHANG)
            if finished:
                break
            time.sleep(0.05)
        else:
            os.kill(pid, 9)
            os.waitpid(pid, 0)
            pytest.fail("forked child blocked on an inherited setup lock")
        assert os.waitstatus_to_exitcode(status) == 0

        probe = subprocess.run(
            [sys.executable, "-c",
             "import fcntl, os, sys\n"
             "fd = os.open(sys.argv[1], os.O_RDWR)\n"
             "try:\n    fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)\nexcept OSError:\n    sys.exit(3)\n",
             str(parent_db) + ".lock"],
            timeout=10)
        assert probe.returncode == 3, "the child's exit must not release the parent's setup lock"
    finally:
        release.set()
        holder.join(10)


def _cold_reader(paths: list[str], barrier, results) -> None:
    """Open each fresh database at the same moment as the other processes."""
    failures = []
    for path in paths:
        inventory_cache._db_path = lambda path=path: Path(path)
        barrier.wait()
        for _ in range(3):
            try:
                with inventory_cache._get_conn() as conn:
                    conn.execute("SELECT count(*) FROM inventory").fetchone()
            except Exception as exc:  # reported to the parent
                failures.append(repr(exc))
    results.put(failures)


def test_concurrent_cold_access_from_processes_does_not_fail(tmp_path: Path) -> None:
    """Simultaneous first access from several processes is serialized without lock errors."""
    ctx = mp.get_context("fork")
    workers = 4
    paths = [str(tmp_path / f"cold_{index}.db") for index in range(15)]
    barrier = ctx.Barrier(workers)
    results = ctx.Queue()
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message=".*fork.*", category=DeprecationWarning)
        processes = [ctx.Process(target=_cold_reader, args=(paths, barrier, results)) for _ in range(workers)]
        for process in processes:
            process.start()
    failures = [failure for _ in processes for failure in results.get(timeout=120)]
    for process in processes:
        process.join(30)
    assert all(process.exitcode == 0 for process in processes)
    assert failures == []


def test_concurrent_cold_readers_do_not_fail(tmp_path: Path, monkeypatch) -> None:
    """Simultaneous first accesses from threads to new databases complete without lock errors.

    Before the fix about one in ten fresh databases reported "database is
    locked" from the concurrent WAL switch; 40 databases make a miss unlikely.
    """
    workers = 8
    errors: list[str] = []
    for index in range(40):
        db = tmp_path / f"inventory_cache_{index}.db"
        monkeypatch.setattr(inventory_cache, "_db_path", lambda db=db: db)
        barrier = threading.Barrier(workers)

        def reader() -> None:
            """Start together, then read through fresh connections."""
            barrier.wait()
            for _ in range(5):
                try:
                    _count_rows()
                except Exception as exc:  # collected and asserted below
                    errors.append(repr(exc))

        threads = [threading.Thread(target=reader) for _ in range(workers)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=60)
        assert not any(thread.is_alive() for thread in threads)

    assert errors == []
