"""Income restore integration with temporary SQLite data and real local leases."""

import asyncio
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, nullcontext
from functools import partial
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, call

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

import Database as database_mod
import PBApiServer
import pbgui_purefunc
import sqlite_backup
from api import cluster, coin_data, dashboard, db_tools, pareto_explorer, vps_manager
from database_lock import DatabaseBusyError, acquire_database_lock
from master_update_lock import (
    MasterUpdateBusyError,
    acquire_master_runtime_lock,
    acquire_master_update_lock,
)


@pytest.fixture(autouse=True)
def isolated_runtime(tmp_path, monkeypatch):
    """Keep all databases/leases local to the test and forbid process operations."""
    for module in (database_mod, PBApiServer, pbgui_purefunc, dashboard, db_tools):
        monkeypatch.setattr(module, "PBGDIR", tmp_path)
    monkeypatch.setattr(dashboard, "_income_restore_active", threading.Event())
    for name in ("_get_db", "_pbdata_stop", "_pbdata_start"):
        monkeypatch.setattr(dashboard, name, Mock(side_effect=AssertionError(name)))
    for name in ("_backup_local_file", "_pool"):
        monkeypatch.setattr(db_tools, name, Mock(side_effect=AssertionError(name)))
    for name in ("_stop_target_pbdata", "_start_target_pbdata"):
        monkeypatch.setattr(db_tools, name, AsyncMock(side_effect=AssertionError(name)))
    monkeypatch.setattr(db_tools.shutil, "copy2", Mock(side_effect=AssertionError("Disk install")))
    for name in ("_restart_status_payload", "_restart_current_api_systemd_unit"):
        monkeypatch.setattr(PBApiServer, name, Mock(side_effect=AssertionError(name)))
    monkeypatch.setattr(db_tools, "_operations", {})
    for module in (cluster, coin_data, pareto_explorer):
        monkeypatch.setattr(module, "restart_block_reason", lambda: "")
    for name in ("profit_sweep_restart_block_reason", "ai_restart_block_reason",
                 "credential_migration_restart_block_reason"):
        monkeypatch.setattr(PBApiServer, name, lambda *_: "")
    monkeypatch.setattr(vps_manager, "get_service_instance", lambda: SimpleNamespace(
        active_vps_deploy_summary=lambda: {"active": False},
    ))
    monkeypatch.setattr(dashboard, "_log", Mock())
    monkeypatch.setattr(database_mod, "_human_log", Mock())
    yield
    for name in ("_get_db", "_pbdata_stop", "_pbdata_start"):
        getattr(dashboard, name).assert_not_called()
    PBApiServer._restart_status_payload.assert_not_called()
    PBApiServer._restart_current_api_systemd_unit.assert_not_called()
    db_tools.shutil.copy2.assert_not_called()
    db_tools._pool.assert_not_called()
    assert not dashboard._income_restore_active.is_set()


@pytest.fixture
def client():
    """Mount production handlers without application lifespan or real auth state."""
    app = FastAPI()
    app.include_router(dashboard.router, prefix="/api/dashboard")
    app.add_api_route("/api/server-restart", PBApiServer.server_restart, methods=["POST"])
    app.dependency_overrides[dashboard.require_auth] = lambda: object()
    app.dependency_overrides[PBApiServer.require_auth] = lambda: object()
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def database(tmp_path):
    """Create the real main schema, retaining the Database object's WAL connection."""
    (tmp_path / "data").mkdir()
    db = database_mod.Database.__new__(database_mod.Database)
    db.db = tmp_path / "data" / "pbgui.db"
    db._write_lock = threading.Lock()
    with closing(db._connect()) as conn:
        db.create_tables()
        conn.execute("PRAGMA wal_autocheckpoint=0")
        conn.execute("INSERT INTO balances(timestamp, balance, user) VALUES (1, 100, 'alice')")
        conn.execute(
            "INSERT INTO history(symbol, timestamp, income, uniqueid, user) VALUES (?, ?, ?, ?, ?)",
            ("BTCUSDT", 1, 7.5, "income-1", "alice"),
        )
        conn.commit()
        yield db


@pytest.mark.parametrize("entrypoint", ["route", "database"])
def test_wal_backup_restore_keeps_cached_readers_and_snapshot(client, database, entrypoint):
    """Both entrypoints restore committed WAL data without replacing connected files."""
    cached = database._connect()
    assert database.db.with_name("pbgui.db-wal").stat().st_size > 0
    backup = database.backup_full_db()
    assert backup is not None
    source = Path(backup)
    original = source.read_bytes()
    inode = database.db.stat().st_ino
    with closing(sqlite3.connect(source)) as snapshot:
        assert snapshot.execute("SELECT balance FROM balances").fetchall() == [(100,)]
        assert snapshot.execute("SELECT income FROM history").fetchall() == [(7.5,)]
    cached.execute("UPDATE balances SET balance=999")
    cached.execute("DELETE FROM history")
    cached.commit()
    with closing(sqlite3.connect(database.db)) as reader:
        reader.execute("BEGIN")
        assert reader.execute("SELECT balance FROM balances").fetchall() == [(999,)]
        if entrypoint == "route":
            response = client.post("/api/dashboard/income/restore", json={"path": backup})
            assert response.status_code == 200
            assert response.json() == {"ok": True}
        else:
            assert database.restore_db_from(backup) is True
        assert database._connect() is cached
        assert cached.execute("SELECT balance FROM balances").fetchall() == [(100,)]
        assert cached.execute("SELECT income FROM history").fetchall() == [(7.5,)]
        assert reader.execute("SELECT balance FROM balances").fetchall() == [(999,)]
        reader.rollback()
        assert reader.execute("SELECT balance FROM balances").fetchall() == [(100,)]
        assert cached.execute("PRAGMA journal_mode").fetchone() == ("wal",)
    assert database.db.stat().st_ino == inode
    assert source.read_bytes() == original
    assert list(source.parent.iterdir()) == [source]
    assert asyncio.run(PBApiServer._restart_block_state()) == (False, "")


def test_incompatible_sqlite_backup_leaves_live_data_untouched(client, database, tmp_path):
    """A readable SQLite file without the required main schema is rejected safely."""
    root = tmp_path / "data" / "backup" / "db"
    root.mkdir(parents=True)
    source = root / "incompatible.db"
    with closing(sqlite3.connect(source)) as conn:
        conn.execute("CREATE TABLE history (id INTEGER)")
        conn.commit()
    response = client.post("/api/dashboard/income/restore", json={"path": str(source)})
    assert response.status_code == 400
    assert database._connect().execute("SELECT balance FROM balances").fetchall() == [(100,)]
    assert database.restore_db_from(str(source)) is False
    assert asyncio.run(PBApiServer._restart_block_state()) == (False, "")
    with acquire_master_update_lock(tmp_path):
        pass


@pytest.mark.parametrize("error", [sqlite_backup.InvalidBackupError, sqlite_backup.RestoreBusyError,
                                 DatabaseBusyError, MasterUpdateBusyError, RuntimeError])
def test_database_restore_failure_keeps_bool_api_and_releases_lease(database, tmp_path, monkeypatch, error):
    """Legacy callers receive False on helper errors, with no leaked exclusive lease."""
    def fail(source, destination, root):
        """Verify the helper runs under the lease before simulating a failure."""
        assert destination == database.db
        assert root == tmp_path / "data" / "backup" / "db"
        with acquire_master_runtime_lock(tmp_path):
            pass
        with pytest.raises(MasterUpdateBusyError):
            acquire_master_update_lock(tmp_path)
        with pytest.raises(DatabaseBusyError):
            acquire_database_lock(tmp_path)
        raise error("isolated failure")

    callback = Mock(side_effect=fail)
    monkeypatch.setattr(database_mod, "restore_sqlite_backup", callback)
    assert database.restore_db_from(str(tmp_path / "snapshot.db")) is False
    callback.assert_called_once()
    with acquire_master_update_lock(tmp_path):
        pass
    with acquire_database_lock(tmp_path, exclusive=True):
        pass


@pytest.mark.parametrize("lease_factory", [
    acquire_master_update_lock, acquire_database_lock, partial(acquire_database_lock, exclusive=True),
])
def test_existing_lease_blocks_restore_before_helper(client, database, tmp_path, monkeypatch, lease_factory):
    """Master-exclusive or either DB lease denies restore before entering its helper."""
    callback = Mock(side_effect=AssertionError("Restore admitted"))
    monkeypatch.setattr(sqlite_backup, "restore_sqlite_backup", callback)
    monkeypatch.setattr(database_mod, "restore_sqlite_backup", callback)
    with lease_factory(tmp_path):
        response = client.post("/api/dashboard/income/restore", json={"path": "unused.db"})
        assert response.status_code == 409
        assert database.restore_db_from("unused.db") is False
        assert not dashboard._income_restore_active.is_set()
    callback.assert_not_called()
    assert asyncio.run(PBApiServer._restart_block_state()) == (False, "")
    with acquire_master_update_lock(tmp_path):
        pass
    with acquire_database_lock(tmp_path, exclusive=True):
        pass


@pytest.mark.parametrize("fail", [False, True])
def test_active_restore_excludes_installer_second_restore_and_restart(client, tmp_path, monkeypatch, fail):
    """An overlapping restore owns admission until its callback succeeds or fails."""
    entered, release = threading.Event(), threading.Event()

    def restore(*_args):
        """Hold the production route inside its callback without using real processes."""
        entered.set()
        assert release.wait(10), "Test did not release restore"
        if fail:
            raise RuntimeError("isolated failure")

    callback = Mock(side_effect=restore)
    monkeypatch.setattr(sqlite_backup, "restore_sqlite_backup", callback)
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(dashboard.restore_income_backup, dashboard.IncomeRestore(path="unused.db"), object())
        try:
            assert entered.wait(10), "Restore did not enter callback"
            reason = dashboard.restart_block_reason()
            assert "Income database restore" in reason
            assert asyncio.run(PBApiServer._restart_block_state()) == (True, reason)
            with pytest.raises(HTTPException) as blocked:
                asyncio.run(db_tools._install_db_bundle("local", {}, "test"))
            assert blocked.value.status_code == 409
            db_tools._backup_local_file.assert_not_called()
            db_tools._stop_target_pbdata.assert_not_called()
            db_tools._start_target_pbdata.assert_not_called()
            assert client.post("/api/dashboard/income/restore", json={"path": "unused.db"}).status_code == 409
            assert dashboard.restart_block_reason() == reason
            assert client.post("/api/server-restart").status_code == 409
            callback.assert_called_once()
        finally:
            release.set()
        if fail:
            with pytest.raises(HTTPException) as failure:
                future.result(timeout=10)
            assert failure.value.status_code == 500
        else:
            assert future.result(timeout=10) == {"ok": True}
    assert asyncio.run(PBApiServer._restart_block_state()) == (False, "")
    with acquire_master_update_lock(tmp_path):
        pass


@pytest.mark.parametrize("manage_pbdata,failure", [(False, "backup"), (True, "backup"),
                                                (True, "stop"), (True, "restart")])
def test_installer_lease_blocks_restore_through_cleanup(client, tmp_path, monkeypatch, manage_pbdata, failure):
    """DB Tools holds master SH and DB EX from backup through failed cleanup."""
    phases = []
    callback = Mock(side_effect=AssertionError("Restore admitted during install"))
    monkeypatch.setattr(sqlite_backup, "restore_sqlite_backup", callback)

    def phase(name):
        """Check shared/exclusive ordering at each mocked pre-install or cleanup step."""
        phases.append(name)
        with acquire_master_runtime_lock(tmp_path):
            pass
        with pytest.raises(MasterUpdateBusyError):
            acquire_master_update_lock(tmp_path)
        with pytest.raises(DatabaseBusyError):
            acquire_database_lock(tmp_path)
        assert client.post("/api/dashboard/income/restore", json={"path": "unused.db"}).status_code == 409
        assert not dashboard._income_restore_active.is_set()
        if failure == name:
            raise RuntimeError(f"{name} failed")

    async def stop(*_args):
        """Simulate PBData ownership without starting or stopping a process."""
        phase("stop")
        return True

    async def restart(*_args):
        """Exercise final cleanup while the installer's shared lease is still held."""
        phase("restart")

    monkeypatch.setattr(db_tools, "_backup_local_file", lambda *_: phase("backup") or "")
    monkeypatch.setattr(db_tools, "_stop_target_pbdata", stop)
    monkeypatch.setattr(db_tools, "_start_target_pbdata", restart)
    with pytest.raises(RuntimeError, match=f"{failure} failed"):
        asyncio.run(db_tools._install_db_bundle("local", {}, "test", manage_pbdata=manage_pbdata))
    assert phases[0] == "backup"
    assert ("stop" in phases) == (manage_pbdata and failure != "backup")
    assert ("restart" in phases) == manage_pbdata
    callback.assert_not_called()
    with acquire_master_update_lock(tmp_path):
        pass
    with acquire_database_lock(tmp_path, exclusive=True):
        pass


def test_restart_route_honors_restore_reason_and_releases_reservation(client, tmp_path):
    """The restart route checks the aggregated restore event even without a held lease."""
    dashboard._income_restore_active.set()
    try:
        response = client.post("/api/server-restart")
        assert response.status_code == 409
        assert dashboard.restart_block_reason() in response.json()["detail"]
        with acquire_master_update_lock(tmp_path):
            pass
    finally:
        dashboard._income_restore_active.clear()
    assert asyncio.run(PBApiServer._restart_block_state()) == (False, "")


def _update_history_worker(database, user):
    """Run a real scan and close its cached SQLite connection in the owning thread."""
    try:
        database.update_history(user)
    finally:
        database.close_thread_connections()


@pytest.mark.parametrize("pause_phase", ["fetch", "rows"])
def test_history_scan_excludes_restore_but_allows_backup_until_all_rows_written(
    client, database, tmp_path, monkeypatch, pause_phase,
):
    """DB SH spans the whole scan, permits backup, and never owns restart admission."""
    user = SimpleNamespace(name="alice", exchange="bybit")
    scan_ts = 100_000_000
    database.set_last_scan_ts(user.name, user.exchange, scan_ts)
    source = database.backup_full_db()
    assert source is not None
    entered, release = threading.Event(), threading.Event()
    rows = [
        {"symbol": "BTCUSDT", "timestamp": scan_ts + index, "income": index, "uniqueid": f"new-{index}"}
        for index in (1, 2)
    ]

    def pause():
        """Expose a deterministic overlap window without holding a SQLite transaction."""
        entered.set()
        assert release.wait(10), "History test did not release worker"

    def fetch(since):
        """Replace only the exchange network call, not Database.fetch_history."""
        assert since == scan_ts - database._HISTORY_SCAN_LOOKBACK_MS
        if pause_phase == "fetch":
            pause()
        return rows

    add_history = database.add_history

    def write(conn, income):
        """Pause after the first committed row to check the lease covers the full batch."""
        add_history(conn, income)
        if pause_phase == "rows" and income[3] == "new-1":
            pause()

    exchange = Mock(fetch_history=Mock(side_effect=fetch))
    factory = Mock(return_value=exchange)
    backup = Mock(wraps=database_mod.backup_sqlite_database)
    monkeypatch.setattr(database_mod, "Exchange", factory)
    monkeypatch.setattr(database, "add_history", write)
    monkeypatch.setattr(database_mod, "backup_sqlite_database", backup)
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(_update_history_worker, database, user)
        try:
            assert entered.wait(10), "History worker did not reach pause"
            with acquire_master_update_lock(tmp_path):
                pass
            with acquire_database_lock(tmp_path):
                pass
            response = client.post("/api/dashboard/income/restore", json={"path": source})
            assert response.status_code == 409
            concurrent_backup = database.backup_full_db()
            assert concurrent_backup is not None
            backup.assert_called_once()
            with closing(sqlite3.connect(concurrent_backup)) as snapshot:
                assert snapshot.execute("SELECT COUNT(*) FROM history").fetchone() == (
                    1 if pause_phase == "fetch" else 2,
                )
            conn = database._connect()
            assert conn.execute("SELECT COUNT(*) FROM history").fetchone() == (
                1 if pause_phase == "fetch" else 2,
            )
            if pause_phase == "fetch":
                assert database.get_last_scan_ts(user.name, user.exchange) == scan_ts
            else:
                assert database.get_last_scan_ts(user.name, user.exchange) > scan_ts
        finally:
            release.set()
        future.result(timeout=10)
    factory.assert_called_once_with(user.exchange, user)
    exchange.fetch_history.assert_called_once()
    exchange.close.assert_called_once()
    assert database._connect().execute("SELECT uniqueid FROM history ORDER BY id").fetchall() == [
        ("income-1",), ("new-1",), ("new-2",),
    ]
    assert database.get_last_scan_ts(user.name, user.exchange) > scan_ts
    response = client.post("/api/dashboard/income/restore", json={"path": source})
    assert response.status_code == 200
    assert database._connect().execute("SELECT uniqueid FROM history").fetchall() == [("income-1",)]
    assert database.get_last_scan_ts(user.name, user.exchange) == scan_ts
    assert database.backup_full_db() is not None
    assert backup.call_count == 2


def test_active_restore_defers_history_without_fetch_or_scan_metadata_changes(
    client, database, monkeypatch,
):
    """A scan skipped at admission leaves its cursor intact and works after restore."""
    user = SimpleNamespace(name="alice", exchange="bybit")
    scan_ts = 100_000_000
    database.set_last_scan_ts(user.name, user.exchange, scan_ts)
    source = database.backup_full_db()
    assert source is not None
    entered, release = threading.Event(), threading.Event()
    native_restore = sqlite_backup.restore_sqlite_backup

    def restore(*args):
        """Pause inside the real route's exclusive lease, then restore actual SQLite data."""
        entered.set()
        assert release.wait(10), "Restore test did not release callback"
        native_restore(*args)

    exchange = Mock(fetch_history=Mock(return_value=[{
        "symbol": "BTCUSDT", "timestamp": scan_ts + 1, "income": 2.5, "uniqueid": "after-restore",
    }]))
    factory = Mock(return_value=exchange)
    fetch = Mock(wraps=database.fetch_history)
    monkeypatch.setattr(sqlite_backup, "restore_sqlite_backup", restore)
    monkeypatch.setattr(database_mod, "Exchange", factory)
    monkeypatch.setattr(database, "fetch_history", fetch)
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(client.post, "/api/dashboard/income/restore", json={"path": source})
        try:
            assert entered.wait(10), "Restore did not enter callback"
            database.update_history(user)
            fetch.assert_not_called()
            factory.assert_not_called()
            assert database.get_last_scan_ts(user.name, user.exchange) == scan_ts
            assert database._connect().execute("SELECT uniqueid FROM history").fetchall() == [("income-1",)]
        finally:
            release.set()
        assert future.result(timeout=10).status_code == 200
        executor.submit(_update_history_worker, database, user).result(timeout=10)
    fetch.assert_called_once_with(user)
    factory.assert_called_once_with(user.exchange, user)
    exchange.fetch_history.assert_called_once_with(scan_ts - database._HISTORY_SCAN_LOOKBACK_MS)
    exchange.close.assert_called_once()
    assert database.get_last_scan_ts(user.name, user.exchange) > scan_ts
    assert database._connect().execute("SELECT income FROM history WHERE uniqueid='after-restore'").fetchone() == (2.5,)


@pytest.mark.parametrize("error", [None, sqlite_backup.RestoreBusyError, RuntimeError])
def test_backup_excludes_install_until_sqlite_helper_finishes(database, tmp_path, monkeypatch, error):
    """Backup holds DB SH, excludes the installer, and leaves master admission independent."""
    entered, release = threading.Event(), threading.Event()
    native_backup = database_mod.backup_sqlite_database

    def backup(source, destination):
        """Hold backup admission while another thread tries a local install."""
        entered.set()
        assert release.wait(10), "Backup test did not release callback"
        if error is not None:
            raise error("isolated backup failure")
        native_backup(source, destination)

    callback = Mock(side_effect=backup)
    monkeypatch.setattr(database_mod, "backup_sqlite_database", callback)
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(database.backup_full_db)
        try:
            assert entered.wait(10), "Backup did not enter callback"
            with acquire_master_update_lock(tmp_path), acquire_database_lock(tmp_path):
                pass
            with pytest.raises(HTTPException) as blocked:
                asyncio.run(db_tools._install_db_bundle("local", {}, "test"))
            assert blocked.value.status_code == 409
            db_tools._backup_local_file.assert_not_called()
            db_tools._stop_target_pbdata.assert_not_called()
            db_tools._start_target_pbdata.assert_not_called()
        finally:
            release.set()
        result = future.result(timeout=10)
    callback.assert_called_once()
    if error is None:
        assert result is not None and Path(result).is_file()
    else:
        assert result is None
    with acquire_master_update_lock(tmp_path):
        pass
    with acquire_database_lock(tmp_path, exclusive=True):
        pass
    monkeypatch.setattr(db_tools, "_backup_local_file", Mock(return_value=""))
    monkeypatch.setattr(db_tools, "_stop_target_pbdata", AsyncMock(return_value=True))
    monkeypatch.setattr(db_tools, "_start_target_pbdata", AsyncMock())
    assert asyncio.run(db_tools._install_db_bundle("local", {}, "test")) == {
        "backups": [], "pbdata_was_running": True,
    }
    db_tools._stop_target_pbdata.assert_awaited_once_with("local", None)
    db_tools._start_target_pbdata.assert_awaited_once_with("local", True, None)


@pytest.mark.parametrize("fail", [False, True])
def test_installer_excludes_backup_and_releases_lease_on_failure(database, tmp_path, monkeypatch, fail):
    """A held installer lease prevents SQLite backup from starting, including failure cleanup."""
    entered, release = threading.Event(), threading.Event()

    def install_backup(*_args):
        """Pause at the installer's first step without backing up or installing files."""
        entered.set()
        assert release.wait(10), "Installer test did not release callback"
        if fail:
            raise RuntimeError("isolated installer failure")
        return ""

    async def install():
        """Run only leased pre-install steps with an empty bundle and no process control."""
        return await db_tools._install_db_bundle("local", {}, "test", manage_pbdata=False)

    backup = Mock(wraps=database_mod.backup_sqlite_database)
    monkeypatch.setattr(db_tools, "_backup_local_file", install_backup)
    monkeypatch.setattr(database_mod, "backup_sqlite_database", backup)
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(asyncio.run, install())
        try:
            assert entered.wait(10), "Installer did not reach backup step"
            assert database.backup_full_db() is None
            backup.assert_not_called()
            with acquire_master_runtime_lock(tmp_path):
                pass
            with pytest.raises(DatabaseBusyError):
                acquire_database_lock(tmp_path)
            with pytest.raises(MasterUpdateBusyError):
                acquire_master_update_lock(tmp_path)
        finally:
            release.set()
        if fail:
            with pytest.raises(RuntimeError, match="isolated installer failure"):
                future.result(timeout=10)
        else:
            assert future.result(timeout=10) == {"backups": [], "pbdata_was_running": False}
    db_tools._stop_target_pbdata.assert_not_called()
    db_tools._start_target_pbdata.assert_not_called()
    assert database.backup_full_db() is not None
    backup.assert_called_once()
    with acquire_master_update_lock(tmp_path):
        pass
    with acquire_database_lock(tmp_path, exclusive=True):
        pass


@pytest.mark.parametrize("target,conflict", [("local", True), ("local", False), ("remote", False)])
def test_full_copy_runner_delegates_pbdata_ownership_to_leased_installer(
    tmp_path, monkeypatch, target, conflict,
):
    """Full-copy staging never stops PBData before the installer accepts ownership."""
    source = "remote-source" if target == "local" else "local"
    operation = Mock(to_dict=Mock(return_value={"id": "isolated-copy"}))
    start = Mock(return_value=operation)
    paths = {name: tmp_path / "staged" / name for name in db_tools.DB_FILE_NAMES}

    async def stage(actual_source, directory, label, progress):
        """Return mock source paths while verifying staging is project-local and passive."""
        assert actual_source == source
        assert directory.is_relative_to(tmp_path) and directory.is_dir()
        assert label == "source" and progress is operation
        db_tools._stop_target_pbdata.assert_not_called()
        db_tools._start_target_pbdata.assert_not_called()
        return paths

    monkeypatch.setattr(db_tools, "tempfile", SimpleNamespace(
        TemporaryDirectory=lambda **kwargs: TemporaryDirectory(dir=tmp_path, **kwargs),
    ))
    monkeypatch.setattr(db_tools, "_assert_known_target", AsyncMock())
    monkeypatch.setattr(db_tools, "_stage_db_bundle", AsyncMock(side_effect=stage))
    monkeypatch.setattr(db_tools, "_start_operation", start)
    monkeypatch.setattr(db_tools, "_log", Mock())
    install_result = {"backups": ["isolated-backup"], "pbdata_was_running": True}
    install = (AsyncMock(wraps=db_tools._install_db_bundle) if conflict
               else AsyncMock(return_value=install_result))
    monkeypatch.setattr(db_tools, "_install_db_bundle", install)
    response = asyncio.run(db_tools.copy_database_run(
        db_tools.CopyDatabaseRequest(source=source, target=target), session=object(),
    ))
    assert response == {"operation": {"id": "isolated-copy"}}
    start.assert_called_once()
    assert start.call_args.args[:2] == ("copy-database", db_tools._operation_total("copy-database"))
    runner = start.call_args.args[2]
    install.assert_not_called()
    db_tools._stage_db_bundle.assert_not_called()
    if conflict:
        with acquire_master_update_lock(tmp_path):
            with pytest.raises(HTTPException) as blocked:
                asyncio.run(runner(operation))
            assert blocked.value.status_code == 409
        db_tools._backup_local_file.assert_not_called()
    else:
        assert asyncio.run(runner(operation)) == {
            "ok": True, "source": source, "target": target, **install_result,
        }
    install.assert_awaited_once_with(target, paths, "full-db-copy", operation)
    db_tools._stage_db_bundle.assert_awaited_once()
    db_tools._stop_target_pbdata.assert_not_called()
    db_tools._start_target_pbdata.assert_not_called()
    assert not list(tmp_path.glob("pbgui-db-tools-full-*"))
    with acquire_master_update_lock(tmp_path):
        pass


@pytest.mark.parametrize("route,payload,method,args", [
    ("delete_ids", {"ids": [1, 2]}, "delete_income_by_ids", ([1, 2],)),
    ("delete_older", {"users": ["alice"], "cutoff_ms": 1000}, "delete_income_older_than", (["alice"], 1000)),
])
@pytest.mark.parametrize("was_running", [False, True])
@pytest.mark.parametrize("outcome", ["no_backup", "delete_error", "success"])
def test_income_delete_requires_backup_and_preserves_pbdata_ownership(
    client, monkeypatch, route, payload, method, args, was_running, outcome,
):
    """Both delete routes fail closed and restart only PBData that they stopped."""
    events = Mock()
    db = events.database
    db.backup_full_db.return_value = None if outcome == "no_backup" else "isolated-backup.db"
    delete = getattr(db, method)
    delete.return_value = 2
    if outcome == "delete_error":
        delete.side_effect = HTTPException(status_code=500, detail="Isolated deletion failure")
    events.stop.return_value = was_running
    with monkeypatch.context() as patch:
        patch.setattr(dashboard, "_get_db", Mock(return_value=db))
        patch.setattr(dashboard, "_pbdata_stop", events.stop)
        patch.setattr(dashboard, "_pbdata_start", events.start)
        response = client.post(f"/api/dashboard/income/{route}", json=payload)
        dashboard._get_db.assert_called_once_with()
        assert response.status_code == {"no_backup": 409, "delete_error": 500, "success": 200}[outcome]
        expected = [call.stop(), call.database.backup_full_db()]
        if outcome == "no_backup":
            db.delete_income_by_ids.assert_not_called()
            db.delete_income_older_than.assert_not_called()
            assert "No income was deleted" in response.json()["detail"]
            dashboard._log.assert_called_once()
        else:
            delete.assert_called_once_with(*args)
            expected.append(getattr(call.database, method)(*args))
        if was_running:
            expected.append(call.start())
        assert events.mock_calls == expected
        if outcome == "success":
            assert response.json() == {"deleted": 2, "backup": "isolated-backup.db"}


@pytest.mark.parametrize("source,targets", [
    ("local", ["remote"]), ("remote", ["other", "local"]), ("remote", ["other"]),
])
@pytest.mark.parametrize("fail", [False, True])
def test_sync_snapshot_and_restore_exclude_each_other_only_for_local_jobs(
    client, tmp_path, monkeypatch, source, targets, fail,
):
    """Local sync holds DB SH for the whole job; neither sync mode owns a master lease."""
    job = {"id": "isolated-sync", "source": source, "targets": targets, "manual": True}
    operation = object()
    touches_local = source == "local" or "local" in targets
    result = {"ok": True, "synced": 3}
    restore = Mock()
    monkeypatch.setattr(sqlite_backup, "restore_sqlite_backup", restore)

    async def exercise():
        """Pause a mock job across an await while testing real restore admission."""
        entered, release = asyncio.Event(), asyncio.Event()

        async def run(job_id, **kwargs):
            """Replace all sync job I/O while preserving the snapshot wrapper lifecycle."""
            assert job_id == job["id"]
            assert kwargs == {"manual": True, "operation": operation, "job_override": job, "persist_state": False}
            assert kwargs["job_override"] is not job
            entered.set()
            await release.wait()
            if fail:
                raise RuntimeError("isolated sync failure")
            return result

        runner = AsyncMock(side_effect=run)
        monkeypatch.setattr(db_tools, "_run_sync_job", runner)
        task = asyncio.create_task(db_tools.run_sync_job_snapshot(job, operation))
        try:
            await asyncio.wait_for(entered.wait(), timeout=10)
            with acquire_master_update_lock(tmp_path), acquire_database_lock(tmp_path):
                pass
            response = client.post("/api/dashboard/income/restore", json={"path": "unused.db"})
            assert response.status_code == (409 if touches_local else 200)
            if touches_local:
                restore.assert_not_called()
            else:
                restore.assert_called_once()
        finally:
            release.set()
            if fail:
                with pytest.raises(RuntimeError, match="isolated sync failure"):
                    await asyncio.wait_for(task, timeout=10)
            else:
                assert await asyncio.wait_for(task, timeout=10) == result
        runner.assert_awaited_once()

    asyncio.run(exercise())
    with acquire_master_update_lock(tmp_path):
        pass
    with acquire_database_lock(tmp_path, exclusive=True):
        pass
    runner = AsyncMock(return_value=result)
    monkeypatch.setattr(db_tools, "_run_sync_job", runner)

    def restore_first(*_args):
        """Reverse ownership: local sync must fail before invoking any job callback."""
        assert dashboard._income_restore_active.is_set()
        if touches_local:
            with pytest.raises(DatabaseBusyError):
                asyncio.run(db_tools.run_sync_job_snapshot(job, operation))
            runner.assert_not_called()
        else:
            assert asyncio.run(db_tools.run_sync_job_snapshot(job, operation)) == result
            runner.assert_awaited_once()

    monkeypatch.setattr(sqlite_backup, "restore_sqlite_backup", restore_first)
    assert client.post("/api/dashboard/income/restore", json={"path": "unused.db"}).status_code == 200
    assert asyncio.run(db_tools.run_sync_job_snapshot(job, operation)) == result
    assert runner.await_count == (1 if touches_local else 2)
    with acquire_master_update_lock(tmp_path):
        pass
    with acquire_database_lock(tmp_path, exclusive=True):
        pass


@pytest.mark.parametrize("mode", ["replace", "add_missing"])
def test_local_user_copy_conflict_precedes_delete_and_connections(tmp_path, monkeypatch, mode):
    """A live-target copy denied by restore must not delete rows or open either bundle."""
    source = {name: tmp_path / "source" / name for name in db_tools.DB_FILE_NAMES}
    target = {name: tmp_path / "data" / name for name in db_tools.DB_FILE_NAMES}
    callbacks = {}
    for name in ("delete_user_rows", "_connect_bundle", "_copy_spec_rows", "_close_bundle"):
        callbacks[name] = Mock(side_effect=AssertionError(f"Unexpected {name}"))
        monkeypatch.setattr(db_tools, name, callbacks[name])
    with acquire_database_lock(tmp_path, exclusive=True):
        with pytest.raises(DatabaseBusyError):
            db_tools.copy_user_rows(source, target, ["alice"], mode)
    for callback in callbacks.values():
        callback.assert_not_called()
    with acquire_database_lock(tmp_path, exclusive=True):
        pass


@pytest.mark.parametrize("live_target", [False, True])
@pytest.mark.parametrize("failure", [None, "delete", "connect_target", "copy", "close_target"])
def test_user_copy_lease_spans_replace_all_tables_and_connection_cleanup(
    tmp_path, monkeypatch, live_target, failure,
):
    """ExitStack keeps DB SH through cleanup without blocking master/restart admission."""
    source = {name: tmp_path / "source" / name for name in db_tools.DB_FILE_NAMES}
    target = {name: tmp_path / ("data" if live_target else "staged") / name for name in db_tools.DB_FILE_NAMES}
    source_conns = {name: object() for name in db_tools.DB_FILE_NAMES}
    target_conns = {name: object() for name in db_tools.DB_FILE_NAMES}
    phases = []
    copied = []

    def phase(name):
        """Verify live ownership at every callback, including both connection closes."""
        phases.append(name)
        if live_target:
            with acquire_master_update_lock(tmp_path), acquire_database_lock(tmp_path):
                pass
            with pytest.raises(DatabaseBusyError):
                acquire_database_lock(tmp_path, exclusive=True)
        if failure == name:
            raise RuntimeError(f"isolated {name} failure")

    def delete(paths, users, operation=None):
        """Stand in for replace deletion without modifying any SQLite rows."""
        assert paths == target and users == ["alice"]
        phase("delete")

    def connect(paths):
        """Return distinguishable bundle handles without opening any database files."""
        phase("connect_source" if paths == source else "connect_target")
        return source_conns if paths == source else target_conns

    def copy(src, dst, spec, users, mode):
        """Exercise every table callback and optionally fail only at the last table."""
        assert src is source_conns[spec.db_name] and dst is target_conns[spec.db_name]
        assert users == ["alice"] and mode == "replace"
        copied.append(spec)
        phase("copy" if spec is db_tools.TABLE_SPECS[-1] else spec.table)
        return {"source": 1, "inserted": 1, "skipped": 0}

    def close(conns):
        """Test reverse-order cleanup, including continuation after a close failure."""
        assert conns is source_conns or conns is target_conns
        phase("close_source" if conns is source_conns else "close_target")

    monkeypatch.setattr(db_tools, "delete_user_rows", delete)
    monkeypatch.setattr(db_tools, "_connect_bundle", connect)
    monkeypatch.setattr(db_tools, "_copy_spec_rows", copy)
    monkeypatch.setattr(db_tools, "_close_bundle", close)
    # A staging-only target must still work while the live database is reserved.
    with nullcontext() if live_target else acquire_database_lock(tmp_path, exclusive=True):
        if failure is not None:
            with pytest.raises(RuntimeError, match=f"isolated {failure} failure"):
                db_tools.copy_user_rows(source, target, ["alice"], "replace")
        else:
            result = db_tools.copy_user_rows(source, target, ["alice"], "replace")
            assert result["inserted"] == result["source_total"] == len(db_tools.TABLE_SPECS)
            assert result["skipped"] == 0
            assert len(result["tables"]) == len(db_tools.TABLE_SPECS)
    if failure == "delete":
        assert phases == ["delete"]
    elif failure == "connect_target":
        assert phases == ["delete", "connect_source", "connect_target", "close_source"]
    else:
        assert phases[:3] == ["delete", "connect_source", "connect_target"]
        assert copied == list(db_tools.TABLE_SPECS)
        assert phases[-2:] == ["close_target", "close_source"]
    with acquire_master_update_lock(tmp_path):
        pass
    with acquire_database_lock(tmp_path, exclusive=True):
        pass


@pytest.mark.parametrize("entrypoint", ["route", "database"])
def test_shared_master_lease_allows_database_restore(client, database, tmp_path, entrypoint):
    """Unrelated master-runtime readers may coexist with a DB-exclusive restore."""
    source = database.backup_full_db()
    assert source is not None
    conn = database._connect()
    conn.execute("UPDATE balances SET balance=999")
    conn.commit()
    with acquire_master_runtime_lock(tmp_path):
        if entrypoint == "route":
            response = client.post("/api/dashboard/income/restore", json={"path": source})
            assert response.status_code == 200
            assert response.json() == {"ok": True}
        else:
            assert database.restore_db_from(source) is True
    assert conn.execute("SELECT balance FROM balances").fetchall() == [(100,)]
    with acquire_database_lock(tmp_path, exclusive=True):
        pass


@pytest.mark.parametrize("lease_factory,blocked", [
    (acquire_master_update_lock, False), (acquire_database_lock, False),
    (partial(acquire_database_lock, exclusive=True), True),
])
def test_history_and_backup_defer_only_for_database_exclusive_ownership(
    database, tmp_path, monkeypatch, lease_factory, blocked,
):
    """DB EX defers scans without metadata changes; master EX and DB SH allow both readers."""
    user = SimpleNamespace(name="alice", exchange="bybit")
    scan_ts = 100_000_000
    database.set_last_scan_ts(user.name, user.exchange, scan_ts)
    exchange = Mock(fetch_history=Mock(return_value=[{
        "symbol": "BTCUSDT", "timestamp": scan_ts + 1, "income": 2.5, "uniqueid": "lease-scan",
    }]))
    factory = Mock(return_value=exchange)
    fetch = Mock(wraps=database.fetch_history)
    backup = Mock(wraps=database_mod.backup_sqlite_database)
    monkeypatch.setattr(database_mod, "Exchange", factory)
    monkeypatch.setattr(database, "fetch_history", fetch)
    monkeypatch.setattr(database_mod, "backup_sqlite_database", backup)
    with lease_factory(tmp_path):
        database.update_history(user)
        source = database.backup_full_db()
        if blocked:
            fetch.assert_not_called()
            factory.assert_not_called()
            backup.assert_not_called()
            assert source is None
            assert database.get_last_scan_ts(user.name, user.exchange) == scan_ts
            assert database._connect().execute("SELECT uniqueid FROM history").fetchall() == [("income-1",)]
        else:
            assert source is not None
            backup.assert_called_once()
    if blocked:
        database.update_history(user)
        assert database.backup_full_db() is not None
        backup.assert_called_once()
    fetch.assert_called_once_with(user)
    factory.assert_called_once_with(user.exchange, user)
    exchange.fetch_history.assert_called_once_with(scan_ts - database._HISTORY_SCAN_LOOKBACK_MS)
    exchange.close.assert_called_once()
    assert database.get_last_scan_ts(user.name, user.exchange) > scan_ts
    assert database._connect().execute("SELECT income FROM history WHERE uniqueid='lease-scan'").fetchone() == (2.5,)
    with acquire_master_update_lock(tmp_path), acquire_database_lock(tmp_path, exclusive=True):
        pass


def test_detached_local_sync_allows_restart_admission_and_keeps_worker_alive(tmp_path, monkeypatch):
    """A detached DB reader must not block the restart reservation or register an API blocker."""
    job = {"id": "detached-sync", "source": "local", "targets": ["remote"]}

    async def exercise():
        """Hold the worker coroutine while exercising the real restart-admission checks."""
        entered, release = asyncio.Event(), asyncio.Event()

        async def run(*_args, **_kwargs):
            """Model independent worker lifetime without scheduling a job or touching a host."""
            entered.set()
            await release.wait()
            return {"ok": True}

        runner = AsyncMock(side_effect=run)
        monkeypatch.setattr(db_tools, "_run_sync_job", runner)
        task = asyncio.create_task(db_tools.run_sync_job_snapshot(job))
        try:
            await asyncio.wait_for(entered.wait(), timeout=10)
            with pytest.raises(DatabaseBusyError):
                acquire_database_lock(tmp_path, exclusive=True)
            assert task not in db_tools._background_tasks
            assert db_tools.restart_block_reason() == ""
            with acquire_master_update_lock(tmp_path):
                assert await PBApiServer._restart_block_state() == (False, "")
                await asyncio.sleep(0)
                assert not task.done()
            await asyncio.sleep(0)
            assert not task.done()
        finally:
            release.set()
            assert await asyncio.wait_for(task, timeout=10) == {"ok": True}
        assert not task.cancelled()
        runner.assert_awaited_once()

    asyncio.run(exercise())
    with acquire_master_update_lock(tmp_path), acquire_database_lock(tmp_path, exclusive=True):
        pass
