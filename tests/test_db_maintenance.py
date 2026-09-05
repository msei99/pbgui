"""Target-owned maintenance tests using only temporary SQLite files and process mocks."""

import ast
import asyncio
import json
import os
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
from contextlib import ExitStack, closing
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from fastapi import HTTPException

import db_maintenance as maintenance
from database_lock import DatabaseBusyError, acquire_database_lock, recovery_record


class Control:
    """Mock service control that asserts actual DB admission at each lifecycle step."""

    def __init__(self, root):
        """Keep observable process ownership without starting any process."""
        self.root = root
        self.events = []

    def inspect(self):
        """Inspect is already protected by EX admission."""
        self.events.append("inspect")
        with pytest.raises(DatabaseBusyError):
            acquire_database_lock(self.root)
        return "systemd"

    def stop(self, marker):
        """A durable journal precedes any service stop side effect."""
        assert marker == "systemd"
        assert recovery_record(self.root)
        with pytest.raises(DatabaseBusyError):
            acquire_database_lock(self.root)
        self.events.append("stop")

    def start(self, marker):
        """Writers may start only after consistency is durably recorded."""
        assert marker == "systemd"
        assert recovery_record(self.root)["phase"] == "consistent"
        with acquire_database_lock(self.root):
            pass
        with pytest.raises(DatabaseBusyError):
            acquire_database_lock(self.root, exclusive=True)
        self.events.append("start")


@pytest.fixture
def tools(tmp_path, monkeypatch):
    """Compile real schemas and SQL/API helpers without API or exchange startup."""
    import shutil
    import shlex
    import tempfile
    import traceback

    names = {"_ensure_schema", "_clean_users", "_local_db_path", "_data_dir", "_target_db_paths_local",
             "_connect_bundle", "_close_bundle", "_table_exists", "_table_columns", "_placeholders",
             "_row_exists", "_copy_spec_rows", "copy_user_rows", "delete_user_rows", "_maintain_target",
             "_install_db_bundle", "restart_block_reason", "_assert_maintenance_available", "_target_id",
             "cleanup_run", "copy_users_run", "restore_backups_run", "copy_database_run", "recover_maintenance",
             "_validate_backup_name", "_run_remote_python", "_upload_source_snapshots", "_remove_remote_snapshots"}
    constants = {"MAIN_DB_NAME", "TRADES_DB_NAME", "DB_FILE_NAMES", "MAIN_SCHEMA", "TRADES_SCHEMA",
                 "MAIN_TABLES", "TRADES_TABLES", "TABLE_SPECS", "_REMOTE_SYNC_APPLY_SCRIPT",
                 "_REMOTE_DELETE_SCRIPT", "_REMOTE_COPY_SCRIPT"}
    path = Path(__file__).resolve().parents[1] / "api" / "db_tools.py"
    tree = ast.parse(path.read_text())
    body = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) and node.name in names | {"TableSpec"}:
            body.append(node)
        elif isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id in constants for t in node.targets):
            body.append(node)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.target.id in constants:
            body.append(node)
    module = ModuleType("api.db_tools")
    module.__dict__.update(asyncio=asyncio, threading=threading, Path=Path, sqlite3=sqlite3, uuid=uuid,
                           json=json, dataclass=dataclass, ExitStack=ExitStack, HTTPException=HTTPException,
                           PBGDIR=tmp_path, _log=Mock(), SERVICE="DbTools", _operations={},
                           shutil=shutil, shlex=shlex, tempfile=tempfile, traceback=traceback,
                           router=SimpleNamespace(post=lambda *args, **kwargs: lambda fn: fn),
                           Depends=lambda fn: None, require_auth=lambda: None,
                           _assert_known_target=AsyncMock(), _start_operation=Mock())
    monkeypatch.setitem(sys.modules, "api.db_tools", module)
    exec(compile(ast.Module(body=ast.parse("from __future__ import annotations").body + body,
                            type_ignores=[]), str(path), "exec"), module.__dict__)
    control = Control(tmp_path)
    monkeypatch.setattr(maintenance, "PBDataControl", lambda root: control)
    module.control = control
    return module


def _bundle(tools, directory, value):
    """Create valid production schemas with distinguishable generation markers."""
    result = {name: directory / name for name in maintenance.DB_NAMES}
    for name, path in result.items():
        tools._ensure_schema(path, name)
    with closing(sqlite3.connect(result["pbgui.db"])) as conn:
        conn.execute("INSERT INTO history VALUES (1, 'BTCUSDT', 1, ?, 'same', 'alice')", (value,))
        conn.commit()
    with closing(sqlite3.connect(result["pbgui_trades.db"])) as conn:
        conn.execute("INSERT INTO executions(exchange,symbol,timestamp,trade_id,user,price) VALUES ('mock','BTCUSDT',1,'same','alice',?)", (value,))
        conn.commit()
    return result


def _values(paths):
    """Read temporary generation markers without retaining cached connections."""
    result = []
    for name, sql in (("pbgui.db", "SELECT income FROM history"), ("pbgui_trades.db", "SELECT price FROM executions")):
        with closing(sqlite3.connect(paths[name])) as conn:
            result.append(conn.execute(sql).fetchone())
    return result


def _request(staged, **kwargs):
    """Create one uniquely owned transaction request."""
    return {"id": uuid.uuid4().hex, "kind": "install", "staged": staged, **kwargs}


def test_install_preserves_wal_cached_connections(tmp_path, tools):
    """Both DBs change generation without replacing an inode or deleting live WAL."""
    live = _bundle(tools, tmp_path / "data", 1)
    staged = _bundle(tools, tmp_path / "source", 2)
    inode = live["pbgui.db"].stat().st_ino
    with closing(sqlite3.connect(live["pbgui.db"])) as cached:
        cached.execute("PRAGMA journal_mode=WAL")
        cached.execute("UPDATE history SET income=3")
        cached.commit()
        result = maintenance.run(tmp_path, _request(staged))
        assert cached.execute("SELECT income FROM history").fetchone() == (2,)
        assert cached.execute("PRAGMA journal_mode").fetchone() == ("wal",)
    assert _values(live) == [(2,), (2,)]
    assert live["pbgui.db"].stat().st_ino == inode
    assert len(result["backups"]) == 2
    assert tools.control.events == ["inspect", "stop", "start"]
    assert recovery_record(tmp_path) is None


@pytest.mark.parametrize("fail_name", maintenance.DB_NAMES)
@pytest.mark.parametrize("reverse", [False, True])
def test_install_rollback_maps_each_database(tmp_path, tools, monkeypatch, fail_name, reverse):
    """First/second restore failures restore the correct generation in either selection order."""
    live = _bundle(tools, tmp_path / "data", 1)
    staged = _bundle(tools, tmp_path / "source", 2)
    if reverse:
        staged = dict(reversed(list(staged.items())))
    original = maintenance.restore_sqlite_backup

    def restore(source, destination, root, **kwargs):
        """Fail only publication, not rollback, for the selected database."""
        if source.name == f"prepared-{fail_name}":
            raise OSError("injected install failure")
        return original(source, destination, root, **kwargs)

    monkeypatch.setattr(maintenance, "restore_sqlite_backup", restore)
    with pytest.raises(OSError, match="injected"):
        maintenance.run(tmp_path, _request(staged))
    assert _values(live) == [(1,), (1,)]
    assert recovery_record(tmp_path) is None
    assert tools.control.events[-1] == "start"


def test_failed_rollback_blocks_writers_until_explicit_recovery(tmp_path, tools, monkeypatch):
    """Retain undo data and the restart blocker rather than resuming a mixed bundle."""
    live = _bundle(tools, tmp_path / "data", 1)
    staged = _bundle(tools, tmp_path / "source", 2)
    original = maintenance.restore_sqlite_backup

    def fail(source, destination, root, **kwargs):
        """Fail second install and the first database rollback."""
        if source.name in {"prepared-pbgui_trades.db", "original-pbgui.db"}:
            raise OSError("isolated disk failure")
        return original(source, destination, root, **kwargs)

    monkeypatch.setattr(maintenance, "restore_sqlite_backup", fail)
    with pytest.raises(DatabaseBusyError, match="rollback incomplete"):
        maintenance.run(tmp_path, _request(staged))
    assert _values(live) == [(2,), (1,)]
    assert "start" not in tools.control.events
    assert "recovery pending" in tools.restart_block_reason()
    with pytest.raises(DatabaseBusyError):
        acquire_database_lock(tmp_path)
    monkeypatch.setattr(maintenance, "restore_sqlite_backup", original)
    assert maintenance.recover(tmp_path)["recovered"] is True
    assert _values(live) == [(1,), (1,)]
    assert recovery_record(tmp_path) is None
    assert maintenance.recover(tmp_path) == {"ok": True, "recovered": False}


def test_cancel_after_first_install_rolls_back_absent_second_db(tmp_path, tools, monkeypatch):
    """Cancellation undoes a newly created optional DB and never confuses backup paths."""
    live = _bundle(tools, tmp_path / "data", 1)
    live["pbgui_trades.db"].unlink()
    staged = _bundle(tools, tmp_path / "source", 2)
    staged = dict(reversed(list(staged.items())))
    cancel = threading.Event()
    original = maintenance.restore_sqlite_backup

    def restore(source, destination, root, **kwargs):
        """Cancel once SQLite has committed the previously absent trades DB."""
        original(source, destination, root, **kwargs)
        if source.name.startswith("prepared-"):
            cancel.set()

    monkeypatch.setattr(maintenance, "restore_sqlite_backup", restore)
    with pytest.raises(maintenance.MaintenanceCancelled):
        maintenance.run(tmp_path, _request(staged), cancel=cancel)
    assert not live["pbgui_trades.db"].exists()
    assert recovery_record(tmp_path) is None


@pytest.mark.parametrize("kind", ["cleanup", "copy"])
def test_row_maintenance_excludes_shared_writer_before_stop(tmp_path, tools, kind):
    """Admission failure has no service, backup, or row-deletion side effects."""
    live = _bundle(tools, tmp_path / "data", 1)
    source = _bundle(tools, tmp_path / "source", 2)
    with acquire_database_lock(tmp_path):
        with pytest.raises(DatabaseBusyError):
            maintenance.run(tmp_path, _request(source, kind=kind, users=["alice"], mode="replace"))
    assert _values(live) == [(1,), (1,)]
    assert tools.control.events == []


@pytest.mark.parametrize("kind", ["cleanup", "copy"])
def test_rows_backup_and_all_commits_share_one_owner(tmp_path, tools, kind):
    """EX admission covers backup, every table, and close without nested SH acquisition."""
    live = _bundle(tools, tmp_path / "data", 1)
    source = _bundle(tools, tmp_path / "source", 2)

    def phase(*args):
        """Attempt a competing cooperative writer after each table commit."""
        with pytest.raises(DatabaseBusyError):
            acquire_database_lock(tmp_path)

    progress = SimpleNamespace(set_current=phase, advance=phase)
    result = maintenance.run(tmp_path, _request(source if kind == "copy" else {}, kind=kind,
                                               users=["alice"], mode="replace"), progress=progress)
    assert result["ok"]
    assert _values(live) == ([(2,), (2,)] if kind == "copy" else [None, None])
    assert len(result["backups"]) == 2


def test_async_cancellation_drains_worker_and_rollback(tmp_path, tools, monkeypatch):
    """Repeated API cancellation cannot outlive its worker or release EX prematurely."""
    live = _bundle(tools, tmp_path / "data", 1)
    source = _bundle(tools, tmp_path / "source", 2)
    entered, release = threading.Event(), threading.Event()
    original = maintenance.restore_sqlite_backup

    def restore(src, dst, root, **kwargs):
        """Pause only after the first real SQLite install commit."""
        original(src, dst, root, **kwargs)
        if src.name == "prepared-pbgui.db":
            entered.set()
            assert release.wait(5)

    monkeypatch.setattr(maintenance, "restore_sqlite_backup", restore)

    async def exercise():
        """Drive the real API-owned wrapper without starting API lifespan."""
        task = asyncio.create_task(tools._install_db_bundle("local", source, "test"))
        try:
            assert await asyncio.to_thread(entered.wait, 5)
            for _ in range(2):
                task.cancel()
                await asyncio.sleep(0.01)
                assert not task.done()
                with pytest.raises(DatabaseBusyError):
                    acquire_database_lock(tmp_path)
        finally:
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await task

    asyncio.run(exercise())
    assert _values(live) == [(1,), (1,)]
    assert recovery_record(tmp_path) is None


def test_crash_recovery_uses_durable_intent(tmp_path, tools):
    """A terminated target worker leaves a recoverable journal, not just Python undo state."""
    live = _bundle(tools, tmp_path / "data", 1)
    staged = _bundle(tools, tmp_path / "source", 2)
    request = _request({name: str(path) for name, path in staged.items()})
    script = """
import json, os, sys
from pathlib import Path
import db_maintenance as m
class Control:
    def inspect(self): return 'systemd'
    def stop(self, marker): pass
    def start(self, marker): raise AssertionError('restart after crash')
original = m.restore_sqlite_backup
def crash(source, target, root):
    original(source, target, root)
    os._exit(73)
m.restore_sqlite_backup = crash
m.run(Path(sys.argv[1]), json.loads(sys.argv[2]), control=Control())
"""
    result = subprocess.run([sys.executable, "-B", "-c", script, str(tmp_path), json.dumps(request)],
                            cwd=Path(__file__).resolve().parents[1], capture_output=True, timeout=10)
    assert result.returncode == 73, result.stderr
    assert _values(live) == [(2,), (1,)]
    with pytest.raises(DatabaseBusyError):
        acquire_database_lock(tmp_path)
    assert maintenance.recover(tmp_path)["recovered"]
    assert _values(live) == [(1,), (1,)]


def test_stop_failure_never_mutates_and_preserves_recovery(tmp_path, tools, monkeypatch):
    """Stop failure is not equivalent to an already stopped PBData process."""
    live = _bundle(tools, tmp_path / "data", 1)
    staged = _bundle(tools, tmp_path / "source", 2)
    monkeypatch.setattr(tools.control, "stop", Mock(side_effect=DatabaseBusyError("stop failed")))
    with pytest.raises(DatabaseBusyError, match="stop failed"):
        maintenance.run(tmp_path, _request(staged))
    assert _values(live) == [(1,), (1,)]
    assert recovery_record(tmp_path)["phase"] == "preparing"
    assert "start" not in tools.control.events


def test_rollback_restores_constraints_of_original_legacy_schema(tmp_path, tools, monkeypatch):
    """Compensation must undo new UNIQUE constraints as well as the installed rows."""
    live = _bundle(tools, tmp_path / "data", 1)
    staged = _bundle(tools, tmp_path / "source", 2)
    with closing(sqlite3.connect(live["pbgui.db"])) as conn:
        conn.execute("DROP TABLE balances")
        conn.execute("CREATE TABLE balances(id INTEGER PRIMARY KEY, timestamp INTEGER NOT NULL, balance REAL NOT NULL, user TEXT NOT NULL)")
        conn.commit()
    original = maintenance.restore_sqlite_backup

    def restore(source, destination, root, **kwargs):
        """Let the main DB gain a UNIQUE constraint, then fail the second install."""
        if source.name == "prepared-pbgui_trades.db":
            raise OSError("second database failed")
        original(source, destination, root, **kwargs)

    monkeypatch.setattr(maintenance, "restore_sqlite_backup", restore)
    with pytest.raises(OSError, match="second database"):
        maintenance.run(tmp_path, _request(staged))
    assert _values(live) == [(1,), (1,)]
    with closing(sqlite3.connect(live["pbgui.db"])) as conn:
        assert conn.execute("PRAGMA index_list(balances)").fetchall() == []
    assert recovery_record(tmp_path) is None


def test_missing_source_preparation_never_changes_live_bundle(tmp_path, tools):
    """Preparing every input first avoids any publication on an invalid second input."""
    live = _bundle(tools, tmp_path / "data", 1)
    staged = _bundle(tools, tmp_path / "source", 2)
    staged["pbgui_trades.db"].unlink()
    with pytest.raises(Exception, match="SQLite path"):
        maintenance.run(tmp_path, _request(staged))
    assert _values(live) == [(1,), (1,)]
    assert recovery_record(tmp_path) is None
    assert not list((tmp_path / "data" / "backup" / "db-tools").glob("recovery-*"))


@pytest.mark.parametrize("handler", ["cleanup_run", "copy_users_run", "restore_backups_run", "copy_database_run", "recover_maintenance"])
def test_routes_return_busy_before_queueing(tmp_path, tools, handler):
    """Known admission conflicts return HTTP 409, not a seemingly accepted operation."""
    payload = SimpleNamespace(target="local", source="other", users=["alice"], mode="all", cutoff_ms=None,
                              backups=["old-pbgui.db"])
    with acquire_database_lock(tmp_path):
        with pytest.raises(HTTPException) as busy:
            asyncio.run(getattr(tools, handler)(payload, session=object()))
    assert busy.value.status_code == 409
    tools._start_operation.assert_not_called()
    assert tools.control.events == []


def test_partial_bundle_open_closes_prior_connections(tmp_path, tools, monkeypatch):
    """A failed second open cannot leak the first connection past maintenance cleanup."""
    first = Mock()
    monkeypatch.setattr(tools.sqlite3, "connect", Mock(side_effect=[first, sqlite3.OperationalError("second open failed")]))
    with pytest.raises(sqlite3.OperationalError, match="second open"):
        tools._connect_bundle({"pbgui.db": tmp_path / "first", "pbgui_trades.db": tmp_path / "second"})
    first.close.assert_called_once()


def test_bundle_close_attempts_all_connections(tools):
    """One close failure must not strand the remaining bundle handles."""
    first, second = Mock(), Mock()
    first.close.side_effect = RuntimeError("close failed")
    with pytest.raises(RuntimeError, match="close failed"):
        tools._close_bundle({"first": first, "second": second})
    first.close.assert_called_once()
    second.close.assert_called_once()


def test_cancellation_after_confirmed_stop_resumes_unchanged_target(tmp_path, tools, monkeypatch):
    """Cancellation during admission must not strand a successfully stopped writer."""
    live = _bundle(tools, tmp_path / "data", 1)
    source = _bundle(tools, tmp_path / "source", 2)
    cancel = threading.Event()
    stop = tools.control.stop

    def stop_then_cancel(marker):
        """Simulate cancellation arriving while the verified stop was in progress."""
        stop(marker)
        cancel.set()

    monkeypatch.setattr(tools.control, "stop", stop_then_cancel)
    with pytest.raises(maintenance.MaintenanceCancelled):
        maintenance.run(tmp_path, _request(source), cancel=cancel)
    assert _values(live) == [(1,), (1,)]
    assert tools.control.events == ["inspect", "stop", "start"]
    assert recovery_record(tmp_path) is None


def test_sql_helpers_reject_wrong_or_released_owner(tmp_path, tools):
    """A private owner parameter is not a general-purpose bypass of live admission."""
    live = _bundle(tools, tmp_path / "data", 1)
    with maintenance.Maintenance(tmp_path, uuid.uuid4().hex) as owner:
        with pytest.raises(DatabaseBusyError, match="active, prepared"):
            tools.delete_user_rows(live, ["alice"], _maintenance=owner)
        owner.prepare({})
        owner.touch(maintenance.DB_NAMES)
        wrong = {name: tmp_path / "other" / name for name in maintenance.DB_NAMES}
        with pytest.raises(DatabaseBusyError, match="active, prepared"):
            tools.delete_user_rows(wrong, ["alice"], _maintenance=owner)
    with pytest.raises(DatabaseBusyError, match="active, prepared"):
        tools.delete_user_rows(live, ["alice"], _maintenance=owner)
    assert _values(live) == [(1,), (1,)]


@pytest.mark.parametrize("phase,call_number", [("backup", 1), ("restore", 6)])
def test_killed_inside_sqlite_helper_cleans_remnants_and_recovers(tmp_path, tools, phase, call_number):
    """Kill only a test child while a real helper still owns its staging directory."""
    live = _bundle(tools, tmp_path / "data", 1)
    staged = _bundle(tools, tmp_path / "source", 2)
    request = _request({name: str(path) for name, path in staged.items()})
    script = """
import json, sys, time
from pathlib import Path
import db_maintenance as m
import sqlite_backup as backup
class Control:
    def inspect(self): return 'systemd'
    def stop(self, marker): pass
    def start(self, marker): raise AssertionError('unexpected restart')
original = backup._copy_database
calls = 0
def pause_inside_helper(source, target, check):
    global calls
    calls += 1
    if calls == int(sys.argv[3]):
        (Path(sys.argv[1]) / 'helper-ready').touch()
        time.sleep(20)
    return original(source, target, check)
backup._copy_database = pause_inside_helper
m.run(Path(sys.argv[1]), json.loads(sys.argv[2]), control=Control())
"""
    process = subprocess.Popen([sys.executable, "-B", "-c", script, str(tmp_path), json.dumps(request), str(call_number)],
                               cwd=Path(__file__).resolve().parents[1], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    directory = tmp_path / "data" / "backup" / "db-tools" / f"recovery-{request['id']}"
    try:
        end = time.monotonic() + 5
        while not (tmp_path / "helper-ready").exists() and time.monotonic() < end:
            time.sleep(0.01)
        assert (tmp_path / "helper-ready").exists()
        remnants = list(directory.glob(f".sqlite-{phase}-*"))
        assert len(remnants) == 1 and (remnants[0] / "snapshot.db").exists()
        process.kill()
        process.communicate(timeout=5)
        assert process.returncode < 0
        with pytest.raises(DatabaseBusyError):
            acquire_database_lock(tmp_path)
        assert _values(live) == ([(1,), (1,)] if phase == "backup" else [(2,), (1,)])
        assert maintenance.recover(tmp_path)["recovered"]
        assert _values(live) == [(1,), (1,)]
        assert not directory.exists()
        assert recovery_record(tmp_path) is None
        assert not tools.restart_block_reason()
        with acquire_database_lock(tmp_path, exclusive=True):
            pass
    finally:
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=5)


@pytest.mark.parametrize("kind", ["directory_symlink", "file_symlink", "unexpected_file", "permissive_directory"])
def test_staging_cleanup_rejects_unowned_or_unexpected_contents(tmp_path, tools, kind):
    """Recognized prefixes never authorize recursive deletion or following a symlink."""
    _bundle(tools, tmp_path / "data", 1)
    outside = tmp_path / "outside"
    outside.mkdir()
    sentinel = outside / "snapshot.db"
    sentinel.write_bytes(b"unrelated data")
    with pytest.raises((DatabaseBusyError, OSError)):
        with maintenance.Maintenance(tmp_path, uuid.uuid4().hex) as owner:
            remnant = owner.directory / ".sqlite-restore-abcd1234"
            if kind == "directory_symlink":
                remnant.symlink_to(outside, target_is_directory=True)
            else:
                remnant.mkdir(mode=0o700)
                if kind == "file_symlink":
                    (remnant / "snapshot.db").symlink_to(sentinel)
                elif kind == "unexpected_file":
                    (remnant / "unrelated").write_bytes(b"keep")
                else:
                    remnant.chmod(0o755)
    assert sentinel.read_bytes() == b"unrelated data"
    assert recovery_record(tmp_path) is not None
    assert remnant.exists()


def test_staging_cleanup_is_limited_to_this_operations_sqlite_files(tmp_path, tools):
    """Known snapshot sidecars are removable, but another operation's staging is untouched."""
    with maintenance.Maintenance(tmp_path, uuid.uuid4().hex) as owner:
        sibling = owner.directory.parent / f"recovery-{uuid.uuid4().hex}"
        sibling.mkdir(mode=0o700)
        sentinel = sibling / "snapshot.db"
        sentinel.write_bytes(b"other operation")
        remnant = owner.directory / ".sqlite-backup-abcd1234"
        remnant.mkdir(mode=0o700)
        for suffix in ("", "-wal", "-shm", "-journal"):
            path = remnant / f"snapshot.db{suffix}"
            path.write_bytes(b"interrupted helper")
            path.chmod(0o600)
    assert not owner.directory.exists()
    assert sentinel.read_bytes() == b"other operation"
    assert recovery_record(tmp_path) is None
