"""Remote maintenance protocol tests with local SQLite targets and mocked SSH only."""

import asyncio
import json
import os
import shutil
import shlex
import signal
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

import db_maintenance as maintenance
from database_lock import DatabaseBusyError, acquire_database_lock, recovery_record
from test_db_maintenance import Control, _bundle, _request, _values, tools


@pytest.fixture
def remote(tmp_path, tools, monkeypatch):
    """Model the SSH boundary without executing a command or touching a real host."""
    root = tmp_path / "remote"
    root.mkdir()
    control = Control(root)
    calls = []

    async def upload(target, paths, operation=None):
        """Copy only test-created sources into the mock target's private upload folder."""
        directory = root / "data" / "tmp" / "db-tools" / uuid.uuid4().hex
        directory.mkdir(parents=True)
        result = {}
        for name, source in paths.items():
            shutil.copy2(source, directory / name)
            result[name] = str(directory / name)
        return result

    async def rpc(target, script, args, timeout=60, *, retry=True):
        """Execute the shared target core locally; service control and network stay mocked."""
        calls.append(script)
        if "remote_main" in script:
            assert timeout == 900
            assert retry is False
            try:
                return await asyncio.to_thread(maintenance.run, root, json.loads(args[0]), control=control)
            except Exception as exc:
                return {"maintenance_error": str(exc), "recovery_pending": recovery_record(root) is not None}
        if "request_cancel" in script:
            maintenance.request_cancel(root, args[0])
        if "clear_cancel" in script:
            maintenance.clear_cancel(root, args[0])
        return {}

    monkeypatch.setattr(tools, "_run_remote_python", rpc, raising=False)
    monkeypatch.setattr(tools, "_upload_source_snapshots", AsyncMock(side_effect=upload), raising=False)
    monkeypatch.setattr(tools, "_remove_remote_snapshots", AsyncMock(), raising=False)
    return SimpleNamespace(root=root, calls=calls, control=control, rpc=rpc)


@pytest.mark.parametrize("kind", ["install", "cleanup", "copy"])
def test_remote_owner_spans_backup_and_all_mutations(tmp_path, tools, remote, kind):
    """All target changes use one remote transaction, not a lease around each RPC."""
    live = _bundle(tools, remote.root / "data", 1)
    source = _bundle(tools, tmp_path / "source", 2)
    request = _request(source if kind != "cleanup" else {}, kind=kind, users=["alice"], mode="replace")
    result = asyncio.run(tools._maintain_target("mock-target", request))
    assert result["ok"]
    assert _values(live) == ([None, None] if kind == "cleanup" else [(2,), (2,)])
    assert remote.control.events == ["inspect", "stop", "start"]
    assert sum("remote_main" in script for script in remote.calls) == 1
    assert not list((tmp_path / "data" / "locks").glob("db-tools-remote-*.json"))


def test_remote_cancellation_rolls_back_and_drains(tmp_path, tools, remote, monkeypatch):
    """A cancelled API owner sends a scoped cancellation and waits for remote rollback."""
    live = _bundle(tools, remote.root / "data", 1)
    source = _bundle(tools, tmp_path / "source", 2)
    entered, release = threading.Event(), threading.Event()
    original = maintenance.restore_sqlite_backup

    def restore(src, dst, root, **kwargs):
        """Pause after first publication while the real remote-root lease stays owned."""
        original(src, dst, root, **kwargs)
        if src.name == "prepared-pbgui.db":
            entered.set()
            assert release.wait(5)

    async def rpc(target, script, args, timeout=60, *, retry=True):
        """Release the paused target only when cancellation has been recorded."""
        result = await remote.rpc(target, script, args, timeout, retry=retry)
        if "request_cancel" in script:
            release.set()
        return result

    monkeypatch.setattr(maintenance, "restore_sqlite_backup", restore)
    monkeypatch.setattr(tools, "_run_remote_python", rpc)

    async def exercise():
        """Check local admission and the target lease during an actual overlap window."""
        task = asyncio.create_task(tools._maintain_target("mock-target", _request(source)))
        try:
            assert await asyncio.to_thread(entered.wait, 5)
            with pytest.raises(DatabaseBusyError):
                acquire_database_lock(remote.root)
            with pytest.raises(HTTPException) as busy:
                await tools._maintain_target("mock-target", {"kind": "cleanup", "users": ["alice"]})
            assert busy.value.status_code == 409
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            release.set()
            if not task.done():
                await asyncio.gather(task, return_exceptions=True)

    asyncio.run(exercise())
    assert _values(live) == [(1,), (1,)]
    assert recovery_record(remote.root) is None
    assert not tools.restart_block_reason()
    tools._remove_remote_snapshots.assert_awaited_once()
    assert any("clear_cancel" in script for script in remote.calls)


def test_remote_lost_reply_keeps_receipt_until_reconciled(tmp_path, tools, remote, monkeypatch):
    """Even a committed remote result remains uncertain locally until explicit recovery."""
    live = _bundle(tools, remote.root / "data", 1)
    source = _bundle(tools, tmp_path / "source", 2)

    async def lost_reply(target, script, args, timeout=60, *, retry=True):
        """Lose only the completed mutation response, never contact a real SSH endpoint."""
        result = await remote.rpc(target, script, args, timeout, retry=retry)
        if "remote_main" in script:
            raise HTTPException(status_code=500, detail="mock lost SSH response")
        return result

    monkeypatch.setattr(tools, "_run_remote_python", lost_reply)
    with pytest.raises(HTTPException):
        asyncio.run(tools._maintain_target("mock-target", _request(source)))
    assert _values(live) == [(2,), (2,)]
    assert "Remote DB Tools outcome unresolved" in tools.restart_block_reason()
    tools._remove_remote_snapshots.assert_not_awaited()
    monkeypatch.setattr(tools, "_run_remote_python", remote.rpc)
    assert asyncio.run(tools._maintain_target("mock-target", {"kind": "recover"}))["ok"]
    assert not tools.restart_block_reason()
    tools._remove_remote_snapshots.assert_awaited_once()


def test_remote_old_checkout_fails_before_upload_or_stop(tmp_path, tools, remote, monkeypatch):
    """An unavailable recovery protocol is a useful 409, not an unsafe fallback."""
    source = _bundle(tools, tmp_path / "source", 2)
    monkeypatch.setattr(tools, "_run_remote_python", AsyncMock(side_effect=HTTPException(
        status_code=500, detail="Update and restart remote PBGui/PBData; recovery guards are missing",
    )))
    with pytest.raises(HTTPException) as error:
        asyncio.run(tools._maintain_target("mock-target", _request(source)))
    assert error.value.status_code == 409 and "Update and restart" in error.value.detail
    tools._upload_source_snapshots.assert_not_awaited()
    assert remote.control.events == []


def test_failed_recovery_probe_does_not_clean_uncertain_sources(tmp_path, tools, remote, monkeypatch):
    """A failed retry must retain the previous receipt and its uploaded recovery sources."""
    from hashlib import sha256
    from secure_files import atomic_write_private_text

    path = tmp_path / "data" / "locks" / f"db-tools-remote-{sha256(b'mock-target').hexdigest()}.json"
    original = json.dumps({"target": "mock-target", "id": uuid.uuid4().hex,
                           "staged": {"pbgui.db": "/mock/owned/source"}})
    atomic_write_private_text(path, original)
    monkeypatch.setattr(tools, "_run_remote_python", AsyncMock(side_effect=HTTPException(status_code=500, detail="unreachable")))
    with pytest.raises(HTTPException):
        asyncio.run(tools._maintain_target("mock-target", {"kind": "recover"}))
    assert path.read_text() == original
    tools._remove_remote_snapshots.assert_not_awaited()


def test_actual_remote_entrypoint_rolls_back_on_sigterm(tmp_path, tools):
    """Execute the real target entrypoint locally and signal only the test-owned child."""
    live = _bundle(tools, tmp_path / "data", 1)
    source = _bundle(tools, tmp_path / "data" / "tmp" / "db-tools" / uuid.uuid4().hex, 2)
    request = _request({name: str(path) for name, path in source.items()})
    script = """
import json, sys, time
from pathlib import Path
import db_maintenance as m
class Control:
    def inspect(self): return 'none'
    def stop(self, marker): pass
    def start(self, marker): pass
m.PBDataControl = lambda root: Control()
m.check_remote_capability = lambda: None
original = m.restore_sqlite_backup
def pause(source, destination, root, **kwargs):
    original(source, destination, root, **kwargs)
    if source.name == 'prepared-pbgui.db':
        Path('ready').touch()
        end = time.monotonic() + 5
        while not Path('release').exists() and time.monotonic() < end:
            time.sleep(0.01)
m.restore_sqlite_backup = pause
try:
    m.remote_main(json.loads(sys.argv[1]))
except m.MaintenanceCancelled:
    print('cancelled-and-rolled-back')
"""
    env = {**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1]), "PYTHONDONTWRITEBYTECODE": "1"}
    process = subprocess.Popen([sys.executable, "-B", "-c", script, json.dumps(request)], cwd=tmp_path,
                               env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        end = time.monotonic() + 5
        while not (tmp_path / "ready").exists() and time.monotonic() < end:
            time.sleep(0.01)
        assert (tmp_path / "ready").exists()
        with pytest.raises(DatabaseBusyError):
            acquire_database_lock(tmp_path)
        process.send_signal(signal.SIGTERM)
        (tmp_path / "release").touch()
        stdout, stderr = process.communicate(timeout=10)
        assert process.returncode == 0, stderr
        assert b"cancelled-and-rolled-back" in stdout
        assert _values(live) == [(1,), (1,)]
        assert recovery_record(tmp_path) is None
    finally:
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=5)


def test_capability_check_rejects_missing_protocol(monkeypatch):
    """An older module fails before process inspection or filesystem operations."""
    monkeypatch.setitem(sys.modules, "Database", ModuleType("Database"))
    with pytest.raises(DatabaseBusyError, match="writer guards are missing"):
        maintenance.check_remote_capability()


def test_capability_check_rejects_old_running_api(tmp_path, monkeypatch):
    """Updated files alone do not prove a running API loaded the writer guards."""
    import psutil

    for name in ("Database", "PBData"):
        module = ModuleType(name)
        module.DB_MAINTENANCE_PROTOCOL = maintenance.PROTOCOL
        monkeypatch.setitem(sys.modules, name, module)
    for name in ("Database.py", "database_lock.py", "db_maintenance.py"):
        (tmp_path / name).write_text("temporary protocol fixture")
    process = SimpleNamespace(pid=os.getpid() + 1, info={"uids": SimpleNamespace(real=os.getuid())},
                              cwd=lambda: str(tmp_path / "other-cwd"),
                              cmdline=lambda: ["python", str(tmp_path / "PBApiServer.py")], create_time=lambda: 0)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(psutil, "process_iter", lambda attributes: iter([process]))
    with pytest.raises(DatabaseBusyError, match="Restart the updated remote PBGui API"):
        maintenance.check_remote_capability()


def test_second_upload_failure_cleans_only_owned_directory(tmp_path, tools, monkeypatch):
    """Preparation failure never invokes the remote transaction and cleans partial uploads."""
    source = _bundle(tools, tmp_path / "source", 2)
    calls = []

    async def run(target, command, **kwargs):
        """Capture filesystem command construction without running any command."""
        calls.append(shlex.split(command))
        return SimpleNamespace(returncode=0)

    pool = SimpleNamespace(run=run, push_file=AsyncMock(side_effect=[True, False]))
    monkeypatch.setattr(tools, "_pool", lambda: pool, raising=False)
    monkeypatch.setattr(tools, "_remote_path", lambda target, *parts: str(tmp_path / "remote" / Path(*parts)), raising=False)
    with pytest.raises(HTTPException, match="Failed to upload"):
        asyncio.run(tools._upload_source_snapshots("mock-target", source))
    assert calls[0][:3] == ["mkdir", "-p", "--"]
    assert calls[1] == ["rm", "-rf", "--", calls[0][3]]
    assert len(calls) == 2 and pool.push_file.await_count == 2
    assert tools.control.events == []


def test_sql_write_dispatcher_takes_target_shared_lease(tmp_path, tools, monkeypatch):
    """Execute only the captured Python payload locally to verify real target-side locking."""
    live = _bundle(tools, tmp_path / "data", 1)
    commands = []
    env = {**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1]), "PYTHONDONTWRITEBYTECODE": "1"}

    async def run(target, command, **kwargs):
        """Never execute the SSH/shell wrapper; use the explicitly approved test interpreter."""
        commands.append(command)
        argv = shlex.split(command)
        offset = argv.index("-c")
        return subprocess.run([sys.executable, "-B", *argv[offset:]], cwd=tmp_path,
                              env=env, text=True, capture_output=True, timeout=5)

    monkeypatch.setattr(tools, "_pool", lambda: SimpleNamespace(run=run), raising=False)
    monkeypatch.setattr(tools, "_remote_pbgui_dir", lambda target: str(tmp_path), raising=False)
    args = [str(live["pbgui.db"]), "history", "user", "timestamp", "", '["alice"]']
    with acquire_database_lock(tmp_path, exclusive=True):
        with pytest.raises(HTTPException) as busy:
            asyncio.run(tools._run_remote_python("mock-target", tools._REMOTE_DELETE_SCRIPT, args))
        assert "DatabaseBusyError" in busy.value.detail
    assert _values(live) == [(1,), (1,)]
    assert asyncio.run(tools._run_remote_python("mock-target", tools._REMOTE_DELETE_SCRIPT, args))["deleted"] == 1
    assert "../venv_pbgui/bin/python" in commands[0]
