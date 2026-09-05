"""Single-attempt SSH transport contracts with fake connections and local temporary DBs."""

import asyncio
import json
import shlex
import shutil
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import asyncssh
import pytest
from fastapi import HTTPException

import db_maintenance as maintenance
from master import async_pool
from test_db_maintenance import Control, _bundle, _request, _values, tools


@pytest.fixture
def pool(monkeypatch):
    """Never connect or inspect inventory; only the production command methods run."""
    instance = async_pool.AsyncSSHPool()
    instance.disconnect = AsyncMock()
    monkeypatch.setattr(async_pool, "_log", Mock())
    monkeypatch.setattr(async_pool, "SFTP_RETRY_DELAY", 0)
    return instance


def _process(wait):
    """Model both collected output pipes and channel close ownership."""
    return SimpleNamespace(wait=wait, close=Mock(), channel=SimpleNamespace(abort=Mock()), wait_closed=AsyncMock())


def test_default_read_command_still_retries_transient_errors(pool):
    """The new single-attempt option does not change existing callers' default."""
    result = SimpleNamespace(returncode=0, stdout="ok", stderr="")
    connection = SimpleNamespace(run=AsyncMock(side_effect=[ConnectionError("lost read reply"), result]))
    pool._ensure_live_connection = AsyncMock(return_value=SimpleNamespace(conn=connection))
    assert asyncio.run(pool.run("mock", "read-only status")) is result
    assert connection.run.await_count == 2
    pool.disconnect.assert_awaited_once_with("mock")


@pytest.mark.parametrize("outcome", ["success", "network_error", "timeout", "cancel", "process_error"])
def test_single_attempt_owns_and_drains_process(pool, outcome):
    """One dispatch owns stdout/stderr and close, even under repeated cancellation."""
    async def exercise():
        """Use explicit events rather than a real SSH process or timing-dependent output."""
        entered, closing, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
        result = SimpleNamespace(returncode=0, stdout="output", stderr="diagnostic")
        command = "PRIVATE-COMMAND-BODY"
        error = asyncssh.ProcessError({}, command, None, 7, None, 7, "output", "error")

        async def wait(check=False):
            """Simulate a result, a lost result, or an owned wait cancelled by the caller."""
            entered.set()
            if outcome == "network_error":
                raise ConnectionError(command)
            if outcome == "process_error":
                raise error
            if outcome in {"timeout", "cancel"}:
                await asyncio.Event().wait()
            return result

        async def wait_closed():
            """Keep channel cleanup observable until the test releases its output pumps."""
            closing.set()
            await release.wait()

        process = _process(wait)
        process.wait_closed = AsyncMock(side_effect=wait_closed)
        connection = SimpleNamespace(create_process=AsyncMock(return_value=process), run=AsyncMock())
        pool._ensure_live_connection = AsyncMock(return_value=SimpleNamespace(conn=connection))
        task = asyncio.create_task(pool.run("mock", command, timeout=0.01 if outcome == "timeout" else None,
                                            check=outcome == "process_error", retry=False))
        try:
            await asyncio.wait_for(entered.wait(), 2)
            if outcome == "cancel":
                task.cancel()
            await asyncio.wait_for(closing.wait(), 2)
            assert not task.done()
            if outcome == "cancel":
                for _ in range(2):
                    task.cancel()
                    await asyncio.sleep(0)
                    assert not task.done()
            release.set()
            if outcome == "cancel":
                with pytest.raises(asyncio.CancelledError):
                    await task
            elif outcome == "process_error":
                with pytest.raises(asyncssh.ProcessError) as raised:
                    await task
                assert raised.value is error
            else:
                assert await task is (result if outcome == "success" else None)
        finally:
            release.set()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        connection.create_process.assert_awaited_once_with(command)
        connection.run.assert_not_awaited()
        pool.disconnect.assert_not_awaited()
        process.close.assert_called_once()
        process.channel.abort.assert_called_once()
        process.wait_closed.assert_awaited_once()
        assert command not in str(async_pool._log.call_args_list)

    asyncio.run(exercise())


@pytest.mark.parametrize("outcome", ["error", "timeout", "cancel"])
def test_uncertain_creation_aborts_only_captured_connection(pool, outcome):
    """No process handle means the captured connection owns the unacknowledged exec."""
    async def exercise():
        """Replace the pool entry while opening a channel to check identity-safe cleanup."""
        entered = asyncio.Event()
        replacement = SimpleNamespace(abort=Mock())
        entry = SimpleNamespace(conn=None)

        async def create(command):
            """Simulate a request sent before the create-process acknowledgement was lost."""
            entered.set()
            entry.conn = replacement
            if outcome == "error":
                raise ConnectionError("PRIVATE-COMMAND-BODY")
            await asyncio.Event().wait()

        connection = SimpleNamespace(create_process=AsyncMock(side_effect=create), abort=Mock(), wait_closed=AsyncMock())
        entry.conn = connection
        pool._ensure_live_connection = AsyncMock(return_value=entry)
        task = asyncio.create_task(pool.run("mock", "PRIVATE-COMMAND-BODY", timeout=0.01 if outcome == "timeout" else None, retry=False))
        await asyncio.wait_for(entered.wait(), 2)
        if outcome == "cancel":
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            assert await task is None
        connection.create_process.assert_awaited_once()
        connection.abort.assert_called_once()
        connection.wait_closed.assert_awaited_once()
        replacement.abort.assert_not_called()
        assert "PRIVATE-COMMAND-BODY" not in str(async_pool._log.call_args_list)

    asyncio.run(exercise())


@pytest.mark.parametrize("failure", ["connection_lost", "timeout"])
def test_committed_maintenance_lost_reply_is_not_replayed_by_real_pool(tmp_path, tools, pool, monkeypatch, failure):
    """Exercise API -> actual pool retry policy -> mock SSH, not a mocked RPC shortcut."""
    root = tmp_path / "remote"
    root.mkdir()
    live = _bundle(tools, root / "data", 1)
    source = _bundle(tools, tmp_path / "source", 2)
    control = Control(root)
    dispatched = []
    processes = []

    async def upload(target, paths, operation=None):
        """Upload only temporary fixture databases into a mock target directory."""
        directory = root / "data" / "tmp" / "db-tools" / uuid.uuid4().hex
        directory.mkdir(parents=True)
        for name, path in paths.items():
            shutil.copy2(path, directory / name)
        return {name: str(directory / name) for name in paths}

    async def dispatch(command, check=False):
        """A real commit loses its reply; a buggy automatic retry would hit backup collision."""
        argv = shlex.split(command)
        script = argv[argv.index("-c") + 1]
        result = {}
        if "remote_main" in script:
            request = json.loads(argv[-1])
            dispatched.append(request["kind"])
            try:
                result = await asyncio.to_thread(maintenance.run, root, request, control=control)
            except Exception as exc:
                result = {"maintenance_error": str(exc), "recovery_pending": maintenance.recovery_record(root) is not None}
            if len(dispatched) == 1:
                if failure == "connection_lost":
                    raise ConnectionError("PRIVATE-COMMAND-BODY")
                await asyncio.Event().wait()
        return SimpleNamespace(returncode=0, stdout=json.dumps(result), stderr="")

    async def create(command):
        """Return owned fake process pipes for the single-attempt path."""
        process = _process(lambda check=False: dispatch(command, check))
        processes.append(process)
        return process

    connection = SimpleNamespace(run=AsyncMock(side_effect=dispatch), create_process=AsyncMock(side_effect=create))
    pool._ensure_live_connection = AsyncMock(return_value=SimpleNamespace(conn=connection))

    async def bounded_run(hostname, command, timeout=30, check=False, *, retry=True):
        """Shorten only the test deadline, retaining the actual pool's retry implementation."""
        return await async_pool.AsyncSSHPool.run(pool, hostname, command, timeout=1, check=check, retry=retry)

    monkeypatch.setattr(pool, "run", bounded_run)
    monkeypatch.setattr(tools, "_pool", lambda: pool, raising=False)
    monkeypatch.setattr(tools, "_remote_pbgui_dir", lambda target: str(root), raising=False)
    monkeypatch.setattr(tools, "_upload_source_snapshots", AsyncMock(side_effect=upload))
    monkeypatch.setattr(tools, "_remove_remote_snapshots", AsyncMock())

    with pytest.raises(HTTPException) as failed:
        asyncio.run(tools._maintain_target("mock", _request(source)))
    assert failed.value.status_code == 500
    assert dispatched == ["install"]
    assert _values(live) == [(2,), (2,)]
    assert "Remote DB Tools outcome unresolved" in tools.restart_block_reason()
    assert len(list((tmp_path / "data" / "locks").glob("db-tools-remote-*.json"))) == 1
    tools._remove_remote_snapshots.assert_not_awaited()
    pool.disconnect.assert_not_awaited()
    processes[0].close.assert_called_once()
    processes[0].wait_closed.assert_awaited_once()
    assert "PRIVATE-COMMAND-BODY" not in str(async_pool._log.call_args_list)
    assert "PRIVATE-COMMAND-BODY" not in str(tools._log.call_args_list)

    assert asyncio.run(tools._maintain_target("mock", {"kind": "recover"}))["ok"]
    assert dispatched == ["install", "recover"]
    assert not tools.restart_block_reason()
    assert _values(live) == [(2,), (2,)]
    tools._remove_remote_snapshots.assert_awaited_once()


def test_single_attempt_cleanup_error_does_not_expose_command(pool):
    """A cleanup exception must not leak its command payload through the caller's logs."""
    process = _process(AsyncMock(return_value=SimpleNamespace(returncode=0, stdout="", stderr="")))
    process.wait_closed.side_effect = ConnectionError("PRIVATE-COMMAND-BODY")
    connection = SimpleNamespace(create_process=AsyncMock(return_value=process))
    pool._ensure_live_connection = AsyncMock(return_value=SimpleNamespace(conn=connection))
    with pytest.raises(RuntimeError, match="remote outcome is unknown") as failed:
        asyncio.run(pool.run("mock", "PRIVATE-COMMAND-BODY", retry=False))
    assert failed.value.__suppress_context__ is True
    assert "PRIVATE-COMMAND-BODY" not in str(failed.value)
    assert "PRIVATE-COMMAND-BODY" not in str(async_pool._log.call_args_list)
