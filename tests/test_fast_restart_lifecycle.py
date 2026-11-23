"""Offline restart latency regressions without real services or user data."""

import asyncio
from pathlib import Path
import threading

import httpx
import pytest
from fastapi import FastAPI
from starlette.responses import StreamingResponse

from api.read_stream_lifecycle import ReadStreamMiddleware, ReadStreamOwner
from credential_migration import CredentialMigrationCoordinator
from secure_files import harden_sensitive_paths


def test_hardening_preserves_managed_scan_cache_for_private_blobs(tmp_path, monkeypatch):
    """Checking correct modes must not invalidate ctime-bound secret scan evidence."""
    coordinator = CredentialMigrationCoordinator(tmp_path)
    directory = coordinator.cluster_root / 'secret_blobs' / 'nested'
    directory.mkdir(parents=True)
    blob = directory / 'test.json'
    blob.write_text('{"value":"harmless fixture"}')
    blob.chmod(0o600)
    harden_sensitive_paths(tmp_path)
    assert coordinator._scan_managed_files({'fixture-secret'}, cache_scope='test') == []
    before = blob.stat()
    reads = []
    original = Path.read_bytes

    def read_bytes(path):
        """Record only reads of the isolated managed fixture."""
        if path == blob:
            reads.append(path)
        return original(path)

    monkeypatch.setattr(Path, 'read_bytes', read_bytes)
    harden_sensitive_paths(tmp_path)
    assert blob.stat().st_ctime_ns == before.st_ctime_ns
    assert coordinator._scan_managed_files({'fixture-secret'}, cache_scope='test') == []
    assert reads == []
    blob.write_text('{"value":"fixture-secret"}')
    assert coordinator._scan_managed_files({'fixture-secret'}, cache_scope='test')
    assert reads == [blob]


def test_fast_scan_does_not_follow_file_or_directory_symlinks(tmp_path):
    """Faster traversal retains the existing no-symlink boundary."""
    coordinator = CredentialMigrationCoordinator(tmp_path / 'gui')
    logs = coordinator.root / 'data' / 'logs'
    logs.mkdir()
    outside = tmp_path / 'outside'
    outside.mkdir()
    (outside / 'secret.log').write_text('fixture-secret')
    (logs / 'linked-dir').symlink_to(outside, target_is_directory=True)
    (logs / 'linked-file.log').symlink_to(outside / 'secret.log')
    assert coordinator._scan_managed_files({'fixture-secret'}, cache_scope='test') == []
    (logs / 'real.log').write_text('fixture-secret')
    assert coordinator._scan_managed_files({'fixture-secret'}, cache_scope='test') == ['data/logs/real.log:managed-log']


@pytest.mark.parametrize('base_middleware', [False, True])
@pytest.mark.parametrize('spec', ['2.3', '2.4'])
def test_read_streams_close_without_waiting_for_keepalive(base_middleware, spec):
    """Real StreamingResponse and HTTP middleware unwind when restart closes reads."""
    async def scenario():
        owner = ReadStreamOwner()
        app = FastAPI()
        entered, cleaned = asyncio.Event(), asyncio.Event()

        async def headers(request, call_next):
            """Exercise the same BaseHTTPMiddleware boundary used by API auth."""
            return await call_next(request)
        if base_middleware:
            app.middleware('http')(headers)

        @app.get('/events')
        async def events():
            """Represent a read stream with an indefinitely idle subscription."""
            async def generate():
                try:
                    yield b'data: fixture\n\n'
                    entered.set()
                    await asyncio.Future()
                finally:
                    cleaned.set()
            return StreamingResponse(generate(), media_type='text/event-stream')

        app.add_middleware(ReadStreamMiddleware, owner=owner)

        async def protocol_app(scope, receive, send):
            """Exercise both native task-group and direct ASGI streaming paths."""
            scope['asgi'] = {'version': '3.0', 'spec_version': spec}
            await app(scope, receive, send)

        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=protocol_app), base_url='http://fixture') as client:
            response = asyncio.create_task(client.get('/events'))
            await asyncio.wait_for(entered.wait(), 2)
            await asyncio.wait_for(owner.close(), 2)
            result = await asyncio.wait_for(response, 2)
            assert result.status_code == 200
            assert cleaned.is_set()
            assert not owner.tasks
            assert not owner.cancelled_tasks
            assert response.cancelling() == 0
            # A reconnect while shutdown is pending cannot reopen a long-lived read.
            assert (await asyncio.wait_for(client.get('/events'), 2)).status_code == 503
            assert not owner.tasks
    asyncio.run(scenario())


@pytest.mark.parametrize('method,content_type', [('POST', 'text/event-stream'), ('GET', 'application/json')])
def test_stream_shutdown_does_not_cancel_writes_or_regular_requests(method, content_type):
    """Only confirmed GET SSE responses belong to disposable restart reads."""
    async def scenario():
        owner = ReadStreamOwner()
        entered, release = asyncio.Event(), asyncio.Event()
        sent = []

        async def app(scope, receive, send):
            """Hold a synthetic request independent of the stream owner."""
            await send({'type': 'http.response.start', 'status': 200,
                        'headers': [(b'content-type', content_type.encode())]})
            entered.set()
            await release.wait()
            await send({'type': 'http.response.body', 'body': b'{}'})

        async def send(message):
            """Collect ASGI messages without opening a socket."""
            sent.append(message)

        async def receive():
            """Provide the isolated request body."""
            return {'type': 'http.request', 'body': b''}

        middleware = ReadStreamMiddleware(app, owner)
        request = asyncio.create_task(middleware({'type': 'http', 'method': method}, receive, send))
        await entered.wait()
        await owner.close()
        assert not request.done()
        release.set()
        await request
        assert sent[-1]['body'] == b'{}'
    asyncio.run(scenario())


def test_ai_retention_does_not_delay_readiness_and_shutdown_joins_it(tmp_path, monkeypatch):
    """Mandatory recovery completes first; optional retention has explicit ownership."""
    from ai_capabilities import AICapabilityService
    service = AICapabilityService(tmp_path / 'capabilities')
    order = []
    entered, release = threading.Event(), threading.Event()
    monkeypatch.setattr(service, '_interrupt_python_analyses', lambda: order.append('interrupt'))
    monkeypatch.setattr(service, '_recover_journals', lambda: order.append('recover'))

    def prune():
        """Hold only disposable cleanup while startup must remain responsive."""
        order.append('prune')
        entered.set()
        assert release.wait(3)

    monkeypatch.setattr(service, '_prune_durable_state', prune)

    async def scenario():
        await asyncio.wait_for(service.startup(), 2)
        assert order[:2] == ['interrupt', 'recover']
        assert await asyncio.to_thread(entered.wait, 2)
        closing = asyncio.create_task(service.shutdown())
        await asyncio.sleep(0.02)
        assert not closing.done()
        release.set()
        await closing
        assert service._maintenance_task is None
    asyncio.run(scenario())



def test_read_stream_header_send_finishes_before_registration():
    """Restart never sends a body before a delayed response-start was accepted."""
    async def scenario():
        owner = ReadStreamOwner()
        entered, release = asyncio.Event(), asyncio.Event()
        messages = []

        async def app(scope, receive, send):
            """Represent a stream waiting to forward its headers."""
            await send({'type': 'http.response.start', 'status': 200,
                        'headers': [(b'content-type', b'text/event-stream')]})
            await asyncio.Future()

        async def send(message):
            """Make the start boundary observable without a real socket."""
            if message['type'] == 'http.response.start':
                entered.set()
                await release.wait()
            messages.append(message['type'])

        request = asyncio.create_task(ReadStreamMiddleware(app, owner)(
            {'type': 'http', 'method': 'GET'}, None, send))
        await entered.wait()
        await owner.close()
        assert not owner.tasks
        assert not messages
        release.set()
        await asyncio.wait_for(request, 2)
        assert messages == ['http.response.start', 'http.response.body']
        assert request.cancelling() == 0
    asyncio.run(scenario())


def test_external_stream_cancellation_is_not_swallowed():
    """An unrelated request timeout remains observable even during shutdown."""
    async def scenario():
        owner = ReadStreamOwner()
        entered = asyncio.Event()

        async def app(scope, receive, send):
            """Expose an idle synthetic SSE request."""
            await send({'type': 'http.response.start', 'status': 200,
                        'headers': [(b'content-type', b'text/event-stream')]})
            entered.set()
            await asyncio.Future()

        async def send(message):
            """Discard isolated ASGI output."""
            return None

        request = asyncio.create_task(ReadStreamMiddleware(app, owner)(
            {'type': 'http', 'method': 'GET'}, None, send))
        await entered.wait()
        request.cancel('external timeout')
        closing = asyncio.create_task(owner.close())
        with pytest.raises(asyncio.CancelledError):
            await request
        await closing
        assert not owner.tasks
        assert not owner.cancelled_tasks
    asyncio.run(scenario())



def test_cancelled_stream_closer_drains_without_cancelling_request():
    """A cancelled shutdown waiter retains child ownership without cancelling callers."""
    async def scenario():
        owner = ReadStreamOwner()
        entered, cleaning, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

        async def app(scope, receive, send):
            """Hold subscription cleanup after the owned stream is cancelled."""
            try:
                await send({'type': 'http.response.start', 'status': 200,
                            'headers': [(b'content-type', b'text/event-stream')]})
                entered.set()
                await asyncio.Future()
            finally:
                cleaning.set()
                await release.wait()

        async def send(message):
            """Discard isolated response output."""
            return None

        request = asyncio.create_task(ReadStreamMiddleware(app, owner)(
            {'type': 'http', 'method': 'GET'}, None, send))
        await entered.wait()
        closing = asyncio.create_task(owner.close())
        await cleaning.wait()
        closing.cancel()
        await asyncio.sleep(0)
        assert not closing.done()
        assert request.cancelling() == 0
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await closing
        await request
        assert not owner.tasks
    asyncio.run(scenario())


def test_restart_rejects_new_read_requests_without_running_them():
    """Polling and reconnects receive a transient 503 while existing work drains."""
    async def scenario():
        owner = ReadStreamOwner()
        await owner.close()
        messages, calls = [], []

        async def app(scope, receive, send):
            """Record isolated application execution; no real data is accessed."""
            calls.append(scope['method'])

        async def send(message):
            """Collect isolated protocol messages."""
            messages.append(message)

        middleware = ReadStreamMiddleware(app, owner)
        await middleware({'type': 'http', 'method': 'GET'}, None, send)
        assert messages[0]['status'] == 503
        assert (b'retry-after', b'1') in messages[0]['headers']
        assert not calls
        await middleware({'type': 'http', 'method': 'POST'}, None, send)
        assert calls == ['POST']
        owner.start()
        await middleware({'type': 'http', 'method': 'GET'}, None, send)
        assert calls == ['POST', 'GET']
        assert not owner.requests
    asyncio.run(scenario())



def test_api_restart_allows_runtime_readers_but_excludes_source_updates(tmp_path, monkeypatch):
    """A readonly helper cannot block restart; a real source writer still must."""
    import os
    import PBApiServer as server
    from master_update_lock import (MasterUpdateBusyError, acquire_master_runtime_lock,
                                    acquire_master_update_lock)
    monkeypatch.setattr(server, 'PBGDIR', str(tmp_path))
    reader = acquire_master_runtime_lock(tmp_path)
    leases = server._acquire_api_restart_leases()
    try:
        assert not os.get_inheritable(leases[-1]._lock_file.fileno())
        with pytest.raises(MasterUpdateBusyError):
            acquire_master_update_lock(tmp_path)
        reader.release()
        # Releasing another same-process reader must not release the restart barrier.
        with pytest.raises(MasterUpdateBusyError):
            acquire_master_update_lock(tmp_path)
    finally:
        reader.release()
        server._release_api_restart_leases(leases)
    writer = acquire_master_update_lock(tmp_path)
    try:
        with pytest.raises(MasterUpdateBusyError):
            server._acquire_api_restart_leases()
    finally:
        writer.release()
    # A rejected restart must also have released its earlier lifecycle reservations.
    leases = server._acquire_api_restart_leases()
    server._release_api_restart_leases(leases)


@pytest.mark.parametrize("schedule_ok", [True, False])
def test_systemd_restart_closes_read_streams_on_owner_loop(monkeypatch, schedule_ok):
    """Both API restart entry points drain on the server loop and reopen on failure."""
    import PBApiServer as server

    async def scenario():
        owner = ReadStreamOwner()
        owner.start()
        monkeypatch.setattr(server, '_read_stream_owner', owner)
        monkeypatch.setattr(server, '_systemd_owns_current_api', lambda: True)
        observed = []

        def schedule(*args, **kwargs):
            """Observe admission before accepting or rejecting an isolated handoff."""
            observed.append(owner.closing)
            return schedule_ok, 'fixture scheduling result'

        monkeypatch.setattr(server, '_queue_current_api_systemd_restart', schedule)
        if schedule_ok:
            assert await asyncio.to_thread(server._restart_current_api_systemd_unit,
                                           monitor_handoff=False)
            assert owner.closing
        else:
            with pytest.raises(RuntimeError, match='fixture scheduling result'):
                await asyncio.to_thread(server._restart_current_api_systemd_unit,
                                        monitor_handoff=False)
            await asyncio.sleep(0)
            assert not owner.closing
        assert observed == [True]
    asyncio.run(scenario())


def test_script_entrypoint_and_lazy_service_import_share_live_state(tmp_path):
    """The real entrypoint guard prevents a second API app in direct CLI mode."""
    import ast
    import subprocess
    import sys
    source_path = Path(__file__).resolve().parents[1] / 'PBApiServer.py'
    tree = ast.parse(source_path.read_text())
    guards = [node for node in tree.body if isinstance(node, ast.If)
              and 'sys.modules["PBApiServer"] =' in ast.unparse(node).replace("'", '"')]
    assert len(guards) == 1
    script = tmp_path / 'entrypoint.py'
    script.write_text('import sys, importlib\n' + ast.unparse(guards[0]) +
                      "\n_read_stream_owner = object()\n" +
                      "service = importlib.import_module('PBApiServer')\n" +
                      "assert service is sys.modules['__main__']\n" +
                      "assert service._read_stream_owner is _read_stream_owner\n")
    result = subprocess.run([sys.executable, str(script)], cwd=tmp_path,
                            capture_output=True, text=True, timeout=5)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize('reason', ['', 'An API-owned operation is running'])
def test_restart_status_does_not_run_mutable_admission_inspection(monkeypatch, reason):
    """Status polling cannot strand an action-preparation worker during shutdown."""
    import PBApiServer as server

    async def unexpected():
        """Fail if passive polling repeats the action's persisted admission checks."""
        pytest.fail('Passive status executed full restart admission')

    monkeypatch.setattr(server, '_restart_block_state', unexpected)
    monkeypatch.setattr(server, '_restart_memory_block_reason', lambda: reason)
    monkeypatch.setattr(server, '_restart_status_payload', lambda: {'needs_restart': False})
    monkeypatch.setattr(server, '_local_master_name', lambda: 'fixture')
    monkeypatch.setattr(server, 'auth_runtime_status', lambda: {})
    payload = asyncio.run(server.server_status(session=object()))
    assert payload['restart_blocked'] is bool(reason)
    assert payload['restart_block_reason'] == reason
