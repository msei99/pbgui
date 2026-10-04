"""Offline regressions for shared optimizer scans, change latches and log caches."""

import asyncio
import builtins
import json
import threading
from types import SimpleNamespace

import pytest
from fastapi import WebSocketDisconnect

from api import optimize_v7, optimize_v8


@pytest.fixture
def v7_store(tmp_path, monkeypatch):
    """Use isolated queue/log/config files and never inspect real OS processes."""
    queue, logs = tmp_path / 'queue', tmp_path / 'logs'
    queue.mkdir()
    logs.mkdir()
    config = tmp_path / 'config.json'
    config.write_text('{}')
    (queue / 'job.json').write_text(json.dumps({'filename': 'job', 'json': str(config)}))
    monkeypatch.setattr(optimize_v7, '_opt_queue_dir', lambda: queue)
    monkeypatch.setattr(optimize_v7, '_opt_log_dir', lambda: logs)
    monkeypatch.setattr(optimize_v7, 'load_pb7_config', lambda _: {'optimize': {}})
    monkeypatch.setattr(optimize_v7, '_build_optimize_process_index', lambda: [])
    monkeypatch.chdir(tmp_path)
    return optimize_v7.OptimizeStore(), queue, logs


def test_v7_clients_share_scans_and_unchanged_reads_do_not_notify(v7_store, monkeypatch):
    """Many clients perform one disk/process scan and cannot wake each other forever."""
    store, queue, _ = v7_store
    scans = []
    original_scan = store._scan_from_disk

    def counted_scan(root):
        """Record scans and execute the real isolated queue parser."""
        scans.append(threading.get_ident())
        return original_scan(root)

    monkeypatch.setattr(store, '_scan_from_disk', counted_scan)

    async def exercise():
        """Exercise concurrent connections, unchanged force reads and invalidation."""
        await asyncio.gather(*(store.refresh_from_disk() for _ in range(10)))
        assert len(scans) == 1
        assert scans[0] != threading.get_ident()
        assert store.changed.is_set()
        store.changed.clear()
        await store.refresh_from_disk(force=True)
        assert not store.changed.is_set()
        data = json.loads((queue / 'job.json').read_text())
        data['name'] = 'Updated'
        (queue / 'job.json').write_text(json.dumps(data))
        store.notify()
        await store.refresh_from_disk()
        assert store.items['job']['name'] == 'Updated'
        assert len(scans) == 3

    asyncio.run(exercise())


def test_v7_process_recovery_is_cached_but_invalidated_by_actions(v7_store, monkeypatch):
    """Unchanged queue scans reuse the process crawl; actions and empty queues are handled."""
    store, queue, _ = v7_store
    crawls = []
    monkeypatch.setattr(optimize_v7, '_build_optimize_process_index', lambda: crawls.append(True) or [])
    monkeypatch.setattr(optimize_v7, '_build_queue_config_counts', lambda _: pytest.fail('second queue scan'))

    async def exercise():
        """Simulate polling deadlines without altering the global asyncio clock."""
        await store.refresh_from_disk()
        store._next_refresh = 0
        await store.refresh_from_disk()
        assert len(crawls) == 1
        store.notify()
        await store.refresh_from_disk()
        assert len(crawls) == 2
        (queue / 'job.json').unlink()
        store.notify()
        await store.refresh_from_disk()
        assert not store.items
        assert len(crawls) == 2

    asyncio.run(exercise())


def test_v7_cancelled_refresh_drains_its_thread(v7_store, monkeypatch):
    """Shutdown must wait for an already-started scan before releasing its lock."""
    store, _, _ = v7_store
    entered, release = threading.Event(), threading.Event()

    def slow_scan(_):
        """Keep a mocked scan alive until the event loop requests shutdown."""
        entered.set()
        assert release.wait(2)
        return {}

    monkeypatch.setattr(store, '_scan_from_disk', slow_scan)

    async def exercise():
        """Cancel while blocked and ensure the thread is owned until it finishes."""
        task = asyncio.create_task(store.refresh_from_disk())
        assert await asyncio.to_thread(entered.wait, 1)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not store._lock.locked()

    asyncio.run(exercise())


def test_v7_new_live_pid_invalidates_cached_process_index(v7_store, monkeypatch):
    """A process started by another controller must not lose its valid PID file."""
    store, queue, _ = v7_store
    crawls = []
    descriptor = {'pid': 12345, 'proc': SimpleNamespace(pid=12345),
                  'args': {optimize_v7._normalize_process_arg_path(queue.parent / 'config.json')},
                  'log_paths': set(), 'create_time': 7.0}
    monkeypatch.setattr(store, '_is_process_running', lambda pid: pid == 12345)
    monkeypatch.setattr(optimize_v7, '_build_optimize_process_index',
                        lambda: crawls.append(True) or ([descriptor] if (queue / 'job.pid').exists() else []))

    async def exercise():
        """Introduce a live PID between polls, without any local notify call."""
        await store.refresh_from_disk()
        (queue / 'job.pid').write_text('12345')
        store._next_refresh = 0
        await store.refresh_from_disk()
        assert len(crawls) == 2
        assert store.items['job']['pid'] == 12345
        assert store.items['job']['status'] == 'running'
        assert (queue / 'job.pid').read_text() == '12345'

    asyncio.run(exercise())


def test_v7_reused_pid_cannot_recover_an_unrelated_optimizer(v7_store, monkeypatch):
    """A cached descriptor loses ownership when the OS reuses its process ID."""
    store, queue, _ = v7_store
    old = {'pid': 12345, 'proc': SimpleNamespace(pid=12345),
           'args': {optimize_v7._normalize_process_arg_path(queue.parent / 'config.json')},
           'log_paths': set(), 'create_time': 7.0}
    crawls = []
    monkeypatch.setattr(store, '_is_process_running', lambda pid: pid == 12345)
    monkeypatch.setattr(optimize_v7.psutil, 'Process', lambda _: SimpleNamespace(create_time=lambda: 9.0))
    monkeypatch.setattr(optimize_v7, '_build_optimize_process_index', lambda: crawls.append(True) or [old])

    async def exercise():
        """Replace the cached process with an optimizer for another configuration."""
        await store.refresh_from_disk()
        unrelated = {**old, 'create_time': 9.0, 'args': {'/other/config.json'}}
        monkeypatch.setattr(optimize_v7, '_build_optimize_process_index', lambda: crawls.append(True) or [unrelated])
        store._next_refresh = 0
        await store.refresh_from_disk()
        assert len(crawls) == 2
        assert store.items['job']['pid'] is None
        assert not (queue / 'job.pid').exists()

    asyncio.run(exercise())


def test_v7_unchanged_log_tail_avoids_reads_and_rotation_reloads(v7_store, monkeypatch):
    """Repeated statuses reuse a tail; append, rotation and deletion stay current."""
    store, _, logs = v7_store
    path = logs / 'job.log'
    path.write_text('Initial population size')
    reads = []
    original_open = builtins.open

    def counted_open(*args, **kwargs):
        """Count actual log reads while leaving temporary fixture writes untouched."""
        reads.append(args[0])
        return original_open(*args, **kwargs)

    monkeypatch.setattr(builtins, 'open', counted_open)
    for _ in range(10):
        assert store._read_log_tail(path) == 'Initial population size'
    assert len(reads) == 1
    path.write_text('Optimization complete')
    assert store._read_log_tail(path) == 'Optimization complete'
    replacement = logs / 'new.log'
    replacement.write_text('new run')
    replacement.replace(path)
    assert store._read_log_tail(path) == 'new run'
    path.unlink()
    assert store._read_log_tail(path) is None
    assert len(reads) == 3


def test_v8_many_clients_share_one_scan_and_external_changes_expire(monkeypatch):
    """Concurrent clients share disk I/O while expired snapshots observe runtime changes."""
    calls = []
    rows = [{'filename': 'job', 'status': 'queued'}, {'filename': 'loop', 'loop_id': 'abc'}]
    monkeypatch.setattr(optimize_v8, '_ws_snapshot_lock', asyncio.Lock())
    monkeypatch.setattr(optimize_v8, '_ws_snapshot', None)
    monkeypatch.setattr(optimize_v8, '_ws_snapshot_until', 0)
    monkeypatch.setattr(optimize_v8, '_load_queue', lambda: calls.append(threading.get_ident()) or list(rows))
    monkeypatch.setattr(optimize_v8, 'load_ini_section', lambda _: {'autostart': 'False'})

    async def exercise():
        """Share ten simultaneous initial subscriptions then expire the cache."""
        payloads = await asyncio.gather(*(optimize_v8._queue_ws_snapshot() for _ in range(10)))
        assert len(calls) == 1
        assert calls[0] != threading.get_ident()
        assert all(payload == payloads[0] for payload in payloads)
        assert payloads[0]['items'] == [rows[0]]
        rows[0] = {'filename': 'job', 'status': 'running'}
        optimize_v8._ws_snapshot_until = 0
        assert (await optimize_v8._queue_ws_snapshot())['items'][0]['status'] == 'running'
        assert len(calls) == 2

    asyncio.run(exercise())


@pytest.mark.parametrize('version', ['v7', 'v8'])
def test_websocket_sends_only_changes_and_backs_off_when_idle(monkeypatch, version):
    """An unchanged frame is suppressed; status and settings changes still push."""
    module = optimize_v7 if version == 'v7' else optimize_v8
    snapshots = [
        {'type': 'queue_update', 'items': [], 'settings': {'autostart': 'False'}},
        {'type': 'queue_update', 'items': [], 'settings': {'autostart': 'False'}},
        {'type': 'queue_update', 'items': [], 'settings': {'autostart': 'True'}},
        {'type': 'queue_update', 'items': [{'filename': 'job', 'status': 'running'}], 'settings': {}},
    ]
    sent, delays = [], []
    store = SimpleNamespace(items={}, changed=asyncio.Event())
    current = {}

    async def snapshot():
        """Return successive snapshots and end the mocked connection."""
        if not snapshots:
            raise WebSocketDisconnect()
        current.clear()
        current.update(snapshots.pop(0))
        store.items = {row['filename']: row for row in current['items']}
        return dict(current)

    async def wait_for(awaitable, timeout):
        """Record intervals without real-time waits or network traffic."""
        delays.append(timeout)
        awaitable.close()
        raise asyncio.TimeoutError()

    async def send(payload):
        """Record only payloads the backend actually broadcasts."""
        sent.append(payload)

    async def receive():
        """Provide the receive coroutine consumed by the timeout mock."""
        return ''

    async def authenticated(_):
        """Use a fake authenticated connection without runtime credentials."""
        return True

    monkeypatch.setattr(module.asyncio, 'wait_for', wait_for)
    monkeypatch.setattr(module, '_ws_clients', set())
    # V8 tracks sockets in a set, so use an identity-hashable stand-in.
    websocket = type('Socket', (), {'send_json': staticmethod(send), 'receive_text': staticmethod(receive)})()
    if version == 'v7':
        store.refresh_from_disk = snapshot
        monkeypatch.setattr(module, '_store', store)
        monkeypatch.setattr(module, '_read_ini_section', lambda: dict(current['settings']))
        asyncio.run(module._ws_push_loop(websocket))
    else:
        monkeypatch.setattr(module, 'authenticate_websocket', authenticated)
        monkeypatch.setattr(module, '_queue_ws_snapshot', snapshot)
        asyncio.run(module.ws_optimize(websocket))
        assert not module._ws_clients
        assert module._ws_snapshot is None
    assert len(sent) == 3
    assert delays == [8.0, 8.0, 8.0, 3.0]
