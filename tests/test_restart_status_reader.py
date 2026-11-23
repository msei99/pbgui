"""Offline coalescing, invalidation and cancellation checks for passive status reads."""

import asyncio
import threading

import pytest

from api.restart_status_reader import RestartStatusReader, RestartStatusUnavailable


def test_concurrent_viewers_share_inspection_and_cannot_mutate_cached_state():
    """A tab burst uses one worker; copies and short expiry keep the display current."""
    async def scenario():
        entered, release = threading.Event(), threading.Event()
        calls = []
        clock = [0.0]

        def inspect():
            """Hold one isolated disk inspection until every viewer has joined."""
            calls.append(1)
            entered.set()
            assert release.wait(3)
            return {'services': [{'serial': 10}]}

        reader = RestartStatusReader(inspect, lambda: 10, clock=lambda: clock[0])
        viewers = [asyncio.create_task(reader.read()) for _ in range(40)]
        assert await asyncio.to_thread(entered.wait, 2)
        await asyncio.sleep(0)
        release.set()
        results = await asyncio.gather(*viewers)
        assert len(calls) == 1
        results[0]['services'][0]['serial'] = 999
        assert results[1]['services'][0]['serial'] == 10
        assert (await reader.read())['services'][0]['serial'] == 10
        clock[0] = 0.3
        await reader.read()
        assert len(calls) == 2
        await reader.close()
    asyncio.run(scenario())


def test_changed_serial_replaces_inflight_result_without_parallel_workers():
    """An inspection begun for old code is joined, discarded and repeated once."""
    async def scenario():
        entered, release = threading.Event(), threading.Event()
        version = [1]
        calls = []

        def inspect():
            """Capture the fixture generation before waiting for a simulated update."""
            value = version[0]
            calls.append(value)
            if value == 1:
                entered.set()
                assert release.wait(3)
            return {'serial': value}

        reader = RestartStatusReader(inspect, lambda: version[0])
        first = asyncio.create_task(reader.read())
        assert await asyncio.to_thread(entered.wait, 2)
        version[0] = 2
        second = asyncio.create_task(reader.read())
        await asyncio.sleep(0)
        assert calls == [1]
        release.set()
        assert await first == await second == {'serial': 2}
        assert calls == [1, 2]
        version[0] = 3
        assert await reader.read() == {'serial': 3}
        await reader.close()
    asyncio.run(scenario())


def test_cancelled_viewer_cannot_cancel_another_viewers_scan():
    """A disconnected tab releases its wait while another tab keeps the same job."""
    async def scenario():
        entered, release = threading.Event(), threading.Event()

        def inspect():
            """Model a shared non-destructive file read."""
            entered.set()
            assert release.wait(3)
            return {'ready': True}

        reader = RestartStatusReader(inspect, lambda: 1)
        first, second = [asyncio.create_task(reader.read()) for _ in range(2)]
        assert await asyncio.to_thread(entered.wait, 2)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        assert not reader.task.cancelled()
        release.set()
        assert await second == {'ready': True}
        await reader.close()
    asyncio.run(scenario())


def test_cancelled_shutdown_still_joins_worker_and_can_reopen():
    """A shutdown timeout never abandons the underlying read thread."""
    async def scenario():
        entered, release, finished = threading.Event(), threading.Event(), threading.Event()

        def inspect():
            """Expose actual thread completion independently of the asyncio waiter."""
            entered.set()
            assert release.wait(3)
            finished.set()
            return {'ready': True}

        reader = RestartStatusReader(inspect, lambda: 1)
        viewer = asyncio.create_task(reader.read())
        assert await asyncio.to_thread(entered.wait, 2)
        viewer.cancel()
        await asyncio.gather(viewer, return_exceptions=True)
        closing = asyncio.create_task(reader.close())
        await asyncio.sleep(0)
        closing.cancel()
        await asyncio.sleep(0)
        assert not closing.done()
        with pytest.raises(RuntimeError, match='closing'):
            await reader.read()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await closing
        assert finished.is_set()
        reader.start()
        assert await reader.read() == {'ready': True}
        await reader.close()
    asyncio.run(scenario())


def test_orphaned_error_is_logged_once_and_next_read_retries(monkeypatch):
    """Failed shared reads are neither cached as success nor left unobserved."""
    import api.restart_status_reader as module
    logs = []
    monkeypatch.setattr(module, '_log', lambda *args, **kwargs: logs.append(kwargs))

    async def scenario():
        entered, release = threading.Event(), threading.Event()
        calls = []

        def inspect():
            """Fail after the only viewer disconnects, then allow a fresh inspection."""
            calls.append(1)
            if len(calls) == 1:
                entered.set()
                assert release.wait(3)
                raise OSError('isolated fixture error')
            return {'ready': True}

        reader = RestartStatusReader(inspect, lambda: 1)
        viewer = asyncio.create_task(reader.read())
        assert await asyncio.to_thread(entered.wait, 2)
        viewer.cancel()
        await asyncio.gather(viewer, return_exceptions=True)
        release.set()
        await asyncio.gather(reader.task, return_exceptions=True)
        assert len(logs) == 1
        assert logs[0]['meta'] == {'error_type': 'OSError'}
        assert await reader.read() == {'ready': True}
        await reader.close()
    asyncio.run(scenario())


def test_http_and_sse_routes_share_worker_without_caching_auth(monkeypatch):
    """Real REST and SSE handlers join one inspection and add auth independently."""
    import PBApiServer as server
    entered, release = threading.Event(), threading.Event()
    calls = []

    def inspect():
        """Supply only public fixture display metadata."""
        calls.append(1)
        entered.set()
        assert release.wait(3)
        return {'needs_restart': False}

    reader = RestartStatusReader(inspect, lambda: 1)
    monkeypatch.setattr(server, '_restart_status_reader', reader)
    monkeypatch.setattr(server, '_restart_memory_block_reason', lambda: '')
    monkeypatch.setattr(server, '_local_master_name', lambda: 'fixture')
    monkeypatch.setattr(server, 'auth_runtime_status', lambda: {'authenticated': True})
    monkeypatch.setattr(server, '_sse_subscribers', [])

    async def scenario():
        response = await server.server_status_stream(session=object())
        iterator = response.body_iterator
        stream = asyncio.create_task(anext(iterator))
        requests = [asyncio.create_task(server.server_status(session=object())) for _ in range(12)]
        assert await asyncio.to_thread(entered.wait, 2)
        release.set()
        await stream
        results = await asyncio.gather(*requests)
        assert len(calls) == 1
        assert all(result['auth'] == {'authenticated': True} for result in results)
        assert await reader.read() == {'needs_restart': False}
        await iterator.aclose()
        assert not server._sse_subscribers
        await reader.close()
    asyncio.run(scenario())


def test_cancelled_inspection_task_still_owns_its_actual_thread():
    """An externally cancelled task cannot hide a running worker or permit overlap."""
    async def scenario():
        entered, release, finished = threading.Event(), threading.Event(), threading.Event()

        def inspect():
            """Hold the real executor job after cancellation of its asyncio driver."""
            entered.set()
            assert release.wait(3)
            finished.set()
            return {'ready': True}

        reader = RestartStatusReader(inspect, lambda: 1)
        viewer = asyncio.create_task(reader.read())
        assert await asyncio.to_thread(entered.wait, 2)
        reader.task.cancel()
        await asyncio.sleep(0)
        with pytest.raises(RuntimeError, match='still running'):
            reader.start()
        closing = asyncio.create_task(reader.close())
        await asyncio.sleep(0)
        assert not closing.done()
        release.set()
        with pytest.raises(RestartStatusUnavailable, match='interrupted'):
            await viewer
        await closing
        assert finished.is_set()
        assert reader.pool is None
    asyncio.run(scenario())


def test_closed_unused_reader_requires_explicit_start():
    """An incoming read cannot silently undo shutdown before first use."""
    async def scenario():
        reader = RestartStatusReader(lambda: {}, lambda: 1)
        await reader.close()
        with pytest.raises(RestartStatusUnavailable, match='closing'):
            await reader.read()
        reader.start()
        assert await reader.read() == {}
        await reader.close()
    asyncio.run(scenario())


def test_repeated_key_changes_fail_transiently_after_two_inspections():
    """Continuous code/settings changes cannot spawn an unlimited retry loop."""
    async def scenario():
        version = [0]

        def inspect():
            """Change the fixture key during every inspection."""
            version[0] += 1
            return {}

        reader = RestartStatusReader(inspect, lambda: version[0])
        with pytest.raises(RestartStatusUnavailable, match='changed'):
            await reader.read()
        assert version[0] == 2
        await reader.close()
    asyncio.run(scenario())


def test_request_timeout_keeps_one_worker_owned_until_shutdown():
    """Timed-out viewers receive a transient failure, with no extra queued threads."""
    async def scenario():
        entered, release = threading.Event(), threading.Event()
        calls = []

        def inspect():
            """Hold a local-read fixture longer than the caller's timeout."""
            calls.append(1)
            entered.set()
            assert release.wait(3)
            return {}

        reader = RestartStatusReader(inspect, lambda: 1, timeout=0.02)
        for _ in range(2):
            with pytest.raises(RestartStatusUnavailable, match='timed out'):
                await reader.read()
        assert len(calls) == 1
        release.set()
        await reader.close()
        assert reader.worker.done()
        assert reader.pool is None
    asyncio.run(scenario())


def test_cache_age_starts_when_inspection_begins():
    """A long scan cannot extend old display data by another full TTL afterward."""
    async def scenario():
        clock = [0.0]
        calls = []

        def inspect():
            """Advance only the isolated monotonic clock during each scan."""
            calls.append(1)
            clock[0] += 1
            return {}

        reader = RestartStatusReader(inspect, lambda: 1, clock=lambda: clock[0])
        await reader.read()
        await reader.read()
        assert len(calls) == 2
        await reader.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('error', [OSError, TimeoutError])
def test_inspection_errors_are_transient_failures_not_caller_timeouts(error):
    """Native IO/timeouts receive a safe retry response rather than misleading timing."""
    def inspect():
        """Raise a synthetic underlying inspection error."""
        raise error('fixture failure')

    async def scenario():
        reader = RestartStatusReader(inspect, lambda: 1)
        with pytest.raises(RestartStatusUnavailable, match='inspection failed'):
            await reader.read()
        await reader.close()
    asyncio.run(scenario())


def test_overlapping_closers_cannot_reopen_or_close_a_new_pool():
    """Handoff and lifespan cleanup must both finish before a reader is reopened."""
    async def scenario():
        entered, release = threading.Event(), threading.Event()

        def inspect():
            """Hold one actual inspection while two cleanup owners join it."""
            entered.set()
            assert release.wait(3)
            return {}

        reader = RestartStatusReader(inspect, lambda: 1)
        request = asyncio.create_task(reader.read())
        assert await asyncio.to_thread(entered.wait, 2)
        closers = [asyncio.create_task(reader.close()) for _ in range(2)]
        await asyncio.sleep(0)
        assert reader.closers == 2
        with pytest.raises(RuntimeError, match='still running'):
            reader.start()
        release.set()
        await asyncio.gather(request, *closers)
        assert reader.closers == 0
        reader.start()
        assert await reader.read() == {}
        await reader.close()
    asyncio.run(scenario())
