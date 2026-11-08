"""Offline report ownership and foreground progress under slow observer operations."""
import asyncio
import threading
import time

import pytest

from pb8_loop_controller import LoopController, owned_thread
from pb8_loop_store import LoopDeletionConflict
from tests.test_pb8_loop_optimizer import OWNER, FakeBackend, record


def test_slow_report_does_not_hold_foreground_controller(record, tmp_path, monkeypatch):
    """A blocked report cannot prevent subsequent optimizer polling or duplicate itself."""
    store, row = record
    row = store.update(OWNER, row['id'], lambda current: current.update(status='running', phase='optimize'))
    controller = LoopController(tmp_path, store=store, backend=FakeBackend())

    async def exercise():
        """Hold one report pass while two real controller claims finish independently."""
        entered, release = asyncio.Event(), asyncio.Event()
        polls, reports = [], []

        async def foreground(_current):
            """Represent an authoritative optimizer status check."""
            polls.append(True)

        async def observer(_current):
            """Keep the report operation unfinished until the test releases it."""
            reports.append(True)
            entered.set()
            await release.wait()

        monkeypatch.setattr(controller, '_tick', foreground)
        monkeypatch.setattr(controller.observers, 'tick', observer)
        try:
            await controller.tick(OWNER, row['id'], background_reports=True)
            await asyncio.wait_for(entered.wait(), 2)
            await asyncio.wait_for(controller.tick(OWNER, row['id'], background_reports=True), 2)
            assert len(polls) == 2
            assert len(reports) == 1
            with pytest.raises(LoopDeletionConflict):
                controller.delete(OWNER, row['id'])
        finally:
            release.set()
            await controller.close()
        assert not controller._observer_steps

    asyncio.run(exercise())


def test_report_claim_serializes_distinct_controllers(record, tmp_path, monkeypatch):
    """Separate API/controller instances cannot process the same report concurrently."""
    store, row = record
    first = LoopController(tmp_path, store=store, backend=FakeBackend())
    second = LoopController(tmp_path, store=store, backend=FakeBackend())

    async def exercise():
        """Keep the first OS-level report claim until the second has attempted it."""
        entered, release = asyncio.Event(), asyncio.Event()
        calls = []

        async def blocked(_current):
            """Retain the first report claim."""
            entered.set()
            await release.wait()

        async def other(_current):
            """Record a forbidden simultaneous report pass."""
            calls.append(True)

        monkeypatch.setattr(first.observers, 'tick', blocked)
        monkeypatch.setattr(second.observers, 'tick', other)
        task = asyncio.create_task(first._observer_tick(OWNER, row['id'], allow_launch=True))
        try:
            await asyncio.wait_for(entered.wait(), 2)
            await second._observer_tick(OWNER, row['id'], allow_launch=True)
            assert not calls
        finally:
            release.set()
            await task
            await first.close()
            await second.close()

    asyncio.run(exercise())


def test_close_joins_started_report_thread(record, tmp_path, monkeypatch):
    """Cancellation joins an already-started filesystem operation without stopping native jobs."""
    store, row = record
    controller = LoopController(tmp_path, store=store, backend=FakeBackend())
    entered, release = threading.Event(), threading.Event()

    def operation():
        """Simulate an owned, finite filesystem operation."""
        entered.set()
        assert release.wait(3)

    async def observer(_current):
        """Use the production cancellation/join boundary."""
        await owned_thread(operation)

    monkeypatch.setattr(controller.observers, 'tick', observer)

    async def exercise():
        """Verify shutdown remains pending until its operation has been joined."""
        controller._schedule_observers(OWNER, row['id'], allow_launch=True)
        assert await asyncio.to_thread(entered.wait, 2)
        closing = asyncio.create_task(controller.close())
        try:
            await asyncio.sleep(.02)
            assert not closing.done()
        finally:
            release.set()
            await closing
        assert not controller._observer_steps
        assert not controller.backend.stops

    asyncio.run(exercise())


def test_scheduled_report_reads_current_pause_state(record, tmp_path, monkeypatch):
    """An independently scheduled pass must not reuse a pre-pause control snapshot."""
    store, row = record
    controller = LoopController(tmp_path, store=store, backend=FakeBackend())
    seen = []

    async def observer(current, *, allow_launch):
        """Inspect only refreshed state and the collect-only policy."""
        seen.append((current['status'], allow_launch))

    monkeypatch.setattr(controller.observers, 'tick', observer)

    async def exercise():
        """Pause before the scheduled task starts and retain its launch restriction."""
        controller._schedule_observers(OWNER, row['id'], allow_launch=False)
        store.update(OWNER, row['id'], lambda current: current.update(status='paused'))
        await asyncio.gather(*list(controller._observer_steps.values()))
        await controller.close()

    asyncio.run(exercise())
    assert seen == [('paused', False)]


@pytest.mark.parametrize('slow', [False, True])
def test_owned_thread_warning_preserves_result_and_timeout_errors(monkeypatch, slow):
    """A timing warning neither exposes arguments nor consumes the worker's own TimeoutError."""
    import pb8_loop_controller as module
    logs = []
    wait_for = asyncio.wait_for

    async def short_wait(awaitable, *, timeout):
        """Exercise the warning with a small deterministic test-only interval."""
        return await wait_for(awaitable, timeout=.005)

    def operation(secret):
        """Complete slowly, or fail immediately with the backend's own timeout."""
        if not slow:
            raise TimeoutError('backend timeout')
        time.sleep(.04)
        return 42

    monkeypatch.setattr(module.asyncio, 'wait_for', short_wait)
    monkeypatch.setattr(module, '_log', lambda *args, **kwargs: logs.append((args, kwargs)))
    if slow:
        assert asyncio.run(module.owned_thread(operation, 'synthetic-private-argument')) == 42
        assert len(logs) == 1
        assert 'synthetic-private-argument' not in repr(logs)
    else:
        with pytest.raises(TimeoutError, match='backend timeout'):
            asyncio.run(module.owned_thread(operation, 'synthetic-private-argument'))
        assert not logs


@pytest.mark.parametrize('status', ['completed', 'stopped', 'failed', 'unconfirmed'])
@pytest.mark.parametrize('cleaned', [False, True])
def test_terminal_reports_never_repeat_completed_native_cleanup(record, tmp_path, monkeypatch, status, cleaned):
    """Reporting cannot reopen native cleanup; unfinished cleanup still runs once."""
    store, row = record
    row = store.update(OWNER, row['id'], lambda current: current.update(status=status, phase='evaluate',
        cleanup_done=cleaned, observer_jobs=[{'operation':'f'*32, 'kind':'observer_full_range',
                                            'round':0, 'status':'planned', 'backend':None, 'started':False}]))
    backend = FakeBackend()
    controller = LoopController(tmp_path, store=store, backend=backend)
    reports = []

    async def observer(current):
        """Keep user-report processing observable independently of native cleanup."""
        reports.append(current['status'])

    monkeypatch.setattr(controller.observers, 'tick', observer)
    asyncio.run(controller.tick(OWNER, row['id']))
    assert len(backend.cleanups) == (0 if cleaned else 1)
    assert reports == [status]


def test_completed_inventory_dispatches_only_reports(record, tmp_path, monkeypatch):
    """A finished run with reports must not perform repeated foreground reads/writes."""
    store, row = record
    store.update(OWNER, row['id'], lambda current: current.update(status='completed', cleanup_done=True,
        observer_jobs=[{'operation':'f'*32, 'kind':'observer_full_range', 'round':0,
                        'status':'planned', 'backend':None, 'started':False}]))
    controller = LoopController(tmp_path, store=store, backend=FakeBackend())
    controller._evidence_retention_at = float('inf')

    async def exercise():
        """Run the real inventory scheduler until its independent report task is entered."""
        entered = asyncio.Event()

        async def forbidden(*_args, **_kwargs):
            """No foreground transition is needed after native cleanup completed."""
            raise AssertionError('Completed report-only Loop repeated foreground work')

        async def observer(current):
            """Receive current report context through the owned report claim."""
            assert current['status'] == 'completed'
            entered.set()

        monkeypatch.setattr(controller, 'tick', forbidden)
        monkeypatch.setattr(controller.observers, 'tick', observer)
        controller.start()
        try:
            await asyncio.wait_for(entered.wait(), 3)
        finally:
            await controller.close()
        assert not controller.backend.cleanups

    asyncio.run(exercise())
