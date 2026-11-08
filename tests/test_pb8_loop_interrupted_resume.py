"""Offline recovery of paused, unconfirmed AI calls without refunding usage."""
import asyncio
import copy
import fcntl
import json
import os
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from api import loop_optimizer_v8 as api
from pb8_loop_controller import LoopController
from tests.test_pb8_loop_optimizer import FakeBackend, OWNER, RULE, record


@pytest.mark.parametrize('provider', ['chatgpt', 'claude-code', 'opencode-go'])
@pytest.mark.parametrize('reservation_state', ['pending', 'interrupted', 'both'])
def test_acknowledged_resume_retires_open_call_once_and_advances(record, monkeypatch, provider, reservation_state):
    """The first resume handles legacy pending state and preserves all prior billing."""
    store, row = record
    reservation = {'id': 'unconfirmed-call', 'stage': 'interpret', 'tokens_reserved': 321,
                   'usd_reserved': 0.5, 'snapshot_id': 'retained-original'}
    ledger = {'calls': [{'id': reservation['id'], 'tokens': 321, 'usd': 0.5, 'usage_confirmed': False}],
              'tokens': 321, 'usd': 0.5}

    def paused(current):
        """Represent pause/shutdown before the controller classifies the open call."""
        current.update(status='paused', ai_calls=7, ai_tokens_reserved=321, ai_usd_reserved=0.5,
                       ai_decisions={'retained': copy.deepcopy(ledger)},
                       pending_ai=reservation if reservation_state in {'pending', 'both'} else None,
                       interrupted_ai=reservation if reservation_state in {'interrupted', 'both'} else None)
        current['settings'].update(provider=provider, historical_advice=False)

    store.update(OWNER, row['id'], paused)
    original = store.read(OWNER, row['id'])
    ai = SimpleNamespace(preflight=AsyncMock(return_value={}),
                         decide=AsyncMock(return_value={'rubric': RULE, 'reason': 'Fixture decision'}))
    controller = LoopController(store.root, store=store, backend=FakeBackend(), ai=ai)
    monkeypatch.setattr(api, '_controller', controller)
    # The actual partial polling read must expose acknowledgement on the first attempt.
    display = store.read(OWNER, row['id'], fields=api._DISPLAY_FIELDS)
    assert api.projection(display)['has_interrupted_ai'] is True
    with pytest.raises(HTTPException) as rejected:
        asyncio.run(api._loop_action_owned(OWNER, row['id'], api.LoopAction(action='resume')))
    assert rejected.value.status_code == 409
    assert store.read(OWNER, row['id']) == original
    ai.preflight.assert_not_awaited()
    ai.decide.assert_not_awaited()

    response = asyncio.run(api._loop_action_owned(
        OWNER, row['id'], api.LoopAction(action='resume', acknowledge_interrupted=True)))
    assert json.loads(response.body)['status'] == 'running'
    current = store.read(OWNER, row['id'])
    assert current['pending_ai'] is None and not current.get('interrupted_ai')
    assert current['interrupted_ai_audit'] == [reservation]
    assert current['ai_decisions'] == original['ai_decisions']
    assert current['ai_calls'] == 7 and current['ai_tokens_reserved'] == 321 and current['ai_usd_reserved'] == 0.5
    assert current['deadline'] == original['deadline'] and current['jobs'] == original['jobs']
    assert current['control_generation'] == original['control_generation'] + 1
    assert api.projection(current)['has_interrupted_ai'] is False
    # Run the real controller transition: the stale pending check must not pause again.
    asyncio.run(controller.tick(OWNER, row['id']))
    ai.decide.assert_awaited_once()
    progressed = store.read(OWNER, row['id'])
    assert progressed['status'] == 'running' and progressed['phase'] == 'optimize'


def test_resume_cannot_retire_a_request_while_its_controller_is_still_active(record, monkeypatch):
    """Acknowledge only after the old owned step has fully left its cross-process lock."""
    store, row = record
    monkeypatch.setattr(api, 'RESUME_LOCK_WAIT_SECONDS', 0)
    store.update(OWNER, row['id'], lambda current: current.update(
        status='paused', pending_ai={'id': 'still-stopping', 'stage': 'interpret'}))
    original = store.read(OWNER, row['id'])
    ai = SimpleNamespace(preflight=AsyncMock(return_value={}))
    monkeypatch.setattr(api, '_controller', LoopController(store.root, store=store, backend=FakeBackend(), ai=ai))
    fd = os.open(store.path(OWNER, row['id']).with_suffix('.controller'), os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(HTTPException) as rejected:
            asyncio.run(api._loop_action_owned(
                OWNER, row['id'], api.LoopAction(action='resume', acknowledge_interrupted=True)))
        assert rejected.value.status_code == 409
        assert 'still stopping' in rejected.value.detail['message']
        assert store.read(OWNER, row['id']) == original
    finally:
        os.close(fd)


def test_live_running_request_is_not_presented_as_interrupted(record):
    """Ordinary in-flight calls must not be reported as interrupted in polling output."""
    store, row = record
    store.update(OWNER, row['id'], lambda current: current.update(
        status='running', pending_ai={'id': 'working', 'stage': 'interpret'}))
    assert api.projection(store.read(OWNER, row['id'], fields=api._DISPLAY_FIELDS))['has_interrupted_ai'] is False


@pytest.mark.parametrize('outcome', ['resume', 'cancel', 'stop'])
def test_resume_waits_for_transient_lock_and_preserves_newer_control(record, monkeypatch, outcome):
    """Short contention waits safely; cancellation/Stop cannot leave a delayed resume behind."""
    store, row = record
    store.update(OWNER, row['id'], lambda current: current.update(status='paused'))
    original = store.read(OWNER, row['id'])
    ai = SimpleNamespace(preflight=AsyncMock(return_value={}))
    monkeypatch.setattr(api, '_controller', LoopController(store.root, store=store, backend=FakeBackend(), ai=ai))
    monkeypatch.setattr(api, 'RESUME_LOCK_WAIT_SECONDS', 2)
    fd = os.open(store.path(OWNER, row['id']).with_suffix('.controller'), os.O_CREAT | os.O_RDWR, 0o600)
    real_flock = fcntl.flock
    real_flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    released = False

    def release():
        """Release the synthetic controller only when the tested transition allows it."""
        nonlocal released
        if not released:
            os.close(fd)
            released = True

    async def exercise():
        """Coordinate actual flock contention without timing-dependent sleeps."""
        loop = asyncio.get_running_loop()
        busy = asyncio.Event()
        def observed_flock(target, operation):
            """Notify the test when resume really encounters the held claim."""
            try:
                return real_flock(target, operation)
            except BlockingIOError:
                loop.call_soon_threadsafe(busy.set)
                raise
        monkeypatch.setattr(api.fcntl, 'flock', observed_flock)
        task = asyncio.create_task(api._loop_action_owned(OWNER, row['id'], api.LoopAction(action='resume')))
        try:
            await asyncio.wait_for(busy.wait(), 3)
            assert store.read(OWNER, row['id'])['status'] == 'paused'
            if outcome == 'cancel':
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
                release()
                assert store.read(OWNER, row['id']) == original
            elif outcome == 'stop':
                store.update(OWNER, row['id'], lambda current: current.update(
                    status='stopping', control_generation=current['control_generation'] + 1))
                release()
                with pytest.raises(HTTPException):
                    await asyncio.wait_for(task, 3)
                current = store.read(OWNER, row['id'])
                assert current['status'] == 'stopping'
                assert current['control_generation'] == original['control_generation'] + 1
            else:
                release()
                response = await asyncio.wait_for(task, 3)
                assert json.loads(response.body)['status'] == 'running'
                current = store.read(OWNER, row['id'])
                assert current['control_generation'] == original['control_generation'] + 1
                assert current['jobs'] == original['jobs'] and current['ai_calls'] == original['ai_calls']
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
    try:
        asyncio.run(exercise())
    finally:
        release()
