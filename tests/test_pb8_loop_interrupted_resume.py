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
