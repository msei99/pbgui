"""Fresh continuation time limits preserve parent state and current rental bounds."""
import asyncio
import copy
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from api import loop_optimizer_v8 as api
from tests.test_pb8_loop_optimizer import OWNER, record, _warm_start_observation, valid_gpu_config


@pytest.mark.parametrize('hours', [0, -.1, 168.1, float('nan'), float('inf')])
def test_invalid_continuation_hours_are_rejected(hours):
    """HTTP request validation rejects invalid time limits before any owned action."""
    with pytest.raises(ValidationError):
        api.LoopContinue(hours=hours, authorization=True)


@pytest.mark.parametrize('hours,rental_hours,expected', [(None, 12, 12), (6.5, 12, 6.5),
                                                     (24, 24, 24), (13, 12, None)])
def test_continue_respects_current_rental_limit_without_silent_explicit_clamp(record, monkeypatch, hours, rental_hours, expected):
    """A deliberate new duration may grow; an excessive explicit value creates no run."""
    store, row = record
    initial = valid_gpu_config()
    row = store.update(OWNER, row['id'], lambda current:current.update(initial_config=initial, comparison=initial['backtest']))
    checkpoint = _warm_start_observation(row, 2, 1)
    checkpoint['operation'] = 'checkpoint'

    def ready(current):
        """Create one verified synthetic source without launching any native process."""
        current.update(best=checkpoint, jobs=[{'operation':'checkpoint','kind':'validation','status':'completed'}])
        current['settings'].update(execution='vast', hours=18)

    parent = store.update(OWNER, row['id'], ready)
    parent = copy.deepcopy(parent)
    captured = []

    async def start(body, session, definition):
        """Capture the effective authorization using only temporary queue storage."""
        captured.append(body.model_dump())
        child = store.create(OWNER, body.model_dump(), definition['bundle']['config'], {}, {}, [], {}, queued=True)
        return JSONResponse({'id':child['id']}, status_code=201)

    async def action(*args):
        """No real Loop, GPU, provider or native job is started by this fixture."""
        return None

    exact = {'metrics':checkpoint['metrics'], 'assessment':checkpoint['assessment']}
    monkeypatch.setattr(api, '_controller', SimpleNamespace(store=store, backend=SimpleNamespace(observations=lambda *_:exact)))
    monkeypatch.setattr(api, '_owner', lambda _:OWNER)
    monkeypatch.setattr(api, '_start_loop', start)
    monkeypatch.setattr(api, 'loop_action', action)
    monkeypatch.setattr(api, 'projection', lambda value:{'id':value['id']})
    monkeypatch.setattr('vast_queue.CloudQueue', lambda:SimpleNamespace(read=lambda:{'gpu_preferences':{'hours':rental_hours, 'budget':5}}))
    body = api.LoopContinue(rounds=25, hours=hours, authorization=True)
    if expected is None:
        with pytest.raises(HTTPException) as error:
            asyncio.run(api.continue_loop(row['id'], body, None))
        assert error.value.status_code == 422
        assert 'Maximum rental hours (12)' in error.value.detail
        assert not captured and len(store.list(OWNER)) == 1
    else:
        assert asyncio.run(api.continue_loop(row['id'], body, None)).status_code == 201
        assert captured[0]['hours'] == expected
        assert captured[0]['max_runs'] == captured[0]['max_rounds'] == 25
        assert captured[0]['max_validations'] == parent['settings']['max_validations']
    assert store.read(OWNER, row['id']) == parent
