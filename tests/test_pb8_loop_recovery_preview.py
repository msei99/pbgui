"""Offline phase-aware AI recovery previews and API event-loop responsiveness."""
import asyncio
import json
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import loop_optimizer_v8 as api
from tests.test_pb8_loop_optimizer import OWNER, RULE, record


@pytest.mark.parametrize('phase', ['interpret', 'bootstrap', 'baseline', 'optimize', 'select', 'validate', 'evaluate', 'holdout'])
@pytest.mark.parametrize('advice', [False, True])
def test_preview_scans_historical_sources_only_for_initial_planning(record, monkeypatch, phase, advice):
    """Later resumes retain settings and budget checks without unrelated archive scans."""
    store, row = record
    store.update(OWNER, row['id'], lambda value: value.update(phase=phase, status='paused', rubric=RULE))
    original = store.read(OWNER, row['id'])
    original['settings']['historical_advice'] = advice
    store.write(original)
    profile = {'context':1000000, 'streaming':True, 'output_tokens':32768}
    preflight = AsyncMock(return_value={'loop_profile':profile, 'output_limit':32768})
    scans, selections, budget_threads = [], [], []
    main_thread = threading.get_ident()

    def candidates(staged):
        """Observe discovery without accessing real optimizer results."""
        scans.append(staged['phase'])
        return [{'id':'fixture'}]

    def sources(staged, content, actual_profile, output, **kwargs):
        """Keep the original whole-source selection boundary for startup phases."""
        selections.append((content['stage'], content['evidence']['existing_results']))
        return [], {'mode':'auto', 'sources':[], 'in_a':0, 'in_b':1}

    def budget(staged, config):
        """Fail if CPU budget work runs on the API event loop."""
        budget_threads.append(threading.get_ident())
        assert staged['settings']['historical_advice'] is advice
        assert staged['ai_model_profile'] == profile
        assert not {'jobs', 'history', 'ai_decisions', 'observations'} & staged.keys()
        return {'effective':539}

    monkeypatch.setattr(api, '_owner', lambda _: OWNER)
    monkeypatch.setattr(api, '_controller', SimpleNamespace(store=store, ai=SimpleNamespace(preflight=preflight),
                                                         backend=SimpleNamespace(existing_candidates=candidates)))
    monkeypatch.setattr('pb8_loop_archive.archive_budget', budget)
    monkeypatch.setattr('pb8_loop_archive.select_sources', sources)
    response = asyncio.run(api.ai_execution_preview(row['id'], api.LoopAction(action='resume'), session=object()))
    value = json.loads(response.body)
    needed = advice and phase in {'interpret','bootstrap'}
    assert bool(scans) is needed and bool(selections) is needed
    assert budget_threads and budget_threads[0] != main_thread
    assert value['historical_sources']['mode'] == ('auto' if needed else 'not_applicable' if advice else 'off')
    assert value['native_archive']['effective'] == 539
    preflight.assert_awaited_once()
    assert preflight.await_args.kwargs == {'check_budget':False}
    saved = store.read(OWNER, row['id'])
    assert saved['status']=='paused' and saved['phase']==phase
    assert saved['settings']==original['settings'] and saved['jobs']==original['jobs']
    assert saved['ai_calls']==original['ai_calls']
    assert saved['ai_preview']==value
    # Reopening the unchanged preview must not rewrite the growing archive or
    # invalidate report caches and paused-controller identity stamps.
    def no_rewrite(*args, **kwargs):
        """Fail if an identical read-only preview changes persistent run identity."""
        pytest.fail('Unchanged preview rewrote the run')

    monkeypatch.setattr(store, 'update', no_rewrite)
    repeated = asyncio.run(api.ai_execution_preview(row['id'], api.LoopAction(action='resume'), session=object()))
    assert json.loads(repeated.body) == value


@pytest.mark.parametrize('ending', ['error', 'cancel'])
def test_preview_timing_task_closes_before_the_route_returns(record, monkeypatch, ending):
    """Failed or abandoned previews cannot retain a timing task after their route ends."""
    from fastapi import HTTPException
    store, row = record
    monkeypatch.setattr(api, '_owner', lambda _: OWNER)

    async def exercise():
        """Hold the metadata read so cancellation happens inside the real route."""
        entered = asyncio.Event()
        release = asyncio.Event()

        async def preflight(*args, **kwargs):
            """Reject locally without invoking a model or touching real runtime data."""
            entered.set()
            await release.wait()
            raise ValueError('Fixture metadata rejected')

        monkeypatch.setattr(api, '_controller', SimpleNamespace(store=store, ai=SimpleNamespace(preflight=preflight)))
        task = asyncio.create_task(api.ai_execution_preview(row['id'], api.LoopAction(action='resume'), session=object()))
        await asyncio.wait_for(entered.wait(), 3)
        if ending == 'cancel':
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            release.set()
            with pytest.raises(HTTPException):
                await task
        assert not [item for item in asyncio.all_tasks() if item.get_name() == 'loop-recovery-preview-timing']

    asyncio.run(exercise())
