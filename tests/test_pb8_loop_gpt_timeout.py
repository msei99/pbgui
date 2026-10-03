"""Native Loop GPT requests use idle progress limits and the frozen run deadline."""
import asyncio
import json
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import ai_chat
import pb8_loop_ai
from ai_chat import CodexRuntime
from pb8_loop_ai import LoopAI, LoopAIRequestFailed, LoopAIRetryScheduled
from tests.test_pb8_loop_optimizer import OWNER, record


def native_ai(store, tmp_path, monkeypatch, feed=None, setup_timeout=False):
    """Exercise the real stream reader using a local fake RPC transport only."""
    runtimes, feeders = [], []

    def runtime_factory(owner, root):
        runtime = CodexRuntime(owner, root)
        runtime.close = AsyncMock()
        runtime._interrupt_turn_and_wait = AsyncMock()

        async def request(method, params=None, timeout=30):
            if method == 'thread/start':
                assert params['model'] == 'pinned'
                assert params['config']['web_search'] == 'disabled'
                return {'model': 'pinned', 'thread': {'id': 'thread'}}
            assert method == 'turn/start'
            assert params['model'] == 'pinned'
            if setup_timeout:
                raise TimeoutError()
            if feed is not None:
                feeders.append(asyncio.create_task(feed(runtime)))
            return {'turn': {'id': 'turn'}}

        runtime.request = request
        runtimes.append(runtime)
        return runtime

    monkeypatch.setattr(ai_chat, 'CodexRuntime', runtime_factory)
    monkeypatch.setattr(ai_chat, '_CHAT_TIMEOUT_SECONDS', .04)
    monkeypatch.setattr(ai_chat, '_CODEX_HIGH_EFFORT_TIMEOUT_SECONDS', .12)
    monkeypatch.setattr('ai_research.IDLE_TIMEOUT', .04)
    service = SimpleNamespace(_profile_runtime=lambda *_args: SimpleNamespace(root=tmp_path / 'codex'))

    class NativeAI(LoopAI):
        """Bypass account/model catalog IO while preserving the real decision transport."""
        async def preflight(self, *_args):
            return {'output_limit': 1000}

    return NativeAI(store, service), runtimes, feeders


async def stop_feeders(feeders):
    """Every test-owned stream producer is cancelled and joined."""
    for task in feeders:
        task.cancel()
    await asyncio.gather(*feeders, return_exceptions=True)


@pytest.mark.parametrize('method', ['item/reasoning/textDelta', 'item/agentMessage/delta'])
def test_progressive_gpt_response_exceeds_old_total_cap(record, tmp_path, monkeypatch, method):
    """Streaming activity may exceed 180 seconds without cancelling a healthy turn."""
    store, row = record
    real_wait_for, real_timeout = asyncio.wait_for, asyncio.timeout

    async def accelerated_wait_for(awaitable, timeout):
        return await real_wait_for(awaitable, timeout=timeout * .0005 if timeout and timeout > 100 else timeout)

    def accelerated_timeout(delay):
        return real_timeout(delay * .0005 if delay and delay > 100 else delay)

    # The previous 180-second outer wait_for now expires at .09 seconds;
    # the original Loop deadline remains proportionally longer.
    monkeypatch.setattr(asyncio, 'wait_for', accelerated_wait_for)
    monkeypatch.setattr(asyncio, 'timeout', accelerated_timeout)

    async def feed(runtime):
        for _index in range(12):
            await asyncio.sleep(.02)
            runtime.notifications.put_nowait({'method': method, 'params': {'turnId': 'turn',
                'delta': 'private-reasoning-marker' if 'reasoning' in method else ' '}})
        runtime.notifications.put_nowait({'method': 'item/completed', 'params': {'turnId': 'turn',
            'item': {'type': 'agentMessage', 'text': '{"reason":"accepted"}'}}})
        runtime.notifications.put_nowait({'method': 'turn/completed', 'params': {'turnId': 'turn',
            'turn': {'id': 'turn', 'status': 'completed'}}})

    ai, runtimes, feeders = native_ai(store, tmp_path, monkeypatch, feed)

    async def scenario():
        try:
            started = time.monotonic()
            assert await ai.decide(row, 'evaluate', {}) == {'reason': 'accepted'}
            assert time.monotonic() - started > .09
        finally:
            await stop_feeders(feeders)
    asyncio.run(scenario())
    saved = store.read(OWNER, row['id'])
    assert saved['last_ai_request']['outcome'] == 'completed'
    assert saved['last_ai_request']['progress_events'] >= 12
    assert saved['pending_ai'] is None and saved['ai_calls'] == 1
    assert 'private-reasoning-marker' not in json.dumps(saved['last_ai_request'])
    runtimes[0].close.assert_awaited()


@pytest.mark.parametrize('effort,seconds', [('', .04), ('high', .12)])
def test_gpt_idle_request_retries_with_nonsecret_activity_evidence(record, tmp_path, monkeypatch, effort, seconds):
    """A genuinely idle request still times out; high uses its longer idle allowance."""
    store, row = record
    row = store.update(OWNER, row['id'], lambda current: current['settings'].update(effort=effort))
    ai, runtimes, _feeders = native_ai(store, tmp_path, monkeypatch)
    with pytest.raises(LoopAIRetryScheduled, match=f'inactive for {seconds:g} seconds'):
        asyncio.run(ai.decide(row, 'evaluate', {}))
    saved = store.read(OWNER, row['id'])
    assert 'last activity: turn_started' in saved['ai_retry']['error']
    assert saved['ai_retry']['attempts'] == 1 and saved['pending_ai'] is None
    assert saved['last_ai_request']['phase'] == 'turn_started'
    assert saved['last_ai_request']['progress_events'] == 0
    assert saved['last_ai_request']['outcome'] == 'error'
    runtimes[0].close.assert_awaited()


def test_progress_cannot_exceed_the_original_loop_deadline(record, tmp_path, monkeypatch):
    """Continuous reasoning does not extend the authorized total hours or start another retry."""
    store, row = record
    row = store.update(OWNER, row['id'], lambda current: current.update(deadline=time.time() + 60.15))

    async def feed(runtime):
        while True:
            runtime.notifications.put_nowait({'method': 'item/reasoning/textDelta',
                'params': {'turnId': 'turn', 'delta': 'Reasoning'}})
            await asyncio.sleep(.015)

    ai, runtimes, feeders = native_ai(store, tmp_path, monkeypatch, feed)

    async def scenario():
        try:
            with pytest.raises(LoopAIRequestFailed, match='remaining Loop time allowance'):
                await ai.decide(row, 'evaluate', {})
        finally:
            await stop_feeders(feeders)
    asyncio.run(scenario())
    saved = store.read(OWNER, row['id'])
    assert saved['last_ai_request']['outcome'] == 'interrupted'
    assert saved['last_ai_request']['progress_events'] > 0
    assert saved.get('ai_retry') is None
    assert saved['ai_calls'] == 1
    runtimes[0].close.assert_awaited()


def test_turn_start_timeout_identifies_the_setup_phase(record, tmp_path, monkeypatch):
    """A 30-second RPC setup timeout must not be mislabeled as a total response timeout."""
    store, row = record
    ai, runtimes, _feeders = native_ai(store, tmp_path, monkeypatch, setup_timeout=True)
    with pytest.raises(LoopAIRetryScheduled, match='phase: starting_turn'):
        asyncio.run(ai.decide(row, 'evaluate', {}))
    saved = store.read(OWNER, row['id'])
    assert saved['last_ai_request']['outcome'] == 'setup_timeout'
    assert saved['last_ai_request']['phase'] == 'starting_turn'
    assert '180 seconds' not in saved['ai_retry']['error']
    runtimes[0].close.assert_awaited()
