"""Offline transient AI retries preserve jobs, reservations and lifecycle controls."""
import asyncio
import copy
import json
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import aiohttp
import pytest

from ai_chat import AIChatError, AIChatService
import pb8_loop_ai
from pb8_loop_ai import LoopAI, LoopAIRequestFailed, LoopAIRetryScheduled, LoopAITransientError
from pb8_loop_controller import LoopController
from tests.test_pb8_loop_optimizer import OWNER, record


@pytest.fixture
def clock(monkeypatch):
    """Advance durable retry deadlines without sleeping or making live requests."""
    value = [time.time()]
    monkeypatch.setattr(pb8_loop_ai.time, 'time', lambda: value[0])
    return value


class SequenceAI(LoopAI):
    """Use real reservations and retries with a deterministic isolated transport."""

    def __init__(self, store, outcomes):
        super().__init__(store)
        self.outcomes = iter(outcomes)
        self.sent = 0

    async def preflight(self, owner, settings):
        """Return a small native-shaped model contract without runtime IO."""
        return {'output_limit': 1000}

    async def _request(self, *args):
        """Count only attempts that actually reach the provider boundary."""
        self.sent += 1
        value = next(self.outcomes)
        if isinstance(value, BaseException):
            raise value
        return value, {'status': 'subscription', 'tokens': None}


def test_timeout_retries_after_pause_and_preserves_completed_jobs(record, clock):
    """A cooldown survives reconstruction and reuses all prior exact results."""
    store, row = record
    row = store.update(OWNER, row['id'], lambda current: current.update(jobs=[
        {'operation': 'saved-result', 'kind': 'validation', 'status': 'complete'},
    ]))
    original = copy.deepcopy(row)
    ai = SequenceAI(store, [TimeoutError()])
    with pytest.raises(LoopAIRetryScheduled, match='Retry 2/3 in 30 seconds'):
        asyncio.run(ai.decide(row, 'evaluate', {}))
    scheduled = store.read(OWNER, row['id'])
    assert scheduled['status'] == 'running' and scheduled['errors'] == 0
    assert scheduled['pending_ai'] is None and scheduled['ai_calls'] == 1
    assert scheduled['ai_retry']['retry_at'] == clock[0] + 30
    assert scheduled['ai_retry']['history'][0]['at'] == clock[0]
    assert scheduled['jobs'] == original['jobs']
    reserved = scheduled['ai_tokens_reserved']
    recovered = SequenceAI(store, ['{}'])
    with pytest.raises(LoopAIRetryScheduled):
        asyncio.run(recovered.decide(scheduled, 'evaluate', {}))
    assert recovered.sent == 0
    clock[0] += 30
    assert asyncio.run(recovered.decide(store.read(OWNER, row['id']), 'evaluate', {})) == {}
    complete = store.read(OWNER, row['id'])
    assert complete['ai_calls'] == 2 and complete['ai_tokens_reserved'] >= 2 * reserved
    assert complete['pending_ai'] is None and 'ai_retry' not in complete
    assert complete['jobs'] == original['jobs'] and complete['initial_config'] == original['initial_config']


def test_three_timeouts_exhaust_retries_with_a_visible_reason(record, clock):
    """Repeated timeouts cannot reset the allowance or send a fourth request."""
    store, row = record
    ai = SequenceAI(store, [TimeoutError(), TimeoutError(), TimeoutError()])
    for delay, message in [(30, 'Retry 2/3'), (60, 'Retry 3/3')]:
        with pytest.raises(LoopAIRetryScheduled, match=message):
            asyncio.run(ai.decide(store.read(OWNER, row['id']), 'evaluate', {}))
        clock[0] += delay
    with pytest.raises(LoopAIRequestFailed, match='timed out.*Failed after 3 attempts'):
        asyncio.run(ai.decide(store.read(OWNER, row['id']), 'evaluate', {}))
    failed = store.read(OWNER, row['id'])
    assert failed['ai_retry']['exhausted'] and failed['pending_ai']
    assert failed['ai_calls'] == ai.sent == 3
    with pytest.raises(ValueError, match='interrupted'):
        asyncio.run(ai.decide(failed, 'evaluate', {}))
    assert ai.sent == 3


@pytest.mark.parametrize('error', [
    aiohttp.ClientConnectionError('test-only disconnect'),
    aiohttp.ClientPayloadError('test-only truncated payload'),
    AIChatError('ChatGPT encountered a temporary server error. Try again later.'),
    AIChatError('The connection to ChatGPT failed or timed out. Try again when the connection is available.'),
    AIChatError('ChatGPT rate limit reached. Wait before sending another request.'),
])
def test_transient_transport_and_chatgpt_failures_schedule_retry(record, clock, error):
    """The account selected by the user receives the same bounded retry policy."""
    store, row = record
    ai = SequenceAI(store, [error])
    with pytest.raises(LoopAIRetryScheduled):
        asyncio.run(ai.decide(row, 'evaluate', {}))
    assert store.read(OWNER, row['id'])['ai_retry']['attempts'] == 1


@pytest.mark.parametrize('message', [
    'ChatGPT authentication failed. Reconnect your ChatGPT account.',
    'ChatGPT usage limit reached. Check your account usage and reset time; Free accounts also have limits.',
    'The selected model is unavailable or not permitted for this ChatGPT account. Select another available model.',
])
def test_permanent_account_failures_do_not_retry(record, message):
    """Login, account quota and model permissions require correcting the selection."""
    store, row = record
    ai = SequenceAI(store, [AIChatError(message)])
    with pytest.raises(LoopAIRequestFailed, match=message.split('.')[0]):
        asyncio.run(ai.decide(row, 'evaluate', {}))
    assert ai.sent == 1 and not store.read(OWNER, row['id']).get('ai_retry')


def test_retry_honors_server_delay_and_remaining_loop_time(record, clock):
    """Retry-After cannot extend the run's existing time authorization."""
    store, row = record
    ai = SequenceAI(store, [LoopAITransientError('HTTP 429', retry_after=120)])
    with pytest.raises(LoopAIRetryScheduled, match='in 120 seconds'):
        asyncio.run(ai.decide(row, 'evaluate', {}))
    assert store.read(OWNER, row['id'])['ai_retry']['retry_at'] == clock[0] + 120
    row = store.update(OWNER, row['id'], lambda value: value.update(pending_ai=None, ai_retry=None, deadline=clock[0] + 80))
    ai = SequenceAI(store, [TimeoutError()])
    with pytest.raises(LoopAIRequestFailed, match='remaining Loop time allowance'):
        asyncio.run(ai.decide(row, 'evaluate', {}))
    assert ai.sent == 1 and store.read(OWNER, row['id'])['ai_retry']['exhausted']


@pytest.mark.parametrize('status', ['paused', 'stopping', 'stopped'])
def test_pause_and_stop_cancel_automatic_retry(record, clock, status):
    """The next scheduled attempt cannot bypass a newer lifecycle action."""
    store, row = record
    ai = SequenceAI(store, [TimeoutError(), '{}'])
    with pytest.raises(LoopAIRetryScheduled):
        asyncio.run(ai.decide(row, 'evaluate', {}))
    store.update(OWNER, row['id'], lambda value: value.update(status=status, control_generation=value['control_generation'] + 1))
    clock[0] += 30
    with pytest.raises(ValueError, match='paused or stopped'):
        asyncio.run(ai.decide(row, 'evaluate', {}))
    assert ai.sent == 1


def test_controller_waits_without_failure_then_records_exhaustion(record, clock, monkeypatch):
    """Cooldown ticks neither count as errors nor create another optimizer cycle."""
    store, row = record
    ai = SequenceAI(store, [TimeoutError(), TimeoutError(), TimeoutError()])
    controller = LoopController(store.root, store=store, backend=object(), ai=ai)
    controller.observers = SimpleNamespace(tick=AsyncMock())
    logs = []
    monkeypatch.setattr('pb8_loop_controller._log', lambda *args, **kwargs: logs.append(kwargs))

    async def evaluate(current):
        """Exercise the controller failure boundary with the real decision wrapper."""
        await ai.decide(current, 'evaluate', {})

    monkeypatch.setattr(controller, '_tick', evaluate)
    for delay in (30, 60):
        asyncio.run(controller.tick(OWNER, row['id']))
        current = store.read(OWNER, row['id'])
        assert current['status'] == 'running' and current['errors'] == 0
        clock[0] += delay
    asyncio.run(controller.tick(OWNER, row['id']))
    current = store.read(OWNER, row['id'])
    assert current['status'] == 'failed' and current['errors'] == 1
    assert 'Failed after 3 attempts' in current['last_error'] == current['reason']
    assert ai.sent == 3
    failure = current['failure']
    assert failure['at'] == clock[0] and failure['stage'] == 'evaluate'
    assert failure['round'] == row['round'] and failure['model'] == row['settings']['model']
    assert failure['error_type'] == 'LoopAIRequestFailed' and failure['attempts'] == 3
    assert [item['attempt'] for item in failure['history']] == [1, 2, 3]
    assert [item['retry_delay'] for item in failure['history']] == [30, 60, None]
    assert all('timed out' in item['error'] for item in failure['history'])
    assert logs[-1]['meta']['failure'] == failure
    from api.loop_optimizer_v8 import projection
    assert projection(current)['failure'] == failure


def test_direct_job_failure_keeps_reason_and_timestamp_through_cleanup(record, clock):
    """Non-AI failure transitions also retain their cause before later cleanup."""
    store, row = record
    controller = LoopController(store.root, store=store, backend=SimpleNamespace(cleanup=lambda row: None))
    failed = controller.commit(row, lambda current: current.update(
        status='failed', reason='Starting baseline backtest failed', last_error='Native process exited with code 1'))
    original = copy.deepcopy(failed['failure'])
    assert original['reason'] == failed['reason'] and original['detail'] == failed['last_error']
    assert original['at'] == clock[0] and original['attempts'] is None
    clock[0] += 30
    asyncio.run(controller._tick(failed))
    cleaned = store.read(OWNER, row['id'])
    assert cleaned['cleanup_done'] and cleaned['failure'] == original


def test_controller_cooldown_returns_before_jobs_or_decisions(record, clock):
    """Normal scans keep the same cycle while a persisted retry deadline is pending."""
    store, row = record
    row = store.update(OWNER, row['id'], lambda value: value.update(ai_retry={'retry_at': clock[0] + 30}))
    controller = LoopController(store.root, store=store, backend=object())
    asyncio.run(controller._tick(row))
    assert store.read(OWNER, row['id'])['jobs'] == row['jobs']


@pytest.mark.parametrize('status,message,retry', [
    (408, 'request failed', True), (429, 'too many requests', True),
    (500, 'server error', True), (502, 'server error', True),
    (503, 'server error', True), (504, 'server error', True),
    (401, 'unauthorized', False), (403, 'unauthorized', False),
    (400, 'invalid request', False), (404, 'model not found', False),
    (429, 'monthly limit reached', False), (503, 'invalid api key', False),
])
def test_native_http_failures_distinguish_transient_and_permanent(record, status, message, retry):
    """Real bounded response parsing distinguishes retryable status from quota/auth."""
    store, row = record
    row['settings'].update(provider='opencode-go')
    closed = []

    class Response:
        """An isolated aiohttp-shaped context with no credentials or sockets."""
        headers = {'Retry-After': '120'}

        def __init__(self):
            self.status = status
            self.content = self

        async def __aenter__(self):
            """Expose the fake provider response."""
            return self

        async def __aexit__(self, *args):
            """Verify context cleanup for both accepted and rejected statuses."""
            closed.append(True)

        async def iter_chunked(self, size):
            """Supply a small bounded error body to the production safe parser."""
            yield json.dumps({'error': {'message': message}}).encode()

    chat = SimpleNamespace(
        credentials=SimpleNamespace(load_go_key=lambda owner: 'test-only-key'),
        _http_session=AsyncMock(return_value=SimpleNamespace(post=lambda *args, **kwargs: Response())),
        _read_json_response=AIChatService._read_json_response,
    )
    ai = LoopAI(store, chat)
    with pytest.raises(LoopAITransientError if retry else AIChatError) as exc:
        asyncio.run(ai._request(row, {'protocol': 'chat'}, {}, 1000))
    if retry:
        assert exc.value.retry_after == 120 and f'HTTP {status}' in str(exc.value)
    assert closed == [True]
