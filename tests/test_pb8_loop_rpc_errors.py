"""RPC rejections at the round boundary keep safe causes and bounded retries."""
import asyncio
import json
from types import SimpleNamespace

import pytest

from ai_chat import AIChatError, CodexRuntime
from pb8_loop_ai import LoopAIRetryScheduled, LoopAIRequestFailed, _transient_ai_error
from tests.test_pb8_loop_ai_retries import SequenceAI, clock
from tests.test_pb8_loop_optimizer import OWNER, record


async def rpc_failure(tmp_path, error):
    """Read a synthetic rejected RPC through the real runtime stream reader."""
    runtime = CodexRuntime(OWNER, tmp_path / 'codex')
    stream = asyncio.StreamReader()
    stream.feed_data((json.dumps({'id': 'request', 'error': error}) + '\n').encode())
    stream.feed_eof()
    runtime.process = SimpleNamespace(stdout=stream, returncode=None)
    future = asyncio.get_running_loop().create_future()
    runtime.pending['request'] = future
    try:
        await runtime._read_stdout()
        await future
    except AIChatError as exc:
        return exc
    finally:
        runtime.pending.clear()
        runtime.process = None
        await runtime.close()
    raise AssertionError('The rejected RPC must fail its pending request')


@pytest.mark.parametrize('error,expected,retry', [
    ({'code': -32603, 'message': 'private-secret'}, 'temporary server error', True),
    ({'code': -32000, 'message': 'network timeout private-secret'}, 'connection', True),
    ({'code': -32000, 'message': 'rate limit private-secret'}, 'rate limit', True),
    ({'code': -32000, 'message': 'private-secret'}, 'runtime request failed', True),
    ({'code': -32000, 'message': 'token expired private-secret'}, 'authentication', False),
    ({'code': -32000, 'codexErrorInfo': 'usageLimitExceeded'}, 'usage limit', False),
    ({'code': -32000, 'message': 'model not supported private-secret'}, 'selected model', False),
    ({'code': -32000, 'codexErrorInfo': 'contextWindowExceeded'}, 'context limit', False),
    ({'code': -32602, 'message': 'Input exceeds the maximum length of 1048576 characters.'},
     'runtime input limit', False),
    *[({'code': code, 'message': 'private-secret'}, 'format or parameters', False)
      for code in (-32700, -32600, -32601, -32602)],
])
def test_rpc_reader_preserves_safe_retry_category(tmp_path, error, expected, retry):
    """Transient failures retry; account/context/protocol failures stay permanent."""
    exc = asyncio.run(rpc_failure(tmp_path, error))
    assert expected in str(exc)
    assert 'private-secret' not in str(exc)
    assert (_transient_ai_error(exc) is not None) is retry


def test_second_round_rpc_rejection_retries_selection_only(record, clock, tmp_path):
    """A rejected select request preserves finished work and resumes the same round."""
    store, row = record
    completed = {'kind': 'optimizer', 'operation': 'completed-first-round',
                 'status': 'complete', 'backend': {'id': 'existing-job', 'execution': 'vast'}}
    row = store.update(OWNER, row['id'], lambda current: current.update(
        round=1, phase='select', jobs=[completed]))
    error = asyncio.run(rpc_failure(tmp_path, {'code': -32000, 'message': 'private-secret'}))
    ai = SequenceAI(store, [error, '{"candidate_ids":["retained-candidate"]}'])
    with pytest.raises(LoopAIRetryScheduled):
        asyncio.run(ai.decide(row, 'select', {'candidates': [{'id': 'retained-candidate'}], 'max_candidates': 1}))
    waiting = store.read(OWNER, row['id'])
    assert waiting['status'] == 'running' and waiting['errors'] == 0
    assert waiting['round'] == 1 and waiting['phase'] == 'select'
    assert waiting['jobs'] == [completed] and not waiting.get('cleanup_done')
    assert waiting['pending_ai'] is None and waiting['ai_calls'] == 1
    with pytest.raises(LoopAIRetryScheduled):
        asyncio.run(ai.decide(waiting, 'select', {'candidates': [{'id': 'retained-candidate'}], 'max_candidates': 1}))
    assert ai.sent == 1
    clock[0] += 30
    result = asyncio.run(ai.decide(waiting, 'select', {'candidates': [{'id': 'retained-candidate'}], 'max_candidates': 1}))
    assert result == {'candidate_ids': ['retained-candidate']}
    recovered = store.read(OWNER, row['id'])
    assert recovered['jobs'] == [completed] and recovered['round'] == 1
    assert recovered['ai_calls'] == 2 and recovered['pending_ai'] is None
    assert not recovered.get('ai_retry')


def test_unclassified_rpc_failure_exhausts_existing_retry_limit(record, clock, tmp_path):
    """Unknown RPC failures cannot create an unlimited sequence of provider calls."""
    store, row = record
    error = asyncio.run(rpc_failure(tmp_path, {'code': -32000, 'message': 'private-secret'}))
    ai = SequenceAI(store, [error, error, error])
    for delay in (30, 60):
        with pytest.raises(LoopAIRetryScheduled):
            asyncio.run(ai.decide(store.read(OWNER, row['id']), 'select', {}))
        clock[0] += delay
    with pytest.raises(LoopAIRequestFailed, match='Failed after 3 attempts'):
        asyncio.run(ai.decide(store.read(OWNER, row['id']), 'select', {}))
    assert ai.sent == 3
    assert store.read(OWNER, row['id'])['ai_retry']['exhausted']


def test_select_compacts_prior_rounds_and_preserves_every_candidate_metric(record, monkeypatch):
    """Round-two selection stays below the real RPC limit without a ranked shortlist."""
    from copy import deepcopy
    from pb8_loop_ai import compact_evaluation_content

    store, row = record
    metrics = {f'long_native_metric_name_{i}': float(i) for i in range(400)}
    metrics.update(gain_strategy_eq=2.5, drawdown_worst_strategy_eq=.49)
    candidates = [{'id': f'candidate_{i}', 'optimizer_job': 'finished-job',
                   'metrics': deepcopy(metrics)} for i in range(35)]
    candidates[1]['metrics'].pop('long_native_metric_name_0')
    candidates[2]['metrics']['long_native_metric_name_0'] = None
    knowledge = [{'observations': [{'metrics': deepcopy(metrics)} for _ in range(8)]}
                 for _ in range(12)]
    monkeypatch.setattr(store, 'context', lambda _record: deepcopy(knowledge))
    row = store.update(OWNER, row['id'], lambda current: current.update(round=1, phase='select'))
    ai = SequenceAI(store, ['{"candidate_ids":["candidate_0"]}'])
    captured = []
    request = ai._request

    async def capture(record, metadata, content, output_tokens):
        """Observe only the isolated test's real serialized selection payload."""
        captured.append(content)
        return await request(record, metadata, content, output_tokens)

    monkeypatch.setattr(ai, '_request', capture)
    result = asyncio.run(ai.decide(row, 'select', {'candidates': candidates, 'max_candidates': 6}))
    assert result['candidate_ids'] == ['candidate_0']
    wire = captured[0]
    assert len(json.dumps(knowledge)) > 1048576
    assert len(json.dumps(wire)) < 1048576
    from pb8_loop_evidence import EvidenceStore
    saved = store.read(OWNER, row['id'])
    snapshot = EvidenceStore(store).load(OWNER, saved['ai_snapshot_id'])
    assert snapshot['data']['evidence']['candidates'] == candidates
    assert 'long_native_metric_name_0' not in wire['evidence']['candidates_metrics']['columns']
    aliases = [alias['id'] for item in wire['evidence']['candidates'] for alias in item.get('aliases', [item])]
    assert set(aliases) == {item['id'] for item in candidates}
    assert candidates[0]['metrics'] == metrics
