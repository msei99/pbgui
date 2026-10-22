"""Offline regressions for recoverable optional-evidence limits in real adapters."""
import asyncio
import json

import pytest

from pb8_loop_ai_policy import DecisionLedger
from pb8_loop_dialog import OpenCodeDialog
from pb8_loop_evidence import EvidenceSession, EvidenceStore, LoopAIPause, encode
from tests.test_pb8_loop_optimizer import OWNER, record


def session_at_limit(record, *, global_limit=False):
    """Create temporary evidence with a realistic reserved mandatory-page boundary."""
    store, row = record
    evidence = EvidenceStore(store)
    snapshot = evidence.create(row, {'stage': 'select', 'values': list(range(2500))},
                               {'stage': 'select', 'values': list(range(128))}, [])
    session = EvidenceSession(evidence, snapshot, 1500)
    if global_limit:
        session.total_calls = 124 - len(session.required_pages)
    else:
        session.calls = 60 - len(session.required_pages)
        session.total_calls = session.calls
    return session


@pytest.mark.parametrize('global_limit', [False, True])
def test_parallel_denials_preserve_read_reserve_and_retry_identity(record, global_limit):
    """A denied batch uses no data quota, opens no data, and retries idempotently."""
    from concurrent.futures import ThreadPoolExecutor
    session = session_at_limit(record, global_limit=global_limit)
    before = session.calls, session.total_calls
    charged = []
    session.charge = lambda: charged.append(True)
    session.store.resolve = lambda *_args: pytest.fail('Denied optional data must not be loaded')
    args = {'path': '/data/values/0', 'offset': 0}
    with ThreadPoolExecutor(max_workers=8) as pool:
        replies = list(pool.map(lambda i: session.read(args, f'parallel-{i}'), range(20)))
    assert all(reply['error'] == 'optional_evidence_budget_reserved' for reply in replies)
    assert session.calls == before[0] and session.total_calls == before[1] and not charged
    assert session.total_budget_notices == 20
    assert session.read(args, 'parallel-0') == replies[0]
    assert session.total_budget_notices == 20
    with pytest.raises(ValueError, match='reused'):
        session.read({'path': '/data/values/1', 'offset': 0}, 'parallel-0')


@pytest.mark.parametrize('global_limit', [False, True])
def test_compaction_recovers_every_required_page_after_optional_notice(record, global_limit):
    """Both per-dialog and decision-wide reserves remain usable after compaction."""
    session = session_at_limit(record, global_limit=global_limit)
    session.compacted()
    notice = session.read({'path': '/data', 'offset': 0}, 'optional')
    assert not notice['can_finalize']
    assert notice['required_reads'] == [{'path': '/required', 'offset': p['offset']}
                                        for p in session.required_pages]
    assert session.missing()
    for index, args in enumerate(notice['required_reads']):
        session.read(args, f'recover-{index}')
    assert not session.missing()
    assert session.calls <= 64 and session.total_calls <= 128


def test_notices_do_not_replenish_limits_across_dialogs_or_ignore_repeated_abuse(record):
    """Normal notices are recoverable, but infinite requests cannot bypass existing bounds."""
    session = session_at_limit(record, global_limit=True)
    session.read({'path': '/data', 'offset': 0}, 'first')
    previous_total = session.total_calls
    session.start_dialog()
    reply = session.read({'path': '/data', 'offset': 0}, 'next-dialog')
    assert reply['error'] == 'optional_evidence_budget_reserved'
    assert session.total_calls == previous_total and session.total_budget_notices == 2
    for i in range(126):
        session.read({'path': '/data', 'offset': 0}, f'ignored-{i}')
    with pytest.raises(LoopAIPause, match='repeatedly ignored'):
        session.read({'path': '/data', 'offset': 0}, 'ignored-over-limit')


@pytest.mark.parametrize('protocol', ['chat', 'responses', 'messages'])
def test_opencode_finishes_in_same_dialog_after_optional_notice(record, protocol):
    """All real OpenCode serializers return a normal tool reply, not a fatal recovery."""
    store, row = record
    session = session_at_limit(record)
    ledger = DecisionLedger(store, row, 'select', session.snapshot['sha256'], {'protocol': protocol})
    calls = []

    async def send(body, profile, remaining):
        """Use a fake provider with genuine native-shaped tool traffic and no network."""
        calls.append(body)
        usage = {'input_tokens': 20, 'output_tokens': 10}
        if len(calls) == 1:
            args = {'path': '/data', 'offset': 0}
            return {
                'chat': {'usage': usage, 'choices': [{'finish_reason': 'tool_calls', 'message': {
                    'role': 'assistant', 'tool_calls': [{'id': 'optional', 'type': 'function', 'function': {
                        'name': 'loop_evidence', 'arguments': encode(args)}}]}}]},
                'responses': {'usage': usage, 'status': 'completed', 'output': [{'type': 'function_call',
                    'call_id': 'optional', 'name': 'loop_evidence', 'arguments': encode(args)}]},
                'messages': {'usage': usage, 'stop_reason': 'tool_use', 'content': [{'type': 'tool_use',
                    'id': 'optional', 'name': 'loop_evidence', 'input': args}]}}[protocol]
        assert 'optional_evidence_budget_reserved' in encode(body)
        return {
            'chat': {'usage': usage, 'choices': [{'finish_reason': 'stop', 'message': {
                'role': 'assistant', 'content': '{"ok":true}'}}]},
            'responses': {'usage': usage, 'status': 'completed', 'output': [{'type': 'message',
                'content': [{'type': 'output_text', 'text': '{"ok":true}'}]}]},
            'messages': {'usage': usage, 'stop_reason': 'end_turn', 'content': [
                {'type': 'text', 'text': '{"ok":true}'}]}}[protocol]

    profile = {'context': 256000, 'page_tokens': 1500, 'output_tokens': 1000,
               'streaming': False, 'call_timeout': 1}
    result = asyncio.run(OpenCodeDialog(row['settings'], {'protocol': protocol}, profile,
                                       session, ledger, send).run('Select a candidate.', session.snapshot['required'], 1000))
    assert json.loads(result) == {'ok': True}
    assert len(calls) == 2 and session.dialog == 0 and session.total_budget_notices == 1


@pytest.mark.parametrize('compact', [False, True])
def test_native_codex_commits_decision_after_budget_notice_without_restarting_dialog(record, tmp_path, monkeypatch, compact):
    """The real Codex stream/tool bridge finishes selection rather than pausing the Loop."""
    import ai_chat
    from tests.test_pb8_loop_gpt_timeout import native_ai, stop_feeders
    store, row = record
    notices = []

    async def feed(runtime):
        """Read distinct optional values until reserve protection replies normally."""
        await asyncio.sleep(.01)
        for index in range(64):
            reply = await runtime.tool_handler({'tool': 'loop_evidence', 'threadId': 'thread', 'turnId': 'turn',
                'callId': f'optional-{index}', 'arguments': {'path': f'/data/evidence/values/{index}', 'offset': 0}})
            assert reply['success']
            value = json.loads(reply['contentItems'][0]['text'])
            if value.get('error') == 'optional_evidence_budget_reserved':
                notices.append(value)
                break
        assert notices
        if compact:
            runtime.notifications.put_nowait({'method': 'thread/compacted', 'params': {'threadId': 'thread'}})
            await asyncio.sleep(.01)
            offset = 0
            while offset is not None:
                reply = await runtime.tool_handler({'tool': 'loop_evidence', 'threadId': 'thread', 'turnId': 'turn',
                    'callId': f'recover-{offset}', 'arguments': {'path': '/required', 'offset': offset}})
                assert reply['success']
                offset = json.loads(reply['contentItems'][0]['text'])['next_offset']
        runtime.notifications.put_nowait({'method': 'item/completed', 'params': {'turnId': 'turn',
            'item': {'type': 'agentMessage', 'text': '{"winner_operation":null,"finish":true}'}}})
        runtime.notifications.put_nowait({'method': 'turn/completed', 'params': {'turnId': 'turn',
            'turn': {'id': 'turn', 'status': 'completed'}}})

    ai, runtimes, feeders = native_ai(store, tmp_path, monkeypatch, feed)
    monkeypatch.setattr(ai_chat, '_CHAT_TIMEOUT_SECONDS', 5)
    monkeypatch.setattr(ai_chat, '_CODEX_HIGH_EFFORT_TIMEOUT_SECONDS', 5)

    async def exercise():
        """Always cancel and join test-owned producers even if an assertion fails."""
        try:
            result = await ai.decide(row, 'evaluate', {'values': list(range(128))})
            assert result == {'winner_operation': None, 'finish': True}
        finally:
            await stop_feeders(feeders)

    asyncio.run(exercise())
    saved = store.read(OWNER, row['id'])
    assert saved.get('last_ai_decision') and len(runtimes) == 1
    assert saved['ai_evidence_status']['optional_read_budget_notices'] == 1
    assert saved['ai_evidence_status']['tool_calls'] <= 64
    assert all(runtime.close.await_count == 1 for runtime in runtimes)
