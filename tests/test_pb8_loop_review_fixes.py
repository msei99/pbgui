"""Offline regressions for reviewed evidence identity, capability and adapter defects."""
import asyncio
import copy
import json
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from pb8_loop_ai_policy import DecisionLedger, ModelProfiles, profile_identity
from pb8_loop_evidence import EvidenceStore, LoopAIPause, encode, sha, tokens, required_content
from pb8_loop_controller import LoopController
from tests.loop_ai_transport import LoopAI
from tests.test_pb8_loop_optimizer import OWNER, RULE, FakeBackend, record


@pytest.fixture(autouse=True)
def forbid_real_service(monkeypatch):
    """Fail immediately if a regression test attempts to reach a real account."""
    monkeypatch.setattr('ai_chat.get_ai_chat_service', lambda: pytest.fail('Real provider access is forbidden'))


@pytest.mark.parametrize('entrypoint', ['decision', 'archive'])
def test_missing_context_settings_have_context_pause_reason(record, entrypoint):
    """Unavailable context settings do not demand a model capability approval."""
    from pb8_loop_ai_v2 import run_decision
    from pb8_loop_archive import archive_budget
    _store, row = record
    row.pop('ai_model_profile', None)
    with pytest.raises(LoopAIPause) as failure:
        if entrypoint == 'archive':
            archive_budget(row, row['initial_config'])
        else:
            ai = SimpleNamespace(preflight=AsyncMock(return_value={}))
            asyncio.run(run_decision(ai, row, 'select', {}))
    assert failure.value.reason == 'context_budget'


class EvidenceAI(LoopAI):
    """Real snapshot orchestration with an explicit local transport fixture."""

    async def preflight(self, *_args, **_kwargs):
        """Avoid catalogs, accounts and real model gates."""
        return {'output_limit': 1000, 'loop_profile': {'context': 4_000_000, 'page_tokens': 1500}}

    async def _request(self, *args):
        """Never fall back to a provider after the boundary wrapper restores a method."""
        return await self._offline_request(*args)


@pytest.mark.parametrize('change', ['jev', 'repair'])
def test_changed_evidence_reaches_model_and_gets_distinct_ledger(record, change):
    """A successful first selection must not hide JEV or controller repair feedback."""
    store, row = record
    ai = EvidenceAI(store)
    requests = []

    async def request(_row, _metadata, content, _output):
        """Capture only temporary mandatory input."""
        requests.append(copy.deepcopy(content))
        return '{"candidate_ids":["candidate"]}', {'tokens': {}}

    ai._offline_request = request
    controller = LoopController(store.root, store=store, backend=FakeBackend(), ai=ai)
    evidence = {'candidates': [{'id': 'candidate', 'optimizer_job': 'job', 'metrics': {'gain': 2}}],
                'max_candidates': 1, 'jev': []}

    async def exercise():
        """Run both requests in the same controller generation."""
        await controller.decision(row, 'select', evidence)
        first = store.read(OWNER, row['id'])
        first_snapshot = first['ai_snapshot_id']
        if change == 'jev':
            evidence['jev'] = ['New JEV evidence']
        else:
            first = store.update(OWNER, row['id'], lambda current: current.update(repair_error='Explain the selection'))
        await controller.decision(first, 'select', evidence)
        saved = store.read(OWNER, row['id'])
        assert saved['ai_snapshot_id'] != first_snapshot
        assert len(saved['ai_decisions']) == 2
        assert ('New JEV evidence' if change == 'jev' else 'Explain the selection') in encode(requests[1])
        await controller.decision(saved, 'select', evidence)
        assert len(requests) == 2

    asyncio.run(exercise())


def test_transport_retry_reuses_identical_snapshot_and_reservations(record):
    """Transport failure changes neither input identity nor uncertain spend."""
    store, row = record
    ai = EvidenceAI(store)
    count = 0

    async def request(*_args):
        """Fail once after the offline adapter reserves the call."""
        nonlocal count
        count += 1
        if count == 1:
            raise ConnectionError('offline fixture')
        return '{"candidate_ids":["candidate"]}', {'tokens': {}}

    ai._offline_request = request
    evidence = {'candidates': [{'id': 'candidate', 'metrics': {'gain': 1}}], 'max_candidates': 1}

    async def exercise():
        """Reconstruct the same request from its persisted state."""
        with pytest.raises(ConnectionError):
            await ai._decide_once(row, 'select', evidence)
        previous = store.read(OWNER, row['id'])
        await ai._decide_once(previous, 'select', evidence)
        saved = store.read(OWNER, row['id'])
        assert saved['ai_snapshot_id'] == previous['ai_snapshot_id']
        assert len(saved['ai_decisions']) == 1
        assert len(next(iter(saved['ai_decisions'].values()))['calls']) == 2

    asyncio.run(exercise())


def test_evidence_revision_cannot_reset_paid_budget(record):
    """Different input identities share the authorized round/stage spending ceiling."""
    store, row = record
    row = store.update(OWNER, row['id'], lambda current: current['settings'].update(
        provider='opencode-zen', ai_budget_confirmed=True, ai_decision_token_limit=3000, ai_decision_usd_limit=1.))
    metadata = {'cost': {'input': 1., 'output': 2.}}
    first = DecisionLedger(store, row, 'select', 'old-input', metadata)
    first.timeout(first.reserve(1000, 1000))
    second = DecisionLedger(store, row, 'select', 'jev-input', metadata)
    assert second.key != first.key
    with pytest.raises(LoopAIPause) as failure:
        second.reserve(1000, 1000)
    assert failure.value.reason == 'ai_budget'


def test_winner_reason_is_checked_inside_adapter(record):
    """A missing reason is corrected before the controller can reject the winner."""
    from pb8_loop_ai_v2 import validate_decision
    with pytest.raises(ValueError, match='explain'):
        validate_decision(record[1], 'evaluate', {'winner_options': {'winner': {}}},
                          '{"winner_operation":"winner","finish":true}')


@pytest.mark.parametrize('context', [256000, 475000, 1000000])
@pytest.mark.parametrize('effort', ['', 'high', 'xhigh'])
def test_any_large_context_model_needs_no_acceptance(record, context, effort):
    """New models/reasoning selections work without a file or per-model gate."""
    store, row = record
    selection = {**row['settings'], 'provider': 'opencode-go', 'model': 'new-model-' + str(context),
                 'effort': effort, 'profile_owner': OWNER}
    value = ModelProfiles(store.root).load(selection, {'context': context, 'protocol': 'chat'})
    assert value['context'] == context and value['context_confirmed']
    assert not (store.root / 'model_profiles').exists()
    assert not (store.root / 'model_runtime').exists()


@pytest.mark.parametrize('context', [None, 0, 255999])
def test_actual_provider_context_below_minimum_is_rejected(record, context):
    """Only an actual context constraint, not a missing approval, blocks a model."""
    store, row = record
    with pytest.raises(LoopAIPause) as failure:
        ModelProfiles(store.root).load({**row['settings'], 'provider': 'opencode-go'}, {'context': context})
    assert failure.value.reason == 'context_budget'


def test_runtime_cache_is_optional_and_does_not_require_acceptance(record):
    """Normal telemetry creates its own cache and can expand the planning floor."""
    store, row = record
    profiles = ModelProfiles(store.root)
    selection = {**row['settings'], 'profile_owner': OWNER}
    pending = profiles.load(selection, {})
    assert pending['context'] == 256000 and pending['context_pending']
    profiles.observe_window(selection, {}, 475000)
    assert profiles.load(selection, {})['context'] == 475000
    other = profiles.load({**selection, 'profile_owner': 'b'*32}, {})
    assert other['context_pending'], 'One owner must not inherit another account context'


@pytest.mark.parametrize('reported', [475000, 128000, None])
def test_context_is_learned_from_normal_decision_without_probe(record, tmp_path, monkeypatch, reported):
    """Real chat/reader code learns context from the decision, with no test turn."""
    import ai_chat
    from pb8_loop_ai import LoopAI as ProductionAI
    runtime_class = ai_chat.CodexRuntime
    store, row = record
    runtimes, sent = [], []

    def factory(owner, root):
        """Only replace the RPC boundary; never start a real provider process."""
        runtime = runtime_class(owner, root)
        runtime.close = AsyncMock()
        runtime._interrupt_turn_and_wait = AsyncMock()
        runtime.start = AsyncMock(side_effect=AssertionError('Real subprocess startup forbidden'))
        async def request(method, params, timeout=30):
            """Supply native telemetry and the ordinary selection response."""
            if method == 'thread/start':
                assert len(params['dynamicTools']) == 1
                return {'model': 'pinned', 'thread': {'id': 'normal'}}
            assert method == 'turn/start'
            message = params['input'][0]['text']
            assert json.loads(message)['stage'] == 'select'
            assert message != 'Reply OK'
            sent.append(message)
            events = []
            if reported is not None:
                events.append({'method': 'thread/tokenUsage/updated', 'params': {'threadId': 'normal',
                    'tokenUsage': {'last': {'totalTokens': 200}, 'modelContextWindow': reported}}})
            events.extend([
                {'method': 'item/completed', 'params': {'turnId': 'turn', 'item': {
                    'type': 'agentMessage', 'text': '{"candidate_ids":["candidate"]}'}}},
                {'method': 'turn/completed', 'params': {'turnId': 'turn', 'turn': {'id': 'turn', 'status': 'completed'}}},
            ])
            reader = asyncio.StreamReader()
            reader.feed_data(('\n'.join(json.dumps(event) for event in events) + '\n').encode())
            reader.feed_eof()
            runtime.process = SimpleNamespace(stdout=reader, returncode=None)
            await runtime._read_stdout()
            return {'turn': {'id': 'turn'}}
        runtime.request = request
        runtimes.append(runtime)
        return runtime

    monkeypatch.setattr(ai_chat, 'CodexRuntime', factory)
    chat = SimpleNamespace(_validate_provider_model=AsyncMock(return_value={}),
                           _selected_reasoning_variant=lambda *_: None,
                           _validate_model_service_tier=lambda *_: None,
                           _profile_runtime=lambda *_: SimpleNamespace(root=tmp_path / 'codex'))
    ai = ProductionAI(store, chat=chat)
    async def exercise():
        """Even uncached start/preview creates no separate runtime or model call."""
        for check_budget in [False, True]:
            metadata = await ai.preflight(OWNER, row['settings'], check_budget=check_budget)
            assert metadata['loop_profile']['context_pending']
            assert not runtimes
        evidence = {'candidates': [{'id': 'candidate', 'metrics': {'gain': 2}}], 'max_candidates': 1}
        if reported == 475000:
            assert (await ai.decide(row, 'select', evidence))['candidate_ids'] == ['candidate']
        else:
            with pytest.raises(LoopAIPause) as failure:
                await ai.decide(row, 'select', evidence)
            assert failure.value.reason == 'context_budget'
    asyncio.run(exercise())
    assert len(sent) == 1 and len(runtimes) == 1
    runtimes[0].close.assert_awaited_once()
    runtimes[0].start.assert_not_awaited()
    saved = store.read(OWNER, row['id'])
    assert bool(saved.get('last_ai_decision')) == (reported == 475000)
    assert not saved['jobs']
    if reported == 475000:
        assert ai.profiles.load({**row['settings'], 'profile_owner': OWNER}, {})['context'] == 475000


def test_first_optimizer_uses_interpreted_rubric(record, monkeypatch):
    """The first native archive bound sees the new goal schema rather than []."""
    from tests.test_pb8_loop_optimizer import FakeAI
    import pb8_loop_archive
    store, row = record
    seen = []
    original = pb8_loop_archive.archive_budget

    def budget(current, config):
        """Inspect the actual first-job allocation input."""
        seen.append(current['rubric'])
        return original(current, config)

    monkeypatch.setattr(pb8_loop_archive, 'archive_budget', budget)
    controller = LoopController(store.root, store=store, backend=FakeBackend(), ai=FakeAI())
    asyncio.run(controller.tick(OWNER, row['id']))
    assert seen == [RULE]
    assert store.read(OWNER, row['id'])['jobs'][0]['optimizer_archive']['effective'] > 0


def test_bootstrap_reserves_opencode_initial_context_before_selecting_source(record):
    """A source fitting only without the initial reserve must fall back to fresh work."""
    from pb8_loop_archive import select_sources
    from pb8_loop_ai import instructions_for_run
    from pb8_loop_ai_v2 import initial_payload, PROTOCOL
    store, row = record
    row['settings']['provider'] = 'opencode-go'
    candidate = {'id': 'candidate', 'optimizer_job': 'job', 'source': {'run': 'source'},
                 'config': {'large': 'test-value-' * 6000}, 'metrics': {'gain': 2}}
    content = {'stage': 'bootstrap', 'initial_config': {}, 'rubric': RULE,
               'evidence': {'existing_results': [candidate]}}
    required = required_content(content)
    instructions = instructions_for_run(row)['text'] + PROTOCOL
    wire = lambda value: initial_payload(row['settings'], {'protocol': 'chat'}, {}, instructions, value, 1000)
    full_cost = tokens(wire(required))
    empty = copy.deepcopy(content)
    empty['evidence']['existing_results'] = []
    empty_cost = tokens(wire(required_content(empty)))
    profile = {'context': 256000, 'page_tokens': 5000, 'additional_context_tokens':
               int((256000 * .9 - 1000 - 512) / 1.25 - (full_cost + empty_cost) / 2)}
    selected, preview = select_sources(row, content, profile, 1000)
    assert selected == [] and preview['fallback_fresh']
    assert preview['in_b'] == 1


def test_stream_progress_never_serializes_accumulated_output(monkeypatch):
    """Thousands of text deltas do not repeatedly encode the growing transcript."""
    import pb8_loop_dialog as dialog
    monkeypatch.setattr(dialog, 'encode', lambda *_: pytest.fail('No whole-state JSON encoding in feed'))
    accumulator = dialog.StreamAccumulator('chat')
    for _ in range(10000):
        assert accumulator.feed({'choices': [{'delta': {'content': 'abcdefgh'}}]})
    assert not accumulator.feed({'choices': [{'delta': {}}]})
    accumulator.done = True
    assert accumulator.finish()['choices'][0]['message']['content'] == 'abcdefgh' * 10000


def test_complete_streamed_malformed_arguments_are_not_incomplete_transport():
    """A terminal Messages call with invalid JSON remains a correctable native call."""
    from pb8_loop_dialog import StreamAccumulator, response_parts
    accumulator = StreamAccumulator('messages')
    accumulator.feed({'type': 'content_block_start', 'index': 0,
                      'content_block': {'type': 'tool_use', 'id': 'bad', 'name': 'loop_evidence', 'input': {}}})
    accumulator.feed({'type': 'content_block_delta', 'index': 0,
                      'delta': {'partial_json': '{broken'}})
    accumulator.feed({'type': 'content_block_stop', 'index': 0})
    accumulator.feed({'type': 'message_stop'})
    _, calls, _ = response_parts('messages', accumulator.finish())
    assert calls == [{'id': 'bad', 'name': 'loop_evidence', 'arguments': '{broken'}]


def test_opencode_exhausted_contract_corrections_have_recovery_reason(record):
    """Invalid final answers do not falsely blame context capacity."""
    from pb8_loop_dialog import OpenCodeDialog
    from tests.test_pb8_loop_dialog_v2 import frozen, profile
    store, row = record
    row = store.update(OWNER, row['id'], lambda current: current['settings'].update(provider='opencode-go'))
    session = frozen((store, row))
    ledger = DecisionLedger(store, row, 'select', session.snapshot['sha256'], {})

    async def send(*_args):
        """Return complete but contract-invalid JSON on every fake HTTP round."""
        return {'choices': [{'message': {'role': 'assistant', 'content': '{"invalid":true}'}}]}

    def validate(_text):
        """Simulate a genuine bounded decision-contract repair."""
        raise ValueError('Missing candidate_ids')

    with pytest.raises(LoopAIPause) as failure:
        asyncio.run(OpenCodeDialog(row['settings'], {'protocol': 'chat'}, profile(), session, ledger, send, validate)
                    .run('Read', session.snapshot['required'], 1000))
    assert failure.value.reason == 'evidence_recovery'
    assert len(ledger.state()['calls']) == 6


@pytest.mark.parametrize('protocol', ['chat', 'responses', 'messages'])
def test_malformed_tool_json_returns_native_error_and_continues(record, protocol):
    """The model can correct a malformed complete call without losing its decision."""
    from pb8_loop_dialog import OpenCodeDialog
    from tests.test_pb8_loop_dialog_v2 import frozen, profile
    store, row = record
    row = store.update(OWNER, row['id'], lambda current: current['settings'].update(provider='opencode-go'))
    session = frozen((store, row))
    ledger = DecisionLedger(store, row, 'select', session.snapshot['sha256'], {})
    requests = []

    async def send(body, *_args):
        """One malformed call, followed by a valid final reply."""
        requests.append(copy.deepcopy(body))
        if len(requests) == 1:
            if protocol == 'chat':
                return {'choices': [{'finish_reason': 'tool_calls', 'message': {'role': 'assistant', 'tool_calls': [
                    {'id': 'bad-call', 'function': {'name': 'loop_evidence', 'arguments': '{broken'}}]}}]}
            if protocol == 'responses':
                return {'output': [{'type': 'function_call', 'call_id': 'bad-call', 'name': 'loop_evidence', 'arguments': '{broken'}]}
            return {'content': [{'type': 'tool_use', 'id': 'bad-call', 'name': 'loop_evidence', 'input': '{broken'}]}
        assert 'bad-call' in encode(body) and 'invalid_evidence_request' in encode(body)
        if protocol == 'chat':
            return {'choices': [{'message': {'role': 'assistant', 'content': '{"ok":true}'}}]}
        if protocol == 'responses':
            return {'output': [{'type': 'message', 'content': [{'type': 'output_text', 'text': '{"ok":true}'}]}]}
        assert body['messages'][-1]['content'][0]['is_error'] is True
        assert body['messages'][-2]['content'][0]['input'] == {}
        return {'content': [{'type': 'text', 'text': '{"ok":true}'}]}

    assert json.loads(asyncio.run(OpenCodeDialog(row['settings'], {'protocol': protocol}, profile(), session, ledger, send)
                                 .run('Read', session.snapshot['required'], 1000))) == {'ok': True}
    assert len(requests) == 2 and session.total_calls == 1


def test_native_sources_are_skipped_from_cached_index(record, monkeypatch):
    """Large advice archives are parsed once, rather than at every AI decision."""
    import pb8_loop_evidence as evidence_module
    store, row = record
    evidence = EvidenceStore(store)
    snapshot = evidence.create(row, {}, {'stage': 'bootstrap'}, [
        {'id': 'native_source:fixture', 'loop': row['id'], 'candidates': [{'large': 'x' * 10000}]}])
    native = snapshot['history']['native_source:fixture']['key'] + '.json'
    original = evidence_module.read_regular_file_nofollow
    reads = []

    def read(path, root):
        """Count original archive reads, excluding the compact cache marker."""
        if path.name == native and path.parent.name == 'evidence':
            reads.append(path)
        return original(path, root)

    monkeypatch.setattr(evidence_module, 'read_regular_file_nofollow', read)
    assert evidence.findings(row) == []
    assert evidence.findings(row) == []
    assert len(reads) == 1


def test_terminal_retention_expires_snapshots_but_keeps_interrupted_proof(record):
    """Fourteen-day expiry is safe even when the ended loop has no new decisions."""
    from secure_files import atomic_write_private_text
    store, row = record
    evidence = EvidenceStore(store)
    snapshots = [evidence.create(row, {}, {'stage': 'select'}, []) for _ in range(2)]
    for snapshot in snapshots:
        snapshot['created_at'] = time.time() - 15 * 86400
        snapshot.pop('sha256')
        snapshot['sha256'] = sha(snapshot)
        atomic_write_private_text(store.owner_dir(OWNER) / 'snapshots' / (snapshot['id'] + '.json'), json.dumps(snapshot))
    row = store.update(OWNER, row['id'], lambda current: current.update(status='completed',
                       interrupted_ai={'snapshot_id': snapshots[1]['id']}))
    evidence.cleanup(row)
    directory = store.owner_dir(OWNER) / 'snapshots'
    assert not (directory / (snapshots[0]['id'] + '.json')).exists()
    assert (directory / (snapshots[1]['id'] + '.json')).exists()


def test_controller_sweeps_even_finished_loops(record, monkeypatch):
    """The registered scan performs retention before skipping completed controllers."""
    store, row = record
    store.update(OWNER, row['id'], lambda current: current.update(status='completed', cleanup_done=True))
    controller = LoopController(store.root, store=store, backend=FakeBackend())
    cleaned = []

    def cleanup(_self, current):
        """Stop after the first isolated scan."""
        cleaned.append(current['id'])
        controller._closing = True

    async def sleep(_seconds):
        """Do not delay an offline scan test."""

    monkeypatch.setattr(EvidenceStore, 'cleanup', cleanup)
    monkeypatch.setattr('pb8_loop_controller.asyncio.sleep', sleep)
    asyncio.run(controller._run())
    assert cleaned == [row['id']]


def test_retention_storage_failure_does_not_block_running_loops(record, monkeypatch):
    """Optional expiry failure never prevents normal controller scheduling."""
    store, row = record
    controller = LoopController(store.root, store=store, backend=FakeBackend())
    controller.tick = AsyncMock()
    original_sleep = asyncio.sleep

    def cleanup(*_args):
        """Simulate an unavailable snapshot disk, without touching any real files."""
        raise OSError('isolated fixture')

    async def sleep(_seconds):
        """Let the scheduled controller step execute, then end this scan."""
        await original_sleep(0)
        controller._closing = True

    monkeypatch.setattr(EvidenceStore, 'cleanup', cleanup)
    monkeypatch.setattr('pb8_loop_controller.asyncio.sleep', sleep)

    async def exercise():
        """Release the registered scan/step tasks deterministically."""
        await controller._run()
        await controller.close()

    asyncio.run(exercise())
    controller.tick.assert_awaited_once_with(OWNER, row['id'])
