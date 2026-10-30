"""Offline fixed configuration delivery and enforcement across Loop providers."""
import asyncio
import copy
import json

import pytest

from pb8_loop_ai_v2 import validate_decision
from tests.test_ai_loop_windows import run_record
from tests.test_pb8_loop_optimizer import OWNER, record
from tests.test_pb8_loop_review_fixes import EvidenceAI


PROVIDERS = [('chatgpt', 'codex'), ('claude-code', 'claude-code'),
             *[(provider, protocol) for provider in ('opencode-go', 'opencode-zen')
               for protocol in ('chat', 'responses', 'messages')]]


@pytest.mark.parametrize('provider,protocol', PROVIDERS)
@pytest.mark.parametrize('stage', ['bootstrap', 'evaluate'])
def test_every_provider_receives_exact_fixed_values_before_generation(record, monkeypatch, provider, protocol, stage):
    """Real snapshot orchestration delivers the immutable schema to every transport."""
    store, row = record
    row = store.update(OWNER, row['id'], lambda value: value['settings'].update(
        provider=provider, scenario_enabled=False, ai_budget_confirmed=True,
        ai_decision_token_limit=1_000_000, ai_decision_usd_limit=10.0))
    ai = EvidenceAI(store)
    captured = []
    monkeypatch.setattr('pb8_loop_archive.select_sources', lambda *_args, **_kwargs:
                        ([{'id': 'synthetic-advice'}], {}))

    async def preflight(*_args, **_kwargs):
        """Use explicit metadata without contacting a provider or reading runtime data."""
        return {'protocol': protocol, 'output_limit': 1000, 'loop_profile': row['ai_model_profile'],
                'cost': {'input': 1.0, 'output': 2.0}}

    async def request(_row, _metadata, content, _output):
        """Inspect the complete mandatory package at the actual dispatch boundary."""
        captured.append(copy.deepcopy(content))
        decision = {'variants': [{'config': copy.deepcopy(row['initial_config']), 'overrides': {}}]}
        if stage == 'evaluate':
            decision.update(winner_operation=None, finish=True, variants=[])
        return json.dumps(decision), {}

    ai.preflight = preflight
    ai._offline_request = request
    initial = copy.deepcopy(row['initial_config'])
    asyncio.run(ai.decide(row, stage, {'max_variants': 1}))
    schema = captured[0]['variant_config_schema']
    fixed = schema['properties']['backtest']['properties']
    assert fixed['start_date'] == {'const': initial['backtest']['start_date']}
    assert fixed['end_date'] == {'const': initial['backtest']['end_date']}
    assert fixed['scenarios'] == {'const': None}
    assert schema['properties']['pbgui'] == {'const': initial['pbgui']}
    assert 'const' not in schema['properties']['optimize']
    assert store.read(OWNER, row['id'])['initial_config'] == initial


@pytest.mark.parametrize('provider', ['chatgpt', 'claude-code', 'opencode-go', 'opencode-zen'])
@pytest.mark.parametrize('problem', ['date', 'scenario', 'provenance', 'omit_provenance'])
def test_shared_decision_gate_enforces_the_same_fixed_schema(tmp_path, provider, problem):
    """No provider can commit changed fixed values even without native schema support."""
    _, row = run_record(tmp_path)
    row['settings'].update(provider=provider, scenario_enabled=False)
    config = copy.deepcopy(row['initial_config'])
    if problem == 'date':
        config['backtest']['start_date'] = '1999-01-01'
    elif problem == 'scenario':
        config['backtest']['starting_balance'] = 999999
    elif problem == 'provenance':
        config['pbgui']['unreviewed'] = True
    else:
        del config['pbgui']
    decision = json.dumps({'variants': [{'config': config, 'overrides': {}}]})
    before = copy.deepcopy(row)
    with pytest.raises(ValueError, match='fixed Loop configuration contract'):
        validate_decision(row, 'bootstrap', {'max_variants': 1}, decision)
    assert row == before


@pytest.mark.parametrize('provider', ['chatgpt', 'claude-code', 'opencode-go', 'opencode-zen'])
def test_shared_contract_keeps_optimizer_search_editable(record, provider):
    """Unchanged fixed settings do not prevent ordinary permitted optimizer edits."""
    _, row = record
    row['settings'].update(provider=provider, scenario_enabled=False)
    candidate = copy.deepcopy(row['initial_config'])
    candidate['optimize']['n_cpus'] = 2
    candidate['optimize']['scoring'] = ['gain']
    decision = {'variants': [{'config': candidate, 'overrides': {}}]}
    assert validate_decision(row, 'bootstrap', {'max_variants': 1}, json.dumps(decision)) == decision


@pytest.mark.parametrize('provider', ['opencode-go', 'opencode-zen'])
@pytest.mark.parametrize('protocol', ['chat', 'responses', 'messages'])
def test_opencode_corrects_changed_fixed_values_in_the_same_dialog(record, provider, protocol):
    """All supported OpenCode transports repair a rejected config before returning it."""
    from pb8_loop_ai_policy import DecisionLedger
    from pb8_loop_config_contract import variant_config_schema
    from pb8_loop_dialog import OpenCodeDialog
    from pb8_loop_evidence import EvidenceSession, EvidenceStore, encode

    store, row = record
    row = store.update(OWNER, row['id'], lambda value: value['settings'].update(
        provider=provider, scenario_enabled=False, ai_budget_confirmed=True,
        ai_decision_token_limit=1_000_000, ai_decision_usd_limit=10.0))
    good = {'variants': [{'config': copy.deepcopy(row['initial_config']), 'overrides': {}}]}
    bad = copy.deepcopy(good)
    bad['variants'][0]['config']['backtest']['end_date'] = '1999-01-01'
    content = {'stage': 'bootstrap', 'variant_config_schema': variant_config_schema(row)}
    snapshot = EvidenceStore(store).create(row, content, {'stage': 'bootstrap'}, [])
    session = EvidenceSession(EvidenceStore(store), snapshot, 1500)
    metadata = {'cost': {'input': 1.0, 'output': 2.0}}
    ledger = DecisionLedger(store, row, 'bootstrap', snapshot['sha256'], metadata)
    sent = []

    async def send(body, _profile, _remaining):
        """Only complete native-shaped synthetic replies, without HTTP or accounts."""
        sent.append(copy.deepcopy(body))
        text = json.dumps(bad if len(sent) == 1 else good)
        return {
            'chat': {'choices': [{'finish_reason': 'stop', 'message': {'content': text}}]},
            'responses': {'status': 'completed', 'output': [
                {'type': 'message', 'content': [{'type': 'output_text', 'text': text}]}]},
            'messages': {'stop_reason': 'end_turn', 'content': [{'type': 'text', 'text': text}]},
        }[protocol]

    def validate(text):
        """Apply the production common decision gate inside the provider adapter."""
        return validate_decision(row, 'bootstrap', {'max_variants': 1}, text)

    answer = asyncio.run(OpenCodeDialog(row['settings'], {'protocol': protocol}, row['ai_model_profile'],
                                       session, ledger, send, validate).run('Use fixed values.', content, 1000))
    assert json.loads(answer) == good
    assert len(sent) == 2
    assert 'variant_config_schema' in encode(sent[0])
    assert 'fixed Loop configuration contract at backtest.end_date' in encode(sent[1])
    assert len(ledger.state()['calls']) == 2


def test_chatgpt_corrects_changed_fixed_values_before_commit(record, tmp_path, monkeypatch):
    """The real Codex stream reader sends contract feedback and commits only the correction."""
    import ai_chat
    from tests.test_pb8_loop_gpt_timeout import native_ai, stop_feeders

    store, row = record
    row = store.update(OWNER, row['id'], lambda value: value['settings'].update(scenario_enabled=False))
    good = {'winner_operation': None, 'finish': True, 'variants': [
        {'config': copy.deepcopy(row['initial_config']), 'overrides': {}}]}
    bad = copy.deepcopy(good)
    bad['variants'][0]['config']['backtest']['end_date'] = '1999-01-01'
    sent, replies = [], []

    async def feed(runtime):
        """Feed a rejected config, then its correction through native notifications."""
        replies.append(True)
        runtime.notifications.put_nowait({'method': 'item/completed', 'params': {'turnId': 'turn',
            'item': {'type': 'agentMessage', 'text': json.dumps(bad if len(replies) == 1 else good)}}})
        runtime.notifications.put_nowait({'method': 'turn/completed', 'params': {'turnId': 'turn',
            'turn': {'id': 'turn', 'status': 'completed'}}})

    ai, runtimes, feeders = native_ai(store, tmp_path, monkeypatch, feed)
    factory = ai_chat.CodexRuntime

    def capture_runtime(owner, root):
        """Capture ordinary native requests without starting a client process."""
        runtime = factory(owner, root)
        request = runtime.request

        async def capture(method, params=None, timeout=30):
            """Retain the emitted input and production correction feedback."""
            if method == 'turn/start':
                sent.append(copy.deepcopy(params))
            return await request(method, params, timeout)

        runtime.request = capture
        return runtime

    monkeypatch.setattr(ai_chat, 'CodexRuntime', capture_runtime)

    async def exercise():
        """Join every synthetic producer on success or failure."""
        try:
            return await ai.decide(row, 'evaluate', {})
        finally:
            await stop_feeders(feeders)

    assert asyncio.run(exercise()) == good
    assert len(sent) == 2
    assert 'variant_config_schema' in json.dumps(sent[0])
    assert 'fixed Loop configuration contract at backtest.end_date' in json.dumps(sent[1])
    assert store.read(OWNER, row['id'])['last_ai_decision']['decision'] == good
    assert all(runtime.close.await_count == 1 for runtime in runtimes)
