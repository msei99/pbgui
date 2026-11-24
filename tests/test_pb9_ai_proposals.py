"""Native optimizer suggestions use the existing explicit one-use review bridge."""
import asyncio
import copy
import json
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace

import pytest
from unittest.mock import AsyncMock

from ai_capabilities import AICapabilityService, AICapabilityError
from secure_files import atomic_write_private_text

OWNER = 'b' * 32
CID = 'c' * 32


@pytest.mark.parametrize('research_only,continuation_error', [(False, False), (True, False), (False, True)])
def test_native_approved_save_continues_only_unrestricted_chat(environment, monkeypatch, research_only, continuation_error):
    """A real approved save resumes PB9 planning, without restoring research permissions."""
    from api import ai as route
    caps, api, config, path, writes = environment
    chat = SimpleNamespace(start_turn=AsyncMock(return_value={'status': 'queued'},
        side_effect=RuntimeError('isolated continuation failure') if continuation_error else None))
    monkeypatch.setattr(route, 'get_ai_capability_service', lambda: caps)
    monkeypatch.setattr(route, 'get_ai_chat_service', lambda: chat)
    monkeypatch.setattr(route, '_owner', lambda _session: OWNER)

    async def run():
        receipt = await caps.dispatch(OWNER, CID, 'propose_pb9_optimizer_config',
            {'name': 'continued_native', 'config': caps._sanitize_config(config)})
        item = caps.proposals[receipt['proposal_id']]
        if research_only:
            caps.restrict_to_analysis(OWNER, CID)
        reviewed = await caps.reviewed_changes.review(OWNER, item.id, CID, item.payload_digest)
        response = await route.approve_proposal(item.id, route.ProposalDecisionRequest(
            payload_digest=item.payload_digest, conversation_id=CID, review_token=reviewed['review_token']), object())
        result = json.loads(response.body)
        assert result['status'] == 'executed'
        assert len(writes) == 1
        if research_only:
            assert 'continuation' not in result
            chat.start_turn.assert_not_awaited()
            assert caps.analysis_only(OWNER, CID)
        else:
            assert result['continuation']['status'] == ('not_started' if continuation_error else 'queued')
            chat.start_turn.assert_awaited_once()
            args = chat.start_turn.await_args.args
            assert args[:2] == (OWNER, CID)
            assert 'continued_native' in args[2] and '"version":"v9"' in args[2]
            assert 'separately reviewed Vast proposal' in args[2]
            assert args[-1] is True
            assert not caps.analysis_only(OWNER, CID)
    asyncio.run(run())


@pytest.fixture
def environment(tmp_path, monkeypatch):
    """Keep sources, durable proposals and writes strictly in temporary storage."""
    import ai_capabilities
    monkeypatch.setattr(ai_capabilities, 'PBGDIR', str(tmp_path))
    caps = AICapabilityService(tmp_path / 'ai')
    config = json.loads((Path(__file__).parent / 'fixtures/pb9_optimize_metadata.json').read_text())['template']
    config['pbgui'] = {'note': 'must survive'}
    root = tmp_path / 'opt9'
    path = root / 'fixture/config.json'
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(config))
    writes = []
    def write(destination, value):
        """Record only canonical native saves, with no queue or remote capability."""
        destination.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_private_text(destination, json.dumps(value))
        writes.append(destination)
    api = SimpleNamespace(_config_lock=lambda: nullcontext(), _configs_dir=lambda: root,
        _config_file=lambda name: root / name / 'config.json',
        prepare_pb8_config=lambda value: copy.deepcopy(value),
        _normalize_config=lambda value, name: {**value, 'backtest': {**value.get('backtest', {}), 'base_dir': 'backtests/pbgui/' + name}},
        native=SimpleNamespace(load=lambda destination: json.loads(destination.read_text()), write=write))
    api._save_config_bundle = lambda name, value, **kw: write(api._config_file(name), value)
    monkeypatch.setattr(caps, '_optimizer_api', lambda version: api if version == 'v9' else pytest.fail('PB8 gateway called'))
    monkeypatch.setattr(caps, '_runtime_fingerprint', lambda version: {'installed': True, 'state_digest': 'fixture'})
    return caps, api, config, path, writes


def test_native_patch_requires_review_and_preserves_sparse_source(environment):
    """A proposal is inert; one explicit owner-bound review permits only a config save."""
    caps, api, config, path, writes = environment
    async def run():
        receipt = await caps.dispatch(OWNER, CID, 'propose_reviewed_config_change', {
            'kind': 'optimizer', 'version': 'v9', 'name': 'fixture',
            'operations': [{'op': 'replace', 'path': '/bot/global/risk/max_positions', 'value': 3}]})
        proposal = next(item for item in await caps.list_proposals(OWNER, CID) if item['proposal_id'] == receipt['proposal_id'])
        assert not writes
        review = await caps.reviewed_changes.review(OWNER, proposal['proposal_id'], CID, proposal['payload_digest'])
        await caps.approve(OWNER, proposal['proposal_id'], proposal['payload_digest'], CID, review_token=review['review_token'])
        assert len(writes) == 1
    asyncio.run(run())
    saved = json.loads(path.read_text())
    assert saved['bot']['global']['risk']['max_positions'] == 3
    assert saved['bot']['long'] == config['bot']['long']
    assert saved['pbgui'] == config['pbgui']


@pytest.mark.parametrize('create', [False, True])
def test_complete_native_proposal_is_reviewable_and_cannot_overwrite_stale_target(environment, create):
    """Existing edits and create-only suggestions bind the exact target state."""
    caps, api, config, path, writes = environment
    name = 'new-config' if create else 'fixture'
    suggested = caps._sanitize_config(copy.deepcopy(config))
    suggested['bot']['global']['risk']['max_positions'] = 5
    async def run():
        receipt = await caps.dispatch(OWNER, CID, 'propose_pb9_optimizer_config', {'name': name, 'config': suggested})
        item = next(item for item in await caps.list_proposals(OWNER, CID) if item['proposal_id'] == receipt['proposal_id'])
        assert not writes
        review = await caps.reviewed_changes.review(OWNER, item['proposal_id'], CID, item['payload_digest'])
        await caps.approve(OWNER, item['proposal_id'], item['payload_digest'], CID, review_token=review['review_token'])
    asyncio.run(run())
    assert json.loads(api._config_file(name).read_text())['bot']['global']['risk']['max_positions'] == 5
    if create:
        assert json.loads(path.read_text()) == config


def test_native_review_aborts_if_normalization_changes_after_approval(environment, monkeypatch):
    """A runtime change cannot turn an approved payload into a different saved config."""
    caps, api, config, path, writes = environment
    async def run():
        receipt = await caps.dispatch('a'*32, 'b'*32, 'propose_reviewed_config_change', {
            'kind': 'optimizer', 'version': 'v9', 'name': 'fixture',
            'operations': [{'op': 'replace', 'path': '/bot/global/risk/max_positions', 'value': 3}]})
        proposal = next(item for item in await caps.list_proposals('a'*32, 'b'*32) if item['proposal_id'] == receipt['proposal_id'])
        review = await caps.reviewed_changes.review('a'*32, receipt['proposal_id'], 'b'*32, proposal['payload_digest'])
        def changed(value):
            """Simulate new runtime normalization after the user reviewed the exact diff."""
            value = copy.deepcopy(value)
            value['bot']['global']['risk']['max_positions'] = 99
            return value
        monkeypatch.setattr(api, 'prepare_pb8_config', changed)
        with pytest.raises(Exception, match='normalization changed after review'):
            await caps.approve('a'*32, receipt['proposal_id'], proposal['payload_digest'], 'b'*32, review_token=review['review_token'])
    asyncio.run(run())
    assert not writes
    assert json.loads(path.read_text()) == config


@pytest.mark.parametrize('tool,method', [
    ('get_pb9_ai_loop_configs', 'read_loop'),
    ('get_pb9_ai_loop_runs', 'read_runs'),
    ('read_pb9_ai_loop_log', 'read_log'),
])
def test_native_read_tools_dispatch_with_owner_and_conversation(environment, monkeypatch, tool, method):
    """The actual provider dispatcher must await native reads with all three arguments."""
    caps, *_ = environment
    async def read(owner, cid, args):
        """Only inert owner-bound evidence, never native runtime writes."""
        assert owner == OWNER and cid == CID and args == {}
        return {'native_read': True}
    monkeypatch.setattr(caps.pb9_loop_tools, method, read)
    assert asyncio.run(caps.dispatch(OWNER, CID, tool, {})) == {'native_read': True}


def test_native_metadata_default_is_compact_and_details_remain_queryable(environment):
    """Large optional registries cannot hide the complete template or metric catalog."""
    caps, api, config, *_ = environment
    metrics = ['cagr_fraction_strategy_equity', 'drawdown_max_fraction_strategy_equity']
    metadata = {'template': config, 'strategies': ['ema_anchor'], 'scoring': {'metrics': metrics},
                'strategy_specs': {'ema_anchor': {str(i): 'x' * 1000 for i in range(600)}}}
    api.native.metadata = lambda: metadata
    result = caps._get_optimizer_metadata({'version': 'v9'})
    caps._require_bounded_result(result)
    assert result['metadata']['template'] == caps._sanitize_config(config)
    assert result['metadata']['scoring']['metrics'] == metrics
    assert 'strategy_specs' in result['available_sections']
    details = caps._get_optimizer_metadata({'version': 'v9', 'section': 'strategy_specs',
                                            'path': ['ema_anchor'], 'offset': 20, 'limit': 2})
    assert set(details['metadata']['strategy_specs']) == {'20', '21'}
    assert details['pagination']['total'] == 600 and details['pagination']['next_offset'] == 22


@pytest.mark.parametrize('create', [False, True])
def test_native_proposal_preserves_four_training_windows_and_excluded_holdout(environment, create):
    """Use the shared preview, native proposal and real approval/save bridge together."""
    caps, api, config, path, writes = environment
    windows = [{'id': 'w' + str(day), 'label': 'train_' + str(day) if day < 5 else 'holdout',
                'role': 'training' if day < 5 else 'holdout',
                'start_date': '2024-01-0' + str(day), 'end_date': '2024-01-0' + str(day)}
               for day in range(1, 6)]
    parameters = {'template': 'custom_windows', 'start_date': '2024-01-01', 'end_date': '2024-01-05',
                  'training_windows': 4, 'windows': windows, 'exchanges': ['bybit', 'hyperliquid']}
    name = 'new-windows' if create else 'fixture'
    if not create:
        original = copy.deepcopy(config)
        original['pbgui']['scenario_template'] = {'contract_version': 99, 'old_plan': True}
        path.write_text(json.dumps(original))
    async def run():
        preview = await caps.dispatch(OWNER, CID, 'preview_optimizer_scenario_template', parameters)
        assert len(preview['training_scenarios']) == 4 and len(preview['holdout_scenarios']) == 1
        receipt = await caps.dispatch(OWNER, CID, 'propose_pb9_optimizer_config',
            {'name': name, 'config': caps._sanitize_config(config), 'scenario_template': parameters})
        item = next(row for row in await caps.list_proposals(OWNER, CID) if row['proposal_id'] == receipt['proposal_id'])
        assert not writes
        assert item['preview']['validation'] == {'valid': True, 'runtime': 'v9'}
        assert len(item['preview']['generated_scenario_template']['holdout_scenarios']) == 1
        review = await caps.reviewed_changes.review(OWNER, item['proposal_id'], CID, item['payload_digest'])
        await caps.approve(OWNER, item['proposal_id'], item['payload_digest'], CID, review_token=review['review_token'])
    asyncio.run(run())
    saved = json.loads(api._config_file(name).read_text())
    from scenario_windows import build_validation_plan
    assert len(saved['backtest']['scenarios']) == 4
    assert all(row['end_date'] < '2024-01-05' for row in saved['backtest']['scenarios'])
    assert build_validation_plan(saved)['holdout_scenarios'][0]['start_date'] == '2024-01-05'
    assert saved['optimize']['write_all_results'] is True
    if not create:
        assert saved['pbgui']['note'] == 'must survive'


@pytest.mark.parametrize('tool,method', [
    ('get_pb9_ai_loop_configs', 'read_loop'),
    ('get_pb9_ai_loop_runs', 'read_runs'),
    ('read_pb9_ai_loop_log', 'read_log'),
])
def test_native_read_tools_receive_owner_and_conversation(environment, monkeypatch, tool, method):
    """The real dispatcher must not route owner-aware coroutines as synchronous reads."""
    caps, *_ = environment
    calls = []
    async def read(owner, conversation, args):
        """Stand in for the owned read boundary, without filesystem access."""
        calls.append((owner, conversation, args))
        return {'native': True}
    monkeypatch.setattr(caps.pb9_loop_tools, method, read)
    result = asyncio.run(caps.dispatch(OWNER, CID, tool, {}))
    assert result == {'native': True} and calls == [(OWNER, CID, {})]


def test_native_save_does_not_turn_trusted_creation_into_research_only(environment, monkeypatch):
    """After save approval a separate native cloud review remains possible, never auto-started."""
    caps, api, config, path, writes = environment
    calls = []
    async def cloud_review(service, owner, conversation, args):
        """No provider call, rental, queue entry or trading process is created."""
        calls.append(args['version'])
        return {'status': 'awaiting_approval'}
    monkeypatch.setattr('ai_vast_optimizer.propose', cloud_review)
    async def run():
        receipt = await caps.dispatch(OWNER, CID, 'propose_pb9_optimizer_config',
            {'name': 'new', 'config': caps._sanitize_config(config)})
        assert not caps.analysis_only(OWNER, CID) and not calls
        proposal = caps.proposals[receipt['proposal_id']]
        review = await caps.reviewed_changes.review(OWNER, proposal.id, CID, proposal.payload_digest)
        await caps.approve(OWNER, proposal.id, proposal.payload_digest, CID, review_token=review['review_token'])
        assert len(writes) == 1 and not calls
        assert await caps.dispatch(OWNER, CID, 'propose_vast_optimizer_run',
            {'version': 'v9', 'name': 'new', 'stop_when_no_improvement': True}) == {'status': 'awaiting_approval'}
        caps.restrict_to_analysis(OWNER, CID)
        with pytest.raises(AICapabilityError, match='analysis'):
            await caps.dispatch(OWNER, CID, 'propose_vast_optimizer_run',
                {'version': 'v9', 'name': 'new', 'stop_when_no_improvement': True})
        assert calls == ['v9']
    asyncio.run(run())


def test_shared_ai_windows_reject_overlapping_training_and_holdout(environment):
    """Legacy generated windows must not silently leak Holdout into training."""
    caps, _, config, *_ = environment
    with pytest.raises(AICapabilityError, match='overlap'):
        caps._apply_optimizer_scenario_template(config, {
            'template': 'walk_forward', 'start_date': '2024-01-01', 'end_date': '2026-01-01',
            'window_days': 90, 'stride_days': 30, 'training_windows': 4, 'holdout_windows': 1})


def test_native_ai_loop_window_parameters_require_the_original_review(environment):
    """Typed ordinary windows cannot forge a visible AI Loop Template review."""
    caps, _, config, _, writes = environment
    async def run():
        with pytest.raises((AICapabilityError, ValueError)):
            await caps.dispatch(OWNER, CID, 'propose_pb9_optimizer_config', {
                'name': 'new', 'config': caps._sanitize_config(config),
                'scenario_template': {'template': 'ai_loop_windows', 'version': 1,
                    'start_date': '2024-01-01', 'end_date': '2024-01-02',
                    'use_holdout': False, 'training_windows': 1,
                    'windows': [{'id': 'w1', 'label': 'train_1', 'role': 'training',
                                 'start_date': '2024-01-01', 'end_date': '2024-01-02'}]}})
        assert not writes
    asyncio.run(run())
