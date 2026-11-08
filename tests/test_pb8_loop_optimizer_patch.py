"""Compact research proposals retain complete backend checks and owned source inputs."""
import copy
import json
import asyncio
from unittest.mock import AsyncMock

import pytest

from pb8_loop_ai_v2 import validate_decision
from pb8_loop_evidence import EvidenceStore, required_content, tokens
from pb8_loop_research import patch_contract, resolve_variant, source_inputs
from tests.test_pb8_loop_research import research, optimizer


def proposal(base='initial', patch=None):
    """Propose only a genuine changed scoring objective at the fixed work budget."""
    return {'base_optimizer': base, 'optimizer_patch': patch or {
        'scoring': [{'metric': 'sortino_ratio_strategy_eq', 'goal': 'max'}]},
        'experiment': {'purpose': 'explore', 'budget_fraction': 1},
        'reason': 'Test another scoring objective while retaining the exact reviewed source inputs'}


@pytest.mark.parametrize('provider', ['chatgpt', 'claude-code', 'opencode-go', 'opencode-zen'])
def test_compact_proposal_uses_the_same_complete_validation_for_every_provider(research, provider):
    """Provider-independent expansion preserves fixed work, workers and source data."""
    _, row, _ = research
    row['settings']['provider'] = provider
    before = copy.deepcopy(row)
    result = validate_decision(row, 'bootstrap', {'max_variants': 1},
                               json.dumps({'variants': [proposal()]}))['variants'][0]
    assert result['config']['optimize']['scoring'][0]['metric'] == 'sortino_ratio_strategy_eq'
    assert result['config']['optimize'].get('gpu') == row['initial_config']['optimize'].get('gpu')
    assert result['config']['optimize']['seed'] is None
    assert result['config']['backtest'] == row['initial_config']['backtest']
    assert 'optimizer_patch' not in result and 'base_optimizer' not in result
    assert row == before


def test_owned_patch_merges_bounds_and_preserves_other_bounds_and_overrides(research):
    """A compact edit is not an unchanged source replay or a replacement of all bounds."""
    _, row, controller = research
    source = optimizer(row, controller, 'owned-source', fraction=1)
    source['overrides'] = {'TEST': {'kept': True}}
    source['executed_config']['optimize']['gpu'] = {'population_size': 9999}
    before = copy.deepcopy(row)
    name = 'long_total_wallet_exposure_limit'
    value = resolve_variant(row, proposal(source['operation'], {'bounds': {name: [.2, .8]}}))
    expected = copy.deepcopy(source['executed_config']['optimize']['bounds'])
    expected[name] = [.2, .8]
    assert value['config']['optimize']['bounds'] == expected
    assert value['config']['optimize'].get('gpu') == row['initial_config']['optimize'].get('gpu')
    assert value['overrides'] == source['overrides'] and row == before


@pytest.mark.parametrize('field', ['seed', 'gpu', 'n_cpus', 'population_size', 'iters', 'fixed_runtime_overrides'])
def test_compact_changes_cannot_address_runtime_or_rng_controls(research, field):
    """The compact format exposes exactly Bounds, Limits and Scoring."""
    _, row, _ = research
    with pytest.raises(ValueError, match='only bounds, limits and scoring'):
        resolve_variant(row, proposal(patch={field: 1}))


@pytest.mark.parametrize('field', ['config', 'overrides'])
def test_compact_and_full_formats_cannot_be_mixed(research, field):
    """An apparent patch cannot hide a second source of fixed settings or overrides."""
    _, row, _ = research
    value = proposal(); value[field] = {}
    with pytest.raises(ValueError, match='omit config and overrides'):
        resolve_variant(row, value)


def test_patch_requires_an_owned_base_and_rejects_an_unchanged_search(research):
    """Neither foreign inputs nor a cosmetic compact edit bypass the novelty gate."""
    _, row, controller = research
    source = optimizer(row, controller, 'owned-source', fraction=1)
    with pytest.raises(ValueError, match='owned optimizer'):
        resolve_variant(row, proposal('foreign-source'))
    value = proposal(source['operation'], {'scoring': source['config']['optimize']['scoring']})
    with pytest.raises(ValueError, match='Unchanged optimizer settings'):
        validate_decision(row, 'bootstrap', {'max_variants': 1}, json.dumps({'variants': [value]}))


def test_patch_does_not_bypass_the_user_exposure_ceiling(research):
    """Full risk validation is applied after sparse bounds expansion."""
    _, row, _ = research
    row['settings']['goals']['max_twe'] = 2
    value = proposal(patch={'bounds': {'long_total_wallet_exposure_limit': [1, 1000]}})
    with pytest.raises(ValueError):
        validate_decision(row, 'bootstrap', {'max_variants': 1}, json.dumps({'variants': [value]}))


def test_compact_required_package_keeps_exact_full_schema_readable(research):
    """Large immutable schemas stay optional without losing source ownership or exact data."""
    store, row, controller = research
    source = optimizer(row, controller, 'owned-source', fraction=1)
    refs, _ = source_inputs(row)
    contract = patch_contract(row, refs)
    # A completed source without qualified measurements must not become the default parent.
    assert contract['preferred_variant']['base_optimizer'] == 'initial'
    assert contract['source_controls']['bounds'] == '/data/initial_config/optimize/bounds'
    raw = {'stage': 'evaluate', 'evidence': {'optimizer_research': {}},
           'optimizer_patch_contract': contract,
           'variant_config_schema': {'large_fixed_worker_contract': 'immutable ' * 10000}}
    required = required_content(raw)
    assert tokens(required) < 2000
    assert required['variant_config_schema']['evidence_path'] == '/data/variant_config_schema'
    evidence = EvidenceStore(store)
    snapshot = evidence.create(row, required, raw, [])
    assert evidence.resolve(snapshot, '/data/variant_config_schema') == raw['variant_config_schema']


def test_controller_creates_a_normal_fixed_budget_job_from_compact_proposal(research, monkeypatch):
    """Controller orchestration expands the compact answer before job/RNG allocation."""
    store, row, controller = research
    row = store.update(row['owner'], row['id'], lambda value: value.update(
        phase='bootstrap', bootstrap_candidates=[], bootstrap={'sources': []}, baseline_required=False))
    monkeypatch.setattr(controller.backend, 'capacity', lambda *_args: 1)
    monkeypatch.setattr(controller, 'decision', AsyncMock(return_value={
        'variants': [proposal()], 'reason': 'Changed scoring hypothesis'}))
    asyncio.run(controller._tick(row))
    current = store.read(row['owner'], row['id'])
    job = current['jobs'][-1]
    assert current['phase'] == 'optimize' and job['kind'] == 'optimizer'
    assert job['research']['budget'] == {'mode': 'proxy', 'value': 100000, 'fraction': 1}
    assert type(job['config']['optimize']['seed']) is int
    assert job['config']['optimize']['scoring'][0]['metric'] == 'sortino_ratio_strategy_eq'


def test_evaluation_can_rebase_to_measured_winner_after_a_losing_scoring_trial(research):
    """The real provider-independent gate accepts a novel experiment from the older winner."""
    from tests.test_pb8_loop_research import observation, assessment
    from pb8_loop_research import trial_records, evidence
    _, row, controller = research
    winner = optimizer(row, controller, 'winner', fraction=1)
    first = observation(row, 'winner', 'cpu-first', 10)
    row['optimizer_assessment'] = assessment('winner')
    row['optimizer_trials'] = trial_records(row, [first])
    row['round'] = 1
    loser = optimizer(row, controller, 'loser', fraction=1)
    loser['config']['optimize']['scoring'] = [{'metric': 'sortino_ratio_strategy_eq', 'goal': 'max'}]
    loser['executed_config'] = copy.deepcopy(loser['config'])
    second = observation(row, 'loser', 'cpu-second', 8)
    row['optimizer_trials'] = trial_records(row, [second])
    row['best'] = first
    supplied = {'observations': [second], 'winner_options': {'cpu-first': first, 'cpu-second': second},
                'optimizer_research': evidence(row, [second]),
                'remaining_rounds': 1, 'remaining_runs': 1, 'max_variants': 1}
    refs, _ = source_inputs(row)
    assert patch_contract(row, refs, supplied['optimizer_research'])['preferred_variant']['base_optimizer'] == 'winner'
    from pb8_loop_bounds import diagnose
    diagnosis = diagnose(row, supplied['winner_options'], supplied['optimizer_research'])
    supplied['boundary_diagnostics'] = diagnosis
    variant = proposal('winner', {'scoring': [{'metric': 'sharpe_ratio_strategy_eq', 'goal': 'max'}]})
    variant['boundary_response'] = [{'id': signal['id'], 'action': 'retain',
                                    'reason': 'Retain the successful reference while testing changed scoring'}
                                   for signal in diagnosis['parameters']]
    decision = {'winner_operation': 'cpu-first', 'reason': 'Keep the best measured growth',
                'optimizer_assessment': assessment('winner'), 'variants': [variant]}
    result = validate_decision(row, 'evaluate', supplied, json.dumps(decision))
    assert result['variants'][0]['config']['optimize']['bounds'] == winner['config']['optimize']['bounds']
    assert result['variants'][0]['config']['optimize']['scoring'][0]['metric'] == 'sharpe_ratio_strategy_eq'
