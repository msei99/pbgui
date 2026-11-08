"""Offline controller randomness and model seed-ownership boundaries."""
import copy
import json

import pytest

from pb8_loop_ai_v2 import validate_decision
from pb8_loop_evidence import EvidenceSession, EvidenceStore, decision_snapshot_stale, required_content
from pb8_loop_research import evidence, experiment_plan, recommendation_configs, validate_assessment
from pb8_loop_rng import allocate_seed
from tests.test_pb8_loop_optimizer import record
from tests.test_pb8_loop_research import research, optimizer, observation, assessment


def test_allocator_resamples_collisions_with_all_persisted_seed_sources(monkeypatch):
    """Initial, planned, executed and legacy research seeds cannot be reissued."""
    row = {'initial_config': {'optimize': {'seed': 7}}, 'jobs': [{
        'config': {'optimize': {'seed': 8}}, 'executed_config': {'optimize': {'seed': 9}},
        'research': {'seed': 10, 'seed_index': 0}}]}
    draws = iter([7, 8, 9, 10, 77, 78])
    monkeypatch.setattr('pb8_loop_rng.secrets.randbelow', lambda n: next(draws))
    assert allocate_seed(row) == 77
    row['jobs'].append({'config': {'optimize': {'seed': 77}}})
    assert allocate_seed(row) == 78


def test_plan_validation_never_draws_randomness(research, monkeypatch):
    """Repeated validation cannot let a model probe future execution seeds."""
    _, row, _ = research
    monkeypatch.setattr('pb8_loop_rng.secrets.randbelow', lambda n: pytest.fail('Planning drew a seed'))
    first = experiment_plan(row, row['initial_config'], {}, {'purpose': 'explore', 'budget_fraction': 1})
    assert experiment_plan(row, row['initial_config'], {}, {'purpose': 'explore', 'budget_fraction': 1}) == first
    assert 'seed' not in first and 'seed_index' not in first


def test_each_new_trial_gets_its_own_durable_random_seed(research, monkeypatch):
    """Different settings get independent controller draws, preserved on reload."""
    store, row, controller = research
    draws = iter([101, 202, 303])
    monkeypatch.setattr('pb8_loop_rng.secrets.randbelow', lambda n: next(draws))
    first = controller.job(row, 'optimizer', row['initial_config'], {}, 'Test search settings',
                           experiment={'purpose': 'explore', 'budget_fraction': 1})
    row['jobs'].append(first)
    different = copy.deepcopy(row['initial_config'])
    different['optimize']['scoring'] = [{'metric': 'sortino_ratio_strategy_eq', 'goal': 'max'}]
    second = controller.job(row, 'optimizer', different, {}, 'Test alternative settings',
                            experiment={'purpose': 'explore', 'budget_fraction': 1})
    row['jobs'].append(second)
    third_config = copy.deepcopy(row['initial_config'])
    third_config['optimize']['scoring'] = [{'metric': 'adg_rolling_hmean_strategy_eq', 'goal': 'max'}]
    third = controller.job(row, 'optimizer', third_config, {}, 'Test another scoring', experiment={
        'purpose': 'explore', 'budget_fraction': 1})
    row['jobs'].append(third)
    assert [j['config']['optimize']['seed'] for j in row['jobs']] == [101, 202, 303]
    assert all(j['research']['seed'] == j['optimizer_start']['seed'] == j['config']['optimize']['seed'] for j in row['jobs'])
    assert all('seed_index' not in j['research'] for j in row['jobs'])
    store.update(row['owner'], row['id'], lambda current: current.update(jobs=row['jobs']))
    assert [j['research']['seed'] for j in store.read(row['owner'], row['id'])['jobs']] == [101, 202, 303]


@pytest.mark.parametrize('provider', ['chatgpt', 'claude-code', 'opencode-go', 'opencode-zen'])
def test_every_provider_rejects_ai_seed_selection(research, provider):
    """No model can select an RNG seed through config or experiment fields."""
    _, row, _ = research
    row['settings']['provider'] = provider
    variant = {'config': copy.deepcopy(row['initial_config']), 'overrides': {}, 'reason': 'Test optimizer settings',
               'experiment': {'purpose': 'explore', 'budget_fraction': 1}}
    decision = {'variants': [variant]}
    variant['config']['optimize']['seed'] = 123456
    with pytest.raises(ValueError, match='optimize.seed'):
        validate_decision(row, 'bootstrap', {'max_variants': 1}, json.dumps(decision))
    variant['config']['optimize']['seed'] = None
    assert validate_decision(row, 'bootstrap', {'max_variants': 1}, json.dumps(decision))
    variant['config']['optimize'].pop('seed')
    assert validate_decision(row, 'bootstrap', {'max_variants': 1}, json.dumps(decision))
    variant['experiment']['seed'] = 123456
    with pytest.raises(ValueError, match='controller-owned'):
        validate_decision(row, 'bootstrap', {'max_variants': 1}, json.dumps(decision))


def test_model_evidence_and_scalar_history_reads_cannot_reveal_seed_values(record):
    """Mask numeric seeds even in old signed snapshots, deltas and indexed scalar reads."""
    store, row = record
    raw = {'stage': 'evaluate', 'initial_config': {'optimize': {'seed': 7654321}}, 'rubric': [],
           'variant_config_schema': {'properties': {'optimize': {'properties': {'seed': {'const': None}}}}},
           'evidence': {'optimizer_research': {}, 'changes': {'set': [
               {'path': ['optimize', 'seed'], 'value': 7654321}]}, 'winner_options': {},
               'large': {'seeds': list(range(5000))}, 'gain': 3.5}}
    before = copy.deepcopy(raw)
    required = required_content(raw)
    assert required['variant_config_schema']['properties']['optimize']['properties']['seed'] == {'const': None}
    assert '7654321' not in json.dumps(required)
    archive = EvidenceStore(store)
    snapshot = archive.create(row, required, raw, [{'id': 'old', 'loop': row['id'],
        'changes': {'set': [{'path': ['optimize', 'seed'], 'value': 7654321}]}}])
    session = EvidenceSession(archive, snapshot, 1500)
    try:
        for i, path in enumerate(['/data/initial_config', '/data/initial_config/optimize/seed',
                                  '/data/evidence/changes', '/data/evidence/changes/set/0/value',
                                  '/history/old', '/history/old/changes/set/0/value',
                                  '/data/evidence/large/seeds/4321']):
            result = session.read({'path': path, 'offset': 0}, f'read{i}')
            assert '7654321' not in json.dumps(result)
            if path.endswith('/4321'):
                assert result['entries'][0]['value'] is None
        result = session.read({'path': '/data/evidence/gain', 'offset': 0}, 'gain')
        assert result['entries'][0]['value'] == 3.5
    finally:
        session.close()
    assert raw == before
    assert archive.load(row['owner'], snapshot['id']) == snapshot
    assert not decision_snapshot_stale(snapshot)
    legacy = copy.deepcopy(snapshot)
    legacy['required'].pop('rng_visibility_version')
    assert decision_snapshot_stale(legacy)


def test_setting_recommendations_never_export_or_rank_a_seed(research):
    """The recommended config preserves measured search controls but leaves future randomness fresh."""
    _, row, controller = research
    job = optimizer(row, controller, 'optimizer')
    cpu = observation(row, 'optimizer', 'cpu', 3)
    row['optimizer_assessment'] = validate_assessment(row, [cpu], assessment())
    model = evidence(row, [cpu])
    assert 'seed' not in model['trials']['optimizer'] and 'seed_index' not in model['trials']['optimizer']
    assert 'paired_seed_policy' not in model
    assert 'per_trial_best' in model['groups'][0]
    assert 'per_seed_best' not in model['groups'][0]
    assert 'measured_seed_count' not in model['groups'][0]
    exported = recommendation_configs(row)['recommendations'][0]
    assert exported['config']['optimize']['seed'] is None
    assert 'seed' not in exported['experiment']
    assert job['config']['optimize']['seed'] is not None
