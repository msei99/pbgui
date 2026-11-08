"""Offline optimizer-research contracts use only isolated synthetic training evidence."""
import asyncio
import copy
from types import SimpleNamespace

import pytest

from pb8_loop_research import (budget, evidence, execution_check, experiment_plan, progress_sample,
                               recommendation_configs, signature, summarize, trial_records, validate_assessment)
from pb8_loop_controller import LoopController
from pb8_loop_store import LoopStore, fresh_optimizer_config
from tests.test_pb8_loop_optimizer import settings, valid_gpu_config


@pytest.fixture
def research(tmp_path):
    """Create a versioned run and controller entirely inside the test directory."""
    initial = fresh_optimizer_config(valid_gpu_config())
    store = LoopStore(tmp_path / 'loops')
    row = store.create('a' * 32, settings(execution='gpu', run_limit_mode='proxy', run_proxy=100000),
                       initial, {}, {}, [], initial['backtest'], optimizer_research_version=1)
    row = store.update(row['owner'], row['id'], lambda r: r.update(
        ai_model_profile={'context': 4_000_000, 'page_tokens': 1500, 'output_tokens': 1000}))
    return store, row, LoopController(tmp_path, store=store)


def optimizer(row, controller, name, fraction=.5):
    """Restore a historical trial fixture, including old fractional/repeated inputs."""
    job = controller.job({**row, 'jobs': []}, 'optimizer', copy.deepcopy(row['initial_config']), {}, 'Historical fixture',
                         experiment={'purpose': 'explore', 'budget_fraction': 1})
    job['research']['budget'].update(fraction=fraction, value=int(100000 * fraction))
    job.update(operation=name, status='cancelled', results_consumed=True, result_count=2,
               proxy_evaluations=int(100000 * fraction), limit_reached='proxy',
               research_verification={'status': 'verified'}, executed_config=copy.deepcopy(job['config']))
    row['jobs'].append(job)
    return job


def observation(row, source, operation, gain, qualified=True):
    """Bind a CPU report to its actual optimizer, separate from observer reports."""
    candidate = {'id': operation, 'optimizer_job': source, 'config': row['initial_config'], 'overrides': {}}
    row['jobs'].append({'operation': operation, 'kind': 'validation', 'candidate': candidate,
                        'status': 'complete', 'round': row['round']})
    return {'operation': operation, 'exact': True, 'candidate': candidate,
            'assessment': {'comparable': True, 'simulation_complete': True, 'hard_targets_met': qualified},
            'metrics': {'gain': gain}, 'per_report': [{'gain': gain}, {'gain': gain}]}


def changed_config(config):
    """Return a genuine scoring alternative, not a changed seed or work budget."""
    value = copy.deepcopy(config)
    value['optimize']['scoring'] = [{'metric': 'sortino_ratio_strategy_eq', 'goal': 'max'}]
    value['optimize']['seed'] = None
    return value


def assessment(operation='optimizer'):
    """Choose an explicit total-return research criterion without a weighted score."""
    return {'criteria': [{'metric': 'gain', 'direction': 'max', 'reducer': 'mean', 'reason': 'Total return'}],
            'recommendations': [{'operation': operation, 'reason': 'Measured short-run return'}],
            'reason': 'Provisional fixed-budget training evidence; later deep search is user-owned'}


def test_settings_get_new_random_seeds_and_planning_does_not_assign_them(research):
    """Only durable jobs get random seeds; settings and repetitions never select a panel."""
    store, row, controller = research
    first = optimizer(row, controller, 'first')
    other = copy.deepcopy(row['initial_config'])
    other['optimize']['scoring'] = [{'metric': 'sortino_ratio_strategy_eq', 'goal': 'max'}]
    plan = experiment_plan(row, other, {}, {'purpose': 'explore', 'budget_fraction': 1})
    assert 'seed' not in plan and 'seed_index' not in plan
    second = controller.job(row, 'optimizer', other, {}, 'Different settings',
                            experiment={'purpose': 'explore', 'budget_fraction': 1})
    assert second['research']['seed'] != first['research']['seed']
    store.update(row['owner'], row['id'], lambda r: r.update(jobs=row['jobs']))
    restored = store.read(row['owner'], row['id'])
    with pytest.raises(ValueError, match='forbidden'):
        experiment_plan(restored, first['config'], {},
            {'purpose': 'replicate', 'source_operation': 'first', 'budget_fraction': 1})


@pytest.mark.parametrize('fraction', [0, .1, .5, .99, 1.01, True, float('nan'), float('inf')])
def test_experiment_cannot_exceed_reviewed_proxy_budget(research, fraction):
    """Model-authored fractions never expand paid or per-run authorization."""
    _, row, _ = research
    with pytest.raises(ValueError, match='budget_fraction'):
        budget(row, fraction)


@pytest.mark.parametrize('purpose', ['replicate', 'confirm'])
def test_repetitions_and_confirmations_are_forbidden(research, purpose):
    """The user's fixed per-run budget cannot become a seed repeat or longer run."""
    _, row, controller = research
    first = optimizer(row, controller, 'first')
    with pytest.raises(ValueError, match='forbidden'):
        experiment_plan(row, first['config'], {}, dict(purpose=purpose, source_operation='first', budget_fraction=1))


def test_lost_reducer_objective_is_rejected_before_launch(research):
    """Round-14 style duplicate metrics cannot masquerade as a two-objective trial."""
    _, row, _ = research
    proposal = copy.deepcopy(row['initial_config'])
    proposal['optimize']['scoring'] = [
        {'metric': 'adg_strategy_eq', 'goal': 'max', 'reducer': reducer} for reducer in ['mean', 'min']]
    with pytest.raises(ValueError, match='repeated scoring'):
        experiment_plan(row, proposal, {})
    executed = copy.deepcopy(proposal)
    executed['optimize']['scoring'].pop()
    with pytest.raises(ValueError, match='normalization'):
        execution_check(proposal, executed, {})


def test_runtime_sizing_does_not_hide_strategy_normalization(research):
    """Hardware-owned sizing is distinct from changed scoring/limits/strategy bounds."""
    _, row, _ = research
    proposal = copy.deepcopy(row['initial_config'])
    executed = copy.deepcopy(proposal)
    executed['optimize'].update(gpu={'population_size': 8192}, n_cpus=21)
    assert execution_check(proposal, executed, {})['status'] == 'verified'
    executed['optimize']['scoring'][0]['goal'] = 'min'
    with pytest.raises(ValueError, match='normalization'):
        execution_check(proposal, executed, {})


def test_research_separates_settings_quality_from_bot_incumbent_and_observers(research):
    """A losing bot result remains useful search evidence; observer metrics never enter it."""
    _, row, controller = research
    optimizer(row, controller, 'optimizer')
    candidate = observation(row, 'optimizer', 'cpu', 3)
    failed = observation(row, 'optimizer', 'unsafe', 100, qualified=False)
    private = dict(candidate, operation='private', observer_only=True, metrics={'gain': 10000})
    row['jobs'].append({'operation': 'private', 'kind': 'observer_holdout', 'status': 'complete'})
    row.update(phase='evaluate', best={'metrics': {'gain': 5}})
    result = validate_assessment(row, [candidate, failed, private], assessment())
    group = result['groups'][0]
    assert group['distribution'][0]['mean'] == 3
    assert group['qualification_rate'] == .5
    assert group['successful_seed_rate'] == 1 and group['measured_seed_count'] == 1
    assert group['distribution'][0]['stdev'] is None
    assert 'One completed trial' in group['uncertainty']
    assert len(trial_records(row, [candidate, failed, private])['optimizer']['outcomes']) == 2


def test_historical_repeated_trials_have_variability_and_different_budgets_are_separate(research):
    """Compute per-seed best outcomes and never pool screening with confirmation work."""
    _, row, controller = research
    optimizer(row, controller, 'first')
    first = observation(row, 'first', 'cpu1', 2)
    row['optimizer_trials'] = trial_records(row, [first], assessment()['criteria'])
    row['round'] += 1
    optimizer(row, controller, 'second')
    second = observation(row, 'second', 'cpu2', 4)
    optimizer(row, controller, 'long', fraction=1)
    third = observation(row, 'long', 'cpu3', 8)
    result = validate_assessment(row, [second, third], assessment('second'))
    assert len(result['groups']) == 2
    repeated = next(g for g in result['groups'] if g['trial_count'] == 2)
    assert repeated['distribution'][0]['mean'] == 3
    assert repeated['distribution'][0]['stdev'] == pytest.approx(2 ** .5)
    assert repeated['seeds'] == ['first', 'second']


def test_unmeasured_or_missing_criteria_cannot_cherry_pick_settings(research):
    """Every qualified candidate must be recomputed under identical available metrics."""
    _, row, controller = research
    optimizer(row, controller, 'optimizer')
    candidate = observation(row, 'optimizer', 'cpu', 3)
    with pytest.raises(ValueError, match='verified optimizer'):
        validate_assessment(row, [candidate], assessment('cpu'))
    choice = assessment()
    choice['criteria'][0]['metric'] = 'not_measured'
    with pytest.raises(ValueError, match='unavailable in all'):
        validate_assessment(row, [candidate], choice)


def test_unverified_or_underbudget_trial_cannot_be_recommended(research):
    """Partial interrupted work is retained as evidence but cannot win a fair comparison."""
    _, row, controller = research
    job = optimizer(row, controller, 'optimizer')
    candidate = observation(row, 'optimizer', 'cpu', 3)
    job['proxy_evaluations'] = 40000
    with pytest.raises(ValueError, match='verified optimizer'):
        validate_assessment(row, [candidate], assessment())
    job['proxy_evaluations'] = 50000
    job['research_verification'] = {'status': 'mismatch'}
    with pytest.raises(ValueError, match='verified optimizer'):
        validate_assessment(row, [candidate], assessment())


def test_export_contains_actual_optimizer_not_bot_winner(research):
    """Export a complete source bundle and mark deep-search performance as unverified."""
    _, row, controller = research
    job = optimizer(row, controller, 'optimizer')
    candidate = observation(row, 'optimizer', 'cpu', 3)
    row['optimizer_assessment'] = validate_assessment(row, [candidate], assessment())
    row['best'] = {'candidate': {'config': {'wrong': True}}}
    exported = recommendation_configs(row)
    expected = copy.deepcopy(job['executed_config'])
    expected['optimize']['seed'] = None
    assert exported['recommendations'][0]['config'] == expected
    assert 'seed' not in exported['recommendations'][0]['experiment']
    assert exported['deep_search_verified'] is False
    exported['recommendations'][0]['config']['optimize']['seed'] = -1
    assert job['executed_config']['optimize']['seed'] >= 0


def test_progress_is_bounded_and_native_metrics_never_qualify_a_candidate(research):
    """Native convergence evidence stays labelled and cannot manufacture CPU validation."""
    _, row, controller = research
    job = optimizer(row, controller, 'optimizer')
    for index in range(100):
        progress_sample(job, {'proxy_evaluations': index * 1000, 'generation': index,
                              'objectives': {'gain': index, 'bad': float('nan')}})
    assert len(job['research_progress']) == 32
    assert job['research_progress'][0]['proxies'] == 0
    assert job['research_progress'][-1]['native_objectives'] == {'gain': 99}
    assert summarize(trial_records(row), assessment()['criteria'])[0]['qualified_count'] == 0


@pytest.mark.parametrize('finish', [False, True])
def test_controller_keeps_bot_and_optimizer_results_separate_without_stagnation_stop(research, finish):
    """Persist useful measured search settings even when every new bot loses to baseline."""
    store, row, controller = research
    job = optimizer(row, controller, 'optimizer')
    candidate = observation(row, 'optimizer', 'cpu', 3)
    baseline = dict(candidate, operation='baseline', candidate={**candidate['candidate'], 'id': 'baseline'},
                    metrics={'gain': 5}, per_report=[{'gain': 5}], reports=1)
    row.update(phase='evaluate', observations=[candidate], baseline=baseline, best=baseline,
               stagnation=99, rubric=[{'goal': 'gain', 'metrics': ['gain'], 'direction': 'max', 'target': None}])
    row['settings']['patience'] = 1
    row = store.update(row['owner'], row['id'], lambda current: current.update(row))

    async def decide(current, stage, supplied):
        """A useful trial does not need to displace the better baseline bot."""
        assert supplied['optimizer_research']['trials']['optimizer']['qualified_count'] == 1
        assert 'outcomes' not in supplied['optimizer_research']['trials']['optimizer']
        return {'winner_operation': 'baseline', 'reason': 'Retain higher-return bot', 'finish': finish,
                'optimizer_assessment': assessment(), 'variants': [{'config': changed_config(job['config']), 'overrides': {},
                'reason': 'Test alternative scoring at fixed work', 'experiment': {
                    'purpose': 'explore', 'budget_fraction': 1}}]}

    controller.decision = decide
    controller.backend = SimpleNamespace(capacity=lambda _: 1)
    asyncio.run(controller._tick(row))
    saved = store.read(row['owner'], row['id'])
    assert saved['best']['operation'] == 'baseline'
    assert saved['optimizer_assessment']['recommendations'][0]['operation'] == 'optimizer'
    if finish:
        assert saved['stop_reason'] == 'AI concluded the search'
        assert saved['status'] == 'completed'
    else:
        assert saved['phase'] == 'optimize' and saved['round'] == 1
        assert saved['jobs'][-1]['research']['seed'] != job['research']['seed']
        assert 'seed_index' not in saved['jobs'][-1]['research']


@pytest.mark.parametrize('remote_stopped', [False, True])
def test_controller_stops_historical_fractional_job_at_its_saved_quota(research, remote_stopped):
    """A 50% trial stops at 50k, and completion evidence uses that actual authorized target."""
    import time
    from pb8_loop_store import optimizer_execution_evidence
    store, row, controller = research
    job = optimizer(row, controller, 'optimizer')
    job.update(status='running', backend={'id': 'native', 'execution': 'gpu'}, started=True,
               run_started_at=time.time()-60, proxy_evaluations=60000, limit_reached=None)
    row.update(phase='optimize')
    row = store.update(row['owner'], row['id'], lambda current: current.update(row))
    stopped = []

    def poll(current, target):
        """Simulate bounded quota overshoot, with no process or real data access."""
        return {'status': 'cancelled' if stopped or remote_stopped else 'running', 'started': True,
                'proxy_evaluations': 60000, 'executed_config': job['config'], 'source': {}}

    controller.backend = SimpleNamespace(poll=poll, stop=lambda current, target: stopped.append(target['operation']))
    saved = asyncio.run(controller.sync_jobs(row))
    assert stopped == ([] if remote_stopped else ['optimizer'])
    actual = saved['jobs'][0]
    assert actual['limit_reached'] == 'proxy'
    result = optimizer_execution_evidence(actual, row['settings'], saved)
    assert result['completion_reason'] == 'proxy_limit_reached' and result['proxy_overshoot'] == 10000


def test_versioned_decision_requires_research_and_explicit_experiment_plan(research):
    """All providers share versioned validation; legacy prompts retain their old contract."""
    import json
    from pb8_loop_ai_v2 import decision_instructions, validate_decision
    _, row, controller = research
    assert 'Optimizer research v1 overrides' in decision_instructions(row)
    legacy = copy.deepcopy(row)
    legacy.pop('optimizer_research_version')
    assert 'Optimizer research v1 overrides' not in decision_instructions(legacy)
    job = optimizer(row, controller, 'optimizer')
    candidate = observation(row, 'optimizer', 'cpu', 3)
    supplied = {'observations': [candidate], 'winner_options': {'cpu': {}}, 'remaining_rounds': 0,
                'remaining_runs': 0}
    decision = {'winner_operation': 'cpu', 'reason': 'Qualified candidate', 'finish': True}
    with pytest.raises(ValueError, match='optimizer_assessment'):
        validate_decision(row, 'evaluate', supplied, json.dumps(decision))
    decision['optimizer_assessment'] = assessment()
    assert validate_decision(row, 'evaluate', supplied, json.dumps(decision))['winner_operation'] == 'cpu'
    model_config = copy.deepcopy(job['config'])
    model_config['optimize']['seed'] = None
    with pytest.raises(ValueError, match='explicit experiment'):
        validate_decision(row, 'bootstrap', {'max_variants': 1}, json.dumps({
            'variants': [{'config': model_config, 'overrides': {}, 'reason': 'Experiment without a plan'}]}))


def test_export_endpoint_is_owner_scoped_and_no_store(research, monkeypatch):
    """The same authenticated export route rejects another user's loop identity."""
    import json
    from fastapi import HTTPException
    from api import loop_optimizer_v8 as api
    store, row, controller = research
    optimizer(row, controller, 'optimizer')
    candidate = observation(row, 'optimizer', 'cpu', 3)
    row['optimizer_assessment'] = validate_assessment(row, [candidate], assessment())
    store.update(row['owner'], row['id'], lambda current: current.update(row))
    monkeypatch.setattr(api, '_controller', controller)
    monkeypatch.setattr(api, '_owner', lambda session: session)
    response = asyncio.run(api.optimizer_configs(row['id'], row['owner']))
    assert response.headers['cache-control'] == 'no-store'
    assert json.loads(response.body)['recommendations'][0]['operation'] == 'optimizer'
    with pytest.raises(HTTPException) as exc:
        asyncio.run(api.optimizer_configs(row['id'], 'c' * 32))
    assert exc.value.status_code == 404


def test_failed_training_jobs_are_separate_from_qualification_rate(research):
    """Do not inflate search quality by dropping failed independent validations."""
    _, row, controller = research
    optimizer(row, controller, 'optimizer')
    candidate = observation(row, 'optimizer', 'cpu', 3)
    row['jobs'].append({'kind': 'validation', 'operation': 'failed', 'status': 'failed',
                        'candidate': {'optimizer_job': 'optimizer'}})
    result = validate_assessment(row, [candidate], assessment())
    assert result['groups'][0]['qualification_rate'] == 1
    assert result['groups'][0]['technical_failure_count'] == 1
    # Even a colliding observer ID cannot smuggle metrics into training evidence.
    private = dict(candidate, observer_only=True, metrics={'gain': 9999})
    assert trial_records(row, [private])['optimizer']['outcomes'][0]['qualified'] is False


def test_repeat_plan_uses_effective_private_completion_filter(research):
    """Known GPU timestamp tolerance preserves repeat settings without seed planning."""
    _, row, controller = research
    config = copy.deepcopy(row['initial_config'])
    config['optimize']['limits'] = [{'metric': 'backtest_completion_ratio', 'penalize_if': 'less_than',
                                    'value': 1, 'reducer': 'min'}]
    first = controller.job(row, 'optimizer', config, {}, 'Complete reports',
                           experiment={'purpose': 'explore', 'budget_fraction': 1})
    row['jobs'].append(first)
    assert first['config']['optimize']['limits'][0]['value'] == .99
    assert first['research']['signature'] == signature(first['config'], {})
    with pytest.raises(ValueError, match='Unchanged optimizer settings'):
        experiment_plan(row, config, {}, {'purpose': 'explore', 'budget_fraction': 1})


def test_compact_trials_reload_changed_criteria_from_native_reports(research):
    """Hundreds of unselected metrics are catalogued once rather than duplicated per report."""
    import json
    _, row, controller = research
    optimizer(row, controller, 'optimizer')
    candidate = observation(row, 'optimizer', 'cpu', 3)
    for report in candidate['per_report']:
        report.update({f'extra_metric_{i}': i + .123 for i in range(300)})
    original = copy.deepcopy(candidate)
    row['optimizer_trials'] = trial_records(row, [candidate], assessment()['criteria'])
    persisted = row['optimizer_trials']['optimizer']
    assert len(persisted['metric_catalogue']) == 301
    assert all(set(r) == {'gain'} for r in persisted['outcomes'][0]['per_report'])
    assert len(json.dumps(persisted)) < len(json.dumps(original)) / 2
    row['phase'] = 'select'
    package = evidence(row)
    assert 'outcomes' not in package['trials']['optimizer'] and 'metric_catalogue' not in package['trials']['optimizer']
    assert 'extra_metric_17' in package['available_metrics']
    choice = assessment()
    choice['criteria'][0]['metric'] = 'extra_metric_17'
    loaded = []
    result = validate_assessment(row, [], choice,
        report_loader=lambda operation: (loaded.append(operation), original)[1], include_trials=True)
    assert loaded == ['cpu']
    assert result['groups'][0]['distribution'][0]['mean'] == pytest.approx(17.123)
    assert all(set(r) == {'extra_metric_17'} for r in result['_trials']['optimizer']['outcomes'][0]['per_report'])


def test_nonfinite_outlier_is_partial_coverage_not_permanent_metric_ban(research):
    """A single infinite value cannot remove a metric from all later comparable trials."""
    _, row, controller = research
    optimizer(row, controller, 'optimizer')
    first = observation(row, 'optimizer', 'first', 3)
    second = observation(row, 'optimizer', 'second', 4)
    for report in first['per_report']:
        report['sharpe'] = float('inf')
    for report in second['per_report']:
        report['sharpe'] = 2
    choice = assessment()
    choice['criteria'][0]['metric'] = 'sharpe'
    result = validate_assessment(row, [first, second], choice)
    group = result['groups'][0]
    assert group['measurement_coverage'] == .5
    assert group['qualified_count'] == 2 and group['distribution'][0]['mean'] == 2
    assert 'partial metric coverage' in group['uncertainty']
    assert evidence(dict(row, phase='evaluate'), [first, second])['metric_coverage']['sharpe'] == {'available': 1, 'qualified': 2}


def test_recovery_retries_cpu_validation_without_new_gpu_work(research, monkeypatch):
    """Technical failures recover automatically, preserving cause and paid-round allowance."""
    store, row, controller = research
    optimizer(row, controller, 'optimizer')
    failed = observation(row, 'optimizer', 'failed', 3)
    row['jobs'][-1].update(status='failed', error='Worker temporarily unavailable')
    row.update(phase='evaluate', observations=[], repair_error='Worker temporarily unavailable')
    row = store.update(row['owner'], row['id'], lambda r: r.update(row))
    clock = [row['created_at'] + 1]
    monkeypatch.setattr('pb8_loop_controller.time.time', lambda: clock[0])
    controller.backend = SimpleNamespace(validation_config=lambda current, candidate: copy.deepcopy(candidate['config']))

    async def no_ai(*_):
        """No paid search decision is needed to retry a failed CPU validation."""
        pytest.fail('Recovery must precede the next optimizer decision')

    controller.decision = no_ai
    asyncio.run(controller._tick(row))
    current = store.read(row['owner'], row['id'])
    assert current['status'] == 'running' and current['phase'] == 'recover_validation'
    assert current['repair_error'] == 'Worker temporarily unavailable'
    asyncio.run(controller._tick(current))
    assert len(store.read(row['owner'], row['id'])['jobs']) == 2
    clock[0] += 16
    asyncio.run(controller._tick(store.read(row['owner'], row['id'])))
    current = store.read(row['owner'], row['id'])
    assert current['phase'] == 'validate' and current['jobs'][-1]['recovery_of'] == 'failed'
    assert sum(j['kind'] == 'optimizer' for j in current['jobs']) == 1
    assert current['validation_recovery']['attempts'] == 1
    assert current['validation_count'] == row['validation_count'] + 1


def test_repeated_validation_failure_uses_unchanged_baseline_probe(research, monkeypatch):
    """After local retries distinguish a broken candidate from a broken training pipeline."""
    store, row, controller = research
    optimizer(row, controller, 'optimizer')
    observation(row, 'optimizer', 'failed', 3)
    row['jobs'][-1]['status'] = 'failed'
    row['baseline'] = {'candidate': {'id': 'baseline', 'optimizer_job': None,
                                     'config': row['initial_config'], 'overrides': {}}}
    row.update(phase='recover_validation', validation_recovery={
        'round': 0, 'attempts': 2, 'retry_at': row['created_at'] - 1, 'error': 'Bad candidate', 'history': [], 'active': True})
    row = store.update(row['owner'], row['id'], lambda r: r.update(row))
    controller.backend = SimpleNamespace(validation_config=lambda current, candidate: copy.deepcopy(candidate['config']))
    asyncio.run(controller._tick(row))
    retry = store.read(row['owner'], row['id'])['jobs'][-1]
    assert retry['diagnostic_only'] is True and retry['candidate']['id'] == 'baseline'
    assert retry['candidate']['config'] == row['initial_config']


@pytest.mark.parametrize('mode,value', [('hours', 4), ('iters', 1000), ('config', None)])
def test_nonproxy_research_projection_has_no_proxy_denominator(research, mode, value):
    """Hours and exact iteration budgets must never become a proxy-count denominator."""
    from api.loop_optimizer_v8 import projection
    _, row, controller = research
    job = optimizer(row, controller, 'optimizer')
    row['settings']['run_limit_mode'] = mode
    job['research']['budget'].update(mode=mode, value=value)
    job['proxy_evaluations'] = 123456
    assert projection(row)['jobs'][0]['proxy_limit'] is None


def test_recovery_preparation_failure_keeps_running_with_backoff(research):
    """Native preparation errors cannot fall into the generic three-error stop loop."""
    store, row, controller = research
    row.update(phase='recover_validation', validation_recovery={'round': 0, 'attempts': 0,
        'retry_at': row['created_at'] - 1, 'error': 'Temporary local input error', 'history': [], 'active': True})
    row = store.update(row['owner'], row['id'], lambda r: r.update(row))
    def unavailable(*_):
        """Simulate transient native compatibility/availability without real files."""
        raise OSError('Local helper temporarily unavailable')
    controller.backend = SimpleNamespace(validation_config=unavailable)
    asyncio.run(controller._tick(row))
    saved = store.read(row['owner'], row['id'])
    assert saved['status'] == 'running' and saved['phase'] == 'recover_validation'
    assert saved['validation_recovery']['attempts'] == 1
    assert saved['validation_recovery']['retry_at'] > row['created_at']
    assert saved['jobs'] == [] and saved['validation_count'] == 0


def test_initial_baseline_failure_uses_same_automatic_recovery(research):
    """Research cannot stop before its first optimizer because a local baseline failed once."""
    store, row, controller = research
    row.update(phase='baseline', after_baseline='bootstrap', jobs=[{'kind': 'baseline', 'round': 0,
        'operation': 'failed', 'status': 'failed', 'error': 'Temporary worker problem'}])
    row = store.update(row['owner'], row['id'], lambda r: r.update(row))
    async def synchronized(current):
        """Already failed fixture jobs need no native API access."""
        return current
    controller.sync_jobs = synchronized
    asyncio.run(controller._tick(row))
    saved = store.read(row['owner'], row['id'])
    assert saved['status'] == 'running' and saved['phase'] == 'recover_validation'
    assert saved['validation_recovery']['job_kind'] == 'baseline'


def test_recovery_respects_original_validation_allowance(research):
    """Automatic repair cannot consume unlimited local jobs or paid search attempts."""
    store, row, controller = research
    row.update(phase='recover_validation', validation_count=row['settings']['max_validations'],
               validation_recovery={'round': 0, 'attempts': 3, 'retry_at': row['created_at'] - 1,
                                    'error': 'No local worker', 'history': [], 'active': True})
    row = store.update(row['owner'], row['id'], lambda r: r.update(row))
    asyncio.run(controller._tick(row))
    saved = store.read(row['owner'], row['id'])
    assert 'Validation allowance reached' in saved['reason']
    assert saved['status'] == 'unconfirmed' and saved['jobs'] == []


def test_invalid_variant_schema_precedes_experiment_planning(research, monkeypatch):
    """A malformed model config produces a schema error before research accesses its fields."""
    import json
    from pb8_loop_ai_v2 import validate_decision
    _, row, _ = research
    monkeypatch.setattr('pb8_loop_research.experiment_plan', lambda *_: pytest.fail('Invalid config reached research planner'))
    with pytest.raises(ValueError):
        validate_decision(row, 'bootstrap', {'max_variants': 1}, json.dumps({
            'variants': [{'config': ['not', 'a', 'config'], 'experiment': {'purpose': 'explore'}, 'reason': 'Invalid'}]}))


def test_research_corrects_invalid_proposals_beyond_legacy_three_error_stop(research):
    """Correctable AI schema errors keep feedback and backoff without dispatching paid jobs."""
    store, row, controller = research
    row.update(phase='evaluate', errors=2)
    row = store.update(row['owner'], row['id'], lambda r: r.update(row))
    original_tick = controller._tick
    async def invalid(current):
        """Represent a malformed model proposal, not a transport or storage failure."""
        raise ValueError('Correct the unsupported optimizer objective')
    async def no_observers(current):
        """User-only diagnostics are unrelated to this isolated decision repair."""
    controller._tick = invalid
    controller.observers = SimpleNamespace(tick=no_observers)
    asyncio.run(controller.tick(row['owner'], row['id']))
    saved = store.read(row['owner'], row['id'])
    assert saved['status'] == 'running' and saved['errors'] == 3
    assert saved['decision_repair']['active'] and 'unsupported optimizer' in saved['repair_error']
    assert saved['jobs'] == []
    asyncio.run(original_tick(saved))
    assert store.read(row['owner'], row['id'])['phase'] == 'evaluate'


@pytest.mark.parametrize('active,old_kind', [(False, 'baseline'), (True, 'baseline'), (False, 'validation')])
def test_recovery_context_is_fresh_for_training_round_zero(research, active, old_kind):
    """Baseline recovery and completed episodes cannot redirect training back to bootstrap."""
    store, row, controller = research
    optimizer(row, controller, 'optimizer')
    observation(row, 'optimizer', 'failed', 3)
    row['jobs'][-1]['status'] = 'failed'
    row.update(phase='evaluate', observations=[], validation_recovery={
        'round': 0, 'active': active, 'job_kind': old_kind, 'attempts': 5,
        'retry_at': 0, 'history': [], 'error': 'Old baseline failure'})
    row = store.update(row['owner'], row['id'], lambda r: r.update(row))
    asyncio.run(controller._tick(row))
    saved = store.read(row['owner'], row['id'])
    state = saved['validation_recovery']
    assert state['job_kind'] == 'validation' and state['attempts'] == 0 and state['active']
    state['retry_at'] = 0
    saved = store.update(row['owner'], row['id'], lambda r: r.update(validation_recovery=state))
    controller.backend = SimpleNamespace(validation_config=lambda _, candidate: candidate['config'])
    asyncio.run(controller._tick(saved))
    saved = store.read(row['owner'], row['id'])
    assert saved['phase'] == 'validate'
    assert saved['jobs'][-1]['kind'] == 'validation'
    assert not any(j['kind'] == 'baseline' for j in saved['jobs'])


def test_recovery_retries_all_failed_candidates_and_preserves_partial_success(research):
    """A transient batch failure retries every candidate; a successful retry is not repeated."""
    store, row, controller = research
    optimizer(row, controller, 'optimizer')
    for i in range(4):
        observation(row, 'optimizer', f'cpu{i}', 3)
        row['jobs'][-1]['status'] = 'failed'
    row.update(phase='recover_validation', observations=[], validation_recovery={
        'round': 0, 'active': True, 'job_kind': 'validation', 'attempts': 0,
        'retry_at': 0, 'history': [], 'error': 'Transient worker failure'})
    row = store.update(row['owner'], row['id'], lambda r: r.update(row))
    controller.backend = SimpleNamespace(validation_config=lambda _, candidate: candidate['config'])
    asyncio.run(controller._tick(row))
    saved = store.read(row['owner'], row['id'])
    retries = saved['jobs'][-4:]
    assert {j['recovery_of'] for j in retries} == {f'cpu{i}' for i in range(4)}
    assert all(not j['diagnostic_only'] for j in retries)
    assert saved['validation_count'] == row['validation_count'] + 4
    success = {'operation': retries[0]['operation'], 'exact': True, 'per_report': [{'gain': 3}],
               'assessment': {'comparable': True, 'simulation_complete': True}}
    for job in retries:
        job['status'] = 'failed'
    retries[0]['status'] = 'complete'
    saved.update(phase='evaluate', observations=[success])
    saved['validation_recovery']['retry_at'] = 0
    saved = store.update(row['owner'], row['id'], lambda r: r.update(saved))
    asyncio.run(controller._tick(saved))
    final = store.read(row['owner'], row['id'])
    assert final['phase'] == 'validate'
    assert {j['recovery_of'] for j in final['jobs'][-3:]} == {j['operation'] for j in retries[1:]}
    assert final['validation_recovery']['attempts'] == 2


def test_diagnostic_success_starts_next_round_with_fresh_error_counter(research):
    """A repaired pipeline cannot carry schema failures into the next optimizer stage."""
    store, row, controller = research
    job = optimizer(row, controller, 'optimizer')
    probe = observation(row, None, 'probe', 3)
    row['jobs'][-1]['diagnostic_only'] = True
    baseline = dict(probe, operation='baseline', reports=1)
    row.update(phase='evaluate', observations=[probe], baseline=baseline, best=baseline,
               errors=2, repair_error='Previous proposal errors',
               validation_recovery={'round': 0, 'active': True, 'attempts': 3, 'history': [{'error': 'Pipeline failure'}]},
               rubric=[{'goal': 'gain', 'metrics': ['gain'], 'direction': 'max', 'target': None}])
    row = store.update(row['owner'], row['id'], lambda r: r.update(row))
    async def decide(*_):
        """A healthy probe permits a revised search without crediting its measurements."""
        choice = assessment()
        choice['recommendations'] = []
        return {'winner_operation': 'baseline', 'reason': 'Retain baseline', 'finish': False,
                'optimizer_assessment': choice, 'variants': [{'config': changed_config(job['config']), 'overrides': {},
                'reason': 'Test changed search settings after diagnosis', 'experiment': {'purpose': 'explore', 'budget_fraction': 1}}]}
    controller.decision = decide
    controller.backend = SimpleNamespace(capacity=lambda _: 1)
    asyncio.run(controller._tick(row))
    saved = store.read(row['owner'], row['id'])
    assert saved['round'] == 1 and saved['phase'] == 'optimize'
    assert saved['errors'] == 0 and saved['repair_error'] is None
    assert saved['validation_recovery']['history'] == [{'error': 'Pipeline failure'}]


@pytest.mark.parametrize('failure', [FileNotFoundError, ValueError])
def test_missing_native_report_does_not_repeat_reassessment_or_discard_cache(research, failure):
    """Unavailable historical sources reduce evidence without retrying paid AI decisions."""
    _, row, controller = research
    optimizer(row, controller, 'optimizer')
    candidate = observation(row, 'optimizer', 'cpu', 3)
    for report in candidate['per_report']:
        report['sharpe'] = 2
    row['optimizer_trials'] = trial_records(row, [candidate], assessment()['criteria'])
    row['phase'] = 'select'
    choice = assessment()
    choice['criteria'][0]['metric'] = 'sharpe'
    calls = []
    def missing(operation):
        """Represent removed/corrupt artifacts without accessing user reports."""
        calls.append(operation)
        raise failure('Original report unavailable')
    result = validate_assessment(row, [], choice, report_loader=missing, include_trials=True)
    assert calls == ['cpu'] and result['recommendations'] == []
    assert 'withheld' in result['reason']
    row['optimizer_trials'] = result['_trials']
    assert row['optimizer_trials']['optimizer']['outcomes'][0]['per_report'][0]['gain'] == 3
    assert 'sharpe' not in evidence(row)['available_metrics']
    assert evidence(row)['trials']['optimizer']['unavailable_report_count'] == 1
    result = validate_assessment(row, [], choice, report_loader=missing, include_trials=True)
    assert calls == ['cpu'] and result['recommendations'] == []
    assert validate_assessment(row, [], assessment())['recommendations'][0]['operation'] == 'optimizer'
    forged = copy.deepcopy(choice)
    forged['criteria'][0]['metric'] = 'invented_metric'
    with pytest.raises(ValueError, match='unavailable in all'):
        validate_assessment(row, [], forged)
    forged = copy.deepcopy(choice)
    forged['recommendations'][0]['operation'] = 'unknown_optimizer'
    with pytest.raises(ValueError, match='verified optimizer'):
        validate_assessment(row, [], forged)



def test_recovery_batch_never_exceeds_remaining_cpu_allowance(research):
    """Batch repair keeps the reviewed global validation ceiling."""
    store, row, controller = research
    for i in range(4):
        observation(row, 'optimizer', f'failed{i}', 3)
        row['jobs'][-1]['status'] = 'failed'
    row.update(phase='recover_validation', validation_count=row['settings']['max_validations'] - 2,
               validation_recovery={'round': 0, 'active': True, 'job_kind': 'validation',
                                    'attempts': 0, 'retry_at': 0, 'history': [], 'error': 'Worker failure'})
    row = store.update(row['owner'], row['id'], lambda r: r.update(row))
    controller.backend = SimpleNamespace(validation_config=lambda _, candidate: candidate['config'])
    asyncio.run(controller._tick(row))
    saved = store.read(row['owner'], row['id'])
    assert len(saved['jobs']) == 6
    assert saved['validation_count'] == row['settings']['max_validations']


@pytest.mark.parametrize('provider', ['chatgpt', 'claude-code', 'opencode-go', 'opencode-zen'])
def test_every_provider_must_address_measured_boundary_pressure(research, provider):
    """The actual shared decision gate requires a response before accepting another paid trial."""
    import json
    from pb8_loop_ai_v2 import validate_decision
    from pb8_loop_bounds import diagnose
    _, row, controller = research
    row['settings']['provider'] = provider
    bounds = row['initial_config']['optimize'].setdefault('bounds', {}).setdefault('long', {})
    bounds.setdefault('strategy', {}).setdefault('ema_anchor', {})['base_qty_pct'] = [.001, .05]
    job = optimizer(row, controller, 'optimizer')
    candidate = observation(row, 'optimizer', 'cpu', 3)
    candidate['candidate']['config'] = copy.deepcopy(row['initial_config'])
    candidate['candidate']['config']['bot']['long'].setdefault('strategy', {}).setdefault('ema_anchor', {})['base_qty_pct'] = .049
    row['best'] = candidate
    supplied = {'observations': [candidate], 'winner_options': {'cpu': candidate},
                'remaining_rounds': 1, 'remaining_runs': 1, 'max_variants': 1}
    diagnosis = diagnose(row, supplied['winner_options'])
    assert diagnosis['signal_count']
    variant = {'config': changed_config(job['config']), 'overrides': {},
               'experiment': {'purpose': 'explore', 'budget_fraction': 1},
               'reason': 'Keep this single-seed range while assessing another hypothesis'}
    variant['config']['optimize']['seed'] = None
    decision = {'winner_operation': 'cpu', 'reason': 'Qualified CPU winner',
                'optimizer_assessment': assessment(), 'variants': [variant]}
    with pytest.raises(ValueError, match='boundary_response'):
        validate_decision(row, 'evaluate', supplied, json.dumps(decision))
    variant['boundary_response'] = [{'id': r['id'], 'action': 'retain',
                                    'reason': 'One measured seed does not establish repeatability'}
                                   for r in diagnosis['parameters']]
    assert validate_decision(row, 'evaluate', supplied, json.dumps(decision))['winner_operation'] == 'cpu'
    supplied['boundary_diagnostics'] = copy.deepcopy(diagnosis)
    supplied['boundary_diagnostics']['unchanged_bounds_trials'] = 3
    for signal in supplied['boundary_diagnostics']['parameters']:
        signal.update(strength='repeated', incumbent_near=True, quality_basis='top_quartile_with_ties')
    row['stagnation'] = 1
    # The still-unrecorded current evaluation is the second stagnant evaluation.
    with pytest.raises(ValueError, match='all relevant untested boundary'):
        validate_decision(row, 'evaluate', supplied, json.dumps(decision))
    row['history'] = [{'round': row['round']}]
    assert validate_decision(row, 'evaluate', supplied, json.dumps(decision))['winner_operation'] == 'cpu'
    row['history'] = []
    row['stagnation'] = 0
    assert validate_decision(row, 'evaluate', supplied, json.dumps(decision))['winner_operation'] == 'cpu'
    supplied['remaining_rounds'] = 0
    decision.update(variants=[], finish=True)
    assert validate_decision(row, 'evaluate', supplied, json.dumps(decision))['finish']


def test_dotted_bounds_patch_updates_native_nested_parameter_instead_of_shadow(research):
    """Diagnostic dotted names must change the actual PB8 bounds consumed by normalization."""
    from pb8_loop_research import resolve_variant
    _, row, _ = research
    row['initial_config']['optimize']['bounds'] = {'long': {'unstuck': {'threshold': [.4, .95, .001]},
        'strategy': {'ema_anchor': {'offset_volatility_ema_span_1h': [1, 672, 1]}}}, 'short': {}}
    before = copy.deepcopy(row['initial_config'])
    resolved = resolve_variant(row, {'base_optimizer': 'initial', 'optimizer_patch': {'bounds': {
        'long.unstuck.threshold': [.345, .95, .001],
        'long.strategy.ema_anchor.offset_volatility_ema_span_1h': [1, 740, 1]}}})
    bounds = resolved['config']['optimize']['bounds']
    assert bounds['long']['unstuck']['threshold'] == [.345, .95, .001]
    assert bounds['long']['strategy']['ema_anchor']['offset_volatility_ema_span_1h'] == [1, 740, 1]
    assert set(bounds) == {'long', 'short'}
    assert row['initial_config'] == before
    execution_check(resolved['config'], copy.deepcopy(resolved['config']), {})


@pytest.mark.parametrize('bounds', [
    {'long': {'unstuck': {'threshold': [0, 1]}}, 'long.unstuck.threshold': [0, 2]},
    {'long.unstuck': None, 'long.unstuck.threshold': [0, 2]},
    {'long..unstuck.threshold': [0, 2]},
])
def test_ambiguous_or_malformed_bounds_paths_rejected_before_job_creation(research, bounds):
    """Conflicting representations cannot pass planning then disappear during preparation."""
    from pb8_loop_research import resolve_variant
    _, row, _ = research
    with pytest.raises(ValueError, match='parameter path'):
        resolve_variant(row, {'base_optimizer': 'initial', 'optimizer_patch': {'bounds': bounds}})


def test_dotted_bounds_deletion_and_nested_empty_patch_preserve_unrelated_controls():
    """Null deletion is canonical while an empty sparse object changes no existing bound."""
    from pb8_loop_research import _merge_bounds
    original = {'long': {'one': [0, 1], 'two': [2, 3]}, 'short': {}}
    assert _merge_bounds(original, {'long': {}}) == original
    assert _merge_bounds(original, {'long.one': None, 'short': {}}) == {
        'long': {'two': [2, 3]}, 'short': {}}


def test_full_config_dotted_bounds_are_canonical_and_conflicting_shadow_keys_rejected(research):
    """Full variants obey the same bounds representation boundary as compact patches."""
    from pb8_loop_research import resolve_variant
    _, row, _ = research
    config = copy.deepcopy(row['initial_config'])
    config['optimize']['bounds'] = {'long.unstuck.threshold': [.3, .9, .001]}
    assert resolve_variant(row, {'config': config})['config']['optimize']['bounds'] == {
        'long': {'unstuck': {'threshold': [.3, .9, .001]}}}
    config['optimize']['bounds']['long'] = {'unstuck': {'threshold': [.4, .9, .001]}}
    with pytest.raises(ValueError, match='conflicting'):
        resolve_variant(row, {'config': config})
