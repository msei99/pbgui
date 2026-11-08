"""Offline native CPU reuse contracts; no cloud jobs or production data are touched."""
import asyncio
import copy

import pytest

from pb8_loop_backend import LoopBackend
from pb8_loop_native import observation, select, enabled
from pb8_loop_observer import ObserverJobs
from pb8_loop_store import qualified_winner
from tests.test_pb8_loop_research import research, optimizer, changed_config


@pytest.fixture
def native(research):
    """Build an owned verified optimizer and two explicit exact training windows."""
    store, row, controller = research
    row['settings']['execution'] = 'vast'
    row['rubric'] = [dict(goal='gain', metrics=['gain'], direction='max', target=None),
                     dict(goal='drawdown', metrics=['drawdown'], direction='min', target=.5)]
    row['initial_config']['backtest'].update(suite_enabled=True, scenarios=[
        dict(label='a', start_date='2020-01-01', end_date='2020-01-10'),
        dict(label='b', start_date='2020-01-11', end_date='2020-02-01')])
    row['comparison'] = copy.deepcopy(row['initial_config']['backtest'])
    job = optimizer(row, controller, 'source')
    job['backend'] = dict(id='owned-job', execution='vast')
    data = copy.deepcopy(job['executed_config'])
    data['backtest']['coins'] = {'combined': ['BTC', 'ETH']}
    data['optimizer_evaluation_contract'] = dict(version=2, implementation=dict(python_source_sha256='a' * 64))
    data['metrics'] = dict(objectives=dict(w_0=-3), gpu_validation=dict(exact_objectives=[-2]))
    suite = {key: dict(scenarios=dict(zip(['a', 'b'], values))) for key, values in
             dict(gain=[2, 4], drawdown=[.2, .49], backtest_completion_ratio=[1, 1], liquidated_usd=[0, 0]).items()}
    return store, row, controller, job, data, suite


def result(native):
    """Read the fixture exactly as the imported native backend does."""
    _, row, _, job, data, suite = native
    return observation(row, job, data, suite, ['a', 'b'])


def candidates(native, count=8):
    """Create distinct bot configs with measured gain and immutable source references."""
    _, row, _, job, _, _ = native
    outcome = result(native)
    values = []
    for index in range(count):
        config = copy.deepcopy(job['config'])
        config['bot']['long']['risk']['total_wallet_exposure_limit'] = index + 1
        measured = copy.deepcopy(outcome)
        for report in measured['per_report']:
            report['gain'] = index + 1
        measured['metrics']['gain'] = index + 1
        values.append(dict(id='c' + str(index), optimizer_job=job['operation'], config=config,
                           overrides={}, native_training=measured))
    return values


def test_exact_reports_preserve_worst_window_safety_and_source(native):
    """Native scaled objectives need not equal raw objectives; safety is independently checked."""
    outcome = result(native)
    assert qualified_winner(outcome)
    assert outcome['metrics']['gain'] == 3
    assert outcome['metrics']['drawdown'] == .49
    assert outcome['training_source']['vast_job_id'] == 'owned-job'
    assert outcome['metric_basis']['source'] == 'vast_cpu_exact'
    assert outcome['reports'] == 2
    assert 'sharpe' not in outcome['metrics']


@pytest.mark.parametrize('change', ['proxy', 'contract', 'unverified', 'owner', 'dates', 'fees', 'coins', 'labels', 'completion', 'liquidation'])
def test_unproven_or_mismatched_native_evidence_is_never_accepted(native, change):
    """Missing exact/safety/provenance fields cannot become a qualified CPU observation."""
    _, row, _, job, data, suite = native
    if change == 'proxy': data['metrics'].pop('gpu_validation')
    if change == 'contract': data.pop('optimizer_evaluation_contract')
    if change == 'unverified': job['research_verification'] = {}
    if change == 'owner': job['backend']['id'] = None
    if change == 'dates': data['backtest']['scenarios'][0]['end_date'] = '2020-01-09'
    if change == 'fees': data['backtest']['maker_fee_override'] = .01
    if change == 'coins': data['backtest']['coins'] = {'combined': ['BTC']}
    if change == 'labels': data['backtest']['scenarios'][1]['label'] = 'a'
    if change == 'completion': suite['backtest_completion_ratio']['scenarios'].pop('b')
    if change == 'liquidation': suite.pop('liquidated_usd')
    with pytest.raises(ValueError): result(native)


@pytest.mark.parametrize('field,value', [('drawdown', .6), ('liquidated_usd', 1), ('backtest_completion_ratio', .9)])
def test_unsafe_or_incomplete_window_remains_exact_but_cannot_qualify(native, field, value):
    """A good aggregate never hides one unsafe/incomplete training window."""
    native[-1][field]['scenarios']['b'] = value
    assert result(native)['exact']
    assert not qualified_winner(result(native))


def test_native_date_normalization_preserves_exact_time(native):
    """Equivalent ISO dates compare equal, while a changed time is rejected."""
    native[4]['backtest']['start_date'] += 'T00:00:00+00:00'
    assert qualified_winner(result(native))
    native[4]['backtest']['start_date'] = '2020-01-01T01:00:00+00:00'
    with pytest.raises(ValueError): result(native)


def test_single_window_requires_explicit_cpu_safety(native):
    """Single-window exact reports reuse finite measurements without invented fields."""
    _, row, _, job, data, _ = native
    for config in [job['config'], job['executed_config'], data]:
        config['backtest']['suite_enabled'] = False
        config['backtest'].pop('scenarios')
    row['comparison'] = copy.deepcopy(job['config']['backtest'])
    data['metrics']['liquidated'] = False
    report = dict(gain=3, drawdown=.4, backtest_completion_ratio=1)
    assert qualified_winner(observation(row, job, data, {}, [], report))
    report.pop('backtest_completion_ratio')
    with pytest.raises(ValueError): observation(row, job, data, {}, [], report)


def test_top_five_distinct_configs_obey_hard_limits_and_requested_criteria(native):
    """High gain with excessive risk is excluded, and aliases do not consume five slots."""
    _, row, _, _, _, _ = native
    values = candidates(native)
    values[-1]['native_training']['assessment']['hard_targets_met'] = False
    values.append(copy.deepcopy(values[-2]))
    values[-1]['id'] = 'alias'
    chosen, summary = select(row, values)
    assert [c['id'] for c in chosen] == ['c6', 'c5', 'c4', 'c3', 'c2']
    assert summary['qualified'] == 8
    row['evaluation_method'] = [dict(metric='gain', direction='min', reducer='min')]
    assert select(row, values)[0][0]['id'] == 'c0'


def test_controller_reuses_native_results_without_any_local_execution(native, monkeypatch):
    """Five durable exact checkpoints advance directly to evaluate and background reports."""
    store, row, controller, _, _, _ = native
    values = candidates(native)
    row.update(phase='select', status='running', candidates=values, validation_count=1)
    row = store.update(row['owner'], row['id'], lambda current: current.update(row))
    def forbidden(*args, **kwargs):
        """No prepare/start/local observations or model selection is needed."""
        raise AssertionError('duplicate execution')
    monkeypatch.setattr(controller.backend, 'prepare', forbidden)
    monkeypatch.setattr(controller.backend, 'start', forbidden)
    monkeypatch.setattr(controller, 'decision', forbidden)
    monkeypatch.setattr(controller.backend, 'validation_config', lambda r, c, holdout=False: copy.deepcopy(c['config']))
    asyncio.run(controller.import_native_training(row))
    current = store.read(row['owner'], row['id'])
    assert current['phase'] == 'evaluate'
    assert current['validation_count'] == 1
    checks = [job for job in current['jobs'] if job.get('native_reused')]
    assert len(checks) == len(current['observations']) == 5
    assert all(job['status'] == 'complete' and not job['started'] and not job['backend'] for job in checks)
    assert len(ObserverJobs.sources(current)) == 5
    assert LoopBackend.observations(controller.backend, current, checks[0]) == checks[0]['native_observation']
    from api.loop_optimizer_v8 import projection
    public = projection(current)
    reused = [job for job in public['jobs'] if job['kind'] == 'validation']
    assert len(reused) == 5
    assert all(job['execution'] == 'vast_cpu' and not job['log_available'] for job in reused)
    assert public['native_training_summary']['selected'] == 5
    from ai_loop_search_limits import validation_budget
    current['validation_count'] = current['settings']['max_validations']
    assert validation_budget(current, 8)['max_candidates'] == 5
    current['settings']['execution'] = 'gpu'
    assert not enabled(current)
    assert validation_budget(current, 8)['max_candidates'] == 0


@pytest.mark.parametrize('missing', [False, True])
def test_missing_native_reports_use_bounded_retries_then_local_fallback(native, monkeypatch, missing):
    """Failed import retries preserve cached configs and never launch another paid experiment."""
    store, row, controller, _, _, _ = native
    values = [] if missing else candidates(native)
    for candidate in values:
        candidate.pop('native_training')
        candidate['native_training_error'] = 'missing exact report'
    row.update(phase='select', status='running', observer_enabled=False, candidates=values)
    row = store.update(row['owner'], row['id'], lambda current: current.update(row))
    def forbidden(*args):
        """Retries cannot prepare or start local/cloud jobs prematurely."""
        raise AssertionError('unexpected work before fallback')
    monkeypatch.setattr(controller.backend, 'prepare', forbidden)
    monkeypatch.setattr(controller.backend, 'start', forbidden)
    asyncio.run(controller.import_native_training(row))
    current = store.read(row['owner'], row['id'])
    assert current['status'] == 'running' and current['native_training_retry']['attempts'] == 1
    assert current['native_training_retry']['retry_at'] > 0
    def still_broken(*args):
        """The cached configs remain usable locally when native rereading fails."""
        raise ValueError('unsupported native contract')
    monkeypatch.setattr(controller.backend, 'candidates', still_broken)
    asyncio.run(controller.import_native_training(current))
    assert store.read(row['owner'], row['id'])['native_training_retry']['attempts'] == 1
    for attempt in (2, 3):
        current = store.update(row['owner'], row['id'], lambda r: r['native_training_retry'].update(retry_at=0))
        asyncio.run(controller.import_native_training(current))
    current = store.read(row['owner'], row['id'])
    assert current['status'] == 'running' and current['native_training_fallback']['attempts'] == 3
    assert current['native_training_retry'] is None
    assert current['phase'] == ('evaluate' if missing else 'select')
    assert len(current['candidates']) == len(values)
    assert len(current['jobs']) == 1 and current['validation_count'] == 0
    assert not enabled(current)
    assert enabled({**current, 'round': current['round'] + 1})


def test_fast_round_evaluates_and_prepares_next_optimizer_while_reports_are_pending(native, monkeypatch):
    """No observer completion or second CPU training run gates the next optimizer."""
    store, row, controller, source, _, _ = native
    row.update(phase='select', status='running', candidates=candidates(native), validation_count=1)
    row = store.update(row['owner'], row['id'], lambda current: current.update(row))
    monkeypatch.setattr(controller.backend, 'validation_config', lambda r, c, holdout=False: copy.deepcopy(c['config']))
    monkeypatch.setattr(controller.backend, 'capacity', lambda r: 1)
    async def decision(current, stage, evidence):
        """Evaluate actual reused observations and request a source-owned fresh trial."""
        assert stage == 'evaluate'
        assert 'observer_jobs' not in evidence
        assert all('native_training' not in c for c in evidence['native_population'])
        options = evidence['winner_options']
        winner = max(options, key=lambda k: options[k]['metrics']['gain'])
        return dict(winner_operation=winner, reason='Best qualified native gain', finish=False,
                    knowledge='Exact CPU gain reused', optimizer_assessment=dict(
                        criteria=[dict(metric='gain', direction='max', reducer='mean', reason='Return')],
                        recommendations=[dict(operation=source['operation'], reason='Measured exact return')],
                        reason='Short training evidence'), variants=[dict(reason='Test distinct scoring at fixed work', config=changed_config(source['config']), overrides={},
                        experiment=dict(purpose='explore', budget_fraction=1))])
    monkeypatch.setattr(controller, 'decision', decision)
    async def run():
        """Run the real select/evaluate transitions, never native preparation/start."""
        await controller._tick(row)
        current = store.read(row['owner'], row['id'])
        await controller._tick(current)
    asyncio.run(run())
    current = store.read(row['owner'], row['id'])
    assert current['phase'] == 'optimize' and current['round'] == 1
    assert current['best']['metrics']['gain'] == 8
    assert current['validation_count'] == 1
    assert len(current['history']) == 1
    assert current['jobs'][-1]['kind'] == 'optimizer' and not current['jobs'][-1]['started']
    assert current['optimizer_trials'][source['operation']]['native_cpu_population']['verified'] == 8
    current['observer_holdouts'] = [dict(label='holdout', start_date='2020-02-02', end_date='2020-03-01')]
    current['observer_enabled'] = True
    planned = controller.observers.plan(current)
    assert len(planned['observer_jobs']) == 10
    assert {job['kind'] for job in planned['observer_jobs']} == {'observer_holdout', 'observer_full_range'}
    assert all(job['status'] == 'planned' for job in planned['observer_jobs'])


def test_all_unsafe_native_candidates_skip_local_recovery(native, monkeypatch):
    """An exact unsafe population remains useful negative evidence, without repeated backtests."""
    store, row, controller, _, _, _ = native
    values = candidates(native)
    for candidate in values:
        candidate['native_training']['assessment']['hard_targets_met'] = False
    row.update(phase='select', status='running', candidates=values)
    row = store.update(row['owner'], row['id'], lambda current: current.update(row))
    asyncio.run(controller.import_native_training(row))
    current = store.read(row['owner'], row['id'])
    assert current['phase'] == 'evaluate' and current['observations'] == []
    assert current['native_training_summary']['qualified'] == 0
    assert len(current['jobs']) == 1
    from pb8_loop_research import evidence
    info = evidence(current)
    assert info['trials'][row['jobs'][0]['operation']]['qualified_count'] == 0


def test_native_population_qualification_is_not_selected_top_five_success_rate(native, monkeypatch):
    """The research summary counts loaded safe/unsafe native results, not just chosen winners."""
    store, row, controller, _, _, _ = native
    values = candidates(native, 10)
    for candidate in values[:4]:
        candidate['native_training']['assessment']['hard_targets_met'] = False
    row.update(phase='select', status='running', candidates=values)
    row = store.update(row['owner'], row['id'], lambda current: current.update(row))
    monkeypatch.setattr(controller.backend, 'validation_config', lambda r, c, holdout=False: copy.deepcopy(c['config']))
    asyncio.run(controller.import_native_training(row))
    current = store.read(row['owner'], row['id'])
    from pb8_loop_research import trial_records, summarize
    criteria = [dict(metric='gain', direction='max', reducer='mean')]
    trials = trial_records(current, current['observations'], criteria=criteria)
    group = summarize(trials, criteria)[0]
    assert group['qualification_rate'] == .6
    assert group['validated_count'] == 10 and group['qualified_count'] == 6
    assert group['measured_candidate_count'] == 5


def test_import_retry_keeps_gpu_replacement_successor(native, monkeypatch):
    """A successor is the current owned trial and must not disappear during import recovery."""
    store, row, controller, source, _, _ = native
    source['gpu_replacement_successor'] = 'replacement-id'
    row.update(phase='select', status='running', candidates=[],
               native_training_retry=dict(round=0, attempts=1, retry_at=0))
    row = store.update(row['owner'], row['id'], lambda current: current.update(row))
    seen = []
    def reload(current, job):
        """Read only the synthetic successor result."""
        seen.append(job['operation'])
        return candidates(native)
    monkeypatch.setattr(controller.backend, 'candidates', reload)
    monkeypatch.setattr(controller.backend, 'validation_config', lambda r, c: c['config'])
    asyncio.run(controller.import_native_training(row))
    current = store.read(row['owner'], row['id'])
    assert seen == [source['operation']]
    assert current['phase'] == 'evaluate' and current['native_training_retry'] is None


def test_unavailable_ranking_uses_existing_reports_and_deduplicates_before_ai(native, monkeypatch):
    """A qualitative ranking can select existing exact configs without any new simulation."""
    store, row, controller, _, _, _ = native
    values = candidates(native, 4)
    values.append(copy.deepcopy(values[0]))
    values[-1]['id'] = 'alias'
    row.update(phase='select', status='running', candidates=values,
               evaluation_method=[dict(metric='missing_metric', direction='max', reducer='mean')])
    row = store.update(row['owner'], row['id'], lambda current: current.update(row))
    async def choose(current, stage, evidence):
        """Choose only unique already-qualified results."""
        assert stage == 'select' and evidence['max_candidates'] == 4
        assert len(evidence['candidates']) == 4
        return dict(candidate_ids=[c['id'] for c in evidence['candidates']], reason='Compare existing exact reports')
    monkeypatch.setattr(controller, 'decision', choose)
    monkeypatch.setattr(controller.backend, 'validation_config', lambda r, c: c['config'])
    asyncio.run(controller.import_native_training(row))
    current = store.read(row['owner'], row['id'])
    assert len(current['observations']) == 4
    assert current['validation_count'] == 0


def test_native_limits_are_checked_for_every_window_including_minimum_return(native):
    """Average return cannot mask a hard minimum target violated in one scenario."""
    native[1]['rubric'][0]['target'] = 3
    assert result(native)['metrics']['gain'] == 3
    assert not qualified_winner(result(native))


@pytest.mark.parametrize('violation', ['twe', 'hsl'])
def test_reused_checkpoints_apply_same_candidate_safety_checks_as_local_jobs(native, monkeypatch, violation):
    """Native reuse cannot bypass either reviewed exposure or a fixed HSL policy."""
    store, row, controller, _, _, _ = native
    values = candidates(native)
    if violation == 'twe':
        row['settings']['goals']['max_twe'] = 4
    else:
        row['settings']['hsl_mode'] = 'off'
        values[-1]['config']['bot']['long'].setdefault('hsl', {})['enabled'] = True
    row.update(phase='select', status='running', candidates=values)
    row = store.update(row['owner'], row['id'], lambda current: current.update(row))
    monkeypatch.setattr(controller.backend, 'validation_config', lambda r, c: c['config'])
    from pb8_loop_evidence import LoopAIPause
    with pytest.raises(LoopAIPause, match='safety check failed'):
        asyncio.run(controller.import_native_training(row))
    current = store.read(row['owner'], row['id'])
    assert len(current['jobs']) == 1 and not current.get('observations')


def test_successful_retry_restores_native_reports_and_clears_import_error(native, monkeypatch):
    """A repaired import resumes native evaluation and clears stale source errors."""
    store, row, controller, _, _, _ = native
    row['jobs'][0].update(result_error='old missing report', results_consumed=False, result_count=0)
    row.update(phase='select', status='running', candidates=[], native_training_retry=dict(
        round=0, attempts=1, retry_at=0))
    row = store.update(row['owner'], row['id'], lambda current: current.update(row))
    monkeypatch.setattr(controller.backend, 'candidates', lambda *args: candidates(native))
    monkeypatch.setattr(controller.backend, 'validation_config', lambda r, c: c['config'])
    asyncio.run(controller.import_native_training(row))
    current = store.read(row['owner'], row['id'])
    assert current['phase'] == 'evaluate' and current['native_training_retry'] is None
    assert current['jobs'][0]['result_error'] is None
    assert current['jobs'][0]['results_consumed'] and current['jobs'][0]['result_count'] == 8


def test_absent_imported_vast_result_reaches_diagnostic_recovery_without_a_paid_rerun(native, monkeypatch):
    """An absent result folder gets bounded import retries, then the existing diagnostic path."""
    store, row, controller, source, _, _ = native
    source.update(status='complete', limit_reached=None)
    row.update(phase='optimize', status='running', observer_enabled=False, baseline_required=False)
    row = store.update(row['owner'], row['id'], lambda current: current.update(row))
    async def unchanged(current, launch=True):
        """All synthetic optimizer work has already finished."""
        return current
    async def no_observers(*args):
        """No real processes are involved in this missing-report fixture."""
    def missing(*args):
        """Model the backend's missing/unattributable result error."""
        raise ValueError('Optimizer result is not yet uniquely attributable to its loop job')
    monkeypatch.setattr(controller, 'sync_jobs', unchanged)
    monkeypatch.setattr(controller.observers, 'tick', no_observers)
    monkeypatch.setattr(controller.backend, 'candidates', missing)
    async def run():
        """Collect the missing source, exhaust bounded retries, and enter CPU diagnosis."""
        await controller.tick(row['owner'], row['id'])
        for _ in range(3):
            store.update(row['owner'], row['id'], lambda current: (current.get('native_training_retry') or {}).update(retry_at=0))
            await controller.tick(row['owner'], row['id'])
        current = store.read(row['owner'], row['id'])
        assert current['phase'] == 'evaluate' and current['native_training_fallback']['active']
        await controller.tick(row['owner'], row['id'])
    asyncio.run(run())
    current = store.read(row['owner'], row['id'])
    assert current['status'] == 'running' and current['phase'] == 'recover_validation'
    assert 'uniquely attributable' in current['repair_error']
    assert len(current['jobs']) == 1 and current['validation_count'] == 0
    assert current['validation_recovery']['active'] and not current.get('decision_repair')


@pytest.mark.parametrize('remaining', [2, 10])
def test_local_fallback_creates_bounded_cpu_jobs_and_honest_research_counts(native, monkeypatch, remaining):
    """Fallback charges the existing allowance and cannot report zero native coverage as CPU failure."""
    store, row, controller, source, _, _ = native
    values = candidates(native)
    for candidate in values:
        candidate.pop('native_training')
        candidate['native_training_error'] = 'unsupported contract'
    row.update(phase='select', status='running', candidates=values,
               native_training_retry=dict(round=0, attempts=2, retry_at=0))
    row['settings']['max_validations'] = remaining + 1
    row['validation_count'] = 1
    row = store.update(row['owner'], row['id'], lambda current: current.update(row))
    monkeypatch.setattr(controller.backend, 'candidates', lambda *args: values)
    monkeypatch.setattr(controller.backend, 'validation_config', lambda r, c: c['config'])
    async def choose(current, stage, evidence):
        """Spend at most five validations, never the rest of a larger allowance."""
        assert stage == 'select' and evidence['max_candidates'] == min(5, remaining)
        return dict(candidate_ids=[c['id'] for c in evidence['candidates'][:min(5, remaining)]],
                    reason='Check native candidates locally after failed exact import')
    monkeypatch.setattr(controller, 'decision', choose)
    asyncio.run(controller._tick(row))
    current = store.read(row['owner'], row['id'])
    assert current['phase'] == 'validate' and current['validation_count'] == 1 + min(5, remaining)
    checks = [job for job in current['jobs'] if job['kind'] == 'validation']
    assert len(checks) == min(5, remaining)
    assert all(job['status'] == 'planned' and not job.get('native_reused') for job in checks)
    assert len([job for job in current['jobs'] if job['kind'] == 'optimizer']) == 1
    from pb8_loop_research import trial_records
    observations = []
    for job in checks:
        job['status'] = 'complete'
        observations.append(dict(operation=job['operation'], exact=True, per_report=[dict(gain=2)],
            assessment=dict(comparable=True, simulation_complete=True, hard_targets_met=True)))
    trials = trial_records(current, observations, criteria=[dict(metric='gain', direction='max', reducer='mean')])
    assert 'native_cpu_population' not in trials[source['operation']]
    assert all(outcome['qualified'] for outcome in trials[source['operation']]['outcomes'])
