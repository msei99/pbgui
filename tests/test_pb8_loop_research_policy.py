"""Permanent research invariants: fixed work, no repeats, explicit user freedoms only."""
import copy
import json

import pytest

from pb8_loop_ai_v2 import decision_instructions, validate_decision
from pb8_loop_research import budget, experiment_plan, experiment_signature, summarize, trial_records
from pb8_loop_store import validate_change
from tests.test_pb8_loop_research import research, optimizer, changed_config


@pytest.mark.parametrize('purpose', ['replicate', 'confirm'])
@pytest.mark.parametrize('fraction', [.1, .5, 1, 2])
def test_no_purpose_or_work_fraction_can_authorize_repetition(research, purpose, fraction):
    """Neither a smaller nor a longer repeat can override the user's fixed work."""
    _, row, controller = research
    source = optimizer(row, controller, 'original')
    with pytest.raises(ValueError, match='forbidden'):
        experiment_plan(row, source['config'], {}, dict(purpose=purpose, source_operation='original', budget_fraction=fraction))


@pytest.mark.parametrize('mode,setting,value', [('proxy', 'run_proxy', 100000), ('iters', 'run_iters', 8192), ('hours', 'run_hours', 2)])
def test_user_budget_is_fixed_for_every_run_mode(research, mode, setting, value):
    """Explicit run settings cannot be fractionally allocated by any model."""
    _, row, _ = research
    row['settings'].update(run_limit_mode=mode)
    row['settings'][setting] = value
    assert budget(row) == dict(mode=mode, value=value, fraction=1)
    for fraction in (.1, .99, 1.01, 2):
        with pytest.raises(ValueError, match='fixed'):
            budget(row, fraction)


def test_config_iteration_budget_cannot_be_changed_by_ai(research):
    """Config-mode iteration limits are user-owned even though old prompts allowed edits."""
    _, row, _ = research
    row['settings']['run_limit_mode'] = 'config'
    config = copy.deepcopy(row['initial_config'])
    assert budget(row)['value'] == config['optimize']['iters']
    config['optimize']['iters'] *= 2
    with pytest.raises(ValueError, match='budget is fixed'):
        experiment_plan(row, config, {})


@pytest.mark.parametrize('edit', ['none', 'seed', 'gpu', 'workers', 'recorder', 'reference_bot', 'step', 'inactive_short', 'numeric_spelling'])
def test_cosmetic_and_inactive_edits_do_not_make_explore_novel(research, edit):
    """Changing execution details or inactive bounds cannot disguise an unchanged search."""
    _, row, controller = research
    source = optimizer(row, controller, 'original')
    config = copy.deepcopy(source['config'])
    if edit == 'seed': config['optimize']['seed'] = 123
    if edit == 'gpu': config['optimize'].setdefault('gpu', {})['population_size'] = 1234
    if edit == 'workers': config['optimize']['n_cpus'] = 123
    if edit == 'recorder': config['optimize']['pareto_max_size'] = 123
    if edit == 'reference_bot':
        # A value under a genuine bound is a reference, not a reused population.
        config['bot']['long']['risk']['total_wallet_exposure_limit'] += .1
    if edit == 'step': config['optimize']['bounds']['long_total_wallet_exposure_limit'].append(.001)
    if edit == 'inactive_short':
        for value in (config, source['config'], source['executed_config']):
            value['bot']['short']['risk']['n_positions'] = 0
            value['optimize']['bounds']['short_n_positions'] = [0, 0]
        config['optimize']['bounds']['short_total_wallet_exposure_limit'] = [.1, 99]
    if edit == 'numeric_spelling':
        old = config['optimize']['bounds']['long_total_wallet_exposure_limit']
        config['optimize']['bounds']['long_total_wallet_exposure_limit'] = [float(v) for v in old]
    with pytest.raises(ValueError, match='Unchanged optimizer settings'):
        experiment_plan(row, config, {}, dict(purpose='explore', budget_fraction=1))


def test_earlier_or_parallel_same_settings_are_not_new_experiments(research):
    """Novelty compares all previous/planned jobs, not just the most recent round."""
    _, row, controller = research
    source = optimizer(row, controller, 'original')
    alternative = controller.job(row, 'optimizer', changed_config(source['config']), {}, 'Different scoring',
                                 experiment=dict(purpose='explore', budget_fraction=1))
    row['jobs'].append(alternative)
    for candidate in (source['config'], alternative['config']):
        with pytest.raises(ValueError, match='Unchanged optimizer settings'):
            controller.job(row, 'optimizer', candidate, {}, 'Relabel same input as Explore', experiment=dict(purpose='explore'))


@pytest.mark.parametrize('provider', ['chatgpt', 'claude-code', 'opencode-go', 'opencode-zen'])
def test_model_decision_validator_rejects_same_config_even_under_explore(research, provider):
    """The policy is enforced before a provider reply can authorize a job."""
    _, row, controller = research
    source = optimizer(row, controller, 'original')
    row['settings']['provider'] = provider
    config = copy.deepcopy(source['config'])
    config['optimize']['seed'] = None
    reply = dict(variants=[dict(config=config, overrides={}, reason='Repeat a promising trial',
                               experiment=dict(purpose='explore', budget_fraction=1))])
    with pytest.raises(ValueError, match='Unchanged optimizer settings'):
        validate_decision(row, 'bootstrap', dict(max_variants=1), json.dumps(reply))


@pytest.mark.parametrize('kind', ['bounds', 'limits', 'scoring'])
def test_regular_search_controls_remain_open_at_exact_user_work(research, kind):
    """The ban prevents repeated settings, not purposeful optimizer research."""
    _, row, controller = research
    source = optimizer(row, controller, 'original')
    config = copy.deepcopy(source['config'])
    if kind == 'bounds': config['optimize']['bounds']['long_total_wallet_exposure_limit'][1] += 1
    if kind == 'limits': config['optimize']['limits'].append(dict(metric='drawdown_worst_strategy_eq', value=.4, penalize_if='greater_than'))
    if kind == 'scoring': config = changed_config(config)
    plan = experiment_plan(row, config, {}, dict(purpose='explore'))
    assert plan['budget'] == dict(mode='proxy', value=100000, fraction=1)


@pytest.mark.parametrize('opened', [True, False])
def test_strategy_exception_requires_explicit_user_permission(research, opened):
    """An open strategy is a genuine user-authorized experiment; a closed one stays fixed."""
    _, row, controller = research
    source = optimizer(row, controller, 'original')
    row['settings']['strategy_enabled'] = opened
    config = copy.deepcopy(source['config'])
    config['live']['strategy_kind'] = 'trailing_martingale'
    if opened:
        plan = experiment_plan(row, config, {}, dict(purpose='explore'))
        assert plan['budget']['value'] == 100000
        validate_change(row, config, {})
    else:
        with pytest.raises(ValueError, match='Unchanged optimizer settings'):
            experiment_plan(row, config, {})
        with pytest.raises(ValueError, match='Strategy changes are disabled'):
            validate_change(row, config, {})


def test_protocol_overrides_historical_instructions_allowing_repeats(research):
    """All providers receive the fixed-budget policy despite obsolete stored advice."""
    _, row, _ = research
    text = decision_instructions(row)
    assert 'Replicate and confirm are FORBIDDEN' in text
    assert 'budget_fraction:1' in text
    assert 'Historical advice about repetition' in text


def test_retained_nomination_coverage_is_not_missing_metric_coverage(research):
    """Five fully measured nominated outcomes are 100% measured, not 2% measured."""
    from tests.test_pb8_loop_research import observation, assessment
    _, row, controller = research
    source = optimizer(row, controller, 'original', fraction=1)
    outcomes = [observation(row, source['operation'], 'cpu' + str(i), i + 1) for i in range(5)]
    trials = trial_records(row, outcomes, assessment()['criteria'])
    trials[source['operation']]['native_cpu_population'] = dict(offered=248, verified=248, qualified=248)
    group = summarize(trials, assessment()['criteria'])[0]
    assert group['measurement_coverage'] == 1
    assert group['selection_coverage'] == pytest.approx(5 / 248)
    assert group['measurement_coverage_basis'] == 'retained qualified outcomes'
    assert 'partial metric coverage' not in group['uncertainty']


def test_open_scenario_allows_real_window_change_but_not_template_rename(research):
    """An explicit exception admits changed simulation conditions, not display metadata."""
    _, row, controller = research
    source = optimizer(row, controller, 'original')
    row['settings']['scenario_enabled'] = True
    config = copy.deepcopy(source['config'])
    config.setdefault('pbgui', {})['scenario_template'] = {'name': 'another display label'}
    with pytest.raises(ValueError, match='Unchanged optimizer settings'):
        experiment_plan(row, config, {})
    config['backtest']['start_date'] = '2020-01-02'
    assert experiment_plan(row, config, {})['budget']['value'] == 100000


def test_hsl_auto_allows_activation_but_not_false_default_spelling(research):
    """The user's Auto choice admits a real HSL change, never a cosmetic default."""
    _, row, controller = research
    source = optimizer(row, controller, 'original')
    row['settings']['hsl_mode'] = 'auto'
    config = copy.deepcopy(source['config'])
    config['bot']['long'].setdefault('hsl', {})['enabled'] = False
    with pytest.raises(ValueError, match='Unchanged optimizer settings'):
        experiment_plan(row, config, {})
    config['bot']['long']['hsl']['enabled'] = True
    assert experiment_plan(row, config, {})['budget']['value'] == 100000


def test_bounded_ai_group_query_keeps_selection_and_measurement_coverage_distinct(research):
    """The bounded view must not drop the coverage basis before the AI sees it."""
    from pb8_loop_evaluation_evidence import query
    group = dict(id='g', measurement_coverage=1, measurement_coverage_basis='retained qualified outcomes',
                 retained_qualified_count=5, selection_coverage=5 / 248, qualified_count=248,
                 per_trial_best=[dict(operation='optimizer')])
    snapshot = dict(id='a' * 32, sha256='b' * 64, data=dict(stage='evaluate', evidence=dict(optimizer_research=dict(groups=[group]))))
    page = query(snapshot, '/evaluation/research', 0, 1500)
    assert page['entries'][0]['measurement_coverage'] == 1
    assert page['entries'][0]['selection_coverage'] == 5 / 248
    assert page['entries'][0]['measurement_coverage_basis'] == 'retained qualified outcomes'
