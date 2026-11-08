"""Isolated regressions for successful references and coherent optimizer experiments."""
import copy
import json

import pytest

from pb8_loop_bounds import diagnose, validate_response
from pb8_loop_planning import planning_context, validate_plan
from pb8_loop_research import patch_contract, source_inputs
from tests.test_pb8_loop_bounds import dataset


def programme(values=(10, 8, 7), expanded=True):
    """Model a qualified winner followed by measured, worse optimizer descendants."""
    record, options, config = dataset(len(values))
    record.update(id='a' * 32, optimizer_research_version=1, phase='optimize', round=len(values) - 1,
                  initial_config=copy.deepcopy(config), initial_overrides={}, optimizer_trials={})
    record['settings'].update(run_limit_mode='proxy', run_proxy=100000)
    criteria = [{'metric': 'gain', 'direction': 'max', 'reducer': 'mean', 'reason': 'User growth goal'}]
    record['optimizer_assessment'] = {'criteria': criteria}
    for index, value in enumerate(values):
        source, validation = record['jobs'][2 * index:2 * index + 2]
        if expanded:
            source['config']['optimize']['bounds']['long']['strategy']['ema_anchor']['base_qty_pct'][1] += .02 * index
        source.update(status='cancelled', overrides={}, research_verification={'status': 'verified'},
                      limit_reached='proxy', proxy_evaluations=100000)
        source['research'].update(purpose='explore', budget={'mode': 'proxy', 'value': 100000, 'fraction': 1})
        options[validation['operation']]['per_report'] = [{'gain': value}]
        record['optimizer_trials'][source['operation']] = {
            'operation': source['operation'], 'round': index, 'signature': str(index),
            'budget': copy.deepcopy(source['research']['budget']), 'execution_verified': True,
            'status': 'cancelled', 'limit_reached': 'proxy', 'completed_proxies': 100000,
            'outcomes': [{'operation': validation['operation'], 'qualified': True,
                          'per_report': [{'gain': value}]}]}
    return record, options, config


def test_reference_is_measured_winner_not_latest_losing_descendant():
    """The compact source and bounds diagnosis use the same successful reference."""
    record, options, _ = programme()
    before = copy.deepcopy(record)
    context = planning_context(record)
    assert context['reference_operation'] == 'o0' and context['latest_operation'] == 'o2'
    assert context['trials_without_improvement'] == 2
    assert context['scoring_change_required']
    refs, _ = source_inputs(record)
    assert patch_contract(record, refs)['preferred_variant']['base_optimizer'] == 'o0'
    diagnosis = diagnose(record, options)
    assert diagnosis['reference_operation'] == 'o0'
    assert diagnosis['parameters'][0]['bounds'] == [.001, .1]
    assert diagnosis['parameters'][0]['outward_test_count'] == 2
    assert not diagnosis['parameters'][0]['batch_required']
    assert record == before


@pytest.mark.parametrize('problem', ['unverified', 'under_budget', 'unsafe', 'missing', 'different_budget'])
def test_invalid_or_incomparable_trial_cannot_become_reference(problem):
    """Unverified, incomplete, unsafe and unavailable evidence never outranks measured settings."""
    record, _, _ = programme((10, 100))
    trial = record['optimizer_trials']['o1']
    if problem == 'unverified':
        trial['execution_verified'] = False
    elif problem == 'under_budget':
        trial['completed_proxies'] = 90000
    elif problem == 'unsafe':
        trial['outcomes'][0]['qualified'] = False
    elif problem == 'missing':
        trial['outcomes'][0]['per_report'] = [{'gain': None}]
    else:
        trial['budget']['value'] = 50000
    assert planning_context(record)['reference_operation'] == 'o0'


def test_stagnation_requires_scoring_change_even_when_bounds_change():
    """Bounds-only novelty cannot postpone a scoring experiment indefinitely."""
    record, _, config = programme(expanded=False)
    context = planning_context(record)
    variant = {'config': copy.deepcopy(config)}
    variant['config']['optimize']['bounds']['long']['strategy']['ema_anchor']['base_qty_pct'][1] = .13
    with pytest.raises(ValueError, match='different effective scoring'):
        validate_plan(record, variant, context)
    variant['config']['optimize']['scoring'] = [{'metric': 'sortino_ratio_strategy_eq', 'goal': 'max'}]
    validate_plan(record, variant, context)


def test_losing_expansion_cannot_be_extended_but_rebasing_remains_possible():
    """Discard inherited losing bounds while retaining permission for a changed experiment."""
    record, _, config = programme((10, 8))
    variant = {'config': copy.deepcopy(record['jobs'][2]['config'])}
    variant['config']['optimize']['bounds']['long']['strategy']['ema_anchor']['base_qty_pct'][1] = .15
    context = planning_context(record)
    with pytest.raises(ValueError, match='underperforming optimizer branch'):
        validate_plan(record, variant, context)
    variant['config'] = copy.deepcopy(config)
    variant['config']['optimize']['bounds']['long']['strategy']['ema_anchor']['offset'] = [0, .05]
    validate_plan(record, variant, context)


def test_all_untested_pressured_bounds_are_adjusted_in_one_batch():
    """One adjusted endpoint plus another deferred endpoint is not a coherent bounds batch."""
    record, options, config = programme(expanded=False)
    for job in record['jobs']:
        target = job['config'] if job['kind'] == 'optimizer' else job['candidate']['config']
        target['optimize']['bounds']['long']['strategy']['ema_anchor']['offset'] = [.001, .1]
        target['bot']['long']['strategy']['ema_anchor']['offset'] = .099
    diagnosis = diagnose(record, options)
    assert len([r for r in diagnosis['parameters'] if r['batch_required']]) == 2
    variant = {'config': copy.deepcopy(record['jobs'][0]['config']), 'experiment': {'purpose': 'explore'},
               'boundary_response': [{'id': r['id'], 'action': 'adjust' if r['parameter'].endswith('base_qty_pct') else 'retain',
                                      'reason': 'Test the active pressure coherently'} for r in diagnosis['parameters']]}
    variant['config']['optimize']['bounds']['long']['strategy']['ema_anchor']['base_qty_pct'][1] = .13
    with pytest.raises(ValueError, match='all relevant untested boundary'):
        validate_response(record, diagnosis, variant)
    variant['config']['optimize']['bounds']['long']['strategy']['ema_anchor']['offset'][1] = .13
    for response in variant['boundary_response']:
        response['action'] = 'adjust'
    validate_response(record, diagnosis, variant)


def test_feedback_stays_bounded_without_exposing_seeds_or_user_reports():
    """Large histories retain global ranking but only four recent trial summaries."""
    record, _, _ = programme(tuple(range(1000, 0, -1)), expanded=False)
    context = planning_context(record)
    assert context['reference_operation'] == 'o0'
    assert len(context['recent_trials']) == 4
    assert len(json.dumps(context)) < 4000
    assert all('seed' not in item for item in context['recent_trials'])


def test_objective_reordering_cannot_satisfy_scoring_change():
    """Reordering simultaneous Pareto objectives is not a new search hypothesis."""
    record, _, config = programme(expanded=False)
    objectives = [{'metric': 'adg_strategy_eq', 'goal': 'max'},
                  {'metric': 'drawdown_worst_strategy_eq', 'goal': 'min'}]
    for job in record['jobs']:
        if job['kind'] == 'optimizer':
            job['config']['optimize']['scoring'] = copy.deepcopy(objectives)
    variant = {'config': copy.deepcopy(record['jobs'][0]['config'])}
    variant['config']['optimize']['scoring'].reverse()
    with pytest.raises(ValueError, match='different effective scoring'):
        validate_plan(record, variant, planning_context(record))


def test_protected_user_ceiling_is_excluded_from_bounds_batch():
    """A simultaneous bounds experiment must not widen the user's exposure ceiling."""
    record, options, _ = programme(expanded=False)
    for job in record['jobs']:
        target = job['config'] if job['kind'] == 'optimizer' else job['candidate']['config']
        target['bot']['long']['risk']['total_wallet_exposure_limit'] = 7.99
    diagnosis = diagnose(record, options)
    protected = next(r for r in diagnosis['parameters'] if r['protected'])
    assert protected['parameter'].endswith('total_wallet_exposure_limit')
    assert not protected['batch_required']


def test_other_budget_outward_trials_do_not_resolve_current_budget_pressure():
    """An incomparable historical short run must not excuse another untested endpoint."""
    record, options, _ = programme((10, 8))
    record['optimizer_trials']['o1']['budget']['value'] = 50000
    diagnosis = diagnose(record, options)
    signal = diagnosis['parameters'][0]
    assert signal['outward_test_count'] == 0 and signal['batch_required']


def test_positive_quantized_lower_edges_have_a_feasible_batch_adjustment():
    """Broad positive EMA intervals must not force negative spans to meet a 10% gate."""
    record, options, _ = programme(expanded=False)
    for job in record['jobs']:
        target = job['config'] if job['kind'] == 'optimizer' else job['candidate']['config']
        target['optimize']['bounds']['long']['strategy']['ema_anchor']['ema_span_0'] = [20, 1800, 1]
        target['bot']['long']['strategy']['ema_anchor']['ema_span_0'] = 20
    diagnosis = diagnose(record, options)
    signal = next(r for r in diagnosis['parameters'] if r['parameter'].endswith('ema_span_0'))
    assert signal['edge'] == 'lower' and signal['minimum_adjustment'] == 19
    assert signal['suggested_endpoint'] == 1 and signal['batch_required']
    variant = {'config': copy.deepcopy(record['jobs'][0]['config']), 'experiment': {'purpose': 'explore'},
               'boundary_response': [{'id': r['id'], 'action': 'adjust', 'reason': 'Test every open endpoint'}
                                     for r in diagnosis['parameters']]}
    variant['config']['optimize']['bounds']['long']['strategy']['ema_anchor']['ema_span_0'][0] = 1
    variant['config']['optimize']['bounds']['long']['strategy']['ema_anchor']['base_qty_pct'][1] = .13
    validate_response(record, diagnosis, variant)


def test_lower_edge_already_at_positive_step_floor_can_be_retained():
    """An EMA already at its positive grid floor cannot force an impossible bounds batch."""
    record, options, _ = programme(expanded=False)
    for job in record['jobs']:
        target = job['config'] if job['kind'] == 'optimizer' else job['candidate']['config']
        target['optimize']['bounds']['long']['strategy']['ema_anchor']['ema_span_0'] = [1, 1800, 1]
        target['bot']['long']['strategy']['ema_anchor']['ema_span_0'] = 1
    signal = next(r for r in diagnose(record, options)['parameters'] if r['parameter'].endswith('ema_span_0'))
    assert signal['positive_step_floor'] and not signal['batch_required']
