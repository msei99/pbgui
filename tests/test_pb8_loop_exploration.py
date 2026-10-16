"""Offline regressions for broad AI search proposals and launch rejection."""
import asyncio
import copy
import json

import pytest

from pb8_loop_ai_v2 import validate_decision
from pb8_loop_controller import LoopController
from pb8_loop_exploration import evaluated_stagnation, exploration_policy, validate_exploration
from tests.test_pb8_loop_optimizer import OWNER, FakeAI, FakeBackend, config, record


def search_record():
    """Use synthetic EMA bounds independent of private user configurations."""
    source = config()
    source['bot']['short']['risk'].update(n_positions=0, total_wallet_exposure_limit=0)
    source['optimize']['scoring'] = [{'metric': 'adg_strategy_eq', 'goal': 'max'}]
    source['optimize']['limits'] = [{'metric': 'drawdown_worst_strategy_eq', 'value': 0.5}]
    source['optimize']['bounds'] = {
        'long': {'strategy': {'ema_anchor': {'ema_span_0': [20, 1440, 1],
                                            'ema_span_1': [20, 1440, 1],
                                            'entry_double_down_factor': [0, 2, 0.01]}}},
        'short': {'strategy': {'ema_anchor': {'ema_span_0': [20, 1440, 1]}}}}
    return {'initial_config': source, 'jobs': [{'kind': 'optimizer', 'round': 0,
            'operation': 'previous', 'config': copy.deepcopy(source)}]}


@pytest.mark.parametrize('edit', ['seed', 'mutation', 'limit', 'bot', 'step', 'small', 'one_bound', 'inactive'])
def test_cosmetic_or_single_bound_proposals_are_rejected(edit):
    """Neither controller seeds nor a token second edit satisfy broad exploration."""
    row = search_record()
    proposed = copy.deepcopy(row['initial_config'])
    optimize = proposed['optimize']
    bounds = optimize['bounds']['long']['strategy']['ema_anchor']
    if edit == 'seed':
        optimize['seed'] = 123
    elif edit == 'mutation':
        optimize['mutation_probability'] = 0.4
    elif edit == 'limit':
        optimize['limits'] = [{'metric': 'drawdown_worst_strategy_eq', 'value': 0.49}]
    elif edit == 'bot':
        proposed['bot']['long']['risk']['total_wallet_exposure_limit'] = 0.9
    elif edit == 'step':
        bounds['ema_span_0'][2] = bounds['ema_span_1'][2] = 5
    elif edit == 'small':
        bounds['ema_span_0'][1] = bounds['ema_span_1'][1] = 1450
    elif edit == 'one_bound':
        bounds['ema_span_0'][1] = 2880
        bounds['ema_span_1'][1] = 1450
    else:
        bounds['ema_span_0'][1] = 2880
        optimize['bounds']['short']['strategy']['ema_anchor']['ema_span_0'][1] = 2880
    before = copy.deepcopy((row, proposed))
    with pytest.raises(ValueError, match='Broad exploration requires'):
        validate_exploration(row, proposed)
    assert (row, proposed) == before


@pytest.mark.parametrize('upper', [1795, 2880])
def test_two_material_search_bounds_pass_at_threshold(upper):
    """A 25-percent endpoint shift is sufficient without requiring range doubling."""
    row = search_record()
    proposed = copy.deepcopy(row['initial_config'])
    bounds = proposed['optimize']['bounds']['long']['strategy']['ema_anchor']
    bounds['ema_span_0'][1] = bounds['ema_span_1'][1] = upper
    assert len(validate_exploration(row, proposed)) == 2


def test_actual_last_input_is_reference_and_fixed_bounds_do_not_count():
    """Compare against execution, never the incumbent, initial config or future job."""
    row = search_record()
    executed = copy.deepcopy(row['initial_config'])
    executed['optimize']['bounds']['long']['strategy']['ema_anchor']['ema_span_0'][1] = 2880
    row['jobs'][0]['executed_config'] = executed
    executed['optimize']['fixed_runtime_overrides'] = {'long.strategy.ema_anchor.ema_span_1': 800}
    policy = exploration_policy(row)
    assert policy['reference_operation'] == 'previous'
    assert policy['reference_bounds']['long.strategy.ema_anchor.ema_span_0'] == [20, 2880]
    assert 'long.strategy.ema_anchor.ema_span_1' not in policy['reference_bounds']
    with pytest.raises(ValueError, match='found 0'):
        validate_exploration(row, executed)


@pytest.mark.parametrize('fixed_path', ['long.strategy.ema_anchor.ema_span_1',
                                      'bot.long.strategy.ema_anchor.ema_span_1',
                                      'long.strategy.ema_anchor'])
def test_proposal_cannot_count_bounds_it_overrides_with_fixed_values(fixed_path):
    """Expanding an ignored bound must not disguise a miniature effective search."""
    row = search_record()
    proposed = copy.deepcopy(row['initial_config'])
    bounds = proposed['optimize']['bounds']['long']['strategy']['ema_anchor']
    bounds['ema_span_0'][1] = bounds['ema_span_1'][1] = 2880
    proposed['optimize']['fixed_runtime_overrides'] = {fixed_path: 800}
    with pytest.raises(ValueError, match='Broad exploration requires'):
        validate_exploration(row, proposed)


def test_single_available_range_requires_one_and_fixed_search_is_exempt():
    """Do not demand nonexistent free parameters from narrow user-constrained searches."""
    row = search_record()
    row['initial_config']['optimize']['bounds'] = {'long': {'x': [0, 100], 'fixed': [2, 2]}}
    row['jobs'] = []
    proposed = copy.deepcopy(row['initial_config'])
    proposed['optimize']['bounds']['long']['x'][1] = 125
    assert len(validate_exploration(row, proposed)) == 1
    row['initial_config']['optimize']['bounds']['long']['x'] = [0, 0]
    row['initial_config']['optimize'].update(scoring=[], limits=[])
    assert validate_exploration(row, proposed) == []


@pytest.mark.parametrize('remaining_rounds', [0, 1])
def test_ai_decision_gate_rejects_followup_but_allows_final_winner(record, remaining_rounds):
    """A final evaluation needs no exploration; continued search enters native repair."""
    _, row = record
    row.update(search_record())
    decision = {'winner_operation': 'winner', 'reason': 'Retain exact winner', 'finish': False,
                'variants': [{'config': row['initial_config'], 'overrides': {}}]}
    evidence = {'winner_options': {'winner': {}}, 'remaining_rounds': remaining_rounds,
                'remaining_runs': 2, 'free_slots': 1}
    if remaining_rounds:
        with pytest.raises(ValueError, match='Broad exploration'):
            validate_decision(row, 'evaluate', evidence, json.dumps(decision))
    else:
        assert validate_decision(row, 'evaluate', evidence, json.dumps(decision))['winner_operation'] == 'winner'


def test_controller_rejects_mini_edit_before_history_or_new_job(record):
    """Even an adapter bypassing AI validation cannot spend another GPU/CPU attempt."""
    store, row = record
    backend = FakeBackend()
    controller = LoopController(store.root, store=store, backend=backend, ai=FakeAI())
    source = search_record()['initial_config']
    store.update(OWNER, row['id'], lambda current: current.update(initial_config=source))

    async def exercise():
        """Complete one isolated fake round and reject its cosmetic continuation."""
        for _ in range(12):
            await controller.tick(OWNER, row['id'])
            current = store.read(OWNER, row['id'])
            if current.get('repair_error'):
                assert 'Broad exploration' in current['repair_error']
                assert current['history'] == [] and current['round'] == 0
                assert len([j for j in current['jobs'] if j['kind'] == 'optimizer']) == 1
                await controller.tick(OWNER, row['id'])
                await controller.tick(OWNER, row['id'])
                failed = store.read(OWNER, row['id'])
                assert failed['status'] == 'failed' and failed['errors'] == 3
                assert failed['history'] == [] and failed['round'] == 0
                assert len([j for j in failed['jobs'] if j['kind'] == 'optimizer']) == 1
                return
        pytest.fail('The miniature proposal was not rejected')

    asyncio.run(exercise())


def test_scoring_change_is_sufficient_without_bound_edits():
    """A different permitted objective is a complete alternative search hypothesis."""
    row = search_record()
    proposed = copy.deepcopy(row['initial_config'])
    proposed['optimize']['scoring'].append({'metric': 'drawdown_worst_strategy_eq', 'goal': 'min'})
    assert validate_exploration(row, proposed) == ['scoring']


@pytest.mark.parametrize('cosmetic', ['order', 'duplicate', 'alias', 'label', 'disabled_filter'])
def test_objective_and_filter_cosmetics_do_not_count(cosmetic):
    """Scoring/limit formatting changes cannot bypass exploration validation."""
    row = search_record()
    row['initial_config']['optimize']['scoring'].append({'metric': 'drawdown_worst_strategy_eq', 'goal': 'min'})
    row['jobs'][0]['config'] = copy.deepcopy(row['initial_config'])
    proposed = copy.deepcopy(row['initial_config'])
    scoring = proposed['optimize']['scoring']
    if cosmetic == 'order':
        scoring.reverse()
    elif cosmetic == 'duplicate':
        scoring.append(copy.deepcopy(scoring[0]))
    elif cosmetic == 'alias':
        scoring[0]['aggregate'] = 'mean'
        proposed['optimize']['limits'][0]['stat'] = 'mean'
    elif cosmetic == 'label':
        scoring[0]['label'] = 'Another label'
    else:
        proposed['optimize']['limits'].append({'metric': 'equity_choppiness', 'enabled': False, 'value': 10})
    with pytest.raises(ValueError, match='Broad exploration'):
        validate_exploration(row, proposed)


@pytest.mark.parametrize('change', ['add', 'remove', 'threshold'])
def test_material_optional_limit_changes_are_allowed(change):
    """Substantive filter changes are alternatives to changing bounds or scoring."""
    row = search_record()
    optional = {'metric': 'equity_choppiness', 'value': 10}
    if change != 'add':
        row['initial_config']['optimize']['limits'].append(optional)
        row['jobs'][0]['config'] = copy.deepcopy(row['initial_config'])
    proposed = copy.deepcopy(row['initial_config'])
    if change == 'add':
        proposed['optimize']['limits'].append(optional)
    elif change == 'remove':
        proposed['optimize']['limits'].pop()
    else:
        proposed['optimize']['limits'][-1]['value'] = 7
    assert validate_exploration(row, proposed) == ['limits']


@pytest.mark.parametrize('change', ['remove', 'disable', 'loosen'])
def test_hard_user_goal_filter_cannot_be_relaxed(record, change):
    """Changing search scoring never loosens an explicit drawdown safety target."""
    _, row = record
    row.update(search_record())
    row['rubric'] = [{'goal': 'drawdown', 'metrics': ['drawdown_worst_strategy_eq'],
                      'direction': 'min', 'target': 0.5}]
    proposed = copy.deepcopy(row['initial_config'])
    if change == 'remove':
        proposed['optimize']['limits'] = []
    elif change == 'disable':
        proposed['optimize']['limits'][0]['enabled'] = False
    else:
        proposed['optimize']['limits'][0]['value'] = 0.6
    proposed['optimize']['scoring'].append({'metric': 'equity_choppiness', 'goal': 'min'})
    with pytest.raises(ValueError, match='hard user-goal filter'):
        validate_exploration(row, proposed)


@pytest.mark.parametrize('noise', [False, True])
def test_stagnation_cannot_return_to_a_previous_objective_set(noise):
    """After two failures A-to-B-to-A, even with tiny bound noise, is not new."""
    row = search_record()
    alternative = copy.deepcopy(row['initial_config'])
    alternative['optimize']['scoring'] = [{'metric': 'equity_choppiness', 'goal': 'min'}]
    row['jobs'].append({'kind': 'optimizer', 'round': 1, 'operation': 'second', 'config': alternative})
    proposed = copy.deepcopy(row['initial_config'])
    if noise:
        proposed['optimize']['bounds']['long']['strategy']['ema_anchor']['ema_span_0'][1] += 1
    assert validate_exploration(row, proposed, stagnation_after=1) == ['scoring']
    with pytest.raises(ValueError, match='untested'):
        validate_exploration(row, proposed, stagnation_after=2)
    proposed['optimize']['scoring'].append({'metric': 'drawdown_worst_strategy_eq', 'goal': 'min'})
    assert validate_exploration(row, proposed, stagnation_after=2) == ['scoring']


def test_stagnation_boundary_includes_current_evaluation_once():
    """The second unchanged incumbent triggers novelty, but a new winner resets it."""
    row = {'round': 1, 'stagnation': 1, 'best': {'operation': 'incumbent'}, 'history': []}
    assert evaluated_stagnation(row, 'incumbent') == 2
    assert evaluated_stagnation(row, 'new') == 0
    row['history'] = [{'round': 1}]
    assert evaluated_stagnation(row, 'incumbent') == 1


def test_experiment_summary_uses_candidate_evidence_not_retained_winner():
    """Expose training results only, without misrepresenting a retained best as a new result."""
    row = search_record()
    row['history'] = [{'round': 0, 'metrics': {'gain_strategy_eq': 999}, 'observations': [
        {'exact': True, 'metrics': {'gain_strategy_eq': 2.1}}]}]
    row['observer_jobs'] = [{'metrics': {'gain_strategy_eq': 12345}}]
    policy = exploration_policy(row)
    assert policy['recent_experiments'][0]['exact_candidate_gains'] == [2.1]
    assert '12345' not in json.dumps(policy) and '999' not in json.dumps(policy)


def test_twenty_percent_decimal_threshold_change_counts():
    """Floating-point representation must not reject an exact 20-percent change."""
    row = search_record()
    proposed = copy.deepcopy(row['initial_config'])
    proposed['optimize']['limits'][0]['value'] = 0.4
    assert validate_exploration(row, proposed) == ['limits']


@pytest.mark.parametrize('edit', ['tiny', 'duplicate', 'alias', 'material'])
def test_interval_filters_require_effective_material_change(edit):
    """Range aliases/duplicates and tiny interval shifts cannot count as new filters."""
    row = search_record()
    row['initial_config']['optimize']['limits'].append({
        'metric': 'equity_choppiness', 'penalize_if': 'outside_range', 'range': [2, 10]})
    row['jobs'][0]['config'] = copy.deepcopy(row['initial_config'])
    proposed = copy.deepcopy(row['initial_config'])
    entry = proposed['optimize']['limits'][-1]
    if edit == 'tiny':
        entry['range'][1] = 10.01
    elif edit == 'duplicate':
        proposed['optimize']['limits'].append(copy.deepcopy(entry))
    elif edit == 'alias':
        entry['stat'] = 'mean'
    else:
        entry['range'][1] = 12
    if edit == 'material':
        assert validate_exploration(row, proposed) == ['limits']
    else:
        with pytest.raises(ValueError, match='Broad exploration'):
            validate_exploration(row, proposed)


@pytest.mark.parametrize('floor', [0.99, 0.98])
def test_gpu_completion_floor_remains_compatible_with_exact_target(record, floor):
    """Respect the existing authorized GPU precision floor while rejecting further relaxation."""
    _, row = record
    row.update(search_record())
    row['settings']['execution'] = 'vast'
    row['initial_config']['optimize']['limits'].append({
        'metric': 'backtest_completion_ratio', 'penalize_if': 'less_than', 'value': 1.0})
    row['jobs'][0]['config'] = copy.deepcopy(row['initial_config'])
    row['rubric'] = [{'goal': 'custom', 'metrics': ['backtest_completion_ratio'],
                      'direction': 'max', 'target': 1.0}]
    proposed = copy.deepcopy(row['initial_config'])
    proposed['optimize']['limits'][-1]['value'] = floor
    proposed['optimize']['scoring'].append({'metric': 'drawdown_worst_strategy_eq', 'goal': 'min'})
    if floor == 0.99:
        assert validate_exploration(row, proposed) == ['scoring']
    else:
        with pytest.raises(ValueError, match='hard user-goal'):
            validate_exploration(row, proposed)


@pytest.mark.parametrize('reason', [None, '', 'Explore a gain/drawdown Pareto tradeoff'])
def test_material_ai_proposal_requires_experiment_reason(record, reason):
    """A changed objective must come with a concrete per-variant hypothesis."""
    _, row = record
    row.update(search_record())
    proposed = copy.deepcopy(row['initial_config'])
    proposed['optimize']['scoring'].append({'metric': 'drawdown_worst_strategy_eq', 'goal': 'min'})
    decision = {'winner_operation': 'winner', 'reason': 'Retain the exact incumbent', 'finish': False,
                'variants': [{'config': proposed, 'overrides': {}, 'reason': reason}]}
    evidence = {'winner_options': {'winner': {}}, 'remaining_rounds': 1, 'remaining_runs': 2, 'free_slots': 1}
    if reason:
        assert validate_decision(row, 'evaluate', evidence, json.dumps(decision))['variants']
    else:
        with pytest.raises(ValueError, match='hypothesis'):
            validate_decision(row, 'evaluate', evidence, json.dumps(decision))
