"""Template searches expose the full scoring catalog without forcing per-round choices."""
import copy

import pytest

from ai_loop_objectives import scoring_objectives, validate_objective, validate_template_rubric
from ai_loop_request import bind_request
from pb8_loop_exploration import exploration_policy, validate_exploration
from tests.test_ai_loop_objectives import reviewed_run, RUBRIC
from tests.test_ai_loop_windows import request
from vast_config_validation import METRICS, REDUCERS


def search_run(tmp_path, scores=None):
    """Represent completed synthetic searches only in temporary storage."""
    _, row, _ = reviewed_run(tmp_path)
    row['rubric'] = copy.deepcopy(RUBRIC)
    row['initial_config']['optimize']['limits'] = [
        {'metric': 'drawdown_worst_strategy_eq', 'penalize_if': 'greater_than', 'reducer': 'max', 'value': .5}]
    row['initial_config']['optimize']['bounds'] = {'long': {'risk': {'first': [0, 10], 'second': [0, 10]}}}
    row['jobs'] = []
    for number, score in enumerate(scores or [[{'metric': 'adg_strategy_eq', 'goal': 'max', 'reducer': 'mean'}]]):
        config = copy.deepcopy(row['initial_config'])
        config['optimize']['scoring'] = score
        row['jobs'].append({'kind': 'optimizer', 'operation': str(number + 1) * 32,
                            'round': number, 'config': config, 'started': True,
                            'status': 'cancelled', 'results_consumed': True, 'result_count': 1})
    return row


def test_cpu_gain_is_not_a_fixed_gpu_scoring_formula(tmp_path):
    """Gain stays the exact winner criterion while many supported proxies can guide search."""
    objective = {'metric': 'gain_strategy_eq', 'goal': 'max', 'reducer': 'mean'}
    assert validate_objective(objective) == objective
    _, row, _ = reviewed_run(tmp_path)
    original = copy.deepcopy(row['initial_config'])
    for metric in METRICS:
        for reducer in REDUCERS:
            candidate = copy.deepcopy(original)
            candidate['optimize']['scoring'] = [{'metric': metric, 'goal': 'max', 'reducer': reducer}]
            assert scoring_objectives(candidate)
            validate_template_rubric(row, candidate, RUBRIC)
    assert row['initial_config'] == original
    validate_template_rubric(row, original, RUBRIC)


@pytest.mark.parametrize('experiment', ['bounds', 'seed', 'repeat', 'combination'])
def test_ai_can_choose_controlled_experiments_without_forced_switches(tmp_path, experiment):
    """Controlled comparisons and repeated seeds remain possible after stagnation."""
    row = search_run(tmp_path)
    candidate = copy.deepcopy(row['jobs'][-1]['config'])
    if experiment == 'bounds':
        candidate['optimize']['bounds']['long']['risk']['first'][1] = 10.5
    elif experiment == 'seed':
        candidate['optimize']['seed'] = 42
    elif experiment == 'combination':
        candidate['optimize']['scoring'].append({'metric': 'drawdown_worst_strategy_eq', 'goal': 'min', 'reducer': 'max'})
    validate_exploration(row, candidate, stagnation_after=5)
    validate_template_rubric(row, candidate, RUBRIC)


def test_freedom_does_not_remove_hard_user_filters(tmp_path):
    """Adaptive experiments cannot loosen the user drawdown ceiling."""
    row = search_run(tmp_path)
    candidate = copy.deepcopy(row['initial_config'])
    candidate['optimize']['limits'][0]['value'] = .6
    with pytest.raises(ValueError, match='hard user-goal filter'):
        validate_exploration(row, candidate)


def test_catalog_is_complete_and_unstarted_attempt_is_not_tested(tmp_path):
    """Coverage is based on actual consumed optimizer output, not proposed configurations."""
    row = search_run(tmp_path)
    pending = copy.deepcopy(row['jobs'][0])
    pending.update(round=0, operation='f' * 32, started=False, results_consumed=False, result_count=0)
    row['history'] = [{'round': 0, 'observations': [{'exact': True, 'metrics': {'gain_strategy_eq': 2}}]}]
    pending['config']['optimize']['scoring'].append({'metric': 'drawdown_worst_strategy_eq', 'goal': 'min', 'reducer': 'max'})
    row['jobs'].append(pending)
    coverage = exploration_policy(row)['scoring_exploration']
    assert set(coverage['catalog']['metrics']) == set(METRICS)
    assert set(coverage['catalog']['reducers']) == set(REDUCERS)
    assert coverage['tested_metrics'] == ['adg_strategy_eq']
    assert 'drawdown_worst_strategy_eq' in coverage['untested_metrics']
    assert len(coverage['tested_scoring_signatures']) == 1


def test_coverage_informs_without_prescribing_next_experiment(tmp_path):
    """Catalog coverage is advice, not a per-round metric or filter quota."""
    row = search_run(tmp_path, [[{'metric': 'adg_strategy_eq', 'goal': 'max', 'reducer': reducer}]
                               for reducer in ['mean', 'min']])
    policy = exploration_policy(row)
    assert policy['mode'] == 'adaptive'
    coverage = policy['scoring_exploration']
    assert not coverage['required']
    assert not coverage['require_untested_metric']
    assert not coverage['require_new_optional_limits']
