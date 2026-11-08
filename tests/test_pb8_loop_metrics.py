"""Provider-independent training metric bases never mix means and extrema."""
import copy

import pytest

from pb8_loop_candidates import metric_value
from pb8_loop_evidence import required_content
from pb8_loop_metrics import annotate_observations, native_metric_evidence, report_metric_evidence


DD = 'drawdown_worst_strategy_eq'
ADG = 'adg_strategy_eq'
ROW = {'rubric': [{'metrics': [DD], 'direction': 'min', 'target': .5},
                  {'metrics': [ADG], 'direction': 'max', 'target': None}]}


def native(blocks, labels=()):
    """Create isolated native evidence with a deliberately misleading aggregate."""
    return native_metric_evidence(ROW, {}, blocks, list(labels),
                                  {DD: .33, ADG: .01}, {'backtest': {'suite_enabled': True}})


def test_native_and_cpu_use_identical_risk_basis_and_keep_mean():
    """The same four windows expose worst drawdown and mean return on both paths."""
    reports = [{DD: value, ADG: index / 1000} for index, value in enumerate((.2, .3, .37, .475), 1)]
    cpu = report_metric_evidence(ROW, reports)
    blocks = {key: {'stats': stats, 'aggregated': -99} for key, stats in cpu['metric_statistics'].items()}
    gpu = native(blocks, ['a', 'b', 'c', 'd'])
    assert gpu['metrics'] == cpu['metrics'] == {DD: .475, ADG: .0025}
    assert gpu['metric_statistics'][DD]['mean'] == .33625
    assert gpu['metric_basis']['reducers'] == {DD: 'max', ADG: 'mean'}
    assert gpu['metric_basis']['window_count'] == 4
    assert cpu['metric_basis']['source'] == 'independent_cpu'


@pytest.mark.parametrize('value', [None, float('nan'), float('inf'), False])
def test_missing_native_extremum_never_uses_mean_or_zero(value):
    """Missing, boolean and nonfinite bounds stay unavailable despite a valid mean."""
    gpu = native({DD: {'stats': {'mean': .33, 'max': value}, 'aggregated': .33}})
    assert DD not in gpu['metrics']
    assert gpu['metric_statistics'][DD] == {'mean': .33}
    assert gpu['metric_basis']['unavailable'][DD] == 'max'


def test_scenario_values_reconstruct_extrema_only_when_complete():
    """A missing training window cannot manufacture a favorable worst-case value."""
    block = {DD: {'scenarios': {'a': .2, 'b': .6}, 'aggregated': .4}}
    assert native(block, ['a', 'b'])['metrics'][DD] == .6
    incomplete = native(block, ['a', 'b', 'c'])
    assert DD not in incomplete['metrics']
    assert incomplete['metric_basis']['unavailable'][DD] == 'max'


def test_configured_aggregate_is_not_assumed_to_be_mean_or_max():
    """Native reducer settings cannot change the meaning of the AI comparison."""
    gpu = native({DD: {'aggregated': .2}, ADG: {'aggregated': .9}})
    assert gpu['metrics'] == {}
    assert gpu['metric_basis']['unavailable'] == {DD: 'max', ADG: 'mean'}


def test_single_window_values_are_also_explicit():
    """Legacy single-window artifacts have equal mean and extrema, including zero."""
    gpu = native_metric_evidence(ROW, {}, {}, [], {DD: 0, ADG: .01}, {})
    assert gpu['metric_statistics'][DD] == {'mean': 0, 'min': 0, 'max': 0}
    assert gpu['metrics'] == {DD: 0, ADG: .01}


@pytest.mark.parametrize('alternate', [False, True])
def test_native_stream_nominations_use_only_requested_known_reducer(alternate):
    """Both native suite formats enforce worst-window screening before nomination."""
    stats = {DD: {'mean': .33, 'max': .6}}
    suite = {'aggregate': {'stats': stats}} if alternate else {'metrics': {DD: {'stats': stats[DD]}}}
    assert metric_value({'suite_metrics': suite}, DD, 'max', {DD: .33}) == .6
    del stats[DD]['max']
    assert metric_value({'suite_metrics': suite}, DD, 'max', {DD: .33}) is None


def test_cpu_evidence_keeps_original_arithmetic_and_missingness():
    """No change to CPU winners, floating point rounding, or incomplete metrics."""
    reports = [{DD: .2, ADG: .1, 'extra': 0}, {DD: .6, ADG: .2}, {DD: .3, ADG: .3}]
    package = report_metric_evidence(ROW, reports)
    assert package['metrics'] == {DD: .6, ADG: sum(report[ADG] for report in reports) / len(reports)}
    assert 'extra' not in package['metrics']


def test_historical_cpu_labels_require_retained_reports_and_matching_values():
    """Annotate future evidence without altering historical metrics or stored history."""
    observation = {'metrics': {DD: .6, ADG: 999}, 'per_report': [{DD: .2, ADG: .1}, {DD: .6, ADG: .3}],
                   'candidate': {'metrics': {DD: .4}}}
    source = {'best': observation, 'observations': [copy.deepcopy(observation)],
              'winner_options': {'old': {'metrics': {DD: .33}}}}
    before = copy.deepcopy(source)
    result = annotate_observations(ROW, source)
    assert source == before
    assert result['best']['metrics'] == observation['metrics']
    assert result['best']['metric_basis']['reducers'] == {DD: 'max'}
    assert result['best']['metric_statistics'][DD]['mean'] == .4
    assert result['best']['candidate']['metric_basis']['source'] == 'legacy_unlabelled'
    assert result['winner_options']['old']['metric_basis']['source'] == 'legacy_unlabelled'


def test_mandatory_evidence_preserves_labels_and_distinct_distributions():
    """Compaction cannot merge candidates with equal maxima and different means."""
    first = native({DD: {'stats': {'mean': .33, 'max': .47}}, 'optional': {'stats': {'mean': 99}}})
    second = native({DD: {'stats': {'mean': .4, 'max': .47}}})
    source = {'stage': 'select', 'initial_config': {}, 'rubric': ROW['rubric'],
              'evidence': {'candidates': [{'id': 'a', **first}, {'id': 'b', **second}]}}
    compact = required_content(source)['evidence']
    from pb8_loop_selection import query
    assert compact['selection']['candidate_count'] == 2
    frozen = {'id': 'a' * 32, 'sha256': 'b' * 64, 'data': source}
    candidates = query(frozen, '/selection/rows', 0, 5000)['entries']
    assert len(candidates) == 2
    assert candidates[0]['metric_statistics'][DD] == {'mean': .33, 'max': .47}
    assert 'optional' not in candidates[0]['metric_statistics']
    assert candidates[0]['metric_basis']['reducers'][DD] == 'max'
    assert first['metric_statistics']['optional']['mean'] == 99
