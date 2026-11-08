"""Offline native parameter evidence: provenance, finite bins and bounded projection."""
import copy
import json

import pytest

from pb8_loop_bounds import diagnose
from pb8_loop_evaluation_evidence import evaluation_overview
from pb8_loop_parameter_regions import regions, MAX_PARAMETERS
from tests.test_pb8_loop_bounds import dataset


@pytest.fixture
def cohort():
    """Use synthetic, source-owned native nominations; never read production data."""
    record, _, config = dataset(0)
    record.update(optimizer_research_version=1, candidates=[], evaluation_method=[
        {'metric': 'gain', 'direction': 'max', 'reducer': 'mean'}])
    source = {'kind': 'optimizer', 'operation': 'source', 'round': 0, 'config': config,
              'research': {'budget': {'mode': 'proxy', 'value': 100000, 'fraction': 1}},
              'proxy_evaluations': 106496}
    record['jobs'] = [source]
    record['settings'].update(run_limit_mode='proxy', run_proxy=100000)
    record['initial_config'] = copy.deepcopy(config)
    record['best']['candidate'] = {'config': copy.deepcopy(config)}
    record['best']['candidate']['config']['bot']['long']['strategy']['ema_anchor']['base_qty_pct'] = .09
    for index in range(64):
        candidate = {'id': str(index), 'optimizer_job': 'source', 'config': copy.deepcopy(config),
                     'native_cpu_qualified': True, 'overrides': {},
                     'metric_statistics': {'gain': {'mean': 100 if index < 16 else 1 + index / 100}}}
        candidate['config']['bot']['long']['strategy']['ema_anchor']['base_qty_pct'] = (
            .005 + index * .0001 if index < 16 else .001 + .099 * (index - 16) / 47)
        record['candidates'].append(candidate)
    return record


def test_regions_keep_cutoff_ties_native_criterion_and_incumbent(cohort):
    """Elite distribution describes all cutoff ties and never silently excludes the winner."""
    result = regions(cohort, cohort['evaluation_method'])
    profile = result['profiles'][0]
    row = next(row for row in profile['parameters'] if row['parameter'].endswith('base_qty_pct'))
    assert result['available'] and profile['qualified_distinct_count'] == 64
    assert profile['elite_count'] == 16 and row['elite_bins'] == [16, 0, 0, 0, 0, 0, 0, 0]
    assert sum(row['qualified_bins']) == 64 and row['qualified_bins'][-1] > 0
    assert row['incumbent_value'] == .09 and row['incumbent_inside_range']
    assert profile['actual_proxy_evaluations'] == 106496 and profile['requested_budget']['value'] == 100000
    assert 'not causal proof' in result['interpretation']


@pytest.mark.parametrize('change', ['unqualified', 'observer', 'overrides', 'nan', 'bool', 'duplicate'])
def test_regions_exclude_ineligible_or_duplicate_evidence(cohort, change):
    """Proxy, observer, missing/nonfinite metrics and duplicated bots add no evidence."""
    added = copy.deepcopy(cohort['candidates'][0])
    added['id'] = 'extra'
    if change == 'unqualified':
        added['native_cpu_qualified'] = False
    elif change == 'observer':
        added['observer_only'] = True
    elif change == 'overrides':
        added['overrides'] = {'HYPE.json': {'anything': 1}}
    elif change == 'nan':
        added['metric_statistics']['gain']['mean'] = float('nan')
    elif change == 'bool':
        added['metric_statistics']['gain']['mean'] = True
    cohort['candidates'].append(added)
    assert regions(cohort, cohort['evaluation_method'])['profiles'][0]['qualified_distinct_count'] == 64


def test_regions_respect_min_direction_and_bound_summary_size(cohort):
    """Many active parameters produce capped rows and a visible omitted-name catalogue."""
    source = cohort['jobs'][0]['config']['optimize']['bounds']['long']['strategy']['ema_anchor']
    for number in range(40):
        source[f'knob_{number}'] = [0, 1]
        for index, candidate in enumerate(cohort['candidates']):
            candidate['config']['bot']['long']['strategy']['ema_anchor'][f'knob_{number}'] = index / 63
    criteria = [{**cohort['evaluation_method'][0], 'direction': 'min'}]
    profile = regions(cohort, criteria)['profiles'][0]
    assert profile['elite_count'] == 13 and profile['criterion']['direction'] == 'min'
    assert len(profile['parameters']) == MAX_PARAMETERS and profile['other_parameter_names']
    assert len(json.dumps(profile)) < 15000


def test_regions_small_cohort_or_legacy_cannot_create_false_support(cohort):
    """A few nominated winners do not become population-wide confidence evidence."""
    cohort['candidates'] = cohort['candidates'][:5]
    assert not regions(cohort, cohort['evaluation_method'])['available']
    cohort.pop('optimizer_research_version')
    assert not regions(cohort, cohort['evaluation_method'])['available']


def test_regions_overflow_means_stay_json_finite_and_summary_is_discoverable(cohort):
    """Finite but huge metrics cannot pause a Loop through nonfinite summary JSON."""
    for candidate in cohort['candidates']:
        candidate['metric_statistics']['gain']['mean'] = 1e308
    result = regions(cohort, cohort['evaluation_method'])
    json.dumps(result, allow_nan=False)
    diagnosis = diagnose(cohort, {}, {'groups': []})
    overview = evaluation_overview({'boundary_diagnostics': diagnosis}, [])
    summary = overview['bounds']['parameter_regions_summary']
    assert summary['available'] and summary['path'].endswith('/parameter_regions')
    assert 'parameter_regions' not in overview['bounds']


def test_regions_do_not_expose_rng_or_worker_settings(cohort):
    """Added summaries contain parameter measurements, never controller RNG or GPU knobs."""
    cohort['jobs'][0]['config']['optimize']['seed'] = 123456789
    cohort['jobs'][0]['research']['seed'] = 123456789
    cohort['best']['candidate']['config']['optimize']['seed'] = 123456789
    cohort['jobs'][0]['config']['optimize']['gpu']['population_size'] = 987654321
    payload = json.dumps(regions(cohort, cohort['evaluation_method']))
    assert '123456789' not in payload and '987654321' not in payload
    assert 'population_size' not in payload
