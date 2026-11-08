"""Offline boundary diagnosis and planning contracts use isolated synthetic CPU evidence."""
import copy

import pytest

from pb8_loop_bounds import active_ranges, diagnose, validate_response
from pb8_loop_evaluation_evidence import query
from pb8_loop_evidence import required_content, tokens


def dataset(count=8):
    """Build two paired seeds with tied qualified CPU results and active long bounds."""
    config = {'live': {'strategy_kind': 'ema_anchor'}, 'bot': {
        'long': {'risk': {'n_positions': 1, 'total_wallet_exposure_limit': 2},
                 'strategy': {'ema_anchor': {'base_qty_pct': .099}},
                 'hsl': {'enabled': False}},
        'short': {'risk': {'n_positions': 0, 'total_wallet_exposure_limit': 0}}},
        'optimize': {'bounds': {'long': {'risk': {'n_positions': [1, 1],
            'total_wallet_exposure_limit': [1, 8]},
            'strategy': {'ema_anchor': {'base_qty_pct': [.001, .1]}}}},
            'gpu': {'population_size': None}}}
    record = {'settings': {'goals': {'coins': ['HYPE'], 'max_twe': 8}},
              'rubric': [{'metrics': ['gain'], 'target': None, 'direction': 'max'}],
              'jobs': [], 'best': {'operation': 'v0'}, 'stagnation': 3}
    options = {}
    for i in range(count):
        source = {'kind': 'optimizer', 'operation': f'o{i}', 'round': i,
                  'config': copy.deepcopy(config), 'research': {'seed': i % 2}}
        candidate = {'config': copy.deepcopy(config), 'optimizer_job': source['operation'], 'overrides': {}}
        record['jobs'].extend([source, {'kind': 'validation', 'operation': f'v{i}', 'candidate': candidate}])
        options[f'v{i}'] = {'per_report': [{'gain': 2}],
                          'assessment': {'comparable': True, 'hard_targets_met': True, 'simulation_complete': True}}
    return record, options, config


def response(diagnosis, config, action='retain'):
    """Explain every observed signal using its exact controller identity."""
    experiment = {'purpose': 'explore'} if action == 'adjust' else {'purpose': 'replicate', 'source_operation': 'o0'}
    return {'config': copy.deepcopy(config), 'experiment': experiment,
            'boundary_response': [{'id': r['id'], 'action': action,
                                   'reason': 'Test whether the measured boundary effect repeats'}
                                  for r in diagnosis['parameters']]}


def test_executed_bounds_exact_values_and_distinct_trials():
    """Actual execution, qualified CPU evidence and distinct source trials drive the signal."""
    record, options, config = dataset()
    before = copy.deepcopy((record, options))
    report = diagnose(record, options)
    row = report['parameters'][0]
    assert row['parameter'] == 'long.strategy.ema_anchor.base_qty_pct'
    assert row['bounds'] == [.001, .1]
    assert row['near_good_count'] == 8 and row['source_trial_count'] == 8
    assert row['strength'] == 'repeated' and row['incumbent_near']
    assert report['unchanged_bounds_trials'] == 8
    assert (record, options) == before
    # An older proposal must not replace the actual executed upper bound.
    record['jobs'][0]['executed_config'] = copy.deepcopy(config)
    record['jobs'][0]['executed_config']['optimize']['bounds']['long']['strategy']['ema_anchor']['base_qty_pct'] = [.001, .2]
    assert diagnose(record, options)['parameters'][0]['qualified_count'] == 7


@pytest.mark.parametrize('problem', ['proxy', 'holdout', 'failed', 'unqualified', 'incomplete', 'overrides', 'missing', 'nan', 'out_of_range'])
def test_ineligible_data_never_supports_a_boundary_signal(problem):
    """Observer, unsafe, unavailable and ambiguously overridden values cannot drive expansion."""
    record, options, _ = dataset(1)
    job = record['jobs'][1]
    if problem in {'proxy', 'holdout'}:
        job['kind'] = 'optimizer' if problem == 'proxy' else 'holdout'
        if problem == 'proxy':
            record['jobs'].pop()
    elif problem in {'failed', 'unqualified', 'incomplete'}:
        options['v0']['assessment'][{'failed': 'comparable', 'unqualified': 'hard_targets_met', 'incomplete': 'simulation_complete'}[problem]] = False
    elif problem == 'overrides':
        job['candidate']['overrides'] = {'HYPE': {'bot': {}}}
    else:
        bot = job['candidate']['config']['bot']['long']['strategy']['ema_anchor']
        if problem == 'missing':
            bot.pop('base_qty_pct')
        else:
            bot['base_qty_pct'] = float('nan') if problem == 'nan' else .5
    assert not diagnose(record, options)['parameters']


def test_inactive_and_fixed_controls_are_excluded():
    """Disabled sides/controllers, irrelevant forager controls and fixed leaves are not tunable."""
    record, _, config = dataset()
    bounds = config['optimize']['bounds']
    bounds['short'] = {'strategy': {'ema_anchor': {'base_qty_pct': [.001, .1]}}}
    bounds['long'].update(hsl={'threshold': [0, 1]}, forager={'volume': [1, 10]})
    bounds['long']['strategy']['other'] = {'value': [0, 1]}
    config['optimize']['fixed_params'] = ['bot.long.strategy.ema_anchor.base_qty_pct']
    assert set(active_ranges(config, record['settings']['goals'])) == {'long.risk.total_wallet_exposure_limit'}


def test_only_top_cpu_quartile_drives_pressure_with_ties_and_no_proxy_ranking():
    """Poor edge candidates do not force changes when independently better candidates are interior."""
    record, options, _ = dataset()
    for i in range(2):
        options[f'v{i}']['per_report'] = [{'gain': 3}]
        record['jobs'][i * 2 + 1]['candidate']['config']['bot']['long']['strategy']['ema_anchor']['base_qty_pct'] = .05
    assert not diagnose(record, options)['parameters']
    options['v2']['per_report'] = [{'gain': 3}]
    assert diagnose(record, options)['parameters'][0]['near_good_count'] == 1


def test_hard_exposure_edge_is_reported_as_protected():
    """Hard caps remain visible without requiring expansion beyond the user's ceiling."""
    record, options, _ = dataset(1)
    record['jobs'][1]['candidate']['config']['bot']['long']['risk']['total_wallet_exposure_limit'] = 8
    report = diagnose(record, options)
    row = next(r for r in report['parameters'] if '.risk.' in r['parameter'])
    assert row['protected'] and row['edge'] == 'upper'


def test_hard_caps_can_be_retained_but_not_expanded():
    """Sustained pressure at a user ceiling never demands or accepts crossing it."""
    record, options, config = dataset()
    for job in record['jobs']:
        if job['kind'] == 'validation':
            bot = job['candidate']['config']['bot']['long']
            bot['strategy']['ema_anchor']['base_qty_pct'] = .05
            bot['risk']['total_wallet_exposure_limit'] = 8
    report = diagnose(record, options)
    assert len(report['parameters']) == 1 and report['parameters'][0]['protected']
    validate_response(record, report, response(report, config))
    variant = response(report, config, 'adjust')
    variant['config']['optimize']['bounds']['long']['risk']['total_wallet_exposure_limit'][1] = 9
    with pytest.raises(ValueError, match='protected'):
        validate_response(record, report, variant)


def test_tiny_bound_noise_is_not_an_adjustment():
    """A negligible endpoint edit cannot satisfy the adaptive experiment contract."""
    record, options, config = dataset(1)
    report = diagnose(record, options)
    variant = response(report, config, 'adjust')
    variant['config']['optimize']['bounds']['long']['strategy']['ema_anchor']['base_qty_pct'][1] += 1e-8
    with pytest.raises(ValueError, match='10%'):
        validate_response(record, report, variant)


def test_testing_a_weak_signal_cannot_defer_sustained_incumbent_pressure():
    """The required experiment must address a sustained signal, not an unrelated edge."""
    record, options, config = dataset()
    report = diagnose(record, options)
    weak = {**report['parameters'][0], 'id': 'weak', 'strength': 'single_trial', 'incumbent_near': False}
    report['parameters'].append(weak)
    variant = response(report, config)
    variant['boundary_response'][1]['action'] = 'verify'
    with pytest.raises(ValueError, match='scoring-only'):
        validate_response(record, report, variant)


def test_single_seed_can_be_retained_but_sustained_pressure_requires_a_test():
    """A weak hypothesis needs explanation; repeated stagnant pressure cannot be ignored forever."""
    record, options, config = dataset(1)
    report = diagnose(record, options)
    validate_response(record, report, response(report, config))
    record, options, config = dataset()
    report = diagnose(record, options)
    with pytest.raises(ValueError, match='scoring-only'):
        validate_response(record, report, response(report, config))
    validate_response(record, report, response(report, config, 'verify'))
    variant = response(report, config, 'adjust')
    variant['config']['optimize']['bounds']['long']['strategy']['ema_anchor']['base_qty_pct'][1] = .12
    validate_response(record, report, variant)


@pytest.mark.parametrize('problem', ['missing', 'unknown', 'duplicate', 'false_adjust', 'false_verify', 'false_retain'])
def test_planning_gate_checks_claimed_actions_against_actual_variants(problem):
    """Model prose cannot masquerade as an actual bound test or use stale signal IDs."""
    record, options, config = dataset(1)
    report = diagnose(record, options)
    variant = response(report, config)
    if problem == 'missing':
        variant.pop('boundary_response')
    elif problem == 'unknown':
        variant['boundary_response'][0]['id'] = 'unknown'
    elif problem == 'duplicate':
        variant['boundary_response'] *= 2
    elif problem == 'false_adjust':
        variant['boundary_response'][0]['action'] = 'adjust'
    elif problem == 'false_verify':
        variant['boundary_response'][0]['action'] = 'verify'
        variant['experiment']['purpose'] = 'explore'
    else:
        variant['config']['optimize']['bounds']['long']['strategy']['ema_anchor']['base_qty_pct'][1] = .12
    with pytest.raises(ValueError):
        validate_response(record, report, variant)


def test_bounded_evidence_exposes_all_signals_without_history_sized_context():
    """Full diagnostics stay pageable while the mandatory summary has fixed shape."""
    record, options, _ = dataset(100)
    diagnosis = diagnose(record, options)
    diagnosis['parameters'] *= 1000
    raw = {'stage': 'evaluate', 'rubric': [], 'evidence': {'winner_options': options,
           'optimizer_research': {}, 'boundary_diagnostics': diagnosis}}
    required = required_content(raw)
    assert required['evidence']['evaluation']['bounds']['signal_count'] == 1
    assert tokens(required) < 1800
    snapshot = {'id': 'a' * 32, 'sha256': 'b' * 64, 'data': raw}
    cursor, seen = 0, 0
    while cursor is not None:
        page = query(snapshot, '/evaluation/bounds', cursor, 1500)
        assert tokens(page) <= 1500
        seen += len(page['entries'])
        cursor = page['next_offset']
    assert seen == 1000


@pytest.mark.parametrize('purpose', ['replicate', 'confirm'])
@pytest.mark.parametrize('action', ['retain', 'verify'])
def test_older_source_bounds_remain_repeatable(purpose, action):
    """A five-percent difference from the latest trial cannot invalidate an exact source repeat."""
    record, options, config = dataset(2)
    report = diagnose(record, options)
    source = copy.deepcopy(config)
    source['optimize']['bounds']['long']['strategy']['ema_anchor']['base_qty_pct'] = [.001, .105]
    record['jobs'][0]['executed_config'] = source
    variant = response(report, source, action)
    variant['experiment']['purpose'] = purpose
    validate_response(record, report, variant)


@pytest.mark.parametrize('edge', ['lower', 'upper'])
def test_adjust_must_move_the_pressured_endpoint(edge):
    """Moving the opposite endpoint cannot masquerade as addressing measured pressure."""
    record, options, config = dataset(1)
    if edge == 'lower':
        record['jobs'][1]['candidate']['config']['bot']['long']['strategy']['ema_anchor']['base_qty_pct'] = .001
    report = diagnose(record, options)
    assert report['parameters'][0]['edge'] == edge
    variant = response(report, config, 'adjust')
    bounds = variant['config']['optimize']['bounds']['long']['strategy']['ema_anchor']['base_qty_pct']
    bounds[0 if edge == 'upper' else 1] += .02
    with pytest.raises(ValueError, match='pressured endpoint'):
        validate_response(record, report, variant)


@pytest.mark.parametrize('width', [1e-10, 1, 1e10])
@pytest.mark.parametrize('edge', ['lower', 'upper'])
def test_exact_ten_percent_change_tolerates_float_roundoff(width, edge):
    """Relative tolerance accepts the exact mathematical threshold without allowing tiny edits."""
    record, options, config = dataset(1)
    config['optimize']['bounds']['long']['strategy']['ema_anchor']['base_qty_pct'] = [0, width]
    record['jobs'][0]['config'] = copy.deepcopy(config)
    record['jobs'][1]['candidate']['config']['bot']['long']['strategy']['ema_anchor']['base_qty_pct'] = width if edge == 'upper' else 0
    report = diagnose(record, options)
    variant = response(report, config, 'adjust')
    interval = variant['config']['optimize']['bounds']['long']['strategy']['ema_anchor']['base_qty_pct']
    interval[1 if edge == 'upper' else 0] = 1.1 * width if edge == 'upper' else -.1 * width
    validate_response(record, report, variant)
    interval[1 if edge == 'upper' else 0] = 1.099999 * width if edge == 'upper' else -.099999 * width
    with pytest.raises(ValueError, match='10%'):
        validate_response(record, report, variant)


def test_one_unchanged_edge_can_be_retained_while_the_other_is_adjusted():
    """Opposing signals can be answered consistently without declaring a false second adjustment."""
    record, options, config = dataset(2)
    record['jobs'][1]['candidate']['config']['bot']['long']['strategy']['ema_anchor']['base_qty_pct'] = .001
    report = diagnose(record, options)
    assert {r['edge'] for r in report['parameters']} == {'lower', 'upper'}
    variant = response(report, config, 'adjust')
    variant['config']['optimize']['bounds']['long']['strategy']['ema_anchor']['base_qty_pct'][1] = .12
    for row, answer in zip(report['parameters'], variant['boundary_response']):
        if row['edge'] == 'lower':
            answer['action'] = 'retain'
    validate_response(record, report, variant)


def test_override_exclusions_remain_visible_when_no_signals_can_be_measured():
    """Missing override coverage is distinguished from an observed absence of boundary pressure."""
    record, options, _ = dataset(2)
    for job in record['jobs']:
        if job['kind'] == 'validation':
            job['candidate']['overrides'] = {'HYPE': {'bot': {}}}
    report = diagnose(record, options)
    assert report['candidate_count'] == report['signal_count'] == 0
    assert report['excluded_override_count'] == 2
    raw = {'stage': 'evaluate', 'rubric': [], 'evidence': {
           'winner_options': options, 'optimizer_research': {}, 'boundary_diagnostics': report}}
    summary = required_content(raw)['evidence']['evaluation']['bounds']
    assert summary['excluded_override_count'] == 2
    assert 'excluded' in summary['override_coverage']


@pytest.mark.parametrize('edge', ['lower', 'upper'])
@pytest.mark.parametrize('fraction', [.1, .5])
def test_inward_contraction_cannot_satisfy_the_stagnation_gate(edge, fraction):
    """Cutting away a pressured region is never counted as a boundary adjustment."""
    record, options, config = dataset(8)
    if edge == 'lower':
        for job in record['jobs']:
            if job['kind'] == 'validation':
                job['candidate']['config']['bot']['long']['strategy']['ema_anchor']['base_qty_pct'] = .001
    report = diagnose(record, options)
    assert report['parameters'][0]['edge'] == edge
    variant = response(report, config, 'adjust')
    interval = variant['config']['optimize']['bounds']['long']['strategy']['ema_anchor']['base_qty_pct']
    width = interval[1] - interval[0]
    if edge == 'upper':
        interval[1] -= fraction * width
    else:
        interval[0] += fraction * width
    with pytest.raises(ValueError, match='outward'):
        validate_response(record, report, variant)


@pytest.mark.parametrize('purpose', ['replicate', 'confirm'])
@pytest.mark.parametrize('near', [True, False])
def test_completed_verification_releases_stagnation_gate(purpose, near):
    """A measured repeat resolves the forced test even when boundary pressure disappears."""
    record, options, config = dataset()
    source = record['jobs'][-2]
    source.update(status='cancelled', proxy_evaluations=100,
                  research_verification={'status': 'verified'})
    source['research'].update(purpose=purpose, budget={'mode': 'proxy', 'value': 100})
    if not near:
        record['jobs'][-1]['candidate']['config']['bot']['long']['strategy']['ema_anchor']['base_qty_pct'] = .05
    report = diagnose(record, options)
    assert report['parameters'][0]['verification_count'] == 1
    variant = response(report, config)
    variant['experiment'] = {'purpose': 'explore'}
    validate_response(record, report, variant)


@pytest.mark.parametrize('problem', ['running', 'under_budget', 'complete_under_budget', 'failed', 'unverified', 'unqualified', 'different_bounds', 'missing_parameter'])
def test_unfinished_or_unmeasured_repeat_does_not_release_gate(problem):
    """Planning a repeat, interrupting it or changing its ranges cannot manufacture verification."""
    record, options, config = dataset()
    source = record['jobs'][2]
    source.update(status='cancelled', proxy_evaluations=100, research_verification={'status': 'verified'})
    source['research'].update(purpose='replicate', budget={'mode': 'proxy', 'value': 100})
    if problem == 'running':
        source['status'] = 'running'
    elif problem in {'under_budget', 'complete_under_budget'}:
        source['proxy_evaluations'] = 99
        if problem == 'complete_under_budget':
            source['status'] = 'complete'
    elif problem == 'failed':
        source['error'] = 'Temporary failure'
    elif problem == 'unverified':
        source['research_verification']['status'] = 'failed'
    elif problem == 'unqualified':
        options['v1']['assessment']['hard_targets_met'] = False
    elif problem == 'missing_parameter':
        record['jobs'][3]['candidate']['config']['bot']['long']['strategy']['ema_anchor'].pop('base_qty_pct')
    else:
        source['config']['optimize']['bounds']['long']['strategy']['ema_anchor']['base_qty_pct'][1] = .11
    report = diagnose(record, options)
    assert report['parameters'][0]['verification_count'] == 0
    with pytest.raises(ValueError, match='scoring-only'):
        validate_response(record, report, response(report, config))


def test_zero_lower_edge_can_be_retained_without_crossing_zero():
    """A deliberate zero floor permits new experiments instead of mandatory negative expansion."""
    record, options, config = dataset()
    for job in record['jobs']:
        target = job['config'] if job['kind'] == 'optimizer' else job['candidate']['config']
        target['optimize']['bounds']['long']['strategy']['ema_anchor']['base_qty_pct'][0] = 0
        target['bot']['long']['strategy']['ema_anchor']['base_qty_pct'] = .001
    config['optimize']['bounds']['long']['strategy']['ema_anchor']['base_qty_pct'][0] = 0
    report = diagnose(record, options)
    assert report['parameters'][0]['retention_basis'] == 'zero_lower_edge'
    variant = response(report, config)
    variant['experiment'] = {'purpose': 'explore'}
    validate_response(record, report, variant)
