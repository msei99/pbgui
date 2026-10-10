"""Pinned, independently derived direction expectations without PB8 or CUDA."""
import copy
import json
from pathlib import Path

import pytest

from vast_direction_validation import (
    DirectionBound, gpu_sides, materialize_direction, scenario_direction, validate_gpu_directions,
)
from vast_direction_fixtures import direction_config
from vast_scenarios import flatten_overrides

CONTRACT = json.loads((Path(__file__).parent / 'fixtures/vast_direction_contract.json').read_text())


@pytest.mark.parametrize('seed_exposure', [0, 1])
def test_short_zero_crossing_names_range_and_only_marks_exposure(seed_exposure):
    """Whether Short starts on or off, a 0–1 WE range identifies the same actual control."""
    config = direction_config()
    config['live']['approved_coins']['short'] = ['BTC']
    config['bot']['short']['risk'].update(n_positions=1, total_wallet_exposure_limit=seed_exposure)
    config['optimize']['bounds'].update(short_n_positions=[1, 1], short_total_wallet_exposure_limit=[0, 1])
    errors, _ = validate_gpu_directions(config)
    finding = next(item for item in errors if 'Short must stay' in item['message'])
    assert 'range [0, 1] includes 0 (off) and positive values (on)' in finding['message']
    assert 'minimum above 0' in finding['message']
    assert finding['bound_keys'] == ['short.risk.total_wallet_exposure_limit']
    config['optimize']['bounds']['short_total_wallet_exposure_limit'] = [0, 0]
    assert not any('short.risk.total_wallet_exposure_limit' in item.get('bound_keys', [])
                   for item in validate_gpu_directions(config)[0])


def test_unknown_coin_count_is_deferred_but_known_violation_still_blocks():
    """Suppress provisional UI findings without weakening authoritative count checks."""
    config = direction_config()
    config['live']['approved_coins']['long'] = ['BTC', 'ETH', 'SOL']
    config['optimize']['bounds']['long_n_positions'] = [1, 7]
    errors, warnings = validate_gpu_directions(config)
    assert not errors and warnings
    assert all(item['pending_coin_count'] is True for item in warnings)
    errors, warnings = validate_gpu_directions(config, coin_counts={'backtest': 3})
    assert not warnings
    assert any('positions' in item['message'] for item in errors)
    assert all(not item.get('pending_coin_count') for item in errors)
    config['optimize']['bounds']['long_n_positions'] = [1, 3]
    assert validate_gpu_directions(config, coin_counts={'backtest': 3}) == ([], [])


@pytest.mark.parametrize('case', CONTRACT['cases'], ids=lambda case: case['name'])
def test_pinned_direction_gold(case):
    """Assert intermediate gates and decisions against literal upstream expectations."""
    config = case['config']
    original = copy.deepcopy(config)
    effective, base, bounds = materialize_direction(config)
    expected = case['expected']
    assert base == expected['base']
    assert {key: [bound.low, bound.high] for key, bound in bounds.items()} == expected['bounds']
    early = [sorted(gpu_sides(effective))]
    for scenario in config['backtest'].get('scenarios', []):
        context, _, _ = scenario_direction(effective, base, bounds, flatten_overrides(scenario.get('overrides', {})))
        early.append(sorted(gpu_sides(context)))
    assert early == expected['early_sides']
    for scenario, late_expected in zip(config['backtest'].get('scenarios', []), expected['scenarios'], strict=True):
        inherited_coins = sorted(set(sum(config['live']['approved_coins'].values(), [])))
        late, values, edges = scenario_direction(effective, base, bounds,
            flatten_overrides(scenario.get('overrides', {})), scenario.get('coins', inherited_coins))
        assert sorted(gpu_sides(late)) == late_expected['sides']
        assert values == late_expected['base']
        assert {key: [edge.low, edge.high] for key, edge in edges.items()} == late_expected['bounds']
    errors, warnings = validate_gpu_directions(config, coin_counts=case['coin_counts'])
    assert len(errors) == expected['errors'], errors
    assert len(warnings) == expected['warnings'], warnings
    if expected['error_contains']:
        assert any(expected['error_contains'] in item['message'] for item in errors)
    assert config == original


def test_gold_source_matches_the_current_cloud_revision():
    """Changing the worker pin requires deliberate review of this direction contract."""
    from vast_jobs import IMAGE, REVISION
    from vast_config_validation import PROFILE_IMAGE, PROFILE_REVISION
    assert CONTRACT['reference_commit'] == REVISION == PROFILE_REVISION
    assert IMAGE == PROFILE_IMAGE


@pytest.mark.parametrize('bound,seed,digits,expected', [
    ([1, 3, 1], 1.5, 6, 2), ([1, 3, 1], 99, 6, 3),
    ([.1, .4, .1], .25, 6, .2), ([.1, .4, .1], .35, 6, .3),
    ([0, 2], 1.23456789, 6, 1.23457), ([1.234566, 1.234568], 1.23456789, 6, 1.234568),
    ([1, 1, .1], 2, 6, 1), ([0, 1, 2], .12345678, 6, .123457),
])
def test_seed_quantization_and_significant_rounding(bound, seed, digits, expected):
    """Use PB8's index rounding for steps and rounding-before-clamp for continuous bounds."""
    assert DirectionBound.parse(bound).project(seed, digits) == pytest.approx(expected)


@pytest.mark.parametrize('selector', ['risk.*', 'short.risk.*', 'bot.short.risk.*', '*.short.risk.*'])
def test_fixed_params_wildcards_precede_collapse(selector):
    """A fixed current value takes precedence over an originally activatable range."""
    config = direction_config()
    config['optimize']['bounds'].update(short_total_wallet_exposure_limit=[0, 1], short_n_positions=[0, 1])
    config['optimize']['fixed_params'] = [selector]
    _, base, bounds = materialize_direction(config)
    assert bounds['short_n_positions'].high == base['short_n_positions'] == 0


@pytest.mark.parametrize('parent', ['bot.short', 'bot.short.risk'])
def test_scenario_parent_overrides_shadow_descendants(parent):
    """Parent overrides pin both gates, even after side collapse."""
    config = direction_config()
    value = {'total_wallet_exposure_limit': 1, 'n_positions': 1}
    override = {'risk': value} if parent == 'bot.short' else value
    effective, base, bounds = materialize_direction(config)
    scenario, values, edges = scenario_direction(effective, base, bounds, {parent: override}, ['BTC'])
    assert gpu_sides(scenario) == {'long', 'short'}
    assert values['short_n_positions'] == edges['short_n_positions'].low == edges['short_n_positions'].high == 1
    config['live']['approved_coins']['short'] = ['BTC']
    config['backtest'].update(suite_enabled=True, scenarios=[{'label': 'parent', 'overrides': {parent: override}}])
    from vast_config_validation import validate_cloud_config
    assert validate_cloud_config(config) == []


def test_override_without_bound_does_not_pin_vector():
    """The scenario config changes, but missing bound keys keep the existing base fallback."""
    config = direction_config()
    effective, base, bounds = materialize_direction(config)
    del bounds['short_n_positions']
    _, values, edges = scenario_direction(effective, base, bounds, {'bot.short.risk.n_positions': 1})
    assert values['short_n_positions'] == 0
    assert 'short_n_positions' not in edges


@pytest.mark.parametrize('value', [True, '1', float('nan'), float('inf')])
def test_runtime_gate_requires_finite_numeric_value(value):
    """Invalid pinned values cannot silently disable Short."""
    config = direction_config()
    config['optimize']['fixed_runtime_overrides']['bot.short.risk.n_positions'] = value
    errors, _ = validate_gpu_directions(config)
    assert errors and 'finite numeric' in errors[0]['message']


def test_unknown_override_path_is_rejected():
    """A missing canonical runtime path is a checkable input error."""
    config = direction_config()
    config['optimize']['fixed_runtime_overrides']['bot.short.risk.missing'] = 1
    assert validate_gpu_directions(config)[0]


def test_clamped_seed_may_activate_a_previously_inactive_side():
    """Do not reintroduce the removed _validate_seed_side_match restriction."""
    config = direction_config()
    config['bot']['long']['risk'].update(total_wallet_exposure_limit=0, n_positions=0)
    assert validate_gpu_directions(config) == ([], [])


def test_position_rounding_is_only_used_for_config_activation():
    """gpu_side_enabled rounds positions, unlike vector_side_enabled."""
    config = direction_config()
    config['bot']['long']['risk']['n_positions'] = .5
    config['optimize']['bounds']['long_n_positions'] = [.5, .5]
    assert not gpu_sides(config)
    errors, _ = validate_gpu_directions(config)
    assert any('at least one enabled side' in item['message'] for item in errors)
