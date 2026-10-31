"""Offline native-oracle regressions for metric availability before any AI launch."""
import copy
import json
from pathlib import Path

import pytest

from pb8_metric_dependencies import metric_dependency_errors
from pb8_loop_ai_v2 import validate_decision
from tests.test_pb8_loop_optimizer import config, settings, valid_gpu_config

GOLD = json.loads((Path(__file__).parent / 'fixtures/pb8_metric_dependencies.json').read_text())


def source(mode='coin', long=False, short=False, portfolio=False):
    """Build an isolated run with explicit controller policy and no runtime files."""
    value = config()
    value['live']['hsl_signal_mode'] = mode
    for side, enabled in [('long', long), ('short', short), ('hsl', portfolio)]:
        if side == 'hsl':
            value['bot']['hsl'] = {'enabled': enabled}
        else:
            value['bot'][side]['hsl'] = {'enabled': enabled}
    return value


@pytest.mark.parametrize('case', GOLD['cases'].values(), ids=GOLD['cases'].keys())
def test_all_metrics_match_native_hsl_oracle(case):
    """527 native names/aliases match the real PB8 validator across twelve modes."""
    value = source(case['mode'], case['long'], case['short'], case['portfolio'])
    expected = set(case['rejected'])
    for metric in GOLD['metrics']:
        value['optimize']['scoring'] = [{'metric': metric}]
        assert bool(metric_dependency_errors(value)) == (metric in expected), metric


@pytest.mark.parametrize('group', ['scoring', 'limits'])
@pytest.mark.parametrize('metric', ['drawdown_worst_ema_strategy_eq',
                                 'drawdown_worst_mean_1pct_ema_strategy_eq',
                                 'hard_stop_count_long', 'usd_drawdown_worst_ema_hsl',
                                 'drawdown_worst_ema_hsl_mean', 'drawdown_worst_ema_hsl_mean_usd'])
def test_rejects_dependencies_in_both_editors_without_mutation(group, metric):
    """Aliases, currency spelling and statistics cannot bypass an HSL-off contract."""
    value = source()
    value['optimize'][group] = [{'metric': metric}]
    before = copy.deepcopy(value)
    errors = metric_dependency_errors(value)
    assert errors and errors[0]['path'] == f'optimize.{group}.0.metric'
    assert value == before


def test_scenarios_check_only_consumed_results_and_skip_disabled_limits():
    """Named objectives, inherited objective_scenario and aggregate limits differ."""
    value = source()
    value['backtest'].update(suite_enabled=True, scenarios=[
        {'label': 'on', 'overrides': {'bot.long.hsl.enabled': True}},
        {'label': 'off', 'overrides': {}},
    ])
    value['optimize'].update(objective_scenario='on',
                            scoring=[{'metric': 'drawdown_worst_ema_strategy_eq'}],
                            limits=[{'metric': 'hard_stop_count_short', 'enabled': False}])
    assert metric_dependency_errors(value) == []
    value['optimize']['scoring'][0]['scenario'] = None
    errors = metric_dependency_errors(value)
    assert len(errors) == 1 and "'off'" in errors[0]['message']
    value['optimize']['scoring'][0]['scenario'] = 'on'
    value['optimize']['limits'] = [{'metric': 'hard_stop_count_long'}]
    assert "'off'" in metric_dependency_errors(value)[0]['message']
    value['optimize']['limits'][0]['scenario'] = 'on'
    assert metric_dependency_errors(value) == []


def test_effective_runtime_and_nested_scenario_patches():
    """Validate effective controllers, including dotted and parent subtree patches."""
    value = source(long=True)
    value['optimize']['scoring'] = [{'metric': 'drawdown_worst_ema_strategy_eq_long'}]
    value['optimize']['fixed_runtime_overrides'] = {'bot.long.hsl.enabled': False}
    assert metric_dependency_errors(value)
    value['backtest'].update(suite_enabled=True, scenarios=[
        {'label': 'enabled', 'overrides': {'bot': {'long': {'hsl': {'enabled': True}}}}}])
    assert metric_dependency_errors(value) == []


def test_declared_coin_policy_can_supply_coin_mode_controller():
    """Native coin-mode policy overrides count; pside ignores per-coin controllers."""
    value = source()
    value['coin_overrides'] = {'BTC': {'bot': {'long': {'hsl': {'enabled': True}}}}}
    value['optimize']['scoring'] = [{'metric': 'drawdown_worst_ema_strategy_eq_long'}]
    assert metric_dependency_errors(value) == []
    value['live']['hsl_signal_mode'] = 'pside'
    assert metric_dependency_errors(value)


def test_separate_loop_coin_patches_use_canonical_nested_policy():
    """Sparse Loop bundles are checked without saving or mutating coin files."""
    value = source()
    value['optimize']['scoring'] = [{'metric': 'drawdown_worst_ema_strategy_eq_long'}]
    patches = {'BTC': {'bot.long.hsl.enabled': True}}
    before = copy.deepcopy((value, patches))
    assert metric_dependency_errors(value, patches) == []
    assert (value, patches) == before


def test_cloud_preparation_rejects_dependency_before_export(tmp_path, monkeypatch):
    """The common cloud boundary stops bad manual or AI jobs before input export."""
    from vast_jobs import JobStore
    from vast_provider import VastError
    value = valid_gpu_config()
    value['optimize']['scoring'] = [{'metric': 'drawdown_worst_ema_strategy_eq'}]

    def forbidden(*args, **kwargs):
        """Invalid objectives must not reach native data preparation."""
        pytest.fail('Input export started for an unavailable metric')

    monkeypatch.setattr('setup.vast_gpu_benchmark.prepare.select_shards', forbidden)
    with pytest.raises(VastError, match='no enabled controller'):
        JobStore(tmp_path / 'vast').prepare('invalid', value, 'hash', tmp_path,
                                           tmp_path, tmp_path, 512, 4, False)


@pytest.mark.parametrize('provider', ['chatgpt', 'opencode-go', 'opencode-zen', 'claude-code'])
@pytest.mark.parametrize('execution', ['cpu', 'gpu', 'vast'])
def test_every_loop_provider_rejects_bad_variant_before_accepting_decision(provider, execution):
    """Production decision validation returns correction feedback before job creation."""
    original = source() if execution == 'cpu' else valid_gpu_config()
    for side in ('long', 'short'):
        original['bot'][side]['hsl'] = {'enabled': False}
    original['optimize']['backend'] = 'pymoo' if execution == 'cpu' else 'gpu'
    row = {'settings': settings(provider=provider, execution=execution, hsl_mode='off'),
           'initial_config': original, 'initial_overrides': {}}
    candidate = copy.deepcopy(original)
    candidate['optimize']['scoring'] = [{'metric': 'drawdown_worst_ema_strategy_eq'}]
    decision = {'variants': [{'config': candidate, 'overrides': {}, 'reason': 'experiment'}]}
    with pytest.raises(ValueError, match='has no enabled controller'):
        validate_decision(row, 'bootstrap', {'max_variants': 1}, json.dumps(decision))
    candidate['optimize']['scoring'][0]['metric'] = 'adg_strategy_eq'
    assert validate_decision(row, 'bootstrap', {'max_variants': 1}, json.dumps(decision)) == decision


def test_native_oracle_revision_and_alias_contract_are_pinned():
    """An updated worker requires deliberate review of conditional metric rules."""
    from vast_config_validation import PROFILE_REVISION, _METRIC_CONTRACT
    assert GOLD['reference_commit'] == PROFILE_REVISION
    assert GOLD['source_sha256'] == _METRIC_CONTRACT['source_sha256']['config.hsl']
