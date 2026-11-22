"""Native source-binding ownership in disposable PB9 Suite data projections."""
import copy
import sys
from types import ModuleType, SimpleNamespace

import pytest

from pb9_config_helper import _cloud_export_plan


@pytest.fixture
def native_contract(monkeypatch):
    """Model the native stale-binding rejection without importing a real bot tree."""
    calls = []
    def universe(config, field, default):
        """Resolve the scenario's current source lists."""
        return {side: config['bot'][side]['universe'][field] for side in ('long', 'short')}
    def scenarios(backtest, **kwargs):
        """Return already parsed native scenarios, as the real helper consumes them."""
        return backtest['scenarios'], {}
    def apply(config, patches):
        """Native scenario normalization changes the source-authority identity."""
        config['_source_epoch'] += 1
        for dotted, value in patches.items():
            target = config
            parts = dotted.split('.')
            for part in parts[:-1]:
                target = target[part]
            target[parts[-1]] = copy.deepcopy(value)
        calls.append(('apply', config))
    def refresh(config):
        """The native owner rebinds the existing optimizer ranges to the new source."""
        assert config['backtest']['start_date'] == '2025-01-01'
        assert config['backtest']['end_date'] == '2025-06-01'
        assert config['backtest']['exchanges'] in (['bybit'], ['hyperliquid'])
        config['_bound_epoch'] = config['_source_epoch']
        calls.append(('refresh', config))
    def optimizer_warmup(config):
        """Old helper code reaches this rejection after a Suite source mutation."""
        if config['_bound_epoch'] != config['_source_epoch']:
            raise ValueError('optimizer binding plan does not match source authority')
        calls.append(('warmup', config))
        return config['optimize']['bounds']['warmup']
    modules = {
        'config.universe': {'get_side_universe_sources': universe},
        'config.age_policy': {'minimum_coin_age_days': lambda config, side: config['bot'][side]['universe']['minimum_coin_age_days']},
        'optimization.warmup': {'compute_optimizer_backtest_warmup_minutes': optimizer_warmup, 'refresh_optimizer_source_bindings': refresh},
        'warmup_utils': {'compute_backtest_warmup_minutes': lambda config: 60},
        'suite_runner': {'build_scenarios': scenarios, 'apply_scenario_overrides': apply},
    }
    for name, functions in modules.items():
        module = ModuleType(name)
        module.__dict__.update(functions)
        monkeypatch.setitem(sys.modules, name, module)
    return calls


@pytest.mark.parametrize('suite', [False, True])
@pytest.mark.parametrize('coins', [None, ['ETH'], []])
def test_cloud_projection_refreshes_each_scenario_without_mutating_source(native_contract, suite, coins):
    """Rebinding preserves source identity, scenario filters, dates and optimizer bounds."""
    source = {'_source_epoch': 1, '_bound_epoch': 1,
              'bot': {side: {'universe': {'approved_coins': ['BTC'], 'ignored_coins': [], 'minimum_coin_age_days': 30}} for side in ('long', 'short')},
              'optimize': {'bounds': {'warmup': 120}},
              'backtest': {'suite_enabled': suite, 'exchanges': ['bybit'], 'start_date': '2024-01-01', 'end_date': '2026-01-01', 'scenarios': []}}
    for label, exchange in [('train', 'bybit'), ('holdout', 'hyperliquid')]:
        source['backtest']['scenarios'].append(SimpleNamespace(
            label=label, coins=coins, ignored_coins=['DOGE'],
            start_date='2025-01-01', end_date='2025-06-01', exchanges=[exchange],
            overrides={'bot.long.universe.minimum_coin_age_days': 90}))
    before = copy.deepcopy(source)
    plans = _cloud_export_plan(source)['data_plans']
    assert source == before
    assert len(plans) == (3 if suite else 1)
    assert plans[0]['live']['approved_coins'] == {'long': ['BTC'], 'short': ['BTC']}
    assert plans[0]['live']['minimum_coin_age_days'] == 30
    assert all(plan['warmup_minutes'] == 120 for plan in plans)
    refreshed = [config for action, config in native_contract if action == 'refresh']
    assert len(refreshed) == (2 if suite else 0)
    for index, plan in enumerate(plans[1:]):
        expected = ['BTC'] if coins is None else coins
        assert plan['live']['approved_coins'] == {'long': expected, 'short': expected}
        assert plan['live']['minimum_coin_age_days'] == 90
        assert plan['backtest']['start_date'] == '2025-01-01'
        assert plan['backtest']['end_date'] == '2025-06-01'
        assert plan['backtest']['exchanges'] == [['bybit'], ['hyperliquid']][index]
        assert refreshed[index]['optimize'] == before['optimize']
        assert refreshed[index] is not source


def test_native_rebinding_errors_remain_fail_closed(native_contract, monkeypatch):
    """An invalid native source is not turned into an apparently usable data plan."""
    def reject(config):
        """Model an actual optimizer-range/source incompatibility."""
        raise ValueError('Invalid source binding')
    monkeypatch.setattr(sys.modules['optimization.warmup'], 'refresh_optimizer_source_bindings', reject)
    scenario = SimpleNamespace(coins=None, ignored_coins=None, overrides={}, start_date=None, end_date=None, exchanges=None)
    source = {'_source_epoch': 0, '_bound_epoch': 0,
              'bot': {side: {'universe': {'approved_coins': ['BTC']}} for side in ('long', 'short')},
              'backtest': {'suite_enabled': True, 'exchanges': ['bybit'], 'scenarios': [scenario]}}
    with pytest.raises(ValueError, match='Invalid source binding'):
        _cloud_export_plan(source)


def test_base_export_does_not_require_suite_rebinding_api(native_contract, monkeypatch):
    """Older native runtimes keep their working non-Suite export path."""
    monkeypatch.delattr(sys.modules['optimization.warmup'], 'refresh_optimizer_source_bindings')
    source = {'_source_epoch': 1, '_bound_epoch': 1,
              'bot': {side: {'universe': {'approved_coins': ['BTC'], 'minimum_coin_age_days': 30}} for side in ('long', 'short')},
              'optimize': {'bounds': {'warmup': 120}},
              'backtest': {'suite_enabled': False, 'exchanges': ['bybit']}}
    assert len(_cloud_export_plan(source)['data_plans']) == 1


@pytest.mark.parametrize('scenario_reference', [False, True])
def test_unresolved_source_rejected_before_rebinding(native_contract, scenario_reference):
    """An external selector cannot reach the native refresh or warmup functions."""
    from pb9_config_helper import _HelperError
    source = {'_source_epoch': 1, '_bound_epoch': 1,
              'bot': {side: {'universe': {'approved_coins': ['BTC']}} for side in ('long', 'short')},
              'backtest': {'suite_enabled': scenario_reference, 'exchanges': ['bybit'], 'scenarios': []}}
    if scenario_reference:
        source['backtest']['scenarios'] = [SimpleNamespace(coins=None, ignored_coins=None,
            overrides={'bot.long.universe.approved_coins': 'external.json'}, start_date=None, end_date=None, exchanges=None)]
    else:
        source['bot']['long']['universe']['approved_coins'] = 'external.json'
    with pytest.raises(_HelperError, match='external_reference_rejected'):
        _cloud_export_plan(source)
    assert not any(action in ('refresh', 'warmup') for action, _ in native_contract)
