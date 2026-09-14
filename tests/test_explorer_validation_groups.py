"""Offline recovery of Explorer validation groups without PB8 GUI metadata."""
import copy

from api.backtest_v8 import _derived_optimize_result_group


def test_explorer_periods_group_by_configuration_not_name():
    """Different periods match, different strategies never share a derived group."""
    config = {'bot': {'long': {'entry': 1}}, 'backtest': {'start_date':'2020-01-01','end_date':'2021-01-01','base_dir':'one'}}
    groups = []
    for item in ['train_01_20200216_400d','holdout_01_20250808_400d','full_timerange']:
        candidate = copy.deepcopy(config)
        candidate['backtest']['base_dir'] = item
        candidate['backtest']['start_date'] = '2025-01-01'
        groups.append(_derived_optimize_result_group(candidate, ('backtests_' + item,'combined','stamp','analysis.json')))
    assert len({g['id'] for g in groups}) == 1
    assert len({g['item'] for g in groups}) == 3
    other = copy.deepcopy(config)
    other['bot']['long']['entry'] = 2
    assert _derived_optimize_result_group(other, ('backtests_full_timerange',))['id'] != groups[0]['id']
    assert _derived_optimize_result_group(config, ('backtests',)) is None
    assert _derived_optimize_result_group(config, ('unrelated_full_timerange',)) is None
    assert _derived_optimize_result_group(config, ('pareto_config_217_full_timerange',))['label'] == 'Explorer candidate #217'
