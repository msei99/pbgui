"""Offline regression coverage for suite side-list alignment."""
import copy

import pytest
from fastapi import HTTPException
from api import backtest_v8


@pytest.mark.parametrize('inactive', ['long', 'short'])
@pytest.mark.parametrize('zero_key', ['n_positions', 'total_wallet_exposure_limit'])
def test_disabled_side_inherits_lists_without_enabling_trading(inactive, zero_key):
    """Only the inactive side changes; the caller's config remains untouched."""
    active = 'short' if inactive == 'long' else 'long'
    config = {
        'backtest': {'suite_enabled': True, 'scenarios': [{'label': '2021'}]},
        'bot': {inactive: {'risk': {zero_key: 0}}, active: {'risk': {'n_positions': 1, 'total_wallet_exposure_limit': 1}}},
        'live': {'approved_coins': {active: ['BTC'], inactive: []},
                 'ignored_coins': {active: ['DOGE'], inactive: []}},
    }
    original = copy.deepcopy(config)
    result = backtest_v8._normalize_config(config, 'demo')
    assert config == original
    assert result['bot'] == original['bot']
    assert result['backtest']['scenarios'] == original['backtest']['scenarios']
    for field in ('approved_coins', 'ignored_coins'):
        assert result['live'][field][inactive] == result['live'][field][active]


def test_two_active_sides_reject_asymmetry():
    """Never silently widen or reduce either active trading universe."""
    config = {'backtest': {'suite_enabled': True},
              'bot': {s: {'risk': {'n_positions': 1, 'total_wallet_exposure_limit': 1}} for s in ('long', 'short')},
              'live': {'approved_coins': {'long': ['BTC'], 'short': ['ETH']}}}
    with pytest.raises(HTTPException, match='identical') as exc:
        backtest_v8._normalize_config(config, 'demo')
    assert exc.value.status_code == 422


def test_non_suite_keeps_asymmetric_lists():
    """Ordinary single backtests still support independent side lists."""
    config = {'backtest': {'suite_enabled': False}, 'live': {'approved_coins': {'long': ['BTC'], 'short': []}}}
    assert backtest_v8._normalize_config(config, 'demo')['live'] == config['live']
