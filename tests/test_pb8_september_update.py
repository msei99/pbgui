"""Compatibility regressions for PB8 offline simulations and objective-aware drift."""

import asyncio
from contextlib import contextmanager

import pytest

from api import pb8_ohlcv_runtime_helper as helper
from vast_config_validation import validate_cloud_config


@pytest.mark.parametrize('offline', [False, True])
@pytest.mark.parametrize('failure', [False, True])
def test_readiness_scopes_offline_metadata_and_restores_policy(monkeypatch, offline, failure):
    """Inception/market lookups obey the selected policy, even on errors."""
    active = []

    @contextmanager
    def policy(config):
        """Model the task-local PB8 simulation policy."""
        active.append(config['backtest']['offline'])
        try:
            yield
        finally:
            active.pop()

    modules = {'simulation_data_policy': policy}

    async def build(payload, modules=None):
        """Observe the policy at the real planner boundary."""
        assert active == [offline]
        assert modules['simulation_data_policy'] is policy
        if failure:
            raise ValueError('missing offline metadata')
        return {'ok': True}

    monkeypatch.setattr(helper, '_load_modules', lambda _: modules)
    monkeypatch.setattr(helper, '_build', build)
    request = {'config': {'backtest': {'offline': offline}}}
    if failure:
        with pytest.raises(ValueError, match='missing offline metadata'):
            asyncio.run(helper._build_with_data_policy(request))
    else:
        assert asyncio.run(helper._build_with_data_policy(request)) == {'ok': True}
    assert active == []


def test_older_runtime_cannot_silently_ignore_offline(monkeypatch):
    """Reject unsupported offline mode before metadata planning starts."""
    monkeypatch.setattr(helper, '_load_modules', lambda _: {})
    with pytest.raises(ValueError, match='does not support'):
        asyncio.run(helper._build_with_data_policy({'config': {'backtest': {'offline': True}}}))
    with pytest.raises(ValueError, match='must be a boolean'):
        asyncio.run(helper._build_with_data_policy({'config': {'backtest': {'offline': 'false'}}}))


@pytest.mark.parametrize('field,value,rejected', [
    ('offline', True, True), ('offline', False, False),
    ('drift_rank_halt', None, False), ('drift_rank_halt', 0.8, True),
    ('drift_objective_tolerance', 1e-6, False), ('drift_objective_tolerance', 0, True),
    ('drift_objective_tolerance', float('nan'), True),
])
def test_pinned_cloud_worker_rejects_unsupported_new_options(field, value, rejected):
    """New local defaults remain usable; explicit unsupported cloud behavior fails early."""
    config = {
        'live': {'strategy_kind': 'ema_anchor', 'approved_coins': {'long': ['BTC'], 'short': []}},
        'bot': {'long': {}, 'short': {}}, 'backtest': {'exchanges': ['binance']},
        'optimize': {'iters': 512, 'n_cpus': 4, 'gpu': {},
                     'scoring': [{'metric': 'adg_strategy_eq', 'goal': 'max'}]},
    }
    section = config['backtest'] if field == 'offline' else config['optimize']['gpu']
    section[field] = value
    errors = validate_cloud_config(config)
    assert any(error['path'].endswith('.' + field) for error in errors) is rejected
