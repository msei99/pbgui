"""Pure workload estimates use explicit scenario dates and distinct coin lists."""
from copy import deepcopy

import pytest

from optimizer_workload import estimate_coin_candles, estimate_snapshot


def config():
    """Two coins across both sides with one inclusive two-day base window."""
    return {'live': {'approved_coins': {'long': ['ETH', 'SOL'], 'short': ['ETH']}},
            'backtest': {'start_date': '2026-01-01', 'end_date': '2026-01-02',
                         'candle_interval_minutes': 1, 'exchanges': ['binance', 'bybit']}}


def test_distinct_coins_and_inclusive_days():
    """Neither both sides nor multiple exchanges multiply the data proxy."""
    value = config()
    before = deepcopy(value)
    assert estimate_coin_candles(value) == 5760
    assert value == before


def test_scenarios_and_ignored_coins():
    """Only active training scenarios count, with their own selection and dates."""
    value = config()
    value['backtest'].update(suite_enabled=True, scenarios=[
        {'coins': ['ETH'], 'end_date': '2026-01-01'},
        {'start_date': '2026-01-02', 'ignored_coins': ['SOL']},
    ])
    assert estimate_coin_candles(value) == 2880
    value['backtest']['suite_enabled'] = False
    assert estimate_coin_candles(value) == 5760


@pytest.mark.parametrize('field,value', [('end_date', 'now'), ('end_date', '2025-01-01'),
    ('candle_interval_minutes', 0), ('candle_interval_minutes', float('nan')),
    ('candle_interval_minutes', True)])
def test_unresolved_inputs_are_unknown(field, value):
    """Do not produce plausible numeric values for invalid or relative input."""
    source = config(); source['backtest'][field] = value
    assert estimate_coin_candles(source) is None


def test_unknown_coins_and_unsafe_overrides():
    """Dynamic selections and unresolved data overrides stay unknown."""
    source = config(); source['live']['approved_coins'] = {'long': [], 'short': []}
    assert estimate_coin_candles(source) is None
    source = config(); source['backtest'].update(suite_enabled=True, scenarios=[{'overrides': {'backtest.end_date':'2028-01-01'}}])
    assert estimate_coin_candles(source) is None


def test_snapshot_missing_and_escape(tmp_path):
    """A missing snapshot or an escaped symlink cannot supply an estimate."""
    root = tmp_path / 'jobs'; root.mkdir()
    assert estimate_snapshot(root / 'missing.json', root) is None
    outside = tmp_path / 'outside.json'; outside.write_text('{}')
    (root / 'config.json').symlink_to(outside)
    assert estimate_snapshot(root / 'config.json', root) is None


def test_six_2026_training_windows_exclude_two_holdouts():
    """Six 32-day windows with four coins produce 1,105,920 candidate coin candles."""
    from datetime import date, timedelta

    value = config()
    value['live']['approved_coins'] = {'long': ['SOL', 'DOGE', 'ADA', 'BNB'], 'short': []}
    start = date(2026, 1, 6)
    windows = [{'start_date': (start + timedelta(days=32*i)).isoformat(),
                'end_date': (start + timedelta(days=32*i+31)).isoformat()} for i in range(8)]
    value['backtest'].update(start_date='2026-01-01', end_date='2026-09-18',
                            suite_enabled=True, scenarios=[w for i,w in enumerate(windows) if i not in (2,4)])
    value['pbgui'] = {'scenario_template': {'holdout_scenarios': [windows[2], windows[4]]}}
    assert estimate_coin_candles(value) == 1105920
