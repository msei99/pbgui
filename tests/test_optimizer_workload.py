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
    """Both sides share a coin, while distinct exchanges add candle workloads."""
    value = config()
    before = deepcopy(value)
    assert estimate_coin_candles(value) == 11520
    assert value == before


def test_scenarios_and_ignored_coins():
    """Only active training scenarios count, with their own selection and dates."""
    value = config()
    value['backtest'].update(suite_enabled=True, scenarios=[
        {'coins': ['ETH'], 'end_date': '2026-01-01'},
        {'start_date': '2026-01-02', 'ignored_coins': ['SOL']},
    ])
    assert estimate_coin_candles(value) == 5760
    value['backtest']['suite_enabled'] = False
    assert estimate_coin_candles(value) == 11520


@pytest.mark.parametrize('field,value', [('end_date', 'now'), ('end_date', '2025-01-01'),
    ('candle_interval_minutes', 0), ('candle_interval_minutes', float('nan')),
    ('candle_interval_minutes', True)])
def test_unresolved_inputs_are_unknown(field, value):
    """Do not produce plausible numeric values for invalid or relative input."""
    source = config(); source['backtest'][field] = value
    assert estimate_coin_candles(source) is None


def test_scenario_exchange_override_counts_each_selected_venue():
    """Scenario exchanges replace the base list; duplicate names count once."""
    value = config()
    value['live']['approved_coins'] = {'long': ['HYPE'], 'short': ['HYPE']}
    value['backtest'].update(suite_enabled=True, scenarios=[
        {'start_date': '2026-01-01', 'end_date': '2026-01-01'},
        {'start_date': '2026-01-02', 'end_date': '2026-01-02', 'exchanges': ['bybit', 'BYBIT']},
    ])
    assert estimate_coin_candles(value) == 4320


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


def test_six_hype_windows_double_for_bybit_and_hyperliquid():
    """The reported 90-day HYPE suite counts both inherited exchanges."""
    from datetime import date, timedelta

    value = config()
    value['live']['approved_coins'] = {'long': ['HYPE'], 'short': ['HYPE']}
    windows = [
        {'start_date': (date(2025, 1, 6) + timedelta(days=90 * index)).isoformat(),
         'end_date': (date(2025, 1, 6) + timedelta(days=90 * index + 89)).isoformat()}
        for index in range(6)
    ]
    value['backtest'].update(exchanges=['bybit', 'hyperliquid'], suite_enabled=True, scenarios=windows)
    assert estimate_coin_candles(value) == 1_555_200
    value['backtest']['exchanges'] = ['hyperliquid']
    assert estimate_coin_candles(value) == 777_600


def test_six_2026_training_windows_exclude_two_holdouts():
    """Six 32-day windows with four coins on two exchanges yield 2,211,840."""
    from datetime import date, timedelta

    value = config()
    value['live']['approved_coins'] = {'long': ['SOL', 'DOGE', 'ADA', 'BNB'], 'short': []}
    start = date(2026, 1, 6)
    windows = [{'start_date': (start + timedelta(days=32*i)).isoformat(),
                'end_date': (start + timedelta(days=32*i+31)).isoformat()} for i in range(8)]
    value['backtest'].update(start_date='2026-01-01', end_date='2026-09-18',
                            suite_enabled=True, scenarios=[w for i,w in enumerate(windows) if i not in (2,4)])
    value['pbgui'] = {'scenario_template': {'holdout_scenarios': [windows[2], windows[4]]}}
    assert estimate_coin_candles(value) == 2211840
