"""Pure workload estimates use explicit scenario dates and distinct coin lists."""
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
import os

import pytest

from optimizer_workload import estimate_coin_candles, estimate_snapshot


def test_snapshot_estimate_is_shared_and_invalidated_by_replacement(tmp_path, monkeypatch):
    """Concurrent polls parse once; same-size replacements with restored mtime reload."""
    import pb8_config
    root = tmp_path / 'jobs'
    root.mkdir()
    path = root / 'job.json'
    path.write_text('{}')
    calls = []

    def load(path):
        """Record normalized config reads without calling any runtime helper."""
        calls.append(path)
        source = config()
        if len(calls) > 1:
            source['backtest']['end_date'] = '2026-01-03'
        return source

    monkeypatch.setattr(pb8_config, 'load_pb8_config', load)
    with ThreadPoolExecutor(max_workers=8) as pool:
        assert list(pool.map(lambda _: estimate_snapshot(path, root), range(16))) == [11520] * 16
    assert len(calls) == 1
    before = path.stat()
    replacement = root / 'replacement.json'
    replacement.write_text('[]')
    os.utime(replacement, ns=(before.st_atime_ns, before.st_mtime_ns))
    replacement.replace(path)
    assert estimate_snapshot(path, root) == 17280
    assert len(calls) == 2
    path.unlink()
    assert estimate_snapshot(path, root) is None


def test_cached_snapshot_still_checks_containment_and_symlinks(tmp_path, monkeypatch):
    """A cached estimate cannot bypass a different root or a symlink selector."""
    import pb8_config
    root = tmp_path / 'jobs'
    root.mkdir()
    path = root / 'job.json'
    path.write_text('{}')
    monkeypatch.setattr(pb8_config, 'load_pb8_config', lambda _: config())
    assert estimate_snapshot(path, root) == 11520
    assert estimate_snapshot(path, tmp_path / 'other-root') is None
    link = root / 'link.json'
    link.symlink_to(path)
    assert estimate_snapshot(link, root) is None


def test_snapshot_parse_failure_can_recover_without_file_change(tmp_path, monkeypatch):
    """Transient helper errors must not cache an unknown estimate indefinitely."""
    import pb8_config
    path = tmp_path / 'job.json'
    path.write_text('{}')

    def unavailable(_):
        """Simulate a transient helper startup failure."""
        raise RuntimeError('helper unavailable')

    monkeypatch.setattr(pb8_config, 'load_pb8_config', unavailable)
    assert estimate_snapshot(path, tmp_path) is None
    monkeypatch.setattr(pb8_config, 'load_pb8_config', lambda _: config())
    assert estimate_snapshot(path, tmp_path) == 11520


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
