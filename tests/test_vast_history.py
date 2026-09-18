"""Offline date-envelope, warmup and daily-shard regression coverage."""

import copy
import json
import sys
from types import ModuleType

import pytest

from vast_history import history_window
from vast_scenarios import scenario_plan
from setup.vast_gpu_benchmark.prepare import select_shards


@pytest.fixture
def config():
    """A date-bounded job whose runtime boundary is mocked separately."""
    return {'backtest': {'start_date': '2026-01-01', 'end_date': '2026-09-18',
                         'exchanges': ['binance']},
            'live': {'approved_coins': {'long': ['BTC'], 'short': []}}}


@pytest.fixture
def warmup(monkeypatch):
    """Record the complete configs passed to the isolated native helper."""
    values, seen = [12096], []

    def helper(operation, **payload):
        """Return a configurable native warmup response without local PB8 access."""
        assert operation == 'optimizer_warmup'
        seen.extend(payload['configs'])
        return {'minutes': values * len(payload['configs']) if len(values) == 1 else values}

    monkeypatch.setattr('pb8_config._call_helper', helper)
    return values, seen


def test_window_rounds_warmup_outward_and_preserves_config(config, warmup):
    """8.4 days requires nine complete daily shards before January 1."""
    before = copy.deepcopy(config)
    contexts, _, _ = scenario_plan(config)
    assert history_window(config, contexts) == ('2025-12-23', '2026-09-18')
    assert config == before
    assert warmup[1] == [config, config]


def test_active_suite_envelope_includes_base_and_scenario_overrides(config, warmup):
    """Match native shared preload rather than dropping gaps or base dates."""
    config['backtest'].update(suite_enabled=True, scenarios=[
        {'label': 'earlier', 'start_date': '2025-12-01', 'end_date': '2025-12-10'},
        {'label': 'later', 'start_date': '2026-10-01', 'end_date': '2026-10-31'}])
    warmup[0][:] = [0, 1440, 2881]
    contexts, _, errors = scenario_plan(config)
    assert not errors
    assert history_window(config, contexts) == ('2025-11-28', '2026-10-31')
    assert warmup[1][1]['backtest']['start_date'] == '2025-12-01'


def test_disabled_scenarios_do_not_expand_dates(config, warmup):
    """Draft scenario dates must not reintroduce historical exports."""
    config['backtest'].update(suite_enabled=False, scenarios=[
        {'label': 'draft', 'start_date': '2019-01-01', 'end_date': '2030-01-01'}])
    contexts, _, _ = scenario_plan(config)
    assert history_window(config, contexts) == ('2025-12-23', '2026-09-18')


@pytest.mark.parametrize('value', [None, -1, True, 1.5, '12'])
def test_invalid_native_warmup_never_exports_all_history(config, warmup, value):
    """Fail closed instead of guessing or silently using every local file."""
    warmup[0][:] = [value]
    with pytest.raises(ValueError, match='warmup'):
        history_window(config, [])


@pytest.mark.parametrize('start,end', [('2026-02-30', '2026-03-01'),
                                     ('2026-09-19', '2026-09-18'), (None, '2026-09-18')])
def test_invalid_dates_fail_before_runtime_call(config, warmup, start, end):
    """Invalid dates cannot widen the export or trigger unnecessary runtime work."""
    config['backtest'].update(start_date=start, end_date=end)
    with pytest.raises(ValueError, match='date'):
        history_window(config, [])
    assert not warmup[1]


def test_export_keeps_only_requested_days_and_warmup(config, warmup, tmp_path):
    """Actual manifest candidates exclude years of history and post-end files."""
    mapping = tmp_path / 'mapping/binance'
    mapping.mkdir(parents=True)
    (mapping / 'mapping.json').write_text(json.dumps([dict(
        coin='BTC', quote='USDT', swap=True, linear=True, ccxt_symbol='BTC/USDT:USDT')]))
    folder = tmp_path / 'raw/binanceusdm/1m/BTC_USDT:USDT'
    folder.mkdir(parents=True)
    dates = ['2019-01-01', '2025-12-22', '2025-12-23', '2026-01-01', '2026-09-18', '2026-09-19']
    for day in dates:
        (folder / (day + '.npz')).write_bytes(b'candles')
    selected = select_shards(config, tmp_path / 'raw', tmp_path / 'mapping')
    assert [path.stem for _, path in selected] == ['2025-12-23', '2026-01-01', '2026-09-18']
    for _, path in selected:
        (folder / path.name).unlink()
    with pytest.raises(ValueError, match='including warmup'):
        select_shards(config, tmp_path / 'raw', tmp_path / 'mapping')


def test_native_helper_combines_optimizer_and_preload_warmup(monkeypatch):
    """Delegate to native rules, including the uncapped zero-cap configuration."""
    from pb8_config_helper import _optimizer_warmup

    seen = []
    optimizer = ModuleType('optimization.warmup')
    preload = ModuleType('warmup_utils')
    optimizer.compute_optimizer_backtest_warmup_minutes = lambda c: seen.append(c) or 12096
    preload.compute_backtest_warmup_minutes = lambda c: 15000
    monkeypatch.setitem(sys.modules, 'optimization.warmup', optimizer)
    monkeypatch.setitem(sys.modules, 'warmup_utils', preload)
    c = {'live': {'max_warmup_minutes': 0}, 'optimize': {'bounds': {'example': [1, 10]}}}
    assert _optimizer_warmup([c]) == {'minutes': [15000]}
    assert seen == [c]
