"""Offline contracts for the uploaded worker's calibration controller."""
from __future__ import annotations

from pathlib import Path
import pytest

from setup.vast_gpu_benchmark import cloud_worker


def test_each_calibration_case_gets_offline_market_and_inception_caches(tmp_path: Path) -> None:
    """PB8's per-population cwd must contain the staged market metadata."""
    import os
    import time

    source = tmp_path / 'output/caches'
    market = source / 'binance/markets.json'
    market.parent.mkdir(parents=True)
    market.write_text('{"BTC/USDT:USDT": {"symbol": "BTC/USDT:USDT"}}')
    stamp = time.time() - 60
    os.utime(market, (stamp, stamp))
    for name in (
        'first_ohlcv_timestamps_unified.json',
        'first_ohlcv_timestamps_unified_exchange_specific.json',
        'first_ohlcv_timestamps_unified_exchange_specific_symbols.json',
        'first_ohlcv_timestamps_unified.version',
    ):
        (source / name).write_text('2')
    case = tmp_path / 'calibration-cases/4096'
    case.mkdir(parents=True)

    cloud_worker.seed_calibration_case_caches(tmp_path / 'output', case, ['binance'])

    copied = case / 'caches/binance/markets.json'
    assert copied.read_text() == market.read_text()
    assert abs(copied.stat().st_mtime - stamp) < .01
    assert (case / 'caches/first_ohlcv_timestamps_unified.version').read_text() == '2'


def test_calibration_refuses_to_start_without_local_market_cache(tmp_path: Path, monkeypatch) -> None:
    """A missing per-case cache fails before PB8 can contact the exchange."""
    import json
    import time
    import pytest

    monkeypatch.setattr(cloud_worker, 'ROOT', tmp_path)
    monkeypatch.setattr(cloud_worker, 'GUARD_ROOT', tmp_path)
    (tmp_path / 'guard.json').write_text(json.dumps({'deadline': time.time() + 3600}))
    plan = {'protocol': 3, 'populations': [4096, 8192], 'sample_seconds': 300,
            'population_step': 4096, 'min_scale_gain': .10,
            'max_population': 131072, 'vram_reserve_ratio': .15}
    config = {'backtest': {'exchanges': ['binance']}}
    with pytest.raises(ValueError, match='caches are not staged'):
        cloud_worker.run_calibration(config, {}, {}, plan)
    assert not (tmp_path / 'calibration-cases/4096/optimizer.log').exists()


def test_worker_reads_only_population_heartbeat(tmp_path: Path) -> None:
    """Starting-config progress cannot prematurely complete a population case."""
    log = tmp_path / 'optimizer.log'
    log.write_text(
        'Optimizer starting configs progress | completed=50/100 pending=8 submitted=58 '
        'elapsed=300.0s rate=0.167/s eta=300.0s\n'
    )
    assert cloud_worker.calibration_progress(log) is None
    log.write_text(
        'Optimizer population progress | completed=100/8192 pending=8 submitted=108 '
        'elapsed=300.0s rate=0.333/s eta=unknown\n'
    )
    assert cloud_worker.calibration_progress(log) == {
        'source': 'heartbeat', 'completed': 100, 'total': 8192,
        'pending': 8, 'submitted': 108, 'elapsed_seconds': 300.0, 'rate_window_seconds': 300.0,
        'rate_per_second': .333, 'valid': True,
    }


def test_worker_uses_completed_gpu_generations_and_exposes_dispatch(tmp_path: Path) -> None:
    """A stale gen-1 summary must not hide newer, fully measured GPU work."""
    import json

    log = tmp_path / 'optimizer.log'
    def profile(generation: int, wall: float, last_batch: int) -> str:
        record = {
            'event': 'generation', 'generation': generation,
            'full_history_candidate_count': 8192,
            'timings_seconds': {'wall': wall, 'proxy_evaluation': wall - 2},
            'proxy_profiles': [{
                'strategy': 'ema_anchor', 'coin_count': 3, 'side_count': 1,
                'dispatch_batch_size': 1186, 'dispatch_count': 7,
                'candidate_bars': 13_808_984_064,
                'actual_dispatch_batch_sizes': [1186] * 6 + [last_batch],
                'timings_seconds': {'kernel_execution': wall - 3},
            }],
        }
        return '[gpu-profile] ' + json.dumps(record)
    log.write_text('\n'.join([
        'GPU optimize | gen=1 proxy=8192 (20.0/s) exact=0 inflight=8',
        'Optimizer population progress | completed=200/8192 pending=8 submitted=208 '
        'elapsed=100.0s rate=2.0/s eta=unknown',
        profile(1, 142.0, 1076), profile(2, 138.0, 1076),
    ]) + '\n')

    result = cloud_worker.calibration_progress(log)

    assert result['source'] == 'generation_profile'
    assert result['rate_per_second'] == round(16384 / 280, 3)
    assert result['generation_count'] == 2
    assert result['rate_window_seconds'] == 280
    assert result['dispatch_count'] == 14
    assert result['dispatch_batch_size'] == 1186
    assert result['last_dispatch_size'] == 1076
    assert result['strategy'] == 'ema_anchor'
    assert result['candidate_bars'] == 13_808_984_064


def test_calibration_dispatch_limit_scales_with_population() -> None:
    """Each canonical probe has room for a full batch despite warmup bars."""
    config = {
        'backtest': {'start_date': '2024-01-01', 'end_date': '2024-12-31',
                     'candle_interval_minutes': 1},
        'live': {'approved_coins': {'long': ['BTC', 'ETH', 'SOL'],
                                    'short': ['BTC', 'ETH', 'SOL']}},
    }
    first = cloud_worker.calibration_dispatch_limit(config, 4096)
    assert first > 4096 * 561889 * 3
    assert cloud_worker.calibration_dispatch_limit(config, 8192) == 2 * first


def test_worker_mirrors_only_new_case_log_bytes(tmp_path: Path) -> None:
    """The visible optimizer log advances during a population without duplicates."""
    case_log = tmp_path / 'case.log'
    visible_log = tmp_path / 'visible.log'
    case_log.write_bytes(b'population start\n')
    with visible_log.open('ab') as destination:
        offset = cloud_worker.mirror_log_delta(case_log, destination, 0)
        with case_log.open('ab') as source:
            source.write(b'GPU proxy dispatch progress 2/7\n')
        offset = cloud_worker.mirror_log_delta(case_log, destination, offset)
        assert cloud_worker.mirror_log_delta(case_log, destination, offset) == offset
    assert visible_log.read_bytes() == b'population start\nGPU proxy dispatch progress 2/7\n'


def test_worker_adaptive_sequence_stops_on_plateau() -> None:
    """Every valid anchor sequence probes 12k and keeps a positive sub-ten-percent gain."""
    cases = {str(population): {'valid': True, 'rate_per_second': rate}
             for population, rate in [(4096, 100), (8192, 105)]}
    assert cloud_worker.calibration_decision(cases)['next_population'] == 12288
    cases['12288'] = {'valid': True, 'rate_per_second': 109}
    assert cloud_worker.calibration_decision(cases)['next_population'] is None
    assert cloud_worker.calibration_decision(cases)['recommended_population'] == 12288


def test_worker_protocol3_skips_sub_4k_probes() -> None:
    """New fixed-workload cases begin at 4k."""
    assert cloud_worker.calibration_decision({})['next_population'] == 4096
    assert cloud_worker.calibration_decision({
        '4096': {'valid': True, 'rate_per_second': 50},
        '8192': {'valid': True, 'rate_per_second': 60},
    })['next_population'] == 12288


def test_worker_continues_above_32k_while_gain_remains_strong() -> None:
    """Large cards advance in fixed 4k steps without the old 32k ceiling."""
    cases = {str(population): {'valid': True, 'rate_per_second': rate}
             for population, rate in [
                 (4096, 40), (8192, 80),
                 (12288, 90), (16384, 102), (20480, 115), (24576, 129),
                 (28672, 144), (32768, 160),
             ]}
    assert cloud_worker.calibration_decision(cases)['next_population'] == 36864


def test_protocol4_worker_decision_matches_half_k_search() -> None:
    """The uploaded worker mirrors PBGui's bounded adaptive decision."""
    plan = {'protocol': 4, 'populations': [5632, 6144], 'sample_seconds': 300,
            'population_step': 512, 'min_scale_gain': .05, 'max_population': 7168,
            'vram_reserve_ratio': .15, 'case_timeout_seconds': 3600}
    assert cloud_worker.configurable_calibration_decision({}, plan)['next_population'] == 5632
    cases = {'5632': {'valid': True, 'rate_per_second': 100},
             '6144': {'valid': True, 'rate_per_second': 103}}
    decision = cloud_worker.configurable_calibration_decision(cases, plan)
    assert decision['recommended_population'] == 6144
    assert decision['stop_reason'] == 'scaling_plateau'


@pytest.mark.parametrize('second_dispatches, expected_full', [(1, True), (2, False)])
def test_protocol4_requires_every_scenario_to_fit_one_batch(
    tmp_path: Path, second_dispatches: int, expected_full: bool
) -> None:
    """One-batch evidence cannot be inferred from only the first scenario."""
    import json
    record = {
        'event': 'generation', 'full_history_candidate_count': 6144,
        'timings_seconds': {'wall': 120, 'proxy_evaluation': 110},
        'proxy_profiles': [
            {'strategy': 'ema_anchor', 'dispatch_batch_size': 6144,
             'dispatch_count': 1, 'candidate_bars': 5_000_000},
            {'strategy': 'ema_anchor', 'dispatch_batch_size': 6144,
             'dispatch_count': second_dispatches, 'candidate_bars': 8_000_000},
        ],
    }
    log = tmp_path / 'optimizer.log'
    log.write_text('[gpu-profile] ' + json.dumps(record) + '\n')
    profile = cloud_worker.calibration_generation_profile(log)
    assert profile['all_dispatches_full'] is expected_full
    assert profile['candidate_bars'] == 8_000_000
    assert profile['scenario_count'] == 2
    assert profile['dispatch_count'] == 1 + second_dispatches
