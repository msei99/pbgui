"""Offline tests for rental-aware GPU execution defaults."""

from copy import deepcopy
import math

import pytest

from vast_gpu_tuning import resolve_gpu_settings


def workload(*, population=None, dispatch=None):
    """Return a large one-sided workload resembling a broad suite scenario."""
    coins = ['C' + str(index) for index in range(41)]
    return {
        'live': {'approved_coins': {'long': coins, 'short': coins},
                 'ignored_coins': {'long': [], 'short': []}},
        'bot': {
            'long': {'risk': {'n_positions': 5, 'total_wallet_exposure_limit': 1}},
            'short': {'risk': {'n_positions': 0, 'total_wallet_exposure_limit': 0}},
        },
        'backtest': {
            'start_date': '2026-01-01', 'end_date': '2026-09-18',
            'candle_interval_minutes': 1, 'suite_enabled': False,
        },
        'optimize': {
            'iters': 20_000,
            'bounds': {'p' + str(index): [0, 1] for index in range(54)},
            'gpu': {'population_size': population, 'max_dispatch_candidate_bars': dispatch},
        },
    }


@pytest.mark.parametrize(('vram_gb', 'target_candidates', 'dispatch_floor'), [
    (8, 2048, 500_000_000), (12, 8192, 1_000_000_000),
    (16, 8192, 1_500_000_000), (24, 8192, 2_000_000_000),
    (32, 8192, 3_000_000_000), (48, 8192, 4_000_000_000),
])
def test_rented_vram_resolves_bounded_dispatch(
    vram_gb, target_candidates, dispatch_floor
):
    """Unknown cards keep every automatic candidate in the CUDA dispatch."""
    config = workload()
    result = resolve_gpu_settings(config, {'workers': 23, 'hardware': {
        'gpu': 'arbitrary card', 'vram_bytes': vram_gb * 1024**3}}, {'vram_gb': 99})
    dispatch_candidates = target_candidates
    dispatch = max(
        dispatch_floor,
        result['largest_candidate_bars'] * dispatch_candidates,
    )
    assert config['optimize']['gpu']['max_dispatch_candidate_bars'] == dispatch
    assert result['automatic']['max_dispatch_candidate_bars'] == dispatch
    scale = target_candidates // 1024
    assert result['automatic']['population_size'] == target_candidates
    assert result['automatic']['batch_size'] == target_candidates
    assert result['automatic']['validate_per_generation'] == 8 * scale
    assert result['automatic']['drift_probes'] == 4 * scale
    assert result['automatic']['drift_window'] == 128 * scale
    assert result['dispatch_candidates'] == dispatch_candidates
    assert result['dispatch_chunks'] == 1
    assert result['vram_gb'] == vram_gb


def test_large_workload_scales_population_and_exact_evidence_together():
    """A broad CUDA scenario preserves the proxy-to-exact evidence density."""
    config = workload()
    original = deepcopy(config)
    result = resolve_gpu_settings(config, {'workers': 23, 'hardware': {
        'gpu': 'unknown 12 GB card', 'vram_bytes': 12 * 1024**3}}, {'gpu_mem_bw_gbps': 315})
    assert config['optimize']['gpu']['population_size'] == 8192
    assert config['optimize']['gpu']['batch_size'] == 8192
    assert config['optimize']['gpu']['validate_per_generation'] == 64
    assert config['optimize']['gpu']['drift_probes'] == 32
    assert config['optimize']['gpu']['drift_window'] == 1024
    assert result['largest_candidate_bars'] == 261 * 1440 * 41
    assert result['active_parameters'] == 54
    assert result['memory_bandwidth_gbps'] == 315
    assert result['dispatch_candidates'] == 8192
    assert result['dispatch_chunks'] == 1
    assert original['optimize']['gpu']['population_size'] is None


@pytest.mark.parametrize(('name', 'vram_gb', 'bandwidth', 'tflops', 'population'), [
    ('RTX 3060', 12, 318, 12.3, 8192),
    ('RTX 4060 Ti', 16, 236, 21.6, 8192),
    ('RTX 3090', 24, 805, 35.3, 24576),
    ('fast 16 GB card', 16, 672, 44.0, 16384),
    ('large card with unknown compute', 24, 805, None, 8192),
])
def test_cuda_profile_requires_balanced_fast_hardware(
    name, vram_gb, bandwidth, tflops, population
):
    """Wider profiles require enough VRAM, bandwidth, and measured compute."""
    config = workload()
    offer = {'gpu_name': name, 'vram_gb': vram_gb,
             'gpu_mem_bw_gbps': bandwidth, 'tflops': tflops}
    result = resolve_gpu_settings(config, {'workers': 16}, offer)
    scale = population // 1024
    assert result['automatic']['population_size'] == population
    assert result['automatic']['validate_per_generation'] == 8 * scale
    assert result['automatic']['drift_probes'] == 4 * scale
    assert result['automatic']['drift_window'] == 128 * scale
    assert result['tflops'] == tflops


def test_explicit_gpu_values_are_preserved():
    """User-entered dispatch and population values always win over automatic tuning."""
    config = workload(population=384, dispatch=1_234_567_890)
    result = resolve_gpu_settings(config, {'workers': 28, 'hardware': {
        'gpu': 'RTX 3090', 'vram_bytes': 24 * 1024**3}}, {'gpu_mem_bw_gbps': 805})
    assert config['optimize']['gpu']['population_size'] == 384
    assert config['optimize']['gpu']['max_dispatch_candidate_bars'] == 1_234_567_890
    assert result['automatic'] == {}
    assert result['preserved'] == {
        'max_dispatch_candidate_bars': 1_234_567_890, 'population_size': 384}


def test_8gb_workload_uses_smaller_coordinated_cuda_profile():
    """An 8 GB rental keeps the same proxy-to-exact evidence density at 2048."""
    config = workload()
    config['live']['approved_coins'] = {'long': ['BTC'], 'short': []}
    config['backtest']['end_date'] = '2026-01-07'
    result = resolve_gpu_settings(config, {'workers': 8, 'hardware': {
        'gpu': 'small', 'vram_bytes': 8 * 1024**3}}, {})
    assert config['optimize']['gpu']['population_size'] == 2048
    assert result['automatic']['population_size'] == 2048
    assert result['automatic']['validate_per_generation'] == 16
    assert result['automatic']['drift_probes'] == 8
    assert result['automatic']['drift_window'] == 256


def test_short_run_keeps_default_evidence_window():
    """Automatic sizing does not create a drift window longer than the run."""
    config = workload()
    config['optimize']['iters'] = 256
    result = resolve_gpu_settings(config, {'workers': 8, 'hardware': {
        'gpu': 'RTX 3060', 'vram_bytes': 12 * 1024**3}}, {})
    assert 'population_size' not in result['automatic']
    assert config['optimize']['gpu']['population_size'] is None


def test_pbgui_resolves_standard_cuda_profile_for_new_worker():
    """PBGui keeps a measured RTX 3060 dispatch at its full standard width."""
    config = workload()
    result = resolve_gpu_settings(config, {'workers': 23, 'hardware': {
        'gpu': 'RTX 3060', 'vram_bytes': 12 * 1024**3}}, {
        'gpu_mem_bw_gbps': 315, 'tflops': 12.7,
    })
    assert result['automatic']['population_size'] == 8192
    assert result['applied_by'] == 'pbgui'
    assert config['optimize']['gpu']['population_size'] == 8192
    assert config['optimize']['gpu']['batch_size'] == 8192
    assert config['optimize']['gpu']['max_dispatch_candidate_bars'] == (
        result['largest_candidate_bars'] * 8192
    )
    assert result['dispatch_chunks'] == 1


def test_fast_cuda_profile_is_hardware_resolved_by_pbgui():
    """Measured RTX 3090 hardware selects the verified 24576-wide profile."""
    config = workload()
    result = resolve_gpu_settings(config, {'workers': 23}, {
        'gpu_name': 'RTX 3090', 'vram_gb': 24,
        'gpu_mem_bw_gbps': 805, 'tflops': 35.3,
    })
    assert result['automatic']['population_size'] == 24576
    assert result['applied_by'] == 'pbgui'
    assert config['optimize']['gpu']['population_size'] == 24576

