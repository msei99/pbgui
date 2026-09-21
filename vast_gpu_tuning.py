"""Resolve safe PB8 GPU defaults from rented hardware and frozen workload."""

from __future__ import annotations

from datetime import date
import hashlib
import json
import math


def _positive_number(value):
    """Return finite positive numeric input without accepting booleans."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) and number > 0 else None


def _active_parameter_count(value) -> int:
    """Count non-fixed numeric bound pairs in flat or nested bounds."""
    if isinstance(value, dict):
        return sum(_active_parameter_count(item) for item in value.values())
    if (isinstance(value, list) and len(value) >= 2
            and all(isinstance(item, (int, float)) and not isinstance(item, bool) for item in value[:2])):
        return int(value[0] != value[1])
    return 0


def _side_active(config: dict, side: str) -> bool:
    """Resolve disabled strategy sides conservatively from PB8 risk settings."""
    approved = config.get('live', {}).get('approved_coins', {}).get(side, [])
    if not isinstance(approved, list) or not approved:
        return False
    risk = config.get('bot', {}).get(side, {}).get('risk', {})
    for key in ('n_positions', 'total_wallet_exposure_limit'):
        value = risk.get(key)
        if isinstance(value, (int, float)) and not isinstance(value, bool) and value <= 0:
            return False
    return True


def _largest_candidate_bars(config: dict) -> int | None:
    """Estimate the largest active scenario's coin-side minute-bar workload."""
    try:
        backtest = config['backtest']
        interval = _positive_number(backtest.get('candle_interval_minutes', 1))
        if interval is None:
            return None
        approved = config['live']['approved_coins']
        ignored = config['live'].get('ignored_coins') or {}
        active_sides = [side for side in ('long', 'short') if _side_active(config, side)]
        if not active_sides:
            return None
        scenarios = backtest.get('scenarios') if backtest.get('suite_enabled') else [{}]
        if not isinstance(scenarios, list) or not scenarios:
            return None
        largest = 0
        for scenario in scenarios:
            if not isinstance(scenario, dict):
                return None
            selected = scenario.get('coins')
            if selected is not None and (not isinstance(selected, list) or not selected):
                return None
            scenario_ignored = scenario.get('ignored_coins') or []
            if not isinstance(scenario_ignored, list):
                return None
            ignored_set = {str(item).strip().upper() for item in scenario_ignored}
            side_work = 0
            for side in active_sides:
                values = selected if selected is not None else approved.get(side, [])
                if not isinstance(values, list):
                    return None
                base_ignored = ignored.get(side, [])
                if not isinstance(base_ignored, list):
                    return None
                excluded = ignored_set | {str(item).strip().upper() for item in base_ignored}
                coins = {str(item).strip().upper() for item in values if str(item).strip()} - excluded
                side_work += len(coins)
            start = date.fromisoformat(scenario.get('start_date') or backtest['start_date'])
            end = date.fromisoformat(scenario.get('end_date') or backtest['end_date'])
            days = (end - start).days + 1
            if days <= 0 or side_work <= 0:
                return None
            largest = max(largest, math.ceil(days * 1440 / interval) * side_work)
        return largest or None
    except (KeyError, TypeError, ValueError, OverflowError):
        return None


def _cuda_population(
    vram_gb: float,
    memory_bandwidth_gbps: float | None = None,
    tflops: float | None = None,
) -> int:
    """Select a measured width only when capacity and throughput both qualify."""
    if vram_gb <= 8:
        return 2048
    if (
        vram_gb >= 20
        and memory_bandwidth_gbps is not None
        and memory_bandwidth_gbps >= 600
        and tflops is not None
        and tflops >= 25
    ):
        return 24576
    if (
        vram_gb >= 15
        and memory_bandwidth_gbps is not None
        and memory_bandwidth_gbps >= 500
        and tflops is not None
        and tflops >= 25
    ):
        return 16384
    if vram_gb >= 11.5:
        return 8192
    return 4096


def _dispatch_limit(
    vram_gb: float,
    candidate_bars: int | None,
    target_candidates: int | None = None,
    memory_bandwidth_gbps: float | None = None,
    tflops: float | None = None,
) -> int:
    """Keep one CUDA generation wide enough to occupy the rented GPU."""
    if vram_gb <= 8:
        floor = 500_000_000
    elif vram_gb <= 12:
        floor = 1_000_000_000
    elif vram_gb <= 16:
        floor = 1_500_000_000
    elif vram_gb <= 24:
        floor = 2_000_000_000
    elif vram_gb <= 32:
        floor = 3_000_000_000
    else:
        floor = 4_000_000_000
    if candidate_bars is None:
        return floor
    if target_candidates is None:
        target_candidates = _cuda_population(vram_gb)
    # PB8 launches one CUDA thread per candidate. Its EMA-anchor multicoin
    # kernel uses 32-thread blocks, so narrowing an automatic generation
    # to roughly one hundred candidates leaves most SMs without a resident
    # block even though nvidia-smi reports the long-running kernel as 100% busy.
    # Keep the full generation in one candidate dispatch. PB8 owns temporal
    # replay chunking where a strategy supports it; candidate dispatch sizing
    # must not be reused as a UI progress interval.
    return max(floor, int(candidate_bars) * int(target_candidates))


def resolve_gpu_settings(
    config: dict,
    state: dict,
    offer: dict,
) -> dict:
    """Fill only automatic GPU fields and return an auditable tuning record."""
    optimize = config.setdefault('optimize', {})
    gpu = optimize.setdefault('gpu', {})
    hardware = state.get('hardware') if isinstance(state.get('hardware'), dict) else {}
    actual_bytes = _positive_number(hardware.get('vram_bytes'))
    vram_gb = actual_bytes / 1024**3 if actual_bytes is not None else _positive_number(offer.get('vram_gb'))
    memory_bandwidth_gbps = _positive_number(offer.get('gpu_mem_bw_gbps'))
    tflops = _positive_number(offer.get('tflops'))
    candidate_bars = _largest_candidate_bars(config)
    parameters = _active_parameter_count(optimize.get('bounds', {}))
    automatic = {}
    preserved = {}

    profile_population = (
        _cuda_population(vram_gb, memory_bandwidth_gbps, tflops)
        if vram_gb is not None else None
    )
    profile_scale = profile_population // 1024 if profile_population is not None else None
    profile_window = 128 * profile_scale if profile_scale is not None else None
    iterations = optimize.get('iters')
    auto_profile = (
        vram_gb is not None
        and isinstance(iterations, int)
        and not isinstance(iterations, bool)
        and iterations >= profile_window
        and gpu.get('auto_lean_parallelism', True) is not False
        and optimize.get('population_size') is None
        and gpu.get('population_size') is None
        and gpu.get('batch_size') is None
        and gpu.get('max_dispatch_candidate_bars') is None
        and int(gpu.get('validate_per_generation') or 8) == 8
        and int(gpu.get('drift_probes') if gpu.get('drift_probes') is not None else 4) == 4
        and int(gpu.get('drift_window') or 128) == 128
    )
    if auto_profile:
        population = profile_population
        scale = profile_scale
        automatic.update(
            population_size=population,
            batch_size=population,
            validate_per_generation=8 * scale,
            drift_probes=4 * scale,
            drift_window=128 * scale,
        )
        if candidate_bars is not None:
            automatic['max_dispatch_candidate_bars'] = _dispatch_limit(
                vram_gb, candidate_bars, population,
                memory_bandwidth_gbps, tflops,
            )
        gpu.update(automatic)

    if gpu.get('max_dispatch_candidate_bars') is None and vram_gb is not None:
        configured_population = optimize.get('population_size') or gpu.get('population_size')
        target_candidates = (
            int(configured_population)
            if isinstance(configured_population, int) and configured_population > 0
            else 1024
        )
        automatic['max_dispatch_candidate_bars'] = _dispatch_limit(
            vram_gb, candidate_bars, target_candidates,
            memory_bandwidth_gbps, tflops,
        )
        gpu['max_dispatch_candidate_bars'] = automatic['max_dispatch_candidate_bars']
    elif ('max_dispatch_candidate_bars' not in automatic
          and gpu.get('max_dispatch_candidate_bars') is not None):
        preserved['max_dispatch_candidate_bars'] = gpu['max_dispatch_candidate_bars']

    if not auto_profile:
        population = optimize.get('population_size')
        if population is None:
            population = gpu.get('population_size')
        if population is not None:
            preserved['population_size'] = population

    population = optimize.get('population_size') or gpu.get('population_size')
    dispatch_limit = gpu.get('max_dispatch_candidate_bars')
    dispatch_candidates = (
        max(1, min(int(population or 1024), int(dispatch_limit) // candidate_bars))
        if candidate_bars and isinstance(dispatch_limit, int) and dispatch_limit > 0 else None
    )
    dispatch_chunks = (
        math.ceil(int(population) / dispatch_candidates)
        if isinstance(population, int) and population > 0 and dispatch_candidates else None
    )
    profile = {
        'schema_version': 1,
        'gpu_name': hardware.get('gpu') or offer.get('gpu_name'),
        'vram_gb': round(vram_gb, 2) if vram_gb is not None else None,
        'memory_bandwidth_gbps': memory_bandwidth_gbps,
        'tflops': tflops,
        'applied_by': 'pbgui',
        'cpu_workers': state.get('workers'),
        'largest_candidate_bars': candidate_bars,
        'active_parameters': parameters,
        'dispatch_candidates': dispatch_candidates,
        'dispatch_chunks': dispatch_chunks,
        'automatic': automatic,
        'preserved': preserved,
    }
    encoded = json.dumps(profile, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
    profile['fingerprint'] = hashlib.sha256(encoded).hexdigest()
    return profile
