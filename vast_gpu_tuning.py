"""Resolve safe PB8 GPU defaults from rented hardware and frozen workload."""

from __future__ import annotations

from datetime import date
import hashlib
import json
import math
from pathlib import Path


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




def rental_card_profile(offer: dict, state: dict | None = None,
                        profile_root: Path | None = None) -> dict:
    """Preview one card-wide profile, preliminary before rental and verified when possible."""
    from vast_calibration import (CalibrationProfiles, COMPATIBLE_CALIBRATION_IMAGES,
                                  SUPPORTED_PROFILE_PROTOCOLS, gpu_variant_identity)
    from vast_jobs import IMAGE, REVISION

    state = state if isinstance(state, dict) else {}
    hardware = state.get('hardware') if isinstance(state.get('hardware'), dict) else {}
    metrics = state.get('runtime_metrics') if isinstance(state.get('runtime_metrics'), dict) else {}
    actual_bytes = _positive_number(hardware.get('vram_bytes'))
    vram_gb = actual_bytes / 1024**3 if actual_bytes is not None else _positive_number(offer.get('vram_gb'))
    if vram_gb is None:
        raise ValueError('GPU VRAM is unavailable')
    power = _positive_number(metrics.get('gpu_power_limit_watts'))
    runtime_identity = dict(hardware)
    if power is not None:
        runtime_identity['gpu_power_limit_watts'] = power
    identity = gpu_variant_identity(offer, runtime_identity)
    match = None
    try:
        profiles = CalibrationProfiles(profile_root or Path(__file__).resolve().parent / 'data/vast')
        match = (profiles.match(identity) if identity['runtime_verified']
                 else profiles.preliminary_match(offer))
        if not (match and match.get('protocol') in SUPPORTED_PROFILE_PROTOCOLS
                and IMAGE in COMPATIBLE_CALIBRATION_IMAGES
                and match.get('worker_image') in COMPATIBLE_CALIBRATION_IMAGES
                and match.get('pb8_revision') == REVISION):
            match = None
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        match = None
    bandwidth = _positive_number(offer.get('gpu_mem_bw_gbps'))
    tflops = _positive_number(offer.get('tflops'))
    population = int(match['population_size']) if match else _cuda_population(vram_gb, bandwidth, tflops)
    measured_limit = match.get('max_dispatch_candidate_bars') if match else None
    has_measured_limit = type(measured_limit) is int and measured_limit > 0
    return {
        'population_size': population,
        'batch_size': population,
        'max_dispatch_candidate_bars': (measured_limit if has_measured_limit
                                        else _dispatch_limit(vram_gb, None)),
        'source': match.get('source') if match else 'hardware_fallback',
        'match_type': match.get('match_type') if match else None,
        'profile_power_limit_watts': ((match.get('hardware_identity') or {}).get('gpu_power_limit_watts')
                                      if match else None),
        'card_power_limit_watts': power or _positive_number(offer.get('gpu_max_power_watts')),
        'runtime_verified': bool(identity['runtime_verified'] and power is not None),
        'work_limit_is_floor': not has_measured_limit,
    }


def _validated_rental_gpu_profile(value: dict | None) -> dict | None:
    """Reject malformed persisted rental overrides before preparing execution input."""
    if value is None:
        return None
    if not isinstance(value, dict) or set(value) != {
        'population_size', 'batch_size', 'max_dispatch_candidate_bars',
    }:
        raise ValueError('Invalid rental GPU profile')
    population = value['population_size']
    batch = value['batch_size']
    limit = value['max_dispatch_candidate_bars']
    if (type(population) is not int or not 1024 <= population <= 131072
            or type(batch) is not int or not 1 <= batch <= population
            or type(limit) is not int or not 1 <= limit <= 1_000_000_000_000):
        raise ValueError('Invalid rental GPU profile')
    return dict(value)


def resolve_gpu_settings(
    config: dict,
    state: dict,
    offer: dict,
    rental_gpu_profile: dict | None = None,
) -> dict:
    """Fill only automatic GPU fields and return an auditable tuning record."""
    optimize = config.setdefault('optimize', {})
    gpu = optimize.setdefault('gpu', {})
    hardware = state.get('hardware') if isinstance(state.get('hardware'), dict) else {}
    measured_power_limit = _positive_number((state.get('runtime_metrics') or {}).get('gpu_power_limit_watts'))
    actual_bytes = _positive_number(hardware.get('vram_bytes'))
    vram_gb = actual_bytes / 1024**3 if actual_bytes is not None else _positive_number(offer.get('vram_gb'))
    memory_bandwidth_gbps = _positive_number(offer.get('gpu_mem_bw_gbps'))
    tflops = _positive_number(offer.get('tflops'))
    candidate_bars = _largest_candidate_bars(config)
    parameters = _active_parameter_count(optimize.get('bounds', {}))
    automatic = {}
    preserved = {}
    rental_override = _validated_rental_gpu_profile(rental_gpu_profile)

    calibrated_profile = None
    try:
        from vast_calibration import (CalibrationProfiles, SUPPORTED_PROFILE_PROTOCOLS,
                                      COMPATIBLE_CALIBRATION_IMAGES, gpu_variant_identity)
        from vast_jobs import IMAGE, REVISION
        runtime_identity = dict(hardware)
        runtime_metrics = state.get('runtime_metrics') or {}
        power_limit = _positive_number(runtime_metrics.get('gpu_power_limit_watts'))
        if power_limit is not None:
            runtime_identity['gpu_power_limit_watts'] = power_limit
        identity = gpu_variant_identity(offer, runtime_identity)
        candidate = CalibrationProfiles(Path(__file__).resolve().parent / 'data/vast').match(identity)
        if (candidate and candidate.get('protocol') in SUPPORTED_PROFILE_PROTOCOLS
                and IMAGE in COMPATIBLE_CALIBRATION_IMAGES
                and candidate.get('worker_image') in COMPATIBLE_CALIBRATION_IMAGES
                and candidate.get('pb8_revision') == REVISION):
            calibrated_profile = candidate
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        calibrated_profile = None

    profile_population = (
        rental_override['population_size'] if rental_override is not None
        else int(calibrated_profile['population_size']) if calibrated_profile is not None
        else _cuda_population(vram_gb, memory_bandwidth_gbps, tflops)
        if vram_gb is not None else None
    )
    profile_dispatch_limit = (
        rental_override['max_dispatch_candidate_bars'] if rental_override is not None
        else calibrated_profile.get('max_dispatch_candidate_bars') if calibrated_profile is not None
        else None
    )
    if type(profile_dispatch_limit) is not int or profile_dispatch_limit <= 0:
        profile_dispatch_limit = None
    profile_scale = max(1, profile_population // 1024) if profile_population is not None else None
    profile_window = 128 * profile_scale if profile_scale is not None else None
    iterations = optimize.get('iters')
    auto_profile = (
        vram_gb is not None
        and isinstance(iterations, int)
        and not isinstance(iterations, bool)
        and (rental_override is not None or iterations >= profile_window)
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
            batch_size=rental_override['batch_size'] if rental_override is not None else population,
            validate_per_generation=8 * scale,
            drift_probes=4 * scale,
            drift_window=128 * scale,
        )
        if profile_dispatch_limit is not None:
            automatic['max_dispatch_candidate_bars'] = profile_dispatch_limit
        elif candidate_bars is not None:
            automatic['max_dispatch_candidate_bars'] = _dispatch_limit(
                vram_gb, candidate_bars, population,
                memory_bandwidth_gbps, tflops,
            )
        gpu.update(automatic)

    if gpu.get('max_dispatch_candidate_bars') is None and vram_gb is not None:
        configured_population = gpu.get('population_size') or optimize.get('population_size')
        target_candidates = (
            int(configured_population)
            if isinstance(configured_population, int) and configured_population > 0
            else 1024
        )
        automatic['max_dispatch_candidate_bars'] = (
            profile_dispatch_limit if profile_dispatch_limit is not None
            else _dispatch_limit(vram_gb, candidate_bars, target_candidates,
                                 memory_bandwidth_gbps, tflops)
        )
        gpu['max_dispatch_candidate_bars'] = automatic['max_dispatch_candidate_bars']
    elif ('max_dispatch_candidate_bars' not in automatic
          and gpu.get('max_dispatch_candidate_bars') is not None):
        preserved['max_dispatch_candidate_bars'] = gpu['max_dispatch_candidate_bars']

    if not auto_profile:
        population = gpu.get('population_size')
        if population is None:
            population = optimize.get('population_size')
        if population is not None:
            preserved['population_size'] = population
        if gpu.get('batch_size') is not None:
            preserved['batch_size'] = gpu['batch_size']

    population = gpu.get('population_size') or optimize.get('population_size')
    batch_size = gpu.get('batch_size')
    dispatch_limit = gpu.get('max_dispatch_candidate_bars')
    dispatch_candidates = (
        max(1, min(int(population or 1024),
                   int(batch_size) if isinstance(batch_size, int) and batch_size > 0 else int(population or 1024),
                   int(dispatch_limit) // candidate_bars))
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
        'applied_by': ('rental_override' if rental_override is not None and auto_profile
                       else 'local_calibration' if calibrated_profile and calibrated_profile.get('source') == 'local'
                       else 'pbgui_reference' if calibrated_profile else 'pbgui'),
        'calibration_profile_id': calibrated_profile.get('id') if calibrated_profile else None,
        'calibration_profile_match': calibrated_profile.get('match_type') if calibrated_profile else None,
        'calibration_max_dispatch_candidate_bars': (calibrated_profile.get('max_dispatch_candidate_bars')
                                                    if calibrated_profile else None),
        'gpu_power_limit_watts': measured_power_limit,
        'calibration_power_limit_delta_watts': calibrated_profile.get('power_limit_delta_watts') if calibrated_profile else None,
        'calibration_profile_power_limit_watts': (
            (calibrated_profile.get('hardware_identity') or {}).get('gpu_power_limit_watts')
            if calibrated_profile else None
        ),
        'cpu_workers': state.get('workers'),
        'largest_candidate_bars': candidate_bars,
        'active_parameters': parameters,
        'rental_gpu_profile': rental_override if auto_profile else None,
        'dispatch_candidates': dispatch_candidates,
        'dispatch_chunks': dispatch_chunks,
        'automatic': automatic,
        'preserved': preserved,
    }
    encoded = json.dumps(profile, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
    profile['fingerprint'] = hashlib.sha256(encoded).hexdigest()
    return profile
