"""Explicit GPU sizing preview and measured, workload-specific suggestions.

Suggestions never modify the submitted optimizer config. Provider offer
identity is preliminary until the rented card is verified at runtime.
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
from pathlib import Path

from vast_gpu_tuning import _largest_candidate_bars

SERVICE = "VastGpuRecommendation"
REFERENCE_PATH = Path(__file__).resolve().parent / "setup/vast_gpu_benchmark/throughput_profiles.json"


def workload_fingerprint(config: dict) -> str:
    """Identify proxy workload semantics while ignoring run length and sizing."""
    normalized = copy.deepcopy(config)
    normalized.pop("pbgui", None)
    live = normalized.get("live")
    if isinstance(live, dict):
        live.pop("user", None)
    backtest = normalized.get("backtest")
    if isinstance(backtest, dict):
        for key in ("ohlcv_source_dir", "hlcvs_data_dir", "base_dir"):
            backtest.pop(key, None)
    optimize = normalized.get("optimize")
    if isinstance(optimize, dict):
        for key in ("backend", "iters", "n_cpus", "seed", "population_size"):
            optimize.pop(key, None)
        gpu = optimize.get("gpu")
        if isinstance(gpu, dict):
            for key in (
                "auto_lean_parallelism", "population_size", "batch_size",
                "max_dispatch_candidate_bars", "exact_workers",
                "max_pending_exact", "validate_per_generation",
                "checkpoint_interval_seconds", "drift_probes", "drift_window",
                "drift_min_samples", "drift_halt", "drift_rank_halt",
                "drift_objective_tolerance",
            ):
                gpu.pop(key, None)
            if not gpu:
                optimize.pop("gpu", None)
    def normalize_numbers(value):
        if isinstance(value, float) and value.is_integer():
            return int(value)
        if isinstance(value, dict):
            return {key: normalize_numbers(item) for key, item in value.items()}
        if isinstance(value, list):
            return [normalize_numbers(item) for item in value]
        return value

    payload = json.dumps(normalize_numbers(normalized), sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(payload.encode()).hexdigest()


def dispatch_preview(config: dict, *, measured_candidate_bars: int | None = None) -> dict:
    """Estimate batches from explicit fields and measured or calendar work."""
    optimize = config.get("optimize") if isinstance(config, dict) else None
    optimize = optimize if isinstance(optimize, dict) else {}
    gpu = optimize.get("gpu")
    gpu = gpu if isinstance(gpu, dict) else {}
    candidate_bars = (measured_candidate_bars if type(measured_candidate_bars) is int
                      and measured_candidate_bars > 0 else _largest_candidate_bars(config))
    population = gpu.get("population_size")
    if population is None:
        population = optimize.get("population_size")
    batch = gpu.get("batch_size")
    cap = gpu.get("max_dispatch_candidate_bars")
    result = {
        "candidate_bars_estimate": candidate_bars,
        "population_size": population,
        "batch_size": batch,
        "max_dispatch_candidate_bars": cap,
        "minimum_one_batch_bars_estimate": None,
        "effective_batch_estimate": None,
        "dispatches_per_largest_scenario_estimate": None,
        "basis": ("measured matching workload" if measured_candidate_bars else
                  "conservative calendar estimate; actual PB8 safety limits and market candles may differ"),
    }
    if not all(type(value) is int and value > 0 for value in (population, batch, cap, candidate_bars)):
        return result
    effective = min(population, batch, max(1, cap // candidate_bars))
    result["minimum_one_batch_bars_estimate"] = population * candidate_bars
    result["effective_batch_estimate"] = effective
    result["dispatches_per_largest_scenario_estimate"] = math.ceil(population / effective)
    return result


def measured_suggestion(config: dict, offer: dict | None) -> dict:
    """Offer an explicit triple only for a recorded workload and power class."""
    if not isinstance(offer, dict):
        return {"profile": None, "reason": "Select a GPU offer to compare measured profiles."}
    try:
        payload = json.loads(REFERENCE_PATH.read_text())
    except (OSError, ValueError, json.JSONDecodeError):
        return {"profile": None, "reason": "Measured GPU reference profiles are unavailable."}
    if payload.get("schema") != 1 or not isinstance(payload.get("profiles"), list):
        return {"profile": None, "reason": "Measured GPU reference profiles are invalid."}
    name = "".join(char for char in str(offer.get("gpu_name") or "").lower() if char.isalnum())
    power = offer.get("gpu_max_power_watts")
    vram = offer.get("vram_gb")
    if isinstance(power, bool) or not isinstance(power, (int, float)) or not math.isfinite(power):
        return {"profile": None, "reason": "This offer has no advertised power limit; no preliminary profile match."}
    if isinstance(vram, bool) or not isinstance(vram, (int, float)) or not math.isfinite(vram):
        return {"profile": None, "reason": "This offer has no usable VRAM figure."}
    fingerprint = workload_fingerprint(config)
    for row in payload["profiles"]:
        if not isinstance(row, dict):
            continue
        if (row.get("workload_fingerprint") != fingerprint
                or row.get("gpu_name_key") != name
                or abs(float(row.get("vram_gb", 0)) - vram) > 0.25
                or abs(float(row.get("measured_power_limit_watts", 0)) - power) > 0.5):
            continue
        values = (row.get("population_size"), row.get("batch_size"), row.get("max_dispatch_candidate_bars"))
        if not all(type(value) is int and value > 0 for value in values):
            continue
        return {"profile": {
            "population_size": values[0],
            "batch_size": values[1],
            "max_dispatch_candidate_bars": values[2],
            "measured_power_limit_watts": row["measured_power_limit_watts"],
            "workload": row.get("workload"),
            "source": row.get("source"),
            "candidates_per_second": row.get("candidates_per_second"),
            "peak_vram_gib": row.get("peak_vram_gib"),
            "candidate_bars_per_candidate": row.get("candidate_bars_per_candidate"),
            "match": "preliminary offer; runtime GPU identity is not yet verified",
        }, "reason": None}
    return {"profile": None, "reason": "No measured profile for this exact workload and advertised GPU power/VRAM. Set the three values manually."}


def matching_measured_profiles(config: dict, offer: dict, reference_path: Path | None = None) -> list[dict]:
    """List shipped measurements for this exact workload and GPU/VRAM, including other power limits."""
    try:
        payload = json.loads((reference_path or REFERENCE_PATH).read_text())
    except (OSError, ValueError, json.JSONDecodeError):
        return []
    if payload.get('schema') != 1 or not isinstance(payload.get('profiles'), list):
        return []
    name = ''.join(char for char in str(offer.get('gpu_name') or '').lower() if char.isalnum())
    vram = offer.get('vram_gb')
    if not name or isinstance(vram, bool) or not isinstance(vram, (int, float)) or not math.isfinite(vram):
        return []
    fingerprint = workload_fingerprint(config)
    offered_power = offer.get('gpu_max_power_watts')
    choices = []
    for row in payload['profiles']:
        if not isinstance(row, dict) or row.get('workload_fingerprint') != fingerprint or row.get('gpu_name_key') != name:
            continue
        try:
            row_vram = float(row.get('vram_gb'))
            watts = float(row.get('measured_power_limit_watts'))
            values = (row.get('population_size'), row.get('batch_size'), row.get('max_dispatch_candidate_bars'))
            if (not math.isfinite(row_vram) or abs(row_vram - vram) > .25
                    or not math.isfinite(watts) or watts <= 0
                    or not all(type(value) is int and value > 0 for value in values)
                    or values[1] > values[0]):
                continue
        except (TypeError, ValueError):
            continue
        choices.append({
            'population_size': values[0], 'batch_size': values[1],
            'max_dispatch_candidate_bars': values[2],
            'measured_power_limit_watts': watts,
            'offered_power_limit_watts': offered_power,
            'candidates_per_second': row.get('candidates_per_second'),
            'peak_vram_gib': row.get('peak_vram_gib'),
            'candidate_bars_per_candidate': row.get('candidate_bars_per_candidate'),
            'workload': row.get('workload'),
            'source': row.get('source'),
        })
    return sorted(choices, key=lambda row: abs(row['measured_power_limit_watts'] - offered_power)
                  if isinstance(offered_power, (int, float)) and offered_power > 0 else 0)


def queued_gpu_previews(queue, offer: dict, *, limit: int = 100) -> dict:
    """Preview the frozen Auto sizing for each ready optimizer on one selected offer."""
    from pb8_config import load_pb8_config
    from vast_gpu_tuning import resolve_gpu_settings
    from logging_helpers import human_log as _log

    pending = queue.waiting()
    jobs = []
    for row in pending[:limit]:
        item = {'id': row['id'], 'name': row.get('config_name') or row['id'],
                'estimated_coin_candles': row.get('estimated_coin_candles')}
        path = queue.store.directory(row['id']) / 'input/optimize.json'
        try:
            if path.is_symlink() or not path.is_file() or path.stat().st_size > 4_000_000:
                raise ValueError('Prepared optimizer config unavailable')
            frozen = load_pb8_config(path)
            prepared = copy.deepcopy(frozen)
            tuning = resolve_gpu_settings(prepared, {'workers': row.get('workers')}, offer)
            optimize = prepared.get('optimize') or {}
            gpu = optimize.get('gpu') or {}
            sizing = {key: gpu.get(key) for key in (
                'population_size', 'batch_size', 'max_dispatch_candidate_bars')}
            sizing['population_size'] = sizing['population_size'] or optimize.get('population_size')
            item.update(
                mode=('auto' if tuning['automatic'].get('population_size') else 'configured'),
                auto=sizing,
                candidate_bars_per_largest_scenario=tuning.get('largest_candidate_bars'),
                dispatch=dispatch_preview(prepared),
                measurements=matching_measured_profiles(frozen, offer),
            )
        except (OSError, ValueError, TypeError, KeyError) as exc:
            _log(SERVICE, f"GPU rental preview unavailable for {row['id']}: {type(exc).__name__}", level='WARNING')
            item['error'] = 'Prepared GPU settings could not be previewed'
        jobs.append(item)
    return {'jobs': jobs, 'total_jobs': len(pending), 'truncated': len(pending) > limit}
