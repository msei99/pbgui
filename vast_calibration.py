"""Versioned Vast GPU calibration identities, profiles and selection rules."""
from __future__ import annotations

import copy
import hashlib
import json
import math
import re
from pathlib import Path

from file_lock import advisory_file_lock
from secure_files import atomic_write_private_text, ensure_private_directory

SERVICE = "VastCalibration"
PROFILE_SCHEMA = 1
PROTOCOL_VERSION = 3
CONFIGURABLE_PROTOCOL_VERSION = 4
SUPPORTED_PROFILE_PROTOCOLS = frozenset({1, 2, PROTOCOL_VERSION, CONFIGURABLE_PROTOCOL_VERSION})
# The published upstream worker supports both fixed and configurable tests.
CALIBRATION_WORKER_DIGEST: str | None = "09cb0f9ba004db44f3ca02a7b3b03ea3211fd9e6c3c9ea33794cd4c6d24dee30"
PREVIOUS_CALIBRATION_WORKER_DIGEST = "8ad62f43decae47fe670f3ac15ba7e4d7c648f0bb030fa511cd4771321ff4327"
# Old evidence stays readable; new tests require the pinned current image.
COMPATIBLE_CALIBRATION_IMAGES = frozenset({
    "ghcr.io/msei99/pbgui-pb8-worker@sha256:" + CALIBRATION_WORKER_DIGEST,
    "ghcr.io/msei99/pbgui-pb8-worker@sha256:f078b47466f53e3039b13e41905531d9504ca6caca7b57469499fae20b77ec0e",
    "ghcr.io/msei99/pbgui-pb8-worker@sha256:" + PREVIOUS_CALIBRATION_WORKER_DIGEST,
    "ghcr.io/msei99/pbgui-pb8-worker@sha256:70366b9989a12528245d4c263e0e3dc4350271427afd76f9568dcf29cf33878f",
    "ghcr.io/msei99/pbgui-pb8-worker@sha256:bee2e513d77e2c22f5b0671392063c51bb04bd547c7334e614fe918ada2d49e2",
    "ghcr.io/msei99/pbgui-pb8-worker@sha256:8a85444f341a447564fc23e955b34f8a68b1970afc54026334aefa204c6cbc56",
})
LEGACY_ANCHOR_POPULATIONS = (1024, 2048, 4096, 8192)
ANCHOR_POPULATIONS = (4096, 8192)
POPULATION_STEP = 4096
MAX_POPULATION = 131072
MIN_SCALE_GAIN = 0.10
OPTIONAL_POPULATIONS = tuple(range(12288, MAX_POPULATION + 1, POPULATION_STEP))
ALL_POPULATIONS = (1024, 2048) + ANCHOR_POPULATIONS + OPTIONAL_POPULATIONS
WORKLOAD_VERSION = "pb8-gpu-calibration-v2"
CONFIGURABLE_WORKLOAD_VERSION = "pb8-gpu-calibration-v3"
PRESET_COINS = {
    'small': ('BTC', 'ETH', 'SOL'),
    'medium': ('BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'DOGE', 'ADA', 'LINK', 'AVAX', 'LTC'),
    'large': ('BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'DOGE', 'ADA', 'LINK', 'AVAX', 'LTC',
              'TRX', 'DOT', 'BCH', 'UNI', 'ATOM', 'ETC', 'XLM', 'NEAR', 'APT', 'ARB',
              'OP', 'FIL', 'INJ', 'SUI', 'AAVE', 'MKR', 'RUNE', 'ICP', 'TIA', 'SEI',
              'WIF', 'PEPE', 'FET', 'GRT', 'ALGO', 'SAND', 'MANA', 'CRV', 'LDO', 'JTO', 'ORDI'),
}
PRESET_SCENARIOS = {
    'small': (('2024-01-01', '2024-12-31'),),
    'medium': (('2023-01-01', '2023-12-31'), ('2024-01-01', '2024-12-31')),
    'large': (('2024-01-01', '2024-03-31'), ('2024-04-01', '2024-06-30'),
              ('2024-07-01', '2024-09-30'), ('2024-10-01', '2024-12-31')),
}
def calibration_worker_available(image: str, protocol: int = PROTOCOL_VERSION) -> bool:
    """Require an immutable image pin for the exact protocol sent to the worker."""
    return (protocol in (PROTOCOL_VERSION, CONFIGURABLE_PROTOCOL_VERSION)
            and bool(CALIBRATION_WORKER_DIGEST)
            and image.endswith('@sha256:' + CALIBRATION_WORKER_DIGEST))


_PROFILE_ID = re.compile(r"^[0-9a-f]{64}$")


def _encoded(value: object) -> str:
    """Return a stable representation for persistent fingerprints."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _text(value: object) -> str:
    """Normalize public hardware labels without guessing model aliases."""
    return " ".join(str(value or "").strip().upper().split())[:160]


def _number(value: object) -> float | None:
    """Accept finite non-negative public hardware measurements only."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    result = float(value)
    return result if math.isfinite(result) and result >= 0 else None


def gpu_variant_identity(offer: dict, runtime: dict | None = None) -> dict:
    """Build an auditable exact or preliminary single-GPU variant identity."""
    runtime = runtime or {}
    runtime_name = _text(runtime.get("gpu") or runtime.get("gpu_name"))
    runtime_vram = runtime.get("vram_bytes")
    if isinstance(runtime_vram, bool) or not isinstance(runtime_vram, int) or runtime_vram <= 0:
        runtime_vram = None
    advertised_vram = _number(offer.get("vram_gb"))
    power_limit = _number(runtime.get("gpu_power_limit_watts"))
    if power_limit is not None and power_limit <= 0:
        power_limit = None
    identity = {
        "schema": PROFILE_SCHEMA,
        "runtime_verified": bool(runtime_name and runtime_vram),
        "gpu_name": runtime_name or _text(offer.get("gpu_name")),
        "vram_mib": int(round(runtime_vram / 1024**2)) if runtime_vram else (
            int(round(advertised_vram * 1024)) if advertised_vram is not None else None
        ),
        "pci_device_id": _text(runtime.get("pci_device_id")) or None,
        "compute_capability": _text(runtime.get("compute_capability")) or None,
        "gpu_mem_bw_gbps": _number(offer.get("gpu_mem_bw_gbps")),
        "gpu_power_limit_watts": power_limit,
        "gpu_power_limit_source": "runtime_nvidia_smi" if power_limit is not None else None,
    }
    fingerprint_fields = {key: identity[key] for key in (
        "schema", "runtime_verified", "gpu_name", "vram_mib", "pci_device_id",
        "compute_capability", "gpu_mem_bw_gbps", "gpu_power_limit_watts",
    )}
    identity["fingerprint"] = hashlib.sha256(_encoded(fingerprint_fields).encode()).hexdigest()
    identity["provider"] = {
        "gpu_name": _text(offer.get("gpu_name")),
        "vram_gb": advertised_vram,
        "machine_id": offer.get("machine_id") if type(offer.get("machine_id")) is int else None,
        "offer_id": offer.get("id") if type(offer.get("id")) is int else None,
        "gpu_max_power_watts": _number(offer.get("gpu_max_power_watts")),
    }
    return identity


def calibration_workload_fingerprint(config: dict) -> str:
    """Hash semantic workload settings while excluding calibrated parallelism."""
    normalized = copy.deepcopy(config)
    normalized.pop('pbgui', None)
    backtest = normalized.get('backtest') if isinstance(normalized, dict) else None
    if isinstance(backtest, dict):
        for key in ('ohlcv_source_dir', 'hlcvs_data_dir', 'base_dir'):
            backtest.pop(key, None)
    optimize = normalized.get('optimize') if isinstance(normalized, dict) else None
    if isinstance(optimize, dict):
        optimize.pop('population_size', None)
        optimize.pop('n_cpus', None)
        gpu = optimize.get('gpu')
        if isinstance(gpu, dict):
            for key in ('population_size', 'batch_size', 'max_dispatch_candidate_bars',
                        'validate_per_generation', 'drift_probes', 'drift_window', 'exact_workers'):
                gpu.pop(key, None)
    return hashlib.sha256(_encoded({'schema': PROFILE_SCHEMA, 'config': normalized}).encode()).hexdigest()


def canonical_calibration_config(template: dict, *, optimize_metadata: dict | None = None) -> dict:
    """Build PBGui's versioned EMA-anchor workload without user config input."""
    from pb8_config import get_pb8_optimize_metadata
    metadata = optimize_metadata if optimize_metadata is not None else get_pb8_optimize_metadata()
    if 'ema_anchor' not in metadata.get('strategies', []):
        raise ValueError('Installed PB8 runtime does not support EMA-anchor calibration')
    defaults = metadata['strategy_defaults']
    bounds = metadata['active_bounds']['ema_anchor']
    config = copy.deepcopy(template)
    live = config['live']
    live['user'] = 'pbgui_calibration'
    live['strategy_kind'] = 'ema_anchor'
    for side in ('long', 'short'):
        config.setdefault('bot', {}).setdefault(side, {})['strategy'] = {
            'ema_anchor': copy.deepcopy(defaults[side]['ema_anchor'])
        }
    live['approved_coins'] = {'long': ['BTC', 'ETH', 'SOL'], 'short': ['BTC', 'ETH', 'SOL']}
    live['ignored_coins'] = {'long': [], 'short': []}
    backtest = config['backtest']
    backtest.update(
        start_date='2024-01-01', end_date='2024-12-31', exchanges=['binance'],
        coin_sources={}, scenarios=[{'label': 'pbgui_calibration_v1'}], suite_enabled=False,
    )
    optimize = config['optimize']
    optimize.update(backend='gpu', iters=10_000_000, n_cpus=4, seed=7)
    optimize['scoring'] = [{'metric': 'adg_strategy_eq', 'goal': 'max'}]
    optimize['bounds'] = copy.deepcopy(bounds)
    optimize['enable_overrides'] = []
    optimize['fixed_params'] = []
    optimize['fixed_runtime_overrides'] = {}
    for side in ('long', 'short'):
        optimize['bounds'].setdefault(side, {}).setdefault('risk', {})['n_positions'] = [1, 3, 1]
    gpu = optimize['gpu']
    gpu.update(auto_lean_parallelism=False, exact_workers=4,
               population_size=ANCHOR_POPULATIONS[0], batch_size=ANCHOR_POPULATIONS[0])
    config['pbgui'] = {'calibration_workload': WORKLOAD_VERSION}
    return config


def preset_calibration_config(template: dict, preset: str, *, optimize_metadata: dict | None = None) -> dict:
    """Build a deterministic, versioned EMA-anchor workload for one preset."""
    if preset not in PRESET_COINS:
        raise ValueError('Unknown GPU performance-test preset')
    config = canonical_calibration_config(template, optimize_metadata=optimize_metadata)
    coins = list(PRESET_COINS[preset])
    config['live']['approved_coins'] = {'long': coins, 'short': coins}
    windows = PRESET_SCENARIOS[preset]
    config['backtest'].update(
        start_date=windows[0][0], end_date=windows[-1][1],
        exchanges=['binance'] if preset == 'small' else ['binance', 'bybit'],
        scenarios=[{'label': f'pbgui_{preset}_{index}', 'start_date': start, 'end_date': end}
                   for index, (start, end) in enumerate(windows, 1)],
        suite_enabled=len(windows) > 1,
    )
    config['pbgui']['calibration_workload'] = CONFIGURABLE_WORKLOAD_VERSION + '-' + preset
    return config


def configurable_calibration_plan(start: int, step: int, maximum: int, min_gain: float,
                                  sample_seconds: int = 300,
                                  case_timeout_seconds: int = 3600) -> dict:
    """Validate a bounded one-batch search before any rental begins."""
    if (type(start) is not int or type(step) is not int or type(maximum) is not int
            or start < 4096 or step < 512 or maximum < start + step
            or maximum > 131072 or (maximum - start) // step > 64
            or not isinstance(min_gain, (int, float)) or not .0 <= min_gain <= .5
            or type(sample_seconds) is not int or not 300 <= sample_seconds <= 1800
            or type(case_timeout_seconds) is not int or not 600 <= case_timeout_seconds <= 7200):
        raise ValueError('Invalid configurable GPU performance-test plan')
    return {'protocol': CONFIGURABLE_PROTOCOL_VERSION, 'populations': [start, start + step],
            'sample_seconds': sample_seconds, 'population_step': step,
            'min_scale_gain': float(min_gain), 'max_population': maximum,
            'vram_reserve_ratio': .15, 'case_timeout_seconds': case_timeout_seconds}


def calibration_decision(cases: dict[int, dict], *, protocol: int = PROTOCOL_VERSION,
                         plan: dict | None = None) -> dict:
    """Choose the next linear probe while retaining protocol-2 anchor semantics."""
    if protocol == CONFIGURABLE_PROTOCOL_VERSION:
        if not isinstance(plan, dict):
            raise ValueError('Missing configurable calibration plan')
        validated = configurable_calibration_plan(
            plan.get('populations', [None])[0], plan.get('population_step'),
            plan.get('max_population'), plan.get('min_scale_gain'), plan.get('sample_seconds'),
            plan.get('case_timeout_seconds'),
        )
        if plan != validated:
            raise ValueError('Invalid configurable calibration plan')
        anchors = tuple(validated['populations'])
        for population in anchors:
            if population not in cases:
                return {'next_population': population, 'recommended_population': None, 'stop_reason': None}
        valid = {int(population): row for population, row in cases.items()
                 if isinstance(row, dict) and row.get('valid') is True
                 and _number(row.get('rate_per_second')) not in (None, 0)}
        if any(population not in valid for population in anchors):
            return {'next_population': None, 'recommended_population': None, 'stop_reason': 'invalid_evidence'}
        latest = max(valid)
        previous = latest - validated['population_step']
        if previous not in valid:
            return {'next_population': None, 'recommended_population': None, 'stop_reason': 'invalid_evidence'}
        gain = float(valid[latest]['rate_per_second']) / float(valid[previous]['rate_per_second']) - 1
        if gain < 0:
            return {'next_population': None, 'recommended_population': previous,
                    'stop_reason': 'performance_regression', 'gain': gain}
        if gain < validated['min_scale_gain']:
            return {'next_population': None, 'recommended_population': latest,
                    'stop_reason': 'scaling_plateau', 'gain': gain}
        next_population = latest + validated['population_step']
        if next_population > validated['max_population']:
            return {'next_population': None, 'recommended_population': latest,
                    'stop_reason': 'maximum_safety_limit', 'gain': gain}
        return {'next_population': next_population, 'recommended_population': None,
                'stop_reason': None, 'gain': gain}
    if protocol not in (2, PROTOCOL_VERSION):
        raise ValueError('Unsupported adaptive calibration protocol')
    anchors = LEGACY_ANCHOR_POPULATIONS if protocol == 2 else ANCHOR_POPULATIONS
    for population in anchors:
        if population not in cases:
            return {"next_population": population, "recommended_population": None, "stop_reason": None}
    valid = {int(population): row for population, row in cases.items()
             if isinstance(row, dict) and row.get("valid") is True
             and _number(row.get("rate_per_second")) not in (None, 0)}
    if any(population not in valid for population in anchors):
        return {"next_population": None, "recommended_population": None, "stop_reason": "invalid_evidence"}
    probed = sorted(population for population in valid if population >= 8192)
    latest = probed[-1]
    if latest == 8192:
        return {"next_population": 12288, "recommended_population": None, "stop_reason": None}
    previous = latest - POPULATION_STEP
    if previous not in valid:
        return {"next_population": None, "recommended_population": None, "stop_reason": "invalid_evidence"}
    latest_rate = float(valid[latest]["rate_per_second"])
    previous_rate = float(valid[previous]["rate_per_second"])
    gain = latest_rate / previous_rate - 1.0
    if gain < 0.0:
        return {"next_population": None, "recommended_population": previous,
                "stop_reason": "performance_regression", "gain": gain}
    if gain < MIN_SCALE_GAIN:
        return {"next_population": None, "recommended_population": latest,
                "stop_reason": "scaling_plateau", "gain": gain}
    if latest >= MAX_POPULATION:
        return {"next_population": None, "recommended_population": latest,
                "stop_reason": "maximum_safety_limit", "gain": gain}
    return {"next_population": latest + POPULATION_STEP, "recommended_population": None,
            "stop_reason": None, "gain": gain}


def recommended_population(
    cases: dict[int, dict], *, protocol: int = PROTOCOL_VERSION, stop_reason: str | None = None,
    plan: dict | None = None
) -> int | None:
    """Return the recommendation defined by legacy or current protocol evidence."""
    rates = {int(population): _number(row.get("rate_per_second"))
             for population, row in cases.items() if isinstance(row, dict) and row.get("valid") is True}
    rates = {population: rate for population, rate in rates.items() if rate not in (None, 0)}
    if not rates:
        return None
    if protocol == CONFIGURABLE_PROTOCOL_VERSION:
        if stop_reason == 'rental_deadline':
            return max(rates, key=rates.get)
        if stop_reason == 'vram_limit':
            return max(rates)
        return calibration_decision(cases, protocol=protocol, plan=plan).get('recommended_population')
    if protocol in (2, PROTOCOL_VERSION):
        if stop_reason == "rental_deadline":
            return max(rates, key=rates.get)
        if stop_reason in {"vram_limit", "maximum_safety_limit"}:
            return max(rates)
        decision = calibration_decision(cases, protocol=protocol)
        return decision.get("recommended_population")
    threshold = max(rates.values()) * 0.95
    return min(population for population, rate in rates.items() if rate >= threshold)


class CalibrationProfiles:
    """Read shipped evidence and serialize accepted local profile overrides."""

    def __init__(self, root: Path, shipped_path: Path | None = None):
        self.root = Path(root)
        self.local_path = self.root / "calibration_profiles.json"
        self.shipped_path = shipped_path or Path(__file__).resolve().parent / "setup/vast_gpu_benchmark/reference_profiles.json"

    @staticmethod
    def _validated(payload: object, source: str) -> list[dict]:
        """Reject malformed profile bundles instead of partially trusting them."""
        if not isinstance(payload, dict) or payload.get("schema") != PROFILE_SCHEMA or not isinstance(payload.get("profiles"), list):
            raise ValueError("Unsupported GPU calibration profile schema")
        result = []
        for row in payload["profiles"]:
            if (not isinstance(row, dict) or not _PROFILE_ID.fullmatch(str(row.get("id") or ""))
                    or not _PROFILE_ID.fullmatch(str(row.get("gpu_variant_fingerprint") or ""))
                    or type(row.get("population_size")) is not int or row["population_size"] <= 0
                    or (row.get('max_dispatch_candidate_bars') is not None
                        and (type(row['max_dispatch_candidate_bars']) is not int
                             or row['max_dispatch_candidate_bars'] <= 0))):
                raise ValueError("Invalid GPU calibration profile")
            result.append(dict(row, source=source))
        return result

    def list(self) -> list[dict]:
        """Return local profiles before immutable shipped references."""
        shipped = json.loads(self.shipped_path.read_text())
        local = {"schema": PROFILE_SCHEMA, "profiles": []}
        if self.local_path.is_file() and not self.local_path.is_symlink():
            local = json.loads(self.local_path.read_text())
        return self._validated(local, "local") + self._validated(shipped, "pbgui")

    def _preferred_rows(self) -> list[dict]:
        """Keep local precedence while preferring newer protocols at equal hardware match."""
        return sorted(self.list(), key=lambda row: (
            row.get('source') != 'local',
            -(row['protocol'] if type(row.get('protocol')) is int else 0),
        ))

    def match(self, identity: dict, workload_fingerprint: str | None = None) -> dict | None:
        """Prefer exact identities, then nearest power limit on the exact runtime card variant."""
        power = _number(identity.get("gpu_power_limit_watts"))
        if (identity.get("runtime_verified") is not True
                or identity.get("gpu_power_limit_source") != "runtime_nvidia_smi"
                or power is None or power <= 0):
            return None
        rows = self._preferred_rows()
        fingerprint = identity.get("fingerprint")
        for row in rows:
            expected = row.get("workload_fingerprint")
            if (row["gpu_variant_fingerprint"] == fingerprint
                    and (workload_fingerprint is None or expected in (None, workload_fingerprint))):
                return dict(row, match_type="exact", power_limit_delta_watts=0.0)

        exact_variant_fields = ("gpu_name", "vram_mib", "pci_device_id", "compute_capability")
        if any(identity.get(key) in (None, "") for key in exact_variant_fields):
            return None
        nearest = None
        for order, row in enumerate(rows):
            expected = row.get("workload_fingerprint")
            if workload_fingerprint is not None and expected not in (None, workload_fingerprint):
                continue
            candidate = row.get("hardware_identity") or {}
            if (candidate.get("runtime_verified") is not True
                    or any(candidate.get(key) != identity.get(key) for key in exact_variant_fields)):
                continue
            candidate_power = _number(candidate.get("gpu_power_limit_watts"))
            if candidate_power is None or candidate_power <= 0:
                continue
            delta = abs(candidate_power - power)
            if nearest is None or delta < nearest[0]:
                nearest = (delta, order, row)
        if nearest is None:
            return None
        delta, _order, row = nearest
        return dict(row, match_type="nearest_power_limit", power_limit_delta_watts=delta)

    def preliminary_match(self, offer: dict) -> dict | None:
        """Return the nearest preliminary same-model and exact-advertised-VRAM profile."""
        expected_name = _text(offer.get('gpu_name'))
        expected_vram = _number(offer.get('vram_gb'))
        if not expected_name or expected_vram is None:
            return None
        offer_power = _number(offer.get('gpu_max_power_watts'))
        candidates = []
        for order, row in enumerate(self._preferred_rows()):
            provider = (row.get('hardware_identity') or {}).get('provider') or {}
            if (_text(provider.get('gpu_name')) != expected_name
                    or _number(provider.get('vram_gb')) != expected_vram):
                continue
            profile_power = _number((row.get('hardware_identity') or {}).get('gpu_power_limit_watts'))
            delta = abs(profile_power - offer_power) if profile_power and offer_power else None
            candidates.append((float('inf') if delta is None else delta, order, row, delta))
        if not candidates:
            return None
        _sort_delta, _order, row, delta = min(candidates, key=lambda item: (item[0], item[1]))
        return dict(row, match_type='preliminary_nearest' if delta is not None else 'preliminary',
                    power_limit_delta_watts=delta)

    def accept(self, profile: dict) -> dict:
        """Atomically add or replace one accepted exact local profile."""
        validated = self._validated({"schema": PROFILE_SCHEMA, "profiles": [profile]}, "local")[0]
        ensure_private_directory(self.root)
        with advisory_file_lock(self.root / ".calibration-profiles-lock"):
            current = {"schema": PROFILE_SCHEMA, "profiles": []}
            if self.local_path.is_file() and not self.local_path.is_symlink():
                current = json.loads(self.local_path.read_text())
            rows = self._validated(current, "local")
            key = (validated["gpu_variant_fingerprint"], validated.get("workload_fingerprint"))
            rows = [row for row in rows if (row["gpu_variant_fingerprint"], row.get("workload_fingerprint")) != key]
            rows.append({key: value for key, value in validated.items() if key != "source"})
            atomic_write_private_text(self.local_path, json.dumps({"schema": PROFILE_SCHEMA, "profiles": rows}, indent=4) + "\n")
        return validated


def finalize_calibration_result(store, identifier: str) -> dict:
    """Validate downloaded worker evidence and build a pending local profile."""
    from secure_files import read_regular_file_nofollow
    from vast_jobs import job_id
    from pb8_config import load_pb8_config

    identifier = job_id(identifier)
    state = store.read(identifier)
    result_path = store.directory(identifier) / 'final-results/calibration-result.json'
    try:
        if result_path.stat(follow_symlinks=False).st_size > 1024 * 1024:
            raise ValueError('Oversized calibration evidence')
        raw = read_regular_file_nofollow(result_path, store.root)
        result = json.loads(raw)
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
        raise ValueError('Calibration evidence is missing or invalid') from exc
    protocol = result.get('protocol')
    stop_reason = result.get('stop_reason')
    plan = state.get('calibration_plan') if protocol == CONFIGURABLE_PROTOCOL_VERSION else None
    if protocol == CONFIGURABLE_PROTOCOL_VERSION:
        if not isinstance(plan, dict):
            raise ValueError('Configurable calibration plan is missing')
        expected = configurable_calibration_plan(
            plan.get('populations', [None])[0], plan.get('population_step'),
            plan.get('max_population'), plan.get('min_scale_gain'), plan.get('sample_seconds'),
            plan.get('case_timeout_seconds'))
        if plan != expected:
            raise ValueError('Configurable calibration plan changed')
    if (not isinstance(result, dict) or result.get('schema') != PROFILE_SCHEMA
            or protocol not in SUPPORTED_PROFILE_PROTOCOLS or result.get('status') != 'completed'
            or type(result.get('population_size')) is not int
            or (protocol != CONFIGURABLE_PROTOCOL_VERSION
                and result['population_size'] not in ALL_POPULATIONS)
            or not isinstance(result.get('cases'), dict)):
        raise ValueError('Calibration evidence did not complete validly')
    if protocol in (2, PROTOCOL_VERSION, CONFIGURABLE_PROTOCOL_VERSION) and stop_reason not in {
        'scaling_plateau', 'performance_regression', 'vram_limit',
        'rental_deadline', 'maximum_safety_limit',
    }:
        raise ValueError('Calibration evidence has no valid stop reason')
    cases = {}
    for key, row in result['cases'].items():
        try:
            population = int(key)
        except (TypeError, ValueError):
            raise ValueError('Calibration evidence contains an invalid population') from None
        if ((protocol != CONFIGURABLE_PROTOCOL_VERSION and population not in ALL_POPULATIONS)
                or (protocol == CONFIGURABLE_PROTOCOL_VERSION
                    and (population < plan['populations'][0] or population > plan['max_population']
                         or (population - plan['populations'][0]) % plan['population_step'] != 0))
                or not isinstance(row, dict)
                or row.get('population_size') != population or row.get('valid') is not True
                or _number(row.get('rate_per_second')) in (None, 0)):
            raise ValueError('Calibration evidence contains an invalid case')
        cases[population] = row
    if recommended_population(cases, protocol=protocol, stop_reason=stop_reason,
                              plan=plan) != result['population_size']:
        raise ValueError('Calibration recommendation does not match its evidence')
    measured_dispatch_limit = None
    if protocol >= 3:
        chosen = cases[result['population_size']]
        measured_dispatch_limit = chosen.get('candidate_bars')
        tested_dispatch_limit = chosen.get('tested_dispatch_limit')
        if ((protocol == CONFIGURABLE_PROTOCOL_VERSION
             and chosen.get('all_dispatches_full') is not True)
                or chosen.get('dispatch_batch_size') != result['population_size']
                or type(measured_dispatch_limit) is not int or measured_dispatch_limit <= 0
                or type(tested_dispatch_limit) is not int
                or tested_dispatch_limit < measured_dispatch_limit):
            raise ValueError('Calibration did not measure a full population dispatch')
    lease_id = state.get('lease_id') or identifier
    lease_intent = store.read(lease_id, 'intent.json')
    offer = lease_intent.get('offer') or {}
    runtime_identity = dict(result.get('hardware') or {})
    runtime_metrics = state.get('runtime_metrics') or {}
    measured_power_limit = _number(runtime_metrics.get('gpu_power_limit_watts'))
    if measured_power_limit is None or measured_power_limit <= 0:
        raise ValueError(
            'GPU power limit was not measured by nvidia-smi; result cannot be saved as a reusable profile'
        )
    runtime_identity['gpu_power_limit_watts'] = measured_power_limit
    identity = gpu_variant_identity(offer, runtime_identity)
    if not identity.get('runtime_verified'):
        raise ValueError('Runtime GPU identity was not verified')
    own_intent = store.read(identifier, 'intent.json')
    config = load_pb8_config(store.directory(identifier) / 'input/optimize.json')
    workload_fingerprint = calibration_workload_fingerprint(config)
    evidence = {
        'schema': PROFILE_SCHEMA, 'protocol': protocol,
        'gpu_variant_fingerprint': identity['fingerprint'],
        'workload_fingerprint': workload_fingerprint,
        'population_size': result['population_size'], 'cases': result['cases'],
        'hardware_identity': identity, 'pb8_revision': own_intent.get('pb8_revision'),
        'worker_image': own_intent.get('image'), 'finished_at': result.get('finished_at'),
        'stop_reason': result.get('stop_reason'),
        'source_job_id': identifier,
    }
    if measured_dispatch_limit is not None:
        evidence['max_dispatch_candidate_bars'] = measured_dispatch_limit
    evidence['id'] = hashlib.sha256(_encoded(evidence).encode()).hexdigest()
    return evidence
