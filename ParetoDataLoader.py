"""
ParetoDataLoader - Loads and analyzes all_results.bin from Passivbot optimization runs
Provides comprehensive metrics, robustness scores, and parameter analysis
"""

import os
import heapq
import msgpack
import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Optional, Any, Iterator
from dataclasses import dataclass
import json
import glob
from pathlib import Path, PurePath
import re
import time
import hashlib

try:
    import msgspec  # type: ignore
except Exception:
    msgspec = None

_HAS_MSGSPEC = (msgspec is not None) and (os.environ.get("PBG_DISABLE_MSGSPEC") != "1")
_MSGSPEC_MSGPACK_DECODER = msgspec.msgpack.Decoder() if _HAS_MSGSPEC else None

_DISABLE_SCAN_CACHE = os.environ.get("PBG_DISABLE_SCAN_CACHE") == "1"
_FULL_SCAN_CACHE = os.environ.get("PBG_FULL_SCAN_CACHE") == "1"


@dataclass
class ConfigMetrics:
    """Holds all metrics and analysis for a single config"""
    config_index: int
    config_hash: str
    
    # Objectives
    objectives: Dict[str, float]
    constraint_violation: float
    
    # Suite metrics (aggregated)
    suite_metrics: Dict[str, float]
    
    # Scenario-specific metrics
    scenario_metrics: Dict[str, Dict[str, float]]  # scenario -> metric -> value
    
    # Robustness scores
    robustness_scores: Dict[str, float]  # metric -> robustness score (1/CV)
    
    # Bot parameters
    bot_params: Dict[str, Any]
    
    # Optimize bounds (parameter search space)
    bounds: Dict[str, Tuple[float, float]] = None
    
    # Optimize settings (scoring, limits, etc)
    optimize_settings: Dict[str, Any] = None
    
    # Backtest scenarios details
    scenario_details: List[Dict[str, Any]] = None
    
    # Stats for each metric (mean, min, max, std across scenarios)
    metric_stats: Dict[str, Dict[str, float]] = None
    
    # Is this config on the Pareto front?
    is_pareto: bool = False

    # Cached overall robustness score [0, 1] used when `robustness_scores` isn't loaded.
    overall_robustness: float = 0.0

    # Lazy-loading flags (all_results.bin mode)
    details_loaded: bool = True
    bot_params_loaded: bool = True


class ParetoDataLoader:
    """Loads and analyzes all_results.bin from optimize runs"""
    
    def __init__(self, results_path: str):
        """
        Initialize loader with results directory
        
        Args:
            results_path: Path to optimize_results directory (contains all_results.bin)
        """
        self.results_path = results_path
        self.all_results_path = os.path.join(results_path, "all_results.bin")
        self.pareto_dir = os.path.join(results_path, "pareto")
        
        self.configs: List[ConfigMetrics] = []
        self.pareto_hashes: set = set()
        self.scoring_metrics: List[str] = []
        self.scenario_labels: List[str] = []
        
        # Cache for pareto JSON configs (index -> full config)
        self.pareto_configs_cache: Dict[int, Dict] = {}
        
        # Cache for raw config_data from all_results.bin (config_index -> config_data)
        # This avoids re-reading the entire file for each config
        self.raw_configs_cache: Dict[int, Dict] = {}

        # If a load fails for a known, user-facing reason, store it here.
        # The UI can surface it without a stack trace.
        self.last_error: Optional[str] = None
        
        # Global optimization settings (same for all configs)
        self.optimize_bounds: Dict[str, Tuple[float, float]] = {}
        self.optimize_limits: List[Dict] = []
        self.backtest_scenarios: List[Dict] = []

    def _scan_cache_paths(self) -> Tuple[str, str]:
        """Return (meta_json_path, npz_path) for the persistent scan cache."""
        base = self.all_results_path + ".scan_cache"
        return base + ".meta.json", base + ".npz"

    @staticmethod
    def _normalize_optimize_bounds(bounds: Dict) -> Dict[str, Tuple[float, float]]:
        """Normalize PB7 optimize bounds to a simple (lower, upper) tuple per parameter.

        PB7 may store bounds as e.g. [lower, upper, step].
        This converts list/tuple/dict formats into (lower, upper) floats.
        Unparseable entries are skipped.
        """

        def _to_pair(v):
            if v is None:
                return None
            if isinstance(v, dict):
                for lo_key, hi_key in (("lower", "upper"), ("min", "max"), ("lo", "hi")):
                    if lo_key in v and hi_key in v:
                        try:
                            return float(v[lo_key]), float(v[hi_key])
                        except Exception:
                            return None
                if "bounds" in v:
                    return _to_pair(v.get("bounds"))
                return None
            if isinstance(v, (list, tuple)):
                if len(v) < 2:
                    return None
                try:
                    return float(v[0]), float(v[1])
                except Exception:
                    return None
            return None

        out: Dict[str, Tuple[float, float]] = {}
        if not isinstance(bounds, dict):
            return out
        for k, v in bounds.items():
            if not isinstance(k, str):
                continue
            pair = _to_pair(v)
            if not pair:
                continue
            lo, hi = pair
            if lo > hi:
                lo, hi = hi, lo
            out[k] = (lo, hi)
        return out

    def _is_scan_cache_valid(self) -> bool:
        """Check if scan cache exists and matches current all_results.bin (mtime+size)."""
        meta_path, npz_path = self._scan_cache_paths()
        try:
            if not os.path.exists(meta_path) or not os.path.exists(npz_path):
                return False
            if not os.path.exists(self.all_results_path):
                return False
            st_bin = os.stat(self.all_results_path)
            with open(meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
            src = meta.get("source") or {}
            return int(src.get("size") or -1) == int(st_bin.st_size) and float(src.get("mtime") or -1) == float(st_bin.st_mtime)
        except Exception:
            return False

    def _load_scan_cache(self) -> Optional[Dict[str, Any]]:
        """Load scan cache (meta + numpy arrays). Returns dict or None on failure."""
        meta_path, npz_path = self._scan_cache_paths()
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
            arrays = np.load(npz_path, allow_pickle=False)
            return {"meta": meta, "arrays": arrays}
        except Exception:
            return None

    def _write_scan_cache(self, meta: Dict[str, Any], arrays: Dict[str, np.ndarray]) -> None:
        """Write scan cache to disk (best-effort)."""
        meta_path, npz_path = self._scan_cache_paths()
        try:
            os.makedirs(os.path.dirname(meta_path) or ".", exist_ok=True)
            tmp_meta = meta_path + ".tmp"
            # NOTE: np.savez appends ".npz" if the path doesn't end with it.
            # Ensure the temp file ends with ".npz" so os.replace() targets the correct filename.
            tmp_npz = npz_path + ".tmp.npz"

            with open(tmp_meta, "w", encoding="utf-8") as f:
                json.dump(meta, f)

            np.savez(tmp_npz, **arrays)

            os.replace(tmp_meta, meta_path)
            os.replace(tmp_npz, npz_path)
        except Exception:
            # Cache is an optimization; never fail loading due to cache write.
            try:
                if os.path.exists(tmp_meta):
                    os.remove(tmp_meta)
                if os.path.exists(tmp_npz):
                    os.remove(tmp_npz)
            except Exception:
                pass

    def _is_legacy_results_format(self) -> bool:
        """Detect older optimize_results formats which this UI does not support.

        Legacy signature (observed):
        - `pareto/*.json` filenames start with a numeric score prefix like `001.2345_<hash>.json`.
        - `pareto/*.json` content lacks `suite_metrics` (often contains `analyses*` keys instead).
        """
        try:
            if not os.path.exists(self.pareto_dir):
                return False
            json_files = glob.glob(os.path.join(self.pareto_dir, "*.json"))
            if not json_files:
                return False

            sample_file = os.path.basename(sorted(json_files)[0])
            if re.match(r"^\d+\.\d+_", sample_file):
                return True

            # Content-based fallback (cheap: inspect one file)
            sample_path = sorted(json_files)[0]
            with open(sample_path, "r", encoding="utf-8") as f:
                obj = json.load(f)
            if isinstance(obj, dict) and "suite_metrics" not in obj and ("analyses" in obj or "analyses_combined" in obj):
                return True

            return False
        except Exception:
            # Never block loading due to detection issues.
            return False

    def _decode_msgpack_object(self, payload: bytes) -> Any:
        """Decode a single msgpack object from a bytes slice.

        Prefers msgspec if installed; falls back to msgpack.
        """
        if _HAS_MSGSPEC and _MSGSPEC_MSGPACK_DECODER is not None:
            return _MSGSPEC_MSGPACK_DECODER.decode(payload)
        return msgpack.loads(payload, raw=False, strict_map_key=False)
        
    def load(self, load_strategy: List[str] = None, max_configs: int = 2000, progress_callback=None) -> bool:
        """
        Load all_results.bin and parse all configs
        
        Args:
            load_strategy: List of criteria to use for selecting top configs
                         Options: 'performance', 'robustness', 'sharpe', 'drawdown', 
                                 'calmar', 'sortino', 'omega', 'volatility', 'recovery'
                         Default: ['performance'] (Passivbot official)
            max_configs: Maximum number of configs to keep (default: 2000)
            progress_callback: Optional callback(current, total, message) for progress updates
        
        Returns:
            True if successful, False otherwise
        """
        self.last_error = None

        if self._is_legacy_results_format():
            self.last_error = "Old format not supported"
            return False
        if load_strategy is None:
            load_strategy = ['performance']  # Default: Passivbot official
        
        if not os.path.exists(self.all_results_path):
            return False
        
        t_total0 = time.perf_counter()

        # Reset state
        self.configs = []
        self.raw_configs_cache = {}
        self.pareto_configs_cache = {}
        self.scoring_metrics = []
        self.scenario_labels = []
        self.optimize_bounds = {}
        self.optimize_limits = []
        self.backtest_scenarios = []

        # Load pareto hashes to mark pareto configs
        t0 = time.perf_counter()
        self._load_pareto_hashes()
        t_load_pareto_hashes = time.perf_counter() - t0

        # Preload globals from first object (cheap) so the main scan can run in raw=True mode.
        try:
            with open(self.all_results_path, 'rb') as f:
                u0 = msgpack.Unpacker(f, raw=False, strict_map_key=False, read_size=1024 * 1024)
                first_obj = next(iter(u0))
            if isinstance(first_obj, dict):
                # populate scoring_metrics, optimize bounds/limits, scenarios, scenario_labels
                try:
                    optimize_config = first_obj.get('optimize', {}) or {}
                    self.scoring_metrics = optimize_config.get('scoring', []) or []
                    self.optimize_bounds = self._normalize_optimize_bounds(optimize_config.get('bounds', {}) or {})
                    self.optimize_limits = optimize_config.get('limits', []) or []
                except Exception:
                    pass

                try:
                    if 'suite_metrics' in first_obj:
                        suite_metrics_block = first_obj.get('suite_metrics', {}) or {}
                        if not self.scenario_labels:
                            self.scenario_labels = suite_metrics_block.get('scenario_labels', []) or []
                    backtest_config = first_obj.get('backtest', {}) or {}
                    suite_config = backtest_config.get('suite', {}) or {}
                    if suite_config.get('enabled'):
                        self.backtest_scenarios = suite_config.get('scenarios', []) or []
                except Exception:
                    pass
        except Exception:
            pass

        # -------- Persistent scan cache fast-path --------
        # If the cache is valid, avoid decoding the entire all_results.bin.
        # We'll select indices from cached arrays and only unpack selected configs by offsets.
        t_cache_load0 = time.perf_counter()
        scan_cache = self._load_scan_cache() if (not _DISABLE_SCAN_CACHE and self._is_scan_cache_valid()) else None
        t_cache_load = time.perf_counter() - t_cache_load0

        if scan_cache is not None:
            meta = scan_cache.get("meta") or {}
            arrays = scan_cache.get("arrays")
            try:
                # Restore globals from cache
                self.scoring_metrics = meta.get("scoring_metrics") or []
                self.scenario_labels = meta.get("scenario_labels") or []
                self.optimize_bounds = self._normalize_optimize_bounds(meta.get("optimize_bounds") or {})
                self.optimize_limits = meta.get("optimize_limits") or []
                self.backtest_scenarios = meta.get("backtest_scenarios") or []

                # If cache doesn't include fields required by the current strategy, fall back.
                strategy = list(load_strategy) if load_strategy else ['performance']

                required_fields = {"offsets", "constraint_violation", "primary"}
                if 'robustness' in strategy:
                    required_fields.add("overall_robustness")
                if 'sharpe' in strategy:
                    required_fields.add("sharpe")
                if 'drawdown' in strategy:
                    required_fields.add("drawdown")
                if 'calmar' in strategy:
                    required_fields.add("calmar")
                if 'sortino' in strategy:
                    required_fields.add("sortino")
                if 'omega' in strategy:
                    required_fields.add("omega")
                if 'volatility' in strategy:
                    required_fields.add("volatility")
                if 'recovery' in strategy:
                    required_fields.add("recovery")

                available_fields = set(getattr(arrays, "files", []) or [])
                missing = required_fields - available_fields
                if missing:
                    scan_cache = None
                    raise RuntimeError(f"scan cache missing fields: {sorted(missing)}")

                offsets = arrays["offsets"]
                constraint_v = arrays["constraint_violation"]
                primary_v = arrays["primary"]
                overall_rob_v = arrays["overall_robustness"] if "overall_robustness" in available_fields else None
                sharpe_v = arrays["sharpe"] if "sharpe" in available_fields else None
                drawdown_v = arrays["drawdown"] if "drawdown" in available_fields else None
                calmar_v = arrays["calmar"] if "calmar" in available_fields else None
                sortino_v = arrays["sortino"] if "sortino" in available_fields else None
                omega_v = arrays["omega"] if "omega" in available_fields else None
                volatility_v = arrays["volatility"] if "volatility" in available_fields else None
                recovery_v = arrays["recovery"] if "recovery" in available_fields else None

                total_parsed = int(len(offsets))
                if total_parsed <= 0:
                    scan_cache = None
                else:
                    idxs = np.arange(total_parsed, dtype=np.int32)
                    configs_per_criterion = max(1, int(max_configs // len(strategy))) if strategy else max_configs

                    def _topk_by(keys: Tuple[np.ndarray, ...], k: int) -> np.ndarray:
                        order = np.lexsort(keys)
                        return order[:k]

                    # Precompute full performance ordering (used for fill)
                    perf_order = _topk_by((idxs, primary_v, constraint_v), min(max_configs, total_parsed))
                    # Note: primary_v is stored as (-primary_metric) already? No; stored as positive.
                    # For performance we want constraint asc, -primary desc, idx asc => keys: (idx, -primary, constraint)
                    perf_order = np.lexsort((idxs, -primary_v, constraint_v))[: min(max_configs, total_parsed)]

                    selected_order: List[int] = []
                    selected_set: set = set()

                    for criterion in strategy:
                        if criterion == 'performance':
                            order = perf_order[: min(configs_per_criterion, total_parsed)]
                        elif criterion == 'robustness':
                            if overall_rob_v is None:
                                scan_cache = None
                                raise RuntimeError('scan cache missing overall_robustness')
                            order = np.lexsort((idxs, constraint_v, -overall_rob_v))[: min(configs_per_criterion, total_parsed)]
                        elif criterion == 'sharpe':
                            if sharpe_v is None:
                                scan_cache = None
                                raise RuntimeError('scan cache missing sharpe')
                            order = np.lexsort((idxs, -sharpe_v, constraint_v))[: min(configs_per_criterion, total_parsed)]
                        elif criterion == 'drawdown':
                            if drawdown_v is None:
                                scan_cache = None
                                raise RuntimeError('scan cache missing drawdown')
                            order = np.lexsort((idxs, -drawdown_v, constraint_v))[: min(configs_per_criterion, total_parsed)]
                        elif criterion == 'calmar':
                            if calmar_v is None:
                                scan_cache = None
                                raise RuntimeError('scan cache missing calmar')
                            order = np.lexsort((idxs, -calmar_v, constraint_v))[: min(configs_per_criterion, total_parsed)]
                        elif criterion == 'sortino':
                            if sortino_v is None:
                                scan_cache = None
                                raise RuntimeError('scan cache missing sortino')
                            order = np.lexsort((idxs, -sortino_v, constraint_v))[: min(configs_per_criterion, total_parsed)]
                        elif criterion == 'omega':
                            if omega_v is None:
                                scan_cache = None
                                raise RuntimeError('scan cache missing omega')
                            order = np.lexsort((idxs, -omega_v, constraint_v))[: min(configs_per_criterion, total_parsed)]
                        elif criterion == 'volatility':
                            if volatility_v is None:
                                scan_cache = None
                                raise RuntimeError('scan cache missing volatility')
                            order = np.lexsort((idxs, volatility_v, constraint_v))[: min(configs_per_criterion, total_parsed)]
                        elif criterion == 'recovery':
                            if recovery_v is None:
                                scan_cache = None
                                raise RuntimeError('scan cache missing recovery')
                            order = np.lexsort((idxs, recovery_v, constraint_v))[: min(configs_per_criterion, total_parsed)]
                        else:
                            order = perf_order[: min(configs_per_criterion, total_parsed)]

                        for cand in order.tolist():
                            if cand in selected_set:
                                continue
                            selected_set.add(int(cand))
                            selected_order.append(int(cand))

                    if len(selected_order) < max_configs:
                        for cand in perf_order.tolist():
                            if len(selected_order) >= max_configs:
                                break
                            if cand in selected_set:
                                continue
                            selected_set.add(int(cand))
                            selected_order.append(int(cand))

                    if len(selected_order) > max_configs:
                        selected_order = selected_order[:max_configs]

                    # Parse selected configs by offsets
                    if progress_callback:
                        progress_callback(0, max(1, len(selected_order)), "Parsing selected configs (offset cache)...")

                    t_parse_selected0 = time.perf_counter()
                    pairs = [(int(offsets[i]), int(i)) for i in selected_order if 0 <= int(i) < total_parsed]
                    pairs.sort(key=lambda x: x[0])

                    idx_to_metrics: Dict[int, ConfigMetrics] = {}
                    idx_to_raw: Dict[int, Dict] = {}

                    with open(self.all_results_path, 'rb') as f:
                        file_size = os.path.getsize(self.all_results_path)
                        for n_done, (off, i) in enumerate(pairs, 1):
                            try:
                                # Prefer decoding from a bounded bytes slice so msgspec can be used.
                                end = int(offsets[i + 1]) if (int(i) + 1) < total_parsed else int(file_size)
                                if end <= int(off):
                                    raise ValueError("invalid offset range")
                                f.seek(int(off))
                                payload = f.read(int(end - int(off)))

                                config_data = self._decode_msgpack_object(payload)

                                # Reuse precomputed robustness from scan cache (if present).
                                pre_rob = None
                                if overall_rob_v is not None:
                                    try:
                                        pre_rob = float(overall_rob_v[i])
                                    except Exception:
                                        pre_rob = None

                                if isinstance(config_data, dict) and any(isinstance(k, bytes) for k in config_data.keys()):
                                    metrics = self._parse_config_light_raw(i, config_data, precomputed_overall_robustness=pre_rob)
                                else:
                                    metrics = self._parse_config_light(
                                        i,
                                        config_data,
                                        precomputed_overall_robustness=pre_rob,
                                        compute_overall_robustness=False,
                                    )

                                idx_to_metrics[i] = metrics
                                idx_to_raw[i] = config_data
                            except Exception:
                                # Fall back to legacy per-object unpack
                                try:
                                    f.seek(int(off))
                                    unpacker = msgpack.Unpacker(f, raw=True, strict_map_key=False)
                                    config_data = next(iter(unpacker))
                                    pre_rob = None
                                    if overall_rob_v is not None:
                                        try:
                                            pre_rob = float(overall_rob_v[i])
                                        except Exception:
                                            pre_rob = None
                                    metrics = self._parse_config_light_raw(i, config_data, precomputed_overall_robustness=pre_rob)
                                    idx_to_metrics[i] = metrics
                                    idx_to_raw[i] = config_data
                                except Exception:
                                    continue
                            if progress_callback and (n_done % 50 == 0 or n_done == len(pairs)):
                                progress_callback(n_done, len(pairs), f"Parsed {n_done}/{len(pairs)} selected configs")

                    self.configs = [idx_to_metrics[i] for i in selected_order if i in idx_to_metrics]
                    self.raw_configs_cache = idx_to_raw
                    t_parse_selected = time.perf_counter() - t_parse_selected0

                    # Compute Pareto front
                    t_pareto0 = time.perf_counter()
                    self._compute_pareto_front_fast()
                    t_pareto = time.perf_counter() - t_pareto0

                    t_total = time.perf_counter() - t_total0
                    self.load_stats = {
                        'total_parsed': total_parsed,
                        'selected_configs': len(self.configs),
                        'pareto_configs': sum(1 for c in self.configs if c.is_pareto),
                        'scenarios': self.scenario_labels,
                        'scoring_metrics': self.scoring_metrics,
                        'load_strategy': load_strategy,
                        'max_configs': max_configs,
                        'timings': {
                            'total': t_total,
                            'load_pareto_hashes': t_load_pareto_hashes,
                            'parse_all_results': t_cache_load + t_parse_selected,
                            'scan_all_results': 0.0,
                            'parse_selected_configs': t_parse_selected,
                            'select_top_configs': 0.0,
                            'compute_pareto_front': t_pareto,
                        },
                    }
                    return True
            except Exception:
                # Fall back to full scan
                scan_cache = None

        # Parse configs streaming from msgpack (avoid materializing entire file as a list)
        if progress_callback:
            progress_callback(0, os.path.getsize(self.all_results_path), "Loading/parsing all_results.bin...")

        def _metrics_dict_from_config_data(config_data: Dict) -> Dict:
            if 'suite_metrics' in config_data:
                suite_metrics_block = config_data.get('suite_metrics', {}) or {}
                if not self.scenario_labels:
                    self.scenario_labels = suite_metrics_block.get('scenario_labels', []) or []
                return suite_metrics_block.get('metrics', {}) or {}
            metrics_block = config_data.get('metrics', {}) or {}
            return metrics_block.get('stats', {}) or {}

        def _ensure_globals_from_config_data(config_data: Dict) -> None:
            if 'optimize' in config_data:
                optimize_config = config_data.get('optimize', {}) or {}
                if not self.scoring_metrics:
                    self.scoring_metrics = optimize_config.get('scoring', []) or []
                if not self.optimize_bounds:
                    self.optimize_bounds = self._normalize_optimize_bounds(optimize_config.get('bounds', {}) or {})
                if not self.optimize_limits:
                    self.optimize_limits = optimize_config.get('limits', []) or []

            if not self.backtest_scenarios and 'backtest' in config_data:
                backtest_config = config_data.get('backtest', {}) or {}
                suite_config = backtest_config.get('suite', {}) or {}
                if suite_config.get('enabled'):
                    self.backtest_scenarios = suite_config.get('scenarios', []) or []

        def _aggregated_metric_value(metric_data: Any) -> float:
            if isinstance(metric_data, dict):
                if 'aggregated' in metric_data:
                    try:
                        return float(metric_data.get('aggregated') or 0.0)
                    except Exception:
                        return 0.0
                if 'mean' in metric_data:
                    try:
                        return float(metric_data.get('mean') or 0.0)
                    except Exception:
                        return 0.0
                stats = metric_data.get('stats')
                if isinstance(stats, dict):
                    try:
                        return float(stats.get('mean') or 0.0)
                    except Exception:
                        return 0.0
                return 0.0
            if isinstance(metric_data, (int, float)):
                return float(metric_data)
            return 0.0

        def _mean_std_from_metric_data(metric_data: Any) -> Tuple[float, float]:
            if not isinstance(metric_data, dict):
                return 0.0, 0.0
            if 'stats' in metric_data and isinstance(metric_data.get('stats'), dict):
                stats = metric_data.get('stats', {}) or {}
                mean = stats.get('mean', 0.0)
                std = stats.get('std', 0.0)
            else:
                mean = metric_data.get('mean', 0.0)
                std = metric_data.get('std', 0.0)
            try:
                return float(mean or 0.0), float(std or 0.0)
            except Exception:
                return 0.0, 0.0

        parsed_count = 0
        total_parsed = 0

        strategy = list(load_strategy) if load_strategy else ['performance']
        only_performance = len(strategy) == 1 and strategy[0] == 'performance'
        configs_per_criterion = max_configs // len(strategy) if strategy else max_configs
        configs_per_criterion = max(1, int(configs_per_criterion))
        needs_robustness = 'robustness' in strategy

        needs_sharpe = 'sharpe' in strategy
        needs_drawdown = 'drawdown' in strategy
        needs_calmar = 'calmar' in strategy
        needs_sortino = 'sortino' in strategy
        needs_omega = 'omega' in strategy
        needs_volatility = 'volatility' in strategy
        needs_recovery = 'recovery' in strategy

        # Heaps store (priority_tuple, idx, key_tuple)
        heaps: Dict[str, List[Tuple[Tuple[float, ...], int, Tuple[float, ...]]]] = {c: [] for c in strategy}
        perf_fill_heap: List[Tuple[Tuple[float, ...], int, Tuple[float, ...]]] = []

        def _push_candidate(heap: List, k: int, idx: int, key: Tuple[float, ...]):
            priority = tuple(-float(x) for x in key)
            item = (priority, int(idx), key)
            if len(heap) < k:
                heapq.heappush(heap, item)
                return
            if item[0] > heap[0][0]:
                heapq.heapreplace(heap, item)

        # For building a persistent scan cache
        offsets_all: List[int] = []
        constraint_all: List[float] = []
        primary_all: List[float] = []

        # Default: cache only fields required by the current strategy.
        # Opt-in full cache (faster strategy switching, slower first scan) via PBG_FULL_SCAN_CACHE=1.
        cache_full = bool(_FULL_SCAN_CACHE)
        store_sharpe = cache_full or needs_sharpe
        store_drawdown = cache_full or needs_drawdown
        store_calmar = cache_full or needs_calmar
        store_sortino = cache_full or needs_sortino
        store_omega = cache_full or needs_omega
        store_volatility = cache_full or needs_volatility
        store_recovery = cache_full or needs_recovery
        store_overall_rob = cache_full or needs_robustness

        fast_perf_scan = bool(only_performance and not cache_full)

        sharpe_all: Optional[List[float]] = [] if store_sharpe else None
        drawdown_all: Optional[List[float]] = [] if store_drawdown else None
        calmar_all: Optional[List[float]] = [] if store_calmar else None
        sortino_all: Optional[List[float]] = [] if store_sortino else None
        omega_all: Optional[List[float]] = [] if store_omega else None
        volatility_all: Optional[List[float]] = [] if store_volatility else None
        recovery_all: Optional[List[float]] = [] if store_recovery else None
        overall_rob_all: Optional[List[float]] = [] if store_overall_rob else None

        # Use raw=True for the scan to avoid decoding all strings on first load.
        # We'll parse selected configs with raw=False afterwards.
        primary_metric = self.scoring_metrics[0] if self.scoring_metrics else 'adg_w_usd'
        primary_metric_b = primary_metric.encode('utf-8', errors='ignore')

        def _get(d: Any, k: Any, default=None):
            if isinstance(d, dict):
                return d.get(k, default)
            return default

        def _metrics_dict_from_raw(obj: Dict) -> Dict:
            if b'suite_metrics' in obj:
                suite = _get(obj, b'suite_metrics', {}) or {}
                return _get(suite, b'metrics', {}) or {}
            m = _get(obj, b'metrics', {}) or {}
            return _get(m, b'stats', {}) or {}

        def _aggregated_metric_value_raw(metric_data: Any) -> float:
            if isinstance(metric_data, dict):
                if b'aggregated' in metric_data:
                    v = metric_data.get(b'aggregated')
                    try:
                        return float(v or 0.0)
                    except Exception:
                        return 0.0
                if b'mean' in metric_data:
                    v = metric_data.get(b'mean')
                    try:
                        return float(v or 0.0)
                    except Exception:
                        return 0.0
                stats = metric_data.get(b'stats')
                if isinstance(stats, dict):
                    v = stats.get(b'mean')
                    try:
                        return float(v or 0.0)
                    except Exception:
                        return 0.0
                return 0.0
            if isinstance(metric_data, (int, float)):
                return float(metric_data)
            return 0.0

        def _mean_std_from_metric_data_raw(metric_data: Any) -> Tuple[float, float]:
            if not isinstance(metric_data, dict):
                return 0.0, 0.0
            if b'stats' in metric_data and isinstance(metric_data.get(b'stats'), dict):
                stats = metric_data.get(b'stats', {}) or {}
                mean = stats.get(b'mean', 0.0)
                std = stats.get(b'std', 0.0)
            else:
                mean = metric_data.get(b'mean', 0.0)
                std = metric_data.get(b'std', 0.0)
            try:
                return float(mean or 0.0), float(std or 0.0)
            except Exception:
                return 0.0, 0.0

        t_scan0 = time.perf_counter()
        try:
            for idx, (offset, config_data) in enumerate(self._iter_binary_file(progress_callback=progress_callback, with_offsets=True, raw_mode=True)):
                total_parsed += 1

                offsets_all.append(int(offset))

                # raw=True scan extraction
                metrics_dict = _metrics_dict_from_raw(config_data)
                metrics_block = _get(config_data, b'metrics', {}) or {}
                try:
                    constraint_violation = float(metrics_block.get(b'constraint_violation', 0.0) or 0.0)
                except Exception:
                    constraint_violation = 0.0

                primary_val = _aggregated_metric_value_raw(metrics_dict.get(primary_metric_b))

                if fast_perf_scan:
                    constraint_all.append(float(constraint_violation))
                    primary_all.append(float(primary_val))
                    sharpe_val = drawdown_val = calmar_val = sortino_val = omega_val = volatility_val = recovery_val = 0.0
                    overall_robustness = 0.0
                else:
                    sharpe_val = _aggregated_metric_value_raw(metrics_dict.get(b'sharpe_ratio_usd')) if store_sharpe else 0.0
                    drawdown_val = _aggregated_metric_value_raw(metrics_dict.get(b'drawdown_worst_usd')) if store_drawdown else 0.0
                    calmar_val = _aggregated_metric_value_raw(metrics_dict.get(b'calmar_ratio_usd')) if store_calmar else 0.0
                    sortino_val = _aggregated_metric_value_raw(metrics_dict.get(b'sortino_ratio_usd')) if store_sortino else 0.0
                    omega_val = _aggregated_metric_value_raw(metrics_dict.get(b'omega_ratio_usd')) if store_omega else 0.0
                    volatility_val = _aggregated_metric_value_raw(metrics_dict.get(b'equity_volatility_usd')) if store_volatility else 0.0
                    recovery_val = _aggregated_metric_value_raw(metrics_dict.get(b'drawdown_recovery_hours_mean')) if store_recovery else 0.0

                    overall_robustness = 0.0
                    if store_overall_rob and metrics_dict:
                        # Match compute_overall_robustness() semantics without allocating dicts
                        score_sum = 0.0
                        score_n = 0
                        for metric_data in metrics_dict.values():
                            if not isinstance(metric_data, dict):
                                continue
                            mean, std = _mean_std_from_metric_data_raw(metric_data)
                            if abs(mean) > 1e-10:
                                cv = abs(std / mean)
                                score_sum += 1.0 / (1.0 + cv)
                            else:
                                score_sum += 1.0
                            score_n += 1
                        overall_robustness = (score_sum / score_n) if score_n else 0.0

                    constraint_all.append(float(constraint_violation))
                    primary_all.append(float(primary_val))
                    if sharpe_all is not None:
                        sharpe_all.append(float(sharpe_val))
                    if drawdown_all is not None:
                        drawdown_all.append(float(drawdown_val))
                    if calmar_all is not None:
                        calmar_all.append(float(calmar_val))
                    if sortino_all is not None:
                        sortino_all.append(float(sortino_val))
                    if omega_all is not None:
                        omega_all.append(float(omega_val))
                    if volatility_all is not None:
                        volatility_all.append(float(volatility_val))
                    if recovery_all is not None:
                        recovery_all.append(float(recovery_val))
                    if overall_rob_all is not None:
                        overall_rob_all.append(float(overall_robustness))

                key_perf = (constraint_violation, -primary_val, int(idx))

                # Always maintain performance heap for fill.
                _push_candidate(perf_fill_heap, max_configs, idx, key_perf)

                for criterion in strategy:
                    if criterion == 'performance':
                        key = key_perf
                        _push_candidate(heaps[criterion], configs_per_criterion, idx, key)
                    elif criterion == 'robustness':
                        key = (-overall_robustness, constraint_violation, int(idx))
                        _push_candidate(heaps[criterion], configs_per_criterion, idx, key)
                    elif criterion == 'sharpe':
                        key = (constraint_violation, -sharpe_val, int(idx))
                        _push_candidate(heaps[criterion], configs_per_criterion, idx, key)
                    elif criterion == 'drawdown':
                        key = (constraint_violation, -drawdown_val, int(idx))
                        _push_candidate(heaps[criterion], configs_per_criterion, idx, key)
                    elif criterion == 'calmar':
                        key = (constraint_violation, -calmar_val, int(idx))
                        _push_candidate(heaps[criterion], configs_per_criterion, idx, key)
                    elif criterion == 'sortino':
                        key = (constraint_violation, -sortino_val, int(idx))
                        _push_candidate(heaps[criterion], configs_per_criterion, idx, key)
                    elif criterion == 'omega':
                        key = (constraint_violation, -omega_val, int(idx))
                        _push_candidate(heaps[criterion], configs_per_criterion, idx, key)
                    elif criterion == 'volatility':
                        key = (constraint_violation, volatility_val, int(idx))
                        _push_candidate(heaps[criterion], configs_per_criterion, idx, key)
                    elif criterion == 'recovery':
                        key = (constraint_violation, recovery_val, int(idx))
                        _push_candidate(heaps[criterion], configs_per_criterion, idx, key)
                    else:
                        key = key_perf
                        _push_candidate(heaps[criterion], configs_per_criterion, idx, key)

                parsed_count += 1
        except Exception:
            import traceback
            traceback.print_exc()
            return False
        t_scan = time.perf_counter() - t_scan0

        if parsed_count == 0:
            return False

        # Determine selected indices in stable order (same semantics as _select_top_configs_by_strategy)
        t_select0 = time.perf_counter()
        selected_order: List[int] = []
        selected_set: set = set()

        for criterion in strategy:
            heap_items = heaps.get(criterion) or []
            # Sort by original key (ascending = best first)
            for _, cand_idx, _ in sorted(heap_items, key=lambda x: x[2]):
                if cand_idx in selected_set:
                    continue
                selected_set.add(cand_idx)
                selected_order.append(cand_idx)

        # Fill remaining with best-by-performance
        if len(selected_order) < max_configs:
            for _, cand_idx, _ in sorted(perf_fill_heap, key=lambda x: x[2]):
                if len(selected_order) >= max_configs:
                    break
                if cand_idx in selected_set:
                    continue
                selected_set.add(cand_idx)
                selected_order.append(cand_idx)

        # Ensure hard cap
        if len(selected_order) > max_configs:
            selected_order = selected_order[:max_configs]
            selected_set = set(selected_order)

        t_select = time.perf_counter() - t_select0

        # Parse selected configs by offsets (no second full-file pass; no candidate dict caching)
        if progress_callback:
            progress_callback(0, max(1, len(selected_order)), "Parsing selected configs (offsets)...")

        t_parse_selected0 = time.perf_counter()
        pairs = [(int(offsets_all[i]), int(i)) for i in selected_order if 0 <= int(i) < len(offsets_all)]
        pairs.sort(key=lambda x: x[0])

        parsed_selected: Dict[int, ConfigMetrics] = {}
        raw_selected: Dict[int, Dict] = {}
        with open(self.all_results_path, 'rb') as f:
            file_size = os.path.getsize(self.all_results_path)
            for n_done, (off, idx) in enumerate(pairs, 1):
                try:
                    # Prefer decoding from a bounded bytes slice so msgspec can be used.
                    end = int(offsets_all[idx + 1]) if (int(idx) + 1) < len(offsets_all) else int(file_size)
                    if end <= int(off):
                        raise ValueError("invalid offset range")
                    f.seek(int(off))
                    payload = f.read(int(end - int(off)))

                    config_data = self._decode_msgpack_object(payload)

                    # Use precomputed robustness from the scan phase (if available) to skip recomputation here.
                    pre_rob = None
                    if overall_rob_all is not None:
                        try:
                            pre_rob = float(overall_rob_all[idx])
                        except Exception:
                            pre_rob = None

                    if isinstance(config_data, dict) and any(isinstance(k, bytes) for k in config_data.keys()):
                        metrics = self._parse_config_light_raw(idx, config_data, precomputed_overall_robustness=pre_rob)
                    else:
                        metrics = self._parse_config_light(
                            idx,
                            config_data,
                            precomputed_overall_robustness=pre_rob,
                            compute_overall_robustness=False,
                        )

                    parsed_selected[idx] = metrics
                    raw_selected[idx] = config_data
                except Exception:
                    # Fall back to legacy per-object unpack
                    try:
                        f.seek(int(off))
                        unpacker = msgpack.Unpacker(f, raw=True, strict_map_key=False)
                        config_data = next(iter(unpacker))
                        pre_rob = None
                        if overall_rob_all is not None:
                            try:
                                pre_rob = float(overall_rob_all[idx])
                            except Exception:
                                pre_rob = None
                        metrics = self._parse_config_light_raw(idx, config_data, precomputed_overall_robustness=pre_rob)
                        parsed_selected[idx] = metrics
                        raw_selected[idx] = config_data
                    except Exception:
                        continue
                if progress_callback and (n_done % 50 == 0 or n_done == len(pairs)):
                    progress_callback(n_done, len(pairs), f"Parsed {n_done}/{len(pairs)} selected configs")

        self.raw_configs_cache = raw_selected
        self.configs = [parsed_selected[i] for i in selected_order if i in parsed_selected]
        t_parse_selected = time.perf_counter() - t_parse_selected0

        total_parsed = total_parsed
        t_parse = t_scan + t_parse_selected

        # Write persistent scan cache (best-effort)
        if not _DISABLE_SCAN_CACHE:
            try:
                st_bin = os.stat(self.all_results_path)
                cache_meta = {
                    "version": 2,
                    "source": {"size": int(st_bin.st_size), "mtime": float(st_bin.st_mtime)},
                    "scoring_metrics": self.scoring_metrics,
                    "scenario_labels": self.scenario_labels,
                    "optimize_bounds": self.optimize_bounds,
                    "optimize_limits": self.optimize_limits,
                    "backtest_scenarios": self.backtest_scenarios,
                    "has_overall_robustness": bool(store_overall_rob),
                    "cached_fields": [
                        "offsets",
                        "constraint_violation",
                        "primary",
                        *( ["sharpe"] if sharpe_all is not None else [] ),
                        *( ["drawdown"] if drawdown_all is not None else [] ),
                        *( ["calmar"] if calmar_all is not None else [] ),
                        *( ["sortino"] if sortino_all is not None else [] ),
                        *( ["omega"] if omega_all is not None else [] ),
                        *( ["volatility"] if volatility_all is not None else [] ),
                        *( ["recovery"] if recovery_all is not None else [] ),
                        *( ["overall_robustness"] if overall_rob_all is not None else [] ),
                    ],
                }
                cache_arrays = {
                    "offsets": np.asarray(offsets_all, dtype=np.int64),
                    "constraint_violation": np.asarray(constraint_all, dtype=np.float32),
                    "primary": np.asarray(primary_all, dtype=np.float32),
                }
                if sharpe_all is not None:
                    cache_arrays["sharpe"] = np.asarray(sharpe_all, dtype=np.float32)
                if drawdown_all is not None:
                    cache_arrays["drawdown"] = np.asarray(drawdown_all, dtype=np.float32)
                if calmar_all is not None:
                    cache_arrays["calmar"] = np.asarray(calmar_all, dtype=np.float32)
                if sortino_all is not None:
                    cache_arrays["sortino"] = np.asarray(sortino_all, dtype=np.float32)
                if omega_all is not None:
                    cache_arrays["omega"] = np.asarray(omega_all, dtype=np.float32)
                if volatility_all is not None:
                    cache_arrays["volatility"] = np.asarray(volatility_all, dtype=np.float32)
                if recovery_all is not None:
                    cache_arrays["recovery"] = np.asarray(recovery_all, dtype=np.float32)
                if overall_rob_all is not None:
                    cache_arrays["overall_robustness"] = np.asarray(overall_rob_all, dtype=np.float32)
                self._write_scan_cache(cache_meta, cache_arrays)
            except Exception:
                pass

        # Compute Pareto front based on objectives (optimized)
        t_pareto0 = time.perf_counter()
        self._compute_pareto_front_fast()
        t_pareto = time.perf_counter() - t_pareto0

        t_total = time.perf_counter() - t_total0

        # Store stats for GUI display
        self.load_stats = {
            'total_parsed': total_parsed,
            'selected_configs': len(self.configs),
            'pareto_configs': sum(1 for c in self.configs if c.is_pareto),
            'scenarios': self.scenario_labels,
            'scoring_metrics': self.scoring_metrics,
            'load_strategy': load_strategy,
            'max_configs': max_configs,
            'timings': {
                'total': t_total,
                'load_pareto_hashes': t_load_pareto_hashes,
                'parse_all_results': t_parse,
                'scan_all_results': t_scan,
                'parse_selected_configs': t_parse_selected,
                'select_top_configs': t_select,
                'compute_pareto_front': t_pareto,
            },
        }

        return True
    def _select_top_configs_by_strategy(self, all_configs: List[ConfigMetrics], 
                                        strategy: List[str], 
                                        max_count: int = 2000) -> List[ConfigMetrics]:
        """
        Select top configs based on multiple criteria
        
        Args:
            all_configs: List of all parsed configs
            strategy: List of selection criteria
            max_count: Maximum number of configs to keep
        
        Returns:
            Selected configs (deduplicated)
        """
        if len(all_configs) <= max_count:
            return all_configs
        
        primary_metric = self.scoring_metrics[0] if self.scoring_metrics else 'adg_w_usd'
        
        # Calculate how many configs per criterion
        configs_per_criterion = max_count // len(strategy) if strategy else max_count
        
        selected_configs = []  # Use list (ConfigMetrics is not hashable)
        selected_indices = set()  # Track indices to avoid duplicates
        
        for criterion in strategy:
            if criterion == 'performance':
                # Sort by constraint_violation, then primary metric (Passivbot official)
                top = heapq.nsmallest(
                    configs_per_criterion,
                    all_configs,
                    key=lambda c: (
                        c.constraint_violation,
                        -c.suite_metrics.get(primary_metric, float('-inf')),
                        c.config_index,
                    ),
                )
                for config in top:
                    if config.config_index not in selected_indices:
                        selected_configs.append(config)
                        selected_indices.add(config.config_index)
            
            elif criterion == 'robustness':
                # Sort by robustness score (low CV across scenarios)
                configs_with_robustness = [
                    (c, self.compute_overall_robustness(c)) for c in all_configs
                ]
                top = heapq.nsmallest(
                    configs_per_criterion,
                    configs_with_robustness,
                    key=lambda x: (-x[1], x[0].constraint_violation, x[0].config_index),
                )
                for config, _ in top:
                    if config.config_index not in selected_indices:
                        selected_configs.append(config)
                        selected_indices.add(config.config_index)
            
            elif criterion == 'sharpe':
                # Best Sharpe ratio
                top = heapq.nsmallest(
                    configs_per_criterion,
                    all_configs,
                    key=lambda c: (
                        c.constraint_violation,
                        -c.suite_metrics.get('sharpe_ratio_usd', float('-inf')),
                        c.config_index,
                    ),
                )
                for config in top:
                    if config.config_index not in selected_indices:
                        selected_configs.append(config)
                        selected_indices.add(config.config_index)
            
            elif criterion == 'drawdown':
                # Lowest drawdown (higher is better, drawdown is negative)
                top = heapq.nsmallest(
                    configs_per_criterion,
                    all_configs,
                    key=lambda c: (
                        c.constraint_violation,
                        -c.suite_metrics.get('drawdown_worst_usd', float('inf')),
                        c.config_index,
                    ),
                )
                for config in top:
                    if config.config_index not in selected_indices:
                        selected_configs.append(config)
                        selected_indices.add(config.config_index)
            
            elif criterion == 'calmar':
                # Best Calmar ratio
                top = heapq.nsmallest(
                    configs_per_criterion,
                    all_configs,
                    key=lambda c: (
                        c.constraint_violation,
                        -c.suite_metrics.get('calmar_ratio_usd', float('-inf')),
                        c.config_index,
                    ),
                )
                for config in top:
                    if config.config_index not in selected_indices:
                        selected_configs.append(config)
                        selected_indices.add(config.config_index)
            
            elif criterion == 'sortino':
                # Best Sortino ratio
                top = heapq.nsmallest(
                    configs_per_criterion,
                    all_configs,
                    key=lambda c: (
                        c.constraint_violation,
                        -c.suite_metrics.get('sortino_ratio_usd', float('-inf')),
                        c.config_index,
                    ),
                )
                for config in top:
                    if config.config_index not in selected_indices:
                        selected_configs.append(config)
                        selected_indices.add(config.config_index)
            
            elif criterion == 'omega':
                # Best Omega ratio
                top = heapq.nsmallest(
                    configs_per_criterion,
                    all_configs,
                    key=lambda c: (
                        c.constraint_violation,
                        -c.suite_metrics.get('omega_ratio_usd', float('-inf')),
                        c.config_index,
                    ),
                )
                for config in top:
                    if config.config_index not in selected_indices:
                        selected_configs.append(config)
                        selected_indices.add(config.config_index)
            
            elif criterion == 'volatility':
                # Lowest volatility (stable returns)
                top = heapq.nsmallest(
                    configs_per_criterion,
                    all_configs,
                    key=lambda c: (
                        c.constraint_violation,
                        c.suite_metrics.get('equity_volatility_usd', float('inf')),
                        c.config_index,
                    ),
                )
                for config in top:
                    if config.config_index not in selected_indices:
                        selected_configs.append(config)
                        selected_indices.add(config.config_index)
            
            elif criterion == 'recovery':
                # Fastest recovery from drawdowns
                top = heapq.nsmallest(
                    configs_per_criterion,
                    all_configs,
                    key=lambda c: (
                        c.constraint_violation,
                        c.suite_metrics.get('drawdown_recovery_hours_mean', float('inf')),
                        c.config_index,
                    ),
                )
                for config in top:
                    if config.config_index not in selected_indices:
                        selected_configs.append(config)
                        selected_indices.add(config.config_index)
        
        # If we still need more configs to reach max_count, fill with best by performance
        if len(selected_configs) < max_count:
            remaining = max_count - len(selected_configs)
            top = heapq.nsmallest(
                max_count,
                all_configs,
                key=lambda c: (
                    c.constraint_violation,
                    -c.suite_metrics.get(primary_metric, float('-inf')),
                    c.config_index,
                ),
            )
            for config in top:
                if len(selected_configs) >= max_count:
                    break
                if config.config_index not in selected_indices:
                    selected_configs.append(config)
                    selected_indices.add(config.config_index)
        
        return selected_configs
    
    def get_view_slice(self, start_rank: int, end_rank: int) -> List[ConfigMetrics]:
        """
                Get configs in rank range.

                IMPORTANT:
                - This must be fast: it's used by the UI slider.
                - It must not recompute Pareto or mutate `is_pareto` flags.
                    Pareto membership is computed once when loading (based on objectives / pareto hashes).
        
        Args:
            start_rank: First rank to include (0-indexed)
            end_rank: Last rank to include (exclusive, like Python slicing)
        
        Returns:
            List of configs in range
        """
        # Use _full_configs if available (prevents corruption from repeated filtering)
        source_configs = getattr(self, '_full_configs', self.configs)
        
        # Get slice (already sorted by load strategy)
        start_idx = max(0, start_rank)
        end_idx = min(len(source_configs), end_rank)
        
        if start_idx >= end_idx:
            return []
        
        view_configs = source_configs[start_idx:end_idx]

        return view_configs
    
    def _compute_pareto_front_for_configs(self, configs: List[ConfigMetrics]):
        """
        Compute Pareto front for given config list (modifies is_pareto flag in-place)
        Uses efficient Skyline algorithm for large datasets
        
        Args:
            configs: List of configs to compute Pareto front for
        """
        if not configs:
            return
        
        if len(configs) == 1:
            configs[0].is_pareto = True
            return
        
        # Extract objectives (minimize all)
        objective_keys = sorted(configs[0].objectives.keys())
        objectives = np.array([[c.objectives[key] for key in objective_keys] for c in configs])
        n_points = len(configs)
        
        # EFFICIENT SKYLINE ALGORITHM for all dataset sizes
        # Sort by first objective (helps prune dominated points early)
        sorted_indices = np.argsort(objectives[:, 0])
        is_pareto = np.zeros(n_points, dtype=bool)
        
        # Process points in sorted order
        for idx in sorted_indices:
            # Check if current point is dominated by any already-found Pareto point
            is_dominated = False
            
            for pareto_idx in np.where(is_pareto)[0]:
                # Check if pareto_idx dominates idx
                # (all objectives better or equal, at least one strictly better)
                better_equal = np.all(objectives[pareto_idx] <= objectives[idx])
                strictly_better = np.any(objectives[pareto_idx] < objectives[idx])
                
                if better_equal and strictly_better:
                    is_dominated = True
                    break
            
            if not is_dominated:
                is_pareto[idx] = True
        
        # Mark Pareto configs
        for i, config in enumerate(configs):
            config.is_pareto = bool(is_pareto[i])
    
    def load_pareto_jsons_only(self) -> bool:
        """
        Fast load: Load only Pareto configs from pareto/*.json files
        This is much faster than loading all_results.bin for large optimizations.
        
        Returns:
            True if successful, False otherwise
        """
        self.last_error = None

        if self._is_legacy_results_format():
            self.last_error = "Old format not supported"
            return False

        if not os.path.exists(self.pareto_dir):
            return False
        
        t_total0 = time.perf_counter()

        # Reset state
        self.configs = []
        self.raw_configs_cache = {}

        # Find all JSON files in pareto directory
        json_pattern = os.path.join(self.pareto_dir, "*.json")
        json_files = sorted(glob.glob(json_pattern))
        
        if not json_files:
            return False
        
        # Parse each JSON file
        parsed_count = 0
        t_parse0 = time.perf_counter()
        for json_file in json_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    config_data = json.load(f)
                    
                    # Create ConfigMetrics from JSON
                    metrics = self._parse_json_config(config_data, parsed_count)
                    if metrics:
                        metrics.is_pareto = True  # All configs in pareto/ are Pareto-optimal
                        self.configs.append(metrics)
                        parsed_count += 1
            except Exception as e:
                if parsed_count < 3:  # Show first 3 errors
                    import traceback
                    print(f"Error parsing {json_file}:")
                    traceback.print_exc()
                continue

            t_parse = time.perf_counter() - t_parse0
        
        if parsed_count == 0:
            return False
        
        # Extract scenario labels and scoring metrics from first config
        # NOTE: Use optimize.scoring, not all suite_metrics, for consistency with all_results.bin mode
        if self.configs:
            first_config = self.configs[0]
            if first_config.scenario_metrics:
                self.scenario_labels = list(first_config.scenario_metrics.keys())
            # scoring_metrics are already set by _parse_json_config from optimize.scoring
            # Fallback: if not set, use first few metrics from suite_metrics
            if not self.scoring_metrics and first_config.suite_metrics:
                # Use first 3 metrics as default
                self.scoring_metrics = list(first_config.suite_metrics.keys())[:3]
        
        t_total = time.perf_counter() - t_total0

        # Store stats
        self.load_stats = {
            'total_parsed': parsed_count,
            'selected_configs': parsed_count,
            'pareto_configs': parsed_count,  # All are Pareto
            'scenarios': self.scenario_labels,
            'scoring_metrics': self.scoring_metrics,
            'load_strategy': ['pareto_only'],
            'max_configs': parsed_count,
            'timings': {
                'total': t_total,
                'parse_pareto_jsons': t_parse,
                'pareto_json_files': len(json_files),
            },
        }
        
        return True
    
    def _compute_pareto_front_fast(self):
        """Compute Pareto front for the full loaded set.

        - Prefer feasible solutions (`constraint_violation` ~ 0).
        - Compute a true Pareto front using the objective vector stored in `metrics.objectives`.
        - All objectives are treated as minimization (PB7 typically encodes maximization as negative objectives).
        """

        # Reset
        for c in self.configs:
            c.is_pareto = False

        if not self.configs:
            return

        eps_cv = 1e-12
        feasible = [c for c in self.configs if float(getattr(c, "constraint_violation", 0.0) or 0.0) <= eps_cv]
        candidates = feasible
        if not candidates:
            min_cv = min(float(getattr(c, "constraint_violation", 0.0) or 0.0) for c in self.configs)
            candidates = [c for c in self.configs if float(getattr(c, "constraint_violation", 0.0) or 0.0) <= min_cv + eps_cv]

        if not candidates:
            return

        # Determine objective keys
        first_obj = candidates[0].objectives if isinstance(getattr(candidates[0], "objectives", None), dict) else {}
        objective_keys = sorted(first_obj.keys())
        if not objective_keys:
            # No objectives available: best we can do is mark the feasible/min-cv group.
            for c in candidates:
                c.is_pareto = True
            return

        # Filter candidates to those with numeric objective values for all keys.
        valid: List[ConfigMetrics] = []
        obj_rows = []
        for c in candidates:
            o = c.objectives if isinstance(getattr(c, "objectives", None), dict) else {}
            row = []
            ok = True
            for k in objective_keys:
                try:
                    row.append(float(o[k]))
                except Exception:
                    ok = False
                    break
            if ok:
                valid.append(c)
                obj_rows.append(row)

        if not valid:
            for c in candidates:
                c.is_pareto = True
            return

        # 1D objective: Pareto front = minimum objective value.
        if len(objective_keys) == 1:
            vals = [r[0] for r in obj_rows]
            best = min(vals)
            tol = 1e-12 if best == 0 else abs(best) * 1e-12
            for c, v in zip(valid, vals):
                if v <= best + tol:
                    c.is_pareto = True
            return

        objectives = np.asarray(obj_rows, dtype=float)
        n_points = len(valid)
        sorted_indices = np.argsort(objectives[:, 0])
        is_pareto = np.zeros(n_points, dtype=bool)

        # Maintain the current Pareto set explicitly and test dominance in vectorized form.
        # This avoids the Python-level nested loop over previously-marked Pareto indices.
        pareto_objs = np.empty((0, objectives.shape[1]), dtype=float)
        pareto_idxs = np.empty((0,), dtype=np.int32)

        for idx in sorted_indices:
            p = objectives[idx]

            if pareto_objs.shape[0]:
                dominates_p = np.all(pareto_objs <= p, axis=1) & np.any(pareto_objs < p, axis=1)
                if np.any(dominates_p):
                    continue

                dominated_by_p = np.all(p <= pareto_objs, axis=1) & np.any(p < pareto_objs, axis=1)
                if np.any(dominated_by_p):
                    keep = ~dominated_by_p
                    pareto_objs = pareto_objs[keep]
                    pareto_idxs = pareto_idxs[keep]

            pareto_objs = np.vstack([pareto_objs, p])
            pareto_idxs = np.append(pareto_idxs, np.int32(idx))
            is_pareto[idx] = True

        for i, config in enumerate(valid):
            config.is_pareto = bool(is_pareto[i])

    def _iter_binary_file(self, progress_callback=None, with_offsets: bool = False, raw_mode: bool = False) -> Iterator[Any]:
        """
        Stream msgpack objects from all_results.bin (handles multiple packed objects).
        Avoids loading the entire file into memory as a Python list.

        Args:
            progress_callback: Optional callback(current, total, message) for progress updates.
                               In this method, current/total are byte offsets.
        Yields:
            Each unpacked object (dict).
        """
        file_size = os.path.getsize(self.all_results_path)
        with open(self.all_results_path, 'rb') as f:
            unpacker = msgpack.Unpacker(f, raw=bool(raw_mode), strict_map_key=False, read_size=8 * 1024 * 1024)
            count = 0
            last_pos = unpacker.tell()
            for obj in unpacker:
                cur_pos = unpacker.tell()
                count += 1
                if progress_callback and count % 100 == 0:
                    progress = (f.tell() / file_size) if file_size else 1.0
                    progress_callback(
                        f.tell(),
                        file_size,
                        f"Loading binary file: {count:,} configs ({progress*100:.0f}%)",
                    )
                if with_offsets:
                    yield last_pos, obj
                else:
                    yield obj
                last_pos = cur_pos
    
    def _load_binary_file(self, progress_callback=None) -> List[Dict]:
        """
        Load msgpack binary file (handles multiple packed objects)
        
        Args:
            progress_callback: Optional callback for progress updates
        """
        configs: List[Dict] = []

        # Get file size for final progress update
        file_size = os.path.getsize(self.all_results_path)

        for obj in self._iter_binary_file(progress_callback=progress_callback):
            configs.append(obj)

        if progress_callback:
            progress_callback(file_size, file_size, f"Loaded {len(configs)} configs from binary file")

        return configs
    
    def _load_pareto_hashes(self):
        """Load config hashes from pareto/*.json files"""
        if not os.path.exists(self.pareto_dir):
            return
        
        for filename in os.listdir(self.pareto_dir):
            if filename.endswith('.json'):
                config_hash = filename.replace('.json', '')
                self.pareto_hashes.add(config_hash)
    
    def _compute_config_hash(self, config_data: Dict) -> str:
        """
        Compute config hash (simplified - uses results_filename if available)
        In real implementation, would hash the bot parameters
        """
        def _get_field(name: str):
            if not isinstance(config_data, dict):
                return None
            v = config_data.get(name)
            if v is None:
                v = config_data.get(name.encode('utf-8', errors='ignore'))
            if isinstance(v, bytes):
                try:
                    v = v.decode('utf-8', errors='ignore')
                except Exception:
                    v = ''
            return v

        # Prefer a stable, explicit identifier when present.
        results_filename = _get_field('results_filename')
        if isinstance(results_filename, str) and results_filename:
            # Typically something like "<hash>.json" or "<hash>".
            base = os.path.basename(results_filename)
            return base[:-5] if base.endswith('.json') else base

        results_dir = _get_field('results_dir')
        if isinstance(results_dir, str) and results_dir:
            return os.path.basename(results_dir)

        # Fallback: stable hash of bot params.
        # IMPORTANT: must be deterministic across raw msgpack (bytes keys) and decoded dicts.
        bot = _get_field('bot')
        try:
            bot_norm = self._deep_decode_bytes(bot)

            def _to_jsonable(x: Any) -> Any:
                if isinstance(x, dict):
                    # sort keys for deterministic output
                    out = {}
                    for k in sorted(x.keys(), key=lambda kk: str(kk)):
                        out[str(k)] = _to_jsonable(x[k])
                    return out
                if isinstance(x, (list, tuple)):
                    return [_to_jsonable(v) for v in x]
                if isinstance(x, bytes):
                    try:
                        return x.decode('utf-8', errors='ignore')
                    except Exception:
                        return ''
                # normalize numpy scalars etc.
                try:
                    if hasattr(x, 'item') and callable(x.item):
                        return _to_jsonable(x.item())
                except Exception:
                    pass
                if isinstance(x, (str, int, float, bool)) or x is None:
                    return x
                return str(x)

            payload_obj = _to_jsonable(bot_norm)
            payload = json.dumps(payload_obj, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode('utf-8')
            return f"config_{hashlib.sha1(payload).hexdigest()[:12]}"
        except Exception:
            return "config_unknown"

    def _deep_decode_bytes(self, obj: Any) -> Any:
        """Recursively decode bytes keys/values into str.

        Used only on-demand (e.g., ensure_details) to keep load fast.
        """
        if isinstance(obj, dict):
            out: Dict[Any, Any] = {}
            for k, v in obj.items():
                if isinstance(k, bytes):
                    try:
                        k = k.decode('utf-8', errors='ignore')
                    except Exception:
                        k = str(k)
                out[k] = self._deep_decode_bytes(v)
            return out
        if isinstance(obj, list):
            return [self._deep_decode_bytes(x) for x in obj]
        if isinstance(obj, tuple):
            return tuple(self._deep_decode_bytes(x) for x in obj)
        if isinstance(obj, bytes):
            try:
                return obj.decode('utf-8', errors='ignore')
            except Exception:
                return ''
        return obj

    def _parse_config_light_raw(
        self,
        idx: int,
        config_data: Dict,
        precomputed_overall_robustness: Optional[float] = None,
    ) -> ConfigMetrics:
        """Light parse for bytes-keyed msgpack objects (raw=True)."""

        def _get(d: Any, k: bytes, default=None):
            if isinstance(d, dict):
                return d.get(k, default)
            return default

        metrics_block = _get(config_data, b'metrics', {}) or {}
        objectives_raw = _get(metrics_block, b'objectives', {}) or {}
        objectives: Dict[str, float] = {}
        if isinstance(objectives_raw, dict):
            for k, v in objectives_raw.items():
                if isinstance(k, bytes):
                    try:
                        k = k.decode('utf-8', errors='ignore')
                    except Exception:
                        k = str(k)
                try:
                    objectives[str(k)] = float(v or 0.0)
                except Exception:
                    objectives[str(k)] = 0.0

        try:
            constraint_violation = float(_get(metrics_block, b'constraint_violation', 0.0) or 0.0)
        except Exception:
            constraint_violation = 0.0

        if b'suite_metrics' in config_data:
            suite_metrics_block = _get(config_data, b'suite_metrics', {}) or {}
            metrics_dict = _get(suite_metrics_block, b'metrics', {}) or {}
            if not self.scenario_labels:
                raw_labels = _get(suite_metrics_block, b'scenario_labels', []) or []
                if isinstance(raw_labels, list):
                    self.scenario_labels = [
                        (x.decode('utf-8', errors='ignore') if isinstance(x, bytes) else str(x)) for x in raw_labels
                    ]
        else:
            metrics_dict = _get(metrics_block, b'stats', {}) or {}

        suite_metrics: Dict[str, float] = {}
        if isinstance(metrics_dict, dict):
            for metric_name, metric_data in metrics_dict.items():
                if isinstance(metric_name, bytes):
                    try:
                        metric_name = metric_name.decode('utf-8', errors='ignore')
                    except Exception:
                        metric_name = str(metric_name)
                if isinstance(metric_data, dict):
                    aggregated = metric_data.get(b'aggregated', metric_data.get(b'mean', 0.0))
                    try:
                        suite_metrics[str(metric_name)] = float(aggregated or 0.0)
                    except Exception:
                        suite_metrics[str(metric_name)] = 0.0
                else:
                    try:
                        suite_metrics[str(metric_name)] = float(metric_data or 0.0)
                    except Exception:
                        suite_metrics[str(metric_name)] = 0.0

        optimize_settings: Dict[str, Any] = {}
        opt = _get(config_data, b'optimize')
        if isinstance(opt, dict):
            scoring = opt.get(b'scoring', [])
            if isinstance(scoring, list):
                scoring = [(x.decode('utf-8', errors='ignore') if isinstance(x, bytes) else x) for x in scoring]
            optimize_settings = {
                'scoring': scoring or [],
                'limits': opt.get(b'limits', []) or [],
                'iters': opt.get(b'iters', 0) or 0,
                'population_size': opt.get(b'population_size', 0) or 0,
                'pareto_max_size': opt.get(b'pareto_max_size', 0) or 0,
            }

        scenario_details = None
        backtest = _get(config_data, b'backtest')
        if isinstance(backtest, dict):
            suite = backtest.get(b'suite')
            if isinstance(suite, dict) and suite.get(b'enabled'):
                scenario_details = suite.get(b'scenarios', []) or []

        config_hash = self._compute_config_hash(config_data)
        is_pareto = config_hash in self.pareto_hashes

        overall_robustness = float(precomputed_overall_robustness or 0.0)

        return ConfigMetrics(
            config_index=idx,
            config_hash=config_hash,
            objectives=objectives,
            constraint_violation=constraint_violation,
            suite_metrics=suite_metrics,
            scenario_metrics={},
            robustness_scores={},
            bot_params={},
            bounds=self.optimize_bounds,
            optimize_settings=optimize_settings,
            scenario_details=scenario_details,
            metric_stats={},
            is_pareto=is_pareto,
            overall_robustness=overall_robustness,
            details_loaded=False,
            bot_params_loaded=False,
        )
    
    def get_full_config(self, config_index: int) -> Optional[Dict]:
        """
        Get full config for a specific index
        
        Two modes:
        1. Fast mode (pareto JSONs only): Load JSON file directly at sorted index
        2. All results mode: Merge all_results.bin[index] with pareto JSON template
        
        Args:
            config_index: Index in self.configs list (0-based)
            
        Returns:
            Complete config dict with all sections (backtest, bot, optimize, live, etc.)
            or None if not found
        """
        # Check cache first
        if config_index in self.pareto_configs_cache:
            return self.pareto_configs_cache[config_index]
        
        try:
            import json
            
            # Get sorted pareto JSON files
            pareto_files = sorted([f for f in os.listdir(self.pareto_dir) if f.endswith('.json')])
            if not pareto_files:
                return None
            
            # FAST MODE: Configs loaded from pareto/*.json
            # In this mode, config_index is the position in sorted pareto files
            # Check if we loaded from JSONs (load_stats has 'pareto_only' strategy)
            load_strategy = self.load_stats.get('load_strategy', [])
            if 'pareto_only' in load_strategy or len(self.configs) == len(pareto_files):
                # Fast mode: Load JSON directly at this index
                if 0 <= config_index < len(pareto_files):
                    json_file = os.path.join(self.pareto_dir, pareto_files[config_index])
                    with open(json_file, 'r') as f:
                        full_config = json.load(f)
                    self.pareto_configs_cache[config_index] = full_config
                    return full_config
                return None
            
            # ALL RESULTS MODE: Configs loaded from all_results.bin
            # Load template from any pareto JSON (they share same base structure)
            template_file = os.path.join(self.pareto_dir, pareto_files[0])
            with open(template_file, 'r') as f:
                full_config = json.load(f)
            
            # Get config_data from cache (avoids re-reading entire file!)
            config_data = self.raw_configs_cache.get(config_index)
            if config_data:
                # Merge bot params (keep missing params from pareto template)
                bot_section = None
                if isinstance(config_data, dict):
                    bot_section = config_data.get('bot')
                    if bot_section is None:
                        bot_section = config_data.get(b'bot')

                if isinstance(bot_section, dict):
                    for side in ['long', 'short']:
                        side_dict = bot_section.get(side)
                        if side_dict is None:
                            side_dict = bot_section.get(side.encode('utf-8'))
                        if not isinstance(side_dict, dict):
                            continue

                        decoded_side: Dict[str, Any] = {}
                        for k, v in side_dict.items():
                            if isinstance(k, bytes):
                                try:
                                    k = k.decode('utf-8', errors='ignore')
                                except Exception:
                                    k = str(k)
                            decoded_side[str(k)] = v

                        if side in full_config.get('bot', {}):
                            full_config['bot'][side].update(decoded_side)
                        else:
                            full_config.setdefault('bot', {})
                            full_config['bot'][side] = decoded_side
                
                # Cache and return
                self.pareto_configs_cache[config_index] = full_config
                return full_config
            
            return None
            
        except Exception as e:
            import traceback
            print(f"Error loading full config for index {config_index}: {e}")
            traceback.print_exc()
            return None

    def _parse_config_light(
        self,
        idx: int,
        config_data: Dict,
        precomputed_overall_robustness: Optional[float] = None,
        compute_overall_robustness: bool = True,
    ) -> ConfigMetrics:
        """Parse a config cheaply for fast initial UI rendering.

        Keeps:
        - objectives / constraint_violation
        - suite_metrics (aggregated)
        - overall_robustness scalar (computed from mean/std if present)

        Defers:
        - scenario_metrics
        - metric_stats
        - robustness_scores (per-metric)
        - bot_params
        """
        metrics_block = config_data.get('metrics', {}) or {}
        objectives = metrics_block.get('objectives', {}) or {}
        try:
            constraint_violation = float(metrics_block.get('constraint_violation', 0.0) or 0.0)
        except Exception:
            constraint_violation = 0.0

        if 'suite_metrics' in config_data:
            suite_metrics_block = config_data.get('suite_metrics', {}) or {}
            metrics_dict = suite_metrics_block.get('metrics', {}) or {}
            if not self.scenario_labels:
                self.scenario_labels = suite_metrics_block.get('scenario_labels', []) or []
        else:
            metrics_dict = metrics_block.get('stats', {}) or {}

        if not self.scoring_metrics and 'optimize' in config_data:
            optimize_config = config_data.get('optimize', {}) or {}
            self.scoring_metrics = optimize_config.get('scoring', []) or []
            if not self.optimize_bounds:
                self.optimize_bounds = self._normalize_optimize_bounds(optimize_config.get('bounds', {}) or {})
                self.optimize_limits = optimize_config.get('limits', []) or []

        if not self.backtest_scenarios and 'backtest' in config_data:
            backtest_config = config_data.get('backtest', {}) or {}
            suite_config = backtest_config.get('suite', {}) or {}
            if suite_config.get('enabled'):
                self.backtest_scenarios = suite_config.get('scenarios', []) or []

        suite_metrics: Dict[str, float] = {}
        do_robustness = bool(compute_overall_robustness) and precomputed_overall_robustness is None
        score_sum = 0.0
        score_n = 0

        for metric_name, metric_data in metrics_dict.items():
            if isinstance(metric_data, dict):
                aggregated = metric_data.get('aggregated', metric_data.get('mean', 0.0))
                try:
                    suite_metrics[metric_name] = float(aggregated or 0.0)
                except Exception:
                    suite_metrics[metric_name] = 0.0

                if do_robustness:
                    if 'stats' in metric_data and isinstance(metric_data.get('stats'), dict):
                        stats = metric_data.get('stats', {}) or {}
                        mean = stats.get('mean', 0.0)
                        std = stats.get('std', 0.0)
                    else:
                        mean = metric_data.get('mean', 0.0)
                        std = metric_data.get('std', 0.0)

                    try:
                        mean_f = float(mean or 0.0)
                        std_f = float(std or 0.0)
                    except Exception:
                        mean_f = 0.0
                        std_f = 0.0

                    if abs(mean_f) > 1e-10:
                        cv = abs(std_f / mean_f)
                        score_sum += 1.0 / (1.0 + cv)
                    else:
                        score_sum += 1.0
                    score_n += 1
            else:
                # Non-dict format: treat as already-aggregated
                try:
                    suite_metrics[metric_name] = float(metric_data or 0.0)
                except Exception:
                    suite_metrics[metric_name] = 0.0

        if precomputed_overall_robustness is not None:
            overall_robustness = float(precomputed_overall_robustness)
        elif do_robustness:
            overall_robustness = (score_sum / score_n) if score_n else 0.0
        else:
            overall_robustness = 0.0

        optimize_settings: Dict[str, Any] = {}
        if 'optimize' in config_data:
            opt = config_data.get('optimize', {}) or {}
            optimize_settings = {
                'scoring': opt.get('scoring', []) or [],
                'limits': opt.get('limits', []) or [],
                'iters': opt.get('iters', 0) or 0,
                'population_size': opt.get('population_size', 0) or 0,
                'pareto_max_size': opt.get('pareto_max_size', 0) or 0,
            }

        scenario_details = None
        if 'backtest' in config_data:
            suite_config = (config_data.get('backtest', {}) or {}).get('suite', {}) or {}
            if suite_config.get('enabled'):
                scenario_details = suite_config.get('scenarios', []) or []

        config_hash = self._compute_config_hash(config_data)
        is_pareto = config_hash in self.pareto_hashes

        return ConfigMetrics(
            config_index=idx,
            config_hash=config_hash,
            objectives=objectives,
            constraint_violation=constraint_violation,
            suite_metrics=suite_metrics,
            scenario_metrics={},
            robustness_scores={},
            bot_params={},
            bounds=self.optimize_bounds,
            optimize_settings=optimize_settings,
            scenario_details=scenario_details,
            metric_stats={},
            is_pareto=is_pareto,
            overall_robustness=float(overall_robustness),
            details_loaded=False,
            bot_params_loaded=False,
        )

    def ensure_bot_params(self, config: ConfigMetrics) -> None:
        """Populate bot params for a config (without parsing scenario details)."""
        if config.bot_params_loaded:
            return
        config_data = self.raw_configs_cache.get(config.config_index)
        if not isinstance(config_data, dict):
            return

        bot_params: Dict[str, Any] = {}
        bot_config = config_data.get('bot')
        if bot_config is None:
            bot_config = config_data.get(b'bot')
        bot_config = bot_config or {}
        if isinstance(bot_config, dict):
            long_cfg = bot_config.get('long')
            if long_cfg is None:
                long_cfg = bot_config.get(b'long')
            if isinstance(long_cfg, dict):
                for param, value in long_cfg.items():
                    if isinstance(param, bytes):
                        try:
                            param = param.decode('utf-8', errors='ignore')
                        except Exception:
                            param = str(param)
                    bot_params[f'long_{param}'] = value
            short_cfg = bot_config.get('short')
            if short_cfg is None:
                short_cfg = bot_config.get(b'short')
            if isinstance(short_cfg, dict):
                for param, value in short_cfg.items():
                    if isinstance(param, bytes):
                        try:
                            param = param.decode('utf-8', errors='ignore')
                        except Exception:
                            param = str(param)
                    bot_params[f'short_{param}'] = value

        config.bot_params = bot_params
        config.bot_params_loaded = True

    def ensure_details(self, config: ConfigMetrics) -> None:
        """Populate scenario_metrics/metric_stats/robustness_scores (expensive)."""
        if config.details_loaded:
            return
        config_data = self.raw_configs_cache.get(config.config_index)
        if not isinstance(config_data, dict):
            return

        # raw=True cache stores bytes keys/values; decode only when details are requested.
        if isinstance(config_data, dict) and any(isinstance(k, bytes) for k in config_data.keys()):
            config_data = self._deep_decode_bytes(config_data)
        full = self._parse_config(config.config_index, config_data)
        config.objectives = full.objectives
        config.constraint_violation = full.constraint_violation
        config.suite_metrics = full.suite_metrics
        config.scenario_metrics = full.scenario_metrics
        config.robustness_scores = full.robustness_scores
        config.bot_params = full.bot_params
        config.bot_params_loaded = True
        config.bounds = full.bounds
        config.optimize_settings = full.optimize_settings
        config.scenario_details = full.scenario_details
        config.metric_stats = full.metric_stats
        config.config_hash = full.config_hash
        config.is_pareto = full.is_pareto
        config.overall_robustness = full.overall_robustness
        config.details_loaded = True
    
    def _parse_config(self, idx: int, config_data: Dict) -> ConfigMetrics:
        """Parse a single config from all_results.bin"""
        
        # Extract objectives
        metrics_block = config_data.get('metrics', {})
        objectives = metrics_block.get('objectives', {})
        constraint_violation = metrics_block.get('constraint_violation', 0.0)
        
        # Extract suite metrics - support both suite and non-suite formats
        # Suite format: {"suite_metrics": {"metrics": {...}}}
        # Non-suite format: {"metrics": {"stats": {...}, "objectives": {...}}}
        if 'suite_metrics' in config_data:
            suite_metrics_block = config_data.get('suite_metrics', {})
            metrics_dict = suite_metrics_block.get('metrics', {})
            # Store scenario labels (first time only)
            if not self.scenario_labels:
                self.scenario_labels = suite_metrics_block.get('scenario_labels', [])
        else:
            # Non-suite format: metrics.stats contains the actual metrics
            metrics_block = config_data.get('metrics', {})
            metrics_dict = metrics_block.get('stats', {})
        
        # Extract scoring metrics from optimize config (first time only)
        if not self.scoring_metrics and 'optimize' in config_data:
            optimize_config = config_data['optimize']
            self.scoring_metrics = optimize_config.get('scoring', [])
            
            # Extract global optimize settings (first time only)
            if not self.optimize_bounds:
                self.optimize_bounds = self._normalize_optimize_bounds(optimize_config.get('bounds', {}) or {})
                self.optimize_limits = optimize_config.get('limits', [])
        
        # Extract backtest scenarios (first time only)
        if not self.backtest_scenarios and 'backtest' in config_data:
            backtest_config = config_data['backtest']
            suite_config = backtest_config.get('suite', {})
            if suite_config.get('enabled'):
                self.backtest_scenarios = suite_config.get('scenarios', [])
        
        # Parse aggregated suite metrics
        suite_metrics = {}
        scenario_metrics = {}
        robustness_scores = {}
        metric_stats = {}
        
        for metric_name, metric_data in metrics_dict.items():
            if isinstance(metric_data, dict):
                # Try to get aggregated value, fallback to mean if not present
                aggregated = metric_data.get('aggregated', metric_data.get('mean', 0.0))
                suite_metrics[metric_name] = aggregated
                
                # Stats - either from nested 'stats' dict or direct dict values
                if 'stats' in metric_data:
                    stats = metric_data.get('stats', {})
                    std = stats.get('std', 0.0)
                    mean = stats.get('mean', 0.0)
                    min_val = stats.get('min', 0.0)
                    max_val = stats.get('max', 0.0)
                else:
                    # Direct format: {"mean": X, "std": Y, ...}
                    std = metric_data.get('std', 0.0)
                    mean = metric_data.get('mean', 0.0)
                    min_val = metric_data.get('min', 0.0)
                    max_val = metric_data.get('max', 0.0)
                
                # Store full stats
                metric_stats[metric_name] = {
                    'mean': mean,
                    'std': std,
                    'min': min_val,
                    'max': max_val
                }
                
                # Robustness score = 1 / CV (Coefficient of Variation)
                # Lower CV = more robust (consistent across scenarios)
                if abs(mean) > 1e-10:
                    cv = abs(std / mean)
                    robustness_scores[metric_name] = 1.0 / (1.0 + cv)  # Normalize to [0, 1]
                else:
                    robustness_scores[metric_name] = 1.0
                
                # Scenario-specific values
                scenarios = metric_data.get('scenarios', {})
                for scenario_label, scenario_value in scenarios.items():
                    if scenario_label not in scenario_metrics:
                        scenario_metrics[scenario_label] = {}
                    scenario_metrics[scenario_label][metric_name] = scenario_value
        
        # Extract bot parameters
        bot_params = {}
        bot_config = config_data.get('bot', {})
        if 'long' in bot_config:
            for param, value in bot_config['long'].items():
                bot_params[f'long_{param}'] = value
        if 'short' in bot_config:
            for param, value in bot_config['short'].items():
                bot_params[f'short_{param}'] = value
        
        # Extract optimize settings for this config
        optimize_settings = {}
        if 'optimize' in config_data:
            opt = config_data['optimize']
            optimize_settings = {
                'scoring': opt.get('scoring', []),
                'limits': opt.get('limits', []),
                'iters': opt.get('iters', 0),
                'population_size': opt.get('population_size', 0),
                'pareto_max_size': opt.get('pareto_max_size', 0),
            }
        
        # Extract scenario details
        scenario_details = None
        if 'backtest' in config_data:
            suite_config = config_data['backtest'].get('suite', {})
            if suite_config.get('enabled'):
                scenario_details = suite_config.get('scenarios', [])
        
        # Compute config hash
        config_hash = self._compute_config_hash(config_data)
        is_pareto = config_hash in self.pareto_hashes

        overall_robustness = float(np.mean(list(robustness_scores.values()))) if robustness_scores else 0.0
        
        return ConfigMetrics(
            config_index=idx,
            config_hash=config_hash,
            objectives=objectives,
            constraint_violation=constraint_violation,
            suite_metrics=suite_metrics,
            scenario_metrics=scenario_metrics,
            robustness_scores=robustness_scores,
            bot_params=bot_params,
            bounds=self.optimize_bounds,
            optimize_settings=optimize_settings,
            scenario_details=scenario_details,
            metric_stats=metric_stats,
            is_pareto=is_pareto,
            overall_robustness=overall_robustness,
            details_loaded=True,
            bot_params_loaded=True,
        )
    
    def _parse_json_config(self, config_data: Dict, idx: int) -> Optional[ConfigMetrics]:
        """
        Parse a single config from pareto/*.json file
        Similar to _parse_config but adapted for JSON format
        """
        try:
            # Extract objectives
            metrics_block = config_data.get('metrics', {})
            objectives = metrics_block.get('objectives', {})
            constraint_violation = metrics_block.get('constraint_violation', 0.0)
            
            # Extract suite metrics - support both suite and non-suite formats
            if 'suite_metrics' in config_data:
                suite_metrics_block = config_data.get('suite_metrics', {})
                metrics_dict = suite_metrics_block.get('metrics', {})
                # Store scenario labels (first time only)
                if not self.scenario_labels:
                    self.scenario_labels = suite_metrics_block.get('scenario_labels', [])
            else:
                # Non-suite format
                metrics_dict = metrics_block.get('stats', {})
            
            # Extract scoring metrics (first time only)
            if not self.scoring_metrics and 'optimize' in config_data:
                optimize_config = config_data['optimize']
                self.scoring_metrics = optimize_config.get('scoring', [])
                
                # Extract global optimize settings
                if not self.optimize_bounds:
                    self.optimize_bounds = self._normalize_optimize_bounds(optimize_config.get('bounds', {}) or {})
                    self.optimize_limits = optimize_config.get('limits', [])
            
            # Extract backtest scenarios (first time only)
            if not self.backtest_scenarios and 'backtest' in config_data:
                backtest_config = config_data['backtest']
                suite_config = backtest_config.get('suite', {})
                if suite_config.get('enabled'):
                    self.backtest_scenarios = suite_config.get('scenarios', [])
            
            # Parse metrics (same logic as _parse_config)
            suite_metrics = {}
            scenario_metrics = {}
            robustness_scores = {}
            metric_stats = {}
            
            for metric_name, metric_data in metrics_dict.items():
                if isinstance(metric_data, dict):
                    aggregated = metric_data.get('aggregated', metric_data.get('mean', 0.0))
                    suite_metrics[metric_name] = aggregated
                    
                    # Stats
                    if 'stats' in metric_data:
                        stats = metric_data.get('stats', {})
                        std = stats.get('std', 0.0)
                        mean = stats.get('mean', 0.0)
                        min_val = stats.get('min', 0.0)
                        max_val = stats.get('max', 0.0)
                    else:
                        # Fallback: use direct dict values
                        std = metric_data.get('std', 0.0)
                        mean = metric_data.get('mean', aggregated)
                        min_val = metric_data.get('min', aggregated)
                        max_val = metric_data.get('max', aggregated)
                    
                    metric_stats[metric_name] = {
                        'mean': mean,
                        'std': std,
                        'min': min_val,
                        'max': max_val
                    }
                    
                    # Robustness score: 1/(1+CV) normalized to [0,1]
                    # Higher score = more consistent across scenarios
                    if std > 0 and abs(mean) > 1e-10:
                        cv = abs(std / mean)
                        robustness_scores[metric_name] = 1.0 / (1.0 + cv)
                    else:
                        robustness_scores[metric_name] = 1.0  # Perfect consistency
                        robustness_scores[metric_name] = 1.0  # Perfect consistency
                    
                    # Scenario-specific metrics
                    scenarios = metric_data.get('scenarios', {})
                    for scenario_name, scenario_value in scenarios.items():
                        if scenario_name not in scenario_metrics:
                            scenario_metrics[scenario_name] = {}
                        scenario_metrics[scenario_name][metric_name] = scenario_value
                else:
                    # Simple value
                    suite_metrics[metric_name] = metric_data
            
            # Extract bot parameters
            bot_params = {}
            bot_config = config_data.get('bot', {})
            if 'long' in bot_config:
                for param, value in bot_config['long'].items():
                    bot_params[f'long_{param}'] = value
            if 'short' in bot_config:
                for param, value in bot_config['short'].items():
                    bot_params[f'short_{param}'] = value
            
            # Extract optimize settings
            optimize_settings = {}
            if 'optimize' in config_data:
                opt = config_data['optimize']
                optimize_settings = {
                    'scoring': opt.get('scoring', []),
                    'limits': opt.get('limits', []),
                    'iters': opt.get('iters', 0),
                    'population_size': opt.get('population_size', 0),
                    'pareto_max_size': opt.get('pareto_max_size', 0),
                }
            
            # Extract scenario details
            scenario_details = None
            if 'backtest' in config_data:
                suite_config = config_data['backtest'].get('suite', {})
                if suite_config.get('enabled'):
                    scenario_details = suite_config.get('scenarios', [])
            
            # Compute config hash
            config_hash = self._compute_config_hash(config_data)

            overall_robustness = float(np.mean(list(robustness_scores.values()))) if robustness_scores else 0.0
            
            return ConfigMetrics(
                config_index=idx,
                config_hash=config_hash,
                objectives=objectives,
                constraint_violation=constraint_violation,
                suite_metrics=suite_metrics,
                scenario_metrics=scenario_metrics,
                robustness_scores=robustness_scores,
                bot_params=bot_params,
                bounds=self.optimize_bounds,
                optimize_settings=optimize_settings,
                scenario_details=scenario_details,
                metric_stats=metric_stats,
                is_pareto=True,  # All JSON configs are Pareto
                overall_robustness=overall_robustness,
                details_loaded=True,
                bot_params_loaded=True,
            )
        
        except Exception as e:
            import traceback
            print(f"Error parsing JSON config at index {idx}:")
            traceback.print_exc()
            return None
    
    def get_pareto_configs(self) -> List[ConfigMetrics]:
        """Get only Pareto-optimal configs"""
        return [c for c in self.configs if c.is_pareto]
    
    def get_all_configs(self) -> List[ConfigMetrics]:
        """Get all configs"""
        return self.configs
    
    def get_config_by_index(self, config_index: int) -> Optional[ConfigMetrics]:
        """Get a config by its config_index"""
        return next((c for c in self.configs if c.config_index == config_index), None)
    
    def to_dataframe(self, pareto_only: bool = False) -> pd.DataFrame:
        """
        Convert configs to pandas DataFrame for easy analysis
        
        Args:
            pareto_only: If True, only return Pareto configs
        
        Returns:
            DataFrame with all metrics and parameters
        """
        configs = self.get_pareto_configs() if pareto_only else self.get_all_configs()
        
        if not configs:
            return pd.DataFrame()
        
        rows = []
        for config in configs:
            # Export should include rich fields; keep this lazy to speed up initial load.
            if not config.details_loaded:
                self.ensure_details(config)
            row = {
                'config_index': config.config_index,
                'config_hash': config.config_hash,
                'is_pareto': config.is_pareto,
                'constraint_violation': config.constraint_violation,
            }
            
            # Add objectives
            for key, value in config.objectives.items():
                row[f'obj_{key}'] = value
            
            # Add suite metrics
            row.update(config.suite_metrics)
            
            # Add robustness scores
            for metric, score in config.robustness_scores.items():
                row[f'robust_{metric}'] = score
            
            # Add bot parameters
            if not config.bot_params_loaded or not config.bot_params:
                self.ensure_bot_params(config)
            row.update(config.bot_params)
            
            # Add scenario-specific metrics (optional)
            for scenario, metrics in config.scenario_metrics.items():
                for metric, value in metrics.items():
                    row[f'scenario_{scenario}_{metric}'] = value
            
            rows.append(row)
        
        return pd.DataFrame(rows)
    
    def get_metric_summary(self, metric_name: str) -> Dict[str, float]:
        """
        Get summary statistics for a specific metric across all configs
        
        Args:
            metric_name: Name of the metric
        
        Returns:
            Dict with min, max, mean, std, median
        """
        values = [c.suite_metrics.get(metric_name, 0.0) for c in self.configs]
        values = [v for v in values if v is not None]
        
        if not values:
            return {}
        
        return {
            'min': float(np.min(values)),
            'max': float(np.max(values)),
            'mean': float(np.mean(values)),
            'std': float(np.std(values)),
            'median': float(np.median(values)),
            'count': len(values)
        }
    
    def get_top_configs(self, metric_name: str, n: int = 10, 
                       ascending: bool = False, pareto_only: bool = False) -> List[ConfigMetrics]:
        """
        Get top N configs sorted by a specific metric
        
        Args:
            metric_name: Metric to sort by
            n: Number of configs to return
            ascending: If True, sort ascending (lower is better)
            pareto_only: If True, only consider Pareto configs
        
        Returns:
            List of top N configs
        """
        configs = self.get_pareto_configs() if pareto_only else self.get_all_configs()
        
        # Sort by metric
        sorted_configs = sorted(
            configs,
            key=lambda c: c.suite_metrics.get(metric_name, float('-inf') if ascending else float('inf')),
            reverse=not ascending
        )
        
        return sorted_configs[:n]
    
    def compute_overall_robustness(self, config: ConfigMetrics) -> float:
        """
        Compute overall robustness score for a config
        Average of all individual robustness scores
        
        Args:
            config: ConfigMetrics instance
        
        Returns:
            Overall robustness score [0, 1] (higher is more robust)
        """
        if config.robustness_scores:
            scores = list(config.robustness_scores.values())
            return float(np.mean(scores))
        return float(getattr(config, 'overall_robustness', 0.0) or 0.0)
    
    def find_similar_configs(self, reference_config: ConfigMetrics, 
                            n: int = 5, 
                            param_subset: Optional[List[str]] = None) -> List[Tuple[ConfigMetrics, float]]:
        """
        Find configs similar to a reference config based on parameter distance
        
        Args:
            reference_config: Reference config to compare against
            n: Number of similar configs to return
            param_subset: Optional list of parameters to consider (default: all)
        
        Returns:
            List of (config, distance) tuples, sorted by distance
        """
        if param_subset is None:
            param_subset = list(reference_config.bot_params.keys())
        
        distances = []
        for config in self.configs:
            if config.config_index == reference_config.config_index:
                continue
            
            # Compute euclidean distance in parameter space
            dist = 0.0
            for param in param_subset:
                ref_val = reference_config.bot_params.get(param, 0.0)
                cfg_val = config.bot_params.get(param, 0.0)
                dist += (ref_val - cfg_val) ** 2
            
            dist = np.sqrt(dist)
            distances.append((config, dist))
        
        # Sort by distance and return top N
        distances.sort(key=lambda x: x[1])
        return distances[:n]
    
    def get_parameters_at_bounds(self, tolerance: float = 0.05, top_n: int = 10) -> Dict[str, Dict]:
        """
        Find parameters that are at or near bounds limits in TOP performing configs
        
        Args:
            tolerance: How close to bounds (0.05 = within 5% of limit)
            top_n: Only check top N configs by composite score (default: 10)
        
        Returns:
            Dict with 'at_lower', 'at_upper' dicts containing param info
            Example: {'at_lower': {'param_name': {'value': 0.01, 'bound': 0.01}}, ...}
        """
        at_lower = {}
        at_upper = {}
        within_range = []
        
        # Get top N Pareto configs by composite score (performance × robustness)
        pareto_configs = self.get_pareto_configs()
        if not pareto_configs:
            return {'at_lower': {}, 'at_upper': {}, 'within_range': []}
        
        # Sort by composite score
        primary_metric = self.scoring_metrics[0] if self.scoring_metrics else 'adg_w_usd'
        scored_configs = [
            (c, c.suite_metrics.get(primary_metric, 0) * self.compute_overall_robustness(c))
            for c in pareto_configs
        ]
        scored_configs.sort(key=lambda x: x[1], reverse=True)
        
        # Take only top N
        top_configs = [c for c, score in scored_configs[:top_n]]

        def _bounds_to_pair(v):
            """Return (lower, upper) as floats for various bounds formats.

            PB7 optimize bounds may be stored as:
            - [lower, upper]
            - [lower, upper, step]
            - (lower, upper)
            - {'lower': x, 'upper': y} / {'min': x, 'max': y}
            """
            if v is None:
                return None
            if isinstance(v, dict):
                for lo_key, hi_key in (("lower", "upper"), ("min", "max"), ("lo", "hi")):
                    if lo_key in v and hi_key in v:
                        try:
                            return float(v[lo_key]), float(v[hi_key])
                        except Exception:
                            return None
                if "bounds" in v:
                    return _bounds_to_pair(v.get("bounds"))
                return None
            if isinstance(v, (list, tuple)):
                if len(v) < 2:
                    return None
                # Common PB7 format: [lower, upper, step]
                try:
                    return float(v[0]), float(v[1])
                except Exception:
                    return None
            return None

        # Parameter analysis requires bot params
        for cfg in top_configs:
            self.ensure_bot_params(cfg)
        
        # Check if long/short are enabled by looking at key parameters
        long_np = _bounds_to_pair(self.optimize_bounds.get('long_n_positions')) or (0.0, 0.0)
        long_twel = _bounds_to_pair(self.optimize_bounds.get('long_total_wallet_exposure_limit')) or (0.0, 0.0)
        short_np = _bounds_to_pair(self.optimize_bounds.get('short_n_positions')) or (0.0, 0.0)
        short_twel = _bounds_to_pair(self.optimize_bounds.get('short_total_wallet_exposure_limit')) or (0.0, 0.0)
        long_enabled = (long_np[1] > 0) or (long_twel[1] > 0)
        short_enabled = (short_np[1] > 0) or (short_twel[1] > 0)
        
        for param_name, bounds in self.optimize_bounds.items():
            pair = _bounds_to_pair(bounds)
            if not pair:
                continue
            lower, upper = pair
            if lower > upper:
                lower, upper = upper, lower
            # Skip disabled side (long/short)
            if param_name.startswith('long_') and not long_enabled:
                continue
            if param_name.startswith('short_') and not short_enabled:
                continue
            
            # Skip parameters where bounds are both 0 (disabled)
            if lower == 0 and upper == 0:
                continue
            
            param_range = upper - lower
            
            # Skip if range is too small (essentially disabled)
            if param_range < 1e-10:
                continue
            
            threshold_lower = lower + (param_range * tolerance)
            threshold_upper = upper - (param_range * tolerance)
            
            # Check TOP configs only
            values = [c.bot_params.get(param_name, 0) for c in top_configs if param_name in c.bot_params]
            
            if not values:
                continue
            
            min_val = min(values)
            max_val = max(values)
            
            if min_val <= threshold_lower:
                at_lower[param_name] = {'value': min_val, 'bound': lower}
            elif max_val >= threshold_upper:
                at_upper[param_name] = {'value': max_val, 'bound': upper}
            else:
                within_range.append(param_name)
        
        return {
            'at_lower': at_lower,
            'at_upper': at_upper,
            'within_range': within_range
        }
    
    def get_special_metrics(self) -> Dict[str, List[str]]:
        """
        Categorize metrics by type for easier visualization
        
        Returns:
            Dict with categorized metric lists
        """
        all_metrics = list(self.configs[0].suite_metrics.keys()) if self.configs else []
        
        categories = {
            'performance': [],
            'risk': [],
            'quality': [],
            'activity': [],
            'risk_adjusted': []
        }
        
        for metric in all_metrics:
            metric_lower = metric.lower()
            
            # Performance metrics
            if any(x in metric_lower for x in ['adg', 'gain', 'profit']):
                categories['performance'].append(metric)
            
            # Risk metrics
            elif any(x in metric_lower for x in ['drawdown', 'loss', 'shortfall', 'equity_balance_diff_neg']):
                categories['risk'].append(metric)
            
            # Quality metrics (equity curve quality)
            elif any(x in metric_lower for x in ['choppiness', 'jerkiness', 'exponential_fit']):
                categories['quality'].append(metric)
            
            # Activity metrics
            elif any(x in metric_lower for x in ['position', 'volume', 'held', 'recovery_hours']):
                categories['activity'].append(metric)
            
            # Risk-adjusted metrics
            elif any(x in metric_lower for x in ['sharpe', 'sortino', 'calmar', 'omega', 'sterling']):
                categories['risk_adjusted'].append(metric)
        
        return categories
    
    def compute_trading_style(self, config: ConfigMetrics) -> str:
        """
        Classify config into trading style based on activity metrics
        
        Args:
            config: ConfigMetrics instance
        
        Returns:
            Style label: "Sniper", "Scalper", "Balanced", "Idle"
        """
        positions_per_day = config.suite_metrics.get('positions_held_per_day', 0)
        holding_hours_mean = config.suite_metrics.get('position_held_hours_mean', 0)
        volume_pct = config.suite_metrics.get('volume_pct_per_day_avg', 0)
        
        # Classification logic
        if positions_per_day < 0.5 and holding_hours_mean > 20:
            return "🎯 Sniper (Low frequency, long holds)"
        elif positions_per_day > 2 and holding_hours_mean < 5:
            return "⚡ Scalper (High frequency, quick trades)"
        elif volume_pct < 0.2:
            return "💤 Idle (Very low activity)"
        else:
            return "⚖️ Balanced (Moderate trading)"
    
    def compute_risk_profile_score(self, config: ConfigMetrics) -> Dict[str, float]:
        """
        Compute multi-dimensional risk profile
        
        Args:
            config: ConfigMetrics instance
        
        Returns:
            Dict with risk scores [0-10] for different dimensions
        """
        # Extract key risk metrics (normalize to 0-10 scale)
        dd = config.suite_metrics.get('drawdown_worst_usd', 0)
        choppiness = config.suite_metrics.get('equity_choppiness_usd', 0)
        jerkiness = config.suite_metrics.get('equity_jerkiness_usd', 0)
        shortfall = config.suite_metrics.get('expected_shortfall_1pct_usd', 0)
        loss_profit = config.suite_metrics.get('loss_profit_ratio', 0)
        
        # Normalize (lower risk = higher score)
        dd_score = max(0, 10 - (dd / 0.01))  # 0.1 DD = score 0
        chop_score = max(0, 10 - choppiness)
        jerk_score = max(0, 10 - (jerkiness / 0.001))
        shortfall_score = max(0, 10 - (shortfall / 0.01))
        lpr_score = max(0, 10 - (loss_profit / 0.05))
        
        return {
            'drawdown': min(10, dd_score),
            'choppiness': min(10, chop_score),
            'jerkiness': min(10, jerk_score),
            'tail_risk': min(10, shortfall_score),
            'loss_magnitude': min(10, lpr_score),
            'overall': np.mean([dd_score, chop_score, jerk_score, shortfall_score, lpr_score])
        }
