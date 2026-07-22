"""
api/pareto_explorer.py - FastAPI page and API for the PBv7 Pareto Explorer.

Provides the FastAPI/JS page shell, bootstrap session payload, and result-path
validation for the Pareto Explorer.
"""

from __future__ import annotations

import asyncio
import copy
import json
import math
import threading
import time
import uuid
from collections import Counter
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
import numpy as np
import pandas as pd

from api.auth import SessionToken, require_auth
from ParetoDataLoader import ParetoDataLoader
from pareto_preset_generator import OPTIMIZE_PRESET_DIRECTIONS, build_optimize_preset
from pbgui_purefunc import PBGUI_SERIAL, PBGUI_VERSION, load_ini, pb7dir, pb8_runtime_status, save_ini_section

router = APIRouter()

_DEFAULT_LOAD_STRATEGY = ["performance", "robustness", "sharpe"]
_DEFAULT_MAX_CONFIGS = 2000
_LOAD_JOB_TTL_SECONDS = 900
_LOAD_JOBS: dict[str, dict] = {}
_LOAD_JOBS_LOCK = threading.Lock()
_LOAD_WORKERS: dict[str, threading.Thread] = {}
_LOAD_CANCEL_EVENTS: dict[str, threading.Event] = {}
_LOAD_KEYS: dict[str, str] = {}
_LOAD_WORKERS_LOCK = threading.RLock()
_LOADER_CACHE: dict[str, dict] = {}
_LOADER_CACHE_LOCK = threading.Lock()


def _prune_load_jobs() -> None:
    cutoff = time.time() - _LOAD_JOB_TTL_SECONDS
    stale_ids = []
    for job_id, job in _LOAD_JOBS.items():
        updated_at = float(job.get("updated_at") or 0.0)
        if updated_at < cutoff and str(job.get("status") or "") in {"complete", "error"}:
            stale_ids.append(job_id)
    for job_id in stale_ids:
        _LOAD_JOBS.pop(job_id, None)


def _prune_loader_cache() -> None:
    cutoff = time.time() - _LOAD_JOB_TTL_SECONDS
    stale_keys = []
    for key, entry in _LOADER_CACHE.items():
        updated_at = float(entry.get("updated_at") or 0.0)
        if updated_at < cutoff:
            stale_keys.append(key)
    for key in stale_keys:
        _LOADER_CACHE.pop(key, None)


def _loader_cache_key(*, result_dir: Path, all_results_loaded: bool, load_strategy: list[str], max_configs: int) -> str:
    return json.dumps([
        str(result_dir),
        "full" if all_results_loaded else "fast",
        list(load_strategy),
        int(max_configs),
    ], ensure_ascii=False, separators=(",", ":"))


def _clone_loader(loader: ParetoDataLoader, *, visible_configs: list | None = None, view_range: dict | None = None) -> ParetoDataLoader:
    cloned = copy.copy(loader)
    if visible_configs is not None:
        cloned.configs = list(visible_configs)
        setattr(cloned, "_visible_range_locked", True)
        setattr(cloned, "_visible_view_range", dict(view_range or {}))
    else:
        setattr(cloned, "_visible_range_locked", False)
        setattr(cloned, "_visible_view_range", None)
    return cloned


def _restore_pareto_flags(loader: ParetoDataLoader, flags_by_index: dict[int, bool]) -> None:
    for config in list(loader.configs or []):
        config_index = getattr(config, "config_index", None)
        if config_index in flags_by_index:
            config.is_pareto = bool(flags_by_index[config_index])


def _with_preserved_pareto_flags(loader: ParetoDataLoader, builder):
    flags_by_index = {
        int(getattr(config, "config_index")): bool(getattr(config, "is_pareto", False))
        for config in list(loader.configs or [])
        if getattr(config, "config_index", None) is not None
    }
    result = builder()
    _restore_pareto_flags(loader, flags_by_index)
    return result


def _cache_loader(key: str, loader: ParetoDataLoader) -> None:
    with _LOADER_CACHE_LOCK:
        _prune_loader_cache()
        _LOADER_CACHE[key] = {
            "loader": loader,
            "updated_at": time.time(),
        }


def _get_cached_loader(key: str) -> ParetoDataLoader | None:
    with _LOADER_CACHE_LOCK:
        _prune_loader_cache()
        entry = _LOADER_CACHE.get(key)
        if entry is None:
            return None
        entry["updated_at"] = time.time()
        loader = entry.get("loader")
        if not isinstance(loader, ParetoDataLoader):
            return None
        return _clone_loader(loader)


def _get_cached_loader_for_options(
    result_path: str,
    *,
    all_results_loaded: bool,
    load_strategy: list[str],
    max_configs: int,
) -> tuple[Path, ParetoDataLoader | None]:
    result_dir = _resolve_result_dir(result_path)
    if result_dir is None:
        raise HTTPException(status_code=404, detail="Invalid optimize result path")
    cache_key = _loader_cache_key(
        result_dir=result_dir,
        all_results_loaded=all_results_loaded,
        load_strategy=list(load_strategy),
        max_configs=max_configs,
    )
    return result_dir, _get_cached_loader(cache_key)


def _serialize_load_job(job: dict) -> dict:
    return {
        "job_id": job.get("job_id"),
        "status": job.get("status"),
        "stage": job.get("stage"),
        "message": job.get("message"),
        "error": job.get("error"),
        "progress": int(job.get("progress") or 0),
        "current": _safe_number(job.get("current")),
        "total": _safe_number(job.get("total")),
        "result_path": job.get("result_path"),
        "load_strategy": list(job.get("load_strategy") or []),
        "max_configs": int(job.get("max_configs") or _DEFAULT_MAX_CONFIGS),
        "started_at": _safe_number(job.get("started_at"), digits=6),
        "updated_at": _safe_number(job.get("updated_at"), digits=6),
    }


def _create_load_job(*, result_path: str, load_strategy: list[str], max_configs: int) -> dict:
    job_id = uuid.uuid4().hex
    now = time.time()
    job = {
        "job_id": job_id,
        "status": "loading",
        "stage": "loading",
        "message": "Starting full result load...",
        "error": None,
        "progress": 0,
        "current": 0,
        "total": 1,
        "result_path": result_path,
        "load_strategy": list(load_strategy),
        "max_configs": max_configs,
        "payload": None,
        "started_at": now,
        "updated_at": now,
    }
    with _LOAD_JOBS_LOCK:
        _prune_load_jobs()
        _LOAD_JOBS[job_id] = job
    return _serialize_load_job(job)


def _update_load_job(job_id: str, **updates: object) -> dict | None:
    with _LOAD_JOBS_LOCK:
        job = _LOAD_JOBS.get(job_id)
        if job is None:
            return None
        job.update(updates)
        job["updated_at"] = time.time()
        return dict(job)


def _get_load_job(job_id: str) -> dict | None:
    with _LOAD_JOBS_LOCK:
        _prune_load_jobs()
        job = _LOAD_JOBS.get(job_id)
        return dict(job) if job is not None else None


def _optimize_result_roots_by_version() -> list[tuple[str, Path]]:
    roots: list[tuple[str, Path]] = []
    try:
        roots.append(("v7", (Path(pb7dir()) / "optimize_results").expanduser().resolve()))
    except Exception:
        pass
    try:
        pb8_dir = str(pb8_runtime_status().get("pb8dir") or "").strip()
        if pb8_dir:
            roots.append(("v8", (Path(pb8_dir) / "optimize_results").expanduser().resolve()))
    except Exception:
        pass
    return roots


def _optimize_result_roots() -> list[Path]:
    return [root for _version, root in _optimize_result_roots_by_version()]


def _result_optimize_version(result_dir: Path) -> str:
    resolved = result_dir.resolve()
    for version, root in _optimize_result_roots_by_version():
        if resolved.is_relative_to(root):
            return version
    roots = _optimize_result_roots()
    for index, root in enumerate(roots):
        if resolved.is_relative_to(root):
            return "v8" if index == 1 else "v7"
    return "v7"


def _resolve_result_dir(path: str) -> Path | None:
    raw = str(path or "").strip()
    if not raw:
        return None
    try:
        resolved = Path(raw).expanduser().resolve()
    except Exception:
        return None
    if not resolved.exists() or not resolved.is_dir():
        return None
    if not any(resolved.is_relative_to(root) for root in _optimize_result_roots()):
        return None
    if not (resolved / "all_results.bin").is_file() and not (resolved / "pareto").is_dir():
        return None
    return resolved


def _refresh_options_from_body(body: dict | None, *, view_range: object = None) -> dict:
    payload = body or {}
    return {
        "selected_config_index": payload.get("selected_config_index", payload.get("config_index")),
        "playground_perf_weight": payload.get("playground_perf_weight", payload.get("perf_weight", 80)),
        "playground_risk_weight": payload.get("playground_risk_weight", payload.get("risk_weight", 60)),
        "playground_robust_weight": payload.get("playground_robust_weight", payload.get("robust_weight", 70)),
        "playground_show_all": bool(payload.get("playground_show_all", payload.get("show_all", False))),
        "playground_use_weighted": bool(payload.get("playground_use_weighted", payload.get("use_weighted", True))),
        "playground_use_btc": bool(payload.get("playground_use_btc", payload.get("use_btc", False))),
        "playground_viz_type": str(payload.get("playground_viz_type") or payload.get("viz_type") or "2D Scatter"),
        "playground_quick_view": str(payload.get("playground_quick_view") or payload.get("quick_view") or "Profit vs Risk"),
        "playground_color_metric": str(payload.get("playground_color_metric") or payload.get("color_metric") or ""),
        "playground_custom_x_metric": str(payload.get("playground_custom_x_metric") or payload.get("custom_x_metric") or ""),
        "playground_custom_y_metric": str(payload.get("playground_custom_y_metric") or payload.get("custom_y_metric") or ""),
        "playground_custom_z_metric": str(payload.get("playground_custom_z_metric") or payload.get("custom_z_metric") or ""),
        "preview_use_weighted": bool(payload.get("preview_use_weighted", True)),
        "preview_show_all": bool(payload.get("preview_show_all", False)),
        "deep_tab": str(payload.get("deep_tab") or "parameters"),
        "deep_parameters_top_n": payload.get("deep_parameters_top_n", payload.get("top_n", 20)),
        "deep_scenarios_metric": str(payload.get("deep_scenarios_metric") or payload.get("metric") or ""),
        "deep_evolution_metric": str(payload.get("deep_evolution_metric") or payload.get("metric") or ""),
        "deep_evolution_show_all": bool(payload.get("deep_evolution_show_all", payload.get("show_all", False))),
        "deep_evolution_hide_outliers": bool(payload.get("deep_evolution_hide_outliers", payload.get("hide_outliers", True))),
        "deep_evolution_use_weighted": bool(payload.get("deep_evolution_use_weighted", payload.get("use_weighted", True))),
        "deep_evolution_use_btc": bool(payload.get("deep_evolution_use_btc", payload.get("use_btc", False))),
        "deep_evolution_window_percent": payload.get("deep_evolution_window_percent", payload.get("window_percent", 5)),
        "deep_evolution_improvement_threshold_pct": payload.get("deep_evolution_improvement_threshold_pct", payload.get("improvement_threshold_pct", 1)),
        "deep_correlations_strategy": str(payload.get("deep_correlations_strategy") or payload.get("strategy") or "Top Performers"),
        "deep_correlations_num_configs": payload.get("deep_correlations_num_configs", payload.get("num_configs", 5)),
        "deep_correlations_use_weighted": bool(payload.get("deep_correlations_use_weighted", payload.get("use_weighted", True))),
        "deep_correlations_use_btc": bool(payload.get("deep_correlations_use_btc", payload.get("use_btc", False))),
        "view_range": view_range if view_range is not None else payload.get("view_range"),
    }


def _start_full_load_job(result_path: str, *, load_strategy: list[str], max_configs: int, refresh_options: dict | None = None) -> dict:
    normalized_options = dict(refresh_options or {})
    dedupe_key = json.dumps(
        [str(result_path), list(load_strategy), int(max_configs), normalized_options],
        ensure_ascii=False,
        sort_keys=True,
        default=str,
        separators=(",", ":"),
    )
    with _LOAD_WORKERS_LOCK:
        existing_id = _LOAD_KEYS.get(dedupe_key)
        if existing_id:
            existing = _get_load_job(existing_id)
            thread = _LOAD_WORKERS.get(existing_id)
            if existing and existing.get("status") == "loading" and thread is not None and thread.is_alive():
                return _serialize_load_job(existing)
            _LOAD_KEYS.pop(dedupe_key, None)

        job = _create_load_job(result_path=result_path, load_strategy=load_strategy, max_configs=max_configs)
        job_id = str(job["job_id"])
        _update_load_job(job_id, refresh_options=normalized_options, dedupe_key=dedupe_key)
        cancel_event = threading.Event()
        thread = threading.Thread(
            target=_run_full_load_job,
            args=(job_id,),
            name=f"pareto-full-load-{job_id[:8]}",
        )
        _LOAD_CANCEL_EVENTS[job_id] = cancel_event
        _LOAD_WORKERS[job_id] = thread
        _LOAD_KEYS[dedupe_key] = job_id
        try:
            thread.start()
        except Exception as exc:
            _LOAD_CANCEL_EVENTS.pop(job_id, None)
            _LOAD_WORKERS.pop(job_id, None)
            _LOAD_KEYS.pop(dedupe_key, None)
            _update_load_job(
                job_id,
                status="error",
                stage="error",
                message=f"Failed to start full load: {exc}",
                error=str(exc),
            )
            raise
        return _serialize_load_job(_get_load_job(job_id) or job)


def _background_full_load_response(result_path: str, *, load_strategy: list[str], max_configs: int, body: dict | None = None) -> dict:
    job = _start_full_load_job(
        result_path,
        load_strategy=load_strategy,
        max_configs=max_configs,
        refresh_options=_refresh_options_from_body(body, view_range=(body or {}).get("view_range")),
    )
    return {"ok": True, "status": "loading", "job": job}


def _load_loader_or_background_response(
    result_path: str,
    *,
    all_results_loaded: bool,
    load_strategy: list[str],
    max_configs: int,
    body: dict | None = None,
) -> tuple[ParetoDataLoader | None, dict | None]:
    if all_results_loaded:
        _result_dir, cached_loader = _get_cached_loader_for_options(
            result_path,
            all_results_loaded=True,
            load_strategy=load_strategy,
            max_configs=max_configs,
        )
        if cached_loader is None:
            return None, _background_full_load_response(
                result_path,
                load_strategy=load_strategy,
                max_configs=max_configs,
                body=body,
            )
        return cached_loader, None
    return _load_loader(
        result_path,
        all_results_loaded=False,
        load_strategy=load_strategy,
        max_configs=max_configs,
    ), None


def _result_meta(result_dir: Path) -> dict:
    all_results = result_dir / "all_results.bin"
    pareto_dir = result_dir / "pareto"
    pareto_count = len(list(pareto_dir.glob("*.json"))) if pareto_dir.is_dir() else 0
    return {
        "path": str(result_dir),
        "name": result_dir.name,
        "optimize_version": _result_optimize_version(result_dir),
        "has_all_results": all_results.exists(),
        "has_pareto_dir": pareto_dir.is_dir(),
        "pareto_count": pareto_count,
    }


def _load_pareto_defaults() -> tuple[list[str], int]:
    raw_strategy = str(load_ini("pareto", "load_strategy") or "").strip()
    load_strategy = [item.strip() for item in raw_strategy.split(",") if item.strip()] or list(_DEFAULT_LOAD_STRATEGY)
    raw_max = str(load_ini("pareto", "max_configs") or "").strip()
    try:
        max_configs = int(raw_max)
    except Exception:
        max_configs = _DEFAULT_MAX_CONFIGS
    if max_configs < 100:
        max_configs = _DEFAULT_MAX_CONFIGS
    return load_strategy, max_configs


def _normalize_load_strategy(values: object) -> list[str]:
    if isinstance(values, list):
        normalized = [str(item or "").strip() for item in values if str(item or "").strip()]
        if normalized:
            return normalized
    return list(_DEFAULT_LOAD_STRATEGY)


def _normalize_max_configs(value: object) -> int:
    try:
        parsed = int(value)
    except Exception:
        return _DEFAULT_MAX_CONFIGS
    return max(100, min(10000, parsed))


def _sanitize_preset_name(value: object, *, default: str) -> str:
    name = str(value or "").strip() or default
    for char in (' ', '/', '\\', ':', '*', '?', '"', '<', '>', '|', '\0'):
        name = name.replace(char, "_")
    name = name.strip("._")
    return name[:64] or default


def _safe_number(value: object, digits: int = 6) -> float | int | None:
    try:
        num = float(value)
    except Exception:
        return None
    if not math.isfinite(num):
        return None
    rounded = round(num, digits)
    if rounded.is_integer():
        return int(rounded)
    return rounded


def _json_safe(value: object) -> object:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8", errors="ignore")
        except Exception:
            return str(value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(_json_safe(key)): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]

    maybe_number = _safe_number(value, digits=9)
    if maybe_number is not None:
        return maybe_number
    return str(value)


def _loader_summary(loader: ParetoDataLoader, *, visible_configs: list | None = None) -> dict:
    configs = list(visible_configs if visible_configs is not None else loader.configs)
    pareto_configs = [cfg for cfg in configs if getattr(cfg, "is_pareto", False)]
    best_metric_name = loader.scoring_metrics[0] if loader.scoring_metrics else None
    best_metric_value = None
    if best_metric_name and configs:
        vals = [cfg.suite_metrics.get(best_metric_name) for cfg in configs if isinstance(getattr(cfg, "suite_metrics", None), dict)]
        vals = [float(v) for v in vals if isinstance(v, (int, float))]
        if vals:
            best_metric_value = min(vals) if _metric_lower_is_better_for_loader(loader, best_metric_name) else max(vals)

    avg_robustness = None
    if pareto_configs:
        robustness = [loader.compute_overall_robustness(cfg) for cfg in pareto_configs]
        if robustness:
            avg_robustness = sum(robustness) / len(robustness)

    return {
        "visible_configs": len(configs),
        "pareto_configs": len(pareto_configs),
        "scenario_count": len(loader.scenario_labels),
        "best_metric_name": best_metric_name,
        "best_metric_value": _safe_number(best_metric_value),
        "avg_robustness": _safe_number(avg_robustness, digits=4),
    }


def _normalize_view_range(value: object, *, total_configs: int, all_results_loaded: bool) -> dict | None:
    if not all_results_loaded:
        return None
    max_configs = max(0, int(total_configs or 0))
    default_end = min(500, max_configs)
    start = 0
    end = default_end
    if isinstance(value, dict):
        try:
            start = int(value.get("start", 0) or 0)
        except Exception:
            start = 0
        try:
            end = int(value.get("end", default_end) or default_end)
        except Exception:
            end = default_end
    elif isinstance(value, (list, tuple)) and len(value) >= 2:
        try:
            start = int(value[0] or 0)
        except Exception:
            start = 0
        try:
            end = int(value[1] or default_end)
        except Exception:
            end = default_end
    start = max(0, min(start, max_configs))
    end = max(start, min(end, max_configs))
    return {"start": start, "end": end, "max": max_configs}


def _visible_loader(loader: ParetoDataLoader, *, all_results_loaded: bool, view_range: object = None) -> tuple[ParetoDataLoader, dict | None]:
    if bool(getattr(loader, "_visible_range_locked", False)):
        locked_range = getattr(loader, "_visible_view_range", None)
        return _clone_loader(loader, visible_configs=list(loader.configs or []), view_range=locked_range), locked_range
    normalized_range = _normalize_view_range(
        view_range,
        total_configs=len(loader.configs or []),
        all_results_loaded=all_results_loaded,
    )
    if not normalized_range:
        return _clone_loader(loader), None
    start = int(normalized_range["start"])
    end = int(normalized_range["end"])
    visible_configs = loader.get_view_slice(start, end) if end > start else []
    return _clone_loader(loader, visible_configs=visible_configs, view_range=normalized_range), normalized_range


def _load_loader(
    result_path: str,
    *,
    all_results_loaded: bool,
    load_strategy: list[str],
    max_configs: int,
    progress_callback=None,
) -> ParetoDataLoader:
    result_dir = _resolve_result_dir(result_path)
    if result_dir is None:
        raise HTTPException(status_code=404, detail="Invalid optimize result path")

    cache_key = _loader_cache_key(
        result_dir=result_dir,
        all_results_loaded=all_results_loaded,
        load_strategy=list(load_strategy),
        max_configs=max_configs,
    )
    if progress_callback is None:
        cached_loader = _get_cached_loader(cache_key)
        if cached_loader is not None:
            return cached_loader

    loader = ParetoDataLoader(str(result_dir))
    loader.optimize_version = _result_optimize_version(result_dir)
    if all_results_loaded:
        ok = loader.load(load_strategy=list(load_strategy), max_configs=max_configs, progress_callback=progress_callback)
    else:
        ok = loader.load_pareto_jsons_only()
        if not ok and not getattr(loader, "last_error", None):
            loader = ParetoDataLoader(str(result_dir))
            ok = loader.load(load_strategy=list(load_strategy), max_configs=max_configs, progress_callback=progress_callback)

    if not ok:
        raise HTTPException(status_code=422, detail=str(getattr(loader, "last_error", None) or "Failed to load pareto data"))
    _cache_loader(cache_key, loader)
    return _clone_loader(loader)


def _build_champions(loader: ParetoDataLoader, limit: int = 5) -> list[dict]:
    pareto_configs = [cfg for cfg in loader.get_pareto_configs() if cfg is not None]
    if not pareto_configs:
        return []

    primary_metric = loader.scoring_metrics[0] if loader.scoring_metrics else "adg_w_usd"
    performance_bounds = _metric_bounds(pareto_configs, primary_metric)
    primary_lower_is_better = _metric_lower_is_better_for_loader(loader, primary_metric)
    configs_with_scores = []
    for config in pareto_configs:
        metrics = getattr(config, "suite_metrics", None) or {}
        performance = float(metrics.get(primary_metric, 0.0) or 0.0)
        performance_score = _normalized_metric_score(performance, performance_bounds, lower_is_better=primary_lower_is_better)
        robustness = float(loader.compute_overall_robustness(config) or 0.0)
        composite_score = performance_score * robustness
        configs_with_scores.append((config, performance, performance_score, robustness, composite_score))

    configs_with_scores.sort(key=lambda item: (-item[4], -item[3], -item[2]))

    champions: list[dict] = []
    similarity_threshold = 0.02
    for config, performance, _performance_score, robustness, composite_score in configs_with_scores:
        if len(champions) >= limit:
            break

        too_similar = False
        for existing in champions:
            existing_score = float(existing["composite_score"] or 0.0)
            existing_perf = float(existing["performance"] or 0.0)
            existing_rob = float(existing["robustness"] or 0.0)
            score_diff = abs(composite_score - existing_score) / existing_score if existing_score else abs(composite_score - existing_score)
            perf_diff = abs(performance - existing_perf) / existing_perf if existing_perf else abs(performance - existing_perf)
            rob_diff = abs(robustness - existing_rob)
            if score_diff < similarity_threshold and perf_diff < similarity_threshold and rob_diff < 0.02:
                too_similar = True
                break
        if too_similar:
            continue

        risk_profile = loader.compute_risk_profile_score(config)
        champions.append({
            "config_index": config.config_index,
            "style": loader.compute_trading_style(config),
            "performance": _safe_number(performance),
            "robustness": _safe_number(robustness, digits=4),
            "composite_score": _safe_number(composite_score, digits=9),
            "risk_overall": _safe_number(risk_profile.get("overall"), digits=3),
        })
    return champions


def _build_insights(loader: ParetoDataLoader) -> list[dict]:
    insights: list[dict] = []
    bounds_info = loader.get_parameters_at_bounds(tolerance=0.1)
    at_bounds_count = len(bounds_info.get("at_lower", {})) + len(bounds_info.get("at_upper", {}))
    if at_bounds_count > 0:
        insights.append({"level": "warning", "text": f"{at_bounds_count} parameters are near bounds - consider extending search space!"})
    else:
        insights.append({"level": "success", "text": "All parameters are well within bounds - good search space coverage!"})

    pareto_configs = [cfg for cfg in loader.get_pareto_configs() if cfg is not None]
    if pareto_configs:
        robustness_scores = [float(loader.compute_overall_robustness(cfg) or 0.0) for cfg in pareto_configs]
        if robustness_scores:
            avg_robust = sum(robustness_scores) / len(robustness_scores)
            has_scenarios = len(loader.scenario_labels) > 1
            if avg_robust > 0.85:
                insights.append({
                    "level": "success",
                    "text": f"Excellent {'robustness across scenarios' if has_scenarios else 'consistency in metrics'} (avg: {avg_robust:.2f})!",
                })
            elif avg_robust < 0.70:
                insights.append({
                    "level": "warning",
                    "text": f"Configs show high variability {'across scenarios' if has_scenarios else 'in metrics'} (avg robustness: {avg_robust:.2f})",
                })

    styles = Counter(loader.compute_trading_style(config) for config in pareto_configs)
    if len(styles) > 1:
        insights.append({"level": "info", "text": f"Good diversity: {len(styles)} different trading styles in Pareto set"})
    return insights


def _build_config_options(loader: ParetoDataLoader, limit: int = 500) -> dict:
    all_configs = [cfg for cfg in (loader.configs or []) if cfg is not None]
    options = []
    for config in all_configs[:limit]:
        label = f"Config #{config.config_index}"
        if bool(getattr(config, "is_pareto", False)):
            label += " STAR"
        options.append({
            "config_index": config.config_index,
            "label": label,
            "is_pareto": bool(getattr(config, "is_pareto", False)),
        })
    return {
        "options": options,
        "pareto_count": sum(1 for cfg in all_configs if bool(getattr(cfg, "is_pareto", False))),
        "total_count": len(all_configs),
    }


def _metric_variants(base: str, *, use_weighted: bool, use_btc: bool) -> list[str]:
    currency = "btc" if use_btc else "usd"
    weighted_suffix = "_w" if use_weighted else ""
    variants = []
    if base in {"positions_held_per_day", "position_held_hours_mean", "total_wallet_exposure_mean"}:
        variants.append(base)
    else:
        variants.append(f"{base}{weighted_suffix}_{currency}")
        if use_weighted:
            variants.append(f"{base}_{currency}")
        variants.append(base)
    seen = []
    for item in variants:
        if item not in seen:
            seen.append(item)
    return seen


def _exposure_metric_variants(*, use_weighted: bool, use_btc: bool) -> list[str]:
    currency = "btc" if use_btc else "usd"
    base = "adg_w_per_exposure_long" if use_weighted else "adg_per_exposure_long"
    variants = [f"{base}_{currency}"]
    if use_weighted:
        variants.append(f"adg_per_exposure_long_{currency}")
    variants.extend([base, "adg_per_exposure_long"])
    seen = []
    for item in variants:
        if item not in seen:
            seen.append(item)
    return seen


def _normalize_weight(value: object, default: int) -> int:
    try:
        parsed = int(value)
    except Exception:
        parsed = int(default)
    return max(0, min(100, parsed))


def _resolve_existing_metric(loader: ParetoDataLoader, metric_names: list[str]) -> str | None:
    configs = [cfg for cfg in (loader.configs or []) if isinstance(getattr(cfg, "suite_metrics", None), dict)]
    if not configs:
        return None
    sample_metrics = set(configs[0].suite_metrics.keys())
    for name in metric_names:
        if name in sample_metrics:
            return name
    return None


def _metric_display_name(metric_name: str | None) -> str:
    if not metric_name:
        return "Metric"
    raw_label = str(metric_name).strip()
    suffix = ""
    if raw_label.endswith("_w_usd"):
        raw_label = raw_label[:-6]
        suffix = "_w USD"
    elif raw_label.endswith("_w_btc"):
        raw_label = raw_label[:-6]
        suffix = "_w BTC"
    elif raw_label.endswith("_usd"):
        raw_label = raw_label[:-4]
        suffix = " USD"
    elif raw_label.endswith("_btc"):
        raw_label = raw_label[:-4]
        suffix = " BTC"
    label = raw_label.replace("_", " ").strip()
    return (label + suffix).strip().title().replace("_W", "_w")


def _metric_numeric_value(config: object, metric: str | None) -> float | None:
    if not metric:
        return None
    try:
        value = (getattr(config, "suite_metrics", None) or {}).get(metric)
        numeric = float(value)
    except Exception:
        return None
    if not math.isfinite(numeric):
        return None
    return numeric


def _metric_bounds(configs: list, metric: str | None) -> tuple[float, float] | None:
    values = [_metric_numeric_value(config, metric) for config in configs]
    numeric_values = [value for value in values if value is not None]
    if not numeric_values:
        return None
    return min(numeric_values), max(numeric_values)


def _normalized_metric_score(value: float | None, bounds: tuple[float, float] | None, *, lower_is_better: bool) -> float:
    if value is None or bounds is None:
        return 0.0
    lo, hi = bounds
    span = hi - lo
    if abs(span) <= 1e-12:
        return 1.0
    score = (value - lo) / span
    if lower_is_better:
        score = 1.0 - score
    return max(0.0, min(1.0, score))


def _unique_metric_names(metrics: list[str | None]) -> list[str]:
    out: list[str] = []
    for metric in metrics:
        metric_name = str(metric or "").strip()
        if metric_name and metric_name not in out:
            out.append(metric_name)
    return out


def _best_match_metric_terms(loader: ParetoDataLoader, configs: list, metrics: list[str | None], *, perf_weight: int, risk_weight: int) -> list[dict]:
    performance_terms: list[dict] = []
    risk_terms: list[dict] = []
    for metric in _unique_metric_names(metrics):
        bounds = _metric_bounds(configs, metric)
        if bounds is None:
            continue
        lower_is_better = _metric_lower_is_better_for_loader(loader, metric)
        term = {"metric": metric, "bounds": bounds, "lower_is_better": lower_is_better}
        if lower_is_better:
            risk_terms.append(term)
        else:
            performance_terms.append(term)

    terms: list[dict] = []
    if performance_terms:
        weight = float(perf_weight) / float(len(performance_terms))
        for term in performance_terms:
            terms.append({**term, "weight": weight})
    if risk_terms:
        weight = float(risk_weight) / float(len(risk_terms))
        for term in risk_terms:
            terms.append({**term, "weight": weight})
    return terms


def _compute_weighted_config_score(loader: ParetoDataLoader, config: object, terms: list[dict], *, robust_weight: int, total_weight: float | None = None) -> float:
    weighted_sum = 0.0
    active_weight = 0.0
    for term in terms:
        weight = float(term.get("weight") or 0.0)
        if weight <= 0.0:
            continue
        score = _normalized_metric_score(
            _metric_numeric_value(config, term.get("metric")),
            term.get("bounds"),
            lower_is_better=bool(term.get("lower_is_better")),
        )
        weighted_sum += score * weight
        active_weight += weight

    if robust_weight > 0:
        robust = float(loader.compute_overall_robustness(config) or 0.0)
        robust = max(0.0, min(1.0, robust)) if math.isfinite(robust) else 0.0
        weighted_sum += robust * float(robust_weight)
        active_weight += float(robust_weight)

    denominator = total_weight if total_weight is not None else active_weight
    return weighted_sum / (denominator if abs(float(denominator or 0.0)) > 1e-12 else 1.0)


def _compute_best_match(
    loader: ParetoDataLoader,
    *,
    perf_weight: int,
    risk_weight: int,
    robust_weight: int,
    performance_metric: str | None = None,
    risk_metric: str | None = None,
    score_metrics: list[str | None] | None = None,
    configs_override: list | None = None,
) -> tuple[object | None, float | None, str | None]:
    pareto_configs = [cfg for cfg in (configs_override if configs_override is not None else (loader.get_pareto_configs() or [])) if cfg is not None]
    if not pareto_configs:
        pareto_configs = [cfg for cfg in (loader.configs or [])[:100] if cfg is not None]
    if not pareto_configs:
        return None, None, None

    primary_metric = performance_metric or (loader.scoring_metrics[0] if loader.scoring_metrics else "adg_w_usd")
    if score_metrics is None:
        score_metrics = [primary_metric, risk_metric or "drawdown_worst_usd"]
    metric_terms = _best_match_metric_terms(loader, pareto_configs, score_metrics, perf_weight=perf_weight, risk_weight=risk_weight)

    best_match = None
    best_score = None
    for config in pareto_configs:
        score = _compute_weighted_config_score(loader, config, metric_terms, robust_weight=robust_weight)
        if not math.isfinite(score):
            continue
        if best_score is None or score > best_score:
            best_match = config
            best_score = score
    return best_match, _safe_number(best_score, digits=6), primary_metric


def _compute_explorer_score(
    loader: ParetoDataLoader,
    config: object,
    *,
    primary_metric: str,
    perf_weight: int,
    risk_weight: int,
    robust_weight: int,
    performance_bounds: tuple[float, float] | None = None,
    risk_metric: str = "drawdown_worst_usd",
    risk_bounds: tuple[float, float] | None = None,
    total_weight: float | None = None,
) -> float:
    configs = [config]
    terms = []
    performance_bounds = performance_bounds or _metric_bounds(configs, primary_metric)
    if performance_bounds is not None:
        terms.append({
            "metric": primary_metric,
            "bounds": performance_bounds,
            "lower_is_better": _metric_lower_is_better_for_loader(loader, primary_metric),
            "weight": float(perf_weight),
        })
    risk_bounds = risk_bounds or _metric_bounds(configs, risk_metric)
    if risk_bounds is not None:
        terms.append({
            "metric": risk_metric,
            "bounds": risk_bounds,
            "lower_is_better": _metric_lower_is_better_for_loader(loader, risk_metric),
            "weight": float(risk_weight),
        })
    return _compute_weighted_config_score(loader, config, terms, robust_weight=robust_weight, total_weight=total_weight)


def _build_playground_scatter(
    loader: ParetoDataLoader,
    *,
    x_metric: str,
    y_metric: str,
    color_metric: str | None,
    show_all: bool,
    best_match: object | None,
    selected_config: object | None,
    title_prefix: str,
    configs_override: list | None = None,
) -> dict:
    source_configs = configs_override if configs_override is not None else (loader.configs if show_all else loader.get_pareto_configs())
    configs = [cfg for cfg in source_configs if cfg is not None]
    if not configs:
        return {"traces": [], "layout": {"title": "No data available"}}

    pareto_configs = [cfg for cfg in configs if bool(getattr(cfg, "is_pareto", False))]
    non_pareto_configs = [cfg for cfg in configs if not bool(getattr(cfg, "is_pareto", False))]

    color_range = None
    if color_metric:
        all_color_values: list[float] = []
        for cfg in configs:
            value = _safe_number(cfg.suite_metrics.get(color_metric), digits=9)
            if value is None:
                continue
            try:
                all_color_values.append(float(value))
            except Exception:
                continue
        if all_color_values:
            color_min = min(all_color_values)
            color_max = max(all_color_values)
            if abs(color_max - color_min) < 1e-12:
                color_max = color_min + 1e-12
            color_range = (color_min, color_max)

    def to_trace(items: list, *, name: str, color: str, symbol: str, opacity: float, color_values: list | None = None, show_scale: bool = False) -> dict:
        return {
            "name": name,
            "mode": "markers",
            "type": "scatter",
            "x": [_safe_number(cfg.suite_metrics.get(x_metric), digits=9) for cfg in items],
            "y": [_safe_number(cfg.suite_metrics.get(y_metric), digits=9) for cfg in items],
            "customdata": [{"config_index": cfg.config_index} for cfg in items],
            "marker": {
                "size": 8 if symbol == "circle" else 10,
                "color": color_values if color_values else color,
                "symbol": symbol,
                "opacity": opacity,
                "line": {"width": 1, "color": "white"} if symbol != "circle" else {"width": 0},
                "colorscale": "Viridis" if color_values else None,
                "showscale": bool(color_values) and bool(show_scale),
                "cmin": color_range[0] if color_values and color_range else None,
                "cmax": color_range[1] if color_values and color_range else None,
                "colorbar": {
                    "thickness": 18,
                    "len": 0.82,
                    "x": 0.98,
                    "xanchor": "left",
                    "y": 0.5,
                    "yanchor": "middle",
                    "outlinewidth": 0,
                    "tickfont": {"color": "#fafafa"},
                } if color_values and show_scale else None,
            },
            "hovertemplate": "%{text}<extra></extra>",
            "text": [
                f"Config #{cfg.config_index}<br>{x_metric}: {cfg.suite_metrics.get(x_metric, 0):.6f}<br>{y_metric}: {cfg.suite_metrics.get(y_metric, 0):.6f}"
                for cfg in items
            ],
        }

    traces: list[dict] = []
    if show_all and non_pareto_configs:
        traces.append(to_trace(
            non_pareto_configs,
            name="All Configs",
            color="lightblue",
            symbol="circle",
            opacity=0.6,
            color_values=[_safe_number(cfg.suite_metrics.get(color_metric), digits=9) for cfg in non_pareto_configs] if color_metric else None,
            show_scale=True,
        ))
    if pareto_configs:
        traces.append(to_trace(
            pareto_configs,
            name="Pareto Front",
            color="red",
            symbol="star",
            opacity=0.9,
            color_values=[_safe_number(cfg.suite_metrics.get(color_metric), digits=9) for cfg in pareto_configs] if color_metric else None,
            show_scale=not (show_all and non_pareto_configs),
        ))
    if best_match is not None:
        traces.append({
            "name": "Best Match",
            "mode": "markers",
            "type": "scatter",
            "x": [_safe_number(best_match.suite_metrics.get(x_metric), digits=9)],
            "y": [_safe_number(best_match.suite_metrics.get(y_metric), digits=9)],
            "customdata": [{"config_index": best_match.config_index}],
            "marker": {"size": 18, "color": "lime", "symbol": "star", "line": {"width": 3, "color": "darkgreen"}},
            "hovertemplate": "%{text}<extra></extra>",
            "text": [f"Best Match Config #{best_match.config_index}<br>{x_metric}: {best_match.suite_metrics.get(x_metric, 0):.6f}<br>{y_metric}: {best_match.suite_metrics.get(y_metric, 0):.6f}"],
        })
    if selected_config is not None and getattr(selected_config, "config_index", None) != getattr(best_match, "config_index", None):
        traces.append({
            "name": "Selected Config",
            "mode": "markers",
            "type": "scatter",
            "x": [_safe_number(selected_config.suite_metrics.get(x_metric), digits=9)],
            "y": [_safe_number(selected_config.suite_metrics.get(y_metric), digits=9)],
            "customdata": [{"config_index": selected_config.config_index}],
            "marker": {"size": 18, "color": "#ffd54f", "symbol": "star", "line": {"width": 3, "color": "#ff9800"}},
            "hovertemplate": "%{text}<extra></extra>",
            "text": [f"Selected Config #{selected_config.config_index}<br>{x_metric}: {selected_config.suite_metrics.get(x_metric, 0):.6f}<br>{y_metric}: {selected_config.suite_metrics.get(y_metric, 0):.6f}"],
        })

    x_label = _metric_display_name(x_metric)
    y_label = _metric_display_name(y_metric)

    return {
        "traces": traces,
        "layout": {
            "title": None,
            "xaxis": {"title": {"text": x_label, "standoff": 18}, "automargin": True},
            "yaxis": {"title": {"text": y_label, "standoff": 18}, "automargin": True},
            "height": 750,
            "hovermode": "closest",
            "template": "plotly_white",
            "dragmode": "zoom",
            "margin": {"l": 80, "r": 80 if color_metric else 40, "t": 92, "b": 70},
            "legend": {
                "orientation": "h",
                "x": 0,
                "xanchor": "left",
                "y": 1.12,
                "yanchor": "bottom",
            },
        },
    }


def _view_configs(loader: ParetoDataLoader, *, all_results_loaded: bool) -> list:
    if bool(getattr(loader, "_visible_range_locked", False)):
        return list(loader.configs or [])
    if all_results_loaded:
        end = min(len(loader.configs or []), 500)
        return loader.get_view_slice(0, end) if end > 0 else []
    return list(loader.configs or [])


def _preview_display_metrics(loader: ParetoDataLoader, *, use_weighted: bool, configs: list) -> list[str]:
    scoring_metrics = list(loader.scoring_metrics or [])
    if not scoring_metrics:
        scoring_metrics = ["adg_w_usd"]
    display_metrics: list[str] = []
    sample_metrics = set((configs[0].suite_metrics or {}).keys()) if configs else set()

    def _metric_variant(metric: str) -> str:
        raw_metric = str(metric or "").strip()
        candidates: list[str] = []
        if use_weighted:
            if raw_metric.endswith("_w_usd") or raw_metric.endswith("_w_btc") or raw_metric.endswith("_w") or "_w_" in raw_metric:
                candidates.append(raw_metric)
            elif raw_metric.endswith("_usd"):
                candidates.extend([raw_metric[:-4] + "_w_usd", raw_metric])
            elif raw_metric.endswith("_btc"):
                candidates.extend([raw_metric[:-4] + "_w_btc", raw_metric])
            else:
                candidates.extend([raw_metric + "_w", raw_metric])
        else:
            if raw_metric.endswith("_w_usd"):
                candidates.extend([raw_metric[:-6] + "_usd", raw_metric])
            elif raw_metric.endswith("_w_btc"):
                candidates.extend([raw_metric[:-6] + "_btc", raw_metric])
            elif raw_metric.endswith("_w"):
                candidates.extend([raw_metric[:-2], raw_metric])
            elif "_w_" in raw_metric:
                candidates.extend([raw_metric.replace("_w_", "_", 1), raw_metric])
            else:
                candidates.append(raw_metric)
        for candidate in candidates:
            if candidate in sample_metrics:
                return candidate
        return raw_metric

    for metric in scoring_metrics:
        resolved_metric = _metric_variant(metric)
        if resolved_metric and resolved_metric not in display_metrics:
            display_metrics.append(resolved_metric)

    if len(display_metrics) < 2:
        available_metrics = list(sample_metrics)
        preferred_fallbacks = [
            "adg_w_usd" if use_weighted else "adg_usd",
            "mdg_w_usd" if use_weighted else "mdg_usd",
            "gain_usd",
            "adg_pnl_w" if use_weighted else "adg_pnl",
            "mdg_pnl_w" if use_weighted else "mdg_pnl",
            "drawdown_worst_w_usd" if use_weighted else "drawdown_worst_usd",
            "drawdown_worst_usd",
        ]
        for metric in preferred_fallbacks:
            if metric in available_metrics and metric not in display_metrics:
                display_metrics.append(metric)
            if len(display_metrics) >= 2:
                break
    return display_metrics


def _build_preview_payload(
    loader: ParetoDataLoader,
    *,
    all_results_loaded: bool,
    use_weighted: bool,
    show_all: bool,
    selected_config_index: int | None = None,
    view_range: object = None,
) -> dict:
    visible_loader, normalized_range = _visible_loader(loader, all_results_loaded=all_results_loaded, view_range=view_range)
    configs = _view_configs(visible_loader, all_results_loaded=all_results_loaded)
    preview_configs = list(configs if show_all else [config for config in configs if bool(getattr(config, "is_pareto", False))])
    if not preview_configs and configs:
        preview_configs = list(configs)
    if not configs:
        return {
            "display_metrics": [],
            "counts": {"configs": 0, "pareto": 0},
            "view_range": normalized_range,
            "pareto_analysis": {"traces": [], "layout": {"title": "No data available"}},
            "robustness": {"traces": [], "layout": {"title": "No data available"}},
        }

    display_metrics = _preview_display_metrics(visible_loader, use_weighted=use_weighted, configs=preview_configs)
    pareto_count = sum(1 for config in configs if bool(getattr(config, "is_pareto", False)))
    x_metric = display_metrics[0] if display_metrics else "adg_w_usd"
    y_metric = display_metrics[1] if len(display_metrics) > 1 else x_metric
    color_metric = _resolve_existing_metric(
        visible_loader,
        _metric_variants("drawdown_worst", use_weighted=use_weighted, use_btc=False),
    )
    best_match, _, _ = _compute_best_match(
        visible_loader,
        perf_weight=80,
        risk_weight=60,
        robust_weight=70,
        performance_metric=x_metric,
        risk_metric=color_metric or y_metric,
        score_metrics=[x_metric, y_metric, color_metric],
        configs_override=preview_configs,
    )
    selected_config = visible_loader.get_config_by_index(selected_config_index) if selected_config_index is not None else None
    return {
        "display_metrics": display_metrics,
        "counts": {"configs": len(preview_configs), "pareto": pareto_count, "total_configs": len(configs), "show_all": bool(show_all)},
        "view_range": normalized_range,
        "pareto_analysis": _build_playground_scatter(
            visible_loader,
            x_metric=x_metric,
            y_metric=y_metric,
            color_metric=color_metric,
            show_all=show_all,
            best_match=None,
            selected_config=selected_config,
            title_prefix="Pareto Analysis",
            configs_override=preview_configs,
        ),
        "robustness": _build_robustness_quadrant(visible_loader, performance_metric=x_metric, show_all=show_all, best_match=selected_config or best_match, configs_override=preview_configs),
    }


def _build_robustness_quadrant(loader: ParetoDataLoader, *, performance_metric: str, show_all: bool, best_match: object | None = None, configs_override: list | None = None) -> dict:
    configs = list(configs_override if configs_override is not None else (loader.configs if show_all else loader.get_pareto_configs()))
    if not configs:
        return {"traces": [], "layout": {"title": "No data available"}}
    performance = []
    robustness = []
    colors = []
    config_indices = []
    for config in configs:
        performance.append(_safe_number(config.suite_metrics.get(performance_metric), digits=9))
        robustness.append(_safe_number(loader.compute_overall_robustness(config), digits=9))
        colors.append("red" if bool(getattr(config, "is_pareto", False)) else "blue")
        config_indices.append(config.config_index)
    valid_perf = [float(value) for value in performance if isinstance(value, (int, float))]
    valid_rob = [float(value) for value in robustness if isinstance(value, (int, float))]
    mean_perf = float(np.mean(valid_perf)) if valid_perf else 0.0
    mean_robust = float(np.mean(valid_rob)) if valid_rob else 0.0
    max_perf = max(valid_perf) if valid_perf else 0.0
    max_robust = max(valid_rob) if valid_rob else 0.0
    traces = [{
        "type": "scatter",
        "mode": "markers",
        "x": performance,
        "y": robustness,
        "marker": {"size": 10, "color": colors, "line": {"width": 1, "color": "white"}},
        "text": [f"Config #{idx}" for idx in config_indices],
        "customdata": [[idx] for idx in config_indices],
        "hovertemplate": "<b>%{text}</b><br>Performance: %{x:.6f}<br>Robustness: %{y:.3f}<extra></extra>",
        "name": "Configs",
    }]
    if best_match is not None:
        traces.append({
            "type": "scatter",
            "mode": "markers",
            "x": [_safe_number(best_match.suite_metrics.get(performance_metric), digits=9)],
            "y": [_safe_number(loader.compute_overall_robustness(best_match), digits=9)],
            "marker": {"size": 18, "color": "lime", "symbol": "star", "line": {"width": 3, "color": "darkgreen"}},
            "customdata": [[best_match.config_index]],
            "name": "Best Match",
            "hovertemplate": "<b>Best Match Config %{customdata[0]}</b><br>Performance: %{x:.6f}<br>Robustness: %{y:.3f}<extra></extra>",
        })
    performance_label = _metric_display_name(performance_metric)

    return {
        "traces": traces,
        "layout": {
            "title": None,
            "xaxis": {"title": {"text": performance_label, "standoff": 18}, "automargin": True},
            "yaxis": {"title": {"text": "Robustness Score (0-1)", "standoff": 18}, "automargin": True},
            "height": 750,
            "template": "plotly_white",
            "margin": {"l": 80, "r": 40, "t": 92, "b": 70},
            "legend": {
                "orientation": "h",
                "x": 0,
                "xanchor": "left",
                "y": 1.12,
                "yanchor": "bottom",
            },
            "shapes": [
                {"type": "line", "x0": mean_perf, "x1": mean_perf, "y0": 0, "y1": max_robust or 1, "line": {"dash": "dash", "color": "gray"}, "opacity": 0.5},
                {"type": "line", "x0": 0, "x1": max_perf or 1, "y0": mean_robust, "y1": mean_robust, "line": {"dash": "dash", "color": "gray"}, "opacity": 0.5},
            ],
            "annotations": [
                {"x": max_perf * 0.9 if max_perf else 0.9, "y": max_robust * 0.9 if max_robust else 0.9, "text": "Trophy Best of Both", "showarrow": False, "font": {"size": 14, "color": "green"}},
                {"x": mean_perf * 0.3 if mean_perf else 0.3, "y": max_robust * 0.9 if max_robust else 0.9, "text": "Stable but Slow", "showarrow": False, "font": {"size": 12, "color": "blue"}},
                {"x": max_perf * 0.9 if max_perf else 0.9, "y": mean_robust * 0.3 if mean_robust else 0.3, "text": "High Risk", "showarrow": False, "font": {"size": 12, "color": "orange"}},
            ],
        },
    }


def _radar_metric_display_name(metric_name: str) -> str:
    if metric_name == "robustness":
        return "Robustness"
    return str(metric_name).replace("_usd", "").replace("_btc", "").replace("_", " ").title()


def _select_playground_radar_configs(loader: ParetoDataLoader, *, best_match: object, top_n: int = 5) -> list[object]:
    pareto_configs = loader.get_pareto_configs()
    primary_metric = loader.scoring_metrics[0] if loader.scoring_metrics else "adg_w_usd"
    comparison_configs = sorted(
        [config for config in pareto_configs if config.config_index != best_match.config_index],
        key=lambda config: _metric_sort_value_for_loader(loader, config, primary_metric),
    )[:top_n]
    return [best_match] + comparison_configs


def _build_playground_radar(loader: ParetoDataLoader, *, best_match: object | None, quick_view: str) -> dict:
    if best_match is None:
        return {"traces": [], "layout": {"title": "No Best Match config available"}}

    all_configs = _select_playground_radar_configs(loader, best_match=best_match)
    comparison_configs = all_configs[1:]
    if quick_view == "Risk Profile":
        labels = [f"Best Match (#{best_match.config_index})"] + [f"Config #{config.config_index}" for config in comparison_configs]
        return _build_risk_profile_radar(
            loader,
            config_indices=[config.config_index for config in all_configs],
            labels=labels,
        )

    metrics = None
    available_metrics = list((best_match.suite_metrics or {}).keys())
    if metrics is None:
        metrics = []
        for metric in ["adg_w_usd", "gain_usd"]:
            if metric in available_metrics:
                metrics.append(metric)
                break
        for metric in ["sharpe_ratio_usd", "sortino_ratio_usd", "calmar_ratio_usd"]:
            if metric in available_metrics:
                metrics.append(metric)
                break
        for metric in ["drawdown_worst_usd", "equity_choppiness_usd"]:
            if metric in available_metrics:
                metrics.append(metric)
                break
        metrics.append("robustness")
        for metric in ["equity_volatility_usd"]:
            if metric in available_metrics:
                metrics.append(metric)
                break
    metric_ranges = {}
    for metric in metrics:
        values = []
        for config in all_configs:
            value = loader.compute_overall_robustness(config) if metric == "robustness" else config.suite_metrics.get(metric, 0)
            values.append(value)
        metric_ranges[metric] = {"min": min(values), "max": max(values), "range": max(values) - min(values) if max(values) != min(values) else 1}

    angles = np.linspace(0, 2 * np.pi, len(metrics), endpoint=False).tolist()
    theta_labels = [_radar_metric_display_name(metric) for metric in metrics]

    def polar_to_cartesian(r: float, theta: float) -> tuple[float, float]:
        return r * np.cos(theta), r * np.sin(theta)

    def normalized_values(config: object) -> list[float]:
        values = []
        for metric in metrics:
            value = loader.compute_overall_robustness(config) if metric == "robustness" else config.suite_metrics.get(metric, 0)
            bounds = metric_ranges[metric]
            normalized = (value - bounds["min"]) / bounds["range"] if bounds["range"] > 0 else 0.5
            if _metric_lower_is_better_for_loader(loader, metric):
                normalized = 1.0 - normalized
            values.append(normalized)
        return values

    traces = []
    palette = [
        ("lime", "rgba(0,255,100,0.2)", "star"),
        ("rgb(100,150,255)", "rgba(100,150,255,0.15)", "circle"),
        ("rgb(255,150,100)", "rgba(255,150,100,0.15)", "square"),
        ("rgb(150,100,255)", "rgba(150,100,255,0.15)", "diamond"),
        ("rgb(255,200,100)", "rgba(255,200,100,0.15)", "cross"),
        ("rgb(100,255,200)", "rgba(100,255,200,0.15)", "x"),
    ]
    for idx, config in enumerate(all_configs):
        r_values = normalized_values(config)
        r_closed = r_values + [r_values[0]]
        angles_closed = angles + [angles[0]]
        x_vals = []
        y_vals = []
        for r_value, theta in zip(r_closed, angles_closed):
            x_value, y_value = polar_to_cartesian(r_value, theta)
            x_vals.append(_safe_number(x_value, digits=9))
            y_vals.append(_safe_number(y_value, digits=9))
        line_color, fill_color, symbol = palette[min(idx, len(palette) - 1)]
        name = f"Best Match (#{config.config_index})" if idx == 0 else f"Config #{config.config_index}"

        traces.append({
            "type": "scatter",
            "x": x_vals,
            "y": y_vals,
            "mode": "lines",
            "name": name + " (area)",
            "line": {"color": "rgba(0,0,0,0)", "width": 0},
            "fill": "toself",
            "fillcolor": fill_color,
            "meta": {"config_index": config.config_index},
            "showlegend": False,
            "hoverinfo": "skip",
        })
        traces.append({
            "type": "scatter",
            "x": x_vals,
            "y": y_vals,
            "mode": "lines",
            "name": name,
            "line": {"color": line_color, "width": 3 if idx == 0 else 2},
            "meta": {"config_index": config.config_index},
            "showlegend": True,
            "hoverinfo": "skip",
        })
        traces.append({
            "type": "scatter",
            "x": [_safe_number(polar_to_cartesian(r_value, theta)[0], digits=9) for r_value, theta in zip(r_values, angles)],
            "y": [_safe_number(polar_to_cartesian(r_value, theta)[1], digits=9) for r_value, theta in zip(r_values, angles)],
            "mode": "markers",
            "name": name + " (points)",
            "marker": {"size": 15 if idx == 0 else 12, "color": line_color, "symbol": symbol, "line": {"color": "darkgreen" if idx == 0 else "white", "width": 2 if idx == 0 else 1}},
            "meta": {"config_index": config.config_index},
            "customdata": [[config.config_index, theta_labels[i], r_values[i]] for i in range(len(r_values))],
            "hovertemplate": ("<b>Best Match (Config %{customdata[0]})</b><br>%{customdata[1]}: %{customdata[2]:.1%}<extra></extra>" if idx == 0 else f"<b>Config #{config.config_index}</b><br>%{{customdata[1]}}: %{{customdata[2]:.1%}}<extra></extra>"),
            "showlegend": False,
        })

    axis_annotations = []
    for idx, theta in enumerate(angles):
        x_line, y_line = polar_to_cartesian(1.2, theta)
        traces.append({
            "type": "scatter",
            "x": [0, _safe_number(x_line, digits=9)],
            "y": [0, _safe_number(y_line, digits=9)],
            "mode": "lines",
            "line": {"color": "rgba(240,240,240,0.55)", "width": 1},
            "showlegend": False,
            "hoverinfo": "skip",
        })
        label_x, label_y = polar_to_cartesian(1.3, theta)
        xanchor = "center"
        if np.cos(theta) > 0.45:
            xanchor = "left"
        elif np.cos(theta) < -0.45:
            xanchor = "right"
        yanchor = "middle"
        if np.sin(theta) > 0.75:
            yanchor = "bottom"
        elif np.sin(theta) < -0.75:
            yanchor = "top"
        axis_annotations.append({
            "x": _safe_number(label_x, digits=9),
            "y": _safe_number(label_y, digits=9),
            "text": f"<b>{theta_labels[idx]}</b>",
            "showarrow": False,
            "font": {"size": 14, "color": "#fafafa"},
            "xanchor": xanchor,
            "yanchor": yanchor,
            "bgcolor": "rgba(0,0,0,0.72)",
            "borderpad": 4,
        })

    circle_angles = np.linspace(0, 2 * np.pi, 100).tolist()
    for radius in [0.25, 0.5, 0.75, 1.0]:
        traces.append({
            "type": "scatter",
            "x": [_safe_number(radius * np.cos(angle), digits=9) for angle in circle_angles],
            "y": [_safe_number(radius * np.sin(angle), digits=9) for angle in circle_angles],
            "mode": "lines",
            "line": {"color": "rgba(240,240,240,0.7)", "width": 1, "dash": "dot"},
            "showlegend": False,
            "hoverinfo": "skip",
        })
        axis_annotations.append({
            "x": 0,
            "y": radius,
            "text": f"<b>{int(radius * 100)}%</b>",
            "showarrow": False,
            "font": {"size": 10, "color": "#fafafa"},
            "xanchor": "center",
            "yanchor": "bottom",
            "bgcolor": "rgba(0,0,0,0.55)",
            "borderpad": 2,
        })

    return {
        "traces": traces,
        "layout": {
            "title": None,
            "height": 700,
            "template": "plotly_white",
            "xaxis": {
                "showgrid": False,
                "zeroline": False,
                "showticklabels": False,
                "scaleanchor": "y",
                "scaleratio": 1,
                "range": [-1.45, 1.55],
            },
            "yaxis": {
                "showgrid": False,
                "zeroline": False,
                "showticklabels": False,
                "range": [-1.25, 1.25],
            },
            "annotations": axis_annotations,
            "hovermode": "closest",
            "dragmode": "zoom",
            "margin": {"l": 50, "r": 190, "t": 36, "b": 36},
            "legend": {
                "x": 1.01,
                "xanchor": "left",
                "y": 1,
                "yanchor": "top",
            },
        },
    }


def _build_playground_projection_chart(
    loader: ParetoDataLoader,
    *,
    x_metric: str,
    y_metric: str,
    z_metric: str,
    best_match: object | None,
    selected_config: object | None,
    title_prefix: str,
    color_metric: str | None,
    show_all: bool,
) -> dict:
    payload = _build_playground_scatter(
        loader,
        x_metric=x_metric,
        y_metric=y_metric,
        color_metric=color_metric,
        show_all=show_all,
        best_match=best_match,
        selected_config=selected_config,
        title_prefix=title_prefix,
    )
    if not color_metric:
        return payload

    color_values: list[float] = []
    for trace in list(payload.get("traces") or []):
        marker = trace.get("marker") if isinstance(trace, dict) else None
        if not isinstance(marker, dict):
            continue
        colors = marker.get("color")
        if not isinstance(colors, list):
            continue
        numeric_values: list[float] = []
        for value in colors:
            try:
                numeric_values.append(float(value))
            except Exception:
                continue
        if numeric_values:
            color_values.extend(numeric_values)
            marker["coloraxis"] = "coloraxis"
            marker.pop("colorscale", None)
            marker.pop("showscale", None)
            marker.pop("colorbar", None)
            marker.pop("cmin", None)
            marker.pop("cmax", None)

    if color_values:
        layout = payload.setdefault("layout", {})
        color_min = min(color_values)
        color_max = max(color_values)
        if abs(color_max - color_min) < 1e-12:
            color_max = color_min + 1e-12
        annotations = [annotation for annotation in list(layout.get("annotations") or []) if str((annotation or {}).get("name") or "") != "projection-colorbar-title"]
        annotations.append({
            "name": "projection-colorbar-title",
            "text": _metric_display_name(color_metric),
            "xref": "paper",
            "yref": "paper",
            "x": 1.02,
            "y": 1.04,
            "xanchor": "center",
            "yanchor": "bottom",
            "showarrow": False,
            "align": "center",
            "font": {"color": "#fafafa", "size": 10},
        })
        layout["coloraxis"] = {
            "colorscale": "Viridis",
            "cmin": color_min,
            "cmax": color_max,
            "colorbar": {
                "thickness": 14,
                "len": 0.78,
                "x": 1.02,
                "xanchor": "left",
                "y": 0.5,
                "yanchor": "middle",
                "outlinewidth": 0,
                "tickfont": {"color": "#fafafa", "size": 10},
            },
        }
        layout["annotations"] = annotations
        layout["margin"] = {"l": 80, "r": 96, "t": 104, "b": 70}
    return payload


def _build_playground_3d(loader: ParetoDataLoader, *, x_metric: str, y_metric: str, z_metric: str, color_metric: str | None, show_all: bool, best_match: object | None, title_prefix: str) -> dict:
    configs = [cfg for cfg in (loader.configs if show_all else loader.get_pareto_configs()) if cfg is not None]
    if not configs:
        return {"traces": [], "layout": {"title": "No data available"}}
    x_label = _metric_display_name(x_metric)
    y_label = _metric_display_name(y_metric)
    z_label = _metric_display_name(z_metric)
    pareto_configs = [cfg for cfg in configs if bool(getattr(cfg, "is_pareto", False))]
    non_pareto_configs = [cfg for cfg in configs if not bool(getattr(cfg, "is_pareto", False))]
    color_range = None
    if color_metric:
        all_color_values: list[float] = []
        for cfg in configs:
            value = _safe_number(cfg.suite_metrics.get(color_metric), digits=9)
            if value is None:
                continue
            try:
                all_color_values.append(float(value))
            except Exception:
                continue
        if all_color_values:
            color_min = min(all_color_values)
            color_max = max(all_color_values)
            if abs(color_max - color_min) < 1e-12:
                color_max = color_min + 1e-12
            color_range = (color_min, color_max)
    traces = []
    if show_all and non_pareto_configs:
        traces.append({
            "type": "scatter3d",
            "mode": "markers",
            "x": [_safe_number(cfg.suite_metrics.get(x_metric), digits=9) for cfg in non_pareto_configs],
            "y": [_safe_number(cfg.suite_metrics.get(y_metric), digits=9) for cfg in non_pareto_configs],
            "z": [_safe_number(cfg.suite_metrics.get(z_metric), digits=9) for cfg in non_pareto_configs],
            "marker": {"size": 3, "color": [_safe_number(cfg.suite_metrics.get(color_metric), digits=9) for cfg in non_pareto_configs] if color_metric else "lightblue", "colorscale": "Viridis" if color_metric else None, "showscale": bool(color_metric), "cmin": color_range[0] if color_metric and color_range else None, "cmax": color_range[1] if color_metric and color_range else None, "colorbar": {"thickness": 18, "len": 0.82, "x": 0.98, "xanchor": "left", "y": 0.5, "yanchor": "middle", "outlinewidth": 0, "tickfont": {"color": "#fafafa"}}, "opacity": 0.35},
            "name": "All Configs",
            "customdata": [[cfg.config_index] for cfg in non_pareto_configs],
            "hovertemplate": f"<b>Config %{{customdata[0]}}</b><br>{x_label}: %{{x:.6f}}<br>{y_label}: %{{y:.6f}}<br>{z_label}: %{{z:.6f}}<extra></extra>",
        })
    if pareto_configs:
        traces.append({
            "type": "scatter3d",
            "mode": "markers",
            "x": [_safe_number(cfg.suite_metrics.get(x_metric), digits=9) for cfg in pareto_configs],
            "y": [_safe_number(cfg.suite_metrics.get(y_metric), digits=9) for cfg in pareto_configs],
            "z": [_safe_number(cfg.suite_metrics.get(z_metric), digits=9) for cfg in pareto_configs],
            "marker": {"size": 4, "color": [_safe_number(cfg.suite_metrics.get(color_metric), digits=9) for cfg in pareto_configs] if color_metric else "red", "colorscale": "Viridis" if color_metric else None, "showscale": bool(color_metric) and not (show_all and non_pareto_configs), "cmin": color_range[0] if color_metric and color_range else None, "cmax": color_range[1] if color_metric and color_range else None, "colorbar": {"thickness": 18, "len": 0.82, "x": 0.98, "xanchor": "left", "y": 0.5, "yanchor": "middle", "outlinewidth": 0, "tickfont": {"color": "#fafafa"}} if color_metric and not (show_all and non_pareto_configs) else None, "line": {"width": 1, "color": "white"}, "opacity": 0.8},
            "name": "Pareto Front",
            "customdata": [[cfg.config_index] for cfg in pareto_configs],
            "hovertemplate": f"<b>Pareto Config %{{customdata[0]}}</b><br>{x_label}: %{{x:.6f}}<br>{y_label}: %{{y:.6f}}<br>{z_label}: %{{z:.6f}}<extra></extra>",
        })
    if best_match is not None:
        traces.append({
            "type": "scatter3d",
            "mode": "markers",
            "x": [_safe_number(best_match.suite_metrics.get(x_metric), digits=9)],
            "y": [_safe_number(best_match.suite_metrics.get(y_metric), digits=9)],
            "z": [_safe_number(best_match.suite_metrics.get(z_metric), digits=9)],
            "marker": {"size": 8, "color": "lime", "line": {"width": 2, "color": "darkgreen"}},
            "name": "Best Match",
            "customdata": [[best_match.config_index]],
            "hovertemplate": f"<b>Best Match Config %{{customdata[0]}}</b><br>{x_label}: %{{x:.6f}}<br>{y_label}: %{{y:.6f}}<br>{z_label}: %{{z:.6f}}<extra></extra>",
        })
    return {
        "traces": traces,
        "layout": {
            "title": f"{title_prefix}: {x_label} vs {y_label} vs {z_label}",
            "scene": {
                "xaxis": {"title": {"text": x_label}},
                "yaxis": {"title": {"text": y_label}},
                "zaxis": {"title": {"text": z_label}},
            },
            "height": 760,
            "template": "plotly_white",
            "clickmode": "event+select",
            "dragmode": "orbit",
            "margin": {"l": 0, "r": 80 if color_metric else 20, "t": 60, "b": 0},
        },
    }


def _build_bounds_rows(bounds_info: dict) -> list[dict]:
    rows: list[dict] = []
    for side_key, side_label in (("at_lower", "Lower"), ("at_upper", "Upper")):
        for param, info in sorted((bounds_info.get(side_key) or {}).items()):
            rows.append({
                "parameter": param,
                "side": side_label,
                "value": _safe_number((info or {}).get("value"), digits=9),
                "bound": _safe_number((info or {}).get("bound"), digits=9),
            })
    return rows


def _build_parameter_heatmap(loader: ParetoDataLoader, *, top_n: int = 20, bounds_info: dict | None = None) -> dict:
    df = loader.to_dataframe(pareto_only=False)
    if df.empty:
        return {"traces": [], "layout": {"title": "No data available"}}

    param_cols = [col for col in df.columns if col.startswith("long_") or col.startswith("short_")]
    if not param_cols:
        return {"traces": [], "layout": {"title": "No parameter data available"}}

    df_params_numeric = df[param_cols].apply(pd.to_numeric, errors="coerce")
    param_cols = [col for col in param_cols if df_params_numeric[col].notna().any()]
    if not param_cols:
        return {"traces": [], "layout": {"title": "No numeric parameter data available"}}

    near_bound_params: list[str] = []
    if bounds_info:
        near_bound_params = list((bounds_info.get("at_lower") or {}).keys()) + list((bounds_info.get("at_upper") or {}).keys())
        near_bound_params = [param for param in near_bound_params if param in param_cols]
    if near_bound_params:
        candidate_params = near_bound_params
    else:
        candidate_params = param_cols

    param_variance = df_params_numeric[param_cols].var().sort_values(ascending=False)
    params = [param for param in param_variance.index.tolist() if param in candidate_params][:top_n]

    metrics = list(loader.scoring_metrics or [])
    if not metrics:
        metrics = ["adg_w_usd", "sharpe_ratio_usd", "gain_usd", "drawdown_worst_usd"]
    metrics = [metric for metric in metrics if metric in df.columns]
    params = [param for param in params if param in df.columns]
    if not params or not metrics:
        return {"traces": [], "layout": {"title": "Insufficient data for correlation"}}

    df_corr = df[params + metrics].apply(pd.to_numeric, errors="coerce")
    min_non_null = min(10, max(3, int(len(df_corr) * 0.01)))

    def _is_usable_series(series: pd.Series) -> bool:
        series = series.dropna()
        return len(series) >= min_non_null and series.nunique() >= 2

    params = [param for param in params if _is_usable_series(df_corr[param])]
    metrics = [metric for metric in metrics if _is_usable_series(df_corr[metric])]
    if not params or not metrics:
        return {"traces": [], "layout": {"title": "Insufficient data for correlation"}}

    corr_matrix = df_corr[params + metrics].corr().loc[params, metrics]
    z = corr_matrix.to_numpy(dtype=float)
    z_nan = np.isnan(z)
    z_clean = np.where(z_nan, 0.0, z)
    text = np.where(z_nan, "", np.round(z_clean, 2))
    bound_meta_by_param = {param: _parameter_bound_summary(loader, param) for param in params}
    customdata = []
    for param in params:
        meta = bound_meta_by_param.get(param) or {}
        row = []
        for _metric in metrics:
            row.append([
                meta.get("side") or "n/a",
                f"{float(meta.get('distance_pct')):.2f}%" if meta.get("distance_pct") is not None else "n/a",
                f"{float(meta.get('bound_value')):.6g}" if meta.get("bound_value") is not None else "n/a",
                f"{float(meta.get('observed_value')):.6g}" if meta.get("observed_value") is not None else "n/a",
            ])
        customdata.append(row)

    return {
        "params": params,
        "metrics": metrics,
        "traces": [{
            "type": "heatmap",
            "z": z_clean.tolist(),
            "x": [metric.replace("_", " ").title() for metric in metrics],
            "y": [param.replace("_", " ").replace("long ", "").replace("short ", "") for param in params],
            "colorscale": "RdBu",
            "zmid": 0,
            "text": text.tolist(),
            "customdata": customdata,
            "texttemplate": "%{text}",
            "textfont": {"size": 14},
            "colorbar": {
                "title": {"text": "Correlation", "font": {"size": 15}},
                "tickfont": {"size": 13},
            },
            "hovertemplate": "Parameter %{y}<br>Metric %{x}<br>Correlation %{z:.2f}<br>Nearest bound %{customdata[0]}<br>Distance %{customdata[1]}<br>Observed %{customdata[3]}<br>Bound %{customdata[2]}<extra></extra>",
        }],
        "layout": {
            "title": "Parameter Influence Heatmap (Correlation Analysis)",
            "xaxis": {"title": "Metrics", "tickfont": {"size": 14}, "automargin": True},
            "yaxis": {"title": "Parameters", "tickfont": {"size": 17}, "automargin": True},
            "height": max(320, 120 + len(params) * 32),
            "margin": {"l": 240, "r": 80, "t": 64, "b": 56},
            "template": "plotly_white",
            "font": {"size": 15},
        },
    }


def _build_bounds_chart(loader: ParetoDataLoader, *, bounds_info: dict, top_n: int = 15) -> dict:
    at_bounds = list((bounds_info.get("at_lower") or {}).keys()) + list((bounds_info.get("at_upper") or {}).keys())
    if not at_bounds:
        return {"traces": [], "layout": {"title": "No parameters at bounds"}}

    pareto_configs = loader.get_pareto_configs()
    for config in pareto_configs:
        loader.ensure_bot_params(config)

    distances = []
    param_names = []
    colors = []

    for param in at_bounds[:top_n]:
        bounds = loader.optimize_bounds.get(param)
        if not isinstance(bounds, (list, tuple)) or len(bounds) < 2:
            continue
        try:
            lower = float(bounds[0])
            upper = float(bounds[1])
        except Exception:
            continue
        param_range = upper - lower
        if abs(param_range) < 1e-12:
            continue

        values = []
        for config in pareto_configs:
            value = getattr(config, "bot_params", {}).get(param)
            if isinstance(value, (int, float)):
                values.append(float(value))
        if not values:
            continue

        min_val = min(values)
        max_val = max(values)
        dist_lower = (min_val - lower) / param_range
        dist_upper = (upper - max_val) / param_range
        if dist_lower < dist_upper:
            distances.append(_safe_number(dist_lower * 100, digits=3))
            colors.append("red" if dist_lower < 0.05 else "orange")
            label = f"{param.replace('long_', '').replace('short_', '')} (Lower)"
        else:
            distances.append(_safe_number(dist_upper * 100, digits=3))
            colors.append("red" if dist_upper < 0.05 else "orange")
            label = f"{param.replace('long_', '').replace('short_', '')} (Upper)"
        param_names.append(label)

    if not param_names:
        return {"traces": [], "layout": {"title": "No parameters at bounds"}}

    return {
        "traces": [{
            "type": "bar",
            "orientation": "h",
            "x": distances,
            "y": param_names,
            "marker": {"color": colors},
            "text": [f"{float(distance):.1f}%" for distance in distances],
            "textposition": "auto",
            "textfont": {"size": 17},
            "hovertemplate": "<b>%{y}</b><br>Distance: %{x:.1f}%<extra></extra>",
        }],
        "layout": {
            "title": "Parameters Near Bounds (< 10% from limit)",
            "xaxis": {"title": "Distance from Bound (%)", "tickfont": {"size": 14}},
            "yaxis": {"title": "Parameter", "tickfont": {"size": 17}, "automargin": True},
            "height": max(320, 120 + len(param_names) * 32),
            "margin": {"l": 240, "r": 40, "t": 56, "b": 56},
            "font": {"size": 15},
            "uniformtext": {"minsize": 15, "mode": "show"},
            "template": "plotly_white",
        },
    }


def _parameter_bound_summary(loader: ParetoDataLoader, param: str) -> dict | None:
    bounds = loader.optimize_bounds.get(param)
    if not isinstance(bounds, (list, tuple)) or len(bounds) < 2:
        return None
    try:
        lower = float(bounds[0])
        upper = float(bounds[1])
    except Exception:
        return None

    param_range = upper - lower
    if abs(param_range) < 1e-12:
        return None

    pareto_configs = loader.get_pareto_configs()
    for config in pareto_configs:
        loader.ensure_bot_params(config)

    values = []
    for config in pareto_configs:
        value = getattr(config, "bot_params", {}).get(param)
        if isinstance(value, (int, float)):
            values.append(float(value))
    if not values:
        return None

    min_val = min(values)
    max_val = max(values)
    dist_lower = (min_val - lower) / param_range
    dist_upper = (upper - max_val) / param_range
    if dist_lower < dist_upper:
        return {
            "side": "Lower",
            "distance_pct": _safe_number(dist_lower * 100, digits=3),
            "distance_ratio": _safe_number(dist_lower, digits=6),
            "bound_value": _safe_number(lower, digits=9),
            "observed_value": _safe_number(min_val, digits=9),
            "range_pct": _safe_number(dist_lower * 100, digits=3),
        }
    return {
        "side": "Upper",
        "distance_pct": _safe_number(dist_upper * 100, digits=3),
        "distance_ratio": _safe_number(dist_upper, digits=6),
        "bound_value": _safe_number(upper, digits=9),
        "observed_value": _safe_number(max_val, digits=9),
        "range_pct": _safe_number(dist_upper * 100, digits=3),
    }


def _available_scenario_metrics(loader: ParetoDataLoader) -> list[str]:
    pareto_configs = loader.get_pareto_configs()
    if pareto_configs and isinstance(getattr(pareto_configs[0], "suite_metrics", None), dict):
        return list(pareto_configs[0].suite_metrics.keys())
    return list(loader.scoring_metrics or ["adg_w_usd", "sharpe_ratio_usd"])


def _available_metrics_superset(loader: ParetoDataLoader) -> list[str]:
    for config in (loader.configs or []):
        suite_metrics = getattr(config, "suite_metrics", None)
        if isinstance(suite_metrics, dict) and suite_metrics:
            return list(suite_metrics.keys())
    return list(loader.scoring_metrics or ["adg_w_usd"])


def _filter_metrics_for_currency_weight(available_metrics: list[str], *, use_weighted: bool, use_btc: bool) -> list[str]:
    available_set = set(available_metrics)
    filtered_metrics: list[str] = []
    for metric in available_metrics:
        is_usd = metric.endswith("_usd") or metric.endswith("_w_usd")
        is_btc = metric.endswith("_btc") or metric.endswith("_w_btc")

        if use_btc and is_usd:
            btc_version = metric.replace("_usd", "_btc").replace("_w_usd", "_w_btc")
            if btc_version in available_set:
                continue
        if (not use_btc) and is_btc:
            usd_version = metric.replace("_btc", "_usd").replace("_w_btc", "_w_usd")
            if usd_version in available_set:
                continue

        if use_weighted:
            if is_usd and not metric.endswith("_w_usd"):
                weighted_version = metric.replace("_usd", "_w_usd")
                if weighted_version in available_set:
                    continue
            if is_btc and not metric.endswith("_w_btc"):
                weighted_version = metric.replace("_btc", "_w_btc")
                if weighted_version in available_set:
                    continue
        else:
            if metric.endswith("_w_usd") and metric.replace("_w_usd", "_usd") in available_set:
                continue
            if metric.endswith("_w_btc") and metric.replace("_w_btc", "_btc") in available_set:
                continue

        filtered_metrics.append(metric)
    return filtered_metrics or list(available_metrics)


def _pick_default_metric(filtered_metrics: list[str], *, use_weighted: bool, use_btc: bool) -> str:
    currency = "btc" if use_btc else "usd"
    suffix = "_w" if use_weighted else ""
    preferred_defaults = [f"adg{suffix}_{currency}", f"sharpe_ratio{suffix}_{currency}", f"adg_{currency}", f"sharpe_ratio_{currency}"]
    for preferred in preferred_defaults:
        if preferred in filtered_metrics:
            return preferred
    return filtered_metrics[0] if filtered_metrics else ""


def _build_scenarios_boxplot(loader: ParetoDataLoader, *, metric: str) -> dict:
    configs = loader.get_pareto_configs()
    for config in configs:
        loader.ensure_details(config)

    if not configs or not loader.scenario_labels:
        return {"traces": [], "layout": {"title": "No scenario data available"}}

    traces: list[dict] = []
    for scenario in loader.scenario_labels:
        values = []
        for config in configs:
            if scenario not in (getattr(config, "scenario_metrics", None) or {}):
                continue
            value = (config.scenario_metrics or {}).get(scenario, {}).get(metric)
            try:
                value = float(value)
            except Exception:
                continue
            if math.isnan(value):
                continue
            values.append(value)
        if values:
            traces.append({
                "type": "box",
                "y": values,
                "name": str(scenario).title(),
                "boxmean": "sd",
                "hovertemplate": f"{str(scenario).title()}<br>{metric}: %{{y:.6f}}<extra></extra>",
            })

    return {
        "traces": traces,
        "layout": {
            "title": f"Scenario Comparison: {metric.replace('_', ' ').title()}",
            "yaxis": {"title": metric.replace("_", " ").title(), "tickfont": {"size": 14}},
            "xaxis": {"title": "Scenario", "tickfont": {"size": 14}},
            "height": 600,
            "template": "plotly_white",
            "showlegend": False,
            "font": {"size": 15},
        },
    }


def _build_scenario_statistics(loader: ParetoDataLoader, *, metric: str) -> list[dict]:
    pareto_configs = loader.get_pareto_configs()
    for config in pareto_configs:
        loader.ensure_details(config)

    rows: list[dict] = []
    for scenario in loader.scenario_labels:
        values = []
        for config in pareto_configs:
            if scenario not in (getattr(config, "scenario_metrics", None) or {}):
                continue
            value = (config.scenario_metrics or {}).get(scenario, {}).get(metric)
            try:
                value = float(value)
            except Exception:
                continue
            if math.isnan(value):
                continue
            values.append(value)
        if not values:
            continue
        rows.append({
            "scenario": str(scenario),
            "mean": _safe_number(float(np.mean(values)), digits=6),
            "std": _safe_number(float(np.std(values)), digits=6),
            "min": _safe_number(float(np.min(values)), digits=6),
            "max": _safe_number(float(np.max(values)), digits=6),
        })
    return rows


def _build_evolution_chart(loader: ParetoDataLoader, *, metric: str, window: int, show_all: bool, hide_outliers: bool, improvement_threshold_pct: float = 1.0) -> dict:
    all_df = loader.to_dataframe(pareto_only=False)
    if all_df.empty or metric not in all_df.columns:
        return {"traces": [], "layout": {"title": "No data available"}}

    all_df = all_df.sort_values("config_index")
    metric_values = pd.to_numeric(all_df[metric], errors="coerce")
    if hide_outliers:
        keep_mask = metric_values.isna() | (metric_values > -0.95)
        all_df = all_df[keep_mask].copy()
        metric_values = metric_values[keep_mask]
    if all_df.empty:
        return {"traces": [], "layout": {"title": "No data available"}}

    df = all_df if show_all else all_df[all_df.get("is_pareto") == True].copy()
    if df.empty:
        return {"traces": [], "layout": {"title": "No data available"}}

    df = df.sort_values("config_index")
    x_values = [_safe_number(value, digits=9) for value in df["config_index"].tolist()]
    metric_series = pd.to_numeric(df[metric], errors="coerce")
    y_values = [_safe_number(value, digits=9) for value in metric_series.tolist()]
    improvement_threshold_pct = max(0.0, min(25.0, float(improvement_threshold_pct or 0.0)))
    if show_all:
        traces: list[dict] = [{
            "type": "scatter",
            "mode": "markers",
            "x": x_values,
            "y": y_values,
            "marker": {"size": 5, "color": "lightblue", "opacity": 0.5},
            "name": "All Configs",
            "hovertemplate": f"<b>Config #%{{x:.0f}}</b><br>{metric.replace('_', ' ').title()}: %{{y:.6f}}<br><i>All visible configs</i><extra></extra>",
        }]
    else:
        traces = [{
            "type": "scatter",
            "mode": "markers",
            "x": x_values,
            "y": y_values,
            "marker": {"size": 10, "color": "red", "symbol": "star", "line": {"width": 1, "color": "white"}},
            "name": "Pareto Configs",
            "customdata": [[_safe_number(value, digits=9)] for value in df["config_index"].tolist()],
            "hovertemplate": f"<b>STAR Pareto Config #%{{customdata[0]:.0f}}</b><br>Iteration: %{{x:.0f}}<br>{metric.replace('_', ' ').title()}: %{{y:.6f}}<br><i>Click to view details</i><extra></extra>",
        }]

    lower_is_better = _metric_lower_is_better_for_loader(loader, metric)
    best_values: list[float | None] = []
    best_value: float | None = None
    best_config_index: int | None = None
    meaningful_best_value: float | None = None
    last_meaningful_improvement_index: int | None = None
    improvement_count = 0
    meaningful_improvement_count = 0
    best_at_80pct: float | None = None
    max_config_index = int(max(df["config_index"].tolist())) if x_values else None
    eighty_pct_index = int(max_config_index * 0.8) if max_config_index is not None else None
    for config_index, value in zip(df["config_index"].tolist(), metric_series.tolist()):
        try:
            numeric_value = float(value)
        except Exception:
            best_values.append(best_value)
            continue
        if not math.isfinite(numeric_value):
            best_values.append(best_value)
            continue
        improved = best_value is None or (numeric_value < best_value if lower_is_better else numeric_value > best_value)
        if improved:
            best_value = numeric_value
            best_config_index = int(config_index)
            improvement_count += 1
            if _is_meaningful_improvement(numeric_value, meaningful_best_value, lower_is_better=lower_is_better, threshold_pct=improvement_threshold_pct):
                meaningful_best_value = numeric_value
                last_meaningful_improvement_index = int(config_index)
                meaningful_improvement_count += 1
        best_values.append(best_value)
        if eighty_pct_index is not None and int(config_index) <= eighty_pct_index:
            best_at_80pct = best_value

    if any(value is not None for value in best_values):
        direction_label = "lowest" if lower_is_better else "highest"
        traces.append({
            "type": "scatter",
            "mode": "lines",
            "x": x_values,
            "y": [_safe_number(value, digits=9) for value in best_values],
            "line": {"color": "blue", "width": 2},
            "name": f"Best So Far ({direction_label})",
            "customdata": [[_safe_number(value, digits=9)] for value in df["config_index"].tolist()],
            "hovertemplate": f"<b>Iteration %{{x:.0f}}</b><br>Best so far: %{{y:.6f}}<br><i>Cumulative {direction_label} {metric}</i><extra></extra>",
        })

    pareto_df = all_df[all_df["is_pareto"] == True] if "is_pareto" in all_df.columns else pd.DataFrame()
    if show_all and not pareto_df.empty:
        traces.append({
            "type": "scatter",
            "mode": "markers",
            "x": [_safe_number(value, digits=9) for value in pareto_df["config_index"].tolist()],
            "y": [_safe_number(value, digits=9) for value in pd.to_numeric(pareto_df[metric], errors="coerce").tolist()],
            "marker": {"size": 10, "color": "red", "symbol": "star", "line": {"width": 1, "color": "white"}},
            "name": "Pareto Configs",
            "customdata": [[_safe_number(value, digits=9)] for value in pareto_df["config_index"].tolist()],
            "hovertemplate": f"<b>STAR Pareto Config #%{{customdata[0]:.0f}}</b><br>Iteration: %{{x:.0f}}<br>{metric.replace('_', ' ').title()}: %{{y:.6f}}<br><i>Click to view details</i><extra></extra>",
        })

    tail_improvement_pct = _relative_improvement_pct(best_value, best_at_80pct, lower_is_better=lower_is_better)
    suggested_min_iters = None
    if last_meaningful_improvement_index is not None:
        suggested_min_iters = int(math.ceil((last_meaningful_improvement_index * 1.05) / 1000) * 1000)
        if max_config_index is not None:
            suggested_min_iters = min(suggested_min_iters, max_config_index)

    return {
        "traces": traces,
        "layout": {
            "title": f"Evolution Timeline: {metric.replace('_', ' ').title()}",
            "xaxis": {"title": "Iteration", "separatethousands": False},
            "yaxis": {"title": metric.replace("_", " ").title(), "separatethousands": False},
            "height": 500,
            "template": "plotly_white",
            "hovermode": "closest",
            "dragmode": "zoom",
        },
        "best_so_far": {
            "direction": "min" if lower_is_better else "max",
            "best_config_index": best_config_index,
            "best_value": _safe_number(best_value, digits=9),
            "improvement_count": improvement_count,
            "meaningful_improvement_count": meaningful_improvement_count,
            "last_meaningful_improvement_config_index": last_meaningful_improvement_index,
            "improvement_threshold_pct": _safe_number(improvement_threshold_pct, digits=3),
            "best_at_80pct": _safe_number(best_at_80pct, digits=9),
            "tail_improvement_pct": _safe_number(tail_improvement_pct, digits=3),
            "suggested_min_iters": suggested_min_iters,
            "max_config_index": max_config_index,
        },
    }


def _is_meaningful_improvement(value: float, reference: float | None, *, lower_is_better: bool, threshold_pct: float) -> bool:
    if reference is None:
        return True
    delta = reference - value if lower_is_better else value - reference
    if delta <= 0:
        return False
    threshold = abs(reference) * (max(0.0, threshold_pct) / 100.0)
    return delta >= max(threshold, 1e-12)


def _relative_improvement_pct(value: float | None, reference: float | None, *, lower_is_better: bool) -> float | None:
    if value is None or reference is None:
        return None
    delta = reference - value if lower_is_better else value - reference
    if abs(reference) <= 1e-12:
        return None
    return (delta / abs(reference)) * 100.0


def _metric_lower_is_better(metric: str) -> bool:
    name = str(metric or "").strip().lower()
    if name.endswith(("_usd", "_btc")):
        name = name[:-4]
    lower_prefixes = (
        "drawdown_",
        "expected_shortfall_",
        "equity_balance_diff_",
        "loss_",
        "trade_loss_",
        "peak_recovery_",
        "position_held_",
        "position_unchanged_",
        "positions_held_",
        "high_exposure_",
        "hard_stop_",
    )
    lower_names = {
        "loss_profit_ratio",
        "paper_loss_ratio",
        "paper_loss_mean_ratio",
        "total_wallet_exposure_mean",
        "equity_volatility",
        "equity_volatility_w",
        "equity_choppiness",
        "equity_choppiness_w",
        "equity_jerkiness",
        "equity_jerkiness_w",
        "exponential_fit_error",
        "exponential_fit_error_w",
    }
    return name in lower_names or name.startswith(lower_prefixes)


def _metric_goal_for_loader(loader: ParetoDataLoader, metric: str | None) -> str | None:
    goals = getattr(loader, "scoring_goals", None) or {}
    metric_name = str(metric or "").strip()
    candidates = [metric_name]
    if metric_name.endswith("_w_usd"):
        candidates.append(metric_name[:-6] + "_usd")
    elif metric_name.endswith("_w_btc"):
        candidates.append(metric_name[:-6] + "_btc")
    elif metric_name.endswith("_w"):
        candidates.append(metric_name[:-2])
    if "_w_" in metric_name:
        candidates.append(metric_name.replace("_w_", "_", 1))
    for candidate in candidates:
        goal = str(goals.get(candidate, "")).strip().lower()
        if goal in {"min", "max"}:
            return goal
    return None


def _metric_lower_is_better_for_loader(loader: ParetoDataLoader, metric: str | None) -> bool:
    goal = _metric_goal_for_loader(loader, metric)
    if goal == "min":
        return True
    if goal == "max":
        return False
    return _metric_lower_is_better(str(metric or ""))


def _metric_sort_value_for_loader(loader: ParetoDataLoader, config: object, metric: str | None) -> float:
    value = _metric_numeric_value(config, metric)
    if value is None:
        return float("inf")
    return value if _metric_lower_is_better_for_loader(loader, metric) else -value


def _find_metric_variant(available_metrics: set[str], base_name: str, *, use_weighted: bool, currency_suffix: str, need_currency: bool) -> str:
    if need_currency:
        base_metric = f"{base_name}{currency_suffix}"
        weighted_metric = f"{base_name}_w{currency_suffix}"
        if use_weighted and weighted_metric in available_metrics:
            return weighted_metric
        return weighted_metric if weighted_metric in available_metrics else base_metric
    base_metric = base_name
    weighted_metric = f"{base_name}_w"
    if use_weighted and weighted_metric in available_metrics:
        return weighted_metric
    return weighted_metric if weighted_metric in available_metrics else base_metric


def _select_correlation_configs(loader: ParetoDataLoader, *, strategy: str, num_configs: int, use_weighted: bool, use_btc: bool) -> tuple[list[int], list[str]]:
    pareto_configs = loader.get_pareto_configs()
    if not pareto_configs:
        return [], []

    currency_suffix = "_btc" if use_btc else "_usd"
    available_metrics: set[str] = set()
    for config in pareto_configs:
        suite_metrics = getattr(config, "suite_metrics", None)
        if isinstance(suite_metrics, dict):
            available_metrics.update(str(metric) for metric in suite_metrics.keys())
    selected_configs: list[int] = []
    labels: list[str] = []

    if strategy == "Top Performers":
        metric_defs = [
            ("adg", True, True, "Top ADG", ".6f"),
            ("sharpe_ratio", True, True, "Top Sharpe", ".3f"),
            ("drawdown_worst", False, True, "Best DD", ".4f"),
            ("calmar_ratio", True, True, "Top Calmar", ".3f"),
            ("sortino_ratio", True, True, "Top Sortino", ".3f"),
            ("omega_ratio", True, True, "Top Omega", ".3f"),
            ("gain", True, True, "Top Gain", ".2f"),
            ("loss_profit_ratio", False, False, "Best L/P", ".3f"),
            ("position_held_hours_mean", False, False, "Fast Trade", ".1f"),
            ("volume_pct_per_day_avg", True, False, "Top Volume", ".2f"),
        ]
        candidates: list[tuple[object, str]] = []
        for base_name, maximize, need_currency, label_prefix, fmt in metric_defs:
            metric_name = _find_metric_variant(available_metrics, base_name, use_weighted=use_weighted, currency_suffix=currency_suffix, need_currency=need_currency)
            if metric_name not in available_metrics:
                continue
            lower_is_better = _metric_lower_is_better_for_loader(loader, metric_name)
            if maximize and lower_is_better:
                lower_is_better = False
            elif not maximize:
                lower_is_better = True
            def _candidate_sort_key(cfg: object) -> float:
                value = _metric_numeric_value(cfg, metric_name)
                if value is None:
                    return float("inf")
                return value if lower_is_better else -value

            best = min(pareto_configs, key=_candidate_sort_key, default=None)
            if not best:
                continue
            value = _metric_numeric_value(best, metric_name)
            if value is None:
                continue
            label = f"{label_prefix} ({value:{fmt}})"
            if base_name == "position_held_hours_mean":
                label = f"{label_prefix} ({value:.1f}h)"
            elif base_name == "volume_pct_per_day_avg":
                label = f"{label_prefix} ({value:.2f}%)"
            candidates.append((best, label))
        seen_config_indices: set[int] = set()
        for config, label in candidates:
            if len(selected_configs) >= num_configs:
                break
            if config.config_index in seen_config_indices:
                continue
            seen_config_indices.add(config.config_index)
            selected_configs.append(config.config_index)
            labels.append(f"#{config.config_index} {label}")

    elif strategy == "Diverse Styles":
        styles: dict[str, list] = {}
        for config in pareto_configs:
            style = loader.compute_trading_style(config)
            styles.setdefault(style, []).append(config)
        if not styles:
            return [], []

        style_metric = _find_metric_variant(available_metrics, "adg", use_weighted=use_weighted, currency_suffix=currency_suffix, need_currency=True)
        if style_metric not in available_metrics:
            style_metric = loader.scoring_metrics[0] if getattr(loader, "scoring_metrics", None) else "adg_w_usd"

        def style_key(cfg: ConfigMetrics) -> float:
            return _metric_sort_value_for_loader(loader, cfg, style_metric)

        for style in styles:
            styles[style] = sorted(styles[style], key=style_key)

        style_names = sorted(styles.keys())
        if not style_names:
            return [], []

        target_count = min(num_configs, sum(len(style_configs) for style_configs in styles.values()))
        round_num = 0
        while len(selected_configs) < target_count:
            added_this_round = False
            for style in style_names:
                style_configs = styles[style]
                if round_num >= len(style_configs):
                    continue
                config = style_configs[round_num]
                selected_configs.append(config.config_index)
                added_this_round = True
                adg = config.suite_metrics.get(style_metric, 0)
                labels.append(f"#{config.config_index} {style} ({style_metric}: {adg:.6f})")
                if len(selected_configs) >= target_count:
                    break
            if not added_this_round:
                break
            round_num += 1

    else:
        configs_with_risk = []
        for config in pareto_configs:
            risk_profile = loader.compute_risk_profile_score(config)
            avg_risk = sum(risk_profile.values()) / len(risk_profile) if risk_profile else 0
            configs_with_risk.append((config, avg_risk))
        configs_with_risk.sort(key=lambda item: item[1])
        if len(configs_with_risk) <= num_configs:
            selected_items = configs_with_risk
        else:
            step = (len(configs_with_risk) - 1) / (num_configs - 1)
            indices = [int(i * step) for i in range(num_configs)]
            selected_items = [configs_with_risk[index] for index in indices]
        for config, risk_score in selected_items:
            selected_configs.append(config.config_index)
            risk_label = "High Risk" if risk_score < 4 else "Medium Risk" if risk_score < 7 else "Low Risk"
            labels.append(f"#{config.config_index} {risk_label} ({risk_score:.1f}/10)")

    return selected_configs, labels


def _build_risk_profile_radar(loader: ParetoDataLoader, *, config_indices: list[int], labels: list[str]) -> dict:
    if not config_indices:
        return {"traces": [], "layout": {"title": "No configs selected"}}

    configs = []
    for target_idx in config_indices:
        matching = [config for config in (loader.configs or []) if getattr(config, "config_index", None) == target_idx]
        if matching:
            configs.append(matching[0])
    if not configs:
        return {"traces": [], "layout": {"title": "Invalid config indices"}}

    risk_dimensions = ["Drawdown", "Choppiness", "Jerkiness", "Tail Risk", "Loss Magnitude"]
    fill_traces: list[dict] = []
    line_traces: list[dict] = []
    palette = [
        ("lime", "rgba(0,255,100,0.18)", "star"),
        ("rgb(100,150,255)", "rgba(100,150,255,0.12)", "circle"),
        ("rgb(255,150,100)", "rgba(255,150,100,0.12)", "square"),
        ("rgb(150,100,255)", "rgba(150,100,255,0.12)", "diamond"),
        ("rgb(255,200,100)", "rgba(255,200,100,0.12)", "cross"),
        ("rgb(100,255,200)", "rgba(100,255,200,0.12)", "x"),
    ]
    for index, config in enumerate(configs):
        risk_scores = loader.compute_risk_profile_score(config)
        values = [
            risk_scores.get("drawdown", 0),
            risk_scores.get("choppiness", 0),
            risk_scores.get("jerkiness", 0),
            risk_scores.get("tail_risk", 0),
            risk_scores.get("loss_magnitude", 0),
        ]
        values_closed = values + [values[0]]
        theta_closed = risk_dimensions + [risk_dimensions[0]]
        label = labels[index] if index < len(labels) else f"Config #{config.config_index}"
        legend_group = f"config-{config.config_index}"
        line_color, fill_color, symbol = palette[min(index, len(palette) - 1)]

        fill_traces.append({
            "type": "scatterpolar",
            "mode": "lines",
            "r": values_closed,
            "theta": theta_closed,
            "fill": "toself",
            "name": label + " (area)",
            "line": {"color": "rgba(0,0,0,0)", "width": 0},
            "fillcolor": fill_color,
            "legendgroup": legend_group,
            "meta": {"config_index": config.config_index},
            "showlegend": False,
            "hoverinfo": "skip",
        })
        line_traces.append({
            "type": "scatterpolar",
            "mode": "lines+markers",
            "r": values_closed,
            "theta": theta_closed,
            "name": label,
            "line": {"color": line_color, "width": 3 if index == 0 else 2},
            "marker": {
                "size": 11 if index == 0 else 9,
                "symbol": symbol,
                "color": line_color,
                "line": {"color": "darkgreen" if index == 0 else "white", "width": 2 if index == 0 else 1},
            },
            "legendgroup": legend_group,
            "meta": {"config_index": config.config_index},
            "customdata": [[config.config_index]] * len(values_closed),
            "hovertemplate": "<b>%{fullData.name}</b><br>%{theta}: %{r:.1f}/10<extra></extra>",
            "hoveron": "points",
        })

    return {
        "traces": fill_traces + line_traces,
        "layout": {
            "polar": {
                "domain": {"x": [0.08, 0.64], "y": [0.08, 0.94]},
                "radialaxis": {
                    "visible": True,
                    "range": [0, 10],
                    "gridcolor": "rgba(255,255,255,0.18)",
                    "linecolor": "rgba(255,255,255,0.22)",
                    "tickfont": {"color": "#fafafa"},
                },
                "angularaxis": {
                    "gridcolor": "rgba(255,255,255,0.18)",
                    "linecolor": "rgba(255,255,255,0.22)",
                    "tickfont": {"color": "#fafafa", "size": 12},
                },
                "bgcolor": "#0e1117",
            },
            "title": "Risk Profile Comparison (Higher = Lower Risk)",
            "height": 560,
            "autosize": True,
            "margin": {"l": 56, "r": 300, "t": 56, "b": 32},
            "template": "plotly_white",
            "legend": {
                "itemclick": "toggle",
                "itemdoubleclick": "toggleothers",
                "x": 1.01,
                "xanchor": "left",
                "y": 1.0,
                "yanchor": "top",
                "orientation": "v",
            },
            "dragmode": "zoom",
            "hovermode": "closest",
        },
    }


def _serialize_config_detail(
    loader: ParetoDataLoader,
    config_index: int,
    *,
    perf_weight: int = 80,
    risk_weight: int = 60,
    robust_weight: int = 70,
    score_configs_override: list | None = None,
) -> dict:
    config = loader.get_config_by_index(config_index)
    if config is None:
        raise HTTPException(status_code=404, detail="Config not found")

    loader.ensure_bot_params(config)
    loader.ensure_details(config)
    risk_profile = loader.compute_risk_profile_score(config)
    full_config = loader.get_full_config(config.config_index)
    preferred_metrics: list[str] = []
    for metric_name in list(loader.scoring_metrics or []):
        if metric_name not in preferred_metrics:
            preferred_metrics.append(metric_name)
    for metric_name in ["gain_usd", "drawdown_worst_usd"]:
        if metric_name in (config.suite_metrics or {}) and metric_name not in preferred_metrics:
            preferred_metrics.append(metric_name)

    metric_label_overrides = {
        "gain_usd": "gain",
        "drawdown_worst_usd": "max_drawdown",
    }

    top_metric_names = preferred_metrics[:5]
    if "gain_usd" in (config.suite_metrics or {}) and "gain_usd" not in top_metric_names:
        top_metric_names.append("gain_usd")

    top_metrics = []
    for metric_name in top_metric_names:
        if metric_name in config.suite_metrics:
            top_metrics.append({
                "name": metric_label_overrides.get(metric_name, metric_name),
                "value": _safe_number(config.suite_metrics.get(metric_name)),
            })

    all_metrics = [
        {"name": metric_name, "value": _safe_number(metric_value)}
        for metric_name, metric_value in sorted((config.suite_metrics or {}).items())
    ]

    primary_metric = loader.scoring_metrics[0] if loader.scoring_metrics else "adg_w_usd"
    score_source = score_configs_override if score_configs_override is not None else (loader.get_pareto_configs() or [])
    score_configs = [cfg for cfg in score_source if cfg is not None]
    if not score_configs:
        score_configs = [cfg for cfg in (loader.configs or [])[:100] if cfg is not None]
    risk_metric = "drawdown_worst_usd"
    explorer_score = _compute_explorer_score(
        loader,
        config,
        primary_metric=primary_metric,
        performance_bounds=_metric_bounds(score_configs, primary_metric),
        risk_metric=risk_metric,
        risk_bounds=_metric_bounds(score_configs, risk_metric),
        perf_weight=perf_weight,
        risk_weight=risk_weight,
        robust_weight=robust_weight,
    )

    return {
        "config_index": config.config_index,
        "is_pareto": bool(config.is_pareto),
        "style": loader.compute_trading_style(config),
        "robustness": _safe_number(loader.compute_overall_robustness(config), digits=4),
        "explorer_score": _safe_number(explorer_score, digits=6),
        "risk_profile": {key: _safe_number(value, digits=4) for key, value in risk_profile.items()},
        "top_metrics": top_metrics,
        "all_metrics": all_metrics,
        "scenario_labels": list(loader.scenario_labels),
        "scenario_metrics": _json_safe(config.scenario_metrics),
        "has_scenarios": bool(loader.scenario_labels) and bool(config.scenario_metrics),
        "full_config": _json_safe(full_config),
    }


def _serialize_load_result_from_loader(
    result_dir: Path,
    loader: ParetoDataLoader,
    *,
    all_results_loaded: bool,
    load_strategy: list[str],
    max_configs: int,
    view_range: object = None,
) -> dict:
    load_stats = dict(getattr(loader, "load_stats", {}) or {})
    visible_loader, normalized_range = _visible_loader(loader, all_results_loaded=all_results_loaded, view_range=view_range)
    visible_configs = list(visible_loader.configs or [])

    return {
        "result": _result_meta(result_dir),
        "mode": "full" if all_results_loaded else "fast",
        "load_strategy": list(load_strategy),
        "max_configs": max_configs,
        "load_stats": load_stats,
        "summary": _loader_summary(visible_loader, visible_configs=visible_configs),
        "view_range": normalized_range,
        "scoring_metrics": list(getattr(loader, "scoring_metrics", []) or []),
        "scenario_labels": list(getattr(loader, "scenario_labels", []) or []),
        "messages": _build_load_messages(result_dir, load_stats, all_results_loaded),
    }


def _serialize_load_result(result_path: str, *, all_results_loaded: bool, load_strategy: list[str], max_configs: int, view_range: object = None) -> dict:
    result_dir = _resolve_result_dir(result_path)
    if result_dir is None:
        raise HTTPException(status_code=404, detail="Invalid optimize result path")

    loader = _load_loader(
        result_path,
        all_results_loaded=all_results_loaded,
        load_strategy=load_strategy,
        max_configs=max_configs,
    )
    return _serialize_load_result_from_loader(
        result_dir,
        loader,
        all_results_loaded=all_results_loaded,
        load_strategy=load_strategy,
        max_configs=max_configs,
        view_range=view_range,
    )


def _run_full_load_job(job_id: str) -> None:
    job = _get_load_job(job_id)
    if job is None:
        return
    with _LOAD_WORKERS_LOCK:
        cancel_event = _LOAD_CANCEL_EVENTS.get(job_id)
    if cancel_event is None:
        cancel_event = threading.Event()

    result_path = str(job.get("result_path") or "")
    load_strategy = [str(item) for item in list(job.get("load_strategy") or []) if str(item).strip()]
    max_configs = _normalize_max_configs(job.get("max_configs"))

    try:
        if cancel_event.is_set():
            raise InterruptedError("Full load interrupted by API shutdown")
        result_dir = _resolve_result_dir(result_path)
        if result_dir is None:
            raise HTTPException(status_code=404, detail="Invalid optimize result path")

        def _progress_callback(current: object, total: object, message: object) -> None:
            if cancel_event.is_set():
                raise InterruptedError("Full load interrupted by API shutdown")
            try:
                current_num = float(current or 0)
            except Exception:
                current_num = 0.0
            try:
                total_num = float(total or 0)
            except Exception:
                total_num = 0.0
            if total_num > 0:
                progress = int(max(0, min(99, round((current_num / total_num) * 100))))
            else:
                progress = 0
            _update_load_job(
                job_id,
                status="loading",
                stage="loading",
                message=str(message or "Loading full result set..."),
                current=current_num,
                total=total_num,
                progress=progress,
                error=None,
            )

        loader = _load_loader(
            result_path,
            all_results_loaded=True,
            load_strategy=load_strategy,
            max_configs=max_configs,
            progress_callback=_progress_callback,
        )
        if cancel_event.is_set():
            raise InterruptedError("Full load interrupted by API shutdown")
        payload = _serialize_load_result_from_loader(
            result_dir,
            loader,
            all_results_loaded=True,
            load_strategy=load_strategy,
            max_configs=max_configs,
            view_range=(job.get("refresh_options") or {}).get("view_range"),
        )
        if cancel_event.is_set():
            raise InterruptedError("Full load interrupted by API shutdown")
        payload["refresh_bundle"] = _build_server_refresh_bundle(
            loader,
            all_results_loaded=True,
            options=dict(job.get("refresh_options") or {}),
        )
        _update_load_job(
            job_id,
            status="complete",
            stage="loaded",
            message=f"Full result loaded: {len(loader.configs)} configs ready.",
            current=1,
            total=1,
            progress=100,
            payload=payload,
            error=None,
        )
    except HTTPException as exc:
        if cancel_event.is_set():
            _update_load_job(
                job_id,
                status="error",
                stage="interrupted",
                message="Full load interrupted by API shutdown.",
                error="Full load interrupted by API shutdown.",
            )
            return
        _update_load_job(
            job_id,
            status="error",
            stage="error",
            message=str(exc.detail),
            error=str(exc.detail),
            progress=0,
        )
    except InterruptedError as exc:
        _update_load_job(
            job_id,
            status="error",
            stage="interrupted",
            message=str(exc),
            error=str(exc),
        )
    except Exception as exc:
        _update_load_job(
            job_id,
            status="error",
            stage="error",
            message=f"Full load failed: {exc}",
            error=str(exc),
            progress=0,
        )
    finally:
        dedupe_key = str(job.get("dedupe_key") or "")
        with _LOAD_WORKERS_LOCK:
            _LOAD_WORKERS.pop(job_id, None)
            _LOAD_CANCEL_EVENTS.pop(job_id, None)
            if dedupe_key and _LOAD_KEYS.get(dedupe_key) == job_id:
                _LOAD_KEYS.pop(dedupe_key, None)


def restart_block_reason() -> str:
    """Return a reason while a full Pareto result load is active."""
    with _LOAD_WORKERS_LOCK:
        active = sum(1 for thread in _LOAD_WORKERS.values() if thread.is_alive())
    if active:
        return f"Pareto Explorer has {active} active full-load job(s)"
    return ""


async def shutdown() -> None:
    """Interrupt and join every API-owned Pareto full-load thread."""
    with _LOAD_WORKERS_LOCK:
        workers = [(job_id, thread) for job_id, thread in _LOAD_WORKERS.items() if thread.ident is not None]
        for job_id, _thread in workers:
            cancel_event = _LOAD_CANCEL_EVENTS.get(job_id)
            if cancel_event is not None:
                cancel_event.set()
    if workers:
        await asyncio.gather(
            *(asyncio.to_thread(thread.join) for _job_id, thread in workers),
            return_exceptions=True,
        )
    with _LOAD_WORKERS_LOCK:
        _LOAD_WORKERS.clear()
        _LOAD_CANCEL_EVENTS.clear()
        _LOAD_KEYS.clear()


def _build_load_messages(result_dir: Path, load_stats: dict, all_results_loaded: bool) -> list[dict]:
    messages: list[dict] = []

    pareto_count = int(load_stats.get("pareto_configs") or 0)
    if pareto_count <= 0:
        messages.append({
            "level": "warning",
            "text": "No pareto configs were loaded.",
        })
    return messages


def _build_command_center_payload(loader: ParetoDataLoader) -> dict:
    return {
        "ok": True,
        "summary": _loader_summary(loader),
        "champions": _build_champions(loader),
        "insights": _build_insights(loader),
        "config_selector": _build_config_options(loader),
    }


def _build_playground_payload(
    loader: ParetoDataLoader,
    *,
    all_results_loaded: bool,
    perf_weight: int,
    risk_weight: int,
    robust_weight: int,
    show_all: bool,
    use_weighted: bool,
    preview_use_weighted: bool,
    preview_show_all: bool,
    selected_config_index: int | None,
    use_btc: bool,
    viz_type: str,
    quick_view: str,
    color_metric_input: str,
    custom_x_metric: str,
    custom_y_metric: str,
    custom_z_metric: str,
    view_range: object = None,
) -> dict:
    visible_loader, normalized_range = _visible_loader(loader, all_results_loaded=all_results_loaded, view_range=view_range)
    primary_metric = visible_loader.scoring_metrics[0] if visible_loader.scoring_metrics else "adg_w_usd"

    x_metric = _resolve_existing_metric(visible_loader, _metric_variants("adg", use_weighted=use_weighted, use_btc=use_btc)) or primary_metric or "adg_w_usd"
    y_metric = _resolve_existing_metric(visible_loader, _metric_variants("drawdown_worst", use_weighted=use_weighted, use_btc=use_btc)) or "drawdown_worst_usd"
    color_metric = None
    z_metric = _resolve_existing_metric(visible_loader, _metric_variants("equity_jerkiness", use_weighted=use_weighted, use_btc=use_btc)) or _resolve_existing_metric(visible_loader, _metric_variants("sharpe_ratio", use_weighted=use_weighted, use_btc=use_btc)) or primary_metric or x_metric

    preset_2d = {
        "Profit vs Risk": (
            _resolve_existing_metric(visible_loader, _metric_variants("adg", use_weighted=use_weighted, use_btc=use_btc)) or x_metric,
            _resolve_existing_metric(visible_loader, _metric_variants("drawdown_worst", use_weighted=use_weighted, use_btc=use_btc)) or y_metric,
        ),
        "Risk-Adjusted": (
            _resolve_existing_metric(visible_loader, _metric_variants("sharpe_ratio", use_weighted=use_weighted, use_btc=use_btc)) or x_metric,
            _resolve_existing_metric(visible_loader, _metric_variants("sortino_ratio", use_weighted=use_weighted, use_btc=use_btc)) or y_metric,
        ),
        "Profit vs Quality": (
            _resolve_existing_metric(visible_loader, _metric_variants("adg", use_weighted=use_weighted, use_btc=use_btc)) or x_metric,
            _resolve_existing_metric(visible_loader, _metric_variants("equity_choppiness", use_weighted=use_weighted, use_btc=use_btc)) or y_metric,
        ),
        "Efficiency": (
            _resolve_existing_metric(visible_loader, _metric_variants("adg", use_weighted=use_weighted, use_btc=use_btc)) or x_metric,
            _resolve_existing_metric(visible_loader, _exposure_metric_variants(use_weighted=use_weighted, use_btc=use_btc)) or y_metric,
        ),
        "Multi-Risk": (
            _resolve_existing_metric(visible_loader, _metric_variants("drawdown_worst", use_weighted=use_weighted, use_btc=use_btc)) or x_metric,
            _resolve_existing_metric(visible_loader, _metric_variants("expected_shortfall_1pct", use_weighted=use_weighted, use_btc=use_btc)) or y_metric,
        ),
        "Profit vs Recovery": (
            _resolve_existing_metric(visible_loader, _metric_variants("adg", use_weighted=use_weighted, use_btc=use_btc)) or x_metric,
            _resolve_existing_metric(visible_loader, _metric_variants("peak_recovery_hours_equity", use_weighted=use_weighted, use_btc=use_btc)) or y_metric,
        ),
        "Performance Ratios": (
            _resolve_existing_metric(visible_loader, _metric_variants("calmar_ratio", use_weighted=use_weighted, use_btc=use_btc)) or x_metric,
            _resolve_existing_metric(visible_loader, _metric_variants("omega_ratio", use_weighted=use_weighted, use_btc=use_btc)) or y_metric,
        ),
        "Exposure Analysis": (
            _resolve_existing_metric(visible_loader, _metric_variants("adg", use_weighted=use_weighted, use_btc=use_btc)) or x_metric,
            _resolve_existing_metric(visible_loader, ["total_wallet_exposure_mean"]) or y_metric,
        ),
    }
    preset_3d = {
        "Risk-Reward Triangle": (
            _resolve_existing_metric(visible_loader, _metric_variants("adg", use_weighted=use_weighted, use_btc=use_btc)) or x_metric,
            _resolve_existing_metric(visible_loader, _metric_variants("drawdown_worst", use_weighted=use_weighted, use_btc=use_btc)) or y_metric,
            _resolve_existing_metric(visible_loader, _metric_variants("equity_jerkiness", use_weighted=use_weighted, use_btc=use_btc)) or z_metric,
        ),
        "Recovery Performance": (
            _resolve_existing_metric(visible_loader, _metric_variants("adg", use_weighted=use_weighted, use_btc=use_btc)) or x_metric,
            _resolve_existing_metric(visible_loader, _metric_variants("peak_recovery_hours_equity", use_weighted=use_weighted, use_btc=use_btc)) or y_metric,
            _resolve_existing_metric(visible_loader, _metric_variants("drawdown_worst", use_weighted=use_weighted, use_btc=use_btc)) or z_metric,
        ),
        "Trading Efficiency": (
            _resolve_existing_metric(visible_loader, _metric_variants("adg", use_weighted=use_weighted, use_btc=use_btc)) or x_metric,
            _resolve_existing_metric(visible_loader, _exposure_metric_variants(use_weighted=use_weighted, use_btc=use_btc)) or y_metric,
            _resolve_existing_metric(visible_loader, ["total_wallet_exposure_mean"]) or z_metric,
        ),
        "Risk Spectrum": (
            _resolve_existing_metric(visible_loader, _metric_variants("sharpe_ratio", use_weighted=use_weighted, use_btc=use_btc)) or x_metric,
            _resolve_existing_metric(visible_loader, _metric_variants("sortino_ratio", use_weighted=use_weighted, use_btc=use_btc)) or y_metric,
            _resolve_existing_metric(visible_loader, _metric_variants("calmar_ratio", use_weighted=use_weighted, use_btc=use_btc)) or z_metric,
        ),
        "Stability Analysis": (
            _resolve_existing_metric(visible_loader, _metric_variants("adg", use_weighted=use_weighted, use_btc=use_btc)) or x_metric,
            _resolve_existing_metric(visible_loader, _metric_variants("equity_choppiness", use_weighted=use_weighted, use_btc=use_btc)) or y_metric,
            _resolve_existing_metric(visible_loader, ["loss_profit_ratio"]) or z_metric,
        ),
        "Trading Activity": (
            _resolve_existing_metric(visible_loader, _metric_variants("adg", use_weighted=use_weighted, use_btc=use_btc)) or x_metric,
            _resolve_existing_metric(visible_loader, ["positions_held_per_day"]) or y_metric,
            _resolve_existing_metric(visible_loader, ["position_held_hours_mean"]) or z_metric,
        ),
        "Stress Test": (
            _resolve_existing_metric(visible_loader, _metric_variants("drawdown_worst", use_weighted=use_weighted, use_btc=use_btc)) or x_metric,
            _resolve_existing_metric(visible_loader, _metric_variants("expected_shortfall_1pct", use_weighted=use_weighted, use_btc=use_btc)) or y_metric,
            _resolve_existing_metric(visible_loader, ["loss_profit_ratio"]) or z_metric,
        ),
    }

    if quick_view == "Custom...":
        x_metric = _resolve_existing_metric(visible_loader, [custom_x_metric]) or x_metric
        y_metric = _resolve_existing_metric(visible_loader, [custom_y_metric]) or y_metric
        if viz_type in {"3D Scatter", "3D Projections"}:
            z_metric = _resolve_existing_metric(visible_loader, [custom_z_metric]) or z_metric
    elif viz_type == "2D Scatter" and quick_view in preset_2d:
        x_metric, y_metric = preset_2d[quick_view]
    elif viz_type in {"3D Scatter", "3D Projections"} and quick_view in preset_3d:
        x_metric, y_metric, z_metric = preset_3d[quick_view]

    if color_metric_input and color_metric_input != "None":
        color_metric = _resolve_existing_metric(visible_loader, [color_metric_input]) or color_metric

    score_metrics = [x_metric, y_metric]
    if viz_type in {"3D Scatter", "3D Projections"}:
        score_metrics.append(z_metric)
    best_match_configs = list(visible_loader.configs if show_all else visible_loader.get_pareto_configs())

    best_match, best_score, primary_metric = _compute_best_match(
        visible_loader,
        perf_weight=perf_weight,
        risk_weight=risk_weight,
        robust_weight=robust_weight,
        performance_metric=x_metric,
        risk_metric=y_metric,
        score_metrics=score_metrics,
        configs_override=best_match_configs,
    )
    selected_config = visible_loader.get_config_by_index(selected_config_index) if selected_config_index is not None else None
    visualizations = {
        "preview": _build_preview_payload(
            visible_loader,
            all_results_loaded=all_results_loaded,
            use_weighted=preview_use_weighted,
            show_all=preview_show_all,
            selected_config_index=selected_config_index,
            view_range=normalized_range,
        ),
        "scatter_2d": _build_playground_scatter(
            visible_loader,
            x_metric=x_metric,
            y_metric=y_metric,
            color_metric=color_metric,
            show_all=show_all,
            best_match=best_match,
            selected_config=selected_config,
            title_prefix=quick_view or "Profit vs Risk",
        ),
        "scatter_3d": _build_playground_3d(
            visible_loader,
            x_metric=x_metric,
            y_metric=y_metric,
            z_metric=z_metric,
            color_metric=color_metric,
            show_all=show_all,
            best_match=best_match,
            title_prefix=quick_view or "3D Scatter",
        ),
        "projections": {
            "xy": _build_playground_projection_chart(visible_loader, x_metric=x_metric, y_metric=y_metric, z_metric=z_metric, best_match=best_match, selected_config=selected_config, title_prefix="", color_metric=color_metric, show_all=show_all),
            "xz": _build_playground_projection_chart(visible_loader, x_metric=x_metric, y_metric=z_metric, z_metric=y_metric, best_match=best_match, selected_config=selected_config, title_prefix="", color_metric=color_metric, show_all=show_all),
            "yz": _build_playground_projection_chart(visible_loader, x_metric=y_metric, y_metric=z_metric, z_metric=x_metric, best_match=best_match, selected_config=selected_config, title_prefix="", color_metric=color_metric, show_all=show_all),
        },
        "radar": _build_playground_radar(visible_loader, best_match=best_match, quick_view=quick_view),
    }

    return {
        "ok": True,
        "best_match": {
            "config_index": getattr(best_match, "config_index", None),
            "score": best_score,
            "style": loader.compute_trading_style(best_match) if best_match is not None else None,
        },
        "weights": {
            "performance": perf_weight,
            "risk": risk_weight,
            "robustness": robust_weight,
        },
        "metrics": {
            "x_metric": x_metric,
            "y_metric": y_metric,
            "z_metric": z_metric,
            "color_metric": color_metric,
            "primary_metric": primary_metric,
        },
        "viz_type": viz_type,
        "quick_view": quick_view,
        "available_metrics": _available_metrics_superset(visible_loader),
        "view_range": normalized_range,
        "visualizations": visualizations,
    }


def _build_deep_parameters_payload(loader: ParetoDataLoader, *, top_n: int) -> dict:
    bounds_info = loader.get_parameters_at_bounds(tolerance=0.1)
    rows = _build_bounds_rows(bounds_info)
    effective_top_n = min(top_n, len(rows)) if rows else top_n
    return {
        "ok": True,
        "near_bounds_count": len(rows),
        "top_n": effective_top_n,
        "rows": rows,
        "heatmap": _build_parameter_heatmap(loader, top_n=effective_top_n, bounds_info=bounds_info),
        "bounds_chart": _build_bounds_chart(loader, bounds_info=bounds_info, top_n=effective_top_n),
    }


def _build_deep_scenarios_payload(loader: ParetoDataLoader, *, metric: str) -> dict:
    available_metrics = _available_scenario_metrics(loader)
    selected_metric = str(metric or "").strip()
    if selected_metric not in available_metrics:
        selected_metric = available_metrics[0] if available_metrics else ""
    return {
        "ok": True,
        "has_scenarios": bool(loader.scenario_labels),
        "scenario_labels": list(loader.scenario_labels),
        "available_metrics": available_metrics,
        "selected_metric": selected_metric,
        "chart": _build_scenarios_boxplot(loader, metric=selected_metric) if selected_metric else {"traces": [], "layout": {"title": "No scenario metric available"}},
        "statistics": _build_scenario_statistics(loader, metric=selected_metric) if selected_metric else [],
    }


def _build_deep_evolution_payload(
    loader: ParetoDataLoader,
    *,
    all_results_loaded: bool,
    metric: str,
    use_weighted: bool,
    use_btc: bool,
    window_percent: int,
    improvement_threshold_pct: float,
    show_all: bool,
    hide_outliers: bool,
) -> dict:
    available_metrics = _available_metrics_superset(loader)
    filtered_metrics = _filter_metrics_for_currency_weight(available_metrics, use_weighted=use_weighted, use_btc=use_btc)
    selected_metric = str(metric or "").strip()
    if selected_metric not in filtered_metrics:
        selected_metric = _pick_default_metric(filtered_metrics, use_weighted=use_weighted, use_btc=use_btc)
    visible_configs = len(loader.configs or [])
    displayed_configs = visible_configs if show_all else len(loader.get_pareto_configs() or [])
    window = max(10, int(displayed_configs * window_percent / 100)) if displayed_configs else 10
    if not all_results_loaded:
        return {
            "ok": True,
            "available_metrics": filtered_metrics,
            "selected_metric": selected_metric,
            "use_weighted": use_weighted,
            "use_btc": use_btc,
            "show_all": show_all,
            "hide_outliers": hide_outliers,
            "window_percent": window_percent,
            "improvement_threshold_pct": _safe_number(improvement_threshold_pct, digits=3),
            "window": window,
            "total_configs": visible_configs,
            "displayed_configs": displayed_configs,
            "requires_full_mode": True,
            "message": "Evolution requires full mode because pareto JSON files do not contain the original all_results config index.",
            "chart": {"traces": [], "layout": {"title": "Load all_results.bin to inspect when Pareto configs were found."}},
        }
    return {
        "ok": True,
        "available_metrics": filtered_metrics,
        "selected_metric": selected_metric,
        "use_weighted": use_weighted,
        "use_btc": use_btc,
        "show_all": show_all,
        "hide_outliers": hide_outliers,
        "window_percent": window_percent,
        "improvement_threshold_pct": _safe_number(improvement_threshold_pct, digits=3),
        "window": window,
        "total_configs": visible_configs,
        "displayed_configs": displayed_configs,
        "requires_full_mode": False,
        "chart": _build_evolution_chart(loader, metric=selected_metric, window=window, show_all=show_all, hide_outliers=hide_outliers, improvement_threshold_pct=improvement_threshold_pct) if selected_metric else {"traces": [], "layout": {"title": "No evolution metric available"}},
    }


def _build_deep_correlations_payload(
    loader: ParetoDataLoader,
    *,
    strategy: str,
    num_configs: int,
    use_weighted: bool,
    use_btc: bool,
) -> dict:
    if strategy not in {"Top Performers", "Diverse Styles", "Risk Spectrum"}:
        strategy = "Top Performers"
    selected_configs, labels = _select_correlation_configs(
        loader,
        strategy=strategy,
        num_configs=num_configs,
        use_weighted=use_weighted,
        use_btc=use_btc,
    )
    available_styles = sorted({loader.compute_trading_style(config) for config in loader.get_pareto_configs()}) if strategy == "Diverse Styles" else []
    return {
        "ok": True,
        "strategy": strategy,
        "num_configs": num_configs,
        "use_weighted": use_weighted,
        "use_btc": use_btc,
        "available_styles": available_styles,
        "available_style_count": len(available_styles),
        "selected_configs": selected_configs,
        "labels": labels,
        "chart": _build_risk_profile_radar(loader, config_indices=selected_configs, labels=labels),
    }


def _build_deep_intelligence_payload(loader: ParetoDataLoader, *, deep_tab: str, options: dict, all_results_loaded: bool = True) -> dict:
    tab = str(deep_tab or "parameters").strip() or "parameters"
    if tab == "scenarios":
        payload = _build_deep_scenarios_payload(loader, metric=str(options.get("deep_scenarios_metric") or ""))
    elif tab == "evolution":
        payload = _build_deep_evolution_payload(
            loader,
            all_results_loaded=all_results_loaded,
            metric=str(options.get("deep_evolution_metric") or ""),
            use_weighted=bool(options.get("deep_evolution_use_weighted", True)),
            use_btc=bool(options.get("deep_evolution_use_btc", False)),
            window_percent=max(1, min(25, int(options.get("deep_evolution_window_percent", 5) or 5))),
            improvement_threshold_pct=max(0.0, min(25.0, float(options.get("deep_evolution_improvement_threshold_pct", 1) or 0.0))),
            show_all=bool(options.get("deep_evolution_show_all", False)),
            hide_outliers=bool(options.get("deep_evolution_hide_outliers", True)),
        )
    elif tab == "correlations":
        payload = _build_deep_correlations_payload(
            loader,
            strategy=str(options.get("deep_correlations_strategy") or "Top Performers"),
            num_configs=max(3, min(10, int(options.get("deep_correlations_num_configs", 5) or 5))),
            use_weighted=bool(options.get("deep_correlations_use_weighted", True)),
            use_btc=bool(options.get("deep_correlations_use_btc", False)),
        )
    else:
        tab = "parameters"
        requested_top_n = options.get("deep_parameters_top_n", 20)
        try:
            top_n = int(requested_top_n or 20)
        except Exception:
            top_n = 20
        top_n = max(1, min(40, top_n))
        payload = _build_deep_parameters_payload(loader, top_n=top_n)
    return {"tab": tab, "payload": payload}


def _resolve_selected_config_index(loader: ParetoDataLoader, *, requested_index: object, command_center: dict) -> int | None:
    try:
        parsed = int(requested_index) if requested_index is not None else None
    except Exception:
        parsed = None
    if parsed is not None and loader.get_config_by_index(parsed) is not None:
        return parsed
    champions = list((command_center or {}).get("champions") or [])
    if champions:
        try:
            return int(champions[0].get("config_index"))
        except Exception:
            pass
    selector = (command_center or {}).get("config_selector") or {}
    options = list(selector.get("options") or [])
    if options:
        try:
            return int(options[0].get("config_index"))
        except Exception:
            pass
    return None


def _build_server_refresh_bundle(loader: ParetoDataLoader, *, all_results_loaded: bool, options: dict) -> dict:
    requested_view_range = (options or {}).get("view_range")
    visible_loader, normalized_range = _visible_loader(loader, all_results_loaded=all_results_loaded, view_range=requested_view_range)
    command_center_loader = _clone_loader(visible_loader, visible_configs=list(visible_loader.configs or []), view_range=normalized_range)
    detail_loader = _clone_loader(visible_loader, visible_configs=list(visible_loader.configs or []), view_range=normalized_range)
    playground_loader = _clone_loader(visible_loader, visible_configs=list(visible_loader.configs or []), view_range=normalized_range)
    deep_loader = _clone_loader(visible_loader, visible_configs=list(visible_loader.configs or []), view_range=normalized_range)
    command_center = _build_command_center_payload(command_center_loader)
    selected_config_index = _resolve_selected_config_index(
        detail_loader,
        requested_index=options.get("selected_config_index"),
        command_center=command_center,
    )
    detail = _with_preserved_pareto_flags(
        detail_loader,
        lambda: _serialize_config_detail(
            detail_loader,
            selected_config_index,
            perf_weight=_normalize_weight(options.get("playground_perf_weight", 80), 80),
            risk_weight=_normalize_weight(options.get("playground_risk_weight", 60), 60),
            robust_weight=_normalize_weight(options.get("playground_robust_weight", 70), 70),
        ),
    ) if selected_config_index is not None else None
    playground = _with_preserved_pareto_flags(
        playground_loader,
        lambda: _build_playground_payload(
            playground_loader,
            all_results_loaded=all_results_loaded,
            perf_weight=_normalize_weight(options.get("playground_perf_weight", 80), 80),
            risk_weight=_normalize_weight(options.get("playground_risk_weight", 60), 60),
            robust_weight=_normalize_weight(options.get("playground_robust_weight", 70), 70),
            show_all=bool(options.get("playground_show_all")),
            use_weighted=bool(options.get("playground_use_weighted", True)),
            preview_use_weighted=bool(options.get("preview_use_weighted", True)),
            preview_show_all=bool(options.get("preview_show_all", False)),
            selected_config_index=selected_config_index,
            use_btc=bool(options.get("playground_use_btc", False)),
            viz_type=str(options.get("playground_viz_type") or "2D Scatter").strip() or "2D Scatter",
            quick_view=str(options.get("playground_quick_view") or "Profit vs Risk").strip() or "Profit vs Risk",
            color_metric_input=str(options.get("playground_color_metric") or "").strip(),
            custom_x_metric=str(options.get("playground_custom_x_metric") or "").strip(),
            custom_y_metric=str(options.get("playground_custom_y_metric") or "").strip(),
            custom_z_metric=str(options.get("playground_custom_z_metric") or "").strip(),
            view_range=normalized_range,
        ),
    )
    deep_intelligence = _with_preserved_pareto_flags(
        deep_loader,
        lambda: _build_deep_intelligence_payload(
            deep_loader,
            deep_tab=str(options.get("deep_tab") or "parameters"),
            options=options,
            all_results_loaded=all_results_loaded,
        ),
    )
    return {
        "selected_config_index": selected_config_index,
        "view_range": normalized_range,
        "command_center": command_center,
        "detail": detail,
        "playground": playground,
        "deep_intelligence": deep_intelligence,
    }


@router.get("/main_page", response_class=HTMLResponse)
def main_page(
    request: Request,
    result_path: str = Query(default="", description="Optimize result directory to open"),
    optimize_version: str = Query(default="v7", description="Explorer generation when no result is selected"),
    _session: SessionToken = Depends(require_auth),
) -> HTMLResponse:
    html_path = Path(__file__).resolve().parent.parent / "frontend" / "v7_pareto_explorer.html"
    if not html_path.exists():
        raise HTTPException(404, "v7_pareto_explorer.html not found")

    html = html_path.read_text(encoding="utf-8")

    scheme = request.url.scheme
    host = request.url.hostname or "127.0.0.1"
    port = request.url.port
    origin = f"{scheme}://{host}" + (f":{port}" if port else "")
    api_base = origin + "/api/pareto-explorer"
    ws_base = origin.replace("http://", "ws://").replace("https://", "wss://")

    html = html.replace('"%%API_BASE%%"', json.dumps(api_base))
    html = html.replace('"%%WS_BASE%%"', json.dumps(ws_base))
    html = html.replace('"%%RESULT_PATH%%"', json.dumps(str(result_path or "")))
    result_dir = _resolve_result_dir(result_path)
    optimize_version = _result_optimize_version(result_dir) if result_dir else (
        "v8" if str(optimize_version).strip().lower() == "v8" else "v7"
    )
    html = html.replace('"%%OPTIMIZE_VERSION%%"', json.dumps(optimize_version))
    html = html.replace('"%%VERSION%%"', json.dumps(PBGUI_VERSION))
    html = html.replace("%%VERSION%%", PBGUI_VERSION)
    html = html.replace('"%%SERIAL%%"', json.dumps(PBGUI_SERIAL))
    html = html.replace("%%SERIAL%%", PBGUI_SERIAL)

    nav_js = Path(__file__).resolve().parent.parent / "frontend" / "pbgui_nav.js"
    nav_hash = str(int(nav_js.stat().st_mtime)) if nav_js.exists() else PBGUI_VERSION
    html = html.replace("%%NAV_HASH%%", nav_hash)

    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})


@router.get("/session")
def get_session(
    result_path: str = Query(default="", description="Optimize result directory to open"),
    optimize_version: str = Query(default="v7", description="Explorer generation when no result is selected"),
    session: SessionToken = Depends(require_auth),
):
    result_dir = _resolve_result_dir(result_path)
    result_meta = _result_meta(result_dir) if result_dir else None
    optimize_version = str((result_meta or {}).get("optimize_version") or (
        "v8" if str(optimize_version).strip().lower() == "v8" else "v7"
    ))
    load_strategy, max_configs = _load_pareto_defaults()

    load_payload = None
    messages = [] if result_meta else [
        {
            "level": "warning",
            "text": "Open Pareto Explorer from an Optimize result or enter a valid optimize result path to load analysis data.",
        }
    ]
    if result_meta:
        try:
            load_payload = _serialize_load_result(
                result_meta["path"],
                all_results_loaded=False,
                load_strategy=load_strategy,
                max_configs=max_configs,
                view_range=None,
            )
            messages = list(load_payload.get("messages") or [])
        except HTTPException as exc:
            messages = [{"level": "error", "text": str(exc.detail)}]

    return {
        "ok": True,
        "page": {
            "title": "Pareto Explorer",
            "subtitle": f"PB{optimize_version} Pareto Explorer",
            "stages": [
                {"key": "command_center", "label": "Command Center"},
                {"key": "pareto_playground", "label": "Pareto Playground"},
                {"key": "deep_intelligence", "label": "Deep Intelligence"},
            ],
            "deep_tabs": [
                {"key": "parameters", "label": "Parameters"},
                {"key": "scenarios", "label": "Scenarios"},
                {"key": "evolution", "label": "Evolution"},
                {"key": "correlations", "label": "Correlations"},
            ],
        },
        "result": result_meta,
        "result_path": str(result_path or ""),
        "optimize_version": optimize_version,
        "result_valid": bool(result_meta),
        "load": load_payload,
        "actions": {
            "can_reload": bool(result_meta),
            "can_export": bool(load_payload),
            "can_run_backtest": False,
            "can_seed_selected": False,
            "can_seed_whole_result": bool(result_meta),
            "can_open_pareto_dash": bool(result_meta),
            "can_open_3d_plot": bool(result_meta),
        },
        "defaults": {
            "stage": "command_center",
            "deep_tab": "parameters",
            "all_results_loaded": False,
            "load_strategy": load_strategy,
            "max_configs": max_configs,
            "show_timings": False,
        },
        "messages": messages,
    }


@router.get("/validate-result")
def validate_result(
    result_path: str = Query(default="", description="Optimize result directory to validate"),
    session: SessionToken = Depends(require_auth),
):
    result_dir = _resolve_result_dir(result_path)
    if result_dir is None:
        return {"ok": False, "valid": False, "result": None}
    return {"ok": True, "valid": True, "result": _result_meta(result_dir)}


@router.post("/command-center")
def get_command_center(body: dict, session: SessionToken = Depends(require_auth)):
    result_path = str((body or {}).get("result_path") or "").strip()
    if not result_path:
        raise HTTPException(status_code=400, detail="Missing result_path")

    load_strategy = _normalize_load_strategy((body or {}).get("load_strategy"))
    max_configs = _normalize_max_configs((body or {}).get("max_configs"))
    all_results_loaded = bool((body or {}).get("all_results_loaded"))
    loader, loading_response = _load_loader_or_background_response(
        result_path,
        all_results_loaded=all_results_loaded,
        load_strategy=load_strategy,
        max_configs=max_configs,
        body=body,
    )
    if loading_response is not None:
        return loading_response
    visible_loader, normalized_range = _visible_loader(loader, all_results_loaded=all_results_loaded, view_range=(body or {}).get("view_range"))
    payload = _build_command_center_payload(visible_loader)
    payload["view_range"] = normalized_range
    return payload


@router.post("/config-detail")
def get_config_detail(body: dict, session: SessionToken = Depends(require_auth)):
    result_path = str((body or {}).get("result_path") or "").strip()
    if not result_path:
        raise HTTPException(status_code=400, detail="Missing result_path")
    try:
        config_index = int((body or {}).get("config_index"))
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Missing or invalid config_index") from exc

    load_strategy = _normalize_load_strategy((body or {}).get("load_strategy"))
    max_configs = _normalize_max_configs((body or {}).get("max_configs"))
    all_results_loaded = bool((body or {}).get("all_results_loaded"))
    perf_weight = _normalize_weight((body or {}).get("perf_weight", 80), 80)
    risk_weight = _normalize_weight((body or {}).get("risk_weight", 60), 60)
    robust_weight = _normalize_weight((body or {}).get("robust_weight", 70), 70)
    loader, loading_response = _load_loader_or_background_response(
        result_path,
        all_results_loaded=all_results_loaded,
        load_strategy=load_strategy,
        max_configs=max_configs,
        body=body,
    )
    if loading_response is not None:
        return loading_response
    visible_loader, normalized_range = _visible_loader(loader, all_results_loaded=all_results_loaded, view_range=(body or {}).get("view_range"))
    score_configs = visible_loader.get_pareto_configs() or list(visible_loader.configs or [])
    detail = _serialize_config_detail(
        loader,
        config_index,
        perf_weight=perf_weight,
        risk_weight=risk_weight,
        robust_weight=robust_weight,
        score_configs_override=score_configs,
    )
    return {
        "ok": True,
        "view_range": normalized_range,
        "detail": detail,
    }


@router.post("/optimize-preset/build")
def build_optimize_preset_preview(body: dict, session: SessionToken = Depends(require_auth)):
    result_path = str((body or {}).get("result_path") or "").strip()
    if not result_path:
        raise HTTPException(status_code=400, detail="Missing result_path")
    try:
        config_index = int((body or {}).get("config_index"))
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Missing or invalid config_index") from exc

    load_strategy = _normalize_load_strategy((body or {}).get("load_strategy"))
    max_configs = _normalize_max_configs((body or {}).get("max_configs"))
    all_results_loaded = bool((body or {}).get("all_results_loaded"))
    loader, loading_response = _load_loader_or_background_response(
        result_path,
        all_results_loaded=all_results_loaded,
        load_strategy=load_strategy,
        max_configs=max_configs,
        body=body,
    )
    if loading_response is not None:
        return loading_response
    config = loader.get_config_by_index(config_index)
    if config is None:
        raise HTTPException(status_code=404, detail="Config not found")
    loader.ensure_bot_params(config)
    loader.ensure_details(config)
    full_config = loader.get_full_config(config.config_index)
    if not isinstance(full_config, dict):
        raise HTTPException(status_code=422, detail="Full config data not available")

    params = dict((body or {}).get("preset") or {})
    direction = str(params.get("direction") or OPTIMIZE_PRESET_DIRECTIONS[0]).strip()
    if direction not in OPTIMIZE_PRESET_DIRECTIONS:
        direction = OPTIMIZE_PRESET_DIRECTIONS[0]
    params["direction"] = direction
    default_name = f"pareto_refine_cfg_{config.config_index}"
    safe_name = _sanitize_preset_name(params.get("preset_name"), default=default_name)
    near_bounds = None
    if bool(params.get("show_near_bounds", False)) or bool(params.get("only_adjust_near_bounds", False)):
        try:
            tolerance = float(params.get("near_bounds_tol", 0.10) or 0.10)
        except Exception:
            tolerance = 0.10
        tolerance = max(0.01, min(0.25, tolerance))
        params["near_bounds_tol"] = tolerance
        near_bounds = loader.get_parameters_at_bounds(tolerance=tolerance, top_n=10)

    payload = build_optimize_preset(
        config_context={
            "bounds": dict(getattr(config, "bounds", {}) or {}),
            "bot_params": dict(getattr(config, "bot_params", {}) or {}),
            "suite_metrics": dict(getattr(config, "suite_metrics", {}) or {}),
            "optimize_settings": dict(getattr(config, "optimize_settings", {}) or {}),
            "scoring_goals": dict(getattr(loader, "scoring_goals", {}) or {}),
            "optimize_version": str(getattr(loader, "optimize_version", "v7") or "v7"),
            "config_index": config.config_index,
        },
        full_config_data=full_config,
        params=params,
        near_bounds=near_bounds,
    )
    safe_payload = _json_safe(payload)
    if not bool((body or {}).get("include_config", True)) and isinstance(safe_payload, dict):
        safe_payload.pop("preset_config", None)
    return {
        "ok": True,
        "config_index": config.config_index,
        "preset_name": safe_name,
        "directions": OPTIMIZE_PRESET_DIRECTIONS,
        **safe_payload,
    }


@router.post("/playground")
def get_playground(body: dict, session: SessionToken = Depends(require_auth)):
    result_path = str((body or {}).get("result_path") or "").strip()
    if not result_path:
        raise HTTPException(status_code=400, detail="Missing result_path")

    load_strategy = _normalize_load_strategy((body or {}).get("load_strategy"))
    max_configs = _normalize_max_configs((body or {}).get("max_configs"))
    all_results_loaded = bool((body or {}).get("all_results_loaded"))
    perf_weight = _normalize_weight((body or {}).get("perf_weight", 80), 80)
    risk_weight = _normalize_weight((body or {}).get("risk_weight", 60), 60)
    robust_weight = _normalize_weight((body or {}).get("robust_weight", 70), 70)
    show_all = bool((body or {}).get("show_all"))
    use_weighted = bool((body or {}).get("use_weighted", True))
    preview_use_weighted = bool((body or {}).get("preview_use_weighted", True))
    preview_show_all = bool((body or {}).get("preview_show_all", False))
    selected_config_index_raw = (body or {}).get("selected_config_index")
    use_btc = bool((body or {}).get("use_btc", False))
    viz_type = str((body or {}).get("viz_type") or "2D Scatter").strip() or "2D Scatter"
    quick_view = str((body or {}).get("quick_view") or "Profit vs Risk").strip() or "Profit vs Risk"
    color_metric_input = str((body or {}).get("color_metric") or "").strip()
    custom_x_metric = str((body or {}).get("custom_x_metric") or "").strip()
    custom_y_metric = str((body or {}).get("custom_y_metric") or "").strip()
    custom_z_metric = str((body or {}).get("custom_z_metric") or "").strip()
    try:
        selected_config_index = int(selected_config_index_raw) if selected_config_index_raw is not None else None
    except Exception:
        selected_config_index = None

    loader, loading_response = _load_loader_or_background_response(
        result_path,
        all_results_loaded=all_results_loaded,
        load_strategy=load_strategy,
        max_configs=max_configs,
        body=body,
    )
    if loading_response is not None:
        return loading_response
    return _build_playground_payload(
        loader,
        all_results_loaded=all_results_loaded,
        perf_weight=perf_weight,
        risk_weight=risk_weight,
        robust_weight=robust_weight,
        show_all=show_all,
        use_weighted=use_weighted,
        preview_use_weighted=preview_use_weighted,
        preview_show_all=preview_show_all,
        selected_config_index=selected_config_index,
        use_btc=use_btc,
        viz_type=viz_type,
        quick_view=quick_view,
        color_metric_input=color_metric_input,
        custom_x_metric=custom_x_metric,
        custom_y_metric=custom_y_metric,
        custom_z_metric=custom_z_metric,
        view_range=(body or {}).get("view_range"),
    )


@router.post("/deep-intelligence/parameters")
def get_deep_intelligence_parameters(body: dict, session: SessionToken = Depends(require_auth)):
    result_path = str((body or {}).get("result_path") or "").strip()
    if not result_path:
        raise HTTPException(status_code=400, detail="Missing result_path")

    load_strategy = _normalize_load_strategy((body or {}).get("load_strategy"))
    max_configs = _normalize_max_configs((body or {}).get("max_configs"))
    all_results_loaded = bool((body or {}).get("all_results_loaded"))
    try:
        top_n = int((body or {}).get("top_n") or 20)
    except Exception:
        top_n = 20
    top_n = max(1, min(40, top_n))
    loader, loading_response = _load_loader_or_background_response(
        result_path,
        all_results_loaded=all_results_loaded,
        load_strategy=load_strategy,
        max_configs=max_configs,
        body=body,
    )
    if loading_response is not None:
        return loading_response
    visible_loader, normalized_range = _visible_loader(
        loader,
        all_results_loaded=all_results_loaded,
        view_range=(body or {}).get("view_range"),
    )
    payload = _build_deep_parameters_payload(visible_loader, top_n=top_n)
    payload["view_range"] = normalized_range
    return payload


@router.post("/deep-intelligence/scenarios")
def get_deep_intelligence_scenarios(body: dict, session: SessionToken = Depends(require_auth)):
    result_path = str((body or {}).get("result_path") or "").strip()
    if not result_path:
        raise HTTPException(status_code=400, detail="Missing result_path")

    load_strategy = _normalize_load_strategy((body or {}).get("load_strategy"))
    max_configs = _normalize_max_configs((body or {}).get("max_configs"))
    all_results_loaded = bool((body or {}).get("all_results_loaded"))
    loader, loading_response = _load_loader_or_background_response(
        result_path,
        all_results_loaded=all_results_loaded,
        load_strategy=load_strategy,
        max_configs=max_configs,
        body=body,
    )
    if loading_response is not None:
        return loading_response
    visible_loader, normalized_range = _visible_loader(
        loader,
        all_results_loaded=all_results_loaded,
        view_range=(body or {}).get("view_range"),
    )
    payload = _build_deep_scenarios_payload(visible_loader, metric=str((body or {}).get("metric") or ""))
    payload["view_range"] = normalized_range
    return payload


@router.post("/deep-intelligence/evolution")
def get_deep_intelligence_evolution(body: dict, session: SessionToken = Depends(require_auth)):
    result_path = str((body or {}).get("result_path") or "").strip()
    if not result_path:
        raise HTTPException(status_code=400, detail="Missing result_path")

    load_strategy = _normalize_load_strategy((body or {}).get("load_strategy"))
    max_configs = _normalize_max_configs((body or {}).get("max_configs"))
    all_results_loaded = bool((body or {}).get("all_results_loaded"))
    use_weighted = bool((body or {}).get("use_weighted", True))
    use_btc = bool((body or {}).get("use_btc", False))
    show_all = bool((body or {}).get("show_all", False))
    hide_outliers = bool((body or {}).get("hide_outliers", True))
    window_percent = max(1, min(25, int((body or {}).get("window_percent", 5) or 5)))

    loader, loading_response = _load_loader_or_background_response(
        result_path,
        all_results_loaded=all_results_loaded,
        load_strategy=load_strategy,
        max_configs=max_configs,
        body=body,
    )
    if loading_response is not None:
        return loading_response
    visible_loader, _normalized_range = _visible_loader(
        loader,
        all_results_loaded=all_results_loaded,
        view_range=(body or {}).get("view_range"),
    )
    return _build_deep_evolution_payload(
        visible_loader,
        all_results_loaded=all_results_loaded,
        metric=str((body or {}).get("metric") or ""),
        use_weighted=use_weighted,
        use_btc=use_btc,
        window_percent=window_percent,
        improvement_threshold_pct=max(0.0, min(25.0, float((body or {}).get("improvement_threshold_pct", 1) or 0.0))),
        show_all=show_all,
        hide_outliers=hide_outliers,
    )


@router.post("/deep-intelligence/correlations")
def get_deep_intelligence_correlations(body: dict, session: SessionToken = Depends(require_auth)):
    result_path = str((body or {}).get("result_path") or "").strip()
    if not result_path:
        raise HTTPException(status_code=400, detail="Missing result_path")

    load_strategy = _normalize_load_strategy((body or {}).get("load_strategy"))
    max_configs = _normalize_max_configs((body or {}).get("max_configs"))
    all_results_loaded = bool((body or {}).get("all_results_loaded"))
    strategy = str((body or {}).get("strategy") or "Top Performers").strip() or "Top Performers"
    if strategy not in {"Top Performers", "Diverse Styles", "Risk Spectrum"}:
        strategy = "Top Performers"
    num_configs = max(3, min(10, int((body or {}).get("num_configs", 5) or 5)))
    use_weighted = bool((body or {}).get("use_weighted", True))
    use_btc = bool((body or {}).get("use_btc", False))

    loader, loading_response = _load_loader_or_background_response(
        result_path,
        all_results_loaded=all_results_loaded,
        load_strategy=load_strategy,
        max_configs=max_configs,
        body=body,
    )
    if loading_response is not None:
        return loading_response
    visible_loader, normalized_range = _visible_loader(
        loader,
        all_results_loaded=all_results_loaded,
        view_range=(body or {}).get("view_range"),
    )
    payload = _build_deep_correlations_payload(
        visible_loader,
        strategy=strategy,
        num_configs=num_configs,
        use_weighted=use_weighted,
        use_btc=use_btc,
    )
    payload["view_range"] = normalized_range
    return payload


@router.post("/load")
def load_result_data(body: dict, session: SessionToken = Depends(require_auth)):
    result_path = str((body or {}).get("result_path") or "").strip()
    if not result_path:
        raise HTTPException(status_code=400, detail="Missing result_path")

    load_strategy = _normalize_load_strategy((body or {}).get("load_strategy"))
    max_configs = _normalize_max_configs((body or {}).get("max_configs"))
    all_results_loaded = bool((body or {}).get("all_results_loaded"))
    persist_defaults = bool((body or {}).get("persist_defaults"))
    view_range = (body or {}).get("view_range")

    if persist_defaults:
        save_ini_section("pareto", {
            "load_strategy": ",".join(load_strategy),
            "max_configs": str(max_configs),
        })

    cached_result_dir = None
    cached_loader = None
    refresh_options = _refresh_options_from_body(body, view_range=view_range)
    if all_results_loaded:
        cached_result_dir, cached_loader = _get_cached_loader_for_options(
            result_path,
            all_results_loaded=True,
            load_strategy=load_strategy,
            max_configs=max_configs,
        )
        if cached_loader is not None:
            payload = _serialize_load_result_from_loader(
                cached_result_dir,
                cached_loader,
                all_results_loaded=True,
                load_strategy=load_strategy,
                max_configs=max_configs,
                view_range=view_range,
            )
            payload["refresh_bundle"] = _build_server_refresh_bundle(
                cached_loader,
                all_results_loaded=True,
                options=refresh_options,
            )
            payload["ok"] = True
            payload["status"] = "complete"
            payload["cache_hit"] = True
            return payload

    if all_results_loaded:
        job = _start_full_load_job(
            result_path,
            load_strategy=load_strategy,
            max_configs=max_configs,
            refresh_options=refresh_options,
        )
        return {
            "ok": True,
            "status": "loading",
            "job": job,
        }

    payload = _serialize_load_result(
        result_path,
        all_results_loaded=all_results_loaded,
        load_strategy=load_strategy,
        max_configs=max_configs,
        view_range=view_range,
    )
    payload["refresh_bundle"] = _build_server_refresh_bundle(
        _load_loader(
            result_path,
            all_results_loaded=all_results_loaded,
            load_strategy=load_strategy,
            max_configs=max_configs,
        ),
        all_results_loaded=all_results_loaded,
        options=refresh_options,
    )
    payload["ok"] = True
    payload["status"] = "complete"
    return payload


@router.get("/load-status")
def get_load_status(job_id: str = Query(default="", description="Background full-load job id"), session: SessionToken = Depends(require_auth)):
    job = _get_load_job(str(job_id or "").strip())
    if job is None:
        raise HTTPException(status_code=404, detail="Load job not found")
    payload = job.get("payload")
    status = str(job.get("status") or "loading")
    if status == "complete" and payload is None:
        status = "loading"
    return {
        "ok": True,
        "status": status,
        "job": _serialize_load_job(job),
        "payload": payload if status == "complete" else None,
    }
