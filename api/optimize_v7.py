"""
api/optimize_v7.py — FastAPI backend for V7 Optimize management.

Provides:
- REST endpoints for optimize configs, queue, results, and paretos
- WebSocket endpoint for real-time queue updates
- Background worker that processes queued optimize jobs
"""

import asyncio
import copy
import configparser
import datetime
import json
import multiprocessing
import numpy as np
import os
import platform
import plotly.graph_objects as go
import re
import socket
import subprocess
import threading
import time
import traceback
import uuid
from pathlib import Path, PurePath
from shutil import copy2, rmtree
from typing import Optional

import httpx
import psutil
from fastapi import APIRouter, Depends, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, Response

from api.auth import SessionToken, require_auth, validate_token
from api.pb7_bridge import (
    get_bot_param_keys,
    get_hsl_signal_modes,
    get_optimize_backend_options,
    get_optimize_limits_meta_payload,
    get_pymoo_algorithm_options,
    get_pymoo_ref_dir_method_options,
    get_template_config,
)
from logging_helpers import human_log as _log
from pb7_config import load_pb7_config, prepare_pb7_config_dict, save_pb7_config
from pbgui_purefunc import PBGDIR, pb7_suite_preflight_errors, pb7dir, pb7venv, save_ini

SERVICE = "OptimizeV7API"

router = APIRouter()

_CONFIG_SECTIONS = ("backtest", "bot", "live", "optimize", "pbgui", "coin_overrides")
_RESULT_SUMMARY_FIELDS = (
    ("adg", ("adg_w_usd", "adg_usd", "adg_weighted", "adg")),
    ("gain", ("gain_usd", "gain")),
    ("drawdown_worst", ("drawdown_worst_usd", "drawdown_worst")),
    ("sharpe_ratio", ("sharpe_ratio_usd", "sharpe_ratio")),
    ("loss_profit_ratio", ("loss_profit_ratio",)),
    ("sortino_ratio", ("sortino_ratio_usd", "sortino_ratio")),
    ("omega_ratio", ("omega_ratio_usd", "omega_ratio")),
    (
        "equity_balance_diff_neg_max",
        ("equity_balance_diff_neg_max_usd", "equity_balance_diff_neg_max"),
    ),
)
_PARETO_STATISTICS = ("mean", "min", "max", "std")
_LEGACY_DEAP_HINT_KEYS = (
    "crossover_probability",
    "mutation_probability",
    "mutation_indpb",
    "offspring_multiplier",
)
_OPT_LOG_LINE_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)\s+(?P<level>[A-Z]+)\s+(?P<msg>.*)$"
)
_OPT_LOG_BACKEND_RE = re.compile(r"Selected optimizer backend:\s*(?P<backend>[a-z0-9_]+)", re.IGNORECASE)
_OPT_LOG_STARTING_CONFIGS_RE = re.compile(
    r"Loaded\s+(?P<loaded>\d+)\s+starting configs.*population size=(?P<population>\d+)",
    re.IGNORECASE,
)
_OPT_LOG_STARTING_PROGRESS_RE = re.compile(
    r"Evaluated\s+(?P<done>\d+)\s*/\s*(?P<total>\d+)\s+starting configs",
    re.IGNORECASE,
)
_OPT_LOG_STARTING_DONE_RE = re.compile(r"Evaluated\s+(?P<done>\d+)\s+starting configs", re.IGNORECASE)
_OPT_LOG_PYMOO_RE = re.compile(
    r"Using pymoo\s+(?P<algorithm>[a-z0-9_]+)\s*\|\s*n_obj=(?P<n_obj>\d+)\s*\|\s*ref_dirs=(?P<ref_dirs>\d+)\s*\|\s*n_partitions=(?P<n_partitions>\d+)\s*\((?P<mode>[^)]+)\)",
    re.IGNORECASE,
)
_OPT_LOG_ITER_RE = re.compile(
    r"Iter:\s*(?P<iter>\d+)\s*\|\s*Pareto\s+[↑-]\s*\|\s*\+(?P<added>\d+)\s*/\s*-(?P<removed>\d+)\s*\|\s*size:(?P<size>\d+)\s*\|\s*(?P<ranges>.*)$",
    re.IGNORECASE,
)
_OPT_LOG_PARETO_UPDATE_RE = re.compile(
    r"Pareto update\s*\|\s*eval=(?P<eval>\d+)\s*\|\s*front=(?P<front>\d+)\s*\|\s*objectives=\[(?P<objectives>[^\]]*)\]\s*\|\s*constraint=(?P<constraint>.+)$",
    re.IGNORECASE,
)
_OPT_LOG_RANGE_RE = re.compile(r"(?P<name>[a-zA-Z0-9_]+):\((?P<min>[^,]+),(?P<max>[^)]+)\)")
_OPT_LOG_OBJECTIVE_RE = re.compile(r"(?P<name>[a-zA-Z0-9_]+)=(?P<value>[^,\]]+)")
_PARETO_DASH_PROXY_REQ_DROP = {"host", "content-length", "connection"}
_PARETO_DASH_PROXY_RESP_DROP = {"content-length", "connection", "transfer-encoding", "content-encoding"}
_pareto_dash_sessions: dict[str, dict] = {}
_pareto_dash_lock = threading.RLock()


def _validate_name(name: str) -> None:
    if not name or any(ch in name for ch in ("/", "\\", "\x00")) or name in (".", ".."):
        raise HTTPException(400, "Invalid name")


def _infer_optimize_backend_hint(raw_cfg: dict | None) -> str | None:
    if not isinstance(raw_cfg, dict):
        return None
    optimize = raw_cfg.get("optimize")
    if not isinstance(optimize, dict):
        return None
    backend = str(optimize.get("backend") or "").strip().lower()
    if backend in {"deap", "pymoo"}:
        return None
    if any(key in optimize for key in _LEGACY_DEAP_HINT_KEYS):
        return "deap"
    return None


def _restore_optimize_editor_backend_semantics(cfg: dict, raw_cfg: dict | None) -> dict:
    """Keep legacy optimize payloads aligned with the raw file semantics."""
    if not isinstance(cfg, dict) or not isinstance(raw_cfg, dict):
        return cfg

    raw_optimize = raw_cfg.get("optimize")
    if not isinstance(raw_optimize, dict):
        return cfg

    raw_backend = str(raw_optimize.get("backend") or "").strip().lower()
    if raw_backend in {"deap", "pymoo"}:
        return cfg

    optimize = cfg.get("optimize")
    if isinstance(optimize, dict):
        optimize.pop("backend", None)
    return cfg


def _editor_config_payload(cfg: dict, *, name: str | None = None, backend_hint: str | None = None) -> dict:
    param_status = cfg.pop("_pbgui_param_status", {}) if isinstance(cfg, dict) else {}
    payload = {"config": cfg, "param_status": param_status}
    if name is not None:
        payload["name"] = name
    if backend_hint:
        payload["backend_hint"] = backend_hint
    return payload


def _merge_nested_dicts(base: dict, overlay: dict) -> dict:
    merged = copy.deepcopy(base)
    for key, value in overlay.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge_nested_dicts(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def _get_new_optimize_template() -> dict:
    try:
        tmpl = get_template_config()
    except Exception as exc:
        _log(SERVICE, f"Failed to load optimize template config: {exc}", level="WARNING")
        tmpl = {"backtest": {}, "bot": {}, "live": {}, "optimize": {}}
    backtest = tmpl.setdefault("backtest", {})
    optimize = tmpl.setdefault("optimize", {})
    backtest.setdefault(
        "start_date",
        (datetime.date.today() - datetime.timedelta(days=365 * 4)).strftime("%Y-%m-%d"),
    )
    backtest.setdefault("end_date", datetime.date.today().strftime("%Y-%m-%d"))
    optimize.setdefault("n_cpus", multiprocessing.cpu_count())
    return tmpl


def _load_editor_payload_from_config_path(cfg_file: Path, *, name: str | None = None) -> dict:
    if not cfg_file.exists():
        raise HTTPException(404, f"Config not found: {cfg_file}")
    try:
        raw_cfg = None
        try:
            with open(cfg_file, "r", encoding="utf-8") as f:
                raw_cfg = json.load(f)
        except (OSError, json.JSONDecodeError):
            raw_cfg = None
        try:
            cfg = load_pb7_config(cfg_file, neutralize_added=True)
        except Exception as exc:
            if not isinstance(raw_cfg, dict):
                raise
            merged_cfg = _merge_nested_dicts(_get_new_optimize_template(), raw_cfg)
            cfg = prepare_pb7_config_dict(
                merged_cfg,
                neutralize_added=True,
                base_config_path=str(cfg_file),
            )
            _log(
                SERVICE,
                f"Loaded optimize config '{cfg_file}' via template merge fallback after {exc}",
                level="INFO",
            )
        cfg = _restore_optimize_editor_backend_semantics(cfg, raw_cfg)
        payload_name = name if name is not None else cfg_file.stem
        return _editor_config_payload(cfg, name=payload_name, backend_hint=_infer_optimize_backend_hint(raw_cfg))
    except HTTPException:
        raise
    except Exception as exc:
        detail = str(exc).strip() or exc.__class__.__name__
        _log(
            SERVICE,
            f"Failed to load optimize config '{cfg_file}': {detail}",
            level="WARNING",
            meta={"traceback": traceback.format_exc()},
        )
        raise HTTPException(status_code=422, detail=detail) from exc


def _queue_config_snapshot(data: dict | None) -> dict | None:
    snapshot = (data or {}).get("config_snapshot")
    return copy.deepcopy(snapshot) if isinstance(snapshot, dict) else None


def _find_queue_config_candidates(cfg_file: Path) -> list[Path]:
    stem = cfg_file.stem.strip()
    if not stem:
        return []
    configs_dir = _opt_configs_dir()
    candidates = []
    try:
        for candidate in sorted(configs_dir.glob(f"{stem}*.json")):
            if candidate == cfg_file or not candidate.is_file():
                continue
            candidates.append(candidate)
    except Exception:
        return []
    return candidates


def _missing_queue_config_detail(
    cfg_file: Path,
    candidates: list[Path] | None = None,
    *,
    filename: str | None = None,
) -> dict:
    matches = list(candidates or [])
    detail = {
        "code": "queue_config_missing",
        "message": "",
        "config_path": str(cfg_file),
        "queue_filename": str(filename or ""),
        "candidates": [{"name": match.stem, "path": str(match)} for match in matches],
    }
    if len(matches) == 1:
        detail["message"] = (
            f"Config not found: {cfg_file}. PBGui found one matching config '{matches[0].stem}' and will use it automatically. "
            f"Save or requeue the item once if you want to persist the new path."
        )
        return detail
    if matches:
        labels = ", ".join(match.stem for match in matches)
        detail["code"] = "queue_config_ambiguous"
        detail["message"] = (
            f"Config not found: {cfg_file}. This queue item predates embedded queue snapshots. "
            f"Matching configs found: {labels}. Select the correct config to repair this queued item or open it directly."
        )
        return detail
    detail["message"] = (
        f"Config not found: {cfg_file}. This queue item predates embedded queue snapshots and cannot be reopened automatically. "
        f"Open an existing config and queue it again."
    )
    return detail


def _load_editor_payload_from_queue_data(data: dict, *, filename: str | None = None) -> dict:
    cfg_file = Path(str(data.get("json") or ""))
    if cfg_file.exists():
        return _load_editor_payload_from_config_path(cfg_file)

    snapshot = _queue_config_snapshot(data)
    if snapshot is not None:
        cfg = _restore_optimize_editor_backend_semantics(copy.deepcopy(snapshot), snapshot)
        payload_name = cfg_file.stem or str(data.get("name") or filename or "")
        return _editor_config_payload(cfg, name=payload_name, backend_hint=_infer_optimize_backend_hint(snapshot))

    candidates = _find_queue_config_candidates(cfg_file)
    if len(candidates) == 1:
        return _load_editor_payload_from_config_path(candidates[0])

    raise HTTPException(
        status_code=409 if len(candidates) > 1 else 404,
        detail=_missing_queue_config_detail(cfg_file, candidates, filename=filename),
    )


def _update_queue_config_references(
    source_paths: list[Path],
    *,
    target_path: Path,
    target_name: str,
    config_snapshot: dict,
) -> int:
    queue_dir = _opt_queue_dir()
    queue_dir.mkdir(parents=True, exist_ok=True)
    normalized_sources = {_normalize_process_arg_path(path) for path in source_paths if path}
    if not normalized_sources:
        return 0

    changed_count = 0
    for fp in sorted(queue_dir.glob("*.json")):
        try:
            with open(fp, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            continue
        if _normalize_process_arg_path(data.get("json")) not in normalized_sources:
            continue

        data["json"] = str(target_path)
        data["name"] = target_name
        data["config_snapshot"] = copy.deepcopy(config_snapshot)
        with open(fp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
            f.write("\n")
        changed_count += 1

    return changed_count


def _read_queue_item_data(filename: str) -> dict:
    queue_file = _opt_queue_dir() / f"{filename}.json"
    if not queue_file.exists():
        raise HTTPException(404, "Queue item not found")
    try:
        with open(queue_file, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as exc:
        raise HTTPException(500, f"Error reading queue item: {exc}") from exc


def _coerce_queue_order(value) -> int | None:
    try:
        if value in (None, ""):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _queue_sort_key(data: dict, created_ts: float, fallback_name: str) -> tuple[int, float, str]:
    order = _coerce_queue_order(data.get("order") if isinstance(data, dict) else None)
    if order is not None:
        return (0, float(order), fallback_name)
    return (1, -float(created_ts or 0.0), fallback_name)


def _load_sorted_queue_entries(queue_dir: Path, *, error_prefix: str = "queue item") -> list[tuple[Path, dict, float]]:
    entries: list[tuple[Path, dict, float]] = []
    for fp in queue_dir.glob("*.json"):
        try:
            with open(fp, "r", encoding="utf-8") as f:
                data = json.load(f)
            created_ts = fp.stat().st_mtime
            entries.append((fp, data, created_ts))
        except Exception as exc:
            _log(SERVICE, f"Error loading optimize {error_prefix} {fp}: {exc}", level="WARNING")
    entries.sort(
        key=lambda entry: _queue_sort_key(
            entry[1],
            entry[2],
            str((entry[1] or {}).get("filename") or entry[0].stem),
        )
    )
    return entries


def _next_queue_insert_order(queue_dir: Path) -> int:
    min_order = None
    for _, data, _ in _load_sorted_queue_entries(queue_dir, error_prefix="queue order item"):
        order = _coerce_queue_order(data.get("order") if isinstance(data, dict) else None)
        if order is None:
            continue
        if min_order is None or order < min_order:
            min_order = order
    if min_order is not None:
        return min_order - 1
    return -int(time.time_ns())


def _queue_launch_item_from_data(filename: str, data: dict) -> dict:
    return {
        "filename": filename,
        "name": data.get("name", filename),
        "json": str(data.get("json") or ""),
        "exchange": _serialize_exchange(data.get("exchange")),
        "config_snapshot": _queue_config_snapshot(data),
    }


def _resolve_queue_launch_config(item: dict) -> tuple[Path, dict]:
    config_path = Path(str(item.get("json") or ""))
    snapshot = _queue_config_snapshot(item)

    if config_path.exists():
        return config_path, load_pb7_config(config_path)

    if snapshot is not None:
        return config_path, copy.deepcopy(snapshot)

    candidates = _find_queue_config_candidates(config_path)
    if len(candidates) == 1:
        config_path = candidates[0]
        return config_path, load_pb7_config(config_path)

    raise HTTPException(
        status_code=409 if len(candidates) > 1 else 404,
        detail=_missing_queue_config_detail(config_path, candidates, filename=str(item.get("filename") or "")),
    )


def _validate_queue_item_requeueable(filename: str, data: dict) -> None:
    item = _queue_launch_item_from_data(filename, data)
    try:
        _, config = _resolve_queue_launch_config(item)
    except HTTPException:
        raise
    except Exception as exc:
        detail = str(exc).strip() or exc.__class__.__name__
        raise HTTPException(
            status_code=422,
            detail=f"Queue item cannot be requeued until its config is valid: {detail}",
        ) from exc
    preflight_errors = pb7_suite_preflight_errors(config)
    if preflight_errors:
        detail = str(preflight_errors[0]).strip() or "Optimize config failed preflight"
        raise HTTPException(
            status_code=422,
            detail=f"Queue item cannot be requeued until its config is valid: {detail}",
        )


def _repair_queue_item_config_reference(filename: str, data: dict, target_name: str) -> dict:
    queue_file = _opt_queue_dir() / f"{filename}.json"
    cfg_file = Path(str(data.get("json") or ""))
    candidates = _find_queue_config_candidates(cfg_file)
    target_path = _opt_configs_dir() / f"{target_name}.json"

    if target_path not in candidates:
        raise HTTPException(
            status_code=409 if candidates else 404,
            detail=_missing_queue_config_detail(cfg_file, candidates, filename=filename),
        )

    candidate_load_failed = False
    try:
        load_pb7_config(target_path)
    except Exception:
        candidate_load_failed = True

    payload = _load_editor_payload_from_config_path(target_path, name=target_name)
    config_snapshot = payload.get("config") if isinstance(payload, dict) else None
    if not isinstance(config_snapshot, dict):
        raise HTTPException(status_code=500, detail="Failed to prepare queue config snapshot")

    if candidate_load_failed:
        save_pb7_config(config_snapshot, target_path)
        _log(
            SERVICE,
            f"Normalized queue repair config '{target_path}' while rebinding legacy queue item {filename}",
            level="INFO",
        )

    updated = copy.deepcopy(data)
    updated["name"] = target_name
    updated["json"] = str(target_path)
    updated["config_snapshot"] = copy.deepcopy(config_snapshot)
    updated["exchange"] = _serialize_exchange(config_snapshot.get("backtest", {}).get("exchanges"))

    queue_file.parent.mkdir(parents=True, exist_ok=True)
    with open(queue_file, "w", encoding="utf-8") as f:
        json.dump(updated, f, indent=4)
        f.write("\n")

    return updated


def _opt_queue_dir() -> Path:
    return Path(PBGDIR) / "data" / "opt_v7_queue"


def _opt_configs_dir() -> Path:
    return Path(PBGDIR) / "data" / "opt_v7"


def _opt_results_base() -> Path:
    return Path(pb7dir()) / "optimize_results"


def _opt_log_dir() -> Path:
    return Path(PBGDIR) / "data" / "logs" / "optimizes"


def _read_ini_section(section: str = "optimize_v7") -> dict:
    cfg = configparser.ConfigParser()
    cfg.read("pbgui.ini")
    if not cfg.has_section(section):
        # cpu_override controls whether autostart rewrites optimize.n_cpus before launch.
        return {"autostart": "False", "cpu": "1", "cpu_override": "True"}
    # Ensure new keys get a stable default even when the section exists.
    items = dict(cfg.items(section))
    items.setdefault("autostart", "False")
    items.setdefault("cpu", "1")
    items.setdefault("cpu_override", "True")
    return items


def _write_ini(key: str, value: str, section: str = "optimize_v7") -> None:
    save_ini(section, key, value)


def _normalize_autostart_cpu(value) -> int:
    try:
        cpu = int(value)
    except Exception:
        cpu = 1
    return max(1, min(cpu, multiprocessing.cpu_count()))


def _parse_log_number(value) -> Optional[float]:
    try:
        text = str(value).strip()
        if not text:
            return None
        return float(text)
    except Exception:
        return None


def _read_optimize_log_excerpt(log_path: Path, *, head_kb: int = 32, tail_kb: int = 512) -> str:
    if not log_path.exists():
        return ""
    try:
        with open(log_path, "rb") as handle:
            handle.seek(0, 2)
            file_size = handle.tell()
            head_bytes = max(0, head_kb * 1024)
            tail_bytes = max(0, tail_kb * 1024)
            if file_size <= head_bytes + tail_bytes + 1:
                handle.seek(0)
                return handle.read().decode("utf-8", errors="ignore")
            handle.seek(0)
            head = handle.read(head_bytes).decode("utf-8", errors="ignore")
            handle.seek(max(file_size - tail_bytes, 0))
            tail = handle.read().decode("utf-8", errors="ignore")
            return head + "\n...\n" + tail
    except Exception:
        return ""


def _parse_optimize_log_summary(log_text: str) -> dict:
    summary = {
        "phase": "queued",
        "backend": None,
        "algorithm": None,
        "objective_count": None,
        "ref_dirs": None,
        "n_partitions": None,
        "n_partitions_mode": None,
        "starting_configs_loaded": None,
        "starting_configs_done": None,
        "starting_configs_total": None,
        "population_size": None,
        "iter": None,
        "eval": None,
        "front": None,
        "pareto_added": None,
        "pareto_removed": None,
        "constraint": None,
        "objective_ranges": {},
        "objectives": {},
        "last_log_at": None,
        "last_line": "",
        "last_error": None,
    }
    for raw_line in (log_text or "").splitlines():
        line = raw_line.strip()
        if not line or line == "...":
            continue
        message = line
        match = _OPT_LOG_LINE_RE.match(line)
        if match:
            message = match.group("msg").strip()
            summary["last_log_at"] = match.group("ts")
            summary["last_line"] = message
            if match.group("level") in {"ERROR", "CRITICAL"}:
                summary["last_error"] = message
                summary["phase"] = "error"

        backend_match = _OPT_LOG_BACKEND_RE.search(message)
        if backend_match:
            summary["backend"] = backend_match.group("backend").lower()

        loaded_match = _OPT_LOG_STARTING_CONFIGS_RE.search(message)
        if loaded_match:
            summary["starting_configs_loaded"] = int(loaded_match.group("loaded"))
            summary["population_size"] = int(loaded_match.group("population"))
            summary["phase"] = "initializing"

        progress_match = _OPT_LOG_STARTING_PROGRESS_RE.search(message)
        if progress_match:
            summary["starting_configs_done"] = int(progress_match.group("done"))
            summary["starting_configs_total"] = int(progress_match.group("total"))
            summary["phase"] = "evaluating_starts"

        done_match = _OPT_LOG_STARTING_DONE_RE.search(message)
        if done_match and summary["starting_configs_done"] is None:
            summary["starting_configs_done"] = int(done_match.group("done"))

        pymoo_match = _OPT_LOG_PYMOO_RE.search(message)
        if pymoo_match:
            summary["algorithm"] = pymoo_match.group("algorithm").lower()
            summary["objective_count"] = int(pymoo_match.group("n_obj"))
            summary["ref_dirs"] = int(pymoo_match.group("ref_dirs"))
            summary["n_partitions"] = int(pymoo_match.group("n_partitions"))
            summary["n_partitions_mode"] = pymoo_match.group("mode").strip().lower()
            summary["phase"] = "optimizing"

        iter_match = _OPT_LOG_ITER_RE.search(message)
        if iter_match:
            summary["iter"] = int(iter_match.group("iter"))
            summary["pareto_added"] = int(iter_match.group("added"))
            summary["pareto_removed"] = int(iter_match.group("removed"))
            summary["front"] = int(iter_match.group("size"))
            ranges = {}
            for range_match in _OPT_LOG_RANGE_RE.finditer(iter_match.group("ranges")):
                name = range_match.group("name")
                ranges[name] = {
                    "min": _parse_log_number(range_match.group("min")),
                    "max": _parse_log_number(range_match.group("max")),
                }
            if ranges:
                summary["objective_ranges"] = ranges
            summary["phase"] = "optimizing"

        pareto_match = _OPT_LOG_PARETO_UPDATE_RE.search(message)
        if pareto_match:
            summary["eval"] = int(pareto_match.group("eval"))
            summary["front"] = int(pareto_match.group("front"))
            objectives = {}
            for objective_match in _OPT_LOG_OBJECTIVE_RE.finditer(pareto_match.group("objectives")):
                objectives[objective_match.group("name")] = _parse_log_number(objective_match.group("value"))
            if objectives:
                summary["objectives"] = objectives
            summary["constraint"] = _parse_log_number(pareto_match.group("constraint"))
            summary["phase"] = "optimizing"

        if "Finished preparing hlcvs data" in message or "Finished initializing evaluator" in message:
            summary["phase"] = "initializing"
        elif message.startswith("Starting optimize"):
            summary["phase"] = "optimizing"
        elif "Optimization complete" in message or "successfully processed optimize_results" in message:
            summary["phase"] = "complete"

    return summary


def _collect_optimize_process_stats(pid: Optional[int]) -> dict:
    stats = {
        "running": False,
        "pid": pid,
        "status": None,
        "rss_bytes": None,
        "memory_percent": None,
        "cpu_percent": None,
        "cpu_cores_est": None,
        "threads": None,
        "children": 0,
        "started_at": None,
    }
    if pid is None or not _store._is_process_running(pid):
        return stats
    try:
        parent = psutil.Process(pid)
        procs = [parent] + parent.children(recursive=True)
        rss_bytes = 0
        threads = 0
        memory_percent = 0.0
        cpu_percent = 0.0
        for proc in procs:
            try:
                with proc.oneshot():
                    rss_bytes += proc.memory_info().rss
                    threads += proc.num_threads()
                    memory_percent += proc.memory_percent()
                    # Non-blocking: returns % since last call; first call may be 0.0
                    cpu_percent += float(proc.cpu_percent(interval=None) or 0.0)
            except (psutil.NoSuchProcess, psutil.ZombieProcess, psutil.AccessDenied):
                continue
        with parent.oneshot():
            stats.update(
                {
                    "running": True,
                    "status": parent.status(),
                    "rss_bytes": rss_bytes,
                    "memory_percent": round(memory_percent, 2),
                    "cpu_percent": round(cpu_percent, 1),
                    "cpu_cores_est": round(cpu_percent / 100.0, 2),
                    "threads": threads,
                    "children": max(len(procs) - 1, 0),
                    "started_at": datetime.datetime.fromtimestamp(parent.create_time(), datetime.UTC).isoformat(),
                }
            )
    except (psutil.NoSuchProcess, psutil.ZombieProcess, psutil.AccessDenied):
        return stats
    return stats


def _normalize_process_arg_path(value: str | Path | None) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        return str(Path(raw).expanduser().resolve(strict=False))
    except Exception:
        try:
            return str(Path(raw).expanduser().absolute())
        except Exception:
            return raw


def _is_optimize_process_cmdline(cmdline: list[str] | tuple[str, ...] | None) -> bool:
    return any(str(part).lower().endswith("optimize.py") for part in (cmdline or []))


def _get_process_open_file_paths(proc: psutil.Process) -> set[str]:
    paths: set[str] = set()
    try:
        process_tree = [proc] + proc.children(recursive=True)
    except (psutil.NoSuchProcess, psutil.ZombieProcess, psutil.AccessDenied):
        process_tree = [proc]
    for current in process_tree:
        try:
            for open_file in current.open_files() or []:
                normalized = _normalize_process_arg_path(open_file.path)
                if normalized:
                    paths.add(normalized)
        except (psutil.NoSuchProcess, psutil.ZombieProcess, psutil.AccessDenied):
            continue
    return paths


def _build_optimize_process_index() -> list[dict]:
    candidates: list[psutil.Process] = []
    for proc in psutil.process_iter(["pid", "ppid", "cmdline", "create_time"]):
        try:
            cmdline = [str(part) for part in (proc.info.get("cmdline") or [])]
            if _is_optimize_process_cmdline(cmdline):
                candidates.append(proc)
        except (psutil.NoSuchProcess, psutil.ZombieProcess, psutil.AccessDenied):
            continue

    if not candidates:
        return []

    candidate_pids = {proc.pid for proc in candidates}
    roots: list[psutil.Process] = []
    for proc in candidates:
        try:
            parent_pid = proc.ppid()
        except (psutil.NoSuchProcess, psutil.ZombieProcess, psutil.AccessDenied):
            parent_pid = None
        if parent_pid not in candidate_pids:
            roots.append(proc)

    descriptors = []
    for proc in (roots or candidates):
        try:
            cmdline = [str(part) for part in (proc.info.get("cmdline") or proc.cmdline() or [])]
            descriptors.append(
                {
                    "pid": proc.pid,
                    "proc": proc,
                    "args": {_normalize_process_arg_path(part) for part in cmdline if str(part).strip()},
                    "log_paths": _get_process_open_file_paths(proc),
                    "create_time": float(proc.info.get("create_time") or 0.0),
                }
            )
        except (psutil.NoSuchProcess, psutil.ZombieProcess, psutil.AccessDenied):
            continue
    descriptors.sort(key=lambda item: (item["create_time"], item["pid"]))
    return descriptors


def _build_queue_config_counts(queue_dir: Path) -> dict[str, int]:
    counts: dict[str, int] = {}
    for fp in sorted(queue_dir.glob("*.json")):
        try:
            with open(fp, "r", encoding="utf-8") as f:
                data = json.load(f)
            normalized = _normalize_process_arg_path(data.get("json"))
            if normalized:
                counts[normalized] = counts.get(normalized, 0) + 1
        except Exception:
            continue
    return counts


def _select_optimize_process_descriptor(
    config_path: Path | str | None,
    log_path: Path | str | None,
    *,
    process_index: list[dict] | None = None,
    allow_config_fallback: bool = True,
) -> Optional[dict]:
    target_config = _normalize_process_arg_path(config_path)
    target_log = _normalize_process_arg_path(log_path)
    if not target_config and not target_log:
        return None

    descriptors = process_index if process_index is not None else _build_optimize_process_index()
    exact_matches: list[dict] = []
    config_matches: list[dict] = []
    for descriptor in descriptors:
        args = descriptor.get("args") or set()
        if target_config and target_config not in args:
            continue
        if target_log and target_log in (descriptor.get("log_paths") or set()):
            exact_matches.append(descriptor)
            continue
        config_matches.append(descriptor)

    if exact_matches:
        return exact_matches[0]
    if allow_config_fallback and len(config_matches) == 1:
        return config_matches[0]
    return None


def _find_optimize_process_roots_for_config(
    config_path: Path | str | None,
    log_path: Path | str | None = None,
    *,
    process_index: list[dict] | None = None,
    allow_config_fallback: bool = True,
) -> list[psutil.Process]:
    descriptor = _select_optimize_process_descriptor(
        config_path,
        log_path,
        process_index=process_index,
        allow_config_fallback=allow_config_fallback,
    )
    return [descriptor["proc"]] if descriptor else []


def _kill_process_tree(pid: int) -> set[int]:
    killed: set[int] = set()
    if not pid:
        return killed
    try:
        parent = psutil.Process(pid)
        children = parent.children(recursive=True)
        children.append(parent)
        for proc in children:
            try:
                proc.kill()
                killed.add(proc.pid)
            except psutil.NoSuchProcess:
                pass
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        pass
    return killed


def _collect_optimize_system_stats() -> dict:
    memory = psutil.virtual_memory()
    swap = psutil.swap_memory()
    cpu_per_core = []
    try:
        cpu_per_core = [round(float(value), 1) for value in psutil.cpu_percent(interval=0.05, percpu=True)]
    except Exception:
        cpu_per_core = []
    load_avg = None
    try:
        if hasattr(os, "getloadavg"):
            load_avg = tuple(round(value, 2) for value in os.getloadavg())
    except Exception:
        load_avg = None
    overall_cpu = round(sum(cpu_per_core) / len(cpu_per_core), 1) if cpu_per_core else round(psutil.cpu_percent(interval=0.05), 1)
    return {
        "cpu_percent": overall_cpu,
        "cpu_per_core": cpu_per_core,
        "cpu_core_count": len(cpu_per_core),
        "memory_percent": round(memory.percent, 1),
        "memory_used_bytes": int(memory.used),
        "memory_total_bytes": int(memory.total),
        "swap_used_bytes": int(swap.used),
        "swap_total_bytes": int(swap.total),
        "load_avg": load_avg,
    }


def _build_optimize_runtime_status(item: dict) -> dict:
    log_path = Path(str(item.get("log_path") or ""))
    _store._migrate_old_log(str(item.get("filename") or ""), log_path)
    log_excerpt = _read_optimize_log_excerpt(log_path)
    log_summary = _parse_optimize_log_summary(log_excerpt)

    config_meta = {
        "backend": None,
        "iters": None,
        "n_cpus": None,
        "seed_mode": item.get("seed_mode") or "none",
        "exchange": item.get("exchange"),
    }
    config_path = Path(str(item.get("json") or ""))
    if config_path.exists():
        try:
            cfg = load_pb7_config(config_path)
            optimize = cfg.get("optimize") if isinstance(cfg.get("optimize"), dict) else {}
            config_meta["backend"] = str(optimize.get("backend") or "").strip().lower() or None
            try:
                config_meta["iters"] = int(optimize.get("iters")) if optimize.get("iters") not in (None, "") else None
            except Exception:
                config_meta["iters"] = None
            try:
                config_meta["n_cpus"] = int(optimize.get("n_cpus")) if optimize.get("n_cpus") not in (None, "") else None
            except Exception:
                config_meta["n_cpus"] = None
        except Exception:
            pass

    eval_count = log_summary.get("eval") or log_summary.get("iter")
    target_iters = config_meta.get("iters")
    progress_pct = None
    if isinstance(eval_count, int) and isinstance(target_iters, int) and target_iters > 0:
        progress_pct = max(0.0, min(100.0, (eval_count / target_iters) * 100.0))

    process_stats = _collect_optimize_process_stats(item.get("pid"))
    system_stats = _collect_optimize_system_stats()

    log_stat = None
    if log_path.exists():
        try:
            stat = log_path.stat()
            log_stat = {
                "size_bytes": stat.st_size,
                "updated_at": datetime.datetime.fromtimestamp(stat.st_mtime, datetime.UTC).isoformat(),
            }
        except Exception:
            log_stat = None

    queue_items = _load_queue_sync()
    overview = {
        "queued": sum(1 for queued_item in queue_items if queued_item.get("status") == "queued"),
        "running": sum(1 for queued_item in queue_items if queued_item.get("status") in {"running", "optimizing"}),
        "error": sum(1 for queued_item in queue_items if queued_item.get("status") == "error"),
        "complete": sum(1 for queued_item in queue_items if queued_item.get("status") == "complete"),
    }

    phase = log_summary.get("phase") or item.get("status") or "queued"
    if item.get("status") == "complete":
        phase = "complete"
    elif item.get("status") == "error" and phase == "queued":
        phase = "error"
    elif item.get("status") in {"running", "optimizing"} and phase == "queued":
        phase = item.get("status")

    return {
        "filename": item.get("filename"),
        "name": item.get("name"),
        "status": item.get("status"),
        "phase": phase,
        "progress": {
            "eval": eval_count,
            "iter": log_summary.get("iter"),
            "target_iters": target_iters,
            "percent": progress_pct,
            "front": log_summary.get("front"),
            "pareto_added": log_summary.get("pareto_added"),
            "pareto_removed": log_summary.get("pareto_removed"),
            "starting_configs_loaded": log_summary.get("starting_configs_loaded"),
            "starting_configs_done": log_summary.get("starting_configs_done"),
            "starting_configs_total": log_summary.get("starting_configs_total"),
            "population_size": log_summary.get("population_size"),
        },
        "runtime": {
            "backend": log_summary.get("backend") or config_meta.get("backend"),
            "algorithm": log_summary.get("algorithm"),
            "objective_count": log_summary.get("objective_count"),
            "config_n_cpus": config_meta.get("n_cpus"),
            "ref_dirs": log_summary.get("ref_dirs"),
            "n_partitions": log_summary.get("n_partitions"),
            "n_partitions_mode": log_summary.get("n_partitions_mode"),
            "constraint": log_summary.get("constraint"),
            "last_log_at": log_summary.get("last_log_at"),
            "last_line": log_summary.get("last_line"),
            "last_error": log_summary.get("last_error"),
            "seed_mode": config_meta.get("seed_mode"),
            "exchange": config_meta.get("exchange"),
        },
        "metrics": {
            "objectives": log_summary.get("objectives") or {},
            "ranges": log_summary.get("objective_ranges") or {},
        },
        "process": process_stats,
        "system": system_stats,
        "log": log_stat,
        "queue": overview,
    }


def _load_pareto_json(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _coerce_metric_float(value) -> Optional[float]:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _extract_config_from_pareto_data(data: dict) -> dict:
    base = {key: data.get(key, {}) for key in _CONFIG_SECTIONS if key in data}
    return prepare_pb7_config_dict(base, neutralize_added=True)


def _result_name_from_data(data: dict, fallback_name: str) -> str:
    base_dir = str(data.get("backtest", {}).get("base_dir") or "").strip()
    if base_dir:
        return PurePath(base_dir).name
    return fallback_name


def _detect_pareto_mode(data: dict) -> tuple[str, list[str]]:
    suite_block = data.get("suite_metrics") if isinstance(data.get("suite_metrics"), dict) else {}
    suite_metrics = suite_block.get("metrics") if isinstance(suite_block.get("metrics"), dict) else {}
    if suite_metrics:
        raw_labels = suite_block.get("scenario_labels") if isinstance(suite_block.get("scenario_labels"), list) else []
        labels = [str(label).strip() for label in raw_labels if str(label).strip()]
        return "suite", labels
    metrics_block = data.get("metrics") if isinstance(data.get("metrics"), dict) else {}
    if metrics_block:
        return "stats", []
    if "analyses_combined" in data or "analyses" in data:
        return "legacy", []
    return "unknown", []


def _normalize_pareto_statistic(statistic: str | None) -> str:
    normalized = str(statistic or "mean").strip().lower()
    return normalized if normalized in _PARETO_STATISTICS else "mean"


def _normalize_pareto_scenario(scenario: str | None, scenario_labels: list[str]) -> str:
    normalized = str(scenario or "Aggregated").strip() or "Aggregated"
    allowed = {"Aggregated", *scenario_labels}
    return normalized if normalized in allowed else "Aggregated"


def _metric_value_from_metric_dict(metric_data, *, statistic: str, scenario: str) -> Optional[float]:
    if not isinstance(metric_data, dict):
        return _coerce_metric_float(metric_data)
    if scenario != "Aggregated":
        scenarios = metric_data.get("scenarios") if isinstance(metric_data.get("scenarios"), dict) else {}
        value = _coerce_metric_float(scenarios.get(scenario))
        if value is not None:
            return value
    stats = metric_data.get("stats") if isinstance(metric_data.get("stats"), dict) else {}
    if statistic == "mean":
        value = _coerce_metric_float(metric_data.get("aggregated"))
        if value is not None:
            return value
    value = _coerce_metric_float(stats.get(statistic))
    if value is not None:
        return value
    if statistic == "mean":
        value = _coerce_metric_float(metric_data.get("mean"))
        if value is not None:
            return value
    return _coerce_metric_float(metric_data.get(statistic))


def _summary_from_pareto_data(data: dict, *, statistic: str, scenario: str) -> dict:
    summary: dict[str, float] = {}
    suite_block = data.get("suite_metrics") if isinstance(data.get("suite_metrics"), dict) else {}
    suite_metrics = suite_block.get("metrics") if isinstance(suite_block.get("metrics"), dict) else {}
    if suite_metrics:
        for summary_key, metric_names in _RESULT_SUMMARY_FIELDS:
            for metric_name in metric_names:
                value = _metric_value_from_metric_dict(
                    suite_metrics.get(metric_name),
                    statistic=statistic,
                    scenario=scenario,
                )
                if value is not None:
                    summary[summary_key] = value
                    break
        return summary

    metrics_root = data.get("metrics") if isinstance(data.get("metrics"), dict) else {}
    metric_blocks = []
    if metrics_root:
        metric_blocks.append(metrics_root)
        nested_stats = metrics_root.get("stats") if isinstance(metrics_root.get("stats"), dict) else {}
        if nested_stats:
            metric_blocks.append(nested_stats)

    for summary_key, metric_names in _RESULT_SUMMARY_FIELDS:
        for metric_name in metric_names:
            value = _coerce_metric_float(data.get(metric_name))
            if value is not None:
                summary[summary_key] = value
                break
            for metric_block in metric_blocks:
                value = _metric_value_from_metric_dict(
                    metric_block.get(metric_name),
                    statistic=statistic,
                    scenario="Aggregated",
                )
                if value is not None:
                    summary[summary_key] = value
                    break
            if summary_key in summary:
                break
    return summary


def _pareto_meta_from_data(data: dict, *, selected_scenario: str | None, selected_statistic: str | None) -> dict:
    mode, scenario_labels = _detect_pareto_mode(data)
    normalized_statistic = _normalize_pareto_statistic(selected_statistic)
    normalized_scenario = (
        _normalize_pareto_scenario(selected_scenario, scenario_labels)
        if mode == "suite"
        else "Aggregated"
    )
    return {
        "mode": mode,
        "has_suite_metrics": mode == "suite",
        "scenario_labels": scenario_labels,
        "available_statistics": list(_PARETO_STATISTICS),
        "selected_scenario": normalized_scenario,
        "selected_statistic": normalized_statistic,
        "statistic_enabled": mode != "suite" or normalized_scenario == "Aggregated",
    }


def _normalize_plot_goal(goal: str | None) -> str:
    return "min" if str(goal or "").strip().lower() == "min" else "max"


def _extract_plot_objective_specs(data: dict) -> list[dict[str, str]]:
    optimize = data.get("optimize") if isinstance(data.get("optimize"), dict) else {}
    scoring = optimize.get("scoring")
    specs: list[dict[str, str]] = []
    if not isinstance(scoring, list):
        return specs
    for item in scoring:
        if isinstance(item, dict):
            metric = str(item.get("metric") or "").strip()
            if metric:
                specs.append({"metric": metric, "goal": _normalize_plot_goal(item.get("goal"))})
        else:
            metric = str(item or "").strip()
            if metric:
                specs.append({"metric": metric, "goal": "max"})
    return specs


def _extract_plot_objectives(data: dict) -> dict[str, float]:
    result = data.get("result") if isinstance(data.get("result"), dict) else data
    objectives = result.get("objectives") if isinstance(result, dict) else None
    if not isinstance(objectives, dict):
        metrics = data.get("metrics") if isinstance(data.get("metrics"), dict) else {}
        if isinstance(metrics.get("objectives"), dict):
            objectives = metrics.get("objectives")
        else:
            objectives = data.get("objectives") if isinstance(data.get("objectives"), dict) else {}
    extracted: dict[str, float] = {}
    for key, value in objectives.items():
        numeric = _coerce_metric_float(value)
        if numeric is not None:
            extracted[str(key)] = numeric
    return extracted


def _build_pareto_3d_plot_payload(result_dir: Path) -> dict:
    first_pareto = _first_pareto_file(result_dir)
    if not first_pareto:
        raise HTTPException(404, "No pareto config found for result")

    pareto_files = sorted(first_pareto.parent.glob("*.json"))
    if not pareto_files:
        raise HTTPException(404, "No pareto files found for result")

    first_data = _load_pareto_json(first_pareto)
    result_name = _result_name_from_data(first_data, result_dir.name)
    specs = _extract_plot_objective_specs(first_data)
    first_objectives = _extract_plot_objectives(first_data)
    if not specs and first_objectives:
        specs = [{"metric": key, "goal": "max"} for key in first_objectives.keys()]

    objective_names = [spec["metric"] for spec in specs]
    if len(objective_names) != 3:
        label = "objective" if len(objective_names) == 1 else "objectives"
        return {
            "ok": False,
            "message": (
                "PB7 3D plot is only available when a result exposes exactly 3 objectives. "
                f"This result exposes {len(objective_names)} {label}."
            ),
            "output": json.dumps({"objectives": objective_names}, indent=2),
        }

    points: list[list[float]] = []
    hover_texts: list[str] = []
    filenames: list[str] = []
    for path in pareto_files:
        try:
            data = _load_pareto_json(path)
        except Exception:
            continue
        objectives = _extract_plot_objectives(data)
        values = [objectives.get(name) for name in objective_names]
        if any(value is None for value in values):
            continue
        numeric_values = [float(value) for value in values]
        points.append(numeric_values)
        filenames.append(path.name)
        hover_lines = [f"Pareto file: {path.name}"]
        hover_lines.extend(
            f"{metric}: {numeric_values[index]:.6g}" for index, metric in enumerate(objective_names)
        )
        hover_texts.append("<br>".join(hover_lines))

    if not points:
        return {
            "ok": False,
            "message": f"PB7 reported no valid Pareto points for {result_name}.",
            "output": json.dumps({"objectives": objective_names, "points": 0}, indent=2),
        }

    values_matrix = np.asarray(points, dtype=float)
    ideal = np.asarray(
        [
            float(np.min(values_matrix[:, index])) if spec["goal"] == "min" else float(np.max(values_matrix[:, index]))
            for index, spec in enumerate(specs)
        ],
        dtype=float,
    )
    mins = np.min(values_matrix, axis=0)
    maxs = np.max(values_matrix, axis=0)
    norm_matrix = np.asarray(
        [
            [
                (value - mins[index]) / (maxs[index] - mins[index]) if maxs[index] > mins[index] else value
                for index, value in enumerate(row)
            ]
            for row in values_matrix
        ],
        dtype=float,
    )
    ideal_norm = np.asarray(
        [
            (ideal[index] - mins[index]) / (maxs[index] - mins[index]) if maxs[index] > mins[index] else ideal[index]
            for index in range(len(ideal))
        ],
        dtype=float,
    )
    distances = np.linalg.norm(norm_matrix - ideal_norm, axis=1)
    closest_idx = int(np.argmin(distances))

    fig = go.Figure()
    fig.add_trace(
        go.Scatter3d(
            x=values_matrix[:, 0],
            y=values_matrix[:, 1],
            z=values_matrix[:, 2],
            mode="markers",
            marker={"size": 4, "color": "#1d4ed8"},
            name="Pareto Members",
            text=hover_texts,
            hovertemplate="%{text}<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter3d(
            x=[ideal[0]],
            y=[ideal[1]],
            z=[ideal[2]],
            mode="markers",
            marker={"size": 8, "color": "#16a34a"},
            name="Ideal Point",
        )
    )
    fig.add_trace(
        go.Scatter3d(
            x=[values_matrix[closest_idx, 0]],
            y=[values_matrix[closest_idx, 1]],
            z=[values_matrix[closest_idx, 2]],
            mode="markers",
            marker={"size": 8, "color": "#dc2626"},
            name="Closest to Ideal",
            text=[hover_texts[closest_idx]],
            hovertemplate="%{text}<extra></extra>",
        )
    )
    fig.update_layout(
        title="Pareto Front (3D Interactive)",
        scene={
            "xaxis_title": objective_names[0],
            "yaxis_title": objective_names[1],
            "zaxis_title": objective_names[2],
        },
        margin={"l": 0, "r": 0, "b": 0, "t": 48},
        legend={"x": 0.01, "y": 0.99},
    )
    plot_html = fig.to_html(full_html=False, include_plotlyjs=True, config={"responsive": True, "displaylogo": False})
    html = (
        "<!doctype html><html><head><meta charset=\"utf-8\">"
        "<style>html,body{margin:0;height:100%;overflow:hidden;background:#fff;}"
        "body{font-family:system-ui,sans-serif;}"
        ".plot-shell{width:100%;height:100%;}.plot-shell>div{width:100%;height:100%;}</style>"
        "</head><body><div class=\"plot-shell\">"
        f"{plot_html}"
        "</div></body></html>"
    )
    summary_lines = [
        f"Found {len(points)} Pareto members.",
        "Ideal point:",
        *(f"  {metric}: {ideal[index]:.6g}" for index, metric in enumerate(objective_names)),
        f"Closest to ideal: {filenames[closest_idx]} | norm_dist={distances[closest_idx]:.6g}",
        *(
            f"  {metric}: {values_matrix[closest_idx, index]:.6g}"
            for index, metric in enumerate(objective_names)
        ),
    ]
    return {
        "ok": True,
        "message": f"PB7 3D plot opened for {result_name}.",
        "html": html,
        "output": "\n".join(summary_lines),
    }


def _first_pareto_file(result_dir: Path) -> Optional[Path]:
    pareto_dir = result_dir / "pareto"
    if not pareto_dir.exists():
        return None
    for path in sorted(pareto_dir.glob("*.json")):
        return path
    return None


def _result_name_from_pareto(path: Path) -> str:
    try:
        data = _load_pareto_json(path)
        return _result_name_from_data(data, path.parent.parent.name)
    except Exception:
        pass
    return path.parent.parent.name


def _serialize_exchange(exchange_value):
    if exchange_value is None:
        return []
    if isinstance(exchange_value, list):
        return exchange_value
    if isinstance(exchange_value, tuple):
        return list(exchange_value)
    return [exchange_value]


def _resolve_optimize_seed(config: dict | None) -> tuple[str, str]:
    cfg = config if isinstance(config, dict) else {}
    pbgui = cfg.get("pbgui") if isinstance(cfg.get("pbgui"), dict) else {}
    optimize = cfg.get("optimize") if isinstance(cfg.get("optimize"), dict) else {}

    mode = str(pbgui.get("optimize_seed_mode") or "").strip().lower()
    path = str(pbgui.get("optimize_seed_path") or "").strip()
    legacy_self = bool(pbgui.get("starting_config"))
    legacy_path = str(optimize.get("starting_config") or "").strip()

    if mode not in {"none", "self", "path"}:
        if path:
            mode = "path"
        elif legacy_self:
            mode = "self"
        elif legacy_path:
            mode = "path"
            path = legacy_path
        else:
            mode = "none"

    if mode == "path" and not path:
        if legacy_path:
            path = legacy_path
        elif legacy_self:
            mode = "self"
        else:
            mode = "none"

    if mode != "path":
        path = ""

    return mode, path


def _ensure_result_path(path: str) -> Path:
    result_dir = Path(path).resolve()
    base = _opt_results_base().resolve()
    if not result_dir.is_relative_to(base):
        raise HTTPException(400, "Invalid result path")
    return result_dir


def _ensure_pareto_path(path: str) -> Path:
    pareto_path = Path(path).resolve()
    base = _opt_results_base().resolve()
    if not pareto_path.is_relative_to(base):
        raise HTTPException(400, "Invalid pareto path")
    return pareto_path


def _create_pareto_seed_bundle(result_dir: Path, pareto_paths: list[Path]) -> Path:
    bundle_root = result_dir / "_seed_bundles"
    bundle_root.mkdir(parents=True, exist_ok=True)

    bundle_name = f"{datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    temp_dir = bundle_root / f".{bundle_name}.tmp"
    bundle_dir = bundle_root / bundle_name
    temp_dir.mkdir(parents=True, exist_ok=False)

    try:
        for index, pareto_path in enumerate(pareto_paths, start=1):
            target = temp_dir / pareto_path.name
            if target.exists():
                target = temp_dir / f"{index:03d}_{pareto_path.name}"
            copy2(pareto_path, target)
        temp_dir.rename(bundle_dir)
    except Exception:
        rmtree(temp_dir, ignore_errors=True)
        raise

    return bundle_dir


def _pareto_dash_cache_root() -> Path:
    return Path(PBGDIR) / "data" / "cache" / "pareto_dash"


def _pareto_dash_log_path() -> Path:
    return Path(PBGDIR) / "data" / "logs" / "pareto_dash.log"


def _read_text_excerpt(path: Path, *, tail_kb: int = 32) -> str:
    if not path.exists():
        return ""
    try:
        with open(path, "rb") as handle:
            handle.seek(0, 2)
            file_size = handle.tell()
            tail_bytes = max(0, tail_kb * 1024)
            handle.seek(max(file_size - tail_bytes, 0))
            return handle.read().decode("utf-8", errors="ignore")
    except Exception:
        return ""


def _find_free_local_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as handle:
        handle.bind(("127.0.0.1", 0))
        handle.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return int(handle.getsockname()[1])


def _cleanup_pareto_dash_stage(stage_root: Optional[Path]) -> None:
    if not stage_root:
        return
    rmtree(stage_root, ignore_errors=True)


def _terminate_process(proc: Optional[subprocess.Popen]) -> None:
    if proc is None or proc.poll() is not None:
        return
    try:
        proc.terminate()
        proc.wait(timeout=2)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=2)
    except Exception:
        pass


def _prune_dead_pareto_dash_sessions() -> None:
    stale_ids = []
    with _pareto_dash_lock:
        for session_id, session in _pareto_dash_sessions.items():
            proc = session.get("process")
            if proc is not None and proc.poll() is None:
                continue
            stale_ids.append(session_id)
        for session_id in stale_ids:
            session = _pareto_dash_sessions.pop(session_id, None)
            if session:
                _cleanup_pareto_dash_stage(session.get("stage_root"))


def _stop_pareto_dash_session(session_id: str) -> None:
    with _pareto_dash_lock:
        session = _pareto_dash_sessions.pop(session_id, None)
    if not session:
        return
    _terminate_process(session.get("process"))
    _cleanup_pareto_dash_stage(session.get("stage_root"))
    _log(SERVICE, f"Stopped Pareto Dash session {session_id}")


def _wait_for_pareto_dash_ready(proc: subprocess.Popen, port: int, prefix: str, *, timeout: float = 8.0) -> tuple[bool, str]:
    target_url = f"http://127.0.0.1:{port}{prefix}"
    deadline = time.time() + timeout
    with httpx.Client(timeout=1.0, follow_redirects=False) as client:
        while time.time() < deadline:
            if proc.poll() is not None:
                return False, "Pareto Dash process exited before it became ready."
            try:
                response = client.get(target_url)
                if response.status_code < 500:
                    return True, ""
            except Exception:
                pass
            time.sleep(0.2)
    return False, f"Pareto Dash did not become ready at {target_url}."


def _launch_pareto_dash_session(session_id: str, result_dir: Path, pathname_prefix: str) -> dict:
    if not (result_dir / "pareto").is_dir():
        raise HTTPException(400, "Result has no pareto directory")

    stage_root = _pareto_dash_cache_root() / session_id
    data_root = stage_root / "runs"
    link_path = data_root / result_dir.name
    port = _find_free_local_port()
    log_path = _pareto_dash_log_path()

    data_root.mkdir(parents=True, exist_ok=False)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        link_path.symlink_to(result_dir, target_is_directory=True)
    except Exception:
        _cleanup_pareto_dash_stage(stage_root)
        raise HTTPException(500, "Unable to prepare Pareto Dash staging directory")

    script_path = Path(pb7dir()) / "src" / "tools" / "pareto_dash.py"
    cmd = [
        pb7venv(),
        "-u",
        str(script_path),
        "--data-root",
        str(data_root),
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--pathname-prefix",
        pathname_prefix,
    ]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(Path(pb7dir()) / "src")

    log_handle = open(log_path, "a", encoding="utf-8")
    log_handle.write(f"\n[{datetime.datetime.utcnow().isoformat()}] start session={session_id} result={result_dir}\n")
    log_handle.flush()
    try:
        popen_kwargs = dict(
            stdout=log_handle,
            stderr=log_handle,
            cwd=pb7dir(),
            env=env,
        )
        if platform.system() == "Windows":
            popen_kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS
            proc = subprocess.Popen(cmd, **popen_kwargs)
        else:
            proc = subprocess.Popen(cmd, start_new_session=True, **popen_kwargs)
    except Exception:
        log_handle.close()
        _cleanup_pareto_dash_stage(stage_root)
        raise
    finally:
        log_handle.close()

    ready, message = _wait_for_pareto_dash_ready(proc, port, pathname_prefix)
    if not ready:
        _terminate_process(proc)
        output = _read_text_excerpt(log_path)
        _cleanup_pareto_dash_stage(stage_root)
        detail = message if not output else f"{message}\n\n{output.strip()}"
        raise HTTPException(500, detail.strip())

    session = {
        "session_id": session_id,
        "result_dir": str(result_dir),
        "pathname_prefix": pathname_prefix,
        "port": port,
        "process": proc,
        "stage_root": stage_root,
        "started_at": time.time(),
    }
    with _pareto_dash_lock:
        _pareto_dash_sessions[session_id] = session

    _log(SERVICE, f"Started Pareto Dash session {session_id} for {result_dir.name} on port {port}")
    return session


def _get_pareto_dash_session(session_id: str) -> Optional[dict]:
    _prune_dead_pareto_dash_sessions()
    with _pareto_dash_lock:
        session = _pareto_dash_sessions.get(session_id)
        if session:
            session["last_seen_at"] = time.time()
        return session


class OptimizeStore:
    """In-memory view of the optimize queue, refreshed from disk."""

    def __init__(self):
        self.items: dict[str, dict] = {}
        self.changed = asyncio.Event()
        self._lock = asyncio.Lock()

    async def refresh_from_disk(self) -> None:
        async with self._lock:
            queue_dir = _opt_queue_dir()
            queue_dir.mkdir(parents=True, exist_ok=True)
            process_index = _build_optimize_process_index()
            config_counts = _build_queue_config_counts(queue_dir)
            found = {}
            for fp, data, created_ts in _load_sorted_queue_entries(queue_dir):
                try:
                    filename = data.get("filename", fp.stem)
                    cfg_path = Path(str(data.get("json") or ""))
                    log_path = _opt_log_dir() / f"{filename}.log"
                    pid = self._resolve_runtime_pid(
                        filename,
                        self._read_pid(filename),
                        cfg_path,
                        log_path,
                        process_index=process_index,
                        config_counts=config_counts,
                    )
                    self._migrate_old_log(filename, log_path)
                    status = self._determine_status(pid, log_path)
                    seed_mode = "none"
                    seed_path = ""
                    if cfg_path.exists():
                        try:
                            cfg = load_pb7_config(cfg_path)
                            seed_mode, seed_path = _resolve_optimize_seed(cfg)
                        except Exception:
                            seed_mode = "none"
                            seed_path = ""
                    found[filename] = {
                        "filename": filename,
                        "name": data.get("name", filename),
                        "json": str(data.get("json") or ""),
                        "exchange": _serialize_exchange(data.get("exchange")),
                        "status": status,
                        "pid": pid,
                        "log_path": str(log_path),
                        "starting_config": seed_mode != "none",
                        "seed_mode": seed_mode,
                        "seed_path": seed_path,
                        "created": datetime.datetime.fromtimestamp(created_ts).isoformat(),
                        "order": _coerce_queue_order(data.get("order")),
                    }
                except Exception as exc:
                    _log(SERVICE, f"Error loading queue item {fp}: {exc}", level="WARNING")
            self.items = found
            self.changed.set()

    def _migrate_old_log(self, filename: str, log_path: Path) -> None:
        old_log = _opt_queue_dir() / f"{filename}.log"
        if old_log.exists() and not log_path.exists():
            log_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                old_log.rename(log_path)
            except Exception:
                pass

    def _read_pid(self, filename: str) -> Optional[int]:
        pid_file = _opt_queue_dir() / f"{filename}.pid"
        if not pid_file.exists():
            return None
        try:
            text = pid_file.read_text(encoding="utf-8").strip()
            return int(text) if text.isdigit() else None
        except Exception:
            return None

    def _write_pid(self, filename: str, pid: Optional[int]) -> None:
        pid_file = _opt_queue_dir() / f"{filename}.pid"
        if pid is None:
            pid_file.unlink(missing_ok=True)
            return
        pid_file.write_text(str(pid), encoding="utf-8")

    def _is_process_running(self, pid: int) -> bool:
        try:
            if pid and psutil.pid_exists(pid):
                proc = psutil.Process(pid)
                return _is_optimize_process_cmdline(proc.cmdline())
        except (psutil.NoSuchProcess, psutil.ZombieProcess, psutil.AccessDenied):
            pass
        return False

    def _resolve_runtime_pid(
        self,
        filename: str,
        pid: Optional[int],
        config_path: Path | str | None,
        log_path: Path | str | None,
        *,
        process_index: list[dict] | None = None,
        config_counts: dict[str, int] | None = None,
    ) -> Optional[int]:
        normalized_config = _normalize_process_arg_path(config_path)
        allow_config_fallback = (config_counts or {}).get(normalized_config, 0) <= 1 if normalized_config else True

        tracked_descriptor = None
        if pid is not None and self._is_process_running(pid):
            for descriptor in (process_index if process_index is not None else _build_optimize_process_index()):
                if descriptor.get("pid") == pid:
                    tracked_descriptor = descriptor
                    break
        if tracked_descriptor is not None:
            selected = _select_optimize_process_descriptor(
                config_path,
                log_path,
                process_index=[tracked_descriptor],
                allow_config_fallback=allow_config_fallback,
            )
            if selected is not None:
                return pid

        live_roots = _find_optimize_process_roots_for_config(
            config_path,
            log_path,
            process_index=process_index,
            allow_config_fallback=allow_config_fallback,
        )
        live_pid = live_roots[0].pid if live_roots else None
        if live_pid != pid:
            self._write_pid(filename, live_pid)
            if live_pid is not None:
                _log(
                    SERVICE,
                    f"Recovered live optimize pid {live_pid} for {filename} after tracked pid {pid} went stale",
                    level="INFO",
                )
        return live_pid

    def _read_log_tail(self, log_path: Path, size_kb: int = 50) -> Optional[str]:
        if not log_path.exists():
            return None
        try:
            with open(log_path, "rb") as f:
                f.seek(0, 2)
                file_size = f.tell()
                start = max(file_size - size_kb * 1024, 0)
                f.seek(start)
                return f.read().decode("utf-8", errors="ignore")
        except Exception:
            return None

    def _determine_status(self, pid: Optional[int], log_path: Path) -> str:
        running = pid is not None and self._is_process_running(pid)
        log_tail = self._read_log_tail(log_path)
        if running:
            if log_tail and "Initial population size" in log_tail and "Optimization complete" not in log_tail:
                return "optimizing"
            return "running"
        if log_tail:
            if "successfully processed optimize_results" in log_tail or "Optimization complete" in log_tail:
                return "complete"
            return "error"
        return "queued"

    def notify(self) -> None:
        self.changed.set()


_store = OptimizeStore()


class OptimizeWorker:
    """Processes queued optimize jobs as a background task."""

    def __init__(self, store: OptimizeStore):
        self.store = store
        self._task: Optional[asyncio.Task] = None
        self._running = False

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._running = True
            self._task = asyncio.create_task(self._loop(), name="optimize-v7-worker")
            _log(SERVICE, "Optimize worker started", level="INFO")

    def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()

    async def _loop(self) -> None:
        try:
            while self._running:
                settings = _read_ini_section()
                autostart = settings.get("autostart", "False").lower() == "true"
                cpu_override_enabled = settings.get("cpu_override", "True").lower() == "true"
                autostart_cpu = _normalize_autostart_cpu(settings.get("cpu", "1"))
                if not autostart:
                    await asyncio.sleep(5)
                    continue

                await self.store.refresh_from_disk()
                items = list(self.store.items.values())
                if any(item["status"] in ("running", "optimizing") for item in items):
                    await asyncio.sleep(10)
                    continue

                queued = [item for item in items if item["status"] == "queued"]
                if queued:
                    launch_filename = str(queued[0].get("filename") or "")
                    launch_item = _queue_launch_item_from_data(launch_filename, _read_queue_item_data(launch_filename))
                    self._launch_optimize(launch_item, cpu_override=(autostart_cpu if cpu_override_enabled else None))
                    _log(SERVICE, f"Launched optimize: {launch_item['name']} ({launch_item['filename']})", level="INFO")
                    await asyncio.sleep(1)
                    await self.store.refresh_from_disk()

                await asyncio.sleep(10)
        except asyncio.CancelledError:
            _log(SERVICE, "Optimize worker stopped", level="INFO")
        except Exception as exc:
            _log(SERVICE, f"Optimize worker error: {exc}", level="ERROR", meta={"traceback": traceback.format_exc()})

    def _launch_optimize(self, item: dict, cpu_override: Optional[int] = None) -> None:
        filename = str(item.get("filename") or "")
        snapshot = _queue_config_snapshot(item)
        original_config_path = Path(str(item.get("json") or ""))

        try:
            config_path, config = _resolve_queue_launch_config(item)
        except HTTPException:
            raise
        except Exception as exc:
            log_path = _opt_log_dir() / f"{filename}.log"
            log_path.parent.mkdir(parents=True, exist_ok=True)
            log_path.write_text(f"Failed to load optimize config: {exc}\n", encoding="utf-8")
            return

        if not original_config_path.exists() and snapshot is not None and config_path == original_config_path:
            config = copy.deepcopy(snapshot)
            try:
                config_path.parent.mkdir(parents=True, exist_ok=True)
                save_pb7_config(config, config_path)
                _log(
                    SERVICE,
                    f"Restored missing optimize config {config_path} from queue snapshot for {item.get('name') or filename}",
                    level="INFO",
                )
            except Exception as exc:
                log_path = _opt_log_dir() / f"{filename}.log"
                log_path.parent.mkdir(parents=True, exist_ok=True)
                log_path.write_text(f"Failed to restore optimize config from queue snapshot: {exc}\n", encoding="utf-8")
                return

        if cpu_override is not None:
            try:
                override_cpu = _normalize_autostart_cpu(cpu_override)
                optimize = config.setdefault("optimize", {})
                current_cpu = optimize.get("n_cpus")
                try:
                    current_cpu = int(current_cpu) if current_cpu not in (None, "") else None
                except Exception:
                    current_cpu = None
                if current_cpu != override_cpu:
                    optimize["n_cpus"] = override_cpu
                    save_pb7_config(config, config_path)
                    _log(
                        SERVICE,
                        f"Adjusted optimize.n_cpus to {override_cpu} before autostart launch for {item.get('name') or filename}",
                        level="INFO",
                    )
            except Exception as exc:
                log_path = _opt_log_dir() / f"{filename}.log"
                log_path.parent.mkdir(parents=True, exist_ok=True)
                log_path.write_text(f"Failed to apply autostart CPU override: {exc}\n", encoding="utf-8")
                return

        preflight_errors = pb7_suite_preflight_errors(config)
        if preflight_errors:
            log_path = _opt_log_dir() / f"{filename}.log"
            log_path.parent.mkdir(parents=True, exist_ok=True)
            log_path.write_text("\n\n".join(preflight_errors) + "\n", encoding="utf-8")
            return

        seed_mode, seed_path = _resolve_optimize_seed(config)
        log_path = _opt_log_dir() / f"{filename}.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)

        cmd = [pb7venv(), "-u", str(PurePath(f"{pb7dir()}/src/optimize.py"))]
        if seed_mode == "self":
            cmd.extend(["-t", str(config_path), str(config_path)])
        elif seed_mode == "path":
            cmd.extend(["-t", seed_path, str(config_path)])
        else:
            cmd.append(str(config_path))

        env = os.environ.copy()
        env["PATH"] = os.path.dirname(pb7venv()) + os.pathsep + env.get("PATH", "")
        log_file = open(log_path, "w", encoding="utf-8")
        if platform.system() == "Windows":
            flags = subprocess.DETACHED_PROCESS | subprocess.CREATE_NO_WINDOW
            proc = subprocess.Popen(
                cmd,
                stdout=log_file,
                stderr=log_file,
                cwd=pb7dir(),
                text=True,
                creationflags=flags,
                env=env,
            )
        else:
            proc = subprocess.Popen(
                cmd,
                stdout=log_file,
                stderr=log_file,
                cwd=pb7dir(),
                text=True,
                start_new_session=True,
                env=env,
            )
        (_opt_queue_dir() / f"{filename}.pid").write_text(str(proc.pid), encoding="utf-8")


_worker = OptimizeWorker(_store)


_ws_clients: set[WebSocket] = set()


async def _ws_push_loop(ws: WebSocket) -> None:
    try:
        while True:
            try:
                await _store.refresh_from_disk()
                _store.changed.clear()
                await ws.send_json(
                    {
                        "type": "queue_update",
                        "items": list(_store.items.values()),
                        "settings": _read_ini_section(),
                    }
                )
            except (WebSocketDisconnect, RuntimeError):
                break
            except Exception as exc:
                _log(SERVICE, f"Optimize WS push error: {exc}", level="WARNING")
            try:
                await asyncio.wait_for(_store.changed.wait(), timeout=3.0)
            except asyncio.TimeoutError:
                pass
    except asyncio.CancelledError:
        pass


@router.websocket("/ws/opt7")
async def ws_optimize(websocket: WebSocket):
    token = websocket.query_params.get("token", "")
    if not validate_token(token):
        await websocket.close(code=4001)
        return
    await websocket.accept()
    _ws_clients.add(websocket)
    push_task = asyncio.create_task(_ws_push_loop(websocket))
    try:
        while True:
            data = await websocket.receive_text()
            try:
                msg = json.loads(data)
                if msg.get("type") == "refresh":
                    await _store.refresh_from_disk()
                    _store.notify()
            except json.JSONDecodeError:
                pass
    except WebSocketDisconnect:
        pass
    finally:
        _ws_clients.discard(websocket)
        push_task.cancel()
        try:
            await push_task
        except asyncio.CancelledError:
            pass


def startup() -> None:
    _worker.start()


def shutdown() -> None:
    _worker.stop()


@router.get("/main_page", response_class=HTMLResponse)
def main_page(
    request: Request,
    st_base: str = Query(default="", description="Streamlit base URL"),
    session: SessionToken = Depends(require_auth),
) -> HTMLResponse:
    html_path = Path(__file__).resolve().parent.parent / "frontend" / "v7_optimize.html"
    if not html_path.exists():
        raise HTTPException(404, "v7_optimize.html not found")
    html = html_path.read_text(encoding="utf-8")

    scheme = request.url.scheme
    host = request.url.hostname or "127.0.0.1"
    port = request.url.port
    origin = f"{scheme}://{host}" + (f":{port}" if port else "")
    api_base = origin + "/api/optimize-v7"
    ws_base = origin.replace("http://", "ws://").replace("https://", "wss://")

    html = html.replace('"%%TOKEN%%"', json.dumps(session.token))
    html = html.replace('"%%API_BASE%%"', json.dumps(api_base))
    html = html.replace('"%%WS_BASE%%"', json.dumps(ws_base))
    html = html.replace("%%LIMITS_META%%", json.dumps(get_optimize_limits_meta_payload()))

    if not st_base:
        st_base = f"http://{host}:8501"
    html = html.replace('"%%ST_BASE%%"', json.dumps(st_base))

    from pbgui_purefunc import PBGUI_SERIAL, PBGUI_VERSION
    html = html.replace('"%%VERSION%%"', json.dumps(PBGUI_VERSION))
    html = html.replace("%%VERSION%%", PBGUI_VERSION)
    html = html.replace('"%%SERIAL%%"', json.dumps(PBGUI_SERIAL))
    html = html.replace("%%SERIAL%%", PBGUI_SERIAL)

    nav_js = Path(__file__).resolve().parent.parent / "frontend" / "pbgui_nav.js"
    nav_hash = str(int(nav_js.stat().st_mtime)) if nav_js.exists() else PBGUI_VERSION
    html = html.replace("%%NAV_HASH%%", nav_hash)

    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})


@router.get("/settings")
def get_settings(session: SessionToken = Depends(require_auth)):
    settings = _read_ini_section()
    template = get_template_config()
    optimize_template = template.get("optimize", {}) if isinstance(template, dict) else {}
    optimize_template = dict(optimize_template or {})
    optimize_template.setdefault("n_cpus", multiprocessing.cpu_count())
    cpu_max = multiprocessing.cpu_count()
    return {
        "autostart": settings.get("autostart", "False").lower() == "true",
        "cpu": _normalize_autostart_cpu(settings.get("cpu", "1")),
        "cpu_override": settings.get("cpu_override", "True").lower() == "true",
        "cpu_max": cpu_max,
        "host_cpu_count": cpu_max,
        "hsl_signal_modes": get_hsl_signal_modes(),
        "optimize_backend_options": get_optimize_backend_options(),
        "optimize_backend_default": optimize_template.get("backend", "pymoo"),
        "pymoo_algorithm_options": get_pymoo_algorithm_options(),
        "pymoo_ref_dir_method_options": get_pymoo_ref_dir_method_options(),
        "optimize_defaults": optimize_template,
    }


@router.get("/pbgui_data_path")
def get_pbgui_data_path(session: SessionToken = Depends(require_auth)):
    """Return the PBGui-managed market data root directory."""
    from market_data import get_market_data_root_dir
    return {"path": str(get_market_data_root_dir())}


@router.get("/bot-params")
def get_bot_params(session: SessionToken = Depends(require_auth)):
    """Return list of bot.long parameter names from passivbot schema."""
    try:
        return {"params": [{"key": key} for key in get_bot_param_keys()]}
    except Exception as exc:
        _log(SERVICE, f"Failed to load bot params: {exc}", level="warning")
        return {"params": []}


@router.post("/settings")
def update_settings(body: dict, session: SessionToken = Depends(require_auth)):
    if "autostart" in body:
        _write_ini("autostart", str(bool(body["autostart"])))
    if "cpu" in body:
        _write_ini("cpu", str(_normalize_autostart_cpu(body["cpu"])))
    if "cpu_override" in body:
        _write_ini("cpu_override", str(bool(body["cpu_override"])))
    _store.notify()
    return {"ok": True}


@router.get("/configs/new-config")
def get_new_optimize_config(session: SessionToken = Depends(require_auth)):
    tmpl = _get_new_optimize_template()
    return _editor_config_payload(tmpl)


@router.post("/configs/prepare")
def prepare_config_for_editor(body: dict, session: SessionToken = Depends(require_auth)):
    cfg = body.get("config") if isinstance(body, dict) else None
    if not isinstance(cfg, dict):
        raise HTTPException(status_code=400, detail="Missing or invalid 'config' in body")
    try:
        prepared = prepare_pb7_config_dict(cfg, neutralize_added=True)
        prepared = _restore_optimize_editor_backend_semantics(prepared, cfg)
        return _editor_config_payload(prepared, backend_hint=_infer_optimize_backend_hint(cfg))
    except Exception as exc:
        detail = str(exc).strip() or exc.__class__.__name__
        _log(
            SERVICE,
            f"Failed to prepare optimize config: {detail}",
            level="WARNING",
            meta={"traceback": traceback.format_exc()},
        )
        raise HTTPException(status_code=422, detail=detail) from exc


@router.get("/configs")
def list_configs(session: SessionToken = Depends(require_auth)):
    configs = []
    base = _opt_configs_dir()
    if base.exists():
        for cfg_file in sorted(base.glob("*.json")):
            try:
                cfg = load_pb7_config(cfg_file, neutralize_added=True)
                name = cfg_file.stem
                backtests_dir = Path(pb7dir()) / "backtests" / "pbgui" / name
                backtest_count = len(list(backtests_dir.glob("**/analysis.json"))) if backtests_dir.exists() else 0
                bt = cfg.get("backtest", {})
                seed_mode, seed_path = _resolve_optimize_seed(cfg)
                configs.append(
                    {
                        "name": name,
                        "exchange": _serialize_exchange(bt.get("exchanges")),
                        "backtest_count": backtest_count,
                        "modified": datetime.datetime.fromtimestamp(cfg_file.stat().st_mtime).isoformat(),
                        "start_date": bt.get("start_date", ""),
                        "end_date": bt.get("end_date", ""),
                        "starting_config": seed_mode != "none",
                        "seed_mode": seed_mode,
                        "seed_path": seed_path,
                    }
                )
            except Exception as exc:
                _log(SERVICE, f"Error reading optimize config {cfg_file}: {exc}", level="WARNING")
    return {"configs": configs}


@router.get("/configs/{name}")
def get_config(name: str, session: SessionToken = Depends(require_auth)):
    _validate_name(name)
    cfg_file = _opt_configs_dir() / f"{name}.json"
    if not cfg_file.exists():
        raise HTTPException(404, f"Config '{name}' not found")
    return _load_editor_payload_from_config_path(cfg_file, name=name)


@router.put("/configs/{name}")
def save_config(name: str, body: dict, source_name: str | None = None,
                session: SessionToken = Depends(require_auth)):
    _validate_name(name)
    cfg = dict(body or {})
    backtest = dict(cfg.get("backtest") or {})
    backtest["base_dir"] = f"backtests/pbgui/{name}"
    cfg["backtest"] = backtest
    optimize = dict(cfg.get("optimize") or {})
    raw_n_cpus = optimize.get("n_cpus")
    if raw_n_cpus not in (None, ""):
        try:
            optimize["n_cpus"] = max(1, min(multiprocessing.cpu_count(), int(raw_n_cpus)))
        except (TypeError, ValueError):
            optimize["n_cpus"] = multiprocessing.cpu_count()
        cfg["optimize"] = optimize
    cfg_file = _opt_configs_dir() / f"{name}.json"
    old_cfg_file = _opt_configs_dir() / f"{source_name}.json" if source_name else cfg_file
    save_pb7_config(cfg, cfg_file)
    _update_queue_config_references(
        [old_cfg_file, cfg_file],
        target_path=cfg_file,
        target_name=name,
        config_snapshot=cfg,
    )
    if source_name and source_name != name:
        _validate_name(source_name)
        (_opt_configs_dir() / f"{source_name}.json").unlink(missing_ok=True)
    return {"ok": True, "name": name}


@router.delete("/configs/{name}")
def delete_config(name: str, session: SessionToken = Depends(require_auth)):
    _validate_name(name)
    cfg_file = _opt_configs_dir() / f"{name}.json"
    if not cfg_file.exists():
        raise HTTPException(404, f"Config '{name}' not found")
    cfg_file.unlink(missing_ok=True)
    return {"ok": True}


@router.post("/configs/{name}/duplicate")
def duplicate_config(name: str, body: dict, session: SessionToken = Depends(require_auth)):
    _validate_name(name)
    new_name = str(body.get("new_name") or "")
    _validate_name(new_name)
    src = _opt_configs_dir() / f"{name}.json"
    dst = _opt_configs_dir() / f"{new_name}.json"
    if not src.exists():
        raise HTTPException(404, f"Config '{name}' not found")
    if dst.exists():
        raise HTTPException(409, f"Config '{new_name}' already exists")
    cfg = load_pb7_config(src, neutralize_added=True)
    cfg.setdefault("backtest", {})["base_dir"] = f"backtests/pbgui/{new_name}"
    save_pb7_config(cfg, dst)
    return {"ok": True, "name": new_name}


def _load_queue_sync() -> list[dict]:
    queue_dir = _opt_queue_dir()
    queue_dir.mkdir(parents=True, exist_ok=True)
    process_index = _build_optimize_process_index()
    config_counts = _build_queue_config_counts(queue_dir)
    items = []
    for fp, data, created_ts in _load_sorted_queue_entries(queue_dir):
        try:
            filename = data.get("filename", fp.stem)
            cfg_path = Path(str(data.get("json") or ""))
            log_path = _opt_log_dir() / f"{filename}.log"
            pid = _store._resolve_runtime_pid(
                filename,
                _store._read_pid(filename),
                cfg_path,
                log_path,
                process_index=process_index,
                config_counts=config_counts,
            )
            _store._migrate_old_log(filename, log_path)
            status = _store._determine_status(pid, log_path)
            items.append(
                {
                    "filename": filename,
                    "name": data.get("name", filename),
                    "json": str(data.get("json") or ""),
                    "exchange": _serialize_exchange(data.get("exchange")),
                    "status": status,
                    "pid": pid,
                    "log_path": str(log_path),
                    "created": datetime.datetime.fromtimestamp(created_ts).isoformat(),
                    "order": _coerce_queue_order(data.get("order")),
                }
            )
        except Exception as exc:
            _log(SERVICE, f"Error loading optimize queue item {fp}: {exc}", level="WARNING")
    return items


@router.get("/queue")
def get_queue(session: SessionToken = Depends(require_auth)):
    return {"items": _load_queue_sync()}


@router.post("/queue/reorder")
def reorder_queue(body: dict, session: SessionToken = Depends(require_auth)):
    filenames = body.get("filenames") if isinstance(body, dict) else None
    if filenames is None:
        raise HTTPException(status_code=400, detail="filenames is required")
    if not isinstance(filenames, list):
        raise HTTPException(status_code=400, detail="filenames must be a list")

    queue_dir = _opt_queue_dir()
    queue_dir.mkdir(parents=True, exist_ok=True)
    entries = _load_sorted_queue_entries(queue_dir, error_prefix="queue reorder item")
    by_filename: dict[str, tuple[Path, dict]] = {}
    for fp, data, _ in entries:
        filename = str(data.get("filename") or fp.stem)
        by_filename[filename] = (fp, data)

    if not by_filename and not filenames:
        return {"ok": True, "count": 0}

    normalized: list[str] = []
    seen: set[str] = set()
    for raw in filenames:
        filename = str(raw or "").strip()
        _validate_name(filename)
        if filename in seen:
            raise HTTPException(status_code=400, detail="filenames must not contain duplicates")
        seen.add(filename)
        normalized.append(filename)

    if set(normalized) != set(by_filename) or len(normalized) != len(by_filename):
        raise HTTPException(status_code=400, detail="filenames must include all queue items exactly once")

    for index, filename in enumerate(normalized):
        fp, data = by_filename[filename]
        updated = copy.deepcopy(data)
        updated["filename"] = filename
        updated["order"] = index
        with open(fp, "w", encoding="utf-8") as f:
            json.dump(updated, f, indent=4)
            f.write("\n")

    _store.notify()
    return {"ok": True, "count": len(normalized)}


@router.post("/queue")
def add_to_queue(body: dict, session: SessionToken = Depends(require_auth)):
    name = str(body.get("name") or "")
    if not name:
        raise HTTPException(400, "name is required")
    _validate_name(name)

    cfg_file = _opt_configs_dir() / f"{name}.json"
    config = body.get("config")
    if config:
        cfg = dict(config)
        cfg.setdefault("backtest", {})["base_dir"] = f"backtests/pbgui/{name}"
        save_pb7_config(cfg, cfg_file)
    if not cfg_file.exists():
        raise HTTPException(404, f"Config '{name}' not found")

    try:
        cfg = load_pb7_config(cfg_file)
    except Exception as exc:
        raise HTTPException(500, f"Error reading config: {exc}") from exc

    filename = str(uuid.uuid4())
    queue_dir = _opt_queue_dir()
    queue_dir.mkdir(parents=True, exist_ok=True)
    queue_data = {
        "name": name,
        "filename": filename,
        "json": str(cfg_file),
        "config_snapshot": copy.deepcopy(cfg),
        "exchange": _serialize_exchange(cfg.get("backtest", {}).get("exchanges")),
        "order": _next_queue_insert_order(queue_dir),
    }
    queue_file = queue_dir / f"{filename}.json"
    with open(queue_file, "w", encoding="utf-8") as f:
        json.dump(queue_data, f, indent=4)
        f.write("\n")

    _store.notify()
    return {"ok": True, "filename": filename}


@router.post("/queue/{filename}/start")
def start_queue_item(filename: str, session: SessionToken = Depends(require_auth)):
    _validate_name(filename)
    data = _read_queue_item_data(filename)
    item = _queue_launch_item_from_data(filename, data)
    _worker._launch_optimize(item)
    _store.notify()
    return {"ok": True}


@router.post("/queue/{filename}/restart")
def restart_queue_item(filename: str, session: SessionToken = Depends(require_auth)):
    # Backwards compatible alias: "restart" now means "requeue".
    return requeue_queue_item(filename, session)


@router.post("/queue/{filename}/requeue")
def requeue_queue_item(filename: str, session: SessionToken = Depends(require_auth)):
    """Reset a queue item back to 'queued' so autostart can pick it up.

    This stops a running process (if any) and removes pid/log files so the
    item no longer shows up as running/error/complete.
    """
    _validate_name(filename)
    queue_file = _opt_queue_dir() / f"{filename}.json"
    if not queue_file.exists():
        raise HTTPException(404, "Queue item not found")

    data = _read_queue_item_data(filename)
    _validate_queue_item_requeueable(filename, data)

    # Ensure no process is running.
    stop_queue_item(filename, session)

    # Reset status by removing runtime artifacts.
    (_opt_queue_dir() / f"{filename}.pid").unlink(missing_ok=True)
    (_opt_log_dir() / f"{filename}.log").unlink(missing_ok=True)
    (_opt_queue_dir() / f"{filename}.log").unlink(missing_ok=True)

    _store.notify()
    return {"ok": True}


@router.post("/queue/{filename}/stop")
def stop_queue_item(filename: str, session: SessionToken = Depends(require_auth)):
    _validate_name(filename)
    queue_file = _opt_queue_dir() / f"{filename}.json"
    config_path = None
    log_path = _opt_log_dir() / f"{filename}.log"
    if queue_file.exists():
        try:
            with open(queue_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            config_path = Path(str(data.get("json") or ""))
        except Exception:
            config_path = None

    process_index = _build_optimize_process_index()
    config_counts = _build_queue_config_counts(_opt_queue_dir())
    tracked_pid = _store._resolve_runtime_pid(
        filename,
        _store._read_pid(filename),
        config_path,
        log_path,
        process_index=process_index,
        config_counts=config_counts,
    )
    killed = _kill_process_tree(tracked_pid or 0)
    for proc in _find_optimize_process_roots_for_config(
        config_path,
        log_path,
        process_index=process_index,
        allow_config_fallback=(config_counts.get(_normalize_process_arg_path(config_path), 0) <= 1 if config_path else True),
    ):
        if proc.pid in killed:
            continue
        killed.update(_kill_process_tree(proc.pid))

    refreshed_pid = _store._resolve_runtime_pid(
        filename,
        _store._read_pid(filename),
        config_path,
        log_path,
        process_index=_build_optimize_process_index(),
        config_counts=config_counts,
    )
    if refreshed_pid is None:
        _store._write_pid(filename, None)
    _store.notify()
    return {"ok": True}


@router.get("/queue/{filename}/config")
def get_queue_item_config(filename: str, session: SessionToken = Depends(require_auth)):
    _validate_name(filename)
    data = _read_queue_item_data(filename)
    return _load_editor_payload_from_queue_data(data, filename=filename)


@router.post("/queue/{filename}/repair-config")
def repair_queue_item_config(filename: str, body: dict, session: SessionToken = Depends(require_auth)):
    _validate_name(filename)
    target_name = str((body or {}).get("name") or "")
    if not target_name:
        raise HTTPException(status_code=400, detail="name is required")
    _validate_name(target_name)
    data = _read_queue_item_data(filename)
    repaired = _repair_queue_item_config_reference(filename, data, target_name)
    _store.notify()
    return {
        "ok": True,
        "filename": filename,
        "name": repaired.get("name", target_name),
        "json": repaired.get("json", ""),
    }


@router.delete("/queue/{filename}")
def remove_queue_item(filename: str, session: SessionToken = Depends(require_auth)):
    _validate_name(filename)
    stop_queue_item(filename, session)
    (_opt_queue_dir() / f"{filename}.json").unlink(missing_ok=True)
    (_opt_queue_dir() / f"{filename}.pid").unlink(missing_ok=True)
    (_opt_log_dir() / f"{filename}.log").unlink(missing_ok=True)
    (_opt_queue_dir() / f"{filename}.log").unlink(missing_ok=True)
    _store.notify()
    return {"ok": True}


@router.post("/queue/clear-finished")
def clear_finished(session: SessionToken = Depends(require_auth)):
    removed = 0
    for item in _load_queue_sync():
        if item["status"] == "complete":
            filename = item["filename"]
            (_opt_queue_dir() / f"{filename}.json").unlink(missing_ok=True)
            (_opt_queue_dir() / f"{filename}.pid").unlink(missing_ok=True)
            (_opt_log_dir() / f"{filename}.log").unlink(missing_ok=True)
            (_opt_queue_dir() / f"{filename}.log").unlink(missing_ok=True)
            removed += 1
    _store.notify()
    return {"ok": True, "removed": removed}


@router.get("/queue/{filename}/log")
def get_queue_log(filename: str, lines: int = 100,
                  session: SessionToken = Depends(require_auth)):
    _validate_name(filename)
    log_path = _opt_log_dir() / f"{filename}.log"
    _store._migrate_old_log(filename, log_path)
    if not log_path.exists():
        return {"log": "", "exists": False}
    tail = _store._read_log_tail(log_path, size_kb=max(10, lines))
    return {"log": tail or "", "exists": True}


@router.get("/queue/{filename}/status")
def get_queue_status(filename: str, session: SessionToken = Depends(require_auth)):
    _validate_name(filename)
    item = next((queued_item for queued_item in _load_queue_sync() if queued_item.get("filename") == filename), None)
    if not item:
        raise HTTPException(404, "Queue item not found")
    return _build_optimize_runtime_status(item)


@router.get("/results")
def list_results(session: SessionToken = Depends(require_auth)):
    base = _opt_results_base()
    if not base.exists():
        return {"results": []}
    results = []
    for all_results in sorted(base.glob("*/all_results.bin")):
        result_dir = all_results.parent
        pareto_dir = result_dir / "pareto"
        pareto_files = sorted(pareto_dir.glob("*.json")) if pareto_dir.exists() else []
        first_pareto = pareto_files[0] if pareto_files else None
        result_name = result_dir.name
        mode = "unknown"
        scenario_count = 0
        if first_pareto:
            try:
                first_data = _load_pareto_json(first_pareto)
                result_name = _result_name_from_data(first_data, result_dir.name)
                mode, scenario_labels = _detect_pareto_mode(first_data)
                scenario_count = len(scenario_labels)
            except Exception:
                result_name = result_dir.name
        results.append(
            {
                "path": str(result_dir),
                "result": result_dir.name,
                "name": result_name,
                "pareto_count": len(pareto_files),
                "mode": mode,
                "scenario_count": scenario_count,
                "modified": datetime.datetime.fromtimestamp(all_results.stat().st_mtime).isoformat(),
            }
        )
    return {"results": results}


@router.get("/results/config")
def get_result_config(path: str, session: SessionToken = Depends(require_auth)):
    result_dir = _ensure_result_path(path)
    first_pareto = _first_pareto_file(result_dir)
    if not first_pareto:
        raise HTTPException(404, "No pareto config found for result")
    try:
        data = _load_pareto_json(first_pareto)
        cfg = _extract_config_from_pareto_data(data)
        return _editor_config_payload(cfg, backend_hint=_infer_optimize_backend_hint(cfg))
    except Exception as exc:
        raise HTTPException(500, f"Error reading pareto config: {exc}") from exc


@router.delete("/results")
def delete_result(path: str, session: SessionToken = Depends(require_auth)):
    result_dir = _ensure_result_path(path)
    if not result_dir.exists():
        raise HTTPException(404, "Result not found")
    rmtree(str(result_dir), ignore_errors=True)
    return {"ok": True}


@router.post("/results/3d-plot")
def launch_result_3d_plot(body: dict, session: SessionToken = Depends(require_auth)):
    result_dir = _ensure_result_path(str((body or {}).get("path") or ""))
    payload = _build_pareto_3d_plot_payload(result_dir)
    if payload.get("ok"):
        _log(SERVICE, f"Rendered PB7 3D plot for {result_dir}")
    else:
        _log(SERVICE, f"PB7 3D plot unavailable for {result_dir}: {payload.get('message')}", level="warning")
    return payload


@router.post("/results/pareto-dash")
def launch_result_pareto_dash(body: dict, request: Request, session: SessionToken = Depends(require_auth)):
    result_dir = _ensure_result_path(str((body or {}).get("path") or ""))
    session_id = uuid.uuid4().hex[:12]
    pathname_prefix = request.app.url_path_for(
        "optimize_result_pareto_dash_proxy_root",
        session_id=session_id,
        access_token=session.token,
    )
    launched = _launch_pareto_dash_session(session_id, result_dir, str(pathname_prefix))
    launched["url"] = str(pathname_prefix)
    return {
        "ok": True,
        "session_id": launched["session_id"],
        "url": launched["url"],
        "result": result_dir.name,
    }


@router.api_route(
    "/results/pareto-dash/{session_id}/{access_token}/",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    name="optimize_result_pareto_dash_proxy_root",
)
@router.api_route(
    "/results/pareto-dash/{session_id}/{access_token}/{dash_path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    name="optimize_result_pareto_dash_proxy_path",
)
async def proxy_result_pareto_dash(
    session_id: str,
    access_token: str,
    request: Request,
    dash_path: str = "",
):
    if not validate_token(access_token):
        raise HTTPException(401, "Invalid or expired token")

    launched = _get_pareto_dash_session(session_id)
    if not launched:
        raise HTTPException(404, "Pareto Dash session not found")

    prefix = str(launched["pathname_prefix"])
    suffix = dash_path.lstrip("/")
    target_path = prefix if not suffix else f"{prefix}{suffix}"
    query_string = request.url.query
    target_url = f"http://127.0.0.1:{launched['port']}{target_path}"
    if query_string:
        target_url = f"{target_url}?{query_string}"

    forwarded_headers = {
        key: value
        for key, value in request.headers.items()
        if key.lower() not in _PARETO_DASH_PROXY_REQ_DROP
    }
    body = await request.body()

    try:
        async with httpx.AsyncClient(timeout=60.0, follow_redirects=False) as client:
            upstream = await client.request(
                request.method,
                target_url,
                headers=forwarded_headers,
                content=body,
            )
    except httpx.HTTPError as exc:
        raise HTTPException(502, f"Pareto Dash proxy request failed: {exc}") from exc

    proxy_root = str(
        request.app.url_path_for(
            "optimize_result_pareto_dash_proxy_root",
            session_id=session_id,
            access_token=access_token,
        )
    )
    upstream_base = f"http://127.0.0.1:{launched['port']}"
    response_headers = {
        key: value
        for key, value in upstream.headers.items()
        if key.lower() not in _PARETO_DASH_PROXY_RESP_DROP
    }
    location = response_headers.get("location")
    if location:
        if location.startswith(upstream_base):
            response_headers["location"] = location.replace(upstream_base, "", 1)
        elif location.startswith(prefix):
            response_headers["location"] = location.replace(prefix, proxy_root, 1)

    return Response(
        content=upstream.content,
        status_code=upstream.status_code,
        headers=response_headers,
    )


@router.delete("/results/pareto-dash/{session_id}")
def stop_result_pareto_dash(session_id: str, session: SessionToken = Depends(require_auth)):
    _stop_pareto_dash_session(session_id)
    return {"ok": True}


@router.get("/paretos")
def list_paretos(
    result_path: str,
    scenario: str = Query("Aggregated"),
    statistic: str = Query("mean"),
    session: SessionToken = Depends(require_auth),
):
    result_dir = _ensure_result_path(result_path)
    pareto_dir = result_dir / "pareto"
    if not pareto_dir.exists():
        return {
            "paretos": [],
            "meta": {
                "mode": "none",
                "has_suite_metrics": False,
                "scenario_labels": [],
                "available_statistics": list(_PARETO_STATISTICS),
                "selected_scenario": "Aggregated",
                "selected_statistic": _normalize_pareto_statistic(statistic),
                "statistic_enabled": True,
            },
        }
    selected_statistic = _normalize_pareto_statistic(statistic)
    selected_scenario = str(scenario or "Aggregated").strip() or "Aggregated"
    pareto_files = sorted(pareto_dir.glob("*.json"))
    meta = {
        "mode": "unknown",
        "has_suite_metrics": False,
        "scenario_labels": [],
        "available_statistics": list(_PARETO_STATISTICS),
        "selected_scenario": "Aggregated",
        "selected_statistic": selected_statistic,
        "statistic_enabled": True,
    }
    for path in pareto_files:
        try:
            meta = _pareto_meta_from_data(
                _load_pareto_json(path),
                selected_scenario=selected_scenario,
                selected_statistic=selected_statistic,
            )
            selected_scenario = meta["selected_scenario"]
            selected_statistic = meta["selected_statistic"]
            break
        except Exception as exc:
            _log(SERVICE, f"Error reading pareto meta {path}: {exc}", level="WARNING")
    paretos = []
    for path in pareto_files:
        try:
            data = _load_pareto_json(path)
            summary = _summary_from_pareto_data(
                data,
                statistic=selected_statistic,
                scenario=selected_scenario,
            )
            paretos.append(
                {
                    "path": str(path),
                    "name": path.stem,
                    "modified": datetime.datetime.fromtimestamp(path.stat().st_mtime).isoformat(),
                    "summary": summary,
                }
            )
        except Exception as exc:
            _log(SERVICE, f"Error reading pareto {path}: {exc}", level="WARNING")
    return {"paretos": paretos, "meta": meta}


@router.get("/paretos/file")
def get_pareto_file(path: str, session: SessionToken = Depends(require_auth)):
    pareto_path = _ensure_pareto_path(path)
    if not pareto_path.exists():
        raise HTTPException(404, "Pareto file not found")
    return _load_pareto_json(pareto_path)


@router.post("/paretos/seed-bundle")
def create_pareto_seed_bundle(body: dict, session: SessionToken = Depends(require_auth)):
    result_path = str(body.get("result_path") or "").strip()
    raw_paths = body.get("paths")
    if not result_path:
        raise HTTPException(400, "result_path is required")
    if not isinstance(raw_paths, list):
        raise HTTPException(400, "paths must be a list")

    result_dir = _ensure_result_path(result_path)
    pareto_dir = (result_dir / "pareto").resolve()
    selected_paths: list[Path] = []
    seen: set[str] = set()

    for raw_path in raw_paths:
        normalized = str(raw_path or "").strip()
        if not normalized or normalized in seen:
            continue
        pareto_path = _ensure_pareto_path(normalized)
        if not pareto_path.exists():
            raise HTTPException(404, f"Pareto file not found: {pareto_path}")
        if pareto_path.parent != pareto_dir:
            raise HTTPException(400, "All pareto files must belong to the selected result")
        selected_paths.append(pareto_path)
        seen.add(normalized)

    if not selected_paths:
        raise HTTPException(400, "No pareto files selected")
    if len(selected_paths) == 1:
        return {"path": str(selected_paths[0]), "count": 1}

    try:
        bundle_dir = _create_pareto_seed_bundle(result_dir, selected_paths)
        return {"path": str(bundle_dir), "count": len(selected_paths)}
    except Exception as exc:
        detail = str(exc).strip() or exc.__class__.__name__
        _log(
            SERVICE,
            f"Failed to create pareto seed bundle: {detail}",
            level="WARNING",
            meta={"traceback": traceback.format_exc()},
        )
        raise HTTPException(status_code=500, detail=f"Failed to create seed bundle: {detail}") from exc