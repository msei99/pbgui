"""Shared PB7 OHLCV readiness helpers for FastAPI editors.

Provides:
- read-only OHLCV readiness/preflight payloads based on PB7's v2 planner
- background preload jobs using PB7's existing OHLCV download tool
"""

from __future__ import annotations

import asyncio
import os
import platform
import subprocess
import threading
import time
import uuid
from collections import deque
from copy import deepcopy
from pathlib import Path
from typing import Any

from api.pb7_bridge import ensure_pb7_src_importable
from logging_helpers import human_log as _log
from pb7_config import prepare_pb7_config_dict, save_pb7_config, strip_pbgui_param_status
from pbgui_purefunc import PBGDIR, pb7dir, pb7venv

SERVICE = "PB7OhlcvAPI"

_PRELOAD_JOBS: dict[str, dict[str, Any]] = {}
_PRELOAD_LOCK = threading.Lock()
_PRELOAD_JOB_TTL_SECONDS = 24 * 60 * 60
_SAMPLE_LIMIT = 6

_STATUS_LABELS = {
    "store_complete": "Local v2 ready",
    "legacy_importable": "Local legacy import",
    "missing_local": "Will fetch on start",
    "blocked_by_persistent_gap": "Blocked by persistent gap",
    "missing_market": "Coin not on exchange",
    "coin_too_young": "Too young for window",
}

_STATUS_RANK = {
    "store_complete": 0,
    "legacy_importable": 1,
    "missing_local": 2,
    "blocked_by_persistent_gap": 3,
    "coin_too_young": 4,
    "missing_market": 5,
}

_STATUS_KEYS = tuple(_STATUS_LABELS.keys())


def _utc_ms() -> int:
    return int(time.time() * 1000)


def _iso_from_ms(ts_ms: int | None) -> str | None:
    if not ts_ms:
        return None
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(int(ts_ms) / 1000.0))


def _resolve_pb7_path(value: str | Path | None) -> Path | None:
    if value is None:
        return None
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = Path(pb7dir()) / path
    try:
        return path.resolve()
    except Exception:
        return path


def _status_counts_template() -> dict[str, int]:
    return {key: 0 for key in _STATUS_KEYS}


def _uses_all_coins(source: Any) -> bool:
    if isinstance(source, str):
        return source.strip().lower() == "all"
    if isinstance(source, (list, tuple)):
        return len(source) == 1 and str(source[0]).strip().lower() == "all"
    if isinstance(source, dict):
        return any(_uses_all_coins(source.get(side)) for side in ("long", "short"))
    return False


def _tail_text_lines(path: Path, limit: int = 40) -> list[str]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            return [line.rstrip("\n") for line in deque(handle, maxlen=max(1, int(limit)))]
    except Exception:
        return []


def _compact_entry(entry: dict[str, Any]) -> dict[str, Any]:
    compact = {
        "coin": entry.get("coin"),
        "exchange": entry.get("exchange"),
        "status": entry.get("status"),
        "status_label": entry.get("status_label"),
        "note": entry.get("note"),
    }
    if entry.get("sides"):
        compact["sides"] = list(entry["sides"])
    if entry.get("symbol"):
        compact["symbol"] = entry["symbol"]
    if entry.get("effective_start_date"):
        compact["effective_start_date"] = entry["effective_start_date"]
    if entry.get("catalog_bounds"):
        compact["catalog_bounds"] = entry["catalog_bounds"]
    if entry.get("persistent_gap"):
        compact["persistent_gap"] = entry["persistent_gap"]
    return compact


def _group_samples(entries: list[dict[str, Any]], limit: int = _SAMPLE_LIMIT) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {key: [] for key in _STATUS_KEYS}
    for entry in sorted(entries, key=lambda item: (item.get("coin") or "", item.get("exchange") or "")):
        status = str(entry.get("status") or "").strip()
        if status not in grouped:
            continue
        if len(grouped[status]) >= limit:
            continue
        grouped[status].append(_compact_entry(entry))
    return {key: value for key, value in grouped.items() if value}


def _build_summary(best_counts: dict[str, int], coin_count: int) -> dict[str, Any]:
    ready = int(best_counts.get("store_complete") or 0)
    legacy = int(best_counts.get("legacy_importable") or 0)
    fetch = int(best_counts.get("missing_local") or 0)
    blocked = int(best_counts.get("blocked_by_persistent_gap") or 0)
    missing_market = int(best_counts.get("missing_market") or 0)
    too_young = int(best_counts.get("coin_too_young") or 0)

    parts = []
    if ready:
        parts.append(f"{ready} ready locally")
    if legacy:
        parts.append(f"{legacy} can import from local legacy data")
    if fetch:
        parts.append(f"{fetch} would fetch on start")
    if blocked:
        parts.append(f"{blocked} blocked by persistent gaps")
    if missing_market:
        parts.append(f"{missing_market} not available on the selected exchange set")
    if too_young:
        parts.append(f"{too_young} too young for the requested window")

    if coin_count <= 0:
        overall_status = "empty"
        headline = "No approved coins resolved"
    elif ready == coin_count:
        overall_status = "ready"
        headline = "Local v2 data is ready"
    elif fetch > 0:
        overall_status = "preload"
        headline = "Some coins would fetch on start"
    elif blocked > 0 and ready == 0 and legacy == 0:
        overall_status = "blocked"
        headline = "Persistent gaps block local readiness"
    elif legacy > 0 and fetch == 0 and blocked == 0 and missing_market == 0 and too_young == 0:
        overall_status = "legacy"
        headline = "Local legacy data can satisfy the request"
    else:
        overall_status = "mixed"
        headline = "OHLCV readiness is mixed"

    return {
        "overall_status": overall_status,
        "headline": headline,
        "detail": ", ".join(parts) if parts else "No readiness data available.",
        "counts": dict(best_counts),
        "preload_supported": fetch > 0,
        "preload_label": "Preload missing OHLCV data" if fetch > 0 else "No preload needed",
        "preload_detail": (
            "Run PB7's download tool now so the later start does not need to fetch these ranges."
            if fetch > 0
            else "Nothing in the current best-per-coin view needs a remote preload."
        ),
    }


def _preload_job_payload(job: dict[str, Any]) -> dict[str, Any]:
    log_path = Path(job["log_path"])
    return {
        "job_id": job["job_id"],
        "status": job["status"],
        "started_at": job.get("started_at"),
        "started_at_iso": _iso_from_ms(job.get("started_at")),
        "finished_at": job.get("finished_at"),
        "finished_at_iso": _iso_from_ms(job.get("finished_at")),
        "pid": job.get("pid"),
        "returncode": job.get("returncode"),
        "error": job.get("error"),
        "log_path": str(log_path),
        "log_tail": _tail_text_lines(log_path, limit=40),
    }


def _cleanup_preload_jobs() -> None:
    cutoff = _utc_ms() - (_PRELOAD_JOB_TTL_SECONDS * 1000)
    with _PRELOAD_LOCK:
        stale_ids = []
        for job_id, job in _PRELOAD_JOBS.items():
            finished_at = int(job.get("finished_at") or 0)
            if finished_at and finished_at < cutoff:
                stale_ids.append(job_id)
        for job_id in stale_ids:
            _PRELOAD_JOBS.pop(job_id, None)


def _prepare_runtime_config(raw_config: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(raw_config, dict):
        raise ValueError("config must be a JSON object")
    config = prepare_pb7_config_dict(deepcopy(raw_config), neutralize_added=False)
    strip_pbgui_param_status(config)
    return config


def _write_preload_config(job_id: str, config: dict[str, Any]) -> Path:
    work_dir = Path(PBGDIR) / "data" / "ohlcv_preload"
    work_dir.mkdir(parents=True, exist_ok=True)
    config_path = work_dir / f"preload_{job_id}.json"
    save_pb7_config(config, config_path)
    return config_path


def _preload_command(config_path: Path) -> list[str]:
    return [
        pb7venv(),
        "-u",
        str(Path(pb7dir()) / "src" / "ohlcv_download.py"),
        str(config_path),
    ]


def _spawn_preload_worker(job_id: str) -> None:
    def runner() -> None:
        with _PRELOAD_LOCK:
            job = _PRELOAD_JOBS.get(job_id)
        if not job:
            return

        cmd = list(job["command"])
        log_path = Path(job["log_path"])
        cwd = Path(job["cwd"])
        env = os.environ.copy()
        env["PATH"] = os.path.dirname(pb7venv()) + os.pathsep + env.get("PATH", "")
        popen_kwargs: dict[str, Any] = {
            "cwd": str(cwd),
            "env": env,
            "stdout": subprocess.DEVNULL,
            "stderr": subprocess.DEVNULL,
        }

        try:
            log_path.parent.mkdir(parents=True, exist_ok=True)
            with log_path.open("a", encoding="utf-8") as handle:
                handle.write("$ " + " ".join(cmd) + "\n")
                handle.flush()
                popen_kwargs["stdout"] = handle
                popen_kwargs["stderr"] = subprocess.STDOUT
                if platform.system() == "Windows":
                    popen_kwargs["creationflags"] = (
                        subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS
                    )
                    proc = subprocess.Popen(cmd, **popen_kwargs)
                else:
                    proc = subprocess.Popen(cmd, start_new_session=True, **popen_kwargs)
                with _PRELOAD_LOCK:
                    if job_id in _PRELOAD_JOBS:
                        _PRELOAD_JOBS[job_id]["status"] = "running"
                        _PRELOAD_JOBS[job_id]["pid"] = proc.pid
                _log(SERVICE, f"Started OHLCV preload job {job_id} (pid={proc.pid})", level="INFO")
                returncode = proc.wait()
                finished_at = _utc_ms()
                with _PRELOAD_LOCK:
                    if job_id in _PRELOAD_JOBS:
                        _PRELOAD_JOBS[job_id]["returncode"] = returncode
                        _PRELOAD_JOBS[job_id]["finished_at"] = finished_at
                        _PRELOAD_JOBS[job_id]["status"] = "completed" if returncode == 0 else "error"
                if returncode == 0:
                    _log(SERVICE, f"Finished OHLCV preload job {job_id}", level="INFO")
                else:
                    _log(
                        SERVICE,
                        f"OHLCV preload job {job_id} failed with return code {returncode}",
                        level="WARNING",
                    )
        except Exception as exc:
            finished_at = _utc_ms()
            try:
                with log_path.open("a", encoding="utf-8") as handle:
                    handle.write(f"\nERROR: {type(exc).__name__}: {exc}\n")
            except Exception:
                pass
            with _PRELOAD_LOCK:
                if job_id in _PRELOAD_JOBS:
                    _PRELOAD_JOBS[job_id]["status"] = "error"
                    _PRELOAD_JOBS[job_id]["error"] = str(exc)
                    _PRELOAD_JOBS[job_id]["finished_at"] = finished_at
            _log(SERVICE, f"Failed to start OHLCV preload job {job_id}: {exc}", level="WARNING")

    threading.Thread(target=runner, daemon=True, name=f"ohlcv-preload-{job_id}").start()


async def build_ohlcv_preflight(raw_config: dict[str, Any]) -> dict[str, Any]:
    ensure_pb7_src_importable()
    from config.access import require_config_value, require_live_value
    from hlcv_preparation import HLCVManager
    from ohlcv_catalog import OhlcvCatalog
    from ohlcv_legacy_import import inspect_legacy_range
    from ohlcv_planner import plan_local_symbol_range
    from procedures import date_to_ts, ts_to_date
    from utils import format_approved_ignored_coins, format_end_date, to_ccxt_exchange_id, to_standard_exchange_name
    from warmup_utils import compute_backtest_warmup_minutes

    config = _prepare_runtime_config(raw_config)
    backtest = config.get("backtest", {}) if isinstance(config, dict) else {}
    live = config.get("live", {}) if isinstance(config, dict) else {}
    exchanges = list(require_config_value(config, "backtest.exchanges") or [])
    if not exchanges:
        raise ValueError("backtest.exchanges is empty")

    requested_start_date = str(require_config_value(config, "backtest.start_date"))
    end_date = format_end_date(require_config_value(config, "backtest.end_date"))
    requested_start_ts = int(date_to_ts(requested_start_date))
    end_ts = int(date_to_ts(end_date))
    warmup_minutes = max(0, int(compute_backtest_warmup_minutes(config)))
    minute_ms = 60_000
    effective_start_ts = max(0, requested_start_ts - (warmup_minutes * minute_ms))
    effective_start_ts = (effective_start_ts // minute_ms) * minute_ms
    minimum_coin_age_days = float(require_live_value(config, "minimum_coin_age_days"))
    min_coin_age_ms = int(max(0.0, minimum_coin_age_days) * 24.0 * 60.0 * 60.0 * 1000.0)
    source_dir_value = str((backtest.get("ohlcv_source_dir") or "")).strip()
    source_dir = _resolve_pb7_path(source_dir_value) if source_dir_value else None
    legacy_root = source_dir or _resolve_pb7_path(Path("caches") / "ohlcv")
    if legacy_root and not legacy_root.exists():
        legacy_root = None
    catalog_path = Path(pb7dir()) / "caches" / "ohlcvs" / "catalog.sqlite"
    catalog = OhlcvCatalog(catalog_path) if catalog_path.exists() else None

    await format_approved_ignored_coins(config, exchanges)
    approved = require_live_value(config, "approved_coins")
    coin_sides: dict[str, list[str]] = {}
    for side in ("long", "short"):
        values = approved.get(side, [])
        if isinstance(values, str):
            values = [values]
        for coin in values or []:
            coin_name = str(coin or "").strip()
            if not coin_name or coin_name == "all":
                continue
            sides = coin_sides.setdefault(coin_name, [])
            if side not in sides:
                sides.append(side)
    coins = sorted(coin_sides)
    if not coins:
        return {
            "summary": {
                "overall_status": "empty",
                "headline": "No approved coins resolved",
                "detail": "Select approved coins or apply filters before running the OHLCV check.",
                "counts": _status_counts_template(),
                "preload_supported": False,
                "preload_label": "No preload needed",
                "preload_detail": "There are no approved coins in the current config.",
            },
            "request": {
                "requested_start_date": requested_start_date,
                "effective_start_date": ts_to_date(int(effective_start_ts)),
                "end_date": end_date,
                "warmup_minutes": warmup_minutes,
                "minimum_coin_age_days": minimum_coin_age_days,
                "source_dir": str(source_dir) if source_dir else None,
                "catalog_path": str(catalog_path),
                "catalog_present": catalog is not None,
            },
            "universe": {
                "coin_count": 0,
                "coins_mode": "all" if _uses_all_coins(live.get("approved_coins")) else "explicit",
                "exchange_count": len(exchanges),
            },
            "best_samples": {},
            "exchanges": [],
            "notes": [
                "Preflight uses the current approved_coins selection from the editor.",
                "Approved coins are resolved from both long and short lists.",
            ],
        }

    cm_debug_level = int(backtest.get("cm_debug_level", 0) or 0)
    cm_progress_interval = float(backtest.get("cm_progress_log_interval_seconds", 10.0) or 10.0)
    gap_tolerance = require_config_value(config, "backtest.gap_tolerance_ohlcvs_minutes")

    entries_by_coin: dict[str, list[dict[str, Any]]] = {coin: [] for coin in coins}
    exchange_payloads: list[dict[str, Any]] = []

    for raw_exchange in exchanges:
        ccxt_exchange = to_ccxt_exchange_id(raw_exchange)
        store_exchange = to_standard_exchange_name(ccxt_exchange)
        om = HLCVManager(
            ccxt_exchange,
            ts_to_date(int(effective_start_ts)),
            end_date,
            gap_tolerance_ohlcvs_minutes=gap_tolerance,
            cm_debug_level=cm_debug_level,
            cm_progress_log_interval_seconds=cm_progress_interval,
            force_refetch_gaps=False,
            ohlcv_source_dir=source_dir_value or None,
        )
        try:
            await om.load_markets()
            exchange_entries: list[dict[str, Any]] = []
            counts = _status_counts_template()
            for coin in coins:
                entry: dict[str, Any] = {
                    "coin": coin,
                    "exchange": store_exchange,
                    "requested_exchange": str(raw_exchange),
                    "sides": list(coin_sides.get(coin, [])),
                    "effective_start_date": ts_to_date(int(effective_start_ts)),
                }
                if not om.has_coin(coin):
                    entry["status"] = "missing_market"
                    entry["status_label"] = _STATUS_LABELS["missing_market"]
                    entry["note"] = "Coin is not listed on this exchange."
                else:
                    first_ts_guess = om.load_first_timestamp(coin)
                    adjusted_start_ts = int(effective_start_ts)
                    if first_ts_guess:
                        adjusted_start_ts = max(adjusted_start_ts, int(float(first_ts_guess)) + min_coin_age_ms)
                        entry["first_cached_date"] = ts_to_date(int(float(first_ts_guess)))
                    symbol = om.get_symbol(coin)
                    entry["symbol"] = symbol
                    entry["effective_start_date"] = ts_to_date(int(adjusted_start_ts))
                    if adjusted_start_ts > end_ts:
                        entry["status"] = "coin_too_young"
                        entry["status_label"] = _STATUS_LABELS["coin_too_young"]
                        entry["note"] = "Minimum coin age pushes the usable start beyond the end date."
                    else:
                        if catalog is None:
                            legacy_inspection = None
                            if legacy_root is not None and legacy_root.exists():
                                legacy_inspection = inspect_legacy_range(
                                    legacy_root=legacy_root,
                                    exchange=store_exchange,
                                    timeframe="1m",
                                    symbol=symbol,
                                    start_ts=int(adjusted_start_ts),
                                    end_ts=int(end_ts),
                                )
                            if legacy_inspection is not None and legacy_inspection.all_days_present:
                                entry["status"] = "legacy_importable"
                                entry["status_label"] = _STATUS_LABELS["legacy_importable"]
                                entry["note"] = "All requested days exist in the configured legacy source."
                            else:
                                entry["status"] = "missing_local"
                                entry["status_label"] = _STATUS_LABELS["missing_local"]
                                entry["note"] = "No local v2 catalog is available for this range."
                        else:
                            plan = plan_local_symbol_range(
                                catalog=catalog,
                                legacy_root=legacy_root,
                                exchange=store_exchange,
                                timeframe="1m",
                                symbol=symbol,
                                start_ts=int(adjusted_start_ts),
                                end_ts=int(end_ts),
                            )
                            entry["status"] = plan.status
                            entry["status_label"] = _STATUS_LABELS.get(plan.status, plan.status)
                            if plan.bounds[0] is not None or plan.bounds[1] is not None:
                                entry["catalog_bounds"] = {
                                    "first": ts_to_date(int(plan.bounds[0])) if plan.bounds[0] is not None else None,
                                    "last": ts_to_date(int(plan.bounds[1])) if plan.bounds[1] is not None else None,
                                }
                            if plan.legacy_inspection is not None:
                                entry["legacy_days_present"] = len(plan.legacy_inspection.present_days)
                            if plan.persistent_gaps:
                                gap = plan.persistent_gaps[0]
                                entry["persistent_gap"] = {
                                    "start": ts_to_date(int(gap.start_ts)),
                                    "end": ts_to_date(int(gap.end_ts)),
                                    "reason": gap.reason,
                                    "retry_count": int(gap.retry_count or 0),
                                }
                            if plan.status == "store_complete":
                                entry["note"] = "PB7 can use the local v2 store without fetching."
                            elif plan.status == "legacy_importable":
                                entry["note"] = "PB7 can import this range from the configured legacy source on start."
                            elif plan.status == "missing_local":
                                entry["note"] = "PB7 would fetch this range when the run starts."
                            elif plan.status == "blocked_by_persistent_gap":
                                if entry.get("persistent_gap"):
                                    gap_info = entry["persistent_gap"]
                                    entry["note"] = (
                                        f"Persistent gap {gap_info['start']} -> {gap_info['end']} "
                                        f"({gap_info['reason']})."
                                    )
                                else:
                                    entry["note"] = "Persistent gaps block the local range."

                counts[entry["status"]] += 1
                exchange_entries.append(entry)
                entries_by_coin.setdefault(coin, []).append(entry)

            exchange_payloads.append(
                {
                    "exchange": store_exchange,
                    "input_exchange": str(raw_exchange),
                    "counts": counts,
                    "samples": _group_samples(exchange_entries),
                }
            )
        finally:
            await om.aclose()
            if om.cc:
                await om.cc.close()

    best_entries: list[dict[str, Any]] = []
    best_counts = _status_counts_template()
    for coin in coins:
        coin_entries = entries_by_coin.get(coin, [])
        if not coin_entries:
            continue
        best_entry = min(
            coin_entries,
            key=lambda item: (_STATUS_RANK.get(str(item.get("status") or ""), 999), str(item.get("exchange") or "")),
        )
        best_entries.append(best_entry)
        best_counts[best_entry["status"]] += 1

    notes = [
        "Preflight is read-only. It shows what PB7 would need before a run can start cleanly.",
        "Approved coins are resolved from both long and short lists.",
    ]
    if len(exchanges) > 1:
        notes.append(
            "Multiple exchanges are evaluated per coin and the summary uses the best available exchange result."
        )
    if legacy_root is not None:
        notes.append(f"Legacy source checked: {legacy_root}")
    if bool(backtest.get("suite_enabled")) and backtest.get("scenarios"):
        notes.append("Suite mode is enabled; this check only reflects the current base editor config.")

    return {
        "summary": _build_summary(best_counts, len(best_entries)),
        "request": {
            "requested_start_date": requested_start_date,
            "effective_start_date": ts_to_date(int(effective_start_ts)),
            "end_date": end_date,
            "warmup_minutes": warmup_minutes,
            "minimum_coin_age_days": minimum_coin_age_days,
            "source_dir": str(source_dir) if source_dir else None,
            "catalog_path": str(catalog_path),
            "catalog_present": catalog is not None,
        },
        "universe": {
            "coin_count": len(best_entries),
            "coins_mode": "all" if _uses_all_coins(live.get("approved_coins")) else "explicit",
            "exchange_count": len(exchanges),
        },
        "best_samples": _group_samples(best_entries),
        "exchanges": exchange_payloads,
        "notes": notes,
    }


def start_ohlcv_preload_job(raw_config: dict[str, Any]) -> dict[str, Any]:
    _cleanup_preload_jobs()
    config = _prepare_runtime_config(raw_config)
    job_id = uuid.uuid4().hex[:12]
    config_path = _write_preload_config(job_id, config)
    log_path = Path(PBGDIR) / "data" / "logs" / f"ohlcv_preload_{job_id}.log"
    job = {
        "job_id": job_id,
        "status": "queued",
        "started_at": _utc_ms(),
        "finished_at": None,
        "pid": None,
        "returncode": None,
        "error": None,
        "command": _preload_command(config_path),
        "cwd": str(Path(pb7dir())),
        "config_path": str(config_path),
        "log_path": str(log_path),
    }
    with _PRELOAD_LOCK:
        _PRELOAD_JOBS[job_id] = job
    _spawn_preload_worker(job_id)
    return _preload_job_payload(job)


def get_ohlcv_preload_job(job_id: str) -> dict[str, Any] | None:
    _cleanup_preload_jobs()
    with _PRELOAD_LOCK:
        job = deepcopy(_PRELOAD_JOBS.get(job_id))
    if not job:
        return None
    return _preload_job_payload(job)