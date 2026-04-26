"""Shared PB7 OHLCV readiness helpers for FastAPI editors.

Provides:
- read-only OHLCV readiness/preflight payloads based on PB7's v2 planner
- background preload jobs using PB7's existing OHLCV download tool
"""

from __future__ import annotations

import asyncio
import os
import platform
import re
import signal
import subprocess
import threading
import time
import uuid
from collections import deque
from copy import deepcopy
from datetime import datetime, timezone
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

_LOG_LINE_RE = re.compile(r"^(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\s+(?P<level>[A-Z]+)\s+(?P<msg>.*)$")
_ARCHIVE_START_RE = re.compile(
    r"^\[(?P<exchange>[^\]]+)\] download archive start symbol=(?P<symbol>\S+) days=(?P<days>\d+) parallel=(?P<parallel>\d+) range=(?P<range>\S+)$"
)
_ARCHIVE_PROGRESS_RE = re.compile(
    r"^\[(?P<exchange>[^\]]+)\] download archive progress symbol=(?P<symbol>\S+) (?P<completed>\d+)/(?P<total>\d+) \((?P<pct>\d+)%\) batch=(?P<batch>.+)$"
)
_ARCHIVE_DONE_RE = re.compile(
    r"^\[(?P<exchange>[^\]]+)\] download archive done symbol=(?P<symbol>\S+) fetched=(?P<fetched>\d+) skipped=(?P<skipped>\d+) total=(?P<total>\d+) elapsed_s=(?P<elapsed_s>\S+)$"
)
_CCXT_START_RE = re.compile(
    r"^\[(?P<exchange>[^\]]+)\] download ccxt start symbol=(?P<symbol>\S+) tf=(?P<tf>\S+) since=(?P<since>\S+) since_ms=(?P<since_ms>\d+) limit=(?P<limit>\d+) params=(?P<params>.+)$"
)
_CCXT_OK_RE = re.compile(
    r"^\[(?P<exchange>[^\]]+)\] download ccxt ok symbol=(?P<symbol>\S+) tf=(?P<tf>\S+) rows=(?P<rows>\d+) first=(?P<first>\S+) last=(?P<last>\S+) elapsed_ms=(?P<elapsed_ms>\d+)"
)
_PCT_PROGRESS_RE = re.compile(
    r"^(?P<pct>\d+)%\s*\|\s*(?P<context>.+?)\s+(?P<processed>\d+)/(?P<total>\d+)(?:\s+current=(?P<current>\S+))?(?:\s+ETA\s+(?P<eta>\d+)s)?$"
)


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


def _parse_log_iso_to_ms(value: str | None) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        if text.isdigit():
            return int(text)
        if len(text) == 10 and text[4] == "-" and text[7] == "-":
            return int(datetime.strptime(text, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.timestamp() * 1000)
    except Exception:
        return None


def _ms_to_iso(value: int | None) -> str | None:
    if value is None:
        return None
    try:
        return datetime.fromtimestamp(int(value) / 1000.0, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return None


def _safe_pct(value: float | int | None) -> int | None:
    if value is None:
        return None
    try:
        return max(0, min(100, int(round(float(value)))))
    except Exception:
        return None


def _coerce_ts_ms(value: Any) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = int(float(text))
    except Exception:
        return None
    return parsed if parsed > 0 else None


def _extract_market_start_ts(market: dict[str, Any] | None) -> int | None:
    if not isinstance(market, dict):
        return None

    candidates: list[Any] = [market.get("created")]
    info = market.get("info")
    if isinstance(info, dict):
        candidates.extend(
            [
                info.get("launchTime"),
                info.get("onboardDate"),
                info.get("launch_time"),
                info.get("listingTime"),
                info.get("createTime"),
            ]
        )

    values = [ts_ms for ts_ms in (_coerce_ts_ms(candidate) for candidate in candidates) if ts_ms is not None]
    return min(values) if values else None


def _collect_coin_sides(approved: Any) -> dict[str, list[str]]:
    coin_sides: dict[str, list[str]] = {}
    if not isinstance(approved, dict):
        return coin_sides
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
    return coin_sides


async def _prune_preload_config(config: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    ensure_pb7_src_importable()
    from config.access import require_config_value, require_live_value
    from hlcv_preparation import HLCVManager
    from procedures import date_to_ts, ts_to_date
    from utils import format_approved_ignored_coins, format_end_date, to_ccxt_exchange_id
    from warmup_utils import compute_backtest_warmup_minutes

    filtered = deepcopy(config)
    backtest = filtered.get("backtest", {}) if isinstance(filtered, dict) else {}
    live = filtered.get("live", {}) if isinstance(filtered, dict) else {}
    exchanges = list(require_config_value(filtered, "backtest.exchanges") or [])
    if not exchanges:
        raise ValueError("backtest.exchanges is empty")

    requested_start_date = str(require_config_value(filtered, "backtest.start_date"))
    end_date = format_end_date(require_config_value(filtered, "backtest.end_date"))
    requested_start_ts = int(date_to_ts(requested_start_date))
    end_ts = int(date_to_ts(end_date))
    warmup_minutes = max(0, int(compute_backtest_warmup_minutes(filtered)))
    effective_start_ts = max(0, requested_start_ts - (warmup_minutes * 60_000))
    effective_start_ts = (effective_start_ts // 60_000) * 60_000
    backtest["start_date"] = ts_to_date(int(effective_start_ts))
    minimum_coin_age_days = float(require_live_value(filtered, "minimum_coin_age_days"))
    min_coin_age_ms = int(max(0.0, minimum_coin_age_days) * 24.0 * 60.0 * 60.0 * 1000.0)

    await format_approved_ignored_coins(filtered, exchanges)
    approved = require_live_value(filtered, "approved_coins")
    coin_sides = _collect_coin_sides(approved)
    coins = sorted(coin_sides)
    if not coins:
        return filtered, []

    source_dir_value = str((backtest.get("ohlcv_source_dir") or "")).strip()
    gap_tolerance = require_config_value(filtered, "backtest.gap_tolerance_ohlcvs_minutes")
    cm_debug_level = int(backtest.get("cm_debug_level", 0) or 0)
    cm_progress_interval = float(backtest.get("cm_progress_log_interval_seconds", 10.0) or 10.0)

    viable_coins = set(coins)
    skipped_reasons: dict[str, str] = {}
    market_seen: dict[str, bool] = {coin: False for coin in coins}
    serviceable_seen: dict[str, bool] = {coin: False for coin in coins}
    unknown_seen: dict[str, bool] = {coin: False for coin in coins}

    for raw_exchange in exchanges:
        ccxt_exchange = to_ccxt_exchange_id(raw_exchange)
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
            for coin in coins:
                if not om.has_coin(coin):
                    continue
                market_seen[coin] = True
                symbol = om.get_symbol(coin)
                market = (om.markets or {}).get(symbol) if isinstance(om.markets, dict) else None
                market_start_ts = _extract_market_start_ts(market)
                if market_start_ts is None:
                    unknown_seen[coin] = True
                    serviceable_seen[coin] = True
                    continue
                usable_start_ts = int(market_start_ts) + min_coin_age_ms
                if usable_start_ts <= end_ts:
                    serviceable_seen[coin] = True
                    continue
                skipped_reasons[coin] = (
                    f"{coin} starts trading at {_ms_to_iso(market_start_ts) or 'n/a'} on {raw_exchange}, after the requested preload window."
                )
        finally:
            await om.aclose()
            if om.cc:
                await om.cc.close()

    for coin in coins:
        if serviceable_seen.get(coin) or unknown_seen.get(coin):
            continue
        if market_seen.get(coin):
            viable_coins.discard(coin)
        else:
            skipped_reasons.setdefault(coin, f"{coin} is not listed on the selected exchanges.")
            viable_coins.discard(coin)

    if viable_coins == set(coins):
        return filtered, []

    filtered_live = filtered.setdefault("live", {})
    filtered_approved = filtered_live.setdefault("approved_coins", {})
    for side in ("long", "short"):
        side_values = approved.get(side, []) if isinstance(approved, dict) else []
        if isinstance(side_values, str):
            side_values = [side_values]
        filtered_approved[side] = [coin for coin in side_values if str(coin or "").strip() in viable_coins]

    skipped_notes = [skipped_reasons[coin] for coin in coins if coin not in viable_coins and coin in skipped_reasons]
    if not viable_coins:
        raise ValueError(skipped_notes[0] if skipped_notes else "No approved coins remain preloadable for the requested window.")
    return filtered, skipped_notes


def _derive_target_end_ms_from_config(config: dict[str, Any]) -> int | None:
    try:
        ensure_pb7_src_importable()
        from procedures import date_to_ts
        from utils import format_end_date

        return int(date_to_ts(format_end_date((config.get("backtest", {}) or {}).get("end_date"))))
    except Exception:
        return None


def _extract_preload_config_path(log_lines: list[str]) -> Path | None:
    for raw_line in log_lines[:8]:
        line = str(raw_line or "").strip()
        if not line:
            continue
        if " loading config " in line:
            config_path = line.split(" loading config ", 1)[1].strip()
            if config_path:
                return Path(config_path)
        if line.startswith("$ "):
            parts = line.split()
            if parts:
                candidate = parts[-1].strip()
                if candidate.endswith(".json"):
                    return Path(candidate)
    return None


def _derive_target_end_ms_from_log(log_lines: list[str]) -> int | None:
    config_path = _extract_preload_config_path(log_lines)
    if not config_path or not config_path.exists():
        return None
    try:
        import json

        with config_path.open("r", encoding="utf-8") as handle:
            config = json.load(handle)
        if not isinstance(config, dict):
            return None
        return _derive_target_end_ms_from_config(config)
    except Exception:
        return None


def _parse_log_date_range(value: str | None) -> tuple[int | None, int | None]:
    text = str(value or "").strip()
    if not text or ".." not in text:
        return None, None
    start_text, end_text = text.split("..", 1)
    return _parse_log_iso_to_ms(start_text), _parse_log_iso_to_ms(end_text)


def _derive_archive_progress(
    *,
    total: int | None,
    range_text: str | None,
    batch_text: str | None,
    completed: int | None,
    pct: int | None,
) -> tuple[int | None, int | None]:
    safe_total = int(total) if total is not None else None
    display_completed = completed
    display_pct = pct
    if not safe_total or safe_total <= 0:
        return display_completed, display_pct

    range_start_ms, _range_end_ms = _parse_log_date_range(range_text)
    _batch_start_ms, batch_end_ms = _parse_log_date_range(batch_text)
    if range_start_ms is None or batch_end_ms is None or batch_end_ms < range_start_ms:
        return display_completed, display_pct

    processed_days = int(((batch_end_ms - range_start_ms) / 86_400_000)) + 1
    processed_days = max(0, min(safe_total, processed_days))
    display_completed = max(int(display_completed or 0), processed_days)
    derived_pct = _safe_pct((processed_days / safe_total) * 100.0)
    if derived_pct is not None:
        display_pct = max(int(display_pct or 0), derived_pct)
    return display_completed, display_pct


def _derive_ccxt_cursor_progress(
    *,
    first_request_ms: int | None,
    cursor_ms: int | None,
    target_end_ms: int | None,
) -> int | None:
    if first_request_ms is None or cursor_ms is None or target_end_ms is None:
        return None
    if target_end_ms <= first_request_ms:
        return 99
    clamped_cursor_ms = max(first_request_ms, min(target_end_ms, cursor_ms))
    derived_pct = _safe_pct(((clamped_cursor_ms - first_request_ms) / (target_end_ms - first_request_ms)) * 100.0)
    if derived_pct is None:
        return None
    # Keep running CCXT tasks below 100%. The job completion state is the real terminal signal.
    return min(99, derived_pct)


def _build_preload_progress(log_lines: list[str], *, target_end_ms: int | None = None) -> dict[str, Any]:
    task_map: dict[tuple[str, str], dict[str, Any]] = {}
    tracker_summary: dict[str, Any] | None = None

    def get_task(exchange: str, symbol: str) -> dict[str, Any]:
        key = (str(exchange or "").strip(), str(symbol or "").strip())
        task = task_map.get(key)
        if task is None:
            task = {
                "exchange": key[0],
                "symbol": key[1],
                "kind": None,
                "status": "running",
                "pct": None,
                "completed": None,
                "total": None,
                "batch": None,
                "range": None,
                "detail": None,
                "first_request_ms": None,
                "cursor_ms": None,
                "cursor_iso": None,
                "since_ms": None,
                "since_iso": None,
                "response_first_ms": None,
                "response_first_iso": None,
                "response_ignored_cursor": False,
                "last_ms": None,
                "last_iso": None,
                "limit": None,
                "updated_at": None,
            }
            task_map[key] = task
        return task

    for raw_line in log_lines:
        line = str(raw_line or "").rstrip("\n")
        if not line:
            continue
        ts_match = _LOG_LINE_RE.match(line)
        line_ts = None
        message = line
        if ts_match:
            line_ts = _parse_log_iso_to_ms(ts_match.group("ts") + "Z")
            message = ts_match.group("msg").strip()

        tracker_match = _PCT_PROGRESS_RE.match(message.strip())
        if tracker_match:
            tracker_summary = {
                "pct": _safe_pct(tracker_match.group("pct")),
                "context": tracker_match.group("context"),
                "processed": int(tracker_match.group("processed")),
                "total": int(tracker_match.group("total")),
                "current": tracker_match.group("current"),
                "eta_seconds": int(tracker_match.group("eta")) if tracker_match.group("eta") else None,
            }

        match = _ARCHIVE_START_RE.match(message)
        if match:
            task = get_task(match.group("exchange"), match.group("symbol"))
            task["kind"] = "archive"
            task["status"] = "running"
            task["completed"] = 0
            task["total"] = int(match.group("days"))
            task["pct"] = 0
            task["range"] = match.group("range")
            task["detail"] = f"Archive 0/{task['total']}"
            task["updated_at"] = line_ts
            continue

        match = _ARCHIVE_PROGRESS_RE.match(message)
        if match:
            task = get_task(match.group("exchange"), match.group("symbol"))
            completed = int(match.group("completed"))
            total = int(match.group("total"))
            pct = _safe_pct(match.group("pct"))
            batch = match.group("batch")
            completed, pct = _derive_archive_progress(
                total=total,
                range_text=task.get("range"),
                batch_text=batch,
                completed=completed,
                pct=pct,
            )
            task["kind"] = "archive"
            task["status"] = "running"
            task["completed"] = completed
            task["total"] = total
            task["pct"] = pct
            task["batch"] = batch
            task["detail"] = f"Archive {completed}/{total}"
            task["updated_at"] = line_ts
            continue

        match = _ARCHIVE_DONE_RE.match(message)
        if match:
            task = get_task(match.group("exchange"), match.group("symbol"))
            fetched = int(match.group("fetched"))
            skipped = int(match.group("skipped"))
            total = int(match.group("total"))
            task["kind"] = "archive"
            task["status"] = "done"
            task["completed"] = fetched + skipped
            task["total"] = total
            task["pct"] = 100
            task["detail"] = f"Archive {fetched + skipped}/{total}"
            task["updated_at"] = line_ts
            continue

        match = _CCXT_START_RE.match(message)
        if match:
            task = get_task(match.group("exchange"), match.group("symbol"))
            since_ms = int(match.group("since_ms"))
            task["kind"] = "ccxt"
            task["status"] = "running"
            task["since_ms"] = since_ms
            task["since_iso"] = match.group("since")
            task["cursor_ms"] = since_ms
            task["cursor_iso"] = match.group("since")
            if task.get("first_request_ms") is None:
                task["first_request_ms"] = since_ms
            else:
                task["first_request_ms"] = min(int(task["first_request_ms"]), since_ms)
            task["limit"] = int(match.group("limit"))
            cursor_pct = _derive_ccxt_cursor_progress(
                first_request_ms=task.get("first_request_ms"),
                cursor_ms=task.get("cursor_ms"),
                target_end_ms=target_end_ms,
            )
            if cursor_pct is not None:
                task["pct"] = max(int(task.get("pct") or 0), cursor_pct)
            task["detail"] = f"Cursor {task['cursor_iso']}"
            task["updated_at"] = line_ts
            continue

        match = _CCXT_OK_RE.match(message)
        if match:
            task = get_task(match.group("exchange"), match.group("symbol"))
            first_ms = _parse_log_iso_to_ms(match.group("first"))
            last_ms = _parse_log_iso_to_ms(match.group("last"))
            cursor_ms = task.get("cursor_ms")
            response_ignored_cursor = bool(
                first_ms is not None and cursor_ms is not None and first_ms > (cursor_ms + 60_000)
            )
            pct = task.get("pct")
            if not response_ignored_cursor:
                coverage_pct = _derive_ccxt_cursor_progress(
                    first_request_ms=task.get("first_request_ms"),
                    cursor_ms=last_ms,
                    target_end_ms=target_end_ms,
                )
                if coverage_pct is not None:
                    pct = max(int(pct or 0), coverage_pct)
            task["kind"] = "ccxt"
            task["response_first_ms"] = first_ms
            task["response_first_iso"] = match.group("first")
            task["response_ignored_cursor"] = response_ignored_cursor
            task["last_ms"] = last_ms
            task["last_iso"] = match.group("last")
            task["pct"] = pct
            if response_ignored_cursor:
                task["detail"] = f"Cursor {task.get('cursor_iso') or task.get('since_iso') or 'n/a'}"
            else:
                task["detail"] = f"Fetched through {task['last_iso']}"
            task["updated_at"] = line_ts
            continue

    tasks = list(task_map.values())
    tasks.sort(key=lambda item: (item.get("status") != "running", -(item.get("updated_at") or 0)))
    active_tasks = [task for task in tasks if task.get("status") == "running"]
    finished_tasks = [task for task in tasks if task.get("status") == "done"]
    current_task = active_tasks[0] if active_tasks else (tasks[0] if tasks else None)

    return {
        "tracker": tracker_summary,
        "observed_tasks": len(tasks),
        "active_tasks": len(active_tasks),
        "finished_tasks": len(finished_tasks),
        "current_task": current_task,
        "tasks": tasks[:6],
    }


def _read_preload_log_state(path: Path, limit: int = 40, *, target_end_ms: int | None = None) -> dict[str, Any]:
    empty_state = {
        "tail": [],
        "line_count": 0,
        "last_line": None,
        "updated_at": None,
        "size_bytes": 0,
        "progress": {
            "tracker": None,
            "observed_tasks": 0,
            "active_tasks": 0,
            "finished_tasks": 0,
            "current_task": None,
            "tasks": [],
        },
    }
    if not path.exists():
        return empty_state
    try:
        tail = deque(maxlen=max(1, int(limit)))
        all_lines: list[str] = []
        line_count = 0
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            for raw_line in handle:
                line = raw_line.rstrip("\n")
                tail.append(line)
                all_lines.append(line)
                line_count += 1
        if target_end_ms is None:
            target_end_ms = _derive_target_end_ms_from_log(all_lines)
        stat = path.stat()
        last_line = next((line for line in reversed(tail) if str(line).strip()), None)
        return {
            "tail": list(tail),
            "line_count": line_count,
            "last_line": last_line,
            "updated_at": int(stat.st_mtime * 1000),
            "size_bytes": int(stat.st_size),
            "progress": _build_preload_progress(all_lines, target_end_ms=target_end_ms),
        }
    except Exception:
        return empty_state


def _finalize_preload_progress(progress: dict[str, Any], job_status: str) -> dict[str, Any]:
    if not isinstance(progress, dict) or job_status not in {"completed", "stopped", "error"}:
        return progress
    tasks = [dict(task) for task in (progress.get("tasks") or []) if isinstance(task, dict)]
    if not tasks:
        return progress

    for task in tasks:
        if task.get("status") != "running":
            continue
        if job_status == "completed":
            task["status"] = "done"
            task["pct"] = 100
            if task.get("total") is not None and task.get("completed") is None:
                task["completed"] = task.get("total")
        else:
            task["status"] = job_status

    finalized = dict(progress)
    finalized["tasks"] = tasks
    finalized["active_tasks"] = sum(1 for task in tasks if task.get("status") == "running")
    finalized["finished_tasks"] = sum(1 for task in tasks if task.get("status") == "done")
    finalized["current_task"] = tasks[0] if tasks else None
    return finalized


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
    elif too_young == coin_count:
        overall_status = "too_young"
        headline = "Selected coins start after the requested window"
    elif missing_market == coin_count:
        overall_status = "missing_market"
        headline = "Approved coins are not on the selected exchanges"
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
    log_state = _read_preload_log_state(log_path, limit=40, target_end_ms=job.get("target_end_ms"))
    progress = _finalize_preload_progress(log_state["progress"], str(job.get("status") or ""))
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
        "target_end_ms": job.get("target_end_ms"),
        "target_end_iso": _ms_to_iso(job.get("target_end_ms")),
        "log_tail": log_state["tail"],
        "log_line_count": log_state["line_count"],
        "last_log_line": log_state["last_line"],
        "log_updated_at": log_state["updated_at"],
        "log_updated_at_iso": _iso_from_ms(log_state["updated_at"]),
        "log_size_bytes": log_state["size_bytes"],
        "progress": progress,
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
        if job.get("stop_requested"):
            with _PRELOAD_LOCK:
                if job_id in _PRELOAD_JOBS:
                    _PRELOAD_JOBS[job_id]["status"] = "stopped"
                    _PRELOAD_JOBS[job_id]["finished_at"] = _utc_ms()
                    _PRELOAD_JOBS[job_id]["returncode"] = -1
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
                with _PRELOAD_LOCK:
                    current = _PRELOAD_JOBS.get(job_id)
                    if not current:
                        return
                    if current.get("stop_requested"):
                        _PRELOAD_JOBS[job_id]["status"] = "stopped"
                        _PRELOAD_JOBS[job_id]["finished_at"] = _utc_ms()
                        _PRELOAD_JOBS[job_id]["returncode"] = -1
                        return
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
                stop_requested = False
                with _PRELOAD_LOCK:
                    if job_id in _PRELOAD_JOBS:
                        stop_requested = bool(_PRELOAD_JOBS[job_id].get("stop_requested"))
                        _PRELOAD_JOBS[job_id]["returncode"] = returncode
                        _PRELOAD_JOBS[job_id]["finished_at"] = finished_at
                        _PRELOAD_JOBS[job_id]["status"] = "stopped" if stop_requested else ("completed" if returncode == 0 else "error")
                if stop_requested:
                    _log(SERVICE, f"Stopped OHLCV preload job {job_id}", level="INFO")
                elif returncode == 0:
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
    coin_sides = _collect_coin_sides(approved)
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
                    symbol = om.get_symbol(coin)
                    market = (om.markets or {}).get(symbol) if isinstance(om.markets, dict) else None
                    market_start_ts = _extract_market_start_ts(market)
                    if market_start_ts is not None:
                        entry["market_start_date"] = ts_to_date(int(market_start_ts))
                    first_ts_guess = om.load_first_timestamp(coin)
                    adjusted_start_ts = int(effective_start_ts)
                    age_anchor_ts = int(market_start_ts) if market_start_ts is not None else None
                    if age_anchor_ts is None and first_ts_guess:
                        age_anchor_ts = int(float(first_ts_guess))
                    if first_ts_guess:
                        entry["first_cached_date"] = ts_to_date(int(float(first_ts_guess)))
                    if age_anchor_ts is not None:
                        adjusted_start_ts = max(adjusted_start_ts, age_anchor_ts + min_coin_age_ms)
                    entry["symbol"] = symbol
                    entry["effective_start_date"] = ts_to_date(int(adjusted_start_ts))
                    if adjusted_start_ts > end_ts:
                        entry["status"] = "coin_too_young"
                        entry["status_label"] = _STATUS_LABELS["coin_too_young"]
                        if market_start_ts is not None and int(market_start_ts) > end_ts:
                            entry["note"] = (
                                f"Market starts at {ts_to_date(int(market_start_ts))}, after the requested window."
                            )
                        elif market_start_ts is not None and min_coin_age_ms > 0:
                            entry["note"] = (
                                f"Market starts at {ts_to_date(int(market_start_ts))}; minimum coin age pushes the usable start beyond the end date."
                            )
                        else:
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
    config, skipped_notes = asyncio.run(_prune_preload_config(config))
    target_end_ms = _derive_target_end_ms_from_config(config)
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
        "target_end_ms": target_end_ms,
        "skipped_notes": skipped_notes,
    }
    with _PRELOAD_LOCK:
        _PRELOAD_JOBS[job_id] = job
    _spawn_preload_worker(job_id)
    return _preload_job_payload(job)


def stop_ohlcv_preload_job(job_id: str) -> dict[str, Any] | None:
    _cleanup_preload_jobs()
    with _PRELOAD_LOCK:
        job = _PRELOAD_JOBS.get(job_id)
        if not job:
            return None
        job["stop_requested"] = True
        pid = job.get("pid")
        status = str(job.get("status") or "")
        if status == "queued" and not pid:
            job["status"] = "stopped"
            job["finished_at"] = _utc_ms()
            job["returncode"] = -1
            return _preload_job_payload(deepcopy(job))

    if status not in ("queued", "running") or not pid:
        return get_ohlcv_preload_job(job_id)

    try:
        if platform.system() == "Windows":
            os.kill(pid, signal.SIGTERM)
        else:
            os.killpg(pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    except Exception as exc:
        _log(SERVICE, f"Error stopping OHLCV preload job {job_id}: {exc}", level="WARNING")

    deadline = time.time() + 2.0
    while time.time() < deadline:
        with _PRELOAD_LOCK:
            current = _PRELOAD_JOBS.get(job_id)
            if not current or current.get("status") == "stopped":
                break
        try:
            os.kill(pid, 0)
        except OSError:
            break
        time.sleep(0.1)
    else:
        try:
            if platform.system() == "Windows":
                os.kill(pid, signal.SIGKILL)
            else:
                os.killpg(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        except Exception as exc:
            _log(SERVICE, f"Error force-stopping OHLCV preload job {job_id}: {exc}", level="WARNING")

    with _PRELOAD_LOCK:
        current = _PRELOAD_JOBS.get(job_id)
        if not current:
            return None
        if current.get("status") in ("queued", "running"):
            current["status"] = "stopped"
            current["finished_at"] = current.get("finished_at") or _utc_ms()
            current["returncode"] = -1
        payload = _preload_job_payload(deepcopy(current))
    return payload


def get_ohlcv_preload_job(job_id: str) -> dict[str, Any] | None:
    _cleanup_preload_jobs()
    with _PRELOAD_LOCK:
        job = deepcopy(_PRELOAD_JOBS.get(job_id))
    if not job:
        return None
    return _preload_job_payload(job)