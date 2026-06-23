"""
api/backtest_v7.py — FastAPI backend for V7 Backtest management.

Provides:
- REST endpoints for backtest configs, queue, results, archives, settings
- WebSocket endpoint for real-time queue status updates
- Background worker (asyncio) that processes queued backtests
"""

import asyncio
import configparser
import csv
import datetime
import glob
import gzip
import io
import json
import multiprocessing
import os
import platform
import secrets
import shutil
import subprocess
import time
import traceback
import uuid
from pathlib import Path, PurePath
from shutil import rmtree
from typing import Any, Optional

import psutil
from fastapi import APIRouter, Depends, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse

from api.archive_helpers import (
    atomic_write_json,
    archive_migration_status,
    build_archive_score_payload,
    cleanup_empty_parents,
    copy_backtest_result_to_archive,
    detect_liquidation,
    ensure_config_version,
    is_inside_archive,
    list_archive_backtest_results,
    list_archive_optimize_configs,
    load_archive_manifest,
    load_archive_readme_config,
    load_json_file,
    maybe_migrate_own_archive,
    migrate_archive_layout,
    save_archive_readme_config,
    rebuild_archive_manifest,
    remove_duplicate_results,
    remove_liquidated_results,
    resolve_optimize_archive_destination,
    safe_path_part,
    score_archive_results,
    update_archive_readme,
    update_archive_scores_and_readme,
    utc_now_iso,
    write_optimize_meta,
)
from api.auth import SessionToken, require_auth, validate_token
from api.pb7_bridge import (
    get_allowed_override_params,
    get_bot_param_keys,
    get_hsl_signal_modes,
    get_template_config,
    prepare_override_config,
)
from api.pb7_ohlcv_tools import (
    build_ohlcv_preflight,
    get_ohlcv_preload_job,
    start_ohlcv_preload_job,
    stop_ohlcv_preload_job,
)
from logging_helpers import human_log as _log
from pb7_config import load_pb7_config, prepare_pb7_config_dict, save_pb7_config
from pbgui_purefunc import PBGDIR, load_ini, load_ini_section, save_ini, pb7dir, pb7venv

SERVICE = "BacktestQueueAPI"
ARCHIVE_SERVICE = "ArchiveSync"
CLEANUP_SERVICE = "HLCVSCleanup"
ARCHIVE_RETEST_SERVICE = "ArchiveRetest"

# ── Draft stores for cross-page handoffs ─────────────────────────────────────
_opt_draft_store: dict[str, tuple[float, dict]] = {}
_queue_draft_store: dict[str, tuple[float, list[dict]]] = {}
_OPT_DRAFT_TTL = 600  # 10 minutes
_ARCHIVE_LIST_CACHE_TTL = 2
_ARCHIVE_RESULTS_CACHE_TTL = 60
_archives_list_cache: dict[str, Any] = {}
_archive_results_cache: dict[str, dict[str, Any]] = {}

def _clean_opt_drafts() -> None:
    now = time.time()
    for store in (_opt_draft_store, _queue_draft_store):
        for k in [k for k, v in store.items() if now - v[0] > _OPT_DRAFT_TTL]:
            del store[k]

router = APIRouter()


def _get_cached_archive_results(name: str) -> dict | None:
    entry = _archive_results_cache.get(name)
    if not entry:
        return None
    if time.time() - float(entry.get("ts", 0) or 0) > _ARCHIVE_RESULTS_CACHE_TTL:
        _archive_results_cache.pop(name, None)
        return None
    return entry


def _set_cached_archive_results(name: str, results: list[dict], migration_status: dict) -> None:
    _archive_results_cache[name] = {
        "ts": time.time(),
        "results": results,
        "migration_status": migration_status,
    }


def _invalidate_archive_cache(name: str | None = None) -> None:
    _archives_list_cache.clear()
    if name:
        _archive_results_cache.pop(name, None)
    else:
        _archive_results_cache.clear()

# ── Helpers ───────────────────────────────────────────────────

def _validate_name(name: str):
    """Reject path-traversal attempts."""
    if not name or any(c in name for c in ("/", "\\", "\x00")) or name in (".", ".."):
        raise HTTPException(400, "Invalid name")


def _editor_config_payload(cfg: dict, *, name: str | None = None) -> dict:
    """Return a Run-style editor payload with separated param status metadata."""
    if isinstance(cfg, dict):
        cfg = dict(cfg)
        backtest = cfg.get("backtest")
        if isinstance(backtest, dict):
            backtest = dict(backtest)
            backtest.pop("base_dir", None)
            cfg["backtest"] = backtest
    param_status = cfg.pop("_pbgui_param_status", {}) if isinstance(cfg, dict) else {}
    payload = {"config": cfg, "param_status": param_status}
    if name is not None:
        payload["name"] = name
    return payload


def _managed_backtest_base_dir(name: str) -> str:
    return f"backtests/pbgui/{name}"


def _normalize_backtest_base_dir(cfg: dict, name: str) -> dict:
    if not isinstance(cfg, dict):
        return cfg
    backtest = cfg.get("backtest")
    if not isinstance(backtest, dict):
        backtest = {}
        cfg["backtest"] = backtest
    backtest["base_dir"] = _managed_backtest_base_dir(name)
    return cfg


def _load_and_repair_backtest_config(name: str, cfg_file: Path) -> dict:
    try:
        cfg = load_pb7_config(cfg_file)
    except Exception as exc:
        raise HTTPException(500, f"Error reading config: {exc}") from exc

    expected_base_dir = _managed_backtest_base_dir(name)
    current_base_dir = str((cfg.get("backtest", {}) or {}).get("base_dir") or "")
    if current_base_dir != expected_base_dir:
        _normalize_backtest_base_dir(cfg, name)
        try:
            save_pb7_config(cfg, cfg_file)
        except Exception as exc:
            raise HTTPException(500, f"Error repairing config: {exc}") from exc
    return cfg


def _bt_queue_dir() -> Path:
    return Path(PBGDIR) / "data" / "bt_v7_queue"


def _bt_configs_dir() -> Path:
    return Path(PBGDIR) / "data" / "bt_v7"


def _bt_results_base() -> str:
    """Base directory for backtest results (inside pb7)."""
    return str(Path(pb7dir()) / "backtests" / "pbgui")


def _bt_results_root() -> Path:
    return Path(pb7dir()) / "backtests"


def _legacy_results_roots() -> list[Path]:
    root = _bt_results_root()
    if not root.exists():
        return []
    return [entry.resolve() for entry in sorted(root.iterdir()) if entry.is_dir() and entry.name != "pbgui"]


def _resolve_result_dir(
    path: str | Path,
    *,
    allow_pbgui: bool = True,
    allow_legacy: bool = True,
    allow_archives: bool = True,
) -> Path:
    result_dir = Path(path).resolve()
    allowed_roots: list[Path] = []
    if allow_pbgui:
        allowed_roots.append(Path(_bt_results_base()).resolve())
    if allow_legacy:
        allowed_roots.extend(_legacy_results_roots())
    if allow_archives:
        allowed_roots.append(_archives_dir().resolve())
    for root in allowed_roots:
        if result_dir.is_relative_to(root):
            return result_dir
    raise HTTPException(400, "Invalid result path")


def _find_legacy_result_root(result_dir: Path) -> Path:
    resolved = result_dir.resolve()
    for root in _legacy_results_roots():
        if resolved.is_relative_to(root):
            return root
    raise HTTPException(400, "Invalid legacy result path")


def _bt_log_dir() -> Path:
    return Path(PBGDIR) / "data" / "logs" / "backtests"


def _archives_dir() -> Path:
    return Path(PBGDIR) / "data" / "archives"


def _archive_retests_dir() -> Path:
    return Path(PBGDIR) / "data" / "archive_retests"


def _archive_retest_runs_path() -> Path:
    return _archive_retests_dir() / "runs.json"


def _archive_retest_schedules_path() -> Path:
    return _archive_retests_dir() / "schedules.json"


def _archive_retest_queue_configs_dir() -> Path:
    return _archive_retests_dir() / "queue_configs"


def _archive_retest_stage_dir() -> Path:
    return _archive_retests_dir() / "stage"


def _opt_archive_configs_dir() -> Path:
    return Path(PBGDIR) / "data" / "opt_v7"


def _own_archive_name() -> str:
    return load_ini("config_archive", "my_archive") or ""


def _read_ini_section(section: str = "backtest_v7") -> dict:
    """Read backtest_v7 settings from pbgui.ini."""
    settings = load_ini_section(section)
    if not settings:
        return {"autostart": "False", "cpu": "1"}
    return settings


def _write_ini(key: str, value: str, section: str = "backtest_v7"):
    save_ini(section, key, value)


def _get_pbgui_market_data_path() -> str:
    from market_data import get_market_data_root_dir
    return str(get_market_data_root_dir())


def _apply_pbgui_market_data_override(cfg: dict, enabled: bool) -> tuple[bool, str | None]:
    if not enabled:
        return False, None
    backtest = cfg.setdefault("backtest", {})
    target_path = _get_pbgui_market_data_path()
    current_path = str(backtest.get("ohlcv_source_dir") or "").strip()
    if current_path == target_path:
        return False, target_path
    backtest["ohlcv_source_dir"] = target_path
    return True, target_path


def _load_archive_retest_runs() -> list[dict]:
    data = load_json_file(_archive_retest_runs_path())
    runs = data.get("runs") if isinstance(data, dict) else []
    return runs if isinstance(runs, list) else []


def _save_archive_retest_runs(runs: list[dict]) -> None:
    atomic_write_json(_archive_retest_runs_path(), {"schema_version": 1, "runs": runs})


def _load_archive_retest_schedules() -> list[dict]:
    data = load_json_file(_archive_retest_schedules_path())
    schedules = data.get("schedules") if isinstance(data, dict) else []
    return schedules if isinstance(schedules, list) else []


def _save_archive_retest_schedules(schedules: list[dict]) -> None:
    atomic_write_json(_archive_retest_schedules_path(), {"schema_version": 1, "schedules": schedules})


def _parse_ymd(value: Any) -> datetime.date | None:
    try:
        return datetime.date.fromisoformat(str(value or "")[:10])
    except (TypeError, ValueError):
        return None


def _clamped_days(value: Any, default: int = 365) -> int:
    try:
        days = int(value)
    except (TypeError, ValueError):
        days = default
    return max(1, min(days, 3650))


def _archive_retest_options(body: dict | None) -> dict:
    source = body if isinstance(body, dict) else {}
    if isinstance(source.get("overrides"), dict):
        source = {**source, **source["overrides"]}
    mode = str(source.get("date_mode") or "until_yesterday")
    if mode not in {"until_yesterday", "last_x_days"}:
        mode = "until_yesterday"
    exchanges = source.get("exchanges")
    return {
        "date_mode": mode,
        "last_days": _clamped_days(source.get("last_days"), 365),
        "starting_balance": source.get("starting_balance"),
        "exchanges": exchanges if isinstance(exchanges, list) else [],
        "use_pbgui_market_data": bool(source.get("use_pbgui_market_data", False)),
        "skip_liquidated": bool(source.get("skip_liquidated", True)),
    }


def _apply_archive_retest_date_policy(
    cfg: dict,
    options: dict,
    *,
    today: datetime.date | None = None,
) -> dict:
    """Apply archive-retest date rules and return the concrete date metadata."""
    backtest = cfg.setdefault("backtest", {})
    yesterday = (today or datetime.date.today()) - datetime.timedelta(days=1)
    mode = str((options or {}).get("date_mode") or "until_yesterday")
    last_days = _clamped_days((options or {}).get("last_days"), 365)

    if mode == "last_x_days":
        window_days = last_days
    else:
        start_old = _parse_ymd(backtest.get("start_date"))
        end_old = _parse_ymd(backtest.get("end_date"))
        if start_old and end_old and end_old >= start_old:
            window_days = max(1, (end_old - start_old).days + 1)
        else:
            window_days = last_days
        mode = "until_yesterday"

    start_date = yesterday - datetime.timedelta(days=window_days - 1)
    backtest["start_date"] = start_date.isoformat()
    backtest["end_date"] = yesterday.isoformat()
    return {
        "date_mode": mode,
        "window_days": window_days,
        "start_date": backtest["start_date"],
        "end_date": backtest["end_date"],
    }


def _archive_retest_config_name(result_dir: Path, cfg: dict) -> str:
    backtest = cfg.get("backtest", {}) if isinstance(cfg, dict) else {}
    base_dir = str(backtest.get("base_dir") or "").strip()
    if base_dir:
        return safe_path_part(Path(base_dir).name, "retest")
    try:
        return safe_path_part(result_dir.parent.parent.name, "retest")
    except Exception:
        return "retest"


def _archive_retest_queue_name(config_name: str, run_id: str) -> str:
    return safe_path_part(f"archive_retest_{config_name}_{run_id[:8]}", "archive_retest")


def _queue_archive_retest_run(
    archive_name: str,
    archive_dir: Path,
    result_dir: Path,
    options: dict,
    *,
    schedule_id: str | None = None,
    schedule_target_id: str | None = None,
) -> dict:
    cfg_file = result_dir / "config.json"
    if not cfg_file.exists():
        raise HTTPException(404, f"config.json not found: {result_dir}")
    cfg = load_pb7_config(cfg_file, neutralize_added=True)
    run_id = uuid.uuid4().hex
    source_relative_path = result_dir.resolve().relative_to(archive_dir.resolve()).as_posix()
    archive_config_name = _archive_retest_config_name(result_dir, cfg)
    queue_name = _archive_retest_queue_name(archive_config_name, run_id)
    date_meta = _apply_archive_retest_date_policy(cfg, options)

    backtest = cfg.setdefault("backtest", {})
    if options.get("starting_balance") not in (None, ""):
        backtest["starting_balance"] = options["starting_balance"]
    if options.get("exchanges"):
        backtest["exchanges"] = options["exchanges"]
    if options.get("use_pbgui_market_data"):
        _apply_pbgui_market_data_override(cfg, True)
    _normalize_backtest_base_dir(cfg, queue_name)

    pbgui = cfg.setdefault("pbgui", {})
    pbgui["archive_retest"] = {
        "run_id": run_id,
        "archive_name": archive_name,
        "source_relative_path": source_relative_path,
        "archive_config_name": archive_config_name,
        "created_at": utc_now_iso(),
    }

    snapshot_dir = _archive_retest_queue_configs_dir()
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    snapshot_file = snapshot_dir / f"{run_id}.json"
    save_pb7_config(cfg, snapshot_file)

    filename = str(uuid.uuid4())
    exchange_value = backtest.get("exchanges", [])
    exchange_list = exchange_value if isinstance(exchange_value, list) else [exchange_value]
    queue_data = {
        "name": archive_config_name,
        "filename": filename,
        "json": str(snapshot_file),
        "exchange": exchange_list,
        "archive_retest": {"run_id": run_id},
    }
    queue_dir = _bt_queue_dir()
    queue_dir.mkdir(parents=True, exist_ok=True)
    atomic_write_json(queue_dir / f"{filename}.json", queue_data)
    _store.notify()

    now = utc_now_iso()
    return {
        "id": run_id,
        "archive_name": archive_name,
        "source_relative_path": source_relative_path,
        "archive_config_name": archive_config_name,
        "queue_name": queue_name,
        "queue_filename": filename,
        "queue_config": str(snapshot_file),
        "schedule_id": schedule_id or "",
        "schedule_target_id": schedule_target_id or "",
        "status": "queued",
        "created_at": now,
        "queued_at": now,
        "date": date_meta,
        "options": options,
    }


def _find_archive_retest_local_result(run: dict) -> Path | None:
    queue_name = str(run.get("queue_name") or "")
    run_id = str(run.get("id") or "")
    if not queue_name or not run_id:
        return None
    root = Path(_bt_results_base()) / queue_name
    if not root.exists():
        return None
    candidates = sorted(root.glob("**/analysis.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    fallback = candidates[0].parent if candidates else None
    for analysis_file in candidates:
        result_dir = analysis_file.parent
        cfg_file = result_dir / "config.json"
        if not cfg_file.exists():
            continue
        try:
            cfg = load_pb7_config(cfg_file, neutralize_added=True)
        except Exception:
            continue
        meta = ((cfg.get("pbgui") or {}).get("archive_retest") or {}) if isinstance(cfg, dict) else {}
        if meta.get("run_id") == run_id:
            return result_dir
    return fallback


def _archive_retest_result_liquidated(result_dir: Path) -> tuple[bool, str]:
    analysis = load_json_file(result_dir / "analysis.json")
    try:
        cfg = load_pb7_config(result_dir / "config.json", neutralize_added=True)
    except Exception:
        cfg = {}
    return detect_liquidation(analysis, cfg)


def _stage_archive_retest_result(result_dir: Path, run: dict) -> tuple[Path, Path]:
    run_id = str(run.get("id") or uuid.uuid4().hex)
    stage_parent = _archive_retest_stage_dir() / run_id
    rmtree(str(stage_parent), ignore_errors=True)
    staged = stage_parent / result_dir.name
    staged.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(str(result_dir), str(staged))
    cfg_file = staged / "config.json"
    if cfg_file.exists():
        try:
            cfg = load_pb7_config(cfg_file)
        except Exception:
            cfg = load_json_file(cfg_file)
        archive_config_name = safe_path_part(run.get("archive_config_name"), "retest")
        _normalize_backtest_base_dir(cfg, archive_config_name)
        try:
            save_pb7_config(cfg, cfg_file)
        except Exception:
            atomic_write_json(cfg_file, cfg)
    return staged, stage_parent


def _cleanup_archive_retest_local_artifacts(run: dict, local_result: Path | None) -> dict:
    """Best-effort cleanup after a retest result was safely copied into the archive."""
    removed: list[str] = []
    errors: list[str] = []

    def remove_file(path: Path, label: str) -> None:
        try:
            if path.exists():
                path.unlink()
                removed.append(label)
        except Exception as exc:
            errors.append(f"{label}: {exc}")

    results_base = Path(_bt_results_base()).resolve()
    queue_name = str(run.get("queue_name") or "").strip()
    local_root: Path | None = None
    if queue_name.startswith("archive_retest_"):
        candidate = (results_base / queue_name).resolve()
        if local_result:
            try:
                if local_result.resolve().is_relative_to(candidate):
                    local_root = candidate
            except Exception:
                local_root = None
        elif candidate.is_relative_to(results_base):
            local_root = candidate
    if local_root is None and local_result:
        try:
            local_resolved = local_result.resolve()
            if local_resolved.is_relative_to(results_base):
                local_root = local_resolved
        except Exception:
            local_root = None
    if local_root and local_root.exists():
        try:
            rmtree(str(local_root), ignore_errors=True)
            if local_root.exists():
                errors.append(f"local_result: failed to remove {local_root}")
            else:
                removed.append("local_result")
                cleanup_empty_parents(local_root, results_base)
        except Exception as exc:
            errors.append(f"local_result: {exc}")

    queue_filename = str(run.get("queue_filename") or "").strip()
    if queue_filename:
        try:
            _validate_name(queue_filename)
            remove_file(_bt_queue_dir() / f"{queue_filename}.json", "queue_json")
            remove_file(_bt_queue_dir() / f"{queue_filename}.pid", "queue_pid")
            remove_file(_bt_log_dir() / f"{queue_filename}.log", "queue_log")
        except Exception as exc:
            errors.append(f"queue_files: {exc}")

    queue_config = str(run.get("queue_config") or "").strip()
    if queue_config:
        try:
            queue_config_path = Path(queue_config).resolve()
            config_root = _archive_retest_queue_configs_dir().resolve()
            if queue_config_path.is_relative_to(config_root):
                remove_file(queue_config_path, "queue_config")
        except Exception as exc:
            errors.append(f"queue_config: {exc}")

    if removed or errors:
        level = "WARNING" if errors else "INFO"
        _log(
            ARCHIVE_RETEST_SERVICE,
            f"Archive retest cleanup removed={removed} errors={errors}",
            level=level,
        )
        _store.notify()
    return {"removed": removed, "errors": errors}


def _replace_archive_result_from_local(run: dict) -> dict:
    archive_name = str(run.get("archive_name") or "")
    archive_dir = (_archives_dir() / archive_name).resolve()
    if archive_name != _own_archive_name():
        raise RuntimeError("Archive retest replacement is only allowed for the configured own archive")
    if not archive_dir.exists():
        raise RuntimeError(f"Archive '{archive_name}' not found")
    old_result = (archive_dir / str(run.get("source_relative_path") or "")).resolve()
    if not is_inside_archive(old_result, archive_dir) or not old_result.exists():
        raise RuntimeError("Original archive result no longer exists")
    local_result = _find_archive_retest_local_result(run)
    if not local_result or not local_result.exists():
        raise RuntimeError("Finished local retest result not found")
    if bool((run.get("options") or {}).get("skip_liquidated", True)):
        liquidated, reason = _archive_retest_result_liquidated(local_result)
        if liquidated:
            raise RuntimeError(f"New retest result is liquidated ({reason}); archive unchanged")

    staged, stage_parent = _stage_archive_retest_result(local_result, run)
    try:
        copied = copy_backtest_result_to_archive(staged, archive_dir)
        new_result = Path(copied["path"]).resolve()
        if not is_inside_archive(new_result, archive_dir):
            raise RuntimeError("Copied result escaped archive root")
        if new_result == old_result:
            raise RuntimeError("Replacement would overwrite the original result path")
        rmtree(str(old_result), ignore_errors=True)
        if old_result.exists():
            raise RuntimeError("Failed to remove original archive result")
        cleanup_empty_parents(old_result, archive_dir)
        _invalidate_archive_cache(archive_name)
        manifest = rebuild_archive_manifest(archive_dir)
        cleanup = _cleanup_archive_retest_local_artifacts(run, local_result)
        return {
            "old_relative_path": str(run.get("source_relative_path") or ""),
            "new_relative_path": new_result.relative_to(archive_dir).as_posix(),
            "new_path": str(new_result),
            "manifest": manifest,
            "cleanup": cleanup,
        }
    finally:
        rmtree(str(stage_parent), ignore_errors=True)


# ── BacktestStore — in-memory state with change notification ──

class BacktestStore:
    """In-memory view of the backtest queue, refreshed from disk."""

    def __init__(self):
        self.items: dict[str, dict] = {}   # filename → item dict
        self.changed = asyncio.Event()
        self._lock = asyncio.Lock()

    async def refresh_from_disk(self):
        """Reload queue items from data/bt_v7_queue/*.json."""
        async with self._lock:
            dest = _bt_queue_dir()
            dest.mkdir(parents=True, exist_ok=True)
            found = {}
            for fp in sorted(dest.glob("*.json")):
                try:
                    with open(fp, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    filename = data.get("filename", fp.stem)
                    pid = self._read_pid(filename)
                    log_path = _bt_log_dir() / f"{filename}.log"
                    # Auto-migrate old log location
                    old_log = dest / f"{filename}.log"
                    if old_log.exists() and not log_path.exists():
                        log_path.parent.mkdir(parents=True, exist_ok=True)
                        old_log.rename(log_path)
                    status = self._determine_status(pid, log_path)
                    mtime = fp.stat().st_mtime
                    found[filename] = {
                        "filename": filename,
                        "name": data.get("name", filename),
                        "json": data.get("json", ""),
                        "exchange": data.get("exchange", ""),
                        "archive_retest": data.get("archive_retest") if isinstance(data.get("archive_retest"), dict) else None,
                        "status": status,
                        "pid": pid,
                        "log_path": str(log_path),
                        "created": datetime.datetime.fromtimestamp(mtime).isoformat(),
                    }
                except Exception as e:
                    _log(SERVICE, f"Error loading queue item {fp}: {e}", level="ERROR")
            self.items = found
            self.changed.set()

    def _read_pid(self, filename: str) -> Optional[int]:
        pidfile = _bt_queue_dir() / f"{filename}.pid"
        if pidfile.exists():
            try:
                txt = pidfile.read_text().strip()
                return int(txt) if txt.isdigit() else None
            except Exception:
                return None
        return None

    def _is_process_running(self, pid: int) -> bool:
        try:
            if pid and psutil.pid_exists(pid):
                proc = psutil.Process(pid)
                return any(sub.lower().endswith("backtest.py") for sub in proc.cmdline())
        except (psutil.NoSuchProcess, psutil.ZombieProcess, psutil.AccessDenied):
            pass
        return False

    def _determine_status(self, pid: Optional[int], log_path: Path) -> str:
        running = pid is not None and self._is_process_running(pid)
        log_tail = self._read_log_tail(log_path)

        if running:
            if log_tail and ("Backtesting " in log_tail or "Running scenario" in log_tail):
                return "backtesting"
            return "running"

        if log_tail:
            if "seconds elapsed for backtest:" in log_tail or ("Suite" in log_tail and "completed" in log_tail):
                return "complete"
            return "error"

        return "queued"

    def _read_log_tail(self, log_path: Path, size_kb: int = 50) -> Optional[str]:
        if not log_path or not log_path.exists():
            return None
        try:
            with open(log_path, "rb") as f:
                f.seek(0, 2)
                file_size = f.tell()
                start_pos = max(file_size - size_kb * 1024, 0)
                f.seek(start_pos)
                return f.read().decode("utf-8", errors="ignore")
        except Exception:
            return None

    def notify(self):
        """Signal change to WebSocket push loops."""
        self.changed.set()


_store = BacktestStore()


# ── BacktestWorker — asyncio background task ──────────────────

class BacktestWorker:
    """Processes queued backtests as an asyncio background task."""

    def __init__(self, store: BacktestStore):
        self.store = store
        self._task: Optional[asyncio.Task] = None
        self._running = False

    def start(self):
        if self._task is None or self._task.done():
            self._running = True
            self._task = asyncio.create_task(self._loop(), name="backtest-worker")
            _log(SERVICE, "Backtest worker started", level="INFO")

    def stop(self):
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()

    async def _loop(self):
        """Main worker loop: checks queue, launches backtests respecting CPU limit."""
        try:
            while self._running:
                settings = _read_ini_section()
                autostart = settings.get("autostart", "False").lower() == "true"
                if not autostart:
                    await asyncio.sleep(5)
                    continue

                cpu_limit = min(
                    int(settings.get("cpu", "1")),
                    multiprocessing.cpu_count()
                )

                await self.store.refresh_from_disk()
                items = self.store.items

                running_count = sum(
                    1 for it in items.values() if it["status"] in ("running", "backtesting")
                )
                downloading = any(
                    it["status"] == "running" for it in items.values()
                )

                for filename, item in items.items():
                    if item["status"] != "queued":
                        continue
                    # Wait for CPU slot
                    while running_count >= cpu_limit:
                        await asyncio.sleep(3)
                        settings = _read_ini_section()
                        if settings.get("autostart", "False").lower() != "true":
                            break
                        cpu_limit = min(
                            int(settings.get("cpu", "1")),
                            multiprocessing.cpu_count()
                        )
                        await self.store.refresh_from_disk()
                        running_count = sum(
                            1 for it in self.store.items.values()
                            if it["status"] in ("running", "backtesting")
                        )
                    if settings.get("autostart", "False").lower() != "true":
                        break
                    # Wait for downloads to finish
                    while any(
                        it["status"] == "running" for it in self.store.items.values()
                    ):
                        await asyncio.sleep(3)
                        await self.store.refresh_from_disk()
                    # Re-check autostart
                    settings = _read_ini_section()
                    if settings.get("autostart", "False").lower() != "true":
                        break
                    # Re-check this item hasn't been removed or already started
                    if filename not in self.store.items:
                        continue
                    if self.store.items[filename]["status"] != "queued":
                        continue

                    self._launch_backtest(item)
                    _log(SERVICE, f"Launched backtest: {item['name']} ({filename})", level="INFO")
                    running_count += 1
                    await asyncio.sleep(1)
                    await self.store.refresh_from_disk()

                await asyncio.sleep(10)
        except asyncio.CancelledError:
            _log(SERVICE, "Backtest worker stopped", level="INFO")
        except Exception as e:
            _log(SERVICE, f"Backtest worker error: {e}", level="ERROR",
                 meta={"traceback": traceback.format_exc()})

    def _launch_backtest(self, item: dict):
        """Spawn a backtest subprocess (detached)."""
        venv = pb7venv()
        pb7 = pb7dir()
        config_path = item["json"]
        filename = item["filename"]

        log_path = _bt_log_dir() / f"{filename}.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            cfg = load_pb7_config(Path(config_path))
            settings = _read_ini_section()
            use_pbgui_market_data = settings.get("use_pbgui_market_data", "False").lower() == "true"
            changed, pbgui_data_path = _apply_pbgui_market_data_override(cfg, use_pbgui_market_data)
            if changed:
                save_pb7_config(cfg, Path(config_path))
                _log(
                    SERVICE,
                    f"Adjusted backtest.ohlcv_source_dir to PBGui market data before launch for {item.get('name') or filename}: {pbgui_data_path}",
                    level="INFO",
                )
        except Exception as exc:
            log_path.write_text(f"Failed to prepare backtest config before launch: {exc}\n", encoding="utf-8")
            return

        cmd = [venv, "-u", str(PurePath(f"{pb7}/src/backtest.py")), str(PurePath(config_path))]
        log_file = open(log_path, "w")

        old_path = os.environ.get("PATH", "")
        new_path = os.path.dirname(venv) + os.pathsep + old_path
        env = os.environ.copy()
        env["PATH"] = new_path

        if platform.system() == "Windows":
            flags = subprocess.DETACHED_PROCESS | subprocess.CREATE_NO_WINDOW
            proc = subprocess.Popen(
                cmd, stdout=log_file, stderr=log_file,
                cwd=pb7, text=True, creationflags=flags, env=env
            )
        else:
            proc = subprocess.Popen(
                cmd, stdout=log_file, stderr=log_file,
                cwd=pb7, text=True, start_new_session=True, env=env
            )

        # Write PID file
        pidfile = _bt_queue_dir() / f"{filename}.pid"
        pidfile.write_text(str(proc.pid))


_worker = BacktestWorker(_store)


# ── ArchiveSyncWorker — auto-pull all archives ─────────────────

def _log_archive(msg: str, level: str = "INFO"):
    """Write to ArchiveSync.log, then also route via _log for normal log infrastructure."""
    _log(ARCHIVE_SERVICE, msg, level=level)


def _mask_archive_secret(text: str, secret: str = "") -> str:
    """Return command output with transient credentials redacted."""
    if not text:
        return ""
    if secret:
        text = text.replace(secret, "***")
    return text.strip()


def _run_archive_git_step(
    name: str,
    dest: Path,
    label: str,
    cmd: list[str],
    *,
    timeout: int,
    secret: str = "",
    ok_returncodes: tuple[int, ...] = (0,),
) -> tuple[subprocess.CompletedProcess, str]:
    """Run one git step and write start/finish progress to ArchiveSync.log."""
    _log_archive(f"[{name}] {label} started")
    try:
        result = subprocess.run(cmd, cwd=str(dest), capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        _log_archive(f"[{name}] {label} timed out after {timeout}s", level="ERROR")
        raise
    output = _mask_archive_secret((result.stdout or "") + (result.stderr or ""), secret)
    if result.returncode in ok_returncodes:
        suffix = "" if result.returncode == 0 else f" (return code {result.returncode})"
        _log_archive(f"[{name}] {label} complete{suffix}: {output or 'ok'}")
    else:
        _log_archive(f"[{name}] {label} failed ({result.returncode}): {output or 'no output'}", level="ERROR")
    return result, output


def _archive_push_url(dest: Path, access_token: str) -> str | None:
    """Return an HTTPS remote URL with a transient token injected for push/fetch."""
    if not access_token:
        return None
    url_result = subprocess.run(
        ["git", "remote", "get-url", "origin"], cwd=str(dest),
        capture_output=True, text=True, timeout=10,
    )
    remote_url = (url_result.stdout or "").strip()
    if remote_url.startswith("http://"):
        return remote_url.replace("http://", f"http://{access_token}@", 1)
    if remote_url.startswith("https://"):
        return remote_url.replace("https://", f"https://{access_token}@", 1)
    return None


def _format_bytes(size: int) -> str:
    """Format a byte count for archive storage previews."""
    value = float(max(0, size))
    units = ["Bytes", "KiB", "MiB", "GiB", "TiB"]
    unit = units[0]
    for unit in units:
        if value < 1024 or unit == units[-1]:
            break
        value /= 1024
    if unit == "Bytes":
        return f"{int(value)} Bytes"
    return f"{value:.2f} {unit}"


def _parse_count_objects_bytes(output: str) -> int:
    """Return total git object-store bytes from `git count-objects -v` output."""
    total_kib = 0
    for line in (output or "").splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        if key.strip() not in {"size", "size-pack", "size-garbage"}:
            continue
        try:
            total_kib += int(value.strip())
        except ValueError:
            pass
    return total_kib * 1024


def _current_tree_object_ids(dest: Path) -> list[str]:
    """Return object ids needed by the current HEAD tree."""
    root_result = subprocess.run(
        ["git", "rev-parse", "HEAD^{tree}"], cwd=str(dest),
        capture_output=True, text=True, timeout=10,
    )
    if root_result.returncode != 0:
        return []
    object_ids = {root_result.stdout.strip()}
    tree_result = subprocess.run(
        ["git", "ls-tree", "-r", "-t", "-z", "HEAD"], cwd=str(dest),
        capture_output=True, text=True, timeout=60,
    )
    if tree_result.returncode != 0:
        return sorted(object_ids)
    for record in tree_result.stdout.split("\x00"):
        if not record:
            continue
        meta = record.split("\t", 1)[0].split()
        if len(meta) >= 3:
            object_ids.add(meta[2])
    return sorted(obj for obj in object_ids if obj)


def _git_object_disk_bytes(dest: Path, object_ids: list[str]) -> int:
    """Return current on-disk size for a set of git objects."""
    if not object_ids:
        return 0
    result = subprocess.run(
        ["git", "cat-file", "--batch-check=%(objectname) %(objectsize:disk)"],
        cwd=str(dest), capture_output=True, text=True, input="\n".join(object_ids) + "\n", timeout=120,
    )
    if result.returncode != 0:
        return 0
    total = 0
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) < 2:
            continue
        try:
            total += int(parts[-1])
        except ValueError:
            pass
    return total


def _archive_storage_estimate(name: str, dest: Path, access_token: str = "") -> dict[str, Any]:
    """Estimate git object storage saved by compacting archive history."""
    count_result, count_output = _run_archive_git_step(
        name, dest, "git storage estimate", ["git", "count-objects", "-v"], timeout=20, secret=access_token,
    )
    current_bytes = _parse_count_objects_bytes(count_output) if count_result.returncode == 0 else 0
    snapshot_bytes = _git_object_disk_bytes(dest, _current_tree_object_ids(dest))
    saved_bytes = max(0, current_bytes - snapshot_bytes) if current_bytes and snapshot_bytes else 0
    percent = round((saved_bytes / current_bytes) * 100, 1) if current_bytes else 0.0
    return {
        "available": bool(current_bytes and snapshot_bytes),
        "current_bytes": current_bytes,
        "current_human": _format_bytes(current_bytes),
        "after_bytes": snapshot_bytes,
        "after_human": _format_bytes(snapshot_bytes),
        "saved_bytes": saved_bytes,
        "saved_human": _format_bytes(saved_bytes),
        "saved_percent": percent,
        "note": "Estimate compares current Git object storage with the current archive snapshot. Actual remote savings appear after remote garbage collection.",
    }


def _archive_compact_preview(name: str, dest: Path, access_token: str = "") -> dict[str, Any]:
    """Collect read-only information before archive history compaction."""
    branch_result, branch = _run_archive_git_step(
        name, dest, "git branch", ["git", "branch", "--show-current"], timeout=10, secret=access_token,
    )
    remote_result, remote_url = _run_archive_git_step(
        name, dest, "git remote", ["git", "remote", "get-url", "origin"], timeout=10, secret=access_token,
    )
    status_result, status = _run_archive_git_step(
        name, dest, "git status", ["git", "status", "--short"], timeout=10, secret=access_token,
    )
    count_result, commit_count = _run_archive_git_step(
        name, dest, "git commit count", ["git", "rev-list", "--count", "HEAD"], timeout=10, secret=access_token,
    )
    objects_result, object_size = _run_archive_git_step(
        name, dest, "git object size", ["git", "count-objects", "-vH"], timeout=20, secret=access_token,
    )
    manifest = load_archive_manifest(dest)
    return {
        "branch": branch if branch_result.returncode == 0 else "",
        "remote_url": remote_url if remote_result.returncode == 0 else "",
        "dirty": bool(status),
        "status": status.splitlines() if status_result.returncode == 0 and status else [],
        "commit_count": commit_count if count_result.returncode == 0 else "unknown",
        "object_size": object_size if objects_result.returncode == 0 else "",
        "storage_estimate": _archive_storage_estimate(name, dest, access_token),
        "manifest_items": len(manifest["items"]) if manifest else 0,
    }


def _compact_archive_history(name: str, dest: Path, body: dict) -> dict[str, Any]:
    """Replace archive git history with one root commit and force-push with lease."""
    username = str(body.get("username") or "")
    email = str(body.get("email") or "")
    access_token = str(body.get("access_token") or "")
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    message = str(body.get("message") or f"Compact {name} archive at {timestamp}")
    log_lines: list[str] = []

    _log_archive(f"[{name}] compact history requested")
    if username:
        subprocess.run(["git", "config", "user.name", username], cwd=str(dest), capture_output=True, timeout=10)
    if email:
        subprocess.run(["git", "config", "user.email", email], cwd=str(dest), capture_output=True, timeout=10)

    branch_result, branch = _run_archive_git_step(
        name, dest, "git branch", ["git", "branch", "--show-current"], timeout=10, secret=access_token,
    )
    branch = branch.strip()
    log_lines.append(f"Git branch:\n{branch}")
    if branch_result.returncode != 0 or not branch:
        raise HTTPException(500, "Could not determine current archive branch")

    push_url = _archive_push_url(dest, access_token)
    remote_target = push_url or "origin"
    fetch_result, fetch_out = _run_archive_git_step(
        name, dest, "git fetch", ["git", "fetch", remote_target, branch], timeout=120, secret=access_token,
    )
    log_lines.append(f"Git fetch:\n{fetch_out}")
    if fetch_result.returncode != 0:
        raise HTTPException(500, f"git fetch failed: {fetch_out}")

    remote_result, remote_head = _run_archive_git_step(
        name, dest, "git remote head", ["git", "rev-parse", "FETCH_HEAD"], timeout=10, secret=access_token,
    )
    remote_head = remote_head.strip()
    if remote_result.returncode != 0 or not remote_head:
        raise HTTPException(500, "Could not determine remote archive HEAD")

    compare_result, compare_out = _run_archive_git_step(
        name, dest, "git remote compare", ["git", "rev-list", "--left-right", "--count", f"HEAD...{remote_head}"], timeout=20, secret=access_token,
    )
    log_lines.append(f"Remote compare:\n{compare_out}")
    if compare_result.returncode != 0:
        raise HTTPException(500, f"Could not compare local and remote history: {compare_out}")
    try:
        _ahead, behind = [int(part) for part in compare_out.split()[:2]]
    except (ValueError, IndexError) as exc:
        raise HTTPException(500, f"Unexpected remote comparison output: {compare_out}") from exc
    if behind:
        raise HTTPException(409, "Remote archive has commits missing locally. Run Git Pull before compacting history.")

    score_payload = update_archive_scores_and_readme(dest)
    manifest = score_payload.get("manifest", {})
    log_lines.append(f"Archive scores: {score_payload.get('scored', 0)} result(s) scored, manifest and README updated")
    add_result, add_out = _run_archive_git_step(
        name, dest, "git add", ["git", "add", "-A"], timeout=60, secret=access_token,
    )
    log_lines.append(f"Git add:\n{add_out}")
    if add_result.returncode != 0:
        raise HTTPException(500, f"git add failed: {add_out}")

    tree_result, tree_sha = _run_archive_git_step(
        name, dest, "git write-tree", ["git", "write-tree"], timeout=30, secret=access_token,
    )
    tree_sha = tree_sha.strip()
    if tree_result.returncode != 0 or not tree_sha:
        raise HTTPException(500, f"git write-tree failed: {tree_sha}")

    commit_result, commit_sha = _run_archive_git_step(
        name, dest, "git commit-tree", ["git", "commit-tree", tree_sha, "-m", message], timeout=60, secret=access_token,
    )
    commit_sha = commit_sha.strip()
    log_lines.append(f"Git commit-tree:\n{commit_sha}")
    if commit_result.returncode != 0 or not commit_sha:
        raise HTTPException(500, f"git commit-tree failed: {commit_sha}")

    lease = f"--force-with-lease=refs/heads/{branch}:{remote_head}"
    push_result, push_out = _run_archive_git_step(
        name, dest, "git force push", ["git", "push", lease, remote_target, f"{commit_sha}:refs/heads/{branch}"], timeout=300, secret=access_token,
    )
    log_lines.append(f"Git force push:\n{push_out}")
    if push_result.returncode != 0:
        raise HTTPException(500, f"git force push failed: {push_out}")

    update_result, update_out = _run_archive_git_step(
        name, dest, "git update-ref", ["git", "update-ref", f"refs/heads/{branch}", commit_sha], timeout=30, secret=access_token,
    )
    log_lines.append(f"Git update-ref:\n{update_out}")
    if update_result.returncode != 0:
        raise HTTPException(500, f"git update-ref failed: {update_out}")

    _invalidate_archive_cache(name)
    _log_archive(f"[{name}] compact history complete: {commit_sha}")
    return {
        "ok": True,
        "dry_run": False,
        "branch": branch,
        "commit": commit_sha,
        "manifest": manifest,
        "output": "\n\n".join(log_lines),
    }


def _read_auto_pull_interval() -> int:
    """Return auto-pull interval in minutes (0 = disabled)."""
    val = load_ini("config_archive", "auto_pull_interval") or "0"
    try:
        return max(0, int(val))
    except (ValueError, TypeError):
        return 0


def _pull_all_archives_sync() -> list[dict]:
    """Pull all cloned archives; returns list of {name, output, error} dicts."""
    base = _archives_dir()
    results = []
    if not base.exists():
        return results
    for d in sorted(base.iterdir()):
        if not (d / ".git" / "config").exists():
            continue
        name = d.name
        try:
            result = subprocess.run(
                ["git", "pull"], cwd=str(d),
                capture_output=True, text=True, timeout=60
            )
            output = (result.stdout + result.stderr).strip()
            _log_archive(f"[{name}] git pull: {output or 'ok'}")
            results.append({"name": name, "output": output, "error": ""})
        except subprocess.TimeoutExpired:
            _log_archive(f"[{name}] git pull timed out", level="ERROR")
            results.append({"name": name, "output": "", "error": "timed out"})
        except Exception as exc:
            _log_archive(f"[{name}] git pull failed: {exc}", level="ERROR")
            results.append({"name": name, "output": "", "error": str(exc)})
    return results


class ArchiveSyncWorker:
    """Background asyncio task: periodically pulls all archives."""

    def __init__(self):
        self._task: Optional[asyncio.Task] = None
        self._running = False

    def start(self):
        if self._task is None or self._task.done():
            self._running = True
            self._task = asyncio.create_task(self._loop(), name="archive-sync-worker")

    def stop(self):
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()

    async def _loop(self):
        try:
            while self._running:
                interval = _read_auto_pull_interval()
                if interval <= 0:
                    await asyncio.sleep(60)
                    continue
                _log_archive(f"Auto-pulling all archives (interval={interval}min)…")
                await asyncio.get_event_loop().run_in_executor(None, _pull_all_archives_sync)
                # Sleep interval minutes, checking for stop/config changes every 30s
                remaining = interval * 60
                while remaining > 0 and self._running:
                    await asyncio.sleep(min(30, remaining))
                    remaining -= 30
                    new_interval = _read_auto_pull_interval()
                    if new_interval != interval:
                        break
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            _log_archive(f"ArchiveSyncWorker error: {exc}", level="ERROR")


_archive_sync_worker = ArchiveSyncWorker()


def _queue_status(filename: str) -> str:
    for item in _load_queue_sync():
        if item.get("filename") == filename:
            return str(item.get("status") or "")
    return "missing"


def _mark_archive_retest_schedule_result(run: dict, status: str, message: str = "", new_relative_path: str = "") -> None:
    schedule_id = str(run.get("schedule_id") or "")
    target_id = str(run.get("schedule_target_id") or "")
    if not schedule_id:
        return
    schedules = _load_archive_retest_schedules()
    changed = False
    for schedule in schedules:
        if schedule.get("id") != schedule_id:
            continue
        schedule["last_status"] = status
        schedule["last_message"] = message
        schedule["last_completed_at"] = utc_now_iso()
        if status == "complete" and new_relative_path and target_id:
            for target in schedule.get("targets") or []:
                if target.get("id") == target_id:
                    target["relative_path"] = new_relative_path
                    changed = True
                    break
        changed = True
        break
    if changed:
        _save_archive_retest_schedules(schedules)


def _process_archive_retest_completions() -> None:
    runs = _load_archive_retest_runs()
    changed = False
    for run in runs:
        if run.get("status") == "complete" and not ((run.get("result") or {}).get("cleanup")):
            result = run.get("result") if isinstance(run.get("result"), dict) else {}
            result["cleanup"] = _cleanup_archive_retest_local_artifacts(
                run,
                _find_archive_retest_local_result(run),
            )
            run["result"] = result
            changed = True
            continue
        if run.get("status") not in {"queued", "processing"}:
            continue
        status = _queue_status(str(run.get("queue_filename") or ""))
        if status in {"queued", "running", "backtesting"}:
            continue
        if status == "missing":
            local_result = _find_archive_retest_local_result(run)
            if local_result and (local_result / "analysis.json").exists():
                status = "complete"
            else:
                run["status"] = "error"
                run["error"] = "Queue item is missing before completion"
                run["completed_at"] = utc_now_iso()
                _mark_archive_retest_schedule_result(run, "error", run["error"])
                changed = True
                continue
        if status == "error":
            run["status"] = "error"
            run["error"] = "Backtest queue item failed"
            run["completed_at"] = utc_now_iso()
            _mark_archive_retest_schedule_result(run, "error", run["error"])
            changed = True
            continue
        if status != "complete":
            continue
        run["status"] = "processing"
        try:
            result = _replace_archive_result_from_local(run)
            run["status"] = "complete"
            run["completed_at"] = utc_now_iso()
            run["result"] = result
            _mark_archive_retest_schedule_result(run, "complete", "", result.get("new_relative_path", ""))
            _log(
                ARCHIVE_RETEST_SERVICE,
                f"Replaced archive result {result.get('old_relative_path')} -> {result.get('new_relative_path')}",
                level="INFO",
            )
        except Exception as exc:
            run["status"] = "error"
            run["error"] = str(exc)
            run["completed_at"] = utc_now_iso()
            _mark_archive_retest_schedule_result(run, "error", str(exc))
            _log(
                ARCHIVE_RETEST_SERVICE,
                f"Archive retest replacement failed: {exc}",
                level="WARNING",
                meta={"traceback": traceback.format_exc()},
            )
        changed = True
    if changed:
        _save_archive_retest_runs(runs)


def _parse_schedule_time(value: Any) -> tuple[int, int]:
    text = str(value or "02:00")
    try:
        hour_s, minute_s = text.split(":", 1)
        hour = max(0, min(23, int(hour_s)))
        minute = max(0, min(59, int(minute_s)))
        return hour, minute
    except (ValueError, TypeError):
        return 2, 0


def _next_archive_retest_run_at(schedule: dict, now: datetime.datetime | None = None) -> str:
    current = (now or datetime.datetime.now()).replace(second=0, microsecond=0)
    hour, minute = _parse_schedule_time(schedule.get("time"))
    cadence = str(schedule.get("cadence") or "daily")
    candidate = current.replace(hour=hour, minute=minute)
    if cadence == "weekly":
        try:
            weekday = max(0, min(6, int(schedule.get("weekday", 0))))
        except (TypeError, ValueError):
            weekday = 0
        days_ahead = (weekday - current.weekday()) % 7
        candidate = (current + datetime.timedelta(days=days_ahead)).replace(hour=hour, minute=minute)
        if candidate <= current:
            candidate += datetime.timedelta(days=7)
    else:
        if candidate <= current:
            candidate += datetime.timedelta(days=1)
    return candidate.isoformat()


def _schedule_is_due(schedule: dict, now: datetime.datetime | None = None) -> bool:
    if not schedule.get("enabled", True):
        return False
    due_at = _parse_ymd(str(schedule.get("next_run_at") or "")[:10])
    try:
        due_dt = datetime.datetime.fromisoformat(str(schedule.get("next_run_at") or ""))
    except (TypeError, ValueError):
        due_dt = None
    if not due_dt and due_at:
        due_dt = datetime.datetime.combine(due_at, datetime.time.min)
    if not due_dt:
        schedule["next_run_at"] = _next_archive_retest_run_at(schedule, now)
        return False
    return (now or datetime.datetime.now()) >= due_dt


def _has_pending_archive_retest_run(runs: list[dict], schedule_id: str) -> bool:
    return any(
        run.get("schedule_id") == schedule_id and run.get("status") in {"queued", "processing"}
        for run in runs
    )


def _queue_archive_retest_schedule(schedule: dict, *, force: bool = False) -> dict:
    archive_name = str(schedule.get("archive_name") or "")
    if archive_name != _own_archive_name():
        raise RuntimeError("Scheduled archive retests are only allowed for the configured own archive")
    archive_dir = (_archives_dir() / archive_name).resolve()
    if not archive_dir.exists():
        raise RuntimeError(f"Archive '{archive_name}' not found")

    runs = _load_archive_retest_runs()
    if not force and _has_pending_archive_retest_run(runs, str(schedule.get("id") or "")):
        schedule["last_status"] = "skipped"
        schedule["last_message"] = "Previous scheduled retest is still pending"
        return {"queued": 0, "skipped": True, "reason": schedule["last_message"]}

    queued = []
    for target in schedule.get("targets") or []:
        rel = str(target.get("relative_path") or "")
        result_dir = (archive_dir / rel).resolve()
        if not is_inside_archive(result_dir, archive_dir) or not result_dir.exists():
            schedule["last_status"] = "error"
            schedule["last_message"] = f"Scheduled target missing: {rel}"
            continue
        run = _queue_archive_retest_run(
            archive_name,
            archive_dir,
            result_dir,
            schedule.get("options") or {},
            schedule_id=str(schedule.get("id") or ""),
            schedule_target_id=str(target.get("id") or ""),
        )
        runs.append(run)
        queued.append(run)
    if queued:
        schedule["last_status"] = "queued"
        schedule["last_message"] = ""
        schedule["last_queued_at"] = utc_now_iso()
        _save_archive_retest_runs(runs)
    return {"queued": len(queued), "runs": queued}


def _queue_due_archive_retest_schedules() -> None:
    schedules = _load_archive_retest_schedules()
    if not schedules:
        return
    now = datetime.datetime.now()
    changed = False
    for schedule in schedules:
        if not _schedule_is_due(schedule, now):
            continue
        try:
            _queue_archive_retest_schedule(schedule)
        except Exception as exc:
            schedule["last_status"] = "error"
            schedule["last_message"] = str(exc)
            _log(
                ARCHIVE_RETEST_SERVICE,
                f"Scheduled archive retest failed to queue: {exc}",
                level="WARNING",
                meta={"traceback": traceback.format_exc()},
            )
        schedule["last_run_at"] = utc_now_iso()
        schedule["next_run_at"] = _next_archive_retest_run_at(schedule, now)
        changed = True
    if changed:
        _save_archive_retest_schedules(schedules)


class ArchiveRetestWorker:
    """Background task for scheduled archive retests and completion replacement."""

    def __init__(self):
        self._task: Optional[asyncio.Task] = None
        self._running = False

    def start(self):
        if self._task is None or self._task.done():
            self._running = True
            self._task = asyncio.create_task(self._loop(), name="archive-retest-worker")

    def stop(self):
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()

    async def _loop(self):
        try:
            while self._running:
                await asyncio.get_event_loop().run_in_executor(None, _process_archive_retest_completions)
                await asyncio.get_event_loop().run_in_executor(None, _queue_due_archive_retest_schedules)
                await asyncio.sleep(30)
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            _log(
                ARCHIVE_RETEST_SERVICE,
                f"ArchiveRetestWorker error: {exc}",
                level="ERROR",
                meta={"traceback": traceback.format_exc()},
            )


_archive_retest_worker = ArchiveRetestWorker()


# ── WebSocket ─────────────────────────────────────────────────

_ws_clients: set[WebSocket] = set()

# ── Archive inotify watcher ────────────────────────────────────
# Uses raw Linux inotify via ctypes (same approach as master/v7_config_sync.py).
# Watches the archives directory tree for IN_CREATE / IN_MOVED_TO / IN_DELETE
# events and signals connected WS clients so they can refresh without polling.

import ctypes
import ctypes.util
import struct

_libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)

# inotify event flags
_IN_CREATE   = 0x00000100
_IN_DELETE   = 0x00000200
_IN_MOVED_TO = 0x00000080
_IN_MOVE_SELF= 0x00000800
_INOTIFY_MASK = _IN_CREATE | _IN_DELETE | _IN_MOVED_TO | _IN_MOVE_SELF
_INOTIFY_EVENT_STRUCT = struct.Struct("iIII")  # wd, mask, cookie, len

_archive_watcher_task: asyncio.Task | None = None


async def _archive_watcher_loop() -> None:
    """inotify-based watcher for the local archives directory.

    Adds watches on the archives root and all immediate subdirectories
    (one level = one archive, each containing per-config subdirs).
    When files are created/deleted/moved in the tree, sets
    ``_archive_changed`` so the WS push loop can broadcast an
    ``archive_update`` message to all connected clients.
    """
    loop = asyncio.get_running_loop()
    fd: int = -1
    try:
        fd = _libc.inotify_init1(0o00004000)  # IN_NONBLOCK = O_NONBLOCK
        if fd < 0:
            _log(SERVICE, "inotify_init1 failed — archive watcher disabled", level="WARNING")
            return

        def _add_watch(path: str) -> int:
            return _libc.inotify_add_watch(fd, path.encode(), _INOTIFY_MASK)

        # Watch root + all existing subdirs (recursion depth 1 is enough:
        # archives/<name>/<config>/<timestamp>/analysis.json)
        watched: dict[int, str] = {}  # wd → path

        def _setup_watches() -> None:
            watched.clear()
            root = _archives_dir()
            root.mkdir(parents=True, exist_ok=True)
            wd = _add_watch(str(root))
            if wd >= 0:
                watched[wd] = str(root)
            # Watch each archive dir and each config dir inside it
            for archive_dir in root.iterdir():
                if not archive_dir.is_dir():
                    continue
                wd = _add_watch(str(archive_dir))
                if wd >= 0:
                    watched[wd] = str(archive_dir)
                for cfg_dir in archive_dir.iterdir():
                    if not cfg_dir.is_dir():
                        continue
                    wd = _add_watch(str(cfg_dir))
                    if wd >= 0:
                        watched[wd] = str(cfg_dir)

        _setup_watches()

        _log(SERVICE, f"Archive inotify watcher started ({len(watched)} watches)", level="DEBUG")

        reader_fd = os.fdopen(fd, "rb", buffering=0, closefd=False)
        ev_size = _INOTIFY_EVENT_STRUCT.size

        def _readable() -> bytes:
            return reader_fd.read(4096)

        while True:
            # Wait for data on fd using asyncio's add_reader
            data_ready = asyncio.Event()
            loop.add_reader(fd, data_ready.set)
            try:
                await data_ready.wait()
            finally:
                loop.remove_reader(fd)

            try:
                raw = _readable()
            except BlockingIOError:
                continue

            pos = 0
            needs_rewwatch = False
            while pos + ev_size <= len(raw):
                wd, mask, _cookie, name_len = _INOTIFY_EVENT_STRUCT.unpack_from(raw, pos)
                pos += ev_size + name_len
                if mask & (_IN_CREATE | _IN_MOVED_TO | _IN_DELETE | _IN_MOVE_SELF):
                    # Broadcast archive_update directly to all connected WS clients
                    for _client in list(_ws_clients):
                        try:
                            await _client.send_json({"type": "archive_update"})
                        except Exception:
                            pass
                    # New subdirectory created → add watch for it
                    if mask & (_IN_CREATE | _IN_MOVED_TO):
                        needs_rewwatch = True

            if needs_rewwatch:
                # Re-scan watches after short delay (dir may not be fully created yet)
                await asyncio.sleep(0.2)
                _setup_watches()

    except asyncio.CancelledError:
        pass
    except Exception as e:
        _log(SERVICE, f"Archive watcher error: {e}", level="WARNING")
    finally:
        if fd >= 0:
            try:
                os.close(fd)
            except OSError:
                pass
        _log(SERVICE, "Archive inotify watcher stopped", level="DEBUG")


async def _ws_push_loop(ws: WebSocket):
    """Push queue state to a single WebSocket client on changes."""
    try:
        while True:
            try:
                await _store.refresh_from_disk()
                _store.changed.clear()   # clear AFTER refresh (refresh sets the event)
                msg = {
                    "type": "queue_update",
                    "items": list(_store.items.values()),
                    "settings": _read_ini_section(),
                }
                await ws.send_json(msg)
            except (WebSocketDisconnect, RuntimeError):
                break
            except Exception as e:
                _log(SERVICE, f"WS push error: {e}", level="WARNING")
            # Wait for next change or poll every 3 seconds
            try:
                await asyncio.wait_for(_store.changed.wait(), timeout=3.0)
            except asyncio.TimeoutError:
                pass
    except asyncio.CancelledError:
        pass


@router.websocket("/ws/bt7")
async def ws_backtest(websocket: WebSocket):
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
            # Handle client messages (e.g. request refresh)
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


# ── HLCVS Cache Cleanup Worker ────────────────────────────────

class HLCVSCleanupWorker:
    """Periodically removes old hlcvs_data directories to free disk space."""

    def __init__(self):
        self._task: Optional[asyncio.Task] = None
        self._running = False

    def start(self):
        if self._task is None or self._task.done():
            self._running = True
            self._task = asyncio.create_task(self._loop(), name="hlcvs-cleanup")
            _log(CLEANUP_SERVICE, "HLCVS cleanup worker started", level="INFO")

    def stop(self):
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()

    async def _loop(self):
        try:
            while self._running:
                try:
                    settings = _read_ini_section()
                    enabled = settings.get("hlcvs_cleanup_enabled", "False").lower() == "true"
                    interval_h = max(1, int(settings.get("hlcvs_cleanup_interval_h", "24")))
                    if enabled:
                        days = max(1, int(settings.get("hlcvs_cleanup_days", "7")))
                        await asyncio.to_thread(self._do_cleanup, days)
                    await asyncio.sleep(interval_h * 3600)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    _log(CLEANUP_SERVICE, f"Error in cleanup loop: {e}",
                         level="ERROR", meta={"traceback": traceback.format_exc()})
                    await asyncio.sleep(300)
        except asyncio.CancelledError:
            pass

    @staticmethod
    def _do_cleanup(retention_days: int):
        result = _cleanup_cache_roots(retention_days)
        removed = int(result.get("removed") or 0)
        freed_bytes = int(result.get("freed_bytes") or 0)
        errors = int(result.get("errors") or 0)
        if removed > 0:
            _log(
                CLEANUP_SERVICE,
                f"Cleaned {removed} cache dirs older than {retention_days}d, freed {freed_bytes // (1024 * 1024)} MB"
                + (f" ({errors} errors)" if errors else ""),
                level="INFO",
            )
        elif errors > 0:
            _log(
                CLEANUP_SERVICE,
                f"Cleanup older than {retention_days}d finished with {errors} errors",
                level="WARNING",
            )


def _cleanup_cache_targets() -> list[tuple[str, Path]]:
    cache_root = Path(pb7dir()) / "caches"
    return [
        ("hlcvs_data", cache_root / "hlcvs_data"),
        ("ohlcvs/materialized", cache_root / "ohlcvs" / "materialized"),
    ]


def _cleanup_cache_roots(retention_days: int) -> dict[str, Any]:
    cutoff = datetime.datetime.now().timestamp() - (retention_days * 86400)
    targets: list[dict[str, Any]] = []
    removed = 0
    freed = 0
    errors = 0

    for label, root in _cleanup_cache_targets():
        target_removed = 0
        target_freed = 0
        target_errors = 0
        if root.is_dir():
            for entry in root.iterdir():
                if not entry.is_dir():
                    continue
                try:
                    if entry.stat().st_mtime < cutoff:
                        size = sum(f.stat().st_size for f in entry.rglob("*") if f.is_file())
                        rmtree(entry)
                        target_removed += 1
                        target_freed += size
                except Exception as e:
                    target_errors += 1
                    _log(CLEANUP_SERVICE, f"Failed to remove {label}/{entry.name}: {e}", level="WARNING")
        targets.append(
            {
                "label": label,
                "path": str(root),
                "removed": target_removed,
                "freed_bytes": target_freed,
                "errors": target_errors,
            }
        )
        removed += target_removed
        freed += target_freed
        errors += target_errors

    return {
        "removed": removed,
        "freed_bytes": freed,
        "errors": errors,
        "targets": targets,
    }


_hlcvs_cleanup_worker = HLCVSCleanupWorker()


# ── Lifespan hook ─────────────────────────────────────────────

def startup():
    """Called from PBApiServer lifespan to start the worker."""
    global _archive_watcher_task
    _worker.start()
    _archive_sync_worker.start()
    _archive_retest_worker.start()
    _hlcvs_cleanup_worker.start()
    _archive_watcher_task = asyncio.create_task(
        _archive_watcher_loop(), name="archive-inotify-watcher"
    )


def shutdown():
    """Called from PBApiServer lifespan to stop the worker."""
    global _archive_watcher_task
    _worker.stop()
    _archive_sync_worker.stop()
    _archive_retest_worker.stop()
    _hlcvs_cleanup_worker.stop()
    if _archive_watcher_task and not _archive_watcher_task.done():
        _archive_watcher_task.cancel()


# ── REST: Main page ───────────────────────────────────────────

@router.get("/main_page", response_class=HTMLResponse)
def main_page(
    request: Request,
    session: SessionToken = Depends(require_auth),
) -> HTMLResponse:
    html_path = Path(__file__).resolve().parent.parent / "frontend" / "v7_backtest.html"
    if not html_path.exists():
        raise HTTPException(404, "v7_backtest.html not found")
    html = html_path.read_text(encoding="utf-8")

    scheme = request.url.scheme
    host = request.url.hostname or "127.0.0.1"
    port = request.url.port
    origin = f"{scheme}://{host}" + (f":{port}" if port else "")
    api_base = origin + "/api/backtest-v7"
    ws_base = origin.replace("http://", "ws://").replace("https://", "wss://")

    html = html.replace('"%%TOKEN%%"', json.dumps(session.token))
    html = html.replace('"%%API_BASE%%"', json.dumps(api_base))
    html = html.replace('"%%WS_BASE%%"', json.dumps(ws_base))

    from pbgui_purefunc import PBGUI_VERSION, PBGUI_SERIAL
    html = html.replace('"%%VERSION%%"', json.dumps(PBGUI_VERSION))
    html = html.replace("%%VERSION%%", PBGUI_VERSION)
    html = html.replace('"%%SERIAL%%"', json.dumps(PBGUI_SERIAL))
    html = html.replace("%%SERIAL%%", PBGUI_SERIAL)

    nav_js = Path(__file__).resolve().parent.parent / "frontend" / "pbgui_nav.js"
    nav_hash = str(int(nav_js.stat().st_mtime)) if nav_js.exists() else PBGUI_VERSION
    html = html.replace("%%NAV_HASH%%", nav_hash)

    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})


# ── REST: Optimize draft (optimize-from-result) ──────────────

@router.post("/optimize-draft")
def create_optimize_draft(body: dict, session: SessionToken = Depends(require_auth)):
    """Store a config dict as a short-lived draft for the Optimize editor."""
    _clean_opt_drafts()
    config = body.get("config")
    if not isinstance(config, dict):
        raise HTTPException(status_code=422, detail="config must be a dict")
    draft_id = secrets.token_urlsafe(16)
    _opt_draft_store[draft_id] = (time.time(), config)
    return {"draft_id": draft_id}


@router.get("/optimize-draft/{draft_id}")
def get_optimize_draft(draft_id: str, session: SessionToken = Depends(require_auth)):
    """Retrieve a previously stored optimize draft."""
    entry = _opt_draft_store.get(draft_id)
    if not entry:
        raise HTTPException(status_code=404, detail="Draft not found or expired")
    return {"config": entry[1]}


@router.post("/queue-draft")
def create_queue_draft(body: dict, session: SessionToken = Depends(require_auth)):
    """Store multiple backtest configs as a short-lived draft for queue parameter selection."""
    _clean_opt_drafts()
    items = body.get("items")
    if not isinstance(items, list) or not items:
        raise HTTPException(status_code=422, detail="items must be a non-empty list")

    normalized = []
    for item in items:
        if not isinstance(item, dict):
            raise HTTPException(status_code=422, detail="each item must be an object")
        config = item.get("config")
        if not isinstance(config, dict):
            raise HTTPException(status_code=422, detail="each item.config must be a dict")
        name = str(item.get("name") or "rebacktest")
        normalized.append({"name": name, "config": config})

    draft_id = secrets.token_urlsafe(16)
    _queue_draft_store[draft_id] = (time.time(), normalized)
    return {"draft_id": draft_id}


@router.get("/queue-draft/{draft_id}")
def get_queue_draft(draft_id: str, session: SessionToken = Depends(require_auth)):
    """Retrieve a previously stored backtest queue draft."""
    entry = _queue_draft_store.get(draft_id)
    if not entry:
        raise HTTPException(status_code=404, detail="Draft not found or expired")
    return {"items": entry[1]}


# ── REST: PBGui data path ─────────────────────────────────────

@router.get("/pbgui_data_path")
def get_pbgui_data_path(session: SessionToken = Depends(require_auth)):
    """Return the PBGui-managed market data root directory."""
    return {"path": _get_pbgui_market_data_path()}


@router.post("/ohlcv-preflight")
async def get_ohlcv_preflight(body: dict, session: SessionToken = Depends(require_auth)):
    config = body.get("config") if isinstance(body, dict) else None
    if not isinstance(config, dict):
        raise HTTPException(status_code=400, detail="Missing or invalid 'config' in body")
    try:
        return await build_ohlcv_preflight(config)
    except HTTPException:
        raise
    except Exception as exc:
        detail = str(exc).strip() or exc.__class__.__name__
        _log(
            SERVICE,
            f"Failed to build OHLCV preflight: {detail}",
            level="WARNING",
            meta={"traceback": traceback.format_exc()},
        )
        raise HTTPException(status_code=422, detail=detail) from exc


@router.post("/ohlcv-preload")
def start_editor_ohlcv_preload(body: dict, session: SessionToken = Depends(require_auth)):
    config = body.get("config") if isinstance(body, dict) else None
    if not isinstance(config, dict):
        raise HTTPException(status_code=400, detail="Missing or invalid 'config' in body")
    try:
        return start_ohlcv_preload_job(config)
    except Exception as exc:
        detail = str(exc).strip() or exc.__class__.__name__
        _log(
            SERVICE,
            f"Failed to start OHLCV preload: {detail}",
            level="WARNING",
            meta={"traceback": traceback.format_exc()},
        )
        raise HTTPException(status_code=422, detail=detail) from exc


@router.get("/ohlcv-preload/{job_id}")
def get_editor_ohlcv_preload(job_id: str, session: SessionToken = Depends(require_auth)):
    payload = get_ohlcv_preload_job(job_id)
    if payload is None:
        raise HTTPException(status_code=404, detail="OHLCV preload job not found")
    return payload


@router.delete("/ohlcv-preload/{job_id}")
def stop_editor_ohlcv_preload(job_id: str, session: SessionToken = Depends(require_auth)):
    payload = stop_ohlcv_preload_job(job_id)
    if payload is None:
        raise HTTPException(status_code=404, detail="OHLCV preload job not found")
    return payload


# ── REST: Settings ────────────────────────────────────────────

@router.get("/settings")
def get_settings(session: SessionToken = Depends(require_auth)):
    settings = _read_ini_section()
    cpu_max = multiprocessing.cpu_count()
    return {
        "autostart": settings.get("autostart", "False").lower() == "true",
        "cpu": min(int(settings.get("cpu", "1")), cpu_max),
        "use_pbgui_market_data": settings.get("use_pbgui_market_data", "False").lower() == "true",
        "cpu_max": cpu_max,
        "hsl_signal_modes": get_hsl_signal_modes(),
        "hlcvs_cleanup_enabled": settings.get("hlcvs_cleanup_enabled", "False").lower() == "true",
        "hlcvs_cleanup_days": int(settings.get("hlcvs_cleanup_days", "7")),
        "hlcvs_cleanup_interval_h": int(settings.get("hlcvs_cleanup_interval_h", "24")),
    }


@router.post("/settings")
def update_settings(body: dict, session: SessionToken = Depends(require_auth)):
    if "autostart" in body:
        _write_ini("autostart", str(bool(body["autostart"])))
    if "cpu" in body:
        cpu = max(1, min(int(body["cpu"]), multiprocessing.cpu_count()))
        _write_ini("cpu", str(cpu))
    if "use_pbgui_market_data" in body:
        _write_ini("use_pbgui_market_data", str(bool(body["use_pbgui_market_data"])))
    if "hlcvs_cleanup_enabled" in body:
        _write_ini("hlcvs_cleanup_enabled", str(bool(body["hlcvs_cleanup_enabled"])))
    if "hlcvs_cleanup_days" in body:
        days = max(1, min(int(body["hlcvs_cleanup_days"]), 365))
        _write_ini("hlcvs_cleanup_days", str(days))
    if "hlcvs_cleanup_interval_h" in body:
        interval = max(1, min(int(body["hlcvs_cleanup_interval_h"]), 168))
        _write_ini("hlcvs_cleanup_interval_h", str(interval))
    _store.notify()
    return {"ok": True}


@router.post("/settings/hlcvs-cleanup-now")
async def hlcvs_cleanup_now(body: dict, session: SessionToken = Depends(require_auth)):
    """Trigger an immediate HLCVS cache cleanup."""
    days = max(1, min(int(body.get("days", 7)), 365))
    result = await asyncio.to_thread(_hlcvs_cleanup_now_sync, days)
    return result


def _hlcvs_cleanup_now_sync(retention_days: int) -> dict:
    result = _cleanup_cache_roots(retention_days)
    removed = int(result.get("removed") or 0)
    freed = int(result.get("freed_bytes") or 0)
    errors = int(result.get("errors") or 0)
    if removed > 0:
        _log(
            CLEANUP_SERVICE,
            f"Manual cleanup: removed {removed} dirs older than {retention_days}d, freed {freed // (1024 * 1024)} MB"
            + (f" ({errors} errors)" if errors else ""),
            level="INFO",
        )
    return {
        "removed": removed,
        "freed_mb": round(freed / (1024 * 1024)),
        "errors": errors,
        "targets": [
            {
                "label": str(target.get("label") or ""),
                "path": str(target.get("path") or ""),
                "removed": int(target.get("removed") or 0),
                "freed_mb": round(int(target.get("freed_bytes") or 0) / (1024 * 1024)),
                "errors": int(target.get("errors") or 0),
            }
            for target in (result.get("targets") or [])
        ],
    }


# ── REST: Bot params (from passivbot schema) ─────────────────

@router.get("/configs/new-config")
def get_new_backtest_config(session: SessionToken = Depends(require_auth)):
    """Return a default backtest config from the passivbot schema.

    Using get_template_config() keeps the defaults always in sync with the
    installed passivbot version without any manual maintenance.
    """
    try:
        tmpl = get_template_config()
    except Exception as exc:
        _log(SERVICE, f"Failed to load template config: {exc}", level="warning")
        tmpl = {"backtest": {}, "bot": {}, "live": {}, "optimize": {}}
    return _editor_config_payload(tmpl)


@router.get("/bot-params")
def get_bot_params(session: SessionToken = Depends(require_auth)):
    """Return list of bot.long parameter names from passivbot schema."""
    try:
        return {"params": [{"key": key} for key in get_bot_param_keys()]}
    except Exception as exc:
        _log(SERVICE, f"Failed to load bot params: {exc}", level="warning")
        return {"params": []}


@router.get("/override-params")
def get_override_params(session: SessionToken = Depends(require_auth)):
    """Return allowed coin_overrides parameters from passivbot."""
    try:
        return {"params": get_allowed_override_params()}
    except Exception as exc:
        _log(SERVICE, f"Failed to load override params: {exc}", level="warning")
        return {"params": {}}


@router.get("/override-config/{config_name}/{filename}")
def get_override_config(config_name: str, filename: str,
                        session: SessionToken = Depends(require_auth)):
    """Read an override config file (e.g. 1000BONKUSDT.json) from a config directory."""
    _validate_name(config_name)
    # Sanitize filename — only allow simple filenames, no path traversal
    if "/" in filename or "\\" in filename or ".." in filename:
        raise HTTPException(400, "Invalid filename")
    cfg_dir = _bt_configs_dir() / config_name
    override_file = cfg_dir / filename
    if not override_file.exists():
        # Fallback: find pre-normalization file (e.g. BONK.json → 1000BONKUSDT.json)
        stem = filename.rsplit(".", 1)[0] if "." in filename else filename
        norm = _normalize_coin_name(stem)
        for f in cfg_dir.iterdir():
            if f.suffix == ".json" and f.name != "backtest.json":
                if _normalize_coin_name(f.stem) == norm:
                    override_file = f
                    break
    if not override_file.exists():
        raise HTTPException(404, f"Override config '{filename}' not found in '{config_name}'")
    try:
        with open(override_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        return {"config": prepare_override_config(data, verbose=False)}
    except Exception as exc:
        raise HTTPException(500, f"Error reading override config: {exc}")


@router.put("/override-config/{config_name}/{filename}")
def save_override_config(config_name: str, filename: str, body: dict,
                         session: SessionToken = Depends(require_auth)):
    """Save an override config file (e.g. HYPE.json) to a config directory.

    The request body contains only the override params
    ({bot: {long: {...}, short: {...}}, live: {...}}).
    Written as-is — override files are sparse diffs, not full configs.
    """
    _validate_name(config_name)
    if "/" in filename or "\\" in filename or ".." in filename:
        raise HTTPException(400, "Invalid filename")
    if not filename.endswith(".json"):
        raise HTTPException(400, "Filename must end with .json")
    cfg_dir = _bt_configs_dir() / config_name
    cfg_dir.mkdir(parents=True, exist_ok=True)
    override_file = cfg_dir / filename
    # Ensure ``live`` key exists so passivbot's load_prepared_config can
    # detect the "live_only" flavor when loading the file at backtest time.
    if "live" not in body:
        body["live"] = {}
    tmp = override_file.with_suffix(".json.tmp")
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(body, f, indent=4)
            f.write("\n")
        os.replace(str(tmp), str(override_file))
    except Exception:
        if tmp.exists():
            tmp.unlink(missing_ok=True)
        raise
    return {"ok": True}


# ── REST: Configs (saved backtest configurations) ─────────────

@router.get("/configs")
def list_configs(session: SessionToken = Depends(require_auth)):
    """List saved backtest configs from data/bt_v7/*/backtest.json."""
    base = _bt_configs_dir()
    configs = []
    if base.exists():
        for p in sorted(base.iterdir()):
            cfg_file = p / "backtest.json"
            if cfg_file.exists():
                try:
                    with open(cfg_file, "r", encoding="utf-8") as f:
                        cfg = json.load(f)
                    # Count results
                    results_path = Path(_bt_results_base()) / p.name
                    result_count = len(list(results_path.glob("**/analysis.json"))) if results_path.exists() else 0
                    # Extract key info
                    bt = cfg.get("backtest", {})
                    bot = cfg.get("bot", {})
                    live = cfg.get("live", {})
                    exchanges = bt.get("exchanges", [])
                    approved_long = live.get("approved_coins", {}).get("long", [])
                    approved_short = live.get("approved_coins", {}).get("short", [])
                    coins = list(set(approved_long + approved_short))
                    configs.append({
                        "name": p.name,
                        "exchanges": exchanges,
                        "coins": len(coins),
                        "coin_list": coins,
                        "results": result_count,
                        "start_date": bt.get("start_date", ""),
                        "end_date": bt.get("end_date", ""),
                        "starting_balance": bt.get("starting_balance", 0),
                        "twe_long": bot.get("long", {}).get("total_wallet_exposure_limit", 0),
                        "twe_short": bot.get("short", {}).get("total_wallet_exposure_limit", 0),
                        "pos_long": bot.get("long", {}).get("n_positions", 0),
                        "pos_short": bot.get("short", {}).get("n_positions", 0),
                        "modified": datetime.datetime.fromtimestamp(cfg_file.stat().st_mtime).isoformat(),
                    })
                except Exception as e:
                    _log(SERVICE, f"Error reading config {cfg_file}: {e}", level="WARNING")
    return {"configs": configs}


@router.get("/configs/{name}")
def get_config(name: str, session: SessionToken = Depends(require_auth)):
    _validate_name(name)
    cfg_file = _bt_configs_dir() / name / "backtest.json"
    if not cfg_file.exists():
        raise HTTPException(404, f"Config '{name}' not found")
    try:
        cfg = load_pb7_config(cfg_file, neutralize_added=True)
        return _editor_config_payload(cfg, name=name)
    except HTTPException:
        raise
    except Exception as exc:
        detail = str(exc).strip() or exc.__class__.__name__
        _log(
            SERVICE,
            f"Failed to load backtest config '{name}': {detail}",
            level="WARNING",
            meta={"traceback": traceback.format_exc()},
        )
        raise HTTPException(status_code=422, detail=detail)


@router.post("/configs/prepare")
def prepare_config_for_editor(body: dict, session: SessionToken = Depends(require_auth)):
    """Normalize an in-memory config dict for Backtest editor import flows."""
    cfg = body.get("config") if isinstance(body, dict) else None
    if not isinstance(cfg, dict):
        raise HTTPException(status_code=400, detail="Missing or invalid 'config' in body")
    try:
        prepared = prepare_pb7_config_dict(cfg, neutralize_added=True)
        return _editor_config_payload(prepared)
    except Exception as exc:
        detail = str(exc).strip() or exc.__class__.__name__
        _log(
            SERVICE,
            f"Failed to prepare imported backtest config: {detail}",
            level="WARNING",
            meta={"traceback": traceback.format_exc()},
        )
        raise HTTPException(status_code=422, detail=detail) from exc


def _normalize_coin_name(symbol: str) -> str:
    """Normalize exchange symbol to short coin name (mirrors JS _covNormalizeCoin).
    E.g. HYPEUSDT → HYPE, 1000BONKUSDT → BONK, kSHIB → SHIB."""
    import re
    s = symbol.upper()
    for q in ('USDT', 'USDC', 'BUSD', 'USD'):
        if len(s) > len(q) and s.endswith(q):
            s = s[:-len(q)]
            break
    m = re.match(r'^(10+)([A-Z].*)', s)
    if m:
        s = m.group(2)
    if len(s) > 1 and s[0] == 'K' and s[1] != 'K':
        tail = s[1:]
        if re.match(r'^[A-Z]+$', tail):
            s = tail
    return s


def _copy_override_files(cfg: dict, src_dir: Path, dst_dir: Path) -> None:
    """Copy override_config_path files referenced in coin_overrides.
    Handles normalized filenames: if HYPE.json is referenced but only
    HYPEUSDT.json exists in src, copies it with the new name."""
    import shutil
    overrides = cfg.get("coin_overrides", {})
    # Build reverse map: normalized coin name → source file on disk
    src_file_map = {}
    if src_dir.is_dir():
        for f in src_dir.iterdir():
            if f.suffix == ".json" and f.name != "backtest.json":
                norm = _normalize_coin_name(f.stem)
                src_file_map[norm] = f
    for coin, ov in overrides.items():
        fname = ov.get("override_config_path", "")
        if not fname:
            continue
        safe = Path(fname).name  # prevent path traversal
        src_file = src_dir / safe
        if not src_file.is_file():
            # Fallback: find source file via normalization
            norm = _normalize_coin_name(coin)
            src_file = src_file_map.get(norm)
        if src_file and src_file.is_file():
            shutil.copy2(str(src_file), str(dst_dir / safe))


@router.put("/configs/{name}")
def save_config(name: str, body: dict, source_name: str = None,
               session: SessionToken = Depends(require_auth)):
    _validate_name(name)
    cfg = _normalize_backtest_base_dir(body, name)
    cfg_dir = _bt_configs_dir() / name
    cfg_dir.mkdir(parents=True, exist_ok=True)
    # Copy override_config_path files from source when saving as new name
    if source_name and source_name != name:
        _validate_name(source_name)
        _copy_override_files(cfg, _bt_configs_dir() / source_name, cfg_dir)
    cfg_file = cfg_dir / "backtest.json"
    save_pb7_config(cfg, cfg_file)
    # Rename pre-normalization override files and delete truly orphaned ones
    _cleanup_orphaned_overrides(cfg, cfg_dir)
    return {"ok": True, "name": name}


def _cleanup_orphaned_overrides(cfg: dict, cfg_dir: Path) -> None:
    """Rename pre-normalization override files (e.g. HYPEUSDT.json → HYPE.json),
    ensure every referenced override file contains ``live`` key (required for
    passivbot's flavor detection), and delete truly orphaned .json files."""
    referenced = set()
    for coin, ov in cfg.get("coin_overrides", {}).items():
        fname = ov.get("override_config_path", "")
        if fname:
            referenced.add(Path(fname).name)
    for f in list(cfg_dir.iterdir()):
        if f.suffix != ".json" or f.name == "backtest.json":
            continue
        if f.name in referenced:
            # Ensure ``live`` key exists for passivbot flavor detection
            try:
                with open(f, "r", encoding="utf-8") as fh:
                    data = json.load(fh)
                if "live" not in data:
                    data["live"] = {}
                    tmp = f.with_suffix(".json.tmp")
                    with open(tmp, "w", encoding="utf-8") as fh:
                        json.dump(data, fh, indent=4)
                        fh.write("\n")
                    os.replace(str(tmp), str(f))
            except (OSError, json.JSONDecodeError):
                pass
            continue  # keep referenced file
        # Check if this is a pre-normalization file that should be renamed
        norm_fname = _normalize_coin_name(f.stem) + ".json"
        if norm_fname in referenced and not (cfg_dir / norm_fname).exists():
            f.rename(cfg_dir / norm_fname)
        else:
            f.unlink(missing_ok=True)


@router.delete("/configs/{name}")
def delete_config(name: str, remove_results: bool = False,
                  session: SessionToken = Depends(require_auth)):
    _validate_name(name)
    cfg_dir = _bt_configs_dir() / name
    if not cfg_dir.exists():
        raise HTTPException(404, f"Config '{name}' not found")
    rmtree(str(cfg_dir), ignore_errors=True)
    if remove_results:
        results_dir = Path(_bt_results_base()) / name
        if results_dir.exists():
            rmtree(str(results_dir), ignore_errors=True)
    return {"ok": True}


@router.post("/configs/{name}/duplicate")
def duplicate_config(name: str, body: dict, session: SessionToken = Depends(require_auth)):
    _validate_name(name)
    new_name = body.get("new_name", "")
    _validate_name(new_name)
    src_dir = _bt_configs_dir() / name
    if not (src_dir / "backtest.json").exists():
        raise HTTPException(404, f"Config '{name}' not found")
    dst_dir = _bt_configs_dir() / new_name
    if dst_dir.exists():
        raise HTTPException(409, f"Config '{new_name}' already exists")
    import shutil
    shutil.copytree(str(src_dir), str(dst_dir))
    dst_cfg_file = dst_dir / "backtest.json"
    try:
        cfg = load_pb7_config(dst_cfg_file)
        _normalize_backtest_base_dir(cfg, new_name)
        save_pb7_config(cfg, dst_cfg_file)
    except Exception as exc:
        rmtree(str(dst_dir), ignore_errors=True)
        raise HTTPException(500, f"Failed to duplicate config: {exc}") from exc
    return {"ok": True, "name": new_name}


# ── REST: Queue ───────────────────────────────────────────────

@router.get("/queue")
def get_queue(session: SessionToken = Depends(require_auth)):
    """Get current queue items with status."""
    # Synchronous refresh for REST
    items = _load_queue_sync()
    return {"items": items}


def _load_queue_sync() -> list[dict]:
    """Load queue items from disk (sync version for REST endpoints)."""
    dest = _bt_queue_dir()
    dest.mkdir(parents=True, exist_ok=True)
    items = []
    for fp in sorted(dest.glob("*.json")):
        try:
            with open(fp, "r", encoding="utf-8") as f:
                data = json.load(f)
            filename = data.get("filename", fp.stem)
            pid = _store._read_pid(filename)
            log_path = _bt_log_dir() / f"{filename}.log"
            status = _store._determine_status(pid, log_path)
            items.append({
                "filename": filename,
                "name": data.get("name", filename),
                "json": data.get("json", ""),
                "exchange": data.get("exchange", ""),
                "archive_retest": data.get("archive_retest") if isinstance(data.get("archive_retest"), dict) else None,
                "status": status,
                "pid": pid,
                "log_path": str(log_path),
                "created": datetime.datetime.fromtimestamp(fp.stat().st_mtime).isoformat(),
            })
        except Exception as e:
            _log(SERVICE, f"Error loading queue item {fp}: {e}", level="WARNING")
    return items


@router.post("/queue")
def add_to_queue(body: dict, session: SessionToken = Depends(require_auth)):
    """Add a backtest config to the queue.

    Body: {name} or {name, config}.
    Without ``config``: uses the existing config at data/bt_v7/{name}/backtest.json
    directly (override files sit next to it, matching the historical layout).
    With ``config``: saves to data/bt_v7/{name}/backtest.json first (for re-backtest
    from results with modified params).
    """
    name = body.get("name", "")
    if not name:
        raise HTTPException(400, "name is required")
    _validate_name(name)

    cfg_dir = _bt_configs_dir() / name
    cfg_file = cfg_dir / "backtest.json"

    # If config body provided, save it first (re-backtest scenario)
    config = body.get("config")
    if config:
        cfg_dir.mkdir(parents=True, exist_ok=True)
        _normalize_backtest_base_dir(config, name)
        save_pb7_config(config, cfg_file)

    if not cfg_file.exists():
        raise HTTPException(404, f"Config '{name}' not found")

    # Repair older configs created before base_dir normalization before queueing.
    cfg = _load_and_repair_backtest_config(name, cfg_file)

    filename = str(uuid.uuid4())
    bt = cfg.get("backtest", {})
    exchanges = bt.get("exchanges", [])
    exchange_str = exchanges if isinstance(exchanges, list) else [exchanges]

    # Save queue metadata — json points to the original config file
    queue_dir = _bt_queue_dir()
    queue_dir.mkdir(parents=True, exist_ok=True)
    queue_file = queue_dir / f"{filename}.json"
    queue_data = {
        "name": name,
        "filename": filename,
        "json": str(cfg_file),
        "exchange": exchange_str,
    }
    with open(queue_file, "w", encoding="utf-8") as f:
        json.dump(queue_data, f, indent=4)

    _store.notify()
    return {"ok": True, "filename": filename}


@router.post("/queue/{filename}/start")
def start_queue_item(filename: str, session: SessionToken = Depends(require_auth)):
    """Manually start a single queued backtest."""
    _validate_name(filename)
    queue_file = _bt_queue_dir() / f"{filename}.json"
    if not queue_file.exists():
        raise HTTPException(404, "Queue item not found")

    with open(queue_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    item = {
        "filename": filename,
        "name": data.get("name", filename),
        "json": data.get("json", ""),
        "exchange": data.get("exchange", ""),
    }
    _worker._launch_backtest(item)
    _store.notify()
    return {"ok": True}


@router.post("/queue/{filename}/restart")
def restart_queue_item(filename: str, session: SessionToken = Depends(require_auth)):
    """Reset an errored queue item and launch it immediately."""
    _validate_name(filename)
    queue_file = _bt_queue_dir() / f"{filename}.json"
    if not queue_file.exists():
        raise HTTPException(404, "Queue item not found")
    with open(queue_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    data["status"] = "queued"
    data.pop("error", None)
    with open(queue_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    # Launch directly — don't rely on autostart being enabled
    item = {
        "filename": filename,
        "name": data.get("name", filename),
        "json": data.get("json", ""),
        "exchange": data.get("exchange", ""),
    }
    _worker._launch_backtest(item)
    _store.notify()
    return {"ok": True}


@router.post("/queue/{filename}/stop")
def stop_queue_item(filename: str, session: SessionToken = Depends(require_auth)):
    """Stop a running backtest."""
    _validate_name(filename)
    pid = _store._read_pid(filename)
    if pid and _store._is_process_running(pid):
        try:
            psutil.Process(pid).kill()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    _store.notify()
    return {"ok": True}


@router.delete("/queue/{filename}")
def remove_queue_item(filename: str, session: SessionToken = Depends(require_auth)):
    """Remove a queue item (stops if running)."""
    _validate_name(filename)
    # Stop if running
    pid = _store._read_pid(filename)
    if pid and _store._is_process_running(pid):
        try:
            psutil.Process(pid).kill()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    # Remove files
    (_bt_queue_dir() / f"{filename}.json").unlink(missing_ok=True)
    (_bt_queue_dir() / f"{filename}.pid").unlink(missing_ok=True)
    (_bt_log_dir() / f"{filename}.log").unlink(missing_ok=True)
    _store.notify()
    return {"ok": True}


@router.post("/queue/clear-finished")
def clear_finished(session: SessionToken = Depends(require_auth)):
    """Remove all finished queue items."""
    items = _load_queue_sync()
    removed = 0
    for item in items:
        if item["status"] == "complete":
            fn = item["filename"]
            (_bt_queue_dir() / f"{fn}.json").unlink(missing_ok=True)
            (_bt_queue_dir() / f"{fn}.pid").unlink(missing_ok=True)
            (_bt_log_dir() / f"{fn}.log").unlink(missing_ok=True)
            removed += 1
    _store.notify()
    return {"ok": True, "removed": removed}


@router.get("/queue/{filename}/log")
def get_queue_log(filename: str, lines: int = 100,
                  session: SessionToken = Depends(require_auth)):
    """Get tail of a backtest log."""
    _validate_name(filename)
    log_path = _bt_log_dir() / f"{filename}.log"
    if not log_path.exists():
        return {"log": "", "exists": False}
    tail = _store._read_log_tail(log_path, size_kb=max(10, lines))
    return {"log": tail or "", "exists": True}


# ── REST: Results ─────────────────────────────────────────────

@router.get("/results")
def list_results(name: str = None, session: SessionToken = Depends(require_auth)):
    """List backtest results. If name given, only for that config."""
    base = Path(_bt_results_base())
    if not base.exists():
        return {"results": []}

    results = []
    search_dirs = [base / name] if name else [d for d in base.iterdir() if d.is_dir()]

    for config_dir in search_dirs:
        if not config_dir.exists():
            continue
        for analysis_file in config_dir.glob("**/analysis.json"):
            result_dir = analysis_file.parent
            try:
                with open(analysis_file, "r", encoding="utf-8") as f:
                    analysis = json.load(f)
                config_file = result_dir / "config.json"
                config_data = {}
                if config_file.exists():
                    with open(config_file, "r", encoding="utf-8") as f:
                        config_data = json.load(f)

                bt = config_data.get("backtest", {})
                bot = config_data.get("bot", {})

                # Support old & new analysis key formats
                adg = analysis.get("adg_usd", analysis.get("adg", 0))
                drawdown = analysis.get("drawdown_worst_usd", analysis.get("drawdown_worst", 0))
                sharpe = analysis.get("sharpe_ratio_usd", analysis.get("sharpe_ratio", 0))
                eqbal_diff = analysis.get(
                    "equity_balance_diff_neg_max_usd",
                    analysis.get("equity_balance_diff_neg_max", 0)
                )
                gain = analysis.get("gain_usd", analysis.get("gain", 0))
                starting_balance = bt.get("starting_balance", 0)
                final_balance = starting_balance * gain if starting_balance else 0

                # Liquidation detection: use passivbot's flag if available,
                # fall back to heuristic for older results
                liq_threshold = bt.get("liquidation_threshold", 0.05)
                if "liquidated" in analysis:
                    liquidated = bool(analysis["liquidated"])
                else:
                    liquidated = (
                        drawdown >= 0.95
                        or eqbal_diff >= 0.95
                        or (starting_balance > 0 and final_balance < starting_balance * liq_threshold)
                    )

                results.append({
                    "path": str(result_dir),
                    "display_name": str(result_dir.relative_to(base)),
                    "config_name": config_dir.name,
                    "result_name": result_dir.name,
                    "exchange_dir": result_dir.parent.name,
                    "adg": adg,
                    "drawdown_worst": drawdown,
                    "sharpe_ratio": sharpe,
                    "equity_balance_diff_neg_max": eqbal_diff,
                    "gain": gain,
                    "starting_balance": starting_balance,
                    "final_balance": final_balance,
                    "liquidated": liquidated,
                    "exchanges": bt.get("exchanges", []),
                    "start_date": bt.get("start_date", ""),
                    "end_date": bt.get("end_date", ""),
                    "btc_collateral_cap": float(bt.get("btc_collateral_cap") or 0),
                    "twe_long": bot.get("long", {}).get("total_wallet_exposure_limit", 0),
                    "twe_short": bot.get("short", {}).get("total_wallet_exposure_limit", 0),
                    "pos_long": bot.get("long", {}).get("n_positions", 0),
                    "pos_short": bot.get("short", {}).get("n_positions", 0),
                    "modified": datetime.datetime.fromtimestamp(
                        analysis_file.stat().st_mtime
                    ).isoformat(),
                    "analysis": analysis,
                })
            except Exception as e:
                _log(SERVICE, f"Error reading result {result_dir}: {e}", level="WARNING")

    return {"results": results}


@router.get("/legacy/results")
def list_legacy_results(session: SessionToken = Depends(require_auth)):
    """List legacy results found under pb7/backtests/* outside pbgui."""
    root = _bt_results_root().resolve()
    if not root.exists():
        return {"results": []}

    results = []
    for source_dir in _legacy_results_roots():
        source_name = source_dir.name
        for analysis_file in source_dir.glob("**/analysis.json"):
            result_dir = analysis_file.parent
            try:
                with open(analysis_file, "r", encoding="utf-8") as f:
                    analysis = json.load(f)
                config_file = result_dir / "config.json"
                config_data = {}
                if config_file.exists():
                    with open(config_file, "r", encoding="utf-8") as f:
                        config_data = json.load(f)

                bt = config_data.get("backtest", {})
                bot = config_data.get("bot", {})

                adg = analysis.get("adg_usd", analysis.get("adg", 0))
                drawdown = analysis.get("drawdown_worst_usd", analysis.get("drawdown_worst", 0))
                sharpe = analysis.get("sharpe_ratio_usd", analysis.get("sharpe_ratio", 0))
                eqbal_diff = analysis.get(
                    "equity_balance_diff_neg_max_usd",
                    analysis.get("equity_balance_diff_neg_max", 0)
                )
                gain = analysis.get("gain_usd", analysis.get("gain", 0))
                starting_balance = bt.get("starting_balance", 0)
                final_balance = starting_balance * gain if starting_balance else 0

                liq_threshold = bt.get("liquidation_threshold", 0.05)
                if "liquidated" in analysis:
                    liquidated = bool(analysis["liquidated"])
                else:
                    liquidated = (
                        drawdown >= 0.95
                        or eqbal_diff >= 0.95
                        or (starting_balance > 0 and final_balance < starting_balance * liq_threshold)
                    )

                base_dir_val = str(bt.get("base_dir") or "").strip()
                base_dir_name = Path(base_dir_val).name if base_dir_val else ""
                if base_dir_name and base_dir_name != "backtests":
                    config_name = base_dir_name
                    suggested_name = base_dir_name
                else:
                    config_name = f"Legacy {source_name}"
                    suggested_name = f"legacy_{source_name}_{result_dir.name}"

                results.append({
                    "path": str(result_dir),
                    "display_name": str(result_dir.relative_to(root)),
                    "config_name": config_name,
                    "result_name": result_dir.name,
                    "exchange_dir": source_name,
                    "suggested_name": suggested_name,
                    "adg": adg,
                    "drawdown_worst": drawdown,
                    "sharpe_ratio": sharpe,
                    "equity_balance_diff_neg_max": eqbal_diff,
                    "gain": gain,
                    "starting_balance": starting_balance,
                    "final_balance": final_balance,
                    "liquidated": liquidated,
                    "exchanges": bt.get("exchanges", []),
                    "start_date": bt.get("start_date", ""),
                    "end_date": bt.get("end_date", ""),
                    "btc_collateral_cap": float(bt.get("btc_collateral_cap") or 0),
                    "twe_long": bot.get("long", {}).get("total_wallet_exposure_limit", 0),
                    "twe_short": bot.get("short", {}).get("total_wallet_exposure_limit", 0),
                    "pos_long": bot.get("long", {}).get("n_positions", 0),
                    "pos_short": bot.get("short", {}).get("n_positions", 0),
                    "modified": datetime.datetime.fromtimestamp(
                        analysis_file.stat().st_mtime
                    ).isoformat(),
                    "analysis": analysis,
                })
            except Exception as e:
                _log(SERVICE, f"Error reading legacy result {result_dir}: {e}", level="WARNING")

    return {"results": results}


@router.get("/results/analysis")
def get_result_analysis(path: str, session: SessionToken = Depends(require_auth)):
    """Get full analysis.json for a result. Path is the result directory."""
    result_dir = _resolve_result_dir(path)
    analysis_file = result_dir / "analysis.json"
    if not analysis_file.exists():
        raise HTTPException(404, "analysis.json not found")
    with open(analysis_file, "r", encoding="utf-8") as f:
        return json.load(f)


@router.get("/results/config")
def get_result_config(path: str, session: SessionToken = Depends(require_auth)):
    """Get config.json for a result, with missing params neutralized."""
    result_dir = _resolve_result_dir(path)
    config_file = result_dir / "config.json"
    if not config_file.exists():
        raise HTTPException(404, "config.json not found")
    return load_pb7_config(config_file, neutralize_added=True)


@router.get("/results/equity")
def get_result_equity(path: str, session: SessionToken = Depends(require_auth)):
    """Stream balance_and_equity CSV file directly for client-side parsing."""
    result_dir = _resolve_result_dir(path)

    csv_file = result_dir / "balance_and_equity.csv"
    gz_file = result_dir / "balance_and_equity.csv.gz"

    if csv_file.exists():
        return FileResponse(str(csv_file), media_type="text/csv",
                            headers={"Cache-Control": "max-age=3600"})
    elif gz_file.exists():
        return FileResponse(str(gz_file), media_type="text/csv",
                            headers={"Content-Encoding": "gzip",
                                     "Cache-Control": "max-age=3600"})
    else:
        raise HTTPException(404, "balance_and_equity data not found")


@router.get("/results/fills")
def get_result_fills(path: str, session: SessionToken = Depends(require_auth)):
    """Stream fills CSV file directly for client-side parsing."""
    result_dir = _resolve_result_dir(path)

    csv_file = result_dir / "fills.csv"
    gz_file = result_dir / "fills.csv.gz"

    if csv_file.exists():
        return FileResponse(str(csv_file), media_type="text/csv",
                            headers={"Cache-Control": "max-age=3600"})
    elif gz_file.exists():
        return FileResponse(str(gz_file), media_type="text/csv",
                            headers={"Content-Encoding": "gzip",
                                     "Cache-Control": "max-age=3600"})
    else:
        raise HTTPException(404, "fills data not found")


@router.get("/results/files")
def list_result_files(path: str, session: SessionToken = Depends(require_auth)):
    """List all files in a result directory (for UI to know what's available)."""
    result_dir = _resolve_result_dir(path)
    if not result_dir.exists():
        raise HTTPException(404, "Result not found")
    files = []
    for f in sorted(result_dir.rglob("*")):
        if f.is_file():
            files.append(str(f.relative_to(result_dir)))
    return {"files": files}


@router.get("/results/image")
def get_result_image(path: str, filename: str,
                     session: SessionToken = Depends(require_auth)):
    """Serve a PNG image from a result directory."""
    result_dir = _resolve_result_dir(path)
    # Security: prevent path traversal
    if ".." in filename:
        raise HTTPException(400, "Invalid filename")
    img_path = (result_dir / filename).resolve()
    if not img_path.is_relative_to(result_dir.resolve()):
        raise HTTPException(400, "Invalid filename")
    if not img_path.exists() or not img_path.is_file():
        raise HTTPException(404, "Image not found")
    media = "image/png" if img_path.suffix == ".png" else "application/octet-stream"
    return FileResponse(str(img_path), media_type=media,
                        headers={"Cache-Control": "max-age=3600"})


@router.delete("/results")
def delete_result(path: str, session: SessionToken = Depends(require_auth)):
    """Delete a single result directory."""
    result_dir = _resolve_result_dir(path, allow_legacy=False, allow_archives=False)
    if not result_dir.exists():
        return {"ok": True, "missing": True}
    rmtree(str(result_dir), ignore_errors=True)
    return {"ok": True, "missing": False}


@router.delete("/legacy/results")
def delete_legacy_result(path: str, session: SessionToken = Depends(require_auth)):
    """Delete a single legacy result directory."""
    result_dir = _resolve_result_dir(path, allow_pbgui=False, allow_legacy=True, allow_archives=False)
    if not result_dir.exists():
        raise HTTPException(404, "Result not found")
    legacy_root = _find_legacy_result_root(result_dir)
    rmtree(str(result_dir), ignore_errors=True)
    parent = result_dir.parent
    while parent != legacy_root and parent.is_dir():
        try:
            parent.rmdir()
        except OSError:
            break
        parent = parent.parent
    return {"ok": True}


# ── REST: Archives ────────────────────────────────────────────

@router.get("/archives")
def list_archives(session: SessionToken = Depends(require_auth)):
    """List configured git archives."""
    cached = _archives_list_cache.get("payload")
    if cached and time.time() - float(cached.get("ts", 0) or 0) <= _ARCHIVE_LIST_CACHE_TTL:
        return cached["data"]
    base = _archives_dir()
    own_archive = _own_archive_name()
    archives = []
    if base.exists():
        for d in sorted(base.iterdir()):
            git_config = d / ".git" / "config"
            if git_config.exists():
                # Parse remote URL
                url = ""
                try:
                    cfg = configparser.ConfigParser()
                    cfg.read(str(git_config))
                    url = cfg.get('remote "origin"', "url", fallback="")
                except Exception:
                    pass
                migration = archive_migration_status(d, fast=True)
                manifest = load_archive_manifest(d)
                if manifest:
                    result_count = len([item for item in manifest["items"] if item.get("type") == "backtest_result"])
                    optimize_count = len([item for item in manifest["items"] if item.get("type") == "optimize_config"])
                else:
                    cached = _get_cached_archive_results(d.name)
                    result_count = len(cached["results"]) if cached else 0
                    optimize_count = 0
                archives.append({
                    "name": d.name,
                    "path": str(d),
                    "url": url,
                    "configs": result_count,
                    "results": result_count,
                    "optimize_configs": optimize_count,
                    "is_own": d.name == own_archive,
                    "migration_status": migration,
                    "manifest": {"present": bool(manifest), "items": len(manifest["items"]) if manifest else 0},
                })
    payload = {"archives": archives}
    _archives_list_cache["payload"] = {"ts": time.time(), "data": payload}
    return payload


@router.get("/archives/{name}/results")
def list_archive_results(name: str, session: SessionToken = Depends(require_auth)):
    """List results in an archive (same format as /results)."""
    _validate_name(name)
    archive_dir = _archives_dir() / name
    if not archive_dir.exists():
        raise HTTPException(404, f"Archive '{name}' not found")
    migration = maybe_migrate_own_archive(name, archive_dir, _own_archive_name())
    if (migration.get("result") or {}).get("migrated") or (migration.get("result") or {}).get("removed_duplicates"):
        _invalidate_archive_cache(name)
        rebuild_archive_manifest(archive_dir)
    migration_status = migration.get("status") or archive_migration_status(archive_dir)
    cached = _get_cached_archive_results(name)
    if cached:
        return {"results": cached["results"], "migration_status": cached.get("migration_status") or migration_status, "cached": True}
    results = score_archive_results(list_archive_backtest_results(archive_dir))
    _set_cached_archive_results(name, results, migration_status)
    return {"results": results, "migration_status": migration_status, "cached": False}


@router.post("/archives")
def create_archive(body: dict, session: SessionToken = Depends(require_auth)):
    """Clone a git repo as archive."""
    name = body.get("name", "")
    url = body.get("url", "")
    if not name or not url:
        raise HTTPException(400, "name and url are required")
    _validate_name(name)

    dest = _archives_dir() / name
    if dest.exists():
        raise HTTPException(409, f"Archive '{name}' already exists")

    _archives_dir().mkdir(parents=True, exist_ok=True)
    try:
        result = subprocess.run(
            ["git", "clone", url, str(dest)],
            capture_output=True, text=True, timeout=120
        )
        if result.returncode != 0:
            raise HTTPException(500, f"git clone failed: {result.stderr}")
    except subprocess.TimeoutExpired:
        raise HTTPException(500, "git clone timed out")

    _invalidate_archive_cache()
    return {"ok": True, "name": name}


@router.delete("/archives/{name}")
def delete_archive(name: str, session: SessionToken = Depends(require_auth)):
    _validate_name(name)
    dest = _archives_dir() / name
    if not dest.exists():
        raise HTTPException(404, f"Archive '{name}' not found")
    rmtree(str(dest), ignore_errors=True)
    _invalidate_archive_cache(name)
    return {"ok": True}


@router.delete("/archives/{name}/results")
def delete_archive_result(name: str, path: str, session: SessionToken = Depends(require_auth)):
    """Delete a single result directory from an archive."""
    _validate_name(name)
    result_dir = Path(path).resolve()
    archive_base = (_archives_dir() / name).resolve()
    if not is_inside_archive(result_dir, archive_base):
        raise HTTPException(400, "Invalid result path")
    if not result_dir.exists():
        raise HTTPException(404, "Result not found")
    rmtree(str(result_dir), ignore_errors=True)
    # Remove empty parent directories up to (but not including) the archive root
    parent = result_dir.parent
    while parent != archive_base and parent.is_dir():
        try:
            parent.rmdir()  # only succeeds if directory is empty
        except OSError:
            break  # not empty — stop climbing
        parent = parent.parent
    _invalidate_archive_cache(name)
    rebuild_archive_manifest(archive_base)
    return {"ok": True}


@router.post("/archives/{name}/pull")
def git_pull(name: str, session: SessionToken = Depends(require_auth)):
    """Pull a single archive."""
    _validate_name(name)
    dest = _archives_dir() / name
    if not dest.exists():
        raise HTTPException(404, f"Archive '{name}' not found")
    try:
        result = subprocess.run(
            ["git", "pull"], cwd=str(dest),
            capture_output=True, text=True, timeout=60
        )
        output = (result.stdout + result.stderr).strip()
        _log_archive(f"[{name}] git pull: {output or 'ok'}")
        _invalidate_archive_cache(name)
        return {"ok": True, "output": output}
    except subprocess.TimeoutExpired:
        raise HTTPException(500, "git pull timed out")


@router.post("/archives/pull-all")
def pull_all_archives(session: SessionToken = Depends(require_auth)):
    """Pull all cloned archives."""
    _log_archive("Manual pull-all triggered")
    results = _pull_all_archives_sync()
    _invalidate_archive_cache()
    return {"ok": True, "results": results}


@router.post("/archives/{name}/push")
def git_push(name: str, body: dict = None, session: SessionToken = Depends(require_auth)):
    """Git pull + add + commit + push for own archive.
    Accepts optional access_token to inject into the HTTPS remote URL for auth.
    Pass dry_run=true to test credentials without actually pushing.
    """
    _validate_name(name)
    dest = _archives_dir() / name
    if not dest.exists():
        raise HTTPException(404, f"Archive '{name}' not found")

    body = body or {}
    username = body.get("username", "")
    email = body.get("email", "")
    access_token = body.get("access_token", "")
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    message = body.get("message", f"Update {name} at {timestamp}")
    dry_run = bool(body.get("dry_run", False))

    log_lines = []

    try:
        _log_archive(f"[{name}] git push requested{' (dry-run)' if dry_run else ''}")
        if username:
            subprocess.run(["git", "config", "user.name", username], cwd=str(dest),
                           capture_output=True, timeout=10)
        if email:
            subprocess.run(["git", "config", "user.email", email], cwd=str(dest),
                           capture_output=True, timeout=10)

        if not dry_run:
            # Pull before push to avoid overwriting remote archive updates.
            pull_result, pull_out = _run_archive_git_step(
                name, dest, "git pull (pre-push)", ["git", "pull"], timeout=60, secret=access_token,
            )
            log_lines.append(f"Git pull:\n{pull_out}")
            if pull_result.returncode != 0:
                raise HTTPException(500, f"git pull failed: {pull_out}")

            _log_archive(f"[{name}] archive migration check started")
            migration = maybe_migrate_own_archive(name, dest, _own_archive_name())
            if migration.get("result"):
                migration_result = migration["result"]
                if migration_result.get("skipped"):
                    log_lines.append(f"Archive migration skipped: {migration_result.get('reason', 'unknown')}")
                    _log_archive(f"[{name}] archive migration skipped: {migration_result.get('reason', 'unknown')}")
                elif migration_result.get("migrated") or migration_result.get("removed_duplicates"):
                    _invalidate_archive_cache(name)
                    rebuild_archive_manifest(dest)
                    log_lines.append(
                        "Archive migration: "
                        f"{migration_result.get('migrated', 0)} moved, "
                        f"{migration_result.get('removed_duplicates', 0)} duplicate(s) removed"
                    )
                    _log_archive(
                        f"[{name}] archive migration complete: "
                        f"{migration_result.get('migrated', 0)} moved, "
                        f"{migration_result.get('removed_duplicates', 0)} duplicate(s) removed"
                    )
                else:
                    _log_archive(f"[{name}] archive migration check complete: no changes")
            else:
                _log_archive(f"[{name}] archive migration check complete: no changes")

            _log_archive(f"[{name}] archive scoring started")
            score_payload = update_archive_scores_and_readme(dest)
            log_lines.append(
                "Archive scores:\n"
                f"{score_payload.get('scored', 0)} result(s) scored, "
                "manifest and README updated"
            )
            _invalidate_archive_cache(name)
            _log_archive(f"[{name}] archive scoring complete: {score_payload.get('scored', 0)} result(s)")

            add_result, add_out = _run_archive_git_step(
                name, dest, "git add", ["git", "add", "-A"], timeout=30, secret=access_token,
            )
            log_lines.append(f"Git add:\n{add_out}")
            if add_result.returncode != 0:
                raise HTTPException(500, f"git add failed: {add_out}")

            commit_result, commit_out = _run_archive_git_step(
                name, dest, "git commit", ["git", "commit", "-m", message],
                timeout=30, secret=access_token, ok_returncodes=(0, 1),
            )
            log_lines.append(f"Git commit:\n{commit_out}")

        # Build push command — inject access token into HTTPS URL when provided
        push_url = _archive_push_url(dest, access_token)

        push_cmd = ["git", "push"]
        if dry_run:
            push_cmd.append("--dry-run")
        if push_url:
            push_cmd.append(push_url)

        result, push_out = _run_archive_git_step(
            name, dest, f"git push{' (dry-run)' if dry_run else ''}", push_cmd,
            timeout=120, secret=access_token,
        )
        log_lines.append(f"Git push:\n{push_out}")

        if result.returncode != 0:
            raise HTTPException(500, f"git push failed: {push_out}")
        _invalidate_archive_cache(name)
        _log_archive(f"[{name}] git push complete")
        return {"ok": True, "output": "\n\n".join(log_lines)}
    except HTTPException:
        raise
    except subprocess.TimeoutExpired:
        raise HTTPException(500, "git operation timed out")


@router.post("/archives/{name}/compact")
def compact_archive_history(name: str, body: dict = None, session: SessionToken = Depends(require_auth)):
    """Compact own archive git history into one root commit and force-push with lease."""
    _validate_name(name)
    if name != _own_archive_name():
        raise HTTPException(403, "Archive history compaction is only allowed for the configured own archive")
    dest = _archives_dir() / name
    if not dest.exists():
        raise HTTPException(404, f"Archive '{name}' not found")

    body = body or {}
    dry_run = bool(body.get("dry_run", True))

    if dry_run:
        preview = _archive_compact_preview(name, dest, str(body.get("access_token") or ""))
        return {"ok": True, "dry_run": True, **preview}
    return _compact_archive_history(name, dest, body)


@router.get("/archives/{name}/scores/preview")
def preview_archive_scores(name: str, session: SessionToken = Depends(require_auth)):
    """Return a read-only PBGui score preview for one archive."""
    _validate_name(name)
    archive_dir = _archives_dir() / name
    if not archive_dir.exists():
        raise HTTPException(404, f"Archive '{name}' not found")
    return build_archive_score_payload(archive_dir)


@router.post("/archives/{name}/scores/rebuild")
def rebuild_archive_scores(name: str, session: SessionToken = Depends(require_auth)):
    """Recalculate scores and update manifest/README for the configured own archive."""
    _validate_name(name)
    if name != _own_archive_name():
        raise HTTPException(403, "Score rebuild is only available for the configured own archive")
    archive_dir = _archives_dir() / name
    if not archive_dir.exists():
        raise HTTPException(404, f"Archive '{name}' not found")
    payload = update_archive_scores_and_readme(archive_dir)
    _invalidate_archive_cache(name)
    return payload


@router.post("/archives/{name}/add-config")
def add_config_to_archive(name: str, body: dict,
                          session: SessionToken = Depends(require_auth)):
    """Copy a result directory into an archive using the versioned layout."""
    _validate_name(name)
    source_path = body.get("source_path", "")
    if not source_path:
        raise HTTPException(400, "source_path is required")

    src = Path(source_path)
    if not src.exists():
        raise HTTPException(404, "Source path not found")

    # Security: validate source is under current results, legacy results, or archives
    src = _resolve_result_dir(src)

    archive_dir = _archives_dir() / name
    if not archive_dir.exists():
        raise HTTPException(404, f"Archive '{name}' not found")

    migration = maybe_migrate_own_archive(name, archive_dir, _own_archive_name())
    copied = copy_backtest_result_to_archive(src, archive_dir)
    copied["migration_status"] = migration.get("status") or archive_migration_status(archive_dir)
    _invalidate_archive_cache(name)
    copied["manifest"] = rebuild_archive_manifest(archive_dir)
    return copied


@router.post("/archives/{name}/migrate")
def migrate_archive(name: str, session: SessionToken = Depends(require_auth)):
    """Explicitly migrate the configured own archive to the versioned layout."""
    _validate_name(name)
    if name != _own_archive_name():
        raise HTTPException(403, "Only the configured own archive can be migrated")
    archive_dir = _archives_dir() / name
    if not archive_dir.exists():
        raise HTTPException(404, f"Archive '{name}' not found")
    result = migrate_archive_layout(archive_dir)
    if not result.get("skipped"):
        _invalidate_archive_cache(name)
        result["manifest"] = rebuild_archive_manifest(archive_dir)
    return result


@router.post("/archives/{name}/add-optimize-config")
def add_optimize_config_to_archive(name: str, body: dict, session: SessionToken = Depends(require_auth)):
    """Copy an Optimize config into the versioned archive layout."""
    _validate_name(name)
    config_name = str((body or {}).get("config_name") or "").strip()
    _validate_name(config_name)
    archive_dir = _archives_dir() / name
    if not archive_dir.exists():
        raise HTTPException(404, f"Archive '{name}' not found")
    cfg_file = _opt_archive_configs_dir() / f"{config_name}.json"
    if not cfg_file.exists():
        raise HTTPException(404, f"Optimize config '{config_name}' not found")
    try:
        cfg = load_pb7_config(cfg_file)
        cfg = ensure_config_version(cfg, get_template_config)
        dest, meta, skipped = resolve_optimize_archive_destination(archive_dir, config_name, cfg)
        if not skipped:
            dest.parent.mkdir(parents=True, exist_ok=True)
            save_pb7_config(cfg, dest)
        meta_path = dest.with_name(dest.stem + ".meta.json")
        write_optimize_meta(meta_path, meta)
        _invalidate_archive_cache(name)
        manifest = rebuild_archive_manifest(archive_dir)
        return {"ok": True, "path": str(dest), "relative_path": str(dest.relative_to(archive_dir)), "skipped": skipped, "meta": meta, "manifest": manifest}
    except HTTPException:
        raise
    except Exception as exc:
        _log(SERVICE, f"Failed to archive optimize config {config_name}: {exc}", level="WARNING", meta={"traceback": traceback.format_exc()})
        raise HTTPException(500, f"Failed to archive optimize config: {exc}") from exc


@router.get("/archives/{name}/optimize-configs")
def list_archive_optimize(name: str, session: SessionToken = Depends(require_auth)):
    """List Optimize configs stored in an archive."""
    _validate_name(name)
    archive_dir = _archives_dir() / name
    if not archive_dir.exists():
        raise HTTPException(404, f"Archive '{name}' not found")
    return {"configs": list_archive_optimize_configs(archive_dir)}


@router.get("/archives/{name}/optimize-configs/config")
def get_archive_optimize_config(name: str, path: str, session: SessionToken = Depends(require_auth)):
    """Load one archived Optimize config JSON."""
    _validate_name(name)
    archive_dir = (_archives_dir() / name).resolve()
    config_file = Path(path).resolve()
    if not is_inside_archive(config_file, archive_dir):
        raise HTTPException(400, "Invalid optimize config path")
    if not config_file.exists() or not config_file.is_file():
        raise HTTPException(404, "Optimize config not found")
    return load_pb7_config(config_file, neutralize_added=True)


@router.post("/archives/{name}/optimize-configs/import")
def import_archive_optimize_config(name: str, body: dict, session: SessionToken = Depends(require_auth)):
    """Import one archived Optimize config into local Optimize configs."""
    _validate_name(name)
    archive_dir = (_archives_dir() / name).resolve()
    source_path = str((body or {}).get("path") or "").strip()
    if not source_path:
        raise HTTPException(400, "path is required")
    source_file = Path(source_path).resolve()
    if not is_inside_archive(source_file, archive_dir):
        raise HTTPException(400, "Invalid optimize config path")
    if not source_file.exists() or not source_file.is_file():
        raise HTTPException(404, "Optimize config not found")
    import_name = str((body or {}).get("name") or source_file.stem).strip()
    _validate_name(import_name)
    overwrite = bool((body or {}).get("overwrite", False))
    target_file = _opt_archive_configs_dir() / f"{import_name}.json"
    if target_file.exists() and not overwrite:
        raise HTTPException(409, f"Optimize config '{import_name}' already exists")
    cfg = load_pb7_config(source_file, neutralize_added=True)
    cfg = ensure_config_version(cfg, get_template_config)
    save_pb7_config(cfg, target_file)
    return {"ok": True, "name": import_name}


@router.delete("/archives/{name}/optimize-configs/config")
def delete_archive_optimize_config(name: str, path: str, session: SessionToken = Depends(require_auth)):
    """Delete one archived Optimize config from the configured own archive."""
    _validate_name(name)
    if name != _own_archive_name():
        raise HTTPException(403, "Optimize config deletion is only available for the configured own archive")
    archive_dir = (_archives_dir() / name).resolve()
    if not archive_dir.exists():
        raise HTTPException(404, f"Archive '{name}' not found")
    config_file = Path(path).resolve()
    if not is_inside_archive(config_file, archive_dir):
        raise HTTPException(400, "Invalid optimize config path")
    try:
        rel_parts = config_file.relative_to(archive_dir).parts
    except ValueError:
        raise HTTPException(400, "Invalid optimize config path")
    if len(rel_parts) != 5 or rel_parts[0] != "pbgui" or rel_parts[1] != "configs" or rel_parts[3] != "optimize":
        raise HTTPException(400, "Invalid optimize config path")
    if config_file.name.endswith(".meta.json") or config_file.suffix.lower() != ".json":
        raise HTTPException(400, "Invalid optimize config path")
    if not config_file.exists() or not config_file.is_file():
        raise HTTPException(404, "Optimize config not found")
    meta_file = config_file.with_name(config_file.stem + ".meta.json")
    try:
        config_file.unlink()
        if meta_file.exists() and meta_file.is_file() and is_inside_archive(meta_file, archive_dir):
            meta_file.unlink()
        cleanup_empty_parents(config_file, archive_dir)
        _invalidate_archive_cache(name)
        manifest = rebuild_archive_manifest(archive_dir)
        return {"ok": True, "relative_path": str(config_file.relative_to(archive_dir)), "manifest": manifest}
    except HTTPException:
        raise
    except Exception as exc:
        _log(SERVICE, f"Failed to delete archived optimize config {config_file}: {exc}", level="WARNING", meta={"traceback": traceback.format_exc()})
        raise HTTPException(500, f"Failed to delete optimize config: {exc}") from exc


@router.post("/archives/{name}/results/rebacktest")
def rebacktest_archive_results(name: str, body: dict, session: SessionToken = Depends(require_auth)):
    """Queue local backtests from archived result configs without mutating the archive."""
    _validate_name(name)
    archive_dir = (_archives_dir() / name).resolve()
    if not archive_dir.exists():
        raise HTTPException(404, f"Archive '{name}' not found")
    paths = (body or {}).get("paths") or []
    if not isinstance(paths, list) or not paths:
        raise HTTPException(400, "paths must be a non-empty list")
    overrides = (body or {}).get("overrides") or {}
    if not isinstance(overrides, dict):
        overrides = {}
    queue_items = []
    for raw_path in paths:
        result_dir = Path(str(raw_path)).resolve()
        if not is_inside_archive(result_dir, archive_dir):
            raise HTTPException(400, "Invalid result path")
        cfg_file = result_dir / "config.json"
        if not cfg_file.exists():
            raise HTTPException(404, f"config.json not found: {result_dir}")
        cfg = load_pb7_config(cfg_file, neutralize_added=True)
        backtest = cfg.setdefault("backtest", {})
        if overrides.get("start_date"):
            backtest["start_date"] = overrides["start_date"]
        if overrides.get("end_date"):
            backtest["end_date"] = overrides["end_date"]
        if overrides.get("starting_balance") not in (None, ""):
            backtest["starting_balance"] = overrides["starting_balance"]
        if isinstance(overrides.get("exchanges"), list) and overrides["exchanges"]:
            backtest["exchanges"] = overrides["exchanges"]
        if overrides.get("use_pbgui_market_data"):
            _apply_pbgui_market_data_override(cfg, True)
        base_dir = str(backtest.get("base_dir") or "").strip()
        queue_name = Path(base_dir).name if base_dir else result_dir.parent.parent.name
        queued = add_to_queue({"name": queue_name, "config": cfg}, session=session)
        queue_items.append(queued)
    return {"ok": True, "queued": len(queue_items), "queue_items": queue_items}


@router.post("/archives/{name}/results/retest-replace")
def retest_replace_archive_results(name: str, body: dict, session: SessionToken = Depends(require_auth)):
    """Queue archive retests that replace the original archive result after success."""
    _validate_name(name)
    if name != _own_archive_name():
        raise HTTPException(403, "Retest & Replace is only allowed for the configured own archive")
    archive_dir = (_archives_dir() / name).resolve()
    if not archive_dir.exists():
        raise HTTPException(404, f"Archive '{name}' not found")
    paths = (body or {}).get("paths") or []
    if not isinstance(paths, list) or not paths:
        raise HTTPException(400, "paths must be a non-empty list")
    options = _archive_retest_options(body)
    runs = _load_archive_retest_runs()
    queued = []
    for raw_path in paths:
        result_dir = Path(str(raw_path)).resolve()
        if not is_inside_archive(result_dir, archive_dir):
            raise HTTPException(400, "Invalid result path")
        if not result_dir.exists():
            raise HTTPException(404, f"Archive result not found: {result_dir}")
        run = _queue_archive_retest_run(name, archive_dir, result_dir, options)
        runs.append(run)
        queued.append(run)
    _save_archive_retest_runs(runs)
    return {"ok": True, "queued": len(queued), "runs": queued}


@router.get("/archives/{name}/retest-schedules")
def list_archive_retest_schedules(name: str, session: SessionToken = Depends(require_auth)):
    """List scheduled retests and recent runs for one archive."""
    _validate_name(name)
    schedules = [s for s in _load_archive_retest_schedules() if s.get("archive_name") == name]
    runs = [r for r in _load_archive_retest_runs() if r.get("archive_name") == name]
    runs = sorted(runs, key=lambda r: str(r.get("created_at") or ""), reverse=True)[:50]
    return {"schedules": schedules, "runs": runs}


@router.post("/archives/{name}/retest-schedules")
def create_archive_retest_schedule(name: str, body: dict, session: SessionToken = Depends(require_auth)):
    """Create a daily or weekly archive retest schedule for selected archive results."""
    _validate_name(name)
    if name != _own_archive_name():
        raise HTTPException(403, "Scheduled retests are only allowed for the configured own archive")
    archive_dir = (_archives_dir() / name).resolve()
    if not archive_dir.exists():
        raise HTTPException(404, f"Archive '{name}' not found")
    paths = (body or {}).get("paths") or []
    if not isinstance(paths, list) or not paths:
        raise HTTPException(400, "paths must be a non-empty list")
    cadence = str((body or {}).get("cadence") or "daily")
    if cadence not in {"daily", "weekly"}:
        cadence = "daily"
    try:
        weekday = max(0, min(6, int((body or {}).get("weekday", 0))))
    except (TypeError, ValueError):
        weekday = 0
    schedule_id = uuid.uuid4().hex
    targets = []
    for raw_path in paths:
        result_dir = Path(str(raw_path)).resolve()
        if not is_inside_archive(result_dir, archive_dir):
            raise HTTPException(400, "Invalid result path")
        if not result_dir.exists():
            raise HTTPException(404, f"Archive result not found: {result_dir}")
        targets.append({
            "id": uuid.uuid4().hex,
            "relative_path": result_dir.relative_to(archive_dir).as_posix(),
            "label": result_dir.name,
        })
    schedule = {
        "id": schedule_id,
        "archive_name": name,
        "enabled": bool((body or {}).get("enabled", True)),
        "cadence": cadence,
        "time": str((body or {}).get("time") or "02:00"),
        "weekday": weekday,
        "targets": targets,
        "options": _archive_retest_options(body),
        "created_at": utc_now_iso(),
        "last_status": "created",
        "last_message": "",
    }
    schedule["next_run_at"] = _next_archive_retest_run_at(schedule)
    schedules = _load_archive_retest_schedules()
    schedules.append(schedule)
    _save_archive_retest_schedules(schedules)
    return {"ok": True, "schedule": schedule}


@router.post("/archives/{name}/retest-schedules/{schedule_id}/run")
def run_archive_retest_schedule_now(name: str, schedule_id: str, session: SessionToken = Depends(require_auth)):
    """Queue all targets of one archive retest schedule immediately."""
    _validate_name(name)
    _validate_name(schedule_id)
    if name != _own_archive_name():
        raise HTTPException(403, "Scheduled retests are only allowed for the configured own archive")
    schedules = _load_archive_retest_schedules()
    for schedule in schedules:
        if schedule.get("id") == schedule_id and schedule.get("archive_name") == name:
            try:
                result = _queue_archive_retest_schedule(schedule, force=True)
            except Exception as exc:
                raise HTTPException(500, str(exc)) from exc
            schedule["last_run_at"] = utc_now_iso()
            _save_archive_retest_schedules(schedules)
            return {"ok": True, **result}
    raise HTTPException(404, "Schedule not found")


@router.post("/archives/{name}/retest-schedules/{schedule_id}/toggle")
def toggle_archive_retest_schedule(name: str, schedule_id: str, body: dict = None, session: SessionToken = Depends(require_auth)):
    """Enable or disable one archive retest schedule."""
    _validate_name(name)
    _validate_name(schedule_id)
    if name != _own_archive_name():
        raise HTTPException(403, "Scheduled retests are only allowed for the configured own archive")
    schedules = _load_archive_retest_schedules()
    for schedule in schedules:
        if schedule.get("id") == schedule_id and schedule.get("archive_name") == name:
            if isinstance(body, dict) and "enabled" in body:
                schedule["enabled"] = bool(body["enabled"])
            else:
                schedule["enabled"] = not bool(schedule.get("enabled", True))
            schedule["next_run_at"] = _next_archive_retest_run_at(schedule)
            _save_archive_retest_schedules(schedules)
            return {"ok": True, "schedule": schedule}
    raise HTTPException(404, "Schedule not found")


@router.delete("/archives/{name}/retest-schedules/{schedule_id}")
def delete_archive_retest_schedule(name: str, schedule_id: str, session: SessionToken = Depends(require_auth)):
    """Delete one archive retest schedule."""
    _validate_name(name)
    _validate_name(schedule_id)
    if name != _own_archive_name():
        raise HTTPException(403, "Scheduled retests are only allowed for the configured own archive")
    schedules = _load_archive_retest_schedules()
    kept = [s for s in schedules if not (s.get("id") == schedule_id and s.get("archive_name") == name)]
    if len(kept) == len(schedules):
        raise HTTPException(404, "Schedule not found")
    _save_archive_retest_schedules(kept)
    return {"ok": True}


@router.post("/archives/{name}/results/remove-liquidated")
def remove_archive_liquidated_results(name: str, body: dict, session: SessionToken = Depends(require_auth)):
    """Remove liquidated archived results from the configured own archive."""
    _validate_name(name)
    if name != _own_archive_name():
        raise HTTPException(403, "Liquidated cleanup is only allowed for the configured own archive")
    archive_dir = (_archives_dir() / name).resolve()
    if not archive_dir.exists():
        raise HTTPException(404, f"Archive '{name}' not found")
    paths = (body or {}).get("paths") or []
    if not isinstance(paths, list):
        raise HTTPException(400, "paths must be a list")
    scope = str((body or {}).get("scope") or "selected_results")
    dry_run = bool((body or {}).get("dry_run", True))
    result = remove_liquidated_results(archive_dir, paths, scope, dry_run)
    if not dry_run:
        _invalidate_archive_cache(name)
        result["manifest"] = rebuild_archive_manifest(archive_dir)
    return result


@router.post("/archives/{name}/results/remove-duplicates")
def remove_archive_duplicate_results(name: str, body: dict, session: SessionToken = Depends(require_auth)):
    """Remove duplicate archived results from the configured own archive."""
    _validate_name(name)
    if name != _own_archive_name():
        raise HTTPException(403, "Duplicate cleanup is only allowed for the configured own archive")
    archive_dir = (_archives_dir() / name).resolve()
    if not archive_dir.exists():
        raise HTTPException(404, f"Archive '{name}' not found")
    paths = (body or {}).get("paths") or []
    if not isinstance(paths, list):
        raise HTTPException(400, "paths must be a list")
    scope = str((body or {}).get("scope") or "selected_results")
    dry_run = bool((body or {}).get("dry_run", True))
    result = remove_duplicate_results(archive_dir, paths, scope, dry_run)
    if not dry_run:
        _invalidate_archive_cache(name)
        result["manifest"] = rebuild_archive_manifest(archive_dir)
    return result


@router.get("/archives/settings")
def get_archive_settings(session: SessionToken = Depends(require_auth)):
    """Get archive configuration from INI using the legacy config_archive keys."""
    section = "config_archive"
    my_archive = load_ini(section, "my_archive") or ""
    readme_config = {}
    if my_archive:
        archive_dir = _archives_dir() / my_archive
        if archive_dir.exists():
            readme_config = load_archive_readme_config(archive_dir)
    return {
        "my_archive":        my_archive,
        "my_archive_path":   load_ini(section, "my_archive_path") or "",
        "generated_paths":   True,
        "username":          load_ini(section, "my_archive_username") or "",
        "email":             load_ini(section, "my_archive_email") or "",
        "access_token":      load_ini(section, "my_archive_access_token") or "",
        "auto_pull_interval": _read_auto_pull_interval(),
        "readme_title":      readme_config.get("title", my_archive or "PBGui Config Archive"),
        "readme_static_markdown": readme_config.get("static_markdown", ""),
    }


@router.post("/archives/settings")
def save_archive_settings(body: dict, session: SessionToken = Depends(require_auth)):
    """Save archive configuration to INI using the legacy config_archive keys."""
    section = "config_archive"
    mapping = {
        "my_archive":      "my_archive",
        "username":        "my_archive_username",
        "email":            "my_archive_email",
        "access_token":    "my_archive_access_token",
    }
    for body_key, ini_key in mapping.items():
        if body_key in body:
            save_ini(section, ini_key, str(body[body_key]))
    if "auto_pull_interval" in body:
        try:
            interval = max(0, int(body["auto_pull_interval"]))
        except (ValueError, TypeError):
            interval = 0
        save_ini(section, "auto_pull_interval", str(interval))
    if "readme_title" in body or "readme_static_markdown" in body:
        archive_name = str(body.get("my_archive") or _own_archive_name() or "").strip()
        _validate_name(archive_name)
        archive_dir = _archives_dir() / archive_name
        if not archive_dir.exists():
            raise HTTPException(404, f"Archive '{archive_name}' not found")
        config = save_archive_readme_config(archive_dir, {
            "title": body.get("readme_title", archive_name),
            "static_markdown": body.get("readme_static_markdown", ""),
        })
        update_archive_readme(archive_dir, config)
        _invalidate_archive_cache(archive_name)
    return {"ok": True}


@router.get("/archives/{name}/readme-config")
def get_archive_readme_config(name: str, session: SessionToken = Depends(require_auth)):
    """Return README static-section config for one archive."""
    _validate_name(name)
    archive_dir = _archives_dir() / name
    if not archive_dir.exists():
        raise HTTPException(404, f"Archive '{name}' not found")
    config = load_archive_readme_config(archive_dir)
    return {"ok": True, **config}


@router.post("/archives/{name}/readme-config")
def save_archive_readme_settings(name: str, body: dict, session: SessionToken = Depends(require_auth)):
    """Save README static-section config for the configured own archive."""
    _validate_name(name)
    if name != _own_archive_name():
        raise HTTPException(403, "README configuration is only available for the configured own archive")
    archive_dir = _archives_dir() / name
    if not archive_dir.exists():
        raise HTTPException(404, f"Archive '{name}' not found")
    config = save_archive_readme_config(archive_dir, body or {})
    update_archive_readme(archive_dir, config)
    _invalidate_archive_cache(name)
    return {"ok": True, **config}
