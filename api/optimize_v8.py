"""FastAPI backend for isolated Passivbot V8 optimize management."""

from __future__ import annotations

import asyncio
import copy
import datetime
import json
import multiprocessing
import os
import platform
import re
import signal
import socket
import subprocess
import threading
import time
import traceback
import uuid
from collections import OrderedDict
from contextlib import contextmanager
from pathlib import Path, PurePath
from shutil import copy2, rmtree, which
from typing import Optional

import psutil
import httpx
import msgpack
from fastapi import APIRouter, Depends, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, Response

from api.auth import SessionToken, authenticate_websocket, require_auth
from api.pb8_ohlcv_tools import (
    PB8OhlcvUnavailableError,
    build_pb8_ohlcv_preflight,
    get_pb8_ohlcv_preload_job,
    start_pb8_ohlcv_preload_job,
    stop_pb8_ohlcv_preload_job,
)
from file_lock import advisory_file_lock
from logging_helpers import append_managed_transcript_line, human_log as _log, rotate_managed_log_before_open
from master_update_lock import MasterUpdateBusyError, acquire_master_runtime_lock
from optimize_autostart import claim_autostart, publish_autostart_process, release_autostart
from pb8_config import (
    PB8ConfigurationError,
    cache_prepared_pb8_config,
    get_pb8_optimize_metadata,
    load_pb8_config,
    migrate_pb7_config,
    prepare_pb8_config,
)
from pbgui_purefunc import PBGDIR, PBGUI_SERIAL, PBGUI_VERSION, load_ini_section, pb7dir, pb8_runtime_status, save_ini_section
from secure_files import atomic_write_private_text, ensure_private_directory, ensure_private_directory_tree

SERVICE = "OptimizeV8"
router = APIRouter()

_CONFIG_FILENAME = "optimize.json"
_CONFIG_SECTIONS = ("backtest", "bot", "live", "optimize", "coin_overrides", "pbgui")
_QUEUE_SETTINGS_SECTION = "optimize_v7"
_PARETO_STATISTICS = ("mean", "min", "max", "std", "median")
_LAUNCH_MODES = {"fresh", "pareto_seed", "checkpoint_resume"}
_RESULT_PROGRESS_CACHE_TTL_SECONDS = 15 * 60
_RESULT_PROGRESS_CACHE_MAX_ENTRIES = 64
_RESULT_LIST_SCAN_LIMIT_BYTES = 8 * 1024 * 1024
_BACKTEST_COUNT_CACHE_TTL_SECONDS = 30
_BACKTEST_COUNT_CACHE_MAX_ENTRIES = 128
_PARETO_LIST_CACHE_TTL_SECONDS = 5 * 60
_PARETO_LIST_CACHE_MAX_ENTRIES = 2048
_PARETO_WARNING_TTL_SECONDS = 60
_PARETO_WARNING_MAX_ENTRIES = 64
_DASH_REQUEST_HEADERS_ALLOW = {"accept", "accept-language", "cache-control", "content-type", "pragma", "user-agent"}
_DASH_RESPONSE_HEADERS_DROP = {
    "connection",
    "content-encoding",
    "content-length",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "set-cookie",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}
_DASH_MAX_ACTIVE_SESSIONS = 4
_DASH_IDLE_TTL_SECONDS = 15 * 60
_DASH_REGISTRY_VERSION = 1
_dash_sessions: dict[str, dict] = {}
_dash_pending_sessions: dict[str, dict | None] = {}
_dash_lock = threading.RLock()
_dash_admission_open = False
_result_progress_cache: OrderedDict[str, dict] = OrderedDict()
_result_progress_cache_lock = threading.RLock()
_backtest_count_cache: OrderedDict[str, dict] = OrderedDict()
_backtest_count_cache_lock = threading.RLock()
_pareto_list_cache: OrderedDict[tuple[str, int | None, int, int], dict] = OrderedDict()
_pareto_list_cache_lock = threading.RLock()
_pareto_warning_cache: OrderedDict[str, float] = OrderedDict()

_OPT_LOG_LINE_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)\s+(?:(?P<level>[A-Z]+)\s+)?(?P<msg>.*)$"
)
_OPT_LOG_BACKEND_RE = re.compile(r"Selected optimizer backend:\s*(?P<backend>[a-z0-9_]+)", re.IGNORECASE)
_OPT_LOG_PYMOO_RE = re.compile(
    r"Using pymoo\s+(?P<algorithm>[a-z0-9_]+)(?:\s*\|\s*n_obj=(?P<n_obj>\d+))?",
    re.IGNORECASE,
)
_OPT_LOG_ITER_RE = re.compile(r"Iter:\s*(?P<iter>\d+).*?size:(?P<size>\d+)(?:\s*\|\s*(?P<ranges>.*))?$", re.IGNORECASE)
_OPT_LOG_EVAL_RE = re.compile(r"(?:Pareto update\s*\|\s*)?eval(?:uations)?\s*[=:]\s*(?P<eval>\d+)", re.IGNORECASE)
_OPT_LOG_RANGE_RE = re.compile(r"(?P<name>[a-zA-Z0-9_.-]+):\((?P<min>[^,]+),(?P<max>[^)]+)\)")


def _data_dir() -> Path:
    return Path(PBGDIR) / "data"


def _configs_dir() -> Path:
    return _data_dir() / "opt_v8"


def _v7_configs_dir() -> Path:
    return _data_dir() / "opt_v7"


def _resolve_v7_pareto_config(path: str) -> Path:
    """Resolve one PB7 Pareto JSON below the installed PB7 optimize result root."""
    root = (Path(pb7dir()) / "optimize_results").resolve()
    source = Path(str(path or "")).resolve()
    try:
        source.relative_to(root)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid PB7 Pareto config path") from exc
    if source.is_symlink() or not source.is_file() or source.suffix.lower() != ".json" or source.parent.name != "pareto":
        raise HTTPException(status_code=400, detail="Path is not a managed PB7 Pareto config")
    return source


def _queue_dir() -> Path:
    return _data_dir() / "opt_v8_queue"


def _log_dir() -> Path:
    return _data_dir() / "logs" / "optimizes_v8"


def _results_root() -> Path:
    runtime = pb8_runtime_status()
    pb8_dir = str(runtime.get("pb8dir") or "")
    return Path(pb8_dir) / "optimize_results" if pb8_dir else _data_dir() / ".pb8-unavailable"


def _backtests_root() -> Path:
    runtime = pb8_runtime_status()
    pb8_dir = str(runtime.get("pb8dir") or "")
    return Path(pb8_dir) / "backtests" / "pbgui" if pb8_dir else _data_dir() / ".pb8-unavailable"


def _validate_name(name: str) -> str:
    value = str(name or "").strip()
    if (
        not value
        or value.startswith(".")
        or value in {".", ".."}
        or any(char in value for char in ("/", "\\", "\x00"))
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
        or len(value.encode("utf-8")) > 128
    ):
        raise HTTPException(status_code=400, detail="Invalid name")
    return value


def _safe_path(path: Path, root: Path) -> Path:
    absolute_root = Path(os.path.abspath(root))
    absolute_path = Path(os.path.abspath(path))
    try:
        relative = absolute_path.relative_to(absolute_root)
    except ValueError as exc:
        raise RuntimeError(f"Managed path escaped root: {path}") from exc
    if absolute_root.is_symlink():
        raise RuntimeError(f"Managed root must not be a symlink: {absolute_root}")
    current = absolute_root
    for part in relative.parts:
        current /= part
        if current.is_symlink():
            raise RuntimeError(f"Managed path contains a symlink: {current}")
    return absolute_path


def _config_dir(name: str) -> Path:
    return _safe_path(_configs_dir() / _validate_name(name), _configs_dir())


def _config_file(name: str) -> Path:
    return _safe_path(_config_dir(name) / _CONFIG_FILENAME, _configs_dir())


def _queue_file(filename: str) -> Path:
    return _safe_path(_queue_dir() / f"{_validate_name(filename)}.json", _queue_dir())


def _pid_file(filename: str) -> Path:
    return _safe_path(_queue_dir() / f"{_validate_name(filename)}.pid", _queue_dir())


def _state_file(filename: str) -> Path:
    return _safe_path(_queue_dir() / "state" / f"{_validate_name(filename)}.json", _queue_dir())


def _ready_file(filename: str) -> Path:
    return _safe_path(_queue_dir() / "state" / f"{_validate_name(filename)}.launch-ready", _queue_dir())


def _reorder_file() -> Path:
    return _safe_path(_queue_dir() / "state" / "reorder.json", _queue_dir())


def _snapshot_dir(filename: str) -> Path:
    return _safe_path(_queue_dir() / "snapshots" / _validate_name(filename), _queue_dir())


def _snapshot_file(filename: str) -> Path:
    return _safe_path(_snapshot_dir(filename) / _CONFIG_FILENAME, _queue_dir())


def _launch_dir(filename: str) -> Path:
    return _safe_path(_queue_dir() / "launch" / _validate_name(filename), _queue_dir())


def _launch_config_file(filename: str) -> Path:
    return _safe_path(_launch_dir(filename) / _CONFIG_FILENAME, _queue_dir())


def _launch_options_file(filename: str) -> Path:
    return _safe_path(_launch_dir(filename) / "options.json", _queue_dir())


def _write_json(path: Path, payload: dict) -> None:
    atomic_write_private_text(path, json.dumps(payload, indent=4, allow_nan=False) + "\n")


def _read_json(path: Path) -> dict:
    try:
        if path.is_symlink() or not path.is_file():
            raise OSError("not a regular file")
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Failed to read {path.name}: {exc}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"{path.name} must contain an object")
    return payload


def _recover_config_transactions(root: Path) -> None:
    """Recover PBGui-owned bundle swaps left by an interrupted API process."""
    if not root.is_dir() or root.is_symlink():
        return
    backups: dict[str, list[Path]] = {}
    for entry in root.iterdir():
        if not entry.name.startswith("."):
            continue
        body = entry.name[1:]
        target_name, separator, token = body.rpartition(".backup-")
        if separator and target_name and len(token) == 32 and all(char in "0123456789abcdef" for char in token):
            if entry.is_symlink() or not entry.is_dir():
                raise RuntimeError(f"Unsafe PB8 optimize backup: {entry}")
            backups.setdefault(target_name, []).append(entry)
            continue
        stage_name, separator, token = body.rpartition(".stage-")
        if separator and stage_name and len(token) == 32 and all(char in "0123456789abcdef" for char in token):
            if entry.is_symlink() or not entry.is_dir():
                raise RuntimeError(f"Unsafe PB8 optimize stage: {entry}")
            rmtree(entry, ignore_errors=True)
    for target_name, candidates in backups.items():
        target = _safe_path(root / target_name, root)
        ordered = sorted(candidates, key=lambda path: path.stat().st_mtime_ns, reverse=True)
        if not target.exists():
            os.replace(ordered.pop(0), target)
            _log(SERVICE, f"Recovered interrupted PB8 optimize config save for {target_name}", level="WARNING")
        for backup in ordered:
            rmtree(backup, ignore_errors=True)


@contextmanager
def _config_lock():
    ensure_private_directory(_configs_dir())
    with advisory_file_lock(_safe_path(_configs_dir() / ".write", _configs_dir())):
        _recover_config_transactions(_configs_dir())
        yield


def _queue_lock():
    ensure_private_directory(_queue_dir())
    return advisory_file_lock(_safe_path(_queue_dir() / ".write", _queue_dir()))


def _result_lock():
    lock_root = ensure_private_directory(_data_dir() / "locks")
    return advisory_file_lock(_safe_path(lock_root / "optimize-v8-results", lock_root))


def _configuration_error(operation: str, exc: Exception, status_code: int = 422) -> HTTPException:
    _log(SERVICE, f"{operation} failed: {exc}", level="WARNING")
    return HTTPException(status_code=int(getattr(exc, "status_code", status_code)), detail=str(exc))


def _normalize_config(config: dict, name: str) -> dict:
    candidate = copy.deepcopy(config)
    backtest = candidate.setdefault("backtest", {})
    if not isinstance(backtest, dict):
        raise HTTPException(status_code=422, detail="backtest must be an object")
    backtest["base_dir"] = f"backtests/pbgui/{name}"
    optimize = candidate.get("optimize")
    overrides = optimize.get("fixed_runtime_overrides") if isinstance(optimize, dict) else None
    if isinstance(overrides, dict):
        bot = candidate.setdefault("bot", {})
        if not isinstance(bot, dict):
            raise HTTPException(status_code=422, detail="bot must be an object")
        for side in ("long", "short"):
            for field in ("enabled", "no_restart_drawdown_threshold"):
                override_key = f"bot.{side}.hsl.{field}"
                if override_key not in overrides:
                    continue
                side_config = bot.setdefault(side, {})
                if not isinstance(side_config, dict):
                    raise HTTPException(status_code=422, detail=f"bot.{side} must be an object")
                hsl = side_config.setdefault("hsl", {})
                if not isinstance(hsl, dict):
                    raise HTTPException(status_code=422, detail=f"bot.{side}.hsl must be an object")
                hsl[field] = overrides.pop(override_key)
    return candidate


def _validate_forager_optimize_search_space(config: dict) -> None:
    """Reject candidate ranges that can produce PB8-invalid forager configs."""
    bot = config.get("bot") if isinstance(config.get("bot"), dict) else {}
    optimize = config.get("optimize") if isinstance(config.get("optimize"), dict) else {}
    bounds = optimize.get("bounds") if isinstance(optimize.get("bounds"), dict) else {}
    fixed_params = {
        str(value).removeprefix("bot.")
        for value in optimize.get("fixed_params", [])
        if isinstance(value, str) and value.strip()
    }
    errors = []

    for side in ("long", "short"):
        side_config = bot.get(side) if isinstance(bot.get(side), dict) else {}
        side_bounds = bounds.get(side) if isinstance(bounds.get(side), dict) else {}
        risk = side_config.get("risk") if isinstance(side_config.get("risk"), dict) else {}
        risk_bounds = side_bounds.get("risk") if isinstance(side_bounds.get("risk"), dict) else {}
        forager = side_config.get("forager") if isinstance(side_config.get("forager"), dict) else {}
        forager_bounds = side_bounds.get("forager") if isinstance(side_bounds.get("forager"), dict) else {}
        score_weights = forager.get("score_weights") if isinstance(forager.get("score_weights"), dict) else {}

        def candidate_range(group: str, key: str, current: object, group_bounds: dict) -> tuple[float, float]:
            try:
                current_number = float(current)
            except (TypeError, ValueError):
                current_number = 0.0
            selector = f"{side}.{group}.{key}"
            if selector in fixed_params:
                return current_number, current_number
            raw = group_bounds.get(key)
            if not isinstance(raw, (list, tuple)) or not raw:
                return current_number, current_number
            try:
                low = float(raw[0])
                high = float(raw[1] if len(raw) > 1 else raw[0])
            except (TypeError, ValueError):
                return current_number, current_number
            return min(low, high), max(low, high)

        _, n_positions_max = candidate_range("risk", "n_positions", risk.get("n_positions"), risk_bounds)
        _, exposure_max = candidate_range(
            "risk",
            "total_wallet_exposure_limit",
            risk.get("total_wallet_exposure_limit"),
            risk_bounds,
        )
        if n_positions_max <= 0.0 or exposure_max <= 0.0:
            continue

        _, volume_weight_max = candidate_range(
            "forager",
            "score_weights_volume",
            score_weights.get("volume"),
            forager_bounds,
        )
        _, volatility_weight_max = candidate_range(
            "forager",
            "score_weights_volatility",
            score_weights.get("volatility"),
            forager_bounds,
        )
        _, volume_drop_max = candidate_range(
            "forager",
            "volume_drop_pct",
            forager.get("volume_drop_pct"),
            forager_bounds,
        )
        volume_span_min, _ = candidate_range(
            "forager",
            "volume_ema_span_1m",
            forager.get("volume_ema_span_1m"),
            forager_bounds,
        )
        volatility_span_min, _ = candidate_range(
            "forager",
            "volatility_ema_span_1m",
            forager.get("volatility_ema_span_1m"),
            forager_bounds,
        )
        if (volume_weight_max > 0.0 or volume_drop_max > 0.0) and volume_span_min <= 0.0:
            errors.append(
                f"bot.{side}.forager.volume_ema_span_1m must stay > 0 because optimize bounds can enable volume ranking or pruning; use Fixed to exclude this parameter while retaining its positive bot value"
            )
        if volatility_weight_max > 0.0 and volatility_span_min <= 0.0:
            errors.append(
                f"bot.{side}.forager.volatility_ema_span_1m must stay > 0 because optimize bounds can enable volatility ranking; use Fixed to exclude this parameter while retaining its positive bot value"
            )

    if errors:
        raise HTTPException(status_code=422, detail="Invalid PB8 optimize search space: " + "; ".join(errors))


def _publish_bundle(stage: Path, target: Path) -> None:
    backup = target.parent / f".{target.name}.backup-{uuid.uuid4().hex}"
    existed = target.exists()
    if existed:
        if target.is_symlink() or not target.is_dir():
            raise HTTPException(status_code=409, detail="Config target is not a safe directory")
        os.replace(target, backup)
    try:
        os.replace(stage, target)
    except Exception:
        if existed and backup.exists() and not target.exists():
            os.replace(backup, target)
        raise
    rmtree(backup, ignore_errors=True)


def _save_config_bundle(name: str, config: dict, *, create_only: bool = False) -> dict:
    root = ensure_private_directory(_configs_dir())
    target = _config_dir(name)
    if create_only and target.exists():
        raise HTTPException(status_code=409, detail=f"Config '{name}' already exists")
    stage = _safe_path(root / f".{name}.stage-{uuid.uuid4().hex}", root)
    ensure_private_directory(stage)
    try:
        prepared = prepare_pb8_config(_normalize_config(config, name), base_config_path=str(stage / _CONFIG_FILENAME))
        _validate_forager_optimize_search_space(prepared)
        _write_json(stage / _CONFIG_FILENAME, prepared)
        current_report = target / "migration_report.json"
        if current_report.is_file() and not current_report.is_symlink():
            copy2(current_report, stage / current_report.name)
        _publish_bundle(stage, target)
        cache_prepared_pb8_config(prepared, target / _CONFIG_FILENAME)
        return prepared
    finally:
        rmtree(stage, ignore_errors=True)


def _read_process_record(filename: str) -> dict | None:
    try:
        record = _read_json(_pid_file(filename))
        pid = int(record.get("pid") or 0)
        create_time = float(record.get("create_time") or 0.0)
        if pid <= 0 or create_time <= 0:
            return None
        owned_results = record.get("owned_results")
        return {
            "pid": pid,
            "create_time": create_time,
            "owned_results": list(owned_results) if isinstance(owned_results, list) else [],
        }
    except (RuntimeError, ValueError, TypeError):
        return None


def _systemd_user_manager_available() -> bool:
    """Return whether a transient user unit can isolate a persistent optimizer."""
    if platform.system() != "Linux" or which("systemd-run") is None:
        return False
    runtime_dir = Path(os.environ.get("XDG_RUNTIME_DIR") or f"/run/user/{os.getuid()}")
    return (runtime_dir / "systemd" / "private").exists()


def _launch_optimizer_runner(filename: str, command: list[str], cwd: Path, log_path: Path) -> subprocess.Popen | None:
    """Launch a runner outside the API cgroup when user systemd is available."""
    if _systemd_user_manager_available():
        atomic_write_private_text(log_path, "")
        unit = f"pbgui-pb8-optimize-{filename}-{time.time_ns()}"
        runtime_dir = os.environ.get("XDG_RUNTIME_DIR") or f"/run/user/{os.getuid()}"
        result = subprocess.run(
            [
                str(which("systemd-run")),
                "--user",
                "--quiet",
                "--collect",
                f"--unit={unit}",
                "--property=Type=exec",
                "--property=UMask=0077",
                f"--property=WorkingDirectory={cwd}",
                f"--property=StandardOutput=append:{log_path}",
                f"--property=StandardError=append:{log_path}",
                f"--setenv=PATH={Path(command[0]).parent}{os.pathsep}{os.environ.get('PATH', '')}",
                "--",
                *command,
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=15,
            env={**os.environ, "XDG_RUNTIME_DIR": runtime_dir},
        )
        if result.returncode != 0:
            detail = ((result.stderr or "") + (result.stdout or "")).strip()
            raise RuntimeError(f"Could not start persistent PB8 optimizer unit: {detail or result.returncode}")
        return None

    log_file = open(log_path, "w", encoding="utf-8")
    try:
        kwargs = {
            "cwd": str(cwd),
            "stdout": log_file,
            "stderr": log_file,
            "env": {**os.environ, "PATH": str(Path(command[0]).parent) + os.pathsep + os.environ.get("PATH", "")},
            "close_fds": True,
        }
        if platform.system() == "Windows":
            kwargs["creationflags"] = subprocess.DETACHED_PROCESS | subprocess.CREATE_NO_WINDOW
        else:
            kwargs["start_new_session"] = True
        return subprocess.Popen(command, **kwargs)
    finally:
        log_file.close()


def _process_matches(filename: str, record: dict) -> bool:
    try:
        process = psutil.Process(int(record["pid"]))
        if abs(process.create_time() - float(record["create_time"])) > 0.01:
            return False
        command = [str(part) for part in process.cmdline()]
        return (
            len(command) == 10
            and command[1] == str(Path(PBGDIR) / "pb8_optimize_runner.py")
            and command[2] == "optimize"
            and command[3] == str(_state_file(filename))
            and command[4] == str(_pid_file(filename))
            and command[5] == str(_ready_file(filename))
            and command[8] == str(_launch_config_file(filename).resolve())
            and command[9] == str(_launch_options_file(filename).resolve())
        )
    except (psutil.NoSuchProcess, psutil.ZombieProcess, psutil.AccessDenied, OSError, ValueError, TypeError):
        return False


def _read_runner_state(filename: str) -> dict | None:
    try:
        state = _read_json(_state_file(filename))
        return state if isinstance(state.get("returncode"), int) else None
    except RuntimeError:
        return None


def _read_log_terminal_status(filename: str) -> str | None:
    path = _safe_path(_log_dir() / f"{filename}.log", _log_dir())
    if not path.is_file() or path.is_symlink():
        return None
    try:
        with path.open("rb") as handle:
            handle.seek(0, 2)
            handle.seek(max(0, handle.tell() - 64 * 1024))
            tail = handle.read().decode("utf-8", errors="ignore").lower()
    except OSError:
        return None
    if "optimization complete" in tail or "successfully processed optimize_results" in tail:
        return "complete"
    if tail.strip():
        return "error"
    return None


def _clear_stale_process_record(filename: str, record: dict | None = None) -> None:
    path = _pid_file(filename)
    if path.exists():
        path.unlink(missing_ok=True)
        pid = int((record or {}).get("pid") or 0)
        _log(SERVICE, f"Cleared unverified PB8 optimize ownership for {filename} (PID {pid or 'unknown'})", level="WARNING")


def _queue_status(data: dict) -> tuple[str, int | None]:
    filename = _validate_name(str(data.get("filename") or ""))
    record = _read_process_record(filename)
    if record and _process_matches(filename, record):
        return "running", int(record["pid"])
    if record:
        _clear_stale_process_record(filename, record)
        record = None
    elif _pid_file(filename).exists():
        _clear_stale_process_record(filename)
    override = str(data.get("status_override") or "")
    if override == "error":
        return override, int(record["pid"]) if record else None
    state = _read_runner_state(filename)
    if state is not None:
        return ("complete" if state["returncode"] == 0 else "error"), int(record["pid"]) if record else None
    log_status = _read_log_terminal_status(filename)
    if data.get("started_at") and log_status:
        return log_status, None
    return ("error" if data.get("started_at") else "queued"), int(record["pid"]) if record else None


def _terminate_process(pid: int) -> None:
    try:
        process = psutil.Process(pid)
    except psutil.NoSuchProcess:
        return
    if platform.system() == "Windows":
        members = [*process.children(recursive=True), process]
        for member in members:
            member.terminate()
        _gone, alive = psutil.wait_procs(members, timeout=5)
        for member in alive:
            member.kill()
        return
    process_group = os.getpgid(pid)
    if process_group == os.getpgrp():
        raise RuntimeError("Refusing to terminate the API process group")
    try:
        os.killpg(process_group, signal.SIGTERM)
        _gone, alive = psutil.wait_procs([process], timeout=5)
        if alive:
            os.killpg(process_group, signal.SIGKILL)
    except (ProcessLookupError, psutil.NoSuchProcess):
        pass


def _terminate_verified(filename: str) -> None:
    record = _read_process_record(filename)
    if record is None:
        if _pid_file(filename).exists():
            _clear_stale_process_record(filename)
        return
    pid = int(record["pid"])
    if not psutil.pid_exists(pid):
        _pid_file(filename).unlink(missing_ok=True)
        return
    if not _process_matches(filename, record):
        _clear_stale_process_record(filename, record)
        return
    _terminate_process(pid)
    _pid_file(filename).unlink(missing_ok=True)


def _launch_error_detail(exc: Exception) -> str:
    detail = exc.detail if isinstance(exc, HTTPException) else str(exc)
    if isinstance(detail, dict):
        detail = detail.get("message") or json.dumps(detail, sort_keys=True)
    return str(detail or exc.__class__.__name__).strip()


def _launch_error_is_transient(exc: Exception) -> bool:
    if isinstance(exc, HTTPException) and exc.status_code in {409, 503}:
        return True
    detail = _launch_error_detail(exc).lower()
    return any(
        marker in detail
        for marker in (
            "runtime lock",
            "update is incomplete",
            "runtime is not ready",
            "launch handshake timed out",
            "queue order changed",
        )
    )


def _append_queue_log(filename: str, message: str) -> None:
    ensure_private_directory(_log_dir())
    path = _safe_path(_log_dir() / f"{filename}.log", _log_dir())
    append_managed_transcript_line(
        path,
        f"{datetime.datetime.now(datetime.UTC).isoformat()} PBGui: {message}",
        "optimizes_v8",
    )


def _record_launch_failure(filename: str, exc: Exception) -> bool:
    """Persist a failed launch and return whether it is safe to retry automatically."""
    filename = _validate_name(filename)
    transient = _launch_error_is_transient(exc)
    detail = _launch_error_detail(exc)
    with _queue_lock():
        path = _queue_file(filename)
        if not path.is_file():
            return transient
        data = _read_json(path)
        state = _read_runner_state(filename)
        if state is not None and state["returncode"] == 0:
            return transient
        if data.get("status_override") == "error" and isinstance(exc, HTTPException) and exc.status_code == 409:
            return transient
        record = _read_process_record(filename)
        if record and _process_matches(filename, record):
            return transient
        _pid_file(filename).unlink(missing_ok=True)
        _ready_file(filename).unlink(missing_ok=True)
        if transient:
            _state_file(filename).unlink(missing_ok=True)
            data.pop("started_at", None)
            data.pop("status_override", None)
            data.pop("automatic", None)
            data.pop("error_code", None)
            data.pop("error_reason", None)
            data["launch_message"] = f"Launch deferred; PBGui will retry: {detail}"
        else:
            data["status_override"] = "error"
            data["error_code"] = "prelaunch_failed"
            data["error_reason"] = detail
            data["launch_message"] = f"Launch failed: {detail}. Repair or requeue this item."
        _write_json(path, data)
    _append_queue_log(filename, data["launch_message"])
    return transient


def _resolve_result_path(path: str, *, require_directory: bool = True) -> Path:
    root = _results_root()
    try:
        target = _safe_path(Path(str(path or "")), root)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail="Invalid result path") from exc
    if require_directory:
        if not target.is_dir() or target.is_symlink() or target == root:
            raise HTTPException(status_code=404, detail="Result not found")
    elif not target.exists() or target.is_symlink():
        raise HTTPException(status_code=404, detail="Result artifact not found")
    return target


def _supported_seed_file(path: Path) -> bool:
    name = path.name.lower()
    return path.is_file() and not path.is_symlink() and (name.endswith(".json") or name.endswith("_pareto.txt"))


def _validate_pareto_seed_source(source: str) -> str:
    source_path = _resolve_result_path(source, require_directory=False)
    relative = source_path.relative_to(_results_root())
    in_pareto = "pareto" in relative.parts
    in_bundle = "_seed_bundles" in relative.parts
    if source_path.is_file():
        if not (in_pareto or in_bundle) or not _supported_seed_file(source_path):
            raise HTTPException(
                status_code=422,
                detail="Pareto seed must be a managed .json or *_pareto.txt seed file",
            )
        return str(source_path)
    if not source_path.is_dir() or source_path.is_symlink() or not (source_path.name == "pareto" or in_bundle):
        raise HTTPException(status_code=422, detail="Pareto seed directory is not a managed PB8 seed directory")
    try:
        seeds = [path for path in source_path.iterdir() if _supported_seed_file(path)]
    except OSError as exc:
        raise HTTPException(status_code=422, detail=f"Pareto seed directory is unreadable: {exc}") from exc
    if not seeds:
        raise HTTPException(
            status_code=422,
            detail="Pareto seed directory contains no supported .json or *_pareto.txt seed files",
        )
    return str(source_path)


def _extract_result_config(data: dict | None) -> dict | None:
    if not isinstance(data, dict):
        return None
    config = {key: copy.deepcopy(data[key]) for key in _CONFIG_SECTIONS if isinstance(data.get(key), dict)}
    if data.get("config_version") is not None:
        config["config_version"] = copy.deepcopy(data["config_version"])
    return config if all(isinstance(config.get(key), dict) for key in ("backtest", "optimize")) else None


def _recover_result_config(result_dir: Path) -> dict | None:
    for name in ("config.json", "optimize.json"):
        path = result_dir / name
        if path.is_file() and not path.is_symlink():
            try:
                config = _extract_result_config(_read_json(path))
                if config is not None:
                    return config
            except RuntimeError:
                pass
    pareto = _first_pareto_file(result_dir)
    if pareto is not None:
        try:
            config = _extract_result_config(_read_json(pareto))
            if config is not None:
                return config
        except RuntimeError:
            pass
    config = _extract_result_config(_all_results_first(result_dir / "all_results.bin"))
    if config is not None:
        return config
    return None


def _checkpoint_resume_readiness(result_dir: Path, *, for_listing: bool = False) -> dict:
    checkpoint = _safe_path(result_dir / "checkpoint.pkl", _results_root())
    all_results = _safe_path(result_dir / "all_results.bin", _results_root())
    reasons = []
    try:
        if checkpoint.is_symlink() or not checkpoint.is_file() or checkpoint.stat().st_size <= 0:
            reasons.append("checkpoint.pkl is missing or empty")
        else:
            with checkpoint.open("rb") as handle:
                if not handle.read(1):
                    reasons.append("checkpoint.pkl is unreadable or empty")
    except OSError as exc:
        reasons.append(f"checkpoint.pkl is unreadable: {exc}")

    progress = _all_results_progress_for_listing(all_results) if for_listing else _all_results_progress(all_results)
    if not all_results.is_file() or all_results.is_symlink() or progress.get("bytes", 0) <= 0:
        reasons.append("all_results.bin is missing or empty")
    elif progress.get("error"):
        reasons.append(f"all_results.bin is malformed: {progress['error']}")
    elif not progress.get("scan_deferred") and (
        progress.get("trailing_partial_entry") or progress.get("evaluations", 0) <= 0
    ):
        reasons.append("all_results.bin does not strictly decode to complete result records")

    config = _recover_result_config(result_dir)
    if config is None:
        reasons.append("base result config could not be recovered from native result artifacts")
    elif (config.get("optimize") or {}).get("write_all_results") is not True:
        reasons.append("base result config does not enable optimize.write_all_results")
    return {
        "ready": not reasons,
        "reasons": reasons,
        "config": config,
        "evaluations": progress.get("evaluations", 0),
    }


def _resume_compatibility_fields(config: dict) -> dict:
    optimize = config.get("optimize") if isinstance(config.get("optimize"), dict) else {}
    backtest = config.get("backtest") if isinstance(config.get("backtest"), dict) else {}
    live = config.get("live") if isinstance(config.get("live"), dict) else {}
    return {
        "config_version": config.get("config_version"),
        "optimize.backend": optimize.get("backend"),
        "optimize.scoring": optimize.get("scoring"),
        "optimize.limits": optimize.get("limits"),
        "optimize.bounds": optimize.get("bounds"),
        "optimize.fixed_params": optimize.get("fixed_params"),
        "optimize.fixed_runtime_overrides": optimize.get("fixed_runtime_overrides"),
        "optimize.enable_overrides": optimize.get("enable_overrides"),
        "live.strategy_kind": live.get("strategy_kind"),
        "backtest.exchanges": backtest.get("exchanges"),
        "backtest.start_date": backtest.get("start_date"),
        "backtest.end_date": backtest.get("end_date"),
        "backtest.suite_enabled": backtest.get("suite_enabled"),
        "backtest.scenarios": backtest.get("scenarios"),
        "backtest.suite": backtest.get("suite"),
    }


def _preflight_checkpoint_resume(snapshot: dict, result_dir: Path) -> None:
    readiness = _checkpoint_resume_readiness(result_dir)
    if not readiness["ready"]:
        raise HTTPException(status_code=422, detail="Checkpoint cannot be resumed: " + "; ".join(readiness["reasons"]))
    try:
        queued = prepare_pb8_config(snapshot)
        recovered = prepare_pb8_config(readiness["config"])
    except PB8ConfigurationError as exc:
        raise HTTPException(status_code=422, detail=f"PB8 resume preflight rejected the config: {exc}") from exc
    queued_fields = _resume_compatibility_fields(queued)
    recovered_fields = _resume_compatibility_fields(recovered)
    mismatches = [key for key in queued_fields if queued_fields[key] != recovered_fields[key]]
    if mismatches:
        raise HTTPException(
            status_code=422,
            detail="Queued config is incompatible with the checkpoint: " + ", ".join(mismatches[:8]),
        )


def _validate_launch_options(raw: dict | None) -> dict:
    payload = copy.deepcopy(raw or {})
    mode = str(payload.get("mode") or "fresh").strip().lower()
    if mode not in _LAUNCH_MODES:
        raise HTTPException(status_code=422, detail="Invalid optimize launch mode")
    source = str(payload.get("source") or "").strip()
    if mode == "checkpoint_resume":
        result_dir = _resolve_result_path(source)
        readiness = _checkpoint_resume_readiness(result_dir)
        if not readiness["ready"]:
            raise HTTPException(status_code=422, detail="Checkpoint cannot be resumed: " + "; ".join(readiness["reasons"]))
        source = str(result_dir)
    elif mode == "pareto_seed":
        if source == "__self__":
            source = "__self__"
        else:
            source = _validate_pareto_seed_source(source)
    else:
        source = ""
    fine_tune = payload.get("fine_tune_params", [])
    if isinstance(fine_tune, str):
        fine_tune = [item.strip() for item in fine_tune.split(",") if item.strip()]
    if not isinstance(fine_tune, list) or not all(isinstance(item, str) and item.strip() for item in fine_tune):
        raise HTTPException(status_code=422, detail="fine_tune_params must be a list of paths")
    polish = payload.get("polish_percentage")
    if polish is not None:
        if isinstance(polish, bool) or not isinstance(polish, (int, float)) or polish < 0:
            raise HTTPException(status_code=422, detail="polish_percentage must be a non-negative number")
        polish = float(polish)
    bounds_mode = str(payload.get("polish_bounds_mode") or "clamp")
    if bounds_mode not in {"clamp", "override-tunable", "override-all"}:
        raise HTTPException(status_code=422, detail="Invalid polish_bounds_mode")
    if polish is None and bounds_mode != "clamp":
        raise HTTPException(status_code=422, detail="polish_bounds_mode requires polish_percentage")
    if polish is not None and polish > 1.0 and bounds_mode == "clamp":
        raise HTTPException(
            status_code=422,
            detail="polish_percentage above 1.0 (100%) requires an intentional override bounds mode",
        )
    return {
        "mode": mode,
        "source": source,
        "fine_tune_params": [item.strip() for item in fine_tune],
        "polish_percentage": polish,
        "polish_bounds_mode": bounds_mode,
    }


def _runtime_options_from_config(config: dict) -> dict:
    pbgui = config.get("pbgui") if isinstance(config.get("pbgui"), dict) else {}
    options = copy.deepcopy(pbgui.get("optimize_runtime")) if isinstance(pbgui.get("optimize_runtime"), dict) else {}
    seed_mode = str(pbgui.get("optimize_seed_mode") or "").strip().lower()
    if str(options.get("mode") or "fresh").strip().lower() == "fresh":
        if seed_mode == "path" and str(pbgui.get("optimize_seed_path") or "").strip():
            options.update(mode="pareto_seed", source=str(pbgui["optimize_seed_path"]).strip())
        elif seed_mode == "self":
            options.update(mode="pareto_seed", source="__self__")
    return _validate_launch_options(options)


def _config_seed_metadata(config: dict) -> tuple[str, str]:
    pbgui = config.get("pbgui") if isinstance(config.get("pbgui"), dict) else {}
    runtime = pbgui.get("optimize_runtime") if isinstance(pbgui.get("optimize_runtime"), dict) else {}
    mode = str(runtime.get("mode") or "").strip().lower()
    source = str(runtime.get("source") or "").strip()
    if mode == "pareto_seed":
        return ("self", "__self__") if source == "__self__" else ("path", source)
    if mode == "checkpoint_resume":
        return "none", ""
    legacy_mode = str(pbgui.get("optimize_seed_mode") or "").strip().lower()
    legacy_path = str(pbgui.get("optimize_seed_path") or "").strip()
    if legacy_mode == "self" or pbgui.get("starting_config") is True:
        return "self", "__self__"
    if legacy_mode == "path" and legacy_path:
        return "path", legacy_path
    return "none", ""


def _managed_backtest_count(name: str, config: dict) -> int | None:
    backtest = config.get("backtest") if isinstance(config.get("backtest"), dict) else {}
    base_dir = str(backtest.get("base_dir") or "").strip()
    if PurePath(base_dir).parts != ("backtests", "pbgui", name):
        return None
    root = _backtests_root()
    try:
        target = _safe_path(root / name, root)
    except RuntimeError:
        return None
    if not root.is_dir() or root.is_symlink() or not target.is_dir() or target.is_symlink():
        return None
    try:
        stat_result = target.stat()
    except OSError:
        return None
    key = str(target)
    now = time.monotonic()
    with _backtest_count_cache_lock:
        cached = _backtest_count_cache.get(key)
        if (
            cached
            and cached["mtime_ns"] == stat_result.st_mtime_ns
            and now - cached["loaded_at"] < _BACKTEST_COUNT_CACHE_TTL_SECONDS
        ):
            _backtest_count_cache.move_to_end(key)
            return cached["count"]
    count = 0
    try:
        for index, path in enumerate(target.rglob("analysis.json")):
            if index >= 10_000:
                return None
            if path.is_file() and not path.is_symlink():
                count += 1
    except OSError:
        return None
    with _backtest_count_cache_lock:
        _backtest_count_cache[key] = {"mtime_ns": stat_result.st_mtime_ns, "loaded_at": now, "count": count}
        _backtest_count_cache.move_to_end(key)
        while len(_backtest_count_cache) > _BACKTEST_COUNT_CACHE_MAX_ENTRIES:
            _backtest_count_cache.popitem(last=False)
    return count


def _normalize_autostart_cpu(value) -> int:
    """Clamp an Optimize autostart CPU setting to the current host."""
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = 1
    return max(1, min(parsed, multiprocessing.cpu_count()))


def _apply_queue_launch_settings(
    config: dict,
    settings: dict,
    *,
    automatic: bool,
    pbgui_data_path: str | None = None,
) -> dict:
    """Apply shared queue settings to a launch copy without mutating its snapshot."""
    prepared = copy.deepcopy(config)
    use_pbgui_data = str(settings.get("use_pbgui_market_data", "False")).lower() == "true"
    if use_pbgui_data:
        if not pbgui_data_path:
            raise RuntimeError("PBGui market data path is unavailable")
        prepared.setdefault("backtest", {})["ohlcv_source_dir"] = str(pbgui_data_path)
    cpu_override = str(settings.get("cpu_override", "True")).lower() == "true"
    if automatic and cpu_override:
        prepared.setdefault("optimize", {})["n_cpus"] = _normalize_autostart_cpu(settings.get("cpu", "1"))
    return prepared


def _queue_item(path: Path) -> dict:
    data = _read_json(_safe_path(path, _queue_dir()))
    filename = str(data.get("filename") or path.stem)
    if filename != path.stem:
        raise RuntimeError("Persisted queue filename does not match its file")
    status, pid = _queue_status({**data, "filename": filename})
    options = data.get("launch_options") if isinstance(data.get("launch_options"), dict) else {}
    return {
        "filename": filename,
        "name": str(data.get("name") or filename),
        "exchange": data.get("exchange") or [],
        "status": status,
        "pid": pid,
        "created": datetime.datetime.fromtimestamp(path.stat().st_mtime).isoformat(),
        "started_at": data.get("started_at"),
        "order": data.get("order"),
        "launch_mode": options.get("mode") or "fresh",
        "launch_message": data.get("launch_message") or "",
        "error_code": data.get("error_code") or "",
        "error_reason": data.get("error_reason") or "",
        "pb8_version": data.get("pb8_version") or "",
        "pb8_commit": data.get("pb8_commit") or "",
        "automatic": bool(data.get("automatic", False)),
    }


def _load_queue() -> list[dict]:
    ensure_private_directory(_queue_dir())
    items = []
    for path in _queue_dir().glob("*.json"):
        try:
            items.append(_queue_item(path))
        except Exception as exc:
            _log(SERVICE, f"Failed to load PB8 optimize queue item {path.name}: {exc}", level="WARNING")
    pending_order = _pending_reorder_filenames()
    pending_rank = {filename: index for index, filename in enumerate(pending_order)}
    return sorted(
        items,
        key=lambda item: (
            pending_rank.get(item["filename"], 10**18)
            if pending_rank
            else int(item["order"]) if isinstance(item.get("order"), int) else 10**18,
            item["created"],
            item["filename"],
        ),
    )


def _pending_reorder_filenames() -> list[str]:
    try:
        payload = _read_json(_reorder_file())
        filenames = payload.get("filenames")
        if isinstance(filenames, list):
            normalized = [_validate_name(str(item)) for item in filenames]
            if len(normalized) == len(set(normalized)):
                return normalized
    except (RuntimeError, HTTPException):
        pass
    return []


def _recover_pending_reorder_unlocked() -> None:
    path = _reorder_file()
    if not path.exists():
        return
    filenames = _pending_reorder_filenames()
    existing = {queue_path.stem for queue_path in _queue_dir().glob("*.json")}
    if filenames and set(filenames) == existing:
        for index, filename in enumerate(filenames):
            data = _read_json(_queue_file(filename))
            data["order"] = index
            _write_json(_queue_file(filename), data)
        _log(SERVICE, "Recovered interrupted PB8 optimize queue reorder", level="WARNING")
    else:
        _log(SERVICE, "Discarded invalid interrupted PB8 optimize queue reorder", level="WARNING")
    path.unlink(missing_ok=True)


def _reconcile_queue_artifacts() -> None:
    """Repair queue metadata and remove only non-live, queue-less runtime artifacts."""
    with _queue_lock():
        ensure_private_directory_tree(_queue_dir(), _queue_dir() / "state")
        ensure_private_directory_tree(_queue_dir(), _queue_dir() / "snapshots")
        ensure_private_directory_tree(_queue_dir(), _queue_dir() / "launch")
        _recover_pending_reorder_unlocked()
        queue_ids: set[str] = set()
        next_order = 0
        for path in sorted(_queue_dir().glob("*.json")):
            try:
                filename = _validate_name(path.stem)
                data = _read_json(path)
            except (RuntimeError, HTTPException) as exc:
                _log(SERVICE, f"Could not reconcile PB8 optimize queue record {path.name}: {exc}", level="ERROR")
                continue
            queue_ids.add(filename)
            changed = False
            if data.get("filename") != filename:
                data["filename"] = filename
                changed = True
            if not str(data.get("name") or "").strip():
                data["name"] = filename
                changed = True
            if not isinstance(data.get("order"), int):
                data["order"] = next_order
                changed = True
            next_order = max(next_order, int(data["order"]) + 1)
            expected_snapshot = str(_snapshot_file(filename))
            if data.get("snapshot_path") != expected_snapshot:
                data["snapshot_path"] = expected_snapshot
                changed = True
            record = _read_process_record(filename)
            live = bool(record and _process_matches(filename, record))
            if record and not live:
                _clear_stale_process_record(filename, record)
            if not live:
                _ready_file(filename).unlink(missing_ok=True)
            state = _read_runner_state(filename)
            if not live and (not _snapshot_file(filename).is_file() or _snapshot_file(filename).is_symlink()):
                reason = "Queue config snapshot is missing; select a managed config and use Repair Config."
                if data.get("error_code") != "snapshot_missing" or data.get("error_reason") != reason:
                    data.update(status_override="error", error_code="snapshot_missing", error_reason=reason, launch_message=reason)
                    _append_queue_log(filename, reason)
                    changed = True
            elif (
                not live
                and data.get("started_at")
                and state is None
                and _read_log_terminal_status(filename) != "complete"
                and data.get("status_override") != "error"
            ):
                reason = "Optimizer ownership was stale and no durable runner state was found; requeue to retry."
                data.update(status_override="error", error_code="stale_ownership", error_reason=reason, launch_message=reason)
                _append_queue_log(filename, reason)
                changed = True
            elif state is not None and state["returncode"] != 0 and not data.get("error_reason"):
                reason = str(state.get("error") or f"Optimizer exited with code {state['returncode']}; inspect the retained log.")
                data.update(error_code="runner_failed", error_reason=reason)
                changed = True
            if changed:
                _write_json(path, data)

        artifact_ids: set[str] = set()
        for path in (_queue_dir() / "snapshots").iterdir():
            if path.is_dir() and not path.is_symlink():
                artifact_ids.add(path.name)
        for path in (_queue_dir() / "launch").iterdir():
            if path.is_dir() and not path.is_symlink():
                artifact_ids.add(path.name)
        for path in _queue_dir().glob("*.pid"):
            artifact_ids.add(path.name.removesuffix(".pid"))
        for path in (_queue_dir() / "state").glob("*.json"):
            if path != _reorder_file():
                artifact_ids.add(path.name.removesuffix(".json"))
        for path in (_queue_dir() / "state").glob("*.launch-ready"):
            artifact_ids.add(path.name.removesuffix(".launch-ready"))

        for raw_filename in sorted(artifact_ids - queue_ids):
            try:
                filename = _validate_name(raw_filename)
            except HTTPException:
                continue
            record = _read_process_record(filename)
            if record and _process_matches(filename, record):
                _log(SERVICE, f"Preserved live orphan PB8 optimizer artifacts for {filename}", level="WARNING")
                continue
            for artifact in (_pid_file(filename), _state_file(filename), _ready_file(filename)):
                artifact.unlink(missing_ok=True)
            rmtree(_snapshot_dir(filename), ignore_errors=True)
            rmtree(_launch_dir(filename), ignore_errors=True)
            _log(SERVICE, f"Removed stale orphan PB8 optimize artifacts for {filename}", level="WARNING")


def _runtime_commit(pb8_dir: Path) -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=pb8_dir, capture_output=True, text=True, timeout=5, check=False
        )
        return result.stdout.strip() if result.returncode == 0 else ""
    except (OSError, subprocess.TimeoutExpired):
        return ""


def _result_dirs():
    root = _results_root()
    if not root.is_dir() or root.is_symlink():
        return
    for directory in sorted(root.iterdir()):
        if directory.name.startswith(".") or not directory.is_dir() or directory.is_symlink():
            continue
        if (directory / "all_results.bin").is_file() or (directory / "checkpoint.pkl").is_file() or (directory / "pareto").is_dir():
            yield directory


def _first_pareto_file(result_dir: Path) -> Path | None:
    pareto_dir = result_dir / "pareto"
    if not pareto_dir.is_dir() or pareto_dir.is_symlink():
        return None
    return next((path for path in sorted(pareto_dir.glob("*.json")) if path.is_file() and not path.is_symlink()), None)


def _apply_result_diff(base: dict, diff: dict) -> dict:
    result = {}
    for key in base.keys() | diff.keys():
        if key not in diff:
            result[key] = base[key]
            continue
        value = diff[key]
        if isinstance(value, dict) and value == {"__passivbot_diff_delete__": True}:
            continue
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            result[key] = _apply_result_diff(base[key], value)
        else:
            result[key] = value
    return result


def _all_results_progress(path: Path) -> dict:
    """Decode only appended PB8 MessagePack bytes while retaining diff state."""
    state = _scan_all_results(path)
    payload = {
        "evaluations": state["evaluations"],
        "bytes": state["size"],
        "trailing_partial_entry": state["offset"] < state["size"] and not state.get("error"),
        "latest": _pareto_summary(state["current"], "mean"),
    }
    if state.get("error"):
        payload["error"] = state["error"]
    return payload


def _all_results_progress_for_listing(path: Path) -> dict:
    """Return bounded list metadata without cold-decoding a large result stream."""
    try:
        stat_result = path.stat()
        if path.is_symlink() or not path.is_file():
            raise OSError("not a regular file")
    except OSError:
        return {"evaluations": 0, "bytes": 0, "trailing_partial_entry": False, "latest": {}}
    if stat_result.st_size <= _RESULT_LIST_SCAN_LIMIT_BYTES:
        return _all_results_progress(path)
    key = str(path.resolve(strict=False))
    with _result_progress_cache_lock:
        cached = _result_progress_cache.get(key)
        cache_current = bool(
            cached
            and cached.get("device") == stat_result.st_dev
            and cached.get("inode") == stat_result.st_ino
            and cached.get("size") == stat_result.st_size
            and cached.get("mtime_ns") == stat_result.st_mtime_ns
        )
    if cache_current:
        return _all_results_progress(path)
    return {
        "evaluations": 0,
        "bytes": stat_result.st_size,
        "trailing_partial_entry": False,
        "latest": {},
        "scan_deferred": True,
    }


def _scan_all_results(path: Path) -> dict:
    key = str(path.resolve(strict=False))
    now = time.monotonic()
    with _result_progress_cache_lock:
        for stale_key, cached in list(_result_progress_cache.items()):
            if now - float(cached.get("accessed_at") or 0) > _RESULT_PROGRESS_CACHE_TTL_SECONDS:
                _result_progress_cache.pop(stale_key, None)
        try:
            stat_result = path.stat()
            if path.is_symlink() or not path.is_file():
                raise OSError("not a regular file")
        except OSError:
            _result_progress_cache.pop(key, None)
            return {"device": 0, "inode": 0, "size": 0, "mtime_ns": 0, "offset": 0, "evaluations": 0, "current": {}, "first": None}

        cached = _result_progress_cache.get(key)
        prefix_length = min(int(cached.get("size") or 0), 64) if cached else 0
        suffix_length = prefix_length
        current_prefix = b""
        current_suffix = b""
        if prefix_length:
            try:
                with path.open("rb") as prefix_handle:
                    current_prefix = prefix_handle.read(prefix_length)
                    prefix_handle.seek(int(cached["size"]) - suffix_length)
                    current_suffix = prefix_handle.read(suffix_length)
            except OSError:
                current_prefix = b""
                current_suffix = b""
        same_file = bool(
            cached
            and cached.get("device") == stat_result.st_dev
            and cached.get("inode") == stat_result.st_ino
            and stat_result.st_size >= int(cached.get("size") or 0)
            and current_prefix == bytes(cached.get("prefix") or b"")[:prefix_length]
            and current_suffix == bytes(cached.get("suffix") or b"")[-suffix_length:]
            and not (
                stat_result.st_size == int(cached.get("size") or 0)
                and stat_result.st_mtime_ns != cached.get("mtime_ns")
            )
        )
        if same_file:
            state = cached
            if stat_result.st_size == state["size"]:
                state["accessed_at"] = now
                _result_progress_cache.move_to_end(key)
                return copy.deepcopy(state)
        else:
            state = {
                "device": stat_result.st_dev,
                "inode": stat_result.st_ino,
                "size": 0,
                "mtime_ns": 0,
                "offset": 0,
                "evaluations": 0,
                "current": {},
                "first": None,
            }

        start_offset = int(state["offset"])
        last_complete = start_offset
        state.pop("error", None)
        try:
            with path.open("rb") as handle:
                handle.seek(start_offset)
                unpacker = msgpack.Unpacker(raw=False, strict_map_key=False, max_buffer_size=64 * 1024 * 1024)
                remaining = stat_result.st_size - start_offset
                while remaining > 0:
                    chunk = handle.read(min(1024 * 1024, remaining))
                    if not chunk:
                        break
                    remaining -= len(chunk)
                    unpacker.feed(chunk)
                    for entry in unpacker:
                        last_complete = start_offset + unpacker.tell()
                        if not isinstance(entry, dict):
                            raise ValueError("all_results.bin contains a non-object record")
                        if state["first"] is None:
                            state["first"] = copy.deepcopy(entry)
                        if state["evaluations"] % 100 == 0:
                            state["current"] = copy.deepcopy(entry)
                        else:
                            state["current"] = _apply_result_diff(state["current"], entry)
                        state["evaluations"] += 1
                        state["offset"] = last_complete
        except (OSError, ValueError, msgpack.UnpackException) as exc:
            state["error"] = str(exc)
        if not state.get("error") and state["offset"] < stat_result.st_size:
            tail_size = stat_result.st_size - state["offset"]
            if tail_size <= 64 * 1024 * 1024:
                try:
                    with path.open("rb") as tail_handle:
                        tail_handle.seek(state["offset"])
                        msgpack.unpackb(tail_handle.read(tail_size), raw=False, strict_map_key=False)
                except (msgpack.FormatError, msgpack.StackError) as exc:
                    state["error"] = str(exc) or exc.__class__.__name__
                except ValueError:
                    pass
            else:
                state["error"] = "Trailing MessagePack record exceeds the safe decode limit"
        state.update(
            device=stat_result.st_dev,
            inode=stat_result.st_ino,
            size=stat_result.st_size,
            mtime_ns=stat_result.st_mtime_ns,
            accessed_at=now,
        )
        try:
            with path.open("rb") as prefix_handle:
                state["prefix"] = prefix_handle.read(min(stat_result.st_size, 64))
                prefix_handle.seek(max(0, stat_result.st_size - 64))
                state["suffix"] = prefix_handle.read(min(stat_result.st_size, 64))
        except OSError:
            state["prefix"] = b""
            state["suffix"] = b""
        _result_progress_cache[key] = state
        _result_progress_cache.move_to_end(key)
        while len(_result_progress_cache) > _RESULT_PROGRESS_CACHE_MAX_ENTRIES:
            _result_progress_cache.popitem(last=False)
        return copy.deepcopy(state)


def _all_results_first(path: Path) -> dict | None:
    return _scan_all_results(path).get("first")


def _result_name(result_dir: Path) -> str:
    first = _first_pareto_file(result_dir)
    data = None
    if first:
        try:
            data = _read_json(first)
        except (RuntimeError, AttributeError):
            pass
    if data is None:
        data = _all_results_first(result_dir / "all_results.bin")
    if isinstance(data, dict):
        base_dir = str((data.get("backtest") or {}).get("base_dir") or "")
        if base_dir:
            return PurePath(base_dir).name
    return result_dir.name


def _latest_existing_mtime(paths: list[Path]) -> float | None:
    """Return the latest mtime while tolerating active result-file replacement."""
    mtimes = []
    for path in paths:
        try:
            mtimes.append(path.stat().st_mtime)
        except OSError:
            continue
    return max(mtimes) if mtimes else None


def _list_results() -> list[dict]:
    results = []
    for directory in _result_dirs() or []:
        pareto_dir = directory / "pareto"
        paretos = [path for path in pareto_dir.glob("*.json") if path.is_file() and not path.is_symlink()] if pareto_dir.is_dir() and not pareto_dir.is_symlink() else []
        artifacts = [path for path in (directory / "all_results.bin", directory / "checkpoint.pkl") if path.is_file()]
        modified_paths = [*paretos, *artifacts] or [directory]
        progress = _all_results_progress_for_listing(directory / "all_results.bin")
        first_data = None
        if paretos:
            try:
                first_data = _read_json(paretos[0])
            except RuntimeError:
                pass
        if first_data is None:
            first_data = _all_results_first(directory / "all_results.bin")
        contract = _pareto_contract(first_data or {})
        readiness = _checkpoint_resume_readiness(directory, for_listing=True)
        has_pareto = bool(paretos)
        summary = _pareto_summary(first_data or {}, "mean")
        objective_names = [spec["metric"] for spec in contract["objectives"] if spec["metric"] in summary]
        if not objective_names:
            objective_names = list(summary)
        checkpoint_present = (directory / "checkpoint.pkl").is_file() and not (directory / "checkpoint.pkl").is_symlink()
        modified = _latest_existing_mtime(modified_paths)
        if modified is None:
            modified = _latest_existing_mtime([directory])
        if modified is None:
            continue
        results.append(
            {
                "path": str(directory),
                "result": directory.name,
                "name": _result_name(directory),
                "pareto_count": len(paretos),
                "has_pareto": has_pareto,
                "checkpoint": checkpoint_present,
                "checkpoint_present": checkpoint_present,
                "resumable": readiness["ready"],
                "has_config": readiness["config"] is not None,
                "supports_3d": has_pareto and len(objective_names) == 3,
                "supports_dash": has_pareto,
                "resume_reasons": readiness["reasons"],
                "evaluations": progress["evaluations"],
                "progress": progress,
                "mode": contract["mode"],
                "scenario_count": contract["scenario_count"],
                "scenario_labels": contract["scenario_labels"],
                "modified": datetime.datetime.fromtimestamp(modified).isoformat(),
            }
        )
    return sorted(results, key=lambda item: item["modified"], reverse=True)


def _metric_value(value, statistic: str, scenario: str = "Aggregated") -> float | None:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    if not isinstance(value, dict):
        return None
    if scenario != "Aggregated":
        scenario_value = (value.get("scenarios") or {}).get(scenario) if isinstance(value.get("scenarios"), dict) else None
        if isinstance(scenario_value, (int, float)) and not isinstance(scenario_value, bool):
            return float(scenario_value)
    for candidate in (value.get("aggregated") if statistic == "mean" else None, (value.get("stats") or {}).get(statistic), value.get(statistic)):
        if isinstance(candidate, (int, float)) and not isinstance(candidate, bool):
            return float(candidate)
    return None


def _suite_metric_payload(data: dict) -> tuple[dict, list[str]]:
    suite = data.get("suite_metrics") if isinstance(data.get("suite_metrics"), dict) else {}
    labels = suite.get("scenario_labels") or suite.get("scenarios") or []
    labels = [str(label).strip() for label in labels if str(label).strip()] if isinstance(labels, list) else []
    metrics = suite.get("metrics")
    if not isinstance(metrics, dict):
        aggregate = suite.get("aggregate") if isinstance(suite.get("aggregate"), dict) else {}
        stats = aggregate.get("stats") if isinstance(aggregate.get("stats"), dict) else {}
        aggregated = aggregate.get("aggregated") if isinstance(aggregate.get("aggregated"), dict) else {}
        metrics = {
            str(name): {
                "stats": value if isinstance(value, dict) else {},
                "aggregated": aggregated.get(name, value.get("mean") if isinstance(value, dict) else None),
                "scenarios": {},
            }
            for name, value in {**aggregated, **stats}.items()
        }
    for value in metrics.values() if isinstance(metrics, dict) else []:
        scenarios = value.get("scenarios") if isinstance(value, dict) and isinstance(value.get("scenarios"), dict) else {}
        for label in scenarios:
            normalized = str(label).strip()
            if normalized and normalized not in labels:
                labels.append(normalized)
    return metrics if isinstance(metrics, dict) else {}, labels


def _pareto_objective_specs(data: dict) -> list[dict]:
    optimize = data.get("optimize") if isinstance(data.get("optimize"), dict) else {}
    scoring = optimize.get("scoring") if isinstance(optimize.get("scoring"), list) else []
    specs = []
    for item in scoring:
        if isinstance(item, dict):
            name = str(item.get("metric") or "").strip()
            goal = "min" if str(item.get("goal") or "").strip().lower() == "min" else "max"
        else:
            name = str(item or "").strip()
            goal = "max"
        if name and name not in {entry["metric"] for entry in specs}:
            specs.append({"metric": name, "goal": goal})
    return specs


def _pareto_contract(data: dict) -> dict:
    suite_metrics, labels = _suite_metric_payload(data)
    if suite_metrics:
        mode = "suite"
    elif isinstance(data.get("metrics"), dict) and data["metrics"]:
        mode = "stats"
    elif "analyses_combined" in data or "analyses" in data:
        mode = "legacy"
    else:
        mode = "unknown"
    return {
        "mode": mode,
        "scenario_count": len(labels),
        "scenario_labels": labels,
        "objectives": _pareto_objective_specs(data),
    }


def _pareto_summary(data: dict, statistic: str, scenario: str = "Aggregated") -> dict:
    suite_metrics, _labels = _suite_metric_payload(data)
    if suite_metrics:
        return {
            str(key): numeric
            for key, value in suite_metrics.items()
            if (numeric := _metric_value(value, statistic, scenario)) is not None
        }
    metrics = data.get("metrics") if isinstance(data.get("metrics"), dict) else {}
    roots = [
        data.get("objectives"),
        (data.get("result") or {}).get("objectives") if isinstance(data.get("result"), dict) else None,
        metrics.get("objectives"),
        metrics.get("stats"),
        metrics,
    ]
    result = {}
    for root in roots:
        if not isinstance(root, dict):
            continue
        for key, value in root.items():
            numeric = _metric_value(value, statistic, scenario)
            if numeric is not None:
                result[str(key)] = numeric
        if result:
            break
    return result


def _compact_metric_value(value):
    """Retain only numeric projection fields needed by the Pareto list."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    if not isinstance(value, dict):
        return None
    compact = {}
    for key in ("aggregated", *_PARETO_STATISTICS):
        candidate = value.get(key)
        if isinstance(candidate, (int, float)) and not isinstance(candidate, bool):
            compact[key] = float(candidate)
    stats = value.get("stats") if isinstance(value.get("stats"), dict) else {}
    compact_stats = {
        key: float(stats[key])
        for key in _PARETO_STATISTICS
        if isinstance(stats.get(key), (int, float)) and not isinstance(stats.get(key), bool)
    }
    if compact_stats:
        compact["stats"] = compact_stats
    scenarios = value.get("scenarios") if isinstance(value.get("scenarios"), dict) else {}
    compact_scenarios = {
        str(key): float(candidate)
        for key, candidate in scenarios.items()
        if isinstance(candidate, (int, float)) and not isinstance(candidate, bool)
    }
    if compact_scenarios:
        compact["scenarios"] = compact_scenarios
    return compact or None


def _pareto_list_objective_specs(data: dict) -> list[dict]:
    specs = _pareto_objective_specs(data)
    if specs:
        return specs
    metrics = data.get("metrics") if isinstance(data.get("metrics"), dict) else {}
    roots = [
        data.get("objectives"),
        (data.get("result") or {}).get("objectives") if isinstance(data.get("result"), dict) else None,
        metrics.get("objectives"),
    ]
    names = []
    for root in roots:
        if not isinstance(root, dict):
            continue
        for name in root:
            normalized = str(name).strip()
            if normalized and normalized not in names:
                names.append(normalized)
    return [{"metric": name, "goal": "max"} for name in names]


def _compact_pareto_data(data: dict) -> dict:
    contract = _pareto_contract(data)
    specs = _pareto_list_objective_specs(data)
    names = [spec["metric"] for spec in specs]
    metric_names = list(dict.fromkeys([*names, "gain_usd", "gain_strategy_eq", "gain"]))
    suite_metrics, labels = _suite_metric_payload(data)
    metrics = data.get("metrics") if isinstance(data.get("metrics"), dict) else {}
    stats_root = metrics.get("stats") if isinstance(metrics.get("stats"), dict) else {}
    objective_roots = [
        data.get("objectives"),
        (data.get("result") or {}).get("objectives") if isinstance(data.get("result"), dict) else None,
        metrics.get("objectives"),
    ]
    objective_values = {}
    for name in metric_names:
        for root in objective_roots:
            if not isinstance(root, dict) or name not in root:
                continue
            compact = _compact_metric_value(root[name])
            if compact is not None:
                objective_values[name] = compact
                break
    return {
        "mode": contract["mode"],
        "scenario_count": len(labels) if suite_metrics else contract["scenario_count"],
        "scenario_labels": labels if suite_metrics else contract["scenario_labels"],
        "objectives": specs,
        "objective_names": names,
        "suite_values": {
            name: compact
            for name in metric_names
            if name in suite_metrics and (compact := _compact_metric_value(suite_metrics[name])) is not None
        },
        "stats_values": {
            name: compact
            for name in metric_names
            if name in stats_root and (compact := _compact_metric_value(stats_root[name])) is not None
        },
        "objective_values": objective_values,
    }


def _project_compact_pareto(compact: dict, statistic: str, scenario: str) -> dict:
    suite_values = compact["suite_values"]
    stats_values = compact["stats_values"]
    objective_values = compact["objective_values"]

    def requested_stat(value) -> float | None:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return float(value)
        if not isinstance(value, dict):
            return None
        nested_stats = value.get("stats") if isinstance(value.get("stats"), dict) else {}
        for candidate in (value.get(statistic), nested_stats.get(statistic)):
            if isinstance(candidate, (int, float)) and not isinstance(candidate, bool):
                return float(candidate)
        return None

    def value_for(name: str) -> float | None:
        if compact["mode"] == "suite":
            return _metric_value(suite_values.get(name), statistic, scenario)
        value = requested_stat(stats_values.get(name))
        if value is not None:
            return value
        return _metric_value(objective_values.get(name), statistic, scenario)

    summary = {}
    for name in compact["objective_names"]:
        value = value_for(name)
        if value is not None:
            summary[name] = value
    for alias in ("gain_usd", "gain_strategy_eq", "gain"):
        value = value_for(alias)
        if value is not None:
            summary["gain"] = value
            break
    return summary


def _pareto_file_signature(path: Path) -> tuple[tuple[str, int | None, int, int], os.stat_result]:
    if path.is_symlink():
        raise OSError("symlinked Pareto candidate")
    stat_result = path.stat()
    if not path.is_file():
        raise OSError("Pareto candidate is not a regular file")
    resolved = str(path.resolve())
    inode = int(stat_result.st_ino) if getattr(stat_result, "st_ino", 0) else None
    return (resolved, inode, int(stat_result.st_mtime_ns), int(stat_result.st_size)), stat_result


def _load_compact_pareto(path: Path) -> tuple[dict, os.stat_result]:
    signature, stat_result = _pareto_file_signature(path)
    now = time.monotonic()
    with _pareto_list_cache_lock:
        for key, entry in list(_pareto_list_cache.items()):
            if now - entry["loaded_at"] > _PARETO_LIST_CACHE_TTL_SECONDS:
                _pareto_list_cache.pop(key, None)
        cached = _pareto_list_cache.get(signature)
        if cached is not None:
            _pareto_list_cache.move_to_end(signature)
            return cached["compact"], stat_result
        data = _read_json(path)
        final_signature, final_stat = _pareto_file_signature(path)
        if final_signature != signature:
            raise RuntimeError(f"{path.name} changed while being read")
        compact = _compact_pareto_data(data)
        resolved = signature[0]
        for key in list(_pareto_list_cache):
            if key[0] == resolved and key != signature:
                _pareto_list_cache.pop(key, None)
        _pareto_list_cache[signature] = {"loaded_at": now, "compact": compact}
        _pareto_list_cache.move_to_end(signature)
        while len(_pareto_list_cache) > _PARETO_LIST_CACHE_MAX_ENTRIES:
            _pareto_list_cache.popitem(last=False)
        return compact, final_stat


def _log_pareto_skips(result_dir: Path, count: int, example: str) -> None:
    key = str(result_dir)
    now = time.monotonic()
    with _pareto_list_cache_lock:
        last = _pareto_warning_cache.get(key)
        if last is not None and now - last < _PARETO_WARNING_TTL_SECONDS:
            return
        _pareto_warning_cache[key] = now
        _pareto_warning_cache.move_to_end(key)
        while len(_pareto_warning_cache) > _PARETO_WARNING_MAX_ENTRIES:
            _pareto_warning_cache.popitem(last=False)
    _log(SERVICE, f"Skipped {count} unreadable PB8 Pareto candidate(s) in {result_dir.name}; first error: {example}", level="WARNING")


def _create_seed_bundle(result_dir: Path, paths: list[Path]) -> Path:
    root = _safe_path(result_dir / "_seed_bundles", _results_root())
    root.mkdir(mode=0o700, exist_ok=True)
    name = f"{datetime.datetime.now(datetime.UTC).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    stage = _safe_path(root / f".{name}.tmp", root)
    target = _safe_path(root / name, root)
    stage.mkdir(mode=0o700)
    try:
        for index, source in enumerate(paths, start=1):
            destination = stage / source.name
            if destination.exists():
                destination = stage / f"{index:03d}_{source.name}"
            copy2(source, destination)
            os.chmod(destination, 0o600)
        os.replace(stage, target)
    finally:
        rmtree(stage, ignore_errors=True)
    return target


def _find_free_local_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as handle:
        handle.bind(("127.0.0.1", 0))
        handle.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return int(handle.getsockname()[1])


def _dash_cache_root() -> Path:
    return _data_dir() / "cache" / "pareto_dash_v8"


def _dash_registry_root() -> Path:
    return _data_dir() / "locks" / "pareto_dash_v8"


def _dash_registry_path() -> Path:
    return _dash_registry_root() / "sessions.json"


def _dash_registry_lock():
    ensure_private_directory(_data_dir() / "locks")
    ensure_private_directory(_dash_registry_root())
    return advisory_file_lock(_dash_registry_root() / "sessions")


def _dash_log_path(session_id: str) -> Path:
    return _log_dir() / f"pareto_dash_{session_id}.log"


def _valid_dash_session_id(session_id: str) -> bool:
    return len(session_id) == 12 and all(char in "0123456789abcdef" for char in session_id)


def _dash_registry_record(session_id: str, raw: dict) -> dict | None:
    """Return the non-secret, process-verifiable registry fields for one session."""
    if not _valid_dash_session_id(session_id) or not isinstance(raw, dict):
        return None
    if not isinstance(raw.get("command"), list) or not isinstance(raw.get("command_markers"), list):
        return None
    try:
        command = [str(part) for part in raw["command"]]
        command_markers = [str(part) for part in raw["command_markers"]]
        record = {
            "session_id": session_id,
            "pid": int(raw["pid"]),
            "create_time": float(raw["create_time"]),
            "owner_pid": int(raw["owner_pid"]),
            "owner_create_time": float(raw["owner_create_time"]),
            "command": command,
            "command_markers": command_markers,
            "result_dir": str(raw["result_dir"]),
            "stage_root": str(raw["stage_root"]),
            "port": int(raw["port"]),
            "created_at": float(raw["created_at"]),
            "last_access": float(raw["last_access"]),
        }
    except (KeyError, TypeError, ValueError):
        return None
    if record["pid"] <= 0 or record["owner_pid"] <= 0 or not 0 < record["port"] <= 65535:
        return None
    if not command or len(command) > 32 or len(command_markers) > 31:
        return None
    return record


def _read_dash_registry_unlocked() -> dict[str, dict]:
    path = _dash_registry_path()
    if not path.exists():
        return {}
    try:
        payload = _read_json(path)
    except RuntimeError as exc:
        _log(SERVICE, f"Failed to read PB8 Pareto Dash registry: {exc}", level="WARNING")
        return {}
    if payload.get("version") != _DASH_REGISTRY_VERSION or not isinstance(payload.get("sessions"), dict):
        _log(SERVICE, "Ignoring invalid PB8 Pareto Dash registry", level="WARNING")
        return {}
    records = {}
    for session_id, raw in payload["sessions"].items():
        record = _dash_registry_record(str(session_id), raw)
        if record is not None:
            records[record["session_id"]] = record
    return records


def _write_dash_registry_unlocked(records: dict[str, dict]) -> None:
    sessions = {}
    for session_id, raw in records.items():
        record = _dash_registry_record(str(session_id), raw)
        if record is not None:
            sessions[session_id] = record
    _write_json(_dash_registry_path(), {"version": _DASH_REGISTRY_VERSION, "sessions": sessions})


def _read_dash_registry() -> dict[str, dict]:
    with _dash_registry_lock():
        return _read_dash_registry_unlocked()


def _persist_dash_session(session: dict) -> None:
    session_id = str(session.get("session_id") or "")
    record = _dash_registry_record(session_id, session)
    if record is None:
        raise RuntimeError("Invalid PB8 Pareto Dash session record")
    with _dash_registry_lock():
        records = _read_dash_registry_unlocked()
        active_records = [item for item in records.values() if _dash_owner_matches(item)]
        if session_id not in records and len(active_records) >= _DASH_MAX_ACTIVE_SESSIONS:
            raise RuntimeError("PB8 Pareto Dash session limit reached")
        records[session_id] = record
        _write_dash_registry_unlocked(records)


def _remove_dash_registry_session(session_id: str) -> None:
    if not _valid_dash_session_id(session_id):
        return
    with _dash_registry_lock():
        records = _read_dash_registry_unlocked()
        if records.pop(session_id, None) is not None or _dash_registry_path().exists():
            _write_dash_registry_unlocked(records)


def _dash_process_matches(record: dict) -> bool:
    """Verify PID reuse protection and the complete command before process control."""
    try:
        session_id = str(record["session_id"])
        command = [str(part) for part in record["command"]]
        expected_stage = Path(os.path.abspath(_dash_cache_root() / session_id))
        expected_markers = [
            "-u",
            command[2],
            "--data-root",
            str(expected_stage / "runs"),
            "--host",
            "127.0.0.1",
            "--port",
            str(int(record["port"])),
        ]
        if (
            len(command) != 9
            or Path(command[2]).name != "pareto_dash.py"
            or command != [command[0], *expected_markers]
            or record.get("command_markers") != expected_markers
            or Path(os.path.abspath(str(record["stage_root"]))) != expected_stage
        ):
            return False
        process = psutil.Process(int(record["pid"]))
        return (
            abs(process.create_time() - float(record["create_time"])) <= 0.01
            and [str(part) for part in process.cmdline()] == command
        )
    except (IndexError, KeyError, psutil.Error, OSError, TypeError, ValueError):
        return False


def _dash_owner_matches(record: dict) -> bool:
    try:
        process = psutil.Process(int(record["owner_pid"]))
        return abs(process.create_time() - float(record["owner_create_time"])) <= 0.01
    except (KeyError, psutil.Error, OSError, TypeError, ValueError):
        return False


def _terminate_dash_record(record: dict) -> None:
    if not _dash_process_matches(record):
        return
    pid = int(record["pid"])
    process = psutil.Process(pid)
    if platform.system() == "Windows":
        try:
            process.terminate()
            process.wait(timeout=2)
        except psutil.TimeoutExpired:
            process.kill()
            process.wait(timeout=2)
        except psutil.NoSuchProcess:
            pass
        return
    process_group = os.getpgid(pid)
    if process_group != pid or process_group == os.getpgrp():
        raise RuntimeError("Refusing to terminate an unverified PB8 Pareto Dash process group")
    try:
        os.killpg(process_group, signal.SIGTERM)
        process.wait(timeout=2)
    except psutil.TimeoutExpired:
        if _dash_process_matches(record):
            os.killpg(process_group, signal.SIGKILL)
            try:
                process.wait(timeout=2)
            except (psutil.NoSuchProcess, psutil.TimeoutExpired):
                pass
    except (ProcessLookupError, psutil.NoSuchProcess):
        pass


def _remove_dash_artifacts(session_id: str, record: dict | None = None) -> None:
    if not _valid_dash_session_id(session_id):
        return
    expected_stage = Path(os.path.abspath(_dash_cache_root() / session_id))
    if record is not None and Path(os.path.abspath(str(record.get("stage_root") or ""))) != expected_stage:
        _log(SERVICE, f"Ignored mismatched PB8 Pareto Dash stage path for {session_id}", level="WARNING")
    if expected_stage.is_symlink():
        expected_stage.unlink(missing_ok=True)
    elif expected_stage.is_dir():
        rmtree(expected_stage)
    log_path = Path(os.path.abspath(_dash_log_path(session_id)))
    expected_log_root = Path(os.path.abspath(_log_dir()))
    try:
        log_path.relative_to(expected_log_root)
    except ValueError:
        return
    log_path.unlink(missing_ok=True)
    for rotated in log_path.parent.glob(f"{log_path.name}.*"):
        if rotated.name.removeprefix(f"{log_path.name}.").isdigit():
            rotated.unlink(missing_ok=True)


def _cleanup_dash_record(session_id: str, record: dict | None, *, remove_registry: bool = True) -> None:
    if record is not None:
        try:
            if _dash_process_matches(record):
                _terminate_dash_record(record)
            elif psutil.pid_exists(int(record.get("pid") or 0)):
                _log(SERVICE, f"Refused to signal unverified PB8 Pareto Dash PID for {session_id}", level="WARNING")
        except Exception as exc:
            _log(SERVICE, f"Failed to stop PB8 Pareto Dash session {session_id}: {exc}", level="ERROR")
    try:
        _remove_dash_artifacts(session_id, record)
    except Exception as exc:
        _log(SERVICE, f"Failed to clean PB8 Pareto Dash artifacts for {session_id}: {exc}", level="WARNING")
    if remove_registry:
        try:
            _remove_dash_registry_session(session_id)
        except Exception as exc:
            _log(SERVICE, f"Failed to remove PB8 Pareto Dash registry entry {session_id}: {exc}", level="WARNING")


def _stop_dash_session(session_id: str) -> None:
    if not _valid_dash_session_id(session_id):
        return
    with _dash_lock:
        session = _dash_sessions.pop(session_id, None)
        pending = _dash_pending_sessions.pop(session_id, None)
    record = session or pending
    if record is None:
        try:
            record = _read_dash_registry().get(session_id)
        except Exception as exc:
            _log(SERVICE, f"Failed to inspect PB8 Pareto Dash registry for {session_id}: {exc}", level="WARNING")
    _cleanup_dash_record(session_id, record)


def _stop_all_dash_sessions() -> None:
    global _dash_admission_open
    with _dash_lock:
        _dash_admission_open = False
        session_ids = list(dict.fromkeys([*_dash_sessions, *_dash_pending_sessions]))
    for session_id in session_ids:
        try:
            _stop_dash_session(session_id)
        except Exception as exc:
            _log(SERVICE, f"Failed to clean PB8 Pareto Dash session {session_id}: {exc}", level="ERROR")


def _recover_dash_registry() -> None:
    """Terminate only exactly verified Dash orphans and discard all stale ownership."""
    with _dash_lock:
        local_records = {**_dash_pending_sessions, **_dash_sessions}
        _dash_pending_sessions.clear()
        _dash_sessions.clear()
    try:
        records = _read_dash_registry()
    except Exception as exc:
        _log(SERVICE, f"Failed to load PB8 Pareto Dash recovery registry: {exc}", level="ERROR")
        records = {}
    local_records = {key: value for key, value in local_records.items() if value}
    survivors = {
        session_id: record
        for session_id, record in records.items()
        if session_id not in local_records and _dash_owner_matches(record)
    }
    for session_id, record in {**records, **local_records}.items():
        if session_id in survivors:
            continue
        try:
            _cleanup_dash_record(session_id, record, remove_registry=False)
        except Exception as exc:
            _log(SERVICE, f"Failed to recover PB8 Pareto Dash session {session_id}: {exc}", level="ERROR")
    cache_root = _dash_cache_root()
    if cache_root.is_dir() and not cache_root.is_symlink():
        for stage_root in cache_root.iterdir():
            if _valid_dash_session_id(stage_root.name) and stage_root.name not in survivors:
                try:
                    _remove_dash_artifacts(stage_root.name)
                except Exception as exc:
                    _log(SERVICE, f"Failed to remove stale PB8 Pareto Dash stage {stage_root.name}: {exc}", level="WARNING")
    try:
        with _dash_registry_lock():
            _write_dash_registry_unlocked(survivors)
    except Exception as exc:
        _log(SERVICE, f"Failed to clear PB8 Pareto Dash recovery registry: {exc}", level="ERROR")


def _reap_dash_sessions() -> None:
    now = time.time()
    with _dash_lock:
        stale = [
            session_id
            for session_id, launched in _dash_sessions.items()
            if launched["process"].poll() is not None
            or now - float(launched.get("last_access") or launched.get("created_at") or 0) > _DASH_IDLE_TTL_SECONDS
        ]
        known = {*_dash_sessions, *_dash_pending_sessions}
    try:
        stale.extend(
            session_id
            for session_id, record in _read_dash_registry().items()
            if session_id not in known and not _dash_owner_matches(record)
        )
    except Exception as exc:
        _log(SERVICE, f"Failed to inspect PB8 Pareto Dash registry while reaping: {exc}", level="WARNING")
    for session_id in dict.fromkeys(stale):
        try:
            _stop_dash_session(session_id)
        except Exception as exc:
            _log(SERVICE, f"Failed to reap PB8 Pareto Dash session {session_id}: {exc}", level="ERROR")


def _reserve_dash_launch(session_id: str) -> None:
    if not _valid_dash_session_id(session_id):
        raise HTTPException(status_code=400, detail="Invalid PB8 Pareto Dash session ID")
    _reap_dash_sessions()
    try:
        foreign_sessions = sum(
            1
            for foreign_id, record in _read_dash_registry().items()
            if foreign_id not in _dash_sessions and _dash_owner_matches(record)
        )
    except Exception as exc:
        _log(SERVICE, f"Failed to inspect PB8 Pareto Dash capacity: {exc}", level="WARNING")
        foreign_sessions = _DASH_MAX_ACTIVE_SESSIONS
    with _dash_lock:
        if not _dash_admission_open:
            raise HTTPException(status_code=503, detail="PB8 Pareto Dash is shutting down")
        if len(_dash_sessions) + len(_dash_pending_sessions) + foreign_sessions >= _DASH_MAX_ACTIVE_SESSIONS:
            raise HTTPException(status_code=429, detail="PB8 Pareto Dash session limit reached")
        _dash_pending_sessions[session_id] = None


def _wait_for_dash(process: subprocess.Popen, port: int, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    with httpx.Client(timeout=1.0, follow_redirects=False) as client:
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError("PB8 Pareto Dash exited before it became ready")
            try:
                if client.get(f"http://127.0.0.1:{port}/").status_code < 500:
                    return
            except httpx.HTTPError:
                pass
            time.sleep(0.2)
    raise RuntimeError("PB8 Pareto Dash did not become ready")


def _launch_dash_session(session_id: str, result_dir: Path, proxy_root: str) -> dict:
    _reserve_dash_launch(session_id)
    stage_root = _safe_path(_dash_cache_root() / session_id, _dash_cache_root())
    process = None
    session = None
    runtime_lease = None
    registered = False
    try:
        runtime = pb8_runtime_status()
        if not runtime.get("ready"):
            raise HTTPException(status_code=503, detail="PB8 runtime is not ready")
        data_root = stage_root / "runs"
        ensure_private_directory_tree(_dash_cache_root(), data_root)
        (data_root / result_dir.name).symlink_to(result_dir, target_is_directory=True)
        port = _find_free_local_port()
        script = Path(runtime["pb8dir"]) / "src" / "tools" / "pareto_dash.py"
        if not script.is_file() or script.is_symlink():
            raise HTTPException(status_code=503, detail="PB8 Pareto Dash tool is unavailable")
        try:
            runtime_lease = acquire_master_runtime_lock(Path(PBGDIR))
        except MasterUpdateBusyError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        log_path = _dash_log_path(session_id)
        ensure_private_directory(log_path.parent)
        rotate_managed_log_before_open(log_path, "optimizes_v8")
        command = [
            str(runtime["pb8venv"]),
            "-u",
            str(script),
            "--data-root",
            str(data_root),
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ]
        with open(log_path, "w", encoding="utf-8") as log_file:
            kwargs = {
                "cwd": str(runtime["pb8dir"]),
                "stdout": log_file,
                "stderr": log_file,
                "env": {**os.environ, "PATH": str(Path(runtime["pb8venv"]).parent) + os.pathsep + os.environ.get("PATH", "")},
                "close_fds": True,
            }
            if platform.system() == "Windows":
                kwargs["creationflags"] = subprocess.DETACHED_PROCESS | subprocess.CREATE_NO_WINDOW
            else:
                kwargs["start_new_session"] = True
            with _dash_lock:
                if not _dash_admission_open or session_id not in _dash_pending_sessions:
                    raise HTTPException(status_code=503, detail="PB8 Pareto Dash is shutting down")
            process = subprocess.Popen(command, **kwargs)
        now = time.time()
        session = {
            "session_id": session_id,
            "pid": process.pid,
            "create_time": psutil.Process(process.pid).create_time(),
            "owner_pid": os.getpid(),
            "owner_create_time": psutil.Process(os.getpid()).create_time(),
            "command": command,
            "command_markers": command[1:],
            "result_dir": str(result_dir),
            "proxy_root": proxy_root,
            "port": port,
            "process": process,
            "stage_root": str(stage_root),
            "created_at": now,
            "last_access": now,
        }
        with _dash_lock:
            if not _dash_admission_open or session_id not in _dash_pending_sessions:
                raise HTTPException(status_code=503, detail="PB8 Pareto Dash is shutting down")
            _dash_pending_sessions[session_id] = session
        try:
            _wait_for_dash(process, port)
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        runtime_lease.release()
        runtime_lease = None
        with _dash_lock:
            if not _dash_admission_open or _dash_pending_sessions.get(session_id) is not session:
                raise HTTPException(status_code=503, detail="PB8 Pareto Dash is shutting down")
            try:
                _persist_dash_session(session)
            except Exception as exc:
                raise HTTPException(status_code=500, detail="Failed to register PB8 Pareto Dash session") from exc
            _dash_pending_sessions.pop(session_id, None)
            _dash_sessions[session_id] = session
            registered = True
        return session
    finally:
        if runtime_lease is not None:
            runtime_lease.release()
        if not registered:
            if session is not None:
                _cleanup_dash_record(session_id, session)
            else:
                if process is not None and process.poll() is None:
                    try:
                        process.terminate()
                        process.wait(timeout=2)
                    except (subprocess.TimeoutExpired, psutil.TimeoutExpired):
                        process.kill()
                try:
                    _remove_dash_artifacts(session_id)
                except Exception as exc:
                    _log(SERVICE, f"Failed to clean failed PB8 Pareto Dash launch {session_id}: {exc}", level="WARNING")
            with _dash_lock:
                _dash_pending_sessions.pop(session_id, None)


def _proxy_path(proxy_root: str, path: str) -> str:
    suffix = str(path or "/")
    return f"{proxy_root.rstrip('/')}/{suffix.lstrip('/')}"


def _rewrite_dash_content(content: bytes, content_type: str, proxy_root: str) -> bytes:
    if not content or not any(value in content_type.lower() for value in ("text/html", "javascript", "application/json")):
        return content
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError:
        return content
    root = proxy_root.rstrip("/")
    root_slash = f"{root}/"
    for key in ("requests_pathname_prefix", "url_base_pathname", "routes_pathname_prefix"):
        text = re.sub(
            rf'("{key}"\s*:\s*)"(?:/|\\/|\\u002[fF])"',
            rf'\1"{root_slash}"',
            text,
        )
        text = re.sub(
            rf'("{key}"\s*:\s*)null',
            rf'\1"{root_slash}"',
            text,
        )
    for old, new in {
        'src="/': f'src="{root_slash}',
        'href="/': f'href="{root_slash}',
        'action="/': f'action="{root_slash}',
        "src='/": f"src='{root_slash}",
        "href='/": f"href='{root_slash}",
        "action='/": f"action='{root_slash}",
        '"/_dash-': f'"{root}/_dash-',
        "'/_dash-": f"'{root}/_dash-",
        '"/assets/': f'"{root}/assets/',
        "'/assets/": f"'{root}/assets/",
        '"/favicon.ico"': f'"{root}/favicon.ico"',
        "'/favicon.ico'": f"'{root}/favicon.ico'",
    }.items():
        text = text.replace(old, new)
    return text.encode("utf-8")


def _dash_request_headers(headers) -> dict[str, str]:
    """Forward only content-negotiation metadata, never browser credentials."""
    return {key: value for key, value in headers.items() if key.lower() in _DASH_REQUEST_HEADERS_ALLOW}


def _dash_response_headers(headers) -> dict[str, str]:
    connection_headers = {
        token.strip().lower()
        for token in str(headers.get("connection") or "").split(",")
        if token.strip()
    }
    blocked = _DASH_RESPONSE_HEADERS_DROP | connection_headers
    return {key: value for key, value in headers.items() if key.lower() not in blocked}


class OptimizeV8Worker:
    """Own the PB8 autostart controller while detached optimizers survive API restarts."""

    def __init__(self) -> None:
        self._task: Optional[asyncio.Task] = None
        self._running = False

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._running = True
            self._task = asyncio.create_task(self._loop(), name="optimize-v8-worker")

    async def stop(self) -> None:
        self._running = False
        task = self._task
        if task is None:
            return
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        if self._task is task:
            self._task = None

    async def _loop(self) -> None:
        while self._running:
            delay = 10
            try:
                settings = load_ini_section(_QUEUE_SETTINGS_SECTION)
                if str(settings.get("autostart", "False")).lower() != "true":
                    delay = 5
                else:
                    filename = None
                    with _queue_lock():
                        items = _load_queue()
                        if not any(item["status"] == "running" and item.get("automatic") for item in items):
                            queued = next((item for item in items if item["status"] == "queued"), None)
                            if queued and claim_autostart("v8", queued["filename"]):
                                filename = queued["filename"]
                    if filename:
                        try:
                            record = await asyncio.to_thread(self.launch, filename, None, True)
                            publish_autostart_process(
                                "v8",
                                filename,
                                record["pid"],
                                record["create_time"],
                                [str(Path(PBGDIR) / "pb8_optimize_runner.py"), str(_launch_config_file(filename).resolve())],
                            )
                        except Exception as exc:
                            release_autostart("v8", filename)
                            transient = _record_launch_failure(filename, exc)
                            _log(
                                SERVICE,
                                f"PB8 automatic optimize launch failed: {exc}",
                                level="WARNING" if transient else "ERROR",
                            )
            except asyncio.CancelledError:
                break
            except Exception as exc:
                delay = 5
                _log(SERVICE, f"PB8 optimize worker iteration failed: {exc}", level="ERROR", meta={"traceback": traceback.format_exc()})
            try:
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                break

    def launch(self, filename: str, launch_options: dict | None = None, automatic: bool = False) -> dict:
        """Validate an immutable queue snapshot and launch one detached PB8 optimizer."""
        filename = _validate_name(filename)
        with _queue_lock():
            queue_path = _queue_file(filename)
            if not queue_path.is_file():
                raise HTTPException(status_code=404, detail="Queue item not found")
            data = _read_json(queue_path)
            status, _pid = _queue_status(data)
            if status != "queued":
                raise HTTPException(status_code=409, detail=f"Queue item is already {status}")
            if automatic:
                first_queued = next((item for item in _load_queue() if item["status"] == "queued"), None)
                if first_queued is None or first_queued["filename"] != filename:
                    raise HTTPException(status_code=409, detail="Queue order changed; automatic launch will retry")
            snapshot = _snapshot_file(filename)
            if not snapshot.is_file() or snapshot.is_symlink():
                raise HTTPException(status_code=422, detail="Queue config snapshot is missing")
            options = _validate_launch_options(launch_options or data.get("launch_options"))
            if options["mode"] == "pareto_seed" and options["source"] == "__self__":
                options["source"] = str(snapshot.resolve())
            elif options["mode"] == "checkpoint_resume":
                _preflight_checkpoint_resume(_read_json(snapshot), Path(options["source"]))
            invalid_marker = Path(PBGDIR) / "data" / "locks" / "pb8-runtime-invalid"
            if invalid_marker.exists() or invalid_marker.is_symlink():
                raise HTTPException(status_code=409, detail="PB8 installation or update is incomplete")
            try:
                runtime_lease = acquire_master_runtime_lock(Path(PBGDIR))
            except MasterUpdateBusyError as exc:
                raise HTTPException(status_code=409, detail=str(exc)) from exc
            try:
                settings = load_ini_section(_QUEUE_SETTINGS_SECTION)
                pbgui_data_path = None
                if str(settings.get("use_pbgui_market_data", "False")).lower() == "true":
                    from market_data import get_market_data_root_dir

                    pbgui_data_path = str(get_market_data_root_dir())
                launch_config = _apply_queue_launch_settings(
                    _read_json(snapshot),
                    settings,
                    automatic=automatic,
                    pbgui_data_path=pbgui_data_path,
                )
                prepared = prepare_pb8_config(launch_config, base_config_path=str(snapshot))
                _validate_forager_optimize_search_space(prepared)
                runtime = pb8_runtime_status()
                if not runtime.get("ready"):
                    raise HTTPException(status_code=503, detail="PB8 runtime is not ready")
                launch_dir = _launch_dir(filename)
                rmtree(launch_dir, ignore_errors=True)
                ensure_private_directory_tree(_queue_dir(), launch_dir)
                _write_json(_launch_config_file(filename), prepared)
                _write_json(_launch_options_file(filename), options)
                _state_file(filename).unlink(missing_ok=True)
                _ready_file(filename).unlink(missing_ok=True)
                data.update(
                    {
                        "started_at": time.time(),
                        "launch_options": options,
                        "launch_message": {
                            "fresh": "Fresh optimizer run",
                            "pareto_seed": "Continue from managed Pareto seed",
                            "checkpoint_resume": "Resume exact managed checkpoint",
                        }[options["mode"]],
                        "automatic": bool(automatic),
                        "pb8_version": runtime.get("version") or "",
                        "pb8_commit": _runtime_commit(Path(runtime["pb8dir"])),
                    }
                )
                for key in ("status_override", "error_code", "error_reason"):
                    data.pop(key, None)
                _write_json(queue_path, data)
                log_path = _safe_path(_log_dir() / f"{filename}.log", _log_dir())
                ensure_private_directory(_log_dir())
                rotate_managed_log_before_open(log_path, "optimizes_v8")
                command = [
                    str(runtime["pb8venv"]),
                    str(Path(PBGDIR) / "pb8_optimize_runner.py"),
                    "optimize",
                    str(_state_file(filename)),
                    str(_pid_file(filename)),
                    str(_ready_file(filename)),
                    str(runtime["cli_file"]),
                    str(runtime["pb8dir"]),
                    str(_launch_config_file(filename).resolve()),
                    str(_launch_options_file(filename).resolve()),
                ]
                process = _launch_optimizer_runner(filename, command, Path(runtime["pb8dir"]), log_path)
                try:
                    record = None
                    deadline = time.monotonic() + 120
                    while time.monotonic() < deadline:
                        record = _read_process_record(filename)
                        if record and _ready_file(filename).is_file():
                            if int(_ready_file(filename).read_text(encoding="utf-8").strip() or 0) == record["pid"]:
                                break
                        state = _read_runner_state(filename)
                        if state is not None or (process is not None and process.poll() is not None):
                            state = state or {}
                            raise RuntimeError(str(state.get("error") or "PB8 optimize runner exited before acquiring the runtime lock"))
                        time.sleep(0.05)
                    else:
                        raise RuntimeError("PB8 optimize runner launch handshake timed out")
                    _ready_file(filename).unlink(missing_ok=True)
                    return record
                except Exception:
                    record = record or _read_process_record(filename)
                    if record and _process_matches(filename, record):
                        _terminate_process(int(record["pid"]))
                    elif process is not None:
                        _terminate_process(process.pid)
                    raise
            finally:
                runtime_lease.release()


_worker = OptimizeV8Worker()
_ws_clients: set[WebSocket] = set()


def startup() -> None:
    """Start only the API-owned PB8 optimize controller."""
    global _dash_admission_open
    with _dash_lock:
        _dash_admission_open = False
    _recover_dash_registry()
    with _dash_lock:
        _dash_admission_open = True
    _reconcile_queue_artifacts()
    _worker.start()


async def shutdown() -> None:
    """Stop the controller without terminating detached optimize jobs."""
    global _dash_admission_open
    with _dash_lock:
        _dash_admission_open = False
    await _worker.stop()
    await asyncio.to_thread(_stop_all_dash_sessions)


@router.websocket("/ws/opt8")
async def ws_optimize(websocket: WebSocket) -> None:
    if await authenticate_websocket(websocket) is None:
        return
    _ws_clients.add(websocket)
    try:
        while True:
            payload = await asyncio.to_thread(
                lambda: {
                    "type": "queue_update",
                    "items": _load_queue(),
                    "settings": load_ini_section(_QUEUE_SETTINGS_SECTION),
                }
            )
            await websocket.send_json(payload)
            try:
                await asyncio.wait_for(websocket.receive_text(), timeout=3)
            except asyncio.TimeoutError:
                pass
    except (WebSocketDisconnect, RuntimeError):
        pass
    finally:
        _ws_clients.discard(websocket)


@router.get("/main_page", response_class=HTMLResponse)
def main_page(request: Request, session: SessionToken = Depends(require_auth)) -> HTMLResponse:
    """Render PB8 through the shared optimize page without exposing the session cookie."""
    html_path = Path(PBGDIR) / "frontend" / "v7_optimize.html"
    if not html_path.is_file():
        raise HTTPException(status_code=404, detail="v7_optimize.html not found")
    html = html_path.read_text(encoding="utf-8")
    origin = str(request.base_url).rstrip("/")
    limits = get_pb8_optimize_metadata().get("limits") or {}
    replacements = {
        "%%TOKEN%%": "",
        "%%API_BASE%%": origin + "/api/optimize-v8",
        "%%WS_BASE%%": origin.replace("http://", "ws://").replace("https://", "wss://"),
        "%%LIMITS_META%%": json.dumps(limits),
        "%%VERSION%%": PBGUI_VERSION,
        "%%SERIAL%%": PBGUI_SERIAL,
        "%%OPTIMIZE_VERSION%%": "v8",
        "%%OPTIMIZE_NAV_TITLE%%": "PBv8 OPTIMIZE",
        "%%OPTIMIZE_NAV_CURRENT%%": "v8_optimize",
        "%%BACKTEST_VERSION%%": "v8",
    }
    for old, new in replacements.items():
        html = html.replace(old, new)
    nav_js = Path(PBGDIR) / "frontend" / "pbgui_nav.js"
    html = html.replace("%%NAV_HASH%%", str(int(nav_js.stat().st_mtime)) if nav_js.is_file() else PBGUI_VERSION)
    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})


@router.get("/runtime")
def runtime_status(session: SessionToken = Depends(require_auth)) -> dict:
    return pb8_runtime_status()


@router.get("/metadata")
def get_metadata(session: SessionToken = Depends(require_auth)) -> dict:
    try:
        return get_pb8_optimize_metadata()
    except PB8ConfigurationError as exc:
        raise _configuration_error("Loading PB8 optimize metadata", exc, 503) from exc


@router.get("/pbgui_data_path")
def get_pbgui_data_path(session: SessionToken = Depends(require_auth)) -> dict:
    from market_data import get_market_data_root_dir

    return {"path": str(get_market_data_root_dir())}


@router.post("/ohlcv-preflight")
async def get_ohlcv_preflight(body: dict, session: SessionToken = Depends(require_auth)) -> dict:
    config = body.get("config") if isinstance(body, dict) else None
    if not isinstance(config, dict):
        raise HTTPException(status_code=400, detail="config must be an object")
    try:
        return await build_pb8_ohlcv_preflight(config)
    except HTTPException:
        raise
    except Exception as exc:
        detail = str(exc).strip() or exc.__class__.__name__
        _log(SERVICE, f"PB8 optimize OHLCV readiness failed: {detail}", level="WARNING", meta={"traceback": traceback.format_exc()})
        status_code = 503 if isinstance(exc, PB8OhlcvUnavailableError) else 422
        raise HTTPException(status_code=status_code, detail=detail) from exc


@router.post("/ohlcv-preload")
def start_ohlcv_preload(body: dict, session: SessionToken = Depends(require_auth)) -> dict:
    config = body.get("config") if isinstance(body, dict) else None
    if not isinstance(config, dict):
        raise HTTPException(status_code=400, detail="config must be an object")
    try:
        return start_pb8_ohlcv_preload_job(config)
    except HTTPException:
        raise
    except Exception as exc:
        detail = str(exc).strip() or exc.__class__.__name__
        _log(SERVICE, f"PB8 optimize OHLCV preload failed: {detail}", level="WARNING", meta={"traceback": traceback.format_exc()})
        status_code = 503 if isinstance(exc, PB8OhlcvUnavailableError) else 422
        raise HTTPException(status_code=status_code, detail=detail) from exc


@router.get("/ohlcv-preload/{job_id}")
def get_ohlcv_preload(job_id: str, session: SessionToken = Depends(require_auth)) -> dict:
    payload = get_pb8_ohlcv_preload_job(job_id)
    if payload is None:
        raise HTTPException(status_code=404, detail="OHLCV preload job not found")
    return payload


@router.delete("/ohlcv-preload/{job_id}")
def stop_ohlcv_preload(job_id: str, session: SessionToken = Depends(require_auth)) -> dict:
    payload = stop_pb8_ohlcv_preload_job(job_id)
    if payload is None:
        raise HTTPException(status_code=404, detail="OHLCV preload job not found")
    return payload


@router.get("/settings")
def get_settings(session: SessionToken = Depends(require_auth)) -> dict:
    settings = load_ini_section(_QUEUE_SETTINGS_SECTION)
    metadata = get_metadata(session)
    return {
        "autostart": str(settings.get("autostart", "False")).lower() == "true",
        "cpu": _normalize_autostart_cpu(settings.get("cpu", "1")),
        "cpu_override": str(settings.get("cpu_override", "True")).lower() == "true",
        "use_pbgui_market_data": str(settings.get("use_pbgui_market_data", "False")).lower() == "true",
        "cpu_max": multiprocessing.cpu_count(),
        "host_cpu_count": multiprocessing.cpu_count(),
        "optimize_backend_options": metadata.get("backends") or [],
        "optimize_backend_default": (metadata.get("optimize_defaults") or {}).get("backend", "pymoo"),
        "pymoo_algorithm_options": (metadata.get("pymoo") or {}).get("algorithms") or [],
        "pymoo_ref_dir_method_options": (metadata.get("pymoo") or {}).get("ref_dir_methods") or [],
        "optimize_defaults": metadata.get("optimize_defaults") or {},
        "strategies": metadata.get("strategies") or [],
        "hsl_signal_modes": ["coin", "pside", "unified"],
    }


@router.post("/settings")
def update_settings(body: dict, session: SessionToken = Depends(require_auth)) -> dict:
    updates = {}
    for key in ("autostart", "cpu_override", "use_pbgui_market_data"):
        if key not in body:
            continue
        if type(body[key]) is not bool:
            raise HTTPException(status_code=422, detail=f"{key} must be a boolean")
        updates[key] = str(body[key])
    if "cpu" in body:
        if type(body["cpu"]) is not int:
            raise HTTPException(status_code=422, detail="cpu must be an integer")
        updates["cpu"] = str(_normalize_autostart_cpu(body["cpu"]))
    if updates:
        save_ini_section(_QUEUE_SETTINGS_SECTION, updates)
    return {"ok": True}


@router.get("/bot-params")
def get_bot_params(session: SessionToken = Depends(require_auth)) -> dict:
    metadata = get_metadata(session)
    return {"params": [{"key": path.removeprefix("bot.long.")} for path in metadata.get("bot_parameter_paths") or [] if path.startswith("bot.long.")]}


@router.get("/configs/new-config")
def new_config(session: SessionToken = Depends(require_auth)) -> dict:
    return {"config": get_metadata(session)["template"], "param_status": {}}


@router.post("/configs/prepare")
def prepare_config(body: dict, session: SessionToken = Depends(require_auth)) -> dict:
    config = body.get("config") if isinstance(body, dict) else None
    if not isinstance(config, dict):
        raise HTTPException(status_code=400, detail="config must be an object")
    try:
        return {"config": prepare_pb8_config(config), "param_status": {}}
    except PB8ConfigurationError as exc:
        raise _configuration_error("Preparing PB8 optimize config", exc) from exc


@router.get("/configs")
def list_configs(session: SessionToken = Depends(require_auth)) -> dict:
    configs = []
    with _result_lock():
        result_summaries = _list_results()
    latest_result_by_name = {}
    for result in result_summaries:
        latest_result_by_name.setdefault(result["name"], result)
    root = _configs_dir()
    if root.is_dir() and not root.is_symlink():
        for directory in sorted(root.iterdir()):
            path = directory / _CONFIG_FILENAME
            if not directory.is_dir() or directory.is_symlink() or not path.is_file() or path.is_symlink():
                continue
            try:
                config = _read_json(path)
                backtest = config.get("backtest") if isinstance(config.get("backtest"), dict) else {}
                seed_mode, seed_source = _config_seed_metadata(config)
                result_summary = latest_result_by_name.get(directory.name) or {}
                configs.append(
                    {
                        "name": directory.name,
                        "exchange": backtest.get("exchanges") or [],
                        "start_date": backtest.get("start_date") or "",
                        "end_date": backtest.get("end_date") or "",
                        "strategy": (config.get("live") or {}).get("strategy_kind", ""),
                        "seed_mode": seed_mode,
                        "seed_source": seed_source,
                        "backtest_count": _managed_backtest_count(directory.name, config),
                        "result_mode": result_summary.get("mode", "unknown"),
                        "scenario_count": result_summary.get("scenario_count", 0),
                        "modified": datetime.datetime.fromtimestamp(path.stat().st_mtime).isoformat(),
                    }
                )
            except Exception as exc:
                _log(SERVICE, f"Failed to list PB8 optimize config {directory.name}: {exc}", level="WARNING")
    return {"configs": configs}


@router.get("/configs/{name}")
def get_config(name: str, session: SessionToken = Depends(require_auth)) -> dict:
    path = _config_file(name)
    try:
        with _config_lock():
            if not path.is_file() or path.is_symlink():
                raise HTTPException(status_code=404, detail=f"Config '{name}' not found")
            return {"name": name, "config": load_pb8_config(path), "param_status": {}}
    except PB8ConfigurationError as exc:
        raise _configuration_error(f"Loading PB8 optimize config {name}", exc) from exc


@router.put("/configs/{name}")
def save_config(name: str, body: dict, create_only: bool = False, session: SessionToken = Depends(require_auth)) -> dict:
    name = _validate_name(name)
    config = body.get("config") if isinstance(body, dict) and "config" in body else body
    if not isinstance(config, dict):
        raise HTTPException(status_code=400, detail="config must be an object")
    try:
        with _config_lock():
            prepared = _save_config_bundle(name, config, create_only=create_only)
        return {"ok": True, "name": name, "config": prepared}
    except PB8ConfigurationError as exc:
        raise _configuration_error(f"Saving PB8 optimize config {name}", exc) from exc


@router.post("/configs/{name}/duplicate")
def duplicate_config(name: str, body: dict, session: SessionToken = Depends(require_auth)) -> dict:
    source = _config_file(name)
    new_name = _validate_name(str((body or {}).get("new_name") or ""))
    with _config_lock():
        if not source.is_file() or source.is_symlink():
            raise HTTPException(status_code=404, detail=f"Config '{name}' not found")
        prepared = _save_config_bundle(new_name, load_pb8_config(source), create_only=True)
    return {"ok": True, "name": new_name, "config": prepared}


@router.delete("/configs/{name}")
def delete_config(name: str, session: SessionToken = Depends(require_auth)) -> dict:
    target = _config_dir(name)
    with _config_lock():
        if not target.is_dir() or target.is_symlink():
            raise HTTPException(status_code=404, detail=f"Config '{name}' not found")
        rmtree(target)
    return {"ok": True}


@router.post("/migrate-v7")
def migrate_v7(body: dict, session: SessionToken = Depends(require_auth)) -> dict:
    source_path = str((body or {}).get("source_path") or "").strip()
    if source_path:
        source = _resolve_v7_pareto_config(source_path)
        source_name = source.stem
    else:
        source_name = _validate_name(str((body or {}).get("source_name") or ""))
        source = _safe_path(_v7_configs_dir() / f"{source_name}.json", _v7_configs_dir())
        if not source.is_file() or source.is_symlink():
            raise HTTPException(status_code=404, detail=f"V7 optimize config '{source_name}' not found")
    target_name = _validate_name(str((body or {}).get("target_name") or f"{source_name}_v8"))
    with _config_lock():
        if _config_dir(target_name).exists():
            raise HTTPException(status_code=409, detail=f"Config '{target_name}' already exists")
        stage = _safe_path(_configs_dir() / f".migrate-{uuid.uuid4().hex}", _configs_dir())
        ensure_private_directory(stage)
        try:
            result = migrate_pb7_config(source, stage / _CONFIG_FILENAME)
            report = result.get("report") if isinstance(result.get("report"), dict) else {}
            config = result.get("config")
            unresolved = report.get("manual_review_fields") or report.get("dropped_unsupported_fields")
            if not report.get("output_written") or not isinstance(config, dict) or unresolved:
                raise HTTPException(status_code=422, detail="Migration requires manual review")
            prepared = _save_config_bundle(target_name, config, create_only=True)
            _write_json(_config_dir(target_name) / "migration_report.json", report)
            return {"ok": True, "name": target_name, "config": prepared, "report": report}
        finally:
            rmtree(stage, ignore_errors=True)


@router.get("/queue")
def get_queue(session: SessionToken = Depends(require_auth)) -> dict:
    return {"items": _load_queue()}


@router.post("/queue/reorder")
def reorder_queue(body: dict, session: SessionToken = Depends(require_auth)) -> dict:
    filenames = (body or {}).get("filenames")
    if not isinstance(filenames, list):
        raise HTTPException(status_code=400, detail="filenames must be a list")
    normalized = [_validate_name(str(item)) for item in filenames]
    if len(set(normalized)) != len(normalized):
        raise HTTPException(status_code=400, detail="filenames must not contain duplicates")
    with _queue_lock():
        _recover_pending_reorder_unlocked()
        existing = {item["filename"] for item in _load_queue()}
        if set(normalized) != existing:
            raise HTTPException(status_code=400, detail="filenames must include all queue items exactly once")
        ensure_private_directory_tree(_queue_dir(), _queue_dir() / "state")
        _write_json(_reorder_file(), {"filenames": normalized, "created_at": time.time()})
        for index, filename in enumerate(normalized):
            data = _read_json(_queue_file(filename))
            data["order"] = index
            _write_json(_queue_file(filename), data)
        _reorder_file().unlink(missing_ok=True)
    return {"ok": True, "count": len(normalized)}


@router.post("/queue")
def add_to_queue(body: dict, session: SessionToken = Depends(require_auth)) -> dict:
    name = _validate_name(str((body or {}).get("name") or ""))
    config = (body or {}).get("config")
    try:
        with _config_lock():
            if isinstance(config, dict):
                prepared = _save_config_bundle(name, config)
            else:
                path = _config_file(name)
                if not path.is_file() or path.is_symlink():
                    raise HTTPException(status_code=404, detail=f"Config '{name}' not found")
                prepared = load_pb8_config(path)
        options = _validate_launch_options((body or {}).get("launch_options") or _runtime_options_from_config(prepared))
    except PB8ConfigurationError as exc:
        raise _configuration_error(f"Queueing PB8 optimize config {name}", exc) from exc
    return _create_queue_record(name, prepared, options)


def _create_queue_record(name: str, prepared: dict, options: dict) -> dict:
    """Publish one queue record and remove partial artifacts if persistence fails."""
    filename = str(uuid.uuid4())
    try:
        with _queue_lock():
            ensure_private_directory_tree(_queue_dir(), _snapshot_dir(filename))
            _write_json(_snapshot_file(filename), prepared)
            current = _load_queue()
            order = min((item["order"] for item in current if isinstance(item.get("order"), int)), default=0) - 1
            backtest = prepared.get("backtest") if isinstance(prepared.get("backtest"), dict) else {}
            data = {
                "filename": filename,
                "name": name,
                "config_path": str(_config_file(name)),
                "snapshot_path": str(_snapshot_file(filename)),
                "exchange": backtest.get("exchanges") or [],
                "launch_options": options,
                "order": order,
            }
            _write_json(_queue_file(filename), data)
    except Exception:
        with _queue_lock():
            _queue_file(filename).unlink(missing_ok=True)
            rmtree(_snapshot_dir(filename), ignore_errors=True)
            rmtree(_launch_dir(filename), ignore_errors=True)
        raise
    return {"ok": True, "filename": filename}


@router.post("/queue/{filename}/start")
def start_queue_item(filename: str, body: dict | None = None, session: SessionToken = Depends(require_auth)) -> dict:
    options = (body or {}).get("launch_options") if isinstance(body, dict) else None
    try:
        record = _worker.launch(filename, options, False)
    except Exception as exc:
        _record_launch_failure(filename, exc)
        raise
    return {"ok": True, "pid": record["pid"]}


def _reset_queue_item(filename: str, options: dict) -> dict:
    filename = _validate_name(filename)
    with _queue_lock():
        path = _queue_file(filename)
        if not path.is_file():
            raise HTTPException(status_code=404, detail="Queue item not found")
        data = _read_json(path)
        validated_options = _validate_launch_options(options)
        snapshot = _snapshot_file(filename)
        if not snapshot.is_file() or snapshot.is_symlink():
            raise HTTPException(status_code=422, detail="Queue config snapshot is missing")
        if validated_options["mode"] == "checkpoint_resume":
            _preflight_checkpoint_resume(_read_json(snapshot), Path(validated_options["source"]))
        _terminate_verified(filename)
        data["launch_options"] = validated_options
        for artifact in (_pid_file(filename), _state_file(filename), _ready_file(filename)):
            artifact.unlink(missing_ok=True)
        rmtree(_launch_dir(filename), ignore_errors=True)
        data.pop("started_at", None)
        data.pop("status_override", None)
        data.pop("automatic", None)
        data.pop("error_code", None)
        data.pop("error_reason", None)
        data["launch_message"] = {
            "fresh": "Fresh requeue requested",
            "pareto_seed": "Pareto seed selected for next launch",
            "checkpoint_resume": "Checkpoint resume selected for next launch",
        }[data["launch_options"]["mode"]]
        _write_json(path, data)
    return {"ok": True, "mode": data["launch_options"]["mode"]}


@router.post("/queue/{filename}/requeue")
@router.post("/queue/{filename}/requeue-fresh")
@router.post("/queue/{filename}/restart")
def requeue_fresh(filename: str, session: SessionToken = Depends(require_auth)) -> dict:
    return _reset_queue_item(filename, {"mode": "fresh"})


@router.post("/queue/{filename}/continue-pareto")
def continue_from_pareto(filename: str, body: dict, session: SessionToken = Depends(require_auth)) -> dict:
    return _reset_queue_item(filename, {**(body or {}), "mode": "pareto_seed", "source": (body or {}).get("source")})


@router.post("/queue/{filename}/resume-checkpoint")
def resume_checkpoint(filename: str, body: dict, session: SessionToken = Depends(require_auth)) -> dict:
    return _reset_queue_item(filename, {**(body or {}), "mode": "checkpoint_resume", "source": (body or {}).get("source")})


@router.post("/queue/{filename}/stop")
def stop_queue_item(filename: str, session: SessionToken = Depends(require_auth)) -> dict:
    with _queue_lock():
        path = _queue_file(filename)
        if not path.is_file():
            raise HTTPException(status_code=404, detail="Queue item not found")
        _terminate_verified(filename)
        data = _read_json(path)
        reason = "Stopped by user; requeue this item to run it again."
        data["status_override"] = "error"
        data["error_code"] = "stopped"
        data["error_reason"] = reason
        data["launch_message"] = reason
        _write_json(path, data)
        _append_queue_log(filename, reason)
    return {"ok": True}


@router.get("/queue/{filename}/config")
def get_queue_config(filename: str, session: SessionToken = Depends(require_auth)) -> dict:
    path = _snapshot_file(filename)
    if not path.is_file() or path.is_symlink():
        raise HTTPException(status_code=404, detail="Queue config snapshot not found")
    return {"name": _read_json(_queue_file(filename)).get("name", filename), "config": _read_json(path), "param_status": {}}


@router.post("/queue/{filename}/repair-config")
def repair_queue_config(filename: str, body: dict, session: SessionToken = Depends(require_auth)) -> dict:
    name = _validate_name(str((body or {}).get("name") or ""))
    with _config_lock():
        config_path = _config_file(name)
        if not config_path.is_file() or config_path.is_symlink():
            raise HTTPException(status_code=404, detail=f"Config '{name}' not found")
        try:
            prepared = load_pb8_config(config_path)
        except PB8ConfigurationError as exc:
            raise _configuration_error(f"Repairing PB8 optimize queue config {name}", exc) from exc
    with _queue_lock():
        queue_path = _queue_file(filename)
        if not queue_path.is_file():
            raise HTTPException(status_code=404, detail="Queue item not found")
        data = _read_json(queue_path)
        ensure_private_directory_tree(_queue_dir(), _snapshot_dir(filename))
        _write_json(_snapshot_file(filename), prepared)
        data["name"] = name
        data["config_path"] = str(config_path)
        data["snapshot_path"] = str(_snapshot_file(filename))
        backtest = prepared.get("backtest") if isinstance(prepared.get("backtest"), dict) else {}
        data["exchange"] = backtest.get("exchanges") or []
        _write_json(queue_path, data)
    return {"ok": True, "filename": filename, "name": name, "json": data["config_path"]}


def _remove_queue_item(filename: str, *, require_exists: bool = True) -> bool:
    path = _queue_file(filename)
    if not path.is_file():
        if require_exists:
            raise HTTPException(status_code=404, detail="Queue item not found")
        return False
    _terminate_verified(filename)
    path.unlink(missing_ok=True)
    _pid_file(filename).unlink(missing_ok=True)
    _state_file(filename).unlink(missing_ok=True)
    _ready_file(filename).unlink(missing_ok=True)
    (_log_dir() / f"{filename}.log").unlink(missing_ok=True)
    rmtree(_snapshot_dir(filename), ignore_errors=True)
    rmtree(_launch_dir(filename), ignore_errors=True)
    return True


@router.delete("/queue/{filename}")
def remove_queue_item(filename: str, session: SessionToken = Depends(require_auth)) -> dict:
    with _queue_lock():
        _remove_queue_item(filename)
    return {"ok": True}


@router.post("/queue/delete")
def delete_queue_items(body: dict, session: SessionToken = Depends(require_auth)) -> dict:
    filenames = (body or {}).get("filenames")
    if not isinstance(filenames, list):
        raise HTTPException(status_code=400, detail="filenames must be a list")
    removed = 0
    missing = []
    with _queue_lock():
        for raw in filenames:
            filename = _validate_name(str(raw))
            if _remove_queue_item(filename, require_exists=False):
                removed += 1
            else:
                missing.append(filename)
    return {"ok": True, "removed": removed, "missing": missing}


@router.post("/queue/clear-finished")
def clear_finished(session: SessionToken = Depends(require_auth)) -> dict:
    removed = 0
    with _queue_lock():
        for item in _load_queue():
            if item["status"] == "complete" and _remove_queue_item(item["filename"], require_exists=False):
                removed += 1
    return {"ok": True, "removed": removed}


@router.get("/queue/{filename}/log")
def get_queue_log(filename: str, lines: int = 100, session: SessionToken = Depends(require_auth)) -> dict:
    path = _safe_path(_log_dir() / f"{_validate_name(filename)}.log", _log_dir())
    if not path.is_file() or path.is_symlink():
        return {"log": "", "exists": False}
    with path.open("rb") as handle:
        handle.seek(0, 2)
        handle.seek(max(0, handle.tell() - max(10, min(lines, 5000)) * 1024))
        return {"log": handle.read().decode("utf-8", errors="ignore"), "exists": True}


def _read_optimize_log_excerpt(path: Path, head_bytes: int = 32 * 1024, tail_bytes: int = 512 * 1024) -> str:
    if not path.is_file() or path.is_symlink():
        return ""
    try:
        with path.open("rb") as handle:
            handle.seek(0, 2)
            size = handle.tell()
            if size <= head_bytes + tail_bytes:
                handle.seek(0)
                return handle.read().decode("utf-8", errors="ignore")
            handle.seek(0)
            head = handle.read(head_bytes)
            handle.seek(size - tail_bytes)
            return head.decode("utf-8", errors="ignore") + "\n...\n" + handle.read().decode("utf-8", errors="ignore")
    except OSError:
        return ""


def _parse_log_number(value) -> float | None:
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def _parse_optimize_log_status(text: str) -> dict:
    summary = {
        "phase": "queued",
        "backend": None,
        "algorithm": None,
        "objective_count": None,
        "iter": None,
        "evaluations": None,
        "front": None,
        "ranges": {},
        "objectives": {},
        "last_log_at": None,
        "last_line": "",
        "last_error": None,
    }
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line == "...":
            continue
        message = line
        match = _OPT_LOG_LINE_RE.match(line)
        if match:
            message = match.group("msg").strip()
            summary["last_log_at"] = match.group("ts")
            if str(match.group("level") or "").upper() in {"ERROR", "CRITICAL"}:
                summary["last_error"] = message
                summary["phase"] = "error"
        summary["last_line"] = message
        backend = _OPT_LOG_BACKEND_RE.search(message)
        if backend:
            summary["backend"] = backend.group("backend").lower()
        pymoo = _OPT_LOG_PYMOO_RE.search(message)
        if pymoo:
            summary["algorithm"] = pymoo.group("algorithm").lower()
            if pymoo.group("n_obj"):
                summary["objective_count"] = int(pymoo.group("n_obj"))
            summary["phase"] = "optimizing"
        iteration = _OPT_LOG_ITER_RE.search(message)
        if iteration:
            summary["iter"] = int(iteration.group("iter"))
            summary["front"] = int(iteration.group("size"))
            ranges = iteration.group("ranges") or ""
            for found in _OPT_LOG_RANGE_RE.finditer(ranges):
                summary["ranges"][found.group("name")] = {
                    "min": _parse_log_number(found.group("min")),
                    "max": _parse_log_number(found.group("max")),
                }
            summary["phase"] = "optimizing"
        evaluation = _OPT_LOG_EVAL_RE.search(message)
        if evaluation:
            summary["evaluations"] = int(evaluation.group("eval"))
            summary["phase"] = "optimizing"
        objective_block = re.search(r"objectives=\[([^\]]*)\]", message, re.IGNORECASE)
        if objective_block:
            for raw_objective in objective_block.group(1).split(","):
                name, separator, value = raw_objective.partition("=")
                numeric = _parse_log_number(value)
                if separator and name.strip() and numeric is not None:
                    summary["objectives"][name.strip()] = numeric
        lower = message.lower()
        if "optimization complete" in lower or "successfully processed optimize_results" in lower:
            summary["phase"] = "complete"
        elif "starting optimize" in lower and summary["phase"] == "queued":
            summary["phase"] = "optimizing"
        elif "initializ" in lower and summary["phase"] == "queued":
            summary["phase"] = "initializing"
    return summary


def _verified_process_stats(filename: str) -> dict:
    record = _read_process_record(filename)
    stats = {
        "running": False,
        "pid": int(record["pid"]) if record else None,
        "status": None,
        "rss_bytes": None,
        "memory_percent": None,
        "cpu_percent": None,
        "cpu_cores_est": None,
        "threads": None,
        "children": 0,
        "create_time": float(record["create_time"]) if record else None,
        "started_at": None,
    }
    if not record or not _process_matches(filename, record):
        return stats
    try:
        parent = psutil.Process(int(record["pid"]))
        processes = [parent, *parent.children(recursive=True)]
        rss = threads = 0
        memory_percent = cpu_percent = 0.0
        for process in processes:
            try:
                rss += int(process.memory_info().rss)
                threads += int(process.num_threads())
                memory_percent += float(process.memory_percent())
                cpu_percent += float(process.cpu_percent(interval=None) or 0.0)
            except (psutil.NoSuchProcess, psutil.ZombieProcess, psutil.AccessDenied):
                continue
        stats.update(
            running=True,
            status=parent.status(),
            rss_bytes=rss,
            memory_percent=round(memory_percent, 2),
            cpu_percent=round(cpu_percent, 1),
            cpu_cores_est=round(cpu_percent / 100.0, 2),
            threads=threads,
            children=max(0, len(processes) - 1),
            started_at=datetime.datetime.fromtimestamp(float(record["create_time"]), datetime.UTC).isoformat(),
        )
    except (psutil.NoSuchProcess, psutil.ZombieProcess, psutil.AccessDenied, OSError):
        pass
    return stats


def _system_stats() -> dict:
    memory = psutil.virtual_memory()
    swap = psutil.swap_memory()
    try:
        per_core = [round(float(value), 1) for value in psutil.cpu_percent(interval=0.05, percpu=True)]
    except Exception:
        per_core = []
    try:
        load_avg = tuple(round(value, 2) for value in os.getloadavg()) if hasattr(os, "getloadavg") else None
    except OSError:
        load_avg = None
    overall = round(sum(per_core) / len(per_core), 1) if per_core else round(float(psutil.cpu_percent(interval=0.05)), 1)
    return {
        "cpu_percent": overall,
        "cpu_per_core": per_core,
        "cpu_core_count": len(per_core),
        "memory_percent": round(float(memory.percent), 1),
        "memory_used_bytes": int(memory.used),
        "memory_total_bytes": int(memory.total),
        "swap_percent": round(float(swap.percent), 1),
        "swap_used_bytes": int(swap.used),
        "swap_total_bytes": int(swap.total),
        "load_avg": load_avg,
    }


@router.get("/queue/{filename}/status")
def get_queue_status(filename: str, session: SessionToken = Depends(require_auth)) -> dict:
    item = next((item for item in _load_queue() if item["filename"] == filename), None)
    if item is None:
        raise HTTPException(status_code=404, detail="Queue item not found")
    state = _read_runner_state(filename) or {}
    log_path = _safe_path(_log_dir() / f"{filename}.log", _log_dir())
    log_summary = _parse_optimize_log_status(_read_optimize_log_excerpt(log_path))
    config = {}
    for path in (_launch_config_file(filename), _snapshot_file(filename)):
        try:
            if path.is_file() and not path.is_symlink():
                config = _read_json(path)
                break
        except RuntimeError:
            pass
    optimize = config.get("optimize") if isinstance(config.get("optimize"), dict) else {}
    pymoo = optimize.get("pymoo") if isinstance(optimize.get("pymoo"), dict) else {}
    specs = _pareto_objective_specs(config)
    target = None
    for key in ("iters", "max_evaluations", "n_evaluations"):
        try:
            if optimize.get(key) not in (None, ""):
                target = int(optimize[key])
                break
        except (TypeError, ValueError):
            continue
    evaluations = log_summary["evaluations"]
    if evaluations is None and item["status"] == "complete" and target is not None:
        evaluations = target
    percent = None
    if evaluations is not None and target and target > 0:
        percent = max(0.0, min(100.0, evaluations / target * 100.0))
    queue_items = _load_queue()
    queue_totals = {
        status: sum(1 for queued in queue_items if queued["status"] == status)
        for status in ("queued", "running", "complete", "error")
    }
    log_meta = {
        "exists": False,
        "size_bytes": 0,
        "updated_at": None,
        "activity_seconds": None,
        "last_log_at": log_summary["last_log_at"],
        "last_line": log_summary["last_line"],
        "last_error": log_summary["last_error"] or item.get("error_reason") or None,
    }
    try:
        if log_path.is_file() and not log_path.is_symlink():
            log_stat = log_path.stat()
            log_meta.update(
                exists=True,
                size_bytes=log_stat.st_size,
                updated_at=datetime.datetime.fromtimestamp(log_stat.st_mtime, datetime.UTC).isoformat(),
                activity_seconds=max(0.0, time.time() - log_stat.st_mtime),
            )
    except OSError:
        pass
    phase = log_summary["phase"]
    if item["status"] in {"complete", "error"}:
        phase = item["status"]
    elif item["status"] == "running" and phase == "queued":
        phase = "running"
    return {
        **item,
        "phase": phase,
        "progress": {
            "eval": evaluations,
            "evaluations": evaluations,
            "iter": log_summary["iter"],
            "target_iters": target,
            "target_evaluations": target,
            "percent": percent,
            "front": log_summary["front"],
        },
        "runtime": {
            "launch_mode": item["launch_mode"],
            "backend": log_summary["backend"] or optimize.get("backend"),
            "algorithm": log_summary["algorithm"] or pymoo.get("algorithm"),
            "objective_count": log_summary["objective_count"] or (len(specs) if specs else None),
            "objectives": specs,
            "config_n_cpus": optimize.get("n_cpus"),
            "seed_mode": item["launch_mode"],
            "exchange": item.get("exchange"),
        },
        "metrics": {"objectives": log_summary["objectives"], "ranges": log_summary["ranges"]},
        "process": _verified_process_stats(filename),
        "system": _system_stats(),
        "queue": queue_totals,
        "log": log_meta,
        "runner": state,
    }


@router.get("/results")
def list_results(session: SessionToken = Depends(require_auth)) -> dict:
    with _result_lock():
        return {"results": _list_results()}


@router.get("/results/config")
def get_result_config(path: str, session: SessionToken = Depends(require_auth)) -> dict:
    with _result_lock():
        result_dir = _resolve_result_path(path)
        config = _recover_result_config(result_dir)
    if config is None:
        raise HTTPException(status_code=404, detail="No recoverable PB8 config found for result")
    try:
        return {"config": prepare_pb8_config(config), "param_status": {}}
    except PB8ConfigurationError as exc:
        raise _configuration_error("Loading PB8 result config", exc) from exc


@router.post("/results/resume")
def queue_result_resume(body: dict, session: SessionToken = Depends(require_auth)) -> dict:
    """Validate and atomically create a managed config plus checkpoint queue item."""
    name = _validate_name(str((body or {}).get("name") or ""))
    created_config = False
    with _result_lock():
        result_dir = _resolve_result_path(str((body or {}).get("path") or ""))
        readiness = _checkpoint_resume_readiness(result_dir)
        if not readiness["ready"]:
            raise HTTPException(
                status_code=422,
                detail="Checkpoint cannot be resumed: " + "; ".join(readiness["reasons"]),
            )
        config = copy.deepcopy(readiness["config"])
        try:
            candidate = prepare_pb8_config(_normalize_config(config, name))
        except PB8ConfigurationError as exc:
            raise _configuration_error("Preparing PB8 checkpoint resume", exc) from exc
        _preflight_checkpoint_resume(candidate, result_dir)
        options = _validate_launch_options({"mode": "checkpoint_resume", "source": str(result_dir)})
        try:
            with _config_lock():
                prepared = _save_config_bundle(name, candidate, create_only=True)
                created_config = True
            return _create_queue_record(name, prepared, options)
        except Exception:
            if created_config:
                try:
                    with _config_lock():
                        target = _config_dir(name)
                        if target.is_dir() and not target.is_symlink():
                            rmtree(target)
                except Exception as cleanup_exc:
                    _log(
                        SERVICE,
                        f"Failed to roll back PB8 checkpoint resume config {name}: {cleanup_exc}",
                        level="ERROR",
                    )
            raise


def _source_references_result(source: str, result_dir: Path) -> bool:
    raw = str(source or "").strip()
    if not raw or raw == "__self__":
        return False
    try:
        source_path = Path(os.path.abspath(raw))
        source_path.relative_to(result_dir)
        return True
    except (OSError, ValueError):
        return False


def _assert_result_deletable(result_dir: Path) -> None:
    try:
        pid_paths = list(_queue_dir().glob("*.pid"))
    except OSError as exc:
        raise HTTPException(status_code=409, detail="Active PB8 optimizer ownership could not be verified safely") from exc
    results_root = Path(os.path.abspath(_results_root()))
    for pid_path in pid_paths:
        try:
            filename = _validate_name(pid_path.stem)
            record = _read_process_record(filename)
            if record and _process_matches(filename, record):
                owned_results = set()
                for persisted in record.get("owned_results") or []:
                    try:
                        relative = Path(os.path.abspath(str(persisted))).relative_to(results_root)
                    except (OSError, ValueError):
                        continue
                    if relative.parts:
                        owned_results.add(results_root / relative.parts[0])
                observed_results = set()
                inspection_complete = True
                try:
                    process = psutil.Process(int(record["pid"]))
                except (psutil.NoSuchProcess, psutil.ZombieProcess, psutil.AccessDenied, OSError, ValueError, TypeError):
                    processes = []
                    inspection_complete = False
                else:
                    processes = [process]
                    try:
                        processes.extend(process.children(recursive=True))
                    except (psutil.NoSuchProcess, psutil.ZombieProcess, psutil.AccessDenied, OSError):
                        inspection_complete = False
                for candidate in processes:
                    try:
                        open_files = candidate.open_files()
                    except (psutil.NoSuchProcess, psutil.ZombieProcess, psutil.AccessDenied, OSError):
                        inspection_complete = False
                        continue
                    for opened in open_files:
                        try:
                            relative = Path(os.path.abspath(str(opened.path))).relative_to(results_root)
                        except (AttributeError, OSError, ValueError):
                            continue
                        if relative.parts:
                            observed_results.add(results_root / relative.parts[0])
                if observed_results.difference(owned_results):
                    owned_results.update(observed_results)
                    _write_json(
                        _pid_file(filename),
                        {
                            "pid": record["pid"],
                            "create_time": record["create_time"],
                            "owned_results": sorted(str(path) for path in owned_results),
                        },
                    )
                if result_dir in owned_results:
                    raise HTTPException(
                        status_code=409,
                        detail=f"PB8 optimizer queue item '{filename}' owns result '{result_dir.name}'",
                    )
                if not owned_results:
                    raise HTTPException(
                        status_code=409,
                        detail=f"Could not verify result ownership for active PB8 optimizer queue item '{filename}'; deletion of '{result_dir.name}' is blocked",
                    )
        except HTTPException:
            raise
        except Exception as exc:
            _log(SERVICE, f"Could not inspect PB8 optimizer result ownership: {exc}", level="WARNING")
            raise HTTPException(status_code=409, detail="Active PB8 optimizer ownership could not be verified safely") from exc
    try:
        queue_paths = list(_queue_dir().glob("*.json"))
    except OSError as exc:
        raise HTTPException(status_code=409, detail="PB8 queue references could not be verified safely") from exc
    for queue_path in queue_paths:
        try:
            filename = _validate_name(queue_path.stem)
            data = _read_json(queue_path)
            sources = []
            options = data.get("launch_options") if isinstance(data.get("launch_options"), dict) else {}
            sources.append(options.get("source"))
            launch_options_path = _launch_options_file(filename)
            if launch_options_path.is_file() and not launch_options_path.is_symlink():
                sources.append(_read_json(launch_options_path).get("source"))
            if any(_source_references_result(source, result_dir) for source in sources):
                raise HTTPException(
                    status_code=409,
                    detail=f"Queue item '{filename}' references this result as a continuation source",
                )
        except HTTPException:
            raise
        except Exception as exc:
            _log(SERVICE, f"Could not verify PB8 queue source before result deletion: {exc}", level="WARNING")
            raise HTTPException(status_code=409, detail="PB8 queue references could not be verified safely") from exc
    with _dash_lock:
        dash_records = [record for record in [*_dash_sessions.values(), *_dash_pending_sessions.values()] if record]
    try:
        dash_records.extend(_read_dash_registry().values())
    except Exception as exc:
        raise HTTPException(status_code=409, detail="Pareto Dash ownership could not be verified safely") from exc
    if any(Path(os.path.abspath(str(record.get("result_dir") or ""))) == result_dir for record in dash_records):
        raise HTTPException(status_code=409, detail="A live or persisted Pareto Dash session references this result")


def _forget_result_progress(result_dir: Path) -> None:
    prefix = str(result_dir) + os.sep
    with _result_progress_cache_lock:
        for key in list(_result_progress_cache):
            if key == str(result_dir) or key.startswith(prefix):
                _result_progress_cache.pop(key, None)


def _forget_result_caches(result_dir: Path) -> None:
    _forget_result_progress(result_dir)
    prefix = str(result_dir.resolve()) + os.sep
    with _pareto_list_cache_lock:
        for key in list(_pareto_list_cache):
            if key[0].startswith(prefix):
                _pareto_list_cache.pop(key, None)
        _pareto_warning_cache.pop(str(result_dir), None)


def _stage_delete_result(result_dir: Path) -> Path:
    staged = _safe_path(result_dir.parent / f".{result_dir.name}.delete-{uuid.uuid4().hex}", _results_root())
    os.replace(result_dir, staged)
    _forget_result_caches(result_dir)
    return staged


def _rollback_staged_results(staged: list[tuple[Path, Path]]) -> None:
    for original, temporary in reversed(staged):
        try:
            if temporary.exists() and not original.exists():
                os.replace(temporary, original)
        except OSError as exc:
            _log(SERVICE, f"Failed to roll back staged PB8 result {original.name}: {exc}", level="ERROR")


@router.delete("/results")
def delete_result(path: str, session: SessionToken = Depends(require_auth)) -> dict:
    with _result_lock():
        result_dir = _resolve_result_path(path)
        with _queue_lock():
            _assert_result_deletable(result_dir)
            try:
                staged = _stage_delete_result(result_dir)
            except FileNotFoundError:
                _forget_result_caches(result_dir)
                return {"ok": True, "removed": 0, "missing": [path]}
        try:
            rmtree(staged)
        except FileNotFoundError:
            pass
    return {"ok": True, "removed": 1, "missing": []}


@router.post("/results/delete")
def delete_results(body: dict, session: SessionToken = Depends(require_auth)) -> dict:
    paths = (body or {}).get("paths")
    if not isinstance(paths, list):
        raise HTTPException(status_code=400, detail="paths must be a list")
    missing = []
    resolved = []
    with _result_lock():
        for raw in paths:
            try:
                result_dir = _resolve_result_path(str(raw))
            except HTTPException as exc:
                if exc.status_code == 404:
                    missing.append(str(raw))
                    continue
                raise
            if result_dir not in resolved:
                resolved.append(result_dir)
        staged = []
        with _queue_lock():
            for result_dir in resolved:
                _assert_result_deletable(result_dir)
            try:
                for result_dir in resolved:
                    try:
                        staged.append((result_dir, _stage_delete_result(result_dir)))
                    except FileNotFoundError:
                        _forget_result_caches(result_dir)
                        missing.append(str(result_dir))
            except Exception:
                _rollback_staged_results(staged)
                raise
        for _original, temporary in staged:
            try:
                rmtree(temporary)
            except FileNotFoundError:
                pass
    return {"ok": True, "removed": len(staged), "missing": list(dict.fromkeys(missing))}


@router.post("/results/3d-plot")
def launch_result_3d_plot(body: dict, session: SessionToken = Depends(require_auth)) -> dict:
    """Normalize nested PB8 metrics in a temporary managed stage for the shared plotter."""
    from api.optimize_v7 import _build_pareto_3d_plot_payload

    with _result_lock():
        result_dir = _resolve_result_path(str((body or {}).get("path") or ""))
        pareto_dir = result_dir / "pareto"
        paths = sorted(pareto_dir.glob("*.json")) if pareto_dir.is_dir() and not pareto_dir.is_symlink() else []
        if not paths:
            raise HTTPException(status_code=404, detail="No pareto config found for result")
        rows = []
        first_data = None
        for path in paths:
            try:
                data = _read_json(path)
            except RuntimeError:
                continue
            first_data = first_data or data
            rows.append((path, data))
        if first_data is None:
            raise HTTPException(status_code=422, detail="No readable PB8 Pareto candidates were found")
        contract = _pareto_contract(first_data)
        statistic = str((body or {}).get("statistic") or "mean").strip().lower()
        statistic = statistic if statistic in _PARETO_STATISTICS else "mean"
        scenario = str((body or {}).get("scenario") or "Aggregated").strip() or "Aggregated"
        if scenario not in {"Aggregated", *contract["scenario_labels"]}:
            scenario = "Aggregated"
        summaries = [(path, data, _pareto_summary(data, statistic, scenario)) for path, data in rows]
        specs = contract["objectives"]
        if specs:
            usable_specs = [spec for spec in specs if any(spec["metric"] in summary for _path, _data, summary in summaries)]
        else:
            names = list(summaries[0][2]) if summaries else []
            usable_specs = [{"metric": name, "goal": "max"} for name in names]
        if len(usable_specs) != 3:
            names = [spec["metric"] for spec in usable_specs]
            return {
                "ok": False,
                "message": "PB8 3D plot requires exactly 3 usable objectives for the selected statistic and scenario.",
                "output": json.dumps({"objectives": names, "statistic": statistic, "scenario": scenario}, indent=2),
            }
        complete = [
            (path, data, summary)
            for path, data, summary in summaries
            if all(spec["metric"] in summary for spec in usable_specs)
        ]
        if not complete:
            return {
                "ok": False,
                "message": "PB8 reported no complete 3D Pareto points for the selected statistic and scenario.",
                "output": json.dumps({"objectives": [spec["metric"] for spec in usable_specs], "points": 0}, indent=2),
            }
        stage_root = ensure_private_directory(_data_dir() / "cache" / "pareto_plot_v8")
        stage = _safe_path(stage_root / uuid.uuid4().hex, stage_root)
        stage_pareto = stage / "pareto"
        ensure_private_directory_tree(stage_root, stage_pareto)
        try:
            for path, data, summary in complete:
                normalized = {
                    "backtest": copy.deepcopy(data.get("backtest") or {}),
                    "optimize": {"scoring": copy.deepcopy(usable_specs)},
                    "result": {"objectives": {spec["metric"]: summary[spec["metric"]] for spec in usable_specs}},
                }
                _write_json(stage_pareto / path.name, normalized)
            payload = _build_pareto_3d_plot_payload(stage)
        finally:
            rmtree(stage, ignore_errors=True)
    if isinstance(payload.get("message"), str):
        payload["message"] = payload["message"].replace("PB7", "PB8")
    return payload


@router.post("/results/pareto-dash")
def launch_result_pareto_dash(
    body: dict,
    request: Request,
    session: SessionToken = Depends(require_auth),
) -> dict:
    with _result_lock():
        result_dir = _resolve_result_path(str((body or {}).get("path") or ""))
        if _first_pareto_file(result_dir) is None:
            raise HTTPException(status_code=400, detail="Result has no pareto data")
        session_id = uuid.uuid4().hex[:12]
        proxy_root = str(request.app.url_path_for("optimize_v8_pareto_dash_proxy_root", session_id=session_id))
        launched = _launch_dash_session(session_id, result_dir, proxy_root)
    return {"ok": True, "session_id": session_id, "url": proxy_root, "result": Path(launched["result_dir"]).name}


@router.api_route(
    "/results/pareto-dash/{session_id}/",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    name="optimize_v8_pareto_dash_proxy_root",
)
@router.api_route(
    "/results/pareto-dash/{session_id}/{dash_path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    name="optimize_v8_pareto_dash_proxy_path",
)
async def proxy_result_pareto_dash(
    session_id: str,
    request: Request,
    dash_path: str = "",
    session: SessionToken = Depends(require_auth),
) -> Response:
    _reap_dash_sessions()
    with _dash_lock:
        launched = _dash_sessions.get(session_id)
        if launched is not None:
            launched["last_access"] = time.time()
            try:
                _persist_dash_session(launched)
            except Exception as exc:
                _log(SERVICE, f"Failed to refresh PB8 Pareto Dash session {session_id}: {exc}", level="WARNING")
    if not launched:
        raise HTTPException(status_code=404, detail="PB8 Pareto Dash session not found")
    target_path = "/" if not dash_path else f"/{dash_path.lstrip('/')}"
    target_url = f"http://127.0.0.1:{launched['port']}{target_path}"
    if request.url.query:
        target_url = f"{target_url}?{request.url.query}"
    headers = _dash_request_headers(request.headers)
    try:
        async with httpx.AsyncClient(timeout=60, follow_redirects=False) as client:
            upstream = await client.request(request.method, target_url, headers=headers, content=await request.body())
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"PB8 Pareto Dash proxy failed: {exc}") from exc
    response_headers = _dash_response_headers(upstream.headers)
    location = response_headers.get("location")
    if location:
        upstream_base = f"http://127.0.0.1:{launched['port']}"
        if location.startswith(upstream_base):
            location = location[len(upstream_base):] or "/"
        if location.startswith("/"):
            response_headers["location"] = _proxy_path(launched["proxy_root"], location)
    content = _rewrite_dash_content(
        upstream.content,
        response_headers.get("content-type", ""),
        launched["proxy_root"],
    )
    return Response(content=content, status_code=upstream.status_code, headers=response_headers)


@router.delete("/results/pareto-dash/{session_id}")
def stop_result_pareto_dash(session_id: str, session: SessionToken = Depends(require_auth)) -> dict:
    _stop_dash_session(session_id)
    return {"ok": True}


@router.get("/paretos")
def list_paretos(
    result_path: str,
    scenario: str = Query("Aggregated"),
    statistic: str = Query("mean"),
    session: SessionToken = Depends(require_auth),
) -> dict:
    with _result_lock():
        result_dir = _resolve_result_path(result_path)
        selected_statistic = statistic if statistic in _PARETO_STATISTICS else "mean"
        pareto_dir = result_dir / "pareto"
        try:
            paths = sorted(pareto_dir.glob("*.json")) if pareto_dir.is_dir() and not pareto_dir.is_symlink() else []
        except OSError as exc:
            paths = []
            _log_pareto_skips(result_dir, 1, str(exc))
        candidates = []
        errors = []
        for path in paths:
            try:
                compact, stat_result = _load_compact_pareto(path)
                candidates.append((path, compact, stat_result))
            except (OSError, RuntimeError) as exc:
                errors.append(f"{path.name}: {exc}")
                continue
        if errors:
            _log_pareto_skips(result_dir, len(errors), errors[0])
        contract = candidates[0][1] if candidates else {
            "mode": "unknown",
            "scenario_count": 0,
            "scenario_labels": [],
            "objectives": [],
        }
        selected_scenario = scenario if scenario in {"Aggregated", *contract["scenario_labels"]} else "Aggregated"
        paretos = [
            {
                "path": str(path),
                "name": path.stem,
                "modified": datetime.datetime.fromtimestamp(stat_result.st_mtime).isoformat(),
                "summary": _project_compact_pareto(compact, selected_statistic, selected_scenario),
            }
            for path, compact, stat_result in candidates
        ]
    return {
        "paretos": paretos,
        "meta": {
            "mode": contract["mode"],
            "has_suite_metrics": contract["mode"] == "suite",
            "scenario_count": contract["scenario_count"],
            "scenario_labels": contract["scenario_labels"],
            "objectives": contract["objectives"],
            "available_statistics": list(_PARETO_STATISTICS),
            "selected_scenario": selected_scenario,
            "selected_statistic": selected_statistic,
            "statistic_enabled": contract["mode"] != "suite" or selected_scenario == "Aggregated",
        },
    }


@router.get("/paretos/file")
def get_pareto_file(path: str, session: SessionToken = Depends(require_auth)) -> dict:
    with _result_lock():
        pareto = _resolve_result_path(path, require_directory=False)
        if not pareto.is_file() or pareto.parent.name != "pareto":
            raise HTTPException(status_code=400, detail="Path is not a managed PB8 pareto file")
        return _read_json(pareto)


@router.post("/paretos/seed-bundle")
def create_pareto_seed_bundle(body: dict, session: SessionToken = Depends(require_auth)) -> dict:
    with _result_lock():
        result_dir = _resolve_result_path(str((body or {}).get("result_path") or ""))
        raw_paths = (body or {}).get("paths")
        if not isinstance(raw_paths, list):
            raise HTTPException(status_code=400, detail="paths must be a list")
        pareto_dir = _safe_path(result_dir / "pareto", _results_root())
        paths = []
        seen = set()
        for raw in raw_paths:
            path = _resolve_result_path(str(raw), require_directory=False)
            if path.parent != pareto_dir or not _supported_seed_file(path):
                raise HTTPException(status_code=422, detail="All Pareto seeds must be supported files from the selected result")
            if str(path) not in seen:
                paths.append(path)
                seen.add(str(path))
        if not paths:
            raise HTTPException(status_code=422, detail="No Pareto seed files selected")
        if len(paths) == 1:
            return {"path": str(paths[0]), "count": 1}
        bundle = _create_seed_bundle(result_dir, paths)
        return {"path": str(bundle), "count": len(paths)}
