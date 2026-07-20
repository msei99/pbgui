"""FastAPI backend for isolated Passivbot V8 backtest management."""

from __future__ import annotations

import asyncio
import copy
import csv
import datetime
import gzip
import json
import math
import multiprocessing
import os
import platform
import signal
import shutil
import subprocess
import time
import traceback
import uuid
from contextlib import contextmanager
from pathlib import Path
from shutil import rmtree
from typing import Optional

import psutil
from fastapi import APIRouter, Depends, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse

from api.archive_helpers import atomic_write_json
from api.auth import SessionToken, authenticate_websocket, require_auth
from file_lock import advisory_file_lock
from logging_helpers import human_log as _log, rotate_managed_log_before_open
from master_update_lock import MasterUpdateBusyError, acquire_master_runtime_lock
from pb8_config import (
    PB8ConfigurationError,
    cache_prepared_pb8_config,
    get_pb8_result_metrics,
    get_pb8_template_config,
    load_pb8_config,
    migrate_pb7_config,
    prepare_pb8_config,
    save_prepared_pb8_config,
)
from pbgui_purefunc import (
    PBGDIR,
    PBGUI_SERIAL,
    PBGUI_VERSION,
    load_ini_section,
    pb7dir,
    pb8_runtime_status,
    save_ini_section,
)
from secure_files import atomic_write_private_text

SERVICE = "BacktestV8"
router = APIRouter()


def _configuration_error(operation: str, exc: Exception, status_code: int = 422) -> HTTPException:
    """Log a PB8 config failure before returning a browser-safe HTTP error."""
    _log(SERVICE, f"{operation} failed: {exc}", level="WARNING")
    return HTTPException(status_code=status_code, detail=str(exc))


def _data_dir() -> Path:
    return Path(PBGDIR) / "data"


def _configs_dir() -> Path:
    return _data_dir() / "bt_v8"


def _v7_configs_dir() -> Path:
    return _data_dir() / "bt_v7"


def _run_v7_dir() -> Path:
    return _data_dir() / "run_v7"


def _v7_results_dir() -> Path:
    return Path(pb7dir()) / "backtests" / "pbgui"


def _queue_dir() -> Path:
    return _data_dir() / "bt_v8_queue"


def _log_dir() -> Path:
    return _data_dir() / "logs" / "backtests_v8"


def _results_root() -> Path:
    status = pb8_runtime_status()
    pb8_dir = str(status.get("pb8dir") or "")
    return Path(pb8_dir) / "backtests" / "pbgui" if pb8_dir else _data_dir() / ".pb8-unavailable"


def _validate_name(name: str) -> str:
    """Reject names that can escape a managed root."""
    value = str(name or "").strip()
    if not value or value.startswith(".") or value in {".", ".."} or any(char in value for char in ("/", "\\", "\x00")):
        raise HTTPException(status_code=400, detail="Invalid name")
    if any(ord(char) < 32 for char in value):
        raise HTTPException(status_code=400, detail="Invalid name")
    if len(value.encode("utf-8")) > 128:
        raise HTTPException(status_code=400, detail="Name is too long")
    return value


def _managed_base_dir(name: str) -> str:
    return f"backtests/pbgui/{name}"


def _normalize_config(config: dict, name: str) -> dict:
    candidate = copy.deepcopy(config)
    backtest = candidate.setdefault("backtest", {})
    if not isinstance(backtest, dict):
        raise HTTPException(status_code=422, detail="backtest must be an object")
    backtest["base_dir"] = _managed_base_dir(name)
    return candidate


def _v7_migration_source(body: dict) -> tuple[Path, Path, str]:
    """Resolve one supported V7 source without accepting arbitrary filesystem paths."""
    source_type = str(body.get("source_type") or "backtest_config")
    source_name = _validate_name(str(body.get("source_name") or ""))
    if source_type == "backtest_config":
        root = _v7_configs_dir()
        source = root / source_name / "backtest.json"
    elif source_type == "run_config":
        root = _run_v7_dir()
        source = root / source_name / "config.json"
    elif source_type == "backtest_result":
        root = _v7_results_dir()
        raw_path = str(body.get("source_path") or "").strip()
        if not raw_path:
            raise HTTPException(status_code=400, detail="source_path is required for a backtest result")
        try:
            result_dir = _safe_path(Path(raw_path), root)
            source = _safe_path(result_dir / "config.json", root)
            analysis = _safe_path(result_dir / "analysis.json", root)
        except RuntimeError as exc:
            raise HTTPException(status_code=400, detail="Invalid V7 result path") from exc
        if not analysis.is_file() or analysis.is_symlink():
            raise HTTPException(status_code=400, detail="Path is not a V7 backtest result")
    else:
        raise HTTPException(status_code=422, detail="Unsupported V7 migration source type")
    try:
        source = _safe_path(source, root)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail="Invalid V7 migration source") from exc
    if not source.is_file() or source.is_symlink():
        raise HTTPException(status_code=404, detail=f"V7 source config '{source_name}' not found")
    return source, root, source_type


def _sanitize_v7_migration_payload(config: dict) -> tuple[dict, list[str]]:
    """Remove PBGui-only metadata that is not part of Passivbot's V7 strategy."""
    candidate = copy.deepcopy(config)
    removed = []
    if "pbgui" in candidate:
        candidate.pop("pbgui", None)
        removed.append("pbgui")
    live = candidate.get("live")
    if isinstance(live, dict) and "base_config_path" in live:
        live.pop("base_config_path", None)
        removed.append("live.base_config_path")
    return candidate, removed


def _override_filenames(config: dict) -> set[str]:
    """Return validated local override filenames referenced by a config."""
    overrides = config.get("coin_overrides")
    if overrides is None:
        return set()
    if not isinstance(overrides, dict):
        raise HTTPException(status_code=422, detail="coin_overrides must be an object")
    filenames = set()
    for override in overrides.values():
        if not isinstance(override, dict) or not override.get("override_config_path"):
            continue
        raw = _validate_override_filename(str(override["override_config_path"]))
        filenames.add(raw)
    return filenames


def _validate_override_filename(filename: str) -> str:
    """Return a safe sparse-override filename within one config bundle."""
    value = str(filename or "").strip()
    if (
        not value.endswith(".json")
        or value.startswith(".")
        or Path(value).name != value
        or value in {"backtest.json", "migration_report.json", ".", ".."}
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise HTTPException(status_code=422, detail=f"Invalid override config path: {value}")
    return value


def _validate_override_payloads(config: dict, payloads: object) -> dict[str, dict]:
    """Validate dirty sparse files supplied with a config-bundle save."""
    if payloads is None:
        return {}
    if not isinstance(payloads, dict):
        raise HTTPException(status_code=422, detail="override_configs must be an object")
    referenced = _override_filenames(config)
    validated = {}
    for raw_filename, payload in payloads.items():
        filename = _validate_override_filename(str(raw_filename))
        if filename not in referenced:
            raise HTTPException(status_code=422, detail=f"Unreferenced override config: {filename}")
        if not isinstance(payload, dict):
            raise HTTPException(status_code=422, detail=f"Override config {filename} must be an object")
        try:
            json.dumps(payload, allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=422, detail=f"Override config {filename} is not valid JSON") from exc
        validated[filename] = copy.deepcopy(payload)
    return validated


def _copy_override_files(config: dict, source_dir: Path, destination_dir: Path, source_root: Path) -> None:
    """Copy all referenced overrides between validated managed directories."""
    for filename in _override_filenames(config):
        source = _safe_path(source_dir / filename, source_root)
        destination = _safe_path(destination_dir / filename, destination_dir)
        if not source.is_file() or source.is_symlink():
            raise HTTPException(status_code=422, detail=f"Override config not found: {filename}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)


def _require_override_files(config: dict, config_dir: Path) -> None:
    """Reject configs whose local override references are unavailable."""
    for filename in _override_filenames(config):
        path = _safe_path(config_dir / filename, config_dir)
        if not path.is_file() or path.is_symlink():
            raise HTTPException(status_code=422, detail=f"Override config not found: {filename}")


def _publish_config_bundle(stage_dir: Path, target_dir: Path) -> None:
    """Publish a validated config directory and restore the previous bundle on failure."""
    backup_dir = target_dir.parent / f".{target_dir.name}.backup-{uuid.uuid4().hex}"
    target_existed = target_dir.exists()
    if target_existed:
        if not target_dir.is_dir() or target_dir.is_symlink():
            raise HTTPException(status_code=409, detail=f"Config target '{target_dir.name}' is not a safe directory")
        os.replace(target_dir, backup_dir)
    try:
        os.replace(stage_dir, target_dir)
    except Exception:
        if target_existed and backup_dir.exists() and not target_dir.exists():
            os.replace(backup_dir, target_dir)
        raise
    if backup_dir.exists():
        rmtree(backup_dir, ignore_errors=True)


def _save_config_bundle_locked(
    name: str,
    config: dict,
    override_payloads: object,
    *,
    create_only: bool,
    source_name: str | None,
    inherit_existing_overrides: bool,
) -> dict:
    """Stage, validate, and publish one complete PB8 config bundle under the config lock."""
    root = _configs_dir()
    root.mkdir(parents=True, exist_ok=True)
    target_dir = _safe_path(root / name, root)
    if create_only and target_dir.exists():
        raise HTTPException(status_code=409, detail=f"Config '{name}' already exists")

    source_dir: Path | None = target_dir if inherit_existing_overrides else None
    if source_name and source_name != name:
        source_name = _validate_name(source_name)
        source_dir = _safe_path(root / source_name, root)
        if not _config_file(source_name).is_file():
            raise HTTPException(status_code=404, detail=f"Source config '{source_name}' not found")

    normalized = _normalize_config(config, name)
    payloads = _validate_override_payloads(normalized, override_payloads)
    referenced = _override_filenames(normalized)
    stage_dir = _safe_path(root / f".{name}.stage-{uuid.uuid4().hex}", root)
    stage_dir.mkdir(mode=0o700)
    try:
        for filename in referenced:
            destination = _safe_path(stage_dir / filename, stage_dir)
            if filename in payloads:
                atomic_write_json(destination, payloads[filename])
                continue
            if source_dir is None:
                raise HTTPException(status_code=422, detail=f"Override config not supplied: {filename}")
            source = _safe_path(source_dir / filename, root)
            if not source.is_file() or source.is_symlink():
                raise HTTPException(status_code=422, detail=f"Override config not found: {filename}")
            shutil.copy2(source, destination)

        if source_dir is not None:
            report = _safe_path(source_dir / "migration_report.json", root)
            if report.is_file() and not report.is_symlink():
                shutil.copy2(report, stage_dir / report.name)

        prepared = prepare_pb8_config(normalized, base_config_path=str(stage_dir / "backtest.json"))
        if _override_filenames(prepared) != referenced:
            raise HTTPException(status_code=422, detail="PB8 preparation changed override-file references")
        _require_override_files(prepared, stage_dir)
        atomic_write_json(stage_dir / "backtest.json", prepared)
        _publish_config_bundle(stage_dir, target_dir)
        cache_prepared_pb8_config(prepared, target_dir / "backtest.json")
        return prepared
    finally:
        if stage_dir.exists():
            rmtree(stage_dir, ignore_errors=True)


def _config_file(name: str) -> Path:
    return _safe_path(_configs_dir() / name / "backtest.json", _configs_dir())


def _queue_file(filename: str) -> Path:
    return _safe_path(_queue_dir() / f"{filename}.json", _queue_dir())


def _pid_file(filename: str) -> Path:
    return _safe_path(_queue_dir() / f"{filename}.pid", _queue_dir())


def _state_file(filename: str) -> Path:
    return _safe_path(_queue_dir() / "state" / f"{filename}.json", _queue_dir())


def _launch_ready_file(filename: str) -> Path:
    return _safe_path(_queue_dir() / "state" / f"{filename}.launch-ready", _queue_dir())


def _snapshot_file(filename: str) -> Path:
    return _safe_path(_queue_dir() / "configs" / filename / "backtest.json", _queue_dir())


def _safe_path(path: Path, root: Path) -> Path:
    """Keep managed paths below a non-symlink root and reject symlink components."""
    absolute_root = Path(os.path.abspath(root))
    absolute_path = Path(os.path.abspath(path))
    try:
        relative = absolute_path.relative_to(absolute_root)
    except ValueError as exc:
        raise RuntimeError(f"Managed path escaped root: {path}") from exc
    current = absolute_root
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise RuntimeError(f"Managed path contains a symlink: {current}")
    if absolute_root.is_symlink():
        raise RuntimeError(f"Managed root must not be a symlink: {absolute_root}")
    return absolute_path


def _queue_lock():
    return advisory_file_lock(_safe_path(_queue_dir() / ".write", _queue_dir()))


def _recover_config_transactions(root: Path) -> None:
    """Recover or remove PBGui-owned directory-swap artifacts after a process crash."""
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
                raise RuntimeError(f"Unsafe PB8 config transaction backup: {entry}")
            backups.setdefault(target_name, []).append(entry)
            continue
        stage_name, separator, token = body.rpartition(".stage-")
        if separator and stage_name and len(token) == 32 and all(char in "0123456789abcdef" for char in token):
            if entry.is_symlink() or not entry.is_dir():
                raise RuntimeError(f"Unsafe PB8 config transaction stage: {entry}")
            rmtree(entry, ignore_errors=True)
    for target_name, candidates in backups.items():
        target = _safe_path(root / target_name, root)
        ordered = sorted(candidates, key=lambda path: path.stat().st_mtime_ns, reverse=True)
        if not target.exists():
            os.replace(ordered.pop(0), target)
            _log(SERVICE, f"Recovered interrupted PB8 config save for {target_name}", level="WARNING")
        for backup in ordered:
            rmtree(backup, ignore_errors=True)


@contextmanager
def _config_lock():
    root = _configs_dir()
    root.mkdir(parents=True, exist_ok=True)
    with advisory_file_lock(_safe_path(root / ".write", root)):
        _recover_config_transactions(root)
        yield


def _get_pbgui_market_data_path() -> str:
    """Return the server-managed market-data root used by PB8 launches."""
    from market_data import get_market_data_root_dir

    return str(get_market_data_root_dir())


def _apply_pbgui_market_data_override(config: dict, enabled: bool) -> tuple[bool, str]:
    """Apply the shared market-data setting without clearing custom source paths."""
    target_path = _get_pbgui_market_data_path()
    backtest = config.get("backtest")
    if not isinstance(backtest, dict):
        if not enabled:
            return False, target_path
        backtest = {}
        config["backtest"] = backtest
    current_path = str(backtest.get("ohlcv_source_dir") or "").strip()
    current_normalized = current_path.rstrip("/\\")
    target_normalized = target_path.rstrip("/\\")
    if not enabled:
        if current_normalized != target_normalized:
            return False, target_path
        backtest.pop("ohlcv_source_dir", None)
        return True, target_path
    if current_normalized == target_normalized:
        return False, target_path
    backtest["ohlcv_source_dir"] = target_path
    return True, target_path


def _read_json(path: Path) -> dict:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Failed to read {path.name}: {exc}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"{path.name} must contain an object")
    return payload


def _result_effective_fee_overrides(result_dir: Path, result_root: Path) -> dict[str, float | None]:
    """Infer effective linear-market fees recorded by one V7 result."""
    try:
        result_dir = _safe_path(result_dir, result_root)
        fills_path = _safe_path(result_dir / "fills.csv", result_root)
        dataset_path = _safe_path(result_dir / "dataset.json", result_root)
        if not fills_path.is_file() or not dataset_path.is_file():
            return {}

        dataset = _read_json(dataset_path)
        settings_value = str(dataset.get("market_specific_settings_file") or "").strip()
        if not settings_value:
            return {}
        pb7_root = Path(pb7dir())
        settings_candidate = Path(settings_value)
        if not settings_candidate.is_absolute():
            settings_candidate = pb7_root / settings_candidate
        settings_path = _safe_path(settings_candidate, pb7_root)
        if not settings_path.is_file():
            return {}
        market_settings = _read_json(settings_path)

        states = {
            "maker": {"count": 0, "sum": 0.0, "min": math.inf, "max": -math.inf, "supported": True},
            "taker": {"count": 0, "sum": 0.0, "min": math.inf, "max": -math.inf, "supported": True},
        }
        with fills_path.open("r", encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                liquidity = str(row.get("liquidity") or "").strip().lower()
                if liquidity not in states:
                    continue
                coin = str(row.get("coin") or "").strip()
                settings = market_settings.get(coin)
                if not isinstance(settings, dict) or settings.get("linear") is not True:
                    states[liquidity]["supported"] = False
                    continue
                try:
                    c_mult = float(settings.get("c_mult", 1.0))
                    qty = float(row.get("qty") or 0.0)
                    price = float(row.get("price") or 0.0)
                    fee_paid = float(row.get("fee_paid") or 0.0)
                    notional = abs(qty * price * c_mult)
                    rate = -fee_paid / notional
                except (TypeError, ValueError, ZeroDivisionError):
                    states[liquidity]["supported"] = False
                    continue
                if notional <= 0.0 or not math.isfinite(rate) or abs(rate) > 0.05:
                    states[liquidity]["supported"] = False
                    continue
                state = states[liquidity]
                state["count"] += 1
                state["sum"] += rate
                state["min"] = min(state["min"], rate)
                state["max"] = max(state["max"], rate)

        overrides: dict[str, float | None] = {}
        for liquidity, state in states.items():
            if not state["supported"] or not state["count"]:
                continue
            mean_rate = state["sum"] / state["count"]
            tolerance = max(1e-10, abs(mean_rate) * 1e-6)
            overrides[liquidity] = (
                mean_rate if state["max"] - state["min"] <= tolerance else None
            )
        return overrides
    except (OSError, RuntimeError, csv.Error) as exc:
        _log(SERVICE, f"Could not infer effective V7 result fees: {exc}", level="WARNING")
        return {}


def _apply_result_effective_fees(config: dict, result_dir: Path, result_root: Path) -> list[dict]:
    """Correct normalized V7 result defaults with fees evidenced by its fills."""
    backtest = config.get("backtest")
    if not isinstance(backtest, dict):
        return []
    adjustments = []
    for liquidity, effective_value in _result_effective_fee_overrides(result_dir, result_root).items():
        field = f"{liquidity}_fee_override"
        previous_value = backtest.get(field)
        values_match = previous_value is None and effective_value is None
        if not values_match and previous_value is not None and effective_value is not None:
            try:
                values_match = math.isclose(float(previous_value), effective_value, rel_tol=1e-12, abs_tol=1e-12)
            except (TypeError, ValueError):
                values_match = False
        if values_match:
            continue
        backtest[field] = effective_value
        adjustments.append(
            {
                "field": f"backtest.{field}",
                "result_config_value": previous_value,
                "effective_value": effective_value,
                "evidence": "fills.csv",
            }
        )
    return adjustments


def _read_process_record(filename: str) -> Optional[dict]:
    try:
        record = json.loads(_pid_file(filename).read_text(encoding="utf-8"))
        if not isinstance(record, dict):
            return None
        pid = int(record.get("pid") or 0)
        create_time = float(record.get("create_time") or 0)
        if pid <= 0 or create_time <= 0:
            return None
        return {"pid": pid, "create_time": create_time}
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return None


def _process_matches(filename: str, record: dict) -> bool:
    """Verify a PID belongs to this V8 queue item before process control."""
    try:
        process = psutil.Process(int(record["pid"]))
        if abs(process.create_time() - float(record["create_time"])) > 0.01:
            return False
        command = process.cmdline()
        snapshot = str(_snapshot_file(filename).resolve())
        runner = str(Path(PBGDIR) / "pb8_backtest_runner.py")
        return runner in command and "backtest" in command and snapshot in command
    except (psutil.NoSuchProcess, psutil.ZombieProcess, psutil.AccessDenied, OSError):
        return False


def _terminate_process(pid: int) -> None:
    """Terminate one newly created detached runner and its process group."""
    try:
        process = psutil.Process(pid)
    except psutil.NoSuchProcess:
        return
    if platform.system() == "Windows":
        children = process.children(recursive=True)
        for child in children:
            child.terminate()
        process.terminate()
        _gone, alive = psutil.wait_procs([process, *children], timeout=5)
        for member in alive:
            member.kill()
        return
    try:
        process_group = os.getpgid(pid)
        if process_group == os.getpgrp():
            raise RuntimeError("Refusing to terminate the API process group")
        os.killpg(process_group, signal.SIGTERM)
        try:
            process.wait(timeout=1)
        except psutil.TimeoutExpired:
            pass
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                os.killpg(process_group, 0)
            except ProcessLookupError:
                return
            time.sleep(0.05)
        os.killpg(process_group, signal.SIGKILL)
        try:
            process.wait(timeout=1)
        except (psutil.TimeoutExpired, psutil.NoSuchProcess):
            pass
    except (ProcessLookupError, psutil.NoSuchProcess):
        return


def _terminate_verified(filename: str) -> None:
    """Terminate a queue-owned runner only after PID and start-time verification."""
    record = _read_process_record(filename)
    if record is None:
        return
    pid = int(record["pid"])
    if not psutil.pid_exists(pid):
        _pid_file(filename).unlink(missing_ok=True)
        return
    if not _process_matches(filename, record):
        if _read_runner_state(filename) is not None:
            _pid_file(filename).unlink(missing_ok=True)
            return
        raise HTTPException(status_code=409, detail="Queue process ownership could not be verified")
    _terminate_process(pid)
    _pid_file(filename).unlink(missing_ok=True)


def _read_runner_state(filename: str) -> Optional[dict]:
    try:
        state = _read_json(_state_file(filename))
        return state if isinstance(state.get("returncode"), int) else None
    except (OSError, RuntimeError):
        return None


def _queue_status(data: dict) -> tuple[str, Optional[int]]:
    filename = _validate_name(str(data.get("filename") or ""))
    record = _read_process_record(filename)
    if record and _process_matches(filename, record):
        return "running", int(record["pid"])
    override = str(data.get("status_override") or "")
    if override in {"stopped", "error"}:
        return override, int(record["pid"]) if record else None
    state = _read_runner_state(filename)
    if state is not None:
        return ("complete" if state["returncode"] == 0 else "error"), int(record["pid"]) if record else None
    if record and psutil.pid_exists(int(record["pid"])):
        return "unknown", int(record["pid"])
    return ("error" if data.get("started_at") else "queued"), int(record["pid"]) if record else None


def _queue_item(path: Path) -> dict:
    path = _safe_path(path, _queue_dir())
    data = _read_json(path)
    filename = str(data.get("filename") or path.stem)
    if filename != path.stem:
        raise RuntimeError("Persisted queue filename does not match its file")
    _validate_name(filename)
    _validate_name(str(data.get("name") or ""))
    status, pid = _queue_status({**data, "filename": filename})
    return {
        "filename": filename,
        "name": str(data.get("name") or filename),
        "exchange": data.get("exchange") or [],
        "status": status,
        "pid": pid,
        "created": datetime.datetime.fromtimestamp(path.stat().st_mtime).isoformat(),
        "started_at": data.get("started_at"),
        "pb8_version": data.get("pb8_version") or "",
        "pb8_commit": data.get("pb8_commit") or "",
    }


def _load_queue() -> list[dict]:
    _queue_dir().mkdir(parents=True, exist_ok=True)
    items = []
    for path in sorted(_queue_dir().glob("*.json")):
        try:
            items.append(_queue_item(path))
        except Exception as exc:
            _log(SERVICE, f"Failed to load V8 queue item {path.name}: {exc}", level="WARNING")
    return items


def _runtime_commit(pb8_dir: Path) -> str:
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=pb8_dir,
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
        return proc.stdout.strip() if proc.returncode == 0 else ""
    except (OSError, subprocess.TimeoutExpired):
        return ""


def _cpu_limit(settings: dict) -> int:
    """Return a bounded CPU setting even when an old INI value is malformed."""
    try:
        configured = int(settings.get("cpu", "1"))
    except (TypeError, ValueError):
        configured = 1
    return max(1, min(configured, multiprocessing.cpu_count()))


def _bounded_setting(settings: dict, key: str, default: int, minimum: int, maximum: int) -> int:
    """Read one bounded integer setting without trusting persisted INI text."""
    try:
        value = int(settings.get(key, str(default)))
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(value, maximum))


def _cleanup_pb8_caches(retention_days: int) -> dict:
    """Remove expired PB8 HLCV cache directories from the configured PB8 root."""
    runtime = pb8_runtime_status()
    pb8_dir = Path(str(runtime.get("pb8dir") or ""))
    if not pb8_dir.is_dir():
        raise RuntimeError("PB8 directory is not available")
    cutoff = time.time() - max(1, retention_days) * 86400
    targets = [pb8_dir / "caches" / "hlcvs_data", pb8_dir / "caches" / "ohlcvs" / "materialized"]
    removed = 0
    freed = 0
    errors = 0
    for root in targets:
        if not root.is_dir() or root.is_symlink():
            continue
        for entry in root.iterdir():
            if not entry.is_dir() or entry.is_symlink():
                continue
            try:
                if entry.stat().st_mtime >= cutoff:
                    continue
                size = sum(item.stat().st_size for item in entry.rglob("*") if item.is_file() and not item.is_symlink())
                rmtree(entry)
                removed += 1
                freed += size
            except OSError as exc:
                errors += 1
                _log(SERVICE, f"Failed to clean PB8 cache {entry}: {exc}", level="WARNING")
    return {"removed": removed, "freed_mb": round(freed / (1024 * 1024)), "errors": errors}


def _scalar_metrics(analysis: dict) -> dict:
    preferred = (
        "starting_balance",
        "starting_balance_usd",
        "final_balance",
        "final_balance_usd",
        "final_equity",
        "final_equity_usd",
        "adg",
        "adg_usd",
        "adg_w_usd",
        "adg_per_exposure",
        "drawdown_max",
        "drawdown_worst",
        "drawdown_worst_usd",
        "drawdown_worst_w_usd",
        "equity_balance_diff_neg_max",
        "equity_balance_diff_neg_max_usd",
        "sharpe_ratio",
        "sharpe_ratio_usd",
        "sharpe_ratio_w_usd",
        "sortino_ratio",
        "n_fills",
    )
    result = {key: analysis[key] for key in preferred if isinstance(analysis.get(key), (int, float, str, bool))}
    if len(result) < 12:
        for key in sorted(analysis):
            value = analysis[key]
            if key not in result and isinstance(value, (int, float, str, bool)):
                result[key] = value
            if len(result) >= 12:
                break
    return result


def _analysis_value(analysis: dict, *keys: str, default=0):
    """Return the first scalar analysis value used by the shared results table."""
    for key in keys:
        value = analysis.get(key)
        if isinstance(value, (int, float, str, bool)):
            return value
    return default


def _numeric_analysis_value(analysis: dict, *keys: str, default=None):
    """Return the first numeric non-bool analysis value for derived result fields."""
    for key in keys:
        value = analysis.get(key)
        if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(float(value)):
            return value
    return default


def _result_config(result_dir: Path) -> dict:
    """Load a result config when PB8 emitted one, otherwise return an empty object."""
    for filename in ("config.json", "backtest.json"):
        path = result_dir / filename
        if path.is_file() and not path.is_symlink():
            try:
                return _read_json(path)
            except RuntimeError:
                return {}
    return {}


def _resolve_result_dir(path: str) -> Path:
    """Resolve a browser-provided PB8 result path below the managed result root."""
    root = _results_root().resolve()
    result_dir = Path(str(path or "")).resolve()
    try:
        result_dir.relative_to(root)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid result path") from exc
    if not result_dir.is_dir() or result_dir.is_symlink():
        raise HTTPException(status_code=404, detail="Result not found")
    if result_dir == root:
        raise HTTPException(status_code=400, detail="Result root cannot be selected")
    analysis_path = result_dir / "analysis.json"
    if not analysis_path.is_file() or analysis_path.is_symlink():
        raise HTTPException(status_code=400, detail="Path is not a PB8 result directory")
    return result_dir


def _resolve_result_file(result_dir: Path, filename: str) -> Path:
    """Resolve one relative result artifact without allowing traversal or symlinks."""
    raw = str(filename or "")
    relative = Path(raw)
    if (
        not raw
        or relative.is_absolute()
        or any(part in {"", ".", ".."} for part in relative.parts)
        or any(ord(char) < 32 for char in raw)
    ):
        raise HTTPException(status_code=400, detail="Invalid result filename")
    target = _safe_path(result_dir / relative, result_dir)
    if not target.is_file() or target.is_symlink():
        raise HTTPException(status_code=404, detail="Result file not found")
    return target


def _flatten_leaf_paths(value: dict, prefix: str = "") -> list[str]:
    """Return dotted leaf paths for V8 bot-side editor selectors."""
    paths = []
    for key, item in value.items():
        path = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(item, dict):
            paths.extend(_flatten_leaf_paths(item, path))
        else:
            paths.append(path)
    return paths


def _flatten_leaf_metadata(value: dict, prefix: str = "") -> dict[str, dict]:
    """Return dotted leaf paths with value types for shared override controls."""
    metadata = {}
    for key, item in value.items():
        path = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(item, dict):
            metadata.update(_flatten_leaf_metadata(item, path))
            continue
        if isinstance(item, bool):
            value_type = "boolean"
        elif isinstance(item, (int, float)):
            value_type = "number"
        elif isinstance(item, str):
            value_type = "string"
        elif item is None:
            value_type = "null"
        elif isinstance(item, list):
            value_type = "array"
        else:
            value_type = "json"
        metadata[path] = {"type": value_type, "default": item}
    return metadata


def _bot_risk_value(config: dict, side: str, key: str, default=0):
    side_config = ((config.get("bot") or {}).get(side) or {}) if isinstance(config, dict) else {}
    risk = side_config.get("risk") if isinstance(side_config, dict) else {}
    return risk.get(key, default) if isinstance(risk, dict) else default


def _result_terminal_balances(result_dir: Path) -> dict[str, float]:
    """Stream the authoritative terminal USD balance and equity from a PB8 result."""
    for filename in ("balance_and_equity.csv", "balance_and_equity.csv.gz"):
        path = result_dir / filename
        if not path.is_file() or path.is_symlink():
            continue
        values: dict[str, float] = {}
        last_row_valid = False
        try:
            opener = gzip.open if path.suffix == ".gz" else open
            with opener(path, "rt", encoding="utf-8", newline="") as handle:
                for row in csv.DictReader(handle):
                    last_row_valid = False
                    try:
                        balance = float(row.get("usd_total_balance", ""))
                        equity = float(row.get("usd_total_equity", ""))
                    except (TypeError, ValueError):
                        continue
                    if math.isfinite(balance) and math.isfinite(equity):
                        values = {"usd_total_balance": balance, "usd_total_equity": equity}
                        last_row_valid = True
            if values and last_row_valid:
                return values
        except (OSError, EOFError, UnicodeError, csv.Error) as exc:
            _log(SERVICE, f"Failed to read PB8 terminal balances from {path}: {exc}", level="WARNING")
    return {}


def _list_results() -> list[dict]:
    root = _results_root()
    if not root.is_dir():
        return []
    results = []
    for analysis_path in root.glob("**/analysis.json"):
        try:
            resolved = analysis_path.resolve()
            relative = resolved.relative_to(root.resolve())
            analysis = _read_json(resolved)
            parts = relative.parts
            result_dir = resolved.parent
            config = _result_config(result_dir)
            backtest = config.get("backtest") if isinstance(config.get("backtest"), dict) else {}
            live = config.get("live") if isinstance(config.get("live"), dict) else {}
            approved = live.get("approved_coins") if isinstance(live.get("approved_coins"), dict) else {}
            coins = sorted(set((approved.get("long") or []) + (approved.get("short") or [])))
            metrics = _scalar_metrics(analysis)
            exchange = parts[1] if len(parts) > 1 else ""
            starting_balance = _analysis_value(
                analysis,
                "starting_balance_usd",
                "starting_balance",
                default=backtest.get("starting_balance", 0),
            )
            gain = _analysis_value(analysis, "gain_usd", "gain_strategy_eq", "gain")
            terminal_balances = _result_terminal_balances(result_dir)
            final_balance = _numeric_analysis_value(
                analysis,
                "final_balance_usd",
                "final_balance",
                "final_balance_strategy_eq",
                default=terminal_balances.get("usd_total_balance"),
            )
            if final_balance is None:
                start_num = starting_balance if isinstance(starting_balance, (int, float)) and not isinstance(starting_balance, bool) else None
                gain_num = _numeric_analysis_value(analysis, "gain_usd", "gain", default=None)
                final_balance = start_num * gain_num if start_num is not None and gain_num is not None else 0
            equity_balance_diff = _analysis_value(
                analysis,
                "equity_balance_diff_neg_max_usd",
                "equity_balance_diff_neg_max",
                "equity_balance_diff_max_usd",
                "equity_balance_diff_max",
                "balance_equity_diff",
                "equity_balance_diff",
            )
            final_equity = _numeric_analysis_value(
                analysis,
                "final_equity_usd",
                "final_equity",
                "final_equity_strategy_eq",
                default=terminal_balances.get("usd_total_equity", 0),
            )
            results.append(
                {
                    "config_name": parts[0] if parts else "",
                    "exchange": exchange,
                    "exchange_dir": exchange,
                    "exchanges": [exchange] if exchange else [],
                    "run": "/".join(parts[2:-1]) if len(parts) > 3 else (parts[-2] if len(parts) > 1 else ""),
                    "result_name": result_dir.name,
                    "path": str(result_dir),
                    "coins": coins,
                    "coins_text": ", ".join(coins),
                    "modified": datetime.datetime.fromtimestamp(resolved.stat().st_mtime).isoformat(),
                    "metrics": metrics,
                    "adg": _analysis_value(analysis, "adg_w_usd", "adg_usd", "adg_strategy_eq", "adg"),
                    "gain": gain,
                    "drawdown_worst": _analysis_value(analysis, "drawdown_worst_w_usd", "drawdown_worst_usd", "drawdown_worst_strategy_eq", "drawdown_worst"),
                    "sharpe_ratio": _analysis_value(analysis, "sharpe_ratio_w_usd", "sharpe_ratio_usd", "sharpe_ratio_strategy_eq", "sharpe_ratio"),
                    "starting_balance": starting_balance,
                    "final_balance": final_balance,
                    "final_equity": final_equity,
                    "balance_equity_diff": equity_balance_diff,
                    "equity_balance_diff_neg_max": equity_balance_diff,
                    "btc_collateral_cap": backtest.get("btc_collateral_cap", 0),
                    "end_date": backtest.get("end_date", ""),
                    "twe_long": _bot_risk_value(config, "long", "total_wallet_exposure_limit"),
                    "twe_short": _bot_risk_value(config, "short", "total_wallet_exposure_limit"),
                    "pos_long": _bot_risk_value(config, "long", "n_positions"),
                    "pos_short": _bot_risk_value(config, "short", "n_positions"),
                    "liquidated": bool(analysis.get("liquidated", False)),
                }
            )
        except (OSError, RuntimeError, ValueError) as exc:
            _log(SERVICE, f"Failed to read V8 result {analysis_path}: {exc}", level="WARNING")
    return sorted(results, key=lambda item: item["modified"], reverse=True)


class BacktestV8Worker:
    """Launch queued V8 backtests while leaving child jobs independently running."""

    def __init__(self):
        self._task: Optional[asyncio.Task] = None
        self._running = False
        self._last_cleanup_at = 0.0

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._running = True
            self._task = asyncio.create_task(self._loop(), name="backtest-v8-worker")
            _log(SERVICE, "V8 backtest worker started", level="INFO")

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
        try:
            while self._running:
                settings = load_ini_section("backtest_v8")
                if str(settings.get("hlcvs_cleanup_enabled", "False")).lower() == "true":
                    interval = _bounded_setting(settings, "hlcvs_cleanup_interval_h", 24, 1, 168) * 3600
                    if time.time() - self._last_cleanup_at >= interval:
                        days = _bounded_setting(settings, "hlcvs_cleanup_days", 7, 1, 365)
                        try:
                            await asyncio.to_thread(_cleanup_pb8_caches, days)
                        except Exception as exc:
                            _log(SERVICE, f"PB8 cache cleanup failed: {exc}", level="WARNING")
                        self._last_cleanup_at = time.time()
                if str(settings.get("autostart", "False")).lower() != "true":
                    await asyncio.sleep(5)
                    continue
                cpu_limit = _cpu_limit(settings)
                items = _load_queue()
                running = sum(item["status"] == "running" for item in items)
                for item in items:
                    if item["status"] != "queued" or running >= cpu_limit:
                        continue
                    try:
                        await asyncio.to_thread(self.launch, item["filename"])
                        running += 1
                    except HTTPException as exc:
                        if exc.status_code == 409:
                            _log(SERVICE, f"V8 backtest {item['filename']} remains queued: {exc.detail}", level="INFO")
                            continue
                        _log(SERVICE, f"Failed to launch queued V8 backtest {item['filename']}: {exc.detail}", level="ERROR")
                    except Exception as exc:
                        _log(SERVICE, f"Failed to launch queued V8 backtest {item['filename']}: {exc}", level="ERROR")
                await asyncio.sleep(5)
        except asyncio.CancelledError:
            _log(SERVICE, "V8 backtest worker stopped", level="INFO")
        except Exception as exc:
            _log(
                SERVICE,
                f"V8 backtest worker failed: {exc}",
                level="ERROR",
                meta={"traceback": traceback.format_exc()},
            )

    def launch(self, filename: str) -> None:
        """Validate the queued snapshot and launch the configured PB8 CLI."""
        _validate_name(filename)
        with _queue_lock():
            self._launch_locked(filename)

    def _launch_locked(self, filename: str, *, restart: bool = False) -> None:
        queue_file = _queue_file(filename)
        if not queue_file.exists():
            raise HTTPException(status_code=404, detail="Queue item not found")
        data = _read_json(queue_file)
        status, _pid = _queue_status(data)
        if not restart and status != "queued":
            raise HTTPException(status_code=409, detail=f"Queue item is already {status}")
        snapshot = _snapshot_file(filename)
        if not snapshot.is_file() or snapshot.is_symlink():
            raise HTTPException(status_code=422, detail="Queue config snapshot is missing")
        runtime_invalid_marker = Path(PBGDIR) / "data" / "locks" / "pb8-runtime-invalid"
        if runtime_invalid_marker.exists() or runtime_invalid_marker.is_symlink():
            raise HTTPException(status_code=409, detail="PB8 installation or update is incomplete. The backtest remains queued.")
        try:
            runtime_lease = acquire_master_runtime_lock(Path(PBGDIR))
        except MasterUpdateBusyError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        try:
            base_snapshot = data.get("config_snapshot")
            if isinstance(base_snapshot, dict):
                launch_config = prepare_pb8_config(copy.deepcopy(base_snapshot), base_config_path=str(snapshot))
            else:
                launch_config = load_pb8_config(snapshot)
            settings = load_ini_section("backtest_v8")
            explicit_market_data = data.get("use_pbgui_market_data")
            if isinstance(explicit_market_data, bool):
                use_pbgui_market_data = explicit_market_data
                market_data_changed, market_data_path = _apply_pbgui_market_data_override(
                    launch_config,
                    explicit_market_data,
                )
            else:
                use_pbgui_market_data = str(settings.get("use_pbgui_market_data", "False")).lower() == "true"
                if use_pbgui_market_data:
                    market_data_changed, market_data_path = _apply_pbgui_market_data_override(launch_config, True)
                else:
                    market_data_changed, market_data_path = False, _get_pbgui_market_data_path()
            if isinstance(base_snapshot, dict) or market_data_changed:
                save_prepared_pb8_config(launch_config, snapshot)
            if market_data_changed:
                action = "Set" if use_pbgui_market_data else "Cleared"
                detail = f" to {market_data_path}" if use_pbgui_market_data else ""
                _log(SERVICE, f"{action} backtest.ohlcv_source_dir{detail} before PB8 launch for {filename}", level="INFO")
            runtime = pb8_runtime_status()
            if not runtime.get("ready"):
                _log(SERVICE, f"PB8 runtime is not ready for queue item {filename}", level="WARNING")
                raise HTTPException(status_code=503, detail="PB8 runtime is not ready")
            pb8_dir = Path(runtime["pb8dir"])
            cli_path = str(runtime["cli_file"])
            log_path = _safe_path(_log_dir() / f"{filename}.log", _log_dir())
            log_path.parent.mkdir(parents=True, exist_ok=True)
            rotate_managed_log_before_open(log_path, "backtests_v8")
            data["started_at"] = time.time()
            data["pb8_version"] = runtime.get("version") or ""
            data["pb8_commit"] = _runtime_commit(pb8_dir)
            data.pop("status_override", None)
            _state_file(filename).unlink(missing_ok=True)
            _launch_ready_file(filename).unlink(missing_ok=True)
            atomic_write_json(queue_file, data)

            runner = str(Path(PBGDIR) / "pb8_backtest_runner.py")
            command = [
                str(runtime["pb8venv"]),
                runner,
                "backtest",
                str(_state_file(filename)),
                str(_pid_file(filename)),
                str(_launch_ready_file(filename)),
                cli_path,
                str(pb8_dir),
                str(snapshot.resolve()),
            ]
            log_file = open(log_path, "w", encoding="utf-8")
            try:
                kwargs = {
                    "cwd": str(pb8_dir),
                    "stdout": log_file,
                    "stderr": log_file,
                    "env": {**os.environ, "PATH": str(Path(cli_path).parent) + os.pathsep + os.environ.get("PATH", "")},
                    "close_fds": True,
                }
                if platform.system() == "Windows":
                    kwargs["creationflags"] = subprocess.DETACHED_PROCESS | subprocess.CREATE_NO_WINDOW
                else:
                    kwargs["start_new_session"] = True
                process = subprocess.Popen(command, **kwargs)
            finally:
                log_file.close()
            try:
                create_time = psutil.Process(process.pid).create_time()
                atomic_write_private_text(
                    _pid_file(filename),
                    json.dumps({"pid": process.pid, "create_time": create_time}, indent=4) + "\n",
                )
                ready_deadline = time.monotonic() + 120
                while time.monotonic() < ready_deadline:
                    if _launch_ready_file(filename).is_file():
                        ready_pid = int(_launch_ready_file(filename).read_text(encoding="utf-8").strip() or 0)
                        if ready_pid == process.pid:
                            break
                    if hasattr(process, "poll") and process.poll() is not None:
                        raise RuntimeError("PB8 runner exited before acquiring the runtime launch lock")
                    time.sleep(0.05)
                else:
                    raise RuntimeError("PB8 runner did not acquire the runtime launch lock before timeout")
                _launch_ready_file(filename).unlink(missing_ok=True)
            except Exception as exc:
                _terminate_process(process.pid)
                data["status_override"] = "error"
                atomic_write_json(queue_file, data)
                _log(SERVICE, f"Failed to publish V8 process ownership for {filename}: {exc}", level="ERROR")
                raise HTTPException(status_code=500, detail=f"Failed to publish PB8 process ownership: {exc}") from exc
            _log(SERVICE, f"Launched V8 backtest {data.get('name')} ({filename})", level="INFO")
        except HTTPException:
            raise
        except PB8ConfigurationError as exc:
            data["status_override"] = "error"
            atomic_write_json(queue_file, data)
            raise _configuration_error(f"Validating V8 queue item {filename}", exc) from exc
        except Exception as exc:
            data["status_override"] = "error"
            atomic_write_json(queue_file, data)
            _log(SERVICE, f"Failed to launch V8 backtest {filename}: {exc}", level="ERROR")
            raise HTTPException(status_code=500, detail=f"Failed to launch PB8 backtest: {exc}") from exc
        finally:
            runtime_lease.release()


_worker = BacktestV8Worker()


def _cleanup_orphan_queue_snapshots() -> None:
    """Remove UUID snapshot bundles which have no authoritative queue record."""
    snapshot_root = _queue_dir() / "configs"
    if not snapshot_root.is_dir() or snapshot_root.is_symlink():
        return
    with _queue_lock():
        for directory in snapshot_root.iterdir():
            if not directory.is_dir() or directory.is_symlink():
                continue
            try:
                parsed = uuid.UUID(directory.name)
            except ValueError:
                continue
            if str(parsed) != directory.name or _queue_file(directory.name).is_file():
                continue
            rmtree(directory, ignore_errors=True)


def startup() -> None:
    """Start the V8 queue controller for this API lifespan."""
    _cleanup_orphan_queue_snapshots()
    _worker.start()


async def shutdown() -> None:
    """Stop only the controller; detached PB8 backtests remain running."""
    await _worker.stop()


@router.get("/main_page", response_class=HTMLResponse)
def main_page(request: Request, session: SessionToken = Depends(require_auth)) -> HTMLResponse:
    """Render V8 through the exact shared V7 backtest page and editor."""
    html_path = Path(PBGDIR) / "frontend" / "v7_backtest.html"
    if not html_path.exists():
        raise HTTPException(status_code=404, detail="v7_backtest.html not found")
    html = html_path.read_text(encoding="utf-8")
    origin = str(request.base_url).rstrip("/")
    replacements = {
        '"%%TOKEN%%"': json.dumps(""),
        '"%%API_BASE%%"': json.dumps(origin + "/api/backtest-v8"),
        '"%%WS_BASE%%"': json.dumps(origin.replace("http://", "ws://").replace("https://", "wss://")),
        "%%VERSION%%": PBGUI_VERSION,
        "%%SERIAL%%": PBGUI_SERIAL,
        "%%BACKTEST_VERSION%%": "v8",
        "%%BACKTEST_LABEL%%": "V8",
        "%%BACKTEST_SUBTITLE%%": "PBv8 BACKTEST",
        "%%BACKTEST_NAV_CURRENT%%": "v8_backtest",
    }
    for old, new in replacements.items():
        html = html.replace(old, new)
    nav_path = Path(PBGDIR) / "frontend" / "pbgui_nav.js"
    nav_hash = str(int(nav_path.stat().st_mtime)) if nav_path.exists() else PBGUI_VERSION
    html = html.replace("%%NAV_HASH%%", nav_hash)
    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})


@router.get("/runtime")
def runtime_status(session: SessionToken = Depends(require_auth)) -> dict:
    """Return static PB8 readiness for the page banner."""
    return pb8_runtime_status()


@router.get("/settings")
def get_settings(session: SessionToken = Depends(require_auth)) -> dict:
    settings = load_ini_section("backtest_v8")
    cpu_max = multiprocessing.cpu_count()
    return {
        "autostart": str(settings.get("autostart", "False")).lower() == "true",
        "cpu": _cpu_limit(settings),
        "cpu_max": cpu_max,
        "use_pbgui_market_data": str(settings.get("use_pbgui_market_data", "False")).lower() == "true",
        "hsl_signal_modes": ["coin", "pside", "unified"],
        "hlcvs_cleanup_enabled": str(settings.get("hlcvs_cleanup_enabled", "False")).lower() == "true",
        "hlcvs_cleanup_days": _bounded_setting(settings, "hlcvs_cleanup_days", 7, 1, 365),
        "hlcvs_cleanup_interval_h": _bounded_setting(settings, "hlcvs_cleanup_interval_h", 24, 1, 168),
    }


@router.post("/settings")
def update_settings(body: dict, session: SessionToken = Depends(require_auth)) -> dict:
    updates = {}
    if "autostart" in body:
        if type(body["autostart"]) is not bool:
            raise HTTPException(status_code=422, detail="autostart must be a boolean")
        updates["autostart"] = str(body["autostart"])
    if "cpu" in body:
        if type(body["cpu"]) is not int:
            raise HTTPException(status_code=422, detail="cpu must be an integer")
        updates["cpu"] = str(max(1, min(body["cpu"], multiprocessing.cpu_count())))
    if "use_pbgui_market_data" in body:
        if type(body["use_pbgui_market_data"]) is not bool:
            raise HTTPException(status_code=422, detail="use_pbgui_market_data must be a boolean")
        updates["use_pbgui_market_data"] = str(body["use_pbgui_market_data"])
    if "hlcvs_cleanup_enabled" in body:
        if type(body["hlcvs_cleanup_enabled"]) is not bool:
            raise HTTPException(status_code=422, detail="hlcvs_cleanup_enabled must be a boolean")
        updates["hlcvs_cleanup_enabled"] = str(body["hlcvs_cleanup_enabled"])
    if "hlcvs_cleanup_days" in body:
        if type(body["hlcvs_cleanup_days"]) is not int:
            raise HTTPException(status_code=422, detail="hlcvs_cleanup_days must be an integer")
        updates["hlcvs_cleanup_days"] = str(max(1, min(body["hlcvs_cleanup_days"], 365)))
    if "hlcvs_cleanup_interval_h" in body:
        if type(body["hlcvs_cleanup_interval_h"]) is not int:
            raise HTTPException(status_code=422, detail="hlcvs_cleanup_interval_h must be an integer")
        updates["hlcvs_cleanup_interval_h"] = str(max(1, min(body["hlcvs_cleanup_interval_h"], 168)))
    save_ini_section("backtest_v8", updates)
    return {"ok": True}


@router.post("/settings/hlcvs-cleanup-now")
async def cleanup_hlcvs_now(body: dict, session: SessionToken = Depends(require_auth)) -> dict:
    """Run the shared settings dialog's explicit PB8 cache cleanup action."""
    days = body.get("days", 7) if isinstance(body, dict) else 7
    if type(days) is not int:
        raise HTTPException(status_code=422, detail="days must be an integer")
    try:
        return await asyncio.to_thread(_cleanup_pb8_caches, max(1, min(days, 365)))
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.get("/configs/new-config")
def new_config(session: SessionToken = Depends(require_auth)) -> dict:
    try:
        return {"config": get_pb8_template_config(), "param_status": {}}
    except PB8ConfigurationError as exc:
        raise _configuration_error("Loading PB8 template", exc, 503) from exc


@router.post("/configs/prepare")
def prepare_config(body: dict, session: SessionToken = Depends(require_auth)) -> dict:
    config = body.get("config") if isinstance(body, dict) else None
    if not isinstance(config, dict):
        raise HTTPException(status_code=400, detail="config must be an object")
    try:
        return {"config": prepare_pb8_config(config), "param_status": {}}
    except PB8ConfigurationError as exc:
        raise _configuration_error("Preparing PB8 config", exc) from exc


@router.get("/bot-params")
def get_bot_params(session: SessionToken = Depends(require_auth)) -> dict:
    """Return V8 bot-side leaf paths for the shared suite editor."""
    try:
        template = get_pb8_template_config()
        long_config = ((template.get("bot") or {}).get("long") or {})
        return {"params": [{"key": key} for key in sorted(_flatten_leaf_paths(long_config))]}
    except PB8ConfigurationError as exc:
        raise _configuration_error("Loading PB8 bot parameters", exc, 503) from exc


@router.get("/result-metrics")
def get_result_metrics(session: SessionToken = Depends(require_auth)) -> dict:
    """Return metrics accepted by the installed PB8 visibility configuration."""
    try:
        return {"metrics": get_pb8_result_metrics()}
    except PB8ConfigurationError as exc:
        raise _configuration_error("Loading PB8 result metrics", exc, 503) from exc


@router.get("/override-params")
def get_override_params(session: SessionToken = Depends(require_auth)) -> dict:
    """Return V8 leaf paths accepted by the shared coin-override editor."""
    try:
        template = get_pb8_template_config()
        bot = template.get("bot") if isinstance(template.get("bot"), dict) else {}
        live = template.get("live") if isinstance(template.get("live"), dict) else {}
        return {
            "params": {
                "bot": {
                    side: _flatten_leaf_metadata((bot.get(side) or {}))
                    for side in ("long", "short")
                },
                "live": _flatten_leaf_metadata(live),
            }
        }
    except PB8ConfigurationError as exc:
        raise _configuration_error("Loading PB8 override parameters", exc, 503) from exc


@router.get("/override-config/{config_name}/{filename}")
def get_override_config(config_name: str, filename: str, session: SessionToken = Depends(require_auth)) -> dict:
    """Read one sparse V8 override config from a managed config bundle."""
    config_name = _validate_name(config_name)
    filename = _validate_name(filename)
    if not filename.endswith(".json") or Path(filename).name != filename:
        raise HTTPException(status_code=400, detail="Invalid override filename")
    path = _safe_path(_config_file(config_name).parent / filename, _configs_dir())
    try:
        with _config_lock():
            if not path.is_file() or path.is_symlink():
                raise HTTPException(status_code=404, detail=f"Override config '{filename}' not found")
            return {"config": _read_json(path)}
    except RuntimeError as exc:
        _log(SERVICE, f"Failed to read V8 override {config_name}/{filename}: {exc}", level="WARNING")
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.put("/override-config/{config_name}/{filename}")
def save_override_config(
    config_name: str,
    filename: str,
    body: dict,
    session: SessionToken = Depends(require_auth),
) -> dict:
    """Atomically save one sparse V8 override config."""
    config_name = _validate_name(config_name)
    filename = _validate_override_filename(filename)
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="override config must be an object")
    with _config_lock():
        config_path = _config_file(config_name)
        if not config_path.is_file() or config_path.is_symlink():
            raise HTTPException(status_code=404, detail=f"Config '{config_name}' not found")
        config = load_pb8_config(config_path)
        if filename not in _override_filenames(config):
            raise HTTPException(status_code=422, detail=f"Override config is not referenced: {filename}")
        _save_config_bundle_locked(
            config_name,
            config,
            {filename: body},
            create_only=False,
            source_name=None,
            inherit_existing_overrides=True,
        )
    return {"ok": True}


@router.get("/pbgui_data_path")
def get_pbgui_data_path(session: SessionToken = Depends(require_auth)) -> dict:
    """Return the PBGui-managed market-data root used by the shared editor."""
    from market_data import get_market_data_root_dir

    return {"path": str(get_market_data_root_dir())}


@router.post("/ohlcv-preflight")
async def get_ohlcv_preflight(body: dict, session: SessionToken = Depends(require_auth)) -> dict:
    """Run the existing PBGui market-data readiness check for a V8 config."""
    from api.pb7_ohlcv_tools import build_ohlcv_preflight

    config = body.get("config") if isinstance(body, dict) else None
    if not isinstance(config, dict):
        raise HTTPException(status_code=400, detail="config must be an object")
    try:
        return await build_ohlcv_preflight(config)
    except HTTPException:
        raise
    except Exception as exc:
        _log(SERVICE, f"V8 OHLCV readiness failed: {exc}", level="WARNING", meta={"traceback": traceback.format_exc()})
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post("/ohlcv-preload")
def start_ohlcv_preload(body: dict, session: SessionToken = Depends(require_auth)) -> dict:
    """Start the existing detached OHLCV preload for a V8 editor config."""
    from api.pb7_ohlcv_tools import start_ohlcv_preload_job

    config = body.get("config") if isinstance(body, dict) else None
    if not isinstance(config, dict):
        raise HTTPException(status_code=400, detail="config must be an object")
    return start_ohlcv_preload_job(config)


@router.get("/ohlcv-preload/{job_id}")
def get_ohlcv_preload(job_id: str, session: SessionToken = Depends(require_auth)) -> dict:
    """Return one shared OHLCV preload job."""
    from api.pb7_ohlcv_tools import get_ohlcv_preload_job

    payload = get_ohlcv_preload_job(job_id)
    if payload is None:
        raise HTTPException(status_code=404, detail="OHLCV preload job not found")
    return payload


@router.delete("/ohlcv-preload/{job_id}")
def stop_ohlcv_preload(job_id: str, session: SessionToken = Depends(require_auth)) -> dict:
    """Stop one shared OHLCV preload job."""
    from api.pb7_ohlcv_tools import stop_ohlcv_preload_job

    payload = stop_ohlcv_preload_job(job_id)
    if payload is None:
        raise HTTPException(status_code=404, detail="OHLCV preload job not found")
    return payload


@router.get("/configs")
def list_configs(session: SessionToken = Depends(require_auth)) -> dict:
    configs = []
    root = _configs_dir()
    if root.is_dir() and not root.is_symlink():
        for directory in sorted(root.iterdir()):
            config_path = directory / "backtest.json"
            if not directory.is_dir() or directory.is_symlink() or not config_path.is_file() or config_path.is_symlink():
                continue
            try:
                config = _read_json(config_path)
                backtest = config.get("backtest") if isinstance(config.get("backtest"), dict) else {}
                live = config.get("live") if isinstance(config.get("live"), dict) else {}
                approved = live.get("approved_coins") if isinstance(live.get("approved_coins"), dict) else {}
                coins = set((approved.get("long") or []) + (approved.get("short") or []))
                bot = config.get("bot") if isinstance(config.get("bot"), dict) else {}
                result_count = len(list((_results_root() / directory.name).glob("**/analysis.json")))
                configs.append(
                    {
                        "name": directory.name,
                        "exchanges": backtest.get("exchanges") or [],
                        "coins": len(coins),
                        "start_date": backtest.get("start_date") or "",
                        "end_date": backtest.get("end_date") or "",
                        "starting_balance": backtest.get("starting_balance") or 0,
                        "twe_long": _bot_risk_value({"bot": bot}, "long", "total_wallet_exposure_limit"),
                        "twe_short": _bot_risk_value({"bot": bot}, "short", "total_wallet_exposure_limit"),
                        "pos_long": _bot_risk_value({"bot": bot}, "long", "n_positions"),
                        "pos_short": _bot_risk_value({"bot": bot}, "short", "n_positions"),
                        "results": result_count,
                        "modified": datetime.datetime.fromtimestamp(config_path.stat().st_mtime).isoformat(),
                    }
                )
            except Exception as exc:
                _log(SERVICE, f"Failed to list V8 config {directory.name}: {exc}", level="WARNING")
    return {"configs": configs}


@router.get("/configs/{name}")
def get_config(name: str, session: SessionToken = Depends(require_auth)) -> dict:
    name = _validate_name(name)
    path = _config_file(name)
    try:
        with _config_lock():
            if not path.is_file() or path.is_symlink():
                raise HTTPException(status_code=404, detail=f"Config '{name}' not found")
            return {"name": name, "config": load_pb8_config(path), "param_status": {}}
    except PB8ConfigurationError as exc:
        raise _configuration_error(f"Loading PB8 config {name}", exc) from exc


@router.put("/configs/{name}")
def save_config(
    name: str,
    body: dict,
    create_only: bool = False,
    source_name: Optional[str] = None,
    inherit_existing_overrides: bool = True,
    session: SessionToken = Depends(require_auth),
) -> dict:
    name = _validate_name(name)
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="config must be an object")
    bundle_request = "override_configs" in body
    config = body.get("config") if bundle_request else body
    override_payloads = body.get("override_configs") if bundle_request else {}
    if not isinstance(config, dict):
        raise HTTPException(status_code=400, detail="config must be an object")
    try:
        with _config_lock():
            prepared = _save_config_bundle_locked(
                name,
                config,
                override_payloads,
                create_only=create_only,
                source_name=source_name,
                inherit_existing_overrides=inherit_existing_overrides,
            )
        return {"ok": True, "name": name, "config": prepared}
    except PB8ConfigurationError as exc:
        raise _configuration_error(f"Saving PB8 config {name}", exc) from exc


@router.delete("/configs/{name}")
def delete_config(name: str, remove_results: bool = False, session: SessionToken = Depends(require_auth)) -> dict:
    name = _validate_name(name)
    target = _configs_dir() / name
    with _config_lock():
        if not target.is_dir() or target.is_symlink():
            raise HTTPException(status_code=404, detail=f"Config '{name}' not found")
        rmtree(target)
    if remove_results:
        results_dir = _safe_path(_results_root() / name, _results_root())
        if results_dir.is_dir() and not results_dir.is_symlink():
            rmtree(results_dir)
    return {"ok": True}


@router.post("/migrate-v7")
def migrate_v7(body: dict, session: SessionToken = Depends(require_auth)) -> dict:
    source, source_root, source_type = _v7_migration_source(body)
    source_name = _validate_name(str(body.get("source_name") or ""))
    target_name = _validate_name(str(body.get("target_name") or f"{source_name}_v8"))
    allow_manual_review = body.get("allow_manual_review_output", False)
    if type(allow_manual_review) is not bool:
        raise HTTPException(status_code=422, detail="allow_manual_review_output must be a boolean")
    target_dir = _safe_path(_configs_dir() / target_name, _configs_dir())
    stage_dir = _safe_path(_configs_dir() / f".migrate-{uuid.uuid4().hex}", _configs_dir())
    try:
        with _config_lock():
            if target_dir.exists():
                raise HTTPException(status_code=409, detail=f"V8 config '{target_name}' already exists")
            stage_dir.mkdir(parents=True)
            output = stage_dir / "backtest.json"
            source_payload = _read_json(source)
            migration_payload, source_adjustments = _sanitize_v7_migration_payload(source_payload)
            result_fee_adjustments = []
            if source_type == "backtest_result":
                result_fee_adjustments = _apply_result_effective_fees(
                    migration_payload,
                    source.parent,
                    source_root,
                )
            migration_source = stage_dir / ".source-v7.json"
            atomic_write_json(migration_source, migration_payload)
            _copy_override_files(source_payload, source.parent, stage_dir, source_root)
            result = migrate_pb7_config(
                migration_source,
                output,
                allow_manual_review_output=allow_manual_review,
            )
            report = result.get("report") if isinstance(result.get("report"), dict) else {}
            report = copy.deepcopy(report)
            if source_adjustments:
                report["pbgui_source_adjustments"] = source_adjustments
            if result_fee_adjustments:
                report["pbgui_result_fee_adjustments"] = result_fee_adjustments
            config = result.get("config")
            unresolved = report.get("manual_review_fields") or report.get("dropped_unsupported_fields")
            if not report.get("output_written") or not isinstance(config, dict) or unresolved:
                detail = "Migration requires manual review"
                fields = report.get("manual_review_fields") or report.get("dropped_unsupported_fields") or []
                if fields:
                    detail += ": " + "; ".join(str(item) for item in fields[:5])
                raise HTTPException(status_code=422, detail=detail)
            migration_source.unlink(missing_ok=True)
            prepared = save_prepared_pb8_config(_normalize_config(config, target_name), output)
            atomic_write_json(stage_dir / "migration_report.json", report)
            os.replace(stage_dir, target_dir)
            cache_prepared_pb8_config(prepared, target_dir / "backtest.json")
        _log(SERVICE, f"Migrated V7 {source_type} {source_name} to V8 config {target_name}", level="INFO")
        return {"ok": True, "name": target_name, "config": prepared, "report": report}
    except HTTPException:
        rmtree(stage_dir, ignore_errors=True)
        raise
    except PB8ConfigurationError as exc:
        rmtree(stage_dir, ignore_errors=True)
        raise _configuration_error(f"Migrating V7 config {source_name}", exc) from exc
    except Exception as exc:
        rmtree(stage_dir, ignore_errors=True)
        _log(
            SERVICE,
            f"V7 to V8 migration failed for {source_name}: {exc}",
            level="ERROR",
            meta={"traceback": traceback.format_exc()},
        )
        raise HTTPException(status_code=500, detail="V7 to V8 migration failed") from exc


@router.get("/queue")
def get_queue(session: SessionToken = Depends(require_auth)) -> dict:
    return {"items": _load_queue()}


@router.websocket("/ws/bt7")
async def ws_backtest(websocket: WebSocket) -> None:
    """Push the V8 queue in the same message contract consumed by the shared page."""
    if await authenticate_websocket(websocket) is None:
        return
    try:
        while True:
            settings = load_ini_section("backtest_v8")
            await websocket.send_json(
                {
                    "type": "queue_update",
                    "items": _load_queue(),
                    "settings": {
                        "autostart": str(settings.get("autostart", "False")).lower() == "true",
                        "cpu": _cpu_limit(settings),
                        "use_pbgui_market_data": str(settings.get("use_pbgui_market_data", "False")).lower() == "true",
                    },
                }
            )
            try:
                await asyncio.wait_for(websocket.receive_text(), timeout=3)
            except asyncio.TimeoutError:
                pass
    except (WebSocketDisconnect, RuntimeError):
        return
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        _log(SERVICE, f"V8 queue WebSocket failed: {exc}", level="WARNING")


@router.post("/queue")
def add_to_queue(body: dict, session: SessionToken = Depends(require_auth)) -> dict:
    name = _validate_name(str(body.get("name") or ""))
    config_path = _config_file(name)
    filename = str(uuid.uuid4())
    with _queue_lock():
        with _config_lock():
            try:
                provided = body.get("config") if isinstance(body, dict) else None
                if provided is not None and not isinstance(provided, dict):
                    raise HTTPException(status_code=400, detail="config must be an object")
                if isinstance(provided, dict):
                    config = prepare_pb8_config(_normalize_config(provided, name), base_config_path=str(config_path))
                else:
                    if not config_path.is_file() or config_path.is_symlink():
                        raise HTTPException(status_code=404, detail=f"Config '{name}' not found")
                    config = load_pb8_config(config_path)
                backtest = config.get("backtest") if isinstance(config.get("backtest"), dict) else {}
                payload = {
                    "name": name,
                    "filename": filename,
                    "exchange": backtest.get("exchanges") or [],
                    "config_snapshot": config,
                    "created_at": time.time(),
                }
                if isinstance(body.get("use_pbgui_market_data"), bool):
                    payload["use_pbgui_market_data"] = body["use_pbgui_market_data"]
                _queue_dir().mkdir(parents=True, exist_ok=True)
                snapshot = _snapshot_file(filename)
                snapshot.parent.mkdir(parents=True, exist_ok=True)
                if config_path.parent.is_dir():
                    _copy_override_files(config, config_path.parent, snapshot.parent, _configs_dir())
                else:
                    _require_override_files(config, config_path.parent)
                save_prepared_pb8_config(config, snapshot)
                atomic_write_json(_queue_file(filename), payload)
            except PB8ConfigurationError as exc:
                rmtree(_snapshot_file(filename).parent, ignore_errors=True)
                raise _configuration_error(f"Snapshotting PB8 config {name}", exc) from exc
            except Exception:
                rmtree(_snapshot_file(filename).parent, ignore_errors=True)
                raise
    return {"ok": True, "filename": filename}


@router.post("/queue/{filename}/start")
def start_queue_item(filename: str, session: SessionToken = Depends(require_auth)) -> dict:
    _worker.launch(_validate_name(filename))
    return {"ok": True}


@router.post("/queue/{filename}/restart")
def restart_queue_item(filename: str, session: SessionToken = Depends(require_auth)) -> dict:
    filename = _validate_name(filename)
    with _queue_lock():
        path = _queue_file(filename)
        if not path.is_file():
            raise HTTPException(status_code=404, detail="Queue item not found")
        _terminate_verified(filename)
        data = _read_json(path)
        data.pop("started_at", None)
        data.pop("status_override", None)
        atomic_write_json(path, data)
        _state_file(filename).unlink(missing_ok=True)
        _launch_ready_file(filename).unlink(missing_ok=True)
        _worker._launch_locked(filename, restart=True)
    return {"ok": True}


@router.post("/queue/{filename}/stop")
def stop_queue_item(filename: str, session: SessionToken = Depends(require_auth)) -> dict:
    filename = _validate_name(filename)
    with _queue_lock():
        path = _queue_file(filename)
        if not path.is_file():
            raise HTTPException(status_code=404, detail="Queue item not found")
        _terminate_verified(filename)
        data = _read_json(path)
        data["status_override"] = "stopped"
        atomic_write_json(path, data)
    return {"ok": True}


@router.delete("/queue/{filename}")
def delete_queue_item(filename: str, session: SessionToken = Depends(require_auth)) -> dict:
    filename = _validate_name(filename)
    with _queue_lock():
        _delete_queue_item_locked(filename)
    return {"ok": True}


def _delete_queue_item_locked(filename: str) -> None:
    """Delete one queue item while the caller holds the queue lock."""
    if not _queue_file(filename).is_file():
        raise HTTPException(status_code=404, detail="Queue item not found")
    _terminate_verified(filename)
    _queue_file(filename).unlink(missing_ok=True)
    _pid_file(filename).unlink(missing_ok=True)
    _state_file(filename).unlink(missing_ok=True)
    rmtree(_snapshot_file(filename).parent, ignore_errors=True)
    _safe_path(_log_dir() / f"{filename}.log", _log_dir()).unlink(missing_ok=True)


@router.post("/queue/clear-finished")
def clear_finished(session: SessionToken = Depends(require_auth)) -> dict:
    removed = 0
    with _queue_lock():
        for item in _load_queue():
            if item["status"] not in {"complete", "error", "stopped"}:
                continue
            current = _queue_item(_queue_file(item["filename"]))
            if current["status"] in {"complete", "error", "stopped"}:
                _delete_queue_item_locked(item["filename"])
                removed += 1
    return {"ok": True, "removed": removed}


@router.get("/queue/{filename}/log")
def queue_log(filename: str, session: SessionToken = Depends(require_auth)) -> dict:
    filename = _validate_name(filename)
    path = _safe_path(_log_dir() / f"{filename}.log", _log_dir())
    if not path.is_file():
        return {"exists": False, "log": ""}
    try:
        with path.open("rb") as handle:
            handle.seek(0, 2)
            handle.seek(max(0, handle.tell() - 100 * 1024))
            content = handle.read().decode("utf-8", errors="replace")
    except OSError as exc:
        _log(SERVICE, f"Failed to read V8 queue log {filename}: {exc}", level="WARNING")
        raise HTTPException(status_code=500, detail=f"Failed to read log: {exc}") from exc
    return {"exists": True, "log": content}


@router.get("/results")
def get_results(session: SessionToken = Depends(require_auth)) -> dict:
    return {"results": _list_results()}


@router.get("/results/analysis")
def get_result_analysis(path: str, session: SessionToken = Depends(require_auth)) -> dict:
    """Return analysis JSON for the shared result details panel."""
    analysis_path = _resolve_result_dir(path) / "analysis.json"
    if not analysis_path.is_file() or analysis_path.is_symlink():
        raise HTTPException(status_code=404, detail="analysis.json not found")
    try:
        return _read_json(analysis_path)
    except RuntimeError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.get("/results/config")
def get_result_config(path: str, session: SessionToken = Depends(require_auth)) -> dict:
    """Return the canonical config emitted with a PB8 result."""
    result_dir = _resolve_result_dir(path)
    config = _result_config(result_dir)
    if not config:
        raise HTTPException(status_code=404, detail="Result config not found")
    return config


@router.get("/results/files")
def get_result_files(path: str, session: SessionToken = Depends(require_auth)) -> dict:
    """List regular result files available to the shared result panel."""
    result_dir = _resolve_result_dir(path)
    return {
        "files": sorted(
            str(item.relative_to(result_dir))
            for item in result_dir.rglob("*")
            if item.is_file() and not item.is_symlink()
        )
    }


def _result_csv(path: str, filename: str) -> FileResponse:
    """Return a plain or gzip-compressed CSV result artifact."""
    result_dir = _resolve_result_dir(path)
    csv_path = _safe_path(result_dir / filename, result_dir)
    gzip_path = _safe_path(result_dir / f"{filename}.gz", result_dir)
    if csv_path.is_file() and not csv_path.is_symlink():
        return FileResponse(csv_path, media_type="text/csv", headers={"Cache-Control": "max-age=3600"})
    if gzip_path.is_file() and not gzip_path.is_symlink():
        return FileResponse(
            gzip_path,
            media_type="text/csv",
            headers={"Content-Encoding": "gzip", "Cache-Control": "max-age=3600"},
        )
    raise HTTPException(status_code=404, detail=f"{filename} not found")


@router.get("/results/equity")
def get_result_equity(path: str, session: SessionToken = Depends(require_auth)) -> FileResponse:
    """Serve PB8 balance/equity CSV through the shared V7 result contract."""
    return _result_csv(path, "balance_and_equity.csv")


@router.get("/results/fills")
def get_result_fills(path: str, session: SessionToken = Depends(require_auth)) -> FileResponse:
    """Serve PB8 fills CSV through the shared V7 result contract."""
    return _result_csv(path, "fills.csv")


@router.get("/results/image")
def get_result_image(path: str, filename: str, session: SessionToken = Depends(require_auth)) -> FileResponse:
    """Serve one managed PB8 result image."""
    if Path(filename).suffix.lower() not in {".png", ".jpg", ".jpeg", ".webp"}:
        raise HTTPException(status_code=400, detail="Unsupported image file")
    result_dir = _resolve_result_dir(path)
    target = _resolve_result_file(result_dir, filename)
    return FileResponse(target)


@router.get("/results/{filename}")
def get_result_file(filename: str, path: str, session: SessionToken = Depends(require_auth)) -> FileResponse:
    """Serve one CSV or JSON artifact below a managed PB8 result directory."""
    filename = _validate_name(filename)
    result_dir = _resolve_result_dir(path)
    target = _safe_path(result_dir / filename, result_dir)
    if not target.is_file() or target.is_symlink():
        raise HTTPException(status_code=404, detail=f"{filename} not found")
    return FileResponse(target)


@router.delete("/results")
def delete_result(path: str, session: SessionToken = Depends(require_auth)) -> dict:
    """Delete one explicitly selected PB8 result directory."""
    result_dir = _resolve_result_dir(path)
    rmtree(result_dir)
    return {"ok": True}
