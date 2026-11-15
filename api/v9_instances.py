"""Thin authenticated local-only PB9 live-instance API on the shared live bundle store.

A PB9 live bundle is ``data/run_v9/<name>/config.json`` (pure authored native
source) plus the private ``pbgui.json`` sidecar. PBGui metadata is merged into
``config.pbgui`` only in API responses for the shared Run pages; it is never
written into the native file. All synchronous work runs in the workers owned by
``api.pb9``: routes forward ``run_*`` operations through ``api.pb9._execute``
and ``api.pb9._operation`` dispatches them to ``_operation`` in this module.
"""

from __future__ import annotations

import asyncio
import copy
import json
import math
import os
import platform
import re
import secrets
import shutil
import stat
import threading
import time
import uuid
from contextlib import ExitStack, contextmanager
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import psutil
from fastapi import APIRouter, Depends, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.routing import APIRoute

from api import live_bundle_store
from api.auth import SessionToken, authenticated_push_websocket, require_auth
from api.live_bundle_store import (
    BundleStore,
    backup_dirs_unlocked as _backup_dirs_unlocked,
    coerce_version as _coerce_version,
    prune_backups_unlocked as _prune_backups_unlocked,
    validate_backup_id as _validate_backup_id,
    validate_name,
    validate_target as _validate_target,
)
from file_lock import advisory_file_lock
from logging_helpers import human_log as _log
from pb9_config import (
    MAX_DOCUMENT_BYTES,
    PB9CancelSignal,
    PB9ConfigError,
    load_pb9_template,
)
from pb9_runtime import inspect_pb9_runtime
import pbgui_purefunc
from pbgui_purefunc import PBGDIR
from secure_files import atomic_write_private_text, ensure_private_directory_tree


SERVICE = "V9Instances"
_SIDECAR = "pbgui.json"
_RUNNING_MARKER = "running_version.txt"
_STAGE_PREFIX = ".pbgui-v9-stage-"
_LOG_FILES = ("passivbot_err.log", "passivbot_err.log.old")
_OTHER_SLOT_KEYS = ("pb7dir", "pb7venv", "pb8dir", "pb8venv")
_FORCED_MODES = {
    "panic": "panic",
    "graceful_stop": "graceful_stop",
    "tp_only": "tp_only",
    "take_profit_only": "tp_only",
    "normal": None,
}
_MAX_BODY_BYTES = MAX_DOCUMENT_BYTES
_MAX_CONFIG_FILE_BYTES = 8 * 1024 * 1024
_MAX_SIDECAR_BYTES = 64 * 1024
_MAX_LOG_BYTES = 2 * 1024 * 1024
_MAX_NOTE_LENGTH = 2000
_DRAFT_TTL_SECONDS = 300
_MAX_DRAFTS = 100
_LIST_TTL_SECONDS = 1.0
_WS_INTERVAL_SECONDS = 3.0
_WS_SEND_TIMEOUT_SECONDS = 10.0
_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)
_NONBLOCK = getattr(os, "O_NONBLOCK", 0)

_drafts: dict[str, tuple[float, dict[str, Any]]] = {}
_draft_lock = threading.RLock()
_user_exchange_cache: tuple[float, dict[str, str]] = (0.0, {})
_user_exchange_lock = threading.RLock()
# Generation-guarded list snapshot: a scan invalidated by a mutation is never stored.
_list_guard = threading.Lock()
_list_generation = 0
_list_snapshot: tuple[str, float, list[dict[str, Any]]] | None = None
_logged_list_errors: dict[str, str] = {}


class _BundleError(ValueError):
    """Carry a fixed, path-free reason why a stored bundle is unusable."""


class _NoStoreRoute(APIRoute):
    """Keep authenticated PB9 Run responses and errors out of browser caches."""

    def get_route_handler(self):
        """Wrap the shared handler without changing its statuses."""
        original = super().get_route_handler()

        async def uncached(request):
            """Apply no-store to success and expected HTTP errors alike."""
            try:
                response = await original(request)
            except HTTPException as exc:
                exc.headers = {**(exc.headers or {}), "Cache-Control": "no-store"}
                raise
            response.headers["Cache-Control"] = "no-store"
            return response

        return uncached


router = APIRouter(route_class=_NoStoreRoute)


def get_pb9_editor_metadata(*args, **kwargs):
    """Load the new bridge only on use, allowing an older API to prepare its replacement."""
    from pb9_config import get_pb9_editor_metadata as inspect
    return inspect(*args, **kwargs)


def validate_pb9_live_config(*args, **kwargs):
    """Resolve native validation in the replacement process, never at router import."""
    from pb9_config import validate_pb9_live_config as validate
    return validate(*args, **kwargs)


# ── Paths, names and bounded file access ─────────────────────────────────────

def _validate_name(name: Any) -> str:
    """Validate one instance name, additionally rejecting DEL and C1 controls."""
    if not isinstance(name, str) or any(ord(char) == 127 or 128 <= ord(char) < 160 for char in name):
        raise HTTPException(status_code=400, detail="Invalid instance name")
    return validate_name(name)


def _data_root() -> Path:
    """Return the PBGui data root that owns all PB9 live state."""
    return Path(PBGDIR) / "data"


def _run_root() -> Path:
    """Return the PBGui-owned PB9 live instance root."""
    return _data_root() / "run_v9"


def _backup_root() -> Path:
    """Return the PBGui-owned PB9 live backup root."""
    return _data_root() / "backup" / "v9"


def _instance_dir(name: str) -> Path:
    """Resolve a validated instance directory below data/run_v9."""
    return _run_root() / _validate_name(name)


def _assert_safe_root() -> Path:
    """Reject symlinked storage parents before any read, write or lock."""
    root = _run_root()
    if _data_root().is_symlink() or root.is_symlink():
        raise HTTPException(status_code=409, detail="PB9 live storage must not use symlinked directories")
    return root


@contextmanager
def _run_lock(*, clean: bool = True):
    """Serialize complete PB9 bundle transactions across threads and processes."""
    root = _assert_safe_root()
    try:
        ensure_private_directory_tree(_data_root(), root)
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail="PB9 live storage must be a safe private directory") from exc
    with advisory_file_lock(root / ".write"):
        if clean:
            for entry in root.glob(f"{_STAGE_PREFIX}*"):
                if entry.is_symlink() or not entry.is_dir():
                    raise HTTPException(status_code=409, detail="Unsafe PB9 live transaction stage")
                shutil.rmtree(entry)
        yield root


@contextmanager
def _read_lock():
    """Read complete bundles under the writer lock without creating storage."""
    root = _assert_safe_root()
    if not root.is_dir():
        yield root
        return
    with advisory_file_lock(root / ".write"):
        yield root


def _read_json(path: Path, limit: int) -> Any:
    """Read one bounded regular JSON file without following a final symlink."""
    def reject_constant(value):
        """Stored bundles are strict JSON; NaN/Infinity never reach the editor."""
        raise ValueError("Nonfinite JSON constant")

    descriptor = os.open(path, os.O_RDONLY | _NOFOLLOW | _NONBLOCK)
    with os.fdopen(descriptor, "rb") as handle:
        if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
            raise _BundleError("A PB9 bundle file is not a regular file")
        data = handle.read(limit + 1)
    if len(data) > limit:
        raise _BundleError("A PB9 bundle file exceeds its size limit")
    return json.loads(data.decode("utf-8"), parse_constant=reject_constant)


def _checked_sidecar(raw: Any) -> dict[str, Any]:
    """Return owned runtime metadata and bounded non-secret editor preferences."""
    if not isinstance(raw, dict) or raw.get("runtime") != "pb9":
        raise _BundleError("pbgui.json must declare the pb9 runtime")
    version = raw.get("version")
    if type(version) is not int or version < 1:
        raise _BundleError("pbgui.json requires a positive integer version")
    note = raw.get("note", "")
    if not isinstance(note, str) or len(note) > _MAX_NOTE_LENGTH:
        raise _BundleError("pbgui.json has an invalid note")
    try:
        enabled_on = _validate_target(raw.get("enabled_on"))
    except HTTPException:
        raise _BundleError("pbgui.json has an invalid enabled_on target") from None
    return {"runtime": "pb9", "version": version, "enabled_on": enabled_on, "note": note,
            **_filter_preferences(raw)}


def _filter_preferences(raw: dict) -> dict:
    """Keep Run and Backtest filter validation identical."""
    from pb9_gui_preferences import filter_preferences
    try:
        return filter_preferences(raw)
    except ValueError as exc:
        raise _BundleError(str(exc)) from None


def _read_bundle(directory: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    """Read authored native source and its sidecar; never invoke native code."""
    if directory.is_symlink() or not directory.is_dir():
        raise _BundleError("The PB9 bundle directory is missing or unsafe")
    native = _read_json(directory / "config.json", _MAX_CONFIG_FILE_BYTES)
    if not isinstance(native, dict) or "pbgui" in native:
        raise _BundleError("config.json must be a native PB9 object without PBGui metadata")
    return native, _checked_sidecar(_read_json(directory / _SIDECAR, _MAX_SIDECAR_BYTES))


def _bundle_http_error(action: str, exc: Exception) -> HTTPException:
    """Map unreadable stored bundles to a concise response without paths or content."""
    _log(SERVICE, f"{action} failed: {type(exc).__name__}", level="WARNING")
    reason = str(exc) if isinstance(exc, _BundleError) else "the stored bundle is missing, unsafe or not valid JSON"
    return HTTPException(status_code=422, detail=f"{action} failed: {reason}")


def _load_instance(name: str) -> tuple[Path, dict[str, Any], dict[str, Any]]:
    """Load one existing live bundle; callers hold the read or write lock."""
    directory = _instance_dir(name)
    config_path = directory / "config.json"
    if directory.is_symlink() or config_path.is_symlink() or not config_path.is_file():
        raise HTTPException(status_code=404, detail=f"PB9 instance '{name}' not found")
    try:
        native, sidecar = _read_bundle(directory)
    except (OSError, ValueError) as exc:
        raise _bundle_http_error(f"Loading PB9 instance '{name}'", exc) from None
    return directory, native, sidecar


def _current_version(name: str) -> int:
    """Return the current sidecar version, or 0 when no valid metadata exists."""
    directory = _instance_dir(name)
    if directory.is_symlink():
        return 0
    try:
        return _checked_sidecar(_read_json(directory / _SIDECAR, _MAX_SIDECAR_BYTES))["version"]
    except (OSError, ValueError):
        return 0


def _highest_backup_version(name: str) -> int:
    """Return the newest retained backup version so recreated bundles stay monotonic."""
    instance_root = _backup_root() / name
    if _backup_root().is_symlink() or instance_root.is_symlink() or not instance_root.is_dir():
        return 0
    try:
        return max(
            (int(entry.name) for entry in instance_root.iterdir()
             if entry.name.isdigit() and not entry.is_symlink() and entry.is_dir()),
            default=0,
        )
    except OSError:
        return 0


def _running_version(directory: Path) -> int:
    """Read the runner's version marker; 0 means the running version is unconfirmed."""
    try:
        descriptor = os.open(directory / _RUNNING_MARKER, os.O_RDONLY | _NOFOLLOW | _NONBLOCK)
        with os.fdopen(descriptor, "rb") as handle:
            if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
                return 0
            text = handle.read(32).decode("ascii").strip()
    except (OSError, ValueError):
        return 0
    return int(text) if text.isdigit() else 0


def _read_log_tail(path: Path, lines: int) -> str:
    """Read a bounded tail of one own log without following symlinks."""
    if path.parent.is_symlink():
        return ""
    try:
        descriptor = os.open(path, os.O_RDONLY | _NOFOLLOW | _NONBLOCK)
    except OSError:
        return ""
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            return ""
        start = max(0, info.st_size - _MAX_LOG_BYTES)
        data = os.pread(descriptor, _MAX_LOG_BYTES, start)
    except OSError:
        return ""
    finally:
        os.close(descriptor)
    content = data.decode("utf-8", errors="replace").splitlines(keepends=True)
    if start > 0 and content:
        content = content[1:]
    return "".join(content[-lines:])


# ── Host, runtime, users and process identity ────────────────────────────────

def _master_hostname() -> str:
    """Return the configured local PBGui host name, failing closed when unknown."""
    try:
        snapshot = pbgui_purefunc.load_ini_snapshot(Path(PBGDIR) / "pbgui.ini")
        configured = str(snapshot.get("main", "pbname") or "").strip() if snapshot.has_option("main", "pbname") else ""
        hostname = _validate_target(configured or platform.node())
    except Exception as exc:
        _log(SERVICE, f"Local PB9 host identity is unavailable: {type(exc).__name__}", level="ERROR")
        raise HTTPException(status_code=503, detail="The local PBGui host identity is unavailable") from None
    if hostname == "disabled":
        raise HTTPException(status_code=503, detail="The local PBGui host identity is unavailable")
    return hostname


def _paths() -> dict[str, str]:
    """Read only the six allowlisted runtime slots through the PB9 owner module."""
    from api import pb9 as pb9_api

    return pb9_api._read_paths()


def _check_cancel(cancel_event: PB9CancelSignal | None) -> None:
    """Abort before further work once the owning API requested a stop."""
    if cancel_event is not None and cancel_event.is_set():
        raise PB9ConfigError("helper_cancelled")


def _native_arguments(cancel_event) -> tuple[str, str, dict[str, str]]:
    """Return the selected PB9 slot and the other slots for separation checks."""
    paths = _paths()
    _check_cancel(cancel_event)
    return paths["pb9dir"], paths["pb9venv"], {key: paths[key] for key in _OTHER_SLOT_KEYS}


def _validate_native(native: dict[str, Any], cancel_event) -> None:
    """Validate authored live source natively; the authored dict stays untouched."""
    pb9dir, pb9venv, other_slots = _native_arguments(cancel_event)
    validate_pb9_live_config(native, pb9dir, pb9venv, other_slots, cancel_event=cancel_event)
    _check_cancel(cancel_event)


def _native_http_error(exc: PB9ConfigError) -> HTTPException:
    """Map fixed native codes to statuses the owned worker must preserve."""
    from api import pb9 as pb9_api

    code = exc.error_codes[0]
    from pb9_ohlcv_probe import PROBE_ERROR_MESSAGES
    if code in PROBE_ERROR_MESSAGES:
        return pb9_api._error(422, code, PROBE_ERROR_MESSAGES[code])
    if code in pb9_api._VALIDATION_ERRORS:
        return pb9_api._error(422, code, f"PB9 rejected this config ({code})")
    if code == "helper_cancelled":
        return pb9_api._error(503, code, "The PB9 request was cancelled because the API is stopping")
    return pb9_api._error(503, code, f"The PB9 runtime request failed ({code})")


def _local_capability(cancel_event) -> dict[str, Any]:
    """Describe static local PB9 evidence; this is not a launch-readiness promise."""
    try:
        pb9dir, pb9venv, other_slots = _native_arguments(cancel_event)
        status = inspect_pb9_runtime(pb9dir, pb9venv, other_slots, cancel_event=cancel_event)
    except PB9ConfigError as exc:
        if exc.error_codes[0] == "helper_cancelled":
            raise
        status = None
    _check_cancel(cancel_event)
    if status is None:
        capable, reason = None, "PB9 runtime settings could not be read"
    elif not status["configured"]:
        capable, reason = False, "PB9 runtime paths are not configured"
    elif status["detected"]:
        capable, reason = True, "The separate local PB9 runtime layout is detected; launch readiness is not verified here"
    else:
        errors = status.get("errors") or []
        capable, reason = False, str(errors[0]) if errors else "The local PB9 runtime layout is not detected"
    return {
        "pb9_capable": capable,
        "confirmed": capable is not None,
        "source": "local_runtime",
        "reason": reason,
        "stale": False,
        "config_schema": (status or {}).get("config_schema"),
    }


def _available_users() -> list[dict[str, str]]:
    """Return configured exchange users without exposing credential values."""
    from User import Users

    users = Users()
    return [{"name": name, "exchange": users.find_exchange(name) or ""} for name in users.list()]


def _user_exchange_map() -> dict[str, str]:
    """Return a short-lived non-secret exchange lookup for frequent list pushes."""
    global _user_exchange_cache
    with _user_exchange_lock:
        now = time.monotonic()
        if _user_exchange_cache[0] > now:
            return dict(_user_exchange_cache[1])
        values = {
            str(item.get("name") or ""): str(item.get("exchange") or "")
            for item in _available_users()
            if str(item.get("name") or "")
        }
        _user_exchange_cache = (now + 30.0, values)
        return dict(values)


def _runner(name: str, paths: dict[str, str] | None = None, master: str | None = None):
    """Return the exact local RunV9 identity, or None when it cannot be built."""
    if paths is None:
        try:
            paths = _paths()
        except PB9ConfigError:
            return None
    pb9dir = str(paths.get("pb9dir") or "").strip()
    pb9venv = str(paths.get("pb9venv") or "").strip()
    if not pb9dir or not pb9venv:
        return None
    try:
        from PBRun import RunV9
    except ImportError:
        return None
    runner = RunV9()
    for key, value in (
        ("user", name),
        ("path", str(_instance_dir(name))),
        ("name", master if master is not None else _master_hostname()),
        ("pbgdir", Path(PBGDIR)),
        ("pb9dir", pb9dir),
        ("pb9venv", pb9venv),
    ):
        setattr(runner, key, value)
    return runner


def _config_referenced(config_path: str) -> bool:
    """Return whether any process names this exact config file in its arguments."""
    for process in psutil.process_iter():
        try:
            if config_path in process.cmdline():
                return True
        except (psutil.NoSuchProcess, psutil.ZombieProcess):
            continue
    return False


def _assert_stopped(name: str, target: Path) -> None:
    """Verify the exact RunV9 identity and every config-referencing process are absent."""
    try:
        runner = _runner(name)
        if runner is not None and runner.pid() is not None:
            raise HTTPException(status_code=409, detail=f"PB9 instance '{name}' is running locally - stop it first")
        if _config_referenced(str((target / "config.json").resolve())):
            raise HTTPException(
                status_code=409,
                detail=f"A process references PB9 instance '{name}' but its runtime identity is unverified",
            )
    except HTTPException:
        raise
    except Exception as exc:
        _log(SERVICE, f"PB9 stop verification failed for '{name}': {type(exc).__name__}", level="WARNING")
        raise HTTPException(status_code=409, detail="PB9 runtime could not be verified safely; nothing was changed") from None


def _account_guard():
    """Return the shared account start lock and conflict check, failing closed."""
    try:
        from passivbot_account_guard import account_start_lock, check_account_conflict
    except Exception as exc:
        _log(SERVICE, f"Passivbot account guard is unavailable: {type(exc).__name__}", level="ERROR")
        raise HTTPException(
            status_code=503,
            detail="The Passivbot account guard is unavailable; PB9 instances cannot be enabled",
        ) from None
    return account_start_lock, check_account_conflict


def _conflict_reason(result: Any) -> str | None:
    """Normalize the guard result: None means no conflict, text explains one."""
    if not result:
        return None
    if isinstance(result, dict):
        if result.get("ok") is True or result.get("conflict") is False:
            return None
        result = result.get("reason") or result.get("detail") or ""
    text = result if isinstance(result, str) else ""
    return "".join(char for char in text if ord(char) >= 32 and ord(char) != 127)[:300]


def _hold_account_guard(stack: ExitStack, user: str, target: Path) -> None:
    """Hold the cross-generation account start lock and reject a conflicting bot."""
    start_lock, check_conflict = _account_guard()
    try:
        stack.enter_context(start_lock(Path(PBGDIR), user, "pb9", target))
        reason = _conflict_reason(check_conflict(Path(PBGDIR), user, "pb9", target))
    except HTTPException:
        raise
    except Exception as exc:
        _log(SERVICE, f"PB9 account conflict check failed for '{user}': {type(exc).__name__}", level="ERROR")
        raise HTTPException(
            status_code=409,
            detail=f"An account conflict for '{user}' could not be excluded; the instance was not enabled",
        ) from None
    if reason is not None:
        detail = f"Exchange account '{user}' is already used by another Passivbot instance"
        raise HTTPException(status_code=409, detail=f"{detail}: {reason}" if reason else detail)


# ── Shared bundle store and backups ──────────────────────────────────────────

def _read_backup_metadata(config_path: Path) -> dict[str, Any]:
    """Expose sidecar metadata in the shape the shared snapshot helper expects."""
    if config_path.parent.is_symlink():
        raise _BundleError("The PB9 bundle directory is unsafe")
    return {"pbgui": _checked_sidecar(_read_json(config_path.parent / _SIDECAR, _MAX_SIDECAR_BYTES))}


def _bundle_store() -> BundleStore:
    """Build the PB9 store context from current module globals for one call."""
    return BundleStore(
        label="PB9",
        service=SERVICE,
        data_root=_data_root(),
        backup_root=_backup_root(),
        log=_log,
        read_backup_metadata=_read_backup_metadata,
        native_errors=(OSError, ValueError),
        configuration_error=_bundle_http_error,
        sidecar_filenames=lambda _config: (_SIDECAR,),
        optional_logs=lambda _name, instance_dir: {filename: instance_dir / filename for filename in _LOG_FILES},
    )


def _snapshot(name: str, instance_dir: Path) -> str:
    """Create one PB9 backup under the cross-process backup lock."""
    store = _bundle_store()
    with live_bundle_store.backup_lock(store):
        return live_bundle_store.snapshot_bundle_unlocked(store, name, instance_dir)


def _load_backup_unlocked(name: str, backup_id: str) -> tuple[Path, dict[str, Any], dict[str, Any]]:
    """Load one complete PB9 backup bundle while holding the backup lock."""
    backup_dir = _backup_root() / name / backup_id
    if backup_dir.parent.is_symlink() or backup_dir.is_symlink() or not backup_dir.is_dir():
        raise HTTPException(status_code=404, detail=f"PB9 backup '{name}/{backup_id}' not found")
    try:
        native, sidecar = _read_bundle(backup_dir)
    except (OSError, ValueError) as exc:
        raise _bundle_http_error(f"Loading PB9 backup '{name}/{backup_id}'", exc) from None
    return backup_dir, native, sidecar


def _backup_log(backup_dir: Path) -> str:
    """Return the first safe log filename retained in one backup."""
    for filename in _LOG_FILES:
        path = backup_dir / filename
        if path.is_file() and not path.is_symlink():
            return filename
    return ""


# ── Drafts and list snapshot ─────────────────────────────────────────────────

def _clean_drafts() -> None:
    """Remove expired and excess in-memory drafts while holding the draft lock."""
    cutoff = time.time() - _DRAFT_TTL_SECONDS
    for draft_id, (created_at, _payload) in list(_drafts.items()):
        if created_at < cutoff:
            _drafts.pop(draft_id, None)
    excess = len(_drafts) - _MAX_DRAFTS + 1
    if excess > 0:
        for draft_id, _entry in sorted(_drafts.items(), key=lambda item: item[1][0])[:excess]:
            _drafts.pop(draft_id, None)


def _store_draft(payload: dict[str, Any]) -> str:
    """Store one bounded short-lived editor draft owned by this server process."""
    with _draft_lock:
        _clean_drafts()
        draft_id = secrets.token_urlsafe(16)
        _drafts[draft_id] = (time.time(), copy.deepcopy(payload))
    return draft_id


def _get_draft(draft_id: str) -> dict[str, Any]:
    """Return one unexpired PB9 editor draft."""
    if not isinstance(draft_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", draft_id):
        raise HTTPException(status_code=404, detail="Draft not found or expired")
    with _draft_lock:
        _clean_drafts()
        entry = _drafts.get(draft_id)
        if entry is None:
            raise HTTPException(status_code=404, detail="Draft not found or expired")
        return copy.deepcopy(entry[1])


def clear_drafts() -> None:
    """Drop all in-memory drafts; optional for API shutdown."""
    with _draft_lock:
        _drafts.clear()


def _invalidate_instance_list() -> None:
    """Discard the snapshot before returning a successful local mutation."""
    global _list_generation, _list_snapshot
    with _list_guard:
        _list_generation += 1
        _list_snapshot = None


def _cached_instances() -> list[dict[str, Any]] | None:
    """Return an isolated copy of a still-current snapshot without touching disk."""
    scope = str(_run_root())
    with _list_guard:
        if _list_snapshot is not None and _list_snapshot[0] == scope and time.monotonic() < _list_snapshot[1]:
            return copy.deepcopy(_list_snapshot[2])
    return None


def _list_instances() -> list[dict[str, Any]]:
    """Return current rows, storing a scan only when no mutation overtook it."""
    global _list_snapshot
    rows = _cached_instances()
    if rows is not None:
        return rows
    with _list_guard:
        generation = _list_generation
    rows = _scan_instances()
    with _list_guard:
        if generation == _list_generation:
            _list_snapshot = (str(_run_root()), time.monotonic() + _LIST_TTL_SECONDS, copy.deepcopy(rows))
    return rows


def _nested(value: Any, *keys: str) -> Any:
    """Follow object keys in authored source, returning None off the path."""
    for key in keys:
        value = value.get(key) if isinstance(value, dict) else None
    return value


def _exposure_summary(native: dict[str, Any]) -> str:
    """Summarize only authored exposure values; sparse sources may omit them."""
    parts = []
    for side, prefix in (("long", "L"), ("short", "S")):
        if _nested(native, "bot", side, "enabled") is not True:
            continue
        values = {}
        for field in ("max_positions", "total_wallet_exposure_limit_ratio"):
            value = _nested(native, "bot", side, "risk", field)
            values[field] = value if value is not None else _nested(native, "bot", "global", "risk", field)
        try:
            if float(values["max_positions"] or 0) > 0 and values["total_wallet_exposure_limit_ratio"] is not None:
                parts.append(f"{prefix}={round(float(values['total_wallet_exposure_limit_ratio']), 2)}")
        except (TypeError, ValueError):
            continue
    return " | ".join(parts)


def _instance_row(directory: Path, master: str, paths: dict[str, str], exchanges: dict[str, str]) -> dict[str, Any]:
    """Build one row from bounded files and the exact local process identity."""
    name = directory.name
    native: dict[str, Any] = {}
    sidecar: dict[str, Any] = {}
    load_error = ""
    try:
        native, sidecar = _read_bundle(directory)
    except _BundleError as exc:
        load_error = str(exc)
    except (OSError, ValueError):
        load_error = "The stored PB9 bundle is missing, unsafe or not valid JSON"
    if load_error and _logged_list_errors.get(name) != load_error:
        _log(SERVICE, f"PB9 list could not read '{name}': {load_error}", level="WARNING")
    if load_error:
        _logged_list_errors[name] = load_error
    else:
        _logged_list_errors.pop(name, None)

    running: bool | None = None
    try:
        runner = _runner(name, paths, master)
        if runner is not None:
            running = runner.pid() is not None
    except Exception as exc:
        _log(SERVICE, f"PB9 process check failed for '{name}': {type(exc).__name__}", level="WARNING")
    enabled_on = str(sidecar.get("enabled_on") or "disabled")
    version = _coerce_version(sidecar.get("version"))
    running_version = _running_version(directory) if running else 0
    blocked_reason = ""
    if load_error:
        status = "config_error"
    elif enabled_on == "disabled":
        status = "stop_needed" if running else "disabled"
    elif enabled_on != master:
        status = "blocked"
        blocked_reason = (
            f"PB9 runs only on this PBGui host; the instance targets '{enabled_on}'"
            if master else "The local PBGui host identity is unavailable"
        )
    elif running is None:
        status = "blocked"
        blocked_reason = "The local PB9 runtime paths or the PB9 runner are not available; the process state is unknown"
    elif not running:
        status = "activate_needed"
    elif running_version == version:
        status = "synced"
    elif running_version > 0:
        status = "outdated"
    else:
        # Running, but no marker confirms which config version the process loaded.
        status = "collecting"
    user = _nested(native, "live", "account", "user")
    user = user if isinstance(user, str) else ""
    modes = {side: _nested(native, "live", "modes", side) for side in ("long", "short")}
    local = enabled_on == master and bool(master)
    return {
        "name": name,
        "user": user,
        "exchange": str(exchanges.get(user, "") or ""),
        "strategy": str(_nested(native, "bot", "portfolio", "strategy_kind") or "").strip(),
        "enabled_on": enabled_on,
        "version": version,
        "note": str(sidecar.get("note") or ""),
        "runtime": "pb9",
        "status": status,
        "desired_status": "local",
        "desired_state": "stopped" if enabled_on == "disabled" else "running",
        "assigned_host": "",
        "assigned_hostname": master if local else "",
        "conflicted": False,
        "load_error": load_error,
        "runtime_error": "",
        "twe": _exposure_summary(native),
        "forced_mode_long": modes["long"] if isinstance(modes["long"], str) else "",
        "forced_mode_short": modes["short"] if isinstance(modes["short"], str) else "",
        "running_on": [master] if running and master else [],
        "running_version": running_version,
        "config_version_remote": version if local else 0,
        "blocked_on": [enabled_on] if status == "blocked" else [],
        "blocked_reason": blocked_reason,
    }


def _scan_instances() -> list[dict[str, Any]]:
    """List local PB9 bundles without native imports, Git or runtime inspection."""
    root = _run_root()
    if _data_root().is_symlink() or root.is_symlink() or not root.is_dir():
        return []
    try:
        master = _master_hostname()
    except HTTPException:
        master = ""
    try:
        paths = _paths()
    except PB9ConfigError:
        paths = {}
    try:
        exchanges = _user_exchange_map()
    except Exception as exc:
        _log(SERVICE, f"PB9 list has no exchange catalog: {type(exc).__name__}", level="WARNING")
        exchanges = {}
    rows = []
    with _read_lock():
        directories = []
        for directory in sorted(root.iterdir(), key=lambda item: item.name):
            config_path = directory / "config.json"
            if (directory.name.startswith(".") or directory.is_symlink() or not directory.is_dir()
                    or config_path.is_symlink() or not config_path.is_file()):
                continue
            try:
                _validate_name(directory.name)
            except HTTPException:
                continue
            directories.append(directory)
        for stale in set(_logged_list_errors) - {directory.name for directory in directories}:
            _logged_list_errors.pop(stale, None)
        for directory in directories:
            rows.append(_instance_row(directory, master, paths, exchanges))
    return rows


# ── Save pipeline ────────────────────────────────────────────────────────────

def _split_config(config: Any) -> tuple[dict[str, Any], dict[str, Any]]:
    """Separate in-memory PBGui metadata from the authored native source."""
    if not isinstance(config, dict):
        raise HTTPException(status_code=400, detail="config must be an object")
    native = copy.deepcopy(config)
    pbgui = native.pop("pbgui", None)
    if pbgui is None:
        pbgui = {}
    if not isinstance(pbgui, dict):
        raise HTTPException(status_code=422, detail="pbgui must be an object")
    return native, pbgui


def _config_user(native: dict[str, Any]) -> str:
    """Return the nested native account user, validated as an instance name."""
    user = _nested(native, "live", "account", "user")
    if not isinstance(user, str) or not user:
        raise HTTPException(status_code=422, detail="live.account.user is required")
    try:
        return _validate_name(user)
    except HTTPException:
        raise HTTPException(status_code=422, detail="live.account.user is not a valid instance name") from None


def _version_number(value: Any, label: str = "expected_version") -> int:
    """Require an explicit non-negative integer without coercion."""
    if type(value) is not int or value < 0:
        raise HTTPException(status_code=400, detail=f"{label} must be a non-negative integer")
    return value


def _carry_running_marker(target: Path, stage: Path) -> None:
    """Keep the runner's version marker so a replaced bundle is not reported as current."""
    version = _running_version(target) if target.is_dir() and not target.is_symlink() else 0
    if version > 0:
        atomic_write_private_text(stage / _RUNNING_MARKER, str(version))


def _save(
    name: str,
    config: Any,
    *,
    expected_version: int | None,
    cancel_event,
    create_only: bool = False,
    require_stopped: bool = False,
) -> dict[str, Any]:
    """Validate natively, then back up and atomically publish one complete bundle."""
    name = _validate_name(name)
    native, pbgui = _split_config(config)
    if _config_user(native) != name:
        raise HTTPException(status_code=422, detail="PB9 instance name must match live.account.user")
    enabled_on = _validate_target(pbgui.get("enabled_on", "disabled"))
    note = pbgui.get("note")
    note = "" if note is None else note
    if not isinstance(note, str) or len(note) > _MAX_NOTE_LENGTH:
        raise HTTPException(status_code=422, detail="pbgui.note must be text of at most 2000 characters")
    try:
        preferences = _filter_preferences(pbgui)
    except _BundleError:
        raise HTTPException(status_code=422, detail="Invalid PB9 editor filter preferences") from None
    if expected_version is None:
        expected_version = _coerce_version(pbgui.get("version"))
    if enabled_on != "disabled" and enabled_on != _master_hostname():
        raise HTTPException(
            status_code=409,
            detail=f"Cannot target '{enabled_on}': PB9 instances run only on this PBGui host",
        )
    try:
        known_users = {item["name"] for item in _available_users()}
    except Exception as exc:
        _log(SERVICE, f"Could not validate PB9 exchange user: {type(exc).__name__}", level="ERROR")
        raise HTTPException(status_code=503, detail="Exchange user catalog is unavailable") from None
    if name not in known_users:
        raise HTTPException(status_code=409, detail=f"Exchange user '{name}' is not configured")
    # Native validation needs no storage lock; listings stay responsive meanwhile.
    _validate_native(native, cancel_event)

    with _run_lock() as root:
        target = root / name
        config_path = target / "config.json"
        if target.is_symlink() or config_path.is_symlink() or (target.exists() and not target.is_dir()):
            raise HTTPException(status_code=409, detail="PB9 instance path must be a safe directory")
        existed = config_path.is_file()
        if existed and create_only:
            raise HTTPException(status_code=409, detail=f"PB9 instance '{name}' already exists")
        if require_stopped:
            if not existed:
                raise HTTPException(status_code=404, detail=f"PB9 instance '{name}' not found")
            _assert_stopped(name, target)
        current = _current_version(name) if existed else 0
        if expected_version != current:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"PB9 instance '{name}' changed from version {expected_version} to {current}. Reload before saving."
                    if existed else f"PB9 instance '{name}' no longer exists. Reload before saving."
                ),
            )
        sidecar = {
            "runtime": "pb9",
            "version": max(current, _highest_backup_version(name)) + 1,
            "enabled_on": enabled_on,
            "note": note,
            **preferences,
        }
        stage = root / f"{_STAGE_PREFIX}{uuid.uuid4().hex}"
        backup_id = None
        published = False
        try:
            ensure_private_directory_tree(root, stage)
            atomic_write_private_text(
                stage / "config.json",
                json.dumps(native, indent=4, ensure_ascii=False, allow_nan=False) + "\n",
            )
            atomic_write_private_text(stage / _SIDECAR, json.dumps(sidecar, indent=4) + "\n")
            _carry_running_marker(target, stage)
            with ExitStack() as stack:
                if enabled_on != "disabled":
                    _hold_account_guard(stack, name, target)
                _check_cancel(cancel_event)
                if existed:
                    backup_id = _snapshot(name, target)
                live_bundle_store.publish_staged_bundle(stage, target, generation_prefix=".pbgui-v9", label="PB9")
                published = True
        except (HTTPException, PB9ConfigError):
            raise
        except Exception as exc:
            _log(SERVICE, f"Saving PB9 instance '{name}' failed: {type(exc).__name__}", level="ERROR")
            raise HTTPException(
                status_code=500,
                detail="PB9 config was not saved; the previous bundle is unchanged",
            ) from None
        finally:
            if not published:
                shutil.rmtree(stage, ignore_errors=True)
    _invalidate_instance_list()
    _log(SERVICE, f"Saved PB9 live config '{name}' (v{sidecar['version']})", level="INFO")
    return {
        "ok": True,
        "name": name,
        "version": sidecar["version"],
        "config": {**native, "pbgui": sidecar},
        "overrides": [],
        "backup_id": backup_id,
        "sync": {"ok": True, "pending": False, "direct": False, "status": "local"},
    }


# ── Owned-worker operations ──────────────────────────────────────────────────

def _body_fields(body: Any, required: set[str], optional: set[str] = frozenset()) -> dict[str, Any]:
    """Require an object body with exactly the documented fields."""
    if not isinstance(body, dict) or not required <= set(body) or set(body) - required - set(optional):
        raise HTTPException(status_code=400, detail="Request body has missing or unexpected fields")
    return body


def _require_no_overrides(body: dict[str, Any]) -> None:
    """PB9 live bundles carry inline coin_overrides only, never override files."""
    if body.get("override_configs") not in (None, {}):
        raise HTTPException(
            status_code=422,
            detail="PB9 supports inline coin_overrides only; override_configs must be empty",
        )


def _prepare(config: Any, cancel_event) -> dict[str, Any]:
    """Validate natively and return the authored source with in-memory metadata."""
    native, pbgui = _split_config(config)
    _validate_native(native, cancel_event)
    pbgui["runtime"] = "pb9"
    return {**native, "pbgui": pbgui}


def _op_list(args, body, cancel_event) -> dict[str, Any]:
    """List PB9 live instances with their local process state."""
    return {"instances": _list_instances()}


def _op_new_config(args, body, cancel_event) -> dict[str, Any]:
    """Return the native authored template with in-memory PBGui metadata."""
    pb9dir, pb9venv, other_slots = _native_arguments(cancel_event)
    config = load_pb9_template(pb9dir, pb9venv, other_slots, cancel_event=cancel_event)["config"]
    config["pbgui"] = {"runtime": "pb9", "version": 0, "enabled_on": "disabled", "note": ""}
    return {"config": config, "param_status": {}, "override_configs": {}}


def _op_metadata(args, body, cancel_event) -> dict[str, Any]:
    """Return the native field registry and strategy contracts unchanged."""
    pb9dir, pb9venv, other_slots = _native_arguments(cancel_event)
    return get_pb9_editor_metadata(pb9dir, pb9venv, other_slots, cancel_event=cancel_event)


def _op_load(args, body, cancel_event) -> dict[str, Any]:
    """Load one authored bundle exactly as stored; no native hydration."""
    name = _validate_name(args["name"])
    with _read_lock():
        _directory, native, sidecar = _load_instance(name)
    return {"name": name, "config": {**native, "pbgui": sidecar}, "param_status": {}, "override_configs": {}}


def _op_prepare(args, body, cancel_event) -> dict[str, Any]:
    """Validate an editor config without writing files."""
    body = _body_fields(body, {"config"})
    return {"config": _prepare(body["config"], cancel_event), "param_status": {}, "valid": True}


def _op_draft_create(args, body, cancel_event) -> dict[str, Any]:
    """Store a natively validated short-lived editor draft."""
    body = _body_fields(body, {"config"}, {"override_configs"})
    _require_no_overrides(body)
    config = _prepare(body["config"], cancel_event)
    draft_id = _store_draft({"config": config, "param_status": {}, "override_configs": {}})
    return {"draft_id": draft_id, "expires_in": _DRAFT_TTL_SECONDS}


def _op_save(args, body, cancel_event) -> dict[str, Any]:
    """Create or update one bundle under optimistic version control."""
    body = _body_fields(body, {"config"}, {"expected_version", "override_configs"})
    _require_no_overrides(body)
    expected = body.get("expected_version")
    return _save(
        args["name"],
        body["config"],
        expected_version=None if expected is None else _version_number(expected),
        cancel_event=cancel_event,
        create_only=args.get("create_only") is True,
    )


def _op_forced_mode(args, body, cancel_event) -> dict[str, Any]:
    """Set nested live.modes for both sides through the normal save pipeline."""
    name = _validate_name(args["name"])
    body = _body_fields(body, {"mode"}, {"expected_version"})
    if not isinstance(body["mode"], str):
        raise HTTPException(status_code=400, detail="mode must be an explicit string")
    requested = body["mode"].strip().lower()
    if requested not in _FORCED_MODES:
        raise HTTPException(status_code=400, detail="mode must be panic, graceful_stop, tp_only or normal")
    mode = _FORCED_MODES[requested]
    requested_version = body.get("expected_version")
    if mode is None and (type(requested_version) is not int or requested_version < 0):
        raise HTTPException(status_code=400, detail="normal mode requires expected_version")
    with _read_lock():
        _directory, native, sidecar = _load_instance(name)
    if mode is None and requested_version != sidecar["version"]:
        raise HTTPException(status_code=409, detail="PB9 instance changed; reload before clearing forced mode")
    live = native.get("live")
    modes = live.setdefault("modes", {}) if isinstance(live, dict) else None
    if not isinstance(modes, dict):
        raise HTTPException(status_code=422, detail="live.modes must be an object")
    modes["long"] = mode
    modes["short"] = mode
    result = _save(name, {**native, "pbgui": sidecar}, expected_version=sidecar["version"], cancel_event=cancel_event)
    _log(
        SERVICE,
        f"Set PB9 forced mode '{requested}' for all positions on '{name}' (v{result['version']})",
        level="INFO" if mode is None else "WARNING",
    )
    return {
        "ok": True,
        "name": name,
        "mode": "normal" if mode is None else requested,
        "forced_mode": mode or "",
        "version": result["version"],
        "backup_id": result["backup_id"],
        "sync": result["sync"],
    }


def _op_next_version(args, body, cancel_event) -> dict[str, Any]:
    """Return the next version a save of this instance would publish."""
    name = _validate_name(args["name"])
    with _read_lock():
        current = _current_version(name)
    return {"name": name, "next_version": max(current, _highest_backup_version(name)) + 1}


def _op_copy(args, body, cancel_event) -> dict[str, Any]:
    """Copy an edited bundle to another exchange user as a new disabled instance."""
    source = _validate_name(args["name"])
    body = _body_fields(body, {"target_user", "config"}, {"target_name", "override_configs"})
    _require_no_overrides(body)
    if not isinstance(body["target_user"], str) or not body["target_user"]:
        raise HTTPException(status_code=400, detail="target_user and config are required")
    target = _validate_name(body["target_user"])
    if body.get("target_name") not in (None, "", target):
        raise HTTPException(status_code=422, detail="target_name must match target_user")
    if target == source:
        raise HTTPException(status_code=409, detail="Target instance must be different from the source instance")
    with _read_lock():
        _load_instance(source)
    native, pbgui = _split_config(body["config"])
    account = _nested(native, "live", "account")
    if not isinstance(account, dict):
        raise HTTPException(status_code=422, detail="live.account must be an object")
    account["user"] = target
    result = _save(
        target,
        {**native, "pbgui": {**pbgui, "enabled_on": "disabled", "note": pbgui.get("note"), "version": 0}},
        expected_version=0,
        cancel_event=cancel_event,
        create_only=True,
    )
    result["source"] = source
    return result


def _op_delete(args, body, cancel_event) -> dict[str, Any]:
    """Back up and remove one stopped local bundle."""
    name = _validate_name(args["name"])
    if not _assert_safe_root().is_dir():
        raise HTTPException(status_code=404, detail=f"PB9 instance '{name}' not found")
    with _run_lock(clean=False) as root:
        target = root / name
        config_path = target / "config.json"
        if target.is_symlink() or not target.is_dir() or config_path.is_symlink() or not config_path.is_file():
            raise HTTPException(status_code=404, detail=f"PB9 instance '{name}' not found")
        _assert_stopped(name, target)
        _check_cancel(cancel_event)
        try:
            backup_id = _snapshot(name, target)
        except HTTPException:
            raise
        except Exception as exc:
            _log(SERVICE, f"Backing up PB9 instance '{name}' before delete failed: {type(exc).__name__}", level="ERROR")
            raise HTTPException(status_code=500, detail="PB9 instance was not deleted because its backup failed") from None
        try:
            shutil.rmtree(target)
        except OSError as exc:
            _log(SERVICE, f"Removing PB9 instance '{name}' failed: {type(exc).__name__}", level="ERROR")
            raise HTTPException(status_code=500, detail="PB9 instance was backed up, but local cleanup failed") from None
    _invalidate_instance_list()
    _log(SERVICE, f"Deleted PB9 live instance '{name}'", level="INFO")
    return {"ok": True, "name": name, "backup_id": backup_id}


def _op_restart(args, body, cancel_event) -> dict[str, Any]:
    """Stop the exact local process so the PBRun supervisor relaunches it."""
    name = _validate_name(args["name"])
    with _read_lock():
        _directory, _native, sidecar = _load_instance(name)
    if sidecar["enabled_on"] == "disabled":
        raise HTTPException(status_code=409, detail=f"PB9 instance '{name}' is disabled")
    master = _master_hostname()
    if sidecar["enabled_on"] != master:
        raise HTTPException(status_code=409, detail=f"PB9 instance '{name}' is not assigned to this PBGui host")
    runner = _runner(name, master=master)
    if runner is None:
        raise HTTPException(status_code=503, detail="The local PB9 runner is unavailable")
    _check_cancel(cancel_event)
    try:
        process = runner.pid()
        pid = process.pid if process is not None else None
        if process is not None:
            runner.stop()
    except Exception as exc:
        _log(SERVICE, f"PB9 restart could not stop '{name}': {type(exc).__name__}", level="ERROR")
        raise HTTPException(status_code=500, detail=f"PB9 instance '{name}' could not be stopped") from None
    if pid is None:
        raise HTTPException(status_code=409, detail=f"PB9 instance '{name}' is not running on {master}")
    _invalidate_instance_list()
    _log(SERVICE, f"PB9 restart requested for {name} on {master}")
    return {"success": True, "name": name, "host": "local", "pid": pid}


def _last_active_host(name: str) -> dict[str, Any]:
    """Resolve current or backed-up placement without treating it as live proof."""
    master = _master_hostname()
    with _read_lock():
        directory = _instance_dir(name)
        try:
            sidecar = None if directory.is_symlink() else _checked_sidecar(
                _read_json(directory / _SIDECAR, _MAX_SIDECAR_BYTES))
        except (OSError, ValueError):
            sidecar = None
    if sidecar and sidecar["enabled_on"] != "disabled":
        return {"name": name, "host": sidecar["enabled_on"], "version": str(sidecar["version"]),
                "master": master, "source": "current"}
    instance_root = _backup_root() / name
    if instance_root.is_dir() and not instance_root.is_symlink():
        with live_bundle_store.backup_lock(_bundle_store()):
            for backup_dir in _backup_dirs_unlocked(instance_root):
                try:
                    host = _checked_sidecar(_read_json(backup_dir / _SIDECAR, _MAX_SIDECAR_BYTES))["enabled_on"]
                except (OSError, ValueError):
                    continue
                if host == "disabled":
                    continue
                result = {"name": name, "host": host, "version": backup_dir.name, "master": master, "source": "backup"}
                filename = _backup_log(backup_dir)
                if filename:
                    result.update({
                        "backup_id": backup_dir.name,
                        "backup_file": f"BotBackup:{name}:9:{backup_dir.name}:{filename}",
                    })
                return result
    return {"name": name, "host": "", "version": "", "master": master, "source": "none"}


def _op_last_active_host(args, body, cancel_event) -> dict[str, Any]:
    """Return current or historical PB9 placement for post-mortem navigation."""
    return _last_active_host(_validate_name(args["name"]))


def _op_log(args, body, cancel_event) -> dict[str, Any]:
    """Read the own local error log, or the newest backed-up one."""
    name = _validate_name(args["name"])
    lines = args.get("lines")
    if type(lines) is not int or not 1 <= lines <= 10000:
        raise HTTPException(status_code=422, detail="lines must be between 1 and 10000")
    host_info = _last_active_host(name)
    master = host_info["master"]
    directory = _instance_dir(name)
    if not _data_root().is_symlink() and not _run_root().is_symlink() and not directory.is_symlink():
        for filename in _LOG_FILES:
            log = _read_log_tail(directory / filename, lines)
            if log:
                return {**host_info, "host": master, "log": log, "source": "local", "source_label": "local PB9 log"}
    instance_root = _backup_root() / name
    if instance_root.is_dir() and not instance_root.is_symlink():
        with live_bundle_store.backup_lock(_bundle_store()):
            for backup_dir in _backup_dirs_unlocked(instance_root):
                filename = _backup_log(backup_dir)
                if filename:
                    return {
                        **host_info,
                        "host": master,
                        "version": backup_dir.name,
                        "backup_id": backup_dir.name,
                        "backup_file": f"BotBackup:{name}:9:{backup_dir.name}:{filename}",
                        "log": _read_log_tail(backup_dir / filename, lines),
                        "source": f"backup:{backup_dir.name}",
                        "source_label": f"last active (backup {backup_dir.name})",
                    }
    return {**host_info, "log": "", "source": "none", "source_label": "no PB9 log found"}


def _op_users(args, body, cancel_event) -> dict[str, Any]:
    """List configured exchange users; credentials are never returned."""
    try:
        return {"users": _available_users()}
    except Exception as exc:
        _log(SERVICE, f"Could not list PB9 exchange users: {type(exc).__name__}", level="ERROR")
        raise HTTPException(status_code=503, detail="Exchange user catalog is unavailable") from None


def _op_hosts(args, body, cancel_event) -> dict[str, Any]:
    """Offer only 'disabled' and this host when its static PB9 layout is detected."""
    if args.get("name"):
        _validate_name(args["name"])
    required = str(args.get("config_schema") or "").strip()
    master = _master_hostname()
    capability = _local_capability(cancel_event)
    supported = capability["config_schema"]
    compatible = supported == required if required and supported else None
    capability.update({
        "name": master,
        "required_config_schema": required or None,
        "schema_compatible": compatible,
        "schema_reason": (
            f"This host provides PB9 config schema {supported}" if compatible is True
            else f"This host provides only PB9 config schema {supported}" if compatible is False
            else "No PB9 config schema comparison was possible"
        ),
    })
    hosts = ["disabled"]
    if capability["pb9_capable"] is True and compatible is not False:
        hosts.append(master)
    return {
        "request_id": str(args.get("request_id") or ""),
        "generated_at": time.time(),
        "hosts": hosts,
        "host_capabilities": {master: capability},
    }


def _op_backup_settings_get(args, body, cancel_event) -> dict[str, Any]:
    """Return PB9 live backup retention settings."""
    if not _backup_root().exists():
        return {"max_versions": 50}
    store = _bundle_store()
    with live_bundle_store.backup_lock(store):
        return live_bundle_store.backup_settings_unlocked(store)


def _op_backup_settings_put(args, body, cancel_event) -> dict[str, Any]:
    """Atomically update PB9 backup retention and prune existing history."""
    value = _body_fields(body, {"max_versions"})["max_versions"]
    if type(value) is not int:
        raise HTTPException(status_code=400, detail="max_versions must be an integer")
    if value < 1 or value > 1000:
        raise HTTPException(status_code=400, detail="max_versions must be between 1 and 1000")
    with live_bundle_store.backup_lock(_bundle_store()):
        atomic_write_private_text(_backup_root() / "_settings.json", json.dumps({"max_versions": value}, indent=4) + "\n")
        for instance_root in _backup_root().iterdir():
            if instance_root.name.startswith("_") or instance_root.is_symlink() or not instance_root.is_dir():
                continue
            _prune_backups_unlocked(instance_root, value)
    _log(SERVICE, f"PB9 backup retention updated to {value}", level="INFO")
    return {"ok": True, "max_versions": value}


def _op_backups(args, body, cancel_event) -> dict[str, Any]:
    """List immutable PB9 live bundle backups grouped by instance."""
    root = _backup_root()
    if _data_root().is_symlink() or root.is_symlink() or not root.is_dir():
        return {"backups": []}
    backups = []
    with live_bundle_store.backup_lock(_bundle_store()):
        for instance_root in sorted(root.iterdir(), key=lambda item: item.name):
            if instance_root.name.startswith("_") or instance_root.is_symlink() or not instance_root.is_dir():
                continue
            try:
                name = _validate_name(instance_root.name)
            except HTTPException:
                continue
            items = []
            for backup_dir in _backup_dirs_unlocked(instance_root):
                try:
                    created_ts = backup_dir.stat().st_mtime
                except OSError:
                    continue
                items.append({
                    "id": backup_dir.name,
                    "created_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(created_ts)),
                    "created_ts": created_ts,
                })
            if items:
                backups.append({"name": name, "backup_items": items, "timestamps": [item["id"] for item in items]})
    runtime = {item["name"]: item for item in _list_instances()}
    for item in backups:
        directory = _run_root() / item["name"]
        item["currently_exists"] = not directory.is_symlink() and (directory / "config.json").is_file()
        item["running_on"] = runtime.get(item["name"], {}).get("running_on", [])
        item["can_restore"] = True
    return {"backups": backups}


def _op_backup_draft(args, body, cancel_event) -> dict[str, Any]:
    """Load an exact backup into a disabled editor draft without changing live state."""
    name = _validate_name(args["name"])
    backup_id = _validate_backup_id(args["backup_id"])
    edit_base = args.get("edit_url")
    if not isinstance(edit_base, str) or not edit_base or len(edit_base) > 2048 or "?" in edit_base:
        raise HTTPException(status_code=500, detail="PB9 editor URL is unavailable")
    with live_bundle_store.backup_lock(_bundle_store()):
        backup_dir, native, sidecar = _load_backup_unlocked(name, backup_id)
        backup_files = sorted(
            path.name for path in backup_dir.iterdir()
            if path.is_file() and not path.is_symlink() and path.name != "config.json"
        )
    with _read_lock():
        directory = _instance_dir(name)
        exists = not directory.is_symlink() and (directory / "config.json").is_file()
        version = _current_version(name) if exists else 0
    pbgui = {
        **sidecar,
        "runtime": "pb9",
        "version": version,
        "enabled_on": "disabled",
        "note": sidecar["note"],
        "from_backup_config": {"name": name, "timestamp": backup_id},
    }
    draft_id = _store_draft({"config": {**native, "pbgui": pbgui}, "param_status": {}, "override_configs": {}})
    query = {"name": name, "draft_id": draft_id, **({} if exists else {"new": "1"})}
    return {
        "ok": True,
        "name": name,
        "timestamp": backup_id,
        "draft_id": draft_id,
        "version": version,
        "edit_url": f"{edit_base}?{urlencode(query)}",
        "backup_files": backup_files,
    }


def _op_restore(args, body, cancel_event) -> dict[str, Any]:
    """Restore a backup as a new disabled version of a stopped instance."""
    name = _validate_name(args["name"])
    backup_id = _validate_backup_id(args["backup_id"])
    with live_bundle_store.backup_lock(_bundle_store()):
        _backup_dir, native, sidecar = _load_backup_unlocked(name, backup_id)
    with _read_lock():
        current = _current_version(name)
    result = _save(
        name,
        {**native, "pbgui": {**sidecar, "enabled_on": "disabled"}},
        expected_version=current,
        cancel_event=cancel_event,
        require_stopped=True,
    )
    result["restored_backup_id"] = backup_id
    result["pre_restore_backup_id"] = result["backup_id"]
    _log(SERVICE, f"Restored PB9 live instance '{name}' from backup {backup_id} as v{result['version']}", level="INFO")
    return result


def _op_backup_delete(args, body, cancel_event) -> dict[str, Any]:
    """Delete one immutable PB9 backup version."""
    name = _validate_name(args["name"])
    backup_id = _validate_backup_id(args["backup_id"])
    with live_bundle_store.backup_lock(_bundle_store()):
        target = _backup_root() / name / backup_id
        if target.parent.is_symlink() or target.is_symlink() or not target.is_dir():
            raise HTTPException(status_code=404, detail=f"PB9 backup '{name}/{backup_id}' not found")
        shutil.rmtree(target)
        if target.parent.is_dir() and not any(target.parent.iterdir()):
            target.parent.rmdir()
    _log(SERVICE, f"Deleted PB9 backup '{name}/{backup_id}'", level="INFO")
    return {"ok": True, "name": name, "timestamp": backup_id}


_OPERATIONS = {
    "run_list": _op_list,
    "run_new_config": _op_new_config,
    "run_metadata": _op_metadata,
    "run_load": _op_load,
    "run_prepare": _op_prepare,
    "run_draft_create": _op_draft_create,
    "run_save": _op_save,
    "run_forced_mode": _op_forced_mode,
    "run_next_version": _op_next_version,
    "run_copy": _op_copy,
    "run_delete": _op_delete,
    "run_restart": _op_restart,
    "run_last_active_host": _op_last_active_host,
    "run_log": _op_log,
    "run_users": _op_users,
    "run_hosts": _op_hosts,
    "run_backup_settings_get": _op_backup_settings_get,
    "run_backup_settings_put": _op_backup_settings_put,
    "run_backups": _op_backups,
    "run_backup_draft": _op_backup_draft,
    "run_restore": _op_restore,
    "run_backup_delete": _op_backup_delete,
}


def _encode_payload(args: dict[str, Any] | None = None, body: bytes | None = None) -> bytes:
    """Wrap trusted route arguments and the raw body without decoding it on the event loop.

    The worker's strict decoder requires exactly the two members and rejects
    duplicates, so body text can never replace or extend the route arguments.
    """
    raw = body if body is not None and body.strip() else b"null"
    return b'{"args":' + json.dumps(args or {}, allow_nan=False).encode("utf-8") + b',"body":' + raw + b"}"


def _operation(operation: str, cancel_event: PB9CancelSignal, payload: bytes | None = None) -> dict:
    """Run one synchronous ``run_*`` operation inside a worker owned by api.pb9."""
    from api import pb9 as pb9_api

    try:
        _check_cancel(cancel_event)
        handler = _OPERATIONS.get(operation)
        if handler is None:
            raise PB9ConfigError("invalid_request")
        envelope = {"args": {}, "body": None}
        if payload is not None:
            envelope = pb9_api._request_envelope(payload, {"args", "body"})
            if not isinstance(envelope["args"], dict):
                raise PB9ConfigError("invalid_request")
        return handler(envelope["args"], envelope["body"], cancel_event)
    except HTTPException:
        raise
    except PB9ConfigError as exc:
        _log(SERVICE, "PB9 Run operation was rejected", level="WARNING",
             meta={"operation": operation, "error_codes": list(exc.error_codes)})
        raise _native_http_error(exc) from None
    except Exception as exc:
        _log(SERVICE, f"PB9 Run operation failed: {type(exc).__name__}", level="ERROR",
             meta={"operation": operation})
        raise HTTPException(status_code=500, detail="The PB9 Run request failed unexpectedly") from None


# ── Routes ───────────────────────────────────────────────────────────────────

def _check_query(request: Request, allowed: frozenset[str] = frozenset()) -> None:
    """Refuse undeclared query parameters, including any legacy token parameter."""
    if set(request.query_params) - allowed:
        raise HTTPException(status_code=422, detail="Unexpected query parameter")


async def _read_body(request: Request) -> bytes:
    """Bound a request body to 1 MiB before it reaches an owned worker."""
    body = bytearray()
    async for chunk in request.stream():
        if len(body) + len(chunk) > _MAX_BODY_BYTES:
            raise HTTPException(status_code=413, detail="PB9 request body is too large")
        body.extend(chunk)
    return bytes(body)


async def _run(request: Request, operation: str, args: dict[str, Any] | None = None, *,
               with_body: bool = False, query: frozenset[str] = frozenset()):
    """Forward one operation to the PB9 request owner; no local executor exists."""
    from api import pb9 as pb9_api

    _check_query(request, query)
    body = await _read_body(request) if with_body else None
    return await pb9_api._execute(request, operation, _encode_payload(args, body))


@router.get("/instances")
async def get_v9_instances(request: Request, session: SessionToken = Depends(require_auth)):
    """List PB9 live instances and their local process state."""
    return await _run(request, "run_list")


@router.get("/instances/new-config")
async def get_new_v9_instance_config(request: Request, session: SessionToken = Depends(require_auth)):
    """Return the native PB9 template with in-memory PBGui metadata."""
    return await _run(request, "run_new_config")


@router.get("/editor/metadata")
async def get_v9_editor_metadata(request: Request, session: SessionToken = Depends(require_auth)):
    """Return native field and strategy metadata for the shared Run editor."""
    return await _run(request, "run_metadata")


@router.post("/editor/prepare")
async def prepare_v9_editor_config(request: Request, session: SessionToken = Depends(require_auth)):
    """Validate a PB9 Run config natively without writing files."""
    return await _run(request, "run_prepare", with_body=True)


@router.post("/draft")
async def create_v9_editor_draft(request: Request, session: SessionToken = Depends(require_auth)):
    """Store a short-lived validated PB9 editor draft."""
    return await _run(request, "run_draft_create", with_body=True)


@router.get("/draft/{draft_id}")
async def get_v9_editor_draft(draft_id: str, request: Request, session: SessionToken = Depends(require_auth)):
    """Return one unexpired PB9 editor draft from server memory."""
    _check_query(request)
    return _get_draft(draft_id)


@router.get("/instances/{name}/config")
async def get_v9_instance_config(name: str, request: Request, session: SessionToken = Depends(require_auth)):
    """Load one authored PB9 live config with its sidecar metadata."""
    return await _run(request, "run_load", {"name": _validate_name(name)})


@router.put("/instances/{name}/config")
async def save_v9_instance_config(
    name: str,
    request: Request,
    create_only: bool = Query(False),
    session: SessionToken = Depends(require_auth),
):
    """Validate, back up and atomically publish one PB9 live bundle."""
    return await _run(request, "run_save", {"name": _validate_name(name), "create_only": create_only},
                      with_body=True, query=frozenset({"create_only"}))


@router.post("/instances/{name}/restart")
async def restart_v9_instance(name: str, request: Request, session: SessionToken = Depends(require_auth)):
    """Stop one locally assigned PB9 instance so PBRun relaunches it."""
    return await _run(request, "run_restart", {"name": _validate_name(name)})


@router.post("/instances/{name}/forced-mode")
async def set_v9_instance_forced_mode(name: str, request: Request, session: SessionToken = Depends(require_auth)):
    """Set the PB9 forced mode for both sides through the save pipeline."""
    return await _run(request, "run_forced_mode", {"name": _validate_name(name)}, with_body=True)


@router.get("/instances/{name}/next-version")
async def get_v9_instance_next_version(name: str, request: Request, session: SessionToken = Depends(require_auth)):
    """Return the next PB9 live config version."""
    return await _run(request, "run_next_version", {"name": _validate_name(name)})


@router.put("/instances/{name}/copy-config")
async def copy_v9_instance_config(name: str, request: Request, session: SessionToken = Depends(require_auth)):
    """Copy a PB9 live bundle to another user as a new disabled instance."""
    return await _run(request, "run_copy", {"name": _validate_name(name)}, with_body=True)


@router.delete("/instances/{name}")
async def delete_v9_instance(name: str, request: Request, session: SessionToken = Depends(require_auth)):
    """Back up and remove one stopped PB9 live bundle."""
    return await _run(request, "run_delete", {"name": _validate_name(name)})


@router.get("/instances/{name}/last-active-host")
async def get_v9_last_active_host(name: str, request: Request, session: SessionToken = Depends(require_auth)):
    """Return current or historical PB9 placement."""
    return await _run(request, "run_last_active_host", {"name": _validate_name(name)})


@router.get("/instances/{name}/log-smart")
async def get_v9_instance_log_smart(
    name: str,
    request: Request,
    lines: int = Query(500, ge=1, le=10000),
    session: SessionToken = Depends(require_auth),
):
    """Read the bounded local PB9 error log or the newest backed-up one."""
    return await _run(request, "run_log", {"name": _validate_name(name), "lines": lines},
                      query=frozenset({"lines"}))


@router.get("/backup-settings")
async def get_v9_backup_settings(request: Request, session: SessionToken = Depends(require_auth)):
    """Return PB9 live backup retention settings."""
    return await _run(request, "run_backup_settings_get")


@router.put("/backup-settings")
async def put_v9_backup_settings(request: Request, session: SessionToken = Depends(require_auth)):
    """Update PB9 backup retention and prune existing history."""
    return await _run(request, "run_backup_settings_put", with_body=True)


@router.get("/backups")
async def list_v9_backups(request: Request, session: SessionToken = Depends(require_auth)):
    """List immutable PB9 live bundle backups grouped by instance."""
    return await _run(request, "run_backups")


@router.post("/backups/{name}/{backup_id}/draft")
async def create_v9_backup_draft(
    name: str,
    backup_id: str,
    request: Request,
    session: SessionToken = Depends(require_auth),
):
    """Load a PB9 backup into the shared editor without changing live state."""
    return await _run(request, "run_backup_draft", {
        "name": _validate_name(name),
        "backup_id": _validate_backup_id(backup_id),
        "edit_url": str(request.url_for("get_v9_edit_page")),
    })


@router.post("/restore/{name}/{backup_id}")
async def restore_v9_instance(
    name: str,
    backup_id: str,
    request: Request,
    session: SessionToken = Depends(require_auth),
):
    """Restore a backup as a new disabled version of a stopped instance."""
    return await _run(request, "run_restore", {
        "name": _validate_name(name), "backup_id": _validate_backup_id(backup_id)})


@router.delete("/backups/{name}/{backup_id}")
async def delete_v9_backup(
    name: str,
    backup_id: str,
    request: Request,
    session: SessionToken = Depends(require_auth),
):
    """Delete one immutable PB9 backup version."""
    return await _run(request, "run_backup_delete", {
        "name": _validate_name(name), "backup_id": _validate_backup_id(backup_id)})


@router.get("/users")
async def get_v9_users(request: Request, session: SessionToken = Depends(require_auth)):
    """List configured exchange users available to PB9 live configs."""
    return await _run(request, "run_users")


@router.get("/hosts")
async def get_v9_hosts(
    request: Request,
    name: str = Query("", max_length=128),
    config_schema: str = Query("", max_length=32),
    request_id: str = Query("", max_length=128),
    session: SessionToken = Depends(require_auth),
):
    """List 'disabled' plus this host when its PB9 runtime layout is detected."""
    return await _run(
        request, "run_hosts",
        {"name": _validate_name(name) if name else "", "config_schema": config_schema, "request_id": request_id},
        query=frozenset({"name", "config_schema", "request_id"}),
    )


def _render_page(request: Request, filename: str, replacements: dict[str, Any]) -> HTMLResponse:
    """Render a shared Run page for PB9 with same-origin API placeholders."""
    from api.page_templates import render_page_urls, script_json

    frontend = Path(__file__).parent.parent / "frontend"
    html = render_page_urls(request, (frontend / filename).read_text(encoding="utf-8"), "/api/v9")
    try:
        master = _master_hostname()
    except HTTPException:
        master = ""
    values = {
        "VERSION": pbgui_purefunc.PBGUI_VERSION,
        "SERIAL": pbgui_purefunc.PBGUI_SERIAL,
        "MASTER_NAME": master,
        **replacements,
    }
    for key, value in values.items():
        html = html.replace(f'"%%{key}%%"', script_json(value))
        if key in {"VERSION", "SERIAL"}:
            html = html.replace(f"%%{key}%%", str(value))
    html = html.replace("%%RUN_LABEL%%", "PBv9")
    nav = frontend / "pbgui_nav.js"
    html = html.replace("%%NAV_HASH%%", str(int(nav.stat().st_mtime)) if nav.exists() else pbgui_purefunc.PBGUI_VERSION)
    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})


@router.get("/main_page", response_class=HTMLResponse)
def get_v9_main_page(request: Request, session: SessionToken = Depends(require_auth)) -> HTMLResponse:
    """Serve the shared Run list page for PB9."""
    return _render_page(request, "v7_run.html", {"RUN_VERSION": "v9", "RUN_LABEL": "PBv9"})


@router.get("/edit_page", response_class=HTMLResponse)
def get_v9_edit_page(
    request: Request,
    name: str = Query(""),
    new: str = Query(""),
    draft_id: str = Query(""),
    session: SessionToken = Depends(require_auth),
) -> HTMLResponse:
    """Serve the shared structured Run editor for PB9."""
    if name:
        _validate_name(name)
    return _render_page(request, "v7_edit.html", {
        "INSTANCE": name,
        "IS_NEW": "true" if new == "1" else "false",
        "DRAFT_ID": draft_id,
        "RUN_VERSION": "v9",
        "RUN_LABEL": "PBv9",
    })


@router.websocket("/ws/v9")
@authenticated_push_websocket
async def ws_v9(websocket: WebSocket) -> None:
    """Push changed PB9 snapshots through the request owner; busy capacity is transient."""
    previous = None
    try:
        while True:
            started = time.monotonic()
            instances = _cached_instances()
            if instances is None:
                owner = getattr(websocket.app.state, "pb9_requests", None)
                if owner is None:
                    break
                try:
                    instances = (await owner.run("run_list"))["instances"]
                except HTTPException as exc:
                    if (exc.headers or {}).get("X-PBGUI-Error-Codes") == "pb9_stopping":
                        break
                    # Busy workers or a pending restart: keep the socket and retry.
                except PB9ConfigError:
                    pass
            if instances is not None and instances != previous:
                await asyncio.wait_for(
                    websocket.send_json({"type": "instances", "data": instances, "generated_at": time.time()}),
                    timeout=_WS_SEND_TIMEOUT_SECONDS,
                )
                previous = instances
            await asyncio.sleep(max(0.0, _WS_INTERVAL_SECONDS - (time.monotonic() - started)))
    except (WebSocketDisconnect, asyncio.CancelledError):
        pass
    except Exception as exc:
        _log(SERVICE, f"PB9 status websocket failed: {type(exc).__name__}", level="WARNING")
    finally:
        try:
            await websocket.close()
        except (RuntimeError, WebSocketDisconnect):
            pass
