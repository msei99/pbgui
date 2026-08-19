"""FastAPI CRUD and deployment surface for PB8 live instances."""

from __future__ import annotations

import asyncio
import copy
import json
import os
import platform
import secrets
import shutil
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

from api.auth import SessionToken, authenticate_websocket, require_auth
from file_lock import advisory_file_lock
from logging_helpers import human_log as _log
from master.cluster_state import (
    PB8_OPERATION_CAPABILITY,
    append_node_placeholder,
    append_operation,
    cluster_node_was_removed,
    default_cluster_root,
    ensure_local_identity,
    generate_node_id,
    load_operations,
    persist_config_manifest_blobs,
    read_local_identity,
    read_materialized_state,
    rebuild_materialized_state,
)
from master.cluster_sync_worker import push_pb8_activation
from pb8_config import (
    PB8ConfigurationError,
    PB8MarketDataUnavailableError,
    PB8MarketRequestError,
    PB8RuntimeBusyError,
    cache_prepared_pb8_config,
    get_pb8_template_config,
    get_pb8_optimize_metadata,
    get_pb8_market_identifiers,
    get_pb8_coin_override_metadata,
    get_pb8_exchange_metadata,
    load_pb8_config,
    prepare_pb8_config,
    save_prepared_pb8_config,
    validate_pb8_override_bundle,
)
import pbgui_purefunc
from pbgui_purefunc import PBGDIR
from secure_files import atomic_write_private_text, ensure_private_directory_tree, secure_private_file


SERVICE = "V8Instances"
_PB8_RUNTIME_PROFILES = {"pb8", "pb7_pb8"}
_REMOTE_HOST_META_MAX_AGE_SECONDS = 30.0
_HOST_NODE_IDS_FILE = "host_node_ids.json"
_PB8_OPERATION_NAMES = {
    "UPSERT_PB8_CONFIG",
    "MOVE_PB8_INSTANCE",
    "DELETE_PB8_INSTANCE",
    "TOMBSTONE_PB8_INSTANCE",
}
_DRAFT_TTL_SECONDS = 300
_MAX_DRAFTS = 100
_MAX_OVERRIDE_BYTES = 1024 * 1024
_drafts: dict[str, tuple[float, dict[str, Any]]] = {}
_draft_lock = threading.RLock()
_user_exchange_cache: tuple[float, dict[str, str]] = (0.0, {})
_user_exchange_lock = threading.RLock()

router = APIRouter()
_monitor = None


def init(monitor) -> None:
    """Inject the shared VPS monitor used for fresh host capability data."""

    global _monitor
    _monitor = monitor


def _run_root() -> Path:
    """Return the PBGui-owned PB8 live instance root."""

    return Path(PBGDIR) / "data" / "run_v8"


def _cluster_root() -> Path:
    """Return the local PBCluster state root."""

    return default_cluster_root(Path(PBGDIR))


def _backup_root() -> Path:
    """Return the PBGui-owned PB8 live backup root."""
    return Path(PBGDIR) / "data" / "backup" / "v8"


def _master_hostname() -> str:
    """Return the configured local PBGui host name."""

    snapshot = pbgui_purefunc.load_ini_snapshot(Path(PBGDIR) / "pbgui.ini")
    if snapshot.has_option("main", "pbname"):
        configured = str(snapshot.get("main", "pbname") or "").strip()
        if configured:
            return configured
    return platform.node() or "local"


def _validate_name(name: str) -> str:
    """Validate one filesystem component used as an instance name."""

    value = str(name or "")
    if value != value.strip() or not value or value.startswith(".") or value in {".", ".."}:
        raise HTTPException(status_code=400, detail="Invalid instance name")
    if any(char in value for char in ("/", "\\", "\x00")) or any(ord(char) < 32 for char in value):
        raise HTTPException(status_code=400, detail="Invalid instance name")
    if len(value.encode("utf-8")) > 128:
        raise HTTPException(status_code=400, detail="Instance name is too long")
    return value


def _validate_target(hostname: Any) -> str:
    """Validate an exact enabled_on target without normalizing it silently."""

    if not isinstance(hostname, str) or hostname != hostname.strip():
        raise HTTPException(status_code=400, detail="Invalid enabled_on target")
    if not hostname or hostname in {".", ".."} or any(char in hostname for char in ("/", "\\", "\x00")):
        raise HTTPException(status_code=400, detail="Invalid enabled_on target")
    if any(ord(char) < 32 for char in hostname):
        raise HTTPException(status_code=400, detail="Invalid enabled_on target")
    return hostname


def _instance_dir(name: str) -> Path:
    """Resolve a validated instance directory below data/run_v8."""

    return _run_root() / _validate_name(name)


def _config_path(name: str) -> Path:
    """Resolve a validated PB8 live config path."""

    return _instance_dir(name) / "config.json"


def _validate_override_filename(filename: Any) -> str:
    """Validate one sparse PB8 override filename."""
    value = str(filename or "")
    if (
        not value
        or Path(value).name != value
        or not value.endswith(".json")
        or value == "config.json"
        or any(char in value for char in ("/", "\\", "\x00"))
        or any(ord(char) < 32 for char in value)
    ):
        raise HTTPException(status_code=400, detail="Invalid override filename")
    return value


def _validate_backup_id(backup_id: Any) -> str:
    """Validate an immutable numeric PB8 config-version backup ID."""
    value = str(backup_id or "")
    if not value.isdigit() or int(value) < 1:
        raise HTTPException(status_code=400, detail="Invalid backup ID")
    return value


def _referenced_overrides(config: dict[str, Any]) -> dict[str, str]:
    """Return exact PB8 market-ID-to-filename references from one config."""
    raw = config.get("coin_overrides") if isinstance(config, dict) else None
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise HTTPException(status_code=422, detail="coin_overrides must be an object")
    references: dict[str, str] = {}
    owners: dict[str, str] = {}
    seen_coins: set[str] = set()
    for coin, item in raw.items():
        if not isinstance(coin, str) or not isinstance(item, dict):
            raise HTTPException(status_code=422, detail="Invalid coin override entry")
        coin_name = coin.strip()
        if (
            not coin_name
            or len(coin_name.encode("utf-8")) > 256
            or any(ord(char) < 32 or ord(char) == 127 for char in coin_name)
            or coin_name in seen_coins
        ):
            raise HTTPException(status_code=422, detail="Invalid or duplicate coin override identifier")
        seen_coins.add(coin_name)
        filename = item.get("override_config_path")
        if filename is None:
            continue
        clean = _validate_override_filename(filename)
        previous = owners.get(clean)
        if previous and previous != coin_name:
            raise HTTPException(status_code=422, detail=f"Override file '{clean}' is referenced by multiple coins")
        owners[clean] = coin_name
        references[coin_name] = clean
    return references


def _read_override_file(path: Path) -> dict[str, Any]:
    """Read one bounded private override without following symlinks."""
    if path.is_symlink() or not path.is_file():
        raise HTTPException(status_code=422, detail=f"Referenced override '{path.name}' is missing")
    if path.stat().st_size > _MAX_OVERRIDE_BYTES:
        raise HTTPException(status_code=422, detail=f"Override '{path.name}' is too large")
    try:
        payload = json.loads(
            path.read_text(encoding="utf-8"),
            parse_constant=lambda value: (_ for _ in ()).throw(ValueError(f"Invalid JSON constant: {value}")),
        )
    except (OSError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=f"Override '{path.name}' is invalid JSON") from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=422, detail=f"Override '{path.name}' must be an object")
    return payload


def _override_payloads_by_filename(
    config: dict[str, Any],
    supplied: Any,
    current_dir: Path,
) -> dict[str, dict[str, Any]]:
    """Resolve a complete exact override bundle from submitted and existing files."""
    references = _referenced_overrides(config)
    submitted = supplied if isinstance(supplied, dict) else {}
    unknown = set(str(name) for name in submitted) - set(references.values())
    if unknown:
        raise HTTPException(status_code=422, detail=f"Unreferenced override file: {sorted(unknown)[0]}")
    result: dict[str, dict[str, Any]] = {}
    for filename in sorted(set(references.values())):
        if filename in submitted:
            payload = submitted[filename]
            if not isinstance(payload, dict):
                raise HTTPException(status_code=422, detail=f"Override '{filename}' must be an object")
            try:
                encoded = json.dumps(payload, separators=(",", ":"), allow_nan=False).encode("utf-8")
            except (TypeError, ValueError) as exc:
                raise HTTPException(status_code=422, detail=f"Override '{filename}' is not valid JSON") from exc
            if len(encoded) > _MAX_OVERRIDE_BYTES:
                raise HTTPException(status_code=422, detail=f"Override '{filename}' is too large")
            result[filename] = copy.deepcopy(payload)
        else:
            result[filename] = _read_override_file(current_dir / filename)
    return result


def _override_payloads_by_coin(instance_dir: Path, config: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Return referenced override payloads keyed for the shared frontend editor."""
    return {
        coin: _read_override_file(instance_dir / filename)
        for coin, filename in _referenced_overrides(config).items()
    }


@contextmanager
def _backup_lock():
    """Serialize PB8 backup settings, snapshots, retention, and deletion."""
    root = _backup_root()
    ensure_private_directory_tree(Path(PBGDIR) / "data", root.parent)
    ensure_private_directory_tree(root.parent, root)
    if root.is_symlink():
        raise RuntimeError("PB8 backup root must not be a symlink")
    with advisory_file_lock(root / ".write"):
        yield


def _backup_settings_unlocked() -> dict[str, int]:
    """Load bounded PB8 backup settings while holding the backup lock."""
    path = _backup_root() / "_settings.json"
    if not path.exists():
        return {"max_versions": 50}
    if path.is_symlink() or not path.is_file():
        raise HTTPException(status_code=409, detail="PB8 backup settings path is unsafe")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        value = int(payload.get("max_versions", 50)) if isinstance(payload, dict) else 50
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        _log(SERVICE, f"PB8 backup settings are invalid: {exc}", level="WARNING")
        return {"max_versions": 50}
    return {"max_versions": max(1, min(1000, value))}


def _backup_dirs_unlocked(instance_root: Path) -> list[Path]:
    """Return valid PB8 backup directories newest first."""
    if not instance_root.is_dir() or instance_root.is_symlink():
        return []
    result = []
    for entry in instance_root.iterdir():
        if entry.is_symlink() or not entry.is_dir() or not entry.name.isdigit():
            continue
        config_path = entry / "config.json"
        if config_path.is_file() and not config_path.is_symlink():
            result.append(entry)
    return sorted(result, key=lambda item: (int(item.name), item.stat().st_mtime_ns), reverse=True)


def _prune_backups_unlocked(instance_root: Path, max_versions: int) -> None:
    """Remove PB8 backup versions beyond retention while holding the lock."""
    for entry in _backup_dirs_unlocked(instance_root)[max_versions:]:
        shutil.rmtree(entry)
    if instance_root.is_dir() and not any(instance_root.iterdir()):
        instance_root.rmdir()


def _snapshot_v8_bundle_unlocked(name: str, instance_dir: Path) -> str:
    """Atomically snapshot one exact current PB8 live bundle by config version."""
    name = _validate_name(name)
    if instance_dir.is_symlink() or not instance_dir.is_dir():
        raise HTTPException(status_code=404, detail=f"PB8 instance '{name}' not found")
    config_path = instance_dir / "config.json"
    if config_path.is_symlink() or not config_path.is_file():
        raise HTTPException(status_code=404, detail=f"PB8 instance '{name}' has no safe config.json")
    try:
        config = load_pb8_config(config_path)
    except PB8ConfigurationError as exc:
        raise _configuration_http_error(f"Backing up PB8 instance '{name}'", exc) from exc
    pbgui = config.get("pbgui") if isinstance(config.get("pbgui"), dict) else {}
    version = _coerce_version(pbgui.get("version"))
    if version < 1:
        raise HTTPException(status_code=422, detail="PB8 backup requires a positive config version")

    instance_root = _backup_root() / name
    ensure_private_directory_tree(_backup_root(), instance_root)
    target = instance_root / str(version)
    if target.exists():
        if target.is_symlink() or not target.is_dir() or not (target / "config.json").is_file():
            raise HTTPException(status_code=409, detail=f"PB8 backup '{name}/{version}' is unsafe")
        return str(version)

    stage = instance_root / f".stage-{uuid.uuid4().hex}"
    ensure_private_directory_tree(instance_root, stage)
    try:
        filenames = ["config.json", *sorted(set(_referenced_overrides(config).values()))]
        for filename in filenames:
            source = instance_dir / filename
            if source.is_symlink() or not source.is_file():
                raise HTTPException(status_code=422, detail=f"PB8 backup source '{filename}' is missing or unsafe")
            destination = stage / filename
            shutil.copy2(source, destination)
            secure_private_file(destination)
        os.replace(stage, target)
    except Exception:
        shutil.rmtree(stage, ignore_errors=True)
        raise
    _prune_backups_unlocked(instance_root, _backup_settings_unlocked()["max_versions"])
    _log(SERVICE, f"Backed up PB8 live instance '{name}' as version {version}", level="INFO")
    return str(version)


def _snapshot_v8_bundle(name: str, instance_dir: Path) -> str:
    """Create one PB8 backup under the cross-process backup lock."""
    with _backup_lock():
        return _snapshot_v8_bundle_unlocked(name, instance_dir)


def _load_backup_bundle_unlocked(name: str, backup_id: str) -> tuple[Path, dict[str, Any], dict[str, dict[str, Any]]]:
    """Load and validate one complete PB8 backup bundle."""
    name = _validate_name(name)
    backup_id = _validate_backup_id(backup_id)
    backup_dir = _backup_root() / name / backup_id
    if backup_dir.is_symlink() or not backup_dir.is_dir():
        raise HTTPException(status_code=404, detail=f"PB8 backup '{name}/{backup_id}' not found")
    config_path = backup_dir / "config.json"
    if config_path.is_symlink() or not config_path.is_file():
        raise HTTPException(status_code=422, detail=f"PB8 backup '{name}/{backup_id}' has no safe config.json")
    try:
        config = load_pb8_config(config_path)
    except PB8ConfigurationError as exc:
        raise _configuration_http_error(f"Loading PB8 backup '{name}/{backup_id}'", exc) from exc
    overrides = _override_payloads_by_coin(backup_dir, config)
    return backup_dir, config, overrides


def _clean_drafts() -> None:
    """Remove expired in-memory editor drafts while holding the draft lock."""
    cutoff = time.time() - _DRAFT_TTL_SECONDS
    for draft_id, (created_at, _payload) in list(_drafts.items()):
        if created_at < cutoff:
            _drafts.pop(draft_id, None)
    excess = len(_drafts) - _MAX_DRAFTS + 1
    if excess > 0:
        oldest = sorted(_drafts.items(), key=lambda item: item[1][0])[:excess]
        for draft_id, _entry in oldest:
            _drafts.pop(draft_id, None)


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
def _flatten_leaf_metadata(value: dict[str, Any], prefix: str = "") -> dict[str, dict[str, Any]]:
    """Return dotted leaf metadata for shared structured controls."""
    result: dict[str, dict[str, Any]] = {}
    for key, item in value.items():
        path = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(item, dict):
            if item:
                result.update(_flatten_leaf_metadata(item, path))
            else:
                result[path] = {"type": "json", "default": {}}
            continue
        value_type = (
            "boolean" if isinstance(item, bool)
            else "number" if isinstance(item, (int, float))
            else "string" if isinstance(item, str)
            else "null" if item is None
            else "array" if isinstance(item, list)
            else "json"
        )
        result[path] = {"type": value_type, "default": item}
    return result


def _publish_staged_bundle(stage_dir: Path, target_dir: Path) -> None:
    """Atomically replace one complete live bundle, restoring on placement failure."""
    previous_dir = target_dir.parent / f".pbgui-v8-old-{uuid.uuid4().hex}"
    moved_previous = False
    try:
        if target_dir.exists():
            if target_dir.is_symlink() or not target_dir.is_dir():
                raise OSError("PB8 target directory is unsafe")
            os.replace(target_dir, previous_dir)
            moved_previous = True
        os.replace(stage_dir, target_dir)
    except OSError:
        if moved_previous and previous_dir.is_dir() and not target_dir.exists():
            os.replace(previous_dir, target_dir)
        raise
    if moved_previous:
        shutil.rmtree(previous_dir, ignore_errors=True)


@contextmanager
def _run_lock():
    """Serialize complete PB8 live bundle transactions across processes."""

    root = _run_root()
    ensure_private_directory_tree(Path(PBGDIR) / "data", root)
    if root.is_symlink():
        raise RuntimeError("PB8 live root must not be a symlink")
    with advisory_file_lock(root / ".write"):
        for entry in root.glob(".pbgui-v8-stage-*"):
            if entry.is_symlink() or not entry.is_dir():
                raise RuntimeError(f"Unsafe PB8 live transaction stage: {entry}")
            shutil.rmtree(entry)
        yield


def _configuration_http_error(action: str, exc: Exception) -> HTTPException:
    """Map isolated PB8 helper failures to a concise API response."""

    status_code = 503 if isinstance(exc, PB8RuntimeBusyError) else 422
    _log(SERVICE, f"{action} failed: {exc}", level="WARNING")
    return HTTPException(status_code=status_code, detail=str(exc))


def _market_data_http_error(action: str, exc: Exception) -> HTTPException:
    """Map PB8 market-catalog failures without changing config endpoint semantics."""
    status_code = getattr(exc, "status_code", 503)
    _log(SERVICE, f"{action} failed: {exc}", level="WARNING")
    return HTTPException(status_code=status_code, detail=str(exc))


def _managed_vps_entries() -> dict[str, Any]:
    """Return managed VPS inventory entries keyed by exact hostname."""

    try:
        from api.vps_manager import get_service_instance

        manager = get_service_instance().vpsmanager
        entries = getattr(manager, "vpss", [])
    except Exception:
        return {}
    return {
        str(getattr(item, "hostname", "") or "").strip(): item
        for item in entries
        if str(getattr(item, "hostname", "") or "").strip()
    }


def _managed_runtime_capability(hostname: str) -> dict[str, Any] | None:
    """Return inventory-backed PB8 eligibility for a managed VPS host."""

    item = _managed_vps_entries().get(hostname)
    if item is None:
        return None
    profile = str(getattr(item, "runtime_profile", "") or "").strip().lower()
    setup_status = str(getattr(item, "setup_status", "") or "").strip().lower()
    capable = profile in _PB8_RUNTIME_PROFILES and setup_status == "successful"
    if profile not in _PB8_RUNTIME_PROFILES:
        reason = f"VPS runtime profile is {profile or 'unknown'}"
    elif setup_status != "successful":
        reason = f"VPS setup status is {setup_status or 'unknown'}"
    else:
        reason = "VPS inventory confirms a completed PB8 setup"
    return {
        "pb8_capable": capable,
        "confirmed": True,
        "source": "vps_inventory",
        "reason": reason,
        "stale": False,
        "runtime_profile": profile or None,
        "setup_status": setup_status or None,
        "config_schema": None,
    }


def _parse_config_schema(value: object) -> tuple[int, ...] | None:
    """Parse PB8 config schema versions such as ``v8.1.0``."""

    if not isinstance(value, str):
        return None
    normalized = value.strip().lower()
    if normalized.startswith("v"):
        normalized = normalized[1:]
    parts = normalized.split(".")
    if not parts or any(not part.isdigit() for part in parts):
        return None
    return tuple(int(part) for part in parts)


def _host_supports_config_schema(required: object, supported: object) -> bool | None:
    """Return whether a host schema is at least as new as the config schema."""

    required_parts = _parse_config_schema(required)
    supported_parts = _parse_config_schema(supported)
    if required_parts is None or supported_parts is None:
        return None
    width = max(len(required_parts), len(supported_parts))
    required_cmp = required_parts + (0,) * (width - len(required_parts))
    supported_cmp = supported_parts + (0,) * (width - len(supported_parts))
    return supported_cmp >= required_cmp


def _with_schema_compatibility(capability: dict[str, Any], required_schema: str) -> dict[str, Any]:
    """Attach the config-schema decision used by the PB8 host selector."""

    result = dict(capability)
    supported_schema = result.get("config_schema")
    compatible = _host_supports_config_schema(required_schema, supported_schema) if required_schema else None
    result["required_config_schema"] = required_schema or None
    result["schema_compatible"] = compatible
    if compatible is True:
        result["schema_reason"] = f"Host supports PB8 config schema {supported_schema}"
    elif compatible is False:
        result["schema_reason"] = f"Host supports only PB8 config schema {supported_schema}"
    elif required_schema:
        result["schema_reason"] = "PB8 config schema capability is unavailable"
    else:
        result["schema_reason"] = "No PB8 config schema was requested"
    return result


def _remote_runtime_capability(hostname: str) -> dict[str, Any]:
    """Return PB8 eligibility from a fresh host-meta pb8ready value."""

    unknown = {
        "pb8_capable": None,
        "confirmed": False,
        "source": "host_meta",
        "reason": "Fresh PB8 runtime metadata has not been reported",
        "stale": False,
        "runtime_profile": None,
        "setup_status": None,
        "config_schema": None,
    }
    store = getattr(_monitor, "store", None) if _monitor is not None else None
    host_meta = getattr(store, "host_meta", {}) if store is not None else {}
    meta = host_meta.get(hostname, {}) if isinstance(host_meta, dict) else {}
    if not isinstance(meta, dict):
        return unknown
    try:
        generated_at = float(meta.get("generated_at") or 0.0)
    except (TypeError, ValueError):
        generated_at = 0.0
    age = time.time() - generated_at if generated_at > 0 else None
    if age is None or age < -300.0 or age > _REMOTE_HOST_META_MAX_AGE_SECONDS:
        unknown["stale"] = generated_at > 0
        unknown["reason"] = "PB8 runtime metadata is stale" if generated_at > 0 else "PB8 runtime metadata has no valid timestamp"
        return unknown
    ready = meta.get("pb8ready")
    if not isinstance(ready, bool):
        unknown["reason"] = "Fresh host metadata does not include pb8ready"
        return unknown
    return {
        **unknown,
        "pb8_capable": ready,
        "confirmed": True,
        "reason": "Fresh host metadata confirms PB8 readiness" if ready else "Fresh host metadata reports PB8 is not ready",
        "config_schema": str(meta.get("pb8_config_schema") or "").strip() or None,
    }


def _host_runtime_capability(hostname: str) -> dict[str, Any]:
    """Return secret-free tri-state PB8 capability for one exact target."""

    target = str(hostname or "").strip()
    if target == "disabled":
        return {
            "pb8_capable": True,
            "confirmed": True,
            "source": "disabled",
            "reason": "Disabled targets do not require PB8",
            "stale": False,
            "runtime_profile": None,
            "setup_status": None,
            "config_schema": None,
        }
    if target == _master_hostname():
        status: dict[str, Any] = {}
        try:
            status = pbgui_purefunc.pb8_runtime_status()
            ready = status.get("ready")
        except Exception as exc:
            _log(SERVICE, f"Local PB8 runtime capability unavailable: {exc.__class__.__name__}", level="WARNING")
            ready = None
        return {
            "pb8_capable": ready if isinstance(ready, bool) else None,
            "confirmed": isinstance(ready, bool),
            "source": "local_runtime",
            "reason": (
                "Local PB8 runtime is ready" if ready is True
                else "Local PB8 runtime is not ready" if ready is False
                else "Local PB8 runtime status is unavailable"
            ),
            "stale": False,
            "runtime_profile": "pb8" if ready is True else None,
            "setup_status": "successful" if ready is True else None,
            "config_schema": str(status.get("config_schema") or "").strip() or None,
        }
    managed = _managed_runtime_capability(target)
    remote = _remote_runtime_capability(target)
    capability = remote if remote["pb8_capable"] is not None else (managed if managed is not None else remote)
    if capability["pb8_capable"] is True:
        cluster_status = _remote_cluster_target_status(target)
        capability["cluster_ready"] = cluster_status["ready"]
        if not cluster_status["ready"]:
            capability["pb8_capable"] = False
            capability["confirmed"] = True
            capability["source"] = "cluster_state"
            capability["reason"] = cluster_status["reason"]
    return capability


def _remote_cluster_target_status(hostname: str) -> dict[str, Any]:
    """Return whether a remote host has a joined, reachable Cluster identity."""

    existing = _best_node_for_host(hostname)
    if existing is None:
        return {"ready": False, "reason": "Host is not registered in Cluster"}
    node_id = existing[0]
    node = _cluster_nodes().get(node_id)
    if not isinstance(node, dict):
        return {"ready": False, "reason": "Host is not registered in Cluster"}
    if node.get("enabled") is False:
        return {"ready": False, "reason": "Cluster node is disabled"}
    if node.get("state_replica") is False:
        return {"ready": False, "reason": "Host has not completed Cluster Remote Join"}
    if str(node.get("sync_mode") or "").strip().lower() != "reachable":
        return {"ready": False, "reason": "Cluster node is not reachable"}
    return {"ready": True, "reason": "Cluster node is joined and reachable", "node_id": node_id}


def _persisted_target(name: str) -> str | None:
    """Return the exact currently persisted enabled_on target, if readable."""

    path = _config_path(name)
    if _run_root().is_symlink() or path.parent.is_symlink() or not path.is_file() or path.is_symlink():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    pbgui = data.get("pbgui") if isinstance(data, dict) else None
    value = pbgui.get("enabled_on") if isinstance(pbgui, dict) else None
    return value if isinstance(value, str) else None


async def _ensure_target_compatible(name: str, enabled_on: str, config_schema: object = None) -> None:
    """Reject runtime- or schema-incompatible PB8 targets."""

    target = _validate_target(enabled_on)
    if target == "disabled":
        return
    required_schema = config_schema.strip() if isinstance(config_schema, str) else ""
    capability = _host_runtime_capability(target)
    needs_refresh = capability["pb8_capable"] is None or (
        bool(required_schema) and not capability.get("config_schema")
    )
    if needs_refresh and _monitor is not None and hasattr(_monitor, "collect_host_meta_now"):
        try:
            await _monitor.collect_host_meta_now(target, include_package_status=False)
        except Exception as exc:
            _log(SERVICE, f"PB8 capability refresh failed for '{target}': {exc.__class__.__name__}", level="WARNING")
        capability = _host_runtime_capability(target)
    if capability["pb8_capable"] is True:
        if not required_schema:
            return
        schema_compatible = _host_supports_config_schema(required_schema, capability.get("config_schema"))
        if schema_compatible is True:
            return
        if schema_compatible is False:
            supported_schema = capability.get("config_schema")
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Cannot target '{target}' with PB8 instance '{name}': config schema {required_schema} "
                    f"requires a newer PB8 runtime; this host supports only {supported_schema}. Update PB8 on "
                    f"'{target}' before saving or starting this bot."
                ),
            )
        if _persisted_target(name) == target:
            return
        raise HTTPException(
            status_code=409,
            detail=(
                f"Cannot target '{target}' with PB8 instance '{name}': support for config schema "
                f"{required_schema} is not confirmed. Wait for fresh host metadata or update PB8 on '{target}'."
            ),
        )
    reason = str(capability.get("reason") or "PB8 capability is unavailable")
    if capability["pb8_capable"] is None and _persisted_target(name) == target:
        return
    raise HTTPException(
        status_code=409,
        detail=f"Cannot target '{target}' with PB8 instance '{name}': {reason}.",
    )


def _cluster_nodes() -> dict[str, dict[str, Any]]:
    """Return current materialized cluster nodes without trusting malformed values."""

    try:
        materialized = read_materialized_state(_cluster_root())
    except Exception:
        return {}
    cluster_nodes = materialized.get("cluster_nodes") if isinstance(materialized, dict) else None
    nodes = cluster_nodes.get("nodes") if isinstance(cluster_nodes, dict) else None
    return nodes if isinstance(nodes, dict) else {}


def _best_node_for_host(hostname: str) -> tuple[str, str] | None:
    """Find the best existing cluster node for one hostname or pbname."""

    candidates: list[tuple[tuple[int, int], str, dict[str, Any]]] = []
    for node_id, node in _cluster_nodes().items():
        if not isinstance(node, dict):
            continue
        names = {str(node.get("hostname") or "").strip(), str(node.get("pbname") or "").strip()}
        if hostname not in names:
            continue
        score = (1 if node.get("enabled") is not False else 0, 1 if node.get("state_replica") is not False else 0)
        candidates.append((score, str(node_id), node))
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0], reverse=True)
    _, node_id, node = candidates[0]
    return node_id, str(node.get("role") or "vps")


def _host_node_mapping(hostname: str) -> str:
    """Return a stable generated node ID using a locked atomic mapping update."""

    root = _cluster_root()
    path = root / _HOST_NODE_IDS_FILE
    if path.is_symlink():
        raise RuntimeError("Cluster host-node mapping must not be a symlink")
    with advisory_file_lock(path):
        data: dict[str, Any] = {"schema_version": 1, "hosts": {}}
        if path.is_file() and not path.is_symlink():
            try:
                loaded = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(loaded, dict) and isinstance(loaded.get("hosts"), dict):
                    data = {"schema_version": 1, "hosts": dict(loaded["hosts"])}
            except (OSError, json.JSONDecodeError):
                pass
        entry = data["hosts"].get(hostname)
        node_id = str(entry.get("node_id") or "") if isinstance(entry, dict) else ""
        if node_id and not cluster_node_was_removed(root, node_id):
            return node_id
        node_id = generate_node_id()
        data["hosts"][hostname] = {"node_id": node_id, "created_at": int(time.time()), "role": "vps"}
        atomic_write_private_text(path, json.dumps(data, indent=4, sort_keys=True) + "\n")
        return node_id


def _ensure_node_record(node_id: str, hostname: str, role: str, identity: dict[str, Any]) -> None:
    """Publish local membership or a non-replica remote inventory placeholder."""

    if node_id in _cluster_nodes():
        return
    payload = {
        "node_id": node_id,
        "role": role,
        "pbname": hostname,
        "hostname": hostname,
        "sync_mode": "outbound_only" if role == "master" else "disabled",
        "sync_enabled": role == "master",
    }
    if node_id == str(identity["node_id"]):
        append_operation(_cluster_root(), "ADD_NODE", payload)
    else:
        append_node_placeholder(_cluster_root(), payload)


def _assigned_cluster_node(enabled_on: str, identity: dict[str, Any]) -> tuple[str, str, str]:
    """Resolve enabled_on to the stable cluster node referenced by desired state."""

    master = _master_hostname()
    if enabled_on in {"disabled", master}:
        return str(identity["node_id"]), master, "master"
    existing = _best_node_for_host(enabled_on)
    if existing is not None:
        return existing[0], enabled_on, existing[1]
    return _host_node_mapping(enabled_on), enabled_on, "vps"


def _ensure_pb8_cluster_rollout_ready(identity: dict[str, Any]) -> None:
    """Block PB8 operations until every active remote state replica confirms support."""

    local_node_id = str(identity.get("node_id") or "")
    required_nodes = {
        str(node_id): node
        for node_id, node in _cluster_nodes().items()
        if str(node_id) != local_node_id
        and isinstance(node, dict)
        and node.get("enabled") is not False
        and node.get("state_replica") is not False
    }
    if not required_nodes:
        return
    status_path = _cluster_root() / "sync_status.json"
    try:
        status = json.loads(status_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        status = {}
    finished_at = int(status.get("finished_at") or 0) if isinstance(status, dict) else 0
    peers = status.get("peers") if isinstance(status, dict) else []
    peers_by_id = {
        str(item.get("node_id") or ""): item
        for item in peers
        if isinstance(item, dict) and item.get("node_id")
    } if isinstance(peers, list) else {}
    blockers = []
    status_fresh = finished_at > 0 and abs(time.time() - finished_at) <= 120
    for node_id, node in sorted(required_nodes.items()):
        peer = peers_by_id.get(node_id, {})
        if not status_fresh or peer.get("pb8_capability") is not True:
            blockers.append(str(node.get("pbname") or node.get("hostname") or node_id))
    if blockers:
        raise HTTPException(
            status_code=409,
            detail=(
                f"PB8 Cluster rollout is not ready for {', '.join(blockers)}. "
                f"Update all active Cluster state replicas and wait for fresh {PB8_OPERATION_CAPABILITY} handshakes."
            ),
        )


def _coerce_version(value: Any) -> int:
    """Return a non-negative integer config version."""

    if isinstance(value, bool):
        return 0
    try:
        return max(0, int(str(value).strip()))
    except (TypeError, ValueError):
        return 0


def _current_version(name: str) -> int:
    """Return the current local PB8 config version."""

    path = _config_path(name)
    if _run_root().is_symlink() or path.parent.is_symlink() or not path.is_file() or path.is_symlink():
        return 0
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return 0
    pbgui = data.get("pbgui") if isinstance(data, dict) else None
    return _coerce_version(pbgui.get("version")) if isinstance(pbgui, dict) else 0


def _highest_cluster_version(name: str) -> int:
    """Return the highest PB8 version recorded in local cluster history."""

    try:
        root = _cluster_root()
        identity = read_local_identity(root)
        operations = load_operations(root, expected_cluster_id=str(identity["cluster_id"]))
    except Exception:
        return 0
    highest = 0
    for operation in operations:
        if str(operation.get("instance") or "") != name or str(operation.get("op") or "") not in _PB8_OPERATION_NAMES:
            continue
        highest = max(highest, _coerce_version(operation.get("version")), _coerce_version(operation.get("parent_version")))
    return highest


def _record_upsert(name: str, instance_dir: Path, config: dict[str, Any], parent_version: int, is_new: bool) -> dict[str, Any]:
    """Publish one immutable PB8 config manifest and explicit upsert operation."""

    root = _cluster_root()
    identity = ensure_local_identity(root, role="master", pbname=_master_hostname())
    _ensure_pb8_cluster_rollout_ready(identity)
    pbgui = config["pbgui"]
    enabled_on = str(pbgui["enabled_on"])
    assigned_id, assigned_name, assigned_role = _assigned_cluster_node(enabled_on, identity)
    _ensure_node_record(str(identity["node_id"]), _master_hostname(), "master", identity)
    if assigned_id != str(identity["node_id"]):
        _ensure_node_record(assigned_id, assigned_name, assigned_role, identity)
    manifest_hash = persist_config_manifest_blobs(root, instance_dir)
    operation = append_operation(root, "UPSERT_PB8_CONFIG", {
        "instance": name,
        "version": str(pbgui["version"]),
        "parent_version": str(parent_version),
        "assigned_host": assigned_id,
        "desired_state": "running" if enabled_on != "disabled" else "stopped",
        "config_manifest_hash": manifest_hash,
        "enabled_on": enabled_on,
        "allow_tombstone_recreate": bool(is_new),
    })
    try:
        rebuild_materialized_state(root)
    except Exception as exc:
        _log(SERVICE, f"PB8 desired-state rebuild deferred after {operation['op_id']}: {exc}", level="WARNING")
    return operation


async def _activate_pb8_target(name: str, operation: dict[str, Any]) -> dict[str, Any]:
    """Try one bounded direct activation before PBCluster completes replication."""

    try:
        direct = await asyncio.to_thread(push_pb8_activation, _cluster_root(), operation, timeout=3)
        success = bool(direct.get("ok"))
        return {
            "ok": success,
            "direct": bool(direct.get("direct")),
            "pending": not success,
            "node_id": str(direct.get("node_id") or ""),
            "host": str(direct.get("pbname") or ""),
            "materialization": direct.get("materialization") or {},
        }
    except Exception as exc:
        _log(SERVICE, f"Fast PB8 activation for '{name}' deferred to PBCluster: {exc}", level="WARNING")
        return {
            "ok": False,
            "direct": False,
            "pending": True,
            "reason": "Fast activation deferred to PBCluster",
        }


def _record_delete(name: str, version: int) -> dict[str, Any]:
    """Publish an explicit PB8 tombstone operation before local deletion."""

    root = _cluster_root()
    identity = ensure_local_identity(root, role="master", pbname=_master_hostname())
    _ensure_pb8_cluster_rollout_ready(identity)
    _ensure_node_record(str(identity["node_id"]), _master_hostname(), "master", identity)
    operation = append_operation(root, "DELETE_PB8_INSTANCE", {"instance": name, "version": str(version)})
    try:
        rebuild_materialized_state(root)
    except Exception as exc:
        _log(SERVICE, f"PB8 desired-state rebuild deferred after {operation['op_id']}: {exc}", level="WARNING")
    return operation


def _desired_pb8_state() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Return PB8 instances, tombstones, and cluster nodes from materialized state."""

    try:
        materialized = read_materialized_state(_cluster_root())
    except Exception:
        return {}, {}, {}
    desired = materialized.get("desired_state") if isinstance(materialized, dict) else {}
    cluster_nodes = materialized.get("cluster_nodes") if isinstance(materialized, dict) else {}
    instances = desired.get("pb8_instances") if isinstance(desired, dict) else {}
    tombstones = desired.get("pb8_tombstones") if isinstance(desired, dict) else {}
    nodes = cluster_nodes.get("nodes") if isinstance(cluster_nodes, dict) else {}
    return (
        instances if isinstance(instances, dict) else {},
        tombstones if isinstance(tombstones, dict) else {},
        nodes if isinstance(nodes, dict) else {},
    )


def _list_instances() -> list[dict[str, Any]]:
    """Load canonical local PB8 configs and merge desired deployment state."""

    desired, tombstones, nodes = _desired_pb8_state()
    root = _run_root()
    if not root.is_dir() or root.is_symlink():
        return []
    result: list[dict[str, Any]] = []
    with _run_lock():
        for directory in sorted(root.iterdir(), key=lambda item: item.name):
            config_path = directory / "config.json"
            if directory.name.startswith(".") or not directory.is_dir() or directory.is_symlink() or not config_path.is_file() or config_path.is_symlink():
                continue
            try:
                config = load_pb8_config(config_path)
            except Exception as exc:
                _log(SERVICE, f"PB8 loader could not canonicalize live config '{directory.name}': {exc}", level="WARNING")
                try:
                    config = json.loads(config_path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError):
                    continue
                if not isinstance(config, dict):
                    continue
                load_error = str(exc)
            else:
                load_error = ""
            live = config.get("live") if isinstance(config.get("live"), dict) else {}
            pbgui = config.get("pbgui") if isinstance(config.get("pbgui"), dict) else {}
            bot = config.get("bot") if isinstance(config.get("bot"), dict) else {}
            exposure_parts = []
            for side, prefix in (("long", "L"), ("short", "S")):
                side_config = bot.get(side) if isinstance(bot.get(side), dict) else {}
                risk = side_config.get("risk") if isinstance(side_config.get("risk"), dict) else {}
                try:
                    if float(risk.get("n_positions") or 0) > 0:
                        exposure_parts.append(f"{prefix}={round(float(risk.get('total_wallet_exposure_limit') or 0), 2)}")
                except (TypeError, ValueError):
                    pass
            record = desired.get(directory.name) if isinstance(desired.get(directory.name), dict) else {}
            assigned_id = str(record.get("assigned_host") or "")
            node = nodes.get(assigned_id) if isinstance(nodes.get(assigned_id), dict) else {}
            enabled_on = str(pbgui.get("enabled_on") or "disabled")
            if load_error:
                status = "config_error"
            elif directory.name in tombstones:
                status = "tombstoned"
            elif record.get("conflicted") is True:
                status = "conflicted"
            elif not record:
                status = "unpublished"
            elif str(record.get("version") or "") != str(pbgui.get("version") or ""):
                status = "outdated"
            elif str(record.get("desired_state") or "") == "running":
                status = "desired_running"
            else:
                status = "desired_stopped"
            result.append({
                "name": directory.name,
                "user": str(live.get("user") or ""),
                "strategy": str(live.get("strategy_kind") or "").strip(),
                "enabled_on": enabled_on,
                "version": _coerce_version(pbgui.get("version")),
                "note": str(pbgui.get("note") or ""),
                "runtime": str(pbgui.get("runtime") or ""),
                "status": status,
                "desired_status": status,
                "desired_state": str(record.get("desired_state") or ""),
                "assigned_host": assigned_id,
                "assigned_hostname": str(node.get("pbname") or node.get("hostname") or ""),
                "conflicted": record.get("conflicted") is True,
                "load_error": load_error,
                "twe": " | ".join(exposure_parts),
            })
    return _enrich_v8_runtime(result)


def _enrich_v8_runtime(instances: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Merge exact PB8 process observations without inventing runtime state."""
    store = getattr(_monitor, "store", None) if _monitor is not None else None
    runtime_data = getattr(store, "v8_instances", {}) if store is not None else {}
    runtime_data = runtime_data if isinstance(runtime_data, dict) else {}
    observations: dict[str, dict[str, Any]] = {}
    for host, items in runtime_data.items():
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "")
            if not name:
                continue
            info = observations.setdefault(name, {
                "running_on": [], "running_version": 0, "config_version_remote": 0,
                "blocked_on": [], "blocked_reason": "", "pb8_update_required_on": [], "has_data": False,
            })
            info["has_data"] = True
            if item.get("running") is True:
                info["running_on"].append(str(host))
                info["running_version"] = max(info["running_version"], _coerce_version(item.get("rv")))
            if item.get("blocked") is True:
                info["blocked_on"].append(str(host))
                if not info["blocked_reason"]:
                    info["blocked_reason"] = str(item.get("blocked_reason") or "")
                if str(item.get("cluster_gate") or "") == "runtime_not_ready":
                    info["pb8_update_required_on"].append(str(host))
            info["config_version_remote"] = max(
                info["config_version_remote"], _coerce_version(item.get("cv")),
            )

    master_name = _master_hostname()
    try:
        runtime_status = pbgui_purefunc.pb8_runtime_status()
        local_runtime_ready = runtime_status.get("ready") is True
    except Exception:
        runtime_status = {}
        local_runtime_ready = False
    if local_runtime_ready:
        from PBRun import RunV8

        for instance in instances:
            if str(instance.get("enabled_on") or "") != master_name:
                continue
            info = observations.setdefault(instance["name"], {
                "running_on": [], "running_version": 0, "config_version_remote": 0,
                "blocked_on": [], "blocked_reason": "", "pb8_update_required_on": [], "has_data": False,
            })
            info["has_data"] = True
            info["config_version_remote"] = _coerce_version(instance.get("version"))
            runner = RunV8()
            runner.user = instance["name"]
            runner.path = str(_instance_dir(instance["name"]))
            runner.name = master_name
            runner.pb8dir = str(runtime_status.get("pb8dir") or "")
            runner.pb8venv = str(runtime_status.get("pb8venv") or "")
            runner.pbgdir = Path(PBGDIR)
            if runner.is_running():
                info["running_on"].append(master_name)
                info["running_version"] = _coerce_version(instance.get("version"))

    try:
        exchanges = _user_exchange_map()
    except Exception:
        exchanges = {}
    for instance in instances:
        info = observations.get(instance["name"], {})
        running_on = list(dict.fromkeys(info.get("running_on", [])))
        instance["running_on"] = running_on
        instance["running_version"] = _coerce_version(info.get("running_version"))
        instance["config_version_remote"] = _coerce_version(info.get("config_version_remote"))
        instance["blocked_on"] = list(dict.fromkeys(info.get("blocked_on", [])))
        instance["blocked_reason"] = str(info.get("blocked_reason") or "")
        instance["pb8_update_required_on"] = list(dict.fromkeys(info.get("pb8_update_required_on", [])))
        instance["exchange"] = str(exchanges.get(instance.get("user"), "") or "")

        if instance["status"] in {"conflicted", "tombstoned", "config_error"}:
            continue
        enabled_on = str(instance.get("enabled_on") or "disabled")
        version = _coerce_version(instance.get("version"))
        running_version = instance["running_version"]
        if enabled_on == "disabled":
            instance["status"] = "stop_needed" if running_on else "disabled"
        elif instance["blocked_on"] and not running_on:
            instance["status"] = "blocked"
        elif instance.get("desired_status") in {"unpublished", "outdated"}:
            instance["status"] = "outdated" if running_on else "activate_needed"
        elif enabled_on in running_on and version == running_version:
            instance["status"] = "synced"
        elif running_on:
            instance["status"] = "outdated"
        elif not info.get("has_data"):
            instance["status"] = "collecting"
        else:
            instance["status"] = "activate_needed"
    return instances


@router.get("/instances")
def get_v8_instances(session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """List PB8 live instances and their materialized desired state."""

    return {"instances": _list_instances()}


@router.get("/instances/new-config")
def get_new_v8_instance_config(session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Return the installed PB8 template with PBGui live metadata."""

    try:
        config = get_pb8_template_config()
    except PB8ConfigurationError as exc:
        raise _configuration_http_error("Loading the PB8 live template", exc) from exc
    live = config.setdefault("live", {})
    if not isinstance(live, dict):
        config["live"] = {}
    config["pbgui"] = {"runtime": "pb8", "version": 0, "enabled_on": "disabled", "note": ""}
    return {"config": config, "param_status": {}, "override_configs": {}}


@router.get("/editor/metadata")
def get_v8_editor_metadata(session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Return runtime-derived metadata used by the shared Run editor."""
    try:
        optimize_metadata = get_pb8_optimize_metadata()
        template = optimize_metadata["template"]
        bot = template.get("bot") if isinstance(template.get("bot"), dict) else {}
        live = template.get("live") if isinstance(template.get("live"), dict) else {}
        logging_config = template.get("logging") if isinstance(template.get("logging"), dict) else {}
        monitor = template.get("monitor") if isinstance(template.get("monitor"), dict) else {}
        return {
            "contract_version": 1,
            "runtime": pbgui_purefunc.pb8_runtime_status(),
            "strategies": optimize_metadata.get("strategies", []),
            "strategy_specs": optimize_metadata.get("strategy_specs", {}),
            "strategy_defaults": optimize_metadata.get("strategy_defaults", {}),
            "params": {
                "bot": {
                    side: _flatten_leaf_metadata(bot.get(side) if isinstance(bot.get(side), dict) else {})
                    for side in ("long", "short")
                },
                "live": _flatten_leaf_metadata(live),
                "logging": _flatten_leaf_metadata(logging_config),
                "monitor": _flatten_leaf_metadata(monitor),
            },
        }
    except (PB8ConfigurationError, PB8RuntimeBusyError) as exc:
        raise _configuration_http_error("Loading PB8 editor metadata", exc) from exc


@router.post("/editor/prepare")
def prepare_v8_editor_config(body: dict = Body(...), session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Canonicalize a PB8 Run config without writing files or Cluster state."""
    config = body.get("config") if isinstance(body, dict) else None
    if not isinstance(config, dict):
        raise HTTPException(status_code=400, detail="config must be an object")
    try:
        prepared = prepare_pb8_config(copy.deepcopy(config))
    except (PB8ConfigurationError, PB8RuntimeBusyError) as exc:
        raise _configuration_http_error("Preparing PB8 editor config", exc) from exc
    pbgui = prepared.get("pbgui") if isinstance(prepared.get("pbgui"), dict) else {}
    pbgui["runtime"] = "pb8"
    prepared["pbgui"] = pbgui
    return {"config": prepared, "param_status": {}, "valid": True}


@router.post("/draft")
def create_v8_editor_draft(body: dict = Body(...), session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Store a short-lived canonical PB8 editor draft with optional override payloads."""
    config = body.get("config") if isinstance(body, dict) else None
    if not isinstance(config, dict):
        raise HTTPException(status_code=400, detail="config must be an object")
    prepared = prepare_v8_editor_config({"config": config}, session)
    return store_v8_editor_draft(prepared["config"], body.get("override_configs"))


def store_v8_editor_draft(
    config: dict[str, Any],
    submitted_overrides: Any = None,
    *,
    param_status: dict[str, Any] | None = None,
    migration_report: dict[str, Any] | None = None,
    migration_review_values: dict[str, Any] | None = None,
    migration_message: str = "",
) -> dict[str, Any]:
    """Store an already canonical PB8 config without invoking the PB8 helper again."""

    if not isinstance(config, dict):
        raise HTTPException(status_code=400, detail="config must be an object")
    submitted_overrides = submitted_overrides or {}
    if not isinstance(submitted_overrides, dict):
        raise HTTPException(status_code=422, detail="override_configs must be an object")
    references = _referenced_overrides(config)
    override_configs = {}
    for coin, filename in references.items():
        value = submitted_overrides.get(filename, submitted_overrides.get(coin))
        if value is None:
            raise HTTPException(status_code=422, detail=f"Referenced override '{filename}' is missing from the draft")
        if not isinstance(value, dict):
            raise HTTPException(status_code=422, detail=f"Override '{filename}' must be an object")
        override_configs[coin] = copy.deepcopy(value)
    accepted_keys = set(references) | set(references.values())
    extras = set(str(key) for key in submitted_overrides) - accepted_keys
    if extras:
        raise HTTPException(status_code=422, detail=f"Unreferenced override '{sorted(extras)[0]}'")
    payload = {
        "config": copy.deepcopy(config),
        "param_status": copy.deepcopy(param_status or {}),
        "override_configs": override_configs,
    }
    if migration_report:
        payload["migration_report"] = copy.deepcopy(migration_report)
        payload["migration_review_values"] = copy.deepcopy(migration_review_values or {})
        payload["migration_message"] = str(migration_message or "")
    with _draft_lock:
        _clean_drafts()
        draft_id = secrets.token_urlsafe(16)
        _drafts[draft_id] = (time.time(), payload)
    return {"draft_id": draft_id, "expires_in": _DRAFT_TTL_SECONDS}


@router.get("/draft/{draft_id}")
def get_v8_editor_draft(draft_id: str, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Return one unexpired PB8 editor draft."""
    with _draft_lock:
        _clean_drafts()
        entry = _drafts.get(str(draft_id or ""))
        if entry is None:
            raise HTTPException(status_code=404, detail="Draft not found or expired")
        return copy.deepcopy(entry[1])


@router.get("/symbols")
def get_v8_symbols(exchange: str = Query(...), session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Return PB8's collision-safe market identifiers for one exchange."""
    from api.editor_market_data import normalize_exchanges

    exchanges = normalize_exchanges([exchange])
    if not exchanges:
        raise HTTPException(status_code=422, detail="Select a valid exchange")
    try:
        return get_pb8_market_identifiers(exchanges)
    except (PB8ConfigurationError, PB8MarketDataUnavailableError, PB8MarketRequestError) as exc:
        raise _market_data_http_error("PB8 market catalog", exc) from exc


@router.get("/tags")
def get_v8_tags(exchange: str = Query(...), session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Return generation-neutral CoinData tags for the shared editor."""
    from api.editor_market_data import tags
    return {"tags": tags(exchange)}


@router.get("/coins/filter")
def filter_v8_coins(
    exchange: str = Query(...),
    market_cap: int = Query(0),
    vol_mcap: float = Query(10.0),
    only_cpt: bool = Query(False),
    notices_ignore: bool = Query(False),
    tags: str = Query(""),
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """Apply common PBGui filters for a PB8 Run editor preview."""
    from api.editor_market_data import filter_symbols

    approved, ignored = filter_symbols(exchange, market_cap, vol_mcap, only_cpt, notices_ignore, tags)
    try:
        resolved = get_pb8_market_identifiers([exchange])
    except (PB8ConfigurationError, PB8MarketDataUnavailableError, PB8MarketRequestError) as exc:
        raise _market_data_http_error("PB8 filtered market catalog", exc) from exc
    approved_coins = {str(item).upper() for item in approved}
    ignored_coins = {str(item).upper() for item in ignored}
    projected_approved = []
    projected_ignored = []
    matched = set()
    for entry in resolved["catalog"]:
        if not isinstance(entry.get("resolutions"), list) or not entry["resolutions"]:
            continue
        coin = str(entry.get("coin") or "").upper()
        coin_aliases = {coin}
        if ":" in coin:
            namespace, market_name = coin.split(":", 1)
            if namespace and market_name:
                coin_aliases.add(f"{namespace.upper()}-{market_name.upper()}")
        config_id = str(entry.get("config_id") or "")
        if not config_id:
            continue
        approved_match = coin_aliases & approved_coins
        ignored_match = coin_aliases & ignored_coins
        if approved_match:
            projected_approved.append(config_id)
            matched.update(approved_match)
        elif ignored_match:
            projected_ignored.append(config_id)
            matched.update(ignored_match)
    return {
        "approved": projected_approved,
        "ignored": projected_ignored,
        "unresolved": sorted((approved_coins | ignored_coins) - matched),
    }


@router.post("/coins/status")
def get_v8_coin_statuses(body: dict = Body(...), session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Resolve PB8 editor selections through PB8's official market resolver."""
    from api.editor_market_data import normalize_exchanges

    raw_exchanges = body.get("exchanges", [])
    if not isinstance(raw_exchanges, list) or any(
        not isinstance(item, str) or item != item.strip() or not item for item in raw_exchanges
    ):
        raise HTTPException(status_code=422, detail="exchanges must be an array of non-empty trimmed strings")
    exchanges = normalize_exchanges(raw_exchanges)
    raw_coins = body.get("coins", [])
    if not isinstance(raw_coins, list):
        raise HTTPException(status_code=422, detail="coins must be an array")
    if not exchanges:
        raise HTTPException(status_code=422, detail="Select at least one exchange")
    coins = []
    for item in raw_coins:
        if not isinstance(item, str) or item != item.strip() or not item:
            raise HTTPException(status_code=422, detail="coins entries must be non-empty trimmed strings")
        coins.append(item)
    if not coins:
        try:
            return get_pb8_market_identifiers(exchanges)
        except (PB8ConfigurationError, PB8MarketDataUnavailableError, PB8MarketRequestError) as exc:
            raise _market_data_http_error("PB8 market identifier catalog", exc) from exc
    all_values = [coin for coin in coins if coin.lower() == "all"]
    requested = [coin for coin in coins if coin.lower() != "all"]
    try:
        result = get_pb8_market_identifiers(exchanges, requested)
    except (PB8ConfigurationError, PB8MarketDataUnavailableError, PB8MarketRequestError) as exc:
        raise _market_data_http_error("PB8 market identifier status", exc) from exc
    for value in all_values:
        result["statuses"][value] = {
            "input": value,
            "normalized": "all",
            "status": "valid",
            "reason": "all",
            "detail": "",
            "resolutions": [],
            "display": "all",
        }
    return result


@router.get("/override-params")
def get_v8_override_params(
    hsl_signal_mode: str = Query(...),
    strategy_kind: str = Query(...),
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """Return PB8's typed coin-override policy for the effective live context."""
    try:
        return get_pb8_coin_override_metadata(hsl_signal_mode, strategy_kind)
    except (PB8ConfigurationError, PB8RuntimeBusyError) as exc:
        raise _configuration_http_error("Loading PB8 coin override metadata", exc) from exc


@router.get("/override-config/{name}/{filename}")
def get_v8_override_config(name: str, filename: str, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Read one referenced sparse override from a managed PB8 live bundle."""
    name = _validate_name(name)
    filename = _validate_override_filename(filename)
    path = _instance_dir(name) / filename
    with _run_lock():
        return {"config": _read_override_file(path)}


@router.get("/instances/{name}/config")
def get_v8_instance_config(name: str, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Load one canonical PB8 live config through the PB8 loader."""

    path = _config_path(name)
    if _run_root().is_symlink() or path.parent.is_symlink() or not path.is_file() or path.is_symlink():
        raise HTTPException(status_code=404, detail=f"PB8 instance '{name}' not found")
    try:
        with _run_lock():
            config = load_pb8_config(path)
            return {
                "name": name,
                "config": config,
                "param_status": {},
                "override_configs": _override_payloads_by_coin(path.parent, config),
            }
    except PB8ConfigurationError as exc:
        raise _configuration_http_error(f"Loading PB8 instance '{name}'", exc) from exc


@router.put("/instances/{name}/config")
async def save_v8_instance_config(
    name: str,
    body: dict = Body(...),
    create_only: bool = Query(False),
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """Prepare, atomically save, manifest, and publish one PB8 live config."""

    name = _validate_name(name)
    config = body.get("config") if isinstance(body, dict) else None
    if not isinstance(config, dict):
        raise HTTPException(status_code=400, detail="config must be an object")
    candidate = copy.deepcopy(config)
    expected_version = body.get("expected_version") if isinstance(body, dict) else None
    if expected_version is not None:
        try:
            expected_version = int(expected_version)
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail="expected_version must be an integer") from exc
        if expected_version < 0:
            raise HTTPException(status_code=400, detail="expected_version must not be negative")
    live = candidate.get("live")
    if not isinstance(live, dict):
        raise HTTPException(status_code=422, detail="live must be an object")
    user = str(live.get("user") or "").strip()
    if not user or any(ord(char) < 32 for char in user):
        raise HTTPException(status_code=422, detail="live.user is required")
    if name != user:
        raise HTTPException(status_code=422, detail="PB8 instance name must match live.user")
    raw_pbgui = candidate.get("pbgui")
    pbgui = dict(raw_pbgui) if isinstance(raw_pbgui, dict) else {}
    pbgui.pop("from_backup_config", None)
    enabled_on = _validate_target(pbgui.get("enabled_on", "disabled"))
    note = str(pbgui.get("note") or "")
    if len(note) > 2000:
        raise HTTPException(status_code=422, detail="pbgui.note is too long")
    try:
        available_users = _available_users()
    except Exception as exc:
        _log(SERVICE, f"Could not validate PB8 exchange user: {exc}", level="ERROR")
        raise HTTPException(status_code=503, detail="Exchange user catalog is unavailable") from exc
    if user not in {item["name"] for item in available_users}:
        raise HTTPException(status_code=409, detail=f"Exchange user '{user}' is not configured")
    await _ensure_target_compatible(name, enabled_on, candidate.get("config_version"))

    path = _config_path(name)
    with _run_lock():
        if path.is_symlink() or path.parent.is_symlink() or (path.parent.exists() and not path.parent.is_dir()):
            raise HTTPException(status_code=409, detail="PB8 instance path must be a safe directory")
        existed = path.is_file()
        if create_only and existed:
            raise HTTPException(status_code=409, detail=f"PB8 instance '{name}' already exists")
        current_version = _current_version(name)
        if expected_version not in (None, 0) and expected_version != current_version:
            raise HTTPException(
                status_code=409,
                detail=f"PB8 instance '{name}' changed from version {expected_version} to {current_version}. Reload before saving.",
            )
        parent_version = max(_current_version(name), _highest_cluster_version(name))
        pbgui.update({
            "runtime": "pb8",
            "version": parent_version + 1,
            "enabled_on": enabled_on,
            "note": note,
        })
        candidate["pbgui"] = pbgui
        stage_dir = _run_root() / f".pbgui-v8-stage-{uuid.uuid4().hex}"
        try:
            ensure_private_directory_tree(_run_root(), stage_dir)
            override_payloads = _override_payloads_by_filename(
                candidate,
                body.get("override_configs") if isinstance(body, dict) else {},
                path.parent,
            )
            for filename, payload in override_payloads.items():
                atomic_write_private_text(stage_dir / filename, json.dumps(payload, indent=4, allow_nan=False) + "\n")
            prepared = prepare_pb8_config(candidate, base_config_path=str(stage_dir / "config.json"))
            prepared_files = set(_referenced_overrides(prepared).values())
            if prepared_files != set(override_payloads):
                raise HTTPException(status_code=422, detail="PB8 preparation changed override references unexpectedly")
            prepared_pbgui = prepared.get("pbgui") if isinstance(prepared.get("pbgui"), dict) else {}
            prepared_pbgui.update(pbgui)
            prepared["pbgui"] = prepared_pbgui
            saved = save_prepared_pb8_config(prepared, stage_dir / "config.json")
            secure_private_file(stage_dir / "config.json")
            validate_pb8_override_bundle(stage_dir / "config.json")
        except HTTPException:
            shutil.rmtree(stage_dir, ignore_errors=True)
            raise
        except (PB8ConfigurationError, PB8RuntimeBusyError) as exc:
            shutil.rmtree(stage_dir, ignore_errors=True)
            raise _configuration_http_error(f"Preparing PB8 instance '{name}'", exc) from exc
        except Exception as exc:
            shutil.rmtree(stage_dir, ignore_errors=True)
            _log(SERVICE, f"Staging PB8 instance '{name}' failed: {exc}", level="ERROR")
            raise HTTPException(status_code=500, detail="PB8 config could not be staged") from exc
        backup_id = None
        if existed:
            try:
                backup_id = _snapshot_v8_bundle(name, path.parent)
            except HTTPException:
                shutil.rmtree(stage_dir, ignore_errors=True)
                raise
            except Exception as exc:
                shutil.rmtree(stage_dir, ignore_errors=True)
                _log(SERVICE, f"Backing up PB8 instance '{name}' failed: {exc}", level="ERROR")
                raise HTTPException(status_code=500, detail="PB8 config was not saved because its backup failed") from exc
        try:
            operation = _record_upsert(name, stage_dir, saved, parent_version, not existed)
        except HTTPException:
            shutil.rmtree(stage_dir, ignore_errors=True)
            raise
        except Exception as exc:
            shutil.rmtree(stage_dir, ignore_errors=True)
            _log(SERVICE, f"Publishing PB8 instance '{name}' failed: {exc}", level="ERROR")
            raise HTTPException(status_code=500, detail="PB8 config was not published; the previous local config was restored") from exc
        try:
            _publish_staged_bundle(stage_dir, path.parent)
            secure_private_file(path)
        except OSError as exc:
            shutil.rmtree(stage_dir, ignore_errors=True)
            _log(SERVICE, f"PB8 operation {operation['op_id']} published but local config placement failed: {exc}", level="ERROR")
            raise HTTPException(
                status_code=500,
                detail="PB8 desired state was published, but the local config awaits Cluster Sync materialization",
            ) from exc
        try:
            cache_prepared_pb8_config(saved, path)
        except Exception as exc:
            _log(SERVICE, f"PB8 config cache warmup skipped for '{name}': {exc}", level="WARNING")
    activation = await _activate_pb8_target(name, operation)
    _log(SERVICE, f"Saved PB8 live config '{name}' (v{saved['pbgui']['version']})", level="INFO")
    return {
        "ok": True,
        "name": name,
        "version": saved["pbgui"]["version"],
        "config": saved,
        "operation": operation["op"],
        "op_id": operation["op_id"],
        "overrides": sorted(override_payloads),
        "backup_id": backup_id,
        "sync": activation,
    }


@router.get("/instances/{name}/next-version")
def get_v8_instance_next_version(name: str, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Return the authoritative next PB8 live config version."""
    name = _validate_name(name)
    return {"name": name, "next_version": max(_current_version(name), _highest_cluster_version(name)) + 1}


@router.put("/instances/{name}/copy-config")
async def copy_v8_instance_config(
    name: str,
    body: dict = Body(...),
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """Copy a complete PB8 live bundle to another user in disabled state."""
    source_name = _validate_name(name)
    target_user = str(body.get("target_user") or "").strip() if isinstance(body, dict) else ""
    target_name = _validate_name(target_user) if target_user else ""
    config = body.get("config") if isinstance(body, dict) else None
    if not target_user or not isinstance(config, dict):
        raise HTTPException(status_code=400, detail="target_user and config are required")
    submitted_target_name = str(body.get("target_name") or "").strip()
    if submitted_target_name and submitted_target_name != target_user:
        raise HTTPException(status_code=422, detail="target_name must match target_user")
    if target_name == source_name:
        raise HTTPException(status_code=409, detail="Target instance must be different from the source instance")
    source_path = _config_path(source_name)
    if not source_path.is_file() or source_path.is_symlink():
        raise HTTPException(status_code=404, detail=f"PB8 instance '{source_name}' not found")
    copy_config = copy.deepcopy(config)
    copy_live = copy_config.get("live")
    if not isinstance(copy_live, dict):
        raise HTTPException(status_code=422, detail="live must be an object")
    copy_pbgui = copy_config.get("pbgui")
    if copy_pbgui is not None and not isinstance(copy_pbgui, dict):
        raise HTTPException(status_code=422, detail="pbgui must be an object")
    copy_live["user"] = target_user
    copy_config["pbgui"] = dict(copy_pbgui or {})
    copy_config["pbgui"]["enabled_on"] = "disabled"
    source_config = load_pb8_config(source_path)
    source_refs = _referenced_overrides(source_config)
    requested_refs = set(_referenced_overrides(copy_config).values())
    submitted = body.get("override_configs") or {}
    if not isinstance(submitted, dict):
        raise HTTPException(status_code=422, detail="override_configs must be an object")
    override_files = {}
    for filename in sorted(requested_refs):
        if filename in submitted:
            if not isinstance(submitted[filename], dict):
                raise HTTPException(status_code=422, detail=f"Override '{filename}' must be an object")
            override_files[filename] = copy.deepcopy(submitted[filename])
        elif filename in set(source_refs.values()):
            override_files[filename] = _read_override_file(source_path.parent / filename)
        else:
            raise HTTPException(status_code=422, detail=f"Referenced override '{filename}' is missing")
    result = await save_v8_instance_config(
        target_name,
        {"config": copy_config, "override_configs": override_files},
        False,
        session,
    )
    result["source"] = source_name
    return result


@router.delete("/instances/{name}")
def delete_v8_instance(name: str, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Tombstone a PB8 instance and remove its local run bundle."""

    name = _validate_name(name)
    target = _instance_dir(name)
    with _run_lock():
        if not target.is_dir() or target.is_symlink() or not (target / "config.json").is_file():
            raise HTTPException(status_code=404, detail=f"PB8 instance '{name}' not found")
        version = max(_current_version(name), _highest_cluster_version(name))
        try:
            backup_id = _snapshot_v8_bundle(name, target)
        except HTTPException:
            raise
        except Exception as exc:
            _log(SERVICE, f"Backing up PB8 instance '{name}' before delete failed: {exc}", level="ERROR")
            raise HTTPException(status_code=500, detail="PB8 instance was not deleted because its backup failed") from exc
        try:
            operation = _record_delete(name, version)
        except HTTPException:
            raise
        except Exception as exc:
            _log(SERVICE, f"Tombstoning PB8 instance '{name}' failed: {exc}", level="ERROR")
            raise HTTPException(status_code=500, detail="PB8 instance was not deleted because its tombstone could not be published") from exc
        try:
            shutil.rmtree(target)
        except OSError as exc:
            _log(SERVICE, f"PB8 instance '{name}' was tombstoned but local cleanup failed: {exc}", level="ERROR")
            raise HTTPException(status_code=500, detail="PB8 instance was tombstoned, but local cleanup failed") from exc
    _log(SERVICE, f"Deleted PB8 live instance '{name}'", level="INFO")
    return {
        "ok": True,
        "name": name,
        "operation": operation["op"],
        "op_id": operation["op_id"],
        "backup_id": backup_id,
    }


@router.get("/backup-settings")
def get_v8_backup_settings(session: SessionToken = Depends(require_auth)) -> dict[str, int]:
    """Return PB8 live backup retention settings."""
    if not _backup_root().exists():
        return {"max_versions": 50}
    with _backup_lock():
        return _backup_settings_unlocked()


@router.put("/backup-settings")
def put_v8_backup_settings(body: dict = Body(...), session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Atomically update PB8 backup retention and prune existing history."""
    raw = body.get("max_versions") if isinstance(body, dict) else None
    if isinstance(raw, bool):
        raise HTTPException(status_code=400, detail="max_versions must be an integer")
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="max_versions must be an integer") from exc
    if value < 1 or value > 1000:
        raise HTTPException(status_code=400, detail="max_versions must be between 1 and 1000")
    with _backup_lock():
        atomic_write_private_text(
            _backup_root() / "_settings.json",
            json.dumps({"max_versions": value}, indent=4) + "\n",
        )
        for instance_root in _backup_root().iterdir():
            if instance_root.name.startswith("_") or instance_root.is_symlink() or not instance_root.is_dir():
                continue
            _prune_backups_unlocked(instance_root, value)
    _log(SERVICE, f"PB8 backup retention updated to {value}", level="INFO")
    return {"ok": True, "max_versions": value}


@router.get("/backups")
def list_v8_backups(session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """List immutable PB8 live bundle backups grouped by instance."""
    root = _backup_root()
    if not root.is_dir() or root.is_symlink():
        return {"backups": []}
    backups = []
    with _backup_lock():
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
        item["currently_exists"] = _instance_dir(item["name"]).is_dir()
        item["running_on"] = runtime.get(item["name"], {}).get("running_on", [])
        item["can_restore"] = True
    return {"backups": backups}


@router.post("/backups/{name}/{backup_id}/draft")
def create_v8_backup_draft(
    name: str,
    backup_id: str,
    request: Request,
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """Load an exact PB8 backup into the shared editor without changing live state."""
    with _backup_lock():
        backup_dir, config, override_configs = _load_backup_bundle_unlocked(name, backup_id)
    current_path = _config_path(name)
    exists = current_path.is_file() and not current_path.is_symlink() and not current_path.parent.is_symlink()
    pbgui = config.get("pbgui") if isinstance(config.get("pbgui"), dict) else {}
    pbgui = dict(pbgui)
    pbgui["version"] = _current_version(name) if exists else 0
    pbgui["enabled_on"] = str(pbgui.get("enabled_on") or "disabled")
    pbgui["from_backup_config"] = {"name": name, "timestamp": backup_id}
    config["pbgui"] = pbgui
    payload = {"config": config, "param_status": {}, "override_configs": override_configs}
    with _draft_lock:
        _clean_drafts()
        draft_id = secrets.token_urlsafe(16)
        _drafts[draft_id] = (time.time(), payload)
    edit_url = request.url_for("get_v8_edit_page").include_query_params(
        name=name,
        draft_id=draft_id,
        **({"new": "1"} if not exists else {}),
    )
    return {
        "ok": True,
        "name": name,
        "timestamp": backup_id,
        "draft_id": draft_id,
        "version": pbgui["version"],
        "edit_url": str(edit_url),
        "backup_files": sorted(path.name for path in backup_dir.iterdir() if path.is_file() and path.name != "config.json"),
    }


@router.delete("/backups/{name}/{backup_id}")
def delete_v8_backup(name: str, backup_id: str, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Delete one immutable PB8 backup version."""
    name = _validate_name(name)
    backup_id = _validate_backup_id(backup_id)
    with _backup_lock():
        target = _backup_root() / name / backup_id
        if target.is_symlink() or not target.is_dir():
            raise HTTPException(status_code=404, detail=f"PB8 backup '{name}/{backup_id}' not found")
        shutil.rmtree(target)
        parent = target.parent
        if parent.is_dir() and not any(parent.iterdir()):
            parent.rmdir()
    _log(SERVICE, f"Deleted PB8 backup '{name}/{backup_id}'", level="INFO")
    return {"ok": True, "name": name, "timestamp": backup_id}


@router.get("/users")
def get_v8_users(session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """List configured exchange users available to PB8 live configs."""

    try:
        return {"users": _available_users()}
    except Exception as exc:
        _log(SERVICE, f"Could not list PB8 exchange users: {exc}", level="ERROR")
        raise HTTPException(status_code=503, detail="Exchange user catalog is unavailable") from exc


def _available_users() -> list[dict[str, str]]:
    """Return configured exchange users without exposing credential values."""

    from User import Users

    users = Users()
    supported = set(get_pb8_exchange_metadata()["live"])
    return [
        {"name": name, "exchange": users.find_exchange(name) or ""}
        for name in users.list()
        if (users.find_exchange(name) or "") in supported
    ]


@router.get("/hosts")
def get_v8_hosts(
    name: str = Query("", description="Existing instance whose unchanged unknown target may be preserved"),
    config_schema: str = Query("", description="PB8 config schema required by the current editor config"),
    request_id: str = Query(""),
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """List only confirmed PB8-capable targets plus an unchanged unknown legacy target."""

    candidates = {_master_hostname(), *_managed_vps_entries().keys()}
    if _monitor is not None:
        candidates.update(str(item) for item in getattr(_monitor, "enabled_hosts", []) if str(item))
        store = getattr(_monitor, "store", None)
        host_meta = getattr(store, "host_meta", {}) if store is not None else {}
        if isinstance(host_meta, dict):
            candidates.update(str(item) for item in host_meta if str(item))
    required_schema = config_schema.strip() if isinstance(config_schema, str) else ""
    capabilities = {
        host: _with_schema_compatibility(
            {"name": host, **_host_runtime_capability(host)},
            required_schema,
        )
        for host in sorted(candidates)
    }
    hosts = ["disabled"] + [
        host for host in sorted(candidates)
        if capabilities[host]["pb8_capable"] is True
        and (not required_schema or capabilities[host]["schema_compatible"] is True)
    ]
    legacy_target = _persisted_target(name) if name else None
    if legacy_target and legacy_target not in hosts:
        capability = capabilities.get(legacy_target) or _with_schema_compatibility(
            {"name": legacy_target, **_host_runtime_capability(legacy_target)},
            required_schema,
        )
        capabilities[legacy_target] = capability
        if capability["pb8_capable"] is None or (
            capability["pb8_capable"] is True and capability["schema_compatible"] is None
        ):
            hosts.append(legacy_target)
            capability["legacy_preserved"] = True
    return {
        "request_id": request_id,
        "generated_at": time.time(),
        "hosts": hosts,
        "host_capabilities": capabilities,
    }


def _render_page(request: Request, filename: str, replacements: dict[str, Any]) -> HTMLResponse:
    """Render a PB8 standalone page with same-origin API and shared-nav placeholders."""

    path = Path(__file__).parent.parent / "frontend" / filename
    html = path.read_text(encoding="utf-8")
    scheme = request.url.scheme
    host = request.url.hostname or "127.0.0.1"
    port = request.url.port
    origin = f"{scheme}://{host}" + (f":{port}" if port else "")
    values = {
        "API_BASE": origin + "/api/v8",
        "WS_BASE": origin.replace("http://", "ws://").replace("https://", "wss://"),
        "VERSION": pbgui_purefunc.PBGUI_VERSION,
        "SERIAL": pbgui_purefunc.PBGUI_SERIAL,
        "MASTER_NAME": _master_hostname(),
        **replacements,
    }
    for key, value in values.items():
        html = html.replace(f'"%%{key}%%"', json.dumps(value))
        html = html.replace(f"%%{key}%%", str(value))
    nav = Path(__file__).parent.parent / "frontend" / "pbgui_nav.js"
    html = html.replace("%%NAV_HASH%%", str(int(nav.stat().st_mtime)) if nav.exists() else pbgui_purefunc.PBGUI_VERSION)
    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})


@router.get("/main_page", response_class=HTMLResponse)
def get_v8_main_page(request: Request, session: SessionToken = Depends(require_auth)) -> HTMLResponse:
    """Serve the PB8 Run list page."""

    return _render_page(request, "v7_run.html", {"RUN_VERSION": "v8"})


@router.get("/edit_page", response_class=HTMLResponse)
def get_v8_edit_page(
    request: Request,
    name: str = Query(""),
    new: str = Query(""),
    draft_id: str = Query(""),
    session: SessionToken = Depends(require_auth),
) -> HTMLResponse:
    """Serve the shared structured PB7/PB8 Run editor for PB8."""

    if name:
        _validate_name(name)
    return _render_page(request, "v7_edit.html", {
        "INSTANCE": name,
        "IS_NEW": "true" if new == "1" else "false",
        "DRAFT_ID": draft_id,
        "RUN_VERSION": "v8",
    })


@router.websocket("/ws/v8")
async def ws_v8(websocket: WebSocket) -> None:
    """Push PB8 desired-state list updates to authenticated browser sessions."""

    if await authenticate_websocket(websocket) is None:
        return
    try:
        while True:
            instances = await asyncio.to_thread(_list_instances)
            await websocket.send_json({"type": "instances", "data": instances, "generated_at": time.time()})
            try:
                await asyncio.wait_for(websocket.receive_text(), timeout=2.0)
            except asyncio.TimeoutError:
                pass
    except (WebSocketDisconnect, asyncio.CancelledError):
        pass
    except Exception as exc:
        _log(SERVICE, f"PB8 status websocket failed: {exc}", level="WARNING")
