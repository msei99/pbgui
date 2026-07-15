"""
FastAPI router for v7 instance list and cluster materialization state.

Endpoints:
    GET    /instances                   → list all v7 instances with sync status
    POST   /activate-all                → mark instances that need cluster materialization
    DELETE /instances/{name}             → backup + delete instance locally + record tombstone
    GET    /backups                      → list all instance backups
    POST   /backups/{name}/{timestamp}/draft → load backup as editor draft
    POST   /restore/{name}/{timestamp}   → restore/rollback instance from backup
    POST   /instances/{name}/forced-mode → set global PB7 forced mode
    DELETE /backups/{name}/{timestamp}    → delete a specific backup
    GET    /main_page                    → serve the standalone HTML page
"""

from __future__ import annotations

import asyncio
import json
import os
import platform
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import urlencode

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

from api.auth import SessionToken, authenticate_websocket, require_auth, validate_token
from api.pb7_bridge import get_allowed_override_params, get_template_config
from cmc_pool import CmcPoolClient
from credential_store import CredentialStore
from logging_helpers import human_log as _log
from master.cluster_state import (
    append_node_placeholder,
    append_operation,
    build_config_manifest,
    compute_config_manifest_hash,
    default_cluster_root,
    ensure_local_identity,
    generate_node_id,
    load_operations,
    local_cmc_credential_readiness,
    read_local_identity,
    rebuild_materialized_state,
)
from pb7_config import load_pb7_config, prepare_pb7_config_dict, save_pb7_config, strip_pbgui_param_status
import pbgui_purefunc
from pbgui_purefunc import PBGDIR

SERVICE = "V7Instances"
LEGACY_V7_API_SSH_SYNC_DISABLED = True
LEGACY_V7_API_SSH_SYNC_DISABLED_REASON = (
    "Legacy V7 API SSH sync is disabled on cluster-mode; use explicit Cluster Sync materialization."
)

router = APIRouter()

# ── Draft config store (in-memory, TTL-limited) ─────────────
import secrets as _secrets

_draft_configs: dict[str, tuple[float, dict]] = {}  # id → (created_ts, config)
_DRAFT_TTL = 300  # 5 minutes

_CLUSTER_HOST_NODE_IDS_FILE = "host_node_ids.json"



# ── Injected at startup ─────────────────────────────────────

_monitor = None  # VPSMonitor


def init(monitor):
    """Called by PBApiServer lifespan to inject shared objects."""
    global _monitor
    _monitor = monitor


# ── Helpers ──────────────────────────────────────────────────

def _get_master_hostname() -> str:
    """Get the hostname of this master (from pbgui.ini or platform.node())."""
    snapshot = pbgui_purefunc.load_ini_snapshot(Path(PBGDIR) / "pbgui.ini")
    if snapshot.has_option("main", "pbname"):
        return snapshot.get("main", "pbname")
    return platform.node()


def _cluster_root() -> Path:
    """Return the local cluster state root for this PBGui install."""

    return default_cluster_root(Path(PBGDIR))


def _read_cluster_host_node_ids(cluster_root: Path) -> dict:
    """Read host→node_id mappings used until remote nodes provide identities."""

    path = cluster_root / _CLUSTER_HOST_NODE_IDS_FILE
    if not path.is_file():
        return {"schema_version": 1, "hosts": {}}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {"schema_version": 1, "hosts": {}}
    if not isinstance(data, dict):
        return {"schema_version": 1, "hosts": {}}
    hosts = data.get("hosts")
    if not isinstance(hosts, dict):
        hosts = {}
    return {"schema_version": 1, "hosts": hosts}


def _write_cluster_host_node_ids(cluster_root: Path, data: dict) -> None:
    """Atomically write host→node_id mappings."""

    path = cluster_root / _CLUSTER_HOST_NODE_IDS_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=4, sort_keys=True), encoding="utf-8")
    os.replace(tmp, path)


def _cluster_node_known(cluster_root: Path, node_id: str, pending: set[str]) -> bool:
    """Return True when a node is already materialized or queued this call."""

    if node_id in pending:
        return True
    path = cluster_root / "cluster_nodes.json"
    if not path.is_file():
        return False
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return False
    nodes = data.get("nodes") if isinstance(data, dict) else None
    return isinstance(nodes, dict) and node_id in nodes


def _read_cluster_nodes(cluster_root: Path) -> dict[str, dict]:
    """Read materialized cluster nodes keyed by node id."""

    path = cluster_root / "cluster_nodes.json"
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}
    nodes = data.get("nodes") if isinstance(data, dict) else None
    return nodes if isinstance(nodes, dict) else {}


def _best_existing_cluster_node_for_host(cluster_root: Path, hostname: str) -> tuple[str, str] | None:
    """Return the best existing node id/role for a hostname or pbname."""

    host = str(hostname or "").strip()
    if not host:
        return None
    candidates: list[tuple[tuple[int, int, int], str, dict]] = []
    for node_id, node in _read_cluster_nodes(cluster_root).items():
        if not isinstance(node, dict):
            continue
        names = {
            str(node.get("hostname") or "").strip(),
            str(node.get("pbname") or "").strip(),
        }
        if host not in names:
            continue
        score = (
            1 if node.get("enabled") is not False else 0,
            1 if node.get("sync_enabled") is True else 0,
            1 if str(node.get("sync_mode") or "") != "disabled" else 0,
        )
        candidates.append((score, str(node_id), node))
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0], reverse=True)
    _, node_id, node = candidates[0]
    return node_id, str(node.get("role") or "vps")


def _cluster_node_payload(node_id: str, hostname: str, role: str) -> dict:
    """Build a cluster membership payload for a local or known VPS host."""

    payload = {
        "node_id": node_id,
        "role": role,
        "pbname": hostname,
        "hostname": hostname,
        "sync_mode": "outbound_only" if role == "master" else "disabled",
        "sync_enabled": role == "master",
    }
    if role == "vps" and _monitor and _monitor.pool:
        entry = _monitor.pool.get_connection(hostname)
        if entry:
            payload.update({
                "ssh_host": entry.config.ip,
                "ssh_port": entry.config.ssh_port,
                "ssh_user": entry.config.user,
            })
    return payload


def _ensure_cluster_node_record(
    cluster_root: Path,
    node_id: str,
    hostname: str,
    role: str,
    pending: set[str],
) -> None:
    """Add local membership or remote non-replica inventory when unknown."""

    if _cluster_node_known(cluster_root, node_id, pending):
        return
    payload = _cluster_node_payload(node_id, hostname, role)
    identity = read_local_identity(cluster_root)
    if node_id == str(identity.get("node_id") or ""):
        append_operation(cluster_root, "ADD_NODE", payload)
    else:
        append_node_placeholder(cluster_root, payload)
    pending.add(node_id)


def _cluster_node_for_enabled_host(
    cluster_root: Path,
    identity: dict,
    enabled_on: str,
) -> tuple[str, str, str]:
    """Resolve a PBGui enabled_on host name to a stable cluster node id."""

    hostname = str(enabled_on or "").strip()
    master_hostname = _get_master_hostname()
    if not hostname or hostname == "disabled" or hostname == master_hostname:
        return str(identity["node_id"]), master_hostname, "master"

    mapping = _read_cluster_host_node_ids(cluster_root)
    hosts = mapping.setdefault("hosts", {})
    existing = _best_existing_cluster_node_for_host(cluster_root, hostname)
    if existing:
        node_id, role = existing
        entry = hosts.get(hostname)
        if not isinstance(entry, dict) or str(entry.get("node_id") or "") != node_id:
            hosts[hostname] = {
                "node_id": node_id,
                "created_at": int(time.time()),
                "role": role,
            }
            _write_cluster_host_node_ids(cluster_root, mapping)
        return node_id, hostname, role

    entry = hosts.get(hostname)
    if not isinstance(entry, dict):
        entry = {}
    node_id = str(entry.get("node_id") or "")
    if not node_id:
        node_id = generate_node_id()
        hosts[hostname] = {
            "node_id": node_id,
            "created_at": int(time.time()),
            "role": "vps",
        }
        _write_cluster_host_node_ids(cluster_root, mapping)
    return node_id, hostname, "vps"


def _record_cluster_config_upsert(
    name: str,
    instance_dir: Path,
    cfg: dict,
    *,
    parent_version: int | str | None = None,
    allow_tombstone_recreate: bool = False,
) -> None:
    """Record a V7 config write in the local cluster oplog without blocking saves."""

    try:
        cluster_root = _cluster_root()
        identity = ensure_local_identity(
            cluster_root,
            role="master",
            pbname=_get_master_hostname(),
        )
        raw_pbgui = cfg.get("pbgui", {}) if isinstance(cfg, dict) else {}
        pbgui = raw_pbgui if isinstance(raw_pbgui, dict) else {}
        enabled_on = str(pbgui.get("enabled_on") or "disabled").strip() or "disabled"
        version = str(pbgui.get("version", 0))
        desired_state = "running" if enabled_on != "disabled" else "stopped"
        assigned_node_id, assigned_hostname, assigned_role = _cluster_node_for_enabled_host(
            cluster_root,
            identity,
            enabled_on,
        )
        recorded_nodes: set[str] = set()
        _ensure_cluster_node_record(
            cluster_root,
            str(identity["node_id"]),
            _get_master_hostname(),
            "master",
            recorded_nodes,
        )
        _ensure_cluster_node_record(
            cluster_root,
            assigned_node_id,
            assigned_hostname,
            assigned_role,
            recorded_nodes,
        )
        manifest = build_config_manifest(instance_dir)
        payload = {
            "instance": name,
            "version": version,
            "assigned_host": assigned_node_id,
            "desired_state": desired_state,
            "config_manifest_hash": compute_config_manifest_hash(manifest),
            "enabled_on": enabled_on,
        }
        if parent_version is not None:
            payload["parent_version"] = str(parent_version)
        if allow_tombstone_recreate:
            payload["allow_tombstone_recreate"] = True
        append_operation(cluster_root, "UPSERT_CONFIG", payload)
        rebuild_materialized_state(cluster_root)
    except Exception as exc:
        _log(SERVICE, f"Cluster oplog update skipped for V7 config '{name}': {exc}", level="WARNING")


def _record_cluster_instance_delete(name: str, version: int | str | None) -> None:
    """Record a V7 instance delete in the local cluster oplog without blocking deletes."""

    try:
        cluster_root = _cluster_root()
        identity = ensure_local_identity(
            cluster_root,
            role="master",
            pbname=_get_master_hostname(),
        )
        recorded_nodes: set[str] = set()
        _ensure_cluster_node_record(
            cluster_root,
            str(identity["node_id"]),
            _get_master_hostname(),
            "master",
            recorded_nodes,
        )
        append_operation(
            cluster_root,
            "DELETE_INSTANCE",
            {"instance": name, "version": str(version if version is not None else 0)},
        )
        rebuild_materialized_state(cluster_root)
    except Exception as exc:
        _log(SERVICE, f"Cluster oplog delete skipped for V7 instance '{name}': {exc}", level="WARNING")


def _load_local_running_v7() -> dict[str, dict]:
    """Detect locally running v7 instances by checking actual processes.

    Uses the same logic as the SSH INSTANCE_COLLECT_SCRIPT: scan `ps aux`
    for passivbot processes, then match against run_v7 instance dirs.

    Returns: {name: {running: bool, rv: int, cv: int, eo: str}}
    """
    import subprocess as _sp

    run_dir = Path(PBGDIR) / "data" / "run_v7"
    if not run_dir.is_dir():
        return {}

    # Find running passivbot directories from process list
    running_dirs: set[str] = set()
    try:
        out = _sp.check_output(["ps", "aux"], text=True, timeout=5)
        for line in out.splitlines():
            if "main.py" in line and "config_run.json" in line:
                for part in line.split():
                    if part.endswith("/config_run.json"):
                        running_dirs.add(os.path.dirname(part))
    except Exception:
        pass

    result = {}
    for d in run_dir.iterdir():
        if not d.is_dir():
            continue
        name = d.name
        running = str(d) in running_dirs
        if not running:
            continue
        # Read running_version.txt
        rv = 0
        rv_file = d / "running_version.txt"
        if rv_file.is_file():
            try:
                rv = int(rv_file.read_text().strip())
            except (ValueError, OSError):
                pass
        # Read config version + enabled_on
        cv = 0
        eo = ""
        cfg_file = d / "config.json"
        if cfg_file.is_file():
            try:
                cfg = json.loads(cfg_file.read_text(encoding="utf-8"))
                pbgui = cfg.get("pbgui", {})
                cv = pbgui.get("version", 0)
                eo = pbgui.get("enabled_on", "")
            except (json.JSONDecodeError, OSError):
                pass
        result[name] = {"running": True, "rv": rv, "cv": cv, "eo": eo}
    return result


def _load_local_instances() -> list[dict]:
    """Read all v7 instance configs from local disk."""
    run_dir = Path(f"{PBGDIR}/data/run_v7")
    if not run_dir.is_dir():
        return []
    instances = []
    for d in sorted(run_dir.iterdir()):
        cfg_file = d / "config.json"
        if not cfg_file.is_file():
            continue
        try:
            with open(cfg_file, "r", encoding="utf-8") as f:
                cfg = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue
        live = cfg.get("live", {})
        pbgui = cfg.get("pbgui", {})
        bot = cfg.get("bot", {})
        long_cfg = bot.get("long", {})
        short_cfg = bot.get("short", {})

        # TWE string
        l_twe = round(long_cfg.get("total_wallet_exposure_limit", 0), 2)
        l_n = long_cfg.get("n_positions", 0)
        s_twe = round(short_cfg.get("total_wallet_exposure_limit", 0), 2)
        s_n = short_cfg.get("n_positions", 0)
        parts = []
        if l_n > 0:
            parts.append(f"L={l_twe}")
        if s_n > 0:
            parts.append(f"S={s_twe}")
        twe_str = " | ".join(parts)

        instances.append({
            "name": d.name,
            "user": live.get("user", d.name),
            "enabled_on": pbgui.get("enabled_on", "disabled"),
            "version": pbgui.get("version", 0),
            "note": pbgui.get("note", ""),
            "twe": twe_str,
        })
    return instances


def _enrich_with_vps_data(instances: list[dict]) -> list[dict]:
    """Merge VPSMonitor v7_instances data + local PBRun status."""
    store = _monitor.store if _monitor else None
    v7_data = store.v7_instances if store else {}  # {hostname: [{name, running, cv, rv, eo}, ...]}
    # If no VPS host has reported yet, we're still in initial collection phase
    any_vps_data = bool(v7_data)

    # Build lookup: name → best match across all hosts
    # "best" = the host where enabled_on matches, or any running host
    # has_data: True if we received collect data from at least one VPS host
    # for this instance (even if running=False). Used to distinguish
    # "confirmed not running" from "no data yet" — the latter must not
    # show "disabled" when the bot might still be running.
    vps_info = {}  # name → {running_on: [...], rv, cv_remote, has_data, blocked...}
    for host, items in v7_data.items():
        for item in items:
            name = item.get("name", "")
            if name not in vps_info:
                vps_info[name] = {
                    "running_on": [],
                    "rv": 0,
                    "cv_remote": 0,
                    "has_data": False,
                    "blocked_on": [],
                    "blocked_reason": "",
                    "cluster_gate": "",
                }
            vps_info[name]["has_data"] = True
            if item.get("running"):
                vps_info[name]["running_on"].append(host)
                vps_info[name]["rv"] = item.get("rv", 0)
            if item.get("blocked"):
                vps_info[name]["blocked_on"].append(host)
                if not vps_info[name]["blocked_reason"]:
                    vps_info[name]["blocked_reason"] = str(item.get("blocked_reason") or "")
                if not vps_info[name]["cluster_gate"]:
                    vps_info[name]["cluster_gate"] = str(item.get("cluster_gate") or "")
            vps_info[name]["cv_remote"] = max(
                vps_info[name]["cv_remote"], item.get("cv", 0)
            )

    # Include locally running instances (PBRun on this master)
    master_host = _get_master_hostname()
    local_running = _load_local_running_v7()
    for name, info in local_running.items():
        if name not in vps_info:
            vps_info[name] = {"running_on": [], "rv": 0, "cv_remote": 0, "has_data": False}
        vps_info[name]["has_data"] = True
        if master_host not in vps_info[name]["running_on"]:
            vps_info[name]["running_on"].append(master_host)
        vps_info[name]["rv"] = max(vps_info[name]["rv"], info["rv"])
        vps_info[name]["cv_remote"] = max(
            vps_info[name]["cv_remote"], info["cv"]
        )

    for inst in instances:
        name = inst["name"]
        info = vps_info.get(name)
        if info:
            inst["running_on"] = info["running_on"]
            inst["running_version"] = info["rv"]
            inst["config_version_remote"] = info["cv_remote"]
            inst["blocked_on"] = info.get("blocked_on", [])
            inst["blocked_reason"] = info.get("blocked_reason", "")
            inst["cluster_gate"] = info.get("cluster_gate", "")
        else:
            inst["running_on"] = []
            inst["running_version"] = 0
            inst["config_version_remote"] = 0
            inst["blocked_on"] = []
            inst["blocked_reason"] = ""
            inst["cluster_gate"] = ""

        # Compute sync status
        enabled = inst["enabled_on"]
        running_on = inst["running_on"]
        version = inst["version"]
        rv = inst["running_version"]
        has_data = info.get("has_data", False) if info else False
        desired_stopped_block = inst["cluster_gate"] == "desired_stopped"

        if enabled == "disabled":
            if desired_stopped_block:
                inst["blocked_on"] = []
                inst["blocked_reason"] = ""
                inst["cluster_gate"] = ""
            if running_on:
                inst["status"] = "stop_needed"
            else:
                inst["status"] = "disabled"
        elif inst["blocked_on"] and not running_on:
            inst["status"] = "blocked"
        elif not any_vps_data and not running_on:
            # No VPS host has reported yet (server just restarted) — don't guess
            inst["status"] = "collecting"
        elif enabled in running_on and version == rv:
            inst["status"] = "synced"
        elif running_on:
            inst["status"] = "outdated"
        else:
            inst["status"] = "activate_needed"

    return instances


def _parse_schema_version(value: object) -> tuple[int, ...] | None:
    """Parse config schema versions like ``v7.12.0`` for safe comparisons."""
    if not isinstance(value, str):
        return None
    normalized = value.strip().lower()
    if normalized.startswith("v"):
        normalized = normalized[1:]
    parts = normalized.split(".")
    if not parts or any(not part.isdigit() for part in parts):
        return None
    return tuple(int(part) for part in parts)


def _schema_newer_than(left: object, right: object) -> bool:
    """Return True when schema version ``left`` is newer than ``right``."""
    left_parsed = _parse_schema_version(left)
    right_parsed = _parse_schema_version(right)
    if left_parsed is None or right_parsed is None:
        return False
    width = max(len(left_parsed), len(right_parsed))
    left_cmp = left_parsed + (0,) * (width - len(left_parsed))
    right_cmp = right_parsed + (0,) * (width - len(right_parsed))
    return left_cmp > right_cmp


def _local_pb7_config_schema_version() -> str | None:
    """Return the local PB7 config schema version if the PB7 bridge is importable."""
    try:
        schema = get_template_config().get("config_version")
    except Exception:
        return None
    if isinstance(schema, str) and schema.strip():
        return schema.strip()
    return None


def _host_pb7_config_schema_version(hostname: str) -> str | None:
    """Return the last collected PB7 config schema version for a host."""
    if not hostname or hostname == "disabled":
        return None
    if hostname == _get_master_hostname():
        return _local_pb7_config_schema_version()
    store = _monitor.store if _monitor else None
    meta = store.host_meta.get(hostname, {}) if store else {}
    if not isinstance(meta, dict):
        return None
    schema = meta.get("pb7_config_schema")
    if isinstance(schema, str) and schema.strip() and schema.strip().upper() != "N/A":
        return schema.strip()
    return None


def _host_meta_credential_active(hostname: str) -> bool | None:
    """Return confirmed credential capability from monitor host metadata."""
    store = _monitor.store if _monitor else None
    meta = store.host_meta.get(hostname, {}) if store else {}
    if not isinstance(meta, dict):
        return None
    value = meta.get("credential_active")
    if isinstance(value, bool):
        return value
    return None


def _local_credential_capability() -> dict:
    """Return local master CMC capability from the materialized store and pool."""

    result = {
        "credential_protocol_version": 2,
        "credential_active": None,
        "credential_reason": "CMC credential pool status unavailable",
        "cmc_catalog_generation": None,
        "cmc_materialized_generation": None,
        "cmc_active_key_count": None,
    }
    try:
        store = CredentialStore(Path(PBGDIR) / "data" / "credentials")
        records = store.list_cmc(active_only=True)
        status = CmcPoolClient(
            credential_store=store,
            state_root=store.root / "cmc_pool",
            desired_state_provider=lambda: rebuild_materialized_state(
                default_cluster_root(Path(PBGDIR)),
                write=False,
            ),
        ).status()
    except Exception as exc:
        _log(SERVICE, f"Local CMC credential capability unavailable: {exc.__class__.__name__}", level="WARNING")
        return result
    active_count = max(int(status.get("active_credentials") or 0), 0)
    try:
        cluster_root = default_cluster_root(Path(PBGDIR))
        materialized = rebuild_materialized_state(cluster_root, write=False)
        try:
            node_id = str(read_local_identity(cluster_root)["node_id"])
        except Exception:
            nodes = ((materialized.get("cluster_nodes") or {}).get("nodes") or {})
            node_id = str(next(iter(nodes))) if len(nodes) == 1 else ""
        result.update(local_cmc_credential_readiness(materialized, node_id, records))
    except Exception:
        standalone = [
            record for record in records
            if isinstance(record, dict) and record.get("active") and not record.get("pending")
        ]
        generation = max((int(record.get("generation") or 0) for record in standalone), default=0)
        result.update({
            "credential_active": bool(standalone),
            "credential_reason": "CMC credential pool active" if standalone else "No active materialized CMC credentials",
            "cmc_catalog_generation": generation,
            "cmc_materialized_generation": generation,
            "cmc_active_key_count": len(standalone),
            "cluster_origin_metadata": False,
        })
    if result.get("credential_active") is True and active_count < 1:
        result["credential_active"] = False
        result["credential_reason"] = "CMC pool has no exact eligible credential"
    return result


def _host_meta_credential_capability(hostname: str) -> dict:
    """Return whitelisted remote capability metadata from monitor host meta."""

    store = _monitor.store if _monitor else None
    meta = store.host_meta.get(hostname, {}) if store else {}
    result = {
        "credential_protocol_version": None,
        "credential_active": None,
        "credential_reason": "CMC credential capability has not been reported",
        "cmc_catalog_generation": None,
        "cmc_materialized_generation": None,
        "cmc_active_key_count": None,
    }
    if not isinstance(meta, dict):
        return result
    active = meta.get("credential_active")
    result["credential_active"] = active if isinstance(active, bool) else None
    for field in (
        "credential_protocol_version",
        "cmc_catalog_generation",
        "cmc_materialized_generation",
        "cmc_active_key_count",
    ):
        try:
            result[field] = max(int(meta[field]), 0)
        except (KeyError, TypeError, ValueError):
            result[field] = None
    reason = meta.get("credential_reason")
    if isinstance(reason, str) and reason.strip():
        result["credential_reason"] = reason.strip()
    elif result["credential_active"] is False:
        result["credential_reason"] = "No active materialized CMC credentials"
    return result


def _host_credential_capability(hostname: str) -> dict:
    """Return local or remote secret-free CMC capability metadata."""

    clean_host = str(hostname or "").strip()
    if not clean_host or clean_host == "disabled":
        return {
            "credential_protocol_version": None,
            "credential_active": True,
            "credential_reason": "Disabled targets do not require a CMC credential pool",
            "cmc_catalog_generation": None,
            "cmc_materialized_generation": None,
            "cmc_active_key_count": None,
        }
    if clean_host == _get_master_hostname():
        return _local_credential_capability()
    return _host_meta_credential_capability(clean_host)


def _host_credential_active(hostname: str) -> bool | None:
    """Return tri-state credential capability for a target host."""

    return _host_credential_capability(hostname)["credential_active"]


def _host_dropdown_detail(hostname: str) -> dict:
    """Return host metadata used by the PBv7 enabled_on dropdown."""
    clean_host = str(hostname or "").strip()
    capability = _host_credential_capability(clean_host)
    return {
        "name": clean_host,
        **capability,
        "dynamic_ignore_allowed": capability["credential_active"] is True,
    }


async def _refresh_host_schema_if_missing(hostname: str) -> None:
    """Collect host metadata once when the schema field is not yet available."""
    if not hostname or hostname == "disabled" or hostname == _get_master_hostname():
        return
    if _host_pb7_config_schema_version(hostname):
        return
    if _monitor and hasattr(_monitor, "collect_host_meta_now"):
        await _monitor.collect_host_meta_now(hostname, include_package_status=False)


async def _refresh_host_credential_if_missing(hostname: str) -> None:
    """Collect host metadata once when credential capability is unavailable."""
    if not hostname or hostname == "disabled" or hostname == _get_master_hostname():
        return
    if _host_meta_credential_active(hostname) is not None:
        return
    if _monitor and hasattr(_monitor, "collect_host_meta_now"):
        await _monitor.collect_host_meta_now(hostname, include_package_status=False)


async def _target_schema_incompatibility_detail(name: str, cfg: dict) -> str | None:
    """Return a user-facing error when target VPS PB7 cannot load this config."""
    if not isinstance(cfg, dict):
        return None
    config_schema = cfg.get("config_version")
    if not isinstance(config_schema, str) or not config_schema.strip():
        return None
    enabled_on = cfg.get("pbgui", {}).get("enabled_on", "disabled")
    if not isinstance(enabled_on, str) or not enabled_on.strip() or enabled_on == "disabled":
        return None
    enabled_on = enabled_on.strip()
    await _refresh_host_schema_if_missing(enabled_on)
    host_schema = _host_pb7_config_schema_version(enabled_on)
    if host_schema and _schema_newer_than(config_schema, host_schema):
        return (
            f"Update your VPS first: '{name}' uses PB7 config schema {config_schema}, "
            f"but {enabled_on} supports only {host_schema}. Upgrade Passivbot on "
            f"{enabled_on} before saving, syncing, or starting this bot."
        )
    return None


async def _target_dynamic_ignore_incompatibility_detail(name: str, cfg: dict) -> str | None:
    """Return a user-facing error when dynamic_ignore targets a host without CMC."""
    if not isinstance(cfg, dict):
        return None
    pbgui = cfg.get("pbgui") or {}
    if not isinstance(pbgui, dict) or not bool(pbgui.get("dynamic_ignore")):
        return None
    enabled_on = pbgui.get("enabled_on", "disabled")
    if not isinstance(enabled_on, str) or not enabled_on.strip() or enabled_on == "disabled":
        return None
    enabled_on = enabled_on.strip()
    await _refresh_host_credential_if_missing(enabled_on)
    capability = _host_credential_capability(enabled_on)
    credential_active = capability["credential_active"]
    if credential_active is not True:
        status = "has no active" if credential_active is False else "has no confirmed active"
        reason = str(capability.get("credential_reason") or "CMC credential pool unavailable")
        return (
            f"'{name}' uses dynamic_ignore but {enabled_on} {status} CMC credential pool ({reason}). "
            "Activate and materialize the Cluster credential pool on that host before saving, syncing, or starting this bot."
        )
    return None


async def _ensure_target_schema_compatible(name: str, cfg: dict) -> None:
    """Raise 409 when a config targets a VPS with an older PB7 schema."""
    detail = await _target_schema_incompatibility_detail(name, cfg)
    if detail:
        raise HTTPException(status_code=409, detail=detail)


async def _ensure_target_runtime_compatible(name: str, cfg: dict) -> None:
    """Raise 409 when target runtime prerequisites are not configured."""
    await _ensure_target_schema_compatible(name, cfg)
    detail = await _target_dynamic_ignore_incompatibility_detail(name, cfg)
    if detail:
        raise HTTPException(status_code=409, detail=detail)


# ── Cluster Sync Handoff ─────────────────────────────────────

async def _ssh_sync_instance(name: str) -> dict:
    """Return the legacy activation payload without remote SSH writes."""
    config_path = Path(f"{PBGDIR}/data/run_v7/{name}/config.json")
    if not config_path.is_file():
        return {"name": name, "error": f"Config not found: {name}"}
    _log(SERVICE, f"Skipped legacy V7 remote write for '{name}': {LEGACY_V7_API_SSH_SYNC_DISABLED_REASON}")
    return {
        "name": name,
        "local": True,
        "hosts": {},
        "ok": 0,
        "failed": 0,
        "disabled": True,
        "cluster_sync": True,
        "reason": LEGACY_V7_API_SSH_SYNC_DISABLED_REASON,
    }


# ── Endpoints ────────────────────────────────────────────────


def _clean_drafts():
    """Remove expired drafts."""
    now = time.time()
    expired = [k for k, (ts, _) in _draft_configs.items() if now - ts > _DRAFT_TTL]
    for k in expired:
        _draft_configs.pop(k, None)


@router.post("/draft")
def create_draft(
    request_body: dict,
    session: SessionToken = Depends(require_auth),
):
    """Store a config temporarily and return a draft_id.

    Body: { "config": <dict> }
    Returns: { "draft_id": "abc123" }
    """
    _clean_drafts()
    config = request_body.get("config")
    if not isinstance(config, dict):
        raise HTTPException(400, "config must be a JSON object")
    draft_id = _secrets.token_urlsafe(16)
    _draft_configs[draft_id] = (time.time(), config)
    return {"draft_id": draft_id}


@router.get("/draft/{draft_id}")
def get_draft(
    draft_id: str,
    session: SessionToken = Depends(require_auth),
):
    """Retrieve and consume a draft config."""
    _clean_drafts()
    entry = _draft_configs.pop(draft_id, None)
    if not entry:
        raise HTTPException(404, "Draft not found or expired")
    try:
        cfg = prepare_pb7_config_dict(entry[1], neutralize_added=True)
    except Exception as exc:
        raise HTTPException(400, f"Invalid draft config: {exc}") from exc
    override_configs = {}
    backup_info = cfg.get("pbgui", {}).get("from_backup_config")
    if isinstance(backup_info, dict):
        backup_name = str(backup_info.get("name") or "").strip()
        backup_ts = str(backup_info.get("timestamp") or "").strip()
        try:
            _validate_name(backup_name)
            _validate_name(backup_ts)
            backup_dir = Path(PBGDIR) / "data" / "backup" / "v7" / backup_name / backup_ts
            for coin, override in (cfg.get("coin_overrides") or {}).items():
                if not isinstance(override, dict) or not override.get("override_config_path"):
                    continue
                filename = Path(str(override["override_config_path"])).name
                src_file = backup_dir / filename
                if not src_file.is_file():
                    continue
                data = load_pb7_config(src_file, neutralize_added=False)
                override_configs[str(coin)] = {"bot": data.get("bot", {})}
        except Exception as exc:
            _log(SERVICE, f"Failed to load backup override configs for draft {draft_id}: {exc}", level="WARNING")
    param_status = cfg.pop("_pbgui_param_status", {})
    return {"config": cfg, "param_status": param_status, "override_configs": override_configs}


@router.get("/instances")
def get_instances(session: SessionToken = Depends(require_auth)):
    """List all v7 instances with sync status from VPS data."""
    instances = _load_local_instances()
    instances = _enrich_with_vps_data(instances)
    return {"instances": instances}


@router.get("/instances/new-config")
def get_new_instance_config(session: SessionToken = Depends(require_auth)):
    """Return a default config for a new instance, pulled from the passivbot schema.

    Using get_template_config() keeps the defaults always in sync with the
    installed passivbot version without any manual maintenance.
    """
    try:
        tmpl = get_template_config()
    except Exception:
        # Fallback: minimal safe defaults if pb7 import fails
        tmpl = {
            "live": {},
            "bot": {"long": {"n_positions": 10, "total_wallet_exposure_limit": 1.25}, "short": {"n_positions": 10, "total_wallet_exposure_limit": 0}},
            "logging": {"level": 1},
            "backtest": {},
            "optimize": {},
        }
    # Inject pbgui-specific metadata (not part of passivbot schema)
    tmpl["pbgui"] = {
        "version": 0,
        "enabled_on": "disabled",
        "note": "",
        "market_cap": 0,
        "vol_mcap": 10.0,
        "tags": [],
        "only_cpt": False,
        "notices_ignore": False,
        "dynamic_ignore": False,
        "starting_config": False,
    }
    tmpl.setdefault("coin_overrides", {})
    return {"config": tmpl}


async def activate_instance(
    name: str,
    session: SessionToken = Depends(require_auth),
):
    """Update local status metadata and return the cluster handoff result."""
    config_path = Path(f"{PBGDIR}/data/run_v7/{name}/config.json")
    if not config_path.is_file():
        raise HTTPException(status_code=404, detail=f"Instance '{name}' not found")
    result = await _ssh_sync_instance(name)
    return result


@router.post("/activate-all")
async def activate_all(session: SessionToken = Depends(require_auth)):
    """Mark all locally outdated instances for cluster materialization."""
    instances = _load_local_instances()
    instances = _enrich_with_vps_data(instances)

    to_activate = [
        inst for inst in instances
        if inst["status"] in ("outdated", "activate_needed", "stop_needed")
    ]

    if not to_activate:
        return {"activated": 0, "results": [], "message": "All instances in sync"}

    results = []
    for inst in to_activate:
        r = await _ssh_sync_instance(inst["name"])
        results.append(r)

    ok = sum(1 for r in results if r.get("ok", 0) > 0)
    return {"activated": len(to_activate), "ok": ok, "results": results}


@router.post("/instances/{name}/forced-mode")
async def set_instance_forced_mode(
    name: str,
    body: dict = Body(...),
    session: SessionToken = Depends(require_auth),
):
    """Set global PB7 forced mode for all long and short positions."""
    _validate_name(name)
    requested = str(body.get("mode") or "").strip().lower()
    if requested == "panic":
        mode = "p"
        label = "panic"
    elif requested == "graceful_stop":
        mode = "graceful_stop"
        label = "graceful stop"
    elif requested in {"tp_only", "take_profit_only"}:
        mode = "tp_only"
        label = "take profit only"
    else:
        raise HTTPException(status_code=400, detail="mode must be panic, graceful_stop or tp_only")

    instance_dir = Path(PBGDIR) / "data" / "run_v7" / name
    config_path = instance_dir / "config.json"
    if not config_path.is_file():
        raise HTTPException(status_code=404, detail=f"Instance '{name}' not found")

    try:
        cfg = load_pb7_config(config_path, neutralize_added=False)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Could not read config for '{name}': {exc}") from exc

    try:
        old_version = int(cfg.get("pbgui", {}).get("version", 0) or 0)
    except (TypeError, ValueError):
        old_version = 0
    try:
        backup_dir = Path(PBGDIR) / "data" / "backup" / "v7" / name / str(old_version)
        if not backup_dir.exists():
            backup_dir.mkdir(parents=True, exist_ok=True)
            for item in _iter_backup_json_files(instance_dir):
                shutil.copy2(str(item), str(backup_dir / item.name))
    except Exception as exc:
        _log(SERVICE, f"Failed to backup '{name}' before forced-mode change: {exc}", level="WARNING")

    history_version = _highest_cluster_instance_version(name)
    version_base = max(old_version, history_version)
    live = cfg.setdefault("live", {})
    live["forced_mode_long"] = mode
    live["forced_mode_short"] = mode
    cfg.setdefault("pbgui", {})["version"] = version_base + 1

    try:
        save_pb7_config(cfg, config_path)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Could not save config for '{name}': {exc}") from exc

    _record_cluster_config_upsert(name, instance_dir, cfg, parent_version=version_base)
    sync_result = await _ssh_sync_instance(name)
    _log(SERVICE, f"Set forced mode '{label}' for all positions on '{name}' (v{cfg['pbgui']['version']})", level="WARNING")
    return {
        "ok": True,
        "name": name,
        "mode": requested,
        "forced_mode": mode,
        "version": cfg["pbgui"]["version"],
        "sync": sync_result,
    }


@router.delete("/instances/{name}")
async def delete_instance(
    name: str,
    session: SessionToken = Depends(require_auth),
):
    """Delete a v7 instance locally and record the cluster tombstone."""
    # Sanitise name — must be a plain directory name, no path traversal
    if not name or "/" in name or "\\" in name or name in (".", ".."):
        raise HTTPException(status_code=400, detail="Invalid instance name")

    instance_dir = Path(PBGDIR) / "data" / "run_v7" / name
    if not instance_dir.is_dir():
        raise HTTPException(status_code=404, detail=f"Instance '{name}' not found")
    deleted_version = _instance_config_version(name)

    # Check if running on any VPS or locally
    instances = _load_local_instances()
    instances = _enrich_with_vps_data(instances)
    inst = next((i for i in instances if i["name"] == name), None)
    if inst and inst.get("running_on"):
        hosts = ", ".join(inst["running_on"])
        raise HTTPException(
            status_code=409,
            detail=f"Instance '{name}' is running on {hosts} — stop it first",
        )

    # 1) Backup locally before delete
    backup_ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    backup_dir = Path(PBGDIR) / "data" / "backup" / "v7" / name / backup_ts
    try:
        backup_dir.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(instance_dir, backup_dir)
        _log(SERVICE, f"Backed up '{name}' → {backup_dir}")
    except OSError as e:
        _log(SERVICE, f"Backup failed for '{name}': {e}", level="WARNING")
        # Continue with delete even if backup fails — log the warning

    # 2) Delete locally
    try:
        shutil.rmtree(instance_dir)
    except OSError as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete locally: {e}")

    _log(SERVICE, f"Deleted instance '{name}' locally")

    # 3) Record the Cluster tombstone; PBRun polls Cluster/run_v7 state locally.
    _record_cluster_instance_delete(name, deleted_version)

    return {
        "ok": True,
        "name": name,
        "hosts": {},
        "cluster_sync": True,
        "reason": LEGACY_V7_API_SSH_SYNC_DISABLED_REASON,
    }


# ── Backup / Restore ────────────────────────────────────────

def _validate_name(name: str) -> None:
    """Raise 400 if name contains path traversal characters."""
    if not name or "/" in name or "\\" in name or name in (".", ".."):
        raise HTTPException(status_code=400, detail="Invalid name")


def _iter_backup_json_files(instance_dir: Path) -> list[Path]:
    """Return syncable JSON config files for backup/restore."""
    return sorted(
        [
            item for item in instance_dir.iterdir()
            if item.is_file() and item.suffix == ".json" and item.name not in (
                "config.json.tmp", "ignored_coins.json", "approved_coins.json",
                "config_run.json",
            )
        ],
        key=lambda item: item.name,
    )


def _next_backup_dir(name: str, suffix: str = "") -> Path:
    """Create a unique backup dir path for ad-hoc backups."""
    backup_root = Path(PBGDIR) / "data" / "backup" / "v7" / name
    stamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    candidate = f"{stamp}{suffix}"
    backup_dir = backup_root / candidate
    counter = 2
    while backup_dir.exists():
        backup_dir = backup_root / f"{candidate}_{counter}"
        counter += 1
    return backup_dir


def _load_backup_payload(backup_dir: Path) -> list[tuple[str, bytes]]:
    """Read all restoreable config files from a backup directory."""
    payload = []
    for item in _iter_backup_json_files(backup_dir):
        try:
            payload.append((item.name, item.read_bytes()))
        except OSError as exc:
            raise HTTPException(status_code=500, detail=f"Failed to read backup file '{item.name}': {exc}") from exc
    if not payload:
        raise HTTPException(status_code=500, detail=f"Backup '{backup_dir.name}' contains no restoreable config files")
    return payload


def _write_restore_payload(target_dir: Path, payload: list[tuple[str, bytes]]) -> None:
    """Atomically write restored config files into an instance dir."""
    target_dir.mkdir(parents=True, exist_ok=True)
    for filename, content in payload:
        tmp_path = target_dir / f"{filename}.restore_tmp"
        with open(tmp_path, "wb") as f:
            f.write(content)
        os.replace(tmp_path, target_dir / filename)


def _bump_restored_instance_version(target_dir: Path, previous_version: int) -> int | None:
    """Keep restore content but advance config version so activation detects it."""
    config_path = target_dir / "config.json"
    if not config_path.is_file():
        return None
    try:
        cfg = load_pb7_config(config_path, neutralize_added=False)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Restore failed: invalid restored config.json: {exc}") from exc

    cfg.setdefault("pbgui", {})["version"] = previous_version + 1
    try:
        save_pb7_config(cfg, config_path)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Restore failed: could not write updated config version: {exc}") from exc
    return cfg["pbgui"]["version"]


def _instance_config_version(name: str) -> int:
    """Return the current local instance config version, or 0 when absent/unreadable."""
    config_path = Path(PBGDIR) / "data" / "run_v7" / name / "config.json"
    if not config_path.is_file():
        return 0
    try:
        current = json.loads(config_path.read_text(encoding="utf-8"))
        return int(current.get("pbgui", {}).get("version", 0) or 0)
    except (json.JSONDecodeError, OSError, TypeError, ValueError):
        return 0


def _coerce_config_version(value: object) -> int:
    """Return a numeric config version, or 0 for non-numeric history values."""

    try:
        return max(0, int(str(value).strip()))
    except (TypeError, ValueError):
        return 0


def _highest_cluster_instance_version(name: str) -> int:
    """Return the highest recorded cluster version for an instance name."""

    try:
        root = _cluster_root()
        identity = read_local_identity(root)
        operations = load_operations(root, expected_cluster_id=str(identity["cluster_id"]))
    except Exception:
        return 0
    highest = 0
    for operation in operations:
        if str(operation.get("instance") or "") != name:
            continue
        if str(operation.get("op") or "") not in {"UPSERT_CONFIG", "MOVE_INSTANCE", "DELETE_INSTANCE", "TOMBSTONE_INSTANCE"}:
            continue
        highest = max(
            highest,
            _coerce_config_version(operation.get("version")),
            _coerce_config_version(operation.get("parent_version")),
        )
    return highest


def _highest_backup_version(name: str) -> int:
    """Return the highest numeric backup version for an instance."""
    backup_root = Path(PBGDIR) / "data" / "backup" / "v7" / name
    if not backup_root.is_dir():
        return 0
    highest = 0
    for entry in backup_root.iterdir():
        if not entry.is_dir():
            continue
        try:
            highest = max(highest, int(entry.name))
        except ValueError:
            continue
    return highest


def _next_instance_config_version(name: str) -> int:
    """Return the next config version after local state and Cluster history."""
    return max(_instance_config_version(name), _highest_cluster_instance_version(name)) + 1


def _backup_config_payload(name: str, timestamp: str) -> tuple[Path, dict]:
    """Return backup directory and parsed config.json for a backup."""
    backup_dir = Path(PBGDIR) / "data" / "backup" / "v7" / name / timestamp
    if not backup_dir.is_dir():
        raise HTTPException(
            status_code=404,
            detail=f"Backup '{name}/{timestamp}' not found",
        )
    config_path = backup_dir / "config.json"
    if not config_path.is_file():
        raise HTTPException(status_code=500, detail=f"Backup '{name}/{timestamp}' has no config.json")
    try:
        return backup_dir, json.loads(config_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        raise HTTPException(status_code=500, detail=f"Restore failed: invalid backup config.json: {exc}") from exc


@router.get("/backups")
def list_backups(session: SessionToken = Depends(require_auth)):
    """List all v7 instance backups grouped by instance name."""
    backup_root = Path(PBGDIR) / "data" / "backup" / "v7"
    if not backup_root.is_dir():
        return {"backups": []}
    instance_status = {
        inst["name"]: inst for inst in _enrich_with_vps_data(_load_local_instances())
    }
    result = []
    for inst_dir in sorted(backup_root.iterdir()):
        if not inst_dir.is_dir():
            continue
        backup_items = []
        for backup_dir in inst_dir.iterdir():
            if not backup_dir.is_dir():
                continue
            if not (backup_dir / "config.json").is_file():
                continue
            try:
                created_ts = backup_dir.stat().st_mtime
            except OSError:
                continue
            backup_items.append({
                "id": backup_dir.name,
                "created_at": datetime.fromtimestamp(created_ts).strftime("%Y-%m-%d %H:%M:%S"),
                "created_ts": created_ts,
            })
        backup_items.sort(key=lambda item: item["created_ts"], reverse=True)
        if backup_items:
            exists = (Path(PBGDIR) / "data" / "run_v7" / inst_dir.name).is_dir()
            running_on = instance_status.get(inst_dir.name, {}).get("running_on", [])
            result.append({
                "name": inst_dir.name,
                "timestamps": [item["id"] for item in backup_items],
                "backup_items": backup_items,
                "currently_exists": exists,
                "running_on": running_on,
                "can_restore": True,
            })
    return {"backups": result}


@router.post("/restore/{name}/{timestamp}")
async def restore_instance(
    name: str,
    timestamp: str,
    session: SessionToken = Depends(require_auth),
):
    """Restore a v7 instance from backup, or rollback an existing stopped one."""
    _validate_name(name)
    _validate_name(timestamp)

    backup_dir = Path(PBGDIR) / "data" / "backup" / "v7" / name / timestamp
    if not backup_dir.is_dir():
        raise HTTPException(
            status_code=404,
            detail=f"Backup '{name}/{timestamp}' not found",
        )

    restore_payload = _load_backup_payload(backup_dir)
    restore_config = None
    for filename, content in restore_payload:
        if filename == "config.json":
            try:
                restore_config = json.loads(content.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise HTTPException(
                    status_code=500,
                    detail=f"Restore failed: invalid backup config.json: {exc}",
                ) from exc
            break
    if isinstance(restore_config, dict):
        await _ensure_target_runtime_compatible(name, restore_config)

    instance_dir = Path(PBGDIR) / "data" / "run_v7" / name
    rollback = instance_dir.is_dir()
    previous_version = max(_highest_backup_version(name), _highest_cluster_instance_version(name))
    if rollback:
        instances = _enrich_with_vps_data(_load_local_instances())
        inst = next((item for item in instances if item["name"] == name), None)
        if inst and inst.get("running_on"):
            hosts = ", ".join(inst["running_on"])
            raise HTTPException(
                status_code=409,
                detail=f"Instance '{name}' is running on {hosts} — load the backup in the editor and save it explicitly",
            )

    # Create a safety snapshot before overwriting an existing instance.
    try:
        if rollback:
            config_path = instance_dir / "config.json"
            if config_path.is_file():
                try:
                    current_cfg = json.loads(config_path.read_text(encoding="utf-8"))
                    previous_version = max(
                        previous_version,
                        int(current_cfg.get("pbgui", {}).get("version", 0) or 0),
                    )
                except (json.JSONDecodeError, OSError, ValueError):
                    pass
            pre_restore_dir = _next_backup_dir(name, "_pre-restore")
            pre_restore_dir.mkdir(parents=True, exist_ok=True)
            for item in _iter_backup_json_files(instance_dir):
                shutil.copy2(str(item), str(pre_restore_dir / item.name))
            for item in _iter_backup_json_files(instance_dir):
                item.unlink(missing_ok=True)
        _write_restore_payload(instance_dir, restore_payload)
    except OSError as e:
        raise HTTPException(status_code=500, detail=f"Restore failed: {e}")

    restored_version = _bump_restored_instance_version(instance_dir, previous_version)
    if restored_version is not None:
        try:
            restored_cfg = load_pb7_config(instance_dir / "config.json", neutralize_added=False)
            _record_cluster_config_upsert(
                name,
                instance_dir,
                restored_cfg,
                parent_version=previous_version,
                allow_tombstone_recreate=not rollback,
            )
        except Exception as exc:
            _log(SERVICE, f"Cluster oplog update skipped for restored V7 config '{name}': {exc}", level="WARNING")

    _log(
        SERVICE,
        f"{'Rolled back' if rollback else 'Restored'} '{name}' from backup {timestamp}"
        + (f" as version {restored_version}" if restored_version is not None else ""),
    )

    # Return a legacy activation payload; PBCluster owns remote materialization.
    result = await _ssh_sync_instance(name)
    return {
        "ok": True,
        "name": name,
        "timestamp": timestamp,
        "rollback": rollback,
        "version": restored_version,
        "activate": result,
    }


@router.get("/backup-settings")
def get_backup_settings(session: SessionToken = Depends(require_auth)):
    """Return current backup retention settings."""
    settings_file = Path(PBGDIR) / "data" / "backup" / "v7" / "_settings.json"
    max_versions = 50
    if settings_file.exists():
        try:
            with open(settings_file, "r", encoding="utf-8") as f:
                settings = json.load(f)
            max_versions = max(int(settings.get("max_versions", 50)), 1)
        except (json.JSONDecodeError, OSError, ValueError):
            pass
    return {"max_versions": max_versions}


@router.put("/backup-settings")
def put_backup_settings(
    body: dict = Body(...),
    session: SessionToken = Depends(require_auth),
):
    """Update backup retention settings."""
    raw = body.get("max_versions")
    if raw is None:
        raise HTTPException(status_code=400, detail="max_versions required")
    try:
        val = max(int(raw), 1)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="max_versions must be an integer")
    settings_file = Path(PBGDIR) / "data" / "backup" / "v7" / "_settings.json"
    settings_file.parent.mkdir(parents=True, exist_ok=True)
    # Read existing settings to preserve other fields
    settings = {}
    if settings_file.exists():
        try:
            with open(settings_file, "r", encoding="utf-8") as f:
                settings = json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    settings["max_versions"] = val
    tmp = settings_file.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(settings, f, indent=4)
    tmp.rename(settings_file)
    _log(SERVICE, f"Backup retention updated to {val}")
    return {"ok": True, "max_versions": val}


@router.post("/backups/{name}/{timestamp}/draft")
def create_backup_draft(
    name: str,
    timestamp: str,
    request: Request,
    session: SessionToken = Depends(require_auth),
):
    """Load a backup into a temporary editor draft without writing or syncing it."""
    _validate_name(name)
    _validate_name(timestamp)
    backup_dir, cfg = _backup_config_payload(name, timestamp)
    next_version = max(_instance_config_version(name), int(cfg.get("pbgui", {}).get("version", 0) or 0)) + 1
    cfg.setdefault("pbgui", {})["version"] = next_version
    cfg["pbgui"]["from_backup_config"] = {"name": name, "timestamp": timestamp}
    _clean_drafts()
    draft_id = _secrets.token_urlsafe(16)
    _draft_configs[draft_id] = (time.time(), cfg)

    params = {
        "token": session.token,
        "name": name,
        "draft_id": draft_id,
    }
    edit_url = str(request.url_for("get_edit_page")) + "?" + urlencode(params)
    return {
        "ok": True,
        "name": name,
        "timestamp": timestamp,
        "draft_id": draft_id,
        "version": next_version,
        "edit_url": edit_url,
        "backup_files": [item.name for item in _iter_backup_json_files(backup_dir) if item.name != "config.json"],
    }


@router.delete("/backups/{name}/{timestamp}")
def delete_backup(
    name: str,
    timestamp: str,
    session: SessionToken = Depends(require_auth),
):
    """Delete a specific backup."""
    _validate_name(name)
    _validate_name(timestamp)

    backup_dir = Path(PBGDIR) / "data" / "backup" / "v7" / name / timestamp
    if not backup_dir.is_dir():
        raise HTTPException(
            status_code=404,
            detail=f"Backup '{name}/{timestamp}' not found",
        )

    try:
        shutil.rmtree(backup_dir)
    except OSError as e:
        raise HTTPException(status_code=500, detail=f"Delete failed: {e}")

    # Clean up parent dir if empty
    parent = backup_dir.parent
    if parent.is_dir() and not any(parent.iterdir()):
        parent.rmdir()

    _log(SERVICE, f"Deleted backup '{name}/{timestamp}'")
    return {"ok": True, "name": name, "timestamp": timestamp}


# ── Instance Config (Edit) ──────────────────────────────────

@router.get("/instances/{name}/config")
def get_instance_config(
    name: str,
    session: SessionToken = Depends(require_auth),
):
    """Load the full config.json for a v7 instance via pb7_config pipeline.

    Uses passivbot's own normalize/migrate/hydrate pipeline so renamed and
    new parameters are properly handled.  Parameters that are newly added
    by the pipeline (not present in the file) are neutralized to safe
    feature-off values and flagged in _pbgui_param_status for UI display.
    """
    _validate_name(name)
    config_path = Path(PBGDIR) / "data" / "run_v7" / name / "config.json"
    if not config_path.is_file():
        raise HTTPException(status_code=404, detail=f"Instance '{name}' not found")
    cfg = load_pb7_config(config_path, neutralize_added=True)
    param_status = cfg.pop("_pbgui_param_status", {})
    return {"name": name, "config": cfg, "param_status": param_status}


@router.get("/instances/{name}/next-version")
def get_instance_next_version(
    name: str,
    session: SessionToken = Depends(require_auth),
):
    """Return the next pbgui.version for an existing or pending v7 instance."""
    _validate_name(name)
    return {"name": name, "next_version": _next_instance_config_version(name)}


@router.put("/instances/{name}/config")
async def save_instance_config(
    name: str,
    request: Request,
    session: SessionToken = Depends(require_auth),
):
    """Save config.json for a v7 instance via pb7_config pipeline.

    Applies the same legacy instance-save logic as the previous editor:
      - Strips _pbgui_param_status before writing
      - Increments pbgui.version
      - Sets backtest.exchange from user→exchange mapping
      - Creates versioned backup before overwriting
      - Atomic write via temp-file rename
      - Records PBCluster desired state for remote materialization
    """
    body = await request.json()
    cfg = body.get("config")
    if not isinstance(cfg, dict):
        raise HTTPException(status_code=400, detail="Missing or invalid 'config' in body")
    return await _save_instance_config_payload(name, cfg)


async def _save_instance_config_payload(
    name: str,
    cfg: dict,
    override_source_name: str | None = None,
) -> dict:
    """Save a prepared v7 config and optionally copy referenced override files."""
    _validate_name(name)

    strip_pbgui_param_status(cfg)
    await _ensure_target_runtime_compatible(name, cfg)

    instance_dir = Path(PBGDIR) / "data" / "run_v7" / name
    instance_dir.mkdir(parents=True, exist_ok=True)
    config_path = instance_dir / "config.json"
    is_new_instance = not config_path.is_file()
    previous_version = _instance_config_version(name)
    history_version = _highest_cluster_instance_version(name)
    version_base = max(previous_version, history_version)
    override_copy = {"copied": [], "missing": []}

    # Copy coin override files from source backtest config on first save of a new instance.
    # The JS sets pbgui.from_backtest_config when navigating from "Add to Run".
    # Only copy files that are actually referenced in coin_overrides to avoid stale USDT-named files.
    pbgui_meta = cfg.setdefault("pbgui", {})
    from_bt_config = pbgui_meta.pop("from_backtest_config", None)
    from_backup_config = pbgui_meta.pop("from_backup_config", None)
    is_backup_draft = isinstance(from_backup_config, dict)
    if from_bt_config and not config_path.is_file():
        bt_src_dir = Path(PBGDIR) / "data" / "bt_v7" / from_bt_config
        if bt_src_dir.is_dir():
            referenced = {
                Path(ov["override_config_path"]).name
                for ov in cfg.get("coin_overrides", {}).values()
                if ov.get("override_config_path")
            }
            copied = []
            for fname in referenced:
                src_file = bt_src_dir / fname
                if src_file.is_file():
                    try:
                        normalized_override = load_pb7_config(src_file, neutralize_added=False)
                        save_pb7_config(normalized_override, instance_dir / fname)
                    except Exception:
                        shutil.copy2(str(src_file), str(instance_dir / fname))
                    copied.append(fname)
            if copied:
                _log(SERVICE, f"Copied {len(copied)} coin override file(s) from backtest config '{from_bt_config}' to instance '{name}'")

    backup_src_dir = None
    if isinstance(from_backup_config, dict):
        backup_name = str(from_backup_config.get("name") or "").strip()
        backup_ts = str(from_backup_config.get("timestamp") or "").strip()
        if backup_name == name and backup_ts:
            try:
                _validate_name(backup_name)
                _validate_name(backup_ts)
                candidate = Path(PBGDIR) / "data" / "backup" / "v7" / backup_name / backup_ts
                if candidate.is_dir():
                    backup_src_dir = candidate
            except HTTPException:
                backup_src_dir = None

    # The submitted version is display-only. Always write exactly the next local/cluster version.
    cfg["pbgui"]["version"] = version_base + 1

    # Set backtest.exchange from user→exchange mapping
    live_user = cfg.get("live", {}).get("user", "")
    from User import Users
    users = Users()
    exchange = users.find_exchange(live_user)
    if exchange:
        bt_exchange = exchange
        if bt_exchange in ("bitget", "okx", "hyperliquid"):
            bt_exchange = "binance"
        cfg.setdefault("backtest", {})["exchange"] = bt_exchange
    cfg.setdefault("backtest", {})["base_dir"] = f"backtests/pbgui/{live_user}"

    # Versioned backup before overwriting to preserve the previous config version.
    if config_path.is_file():
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                old_cfg = json.load(f)
            old_version = old_cfg.get("pbgui", {}).get("version", 0)
            data_dir = instance_dir.parent.parent
            backup_dir = data_dir / "backup" / "v7" / name / str(old_version)
            if not backup_dir.exists():
                backup_dir.mkdir(parents=True, exist_ok=True)
                for item in instance_dir.iterdir():
                    if item.suffix == ".json" and item.name not in (
                        "config.json.tmp", "ignored_coins.json",
                        "approved_coins.json", "config_run.json"
                    ):
                        shutil.copy2(str(item), str(backup_dir / item.name))
        except Exception:
            pass  # backup failure must never block the save

    if backup_src_dir is not None:
        copied = []
        for item in _iter_backup_json_files(backup_src_dir):
            if item.name == "config.json":
                continue
            shutil.copy2(str(item), str(instance_dir / item.name))
            copied.append(item.name)
        if copied:
            _log(SERVICE, f"Copied {len(copied)} coin override file(s) from backup '{name}/{backup_src_dir.name}' before saving")

    if override_source_name:
        override_copy = _copy_referenced_instance_override_files(override_source_name, name, instance_dir, cfg)
        if override_copy["copied"]:
            _log(
                SERVICE,
                f"Copied {len(override_copy['copied'])} coin override file(s) from instance '{override_source_name}' to '{name}'",
            )

    save_pb7_config(cfg, config_path)

    # Keep referenced per-coin override files on the current PB7 schema even when
    # the instance save did not edit them directly.
    referenced_override_files = {
        Path(ov["override_config_path"]).name
        for ov in cfg.get("coin_overrides", {}).values()
        if isinstance(ov, dict) and ov.get("override_config_path")
    }
    for fname in referenced_override_files:
        override_path = instance_dir / fname
        if not override_path.is_file():
            continue
        try:
            normalized_override = load_pb7_config(override_path, neutralize_added=False)
            save_pb7_config(normalized_override, override_path)
        except Exception as exc:
            _log(
                SERVICE,
                f"Failed to normalize override config '{fname}' for instance '{name}': {exc}",
                level="WARNING",
            )

    version = cfg["pbgui"]["version"]

    _record_cluster_config_upsert(
        name,
        instance_dir,
        cfg,
        parent_version=version_base,
        allow_tombstone_recreate=backup_src_dir is not None or is_new_instance,
    )

    # Return a legacy sync payload; PBCluster owns remote materialization.
    sync_result = await _ssh_sync_instance(name)

    _log(SERVICE, f"Saved config for '{name}' (v{version})")
    result = {
        "ok": True,
        "name": name,
        "version": version,
        "sync": sync_result,
    }
    if override_source_name:
        result["override_copy"] = override_copy
    return result


def _referenced_override_filenames(cfg: dict) -> list[str]:
    """Return sanitized per-coin override filenames referenced by a config."""
    filenames: set[str] = set()
    for override in (cfg.get("coin_overrides") or {}).values():
        if not isinstance(override, dict) or not override.get("override_config_path"):
            continue
        filename = Path(str(override["override_config_path"])).name
        if filename and filename.endswith(".json"):
            filenames.add(filename)
    return sorted(filenames)


def _copy_referenced_instance_override_files(
    source_name: str,
    target_name: str,
    target_dir: Path,
    cfg: dict,
) -> dict[str, list[str]]:
    """Copy referenced override files from one run instance directory to another."""
    if source_name == target_name:
        return {"copied": [], "missing": []}
    _validate_name(source_name)
    source_dir = Path(PBGDIR) / "data" / "run_v7" / source_name
    copied: list[str] = []
    missing: list[str] = []
    for filename in _referenced_override_filenames(cfg):
        src_file = source_dir / filename
        if not src_file.is_file():
            missing.append(filename)
            continue
        try:
            normalized_override = load_pb7_config(src_file, neutralize_added=False)
            save_pb7_config(normalized_override, target_dir / filename)
        except Exception:
            shutil.copy2(str(src_file), str(target_dir / filename))
        copied.append(filename)
    if missing:
        _log(
            SERVICE,
            f"Missing {len(missing)} referenced coin override file(s) while copying instance '{source_name}' to '{target_name}': {', '.join(missing)}",
            level="WARNING",
        )
    return {"copied": copied, "missing": missing}


@router.put("/instances/{source_name}/copy-config")
async def copy_instance_config(
    source_name: str,
    request: Request,
    session: SessionToken = Depends(require_auth),
):
    """Copy the current editor config to another v7 user instance."""
    _validate_name(source_name)
    source_config_path = Path(PBGDIR) / "data" / "run_v7" / source_name / "config.json"
    if not source_config_path.is_file():
        raise HTTPException(status_code=404, detail=f"Source instance '{source_name}' not found")
    body = await request.json()
    target_user = str(body.get("target_user") or "").strip()
    if not target_user:
        raise HTTPException(status_code=400, detail="Missing target_user")
    _validate_name(target_user)
    if target_user == source_name:
        raise HTTPException(status_code=400, detail="Target user must be different from the source instance")
    cfg = body.get("config")
    if not isinstance(cfg, dict):
        raise HTTPException(status_code=400, detail="Missing or invalid 'config' in body")
    if not isinstance(cfg.get("live"), dict):
        cfg["live"] = {}
    cfg["live"]["user"] = target_user
    if not isinstance(cfg.get("pbgui"), dict):
        cfg["pbgui"] = {}
    cfg["pbgui"]["enabled_on"] = "disabled"
    target_config_path = Path(PBGDIR) / "data" / "run_v7" / target_user / "config.json"
    if target_config_path.is_file():
        cfg["pbgui"]["version"] = _instance_config_version(target_user)
    result = await _save_instance_config_payload(target_user, cfg, override_source_name=source_name)
    result["source"] = source_name
    return result


@router.get("/users")
def get_users(session: SessionToken = Depends(require_auth)):
    """List all v7-compatible users with their exchanges."""
    from User import Users
    users = Users()
    result = []
    for name in users.list_v7():
        exchange = users.find_exchange(name)
        result.append({"name": name, "exchange": exchange or ""})
    return {"users": result}


@router.get("/hosts")
def get_hosts(
    request_id: str = "",
    session: SessionToken = Depends(require_auth),
):
    """List available hosts for the 'enabled_on' dropdown."""
    del session
    master = _get_master_hostname()
    hosts = ["disabled", master]
    if _monitor and _monitor.pool:
        for h in sorted(_monitor.enabled_hosts):
            if h != master and h not in hosts:
                hosts.append(h)
    return {
        "request_id": request_id,
        "generated_at": time.time(),
        "hosts": hosts,
        "host_details": [_host_dropdown_detail(host) for host in hosts],
    }


def _normalize_exchange_list(values) -> list[str]:
    if isinstance(values, str):
        items = values.split(",")
    elif isinstance(values, list):
        items = values
    else:
        items = []

    exchanges: list[str] = []
    seen: set[str] = set()
    for item in items:
        exchange = str(item or "").strip().lower()
        if not exchange or exchange == "combined" or exchange in seen:
            continue
        seen.add(exchange)
        exchanges.append(exchange)
    return exchanges


def _classify_coins_for_exchanges(exchanges: list[str], coins: list[str]) -> dict[str, dict]:
    from PBCoinData import CoinData, build_symbol_mappings, normalize_symbol

    if not exchanges or not coins:
        return {}

    cd = CoinData()
    active_coins: set[str] = set()
    symbol_mappings: dict[str, str] = {}

    for exchange in exchanges:
        try:
            approved_active, ignored_active = cd.filter_mapping(
                exchange=exchange,
                market_cap_min_m=0,
                vol_mcap_max=float("inf"),
                only_cpt=False,
                notices_ignore=False,
                tags=[],
                quote_filter=None,
                active_only=True,
                use_cache=True,
            )
            active_coins.update(approved_active)
            active_coins.update(ignored_active)

            mapping = cd.load_mapping(exchange=exchange, use_cache=True)
            raw_symbols = [
                str(record.get("symbol") or "").strip().upper()
                for record in mapping
                if str(record.get("symbol") or "").strip()
            ]
            symbol_mappings.update(build_symbol_mappings(raw_symbols))
        except Exception as exc:
            _log(SERVICE, f"Failed to classify coins for exchange {exchange}: {exc}", level="WARNING")

    statuses: dict[str, dict] = {}
    for raw_coin in coins:
        value = str(raw_coin or "").strip()
        if not value:
            continue
        if value.lower() == "all":
            statuses[value] = {"input": value, "normalized": "all", "status": "valid"}
            continue

        normalized = str(normalize_symbol(value.upper(), symbol_mappings) or value).upper()
        status = "valid" if normalized in active_coins else "invalid"

        statuses[value] = {
            "input": value,
            "normalized": normalized,
            "status": status,
        }
    return statuses


@router.get("/symbols")
def get_symbols(
    exchange: str = Query(..., description="Exchange ID (e.g. 'binance')"),
    session: SessionToken = Depends(require_auth),
):
    """Return normalized base coin names for a given exchange (active USDT linear perps).

    Uses the same CoinData.filter_mapping() call as the UI so that
    normalization logic (multiplier prefixes, quote suffixes) stays in one place
    and cannot diverge between code paths.
    """
    from PBCoinData import CoinData
    cd = CoinData()
    approved, ignored = cd.filter_mapping(
        exchange=exchange,
        market_cap_min_m=0,
        vol_mcap_max=float("inf"),
        only_cpt=False,
        notices_ignore=False,
        tags=[],
        quote_filter=None,
        active_only=True,
        use_cache=True,
    )
    # Return all active coins (approved + ignored by filter, but present on exchange)
    symbols = sorted(set(approved) | set(ignored))
    return {"symbols": symbols}


@router.get("/tags")
def get_tags(
    exchange: str = Query(..., description="Exchange ID"),
    session: SessionToken = Depends(require_auth),
):
    """Return available filter tags for a given exchange."""
    from PBCoinData import CoinData
    cd = CoinData()
    tags = cd.get_mapping_tags(exchange=exchange, use_cache=True)
    return {"tags": tags}


@router.get("/coins/filter")
def filter_coins(
    exchange: str = Query(...),
    market_cap: int = Query(0),
    vol_mcap: float = Query(10.0),
    only_cpt: bool = Query(False),
    notices_ignore: bool = Query(False),
    tags: str = Query("", description="Comma-separated tags"),
    session: SessionToken = Depends(require_auth),
):
    """Preview dynamic-ignore filter results."""
    from PBCoinData import CoinData
    cd = CoinData()
    tag_list = [t.strip() for t in tags.split(",") if t.strip()] if tags else []
    approved, ignored = cd.filter_mapping(
        exchange=exchange,
        market_cap_min_m=market_cap,
        vol_mcap_max=vol_mcap,
        only_cpt=only_cpt,
        notices_ignore=notices_ignore,
        tags=tag_list,
        quote_filter=None,
        use_cache=True,
    )
    return {"approved": approved, "ignored": ignored}


@router.post("/coins/status")
def get_coin_statuses(
    body: dict = Body(...),
    session: SessionToken = Depends(require_auth),
):
    """Resolve selected coins to CoinData short names and active/invalid status."""
    exchanges = _normalize_exchange_list(body.get("exchanges", []))
    coins_raw = body.get("coins", [])
    if isinstance(coins_raw, str):
        coins = [c.strip() for c in coins_raw.split(",") if c.strip()]
    elif isinstance(coins_raw, list):
        coins = [str(c).strip() for c in coins_raw if str(c).strip()]
    else:
        coins = []

    statuses = _classify_coins_for_exchanges(exchanges, coins)
    return {"exchanges": exchanges, "statuses": statuses}


@router.get("/log/{name}")
def get_instance_log(
    name: str,
    lines: int = Query(500, ge=1, le=10000),
    session: SessionToken = Depends(require_auth),
):
    """Read the passivbot.log for an instance (tail N lines)."""
    _validate_name(name)
    log_path = Path(PBGDIR) / "data" / "run_v7" / name / "passivbot.log"
    if not log_path.is_file():
        return {"name": name, "log": ""}
    try:
        with open(log_path, "r", encoding="utf-8") as f:
            all_lines = f.readlines()
        tail = all_lines[-lines:] if len(all_lines) > lines else all_lines
        return {"name": name, "log": "".join(reversed(tail))}
    except OSError:
        return {"name": name, "log": ""}


@router.get("/last-active-host/{name}")
def get_last_active_host(
    name: str,
    session: SessionToken = Depends(require_auth),
):
    """Find the last VPS host where a bot was active by scanning backups.

    Scans data/backup/v7/{name}/*/config.json in reverse order (newest first)
    looking for enabled_on != 'disabled'.
    Returns {name, host, version} or {name, host: ''} if none found.
    """
    _validate_name(name)
    master = _get_master_hostname()
    backup_root = Path(PBGDIR) / "data" / "backup" / "v7" / name
    if not backup_root.is_dir():
        return {"name": name, "host": "", "master": master}
    # Sort backup dirs by mtime descending (newest first)
    dirs = sorted(
        [d for d in backup_root.iterdir() if d.is_dir()],
        key=lambda d: d.stat().st_mtime,
        reverse=True,
    )
    for d in dirs:
        cfg_file = d / "config.json"
        if not cfg_file.is_file():
            continue
        try:
            with open(cfg_file, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            host = cfg.get("pbgui", {}).get("enabled_on", "disabled") or "disabled"
            if host != "disabled":
                return {"name": name, "host": host, "version": d.name, "master": master}
        except (json.JSONDecodeError, OSError):
            continue
    return {"name": name, "host": "", "master": master}


@router.get("/log-smart/{name}")
def get_instance_log_smart(
    name: str,
    lines: int = Query(500, ge=1, le=10000),
    session: SessionToken = Depends(require_auth),
):
    """Return passivbot.log with smart fallback.

    - enabled_on != 'disabled': read live log from data/run_v7/{name}/passivbot.log
    - disabled: find most recent backup in data/backup/v7/{name}/ that contains passivbot.log
    Returns {name, log, source, source_label} where source is 'live' or 'backup:{timestamp}'.
    """
    _validate_name(name)

    # Determine enabled_on from saved config
    cfg_path = Path(PBGDIR) / "data" / "run_v7" / name / "config.json"
    enabled_on = "disabled"
    if cfg_path.is_file():
        try:
            with open(cfg_path, "r", encoding="utf-8") as f:
                cfg_data = json.load(f)
            enabled_on = cfg_data.get("pbgui", {}).get("enabled_on", "disabled") or "disabled"
        except (json.JSONDecodeError, OSError):
            pass

    def _read_log(log_path: Path) -> str:
        try:
            with open(log_path, "r", encoding="utf-8") as f:
                all_lines = f.readlines()
            tail = all_lines[-lines:] if len(all_lines) > lines else all_lines
            return "".join(reversed(tail))
        except OSError:
            return ""

    # Try live log first (always preferred if the file exists and bot is not disabled)
    live_log_path = Path(PBGDIR) / "data" / "run_v7" / name / "passivbot.log"
    if enabled_on != "disabled" and live_log_path.is_file():
        return {
            "name": name,
            "log": _read_log(live_log_path),
            "source": "live",
            "source_label": f"live ({enabled_on})",
        }

    # Fallback: most recent backup with passivbot.log
    backup_root = Path(PBGDIR) / "data" / "backup" / "v7" / name
    if backup_root.is_dir():
        timestamps = sorted(
            [d.name for d in backup_root.iterdir() if d.is_dir()],
            reverse=True,
        )
        for ts in timestamps:
            backup_log = backup_root / ts / "passivbot.log"
            if backup_log.is_file():
                return {
                    "name": name,
                    "log": _read_log(backup_log),
                    "source": f"backup:{ts}",
                    "source_label": f"last active (backup {ts})",
                }

    # No log found at all — try live path as last resort even if disabled
    if live_log_path.is_file():
        return {
            "name": name,
            "log": _read_log(live_log_path),
            "source": "live",
            "source_label": "local log",
        }

    return {"name": name, "log": "", "source": "none", "source_label": "no log found"}


@router.get("/override-params")
def get_override_params(session: SessionToken = Depends(require_auth)):
    """Return allowed coin_overrides parameters from passivbot (used by coin_overrides_editor.js)."""
    try:
        return {"params": get_allowed_override_params()}
    except Exception as exc:
        _log(SERVICE, f"Failed to load override params: {exc}", level="warning")
        return {"params": {}}


@router.get("/override-config/{name}/{filename}")
def get_instance_override_config(
    name: str,
    filename: str,
    session: SessionToken = Depends(require_auth),
):
    """Read a per-coin override config file — used by coin_overrides_editor.js.

    Mirrors the backtest /override-config endpoint so the shared JS module
    can be used without modification.  Returns ``{"config": {"bot": {...}}}``.
    """
    _validate_name(name)
    if "/" in filename or "\\" in filename or ".." in filename:
        raise HTTPException(status_code=400, detail="Invalid filename")
    coin_file = Path(PBGDIR) / "data" / "run_v7" / name / filename
    if not coin_file.is_file():
        raise HTTPException(status_code=404, detail=f"Override config '{filename}' not found")
    try:
        data = load_pb7_config(coin_file, neutralize_added=False)
        return {"config": {"bot": data.get("bot", {})}}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Error reading override config: {exc}")


@router.put("/override-config/{name}/{filename}")
async def save_instance_override_config(
    name: str,
    filename: str,
    request: Request,
    session: SessionToken = Depends(require_auth),
):
    """Save a per-coin override config file — used by coin_overrides_editor.js.

    Mirrors the backtest /override-config endpoint.  Body is
    ``{"bot": {"long": {...}, "short": {...}}}``.
    """
    _validate_name(name)
    if "/" in filename or "\\" in filename or ".." in filename:
        raise HTTPException(status_code=400, detail="Invalid filename")
    if not filename.endswith(".json"):
        raise HTTPException(status_code=400, detail="Filename must end with .json")
    body = await request.json()
    coin_dir = Path(PBGDIR) / "data" / "run_v7" / name
    coin_dir.mkdir(parents=True, exist_ok=True)
    coin_file = coin_dir / filename
    full: dict = {}
    if coin_file.is_file():
        try:
            full = load_pb7_config(coin_file, neutralize_added=False)
        except Exception:
            full = {}
    full["bot"] = body.get("bot", {})
    try:
        save_pb7_config(full, coin_file)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Error saving override config: {exc}") from exc
    return {"ok": True}


@router.get("/instances/{name}/coin-config/{symbol}")
def get_coin_config(
    name: str,
    symbol: str,
    session: SessionToken = Depends(require_auth),
):
    """Load per-coin override config (bot section) from {symbol}.json."""
    _validate_name(name)
    config_dir = Path(PBGDIR) / "data" / "run_v7" / name
    config_file = config_dir / f"{symbol}.json"
    if config_file.is_file():
        try:
            with open(config_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            return {"bot": data.get("bot", {})}
        except (json.JSONDecodeError, OSError):
            return {"bot": {}}
    return {"bot": {}}


@router.put("/instances/{name}/coin-config/{symbol}")
def save_coin_config(
    name: str,
    symbol: str,
    body: dict = Body(...),
    session: SessionToken = Depends(require_auth),
):
    """Save per-coin override config (bot section) to {symbol}.json."""
    _validate_name(name)
    config_dir = Path(PBGDIR) / "data" / "run_v7" / name
    config_file = config_dir / f"{symbol}.json"
    # Load existing to preserve non-bot sections
    full = {}
    if config_file.is_file():
        try:
            with open(config_file, "r", encoding="utf-8") as f:
                full = json.load(f)
        except (json.JSONDecodeError, OSError):
            full = {}
    full["bot"] = body.get("bot", {})
    tmp = config_file.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(full, f, indent=4)
    tmp.rename(config_file)
    return {"ok": True}


@router.delete("/instances/{name}/coin-config/{symbol}")
def delete_coin_config(
    name: str,
    symbol: str,
    session: SessionToken = Depends(require_auth),
):
    """Delete per-coin override config file."""
    _validate_name(name)
    config_dir = Path(PBGDIR) / "data" / "run_v7" / name
    config_file = config_dir / f"{symbol}.json"
    config_file.unlink(missing_ok=True)
    return {"ok": True}


@router.get("/main_page", response_class=HTMLResponse)
def get_main_page(
    request: Request,
    session: SessionToken = Depends(require_auth),
) -> HTMLResponse:
    """Serve the standalone v7 Run page."""
    html_path = Path(__file__).parent.parent / "frontend" / "v7_run.html"
    html = html_path.read_text(encoding="utf-8")

    scheme = request.url.scheme
    host = request.url.hostname or "127.0.0.1"
    port = request.url.port
    origin = f"{scheme}://{host}" + (f":{port}" if port else "")
    api_base = origin + "/api/v7"
    ws_base = origin.replace("http://", "ws://").replace("https://", "wss://")

    html = html.replace('"%%TOKEN%%"', json.dumps(session.token))
    html = html.replace('"%%API_BASE%%"', json.dumps(api_base))
    html = html.replace('"%%WS_BASE%%"', json.dumps(ws_base))

    from pbgui_purefunc import PBGUI_VERSION
    from pbgui_purefunc import PBGUI_SERIAL
    html = html.replace('"%%VERSION%%"', json.dumps(PBGUI_VERSION))
    html = html.replace("%%VERSION%%", PBGUI_VERSION)
    html = html.replace('"%%SERIAL%%"', json.dumps(PBGUI_SERIAL))
    html = html.replace("%%SERIAL%%", PBGUI_SERIAL)

    nav_js = Path(__file__).parent.parent / "frontend" / "pbgui_nav.js"
    nav_hash = str(int(nav_js.stat().st_mtime)) if nav_js.exists() else PBGUI_VERSION
    html = html.replace("%%NAV_HASH%%", nav_hash)

    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})


@router.get("/edit_page", response_class=HTMLResponse)
def get_edit_page(
    request: Request,
    name: str = Query(default="", description="Instance name to edit"),
    new: str = Query(default="", description="Set to '1' for new instance"),
    draft_id: str = Query(default="", description="Draft config ID to pre-load"),
    session: SessionToken = Depends(require_auth),
) -> HTMLResponse:
    """Serve the standalone v7 Edit page."""
    html_path = Path(__file__).parent.parent / "frontend" / "v7_edit.html"
    html = html_path.read_text(encoding="utf-8")

    scheme = request.url.scheme
    host = request.url.hostname or "127.0.0.1"
    port = request.url.port
    origin = f"{scheme}://{host}" + (f":{port}" if port else "")
    api_base = origin + "/api/v7"
    ws_base = origin.replace("http://", "ws://").replace("https://", "wss://")

    html = html.replace('"%%TOKEN%%"', json.dumps(session.token))
    html = html.replace('"%%API_BASE%%"', json.dumps(api_base))
    html = html.replace('"%%WS_BASE%%"', json.dumps(ws_base))

    is_new = "true" if new == "1" else "false"
    html = html.replace('"%%INSTANCE%%"', json.dumps(name))
    html = html.replace('"%%IS_NEW%%"', json.dumps(is_new))
    html = html.replace('"%%DRAFT_ID%%"', json.dumps(draft_id))

    from pbgui_purefunc import PBGUI_VERSION
    from pbgui_purefunc import PBGUI_SERIAL
    html = html.replace('"%%VERSION%%"', json.dumps(PBGUI_VERSION))
    html = html.replace("%%VERSION%%", PBGUI_VERSION)
    html = html.replace('"%%SERIAL%%"', json.dumps(PBGUI_SERIAL))
    html = html.replace("%%SERIAL%%", PBGUI_SERIAL)

    nav_js = Path(__file__).parent.parent / "frontend" / "pbgui_nav.js"
    nav_hash = str(int(nav_js.stat().st_mtime)) if nav_js.exists() else PBGUI_VERSION
    html = html.replace("%%NAV_HASH%%", nav_hash)

    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})


# ── WebSocket ────────────────────────────────────────────────

V7_WS_PUSH_INTERVAL = 1.0  # throttle: max 1 push/sec

_ws_clients: set[WebSocket] = set()


@router.websocket("/ws/v7")
async def ws_v7(websocket: WebSocket):
    """WebSocket for real-time v7 instance updates.

    Authentication: HttpOnly session cookie.
    Push: ``{"type": "instances", "data": [...]}`` on every store change.
    """
    if await authenticate_websocket(websocket) is None:
        return
    _ws_clients.add(websocket)
    _log(SERVICE, f"[ws] v7 client connected: {websocket.client}")

    push_task = asyncio.create_task(
        _v7_push_loop(websocket), name="v7-ws-push")

    try:
        while True:
            # Keep connection alive; ignore client messages
            await websocket.receive_text()
    except (WebSocketDisconnect, asyncio.CancelledError):
        pass
    except Exception as e:
        _log(SERVICE, f"[ws] v7 client error: {e}", level="WARNING")
    finally:
        _ws_clients.discard(websocket)
        push_task.cancel()
        await asyncio.gather(push_task, return_exceptions=True)
        _log(SERVICE, f"[ws] v7 client disconnected: {websocket.client}")


async def _v7_push_loop(ws: WebSocket):
    """Push v7 instance state whenever the VPS store changes."""
    try:
        # Send initial state immediately
        await _send_v7_state(ws)
        while True:
            if _monitor and _monitor.store:
                _monitor.store.changed.clear()
                await _monitor.store.changed.wait()
                await asyncio.sleep(V7_WS_PUSH_INTERVAL)
                await _send_v7_state(ws)
            else:
                await asyncio.sleep(V7_WS_PUSH_INTERVAL)
    except (asyncio.CancelledError, WebSocketDisconnect):
        pass
    except Exception as e:
        _log(SERVICE, f"[ws] v7 push error: {e}", level="WARNING")


async def _send_v7_state(ws: WebSocket):
    """Build and push the v7 instance list."""
    instances = _load_local_instances()
    instances = _enrich_with_vps_data(instances)
    await ws.send_json({"type": "instances", "data": instances})
