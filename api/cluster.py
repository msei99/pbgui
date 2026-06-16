"""FastAPI router for read-only Cluster Sync status."""

from __future__ import annotations

import asyncio
import base64
import configparser
import hashlib
import json
import platform
import shlex
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse

from api.auth import SessionToken, require_auth
from api.vps import get_monitor, get_monitor_state_snapshot
from logging_helpers import human_log as _log
from master.async_pool import remote_shell_path
from master.cluster_state import (
    append_operation,
    build_config_manifest,
    ClusterStateError,
    compute_config_manifest_hash,
    default_cluster_root,
    ensure_local_identity,
    generate_node_id,
    load_operations,
    normalize_node_sync_mode,
    rebuild_materialized_state,
)
from pb7_config import load_pb7_config
from pbgui_purefunc import PBGDIR

SERVICE = "Cluster"

router = APIRouter()

_REMOTE_PUSH_JOBS: dict[str, dict[str, Any]] = {}
_REMOTE_PUSH_JOB_TTL_SECONDS = 3600
_REMOTE_PUSH_ACTIVE_STATES = frozenset({"queued", "running"})
_CONFIG_BLOB_BATCH_TARGET_BYTES = 12 * 1024 * 1024
_EDITABLE_NODE_SYNC_MODES = frozenset({"disabled", "outbound_only", "reachable"})


def _cluster_root() -> Path:
    """Return the local cluster state root for this PBGui install."""

    return default_cluster_root(Path(PBGDIR))


def _prune_remote_push_jobs() -> None:
    """Forget stale remote-push progress records."""

    cutoff = int(time.time()) - _REMOTE_PUSH_JOB_TTL_SECONDS
    for job_id, job in list(_REMOTE_PUSH_JOBS.items()):
        if int(job.get("updated_at") or 0) < cutoff:
            _REMOTE_PUSH_JOBS.pop(job_id, None)


def _public_remote_push_job(job: dict[str, Any]) -> dict[str, Any]:
    """Return a JSON-safe view of one remote-push progress record."""

    return {
        "job_id": str(job.get("job_id") or ""),
        "node_id": str(job.get("node_id") or ""),
        "hostname": str(job.get("hostname") or ""),
        "status": str(job.get("status") or "queued"),
        "phase": str(job.get("phase") or "queued"),
        "done": int(job.get("done") or 0),
        "total": int(job.get("total") or 0),
        "remaining": int(job.get("remaining") or 0),
        "created_at": int(job.get("created_at") or 0),
        "updated_at": int(job.get("updated_at") or 0),
        "error": str(job.get("error") or ""),
        "result": job.get("result") if isinstance(job.get("result"), dict) else None,
    }


def _find_active_remote_push_job(node_id: str) -> dict[str, Any] | None:
    """Return an active remote-push job for one node, if present."""

    _prune_remote_push_jobs()
    for job in _REMOTE_PUSH_JOBS.values():
        if str(job.get("node_id") or "") == node_id and str(job.get("status") or "") in _REMOTE_PUSH_ACTIVE_STATES:
            return job
    return None


def _create_remote_push_job(node: dict[str, Any]) -> dict[str, Any]:
    """Create a new local progress record for one remote-push job."""

    node_id = str(node.get("node_id") or "")
    active = _find_active_remote_push_job(node_id)
    if active:
        raise HTTPException(status_code=409, detail="Remote operation push is already running for this node")
    now = int(time.time())
    job_id = uuid.uuid4().hex
    job = {
        "job_id": job_id,
        "node_id": node_id,
        "hostname": str(node.get("pbname") or node.get("hostname") or ""),
        "status": "queued",
        "phase": "queued",
        "done": 0,
        "total": 0,
        "remaining": 0,
        "created_at": now,
        "updated_at": now,
        "error": "",
        "result": None,
    }
    _REMOTE_PUSH_JOBS[job_id] = job
    return job


def _update_remote_push_job(job_id: str, **updates: Any) -> dict[str, Any]:
    """Update and return one local remote-push progress record."""

    job = _REMOTE_PUSH_JOBS.get(str(job_id or ""))
    if not job:
        return {}
    job.update(updates)
    job["updated_at"] = int(time.time())
    return job


def _get_master_pbname() -> str:
    """Return the configured PBGui master name, falling back to the hostname."""

    cfg = configparser.ConfigParser()
    try:
        cfg.read(Path(PBGDIR) / "pbgui.ini")
        if cfg.has_option("main", "pbname"):
            pbname = cfg.get("main", "pbname").strip()
            if pbname:
                return pbname
    except Exception:
        pass
    return platform.node()


def _load_cluster_snapshot() -> dict[str, Any]:
    """Load local identity and rebuild materialized cluster state."""

    root = _cluster_root()
    try:
        identity = ensure_local_identity(root, role="master", pbname=_get_master_pbname())
        materialized = rebuild_materialized_state(root, write=False)
    except ClusterStateError as exc:
        _log(SERVICE, f"Failed to load cluster state: {exc}", level="ERROR")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except Exception as exc:
        _log(SERVICE, f"Unexpected cluster status error: {exc}", level="ERROR")
        raise HTTPException(status_code=500, detail="Failed to load cluster state") from exc

    return {
        "cluster_root": str(root),
        "identity": identity,
        **materialized,
    }


def _node_list(cluster_nodes: dict[str, Any]) -> list[dict[str, Any]]:
    """Return materialized nodes as a stable list."""

    nodes = cluster_nodes.get("nodes") if isinstance(cluster_nodes, dict) else {}
    if not isinstance(nodes, dict):
        return []
    return [dict(nodes[node_id]) for node_id in sorted(nodes)]


def _instance_list(desired_state: dict[str, Any]) -> list[dict[str, Any]]:
    """Return materialized instances as a stable list."""

    instances = desired_state.get("instances") if isinstance(desired_state, dict) else {}
    if not isinstance(instances, dict):
        return []
    result: list[dict[str, Any]] = []
    for name in sorted(instances):
        item = instances.get(name)
        row = dict(item) if isinstance(item, dict) else {}
        row["instance"] = name
        result.append(row)
    return result


def _tombstone_list(desired_state: dict[str, Any]) -> list[dict[str, Any]]:
    """Return materialized tombstones as a stable list."""

    tombstones = desired_state.get("tombstones") if isinstance(desired_state, dict) else {}
    if not isinstance(tombstones, dict):
        return []
    result: list[dict[str, Any]] = []
    for name in sorted(tombstones):
        item = tombstones.get(name)
        row = dict(item) if isinstance(item, dict) else {}
        row["instance"] = name
        result.append(row)
    return result


def _run_v7_root() -> Path:
    """Return the local V7 instance directory root."""

    return Path(PBGDIR) / "data" / "run_v7"


def _vps_hosts_root() -> Path:
    """Return the VPS Manager host config root."""

    return Path(PBGDIR) / "data" / "vpsmanager" / "hosts"


def _validate_instance_name(name: str) -> None:
    """Reject names that cannot safely map to one run_v7 directory."""

    if not name or "/" in name or "\\" in name or "\x00" in name or name in {".", ".."}:
        raise ClusterStateError("invalid instance name")


def _read_host_node_ids() -> dict[str, dict[str, Any]]:
    """Read the local temporary host→node_id mapping used before remote identity sync."""

    path = _cluster_root() / "host_node_ids.json"
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}
    hosts = data.get("hosts") if isinstance(data, dict) else None
    return hosts if isinstance(hosts, dict) else {}


def _write_host_node_ids(hosts: dict[str, dict[str, Any]]) -> None:
    """Atomically write local temporary host→node_id mappings."""

    path = _cluster_root() / "host_node_ids.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        tmp.write_text(json.dumps({"schema_version": 1, "hosts": hosts}, indent=4, sort_keys=True), encoding="utf-8")
        tmp.replace(path)
    finally:
        tmp.unlink(missing_ok=True)


def _coerce_ssh_port(value: Any) -> int:
    """Return a usable SSH port, falling back to 22 for partial VPS configs."""

    try:
        port = int(value or 22)
    except (TypeError, ValueError):
        return 22
    return port if 0 < port <= 65535 else 22


def _cluster_role_from_monitor_role(role: Any) -> str:
    """Map VPS Monitor role metadata to Cluster Sync node roles."""

    normalized = str(role or "").strip().lower()
    return "master" if normalized == "master" else "vps"


def _monitor_host_roles() -> dict[str, str]:
    """Return host roles from the same monitor metadata used by VPS Manager."""

    try:
        state = get_monitor_state_snapshot()
    except Exception:
        return {}
    host_meta = state.get("host_meta") if isinstance(state, dict) else None
    if not isinstance(host_meta, dict):
        return {}
    roles: dict[str, str] = {}
    for hostname, meta in host_meta.items():
        if not isinstance(meta, dict) or "role" not in meta:
            continue
        roles[str(hostname)] = _cluster_role_from_monitor_role(meta.get("role"))
    return roles


def _known_vps_configs() -> list[dict[str, Any]]:
    """Return VPS Manager host configs as bootstrap candidates."""

    root = _vps_hosts_root()
    if not root.is_dir():
        return []
    host_roles = _monitor_host_roles()
    result: list[dict[str, Any]] = []
    for host_dir in sorted(path for path in root.iterdir() if path.is_dir()):
        config_path = host_dir / f"{host_dir.name}.json"
        if not config_path.is_file():
            continue
        try:
            data = json.loads(config_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        if not isinstance(data, dict):
            continue
        hostname = str(data.get("_hostname") or config_path.parent.name).strip()
        if not hostname:
            continue
        result.append({
            "hostname": hostname,
            "role": host_roles.get(hostname),
            "ssh_host": str(data.get("ip") or "").strip(),
            "ssh_user": str(data.get("user") or "").strip(),
            "ssh_port": _coerce_ssh_port(data.get("firewall_ssh_port")),
            "remote_pbgui_dir": str(data.get("remote_pbgui_dir") or "").strip(),
            "config_path": str(config_path),
        })
    return result


def _resolve_bootstrap_assignment(
    identity: dict[str, Any],
    enabled_on: str,
    host_node_ids: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Resolve enabled_on to the current or future cluster node assignment."""

    hostname = str(enabled_on or "").strip()
    master_hostname = _get_master_pbname()
    if not hostname or hostname == "disabled" or hostname == master_hostname:
        return {
            "assigned_host": str(identity["node_id"]),
            "assigned_label": master_hostname,
            "assigned_role": "master",
            "will_create_node_mapping": False,
        }

    entry = host_node_ids.get(hostname)
    node_id = str(entry.get("node_id") or "") if isinstance(entry, dict) else ""
    return {
        "assigned_host": node_id,
        "assigned_label": hostname,
        "assigned_role": "vps",
        "will_create_node_mapping": not bool(node_id),
    }


def _node_payload_from_vps_config(node_id: str, config: dict[str, Any]) -> dict[str, Any]:
    """Build a cluster node payload from one VPS Manager host config."""

    hostname = str(config.get("hostname") or "").strip()
    role = _cluster_role_from_monitor_role(config.get("role"))
    sync_mode = str(config.get("sync_mode") or "").strip().lower()
    if sync_mode not in _EDITABLE_NODE_SYNC_MODES:
        sync_mode = "disabled" if config.get("sync_enabled", False) is False else ("reachable" if str(config.get("ssh_host") or "").strip() else "outbound_only")
    payload: dict[str, Any] = {
        "node_id": node_id,
        "role": role,
        "pbname": hostname,
        "hostname": hostname,
        "sync_mode": sync_mode,
        "sync_enabled": sync_mode != "disabled",
    }
    for source_key, target_key in (
        ("ssh_host", "ssh_host"),
        ("ssh_user", "ssh_user"),
        ("ssh_port", "ssh_port"),
        ("remote_pbgui_dir", "remote_pbgui_dir"),
    ):
        value = config.get(source_key)
        if value not in {None, ""}:
            payload[target_key] = value
    return payload


def _current_node_for_host(
    cluster_nodes: dict[str, Any],
    hostname: str,
    host_node_ids: dict[str, dict[str, Any]],
) -> tuple[str, dict[str, Any]]:
    """Return the current node id/record for a VPS hostname when known."""

    nodes = cluster_nodes.get("nodes") if isinstance(cluster_nodes, dict) else {}
    nodes = nodes if isinstance(nodes, dict) else {}
    mapping = host_node_ids.get(hostname)
    mapped_node_id = str(mapping.get("node_id") or "") if isinstance(mapping, dict) else ""
    if mapped_node_id and isinstance(nodes.get(mapped_node_id), dict):
        return mapped_node_id, dict(nodes[mapped_node_id])
    for node_id, node in nodes.items():
        if not isinstance(node, dict):
            continue
        if str(node.get("pbname") or node.get("hostname") or "") == hostname:
            return str(node_id), dict(node)
    return mapped_node_id, {}


def _node_metadata_matches(current: dict[str, Any], desired: dict[str, Any]) -> bool:
    """Return True when current node metadata already matches desired payload."""

    if normalize_node_sync_mode(current) != normalize_node_sync_mode(desired):
        return False
    for key, value in desired.items():
        if isinstance(value, bool):
            if (current.get(key) is not False) != value:
                return False
            continue
        if key == "ssh_port":
            try:
                if int(current.get(key) or 0) != int(value or 0):
                    return False
            except (TypeError, ValueError):
                return False
            continue
        if str(current.get(key) or "") != str(value or ""):
            return False
    return True


def _validate_node_sync_settings(payload: dict[str, Any]) -> dict[str, Any]:
    """Validate editable Cluster node sync settings from the UI."""

    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Request body must be a JSON object")
    sync_mode = str(payload.get("sync_mode") or "").strip().lower()
    if sync_mode not in _EDITABLE_NODE_SYNC_MODES:
        raise HTTPException(status_code=400, detail="sync_mode must be disabled, outbound_only or reachable")
    ssh_host = str(payload.get("ssh_host") or "").strip()
    ssh_user = str(payload.get("ssh_user") or "").strip()
    raw_port = payload.get("ssh_port", 22)
    try:
        ssh_port = int(raw_port or 22)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="ssh_port must be a number") from exc
    if ssh_port < 1 or ssh_port > 65535:
        raise HTTPException(status_code=400, detail="ssh_port must be between 1 and 65535")
    if sync_mode == "reachable" and not ssh_host:
        raise HTTPException(status_code=400, detail="Reachable nodes require an SSH host")
    return {
        "sync_mode": sync_mode,
        "sync_enabled": sync_mode != "disabled",
        "ssh_host": ssh_host,
        "ssh_user": ssh_user,
        "ssh_port": ssh_port,
    }


def _cluster_remote_command(remote_pbgui_dir: str | None, local_node_id: str, command_text: str) -> str:
    """Build one remote Cluster Sync wrapper command."""

    base = remote_shell_path(remote_pbgui_dir or "software/pbgui")
    local_node = shlex.quote(str(local_node_id))
    return (
        f"base={base}; "
        "parent=\"${base%/*}\"; "
        "if [ -x \"$parent/venv_pbgui/bin/python\" ]; then py=\"$parent/venv_pbgui/bin/python\"; "
        "elif [ -x \"$parent/venv_pbgui312/bin/python\" ]; then py=\"$parent/venv_pbgui312/bin/python\"; "
        "elif [ -x \"$base/.venv/bin/python\" ]; then py=\"$base/.venv/bin/python\"; "
        "else py=python3; fi; "
        "\"$py\" \"$base/cluster_sync_command.py\" --cluster-root \"$base/data/cluster\" "
        f"--remote-node {local_node} --allow-join {command_text}"
    )


def _cluster_hello_command(remote_pbgui_dir: str | None, local_node_id: str) -> str:
    """Build the read-only remote Cluster Sync hello command."""

    return _cluster_remote_command(remote_pbgui_dir, local_node_id, "hello")


def _cluster_state_read_command(remote_pbgui_dir: str | None, local_node_id: str, verb: str) -> str:
    """Build one read-only remote Cluster Sync state command."""

    if verb not in {"get-state-vector", "get-desired-state"}:
        raise ValueError("unsupported read command")
    return _cluster_remote_command(remote_pbgui_dir, local_node_id, verb)


def _cluster_materialize_command(remote_pbgui_dir: str | None, local_node_id: str, verb: str) -> str:
    """Build one remote materialization command."""

    if verb not in {"materialize-v7-preview", "materialize-v7", "materialize-api-keys-preview", "materialize-api-keys"}:
        raise ValueError("unsupported materialize command")
    return _cluster_remote_command(remote_pbgui_dir, local_node_id, verb)


def _cluster_payload_command(remote_pbgui_dir: str | None, local_node_id: str, command_text: str, payload: str) -> str:
    """Build a remote Cluster Sync command that receives a JSON payload on stdin."""

    command = _cluster_remote_command(remote_pbgui_dir, local_node_id, command_text)
    return f"printf '%s' {shlex.quote(str(payload))} | {{ {command}; }}"


async def _run_cluster_payload_command(
    pool: Any,
    hostname: str,
    remote_pbgui_dir: str | None,
    local_node_id: str,
    command_text: str,
    payload: str,
    *,
    timeout: int = 30,
) -> Any:
    """Run a remote Cluster Sync upload command and stream its payload over stdin."""

    command = _cluster_remote_command(remote_pbgui_dir, local_node_id, command_text)
    start_process = getattr(pool, "start_process", None)
    if not callable(start_process):
        return await pool.run(hostname, _cluster_payload_command(remote_pbgui_dir, local_node_id, command_text, payload), timeout=timeout)
    proc = await start_process(hostname, command)
    if proc is None:
        return None
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(input=str(payload)), timeout=timeout)
    except Exception:
        close = getattr(proc, "close", None)
        if callable(close):
            close()
        raise
    return SimpleNamespace(
        exit_status=int(getattr(proc, "exit_status", 1) or 0),
        stdout=stdout or "",
        stderr=stderr or "",
    )


def _cluster_join_command(
    remote_pbgui_dir: str | None,
    local_node_id: str,
    cluster_id: str,
    node: dict[str, Any],
) -> str:
    """Build the remote Cluster Sync identity join command."""

    node_id = shlex.quote(str(node.get("node_id") or ""))
    role = shlex.quote(_cluster_role_from_monitor_role(node.get("role")))
    pbname = shlex.quote(str(node.get("pbname") or node.get("hostname") or "").strip())
    command_text = f"join {shlex.quote(str(cluster_id))} {node_id} {role} {pbname}"
    return _cluster_remote_command(remote_pbgui_dir, local_node_id, command_text)


def _classify_probe_error(text: str) -> str:
    """Return a stable remote probe status for one error message."""

    lowered = str(text or "").lower()
    if "cluster identity is not initialized" in lowered:
        return "not_initialized"
    if "no such file" in lowered or "can't open file" in lowered or "not found" in lowered:
        return "command_unavailable"
    if "foreign cluster_id" in lowered:
        return "foreign_cluster"
    if "permission denied" in lowered or "authentication" in lowered:
        return "auth_failed"
    return "error"


def _probe_error_text(result: Any) -> str:
    """Extract a concise error string from an SSH command result."""

    stderr = str(getattr(result, "stderr", "") or "").strip()
    stdout = str(getattr(result, "stdout", "") or "").strip()
    for raw in (stderr, stdout):
        if not raw:
            continue
        try:
            payload = json.loads(raw.splitlines()[-1])
        except json.JSONDecodeError:
            return raw.splitlines()[-1]
        if isinstance(payload, dict) and payload.get("error"):
            return str(payload.get("error"))
    return "remote hello failed"


def _parse_remote_json_result(result: Any, failure_label: str) -> dict[str, Any]:
    """Parse the last JSON line from an SSH command result."""

    stdout = str(getattr(result, "stdout", "") or "").strip()
    try:
        payload = json.loads(stdout.splitlines()[-1])
    except (IndexError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=502, detail=f"{failure_label} returned an invalid response") from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=502, detail=f"{failure_label} returned an invalid response")
    return payload


async def _probe_cluster_node(node: dict[str, Any], identity: dict[str, Any]) -> dict[str, Any]:
    """Run a read-only restricted hello probe against one remote node."""

    node_id = str(node.get("node_id") or "")
    hostname = str(node.get("pbname") or node.get("hostname") or "")
    local_node_id = str(identity.get("node_id") or "")
    if not node_id or node_id == local_node_id:
        return {"node_id": node_id, "hostname": hostname, "status": "local", "ok": True}
    sync_mode = normalize_node_sync_mode(node)
    if node.get("enabled") is False or sync_mode == "disabled":
        return {"node_id": node_id, "hostname": hostname, "status": "disabled", "ok": False}
    if sync_mode == "outbound_only":
        return {"node_id": node_id, "hostname": hostname, "status": "outbound_only", "ok": True}
    if not str(node.get("ssh_host") or "").strip():
        return {"node_id": node_id, "hostname": hostname, "status": "config_error", "ok": False, "error": "reachable node has no SSH host"}
    if not hostname:
        return {"node_id": node_id, "hostname": hostname, "status": "missing_hostname", "ok": False}
    monitor = get_monitor()
    pool = getattr(monitor, "pool", None) if monitor else None
    if not pool:
        return {"node_id": node_id, "hostname": hostname, "status": "monitor_unavailable", "ok": False}
    command = _cluster_hello_command(str(node.get("remote_pbgui_dir") or ""), local_node_id)
    try:
        result = await pool.run(hostname, command, timeout=10)
    except Exception as exc:
        return {"node_id": node_id, "hostname": hostname, "status": "error", "ok": False, "error": str(exc)}
    if result is None:
        return {"node_id": node_id, "hostname": hostname, "status": "unreachable", "ok": False}
    exit_status = int(getattr(result, "exit_status", 1) or 0)
    if exit_status != 0:
        error = _probe_error_text(result)
        return {
            "node_id": node_id,
            "hostname": hostname,
            "status": _classify_probe_error(error),
            "ok": False,
            "error": error,
        }
    stdout = str(getattr(result, "stdout", "") or "").strip()
    try:
        payload = json.loads(stdout.splitlines()[-1])
    except (IndexError, json.JSONDecodeError):
        return {"node_id": node_id, "hostname": hostname, "status": "invalid_response", "ok": False}
    remote_cluster_id = str(payload.get("cluster_id") or "") if isinstance(payload, dict) else ""
    remote_node_id = str(payload.get("node_id") or "") if isinstance(payload, dict) else ""
    if remote_cluster_id and remote_cluster_id != str(identity.get("cluster_id") or ""):
        return {
            "node_id": node_id,
            "hostname": hostname,
            "status": "foreign_cluster",
            "ok": False,
            "remote_cluster_id": remote_cluster_id,
            "remote_node_id": remote_node_id,
        }
    if remote_node_id and remote_node_id != node_id:
        return {
            "node_id": node_id,
            "hostname": hostname,
            "status": "node_mismatch",
            "ok": False,
            "remote_cluster_id": remote_cluster_id,
            "remote_node_id": remote_node_id,
        }
    return {
        "node_id": node_id,
        "hostname": hostname,
        "status": "ok",
        "ok": True,
        "remote_cluster_id": remote_cluster_id,
        "remote_node_id": remote_node_id,
        "protocol_version": payload.get("protocol_version") if isinstance(payload, dict) else None,
        "role": payload.get("role") if isinstance(payload, dict) else "",
    }


async def _probe_cluster_nodes(nodes: list[dict[str, Any]], identity: dict[str, Any]) -> list[dict[str, Any]]:
    """Probe remote nodes with a small concurrency limit."""

    sem = asyncio.Semaphore(5)

    async def run_one(node: dict[str, Any]) -> dict[str, Any]:
        async with sem:
            return await _probe_cluster_node(node, identity)

    return await asyncio.gather(*(run_one(node) for node in nodes))


def _node_for_id(nodes: list[dict[str, Any]], node_id: str) -> dict[str, Any]:
    """Return one materialized node by id, or an empty dict."""

    for node in nodes:
        if str(node.get("node_id") or "") == node_id:
            return dict(node)
    return {}


async def _run_remote_join(node: dict[str, Any], identity: dict[str, Any]) -> dict[str, Any]:
    """Initialize remote cluster identity for one known node."""

    node_id = str(node.get("node_id") or "")
    hostname = str(node.get("pbname") or node.get("hostname") or "")
    local_node_id = str(identity.get("node_id") or "")
    if not node_id or node_id == local_node_id:
        raise HTTPException(status_code=400, detail="Cannot join the local node")
    sync_mode = normalize_node_sync_mode(node)
    if node.get("enabled") is False or sync_mode == "disabled":
        raise HTTPException(status_code=400, detail="Cluster node is disabled")
    if sync_mode == "outbound_only":
        raise HTTPException(status_code=400, detail="Cluster node is outbound-only")
    if not str(node.get("ssh_host") or "").strip():
        raise HTTPException(status_code=400, detail="Reachable cluster node has no SSH host")
    if not hostname:
        raise HTTPException(status_code=400, detail="Cluster node has no hostname")
    monitor = get_monitor()
    pool = getattr(monitor, "pool", None) if monitor else None
    if not pool:
        raise HTTPException(status_code=503, detail="VPS monitor SSH pool is unavailable")

    command = _cluster_join_command(
        str(node.get("remote_pbgui_dir") or ""),
        local_node_id,
        str(identity.get("cluster_id") or ""),
        node,
    )
    try:
        result = await pool.run(hostname, command, timeout=15)
    except Exception as exc:
        _log(SERVICE, f"Remote cluster join failed for {hostname}: {exc}", level="ERROR")
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    if result is None:
        raise HTTPException(status_code=502, detail="Remote host is unreachable")
    exit_status = int(getattr(result, "exit_status", 1) or 0)
    if exit_status != 0:
        error = _probe_error_text(result)
        _log(SERVICE, f"Remote cluster join rejected by {hostname}: {error}", level="WARNING")
        raise HTTPException(status_code=409, detail=error)
    stdout = str(getattr(result, "stdout", "") or "").strip()
    try:
        payload = json.loads(stdout.splitlines()[-1])
    except (IndexError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=502, detail="Remote join returned an invalid response") from exc
    remote_cluster_id = str(payload.get("cluster_id") or "") if isinstance(payload, dict) else ""
    remote_node_id = str(payload.get("node_id") or "") if isinstance(payload, dict) else ""
    if remote_cluster_id != str(identity.get("cluster_id") or ""):
        raise HTTPException(status_code=409, detail="Remote joined a different cluster_id")
    if remote_node_id != node_id:
        raise HTTPException(status_code=409, detail="Remote joined a different node_id")
    return {
        "ok": True,
        "node_id": node_id,
        "hostname": hostname,
        "remote_cluster_id": remote_cluster_id,
        "remote_node_id": remote_node_id,
        "role": payload.get("role") if isinstance(payload, dict) else node.get("role"),
    }


def _require_remote_node_ready(node: dict[str, Any], identity: dict[str, Any]) -> tuple[str, str, str]:
    """Validate a materialized node is usable for remote reads."""

    node_id = str(node.get("node_id") or "")
    hostname = str(node.get("pbname") or node.get("hostname") or "")
    local_node_id = str(identity.get("node_id") or "")
    if not node_id or node_id == local_node_id:
        raise HTTPException(status_code=400, detail="Cannot read the local node as a remote")
    sync_mode = normalize_node_sync_mode(node)
    if node.get("enabled") is False or sync_mode == "disabled":
        raise HTTPException(status_code=400, detail="Cluster node is disabled")
    if sync_mode == "outbound_only":
        raise HTTPException(status_code=400, detail="Cluster node is outbound-only")
    if not str(node.get("ssh_host") or "").strip():
        raise HTTPException(status_code=400, detail="Reachable cluster node has no SSH host")
    if not hostname:
        raise HTTPException(status_code=400, detail="Cluster node has no hostname")
    return node_id, hostname, local_node_id


async def _run_remote_read_command(node: dict[str, Any], identity: dict[str, Any], verb: str) -> dict[str, Any]:
    """Run one read-only Cluster Sync command against a remote node."""

    node_id, hostname, local_node_id = _require_remote_node_ready(node, identity)
    monitor = get_monitor()
    pool = getattr(monitor, "pool", None) if monitor else None
    if not pool:
        raise HTTPException(status_code=503, detail="VPS monitor SSH pool is unavailable")
    command = _cluster_state_read_command(str(node.get("remote_pbgui_dir") or ""), local_node_id, verb)
    try:
        result = await pool.run(hostname, command, timeout=15)
    except Exception as exc:
        _log(SERVICE, f"Remote cluster read failed for {hostname}: {exc}", level="ERROR")
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    if result is None:
        raise HTTPException(status_code=502, detail="Remote host is unreachable")
    exit_status = int(getattr(result, "exit_status", 1) or 0)
    if exit_status != 0:
        error = _probe_error_text(result)
        _log(SERVICE, f"Remote cluster read rejected by {hostname}: {error}", level="WARNING")
        raise HTTPException(status_code=409, detail=error)
    payload = _parse_remote_json_result(result, "Remote cluster read")
    remote_cluster_id = str(payload.get("cluster_id") or "")
    remote_node_id = str(payload.get("node_id") or "")
    if remote_cluster_id and remote_cluster_id != str(identity.get("cluster_id") or ""):
        raise HTTPException(status_code=409, detail="Remote belongs to a different cluster_id")
    if remote_node_id and remote_node_id != node_id:
        raise HTTPException(status_code=409, detail="Remote reports a different node_id")
    return payload


async def _run_remote_materialize_command(
    node: dict[str, Any],
    identity: dict[str, Any],
    verb: str,
    *,
    timeout: int = 30,
) -> dict[str, Any]:
    """Run one remote V7 config materialization command."""

    node_id, hostname, local_node_id = _require_remote_node_ready(node, identity)
    monitor = get_monitor()
    pool = getattr(monitor, "pool", None) if monitor else None
    if not pool:
        raise HTTPException(status_code=503, detail="VPS monitor SSH pool is unavailable")
    command = _cluster_materialize_command(str(node.get("remote_pbgui_dir") or ""), local_node_id, verb)
    try:
        result = await pool.run(hostname, command, timeout=timeout)
    except Exception as exc:
        _log(SERVICE, f"Remote cluster materialize failed for {hostname}: {exc}", level="ERROR")
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    if result is None:
        raise HTTPException(status_code=502, detail="Remote host is unreachable")
    exit_status = int(getattr(result, "exit_status", 1) or 0)
    if exit_status != 0:
        error = _probe_error_text(result)
        _log(SERVICE, f"Remote cluster materialize rejected by {hostname}: {error}", level="WARNING")
        raise HTTPException(status_code=409, detail=error)
    payload = _parse_remote_json_result(result, "Remote materialize")
    remote_cluster_id = str(payload.get("cluster_id") or "")
    remote_node_id = str(payload.get("node_id") or "")
    if remote_cluster_id and remote_cluster_id != str(identity.get("cluster_id") or ""):
        raise HTTPException(status_code=409, detail="Remote belongs to a different cluster_id")
    if remote_node_id and remote_node_id != node_id:
        raise HTTPException(status_code=409, detail="Remote reports a different node_id")
    return payload


def _as_state_vector(value: Any) -> dict[str, int]:
    """Normalize a state-vector payload into actor sequence integers."""

    if not isinstance(value, dict):
        return {}
    result: dict[str, int] = {}
    for actor, seq in value.items():
        try:
            result[str(actor)] = max(0, int(seq or 0))
        except (TypeError, ValueError):
            result[str(actor)] = 0
    return result


def _compare_state_vectors(local: dict[str, int], remote: dict[str, int]) -> dict[str, Any]:
    """Return a compact read-only diff between local and remote state vectors."""

    rows: list[dict[str, Any]] = []
    counts = {"equal": 0, "local_ahead": 0, "remote_ahead": 0}
    for actor in sorted(set(local) | set(remote)):
        local_seq = int(local.get(actor, 0))
        remote_seq = int(remote.get(actor, 0))
        if local_seq == remote_seq:
            status = "equal"
        elif local_seq > remote_seq:
            status = "local_ahead"
        else:
            status = "remote_ahead"
        counts[status] += 1
        rows.append({
            "actor": actor,
            "local_seq": local_seq,
            "remote_seq": remote_seq,
            "delta": remote_seq - local_seq,
            "status": status,
        })
    return {"counts": counts, "actors": rows}


def _compare_named_records(local: dict[str, Any], remote: dict[str, Any], fields: tuple[str, ...]) -> dict[str, Any]:
    """Compare named desired-state records by a fixed set of top-level fields."""

    local_names = set(local)
    remote_names = set(remote)
    mismatches: list[dict[str, Any]] = []
    for name in sorted(local_names & remote_names):
        local_item = local.get(name) if isinstance(local.get(name), dict) else {}
        remote_item = remote.get(name) if isinstance(remote.get(name), dict) else {}
        diff_fields = [field for field in fields if str(local_item.get(field) or "") != str(remote_item.get(field) or "")]
        if diff_fields:
            mismatches.append({"name": name, "fields": diff_fields})
    return {
        "local_count": len(local_names),
        "remote_count": len(remote_names),
        "missing_on_remote": sorted(local_names - remote_names),
        "missing_locally": sorted(remote_names - local_names),
        "mismatches": mismatches,
    }


def _compare_desired_states(local: dict[str, Any], remote: dict[str, Any]) -> dict[str, Any]:
    """Return a read-only desired-state comparison summary."""

    local_instances = local.get("instances") if isinstance(local, dict) else {}
    remote_instances = remote.get("instances") if isinstance(remote, dict) else {}
    local_tombstones = local.get("tombstones") if isinstance(local, dict) else {}
    remote_tombstones = remote.get("tombstones") if isinstance(remote, dict) else {}
    local_instances = local_instances if isinstance(local_instances, dict) else {}
    remote_instances = remote_instances if isinstance(remote_instances, dict) else {}
    local_tombstones = local_tombstones if isinstance(local_tombstones, dict) else {}
    remote_tombstones = remote_tombstones if isinstance(remote_tombstones, dict) else {}
    instance_diff = _compare_named_records(
        local_instances,
        remote_instances,
        ("version", "desired_state", "assigned_host", "config_manifest_hash", "conflicted"),
    )
    tombstone_diff = _compare_named_records(local_tombstones, remote_tombstones, ("version", "deleted_by", "op_id"))
    api_keys_match = (local.get("api_keys") if isinstance(local, dict) else None) == (remote.get("api_keys") if isinstance(remote, dict) else None)
    return {
        "instances": instance_diff,
        "tombstones": tombstone_diff,
        "api_keys_match": api_keys_match,
    }


def _operation_target(operation: dict[str, Any]) -> str:
    """Return a compact target label for one operation."""

    if operation.get("instance"):
        return str(operation.get("instance") or "")
    if operation.get("node_id"):
        return str(operation.get("node_id") or "")
    if operation.get("api_serial") is not None:
        return "api-keys"
    return "cluster"


def _operation_hash_refs(operation: dict[str, Any]) -> dict[str, list[str]]:
    """Return hash references that a later write phase may need to ship."""

    refs = {"config": [], "api_payload": [], "secret": []}
    config_hash = str(operation.get("config_manifest_hash") or "")
    if config_hash:
        refs["config"].append(config_hash)
    payload_hash = str(operation.get("payload_hash") or "")
    if payload_hash:
        refs["api_payload"].append(payload_hash)
    secret_hash = str(operation.get("secret_blob_hash") or "")
    if secret_hash:
        refs["secret"].append(secret_hash)
    return refs


def _pushed_operation_summary(operation: dict[str, Any], payload_result: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return the compact API summary for one pushed operation."""

    payload_result = payload_result if isinstance(payload_result, dict) else {}
    return {
        "op_id": str(payload_result.get("op_id") or operation.get("op_id") or ""),
        "actor": str(payload_result.get("actor") or operation.get("actor") or ""),
        "seq": int(payload_result.get("seq") or operation.get("seq") or 0),
        "op": str(operation.get("op") or ""),
        "target": _operation_target(operation),
    }


def _canonical_json_bytes(value: Any) -> bytes:
    """Return canonical JSON bytes used for cluster content-addressed blobs."""

    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _collect_current_config_blobs(desired_state: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    """Collect current V7 config manifest and file blobs referenced by desired state."""

    instances = desired_state.get("instances") if isinstance(desired_state, dict) else {}
    instances = instances if isinstance(instances, dict) else {}
    blobs_by_hash: dict[str, dict[str, Any]] = {}
    skipped: list[dict[str, str]] = []
    for name in sorted(instances):
        item = instances.get(name) if isinstance(instances.get(name), dict) else {}
        expected_manifest_hash = str(item.get("config_manifest_hash") or "")
        if not expected_manifest_hash:
            continue
        try:
            _validate_instance_name(str(name))
            instance_dir = _run_v7_root() / str(name)
            manifest = build_config_manifest(instance_dir)
            actual_manifest_hash = compute_config_manifest_hash(manifest)
            if actual_manifest_hash != expected_manifest_hash:
                skipped.append({"instance": str(name), "reason": "local config manifest no longer matches desired state"})
                continue
            manifest_raw = _canonical_json_bytes(manifest)
            blobs_by_hash.setdefault(expected_manifest_hash, {
                "hash": expected_manifest_hash,
                "raw": manifest_raw,
                "kind": "manifest",
                "instance": str(name),
                "name": "manifest.json",
            })
            files = manifest.get("files") if isinstance(manifest, dict) else {}
            files = files if isinstance(files, dict) else {}
            for filename in sorted(files):
                meta = files.get(filename) if isinstance(files.get(filename), dict) else {}
                sha = str(meta.get("sha256") or "")
                if not sha:
                    continue
                _validate_instance_name(str(filename))
                path = instance_dir / str(filename)
                raw = path.read_bytes()
                blob_hash = f"sha256:{sha}"
                blobs_by_hash.setdefault(blob_hash, {
                    "hash": blob_hash,
                    "raw": raw,
                    "kind": "file",
                    "instance": str(name),
                    "name": str(filename),
                })
        except Exception as exc:
            skipped.append({"instance": str(name), "reason": str(exc)})
    return list(blobs_by_hash.values()), skipped


def _cluster_blob_path(base_dir: Path, blob_hash: str) -> Path:
    """Return the content-addressed path for one sha256 blob hash."""

    digest = str(blob_hash or "").removeprefix("sha256:")
    return Path(base_dir) / "sha256" / digest[:2] / f"{digest}.json"


def _read_cluster_blob(base_dir: Path, blob_hash: str) -> bytes:
    """Read and verify one local Cluster Sync blob."""

    text = str(blob_hash or "")
    if not text.startswith("sha256:") or len(text) != len("sha256:") + 64:
        raise ValueError("invalid blob hash")
    path = _cluster_blob_path(base_dir, text)
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != text.removeprefix("sha256:"):
        raise ValueError("blob hash mismatch")
    return raw


def _collect_current_api_key_blobs(desired_state: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, str]]]:
    """Collect current API-key payload and secret blobs referenced by desired state."""

    api_keys = desired_state.get("api_keys") if isinstance(desired_state, dict) else None
    if not isinstance(api_keys, dict):
        return [], [], []
    root = _cluster_root()
    payload_blobs: list[dict[str, Any]] = []
    secret_blobs: list[dict[str, Any]] = []
    skipped: list[dict[str, str]] = []
    payload_hash = str(api_keys.get("payload_hash") or "")
    secret_hash = str(api_keys.get("secret_blob_hash") or "")
    if payload_hash:
        try:
            payload_blobs.append({
                "hash": payload_hash,
                "raw": _read_cluster_blob(root / "config_blobs", payload_hash),
                "kind": "api_payload",
                "name": "api-keys-payload.json",
            })
        except Exception as exc:
            skipped.append({"kind": "api_payload", "hash": payload_hash, "reason": str(exc)})
    if secret_hash:
        try:
            secret_blobs.append({
                "hash": secret_hash,
                "raw": _read_cluster_blob(root / "secret_blobs", secret_hash),
                "kind": "api_secret",
                "name": "api-keys.json",
            })
        except Exception as exc:
            skipped.append({"kind": "api_secret", "hash": secret_hash, "reason": str(exc)})
    return payload_blobs, secret_blobs, skipped


def _blob_batch_payload(blobs: list[dict[str, Any]]) -> str:
    """Build one JSON payload for the remote put-blobs command."""

    return json.dumps({
        "blobs": [
            {"hash": str(blob.get("hash") or ""), "content_b64": base64.b64encode(blob.get("raw") or b"").decode("ascii")}
            for blob in blobs
        ]
    }, sort_keys=True, separators=(",", ":"))


def _chunk_config_blobs(blobs: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    """Split config blobs into bounded JSON payload chunks."""

    chunks: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    current_size = len('{"blobs":[]}')
    for blob in blobs:
        encoded_size = len(str(blob.get("hash") or "")) + len(base64.b64encode(blob.get("raw") or b"")) + 64
        if current and current_size + encoded_size > _CONFIG_BLOB_BATCH_TARGET_BYTES:
            chunks.append(current)
            current = []
            current_size = len('{"blobs":[]}')
        current.append(blob)
        current_size += encoded_size
    if current:
        chunks.append(current)
    return chunks


def _build_operation_sync_preview(local_operations: list[dict[str, Any]], remote_vector: dict[str, int]) -> dict[str, Any]:
    """Build a read-only preview of operations missing across the boundary."""

    local_vector: dict[str, int] = {}
    local_missing_remote: list[dict[str, Any]] = []
    push_by_op: dict[str, int] = {}
    hashes = {"config": set(), "api_payload": set(), "secret": set()}
    for operation in local_operations:
        actor = str(operation.get("actor") or "")
        seq = int(operation.get("seq") or 0)
        if not actor or seq < 1:
            continue
        local_vector[actor] = max(local_vector.get(actor, 0), seq)
        if seq <= int(remote_vector.get(actor, 0)):
            continue
        op_name = str(operation.get("op") or "")
        refs = _operation_hash_refs(operation)
        for key, values in refs.items():
            hashes[key].update(values)
        push_by_op[op_name] = push_by_op.get(op_name, 0) + 1
        local_missing_remote.append({
            "op_id": str(operation.get("op_id") or ""),
            "actor": actor,
            "seq": seq,
            "op": op_name,
            "target": _operation_target(operation),
            "created_at": int(operation.get("created_at") or 0),
            "hash_refs": refs,
        })

    remote_missing_local: list[dict[str, Any]] = []
    for actor in sorted(set(remote_vector) | set(local_vector)):
        remote_seq = int(remote_vector.get(actor, 0))
        local_seq = int(local_vector.get(actor, 0))
        if remote_seq <= local_seq:
            continue
        remote_missing_local.append({
            "actor": actor,
            "local_seq": local_seq,
            "remote_seq": remote_seq,
            "missing_count": remote_seq - local_seq,
        })

    return {
        "read_only": True,
        "local_ops_missing_on_remote": local_missing_remote,
        "remote_ops_missing_locally": remote_missing_local,
        "counts": {
            "local_ops_to_push": len(local_missing_remote),
            "remote_ops_to_pull": sum(item["missing_count"] for item in remote_missing_local),
            "actors_to_push": len({item["actor"] for item in local_missing_remote}),
            "actors_to_pull": len(remote_missing_local),
            "config_hash_refs": len(hashes["config"]),
            "api_payload_hash_refs": len(hashes["api_payload"]),
            "secret_blob_hash_refs": len(hashes["secret"]),
        },
        "push_by_op": {key: push_by_op[key] for key in sorted(push_by_op)},
    }


def _select_operations_missing_on_remote(local_operations: list[dict[str, Any]], remote_vector: dict[str, int]) -> list[dict[str, Any]]:
    """Return full local operations whose actor/seq is above the remote vector."""

    result: list[dict[str, Any]] = []
    for operation in local_operations:
        actor = str(operation.get("actor") or "")
        try:
            seq = int(operation.get("seq") or 0)
        except (TypeError, ValueError):
            continue
        if actor and seq > int(remote_vector.get(actor, 0)):
            result.append(dict(operation))
    result.sort(key=lambda item: (str(item.get("actor") or ""), int(item.get("seq") or 0), str(item.get("op_id") or "")))
    return result


async def _push_config_blobs_to_remote(
    pool: Any,
    hostname: str,
    remote_dir: str,
    local_node_id: str,
    blobs: list[dict[str, Any]],
    progress_callback: Callable[[dict[str, Any]], None],
) -> dict[str, int]:
    """Push config blobs to a remote node, preferring bulk upload."""

    if not blobs:
        progress_callback({"phase": "blobs", "done": 0, "total": 0, "remaining": 0})
        return {"pushed": 0, "total": 0, "fallback": 0}

    pushed = 0
    fallback = False
    progress_callback({"phase": "blobs", "done": 0, "total": len(blobs), "remaining": len(blobs)})
    for chunk in _chunk_config_blobs(blobs):
        payload = _blob_batch_payload(chunk)
        try:
            result = await _run_cluster_payload_command(pool, hostname, remote_dir, local_node_id, "put-blobs", payload, timeout=60)
        except Exception as exc:
            _log(SERVICE, f"Remote cluster put-blobs failed for {hostname}: {exc}", level="ERROR")
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        if result is None:
            raise HTTPException(status_code=502, detail="Remote host is unreachable")
        if int(getattr(result, "exit_status", 1) or 0) == 0:
            _parse_remote_json_result(result, "Remote put-blobs")
            pushed += len(chunk)
            progress_callback({"phase": "blobs", "done": pushed, "total": len(blobs), "remaining": len(blobs) - pushed, "bulk": True})
            continue
        error = _probe_error_text(result)
        if "unsupported command: put-blobs" not in error.lower():
            _log(SERVICE, f"Remote cluster put-blobs rejected by {hostname}: {error}", level="WARNING")
            raise HTTPException(status_code=409, detail=error)
        fallback = True
        _log(SERVICE, f"Remote cluster put-blobs unavailable on {hostname}; falling back to put-blob", level="WARNING")
        for blob in chunk:
            raw = blob.get("raw") or b""
            try:
                text_payload = raw.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise HTTPException(status_code=409, detail="Config blob is not UTF-8; remote bulk upload is required") from exc
            try:
                single_result = await _run_cluster_payload_command(
                    pool,
                    hostname,
                    remote_dir,
                    local_node_id,
                    f"put-blob {shlex.quote(str(blob.get('hash') or ''))}",
                    text_payload,
                    timeout=30,
                )
            except Exception as exc:
                _log(SERVICE, f"Remote cluster put-blob failed for {hostname}: {exc}", level="ERROR")
                raise HTTPException(status_code=502, detail=str(exc)) from exc
            if single_result is None:
                raise HTTPException(status_code=502, detail="Remote host is unreachable")
            if int(getattr(single_result, "exit_status", 1) or 0) != 0:
                error = _probe_error_text(single_result)
                _log(SERVICE, f"Remote cluster put-blob rejected by {hostname}: {error}", level="WARNING")
                raise HTTPException(status_code=409, detail=error)
            _parse_remote_json_result(single_result, "Remote put-blob")
            pushed += 1
            progress_callback({"phase": "blobs", "done": pushed, "total": len(blobs), "remaining": len(blobs) - pushed, "bulk": False})
    return {"pushed": pushed, "total": len(blobs), "fallback": 1 if fallback else 0}


async def _push_secret_blobs_to_remote(
    pool: Any,
    hostname: str,
    remote_dir: str,
    local_node_id: str,
    blobs: list[dict[str, Any]],
    progress_callback: Callable[[dict[str, Any]], None],
) -> dict[str, int]:
    """Push API-key secret blobs to a remote node."""

    if not blobs:
        progress_callback({"phase": "secrets", "done": 0, "total": 0, "remaining": 0})
        return {"pushed": 0, "total": 0}

    pushed = 0
    progress_callback({"phase": "secrets", "done": 0, "total": len(blobs), "remaining": len(blobs)})
    for blob in blobs:
        raw = blob.get("raw") or b""
        try:
            text_payload = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise HTTPException(status_code=409, detail="Secret blob is not UTF-8 JSON") from exc
        try:
            result = await _run_cluster_payload_command(
                pool,
                hostname,
                remote_dir,
                local_node_id,
                f"put-secret-blob {shlex.quote(str(blob.get('hash') or ''))}",
                text_payload,
                timeout=30,
            )
        except Exception as exc:
            _log(SERVICE, f"Remote cluster put-secret-blob failed for {hostname}: {exc}", level="ERROR")
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        if result is None:
            raise HTTPException(status_code=502, detail="Remote host is unreachable")
        if int(getattr(result, "exit_status", 1) or 0) != 0:
            error = _probe_error_text(result)
            _log(SERVICE, f"Remote cluster put-secret-blob rejected by {hostname}: {error}", level="WARNING")
            raise HTTPException(status_code=409, detail=error)
        _parse_remote_json_result(result, "Remote put-secret-blob")
        pushed += 1
        progress_callback({"phase": "secrets", "done": pushed, "total": len(blobs), "remaining": len(blobs) - pushed})
    return {"pushed": pushed, "total": len(blobs)}


async def _push_missing_operations_to_remote(
    node: dict[str, Any],
    identity: dict[str, Any],
    *,
    limit: int | None = None,
    rebuild: bool = True,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Push missing local oplog entries to a remote node and optionally rebuild its materialized state."""

    def report_progress(update: dict[str, Any]) -> None:
        if progress_callback:
            progress_callback(dict(update))

    node_id, hostname, local_node_id = _require_remote_node_ready(node, identity)
    monitor = get_monitor()
    pool = getattr(monitor, "pool", None) if monitor else None
    if not pool:
        raise HTTPException(status_code=503, detail="VPS monitor SSH pool is unavailable")

    root = _cluster_root()
    remote_vector_payload = await _run_remote_read_command(node, identity, "get-state-vector")
    remote_vector = _as_state_vector(remote_vector_payload.get("state_vector") or {})
    local_operations = load_operations(root, expected_cluster_id=str(identity.get("cluster_id") or ""))
    preview = _build_operation_sync_preview(local_operations, remote_vector)
    counts = preview.get("counts") if isinstance(preview, dict) else {}
    if int((counts or {}).get("remote_ops_to_pull") or 0) > 0:
        raise HTTPException(status_code=409, detail="Remote has operations missing locally; pull or resolve before pushing local ops")
    operations = _select_operations_missing_on_remote(local_operations, remote_vector)
    total_missing = len(operations)
    report_progress({"phase": "pushing", "done": 0, "total": total_missing, "remaining": total_missing})
    if limit is not None and limit > 0:
        operations = operations[:limit]
    remaining_after_batch = max(0, total_missing - len(operations))
    if not operations:
        result = {
            "ok": True,
            "node_id": node_id,
            "hostname": hostname,
            "pushed": [],
            "counts": {"pushed": 0, "rebuilt": 0, "local_ops_remaining": 0, "total_missing_before": 0},
            "message": "Remote already has all local operations.",
        }
        report_progress({"phase": "done", "done": 0, "total": 0, "remaining": 0})
        return result

    pushed: list[dict[str, Any]] = []
    remote_dir = str(node.get("remote_pbgui_dir") or "")
    local_materialized = rebuild_materialized_state(root, write=False)
    config_blobs, config_blob_skips = _collect_current_config_blobs(local_materialized.get("desired_state") or {})
    api_payload_blobs, secret_blobs, api_blob_skips = _collect_current_api_key_blobs(local_materialized.get("desired_state") or {})
    blob_counts = await _push_config_blobs_to_remote(
        pool,
        hostname,
        remote_dir,
        local_node_id,
        config_blobs + api_payload_blobs,
        report_progress,
    )
    secret_blob_counts = await _push_secret_blobs_to_remote(pool, hostname, remote_dir, local_node_id, secret_blobs, report_progress)

    bulk_payload = json.dumps({"operations": operations}, sort_keys=True, separators=(",", ":"))
    bulk_unsupported = False
    try:
        bulk_result = await _run_cluster_payload_command(pool, hostname, remote_dir, local_node_id, "put-ops", bulk_payload, timeout=60)
    except Exception as exc:
        _log(SERVICE, f"Remote cluster put-ops failed for {hostname}: {exc}", level="ERROR")
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    if bulk_result is None:
        raise HTTPException(status_code=502, detail="Remote host is unreachable")
    if int(getattr(bulk_result, "exit_status", 1) or 0) == 0:
        payload_result = _parse_remote_json_result(bulk_result, "Remote put-ops")
        returned = payload_result.get("operations") if isinstance(payload_result, dict) else []
        returned = returned if isinstance(returned, list) else []
        for index, operation in enumerate(operations):
            item_result = returned[index] if index < len(returned) and isinstance(returned[index], dict) else None
            pushed.append(_pushed_operation_summary(operation, item_result))
        report_progress({
            "phase": "pushing",
            "done": len(pushed),
            "total": total_missing,
            "remaining": max(0, total_missing - len(pushed)),
            "bulk": True,
        })
    else:
        error = _probe_error_text(bulk_result)
        bulk_unsupported = "unsupported command: put-ops" in error.lower()
        if not bulk_unsupported:
            _log(SERVICE, f"Remote cluster put-ops rejected by {hostname}: {error}", level="WARNING")
            raise HTTPException(status_code=409, detail=error)
        _log(SERVICE, f"Remote cluster put-ops unavailable on {hostname}; falling back to put-op", level="WARNING")

    if bulk_unsupported:
        for operation in operations:
            payload = json.dumps(operation, sort_keys=True, separators=(",", ":"))
            try:
                result = await _run_cluster_payload_command(pool, hostname, remote_dir, local_node_id, "put-op", payload, timeout=30)
            except Exception as exc:
                _log(SERVICE, f"Remote cluster put-op failed for {hostname}: {exc}", level="ERROR")
                raise HTTPException(status_code=502, detail=str(exc)) from exc
            if result is None:
                raise HTTPException(status_code=502, detail="Remote host is unreachable")
            exit_status = int(getattr(result, "exit_status", 1) or 0)
            if exit_status != 0:
                error = _probe_error_text(result)
                _log(SERVICE, f"Remote cluster put-op rejected by {hostname}: {error}", level="WARNING")
                raise HTTPException(status_code=409, detail=error)
            payload_result = _parse_remote_json_result(result, "Remote put-op")
            pushed.append(_pushed_operation_summary(operation, payload_result))
            report_progress({
                "phase": "pushing",
                "done": len(pushed),
                "total": total_missing,
                "remaining": max(0, total_missing - len(pushed)),
                "current": pushed[-1],
                "bulk": False,
            })

    if not rebuild:
        return {
            "ok": True,
            "node_id": node_id,
            "hostname": hostname,
            "pushed": pushed,
            "counts": {
                "pushed": len(pushed),
                "rebuilt": 0,
                "local_ops_remaining": remaining_after_batch,
                "total_missing_before": total_missing,
                "config_blobs_pushed": int(blob_counts.get("pushed") or 0),
                "config_blobs_total": int(blob_counts.get("total") or 0),
                "config_blobs_skipped": len(config_blob_skips),
                "secret_blobs_pushed": int(secret_blob_counts.get("pushed") or 0),
                "secret_blobs_total": int(secret_blob_counts.get("total") or 0),
                "api_blobs_skipped": len(api_blob_skips),
                "config_hash_refs": int((counts or {}).get("config_hash_refs") or 0),
                "api_payload_hash_refs": int((counts or {}).get("api_payload_hash_refs") or 0),
                "secret_blob_hash_refs": int((counts or {}).get("secret_blob_hash_refs") or 0),
            },
            "config_blob_skips": config_blob_skips,
            "api_blob_skips": api_blob_skips,
            "message": "Config/API blobs and missing local operations were pushed. Remote materialized state was not rebuilt yet.",
        }

    report_progress({
        "phase": "rebuilding",
        "done": len(pushed),
        "total": total_missing,
        "remaining": remaining_after_batch,
    })
    rebuild_command = _cluster_remote_command(remote_dir, local_node_id, "rebuild")
    rebuild_result = await pool.run(hostname, rebuild_command, timeout=30)
    if rebuild_result is None:
        raise HTTPException(status_code=502, detail="Remote host became unreachable before rebuild")
    if int(getattr(rebuild_result, "exit_status", 1) or 0) != 0:
        error = _probe_error_text(rebuild_result)
        _log(SERVICE, f"Remote cluster rebuild failed on {hostname}: {error}", level="WARNING")
        raise HTTPException(status_code=409, detail=error)
    rebuild_payload = _parse_remote_json_result(rebuild_result, "Remote rebuild")
    result = {
        "ok": True,
        "node_id": node_id,
        "hostname": hostname,
        "pushed": pushed,
        "rebuild": rebuild_payload,
        "counts": {
            "pushed": len(pushed),
            "rebuilt": 1,
            "local_ops_remaining": remaining_after_batch,
            "total_missing_before": total_missing,
            "config_blobs_pushed": int(blob_counts.get("pushed") or 0),
            "config_blobs_total": int(blob_counts.get("total") or 0),
            "config_blobs_skipped": len(config_blob_skips),
            "secret_blobs_pushed": int(secret_blob_counts.get("pushed") or 0),
            "secret_blobs_total": int(secret_blob_counts.get("total") or 0),
            "api_blobs_skipped": len(api_blob_skips),
            "config_hash_refs": int((counts or {}).get("config_hash_refs") or 0),
            "api_payload_hash_refs": int((counts or {}).get("api_payload_hash_refs") or 0),
            "secret_blob_hash_refs": int((counts or {}).get("secret_blob_hash_refs") or 0),
        },
        "config_blob_skips": config_blob_skips,
        "api_blob_skips": api_blob_skips,
        "message": "Config/API blobs and missing local operations were pushed, then remote materialized state was rebuilt. API-key files were not installed in this phase.",
    }
    report_progress({"phase": "done", "done": len(pushed), "total": total_missing, "remaining": remaining_after_batch})
    return result


async def _run_remote_push_job(job_id: str, node: dict[str, Any], identity: dict[str, Any]) -> None:
    """Run one remote operation push in the background and update local progress."""

    def progress(update: dict[str, Any]) -> None:
        _update_remote_push_job(job_id, status="running", **update)

    _update_remote_push_job(job_id, status="running", phase="starting", done=0, total=0, remaining=0, error="")
    try:
        result = await _push_missing_operations_to_remote(node, identity, progress_callback=progress)
    except HTTPException as exc:
        _update_remote_push_job(
            job_id,
            status="error",
            phase="error",
            error=str(exc.detail),
            result={"status_code": exc.status_code, "detail": exc.detail},
        )
        return
    except Exception as exc:
        _log(SERVICE, f"Remote cluster push job failed: {exc}", level="ERROR")
        _update_remote_push_job(job_id, status="error", phase="error", error=str(exc))
        return

    counts = result.get("counts") if isinstance(result, dict) else {}
    pushed = int((counts or {}).get("pushed") or 0)
    total = int((counts or {}).get("total_missing_before") or pushed)
    _update_remote_push_job(
        job_id,
        status="done",
        phase="done",
        done=pushed,
        total=max(total, pushed),
        remaining=0,
        error="",
        result=result,
    )


async def _require_remote_state_current(
    node: dict[str, Any],
    identity: dict[str, Any],
    local_materialized: dict[str, Any],
) -> None:
    """Block remote writes unless remote state matches local materialized state."""

    remote_vector_payload = await _run_remote_read_command(node, identity, "get-state-vector")
    local_vector = _as_state_vector(local_materialized.get("state_vector") or {})
    remote_vector = _as_state_vector(remote_vector_payload.get("state_vector") or {})
    if local_vector != remote_vector:
        raise HTTPException(status_code=409, detail="Remote state is not synchronized; push or pull operations before materializing configs")
    remote_desired_payload = await _run_remote_read_command(node, identity, "get-desired-state")
    local_desired = local_materialized.get("desired_state") if isinstance(local_materialized, dict) else {}
    remote_desired = remote_desired_payload.get("desired_state") if isinstance(remote_desired_payload, dict) else {}
    if (local_desired if isinstance(local_desired, dict) else {}) != (remote_desired if isinstance(remote_desired, dict) else {}):
        raise HTTPException(status_code=409, detail="Remote desired state differs; rebuild or resync before materializing configs")


async def _build_remote_preview(node: dict[str, Any], identity: dict[str, Any], local_materialized: dict[str, Any]) -> dict[str, Any]:
    """Read remote state and compare it with the local materialized state."""

    remote_vector_payload = await _run_remote_read_command(node, identity, "get-state-vector")
    remote_desired_payload = await _run_remote_read_command(node, identity, "get-desired-state")
    local_vector = _as_state_vector(local_materialized.get("state_vector") or {})
    remote_vector = _as_state_vector(remote_vector_payload.get("state_vector") or {})
    local_desired = local_materialized.get("desired_state") if isinstance(local_materialized, dict) else {}
    remote_desired = remote_desired_payload.get("desired_state") if isinstance(remote_desired_payload, dict) else {}
    local_desired = local_desired if isinstance(local_desired, dict) else {}
    remote_desired = remote_desired if isinstance(remote_desired, dict) else {}
    node_id = str(node.get("node_id") or "")
    hostname = str(node.get("pbname") or node.get("hostname") or "")
    try:
        materialization = await _run_remote_materialize_command(node, identity, "materialize-v7-preview")
    except HTTPException as exc:
        materialization = {
            "ok": False,
            "read_only": True,
            "can_apply": False,
            "counts": {},
            "items": [],
            "error": str(exc.detail),
        }
    try:
        api_key_materialization = await _run_remote_materialize_command(node, identity, "materialize-api-keys-preview")
    except HTTPException as exc:
        api_key_materialization = {
            "ok": False,
            "read_only": True,
            "can_apply": False,
            "counts": {},
            "error": str(exc.detail),
        }
    return {
        "ok": True,
        "read_only": True,
        "node_id": node_id,
        "hostname": hostname,
        "remote_cluster_id": str(remote_desired.get("cluster_id") or remote_desired_payload.get("cluster_id") or ""),
        "remote_node_id": str(remote_desired_payload.get("node_id") or remote_vector_payload.get("node_id") or node_id),
        "state_vector": _compare_state_vectors(local_vector, remote_vector),
        "desired_state": _compare_desired_states(local_desired, remote_desired),
        "operation_sync": _build_operation_sync_preview(
            load_operations(_cluster_root(), expected_cluster_id=str(identity.get("cluster_id") or "")),
            remote_vector,
        ),
        "materialization": materialization,
        "api_key_materialization": api_key_materialization,
    }


def _current_instance_state(desired_state: dict[str, Any], name: str) -> dict[str, Any]:
    """Return current desired-state entry for *name*, or an empty dict."""

    instances = desired_state.get("instances") if isinstance(desired_state, dict) else None
    if not isinstance(instances, dict):
        return {}
    current = instances.get(name)
    return dict(current) if isinstance(current, dict) else {}


def _is_tombstoned(desired_state: dict[str, Any], name: str) -> bool:
    """Return True when desired_state contains a tombstone for *name*."""

    tombstones = desired_state.get("tombstones") if isinstance(desired_state, dict) else None
    return isinstance(tombstones, dict) and name in tombstones


def _build_bootstrap_plan() -> dict[str, Any]:
    """Preview local cluster bootstrap items that can be written into the oplog."""

    snapshot = _load_cluster_snapshot()
    identity = snapshot["identity"]
    cluster_nodes = snapshot["cluster_nodes"]
    desired_state = snapshot["desired_state"]
    run_root = _run_v7_root()
    host_node_ids = _read_host_node_ids()
    items: list[dict[str, Any]] = []
    counts: dict[str, int] = {"add": 0, "update": 0, "skip": 0, "blocked_tombstone": 0, "error": 0}

    for vps_config in _known_vps_configs():
        hostname = str(vps_config.get("hostname") or "").strip()
        item: dict[str, Any] = {"type": "node", "hostname": hostname, "config_path": vps_config.get("config_path")}
        try:
            _validate_instance_name(hostname)
            node_id, current = _current_node_for_host(cluster_nodes, hostname, host_node_ids)
            mapping = host_node_ids.get(hostname)
            mapped_node_id = str(mapping.get("node_id") or "") if isinstance(mapping, dict) else ""
            mapping_matches = bool(node_id) and mapped_node_id == node_id
            monitor_role = str(vps_config.get("role") or "").strip()
            current_role = str(current.get("role") or "").strip() if current else ""
            node_role = _cluster_role_from_monitor_role(monitor_role or current_role or "vps")
            desired_config = dict(vps_config)
            desired_config["role"] = node_role
            desired_config["sync_mode"] = normalize_node_sync_mode(current) if current else "disabled"
            desired_config["sync_enabled"] = desired_config["sync_mode"] != "disabled"
            desired_payload = _node_payload_from_vps_config(node_id or "pending", desired_config)
            item.update({
                "node_id": node_id,
                "pbname": hostname,
                "node_role": node_role,
                "sync_mode": desired_payload.get("sync_mode") or "disabled",
                "sync_enabled": desired_payload.get("sync_enabled") is not False,
                "ssh_host": vps_config.get("ssh_host") or "",
                "ssh_user": vps_config.get("ssh_user") or "",
                "ssh_port": vps_config.get("ssh_port") or 22,
                "remote_pbgui_dir": vps_config.get("remote_pbgui_dir") or "",
                "will_create_node_mapping": not mapping_matches,
            })
            if not current:
                item.update({"action": "add", "reason": "VPS host is not present in cluster nodes"})
            elif not mapping_matches:
                item.update({"action": "update", "reason": "VPS host mapping is missing"})
            elif _node_metadata_matches(current, desired_payload):
                item.update({"action": "skip", "reason": "VPS node already registered"})
            else:
                item.update({"action": "update", "reason": "VPS node metadata differs"})
            counts[item["action"]] = counts.get(item["action"], 0) + 1
        except Exception as exc:
            item.update({"action": "error", "reason": str(exc)})
            counts["error"] += 1
        items.append(item)

    if not run_root.is_dir():
        return {
            "run_v7_root": str(run_root),
            "counts": counts,
            "items": items,
            "can_apply": bool(counts.get("add") or counts.get("update")),
            "message": "No local V7 instance directory exists.",
        }

    for instance_dir in sorted(path for path in run_root.iterdir() if path.is_dir()):
        name = instance_dir.name
        config_path = instance_dir / "config.json"
        item: dict[str, Any] = {"type": "instance", "instance": name, "path": str(instance_dir)}
        try:
            _validate_instance_name(name)
            if not config_path.is_file():
                item.update({"action": "error", "reason": "missing config.json"})
                counts["error"] += 1
                items.append(item)
                continue
            cfg = load_pb7_config(config_path, neutralize_added=False)
            pbgui = cfg.get("pbgui", {}) if isinstance(cfg, dict) else {}
            pbgui = pbgui if isinstance(pbgui, dict) else {}
            enabled_on = str(pbgui.get("enabled_on") or "disabled").strip() or "disabled"
            version = str(pbgui.get("version", 0))
            desired = "running" if enabled_on != "disabled" else "stopped"
            manifest = build_config_manifest(instance_dir)
            manifest_hash = compute_config_manifest_hash(manifest)
            assignment = _resolve_bootstrap_assignment(identity, enabled_on, host_node_ids)
            current = _current_instance_state(desired_state, name)
            item.update({
                "version": version,
                "enabled_on": enabled_on,
                "desired_state": desired,
                "config_manifest_hash": manifest_hash,
                "assigned_host": assignment["assigned_host"],
                "assigned_label": assignment["assigned_label"],
                "assigned_role": assignment["assigned_role"],
                "will_create_node_mapping": assignment["will_create_node_mapping"],
                "current_version": str(current.get("version") or "") if current else "",
                "current_manifest_hash": str(current.get("config_manifest_hash") or "") if current else "",
            })
            if _is_tombstoned(desired_state, name):
                item.update({"action": "blocked_tombstone", "reason": "instance is tombstoned; restore explicitly instead"})
            elif not current:
                item.update({"action": "add", "reason": "not present in desired state"})
            elif current.get("conflicted") is True:
                item.update({"action": "error", "reason": "desired state is conflicted"})
            else:
                current_assigned = str(current.get("assigned_host") or "")
                assignment_matches = bool(assignment["assigned_host"]) and current_assigned == assignment["assigned_host"]
                same_state = (
                    str(current.get("version") or "") == version
                    and str(current.get("desired_state") or "") == desired
                    and str(current.get("config_manifest_hash") or "") == manifest_hash
                    and assignment_matches
                )
                item.update({
                    "action": "skip" if same_state else "update",
                    "reason": "already matches desired state" if same_state else "local config differs from desired state",
                })
            counts[item["action"]] = counts.get(item["action"], 0) + 1
        except Exception as exc:
            item.update({"action": "error", "reason": str(exc)})
            counts["error"] += 1
        items.append(item)

    return {
        "run_v7_root": str(run_root),
        "counts": counts,
        "items": items,
        "can_apply": bool(counts.get("add") or counts.get("update")),
        "message": "Bootstrap writes ADD_NODE for known VPS hosts and UPSERT_CONFIG for local configs. It never infers deletes.",
    }


def _apply_bootstrap_plan(plan: dict[str, Any]) -> dict[str, Any]:
    """Apply add/update items from a previously generated bootstrap plan."""

    from api import v7_instances

    applied: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    host_node_ids = _read_host_node_ids()
    for item in plan.get("items", []):
        action = str(item.get("action") or "")
        item_type = str(item.get("type") or "instance")
        name = str(item.get("instance") or item.get("hostname") or "")
        if action not in {"add", "update"}:
            skipped.append({"type": item_type, "name": name, "action": action, "reason": item.get("reason") or ""})
            continue
        try:
            if item_type == "node":
                _validate_instance_name(name)
                node_id = str(item.get("node_id") or "") or generate_node_id()
                entry = host_node_ids.setdefault(name, {})
                entry.update({
                    "node_id": node_id,
                    "role": _cluster_role_from_monitor_role(item.get("node_role")),
                })
                entry.setdefault("created_at", int(time.time()))
                _write_host_node_ids(host_node_ids)
                append_operation(
                    _cluster_root(),
                    "ADD_NODE",
                    _node_payload_from_vps_config(node_id, {
                        "hostname": name,
                        "role": item.get("node_role") or "vps",
                        "sync_mode": item.get("sync_mode") or "disabled",
                        "ssh_host": item.get("ssh_host") or "",
                        "ssh_user": item.get("ssh_user") or "",
                        "ssh_port": item.get("ssh_port") or 22,
                        "remote_pbgui_dir": item.get("remote_pbgui_dir") or "",
                    }),
                )
                applied.append({"type": item_type, "name": name, "action": action})
            else:
                _validate_instance_name(name)
                instance_dir = _run_v7_root() / name
                cfg = load_pb7_config(instance_dir / "config.json", neutralize_added=False)
                parent_version = item.get("current_version") or None
                v7_instances._record_cluster_config_upsert(name, instance_dir, cfg, parent_version=parent_version)
                applied.append({"type": item_type, "name": name, "action": action})
        except Exception as exc:
            failed.append({"type": item_type, "name": name, "action": action, "reason": str(exc)})
    if applied:
        rebuild_materialized_state(_cluster_root())
    return {
        "applied": applied,
        "skipped": skipped,
        "failed": failed,
        "counts": {
            "applied": len(applied),
            "skipped": len(skipped),
            "failed": len(failed),
        },
    }


@router.get("/status")
def get_status(session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Return a compact read-only Cluster Sync status summary."""

    snapshot = _load_cluster_snapshot()
    cluster_nodes = snapshot["cluster_nodes"]
    desired_state = snapshot["desired_state"]
    nodes = _node_list(cluster_nodes)
    instances = _instance_list(desired_state)
    tombstones = _tombstone_list(desired_state)
    conflict_count = sum(1 for item in instances if item.get("conflicted") is True)
    warnings: list[str] = []
    if not nodes:
        warnings.append("No cluster node membership operation has been recorded yet.")
    if conflict_count:
        warnings.append(f"{conflict_count} V7 instance conflict(s) need review.")

    return {
        "read_only": True,
        "cluster_root": snapshot["cluster_root"],
        "identity": snapshot["identity"],
        "local_node": (cluster_nodes.get("nodes") or {}).get(snapshot["identity"].get("node_id")),
        "generation": int(cluster_nodes.get("generation") or 0),
        "generated_at": int(desired_state.get("generated_at") or 0),
        "counts": {
            "nodes": len(nodes),
            "instances": len(instances),
            "conflicts": conflict_count,
            "tombstones": len(tombstones),
            "oplog": int(cluster_nodes.get("generation") or 0),
        },
        "api_keys": desired_state.get("api_keys"),
        "warnings": warnings,
    }


@router.get("/nodes")
def get_nodes(session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Return materialized Cluster Sync node records."""

    snapshot = _load_cluster_snapshot()
    cluster_nodes = snapshot["cluster_nodes"]
    return {
        "cluster_nodes": cluster_nodes,
        "nodes": _node_list(cluster_nodes),
    }


@router.post("/nodes/{node_id}/sync")
def set_node_sync(
    node_id: str,
    sync_enabled: Annotated[bool, Query()],
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """Enable or disable automatic Cluster Sync for one known node."""

    snapshot = _load_cluster_snapshot()
    nodes = _node_list(snapshot["cluster_nodes"])
    node = _node_for_id(nodes, str(node_id or ""))
    if not node:
        raise HTTPException(status_code=404, detail="Cluster node not found")
    local_node_id = str(snapshot["identity"].get("node_id") or "")
    if str(node.get("node_id") or "") == local_node_id and not sync_enabled:
        raise HTTPException(status_code=400, detail="Local cluster node sync cannot be disabled")
    if node.get("enabled") is False and sync_enabled:
        raise HTTPException(status_code=400, detail="Disabled cluster nodes cannot be enabled for sync")
    current = node.get("sync_enabled", True) is not False
    if current == bool(sync_enabled):
        return {"ok": True, "changed": False, "node": node}

    append_operation(
        _cluster_root(),
        "UPDATE_NODE",
        {
            "node_id": str(node.get("node_id") or ""),
            "sync_enabled": bool(sync_enabled),
            "sync_mode": "reachable" if sync_enabled and str(node.get("ssh_host") or "").strip() else ("outbound_only" if sync_enabled else "disabled"),
        },
    )
    materialized = rebuild_materialized_state(_cluster_root())
    updated = _node_for_id(_node_list(materialized["cluster_nodes"]), str(node.get("node_id") or ""))
    return {"ok": True, "changed": True, "node": updated}


@router.post("/nodes/{node_id}/settings")
async def update_node_settings(
    node_id: str,
    request: Request,
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """Update editable Cluster Sync settings for one known node."""

    try:
        payload = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Request body must be valid JSON") from exc
    settings = _validate_node_sync_settings(payload)
    snapshot = _load_cluster_snapshot()
    nodes = _node_list(snapshot["cluster_nodes"])
    node = _node_for_id(nodes, str(node_id or ""))
    if not node:
        raise HTTPException(status_code=404, detail="Cluster node not found")
    local_node_id = str(snapshot["identity"].get("node_id") or "")
    if str(node.get("node_id") or "") == local_node_id and settings["sync_mode"] == "disabled":
        raise HTTPException(status_code=400, detail="Local cluster node sync cannot be disabled")
    if node.get("enabled") is False and settings["sync_mode"] != "disabled":
        raise HTTPException(status_code=400, detail="Disabled cluster nodes cannot be enabled for sync")

    current = {
        "sync_mode": normalize_node_sync_mode(node),
        "sync_enabled": normalize_node_sync_mode(node) != "disabled",
        "ssh_host": str(node.get("ssh_host") or ""),
        "ssh_user": str(node.get("ssh_user") or ""),
        "ssh_port": int(node.get("ssh_port") or 22),
    }
    if current == settings:
        return {"ok": True, "changed": False, "node": node}

    append_operation(
        _cluster_root(),
        "UPDATE_NODE",
        {
            "node_id": str(node.get("node_id") or ""),
            **settings,
        },
    )
    materialized = rebuild_materialized_state(_cluster_root())
    updated = _node_for_id(_node_list(materialized["cluster_nodes"]), str(node.get("node_id") or ""))
    return {"ok": True, "changed": True, "node": updated}


@router.get("/remote-status")
async def get_remote_status(session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Run read-only Cluster Sync hello probes against materialized nodes."""

    snapshot = _load_cluster_snapshot()
    nodes = _node_list(snapshot["cluster_nodes"])
    probes = await _probe_cluster_nodes(nodes, snapshot["identity"])
    return {
        "count": len(probes),
        "probes": probes,
    }


@router.post("/remote-join/{node_id}")
async def join_remote_identity(node_id: str, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Write a known cluster identity to one uninitialized remote node."""

    snapshot = _load_cluster_snapshot()
    nodes = _node_list(snapshot["cluster_nodes"])
    node = _node_for_id(nodes, str(node_id or ""))
    if not node:
        raise HTTPException(status_code=404, detail="Cluster node not found")
    result = await _run_remote_join(node, snapshot["identity"])
    return result


@router.get("/remote-preview/{node_id}")
async def get_remote_preview(node_id: str, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Read remote cluster state and compare it with local state without writing."""

    snapshot = _load_cluster_snapshot()
    nodes = _node_list(snapshot["cluster_nodes"])
    node = _node_for_id(nodes, str(node_id or ""))
    if not node:
        raise HTTPException(status_code=404, detail="Cluster node not found")
    return await _build_remote_preview(node, snapshot["identity"], snapshot)


@router.post("/remote-push-ops/{node_id}/start")
async def start_remote_push_operations(node_id: str, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Start a background remote operation push and return its local progress job."""

    snapshot = _load_cluster_snapshot()
    nodes = _node_list(snapshot["cluster_nodes"])
    node = _node_for_id(nodes, str(node_id or ""))
    if not node:
        raise HTTPException(status_code=404, detail="Cluster node not found")
    job = _create_remote_push_job(node)
    asyncio.create_task(_run_remote_push_job(str(job["job_id"]), node, dict(snapshot["identity"])))
    return _public_remote_push_job(job)


@router.get("/remote-push-jobs/{job_id}")
def get_remote_push_job(job_id: str, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Return local progress for one background remote operation push."""

    _prune_remote_push_jobs()
    job = _REMOTE_PUSH_JOBS.get(str(job_id or ""))
    if not job:
        raise HTTPException(status_code=404, detail="Remote push job not found")
    return _public_remote_push_job(job)


@router.post("/remote-push-ops/{node_id}")
async def push_remote_operations(
    node_id: str,
    limit: Annotated[int | None, Query(ge=1, le=100)] = None,
    rebuild: Annotated[bool, Query()] = True,
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """Push missing local operations to one remote node and rebuild remote state."""

    snapshot = _load_cluster_snapshot()
    nodes = _node_list(snapshot["cluster_nodes"])
    node = _node_for_id(nodes, str(node_id or ""))
    if not node:
        raise HTTPException(status_code=404, detail="Cluster node not found")
    return await _push_missing_operations_to_remote(node, snapshot["identity"], limit=limit, rebuild=rebuild)


@router.post("/remote-materialize-v7/{node_id}")
async def materialize_remote_v7_configs(node_id: str, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Materialize assigned V7 config blobs on one synchronized remote node."""

    snapshot = _load_cluster_snapshot()
    nodes = _node_list(snapshot["cluster_nodes"])
    node = _node_for_id(nodes, str(node_id or ""))
    if not node:
        raise HTTPException(status_code=404, detail="Cluster node not found")
    await _require_remote_state_current(node, snapshot["identity"], snapshot)
    result = await _run_remote_materialize_command(node, snapshot["identity"], "materialize-v7", timeout=120)
    return {
        "ok": True,
        "node_id": str(node.get("node_id") or ""),
        "hostname": str(node.get("pbname") or node.get("hostname") or ""),
        "materialization": result,
        "message": "Remote V7 configs materialized. No files were deleted and no bots were started or stopped.",
    }


@router.post("/remote-materialize-api-keys/{node_id}")
async def materialize_remote_api_keys(node_id: str, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Materialize api-keys.json from the replicated secret blob on one synchronized remote node."""

    snapshot = _load_cluster_snapshot()
    nodes = _node_list(snapshot["cluster_nodes"])
    node = _node_for_id(nodes, str(node_id or ""))
    if not node:
        raise HTTPException(status_code=404, detail="Cluster node not found")
    await _require_remote_state_current(node, snapshot["identity"], snapshot)
    result = await _run_remote_materialize_command(node, snapshot["identity"], "materialize-api-keys", timeout=60)
    return {
        "ok": True,
        "node_id": str(node.get("node_id") or ""),
        "hostname": str(node.get("pbname") or node.get("hostname") or ""),
        "materialization": result,
        "message": "Remote api-keys.json materialized. No bots were restarted.",
    }


@router.get("/desired-state")
def get_desired_state(session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Return materialized V7 desired state and tombstones."""

    snapshot = _load_cluster_snapshot()
    desired_state = snapshot["desired_state"]
    return {
        "desired_state": desired_state,
        "instances": _instance_list(desired_state),
        "tombstones": _tombstone_list(desired_state),
    }


@router.get("/oplog")
def get_oplog(
    limit: int = Query(50, ge=1, le=500),
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """Return recent local cluster operations, newest first."""

    snapshot = _load_cluster_snapshot()
    root = Path(snapshot["cluster_root"])
    cluster_id = str(snapshot["identity"].get("cluster_id") or "")
    try:
        operations = load_operations(root, expected_cluster_id=cluster_id)
    except ClusterStateError as exc:
        _log(SERVICE, f"Failed to load cluster oplog: {exc}", level="ERROR")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    operations.sort(key=lambda item: (int(item.get("created_at") or 0), str(item.get("op_id") or "")), reverse=True)
    return {
        "count": len(operations),
        "operations": operations[:limit],
    }


@router.get("/bootstrap-preview")
def get_bootstrap_preview(session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Preview local V7 configs that are missing or stale in desired state."""

    try:
        return _build_bootstrap_plan()
    except HTTPException:
        raise
    except Exception as exc:
        _log(SERVICE, f"Failed to build cluster bootstrap preview: {exc}", level="ERROR")
        raise HTTPException(status_code=500, detail="Failed to build bootstrap preview") from exc


@router.post("/bootstrap")
def apply_bootstrap(session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Write local V7 configs into the cluster oplog as explicit UPSERT_CONFIG ops."""

    try:
        plan = _build_bootstrap_plan()
        result = _apply_bootstrap_plan(plan)
        return {
            "ok": result["counts"]["failed"] == 0,
            "before": plan,
            "result": result,
            "after": _build_bootstrap_plan(),
        }
    except HTTPException:
        raise
    except Exception as exc:
        _log(SERVICE, f"Failed to apply cluster bootstrap: {exc}", level="ERROR")
        raise HTTPException(status_code=500, detail="Failed to apply bootstrap") from exc


@router.get("/main_page", response_class=HTMLResponse)
def get_main_page(
    request: Request,
    session: SessionToken = Depends(require_auth),
) -> HTMLResponse:
    """Serve the standalone Cluster Sync status page."""

    html_path = Path(__file__).parent.parent / "frontend" / "cluster.html"
    html = html_path.read_text(encoding="utf-8")

    scheme = request.url.scheme
    host = request.url.hostname or "127.0.0.1"
    port = request.url.port
    origin = f"{scheme}://{host}" + (f":{port}" if port else "")
    api_base = origin + "/api/cluster"
    ws_base = origin.replace("http://", "ws://").replace("https://", "wss://")

    html = html.replace('"%%TOKEN%%"', json.dumps(session.token))
    html = html.replace('"%%API_BASE%%"', json.dumps(api_base))
    html = html.replace('"%%WS_BASE%%"', json.dumps(ws_base))

    from pbgui_purefunc import PBGUI_SERIAL, PBGUI_VERSION

    html = html.replace('"%%VERSION%%"', json.dumps(PBGUI_VERSION))
    html = html.replace("%%VERSION%%", PBGUI_VERSION)
    html = html.replace('"%%SERIAL%%"', json.dumps(PBGUI_SERIAL))
    html = html.replace("%%SERIAL%%", PBGUI_SERIAL)

    nav_js = Path(__file__).parent.parent / "frontend" / "pbgui_nav.js"
    nav_hash = str(int(nav_js.stat().st_mtime)) if nav_js.exists() else PBGUI_VERSION
    html = html.replace("%%NAV_HASH%%", nav_hash)

    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})
