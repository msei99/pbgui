"""FastAPI router for read-only Cluster Sync status."""

from __future__ import annotations

import asyncio
import configparser
import json
import platform
import shlex
import time
import uuid
from pathlib import Path
from typing import Any

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
    rebuild_materialized_state,
)
from pb7_config import load_pb7_config
from pbgui_purefunc import PBGDIR

SERVICE = "Cluster"

router = APIRouter()


def _cluster_root() -> Path:
    """Return the local cluster state root for this PBGui install."""

    return default_cluster_root(Path(PBGDIR))


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
            "role": host_roles.get(hostname, "vps"),
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
    payload: dict[str, Any] = {
        "node_id": node_id,
        "role": role,
        "pbname": hostname,
        "hostname": hostname,
        "sync_enabled": True,
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

    for key, value in desired.items():
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


def _cluster_hello_command(remote_pbgui_dir: str | None, local_node_id: str) -> str:
    """Build the read-only remote Cluster Sync hello command."""

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
        f"--remote-node {local_node} --allow-join hello"
    )


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


async def _probe_cluster_node(node: dict[str, Any], identity: dict[str, Any]) -> dict[str, Any]:
    """Run a read-only restricted hello probe against one remote node."""

    node_id = str(node.get("node_id") or "")
    hostname = str(node.get("pbname") or node.get("hostname") or "")
    local_node_id = str(identity.get("node_id") or "")
    if not node_id or node_id == local_node_id:
        return {"node_id": node_id, "hostname": hostname, "status": "local", "ok": True}
    if node.get("enabled") is False or node.get("sync_enabled") is False:
        return {"node_id": node_id, "hostname": hostname, "status": "disabled", "ok": False}
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
        node_role = _cluster_role_from_monitor_role(vps_config.get("role"))
        item: dict[str, Any] = {"type": "node", "node_role": node_role, "hostname": hostname, "config_path": vps_config.get("config_path")}
        try:
            _validate_instance_name(hostname)
            node_id, current = _current_node_for_host(cluster_nodes, hostname, host_node_ids)
            mapping = host_node_ids.get(hostname)
            mapped_node_id = str(mapping.get("node_id") or "") if isinstance(mapping, dict) else ""
            mapping_matches = bool(node_id) and mapped_node_id == node_id
            desired_payload = _node_payload_from_vps_config(node_id or "pending", vps_config)
            item.update({
                "node_id": node_id,
                "pbname": hostname,
                "node_role": node_role,
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
