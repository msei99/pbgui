"""FastAPI router for read-only Cluster Sync status."""

from __future__ import annotations

import asyncio
import base64
import binascii
import configparser
import getpass
import hashlib
import ipaddress
import json
import os
import platform
import shlex
import socket
import subprocess
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace
from typing import Annotated, Any

import asyncssh
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse

from api.auth import SessionToken, require_auth
from api.vps import get_monitor, get_monitor_state_snapshot
from cluster_sync_command import _materialize_v7_configs
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
    validate_operation,
    write_operation,
)
from master.cluster_ssh_keys import ensure_local_cluster_ssh_material, install_authorized_cluster_key, public_key_fingerprint
from pb7_config import load_pb7_config
from pbgui_purefunc import PBGDIR

SERVICE = "Cluster"

router = APIRouter()

_REMOTE_PUSH_JOBS: dict[str, dict[str, Any]] = {}
_SELF_JOIN_JOBS: dict[str, dict[str, Any]] = {}
_REPAIR_ALL_SSH_JOBS: dict[str, dict[str, Any]] = {}
_REMOTE_PUSH_JOB_TTL_SECONDS = 3600
_REMOTE_PUSH_ACTIVE_STATES = frozenset({"queued", "running"})
_SELF_JOIN_ACTIVE_STATES = frozenset({"queued", "running"})
_REPAIR_ALL_SSH_ACTIVE_STATES = frozenset({"queued", "running"})
_CONFIG_BLOB_BATCH_TARGET_BYTES = 12 * 1024 * 1024
_EDITABLE_NODE_SYNC_MODES = frozenset({"disabled", "outbound_only", "reachable"})


class _SelfJoinPasswordSSHRunner:
    """Short-lived SSH runner for self-join password authentication."""

    def __init__(self, *, hostname: str, ssh_host: str, ssh_user: str, ssh_port: int, ssh_password: str) -> None:
        self.hostname = str(hostname or "")
        self.ssh_host = str(ssh_host or self.hostname or "")
        self.ssh_user = str(ssh_user or "")
        self.ssh_port = int(ssh_port or 22)
        self._ssh_password = str(ssh_password or "")
        self._conn: asyncssh.SSHClientConnection | None = None

    def _is_alive(self) -> bool:
        if self._conn is None:
            return False
        transport = getattr(self._conn, "_transport", None)
        return bool(transport is not None and not transport.is_closing())

    async def _connect(self) -> asyncssh.SSHClientConnection:
        if self._is_alive() and self._conn is not None:
            return self._conn
        if not self.ssh_host:
            raise ConnectionError("SSH host is required for self-join password login.")
        if not self.ssh_user:
            raise ConnectionError("SSH user is required for self-join password login.")
        try:
            self._conn = await asyncio.wait_for(
                asyncssh.connect(
                    host=self.ssh_host,
                    port=self.ssh_port,
                    username=self.ssh_user,
                    password=self._ssh_password,
                    known_hosts=None,
                    keepalive_interval=10,
                ),
                timeout=10,
            )
        except asyncssh.PermissionDenied as exc:
            raise ConnectionError("SSH authentication failed for upstream master.") from exc
        except asyncio.TimeoutError as exc:
            raise ConnectionError("SSH connection to upstream master timed out.") from exc
        except Exception as exc:
            raise ConnectionError(f"SSH connection to upstream master failed: {exc}") from exc
        return self._conn

    async def run(self, hostname: str, command: str, timeout: int | None = 30, check: bool = False) -> Any:
        del hostname
        conn = await self._connect()
        try:
            task = conn.run(command, check=check)
            return await asyncio.wait_for(task, timeout=timeout) if timeout is not None else await task
        except asyncssh.ProcessError:
            raise
        except asyncio.TimeoutError as exc:
            raise TimeoutError("SSH command on upstream master timed out.") from exc

    async def start_process(self, hostname: str, command: str) -> Any:
        del hostname
        conn = await self._connect()
        return await conn.create_process(command)

    async def close(self) -> None:
        conn = self._conn
        self._conn = None
        if conn is None:
            return
        conn.close()
        wait_closed = getattr(conn, "wait_closed", None)
        if callable(wait_closed):
            try:
                await wait_closed()
            except Exception:
                pass


def _cluster_root() -> Path:
    """Return the local cluster state root for this PBGui install."""

    return default_cluster_root(Path(PBGDIR))


def _request_pbcluster_sync(root: Path | None = None) -> None:
    """Best-effort notification for PBCluster to run an immediate sync pass."""

    try:
        cluster_root = Path(root) if root else _cluster_root()
        cluster_root.mkdir(parents=True, exist_ok=True)
        (cluster_root / "sync_request").touch()
    except OSError:
        pass


def _load_sync_status_summary(root: Path) -> dict[str, Any]:
    """Return the latest PBCluster sync status without failing the status page."""

    path = root / "sync_status.json"
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError, UnicodeDecodeError) as exc:
        return {
            "ok": False,
            "status": "unavailable",
            "reason": f"Failed to read sync_status.json: {exc}",
            "peers": [],
        }
    if not isinstance(payload, dict):
        return {
            "ok": False,
            "status": "unavailable",
            "reason": "sync_status.json is not an object",
            "peers": [],
        }

    def as_int(value: Any) -> int:
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0

    peers = []
    for item in payload.get("peers") if isinstance(payload.get("peers"), list) else []:
        if not isinstance(item, dict):
            continue
        peers.append({
            "node_id": str(item.get("node_id") or ""),
            "pbname": str(item.get("pbname") or ""),
            "ok": bool(item.get("ok", False)),
            "status": str(item.get("status") or ""),
            "reason": str(item.get("reason") or ""),
            "remote_node_id": str(item.get("remote_node_id") or ""),
            "last_seen": as_int(item.get("last_seen")),
            "next_retry": as_int(item.get("next_retry")),
            "retry_delay": as_int(item.get("retry_delay")),
        })
    return {
        "ok": bool(payload.get("ok", False)),
        "status": str(payload.get("status") or ""),
        "reason": str(payload.get("reason") or ""),
        "finished_at": as_int(payload.get("finished_at")),
        "peers_ok": as_int(payload.get("peers_ok")),
        "peers_total": as_int(payload.get("peers_total")) or len(peers),
        "peers": peers,
    }


def _local_pbgui_dir_value() -> str:
    """Return the local PBGui checkout path in the form used for Cluster SSH."""

    try:
        pbgui_dir = Path(PBGDIR).expanduser().resolve(strict=False)
    except OSError:
        pbgui_dir = Path(PBGDIR).expanduser().absolute()
    try:
        home_dir = Path.home().resolve(strict=False)
        relative = pbgui_dir.relative_to(home_dir)
    except (OSError, ValueError):
        return str(pbgui_dir)
    return relative.as_posix() if relative.parts else str(pbgui_dir)


def _local_ssh_user_value() -> str:
    """Return the current local login user for Cluster SSH metadata."""

    try:
        return getpass.getuser().strip()
    except Exception:
        return str(os.environ.get("USER") or os.environ.get("LOGNAME") or "").strip()


def _append_ip_candidate(candidates: list[str], value: Any) -> None:
    """Append one usable IPv4 address candidate if it is not already present."""

    text = str(value or "").strip()
    if not text:
        return
    try:
        address = ipaddress.ip_address(text)
    except ValueError:
        return
    if address.version != 4 or address.is_loopback or address.is_link_local or address.is_multicast or address.is_unspecified:
        return
    normalized = str(address)
    if normalized not in candidates:
        candidates.append(normalized)


def _source_ip_for_destination(destination_host: str, destination_port: int) -> str:
    """Return the local source IP the OS would use to reach one peer."""

    host = str(destination_host or "").strip()
    if not host:
        return ""
    try:
        port = int(destination_port or 22)
    except (TypeError, ValueError):
        port = 22
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect((host, port))
            return str(sock.getsockname()[0] or "")
    except OSError:
        return ""


def _local_ssh_host_value(destination_host: str | None = None, destination_port: int = 22) -> str:
    """Return the best detected local IPv4 address for Cluster SSH metadata."""

    candidates: list[str] = []
    _append_ip_candidate(candidates, _source_ip_for_destination(str(destination_host or ""), destination_port))
    try:
        result = subprocess.run(["hostname", "-I"], capture_output=True, check=False, text=True, timeout=2)
        for item in str(result.stdout or "").split():
            _append_ip_candidate(candidates, item)
    except Exception:
        pass
    try:
        _append_ip_candidate(candidates, _source_ip_for_destination("1.1.1.1", 53))
    except Exception:
        pass
    try:
        for item in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET, socket.SOCK_DGRAM):
            sockaddr = item[4]
            if sockaddr:
                _append_ip_candidate(candidates, sockaddr[0])
    except OSError:
        pass
    return candidates[0] if candidates else ""


def _node_with_local_defaults(node: dict[str, Any], local_node_id: str) -> dict[str, Any]:
    """Overlay local-only defaults onto a materialized node for API/UI use."""

    item = dict(node)
    if str(item.get("node_id") or "") != str(local_node_id or ""):
        return item
    if not str(item.get("remote_pbgui_dir") or "").strip():
        item["remote_pbgui_dir"] = _local_pbgui_dir_value()
    if not str(item.get("ssh_host") or "").strip():
        ssh_host = _local_ssh_host_value()
        if ssh_host:
            item["ssh_host"] = ssh_host
    if not str(item.get("ssh_user") or "").strip():
        ssh_user = _local_ssh_user_value()
        if ssh_user:
            item["ssh_user"] = ssh_user
    return item


def _nodes_with_local_defaults(nodes: list[dict[str, Any]], local_node_id: str) -> list[dict[str, Any]]:
    """Return node rows with the local PBGui path filled when missing."""

    return [_node_with_local_defaults(node, local_node_id) for node in nodes]


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


def _prune_self_join_jobs() -> None:
    """Forget stale self-join progress records."""

    cutoff = int(time.time()) - _REMOTE_PUSH_JOB_TTL_SECONDS
    for job_id, job in list(_SELF_JOIN_JOBS.items()):
        if int(job.get("updated_at") or 0) < cutoff:
            _SELF_JOIN_JOBS.pop(job_id, None)


def _public_self_join_job(job: dict[str, Any]) -> dict[str, Any]:
    """Return a JSON-safe view of one self-join progress record."""

    return {
        "job_id": str(job.get("job_id") or ""),
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


def _find_active_self_join_job() -> dict[str, Any] | None:
    """Return the active self-join job, if present."""

    _prune_self_join_jobs()
    for job in _SELF_JOIN_JOBS.values():
        if str(job.get("status") or "") in _SELF_JOIN_ACTIVE_STATES:
            return job
    return None


def _create_self_join_job(settings: dict[str, Any]) -> dict[str, Any]:
    """Create a new local progress record for one self-join job."""

    active = _find_active_self_join_job()
    if active:
        raise HTTPException(status_code=409, detail="Self-join is already running")
    now = int(time.time())
    job_id = uuid.uuid4().hex
    job = {
        "job_id": job_id,
        "hostname": str(settings.get("hostname") or ""),
        "status": "queued",
        "phase": "queued",
        "done": 0,
        "total": 9,
        "remaining": 9,
        "created_at": now,
        "updated_at": now,
        "error": "",
        "result": None,
    }
    _SELF_JOIN_JOBS[job_id] = job
    return job


def _update_self_join_job(job_id: str, **updates: Any) -> dict[str, Any]:
    """Update and return one local self-join progress record."""

    job = _SELF_JOIN_JOBS.get(str(job_id or ""))
    if not job:
        return {}
    job.update(updates)
    if "done" in job and "total" in job:
        job["remaining"] = max(0, int(job.get("total") or 0) - int(job.get("done") or 0))
    job["updated_at"] = int(time.time())
    return job


def _prune_repair_all_ssh_jobs() -> None:
    """Forget stale Repair All SSH progress records."""

    cutoff = int(time.time()) - _REMOTE_PUSH_JOB_TTL_SECONDS
    for job_id, job in list(_REPAIR_ALL_SSH_JOBS.items()):
        if int(job.get("updated_at") or 0) < cutoff:
            _REPAIR_ALL_SSH_JOBS.pop(job_id, None)


def _public_repair_all_ssh_job(job: dict[str, Any]) -> dict[str, Any]:
    """Return a JSON-safe view of one Repair All SSH progress record."""

    return {
        "job_id": str(job.get("job_id") or ""),
        "status": str(job.get("status") or "queued"),
        "phase": str(job.get("phase") or "queued"),
        "done": int(job.get("done") or 0),
        "total": int(job.get("total") or 0),
        "remaining": int(job.get("remaining") or 0),
        "current_node_id": str(job.get("current_node_id") or ""),
        "current_pbname": str(job.get("current_pbname") or ""),
        "created_at": int(job.get("created_at") or 0),
        "updated_at": int(job.get("updated_at") or 0),
        "error": str(job.get("error") or ""),
        "counts": job.get("counts") if isinstance(job.get("counts"), dict) else {},
        "result": job.get("result") if isinstance(job.get("result"), dict) else None,
    }


def _find_active_repair_all_ssh_job() -> dict[str, Any] | None:
    """Return the active Repair All SSH job, if present."""

    _prune_repair_all_ssh_jobs()
    for job in _REPAIR_ALL_SSH_JOBS.values():
        if str(job.get("status") or "") in _REPAIR_ALL_SSH_ACTIVE_STATES:
            return job
    return None


def _create_repair_all_ssh_job() -> dict[str, Any]:
    """Create a new local progress record for Repair All SSH."""

    active = _find_active_repair_all_ssh_job()
    if active:
        raise HTTPException(status_code=409, detail="Repair All SSH is already running")
    now = int(time.time())
    job_id = uuid.uuid4().hex
    job = {
        "job_id": job_id,
        "status": "queued",
        "phase": "queued",
        "done": 0,
        "total": 0,
        "remaining": 0,
        "current_node_id": "",
        "current_pbname": "",
        "created_at": now,
        "updated_at": now,
        "error": "",
        "counts": {},
        "result": None,
    }
    _REPAIR_ALL_SSH_JOBS[job_id] = job
    return job


def _update_repair_all_ssh_job(job_id: str, **updates: Any) -> dict[str, Any]:
    """Update and return one local Repair All SSH progress record."""

    job = _REPAIR_ALL_SSH_JOBS.get(str(job_id or ""))
    if not job:
        return {}
    job.update(updates)
    if "done" in job and "total" in job:
        job["remaining"] = max(0, int(job.get("total") or 0) - int(job.get("done") or 0))
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
    remote_pbgui_dir = str(payload.get("remote_pbgui_dir") or "").strip()
    if any(ord(ch) < 32 for ch in remote_pbgui_dir):
        raise HTTPException(status_code=400, detail="remote_pbgui_dir contains invalid control characters")
    sync_peers = _normalize_sync_peers(payload.get("sync_peers", []))
    return {
        "sync_mode": sync_mode,
        "sync_enabled": sync_mode != "disabled",
        "remote_pbgui_dir": remote_pbgui_dir,
        "ssh_host": ssh_host,
        "ssh_user": ssh_user,
        "ssh_port": ssh_port,
        "sync_peers": sync_peers,
    }


def _normalize_sync_peers(value: Any) -> list[str]:
    """Normalize a user-provided sync peer allowlist."""

    if value is None or value == "":
        return []
    if not isinstance(value, list):
        raise HTTPException(status_code=400, detail="sync_peers must be a list")
    peers: list[str] = []
    seen: set[str] = set()
    for item in value:
        node_id = str(item or "").strip()
        if not node_id:
            continue
        try:
            uuid.UUID(node_id.removeprefix("pbgui-node-"))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="sync_peers contains an invalid node id") from exc
        if node_id not in seen:
            peers.append(node_id)
            seen.add(node_id)
    return peers


def _validate_sync_peers_for_node(settings: dict[str, Any], node: dict[str, Any], nodes: list[dict[str, Any]]) -> None:
    """Validate sync_peers references against current cluster nodes."""

    node_id = str(node.get("node_id") or "")
    known = {str(item.get("node_id") or "") for item in nodes if str(item.get("node_id") or "")}
    for peer_id in settings.get("sync_peers") or []:
        if peer_id == node_id:
            raise HTTPException(status_code=400, detail="sync_peers cannot include the node itself")
        if peer_id not in known:
            raise HTTPException(status_code=400, detail=f"sync_peers contains unknown node {peer_id}")


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


def _cluster_ssh_setup_command(remote_pbgui_dir: str | None, args: list[str]) -> str:
    """Build a remote Cluster SSH setup helper command."""

    base = remote_shell_path(remote_pbgui_dir or "software/pbgui")
    quoted_args = " ".join(shlex.quote(str(item)) for item in args)
    return (
        f"base={base}; "
        "parent=\"${base%/*}\"; "
        "if [ -x \"$parent/venv_pbgui/bin/python\" ]; then py=\"$parent/venv_pbgui/bin/python\"; "
        "elif [ -x \"$parent/venv_pbgui312/bin/python\" ]; then py=\"$parent/venv_pbgui312/bin/python\"; "
        "elif [ -x \"$base/.venv/bin/python\" ]; then py=\"$base/.venv/bin/python\"; "
        "else py=python3; fi; "
        f"\"$py\" \"$base/cluster_ssh_setup.py\" {quoted_args}"
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


def _remote_pbrun_service_command(remote_pbgui_dir: str | None, action: str) -> str:
    """Build a remote command that controls PBRun without touching bot processes directly."""

    normalized = str(action or "").strip().lower()
    if normalized not in {"start", "stop"}:
        raise ValueError("unsupported PBRun action")
    starter_flag = "-s" if normalized == "start" else "-k"

    base = remote_shell_path(remote_pbgui_dir or "software/pbgui")
    return (
        f"base={base}; "
        "parent=\"${base%/*}\"; "
        "if [ -x \"$parent/venv_pbgui/bin/python\" ]; then py=\"$parent/venv_pbgui/bin/python\"; "
        "elif [ -x \"$parent/venv_pbgui312/bin/python\" ]; then py=\"$parent/venv_pbgui312/bin/python\"; "
        "elif [ -x \"$base/.venv/bin/python\" ]; then py=\"$base/.venv/bin/python\"; "
        "else py=python3; fi; "
        "if [ -x \"$base/setup/vps_service_control.sh\" ]; then "
        f"PBGUI_DIR=\"$base\" PBGUI_PYTHON=\"$py\" \"$base/setup/vps_service_control.sh\" {normalized} PBRun; "
        "elif command -v systemctl >/dev/null 2>&1 && "
        "XDG_RUNTIME_DIR=\"${XDG_RUNTIME_DIR:-/run/user/$(id -u)}\" systemctl --user show-environment >/dev/null 2>&1 && "
        "[ -f \"$HOME/.config/systemd/user/pbgui-pbrun.service\" ]; then "
        f"XDG_RUNTIME_DIR=\"${{XDG_RUNTIME_DIR:-/run/user/$(id -u)}}\" systemctl --user {normalized} pbgui-pbrun.service; "
        "elif [ -f \"$base/starter.py\" ]; then "
        f"cd \"$base\" && \"$py\" \"$base/starter.py\" {starter_flag} PBRun; "
        "else echo 'PBRun service control helper not found' >&2; exit 1; fi"
    )


def _remote_stop_pbrun_command(remote_pbgui_dir: str | None) -> str:
    """Build a remote command that stops PBRun without touching bot processes."""

    return _remote_pbrun_service_command(remote_pbgui_dir, "stop")


async def _run_remote_pbrun_service(pool: Any, hostname: str, node: dict[str, Any], action: str) -> dict[str, Any]:
    """Start or stop PBRun on a remote node through the existing SSH pool."""

    normalized = str(action or "").strip().lower()
    command = _remote_pbrun_service_command(str(node.get("remote_pbgui_dir") or ""), normalized)
    try:
        result = await pool.run(hostname, command, timeout=20)
    except Exception as exc:
        _log(SERVICE, f"Remote PBRun {normalized} failed for {hostname}: {exc}", level="ERROR")
        raise HTTPException(status_code=502, detail=f"Could not {normalized} remote PBRun: {exc}") from exc
    if result is None:
        raise HTTPException(status_code=502, detail=f"Remote host is unreachable while trying to {normalized} PBRun")
    exit_status = int(getattr(result, "exit_status", 1) or 0)
    if exit_status != 0:
        error = _probe_error_text(result)
        _log(SERVICE, f"Remote PBRun {normalized} failed for {hostname}: {error}", level="WARNING")
        raise HTTPException(status_code=409, detail=f"Could not {normalized} remote PBRun: {error}")
    return {"ok": True, "action": normalized, "stdout": str(getattr(result, "stdout", "") or "").strip()}


async def _stop_remote_pbrun_for_join(pool: Any, hostname: str, node: dict[str, Any]) -> dict[str, Any]:
    """Stop PBRun on a VPS before joining it to avoid transitional gate stops."""

    result = await _run_remote_pbrun_service(pool, hostname, node, "stop")
    result["stopped"] = True
    return result


def _v7_materialization_current(payload: dict[str, Any]) -> bool:
    """Return True when remote V7 materialization preview has no pending writes."""

    counts = payload.get("counts") if isinstance(payload, dict) else {}
    counts = counts if isinstance(counts, dict) else {}
    return bool(payload.get("ok")) and int(counts.get("error") or 0) == 0 and (int(counts.get("add") or 0) + int(counts.get("update") or 0)) == 0


def _api_key_materialization_current(payload: dict[str, Any]) -> bool:
    """Return True when remote API-key materialization preview has no pending writes."""

    counts = payload.get("counts") if isinstance(payload, dict) else {}
    counts = counts if isinstance(counts, dict) else {}
    return bool(payload.get("ok")) and int(counts.get("error") or 0) == 0 and int(counts.get("write") or 0) == 0


async def _maybe_start_remote_pbrun_after_materialization(node: dict[str, Any], identity: dict[str, Any]) -> dict[str, Any]:
    """Start PBRun on a VPS runner once all remote materialization is current."""

    if _cluster_role_from_monitor_role(node.get("role")) != "vps":
        return {"attempted": False, "started": False, "reason": "not_vps_runner"}
    try:
        v7_preview = await _run_remote_materialize_command(node, identity, "materialize-v7-preview")
        api_preview = await _run_remote_materialize_command(node, identity, "materialize-api-keys-preview")
    except HTTPException as exc:
        return {"attempted": False, "started": False, "reason": "preview_failed", "error": str(exc.detail)}
    if not _v7_materialization_current(v7_preview):
        return {"attempted": False, "started": False, "reason": "v7_materialization_pending"}
    if not _api_key_materialization_current(api_preview):
        return {"attempted": False, "started": False, "reason": "api_key_materialization_pending"}
    hostname = str(node.get("pbname") or node.get("hostname") or "")
    monitor = get_monitor()
    pool = getattr(monitor, "pool", None) if monitor else None
    if not pool:
        return {"attempted": True, "started": False, "reason": "ssh_pool_unavailable"}
    try:
        result = await _run_remote_pbrun_service(pool, hostname, node, "start")
    except HTTPException as exc:
        return {"attempted": True, "started": False, "reason": "start_failed", "error": str(exc.detail)}
    return {"attempted": True, "started": True, "result": result}


async def _complete_remote_join_sync(node: dict[str, Any], identity: dict[str, Any]) -> dict[str, Any]:
    """Push state, materialize files and restart PBRun after a successful join."""

    completion: dict[str, Any] = {"ok": True}
    push_result = await _push_missing_operations_to_remote(node, identity, rebuild=True)
    completion["push"] = push_result

    v7_preview = await _run_remote_materialize_command(node, identity, "materialize-v7-preview")
    completion["v7_preview"] = v7_preview
    v7_current = _v7_materialization_current(v7_preview)
    if v7_preview.get("can_apply"):
        completion["v7_materialization"] = await _run_remote_materialize_command(node, identity, "materialize-v7", timeout=120)
        v7_current = True
    else:
        completion["v7_materialization"] = {"skipped": True, "reason": "current_or_not_applicable"}

    api_key_preview = await _run_remote_materialize_command(node, identity, "materialize-api-keys-preview")
    completion["api_key_preview"] = api_key_preview
    api_key_current = _api_key_materialization_current(api_key_preview)
    if api_key_preview.get("can_apply"):
        completion["api_key_materialization"] = await _run_remote_materialize_command(node, identity, "materialize-api-keys", timeout=60)
        api_key_current = True
    else:
        completion["api_key_materialization"] = {"skipped": True, "reason": "current_or_not_applicable"}

    if _cluster_role_from_monitor_role(node.get("role")) != "vps":
        completion["pbrun_start"] = {"attempted": False, "started": False, "reason": "not_vps_runner"}
    elif not v7_current:
        completion["pbrun_start"] = {"attempted": False, "started": False, "reason": "v7_materialization_pending"}
    elif not api_key_current:
        completion["pbrun_start"] = {"attempted": False, "started": False, "reason": "api_key_materialization_pending"}
    else:
        hostname = str(node.get("pbname") or node.get("hostname") or "")
        monitor = get_monitor()
        pool = getattr(monitor, "pool", None) if monitor else None
        if not pool:
            completion["pbrun_start"] = {"attempted": True, "started": False, "reason": "ssh_pool_unavailable"}
        else:
            try:
                result = await _run_remote_pbrun_service(pool, hostname, node, "start")
                completion["pbrun_start"] = {"attempted": True, "started": True, "result": result}
            except HTTPException as exc:
                completion["pbrun_start"] = {"attempted": True, "started": False, "reason": "start_failed", "error": str(exc.detail)}
    return completion


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
        "cluster_ssh_fingerprint": payload.get("cluster_ssh_fingerprint") if isinstance(payload, dict) else "",
        "cluster_ssh_error": payload.get("cluster_ssh_error") if isinstance(payload, dict) else "",
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


def _state_vector_from_operations(operations: list[dict[str, Any]]) -> dict[str, int]:
    """Build a compact state vector from loaded operations."""

    vector: dict[str, int] = {}
    for operation in operations:
        actor = str(operation.get("actor") or "")
        try:
            seq = int(operation.get("seq") or 0)
        except (TypeError, ValueError):
            continue
        if actor and seq > 0:
            vector[actor] = max(vector.get(actor, 0), seq)
    return {key: vector[key] for key in sorted(vector)}


def _atomic_write_text(path: Path, text: str) -> None:
    """Atomically write one small UTF-8 text file."""

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_text(str(text), encoding="utf-8")
    os.replace(tmp, path)


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    """Atomically write one JSON object."""

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4)
        f.write("\n")
    os.replace(tmp, path)


def _validate_cluster_identifier(value: Any) -> str:
    """Validate and return a Cluster Sync cluster id."""

    text = str(value or "").strip()
    prefix = "pbgui-cluster-"
    if not text.startswith(prefix):
        raise HTTPException(status_code=409, detail="Remote returned an invalid cluster_id")
    try:
        uuid.UUID(text.removeprefix(prefix))
    except ValueError as exc:
        raise HTTPException(status_code=409, detail="Remote returned an invalid cluster_id") from exc
    return text


def _validate_node_identifier(value: Any, label: str = "node_id") -> str:
    """Validate and return a Cluster Sync node id."""

    text = str(value or "").strip()
    prefix = "pbgui-node-"
    if not text.startswith(prefix):
        raise HTTPException(status_code=409, detail=f"Remote returned an invalid {label}")
    try:
        uuid.UUID(text.removeprefix(prefix))
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=f"Remote returned an invalid {label}") from exc
    return text


def _adopt_empty_local_cluster_identity(root: Path, identity: dict[str, Any], cluster_id: str) -> dict[str, Any]:
    """Point an unused local identity at an existing cluster id."""

    current_cluster_id = str(identity.get("cluster_id") or "")
    if current_cluster_id == cluster_id:
        return {"changed": False, "identity": dict(identity)}
    try:
        operations = load_operations(root, expected_cluster_id=current_cluster_id)
    except ClusterStateError as exc:
        raise HTTPException(status_code=409, detail=f"Local cluster state is not safe to adopt: {exc}") from exc
    if operations:
        raise HTTPException(status_code=409, detail="Local cluster already has oplog entries; refusing to adopt another cluster_id")

    node_id = _validate_node_identifier(identity.get("node_id"), "local node_id")
    node_identity_path = root / "node_identity.json"
    node_identity = dict(identity)
    node_identity.update({
        "schema_version": 1,
        "cluster_id": cluster_id,
        "node_id": node_id,
        "role": str(identity.get("role") or "master"),
        "created_from_pbname": str(identity.get("created_from_pbname") or _get_master_pbname()),
    })
    _atomic_write_text(root / "cluster_id", cluster_id)
    _atomic_write_text(root / "node_id", node_id)
    _atomic_write_json(node_identity_path, node_identity)
    return {"changed": True, "identity": node_identity}


def _archive_local_cluster_state_for_join(root: Path, current_cluster_id: str) -> dict[str, Any]:
    """Archive local cluster state files before replacing them with an upstream cluster."""

    stamp = int(time.time())
    suffix = uuid.uuid4().hex[:8]
    archive_dir = root / "archives" / f"self-join-{stamp}-{suffix}"
    archive_dir.mkdir(parents=True, exist_ok=False)
    archived: list[str] = []
    for name in (
        "cluster_id",
        "node_identity.json",
        "cluster_nodes.json",
        "desired_state.json",
        "state_vector.json",
        "sync_status.json",
        "sync_request",
        "oplog",
        "config_blobs",
        "secret_blobs",
    ):
        source = root / name
        if not source.exists():
            continue
        target = archive_dir / name
        target.parent.mkdir(parents=True, exist_ok=True)
        source.rename(target)
        archived.append(name)
    return {
        "changed": bool(archived),
        "path": str(archive_dir),
        "cluster_id": str(current_cluster_id or ""),
        "items": archived,
    }


def _known_vps_config_by_hostname(hostname: str) -> dict[str, Any]:
    """Return one known VPS Manager config by hostname, if present."""

    wanted = str(hostname or "").strip()
    for config in _known_vps_configs():
        if str(config.get("hostname") or "").strip() == wanted:
            return dict(config)
    return {}


def _validate_self_join_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Validate the explicit outbound self-join request."""

    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Request body must be a JSON object")
    hostname = str(payload.get("hostname") or payload.get("upstream_hostname") or "").strip()
    if not hostname:
        raise HTTPException(status_code=400, detail="hostname is required")
    _validate_instance_name(hostname)
    known = _known_vps_config_by_hostname(hostname)
    ssh_host = str(payload.get("ssh_host") or known.get("ssh_host") or hostname).strip()
    ssh_user = str(payload.get("ssh_user") or known.get("ssh_user") or "").strip()
    raw_port = payload.get("ssh_port") or known.get("ssh_port") or 22
    try:
        ssh_port = int(raw_port or 22)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="ssh_port must be a number") from exc
    if ssh_port < 1 or ssh_port > 65535:
        raise HTTPException(status_code=400, detail="ssh_port must be between 1 and 65535")
    remote_pbgui_dir = str(payload.get("remote_pbgui_dir") or known.get("remote_pbgui_dir") or "software/pbgui").strip() or "software/pbgui"
    ssh_password = str(payload.get("ssh_password") or "")
    return {
        "hostname": hostname,
        "ssh_host": ssh_host,
        "ssh_user": ssh_user,
        "ssh_port": ssh_port,
        "ssh_password": ssh_password,
        "remote_pbgui_dir": remote_pbgui_dir,
        "reset_local_cluster_state": bool(payload.get("reset_local_cluster_state") is True),
    }


def _remote_pbgui_dir_candidates(remote_pbgui_dir: Any) -> list[str]:
    """Return remote PBGui path candidates in the same order VPS Manager probes them."""

    candidates: list[str] = []
    for item in (remote_pbgui_dir, "software/pbgui", "pbgui"):
        value = str(item or "").strip().rstrip("/")
        if value and value not in candidates:
            candidates.append(value)
    return candidates


async def _discover_remote_pbgui_dir_for_self_join(pool: Any, hostname: str, remote_pbgui_dir: str) -> dict[str, Any]:
    """Find the upstream PBGui directory using the VPS Manager path candidate order."""

    candidates = _remote_pbgui_dir_candidates(remote_pbgui_dir)
    for candidate in candidates:
        base = remote_shell_path(candidate)
        command = f"base={base}; [ -f \"$base/pbgui.ini\" ]"
        try:
            result = await pool.run(hostname, command, timeout=8)
        except Exception as exc:
            _log(SERVICE, f"Remote PBGui dir discovery failed for {hostname}: {exc}", level="ERROR")
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        if result is None:
            raise HTTPException(status_code=502, detail="Remote host is unreachable")
        if int(getattr(result, "exit_status", 1) or 0) == 0:
            return {"remote_pbgui_dir": candidate, "candidates": candidates}
    raise HTTPException(status_code=409, detail=f"pbgui.ini not found on upstream master in: {', '.join(candidates)}")


async def _run_cluster_json_command_on_host(
    pool: Any,
    hostname: str,
    remote_pbgui_dir: str,
    local_node_id: str,
    command_text: str,
    *,
    timeout: int = 30,
    failure_label: str = "Remote cluster command",
) -> dict[str, Any]:
    """Run one restricted Cluster Sync command against a named SSH-pool host."""

    command = _cluster_remote_command(remote_pbgui_dir, local_node_id, command_text)
    try:
        result = await pool.run(hostname, command, timeout=timeout)
    except Exception as exc:
        _log(SERVICE, f"{failure_label} failed for {hostname}: {exc}", level="ERROR")
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    if result is None:
        raise HTTPException(status_code=502, detail="Remote host is unreachable")
    if int(getattr(result, "exit_status", 1) or 0) != 0:
        error = _probe_error_text(result)
        _log(SERVICE, f"{failure_label} rejected by {hostname}: {error}", level="WARNING")
        raise HTTPException(status_code=409, detail=error)
    return _parse_remote_json_result(result, failure_label)


async def _run_cluster_ssh_setup_on_host(
    pool: Any,
    hostname: str,
    remote_pbgui_dir: str,
    args: list[str],
    *,
    timeout: int = 15,
) -> dict[str, Any]:
    """Run the Cluster SSH setup helper against a named SSH-pool host."""

    command = _cluster_ssh_setup_command(remote_pbgui_dir, args)
    try:
        result = await pool.run(hostname, command, timeout=timeout)
    except Exception as exc:
        _log(SERVICE, f"Remote Cluster SSH setup failed for {hostname}: {exc}", level="ERROR")
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    if result is None:
        raise HTTPException(status_code=502, detail="Remote host is unreachable")
    if int(getattr(result, "exit_status", 1) or 0) != 0:
        raise HTTPException(status_code=409, detail=_probe_error_text(result))
    payload = _parse_remote_json_result(result, "Remote Cluster SSH setup")
    if payload.get("ok") is False:
        raise HTTPException(status_code=409, detail=str(payload.get("error") or "Remote Cluster SSH setup failed"))
    return payload


def _write_cluster_blob(base_dir: Path, blob_hash: str, raw: bytes, *, secret: bool) -> None:
    """Atomically write and verify one pulled content-addressed blob."""

    text = str(blob_hash or "")
    if not text.startswith("sha256:") or len(text) != len("sha256:") + 64:
        raise HTTPException(status_code=409, detail="Remote returned an invalid blob hash")
    if hashlib.sha256(raw).hexdigest() != text.removeprefix("sha256:"):
        raise HTTPException(status_code=409, detail="Remote blob hash mismatch")
    path = _cluster_blob_path(base_dir, text)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_bytes(raw)
    os.chmod(tmp, 0o600 if secret else 0o644)
    os.replace(tmp, path)


def _manifest_file_hashes(manifest_raw: bytes) -> list[str]:
    """Return file blob hashes referenced by a config manifest blob."""

    try:
        manifest = json.loads(manifest_raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=409, detail="Config manifest blob is not valid JSON") from exc
    files = manifest.get("files") if isinstance(manifest, dict) else {}
    files = files if isinstance(files, dict) else {}
    hashes: list[str] = []
    for meta in files.values():
        sha = str((meta if isinstance(meta, dict) else {}).get("sha256") or "")
        if sha:
            hashes.append(f"sha256:{sha}")
    return hashes


def _missing_config_blob_hash(exc: HTTPException) -> str:
    """Extract a missing config blob hash from a remote wrapper error."""

    if int(getattr(exc, "status_code", 0) or 0) != 409:
        return ""
    detail = str(getattr(exc, "detail", "") or "")
    marker = "missing config blob:"
    if marker not in detail:
        return ""
    start = detail.find("sha256:", detail.find(marker))
    if start < 0:
        return ""
    candidate = detail[start:start + len("sha256:") + 64]
    digest = candidate.removeprefix("sha256:")
    if len(digest) == 64 and all(char in "0123456789abcdefABCDEF" for char in digest):
        return f"sha256:{digest.lower()}"
    return ""


async def _ensure_remote_blob_on_host(
    pool: Any,
    hostname: str,
    remote_pbgui_dir: str,
    local_node_id: str,
    base_dir: Path,
    blob_hash: str,
    *,
    secret: bool,
) -> tuple[bytes, bool]:
    """Ensure one local blob exists by pulling it from a reachable upstream host."""

    try:
        return _read_cluster_blob(base_dir, blob_hash), False
    except Exception:
        pass
    verb = "get-secret-blob" if secret else "get-blob"
    payload = await _run_cluster_json_command_on_host(
        pool,
        hostname,
        remote_pbgui_dir,
        local_node_id,
        f"{verb} {shlex.quote(str(blob_hash))}",
        timeout=30,
        failure_label="Remote blob pull",
    )
    remote_hash = str(payload.get("hash") or blob_hash)
    if remote_hash != str(blob_hash):
        raise HTTPException(status_code=409, detail="Remote returned a different blob hash")
    try:
        raw = base64.b64decode(str(payload.get("content_b64") or ""), validate=True)
    except (binascii.Error, ValueError) as exc:
        raise HTTPException(status_code=409, detail="Remote blob payload is not valid base64") from exc
    _write_cluster_blob(base_dir, remote_hash, raw, secret=secret)
    return raw, True


async def _try_pull_historical_config_blob_on_host(
    pool: Any,
    hostname: str,
    remote_pbgui_dir: str,
    local_node_id: str,
    base_dir: Path,
    blob_hash: str,
) -> tuple[bytes | None, bool, str]:
    """Pull a historical config blob, deferring missing blobs for superseded ops."""

    try:
        raw, fetched = await _ensure_remote_blob_on_host(
            pool,
            hostname,
            remote_pbgui_dir,
            local_node_id,
            base_dir,
            blob_hash,
            secret=False,
        )
        return raw, fetched, ""
    except HTTPException as exc:
        missing_hash = _missing_config_blob_hash(exc)
        if missing_hash:
            return None, False, missing_hash
        raise


async def _pull_blobs_for_operations_from_host(
    pool: Any,
    hostname: str,
    remote_pbgui_dir: str,
    local_node_id: str,
    operations: list[dict[str, Any]],
) -> dict[str, int]:
    """Pull all config/API-key blobs required by received operations."""

    counts = {"config": 0, "secret": 0, "missing_config": 0}
    missing_config_hashes: set[str] = set()
    root = _cluster_root()
    config_dir = root / "config_blobs"
    secret_dir = root / "secret_blobs"
    for operation in operations:
        refs = _operation_hash_refs(operation)
        for manifest_hash in refs["config"]:
            manifest_raw, fetched, missing_hash = await _try_pull_historical_config_blob_on_host(
                pool,
                hostname,
                remote_pbgui_dir,
                local_node_id,
                config_dir,
                manifest_hash,
            )
            if missing_hash:
                missing_config_hashes.add(missing_hash)
                continue
            if manifest_raw is None:
                continue
            counts["config"] += 1 if fetched else 0
            for file_hash in _manifest_file_hashes(manifest_raw):
                _raw, file_fetched, missing_file_hash = await _try_pull_historical_config_blob_on_host(
                    pool,
                    hostname,
                    remote_pbgui_dir,
                    local_node_id,
                    config_dir,
                    file_hash,
                )
                if missing_file_hash:
                    missing_config_hashes.add(missing_file_hash)
                    continue
                counts["config"] += 1 if file_fetched else 0
        for payload_hash in refs["api_payload"]:
            _raw, fetched = await _ensure_remote_blob_on_host(
                pool,
                hostname,
                remote_pbgui_dir,
                local_node_id,
                config_dir,
                payload_hash,
                secret=False,
            )
            counts["config"] += 1 if fetched else 0
        for secret_hash in refs["secret"]:
            _raw, fetched = await _ensure_remote_blob_on_host(
                pool,
                hostname,
                remote_pbgui_dir,
                local_node_id,
                secret_dir,
                secret_hash,
                secret=True,
            )
            counts["secret"] += 1 if fetched else 0
    counts["missing_config"] = len(missing_config_hashes)
    return counts


async def _pull_current_desired_blobs_from_host(
    pool: Any,
    hostname: str,
    remote_pbgui_dir: str,
    local_node_id: str,
    desired_state: dict[str, Any],
) -> dict[str, int]:
    """Pull current desired config/API-key blobs and fail if any are missing."""

    counts = {"config": 0, "secret": 0}
    root = _cluster_root()
    config_dir = root / "config_blobs"
    secret_dir = root / "secret_blobs"
    instances = desired_state.get("instances") if isinstance(desired_state, dict) else {}
    instances = instances if isinstance(instances, dict) else {}
    for name in sorted(instances):
        item = instances.get(name) if isinstance(instances.get(name), dict) else {}
        manifest_hash = str(item.get("config_manifest_hash") or "")
        if not manifest_hash:
            continue
        manifest_raw, fetched = await _ensure_remote_blob_on_host(
            pool,
            hostname,
            remote_pbgui_dir,
            local_node_id,
            config_dir,
            manifest_hash,
            secret=False,
        )
        counts["config"] += 1 if fetched else 0
        for file_hash in _manifest_file_hashes(manifest_raw):
            _raw, file_fetched = await _ensure_remote_blob_on_host(
                pool,
                hostname,
                remote_pbgui_dir,
                local_node_id,
                config_dir,
                file_hash,
                secret=False,
            )
            counts["config"] += 1 if file_fetched else 0
    api_keys = desired_state.get("api_keys") if isinstance(desired_state, dict) else None
    if isinstance(api_keys, dict):
        payload_hash = str(api_keys.get("payload_hash") or "")
        if payload_hash:
            _raw, fetched = await _ensure_remote_blob_on_host(
                pool,
                hostname,
                remote_pbgui_dir,
                local_node_id,
                config_dir,
                payload_hash,
                secret=False,
            )
            counts["config"] += 1 if fetched else 0
        secret_hash = str(api_keys.get("secret_blob_hash") or "")
        if secret_hash:
            _raw, fetched = await _ensure_remote_blob_on_host(
                pool,
                hostname,
                remote_pbgui_dir,
                local_node_id,
                secret_dir,
                secret_hash,
                secret=True,
            )
            counts["secret"] += 1 if fetched else 0
    return counts


async def _pull_missing_operations_from_host(
    pool: Any,
    hostname: str,
    remote_pbgui_dir: str,
    local_node_id: str,
    cluster_id: str,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Pull upstream operations and their blobs into the local cluster state."""

    def report_progress(update: dict[str, Any]) -> None:
        if progress_callback:
            progress_callback(dict(update))

    root = _cluster_root()
    report_progress({"phase": "pulling_vector", "done": 0, "total": 0, "remaining": 0})
    vector_payload = await _run_cluster_json_command_on_host(
        pool,
        hostname,
        remote_pbgui_dir,
        local_node_id,
        "get-state-vector",
        timeout=30,
        failure_label="Remote state-vector pull",
    )
    remote_vector = _as_state_vector(vector_payload.get("state_vector") or {})
    local_operations = load_operations(root, expected_cluster_id=cluster_id)
    local_vector = _state_vector_from_operations(local_operations)
    total_missing_ops = sum(max(0, int(remote_vector.get(actor) or 0) - int(local_vector.get(actor) or 0)) for actor in remote_vector)
    pulled = 0
    blob_counts = {"config": 0, "secret": 0, "missing_config": 0}
    report_progress({"phase": "pulling_ops", "done": 0, "total": total_missing_ops, "remaining": total_missing_ops})
    for actor in sorted(remote_vector):
        remote_seq = int(remote_vector.get(actor) or 0)
        local_seq = int(local_vector.get(actor) or 0)
        if remote_seq <= local_seq:
            continue
        start = local_seq + 1
        while start <= remote_seq:
            end = min(remote_seq, start + 999)
            payload = await _run_cluster_json_command_on_host(
                pool,
                hostname,
                remote_pbgui_dir,
                local_node_id,
                f"get-ops {shlex.quote(actor)} {start} {end}",
                timeout=30,
                failure_label="Remote operation pull",
            )
            missing = payload.get("missing") if isinstance(payload, dict) else []
            if missing:
                raise HTTPException(status_code=409, detail=f"Remote is missing operation(s) for {actor}: {missing}")
            operations = payload.get("operations") if isinstance(payload, dict) else []
            operations = operations if isinstance(operations, list) else []
            report_progress({"phase": "pulling_blobs", "done": pulled, "total": total_missing_ops, "remaining": max(0, total_missing_ops - pulled)})
            pulled_blobs = await _pull_blobs_for_operations_from_host(pool, hostname, remote_pbgui_dir, local_node_id, operations)
            blob_counts["config"] += int(pulled_blobs.get("config") or 0)
            blob_counts["secret"] += int(pulled_blobs.get("secret") or 0)
            blob_counts["missing_config"] += int(pulled_blobs.get("missing_config") or 0)
            for operation in operations:
                validate_operation(operation, expected_cluster_id=cluster_id)
                op_path = root / "oplog" / str(operation["actor"]) / f"{int(operation['seq']):08d}.json"
                existed = op_path.exists()
                write_operation(root, operation)
                if not existed:
                    pulled += 1
            start = end + 1
            report_progress({"phase": "pulling_ops", "done": pulled, "total": total_missing_ops, "remaining": max(0, total_missing_ops - pulled)})
    materialized = rebuild_materialized_state(root, write=False)
    report_progress({"phase": "pulling_current_blobs", "done": pulled, "total": total_missing_ops, "remaining": 0})
    current_blob_counts = await _pull_current_desired_blobs_from_host(
        pool,
        hostname,
        remote_pbgui_dir,
        local_node_id,
        materialized.get("desired_state") if isinstance(materialized, dict) else {},
    )
    blob_counts["config"] += int(current_blob_counts.get("config") or 0)
    blob_counts["secret"] += int(current_blob_counts.get("secret") or 0)
    return {
        "remote_vector": remote_vector,
        "local_vector_before": local_vector,
        "pulled_ops": pulled,
        "pulled_config_blobs": blob_counts["config"],
        "pulled_secret_blobs": blob_counts["secret"],
        "deferred_missing_config_blobs": blob_counts["missing_config"],
    }


def _node_payload_updates(current: dict[str, Any], desired: dict[str, Any]) -> dict[str, Any]:
    """Return desired node fields that differ from current materialized values."""

    updates: dict[str, Any] = {}
    for key, value in desired.items():
        if key == "node_id":
            continue
        current_value = current.get(key)
        if isinstance(value, list):
            current_list = current_value if isinstance(current_value, list) else []
            if [str(item) for item in current_list] != [str(item) for item in value]:
                updates[key] = value
        elif isinstance(value, bool):
            if bool(current_value) != value:
                updates[key] = value
        elif isinstance(value, int):
            try:
                same = int(current_value) == value
            except (TypeError, ValueError):
                same = False
            if not same:
                updates[key] = value
        elif str(current_value or "") != str(value or ""):
            updates[key] = value
    return updates


def _append_node_membership_if_needed(current: dict[str, Any], desired: dict[str, Any]) -> dict[str, Any]:
    """Append ADD_NODE/UPDATE_NODE for a desired node record when needed."""

    node_id = str(desired.get("node_id") or "")
    if not node_id:
        return {"changed": False, "operation": "none"}
    if not current:
        append_operation(_cluster_root(), "ADD_NODE", desired)
        return {"changed": True, "operation": "ADD_NODE"}
    updates = _node_payload_updates(current, desired)
    if not updates:
        return {"changed": False, "operation": "none"}
    append_operation(_cluster_root(), "UPDATE_NODE", {"node_id": node_id, **updates})
    return {"changed": True, "operation": "UPDATE_NODE", "updated_fields": sorted(updates)}


def _self_join_transport_node(upstream: dict[str, Any], settings: dict[str, Any]) -> dict[str, Any]:
    """Build a materialized node record usable with the monitor SSH pool."""

    node = dict(upstream)
    node.update({
        "pbname": str(settings.get("hostname") or node.get("pbname") or node.get("hostname") or ""),
        "hostname": str(settings.get("hostname") or node.get("hostname") or node.get("pbname") or ""),
        "sync_mode": "reachable",
        "sync_enabled": True,
        "ssh_host": str(settings.get("ssh_host") or node.get("ssh_host") or ""),
        "ssh_user": str(settings.get("ssh_user") or node.get("ssh_user") or ""),
        "ssh_port": int(settings.get("ssh_port") or node.get("ssh_port") or 22),
        "remote_pbgui_dir": str(settings.get("remote_pbgui_dir") or node.get("remote_pbgui_dir") or "software/pbgui"),
    })
    return node


async def _self_join_existing_cluster(
    settings: dict[str, Any],
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Join this master to an existing cluster through an outbound SSH connection."""

    total_steps = 9

    def report_progress(phase: str, done: int, **extra: Any) -> None:
        if not progress_callback:
            return
        update = {"phase": phase, "done": max(0, min(total_steps, int(done))), "total": total_steps}
        update["remaining"] = max(0, total_steps - int(update["done"]))
        update.update(extra)
        progress_callback(update)

    root = _cluster_root()
    report_progress("starting", 0)
    try:
        identity = ensure_local_identity(root, role="master", pbname=_get_master_pbname())
    except ClusterStateError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    local_node_id = _validate_node_identifier(identity.get("node_id"), "local node_id")
    hostname = str(settings["hostname"])
    password_runner: _SelfJoinPasswordSSHRunner | None = None
    if str(settings.get("ssh_password") or ""):
        password_runner = _SelfJoinPasswordSSHRunner(
            hostname=hostname,
            ssh_host=str(settings.get("ssh_host") or hostname),
            ssh_user=str(settings.get("ssh_user") or ""),
            ssh_port=int(settings.get("ssh_port") or 22),
            ssh_password=str(settings.get("ssh_password") or ""),
        )
        pool = password_runner
    else:
        monitor = get_monitor()
        pool = getattr(monitor, "pool", None) if monitor else None
        if not pool:
            raise HTTPException(status_code=503, detail="VPS monitor SSH pool is unavailable")

    try:
        report_progress("discovering", 0)
        remote_dir_result = await _discover_remote_pbgui_dir_for_self_join(pool, hostname, str(settings["remote_pbgui_dir"]))
        remote_dir = str(remote_dir_result["remote_pbgui_dir"])
        settings["remote_pbgui_dir"] = remote_dir
        report_progress("hello", 1, remote_pbgui_dir=remote_dir)
        hello = await _run_cluster_json_command_on_host(
            pool,
            hostname,
            remote_dir,
            local_node_id,
            "hello",
            timeout=15,
            failure_label="Upstream cluster hello",
        )
        upstream_cluster_id = _validate_cluster_identifier(hello.get("cluster_id"))
        upstream_node_id = _validate_node_identifier(hello.get("node_id"), "upstream node_id")
        if upstream_node_id == local_node_id:
            raise HTTPException(status_code=409, detail="Upstream node_id matches the local node_id; refusing to self-join the same node")

        report_progress("checking_local_state", 2)
        local_cluster_id = str(identity.get("cluster_id") or "")
        try:
            local_operations = load_operations(root, expected_cluster_id=local_cluster_id)
        except ClusterStateError as exc:
            raise HTTPException(status_code=409, detail=f"Local cluster state is not safe to self-join: {exc}") from exc
        adoption = {"changed": False}
        archive_result: dict[str, Any] = {"changed": False}
        if local_cluster_id != upstream_cluster_id:
            if local_operations:
                if not settings.get("reset_local_cluster_state"):
                    raise HTTPException(
                        status_code=409,
                        detail="Local cluster_id differs and local oplog is not empty; enable recovery to archive local cluster state before joining",
                    )
                archive_result = _archive_local_cluster_state_for_join(root, local_cluster_id)
            adoption = _adopt_empty_local_cluster_identity(root, identity, upstream_cluster_id)
            identity = dict(adoption["identity"])

        report_progress("preparing_keys", 3)
        local_key = ensure_local_cluster_ssh_material(Path(PBGDIR), role="master", pbname=_get_master_pbname())
        report_progress("pulling", 4)
        pull_result = await _pull_missing_operations_from_host(
            pool,
            hostname,
            remote_dir,
            local_node_id,
            upstream_cluster_id,
            progress_callback=lambda update: report_progress(str(update.get("phase") or "pulling"), 4, pull=update),
        )
        materialized = rebuild_materialized_state(root)
        local_materialization = _materialize_v7_configs(root, write=True)
        nodes = _node_list(materialized["cluster_nodes"])
        upstream_node = _node_for_id(nodes, upstream_node_id)
        local_node = _node_for_id(nodes, local_node_id)

        report_progress("reading_upstream_key", 5)
        remote_key = await _run_cluster_ssh_setup_on_host(pool, hostname, remote_dir, ["ensure-local", "--node-id", upstream_node_id])
        remote_public_key = str(remote_key.get("public_key") or hello.get("cluster_ssh_public_key") or "").strip()
        remote_fingerprint = str(remote_key.get("fingerprint") or hello.get("cluster_ssh_fingerprint") or "").strip()
        if not remote_public_key:
            raise HTTPException(status_code=409, detail="Upstream did not return a Cluster SSH public key")
        if not remote_fingerprint:
            try:
                remote_fingerprint = public_key_fingerprint(remote_public_key)
            except Exception as exc:
                raise HTTPException(status_code=409, detail="Upstream returned an invalid Cluster SSH public key") from exc
        install_result = await _run_cluster_ssh_setup_on_host(
            pool,
            hostname,
            remote_dir,
            [
                "install-authorized-key",
                "--source-node",
                local_node_id,
                "--source-public-key",
                str(local_key.get("public_key") or ""),
            ],
        )

        report_progress("registering_nodes", 6)
        upstream_desired = {
            "node_id": upstream_node_id,
            "role": _cluster_role_from_monitor_role(upstream_node.get("role") or hello.get("role") or "master"),
            "pbname": str(upstream_node.get("pbname") or upstream_node.get("hostname") or hostname),
            "hostname": str(upstream_node.get("hostname") or upstream_node.get("pbname") or hostname),
            "sync_mode": "reachable",
            "sync_enabled": True,
            "ssh_host": str(settings.get("ssh_host") or ""),
            "ssh_port": int(settings.get("ssh_port") or 22),
            "remote_pbgui_dir": remote_dir,
            "cluster_ssh_public_key": remote_public_key,
            "cluster_ssh_fingerprint": remote_fingerprint,
            "cluster_ssh_mode": "forced",
        }
        if str(settings.get("ssh_user") or ""):
            upstream_desired["ssh_user"] = str(settings.get("ssh_user") or "")
        upstream_membership = _append_node_membership_if_needed(upstream_node, upstream_desired)
        local_ssh_host = _local_ssh_host_value(str(settings.get("ssh_host") or hostname), int(settings.get("ssh_port") or 22))
        local_ssh_user = _local_ssh_user_value()

        local_desired = {
            "node_id": local_node_id,
            "role": "master",
            "pbname": _get_master_pbname(),
            "hostname": _get_master_pbname(),
            "sync_mode": "outbound_only",
            "sync_enabled": True,
            "remote_pbgui_dir": _local_pbgui_dir_value(),
            "sync_peers": [upstream_node_id],
            "cluster_ssh_public_key": str(local_key.get("public_key") or ""),
            "cluster_ssh_fingerprint": str(local_key.get("fingerprint") or ""),
            "cluster_ssh_mode": "forced",
        }
        if local_ssh_host:
            local_desired["ssh_host"] = local_ssh_host
            local_desired["ssh_port"] = 22
        if local_ssh_user:
            local_desired["ssh_user"] = local_ssh_user
        local_membership = _append_node_membership_if_needed(local_node, local_desired)
        materialized = rebuild_materialized_state(root)
        upstream_node = _node_for_id(_node_list(materialized["cluster_nodes"]), upstream_node_id)
        transport_node = _self_join_transport_node(upstream_node, settings)
        report_progress("pushing_registration", 7)
        push_result = await _push_missing_operations_to_remote(
            transport_node,
            identity,
            rebuild=True,
            pool=pool,
            progress_callback=lambda update: report_progress(str(update.get("phase") or "pushing_registration"), 7, push=update),
        )
        report_progress("done", 9)
        return {
            "ok": True,
            "cluster_id": upstream_cluster_id,
            "local_node_id": local_node_id,
            "upstream_node_id": upstream_node_id,
            "upstream_hostname": hostname,
            "remote_pbgui_dir": remote_dir,
            "remote_pbgui_dir_candidates": remote_dir_result.get("candidates") or [],
            "adopted_local_identity": bool(adoption.get("changed")),
            "archived_local_cluster_state": archive_result,
            "pull": pull_result,
            "local_materialization": local_materialization,
            "membership": {
                "upstream": upstream_membership,
                "local": local_membership,
            },
            "cluster_ssh": {
                "upstream_fingerprint": remote_fingerprint,
                "local_fingerprint": str(local_key.get("fingerprint") or ""),
                "authorized_key_changed": bool(install_result.get("changed")),
            },
            "push": push_result,
            "message": "Joined existing cluster through outbound SSH and pushed this master registration upstream.",
        }
    finally:
        if password_runner is not None:
            await password_runner.close()


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

    role = _cluster_role_from_monitor_role(node.get("role"))
    pbrun_stop_result: dict[str, Any] | None = None
    if role == "vps":
        pbrun_stop_result = await _stop_remote_pbrun_for_join(pool, hostname, node)

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
    try:
        completion = await _complete_remote_join_sync(node, identity)
    except HTTPException as exc:
        completion = {"ok": False, "status_code": exc.status_code, "error": str(exc.detail)}
    except Exception as exc:
        _log(SERVICE, f"Remote post-join sync failed for {hostname}: {exc}", level="ERROR")
        completion = {"ok": False, "error": str(exc)}
    return {
        "ok": True,
        "node_id": node_id,
        "hostname": hostname,
        "remote_cluster_id": remote_cluster_id,
        "remote_node_id": remote_node_id,
        "role": payload.get("role") if isinstance(payload, dict) else node.get("role"),
        "pbrun_stopped": bool(pbrun_stop_result),
        "completion": completion,
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


async def _run_remote_read_command(
    node: dict[str, Any],
    identity: dict[str, Any],
    verb: str,
    *,
    pool: Any | None = None,
) -> dict[str, Any]:
    """Run one read-only Cluster Sync command against a remote node."""

    node_id, hostname, local_node_id = _require_remote_node_ready(node, identity)
    if pool is None:
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


async def _optional_json_payload(request: Request | None) -> dict[str, Any]:
    """Return a JSON request body when present."""

    if request is None:
        return {}
    try:
        payload = await request.json()
    except (json.JSONDecodeError, UnicodeDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _normalize_ssh_passwords(payload: Any) -> dict[str, str]:
    """Return a sanitized per-node SSH password map from an API payload."""

    if not isinstance(payload, dict):
        return {}
    raw = payload.get("ssh_passwords")
    if not isinstance(raw, dict):
        return {}
    result: dict[str, str] = {}
    for key, value in raw.items():
        node_key = str(key or "").strip()
        password = str(value or "")
        if node_key and password:
            result[node_key] = password
    return result


def _ssh_password_for_node(node: dict[str, Any], passwords: dict[str, str] | None) -> str:
    """Return a temporary SSH password for one node if the caller supplied one."""

    if not passwords:
        return ""
    for key in (node.get("node_id"), node.get("pbname"), node.get("hostname"), node.get("ssh_host")):
        text = str(key or "").strip()
        if text and text in passwords:
            return str(passwords[text] or "")
    return ""


async def _run_direct_cluster_ssh_setup(node: dict[str, Any], command: str, *, timeout: int = 15, ssh_password: str = "") -> Any:
    """Run Cluster SSH setup directly via a node's stored SSH metadata."""

    label = str(node.get("pbname") or node.get("hostname") or node.get("node_id") or "remote node")
    ssh_host = str(node.get("ssh_host") or "").strip()
    ssh_user = str(node.get("ssh_user") or "").strip() or None
    try:
        ssh_port = int(node.get("ssh_port") or 22)
    except (TypeError, ValueError):
        ssh_port = 22
    if not ssh_host:
        raise HTTPException(status_code=400, detail="Reachable cluster node has no SSH host")
    conn: asyncssh.SSHClientConnection | None = None
    try:
        conn = await asyncio.wait_for(
            asyncssh.connect(
                host=ssh_host,
                port=ssh_port,
                username=ssh_user,
                password=str(ssh_password or "") or None,
                known_hosts=None,
                keepalive_interval=10,
            ),
            timeout=10,
        )
        return await asyncio.wait_for(conn.run(command, check=False), timeout=timeout)
    except asyncssh.PermissionDenied as exc:
        raise HTTPException(status_code=502, detail=f"SSH authentication failed for {label}") from exc
    except asyncio.TimeoutError as exc:
        raise HTTPException(status_code=502, detail=f"SSH connection to {label} timed out") from exc
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"SSH connection to {label} failed: {exc}") from exc
    finally:
        if conn is not None:
            conn.close()
            wait_closed = getattr(conn, "wait_closed", None)
            if callable(wait_closed):
                try:
                    await wait_closed()
                except Exception:
                    pass


async def _run_remote_cluster_ssh_setup(node: dict[str, Any], identity: dict[str, Any], args: list[str], *, timeout: int = 15, ssh_passwords: dict[str, str] | None = None) -> dict[str, Any]:
    """Run the remote Cluster SSH setup helper through monitor SSH or stored node SSH metadata."""

    node_id, hostname, _local_node_id = _require_remote_node_ready(node, identity)
    monitor = get_monitor()
    pool = getattr(monitor, "pool", None) if monitor else None
    command = _cluster_ssh_setup_command(str(node.get("remote_pbgui_dir") or ""), args)
    result = None
    try:
        result = await pool.run(hostname, command, timeout=timeout) if pool else None
    except Exception as exc:
        _log(SERVICE, f"Remote Cluster SSH setup via monitor pool failed for {hostname}: {exc}; trying direct SSH metadata", level="WARNING")
    if result is None:
        result = await _run_direct_cluster_ssh_setup(node, command, timeout=timeout, ssh_password=_ssh_password_for_node(node, ssh_passwords))
    exit_status = int(getattr(result, "exit_status", 1) or 0)
    if exit_status != 0:
        error = _probe_error_text(result)
        raise HTTPException(status_code=409, detail=error)
    payload = _parse_remote_json_result(result, "Remote Cluster SSH setup")
    if payload.get("ok") is False:
        raise HTTPException(status_code=409, detail=str(payload.get("error") or "Remote Cluster SSH setup failed"))
    payload.setdefault("node_id", node_id)
    payload.setdefault("hostname", hostname)
    return payload


def _append_node_update_if_changed(node_id: str, current: dict[str, Any], updates: dict[str, Any]) -> bool:
    """Append UPDATE_NODE when any requested field differs."""

    clean_updates = {key: value for key, value in updates.items() if value not in {None, ""}}
    if not clean_updates:
        return False
    if all(str(current.get(key) or "") == str(value or "") for key, value in clean_updates.items()):
        return False
    append_operation(_cluster_root(), "UPDATE_NODE", {"node_id": node_id, **clean_updates})
    return True


async def _repair_node_cluster_ssh(node: dict[str, Any], identity: dict[str, Any], nodes: list[dict[str, Any]], *, ssh_passwords: dict[str, str] | None = None) -> dict[str, Any]:
    """Repair Cluster SSH key trust for one node and persist discovered key metadata."""

    node_id = str(node.get("node_id") or "")
    local_node_id = str(identity.get("node_id") or "")
    if not node_id:
        raise HTTPException(status_code=404, detail="Cluster node not found")
    local_key = ensure_local_cluster_ssh_material(Path(PBGDIR), role=str(identity.get("role") or "master"), pbname=_get_master_pbname())
    installed: list[dict[str, Any]] = []
    missing_source_keys: list[dict[str, str]] = []
    updates: dict[str, Any] = {}

    if node_id == local_node_id:
        local_public_key = str(local_key.get("public_key") or "").strip()
        updates = {
            "cluster_ssh_public_key": local_public_key,
            "cluster_ssh_fingerprint": str(local_key.get("fingerprint") or ""),
            "cluster_ssh_mode": "forced",
        }
        changed = _append_node_update_if_changed(node_id, node, updates)
        materialized = rebuild_materialized_state(_cluster_root()) if changed else _load_cluster_snapshot()
        updated = _node_for_id(_node_list(materialized["cluster_nodes"]), node_id)
        outbound_installed, outbound_install_errors = await _install_node_key_on_sync_peers(updated or node, identity, nodes, local_public_key, ssh_passwords=ssh_passwords)
        return {
            "ok": True,
            "node_id": node_id,
            "local": True,
            "changed": changed,
            "node": updated,
            "installed": [],
            "outbound_installed": outbound_installed,
            "outbound_install_errors": outbound_install_errors,
            "missing_source_keys": [],
        }

    remote_key = await _run_remote_cluster_ssh_setup(node, identity, ["ensure-local", "--node-id", node_id], ssh_passwords=ssh_passwords)
    remote_public_key = str(remote_key.get("public_key") or "").strip()
    if not remote_public_key:
        raise HTTPException(status_code=409, detail="Remote did not return a Cluster SSH public key")
    remote_fingerprint = str(remote_key.get("fingerprint") or "").strip() or public_key_fingerprint(remote_public_key)

    master_install = await _run_remote_cluster_ssh_setup(
        node,
        identity,
        [
            "install-authorized-key",
            "--source-node",
            local_node_id,
            "--source-public-key",
            str(local_key.get("public_key") or ""),
        ],
        ssh_passwords=ssh_passwords,
    )
    installed.append({"source_node_id": local_node_id, "changed": bool(master_install.get("changed")), "role": "master"})

    for source in nodes:
        source_id = str(source.get("node_id") or "")
        if not source_id or source_id in {local_node_id, node_id}:
            continue
        sync_peers = source.get("sync_peers") if isinstance(source.get("sync_peers"), list) else []
        if node_id not in {str(item) for item in sync_peers}:
            continue
        source_public_key = str(source.get("cluster_ssh_public_key") or "").strip()
        if not source_public_key:
            missing_source_keys.append({"node_id": source_id, "reason": "source node has no known Cluster SSH public key"})
            continue
        result = await _run_remote_cluster_ssh_setup(
            node,
            identity,
            ["install-authorized-key", "--source-node", source_id, "--source-public-key", source_public_key],
            ssh_passwords=ssh_passwords,
        )
        installed.append({"source_node_id": source_id, "changed": bool(result.get("changed")), "role": str(source.get("role") or "node")})

    outbound_installed, outbound_install_errors = await _install_node_key_on_sync_peers(node, identity, nodes, remote_public_key, ssh_passwords=ssh_passwords)

    updates = {
        "cluster_ssh_public_key": remote_public_key,
        "cluster_ssh_fingerprint": remote_fingerprint,
        "cluster_ssh_mode": "forced",
    }
    changed = _append_node_update_if_changed(node_id, node, updates)
    materialized = rebuild_materialized_state(_cluster_root()) if changed else _load_cluster_snapshot()
    updated = _node_for_id(_node_list(materialized["cluster_nodes"]), node_id)
    return {
        "ok": True,
        "node_id": node_id,
        "local": False,
        "changed": changed,
        "node": updated,
        "remote_key": {"fingerprint": remote_fingerprint},
        "installed": installed,
        "outbound_installed": outbound_installed,
        "outbound_install_errors": outbound_install_errors,
        "missing_source_keys": missing_source_keys,
    }


async def _install_node_key_on_sync_peers(source_node: dict[str, Any], identity: dict[str, Any], nodes: list[dict[str, Any]], source_public_key: str, *, ssh_passwords: dict[str, str] | None = None) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    """Install one node's Cluster SSH key on its configured outbound sync peers."""

    source_id = str(source_node.get("node_id") or "")
    local_node_id = str(identity.get("node_id") or "")
    sync_peer_ids = {str(item) for item in source_node.get("sync_peers") if str(item)} if isinstance(source_node.get("sync_peers"), list) else set()
    outbound_installed: list[dict[str, Any]] = []
    outbound_install_errors: list[dict[str, str]] = []
    for target in nodes:
        target_id = str(target.get("node_id") or "")
        if not target_id or target_id == source_id or target_id not in sync_peer_ids:
            continue
        try:
            if target_id == local_node_id:
                result = install_authorized_cluster_key(
                    pbgdir=Path(PBGDIR),
                    source_node_id=source_id,
                    source_public_key=source_public_key,
                )
            else:
                result = await _run_remote_cluster_ssh_setup(
                    target,
                    identity,
                    ["install-authorized-key", "--source-node", source_id, "--source-public-key", source_public_key],
                    ssh_passwords=ssh_passwords,
                )
            outbound_installed.append({"target_node_id": target_id, "changed": bool(result.get("changed")), "role": str(target.get("role") or "node")})
        except HTTPException as exc:
            outbound_install_errors.append({"target_node_id": target_id, "reason": str(exc.detail)})
    return outbound_installed, outbound_install_errors


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
    pool: Any | None = None,
) -> dict[str, Any]:
    """Push missing local oplog entries to a remote node and optionally rebuild its materialized state."""

    def report_progress(update: dict[str, Any]) -> None:
        if progress_callback:
            progress_callback(dict(update))

    node_id, hostname, local_node_id = _require_remote_node_ready(node, identity)
    if pool is None:
        monitor = get_monitor()
        pool = getattr(monitor, "pool", None) if monitor else None
    if not pool:
        raise HTTPException(status_code=503, detail="VPS monitor SSH pool is unavailable")

    root = _cluster_root()
    remote_vector_payload = await _run_remote_read_command(node, identity, "get-state-vector", pool=pool)
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


async def _run_self_join_job(job_id: str, settings: dict[str, Any]) -> None:
    """Run one self-join in the background and update local progress."""

    def progress(update: dict[str, Any]) -> None:
        _update_self_join_job(job_id, status="running", error="", **update)

    _update_self_join_job(job_id, status="running", phase="starting", done=0, total=9, remaining=9, error="")
    try:
        result = await _self_join_existing_cluster(settings, progress_callback=progress)
    except HTTPException as exc:
        _update_self_join_job(
            job_id,
            status="error",
            phase="error",
            error=str(exc.detail),
            result={"status_code": exc.status_code, "detail": exc.detail},
        )
        return
    except Exception as exc:
        _log(SERVICE, f"Self-join job failed for {settings.get('hostname')}: {exc}", level="ERROR")
        _update_self_join_job(job_id, status="error", phase="error", error="Self-join failed")
        return

    _update_self_join_job(
        job_id,
        status="done",
        phase="done",
        done=9,
        total=9,
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
                current = _current_instance_state(desired_state, name)
                if current:
                    item.update({
                        "action": "skip",
                        "reason": "local config.json is missing; desired state already tracks this instance",
                        "current_version": str(current.get("version") or ""),
                        "current_manifest_hash": str(current.get("config_manifest_hash") or ""),
                        "desired_state": str(current.get("desired_state") or ""),
                        "assigned_host": str(current.get("assigned_host") or ""),
                    })
                    counts["skip"] += 1
                else:
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
    root = _cluster_root()
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
        "sync_status": _load_sync_status_summary(root),
        "warnings": warnings,
    }


@router.get("/nodes")
def get_nodes(session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Return materialized Cluster Sync node records."""

    snapshot = _load_cluster_snapshot()
    cluster_nodes = snapshot["cluster_nodes"]
    try:
        local_cluster_ssh = ensure_local_cluster_ssh_material(Path(PBGDIR), role=str(snapshot["identity"].get("role") or "master"), pbname=_get_master_pbname())
    except Exception as exc:
        local_cluster_ssh = {"ok": False, "error": str(exc)}
    nodes = _node_list(cluster_nodes)
    local_node_id = str(snapshot["identity"].get("node_id") or "")
    return {
        "cluster_nodes": cluster_nodes,
        "nodes": _nodes_with_local_defaults(nodes, local_node_id),
        "local_cluster_ssh": {
            "ok": local_cluster_ssh.get("ok", True) is not False,
            "fingerprint": str(local_cluster_ssh.get("fingerprint") or ""),
            "public_key_path": str(local_cluster_ssh.get("public_key_path") or ""),
            "error": str(local_cluster_ssh.get("error") or ""),
        },
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
    is_local_node = str(node.get("node_id") or "") == local_node_id
    if is_local_node:
        if not str(settings.get("remote_pbgui_dir") or "").strip():
            settings["remote_pbgui_dir"] = _local_pbgui_dir_value()
        if not str(settings.get("ssh_user") or "").strip():
            settings["ssh_user"] = _local_ssh_user_value()
        if not str(settings.get("ssh_host") or "").strip():
            peer_host = ""
            for peer_id in settings.get("sync_peers") or []:
                peer = _node_for_id(nodes, str(peer_id or ""))
                peer_host = str(peer.get("ssh_host") or peer.get("hostname") or peer.get("pbname") or "").strip()
                if peer_host:
                    break
            settings["ssh_host"] = _local_ssh_host_value(peer_host, int(settings.get("ssh_port") or 22))
    if node.get("enabled") is False and settings["sync_mode"] != "disabled":
        raise HTTPException(status_code=400, detail="Disabled cluster nodes cannot be enabled for sync")
    _validate_sync_peers_for_node(settings, node, nodes)

    current = {
        "sync_mode": normalize_node_sync_mode(node),
        "sync_enabled": normalize_node_sync_mode(node) != "disabled",
        "remote_pbgui_dir": str(node.get("remote_pbgui_dir") or ""),
        "ssh_host": str(node.get("ssh_host") or ""),
        "ssh_user": str(node.get("ssh_user") or ""),
        "ssh_port": int(node.get("ssh_port") or 22),
        "sync_peers": _normalize_sync_peers(node.get("sync_peers") or []),
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


@router.post("/nodes/{node_id}/remove")
def remove_cluster_node(node_id: str, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Remove a disabled Cluster node from materialized membership."""

    snapshot = _load_cluster_snapshot()
    nodes = _node_list(snapshot["cluster_nodes"])
    node = _node_for_id(nodes, str(node_id or ""))
    if not node:
        raise HTTPException(status_code=404, detail="Cluster node not found")
    if str(node.get("node_id") or "") == str(snapshot["identity"].get("node_id") or ""):
        raise HTTPException(status_code=400, detail="Local cluster node cannot be removed")
    if normalize_node_sync_mode(node) != "disabled":
        raise HTTPException(status_code=400, detail="Only disabled cluster nodes can be removed")
    desired_state = snapshot["desired_state"] if isinstance(snapshot.get("desired_state"), dict) else {}
    instances = desired_state.get("instances") if isinstance(desired_state, dict) else {}
    assigned_instances = [
        str(name)
        for name, item in (instances if isinstance(instances, dict) else {}).items()
        if isinstance(item, dict) and str(item.get("assigned_host") or "") == str(node.get("node_id") or "")
    ]
    if assigned_instances:
        preview = ", ".join(sorted(assigned_instances)[:5])
        raise HTTPException(status_code=400, detail=f"Node still has assigned V7 configs: {preview}")
    append_operation(_cluster_root(), "REMOVE_NODE", {"node_id": str(node.get("node_id") or "")})
    materialized = rebuild_materialized_state(_cluster_root())
    return {
        "ok": True,
        "changed": True,
        "removed_node_id": str(node.get("node_id") or ""),
        "node": node,
        "nodes": _node_list(materialized["cluster_nodes"]),
    }


async def _repair_all_cluster_ssh_run(
    ssh_passwords: dict[str, str] | None = None,
    *,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Run Repair All SSH and optionally report per-node progress."""

    def report_progress(update: dict[str, Any]) -> None:
        if progress_callback:
            progress_callback(dict(update))

    snapshot = _load_cluster_snapshot()
    nodes = _node_list(snapshot["cluster_nodes"])
    identity = snapshot["identity"]
    local_node_id = str(identity.get("node_id") or "")
    counts = {
        "repaired": 0,
        "failed": 0,
        "skipped": 0,
        "outbound_installed": 0,
        "outbound_errors": 0,
        "missing_source_keys": 0,
    }
    results: list[dict[str, Any]] = []
    total = len(nodes)
    report_progress({"phase": "starting", "done": 0, "total": total, "counts": dict(counts)})

    for index, node in enumerate(nodes, start=1):
        node_id = str(node.get("node_id") or "")
        label = str(node.get("pbname") or node.get("hostname") or node_id)
        mode = normalize_node_sync_mode(node)
        report_progress({
            "phase": "repairing",
            "done": index - 1,
            "total": total,
            "current_node_id": node_id,
            "current_pbname": label,
            "counts": dict(counts),
        })
        if not node_id or node.get("enabled") is False or mode == "disabled":
            counts["skipped"] += 1
            results.append({"node_id": node_id, "pbname": label, "ok": True, "status": "skipped", "reason": "sync is disabled"})
            report_progress({"phase": "repairing", "done": index, "total": total, "counts": dict(counts)})
            continue
        if node_id != local_node_id and mode == "outbound_only":
            counts["skipped"] += 1
            results.append({"node_id": node_id, "pbname": label, "ok": True, "status": "skipped", "reason": "node is outbound-only"})
            report_progress({"phase": "repairing", "done": index, "total": total, "counts": dict(counts)})
            continue
        try:
            result = await _repair_node_cluster_ssh(node, identity, nodes, ssh_passwords=ssh_passwords)
        except HTTPException as exc:
            counts["failed"] += 1
            results.append({"node_id": node_id, "pbname": label, "ok": False, "status": "failed", "reason": str(exc.detail)})
            report_progress({"phase": "repairing", "done": index, "total": total, "counts": dict(counts)})
            continue
        outbound_installed = result.get("outbound_installed") if isinstance(result.get("outbound_installed"), list) else []
        outbound_errors = result.get("outbound_install_errors") if isinstance(result.get("outbound_install_errors"), list) else []
        missing_source_keys = result.get("missing_source_keys") if isinstance(result.get("missing_source_keys"), list) else []
        counts["repaired"] += 1
        counts["outbound_installed"] += len(outbound_installed)
        counts["outbound_errors"] += len(outbound_errors)
        counts["missing_source_keys"] += len(missing_source_keys)
        results.append(
            {
                "node_id": node_id,
                "pbname": label,
                "ok": not outbound_errors and not missing_source_keys,
                "status": "repaired",
                "changed": bool(result.get("changed")),
                "installed": result.get("installed") if isinstance(result.get("installed"), list) else [],
                "outbound_installed": outbound_installed,
                "outbound_install_errors": outbound_errors,
                "missing_source_keys": missing_source_keys,
            }
        )
        report_progress({"phase": "repairing", "done": index, "total": total, "counts": dict(counts)})
    _request_pbcluster_sync(_cluster_root())
    payload = {"ok": counts["failed"] == 0 and counts["outbound_errors"] == 0 and counts["missing_source_keys"] == 0, "counts": counts, "results": results}
    report_progress({"phase": "done", "done": total, "total": total, "counts": dict(counts)})
    return payload


async def _run_repair_all_ssh_job(job_id: str, ssh_passwords: dict[str, str]) -> None:
    """Run Repair All SSH in the background and update local progress."""

    def progress(update: dict[str, Any]) -> None:
        _update_repair_all_ssh_job(
            job_id,
            status="running",
            phase=str(update.get("phase") or "running"),
            done=int(update.get("done") or 0),
            total=int(update.get("total") or 0),
            current_node_id=str(update.get("current_node_id") or ""),
            current_pbname=str(update.get("current_pbname") or ""),
            counts=update.get("counts") if isinstance(update.get("counts"), dict) else {},
        )

    _update_repair_all_ssh_job(job_id, status="running", phase="starting")
    try:
        result = await _repair_all_cluster_ssh_run(ssh_passwords, progress_callback=progress)
    except Exception as exc:
        _log(SERVICE, f"Repair All SSH job failed: {exc}", level="ERROR")
        _update_repair_all_ssh_job(job_id, status="error", phase="error", error=str(exc))
        return
    counts = result.get("counts") if isinstance(result.get("counts"), dict) else {}
    _update_repair_all_ssh_job(
        job_id,
        status="done",
        phase="done",
        done=int(_REPAIR_ALL_SSH_JOBS.get(job_id, {}).get("total") or 0),
        current_node_id="",
        current_pbname="",
        counts=counts,
        result=result,
    )


@router.post("/cluster-ssh/repair-all")
async def repair_all_cluster_ssh(request: Request, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Repair Cluster SSH trust for all active reachable nodes."""

    request_payload = await _optional_json_payload(request)
    ssh_passwords = _normalize_ssh_passwords(request_payload)
    return await _repair_all_cluster_ssh_run(ssh_passwords)


@router.post("/cluster-ssh/repair-all/start")
async def start_repair_all_cluster_ssh(request: Request, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Start a background Repair All SSH job and return its local progress record."""

    request_payload = await _optional_json_payload(request)
    ssh_passwords = _normalize_ssh_passwords(request_payload)
    job = _create_repair_all_ssh_job()
    asyncio.create_task(_run_repair_all_ssh_job(str(job["job_id"]), ssh_passwords))
    return _public_repair_all_ssh_job(job)


@router.get("/cluster-ssh/repair-all-jobs/{job_id}")
def get_repair_all_cluster_ssh_job(job_id: str, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Return local progress for one Repair All SSH job."""

    _prune_repair_all_ssh_jobs()
    job = _REPAIR_ALL_SSH_JOBS.get(str(job_id or ""))
    if not job:
        raise HTTPException(status_code=404, detail="Repair All SSH job not found")
    return _public_repair_all_ssh_job(job)


@router.post("/nodes/{node_id}/cluster-ssh/repair")
async def repair_node_cluster_ssh(node_id: str, request: Request, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Repair Cluster SSH key trust for one known node."""

    request_payload = await _optional_json_payload(request)
    ssh_passwords = _normalize_ssh_passwords(request_payload)
    snapshot = _load_cluster_snapshot()
    nodes = _node_list(snapshot["cluster_nodes"])
    node = _node_for_id(nodes, str(node_id or ""))
    if not node:
        raise HTTPException(status_code=404, detail="Cluster node not found")
    result = await _repair_node_cluster_ssh(node, snapshot["identity"], nodes, ssh_passwords=ssh_passwords)
    _request_pbcluster_sync(_cluster_root())
    return result


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


@router.post("/self-join")
async def self_join_existing_cluster(
    request: Request,
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """Join this master to an existing cluster through an outbound SSH connection."""

    try:
        payload = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Request body must be valid JSON") from exc
    settings = _validate_self_join_payload(payload)
    try:
        return await _self_join_existing_cluster(settings)
    except HTTPException:
        raise
    except Exception as exc:
        _log(SERVICE, f"Self-join failed for {settings.get('hostname')}: {exc}", level="ERROR")
        raise HTTPException(status_code=500, detail="Self-join failed") from exc


@router.post("/self-join/start")
async def start_self_join_existing_cluster(
    request: Request,
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """Start a background self-join job and return its local progress record."""

    try:
        payload = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Request body must be valid JSON") from exc
    settings = _validate_self_join_payload(payload)
    job = _create_self_join_job(settings)
    asyncio.create_task(_run_self_join_job(str(job["job_id"]), settings))
    return _public_self_join_job(job)


@router.get("/self-join-jobs/{job_id}")
def get_self_join_job(job_id: str, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Return local progress for one background self-join job."""

    _prune_self_join_jobs()
    job = _SELF_JOIN_JOBS.get(str(job_id or ""))
    if not job:
        raise HTTPException(status_code=404, detail="Self-join job not found")
    return _public_self_join_job(job)


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
    pbrun_start = await _maybe_start_remote_pbrun_after_materialization(node, snapshot["identity"])
    return {
        "ok": True,
        "node_id": str(node.get("node_id") or ""),
        "hostname": str(node.get("pbname") or node.get("hostname") or ""),
        "materialization": result,
        "pbrun_start": pbrun_start,
        "message": "Remote V7 configs materialized.",
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
    pbrun_start = await _maybe_start_remote_pbrun_after_materialization(node, snapshot["identity"])
    return {
        "ok": True,
        "node_id": str(node.get("node_id") or ""),
        "hostname": str(node.get("pbname") or node.get("hostname") or ""),
        "materialization": result,
        "pbrun_start": pbrun_start,
        "message": "Remote api-keys.json materialized.",
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


@router.post("/bootstrap/nodes/{hostname}")
def apply_bootstrap_node(hostname: str, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Write the bootstrap ADD_NODE/UPDATE_NODE operation for one known VPS host only."""

    del session
    target = str(hostname or "").strip()
    try:
        _validate_instance_name(target)
        plan = _build_bootstrap_plan()
        item = next(
            (
                item
                for item in plan.get("items", [])
                if str(item.get("type") or "") == "node"
                and str(item.get("hostname") or item.get("pbname") or "").strip() == target
            ),
            None,
        )
        if not item:
            raise HTTPException(status_code=404, detail="VPS host is not known to VPS Manager")

        action = str(item.get("action") or "")
        if action not in {"add", "update"}:
            return {
                "ok": True,
                "changed": False,
                "before": item,
                "result": {
                    "applied": [],
                    "skipped": [{"type": "node", "name": target, "action": action, "reason": item.get("reason") or ""}],
                    "failed": [],
                    "counts": {"applied": 0, "skipped": 1, "failed": 0},
                },
                "after": _build_bootstrap_plan(),
            }

        result = _apply_bootstrap_plan({"items": [item]})
        return {
            "ok": result["counts"]["failed"] == 0,
            "changed": bool(result["counts"].get("applied")),
            "before": item,
            "result": result,
            "after": _build_bootstrap_plan(),
        }
    except HTTPException:
        raise
    except Exception as exc:
        _log(SERVICE, f"Failed to apply cluster bootstrap node for {target}: {exc}", level="ERROR")
        raise HTTPException(status_code=500, detail="Failed to apply bootstrap node") from exc


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
