"""Cluster state helpers for multi-master PBGui synchronization.

This module is intentionally local-only. It creates stable cluster/node
identity files, validates and appends operations, computes V7 config
manifests, and rebuilds materialized cluster state from the oplog.
Remote transport, UI wiring, and PBRun integration are separate phases.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

SERVICE = "ClusterState"

SCHEMA_VERSION = 1
CLUSTER_ID_PREFIX = "pbgui-cluster-"
NODE_ID_PREFIX = "pbgui-node-"

MEMBERSHIP_OPS = frozenset({
    "ADD_NODE",
    "UPDATE_NODE",
    "UPDATE_NODE_ADDRESS",
    "UPDATE_NODE_SSH",
    "UPDATE_NODE_KEY",
    "DISABLE_NODE",
    "REMOVE_NODE",
})
V7_OPS = frozenset({
    "UPSERT_CONFIG",
    "MOVE_INSTANCE",
    "START_INSTANCE",
    "STOP_INSTANCE",
    "DELETE_INSTANCE",
    "TOMBSTONE_INSTANCE",
})
API_KEY_OPS = frozenset({"UPSERT_API_KEYS"})
SUPPORTED_OPS = MEMBERSHIP_OPS | V7_OPS | API_KEY_OPS
SYNC_MODES = frozenset({"disabled", "outbound_only", "reachable"})

SYNC_EXCLUDE_FILES = frozenset({
    "approved_coins.json",
    "config_run.json",
    "ignored_coins.json",
    "running_version.txt",
})


class ClusterStateError(ValueError):
    """Raised when cluster state data is invalid."""


@dataclass(frozen=True)
class ClusterPaths:
    """Resolved paths for one cluster state directory."""

    root: Path
    cluster_id: Path
    node_id: Path
    node_identity: Path
    cluster_nodes: Path
    desired_state: Path
    state_vector: Path
    oplog: Path
    config_blobs: Path
    secret_blobs: Path

    @classmethod
    def from_root(cls, root: Path) -> "ClusterPaths":
        """Build path metadata for *root*."""

        root = Path(root)
        return cls(
            root=root,
            cluster_id=root / "cluster_id",
            node_id=root / "node_id",
            node_identity=root / "node_identity.json",
            cluster_nodes=root / "cluster_nodes.json",
            desired_state=root / "desired_state.json",
            state_vector=root / "state_vector.json",
            oplog=root / "oplog",
            config_blobs=root / "config_blobs",
            secret_blobs=root / "secret_blobs",
        )


def default_cluster_root(pbgui_root: Path) -> Path:
    """Return the default cluster state directory for a PBGui root."""

    return Path(pbgui_root) / "data" / "cluster"


def generate_cluster_id() -> str:
    """Return a new stable cluster identifier."""

    return f"{CLUSTER_ID_PREFIX}{uuid.uuid4()}"


def generate_node_id() -> str:
    """Return a new stable node identifier."""

    return f"{NODE_ID_PREFIX}{uuid.uuid4()}"


def ensure_local_identity(
    cluster_root: Path,
    *,
    role: str = "master",
    pbname: str | None = None,
    cluster_id: str | None = None,
    node_id: str | None = None,
    created_at: int | None = None,
) -> dict[str, Any]:
    """Create or load local cluster identity files.

    Existing ``cluster_id`` and ``node_id`` files always win. Optional IDs
    are used only for first-time initialization or remote join setup.
    """

    paths = ClusterPaths.from_root(cluster_root)
    paths.root.mkdir(parents=True, exist_ok=True)

    active_cluster_id = _read_text(paths.cluster_id)
    if active_cluster_id is None:
        active_cluster_id = cluster_id or generate_cluster_id()
        _validate_cluster_id(active_cluster_id)
        _atomic_write_text(paths.cluster_id, active_cluster_id)
    else:
        _validate_cluster_id(active_cluster_id)
        if cluster_id and cluster_id != active_cluster_id:
            raise ClusterStateError("existing cluster_id differs from requested cluster_id")

    active_node_id = _read_text(paths.node_id)
    if active_node_id is None:
        active_node_id = node_id or generate_node_id()
        _validate_node_id(active_node_id)
        _atomic_write_text(paths.node_id, active_node_id)
    else:
        _validate_node_id(active_node_id)
        if node_id and node_id != active_node_id:
            raise ClusterStateError("existing node_id differs from requested node_id")

    identity = {
        "schema_version": SCHEMA_VERSION,
        "cluster_id": active_cluster_id,
        "node_id": active_node_id,
        "created_at": int(created_at if created_at is not None else time.time()),
        "created_from_pbname": str(pbname or ""),
        "role": str(role or "master"),
    }
    if paths.node_identity.exists():
        try:
            existing = json.loads(paths.node_identity.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            existing = {}
        if isinstance(existing, dict):
            identity["created_at"] = int(existing.get("created_at") or identity["created_at"])
            identity["created_from_pbname"] = str(
                existing.get("created_from_pbname") or identity["created_from_pbname"]
            )
    _atomic_write_json(paths.node_identity, identity)
    return identity


def normalize_node_sync_mode(node: dict[str, Any]) -> str:
    """Return the effective Cluster Sync peer mode for a node record."""

    if not isinstance(node, dict):
        return "disabled"
    raw_mode = str(node.get("sync_mode") or "").strip().lower()
    if raw_mode in SYNC_MODES:
        return raw_mode
    if node.get("sync_enabled", True) is False:
        return "disabled"
    if str(node.get("ssh_host") or "").strip():
        return "reachable"
    return "outbound_only"


def normalize_node_sync_fields(node: dict[str, Any]) -> dict[str, Any]:
    """Normalize sync_mode and legacy sync_enabled on a node record."""

    mode = normalize_node_sync_mode(node)
    node["sync_mode"] = mode
    node["sync_enabled"] = mode != "disabled"
    return node


def read_local_identity(cluster_root: Path) -> dict[str, Any]:
    """Read and validate local cluster identity files."""

    paths = ClusterPaths.from_root(cluster_root)
    cluster_id = _read_text(paths.cluster_id)
    node_id = _read_text(paths.node_id)
    if cluster_id is None or node_id is None:
        raise ClusterStateError("cluster identity is not initialized")
    _validate_cluster_id(cluster_id)
    _validate_node_id(node_id)
    identity = _read_json(paths.node_identity) if paths.node_identity.exists() else {}
    if not isinstance(identity, dict):
        identity = {}
    identity.update({"cluster_id": cluster_id, "node_id": node_id})
    return identity


def append_operation(
    cluster_root: Path,
    op: str,
    payload: dict[str, Any] | None = None,
    *,
    actor: str | None = None,
    cluster_id: str | None = None,
    created_at: int | None = None,
) -> dict[str, Any]:
    """Append one operation to the local oplog and return it."""

    identity = read_local_identity(cluster_root)
    active_actor = actor or str(identity["node_id"])
    active_cluster_id = cluster_id or str(identity["cluster_id"])
    _validate_node_id(active_actor)
    _validate_cluster_id(active_cluster_id)

    seq = _next_seq(ClusterPaths.from_root(cluster_root), active_actor)
    operation = dict(payload or {})
    operation.update({
        "schema_version": SCHEMA_VERSION,
        "cluster_id": active_cluster_id,
        "op_id": f"{active_actor}:{seq:08d}",
        "actor": active_actor,
        "seq": seq,
        "op": str(op),
        "created_at": int(created_at if created_at is not None else time.time()),
    })
    write_operation(cluster_root, operation)
    return operation


def write_operation(cluster_root: Path, operation: dict[str, Any]) -> Path:
    """Validate and atomically write one operation file."""

    validate_operation(operation)
    paths = ClusterPaths.from_root(cluster_root)
    actor = str(operation["actor"])
    seq = int(operation["seq"])
    op_dir = paths.oplog / actor
    op_path = op_dir / f"{seq:08d}.json"
    if op_path.exists():
        existing = _read_json(op_path)
        if existing == operation:
            return op_path
        raise ClusterStateError(f"operation already exists with different content: {op_path}")
    _atomic_write_json(op_path, operation)
    _touch_sync_request(paths.root)
    return op_path


def validate_operation(
    operation: dict[str, Any],
    *,
    expected_cluster_id: str | None = None,
) -> None:
    """Validate one cluster operation envelope and payload."""

    if not isinstance(operation, dict):
        raise ClusterStateError("operation must be a JSON object")
    required = {"schema_version", "cluster_id", "op_id", "actor", "seq", "op", "created_at"}
    missing = sorted(required - set(operation))
    if missing:
        raise ClusterStateError(f"operation missing required field(s): {', '.join(missing)}")

    if operation["schema_version"] != SCHEMA_VERSION:
        raise ClusterStateError("unsupported operation schema_version")
    cluster_id = str(operation["cluster_id"])
    _validate_cluster_id(cluster_id)
    if expected_cluster_id and cluster_id != expected_cluster_id:
        raise ClusterStateError("foreign cluster_id")

    actor = str(operation["actor"])
    _validate_node_id(actor)
    seq = _as_positive_int(operation["seq"], "seq")
    expected_op_id = f"{actor}:{seq:08d}"
    if operation["op_id"] != expected_op_id:
        raise ClusterStateError("op_id must match actor and seq")
    _as_int(operation["created_at"], "created_at")

    op = str(operation["op"])
    if op not in SUPPORTED_OPS:
        raise ClusterStateError(f"unsupported operation: {op}")
    if op in MEMBERSHIP_OPS:
        _validate_membership_payload(operation)
    elif op in V7_OPS:
        _validate_v7_payload(operation)
    elif op in API_KEY_OPS:
        _validate_api_key_payload(operation)


def load_operations(cluster_root: Path, *, expected_cluster_id: str | None = None) -> list[dict[str, Any]]:
    """Load all valid operations from an oplog in deterministic order."""

    paths = ClusterPaths.from_root(cluster_root)
    operations: list[dict[str, Any]] = []
    if not paths.oplog.exists():
        return operations
    for actor_dir in sorted(p for p in paths.oplog.iterdir() if p.is_dir()):
        for op_path in sorted(actor_dir.glob("*.json")):
            operation = _read_json(op_path)
            if not isinstance(operation, dict):
                raise ClusterStateError(f"operation file is not an object: {op_path}")
            validate_operation(operation, expected_cluster_id=expected_cluster_id)
            actor = str(operation["actor"])
            seq = int(operation["seq"])
            if actor_dir.name != actor or op_path.name != f"{seq:08d}.json":
                raise ClusterStateError(f"operation path does not match actor/seq: {op_path}")
            operations.append(operation)
    operations.sort(key=lambda item: (str(item["actor"]), int(item["seq"]), str(item["op_id"])))
    return operations


def rebuild_materialized_state(cluster_root: Path, *, write: bool = True) -> dict[str, Any]:
    """Rebuild materialized cluster files from the oplog."""

    identity = read_local_identity(cluster_root)
    cluster_id = str(identity["cluster_id"])
    operations = load_operations(cluster_root, expected_cluster_id=cluster_id)
    nodes: dict[str, dict[str, Any]] = {}
    instances: dict[str, dict[str, Any]] = {}
    tombstones: dict[str, dict[str, Any]] = {}
    api_keys: dict[str, Any] | None = None
    state_vector: dict[str, int] = {}
    parent_changes: dict[tuple[str, str], list[dict[str, Any]]] = {}
    generated_at = 0

    for operation in operations:
        actor = str(operation["actor"])
        state_vector[actor] = max(state_vector.get(actor, 0), int(operation["seq"]))
        generated_at = max(generated_at, int(operation.get("created_at") or 0))
        op = str(operation["op"])
        if op in MEMBERSHIP_OPS:
            _apply_membership(nodes, operation)
        elif op in V7_OPS:
            _apply_v7(instances, tombstones, parent_changes, operation)
        elif op == "UPSERT_API_KEYS":
            api_keys = {
                "serial": int(operation["api_serial"]),
                "payload_hash": str(operation["payload_hash"]),
                "secret_blob_hash": str(operation["secret_blob_hash"]),
                "updated_by": actor,
                "updated_at": int(operation["created_at"]),
            }

    _mark_conflicts(instances, parent_changes)

    cluster_nodes = {
        "schema_version": SCHEMA_VERSION,
        "cluster_id": cluster_id,
        "generation": len(operations),
        "nodes": {key: nodes[key] for key in sorted(nodes)},
    }
    desired_state: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "cluster_id": cluster_id,
        "generated_at": generated_at,
        "instances": {key: instances[key] for key in sorted(instances)},
        "tombstones": {key: tombstones[key] for key in sorted(tombstones)},
    }
    if api_keys is not None:
        desired_state["api_keys"] = api_keys
    materialized = {
        "cluster_nodes": cluster_nodes,
        "desired_state": desired_state,
        "state_vector": {key: state_vector[key] for key in sorted(state_vector)},
    }
    if write:
        paths = ClusterPaths.from_root(cluster_root)
        _atomic_write_json(paths.cluster_nodes, cluster_nodes)
        _atomic_write_json(paths.desired_state, desired_state)
        _atomic_write_json(paths.state_vector, materialized["state_vector"])
    return materialized


def build_config_manifest(instance_dir: Path) -> dict[str, Any]:
    """Build a stable manifest for syncable V7 JSON config files."""

    base = Path(instance_dir)
    files: dict[str, dict[str, Any]] = {}
    if not base.is_dir():
        raise ClusterStateError(f"instance directory does not exist: {base}")
    for item in sorted(base.iterdir(), key=lambda path: path.name):
        if not item.is_file() or item.suffix != ".json" or item.name in SYNC_EXCLUDE_FILES:
            continue
        _validate_relative_name(item.name, "config filename")
        raw = item.read_bytes()
        files[item.name] = {
            "sha256": hashlib.sha256(raw).hexdigest(),
            "size": len(raw),
        }
    return {"schema_version": SCHEMA_VERSION, "files": files}


def compute_config_manifest_hash(manifest: dict[str, Any]) -> str:
    """Return the content hash for a config manifest."""

    raw = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return f"sha256:{hashlib.sha256(raw).hexdigest()}"


def _touch_sync_request(cluster_root: Path) -> None:
    """Best-effort notification for PBCluster that new oplog data exists."""

    try:
        paths = ClusterPaths.from_root(cluster_root)
        paths.root.mkdir(parents=True, exist_ok=True)
        (paths.root / "sync_request").touch()
    except OSError:
        pass


def detect_duplicate_node_ids(records: Iterable[dict[str, Any]]) -> set[str]:
    """Return node IDs that appear more than once in *records*."""

    seen: set[str] = set()
    duplicates: set[str] = set()
    for record in records:
        node_id = str((record or {}).get("node_id") or "")
        if not node_id:
            continue
        if node_id in seen:
            duplicates.add(node_id)
        seen.add(node_id)
    return duplicates


def _apply_membership(nodes: dict[str, dict[str, Any]], operation: dict[str, Any]) -> None:
    """Apply a membership operation to materialized nodes."""

    op = str(operation["op"])
    node_id = str(operation["node_id"])
    current = dict(nodes.get(node_id, {"node_id": node_id}))
    if op == "DISABLE_NODE":
        current["enabled"] = False
        current["sync_mode"] = "disabled"
        current["sync_enabled"] = False
        current["updated_at"] = int(operation["created_at"])
        nodes[node_id] = current
        return
    if op == "REMOVE_NODE":
        nodes.pop(node_id, None)
        return
    if "sync_enabled" in operation and "sync_mode" not in operation:
        current.pop("sync_mode", None)
    for key, value in operation.items():
        if key in {"schema_version", "cluster_id", "op_id", "actor", "seq", "op", "created_at"}:
            continue
        current[key] = value
    current.setdefault("enabled", True)
    normalize_node_sync_fields(current)
    current["updated_at"] = int(operation["created_at"])
    nodes[node_id] = current


def _apply_v7(
    instances: dict[str, dict[str, Any]],
    tombstones: dict[str, dict[str, Any]],
    parent_changes: dict[tuple[str, str], list[dict[str, Any]]],
    operation: dict[str, Any],
) -> None:
    """Apply a V7 operation to materialized desired state."""

    op = str(operation["op"])
    instance = str(operation["instance"])
    if op in {"DELETE_INSTANCE", "TOMBSTONE_INSTANCE"}:
        tombstones[instance] = {
            "version": str(operation.get("version") or ""),
            "deleted_by": str(operation["actor"]),
            "deleted_at": int(operation["created_at"]),
            "op_id": str(operation["op_id"]),
        }
        instances.pop(instance, None)
        return

    if instance in tombstones:
        if op == "UPSERT_CONFIG" and operation.get("allow_tombstone_recreate") is True:
            tombstones.pop(instance, None)
        else:
            return
    current = dict(instances.get(instance, {"conflicted": False}))
    if op == "UPSERT_CONFIG":
        current.update({
            "version": str(operation["version"]),
            "desired_state": str(operation.get("desired_state") or current.get("desired_state") or "stopped"),
            "assigned_host": str(operation["assigned_host"]),
            "config_manifest_hash": str(operation["config_manifest_hash"]),
            "updated_by": str(operation["actor"]),
            "updated_at": int(operation["created_at"]),
            "conflicted": False,
        })
        _track_parent_change(parent_changes, operation, current)
    elif op == "MOVE_INSTANCE":
        current.update({
            "version": str(operation["version"]),
            "assigned_host": str(operation["to"]),
            "desired_state": str(operation.get("desired_state") or current.get("desired_state") or "stopped"),
            "updated_by": str(operation["actor"]),
            "updated_at": int(operation["created_at"]),
            "conflicted": False,
        })
        if "config_manifest_hash" in operation:
            current["config_manifest_hash"] = str(operation["config_manifest_hash"])
        _track_parent_change(parent_changes, operation, current)
    elif op == "START_INSTANCE":
        current.update({
            "desired_state": "running",
            "updated_by": str(operation["actor"]),
            "updated_at": int(operation["created_at"]),
        })
    elif op == "STOP_INSTANCE":
        current.update({
            "desired_state": "stopped",
            "updated_by": str(operation["actor"]),
            "updated_at": int(operation["created_at"]),
        })
    instances[instance] = current


def _track_parent_change(
    parent_changes: dict[tuple[str, str], list[dict[str, Any]]],
    operation: dict[str, Any],
    materialized: dict[str, Any],
) -> None:
    """Track changes that can conflict by parent version."""

    parent = operation.get("parent_version")
    if parent is None:
        return
    key = (str(operation["instance"]), str(parent))
    parent_changes.setdefault(key, []).append({
        "op_id": str(operation["op_id"]),
        "parent_version": str(parent),
        "version": str(operation.get("version") or materialized.get("version") or ""),
        "assigned_host": str(materialized.get("assigned_host") or ""),
        "desired_state": str(materialized.get("desired_state") or ""),
        "config_manifest_hash": str(materialized.get("config_manifest_hash") or ""),
    })


def _mark_conflicts(
    instances: dict[str, dict[str, Any]],
    parent_changes: dict[tuple[str, str], list[dict[str, Any]]],
) -> None:
    """Mark instance conflicts created from the same parent version."""

    for (instance, _parent), changes in parent_changes.items():
        signatures = {
            (
                item["version"],
                item["assigned_host"],
                item["desired_state"],
                item["config_manifest_hash"],
            )
            for item in changes
        }
        if len(changes) > 1 and len(signatures) > 1 and instance in instances:
            instances[instance]["conflicted"] = True
            instances[instance]["conflicts"] = changes


def _validate_membership_payload(operation: dict[str, Any]) -> None:
    """Validate operation-specific membership fields."""

    if "node_id" not in operation:
        raise ClusterStateError("membership operation missing node_id")
    _validate_node_id(str(operation["node_id"]))
    if "role" in operation and str(operation["role"]) not in {"master", "vps"}:
        raise ClusterStateError("node role must be master or vps")
    if "sync_mode" in operation and str(operation["sync_mode"] or "").strip().lower() not in SYNC_MODES:
        raise ClusterStateError("node sync_mode must be disabled, outbound_only or reachable")
    if "sync_peers" in operation:
        sync_peers = operation.get("sync_peers")
        if not isinstance(sync_peers, list):
            raise ClusterStateError("node sync_peers must be a list")
        for peer_id in sync_peers:
            _validate_node_id(str(peer_id))


def _validate_v7_payload(operation: dict[str, Any]) -> None:
    """Validate operation-specific V7 fields."""

    op = str(operation["op"])
    if "instance" not in operation:
        raise ClusterStateError("V7 operation missing instance")
    _validate_relative_name(str(operation["instance"]), "instance")
    if op in {"UPSERT_CONFIG", "MOVE_INSTANCE", "DELETE_INSTANCE", "TOMBSTONE_INSTANCE"}:
        if "version" not in operation:
            raise ClusterStateError(f"{op} missing version")
    if op == "UPSERT_CONFIG":
        for field in ("assigned_host", "config_manifest_hash"):
            if field not in operation:
                raise ClusterStateError(f"UPSERT_CONFIG missing {field}")
        _validate_node_id(str(operation["assigned_host"]))
        _validate_hash(str(operation["config_manifest_hash"]), "config_manifest_hash")
    if op == "MOVE_INSTANCE":
        for field in ("from", "to"):
            if field not in operation:
                raise ClusterStateError(f"MOVE_INSTANCE missing {field}")
            _validate_node_id(str(operation[field]))
        if "config_manifest_hash" in operation:
            _validate_hash(str(operation["config_manifest_hash"]), "config_manifest_hash")


def _validate_api_key_payload(operation: dict[str, Any]) -> None:
    """Validate operation-specific API-key fields."""

    _as_positive_int(operation.get("api_serial"), "api_serial")
    for field in ("payload_hash", "secret_blob_hash"):
        if field not in operation:
            raise ClusterStateError(f"UPSERT_API_KEYS missing {field}")
        _validate_hash(str(operation[field]), field)


def _validate_cluster_id(value: str) -> None:
    """Validate a cluster ID."""

    if not value.startswith(CLUSTER_ID_PREFIX):
        raise ClusterStateError("invalid cluster_id prefix")
    _validate_uuid_suffix(value, CLUSTER_ID_PREFIX, "cluster_id")


def _validate_node_id(value: str) -> None:
    """Validate a node ID."""

    if not value.startswith(NODE_ID_PREFIX):
        raise ClusterStateError("invalid node_id prefix")
    _validate_uuid_suffix(value, NODE_ID_PREFIX, "node_id")


def _validate_uuid_suffix(value: str, prefix: str, label: str) -> None:
    """Validate that *value* has a UUID suffix after *prefix*."""

    try:
        uuid.UUID(value[len(prefix):])
    except (TypeError, ValueError) as exc:
        raise ClusterStateError(f"invalid {label}") from exc


def _validate_hash(value: str, field: str) -> None:
    """Validate a sha256:<hex> hash reference."""

    if not value.startswith("sha256:") or len(value) != len("sha256:") + 64:
        raise ClusterStateError(f"invalid {field}")
    try:
        int(value[len("sha256:"):], 16)
    except ValueError as exc:
        raise ClusterStateError(f"invalid {field}") from exc


def _validate_relative_name(value: str, field: str) -> None:
    """Reject names that can escape a cluster-managed directory."""

    if not value or value in {".", ".."} or "/" in value or "\\" in value or "\x00" in value:
        raise ClusterStateError(f"invalid {field}")


def _as_int(value: Any, field: str) -> int:
    """Return *value* as int or raise ClusterStateError."""

    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ClusterStateError(f"{field} must be an integer") from exc


def _as_positive_int(value: Any, field: str) -> int:
    """Return *value* as positive int or raise ClusterStateError."""

    result = _as_int(value, field)
    if result < 1:
        raise ClusterStateError(f"{field} must be positive")
    return result


def _next_seq(paths: ClusterPaths, actor: str) -> int:
    """Return the next local actor sequence number."""

    op_dir = paths.oplog / actor
    if not op_dir.exists():
        return 1
    max_seq = 0
    for item in op_dir.glob("*.json"):
        try:
            max_seq = max(max_seq, int(item.stem))
        except ValueError:
            continue
    return max_seq + 1


def _read_text(path: Path) -> str | None:
    """Read a small text file, returning None when missing."""

    if not path.exists():
        return None
    return path.read_text(encoding="utf-8").strip()


def _read_json(path: Path) -> Any:
    """Read JSON from *path*."""

    return json.loads(path.read_text(encoding="utf-8"))


def _atomic_write_text(path: Path, value: str) -> None:
    """Atomically write text to *path*."""

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        tmp.write_text(str(value), encoding="utf-8")
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def _atomic_write_json(path: Path, value: Any) -> None:
    """Atomically write JSON to *path*."""

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        tmp.write_text(json.dumps(value, indent=4, sort_keys=True), encoding="utf-8")
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)
