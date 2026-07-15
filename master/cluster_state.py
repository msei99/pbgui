"""Cluster state helpers for multi-master PBGui synchronization.

This module is intentionally local-only. It creates stable cluster/node
identity files, validates and appends operations, computes V7 config
manifests, and rebuilds materialized cluster state from the oplog.
Remote transport, UI wiring, and PBRun integration are separate phases.
"""

from __future__ import annotations

import hashlib
import base64
import binascii
import json
import os
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable

from cluster_credentials import (
    activate_prepared_node_key_rotation,
    ClusterCredentialError,
    ensure_node_key_material,
    prepare_node_key_rotation,
    sign_operation,
    verify_operation,
)
from file_lock import advisory_file_lock

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
V2_CREDENTIAL_OPS = frozenset({
    "UPSERT_SECRET",
    "UPDATE_SECRET_RECIPIENTS",
    "TOMBSTONE_SECRET",
    "UPSERT_CMC_POOL_ENTRY",
    "SET_CMC_KEY_STATE",
    "SET_CMC_AUTHORITY",
    "SET_TRADFI_ACTIVE_PROFILE",
    "WRITER_FREEZE",
    "WRITER_FREEZE_ACK",
    "CREDENTIAL_INVENTORY_ACK",
    "CREDENTIAL_MATERIALIZATION_ACK",
    "CREDENTIAL_CUTOFF",
    "CREDENTIAL_CUTOFF_ACK",
    "MIGRATION_SECRET_CANDIDATE",
    "MIGRATION_SECRET_ACCEPTANCE",
    "MIGRATION_SECRET_CONFLICT",
    "MIGRATION_SECRET_CONFLICT_RESOLUTION",
    "TRADFI_PROJECTION_ACK",
    "CREDENTIAL_SCAN_ACK",
})
SUPPORTED_OPS = MEMBERSHIP_OPS | V7_OPS | API_KEY_OPS | V2_CREDENTIAL_OPS
SYNC_MODES = frozenset({"disabled", "outbound_only", "reachable"})
CMC_KEY_STATES = frozenset({
    "pending", "active", "draining", "disabled", "invalid",
    "provider_disabled", "minute_limited", "day_exhausted",
    "month_exhausted", "conflicted", "tombstoned",
})
CRYPTO_PUBLIC_FIELDS = frozenset({
    "signing_public_key",
    "signing_key_id",
    "encryption_public_key",
    "encryption_key_id",
})
MEMBERSHIP_SIGNATURE_FIELDS = frozenset({
    "signer_id",
    "signer_key_id",
    "signature",
    "signature_version",
    "signature_algorithm",
})
MASTER_ADMIN_MEMBERSHIP_OPS = frozenset({
    "UPDATE_NODE_ADDRESS",
    "UPDATE_NODE_SSH",
    "DISABLE_NODE",
    "REMOVE_NODE",
})
VPS_SELF_CREDENTIAL_OPS = frozenset({
    "WRITER_FREEZE_ACK",
    "CREDENTIAL_INVENTORY_ACK",
    "CREDENTIAL_MATERIALIZATION_ACK",
    "CREDENTIAL_CUTOFF_ACK",
    "MIGRATION_SECRET_CANDIDATE",
    "CREDENTIAL_SCAN_ACK",
})
MASTER_ONLY_CREDENTIAL_OPS = V2_CREDENTIAL_OPS - VPS_SELF_CREDENTIAL_OPS

SYNC_EXCLUDE_FILES = frozenset({
    "approved_coins.json",
    "config_run.json",
    "ignored_coins.json",
    "monitor_cache.json",
    "monitor.json",
    "running_version.txt",
})


class ClusterStateError(ValueError):
    """Raised when cluster state data is invalid."""


@dataclass
class MembershipTrust:
    """Authenticated membership state plus sequence-bounded signing keys."""

    nodes: dict[str, dict[str, Any]]
    removed_node_ids: set[str]
    signing_keys: dict[str, list[dict[str, Any]]]
    role_history: dict[str, list[dict[str, Any]]]
    validated_op_ids: set[str]

    @classmethod
    def empty(cls) -> "MembershipTrust":
        """Return an empty membership trust state."""

        return cls(
            nodes={},
            removed_node_ids=set(),
            signing_keys={},
            role_history={},
            validated_op_ids=set(),
        )


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
    sealed_blobs: Path
    mailbox: Path

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
            sealed_blobs=root / "sealed_blobs",
            mailbox=root / "mailbox",
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

    paths = ClusterPaths.from_root(cluster_root)
    with advisory_file_lock(paths.root / ".append_sequence"):
        keys = None
        operation_name = str(op)
        if operation_name in MEMBERSHIP_OPS | V2_CREDENTIAL_OPS:
            if active_actor != str(identity["node_id"]):
                raise ClusterStateError("signed operations can only use the local actor")
            try:
                keys = ensure_node_key_material(cluster_root)
            except ClusterCredentialError as exc:
                raise ClusterStateError(str(exc)) from exc
        if operation_name in MEMBERSHIP_OPS - {"ADD_NODE"}:
            assert keys is not None
            _ensure_local_crypto_membership(
                paths,
                identity,
                keys.public_bundle(active_actor, str(identity.get("role") or "master")),
                created_at=int(created_at if created_at is not None else time.time()),
            )
        if operation_name in V2_CREDENTIAL_OPS:
            assert keys is not None
            _ensure_local_crypto_membership(
                paths,
                identity,
                keys.public_bundle(active_actor, str(identity.get("role") or "master")),
                created_at=int(created_at if created_at is not None else time.time()),
            )
        seq = _next_seq(paths, active_actor)
        operation = dict(payload or {})
        if operation_name in V2_CREDENTIAL_OPS:
            trust = _load_membership_trust(paths.root, expected_cluster_id=active_cluster_id)
            role_events = trust.role_history.get(active_actor, [])
            if not role_events:
                raise ClusterStateError("credential operation actor has no authenticated membership epoch")
            role_event = role_events[-1]
            operation["actor_role_epoch"] = int(role_event.get("role_epoch") or 0)
            operation["actor_membership_op_id"] = str(
                role_event.get("membership_op_id") or ""
            )
        if operation_name == "ADD_NODE":
            target_node_id = str(operation.get("node_id") or "")
            if target_node_id != active_actor:
                raise ClusterStateError("ADD_NODE must be a self-add by the local actor")
            assert keys is not None
            operation.update(keys.public_bundle(active_actor, str(identity.get("role") or "master")))
            operation.setdefault("membership_authorization", {"kind": "bootstrap"})
            operation.setdefault("state_replica", True)
            operation.setdefault("credential_protocol_version", 2)
            operation.setdefault("credential_capable", True)
        operation.update({
            "schema_version": SCHEMA_VERSION,
            "cluster_id": active_cluster_id,
            "op_id": f"{active_actor}:{seq:08d}",
            "actor": active_actor,
            "seq": seq,
            "op": operation_name,
            "created_at": int(created_at if created_at is not None else time.time()),
        })
        if operation_name in MEMBERSHIP_OPS | V2_CREDENTIAL_OPS:
            try:
                assert keys is not None
                operation = sign_operation(operation, keys.signing_private_key, signer_id=active_actor)
            except ClusterCredentialError as exc:
                raise ClusterStateError(str(exc)) from exc
        if operation_name in API_KEY_OPS:
            _validate_api_key_operation_after_cutoff(cluster_root, operation)
        if operation_name == "MIGRATION_SECRET_CANDIDATE":
            _validate_candidate_against_current_freeze(cluster_root, operation)
        write_operation(
            cluster_root,
            operation,
            network_input=operation_name in V2_CREDENTIAL_OPS,
        )
        return operation


def append_node_placeholder(
    cluster_root: Path,
    payload: dict[str, Any],
    *,
    created_at: int | None = None,
) -> dict[str, Any]:
    """Record master-managed inventory without granting replica membership."""

    placeholder = dict(payload)
    placeholder["state_replica"] = False
    return append_operation(
        cluster_root,
        "UPDATE_NODE",
        placeholder,
        created_at=created_at,
    )


def _ensure_local_crypto_membership(
    paths: ClusterPaths,
    identity: dict[str, Any],
    public_bundle: dict[str, str],
    *,
    created_at: int,
) -> None:
    """Publish the local crypto bundle before its first signed operation."""

    cluster_id = str(identity["cluster_id"])
    node_id = str(identity["node_id"])
    trust = _load_membership_trust(paths.root, expected_cluster_id=cluster_id)
    current = trust.nodes.get(node_id)
    if current and all(current.get(field) == public_bundle[field] for field in CRYPTO_PUBLIC_FIELDS):
        return
    if current is None and _membership_target_was_removed(paths, node_id):
        raise ClusterStateError("removed local node cannot publish credential operations")
    seq = _next_seq(paths, node_id)
    operation = {
        **public_bundle,
        "schema_version": SCHEMA_VERSION,
        "cluster_id": cluster_id,
        "op_id": f"{node_id}:{seq:08d}",
        "actor": node_id,
        "seq": seq,
        "op": "UPDATE_NODE_KEY" if current is not None else "ADD_NODE",
        "created_at": created_at,
        "node_id": node_id,
        "role": str(identity.get("role") or "master"),
        "state_replica": True,
        "credential_protocol_version": 2,
        "credential_capable": True,
    }
    if current is None:
        operation["membership_authorization"] = {"kind": "bootstrap"}
    try:
        keys = ensure_node_key_material(paths.root)
        operation = sign_operation(operation, keys.signing_private_key, signer_id=node_id)
    except ClusterCredentialError as exc:
        raise ClusterStateError(str(exc)) from exc
    write_operation(paths.root, operation)


def _membership_target_was_removed(paths: ClusterPaths, node_id: str) -> bool:
    """Return whether membership history permanently removed one node ID."""

    if not paths.oplog.exists():
        return False
    for actor_dir in paths.oplog.iterdir():
        if not actor_dir.is_dir():
            continue
        for op_path in actor_dir.glob("*.json"):
            try:
                operation = _read_json(op_path)
            except (OSError, json.JSONDecodeError):
                continue
            if (
                isinstance(operation, dict)
                and operation.get("op") == "REMOVE_NODE"
                and operation.get("node_id") == node_id
            ):
                return True
    return False


def write_operation(
    cluster_root: Path,
    operation: dict[str, Any],
    *,
    network_input: bool = False,
    membership_trust: MembershipTrust | None = None,
    allow_legacy_membership: bool = False,
    allow_legacy_key_claim: bool = False,
) -> Path:
    """Validate and atomically write one operation file."""

    validate_operation(
        operation,
        cluster_root=cluster_root,
        membership_trust=membership_trust,
        network_input=network_input,
        allow_legacy_membership=allow_legacy_membership,
        allow_legacy_key_claim=allow_legacy_key_claim,
    )
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
    cluster_root: Path | None = None,
    membership_nodes: dict[str, dict[str, Any]] | None = None,
    membership_trust: MembershipTrust | None = None,
    network_input: bool = False,
    validate_membership_auth: bool = True,
    allow_legacy_membership: bool = False,
    allow_legacy_key_claim: bool = False,
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
        if not validate_membership_auth:
            return
        trust = membership_trust
        if trust is None and cluster_root is not None:
            trust = _load_membership_trust(cluster_root, expected_cluster_id=cluster_id)
        if trust is None:
            trust = _trust_from_nodes(membership_nodes or {})
        _accept_membership_operation(
            trust,
            operation,
            allow_unsigned=allow_legacy_membership,
            allow_legacy_key_claim=allow_legacy_key_claim or not network_input,
        )
    elif op in V7_OPS:
        _validate_v7_payload(operation)
    elif op in API_KEY_OPS:
        _validate_api_key_payload(operation)
        if network_input and cluster_root is not None:
            _validate_api_key_operation_after_cutoff(cluster_root, operation)
    elif op in V2_CREDENTIAL_OPS:
        _validate_v2_credential_payload(operation)
        trust = membership_trust
        if trust is None and cluster_root is not None:
            trust = _load_membership_trust(cluster_root, expected_cluster_id=cluster_id)
        if trust is None:
            trust = _trust_from_nodes(membership_nodes or {})
        _validate_v2_operation_signature(operation, trust, network_input=network_input)
        _validate_v2_actor_role(operation, trust, network_input=network_input)
        if network_input and cluster_root is not None and op == "MIGRATION_SECRET_CANDIDATE":
            _validate_candidate_against_current_freeze(cluster_root, operation)


def load_operations(cluster_root: Path, *, expected_cluster_id: str | None = None) -> list[dict[str, Any]]:
    """Load all valid operations from an oplog in deterministic order."""

    paths = ClusterPaths.from_root(cluster_root)
    operations: list[dict[str, Any]] = []
    if not paths.oplog.exists():
        return operations
    trust = _load_membership_trust(
        cluster_root,
        expected_cluster_id=expected_cluster_id or str(read_local_identity(cluster_root)["cluster_id"]),
    )
    for actor_dir in sorted(p for p in paths.oplog.iterdir() if p.is_dir()):
        for op_path in sorted(actor_dir.glob("*.json")):
            operation = _read_json(op_path)
            if not isinstance(operation, dict):
                raise ClusterStateError(f"operation file is not an object: {op_path}")
            validate_operation(
                operation,
                expected_cluster_id=expected_cluster_id,
                cluster_root=cluster_root,
                membership_trust=trust,
                allow_legacy_membership=True,
            )
            actor = str(operation["actor"])
            seq = int(operation["seq"])
            if actor_dir.name != actor or op_path.name != f"{seq:08d}.json":
                raise ClusterStateError(f"operation path does not match actor/seq: {op_path}")
            operations.append(operation)
    operations.sort(key=lambda item: (int(item["created_at"]), str(item["actor"]), int(item["seq"]), str(item["op_id"])))
    return operations


def rebuild_materialized_state(cluster_root: Path, *, write: bool = True) -> dict[str, Any]:
    """Rebuild materialized cluster files from the oplog."""

    identity = read_local_identity(cluster_root)
    cluster_id = str(identity["cluster_id"])
    operations = load_operations(cluster_root, expected_cluster_id=cluster_id)
    nodes: dict[str, dict[str, Any]] = {}
    removed_node_ids: set[str] = set()
    instances: dict[str, dict[str, Any]] = {}
    tombstones: dict[str, dict[str, Any]] = {}
    api_key_operations: list[dict[str, Any]] = []
    actor_sequences: dict[str, set[int]] = {}
    parent_changes: dict[tuple[str, str], list[dict[str, Any]]] = {}
    generated_at = 0
    credential_membership_generation = 0

    for operation in operations:
        actor = str(operation["actor"])
        actor_sequences.setdefault(actor, set()).add(int(operation["seq"]))
        generated_at = max(generated_at, int(operation.get("created_at") or 0))
        op = str(operation["op"])
        if op in MEMBERSHIP_OPS:
            before = _credential_membership_fingerprint(nodes)
            _apply_membership(nodes, removed_node_ids, operation)
            if _credential_membership_fingerprint(nodes) != before:
                credential_membership_generation += 1
        elif op in V7_OPS:
            _apply_v7(instances, tombstones, parent_changes, operation)
        elif op == "UPSERT_API_KEYS":
            api_key_operations.append(operation)

    _mark_conflicts(instances, parent_changes)
    v2_state = _materialize_v2_credentials(operations)
    cutoff = (v2_state.get("credential_migration") or {}).get("cutoff") or {}
    obsolete_api_key_blobs = set(cutoff.get("obsolete_secret_blob_hashes") or [])
    usable_api_key_operations = [
        operation
        for operation in api_key_operations
        if str(operation.get("secret_blob_hash") or "") not in obsolete_api_key_blobs
    ]
    api_keys: dict[str, Any] | None = None
    if usable_api_key_operations:
        operation = usable_api_key_operations[-1]
        api_keys = {
            "serial": int(operation["api_serial"]),
            "payload_hash": str(operation["payload_hash"]),
            "secret_blob_hash": str(operation["secret_blob_hash"]),
            "updated_by": str(operation["actor"]),
            "updated_at": int(operation["created_at"]),
        }
        if operation.get("sanitized") is True:
            api_keys["sanitized"] = True
    state_vector = {
        actor: _highest_contiguous_sequence(sequences)
        for actor, sequences in actor_sequences.items()
        if _highest_contiguous_sequence(sequences) > 0
    }

    cluster_nodes = {
        "schema_version": SCHEMA_VERSION,
        "cluster_id": cluster_id,
        "generation": len(operations),
        "credential_membership_generation": credential_membership_generation,
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
    desired_state.update(v2_state)
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


def credential_lifecycle_status(materialized: dict[str, Any]) -> dict[str, Any]:
    """Build secret-free protocol, recipient, materialization, and barrier status."""

    cluster_nodes = materialized.get("cluster_nodes") if isinstance(materialized, dict) else {}
    cluster_nodes = cluster_nodes if isinstance(cluster_nodes, dict) else {}
    nodes = cluster_nodes.get("nodes") if isinstance(cluster_nodes.get("nodes"), dict) else {}
    desired = materialized.get("desired_state") if isinstance(materialized, dict) else {}
    desired = desired if isinstance(desired, dict) else {}
    secrets = desired.get("secrets") if isinstance(desired.get("secrets"), dict) else {}
    cmc_pool = desired.get("cmc_pool") if isinstance(desired.get("cmc_pool"), dict) else {}
    cmc_entries = cmc_pool.get("entries") if isinstance(cmc_pool.get("entries"), dict) else {}
    has_cluster_cmc_metadata = bool(cmc_entries) or any(
        isinstance(secret, dict) and str(secret.get("secret_kind") or "") == "cmc_api_key"
        for secret in secrets.values()
    )
    acknowledgements = desired.get("credential_materialization_acks")
    acknowledgements = acknowledgements if isinstance(acknowledgements, dict) else {}
    projection_acks = desired.get("tradfi_projection_acks")
    projection_acks = projection_acks if isinstance(projection_acks, dict) else {}
    active_tradfi = desired.get("tradfi_active_profiles")
    active_tradfi = active_tradfi if isinstance(active_tradfi, dict) else {}
    active_profile_generations = {
        str(provider): int(item.get("activation_generation") or 0)
        for provider, item in active_tradfi.items()
        if isinstance(item, dict) and not item.get("conflicted")
    }
    membership_generation = int(cluster_nodes.get("credential_membership_generation") or 0)

    active: dict[str, dict[str, Any]] = {
        str(node_id): node
        for node_id, node in nodes.items()
        if isinstance(node, dict)
        and node.get("enabled", True) is not False
        and node.get("state_replica", True) is not False
        and str(node.get("role") or "") in {"master", "vps"}
    }
    blockers: list[dict[str, Any]] = []
    node_status: dict[str, dict[str, Any]] = {}
    for node_id, node in sorted(nodes.items()):
        if not isinstance(node, dict):
            continue
        try:
            protocol = int(node.get("credential_protocol_version"))
        except (TypeError, ValueError):
            protocol = 0
        missing_ids = sorted(
            field for field in ("signing_key_id", "encryption_key_id") if not node.get(field)
        )
        replica_active = node_id in active
        protocol_ready = (
            replica_active
            and protocol == 2
            and node.get("credential_capable") is True
            and not missing_ids
        )
        if replica_active and not protocol_ready:
            blockers.append({
                "node_id": str(node_id),
                "role": str(node.get("role") or ""),
                "reason": "explicit protocol v2 and crypto registration are required",
                "missing_registration_ids": missing_ids,
            })
        expected_credentials = {
            str(secret_id): int(secret.get("generation") or 0)
            for secret_id, secret in secrets.items()
            if isinstance(secret, dict) and str(node_id) in set(secret.get("recipient_ids") or [])
        }
        expected_recipients = {
            str(secret_id): int(secret.get("recipient_generation") or 1)
            for secret_id, secret in secrets.items()
            if isinstance(secret, dict) and str(node_id) in set(secret.get("recipient_ids") or [])
        }
        ack = acknowledgements.get(node_id) if isinstance(acknowledgements.get(node_id), dict) else {}
        ack_current = (
            int(ack.get("membership_generation") or -1) == membership_generation
            and ack.get("credential_generations", {}) == expected_credentials
            and ack.get("recipient_generations", {}) == expected_recipients
        )
        role = str(node.get("role") or "")
        projection_ack = (
            projection_acks.get(node_id)
            if isinstance(projection_acks.get(node_id), dict)
            else {}
        )
        projection_current = (
            role == "master"
            and int(projection_ack.get("membership_generation") or -1) == membership_generation
            and projection_ack.get("active_profile_generations", {}) == active_profile_generations
            and str(projection_ack.get("projection_status") or "") in {"current", "removed"}
            and int(projection_ack.get("projection_applied_generation") or 0) >= 0
        )
        desired_active_cmc: dict[str, int] = {}
        for credential_id, entry in cmc_entries.items():
            if not isinstance(entry, dict):
                continue
            secret_id = str(entry.get("secret_id") or entry.get("key_id") or credential_id)
            secret = secrets.get(secret_id) if isinstance(secrets.get(secret_id), dict) else {}
            if (
                str(entry.get("state") or "").lower() == "active"
                and not entry.get("conflicted")
                and not entry.get("state_conflicts")
                and str(node_id) in set(secret.get("recipient_ids") or [])
                and int(secret.get("generation") or 0) > 0
            ):
                desired_active_cmc[secret_id] = int(secret["generation"])
        for secret_id, secret in secrets.items():
            if (
                isinstance(secret, dict)
                and str(secret.get("secret_kind") or "") == "cmc_api_key"
                and str(node_id) in set(secret.get("recipient_ids") or [])
                and not any(
                    isinstance(entry, dict)
                    and str(entry.get("secret_id") or entry.get("key_id") or credential_id) == str(secret_id)
                    for credential_id, entry in cmc_entries.items()
                )
            ):
                desired_active_cmc[str(secret_id)] = int(secret.get("generation") or 0)
        cmc_materialized = bool(desired_active_cmc) and ack_current and all(
            int((ack.get("credential_generations") or {}).get(secret_id) or 0) == generation
            and int((ack.get("recipient_generations") or {}).get(secret_id) or 0)
            == int((secrets.get(secret_id) or {}).get("recipient_generation") or 1)
            for secret_id, generation in desired_active_cmc.items()
        )
        credential_active = (
            bool(protocol_ready and cmc_materialized)
            if has_cluster_cmc_metadata
            else None
        )
        node_status[str(node_id)] = {
            "protocol_version": protocol or None,
            "signing_key_id": str(node.get("signing_key_id") or ""),
            "encryption_key_id": str(node.get("encryption_key_id") or ""),
            "crypto_registered": not missing_ids,
            "credential_active": credential_active,
            "cluster_origin_metadata": has_cluster_cmc_metadata,
            "desired_active_cmc": desired_active_cmc,
            "membership_generation": membership_generation,
            "materialization_ack": {
                "current": ack_current,
                "membership_generation": int(ack.get("membership_generation") or 0),
                "credential_generations": dict(ack.get("credential_generations") or {}),
                "recipient_generations": dict(ack.get("recipient_generations") or {}),
                "acked_at": int(ack.get("acked_at") or 0),
                "op_id": str(ack.get("op_id") or ""),
            },
            "cmc_materialized": cmc_materialized,
            "tradfi_status": "not_recipient" if role != "master" else (
                "materialized" if ack_current and projection_current else "pending"
            ),
            "tradfi_projection_ack": {
                "current": projection_current,
                **dict(projection_ack),
            },
        }

    expected_audiences: dict[str, tuple[list[str], dict[str, str]]] = {}
    cluster_ids = sorted(active)
    master_ids = sorted(
        node_id for node_id, node in active.items() if str(node.get("role") or "") == "master"
    )
    expected_audiences["cluster"] = (
        cluster_ids,
        {node_id: str(active[node_id].get("encryption_key_id") or "") for node_id in cluster_ids},
    )
    expected_audiences["masters"] = (
        master_ids,
        {node_id: str(active[node_id].get("encryption_key_id") or "") for node_id in master_ids},
    )
    rewrap_items: list[dict[str, Any]] = []
    cmc: dict[str, Any] = {}
    tradfi: dict[str, Any] = {}
    for secret_id, secret in sorted(secrets.items()):
        if not isinstance(secret, dict):
            continue
        audience = str(secret.get("audience") or "")
        expected_ids, expected_key_ids = expected_audiences.get(audience, ([], {}))
        reasons: list[str] = []
        if int(secret.get("membership_generation") or 0) != membership_generation:
            reasons.append("membership_generation")
        if list(secret.get("recipient_ids") or []) != expected_ids:
            reasons.append("recipient_set")
        if dict(secret.get("recipient_key_ids") or {}) != expected_key_ids:
            reasons.append("recipient_key_registration")
        if secret.get("recipient_conflicted") is True:
            reasons.append("recipient_conflict")
        if reasons:
            rewrap_items.append({"secret_id": str(secret_id), "reasons": reasons})
        item = {
            "generation": int(secret.get("generation") or 0),
            "recipient_generation": int(secret.get("recipient_generation") or 1),
            "membership_generation": int(secret.get("membership_generation") or 0),
            "recipient_ids": list(secret.get("recipient_ids") or []),
            "conflicted": bool(secret.get("conflicted")),
        }
        if str(secret.get("secret_kind") or "") == "cmc_api_key":
            pool_entry = cmc_entries.get(secret_id) if isinstance(cmc_entries.get(secret_id), dict) else {}
            item["catalog_generation"] = int(
                pool_entry.get("catalog_generation", pool_entry.get("generation", 0)) or 0
            )
            cmc[str(secret_id)] = item
        elif str(secret.get("secret_kind") or "") == "tradfi_profile":
            tradfi[str(secret_id)] = item

    migration = desired.get("credential_migration")
    migration = migration if isinstance(migration, dict) else {}
    active_ids = set(active)
    freeze_acks = migration.get("freeze_acks") if isinstance(migration.get("freeze_acks"), dict) else {}
    inventory_acks = migration.get("inventory_acks") if isinstance(migration.get("inventory_acks"), dict) else {}
    materialization_acks = migration.get("materialization_acks") if isinstance(migration.get("materialization_acks"), dict) else {}
    cutoff = migration.get("cutoff") if isinstance(migration.get("cutoff"), dict) else {}
    cleanup_acks = migration.get("cleanup_acks") if isinstance(migration.get("cleanup_acks"), dict) else {}
    scan_acks = migration.get("scan_acks") if isinstance(migration.get("scan_acks"), dict) else {}
    candidates = migration.get("candidates") if isinstance(migration.get("candidates"), dict) else {}
    candidate_acceptances = migration.get("candidate_acceptances") if isinstance(migration.get("candidate_acceptances"), dict) else {}
    cutoff_generation = int(cutoff.get("cutoff_generation") or 0)
    freeze_generation = int(migration.get("freeze_generation") or 0)
    for node_id, status in node_status.items():
        submitted = [
            candidate
            for candidate in candidates.values()
            if isinstance(candidate, dict) and str(candidate.get("submitted_by") or "") == node_id
        ]
        accepted_count = sum(
            1 for candidate in submitted
            if str(candidate.get("candidate_id") or "") in candidate_acceptances
        )
        scan_ack = scan_acks.get(node_id) if isinstance(scan_acks.get(node_id), dict) else {}
        scan_current = bool(
            scan_ack
            and int(scan_ack.get("freeze_generation") or 0) == freeze_generation
            and int(scan_ack.get("cutoff_generation") or 0) == cutoff_generation
        )
        node_blockers: list[str] = []
        if migration.get("frozen") is True and node_id in active:
            if node_id not in freeze_acks:
                node_blockers.append("freeze_ack")
            if node_id not in inventory_acks:
                node_blockers.append("inventory_ack")
            if node_id not in materialization_acks:
                node_blockers.append("materialization_ack")
        if cutoff and node_id in active:
            if node_id not in cleanup_acks:
                node_blockers.append("cleanup_ack")
            if not scan_current:
                node_blockers.append("scan_ack")
            elif str(scan_ack.get("status") or "") != "clean":
                node_blockers.append("scan_findings")
        if submitted and accepted_count < len(submitted):
            node_blockers.append("candidate_acceptance")
        projection_ack = status.get("tradfi_projection_ack") or {}
        if str((nodes.get(node_id) or {}).get("role") or "") == "master" and active_profile_generations and projection_ack.get("current") is not True:
            node_blockers.append("tradfi_projection_ack")
        status["migration_candidate"] = {
            "status": "pending" if submitted and accepted_count < len(submitted) else "accepted" if submitted else "none",
            "submitted_count": len(submitted),
            "accepted_count": accepted_count,
            "pending_count": len(submitted) - accepted_count,
            "candidate_kinds": sorted({str(item.get("candidate_kind") or "unknown") for item in submitted}),
        }
        status["migration_scan_ack"] = {
            "current": scan_current,
            "status": str(scan_ack.get("status") or ("pending" if cutoff else "not_started")),
            "freeze_generation": int(scan_ack.get("freeze_generation") or 0),
            "cutoff_generation": int(scan_ack.get("cutoff_generation") or 0),
            "finding_count": len(scan_ack.get("findings") or []),
            "acked_at": int(scan_ack.get("acked_at") or 0),
        }
        status["migration_blockers"] = node_blockers
    migration_blockers: list[str] = []
    if migration.get("frozen") is True:
        if not active_ids.issubset(freeze_acks):
            migration_blockers.append("freeze_acks")
        if not active_ids.issubset(inventory_acks):
            migration_blockers.append("inventory_acks")
        if not active_ids.issubset(materialization_acks):
            migration_blockers.append("materialization_acks")
    if cutoff and not active_ids.issubset(cleanup_acks):
        migration_blockers.append("cleanup_acks")
    if cutoff and not active_ids.issubset(scan_acks):
        migration_blockers.append("scan_acks")
    if any(
        not isinstance(scan_acks.get(node_id), dict)
        or scan_acks[node_id].get("status") != "clean"
        for node_id in active_ids
    ) and scan_acks:
        migration_blockers.append("scan_findings")
    if blockers:
        migration_blockers.append("protocol_v2")
    if rewrap_items:
        migration_blockers.append("recipient_rewrap")
    return {
        "protocol_version": 2,
        "membership_generation": membership_generation,
        "protocol_barrier": {"ready": not blockers, "blockers": blockers},
        "rewrap_needed": bool(rewrap_items),
        "rewrap_items": rewrap_items,
        "nodes": node_status,
        "cmc": {"catalog": cmc, "count": len(cmc)},
        "tradfi": {"catalog": tradfi, "count": len(tradfi)},
        "migration": {
            "frozen": migration.get("frozen") is True,
            "freeze_generation": int(migration.get("freeze_generation") or 0),
            "freeze_acks": sorted(freeze_acks),
            "inventory_acks": sorted(inventory_acks),
            "materialization_acks": sorted(materialization_acks),
            "cutoff": dict(cutoff),
            "cleanup_acks": sorted(cleanup_acks),
            "scan_acks": {
                node_id: dict(scan_acks[node_id]) for node_id in sorted(scan_acks)
            },
            "blocked": bool(migration_blockers),
            "blockers": migration_blockers,
        },
    }


def local_cmc_credential_readiness(
    materialized: dict[str, Any],
    node_id: str,
    local_records: Iterable[dict[str, Any]],
) -> dict[str, Any]:
    """Return strict local CMC readiness, preserving standalone store behavior."""

    lifecycle = credential_lifecycle_status(materialized)
    node = (lifecycle.get("nodes") or {}).get(str(node_id))
    records = {
        str(record.get("id") or ""): record
        for record in local_records
        if isinstance(record, dict) and record.get("id") and not record.get("deleted_at")
    }
    active_records = {
        credential_id: record
        for credential_id, record in records.items()
        if record.get("active") is True and not record.get("pending")
    }
    if not isinstance(node, dict):
        return {
            "credential_protocol_version": None,
            "credential_active": False,
            "credential_reason": "Local node is absent from credential membership",
            "cmc_catalog_generation": 0,
            "cmc_materialized_generation": max(
                (int(record.get("generation") or 0) for record in active_records.values()),
                default=0,
            ),
            "cmc_active_key_count": len(active_records),
            "cluster_origin_metadata": True,
        }
    if node.get("cluster_origin_metadata") is not True:
        active = bool(active_records)
        generation = max(
            (int(record.get("generation") or 0) for record in active_records.values()),
            default=0,
        )
        return {
            "credential_protocol_version": node.get("protocol_version"),
            "credential_active": active,
            "credential_reason": (
                "Standalone local CMC credential pool active"
                if active
                else "No active standalone CMC credentials"
            ),
            "cmc_catalog_generation": generation,
            "cmc_materialized_generation": generation,
            "cmc_active_key_count": len(active_records),
            "cluster_origin_metadata": False,
        }

    expected = node.get("desired_active_cmc") if isinstance(node.get("desired_active_cmc"), dict) else {}
    desired = materialized.get("desired_state") if isinstance(materialized, dict) else {}
    desired = desired if isinstance(desired, dict) else {}
    secrets = desired.get("secrets") if isinstance(desired.get("secrets"), dict) else {}
    entries = ((desired.get("cmc_pool") or {}).get("entries") or {})
    catalog_generations = [
        int((secrets.get(str(entry.get("secret_id") or entry.get("key_id") or credential_id)) or {}).get("generation") or 0)
        for credential_id, entry in entries.items()
        if isinstance(entry, dict)
        and str(entry.get("state") or "").lower() == "active"
        and not entry.get("conflicted")
        and not entry.get("state_conflicts")
    ]
    exact = bool(expected) and all(
        credential_id in records
        and not records[credential_id].get("pending")
        and int(records[credential_id].get("generation") or 0) == int(generation)
        for credential_id, generation in expected.items()
    )
    active = bool(node.get("credential_active") is True and exact)
    return {
        "credential_protocol_version": node.get("protocol_version"),
        "credential_active": active,
        "credential_reason": (
            "CMC credential pool active"
            if active
            else "Desired CMC generation is not fully materialized"
        ),
        "cmc_catalog_generation": max(catalog_generations or [int(value) for value in expected.values()], default=0),
        "cmc_materialized_generation": max(
            (
                int(records[credential_id].get("generation") or 0)
                for credential_id in expected
                if credential_id in records
            ),
            default=0,
        ),
        "cmc_active_key_count": sum(
            credential_id in records and not records[credential_id].get("pending")
            for credential_id in expected
        ),
        "cluster_origin_metadata": True,
        "membership_generation": node.get("membership_generation"),
        "materialization_ack": dict(node.get("materialization_ack") or {}),
    }


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


def _apply_membership(nodes: dict[str, dict[str, Any]], removed_node_ids: set[str], operation: dict[str, Any]) -> None:
    """Apply a membership operation to materialized nodes."""

    op = str(operation["op"])
    node_id = str(operation["node_id"])
    if node_id in removed_node_ids:
        return
    if op == "DISABLE_NODE":
        current = dict(nodes.get(node_id, {"node_id": node_id}))
        current["enabled"] = False
        current["sync_mode"] = "disabled"
        current["sync_enabled"] = False
        current["updated_at"] = int(operation["created_at"])
        nodes[node_id] = current
        return
    if op == "REMOVE_NODE":
        removed_node_ids.add(node_id)
        nodes.pop(node_id, None)
        return
    current = dict(nodes.get(node_id, {"node_id": node_id}))
    if "sync_enabled" in operation and "sync_mode" not in operation:
        current.pop("sync_mode", None)
    for key, value in operation.items():
        if key in {
            "schema_version", "cluster_id", "op_id", "actor", "seq", "op", "created_at",
            "signer_id", "signer_key_id", "signature", "signature_version",
            "signature_algorithm", "membership_authorization",
        }:
            continue
        current[key] = value
    current.setdefault("enabled", True)
    normalize_node_sync_fields(current)
    current["updated_at"] = int(operation["created_at"])
    nodes[node_id] = current


def _credential_membership_fingerprint(nodes: dict[str, dict[str, Any]]) -> tuple[Any, ...]:
    """Return the recipient-affecting portion of active replica membership."""

    return tuple(
        (
            str(node_id),
            str(node.get("role") or ""),
            node.get("enabled", True) is not False,
            node.get("state_replica", True) is not False,
            node.get("credential_protocol_version"),
            node.get("credential_capable"),
            str(node.get("signing_key_id") or ""),
            str(node.get("encryption_key_id") or ""),
        )
        for node_id, node in sorted(nodes.items())
        if isinstance(node, dict)
    )


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
        current_version = str((instances.get(instance) or {}).get("version") or "")
        conflict_versions = {item["version"] for item in changes}
        if len(changes) > 1 and len(signatures) > 1 and current_version in conflict_versions:
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
    present_crypto_fields = {
        field
        for field in CRYPTO_PUBLIC_FIELDS
        if field in operation and field != "signing_key_id"
    }
    if present_crypto_fields:
        missing = sorted(CRYPTO_PUBLIC_FIELDS - set(operation))
        if missing:
            raise ClusterStateError(
                f"membership crypto bundle missing field(s): {', '.join(missing)}"
            )
        _validate_public_key_bundle(operation)


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
    if "sanitized" in operation and operation["sanitized"] is not True:
        raise ClusterStateError("post-cutoff API-key operation must be explicitly sanitized")
    if "credential_protocol_version" in operation:
        if _as_positive_int(operation["credential_protocol_version"], "credential_protocol_version") < 2:
            raise ClusterStateError("post-cutoff API-key operation requires credential protocol v2")


def _validate_api_key_operation_after_cutoff(
    cluster_root: Path,
    operation: dict[str, Any],
) -> None:
    """Reject obsolete or downgrade API-key operations after signed cutoff."""

    desired_path = ClusterPaths.from_root(cluster_root).desired_state
    if not desired_path.is_file():
        return
    try:
        desired = _read_json(desired_path)
    except (OSError, json.JSONDecodeError):
        raise ClusterStateError("credential cutoff state is unreadable")
    migration = desired.get("credential_migration") if isinstance(desired, dict) else None
    cutoff = migration.get("cutoff") if isinstance(migration, dict) else None
    if not isinstance(cutoff, dict) or cutoff.get("conflicted") is True:
        return
    blob_hash = str(operation.get("secret_blob_hash") or "")
    if blob_hash in set(cutoff.get("obsolete_secret_blob_hashes") or []):
        raise ClusterStateError("pre-cutoff plaintext API-key operation is obsolete")
    vector = cutoff.get("state_vector") if isinstance(cutoff.get("state_vector"), dict) else {}
    before_cutoff = int(operation.get("seq") or 0) <= int(vector.get(str(operation.get("actor") or "")) or 0)
    if not before_cutoff and (
        operation.get("sanitized") is not True
        or int(operation.get("credential_protocol_version") or 0) < 2
    ):
        raise ClusterStateError("credential protocol downgrade rejected after cutoff")


def _validate_v2_credential_payload(operation: dict[str, Any]) -> None:
    """Validate additive Cluster Sync v2 credential operation fields."""

    op = str(operation["op"])
    if op in {"UPSERT_SECRET", "TOMBSTONE_SECRET", "UPDATE_SECRET_RECIPIENTS"}:
        _validate_credential_id(operation.get("secret_id"), "secret_id")
        if op == "UPDATE_SECRET_RECIPIENTS":
            _as_positive_int(operation.get("provider_generation"), "provider_generation")
            recipient_generation = _as_positive_int(
                operation.get("recipient_generation"), "recipient_generation"
            )
            parent_recipient_generation = _as_nonnegative_int(
                operation.get("parent_recipient_generation"),
                "parent_recipient_generation",
            )
            if recipient_generation != parent_recipient_generation + 1:
                raise ClusterStateError("recipient_generation must advance its parent by one")
            _as_nonnegative_int(operation.get("membership_generation"), "membership_generation")
            _validate_hash(str(operation.get("sealed_blob_hash") or ""), "sealed_blob_hash")
            _validate_recipient_registration(operation)
            return
        _as_positive_int(operation.get("generation"), "generation")
        _as_nonnegative_int(operation.get("parent_generation"), "parent_generation")
        if "secret_kind" in operation or "kind" in operation:
            _validate_bounded_text(
                operation.get("secret_kind", operation.get("kind")),
                "secret_kind",
            )
        if op == "UPSERT_SECRET":
            kind = operation.get("secret_kind", operation.get("kind"))
            _validate_bounded_text(kind, "secret_kind")
            if operation.get("audience") not in {"cluster", "masters"}:
                raise ClusterStateError("secret audience must be cluster or masters")
            if kind == "cmc_api_key" and operation.get("audience") != "cluster":
                raise ClusterStateError("CMC secrets require cluster audience")
            if kind == "tradfi_profile" and operation.get("audience") != "masters":
                raise ClusterStateError("TradFi secrets require masters audience")
            _validate_hash(str(operation.get("sealed_blob_hash") or ""), "sealed_blob_hash")
            if "recipient_generation" in operation:
                if _as_positive_int(operation["recipient_generation"], "recipient_generation") != 1:
                    raise ClusterStateError("initial recipient_generation must be one")
                if _as_nonnegative_int(
                    operation.get("parent_recipient_generation"),
                    "parent_recipient_generation",
                ) != 0:
                    raise ClusterStateError("initial parent_recipient_generation must be zero")
                _as_nonnegative_int(operation.get("membership_generation"), "membership_generation")
                _validate_recipient_registration(operation)
        return
    if op == "UPSERT_CMC_POOL_ENTRY":
        _validate_credential_id(operation.get("key_id"), "key_id")
        _validate_credential_id(operation.get("secret_id"), "secret_id")
        _as_positive_int(
            operation.get("catalog_generation", operation.get("generation")),
            "catalog_generation",
        )
        _as_nonnegative_int(operation.get("parent_generation"), "parent_generation")
        return
    if op == "SET_CMC_KEY_STATE":
        _validate_credential_id(operation.get("key_id"), "key_id")
        if str(operation.get("state") or "") not in CMC_KEY_STATES:
            raise ClusterStateError("invalid CMC key state")
        _as_positive_int(
            operation.get("state_generation", operation.get("generation")),
            "state_generation",
        )
        _as_nonnegative_int(operation.get("parent_generation"), "parent_generation")
        return
    if op == "SET_CMC_AUTHORITY":
        _validate_bounded_text(operation.get("quota_domain_id"), "quota_domain_id")
        _validate_node_id(str(operation.get("authority_node_id") or ""))
        _as_positive_int(operation.get("authority_epoch"), "authority_epoch")
        _as_nonnegative_int(operation.get("parent_epoch"), "parent_epoch")
        return
    if op == "SET_TRADFI_ACTIVE_PROFILE":
        _validate_bounded_text(operation.get("provider"), "provider")
        profile_id = operation.get("profile_id")
        if profile_id is not None:
            _validate_credential_id(profile_id, "profile_id")
            if not str(profile_id).startswith("tradfi_"):
                raise ClusterStateError("active TradFi profile ID must be a TradFi credential")
        _as_positive_int(operation.get("activation_generation"), "activation_generation")
        _as_nonnegative_int(operation.get("parent_generation"), "parent_generation")
        return
    if op == "WRITER_FREEZE":
        _as_positive_int(operation.get("freeze_generation"), "freeze_generation")
        if "frozen" in operation and not isinstance(operation["frozen"], bool):
            raise ClusterStateError("frozen must be a boolean")
        _validate_migration_source_metadata(operation)
        return
    if op == "WRITER_FREEZE_ACK":
        _as_positive_int(operation.get("freeze_generation"), "freeze_generation")
        if operation.get("frozen") is not True:
            raise ClusterStateError("WRITER_FREEZE_ACK must explicitly acknowledge frozen state")
        if "node_id" in operation:
            _validate_node_id(str(operation["node_id"]))
            if str(operation["node_id"]) != str(operation["actor"]):
                raise ClusterStateError("WRITER_FREEZE_ACK node_id must match actor")
        if "source_fingerprints" in operation or "source_generations" in operation:
            raise ClusterStateError("WRITER_FREEZE_ACK must not contain inventory metadata")
        return
    if op == "CREDENTIAL_INVENTORY_ACK":
        _as_positive_int(operation.get("freeze_generation"), "freeze_generation")
        if str(operation.get("node_id") or operation["actor"]) != str(operation["actor"]):
            raise ClusterStateError("CREDENTIAL_INVENTORY_ACK node_id must match actor")
        _validate_source_fingerprints(operation.get("source_fingerprints"))
        generations = operation.get("source_generations")
        if not isinstance(generations, dict):
            raise ClusterStateError("source_generations must be an object")
        for source, generation in generations.items():
            _validate_bounded_text(source, "source generation name")
            _as_nonnegative_int(generation, "source generation")
        return
    if op == "CREDENTIAL_MATERIALIZATION_ACK":
        if "node_id" in operation:
            _validate_node_id(str(operation["node_id"]))
            if str(operation["node_id"]) != str(operation["actor"]):
                raise ClusterStateError("CREDENTIAL_MATERIALIZATION_ACK node_id must match actor")
        generations = operation.get("credential_generations")
        if not isinstance(generations, dict):
            raise ClusterStateError("credential_generations must be an object")
        for credential_id, generation in generations.items():
            _validate_credential_id(credential_id, "credential generation ID")
            _as_positive_int(generation, "credential generation")
        if "recipient_generations" in operation:
            recipient_generations = operation["recipient_generations"]
            if not isinstance(recipient_generations, dict):
                raise ClusterStateError("recipient_generations must be an object")
            for credential_id, generation in recipient_generations.items():
                _validate_credential_id(credential_id, "recipient generation ID")
                _as_positive_int(generation, "recipient generation")
            _as_nonnegative_int(operation.get("membership_generation"), "membership_generation")
        elif "freeze_generation" not in operation:
            raise ClusterStateError(
                "materialization ACK requires recipient_generations or freeze_generation"
            )
        if "freeze_generation" in operation:
            _as_positive_int(operation.get("freeze_generation"), "freeze_generation")
        if "source_fingerprints" in operation:
            _validate_source_fingerprints(operation["source_fingerprints"])
        return
    if op == "CREDENTIAL_CUTOFF":
        _as_positive_int(operation.get("cutoff_generation"), "cutoff_generation")
        _as_nonnegative_int(operation.get("parent_generation"), "parent_generation")
        if _as_positive_int(operation.get("min_protocol"), "min_protocol") != 2:
            raise ClusterStateError("credential cutoff min_protocol must be 2")
        _validate_state_vector(operation.get("state_vector"))
        hashes = operation.get("obsolete_secret_blob_hashes")
        if not isinstance(hashes, list) or hashes != sorted(set(str(item) for item in hashes)):
            raise ClusterStateError("obsolete_secret_blob_hashes must be a sorted unique list")
        for blob_hash in hashes:
            _validate_hash(str(blob_hash), "obsolete secret blob hash")
        return
    if op == "CREDENTIAL_CUTOFF_ACK":
        _as_positive_int(operation.get("cutoff_generation"), "cutoff_generation")
        if str(operation.get("node_id") or operation["actor"]) != str(operation["actor"]):
            raise ClusterStateError("CREDENTIAL_CUTOFF_ACK node_id must match actor")
        _validate_state_vector(operation.get("state_vector"))
        removed = operation.get("removed_secret_blob_hashes", [])
        if not isinstance(removed, list) or removed != sorted(set(str(item) for item in removed)):
            raise ClusterStateError("removed_secret_blob_hashes must be a sorted unique list")
        for blob_hash in removed:
            _validate_hash(str(blob_hash), "removed secret blob hash")
        return
    if op == "MIGRATION_SECRET_CANDIDATE":
        _validate_bounded_text(operation.get("candidate_id"), "candidate_id")
        if operation.get("candidate_kind") not in {"cmc_api_key", "tradfi_profile"}:
            raise ClusterStateError("invalid migration candidate kind")
        _as_positive_int(operation.get("freeze_generation"), "freeze_generation")
        _validate_hash(str(operation.get("sealed_blob_hash") or ""), "sealed_blob_hash")
        fingerprint = str(operation.get("source_fingerprint") or "")
        if len(fingerprint) != 64:
            raise ClusterStateError("source_fingerprint must be SHA-256 hex")
        try:
            int(fingerprint, 16)
        except ValueError as exc:
            raise ClusterStateError("source_fingerprint must be SHA-256 hex") from exc
        _as_nonnegative_int(operation.get("source_generation"), "source_generation")
        _as_nonnegative_int(operation.get("membership_generation"), "membership_generation")
        if operation.get("audience") != "masters":
            raise ClusterStateError("migration candidates require masters audience")
        _validate_recipient_registration(operation)
        return
    if op == "MIGRATION_SECRET_ACCEPTANCE":
        _validate_bounded_text(operation.get("candidate_id"), "candidate_id")
        _validate_credential_id(operation.get("credential_id"), "credential_id")
        _as_positive_int(operation.get("credential_generation"), "credential_generation")
        _as_positive_int(operation.get("freeze_generation"), "freeze_generation")
        if str(operation.get("status") or "") != "accepted":
            raise ClusterStateError("migration acceptance status must be accepted")
        return
    if op == "MIGRATION_SECRET_CONFLICT":
        _validate_bounded_text(operation.get("conflict_id"), "conflict_id")
        _validate_bounded_text(operation.get("candidate_id"), "candidate_id")
        _validate_credential_id(operation.get("existing_credential_id"), "existing_credential_id")
        _validate_bounded_text(operation.get("provider"), "provider")
        _as_positive_int(operation.get("freeze_generation"), "freeze_generation")
        if str(operation.get("status") or "") != "unresolved":
            raise ClusterStateError("migration conflict status must be unresolved")
        return
    if op == "MIGRATION_SECRET_CONFLICT_RESOLUTION":
        _validate_bounded_text(operation.get("conflict_id"), "conflict_id")
        if str(operation.get("choice") or "") not in {"candidate", "existing"}:
            raise ClusterStateError("migration conflict choice is invalid")
        _validate_bounded_text(operation.get("resolution_id"), "resolution_id")
        return
    if op == "TRADFI_PROJECTION_ACK":
        if str(operation.get("node_id") or operation["actor"]) != str(operation["actor"]):
            raise ClusterStateError("TRADFI_PROJECTION_ACK node_id must match actor")
        _as_nonnegative_int(operation.get("membership_generation"), "membership_generation")
        generations = operation.get("active_profile_generations")
        if not isinstance(generations, dict):
            raise ClusterStateError("active_profile_generations must be an object")
        for provider, generation in generations.items():
            _validate_bounded_text(provider, "TradFi provider")
            _as_positive_int(generation, "active profile generation")
        _as_positive_int(
            operation.get("projection_applied_generation"),
            "projection_applied_generation",
        )
        if str(operation.get("projection_status") or "") not in {"current", "removed"}:
            raise ClusterStateError("invalid TradFi projection status")
        return
    if op == "CREDENTIAL_SCAN_ACK":
        if str(operation.get("node_id") or operation["actor"]) != str(operation["actor"]):
            raise ClusterStateError("CREDENTIAL_SCAN_ACK node_id must match actor")
        _as_positive_int(operation.get("freeze_generation"), "freeze_generation")
        _as_nonnegative_int(operation.get("cutoff_generation"), "cutoff_generation")
        if str(operation.get("status") or "") not in {"clean", "blocked"}:
            raise ClusterStateError("invalid credential scan status")
        if operation.get("clean") is not (str(operation.get("status") or "") == "clean"):
            raise ClusterStateError("credential scan clean flag does not match status")
        findings = operation.get("findings")
        if not isinstance(findings, list) or len(findings) > 256:
            raise ClusterStateError("credential scan findings must be a bounded list")
        for finding in findings:
            if not isinstance(finding, dict) or set(finding) != {"path_category"}:
                raise ClusterStateError("credential scan findings must contain only path_category")
            _validate_bounded_text(finding["path_category"], "scan path category")
        return


def _validate_state_vector(value: Any) -> None:
    """Validate an exact non-negative actor sequence vector."""

    if not isinstance(value, dict):
        raise ClusterStateError("state_vector must be an object")
    for actor, sequence in value.items():
        _validate_node_id(str(actor))
        _as_nonnegative_int(sequence, "state vector sequence")


def _validate_recipient_registration(operation: dict[str, Any]) -> None:
    """Validate exact recipient IDs and public registration identifiers."""

    recipient_ids = operation.get("recipient_ids")
    recipient_key_ids = operation.get("recipient_key_ids")
    if not isinstance(recipient_ids, list) or not recipient_ids:
        raise ClusterStateError("recipient_ids must be a non-empty list")
    normalized_ids = [str(node_id) for node_id in recipient_ids]
    for node_id in normalized_ids:
        _validate_node_id(node_id)
    if normalized_ids != sorted(set(normalized_ids)):
        raise ClusterStateError("recipient_ids must be unique and sorted")
    if not isinstance(recipient_key_ids, dict) or set(recipient_key_ids) != set(normalized_ids):
        raise ClusterStateError("recipient_key_ids must exactly match recipient_ids")
    for node_id, key_id in recipient_key_ids.items():
        _validate_node_id(str(node_id))
        text = _validate_bounded_text(key_id, "recipient key ID")
        if not text.startswith("x25519:") or len(text) != len("x25519:") + 64:
            raise ClusterStateError("invalid recipient key ID")
        try:
            int(text.removeprefix("x25519:"), 16)
        except ValueError as exc:
            raise ClusterStateError("invalid recipient key ID") from exc


def _validate_migration_source_metadata(operation: dict[str, Any]) -> None:
    """Validate source metadata bound to a writer-freeze generation."""

    if "migration_operation_id" in operation:
        _validate_bounded_text(operation["migration_operation_id"], "migration_operation_id")
    if "source_fingerprints" in operation:
        _validate_source_fingerprints(operation["source_fingerprints"])
    if "source_generations" in operation:
        generations = operation["source_generations"]
        if not isinstance(generations, dict):
            raise ClusterStateError("source_generations must be an object")
        for source, generation in generations.items():
            _validate_bounded_text(source, "source generation name")
            _as_nonnegative_int(generation, "source generation")


def _validate_source_fingerprints(value: Any) -> None:
    """Validate exact SHA-256 source fingerprints without exposing source data."""

    if not isinstance(value, dict):
        raise ClusterStateError("source_fingerprints must be an object")
    for source, fingerprint in value.items():
        _validate_bounded_text(source, "source fingerprint name")
        text = str(fingerprint)
        if len(text) != 64:
            raise ClusterStateError("source fingerprint must be SHA-256 hex")
        try:
            int(text, 16)
        except ValueError as exc:
            raise ClusterStateError("source fingerprint must be SHA-256 hex") from exc


def _validate_process_readiness(value: Any) -> None:
    """Validate bounded, secret-free local service readiness metadata."""
    if not isinstance(value, list) or len(value) > 32:
        raise ClusterStateError("process_readiness must be a bounded list")
    services: list[str] = []
    for item in value:
        if not isinstance(item, dict) or set(item) != {
            "service",
            "credential_protocol_version",
            "code_serial",
            "capability_generation",
        }:
            raise ClusterStateError("process_readiness contains invalid fields")
        service = _validate_bounded_text(item.get("service"), "process readiness service", maximum=96)
        if _as_positive_int(item.get("credential_protocol_version"), "credential_protocol_version") != 2:
            raise ClusterStateError("process readiness requires credential protocol v2")
        _as_positive_int(item.get("capability_generation"), "capability_generation")
        serial = str(item.get("code_serial") or "")
        if len(serial) > 32 or any(char not in "0123456789" for char in serial):
            raise ClusterStateError("process readiness code serial is invalid")
        services.append(service)
    if services != sorted(services) or len(services) != len(set(services)):
        raise ClusterStateError("process_readiness services must be sorted and unique")


def _validate_v2_operation_signature(
    operation: dict[str, Any],
    membership_trust: MembershipTrust,
    *,
    network_input: bool,
) -> None:
    """Verify a v2 operation against the key valid at its actor sequence."""

    actor = str(operation["actor"])
    if str(operation.get("signer_id") or "") != actor:
        raise ClusterStateError("v2 operation signer_id must match actor")
    node = membership_trust.nodes.get(actor)
    if network_input:
        if isinstance(node, dict) and node.get("enabled", True) is False:
            raise ClusterStateError("v2 operation actor is disabled")
        if not isinstance(node, dict) or node.get("state_replica", True) is False:
            raise ClusterStateError("v2 operation actor is not an active state replica")
    public_key = _membership_key_for_operation(membership_trust, operation)
    try:
        verify_operation(operation, public_key)
    except ClusterCredentialError as exc:
        raise ClusterStateError(str(exc)) from exc


def _validate_v2_actor_role(
    operation: dict[str, Any],
    membership_trust: MembershipTrust,
    *,
    network_input: bool,
) -> None:
    """Authorize credential operations from authenticated membership history."""

    actor = str(operation["actor"])
    role = _membership_role_for_operation(
        membership_trust,
        operation,
        current=network_input,
    )
    op = str(operation["op"])
    if role not in {"master", "vps"}:
        raise ClusterStateError("credential operation actor has no authenticated role")
    if op in MASTER_ONLY_CREDENTIAL_OPS and role != "master":
        raise ClusterStateError(f"{op} requires an authenticated master actor")
    if op == "MIGRATION_SECRET_CANDIDATE" and role != "vps":
        raise ClusterStateError("migration candidates may only be appended by a VPS actor")
    if op in VPS_SELF_CREDENTIAL_OPS:
        node_id = str(operation.get("node_id") or actor)
        if node_id != actor:
            raise ClusterStateError(f"{op} node_id must match actor")


def _membership_role_for_operation(
    trust: MembershipTrust,
    operation: dict[str, Any],
    *,
    current: bool = False,
) -> str:
    """Resolve an actor role from current membership or signed role ancestry."""

    actor = str(operation.get("actor") or "")
    role_epoch = operation.get("actor_role_epoch")
    if not isinstance(role_epoch, int) or isinstance(role_epoch, bool) or role_epoch < 1:
        raise ClusterStateError("actor_role_epoch must be a positive integer")
    membership_op_id = operation.get("actor_membership_op_id")
    if not isinstance(membership_op_id, str) or not membership_op_id:
        raise ClusterStateError("actor_membership_op_id must be non-empty text")
    matches = [
        event
        for event in trust.role_history.get(actor, [])
        if int(event.get("role_epoch") or 0) == role_epoch
        and str(event.get("membership_op_id") or "") == membership_op_id
    ]
    if len(matches) != 1:
        raise ClusterStateError("credential operation membership epoch is not authenticated")
    event = matches[0]
    if current:
        history = trust.role_history.get(actor, [])
        node = trust.nodes.get(actor)
        if (
            not history
            or event is not history[-1]
            or str((node or {}).get("role") or "") != str(event.get("role") or "")
        ):
            raise ClusterStateError("credential operation membership epoch is not current")
    return str(event.get("role") or "")


def _validate_candidate_against_current_freeze(
    cluster_root: Path,
    operation: dict[str, Any],
) -> None:
    """Accept a migration candidate only for the currently materialized freeze."""

    desired_path = ClusterPaths.from_root(cluster_root).desired_state
    if not desired_path.is_file():
        raise ClusterStateError("migration candidate requires current writer-freeze state")
    try:
        desired = _read_json(desired_path)
    except (OSError, json.JSONDecodeError) as exc:
        raise ClusterStateError("credential migration state is unreadable") from exc
    migration = desired.get("credential_migration") if isinstance(desired, dict) else None
    if (
        not isinstance(migration, dict)
        or migration.get("frozen") is not True
        or int(migration.get("freeze_generation") or 0)
        != int(operation.get("freeze_generation") or -1)
    ):
        raise ClusterStateError("migration candidate is outside the current writer freeze")


def _validate_public_key_bundle(operation: dict[str, Any]) -> None:
    """Validate membership Ed25519/X25519 public keys and identifiers."""

    for algorithm, public_field, key_id_field in (
        ("ed25519", "signing_public_key", "signing_key_id"),
        ("x25519", "encryption_public_key", "encryption_key_id"),
    ):
        try:
            raw = base64.b64decode(str(operation[public_field]), validate=True)
        except (ValueError, binascii.Error) as exc:
            raise ClusterStateError(f"invalid {public_field}") from exc
        if len(raw) != 32 or base64.b64encode(raw).decode("ascii") != operation[public_field]:
            raise ClusterStateError(f"invalid {public_field}")
        expected_key_id = f"{algorithm}:{hashlib.sha256(raw).hexdigest()}"
        if operation[key_id_field] != expected_key_id:
            raise ClusterStateError(f"invalid {key_id_field}")


def _trust_from_nodes(nodes: dict[str, dict[str, Any]]) -> MembershipTrust:
    """Build compatibility trust from an already authenticated node mapping."""

    trust = MembershipTrust.empty()
    trust.nodes = {
        str(node_id): dict(node)
        for node_id, node in nodes.items()
        if isinstance(node, dict)
    }
    for node_id, node in trust.nodes.items():
        public_key = str(node.get("signing_public_key") or "")
        key_id = str(node.get("signing_key_id") or "")
        if public_key and key_id:
            trust.signing_keys[node_id] = [{
                "start_seq": 1,
                "end_seq": None,
                "key_id": key_id,
                "public_key": public_key,
            }]
        role = str(node.get("role") or "")
        if role:
            trust.role_history[node_id] = [{
                "order_key": (0, "", 0, ""),
                "role": role,
            }]
    return trust


def _membership_signature_key_id(operation: dict[str, Any]) -> str:
    """Return the signer key identifier without confusing a rotated node key."""

    return str(operation.get("signer_key_id", operation.get("signing_key_id")) or "")


def _membership_key_for_operation(
    trust: MembershipTrust,
    operation: dict[str, Any],
) -> str:
    """Resolve only the authenticated key valid for one actor sequence."""

    actor = str(operation.get("actor") or "")
    seq = int(operation.get("seq") or 0)
    key_id = _membership_signature_key_id(operation)
    candidates = [
        item
        for item in trust.signing_keys.get(actor, [])
        if int(item["start_seq"]) <= seq
        and (item.get("end_seq") is None or seq <= int(item["end_seq"]))
        and str(item.get("key_id") or "") == key_id
    ]
    if len(candidates) != 1:
        raise ClusterStateError("operation signer has no authenticated key for this sequence")
    return str(candidates[0]["public_key"])


def membership_signing_public_key(
    cluster_root: Path,
    signer_id: str,
    signing_key_id: str,
) -> str:
    """Resolve a current or historical authenticated membership signing key."""

    identity = read_local_identity(cluster_root)
    trust = _load_membership_trust(
        cluster_root,
        expected_cluster_id=str(identity["cluster_id"]),
    )
    matches = [
        item
        for item in trust.signing_keys.get(str(signer_id), [])
        if str(item.get("key_id") or "") == str(signing_key_id)
    ]
    if len(matches) != 1:
        raise ClusterStateError("sealed credential signer key is not authenticated")
    return str(matches[0]["public_key"])


def rotate_local_node_keys(
    cluster_root: Path,
    *,
    crash_hook: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Rotate local signing/encryption keys with durable crash recovery."""

    root = Path(cluster_root)
    identity = read_local_identity(root)
    node_id = str(identity["node_id"])
    old_keys, new_keys = prepare_node_key_rotation(root)
    old_bundle = old_keys.public_bundle(node_id, str(identity.get("role") or "master"))
    new_bundle = new_keys.public_bundle(node_id, str(identity.get("role") or "master"))
    if crash_hook:
        crash_hook("prepared")

    materialized = rebuild_materialized_state(root, write=False)
    nodes = ((materialized.get("cluster_nodes") or {}).get("nodes") or {})
    current = nodes.get(node_id) if isinstance(nodes.get(node_id), dict) else {}
    current_key_id = str(current.get("signing_key_id") or "")
    if current_key_id != new_bundle["signing_key_id"]:
        if current_key_id and current_key_id != old_bundle["signing_key_id"]:
            raise ClusterStateError("pending key rotation does not match membership")
        append_operation(
            root,
            "UPDATE_NODE_KEY",
            {
                "node_id": node_id,
                **{field: new_bundle[field] for field in CRYPTO_PUBLIC_FIELDS},
                "credential_protocol_version": 2,
                "credential_capable": True,
            },
        )
    if crash_hook:
        crash_hook("membership_published")

    active = activate_prepared_node_key_rotation(root)
    if crash_hook:
        crash_hook("keys_activated")
    active_bundle = active.public_bundle(node_id, str(identity.get("role") or "master"))
    return {
        "node_id": node_id,
        "old_signing_key_id": old_bundle["signing_key_id"],
        "old_encryption_key_id": old_bundle["encryption_key_id"],
        "signing_key_id": active_bundle["signing_key_id"],
        "encryption_key_id": active_bundle["encryption_key_id"],
        "membership_operation_applied": current_key_id != new_bundle["signing_key_id"],
    }


def _record_membership_key(
    trust: MembershipTrust,
    node_id: str,
    operation: dict[str, Any],
    *,
    start_seq: int,
) -> None:
    """Record a verified key and close the preceding key's validity interval."""

    public_key = str(operation.get("signing_public_key") or "")
    key_id = str(operation.get("signing_key_id") or "")
    if not public_key or not key_id:
        return
    history = trust.signing_keys.setdefault(node_id, [])
    if history and history[-1]["key_id"] == key_id and history[-1]["public_key"] == public_key:
        return
    if history and history[-1].get("end_seq") is None:
        history[-1]["end_seq"] = max(int(history[-1]["start_seq"]), start_seq - 1)
    history.append({
        "start_seq": start_seq,
        "end_seq": None,
        "key_id": key_id,
        "public_key": public_key,
    })


def _operation_order_key(operation: dict[str, Any]) -> tuple[int, str, int, str]:
    """Return the deterministic global history order used by membership replay."""

    return (
        int(operation.get("created_at") or 0),
        str(operation.get("actor") or ""),
        int(operation.get("seq") or 0),
        str(operation.get("op_id") or ""),
    )


def _record_membership_role(
    trust: MembershipTrust,
    node_id: str,
    operation: dict[str, Any],
) -> None:
    """Record the authenticated target role at this membership history point."""

    node = trust.nodes.get(node_id)
    role = str((node or {}).get("role") or "")
    if not role:
        return
    history = trust.role_history.setdefault(node_id, [])
    if history and str(history[-1].get("role") or "") == role:
        return
    history.append({
        "role_epoch": int(history[-1].get("role_epoch") or 0) + 1 if history else 1,
        "role": role,
        "membership_op_id": str(operation.get("op_id") or ""),
    })


def _validate_join_authorization(
    trust: MembershipTrust,
    operation: dict[str, Any],
) -> None:
    """Verify a separate existing-master authorization for one self-add."""

    authorization = operation.get("membership_authorization")
    if not isinstance(authorization, dict) or authorization.get("kind") != "join":
        raise ClusterStateError("ADD_NODE requires approved join authorization")
    actor = str(operation["actor"])
    if (
        str(authorization.get("cluster_id") or "") != str(operation["cluster_id"])
        or str(authorization.get("node_id") or "") != actor
        or str(authorization.get("role") or "") != str(operation.get("role") or "")
    ):
        raise ClusterStateError("join authorization does not match ADD_NODE")
    authorizer = str(authorization.get("signer_id") or "")
    node = trust.nodes.get(authorizer)
    if (
        not isinstance(node, dict)
        or node.get("enabled", True) is False
        or node.get("state_replica", True) is False
        or str(node.get("role") or "") != "master"
    ):
        raise ClusterStateError("join authorization signer is not an active master")
    key_id = _membership_signature_key_id(authorization)
    keys = [
        item for item in trust.signing_keys.get(authorizer, [])
        if str(item.get("key_id") or "") == key_id
    ]
    if len(keys) != 1:
        raise ClusterStateError("join authorization signer key is not authenticated")
    try:
        verify_operation(authorization, str(keys[0]["public_key"]))
    except ClusterCredentialError as exc:
        raise ClusterStateError(str(exc)) from exc


def _accept_membership_operation(
    trust: MembershipTrust,
    operation: dict[str, Any],
    *,
    allow_unsigned: bool,
    allow_legacy_key_claim: bool = False,
) -> None:
    """Authenticate and apply one membership operation to a trust state."""

    signed = bool(MEMBERSHIP_SIGNATURE_FIELDS.intersection(operation))
    if not signed:
        if not allow_unsigned:
            raise ClusterStateError("unsigned membership operations are historical replay only")
    op_id = str(operation["op_id"])
    if op_id in trust.validated_op_ids:
        return
    if not signed:
        _apply_membership(trust.nodes, trust.removed_node_ids, operation)
        node_id = str(operation["node_id"])
        _record_membership_key(trust, node_id, operation, start_seq=1)
        _record_membership_role(trust, node_id, operation)
        trust.validated_op_ids.add(op_id)
        return

    actor = str(operation["actor"])
    target = str(operation["node_id"])
    op = str(operation["op"])
    if str(operation.get("signer_id") or "") != actor:
        raise ClusterStateError("membership signer_id must match actor")
    if target in trust.removed_node_ids:
        raise ClusterStateError("removed nodes cannot be changed")

    if op == "ADD_NODE":
        if target != actor:
            raise ClusterStateError("ADD_NODE must be signed by the node being added")
        if target in trust.nodes and trust.nodes[target].get("state_replica", True) is not False:
            raise ClusterStateError("ADD_NODE target already exists")
        if not operation.get("signing_public_key"):
            raise ClusterStateError("ADD_NODE requires the node signing key")
        authorization = operation.get("membership_authorization")
        if isinstance(authorization, dict) and authorization.get("kind") == "bootstrap":
            replicas = [node for node in trust.nodes.values() if node.get("state_replica", True) is not False]
            if replicas:
                raise ClusterStateError("bootstrap ADD_NODE is only valid for an empty cluster")
            if str(operation.get("role") or "") != "master":
                raise ClusterStateError("cluster bootstrap node must be a master")
        else:
            _validate_join_authorization(trust, operation)
        try:
            verify_operation(operation, str(operation["signing_public_key"]))
        except ClusterCredentialError as exc:
            raise ClusterStateError(str(exc)) from exc
        _apply_membership(trust.nodes, trust.removed_node_ids, operation)
        _record_membership_key(trust, target, operation, start_seq=int(operation["seq"]))
        _record_membership_role(trust, target, operation)
        trust.validated_op_ids.add(op_id)
        return

    actor_node = trust.nodes.get(actor)
    if not isinstance(actor_node, dict):
        raise ClusterStateError("membership actor is not registered")
    if actor_node.get("enabled", True) is False or actor_node.get("state_replica", True) is False:
        raise ClusterStateError("membership actor is not an active state replica")
    legacy_key_claim = (
        op == "UPDATE_NODE_KEY"
        and actor == target
        and not trust.signing_keys.get(actor)
        and not any(actor_node.get(field) for field in CRYPTO_PUBLIC_FIELDS)
    )
    if legacy_key_claim:
        if not allow_legacy_key_claim:
            raise ClusterStateError("legacy key claim requires direct authenticated node transport")
        if _membership_signature_key_id(operation) != str(operation.get("signing_key_id") or ""):
            raise ClusterStateError("legacy key claim must be self-signed by the claimed key")
        if "role" in operation and str(operation["role"]) != str(actor_node.get("role") or ""):
            raise ClusterStateError("a node cannot change its own authenticated role")
        try:
            verify_operation(operation, str(operation["signing_public_key"]))
        except ClusterCredentialError as exc:
            raise ClusterStateError(str(exc)) from exc
        _apply_membership(trust.nodes, trust.removed_node_ids, operation)
        _record_membership_key(trust, target, operation, start_seq=int(operation["seq"]))
        _record_membership_role(trust, target, operation)
        trust.validated_op_ids.add(op_id)
        return
    try:
        verify_operation(operation, _membership_key_for_operation(trust, operation))
    except ClusterCredentialError as exc:
        raise ClusterStateError(str(exc)) from exc

    self_update = actor == target and op in {"UPDATE_NODE", "UPDATE_NODE_KEY"}
    if self_update and "role" in operation and str(operation["role"]) != str(actor_node.get("role") or ""):
        raise ClusterStateError("a node cannot change its own authenticated role")
    if not self_update:
        if str(actor_node.get("role") or "") != "master":
            raise ClusterStateError("membership administration requires an active master")
        if any(field in operation for field in ("signing_public_key", "encryption_public_key", "encryption_key_id")):
            raise ClusterStateError("only a node may rotate its own membership key")
    if op in MASTER_ADMIN_MEMBERSHIP_OPS and actor == target:
        raise ClusterStateError(f"{op} requires a different master actor")

    _apply_membership(trust.nodes, trust.removed_node_ids, operation)
    _record_membership_role(trust, target, operation)
    if self_update and operation.get("signing_public_key"):
        _record_membership_key(
            trust,
            target,
            operation,
            start_seq=int(operation["seq"]) + 1,
        )
    trust.validated_op_ids.add(op_id)


def create_join_authorization(
    cluster_root: Path,
    node_id: str,
    role: str,
    *,
    created_at: int | None = None,
) -> dict[str, Any]:
    """Create a signed, separate master approval for a node self-add."""

    identity = read_local_identity(cluster_root)
    actor = str(identity["node_id"])
    trust = _load_membership_trust(cluster_root, expected_cluster_id=str(identity["cluster_id"]))
    node = trust.nodes.get(actor)
    if not isinstance(node, dict) or str(node.get("role") or "") != "master":
        raise ClusterStateError("only a registered local master can authorize a join")
    _validate_node_id(node_id)
    if role not in {"master", "vps"}:
        raise ClusterStateError("node role must be master or vps")
    document = {
        "kind": "join",
        "cluster_id": str(identity["cluster_id"]),
        "node_id": node_id,
        "role": role,
        "created_at": int(created_at if created_at is not None else time.time()),
    }
    keys = ensure_node_key_material(cluster_root)
    return sign_operation(document, keys.signing_private_key, signer_id=actor)


def stage_membership_operations(
    cluster_root: Path,
    operations: Iterable[dict[str, Any]],
    *,
    expected_cluster_id: str,
    authenticated_remote_node: str | None = None,
) -> MembershipTrust:
    """Authenticate network membership changes before exposing staged keys."""

    trust = _load_membership_trust(
        cluster_root,
        expected_cluster_id=expected_cluster_id,
    )
    incoming_operations = list(operations)
    membership_operations = [
        operation
        for operation in incoming_operations
        if str(operation.get("op") or "") in MEMBERSHIP_OPS
    ]
    direct_claim_op_ids: set[str] = set()
    if authenticated_remote_node:
        authenticated_node = trust.nodes.get(authenticated_remote_node)
        authenticated_master = (
            isinstance(authenticated_node, dict)
            and str(authenticated_node.get("role") or "") == "master"
            and authenticated_node.get("enabled", True) is not False
            and authenticated_node.get("state_replica", True) is not False
        )
        actors = {authenticated_remote_node}
        if authenticated_master:
            actors.update(str(item.get("actor") or "") for item in incoming_operations)
        paths = ClusterPaths.from_root(cluster_root)
        for actor in actors:
            if not actor:
                continue
            expected_seq = _next_seq(paths, actor)
            for operation in sorted(
                (
                    item
                    for item in incoming_operations
                    if str(item.get("actor") or "") == actor
                    and int(item.get("seq") or 0) >= expected_seq
                ),
                key=lambda item: int(item.get("seq") or 0),
            ):
                if int(operation.get("seq") or 0) != expected_seq:
                    break
                if str(operation.get("op") or "") == "UPDATE_NODE_KEY":
                    direct_claim_op_ids.add(str(operation.get("op_id") or ""))
                expected_seq += 1
    membership_operations.sort(
        key=lambda item: (
            int(item.get("created_at") or 0),
            str(item.get("actor") or ""),
            int(item.get("seq") or 0),
            str(item.get("op_id") or ""),
        )
    )
    for operation in membership_operations:
        validate_operation(
            operation,
            expected_cluster_id=expected_cluster_id,
            membership_trust=trust,
            network_input=True,
            allow_legacy_key_claim=str(operation.get("op_id") or "") in direct_claim_op_ids,
        )
    return trust


def _load_membership_nodes(
    cluster_root: Path,
    *,
    expected_cluster_id: str,
) -> dict[str, dict[str, Any]]:
    """Return authenticated current membership without validating credential ops."""

    return _load_membership_trust(
        cluster_root,
        expected_cluster_id=expected_cluster_id,
    ).nodes


def _load_membership_trust(
    cluster_root: Path,
    *,
    expected_cluster_id: str,
) -> MembershipTrust:
    """Replay membership history, authenticating signed changes in order."""

    paths = ClusterPaths.from_root(cluster_root)
    membership_operations: list[dict[str, Any]] = []
    if paths.oplog.exists():
        for actor_dir in sorted(path for path in paths.oplog.iterdir() if path.is_dir()):
            for op_path in sorted(actor_dir.glob("*.json")):
                operation = _read_json(op_path)
                if not isinstance(operation, dict) or str(operation.get("op") or "") not in MEMBERSHIP_OPS:
                    continue
                validate_operation(
                    operation,
                    expected_cluster_id=expected_cluster_id,
                    validate_membership_auth=False,
                )
                membership_operations.append(operation)
    membership_operations.sort(
        key=lambda item: (
            int(item["created_at"]),
            str(item["actor"]),
            int(item["seq"]),
            str(item["op_id"]),
        )
    )
    trust = MembershipTrust.empty()
    for operation in membership_operations:
        _accept_membership_operation(
            trust,
            operation,
            allow_unsigned=True,
            allow_legacy_key_claim=True,
        )
    return trust


def _materialize_v2_credentials(operations: list[dict[str, Any]]) -> dict[str, Any]:
    """Build the non-secret v2 credential desired-state collections."""

    secret_ops: dict[str, list[dict[str, Any]]] = {}
    recipient_ops: dict[str, list[dict[str, Any]]] = {}
    pool_entry_ops: dict[str, list[dict[str, Any]]] = {}
    pool_state_ops: dict[str, list[dict[str, Any]]] = {}
    authority_ops: dict[str, list[dict[str, Any]]] = {}
    tradfi_active_ops: dict[str, list[dict[str, Any]]] = {}
    freeze_ops: list[dict[str, Any]] = []
    freeze_acks: list[dict[str, Any]] = []
    inventory_acks: list[dict[str, Any]] = []
    materialization_acks: list[dict[str, Any]] = []
    cutoff_ops: list[dict[str, Any]] = []
    cutoff_acks: list[dict[str, Any]] = []
    migration_candidates: dict[str, list[dict[str, Any]]] = {}
    migration_acceptances: dict[str, list[dict[str, Any]]] = {}
    migration_conflicts: dict[str, list[dict[str, Any]]] = {}
    migration_resolutions: dict[str, list[dict[str, Any]]] = {}
    projection_acks: list[dict[str, Any]] = []
    scan_acks: list[dict[str, Any]] = []
    for operation in operations:
        op = str(operation["op"])
        if op in {"UPSERT_SECRET", "TOMBSTONE_SECRET"}:
            secret_ops.setdefault(str(operation["secret_id"]), []).append(operation)
        elif op == "UPDATE_SECRET_RECIPIENTS":
            recipient_ops.setdefault(str(operation["secret_id"]), []).append(operation)
        elif op == "UPSERT_CMC_POOL_ENTRY":
            pool_entry_ops.setdefault(str(operation["key_id"]), []).append(operation)
        elif op == "SET_CMC_KEY_STATE":
            pool_state_ops.setdefault(str(operation["key_id"]), []).append(operation)
        elif op == "SET_CMC_AUTHORITY":
            authority_ops.setdefault(str(operation["quota_domain_id"]), []).append(operation)
        elif op == "SET_TRADFI_ACTIVE_PROFILE":
            tradfi_active_ops.setdefault(str(operation["provider"]), []).append(operation)
        elif op == "WRITER_FREEZE":
            freeze_ops.append(operation)
        elif op == "WRITER_FREEZE_ACK":
            freeze_acks.append(operation)
        elif op == "CREDENTIAL_INVENTORY_ACK":
            inventory_acks.append(operation)
        elif op == "CREDENTIAL_MATERIALIZATION_ACK":
            materialization_acks.append(operation)
        elif op == "CREDENTIAL_CUTOFF":
            cutoff_ops.append(operation)
        elif op == "CREDENTIAL_CUTOFF_ACK":
            cutoff_acks.append(operation)
        elif op == "MIGRATION_SECRET_CANDIDATE":
            migration_candidates.setdefault(str(operation["candidate_id"]), []).append(operation)
        elif op == "MIGRATION_SECRET_ACCEPTANCE":
            migration_acceptances.setdefault(str(operation["candidate_id"]), []).append(operation)
        elif op == "MIGRATION_SECRET_CONFLICT":
            migration_conflicts.setdefault(str(operation["conflict_id"]), []).append(operation)
        elif op == "MIGRATION_SECRET_CONFLICT_RESOLUTION":
            migration_resolutions.setdefault(str(operation["conflict_id"]), []).append(operation)
        elif op == "TRADFI_PROJECTION_ACK":
            projection_acks.append(operation)
        elif op == "CREDENTIAL_SCAN_ACK":
            scan_acks.append(operation)

    secrets: dict[str, dict[str, Any]] = {}
    secret_tombstones: dict[str, dict[str, Any]] = {}
    for secret_id, changes in secret_ops.items():
        ordered = sorted(changes, key=lambda item: (int(item["generation"]), str(item["op_id"])))
        conflicts = _generation_conflicts(ordered, "generation")
        winner = ordered[-1]
        tombstones = [item for item in ordered if item["op"] == "TOMBSTONE_SECRET"]
        latest_tombstone = tombstones[-1] if tombstones else None
        if latest_tombstone is not None and int(latest_tombstone["generation"]) >= int(winner["generation"]):
            secret_tombstones[secret_id] = {
                "generation": int(latest_tombstone["generation"]),
                "parent_generation": int(latest_tombstone["parent_generation"]),
                "secret_kind": str(
                    latest_tombstone.get("secret_kind", latest_tombstone.get("kind")) or ""
                ),
                "deleted_by": str(latest_tombstone["actor"]),
                "deleted_at": int(latest_tombstone["created_at"]),
                "op_id": str(latest_tombstone["op_id"]),
            }
            continue
        upserts = [item for item in ordered if item["op"] == "UPSERT_SECRET"]
        if not upserts:
            continue
        winner = upserts[-1]
        record = {
            "secret_id": secret_id,
            "secret_kind": str(winner.get("secret_kind", winner.get("kind")) or ""),
            "audience": str(winner["audience"]),
            "generation": int(winner["generation"]),
            "parent_generation": int(winner["parent_generation"]),
            "sealed_blob_hash": str(winner["sealed_blob_hash"]),
            "updated_by": str(winner["actor"]),
            "updated_at": int(winner["created_at"]),
            "op_id": str(winner["op_id"]),
            "conflicted": bool(conflicts),
        }
        initial_recipient_generation = int(winner.get("recipient_generation") or 1)
        recipient_changes = [
            item
            for item in recipient_ops.get(secret_id, [])
            if int(item.get("provider_generation") or 0) == int(winner["generation"])
        ]
        recipient_winner, recipient_conflicts = _recipient_generation_winner(
            winner,
            recipient_changes,
        )
        record.update({
            "recipient_generation": int(
                recipient_winner.get("recipient_generation") or initial_recipient_generation
            ),
            "parent_recipient_generation": int(
                recipient_winner.get("parent_recipient_generation") or 0
            ),
            "sealed_blob_hash": str(recipient_winner["sealed_blob_hash"]),
            "membership_generation": int(
                recipient_winner.get("membership_generation") or 0
            ),
            "recipient_ids": list(recipient_winner.get("recipient_ids") or []),
            "recipient_key_ids": dict(recipient_winner.get("recipient_key_ids") or {}),
            "recipient_updated_by": str(recipient_winner["actor"]),
            "recipient_updated_at": int(recipient_winner["created_at"]),
            "recipient_op_id": str(recipient_winner["op_id"]),
            "recipient_conflicted": bool(recipient_conflicts),
        })
        record["conflicted"] = bool(conflicts or recipient_conflicts)
        for field in ("label", "provider", "active", "shared", "lifecycle_state"):
            if field in winner:
                record[field] = winner[field]
        if conflicts:
            record["conflicts"] = conflicts
        if recipient_conflicts:
            record["recipient_conflicts"] = recipient_conflicts
        secrets[secret_id] = record

    entries: dict[str, dict[str, Any]] = {}
    for key_id, changes in pool_entry_ops.items():
        winner, conflicts = _generation_winner(changes, "catalog_generation", "generation")
        record = {
            key: value
            for key, value in winner.items()
            if key in {
                "key_id", "secret_id", "label", "state", "quota_domain_id",
                "provider_plan", "minute_limit", "daily_limit", "monthly_limit",
                "catalog_generation", "generation", "parent_generation", "active",
            }
        }
        record.update({
            "updated_by": str(winner["actor"]),
            "updated_at": int(winner["created_at"]),
            "op_id": str(winner["op_id"]),
            "conflicted": bool(conflicts),
        })
        if conflicts:
            record["conflicts"] = conflicts
            record["state"] = "conflicted"
        entries[key_id] = record
    for key_id, changes in pool_state_ops.items():
        winner, conflicts = _generation_winner(changes, "state_generation", "generation")
        record = entries.setdefault(key_id, {"key_id": key_id})
        record.update({
            "state": "conflicted" if conflicts else str(winner["state"]),
            "state_generation": int(winner.get("state_generation", winner.get("generation"))),
            "state_updated_by": str(winner["actor"]),
            "state_updated_at": int(winner["created_at"]),
        })
        if conflicts:
            record["state_conflicts"] = conflicts

    authorities: dict[str, dict[str, Any]] = {}
    for domain_id, changes in authority_ops.items():
        winner = sorted(changes, key=lambda item: (int(item["authority_epoch"]), str(item["op_id"])))[-1]
        conflicts = _parent_conflicts(changes, "parent_epoch", "authority_epoch")
        authorities[domain_id] = {
            "quota_domain_id": domain_id,
            "authority_node_id": str(winner["authority_node_id"]),
            "authority_epoch": int(winner["authority_epoch"]),
            "parent_epoch": int(winner["parent_epoch"]),
            "updated_by": str(winner["actor"]),
            "updated_at": int(winner["created_at"]),
            "op_id": str(winner["op_id"]),
            "conflicted": bool(conflicts),
            **({"conflicts": conflicts} if conflicts else {}),
        }

    active_tradfi_profiles: dict[str, dict[str, Any]] = {}
    for provider, changes in tradfi_active_ops.items():
        winner, conflicts = _generation_winner(
            changes,
            "activation_generation",
            "activation_generation",
        )
        profile_id = winner.get("profile_id")
        active_tradfi_profiles[provider] = {
            "provider": provider,
            "profile_id": str(profile_id) if profile_id is not None else None,
            "activation_generation": int(winner["activation_generation"]),
            "parent_generation": int(winner["parent_generation"]),
            "updated_by": str(winner["actor"]),
            "updated_at": int(winner["created_at"]),
            "op_id": str(winner["op_id"]),
            "conflicted": bool(conflicts),
            **({"conflicts": conflicts} if conflicts else {}),
        }

    credential_migration: dict[str, Any] = {"freeze_acks": {}, "inventory_acks": {}}
    current_materialization_acks: dict[str, dict[str, Any]] = {}
    for ack in materialization_acks:
        if "recipient_generations" not in ack:
            continue
        node_id = str(ack.get("node_id") or ack["actor"])
        candidate = {
            "membership_generation": int(ack.get("membership_generation") or 0),
            "credential_generations": dict(ack.get("credential_generations") or {}),
            "recipient_generations": dict(ack.get("recipient_generations") or {}),
            "acked_by": str(ack["actor"]),
            "acked_at": int(ack["created_at"]),
            "op_id": str(ack["op_id"]),
        }
        current = current_materialization_acks.get(node_id)
        if current is None or (
            candidate["membership_generation"],
            candidate["acked_at"],
            candidate["op_id"],
        ) > (
            current["membership_generation"],
            current["acked_at"],
            current["op_id"],
        ):
            current_materialization_acks[node_id] = candidate
    if freeze_ops:
        freeze = sorted(
            freeze_ops,
            key=lambda item: (int(item["freeze_generation"]), str(item["op_id"])),
        )[-1]
        credential_migration.update({
            "freeze_generation": int(freeze["freeze_generation"]),
            "frozen": bool(freeze.get("frozen", True)),
            "updated_by": str(freeze["actor"]),
            "updated_at": int(freeze["created_at"]),
        })
        for field in ("migration_operation_id",):
            if field in freeze:
                credential_migration[field] = freeze[field]
        active_generation = int(freeze["freeze_generation"])
        for ack in freeze_acks:
            if int(ack["freeze_generation"]) != active_generation:
                continue
            node_id = str(ack.get("node_id") or ack["actor"])
            credential_migration["freeze_acks"][node_id] = {
                "freeze_generation": active_generation,
                "acked_by": str(ack["actor"]),
                "acked_at": int(ack["created_at"]),
                "frozen": True,
                "process_readiness": list(ack.get("process_readiness") or []),
            }
        for ack in inventory_acks:
            if int(ack["freeze_generation"]) != active_generation:
                continue
            node_id = str(ack.get("node_id") or ack["actor"])
            credential_migration["inventory_acks"][node_id] = {
                "freeze_generation": active_generation,
                "acked_by": str(ack["actor"]),
                "acked_at": int(ack["created_at"]),
                "source_generations": dict(ack.get("source_generations") or {}),
                "source_fingerprints": dict(ack.get("source_fingerprints") or {}),
                "process_readiness": list(ack.get("process_readiness") or []),
                "op_id": str(ack["op_id"]),
            }
        for ack in materialization_acks:
            if int(ack.get("freeze_generation") or 0) != active_generation:
                continue
            node_id = str(ack.get("node_id") or ack["actor"])
            credential_migration.setdefault("materialization_acks", {})[node_id] = {
                "freeze_generation": active_generation,
                "acked_by": str(ack["actor"]),
                "acked_at": int(ack["created_at"]),
                "credential_generations": ack.get("credential_generations", {}),
            }

    if cutoff_ops:
        cutoff, conflicts = _generation_winner(
            cutoff_ops,
            "cutoff_generation",
            "cutoff_generation",
        )
        cutoff_generation = int(cutoff["cutoff_generation"])
        credential_migration["cutoff"] = {
            "cutoff_generation": cutoff_generation,
            "parent_generation": int(cutoff["parent_generation"]),
            "min_protocol": int(cutoff["min_protocol"]),
            "state_vector": dict(cutoff["state_vector"]),
            "obsolete_secret_blob_hashes": list(cutoff["obsolete_secret_blob_hashes"]),
            "published_by": str(cutoff["actor"]),
            "published_at": int(cutoff["created_at"]),
            "op_id": str(cutoff["op_id"]),
            "conflicted": bool(conflicts),
            **({"conflicts": conflicts} if conflicts else {}),
        }
        cleanup: dict[str, dict[str, Any]] = {}
        for ack in cutoff_acks:
            if int(ack.get("cutoff_generation") or 0) != cutoff_generation:
                continue
            node_id = str(ack.get("node_id") or ack["actor"])
            candidate = {
                "cutoff_generation": cutoff_generation,
                "state_vector": dict(ack.get("state_vector") or {}),
                "removed_secret_blob_hashes": list(ack.get("removed_secret_blob_hashes") or []),
                "acked_by": str(ack["actor"]),
                "acked_at": int(ack["created_at"]),
                "op_id": str(ack["op_id"]),
            }
            current = cleanup.get(node_id)
            if current is None or (candidate["acked_at"], candidate["op_id"]) > (
                current["acked_at"], current["op_id"]
            ):
                cleanup[node_id] = candidate
        credential_migration["cleanup_acks"] = {
            node_id: cleanup[node_id] for node_id in sorted(cleanup)
        }

    candidates: dict[str, dict[str, Any]] = {}
    for candidate_id, changes in migration_candidates.items():
        winner = sorted(changes, key=_operation_order_key)[-1]
        candidates[candidate_id] = {
            "candidate_id": candidate_id,
            "candidate_kind": str(winner["candidate_kind"]),
            "freeze_generation": int(winner["freeze_generation"]),
            "source_fingerprint": str(winner["source_fingerprint"]),
            "source_generation": int(winner["source_generation"]),
            "sealed_blob_hash": str(winner["sealed_blob_hash"]),
            "audience": "masters",
            "membership_generation": int(winner.get("membership_generation") or 0),
            "recipient_ids": list(winner.get("recipient_ids") or []),
            "recipient_key_ids": dict(winner.get("recipient_key_ids") or {}),
            "submitted_by": str(winner["actor"]),
            "submitted_at": int(winner["created_at"]),
            "op_id": str(winner["op_id"]),
        }
    acceptances: dict[str, dict[str, Any]] = {}
    for candidate_id, changes in migration_acceptances.items():
        winner = sorted(changes, key=_operation_order_key)[-1]
        acceptances[candidate_id] = {
            "candidate_id": candidate_id,
            "credential_id": str(winner["credential_id"]),
            "credential_generation": int(winner["credential_generation"]),
            "freeze_generation": int(winner["freeze_generation"]),
            "status": "accepted",
            "accepted_by": str(winner["actor"]),
            "accepted_at": int(winner["created_at"]),
            "op_id": str(winner["op_id"]),
        }
    if candidates:
        credential_migration["candidates"] = {
            key: candidates[key] for key in sorted(candidates)
        }
    if acceptances:
        credential_migration["candidate_acceptances"] = {
            key: acceptances[key] for key in sorted(acceptances)
        }
    conflicts: dict[str, dict[str, Any]] = {}
    for conflict_id, changes in migration_conflicts.items():
        winner = sorted(changes, key=_operation_order_key)[-1]
        resolutions = migration_resolutions.get(conflict_id, [])
        resolution = sorted(resolutions, key=_operation_order_key)[-1] if resolutions else None
        conflicts[conflict_id] = {
            "conflict_id": conflict_id,
            "candidate_id": str(winner["candidate_id"]),
            "existing_credential_id": str(winner["existing_credential_id"]),
            "provider": str(winner["provider"]),
            "freeze_generation": int(winner["freeze_generation"]),
            "status": "resolved" if resolution is not None else "unresolved",
            "reported_by": str(winner["actor"]),
            "reported_at": int(winner["created_at"]),
            "op_id": str(winner["op_id"]),
        }
        if resolution is not None:
            conflicts[conflict_id]["resolution"] = {
                "choice": str(resolution["choice"]),
                "resolution_id": str(resolution["resolution_id"]),
                "resolved_by": str(resolution["actor"]),
                "resolved_at": int(resolution["created_at"]),
                "op_id": str(resolution["op_id"]),
            }
    if conflicts:
        credential_migration["conflicts"] = {
            key: conflicts[key] for key in sorted(conflicts)
        }

    current_projection_acks: dict[str, dict[str, Any]] = {}
    for ack in projection_acks:
        node_id = str(ack.get("node_id") or ack["actor"])
        candidate = {
            "membership_generation": int(ack.get("membership_generation") or 0),
            "active_profile_generations": dict(ack.get("active_profile_generations") or {}),
            "projection_applied_generation": int(ack.get("projection_applied_generation") or 0),
            "projection_status": str(ack.get("projection_status") or ""),
            "acked_by": str(ack["actor"]),
            "acked_at": int(ack["created_at"]),
            "op_id": str(ack["op_id"]),
        }
        current = current_projection_acks.get(node_id)
        if current is None or (candidate["membership_generation"], candidate["acked_at"], candidate["op_id"]) > (
            current["membership_generation"], current["acked_at"], current["op_id"]
        ):
            current_projection_acks[node_id] = candidate

    current_scan_acks: dict[str, dict[str, Any]] = {}
    for ack in scan_acks:
        node_id = str(ack.get("node_id") or ack["actor"])
        candidate = {
            "freeze_generation": int(ack["freeze_generation"]),
            "cutoff_generation": int(ack.get("cutoff_generation") or 0),
            "status": str(ack["status"]),
            "clean": bool(ack.get("clean")),
            "findings": list(ack.get("findings") or []),
            "acked_by": str(ack["actor"]),
            "acked_at": int(ack["created_at"]),
            "op_id": str(ack["op_id"]),
        }
        current = current_scan_acks.get(node_id)
        if current is None or (candidate["freeze_generation"], candidate["cutoff_generation"], candidate["acked_at"], candidate["op_id"]) > (
            current["freeze_generation"], current["cutoff_generation"], current["acked_at"], current["op_id"]
        ):
            current_scan_acks[node_id] = candidate
    if current_scan_acks:
        credential_migration["scan_acks"] = {
            key: current_scan_acks[key] for key in sorted(current_scan_acks)
        }

    return {
        "secrets": {key: secrets[key] for key in sorted(secrets)},
        "secret_tombstones": {
            key: secret_tombstones[key] for key in sorted(secret_tombstones)
        },
        "cmc_pool": {
            "entries": {key: entries[key] for key in sorted(entries)},
            "authorities": {key: authorities[key] for key in sorted(authorities)},
        },
        "tradfi_active_profiles": {
            key: active_tradfi_profiles[key] for key in sorted(active_tradfi_profiles)
        },
        "credential_migration": credential_migration,
        "credential_materialization_acks": {
            key: current_materialization_acks[key]
            for key in sorted(current_materialization_acks)
        },
        "tradfi_projection_acks": {
            key: current_projection_acks[key]
            for key in sorted(current_projection_acks)
        },
    }


def _recipient_generation_winner(
    initial: dict[str, Any],
    changes: list[dict[str, Any]],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Follow the valid recipient CAS chain and report sibling conflicts."""

    current = initial
    current_generation = int(initial.get("recipient_generation") or 1)
    conflicts: list[dict[str, Any]] = []
    by_parent: dict[int, list[dict[str, Any]]] = {}
    for item in changes:
        by_parent.setdefault(int(item["parent_recipient_generation"]), []).append(item)
    visited: set[int] = set()
    while current_generation not in visited:
        visited.add(current_generation)
        children = sorted(
            by_parent.get(current_generation, []),
            key=lambda item: (int(item["recipient_generation"]), str(item["op_id"])),
        )
        if not children:
            break
        signatures = {
            (
                int(item["recipient_generation"]),
                str(item["sealed_blob_hash"]),
                tuple(item.get("recipient_ids") or []),
                tuple(sorted((item.get("recipient_key_ids") or {}).items())),
                int(item["membership_generation"]),
            )
            for item in children
        }
        if len(signatures) > 1:
            conflicts.extend({
                "op_id": str(item["op_id"]),
                "op": str(item["op"]),
                "parent_recipient_generation": current_generation,
                "recipient_generation": int(item["recipient_generation"]),
            } for item in children)
        current = children[-1]
        current_generation = int(current["recipient_generation"])
    return current, conflicts


def _generation_winner(
    changes: list[dict[str, Any]],
    primary_field: str,
    fallback_field: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Return the deterministic generation winner and sibling conflicts."""

    ordered = sorted(
        changes,
        key=lambda item: (int(item.get(primary_field, item.get(fallback_field))), str(item["op_id"])),
    )
    return ordered[-1], _parent_conflicts(ordered, "parent_generation", primary_field, fallback_field)


def _generation_conflicts(
    changes: list[dict[str, Any]],
    generation_field: str,
) -> list[dict[str, Any]]:
    """Return conflicting secret changes sharing one parent generation."""

    return _parent_conflicts(changes, "parent_generation", generation_field)


def _parent_conflicts(
    changes: list[dict[str, Any]],
    parent_field: str,
    generation_field: str,
    fallback_generation_field: str | None = None,
) -> list[dict[str, Any]]:
    """Describe distinct sibling changes without exposing credential values."""

    by_parent: dict[int, list[dict[str, Any]]] = {}
    for item in changes:
        by_parent.setdefault(int(item[parent_field]), []).append(item)
    conflicts: list[dict[str, Any]] = []
    for parent, siblings in by_parent.items():
        signatures = {
            (
                str(item["op"]),
                int(item.get(generation_field, item.get(fallback_generation_field or "", 0))),
                str(
                    item.get("sealed_blob_hash")
                    or item.get("state")
                    or item.get("authority_node_id")
                    or item.get("profile_id")
                    or ""
                ),
                json.dumps(item.get("state_vector") or {}, sort_keys=True, separators=(",", ":")),
                tuple(item.get("obsolete_secret_blob_hashes") or []),
            )
            for item in siblings
        }
        if len(signatures) < 2:
            continue
        conflicts.extend({
            "op_id": str(item["op_id"]),
            "op": str(item["op"]),
            "parent_generation": parent,
            "generation": int(item.get(generation_field, item.get(fallback_generation_field or "", 0))),
        } for item in siblings)
    return conflicts


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


def _as_nonnegative_int(value: Any, field: str) -> int:
    """Return *value* as a nonnegative int or raise ClusterStateError."""

    result = _as_int(value, field)
    if result < 0:
        raise ClusterStateError(f"{field} must be nonnegative")
    return result


def _validate_bounded_text(value: Any, field: str, *, maximum: int = 255) -> str:
    """Validate bounded non-empty operation metadata text."""

    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > maximum:
        raise ClusterStateError(f"invalid {field}")
    if any(ord(character) < 0x20 or ord(character) == 0x7f for character in value):
        raise ClusterStateError(f"invalid {field}")
    return value


def _validate_credential_id(value: Any, field: str) -> str:
    """Validate a stable credential identifier without imposing one store kind."""

    text = _validate_bounded_text(value, field)
    _validate_relative_name(text, field)
    return text


def _highest_contiguous_sequence(sequences: set[int]) -> int:
    """Return the highest actor sequence present without a gap from one."""

    contiguous = 0
    while contiguous + 1 in sequences:
        contiguous += 1
    return contiguous


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
