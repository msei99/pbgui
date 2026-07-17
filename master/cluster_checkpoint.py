"""Deterministic shadow checkpoints for bounded PBCluster history."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import time
from copy import deepcopy
from pathlib import Path
from typing import Any, Iterable, Mapping

from file_lock import advisory_file_lock
from cluster_credentials import (
    ClusterCredentialError,
    ensure_node_key_material,
    sign_operation,
    verify_operation,
)
from master.cluster_state import (
    API_KEY_OPS,
    CLUSTER_POLICY_OPS,
    MEMBERSHIP_OPS,
    V2_CREDENTIAL_OPS,
    V7_OPS,
    ClusterPaths,
    MembershipTrust,
    _apply_v7,
    _credential_membership_fingerprint,
    _load_membership_trust,
    _mark_conflicts,
    _materialize_retention_policy,
    _materialize_v2_credentials,
    append_operation,
    load_operations,
    read_local_identity,
    read_materialized_state,
    rebuild_materialized_state,
    validate_operation,
)
from secure_files import (
    atomic_write_private_text,
    ensure_private_directory,
    secure_private_file,
)


SERVICE = "ClusterCheckpoint"
CHECKPOINT_SCHEMA_VERSION = 2
DEFAULT_HISTORY_SECONDS = 7 * 24 * 60 * 60
DEFAULT_HISTORY_DAYS = 7
RETENTION_MODES = frozenset({"report_only", "oplog", "oplog_and_blobs"})
CHECKPOINT_KIND = "shadow"
CHECKPOINT_DIR_NAME = "checkpoints"
SHADOW_CHECKPOINT_NAME = "shadow.json"
CHECKPOINT_OBJECTS_DIR_NAME = "objects"
CHECKPOINT_COMMIT_NAME = "commit.json"
RETENTION_DIR_NAME = "retention"
PRUNE_JOURNAL_NAME = "prune_journal.json"
RETENTION_REPORT_NAME = "latest_report.json"
GC_CANDIDATES_NAME = "gc_candidates.json"
GC_JOURNAL_NAME = "gc_journal.json"
GC_REPORT_NAME = "latest_gc_report.json"
BLOB_GC_MIN_AGE_SECONDS = 60 * 60
MAX_BUILD_ATTEMPTS = 3


class ClusterCheckpointError(ValueError):
    """Raised when a cluster checkpoint cannot be built or verified safely."""


def build_shadow_checkpoint(
    cluster_root: Path | str,
    *,
    created_at: int | None = None,
    history_seconds: int | None = None,
) -> dict[str, Any]:
    """Build a deterministic checkpoint without changing or deleting cluster data."""

    root = Path(os.path.abspath(Path(cluster_root).expanduser()))
    identity = read_local_identity(root)
    cluster_id = str(identity["cluster_id"])
    timestamp = int(time.time()) if created_at is None else _nonnegative_int(created_at, "created_at")

    for _attempt in range(MAX_BUILD_ATTEMPTS):
        active_seed = read_active_checkpoint(root)
        seed_baseline = _normalized_vector(active_seed["baseline_vector"]) if active_seed is not None else {}
        initial_vector = _operation_file_vector(
            ClusterPaths.from_root(root).oplog,
            baseline=seed_baseline,
        )
        if active_seed is None:
            materialized = rebuild_materialized_state(root, write=False)
            operations = load_operations(root, expected_cluster_id=cluster_id)
            operation_vector = _complete_operation_vector(operations)
            reducer_operations = operations
            trust = _load_membership_trust(root, expected_cluster_id=cluster_id)
            operation_count = len(operations)
        else:
            tail = load_checkpoint_tail(root, active_seed)
            materialized = materialize_checkpoint_tail(active_seed, tail)
            operation_vector = _tail_vector(seed_baseline, tail)
            reducer_operations = [
                *(_json_copy(item) for item in active_seed["reducer_state"]["v2_basis_operations"]),
                *(_json_copy(item) for item in active_seed["reducer_state"]["retention_policy_basis_operations"]),
                *tail,
            ]
            trust = _membership_trust_after_tail(active_seed, tail)
            operation_count = int(active_seed.get("operation_count") or 0) + len(tail)
        baseline_vector = _normalized_vector(materialized.get("state_vector"))
        if initial_vector != baseline_vector or operation_vector != baseline_vector:
            continue

        final_vector = _operation_file_vector(
            ClusterPaths.from_root(root).oplog,
            baseline=seed_baseline,
        )
        if final_vector != baseline_vector:
            continue
        if _normalized_nodes(trust.nodes) != _normalized_nodes(
            (materialized.get("cluster_nodes") or {}).get("nodes")
        ):
            continue

        materialized_state = _json_copy(materialized)
        policy = retention_policy(materialized_state)
        retention = (
            int(policy["history_days"]) * 24 * 60 * 60
            if history_seconds is None
            else _positive_int(history_seconds, "history_seconds")
        )
        membership_trust = _serialize_membership_trust(trust)
        blob_refs = _collect_direct_blob_refs(materialized_state.get("desired_state"))
        migration_seal = build_migration_seal(materialized_state)
        reducer_state = _build_reducer_state(reducer_operations, materialized_state)
        state_hash = _sha256_json(materialized_state)
        membership_hash = _sha256_json(membership_trust)
        migration_seal_hash = _sha256_json(migration_seal)
        reducer_hash = _sha256_json(reducer_state)
        checkpoint_identity = {
            "schema_version": CHECKPOINT_SCHEMA_VERSION,
            "cluster_id": cluster_id,
            "baseline_vector": baseline_vector,
            "state_hash": state_hash,
            "membership_hash": membership_hash,
            "migration_seal_hash": migration_seal_hash,
            "reducer_hash": reducer_hash,
            "blob_refs": blob_refs,
        }
        checkpoint = {
            "schema_version": CHECKPOINT_SCHEMA_VERSION,
            "kind": CHECKPOINT_KIND,
            "cluster_id": cluster_id,
            "checkpoint_id": _sha256_json(checkpoint_identity),
            "created_at": timestamp,
            "history_seconds": retention,
            "history_cutoff": max(0, timestamp - retention),
            "operation_count": operation_count,
            "baseline_vector": baseline_vector,
            "state_hash": state_hash,
            "membership_hash": membership_hash,
            "migration_seal_hash": migration_seal_hash,
            "reducer_hash": reducer_hash,
            "retention_policy": policy,
            "migration_seal": migration_seal,
            "reducer_state": reducer_state,
            "blob_refs": blob_refs,
            "materialized": materialized_state,
            "membership_trust": membership_trust,
        }
        verify_shadow_checkpoint(checkpoint, expected_cluster_id=cluster_id)
        return checkpoint

    raise ClusterCheckpointError("cluster state changed while building checkpoint")


def write_shadow_checkpoint(
    cluster_root: Path | str,
    checkpoint: Mapping[str, Any],
) -> Path:
    """Verify and atomically persist one owner-only shadow checkpoint."""

    root = Path(os.path.abspath(Path(cluster_root).expanduser()))
    cluster_id = str(read_local_identity(root)["cluster_id"])
    clean = verify_shadow_checkpoint(checkpoint, expected_cluster_id=cluster_id)
    checkpoint_root = ensure_private_directory(root / CHECKPOINT_DIR_NAME)
    path = checkpoint_root / SHADOW_CHECKPOINT_NAME
    with advisory_file_lock(root / ".checkpoint"):
        atomic_write_private_text(path, json.dumps(clean, indent=4, sort_keys=True) + "\n")
    return path


def read_shadow_checkpoint(cluster_root: Path | str) -> dict[str, Any] | None:
    """Read and verify the local shadow checkpoint when present."""

    root = Path(os.path.abspath(Path(cluster_root).expanduser()))
    path = root / CHECKPOINT_DIR_NAME / SHADOW_CHECKPOINT_NAME
    if path.is_symlink():
        raise ClusterCheckpointError("shadow checkpoint must not be a symlink")
    if not path.exists():
        return None
    secure_private_file(path)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ClusterCheckpointError("shadow checkpoint is unreadable") from exc
    cluster_id = str(read_local_identity(root)["cluster_id"])
    return verify_shadow_checkpoint(payload, expected_cluster_id=cluster_id)


def create_shadow_checkpoint(
    cluster_root: Path | str,
    *,
    created_at: int | None = None,
    history_seconds: int | None = None,
) -> dict[str, Any]:
    """Build and persist a shadow checkpoint without pruning any files."""

    checkpoint = build_shadow_checkpoint(
        cluster_root,
        created_at=created_at,
        history_seconds=history_seconds,
    )
    write_shadow_checkpoint(cluster_root, checkpoint)
    return checkpoint


def write_checkpoint_object(
    cluster_root: Path | str,
    checkpoint: Mapping[str, Any],
) -> Path:
    """Persist one immutable verified checkpoint object by content identity."""

    root = Path(os.path.abspath(Path(cluster_root).expanduser()))
    cluster_id = str(read_local_identity(root)["cluster_id"])
    clean = verify_shadow_checkpoint(checkpoint, expected_cluster_id=cluster_id)
    objects_root = ensure_private_directory(
        ensure_private_directory(root / CHECKPOINT_DIR_NAME) / CHECKPOINT_OBJECTS_DIR_NAME
    )
    digest = str(clean["checkpoint_id"]).removeprefix("sha256:")
    path = objects_root / f"{digest}.json"
    backup_path = objects_root / f"{digest}.backup.json"
    raw = json.dumps(clean, indent=4, sort_keys=True) + "\n"
    with advisory_file_lock(root / ".append_sequence"):
        if path.exists():
            secure_private_file(path)
            try:
                existing = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise ClusterCheckpointError("checkpoint object is unreadable") from exc
            if verify_shadow_checkpoint(existing, expected_cluster_id=cluster_id) != clean:
                raise ClusterCheckpointError("checkpoint object already exists with different content")
        else:
            atomic_write_private_text(path, raw)
        if backup_path.exists():
            secure_private_file(backup_path)
            try:
                backup = json.loads(backup_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise ClusterCheckpointError("checkpoint backup object is unreadable") from exc
            if verify_shadow_checkpoint(backup, expected_cluster_id=cluster_id) != clean:
                raise ClusterCheckpointError("checkpoint backup object has different content")
        else:
            atomic_write_private_text(backup_path, raw)
    return path


def activate_checkpoint(
    cluster_root: Path | str,
    checkpoint: Mapping[str, Any],
    *,
    commit_proof: Mapping[str, Any],
    activated_at: int | None = None,
    allow_expired_proof: bool = False,
) -> dict[str, Any]:
    """Atomically activate a monotonic checkpoint while retaining one predecessor."""

    root = Path(os.path.abspath(Path(cluster_root).expanduser()))
    cluster_id = str(read_local_identity(root)["cluster_id"])
    clean = verify_shadow_checkpoint(checkpoint, expected_cluster_id=cluster_id)
    if clean["migration_seal"].get("status") != "sealed":
        raise ClusterCheckpointError("checkpoint migration seal is blocked")
    proof = verify_checkpoint_commit_proof(clean, commit_proof)
    timestamp = int(time.time()) if activated_at is None else _nonnegative_int(activated_at, "activated_at")
    if not allow_expired_proof and int((proof.get("proposal") or {}).get("expires_at") or 0) <= timestamp:
        raise ClusterCheckpointError("checkpoint commit proposal has expired")
    checkpoint_root = ensure_private_directory(root / CHECKPOINT_DIR_NAME)
    commit_path = checkpoint_root / CHECKPOINT_COMMIT_NAME
    with advisory_file_lock(root / ".append_sequence"):
        current_commit = _read_checkpoint_commit_unlocked(root)
        previous_id = str((current_commit or {}).get("current_checkpoint_id") or "")
        previous_epoch = int((current_commit or {}).get("epoch") or 0)
        previous_baseline = _normalized_vector((current_commit or {}).get("baseline_vector") or {})
        new_baseline = _normalized_vector(clean["baseline_vector"])
        for actor, sequence in previous_baseline.items():
            if int(new_baseline.get(actor, 0)) < sequence:
                raise ClusterCheckpointError("checkpoint baseline must not move backwards")
        write_checkpoint_object(root, clean)
        commit = {
            "schema_version": 1,
            "cluster_id": cluster_id,
            "epoch": previous_epoch + 1,
            "current_checkpoint_id": str(clean["checkpoint_id"]),
            "previous_checkpoint_id": previous_id or str(clean["checkpoint_id"]),
            "previous_commit_proof": _json_copy(
                (current_commit or {}).get("commit_proof") or proof
            ),
            "baseline_vector": new_baseline,
            "activated_at": timestamp,
            "commit_proof": proof,
        }
        commit["commit_id"] = _sha256_json({
            key: commit[key] for key in commit if key != "commit_id"
        })
        atomic_write_private_text(
            commit_path,
            json.dumps(commit, indent=4, sort_keys=True) + "\n",
        )
        verified = read_active_checkpoint(root)
        if verified is None or verified["checkpoint_id"] != clean["checkpoint_id"]:
            raise ClusterCheckpointError("activated checkpoint verification failed")
        return commit


def create_checkpoint_proposal(
    cluster_root: Path | str,
    checkpoint: Mapping[str, Any],
    *,
    expires_at: int,
    created_at: int | None = None,
) -> dict[str, Any]:
    """Create one coordinator-signed checkpoint proposal."""

    root = Path(os.path.abspath(Path(cluster_root).expanduser()))
    clean = verify_shadow_checkpoint(
        checkpoint,
        expected_cluster_id=str(read_local_identity(root)["cluster_id"]),
    )
    if clean["migration_seal"].get("status") != "sealed":
        raise ClusterCheckpointError("checkpoint migration seal is blocked")
    timestamp = int(time.time()) if created_at is None else _nonnegative_int(created_at, "created_at")
    expiry = _positive_int(expires_at, "expires_at")
    if expiry <= timestamp:
        raise ClusterCheckpointError("checkpoint proposal expiry must be in the future")
    identity = read_local_identity(root)
    node_id = str(identity["node_id"])
    coordinator_id = _checkpoint_coordinator_id(clean)
    if node_id != coordinator_id:
        raise ClusterCheckpointError("only the elected coordinator may propose a checkpoint")
    required_node_ids = list(clean["migration_seal"].get("active_node_ids") or [])
    proposal = {
        "schema_version": 1,
        "kind": "PBCLUSTER_CHECKPOINT_PROPOSAL",
        "cluster_id": str(clean["cluster_id"]),
        "checkpoint_id": str(clean["checkpoint_id"]),
        "baseline_vector": dict(clean["baseline_vector"]),
        "state_hash": str(clean["state_hash"]),
        "membership_hash": str(clean["membership_hash"]),
        "migration_seal_hash": str(clean["migration_seal_hash"]),
        "required_node_ids": required_node_ids,
        "coordinator_id": coordinator_id,
        "created_at": timestamp,
        "expires_at": expiry,
    }
    proposal["proposal_id"] = _sha256_json(proposal)
    return _sign_checkpoint_control(root, proposal)


def verify_checkpoint_proposal(
    checkpoint: Mapping[str, Any],
    proposal: Mapping[str, Any],
    *,
    now: int | None = None,
) -> dict[str, Any]:
    """Verify one exact coordinator proposal against a checkpoint object."""

    clean = verify_shadow_checkpoint(checkpoint)
    value = _json_copy(proposal)
    if not isinstance(value, dict) or value.get("kind") != "PBCLUSTER_CHECKPOINT_PROPOSAL":
        raise ClusterCheckpointError("invalid checkpoint proposal kind")
    timestamp = int(time.time()) if now is None else _nonnegative_int(now, "now")
    if int(value.get("expires_at") or 0) <= timestamp:
        raise ClusterCheckpointError("checkpoint proposal expired")
    coordinator_id = _checkpoint_coordinator_id(clean)
    unsigned = {
        key: item for key, item in value.items()
        if key not in {
            "proposal_id", "signer_id", "signing_key_id", "signer_key_id",
            "signature", "signature_version", "signature_algorithm",
        }
    }
    if str(value.get("proposal_id") or "") != _sha256_json(unsigned):
        raise ClusterCheckpointError("checkpoint proposal ID mismatch")
    expected = {
        "cluster_id": str(clean["cluster_id"]),
        "checkpoint_id": str(clean["checkpoint_id"]),
        "baseline_vector": dict(clean["baseline_vector"]),
        "state_hash": str(clean["state_hash"]),
        "membership_hash": str(clean["membership_hash"]),
        "migration_seal_hash": str(clean["migration_seal_hash"]),
        "required_node_ids": list(clean["migration_seal"].get("active_node_ids") or []),
        "coordinator_id": coordinator_id,
    }
    for field, expected_value in expected.items():
        if value.get(field) != expected_value:
            raise ClusterCheckpointError(f"checkpoint proposal {field} mismatch")
    if str(value.get("signer_id") or "") != coordinator_id:
        raise ClusterCheckpointError("checkpoint proposal signer is not coordinator")
    _verify_checkpoint_control_signature(clean, value, coordinator_id)
    return value


def create_checkpoint_ack(
    cluster_root: Path | str,
    checkpoint: Mapping[str, Any],
    proposal: Mapping[str, Any],
    *,
    created_at: int | None = None,
) -> dict[str, Any]:
    """Create a replica signature over one exact checkpoint proposal."""

    root = Path(os.path.abspath(Path(cluster_root).expanduser()))
    clean = verify_shadow_checkpoint(
        checkpoint,
        expected_cluster_id=str(read_local_identity(root)["cluster_id"]),
    )
    verified_proposal = verify_checkpoint_proposal(clean, proposal)
    node_id = str(read_local_identity(root)["node_id"])
    if node_id not in set(verified_proposal["required_node_ids"]):
        raise ClusterCheckpointError("local node is not a required checkpoint replica")
    timestamp = int(time.time()) if created_at is None else _nonnegative_int(created_at, "created_at")
    ack = {
        "schema_version": 1,
        "kind": "PBCLUSTER_CHECKPOINT_ACK",
        "cluster_id": str(clean["cluster_id"]),
        "proposal_id": str(verified_proposal["proposal_id"]),
        "checkpoint_id": str(clean["checkpoint_id"]),
        "baseline_vector": dict(clean["baseline_vector"]),
        "node_id": node_id,
        "created_at": timestamp,
        "expires_at": int(verified_proposal["expires_at"]),
    }
    return _sign_checkpoint_control(root, ack)


def verify_checkpoint_ack(
    checkpoint: Mapping[str, Any],
    proposal: Mapping[str, Any],
    ack: Mapping[str, Any],
    *,
    now: int | None = None,
) -> dict[str, Any]:
    """Verify one required replica ACK for one proposal."""

    clean = verify_shadow_checkpoint(checkpoint)
    verified_proposal = verify_checkpoint_proposal(clean, proposal, now=now)
    value = _json_copy(ack)
    if not isinstance(value, dict) or value.get("kind") != "PBCLUSTER_CHECKPOINT_ACK":
        raise ClusterCheckpointError("invalid checkpoint ACK kind")
    node_id = str(value.get("node_id") or "")
    if node_id not in set(verified_proposal["required_node_ids"]):
        raise ClusterCheckpointError("checkpoint ACK node is not required")
    expected = {
        "cluster_id": str(clean["cluster_id"]),
        "proposal_id": str(verified_proposal["proposal_id"]),
        "checkpoint_id": str(clean["checkpoint_id"]),
        "baseline_vector": dict(clean["baseline_vector"]),
        "expires_at": int(verified_proposal["expires_at"]),
        "signer_id": node_id,
    }
    for field, expected_value in expected.items():
        if value.get(field) != expected_value:
            raise ClusterCheckpointError(f"checkpoint ACK {field} mismatch")
    _verify_checkpoint_control_signature(clean, value, node_id)
    return value


def create_checkpoint_commit_proof(
    cluster_root: Path | str,
    checkpoint: Mapping[str, Any],
    proposal: Mapping[str, Any],
    acknowledgements: Iterable[Mapping[str, Any]],
    *,
    created_at: int | None = None,
) -> dict[str, Any]:
    """Create a coordinator-signed commit proof from all required ACKs."""

    root = Path(os.path.abspath(Path(cluster_root).expanduser()))
    clean = verify_shadow_checkpoint(
        checkpoint,
        expected_cluster_id=str(read_local_identity(root)["cluster_id"]),
    )
    verified_proposal = verify_checkpoint_proposal(clean, proposal)
    coordinator_id = _checkpoint_coordinator_id(clean)
    if str(read_local_identity(root)["node_id"]) != coordinator_id:
        raise ClusterCheckpointError("only the elected coordinator may commit a checkpoint")
    verified_acks: dict[str, dict[str, Any]] = {}
    for ack in acknowledgements:
        verified = verify_checkpoint_ack(clean, verified_proposal, ack)
        node_id = str(verified["node_id"])
        if node_id in verified_acks:
            raise ClusterCheckpointError("duplicate checkpoint ACK")
        verified_acks[node_id] = verified
    required = list(verified_proposal["required_node_ids"])
    if sorted(verified_acks) != required:
        raise ClusterCheckpointError("checkpoint commit requires every active replica ACK")
    timestamp = int(time.time()) if created_at is None else _nonnegative_int(created_at, "created_at")
    proof = {
        "schema_version": 1,
        "kind": "PBCLUSTER_CHECKPOINT_COMMIT",
        "cluster_id": str(clean["cluster_id"]),
        "proposal": verified_proposal,
        "proposal_id": str(verified_proposal["proposal_id"]),
        "checkpoint_id": str(clean["checkpoint_id"]),
        "baseline_vector": dict(clean["baseline_vector"]),
        "required_node_ids": required,
        "acknowledgements": [verified_acks[node_id] for node_id in required],
        "coordinator_id": coordinator_id,
        "created_at": timestamp,
    }
    proof["commit_proof_id"] = _sha256_json(proof)
    return _sign_checkpoint_control(root, proof)


def verify_checkpoint_commit_proof(
    checkpoint: Mapping[str, Any],
    commit_proof: Mapping[str, Any],
    *,
    anchor_trust: MembershipTrust | None = None,
) -> dict[str, Any]:
    """Verify a complete coordinator commit proof and every embedded ACK."""

    clean = verify_shadow_checkpoint(checkpoint)
    proof = _json_copy(commit_proof)
    if not isinstance(proof, dict) or proof.get("kind") != "PBCLUSTER_CHECKPOINT_COMMIT":
        raise ClusterCheckpointError("invalid checkpoint commit proof kind")
    proposal = verify_checkpoint_proposal(clean, proof.get("proposal") or {}, now=int(proof.get("created_at") or 0))
    coordinator_id = _checkpoint_coordinator_id(clean)
    unsigned = {
        key: item for key, item in proof.items()
        if key not in {
            "commit_proof_id", "signer_id", "signing_key_id", "signer_key_id",
            "signature", "signature_version", "signature_algorithm",
        }
    }
    if str(proof.get("commit_proof_id") or "") != _sha256_json(unsigned):
        raise ClusterCheckpointError("checkpoint commit proof ID mismatch")
    if (
        str(proof.get("checkpoint_id") or "") != str(clean["checkpoint_id"])
        or proof.get("baseline_vector") != clean["baseline_vector"]
        or str(proof.get("proposal_id") or "") != str(proposal["proposal_id"])
        or proof.get("required_node_ids") != proposal["required_node_ids"]
        or str(proof.get("coordinator_id") or "") != coordinator_id
        or str(proof.get("signer_id") or "") != coordinator_id
    ):
        raise ClusterCheckpointError("checkpoint commit proof does not match checkpoint")
    acks = proof.get("acknowledgements")
    if not isinstance(acks, list):
        raise ClusterCheckpointError("checkpoint commit acknowledgements are invalid")
    verified_nodes = sorted(
        str(verify_checkpoint_ack(clean, proposal, ack, now=int(proof.get("created_at") or 0))["node_id"])
        for ack in acks
    )
    if verified_nodes != list(proposal["required_node_ids"]):
        raise ClusterCheckpointError("checkpoint commit ACK set is incomplete")
    _verify_checkpoint_control_signature(clean, proof, coordinator_id)
    if anchor_trust is not None:
        anchor = anchor_trust.nodes.get(coordinator_id)
        if (
            not isinstance(anchor, Mapping)
            or str(anchor.get("role") or "") != "master"
            or anchor.get("enabled", True) is False
            or anchor.get("state_replica", True) is False
        ):
            raise ClusterCheckpointError("checkpoint coordinator is not trusted by local history")
        public_key = str(anchor.get("signing_public_key") or "")
        if not public_key:
            raise ClusterCheckpointError("trusted checkpoint coordinator has no signing key")
        try:
            verify_operation(proof, public_key)
        except ClusterCredentialError as exc:
            raise ClusterCheckpointError("checkpoint proof failed local trust anchor") from exc
    return proof


def _sign_checkpoint_control(root: Path, value: Mapping[str, Any]) -> dict[str, Any]:
    """Sign one domain-separated checkpoint control document with local keys."""

    try:
        keys = ensure_node_key_material(root)
        node_id = str(read_local_identity(root)["node_id"])
        return sign_operation(value, keys.signing_private_key, signer_id=node_id)
    except ClusterCredentialError as exc:
        raise ClusterCheckpointError(str(exc)) from exc


def _verify_checkpoint_control_signature(
    checkpoint: Mapping[str, Any],
    value: Mapping[str, Any],
    node_id: str,
) -> None:
    """Verify one checkpoint control signature against checkpoint trust."""

    node = (checkpoint.get("membership_trust") or {}).get("nodes", {}).get(node_id)
    public_key = str((node or {}).get("signing_public_key") or "")
    if not public_key:
        raise ClusterCheckpointError("checkpoint control signer has no trusted key")
    try:
        verify_operation(value, public_key)
    except ClusterCredentialError as exc:
        raise ClusterCheckpointError(str(exc)) from exc


def _checkpoint_coordinator_id(checkpoint: Mapping[str, Any]) -> str:
    """Return the deterministic active-master checkpoint coordinator."""

    trust_nodes = (checkpoint.get("membership_trust") or {}).get("nodes") or {}
    masters = sorted(
        str(node_id)
        for node_id, node in trust_nodes.items()
        if isinstance(node, Mapping)
        and str(node.get("role") or "") == "master"
        and node.get("enabled", True) is not False
        and node.get("state_replica", True) is not False
    )
    if not masters:
        raise ClusterCheckpointError("checkpoint has no active master coordinator")
    return masters[0]


def read_active_checkpoint(cluster_root: Path | str) -> dict[str, Any] | None:
    """Read the active committed checkpoint object with pointer verification."""

    root = Path(os.path.abspath(Path(cluster_root).expanduser()))
    commit = _read_checkpoint_commit_unlocked(root)
    if commit is None:
        return None
    cluster_id = str(read_local_identity(root)["cluster_id"])
    if str(commit.get("cluster_id") or "") != cluster_id:
        raise ClusterCheckpointError("checkpoint commit belongs to another cluster")
    supplied_commit_id = str(commit.get("commit_id") or "")
    unsigned = {key: value for key, value in commit.items() if key != "commit_id"}
    if supplied_commit_id != _sha256_json(unsigned):
        raise ClusterCheckpointError("checkpoint commit hash mismatch")
    checkpoint_id = str(commit.get("current_checkpoint_id") or "")
    try:
        checkpoint = _read_checkpoint_object(root, checkpoint_id)
        if _normalized_vector(commit.get("baseline_vector")) != checkpoint["baseline_vector"]:
            raise ClusterCheckpointError("checkpoint commit baseline mismatch")
        verify_checkpoint_commit_proof(checkpoint, commit.get("commit_proof") or {})
        return checkpoint
    except ClusterCheckpointError as current_error:
        previous_id = str(commit.get("previous_checkpoint_id") or "")
        if not previous_id or previous_id == checkpoint_id:
            raise current_error
        try:
            previous = _read_checkpoint_object(root, previous_id)
            verify_checkpoint_commit_proof(previous, commit.get("previous_commit_proof") or {})
            return previous
        except ClusterCheckpointError:
            raise current_error


def active_checkpoint_baseline(cluster_root: Path | str) -> dict[str, int]:
    """Return the committed baseline or an empty vector before activation."""

    active = read_active_checkpoint(cluster_root)
    return dict(active["baseline_vector"]) if active is not None else {}


def checkpoint_status(cluster_root: Path | str) -> dict[str, Any]:
    """Return secret-free active checkpoint metadata for sync negotiation."""

    root = Path(os.path.abspath(Path(cluster_root).expanduser()))
    commit = _read_checkpoint_commit_unlocked(root)
    if commit is None:
        return {
            "active": False,
            "checkpoint_id": "",
            "checkpoint_epoch": 0,
            "checkpoint_baseline": {},
            "previous_checkpoint_id": "",
        }
    active = read_active_checkpoint(root)
    assert active is not None
    return {
        "active": True,
        "checkpoint_id": str(active["checkpoint_id"]),
        "checkpoint_epoch": int(commit.get("epoch") or 0),
        "checkpoint_baseline": dict(active["baseline_vector"]),
        "previous_checkpoint_id": str(commit.get("previous_checkpoint_id") or ""),
        "activated_at": int(commit.get("activated_at") or 0),
        "recovery_fallback": str(active["checkpoint_id"]) != str(commit.get("current_checkpoint_id") or ""),
    }


def active_checkpoint_bundle(cluster_root: Path | str) -> dict[str, Any] | None:
    """Return active checkpoint and commit proof for authenticated rebootstrap."""

    root = Path(os.path.abspath(Path(cluster_root).expanduser()))
    commit = _read_checkpoint_commit_unlocked(root)
    if commit is None:
        return None
    checkpoint = read_active_checkpoint(root)
    assert checkpoint is not None
    active_id = str(checkpoint["checkpoint_id"])
    proof = (
        commit.get("commit_proof")
        if active_id == str(commit.get("current_checkpoint_id") or "")
        else commit.get("previous_commit_proof")
    )
    return {
        "checkpoint": checkpoint,
        "commit_proof": _json_copy(proof or {}),
        "epoch": int(commit.get("epoch") or 0),
        "previous_checkpoint_id": str(commit.get("previous_checkpoint_id") or ""),
    }


def load_checkpoint_tail(
    cluster_root: Path | str,
    checkpoint: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Load operation files strictly above one checkpoint baseline."""

    root = Path(os.path.abspath(Path(cluster_root).expanduser()))
    active = (
        verify_shadow_checkpoint(
            checkpoint,
            expected_cluster_id=str(read_local_identity(root)["cluster_id"]),
        )
        if checkpoint is not None
        else read_active_checkpoint(root)
    )
    if active is None:
        return []
    baseline = _normalized_vector(active["baseline_vector"])
    paths = ClusterPaths.from_root(root)
    operations: list[dict[str, Any]] = []
    if not paths.oplog.exists():
        return operations
    for actor_dir in sorted(path for path in paths.oplog.iterdir() if path.is_dir()):
        actor = str(actor_dir.name)
        expected = int(baseline.get(actor, 0)) + 1
        for path in sorted(actor_dir.glob("*.json")):
            try:
                seq = int(path.stem)
            except ValueError as exc:
                raise ClusterCheckpointError("oplog contains an invalid operation filename") from exc
            if seq <= int(baseline.get(actor, 0)):
                continue
            if seq != expected:
                raise ClusterCheckpointError(
                    f"checkpoint tail sequence gap for actor {actor}: expected {expected}, got {seq}"
                )
            try:
                operation = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise ClusterCheckpointError("checkpoint tail operation is unreadable") from exc
            if not isinstance(operation, dict) or str(operation.get("actor") or "") != actor or int(operation.get("seq") or 0) != seq:
                raise ClusterCheckpointError("checkpoint tail operation path mismatch")
            operations.append(operation)
            expected += 1
    operations.sort(key=_operation_order)
    return operations


def install_rebootstrap_checkpoint(
    cluster_root: Path | str,
    checkpoint: Mapping[str, Any],
    commit_proof: Mapping[str, Any],
    *,
    installed_at: int | None = None,
    join_authorization: Mapping[str, Any] | None = None,
    join_anchor_public_key: str | None = None,
    defer_blob_validation: bool = False,
) -> dict[str, Any]:
    """Install a newer proven checkpoint on a non-divergent stale replica."""

    root = Path(os.path.abspath(Path(cluster_root).expanduser()))
    timestamp = int(time.time()) if installed_at is None else _nonnegative_int(installed_at, "installed_at")
    clean = verify_shadow_checkpoint(
        checkpoint,
        expected_cluster_id=str(read_local_identity(root)["cluster_id"]),
    )
    node_id = str(read_local_identity(root)["node_id"])
    joining = join_authorization is not None or join_anchor_public_key is not None
    if joining:
        authorization = _json_copy(join_authorization or {})
        anchor_key = str(join_anchor_public_key or "")
        proof = verify_checkpoint_commit_proof(clean, commit_proof)
        coordinator_id = str(proof.get("coordinator_id") or "")
        if (
            not anchor_key
            or str(authorization.get("kind") or "") != "join"
            or str(authorization.get("cluster_id") or "") != str(clean["cluster_id"])
            or str(authorization.get("node_id") or "") != node_id
            or str(authorization.get("signer_id") or "") != coordinator_id
        ):
            raise ClusterCheckpointError("checkpoint join authorization is invalid")
        try:
            verify_operation(authorization, anchor_key)
            verify_operation(proof, anchor_key)
        except ClusterCredentialError as exc:
            raise ClusterCheckpointError("checkpoint join anchor verification failed") from exc
    else:
        anchor_trust = _load_membership_trust(
            root,
            expected_cluster_id=str(clean["cluster_id"]),
        )
        proof = verify_checkpoint_commit_proof(
            clean,
            commit_proof,
            anchor_trust=anchor_trust,
        )
        if node_id not in set(proof.get("required_node_ids") or []):
            raise ClusterCheckpointError("rebootstrap checkpoint was not acknowledged by local node")
    with advisory_file_lock(root / ".append_sequence"):
        local = rebuild_materialized_state(root, write=False)
        local_vector = _normalized_vector(local.get("state_vector"))
        baseline = _normalized_vector(clean["baseline_vector"])
        for actor, sequence in local_vector.items():
            if sequence > int(baseline.get(actor, 0)):
                raise ClusterCheckpointError("local operation tail diverges beyond rebootstrap checkpoint")
        if joining and local_vector:
            raise ClusterCheckpointError("checkpoint join requires an empty local operation history")
        paths = ClusterPaths.from_root(root)
        reachable = {kind: set(values) for kind, values in (clean.get("blob_refs") or {}).items()}
        for kind in ("config", "secret", "sealed"):
            reachable.setdefault(kind, set())
        if not defer_blob_validation:
            _expand_config_manifest_refs(paths, reachable["config"])
            _verify_reachable_blobs(paths, reachable)
        commit = activate_checkpoint(
            root,
            clean,
            commit_proof=proof,
            activated_at=timestamp,
            allow_expired_proof=True,
        )
        quarantine_path = ""
        if paths.oplog.exists() and any(paths.oplog.iterdir()):
            quarantine_root = ensure_private_directory(root / "oplog_quarantine")
            quarantine = quarantine_root / f"{timestamp}-{str(clean['checkpoint_id']).removeprefix('sha256:')[:12]}"
            if quarantine.exists():
                raise ClusterCheckpointError("rebootstrap oplog quarantine already exists")
            os.replace(paths.oplog, quarantine)
            paths.oplog.mkdir(parents=True, exist_ok=True)
            quarantine_path = str(quarantine.relative_to(root))
        rebuilt = rebuild_materialized_state(root, write=True)
        if rebuilt != clean["materialized"]:
            raise ClusterCheckpointError("rebootstrap materialized state mismatch")
        return {
            "status": "installed",
            "checkpoint_id": str(clean["checkpoint_id"]),
            "epoch": int(commit["epoch"]),
            "quarantine_path": quarantine_path,
        }


def _read_checkpoint_commit_unlocked(root: Path) -> dict[str, Any] | None:
    """Read the owner-only checkpoint pointer without acquiring history lock."""

    path = root / CHECKPOINT_DIR_NAME / CHECKPOINT_COMMIT_NAME
    if path.is_symlink():
        raise ClusterCheckpointError("checkpoint commit must not be a symlink")
    if not path.exists():
        return None
    secure_private_file(path)
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ClusterCheckpointError("checkpoint commit is unreadable") from exc
    if not isinstance(value, dict) or value.get("schema_version") != 1:
        raise ClusterCheckpointError("checkpoint commit is invalid")
    return value


def _checkpoint_object_path(root: Path, checkpoint_id: str) -> Path:
    """Resolve one validated content-addressed checkpoint object path."""

    text = str(checkpoint_id)
    if not _is_sha256(text):
        raise ClusterCheckpointError("invalid checkpoint ID")
    return root / CHECKPOINT_DIR_NAME / CHECKPOINT_OBJECTS_DIR_NAME / f"{text.removeprefix('sha256:')}.json"


def verify_shadow_checkpoint(
    checkpoint: Mapping[str, Any],
    *,
    expected_cluster_id: str | None = None,
) -> dict[str, Any]:
    """Validate checkpoint shape and all deterministic hashes."""

    if not isinstance(checkpoint, Mapping):
        raise ClusterCheckpointError("checkpoint must be an object")
    clean = _json_copy(checkpoint)
    if clean.get("schema_version") != CHECKPOINT_SCHEMA_VERSION:
        raise ClusterCheckpointError("unsupported checkpoint schema_version")
    if clean.get("kind") != CHECKPOINT_KIND:
        raise ClusterCheckpointError("unsupported checkpoint kind")
    cluster_id = str(clean.get("cluster_id") or "")
    if not cluster_id or (expected_cluster_id and cluster_id != expected_cluster_id):
        raise ClusterCheckpointError("checkpoint belongs to another cluster")

    baseline_vector = _normalized_vector(clean.get("baseline_vector"))
    materialized = clean.get("materialized")
    membership_trust = clean.get("membership_trust")
    migration_seal = clean.get("migration_seal")
    reducer_state = clean.get("reducer_state")
    blob_refs = _normalized_blob_refs(clean.get("blob_refs"))
    if (
        not isinstance(materialized, dict)
        or not isinstance(membership_trust, dict)
        or not isinstance(migration_seal, dict)
        or not isinstance(reducer_state, dict)
    ):
        raise ClusterCheckpointError("checkpoint state is invalid")
    if _normalized_vector(materialized.get("state_vector")) != baseline_vector:
        raise ClusterCheckpointError("checkpoint baseline does not match materialized state")
    if str((materialized.get("cluster_nodes") or {}).get("cluster_id") or "") != cluster_id:
        raise ClusterCheckpointError("checkpoint materialized state belongs to another cluster")
    if str((materialized.get("desired_state") or {}).get("cluster_id") or "") != cluster_id:
        raise ClusterCheckpointError("checkpoint desired state belongs to another cluster")

    state_hash = _sha256_json(materialized)
    membership_hash = _sha256_json(membership_trust)
    expected_migration_seal = build_migration_seal(materialized)
    migration_seal_hash = _sha256_json(expected_migration_seal)
    reducer_hash = _sha256_json(reducer_state)
    if clean.get("state_hash") != state_hash:
        raise ClusterCheckpointError("checkpoint state hash mismatch")
    if clean.get("membership_hash") != membership_hash:
        raise ClusterCheckpointError("checkpoint membership hash mismatch")
    if migration_seal != expected_migration_seal or clean.get("migration_seal_hash") != migration_seal_hash:
        raise ClusterCheckpointError("checkpoint migration seal mismatch")
    if clean.get("reducer_hash") != reducer_hash:
        raise ClusterCheckpointError("checkpoint reducer hash mismatch")
    _verify_reducer_state(reducer_state, materialized)
    policy = retention_policy(materialized)
    if clean.get("retention_policy") != policy:
        raise ClusterCheckpointError("checkpoint retention policy mismatch")
    checkpoint_identity = {
        "schema_version": CHECKPOINT_SCHEMA_VERSION,
        "cluster_id": cluster_id,
        "baseline_vector": baseline_vector,
        "state_hash": state_hash,
        "membership_hash": membership_hash,
        "migration_seal_hash": migration_seal_hash,
        "reducer_hash": reducer_hash,
        "blob_refs": blob_refs,
    }
    if clean.get("checkpoint_id") != _sha256_json(checkpoint_identity):
        raise ClusterCheckpointError("checkpoint ID mismatch")

    _nonnegative_int(clean.get("created_at"), "created_at")
    history_seconds = _positive_int(clean.get("history_seconds"), "history_seconds")
    history_cutoff = _nonnegative_int(clean.get("history_cutoff"), "history_cutoff")
    if history_cutoff != max(0, int(clean["created_at"]) - history_seconds):
        raise ClusterCheckpointError("checkpoint history cutoff is invalid")
    _nonnegative_int(clean.get("operation_count"), "operation_count")
    clean["baseline_vector"] = baseline_vector
    clean["blob_refs"] = blob_refs
    return clean


def retention_policy(materialized: Mapping[str, Any] | None) -> dict[str, Any]:
    """Return the fail-safe cluster retention policy from materialized state."""

    desired = materialized.get("desired_state") if isinstance(materialized, Mapping) else None
    raw = desired.get("retention_policy") if isinstance(desired, Mapping) else None
    if not isinstance(raw, Mapping):
        return {
            "generation": 0,
            "mode": "report_only",
            "history_days": DEFAULT_HISTORY_DAYS,
            "conflicted": False,
        }
    try:
        generation = _nonnegative_int(raw.get("generation"), "retention generation")
        history_days = _positive_int(raw.get("history_days"), "history_days")
    except ClusterCheckpointError:
        return {
            "generation": 0,
            "mode": "report_only",
            "history_days": DEFAULT_HISTORY_DAYS,
            "conflicted": True,
        }
    mode = str(raw.get("mode") or "")
    conflicted = raw.get("conflicted") is True or mode not in RETENTION_MODES
    return {
        "generation": generation,
        "mode": "report_only" if conflicted else mode,
        "history_days": history_days,
        "conflicted": conflicted,
        **({"updated_by": str(raw.get("updated_by") or "")} if raw.get("updated_by") else {}),
        **({"updated_at": int(raw.get("updated_at") or 0)} if raw.get("updated_at") else {}),
        **({"op_id": str(raw.get("op_id") or "")} if raw.get("op_id") else {}),
    }


def set_retention_policy(
    cluster_root: Path | str,
    *,
    mode: str,
    history_days: int,
    expected_generation: int,
) -> dict[str, Any]:
    """Append one signed master-only cluster retention policy update."""

    root = Path(os.path.abspath(Path(cluster_root).expanduser()))
    selected_mode = str(mode or "")
    if selected_mode not in RETENTION_MODES:
        raise ClusterCheckpointError("invalid retention mode")
    days = _positive_int(history_days, "history_days")
    if days > 3650:
        raise ClusterCheckpointError("history_days must not exceed 3650")
    expected = _nonnegative_int(expected_generation, "expected_generation")
    materialized = read_materialized_state(root)
    current = retention_policy(materialized)
    if current.get("conflicted") is True:
        raise ClusterCheckpointError("retention policy is conflicted")
    if int(current["generation"]) != expected:
        raise ClusterCheckpointError("retention policy generation changed")
    identity = read_local_identity(root)
    local_node = (((materialized.get("cluster_nodes") or {}).get("nodes") or {}).get(str(identity["node_id"])) or {})
    if (
        str(local_node.get("role") or "") != "master"
        or local_node.get("enabled", True) is False
        or local_node.get("state_replica", True) is False
    ):
        raise ClusterCheckpointError("retention policy requires an active local master")
    if str(current["mode"]) == selected_mode and int(current["history_days"]) == days:
        return {"changed": False, "policy": current}
    operation = append_operation(
        root,
        "SET_RETENTION_POLICY",
        {
            "generation": expected + 1,
            "parent_generation": expected,
            "mode": selected_mode,
            "history_days": days,
        },
    )
    updated = rebuild_materialized_state(root)
    return {
        "changed": True,
        "operation_id": str(operation["op_id"]),
        "policy": retention_policy(updated),
    }


def build_migration_seal(materialized: Mapping[str, Any]) -> dict[str, Any]:
    """Build deterministic v2/CMC safety evidence required before pruning."""

    cluster_nodes = materialized.get("cluster_nodes") if isinstance(materialized, Mapping) else None
    desired = materialized.get("desired_state") if isinstance(materialized, Mapping) else None
    nodes = cluster_nodes.get("nodes") if isinstance(cluster_nodes, Mapping) else None
    nodes = nodes if isinstance(nodes, Mapping) else {}
    desired = desired if isinstance(desired, Mapping) else {}
    active_nodes = {
        str(node_id): node
        for node_id, node in nodes.items()
        if isinstance(node, Mapping)
        and node.get("enabled", True) is not False
        and node.get("state_replica", True) is not False
    }
    active_node_ids = set(active_nodes)
    active_master_ids = {
        node_id for node_id, node in active_nodes.items()
        if str(node.get("role") or "") == "master"
    }
    membership_generation = int((cluster_nodes or {}).get("credential_membership_generation") or 0)
    migration = desired.get("credential_migration") if isinstance(desired.get("credential_migration"), Mapping) else {}
    cutoff = migration.get("cutoff") if isinstance(migration.get("cutoff"), Mapping) else {}
    secrets = desired.get("secrets") if isinstance(desired.get("secrets"), Mapping) else {}
    tombstones = desired.get("secret_tombstones") if isinstance(desired.get("secret_tombstones"), Mapping) else {}
    materialization_acks = desired.get("credential_materialization_acks") if isinstance(desired.get("credential_materialization_acks"), Mapping) else {}
    projection_acks = desired.get("tradfi_projection_acks") if isinstance(desired.get("tradfi_projection_acks"), Mapping) else {}
    active_tradfi = desired.get("tradfi_active_profiles") if isinstance(desired.get("tradfi_active_profiles"), Mapping) else {}
    cmc_pool = desired.get("cmc_pool") if isinstance(desired.get("cmc_pool"), Mapping) else {}
    cmc_entries = cmc_pool.get("entries") if isinstance(cmc_pool.get("entries"), Mapping) else {}
    cmc_authorities = cmc_pool.get("authorities") if isinstance(cmc_pool.get("authorities"), Mapping) else {}
    blockers: set[str] = set()

    if not active_nodes:
        blockers.add("no_active_replicas")
    for node_id, node in active_nodes.items():
        if int(node.get("credential_protocol_version") or 0) < 2 or node.get("credential_capable") is not True:
            blockers.add(f"node_not_v2:{node_id}")
    if migration.get("frozen") is True:
        blockers.add("migration_frozen")
    if migration.get("blocked") is True:
        blockers.add("migration_blocked")
    for item in migration.get("blockers") or []:
        blockers.add(f"migration_blocker:{str(item)}")
    if not cutoff:
        blockers.add("cutoff_missing")
    elif int(cutoff.get("min_protocol") or 0) != 2 or cutoff.get("conflicted") is True:
        blockers.add("cutoff_invalid")
    cutoff_generation = int(cutoff.get("cutoff_generation") or 0)
    cleanup_acks = migration.get("cleanup_acks") if isinstance(migration.get("cleanup_acks"), Mapping) else {}
    scan_acks = migration.get("scan_acks") if isinstance(migration.get("scan_acks"), Mapping) else {}
    for node_id in active_node_ids:
        cleanup = cleanup_acks.get(node_id) if isinstance(cleanup_acks.get(node_id), Mapping) else {}
        if int(cleanup.get("cutoff_generation") or 0) != cutoff_generation:
            blockers.add(f"cleanup_ack:{node_id}")
        scan = scan_acks.get(node_id) if isinstance(scan_acks.get(node_id), Mapping) else {}
        if (
            int(scan.get("cutoff_generation") or 0) != cutoff_generation
            or scan.get("clean") is not True
            or scan.get("findings")
        ):
            blockers.add(f"scan_ack:{node_id}")

    secret_generations = {
        str(secret_id): int(secret.get("generation") or 0)
        for secret_id, secret in secrets.items()
        if isinstance(secret, Mapping)
    }
    recipient_generations = {
        str(secret_id): int(secret.get("recipient_generation") or 0)
        for secret_id, secret in secrets.items()
        if isinstance(secret, Mapping)
    }
    for secret_id, secret in secrets.items():
        if not isinstance(secret, Mapping):
            blockers.add(f"secret_invalid:{secret_id}")
            continue
        if secret.get("conflicted") is True or secret.get("recipient_conflicted") is True:
            blockers.add(f"secret_conflict:{secret_id}")
        intended = {
            str(node_id) for node_id in secret.get("recipient_ids") or []
        }
        if not intended:
            intended = active_master_ids if str(secret.get("audience") or "") == "masters" else active_node_ids
        for node_id in intended & active_node_ids:
            ack = materialization_acks.get(node_id) if isinstance(materialization_acks.get(node_id), Mapping) else {}
            if (
                int(ack.get("membership_generation") or -1) != membership_generation
                or int((ack.get("credential_generations") or {}).get(secret_id) or 0) < int(secret.get("generation") or 0)
                or int((ack.get("recipient_generations") or {}).get(secret_id) or 0) != int(secret.get("recipient_generation") or 0)
            ):
                blockers.add(f"materialization_ack:{node_id}:{secret_id}")
    for secret_id, tombstone in tombstones.items():
        if not isinstance(tombstone, Mapping) or tombstone.get("conflicted") is True:
            blockers.add(f"secret_tombstone_conflict:{secret_id}")

    expected_profiles = {
        str(provider): int(profile.get("activation_generation") or 0)
        for provider, profile in active_tradfi.items()
        if isinstance(profile, Mapping) and profile.get("conflicted") is not True
    }
    for node_id in active_master_ids:
        ack = projection_acks.get(node_id) if isinstance(projection_acks.get(node_id), Mapping) else {}
        if (
            int(ack.get("membership_generation") or -1) != membership_generation
            or str(ack.get("projection_status") or "") != "current"
            or dict(ack.get("active_profile_generations") or {}) != expected_profiles
        ):
            blockers.add(f"projection_ack:{node_id}")
    for provider, profile in active_tradfi.items():
        if not isinstance(profile, Mapping) or profile.get("conflicted") is True:
            blockers.add(f"tradfi_profile_conflict:{provider}")
            continue
        profile_id = profile.get("profile_id")
        if profile_id is not None:
            secret = secrets.get(str(profile_id))
            if not isinstance(secret, Mapping) or str(secret.get("secret_kind") or "") != "tradfi_profile":
                blockers.add(f"tradfi_profile_missing:{provider}")
    for key_id, entry in cmc_entries.items():
        if not isinstance(entry, Mapping) or entry.get("conflicted") is True or entry.get("state_conflicts"):
            blockers.add(f"cmc_entry_conflict:{key_id}")
            continue
        secret = secrets.get(str(entry.get("secret_id") or ""))
        if not isinstance(secret, Mapping) or str(secret.get("secret_kind") or "") != "cmc_api_key":
            blockers.add(f"cmc_secret_missing:{key_id}")
    for domain_id, authority in cmc_authorities.items():
        if (
            not isinstance(authority, Mapping)
            or authority.get("conflicted") is True
            or str(authority.get("authority_node_id") or "") not in active_node_ids
        ):
            blockers.add(f"cmc_authority_invalid:{domain_id}")
    conflicts = migration.get("conflicts") if isinstance(migration.get("conflicts"), Mapping) else {}
    for conflict_id, conflict in conflicts.items():
        if not isinstance(conflict, Mapping) or str(conflict.get("status") or "") != "resolved":
            blockers.add(f"migration_conflict:{conflict_id}")
    acceptances = migration.get("candidate_acceptances") if isinstance(migration.get("candidate_acceptances"), Mapping) else {}
    for candidate_id, acceptance in acceptances.items():
        if not isinstance(acceptance, Mapping):
            blockers.add(f"acceptance_invalid:{candidate_id}")
            continue
        secret = secrets.get(str(acceptance.get("credential_id") or ""))
        if not isinstance(secret, Mapping) or int(secret.get("generation") or 0) < int(acceptance.get("credential_generation") or 0):
            blockers.add(f"acceptance_target_missing:{candidate_id}")

    return {
        "schema_version": 1,
        "status": "sealed" if not blockers else "blocked",
        "cluster_id": str((cluster_nodes or {}).get("cluster_id") or ""),
        "membership_generation": membership_generation,
        "active_node_ids": sorted(active_node_ids),
        "active_master_ids": sorted(active_master_ids),
        "cutoff_generation": cutoff_generation,
        "cutoff_min_protocol": int(cutoff.get("min_protocol") or 0),
        "obsolete_secret_blob_hashes": sorted(str(item) for item in cutoff.get("obsolete_secret_blob_hashes") or []),
        "secret_generations": secret_generations,
        "recipient_generations": recipient_generations,
        "secret_tombstone_count": len(tombstones),
        "candidate_count": len(migration.get("candidates") or {}),
        "acceptance_count": len(acceptances),
        "cmc_entry_count": len(cmc_entries),
        "cmc_authority_count": len(cmc_authorities),
        "tradfi_profile_count": len(active_tradfi),
        "blockers": sorted(blockers),
    }


def retention_preview(
    cluster_root: Path | str,
    checkpoint: Mapping[str, Any] | None = None,
    *,
    now: int | None = None,
    item_limit: int | None = None,
) -> dict[str, Any]:
    """Return operations eligible by checkpoint and age without deleting them."""

    root = Path(os.path.abspath(Path(cluster_root).expanduser()))
    active = (
        verify_shadow_checkpoint(
            checkpoint,
            expected_cluster_id=str(read_local_identity(root)["cluster_id"]),
        )
        if checkpoint is not None
        else read_shadow_checkpoint(root)
    )
    if active is None:
        return {
            "status": "missing_checkpoint",
            "eligible_operations": 0,
            "eligible_bytes": 0,
            "retained_operations": 0,
            "retained_bytes": 0,
            "items": [],
            "items_truncated": False,
        }
    timestamp = int(time.time()) if now is None else _nonnegative_int(now, "now")
    cutoff = max(0, timestamp - int(active["history_seconds"]))
    paths = ClusterPaths.from_root(root)
    committed = read_active_checkpoint(root)
    safe_baseline = _normalized_vector(
        (committed or active).get("baseline_vector") or {}
    )
    candidates = _operation_prune_candidates(
        paths.oplog,
        safe_baseline,
        cutoff,
        require_receipt_age=False,
    )
    eligible_count = len(candidates)
    eligible_bytes = sum(int(item["size"]) for item in candidates)
    eligible = [
        {
            "actor": str(item["actor"]),
            "seq": int(item["seq"]),
            "op_id": str(item["op_id"]),
            "op": str(item["op"]),
            "created_at": int(item["created_at"]),
            "bytes": int(item["size"]),
        }
        for item in candidates[:None if item_limit is None else max(0, int(item_limit))]
    ]
    operation_paths = sorted(paths.oplog.glob("*/*.json")) if paths.oplog.exists() else []
    total_bytes = sum(int(path.stat().st_size) for path in operation_paths)
    retained_count = len(operation_paths) - eligible_count
    retained_bytes = total_bytes - eligible_bytes
    return {
        "status": "dry_run",
        "checkpoint_id": str(active["checkpoint_id"]),
        "history_seconds": int(active["history_seconds"]),
        "history_cutoff": cutoff,
        "eligible_operations": eligible_count,
        "eligible_bytes": eligible_bytes,
        "retained_operations": retained_count,
        "retained_bytes": retained_bytes,
        "blob_gc": _blob_gc_report_for_preview(root, active, timestamp),
        "items": eligible,
        "items_truncated": eligible_count > len(eligible),
    }


def prune_operation_history(
    cluster_root: Path | str,
    *,
    now: int | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Report or prune safely checkpointed operation history with crash recovery."""

    root = Path(os.path.abspath(Path(cluster_root).expanduser()))
    timestamp = int(time.time()) if now is None else _nonnegative_int(now, "now")
    paths = ClusterPaths.from_root(root)
    retention_root = ensure_private_directory(root / RETENTION_DIR_NAME)
    report_path = retention_root / RETENTION_REPORT_NAME
    journal_path = retention_root / PRUNE_JOURNAL_NAME
    with advisory_file_lock(root / ".append_sequence"):
        materialized = rebuild_materialized_state(root, write=False)
        policy = retention_policy(materialized)
        active = read_active_checkpoint(root)
        blockers: list[str] = []
        if policy.get("conflicted") is True:
            blockers.append("retention_policy_conflicted")
        if str(policy.get("mode") or "") == "report_only":
            blockers.append("retention_mode_report_only")
        if active is None:
            blockers.append("checkpoint_missing")
        else:
            if active["migration_seal"].get("status") != "sealed":
                blockers.append("checkpoint_migration_seal_blocked")
            if _normalized_vector(materialized.get("state_vector")) != _normalized_vector(active["baseline_vector"]):
                blockers.append("uncommitted_checkpoint_tail")
            if retention_policy(active["materialized"]) != policy:
                blockers.append("checkpoint_policy_stale")
            current_seal = build_migration_seal(materialized)
            if current_seal.get("status") != "sealed":
                blockers.extend(f"migration_seal:{item}" for item in current_seal.get("blockers") or [])
        try:
            from credential_migration import credential_migration_is_complete

            pbgdir = root.parent.parent if root.parent.name == "data" else root.parent
            if not credential_migration_is_complete(pbgdir):
                blockers.append("local_credential_migration_incomplete")
        except Exception as exc:
            blockers.append(f"local_credential_migration_check_failed:{type(exc).__name__}")

        safe_baseline: dict[str, int] = {}
        previous_checkpoint_id = ""
        if active is not None:
            commit = _read_checkpoint_commit_unlocked(root) or {}
            previous_checkpoint_id = str(commit.get("previous_checkpoint_id") or "")
            try:
                previous = _read_checkpoint_object(root, previous_checkpoint_id)
                safe_baseline = _normalized_vector(previous["baseline_vector"])
            except ClusterCheckpointError as exc:
                blockers.append(f"previous_checkpoint:{exc}")
        cutoff = timestamp - int(policy.get("history_days") or DEFAULT_HISTORY_DAYS) * 24 * 60 * 60
        candidates = _operation_prune_candidates(paths.oplog, safe_baseline, cutoff)
        candidate_digest = _sha256_json(candidates)
        report = {
            "schema_version": 1,
            "status": "blocked" if blockers else "ready",
            "mode": str(policy.get("mode") or "report_only"),
            "history_days": int(policy.get("history_days") or DEFAULT_HISTORY_DAYS),
            "evaluated_at": timestamp,
            "history_cutoff": cutoff,
            "checkpoint_id": str((active or {}).get("checkpoint_id") or ""),
            "previous_checkpoint_id": previous_checkpoint_id,
            "safe_baseline": safe_baseline,
            "candidate_digest": candidate_digest,
            "eligible_operations": len(candidates),
            "eligible_bytes": sum(int(item["size"]) for item in candidates),
            "blockers": sorted(set(blockers)),
            "dry_run": bool(dry_run or blockers),
            "deleted_operations": 0,
            "deleted_bytes": 0,
        }
        if blockers or dry_run or not candidates:
            atomic_write_private_text(report_path, json.dumps(report, indent=4, sort_keys=True) + "\n")
            return report

        journal = _read_json_if_exists(journal_path)
        if not isinstance(journal, dict) or journal.get("phase") == "complete":
            journal = {
                "schema_version": 1,
                "phase": "planned",
                "checkpoint_id": str(active["checkpoint_id"]),
                "previous_checkpoint_id": previous_checkpoint_id,
                "history_cutoff": cutoff,
                "safe_baseline": safe_baseline,
                "candidate_digest": candidate_digest,
                "candidates": candidates,
                "completed": [],
                "created_at": timestamp,
            }
            atomic_write_private_text(journal_path, json.dumps(journal, indent=4, sort_keys=True) + "\n")
        elif (
            journal.get("checkpoint_id") != active["checkpoint_id"]
            or journal.get("candidate_digest") != _sha256_json(journal.get("candidates") or [])
            or _normalized_vector(journal.get("safe_baseline") or {}) != safe_baseline
        ):
            raise ClusterCheckpointError("incomplete prune journal does not match active checkpoint")

        completed = {str(item) for item in journal.get("completed") or []}
        journal["phase"] = "pruning"
        atomic_write_private_text(journal_path, json.dumps(journal, indent=4, sort_keys=True) + "\n")
        deleted_bytes = 0
        for index, candidate in enumerate(journal["candidates"], start=1):
            relative = str(candidate["path"])
            if relative in completed:
                continue
            path = paths.root / relative
            if not path.is_relative_to(paths.oplog):
                raise ClusterCheckpointError("prune candidate escapes oplog root")
            if path.is_symlink():
                raise ClusterCheckpointError("prune candidate must not be a symlink")
            if path.exists():
                actor = str(candidate["actor"])
                seq = int(candidate["seq"])
                if seq > int(safe_baseline.get(actor, 0)):
                    raise ClusterCheckpointError("prune candidate exceeds safe baseline")
                path.unlink()
                deleted_bytes += int(candidate["size"])
            completed.add(relative)
            if index % 100 == 0:
                journal["completed"] = sorted(completed)
                atomic_write_private_text(journal_path, json.dumps(journal, indent=4, sort_keys=True) + "\n")
        journal["phase"] = "pruned"
        journal["completed"] = sorted(completed)
        atomic_write_private_text(journal_path, json.dumps(journal, indent=4, sort_keys=True) + "\n")
        for actor_dir in sorted((path for path in paths.oplog.iterdir() if path.is_dir()), reverse=True):
            try:
                actor_dir.rmdir()
            except OSError:
                pass
        reconstructed = rebuild_materialized_state(root, write=False)
        if reconstructed != active["materialized"]:
            raise ClusterCheckpointError("post-prune checkpoint reconstruction mismatch")
        quarantine_count, quarantine_bytes = _prune_old_oplog_quarantines(root, cutoff)
        checkpoint_count, checkpoint_bytes = _prune_old_checkpoint_objects(
            root,
            {str(active["checkpoint_id"]), previous_checkpoint_id},
        )
        journal["phase"] = "complete"
        journal["completed_at"] = timestamp
        atomic_write_private_text(journal_path, json.dumps(journal, indent=4, sort_keys=True) + "\n")
        report.update({
            "status": "complete",
            "dry_run": False,
            "deleted_operations": len(completed),
            "deleted_bytes": deleted_bytes,
            "deleted_quarantines": quarantine_count,
            "deleted_quarantine_bytes": quarantine_bytes,
            "deleted_checkpoint_objects": checkpoint_count,
            "deleted_checkpoint_bytes": checkpoint_bytes,
        })
        atomic_write_private_text(report_path, json.dumps(report, indent=4, sort_keys=True) + "\n")
        return report


def read_retention_report(cluster_root: Path | str) -> dict[str, Any] | None:
    """Read the latest owner-only retention report when available."""

    root = Path(os.path.abspath(Path(cluster_root).expanduser()))
    value = _read_json_if_exists(root / RETENTION_DIR_NAME / RETENTION_REPORT_NAME)
    return value if isinstance(value, dict) else None


def read_blob_gc_report(cluster_root: Path | str) -> dict[str, Any] | None:
    """Read the latest owner-only blob-GC report when available."""

    root = Path(os.path.abspath(Path(cluster_root).expanduser()))
    value = _read_json_if_exists(root / RETENTION_DIR_NAME / GC_REPORT_NAME)
    return value if isinstance(value, dict) else None


def replica_blob_hashes(cluster_root: Path | str) -> dict[str, list[str]]:
    """Return blob references required to reconstruct the local replica."""

    root = Path(os.path.abspath(Path(cluster_root).expanduser()))
    paths = ClusterPaths.from_root(root)
    active = read_active_checkpoint(root)
    checkpoints = [active] if active is not None else [read_shadow_checkpoint(root) or build_shadow_checkpoint(root)]
    commit = _read_checkpoint_commit_unlocked(root) or {}
    previous_id = str(commit.get("previous_checkpoint_id") or "")
    if previous_id and all(str(item["checkpoint_id"]) != previous_id for item in checkpoints):
        checkpoints.append(_read_checkpoint_object(root, previous_id))

    reachable: dict[str, set[str]] = {"config": set(), "secret": set(), "sealed": set()}
    config_manifest_refs: set[str] = set()
    obsolete_secret_hashes = {
        blob_hash
        for checkpoint in checkpoints
        for blob_hash in _sealed_obsolete_secret_hashes(checkpoint)
    }
    for checkpoint in checkpoints:
        refs = checkpoint.get("blob_refs") or {}
        config_manifest_refs.update(
            _collect_hash_field_refs(checkpoint.get("materialized"), "config_manifest_hash")
        )
        _merge_blob_refs(
            reachable,
            {
                "config": refs.get("config") or [],
                "secret": [blob_hash for blob_hash in refs.get("secret") or [] if blob_hash not in obsolete_secret_hashes],
                "sealed": refs.get("sealed") or [],
            },
        )
    for operation_path in sorted(paths.oplog.glob("*/*.json")) if paths.oplog.exists() else []:
        if operation_path.is_symlink():
            raise ClusterCheckpointError("retained operation must not be a symlink")
        try:
            operation = json.loads(operation_path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            continue
        except (OSError, json.JSONDecodeError) as exc:
            raise ClusterCheckpointError("retained operation is unreadable") from exc
        _merge_operation_blob_refs(reachable, operation, obsolete_secret_hashes)
        config_manifest_refs.update(_collect_hash_field_refs(operation, "config_manifest_hash"))
    _collect_mailbox_blob_refs(paths, reachable, config_manifest_refs=config_manifest_refs)
    _expand_available_config_manifest_refs(paths, config_manifest_refs, reachable["config"])
    return {kind: sorted(values) for kind, values in reachable.items()}


def _reachable_blob_report(reachable: Mapping[str, set[str]]) -> dict[str, Any]:
    """Return deterministic required-blob counts and a cross-node set digest."""

    normalized = {
        kind: sorted(str(blob_hash) for blob_hash in reachable.get(kind, set()))
        for kind in ("config", "secret", "sealed")
    }
    return {
        "reachable": {kind: len(values) for kind, values in normalized.items()},
        "reachable_blobs": sum(len(values) for values in normalized.values()),
        "reachable_digest": _sha256_json(normalized),
    }


def _blob_gc_report_for_preview(root: Path, shadow: Mapping[str, Any], timestamp: int) -> dict[str, Any]:
    """Return an actual or projected blob-GC report without advancing GC state."""

    empty = {
        "status": "not_evaluated",
        "checkpoint_id": "",
        "evaluated_at": 0,
        "eligible_blobs": 0,
        "eligible_bytes": 0,
        "deleted_blobs": 0,
        "deleted_bytes": 0,
        "reachable": {"config": 0, "secret": 0, "sealed": 0},
        "reachable_blobs": 0,
        "reachable_digest": "",
        "blockers": [],
        "dry_run": True,
        "source": "projected",
    }
    try:
        committed = read_active_checkpoint(root)
        report = _read_json_if_exists(root / RETENTION_DIR_NAME / GC_REPORT_NAME)
    except ClusterCheckpointError as exc:
        return {**empty, "status": "error", "blockers": [str(exc)]}
    current_mode = str((shadow.get("retention_policy") or {}).get("mode") or "report_only")
    committed_id = str((committed or {}).get("checkpoint_id") or "")
    shadow_vector = _normalized_vector(shadow.get("baseline_vector") or {})
    committed_vector = _normalized_vector((committed or {}).get("baseline_vector") or {})
    if (
        isinstance(report, Mapping)
        and committed_id
        and str(report.get("checkpoint_id") or "") == committed_id
        and str(report.get("mode") or "report_only") == current_mode
        and shadow_vector == committed_vector
    ):
        try:
            raw_reachable = report.get("reachable") or {}
            reachable_counts = {
                kind: max(0, int(raw_reachable.get(kind) or 0))
                for kind in ("config", "secret", "sealed")
            }
            reachable_digest = str(report.get("reachable_digest") or "")
            if reachable_digest and not _is_sha256(reachable_digest):
                raise ValueError("invalid reachable digest")
            return {
                "status": str(report.get("status") or "unknown"),
                "checkpoint_id": committed_id,
                "evaluated_at": max(0, int(report.get("evaluated_at") or 0)),
                "eligible_blobs": max(0, int(report.get("eligible_blobs") or 0)),
                "eligible_bytes": max(0, int(report.get("eligible_bytes") or 0)),
                "deleted_blobs": max(0, int(report.get("deleted_blobs") or 0)),
                "deleted_bytes": max(0, int(report.get("deleted_bytes") or 0)),
                "reachable": reachable_counts,
                "reachable_blobs": max(0, int(report.get("reachable_blobs") or sum(reachable_counts.values()))),
                "reachable_digest": reachable_digest,
                "blockers": sorted(str(item) for item in report.get("blockers") or []),
                "dry_run": bool(report.get("dry_run", True)),
                "source": "automatic",
            }
        except (TypeError, ValueError):
            return {**empty, "checkpoint_id": committed_id, "status": "error", "blockers": ["blob_gc_report_invalid"]}

    paths = ClusterPaths.from_root(root)
    previous = committed if committed is not None else shadow
    safe_baseline = committed_vector if committed is not None else shadow_vector
    cutoff = timestamp - int(shadow.get("history_seconds") or DEFAULT_HISTORY_SECONDS)
    blockers = ["blob_gc_projection_only"]
    if current_mode == "report_only":
        blockers.append("blob_gc_not_enabled")
    if committed is None:
        blockers.append("checkpoint_missing")
    try:
        pruned_paths = {
            str(item["path"])
            for item in _operation_prune_candidates(
                paths.oplog,
                safe_baseline,
                cutoff,
                require_receipt_age=False,
            )
        }
        reachable: dict[str, set[str]] = {"config": set(), "secret": set(), "sealed": set()}
        _merge_blob_refs(reachable, shadow.get("blob_refs") or {})
        _merge_blob_refs(reachable, previous.get("blob_refs") or {})
        obsolete_secret_hashes = _sealed_obsolete_secret_hashes(shadow)
        for operation_path in sorted(paths.oplog.glob("*/*.json")) if paths.oplog.exists() else []:
            if str(operation_path.relative_to(paths.root)) in pruned_paths:
                continue
            if operation_path.is_symlink():
                raise ClusterCheckpointError("retained operation must not be a symlink")
            operation = json.loads(operation_path.read_text(encoding="utf-8"))
            _merge_operation_blob_refs(reachable, operation, obsolete_secret_hashes)
        _collect_mailbox_blob_refs(paths, reachable)
        _expand_config_manifest_refs(paths, reachable["config"])
        _verify_reachable_blobs(paths, reachable)
        candidates = _blob_gc_candidates(paths, reachable, timestamp - BLOB_GC_MIN_AGE_SECONDS)
    except (OSError, json.JSONDecodeError, ClusterCheckpointError) as exc:
        return {
            **empty,
            "checkpoint_id": str(shadow.get("checkpoint_id") or ""),
            "status": "error",
            "blockers": [f"blob_projection_failed:{exc}"],
        }
    return {
        **empty,
        **_reachable_blob_report(reachable),
        "status": "projected",
        "checkpoint_id": str(shadow.get("checkpoint_id") or ""),
        "evaluated_at": timestamp,
        "eligible_blobs": len(candidates),
        "eligible_bytes": sum(int(item["size"]) for item in candidates),
        "blockers": sorted(blockers),
    }


def garbage_collect_blobs(
    cluster_root: Path | str,
    *,
    now: int | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Mark, report, and optionally sweep unreachable content-addressed blobs."""

    root = Path(os.path.abspath(Path(cluster_root).expanduser()))
    timestamp = int(time.time()) if now is None else _nonnegative_int(now, "now")
    paths = ClusterPaths.from_root(root)
    retention_root = ensure_private_directory(root / RETENTION_DIR_NAME)
    candidate_path = retention_root / GC_CANDIDATES_NAME
    journal_path = retention_root / GC_JOURNAL_NAME
    report_path = retention_root / GC_REPORT_NAME
    with advisory_file_lock(root / ".append_sequence"):
        materialized = rebuild_materialized_state(root, write=False)
        policy = retention_policy(materialized)
        active = read_active_checkpoint(root)
        blockers: list[str] = []
        if str(policy.get("mode") or "report_only") == "report_only":
            blockers.append("blob_gc_not_enabled")
        if policy.get("conflicted") is True:
            blockers.append("retention_policy_conflicted")
        prune_report = read_retention_report(root)
        if (
            not isinstance(prune_report, dict)
            or prune_report.get("status") != "complete"
            or active is None
            or prune_report.get("checkpoint_id") != active.get("checkpoint_id")
        ):
            blockers.append("operation_prune_not_complete")
        if active is None:
            blockers.append("checkpoint_missing")
            current = previous = None
        else:
            current = active
            commit = _read_checkpoint_commit_unlocked(root) or {}
            try:
                previous = _read_checkpoint_object(root, str(commit.get("previous_checkpoint_id") or ""))
            except ClusterCheckpointError as exc:
                previous = None
                blockers.append(f"previous_checkpoint:{exc}")
            if current["migration_seal"].get("status") != "sealed":
                blockers.append("checkpoint_migration_seal_blocked")
            if _normalized_vector(materialized.get("state_vector")) != _normalized_vector(current["baseline_vector"]):
                blockers.append("uncommitted_checkpoint_tail")

        reachable: dict[str, set[str]] = {"config": set(), "secret": set(), "sealed": set()}
        reachable_complete = True
        try:
            if current is not None:
                _merge_blob_refs(reachable, current.get("blob_refs") or {})
            if previous is not None:
                _merge_blob_refs(reachable, previous.get("blob_refs") or {})
            obsolete_secret_hashes = _sealed_obsolete_secret_hashes(current)
            for operation_path in sorted(paths.oplog.glob("*/*.json")) if paths.oplog.exists() else []:
                if operation_path.is_symlink():
                    raise ClusterCheckpointError("retained operation must not be a symlink")
                operation = json.loads(operation_path.read_text(encoding="utf-8"))
                _merge_operation_blob_refs(reachable, operation, obsolete_secret_hashes)
            _collect_mailbox_blob_refs(paths, reachable)
            _expand_config_manifest_refs(paths, reachable["config"])
            _verify_reachable_blobs(paths, reachable)
        except (OSError, json.JSONDecodeError, ClusterCheckpointError) as exc:
            reachable_complete = False
            blockers.append(f"blob_mark_failed:{exc}")

        candidates = _blob_gc_candidates(paths, reachable, timestamp - BLOB_GC_MIN_AGE_SECONDS)
        candidate_digest = _sha256_json(candidates)
        previous_candidates = _read_json_if_exists(candidate_path)
        candidate_state = {
            "schema_version": 1,
            "checkpoint_id": str((active or {}).get("checkpoint_id") or ""),
            "candidate_digest": candidate_digest,
            "first_observed_at": (
                int(previous_candidates.get("first_observed_at") or timestamp)
                if isinstance(previous_candidates, dict)
                and previous_candidates.get("candidate_digest") == candidate_digest
                and previous_candidates.get("checkpoint_id") == str((active or {}).get("checkpoint_id") or "")
                else timestamp
            ),
            "last_observed_at": timestamp,
            "candidates": candidates,
        }
        atomic_write_private_text(candidate_path, json.dumps(candidate_state, indent=4, sort_keys=True) + "\n")
        report = {
            "schema_version": 1,
            "status": "blocked" if blockers else "ready",
            "mode": str(policy.get("mode") or "report_only"),
            "evaluated_at": timestamp,
            "checkpoint_id": str((active or {}).get("checkpoint_id") or ""),
            **(
                _reachable_blob_report(reachable)
                if reachable_complete
                else {
                    "reachable": {"config": 0, "secret": 0, "sealed": 0},
                    "reachable_blobs": 0,
                    "reachable_digest": "",
                }
            ),
            "candidate_digest": candidate_digest,
            "eligible_blobs": len(candidates),
            "eligible_bytes": sum(int(item["size"]) for item in candidates),
            "blockers": sorted(set(blockers)),
            "dry_run": bool(dry_run or blockers),
            "deleted_blobs": 0,
            "deleted_bytes": 0,
        }
        if blockers or dry_run or not candidates:
            atomic_write_private_text(report_path, json.dumps(report, indent=4, sort_keys=True) + "\n")
            return report

        journal = _read_json_if_exists(journal_path)
        if not isinstance(journal, dict) or journal.get("phase") == "complete":
            journal = {
                "schema_version": 1,
                "phase": "planned",
                "checkpoint_id": str(active["checkpoint_id"]),
                "candidate_digest": candidate_digest,
                "candidates": candidates,
                "completed": [],
                "created_at": timestamp,
            }
            atomic_write_private_text(journal_path, json.dumps(journal, indent=4, sort_keys=True) + "\n")
        elif (
            journal.get("checkpoint_id") != active["checkpoint_id"]
            or journal.get("candidate_digest") != candidate_digest
            or journal.get("candidate_digest") != _sha256_json(journal.get("candidates") or [])
        ):
            raise ClusterCheckpointError("incomplete blob GC journal does not match stable report")
        completed = {str(item) for item in journal.get("completed") or []}
        deleted_bytes = 0
        journal["phase"] = "sweeping"
        atomic_write_private_text(journal_path, json.dumps(journal, indent=4, sort_keys=True) + "\n")
        for index, candidate in enumerate(journal["candidates"], start=1):
            relative = str(candidate["path"])
            if relative in completed:
                continue
            path = paths.root / relative
            expected_root = {
                "config": paths.config_blobs,
                "secret": paths.secret_blobs,
                "sealed": paths.sealed_blobs,
            }[str(candidate["kind"])]
            if not path.is_relative_to(expected_root) or path.is_symlink():
                raise ClusterCheckpointError("blob GC candidate path is invalid")
            if path.exists():
                raw = path.read_bytes()
                if hashlib.sha256(raw).hexdigest() != str(candidate["hash"]).removeprefix("sha256:"):
                    raise ClusterCheckpointError("blob GC candidate hash changed")
                path.unlink()
                deleted_bytes += int(candidate["size"])
            completed.add(relative)
            if index % 100 == 0:
                journal["completed"] = sorted(completed)
                atomic_write_private_text(journal_path, json.dumps(journal, indent=4, sort_keys=True) + "\n")
        journal["phase"] = "complete"
        journal["completed"] = sorted(completed)
        journal["completed_at"] = timestamp
        atomic_write_private_text(journal_path, json.dumps(journal, indent=4, sort_keys=True) + "\n")
        report.update({
            "status": "complete",
            "dry_run": False,
            "deleted_blobs": len(completed),
            "deleted_bytes": deleted_bytes,
        })
        atomic_write_private_text(report_path, json.dumps(report, indent=4, sort_keys=True) + "\n")
        return report


def _merge_blob_refs(target: dict[str, set[str]], refs: Mapping[str, Any]) -> None:
    """Merge normalized typed blob references into one mark set."""

    for kind in target:
        values = refs.get(kind) if isinstance(refs, Mapping) else None
        if isinstance(values, list):
            target[kind].update(str(item) for item in values if _is_sha256(str(item)))


def _sealed_obsolete_secret_hashes(checkpoint: Mapping[str, Any] | None) -> set[str]:
    """Return secret hashes authorized as obsolete by a sealed migration."""

    seal = checkpoint.get("migration_seal") if isinstance(checkpoint, Mapping) else None
    if not isinstance(seal, Mapping) or seal.get("status") != "sealed":
        return set()
    return {
        str(item)
        for item in seal.get("obsolete_secret_blob_hashes") or []
        if _is_sha256(str(item))
    }


def _merge_operation_blob_refs(
    target: dict[str, set[str]],
    operation: Mapping[str, Any],
    obsolete_secret_hashes: set[str],
) -> None:
    """Mark operation refs except secrets a sealed migration removed intentionally."""

    refs = _collect_direct_blob_refs(operation)
    refs["secret"] = [
        blob_hash
        for blob_hash in refs["secret"]
        if blob_hash not in obsolete_secret_hashes
    ]
    _merge_blob_refs(target, refs)


def _collect_mailbox_blob_refs(
    paths: ClusterPaths,
    reachable: dict[str, set[str]],
    *,
    config_manifest_refs: set[str] | None = None,
) -> None:
    """Collect explicit typed references from mailbox and durable provider state."""

    candidates: list[Path] = []
    if paths.mailbox.exists():
        candidates.extend(sorted(paths.mailbox.glob("*.json")))
        candidates.extend(sorted(paths.mailbox.glob("*/*.json")))
    candidates.append(paths.mailbox / "cmc_provider_state.json")
    for path in candidates:
        if not path.exists():
            continue
        if path.is_symlink():
            raise ClusterCheckpointError("mailbox state must not be a symlink")
        value = json.loads(path.read_text(encoding="utf-8"))
        _merge_blob_refs(reachable, _collect_direct_blob_refs(value))
        if config_manifest_refs is not None:
            config_manifest_refs.update(_collect_hash_field_refs(value, "config_manifest_hash"))


def _expand_config_manifest_refs(paths: ClusterPaths, config_refs: set[str]) -> None:
    """Expand every marked config manifest and validate all referenced children."""

    queue = list(sorted(config_refs))
    expanded: set[str] = set()
    while queue:
        blob_hash = queue.pop(0)
        if blob_hash in expanded:
            continue
        path = _content_addressed_blob_path(paths.config_blobs, blob_hash)
        raw = _read_verified_blob(path, blob_hash)
        try:
            manifest = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            expanded.add(blob_hash)
            continue
        files = manifest.get("files") if isinstance(manifest, dict) else None
        if files is None:
            expanded.add(blob_hash)
            continue
        if not isinstance(files, dict):
            raise ClusterCheckpointError("config manifest files must be an object")
        for name, record in files.items():
            if not isinstance(name, str) or not name or "/" in name or "\\" in name or name in {".", ".."}:
                raise ClusterCheckpointError("config manifest contains an invalid filename")
            if isinstance(record, str):
                child_hash = record
                expected_size = None
            elif isinstance(record, dict):
                child_hash = str(record.get("hash") or record.get("sha256") or "")
                expected_size = record.get("size")
            else:
                raise ClusterCheckpointError("config manifest file record is invalid")
            if len(child_hash) == 64 and all(character in "0123456789abcdef" for character in child_hash):
                child_hash = f"sha256:{child_hash}"
            if not _is_sha256(child_hash):
                raise ClusterCheckpointError("config manifest contains an invalid blob hash")
            child_raw = _read_verified_blob(_content_addressed_blob_path(paths.config_blobs, child_hash), child_hash)
            if expected_size is not None and int(expected_size) != len(child_raw):
                raise ClusterCheckpointError("config manifest child size mismatch")
            if child_hash not in config_refs:
                config_refs.add(child_hash)
        expanded.add(blob_hash)


def _expand_available_config_manifest_refs(
    paths: ClusterPaths,
    manifest_refs: set[str],
    config_refs: set[str],
) -> None:
    """Expand valid local manifests without requiring every referenced blob locally."""

    queue = list(sorted(manifest_refs))
    expanded: set[str] = set()
    while queue:
        blob_hash = queue.pop(0)
        if blob_hash in expanded:
            continue
        path = _content_addressed_blob_path(paths.config_blobs, blob_hash)
        if path.is_symlink() or not path.is_file():
            expanded.add(blob_hash)
            continue
        try:
            raw = _read_verified_blob(path, blob_hash)
        except ClusterCheckpointError:
            expanded.add(blob_hash)
            continue
        try:
            manifest = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            expanded.add(blob_hash)
            continue
        files = manifest.get("files") if isinstance(manifest, dict) else None
        if files is None:
            expanded.add(blob_hash)
            continue
        if not isinstance(files, dict):
            raise ClusterCheckpointError("config manifest files must be an object")
        for name, record in files.items():
            if not isinstance(name, str) or not name or "/" in name or "\\" in name or name in {".", ".."}:
                raise ClusterCheckpointError("config manifest contains an invalid filename")
            if isinstance(record, str):
                child_hash = record
            elif isinstance(record, dict):
                child_hash = str(record.get("hash") or record.get("sha256") or "")
            else:
                raise ClusterCheckpointError("config manifest file record is invalid")
            if len(child_hash) == 64 and all(character in "0123456789abcdef" for character in child_hash):
                child_hash = f"sha256:{child_hash}"
            if not _is_sha256(child_hash):
                raise ClusterCheckpointError("config manifest contains an invalid blob hash")
            if child_hash not in config_refs:
                config_refs.add(child_hash)
        expanded.add(blob_hash)


def _verify_reachable_blobs(paths: ClusterPaths, reachable: Mapping[str, set[str]]) -> None:
    """Fail closed when any marked blob is missing or hash-invalid."""

    roots = {
        "config": paths.config_blobs,
        "secret": paths.secret_blobs,
        "sealed": paths.sealed_blobs,
    }
    for kind, hashes in reachable.items():
        for blob_hash in hashes:
            _read_verified_blob(_content_addressed_blob_path(roots[kind], blob_hash), blob_hash)


def _blob_gc_candidates(
    paths: ClusterPaths,
    reachable: Mapping[str, set[str]],
    minimum_mtime: int,
) -> list[dict[str, Any]]:
    """List old hash-valid blob files outside the complete mark set."""

    result: list[dict[str, Any]] = []
    roots = {
        "config": paths.config_blobs,
        "secret": paths.secret_blobs,
        "sealed": paths.sealed_blobs,
    }
    for kind, root in roots.items():
        if root.is_symlink():
            raise ClusterCheckpointError("blob root must not be a symlink")
        if not root.exists():
            continue
        for path in sorted(root.glob("sha256/*/*.json")):
            if path.is_symlink():
                raise ClusterCheckpointError("blob file must not be a symlink")
            stat = path.stat()
            digest = path.stem
            blob_hash = f"sha256:{digest}"
            if not _is_sha256(blob_hash):
                raise ClusterCheckpointError("blob store contains an invalid filename")
            raw = path.read_bytes()
            if hashlib.sha256(raw).hexdigest() != digest:
                raise ClusterCheckpointError("blob store contains a hash mismatch")
            if blob_hash not in reachable[kind] and int(stat.st_mtime) < minimum_mtime:
                result.append({
                    "kind": kind,
                    "hash": blob_hash,
                    "size": int(stat.st_size),
                    "mtime": int(stat.st_mtime),
                    "path": str(path.relative_to(paths.root)),
                })
    return result


def _content_addressed_blob_path(root: Path, blob_hash: str) -> Path:
    """Resolve one validated content-addressed JSON blob path."""

    if not _is_sha256(blob_hash):
        raise ClusterCheckpointError("invalid blob hash")
    digest = str(blob_hash).removeprefix("sha256:")
    return root / "sha256" / digest[:2] / f"{digest}.json"


def _read_verified_blob(path: Path, blob_hash: str) -> bytes:
    """Read one regular blob and verify its content address."""

    if path.is_symlink() or not path.is_file():
        raise ClusterCheckpointError(f"reachable blob is missing: {blob_hash}")
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != str(blob_hash).removeprefix("sha256:"):
        raise ClusterCheckpointError(f"reachable blob hash mismatch: {blob_hash}")
    return raw


def _operation_prune_candidates(
    oplog: Path,
    safe_baseline: Mapping[str, int],
    cutoff: int,
    *,
    require_receipt_age: bool = True,
) -> list[dict[str, Any]]:
    """Return deterministic candidates old by signed and optional receipt time."""

    result: list[dict[str, Any]] = []
    if not oplog.exists():
        return result
    for actor_dir in sorted(path for path in oplog.iterdir() if path.is_dir()):
        actor = str(actor_dir.name)
        actor_baseline = int(safe_baseline.get(actor, 0))
        for path in sorted(actor_dir.glob("*.json")):
            if path.is_symlink():
                raise ClusterCheckpointError("oplog operation must not be a symlink")
            try:
                seq = int(path.stem)
                stat = path.stat()
                operation = json.loads(path.read_text(encoding="utf-8"))
            except (ValueError, OSError, json.JSONDecodeError) as exc:
                raise ClusterCheckpointError("oplog operation is unreadable during retention scan") from exc
            created_at = int(operation.get("created_at") or 0) if isinstance(operation, dict) else 0
            receipt_is_old = int(stat.st_mtime) < cutoff
            if seq <= actor_baseline and created_at < cutoff and (receipt_is_old or not require_receipt_age):
                result.append({
                    "actor": actor,
                    "seq": seq,
                    "op_id": str(operation.get("op_id") or ""),
                    "op": str(operation.get("op") or ""),
                    "created_at": created_at,
                    "first_seen_at": int(stat.st_mtime),
                    "size": int(stat.st_size),
                    "path": str(path.relative_to(oplog.parent)),
                })
    return result


def _prune_old_oplog_quarantines(root: Path, cutoff: int) -> tuple[int, int]:
    """Remove old rebootstrap quarantines only after checkpoint reconstruction succeeds."""

    quarantine_root = root / "oplog_quarantine"
    if quarantine_root.is_symlink():
        raise ClusterCheckpointError("oplog quarantine root must not be a symlink")
    if not quarantine_root.exists():
        return 0, 0
    removed = 0
    removed_bytes = 0
    for directory in sorted(path for path in quarantine_root.iterdir() if path.is_dir()):
        if directory.is_symlink() or int(directory.stat().st_mtime) >= cutoff:
            continue
        size = 0
        for path in directory.rglob("*"):
            if path.is_symlink():
                raise ClusterCheckpointError("oplog quarantine must not contain symlinks")
            if path.is_file():
                size += int(path.stat().st_size)
        shutil.rmtree(directory)
        removed += 1
        removed_bytes += size
    return removed, removed_bytes


def _prune_old_checkpoint_objects(
    root: Path,
    keep_checkpoint_ids: set[str],
) -> tuple[int, int]:
    """Retain only current and previous immutable checkpoint object copies."""

    objects = root / CHECKPOINT_DIR_NAME / CHECKPOINT_OBJECTS_DIR_NAME
    if objects.is_symlink():
        raise ClusterCheckpointError("checkpoint objects root must not be a symlink")
    if not objects.exists():
        return 0, 0
    keep_digests = {
        checkpoint_id.removeprefix("sha256:")
        for checkpoint_id in keep_checkpoint_ids
        if _is_sha256(checkpoint_id)
    }
    removed = 0
    removed_bytes = 0
    for path in sorted(objects.glob("*.json")):
        if path.is_symlink():
            raise ClusterCheckpointError("checkpoint object must not be a symlink")
        digest = path.name.split(".", 1)[0]
        if digest in keep_digests:
            continue
        removed_bytes += int(path.stat().st_size)
        path.unlink()
        removed += 1
    return removed, removed_bytes


def _read_json_if_exists(path: Path) -> Any:
    """Read an owner-only JSON file if it exists."""

    if path.is_symlink():
        raise ClusterCheckpointError("retention state must not be a symlink")
    if not path.exists():
        return None
    secure_private_file(path)
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ClusterCheckpointError("retention state is unreadable") from exc


def _read_checkpoint_object(root: Path, checkpoint_id: str) -> dict[str, Any]:
    """Read and fully verify one committed checkpoint object."""

    path = _checkpoint_object_path(root, checkpoint_id)
    backup = path.with_name(f"{path.stem}.backup.json")
    last_error: Exception | None = None
    for candidate in (path, backup):
        if candidate.is_symlink() or not candidate.is_file():
            continue
        secure_private_file(candidate)
        try:
            payload = json.loads(candidate.read_text(encoding="utf-8"))
            checkpoint = verify_shadow_checkpoint(
                payload,
                expected_cluster_id=str(read_local_identity(root)["cluster_id"]),
            )
            if str(checkpoint.get("checkpoint_id") or "") != str(checkpoint_id):
                raise ClusterCheckpointError("checkpoint object ID mismatch")
            return checkpoint
        except (OSError, json.JSONDecodeError, ClusterCheckpointError) as exc:
            last_error = exc
    raise ClusterCheckpointError("checkpoint object and backup are unavailable") from last_error


def checkpoint_materialized_state(checkpoint: Mapping[str, Any]) -> dict[str, Any]:
    """Return a detached verified materialized state for equivalence checks."""

    return deepcopy(verify_shadow_checkpoint(checkpoint)["materialized"])


def materialize_checkpoint_tail(
    checkpoint: Mapping[str, Any],
    tail_operations: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    """Materialize validated post-baseline operations on one checkpoint seed."""

    active = verify_shadow_checkpoint(checkpoint)
    base = deepcopy(active["materialized"])
    baseline = _normalized_vector(active["baseline_vector"])
    vector = dict(baseline)
    operations = sorted(
        (_json_copy(item) for item in tail_operations),
        key=_operation_order,
    )
    trust = _deserialize_membership_trust(active["membership_trust"])
    nodes = trust.nodes
    cluster_id = str(active["cluster_id"])
    cluster_nodes = base["cluster_nodes"]
    desired = base["desired_state"]
    instances = deepcopy(desired.get("instances") or {})
    tombstones = deepcopy(desired.get("tombstones") or {})
    parent_changes: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for instance, record in instances.items():
        for conflict in record.get("conflicts") or []:
            parent = str(conflict.get("parent_version") or "")
            if parent:
                parent_changes.setdefault((str(instance), parent), []).append(dict(conflict))
    v2_tail: list[dict[str, Any]] = []
    policy_tail: list[dict[str, Any]] = []
    membership_changes = 0
    generated_at = int(desired.get("generated_at") or 0)
    api_keys = deepcopy(desired.get("api_keys")) if isinstance(desired.get("api_keys"), dict) else None

    for operation in operations:
        actor = str(operation.get("actor") or "")
        seq = int(operation.get("seq") or 0)
        expected = int(vector.get(actor, 0)) + 1
        if seq != expected:
            raise ClusterCheckpointError(
                f"checkpoint tail sequence gap for actor {actor}: expected {expected}, got {seq}"
            )
        if str(operation.get("cluster_id") or "") != cluster_id:
            raise ClusterCheckpointError("checkpoint tail belongs to another cluster")
        if str(operation.get("base_checkpoint_id") or "") != str(active["checkpoint_id"]):
            raise ClusterCheckpointError("checkpoint tail belongs to a stale checkpoint branch")
        before_membership = _credential_membership_fingerprint(nodes)
        try:
            validate_operation(
                operation,
                expected_cluster_id=cluster_id,
                membership_trust=trust,
                network_input=True,
                allow_legacy_membership=True,
            )
        except Exception as exc:
            raise ClusterCheckpointError(f"checkpoint tail operation is invalid: {exc}") from exc
        if str(operation["op"]) in MEMBERSHIP_OPS:
            if _credential_membership_fingerprint(nodes) != before_membership:
                membership_changes += 1
        elif str(operation["op"]) in V7_OPS:
            _apply_v7(instances, tombstones, parent_changes, operation)
        elif str(operation["op"]) in API_KEY_OPS:
            api_keys = {
                "serial": int(operation["api_serial"]),
                "payload_hash": str(operation["payload_hash"]),
                "secret_blob_hash": str(operation["secret_blob_hash"]),
                "updated_by": actor,
                "updated_at": int(operation["created_at"]),
                **({"sanitized": True} if operation.get("sanitized") is True else {}),
            }
        elif str(operation["op"]) in V2_CREDENTIAL_OPS:
            v2_tail.append(operation)
        elif str(operation["op"]) in CLUSTER_POLICY_OPS:
            policy_tail.append(operation)
        vector[actor] = seq
        generated_at = max(generated_at, int(operation.get("created_at") or 0))

    _mark_conflicts(instances, parent_changes)
    reducer = active["reducer_state"]
    v2_operations = [
        *(_json_copy(item) for item in reducer.get("v2_basis_operations") or []),
        *v2_tail,
    ]
    policy_operations = [
        *(_json_copy(item) for item in reducer.get("retention_policy_basis_operations") or []),
        *policy_tail,
    ]
    v2_state = _materialize_v2_credentials(v2_operations)
    desired.update(v2_state)
    desired["instances"] = {key: instances[key] for key in sorted(instances)}
    desired["tombstones"] = {key: tombstones[key] for key in sorted(tombstones)}
    desired["generated_at"] = generated_at
    if api_keys is None:
        desired.pop("api_keys", None)
    else:
        desired["api_keys"] = api_keys
    if policy_operations:
        desired["retention_policy"] = _materialize_retention_policy(policy_operations)
    else:
        desired.pop("retention_policy", None)
    cluster_nodes["nodes"] = {key: nodes[key] for key in sorted(nodes)}
    cluster_nodes["generation"] = int(cluster_nodes.get("generation") or 0) + len(operations)
    cluster_nodes["credential_membership_generation"] = (
        int(cluster_nodes.get("credential_membership_generation") or 0) + membership_changes
    )
    return {
        "cluster_nodes": cluster_nodes,
        "desired_state": desired,
        "state_vector": {key: vector[key] for key in sorted(vector)},
    }


def _build_reducer_state(
    operations: list[dict[str, Any]],
    materialized: Mapping[str, Any],
) -> dict[str, Any]:
    """Build the minimal verified operation basis required by tail reducers."""

    desired = materialized.get("desired_state") if isinstance(materialized, Mapping) else {}
    desired = desired if isinstance(desired, Mapping) else {}
    expected_v2 = {
        key: deepcopy(desired.get(key) or {})
        for key in (
            "secrets",
            "secret_tombstones",
            "cmc_pool",
            "tradfi_active_profiles",
            "credential_migration",
            "credential_materialization_acks",
            "tradfi_projection_acks",
        )
    }
    referenced_op_ids = _collect_op_ids(expected_v2)
    selected: dict[str, dict[str, Any]] = {
        str(operation["op_id"]): operation
        for operation in operations
        if str(operation.get("op") or "") in V2_CREDENTIAL_OPS
        and str(operation.get("op_id") or "") in referenced_op_ids
    }
    latest_freeze = _latest_operation(
        (item for item in operations if str(item.get("op") or "") == "WRITER_FREEZE"),
        key=lambda item: (int(item.get("freeze_generation") or 0), str(item["op_id"])),
    )
    if latest_freeze is not None:
        selected[str(latest_freeze["op_id"])] = latest_freeze
    latest_states: dict[str, dict[str, Any]] = {}
    for operation in operations:
        if str(operation.get("op") or "") != "SET_CMC_KEY_STATE":
            continue
        key_id = str(operation.get("key_id") or "")
        current = latest_states.get(key_id)
        if current is None or (
            int(operation.get("state_generation", operation.get("generation")) or 0),
            str(operation["op_id"]),
        ) > (
            int(current.get("state_generation", current.get("generation")) or 0),
            str(current["op_id"]),
        ):
            latest_states[key_id] = operation
    for operation in latest_states.values():
        selected[str(operation["op_id"])] = operation

    v2_basis = sorted((_json_copy(item) for item in selected.values()), key=_operation_order)
    if _materialize_v2_credentials(v2_basis) != expected_v2:
        raise ClusterCheckpointError("compact v2 reducer basis does not match full replay")
    policy = desired.get("retention_policy") if isinstance(desired.get("retention_policy"), Mapping) else None
    policy_ids = _collect_op_ids(policy or {})
    policy_basis = sorted(
        (
            _json_copy(operation)
            for operation in operations
            if str(operation.get("op") or "") in CLUSTER_POLICY_OPS
            and str(operation.get("op_id") or "") in policy_ids
        ),
        key=_operation_order,
    )
    if policy is not None and (
        not policy_basis or _materialize_retention_policy(policy_basis) != policy
    ):
        raise ClusterCheckpointError("compact retention policy basis does not match full replay")
    return {
        "schema_version": 1,
        "v2_basis_operations": v2_basis,
        "retention_policy_basis_operations": policy_basis,
    }


def _verify_reducer_state(
    reducer_state: Mapping[str, Any],
    materialized: Mapping[str, Any],
) -> None:
    """Verify a compact reducer basis reproduces the checkpoint state exactly."""

    if set(reducer_state) != {
        "schema_version",
        "v2_basis_operations",
        "retention_policy_basis_operations",
    } or reducer_state.get("schema_version") != 1:
        raise ClusterCheckpointError("checkpoint reducer state has invalid fields")
    v2_basis = reducer_state.get("v2_basis_operations")
    policy_basis = reducer_state.get("retention_policy_basis_operations")
    if not isinstance(v2_basis, list) or not isinstance(policy_basis, list):
        raise ClusterCheckpointError("checkpoint reducer basis must be a list")
    desired = materialized.get("desired_state") if isinstance(materialized, Mapping) else {}
    desired = desired if isinstance(desired, Mapping) else {}
    expected_v2 = {
        key: deepcopy(desired.get(key) or {})
        for key in (
            "secrets",
            "secret_tombstones",
            "cmc_pool",
            "tradfi_active_profiles",
            "credential_migration",
            "credential_materialization_acks",
            "tradfi_projection_acks",
        )
    }
    if _materialize_v2_credentials([_json_copy(item) for item in v2_basis]) != expected_v2:
        raise ClusterCheckpointError("checkpoint v2 reducer basis mismatch")
    expected_policy = desired.get("retention_policy") if isinstance(desired.get("retention_policy"), Mapping) else None
    actual_policy = _materialize_retention_policy([_json_copy(item) for item in policy_basis]) if policy_basis else None
    if actual_policy != expected_policy:
        raise ClusterCheckpointError("checkpoint retention policy basis mismatch")


def _deserialize_membership_trust(value: Mapping[str, Any]) -> MembershipTrust:
    """Restore checkpoint-covered public membership trust for tail validation."""

    return MembershipTrust(
        nodes=_json_copy(value.get("nodes") or {}),
        removed_node_ids={str(item) for item in value.get("removed_node_ids") or []},
        signing_keys=_json_copy(value.get("signing_keys") or {}),
        role_history=_json_copy(value.get("role_history") or {}),
        validated_op_ids={str(item) for item in value.get("validated_op_ids") or []},
    )


def _collect_op_ids(value: Any) -> set[str]:
    """Collect operation IDs from one secret-free materialized subtree."""

    result: set[str] = set()
    if isinstance(value, Mapping):
        for key, item in value.items():
            if str(key) == "op_id" and isinstance(item, str) and item:
                result.add(item)
            else:
                result.update(_collect_op_ids(item))
    elif isinstance(value, list):
        for item in value:
            result.update(_collect_op_ids(item))
    return result


def _latest_operation(
    operations: Iterable[dict[str, Any]],
    *,
    key,
) -> dict[str, Any] | None:
    """Return the greatest operation by one deterministic key."""

    values = list(operations)
    return max(values, key=key) if values else None


def _operation_order(operation: Mapping[str, Any]) -> tuple[int, str, int, str]:
    """Return the canonical cluster operation replay order."""

    return (
        int(operation.get("created_at") or 0),
        str(operation.get("actor") or ""),
        int(operation.get("seq") or 0),
        str(operation.get("op_id") or ""),
    )


def _serialize_membership_trust(trust: MembershipTrust) -> dict[str, Any]:
    """Serialize the authenticated trust anchor without private key material."""

    return {
        "nodes": _normalized_nodes(trust.nodes),
        "removed_node_ids": sorted(str(item) for item in trust.removed_node_ids),
        "signing_keys": {
            str(node_id): sorted(
                (_json_copy(item) for item in records),
                key=lambda item: (
                    int(item.get("valid_from_seq") or 0),
                    str(item.get("signing_key_id") or ""),
                ),
            )
            for node_id, records in sorted(trust.signing_keys.items())
        },
        "role_history": {
            str(node_id): sorted(
                (_json_copy(item) for item in records),
                key=lambda item: (
                    int(item.get("role_epoch") or 0),
                    str(item.get("membership_op_id") or ""),
                ),
            )
            for node_id, records in sorted(trust.role_history.items())
        },
        "validated_op_ids": sorted(str(item) for item in trust.validated_op_ids),
    }


def _complete_operation_vector(operations: Iterable[Mapping[str, Any]]) -> dict[str, int]:
    """Return complete actor maxima and reject sequence gaps before checkpointing."""

    sequences: dict[str, set[int]] = {}
    for operation in operations:
        actor = str(operation.get("actor") or "")
        seq = int(operation.get("seq") or 0)
        if not actor or seq < 1:
            raise ClusterCheckpointError("operation has invalid actor sequence")
        sequences.setdefault(actor, set()).add(seq)
    vector: dict[str, int] = {}
    for actor, values in sorted(sequences.items()):
        maximum = max(values)
        if values != set(range(1, maximum + 1)):
            raise ClusterCheckpointError(f"operation sequence gap prevents checkpoint for actor {actor}")
        vector[actor] = maximum
    return vector


def _operation_file_vector(
    oplog: Path,
    *,
    baseline: Mapping[str, int] | None = None,
) -> dict[str, int]:
    """Read a cheap filename-only vector used to detect checkpoint build races."""

    base = {str(actor): int(seq) for actor, seq in (baseline or {}).items()}
    if not oplog.exists():
        return base
    vector: dict[str, int] = dict(base)
    for actor_dir in sorted(path for path in oplog.iterdir() if path.is_dir()):
        actor = str(actor_dir.name)
        actor_baseline = int(base.get(actor, 0))
        values: set[int] = set()
        for path in actor_dir.glob("*.json"):
            try:
                values.add(int(path.stem))
            except ValueError as exc:
                raise ClusterCheckpointError("oplog contains an invalid operation filename") from exc
        if not values:
            continue
        maximum = max(values)
        tail_values = {value for value in values if value > actor_baseline}
        if tail_values and tail_values != set(range(actor_baseline + 1, maximum + 1)):
            raise ClusterCheckpointError(f"operation sequence gap prevents checkpoint for actor {actor_dir.name}")
        vector[actor] = max(actor_baseline, maximum)
    return vector


def _tail_vector(
    baseline: Mapping[str, int],
    operations: Iterable[Mapping[str, Any]],
) -> dict[str, int]:
    """Advance one baseline through a complete actor-contiguous tail."""

    vector = {str(actor): int(seq) for actor, seq in baseline.items()}
    for operation in sorted(operations, key=lambda item: (str(item.get("actor") or ""), int(item.get("seq") or 0))):
        actor = str(operation.get("actor") or "")
        seq = int(operation.get("seq") or 0)
        if seq != int(vector.get(actor, 0)) + 1:
            raise ClusterCheckpointError(f"checkpoint tail sequence gap for actor {actor}")
        vector[actor] = seq
    return {key: vector[key] for key in sorted(vector)}


def _membership_trust_after_tail(
    checkpoint: Mapping[str, Any],
    operations: Iterable[Mapping[str, Any]],
) -> MembershipTrust:
    """Advance checkpoint membership trust through signed membership tail ops."""

    trust = _deserialize_membership_trust(checkpoint["membership_trust"])
    cluster_id = str(checkpoint["cluster_id"])
    for operation in sorted(operations, key=_operation_order):
        if str(operation.get("op") or "") not in MEMBERSHIP_OPS:
            continue
        try:
            validate_operation(
                dict(operation),
                expected_cluster_id=cluster_id,
                membership_trust=trust,
                network_input=True,
                allow_legacy_membership=True,
            )
        except Exception as exc:
            raise ClusterCheckpointError(f"checkpoint membership tail is invalid: {exc}") from exc
    return trust


def _collect_direct_blob_refs(desired_state: Any) -> dict[str, list[str]]:
    """Collect deterministic direct blob references from secret-free desired state."""

    refs: dict[str, set[str]] = {"config": set(), "secret": set(), "sealed": set()}
    field_kinds = {
        "config_manifest_hash": "config",
        "payload_hash": "config",
        "secret_blob_hash": "secret",
        "sealed_blob_hash": "sealed",
    }

    def visit(value: Any) -> None:
        if isinstance(value, Mapping):
            for key, item in value.items():
                kind = field_kinds.get(str(key))
                if kind and isinstance(item, str) and _is_sha256(item):
                    refs[kind].add(item)
                visit(item)
        elif isinstance(value, list):
            for item in value:
                visit(item)

    visit(desired_state)
    return {kind: sorted(values) for kind, values in refs.items()}


def _collect_hash_field_refs(value: Any, field: str) -> set[str]:
    """Collect valid content-addressed hashes stored under one exact field name."""

    refs: set[str] = set()

    def visit(item: Any) -> None:
        if isinstance(item, Mapping):
            for key, child in item.items():
                if str(key) == field and isinstance(child, str) and _is_sha256(child):
                    refs.add(child)
                visit(child)
        elif isinstance(item, list):
            for child in item:
                visit(child)

    visit(value)
    return refs


def _normalized_blob_refs(value: Any) -> dict[str, list[str]]:
    """Validate and normalize the checkpoint direct-blob reference map."""

    if not isinstance(value, Mapping):
        raise ClusterCheckpointError("checkpoint blob_refs must be an object")
    result: dict[str, list[str]] = {}
    for kind in ("config", "secret", "sealed"):
        items = value.get(kind)
        if not isinstance(items, list) or any(not isinstance(item, str) or not _is_sha256(item) for item in items):
            raise ClusterCheckpointError(f"checkpoint {kind} blob references are invalid")
        if items != sorted(set(items)):
            raise ClusterCheckpointError(f"checkpoint {kind} blob references must be sorted and unique")
        result[kind] = list(items)
    if set(value) != set(result):
        raise ClusterCheckpointError("checkpoint blob_refs contains unknown fields")
    return result


def _normalized_vector(value: Any) -> dict[str, int]:
    """Validate and normalize a positive actor state vector."""

    if not isinstance(value, Mapping):
        raise ClusterCheckpointError("checkpoint state vector must be an object")
    result: dict[str, int] = {}
    for actor, sequence in sorted(value.items()):
        parsed = _positive_int(sequence, "state vector sequence")
        result[str(actor)] = parsed
    return result


def _normalized_nodes(value: Any) -> dict[str, Any]:
    """Return deterministic detached membership nodes."""

    if not isinstance(value, Mapping):
        raise ClusterCheckpointError("checkpoint membership nodes must be an object")
    return {str(node_id): _json_copy(node) for node_id, node in sorted(value.items())}


def _sha256_json(value: Any) -> str:
    """Hash canonical JSON using a tagged SHA-256 value."""

    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _json_copy(value: Any) -> Any:
    """Detach and reject values that cannot be represented as canonical JSON."""

    try:
        return json.loads(json.dumps(value, sort_keys=True, ensure_ascii=True))
    except (TypeError, ValueError) as exc:
        raise ClusterCheckpointError("checkpoint contains a non-JSON value") from exc


def _is_sha256(value: str) -> bool:
    """Return whether *value* is one lowercase tagged SHA-256 hash."""

    text = str(value)
    digest = text.removeprefix("sha256:") if text.startswith("sha256:") else ""
    return len(digest) == 64 and all(character in "0123456789abcdef" for character in digest)


def _positive_int(value: Any, field: str) -> int:
    """Parse a strict positive integer."""

    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise ClusterCheckpointError(f"{field} must be a positive integer")
    return value


def _nonnegative_int(value: Any, field: str) -> int:
    """Parse a strict nonnegative integer."""

    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ClusterCheckpointError(f"{field} must be a nonnegative integer")
    return value


__all__ = [
    "CHECKPOINT_SCHEMA_VERSION",
    "DEFAULT_HISTORY_SECONDS",
    "ClusterCheckpointError",
    "activate_checkpoint",
    "active_checkpoint_baseline",
    "active_checkpoint_bundle",
    "build_migration_seal",
    "build_shadow_checkpoint",
    "checkpoint_materialized_state",
    "checkpoint_status",
    "create_checkpoint_ack",
    "create_checkpoint_commit_proof",
    "create_checkpoint_proposal",
    "create_shadow_checkpoint",
    "garbage_collect_blobs",
    "install_rebootstrap_checkpoint",
    "load_checkpoint_tail",
    "materialize_checkpoint_tail",
    "prune_operation_history",
    "read_shadow_checkpoint",
    "read_active_checkpoint",
    "read_retention_report",
    "retention_policy",
    "retention_preview",
    "set_retention_policy",
    "verify_shadow_checkpoint",
    "verify_checkpoint_ack",
    "verify_checkpoint_commit_proof",
    "verify_checkpoint_proposal",
    "write_shadow_checkpoint",
    "write_checkpoint_object",
]
