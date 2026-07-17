#!/usr/bin/env python3
"""Restricted command wrapper for PBGui Cluster Sync SSH keys.

The script is intended for OpenSSH forced-command use. It accepts one small
Cluster Sync command through SSH_ORIGINAL_COMMAND, validates the peer node, and
limits writes to cluster state plus explicit V7 config materialization.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import hashlib
import json
import os
import shlex
import shutil
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from cluster_credentials import (
    ClusterCredentialError,
    EnvelopeValidationError,
    NotRecipientError,
    SecretContext,
    SecretDecryptionError,
    deserialize_sealed_secret,
    ensure_node_key_material,
    load_node_encryption_private_keys,
    open_sealed_secret,
    verify_operation,
    validate_sealed_secret,
)
from cmc_leases import (
    CmcLeaseError,
    ClusterMailbox,
    MAX_MAILBOX_MESSAGE_BYTES,
)
from credential_store import CredentialStore
from file_lock import advisory_file_lock
from master.cluster_state import (
    ClusterPaths,
    ClusterStateError,
    V2_CREDENTIAL_OPS,
    append_operation,
    build_config_manifest,
    compute_config_manifest_hash,
    create_join_authorization,
    default_cluster_root,
    ensure_local_identity,
    membership_signing_public_key,
    read_materialized_state,
    read_local_identity,
    rebuild_materialized_state,
    stage_membership_operations,
    validate_operation,
    write_operation,
)
from master.cluster_ssh_keys import ensure_cluster_ssh_key
from master.cluster_checkpoint import (
    ClusterCheckpointError,
    activate_checkpoint,
    active_checkpoint_baseline,
    active_checkpoint_bundle,
    build_shadow_checkpoint,
    checkpoint_status,
    create_checkpoint_ack,
    install_rebootstrap_checkpoint,
    retention_preview,
    verify_checkpoint_commit_proof,
    verify_checkpoint_proposal,
    write_checkpoint_object,
)
from pb7_api_keys import (
    PB7ApiKeysMergeWriter,
    build_tradfi_projection,
    exchange_payload,
    project_active_tradfi_profiles,
)
from pbgui_purefunc import PBGDIR, pb7dir
from secure_files import atomic_write_private_bytes, ensure_private_directory_tree

PROTOCOL_VERSION = 2
MAX_COMMAND_BYTES = 4096
MAX_OPERATION_BYTES = 1024 * 1024
MAX_OPERATION_BATCH_BYTES = 16 * 1024 * 1024
MAX_CONFIG_BLOB_BYTES = 16 * 1024 * 1024
MAX_CONFIG_BLOB_BATCH_BYTES = 16 * 1024 * 1024
MAX_SECRET_BLOB_BYTES = 1024 * 1024
MAX_SEALED_BLOB_BYTES = 16 * 1024 * 1024
MAX_APPLY_BUNDLE_BYTES = 24 * 1024 * 1024
MAX_CHECKPOINT_BYTES = 32 * 1024 * 1024
MAX_GET_OPS = 1000
MAX_GET_BLOBS = 100
MAX_BLOB_COVERAGE_HASHES = 1000
MAX_BLOB_COVERAGE_VERIFY_BYTES = 64 * 1024 * 1024
MAILBOX_INDEX_VERBS = frozenset({"get-mailbox-index", "mailbox-list"})
MAILBOX_GET_VERBS = frozenset({"get-mailbox-message", "mailbox-get"})
MAILBOX_PUT_VERBS = frozenset({"put-mailbox-message", "mailbox-put"})
MAILBOX_ACK_VERBS = frozenset({"ack-mailbox-message", "mailbox-ack"})

READ_VERBS = frozenset({
    "handshake",
    "hello",
    "get-state-vector",
    "get-desired-state",
    "get-ops",
    "get-blob",
    "get-blobs",
    "get-secret-blob",
    "get-sealed-blob",
    "missing-blobs",
    *MAILBOX_INDEX_VERBS,
    *MAILBOX_GET_VERBS,
    "materialize-v7-preview",
    "materialize-api-keys-preview",
    "materialize-credentials-preview",
    "get-checkpoint-state",
    "retention-preview",
    "join-checkpoint-state",
    "join-get-blob",
    "join-state-vector",
    "join-get-ops",
})
WRITE_VERBS = frozenset({"join", "join-hello", "join-checkpoint", "join-register", "put-op", "put-ops", "put-blob", "put-blobs", "put-secret-blob", "put-sealed-blob", "apply-bundle", "rebuild", "materialize-v7", "materialize-api-keys", "materialize-credentials", "prepare-checkpoint", "commit-checkpoint", "install-checkpoint", *MAILBOX_PUT_VERBS, *MAILBOX_ACK_VERBS})
STDIN_VERBS = frozenset({"join-checkpoint", "join-register", "put-op", "put-ops", "put-blob", "put-blobs", "put-secret-blob", "put-sealed-blob", "apply-bundle", "prepare-checkpoint", "commit-checkpoint", "install-checkpoint", "missing-blobs", *MAILBOX_PUT_VERBS})
SUPPORTED_VERBS = READ_VERBS | WRITE_VERBS


class ClusterSyncCommandError(RuntimeError):
    """Raised when a restricted Cluster Sync command is invalid."""


def run_command(
    cluster_root: Path,
    remote_node: str,
    command_text: str,
    stdin_data: bytes = b"",
    *,
    allow_join: bool = False,
) -> dict[str, Any]:
    """Execute one restricted Cluster Sync command and return a JSON payload."""

    root = Path(cluster_root)
    _validate_node_id(remote_node, "remote_node")
    tokens = _parse_command(command_text)
    verb = tokens[0]

    if verb == "join":
        return _join_cluster(root, remote_node, tokens, allow_join=allow_join)
    if verb == "join-hello":
        if not allow_join:
            raise ClusterSyncCommandError("join-hello requires allow_join")
        _require_arity(tokens, 3)
        subject = _validate_node_id(tokens[1], "node_id")
        role = _validate_role(tokens[2])
        if subject != remote_node:
            raise ClusterSyncCommandError("join-hello node_id must match remote node")
        identity = read_local_identity(root)
        payload = _hello_payload(root, identity, remote_node)
        payload["join_authorization"] = _safe_state_call(
            lambda: create_join_authorization(root, subject, role)
        )
        return payload
    if verb == "join-checkpoint":
        if not allow_join:
            raise ClusterSyncCommandError("join-checkpoint requires allow_join")
        _require_arity(tokens, 1)
        payload = _read_checkpoint_control_payload(stdin_data)
        cluster_id = _validate_cluster_id(payload.get("cluster_id"))
        node_id = _validate_node_id(payload.get("node_id"), "node_id")
        role = _validate_role(payload.get("role"))
        pbname = str(payload.get("pbname") or "").strip()
        checkpoint = payload.get("checkpoint")
        proof = payload.get("commit_proof")
        authorization = payload.get("authorization")
        anchor_public_key = str(payload.get("anchor_public_key") or "")
        if not isinstance(checkpoint, dict) or not isinstance(proof, dict) or not isinstance(authorization, dict):
            raise ClusterSyncCommandError("join-checkpoint payload is incomplete")
        if str(proof.get("coordinator_id") or "") != remote_node:
            raise ClusterSyncCommandError("join-checkpoint caller must be checkpoint coordinator")
        identity = _safe_state_call(
            lambda: ensure_local_identity(
                root,
                role=role,
                pbname=pbname,
                cluster_id=cluster_id,
                node_id=node_id,
            )
        )
        try:
            installed = install_rebootstrap_checkpoint(
                root,
                checkpoint,
                proof,
                join_authorization=authorization,
                join_anchor_public_key=anchor_public_key,
                defer_blob_validation=True,
            )
            membership = append_operation(
                root,
                "ADD_NODE",
                {
                    "node_id": node_id,
                    "role": role,
                    "pbname": pbname,
                    "membership_authorization": authorization,
                },
            )
            rebuild_materialized_state(root)
        except (ClusterCheckpointError, ClusterStateError) as exc:
            raise ClusterSyncCommandError(str(exc)) from exc
        return {
            "ok": True,
            "cluster_id": str(identity["cluster_id"]),
            "node_id": str(identity["node_id"]),
            "role": role,
            "pbname": pbname,
            "joined_by": remote_node,
            "checkpoint_id": str(checkpoint["checkpoint_id"]),
            "checkpoint_epoch": int(installed["epoch"]),
            "membership_op_id": str(membership["op_id"]),
        }
    if verb == "join-checkpoint-state":
        if not allow_join:
            raise ClusterSyncCommandError("join-checkpoint-state requires allow_join")
        _require_arity(tokens, 2)
        authorization = _verified_join_authorization(root, remote_node, tokens[1])
        bundle = active_checkpoint_bundle(root)
        if bundle is None:
            return {"ok": True, "status": "missing", "bundle": None}
        if str(bundle["commit_proof"].get("coordinator_id") or "") != str(authorization.get("signer_id") or ""):
            raise ClusterSyncCommandError("join must use the elected checkpoint coordinator")
        return {"ok": True, "status": "current", "bundle": bundle}
    if verb == "join-state-vector":
        if not allow_join:
            raise ClusterSyncCommandError("join-state-vector requires allow_join")
        _require_arity(tokens, 2)
        _verified_join_authorization(root, remote_node, tokens[1])
        materialized = _safe_state_call(lambda: rebuild_materialized_state(root, write=False))
        return {
            "ok": True,
            "cluster_id": str((materialized.get("cluster_nodes") or {}).get("cluster_id") or ""),
            "node_id": str(read_local_identity(root)["node_id"]),
            "state_vector": materialized.get("state_vector") or {},
            **checkpoint_status(root),
        }
    if verb == "join-get-ops":
        if not allow_join:
            raise ClusterSyncCommandError("join-get-ops requires allow_join")
        _require_arity(tokens, 5)
        _verified_join_authorization(root, remote_node, tokens[1])
        actor = _validate_node_id(tokens[2], "actor")
        from_seq = _parse_positive_int(tokens[3], "from_seq")
        to_seq = _parse_positive_int(tokens[4], "to_seq")
        if to_seq < from_seq or to_seq - from_seq + 1 > MAX_GET_OPS:
            raise ClusterSyncCommandError("invalid join operation range")
        baseline = active_checkpoint_baseline(root)
        if from_seq <= int(baseline.get(actor, 0)):
            return {"ok": False, "status": "checkpoint_required", **checkpoint_status(root)}
        identity = read_local_identity(root)
        operations, missing = _read_operation_range(
            root,
            actor,
            from_seq,
            to_seq,
            expected_cluster_id=str(identity["cluster_id"]),
        )
        return {
            "ok": True,
            "cluster_id": str(identity["cluster_id"]),
            "node_id": str(identity["node_id"]),
            "operations": operations,
            "missing": missing,
        }
    if verb == "join-get-blob":
        if not allow_join:
            raise ClusterSyncCommandError("join-get-blob requires allow_join")
        _require_arity(tokens, 4)
        _verified_join_authorization(root, remote_node, tokens[1])
        kind = str(tokens[2])
        blob_hash = _validate_hash(tokens[3])
        paths = ClusterPaths.from_root(root)
        if not _join_blob_allowed(root, kind, blob_hash):
            raise ClusterSyncCommandError("join blob is not referenced by the active checkpoint")
        if kind == "config":
            return _read_blob_response(paths.config_blobs, blob_hash, secret=False, cluster_root=root)
        if kind == "secret":
            return _read_blob_response(paths.secret_blobs, blob_hash, secret=True)
        if kind == "sealed":
            return _read_blob_response(paths.sealed_blobs, blob_hash, secret=True)
        raise ClusterSyncCommandError("invalid join blob kind")
    if verb == "join-register":
        if not allow_join:
            raise ClusterSyncCommandError("join-register requires allow_join")
        _require_arity(tokens, 2)
        authorization = _verified_join_authorization(root, remote_node, tokens[1])
        operation = _read_json_payload(stdin_data, MAX_OPERATION_BYTES)
        if (
            str(operation.get("op") or "") != "ADD_NODE"
            or str(operation.get("actor") or "") != remote_node
            or str(operation.get("node_id") or "") != remote_node
            or operation.get("membership_authorization") != authorization
        ):
            raise ClusterSyncCommandError("join-register accepts only the authorized node self-add")
        _safe_state_call(lambda: write_operation(root, operation, network_input=True))
        _safe_state_call(lambda: rebuild_materialized_state(root))
        return {"ok": True, "op_id": str(operation["op_id"]), "node_id": remote_node}

    identity = read_local_identity(root)
    cluster_id = str(identity["cluster_id"])
    _verify_remote_node(root, remote_node, allow_join=False)
    paths = ClusterPaths.from_root(root)

    if verb == "handshake":
        payload = _hello_payload(root, identity, remote_node)
        materialized = _read_materialized_snapshot(root)
        payload["state_vector"] = materialized.get("state_vector") or {}
        return payload
    if verb == "hello":
        payload = _hello_payload(root, identity, remote_node)
        payload["credential_migration_advance"] = {"status": "delegated_to_pbcluster"}
        return payload
    if verb == "get-state-vector":
        materialized = _read_materialized_snapshot(root)
        return {
            "ok": True,
            "cluster_id": cluster_id,
            "node_id": str(identity["node_id"]),
            "state_vector": materialized.get("state_vector") or {},
            **checkpoint_status(root),
        }
    if verb == "get-desired-state":
        materialized = _read_materialized_snapshot(root)
        return {
            "ok": True,
            "cluster_id": cluster_id,
            "node_id": str(identity["node_id"]),
            "desired_state": materialized.get("desired_state") or {},
        }
    if verb == "get-ops":
        _require_arity_range(tokens, 3, 4)
        actor = _validate_node_id(tokens[1], "actor")
        from_seq = _parse_positive_int(tokens[2], "from_seq")
        to_seq = _parse_positive_int(tokens[3], "to_seq") if len(tokens) == 4 else from_seq
        baseline = active_checkpoint_baseline(root)
        if from_seq <= int(baseline.get(actor, 0)):
            return {
                "ok": False,
                "status": "checkpoint_required",
                "cluster_id": cluster_id,
                "node_id": str(identity["node_id"]),
                **checkpoint_status(root),
            }
        operations, missing = _read_operation_range(root, actor, from_seq, to_seq, expected_cluster_id=cluster_id)
        return {
            "ok": True,
            "cluster_id": cluster_id,
            "node_id": str(identity["node_id"]),
            "actor": actor,
            "from_seq": from_seq,
            "to_seq": to_seq,
            "operations": operations,
            "missing": missing,
        }
    if verb == "get-blob":
        _require_arity(tokens, 2)
        return _read_blob_response(paths.config_blobs, tokens[1], secret=False, cluster_root=root)
    if verb == "get-blobs":
        _require_arity_range(tokens, 2, MAX_GET_BLOBS + 1)
        blobs = [
            _read_blob_response(paths.config_blobs, token, secret=False, cluster_root=root)
            for token in tokens[1:]
        ]
        return {"ok": True, "count": len(blobs), "blobs": blobs}
    if verb == "get-secret-blob":
        _require_arity(tokens, 2)
        if tokens[1] in set((_credential_cutoff(root) or {}).get("obsolete_secret_blob_hashes") or []):
            raise ClusterSyncCommandError("pre-cutoff plaintext secret blob is unavailable")
        return _read_blob_response(paths.secret_blobs, tokens[1], secret=True)
    if verb == "get-sealed-blob":
        _require_arity(tokens, 2)
        response = _read_blob_response(paths.sealed_blobs, tokens[1], secret=True)
        _validate_sealed_blob_payload(root, base64.b64decode(response["content_b64"]))
        return response
    if verb == "missing-blobs":
        _require_arity(tokens, 1)
        request = _read_json_payload(stdin_data, MAX_OPERATION_BATCH_BYTES)
        return _missing_blob_coverage(paths, request)
    if verb in MAILBOX_INDEX_VERBS:
        _require_arity(tokens, 1)
        _verify_remote_node(root, remote_node, allow_join=False)
        messages = _mailbox_call(lambda: ClusterMailbox(root).index())
        return {"ok": True, "count": len(messages), "messages": messages}
    if verb in MAILBOX_GET_VERBS:
        _require_arity(tokens, 2)
        _verify_remote_node(root, remote_node, allow_join=False)
        message = _mailbox_call(lambda: ClusterMailbox(root).get(tokens[1]))
        return {"ok": True, "message": message}
    if verb == "materialize-v7-preview":
        return _materialize_v7_configs(root, write=False)
    if verb == "materialize-api-keys-preview":
        return _materialize_api_keys(root, write=False)
    if verb == "materialize-credentials-preview":
        return _materialize_credentials(root, write=False)
    if verb == "get-checkpoint-state":
        _require_arity(tokens, 1)
        bundle = active_checkpoint_bundle(root)
        return {
            "ok": True,
            "status": "current" if bundle is not None else "missing",
            "bundle": bundle,
        }
    if verb == "retention-preview":
        _require_arity(tokens, 1)
        checkpoint = build_shadow_checkpoint(root)
        preview = retention_preview(root, checkpoint, item_limit=200)
        return {"ok": True, **preview}
    if verb == "prepare-checkpoint":
        _require_arity(tokens, 1)
        payload = _read_checkpoint_control_payload(stdin_data)
        checkpoint = payload.get("checkpoint")
        proposal = payload.get("proposal")
        if not isinstance(checkpoint, dict) or not isinstance(proposal, dict):
            raise ClusterSyncCommandError("checkpoint prepare payload is incomplete")
        try:
            verified = verify_checkpoint_proposal(checkpoint, proposal)
            with advisory_file_lock(root / ".append_sequence"):
                local = build_shadow_checkpoint(
                    root,
                    created_at=int(checkpoint.get("created_at") or 0),
                    history_seconds=int(checkpoint.get("history_seconds") or 0),
                )
                if local["checkpoint_id"] != checkpoint.get("checkpoint_id"):
                    raise ClusterCheckpointError("local checkpoint does not match proposal")
                write_checkpoint_object(root, local)
                ack = create_checkpoint_ack(root, local, verified)
                _cleanup_expired_prepared_checkpoints(root)
                prepared_path = _prepared_checkpoint_path(root, str(verified["proposal_id"]))
                atomic_write_private_bytes(
                    prepared_path,
                    (json.dumps({
                        "checkpoint_id": str(local["checkpoint_id"]),
                        "proposal_id": str(verified["proposal_id"]),
                        "expires_at": int(verified["expires_at"]),
                    }, indent=4, sort_keys=True) + "\n").encode("utf-8"),
                )
        except ClusterCheckpointError as exc:
            raise ClusterSyncCommandError(str(exc)) from exc
        return {"ok": True, "status": "prepared", "checkpoint_id": local["checkpoint_id"], "ack": ack}
    if verb == "commit-checkpoint":
        _require_arity(tokens, 1)
        payload = _read_checkpoint_control_payload(stdin_data)
        checkpoint = payload.get("checkpoint")
        proof = payload.get("commit_proof")
        if not isinstance(checkpoint, dict) or not isinstance(proof, dict):
            raise ClusterSyncCommandError("checkpoint commit payload is incomplete")
        try:
            verified_proof = verify_checkpoint_commit_proof(checkpoint, proof)
            prepared_path = _prepared_checkpoint_path(root, str(verified_proof["proposal_id"]))
            if prepared_path.is_symlink() or not prepared_path.is_file():
                raise ClusterCheckpointError("checkpoint was not prepared locally")
            prepared = json.loads(prepared_path.read_text(encoding="utf-8"))
            if (
                not isinstance(prepared, dict)
                or prepared.get("checkpoint_id") != checkpoint.get("checkpoint_id")
                or int(prepared.get("expires_at") or 0) <= int(time.time())
            ):
                raise ClusterCheckpointError("prepared checkpoint is invalid or expired")
            current_vector = (rebuild_materialized_state(root, write=False).get("state_vector") or {})
            if current_vector != checkpoint.get("baseline_vector"):
                raise ClusterCheckpointError("cluster state changed after checkpoint preparation")
            commit = activate_checkpoint(root, checkpoint, commit_proof=proof)
            prepared_path.unlink(missing_ok=True)
        except ClusterCheckpointError as exc:
            raise ClusterSyncCommandError(str(exc)) from exc
        return {
            "ok": True,
            "status": "committed",
            "checkpoint_id": str(checkpoint["checkpoint_id"]),
            "epoch": int(commit["epoch"]),
        }
    if verb == "install-checkpoint":
        _require_arity(tokens, 1)
        payload = _read_checkpoint_control_payload(stdin_data)
        checkpoint = payload.get("checkpoint")
        proof = payload.get("commit_proof")
        if not isinstance(checkpoint, dict) or not isinstance(proof, dict):
            raise ClusterSyncCommandError("checkpoint install payload is incomplete")
        try:
            result = install_rebootstrap_checkpoint(root, checkpoint, proof)
        except ClusterCheckpointError as exc:
            raise ClusterSyncCommandError(str(exc)) from exc
        return {"ok": True, **result}
    if verb == "rebuild":
        materialized = _safe_state_call(lambda: rebuild_materialized_state(root))
        return {
            "ok": True,
            "generation": int((materialized.get("cluster_nodes") or {}).get("generation") or 0),
            "nodes": len(((materialized.get("cluster_nodes") or {}).get("nodes") or {})),
            "instances": len(((materialized.get("desired_state") or {}).get("instances") or {})),
        }
    if verb == "materialize-v7":
        return _materialize_v7_configs(root, write=True)
    if verb == "materialize-api-keys":
        return _materialize_api_keys(root, write=True)
    if verb == "materialize-credentials":
        return _materialize_credentials(root, write=True)
    if verb == "put-op":
        _require_arity(tokens, 1)
        operation = _read_json_payload(stdin_data, MAX_OPERATION_BYTES)
        staged_nodes = _staged_membership_nodes(root, [operation], remote_node=remote_node)
        _safe_state_call(
            lambda: validate_operation(
                operation,
                expected_cluster_id=cluster_id,
                cluster_root=root,
                membership_trust=staged_nodes,
                network_input=True,
            )
        )
        _safe_state_call(
            lambda: write_operation(
                root,
                operation,
                network_input=True,
                membership_trust=staged_nodes,
            )
        )
        return {"ok": True, "op_id": str(operation["op_id"]), "actor": str(operation["actor"]), "seq": int(operation["seq"])}
    if verb == "put-ops":
        _require_arity(tokens, 1)
        operations = _read_operation_batch_payload(stdin_data)
        staged_nodes = _staged_membership_nodes(root, operations, remote_node=remote_node)
        for operation in operations:
            _safe_state_call(
                lambda op=operation: validate_operation(
                    op,
                    expected_cluster_id=cluster_id,
                    cluster_root=root,
                    membership_trust=staged_nodes,
                    network_input=True,
                )
            )
        written: list[dict[str, Any]] = []
        publish_order = sorted(
            operations,
            key=lambda item: str(item.get("op") or "") in V2_CREDENTIAL_OPS,
        )
        for operation in publish_order:
            _safe_state_call(
                lambda op=operation: write_operation(
                    root,
                    op,
                    network_input=True,
                    membership_trust=staged_nodes,
                )
            )
        for operation in operations:
            written.append({"op_id": str(operation["op_id"]), "actor": str(operation["actor"]), "seq": int(operation["seq"])})
        return {"ok": True, "count": len(written), "operations": written}
    if verb == "put-blob":
        _require_arity(tokens, 2)
        blob_hash = _validate_hash(tokens[1])
        with advisory_file_lock(root / ".append_sequence"):
            path = _write_blob(paths.config_blobs, blob_hash, stdin_data, MAX_CONFIG_BLOB_BYTES, secret=False)
        return {"ok": True, "hash": blob_hash, "path": _relative_cluster_path(paths.root, path)}
    if verb == "put-blobs":
        _require_arity(tokens, 1)
        blobs = _read_blob_batch_payload(stdin_data)
        written: list[dict[str, str]] = []
        with advisory_file_lock(root / ".append_sequence"):
            for blob in blobs:
                path = _write_blob(paths.config_blobs, str(blob["hash"]), blob["raw"], MAX_CONFIG_BLOB_BYTES, secret=False)
                written.append({"hash": str(blob["hash"]), "path": _relative_cluster_path(paths.root, path)})
        return {"ok": True, "count": len(written), "blobs": written}
    if verb == "put-secret-blob":
        _require_arity(tokens, 2)
        blob_hash = _validate_hash(tokens[1])
        if blob_hash in set((_credential_cutoff(root) or {}).get("obsolete_secret_blob_hashes") or []):
            raise ClusterSyncCommandError("pre-cutoff plaintext secret blob is obsolete")
        with advisory_file_lock(root / ".append_sequence"):
            path = _write_blob(paths.secret_blobs, blob_hash, stdin_data, MAX_SECRET_BLOB_BYTES, secret=True)
        return {"ok": True, "hash": blob_hash, "path": _relative_cluster_path(paths.root, path)}
    if verb == "put-sealed-blob":
        _require_arity(tokens, 2)
        blob_hash = _validate_hash(tokens[1])
        _validate_sealed_blob_payload(root, stdin_data)
        with advisory_file_lock(root / ".append_sequence"):
            path = _write_blob(paths.sealed_blobs, blob_hash, stdin_data, MAX_SEALED_BLOB_BYTES, secret=True)
        return {"ok": True, "hash": blob_hash, "path": _relative_cluster_path(paths.root, path)}
    if verb in MAILBOX_PUT_VERBS:
        _require_arity(tokens, 1)
        _verify_remote_node(root, remote_node, allow_join=False)
        message = _read_json_payload(stdin_data, MAX_MAILBOX_MESSAGE_BYTES)
        created = _mailbox_call(lambda: ClusterMailbox(root).put(message))
        return {
            "ok": True,
            "message_id": str(message.get("message_id") or ""),
            "created": bool(created),
        }
    if verb in MAILBOX_ACK_VERBS:
        _require_arity(tokens, 2)
        _verify_remote_node(root, remote_node, allow_join=False)
        created = _mailbox_call(lambda: ClusterMailbox(root).ack(tokens[1], remote_node))
        return {"ok": True, "message_id": tokens[1], "created": bool(created)}
    if verb == "apply-bundle":
        _require_arity(tokens, 1)
        with advisory_file_lock(root / ".append_sequence"):
            return _apply_bundle(root, paths, cluster_id, stdin_data, remote_node=remote_node)
    raise ClusterSyncCommandError(f"unsupported command: {verb}")


def main(argv: list[str] | None = None) -> int:
    """Command-line entry point for OpenSSH forced-command execution."""

    parser = argparse.ArgumentParser(description="PBGui restricted Cluster Sync command wrapper")
    parser.add_argument("--cluster-root", default=str(default_cluster_root(Path(PBGDIR))))
    parser.add_argument("--remote-node", required=True)
    parser.add_argument("--allow-join", action="store_true")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)

    try:
        command_text = os.environ.get("SSH_ORIGINAL_COMMAND") or " ".join(args.command).strip()
        stdin_data = _read_stdin_for_command(command_text)
        payload = run_command(
            Path(args.cluster_root),
            str(args.remote_node),
            command_text,
            stdin_data,
            allow_join=bool(args.allow_join),
        )
    except Exception as exc:
        error = {"ok": False, "error": str(exc)}
        sys.stderr.write(json.dumps(error, sort_keys=True) + "\n")
        return 1
    sys.stdout.write(json.dumps(payload, sort_keys=True) + "\n")
    return 0


def _read_stdin_for_command(command_text: str) -> bytes:
    """Read stdin only for commands that carry an upload payload."""

    tokens = _parse_command(command_text)
    if tokens[0] not in STDIN_VERBS:
        return b""
    return sys.stdin.buffer.read(max(MAX_CONFIG_BLOB_BATCH_BYTES, MAX_CONFIG_BLOB_BYTES, MAX_OPERATION_BATCH_BYTES, MAX_APPLY_BUNDLE_BYTES, MAX_CHECKPOINT_BYTES) + 1)


def _hello_payload(cluster_root: Path, identity: dict[str, Any], remote_node: str) -> dict[str, Any]:
    """Return peer metadata used by hello and handshake."""

    payload = {
        "ok": True,
        "protocol_version": PROTOCOL_VERSION,
        "cluster_id": str(identity["cluster_id"]),
        "node_id": str(identity["node_id"]),
        "role": str(identity.get("role") or ""),
        "remote_node": remote_node,
        "mailbox_capability": True,
        "checkpoint_capability": True,
        "blob_coverage_capability": True,
        **checkpoint_status(cluster_root),
    }
    try:
        crypto = ensure_node_key_material(cluster_root)
        public_bundle = crypto.public_bundle(
            str(identity.get("node_id") or ""),
            str(identity.get("role") or "master"),
        )
        payload.update({
            "crypto_public_bundle": public_bundle,
            "credential_capability": {
                "version": 2,
                "sealed_credentials": True,
                "operation_signing": "Ed25519",
                "secret_encryption": "HPKE-X25519-HKDF-SHA256-AES128GCM",
                "audiences": ["cluster", "masters"],
            },
            "capabilities": ["sealed_credentials_v2"],
        })
    except Exception as exc:
        payload["credential_capability"] = {
            "version": 2,
            "sealed_credentials": False,
            "error": str(exc),
        }
    try:
        key = ensure_cluster_ssh_key(cluster_root, node_id=str(identity.get("node_id") or ""))
        payload.update({
            "cluster_ssh_public_key": str(key.get("public_key") or ""),
            "cluster_ssh_fingerprint": str(key.get("fingerprint") or ""),
            "cluster_ssh_mode": "forced",
        })
    except Exception as exc:
        payload["cluster_ssh_error"] = str(exc)
    return payload


def _join_cluster(cluster_root: Path, remote_node: str, tokens: list[str], *, allow_join: bool) -> dict[str, Any]:
    """Initialize local identity for an explicitly approved remote join."""

    if not allow_join:
        raise ClusterSyncCommandError("join requires allow_join")
    _require_arity_range(tokens, 5, 6)
    cluster_id = _validate_cluster_id(tokens[1])
    node_id = _validate_node_id(tokens[2], "node_id")
    role = _validate_role(tokens[3])
    pbname = str(tokens[4] or "").strip()
    identity = _safe_state_call(
        lambda: ensure_local_identity(
            cluster_root,
            role=role,
            pbname=pbname,
            cluster_id=cluster_id,
            node_id=node_id,
        )
    )
    membership = None
    if len(tokens) == 6:
        try:
            decoded = base64.b64decode(tokens[5].encode("ascii"), altchars=b"-_", validate=True)
            bootstrap = json.loads(decoded.decode("utf-8"))
        except (UnicodeEncodeError, UnicodeDecodeError, binascii.Error, json.JSONDecodeError) as exc:
            raise ClusterSyncCommandError("join bootstrap metadata is invalid") from exc
        if not isinstance(bootstrap, dict):
            raise ClusterSyncCommandError("join bootstrap metadata must be an object")
        authorization = bootstrap.get("authorization")
        authorizer_operation = bootstrap.get("authorizer_operation")
        if not isinstance(authorization, dict) or not isinstance(authorizer_operation, dict):
            raise ClusterSyncCommandError("join bootstrap metadata is incomplete")
        if str(authorization.get("signer_id") or "") != remote_node:
            raise ClusterSyncCommandError("join authorization signer must match remote node")
        _safe_state_call(
            lambda: write_operation(
                cluster_root,
                authorizer_operation,
                network_input=True,
            )
        )
        membership = _safe_state_call(
            lambda: append_operation(
                cluster_root,
                "ADD_NODE",
                {
                    "node_id": node_id,
                    "role": role,
                    "pbname": pbname,
                    "membership_authorization": authorization,
                },
            )
        )
        _safe_state_call(lambda: rebuild_materialized_state(cluster_root))
    return {
        "ok": True,
        "cluster_id": str(identity["cluster_id"]),
        "node_id": str(identity["node_id"]),
        "role": str(identity.get("role") or role),
        "pbname": str(identity.get("created_from_pbname") or pbname),
        "joined_by": remote_node,
        "membership_op_id": str((membership or {}).get("op_id") or ""),
    }


def _verified_join_authorization(
    cluster_root: Path,
    remote_node: str,
    encoded: str,
) -> dict[str, Any]:
    """Verify one node-specific join grant from the elected active master."""

    try:
        raw = base64.urlsafe_b64decode(str(encoded).encode("ascii") + b"=" * (-len(str(encoded)) % 4))
        authorization = json.loads(raw.decode("utf-8"))
    except (UnicodeEncodeError, UnicodeDecodeError, binascii.Error, json.JSONDecodeError) as exc:
        raise ClusterSyncCommandError("join authorization token is invalid") from exc
    if (
        not isinstance(authorization, dict)
        or str(authorization.get("kind") or "") != "join"
        or str(authorization.get("node_id") or "") != remote_node
    ):
        raise ClusterSyncCommandError("join authorization does not match remote node")
    created_at = int(authorization.get("created_at") or 0)
    now = int(time.time())
    if created_at < now - 900 or created_at > now + 300:
        raise ClusterSyncCommandError("join authorization has expired")
    materialized = _safe_state_call(lambda: rebuild_materialized_state(cluster_root, write=False))
    if str(authorization.get("cluster_id") or "") != str((materialized.get("cluster_nodes") or {}).get("cluster_id") or ""):
        raise ClusterSyncCommandError("join authorization belongs to another cluster")
    nodes = ((materialized.get("cluster_nodes") or {}).get("nodes") or {})
    masters = sorted(
        str(node_id)
        for node_id, node in nodes.items()
        if isinstance(node, dict)
        and str(node.get("role") or "") == "master"
        and node.get("enabled", True) is not False
        and node.get("state_replica", True) is not False
    )
    signer_id = str(authorization.get("signer_id") or "")
    if not masters or signer_id != masters[0]:
        raise ClusterSyncCommandError("join authorization signer is not checkpoint coordinator")
    public_key = str((nodes.get(signer_id) or {}).get("signing_public_key") or "")
    if not public_key:
        raise ClusterSyncCommandError("join authorization signer has no trusted key")
    try:
        verify_operation(authorization, public_key)
    except ClusterCredentialError as exc:
        raise ClusterSyncCommandError(str(exc)) from exc
    return authorization


def _join_blob_allowed(cluster_root: Path, kind: str, blob_hash: str) -> bool:
    """Allow join-time reads only for blobs reachable from the active checkpoint."""

    bundle = active_checkpoint_bundle(cluster_root)
    if not isinstance(bundle, dict):
        return False
    checkpoint = bundle.get("checkpoint") if isinstance(bundle.get("checkpoint"), dict) else {}
    refs = checkpoint.get("blob_refs") if isinstance(checkpoint.get("blob_refs"), dict) else {}
    direct = {str(item) for item in refs.get(kind) or []}
    if blob_hash in direct:
        return True
    if kind != "config":
        return False
    paths = ClusterPaths.from_root(cluster_root)
    for manifest_hash in refs.get("config") or []:
        try:
            raw = _read_verified_blob(paths.config_blobs, str(manifest_hash), "config blob")
            manifest = json.loads(raw.decode("utf-8"))
        except (ClusterSyncCommandError, UnicodeDecodeError, json.JSONDecodeError):
            continue
        files = manifest.get("files") if isinstance(manifest, dict) else None
        if not isinstance(files, dict):
            continue
        for record in files.values():
            digest = str((record or {}).get("sha256") or "") if isinstance(record, dict) else ""
            if digest and blob_hash == f"sha256:{digest}":
                return True
    return False


def _parse_command(command_text: str) -> list[str]:
    """Parse and validate SSH_ORIGINAL_COMMAND."""

    raw = str(command_text or "").strip()
    if not raw:
        raise ClusterSyncCommandError("missing command")
    if len(raw.encode("utf-8")) > MAX_COMMAND_BYTES:
        raise ClusterSyncCommandError("command too large")
    try:
        tokens = shlex.split(raw)
    except ValueError as exc:
        raise ClusterSyncCommandError("invalid command syntax") from exc
    if not tokens:
        raise ClusterSyncCommandError("missing command")
    verb = tokens[0]
    if verb not in SUPPORTED_VERBS:
        raise ClusterSyncCommandError(f"unsupported command: {verb}")
    return tokens


def _safe_state_call(callback):
    """Convert cluster-state validation errors into wrapper errors."""

    try:
        return callback()
    except ClusterStateError as exc:
        raise ClusterSyncCommandError(str(exc)) from exc


def _read_materialized_snapshot(cluster_root: Path) -> dict[str, Any]:
    """Read the last atomically persisted state without replaying the oplog."""

    return _safe_state_call(lambda: read_materialized_state(cluster_root))


def _mailbox_call(callback):
    """Convert mailbox validation errors into restricted-command errors."""

    try:
        return callback()
    except CmcLeaseError as exc:
        raise ClusterSyncCommandError(str(exc)) from exc


def _staged_membership_nodes(
    cluster_root: Path,
    operations: list[dict[str, Any]],
    *,
    remote_node: str | None = None,
):
    """Stage only membership changes authenticated against prior trust."""

    identity = _safe_state_call(lambda: read_local_identity(cluster_root))
    return _safe_state_call(
        lambda: stage_membership_operations(
            cluster_root,
            operations,
            expected_cluster_id=str(identity["cluster_id"]),
            authenticated_remote_node=remote_node,
        )
    )


def _require_arity(tokens: list[str], expected: int) -> None:
    """Require an exact token count."""

    if len(tokens) != expected:
        raise ClusterSyncCommandError(f"{tokens[0]} expects {expected - 1} argument(s)")


def _require_arity_range(tokens: list[str], minimum: int, maximum: int) -> None:
    """Require a token count within an inclusive range."""

    if len(tokens) < minimum or len(tokens) > maximum:
        raise ClusterSyncCommandError(
            f"{tokens[0]} expects between {minimum - 1} and {maximum - 1} argument(s)"
        )


def _parse_positive_int(value: str, field: str) -> int:
    """Parse a positive integer command argument."""

    try:
        number = int(value)
    except (TypeError, ValueError) as exc:
        raise ClusterSyncCommandError(f"{field} must be a positive integer") from exc
    if number < 1:
        raise ClusterSyncCommandError(f"{field} must be a positive integer")
    return number


def _read_operation_range(
    cluster_root: Path,
    actor: str,
    from_seq: int,
    to_seq: int,
    *,
    expected_cluster_id: str,
) -> tuple[list[dict[str, Any]], list[int]]:
    """Read a bounded operation range for one actor."""

    if to_seq < from_seq:
        raise ClusterSyncCommandError("to_seq must be greater than or equal to from_seq")
    if to_seq - from_seq + 1 > MAX_GET_OPS:
        raise ClusterSyncCommandError(f"get-ops range cannot exceed {MAX_GET_OPS} operation(s)")

    operations: list[dict[str, Any]] = []
    missing: list[int] = []
    op_dir = ClusterPaths.from_root(cluster_root).oplog / actor
    for seq in range(from_seq, to_seq + 1):
        path = op_dir / f"{seq:08d}.json"
        if not path.is_file():
            missing.append(seq)
            continue
        try:
            operation = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ClusterSyncCommandError(f"failed to read operation {actor}:{seq:08d}") from exc
        _safe_state_call(
            lambda op=operation: validate_operation(
                op,
                expected_cluster_id=expected_cluster_id,
                cluster_root=cluster_root,
            )
        )
        if str(operation.get("actor") or "") != actor or int(operation.get("seq") or 0) != seq:
            raise ClusterSyncCommandError(f"operation path does not match actor/seq: {actor}:{seq:08d}")
        operations.append(operation)
    return operations, missing


def _credential_cutoff(cluster_root: Path) -> dict[str, Any] | None:
    """Return the current non-conflicted signed credential cutoff."""

    try:
        materialized = rebuild_materialized_state(cluster_root, write=False)
    except Exception:
        return None
    migration = (materialized.get("desired_state") or {}).get("credential_migration") or {}
    cutoff = migration.get("cutoff") if isinstance(migration.get("cutoff"), dict) else None
    return cutoff if isinstance(cutoff, dict) and cutoff.get("conflicted") is not True else None


def _read_blob_response(
    base_dir: Path,
    blob_hash: str,
    *,
    secret: bool,
    cluster_root: Path | None = None,
) -> dict[str, Any]:
    """Return one verified blob as a base64 JSON response."""

    validated_hash = _validate_hash(blob_hash)
    try:
        raw = _read_verified_blob(base_dir, validated_hash, "secret blob" if secret else "config blob")
    except ClusterSyncCommandError as exc:
        if secret or cluster_root is None or not str(exc).startswith("missing config blob:"):
            raise
        if not _repair_config_blob_response_from_run_v7(Path(cluster_root), validated_hash):
            raise
        raw = _read_verified_blob(base_dir, validated_hash, "config blob")
    return {
        "ok": True,
        "hash": validated_hash,
        "size": len(raw),
        "content_b64": base64.b64encode(raw).decode("ascii"),
    }


def _missing_blob_coverage(paths: ClusterPaths, request: dict[str, Any]) -> dict[str, Any]:
    """Return requested content-addressed hashes absent from local blob stores."""

    kinds = {
        "config": (paths.config_blobs, "config blob", MAX_CONFIG_BLOB_BYTES),
        "secret": (paths.secret_blobs, "secret blob", MAX_SECRET_BLOB_BYTES),
        "sealed": (paths.sealed_blobs, "sealed blob", MAX_SEALED_BLOB_BYTES),
    }
    if set(request) != set(kinds):
        raise ClusterSyncCommandError("missing-blobs payload must contain config, secret, and sealed lists")
    normalized: dict[str, list[str]] = {}
    total = 0
    for kind in kinds:
        values = request.get(kind)
        if not isinstance(values, list) or any(not isinstance(item, str) for item in values):
            raise ClusterSyncCommandError(f"missing-blobs {kind} must be a hash list")
        hashes = [_validate_hash(item) for item in values]
        if hashes != sorted(set(hashes)):
            raise ClusterSyncCommandError(f"missing-blobs {kind} hashes must be sorted and unique")
        normalized[kind] = hashes
        total += len(hashes)
    if total > MAX_BLOB_COVERAGE_HASHES:
        raise ClusterSyncCommandError("missing-blobs request is too large")

    missing: dict[str, list[str]] = {kind: [] for kind in kinds}
    verify: list[tuple[str, str, Path, str, int]] = []
    verify_bytes = 0
    for kind, hashes in normalized.items():
        base_dir, label, max_size = kinds[kind]
        for blob_hash in hashes:
            digest = blob_hash.removeprefix("sha256:")
            path = base_dir / "sha256" / digest[:2] / f"{digest}.json"
            if path.is_symlink() or not path.is_file():
                missing[kind].append(blob_hash)
                continue
            try:
                size = int(path.stat().st_size)
            except OSError:
                missing[kind].append(blob_hash)
                continue
            if size > max_size:
                missing[kind].append(blob_hash)
                continue
            verify_bytes += size
            verify.append((kind, blob_hash, base_dir, label, max_size))
    if verify_bytes > MAX_BLOB_COVERAGE_VERIFY_BYTES:
        raise ClusterSyncCommandError("missing-blobs verification budget exceeded")
    for kind, blob_hash, base_dir, label, max_size in verify:
        try:
            _read_verified_blob(base_dir, blob_hash, label, max_size=max_size)
        except ClusterSyncCommandError:
            missing[kind].append(blob_hash)
    for hashes in missing.values():
        hashes.sort()
    return {"ok": True, "requested": total, "missing": missing}


def _repair_config_blob_response_from_run_v7(cluster_root: Path, requested_hash: str) -> bool:
    """Rebuild a missing config blob from matching local run_v7 files."""

    requested = _validate_hash(requested_hash)
    materialized = _safe_state_call(lambda: rebuild_materialized_state(cluster_root, write=False))
    desired_state = materialized.get("desired_state") if isinstance(materialized, dict) else {}
    desired_state = desired_state if isinstance(desired_state, dict) else {}
    instances = desired_state.get("instances") if isinstance(desired_state, dict) else {}
    instances = instances if isinstance(instances, dict) else {}
    run_root = Path(cluster_root).parent / "run_v7"
    paths = ClusterPaths.from_root(cluster_root)
    for instance in sorted(str(name) for name in instances if str(name)):
        item = instances.get(instance) if isinstance(instances.get(instance), dict) else {}
        raw_manifest_hash = str(item.get("config_manifest_hash") or "")
        if not raw_manifest_hash:
            continue
        try:
            manifest_hash = _validate_hash(raw_manifest_hash)
        except ClusterSyncCommandError:
            continue
        _validate_relative_name(instance, "instance")
        instance_dir = run_root / instance
        if not instance_dir.is_dir():
            continue
        try:
            manifest = build_config_manifest(instance_dir)
        except (ClusterStateError, OSError):
            continue
        if compute_config_manifest_hash(manifest) != manifest_hash:
            continue
        files = manifest.get("files") if isinstance(manifest, dict) else {}
        files = files if isinstance(files, dict) else {}
        file_hashes = {
            f"sha256:{sha}"
            for meta in files.values()
            for sha in [str((meta if isinstance(meta, dict) else {}).get("sha256") or "")]
            if sha
        }
        if requested != manifest_hash and requested not in file_hashes:
            continue
        manifest_raw = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode("utf-8")
        _write_blob(paths.config_blobs, manifest_hash, manifest_raw, MAX_CONFIG_BLOB_BYTES, secret=False)
        for filename, meta in files.items():
            _validate_relative_name(str(filename), "config filename")
            sha = str((meta if isinstance(meta, dict) else {}).get("sha256") or "")
            blob_hash = _validate_hash(f"sha256:{sha}")
            _write_blob(paths.config_blobs, blob_hash, (instance_dir / str(filename)).read_bytes(), MAX_CONFIG_BLOB_BYTES, secret=False)
        return True
    return False


def _verify_remote_node(cluster_root: Path, remote_node: str, *, allow_join: bool) -> None:
    """Ensure the caller is a known node unless this is an explicit join."""

    del allow_join
    materialized = _read_materialized_snapshot(cluster_root)
    nodes = ((materialized.get("cluster_nodes") or {}).get("nodes") or {})
    node = nodes.get(remote_node)
    if not isinstance(node, dict):
        materialized = _safe_state_call(lambda: rebuild_materialized_state(cluster_root, write=False))
        nodes = ((materialized.get("cluster_nodes") or {}).get("nodes") or {})
        node = nodes.get(remote_node)
    if not isinstance(node, dict):
        raise ClusterSyncCommandError("remote node is not registered")
    if node.get("enabled", True) is False:
        raise ClusterSyncCommandError("remote node is disabled")
    if node.get("state_replica", True) is False:
        raise ClusterSyncCommandError("remote node is not a state replica")
    cutoff = ((materialized.get("desired_state") or {}).get("credential_migration") or {}).get("cutoff")
    if isinstance(cutoff, dict) and int(node.get("credential_protocol_version") or 0) < int(cutoff.get("min_protocol") or 2):
        raise ClusterSyncCommandError("credential protocol downgrade rejected after cutoff")


def _materialize_v7_configs(cluster_root: Path, *, write: bool) -> dict[str, Any]:
    """Preview or write V7 config blobs into data/run_v7."""

    materialized = _safe_state_call(
        lambda: rebuild_materialized_state(cluster_root) if write else read_materialized_state(cluster_root)
    )
    identity = _safe_state_call(lambda: read_local_identity(cluster_root))
    desired_state = materialized.get("desired_state") if isinstance(materialized, dict) else {}
    desired_state = desired_state if isinstance(desired_state, dict) else {}
    node_id = str(identity["node_id"])
    role = str(identity.get("role") or "").strip().lower()
    materialize_all = role == "master"
    run_root = Path(cluster_root).parent / "run_v7"
    plan = _build_materialize_v7_plan(Path(cluster_root), run_root, node_id, desired_state, materialize_all=materialize_all)
    if not write:
        plan.update({"ok": True, "read_only": True})
        return plan

    if int((plan.get("counts") or {}).get("error") or 0) > 0:
        repaired = _repair_local_v7_config_blobs(Path(cluster_root), run_root, node_id, desired_state, plan, materialize_all=materialize_all)
        if repaired:
            plan = _build_materialize_v7_plan(Path(cluster_root), run_root, node_id, desired_state, materialize_all=materialize_all)
            plan["repaired_config_blobs"] = repaired

    if int((plan.get("counts") or {}).get("error") or 0) > 0:
        raise ClusterSyncCommandError("materialization blocked by missing or invalid blobs")

    written: list[dict[str, Any]] = []
    deleted: list[dict[str, Any]] = []
    for item in plan.get("items") or []:
        if not isinstance(item, dict) or item.get("action") not in {"add", "update"} or item.get("status") != "ready":
            continue
        instance = str(item.get("instance") or "")
        _validate_relative_name(instance, "instance")
        files_written = 0
        for file_item in item.get("files") or []:
            if not isinstance(file_item, dict) or file_item.get("action") != "write":
                continue
            filename = str(file_item.get("name") or "")
            blob_hash = str(file_item.get("hash") or "")
            _validate_relative_name(filename, "config filename")
            raw = _read_verified_blob(ClusterPaths.from_root(cluster_root).config_blobs, blob_hash, f"file blob for {instance}/{filename}")
            _atomic_write_bytes(run_root / instance / filename, raw, mode=0o644)
            files_written += 1
        written.append({"instance": instance, "files": files_written})
    for item in plan.get("items") or []:
        if not isinstance(item, dict) or item.get("action") != "delete" or item.get("status") != "ready":
            continue
        instance = str(item.get("instance") or "")
        _validate_relative_name(instance, "instance")
        target = run_root / instance
        if not target.is_dir():
            continue
        backup_path = _backup_and_delete_tombstoned_v7_dir(Path(cluster_root), target, instance)
        deleted.append({"instance": instance, "backup": str(backup_path)})

    counts = dict(plan.get("counts") or {})
    counts["written_instances"] = len(written)
    counts["written_files"] = sum(int(item.get("files") or 0) for item in written)
    counts["deleted_instances"] = len(deleted)
    plan.update({
        "ok": True,
        "read_only": False,
        "counts": counts,
        "written": written,
        "deleted": deleted,
        "message": "V7 config files were materialized. Tombstoned local config directories were backed up and removed. No bots were started or stopped.",
    })
    return plan


def _backup_and_delete_tombstoned_v7_dir(cluster_root: Path, target: Path, instance: str) -> Path:
    """Back up then remove a local run_v7 directory covered by a Cluster tombstone."""

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    backup_root = Path(cluster_root).parent / "backup" / "v7" / instance
    backup_root.mkdir(parents=True, exist_ok=True)
    backup_path = backup_root / f"cluster_tombstone_{timestamp}"
    if backup_path.exists():
        backup_path = backup_root / f"cluster_tombstone_{timestamp}_{uuid.uuid4().hex[:8]}"
    shutil.copytree(target, backup_path)
    shutil.rmtree(target)
    return backup_path


def _repair_local_v7_config_blobs(
    cluster_root: Path,
    run_root: Path,
    node_id: str,
    desired_state: dict[str, Any],
    plan: dict[str, Any],
    *,
    materialize_all: bool = False,
) -> int:
    """Rebuild missing local config blobs from already-materialized run_v7 files."""

    instances = desired_state.get("instances") if isinstance(desired_state, dict) else {}
    instances = instances if isinstance(instances, dict) else {}
    error_instances = {
        str(item.get("instance") or "")
        for item in (plan.get("items") or [])
        if isinstance(item, dict) and item.get("status") == "error"
    }
    paths = ClusterPaths.from_root(cluster_root)
    repaired = 0
    for instance in sorted(name for name in error_instances if name):
        item = instances.get(instance) if isinstance(instances.get(instance), dict) else {}
        if not materialize_all and str(item.get("assigned_host") or "") != node_id:
            continue
        manifest_hash = _validate_hash(str(item.get("config_manifest_hash") or ""))
        instance_dir = run_root / instance
        if not instance_dir.is_dir():
            continue
        manifest = build_config_manifest(instance_dir)
        if compute_config_manifest_hash(manifest) != manifest_hash:
            continue
        manifest_raw = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode("utf-8")
        _write_blob(paths.config_blobs, manifest_hash, manifest_raw, MAX_CONFIG_BLOB_BYTES, secret=False)
        files = manifest.get("files") if isinstance(manifest, dict) else {}
        files = files if isinstance(files, dict) else {}
        for filename, meta in files.items():
            _validate_relative_name(str(filename), "config filename")
            sha = str((meta if isinstance(meta, dict) else {}).get("sha256") or "")
            blob_hash = _validate_hash(f"sha256:{sha}")
            _write_blob(paths.config_blobs, blob_hash, (instance_dir / str(filename)).read_bytes(), MAX_CONFIG_BLOB_BYTES, secret=False)
        repaired += 1
    return repaired


def _materialize_api_keys(cluster_root: Path, *, write: bool) -> dict[str, Any]:
    """Preview or merge exchange keys from the desired secret blob."""

    materialized = _safe_state_call(
        lambda: rebuild_materialized_state(cluster_root) if write else read_materialized_state(cluster_root)
    )
    identity = _safe_state_call(lambda: read_local_identity(cluster_root))
    desired_state = materialized.get("desired_state") if isinstance(materialized, dict) else {}
    desired_state = desired_state if isinstance(desired_state, dict) else {}
    plan = _build_materialize_api_keys_plan(Path(cluster_root), desired_state, str(identity["node_id"]))
    if not write:
        plan.update({"ok": True, "read_only": True})
        return plan
    if not plan.get("can_apply"):
        raise ClusterSyncCommandError(str(plan.get("reason") or "api-keys materialization is not ready"))

    target = Path(str(plan.get("path") or ""))
    secret_hash = str(plan.get("secret_blob_hash") or "")
    raw = _read_verified_blob(ClusterPaths.from_root(cluster_root).secret_blobs, secret_hash, "api-keys secret blob")
    source_payload = _read_json_payload(raw, MAX_SECRET_BLOB_BYTES)
    writer = PB7ApiKeysMergeWriter(
        target,
        Path(cluster_root).parent / "credentials" / "pb7_projection.json",
    )
    current = writer.read()
    needs_replacement_backup = bool(
        target.is_file() and exchange_payload(current) != exchange_payload(source_payload)
    )
    is_vps_runner = str(identity.get("role") or "").strip().lower() == "vps"
    backup_file = None
    if not is_vps_runner and needs_replacement_backup:
        backup_dir = Path(PBGDIR) / "data" / "api-keys"
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        backup_file = backup_dir / f"api-keys7_cluster-materialize_{timestamp}.json"
        plan["backup"] = str(backup_file)
    elif is_vps_runner and needs_replacement_backup:
        plan["backup_skipped"] = "vps_runner"
    writer.write_exchange_payload(
        source_payload,
        expected_generation=int(current.get("_api_serial") or 0),
        backup_path=backup_file,
    )
    if exchange_payload(writer.read()) != exchange_payload(source_payload):
        raise ClusterSyncCommandError("exchange API-key merge verification failed")

    counts = dict(plan.get("counts") or {})
    counts["written"] = 1
    plan.update({
        "ok": True,
        "read_only": False,
        "counts": counts,
        "action": "write",
        "status": "written",
        "message": "Exchange API keys were merged from Cluster Sync. No bots were restarted.",
    })
    return plan


def _build_materialize_api_keys_plan(cluster_root: Path, desired_state: dict[str, Any], node_id: str) -> dict[str, Any]:
    """Build a read-only plan for api-keys.json materialization."""

    api_keys = desired_state.get("api_keys") if isinstance(desired_state, dict) else None
    target = _api_keys_target_path()
    base: dict[str, Any] = {
        "cluster_id": str(desired_state.get("cluster_id") or ""),
        "node_id": node_id,
        "path": str(target),
        "counts": {"write": 0, "current": 0, "error": 0, "missing": 0, "written": 0},
        "can_apply": False,
    }
    if not isinstance(api_keys, dict):
        base.update({"action": "skip", "status": "missing", "reason": "desired state has no api_keys metadata"})
        base["counts"]["missing"] = 1
        return base

    secret_hash = str(api_keys.get("secret_blob_hash") or "")
    payload_hash = str(api_keys.get("payload_hash") or "")
    base.update({
        "serial": int(api_keys.get("serial") or 0),
        "payload_hash": payload_hash,
        "secret_blob_hash": secret_hash,
    })
    try:
        raw = _read_verified_blob(ClusterPaths.from_root(cluster_root).secret_blobs, secret_hash, "api-keys secret blob")
        source_payload = _read_json_payload(raw, MAX_SECRET_BLOB_BYTES)
        writer = PB7ApiKeysMergeWriter(
            target,
            Path(cluster_root).parent / "credentials" / "pb7_projection.json",
        )
        current_payload = writer.read()
        if exchange_payload(current_payload) == exchange_payload(source_payload):
            base.update({"action": "skip", "status": "current", "reason": "exchange API keys already match desired state"})
            base["counts"]["current"] = 1
        else:
            base.update({"action": "write", "status": "ready", "reason": "exchange API keys differ from desired state", "can_apply": True})
            base["counts"]["write"] = 1
    except Exception as exc:
        base.update({"action": "skip", "status": "error", "reason": str(exc)})
        base["counts"]["error"] = 1
    return base


def _materialize_credentials(cluster_root: Path, *, write: bool) -> dict[str, Any]:
    """Preview or materialize sealed CMC/TradFi credentials locally."""

    materialized = _safe_state_call(
        lambda: rebuild_materialized_state(cluster_root) if write else read_materialized_state(cluster_root)
    )
    identity = _safe_state_call(lambda: read_local_identity(cluster_root))
    desired = materialized.get("desired_state") if isinstance(materialized, dict) else {}
    desired = desired if isinstance(desired, dict) else {}
    secrets = desired.get("secrets") if isinstance(desired.get("secrets"), dict) else {}
    active_tradfi = desired.get("tradfi_active_profiles")
    active_tradfi = active_tradfi if isinstance(active_tradfi, dict) else {}
    active_tradfi_ids = {
        str(provider): str(item.get("profile_id") or "")
        for provider, item in active_tradfi.items()
        if isinstance(item, dict) and not item.get("conflicted") and item.get("profile_id")
    }
    if not active_tradfi_ids:
        for secret_id, secret in sorted(secrets.items()):
            if (
                isinstance(secret, dict)
                and str(secret.get("secret_kind") or "") == "tradfi_profile"
                and "lifecycle_state" not in secret
                and secret.get("provider")
            ):
                active_tradfi_ids.setdefault(str(secret["provider"]), str(secret_id))
    tombstones = (
        desired.get("secret_tombstones")
        if isinstance(desired.get("secret_tombstones"), dict)
        else {}
    )
    node_id = str(identity["node_id"])
    role = str(identity.get("role") or "").strip().lower()
    membership_generation = int(
        (materialized.get("cluster_nodes") or {}).get(
            "credential_membership_generation", 0
        )
    )
    membership_nodes = ((materialized.get("cluster_nodes") or {}).get("nodes") or {})
    acknowledgements = desired.get("credential_materialization_acks")
    acknowledgements = acknowledgements if isinstance(acknowledgements, dict) else {}
    local_ack = acknowledgements.get(node_id) if isinstance(acknowledgements.get(node_id), dict) else {}
    credentials_root = Path(cluster_root).parent / "credentials"
    store = CredentialStore(credentials_root) if write or credentials_root.exists() else None
    counts = {
        "ready": 0,
        "current": 0,
        "written": 0,
        "not_recipient": 0,
        "conflicted": 0,
        "error": 0,
        "tombstoned": 0,
    }
    items: list[dict[str, Any]] = []
    pending_tombstones = 0

    for secret_id in sorted(secrets):
        secret = secrets.get(secret_id) if isinstance(secrets.get(secret_id), dict) else {}
        kind = str(secret.get("secret_kind") or "")
        generation = int(secret.get("generation") or 0)
        item: dict[str, Any] = {
            "secret_id": str(secret_id),
            "secret_kind": kind,
            "generation": generation,
            "recipient_generation": int(secret.get("recipient_generation") or 1),
            "audience": str(secret.get("audience") or ""),
            "action": "skip",
        }
        try:
            if secret.get("conflicted") is True:
                item.update({"status": "conflicted", "reason": "credential generation is conflicted"})
                counts["conflicted"] += 1
                items.append(item)
                continue
            expected_recipient_ids = sorted(str(value) for value in secret.get("recipient_ids") or [])
            if expected_recipient_ids and node_id not in set(expected_recipient_ids):
                item.update({"status": "not_recipient", "reason": "local node is not an intended recipient"})
                counts["not_recipient"] += 1
                items.append(item)
                continue
            if kind == "tradfi_profile" and role != "master":
                item.update({"status": "not_recipient", "reason": "TradFi credentials are master-only"})
                counts["not_recipient"] += 1
                items.append(item)
                continue
            current_generation = _credential_store_generation(store, str(secret_id), kind)
            current_origin = ""
            if store is not None and current_generation > 0:
                try:
                    current_record = (
                        store.get_cmc(str(secret_id))
                        if kind == "cmc_api_key"
                        else store.get_tradfi(str(secret_id))
                    )
                    current_origin = str(current_record.get("origin") or "")
                except (KeyError, ValueError):
                    current_origin = ""
            requires_cluster_promotion = (
                current_generation >= generation and current_origin != "cluster"
            )
            acked_recipient_generation = int(
                (local_ack.get("recipient_generations") or {}).get(str(secret_id)) or 0
            )
            recipient_generation = int(secret.get("recipient_generation") or 1)
            recipient_current = (
                int(local_ack.get("membership_generation") or -1) == membership_generation
                and acked_recipient_generation == recipient_generation
            )
            if (
                expected_recipient_ids
                and current_generation >= generation
                and recipient_current
                and not requires_cluster_promotion
            ):
                item.update({"status": "current", "reason": "credential generation is already materialized"})
                counts["current"] += 1
                items.append(item)
                continue
            context = SecretContext(
                cluster_id=str(desired.get("cluster_id") or ""),
                secret_id=str(secret_id),
                kind=kind,
                generation=generation,
                audience=str(secret.get("audience") or ""),
            )
            blob_hash = str(secret.get("sealed_blob_hash") or "")
            raw = _read_verified_blob(
                ClusterPaths.from_root(cluster_root).sealed_blobs,
                blob_hash,
                f"sealed credential blob for {secret_id}",
                max_size=MAX_SEALED_BLOB_BYTES,
            )
            envelope = _validate_sealed_blob_payload(
                cluster_root,
                raw,
                expected_context=context,
                membership_nodes=membership_nodes,
            )
            if str(envelope.get("signer_id") or "") != str(secret.get("updated_by") or ""):
                raise ClusterSyncCommandError("sealed credential signer does not match desired-state actor")
            recipient_ids = {str(entry.get("node_id") or "") for entry in envelope["recipients"]}
            if expected_recipient_ids and sorted(recipient_ids) != expected_recipient_ids:
                raise ClusterSyncCommandError(
                    "sealed credential recipients do not match desired recipient state"
                )
            if node_id not in recipient_ids:
                item.update({"status": "not_recipient", "reason": "local node is not an intended recipient"})
                counts["not_recipient"] += 1
                items.append(item)
                continue
            if (
                current_generation >= generation
                and recipient_current
                and not requires_cluster_promotion
            ):
                item.update({"status": "current", "reason": "credential generation is already materialized"})
                counts["current"] += 1
            else:
                item.update({
                    "action": "write" if current_generation < generation else "verify",
                    "status": "ready",
                    "reason": "sealed credential recipient generation is ready",
                })
                counts["ready"] += 1
                if write:
                    plaintext = _open_with_local_key_history(
                        cluster_root,
                        envelope,
                        node_id,
                        _membership_signing_key(
                            cluster_root,
                            str(envelope["signer_id"]),
                            str(envelope.get("signing_key_id") or ""),
                        ),
                        context,
                        _credential_membership_roles(materialized),
                    )
                    value, metadata = _decode_materialized_credential(kind, plaintext, secret)
                    if kind == "tradfi_profile":
                        metadata["active"] = (
                            active_tradfi_ids.get(str(metadata.get("provider") or "")) == str(secret_id)
                        )
                    assert store is not None
                    store.materialize_cluster_secret(
                        str(secret_id),
                        kind,
                        generation,
                        value,
                        metadata=metadata,
                    )
                    item.update({
                        "action": "write" if current_generation < generation else "verify",
                        "status": "written",
                        "reason": "credential recipient generation materialized",
                    })
                    counts["written"] += 1
        except NotRecipientError:
            item.update({"status": "not_recipient", "reason": "local node is not an intended recipient"})
            counts["not_recipient"] += 1
        except Exception as exc:
            item.update({"status": "error", "reason": _credential_error_text(exc)})
            counts["error"] += 1
        items.append(item)

    if not write and store is not None:
        for secret_id in sorted(tombstones):
            tombstone = tombstones.get(secret_id) if isinstance(tombstones.get(secret_id), dict) else {}
            kind = str(tombstone.get("secret_kind") or _credential_kind_from_id(str(secret_id)) or "")
            if kind and _credential_store_generation(store, str(secret_id), kind) > 0:
                pending_tombstones += 1

    if write:
        assert store is not None
        for secret_id in sorted(tombstones):
            tombstone = tombstones.get(secret_id) if isinstance(tombstones.get(secret_id), dict) else {}
            kind = str(tombstone.get("secret_kind") or _credential_kind_from_id(str(secret_id)) or "")
            if kind and store.tombstone_cluster_secret(str(secret_id), kind):
                counts["tombstoned"] += 1
        store.apply_cluster_tradfi_selection(active_tradfi_ids)

    tradfi_projection: dict[str, Any] | None = None
    projection_retry_ready = False
    has_tradfi_state = bool(active_tradfi) or any(
        str((item if isinstance(item, dict) else {}).get("secret_kind") or "") == "tradfi_profile"
        for item in secrets.values()
    ) or any(
        str((item if isinstance(item, dict) else {}).get("secret_kind") or "") == "tradfi_profile"
        for item in tombstones.values()
    )
    if has_tradfi_state and role != "master":
        tradfi_projection = {
            "status": "not_recipient",
            "reason": "TradFi projection is master-only",
        }
    elif has_tradfi_state:
        configured_pb7_dir = str(pb7dir() or "").strip()
        if not configured_pb7_dir or not Path(configured_pb7_dir).is_dir():
            tradfi_projection = {
                "status": "pending",
                "reason": "PB7 directory is not configured",
            }
        elif not write:
            projection_writer = PB7ApiKeysMergeWriter(
                Path(configured_pb7_dir) / "api-keys.json",
                Path(cluster_root).parent / "credentials" / "pb7_projection.json",
            )
            tradfi_projection = projection_writer.projection_status()
            active_fingerprint = build_tradfi_projection(store)[1] if store is not None else ""
            projection_retry_ready = (
                str(tradfi_projection.get("status") or "") != "current"
                or str(tradfi_projection.get("source_fingerprint") or "") != active_fingerprint
            )
        else:
            try:
                assert store is not None
                tradfi_projection = project_active_tradfi_profiles(
                    store,
                    Path(configured_pb7_dir) / "api-keys.json",
                )
            except Exception as exc:
                counts["error"] += 1
                tradfi_projection = {
                    "status": "error",
                    "reason": f"TradFi projection failed ({type(exc).__name__})",
                }

    result = {
        "ok": counts["error"] == 0,
        "read_only": not write,
        "cluster_id": str(desired.get("cluster_id") or ""),
        "node_id": node_id,
        "role": role,
        "counts": counts,
        "items": items,
        "tradfi_projection": tradfi_projection,
        "can_apply": counts["error"] == 0 and (
            counts["ready"] > 0 or pending_tombstones > 0 or projection_retry_ready
        ),
    }
    if write:
        result["migration_ack"] = _append_credential_migration_acks(cluster_root)
        if (
            role == "master"
            and isinstance(tradfi_projection, dict)
            and str(tradfi_projection.get("status") or "") == "current"
        ):
            result["tradfi_projection_ack"] = _append_tradfi_projection_ack(
                cluster_root,
                tradfi_projection,
            )
    return result


def _append_tradfi_projection_ack(
    cluster_root: Path,
    projection: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Append the local master's exact active-profile projection ACK."""

    materialized = read_materialized_state(cluster_root)
    identity = read_local_identity(cluster_root)
    node_id = str(identity["node_id"])
    local_node = (((materialized.get("cluster_nodes") or {}).get("nodes") or {}).get(node_id) or {})
    if str(local_node.get("role") or "").strip().lower() != "master":
        return {"status": "not_recipient"}
    desired = materialized.get("desired_state") or {}
    active_profiles = desired.get("tradfi_active_profiles")
    active_profiles = active_profiles if isinstance(active_profiles, dict) else {}
    expected = {
        str(provider): int(item.get("activation_generation") or 0)
        for provider, item in active_profiles.items()
        if isinstance(item, dict) and not item.get("conflicted")
    }
    status = dict(projection or {})
    if not status:
        configured_pb7_dir = str(pb7dir() or "").strip()
        if not configured_pb7_dir or not Path(configured_pb7_dir).is_dir():
            return {"status": "pending", "reason": "PB7 directory is not configured"}
        status = PB7ApiKeysMergeWriter(
            Path(configured_pb7_dir) / "api-keys.json",
            Path(cluster_root).parent / "credentials" / "pb7_projection.json",
        ).projection_status()
    if str(status.get("status") or "") != "current":
        return {"status": "pending", "reason": "TradFi projection is not current"}
    applied_generation = int(status.get("applied_generation") or 0)
    if applied_generation < 1:
        return {"status": "pending", "reason": "TradFi projection generation is unavailable"}
    membership_generation = int(
        (materialized.get("cluster_nodes") or {}).get("credential_membership_generation") or 0
    )
    acknowledgements = desired.get("tradfi_projection_acks")
    acknowledgements = acknowledgements if isinstance(acknowledgements, dict) else {}
    current = acknowledgements.get(node_id) if isinstance(acknowledgements.get(node_id), dict) else {}
    if (
        int(current.get("membership_generation") or -1) == membership_generation
        and current.get("active_profile_generations", {}) == expected
        and int(current.get("projection_applied_generation") or 0) == applied_generation
        and str(current.get("projection_status") or "") == "current"
    ):
        return {"status": "already_acked", "operation_id": str(current.get("op_id") or "")}
    operation = append_operation(
        cluster_root,
        "TRADFI_PROJECTION_ACK",
        {
            "node_id": node_id,
            "membership_generation": membership_generation,
            "active_profile_generations": expected,
            "projection_applied_generation": applied_generation,
            "projection_status": "current",
        },
    )
    return {"status": "acked", "operation_id": str(operation["op_id"])}


def _append_credential_migration_acks(
    cluster_root: Path,
    *,
    inventory_allowed: bool = True,
    scan_allowed: bool = True,
) -> dict[str, Any]:
    """Append exact recipient ACKs plus migration ACKs when their barriers are ready."""

    materialized = read_materialized_state(cluster_root)
    desired = materialized.get("desired_state") if isinstance(materialized, dict) else {}
    desired = desired if isinstance(desired, dict) else {}
    identity = read_local_identity(cluster_root)
    node_id = str(identity["node_id"])
    role = str(identity.get("role") or "").strip().lower()
    membership_generation = int(
        (materialized.get("cluster_nodes") or {}).get(
            "credential_membership_generation", 0
        )
    )
    secrets = desired.get("secrets") if isinstance(desired.get("secrets"), dict) else {}
    expected = {
        str(secret_id): int(secret.get("generation") or 0)
        for secret_id, secret in secrets.items()
        if isinstance(secret, dict)
        and node_id in set(secret.get("recipient_ids") or [])
    }
    expected_recipients = {
        str(secret_id): int(secret.get("recipient_generation") or 1)
        for secret_id, secret in secrets.items()
        if isinstance(secret, dict)
        and node_id in set(secret.get("recipient_ids") or [])
    }
    migration = desired.get("credential_migration")
    migration = migration if isinstance(migration, dict) else {}
    store = CredentialStore(Path(cluster_root).parent / "credentials")
    for secret_id, generation in expected.items():
        kind = str((secrets.get(secret_id) or {}).get("secret_kind") or "")
        if _credential_store_generation(store, secret_id, kind) != generation:
            return {
                "status": "pending",
                "reason": "local credential generation is not materialized",
                "credential_id": secret_id,
            }

    acknowledgements = desired.get("credential_materialization_acks")
    acknowledgements = acknowledgements if isinstance(acknowledgements, dict) else {}
    current_ack = acknowledgements.get(node_id) if isinstance(acknowledgements.get(node_id), dict) else {}
    recipient_ack: dict[str, Any]
    if (
        int(current_ack.get("membership_generation") or -1) == membership_generation
        and current_ack.get("credential_generations", {}) == expected
        and current_ack.get("recipient_generations", {}) == expected_recipients
    ):
        recipient_ack = {"status": "already_acked"}
    elif migration.get("frozen") is True:
        recipient_ack = {"status": "deferred"}
    else:
        ack_payload: dict[str, Any] = {
            "node_id": node_id,
            "membership_generation": membership_generation,
            "credential_generations": expected,
            "recipient_generations": expected_recipients,
        }
        operation = append_operation(
            cluster_root,
            "CREDENTIAL_MATERIALIZATION_ACK",
            ack_payload,
        )
        recipient_ack = {"status": "acked", "operation_id": str(operation["op_id"])}

    if migration.get("frozen") is True and recipient_ack["status"] == "acked":
        materialized = read_materialized_state(cluster_root)
        desired = materialized["desired_state"]
        migration = desired["credential_migration"]

    if migration.get("frozen") is not True:
        return {
            "status": recipient_ack["status"],
            "recipient_ack": recipient_ack,
            "credential_generations": expected,
            "recipient_generations": expected_recipients,
        }

    from credential_process_registry import process_barrier_readiness

    process_readiness = process_barrier_readiness(Path(cluster_root).parent.parent)
    if not process_readiness["ready"]:
        return {
            "status": "waiting_for_upgrade",
            "recipient_ack": recipient_ack,
            "credential_generations": expected,
            "recipient_generations": expected_recipients,
            "waiting_services": process_readiness["waiting_services"],
        }

    freeze_generation = int(migration.get("freeze_generation") or 0)
    freeze_acks = migration.get("freeze_acks")
    freeze_acks = freeze_acks if isinstance(freeze_acks, dict) else {}
    freeze_ack = freeze_acks.get(node_id) if isinstance(freeze_acks.get(node_id), dict) else {}
    if (
        int(freeze_ack.get("freeze_generation") or 0) != freeze_generation
        or freeze_ack.get("frozen") is not True
    ):
        append_operation(
            cluster_root,
            "WRITER_FREEZE_ACK",
            {
                "freeze_generation": freeze_generation,
                "node_id": node_id,
                "frozen": True,
                "process_readiness": process_readiness["services"],
            },
        )
        materialized = read_materialized_state(cluster_root)
        desired = materialized["desired_state"]
        migration = desired["credential_migration"]

    active_nodes = {
        str(active_id)
        for active_id, active_node in (((materialized.get("cluster_nodes") or {}).get("nodes") or {}).items())
        if isinstance(active_node, dict)
        and active_node.get("enabled", True) is not False
        and active_node.get("state_replica", True) is not False
        and str(active_node.get("role") or "") in {"master", "vps"}
    }
    freeze_acks = migration.get("freeze_acks") if isinstance(migration.get("freeze_acks"), dict) else {}
    if not active_nodes.issubset(freeze_acks):
        return {
            "status": "frozen",
            "recipient_ack": recipient_ack,
            "credential_generations": expected,
            "recipient_generations": expected_recipients,
            "inventory_status": "waiting_for_freeze_acks",
        }
    if not inventory_allowed:
        return {
            "status": "frozen",
            "recipient_ack": recipient_ack,
            "credential_generations": expected,
            "recipient_generations": expected_recipients,
            "inventory_status": "ready",
        }

    from credential_migration import local_legacy_credential_inventory

    source_fingerprints, source_generations = local_legacy_credential_inventory(
        Path(cluster_root).parent.parent,
        require_barrier=True,
    )
    inventory_acks = migration.get("inventory_acks") if isinstance(migration.get("inventory_acks"), dict) else {}
    inventory_ack = inventory_acks.get(node_id) if isinstance(inventory_acks.get(node_id), dict) else {}
    if (
        int(inventory_ack.get("freeze_generation") or 0) != freeze_generation
        or inventory_ack.get("source_fingerprints", {}) != source_fingerprints
        or inventory_ack.get("source_generations", {}) != source_generations
    ):
        append_operation(
            cluster_root,
            "CREDENTIAL_INVENTORY_ACK",
            {
                "freeze_generation": freeze_generation,
                "node_id": node_id,
                "source_fingerprints": source_fingerprints,
                "source_generations": source_generations,
                "process_readiness": process_readiness["services"],
            },
        )
        materialized = read_materialized_state(cluster_root)
        desired = materialized["desired_state"]
        migration = desired["credential_migration"]

    materialization_acks = migration.get("materialization_acks")
    materialization_acks = materialization_acks if isinstance(materialization_acks, dict) else {}
    current = (
        materialization_acks.get(node_id)
        if isinstance(materialization_acks.get(node_id), dict)
        else {}
    )
    if (
        int(current.get("freeze_generation") or 0) == freeze_generation
        and current.get("credential_generations", {}) == expected
    ):
        result = {
            "status": "acked" if recipient_ack["status"] == "acked" else "already_acked",
            "recipient_ack": recipient_ack,
            "credential_generations": expected,
            "recipient_generations": expected_recipients,
        }
        if recipient_ack.get("operation_id"):
            result["operation_id"] = recipient_ack["operation_id"]
        result.update(_append_cutoff_cleanup_ack(cluster_root, node_id, migration))
        if scan_allowed:
            result.update(_append_credential_scan_ack(cluster_root, node_id))
        return result
    operation = append_operation(
        cluster_root,
        "CREDENTIAL_MATERIALIZATION_ACK",
        {
            "freeze_generation": freeze_generation,
            "node_id": node_id,
            "membership_generation": membership_generation,
            "credential_generations": expected,
            "recipient_generations": expected_recipients,
        },
    )
    result = {
        "status": "acked",
        "recipient_ack": recipient_ack,
        "operation_id": str(operation["op_id"]),
        "credential_generations": expected,
        "recipient_generations": expected_recipients,
    }
    result.update(_append_cutoff_cleanup_ack(cluster_root, node_id, migration))
    if scan_allowed:
        result.update(_append_credential_scan_ack(cluster_root, node_id))
    return result


def _append_cutoff_cleanup_ack(
    cluster_root: Path,
    node_id: str,
    migration: dict[str, Any],
) -> dict[str, Any]:
    """Clean exact cutoff blobs and append this replica's idempotent ACK."""

    result: dict[str, Any] = {}
    cutoff = migration.get("cutoff") if isinstance(migration.get("cutoff"), dict) else None
    if cutoff is not None:
        cleanup_acks = migration.get("cleanup_acks") if isinstance(migration.get("cleanup_acks"), dict) else {}
        cutoff_generation = int(cutoff.get("cutoff_generation") or 0)
        state_vector = dict(cutoff.get("state_vector") or {})
        current_cleanup = cleanup_acks.get(node_id) if isinstance(cleanup_acks.get(node_id), dict) else {}
        if (
            int(current_cleanup.get("cutoff_generation") or 0) != cutoff_generation
            or current_cleanup.get("state_vector", {}) != state_vector
        ):
            removed = _cleanup_cutoff_secret_blobs(cluster_root, cutoff)
            cleanup_operation = append_operation(
                cluster_root,
                "CREDENTIAL_CUTOFF_ACK",
                {
                    "cutoff_generation": cutoff_generation,
                    "node_id": node_id,
                    "state_vector": state_vector,
                    "removed_secret_blob_hashes": removed,
                },
            )
            result["cleanup_operation_id"] = str(cleanup_operation["op_id"])
            result["removed_secret_blob_hashes"] = removed
    return result


def _cleanup_cutoff_secret_blobs(cluster_root: Path, cutoff: dict[str, Any]) -> list[str]:
    """Delete only local blobs explicitly made obsolete by the signed cutoff."""

    removed: list[str] = []
    root = ClusterPaths.from_root(cluster_root).secret_blobs
    for blob_hash in cutoff.get("obsolete_secret_blob_hashes") or []:
        try:
            digest = _validate_hash(str(blob_hash)).removeprefix("sha256:")
            path = root / "sha256" / digest[:2] / f"{digest}.json"
        except Exception:
            continue
        if path.is_file() and not path.is_symlink():
            path.unlink()
            removed.append(str(blob_hash))
    return sorted(removed)


def _append_credential_scan_ack(cluster_root: Path, node_id: str) -> dict[str, Any]:
    """Run one bounded redacted local scan and ACK the current migration barrier."""

    materialized = read_materialized_state(cluster_root)
    migration = ((materialized.get("desired_state") or {}).get("credential_migration") or {})
    cutoff = migration.get("cutoff") if isinstance(migration.get("cutoff"), dict) else None
    if migration.get("frozen") is not True or cutoff is None:
        return {}
    freeze_generation = int(migration.get("freeze_generation") or 0)
    cutoff_generation = int(cutoff.get("cutoff_generation") or 0)
    cleanup = migration.get("cleanup_acks") if isinstance(migration.get("cleanup_acks"), dict) else {}
    local_cleanup = cleanup.get(node_id) if isinstance(cleanup.get(node_id), dict) else {}
    if int(local_cleanup.get("cutoff_generation") or 0) != cutoff_generation:
        return {"scan_status": "pending"}
    scan_acks = migration.get("scan_acks") if isinstance(migration.get("scan_acks"), dict) else {}
    current = scan_acks.get(node_id) if isinstance(scan_acks.get(node_id), dict) else {}
    current_matches = (
        int(current.get("freeze_generation") or 0) == freeze_generation
        and int(current.get("cutoff_generation") or 0) == cutoff_generation
    )
    current_status = str(current.get("status") or "")
    if current_matches and current_status == "clean":
        return {"scan_status": "clean"}

    from credential_migration import local_managed_credential_scan

    findings = local_managed_credential_scan(Path(cluster_root).parent.parent)
    status = "blocked" if findings else "clean"
    expected_findings = [{"path_category": item} for item in findings]
    if (
        int(current.get("freeze_generation") or 0) == freeze_generation
        and int(current.get("cutoff_generation") or 0) == cutoff_generation
        and str(current.get("status") or "") == status
        and current.get("findings", []) == expected_findings
    ):
        return {"scan_status": status}
    operation = append_operation(
        cluster_root,
        "CREDENTIAL_SCAN_ACK",
        {
            "node_id": node_id,
            "freeze_generation": freeze_generation,
            "cutoff_generation": cutoff_generation,
            "status": status,
            "clean": not findings,
            "findings": expected_findings,
        },
    )
    return {
        "scan_status": status,
        "scan_operation_id": str(operation["op_id"]),
        "scan_findings": findings,
    }


def _validate_sealed_blob_payload(
    cluster_root: Path,
    raw: bytes,
    *,
    expected_context: SecretContext | None = None,
    membership_nodes: dict[str, dict[str, Any]] | None = None,
    membership_trust: Any | None = None,
) -> dict[str, Any]:
    """Validate an opaque envelope with current or historical membership keys."""

    try:
        envelope = deserialize_sealed_secret(raw)
        identity = read_local_identity(cluster_root)
        materialized = (
            rebuild_materialized_state(cluster_root, write=False)
            if membership_nodes is None
            else None
        )
        nodes = membership_nodes
        if nodes is None:
            nodes = ((materialized or {}).get("cluster_nodes") or {}).get("nodes") or {}
        if str(envelope.get("cluster_id") or "") != str(identity.get("cluster_id") or ""):
            raise EnvelopeValidationError("sealed-secret belongs to another cluster")
        validate_sealed_secret(
            envelope,
            _envelope_signing_key(
                cluster_root,
                nodes,
                str(envelope.get("signer_id") or ""),
                str(envelope.get("signing_key_id") or ""),
                membership_trust=membership_trust,
            ),
            expected_context=expected_context,
            membership_roles=(
                _credential_roles_from_nodes(nodes)
                if expected_context is not None
                else None
            ),
        )
        return envelope
    except (ClusterCredentialError, ClusterStateError) as exc:
        raise ClusterSyncCommandError(str(exc)) from exc


def _credential_membership_roles(materialized: dict[str, Any]) -> dict[str, str]:
    """Return active crypto-capable audience membership."""

    nodes = ((materialized.get("cluster_nodes") or {}).get("nodes") or {})
    return _credential_roles_from_nodes(nodes)


def _credential_roles_from_nodes(nodes: dict[str, Any]) -> dict[str, str]:
    """Return audience roles from an explicit staged membership mapping."""

    return {
        str(node_id): str(node.get("role") or "")
        for node_id, node in nodes.items()
        if isinstance(node, dict)
        and node.get("enabled", True) is not False
        and node.get("state_replica", True) is not False
        and node.get("signing_public_key")
        and node.get("encryption_public_key")
    }


def _membership_signing_key(
    cluster_root: Path,
    signer_id: str,
    signing_key_id: str,
) -> str:
    """Resolve one current or historical envelope signer key."""

    return membership_signing_public_key(cluster_root, signer_id, signing_key_id)


def _envelope_signing_key(
    cluster_root: Path,
    nodes: dict[str, Any],
    signer_id: str,
    signing_key_id: str,
    *,
    membership_trust: Any | None,
) -> str:
    """Resolve an envelope key from staged trust or persisted key history."""

    if membership_trust is not None:
        matches = [
            item
            for item in membership_trust.signing_keys.get(signer_id, [])
            if str(item.get("key_id") or "") == signing_key_id
        ]
        if len(matches) == 1:
            return str(matches[0]["public_key"])
        raise ClusterSyncCommandError("sealed credential signer key is not authenticated")
    node = nodes.get(signer_id) if isinstance(nodes.get(signer_id), dict) else {}
    if (
        str(node.get("signing_key_id") or "") == signing_key_id
        and node.get("signing_public_key")
    ):
        return str(node["signing_public_key"])
    try:
        return membership_signing_public_key(cluster_root, signer_id, signing_key_id)
    except ClusterStateError:
        return _signing_key_from_nodes(nodes, signer_id)


def _open_with_local_key_history(
    cluster_root: Path,
    envelope: dict[str, Any],
    node_id: str,
    signing_public_key: str,
    context: SecretContext,
    membership_roles: dict[str, str],
) -> bytes:
    """Open a current wrapper with the active or an archived local key."""

    last_error: Exception | None = None
    for private_key in load_node_encryption_private_keys(cluster_root):
        try:
            return open_sealed_secret(
                envelope,
                node_id,
                private_key,
                signing_public_key,
                expected_context=context,
                membership_roles=membership_roles,
            )
        except SecretDecryptionError as exc:
            last_error = exc
    if last_error is not None:
        raise last_error
    raise ClusterSyncCommandError("local node has no decryption key")


def _signing_key_from_nodes(nodes: dict[str, Any], signer_id: str) -> str:
    """Resolve one signer key from current or staged membership."""

    node = nodes.get(signer_id) if isinstance(nodes, dict) else None
    public_key = str((node or {}).get("signing_public_key") or "")
    if not public_key:
        raise ClusterSyncCommandError("sealed credential signer has no membership key")
    return public_key


def _credential_store_generation(
    store: CredentialStore | None,
    secret_id: str,
    kind: str,
) -> int:
    """Return the current local generation, or zero when not materialized."""

    if store is None:
        return 0
    try:
        record = store.get_cmc(secret_id) if kind == "cmc_api_key" else store.get_tradfi(secret_id)
    except (KeyError, ValueError):
        return 0
    return int(record.get("generation") or 0)


def _decode_materialized_credential(
    kind: str,
    plaintext: bytes,
    metadata: dict[str, Any],
) -> tuple[str | dict[str, str], dict[str, Any]]:
    """Decode a recipient plaintext without returning it in command responses."""

    try:
        text = plaintext.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ClusterSyncCommandError("decrypted credential is not valid UTF-8") from exc
    details = {
        key: metadata[key]
        for key in ("label", "provider", "active")
        if key in metadata
    }
    if kind == "cmc_api_key":
        try:
            decoded = json.loads(text)
        except json.JSONDecodeError:
            decoded = None
        value = decoded.get("api_key") if isinstance(decoded, dict) else text
        if not isinstance(value, str) or not value:
            raise ClusterSyncCommandError("decrypted CMC credential has invalid format")
        return value, details
    if kind == "tradfi_profile":
        try:
            decoded = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ClusterSyncCommandError("decrypted TradFi credential has invalid format") from exc
        credentials = decoded.get("credentials") if isinstance(decoded, dict) else None
        if not isinstance(credentials, dict):
            credentials = decoded
        if isinstance(decoded, dict) and decoded.get("provider"):
            details["provider"] = decoded["provider"]
        if not isinstance(credentials, dict):
            raise ClusterSyncCommandError("decrypted TradFi credential has invalid format")
        return credentials, details
    raise ClusterSyncCommandError("unsupported sealed credential kind")


def _credential_kind_from_id(secret_id: str) -> str | None:
    """Infer the store kind only for tombstone cleanup."""

    if secret_id.startswith("cmc_"):
        return "cmc_api_key"
    if secret_id.startswith("tradfi_"):
        return "tradfi_profile"
    return None


def _credential_error_text(exc: Exception) -> str:
    """Return a secret-free materialization error."""

    if isinstance(exc, NotRecipientError):
        return "local node is not an intended recipient"
    return str(exc)


def _api_keys_target_path() -> Path:
    """Return the local api-keys.json target path for PB7."""

    try:
        directory = pb7dir()
    except Exception:
        directory = ""
    if directory:
        return Path(directory) / "api-keys.json"
    return Path(PBGDIR) / "api-keys.json"


def _build_materialize_v7_plan(
    cluster_root: Path,
    run_root: Path,
    node_id: str,
    desired_state: dict[str, Any],
    *,
    materialize_all: bool = False,
) -> dict[str, Any]:
    """Build a read-only plan for V7 config materialization."""

    paths = ClusterPaths.from_root(cluster_root)
    instances = desired_state.get("instances") if isinstance(desired_state, dict) else {}
    tombstones = desired_state.get("tombstones") if isinstance(desired_state, dict) else {}
    instances = instances if isinstance(instances, dict) else {}
    tombstones = tombstones if isinstance(tombstones, dict) else {}
    counts: dict[str, int] = {
        "add": 0,
        "update": 0,
        "skip": 0,
        "delete": 0,
        "not_assigned": 0,
        "conflicted": 0,
        "tombstoned": 0,
        "error": 0,
        "files_to_write": 0,
        "dirs_to_delete": 0,
    }
    items: list[dict[str, Any]] = []

    for name in sorted(instances):
        item = instances.get(name) if isinstance(instances.get(name), dict) else {}
        row: dict[str, Any] = {
            "instance": str(name),
            "assigned_host": str(item.get("assigned_host") or ""),
            "desired_state": str(item.get("desired_state") or ""),
            "version": str(item.get("version") or ""),
            "config_manifest_hash": str(item.get("config_manifest_hash") or ""),
            "files": [],
        }
        try:
            _validate_relative_name(str(name), "instance")
            if item.get("conflicted") is True:
                _mark_materialize_skip(row, counts, "conflicted", "desired state is conflicted")
            elif not materialize_all and str(item.get("assigned_host") or "") != node_id:
                _mark_materialize_skip(row, counts, "not_assigned", "instance is assigned to another node")
            else:
                _populate_materialize_files(paths.config_blobs, run_root / str(name), row)
                files_to_write = sum(1 for file_item in row["files"] if file_item.get("action") == "write")
                if files_to_write:
                    row["action"] = "add" if not (run_root / str(name)).is_dir() else "update"
                    row["status"] = "ready"
                    row["reason"] = f"{files_to_write} file(s) need materialization"
                    counts[row["action"]] += 1
                    counts["files_to_write"] += files_to_write
                else:
                    _mark_materialize_skip(row, counts, "current", "local config files already match desired state")
        except Exception as exc:
            row.update({"action": "skip", "status": "error", "reason": str(exc)})
            counts["error"] += 1
        items.append(row)

    for name in sorted(set(tombstones) - set(instances)):
        item = tombstones.get(name) if isinstance(tombstones.get(name), dict) else {}
        row = {
            "instance": str(name),
            "action": "skip",
            "status": "tombstoned",
            "reason": "instance is tombstoned; materialization never recreates tombstones",
            "version": str(item.get("version") or ""),
            "files": [],
        }
        try:
            _validate_relative_name(str(name), "instance")
            if (run_root / str(name)).is_dir():
                row.update({
                    "action": "delete",
                    "status": "ready",
                    "reason": "local config directory is tombstoned and will be backed up before removal",
                    "path": str(run_root / str(name)),
                })
                counts["delete"] += 1
                counts["dirs_to_delete"] += 1
            else:
                counts["skip"] += 1
        except Exception as exc:
            row.update({"action": "skip", "status": "error", "reason": str(exc)})
            counts["error"] += 1
        counts["tombstoned"] += 1
        items.append(row)

    return {
        "cluster_id": str(desired_state.get("cluster_id") or ""),
        "node_id": node_id,
        "materialize_all": bool(materialize_all),
        "run_v7_root": str(run_root),
        "counts": counts,
        "items": items,
        "can_apply": counts["error"] == 0 and (counts["add"] + counts["update"] + counts["delete"]) > 0,
        "message": "Preview only. Apply writes V7 JSON configs from config blobs and removes backed-up local tombstone directories without starting/stopping bots.",
    }


def _mark_materialize_skip(row: dict[str, Any], counts: dict[str, int], status: str, reason: str) -> None:
    """Mark one materialization plan row as skipped."""

    row.update({"action": "skip", "status": status, "reason": reason})
    counts["skip"] += 1
    if status in counts:
        counts[status] += 1


def _populate_materialize_files(config_blobs_root: Path, target_dir: Path, row: dict[str, Any]) -> None:
    """Populate materialization file rows for one assigned instance."""

    manifest_hash = str(row.get("config_manifest_hash") or "")
    if not manifest_hash:
        raise ClusterSyncCommandError("missing config manifest hash")
    manifest_raw = _read_verified_blob(config_blobs_root, manifest_hash, f"manifest blob for {row.get('instance')}")
    try:
        manifest = json.loads(manifest_raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ClusterSyncCommandError("manifest blob is not valid JSON") from exc
    files = manifest.get("files") if isinstance(manifest, dict) else None
    if not isinstance(files, dict):
        raise ClusterSyncCommandError("manifest missing files object")

    file_rows: list[dict[str, Any]] = []
    for filename in sorted(files):
        meta = files.get(filename) if isinstance(files.get(filename), dict) else {}
        _validate_relative_name(str(filename), "config filename")
        sha = str(meta.get("sha256") or "")
        blob_hash = _validate_hash(f"sha256:{sha}")
        raw = _read_verified_blob(config_blobs_root, blob_hash, f"file blob for {row.get('instance')}/{filename}")
        expected_size = meta.get("size")
        if expected_size is not None:
            try:
                size = int(expected_size)
            except (TypeError, ValueError) as exc:
                raise ClusterSyncCommandError(f"invalid size for {filename}") from exc
            if len(raw) != size:
                raise ClusterSyncCommandError(f"file blob size mismatch for {filename}")
        target = target_dir / str(filename)
        current_hash = hashlib.sha256(target.read_bytes()).hexdigest() if target.is_file() else ""
        file_rows.append({
            "name": str(filename),
            "hash": blob_hash,
            "size": len(raw),
            "action": "current" if current_hash == sha else "write",
        })
    row["files"] = file_rows


def _read_verified_blob(
    base_dir: Path,
    blob_hash: str,
    label: str,
    *,
    max_size: int = MAX_CONFIG_BLOB_BYTES,
) -> bytes:
    """Read one content-addressed blob and verify its sha256 digest."""

    text = _validate_hash(blob_hash)
    digest = text.removeprefix("sha256:")
    path = Path(base_dir) / "sha256" / digest[:2] / f"{digest}.json"
    _reject_blob_path_symlinks(base_dir, path)
    if not path.is_file():
        raise ClusterSyncCommandError(f"missing {label}: {text}")
    raw = path.read_bytes()
    if len(raw) > max_size:
        raise ClusterSyncCommandError(f"{label} too large")
    if hashlib.sha256(raw).hexdigest() != digest:
        raise ClusterSyncCommandError(f"{label} hash mismatch: {text}")
    return raw


def _reject_blob_path_symlinks(base_dir: Path, path: Path) -> None:
    """Reject symlinks at every fixed content-addressed store boundary."""

    base = Path(base_dir)
    for candidate in (base, base / "sha256", Path(path).parent, Path(path)):
        if candidate.is_symlink():
            raise ClusterSyncCommandError("blob store path must not contain symlinks")


def _read_json_payload(raw: bytes, max_size: int) -> dict[str, Any]:
    """Read a bounded JSON object payload."""

    if len(raw) > max_size:
        raise ClusterSyncCommandError("payload too large")
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ClusterSyncCommandError("payload must be a JSON object") from exc
    if not isinstance(payload, dict):
        raise ClusterSyncCommandError("payload must be a JSON object")
    return payload


def _read_operation_batch_payload(raw: bytes) -> list[dict[str, Any]]:
    """Read a bounded JSON batch payload containing operation objects."""

    if len(raw) > MAX_OPERATION_BATCH_BYTES:
        raise ClusterSyncCommandError("operation batch payload too large")
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ClusterSyncCommandError("operation batch payload must be a JSON object") from exc
    if not isinstance(payload, dict):
        raise ClusterSyncCommandError("operation batch payload must be a JSON object")
    operations = payload.get("operations")
    if not isinstance(operations, list):
        raise ClusterSyncCommandError("operation batch payload missing operations list")
    result: list[dict[str, Any]] = []
    for item in operations:
        if not isinstance(item, dict):
            raise ClusterSyncCommandError("operation batch contains a non-object operation")
        result.append(item)
    return result


def _read_blob_batch_payload(raw: bytes) -> list[dict[str, Any]]:
    """Read a bounded JSON batch payload containing base64 encoded config blobs."""

    if len(raw) > MAX_CONFIG_BLOB_BATCH_BYTES:
        raise ClusterSyncCommandError("blob batch payload too large")
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ClusterSyncCommandError("blob batch payload must be a JSON object") from exc
    if not isinstance(payload, dict):
        raise ClusterSyncCommandError("blob batch payload must be a JSON object")
    blobs = payload.get("blobs")
    if not isinstance(blobs, list):
        raise ClusterSyncCommandError("blob batch payload missing blobs list")
    return _decode_blob_items(blobs, max_size=MAX_CONFIG_BLOB_BYTES, label="blob batch")


def _read_checkpoint_control_payload(stdin_data: bytes) -> dict[str, Any]:
    """Decode one bounded checkpoint prepare or commit payload."""

    if not stdin_data or len(stdin_data) > MAX_CHECKPOINT_BYTES:
        raise ClusterSyncCommandError("checkpoint control payload size is invalid")
    try:
        payload = json.loads(stdin_data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ClusterSyncCommandError("checkpoint control payload is invalid JSON") from exc
    if not isinstance(payload, dict):
        raise ClusterSyncCommandError("checkpoint control payload must be an object")
    return payload


def _prepared_checkpoint_path(cluster_root: Path, proposal_id: str) -> Path:
    """Resolve one owner-only prepared-checkpoint marker path."""

    validated = _validate_hash(proposal_id).removeprefix("sha256:")
    directory = ensure_private_directory_tree(
        cluster_root,
        cluster_root / "checkpoints" / "prepared",
    )
    return directory / f"{validated}.json"


def _cleanup_expired_prepared_checkpoints(cluster_root: Path) -> None:
    """Remove expired transient proposal markers before storing a new one."""

    directory = ensure_private_directory_tree(
        cluster_root,
        cluster_root / "checkpoints" / "prepared",
    )
    now = int(time.time())
    for path in directory.glob("*.json"):
        if path.is_symlink():
            raise ClusterSyncCommandError("prepared checkpoint marker must not be a symlink")
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ClusterSyncCommandError("prepared checkpoint marker is unreadable") from exc
        if not isinstance(value, dict) or int(value.get("expires_at") or 0) <= now:
            path.unlink(missing_ok=True)


def _read_apply_bundle_payload(raw: bytes) -> dict[str, Any]:
    """Read one bounded apply-bundle payload with blobs and operations."""

    if len(raw) > MAX_APPLY_BUNDLE_BYTES:
        raise ClusterSyncCommandError("apply-bundle payload too large")
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ClusterSyncCommandError("apply-bundle payload must be a JSON object") from exc
    if not isinstance(payload, dict):
        raise ClusterSyncCommandError("apply-bundle payload must be a JSON object")
    operations = payload.get("operations")
    if not isinstance(operations, list):
        raise ClusterSyncCommandError("apply-bundle payload missing operations list")
    clean_operations: list[dict[str, Any]] = []
    for item in operations:
        if not isinstance(item, dict):
            raise ClusterSyncCommandError("apply-bundle contains a non-object operation")
        clean_operations.append(item)
    config_blobs = payload.get("config_blobs")
    secret_blobs = payload.get("secret_blobs")
    sealed_blobs = payload.get("sealed_blobs", [])
    if not isinstance(config_blobs, list):
        raise ClusterSyncCommandError("apply-bundle payload missing config_blobs list")
    if not isinstance(secret_blobs, list):
        raise ClusterSyncCommandError("apply-bundle payload missing secret_blobs list")
    if not isinstance(sealed_blobs, list):
        raise ClusterSyncCommandError("apply-bundle sealed_blobs must be a list")
    return {
        "operations": clean_operations,
        "config_blobs": _decode_blob_items(config_blobs, max_size=MAX_CONFIG_BLOB_BYTES, label="apply-bundle config"),
        "secret_blobs": _decode_blob_items(secret_blobs, max_size=MAX_SECRET_BLOB_BYTES, label="apply-bundle secret"),
        "sealed_blobs": _decode_blob_items(sealed_blobs, max_size=MAX_SEALED_BLOB_BYTES, label="apply-bundle sealed"),
    }


def _decode_blob_items(items: list[Any], *, max_size: int, label: str) -> list[dict[str, Any]]:
    """Decode and verify content-addressed blob payload items."""

    result: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            raise ClusterSyncCommandError(f"{label} contains a non-object blob")
        blob_hash = _validate_hash(str(item.get("hash") or ""))
        encoded = item.get("content_b64")
        if not isinstance(encoded, str):
            raise ClusterSyncCommandError(f"{label} entry missing content_b64")
        try:
            raw_blob = base64.b64decode(encoded.encode("ascii"), validate=True)
        except (UnicodeEncodeError, binascii.Error) as exc:
            raise ClusterSyncCommandError(f"{label} entry has invalid base64") from exc
        if len(raw_blob) > max_size:
            raise ClusterSyncCommandError(f"{label} blob too large")
        expected = blob_hash.removeprefix("sha256:")
        if hashlib.sha256(raw_blob).hexdigest() != expected:
            raise ClusterSyncCommandError(f"{label} blob hash mismatch")
        result.append({"hash": blob_hash, "raw": raw_blob})
    return result


def _apply_bundle(
    cluster_root: Path,
    paths: ClusterPaths,
    cluster_id: str,
    raw: bytes,
    *,
    remote_node: str,
) -> dict[str, Any]:
    """Write blobs and operations, then rebuild and materialize in one command."""

    payload = _read_apply_bundle_payload(raw)
    config_blobs = payload["config_blobs"]
    secret_blobs = payload["secret_blobs"]
    sealed_blobs = payload["sealed_blobs"]
    operations = payload["operations"]
    baseline = active_checkpoint_baseline(cluster_root)
    if any(
        int(operation.get("seq") or 0)
        <= int(baseline.get(str(operation.get("actor") or ""), 0))
        for operation in operations
    ):
        raise ClusterSyncCommandError("apply-bundle contains an operation at or below the checkpoint baseline")
    staged_nodes = _staged_membership_nodes(
        cluster_root,
        operations,
        remote_node=remote_node,
    )
    obsolete_hashes = set((_credential_cutoff(cluster_root) or {}).get("obsolete_secret_blob_hashes") or [])
    if any(str(blob.get("hash") or "") in obsolete_hashes for blob in secret_blobs):
        raise ClusterSyncCommandError("apply-bundle contains a pre-cutoff plaintext secret blob")

    for operation in operations:
        _safe_state_call(
            lambda op=operation: validate_operation(
                op,
                expected_cluster_id=cluster_id,
                cluster_root=cluster_root,
                membership_trust=staged_nodes,
                network_input=True,
            )
        )
    for blob in sealed_blobs:
        _validate_sealed_blob_payload(
            cluster_root,
            blob["raw"],
            membership_nodes=staged_nodes.nodes,
            membership_trust=staged_nodes,
        )

    written_config: list[dict[str, str]] = []
    for blob in config_blobs:
        path = _write_blob(paths.config_blobs, str(blob["hash"]), blob["raw"], MAX_CONFIG_BLOB_BYTES, secret=False)
        written_config.append({"hash": str(blob["hash"]), "path": _relative_cluster_path(paths.root, path)})

    written_secret: list[dict[str, str]] = []
    for blob in secret_blobs:
        path = _write_blob(paths.secret_blobs, str(blob["hash"]), blob["raw"], MAX_SECRET_BLOB_BYTES, secret=True)
        written_secret.append({"hash": str(blob["hash"]), "path": _relative_cluster_path(paths.root, path)})

    written_sealed: list[dict[str, str]] = []
    for blob in sealed_blobs:
        path = _write_blob(paths.sealed_blobs, str(blob["hash"]), blob["raw"], MAX_SEALED_BLOB_BYTES, secret=True)
        written_sealed.append({"hash": str(blob["hash"]), "path": _relative_cluster_path(paths.root, path)})

    written_ops: list[dict[str, Any]] = []
    publish_order = sorted(
        operations,
        key=lambda item: str(item.get("op") or "") in V2_CREDENTIAL_OPS,
    )
    for operation in publish_order:
        _safe_state_call(
            lambda op=operation: write_operation(
                cluster_root,
                op,
                network_input=True,
                membership_trust=staged_nodes,
            )
        )
    for operation in operations:
        written_ops.append({"op_id": str(operation["op_id"]), "actor": str(operation["actor"]), "seq": int(operation["seq"])})

    materialized = _safe_state_call(lambda: rebuild_materialized_state(cluster_root))
    return {
        "ok": True,
        "count": len(written_ops),
        "operations": written_ops,
        "config_blobs": len(written_config),
        "secret_blobs": len(written_secret),
        "sealed_blobs": len(written_sealed),
        "generation": int(((materialized.get("cluster_nodes") or {}).get("generation") or 0)),
        "materialization": {"status": "delegated_to_pbcluster"},
    }


def _write_blob(base_dir: Path, blob_hash: str, raw: bytes, max_size: int, *, secret: bool) -> Path:
    """Validate and atomically write a content-addressed blob."""

    if len(raw) > max_size:
        raise ClusterSyncCommandError("blob too large")
    digest = hashlib.sha256(raw).hexdigest()
    expected = blob_hash.removeprefix("sha256:")
    if digest != expected:
        raise ClusterSyncCommandError("blob hash mismatch")
    target = Path(base_dir) / "sha256" / expected[:2] / f"{expected}.json"
    _reject_blob_path_symlinks(base_dir, target)
    if secret:
        ensure_private_directory_tree(Path(base_dir), target.parent)
    _atomic_write_bytes(target, raw, mode=0o600 if secret else 0o644)
    return target


def _atomic_write_bytes(path: Path, raw: bytes, *, mode: int) -> None:
    """Atomically write bytes with final file permissions."""

    if mode == 0o600:
        atomic_write_private_bytes(path, raw)
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        tmp.write_bytes(raw)
        os.chmod(tmp, mode)
        os.replace(tmp, path)
        os.chmod(path, mode)
    finally:
        tmp.unlink(missing_ok=True)


def _validate_hash(value: str) -> str:
    """Validate a sha256:<hex> blob hash."""

    text = str(value or "")
    if not text.startswith("sha256:") or len(text) != len("sha256:") + 64:
        raise ClusterSyncCommandError("invalid hash")
    try:
        int(text[len("sha256:"):], 16)
    except ValueError as exc:
        raise ClusterSyncCommandError("invalid hash") from exc
    return text


def _validate_cluster_id(value: str) -> str:
    """Validate a pbgui-cluster UUID identifier."""

    text = str(value or "")
    prefix = "pbgui-cluster-"
    if not text.startswith(prefix):
        raise ClusterSyncCommandError("invalid cluster_id")
    try:
        uuid.UUID(text[len(prefix):])
    except (TypeError, ValueError) as exc:
        raise ClusterSyncCommandError("invalid cluster_id") from exc
    return text


def _validate_node_id(value: str, field: str) -> str:
    """Validate a pbgui-node UUID identifier."""

    text = str(value or "")
    prefix = "pbgui-node-"
    if not text.startswith(prefix):
        raise ClusterSyncCommandError(f"invalid {field}")
    try:
        uuid.UUID(text[len(prefix):])
    except (TypeError, ValueError) as exc:
        raise ClusterSyncCommandError(f"invalid {field}") from exc
    return text


def _validate_role(value: str) -> str:
    """Validate a cluster node role."""

    role = str(value or "").strip().lower()
    if role not in {"master", "vps"}:
        raise ClusterSyncCommandError("invalid node role")
    return role


def _validate_relative_name(value: str, field: str) -> str:
    """Reject names that can escape a cluster-managed directory."""

    text = str(value or "")
    if not text or text in {".", ".."} or "/" in text or "\\" in text or "\x00" in text:
        raise ClusterSyncCommandError(f"invalid {field}")
    return text


def _relative_cluster_path(cluster_root: Path, path: Path) -> str:
    """Return a display-safe path relative to the cluster root."""

    try:
        return str(Path(path).resolve().relative_to(Path(cluster_root).resolve()))
    except ValueError:
        raise ClusterSyncCommandError("path escaped cluster root")


if __name__ == "__main__":
    raise SystemExit(main())
    membership_signing_public_key,
