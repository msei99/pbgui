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
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from master.cluster_state import (
    ClusterPaths,
    ClusterStateError,
    default_cluster_root,
    ensure_local_identity,
    read_local_identity,
    rebuild_materialized_state,
    validate_operation,
    write_operation,
)
from pbgui_purefunc import PBGDIR, pb7dir

PROTOCOL_VERSION = 1
MAX_COMMAND_BYTES = 4096
MAX_OPERATION_BYTES = 1024 * 1024
MAX_OPERATION_BATCH_BYTES = 16 * 1024 * 1024
MAX_CONFIG_BLOB_BYTES = 16 * 1024 * 1024
MAX_CONFIG_BLOB_BATCH_BYTES = 16 * 1024 * 1024
MAX_SECRET_BLOB_BYTES = 1024 * 1024

READ_VERBS = frozenset({"hello", "get-state-vector", "get-desired-state", "materialize-v7-preview", "materialize-api-keys-preview"})
WRITE_VERBS = frozenset({"join", "put-op", "put-ops", "put-blob", "put-blobs", "put-secret-blob", "rebuild", "materialize-v7", "materialize-api-keys"})
STDIN_VERBS = frozenset({"put-op", "put-ops", "put-blob", "put-blobs", "put-secret-blob"})
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

    identity = read_local_identity(root)
    cluster_id = str(identity["cluster_id"])
    _verify_remote_node(root, remote_node, allow_join=allow_join)
    paths = ClusterPaths.from_root(root)

    if verb == "hello":
        return {
            "ok": True,
            "protocol_version": PROTOCOL_VERSION,
            "cluster_id": cluster_id,
            "node_id": str(identity["node_id"]),
            "role": str(identity.get("role") or ""),
            "remote_node": remote_node,
        }
    if verb == "get-state-vector":
        materialized = _safe_state_call(lambda: rebuild_materialized_state(root, write=False))
        return {
            "ok": True,
            "cluster_id": cluster_id,
            "node_id": str(identity["node_id"]),
            "state_vector": materialized.get("state_vector") or {},
        }
    if verb == "get-desired-state":
        materialized = _safe_state_call(lambda: rebuild_materialized_state(root, write=False))
        return {
            "ok": True,
            "cluster_id": cluster_id,
            "node_id": str(identity["node_id"]),
            "desired_state": materialized.get("desired_state") or {},
        }
    if verb == "materialize-v7-preview":
        return _materialize_v7_configs(root, write=False)
    if verb == "materialize-api-keys-preview":
        return _materialize_api_keys(root, write=False)
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
    if verb == "put-op":
        _require_arity(tokens, 1)
        operation = _read_json_payload(stdin_data, MAX_OPERATION_BYTES)
        _safe_state_call(lambda: validate_operation(operation, expected_cluster_id=cluster_id))
        _safe_state_call(lambda: write_operation(root, operation))
        return {"ok": True, "op_id": str(operation["op_id"]), "actor": str(operation["actor"]), "seq": int(operation["seq"])}
    if verb == "put-ops":
        _require_arity(tokens, 1)
        operations = _read_operation_batch_payload(stdin_data)
        for operation in operations:
            _safe_state_call(lambda op=operation: validate_operation(op, expected_cluster_id=cluster_id))
        written: list[dict[str, Any]] = []
        for operation in operations:
            _safe_state_call(lambda op=operation: write_operation(root, op))
            written.append({"op_id": str(operation["op_id"]), "actor": str(operation["actor"]), "seq": int(operation["seq"])})
        return {"ok": True, "count": len(written), "operations": written}
    if verb == "put-blob":
        _require_arity(tokens, 2)
        blob_hash = _validate_hash(tokens[1])
        path = _write_blob(paths.config_blobs, blob_hash, stdin_data, MAX_CONFIG_BLOB_BYTES, secret=False)
        return {"ok": True, "hash": blob_hash, "path": _relative_cluster_path(paths.root, path)}
    if verb == "put-blobs":
        _require_arity(tokens, 1)
        blobs = _read_blob_batch_payload(stdin_data)
        written: list[dict[str, str]] = []
        for blob in blobs:
            path = _write_blob(paths.config_blobs, str(blob["hash"]), blob["raw"], MAX_CONFIG_BLOB_BYTES, secret=False)
            written.append({"hash": str(blob["hash"]), "path": _relative_cluster_path(paths.root, path)})
        return {"ok": True, "count": len(written), "blobs": written}
    if verb == "put-secret-blob":
        _require_arity(tokens, 2)
        blob_hash = _validate_hash(tokens[1])
        path = _write_blob(paths.secret_blobs, blob_hash, stdin_data, MAX_SECRET_BLOB_BYTES, secret=True)
        return {"ok": True, "hash": blob_hash, "path": _relative_cluster_path(paths.root, path)}
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
    return sys.stdin.buffer.read(max(MAX_CONFIG_BLOB_BATCH_BYTES, MAX_CONFIG_BLOB_BYTES, MAX_OPERATION_BATCH_BYTES) + 1)


def _join_cluster(cluster_root: Path, remote_node: str, tokens: list[str], *, allow_join: bool) -> dict[str, Any]:
    """Initialize local identity for an explicitly approved remote join."""

    if not allow_join:
        raise ClusterSyncCommandError("join requires allow_join")
    _require_arity(tokens, 5)
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
    return {
        "ok": True,
        "cluster_id": str(identity["cluster_id"]),
        "node_id": str(identity["node_id"]),
        "role": str(identity.get("role") or role),
        "pbname": str(identity.get("created_from_pbname") or pbname),
        "joined_by": remote_node,
    }


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


def _require_arity(tokens: list[str], expected: int) -> None:
    """Require an exact token count."""

    if len(tokens) != expected:
        raise ClusterSyncCommandError(f"{tokens[0]} expects {expected - 1} argument(s)")


def _verify_remote_node(cluster_root: Path, remote_node: str, *, allow_join: bool) -> None:
    """Ensure the caller is a known node unless this is an explicit join."""

    if allow_join:
        return
    materialized = _safe_state_call(lambda: rebuild_materialized_state(cluster_root, write=False))
    nodes = ((materialized.get("cluster_nodes") or {}).get("nodes") or {})
    if remote_node not in nodes:
        raise ClusterSyncCommandError("remote node is not registered")


def _materialize_v7_configs(cluster_root: Path, *, write: bool) -> dict[str, Any]:
    """Preview or write assigned V7 config blobs into data/run_v7."""

    materialized = _safe_state_call(lambda: rebuild_materialized_state(cluster_root, write=write))
    identity = _safe_state_call(lambda: read_local_identity(cluster_root))
    desired_state = materialized.get("desired_state") if isinstance(materialized, dict) else {}
    desired_state = desired_state if isinstance(desired_state, dict) else {}
    node_id = str(identity["node_id"])
    run_root = Path(cluster_root).parent / "run_v7"
    plan = _build_materialize_v7_plan(Path(cluster_root), run_root, node_id, desired_state)
    if not write:
        plan.update({"ok": True, "read_only": True})
        return plan

    if int((plan.get("counts") or {}).get("error") or 0) > 0:
        raise ClusterSyncCommandError("materialization blocked by missing or invalid blobs")

    written: list[dict[str, Any]] = []
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

    counts = dict(plan.get("counts") or {})
    counts["written_instances"] = len(written)
    counts["written_files"] = sum(int(item.get("files") or 0) for item in written)
    plan.update({
        "ok": True,
        "read_only": False,
        "counts": counts,
        "written": written,
        "message": "V7 config files were materialized. No files were deleted and no bots were started or stopped.",
    })
    return plan


def _materialize_api_keys(cluster_root: Path, *, write: bool) -> dict[str, Any]:
    """Preview or write api-keys.json from the desired secret blob."""

    materialized = _safe_state_call(lambda: rebuild_materialized_state(cluster_root, write=write))
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
    if target.exists() and hashlib.sha256(target.read_bytes()).hexdigest() != secret_hash.removeprefix("sha256:"):
        backup_dir = Path(PBGDIR) / "data" / "backup" / "api-keys_cluster" / datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        backup_dir.mkdir(parents=True, exist_ok=True)
        (backup_dir / "api-keys.json").write_bytes(target.read_bytes())
        plan["backup"] = str(backup_dir / "api-keys.json")
    _atomic_write_bytes(target, raw, mode=0o600)
    if hashlib.sha256(target.read_bytes()).hexdigest() != secret_hash.removeprefix("sha256:"):
        raise ClusterSyncCommandError("api-keys write verification failed")

    counts = dict(plan.get("counts") or {})
    counts["written"] = 1
    plan.update({
        "ok": True,
        "read_only": False,
        "counts": counts,
        "action": "write",
        "status": "written",
        "message": "api-keys.json was materialized from the Cluster Sync secret blob. No bots were restarted.",
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
        json.loads(raw.decode("utf-8"))
        current_hash = hashlib.sha256(target.read_bytes()).hexdigest() if target.is_file() else ""
        if current_hash == secret_hash.removeprefix("sha256:"):
            base.update({"action": "skip", "status": "current", "reason": "api-keys.json already matches desired secret blob"})
            base["counts"]["current"] = 1
        else:
            base.update({"action": "write", "status": "ready", "reason": "api-keys.json differs from desired secret blob", "can_apply": True})
            base["counts"]["write"] = 1
    except Exception as exc:
        base.update({"action": "skip", "status": "error", "reason": str(exc)})
        base["counts"]["error"] = 1
    return base


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
        "not_assigned": 0,
        "conflicted": 0,
        "tombstoned": 0,
        "error": 0,
        "files_to_write": 0,
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
            elif str(item.get("assigned_host") or "") != node_id:
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
        counts["skip"] += 1
        counts["tombstoned"] += 1
        items.append(row)

    return {
        "cluster_id": str(desired_state.get("cluster_id") or ""),
        "node_id": node_id,
        "run_v7_root": str(run_root),
        "counts": counts,
        "items": items,
        "can_apply": counts["error"] == 0 and (counts["add"] + counts["update"]) > 0,
        "message": "Preview only. Apply writes assigned V7 JSON configs from config blobs without deleting files or starting/stopping bots.",
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


def _read_verified_blob(base_dir: Path, blob_hash: str, label: str) -> bytes:
    """Read one content-addressed blob and verify its sha256 digest."""

    text = _validate_hash(blob_hash)
    digest = text.removeprefix("sha256:")
    path = Path(base_dir) / "sha256" / digest[:2] / f"{digest}.json"
    if not path.is_file():
        raise ClusterSyncCommandError(f"missing {label}: {text}")
    raw = path.read_bytes()
    if len(raw) > MAX_CONFIG_BLOB_BYTES:
        raise ClusterSyncCommandError(f"{label} too large")
    if hashlib.sha256(raw).hexdigest() != digest:
        raise ClusterSyncCommandError(f"{label} hash mismatch: {text}")
    return raw


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
    result: list[dict[str, Any]] = []
    for item in blobs:
        if not isinstance(item, dict):
            raise ClusterSyncCommandError("blob batch contains a non-object blob")
        blob_hash = _validate_hash(str(item.get("hash") or ""))
        encoded = item.get("content_b64")
        if not isinstance(encoded, str):
            raise ClusterSyncCommandError("blob batch entry missing content_b64")
        try:
            raw_blob = base64.b64decode(encoded.encode("ascii"), validate=True)
        except (UnicodeEncodeError, binascii.Error) as exc:
            raise ClusterSyncCommandError("blob batch entry has invalid base64") from exc
        if len(raw_blob) > MAX_CONFIG_BLOB_BYTES:
            raise ClusterSyncCommandError("blob too large")
        expected = blob_hash.removeprefix("sha256:")
        if hashlib.sha256(raw_blob).hexdigest() != expected:
            raise ClusterSyncCommandError("blob hash mismatch")
        result.append({"hash": blob_hash, "raw": raw_blob})
    return result


def _write_blob(base_dir: Path, blob_hash: str, raw: bytes, max_size: int, *, secret: bool) -> Path:
    """Validate and atomically write a content-addressed blob."""

    if len(raw) > max_size:
        raise ClusterSyncCommandError("blob too large")
    digest = hashlib.sha256(raw).hexdigest()
    expected = blob_hash.removeprefix("sha256:")
    if digest != expected:
        raise ClusterSyncCommandError("blob hash mismatch")
    target = Path(base_dir) / "sha256" / expected[:2] / f"{expected}.json"
    _atomic_write_bytes(target, raw, mode=0o600 if secret else 0o644)
    return target


def _atomic_write_bytes(path: Path, raw: bytes, *, mode: int) -> None:
    """Atomically write bytes with final file permissions."""

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
