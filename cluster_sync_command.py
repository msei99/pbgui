#!/usr/bin/env python3
"""Restricted command wrapper for PBGui Cluster Sync SSH keys.

The script is intended for OpenSSH forced-command use. It accepts one small
Cluster Sync command through SSH_ORIGINAL_COMMAND, validates the peer node, and
only reads/writes under data/cluster.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shlex
import sys
import uuid
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
from pbgui_purefunc import PBGDIR

PROTOCOL_VERSION = 1
MAX_COMMAND_BYTES = 4096
MAX_OPERATION_BYTES = 1024 * 1024
MAX_CONFIG_BLOB_BYTES = 16 * 1024 * 1024
MAX_SECRET_BLOB_BYTES = 1024 * 1024

READ_VERBS = frozenset({"hello", "get-state-vector", "get-desired-state"})
WRITE_VERBS = frozenset({"join", "put-op", "put-blob", "put-secret-blob", "rebuild"})
STDIN_VERBS = frozenset({"put-op", "put-blob", "put-secret-blob"})
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
        return {"ok": True, "state_vector": materialized.get("state_vector") or {}}
    if verb == "get-desired-state":
        materialized = _safe_state_call(lambda: rebuild_materialized_state(root, write=False))
        return {"ok": True, "desired_state": materialized.get("desired_state") or {}}
    if verb == "rebuild":
        materialized = _safe_state_call(lambda: rebuild_materialized_state(root))
        return {
            "ok": True,
            "generation": int((materialized.get("cluster_nodes") or {}).get("generation") or 0),
            "nodes": len(((materialized.get("cluster_nodes") or {}).get("nodes") or {})),
            "instances": len(((materialized.get("desired_state") or {}).get("instances") or {})),
        }
    if verb == "put-op":
        _require_arity(tokens, 1)
        operation = _read_json_payload(stdin_data, MAX_OPERATION_BYTES)
        _safe_state_call(lambda: validate_operation(operation, expected_cluster_id=cluster_id))
        _safe_state_call(lambda: write_operation(root, operation))
        return {"ok": True, "op_id": str(operation["op_id"]), "actor": str(operation["actor"]), "seq": int(operation["seq"])}
    if verb == "put-blob":
        _require_arity(tokens, 2)
        blob_hash = _validate_hash(tokens[1])
        path = _write_blob(paths.config_blobs, blob_hash, stdin_data, MAX_CONFIG_BLOB_BYTES, secret=False)
        return {"ok": True, "hash": blob_hash, "path": _relative_cluster_path(paths.root, path)}
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
    return sys.stdin.buffer.read(max(MAX_CONFIG_BLOB_BYTES, MAX_OPERATION_BYTES) + 1)


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


def _relative_cluster_path(cluster_root: Path, path: Path) -> str:
    """Return a display-safe path relative to the cluster root."""

    try:
        return str(Path(path).resolve().relative_to(Path(cluster_root).resolve()))
    except ValueError:
        raise ClusterSyncCommandError("path escaped cluster root")


if __name__ == "__main__":
    raise SystemExit(main())
