"""Tests for the restricted Cluster Sync command wrapper."""

from __future__ import annotations

import hashlib
import json
import stat
import sys
from pathlib import Path

import pytest

from cluster_sync_command import ClusterSyncCommandError, main, run_command
from master.cluster_state import append_operation, ensure_local_identity, load_operations


CLUSTER_ID = "pbgui-cluster-00000000-0000-4000-8000-000000000001"
FOREIGN_CLUSTER_ID = "pbgui-cluster-00000000-0000-4000-8000-000000000099"
NODE_A = "pbgui-node-00000000-0000-4000-8000-00000000000a"
NODE_B = "pbgui-node-00000000-0000-4000-8000-00000000000b"
HASH_A = "sha256:" + "a" * 64


def _init_cluster(tmp_path: Path) -> Path:
    """Create a deterministic cluster with a registered remote node."""

    root = tmp_path / "cluster"
    ensure_local_identity(
        root,
        role="master",
        pbname="master-a",
        cluster_id=CLUSTER_ID,
        node_id=NODE_A,
        created_at=100,
    )
    append_operation(root, "ADD_NODE", {"node_id": NODE_B, "role": "vps", "pbname": "vps-b"}, created_at=101)
    return root


def _operation(cluster_id: str = CLUSTER_ID) -> dict:
    """Build one deterministic remote operation."""

    return {
        "schema_version": 1,
        "cluster_id": cluster_id,
        "op_id": f"{NODE_B}:00000001",
        "actor": NODE_B,
        "seq": 1,
        "op": "ADD_NODE",
        "created_at": 102,
        "node_id": NODE_B,
        "role": "vps",
        "pbname": "vps-b",
    }


def test_hello_returns_identity_for_registered_peer(tmp_path: Path) -> None:
    """hello returns local identity and protocol information for known peers."""

    root = _init_cluster(tmp_path)

    payload = run_command(root, NODE_B, "hello")

    assert payload["ok"] is True
    assert payload["protocol_version"] == 1
    assert payload["cluster_id"] == CLUSTER_ID
    assert payload["node_id"] == NODE_A
    assert payload["remote_node"] == NODE_B


def test_unknown_peer_is_rejected_without_join_mode(tmp_path: Path) -> None:
    """Only registered peers can use the restricted command unless join mode is explicit."""

    root = _init_cluster(tmp_path)
    unknown = "pbgui-node-00000000-0000-4000-8000-000000000099"

    with pytest.raises(ClusterSyncCommandError, match="not registered"):
        run_command(root, unknown, "hello")

    assert run_command(root, unknown, "hello", allow_join=True)["remote_node"] == unknown


def test_unknown_command_is_rejected(tmp_path: Path) -> None:
    """Unsupported commands are rejected before any state access."""

    root = _init_cluster(tmp_path)

    with pytest.raises(ClusterSyncCommandError, match="unsupported command"):
        run_command(root, NODE_B, "rm -rf /")


def test_put_op_writes_valid_operation(tmp_path: Path) -> None:
    """put-op validates and writes one remote operation file."""

    root = _init_cluster(tmp_path)

    payload = run_command(root, NODE_B, "put-op", json.dumps(_operation()).encode("utf-8"))
    operations = load_operations(root, expected_cluster_id=CLUSTER_ID)

    assert payload == {"ok": True, "op_id": f"{NODE_B}:00000001", "actor": NODE_B, "seq": 1}
    assert any(item["op_id"] == f"{NODE_B}:00000001" for item in operations)


def test_put_op_rejects_foreign_cluster(tmp_path: Path) -> None:
    """put-op rejects operations from another cluster before writing."""

    root = _init_cluster(tmp_path)

    with pytest.raises(ClusterSyncCommandError, match="foreign cluster_id"):
        run_command(root, NODE_B, "put-op", json.dumps(_operation(FOREIGN_CLUSTER_ID)).encode("utf-8"))

    assert not (root / "oplog" / NODE_B).exists()


def test_rebuild_and_get_state_vector(tmp_path: Path) -> None:
    """rebuild materializes state and get-state-vector returns current counters."""

    root = _init_cluster(tmp_path)
    run_command(root, NODE_B, "put-op", json.dumps(_operation()).encode("utf-8"))

    rebuild = run_command(root, NODE_B, "rebuild")
    state = run_command(root, NODE_B, "get-state-vector")

    assert rebuild["generation"] == 2
    assert state["state_vector"] == {NODE_A: 1, NODE_B: 1}


def test_put_blob_validates_hash_and_writes_content_addressed_file(tmp_path: Path) -> None:
    """put-blob writes only when the provided hash matches the payload."""

    root = _init_cluster(tmp_path)
    raw = b'{"config": true}'
    blob_hash = "sha256:" + hashlib.sha256(raw).hexdigest()

    payload = run_command(root, NODE_B, f"put-blob {blob_hash}", raw)
    path = root / payload["path"]

    assert path.read_bytes() == raw
    assert payload["path"].startswith("config_blobs/sha256/")
    with pytest.raises(ClusterSyncCommandError, match="blob hash mismatch"):
        run_command(root, NODE_B, f"put-blob {'sha256:' + '0' * 64}", raw)


def test_put_secret_blob_uses_owner_only_permissions(tmp_path: Path) -> None:
    """Secret blobs are stored with owner-only permissions."""

    root = _init_cluster(tmp_path)
    raw = b'{"secret": true}'
    blob_hash = "sha256:" + hashlib.sha256(raw).hexdigest()

    payload = run_command(root, NODE_B, f"put-secret-blob {blob_hash}", raw)
    path = root / payload["path"]

    mode = stat.S_IMODE(path.stat().st_mode)

    assert path.read_bytes() == raw
    assert payload["path"].startswith("secret_blobs/sha256/")
    assert mode == 0o600


def test_get_desired_state_returns_materialized_snapshot(tmp_path: Path) -> None:
    """get-desired-state returns a deterministic snapshot rebuilt from oplog."""

    root = _init_cluster(tmp_path)
    append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "bybit_BTCUSDT",
            "version": "7",
            "assigned_host": NODE_B,
            "desired_state": "running",
            "config_manifest_hash": HASH_A,
        },
        created_at=102,
    )

    payload = run_command(root, NODE_B, "get-desired-state")

    assert payload["desired_state"]["instances"]["bybit_BTCUSDT"]["version"] == "7"


def test_main_does_not_read_stdin_for_hello(monkeypatch, tmp_path: Path, capsys) -> None:
    """The CLI hello command must not block waiting for stdin."""

    class BlockingStdin:
        class Buffer:
            def read(self, size: int = -1) -> bytes:
                raise AssertionError("stdin should not be read")

        buffer = Buffer()

    root = _init_cluster(tmp_path)
    monkeypatch.setattr(sys, "stdin", BlockingStdin())

    exit_code = main(["--cluster-root", str(root), "--remote-node", NODE_B, "hello"])
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["ok"] is True
    assert output["remote_node"] == NODE_B
