"""Tests for local multi-master cluster state helpers."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from master import cluster_state as cluster_state_module
from master.cluster_state import (
    ClusterStateError,
    append_operation,
    build_config_manifest,
    compute_config_manifest_hash,
    detect_duplicate_node_ids,
    ensure_local_identity,
    read_local_identity,
    rebuild_materialized_state,
    validate_operation,
    write_operation,
)


CLUSTER_ID = "pbgui-cluster-00000000-0000-4000-8000-000000000001"
FOREIGN_CLUSTER_ID = "pbgui-cluster-00000000-0000-4000-8000-000000000099"
NODE_A = "pbgui-node-00000000-0000-4000-8000-00000000000a"
NODE_B = "pbgui-node-00000000-0000-4000-8000-00000000000b"
NODE_C = "pbgui-node-00000000-0000-4000-8000-00000000000c"
HASH_A = "sha256:" + "a" * 64
HASH_B = "sha256:" + "b" * 64
SECRET_HASH = "sha256:" + "c" * 64


def _init_cluster(tmp_path: Path) -> Path:
    """Create a deterministic local cluster identity for tests."""

    root = tmp_path / "cluster"
    ensure_local_identity(
        root,
        role="master",
        pbname="magicnuc1",
        cluster_id=CLUSTER_ID,
        node_id=NODE_A,
        created_at=100,
    )
    return root


def _operation(actor: str, seq: int, op: str, payload: dict) -> dict:
    """Build a deterministic operation dictionary for tests."""

    operation = dict(payload)
    operation.update(
        {
            "schema_version": 1,
            "cluster_id": CLUSTER_ID,
            "op_id": f"{actor}:{seq:08d}",
            "actor": actor,
            "seq": seq,
            "op": op,
            "created_at": 100 + seq,
        }
    )
    return operation


def test_ensure_local_identity_creates_stable_ids(tmp_path: Path) -> None:
    """Local cluster identity is created once and reused on later calls."""

    root = _init_cluster(tmp_path)

    first = read_local_identity(root)
    second = ensure_local_identity(
        root,
        role="vps",
        pbname="renamed",
        cluster_id=CLUSTER_ID,
        node_id=NODE_A,
        created_at=200,
    )

    assert first["cluster_id"] == CLUSTER_ID
    assert first["node_id"] == NODE_A
    assert second["created_at"] == 100
    assert (root / "cluster_id").read_text(encoding="utf-8").strip() == CLUSTER_ID
    assert (root / "node_id").read_text(encoding="utf-8").strip() == NODE_A


def test_ensure_local_identity_rejects_foreign_existing_cluster(tmp_path: Path) -> None:
    """Existing identity cannot be silently moved into another cluster."""

    root = _init_cluster(tmp_path)

    with pytest.raises(ClusterStateError, match="cluster_id differs"):
        ensure_local_identity(root, cluster_id=FOREIGN_CLUSTER_ID)


def test_detect_duplicate_node_ids() -> None:
    """Duplicate node IDs are reported for join safety checks."""

    duplicates = detect_duplicate_node_ids(
        [
            {"node_id": NODE_A, "ssh_host": "10.0.0.1"},
            {"node_id": NODE_B, "ssh_host": "10.0.0.2"},
            {"node_id": NODE_A, "ssh_host": "10.0.0.3"},
        ]
    )

    assert duplicates == {NODE_A}


def test_atomic_json_write_uses_unique_temp_paths(monkeypatch, tmp_path: Path) -> None:
    """Concurrent-safe atomic writes use a fresh temp file for each write."""

    recorded_sources: list[str] = []
    real_replace = cluster_state_module.os.replace

    def fake_replace(src: str | Path, dst: str | Path) -> None:
        recorded_sources.append(Path(src).name)
        real_replace(src, dst)

    monkeypatch.setattr(cluster_state_module.os, "replace", fake_replace)
    target = tmp_path / "state.json"

    cluster_state_module._atomic_write_json(target, {"value": 1})
    cluster_state_module._atomic_write_json(target, {"value": 2})

    assert len(recorded_sources) == 2
    assert len(set(recorded_sources)) == 2
    assert list(tmp_path.glob("*.tmp")) == []
    assert json.loads(target.read_text(encoding="utf-8")) == {"value": 2}


def test_validate_operation_rejects_foreign_cluster() -> None:
    """Operation validation rejects a foreign cluster when expected."""

    operation = _operation(
        NODE_A,
        1,
        "ADD_NODE",
        {"node_id": NODE_A, "role": "master"},
    )

    with pytest.raises(ClusterStateError, match="foreign cluster_id"):
        validate_operation(operation, expected_cluster_id=FOREIGN_CLUSTER_ID)


def test_append_membership_and_rebuild_materializes_nodes(tmp_path: Path) -> None:
    """Membership operations rebuild cluster_nodes and state_vector."""

    root = _init_cluster(tmp_path)
    append_operation(
        root,
        "ADD_NODE",
        {"node_id": NODE_A, "role": "master", "pbname": "magicnuc1", "sync_enabled": True},
        created_at=101,
    )
    append_operation(
        root,
        "ADD_NODE",
        {"node_id": NODE_B, "role": "vps", "pbname": "manibot93", "ssh_host": "10.0.0.2"},
        created_at=102,
    )
    append_operation(root, "UPDATE_NODE_ADDRESS", {"node_id": NODE_B, "ssh_host": "10.0.0.3"}, created_at=103)

    materialized = rebuild_materialized_state(root)

    nodes = materialized["cluster_nodes"]["nodes"]
    assert nodes[NODE_A]["role"] == "master"
    assert nodes[NODE_A]["sync_mode"] == "outbound_only"
    assert nodes[NODE_A]["sync_enabled"] is True
    assert nodes[NODE_B]["ssh_host"] == "10.0.0.3"
    assert nodes[NODE_B]["sync_mode"] == "reachable"
    assert materialized["state_vector"] == {NODE_A: 3}
    saved = json.loads((root / "cluster_nodes.json").read_text(encoding="utf-8"))
    assert saved == materialized["cluster_nodes"]


def test_write_operation_requests_cluster_sync_once(monkeypatch, tmp_path: Path) -> None:
    """A new operation wakes PBCluster, while replaying the same operation is idempotent."""

    root = _init_cluster(tmp_path)
    touched: list[Path] = []
    monkeypatch.setattr(cluster_state_module, "_touch_sync_request", lambda cluster_root: touched.append(cluster_root))
    operation = _operation(NODE_A, 1, "ADD_NODE", {"node_id": NODE_A, "role": "master"})

    write_operation(root, operation)
    write_operation(root, operation)

    assert touched == [root]


def test_v7_operations_materialize_move_stop_and_tombstone(tmp_path: Path) -> None:
    """V7 operations update desired state and tombstones deterministically."""

    root = _init_cluster(tmp_path)
    append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "bybit_BTC",
            "parent_version": "v0",
            "version": "v1",
            "assigned_host": NODE_B,
            "desired_state": "running",
            "config_manifest_hash": HASH_A,
        },
        created_at=101,
    )
    append_operation(
        root,
        "MOVE_INSTANCE",
        {"instance": "bybit_BTC", "parent_version": "v1", "version": "v2", "from": NODE_B, "to": NODE_C},
        created_at=102,
    )
    append_operation(root, "STOP_INSTANCE", {"instance": "bybit_BTC"}, created_at=103)
    append_operation(root, "DELETE_INSTANCE", {"instance": "bybit_BTC", "version": "v3"}, created_at=104)

    desired = rebuild_materialized_state(root)["desired_state"]

    assert "bybit_BTC" not in desired["instances"]
    assert desired["tombstones"]["bybit_BTC"]["version"] == "v3"
    assert desired["tombstones"]["bybit_BTC"]["deleted_by"] == NODE_A


def test_conflict_detection_marks_same_parent_changes(tmp_path: Path) -> None:
    """Two different changes from the same parent version create a conflict."""

    root = _init_cluster(tmp_path)
    write_operation(
        root,
        _operation(
            NODE_A,
            1,
            "UPSERT_CONFIG",
            {
                "instance": "bybit_BTC",
                "parent_version": "v1",
                "version": "v2a",
                "assigned_host": NODE_B,
                "desired_state": "running",
                "config_manifest_hash": HASH_A,
            },
        ),
    )
    write_operation(
        root,
        _operation(
            NODE_B,
            1,
            "UPSERT_CONFIG",
            {
                "instance": "bybit_BTC",
                "parent_version": "v1",
                "version": "v2b",
                "assigned_host": NODE_C,
                "desired_state": "running",
                "config_manifest_hash": HASH_B,
            },
        ),
    )

    instance = rebuild_materialized_state(root)["desired_state"]["instances"]["bybit_BTC"]

    assert instance["conflicted"] is True
    assert {item["op_id"] for item in instance["conflicts"]} == {
        f"{NODE_A}:00000001",
        f"{NODE_B}:00000001",
    }


def test_tombstone_prevents_stale_config_resurrection(tmp_path: Path) -> None:
    """A stale config upsert cannot recreate an instance after tombstone."""

    root = _init_cluster(tmp_path)
    write_operation(
        root,
        _operation(
            NODE_A,
            1,
            "DELETE_INSTANCE",
            {"instance": "bybit_BTC", "version": "v3"},
        ),
    )
    write_operation(
        root,
        _operation(
            NODE_B,
            1,
            "UPSERT_CONFIG",
            {
                "instance": "bybit_BTC",
                "parent_version": "v1",
                "version": "v2",
                "assigned_host": NODE_B,
                "desired_state": "running",
                "config_manifest_hash": HASH_A,
            },
        ),
    )

    desired = rebuild_materialized_state(root)["desired_state"]

    assert "bybit_BTC" not in desired["instances"]
    assert desired["tombstones"]["bybit_BTC"]["version"] == "v3"


def test_explicit_restore_can_recreate_tombstoned_instance(tmp_path: Path) -> None:
    """A restore-marked upsert recreates an instance after an explicit delete."""

    root = _init_cluster(tmp_path)
    append_operation(root, "DELETE_INSTANCE", {"instance": "bybit_BTC", "version": "v3"}, created_at=101)
    append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "bybit_BTC",
            "parent_version": "v3",
            "version": "v4",
            "assigned_host": NODE_B,
            "desired_state": "stopped",
            "config_manifest_hash": HASH_A,
            "allow_tombstone_recreate": True,
        },
        created_at=102,
    )

    desired = rebuild_materialized_state(root)["desired_state"]

    assert "bybit_BTC" in desired["instances"]
    assert "bybit_BTC" not in desired["tombstones"]
    assert desired["instances"]["bybit_BTC"]["version"] == "v4"


def test_config_manifest_hash_changes_with_syncable_json(tmp_path: Path) -> None:
    """Config manifest hash changes only when syncable JSON content changes."""

    inst_dir = tmp_path / "data" / "run_v7" / "bybit_BTC"
    inst_dir.mkdir(parents=True)
    (inst_dir / "config.json").write_text('{"version": 1}', encoding="utf-8")
    (inst_dir / "BTC.json").write_text('{"coin": "BTC"}', encoding="utf-8")
    (inst_dir / "ignored_coins.json").write_text('["ETH"]', encoding="utf-8")
    first = compute_config_manifest_hash(build_config_manifest(inst_dir))

    (inst_dir / "ignored_coins.json").write_text('["SOL"]', encoding="utf-8")
    assert compute_config_manifest_hash(build_config_manifest(inst_dir)) == first

    (inst_dir / "BTC.json").write_text('{"coin": "BTC", "mode": "p"}', encoding="utf-8")
    second = compute_config_manifest_hash(build_config_manifest(inst_dir))

    assert second != first
    assert second.startswith("sha256:")


def test_api_key_operation_materializes_secret_hash_metadata(tmp_path: Path) -> None:
    """API-key desired state stores metadata and the secret blob hash reference."""

    root = _init_cluster(tmp_path)
    append_operation(
        root,
        "UPSERT_API_KEYS",
        {"api_serial": 42, "payload_hash": HASH_A, "secret_blob_hash": SECRET_HASH},
        created_at=101,
    )

    api_keys = rebuild_materialized_state(root)["desired_state"]["api_keys"]

    assert api_keys == {
        "serial": 42,
        "payload_hash": HASH_A,
        "secret_blob_hash": SECRET_HASH,
        "updated_by": NODE_A,
        "updated_at": 101,
    }


def test_append_operation_rejects_path_traversal_instance(tmp_path: Path) -> None:
    """Invalid instance names are rejected before oplog writes."""

    root = _init_cluster(tmp_path)

    with pytest.raises(ClusterStateError, match="invalid instance"):
        append_operation(
            root,
            "UPSERT_CONFIG",
            {
                "instance": "../escape",
                "version": "v1",
                "assigned_host": NODE_B,
                "desired_state": "running",
                "config_manifest_hash": HASH_A,
            },
        )

    assert not (root / "oplog").exists()
