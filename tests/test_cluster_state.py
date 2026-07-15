"""Tests for local multi-master cluster state helpers."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from cluster_credentials import ensure_node_key_material, sign_operation
from master import cluster_state as cluster_state_module
from master.cluster_state import (
    ClusterStateError,
    append_node_placeholder,
    append_operation,
    build_config_manifest,
    compute_config_manifest_hash,
    create_join_authorization,
    detect_duplicate_node_ids,
    ensure_local_identity,
    load_operations,
    read_local_identity,
    rebuild_materialized_state,
    stage_membership_operations,
    validate_operation,
    write_operation as _write_operation,
)


CLUSTER_ID = "pbgui-cluster-00000000-0000-4000-8000-000000000001"
FOREIGN_CLUSTER_ID = "pbgui-cluster-00000000-0000-4000-8000-000000000099"
NODE_A = "pbgui-node-00000000-0000-4000-8000-00000000000a"
NODE_B = "pbgui-node-00000000-0000-4000-8000-00000000000b"
NODE_C = "pbgui-node-00000000-0000-4000-8000-00000000000c"
HASH_A = "sha256:" + "a" * 64
HASH_B = "sha256:" + "b" * 64
SECRET_HASH = "sha256:" + "c" * 64


def write_operation(root: Path, operation: dict, **kwargs):
    """Treat unsigned membership used by older fixtures as explicit disk history."""

    if operation.get("op") in {
        "ADD_NODE", "UPDATE_NODE", "UPDATE_NODE_ADDRESS", "UPDATE_NODE_SSH",
        "UPDATE_NODE_KEY", "DISABLE_NODE", "REMOVE_NODE",
    } and not kwargs.get("network_input"):
        kwargs["allow_legacy_membership"] = True
    return _write_operation(root, operation, **kwargs)


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


def test_unsigned_membership_requires_explicit_legacy_disk_replay(tmp_path: Path) -> None:
    """Unsigned v1 membership is readable only through the explicit legacy path."""

    root = _init_cluster(tmp_path)
    operation = _operation(
        NODE_A,
        1,
        "ADD_NODE",
        {"node_id": NODE_A, "role": "master", "pbname": "legacy-master"},
    )

    with pytest.raises(ClusterStateError, match="historical replay only"):
        _write_operation(root, operation)
    _write_operation(root, operation, allow_legacy_membership=True)

    assert rebuild_materialized_state(root, write=False)["cluster_nodes"]["nodes"][NODE_A]["pbname"] == "legacy-master"


def test_legacy_local_node_claims_first_v2_key_once(tmp_path: Path) -> None:
    """An unsigned legacy node may self-claim its first key but cannot replace it."""

    root = _init_cluster(tmp_path)
    write_operation(
        root,
        _operation(NODE_A, 1, "ADD_NODE", {"node_id": NODE_A, "role": "master"}),
    )

    append_operation(
        root,
        "UPDATE_NODE",
        {"node_id": NODE_A, "pbname": "upgraded-master"},
        created_at=103,
    )

    operations = load_operations(root)
    assert [operation["op"] for operation in operations] == [
        "ADD_NODE",
        "UPDATE_NODE_KEY",
        "UPDATE_NODE",
    ]
    assert operations[1]["signer_key_id"] == operations[1]["signing_key_id"]
    assert rebuild_materialized_state(root, write=False)["cluster_nodes"]["nodes"][NODE_A][
        "credential_protocol_version"
    ] == 2

    replacement_keys = ensure_node_key_material(tmp_path / "replacement")
    replacement = sign_operation(
        _operation(
            NODE_A,
            4,
            "UPDATE_NODE_KEY",
            {**replacement_keys.public_bundle(NODE_A, "master"), "node_id": NODE_A},
        ),
        replacement_keys.signing_private_key,
        signer_id=NODE_A,
    )
    with pytest.raises(ClusterStateError, match="authenticated key"):
        write_operation(root, replacement, network_input=True)


def test_legacy_key_claim_requires_direct_node_and_next_sequence(tmp_path: Path) -> None:
    """Relays and sequence-gap claims cannot bind a legacy node's first key."""

    root = _init_cluster(tmp_path)
    write_operation(
        root,
        _operation(NODE_A, 1, "ADD_NODE", {"node_id": NODE_A, "role": "master"}),
    )
    keys = ensure_node_key_material(tmp_path / "claim")

    def claim(seq: int) -> dict:
        return sign_operation(
            _operation(
                NODE_A,
                seq,
                "UPDATE_NODE_KEY",
                {**keys.public_bundle(NODE_A, "master"), "node_id": NODE_A},
            ),
            keys.signing_private_key,
            signer_id=NODE_A,
        )

    with pytest.raises(ClusterStateError, match="direct authenticated node transport"):
        stage_membership_operations(
            root,
            [claim(2)],
            expected_cluster_id=CLUSTER_ID,
            authenticated_remote_node=NODE_B,
        )
    with pytest.raises(ClusterStateError, match="direct authenticated node transport"):
        stage_membership_operations(
            root,
            [claim(3)],
            expected_cluster_id=CLUSTER_ID,
            authenticated_remote_node=NODE_A,
        )

    trust = stage_membership_operations(
        root,
        [claim(2)],
        expected_cluster_id=CLUSTER_ID,
        authenticated_remote_node=NODE_A,
    )
    assert trust.signing_keys[NODE_A][0]["key_id"] == keys.public_bundle(NODE_A, "master")[
        "signing_key_id"
    ]


def test_authenticated_master_relays_contiguous_legacy_key_claim(tmp_path: Path) -> None:
    """A directly authenticated active master may relay a self-signed first key claim."""
    root = _init_cluster(tmp_path)
    write_operation(root, _operation(NODE_A, 1, "ADD_NODE", {"node_id": NODE_A, "role": "master"}))
    write_operation(root, _operation(NODE_A, 2, "ADD_NODE", {"node_id": NODE_B, "role": "vps"}))
    keys = ensure_node_key_material(tmp_path / "relay-claim")

    def claim(seq: int) -> dict:
        return sign_operation(
            _operation(
                NODE_B,
                seq,
                "UPDATE_NODE_KEY",
                {**keys.public_bundle(NODE_B, "vps"), "node_id": NODE_B},
            ),
            keys.signing_private_key,
            signer_id=NODE_B,
        )

    trust = stage_membership_operations(
        root,
        [claim(1)],
        expected_cluster_id=CLUSTER_ID,
        authenticated_remote_node=NODE_A,
    )
    assert trust.signing_keys[NODE_B][0]["key_id"] == keys.public_bundle(NODE_B, "vps")["signing_key_id"]

    with pytest.raises(ClusterStateError, match="direct authenticated node transport"):
        stage_membership_operations(
            root,
            [claim(2)],
            expected_cluster_id=CLUSTER_ID,
            authenticated_remote_node=NODE_A,
        )


def test_append_membership_and_rebuild_materializes_nodes(tmp_path: Path) -> None:
    """Membership operations rebuild cluster_nodes and state_vector."""

    root = _init_cluster(tmp_path)
    append_operation(
        root,
        "ADD_NODE",
        {"node_id": NODE_A, "role": "master", "pbname": "magicnuc1", "sync_enabled": True},
        created_at=101,
    )
    append_node_placeholder(
        root,
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


def test_remove_node_operation_hides_materialized_node(tmp_path: Path) -> None:
    """REMOVE_NODE removes a node from materialized membership without editing history."""

    root = _init_cluster(tmp_path)
    append_operation(root, "ADD_NODE", {"node_id": NODE_A, "role": "master", "pbname": "master"}, created_at=101)
    append_node_placeholder(root, {"node_id": NODE_B, "role": "master", "pbname": "old-master"}, created_at=102)
    append_operation(root, "REMOVE_NODE", {"node_id": NODE_B}, created_at=103)

    materialized = rebuild_materialized_state(root)
    operations = cluster_state_module.load_operations(root, expected_cluster_id=CLUSTER_ID)

    assert NODE_A in materialized["cluster_nodes"]["nodes"]
    assert NODE_B not in materialized["cluster_nodes"]["nodes"]
    assert [op["op"] for op in operations] == ["ADD_NODE", "UPDATE_NODE", "REMOVE_NODE"]


def test_removed_node_stays_hidden_after_later_membership_update(tmp_path: Path) -> None:
    """Later peer membership updates must not recreate a removed node."""

    root = _init_cluster(tmp_path)
    write_operation(
        root,
        _operation(
            NODE_A,
            1,
            "ADD_NODE",
            {"node_id": NODE_B, "role": "vps", "pbname": "manibot70", "ssh_host": "10.0.0.2", "sync_mode": "disabled", "sync_enabled": False},
        ) | {"created_at": 101},
    )
    write_operation(root, _operation(NODE_A, 2, "REMOVE_NODE", {"node_id": NODE_B}) | {"created_at": 102})
    write_operation(
        root,
        _operation(
            NODE_C,
            1,
            "UPDATE_NODE_ADDRESS",
            {"node_id": NODE_B, "ssh_host": "10.0.0.3", "sync_mode": "reachable", "sync_enabled": True},
        ) | {"created_at": 103},
    )

    materialized = rebuild_materialized_state(root)
    operations = cluster_state_module.load_operations(root, expected_cluster_id=CLUSTER_ID)

    assert NODE_B not in materialized["cluster_nodes"]["nodes"]
    assert [operation["created_at"] for operation in operations] == [101, 102, 103]


def test_newer_membership_update_wins_across_actors(tmp_path: Path) -> None:
    """Newer node membership settings must not be overwritten by older peer ops."""

    root = _init_cluster(tmp_path)
    write_operation(
        root,
        _operation(
            NODE_A,
            1,
            "ADD_NODE",
            {"node_id": NODE_B, "role": "vps", "pbname": "runner", "ssh_host": "10.0.0.2", "sync_mode": "reachable", "sync_enabled": True},
        ) | {"created_at": 200},
    )
    write_operation(
        root,
        _operation(
            NODE_A,
            2,
            "UPDATE_NODE",
            {"node_id": NODE_B, "sync_mode": "disabled", "sync_enabled": False},
        ) | {"created_at": 300},
    )
    write_operation(
        root,
        _operation(
            NODE_C,
            1,
            "UPDATE_NODE",
            {"node_id": NODE_B, "ssh_host": "10.0.0.2", "sync_mode": "reachable", "sync_enabled": True},
        ) | {"created_at": 250},
    )

    materialized = rebuild_materialized_state(root)
    operations = cluster_state_module.load_operations(root, expected_cluster_id=CLUSTER_ID)
    node = materialized["cluster_nodes"]["nodes"][NODE_B]

    assert [operation["created_at"] for operation in operations] == [200, 250, 300]
    assert node["sync_mode"] == "disabled"
    assert node["sync_enabled"] is False


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


def test_later_linear_config_save_clears_old_parent_conflict(tmp_path: Path) -> None:
    """A later config version can supersede stale parent-version collisions."""

    root = _init_cluster(tmp_path)
    write_operation(
        root,
        _operation(
            NODE_A,
            1,
            "UPSERT_CONFIG",
            {
                "instance": "bybit_BTC",
                "parent_version": "1",
                "version": "2",
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
                "parent_version": "1",
                "version": "2",
                "assigned_host": NODE_C,
                "desired_state": "running",
                "config_manifest_hash": HASH_B,
            },
        ),
    )
    write_operation(
        root,
        _operation(
            NODE_A,
            2,
            "UPSERT_CONFIG",
            {
                "instance": "bybit_BTC",
                "parent_version": "2",
                "version": "3",
                "assigned_host": NODE_C,
                "desired_state": "running",
                "config_manifest_hash": HASH_B,
            },
        ),
    )

    instance = rebuild_materialized_state(root)["desired_state"]["instances"]["bybit_BTC"]

    assert instance["version"] == "3"
    assert instance["conflicted"] is False
    assert "conflicts" not in instance


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
    (inst_dir / "monitor_cache.json").write_text('{"runtime": 1}', encoding="utf-8")
    (inst_dir / "monitor.json").write_text('{"st": 100}', encoding="utf-8")
    first = compute_config_manifest_hash(build_config_manifest(inst_dir))

    (inst_dir / "ignored_coins.json").write_text('["SOL"]', encoding="utf-8")
    assert compute_config_manifest_hash(build_config_manifest(inst_dir)) == first

    (inst_dir / "monitor_cache.json").write_text('{"runtime": 2}', encoding="utf-8")
    assert compute_config_manifest_hash(build_config_manifest(inst_dir)) == first

    (inst_dir / "monitor.json").write_text('{"st": 200}', encoding="utf-8")
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


def test_signed_cutoff_preserves_history_and_rejects_post_cutoff_downgrade(tmp_path: Path) -> None:
    """Historical v1 operations remain readable while new unsanitized writes are denied."""

    root = _init_cluster(tmp_path)
    legacy = append_operation(
        root,
        "UPSERT_API_KEYS",
        {"api_serial": 1, "payload_hash": HASH_A, "secret_blob_hash": SECRET_HASH},
    )
    append_operation(
        root,
        "CREDENTIAL_CUTOFF",
        {
            "cutoff_generation": 1,
            "parent_generation": 0,
            "state_vector": {NODE_A: int(legacy["seq"])},
            "min_protocol": 2,
            "obsolete_secret_blob_hashes": [SECRET_HASH],
        },
    )
    rebuild_materialized_state(root)

    assert any(operation["op_id"] == legacy["op_id"] for operation in load_operations(root))
    assert "api_keys" not in rebuild_materialized_state(root, write=False)["desired_state"]
    with pytest.raises(ClusterStateError, match="downgrade"):
        append_operation(
            root,
            "UPSERT_API_KEYS",
            {"api_serial": 2, "payload_hash": HASH_B, "secret_blob_hash": HASH_A},
        )
    append_operation(
        root,
        "UPSERT_API_KEYS",
        {
            "api_serial": 2,
            "payload_hash": HASH_B,
            "secret_blob_hash": HASH_A,
            "sanitized": True,
            "credential_protocol_version": 2,
        },
    )
    assert rebuild_materialized_state(root, write=False)["desired_state"]["api_keys"]["sanitized"] is True


def test_new_replica_replays_obsolete_operation_metadata_contiguously(tmp_path: Path) -> None:
    """A joining replica advances across cutoff history without receiving obsolete plaintext."""

    source = _init_cluster(tmp_path / "source")
    membership = append_operation(source, "ADD_NODE", {"node_id": NODE_A, "role": "master"})
    legacy = append_operation(
        source,
        "UPSERT_API_KEYS",
        {"api_serial": 1, "payload_hash": HASH_A, "secret_blob_hash": SECRET_HASH},
    )
    cutoff = append_operation(
        source,
        "CREDENTIAL_CUTOFF",
        {
            "cutoff_generation": 1,
            "parent_generation": 0,
            "state_vector": {NODE_A: int(legacy["seq"])},
            "min_protocol": 2,
            "obsolete_secret_blob_hashes": [SECRET_HASH],
        },
    )
    destination = _init_cluster(tmp_path / "destination")

    write_operation(destination, membership, network_input=True)
    write_operation(destination, legacy, network_input=True)
    write_operation(destination, cutoff, network_input=True)
    replayed = rebuild_materialized_state(destination, write=False)

    assert replayed["state_vector"][NODE_A] == cutoff["seq"]
    assert "api_keys" not in replayed["desired_state"]
    assert not (destination / "secret_blobs").exists()


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


def _add_local_crypto_membership(root: Path) -> None:
    """Publish the test node's application crypto bundle in membership."""

    bundle = ensure_node_key_material(root).public_bundle(NODE_A, "master")
    append_operation(
        root,
        "ADD_NODE",
        {**bundle, "node_id": NODE_A, "role": "master", "pbname": "master-a"},
        created_at=101,
    )


def test_v1_operations_remain_unsigned_and_v2_signature_detects_tampering(tmp_path: Path) -> None:
    """Only additive v2 operations require signatures and payload tampering fails."""

    root = _init_cluster(tmp_path)
    _add_local_crypto_membership(root)
    legacy = append_operation(root, "STOP_INSTANCE", {"instance": "bot-a"}, created_at=102)
    signed = append_operation(
        root,
        "UPSERT_SECRET",
        {
            "secret_id": "cmc_" + "1" * 32,
            "secret_kind": "cmc_api_key",
            "audience": "cluster",
            "generation": 1,
            "parent_generation": 0,
            "sealed_blob_hash": SECRET_HASH,
        },
        created_at=103,
    )

    assert "signature" not in legacy
    assert signed["signature_algorithm"] == "Ed25519"
    validate_operation(signed, cluster_root=root)
    tampered = dict(signed)
    tampered["generation"] = 2
    with pytest.raises(ClusterStateError, match="signature"):
        validate_operation(tampered, cluster_root=root)


def test_initial_self_add_and_authorized_join_are_signed(tmp_path: Path) -> None:
    """Bootstrap is self-signed and later joins require separate master approval."""

    root = _init_cluster(tmp_path)
    bootstrap = append_operation(
        root,
        "ADD_NODE",
        {"node_id": NODE_A, "role": "master", "pbname": "master-a"},
        created_at=101,
    )
    authorization = create_join_authorization(root, NODE_B, "vps", created_at=102)
    remote_keys = ensure_node_key_material(tmp_path / "remote-b")
    bundle = remote_keys.public_bundle(NODE_B, "vps")
    joined = sign_operation(
        _operation(
            NODE_B,
            1,
            "ADD_NODE",
            {
                **bundle,
                "node_id": NODE_B,
                "role": "vps",
                "state_replica": True,
                "membership_authorization": authorization,
            },
        ),
        remote_keys.signing_private_key,
        signer_id=NODE_B,
    )

    write_operation(root, joined, network_input=True)
    nodes = rebuild_materialized_state(root, write=False)["cluster_nodes"]["nodes"]

    assert bootstrap["signer_id"] == bootstrap["actor"] == NODE_A
    assert bootstrap["signature_algorithm"] == "Ed25519"
    assert nodes[NODE_B]["state_replica"] is True


def test_membership_rejects_forged_actor_and_unapproved_master_self_add(tmp_path: Path) -> None:
    """Signer identity and explicit join approval cannot be replaced by payload claims."""

    root = _init_cluster(tmp_path)
    append_operation(root, "ADD_NODE", {"node_id": NODE_A, "role": "master"}, created_at=101)
    attacker = ensure_node_key_material(tmp_path / "attacker")
    bundle = attacker.public_bundle(NODE_B, "master")
    forged_actor = sign_operation(
        _operation(NODE_B, 1, "UPDATE_NODE", {"node_id": NODE_A, "pbname": "forged"}),
        attacker.signing_private_key,
        signer_id=NODE_C,
    )
    rogue_master = sign_operation(
        _operation(
            NODE_B,
            1,
            "ADD_NODE",
            {
                **bundle,
                "node_id": NODE_B,
                "role": "master",
                "state_replica": True,
                "membership_authorization": {"kind": "bootstrap"},
            },
        ),
        attacker.signing_private_key,
        signer_id=NODE_B,
    )

    with pytest.raises(ClusterStateError, match="signer_id must match actor"):
        write_operation(root, forged_actor, network_input=True)
    with pytest.raises(ClusterStateError, match="empty cluster"):
        write_operation(root, rogue_master, network_input=True)


def test_vps_cannot_claim_master_role_for_credential_admin_operations(tmp_path: Path) -> None:
    """Credential authorization uses authenticated role history, never payload claims."""

    root = _init_cluster(tmp_path)
    append_operation(root, "ADD_NODE", {"node_id": NODE_A, "role": "master"}, created_at=101)
    vps_keys = ensure_node_key_material(tmp_path / "vps-keys")
    bundle = vps_keys.public_bundle(NODE_B, "vps")
    write_operation(
        root,
        _operation(
            NODE_A,
            2,
            "UPDATE_NODE",
            {**bundle, "node_id": NODE_B, "role": "vps", "credential_protocol_version": 2, "credential_capable": True},
        ) | {"created_at": 102},
        allow_legacy_membership=True,
    )
    forged_admin = sign_operation(
        _operation(
            NODE_B,
            1,
            "SET_CMC_KEY_STATE",
            {
                "key_id": "cmc_" + "9" * 32,
                "state": "active",
                "state_generation": 1,
                "parent_generation": 0,
                "role": "master",
                "actor_role_epoch": 1,
                "actor_membership_op_id": f"{NODE_A}:00000002",
            },
        ) | {"created_at": 103},
        vps_keys.signing_private_key,
        signer_id=NODE_B,
    )

    with pytest.raises(ClusterStateError, match="authenticated master"):
        write_operation(root, forged_admin, network_input=True)

    master_operation = append_operation(
        root,
        "SET_CMC_KEY_STATE",
        {
            "key_id": "cmc_" + "9" * 32,
            "state": "active",
            "state_generation": 1,
            "parent_generation": 0,
        },
    )
    assert master_operation["actor"] == NODE_A


def test_demoted_master_cannot_backdate_network_credential_admin_operation(tmp_path: Path) -> None:
    """Network authorization uses current role, not an attacker-controlled timestamp."""

    root = _init_cluster(tmp_path)
    append_operation(root, "ADD_NODE", {"node_id": NODE_A, "role": "master"}, created_at=100)
    remote_keys = ensure_node_key_material(tmp_path / "remote-master-keys")
    authorization = create_join_authorization(root, NODE_B, "master", created_at=101)
    remote_add = sign_operation(
        _operation(
            NODE_B,
            1,
            "ADD_NODE",
            {
                **remote_keys.public_bundle(NODE_B, "master"),
                "node_id": NODE_B,
                "role": "master",
                "state_replica": True,
                "membership_authorization": authorization,
            },
        ) | {"created_at": 102},
        remote_keys.signing_private_key,
        signer_id=NODE_B,
    )
    write_operation(root, remote_add, network_input=True)
    historical = sign_operation(
        _operation(
            NODE_B,
            2,
            "SET_CMC_KEY_STATE",
            {
                "key_id": "cmc_" + "7" * 32,
                "state": "active",
                "state_generation": 1,
                "parent_generation": 0,
                "actor_role_epoch": 1,
                "actor_membership_op_id": str(remote_add["op_id"]),
            },
        ) | {"created_at": 999},
        remote_keys.signing_private_key,
        signer_id=NODE_B,
    )
    write_operation(root, historical, network_input=True)
    append_operation(
        root,
        "UPDATE_NODE",
        {"node_id": NODE_B, "role": "vps"},
        created_at=200,
    )
    backdated = sign_operation(
        _operation(
            NODE_B,
            3,
            "SET_CMC_KEY_STATE",
            {
                "key_id": "cmc_" + "8" * 32,
                "state": "active",
                "state_generation": 1,
                "parent_generation": 0,
                "actor_role_epoch": 1,
                "actor_membership_op_id": str(remote_add["op_id"]),
            },
        ) | {"created_at": 103},
        remote_keys.signing_private_key,
        signer_id=NODE_B,
    )

    with pytest.raises(ClusterStateError, match="membership epoch is not current"):
        write_operation(root, backdated, network_input=True)
    assert historical["op_id"] in {operation["op_id"] for operation in load_operations(root)}


def test_vps_self_ack_requires_exact_current_signed_membership_epoch(tmp_path: Path) -> None:
    """Missing or stale same-role VPS ancestry is rejected before oplog publication."""

    root = _init_cluster(tmp_path)
    append_operation(root, "ADD_NODE", {"node_id": NODE_A, "role": "master"}, created_at=100)
    remote_keys = ensure_node_key_material(tmp_path / "remote-vps-keys")
    authorization = create_join_authorization(root, NODE_B, "vps", created_at=101)
    remote_add = sign_operation(
        _operation(
            NODE_B,
            1,
            "ADD_NODE",
            {
                **remote_keys.public_bundle(NODE_B, "vps"),
                "node_id": NODE_B,
                "role": "vps",
                "state_replica": True,
                "membership_authorization": authorization,
            },
        ) | {"created_at": 102},
        remote_keys.signing_private_key,
        signer_id=NODE_B,
    )
    write_operation(root, remote_add, network_input=True)
    historical = sign_operation(
        _operation(
            NODE_B,
            2,
            "WRITER_FREEZE_ACK",
            {
                "freeze_generation": 1,
                "node_id": NODE_B,
                "frozen": True,
                "actor_role_epoch": 1,
                "actor_membership_op_id": str(remote_add["op_id"]),
            },
        ) | {"created_at": 103},
        remote_keys.signing_private_key,
        signer_id=NODE_B,
    )
    write_operation(root, historical, network_input=True)
    promoted = append_operation(
        root,
        "UPDATE_NODE",
        {"node_id": NODE_B, "role": "master"},
        created_at=104,
    )
    restored = append_operation(
        root,
        "UPDATE_NODE",
        {"node_id": NODE_B, "role": "vps"},
        created_at=105,
    )
    assert promoted["op_id"] != restored["op_id"]

    ack_payload = {
        "freeze_generation": 2,
        "node_id": NODE_B,
        "frozen": True,
    }
    missing = sign_operation(
        _operation(NODE_B, 3, "WRITER_FREEZE_ACK", ack_payload),
        remote_keys.signing_private_key,
        signer_id=NODE_B,
    )
    op_path = root / "oplog" / NODE_B / "00000003.json"
    with pytest.raises(ClusterStateError, match="actor_role_epoch"):
        write_operation(root, missing, network_input=True)
    assert not op_path.exists()

    stale = sign_operation(
        _operation(
            NODE_B,
            3,
            "WRITER_FREEZE_ACK",
            {
                **ack_payload,
                "actor_role_epoch": 1,
                "actor_membership_op_id": str(remote_add["op_id"]),
            },
        ),
        remote_keys.signing_private_key,
        signer_id=NODE_B,
    )
    with pytest.raises(ClusterStateError, match="membership epoch is not current"):
        write_operation(root, stale, network_input=True)
    assert not op_path.exists()

    current = sign_operation(
        _operation(
            NODE_B,
            3,
            "WRITER_FREEZE_ACK",
            {
                **ack_payload,
                "actor_role_epoch": 3,
                "actor_membership_op_id": str(restored["op_id"]),
            },
        ),
        remote_keys.signing_private_key,
        signer_id=NODE_B,
    )
    write_operation(root, current, network_input=True)

    rebuilt = rebuild_materialized_state(root, write=False)
    assert op_path.is_file()
    assert historical["op_id"] in {operation["op_id"] for operation in load_operations(root)}
    assert rebuilt["desired_state"]["credential_migration"]["freeze_acks"] == {}


def test_disabled_local_actor_cannot_append_new_credential_operation(tmp_path: Path) -> None:
    """Local V2 creation enforces current active membership before writing its oplog file."""

    root = _init_cluster(tmp_path)
    append_operation(root, "ADD_NODE", {"node_id": NODE_A, "role": "master"}, created_at=100)
    remote_keys = ensure_node_key_material(tmp_path / "remote-master-keys")
    authorization = create_join_authorization(root, NODE_B, "master", created_at=101)
    remote_add = sign_operation(
        _operation(
            NODE_B,
            1,
            "ADD_NODE",
            {
                **remote_keys.public_bundle(NODE_B, "master"),
                "node_id": NODE_B,
                "role": "master",
                "state_replica": True,
                "membership_authorization": authorization,
            },
        ) | {"created_at": 102},
        remote_keys.signing_private_key,
        signer_id=NODE_B,
    )
    write_operation(root, remote_add, network_input=True)
    disable_local = sign_operation(
        _operation(NODE_B, 2, "DISABLE_NODE", {"node_id": NODE_A})
        | {"created_at": 103},
        remote_keys.signing_private_key,
        signer_id=NODE_B,
    )
    write_operation(root, disable_local, network_input=True)

    op_path = root / "oplog" / NODE_A / "00000002.json"
    with pytest.raises(ClusterStateError, match="actor is disabled"):
        append_operation(
            root,
            "WRITER_FREEZE_ACK",
            {"freeze_generation": 1, "node_id": NODE_A, "frozen": True},
        )

    assert not op_path.exists()
    assert rebuild_materialized_state(root, write=False)["cluster_nodes"]["nodes"][NODE_A][
        "enabled"
    ] is False


def test_key_rotation_preserves_pre_rotation_verification_and_bounds_old_key(tmp_path: Path) -> None:
    """Credential operations use the actor key valid at their sequence across rotation."""

    root = _init_cluster(tmp_path)
    old_keys = ensure_node_key_material(tmp_path / "old-b")
    new_keys = ensure_node_key_material(tmp_path / "new-b")
    old_bundle = old_keys.public_bundle(NODE_B, "master")
    write_operation(
        root,
        _operation(NODE_A, 1, "ADD_NODE", {**old_bundle, "node_id": NODE_B, "role": "master"}),
    )
    pre_rotation = sign_operation(
        _operation(
            NODE_B,
            1,
            "UPSERT_SECRET",
            {
                "secret_id": "cmc_" + "7" * 32,
                "secret_kind": "cmc_api_key",
                "audience": "cluster",
                "generation": 1,
                "parent_generation": 0,
                "sealed_blob_hash": HASH_A,
                "actor_role_epoch": 1,
                "actor_membership_op_id": f"{NODE_A}:00000001",
            },
        ),
        old_keys.signing_private_key,
        signer_id=NODE_B,
    )
    new_bundle = new_keys.public_bundle(NODE_B, "master")
    rotation = sign_operation(
        _operation(
            NODE_B,
            2,
            "UPDATE_NODE_KEY",
            {**new_bundle, "node_id": NODE_B, "role": "master"},
        ),
        old_keys.signing_private_key,
        signer_id=NODE_B,
    )
    post_rotation = sign_operation(
        _operation(
            NODE_B,
            3,
            "TOMBSTONE_SECRET",
            {
                "secret_id": "cmc_" + "7" * 32,
                "secret_kind": "cmc_api_key",
                "generation": 2,
                "parent_generation": 1,
                "actor_role_epoch": 1,
                "actor_membership_op_id": f"{NODE_A}:00000001",
            },
        ),
        new_keys.signing_private_key,
        signer_id=NODE_B,
    )

    write_operation(root, pre_rotation, network_input=True)
    write_operation(root, rotation, network_input=True)
    write_operation(root, post_rotation, network_input=True)
    assert [operation["op"] for operation in cluster_state_module.load_operations(root)[-3:]] == [
        "UPSERT_SECRET",
        "UPDATE_NODE_KEY",
        "TOMBSTONE_SECRET",
    ]

    stale_old_key = sign_operation(
        _operation(NODE_B, 4, "STOP_INSTANCE", {"instance": "bot-a"}),
        old_keys.signing_private_key,
        signer_id=NODE_B,
    )
    stale_old_key["op"] = "UPSERT_SECRET"
    stale_old_key.update({
        "secret_id": "cmc_" + "8" * 32,
        "secret_kind": "cmc_api_key",
        "audience": "cluster",
        "generation": 1,
        "parent_generation": 0,
        "sealed_blob_hash": HASH_B,
        "actor_role_epoch": 1,
        "actor_membership_op_id": f"{NODE_A}:00000001",
    })
    stale_old_key = sign_operation(stale_old_key, old_keys.signing_private_key, signer_id=NODE_B)
    with pytest.raises(ClusterStateError, match="authenticated key"):
        write_operation(root, stale_old_key, network_input=True)


def test_state_vector_stops_at_gap_and_advances_after_repair(tmp_path: Path) -> None:
    """State vectors advertise only the highest contiguous actor sequence."""

    root = _init_cluster(tmp_path)
    write_operation(root, _operation(NODE_A, 1, "ADD_NODE", {"node_id": NODE_A, "role": "master"}))
    write_operation(root, _operation(NODE_A, 3, "STOP_INSTANCE", {"instance": "bot-a"}))

    assert rebuild_materialized_state(root)["state_vector"] == {NODE_A: 1}

    write_operation(root, _operation(NODE_A, 2, "START_INSTANCE", {"instance": "bot-a"}))

    assert rebuild_materialized_state(root)["state_vector"] == {NODE_A: 3}


def test_secret_parent_conflict_and_tombstone_are_materialized_additively(tmp_path: Path) -> None:
    """Sibling secret generations conflict and a newer tombstone dominates them."""

    root = _init_cluster(tmp_path)
    _add_local_crypto_membership(root)
    secret_id = "cmc_" + "2" * 32
    base = {
        "secret_id": secret_id,
        "secret_kind": "cmc_api_key",
        "audience": "cluster",
        "parent_generation": 0,
    }
    append_operation(root, "UPSERT_SECRET", {**base, "generation": 1, "sealed_blob_hash": HASH_A}, created_at=102)
    append_operation(root, "UPSERT_SECRET", {**base, "generation": 2, "sealed_blob_hash": HASH_B}, created_at=103)

    conflicted = rebuild_materialized_state(root)["desired_state"]

    assert conflicted["secrets"][secret_id]["conflicted"] is True
    assert len(conflicted["secrets"][secret_id]["conflicts"]) == 2

    append_operation(
        root,
        "TOMBSTONE_SECRET",
        {
            "secret_id": secret_id,
            "secret_kind": "cmc_api_key",
            "generation": 3,
            "parent_generation": 2,
        },
        created_at=104,
    )
    desired = rebuild_materialized_state(root)["desired_state"]

    assert secret_id not in desired["secrets"]
    assert desired["secret_tombstones"][secret_id]["generation"] == 3
    assert desired["cmc_pool"] == {"entries": {}, "authorities": {}}
    assert desired["credential_migration"] == {"freeze_acks": {}, "inventory_acks": {}}


def test_recipient_generation_cas_materializes_latest_valid_chain_and_conflicts(
    tmp_path: Path,
) -> None:
    """Recipient updates advance independently, ignore orphans, and expose sibling conflicts."""

    root = _init_cluster(tmp_path)
    _add_local_crypto_membership(root)
    secret_id = "cmc_" + "9" * 32
    node = rebuild_materialized_state(root, write=False)["cluster_nodes"]["nodes"][NODE_A]
    registration = {NODE_A: node["encryption_key_id"]}
    append_operation(
        root,
        "UPSERT_SECRET",
        {
            "secret_id": secret_id,
            "secret_kind": "cmc_api_key",
            "audience": "cluster",
            "generation": 1,
            "parent_generation": 0,
            "recipient_generation": 1,
            "parent_recipient_generation": 0,
            "membership_generation": 1,
            "recipient_ids": [NODE_A],
            "recipient_key_ids": registration,
            "sealed_blob_hash": HASH_A,
        },
        created_at=102,
    )
    base = {
        "secret_id": secret_id,
        "provider_generation": 1,
        "recipient_generation": 2,
        "parent_recipient_generation": 1,
        "membership_generation": 2,
        "recipient_ids": [NODE_A],
        "recipient_key_ids": registration,
    }
    append_operation(root, "UPDATE_SECRET_RECIPIENTS", {**base, "sealed_blob_hash": HASH_B}, created_at=103)
    append_operation(root, "UPDATE_SECRET_RECIPIENTS", {**base, "sealed_blob_hash": SECRET_HASH}, created_at=104)

    secret = rebuild_materialized_state(root, write=False)["desired_state"]["secrets"][secret_id]

    assert secret["generation"] == 1
    assert secret["recipient_generation"] == 2
    assert secret["recipient_conflicted"] is True
    assert secret["conflicted"] is True
    assert len(secret["recipient_conflicts"]) == 2

    append_operation(
        root,
        "TOMBSTONE_SECRET",
        {
            "secret_id": secret_id,
            "secret_kind": "cmc_api_key",
            "generation": 2,
            "parent_generation": 1,
        },
        created_at=105,
    )
    desired = rebuild_materialized_state(root, write=False)["desired_state"]
    assert secret_id not in desired["secrets"]
    assert desired["secret_tombstones"][secret_id]["generation"] == 2


def test_cmc_pool_authority_and_writer_freeze_materialize_without_secrets(tmp_path: Path) -> None:
    """Signed pool and migration operations build only non-secret desired metadata."""

    root = _init_cluster(tmp_path)
    _add_local_crypto_membership(root)
    key_id = "cmc_" + "5" * 32
    append_operation(
        root,
        "UPSERT_CMC_POOL_ENTRY",
        {
            "key_id": key_id,
            "secret_id": key_id,
            "catalog_generation": 1,
            "parent_generation": 0,
            "state": "pending",
            "label": "primary",
            "quota_domain_id": "free-plan",
        },
    )
    append_operation(
        root,
        "SET_CMC_KEY_STATE",
        {
            "key_id": key_id,
            "state": "active",
            "state_generation": 1,
            "parent_generation": 0,
        },
    )
    append_operation(
        root,
        "SET_CMC_AUTHORITY",
        {
            "quota_domain_id": "free-plan",
            "authority_node_id": NODE_A,
            "authority_epoch": 1,
            "parent_epoch": 0,
        },
    )
    append_operation(root, "WRITER_FREEZE", {"freeze_generation": 1, "frozen": True})
    append_operation(
        root,
        "WRITER_FREEZE_ACK",
        {"freeze_generation": 1, "node_id": NODE_A, "frozen": True},
    )
    append_operation(
        root,
        "CREDENTIAL_INVENTORY_ACK",
        {
            "freeze_generation": 1,
            "node_id": NODE_A,
            "source_fingerprints": {"ini": "a" * 64},
            "source_generations": {"ini": 7},
        },
    )

    desired = rebuild_materialized_state(root)["desired_state"]

    assert desired["cmc_pool"]["entries"][key_id]["state"] == "active"
    assert desired["cmc_pool"]["authorities"]["free-plan"]["authority_node_id"] == NODE_A
    assert desired["credential_migration"]["frozen"] is True
    assert desired["credential_migration"]["freeze_acks"][NODE_A]["frozen"] is True
    assert desired["credential_migration"]["inventory_acks"][NODE_A]["source_generations"] == {"ini": 7}
    assert "api_key" not in json.dumps(desired)
