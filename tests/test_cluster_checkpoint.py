"""Tests for deterministic PBCluster shadow checkpoints and retention previews."""

from __future__ import annotations

import json
import hashlib
import os
from pathlib import Path

import pytest
import credential_migration

from master import cluster_checkpoint as checkpoint_module
from master.cluster_checkpoint import (
    DEFAULT_HISTORY_SECONDS,
    ClusterCheckpointError,
    build_shadow_checkpoint,
    checkpoint_materialized_state,
    create_shadow_checkpoint,
    create_checkpoint_ack,
    create_checkpoint_commit_proof,
    create_checkpoint_proposal,
    garbage_collect_blobs,
    install_rebootstrap_checkpoint,
    materialize_checkpoint_tail,
    activate_checkpoint,
    read_active_checkpoint,
    read_shadow_checkpoint,
    replica_blob_hashes,
    prune_operation_history,
    retention_preview,
    verify_shadow_checkpoint,
)
from master.cluster_state import (
    ClusterStateError,
    append_operation,
    create_join_authorization,
    ensure_local_identity,
    load_operations,
    rebuild_materialized_state,
    write_operation,
)


CLUSTER_ID = "pbgui-cluster-00000000-0000-4000-8000-000000000001"
NODE_ID = "pbgui-node-00000000-0000-4000-8000-00000000000a"
NODE_B = "pbgui-node-00000000-0000-4000-8000-00000000000b"
HASH_A = "sha256:" + "a" * 64
HASH_B = "sha256:" + "b" * 64
NOW = 2_000_000_000


def _cluster(tmp_path: Path) -> Path:
    """Create one deterministic signed single-master cluster."""

    root = tmp_path / "cluster"
    ensure_local_identity(
        root,
        role="master",
        pbname="master-a",
        cluster_id=CLUSTER_ID,
        node_id=NODE_ID,
        created_at=100,
    )
    append_operation(
        root,
        "ADD_NODE",
        {"node_id": NODE_ID, "role": "master", "pbname": "master-a"},
        created_at=NOW - DEFAULT_HISTORY_SECONDS - 100,
    )
    return root


def test_shadow_checkpoint_is_deterministic_and_matches_full_replay(tmp_path: Path) -> None:
    """Equal cluster state yields one checkpoint ID and an exact materialized snapshot."""

    root = _cluster(tmp_path)
    append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "bybit_BTC",
            "version": "1",
            "parent_version": "0",
            "assigned_host": NODE_ID,
            "desired_state": "running",
            "config_manifest_hash": HASH_A,
        },
        created_at=NOW - 60,
    )
    first = build_shadow_checkpoint(root, created_at=NOW)
    second = build_shadow_checkpoint(root, created_at=NOW + 30)

    assert first["checkpoint_id"] == second["checkpoint_id"]
    assert first["history_cutoff"] == NOW - DEFAULT_HISTORY_SECONDS
    assert first["baseline_vector"] == {NODE_ID: 2}
    assert first["blob_refs"] == {"config": [HASH_A], "secret": [], "sealed": []}
    assert checkpoint_materialized_state(first) == rebuild_materialized_state(root, write=False)
    assert first["membership_trust"]["nodes"][NODE_ID]["role"] == "master"
    assert first["membership_trust"]["signing_keys"][NODE_ID]


def test_shadow_checkpoint_persists_owner_only_and_rejects_tampering(tmp_path: Path) -> None:
    """Persisted checkpoints are owner-only and every state change invalidates their hash."""

    root = _cluster(tmp_path)
    created = create_shadow_checkpoint(root, created_at=NOW)
    path = root / "checkpoints" / "shadow.json"

    assert read_shadow_checkpoint(root) == created
    if os.name == "posix":
        assert path.stat().st_mode & 0o777 == 0o600

    tampered = json.loads(path.read_text(encoding="utf-8"))
    tampered["materialized"]["cluster_nodes"]["generation"] = 999
    path.write_text(json.dumps(tampered), encoding="utf-8")
    with pytest.raises(ClusterCheckpointError, match="state hash mismatch"):
        read_shadow_checkpoint(root)


def test_replica_blob_hashes_keeps_checkpoint_refs_after_oplog_prune(tmp_path: Path) -> None:
    """Replica coverage includes checkpoint state after its source operation is gone."""

    root = _cluster(tmp_path)
    raw = b'{"checkpoint":"config"}\n'
    digest = hashlib.sha256(raw).hexdigest()
    blob_hash = f"sha256:{digest}"
    path = root / "config_blobs" / "sha256" / digest[:2] / f"{digest}.json"
    path.parent.mkdir(parents=True)
    path.write_bytes(raw)
    append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "bybit_BTC",
            "version": "1",
            "parent_version": "0",
            "assigned_host": NODE_ID,
            "desired_state": "running",
            "config_manifest_hash": blob_hash,
        },
        created_at=NOW - 60,
    )
    create_shadow_checkpoint(root, created_at=NOW)
    for operation_path in (root / "oplog").glob("*/*.json"):
        operation_path.unlink()

    hashes = replica_blob_hashes(root)

    assert hashes == {"config": [blob_hash], "secret": [], "sealed": []}


def test_replica_blob_hashes_treats_manifest_children_as_leaves(tmp_path: Path) -> None:
    """A child config file with a files key is not parsed as another manifest."""

    root = _cluster(tmp_path)
    child_raw = b'{"files":"ordinary config value"}\n'
    child_digest = hashlib.sha256(child_raw).hexdigest()
    child_hash = f"sha256:{child_digest}"
    manifest = {"files": {"config.json": {"sha256": child_digest, "size": len(child_raw)}}}
    manifest_raw = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode("utf-8")
    manifest_digest = hashlib.sha256(manifest_raw).hexdigest()
    manifest_hash = f"sha256:{manifest_digest}"
    for blob_hash, raw in ((child_hash, child_raw), (manifest_hash, manifest_raw)):
        digest = blob_hash.removeprefix("sha256:")
        path = root / "config_blobs" / "sha256" / digest[:2] / f"{digest}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
    append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "bybit_BTC",
            "version": "1",
            "parent_version": "0",
            "assigned_host": NODE_ID,
            "desired_state": "running",
            "config_manifest_hash": manifest_hash,
        },
        created_at=NOW - 60,
    )

    hashes = replica_blob_hashes(root)

    assert hashes == {"config": sorted([manifest_hash, child_hash]), "secret": [], "sealed": []}


def test_replica_blob_hashes_does_not_parse_generic_payload_as_manifest(tmp_path: Path) -> None:
    """A generic config-store payload may contain a files key without manifest semantics."""

    root = _cluster(tmp_path)
    payload_raw = b'{"files":"api user name"}\n'
    payload_digest = hashlib.sha256(payload_raw).hexdigest()
    payload_hash = f"sha256:{payload_digest}"
    payload_path = root / "config_blobs" / "sha256" / payload_digest[:2] / f"{payload_digest}.json"
    payload_path.parent.mkdir(parents=True)
    payload_path.write_bytes(payload_raw)
    secret_hash = "sha256:" + "1" * 64
    append_operation(
        root,
        "UPSERT_API_KEYS",
        {"api_serial": 1, "payload_hash": payload_hash, "secret_blob_hash": secret_hash},
        created_at=NOW - 60,
    )

    hashes = replica_blob_hashes(root)

    assert hashes == {"config": [payload_hash], "secret": [secret_hash], "sealed": []}


def test_replica_blob_hashes_filters_obsolete_previous_checkpoint_secret(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A sealed cutoff prevents previous-checkpoint plaintext secret repair."""

    root = _cluster(tmp_path)
    old_secret = "sha256:" + "1" * 64
    active = {
        "checkpoint_id": "sha256:" + "a" * 64,
        "blob_refs": {"config": [], "secret": [], "sealed": []},
        "migration_seal": {"status": "sealed", "obsolete_secret_blob_hashes": [old_secret]},
    }
    previous = {
        "checkpoint_id": "sha256:" + "b" * 64,
        "blob_refs": {"config": [], "secret": [old_secret], "sealed": []},
        "migration_seal": {"status": "pending"},
    }
    monkeypatch.setattr(checkpoint_module, "read_active_checkpoint", lambda _root: active)
    monkeypatch.setattr(
        checkpoint_module,
        "_read_checkpoint_commit_unlocked",
        lambda _root: {"previous_checkpoint_id": previous["checkpoint_id"]},
    )
    monkeypatch.setattr(checkpoint_module, "_read_checkpoint_object", lambda _root, _checkpoint_id: previous)

    hashes = replica_blob_hashes(root)

    assert hashes == {"config": [], "secret": [], "sealed": []}


def test_replica_blob_hashes_ignores_stale_shadow_after_activation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """An active checkpoint prevents stale shadow refs from reviving collected blobs."""

    root = _cluster(tmp_path)
    monkeypatch.setattr(
        checkpoint_module,
        "build_migration_seal",
        lambda materialized: {
            "schema_version": 1,
            "status": "sealed",
            "cluster_id": materialized["cluster_nodes"]["cluster_id"],
            "active_node_ids": [NODE_ID],
            "blockers": [],
        },
    )
    active = build_shadow_checkpoint(root, created_at=NOW)
    proposal = create_checkpoint_proposal(root, active, created_at=NOW, expires_at=NOW + 60)
    ack = create_checkpoint_ack(root, active, proposal, created_at=NOW + 1)
    proof = create_checkpoint_commit_proof(root, active, proposal, [ack], created_at=NOW + 2)
    activate_checkpoint(root, active, commit_proof=proof, activated_at=NOW + 2)

    raw = b'{"stale":"shadow"}\n'
    digest = hashlib.sha256(raw).hexdigest()
    blob_hash = f"sha256:{digest}"
    path = root / "config_blobs" / "sha256" / digest[:2] / f"{digest}.json"
    path.parent.mkdir(parents=True)
    path.write_bytes(raw)
    operation = append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "bybit_BTC",
            "version": "1",
            "parent_version": "0",
            "assigned_host": NODE_ID,
            "desired_state": "running",
            "config_manifest_hash": blob_hash,
        },
        created_at=NOW + 3,
    )
    create_shadow_checkpoint(root, created_at=NOW + 4)
    (root / "oplog" / NODE_ID / f"{int(operation['seq']):08d}.json").unlink()
    path.unlink()

    hashes = replica_blob_hashes(root)

    assert hashes == {"config": [], "secret": [], "sealed": []}


def test_shadow_checkpoint_rejects_foreign_cluster_and_symlink(tmp_path: Path) -> None:
    """Checkpoint trust boundaries reject foreign IDs and symlink replacement."""

    root = _cluster(tmp_path)
    checkpoint = build_shadow_checkpoint(root, created_at=NOW)
    with pytest.raises(ClusterCheckpointError, match="another cluster"):
        verify_shadow_checkpoint(checkpoint, expected_cluster_id="pbgui-cluster-foreign")

    create_shadow_checkpoint(root, created_at=NOW)
    path = root / "checkpoints" / "shadow.json"
    external = tmp_path / "external.json"
    external.write_text(json.dumps(checkpoint), encoding="utf-8")
    path.unlink()
    path.symlink_to(external)
    with pytest.raises(ClusterCheckpointError, match="symlink"):
        read_shadow_checkpoint(root)


def test_retention_preview_keeps_seven_days_and_never_deletes(tmp_path: Path) -> None:
    """Dry-run retention selects only baseline operations older than seven days."""

    root = _cluster(tmp_path)
    append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "bybit_BTC",
            "version": "1",
            "parent_version": "0",
            "assigned_host": NODE_ID,
            "desired_state": "stopped",
            "config_manifest_hash": HASH_A,
        },
        created_at=NOW - DEFAULT_HISTORY_SECONDS,
    )
    append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "bybit_ETH",
            "version": "1",
            "parent_version": "0",
            "assigned_host": NODE_ID,
            "desired_state": "running",
            "config_manifest_hash": HASH_B,
        },
        created_at=NOW - DEFAULT_HISTORY_SECONDS + 1,
    )
    checkpoint = create_shadow_checkpoint(root, created_at=NOW)
    before = sorted(path.relative_to(root).as_posix() for path in (root / "oplog").glob("*/*.json"))

    preview = retention_preview(root, checkpoint, now=NOW)

    assert preview["status"] == "dry_run"
    assert preview["eligible_operations"] == 1
    assert preview["items"][0]["op"] == "ADD_NODE"
    assert preview["retained_operations"] == 2
    assert preview["eligible_bytes"] > 0
    assert sorted(path.relative_to(root).as_posix() for path in (root / "oplog").glob("*/*.json")) == before


def test_retention_preview_normalizes_fresh_receipt_age_for_late_replica(tmp_path: Path) -> None:
    """A verified shadow projects canonical age while actual pruning stays fail-closed."""

    root = _cluster(tmp_path)
    operation_path = root / "oplog" / NODE_ID / "00000001.json"
    os.utime(operation_path, (NOW, NOW))
    checkpoint = create_shadow_checkpoint(root, created_at=NOW)

    preview = retention_preview(root, checkpoint, now=NOW)
    actual = prune_operation_history(root, now=NOW, dry_run=True)

    assert preview["eligible_operations"] == 1
    assert preview["retained_operations"] == 0
    assert actual["eligible_operations"] == 0
    assert "checkpoint_missing" in actual["blockers"]


def test_actual_prune_keeps_fresh_receipt_despite_committed_safe_baseline(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A committed previous checkpoint does not bypass actual receipt-age safety."""

    root = _cluster(tmp_path)
    monkeypatch.setattr(
        checkpoint_module,
        "build_migration_seal",
        lambda materialized: {
            "schema_version": 1,
            "status": "sealed",
            "cluster_id": materialized["cluster_nodes"]["cluster_id"],
            "active_node_ids": [NODE_ID],
            "blockers": [],
        },
    )
    first = build_shadow_checkpoint(root, created_at=NOW - 100)
    first_proposal = create_checkpoint_proposal(root, first, created_at=NOW - 100, expires_at=NOW + 100)
    first_ack = create_checkpoint_ack(root, first, first_proposal, created_at=NOW - 99)
    first_proof = create_checkpoint_commit_proof(root, first, first_proposal, [first_ack], created_at=NOW - 98)
    activate_checkpoint(root, first, commit_proof=first_proof, activated_at=NOW - 98)
    append_operation(root, "UPDATE_NODE", {"node_id": NODE_ID, "pbname": "renamed"}, created_at=NOW - 60)
    second = build_shadow_checkpoint(root, created_at=NOW)
    second_proposal = create_checkpoint_proposal(root, second, created_at=NOW, expires_at=NOW + 100)
    second_ack = create_checkpoint_ack(root, second, second_proposal, created_at=NOW + 1)
    second_proof = create_checkpoint_commit_proof(root, second, second_proposal, [second_ack], created_at=NOW + 2)
    activate_checkpoint(root, second, commit_proof=second_proof, activated_at=NOW + 2)
    first_operation_path = root / "oplog" / NODE_ID / "00000001.json"
    os.utime(first_operation_path, (NOW, NOW))

    report = prune_operation_history(root, now=NOW, dry_run=True)

    assert report["previous_checkpoint_id"] == first["checkpoint_id"]
    assert report["safe_baseline"] == first["baseline_vector"]
    assert report["eligible_operations"] == 0


def test_retention_preview_drops_freshly_received_old_blob_reference(tmp_path: Path) -> None:
    """A late replica does not require blobs referenced only by projected-pruned ops."""

    root = _cluster(tmp_path)
    old_hash = "sha256:" + "1" * 64
    current_raw = b'{"current":true}\n'
    current_digest = hashlib.sha256(current_raw).hexdigest()
    current_hash = f"sha256:{current_digest}"
    current_path = root / "config_blobs" / "sha256" / current_digest[:2] / f"{current_digest}.json"
    current_path.parent.mkdir(parents=True)
    current_path.write_bytes(current_raw)
    old_operation = append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "bybit_BTC",
            "version": "1",
            "parent_version": "0",
            "assigned_host": NODE_ID,
            "desired_state": "stopped",
            "config_manifest_hash": old_hash,
        },
        created_at=NOW - DEFAULT_HISTORY_SECONDS - 100,
    )
    append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "bybit_BTC",
            "version": "2",
            "parent_version": "1",
            "assigned_host": NODE_ID,
            "desired_state": "running",
            "config_manifest_hash": current_hash,
        },
        created_at=NOW - 60,
    )
    old_path = root / "oplog" / NODE_ID / f"{int(old_operation['seq']):08d}.json"
    os.utime(old_path, (NOW, NOW))
    checkpoint = create_shadow_checkpoint(root, created_at=NOW)

    preview = retention_preview(root, checkpoint, now=NOW)

    assert preview["blob_gc"]["status"] == "projected"
    assert not any(str(item).startswith("blob_projection_failed:") for item in preview["blob_gc"]["blockers"])


def test_required_blob_digest_ignores_local_garbage(tmp_path: Path) -> None:
    """Local garbage changes candidates without changing the required-set digest."""

    root = _cluster(tmp_path)
    current_raw = b'{"required":true}\n'
    current_digest = hashlib.sha256(current_raw).hexdigest()
    current_hash = f"sha256:{current_digest}"
    current_path = root / "config_blobs" / "sha256" / current_digest[:2] / f"{current_digest}.json"
    current_path.parent.mkdir(parents=True)
    current_path.write_bytes(current_raw)
    append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "bybit_BTC",
            "version": "1",
            "parent_version": "0",
            "assigned_host": NODE_ID,
            "desired_state": "running",
            "config_manifest_hash": current_hash,
        },
        created_at=NOW - 60,
    )
    checkpoint = build_shadow_checkpoint(root, created_at=NOW)
    before = retention_preview(root, checkpoint, now=NOW)
    orphan_raw = b'{"orphan":true}\n'
    orphan_digest = hashlib.sha256(orphan_raw).hexdigest()
    orphan_path = root / "config_blobs" / "sha256" / orphan_digest[:2] / f"{orphan_digest}.json"
    orphan_path.parent.mkdir(parents=True, exist_ok=True)
    orphan_path.write_bytes(orphan_raw)
    os.utime(orphan_path, (NOW - 2 * 24 * 60 * 60, NOW - 2 * 24 * 60 * 60))

    after = retention_preview(root, checkpoint, now=NOW)

    assert before["blob_gc"]["reachable_blobs"] == 1
    assert before["blob_gc"]["reachable"] == {"config": 1, "secret": 0, "sealed": 0}
    assert before["blob_gc"]["reachable_digest"].startswith("sha256:")
    assert before["blob_gc"]["reachable_digest"] == after["blob_gc"]["reachable_digest"]
    assert before["blob_gc"]["eligible_blobs"] == 0
    assert after["blob_gc"]["eligible_blobs"] == 1


def test_checkpoint_refuses_actor_sequence_gaps(tmp_path: Path) -> None:
    """A missing operation prevents a checkpoint from hiding an incomplete actor log."""

    root = _cluster(tmp_path)
    append_operation(
        root,
        "UPDATE_NODE",
        {"node_id": NODE_ID, "pbname": "renamed"},
        created_at=NOW,
    )
    (root / "oplog" / NODE_ID / "00000001.json").unlink()

    with pytest.raises(ClusterCheckpointError, match="sequence gap"):
        build_shadow_checkpoint(root, created_at=NOW)


def test_checkpoint_tail_matches_full_replay_across_operation_families(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Membership, V7, V2, and policy tail operations reproduce full replay."""

    root = _cluster(tmp_path)
    monkeypatch.setattr(
        checkpoint_module,
        "build_migration_seal",
        lambda materialized: {
            "schema_version": 1,
            "status": "sealed",
            "cluster_id": materialized["cluster_nodes"]["cluster_id"],
            "active_node_ids": [NODE_ID],
            "blockers": [],
        },
    )
    checkpoint = build_shadow_checkpoint(root, created_at=NOW)
    proposal = create_checkpoint_proposal(root, checkpoint, created_at=NOW, expires_at=NOW + 60)
    ack = create_checkpoint_ack(root, checkpoint, proposal, created_at=NOW + 1)
    proof = create_checkpoint_commit_proof(root, checkpoint, proposal, [ack], created_at=NOW + 2)
    activate_checkpoint(root, checkpoint, commit_proof=proof, activated_at=NOW + 2)
    append_operation(root, "UPDATE_NODE", {"node_id": NODE_ID, "pbname": "renamed"}, created_at=NOW + 1)
    append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "bybit_BTC",
            "version": "1",
            "parent_version": "0",
            "assigned_host": NODE_ID,
            "desired_state": "running",
            "config_manifest_hash": HASH_A,
        },
        created_at=NOW + 2,
    )
    append_operation(
        root,
        "SET_CMC_KEY_STATE",
        {
            "key_id": "cmc_" + "1" * 32,
            "state": "disabled",
            "state_generation": 1,
            "parent_generation": 0,
        },
        created_at=NOW + 3,
    )
    append_operation(
        root,
        "SET_RETENTION_POLICY",
        {
            "generation": 1,
            "parent_generation": 0,
            "mode": "report_only",
            "history_days": 30,
        },
        created_at=NOW + 4,
    )
    baseline = checkpoint["baseline_vector"]
    tail = [
        operation for operation in load_operations(root)
        if int(operation["seq"]) > int(baseline.get(str(operation["actor"]), 0))
    ]

    assert materialize_checkpoint_tail(checkpoint, tail) == rebuild_materialized_state(root, write=False)


def test_active_checkpoint_advances_sequences_and_rotates_previous(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Activation is monotonic and subsequent operations start above its baseline."""

    root = _cluster(tmp_path)
    monkeypatch.setattr(
        checkpoint_module,
        "build_migration_seal",
        lambda materialized: {
            "schema_version": 1,
            "status": "sealed",
            "cluster_id": materialized["cluster_nodes"]["cluster_id"],
            "active_node_ids": [NODE_ID],
            "blockers": [],
        },
    )
    first = build_shadow_checkpoint(root, created_at=NOW)
    baseline_operation = json.loads(
        (root / "oplog" / NODE_ID / "00000001.json").read_text(encoding="utf-8")
    )
    first_proposal = create_checkpoint_proposal(
        root,
        first,
        created_at=NOW,
        expires_at=NOW + 60,
    )
    first_ack = create_checkpoint_ack(
        root,
        first,
        first_proposal,
        created_at=NOW + 1,
    )
    first_proof = create_checkpoint_commit_proof(
        root,
        first,
        first_proposal,
        [first_ack],
        created_at=NOW + 2,
    )
    first_commit = activate_checkpoint(
        root,
        first,
        commit_proof=first_proof,
        activated_at=NOW,
    )
    assert first_commit["epoch"] == 1
    assert read_active_checkpoint(root) == first
    assert rebuild_materialized_state(root, write=False) == first["materialized"]
    with pytest.raises(ClusterStateError, match="checkpoint baseline"):
        write_operation(root, baseline_operation)
    stale_branch = {
        "schema_version": 1,
        "cluster_id": CLUSTER_ID,
        "op_id": f"{NODE_ID}:00000002",
        "actor": NODE_ID,
        "seq": 2,
        "op": "STOP_INSTANCE",
        "instance": "bybit_BTC",
        "created_at": NOW + 1,
        "base_checkpoint_id": "sha256:" + "0" * 64,
        "checkpoint_epoch": 1,
    }
    with pytest.raises(ClusterStateError, match="stale checkpoint branch"):
        write_operation(root, stale_branch)
    unsigned_current_branch = dict(stale_branch)
    unsigned_current_branch["base_checkpoint_id"] = first["checkpoint_id"]
    with pytest.raises(ClusterStateError, match="signer_id|signature"):
        write_operation(root, unsigned_current_branch)

    update = append_operation(
        root,
        "UPDATE_NODE",
        {"node_id": NODE_ID, "pbname": "after-checkpoint"},
        created_at=NOW + 1,
    )
    assert update["seq"] == int(first["baseline_vector"][NODE_ID]) + 1
    assert rebuild_materialized_state(root, write=False)["cluster_nodes"]["nodes"][NODE_ID]["pbname"] == "after-checkpoint"

    second = build_shadow_checkpoint(root, created_at=NOW + 2)
    second_proposal = create_checkpoint_proposal(
        root,
        second,
        created_at=NOW + 2,
        expires_at=NOW + 62,
    )
    second_ack = create_checkpoint_ack(
        root,
        second,
        second_proposal,
        created_at=NOW + 3,
    )
    second_proof = create_checkpoint_commit_proof(
        root,
        second,
        second_proposal,
        [second_ack],
        created_at=NOW + 4,
    )
    second_commit = activate_checkpoint(
        root,
        second,
        commit_proof=second_proof,
        activated_at=NOW + 2,
    )
    assert second_commit["epoch"] == 2
    assert second_commit["previous_checkpoint_id"] == first["checkpoint_id"]
    assert read_active_checkpoint(root) == second
    second_digest = second["checkpoint_id"].removeprefix("sha256:")
    (root / "checkpoints" / "objects" / f"{second_digest}.json").write_text("{}", encoding="utf-8")
    assert read_active_checkpoint(root) == second
    (root / "checkpoints" / "objects" / f"{second_digest}.backup.json").write_text("{}", encoding="utf-8")
    assert read_active_checkpoint(root) == first


def test_prune_requires_explicit_policy_and_reconstructs_from_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Eligible history is pruned immediately after every state-based safety gate passes."""

    root = _cluster(tmp_path)
    monkeypatch.setattr(
        checkpoint_module,
        "build_migration_seal",
        lambda materialized: {
            "schema_version": 1,
            "status": "sealed",
            "cluster_id": materialized["cluster_nodes"]["cluster_id"],
            "active_node_ids": [NODE_ID],
            "blockers": [],
        },
    )
    monkeypatch.setattr(credential_migration, "credential_migration_is_complete", lambda _root: True)
    append_operation(
        root,
        "SET_RETENTION_POLICY",
        {
            "generation": 1,
            "parent_generation": 0,
            "mode": "oplog",
            "history_days": 7,
        },
        created_at=NOW - 60,
    )
    checkpoint = build_shadow_checkpoint(root, created_at=NOW - 30)
    proposal = create_checkpoint_proposal(
        root,
        checkpoint,
        created_at=NOW - 200,
        expires_at=NOW + 200,
    )
    ack = create_checkpoint_ack(root, checkpoint, proposal, created_at=NOW - 190)
    proof = create_checkpoint_commit_proof(
        root,
        checkpoint,
        proposal,
        [ack],
        created_at=NOW - 180,
    )
    activate_checkpoint(root, checkpoint, commit_proof=proof, activated_at=NOW - 10)
    old_mtime = NOW - 8 * 24 * 60 * 60
    for path in (root / "oplog").glob("*/*.json"):
        os.utime(path, (old_mtime, old_mtime))

    report = prune_operation_history(root, now=NOW)

    assert report["status"] == "complete"
    assert report["deleted_operations"] == 1
    remaining_paths = list((root / "oplog").glob("*/*.json"))
    assert len(remaining_paths) == 1
    assert json.loads(remaining_paths[0].read_text(encoding="utf-8"))["op"] == "SET_RETENTION_POLICY"
    assert "activation_delay" not in report["blockers"]
    assert rebuild_materialized_state(root, write=False) == checkpoint["materialized"]


def test_blob_gc_runs_automatically_without_a_stability_delay(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """An old unreachable blob is swept in the first healthy automatic evaluation."""

    root = _cluster(tmp_path)
    monkeypatch.setattr(
        checkpoint_module,
        "build_migration_seal",
        lambda materialized: {
            "schema_version": 1,
            "status": "sealed",
            "cluster_id": materialized["cluster_nodes"]["cluster_id"],
            "active_node_ids": [NODE_ID],
            "blockers": [],
        },
    )
    monkeypatch.setattr(credential_migration, "credential_migration_is_complete", lambda _root: True)
    append_operation(
        root,
        "SET_RETENTION_POLICY",
        {
            "generation": 1,
            "parent_generation": 0,
            "mode": "oplog",
            "history_days": 7,
        },
        created_at=NOW - 9 * 24 * 60 * 60,
    )
    orphan_raw = b'{"orphan":true}\n'
    orphan_digest = hashlib.sha256(orphan_raw).hexdigest()
    orphan = root / "config_blobs" / "sha256" / orphan_digest[:2] / f"{orphan_digest}.json"
    orphan.parent.mkdir(parents=True)
    orphan.write_bytes(orphan_raw)
    checkpoint = build_shadow_checkpoint(root, created_at=NOW - 2 * 24 * 60 * 60)
    proposal = create_checkpoint_proposal(root, checkpoint, created_at=NOW - 200, expires_at=NOW + 200)
    ack = create_checkpoint_ack(root, checkpoint, proposal, created_at=NOW - 190)
    proof = create_checkpoint_commit_proof(root, checkpoint, proposal, [ack], created_at=NOW - 180)
    activate_checkpoint(root, checkpoint, commit_proof=proof, activated_at=NOW - 170)
    old_mtime = NOW - 8 * 24 * 60 * 60
    for path in [*list((root / "oplog").glob("*/*.json")), orphan]:
        os.utime(path, (old_mtime, old_mtime))
    assert prune_operation_history(root, now=NOW)["status"] == "complete"

    first = garbage_collect_blobs(root, now=NOW)
    preview = retention_preview(root, checkpoint, now=NOW)
    fresh_raw = b'{"uploading":true}\n'
    fresh_digest = hashlib.sha256(fresh_raw).hexdigest()
    fresh = root / "config_blobs" / "sha256" / fresh_digest[:2] / f"{fresh_digest}.json"
    fresh.parent.mkdir(parents=True, exist_ok=True)
    fresh.write_bytes(fresh_raw)
    os.utime(fresh, (NOW - 30 * 60, NOW - 30 * 60))
    second = garbage_collect_blobs(root, now=NOW + 1)
    third = garbage_collect_blobs(root, now=NOW + 60 * 60 + 1)

    assert first["status"] == "complete"
    assert first["deleted_blobs"] == 1
    assert first["blockers"] == []
    assert preview["blob_gc"]["status"] == "complete"
    assert preview["blob_gc"]["eligible_blobs"] == 1
    assert preview["blob_gc"]["eligible_bytes"] == len(orphan_raw)
    assert preview["blob_gc"]["deleted_blobs"] == 1
    assert second["status"] == "ready"
    assert second["deleted_blobs"] == 0
    assert third["status"] == "complete"
    assert third["deleted_blobs"] == 1
    assert not orphan.exists()
    assert not fresh.exists()


def test_retention_preview_projects_blob_gc_before_checkpoint_commit(tmp_path: Path) -> None:
    """A read-only shadow report predicts old unreachable blobs before cleanup is enabled."""

    root = _cluster(tmp_path)
    orphan_raw = b'{"projected":true}\n'
    orphan_digest = hashlib.sha256(orphan_raw).hexdigest()
    orphan = root / "config_blobs" / "sha256" / orphan_digest[:2] / f"{orphan_digest}.json"
    orphan.parent.mkdir(parents=True)
    orphan.write_bytes(orphan_raw)
    old_mtime = NOW - 2 * 24 * 60 * 60
    os.utime(orphan, (old_mtime, old_mtime))
    checkpoint = build_shadow_checkpoint(root, created_at=NOW)

    preview = retention_preview(root, checkpoint, now=NOW)

    assert preview["blob_gc"]["status"] == "projected"
    assert preview["blob_gc"]["source"] == "projected"
    assert preview["blob_gc"]["eligible_blobs"] == 1
    assert preview["blob_gc"]["eligible_bytes"] == len(orphan_raw)
    assert "checkpoint_missing" in preview["blob_gc"]["blockers"]
    assert "blob_gc_not_enabled" in preview["blob_gc"]["blockers"]
    assert not (root / "retention").exists()


def test_retention_preview_projects_blobs_released_by_pruned_tail(tmp_path: Path) -> None:
    """Projection excludes old blob references only when their operations will be pruned."""

    root = _cluster(tmp_path)
    old_raw = b'{"version":1}\n'
    new_raw = b'{"version":2}\n'
    hashes = []
    for raw in (old_raw, new_raw):
        digest = hashlib.sha256(raw).hexdigest()
        path = root / "config_blobs" / "sha256" / digest[:2] / f"{digest}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
        hashes.append(f"sha256:{digest}")
        os.utime(path, (NOW - 2 * 24 * 60 * 60, NOW - 2 * 24 * 60 * 60))
    append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "bybit_BTC",
            "version": "1",
            "parent_version": "0",
            "assigned_host": NODE_ID,
            "desired_state": "stopped",
            "config_manifest_hash": hashes[0],
        },
        created_at=NOW - DEFAULT_HISTORY_SECONDS - 100,
    )
    append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "bybit_BTC",
            "version": "2",
            "parent_version": "1",
            "assigned_host": NODE_ID,
            "desired_state": "running",
            "config_manifest_hash": hashes[1],
        },
        created_at=NOW - 60,
    )
    for path in (root / "oplog").glob("*/*.json"):
        os.utime(path, (NOW - DEFAULT_HISTORY_SECONDS - 200, NOW - DEFAULT_HISTORY_SECONDS - 200))
    checkpoint = build_shadow_checkpoint(root, created_at=NOW)

    preview = retention_preview(root, checkpoint, now=NOW)

    assert preview["blob_gc"]["eligible_blobs"] == 1
    assert preview["blob_gc"]["eligible_bytes"] == len(old_raw)


def test_blob_projection_allows_sealed_obsolete_secret_to_be_absent(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A sealed migration may remove an old secret while its operation tail is retained."""

    root = _cluster(tmp_path)
    old_secret = "sha256:" + "1" * 64
    payload_hashes = []
    for index in (1, 2):
        raw = json.dumps({"payload": index}).encode()
        digest = hashlib.sha256(raw).hexdigest()
        path = root / "config_blobs" / "sha256" / digest[:2] / f"{digest}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
        payload_hashes.append(f"sha256:{digest}")
    new_secret_raw = b'{"secret":"current"}\n'
    new_secret_digest = hashlib.sha256(new_secret_raw).hexdigest()
    new_secret = f"sha256:{new_secret_digest}"
    new_secret_path = root / "secret_blobs" / "sha256" / new_secret_digest[:2] / f"{new_secret_digest}.json"
    new_secret_path.parent.mkdir(parents=True)
    new_secret_path.write_bytes(new_secret_raw)
    append_operation(
        root,
        "UPSERT_API_KEYS",
        {"api_serial": 1, "payload_hash": payload_hashes[0], "secret_blob_hash": old_secret},
        created_at=NOW - 2 * 24 * 60 * 60,
    )
    append_operation(
        root,
        "UPSERT_API_KEYS",
        {"api_serial": 2, "payload_hash": payload_hashes[1], "secret_blob_hash": new_secret},
        created_at=NOW - 24 * 60 * 60,
    )
    monkeypatch.setattr(
        checkpoint_module,
        "build_migration_seal",
        lambda materialized: {
            "schema_version": 1,
            "status": "sealed",
            "cluster_id": materialized["cluster_nodes"]["cluster_id"],
            "active_node_ids": [NODE_ID],
            "obsolete_secret_blob_hashes": [old_secret],
            "blockers": [],
        },
    )
    checkpoint = build_shadow_checkpoint(root, created_at=NOW)

    preview = retention_preview(root, checkpoint, now=NOW)

    assert preview["blob_gc"]["status"] == "projected"
    assert not any("reachable blob is missing" in blocker for blocker in preview["blob_gc"]["blockers"])


def test_checkpoint_join_bootstraps_new_node_without_genesis_oplog(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A coordinator-anchored join installs checkpoint state before self-add."""

    coordinator = _cluster(tmp_path / "coordinator")
    monkeypatch.setattr(
        checkpoint_module,
        "build_migration_seal",
        lambda materialized: {
            "schema_version": 1,
            "status": "sealed",
            "cluster_id": materialized["cluster_nodes"]["cluster_id"],
            "active_node_ids": [NODE_ID],
            "blockers": [],
        },
    )
    checkpoint = build_shadow_checkpoint(coordinator, created_at=NOW)
    proposal = create_checkpoint_proposal(coordinator, checkpoint, created_at=NOW, expires_at=NOW + 60)
    ack = create_checkpoint_ack(coordinator, checkpoint, proposal, created_at=NOW + 1)
    proof = create_checkpoint_commit_proof(coordinator, checkpoint, proposal, [ack], created_at=NOW + 2)
    authorization = create_join_authorization(coordinator, NODE_B, "vps", created_at=NOW + 3)
    anchor_key = checkpoint["membership_trust"]["nodes"][NODE_ID]["signing_public_key"]

    joining = tmp_path / "joining" / "cluster"
    ensure_local_identity(
        joining,
        role="vps",
        pbname="joining-vps",
        cluster_id=CLUSTER_ID,
        node_id=NODE_B,
        created_at=NOW,
    )
    installed = install_rebootstrap_checkpoint(
        joining,
        checkpoint,
        proof,
        installed_at=NOW + 4,
        join_authorization=authorization,
        join_anchor_public_key=anchor_key,
    )
    membership = append_operation(
        joining,
        "ADD_NODE",
        {
            "node_id": NODE_B,
            "role": "vps",
            "pbname": "joining-vps",
            "membership_authorization": authorization,
        },
        created_at=NOW + 5,
    )

    assert installed["status"] == "installed"
    assert membership["base_checkpoint_id"] == checkpoint["checkpoint_id"]
    state = rebuild_materialized_state(joining, write=False)
    assert set(state["cluster_nodes"]["nodes"]) == {NODE_ID, NODE_B}
