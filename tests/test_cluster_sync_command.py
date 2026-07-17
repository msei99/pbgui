"""Tests for the restricted Cluster Sync command wrapper."""

from __future__ import annotations

import base64
import hashlib
import json
import stat
import sys
from pathlib import Path

import pytest

import cluster_sync_command
from cluster_credentials import (
    SecretContext,
    SecretRecipient,
    ensure_node_key_material,
    seal_secret,
    serialize_sealed_secret,
    sign_operation,
)
from cluster_sync_command import ClusterSyncCommandError, main, run_command
from credential_store import CredentialStore
from master.cluster_state import (
    append_operation,
    credential_lifecycle_status,
    create_join_authorization,
    ensure_local_identity,
    load_operations,
    read_local_identity,
    rebuild_materialized_state,
    write_operation,
)


CLUSTER_ID = "pbgui-cluster-00000000-0000-4000-8000-000000000001"
FOREIGN_CLUSTER_ID = "pbgui-cluster-00000000-0000-4000-8000-000000000099"
NODE_A = "pbgui-node-00000000-0000-4000-8000-00000000000a"
NODE_B = "pbgui-node-00000000-0000-4000-8000-00000000000b"
NODE_C = "pbgui-node-00000000-0000-4000-8000-00000000000c"
HASH_A = "sha256:" + "a" * 64


def _init_cluster(tmp_path: Path) -> Path:
    """Create a deterministic cluster with one historical remote member."""

    root = tmp_path / "cluster"
    ensure_local_identity(
        root,
        role="master",
        pbname="master-a",
        cluster_id=CLUSTER_ID,
        node_id=NODE_A,
        created_at=100,
    )
    write_operation(root, _legacy_membership(NODE_A, 1, NODE_B, "vps", created_at=101), allow_legacy_membership=True)
    return root


def test_current_clean_credential_scan_ack_skips_repeated_managed_scan(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A current clean ACK avoids repeating the expensive managed-file scan."""

    migration = {
        "frozen": True,
        "freeze_generation": 42,
        "cutoff": {"cutoff_generation": 1},
        "cleanup_acks": {NODE_A: {"cutoff_generation": 1}},
        "scan_acks": {NODE_A: {
            "freeze_generation": 42,
            "cutoff_generation": 1,
            "status": "clean",
        }},
    }
    monkeypatch.setattr(
        cluster_sync_command,
        "read_materialized_state",
        lambda *_args, **_kwargs: {"desired_state": {"credential_migration": migration}},
    )

    def fail_scan(_root: Path) -> list[str]:
        raise AssertionError("current scan ACK must be reused")

    monkeypatch.setattr("credential_migration.local_managed_credential_scan", fail_scan)

    result = cluster_sync_command._append_credential_scan_ack(tmp_path / "data" / "cluster", NODE_A)

    assert result == {"scan_status": "clean"}


def _legacy_membership(
    actor: str,
    seq: int,
    node_id: str,
    role: str,
    *,
    created_at: int,
    **payload,
) -> dict:
    """Build an unsigned v1 membership record used only for disk-replay fixtures."""

    return {
        "schema_version": 1,
        "cluster_id": CLUSTER_ID,
        "op_id": f"{actor}:{seq:08d}",
        "actor": actor,
        "seq": seq,
        "op": "ADD_NODE",
        "created_at": created_at,
        "node_id": node_id,
        "role": role,
        **payload,
    }


def _operation(cluster_id: str = CLUSTER_ID) -> dict:
    """Build one deterministic non-membership remote operation."""

    return {
        "schema_version": 1,
        "cluster_id": cluster_id,
        "op_id": f"{NODE_B}:00000001",
        "actor": NODE_B,
        "seq": 1,
        "op": "STOP_INSTANCE",
        "created_at": 102,
        "instance": "remote-b",
    }


def _canonical_json_bytes(value: dict) -> bytes:
    """Return the canonical JSON bytes used by config blob manifests."""

    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _write_config_blob(root: Path, blob_hash: str, raw: bytes) -> None:
    """Write one content-addressed config blob for materialization tests."""

    digest = blob_hash.removeprefix("sha256:")
    path = root / "config_blobs" / "sha256" / digest[:2] / f"{digest}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)


def _write_secret_blob(root: Path, blob_hash: str, raw: bytes) -> None:
    """Write one content-addressed secret blob for materialization tests."""

    digest = blob_hash.removeprefix("sha256:")
    path = root / "secret_blobs" / "sha256" / digest[:2] / f"{digest}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)


def _write_config_blob_set(root: Path, files: dict[str, bytes]) -> str:
    """Write file blobs plus their manifest and return the manifest hash."""

    manifest_files = {}
    for name, raw in files.items():
        digest = hashlib.sha256(raw).hexdigest()
        manifest_files[name] = {"sha256": digest, "size": len(raw)}
        _write_config_blob(root, f"sha256:{digest}", raw)
    manifest_raw = _canonical_json_bytes({"schema_version": 1, "files": manifest_files})
    manifest_hash = "sha256:" + hashlib.sha256(manifest_raw).hexdigest()
    _write_config_blob(root, manifest_hash, manifest_raw)
    return manifest_hash


def _write_local_run_v7_config(root: Path, instance: str, raw_config: bytes) -> tuple[str, str, bytes]:
    """Write a local run_v7 config without writing config blobs."""

    instance_dir = root.parent / "run_v7" / instance
    instance_dir.mkdir(parents=True)
    (instance_dir / "config.json").write_bytes(raw_config)
    file_hash = hashlib.sha256(raw_config).hexdigest()
    manifest_raw = _canonical_json_bytes({
        "schema_version": 1,
        "files": {"config.json": {"sha256": file_hash, "size": len(raw_config)}},
    })
    manifest_hash = "sha256:" + hashlib.sha256(manifest_raw).hexdigest()
    return manifest_hash, file_hash, manifest_raw


def test_hello_returns_identity_for_registered_peer(tmp_path: Path) -> None:
    """hello returns local identity and protocol information for known peers."""

    root = _init_cluster(tmp_path)

    payload = run_command(root, NODE_B, "hello")

    assert payload["ok"] is True
    assert payload["protocol_version"] == 2
    assert payload["cluster_id"] == CLUSTER_ID
    assert payload["node_id"] == NODE_A
    assert payload["remote_node"] == NODE_B
    assert payload["credential_capability"]["sealed_credentials"] is True
    assert payload["crypto_public_bundle"]["node_id"] == NODE_A
    assert payload["capabilities"] == ["sealed_credentials_v2"]


@pytest.mark.parametrize("command", ["handshake", f"get-ops {NODE_B} 1 1"])
def test_read_transport_commands_do_not_advance_migration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    command: str,
) -> None:
    """Read-only sync transport stays bounded independently of migration work."""

    root = _init_cluster(tmp_path)
    run_command(root, NODE_B, "put-op", json.dumps(_operation()).encode("utf-8"))
    rebuild_materialized_state(root)
    monkeypatch.setattr(
        cluster_sync_command,
        "rebuild_materialized_state",
        lambda *_args, **_kwargs: pytest.fail("read-only transport must use the persisted snapshot"),
    )

    payload = run_command(root, NODE_B, command)

    assert payload["ok"] is True


def test_read_transport_replays_newer_membership_before_authentication(tmp_path: Path) -> None:
    """A node revocation newer than the snapshot takes effect before transport auth."""

    root = _init_cluster(tmp_path)
    rebuild_materialized_state(root)
    revoked = _legacy_membership(NODE_A, 2, NODE_B, "vps", created_at=102, enabled=False)
    revoked["op"] = "UPDATE_NODE"
    write_operation(root, revoked, allow_legacy_membership=True)

    with pytest.raises(ClusterSyncCommandError, match="disabled"):
        run_command(root, NODE_B, "handshake")


def test_unknown_peer_is_rejected_without_join_mode(tmp_path: Path) -> None:
    """Only registered peers can use the restricted command unless join mode is explicit."""

    root = _init_cluster(tmp_path)
    unknown = "pbgui-node-00000000-0000-4000-8000-000000000099"

    with pytest.raises(ClusterSyncCommandError, match="not registered"):
        run_command(root, unknown, "hello")

    with pytest.raises(ClusterSyncCommandError, match="not registered"):
        run_command(root, unknown, "hello", allow_join=True)


def test_disabled_peer_is_denied_every_non_join_command(tmp_path: Path) -> None:
    """A disabled SSH key cannot use read or write verbs even with allow-join set."""

    root = _init_cluster(tmp_path)
    local_bundle = ensure_node_key_material(root).public_bundle(NODE_A, "master")
    write_operation(
        root,
        _legacy_membership(
            NODE_A,
            2,
            NODE_A,
            "master",
            created_at=102,
            **{field: local_bundle[field] for field in (
                "signing_public_key", "signing_key_id", "encryption_public_key", "encryption_key_id"
            )},
        ),
        allow_legacy_membership=True,
    )
    append_operation(root, "DISABLE_NODE", {"node_id": NODE_B}, created_at=103)

    for command in ("hello", "get-state-vector", "put-blob sha256:" + "0" * 64):
        with pytest.raises(ClusterSyncCommandError, match="disabled"):
            run_command(root, NODE_B, command, allow_join=True)


def test_network_rejects_unsigned_historical_membership_input(tmp_path: Path) -> None:
    """Unsigned v1 membership remains disk-readable but cannot enter over sync."""

    root = _init_cluster(tmp_path)
    operation = _legacy_membership(NODE_C, 1, NODE_C, "vps", created_at=102)

    with pytest.raises(ClusterSyncCommandError, match="historical replay only"):
        run_command(root, NODE_B, "put-op", json.dumps(operation).encode())


def test_join_initializes_identity_for_approved_remote(tmp_path: Path) -> None:
    """join writes identity files only when explicit join mode is enabled."""

    root = tmp_path / "cluster"

    payload = run_command(root, NODE_A, f"join {CLUSTER_ID} {NODE_B} vps vps-b", allow_join=True)
    identity = read_local_identity(root)

    assert payload["ok"] is True
    assert payload["cluster_id"] == CLUSTER_ID
    assert payload["node_id"] == NODE_B
    assert payload["joined_by"] == NODE_A
    assert identity["cluster_id"] == CLUSTER_ID
    assert identity["node_id"] == NODE_B


def test_join_bootstrap_installs_authorizer_and_signed_self_membership(tmp_path: Path) -> None:
    """Explicit join metadata establishes both trust roots before normal sync."""

    master_root = tmp_path / "master-cluster"
    ensure_local_identity(
        master_root,
        role="master",
        pbname="master-a",
        cluster_id=CLUSTER_ID,
        node_id=NODE_A,
        created_at=100,
    )
    master_op = append_operation(
        master_root,
        "ADD_NODE",
        {"node_id": NODE_A, "role": "master", "pbname": "master-a"},
        created_at=101,
    )
    authorization = create_join_authorization(master_root, NODE_B, "vps", created_at=102)
    bootstrap = base64.urlsafe_b64encode(json.dumps({
        "authorization": authorization,
        "authorizer_operation": master_op,
    }, sort_keys=True, separators=(",", ":")).encode()).decode()
    joined_root = tmp_path / "joined-cluster"

    payload = run_command(
        joined_root,
        NODE_A,
        f"join {CLUSTER_ID} {NODE_B} vps vps-b {bootstrap}",
        allow_join=True,
    )
    operations = load_operations(joined_root, expected_cluster_id=CLUSTER_ID)

    assert payload["membership_op_id"] == f"{NODE_B}:00000001"
    assert {operation["node_id"] for operation in operations if operation["op"] == "ADD_NODE"} == {NODE_A, NODE_B}
    assert all(operation.get("signature_algorithm") == "Ed25519" for operation in operations)
    assert run_command(joined_root, NODE_A, "hello")["remote_node"] == NODE_A


def test_join_rejects_overwriting_existing_identity(tmp_path: Path) -> None:
    """join is idempotent for the same ids but refuses foreign identities."""

    root = tmp_path / "cluster"
    run_command(root, NODE_A, f"join {CLUSTER_ID} {NODE_B} vps vps-b", allow_join=True)

    with pytest.raises(ClusterSyncCommandError, match="existing cluster_id differs"):
        run_command(root, NODE_A, f"join {FOREIGN_CLUSTER_ID} {NODE_B} vps vps-b", allow_join=True)


def test_join_requires_explicit_join_mode(tmp_path: Path) -> None:
    """join cannot run unless the wrapper is invoked with allow_join."""

    root = tmp_path / "cluster"

    with pytest.raises(ClusterSyncCommandError, match="requires allow_join"):
        run_command(root, NODE_A, f"join {CLUSTER_ID} {NODE_B} vps vps-b")


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


def test_put_ops_writes_valid_operation_batch(tmp_path: Path) -> None:
    """put-ops validates and writes a batch of remote operation files."""

    root = _init_cluster(tmp_path)
    op1 = _operation()
    op2 = dict(op1)
    op2.update({"op_id": f"{NODE_B}:00000002", "seq": 2, "node_id": NODE_A})

    payload = run_command(root, NODE_B, "put-ops", json.dumps({"operations": [op1, op2]}).encode("utf-8"))
    operations = load_operations(root, expected_cluster_id=CLUSTER_ID)

    assert payload["ok"] is True
    assert payload["count"] == 2
    assert [item["op_id"] for item in payload["operations"]] == [f"{NODE_B}:00000001", f"{NODE_B}:00000002"]
    assert any(item["op_id"] == f"{NODE_B}:00000001" for item in operations)
    assert any(item["op_id"] == f"{NODE_B}:00000002" for item in operations)


def test_put_ops_rejects_forged_membership_key_substitution(tmp_path: Path) -> None:
    """A batch cannot replace a member key and validate a forged credential with it."""

    root = _init_cluster(tmp_path)
    trusted_keys = ensure_node_key_material(tmp_path / "trusted-remote-keys")
    trusted_bundle = trusted_keys.public_bundle(NODE_B, "vps")
    write_operation(
        root,
        _legacy_membership(
            NODE_A,
            2,
            NODE_B,
            "vps",
            created_at=102,
            **{field: trusted_bundle[field] for field in (
                "signing_public_key", "signing_key_id", "encryption_public_key", "encryption_key_id"
            )},
        ) | {"op": "UPDATE_NODE"},
        allow_legacy_membership=True,
    )
    keys = ensure_node_key_material(tmp_path / "attacker-keys")
    bundle = keys.public_bundle(NODE_B, "vps")
    membership = _operation()
    membership.update({
        "op": "UPDATE_NODE",
        "node_id": NODE_B,
        **{field: bundle[field] for field in (
            "signing_public_key", "signing_key_id", "encryption_public_key", "encryption_key_id"
        )},
    })
    membership = sign_operation(membership, keys.signing_private_key, signer_id=NODE_B)
    unsigned_secret = {
        "schema_version": 1,
        "cluster_id": CLUSTER_ID,
        "op_id": f"{NODE_B}:00000002",
        "actor": NODE_B,
        "seq": 2,
        "op": "UPSERT_SECRET",
        "created_at": 103,
        "secret_id": "cmc_" + "4" * 32,
        "secret_kind": "cmc_api_key",
        "audience": "cluster",
        "generation": 1,
        "parent_generation": 0,
        "sealed_blob_hash": "sha256:" + "4" * 64,
    }
    signed_secret = sign_operation(unsigned_secret, keys.signing_private_key, signer_id=NODE_B)

    with pytest.raises(ClusterSyncCommandError, match="authenticated key|signature"):
        run_command(
            root,
            NODE_B,
            "put-ops",
            json.dumps({"operations": [signed_secret, membership]}).encode(),
        )
    assert not any(operation["actor"] == NODE_B for operation in load_operations(root))


def test_apply_bundle_rejects_forged_same_batch_membership_key(tmp_path: Path) -> None:
    """A bundle cannot authenticate its envelope with an untrusted key update."""

    root = _init_cluster(tmp_path)
    trusted_keys = ensure_node_key_material(tmp_path / "trusted-remote-keys")
    trusted_bundle = trusted_keys.public_bundle(NODE_B, "vps")
    write_operation(
        root,
        _legacy_membership(
            NODE_A,
            2,
            NODE_B,
            "vps",
            created_at=102,
            **{field: trusted_bundle[field] for field in (
                "signing_public_key", "signing_key_id", "encryption_public_key", "encryption_key_id"
            )},
        ) | {"op": "UPDATE_NODE"},
        allow_legacy_membership=True,
    )
    keys = ensure_node_key_material(tmp_path / "attacker-keys")
    bundle = keys.public_bundle(NODE_B, "vps")
    membership = _operation()
    membership.update({
        "op": "UPDATE_NODE",
        "node_id": NODE_B,
        **{field: bundle[field] for field in (
            "signing_public_key", "signing_key_id", "encryption_public_key", "encryption_key_id"
        )},
    })
    membership = sign_operation(membership, keys.signing_private_key, signer_id=NODE_B)
    secret_id = "cmc_" + "6" * 32
    context = SecretContext(CLUSTER_ID, secret_id, "cmc_api_key", 1, "cluster")
    envelope = seal_secret(
        b"opaque-relay-value",
        context,
        [SecretRecipient(NODE_B, "vps", keys.encryption_public_key)],
        keys.signing_private_key,
        signer_id=NODE_B,
    )
    raw = serialize_sealed_secret(envelope)
    blob_hash = "sha256:" + hashlib.sha256(raw).hexdigest()
    signed_secret = sign_operation(
        {
            "schema_version": 1,
            "cluster_id": CLUSTER_ID,
            "op_id": f"{NODE_B}:00000002",
            "actor": NODE_B,
            "seq": 2,
            "op": "UPSERT_SECRET",
            "created_at": 103,
            "secret_id": secret_id,
            "secret_kind": "cmc_api_key",
            "audience": "cluster",
            "generation": 1,
            "parent_generation": 0,
            "sealed_blob_hash": blob_hash,
        },
        keys.signing_private_key,
        signer_id=NODE_B,
    )
    request = {
        "operations": [signed_secret, membership],
        "config_blobs": [],
        "secret_blobs": [],
        "sealed_blobs": [{"hash": blob_hash, "content_b64": base64.b64encode(raw).decode("ascii")}],
    }

    with pytest.raises(ClusterSyncCommandError, match="authenticated key|signature"):
        run_command(root, NODE_B, "apply-bundle", json.dumps(request).encode())
    assert not (root / "sealed_blobs").exists()


def test_apply_bundle_accepts_direct_legacy_node_key_claim(tmp_path: Path) -> None:
    """The authenticated node may publish its first v2 key through the fast path."""

    root = _init_cluster(tmp_path)
    keys = ensure_node_key_material(tmp_path / "remote-keys")
    claim = sign_operation(
        {
            "schema_version": 1,
            "cluster_id": CLUSTER_ID,
            "op_id": f"{NODE_B}:00000001",
            "actor": NODE_B,
            "seq": 1,
            "op": "UPDATE_NODE_KEY",
            "created_at": 102,
            "node_id": NODE_B,
            **keys.public_bundle(NODE_B, "vps"),
        },
        keys.signing_private_key,
        signer_id=NODE_B,
    )
    request = {
        "operations": [claim],
        "config_blobs": [],
        "secret_blobs": [],
        "sealed_blobs": [],
    }

    payload = run_command(root, NODE_B, "apply-bundle", json.dumps(request).encode())

    assert payload["ok"] is True
    operations = load_operations(root, expected_cluster_id=CLUSTER_ID)
    assert operations[-1]["op"] == "UPDATE_NODE_KEY"
    assert operations[-1]["signer_key_id"] == claim["signing_key_id"]
    assert payload["materialization"] == {"status": "delegated_to_pbcluster"}


def test_apply_bundle_accepts_legacy_key_claim_relayed_by_authenticated_master(tmp_path: Path) -> None:
    """A directly authenticated master may fan out a previously accepted legacy key claim."""
    root = _init_cluster(tmp_path)
    write_operation(
        root,
        _legacy_membership(NODE_A, 2, NODE_C, "master", created_at=102),
        allow_legacy_membership=True,
    )
    keys = ensure_node_key_material(tmp_path / "relayed-keys")
    claim = sign_operation(
        {
            "schema_version": 1,
            "cluster_id": CLUSTER_ID,
            "op_id": f"{NODE_B}:00000001",
            "actor": NODE_B,
            "seq": 1,
            "op": "UPDATE_NODE_KEY",
            "created_at": 103,
            "node_id": NODE_B,
            **keys.public_bundle(NODE_B, "vps"),
        },
        keys.signing_private_key,
        signer_id=NODE_B,
    )
    request = {
        "operations": [claim],
        "config_blobs": [],
        "secret_blobs": [],
        "sealed_blobs": [],
    }

    payload = run_command(root, NODE_C, "apply-bundle", json.dumps(request).encode())

    assert payload["ok"] is True
    assert load_operations(root, expected_cluster_id=CLUSTER_ID)[-1]["op_id"] == claim["op_id"]


def test_get_ops_returns_bounded_operation_range(tmp_path: Path) -> None:
    """get-ops returns existing operations and reports missing seq numbers."""

    root = _init_cluster(tmp_path)
    op1 = _operation()
    op3 = dict(op1)
    op3.update({"op_id": f"{NODE_B}:00000003", "seq": 3, "node_id": NODE_A})
    run_command(root, NODE_B, "put-ops", json.dumps({"operations": [op1, op3]}).encode("utf-8"))

    payload = run_command(root, NODE_B, f"get-ops {NODE_B} 1 3")

    assert payload["ok"] is True
    assert [item["seq"] for item in payload["operations"]] == [1, 3]
    assert payload["missing"] == [2]


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
    assert state["cluster_id"] == CLUSTER_ID
    assert state["node_id"] == NODE_A
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


def test_put_blobs_writes_valid_blob_batch(tmp_path: Path) -> None:
    """put-blobs validates and writes multiple config blobs in one command."""

    root = _init_cluster(tmp_path)
    raw_a = b'{"config":true}'
    raw_b = b'{"override":true}'
    hash_a = "sha256:" + hashlib.sha256(raw_a).hexdigest()
    hash_b = "sha256:" + hashlib.sha256(raw_b).hexdigest()
    request = {
        "blobs": [
            {"hash": hash_a, "content_b64": base64.b64encode(raw_a).decode("ascii")},
            {"hash": hash_b, "content_b64": base64.b64encode(raw_b).decode("ascii")},
        ]
    }

    payload = run_command(root, NODE_B, "put-blobs", json.dumps(request).encode("utf-8"))

    assert payload["ok"] is True
    assert payload["count"] == 2
    for blob_hash, raw in ((hash_a, raw_a), (hash_b, raw_b)):
        expected = blob_hash.removeprefix("sha256:")
        path = root / "config_blobs" / "sha256" / expected[:2] / f"{expected}.json"
        assert path.read_bytes() == raw

    single = run_command(root, NODE_B, f"get-blob {hash_a}")
    batch = run_command(root, NODE_B, f"get-blobs {hash_a} {hash_b}")

    assert base64.b64decode(single["content_b64"]) == raw_a
    assert [item["hash"] for item in batch["blobs"]] == [hash_a, hash_b]
    assert [base64.b64decode(item["content_b64"]) for item in batch["blobs"]] == [raw_a, raw_b]


def test_missing_blobs_returns_only_absent_hashes_without_content(tmp_path: Path) -> None:
    """The coverage probe validates requested hashes and returns no blob contents."""

    root = _init_cluster(tmp_path)
    raw = b'{"config":true}'
    present = "sha256:" + hashlib.sha256(raw).hexdigest()
    absent = "sha256:" + "f" * 64
    run_command(root, NODE_B, f"put-blob {present}", raw)
    request = {"config": sorted([present, absent]), "secret": [], "sealed": []}

    payload = run_command(root, NODE_B, "missing-blobs", json.dumps(request).encode("utf-8"))

    assert payload == {
        "ok": True,
        "requested": 2,
        "missing": {"config": [absent], "secret": [], "sealed": []},
    }
    assert "content_b64" not in json.dumps(payload)
    digest = present.removeprefix("sha256:")
    (root / "config_blobs" / "sha256" / digest[:2] / f"{digest}.json").write_bytes(b"corrupt")

    corrupted = run_command(root, NODE_B, "missing-blobs", json.dumps(request).encode("utf-8"))

    assert corrupted["missing"]["config"] == sorted([present, absent])


def test_missing_blobs_rejects_oversized_probe(tmp_path: Path) -> None:
    """Coverage probes have a strict per-command work bound."""

    root = _init_cluster(tmp_path)
    hashes = [
        f"sha256:{index:064x}"
        for index in range(cluster_sync_command.MAX_BLOB_COVERAGE_HASHES + 1)
    ]
    request = {"config": hashes, "secret": [], "sealed": []}

    with pytest.raises(ClusterSyncCommandError, match="request is too large"):
        run_command(root, NODE_B, "missing-blobs", json.dumps(request).encode("utf-8"))


def test_missing_blobs_enforces_verification_byte_budget(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Hash verification cannot exceed its bounded per-command byte budget."""

    root = _init_cluster(tmp_path)
    hashes = []
    for raw in (b'{"one":1}', b'{"two":2}'):
        blob_hash = "sha256:" + hashlib.sha256(raw).hexdigest()
        run_command(root, NODE_B, f"put-blob {blob_hash}", raw)
        hashes.append(blob_hash)
    monkeypatch.setattr(cluster_sync_command, "MAX_BLOB_COVERAGE_VERIFY_BYTES", 10)
    request = {"config": sorted(hashes), "secret": [], "sealed": []}

    with pytest.raises(ClusterSyncCommandError, match="verification budget exceeded"):
        run_command(root, NODE_B, "missing-blobs", json.dumps(request).encode("utf-8"))


def test_get_blob_rejects_symlinked_store_parent(tmp_path: Path) -> None:
    """Restricted blob reads reject symlinks in store parent components."""

    root = _init_cluster(tmp_path)
    raw = b'{"external":true}'
    digest = hashlib.sha256(raw).hexdigest()
    blob_hash = f"sha256:{digest}"
    external_store = tmp_path / "external_store"
    external_path = external_store / digest[:2] / f"{digest}.json"
    external_path.parent.mkdir(parents=True)
    external_path.write_bytes(raw)
    config_root = root / "config_blobs"
    config_root.mkdir(parents=True, exist_ok=True)
    (config_root / "sha256").symlink_to(external_store)

    with pytest.raises(ClusterSyncCommandError, match="must not contain symlinks"):
        run_command(root, NODE_B, f"get-blob {blob_hash}")


def test_get_blob_repairs_missing_manifest_from_existing_run_v7_config(tmp_path: Path) -> None:
    """get-blob rebuilds a missing manifest blob from matching local run_v7 files."""

    root = _init_cluster(tmp_path)
    raw_config = b'{"live":{"user":"local"}}'
    manifest_hash, file_hash, manifest_raw = _write_local_run_v7_config(root, "local_inst", raw_config)
    append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "local_inst",
            "version": "2",
            "assigned_host": NODE_A,
            "desired_state": "running",
            "config_manifest_hash": manifest_hash,
        },
        created_at=102,
    )

    payload = run_command(root, NODE_B, f"get-blob {manifest_hash}")

    assert payload["hash"] == manifest_hash
    assert base64.b64decode(payload["content_b64"]) == manifest_raw
    assert (root / "config_blobs" / "sha256" / file_hash[:2] / f"{file_hash}.json").read_bytes() == raw_config


def test_get_blob_repairs_missing_file_blob_from_existing_run_v7_config(tmp_path: Path) -> None:
    """get-blob rebuilds missing file blobs referenced by matching local configs."""

    root = _init_cluster(tmp_path)
    raw_config = b'{"live":{"user":"local"}}'
    manifest_hash, file_hash, manifest_raw = _write_local_run_v7_config(root, "local_inst", raw_config)
    append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "local_inst",
            "version": "2",
            "assigned_host": NODE_A,
            "desired_state": "running",
            "config_manifest_hash": manifest_hash,
        },
        created_at=102,
    )

    payload = run_command(root, NODE_B, f"get-blob sha256:{file_hash}")
    manifest_digest = manifest_hash.removeprefix("sha256:")

    assert payload["hash"] == f"sha256:{file_hash}"
    assert base64.b64decode(payload["content_b64"]) == raw_config
    assert (
        root / "config_blobs" / "sha256" / manifest_digest[:2] / f"{manifest_digest}.json"
    ).read_bytes() == manifest_raw


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
    assert stat.S_IMODE(path.parent.stat().st_mode) == 0o700
    assert stat.S_IMODE((root / "secret_blobs" / "sha256").stat().st_mode) == 0o700
    assert stat.S_IMODE((root / "secret_blobs").stat().st_mode) == 0o700

    fetched = run_command(root, NODE_B, f"get-secret-blob {blob_hash}")
    assert fetched["hash"] == blob_hash
    assert base64.b64decode(fetched["content_b64"]) == raw


def _credential_cluster(
    tmp_path: Path,
    *,
    local_node: str,
    local_role: str,
    members: list[tuple[str, str, Path]],
) -> Path:
    """Create one test replica with the same crypto-capable membership."""

    root = tmp_path / local_node[-1] / "cluster"
    ensure_local_identity(
        root,
        role=local_role,
        pbname=local_node[-1],
        cluster_id=CLUSTER_ID,
        node_id=local_node,
        created_at=100,
    )
    advertised_root = next(key_root for node_id, _role, key_root in members if node_id == local_node)
    advertised_keys = ensure_node_key_material(advertised_root)
    local_keys = ensure_node_key_material(root)
    for path in local_keys.crypto_root.iterdir():
        path.unlink()
    for path in advertised_keys.crypto_root.iterdir():
        if path.name != ".keys.lock":
            (local_keys.crypto_root / path.name).write_bytes(path.read_bytes())
    ensure_node_key_material(root)
    for index, (node_id, role, key_root) in enumerate(members, start=1):
        bundle = ensure_node_key_material(key_root).public_bundle(node_id, role)
        write_operation(
            root,
            _legacy_membership(
                node_id,
                1,
                node_id,
                role,
                created_at=100 + index,
                 **{field: bundle[field] for field in (
                     "signing_public_key", "signing_key_id", "encryption_public_key", "encryption_key_id"
                 )},
                 credential_protocol_version=2,
                 credential_capable=True,
                 pbname=node_id[-1],
            ),
            allow_legacy_membership=True,
        )
    return root


def test_cutoff_rejects_obsolete_secret_blob_get_put_and_operation_relay(tmp_path: Path) -> None:
    """A signed cutoff prevents every restricted-command plaintext relay path."""

    key_a_root = tmp_path / "keys-a"
    key_b_root = tmp_path / "keys-b"
    root = _credential_cluster(
        tmp_path,
        local_node=NODE_A,
        local_role="master",
        members=[(NODE_A, "master", key_a_root), (NODE_B, "vps", key_b_root)],
    )
    raw = b'{"tradfi":{"api_key":"obsolete"}}'
    blob_hash = "sha256:" + hashlib.sha256(raw).hexdigest()
    _write_secret_blob(root, blob_hash, raw)
    operation = append_operation(
        root,
        "UPSERT_API_KEYS",
        {"api_serial": 1, "payload_hash": HASH_A, "secret_blob_hash": blob_hash},
    )
    append_operation(
        root,
        "CREDENTIAL_CUTOFF",
        {
            "cutoff_generation": 1,
            "parent_generation": 0,
            "state_vector": {NODE_A: int(operation["seq"])},
            "min_protocol": 2,
            "obsolete_secret_blob_hashes": [blob_hash],
        },
    )
    rebuild_materialized_state(root)

    with pytest.raises(ClusterSyncCommandError, match="pre-cutoff"):
        run_command(root, NODE_B, f"get-secret-blob {blob_hash}")
    with pytest.raises(ClusterSyncCommandError, match="pre-cutoff"):
        run_command(root, NODE_B, f"put-secret-blob {blob_hash}", raw)
    relayed = run_command(
        root,
        NODE_B,
        f"get-ops {NODE_A} {operation['seq']} {operation['seq']}",
    )
    assert [item["op_id"] for item in relayed["operations"]] == [operation["op_id"]]
    assert relayed["missing"] == []


@pytest.mark.parametrize(("role", "node_id"), [("master", NODE_A), ("vps", NODE_B)])
def test_materialize_cluster_audience_cmc_on_master_and_vps(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    role: str,
    node_id: str,
) -> None:
    """CMC sealed credentials decrypt and materialize on both cluster roles."""

    key_a_root = tmp_path / "keys-a"
    key_b_root = tmp_path / "keys-b"
    members = [(NODE_A, "master", key_a_root), (NODE_B, "vps", key_b_root)]
    root = _credential_cluster(
        tmp_path,
        local_node=node_id,
        local_role=role,
        members=members,
    )
    local_keys = ensure_node_key_material(root)
    source_keys = ensure_node_key_material(key_a_root)
    secret_id = "cmc_" + "1" * 32
    context = SecretContext(CLUSTER_ID, secret_id, "cmc_api_key", 1, "cluster")
    recipients = [
        SecretRecipient(member_id, member_role, ensure_node_key_material(key_root).encryption_public_key)
        for member_id, member_role, key_root in members
    ]
    recipient_key_ids = {
        member_id: ensure_node_key_material(key_root).public_bundle(member_id, member_role)[
            "encryption_key_id"
        ]
        for member_id, member_role, key_root in members
    }
    envelope = seal_secret(
        b"cmc-test-value",
        context,
        recipients,
        source_keys.signing_private_key,
        signer_id=NODE_A,
    )
    raw = serialize_sealed_secret(envelope)
    blob_hash = "sha256:" + hashlib.sha256(raw).hexdigest()
    secret_payload = {
            "secret_id": secret_id,
            "secret_kind": "cmc_api_key",
            "audience": "cluster",
            "generation": 1,
            "parent_generation": 0,
            "recipient_generation": 1,
            "parent_recipient_generation": 0,
            "membership_generation": 2,
            "recipient_ids": sorted([NODE_A, NODE_B]),
            "recipient_key_ids": recipient_key_ids,
            "sealed_blob_hash": blob_hash,
    }
    if node_id == NODE_A:
        append_operation(root, "UPSERT_SECRET", secret_payload, created_at=110)
    else:
        operation = {
            **secret_payload,
            "schema_version": 1,
            "cluster_id": CLUSTER_ID,
            "op_id": f"{NODE_A}:00000002",
            "actor": NODE_A,
            "seq": 2,
            "op": "UPSERT_SECRET",
            "created_at": 110,
            "actor_role_epoch": 1,
            "actor_membership_op_id": f"{NODE_A}:00000001",
        }
        write_operation(
            root,
            sign_operation(operation, source_keys.signing_private_key, signer_id=NODE_A),
            network_input=True,
        )
    # Re-sign with the advertised key when this replica is the VPS actor-independent recipient.
    operation = load_operations(root)[-1]
    assert local_keys.encryption_public_key is not None
    run_command(root, NODE_A if node_id == NODE_B else NODE_B, f"put-sealed-blob {blob_hash}", raw)

    result = run_command(root, NODE_A if node_id == NODE_B else NODE_B, "materialize-credentials")

    assert result["ok"] is True, result
    assert result["items"][0]["status"] == "written"
    store = CredentialStore(root.parent / "credentials")
    assert store.get_cmc(secret_id)["generation"] == 1
    assert store.load_cmc_key(secret_id) == "cmc-test-value"
    assert "cmc-test-value" not in json.dumps(result)
    lifecycle = credential_lifecycle_status(rebuild_materialized_state(root, write=False))
    assert lifecycle["nodes"][node_id]["credential_active"] is True
    assert lifecycle["nodes"][node_id]["materialization_ack"]["recipient_generations"] == {
        secret_id: 1
    }
    monkeypatch.setattr(
        cluster_sync_command,
        "_validate_sealed_blob_payload",
        lambda *_args, **_kwargs: pytest.fail("current credential preview must not reopen its envelope"),
    )

    preview = run_command(
        root,
        NODE_A if node_id == NODE_B else NODE_B,
        "materialize-credentials-preview",
    )

    assert preview["counts"]["current"] == 1
    assert preview["can_apply"] is False


def test_tradfi_materializes_on_master_and_vps_reports_not_recipient(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Masters open TradFi envelopes while VPS replicas only retain ciphertext."""

    key_a_root = tmp_path / "keys-a"
    key_b_root = tmp_path / "keys-b"
    members = [(NODE_A, "master", key_a_root), (NODE_B, "vps", key_b_root)]
    master_root = _credential_cluster(tmp_path / "master", local_node=NODE_A, local_role="master", members=members)
    vps_root = _credential_cluster(tmp_path / "vps", local_node=NODE_B, local_role="vps", members=members)
    pb7 = tmp_path / "pb7"
    pb7.mkdir()
    monkeypatch.setattr("cluster_sync_command.pb7dir", lambda: str(pb7))
    secret_id = "tradfi_" + "2" * 32
    context = SecretContext(CLUSTER_ID, secret_id, "tradfi_profile", 1, "masters")
    source_keys = ensure_node_key_material(key_a_root)
    envelope = seal_secret(
        json.dumps({"provider": "alpaca", "credentials": {"key": "id", "secret": "value"}}).encode(),
        context,
        [SecretRecipient(NODE_A, "master", source_keys.encryption_public_key)],
        source_keys.signing_private_key,
        signer_id=NODE_A,
    )
    raw = serialize_sealed_secret(envelope)
    blob_hash = "sha256:" + hashlib.sha256(raw).hexdigest()
    source_operation = append_operation(
        master_root,
        "UPSERT_SECRET",
        {
            "secret_id": secret_id,
            "secret_kind": "tradfi_profile",
            "audience": "masters",
            "generation": 1,
            "parent_generation": 0,
            "sealed_blob_hash": blob_hash,
            "provider": "alpaca",
            "actor_role_epoch": 1,
            "actor_membership_op_id": f"{NODE_A}:00000001",
        },
        created_at=110,
    )
    run_command(master_root, NODE_B, f"put-sealed-blob {blob_hash}", raw)
    run_command(vps_root, NODE_A, f"put-sealed-blob {blob_hash}", raw)
    run_command(vps_root, NODE_A, "put-op", json.dumps(source_operation).encode())

    master_result = run_command(master_root, NODE_B, "materialize-credentials")
    vps_result = run_command(vps_root, NODE_A, "materialize-credentials")

    assert master_result["items"][0]["status"] == "written"
    assert CredentialStore(master_root.parent / "credentials").load_tradfi_credentials(secret_id) == {
        "key": "id",
        "secret": "value",
    }
    assert vps_result["items"][0]["status"] == "not_recipient"
    assert CredentialStore(vps_root.parent / "credentials").list_tradfi() == []
    projected = json.loads((pb7 / "api-keys.json").read_text(encoding="utf-8"))
    assert projected["tradfi"]["provider"] == "alpaca"
    assert projected["tradfi"]["key"] == "id"
    assert projected["tradfi"]["secret"] == "value"
    assert master_result["tradfi_projection"]["status"] == "current"
    assert vps_result["tradfi_projection"]["status"] == "not_recipient"
    assert "value" not in json.dumps(master_result)

    master_store = CredentialStore(master_root.parent / "credentials")
    master_store.update_tradfi(secret_id, active=False, origin="legacy_shadow")
    drifted_preview = run_command(master_root, NODE_B, "materialize-credentials-preview")
    assert drifted_preview["can_apply"] is True
    assert drifted_preview["counts"]["ready"] == 1

    repaired = run_command(master_root, NODE_B, "materialize-credentials")
    promoted = master_store.get_tradfi(secret_id)
    assert repaired["items"][0]["status"] == "written"
    assert promoted["origin"] == "cluster"
    assert promoted["active"] is True

    materialized = rebuild_materialized_state(master_root, write=False)
    append_operation(
        master_root,
        "CREDENTIAL_MATERIALIZATION_ACK",
        {
            "node_id": NODE_A,
            "credential_generations": {secret_id: 1},
            "recipient_generations": {secret_id: 1},
            "membership_generation": materialized["cluster_nodes"]["credential_membership_generation"],
        },
    )
    stable_preview = run_command(master_root, NODE_B, "materialize-credentials-preview")
    assert stable_preview["can_apply"] is False, stable_preview
    assert stable_preview["counts"]["current"] == 1
    monkeypatch.setattr(
        cluster_sync_command,
        "_validate_sealed_blob_payload",
        lambda *_args, **_kwargs: pytest.fail("master-only preview must not reopen its envelope on a VPS"),
    )

    vps_preview = run_command(vps_root, NODE_A, "materialize-credentials-preview")

    assert vps_preview["counts"]["not_recipient"] == 1
    assert vps_preview["can_apply"] is False


def test_three_node_vps_relay_forwards_opaque_tradfi_without_decrypting(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """A VPS relays a masters envelope and signed operation without opening it."""

    key_a_root = tmp_path / "keys-a"
    key_b_root = tmp_path / "keys-b"
    key_c_root = tmp_path / "keys-c"
    members = [
        (NODE_A, "master", key_a_root),
        (NODE_B, "vps", key_b_root),
        (NODE_C, "master", key_c_root),
    ]
    relay_root = _credential_cluster(tmp_path / "relay", local_node=NODE_B, local_role="vps", members=members)
    destination_root = _credential_cluster(tmp_path / "destination", local_node=NODE_C, local_role="master", members=members)
    secret_id = "tradfi_" + "3" * 32
    source_keys = ensure_node_key_material(key_a_root)
    context = SecretContext(CLUSTER_ID, secret_id, "tradfi_profile", 1, "masters")
    envelope = seal_secret(
        b'{"provider":"alpaca","credentials":{"secret":"relay-only"}}',
        context,
        [
            SecretRecipient(NODE_A, "master", source_keys.encryption_public_key),
            SecretRecipient(NODE_C, "master", ensure_node_key_material(key_c_root).encryption_public_key),
        ],
        source_keys.signing_private_key,
        signer_id=NODE_A,
    )
    raw = serialize_sealed_secret(envelope)
    blob_hash = "sha256:" + hashlib.sha256(raw).hexdigest()
    operation = sign_operation(
        {
            "schema_version": 1,
            "cluster_id": CLUSTER_ID,
            "op_id": f"{NODE_A}:00000002",
            "actor": NODE_A,
            "seq": 2,
            "op": "UPSERT_SECRET",
            "created_at": 110,
            "secret_id": secret_id,
            "secret_kind": "tradfi_profile",
            "audience": "masters",
            "generation": 1,
            "parent_generation": 0,
            "sealed_blob_hash": blob_hash,
            "provider": "alpaca",
            "actor_role_epoch": 1,
            "actor_membership_op_id": f"{NODE_A}:00000001",
        },
        source_keys.signing_private_key,
        signer_id=NODE_A,
    )
    monkeypatch.setattr(
        "cluster_sync_command.open_sealed_secret",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("relay must not decrypt")),
    )

    run_command(relay_root, NODE_A, f"put-sealed-blob {blob_hash}", raw)
    run_command(relay_root, NODE_A, "put-op", json.dumps(operation).encode())
    forwarded = run_command(relay_root, NODE_C, f"get-sealed-blob {blob_hash}")
    run_command(destination_root, NODE_B, f"put-sealed-blob {blob_hash}", base64.b64decode(forwarded["content_b64"]))
    run_command(destination_root, NODE_B, "put-op", json.dumps(operation).encode())

    digest = blob_hash.removeprefix("sha256:")
    relay_path = relay_root / "sealed_blobs" / "sha256" / digest[:2] / f"{digest}.json"
    destination_path = destination_root / "sealed_blobs" / "sha256" / digest[:2] / f"{digest}.json"
    assert relay_path.read_bytes() == destination_path.read_bytes() == raw
    assert stat.S_IMODE(relay_path.stat().st_mode) == 0o600
    assert stat.S_IMODE(relay_path.parent.stat().st_mode) == 0o700
    assert load_operations(destination_root)[-1]["sealed_blob_hash"] == blob_hash

    replacement = seal_secret(
        b'{"provider":"alpaca","credentials":{"secret":"relay-only"}}',
        context,
        [
            SecretRecipient(NODE_A, "master", source_keys.encryption_public_key),
            SecretRecipient(
                NODE_C,
                "master",
                ensure_node_key_material(key_c_root).encryption_public_key,
            ),
        ],
        source_keys.signing_private_key,
        signer_id=NODE_A,
    )
    replacement_raw = serialize_sealed_secret(replacement)
    replacement_hash = "sha256:" + hashlib.sha256(replacement_raw).hexdigest()
    replacement_operation = sign_operation(
        {
            "schema_version": 1,
            "cluster_id": CLUSTER_ID,
            "op_id": f"{NODE_A}:00000003",
            "actor": NODE_A,
            "seq": 3,
            "op": "UPDATE_SECRET_RECIPIENTS",
            "created_at": 111,
            "secret_id": secret_id,
            "provider_generation": 1,
            "recipient_generation": 2,
            "parent_recipient_generation": 1,
            "membership_generation": 3,
            "recipient_ids": [NODE_A, NODE_C],
            "recipient_key_ids": {
                NODE_A: source_keys.public_bundle(NODE_A, "master")["encryption_key_id"],
                NODE_C: ensure_node_key_material(key_c_root).public_bundle(NODE_C, "master")["encryption_key_id"],
            },
            "sealed_blob_hash": replacement_hash,
            "actor_role_epoch": 1,
            "actor_membership_op_id": f"{NODE_A}:00000001",
        },
        source_keys.signing_private_key,
        signer_id=NODE_A,
    )
    run_command(relay_root, NODE_A, f"put-sealed-blob {replacement_hash}", replacement_raw)
    run_command(relay_root, NODE_A, "put-op", json.dumps(replacement_operation).encode())
    forwarded = run_command(relay_root, NODE_C, f"get-sealed-blob {replacement_hash}")
    run_command(
        destination_root,
        NODE_B,
        f"put-sealed-blob {replacement_hash}",
        base64.b64decode(forwarded["content_b64"]),
    )
    run_command(destination_root, NODE_B, "put-op", json.dumps(replacement_operation).encode())
    current = rebuild_materialized_state(destination_root, write=False)["desired_state"]["secrets"][secret_id]
    assert current["generation"] == 1
    assert current["recipient_generation"] == 2
    assert current["sealed_blob_hash"] == replacement_hash
    assert destination_path.read_bytes() == raw


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


def test_materialize_v7_preview_and_apply_master_writes_all_config_blobs(tmp_path: Path) -> None:
    """materialize-v7 writes all clean V7 JSON configs on master nodes."""

    root = _init_cluster(tmp_path)
    manifest_hash = _write_config_blob_set(root, {"config.json": b'{"live":{"user":"local"}}'})
    other_hash = _write_config_blob_set(root, {"config.json": b'{"live":{"user":"other"}}'})
    append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "local_inst",
            "version": "2",
            "assigned_host": NODE_A,
            "desired_state": "running",
            "config_manifest_hash": manifest_hash,
        },
        created_at=102,
    )
    append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "other_inst",
            "version": "2",
            "assigned_host": NODE_B,
            "desired_state": "running",
            "config_manifest_hash": other_hash,
        },
        created_at=103,
    )
    append_operation(root, "DELETE_INSTANCE", {"instance": "deleted_inst", "version": "1"}, created_at=104)
    append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "conflict_inst",
            "parent_version": "1",
            "version": "2",
            "assigned_host": NODE_A,
            "desired_state": "running",
            "config_manifest_hash": manifest_hash,
        },
        created_at=105,
    )
    append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "conflict_inst",
            "parent_version": "1",
            "version": "3",
            "assigned_host": NODE_A,
            "desired_state": "running",
            "config_manifest_hash": other_hash,
        },
        actor=NODE_B,
        created_at=106,
    )

    preview = run_command(root, NODE_B, "materialize-v7-preview")
    result = run_command(root, NODE_B, "materialize-v7")

    assert preview["read_only"] is True
    assert preview["materialize_all"] is True
    assert preview["counts"]["add"] == 2
    assert preview["counts"]["not_assigned"] == 0
    assert preview["counts"]["conflicted"] == 1
    assert preview["counts"]["tombstoned"] == 1
    assert result["counts"]["written_instances"] == 2
    assert result["counts"]["written_files"] == 2
    assert (root.parent / "run_v7" / "local_inst" / "config.json").read_bytes() == b'{"live":{"user":"local"}}'
    assert (root.parent / "run_v7" / "other_inst" / "config.json").read_bytes() == b'{"live":{"user":"other"}}'
    assert not (root.parent / "run_v7" / "conflict_inst" / "config.json").exists()
    assert not (root.parent / "run_v7" / "deleted_inst" / "config.json").exists()
    after = run_command(root, NODE_B, "materialize-v7-preview")
    assert after["counts"]["add"] == 0
    assert after["counts"]["skip"] >= 1


def test_materialize_v7_deletes_local_tombstoned_config_with_backup(tmp_path: Path) -> None:
    """materialize-v7 removes stale local run_v7 dirs covered by tombstones."""

    root = _init_cluster(tmp_path)
    stale_dir = root.parent / "run_v7" / "deleted_inst"
    stale_dir.mkdir(parents=True)
    (stale_dir / "config.json").write_text('{"pbgui":{"version":1}}', encoding="utf-8")
    append_operation(root, "DELETE_INSTANCE", {"instance": "deleted_inst", "version": "1"}, created_at=104)

    preview = run_command(root, NODE_B, "materialize-v7-preview")
    result = run_command(root, NODE_B, "materialize-v7")

    assert preview["can_apply"] is True
    assert preview["counts"]["delete"] == 1
    assert preview["counts"]["dirs_to_delete"] == 1
    assert result["counts"]["deleted_instances"] == 1
    assert not stale_dir.exists()
    backup = Path(result["deleted"][0]["backup"])
    assert backup.name.startswith("cluster_tombstone_")
    assert (backup / "config.json").read_text(encoding="utf-8") == '{"pbgui":{"version":1}}'


def test_materialize_v7_vps_writes_only_assigned_config_blobs(tmp_path: Path) -> None:
    """VPS nodes materialize only their assigned V7 configs."""

    root = tmp_path / "cluster"
    ensure_local_identity(
        root,
        role="vps",
        pbname="vps-b",
        cluster_id=CLUSTER_ID,
        node_id=NODE_B,
        created_at=100,
    )
    write_operation(root, _legacy_membership(NODE_A, 1, NODE_A, "master", created_at=101, pbname="master-a"), allow_legacy_membership=True)
    manifest_hash = _write_config_blob_set(root, {"config.json": b'{"live":{"user":"local"}}'})
    other_hash = _write_config_blob_set(root, {"config.json": b'{"live":{"user":"other"}}'})
    append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "local_inst",
            "version": "2",
            "assigned_host": NODE_B,
            "desired_state": "running",
            "config_manifest_hash": manifest_hash,
        },
        created_at=102,
    )
    append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "other_inst",
            "version": "2",
            "assigned_host": NODE_A,
            "desired_state": "running",
            "config_manifest_hash": other_hash,
        },
        created_at=103,
    )

    preview = run_command(root, NODE_A, "materialize-v7-preview")
    result = run_command(root, NODE_A, "materialize-v7")

    assert preview["materialize_all"] is False
    assert preview["counts"]["add"] == 1
    assert preview["counts"]["not_assigned"] == 1
    assert result["counts"]["written_instances"] == 1
    assert (root.parent / "run_v7" / "local_inst" / "config.json").read_bytes() == b'{"live":{"user":"local"}}'
    assert not (root.parent / "run_v7" / "other_inst" / "config.json").exists()


def test_materialize_v7_blocks_when_required_blob_is_missing(tmp_path: Path) -> None:
    """materialize-v7 refuses to write when an assigned config blob is missing."""

    root = _init_cluster(tmp_path)
    append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "local_inst",
            "version": "2",
            "assigned_host": NODE_A,
            "desired_state": "running",
            "config_manifest_hash": HASH_A,
        },
        created_at=102,
    )

    preview = run_command(root, NODE_B, "materialize-v7-preview")

    assert preview["counts"]["error"] == 1
    assert preview["can_apply"] is False
    with pytest.raises(ClusterSyncCommandError, match="materialization blocked"):
        run_command(root, NODE_B, "materialize-v7")
    assert not (root.parent / "run_v7" / "local_inst" / "config.json").exists()


def test_materialize_v7_repairs_missing_local_blobs_from_existing_config(tmp_path: Path) -> None:
    """materialize-v7 rebuilds missing blobs from matching local run_v7 files."""

    root = _init_cluster(tmp_path)
    instance_dir = root.parent / "run_v7" / "local_inst"
    instance_dir.mkdir(parents=True)
    raw_config = b'{"live":{"user":"local"}}'
    (instance_dir / "config.json").write_bytes(raw_config)
    file_hash = hashlib.sha256(raw_config).hexdigest()
    manifest_raw = _canonical_json_bytes({"schema_version": 1, "files": {"config.json": {"sha256": file_hash, "size": len(raw_config)}}})
    manifest_hash = "sha256:" + hashlib.sha256(manifest_raw).hexdigest()
    append_operation(
        root,
        "UPSERT_CONFIG",
        {
            "instance": "local_inst",
            "version": "2",
            "assigned_host": NODE_A,
            "desired_state": "running",
            "config_manifest_hash": manifest_hash,
        },
        created_at=102,
    )

    preview = run_command(root, NODE_B, "materialize-v7-preview")
    result = run_command(root, NODE_B, "materialize-v7")
    after = run_command(root, NODE_B, "materialize-v7-preview")

    assert preview["counts"]["error"] == 1
    assert result["counts"]["error"] == 0
    assert result["counts"]["written_instances"] == 0
    assert result["repaired_config_blobs"] == 1
    assert after["counts"]["error"] == 0
    assert after["counts"]["skip"] >= 1


@pytest.mark.parametrize(("role", "expects_backup"), [("master", True), ("vps", False)])
def test_materialize_api_keys_preview_and_apply_writes_secret_blob(
    monkeypatch,
    tmp_path: Path,
    role: str,
    expects_backup: bool,
) -> None:
    """Exchange materialization preserves TradFi and backs up only on masters."""

    root = _init_cluster(tmp_path)
    if role != "master":
        ensure_local_identity(
            root,
            role=role,
            pbname="vps-a",
            cluster_id=CLUSTER_ID,
            node_id=NODE_A,
            created_at=100,
        )
    pb7 = tmp_path / "pb7"
    pb7.mkdir()
    target = pb7 / "api-keys.json"
    target.write_text(
        '{"_api_serial":1,"tradfi":{"provider":"tiingo","api_key":"vault-token"}}',
        encoding="utf-8",
    )
    monkeypatch.setattr("cluster_sync_command.PBGDIR", str(tmp_path))
    monkeypatch.setattr("cluster_sync_command.pb7dir", lambda: str(pb7))
    raw_secret = b'{"_api_serial":7,"user":{"exchange":"binance","secret":"s"}}'
    secret_hash = "sha256:" + hashlib.sha256(raw_secret).hexdigest()
    payload_hash = "sha256:" + hashlib.sha256(b'{"redacted":true}').hexdigest()
    _write_secret_blob(root, secret_hash, raw_secret)
    append_operation(
        root,
        "UPSERT_API_KEYS",
        {"api_serial": 7, "payload_hash": payload_hash, "secret_blob_hash": secret_hash},
        created_at=102,
    )

    preview = run_command(root, NODE_B, "materialize-api-keys-preview")
    result = run_command(root, NODE_B, "materialize-api-keys")

    assert preview["can_apply"] is True
    assert preview["counts"]["write"] == 1
    assert result["status"] == "written"
    written = json.loads(target.read_text(encoding="utf-8"))
    assert written["_api_serial"] == 7
    assert written["user"] == {"exchange": "binance", "secret": "s"}
    assert written["tradfi"] == {"provider": "tiingo", "api_key": "vault-token"}
    assert stat.S_IMODE(target.stat().st_mode) == 0o600
    if expects_backup:
        backup_path = Path(result["backup"])
        assert backup_path.parent == tmp_path / "data" / "api-keys"
        assert backup_path.name.startswith("api-keys7_cluster-materialize_")
        assert json.loads(backup_path.read_text(encoding="utf-8"))["tradfi"]["api_key"] == "vault-token"
    else:
        assert result["backup_skipped"] == "vps_runner"
        assert "backup" not in result
        assert not (tmp_path / "data" / "api-keys").exists()
    after = run_command(root, NODE_B, "materialize-api-keys-preview")
    assert after["counts"]["current"] == 1


def test_materialize_api_keys_blocks_when_secret_blob_is_missing(tmp_path: Path) -> None:
    """materialize-api-keys refuses to write when the desired secret blob is missing."""

    root = _init_cluster(tmp_path)
    append_operation(
        root,
        "UPSERT_API_KEYS",
        {"api_serial": 7, "payload_hash": HASH_A, "secret_blob_hash": "sha256:" + "d" * 64},
        created_at=102,
    )

    preview = run_command(root, NODE_B, "materialize-api-keys-preview")

    assert preview["can_apply"] is False
    assert preview["counts"]["error"] == 1
    with pytest.raises(ClusterSyncCommandError, match="missing api-keys secret blob"):
        run_command(root, NODE_B, "materialize-api-keys")


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
