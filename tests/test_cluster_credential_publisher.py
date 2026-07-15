"""Tests for generic Cluster Sync v2 credential publication."""

from __future__ import annotations

import json
import os
from pathlib import Path
import stat

import pytest

from cluster_credential_publisher import (
    ClusterCredentialPublisher,
    CredentialPublicationError,
)
from cluster_credentials import (
    deserialize_sealed_secret,
    ensure_node_key_material,
    load_node_encryption_private_keys,
    open_sealed_secret,
    SecretContext,
    sign_operation,
    validate_sealed_secret,
)
from credential_store import CredentialStore
from master.cluster_state import (
    ClusterPaths,
    append_operation,
    credential_lifecycle_status,
    create_join_authorization,
    ensure_local_identity,
    load_operations,
    membership_signing_public_key,
    read_local_identity,
    rebuild_materialized_state,
    write_operation,
)


CLUSTER_ID = "pbgui-cluster-00000000-0000-4000-8000-000000000071"
LOCAL_NODE = "pbgui-node-00000000-0000-4000-8000-000000000071"
MASTER_NODE = "pbgui-node-00000000-0000-4000-8000-000000000072"
VPS_NODE = "pbgui-node-00000000-0000-4000-8000-000000000073"


def _cluster(tmp_path: Path) -> Path:
    """Create a local master with non-crypto metadata to preserve."""

    root = tmp_path / "cluster"
    ensure_local_identity(
        root,
        role="master",
        pbname="publisher-master",
        cluster_id=CLUSTER_ID,
        node_id=LOCAL_NODE,
        created_at=100,
    )
    append_operation(
        root,
        "ADD_NODE",
        {
            "node_id": LOCAL_NODE,
            "role": "master",
            "pbname": "publisher-master",
            "ssh_host": "10.0.0.71",
            "sync_mode": "reachable",
            "sync_enabled": True,
        },
        created_at=101,
    )
    return root


def _add_crypto_member(tmp_path: Path, root: Path, node_id: str, role: str):
    """Add one active remote member with a complete public crypto bundle."""

    keys = ensure_node_key_material(tmp_path / f"keys-{node_id[-2:]}")
    _write_remote_member(
        root,
        node_id,
        role,
        keys,
        {
            "credential_capable": True,
            "credential_protocol_version": 2,
        },
    )
    return keys


def _write_remote_member(
    root: Path,
    node_id: str,
    role: str,
    keys,
    extra: dict,
    *,
    include_encryption: bool = True,
) -> None:
    """Write one separately authorized remote self-add under current membership rules."""

    identity = read_local_identity(root)
    bundle = keys.public_bundle(node_id, role)
    if not include_encryption:
        bundle.pop("encryption_public_key", None)
        bundle.pop("encryption_key_id", None)
    authorization = create_join_authorization(root, node_id, role, created_at=150)
    operation = {
        **bundle,
        **extra,
        "schema_version": 1,
        "cluster_id": str(identity["cluster_id"]),
        "op_id": f"{node_id}:00000001",
        "actor": node_id,
        "seq": 1,
        "op": "ADD_NODE",
        "created_at": 200,
        "node_id": node_id,
        "role": role,
        "membership_authorization": authorization,
    }
    write_operation(
        root,
        sign_operation(operation, keys.signing_private_key, signer_id=node_id),
        network_input=True,
    )


def _blob_raw(root: Path, blob_hash: str) -> bytes:
    """Read one publisher blob through its content-addressed path."""

    digest = blob_hash.removeprefix("sha256:")
    return (
        ClusterPaths.from_root(root).sealed_blobs
        / "sha256"
        / digest[:2]
        / f"{digest}.json"
    ).read_bytes()


def test_cmc_first_publication_assigns_and_transfers_signed_authority(tmp_path: Path) -> None:
    """First publication creates epoch one and transfer advances the CAS immediately."""

    root = _cluster(tmp_path)
    _add_crypto_member(tmp_path, root, MASTER_NODE, "master")
    store = CredentialStore(tmp_path / "credentials")
    record = store.create_cmc("authority-secret")
    publisher = ClusterCredentialPublisher(root, store)

    publication = publisher.publish_cmc(
        record["id"],
        pool_metadata={"quota_domain_id": "free-plan"},
    )
    desired = rebuild_materialized_state(root, write=False)["desired_state"]
    first = desired["cmc_pool"]["authorities"]["free-plan"]

    assert publication["cas"]["authority_epoch"] == 1
    assert first["authority_node_id"] == LOCAL_NODE
    assert first["authority_epoch"] == 1
    transfer = publisher.set_cmc_authority("free-plan", MASTER_NODE, expected_epoch=1)
    current = rebuild_materialized_state(root, write=False)["desired_state"]["cmc_pool"]["authorities"]["free-plan"]
    assert transfer["authority_epoch"] == 2
    assert current["authority_node_id"] == MASTER_NODE
    assert current["authority_epoch"] == 2


def test_one_node_cmc_publication_preserves_metadata_and_is_owner_only(tmp_path: Path) -> None:
    """One-node publication keeps node metadata and leaks no key in its result or blob."""

    root = _cluster(tmp_path)
    store = CredentialStore(tmp_path / "credentials")
    record = store.create_cmc(
        "one-node-secret-value",
        label="imported primary",
        origin="imported",
        shared=True,
    )

    result = ClusterCredentialPublisher(root, store).publish_cmc(record["id"])

    assert result["status"] == "published"
    assert result["recipient_ids"] == [LOCAL_NODE]
    assert len(result["operation_ids"]) == 3
    public_result = json.dumps(result)
    assert "one-node-secret-value" not in public_result
    assert "ciphertext" not in public_result
    materialized = rebuild_materialized_state(root)
    local = materialized["cluster_nodes"]["nodes"][LOCAL_NODE]
    assert local["pbname"] == "publisher-master"
    assert local["ssh_host"] == "10.0.0.71"
    assert local["sync_mode"] == "reachable"
    assert local["credential_capable"] is True
    desired = materialized["desired_state"]
    assert desired["secrets"][record["id"]]["shared"] is True
    assert desired["cmc_pool"]["entries"][record["id"]]["state"] == "active"

    raw = _blob_raw(root, result["sealed_blob_hash"])
    assert b"one-node-secret-value" not in raw
    if os.name == "posix":
        sealed_root = ClusterPaths.from_root(root).sealed_blobs
        for path in [sealed_root, *sealed_root.rglob("*")]:
            expected = 0o700 if path.is_dir() else 0o600
            assert stat.S_IMODE(path.stat().st_mode) == expected, path


def test_cmc_publication_includes_active_master_and_vps(tmp_path: Path) -> None:
    """Cluster-audience CMC envelopes include every active master and VPS."""

    root = _cluster(tmp_path)
    _add_crypto_member(tmp_path, root, MASTER_NODE, "master")
    _add_crypto_member(tmp_path, root, VPS_NODE, "vps")
    store = CredentialStore(tmp_path / "credentials")
    record = store.create_cmc("three-node-cmc-secret")

    result = ClusterCredentialPublisher(root, store).publish_cmc(record["id"])
    envelope = deserialize_sealed_secret(_blob_raw(root, result["sealed_blob_hash"]))

    expected = sorted([LOCAL_NODE, MASTER_NODE, VPS_NODE])
    assert result["recipient_ids"] == expected
    assert [entry["node_id"] for entry in envelope["recipients"]] == expected
    assert envelope["audience"] == "cluster"


def test_tradfi_publication_requires_v2_on_every_active_replica(tmp_path: Path) -> None:
    """TradFi publication still blocks on an active pre-v2 VPS replica."""

    root = _cluster(tmp_path)
    _add_crypto_member(tmp_path, root, MASTER_NODE, "master")
    vps_keys = ensure_node_key_material(tmp_path / "keys-vps-legacy")
    _write_remote_member(
        root,
        VPS_NODE,
        "vps",
        vps_keys,
        {
            "credential_capable": False,
            "credential_protocol_version": 1,
        },
    )
    store = CredentialStore(tmp_path / "credentials")
    record = store.create_tradfi(
        "alpaca",
        {"api_key": "tradfi-api-secret", "api_secret": "tradfi-private-secret"},
        shared=True,
    )

    with pytest.raises(CredentialPublicationError, match="active vps.*lacks v2 crypto"):
        ClusterCredentialPublisher(root, store).publish_tradfi(record["id"])

    assert not ClusterPaths.from_root(root).sealed_blobs.exists()


def test_cmc_publication_rejects_active_member_without_v2_capability(tmp_path: Path) -> None:
    """An active pre-v2 member cannot be silently omitted from CMC."""

    root = _cluster(tmp_path)
    vps_keys = ensure_node_key_material(tmp_path / "keys-vps-incomplete")
    _write_remote_member(
        root,
        VPS_NODE,
        "vps",
        vps_keys,
        {
            "credential_capable": False,
            "credential_protocol_version": 1,
        },
    )
    store = CredentialStore(tmp_path / "credentials")
    record = store.create_cmc("must-not-publish")

    with pytest.raises(CredentialPublicationError, match="lacks v2 crypto"):
        ClusterCredentialPublisher(root, store).publish_cmc(record["id"])

    assert not ClusterPaths.from_root(root).sealed_blobs.exists()
    assert not any(operation["op"] == "UPSERT_SECRET" for operation in load_operations(root))


def test_cmc_publication_is_idempotent_and_uses_current_cas_parents(tmp_path: Path) -> None:
    """Retries append nothing, while a replacement generation advances every CAS parent."""

    root = _cluster(tmp_path)
    store = CredentialStore(tmp_path / "credentials")
    record = store.create_cmc("generation-one")
    publisher = ClusterCredentialPublisher(root, store)

    first = publisher.publish_cmc(record["id"])
    operation_count = len(load_operations(root))
    second = publisher.publish_cmc(record["id"])

    assert second["status"] == "already_published"
    assert second["sealed_blob_hash"] == first["sealed_blob_hash"]
    assert len(load_operations(root)) == operation_count

    store.update_cmc(record["id"], api_key="generation-two")
    publisher.publish_cmc(record["id"], generation=2)
    operations = load_operations(root)
    secret = next(
        operation
        for operation in operations
        if operation["op"] == "UPSERT_SECRET" and operation["generation"] == 2
    )
    pool = next(
        operation
        for operation in operations
        if operation["op"] == "UPSERT_CMC_POOL_ENTRY"
        and operation["catalog_generation"] == 2
    )
    key_state = next(
        operation
        for operation in operations
        if operation["op"] == "SET_CMC_KEY_STATE"
        and operation.get("credential_generation") == 2
    )
    assert secret["parent_generation"] == 1
    assert pool["parent_generation"] == 1
    assert key_state["parent_generation"] == 1
    assert key_state["state_generation"] == 2
    desired = rebuild_materialized_state(root)["desired_state"]
    assert desired["secrets"][record["id"]]["conflicted"] is False
    assert desired["cmc_pool"]["entries"][record["id"]]["conflicted"] is False


def test_conflicted_generation_is_not_treated_as_an_idempotent_retry(tmp_path: Path) -> None:
    """Sibling generation conflicts block retry instead of hiding the CAS conflict."""

    root = _cluster(tmp_path)
    store = CredentialStore(tmp_path / "credentials")
    record = store.create_cmc("conflict-source")
    publisher = ClusterCredentialPublisher(root, store)
    publisher.publish_cmc(record["id"])
    append_operation(
        root,
        "UPSERT_SECRET",
        {
            "secret_id": record["id"],
            "secret_kind": "cmc_api_key",
            "audience": "cluster",
            "generation": 2,
            "parent_generation": 0,
            "sealed_blob_hash": "sha256:" + "a" * 64,
        },
    )

    with pytest.raises(CredentialPublicationError, match="conflicted"):
        publisher.publish_cmc(record["id"])


def test_disable_and_tombstone_operations_are_idempotent(tmp_path: Path) -> None:
    """Repeated disable and tombstone requests append each lifecycle change once."""

    root = _cluster(tmp_path)
    store = CredentialStore(tmp_path / "credentials")
    record = store.create_cmc("lifecycle-secret")
    publisher = ClusterCredentialPublisher(root, store)
    publisher.publish_cmc(record["id"])

    disabled = publisher.disable_cmc(record["id"])
    disabled_again = publisher.disable_cmc(record["id"])
    assert disabled["status"] == "disabled"
    assert disabled_again["status"] == "already_disabled"

    tombstoned = publisher.publish_tombstone(record["id"], "cmc_api_key")
    tombstoned_again = publisher.publish_tombstone(record["id"], "cmc_api_key")
    assert tombstoned["status"] == "tombstoned"
    assert tombstoned_again["status"] == "already_tombstoned"

    operations = load_operations(root)
    assert sum(
        operation["op"] == "SET_CMC_KEY_STATE" and operation["state"] == "disabled"
        for operation in operations
    ) == 1
    assert sum(operation["op"] == "TOMBSTONE_SECRET" for operation in operations) == 1
    assert sum(
        operation["op"] == "SET_CMC_KEY_STATE" and operation["state"] == "tombstoned"
        for operation in operations
    ) == 1
    desired = rebuild_materialized_state(root)["desired_state"]
    tombstone = desired["secret_tombstones"][record["id"]]
    assert tombstone["generation"] == 2
    assert tombstone["parent_generation"] == 1
    assert desired["cmc_pool"]["entries"][record["id"]]["state"] == "tombstoned"


def test_recipient_rewrap_tracks_add_role_change_and_remove_without_provider_rotation(
    tmp_path: Path,
) -> None:
    """Recipient CAS updates follow VPS/master lifecycle while provider generations stay fixed."""

    root = _cluster(tmp_path)
    store = CredentialStore(tmp_path / "credentials")
    cmc = store.create_cmc("recipient-cmc")
    tradfi = store.create_tradfi(
        "alpaca",
        {"api_key": "recipient-tradfi", "api_secret": "recipient-private"},
    )
    publisher = ClusterCredentialPublisher(root, store)
    publisher.publish_cmc(cmc["id"])
    publisher.publish_tradfi(tradfi["id"])

    _add_crypto_member(tmp_path, root, VPS_NODE, "vps")
    materialized = rebuild_materialized_state(root, write=False)
    stale = materialized["desired_state"]
    assert stale["secrets"][cmc["id"]]["generation"] == 1
    assert credential_lifecycle_status(materialized)["nodes"][VPS_NODE]["credential_active"] is False

    added_vps = publisher.rewrap()
    desired = rebuild_materialized_state(root, write=False)["desired_state"]
    assert added_vps["rewrapped"] == 2
    assert desired["secrets"][cmc["id"]]["recipient_ids"] == sorted([LOCAL_NODE, VPS_NODE])
    assert desired["secrets"][tradfi["id"]]["recipient_ids"] == [LOCAL_NODE]
    assert desired["secrets"][cmc["id"]]["generation"] == 1
    assert desired["secrets"][cmc["id"]]["recipient_generation"] == 2

    append_operation(root, "UPDATE_NODE", {"node_id": VPS_NODE, "role": "master"})
    publisher.rewrap()
    desired = rebuild_materialized_state(root, write=False)["desired_state"]
    assert desired["secrets"][tradfi["id"]]["recipient_ids"] == sorted([LOCAL_NODE, VPS_NODE])
    assert desired["secrets"][tradfi["id"]]["generation"] == 1
    assert desired["secrets"][tradfi["id"]]["recipient_generation"] == 3

    append_operation(root, "REMOVE_NODE", {"node_id": VPS_NODE})
    publisher.rewrap()
    desired = rebuild_materialized_state(root, write=False)["desired_state"]
    assert desired["secrets"][cmc["id"]]["recipient_ids"] == [LOCAL_NODE]
    assert desired["secrets"][tradfi["id"]]["recipient_ids"] == [LOCAL_NODE]
    assert all(secret["generation"] == 1 for secret in desired["secrets"].values())
    assert sum(
        operation["op"] == "UPDATE_SECRET_RECIPIENTS"
        for operation in load_operations(root)
    ) == 6


def test_add_master_rewraps_cmc_and_tradfi_for_the_new_recipient(tmp_path: Path) -> None:
    """A newly joined v2 master receives every current CMC and TradFi wrapper."""

    root = _cluster(tmp_path)
    store = CredentialStore(tmp_path / "credentials")
    cmc = store.create_cmc("master-cmc")
    tradfi = store.create_tradfi("alpaca", {"key": "master-tradfi"})
    publisher = ClusterCredentialPublisher(root, store)
    publisher.publish_cmc(cmc["id"])
    publisher.publish_tradfi(tradfi["id"])
    _add_crypto_member(tmp_path, root, MASTER_NODE, "master")

    publisher.rewrap()

    desired = rebuild_materialized_state(root, write=False)["desired_state"]
    for credential_id in (cmc["id"], tradfi["id"]):
        secret = desired["secrets"][credential_id]
        assert secret["recipient_ids"] == sorted([LOCAL_NODE, MASTER_NODE])
        envelope = deserialize_sealed_secret(_blob_raw(root, secret["sealed_blob_hash"]))
        assert [entry["node_id"] for entry in envelope["recipients"]] == sorted(
            [LOCAL_NODE, MASTER_NODE]
        )


def test_missing_protocol_is_not_assumed_v2_for_tradfi(tmp_path: Path) -> None:
    """A crypto-keyed active VPS without an explicit protocol blocks TradFi publication."""

    root = _cluster(tmp_path)
    vps_keys = ensure_node_key_material(tmp_path / "keys-vps-no-protocol")
    _write_remote_member(root, VPS_NODE, "vps", vps_keys, {"credential_capable": True})
    store = CredentialStore(tmp_path / "credentials")
    record = store.create_tradfi("alpaca", {"key": "blocked"})

    with pytest.raises(CredentialPublicationError, match="explicit protocol v2"):
        ClusterCredentialPublisher(root, store).publish_tradfi(record["id"])


@pytest.mark.parametrize(
    "crash_stage",
    ["prepared", "membership_published", "keys_activated", "secrets_rewrapped"],
)
def test_local_key_rotation_recovers_every_crash_stage_and_preserves_history(
    tmp_path: Path,
    crash_stage: str,
) -> None:
    """Rotation recovery always leaves the current old/new wrapper decryptable."""

    root = _cluster(tmp_path)
    store = CredentialStore(tmp_path / "credentials")
    record = store.create_cmc("rotation-secret")
    publisher = ClusterCredentialPublisher(root, store)
    publication = publisher.publish_cmc(record["id"])
    old_envelope = deserialize_sealed_secret(_blob_raw(root, publication["sealed_blob_hash"]))
    old_signing_key_id = str(old_envelope["signing_key_id"])

    def crash(stage: str) -> None:
        if stage == crash_stage:
            raise RuntimeError(f"crash at {stage}")

    with pytest.raises(RuntimeError, match="crash at"):
        publisher.rotate_local_keys(crash_hook=crash)

    current = rebuild_materialized_state(root, write=False)["desired_state"]["secrets"][record["id"]]
    current_envelope = deserialize_sealed_secret(_blob_raw(root, current["sealed_blob_hash"]))
    context = SecretContext(CLUSTER_ID, record["id"], "cmc_api_key", 1, "cluster")
    signing_key = membership_signing_public_key(
        root,
        str(current_envelope["signer_id"]),
        str(current_envelope["signing_key_id"]),
    )
    opened = None
    for private_key in load_node_encryption_private_keys(root):
        try:
            opened = open_sealed_secret(
                current_envelope,
                LOCAL_NODE,
                private_key,
                signing_key,
                expected_context=context,
                membership_roles={LOCAL_NODE: "master"},
            )
            break
        except Exception:
            continue
    assert opened is not None and b"rotation-secret" in opened

    completed = publisher.rotate_local_keys()
    final = rebuild_materialized_state(root, write=False)["desired_state"]["secrets"][record["id"]]
    assert completed["status"] == "rotated"
    assert final["generation"] == 1
    assert final["recipient_generation"] == 2
    old_signing_key = membership_signing_public_key(root, LOCAL_NODE, old_signing_key_id)
    assert validate_sealed_secret(
        old_envelope,
        old_signing_key,
        expected_context=context,
        membership_roles={LOCAL_NODE: "master"},
    )
