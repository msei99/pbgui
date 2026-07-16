"""Tests for ACK-gated credential activation and recovery."""

from __future__ import annotations

from pathlib import Path
import json
import time
from unittest.mock import MagicMock

import pytest

from cluster_credential_publisher import ClusterCredentialPublisher
from cluster_credentials import ensure_node_key_material, sign_operation
import credential_reconciler
from credential_reconciler import reconcile_pending_credentials
from credential_store import CredentialStore
from master.cluster_state import (
    append_operation,
    create_join_authorization,
    ensure_local_identity,
    read_local_identity,
    rebuild_materialized_state,
    write_operation,
)


REMOTE_MASTER = "pbgui-node-33333333-3333-4333-8333-333333333333"


def _add_remote_master(cluster_root: Path, key_root: Path) -> None:
    """Add one independently signed protocol-v2 master."""

    keys = ensure_node_key_material(key_root)
    identity = read_local_identity(cluster_root)
    authorization = create_join_authorization(cluster_root, REMOTE_MASTER, "master")
    operation = {
        **keys.public_bundle(REMOTE_MASTER, "master"),
        "schema_version": 1,
        "cluster_id": identity["cluster_id"],
        "op_id": f"{REMOTE_MASTER}:00000001",
        "actor": REMOTE_MASTER,
        "seq": 1,
        "op": "ADD_NODE",
        "created_at": int(time.time()) + 1,
        "node_id": REMOTE_MASTER,
        "role": "master",
        "credential_protocol_version": 2,
        "credential_capable": True,
        "membership_authorization": authorization,
    }
    write_operation(
        cluster_root,
        sign_operation(operation, keys.signing_private_key, signer_id=REMOTE_MASTER),
        network_input=True,
    )


def _stub_cmc_key_info(
    monkeypatch: pytest.MonkeyPatch,
    provider_value,
    *,
    credits_used: int | None = None,
) -> None:
    """Return one isolated successful CoinMarketCap key-info response."""

    payload = json.dumps({
        "status": {"error_code": 0},
        "data": {
            "plan": {
                "name": "Basic",
                "credit_limit_monthly": provider_value,
            },
            "usage": {
                "current_month": (
                    {"credits_used": credits_used}
                    if credits_used is not None
                    else {}
                ),
            },
        },
    }).encode("utf-8")
    response = MagicMock()
    response.__enter__.return_value = response
    response.read.return_value = payload
    monkeypatch.setattr(
        credential_reconciler,
        "urlopen",
        lambda *_args, **_kwargs: response,
    )


@pytest.mark.parametrize("provider_value", [10_000, 10_000.0, "10000.0"])
def test_cmc_provider_snapshot_uses_integer_signed_monthly_limit(
    monkeypatch: pytest.MonkeyPatch,
    provider_value,
) -> None:
    """Whole provider limits remain canonical integers in signed pool metadata."""

    _stub_cmc_key_info(monkeypatch, provider_value, credits_used=125)

    snapshot = credential_reconciler._snapshot_cmc_basis("write-only-secret")

    assert snapshot["provider_limit"] == 10_000.0
    assert snapshot["monthly_limit"] == 10_000
    assert type(snapshot["monthly_limit"]) is int


def test_cmc_provider_snapshot_does_not_round_fractional_monthly_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unexpected fractional provider limits stay local and out of signed metadata."""

    _stub_cmc_key_info(monkeypatch, 10_000.5)

    snapshot = credential_reconciler._snapshot_cmc_basis("write-only-secret")

    assert snapshot["provider_limit"] == 10_000.5
    assert "monthly_limit" not in snapshot


def test_tradfi_activation_waits_for_every_master_ack(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """A multi-master profile remains pending until exact remote materialization ACK."""

    cluster_root = tmp_path / "data" / "cluster"
    ensure_local_identity(cluster_root, role="master", pbname="local")
    store = CredentialStore(tmp_path / "data" / "credentials")
    publisher = ClusterCredentialPublisher(cluster_root, store)
    publisher._ensure_local_crypto_membership()
    remote_key_root = tmp_path / "remote-keys"
    _add_remote_master(cluster_root, remote_key_root)
    pb7_root = tmp_path / "pb7"
    pb7_root.mkdir()
    monkeypatch.setattr(credential_reconciler, "pb7dir", lambda: str(pb7_root))
    record = store.create_tradfi(
        "tiingo",
        {"api_key": "pending-token"},
        pending=True,
        operation_id="tradfi-pending-1",
    )

    pending = reconcile_pending_credentials(tmp_path, store=store, publisher=publisher)
    desired = rebuild_materialized_state(cluster_root, write=False)["desired_state"]
    assert pending["items"][0]["status"] == "pending"
    assert desired["tradfi_active_profiles"] == {}
    assert store.list_tradfi(active_only=True) == []

    materialized = rebuild_materialized_state(cluster_root, write=False)
    secret = materialized["desired_state"]["secrets"][record["id"]]
    membership_generation = materialized["cluster_nodes"]["credential_membership_generation"]
    keys = ensure_node_key_material(remote_key_root)
    ack = {
        "schema_version": 1,
        "cluster_id": read_local_identity(cluster_root)["cluster_id"],
        "op_id": f"{REMOTE_MASTER}:00000002",
        "actor": REMOTE_MASTER,
        "seq": 2,
        "op": "CREDENTIAL_MATERIALIZATION_ACK",
        "created_at": int(time.time()) + 2,
        "node_id": REMOTE_MASTER,
        "membership_generation": membership_generation,
        "credential_generations": {record["id"]: int(secret["generation"])},
        "recipient_generations": {record["id"]: int(secret["recipient_generation"])},
        "actor_role_epoch": 1,
        "actor_membership_op_id": f"{REMOTE_MASTER}:00000001",
    }
    write_operation(
        cluster_root,
        sign_operation(ack, keys.signing_private_key, signer_id=REMOTE_MASTER),
        network_input=True,
    )

    awaiting_projection = reconcile_pending_credentials(tmp_path, store=store, publisher=publisher)
    desired = rebuild_materialized_state(cluster_root, write=False)["desired_state"]
    assert awaiting_projection["items"][0]["status"] == "pending"
    assert desired["tradfi_active_profiles"]["tiingo"]["profile_id"] == record["id"]
    projection_ack = {
        "schema_version": 1,
        "cluster_id": read_local_identity(cluster_root)["cluster_id"],
        "op_id": f"{REMOTE_MASTER}:00000003",
        "actor": REMOTE_MASTER,
        "seq": 3,
        "op": "TRADFI_PROJECTION_ACK",
        "created_at": int(time.time()) + 3,
        "node_id": REMOTE_MASTER,
        "membership_generation": membership_generation,
        "active_profile_generations": {
            "tiingo": desired["tradfi_active_profiles"]["tiingo"]["activation_generation"],
        },
        "projection_applied_generation": 1,
        "projection_status": "current",
        "actor_role_epoch": 1,
        "actor_membership_op_id": f"{REMOTE_MASTER}:00000001",
    }
    write_operation(
        cluster_root,
        sign_operation(projection_ack, keys.signing_private_key, signer_id=REMOTE_MASTER),
        network_input=True,
    )

    active = reconcile_pending_credentials(tmp_path, store=store, publisher=publisher)
    assert active["items"][0]["status"] == "active"
    assert [item["id"] for item in store.list_tradfi(active_only=True)] == [record["id"]]


def test_cmc_activation_waits_for_every_current_recipient_ack(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """CMC remains locally inactive until the remote replica ACKs the exact generation."""

    cluster_root = tmp_path / "data" / "cluster"
    ensure_local_identity(cluster_root, role="master", pbname="local")
    store = CredentialStore(tmp_path / "data" / "credentials")
    publisher = ClusterCredentialPublisher(cluster_root, store)
    publisher._ensure_local_crypto_membership()
    remote_key_root = tmp_path / "remote-keys"
    _add_remote_master(cluster_root, remote_key_root)
    record = store.create_cmc(
        "pending-cmc-token",
        pending=True,
        operation_id="cmc-pending-1",
    )
    _stub_cmc_key_info(monkeypatch, 10_000.0, credits_used=125)

    pending = reconcile_pending_credentials(tmp_path, store=store, publisher=publisher)
    desired = rebuild_materialized_state(cluster_root, write=False)["desired_state"]
    assert pending["items"][0]["status"] == "pending"
    assert desired["cmc_pool"]["entries"][record["id"]]["state"] == "pending"
    assert desired["cmc_pool"]["entries"][record["id"]]["monthly_limit"] == 10_000
    assert type(desired["cmc_pool"]["entries"][record["id"]]["monthly_limit"]) is int
    assert store.list_cmc(active_only=True) == []

    materialized = rebuild_materialized_state(cluster_root, write=False)
    secret = materialized["desired_state"]["secrets"][record["id"]]
    keys = ensure_node_key_material(remote_key_root)
    ack = {
        "schema_version": 1,
        "cluster_id": read_local_identity(cluster_root)["cluster_id"],
        "op_id": f"{REMOTE_MASTER}:00000002",
        "actor": REMOTE_MASTER,
        "seq": 2,
        "op": "CREDENTIAL_MATERIALIZATION_ACK",
        "created_at": int(time.time()) + 2,
        "node_id": REMOTE_MASTER,
        "membership_generation": materialized["cluster_nodes"]["credential_membership_generation"],
        "credential_generations": {record["id"]: int(secret["generation"])},
        "recipient_generations": {record["id"]: int(secret["recipient_generation"])},
        "actor_role_epoch": 1,
        "actor_membership_op_id": f"{REMOTE_MASTER}:00000001",
    }
    write_operation(
        cluster_root,
        sign_operation(ack, keys.signing_private_key, signer_id=REMOTE_MASTER),
        network_input=True,
    )

    active = reconcile_pending_credentials(tmp_path, store=store, publisher=publisher)
    desired = rebuild_materialized_state(cluster_root, write=False)["desired_state"]
    assert active["items"][0]["status"] == "active"
    assert desired["cmc_pool"]["entries"][record["id"]]["state"] == "active"
    assert [item["id"] for item in store.list_cmc(active_only=True)] == [record["id"]]


def test_pending_cmc_rewraps_for_membership_churn_before_ack_evaluation(tmp_path: Path) -> None:
    """Removed recipients stop blocking only after a new exact recipient generation ACK."""

    cluster_root = tmp_path / "data" / "cluster"
    ensure_local_identity(cluster_root, role="master", pbname="local")
    store = CredentialStore(tmp_path / "data" / "credentials")
    publisher = ClusterCredentialPublisher(cluster_root, store)
    publisher._ensure_local_crypto_membership()
    _add_remote_master(cluster_root, tmp_path / "remote-keys")
    record = store.create_cmc(
        "churn-token",
        pending=True,
        operation_id="cmc-churn-1",
    )

    first = reconcile_pending_credentials(tmp_path, store=store, publisher=publisher)
    assert first["items"][0]["status"] == "pending"
    append_operation(
        cluster_root,
        "REMOVE_NODE",
        {"node_id": REMOTE_MASTER},
        created_at=int(time.time()) + 3,
    )

    rewrapped = reconcile_pending_credentials(tmp_path, store=store, publisher=publisher)
    secret = rebuild_materialized_state(cluster_root, write=False)["desired_state"]["secrets"][record["id"]]
    assert rewrapped["items"][0]["status"] == "pending"
    assert secret["recipient_ids"] == [read_local_identity(cluster_root)["node_id"]]
    assert secret["recipient_generation"] == 2

    active = reconcile_pending_credentials(tmp_path, store=store, publisher=publisher)
    assert active["items"][0]["status"] == "active"


def test_tradfi_delete_intent_retries_until_projection_is_removed(tmp_path: Path) -> None:
    """A failed PB7 removal leaves a durable deletion intent for the next cycle."""

    store = CredentialStore(tmp_path / "data" / "credentials")
    record = store.create_tradfi("tiingo", {"api_key": "delete-token"})
    store.begin_tradfi_delete(record["id"], "delete-retry-1")

    class Publisher:
        """Standalone tombstone adapter used after projection succeeds."""

        def publish_tombstone(self, credential_id: str, kind: str) -> dict:
            return {"status": "tombstoned", "credential_id": credential_id, "kind": kind}

    attempts = 0

    def projector(_store: CredentialStore, _pending: str | None) -> dict:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OSError("PB7 projection unavailable")
        return {"status": "current"}

    with pytest.raises(OSError, match="projection unavailable"):
        reconcile_pending_credentials(
            tmp_path,
            store=store,
            publisher=Publisher(),
            tradfi_projector=projector,
        )
    pending = store.get_tradfi(record["id"])
    assert pending["pending_delete"] is True
    assert pending["pending_stage"] == "published"

    result = reconcile_pending_credentials(
        tmp_path,
        store=store,
        publisher=Publisher(),
        tradfi_projector=projector,
    )
    assert result["items"][0]["status"] == "deleted"
    assert store.list_tradfi() == []


def test_cmc_provider_snapshot_failure_is_redacted_and_non_blocking(monkeypatch, tmp_path: Path) -> None:
    """Provider basis network failure records diagnostics but does not block activation."""

    store = CredentialStore(tmp_path / "data" / "credentials")
    record = store.create_cmc(
        "write-only-secret",
        pending=True,
        operation_id="snapshot-best-effort-1",
    )

    class Publisher:
        """Standalone publisher double accepting the production metadata contract."""

        def publish_cmc(self, credential_id: str, **_kwargs) -> dict:
            return {"status": "published", "credential_id": credential_id}

        def disable_cmc(self, credential_id: str) -> dict:
            return {"status": "disabled", "credential_id": credential_id}

    monkeypatch.setattr(
        credential_reconciler,
        "_snapshot_cmc_basis",
        lambda _secret: {
            "validation_status": "unavailable",
            "validation_checked_at": time.time(),
            "validation_error_category": "TimeoutError",
        },
    )

    result = reconcile_pending_credentials(tmp_path, store=store, publisher=Publisher())
    status = credential_reconciler.CmcPoolClient(
        store,
        state_root=store.root / "cmc_pool",
    ).status()["keys"][0]

    assert result["items"][0]["status"] == "active"
    assert store.get_cmc(record["id"]).get("pending") is not True
    assert status["validation_status"] == "unavailable"
    assert status["validation_error_category"] == "TimeoutError"
    assert "write-only-secret" not in str(result) + str(status)
