"""Focused production-construction tests for CMC desired state and leases."""

from __future__ import annotations

import json
from pathlib import Path

import cmc_runtime
import cmc_pool
from cmc_runtime import build_cmc_pool_client
from credential_store import CredentialStore


class _Mailbox:
    """Minimal local mailbox used to exercise the production factory wiring."""

    def __init__(self, cluster_root: Path) -> None:
        """Expose local identity and an isolated provider-state root."""

        self.cluster_root = cluster_root
        self.local_node_id = "node-local"
        self.root = cluster_root / "mailbox"
        self.root.mkdir(parents=True, exist_ok=True)

    def index(self) -> list[dict]:
        """Return no relayed traffic for a synchronous local-authority test."""

        return []


def _strict_snapshot(record: dict, pool: dict) -> dict:
    """Build exact lifecycle metadata for one materialized local credential."""

    node_id = "node-local"
    return {
        "cluster_nodes": {
            "credential_membership_generation": 1,
            "nodes": {node_id: {
                "role": "master",
                "credential_protocol_version": 2,
                "credential_capable": True,
                "signing_key_id": "ed25519:test",
                "encryption_key_id": "x25519:test",
            }},
        },
        "desired_state": {
            "secrets": {record["id"]: {
                "generation": 1,
                "recipient_generation": 1,
                "recipient_ids": [node_id],
                "secret_kind": "cmc_api_key",
            }},
            "credential_materialization_acks": {node_id: {
                "membership_generation": 1,
                "credential_generations": {record["id"]: 1},
                "recipient_generations": {record["id"]: 1},
            }},
            "cmc_pool": pool,
        },
    }


def test_factory_builds_local_authority_from_desired_quota_domain(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Production construction passes desired authority, epoch, and limits to the pool."""

    store = CredentialStore(tmp_path / "data" / "credentials")
    record = store.create_cmc("factory-secret", origin="cluster", shared=True)
    materialized = _strict_snapshot(record, {
                "entries": {record["id"]: {
                    "key_id": record["id"],
                    "secret_id": record["id"],
                    "state": "active",
                    "quota_domain_id": "plan-a",
                    "minute_limit": 5,
                    "daily_limit": 50,
                    "monthly_limit": 500,
                }},
                "authorities": {"plan-a": {
                    "quota_domain_id": "plan-a",
                    "authority_node_id": "node-local",
                    "authority_epoch": 7,
                }},
            })
    monkeypatch.setattr(cmc_runtime, "read_cmc_cluster_snapshot", lambda _root: materialized)
    monkeypatch.setattr(cmc_runtime, "read_local_identity", lambda _root: {"node_id": "node-local"})
    monkeypatch.setattr(cmc_pool, "read_local_identity", lambda _root: {"node_id": "node-local"})
    monkeypatch.setattr(cmc_runtime, "ClusterMailbox", _Mailbox)

    pool = build_cmc_pool_client(tmp_path, credential_store=store)
    acquisition = pool.acquire("/v1/cryptocurrency/map")
    assert acquisition.lease_token is not None
    authority = pool.lease_provider.authority
    lease = authority.status()["requests"]
    granted = next(iter(lease.values()))["lease"]
    assert granted["authority_epoch"] == 7
    assert granted["quota_domain_id"] == "plan-a"
    assert "factory-secret" not in json.dumps(authority.status())


def test_factory_soft_falls_back_to_standalone_local_pool(tmp_path: Path, monkeypatch) -> None:
    """Missing or broken cluster state leaves a materialized local key usable."""

    store = CredentialStore(tmp_path / "data" / "credentials")
    record = store.create_cmc("standalone-secret")
    monkeypatch.setattr(cmc_runtime, "read_cmc_cluster_snapshot", lambda _root: None)

    pool = build_cmc_pool_client(tmp_path, credential_store=store)
    acquisition = pool.acquire("/v1/cryptocurrency/map")
    assert acquisition.credential_id == record["id"]
    assert acquisition.lease_token is None
    assert "standalone-secret" not in json.dumps(pool.status())


def test_factory_soft_falls_back_when_cluster_mailbox_is_broken(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """A desired authority never makes a locally materialized key depend on mailbox health."""

    store = CredentialStore(tmp_path / "data" / "credentials")
    record = store.create_cmc("mailbox-fallback", origin="cluster", shared=True)
    materialized = _strict_snapshot(record, {
                "entries": {record["id"]: {
                    "key_id": record["id"],
                    "secret_id": record["id"],
                    "state": "active",
                    "quota_domain_id": "plan-a",
                }},
                "authorities": {"plan-a": {
                    "quota_domain_id": "plan-a",
                    "authority_node_id": "node-local",
                    "authority_epoch": 1,
                }},
            })
    monkeypatch.setattr(cmc_runtime, "read_cmc_cluster_snapshot", lambda _root: materialized)
    monkeypatch.setattr(cmc_runtime, "read_local_identity", lambda _root: {"node_id": "node-local"})
    monkeypatch.setattr(cmc_pool, "read_local_identity", lambda _root: {"node_id": "node-local"})
    monkeypatch.setattr(
        cmc_runtime,
        "ClusterMailbox",
        lambda _root: (_ for _ in ()).throw(OSError("mailbox unavailable")),
    )

    pool = build_cmc_pool_client(tmp_path, credential_store=store)
    acquisition = pool.acquire("/v1/cryptocurrency/map")
    assert acquisition.credential_id == record["id"]
    assert acquisition.lease_token is None


def test_factory_clients_share_canonical_pool_state(tmp_path: Path, monkeypatch) -> None:
    """Daemon and API construction converge on one credential store and usage journal."""

    store = CredentialStore(tmp_path / "data" / "credentials")
    store.create_cmc("canonical-secret")
    monkeypatch.setattr(cmc_runtime, "read_cmc_cluster_snapshot", lambda _root: None)

    first = build_cmc_pool_client(tmp_path)
    second = build_cmc_pool_client(tmp_path)
    first.acquire("/v1/cryptocurrency/map", estimated_credits=2)

    assert second.store.root == first.store.root
    assert second.state_root == first.state_root
    assert second.status()["keys"][0]["used_credits"] == 2
    assert "canonical-secret" not in json.dumps(second.status())
