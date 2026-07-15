"""Isolated tests for CMC lease authority and transitive relay mailbox."""

from __future__ import annotations

import json
import os
from pathlib import Path
import stat
import time

import pytest

from cluster_credentials import ensure_node_key_material
from cluster_sync_command import ClusterSyncCommandError, run_command
from cmc_leases import CmcLeaseAuthority, CmcLeaseError, CmcLeaseProvider, ClusterMailbox
from cmc_pool import CmcPoolClient
from credential_store import CredentialStore
from master.cluster_state import (
    append_operation,
    create_join_authorization,
    default_cluster_root,
    ensure_local_identity,
    write_operation,
)
from master.cluster_sync_worker import ClusterSyncWorker


CLUSTER_ID = "pbgui-cluster-00000000-0000-4000-8000-000000000020"
MASTER_A = "pbgui-node-00000000-0000-4000-8000-000000000020"
VPS_RELAY = "pbgui-node-00000000-0000-4000-8000-000000000021"
MASTER_B = "pbgui-node-00000000-0000-4000-8000-000000000022"


class _Clock:
    """Controllable integer clock for lease and mailbox expiry."""

    def __init__(self, value: int = 1_700_000_000) -> None:
        """Initialize a deterministic epoch."""

        self.value = value

    def __call__(self) -> float:
        """Return the current test epoch."""

        return float(self.value)


class _LocalPeerClient:
    """Execute Cluster Sync commands against isolated local roots."""

    def __init__(self, roots: dict[str, Path]) -> None:
        """Store cluster roots by node ID."""

        self.roots = roots

    def run(
        self,
        peer: dict,
        local_node_id: str,
        command_text: str,
        payload: str | bytes | None = None,
    ) -> dict:
        """Run one forced command without network access."""

        raw = payload if isinstance(payload, bytes) else str(payload or "").encode("utf-8")
        return run_command(
            self.roots[str(peer["node_id"])],
            local_node_id,
            command_text,
            raw,
        )


def _cluster_roots(tmp_path: Path) -> tuple[dict[str, Path], dict[str, Path]]:
    """Create three replicas with common crypto-capable relay membership."""

    pbgdirs = {
        MASTER_A: tmp_path / "master-a",
        VPS_RELAY: tmp_path / "relay",
        MASTER_B: tmp_path / "master-b",
    }
    roots = {node_id: default_cluster_root(path) for node_id, path in pbgdirs.items()}
    roles = {MASTER_A: "master", VPS_RELAY: "vps", MASTER_B: "master"}
    for node_id, root in roots.items():
        ensure_local_identity(
            root,
            role=roles[node_id],
            pbname=node_id[-2:],
            cluster_id=CLUSTER_ID,
            node_id=node_id,
            created_at=100,
        )
        ensure_node_key_material(root)
    bundles = {
        node_id: ensure_node_key_material(root).public_bundle(node_id, roles[node_id])
        for node_id, root in roots.items()
    }
    sync_peers = {
        MASTER_A: [VPS_RELAY],
        VPS_RELAY: [MASTER_A, MASTER_B],
        MASTER_B: [VPS_RELAY],
    }
    hosts = {MASTER_A: "master-a", VPS_RELAY: "relay", MASTER_B: "master-b"}
    membership = {}
    for index, node_id in enumerate((MASTER_A, VPS_RELAY, MASTER_B), start=1):
        if node_id != MASTER_A:
            for root in roots.values():
                if not (root / "oplog" / MASTER_A / "00000001.json").exists():
                    write_operation(root, membership[MASTER_A])
        authorization = (
            None
            if node_id == MASTER_A
            else create_join_authorization(
                roots[MASTER_A], node_id, roles[node_id], created_at=100 + index
            )
        )
        payload = {
            **bundles[node_id],
            "node_id": node_id,
            "role": roles[node_id],
            "pbname": node_id[-2:],
            "ssh_host": hosts[node_id],
            "sync_peers": sync_peers[node_id],
        }
        if authorization is not None:
            payload["membership_authorization"] = authorization
        membership[node_id] = append_operation(
            roots[node_id],
            "ADD_NODE",
            payload,
            created_at=100 + index,
        )
    for root in roots.values():
        for node_id, operation in membership.items():
            if root != roots[node_id]:
                write_operation(root, operation)
    return roots, pbgdirs


def _candidate(credential_id: str = "cmc_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa") -> dict:
    """Return public imported/shared candidate metadata."""

    return {
        "id": credential_id,
        "generation": 3,
        "origin": "imported",
        "shared": True,
        "quota_domain_id": "provider-account-a",
    }


def test_authority_duplicate_grant_and_settlement_survive_restart(tmp_path: Path) -> None:
    """Request binding and terminal settlement remain exact after a crash restart."""

    clock = _Clock()
    root = tmp_path / "authority"
    authority = CmcLeaseAuthority(root, clock=clock)
    first = authority.grant(
        "request-a",
        [_candidate()],
        recipient=MASTER_A,
        estimated_credits=2,
    )
    duplicate = authority.grant(
        "request-a",
        [_candidate("cmc_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")],
        recipient=MASTER_B,
        estimated_credits=5,
    )

    assert first == duplicate
    assert first is not None
    restarted = CmcLeaseAuthority(root, clock=clock)
    assert restarted.grant(
        "request-a",
        [],
        recipient=MASTER_A,
        estimated_credits=1,
    ) == first
    settled = restarted.settle(
        str(first["lease_id"]),
        outcome="success",
        actual_credits=1,
        status_code=200,
    )
    duplicate_settlement = restarted.settle(
        str(first["lease_id"]),
        outcome="error",
        actual_credits=9,
        status_code=500,
    )

    assert duplicate_settlement == settled
    key = restarted.status()["keys"][str(first["credential_id"])]
    assert key["reserved_credits_micros"] == 0
    assert key["used_credits_micros"] == 1_000_000
    assert key["used_requests"] == 1
    assert stat.S_IMODE(root.stat().st_mode) == 0o700
    assert stat.S_IMODE((root / "journal.json").stat().st_mode) == 0o600


def test_mailbox_serialization_is_signed_private_and_secret_free(tmp_path: Path) -> None:
    """Public signed messages and command responses contain no credential value."""

    roots, _pbgdirs = _cluster_roots(tmp_path)
    mailbox = ClusterMailbox(roots[MASTER_A])
    message = mailbox.create_message(
        "CMC_LEASE_REQUEST",
        MASTER_B,
        {
            "request_id": "request-a",
            "credits_micros": 1_000_000,
            "request_count": 1,
            "candidates": [_candidate()],
        },
    )
    assert mailbox.put(message) is True
    assert mailbox.put(message) is False

    index = run_command(roots[MASTER_A], VPS_RELAY, "get-mailbox-index")
    fetched = run_command(
        roots[MASTER_A],
        VPS_RELAY,
        f"get-mailbox-message {message['message_id']}",
    )
    serialized = json.dumps({"index": index, "fetched": fetched, "message": message})
    assert "signature" in fetched["message"]
    assert "api_key" not in serialized
    assert "secret-value" not in serialized
    assert "payload" not in json.dumps(index)
    unknown = "pbgui-node-00000000-0000-4000-8000-000000000099"
    with pytest.raises(ClusterSyncCommandError, match="not registered"):
        run_command(
            roots[MASTER_A],
            unknown,
            "get-mailbox-index",
            allow_join=True,
        )
    with pytest.raises(CmcLeaseError, match="forbidden field"):
        mailbox.create_message(
            "CMC_LEASE_REQUEST",
            MASTER_B,
            {"api_key": "secret-value"},
        )
    if os.name == "posix":
        for path in [mailbox.root, *mailbox.root.rglob("*")]:
            if path.is_dir():
                assert stat.S_IMODE(path.stat().st_mode) == 0o700, path
            elif path.is_file():
                assert stat.S_IMODE(path.stat().st_mode) == 0o600, path


def test_mailbox_ttl_and_recipient_ack_garbage_collection(tmp_path: Path) -> None:
    """Expired and recipient-acknowledged messages are removed idempotently."""

    roots, _pbgdirs = _cluster_roots(tmp_path)
    clock = _Clock(int(time.time()))
    mailbox = ClusterMailbox(roots[MASTER_A], clock=clock)
    expired = mailbox.create_message(
        "CMC_PROVIDER_EVENT",
        MASTER_B,
        {"event_id": "event-a", "status": "healthy"},
        ttl=5,
    )
    mailbox.put(expired)
    clock.value += 6
    assert mailbox.index() == []

    live = mailbox.create_message(
        "CMC_PROVIDER_EVENT",
        VPS_RELAY,
        {"event_id": "event-b", "status": "healthy"},
    )
    mailbox.put(live)
    first = run_command(
        roots[MASTER_A],
        VPS_RELAY,
        f"ack-mailbox-message {live['message_id']}",
    )
    second = run_command(
        roots[MASTER_A],
        VPS_RELAY,
        f"ack-mailbox-message {live['message_id']}",
    )
    assert first["created"] is True
    assert second["created"] is False
    assert mailbox.index() == []


def test_worker_forwards_master_message_through_vps_relay(tmp_path: Path) -> None:
    """Master A reaches Master B through a VPS without direct master connectivity."""

    roots, _pbgdirs = _cluster_roots(tmp_path)
    message = ClusterMailbox(roots[MASTER_A]).create_message(
        "CMC_LEASE_REQUEST",
        MASTER_B,
        {
            "request_id": "request-relay",
            "credits_micros": 1_000_000,
            "request_count": 1,
            "candidates": [_candidate()],
        },
    )
    ClusterMailbox(roots[MASTER_A]).put(message)
    client = _LocalPeerClient(roots)
    nodes = ClusterMailbox(roots[MASTER_A])._membership_nodes()
    source_worker = ClusterSyncWorker(tmp_path / "master-a", peer_client=client)
    source_status = source_worker._sync_mailbox(nodes[VPS_RELAY], MASTER_A)
    assert source_status["pushed"] >= 1
    assert ClusterMailbox(roots[VPS_RELAY]).get(str(message["message_id"])) == message
    assert ClusterMailbox(roots[VPS_RELAY]).get(str(message["message_id"]))["recipient"] == MASTER_B
    with pytest.raises(CmcLeaseError):
        ClusterMailbox(roots[MASTER_B]).get(str(message["message_id"]))

    relay_nodes = ClusterMailbox(roots[VPS_RELAY])._membership_nodes()
    relay_worker = ClusterSyncWorker(tmp_path / "relay", peer_client=client)
    relay_status = relay_worker._sync_mailbox(relay_nodes[MASTER_B], VPS_RELAY)
    assert relay_status["pushed"] >= 1
    delivered = ClusterMailbox(roots[MASTER_B]).get(str(message["message_id"]))
    assert delivered == message
    assert delivered["sender"] == MASTER_A


def test_provider_processes_only_recipient_and_returns_delivered_grant(tmp_path: Path) -> None:
    """A relay stays opaque while the addressed authority grants a matching lease."""

    roots, _pbgdirs = _cluster_roots(tmp_path)
    source = CmcLeaseProvider(ClusterMailbox(roots[MASTER_A]), MASTER_B)
    candidate = _candidate()
    assert source.acquire([candidate], endpoint="/v1/cryptocurrency/map", estimated_credits=1) is None
    request = next(
        ClusterMailbox(roots[MASTER_A]).get(str(item["message_id"]))
        for item in ClusterMailbox(roots[MASTER_A]).index()
        if item["message_type"] == "CMC_LEASE_REQUEST"
    )

    relay_authority = CmcLeaseAuthority(tmp_path / "relay-authority")
    relay_provider = CmcLeaseProvider(
        ClusterMailbox(roots[VPS_RELAY]),
        VPS_RELAY,
        authority=relay_authority,
    )
    ClusterMailbox(roots[VPS_RELAY]).put(request)
    assert relay_provider.process_inbox()["processed"] == 0
    assert relay_authority.status()["requests"] == {}

    destination_authority = CmcLeaseAuthority(tmp_path / "destination-authority")
    destination_provider = CmcLeaseProvider(
        ClusterMailbox(roots[MASTER_B]),
        MASTER_B,
        authority=destination_authority,
    )
    ClusterMailbox(roots[MASTER_B]).put(request)
    destination_counts = destination_provider.process_inbox()
    assert destination_counts["granted"] == 1
    for item in ClusterMailbox(roots[MASTER_B]).index():
        message = ClusterMailbox(roots[MASTER_B]).get(str(item["message_id"]))
        if message["recipient"] == MASTER_A:
            ClusterMailbox(roots[MASTER_A]).put(message)

    granted = source.acquire([candidate], endpoint="/v1/cryptocurrency/map", estimated_credits=1)
    assert granted is not None
    assert granted["credential_id"] == candidate["id"]
    assert granted["lease_token"]

    assert source.settle(granted["lease_token"], status_code=200) == {
        "queued": True,
        "lease_id": granted["lease_token"],
    }
    assert source.settle(granted["lease_token"], status_code=500) == {
        "queued": True,
        "lease_id": granted["lease_token"],
    }
    settlement_messages = [
        ClusterMailbox(roots[MASTER_A]).get(str(item["message_id"]))
        for item in ClusterMailbox(roots[MASTER_A]).index()
        if item["message_type"] == "CMC_LEASE_SETTLEMENT"
    ]
    assert len(settlement_messages) == 1
    assert settlement_messages[0]["payload"]["outcome"] == "success"
    ClusterMailbox(roots[MASTER_B]).put(settlement_messages[0])
    assert destination_provider.process_inbox()["settled"] == 1
    first_settlement = destination_authority.status()["leases"][granted["lease_token"]]["settlement"]
    ClusterMailbox(roots[MASTER_B]).put(settlement_messages[0])
    assert destination_provider.process_inbox()["settled"] == 1
    assert destination_authority.status()["leases"][granted["lease_token"]]["settlement"] == first_settlement


def test_provider_uses_immediate_local_authority_with_cmc_pool(tmp_path: Path) -> None:
    """A local authority grants and settles synchronously through CmcPoolClient."""

    roots, _pbgdirs = _cluster_roots(tmp_path)
    store = CredentialStore(tmp_path / "local-credentials")
    record = store.create_cmc("local-authority-secret", origin="local", shared=False)
    authority = CmcLeaseAuthority(tmp_path / "local-authority")
    provider = CmcLeaseProvider(
        ClusterMailbox(roots[MASTER_A]),
        MASTER_A,
        authority=authority,
    )
    pool = CmcPoolClient(
        store,
        state_root=tmp_path / "local-authority-pool",
        lease_provider=provider,
    )

    acquisition = pool.acquire("/v1/cryptocurrency/map")
    pool.settle(acquisition, status_code=200)

    assert acquisition.credential_id == record["id"]
    assert acquisition.lease_token is not None
    lease = authority.status()["leases"][acquisition.lease_token]
    assert lease["terminal"] is True
    assert lease["settlement"]["outcome"] == "success"


@pytest.mark.parametrize("broken", [False, True])
def test_cmc_pool_soft_falls_back_when_remote_grant_missing_or_broken(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    broken: bool,
) -> None:
    """Missing or broken best-effort mailbox grants never block local selection."""

    roots, _pbgdirs = _cluster_roots(tmp_path)
    store = CredentialStore(tmp_path / "credentials")
    record = store.create_cmc("local-secret", origin="imported", shared=True)
    mailbox = ClusterMailbox(roots[MASTER_A])
    provider = CmcLeaseProvider(mailbox, MASTER_B)
    if broken:
        monkeypatch.setattr(mailbox, "put", lambda _message: (_ for _ in ()).throw(OSError("broken mailbox")))
    pool = CmcPoolClient(
        store,
        state_root=tmp_path / f"pool-{broken}",
        lease_provider=provider,
    )

    acquisition = pool.acquire("/v1/cryptocurrency/map")

    assert acquisition.credential_id == record["id"]
    assert acquisition.api_key == "local-secret"
    assert acquisition.lease_token is None


def test_authority_enforces_quota_domain_windows_and_concurrency(tmp_path: Path) -> None:
    """Different credential IDs cannot bypass shared rolling or concurrent limits."""

    clock = _Clock(1_704_067_200)
    authority = CmcLeaseAuthority(
        tmp_path / "authority",
        per_key_credit_limit=100,
        minute_request_limit=1,
        daily_credit_limit=2,
        monthly_credit_limit=3,
        concurrent_limit=1,
        clock=clock,
    )
    first = _candidate("cmc_" + "a" * 32)
    second = _candidate("cmc_" + "b" * 32)

    lease = authority.grant("first", [first], recipient=MASTER_A, estimated_credits=2)
    assert lease is not None
    assert authority.grant("concurrent", [second], recipient=MASTER_A, estimated_credits=1) is None
    authority.settle(str(lease["lease_id"]), actual_credits=2)
    assert authority.grant("minute", [second], recipient=MASTER_A, estimated_credits=1) is None

    clock.value += 86_400
    daily_reset = authority.grant("daily-reset", [second], recipient=MASTER_A, estimated_credits=1)
    assert daily_reset is not None
    authority.settle(str(daily_reset["lease_id"]), actual_credits=1)
    assert authority.grant("monthly-full", [first], recipient=MASTER_A, estimated_credits=1) is None

    clock.value = 1_706_745_600
    assert authority.grant("monthly-reset", [first], recipient=MASTER_A, estimated_credits=2) is not None


def test_authority_rejects_stale_epochs_and_counts_expired_spend_uncertainly(
    tmp_path: Path,
) -> None:
    """Epoch changes fence old traffic and expired attempts consume uncertain budget."""

    clock = _Clock()
    authority = CmcLeaseAuthority(
        tmp_path / "authority",
        authority_epochs={"provider-account-a": 2},
        lease_ttl=5,
        clock=clock,
    )
    with pytest.raises(CmcLeaseError, match="stale"):
        authority.grant(
            "stale",
            [_candidate()],
            recipient=MASTER_A,
            estimated_credits=1,
            authority_epoch=1,
        )
    lease = authority.grant(
        "current",
        [_candidate()],
        recipient=MASTER_A,
        estimated_credits=2,
        authority_epoch=2,
    )
    assert lease is not None
    assert lease["authority_epoch"] == 2
    assert lease["quota_domain_id"] == "provider-account-a"

    authority.update_authority_epochs({"provider-account-a": 3})
    with pytest.raises(CmcLeaseError, match="stale"):
        authority.settle(str(lease["lease_id"]), authority_epoch=2)
    clock.value += 6
    domain = authority.status()["domains"]["provider-account-a"]
    assert domain["concurrent_leases"] == 0
    assert domain["uncertain_credits_micros"] == 2_000_000
