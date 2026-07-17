"""Tests for the lightweight PBCluster daemon worker."""

import base64
import json
import hashlib
import stat
import threading
import time
from pathlib import Path

import pytest

from cluster_credentials import ensure_node_key_material
from cluster_sync_command import run_command
from master.cluster_state import (
    append_operation as _append_operation,
    build_config_manifest,
    compute_config_manifest_hash,
    default_cluster_root,
    ensure_local_identity,
    rebuild_materialized_state,
    read_local_identity,
    write_operation,
)
from master.cluster_sync_worker import (
    ClusterBlobBudgetExceeded,
    ClusterSyncWorker,
    ClusterSyncWorkerError,
    SshClusterPeerClient,
    _write_local_blob,
)
import master.cluster_sync_worker as cluster_sync_worker


CLUSTER_ID = "pbgui-cluster-00000000-0000-4000-8000-000000000010"
NODE_ID = "pbgui-node-00000000-0000-4000-8000-000000000010"
NODE_B = "pbgui-node-00000000-0000-4000-8000-000000000011"
NODE_C = "pbgui-node-00000000-0000-4000-8000-000000000012"
HASH_A = "sha256:" + "a" * 64


def append_operation(root: Path, op: str, payload: dict, **kwargs) -> dict:
    """Use historical v1 records for non-local membership in worker fixtures."""

    identity = read_local_identity(root)
    actor = str(identity["node_id"])
    target = str(payload.get("node_id") or "")
    if op == "ADD_NODE" and target:
        actor = target
        actor_dir = Path(root) / "oplog" / actor
        seq = max((int(path.stem) for path in actor_dir.glob("*.json")), default=0) + 1
        membership = dict(payload)
        if target == str(identity["node_id"]):
            membership.update(
                ensure_node_key_material(root).public_bundle(
                    target,
                    str(payload.get("role") or identity.get("role") or "vps"),
                )
            )
        operation = {
            **membership,
            "schema_version": 1,
            "cluster_id": str(identity["cluster_id"]),
            "op_id": f"{actor}:{seq:08d}",
            "actor": actor,
            "seq": seq,
            "op": op,
            "created_at": int(kwargs.get("created_at", 100 + seq)),
        }
        write_operation(root, operation, allow_legacy_membership=True)
        return operation
    return _append_operation(root, op, payload, **kwargs)


def test_worker_pulled_secret_blob_uses_owner_only_tree(tmp_path: Path) -> None:
    """Worker-pulled secret blobs are private from directory creation onward."""
    base = tmp_path / "secret_blobs"
    raw = b'{"secret":"value"}'
    blob_hash = "sha256:" + hashlib.sha256(raw).hexdigest()

    _write_local_blob(base, blob_hash, raw, secret=True)

    digest = blob_hash.removeprefix("sha256:")
    path = base / "sha256" / digest[:2] / f"{digest}.json"
    assert path.read_bytes() == raw
    assert stat.S_IMODE(base.stat().st_mode) == 0o700
    assert stat.S_IMODE(path.parent.stat().st_mode) == 0o700
    assert stat.S_IMODE(path.stat().st_mode) == 0o600


def _node_id(index: int) -> str:
    """Return a deterministic valid test node id."""

    return f"pbgui-node-00000000-0000-4000-8000-{index:012d}"


class _LocalPeerClient:
    """Peer client that executes wrapper commands against local temp roots."""

    def __init__(self, roots: dict[str, Path]) -> None:
        """Map node IDs to cluster roots."""
        self.roots = roots
        self.calls: list[tuple[str, str]] = []

    def run(self, peer: dict, local_node_id: str, command_text: str, payload: str | bytes | None = None) -> dict:
        """Execute a restricted command against the peer's local cluster root."""
        self.calls.append((str(peer["node_id"]), command_text))
        raw = payload if isinstance(payload, bytes) else str(payload or "").encode("utf-8")
        result = run_command(self.roots[str(peer["node_id"])], local_node_id, command_text, raw, allow_join=True)
        if command_text in {"hello", "handshake"}:
            result.pop("cluster_ssh_public_key", None)
            result.pop("cluster_ssh_fingerprint", None)
        return result


class _FailingPeerClient:
    """Peer client that fails if a skipped peer is contacted."""

    def run(self, peer: dict, local_node_id: str, command_text: str, payload: str | bytes | None = None) -> dict:
        """Fail when a test unexpectedly attempts peer transport."""

        raise AssertionError("skipped peer must not be contacted")


class _ParallelPeerClient:
    """Peer client that records concurrent handshake fanout."""

    def __init__(self, remote_vector: dict[str, int]) -> None:
        """Initialize concurrency counters."""

        self.remote_vector = remote_vector
        self.active = 0
        self.max_active = 0
        self.lock = threading.Lock()

    def run(self, peer: dict, local_node_id: str, command_text: str, payload: str | bytes | None = None) -> dict:
        """Delay handshakes so tests can observe parallel execution."""

        if command_text != "handshake":
            raise AssertionError(f"unexpected command: {command_text}")
        with self.lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        try:
            time.sleep(0.05)
        finally:
            with self.lock:
                self.active -= 1
        return {
            "ok": True,
            "cluster_id": CLUSTER_ID,
            "node_id": str(peer["node_id"]),
            "role": str(peer.get("role") or "vps"),
            "remote_node": local_node_id,
            "state_vector": self.remote_vector,
        }


def _write_cluster_blob(cluster_root: Path, blob_hash: str, raw: bytes) -> None:
    """Write one verified config blob into a test cluster root."""
    assert hashlib.sha256(raw).hexdigest() == blob_hash.removeprefix("sha256:")
    digest = blob_hash.removeprefix("sha256:")
    path = cluster_root / "config_blobs" / "sha256" / digest[:2] / f"{digest}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)


def _write_config_blobs_for_instance(cluster_root: Path, instance_dir: Path) -> str:
    """Write config file blobs plus manifest for one test instance."""
    manifest = build_config_manifest(instance_dir)
    files = manifest.get("files") if isinstance(manifest, dict) else {}
    files = files if isinstance(files, dict) else {}
    for filename, meta in files.items():
        sha = str((meta if isinstance(meta, dict) else {}).get("sha256") or "")
        raw = (instance_dir / str(filename)).read_bytes()
        _write_cluster_blob(cluster_root, f"sha256:{sha}", raw)
    manifest_raw = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode("utf-8")
    manifest_hash = compute_config_manifest_hash(manifest)
    _write_cluster_blob(cluster_root, manifest_hash, manifest_raw)
    return manifest_hash


def test_cluster_sync_worker_reports_not_configured(tmp_path: Path) -> None:
    """A runner without cluster identity writes a non-blocking status file."""

    worker = ClusterSyncWorker(tmp_path)

    status = worker.run_once(reason="test")

    assert status["ok"] is False
    assert status["status"] == "not_configured"
    assert (tmp_path / "data" / "cluster" / "sync_status.json").is_file()


def test_cluster_sync_worker_rebuilds_and_writes_status(tmp_path: Path) -> None:
    """A configured node rebuilds local state and records materialization status."""

    cluster_root = default_cluster_root(tmp_path)
    ensure_local_identity(cluster_root, role="vps", pbname="runner-a", cluster_id=CLUSTER_ID, node_id=NODE_ID)
    append_operation(cluster_root, "ADD_NODE", {"node_id": NODE_ID, "role": "vps", "pbname": "runner-a"})
    worker = ClusterSyncWorker(tmp_path)

    status = worker.run_once(reason="test")

    assert status["ok"] is True
    assert status["status"] == "local_reconciled"
    assert status["cluster_id"] == CLUSTER_ID
    assert status["node_id"] == NODE_ID
    assert status["state_vector"] == {NODE_ID: 1}
    assert status["v7_materialization"]["counts"]["skip"] == 0
    assert (cluster_root / "desired_state.json").is_file()
    assert (cluster_root / "sync_status.json").is_file()


def test_cluster_retention_summary_requires_every_automatic_peer_report() -> None:
    """Cluster retention is healthy only when every active replica reports healthy cleanup."""

    cleanup = {
        "oplog": {"status": "ready", "checkpoint_id": HASH_A, "blockers": []},
        "blobs": {"status": "complete", "checkpoint_id": HASH_A, "blockers": []},
    }
    nodes = {
        NODE_ID: {"pbname": "master-a", "enabled": True, "state_replica": True},
        NODE_B: {"pbname": "vps-b", "enabled": True, "state_replica": True},
    }
    peer = {
        "node_id": NODE_B,
        "pbname": "vps-b",
        "ok": True,
        "last_seen": 123,
        "retention_cleanup": cleanup,
    }

    healthy = cluster_sync_worker._cluster_retention_summary(
        nodes,
        NODE_ID,
        {"mode": "oplog"},
        {"checkpoint_id": HASH_A},
        cleanup,
        [peer],
    )
    blocked_cleanup = {
        "oplog": cleanup["oplog"],
        "blobs": {
            "status": "blocked",
            "checkpoint_id": HASH_A,
            "blockers": ["blob_mark_failed:reachable blob is missing"],
        },
    }
    blocked = cluster_sync_worker._cluster_retention_summary(
        nodes,
        NODE_ID,
        {"mode": "oplog"},
        {"checkpoint_id": HASH_A},
        cleanup,
        [{**peer, "retention_cleanup": blocked_cleanup}],
    )

    assert healthy["status"] == "healthy"
    assert healthy["nodes_healthy"] == 2
    assert blocked["status"] == "blocked"
    assert blocked["nodes_healthy"] == 1
    assert blocked["nodes"][1]["blockers"] == ["blob_mark_failed:reachable blob is missing"]


@pytest.mark.parametrize("oplog_status", ["ready", "complete"])
def test_legacy_oplog_policy_runs_blob_gc_after_healthy_oplog_evaluation(
    monkeypatch,
    tmp_path: Path,
    oplog_status: str,
) -> None:
    """Every healthy oplog result proceeds to automatic blob GC."""

    cluster_root = default_cluster_root(tmp_path)
    ensure_local_identity(cluster_root, role="master", pbname="master-a", cluster_id=CLUSTER_ID, node_id=NODE_ID)
    append_operation(cluster_root, "ADD_NODE", {"node_id": NODE_ID, "role": "master", "pbname": "master-a"})
    append_operation(
        cluster_root,
        "SET_RETENTION_POLICY",
        {
            "generation": 1,
            "parent_generation": 0,
            "mode": "oplog",
            "history_days": 14,
        },
    )
    materialized = rebuild_materialized_state(cluster_root, write=False)
    calls: list[str] = []
    monkeypatch.setattr(
        cluster_sync_worker,
        "checkpoint_status",
        lambda _root: {
            "active": True,
            "checkpoint_baseline": dict(materialized["state_vector"]),
        },
    )
    monkeypatch.setattr(cluster_sync_worker, "read_retention_report", lambda _root: None)
    monkeypatch.setattr(
        cluster_sync_worker,
        "prune_operation_history",
        lambda _root, *, now: calls.append("oplog") or {"status": oplog_status},
    )
    monkeypatch.setattr(
        cluster_sync_worker,
        "garbage_collect_blobs",
        lambda _root, *, now: calls.append("blobs") or {"status": "complete"},
    )

    result = ClusterSyncWorker(tmp_path)._maintain_history(
        read_local_identity(cluster_root),
        materialized,
        [],
        reason="manual",
    )

    assert result["status"] == "cleanup_evaluated"
    assert calls == ["oplog", "blobs"]


def test_cluster_sync_worker_does_not_apply_current_v7_materialization(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """A converged turn previews V7 state without entering the replaying write path."""

    cluster_root = default_cluster_root(tmp_path)
    ensure_local_identity(cluster_root, role="vps", pbname="runner-a", cluster_id=CLUSTER_ID, node_id=NODE_ID)
    append_operation(cluster_root, "ADD_NODE", {"node_id": NODE_ID, "role": "vps"})
    writes: list[bool] = []

    def materialize(_root: Path, *, write: bool) -> dict:
        writes.append(write)
        return {"ok": True, "can_apply": False, "counts": {"skip": 1}}

    monkeypatch.setattr(cluster_sync_worker, "_materialize_v7_configs", materialize)

    status = ClusterSyncWorker(tmp_path).run_once(reason="periodic")

    assert writes == [False]
    assert status["v7_materialization"]["counts"] == {"skip": 1}


def test_cluster_sync_worker_applies_changed_v7_materialization(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """A changed V7 preview still enters the write path exactly once."""

    cluster_root = default_cluster_root(tmp_path)
    ensure_local_identity(cluster_root, role="vps", pbname="runner-a", cluster_id=CLUSTER_ID, node_id=NODE_ID)
    append_operation(cluster_root, "ADD_NODE", {"node_id": NODE_ID, "role": "vps"})
    writes: list[bool] = []

    def materialize(_root: Path, *, write: bool) -> dict:
        writes.append(write)
        return {
            "ok": True,
            "can_apply": not write,
            "counts": {"update": 0 if write else 1, "written_instances": 1 if write else 0},
        }

    monkeypatch.setattr(cluster_sync_worker, "_materialize_v7_configs", materialize)

    status = ClusterSyncWorker(tmp_path).run_once(reason="event")

    assert writes == [False, True]
    assert status["v7_materialization"]["counts"]["written_instances"] == 1


def test_cluster_sync_worker_advances_migration_before_and_after_peer_sync(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Every local cycle advances outbound candidates and post-pull acceptance synchronously."""

    cluster_root = default_cluster_root(tmp_path)
    ensure_local_identity(cluster_root, role="vps", pbname="runner-a", cluster_id=CLUSTER_ID, node_id=NODE_ID)
    append_operation(cluster_root, "ADD_NODE", {"node_id": NODE_ID, "role": "vps"})
    calls: list[int] = []
    monkeypatch.setattr(
        cluster_sync_worker,
        "advance_local_credential_migration",
        lambda _root, *, max_items, scan_allowed: calls.append((max_items, scan_allowed)) or {"status": "advanced"},
    )

    status = ClusterSyncWorker(tmp_path).run_once(reason="test")

    assert calls == [(8, True), (8, False)]
    assert status["credential_migration_advance"] == {
        "pre_sync": {"status": "advanced"},
        "post_sync": {"status": "advanced"},
    }


def test_cluster_sync_worker_reports_bounded_migration_error_detail(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Migration status preserves a bounded diagnostic without stopping the turn."""

    cluster_root = default_cluster_root(tmp_path)
    ensure_local_identity(cluster_root, role="vps", pbname="runner-a", cluster_id=CLUSTER_ID, node_id=NODE_ID)
    append_operation(cluster_root, "ADD_NODE", {"node_id": NODE_ID, "role": "vps"})
    detail = "candidate publication rejected " + ("x" * 300)

    def fail_advance(*_args, **_kwargs):
        raise RuntimeError(detail)

    monkeypatch.setattr(cluster_sync_worker, "advance_local_credential_migration", fail_advance)

    status = ClusterSyncWorker(tmp_path).run_once(reason="test")

    expected = {
        "status": "error",
        "error": "RuntimeError",
        "detail": detail[:240],
    }
    assert status["credential_migration_advance"] == {
        "pre_sync": expected,
        "post_sync": expected,
    }


def test_cluster_sync_worker_skips_managed_scan_during_periodic_cycle(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Periodic maintenance never repeats the expensive managed credential scan."""

    cluster_root = default_cluster_root(tmp_path)
    ensure_local_identity(cluster_root, role="vps", pbname="runner-a", cluster_id=CLUSTER_ID, node_id=NODE_ID)
    append_operation(cluster_root, "ADD_NODE", {"node_id": NODE_ID, "role": "vps"})
    calls: list[bool] = []
    monkeypatch.setattr(
        cluster_sync_worker,
        "advance_local_credential_migration",
        lambda _root, *, max_items, scan_allowed: calls.append(scan_allowed) or {"status": "advanced"},
    )

    ClusterSyncWorker(tmp_path).run_once(reason="periodic")

    assert calls == [False, False]


def test_master_cycle_auto_starts_cutover_after_last_v2_peer_sync(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """A master coordinator runs after peer pull so the last v2 update starts cutover."""

    cluster_root = default_cluster_root(tmp_path)
    ensure_local_identity(cluster_root, role="master", pbname="master-a", cluster_id=CLUSTER_ID, node_id=NODE_ID)
    append_operation(cluster_root, "ADD_NODE", {"node_id": NODE_ID, "role": "master"})
    events: list[str] = []
    worker = ClusterSyncWorker(tmp_path)
    monkeypatch.setattr(worker, "_sync_peers", lambda _identity, _state: events.append("sync") or [])
    monkeypatch.setattr(
        cluster_sync_worker,
        "run_credential_migration",
        lambda _root: events.append("coordinator") or {"status": "advancing", "phase": "inventory"},
    )

    status = worker.run_once(reason="test")

    assert events == ["sync", "coordinator"]
    assert status["credential_migration_advance"]["coordinator"] == {
        "status": "advancing",
        "phase": "inventory",
    }


def test_periodic_master_cycle_skips_completed_migration_rescan(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """A completed migration never repeats its full security scan every minute."""

    cluster_root = default_cluster_root(tmp_path)
    ensure_local_identity(cluster_root, role="master", pbname="master-a", cluster_id=CLUSTER_ID, node_id=NODE_ID)
    append_operation(cluster_root, "ADD_NODE", {"node_id": NODE_ID, "role": "master"})
    monkeypatch.setattr(cluster_sync_worker, "credential_migration_is_complete", lambda _root: True)
    monkeypatch.setattr(
        cluster_sync_worker,
        "run_credential_migration",
        lambda _root: (_ for _ in ()).throw(AssertionError("completed migration was rescanned")),
    )

    status = ClusterSyncWorker(tmp_path).run_once(reason="periodic")

    assert status["credential_migration_advance"]["coordinator"] == {
        "status": "complete",
        "phase": "complete",
    }


def test_cluster_sync_worker_consumes_sync_request_trigger(tmp_path: Path) -> None:
    """PBCluster treats a touched sync_request file as one event trigger."""

    cluster_root = default_cluster_root(tmp_path)
    ensure_local_identity(cluster_root, role="vps", pbname="runner-a", cluster_id=CLUSTER_ID, node_id=NODE_ID)
    worker = ClusterSyncWorker(tmp_path)

    assert worker._consume_trigger_change() is False
    worker.trigger_path.parent.mkdir(parents=True, exist_ok=True)
    worker.trigger_path.touch()

    assert worker._consume_trigger_change() is True
    assert worker._consume_trigger_change() is False


def test_cluster_sync_worker_coalesces_triggers_written_during_its_pass(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Operations written by a completed pass do not trigger an immediate retry loop."""

    worker = ClusterSyncWorker(tmp_path)

    def run_once(*, reason: str) -> dict:
        worker.trigger_path.parent.mkdir(parents=True, exist_ok=True)
        worker.trigger_path.touch()
        worker.stop()
        return {"reason": reason}

    monkeypatch.setattr(worker, "run_once", run_once)

    worker.run_forever()

    assert worker._consume_trigger_change() is False


def test_cluster_sync_worker_event_clears_peer_backoff(tmp_path: Path) -> None:
    """Explicit sync events retry peers immediately after repair actions."""

    worker = ClusterSyncWorker(tmp_path)
    worker._peer_backoff[NODE_B] = {"failures": 4, "next_retry": 9999999999, "error": "Permission denied"}

    worker.run_once(reason="event")

    assert worker._peer_backoff == {}


def test_cluster_sync_worker_skips_outbound_only_peer(tmp_path: Path) -> None:
    """Outbound-only peers participate in state but are not contacted over SSH."""

    cluster_root = default_cluster_root(tmp_path)
    ensure_local_identity(cluster_root, role="master", pbname="master-a", cluster_id=CLUSTER_ID, node_id=NODE_ID)
    append_operation(cluster_root, "ADD_NODE", {"node_id": NODE_ID, "role": "master", "pbname": "master-a"}, created_at=100)
    append_operation(
        cluster_root,
        "ADD_NODE",
        {"node_id": NODE_B, "role": "master", "pbname": "master-b", "sync_mode": "outbound_only"},
        created_at=101,
    )
    worker = ClusterSyncWorker(tmp_path, peer_client=_FailingPeerClient())

    status = worker.run_once(reason="test")

    assert status["ok"] is True
    assert status["peers"][0]["status"] == "outbound_only"
    assert status["peers"][0]["ok"] is True


def test_cluster_sync_worker_reports_reachable_peer_without_ssh_host(tmp_path: Path) -> None:
    """Reachable peers without ssh_host are config errors, not hostname fallback targets."""

    cluster_root = default_cluster_root(tmp_path)
    ensure_local_identity(cluster_root, role="master", pbname="master-a", cluster_id=CLUSTER_ID, node_id=NODE_ID)
    append_operation(cluster_root, "ADD_NODE", {"node_id": NODE_ID, "role": "master", "pbname": "master-a"}, created_at=100)
    append_operation(
        cluster_root,
        "ADD_NODE",
        {"node_id": NODE_B, "role": "master", "pbname": "master-b", "sync_mode": "reachable"},
        created_at=101,
    )
    worker = ClusterSyncWorker(tmp_path, peer_client=_FailingPeerClient())

    status = worker.run_once(reason="test")

    assert status["ok"] is True
    assert status["peers_ok"] == 0
    assert status["peers"][0]["status"] == "config_error"
    assert "ssh_host" in status["peers"][0]["reason"]


def test_cluster_sync_worker_syncs_reachable_peers_in_parallel(tmp_path: Path) -> None:
    """PBCluster fans out reachable peer checks concurrently instead of one by one."""

    peer_ids = [_node_id(index) for index in range(12, 20)]
    cluster_root = default_cluster_root(tmp_path)
    ensure_local_identity(cluster_root, role="master", pbname="master-a", cluster_id=CLUSTER_ID, node_id=NODE_ID)
    append_operation(cluster_root, "ADD_NODE", {"node_id": NODE_ID, "role": "master", "pbname": "master-a"}, created_at=100)
    for offset, peer_id in enumerate(peer_ids, start=1):
        append_operation(
            cluster_root,
            "ADD_NODE",
            {"node_id": peer_id, "role": "vps", "pbname": f"runner-{offset}", "sync_mode": "reachable", "ssh_host": f"runner-{offset}"},
            created_at=100 + offset,
        )
    client = _ParallelPeerClient({NODE_ID: 1, **{peer_id: 1 for peer_id in peer_ids}})
    worker = ClusterSyncWorker(tmp_path, peer_client=client, peer_workers=len(peer_ids))

    status = worker.run_once(reason="test")

    assert status["ok"] is True
    assert status["peers_ok"] == len(peer_ids)
    assert client.max_active > 1
    assert {item["status"] for item in status["peers"]} == {"synced"}


def test_cluster_sync_worker_does_not_load_oplog_for_converged_peer(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """A matching remote state vector avoids loading and validating local operations."""

    cluster_root = default_cluster_root(tmp_path)
    ensure_local_identity(cluster_root, role="master", pbname="master-a", cluster_id=CLUSTER_ID, node_id=NODE_ID)
    append_operation(cluster_root, "ADD_NODE", {"node_id": NODE_ID, "role": "master", "pbname": "master-a"})
    append_operation(
        cluster_root,
        "ADD_NODE",
        {"node_id": NODE_B, "role": "vps", "pbname": "runner-b", "sync_mode": "reachable", "ssh_host": "runner-b"},
    )
    materialized = rebuild_materialized_state(cluster_root)
    client = _ParallelPeerClient(materialized["state_vector"])
    worker = ClusterSyncWorker(tmp_path, peer_client=client)
    monkeypatch.setattr(
        cluster_sync_worker,
        "load_operations",
        lambda *_args, **_kwargs: pytest.fail("converged peer must not load the oplog"),
    )

    results = worker._sync_peers(read_local_identity(cluster_root), materialized)

    assert len(results) == 1
    assert results[0]["status"] == "synced"


def test_cluster_sync_worker_skips_vps_peer_without_explicit_topology(tmp_path: Path) -> None:
    """VPS nodes do not initiate SSH fanout unless sync_peers explicitly allows it."""

    cluster_root = default_cluster_root(tmp_path)
    ensure_local_identity(cluster_root, role="vps", pbname="runner-a", cluster_id=CLUSTER_ID, node_id=NODE_ID)
    append_operation(cluster_root, "ADD_NODE", {"node_id": NODE_ID, "role": "vps", "pbname": "runner-a"}, created_at=100)
    append_operation(
        cluster_root,
        "ADD_NODE",
        {"node_id": NODE_B, "role": "vps", "pbname": "runner-b", "sync_mode": "reachable", "ssh_host": "runner-b"},
        created_at=101,
    )
    worker = ClusterSyncWorker(tmp_path, peer_client=_FailingPeerClient())

    status = worker.run_once(reason="test")

    assert status["ok"] is True
    assert status["peers"][0]["status"] == "topology_skipped"


def test_secondary_master_delegates_peer_fanout_to_coordinator() -> None:
    """Only the deterministic coordinator contacts VPS peers and other secondaries."""

    coordinator = {"node_id": NODE_ID, "role": "master", "enabled": True}
    secondary = {
        "node_id": NODE_B,
        "role": "master",
        "enabled": True,
        "sync_peers": [NODE_ID, NODE_C, "runner"],
    }
    other_secondary = {"node_id": NODE_C, "role": "master", "enabled": True}
    runner = {"node_id": "runner", "role": "vps", "enabled": True}
    nodes = {NODE_ID: coordinator, NODE_B: secondary, NODE_C: other_secondary, "runner": runner}

    coordinator_allowed, _reason = cluster_sync_worker._peer_topology_allows(
        NODE_B, secondary, NODE_ID, coordinator, nodes,
    )
    runner_allowed, runner_reason = cluster_sync_worker._peer_topology_allows(
        NODE_B, secondary, "runner", runner, nodes,
    )
    secondary_allowed, _reason = cluster_sync_worker._peer_topology_allows(
        NODE_B, secondary, NODE_C, other_secondary, nodes,
    )

    assert coordinator_allowed is True
    assert runner_allowed is False
    assert secondary_allowed is False
    assert runner_reason == "secondary master fanout is delegated to coordinator"


def test_mailbox_sync_ignores_message_removed_after_index(tmp_path: Path) -> None:
    """A mailbox expiry between index and fetch does not fail the whole peer sync."""

    class ExpiredMailboxPeerClient:
        """Expose one stale index entry and reject its subsequent fetch."""

        def run(self, _peer, _local_node_id, command_text, payload=None) -> dict:
            if command_text == "get-mailbox-index":
                return {"messages": [{"message_id": "expired-message"}]}
            if command_text == "get-mailbox-message expired-message":
                raise RuntimeError('{"error": "mailbox message not found", "ok": false}')
            raise AssertionError(f"unexpected command: {command_text}")

    worker = ClusterSyncWorker(tmp_path, peer_client=ExpiredMailboxPeerClient())

    result = worker._sync_mailbox({"node_id": NODE_B}, NODE_ID)

    assert result == {"supported": True, "pulled": 0, "pushed": 0, "acked": 0}


def test_ssh_cluster_peer_client_uses_dedicated_key_and_forced_command(monkeypatch, tmp_path: Path) -> None:
    """PBCluster SSH calls use the dedicated key and send only the wrapper verb."""

    private_key = tmp_path / "cluster_key"
    private_key.write_text("private", encoding="utf-8")
    monkeypatch.setattr(
        "master.cluster_sync_worker.ensure_cluster_ssh_key",
        lambda cluster_root: {"private_key_path": str(private_key)},
    )
    client = SshClusterPeerClient(cluster_root=tmp_path / "data" / "cluster")

    command = client._ssh_command(
        {"node_id": NODE_B, "ssh_host": "203.0.113.10", "ssh_user": "bot", "ssh_port": 2222},
        NODE_ID,
        "hello",
    )

    assert "-i" in command
    assert str(private_key) in command
    assert "IdentitiesOnly=yes" in command
    assert not any(str(item).startswith("UserKnownHostsFile=") for item in command)
    assert "StrictHostKeyChecking=yes" in command
    assert command[-1] == "hello"
    assert "cluster_sync_command.py" not in command[-1]


def test_cluster_sync_worker_pushes_ops_blobs_and_remote_materializes(tmp_path: Path) -> None:
    """PBCluster pushes local operations and config blobs to reachable peers."""

    root_a = default_cluster_root(tmp_path / "node-a")
    root_b = default_cluster_root(tmp_path / "node-b")
    ensure_local_identity(root_a, role="master", pbname="master-a", cluster_id=CLUSTER_ID, node_id=NODE_ID)
    ensure_local_identity(root_b, role="master", pbname="master-b", cluster_id=CLUSTER_ID, node_id=NODE_B)

    master_membership = append_operation(
        root_a,
        "ADD_NODE",
        {"node_id": NODE_ID, "role": "master", "pbname": "master-a", "ssh_host": "master-a"},
        created_at=100,
    )
    runner_membership = append_operation(
        root_b,
        "ADD_NODE",
        {"node_id": NODE_B, "role": "master", "pbname": "master-b", "ssh_host": "master-b"},
        created_at=101,
    )
    write_operation(root_a, runner_membership, allow_legacy_membership=True)
    write_operation(root_b, master_membership, allow_legacy_membership=True)

    instance_dir = tmp_path / "node-a" / "data" / "run_v7" / "bot-a"
    instance_dir.mkdir(parents=True)
    (instance_dir / "config.json").write_text(json.dumps({"pbgui": {"version": 7}, "live": {"user": "bot-a"}}), encoding="utf-8")
    manifest_hash = _write_config_blobs_for_instance(root_b, instance_dir)
    append_operation(
        root_a,
        "UPSERT_CONFIG",
        {
            "instance": "bot-a",
            "version": "7",
            "assigned_host": NODE_B,
            "desired_state": "running",
            "config_manifest_hash": manifest_hash,
        },
        created_at=102,
    )
    client = _LocalPeerClient({NODE_ID: root_a, NODE_B: root_b})
    worker = ClusterSyncWorker(tmp_path / "node-a", peer_client=client)

    status = worker.run_once(reason="test")

    assert status["ok"] is True
    assert status["pushed_ops"] >= 1
    assert status["peers"][0]["status"] == "changed"
    commands = [command for _, command in client.calls]
    assert "handshake" in commands
    assert "apply-bundle" in commands
    assert "put-ops" not in commands
    assert "rebuild" not in commands
    assert "materialize-v7" not in commands
    convergence_status = worker.run_once(reason="test")
    assert convergence_status["peers"][0]["ok"], convergence_status["peers"][0]["reason"]
    manifest_digest = manifest_hash.removeprefix("sha256:")
    remote_manifest = root_b / "config_blobs" / "sha256" / manifest_digest[:2] / f"{manifest_digest}.json"
    remote_manifest.unlink()
    client.calls.clear()

    repair_status = worker.run_once(reason="test")

    assert repair_status["peers"][0]["ok"], repair_status["peers"][0]["reason"]
    assert repair_status["peers"][0]["remote_vector"] == repair_status["peers"][0]["local_vector"]
    assert repair_status["peers"][0]["pushed_ops"] == 0
    assert repair_status["peers"][0]["blob_coverage_repair_owner"] is True
    assert repair_status["peers"][0]["repaired_config_blobs"] == 1
    assert remote_manifest.is_file()
    assert "missing-blobs" in [command for _, command in client.calls]
    remote_config = tmp_path / "node-b" / "data" / "run_v7" / "bot-a" / "config.json"
    assert not remote_config.exists()
    local_manifest = root_a / "config_blobs" / "sha256" / manifest_digest[:2] / f"{manifest_digest}.json"
    local_manifest.unlink()
    remote_worker = ClusterSyncWorker(tmp_path / "node-b", peer_client=client)
    remote_status = remote_worker.run_once(reason="test")
    assert remote_status["ok"] is True
    assert remote_status["peers"][0]["repaired_config_blobs"] == 1
    assert local_manifest.is_file()
    assert json.loads(remote_config.read_text(encoding="utf-8"))["live"]["user"] == "bot-a"
    assert json.loads((root_b / "desired_state.json").read_text(encoding="utf-8"))["instances"]["bot-a"]["assigned_host"] == NODE_B


def test_cluster_sync_worker_limits_operation_bundle_size() -> None:
    """One peer pass selects a bounded operation batch and reports the remainder."""

    operations = [{"seq": index} for index in range(25)]

    batch, remaining = cluster_sync_worker._bounded_operation_batch(operations)

    assert batch == operations[:cluster_sync_worker.APPLY_BUNDLE_MAX_OPERATIONS]
    assert remaining == 9


def test_blob_coverage_repairs_available_blobs_when_another_is_missing_locally(tmp_path: Path) -> None:
    """One unavailable donor blob does not block other coverage repairs."""

    class CoveragePeerClient:
        """Force probe splitting and report every requested config blob missing."""

        def __init__(self) -> None:
            self.uploads = 0

        def run(self, _peer, _local_node_id, command_text, payload=None) -> dict:
            if command_text == "missing-blobs":
                requested = json.loads(str(payload))["config"]
                if len(requested) > 1:
                    raise RuntimeError("missing-blobs verification budget exceeded")
                return {"ok": True, "missing": {"config": requested, "secret": [], "sealed": []}}
            if command_text == "put-blobs":
                self.uploads += 1
                return {"ok": True, "count": 1}
            raise AssertionError(f"unexpected command: {command_text}")

    root = default_cluster_root(tmp_path)
    available_raw = b'{"available":true}'
    available_hash = "sha256:" + hashlib.sha256(available_raw).hexdigest()
    unavailable_hash = "sha256:" + "f" * 64
    _write_cluster_blob(root, available_hash, available_raw)
    client = CoveragePeerClient()
    worker = ClusterSyncWorker(tmp_path, peer_client=client)

    repaired = worker._repair_remote_blob_coverage(
        {"node_id": NODE_B, "pbname": "master-b"},
        NODE_ID,
        {"config": sorted([available_hash, unavailable_hash]), "secret": [], "sealed": []},
    )

    assert repaired == (1, 0, 0, 0)
    assert client.uploads == 1


def test_blob_coverage_prioritizes_recovering_local_blob_from_peer(tmp_path: Path) -> None:
    """A capable peer heals a locally missing hash before normal cursor scanning."""

    class CoveragePeerClient:
        """Expose every probed blob and return the missing local target."""

        def __init__(self, target_hash: str, target_raw: bytes) -> None:
            self.target_hash = target_hash
            self.target_raw = target_raw
            self.commands: list[str] = []
            self.probes: list[list[str]] = []

        def run(self, _peer, _local_node_id, command_text, payload=None) -> dict:
            self.commands.append(command_text)
            if command_text == "missing-blobs":
                self.probes.append(list(json.loads(str(payload))["config"]))
                return {"ok": True, "missing": {"config": [], "secret": [], "sealed": []}}
            if command_text == f"get-blob {self.target_hash}":
                return {
                    "ok": True,
                    "hash": self.target_hash,
                    "size": len(self.target_raw),
                    "content_b64": base64.b64encode(self.target_raw).decode("ascii"),
                }
            raise AssertionError(f"unexpected command: {command_text}")

    root = default_cluster_root(tmp_path)
    hashes = []
    for index in range(cluster_sync_worker.BLOB_COVERAGE_MAX_HASHES_PER_PEER_PASS + 2):
        raw = json.dumps({"available": index}, separators=(",", ":")).encode("utf-8")
        blob_hash = "sha256:" + hashlib.sha256(raw).hexdigest()
        _write_cluster_blob(root, blob_hash, raw)
        hashes.append(blob_hash)
    target_raw = b'{"recovered":true}'
    target_hash = "sha256:" + hashlib.sha256(target_raw).hexdigest()
    hashes.append(target_hash)
    client = CoveragePeerClient(target_hash, target_raw)
    worker = ClusterSyncWorker(tmp_path, peer_client=client)
    target_index = sorted(hashes).index(target_hash)
    worker._blob_coverage_cursors[NODE_B] = (target_index + 1) % len(hashes)
    worker._blob_recovery_cursors[NODE_B] = target_index

    repaired = worker._repair_remote_blob_coverage(
        {"node_id": NODE_B, "pbname": "master-b"},
        NODE_ID,
        {"config": sorted(hashes), "secret": [], "sealed": []},
    )

    digest = target_hash.removeprefix("sha256:")
    recovered_path = root / "config_blobs" / "sha256" / digest[:2] / f"{digest}.json"
    assert repaired == (0, 0, 0, 1)
    assert recovered_path.read_bytes() == target_raw
    assert f"get-blob {target_hash}" in client.commands
    assert target_hash in client.probes[0]
    assert target_hash not in client.probes[-1]


def test_blob_coverage_rejects_mismatched_peer_blob_response(tmp_path: Path) -> None:
    """A peer cannot substitute another content-addressed blob during recovery."""

    class MismatchedPeerClient:
        """Return valid bytes under a hash other than the requested hash."""

        def run(self, _peer, _local_node_id, _command_text, payload=None) -> dict:
            raw = b'{"wrong":true}'
            return {
                "ok": True,
                "hash": "sha256:" + hashlib.sha256(raw).hexdigest(),
                "size": len(raw),
                "content_b64": base64.b64encode(raw).decode("ascii"),
            }

    root = default_cluster_root(tmp_path)
    expected_hash = "sha256:" + "f" * 64
    worker = ClusterSyncWorker(tmp_path, peer_client=MismatchedPeerClient())

    with pytest.raises(ClusterSyncWorkerError, match="different blob hash"):
        worker._ensure_remote_blob(
            {"node_id": NODE_B},
            NODE_ID,
            root / "config_blobs",
            expected_hash,
            secret=False,
        )


def test_local_blob_exists_rejects_symlink(tmp_path: Path) -> None:
    """Content-addressed blob reads never follow a symlink outside the store."""

    raw = b'{"external":true}'
    blob_hash = "sha256:" + hashlib.sha256(raw).hexdigest()
    digest = blob_hash.removeprefix("sha256:")
    external = tmp_path / "external.json"
    external.write_bytes(raw)
    base_dir = tmp_path / "config_blobs"
    path = base_dir / "sha256" / digest[:2] / f"{digest}.json"
    path.parent.mkdir(parents=True)
    path.symlink_to(external)

    assert cluster_sync_worker._local_blob_exists(base_dir, blob_hash) is False

    parent_base = tmp_path / "parent_symlink_store"
    parent_base.mkdir()
    external_store = tmp_path / "external_store"
    external_path = external_store / digest[:2] / f"{digest}.json"
    external_path.parent.mkdir(parents=True)
    external_path.write_bytes(raw)
    (parent_base / "sha256").symlink_to(external_store)

    assert cluster_sync_worker._local_blob_exists(parent_base, blob_hash) is False


def test_remote_blob_recovery_defers_when_pass_budget_is_exhausted(tmp_path: Path) -> None:
    """A valid peer blob outside the remaining budget is deferred without writing."""

    raw = b'{"deferred":true}'
    blob_hash = "sha256:" + hashlib.sha256(raw).hexdigest()

    class ValidPeerClient:
        """Return one valid blob larger than the supplied remaining budget."""

        def run(self, _peer, _local_node_id, _command_text, payload=None) -> dict:
            return {
                "ok": True,
                "hash": blob_hash,
                "size": len(raw),
                "content_b64": base64.b64encode(raw).decode("ascii"),
            }

    root = default_cluster_root(tmp_path)
    worker = ClusterSyncWorker(tmp_path, peer_client=ValidPeerClient())

    with pytest.raises(ClusterBlobBudgetExceeded, match="coverage pass budget"):
        worker._ensure_remote_blob(
            {"node_id": NODE_B},
            NODE_ID,
            root / "config_blobs",
            blob_hash,
            secret=False,
            remaining_bytes=len(raw) - 1,
        )


def test_blob_coverage_rotates_within_per_peer_repair_budget(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Bounded coverage passes resume at the first blob deferred by the byte cap."""

    class CoveragePeerClient:
        """Report every requested config blob missing and record uploads."""

        def __init__(self) -> None:
            self.uploaded: list[str] = []

        def run(self, _peer, _local_node_id, command_text, payload=None) -> dict:
            request = json.loads(str(payload))
            if command_text == "missing-blobs":
                return {"ok": True, "missing": {"config": request["config"], "secret": [], "sealed": []}}
            if command_text == "put-blobs":
                self.uploaded.extend(str(item["hash"]) for item in request["blobs"])
                return {"ok": True, "count": len(request["blobs"])}
            raise AssertionError(f"unexpected command: {command_text}")

    monkeypatch.setattr(cluster_sync_worker, "BLOB_COVERAGE_MAX_HASHES_PER_PEER_PASS", 2)
    root = default_cluster_root(tmp_path)
    hashes = []
    for index in range(3):
        raw = json.dumps({"item": index}, separators=(",", ":")).encode("utf-8")
        blob_hash = "sha256:" + hashlib.sha256(raw).hexdigest()
        _write_cluster_blob(root, blob_hash, raw)
        hashes.append(blob_hash)
    hashes.sort()
    first_size = (root / "config_blobs" / "sha256" / hashes[0][7:9] / f"{hashes[0][7:]}.json").stat().st_size
    monkeypatch.setattr(cluster_sync_worker, "BLOB_COVERAGE_MAX_BYTES_PER_PEER_PASS", first_size)
    client = CoveragePeerClient()
    worker = ClusterSyncWorker(tmp_path, peer_client=client)
    peer = {"node_id": NODE_B, "pbname": "master-b"}
    inventory = {"config": hashes, "secret": [], "sealed": []}

    first = worker._repair_remote_blob_coverage(peer, NODE_ID, inventory)
    second = worker._repair_remote_blob_coverage(peer, NODE_ID, inventory)
    third = worker._repair_remote_blob_coverage(peer, NODE_ID, inventory)

    assert first == (1, 0, 0, 0)
    assert second == (1, 0, 0, 0)
    assert third == (1, 0, 0, 0)
    assert client.uploaded == hashes


def test_cluster_sync_worker_pulls_ops_and_blobs_from_peer(tmp_path: Path) -> None:
    """PBCluster pulls missing peer operations and required blobs for local materialization."""

    root_a = default_cluster_root(tmp_path / "node-a")
    root_b = default_cluster_root(tmp_path / "node-b")
    ensure_local_identity(root_a, role="master", pbname="master-a", cluster_id=CLUSTER_ID, node_id=NODE_ID)
    ensure_local_identity(root_b, role="vps", pbname="runner-b", cluster_id=CLUSTER_ID, node_id=NODE_B)
    append_operation(root_a, "ADD_NODE", {"node_id": NODE_ID, "role": "master", "pbname": "master-a"}, created_at=100)
    append_operation(root_a, "ADD_NODE", {"node_id": NODE_B, "role": "vps", "pbname": "runner-b", "ssh_host": "runner-b"}, created_at=101)
    append_operation(root_b, "ADD_NODE", {"node_id": NODE_ID, "role": "master", "pbname": "master-a", "ssh_host": "master-a"}, created_at=100)
    append_operation(root_b, "ADD_NODE", {"node_id": NODE_B, "role": "vps", "pbname": "runner-b"}, created_at=101)

    instance_dir = tmp_path / "node-b" / "data" / "run_v7" / "bot-b"
    instance_dir.mkdir(parents=True)
    (instance_dir / "config.json").write_text(json.dumps({"pbgui": {"version": 9}, "live": {"user": "bot-b"}}), encoding="utf-8")
    manifest_hash = _write_config_blobs_for_instance(root_b, instance_dir)
    append_operation(
        root_b,
        "UPSERT_CONFIG",
        {
            "instance": "bot-b",
            "version": "9",
            "assigned_host": NODE_ID,
            "desired_state": "running",
            "config_manifest_hash": manifest_hash,
        },
        created_at=102,
    )
    worker = ClusterSyncWorker(
        tmp_path / "node-a",
        peer_client=_LocalPeerClient({NODE_ID: root_a, NODE_B: root_b}),
    )
    status = worker.run_once(reason="test")

    assert status["ok"] is True
    assert status["pulled_ops"] >= 1
    local_config = tmp_path / "node-a" / "data" / "run_v7" / "bot-b" / "config.json"
    assert json.loads(local_config.read_text(encoding="utf-8"))["live"]["user"] == "bot-b"
    assert json.loads((root_a / "desired_state.json").read_text(encoding="utf-8"))["instances"]["bot-b"]["assigned_host"] == NODE_ID


def test_cluster_sync_worker_repairs_actor_sequence_gap(tmp_path: Path) -> None:
    """A remote contiguous vector causes a local internal sequence gap to be fetched."""

    root_a = default_cluster_root(tmp_path / "node-a")
    root_b = default_cluster_root(tmp_path / "node-b")
    ensure_local_identity(root_a, role="master", pbname="master-a", cluster_id=CLUSTER_ID, node_id=NODE_ID)
    ensure_local_identity(root_b, role="vps", pbname="runner-b", cluster_id=CLUSTER_ID, node_id=NODE_B)
    append_operation(root_a, "ADD_NODE", {"node_id": NODE_ID, "role": "master", "pbname": "master-a"}, created_at=100)
    append_operation(root_a, "ADD_NODE", {"node_id": NODE_B, "role": "vps", "pbname": "runner-b", "ssh_host": "runner-b"}, created_at=101)
    append_operation(root_b, "ADD_NODE", {"node_id": NODE_ID, "role": "master", "pbname": "master-a", "ssh_host": "master-a"}, created_at=100)
    append_operation(root_b, "ADD_NODE", {"node_id": NODE_B, "role": "vps", "pbname": "runner-b"}, created_at=101)

    def operation(seq: int) -> dict:
        """Build one deterministic non-membership operation for the gap actor."""
        return {
            "schema_version": 1,
            "cluster_id": CLUSTER_ID,
            "op_id": f"{NODE_C}:{seq:08d}",
            "actor": NODE_C,
            "seq": seq,
            "op": "DELETE_INSTANCE",
            "created_at": 200 + seq,
            "instance": f"relay-{seq}",
            "version": str(seq),
        }

    write_operation(root_a, operation(1))
    write_operation(root_a, operation(3))
    for seq in (1, 2, 3):
        write_operation(root_b, operation(seq))
    client = _LocalPeerClient({NODE_ID: root_a, NODE_B: root_b})

    status = ClusterSyncWorker(tmp_path / "node-a", peer_client=client).run_once(reason="test")

    assert status["ok"] is True
    assert status["state_vector"][NODE_C] == 3
    assert any(command == f"get-ops {NODE_C} 2 3" for _, command in client.calls)
