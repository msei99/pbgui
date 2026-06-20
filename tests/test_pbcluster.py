"""Tests for the lightweight PBCluster daemon worker."""

import json
import hashlib
import threading
import time
from pathlib import Path

from cluster_sync_command import run_command
from master.cluster_state import (
    append_operation,
    build_config_manifest,
    compute_config_manifest_hash,
    default_cluster_root,
    ensure_local_identity,
)
from master.cluster_sync_worker import ClusterSyncWorker, SshClusterPeerClient


CLUSTER_ID = "pbgui-cluster-00000000-0000-4000-8000-000000000010"
NODE_ID = "pbgui-node-00000000-0000-4000-8000-000000000010"
NODE_B = "pbgui-node-00000000-0000-4000-8000-000000000011"


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
        return run_command(self.roots[str(peer["node_id"])], local_node_id, command_text, raw, allow_join=True)


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
    client = _ParallelPeerClient({NODE_ID: len(peer_ids) + 1})
    worker = ClusterSyncWorker(tmp_path, peer_client=client, peer_workers=len(peer_ids))

    status = worker.run_once(reason="test")

    assert status["ok"] is True
    assert status["peers_ok"] == len(peer_ids)
    assert client.max_active > 1
    assert {item["status"] for item in status["peers"]} == {"synced"}


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
    assert f"UserKnownHostsFile={private_key.with_name('known_hosts')}" in command
    assert "StrictHostKeyChecking=accept-new" in command
    assert command[-1] == "hello"
    assert "cluster_sync_command.py" not in command[-1]


def test_cluster_sync_worker_pushes_ops_blobs_and_remote_materializes(tmp_path: Path) -> None:
    """PBCluster pushes local operations and config blobs to reachable peers."""

    root_a = default_cluster_root(tmp_path / "node-a")
    root_b = default_cluster_root(tmp_path / "node-b")
    ensure_local_identity(root_a, role="master", pbname="master-a", cluster_id=CLUSTER_ID, node_id=NODE_ID)
    ensure_local_identity(root_b, role="vps", pbname="runner-b", cluster_id=CLUSTER_ID, node_id=NODE_B)

    append_operation(root_a, "ADD_NODE", {"node_id": NODE_ID, "role": "master", "pbname": "master-a"}, created_at=100)
    append_operation(root_a, "ADD_NODE", {"node_id": NODE_B, "role": "vps", "pbname": "runner-b", "ssh_host": "runner-b"}, created_at=101)
    append_operation(root_b, "ADD_NODE", {"node_id": NODE_ID, "role": "master", "pbname": "master-a", "ssh_host": "master-a"}, created_at=100)
    append_operation(root_b, "ADD_NODE", {"node_id": NODE_B, "role": "vps", "pbname": "runner-b"}, created_at=101)

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
    assert status["pushed_ops"] >= 3
    assert status["peers"][0]["status"] == "changed"
    commands = [command for _, command in client.calls]
    assert "handshake" in commands
    assert "apply-bundle" in commands
    assert "put-ops" not in commands
    assert "rebuild" not in commands
    assert "materialize-v7" not in commands
    remote_config = tmp_path / "node-b" / "data" / "run_v7" / "bot-a" / "config.json"
    assert json.loads(remote_config.read_text(encoding="utf-8"))["live"]["user"] == "bot-a"
    assert json.loads((root_b / "desired_state.json").read_text(encoding="utf-8"))["instances"]["bot-a"]["assigned_host"] == NODE_B


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
