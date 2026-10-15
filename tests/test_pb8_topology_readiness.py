"""PB8 publication readiness across intentionally restricted Cluster topologies."""

import json
import time

import pytest

from api import v8_instances
from master import cluster_sync_worker as module
from master.cluster_state import PB8_OPERATION_CAPABILITY


CLUSTER = "pbgui-cluster-00000000-0000-4000-8000-000000000001"
COORDINATOR = "pbgui-node-00000000-0000-4000-8000-000000000001"
LOCAL = "pbgui-node-00000000-0000-4000-8000-000000000002"
OTHER = "pbgui-node-00000000-0000-4000-8000-000000000003"
TARGET = "pbgui-node-00000000-0000-4000-8000-000000000004"


def _scenario(tmp_path, monkeypatch, capabilities=None, *, local_role="master"):
    """Build manibot02's restricted peer topology without SSH or real state."""
    monkeypatch.setattr(module, "_log", lambda *_args, **_kwargs: None)
    nodes = {
        COORDINATOR: {"node_id": COORDINATOR, "pbname": "magicnucpro", "role": "master"},
        LOCAL: {"node_id": LOCAL, "pbname": "manibot02", "role": local_role, "sync_peers": [TARGET]},
        OTHER: {"node_id": OTHER, "pbname": "magicnuc1", "role": "master"},
        TARGET: {"node_id": TARGET, "pbname": "manibot62", "role": "vps"},
    }
    for node in nodes.values():
        node.update(enabled=True, state_replica=True, sync_mode="reachable", ssh_host=node["pbname"])
    calls = []

    class Client:
        """Reject anything beyond the authenticated metadata probe."""

        def run(self, peer, local_node_id, command):
            """Return identity-bound capability metadata only."""
            assert local_node_id == LOCAL
            assert command == "hello", "Capability probe must not transfer operations/blobs"
            calls.append(peer["node_id"])
            return {"ok": True, "cluster_id": CLUSTER, "node_id": peer["node_id"],
                    "capabilities": capabilities if capabilities is not None else [PB8_OPERATION_CAPABILITY]}

    worker = module.ClusterSyncWorker(tmp_path, peer_client=Client(), peer_workers=2)

    def target_sync(peer, *_args):
        """The directly selected target has already completed its normal handshake."""
        assert peer["node_id"] == TARGET
        return {"node_id": TARGET, "pbname": "manibot62", "ok": True,
                "status": "synced", "pb8_capability": True}

    monkeypatch.setattr(worker, "_sync_peer", target_sync)
    return worker, {"node_id": LOCAL, "cluster_id": CLUSTER}, {"cluster_nodes": {"nodes": nodes}}, calls


def _assert_gate(worker, identity, state, peers, monkeypatch, *, finished_at=None):
    """Feed actual worker results to the unchanged publication safety gate."""
    root = worker.cluster_root
    root.mkdir(parents=True, exist_ok=True)
    (root / "sync_status.json").write_text(json.dumps({
        "finished_at": int(time.time()) if finished_at is None else finished_at,
        "peers": peers,
    }))
    monkeypatch.setattr(v8_instances, "_cluster_root", lambda: root)
    monkeypatch.setattr(v8_instances, "_cluster_nodes", lambda: state["cluster_nodes"]["nodes"])
    v8_instances._ensure_pb8_cluster_rollout_ready(identity)


def test_secondary_master_can_publish_with_topology_skipped_replicas(tmp_path, monkeypatch):
    """manibot02 can publish after real hello evidence, without full peer fanout."""
    worker, identity, state, calls = _scenario(tmp_path, monkeypatch)
    peers = worker._sync_peers(identity, state)
    assert sorted(calls) == [COORDINATOR, OTHER]
    assert state["cluster_nodes"]["nodes"][LOCAL]["sync_peers"] == [TARGET]
    probes = [peer for peer in peers if peer.get("capability_only")]
    assert len(probes) == 2
    assert all(peer["status"] == "topology_skipped" and peer["pb8_capability"] is True for peer in probes)
    assert all(peer["pulled_ops"] == peer["pushed_ops"] == 0 for peer in probes)
    _assert_gate(worker, identity, state, peers, monkeypatch)


@pytest.mark.parametrize("capabilities", [[], ["sealed_credentials_v2"]])
def test_unsupported_replica_still_blocks_publication(tmp_path, monkeypatch, capabilities):
    """Skipping full replication must never bypass an explicit negative handshake."""
    worker, identity, state, _calls = _scenario(tmp_path, monkeypatch, capabilities)
    peers = worker._sync_peers(identity, state)
    with pytest.raises(v8_instances.HTTPException) as exc:
        _assert_gate(worker, identity, state, peers, monkeypatch)
    assert exc.value.status_code == 409
    assert "handshake reports no PB8 support" in exc.value.detail


@pytest.mark.parametrize("change", ["wrong_cluster", "wrong_node", "failed", "missing_capabilities", "unreachable"])
def test_unverified_probe_cannot_unlock_save(tmp_path, monkeypatch, change):
    """Failed, mismatched and incomplete metadata remain fail-closed."""
    worker, identity, state, _calls = _scenario(tmp_path, monkeypatch)
    original = worker.peer_client.run

    def reply(peer, local_node_id, command):
        """Inject one trust/transport failure into otherwise valid hello replies."""
        if change == "unreachable":
            raise OSError("unreachable")
        result = original(peer, local_node_id, command)
        if change == "wrong_cluster":
            result["cluster_id"] = "different"
        elif change == "wrong_node":
            result["node_id"] = LOCAL
        elif change == "failed":
            result["ok"] = False
        else:
            result.pop("capabilities")
        return result

    monkeypatch.setattr(worker.peer_client, "run", reply)
    peers = worker._sync_peers(identity, state)
    with pytest.raises(v8_instances.HTTPException) as exc:
        _assert_gate(worker, identity, state, peers, monkeypatch)
    assert exc.value.status_code == 409


def test_probe_failures_use_backoff_and_do_not_reuse_success(tmp_path, monkeypatch):
    """A previously positive probe cannot mask a current failure or backoff."""
    worker, identity, state, calls = _scenario(tmp_path, monkeypatch)
    _assert_gate(worker, identity, state, worker._sync_peers(identity, state), monkeypatch)

    def failed(*_args):
        """Simulate a peer becoming unreachable after a successful pass."""
        raise OSError("unreachable")

    monkeypatch.setattr(worker.peer_client, "run", failed)
    worker._sync_peers(identity, state)
    peers = worker._sync_peers(identity, state)
    assert all(peer["status"] == "backoff" for peer in peers if peer["node_id"] != TARGET)
    with pytest.raises(v8_instances.HTTPException):
        _assert_gate(worker, identity, state, peers, monkeypatch)
    assert len(calls) == 2


def test_probe_evidence_expires_with_status_freshness(tmp_path, monkeypatch):
    """Capability probes do not extend the existing two-minute freshness limit."""
    worker, identity, state, _calls = _scenario(tmp_path, monkeypatch)
    peers = worker._sync_peers(identity, state)
    with pytest.raises(v8_instances.HTTPException):
        _assert_gate(worker, identity, state, peers, monkeypatch, finished_at=int(time.time()) - 121)


def test_passive_vps_does_not_gain_capability_probe_fanout(tmp_path, monkeypatch):
    """The fix adds metadata probes only for publishing masters, never VPS nodes."""
    worker, identity, state, calls = _scenario(tmp_path, monkeypatch, local_role="vps")
    peers = worker._sync_peers(identity, state)
    assert calls == []
    assert all(peer["status"] == "topology_skipped" for peer in peers if peer["node_id"] != TARGET)


def test_first_pb8_publication_is_not_waiting_for_existing_pb8_state(tmp_path, monkeypatch):
    """Probes are available before any PB8 desired-state operation exists."""
    worker, identity, state, _calls = _scenario(tmp_path, monkeypatch)
    state["desired_state"] = {"pb8_instances": {}}
    _assert_gate(worker, identity, state, worker._sync_peers(identity, state), monkeypatch)
