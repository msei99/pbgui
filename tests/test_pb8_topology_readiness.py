"""Native publication remains fail-closed without bypassing explicit outbound topology."""

import json
import time

import pytest

from api import v8_instances
from master import cluster_sync_worker as module

CLUSTER = 'pbgui-cluster-00000000-0000-4000-8000-000000000001'
COORDINATOR = 'pbgui-node-00000000-0000-4000-8000-000000000001'
LOCAL = 'pbgui-node-00000000-0000-4000-8000-000000000002'
OTHER = 'pbgui-node-00000000-0000-4000-8000-000000000003'
TARGET = 'pbgui-node-00000000-0000-4000-8000-000000000004'


def scenario(tmp_path, monkeypatch, *, local_role='master'):
    """No real SSH client is allowed to contact either unselected master."""
    monkeypatch.setattr(module, '_log', lambda *a, **k: None)
    nodes = {
        COORDINATOR: {'node_id': COORDINATOR, 'role': 'master'},
        LOCAL: {'node_id': LOCAL, 'role': local_role, 'sync_peers': [TARGET]},
        OTHER: {'node_id': OTHER, 'role': 'master'},
        TARGET: {'node_id': TARGET, 'role': 'vps'},
    }
    for node in nodes.values():
        node.update(enabled=True, state_replica=True, sync_mode='reachable', ssh_host=node['node_id'])
    calls = []

    class Client:
        """Even an authentication-only call to an unselected node is a failure."""
        def run(self, peer, *args, **kwargs):
            """Never contact a peer as part of capability probing."""
            pytest.fail('Unselected peer must not receive SSH: ' + peer['node_id'])

    worker = module.ClusterSyncWorker(tmp_path, peer_client=Client())

    def sync(peer, *args):
        """Only the selected target may enter normal synchronization."""
        calls.append(peer['node_id'])
        assert peer['node_id'] == TARGET
        return {'node_id': TARGET, 'ok': True, 'status': 'synced', 'pb8_capability': True, 'pb9_capability': True}

    monkeypatch.setattr(worker, '_sync_peer', sync)
    monkeypatch.setattr(module, 'verified_capability_flags', lambda root: {})
    return worker, {'node_id': LOCAL, 'cluster_id': CLUSTER}, {'cluster_nodes': {'nodes': nodes}}, calls


def assert_gate(worker, identity, state, peers, monkeypatch, runtime, *, stale=False):
    """Call the actual shared PB8/PB9 readiness gate on isolated status files."""
    root = worker.cluster_root
    root.mkdir(parents=True, exist_ok=True)
    (root/'sync_status.json').write_text(json.dumps({'finished_at': int(time.time()) - (121 if stale else 0), 'peers': peers}))
    monkeypatch.setattr(v8_instances, '_cluster_nodes', lambda *a: state['cluster_nodes']['nodes'])
    v8_instances._ensure_pb8_cluster_rollout_ready(identity, runtime=runtime, cluster_root=root)


@pytest.mark.parametrize('role', ['master', 'vps'])
def test_only_explicit_target_receives_sync_and_no_hidden_probes(tmp_path, monkeypatch, role):
    """The exact user topology cannot be widened by role or capability checks."""
    worker, identity, state, calls = scenario(tmp_path, monkeypatch, local_role=role)
    peers = worker._sync_peers(identity, state)
    assert calls == [TARGET]
    skipped = [p for p in peers if p['node_id'] != TARGET]
    assert all(p['status'] == 'topology_skipped' and p.get('pb9_capability') is not True for p in skipped)
    assert state['cluster_nodes']['nodes'][LOCAL]['sync_peers'] == [TARGET]
    assert worker._peer_backoff == {}


@pytest.mark.parametrize('runtime', ['pb8', 'pb9'])
def test_unverified_skipped_replica_blocks_publication_without_ssh(tmp_path, monkeypatch, runtime):
    """Removing unauthorized probes cannot turn unknown support into confirmation."""
    worker, identity, state, calls = scenario(tmp_path, monkeypatch)
    with pytest.raises(v8_instances.HTTPException) as exc:
        assert_gate(worker, identity, state, worker._sync_peers(identity, state), monkeypatch, runtime)
    assert exc.value.status_code == 409
    assert 'no ' + runtime.upper() + ' capability confirmation' in exc.value.detail
    assert calls == [TARGET]


@pytest.mark.parametrize('runtime', ['pb8', 'pb9'])
def test_gate_rechecks_relay_evidence_instead_of_trusting_saved_flags(tmp_path, monkeypatch, runtime):
    """Expired/removed evidence cannot be revived by a fresh status file with old True flags."""
    worker, identity, state, _ = scenario(tmp_path, monkeypatch)
    peers = worker._sync_peers(identity, state)
    flags = {node: {'pb8_capability': True, 'pb9_capability': True, 'capability_source': 'signed_relay'} for node in [COORDINATOR, OTHER]}
    for peer in peers:
        peer.update(flags.get(peer['node_id'], {}))
    monkeypatch.setattr(v8_instances, 'verified_capability_flags', lambda root: flags)
    assert_gate(worker, identity, state, peers, monkeypatch, runtime)
    monkeypatch.setattr(v8_instances, 'verified_capability_flags', lambda root: {})
    with pytest.raises(v8_instances.HTTPException):
        assert_gate(worker, identity, state, peers, monkeypatch, runtime)
    monkeypatch.setattr(v8_instances, 'verified_capability_flags', lambda root: flags)
    with pytest.raises(v8_instances.HTTPException):
        assert_gate(worker, identity, state, peers, monkeypatch, runtime, stale=True)
