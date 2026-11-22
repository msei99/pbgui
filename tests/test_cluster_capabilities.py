"""Offline signed-evidence and restricted-route regressions with real temporary keys."""

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import json
from pathlib import Path
import time

import pytest

from cluster_credentials import ensure_node_key_material, load_node_key_material, sign_operation
from cluster_sync_command import ClusterSyncCommandError, run_command
from master import cluster_capabilities as evidence
from master import cluster_sync_worker as worker_module
from master.cluster_state import ensure_local_identity, read_local_identity, read_materialized_state, write_operation

CLUSTER = 'pbgui-cluster-00000000-0000-4000-8000-000000000001'
A = 'pbgui-node-00000000-0000-4000-8000-000000000001'
B = 'pbgui-node-00000000-0000-4000-8000-000000000002'
VPS = 'pbgui-node-00000000-0000-4000-8000-000000000003'


@pytest.fixture
def topology(tmp_path, monkeypatch):
    """Three replicas trust identical test membership; masters permit only the common VPS."""
    monkeypatch.setattr(evidence, '_log', lambda *a, **k: None)
    roots = {node: tmp_path / label / 'data/cluster' for node, label in [(A, 'a'), (B, 'b'), (VPS, 'vps')]}
    operations = []
    for node, root in roots.items():
        role = 'vps' if node == VPS else 'master'
        ensure_local_identity(root, role=role, pbname=node, cluster_id=CLUSTER, node_id=node)
        keys = ensure_node_key_material(root)
        operations.append({
            **keys.public_bundle(node, role), 'schema_version': 1, 'cluster_id': CLUSTER,
            'op_id': f'{node}:00000001', 'actor': node, 'seq': 1, 'op': 'ADD_NODE', 'created_at': 100,
            'enabled': True, 'state_replica': True, 'sync_mode': 'reachable', 'ssh_host': node,
            'sync_peers': [] if node == VPS else [VPS],
        })
    for root in roots.values():
        for operation in operations:
            write_operation(root, operation, allow_legacy_membership=True)
        read_materialized_state(root)
    return roots


def proof(root, *, now=None, capabilities=None, **changes):
    """Sign a public test statement with the actual temporary node key."""
    at = int(time.time()) if now is None else now
    identity = read_local_identity(root)
    document = {
        'domain': evidence.DOMAIN, 'cluster_id': CLUSTER, 'node_id': identity['node_id'],
        'issued_at': at, 'expires_at': at + 120,
        'capabilities': ['pb8_instances_v1', 'pb9_instances_v1'] if capabilities is None else capabilities,
        **changes,
    }
    return sign_operation(document, load_node_key_material(root).signing_private_key, signer_id=identity['node_id'])


def merge(root, *proofs, now=None):
    """Use the real bounded cache transaction."""
    return evidence.merge_capability_evidence(root, {'version': 1, 'proofs': list(proofs)}, now=now)


def test_real_signatures_survive_allowed_relay_without_ssh(topology):
    """A common VPS relays authentic statements but holds no originating private key."""
    roots = topology
    statement = proof(roots[A])
    assert merge(roots[VPS], statement) == 1
    exported = evidence.capability_evidence(roots[VPS])
    evidence.merge_capability_evidence(roots[B], exported)
    flags = evidence.verified_capability_flags(roots[B])
    assert flags[A]['pb8_capability'] is True and flags[A]['pb9_capability'] is True
    assert flags[A]['capability_expires_at'] == statement['expires_at']
    assert (roots[B] / 'capability_evidence.json').stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize('fault', ['signature', 'foreign', 'wrong-node', 'expired', 'future', 'long-ttl', 'domain', 'boolean-time', 'unknown-node'])
def test_untrusted_or_stale_proofs_never_establish_support(topology, fault):
    """Cryptographic authenticity alone cannot bypass identity, time or domain validation."""
    roots = topology
    now = int(time.time())
    changes = {
        'foreign': {'cluster_id': CLUSTER + '-foreign'}, 'wrong-node': {'node_id': B},
        'expired': {'issued_at': now - 121, 'expires_at': now - 1},
        'future': {'issued_at': now + 6, 'expires_at': now + 126},
        'long-ttl': {'expires_at': now + 121}, 'domain': {'domain': 'checkpoint'},
        'boolean-time': {'issued_at': True}, 'unknown-node': {'node_id': 'unknown'},
    }.get(fault, {})
    value = proof(roots[A], now=now, **changes)
    if fault == 'signature':
        value['capabilities'] = []
    assert merge(roots[B], value, now=now) == 0
    assert A not in evidence.verified_capability_flags(roots[B], now=now)


def test_replay_does_not_extend_expiry_or_replace_new_negative(topology):
    """A relay can neither refresh an old positive nor hide a newer negative statement."""
    roots = topology
    now = int(time.time())
    positive = proof(roots[A], now=now)
    merge(roots[B], positive, now=now)
    merge(roots[B], proof(roots[A], now=now + 1, capabilities=[]), now=now + 1)
    merge(roots[B], positive, now=now + 2)
    assert evidence.verified_capability_flags(roots[B], now=now + 2)[A]['pb9_capability'] is False
    assert evidence.verified_capability_flags(roots[B], now=now + 121) == {}


def test_read_rechecks_current_membership_key_and_enabled_state(topology, monkeypatch):
    """Rotation, removal and disabling invalidate cached evidence immediately."""
    roots = topology
    merge(roots[B], proof(roots[A]))
    identity, nodes = evidence._context(roots[B])
    for mutation in ['remove', 'disable', 'rotate']:
        changed = deepcopy(nodes)
        if mutation == 'remove':
            changed.pop(A)
        elif mutation == 'disable':
            changed[A]['enabled'] = False
        else:
            changed[A]['signing_public_key'] = changed[B]['signing_public_key']
            changed[A]['signing_key_id'] = changed[B]['signing_key_id']
        monkeypatch.setattr(evidence, '_context', lambda _root, c=changed: (identity, c))
        assert A not in evidence.verified_capability_flags(roots[B])


def test_concurrent_merges_preserve_both_signers(topology):
    """A locked read/modify/write cannot lose another peer's evidence."""
    roots = topology
    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(lambda p: merge(roots[VPS], p), [proof(roots[A]), proof(roots[B])]))
    assert set(evidence.verified_capability_flags(roots[VPS])) == {A, B}


@pytest.mark.parametrize('kind', ['cache', 'lock'])
def test_cache_symlinks_are_rejected_without_external_write(topology, tmp_path, kind):
    """Evidence cannot overwrite a path outside its owned cluster root."""
    root = topology[B]
    external = tmp_path / 'sentinel'
    external.write_text('unchanged')
    name = 'capability_evidence.json' + ('.lock' if kind == 'lock' else '')
    (root / name).symlink_to(external)
    with pytest.raises(ValueError):
        merge(root, proof(topology[A]))
    assert external.read_text() == 'unchanged'


def test_evidence_rpc_requires_registered_peer_and_bounded_upload(topology):
    """The optional RPC cannot act as an unauthenticated upload or an SSH proxy."""
    root = topology[VPS]
    unknown = 'pbgui-node-00000000-0000-4000-8000-000000000099'
    with pytest.raises(ClusterSyncCommandError):
        run_command(root, unknown, 'put-capability-evidence', b'{"version":1,"proofs":[]}')
    with pytest.raises(ClusterSyncCommandError):
        run_command(root, A, 'put-capability-evidence', b'x' * (evidence.MAX_BYTES + 1))
    reply = run_command(root, A, 'put-capability-evidence', json.dumps({'version': 1, 'proofs': [proof(topology[A])]}).encode())
    assert reply['ok'] is True and reply['accepted'] == 1


def test_same_second_negative_cannot_be_overwritten(topology):
    """Replaying a positive from the same second cannot replace a negative statement."""
    now = int(time.time())
    merge(topology[B], proof(topology[A], now=now, capabilities=[]), now=now)
    merge(topology[B], proof(topology[A], now=now), now=now)
    assert evidence.verified_capability_flags(topology[B], now=now)[A]['pb9_capability'] is False


def test_real_restricted_exchange_and_publication_gate(topology, monkeypatch):
    """Two masters publish safely via a shared VPS without any direct master-to-master call."""
    from api import v8_instances

    roots = topology
    calls = []

    class Client:
        """Execute the real forced-command handlers only on the permitted synthetic VPS."""
        def __init__(self, source):
            """Bind this client to one master identity."""
            self.source = source

        def run(self, peer, source, command, payload=None):
            """Treat an unexpected direct connection as a hard test failure."""
            assert source == self.source
            assert peer['node_id'] == VPS
            calls.append((source, VPS, command))
            return run_command(roots[VPS], source, command, (payload or '').encode())

    workers = {node: worker_module.ClusterSyncWorker(root.parent.parent, peer_client=Client(node)) for node, root in roots.items() if node != VPS}
    peer = read_materialized_state(roots[A])['cluster_nodes']['nodes'][VPS]
    for node in [A, B, A]:
        hello = run_command(roots[VPS], node, 'hello')
        workers[node]._exchange_capability_evidence(peer, node, CLUSTER, hello)
    assert evidence.verified_capability_flags(roots[B])[A]['pb9_capability'] is True
    assert evidence.verified_capability_flags(roots[A])[B]['pb9_capability'] is True

    def synced(peer, *args):
        """The shared VPS has the standard direct authenticated compatibility result."""
        assert peer['node_id'] == VPS
        return {'node_id': VPS, 'status': 'synced', 'ok': True, 'pb8_capability': True, 'pb9_capability': True}

    monkeypatch.setattr(workers[B], '_sync_peer', synced)
    state = read_materialized_state(roots[B])
    identity = read_local_identity(roots[B])
    rows = workers[B]._sync_peers(identity, state)
    other = next(row for row in rows if row['node_id'] == A)
    assert other['status'] == 'topology_skipped' and other['capability_source'] == 'signed_relay'
    (roots[B]/'sync_status.json').write_text(json.dumps({'finished_at': int(time.time()), 'peers': rows}))
    for runtime in ['pb8', 'pb9']:
        v8_instances._ensure_pb8_cluster_rollout_ready(identity, runtime=runtime, cluster_root=roots[B])
    # Replacing the proof with a newer authenticated negative must override saved True flags.
    future = int(time.time()) + 1
    merge(roots[B], proof(roots[A], now=future, capabilities=[]), now=future)
    for runtime in ['pb8', 'pb9']:
        with pytest.raises(v8_instances.HTTPException):
            v8_instances._ensure_pb8_cluster_rollout_ready(identity, runtime=runtime, cluster_root=roots[B])
    assert calls and all(target == VPS for _, target, _ in calls)


def test_ssh_transport_rechecks_current_allowlist_before_launch(topology, monkeypatch):
    """No SSH subprocess/key setup runs for denied targets, including after a policy edit."""
    from master.cluster_state import append_operation

    root = topology[A]
    nodes = read_materialized_state(root)['cluster_nodes']['nodes']
    client = worker_module.SshClusterPeerClient(cluster_root=root)
    calls = []
    monkeypatch.setattr(client, '_ssh_command', lambda peer, *_: ['ssh', peer['ssh_host']])

    def subprocess_run(argv, **kwargs):
        """Record only simulated allowed launches, never run real SSH."""
        from types import SimpleNamespace
        calls.append(argv)
        return SimpleNamespace(returncode=0, stdout='{"ok":true}', stderr='')

    monkeypatch.setattr(worker_module.subprocess, 'run', subprocess_run)
    with pytest.raises(worker_module.ClusterSyncWorkerError, match='outbound SSH denied'):
        client.run(nodes[B], A, 'hello')
    assert calls == []
    client.run({**nodes[VPS], 'ssh_host': 'stale-untrusted-endpoint'}, A, 'hello')
    assert calls == [['ssh', VPS]]
    append_operation(root, 'UPDATE_NODE', {'node_id': A, 'sync_peers': []})
    with pytest.raises(worker_module.ClusterSyncWorkerError, match='outbound SSH denied'):
        client.run(nodes[VPS], A, 'hello')
    assert calls == [['ssh', VPS]]


def test_checkpoint_requires_allowed_outbound_route_before_any_rpc(topology, monkeypatch):
    """Automatic checkpoint coordination has no separate topology bypass."""
    root = topology[A]
    worker = worker_module.ClusterSyncWorker(root.parent.parent)
    monkeypatch.setattr(worker_module, 'build_shadow_checkpoint', lambda root: {'migration_seal': {'status': 'sealed'}})
    monkeypatch.setattr(worker_module, 'create_checkpoint_proposal', lambda *a, **k: {'required_node_ids': [A, B, VPS]})
    monkeypatch.setattr(worker_module, 'create_checkpoint_ack', lambda *a, **k: {})
    monkeypatch.setattr(worker.peer_client, 'run', lambda *a, **k: pytest.fail('No checkpoint RPC to denied peer'))
    result = worker._commit_checkpoint_clusterwide(read_local_identity(root), read_materialized_state(root), [])
    assert result['status'] == 'blocked'
    assert B in result['blockers'][0]


def test_cache_union_respects_read_and_wire_byte_limits(topology, monkeypatch):
    """Individually bounded uploads cannot grow the merged cache beyond its own reader limit."""
    roots = topology
    proofs = [proof(root) for root in roots.values()]
    envelope = {'version': 1, 'proofs': proofs}
    # Make the real formatting overhead cross the limit without generating a huge fixture.
    limit = len(json.dumps(envelope).encode()) + 8
    assert len(json.dumps(proofs, indent=4).encode()) > limit
    monkeypatch.setattr(evidence, 'MAX_BYTES', limit)
    evidence.merge_capability_evidence(roots[VPS], envelope)
    cache = roots[VPS]/'capability_evidence.json'
    assert cache.stat().st_size <= limit
    assert evidence.verified_capability_flags(roots[VPS])
    assert len(json.dumps(evidence.capability_evidence(roots[VPS])).encode()) <= limit


@pytest.mark.parametrize('peers', ['all', {}, (), None, False])
def test_malformed_explicit_outbound_list_never_falls_back(peers):
    """A broken allowlist must not regain role defaults or VPS access."""
    local = {'role': 'master', 'sync_peers': peers}
    peer = {'role': 'vps'}
    assert worker_module._peer_topology_allows(A, local, VPS, peer, {})[0] is False


@pytest.mark.parametrize('local', [{'role': ''}, {'role': 'unexpected', 'sync_peers': [VPS]},
                                  {'role': 'master', 'sync_mode': 'disabled', 'sync_peers': [VPS]}])
def test_unknown_or_disabled_local_identity_cannot_start_outbound_ssh(local):
    """An allowlist entry cannot override an invalid or disabled local identity."""
    assert worker_module._peer_topology_allows(A, local, VPS, {'role': 'vps'}, {})[0] is False


@pytest.mark.parametrize('fault', ['timeout', 'list-reply', 'deep-json', 'type'])
def test_optional_relay_failure_does_not_escape_direct_sync(topology, fault, monkeypatch):
    """Optional evidence errors cannot convert successful legacy synchronization into failure."""
    import subprocess
    from types import SimpleNamespace

    roots = topology
    monkeypatch.setattr(worker_module, '_log', lambda *a, **k: None)

    class Client:
        """Simulate the real SSH timeout and malformed response classes."""
        def run(self, *args, **kwargs):
            """Fail only the optional evidence exchange after a valid direct hello."""
            if fault == 'timeout':
                raise subprocess.TimeoutExpired('synthetic-ssh', 1)
            if fault == 'deep-json':
                raise RecursionError('deep JSON')
            if fault == 'type':
                raise TypeError('malformed wire value')
            return []

    worker = worker_module.ClusterSyncWorker(roots[B].parent.parent, peer_client=Client())
    peer = read_materialized_state(roots[B])['cluster_nodes']['nodes'][VPS]
    hello = run_command(roots[VPS], B, 'hello')
    worker._exchange_capability_evidence(peer, B, CLUSTER, hello)


def test_hello_evidence_matches_its_advertised_capabilities(topology):
    """The signed self statement and direct protocol advertisement cannot drift apart."""
    hello = run_command(topology[VPS], A, 'hello')
    own = next(p for p in hello['capability_evidence']['proofs'] if p['node_id'] == VPS)
    assert own['capabilities'] == hello['capabilities']
