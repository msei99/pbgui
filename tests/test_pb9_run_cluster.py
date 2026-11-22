"""Native Run protocol, signed state, checkpoint and complete-bundle regressions."""
import copy
import json
from pathlib import Path

import pytest

import cluster_sync_command as sync
from master.cluster_state import (
    ClusterStateError, append_operation, ensure_local_identity, rebuild_materialized_state,
    validate_operation, load_operations, PB9_OPERATION_CAPABILITY,
)
from master.cluster_checkpoint import build_shadow_checkpoint, materialize_checkpoint_tail, replica_blob_hashes
from tests.test_pb8_cluster_sync import _write_manifest, CLUSTER_ID, NODE_A


@pytest.fixture
def cluster(tmp_path, monkeypatch):
    """Use a private temporary cluster; never inspect production credentials or processes."""
    root = tmp_path / 'data/cluster'
    ensure_local_identity(root, role='master', pbname='node-a', cluster_id=CLUSTER_ID, node_id=NODE_A)
    append_operation(root, 'ADD_NODE', {'node_id': NODE_A, 'role': 'master', 'pbname': 'node-a'})
    monkeypatch.setattr(sync, '_pb8_materialization_blockers', lambda *a, **k: [])
    monkeypatch.setattr(sync, '_require_pb9_stopped', lambda path: None)
    return root


def _upsert(root, *, version=1, parent=0, enabled='node-a', change=None):
    """Append a pure native config and GUI sidecar backed by verified immutable blobs."""
    native = {'config_version': 'v9.0.0', 'live': {'account': {'user': 'fixture'}}}
    gui = {'runtime': 'pb9', 'cluster_managed': True, 'version': version, 'enabled_on': enabled}
    if change:
        change(native, gui)
    digest = _write_manifest(root, {'config.json': json.dumps(native).encode(), 'pbgui.json': json.dumps(gui).encode()})
    operation = append_operation(root, 'UPSERT_PB9_CONFIG', {
        'instance': 'fixture', 'version': str(version), 'parent_version': str(parent),
        'assigned_host': NODE_A, 'desired_state': 'stopped' if enabled == 'disabled' else 'running',
        'config_manifest_hash': digest,
    })
    rebuild_materialized_state(root)
    return operation, digest


def test_pb9_is_signed_without_checkpoint_and_cannot_be_relabelled(cluster):
    """Membership signatures are mandatory even before the first checkpoint."""
    op, _ = _upsert(cluster)
    assert op.get('signature')
    validate_operation(op, cluster_root=cluster)
    for key in ('signature', 'signer_id'):
        bad = copy.deepcopy(op)
        bad.pop(key)
        with pytest.raises(ClusterStateError):
            validate_operation(bad, cluster_root=cluster)
    bad = {**op, 'assigned_host': 'pbgui-node-00000000-0000-4000-8000-00000000008b'}
    with pytest.raises(ClusterStateError):
        validate_operation(bad, cluster_root=cluster)


def test_same_name_generations_reduce_independently(cluster):
    """PB8 and PB9 lifecycle operations never share their maps or tombstones."""
    op, digest = _upsert(cluster)
    append_operation(cluster, 'UPSERT_PB8_CONFIG', {
        'instance': 'fixture', 'version': '8', 'assigned_host': NODE_A,
        'desired_state': 'running', 'config_manifest_hash': digest,
    })
    append_operation(cluster, 'DELETE_PB9_INSTANCE', {'instance': 'fixture', 'version': '1'})
    desired = rebuild_materialized_state(cluster)['desired_state']
    assert desired['pb8_instances']['fixture']['version'] == '8'
    assert desired['pb9_instances'] == {}
    assert desired['pb9_tombstones']['fixture']['version'] == '1'


def test_pb9_checkpoint_tail_and_blob_reachability(cluster, monkeypatch):
    """A checkpoint preserves PB9 state, native file blobs and later PB9 tombstones."""
    _op, digest = _upsert(cluster)
    import time
    from master import cluster_checkpoint as cp
    monkeypatch.setattr(cp, 'build_migration_seal', lambda state: {
        'schema_version': 1, 'status': 'sealed', 'cluster_id': CLUSTER_ID,
        'active_node_ids': [NODE_A], 'blockers': [],
    })
    checkpoint = build_shadow_checkpoint(cluster)
    retained = replica_blob_hashes(cluster)
    assert digest in retained['config']
    now = int(time.time())
    proposal = cp.create_checkpoint_proposal(cluster, checkpoint, created_at=now, expires_at=now + 60)
    ack = cp.create_checkpoint_ack(cluster, checkpoint, proposal, created_at=now)
    proof = cp.create_checkpoint_commit_proof(cluster, checkpoint, proposal, [ack], created_at=now)
    cp.activate_checkpoint(cluster, checkpoint, commit_proof=proof, activated_at=now)
    op = append_operation(cluster, 'DELETE_PB9_INSTANCE', {'instance': 'fixture', 'version': '1'})
    replay = materialize_checkpoint_tail(checkpoint, [op])
    assert replay['desired_state']['pb9_tombstones']['fixture']['version'] == '1'
    assert replay['desired_state']['pb9_instances'] == {}
    assert replay['desired_state'] == rebuild_materialized_state(cluster, write=False)['desired_state']


def test_native_materialization_is_private_complete_and_pb8_independent(cluster):
    """Real signed protocol commands publish complete private PB9 bundles only."""
    _upsert(cluster)
    old = cluster.parent / 'run_v8/fixture/config.json'
    old.parent.mkdir(parents=True)
    old.write_text('PB8 untouched')
    result = sync.run_command(cluster, NODE_A, 'materialize-v9')
    target = cluster.parent / 'run_v9/fixture'
    assert result['ok'] and result['counts']['written_files'] == 2
    assert json.loads((target / 'pbgui.json').read_text())['cluster_managed'] is True
    assert 'pbgui' not in json.loads((target / 'config.json').read_text())
    assert target.stat().st_mode & 0o777 == 0o700
    assert (target / 'config.json').stat().st_mode & 0o777 == 0o600
    assert old.read_text() == 'PB8 untouched'
    assert PB9_OPERATION_CAPABILITY in sync.run_command(cluster, NODE_A, 'hello')['capabilities']
    _upsert(cluster, version=2, parent=1, enabled='disabled')
    sync.run_command(cluster, NODE_A, 'materialize-v9')
    backups = list((cluster.parent / 'backup/v9/fixture').iterdir())
    assert len(backups) == 1 and backups[0].stat().st_mode & 0o777 == 0o700
    assert json.loads((backups[0] / 'pbgui.json').read_text())['version'] == 1


@pytest.mark.parametrize('problem', ['wrong_account', 'wrong_host', 'version', 'not_managed', 'external'])
def test_native_manifest_rejected_before_any_file_is_written(cluster, problem):
    """Bad account/host/version identity and external references fail closed."""
    def change(native, gui):
        """Corrupt one independent boundary in immutable synthetic input."""
        if problem == 'wrong_account': native['live']['account']['user'] = 'other'
        if problem == 'wrong_host': gui['enabled_on'] = 'other'
        if problem == 'version': gui['version'] = 8
        if problem == 'not_managed': gui.pop('cluster_managed')
        if problem == 'external': native['coin_overrides'] = 'outside.json'
    _upsert(cluster, change=change)
    preview = sync.run_command(cluster, NODE_A, 'materialize-v9-preview')
    assert preview['counts']['error'] == 1
    with pytest.raises(sync.ClusterSyncCommandError):
        sync.run_command(cluster, NODE_A, 'materialize-v9')
    assert not (cluster.parent / 'run_v9/fixture/config.json').exists()


@pytest.mark.parametrize('shape', ['absolute', 'relative', 'symlink', 'unrelated'])
def test_shared_process_reference_guard_normalizes_config_paths(tmp_path, monkeypatch, shape):
    """Cleanup cannot miss a config merely because argv uses cwd or a link."""
    from types import SimpleNamespace
    import native_live_process as guard
    config = tmp_path / 'instance/config.json'
    config.parent.mkdir(); config.write_text('{}')
    link = tmp_path / 'linked'
    link.symlink_to(config.parent, target_is_directory=True)
    value = {'absolute': str(config), 'relative': 'instance/config.json',
             'symlink': str(link / 'config.json'), 'unrelated': 'elsewhere/config.json'}[shape]
    process = SimpleNamespace(cmdline=lambda: ['python', 'live', value], cwd=lambda: str(tmp_path))
    monkeypatch.setattr(guard.psutil, 'process_iter', lambda: [process])
    assert guard.config_is_referenced(config) is (shape != 'unrelated')


def test_unadopted_local_bundle_is_not_overwritten_or_deleted(cluster):
    """A replicated name alone never authorizes replacement/deletion of a legacy bundle."""
    target = cluster.parent / 'run_v9/fixture'
    target.mkdir(parents=True)
    (target / 'config.json').write_text('{"legacy":true}')
    (target / 'pbgui.json').write_text('{"runtime":"pb9","version":1,"enabled_on":"disabled"}')
    original = {p.name: p.read_bytes() for p in target.iterdir()}
    _upsert(cluster, version=2, parent=1)
    preview = sync.run_command(cluster, NODE_A, 'materialize-v9-preview')
    assert preview['counts']['error'] == 1
    assert {p.name: p.read_bytes() for p in target.iterdir()} == original
    append_operation(cluster, 'DELETE_PB9_INSTANCE', {'instance': 'fixture', 'version': '2'})
    with pytest.raises(sync.ClusterSyncCommandError):
        sync.run_command(cluster, NODE_A, 'materialize-v9')
    assert {p.name: p.read_bytes() for p in target.iterdir()} == original
