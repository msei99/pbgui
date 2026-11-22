"""Offline PB9 supervision, source isolation and cross-generation account guards."""
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from PBRun import RunV8, RunV9, PBRun
import passivbot_account_guard as guard


@pytest.fixture
def runner(tmp_path, monkeypatch):
    """Use a synthetic source, credentials and instance, with no native execution."""
    source = tmp_path / 'native9'
    source.mkdir()
    (source / 'api-keys.json').write_text(json.dumps({'fixture': {}}))
    instance = tmp_path / 'data/run_v9/fixture'
    instance.mkdir(parents=True)
    (instance / 'config.json').write_text(json.dumps({'config_version': 'v9.0.0', 'live': {'account': {'user': 'fixture'}}, 'bot': {'long': {}}}))
    (instance / 'pbgui.json').write_text(json.dumps({'runtime': 'pb9', 'version': 3, 'enabled_on': 'node'}))
    monkeypatch.setattr(guard.psutil, 'process_iter', lambda *a, **k: [])
    value = RunV9()
    value.path, value.pbgdir, value.name, value.user = str(instance), tmp_path, 'node', 'fixture'
    value.pb9dir, value.pb9venv = str(source), str(tmp_path / 'venv9/bin/python')
    return value


def test_pb9_load_reuses_shared_lifecycle_with_native_sidecar(runner):
    """PB9 gets its own user, source and command without PB8 metadata assumptions."""
    assert isinstance(runner, RunV8)
    assert runner.load()
    assert runner.live_user == 'fixture'
    assert runner.version == 3
    assert runner.source_dir == runner.pb9dir
    assert runner.command == [str(Path(runner.pb9venv).parent / 'passivbot'), 'live', str(Path(runner.path) / 'config.json'), '--fail-on-stale-rust']
    assert runner._cluster_gate_result()['ok'] is True


@pytest.mark.parametrize('change', ['disabled', 'remote', 'schema_metadata', 'symlink'])
def test_invalid_or_unassigned_bundle_never_loads(runner, change):
    """A different host, embedded GUI metadata or a sidecar symlink cannot launch."""
    sidecar = Path(runner.path) / 'pbgui.json'
    if change in ('disabled', 'remote'):
        value = json.loads(sidecar.read_text())
        value['enabled_on'] = change
        sidecar.write_text(json.dumps(value))
    elif change == 'schema_metadata':
        path = Path(runner.path) / 'config.json'
        value = json.loads(path.read_text()); value['pbgui'] = {}
        path.write_text(json.dumps(value))
    else:
        outside = sidecar.parent.parent / 'other.json'
        sidecar.rename(outside)
        sidecar.symlink_to(outside)
    assert not runner.load()


def test_confirmed_launch_records_version_and_failed_launch_does_not(runner, monkeypatch):
    """Only a confirmed new launch supplies the list's running-version marker."""
    runner.version = 3
    monkeypatch.setattr(RunV8, 'start', lambda *a, **k: True)
    assert runner.start()
    marker = Path(runner.path) / 'running_version.txt'
    assert marker.read_text() == '3'
    runner.version = 4
    monkeypatch.setattr(RunV8, 'start', lambda *a, **k: False)
    assert not runner.start()
    assert marker.read_text() == '3'


def test_account_guard_rejects_enabled_pb8_but_allows_disabled(runner):
    """Disabled stale metadata does not block a disjoint PB9 start."""
    path = Path(runner.pbgdir) / 'data/run_v8/fixture'
    path.mkdir(parents=True)
    config = {'live': {'user': 'fixture'}, 'pbgui': {'enabled_on': 'node'}}
    (path / 'config.json').write_text(json.dumps(config))
    with pytest.raises(guard.AccountConflictError):
        with guard.account_start_lock(runner.pbgdir, 'fixture', 'pb9', runner.path):
            pytest.fail('conflicting account lock must not yield')
    config['pbgui']['enabled_on'] = 'disabled'
    (path / 'config.json').write_text(json.dumps(config))
    with guard.account_start_lock(runner.pbgdir, 'fixture', 'pb9', runner.path):
        pass


def test_pb9_watcher_stays_in_own_generation(runner, monkeypatch):
    """The scanner configures only RunV9 identities with their own slot paths."""
    import PBRun as module
    manager = PBRun.__new__(PBRun)
    manager.pbgdir, manager.name = runner.pbgdir, 'node'
    manager.pb9dir, manager.pb9venv, manager.pb9_ready = runner.pb9dir, runner.pb9venv, True
    manager.pb7dir = manager.pb7venv = manager.pb8dir = manager.pb8venv = None
    manager.v9_path = str(Path(runner.pbgdir) / 'data/run_v9')
    manager.run_v9 = []
    watched = []
    monkeypatch.setattr(RunV9, 'watch', lambda value: watched.append(value))
    manager.watch_v9()
    assert len(watched) == 1
    assert watched[0].runtime_generation == 'pb9'
    assert watched[0].pb9dir == runner.pb9dir
    assert not watched[0].pb8dir


def test_account_lock_rejects_symlink_parent(runner):
    """A symlinked lock parent never creates a lock outside PBGui's tree."""
    outside = Path(runner.pbgdir) / 'outside'
    outside.mkdir()
    (Path(runner.pbgdir) / 'data/locks').symlink_to(outside, target_is_directory=True)
    with pytest.raises(guard.AccountLockBusyError):
        with guard.account_lock(runner.pbgdir, 'fixture'):
            pytest.fail('Unsafe lock parent must not yield')
    assert list(outside.iterdir()) == []


def test_stale_native_build_blocks_launch_readiness(runner, monkeypatch):
    """No process launch is possible when the read-only Rust evidence is stale."""
    import PBRun as module
    import pb9_runtime
    monkeypatch.setattr(pb9_runtime, 'inspect_pb9_runtime', lambda *a, **k: {'detected': True})
    calls = []
    def probe(argv, **options):
        """Return stale fingerprint evidence without executing any program."""
        calls.append((argv, options))
        return SimpleNamespace(returncode=0, stdout=json.dumps({'shadowed': False, 'fingerprinted': True, 'stamped': True, 'needs_rebuild': True}))
    monkeypatch.setattr(module.subprocess, 'run', probe)
    assert not runner._runtime_ready()
    assert calls[0][0][1:3] == ['-I', '-B']
    assert 'PYTHONPATH' not in calls[0][1]['env']


def test_pb9_launch_environment_excludes_foreign_python_import_paths(runner, monkeypatch):
    """PBGui's import overrides cannot select modules from another runtime."""
    monkeypatch.setenv('PYTHONPATH', '/foreign/pb8')
    monkeypatch.setenv('PYTHONHOME', '/foreign')
    monkeypatch.setenv('PYTHONUSERBASE', '/foreign/user')
    env = runner._launch_env()
    assert not {'PYTHONPATH', 'PYTHONHOME', 'PYTHONUSERBASE'} & env.keys()
    assert env['PYTHONNOUSERSITE'] == '1'
    assert env['VIRTUAL_ENV'] == str(Path(runner.pb9venv).parent.parent)


@pytest.mark.parametrize('state', ['allowed', 'wrong_host', 'version', 'manifest', 'stopped', 'tombstone', 'pb8_only', 'legacy'])
def test_pb9_cluster_gate_checks_only_native_assignment_and_manifest(runner, state):
    """Use the real shared runner gate without starting or stopping any process."""
    from master.cluster_state import (ensure_local_identity, append_operation, rebuild_materialized_state,
                                     persist_config_manifest_blobs)
    from tests.test_pb8_cluster_sync import CLUSTER_ID, NODE_A, NODE_B
    root = Path(runner.pbgdir) / 'data/cluster'
    ensure_local_identity(root, role='master', pbname='node', cluster_id=CLUSTER_ID, node_id=NODE_A)
    append_operation(root, 'ADD_NODE', {'node_id': NODE_A, 'role': 'master', 'pbname': 'node'})
    sidecar = Path(runner.path) / 'pbgui.json'
    metadata = json.loads(sidecar.read_text())
    if state != 'legacy': metadata['cluster_managed'] = True
    sidecar.write_text(json.dumps(metadata))
    assert runner.load()
    digest = persist_config_manifest_blobs(root, Path(runner.path))
    family = 'PB8' if state in {'pb8_only', 'legacy'} else 'PB9'
    append_operation(root, f'UPSERT_{family}_CONFIG', {
        'instance': 'fixture', 'version': '4' if state == 'version' else '3',
        'assigned_host': NODE_B if state == 'wrong_host' else NODE_A,
        'desired_state': 'stopped' if state == 'stopped' else 'running',
        'config_manifest_hash': digest,
    })
    if state == 'manifest':
        value = json.loads(sidecar.read_text()); value['note'] = 'changed after publication'
        sidecar.write_text(json.dumps(value))
    if state == 'tombstone':
        append_operation(root, 'DELETE_PB9_INSTANCE', {'instance': 'fixture', 'version': '3'})
    rebuild_materialized_state(root)
    result = runner._cluster_gate_result()
    assert result['ok'] is (state in {'allowed', 'legacy'}), result
