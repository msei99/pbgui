"""PB9 Run HTTP and bundle transactions with synthetic accounts and runtimes."""
import copy
import json
import stat
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api import auth, pb9, v9_instances as api
from pb9_config import PB9ConfigError


@pytest.fixture
def run_api(tmp_path, monkeypatch):
    """Own workers and temp storage; no real runtime, credentials or processes."""
    config = {'config_version': 'v9.0.0', 'live': {'account': {'user': 'fixture'}},
              'bot': {'portfolio': {'strategy_kind': 'ema_anchor'}, 'global': {}, 'long': {}, 'short': {}},
              'pbgui': {'runtime': 'pb9', 'version': 0, 'enabled_on': 'disabled', 'note': ''}}
    monkeypatch.setattr(api, 'PBGDIR', tmp_path)
    monkeypatch.setattr(api, '_master_hostname', lambda: 'node')
    monkeypatch.setattr(api, '_paths', lambda: {k: str(tmp_path / k) for k in pb9._PATH_KEYS})
    monkeypatch.setattr(api, '_available_users', lambda: [{'name': 'fixture', 'exchange': 'bybit'}, {'name': 'copy', 'exchange': 'bybit'}])
    monkeypatch.setattr(api, '_user_exchange_map', lambda: {'fixture': 'bybit'})
    monkeypatch.setattr(api, 'validate_pb9_live_config', lambda *a, **k: {'valid': True})
    monkeypatch.setattr(api, 'load_pb9_template', lambda *a, **k: {'config': {k: copy.deepcopy(v) for k, v in config.items() if k != 'pbgui'}})
    monkeypatch.setattr(api, '_runner', lambda *a, **k: SimpleNamespace(pid=lambda: None))
    monkeypatch.setattr(api.psutil, 'process_iter', lambda *a, **k: [])
    from master import cluster_sync_worker
    activations = []
    def activate(_root, operation, **kwargs):
        """Record synthetic delivery only; never contact a real Cluster host."""
        activations.append(copy.deepcopy(operation))
        callback = getattr(state, 'activation_callback', None)
        if callback:
            callback(operation)
        return {'ok': False, 'direct': False}
    monkeypatch.setattr(cluster_sync_worker, 'push_pb9_activation', activate)
    api._invalidate_instance_list()
    api.clear_drafts()

    @asynccontextmanager
    async def lifespan(app):
        """Drain all test workers on normal and failed test exits."""
        pb9.startup(app)
        try:
            yield
        finally:
            await pb9.shutdown(app)

    app = FastAPI(lifespan=lifespan)
    app.dependency_overrides[auth.require_auth] = lambda: SimpleNamespace(user_id='fixture')
    app.include_router(api.router, prefix='/api/v9')
    state = SimpleNamespace(config=config, root=tmp_path, monkeypatch=monkeypatch, activations=activations)
    with TestClient(app) as client:
        state.client = client
        yield state


def _save(state, config=None, **extra):
    """Submit an explicit optimistic save through the owned HTTP worker."""
    return state.client.put('/api/v9/instances/fixture/config', json={'config': config or state.config, **extra})


def test_save_load_and_backup_keep_native_and_gui_separate(run_api):
    """Two saves preserve sparse native source and snapshot the exact prior sidecar."""
    s = run_api
    response = _save(s)
    assert response.status_code == 200, response.text
    native = s.root / 'data/run_v9/fixture/config.json'
    gui = native.with_name('pbgui.json')
    assert 'pbgui' not in json.loads(native.read_text())
    assert json.loads(gui.read_text())['version'] == 1
    assert stat.S_IMODE(native.stat().st_mode) == 0o600
    assert stat.S_IMODE(gui.stat().st_mode) == 0o600
    loaded = s.client.get('/api/v9/instances/fixture/config').json()['config']
    assert loaded['bot']['long'] == {}
    loaded['pbgui']['note'] = 'second'
    second = _save(s, loaded, expected_version=1)
    assert second.status_code == 200, second.text
    assert second.json()['version'] == 2
    backup = s.root / 'data/backup/v9/fixture/1'
    assert json.loads((backup / 'pbgui.json').read_text())['note'] == ''
    assert (backup / 'config.json').read_text() == native.read_text()
    assert s.client.get('/api/v9/instances').json()['instances'][0]['note'] == 'second'
    stale = _save(s, loaded, expected_version=1)
    assert stale.status_code == 409
    assert json.loads(gui.read_text())['version'] == 2


def test_retired_config_remains_visible_and_explicitly_deletable(run_api):
    """An isolated retired bundle is rejected by the editor, never silently removed."""
    s = run_api
    old = copy.deepcopy(s.config)
    old['bot']['long']['hsl'] = {'enabled': False}
    # The fixture validator models an older runtime which accepted this layout.
    assert _save(s, old).status_code == 200
    directory = s.root / 'data/run_v9/fixture'
    original = (directory / 'config.json').read_bytes()
    rows = s.client.get('/api/v9/instances').json()['instances']
    assert rows[0]['status'] == 'config_error'
    assert 'pre-PR #2318' in rows[0]['load_error']
    assert s.client.get('/api/v9/instances/fixture/config').status_code == 422
    assert (directory / 'config.json').read_bytes() == original
    assert s.client.delete('/api/v9/instances/fixture').status_code == 200
    assert not directory.exists()
    assert (s.root / 'data/backup/v9/fixture/1/config.json').read_bytes() == original


def test_rejects_native_error_and_symlink_without_publication(run_api):
    """Validation failure and unsafe storage cannot replace an existing bundle."""
    s = run_api
    def fail(*args, **kwargs):
        """Model a native semantic rejection without running native code."""
        raise PB9ConfigError('native_config_rejected')
    s.monkeypatch.setattr(api, 'validate_pb9_live_config', fail)
    assert _save(s).status_code == 422
    assert not (s.root / 'data/run_v9/fixture').exists()
    s.monkeypatch.setattr(api, 'validate_pb9_live_config', lambda *a, **k: {})
    outside = s.root / 'outside'
    outside.mkdir()
    (s.root / 'data').mkdir(exist_ok=True)
    (s.root / 'data/run_v9').symlink_to(outside, target_is_directory=True)
    assert _save(s).status_code == 409
    assert list(outside.iterdir()) == []


def test_local_enable_conflict_rejects_other_generation(run_api):
    """An enabled PB8 account prevents PB9 publication for that same account."""
    s = run_api
    other = s.root / 'data/run_v8/fixture'
    other.mkdir(parents=True)
    (other / 'config.json').write_text(json.dumps({'live': {'user': 'fixture'}, 'pbgui': {'enabled_on': 'node'}}))
    config = copy.deepcopy(s.config)
    config['pbgui']['enabled_on'] = 'node'
    response = _save(s, config)
    assert response.status_code == 409, response.text
    assert not (s.root / 'data/run_v9/fixture').exists()


def test_backup_restore_is_disabled_and_monotonic(run_api):
    """A restore publishes a new version and never activates saved host metadata."""
    s = run_api
    assert _save(s).status_code == 200
    cfg = s.client.get('/api/v9/instances/fixture/config').json()['config']
    cfg['pbgui']['note'] = 'later'
    assert _save(s, cfg, expected_version=1).status_code == 200
    response = s.client.post('/api/v9/restore/fixture/1')
    assert response.status_code == 200, response.text
    restored = s.client.get('/api/v9/instances/fixture/config').json()['config']['pbgui']
    assert restored['version'] == 3
    assert restored['enabled_on'] == 'disabled'
    assert restored['note'] == ''


def test_duplicate_payload_and_remote_target_are_rejected(run_api):
    """Strict JSON parsing and local-only assignment apply before disk publication."""
    s = run_api
    response = s.client.put('/api/v9/instances/fixture/config', content='{"config":{},"config":{}}', headers={'Content-Type': 'application/json'})
    assert response.status_code in (400, 422)
    cfg = copy.deepcopy(s.config)
    cfg['pbgui']['enabled_on'] = 'remote'
    assert _save(s, cfg).status_code == 409
    assert not (s.root / 'data/run_v9/fixture').exists()


def test_router_import_allows_old_bridge_during_api_replacement(monkeypatch):
    """An old API can import the new router before its fresh replacement loads the bridge."""
    import importlib.util
    import pb9_config
    monkeypatch.delattr(pb9_config, 'get_pb9_editor_metadata')
    monkeypatch.delattr(pb9_config, 'validate_pb9_live_config')
    spec = importlib.util.spec_from_file_location('api._v9_restart_import_fixture', api.__file__)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.router.routes


def test_editor_filter_preferences_survive_save_copy_and_backup(run_api):
    """The shared form keeps filter controls while native configs stay pure."""
    s = run_api
    preferences = {'market_cap': 25, 'vol_mcap': 4.5, 'tags': ['fixture-tag'],
                   'only_cpt': True, 'notices_ignore': False}
    cfg = copy.deepcopy(s.config)
    cfg['pbgui'].update(preferences)
    assert _save(s, cfg).status_code == 200
    loaded = s.client.get('/api/v9/instances/fixture/config').json()['config']
    assert {k: loaded['pbgui'][k] for k in preferences} == preferences
    copy_response = s.client.put('/api/v9/instances/fixture/copy-config', json={'config': loaded, 'target_user': 'copy'})
    assert copy_response.status_code == 200, copy_response.text
    copied = s.client.get('/api/v9/instances/copy/config').json()['config']
    assert {k: copied['pbgui'][k] for k in preferences} == preferences
    loaded['pbgui']['market_cap'] = 50
    assert _save(s, loaded, expected_version=1).status_code == 200
    response = s.client.post('/api/v9/restore/fixture/1')
    assert response.status_code == 200, response.text
    restored = response.json()['config']
    assert {k: restored['pbgui'][k] for k in preferences} == preferences
    native = json.loads((s.root / 'data/run_v9/fixture/config.json').read_text())
    assert 'pbgui' not in native


@pytest.mark.parametrize('value', [-1, True, 10**100, '25'])
def test_invalid_filter_preferences_are_rejected_without_storage(run_api, value):
    """Malformed GUI preferences cannot enter persisted runtime sidecars."""
    cfg = copy.deepcopy(run_api.config)
    cfg['pbgui']['market_cap'] = value
    assert _save(run_api, cfg).status_code == 422
    assert not (run_api.root / 'data/run_v9/fixture').exists()


@pytest.mark.parametrize('long,short,expected', [(True, False, 'L=1.5'), (False, True, 'S=1.5'),
                                               (False, False, ''), (None, None, '')])
def test_exposure_summary_requires_explicit_side_activation(long, short, expected):
    """Inherited positive capacity never grants v9 side activation in the list."""
    native = {'bot': {'global': {'risk': {'max_positions': 5, 'total_wallet_exposure_limit_ratio': 1.5}},
                      'long': {} if long is None else {'enabled': long},
                      'short': {} if short is None else {'enabled': short}}}
    assert api._exposure_summary(native) == expected


@pytest.fixture
def remote_run(run_api):
    """Attach a joined synthetic PB9 target through the production shared monitor contract."""
    import time
    from api import v8_instances as shared
    from master.cluster_state import ensure_local_identity, append_operation
    from tests.test_pb8_cluster_sync import CLUSTER_ID, NODE_A, NODE_B
    s = run_api
    root = s.root / 'data/cluster'
    ensure_local_identity(root, role='master', pbname='node', cluster_id=CLUSTER_ID, node_id=NODE_A)
    append_operation(root, 'ADD_NODE', {'node_id': NODE_A, 'role': 'master', 'pbname': 'node'})
    nodes = {NODE_A: {'pbname': 'node', 'role': 'master'},
             NODE_B: {'pbname': 'remote', 'role': 'vps', 'sync_mode': 'reachable', 'state_replica': True}}
    s.monkeypatch.setattr(shared, '_cluster_nodes', lambda *a, **k: nodes)
    (root / 'sync_status.json').write_text(json.dumps({'finished_at': int(time.time()),
        'peers': [{'node_id': NODE_B, 'ok': True, 'pb9_capability': True}]}))
    store = SimpleNamespace(host_meta={'remote': {'generated_at': time.time(), 'pb9ready': True,
                                                 'pb9_config_schema': 'v9.0.0'}},
                            v9_instances={'remote': []}, v9_snapshot_times={})
    s.monitor = SimpleNamespace(store=store)
    s.monkeypatch.setattr(shared, '_monitor', s.monitor)
    s.config['pbgui']['enabled_on'] = 'remote'
    return s


def test_remote_save_publishes_signed_native_manifest(remote_run):
    """Remote placement uses the existing cluster oplog without embedding GUI metadata."""
    from master.cluster_state import load_operations, rebuild_materialized_state
    s = remote_run
    response = _save(s)
    assert response.status_code == 200, response.text
    assert response.json()['sync']['pending'] is True
    assert response.json()['config']['pbgui']['cluster_managed'] is True
    ops = load_operations(s.root / 'data/cluster')
    assert ops[-1]['op'] == 'UPSERT_PB9_CONFIG' and ops[-1].get('signature')
    desired = rebuild_materialized_state(s.root / 'data/cluster')['desired_state']
    assert desired['pb9_instances']['fixture']['desired_state'] == 'running'
    assert desired.get('pb8_instances') == {}
    assert 'pbgui' not in json.loads((s.root / 'data/run_v9/fixture/config.json').read_text())


@pytest.mark.parametrize('problem', ['stale', 'pb8_only', 'schema', 'no_protocol'])
def test_remote_target_requires_fresh_pb9_runtime_and_protocol(remote_run, problem):
    """A stale agent or old replica cannot accept a newly enabled native instance."""
    s = remote_run
    meta = s.monitor.store.host_meta['remote']
    if problem == 'stale': meta['generated_at'] = 1
    if problem == 'pb8_only': meta.pop('pb9ready'); meta['pb8ready'] = True
    if problem == 'schema': meta['pb9_config_schema'] = 'v8.6.0'
    if problem == 'no_protocol':
        path = s.root / 'data/cluster/sync_status.json'
        state = json.loads(path.read_text()); state['peers'][0]['pb9_capability'] = False
        path.write_text(json.dumps(state))
    response = _save(s)
    assert response.status_code == 409, response.text
    assert not (s.root / 'data/run_v9/fixture/config.json').exists()


@pytest.mark.parametrize('existed', [False, True])
def test_cluster_append_failure_restores_previous_complete_bundle(run_api, existed):
    """A failed durable publication never reports a new config or leaves a mixed bundle."""
    s = run_api
    target = s.root / 'data/run_v9/fixture'
    previous = {}
    if existed:
        response = _save(s)
        assert response.status_code == 200
        s.config = response.json()['config']
        previous = {p.name: p.read_bytes() for p in target.iterdir()}
        s.config['pbgui']['note'] = 'must not survive'
    def fail(*a, **k):
        """Model a disk/oplog failure before a signed operation exists."""
        raise OSError('synthetic append failure')
    s.monkeypatch.setattr(api, '_publish_cluster_bundle', fail)
    response = _save(s)
    assert response.status_code == 500
    if existed:
        assert {p.name: p.read_bytes() for p in target.iterdir()} == previous
    else:
        assert not target.exists()


def test_remote_restart_and_observation_keep_pb9_identity(remote_run):
    """The explicit Restart action sends runtime 9 and exposes the fresh remote version."""
    import time
    s = remote_run
    assert _save(s).status_code == 200
    calls = []
    async def stop(host, name, version):
        """Record the existing transport request without running SSH or signals."""
        calls.append((host, name, version))
        return {'success': True, 'pid': '123'}
    s.monitor.kill_instance = stop
    s.monitor.store.v9_instances['remote'] = [{'name': 'fixture', 'running': True, 'cv': 1, 'rv': 1}]
    s.monitor.store.v9_snapshot_times['remote'] = (time.time(), time.time())
    api._invalidate_instance_list()
    row = s.client.get('/api/v9/instances').json()['instances'][0]
    assert row['status'] == 'synced' and row['running_on'] == ['remote']
    response = s.client.post('/api/v9/instances/fixture/restart')
    assert response.status_code == 200, response.text
    assert calls == [('remote', 'fixture', '9')]
    s.monitor.store.v9_snapshot_times.clear()
    assert s.client.post('/api/v9/instances/fixture/restart').status_code == 409
    assert len(calls) == 1


@pytest.mark.parametrize('remote_result', ['fresh remote log', '', None])
def test_remote_log_never_selects_leftover_local_log(remote_run, remote_result):
    """A moved instance stays on its assigned host even before remote logs arrive."""
    from api import vps
    s = remote_run
    _save(s)
    path = s.root / 'data/run_v9/fixture/passivbot_err.log'
    path.write_text('old local output')
    calls = []

    async def remote_log(host, name, **kwargs):
        """Model success, an empty remote log, and a transient transport failure."""
        calls.append((host, name, kwargs['pb_version']))
        if remote_result is None:
            raise TimeoutError('synthetic')
        return remote_result

    s.monkeypatch.setattr(vps, 'get_bot_log_tail', remote_log)
    data = s.client.get('/api/v9/instances/fixture/log-smart?lines=1').json()
    assert data['host'] == 'remote'
    assert data['source'] == 'remote'
    assert data['log'] == (remote_result or '')
    assert calls == [('remote', 'fixture', '9')]


def test_local_log_and_disabled_history_remain_readable(run_api):
    """Disabled local post-mortems retain their existing log and backup behavior."""
    s = run_api
    _save(s)
    path = s.root / 'data/run_v9/fixture/passivbot_err.log'
    path.write_text('local output')
    data = s.client.get('/api/v9/instances/fixture/log-smart?lines=1').json()
    assert data['host'] == 'node' and data['log'] == 'local output'


def test_blocked_remote_host_reports_disabled_selection(remote_run):
    """A ready target with failed Cluster checks carries a non-selectable reason."""
    s = remote_run
    s.monkeypatch.setattr(api, '_local_capability', lambda *_: {
        'pb9_capable': True, 'config_schema': 'v9.0.0'})
    def blocked(*args):
        """Keep placement blocked without changing runtime readiness."""
        raise api.HTTPException(409, 'PB9 materialization blocked')
    s.monkeypatch.setattr(api, '_cluster_context', blocked)
    data = s.client.get('/api/v9/hosts?config_schema=v9.0.0').json()
    assert 'remote' not in data['hosts']
    capability = data['host_capabilities']['remote']
    assert capability['pb9_capable'] is True
    assert capability['target_eligible'] is False
    assert capability['reason'] == 'PB9 materialization blocked'


def test_remote_delete_requires_disabled_and_fresh_stopped_proof(remote_run):
    """Stale remote state never authorizes removal of the local source or a tombstone."""
    import time
    from master.cluster_state import load_operations
    s = remote_run
    saved = _save(s).json()['config']
    assert s.client.delete('/api/v9/instances/fixture').status_code == 409
    saved['pbgui']['enabled_on'] = 'disabled'
    assert _save(s, saved).status_code == 200
    assert s.client.delete('/api/v9/instances/fixture').status_code == 409
    s.monitor.store.v9_instances['remote'] = [{'name': 'fixture', 'running': False, 'cv': 2, 'rv': 0}]
    s.monitor.store.v9_snapshot_times['remote'] = (time.time(), time.time())
    response = s.client.delete('/api/v9/instances/fixture')
    assert response.status_code == 200, response.text
    assert load_operations(s.root / 'data/cluster')[-1]['op'] == 'DELETE_PB9_INSTANCE'
    assert not (s.root / 'data/run_v9/fixture').exists()


def test_disable_existing_managed_instance_is_not_blocked_by_offline_peer(remote_run):
    """A failed replica must not prevent publishing a stop intent for an existing PB9 run."""
    s = remote_run
    saved = _save(s).json()['config']
    path = s.root / 'data/cluster/sync_status.json'
    state = json.loads(path.read_text()); state['peers'][0].update(ok=False, status='backoff')
    path.write_text(json.dumps(state))
    saved['pbgui']['enabled_on'] = 'disabled'
    response = _save(s, saved)
    assert response.status_code == 200, response.text
    assert response.json()['config']['pbgui']['cluster_managed'] is True
    assert response.json()['sync']['pending'] is True


def test_new_save_preserves_open_log_inode(run_api):
    """A bot's open stderr descriptor continues writing to the published bundle."""
    s = run_api
    saved = _save(s).json()['config']
    path = s.root / 'data/run_v9/fixture/passivbot_err.log'
    with path.open('w') as stream:
        stream.write('before\n'); stream.flush()
        inode = path.stat().st_ino
        saved['pbgui']['note'] = 'next version'
        response = _save(s, saved)
        assert response.status_code == 200, response.text
        stream.write('after\n'); stream.flush()
    assert path.stat().st_ino == inode
    assert path.read_text() == 'before\nafter\n'


def test_native_restart_rejects_blocked_remote_gate(remote_run):
    """Restart never quietly becomes only Stop when the collector reports a blocked launch."""
    import time
    s = remote_run
    assert _save(s).status_code == 200
    s.monitor.store.v9_instances['remote'] = [{'name': 'fixture', 'running': True, 'blocked': True}]
    s.monitor.store.v9_snapshot_times['remote'] = (time.time(), time.time())
    response = s.client.post('/api/v9/instances/fixture/restart')
    assert response.status_code == 409


def test_daemon_snapshot_roundtrip_preserves_pb9_rows_and_freshness(run_api):
    """The real RPC store proxy must retain PB9 data after JSON tuple/list conversion."""
    import time
    from api import v8_instances as shared
    from master.async_store import VPSStore
    from master.vps_monitor_client import VPSStoreProxy
    store = VPSStore()
    store.update_v9_instances('remote', [{'name': 'fixture', 'running': False}],
                              snapshot_generated_at=time.time(), snapshot_checked_at=time.time())
    snapshot = json.loads(json.dumps(store.get_full_state({}, [])))
    proxy = VPSStoreProxy(lambda *a: None)
    proxy.hydrate(snapshot)
    run_api.monkeypatch.setattr(shared, '_monitor', SimpleNamespace(store=proxy))
    assert api._remote_rows('fixture')['remote']['running'] is False
    assert api._remote_snapshot_fresh('remote') is True
    proxy.hydrate({'v8_instances': {'remote': [{'name': 'fixture', 'running': False}]}})
    assert api._remote_rows('fixture') == {}
    assert api._remote_snapshot_fresh('remote') is False


def test_remote_disable_keeps_placement_until_stop_and_move_is_verified(remote_run):
    """Moving requires a disabled, observed-stopped source; disabling retains its old node."""
    import time
    from master.cluster_state import read_materialized_state
    from tests.test_pb8_cluster_sync import NODE_B
    s = remote_run
    saved = _save(s).json()['config']
    moving = copy.deepcopy(saved); moving['pbgui']['enabled_on'] = 'node'
    import threading
    with pytest.raises(api.HTTPException, match='Disable the PB9 instance'):
        api._save('fixture', moving, expected_version=1, cancel_event=threading.Event())
    saved['pbgui']['enabled_on'] = 'disabled'
    disabled = _save(s, saved)
    assert disabled.status_code == 200
    state = read_materialized_state(s.root / 'data/cluster')['desired_state']['pb9_instances']['fixture']
    assert state['assigned_host'] == NODE_B and state['desired_state'] == 'stopped'
    moving = disabled.json()['config']; moving['pbgui']['enabled_on'] = 'node'
    assert _save(s, moving).status_code == 409
    s.monitor.store.v9_instances['remote'] = [{'name':'fixture','running':False,'cv':2,'rv':0}]
    s.monitor.store.v9_snapshot_times['remote'] = (time.time(), time.time())
    assert _save(s, moving).status_code == 200


def test_editor_move_remote_to_local_waits_for_disabled_generation(remote_run):
    """One Save delivers Stop to the source before publishing the target assignment."""
    import time
    s = remote_run
    saved = _save(s).json()['config']
    s.monitor.store.v9_instances['remote'] = [{'name': 'fixture', 'running': True, 'cv': 1, 'rv': 1}]
    def stopped(operation):
        """Simulate the source's observation of this exact disabled generation."""
        if operation['desired_state'] == 'stopped':
            version = int(operation['version'])
            s.monitor.store.v9_instances['remote'] = [{'name': 'fixture', 'running': False, 'cv': version, 'rv': 0}]
            s.monitor.store.v9_snapshot_times['remote'] = (time.time(), time.time())
    s.activation_callback = stopped
    saved['pbgui']['enabled_on'] = 'node'
    result = _save(s, saved)
    assert result.status_code == 200, result.text
    assert result.json()['move'] == {'from': 'remote', 'to': 'node', 'stopped_version': 2}
    assert result.json()['config']['pbgui']['enabled_on'] == 'node'
    assert [(op['version'], op['enabled_on']) for op in s.activations] == [('1', 'remote'), ('2', 'disabled'), ('3', 'node')]


def test_editor_move_local_to_remote_retains_remote_log_target(remote_run):
    """Local Log, a host change and Save cannot redirect the resulting remote log."""
    from api import vps
    s = remote_run
    s.config['pbgui']['enabled_on'] = 'node'
    saved = _save(s).json()['config']
    path = s.root / 'data/run_v9/fixture/passivbot_err.log'
    path.write_text('previous local output')
    assert s.client.get('/api/v9/instances/fixture/log-smart').json()['host'] == 'node'
    running = [True]
    s.monkeypatch.setattr(api, '_runner', lambda *a, **k: SimpleNamespace(pid=lambda: object() if running[0] else None))
    def stopped(operation):
        """Model PBRun stopping after it receives the disabled source config."""
        if operation['desired_state'] == 'stopped':
            running[0] = False
    s.activation_callback = stopped
    saved['pbgui']['enabled_on'] = 'remote'
    result = _save(s, saved)
    assert result.status_code == 200, result.text
    assert result.json()['version'] == 3
    assert result.json()['config']['pbgui']['enabled_on'] == 'remote'
    async def no_remote_output(*a, **k):
        """No remote log exists yet; do not contact any host."""
        return ''
    s.monkeypatch.setattr(vps, 'get_bot_log_tail', no_remote_output)
    log = s.client.get('/api/v9/instances/fixture/log-smart').json()
    assert log['host'] == 'remote' and log['log'] == ''


@pytest.mark.parametrize('snapshot_version', [1, 2])
def test_move_timeout_keeps_source_disabled_and_retry_requires_current_proof(remote_run, snapshot_version):
    """A stale stopped row cannot authorize transfer, including a later Save retry."""
    import time
    s = remote_run
    saved = _save(s).json()['config']
    s.monkeypatch.setattr(api, '_MOVE_STOP_TIMEOUT_SECONDS', 0)
    s.monitor.store.v9_instances['remote'] = [{'name': 'fixture', 'running': False, 'cv': 1, 'rv': 0}]
    s.monitor.store.v9_snapshot_times['remote'] = (time.time(), time.time())
    saved['pbgui']['enabled_on'] = 'node'
    result = _save(s, saved)
    assert result.status_code == 409
    assert result.headers['X-PB9-Saved-Version'] == '2'
    assert s.client.get('/api/v9/instances/fixture/config').json()['config']['pbgui']['enabled_on'] == 'disabled'
    assert [op['enabled_on'] for op in s.activations] == ['remote', 'disabled']
    import shutil
    shutil.rmtree(s.root / 'data/backup/v9/fixture')
    s.monitor.store.v9_instances['remote'][0]['cv'] = snapshot_version
    retry = _save(s, saved, expected_version=2)
    assert retry.status_code == (200 if snapshot_version == 2 else 409)


def test_invalid_move_does_not_disable_source(remote_run):
    """All requested input is validated before the first durable stop command."""
    s = remote_run
    saved = _save(s).json()['config']
    saved['pbgui'].update(enabled_on='node', note='x' * 2001)
    assert _save(s, saved).status_code == 422
    assert len(s.activations) == 1
    current = s.client.get('/api/v9/instances/fixture/config').json()['config']['pbgui']
    assert current['version'] == 1 and current['enabled_on'] == 'remote'


def test_concurrent_save_cancels_move_without_overwriting_newer_state(remote_run):
    """An intervening writer keeps ownership even while a move waits for Stop."""
    import threading
    s = remote_run
    saved = _save(s).json()['config']
    def another_save(operation):
        """Publish a newer disabled edit without invoking transport recursively."""
        if operation['desired_state'] == 'stopped':
            other = copy.deepcopy(saved)
            other['pbgui'].update(enabled_on='disabled', note='another editor')
            api._save('fixture', other, expected_version=2, cancel_event=threading.Event())
    s.activation_callback = another_save
    saved['pbgui']['enabled_on'] = 'node'
    result = _save(s, saved)
    assert result.status_code == 409
    assert 'X-PB9-Saved-Version' not in result.headers
    current = s.client.get('/api/v9/instances/fixture/config').json()['config']['pbgui']
    assert current['version'] == 3 and current['note'] == 'another editor'
    assert current['enabled_on'] == 'disabled'


def test_cancelled_move_never_publishes_target(remote_run):
    """API shutdown leaves the durable source Stop in force without a target start."""
    import threading
    s = remote_run
    saved = _save(s).json()['config']
    cancel = threading.Event()
    s.activation_callback = lambda operation: cancel.set() if operation['desired_state'] == 'stopped' else None
    saved['pbgui']['enabled_on'] = 'node'
    with pytest.raises(PB9ConfigError):
        api._save_from_editor('fixture', saved, expected_version=1, cancel_event=cancel, create_only=False)
    current = s.client.get('/api/v9/instances/fixture/config').json()['config']['pbgui']
    assert current['version'] == 2 and current['enabled_on'] == 'disabled'
    assert [op['enabled_on'] for op in s.activations] == ['remote', 'disabled']


def test_unmaterialized_stop_cannot_reuse_old_stopped_observation(remote_run):
    """A durable Stop still needs its matching desired generation before transfer."""
    import time
    from master import cluster_state
    s = remote_run
    saved = _save(s).json()['config']
    def deferred(*args, **kwargs):
        """Leave the previous desired-state generation in place."""
        raise RuntimeError('synthetic deferred rebuild')
    s.monkeypatch.setattr(cluster_state, 'rebuild_materialized_state', deferred)
    s.monkeypatch.setattr(api, '_MOVE_STOP_TIMEOUT_SECONDS', 0)
    s.monitor.store.v9_instances['remote'] = [{'name': 'fixture', 'running': False, 'cv': 1, 'rv': 0}]
    s.monitor.store.v9_snapshot_times['remote'] = (time.time(), time.time())
    saved['pbgui']['enabled_on'] = 'node'
    result = _save(s, saved)
    assert result.status_code == 409
    assert [op['enabled_on'] for op in s.activations] == ['remote', 'disabled']
