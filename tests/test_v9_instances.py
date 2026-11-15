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
    with TestClient(app) as client:
        yield SimpleNamespace(client=client, config=config, root=tmp_path, monkeypatch=monkeypatch)


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
