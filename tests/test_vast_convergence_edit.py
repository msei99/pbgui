"""Live stopping edits use isolated job state, never real rentals or runtime data."""
import copy
from concurrent.futures import ThreadPoolExecutor, TimeoutError
import threading

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api import vast
from api.auth import require_auth
from vast_convergence import DEFAULTS, advance, observe_points, update_config
from vast_jobs import JobStore, write_json
from vast_provider import VastError


@pytest.fixture
def editable_job(tmp_path, monkeypatch):
    """Build a running ordinary job and immutable input with stopping disabled."""
    monkeypatch.setattr('vast_convergence._log', lambda *args, **kwargs: None)
    store = JobStore(tmp_path)
    identifier = 'a' * 32
    directory = store.root / 'jobs' / identifier
    directory.mkdir(parents=True)
    write_json(directory / 'state.json', {'id': identifier, 'status': 'running', 'lease_id': 'b' * 32,
        'exact_completed': 9000, 'convergence_config': dict(DEFAULTS),
        'convergence': {'phase': 'waiting'}, 'iterations': 10000000})
    write_json(directory / 'control.json', {'stop': False, 'cleanup': False})
    write_json(directory / 'intent.json', {'convergence_config': dict(DEFAULTS), 'immutable': 'input'})
    return store, identifier


def test_enable_uses_new_verified_baseline_and_can_disable(editable_job):
    """Enabling cannot immediately stop for work done before the user enabled it."""
    store, identifier = editable_job
    intent = store.read(identifier, 'intent.json')
    enabled = {**DEFAULTS, 'convergence_enabled': True}
    row = update_config(store, identifier, enabled, DEFAULTS)
    assert row['convergence'] == {}
    assert row['iterations'] == 10000000
    assert not observe_points(store, identifier, [[1, 1]], 9000)
    assert store.read(identifier)['convergence']['last_improvement_exact'] == 9000
    history = store.read(identifier)['convergence']
    assert update_config(store, identifier, enabled, DEFAULTS)['convergence'] == history
    assert JobStore(store.root).read(identifier)['convergence_config'] == enabled
    assert not store.read(identifier, 'control.json')['stop']
    update_config(store, identifier, DEFAULTS, enabled)
    assert not observe_points(store, identifier, [[1, 1]], 10000)
    assert store.read(identifier, 'intent.json') == intent
    assert store.read(identifier)['convergence'] == {}


@pytest.mark.parametrize('change', [
    {'status': 'completed'}, {'status': 'ready'}, {'status': 'provisioning'},
    {'kind': 'worker'}, {'kind': 'calibration'}, {'loop_id': 'protected-loop'},
    {'stop_reason': 'requested'}, {'convergence': {'stop_requested': True}},
])
def test_reject_ineligible_or_stopping_jobs(editable_job, change):
    """Research budgets, terminal results and an existing stop cannot be changed."""
    store, identifier = editable_job
    store.update(identifier, **change)
    before = store.read(identifier)
    with pytest.raises(VastError):
        update_config(store, identifier, {**DEFAULTS, 'convergence_enabled': True}, DEFAULTS)
    assert store.read(identifier) == before


def test_stale_edit_cannot_replace_another_user_edit(editable_job):
    """A reviewed old policy cannot silently overwrite a newer policy."""
    store, identifier = editable_job
    enabled = {**DEFAULTS, 'convergence_enabled': True}
    update_config(store, identifier, enabled, DEFAULTS)
    with pytest.raises(VastError, match='changed elsewhere'):
        update_config(store, identifier, {**enabled, 'convergence_patience': 1024}, DEFAULTS)
    assert store.read(identifier)['convergence_config'] == enabled


def test_settings_edit_serializes_with_inflight_observation(editable_job, monkeypatch):
    """An old in-flight observation must not restore history after a settings reset."""
    store, identifier = editable_job
    enabled = {**DEFAULTS, 'convergence_enabled': True}
    update_config(store, identifier, enabled, DEFAULTS)
    entered, release = threading.Event(), threading.Event()

    def blocked_advance(*args):
        entered.set()
        assert release.wait(5)
        return advance(*args)

    monkeypatch.setattr('vast_convergence.advance', blocked_advance)
    with ThreadPoolExecutor(max_workers=2) as executor:
        observed = executor.submit(observe_points, store, identifier, [[1, 1]], 9000)
        assert entered.wait(5)
        edit_started = threading.Event()

        def edit():
            edit_started.set()
            return update_config(store, identifier, DEFAULTS, enabled)

        edited = executor.submit(edit)
        try:
            assert edit_started.wait(5)
            with pytest.raises(TimeoutError):
                edited.result(timeout=.1)
        finally:
            release.set()
        assert observed.result(timeout=5) is False
        edited.result(timeout=5)
    assert store.read(identifier)['convergence_config'] == DEFAULTS
    assert store.read(identifier)['convergence'] == {}


def test_api_requires_auth_validates_policy_and_returns_current_state(editable_job, monkeypatch):
    """Only authenticated, strictly validated and current edits reach the controller."""
    store, identifier = editable_job
    monkeypatch.setattr(vast, 'JobStore', lambda: store)
    app = FastAPI()
    app.include_router(vast.router, prefix='/api/vast')
    client = TestClient(app)
    payload = {'config': {**DEFAULTS, 'convergence_enabled': True}, 'expected': dict(DEFAULTS)}
    assert client.post(f'/api/vast/jobs/{identifier}/convergence', json=payload).status_code == 401
    app.dependency_overrides[require_auth] = lambda: object()
    invalid = copy.deepcopy(payload)
    invalid['config']['convergence_patience'] = '512'
    assert client.post(f'/api/vast/jobs/{identifier}/convergence', json=invalid).status_code == 422
    response = client.post(f'/api/vast/jobs/{identifier}/convergence', json=payload)
    assert response.status_code == 200
    assert response.headers['cache-control'] == 'no-store'
    assert response.json()['convergence_config']['convergence_enabled'] is True
    assert client.post(f'/api/vast/jobs/{identifier}/convergence', json=payload).status_code == 200


@pytest.mark.parametrize('action', ['stop', 'cleanup'])
def test_pending_control_request_prevents_settings_reset(editable_job, action):
    """A stop or cleanup already requested remains authoritative during an edit."""
    store, identifier = editable_job
    write_json(store.directory(identifier) / 'control.json', {'stop': action == 'stop', 'cleanup': action == 'cleanup'})
    with pytest.raises(VastError):
        update_config(store, identifier, {**DEFAULTS, 'convergence_enabled': True}, DEFAULTS)
    assert store.read(identifier, 'control.json')[action] is True
