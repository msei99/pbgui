"""Offline collect-only API contracts: Pareto publication must never stop work."""

from types import SimpleNamespace
from unittest.mock import Mock

import msgpack
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api import vast
from api.auth import require_auth
from secure_files import ensure_private_directory
from vast_jobs import IMAGE, REVISION, JobStore, write_json
from vast_provider import VastError
import vast_credentials
import vast_transfer


@pytest.fixture
def collection(tmp_path, monkeypatch):
    """Keep all jobs/results local and replace every provider/SSH operation."""
    identifier, lease_id = 'a' * 32, 'b' * 32
    store = JobStore(tmp_path / 'cloud')
    for key in (identifier, lease_id):
        directory = ensure_private_directory(store.root / 'jobs' / key)
        write_json(directory / 'state.json', dict(id=key, kind='optimizer', status='running',
                   rental_state='active', instance_id=123, exact_completed=12,
                   lease_id=lease_id if key == identifier else None, final_collected=False))
        write_json(directory / 'control.json', dict(stop=False, cleanup=False))
        write_json(directory / 'intent.json', dict(id=key, label='pbgui-vast-' + key,
                   image=IMAGE, pb8_revision=REVISION, accepted_at=100, deadline=3700,
                   budget_usd=1, offer=dict(id=1), results_root=str(tmp_path / 'optimize_results'),
                   source_config_sha256='c' * 64))
    write_json(ensure_private_directory(store.directory(identifier) / 'input') / 'manifest.json', {})
    client = Mock()
    client.instances.return_value = [dict(id=123, label='pbgui-vast-' + lease_id,
        actual_status='running', public_ipaddr='8.8.8.8', ports={'22/tcp': [{'HostPort': '2222'}]})]
    client.request.side_effect = AssertionError('Collection must not call provider mutation APIs')
    monkeypatch.setattr(vast_transfer, 'VastClient', lambda _key: client)
    monkeypatch.setattr(vast_credentials, 'VastCredentialStore',
        lambda _root: SimpleNamespace(secrets=lambda: {'api_key': 'isolated-test-key'}))
    monkeypatch.setattr(vast, 'JobStore', lambda: store)
    bootstrap = Mock()
    monkeypatch.setattr(vast_transfer.WorkerConnection, 'bootstrap', bootstrap)
    operation = Mock(side_effect=AssertionError('Collection must not stop the worker'))
    monkeypatch.setattr(vast_transfer.WorkerConnection, 'operation', operation)
    control = Mock(side_effect=AssertionError('Collection must not change stop/cleanup controls'))
    monkeypatch.setattr(store, 'control', control)

    def snapshot(connection, final, timeout):
        """Publish a lightweight worker snapshot, never a terminal marker."""
        assert final is False
        assert 0 < timeout <= 60
        source = ensure_private_directory(connection.directory / 'partial-results/optimize_results/run/pareto')
        write_json(source / 'candidate.json', {'config': {'name': 'test-candidate'}})
        store.update(identifier, last_backup_at=12345)
        return source.parent.parent

    collect = Mock()
    def collect_snapshot(connection, final, timeout):
        """Track real collect arguments while using an isolated snapshot."""
        collect(final=final, timeout=timeout)
        return snapshot(connection, final, timeout)
    monkeypatch.setattr(vast_transfer.WorkerConnection, 'collect', collect_snapshot)
    app = FastAPI()
    app.include_router(vast.router, prefix='/api/vast')
    app.dependency_overrides[require_auth] = lambda: object()
    with TestClient(app) as http:
        yield SimpleNamespace(http=http, store=store, identifier=identifier, lease_id=lease_id,
            provider=client, bootstrap=bootstrap, operation=operation, control=control,
            collect=collect, app=app)


@pytest.mark.parametrize('shared', [False, True])
@pytest.mark.parametrize('status', ['running', 'completed'])
def test_collect_publishes_pareto_without_stop_and_is_idempotent(collection, status, shared):
    """Running and finished jobs keep execution/rental state and one result entry."""
    c = collection
    c.store.update(c.identifier, status=status)
    if not shared:
        c.store.update(c.identifier, lease_id=None)
        c.provider.instances.return_value[0]['label'] = 'pbgui-vast-' + c.identifier
    original_control = c.store.read(c.identifier, 'control.json')
    url = '/api/vast/jobs/' + c.identifier + '/collect'
    first = c.http.post(url)
    assert first.status_code == 200, first.text
    result = first.json()
    assert result['status'] == status and result['rental_state'] == 'active'
    assert result['result_partial'] is True and result['final_collected'] is False
    assert result['pareto_count'] == 1
    c.collect.assert_called_once_with(final=False, timeout=60)
    c.bootstrap.assert_called_once()
    second = c.http.post(url)
    assert second.status_code == 200, second.text
    assert second.json()['result_path'] == result['result_path']
    assert len([path for path in (c.store.root.parent / 'optimize_results').iterdir() if path.is_dir()]) == 1
    assert c.store.read(c.identifier, 'control.json') == original_control
    assert c.store.read(c.lease_id, 'control.json') == original_control
    assert c.store.read(c.lease_id)['status'] == 'running'
    c.operation.assert_not_called()
    c.control.assert_not_called()
    c.provider.request.assert_not_called()


def test_collect_without_pareto_returns_readable_conflict(collection, monkeypatch):
    """An empty snapshot does not publish or mutate execution/rental controls."""
    c = collection
    monkeypatch.setattr(vast_transfer.WorkerConnection, 'collect', Mock())
    response = c.http.post('/api/vast/jobs/' + c.identifier + '/collect')
    assert response.status_code == 409
    assert 'No Pareto results are ready yet' in response.json()['detail']
    assert 'result_path' not in c.store.read(c.identifier)
    c.control.assert_not_called()
    c.provider.request.assert_not_called()


@pytest.mark.parametrize('failure', ['missing', 'offline', 'ssh', 'cleanup'])
def test_collect_unreachable_instance_retains_previous_results(collection, monkeypatch, failure):
    """Missing instances, transport errors and pending cleanup are safe failures."""
    c = collection
    c.store.update(c.identifier, result_path='/previous/result')
    if failure == 'missing':
        c.provider.instances.return_value = []
    elif failure == 'offline':
        c.provider.instances.return_value[0]['actual_status'] = 'offline'
    elif failure == 'cleanup':
        write_json(c.store.directory(c.lease_id) / 'control.json', dict(stop=False, cleanup=True))
    else:
        monkeypatch.setattr(vast_transfer.WorkerConnection, 'bootstrap', Mock(side_effect=VastError('SSH connection refused')))
    response = c.http.post('/api/vast/jobs/' + c.identifier + '/collect')
    assert response.status_code == (502 if failure == 'ssh' else 409)
    assert response.json()['detail']
    assert c.store.read(c.identifier)['result_path'] == '/previous/result'
    assert c.store.read(c.identifier)['status'] == 'running'
    c.collect.assert_not_called()
    c.control.assert_not_called()
    c.provider.request.assert_not_called()


def test_collect_reuses_final_snapshot_without_provider_or_finalizer(collection, monkeypatch):
    """Already verified final data imports locally without triggering lifecycle work."""
    c = collection
    directory = c.store.directory(c.identifier)
    source = ensure_private_directory(directory / 'final-results/optimize_results/run/pareto')
    write_json(source / 'candidate.json', {'config': {}})
    (source.parent / 'all_results.bin').write_bytes(msgpack.packb({'metrics': {}}))
    c.store.update(c.identifier, final_collected=True, status='completed')
    import vast_job_runner
    finalize = Mock(side_effect=AssertionError('Collect-only must not run lifecycle finalization'))
    monkeypatch.setattr(vast_job_runner, 'finalize_collected_results', finalize)
    url = '/api/vast/jobs/' + c.identifier + '/collect'
    first, second = c.http.post(url), c.http.post(url)
    assert first.status_code == second.status_code == 200
    assert first.json()['result_path'] == second.json()['result_path']
    assert first.json()['result_partial'] is False
    assert first.json()['status'] == 'completed'
    imported = vast_transfer.import_results(store=c.store, identifier=c.identifier)
    assert imported['result_path'] == first.json()['result_path']
    c.collect.assert_not_called()
    c.provider.instances.assert_not_called()
    c.control.assert_not_called()
    finalize.assert_not_called()


def test_collect_requires_auth_and_valid_job(collection):
    """A collect endpoint exposes neither anonymous nor invalid job operations."""
    c = collection
    assert c.http.post('/api/vast/jobs/unknown/collect').status_code == 422
    c.app.dependency_overrides.clear()
    assert c.http.post('/api/vast/jobs/' + c.identifier + '/collect').status_code == 401
    c.collect.assert_not_called()


def test_collect_unexpected_transfer_error_is_readable(collection, monkeypatch):
    """Technical snapshot failures become safe errors without changing job controls."""
    c = collection
    monkeypatch.setattr(vast_transfer.WorkerConnection, 'collect', Mock(side_effect=RuntimeError('isolated snapshot failure')))
    response = c.http.post('/api/vast/jobs/' + c.identifier + '/collect')
    assert response.status_code == 502
    assert 'previous results are retained' in response.json()['detail']
    assert c.store.read(c.identifier)['status'] == 'running'
    assert not c.store.read(c.identifier)['final_collected']
    c.control.assert_not_called()
    c.provider.request.assert_not_called()
