"""Offline host exclusions across persistence, marketplace and rental boundaries."""
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from api import vast
from api.auth import require_auth
from vast_jobs import IMAGE, REVISION, JobStore, write_json
from vast_provider import VastClient, VastError
from vast_queue import CloudQueue, blocked_machine_ids


@pytest.fixture
def queue(tmp_path, monkeypatch):
    """Isolate every local read/write and forbid real provider credentials."""
    result = CloudQueue(JobStore(tmp_path / 'vast'))
    monkeypatch.setattr(vast, 'CloudQueue', lambda: result)
    monkeypatch.setattr(vast, 'VastCredentialStore', lambda *args: SimpleNamespace(secrets=lambda: {'api_key': 'fake'}))
    return result


def test_host_blocks_merge_persist_and_unblock(queue):
    """Concurrent exclusions survive reload and preserve unrelated settings."""
    queue.update(paused=True, gpu_preferences={'gpu_name': 'RTX 5090'})
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(lambda machine: queue.set_host_block(machine, True), range(1, 21)))
    assert blocked_machine_ids(CloudQueue(JobStore(queue.root)).read()) == list(range(1, 21))
    queue.set_host_block(7, False)
    queue.set_host_block(7, False)
    assert 7 not in blocked_machine_ids(queue.read())
    assert queue.read()['paused'] is True
    assert queue.read()['gpu_preferences'] == {'gpu_name': 'RTX 5090'}


@pytest.mark.parametrize('invalid', [None, '7', True, -1, 0, 1.5])
def test_block_identifiers_are_strict(queue, invalid):
    """Never coerce a malformed identifier to a different physical host."""
    with pytest.raises(VastError):
        queue.set_host_block(invalid, True)


def test_corrupt_exclusions_fail_closed(queue):
    """Invalid stored exclusions must not silently enable blocked hosts."""
    queue.update(blocked_machine_ids=['7'])
    with pytest.raises(VastError, match='cannot be read'):
        queue.start({'id': 42, 'machine_id': 7}, 1, 1, 300, manual=True)


def test_provider_filter_and_recheck(monkeypatch):
    """Block all offers for one machine even when Vast ignores its filter."""
    calls = []

    def provider(self, method, path, body):
        """Return changing offer IDs, a good host and unknown machine identity."""
        calls.append(body)
        return {'offers': [{'id': i, 'machine_id': machine, 'dph_total': .2}
                           for i, machine in [(1, 7), (2, 7), (3, 8), (4, None), (5, True)]]}

    monkeypatch.setattr(VastClient, 'request', provider)
    assert [row['id'] for row in VastClient('fake').offers(excluded_machine_ids=[7])] == [3]
    assert calls[0]['machine_id'] == {'notin': [7]}
    assert len(VastClient('fake').offers()) == 5


@pytest.mark.parametrize('machine', [7, None, True])
def test_queue_rechecks_exclusions_before_creating_worker(queue, monkeypatch, machine):
    """A stale preview cannot bypass a newly saved exclusion."""
    queue.set_host_block(7, True)
    starts = []
    monkeypatch.setattr(queue.store, 'start', lambda *args: starts.append(args))
    with pytest.raises(VastError, match='blocked'):
        queue.start({'id': 42, 'machine_id': machine}, 1, 1, 300, manual=True)
    assert starts == []
    assert queue.read().get('worker_id') is None


@pytest.mark.parametrize('manual', [False, True])
def test_api_cannot_rent_blocked_offer(queue, monkeypatch, manual):
    """Manual and automatic selection recheck even mocked provider responses."""
    queue.set_host_block(7, True)
    monkeypatch.setattr(queue, 'waiting', lambda: [{'workers': 1}])
    row = dict(id=42, machine_id=7, num_gpus=1, gpu_name='RTX 5090', cuda_max_good=13,
               price_hour_usd=.2, vram_gb=32, ram_gb=64, cpu_cores=16, disk_gb=40, verified=True)
    monkeypatch.setattr(vast, 'VastClient', lambda key: SimpleNamespace(offers=lambda **kwargs: [row]))
    body = vast.StartJobRequest(preferences=vast.GpuPreferences(), accept_rental_and_cleanup=True,
                                rent_only=manual, offer_id=42 if manual else None)
    with pytest.raises(HTTPException) as exc:
        vast.start_queue(body, session=None)
    assert exc.value.status_code == 409
    assert queue.read().get('worker_id') is None


def test_host_block_api_and_preview(queue, monkeypatch):
    """Authenticated routes validate requests, filter previews and restore hosts."""
    app = FastAPI()
    app.include_router(vast.router, prefix='/api/vast')
    app.dependency_overrides[require_auth] = lambda: object()
    monkeypatch.setattr(vast, 'VastClient', lambda key: SimpleNamespace(
        offers=lambda **kwargs: [{'id': 1, 'machine_id': 7}, {'id': 2, 'machine_id': 8}]))
    with TestClient(app) as http:
        assert http.post('/api/vast/blocked-hosts', json={'machine_id': True, 'blocked': True}).status_code == 422
        assert http.post('/api/vast/blocked-hosts', json={'machine_id': 7, 'blocked': True}).json() == {'blocked_machine_ids': [7]}
        assert http.get('/api/vast/offers?include_incompatible=true').json()['offers'] == [{'id': 2, 'machine_id': 8, 'host_history': {}}]
        assert http.post('/api/vast/blocked-hosts', json={'machine_id': 7, 'blocked': False}).status_code == 200
        assert len(http.get('/api/vast/offers').json()['offers']) == 2
        app.dependency_overrides.clear()
        assert http.post('/api/vast/blocked-hosts', json={'machine_id': 7, 'blocked': True}).status_code in (401, 403)


def test_auto_selection_uses_allowed_machine(queue, monkeypatch):
    """The cheapest blocked host is skipped in favor of an allowed machine."""
    queue.set_host_block(7, True)
    monkeypatch.setattr(queue, 'waiting', lambda: [{'workers': 1}])
    offer = dict(num_gpus=1, gpu_name='RTX 5090', cuda_max_good=13, vram_gb=32,
                 ram_gb=64, cpu_cores=16, disk_gb=40, verified=True)
    rows = [dict(offer, id=41, machine_id=7, price_hour_usd=.1),
            dict(offer, id=42, machine_id=8, price_hour_usd=.2)]
    monkeypatch.setattr(vast, 'VastClient', lambda key: SimpleNamespace(offers=lambda **kwargs: rows))
    monkeypatch.setattr(queue, 'start', lambda selected, *args: selected)
    body = vast.StartJobRequest(preferences=vast.GpuPreferences(), accept_rental_and_cleanup=True)
    assert vast.start_queue(body, session=None)['machine_id'] == 8


@pytest.mark.parametrize('legacy', [False, True])
def test_block_rental_resolves_machine_without_remote_mutation(queue, monkeypatch, legacy):
    """Old rentals resolve through ownership; new rentals use saved metadata."""
    identifier = 'a' * 32
    folder = queue.root / 'jobs' / identifier
    folder.mkdir(parents=True)
    intent = dict(id=identifier, image=IMAGE, pb8_revision=REVISION, label='pbgui-vast-' + identifier,
                  accepted_at=1000, deadline=2000, budget_usd=1, offer={'id': 42})
    if not legacy:
        intent['offer']['machine_id'] = 7
    write_json(folder / 'intent.json', intent)
    write_json(folder / 'state.json', dict(id=identifier, status='provisioning', rental_state='active'))
    write_json(folder / 'control.json', dict(stop=False, cleanup=False))
    calls = []

    def instances(*, fresh):
        """Expose only a read operation with an unrelated instance first."""
        calls.append(fresh)
        return [{'id': 9, 'label': 'unrelated', 'machine_id': 99},
                {'id': 10, 'label': intent['label'], 'machine_id': 7}]

    monkeypatch.setattr(vast, 'VastClient', lambda key: SimpleNamespace(instances=instances))
    result = vast.set_rental_host_block(identifier, vast.HostBlockRequest(blocked=True), session=None)
    assert result == {'machine_id': 7, 'blocked_machine_ids': [7]}
    assert calls == ([True] if legacy else [])
    assert queue.store.read(identifier, 'control.json') == {'stop': False, 'cleanup': False}
    assert vast._rental_details(queue.store, identifier)['offer']['machine_id'] == 7
    assert queue.store.read(identifier, 'intent.json') == intent
