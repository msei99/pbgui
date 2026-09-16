"""Offline evidence, priority and hard-limit contracts for known GPU machines."""
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api import vast
from api.auth import require_auth
from vast_hosts import host_history, host_preferences, set_host_preference, search_host_offers
from vast_job_runner import guard_step
from vast_jobs import JobStore, IMAGE, REVISION, write_json
from vast_provider import VastClient, VastError
from vast_queue import CloudQueue


@pytest.fixture
def queue(tmp_path, monkeypatch):
    """Keep account, job and queue data entirely in a temporary directory."""
    queue = CloudQueue(JobStore(tmp_path / 'vast'))
    monkeypatch.setattr(vast, 'CloudQueue', lambda: queue)
    monkeypatch.setattr(vast, 'VastCredentialStore', lambda *args: SimpleNamespace(secrets=lambda: {'api_key': 'fake'}))
    return queue


def save_record(queue, identifier, state, offer=None):
    """Create minimal historical records without invoking any supervisor."""
    folder = queue.root / 'jobs' / identifier
    folder.mkdir(parents=True)
    write_json(folder / 'state.json', dict(id=identifier, **state))
    write_json(folder / 'intent.json', dict(offer=offer or {}))
    return folder


def test_history_requires_real_usage_and_exact_results(queue):
    """Provisioning is not working; shared jobs count one rental and survive hiding."""
    save_record(queue, 'a'*32, dict(kind='worker', instance_id=10), {'machine_id': 7})
    save_record(queue, 'b'*32, dict(lease_id='a'*32, status='failed', exact_completed=0))
    save_record(queue, 'c'*32, dict(lease_id='a'*32, status='cancelled', exact_completed=12, deleted_at=99))
    save_record(queue, 'd'*32, dict(kind='worker', instance_id=11, status='completed'), {'machine_id': 8})
    save_record(queue, 'e'*32, dict(kind='worker', status='provisioning'), {'machine_id': 9})
    save_record(queue, 'f'*32, dict(status='completed', exact_completed=25), {'gpu_name': 'RTX 5090'})
    history = host_history(queue.store, queue.read())
    assert history[7]['used'] and history[7]['working_detected'] and history[7]['working']
    assert history[7]['rentals'] == 1
    assert history[8]['used'] and not history[8]['working']
    assert not history[9]['used'] and not history[9]['working']
    assert set(history) == {7, 8, 9}


def test_independent_marks_persist_without_claiming_usage(queue):
    """Concurrent preference and working changes merge and remain reversible."""
    queue.set_host_block(7, True)
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(set_host_preference, queue, 7, preferred=True),
                   pool.submit(set_host_preference, queue, 7, working=True)]
        for future in futures:
            future.result()
    reloaded = CloudQueue(JobStore(queue.root))
    profile = host_history(reloaded.store, reloaded.read())[7]
    assert profile['preferred'] and profile['working_marked'] and profile['working']
    assert not profile['used'] and not profile['working_detected']
    assert reloaded.read()['blocked_machine_ids'] == [7]
    set_host_preference(queue, 7, preferred=False)
    assert host_preferences(queue.read())['7']['working']
    set_host_preference(queue, 7, working=False)
    assert host_preferences(queue.read()) == {}


@pytest.mark.parametrize('stored', [[], {'../7': {}}, {'07': {}}, {'7': {'working': 'yes'}}, {'7': {'other': True}}])
def test_corrupt_preferences_are_rejected(stored):
    """Invalid marks cannot silently modify the rental ranking policy."""
    with pytest.raises(VastError, match='cannot be read'):
        host_preferences({'host_preferences': stored})


def test_preferred_host_search_goes_beyond_cheapest_page(queue):
    """A preferred host outside the ordinary first page is explicitly searched."""
    set_host_preference(queue, 7, preferred=True)
    calls = []

    def offers(**limits):
        """Model a marketplace that only returns the preferred host when filtered."""
        calls.append(limits)
        if limits.get('included_machine_ids') == [7]:
            return [{'id': 2, 'machine_id': 7, 'price_hour_usd': .4}]
        return [{'id': 1, 'machine_id': 8, 'price_hour_usd': .1}]

    rows = search_host_offers(SimpleNamespace(offers=offers), queue.read(), max_price=.5, excluded_machine_ids=[])
    assert [row['id'] for row in rows] == [2, 1]
    assert calls[1]['max_price'] == .5
    calls.clear()
    search_host_offers(SimpleNamespace(offers=offers), queue.read(), offer_id=1)
    assert len(calls) == 1


@pytest.mark.parametrize('bad', [{}, {'price_hour_usd': 2}, {'cuda_max_good': 12}, {'blocked': True}, {'missing': True}])
def test_preference_respects_limits_and_falls_back(queue, monkeypatch, bad):
    """Priority only wins among allowed available offers meeting hard constraints."""
    set_host_preference(queue, 7, preferred=True)
    if bad.get('blocked'):
        queue.set_host_block(7, True)
    monkeypatch.setattr(queue, 'waiting', lambda: [{'workers': 1}])
    offer = dict(num_gpus=1, gpu_name='RTX 5090', cuda_max_good=13, vram_gb=32,
                 ram_gb=64, cpu_cores=16, disk_gb=40, verified=True)
    preferred = dict(offer, id=2, machine_id=7, price_hour_usd=.4)
    preferred.update({key: value for key, value in bad.items() if key not in {'blocked', 'missing'}})
    other = dict(offer, id=1, machine_id=8, price_hour_usd=.1)
    rows = [other] if bad.get('missing') else [other, preferred]
    monkeypatch.setattr(vast, 'VastClient', lambda key: SimpleNamespace(offers=lambda **limits: rows))
    monkeypatch.setattr(queue, 'start', lambda selected, *args, **kwargs: selected)
    body = vast.StartJobRequest(preferences=vast.GpuPreferences(max_price=.5), accept_rental_and_cleanup=True)
    assert vast.start_queue(body, session=None)['machine_id'] == (8 if bad else 7)
    body.rent_only = True
    body.offer_id = 1
    assert vast.start_queue(body, session=None)['machine_id'] == 8


def test_provider_rechecks_included_machine_filter(monkeypatch):
    """Unexpected provider rows cannot impersonate the preferred subset."""
    queries = []

    def request(self, method, path, body):
        """Return the desired host and two rows that violate the machine filter."""
        queries.append(body)
        return {'offers': [{'id': 1, 'machine_id': 7, 'dph_total': .2},
                           {'id': 2, 'machine_id': 8, 'dph_total': .1}, {'id': 3, 'dph_total': .1}]}

    monkeypatch.setattr(VastClient, 'request', request)
    assert [row['id'] for row in VastClient('fake').offers(included_machine_ids=[7, 8], excluded_machine_ids=[8])] == [1]
    assert queries[0]['machine_id'] == {'in': [7]}


def test_guard_records_legacy_machine_id(queue):
    """Remember identity from an already-owned instance without creating another."""
    identifier = 'a'*32
    folder = save_record(queue, identifier, dict(status='provisioning', rental_state='active', instance_id=10))
    intent = dict(id=identifier, label='pbgui-vast-'+identifier, deadline=2000, accepted_at=1000,
                  image=IMAGE, pb8_revision=REVISION, budget_usd=1, offer={'id': 42})
    write_json(folder / 'intent.json', intent)
    write_json(folder / 'attempt.json', {'attempted_at': 1000})
    write_json(folder / 'control.json', {'stop': False, 'cleanup': False})
    provider = SimpleNamespace(instances=lambda **kw: [{'id': 10, 'label': intent['label'], 'machine_id': 7}])
    guard_step(queue.store, identifier, provider, intent, '', now=1001)
    assert queue.store.read(identifier)['host_machine_id'] == 7
    assert host_history(queue.store, queue.read())[7]['used']


def test_host_routes_are_authenticated_and_marks_appear_in_preview(queue, monkeypatch):
    """Public projections contain marks, never raw provider metadata."""
    app = FastAPI()
    app.include_router(vast.router, prefix='/api/vast')
    app.dependency_overrides[require_auth] = lambda: object()
    monkeypatch.setattr(vast, 'VastClient', lambda key: SimpleNamespace(offers=lambda **kw: [dict(id=1, machine_id=7, price_hour_usd=.2)]))
    with TestClient(app) as http:
        assert http.post('/api/vast/host-preferences', json={'machine_id': True, 'preferred': True}).status_code == 422
        marked = http.post('/api/vast/host-preferences', json={'machine_id': 7, 'preferred': True, 'working': True})
        assert marked.status_code == 200
        assert marked.json()['hosts'][0]['preferred']
        history = http.get('/api/vast/hosts')
        assert history.headers['cache-control'] == 'no-store'
        offer = http.get('/api/vast/offers').json()['offers'][0]
        assert offer['host_history']['working'] and not offer['host_history']['used']
        app.dependency_overrides.clear()
        assert http.get('/api/vast/hosts').status_code in (401, 403)
        assert http.post('/api/vast/host-preferences', json={'machine_id': 7, 'preferred': False}).status_code in (401, 403)
