"""Offline contracts for Vast credentials, marketplace access and API boundaries."""

from __future__ import annotations

import json
import stat
import urllib.error
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api import vast
from api.auth import require_auth
from vast_credentials import VastCredentialStore
from vast_provider import NoRedirect, VastClient, VastError, number


@pytest.fixture
def client(tmp_path, monkeypatch):
    """Build an authenticated isolated app without PBGui startup or real HTTP."""
    store = VastCredentialStore(tmp_path / "vast")
    from vast_queue import CloudQueue
    from vast_jobs import JobStore
    monkeypatch.setattr(vast, 'CloudQueue', lambda: CloudQueue(JobStore(store.root)))
    monkeypatch.setattr(vast, "VastCredentialStore", lambda *args: store)
    app = FastAPI()
    app.include_router(vast.router, prefix="/api/vast")
    app.dependency_overrides[require_auth] = lambda: object()
    with TestClient(app) as test_client:
        yield test_client, store, app


def test_partial_preferences_preserve_other_groups_and_validate(client):
    """Independent form saves merge with stored policy without resetting other fields."""
    http, _, _ = client
    assert http.post('/api/vast/gpu-preferences', json={'gpu_name': 'RTX 3090', 'hours': 4, 'budget': 7}).status_code == 200
    result = http.patch('/api/vast/gpu-preferences', json={'min_cpu': 16})
    assert result.status_code == 200
    assert result.json()['hours'] == 4
    assert result.json()['budget'] == 7
    result = http.patch('/api/vast/gpu-preferences', json={'hours': 2})
    assert result.json()['min_cpu'] == 16
    assert result.json()['gpu_name'] == 'RTX 3090'
    assert http.patch('/api/vast/gpu-preferences', json={'hours': 0}).status_code == 422
    assert http.patch('/api/vast/gpu-preferences', json={'unknown': True}).status_code == 422
    assert http.get('/api/vast/gpu-preferences').json()['hours'] == 2


def test_calibration_status_and_acceptance_are_local_and_explicit(client):
    """Profile discovery is read-only and incomplete evidence cannot be accepted."""
    from secure_files import ensure_private_directory
    from vast_jobs import write_json

    http, store, _ = client
    offer = {'id': 11, 'machine_id': 22, 'gpu_name': 'RTX 3090', 'vram_gb': 24,
             'gpu_mem_bw_gbps': 936, 'price_hour_usd': .5}
    status = http.post('/api/vast/calibration/status', json=offer)
    assert status.status_code == 200
    assert status.headers['cache-control'] == 'no-store'
    assert status.json()['calibration_worker'] is True
    assert status.json()['workload']['version'] == 'pb8-gpu-calibration-v2'
    assert status.json()['workload']['coins'] == ['BTC', 'ETH', 'SOL']
    assert 'inputs' not in status.json()
    assert 'configs' not in status.json()
    assert status.json()['identity']['runtime_verified'] is False

    calibration_id = '2' * 32
    calibration = ensure_private_directory(store.root / 'jobs' / calibration_id)
    candidate = {'id': 'a' * 64, 'gpu_variant_fingerprint': 'b' * 64,
                 'workload_fingerprint': 'c' * 64, 'population_size': 12288,
                 'protocol': 1, 'hardware_identity': {'provider': offer}}
    write_json(calibration / 'state.json', {'id': calibration_id, 'kind': 'calibration',
                                            'status': 'completed', 'rental_state': 'none',
                                            'calibration_profile_candidate': candidate})
    accepted = http.post('/api/vast/calibration/accept', json={'job_id': calibration_id})
    assert accepted.status_code == 422
    assert not (store.root / 'calibration_profiles.json').exists()


def test_calibration_start_requires_paid_rental_consent_before_provider_access(client):
    """Missing confirmation is rejected before any marketplace or rental mutation."""
    http, _, _ = client
    response = http.post('/api/vast/calibration/start', json={
        'offer': {'id': 11, 'machine_id': 22, 'gpu_name': 'RTX 3090', 'vram_gb': 24,
                  'gpu_mem_bw_gbps': 936, 'price_hour_usd': .5},
        'hours': 1, 'budget': 1, 'accept_rental_and_cleanup': False,
    })
    assert response.status_code == 422


def test_waiting_calibration_requires_paid_consent_and_new_pinned_worker(client, monkeypatch):
    """An unattended future rental is rejected before any provider call without both gates."""
    http, store, _ = client
    request = {'preferences': {'gpu_name': 'RTX 3090', 'max_price': .2,
                               'min_power_watts': 350, 'min_reliability_pct': 95},
               'hours': 1, 'budget': 5}
    assert http.post('/api/vast/calibration/watch', json=request).status_code == 422
    request['accept_rental_and_cleanup'] = True
    import vast_calibration
    monkeypatch.setattr(vast_calibration, 'CALIBRATION_WORKER_DIGEST', None)
    response = http.post('/api/vast/calibration/watch', json=request)
    assert response.status_code == 409
    assert not (store.root / 'jobs').exists()


def test_calibration_start_requires_pinned_runner_before_provider_access(client, monkeypatch):
    """A heartbeat-only worker must never create another paid calibration rental."""
    import vast_calibration
    http, _, _ = client
    monkeypatch.setattr(vast_calibration, 'CALIBRATION_WORKER_DIGEST', None)
    response = http.post('/api/vast/calibration/start', json={
        'offer': {'id': 11, 'machine_id': 22, 'gpu_name': 'RTX 3090', 'vram_gb': 24,
                  'gpu_mem_bw_gbps': 936, 'price_hour_usd': .5},
        'hours': 1, 'budget': 1, 'accept_rental_and_cleanup': True,
    })
    assert response.status_code == 409
    assert 'calibration runner' in response.json()['detail']


def test_concurrent_preference_groups_do_not_lose_updates(client):
    """Two browser tabs can save disjoint groups through the same queue lock."""
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    http, _, _ = client
    gate = Barrier(2)

    def save(values):
        """Start the two independent authenticated requests together."""
        gate.wait(timeout=5)
        return http.patch('/api/vast/gpu-preferences', json=values)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(save, [{'min_cpu': 24}, {'hours': 3, 'budget': 8}]))
    assert all(response.status_code == 200 for response in results)
    saved = http.get('/api/vast/gpu-preferences').json()
    assert (saved['min_cpu'], saved['hours'], saved['budget']) == (24, 3, 8)


def test_credentials_are_private_and_responses_do_not_reveal_them(client):
    """Save and read metadata without disclosing either provider credential."""
    http, store, _ = client
    response = http.post('/api/vast/credentials', json={'api_key':'secret-vast', 'registry_token':'secret-pull'})
    assert response.status_code == 200
    assert response.json()['configured'] is True
    assert 'secret-' not in response.text
    assert response.headers['cache-control'] == 'no-store'
    assert stat.S_IMODE((store.root/'credentials.json').stat().st_mode) == 0o600
    assert stat.S_IMODE(store.root.stat().st_mode) == 0o700
    assert store.secrets()['api_key'] == 'secret-vast'
    payload = http.get('/api/vast/settings')
    assert 'secret-' not in payload.text
    assert payload.json()['referral_url'] == 'https://cloud.vast.ai/?ref_id=522435'


@pytest.mark.parametrize('body', [
    {'api_key':{'secret':'never-echo-this'}}, {'api_key':'never-echo-this\n'},
    {'wrong':'never-echo-this'}, {'api_key':'never-echo-this' * 1300},
    ['never-echo-this'], None,
])
def test_bad_secret_requests_do_not_echo_inputs(client, body):
    """Validation errors cannot contain Pydantic input echoes or key values."""
    http, store, _ = client
    response = http.post('/api/vast/credentials', json=body)
    assert response.status_code in (413, 422)
    assert 'never-echo-this' not in response.text
    assert not store.metadata()['configured']


def test_credential_update_merges_explicit_fields(client):
    """Updating registry access must not erase the Vast account key."""
    http, store, _ = client
    http.post('/api/vast/credentials', json={'api_key':'old-key'})
    http.post('/api/vast/credentials', json={'registry_token':'pull-key'})
    assert store.secrets()['api_key'] == 'old-key'
    assert store.secrets()['generation'] == 2


def test_credentials_cannot_change_during_open_rental(client):
    """Retain cleanup access until every owned rental has verified deletion."""
    http, store, _ = client
    store.save(api_key='original')
    path = store.root / 'jobs' / 'test' / 'state.json'
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({'rental_state':'destroy_pending'}))
    assert http.post('/api/vast/credentials', json={'api_key':'replacement'}).status_code == 409
    assert store.secrets()['api_key'] == 'original'


def test_corrupt_credentials_do_not_look_unconfigured(tmp_path):
    """Reject invalid storage explicitly rather than replacing an unreadable key."""
    root = tmp_path/'vast'; root.mkdir()
    (root/'credentials.json').write_text('{invalid')
    with pytest.raises(VastError, match='could not be read'):
        VastCredentialStore(root).metadata()


def test_credentials_symlink_rejected(tmp_path):
    """A link cannot redirect sensitive storage outside the configured root."""
    root = tmp_path/'vast'; root.mkdir()
    other = tmp_path/'other'; other.write_text('{"api_key":"secret"}')
    (root/'credentials.json').symlink_to(other)
    with pytest.raises(VastError):
        VastCredentialStore(root).secrets()


def test_account_projection_and_balance(client, monkeypatch):
    """Show balance while filtering unrelated sensitive provider fields."""
    http, store, _ = client
    store.save(api_key='local-key')
    calls = []
    def request(self, method, path, body=None):
        """Supply only fake account data and record the exact read operation."""
        calls.append((method,path))
        return {'id':522435,'balance':0,'credit':12.34,'sid':'hidden-session','ssh_key':'hidden-key','email':'hidden-email'}
    monkeypatch.setattr(VastClient, 'request', request)
    response = http.post('/api/vast/account')
    assert response.status_code == 200
    assert response.json()['balance_usd'] == 12.34
    assert 'hidden' not in response.text
    assert response.headers['cache-control'] == 'no-store'
    assert calls == [('GET','/users/current/')]


@pytest.mark.parametrize('value, expected', [(0,0),(-2,-2),(None,None),('bad',None),('NaN',None),(float('inf'),None),(True,None)])
def test_balance_unknown_is_not_zero(value, expected):
    """Unknown and nonfinite balance values remain distinguishable from zero."""
    assert number(value, minimum=None) == expected


def test_missing_credit_is_not_replaced_with_ledger_balance(monkeypatch):
    """Do not present a different provider ledger field as rental credit."""
    monkeypatch.setattr(VastClient, 'request', lambda *args, **kwargs: {'balance': 100})
    assert VastClient('key').account()['balance_usd'] is None


def test_offer_projection_and_query(monkeypatch):
    """Use storage-aware on-demand search and do not return raw provider records."""
    calls=[]
    def request(self, method, path, body=None):
        """Provide one affordable offer and one outside the requested price cap."""
        calls.append((method,path,body))
        return {'offers':[
            {'id':7,'gpu_name':'RTX 3090','gpu_ram':24576,'cpu_ram':32768,'cpu_cores_effective':8.7,'total_flops':35.58,'gpu_max_power':200,'dph_total':0.17,'verification':'verified','inet_up_cost':0.01,'inet_down_cost':0.02,'secret':'hidden'},
            {'id':8,'dph_total':3}, {'id':9,'dph_total':'nan'},
        ]}
    monkeypatch.setattr(VastClient,'request',request)
    rows=VastClient('key').offers(max_price=0.5)
    assert len(rows)==1 and rows[0]['vram_gb']==24
    assert rows[0]['verified'] and rows[0]['price_hour_usd']==0.17
    assert 'hidden' not in json.dumps(rows)
    assert rows[0]['tflops'] == 35.58
    assert calls[0][:2]==('POST','/bundles')
    assert calls[0][2]['allocated_storage']==40
    assert calls[0][2]['type']=='on-demand'
    assert calls[0][2]['num_gpus']=={'eq':1}
    assert rows[0]['gpu_max_power_watts'] == 200
    VastClient('key').offers(max_price=.5, max_offers=500)
    assert calls[-1][2]['limit'] == 500


def test_offer_api_passes_validated_filters(client, monkeypatch):
    """Bound browser-supplied filters and never perform rental operations."""
    http, store, _ = client
    store.save(api_key='key')
    monkeypatch.setattr(VastClient,'offers',lambda self,**kw: [{'id':10,'disk_gb':kw['disk_gb']}])
    assert http.get('/api/vast/offers?disk_gb=60').json()['offers'][0]['disk_gb']==60
    assert http.get('/api/vast/offers?disk_gb=-1').status_code==422


def test_provider_errors_are_safe(client, monkeypatch):
    """Show actionable domain errors without upstream bodies or credentials."""
    http,store,_=client;store.save(api_key='sensitive')
    def fail(self):
        """Emulate a permission failure without network access."""
        raise VastError('Vast key lacks permission for this operation')
    monkeypatch.setattr(VastClient,'account',fail)
    response=http.post('/api/vast/account')
    assert response.status_code==502
    assert 'permission' in response.text and 'sensitive' not in response.text


def test_upstream_http_error_does_not_echo_body(monkeypatch):
    """HTTP failures expose only mapped error descriptions."""
    class Opener:
        """Fail the only HTTP call without an actual connection."""
        def open(self,request,timeout):
            """Verify secret transport and emulate a provider denial."""
            assert request.headers['Authorization']=='Bearer sensitive'
            assert 'sensitive' not in request.full_url
            raise urllib.error.HTTPError(request.full_url,403,'sensitive',{},None)
    monkeypatch.setattr('urllib.request.build_opener',lambda *_:Opener())
    with pytest.raises(VastError,match='lacks permission') as exc:
        VastClient('sensitive').account()
    assert 'sensitive' not in str(exc.value)
    assert NoRedirect().redirect_request(None,None,302,'',{},'https://attacker.invalid') is None


@pytest.mark.parametrize(('path', 'message'), [
    ('/asks/123/', 'offer is no longer available'),
    ('/instances/123/', 'resource is no longer available'),
])
def test_http_410_names_the_failed_resource(monkeypatch, path, message):
    """A consumed offer and an unavailable instance must not share one diagnosis."""
    class Opener:
        """Return an isolated gone response."""
        def open(self, request, timeout):
            raise urllib.error.HTTPError(request.full_url, 410, 'secret-body', {}, None)

    monkeypatch.setattr('urllib.request.build_opener', lambda *_: Opener())

    with pytest.raises(VastError, match=message):
        VastClient('sensitive').request('GET', path)


def test_all_routes_require_authentication(client):
    """No account, secret, marketplace or page route is public."""
    _,_,app=client
    for route in app.routes:
        if route.path.startswith('/api/vast'):
            assert any(d.call is require_auth for d in route.dependant.dependencies)


def test_page_has_referral_and_local_assets(client):
    """Registration uses the requested referral without loading external assets."""
    http,_,_=client
    redirect=http.get('/api/vast/main_page', follow_redirects=False)
    assert redirect.status_code == 307
    assert '/api/optimize-v8/main_page?view=queue' in redirect.headers['location']
    response=http.get('/api/vast/fragment')
    assert response.status_code==200
    assert 'https://cloud.vast.ai/?ref_id=522435' in response.text
    assert 'noopener noreferrer sponsored' in response.text
    assert '%%API_BASE%%' not in response.text
    assert '<script src="https://' not in response.text


def test_frontend_secret_and_request_lifecycle_contract():
    """The page avoids unsafe rendering, persistence and stale async updates."""
    source=(Path(__file__).parents[1]/'frontend/js/vast.js').read_text()
    assert 'innerHTML' not in source and 'localStorage' not in source and 'sessionStorage' not in source
    assert 'accountGeneration' in source and 'offerGeneration' in source
    assert "'pagehide'" in source and 'controller.abort()' in source
    assert "el('api-key').value = ''" in source


def test_preview_compatibility_filters_are_explicit(client, monkeypatch):
    """Default preview filters require CUDA 13 and the requested lease duration."""
    http, store, _ = client
    store.save(api_key='fake')
    calls = []
    monkeypatch.setattr(VastClient, 'offers', lambda self, **kwargs: calls.append(kwargs) or [])
    assert http.get('/api/vast/offers?rental_hours=3').status_code == 200
    assert calls[-1]['min_cuda'] == 13
    assert calls[-1]['min_duration'] == 10800
    assert http.get('/api/vast/offers?include_incompatible=true').status_code == 200
    assert calls[-1]['min_cuda'] == 0
    assert calls[-1]['min_duration'] == 0
    assert http.get('/api/vast/offers?rental_hours=-1').status_code == 422


def test_unsaved_config_validation_is_pinned_and_read_only(client, monkeypatch):
    """Editor validation works without account credentials or a GPU."""
    from vast_jobs import IMAGE, REVISION
    http, store, _ = client
    def forbidden(*args, **kwargs):
        """Pure validation must never create a queue or call Vast."""
        raise AssertionError('Runtime access during validation')
    monkeypatch.setattr(vast, 'CloudQueue', forbidden)
    monkeypatch.setattr(vast, 'VastClient', forbidden)
    config = {'live':{'strategy_kind':'ema_anchor','approved_coins':{'long':['BTC'],'short':[]}},
              'bot':{'long':{},'short':{}},'backtest':{'exchanges':['binance']},
              'optimize':{'scoring':[{'metric':'gain_strategy_eq'}],'limits':[]}}
    response = http.post('/api/vast/validate-config', json={'config':config})
    assert response.status_code == 200
    assert response.headers['cache-control'] == 'no-store'
    report = response.json()
    assert not report['valid']
    assert report['image'] == IMAGE and report['revision'] == REVISION
    assert report['errors'][0]['path'] == 'optimize.scoring.0.metric'
    assert 'gain_strategy_eq' not in report['metrics']
    assert 'adg_strategy_eq' in report['metrics']
    assert not store.root.exists()


def test_cloud_delete_and_log_metadata(client, monkeypatch, tmp_path):
    """The API permanently deletes entries and their local optimizer logs."""
    from secure_files import ensure_private_directory
    from vast_jobs import JobStore, write_json
    from vast_queue import CloudQueue
    http, _, _ = client
    queue = CloudQueue(JobStore(tmp_path / 'queue'))
    monkeypatch.setattr(vast, 'CloudQueue', lambda: queue)
    monkeypatch.setattr(vast, 'services_available', lambda: True)
    logs = tmp_path / 'logs'
    logs.mkdir()
    monkeypatch.setattr(vast, 'CLOUD_LOG_ROOT', logs)
    identifier = 'b' * 32
    folder = ensure_private_directory(queue.root / 'jobs' / identifier)
    write_json(folder / 'state.json', {'id':identifier,'status':'ready','rental_state':'none'})
    assert http.get('/api/vast/jobs').json()['jobs'][0]['has_log'] is False
    inputs = ensure_private_directory(folder / 'input')
    write_json(inputs / 'manifest.json', {'files':[{'path':'ohlcv/binance/1m/ETH/2026-01-01.npz'},
                                                  {'path':'ohlcv/bybit/1m/ETH/2026-01-01.npz'}]})
    assert http.get('/api/vast/jobs').json()['jobs'][0]['exchange'] == 'binance, bybit'
    queue.store.update(identifier, exchanges=['bybit'])
    assert http.get('/api/vast/jobs').json()['jobs'][0]['exchange'] == 'bybit'
    (logs / f'vast_{identifier}.log').write_text('test log')
    assert http.get('/api/vast/jobs').json()['jobs'][0]['has_log'] is True
    assert http.delete('/api/vast/jobs/' + identifier).status_code == 200
    assert http.get('/api/vast/jobs').json()['jobs'] == []
    assert not (logs / f'vast_{identifier}.log').exists()


def test_start_uses_persisted_rental_settings(client, monkeypatch, tmp_path):
    """The row Start request uses server-side limits, not unsaved browser fields."""
    from vast_jobs import JobStore
    from vast_queue import CloudQueue
    http, credentials, _ = client
    credentials.save(api_key='mock-key')
    queue = CloudQueue(JobStore(tmp_path / 'queue'))
    monkeypatch.setattr(vast, 'CloudQueue', lambda: queue)
    saved = http.post('/api/vast/gpu-preferences', json={'gpu_name':'RTX 3090','hours':3,'budget':2,'idle_seconds':0})
    assert saved.status_code == 200
    assert http.get('/api/vast/gpu-preferences').json()['hours'] == 3
    monkeypatch.setattr(queue, 'waiting', lambda: [{'workers':4}])
    offer = dict(id=1,gpu_name='RTX 3090',num_gpus=1,cuda_max_good=13,duration_seconds=20000,
                 price_hour_usd=.1,vram_gb=24,ram_gb=32,cpu_cores=8,disk_gb=40,verified=True)
    searches, starts = [], []
    class Provider:
        """Return a fixed offer without network access."""
        def offers(self, **kwargs):
            """Record the actual saved search criteria."""
            searches.append(kwargs)
            return [offer]
    monkeypatch.setattr(vast, 'VastClient', lambda key: Provider())
    monkeypatch.setattr(queue, 'start', lambda *args: starts.append(args) or {'id':'worker'})
    result = http.post('/api/vast/queue/start', json={'use_saved_settings':True,'accept_rental_and_cleanup':True,
                                                    'hours':24,'budget':100})
    assert result.status_code == 202
    assert starts[0][1:] == (3, 2, 0)
    assert searches[0]['min_duration'] == 10800
    assert 'hours' not in searches[0]
    assert http.post('/api/vast/gpu-preferences', json={'hours':0}).status_code == 422
    retained = http.post('/api/vast/gpu-preferences', json={'idle_seconds':-1})
    assert retained.status_code == 200
    assert retained.json()['idle_seconds'] == -1
    assert http.post('/api/vast/gpu-preferences', json={'idle_seconds':12}).status_code == 422


def test_requeue_is_idempotent_under_concurrent_requests(tmp_path, monkeypatch):
    """Repeated requests prepare one replacement and retain original artifacts."""
    from concurrent.futures import ThreadPoolExecutor
    from vast_jobs import JobStore, write_json
    from vast_queue import CloudQueue
    from secure_files import ensure_private_directory
    store = JobStore(tmp_path / 'jobs-root')
    identifier = 'a' * 32
    directory = ensure_private_directory(store.root / 'jobs' / identifier)
    write_json(directory / 'state.json', {'id':identifier, 'status':'failed', 'rental_state':'none',
               'config_name':'test', 'iterations':512, 'workers':4})
    monkeypatch.setattr(vast, 'CloudQueue', lambda: CloudQueue(store))
    calls = []
    def prepare(body, *, requeue_from=None):
        """Create only isolated replacement metadata."""
        calls.append(body)
        replacement = 'b' * 32
        target = ensure_private_directory(store.root / 'jobs' / replacement)
        write_json(target / 'state.json', {'id':replacement, 'status':'ready'})
        return store.read(replacement)
    monkeypatch.setattr(vast, '_prepare_job', prepare)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: vast.requeue_job(identifier, object()), range(2)))
    assert len(calls) == 1
    assert results[0]['id'] == results[1]['id'] == 'b' * 32
    assert store.read(identifier)['deleted_at']


@pytest.mark.parametrize('auto_rent,expected_resumed', [(True, True), (False, False)])
def test_requeue_resumes_only_authorized_auto_pool(tmp_path, monkeypatch, auto_rent, expected_resumed):
    """Explicit retry resumes paid scheduling only under the saved automatic authorization."""
    from vast_jobs import JobStore, write_json
    from vast_queue import CloudQueue
    from secure_files import ensure_private_directory
    store = JobStore(tmp_path / 'jobs-root')
    identifier = 'a' * 32
    directory = ensure_private_directory(store.root / 'jobs' / identifier)
    write_json(directory / 'state.json', {'id':identifier, 'status':'failed', 'rental_state':'none',
               'config_name':'test', 'iterations':512, 'workers':4})
    queue = CloudQueue(store)
    queue.update(paused=True, pool_enabled=True, pool_error=None,
                 pool_authorization={'id':'c' * 32, 'settings':{'auto_rent':auto_rent}})
    monkeypatch.setattr(vast, 'CloudQueue', lambda: queue)

    def prepare(body, *, requeue_from=None):
        """Create one isolated ready replacement."""
        replacement = 'b' * 32
        target = ensure_private_directory(store.root / 'jobs' / replacement)
        write_json(target / 'state.json', {'id':replacement, 'status':'ready', 'rental_state':'none'})
        return store.read(replacement)

    launches = []
    monkeypatch.setattr(vast, '_prepare_job', prepare)
    monkeypatch.setattr('vast_pool.launch_pool', lambda value: launches.append(value))
    assert vast.requeue_job(identifier, object())['id'] == 'b' * 32
    assert queue.read()['paused'] is not expected_resumed
    assert bool(launches) is expected_resumed


@pytest.mark.parametrize('status,visible', [
    ('preparing', 'b'), ('ready', 'b'), ('failed', 'a'), ('cancelled', 'a'),
])
def test_requeue_list_has_one_visible_entry(tmp_path, monkeypatch, status, visible):
    """Polling during replacement preparation must never expose duplicate entries."""
    from vast_jobs import JobStore, write_json
    from vast_queue import CloudQueue
    from secure_files import ensure_private_directory
    from fastapi import Response
    store = JobStore(tmp_path / 'queue')
    for key, fields in [('a', {'status':'cancelled'}), ('b', {'status':status, 'requeue_from':'a'*32})]:
        directory = ensure_private_directory(store.root / 'jobs' / (key*32))
        write_json(directory/'state.json', dict(id=key*32, rental_state='none', **fields))
    monkeypatch.setattr(vast, 'CloudQueue', lambda: CloudQueue(store))
    monkeypatch.setattr(vast, 'services_available', lambda: False)
    assert [row['id'] for row in vast.jobs(Response(), None)['jobs']] == [visible*32]


def test_cancelled_requeue_does_not_hide_running_predecessor(tmp_path, monkeypatch):
    """A cancelled retry must not disconnect an active job from its GPU card."""
    from vast_jobs import JobStore, write_json
    from vast_queue import CloudQueue
    from secure_files import ensure_private_directory
    from fastapi import Response
    store = JobStore(tmp_path / 'queue')
    states = [
        dict(id='a'*32, status='running', created_at=1, lease_id='c'*32),
        dict(id='b'*32, status='cancelled', created_at=2, requeue_from='a'*32),
        dict(id='c'*32, status='running', created_at=0, kind='worker',
             rental_state='active', active_job='a'*32),
    ]
    for state in states:
        directory = ensure_private_directory(store.root / 'jobs' / state['id'])
        write_json(directory / 'state.json', {'rental_state': 'none', **state})
    monkeypatch.setattr(vast, 'CloudQueue', lambda: CloudQueue(store))
    monkeypatch.setattr(vast, 'services_available', lambda: False)
    monkeypatch.setattr(vast, '_rental_details', lambda job_store, lease: {'offer': {}})
    result = vast.jobs(Response(), None)
    assert [row['id'] for row in result['jobs']] == ['a'*32]
    assert result['workers'][0]['active_job'] == 'a'*32


def test_deleted_marker_does_not_hide_worker_owned_optimizer(tmp_path, monkeypatch):
    """Worker ownership keeps a running optimizer visible after stale deletion state."""
    from vast_jobs import JobStore, write_json
    from vast_queue import CloudQueue
    from secure_files import ensure_private_directory
    from fastapi import Response

    store = JobStore(tmp_path / 'queue')
    states = [
        dict(id='a'*32, status='running', created_at=1, lease_id='c'*32,
             deleted_at=2),
        dict(id='c'*32, status='running', created_at=0, kind='worker',
             rental_state='active', active_job='a'*32),
    ]
    for state in states:
        directory = ensure_private_directory(store.root / 'jobs' / state['id'])
        write_json(directory / 'state.json', {'rental_state': 'none', **state})
    monkeypatch.setattr(vast, 'CloudQueue', lambda: CloudQueue(store))
    monkeypatch.setattr(vast, 'services_available', lambda: False)
    monkeypatch.setattr(vast, '_rental_details', lambda job_store, lease: {'offer': {}})

    result = vast.jobs(Response(), None)

    assert [row['id'] for row in result['jobs']] == ['a'*32]
    assert result['jobs'][0]['can_delete'] is False


def test_rental_details_use_saved_offer_and_exclude_credentials(tmp_path):
    """Historical jobs expose only their rental's public offer fields."""
    from vast_jobs import JobStore, write_json
    from secure_files import ensure_private_directory
    store = JobStore(tmp_path/'jobs-root')
    lease = 'a'*32
    directory = ensure_private_directory(store.root/'jobs'/lease)
    write_json(directory/'intent.json', {'offer':{'gpu_name':'RTX 3090', 'inet_down_mbps':800,
        'price_hour_usd':0.15, 'api_key':'must-not-leak'}, 'budget_usd':1, 'deadline':1234,
        'registry_token':'must-not-leak'})
    result = vast._rental_details(store, lease)
    assert result['offer']['inet_down_mbps'] == 800
    assert result['offer']['price_hour_usd'] == 0.15
    assert result['deadline'] == 1234
    assert 'must-not-leak' not in json.dumps(result)
    assert vast._rental_details(store, None) is None


def test_cost_estimate_stops_at_terminal_state():
    """Finished jobs do not keep accumulating estimated compute charges."""
    rental = {'offer':{'price_hour_usd':1,'download_gb_usd':.1,'upload_gb_usd':.2},'deadline':10000}
    row = {'status':'failed','setup_started_at':1000,'updated_at':4600,'uploaded':True,
           'transfer_input_bytes':1000000000,'downloaded_bytes':500000000}
    cost = vast._run_cost_estimate(row,rental,9000)
    assert cost['compute_usd'] == 1
    assert cost['transfer_usd'] == pytest.approx(.2)
    assert cost['total_usd'] == pytest.approx(1.2)
    assert vast._run_cost_estimate({},rental,9000) is None


def test_stop_request_survives_job_polling(tmp_path, monkeypatch):
    """Expose the persisted stop request while the supervisor still reports running."""
    from fastapi import Response
    from vast_jobs import JobStore, write_json
    from vast_queue import CloudQueue
    from secure_files import ensure_private_directory
    store = JobStore(tmp_path / 'queue')
    identifier = 'a' * 32
    directory = ensure_private_directory(store.root / 'jobs' / identifier)
    write_json(directory / 'state.json', {'id': identifier, 'status': 'running', 'rental_state': 'active'})
    write_json(directory / 'control.json', {'stop': True, 'cleanup': False})
    monkeypatch.setattr(vast, 'CloudQueue', lambda: CloudQueue(store))
    monkeypatch.setattr(vast, 'services_available', lambda: False)
    row = vast.jobs(Response(), None)['jobs'][0]
    assert row['status'] == 'running'
    assert row['stop_requested'] is True


def test_provider_log_fetch_time_is_file_snapshot_time(tmp_path, monkeypatch):
    """API polling must not make an older downloaded log appear freshly retrieved."""
    import os
    from fastapi import Response
    from vast_jobs import JobStore, write_json
    from vast_queue import CloudQueue
    from secure_files import ensure_private_directory
    store = JobStore(tmp_path / 'queue')
    identifier = 'a' * 32
    directory = ensure_private_directory(store.root / 'jobs' / identifier)
    write_json(directory / 'state.json', {'id': identifier, 'status': 'provisioning', 'rental_state': 'none'})
    log_root = ensure_private_directory(tmp_path / 'logs')
    path = log_root / ('vast_' + identifier + '_provider.log')
    path.write_text('aaaaaaaaaaaa: Pulling fs layer\n')
    os.utime(path, (1000, 1000))
    monkeypatch.setattr(vast, 'CloudQueue', lambda: CloudQueue(store))
    monkeypatch.setattr(vast, 'CLOUD_LOG_ROOT', log_root)
    monkeypatch.setattr(vast, 'services_available', lambda: False)
    for _ in range(2):
        row = vast.jobs(Response(), None)['jobs'][0]
        assert row['provider_log_fetched_at'] == 1000
        assert row['image_progress']['downloaded'] == 0


@pytest.mark.parametrize('status', [401, 403, 429, 503])
def test_vast_domain_errors_are_distinct_from_browser_auth(client, monkeypatch, status):
    """Preserve provider status while explicitly identifying authenticated domain errors."""
    http, store, _ = client
    store.save(api_key='test-only')
    monkeypatch.setattr(vast, '_log', lambda *args, **kwargs: None)

    def provider_failure(self):
        """Simulate only a redacted provider failure."""
        raise VastError('Vast request rejected', status)

    monkeypatch.setattr(VastClient, 'account', provider_failure)
    response = http.post('/api/vast/account')
    assert response.status_code == status
    assert response.headers['x-pbgui-error-source'] == 'vast'
    assert response.json()['detail'] == 'Vast request rejected'


@pytest.mark.parametrize('status', [401, 403])
def test_browser_auth_error_has_no_vast_marker(client, status):
    """Auth failures are raised before the Vast route and cannot look like provider errors."""
    from fastapi import HTTPException
    http, _, app = client

    def expired_session():
        """Reject the browser without reading production authentication state."""
        raise HTTPException(status_code=status, detail='Authentication required')

    app.dependency_overrides[require_auth] = expired_session
    response = http.post('/api/vast/account')
    assert response.status_code == status
    assert 'x-pbgui-error-source' not in response.headers


def test_exact_offer_search_uses_contract_id(monkeypatch):
    """A selected offer remains discoverable despite general-search deduplication."""
    queries = []
    def request(self, method, path, body=None):
        """Model the provider returning another representative in a general search."""
        queries.append(body)
        identifier = 7 if body.get('ask_contract_id') == {'eq': 7} else 8
        return {'offers': [{'id': identifier, 'dph_total': .17}]}
    monkeypatch.setattr(VastClient, 'request', request)
    assert VastClient('fake').offers()[0]['id'] == 8
    assert VastClient('fake').offers(offer_id=7)[0]['id'] == 7
    assert queries[-1]['ask_contract_id'] == {'eq': 7}
    assert 'id' not in queries[-1]


@pytest.mark.parametrize('deleted', [False, True])
def test_requeue_after_interrupted_attempt_shows_latest_only(tmp_path, monkeypatch, deleted):
    """An old failed sibling must not override the newest replacement in polling."""
    from vast_jobs import JobStore, write_json
    from vast_queue import CloudQueue
    from secure_files import ensure_private_directory
    from fastapi import Response
    store = JobStore(tmp_path / 'queue')
    states = [
        dict(id='a'*32, status='failed', created_at=1, deleted_at=4 if deleted else None),
        dict(id='b'*32, status='failed', created_at=2, requeue_from='a'*32),
        dict(id='c'*32, status='preparing', created_at=3, requeue_from='a'*32),
    ]
    for state in states:
        directory = ensure_private_directory(store.root / 'jobs' / state['id'])
        write_json(directory / 'state.json', dict(rental_state='none', **state))
    monkeypatch.setattr(vast, 'CloudQueue', lambda: CloudQueue(store))
    monkeypatch.setattr(vast, 'services_available', lambda: False)
    assert [row['id'] for row in vast.jobs(Response(), None)['jobs']] == ['c'*32]


def test_deadline_route_requires_explicit_valid_request(client, monkeypatch):
    """The authenticated endpoint accepts bounded whole-minute adjustments."""
    http, _, _ = client
    calls = []
    monkeypatch.setattr('vast_deadline.request_deadline', lambda queue, worker, expected, minutes:
                        calls.append((worker, expected, minutes)) or {'pending':True})
    response = http.post('/api/vast/queue/deadline', json=dict(worker_id='a'*32, expected_deadline=8000, minutes=60))
    assert response.status_code == 202
    assert calls == [('a'*32, 8000, 60)]
    assert http.post('/api/vast/queue/deadline', json=dict(worker_id='a'*32, expected_deadline=8000, minutes=0)).status_code == 422
    assert http.post('/api/vast/queue/deadline', json=dict(worker_id='a'*32, expected_deadline=8000, minutes=1441)).status_code == 422


def test_jobs_statistics_use_existing_local_log_without_remote_access(client, monkeypatch, tmp_path):
    """Existing rentals gain measured throughput through the authenticated job snapshot."""
    from secure_files import ensure_private_directory
    from vast_jobs import JobStore, write_json
    from vast_queue import CloudQueue
    http, _, _ = client
    queue = CloudQueue(JobStore(tmp_path / 'queue'))
    monkeypatch.setattr(vast, 'CloudQueue', lambda: queue)
    monkeypatch.setattr(vast, 'services_available', lambda: True)
    logs = tmp_path / 'logs'
    logs.mkdir()
    monkeypatch.setattr(vast, 'CLOUD_LOG_ROOT', logs)
    identifier = 'c' * 32
    folder = ensure_private_directory(queue.root / 'jobs' / identifier)
    write_json(folder / 'state.json', {'id':identifier, 'status':'running', 'rental_state':'none'})
    logfile = logs / f'vast_{identifier}.log'
    logfile.write_text('2026-09-16T10:00:00Z INFO GPU optimize | gen=1 proxy=100 (1.0/s) exact=10 inflight=0\n'
                      '2026-09-16T10:01:00Z INFO GPU optimize | gen=2 proxy=700 (1.0/s) exact=40 inflight=0\n')
    first = http.get('/api/vast/jobs').json()['jobs'][0]['throughput']
    assert first['proxy_per_minute'] == 600
    assert first['exact_per_minute'] == 30
    assert http.get('/api/vast/jobs').json()['jobs'][0]['throughput'] == first
    logfile.unlink()
    logfile.symlink_to(tmp_path / 'outside.log')
    (tmp_path / 'outside.log').write_text('must not read this file')
    assert http.get('/api/vast/jobs').json()['jobs'][0]['throughput'] == first


def test_performance_api_retains_history_and_checks_comparison_identity(client):
    """Historical comparisons require authenticated, distinct IDs and a verified matching task."""
    from vast_performance import PerformanceHistory
    http, store, _ = client
    history = PerformanceHistory(store.root)
    for character, fingerprint in [('a','f'*64), ('b','f'*64), ('c','0'*64), ('d',None)]:
        history.record({'id':character*32, 'captured_at':1000, 'config_name':'history-only',
            'workload':{'fingerprint':fingerprint}, 'hardware':{'price_hour_usd':.5}},
            [{'sampled_at':1000,'proxy_total':100,'exact_total':10},
             {'sampled_at':1060,'proxy_total':700,'exact_total':40}])
    response = http.get('/api/vast/performance?limit=2')
    assert response.status_code == 200 and response.headers['cache-control'] == 'no-store'
    assert response.json()['total'] == 4 and len(response.json()['runs']) == 2
    assert http.get('/api/vast/performance?fingerprint=' + 'f'*64).json()['total'] == 2
    assert http.get('/api/vast/performance?fingerprint=invalid').status_code == 422
    assert http.post('/api/vast/performance/compare', json={'ids':['a'*32,'b'*32]}).status_code == 200
    assert http.post('/api/vast/performance/compare', json={'ids':['a'*32,'c'*32]}).status_code == 409
    config_comparison = http.post('/api/vast/performance/compare', json={'ids':['a'*32,'c'*32], 'mode':'config'})
    assert config_comparison.status_code == 200
    assert config_comparison.json()['mode'] == 'config'
    assert len(config_comparison.json()['runs']) == 2
    assert http.post('/api/vast/performance/compare', json={'ids':['a'*32,'c'*32], 'mode':'invalid'}).status_code == 422
    assert http.post('/api/vast/performance/compare', json={'ids':['a'*32,'d'*32]}).status_code == 409
    assert http.post('/api/vast/performance/compare', json={'ids':['d'*32]}).status_code == 200
    assert http.post('/api/vast/performance/compare', json={'ids':['a'*32,'a'*32]}).status_code == 422
    assert http.post('/api/vast/performance/compare', json={'ids':['../bad']}).status_code == 422
    assert http.post('/api/vast/performance/compare', json={'ids':['e'*32]}).status_code == 404


def test_performance_collector_lifecycle_is_owned_and_idempotent(monkeypatch, tmp_path):
    """API shutdown always stops and joins its collector without touching rentals."""
    import asyncio
    import vast_performance
    from vast_jobs import JobStore, write_json
    from vast_queue import CloudQueue
    stopped = []
    async def inline_to_thread(function, *args, **kwargs):
        """Keep this isolated lifecycle test independent of a process thread pool."""
        return function(*args, **kwargs)
    class Collector:
        """Track ownership without starting a real background thread."""
        def __init__(self, *args):
            """Accept isolated store arguments."""
        def start(self):
            """Record startup."""
            stopped.append('start')
        def stop(self):
            """Record cancellation signal."""
            stopped.append('stop')
        def join(self):
            """Record deterministic drain."""
            stopped.append('join')
    monkeypatch.setattr(vast, '_PREPARATION_STOPPING', False)
    monkeypatch.setattr(vast, '_PREPARATION_EXECUTOR', None)
    monkeypatch.setattr(vast, '_DELETION_EXECUTOR', None)
    monkeypatch.setattr(vast, '_PERFORMANCE_COLLECTOR', None)
    monkeypatch.setattr(vast_performance, 'PerformanceCollector', Collector)
    monkeypatch.setattr(vast.asyncio, 'to_thread', inline_to_thread)
    store = JobStore(tmp_path)
    monkeypatch.setattr(vast, 'JobStore', lambda: store)
    monkeypatch.setattr(vast, 'CloudQueue', lambda: CloudQueue(store))
    staged = tmp_path / 'jobs' / ('.' + 'a' * 32 + '.delete-interrupted')
    staged.mkdir(parents=True)
    (staged / 'input.tar.gz').write_bytes(b'partial old deletion')
    legacy = tmp_path / 'jobs' / ('b' * 32)
    legacy.mkdir()
    write_json(legacy / 'state.json', {'id': 'b' * 32, 'kind': 'job',
               'status': 'cancelled', 'rental_state': 'none', 'deleted_at': 1})
    (legacy / 'input.tar.gz').write_bytes(b'old hidden input')
    logs = tmp_path / 'logs'
    logs.mkdir()
    monkeypatch.setattr(vast, 'CLOUD_LOG_ROOT', logs)
    (logs / ('vast_' + 'b' * 32 + '.log')).write_text('old log')
    vast.startup(); vast.startup()
    async def shutdown_twice():
        """Exercise idempotence inside the API's single event-loop lifecycle."""
        await vast.shutdown()
        await vast.shutdown()
    asyncio.run(shutdown_twice())
    assert stopped == ['start', 'stop', 'join']
    assert vast._PREPARATION_EXECUTOR is None
    assert vast._DELETION_EXECUTOR is None
    assert vast._PERFORMANCE_COLLECTOR is None
    assert not staged.exists()
    assert not legacy.exists()
    assert not (logs / ('vast_' + 'b' * 32 + '.log')).exists()
