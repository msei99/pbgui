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
    monkeypatch.setattr(vast, "VastCredentialStore", lambda: store)
    app = FastAPI()
    app.include_router(vast.router, prefix="/api/vast")
    app.dependency_overrides[require_auth] = lambda: object()
    with TestClient(app) as test_client:
        yield test_client, store, app


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
            {'id':7,'gpu_name':'RTX 3090','gpu_ram':24576,'cpu_ram':32768,'cpu_cores_effective':8.7,'total_flops':35.58,'dph_total':0.17,'verification':'verified','inet_up_cost':0.01,'inet_down_cost':0.02,'secret':'hidden'},
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
              'bot':{'long':{},'short':{}},'backtest':{},
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
    """The API hides deleted entries and reports actual local log availability."""
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
    assert (logs / f'vast_{identifier}.log').exists()


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


@pytest.mark.parametrize('status,visible', [('preparing','b'), ('ready','b'), ('failed','a')])
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
    """The authenticated endpoint accepts only the two supported deadline steps."""
    http, _, _ = client
    calls = []
    monkeypatch.setattr('vast_deadline.request_deadline', lambda queue, worker, expected, minutes:
                        calls.append((worker, expected, minutes)) or {'pending':True})
    response = http.post('/api/vast/queue/deadline', json=dict(worker_id='a'*32, expected_deadline=8000, minutes=30))
    assert response.status_code == 202
    assert calls == [('a'*32, 8000, 30)]
    assert http.post('/api/vast/queue/deadline', json=dict(worker_id='a'*32, expected_deadline=8000, minutes=60)).status_code == 422
