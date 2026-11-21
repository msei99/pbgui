"""PB9 shares Vast controls without ever using PB8 jobs, images or provider calls."""
import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api import vast_v9 as native, vast as shared
from api.auth import require_auth
from vast_jobs import JobStore
from vast_provider import VastError
from vast_worker_profiles import config_runtime,worker_profile

ROOT=Path(__file__).resolve().parents[1]


@pytest.fixture
def client(monkeypatch,tmp_path):
    """Use an isolated job store and replace authenticated ownership only."""
    import vast_worker_profiles
    monkeypatch.setattr(vast_worker_profiles, 'PB9', replace(vast_worker_profiles.PB9, image=None))
    store=JobStore(tmp_path/'vast')
    monkeypatch.setattr(native,'JobStore',lambda:store)
    monkeypatch.setattr(shared,'JobStore',lambda:store)
    app=FastAPI();app.include_router(native.router,prefix='/api/vast-v9')
    app.dependency_overrides[require_auth]=lambda:SimpleNamespace(user='test')
    with TestClient(app) as client:
        yield client,store


def test_profile_pending_does_not_read_credentials_or_call_provider(client,monkeypatch):
    """All paid entry points reject before even consulting an API credential."""
    client,store=client
    monkeypatch.setattr(shared,'VastClient',lambda *_:pytest.fail('provider called'))
    monkeypatch.setattr(shared,'VastCredentialStore',lambda *_:pytest.fail('credential read'))
    response=client.get('/api/vast-v9/profile')
    assert response.json()['published'] is False
    response=client.post('/api/vast-v9/jobs/prepare',json={'config_name':'native'})
    assert response.status_code==503
    assert not store.root.exists()
    response=client.post('/api/vast-v9/queue/start',json={'accept_rental_and_cleanup':True})
    assert response.status_code==503
    assert not store.root.exists()


def test_native_job_controls_reject_legacy_job_ids(client):
    """Even an authenticated native page cannot act on a legacy rental by ID."""
    client,store=client
    row=store.create_preparation('legacy',512,4,False)
    response=client.post('/api/vast-v9/jobs/'+row['id']+'/stop')
    assert response.status_code==404
    assert store.read(row['id'])['status']=='preparing'


def test_shared_preparation_records_runtime_atomically(tmp_path):
    """PB9 ownership is part of initial durable job publication, with PB8 unchanged."""
    store=JobStore(tmp_path/'vast')
    native=store.create_preparation('native',512,4,False,runtime='pb9')
    legacy=store.create_preparation('legacy',512,4,False)
    assert store.read(native['id'])['runtime']=='pb9'
    assert 'runtime' not in store.read(legacy['id'])
    with pytest.raises(VastError):store.create_preparation('unknown',512,4,False,runtime='pb10')


@pytest.mark.parametrize('value', ['v10.0.0','v9',7,None])
def test_unknown_worker_profile_never_uses_pb8(value):
    """Only a missing legacy schema remains an intentional compatibility default."""
    if value is None:assert config_runtime({})=='pb8'
    else:
        with pytest.raises(VastError):config_runtime({'config_version':value})
    assert config_runtime({'config_version':'v9.0.0'})=='pb9'
    assert worker_profile('pb8').published
    with pytest.raises(VastError):replace(worker_profile('pb9'), image=None).require_published()


def test_native_published_profile_is_bound_to_reviewed_digest_and_source():
    """Native rentals use the exact verified native image, independently of PB8."""
    profile = worker_profile('pb9').require_published()
    assert profile.image == 'ghcr.io/msei99/pbgui-pb9-worker@sha256:7ee6a7bf0f6bb0c235e87ab2988efd1d2f0529f4348f1d9c20a13d8384395bcb'
    assert profile.revision == 'bf7329b039595e65acdf5c14475078912aeede96'
    assert profile.image != worker_profile('pb8').image
    from vast_image_layers import IMAGE_LAYERS
    assert len(IMAGE_LAYERS[profile.image]) == 62
    assert all(size > 0 for size in IMAGE_LAYERS[profile.image].values())


def test_previous_pb9_rental_identity_survives_worker_update():
    """An updated current worker cannot orphan already persisted PR-2305 rentals."""
    from vast_jobs import SUPPORTED_RENTAL_IMAGE_REVISIONS
    previous = 'ghcr.io/msei99/pbgui-pb9-worker@sha256:9a8a4bbb6c5dfb8f927e1652dd52894da83f4bff03b74d0422814d0b3941d99b'
    assert SUPPORTED_RENTAL_IMAGE_REVISIONS[previous] == '1b5805ed52c9fdf8eab6d21ed938e2c73edda85b'


def test_native_calibration_rejects_before_credentials_or_provider(tmp_path, monkeypatch):
    """Image publication must not authorize a legacy calibration on native GPU cohorts."""
    from fastapi import HTTPException
    from vast_calibration import calibration_worker_available, NATIVE_CALIBRATION_UNAVAILABLE
    monkeypatch.setattr('vast_calibration.NATIVE_CALIBRATION_IMAGES', frozenset())
    assert worker_profile('pb9').published
    assert not calibration_worker_available(worker_profile('pb9').image)
    assert not calibration_worker_available(worker_profile('pb9').image, protocol=4)
    assert not calibration_worker_available(None)
    monkeypatch.setattr(shared, 'CloudQueue', lambda: pytest.fail('Queue consulted'))
    monkeypatch.setattr(shared, 'VastCredentialStore', lambda *_: pytest.fail('Credentials read'))
    monkeypatch.setattr(shared, 'VastClient', lambda *_: pytest.fail('Provider called'))
    for endpoint in (shared._start_calibration, shared._start_configurable_calibration,
                     shared._start_calibration_watch):
        with pytest.raises(HTTPException) as error:
            endpoint(SimpleNamespace(accept_rental_and_cleanup=True), runtime='pb9')
        assert error.value.status_code == 409
        assert error.value.detail == NATIVE_CALIBRATION_UNAVAILABLE
    store = JobStore(tmp_path / 'vast')
    row = store.create_preparation('native', 512, 4, False, runtime='pb9')
    store.update(row['id'], status='ready')
    with pytest.raises(VastError, match='PB9 performance tuning requires'):
        store.create_calibration(row['id'], {})
    store.update(row['id'], kind='calibration')
    monkeypatch.setattr('vast_jobs.services_available', lambda: True)
    with pytest.raises(VastError, match='PB9 performance tuning requires'):
        store.start(row['id'], {'id': 1, 'price_hour_usd': .1, 'cuda_max_good': 13,
                               'disk_gb': 40, 'cpu_cores': 4}, .25, .1)


@pytest.mark.parametrize('legacy_count', [1, 101])
def test_native_job_list_skips_legacy_snapshots_before_projection(client, monkeypatch, legacy_count):
    """PB8 work cannot delay or consume the native list's one-hundred-row limit."""
    from vast_queue import CloudQueue
    client, store = client
    monkeypatch.setattr(shared, 'CloudQueue', lambda: CloudQueue(store))
    monkeypatch.setattr(shared, 'services_available', lambda: True)
    native_row = store.create_preparation('native', 512, 4, False, runtime='pb9')
    store.update(native_row['id'], status='ready', exchanges=['binance'], estimated_coin_candles=1234)
    native_input = store.directory(native_row['id']) / 'input'
    native_input.mkdir()
    (native_input / 'optimize.json').write_text('{}')
    for index in range(legacy_count):
        row = store.create_preparation('legacy_' + str(index), 512, 4, False)
        store.update(row['id'], status='completed', exchanges=['bybit'])
        folder = store.directory(row['id']) / 'input'
        folder.mkdir()
        (folder / 'optimize.json').write_text('{}')
    monkeypatch.setattr(shared, '_cached_display_estimate', lambda *args: pytest.fail('PB8 snapshot inspected'))
    response = client.get('/api/vast-v9/jobs')
    assert response.status_code == 200, response.text
    assert [row['id'] for row in response.json()['jobs']] == [native_row['id']]
    assert response.json()['jobs'][0]['estimated_coin_candles'] == 1234


def test_legacy_list_never_inspects_native_snapshots(client, monkeypatch):
    """The PB8 list filters native rows before row limits and snapshot estimates."""
    from fastapi import Response
    from vast_queue import CloudQueue
    _client, store = client
    monkeypatch.setattr(shared, 'CloudQueue', lambda: CloudQueue(store))
    monkeypatch.setattr(shared, 'services_available', lambda: True)
    legacy = store.create_preparation('legacy', 512, 4, False)
    native = store.create_preparation('native', 512, 4, False, runtime='pb9')
    for row in (legacy, native):
        store.update(row['id'], status='ready')
    folder = store.directory(native['id']) / 'input'
    folder.mkdir()
    (folder / 'optimize.json').write_text('{}')
    monkeypatch.setattr(shared, '_cached_display_estimate', lambda *args: pytest.fail('Native snapshot inspected'))
    assert [row['id'] for row in shared.jobs(Response(), None)['jobs']] == [legacy['id']]


def test_native_estimate_failures_remain_optional(tmp_path, monkeypatch):
    """A failed native source inspection cannot fail a display-only estimate."""
    from api.optimize_v9 import engine
    from pb9_config import PB9ConfigError
    path = tmp_path / 'native.json'
    path.write_text('{}')
    def broken_context():
        """Simulate a missing native source without reading local runtimes."""
        raise PB9ConfigError('runtime_not_detected')
    monkeypatch.setattr(engine.native, '_workload_context', broken_context)
    assert engine.native.estimate_snapshot(path, tmp_path) is None


def test_native_build_context_shares_recipe_and_excludes_private_files(tmp_path):
    """Only allowlisted public files enter the Docker context, including its shared worker."""
    from setup.vast_gpu_benchmark.build_worker_context import prepare
    context=prepare('pb9',tmp_path/'worker')
    assert sorted(p.name for p in context.iterdir())==['Dockerfile','cloud_worker.py','optimize_cli.py','pb9_optimize_options.py','run.py','vast_native_calibration.py','verify_native_calibration.py','worker_candidate.json']
    candidate=json.loads((context/'worker_candidate.json').read_text())
    assert candidate['base_revision']==worker_profile('pb9').revision
    assert candidate['runtime']=='pb9'
    assert candidate['overlays']==[]
    assert candidate['upstream_repository']==worker_profile('pb9').repository
    recipe=(context/'Dockerfile').read_text()
    assert 'COPY cloud_worker.py /opt/pbgui/worker.py' in recipe
    assert 'COPY optimize_cli.py pb9_optimize_options.py vast_native_calibration.py /opt/pbgui/' in recipe
    assert 'ENV PBGUI_WORKER_RUNTIME=pb9' in (context/'Dockerfile').read_text()
    assert (context/'cloud_worker.py').read_bytes()==(ROOT/'setup/vast_gpu_benchmark/cloud_worker.py').read_bytes()


def test_native_candidate_can_target_merged_source_without_changing_active_profile(tmp_path):
    """Build a candidate against a new source while paid jobs retain the old pin."""
    from setup.vast_gpu_benchmark.build_worker_context import prepare
    previous = worker_profile('pb9')
    revision = 'a' * 40
    context = prepare('pb9', tmp_path/'candidate', revision=revision,
                      repository='https://github.com/enarjord/passivbot', reuse_runtime_base=True)
    provenance = json.loads((context/'worker_candidate.json').read_text())
    recipe = (context/'Dockerfile').read_text()
    assert provenance['base_revision'] == revision
    assert provenance['runtime_base'] == previous.image
    assert f'FROM {previous.image} AS runtime' in recipe
    assert 'RUN rm -rf /opt/passivbot' in recipe
    assert 'verify_native_calibration.py' in recipe
    assert 'https://github.com/enarjord/passivbot.git' in recipe
    assert worker_profile('pb9') == previous


@pytest.mark.parametrize('options', [
    {'revision':'main'}, {'revision':'a'*40+';touch /tmp/bad'},
    {'repository':'https://example.invalid/unreviewed'},
    {'repository':'https://token@github.com/enarjord/passivbot'},
])
def test_native_candidate_rejects_untrusted_build_overrides_before_writes(tmp_path, options):
    """Candidate source arguments cannot inject Docker commands or private sources."""
    from setup.vast_gpu_benchmark.build_worker_context import prepare
    with pytest.raises(ValueError):
        prepare('pb9', tmp_path/'candidate', **options)
    assert not (tmp_path/'candidate').exists()


def test_native_gpu_tuning_keeps_native_fields_and_explicit_policy():
    """PB8 auto-sizing must not reintroduce population_size/exact_workers inside native GPU."""
    from vast_gpu_tuning import resolve_gpu_settings
    config=json.loads((ROOT/'tests/fixtures/pb9_optimize_metadata.json').read_text())['template']
    config['optimize']['backend']='gpu'
    source=json.loads(json.dumps(config))
    record=resolve_gpu_settings(config,{'hardware':{'vram_bytes':24*1024**3}}, {'gpu_name':'fixture'})
    assert config==source
    assert record['runtime']=='pb9'
    assert record['applied_by']=='native_pb9'
    assert not {'exact_workers','population_size','auto_lean_parallelism'} & config['optimize']['gpu'].keys()
    assert len(record['fingerprint'])==64


def test_native_snapshot_contract_rejects_legacy_fields():
    """Frozen native jobs are checked independently of the local CUDA/runtime installation."""
    from vast_pb9 import snapshot_errors
    metadata=json.loads((ROOT/'tests/fixtures/pb9_optimize_metadata.json').read_text())
    config=metadata['template'];config['backtest']['offline']=True;config['optimize']['backend']='gpu'
    assert snapshot_errors(config,worker_profile('pb9').revision)==[]
    config['optimize']['gpu']['exact_workers']=4
    assert any(item['path']=='optimize.gpu' for item in snapshot_errors(config,worker_profile('pb9').revision))
    assert any(item['path']=='config_version' for item in snapshot_errors(config,'wrong-source'))


def test_native_rental_sizing_retains_authored_audit_and_retries_from_source():
    """Transfer retries derive the same launch policy from the immutable authored snapshot."""
    import copy
    from vast_gpu_tuning import resolve_gpu_settings
    source=json.loads((ROOT/'tests/fixtures/pb9_optimize_metadata.json').read_text())['template']
    source['optimize']['gpu']['tuning_mode']='conservative'
    override={'population_size':6656,'batch_size':6656,'max_dispatch_candidate_bars':4_000_000_000}
    results=[]
    for _ in range(2):
        launch=copy.deepcopy(source)
        record=resolve_gpu_settings(launch,{'hardware':{'gpu':'fixture'}},{'gpu_name':'fixture'},override)
        assert record['preserved']==source['optimize']['gpu']
        assert record['automatic']['population_size']==6656
        assert record['automatic']['tuning_mode']=='off'
        assert launch['optimize']['population_size']==6656
        assert not {'population_size','exact_workers'} & launch['optimize']['gpu'].keys()
        results.append((launch,record))
    assert results[0]==results[1]


def test_native_paid_controls_reject_body_ids_owned_by_legacy(client):
    """Body identifiers are checked before profile/provider lookup, just like URL identifiers."""
    client,store=client
    legacy=store.create_preparation('legacy',512,4,False)
    response=client.post('/api/vast-v9/queue/start',json={'accept_rental_and_cleanup':True,
        'gpu_job_profile_overrides':{legacy['id']:{'population_size':6656,'batch_size':6656,
        'max_dispatch_candidate_bars':4_000_000_000}}})
    assert response.status_code==404,response.text


def test_native_performance_remains_readable_after_job_removal(client,monkeypatch):
    """Retained native history is a free read and must not depend on published images/jobs."""
    from vast_performance import PerformanceHistory
    from vast_queue import CloudQueue
    client,store=client;history=PerformanceHistory(store.root)
    monkeypatch.setattr(shared,'CloudQueue',lambda:CloudQueue(store=store))
    for runtime,identifier in [('pb8','a'*32),('pb9','b'*32)]:
        history.record({'id':identifier,'created_at':1000,'captured_at':1000,
            'workload':{'runtime':runtime,'fingerprint':'f'*64},'hardware':{}},[])
    response=client.get('/api/vast-v9/performance')
    assert response.status_code==200,response.text
    assert [row['id'] for row in response.json()['runs']]==['b'*32]
    response=client.post('/api/vast-v9/performance/compare',json={'ids':['b'*32]})
    assert response.status_code==200,response.text
    assert not (store.root/'jobs'/('b'*32)).exists()
    assert client.post('/api/vast-v9/performance/compare',json={'ids':['a'*32]}).status_code==404
