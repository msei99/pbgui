"""PB9 shares Vast controls without ever using PB8 jobs, images or provider calls."""
import json
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
    with pytest.raises(VastError):worker_profile('pb9').require_published()


def test_native_build_context_shares_recipe_and_excludes_private_files(tmp_path):
    """Only allowlisted public files enter the Docker context, including its shared worker."""
    from setup.vast_gpu_benchmark.build_worker_context import prepare
    context=prepare('pb9',tmp_path/'worker')
    assert sorted(p.name for p in context.iterdir())==['Dockerfile','cloud_worker.py','optimize_cli.py','pb9_optimize_options.py','run.py','worker_candidate.json']
    candidate=json.loads((context/'worker_candidate.json').read_text())
    assert candidate['base_revision']==worker_profile('pb9').revision
    assert candidate['runtime']=='pb9'
    assert candidate['overlays']==[]
    assert candidate['upstream_repository']==worker_profile('pb9').repository
    recipe=(context/'Dockerfile').read_text()
    assert 'COPY cloud_worker.py /opt/pbgui/worker.py' in recipe
    assert 'COPY optimize_cli.py pb9_optimize_options.py /opt/pbgui/' in recipe
    assert 'ENV PBGUI_WORKER_RUNTIME=pb9' in (context/'Dockerfile').read_text()
    assert (context/'cloud_worker.py').read_bytes()==(ROOT/'setup/vast_gpu_benchmark/cloud_worker.py').read_bytes()


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
