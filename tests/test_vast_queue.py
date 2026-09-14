"""Offline shared-rental scheduling, queue cancellation and cached transfer contracts."""
import json
from pathlib import Path

import pytest

from secure_files import ensure_private_directory
from vast_jobs import JobStore, write_json, IMAGE, REVISION
from vast_queue import CloudQueue, worker_step
from vast_job_runner import rental_payload
from setup.vast_gpu_benchmark import cloud_worker


@pytest.fixture
def queue(tmp_path):
    """Make an isolated worker and two pending jobs without provider access."""
    store = JobStore(tmp_path / 'vast')
    queue = CloudQueue(store)
    worker = 'a' * 32
    for i, identifier in enumerate((worker, 'b'*32, 'c'*32)):
        directory = ensure_private_directory(store.root / 'jobs' / identifier)
        write_json(directory/'state.json', {'id':identifier,'kind':'worker' if i==0 else 'job',
                   'status':'running' if i==0 else 'ready','rental_state':'active' if i==0 else 'none',
                   'workers':4,'deadline':10000,'created_at':i,'input_bytes':100,'idle_seconds':300})
        write_json(directory/'control.json', {'stop':False,'cleanup':False})
        write_json(directory/'intent.json', {'id':identifier,'image':IMAGE,'pb8_revision':REVISION,
                   'transfer_reserve_usd':1,'offer':{'download_gb_usd':.01,'upload_gb_usd':.01}})
    queue.update(worker_id=worker)
    return queue, worker


def test_two_jobs_share_worker_and_deadline(queue):
    """Finishing one job starts the next without resetting or deleting the rental."""
    queue, worker = queue
    first = worker_step(queue, worker, now=100)
    assert first == 'b'*32
    queue.store.update(first, status='completed', final_collected=True)
    second = worker_step(queue, worker, now=200)
    assert second == 'c'*32
    assert queue.store.read(second)['lease_id'] == worker
    assert queue.store.read(worker)['deadline'] == 10000
    assert not queue.store.read(worker, 'control.json')['cleanup']
    assert worker_step(queue, worker, now=201) == second


def test_idle_timeout_and_new_job_cancel_countdown(queue):
    """The queue waits five minutes and resumes the same worker when work arrives."""
    queue, worker = queue
    for row in queue.waiting(): queue.store.update(row['id'], status='completed')
    assert worker_step(queue, worker, now=100) is None
    assert not queue.store.read(worker, 'control.json')['cleanup']
    queue.store.update('b'*32, status='ready')
    assert worker_step(queue, worker, now=350) == 'b'*32
    assert queue.store.read(worker)['idle_since'] is None
    queue.store.update('b'*32, status='completed')
    worker_step(queue, worker, now=400)
    worker_step(queue, worker, now=700)
    assert queue.store.read(worker, 'control.json')['cleanup']


def test_stop_job_does_not_mark_complete_before_collection(queue):
    """Cancellation cannot release the GPU while an old process still runs."""
    queue, worker = queue
    first = worker_step(queue, worker, now=100)
    queue.store.control(first, 'stop')
    assert queue.store.read(first)['status'] == 'provisioning'
    assert worker_step(queue, worker, now=110) == first
    assert not queue.store.read(worker, 'control.json')['cleanup']


def test_pause_and_end_never_start_next_job(queue):
    """A paused or ending rental cannot consume ready jobs."""
    queue, worker = queue
    queue.update(paused=True)
    assert worker_step(queue, worker, now=100) is None
    queue.store.control(worker, 'stop')
    assert worker_step(queue, worker, now=101) is None
    assert queue.store.read(worker, 'control.json')['cleanup']
    assert len(queue.waiting()) == 2


def test_rental_deadline_does_not_dispatch_more_jobs(queue):
    """Leave time for cleanup and retain waiting jobs for a future authorized rent."""
    queue, worker = queue
    assert worker_step(queue, worker, now=9800) is None
    assert len(queue.waiting()) == 2
    assert queue.store.read(worker, 'control.json')['cleanup']


def test_public_payload_never_sends_registry_credentials():
    """Even legacy saved registry credentials cannot leak into public rentals."""
    payload = rental_payload({'id':'a'*32,'image':IMAGE,'label':'pbgui-vast-'+'a'*32,
                             'deadline':10000,'offer':{'disk_gb':40}}, 'legacy_private_token')
    assert 'image_login' not in payload
    assert 'legacy_private_token' not in json.dumps(payload)
    assert len(payload['onstart'].encode()) < 1024
    assert 'cp /opt/pbgui/worker.py /work/pbgui/worker.py' in payload['onstart']
    assert 'b64decode' not in payload['onstart']


def test_cache_is_shared_and_revalidates_content(tmp_path, monkeypatch):
    """Two jobs reuse verified data but reject a corrupted shared blob."""
    root = tmp_path/'worker'; root.mkdir(); (root/'cache').mkdir()
    first = root/'jobs'/'first'; first.mkdir(parents=True)
    blob = first/'data'; blob.write_bytes(b'market-data')
    digest = cloud_worker.file_hash(blob)
    (root/'cache'/digest).write_bytes(blob.read_bytes())
    manifest = {'files':[{'path':'ohlcv/a.npz','bytes':11,'sha256':digest}]}
    monkeypatch.setattr(cloud_worker,'GUARD_ROOT',root)
    for name in ('first','second'):
        jobroot = root/'jobs'/name; jobroot.mkdir(exist_ok=True)
        (jobroot/'cache-request.json').write_text(json.dumps(manifest))
        monkeypatch.setattr(cloud_worker,'ROOT',jobroot)
        assert cloud_worker.cache_missing() == {'missing':[]}
    (root/'cache'/digest).write_bytes(b'bad')
    assert cloud_worker.cache_missing() == {'missing':[digest]}


def test_config_queue_honors_cloud_target_without_local_cuda(tmp_path, monkeypatch):
    """Queue Selected uses the saved cloud target and bypasses local CUDA checks."""
    from contextlib import nullcontext
    from types import SimpleNamespace
    from api import optimize_v8
    import vast_jobs
    config = {'pbgui':{'execution':'vast'},'optimize':{'iters':512,'n_cpus':4}}
    path = tmp_path/'config.json'; path.write_text(json.dumps(config))
    monkeypatch.setattr(optimize_v8,'_config_lock',nullcontext)
    monkeypatch.setattr(optimize_v8,'_config_file',lambda name:path)
    monkeypatch.setattr(optimize_v8,'load_pb8_config',lambda path:config)
    monkeypatch.setattr(optimize_v8,'_results_root',lambda:tmp_path/'optimize_results')
    monkeypatch.setattr(optimize_v8,'PBGDIR',str(tmp_path))
    def no_local_cuda(*args, **kwargs):
        """Fail if the local optimizer backend validator is incorrectly invoked."""
        raise AssertionError('Local CUDA validation must not run')
    monkeypatch.setattr(optimize_v8,'_validate_optimize_backend',no_local_cuda)
    calls=[]
    monkeypatch.setattr(vast_jobs,'JobStore',lambda:SimpleNamespace(prepare=lambda *args:calls.append(args) or {'id':'b'*32}))
    result=optimize_v8.add_to_queue({'name':'cloud-config'},session=None)
    assert result == {'ok':True,'filename':'b'*32,'execution':'vast'}
    assert calls[0][0] == 'cloud-config'
    assert calls[0][-3:] == (512,4,False)


def test_private_image_blocks_before_any_provider_call(queue, monkeypatch):
    """Image publication failures never cause a paid instance request."""
    import vast_jobs
    import vast_image
    from vast_provider import VastError
    queue, worker = queue
    queue.store.update(worker,rental_state='deletion_verified')
    monkeypatch.setattr(vast_jobs,'services_available',lambda:True)
    def unavailable(*args):
        """Simulate an inaccessible public manifest."""
        raise VastError('Image not public',409)
    monkeypatch.setattr(vast_image,'require_public_image',unavailable)
    def no_provider(*args):
        """No account or rental call is allowed before image verification."""
        raise AssertionError('Provider called before image verification')
    monkeypatch.setattr(vast_jobs,'VastClient',no_provider)
    with pytest.raises(VastError,match='Image not public'):
        queue.start({'id':42,'price_hour_usd':.15,'cuda_max_good':13,'disk_gb':40,'cpu_cores':8},1,1,300)
    assert queue.worker()['rental_state']=='none'


def test_recover_claim_after_controller_crash(queue):
    """A crash between claim persistence and worker update cannot orphan a job."""
    queue, worker = queue
    queue.store.update('b'*32,lease_id=worker,status='provisioning')
    assert worker_step(queue,worker,now=100)=='b'*32
    assert queue.store.read(worker)['active_job']=='b'*32
    assert queue.store.read('c'*32)['status']=='ready'


def test_repeated_start_reuses_authorization_and_reserves_remaining_budget(queue, monkeypatch):
    """One queue start creates one pair of supervisors and keeps spare transfer budget."""
    from types import SimpleNamespace
    import vast_jobs
    import vast_image
    queue, previous = queue
    queue.store.update(previous, rental_state='deletion_verified')
    monkeypatch.setattr(vast_jobs, 'services_available', lambda: True)
    monkeypatch.setattr(vast_image, 'require_public_image', lambda image: None)
    monkeypatch.setattr(vast_jobs, 'VastCredentialStore', lambda root: SimpleNamespace(secrets=lambda:{'api_key':'fake','generation':1}))
    monkeypatch.setattr(vast_jobs, 'VastClient', lambda key: SimpleNamespace(account=lambda:{'balance_usd':10}))
    launches=[]
    monkeypatch.setattr(queue.store,'launch_service',lambda identifier,mode:launches.append((identifier,mode)))
    offer={'id':42,'price_hour_usd':.15,'cuda_max_good':13,'disk_gb':40,'cpu_cores':8,
           'download_gb_usd':.01,'upload_gb_usd':.01}
    first=queue.start(offer,1,1,300)
    second=queue.start(offer,1,1,300)
    assert first['id']==second['id']
    assert launches==[(first['id'],'run'),(first['id'],'guard')]
    intent=queue.store.read(first['id'],'intent.json')
    assert intent['transfer_reserve_usd']==pytest.approx(.85)
    assert intent['deadline']-intent['accepted_at']==3600
    assert first['allocated_cpus']==8


def test_remove_waiting_job_prevents_claim_and_preserves_artifacts(queue):
    """Queue removal is durable, idempotent and leaves job data intact."""
    queue, worker = queue
    identifier = 'b' * 32
    assert queue.remove_job(identifier)['deleted']
    assert queue.remove_job(identifier)['deleted']
    assert queue.store.directory(identifier).exists()
    assert queue.store.read(identifier)['status'] == 'cancelled'
    assert worker_step(queue, worker, now=100) == 'c' * 32


def test_remove_claimed_job_is_rejected(queue):
    """A scheduler claim cannot be hidden by a subsequent deletion request."""
    from vast_provider import VastError
    queue, worker = queue
    identifier = worker_step(queue, worker, now=100)
    with pytest.raises(VastError, match='Stop the job'):
        queue.remove_job(identifier)
    with pytest.raises(VastError, match='Stop the job'):
        queue.remove_job(worker)
    assert not queue.store.read(identifier).get('deleted_at')


def test_convergence_preferences_are_frozen_at_dispatch(queue):
    """Setup changes affect subsequent jobs, never an already claimed run."""
    from vast_convergence import DEFAULTS
    queue, worker = queue
    settings = {**DEFAULTS, 'convergence_enabled': True, 'convergence_patience': 256}
    queue.update(gpu_preferences=settings)
    first = worker_step(queue, worker, now=100)
    assert queue.store.read(first)['convergence_config'] == settings
    queue.update(gpu_preferences={**settings, 'convergence_enabled': False})
    assert worker_step(queue, worker, now=150) == first
    assert queue.store.read(first)['convergence_config'] == settings
    queue.store.update(first, status='completed')
    second = worker_step(queue, worker, now=200)
    assert not queue.store.read(second)['convergence_config']['convergence_enabled']


def test_new_dispatch_resets_setup_timer_but_recovery_does_not(queue):
    """A fresh dispatch gets a setup window; polling never extends it or the lease."""
    queue, worker = queue
    job = 'b' * 32
    queue.store.update(job, setup_started_at=1, worker_ready=True, uploaded=False)
    assert worker_step(queue, worker, now=1500) == job
    assert queue.store.read(job)['setup_started_at'] is None
    queue.store.update(job, setup_started_at=1501)
    assert worker_step(queue, worker, now=1600) == job
    assert queue.store.read(job)['setup_started_at'] == 1501
    assert queue.store.read(worker)['deadline'] == 10000
