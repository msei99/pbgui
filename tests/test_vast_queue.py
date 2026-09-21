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


def test_deadline_idle_policy_keeps_reusable_worker(queue):
    """Deadline retention never requests cleanup merely because the queue is idle."""
    queue, worker = queue
    for row in queue.waiting():
        queue.store.update(row['id'], status='completed')
    queue.store.update(worker, idle_seconds=-1)
    assert worker_step(queue, worker, now=100) is None
    assert worker_step(queue, worker, now=9_000) is None
    assert not queue.store.read(worker, 'control.json')['cleanup']


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


def test_cached_worker_input_is_hard_linked_after_verification(tmp_path):
    """Reused immutable input must avoid another multi-gigabyte byte copy."""
    cached = tmp_path / 'cache' / 'blob'
    cached.parent.mkdir()
    cached.write_bytes(b'market-data')
    target = tmp_path / 'job' / 'ohlcv' / 'a.npz'
    checksum = cloud_worker.file_hash(cached)
    cloud_worker.install_cached_file(target, cached, cached.stat().st_size, checksum)
    assert target.read_bytes() == b'market-data'
    assert target.stat().st_ino == cached.stat().st_ino


def test_cached_worker_input_rejects_corruption(tmp_path):
    """A wrong cache key cannot be linked into a prepared optimizer job."""
    cached = tmp_path / 'cache' / 'blob'
    cached.parent.mkdir()
    cached.write_bytes(b'corrupt')
    with pytest.raises(ValueError, match='missing or corrupt'):
        cloud_worker.install_cached_file(tmp_path / 'job' / 'a.npz', cached, 7, '0' * 64)


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


def test_manual_rental_waits_for_start_and_then_uses_idle_policy(queue):
    """Reservation survives idle timeout but releases normally after the first run."""
    queue, worker = queue
    queue.update(paused=True)
    queue.store.update(worker, awaiting_queue_start=True, idle_seconds=0)
    assert worker_step(queue, worker, now=100) is None
    assert worker_step(queue, worker, now=1000) is None
    assert not queue.store.read(worker, 'control.json')['cleanup']
    assert queue.store.read(worker)['status'] == 'reserved'
    queue.update(paused=False)
    assert worker_step(queue, worker, now=1001) == 'b'*32
    assert queue.store.read(worker)['awaiting_queue_start'] is False
    for item in ('b'*32, 'c'*32):
        queue.store.update(item, status='completed')
    worker_step(queue, worker, now=1002)
    assert queue.store.read(worker, 'control.json')['cleanup']


def test_resume_repairs_manual_reservation_after_partial_start(queue, monkeypatch):
    """Resume clears the manual start gate even when the queue is already unpaused."""
    queue, worker = queue
    queue.update(paused=False)
    queue.store.update(worker, awaiting_queue_start=True, status='reserved')
    monkeypatch.setattr(queue.store, 'launch_service', lambda *args: None)
    queue.action('resume')
    assert queue.read()['paused'] is False
    assert queue.store.read(worker)['awaiting_queue_start'] is False


@pytest.mark.parametrize('stop,now', [(True, 100), (False, 9900)])
def test_manual_rental_can_end_and_cannot_outlive_deadline(queue, stop, now):
    """Manual reservation never suppresses explicit release or deadline cleanup."""
    queue, worker = queue
    queue.update(paused=True)
    queue.store.update(worker, awaiting_queue_start=True)
    if stop:
        queue.store.control(worker, 'stop')
    worker_step(queue, worker, now=now)
    assert queue.store.read(worker, 'control.json')['cleanup']


def test_rent_without_jobs_starts_services_immediately(tmp_path, monkeypatch):
    """Manual rental creates a paused supervised worker even with an empty queue."""
    queue = CloudQueue(JobStore(tmp_path/'vast'))
    calls = []
    def start(identifier, offer, hours, budget):
        """Capture the rental boundary without launching real services."""
        calls.append(identifier)
        return queue.store.update(identifier, rental_state='creation_pending')
    monkeypatch.setattr(queue.store, 'start', start)
    first = queue.start({'id':123}, 1, 1, 300, manual=True)
    second = queue.start({'id':456}, 1, 1, 300, manual=True)
    assert calls == [first['id']]
    assert second['id'] == first['id']
    assert first['awaiting_queue_start']
    assert queue.read()['paused']


def test_deleted_worker_finishes_claimed_job_and_preserves_waiting_queue(queue):
    """Release a disappeared lease even when its job controller has not returned."""
    queue, worker = queue
    claimed = worker_step(queue, worker, now=100)
    queue.store.update(claimed, status='running', result_path='saved-local-snapshot')
    queue.store.update(worker, rental_state='deletion_verified', rental_end_reason='provider_instance_missing')
    assert worker_step(queue, worker, now=110) is None
    assert queue.store.read(claimed)['status'] == 'failed'
    assert queue.store.read(claimed)['result_path'] == 'saved-local-snapshot'
    assert 'disappeared' in queue.store.read(claimed)['error']
    assert queue.store.read('c'*32)['status'] == 'ready'
    assert queue.store.read(worker)['active_job'] is None
    assert queue.store.read(worker)['rental_state'] == 'deletion_verified'
    assert queue.read()['paused']
    assert worker_step(queue, worker, now=120) is None


def test_deleted_worker_keeps_collected_results_recoverable(queue):
    """An interrupted local import retains its final snapshot and retry metadata."""
    queue, worker = queue
    claimed = worker_step(queue, worker, now=100)
    queue.store.update(claimed, status='collecting', final_collected=True)
    queue.store.update(worker, rental_state='deletion_verified', rental_end_reason='provider_instance_missing')
    worker_step(queue, worker, now=110)
    job = queue.store.read(claimed)
    assert job['final_collected'] and job['status'] == 'failed'
    assert job['error'] == 'Raw results saved locally; result import needs retry'


def test_guard_closes_missing_shared_rental_without_worker_controller(queue):
    """The guard alone releases a lost shared lease and finishes its claimed job."""
    from vast_job_runner import guard_step
    queue, worker = queue
    claimed = worker_step(queue, worker, now=100)
    queue.store.update(worker, instance_id=123)
    intent = {**queue.store.read(worker, 'intent.json'), 'label':'pbgui-vast-'+worker,
              'accepted_at':0, 'deadline':10000}

    class MissingProvider:
        """A fresh successful empty account; no mutation method is supplied."""
        def instances(self, *, fresh=False):
            """Reject cached reads and never return an owned instance."""
            assert fresh
            return []

    assert not guard_step(queue.store, worker, MissingProvider(), intent, '', now=200)
    assert queue.store.read(claimed)['status'] not in {'failed', 'cancelled'}
    assert guard_step(queue.store, worker, MissingProvider(), intent, '', now=210)
    assert queue.store.read(worker)['rental_state'] == 'deletion_verified'
    assert queue.store.read(claimed)['status'] == 'failed'
    assert queue.read()['paused']
    assert queue.store.read('c'*32)['status'] == 'ready'
    # Re-running verification is idempotent and must not change the waiting job.
    assert guard_step(queue.store, worker, MissingProvider(), intent, '', now=220)
    assert queue.store.read('c'*32)['status'] == 'ready'
