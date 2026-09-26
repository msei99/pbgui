"""Offline multi-rental scheduling and authorization regression contracts."""
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
import time

import pytest

from api.vast import RentalPreferences
from pydantic import ValidationError
from secure_files import ensure_private_directory
from vast_jobs import JobStore, write_json
from vast_provider import VastError
from vast_queue import CloudQueue, worker_step
import vast_pool as pool


def add_row(queue, number, *, worker=False, **changes):
    """Persist one isolated queue record with no external services."""
    identifier = f'{number:032x}'
    directory = ensure_private_directory(queue.root / 'jobs' / identifier)
    write_json(directory / 'state.json', dict(id=identifier, kind='worker' if worker else 'job',
        status='provisioning' if worker else 'ready', rental_state='creation_pending' if worker else 'none',
        workers=4, deadline=time.time()+3600, created_at=number, idle_seconds=300, input_bytes=100, **changes))
    write_json(directory / 'control.json', {'stop':False, 'cleanup':False})
    write_json(directory / 'intent.json', {'transfer_reserve_usd':1,
        'offer':{'download_gb_usd':.01, 'upload_gb_usd':.01}})
    return identifier


@pytest.fixture
def queue(tmp_path, monkeypatch):
    """Enable a three-GPU pool with every process/provider boundary replaced."""
    queue = CloudQueue(JobStore(tmp_path / 'vast'))
    settings = RentalPreferences(max_rentals=3).model_dump()
    queue.update(pool_enabled=True, paused=False, pool_authorization={'id':'a'*32, 'settings':settings, 'credential_generation':1})
    monkeypatch.setattr(queue.store, 'launch_service', lambda *args: None)
    monkeypatch.setattr(pool, 'launch_pool', lambda *args: None)
    monkeypatch.setattr(pool, 'VastCredentialStore', lambda *args: SimpleNamespace(
        metadata=lambda: {'configured':True, 'generation':1}, secrets=lambda: {'api_key':'fake'}))
    return queue


@pytest.mark.parametrize('value', [0, 17, -1, 1.5, True, '3'])
def test_invalid_cap_rejected(value):
    """API and persisted authorization reject ambiguous or unbounded limits."""
    with pytest.raises(ValidationError):
        RentalPreferences(max_rentals=value)
    with pytest.raises(VastError):
        pool.pool_limit({'pool_authorization':{'settings':{'max_rentals':value}}})


def test_pending_and_closing_rentals_count_toward_limit(queue):
    """Provisioning and cleanup cannot temporarily exceed the paid rental cap."""
    for i in range(10, 15):
        add_row(queue, i)
    leases = [add_row(queue, i, worker=True) for i in range(1, 4)]
    queue.store.update(leases[0], rental_state='destroy_pending')
    assert not pool.rental_needed(queue, 'a'*32)
    queue.store.update(leases[0], rental_state='deletion_verified')
    assert pool.rental_needed(queue, 'a'*32)


def test_idle_capacity_and_cpu_compatibility(queue):
    """Reuse a compatible free worker but rent when idle CPUs cannot fit a job."""
    job = add_row(queue, 10)
    worker = add_row(queue, 1, worker=True)
    assert not pool.rental_needed(queue, 'a'*32)
    queue.store.update(job, workers=8)
    assert pool.rental_needed(queue, 'a'*32)
    queue.store.update(worker, allocated_cpus=8)
    assert not pool.rental_needed(queue, 'a'*32)
    queue.store.control(worker, 'stop')
    assert pool.rental_needed(queue, 'a'*32)


def test_busy_worker_and_parallel_claims(queue):
    """Three supervisors claim distinct jobs and further jobs keep FIFO order."""
    leases = [add_row(queue, i, worker=True) for i in range(1, 4)]
    jobs = [add_row(queue, i) for i in range(10, 15)]
    with ThreadPoolExecutor(max_workers=3) as executor:
        claimed = list(executor.map(lambda lease: worker_step(queue, lease), leases))
    assert set(claimed) == set(jobs[:3])
    assert len(queue.waiting()) == 2
    queue.store.update(claimed[0], status='completed', final_collected=True)
    assert worker_step(queue, leases[0]) == jobs[3]
    assert queue.worker_for(leases[1])['id'] == leases[1]


@pytest.mark.parametrize('action', ['pause', 'end'])
def test_pause_or_end_during_offer_search_cannot_rent(queue, monkeypatch, action):
    """Consent is rechecked under the dispatch lock after a slow offer lookup."""
    add_row(queue, 10)
    def select(*args):
        """Simulate user control arriving while provider search is outstanding."""
        queue.action(action)
        return {'id':1, 'machine_id':1}
    monkeypatch.setattr(pool, 'select_offer', select)
    with pytest.raises(VastError, match='authorization changed'):
        pool.pool_step(queue)
    assert not queue.workers()


def test_end_stops_every_lease_and_disables_replacements(queue):
    """End persists stop for all rentals, including pending creation."""
    leases = [add_row(queue, i, worker=True) for i in range(1, 4)]
    add_row(queue, 10)
    queue.action('end')
    assert queue.read()['pool_enabled'] is False
    assert queue.read()['paused'] is True
    assert all(queue.store.read(lease, 'control.json')['stop'] for lease in leases)
    assert pool.pool_step(queue) is None


def test_replace_targets_one_gpu_and_releases_unstarted_input_after_cleanup(queue):
    """A pool replacement waits for verified deletion before redispatching its input."""
    first = add_row(queue, 1, worker=True)
    second = add_row(queue, 2, worker=True)
    job = add_row(queue, 10)
    queue.store.update(job, status='uploading', lease_id=first, exact_completed=0,
                       downloaded_bytes=0, upload_progress={'stage':'sending', 'bytes':50, 'total':100})
    queue.store.update(first, active_job=job)
    state = queue.read()
    state['gpu_preferences'] = RentalPreferences(max_rentals=3, auto_rent=True).model_dump()
    queue.update(gpu_preferences=state['gpu_preferences'])

    queue.worker_action(first, 'replace')

    assert queue.store.read(first, 'control.json')['stop'] is True
    assert queue.store.read(second, 'control.json')['stop'] is False
    assert queue.read()['pool_enabled'] is True
    assert queue.store.read(job)['status'] == 'uploading'

    queue.store.update(job, status='cancelled')
    queue.store.update(first, rental_state='deletion_verified')
    worker_step(queue, first)

    retried = queue.store.read(job)
    assert retried['status'] == 'ready'
    assert retried.get('lease_id') is None
    assert retried.get('upload_progress') is None
    assert queue.store.read(job, 'control.json') == {'stop':False, 'cleanup':False}


def test_replace_preserves_started_results_for_explicit_requeue(queue):
    """Automatic replacement cannot erase a run that already produced exact results."""
    worker = add_row(queue, 1, worker=True)
    job = add_row(queue, 10)
    queue.store.update(job, status='cancelled', lease_id=worker, exact_completed=3,
                       downloaded_bytes=0)
    queue.store.update(worker, active_job=job, replacement_requested=True, replacement_job=job,
                       rental_state='deletion_verified')

    worker_step(queue, worker)

    preserved = queue.store.read(job)
    assert preserved['status'] == 'cancelled'
    assert preserved['exact_completed'] == 3


def test_reconstruction_and_lower_limit_preserve_existing_rentals(queue):
    """Reload discovers every lease; a lower limit waits without killing any."""
    leases = [add_row(queue, i, worker=True) for i in range(1, 4)]
    add_row(queue, 10)
    state = queue.read()
    state['pool_authorization']['settings']['max_rentals'] = 1
    queue.update(pool_authorization=state['pool_authorization'])
    reloaded = CloudQueue(JobStore(queue.root))
    assert len(reloaded.workers()) == 3
    assert not pool.rental_needed(reloaded, 'a'*32)
    assert not any(reloaded.store.read(lease, 'control.json')['stop'] for lease in leases)


def test_real_start_admits_three_and_no_more(queue, monkeypatch):
    """Exercise durable start and cap checks while mocking all paid boundaries."""
    import vast_jobs
    import vast_image
    monkeypatch.setattr('vast_queue.preflight_local_metadata', lambda *args: None)
    monkeypatch.setattr(vast_jobs, 'services_available', lambda: True)
    monkeypatch.setattr(vast_image, 'require_public_image', lambda *args: None)
    monkeypatch.setattr(vast_jobs, 'VastCredentialStore', lambda *args: SimpleNamespace(
        secrets=lambda: {'api_key':'fake', 'generation':1}))
    monkeypatch.setattr(vast_jobs, 'VastClient', lambda *args: SimpleNamespace(account=lambda: {'balance_usd':100}))
    offer = {'id':1, 'machine_id':1, 'price_hour_usd':.1, 'cuda_max_good':13,
             'cpu_cores':8, 'disk_gb':40, 'download_gb_usd':.001, 'upload_gb_usd':.001}
    monkeypatch.setattr(pool, 'select_offer', lambda *args: offer)
    for i in range(10, 15):
        add_row(queue, i)
    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(lambda _: pool.pool_step(queue), range(4)))
    assert len([row for row in results if row]) == 3
    assert len(queue.workers()) == 3
    assert all(row['rental_state'] == 'creation_pending' for row in queue.workers())
    assert pool.pool_step(queue) is None
    for row in queue.workers():
        queue.store.update(row['id'], rental_state='deletion_verified')
    state = queue.read()
    state['pool_authorization']['credential_generation'] = 0
    queue.update(pool_authorization=state['pool_authorization'])
    with pytest.raises(VastError, match='credentials changed'):
        pool.pool_step(queue)
    assert not queue.workers()


def test_api_requires_consent_and_authorizes_saved_pool(queue, monkeypatch):
    """Saving settings is inert; explicit Start durably authorizes their snapshot."""
    from api import vast
    from fastapi import HTTPException
    monkeypatch.setattr(vast, 'CloudQueue', lambda: queue)
    monkeypatch.setattr(pool, 'services_available', lambda: True)
    queue.update(pool_enabled=False)
    vast.save_gpu_preferences(RentalPreferences(max_rentals=3), session=None)
    assert not queue.read()['pool_enabled']
    with pytest.raises(HTTPException):
        vast.start_queue(vast.StartJobRequest(use_saved_settings=True), session=None)
    result = vast.start_queue(vast.StartJobRequest(use_saved_settings=True,
        accept_rental_and_cleanup=True), session=None)
    assert result['max_rentals'] == 3
    assert queue.read()['pool_enabled']
    vast.save_gpu_preferences(RentalPreferences(max_rentals=2), session=None)
    assert pool.pool_limit(queue.read()) == 3
    vast.start_queue(vast.StartJobRequest(use_saved_settings=True,
        accept_rental_and_cleanup=True), session=None)
    assert pool.pool_limit(queue.read()) == 2


def test_saved_auto_rent_starts_pool_and_end_disables_it(queue, monkeypatch):
    """One explicit settings save enables future prepared jobs; End turns it off."""
    from api import vast
    monkeypatch.setattr(vast, 'CloudQueue', lambda: queue)
    monkeypatch.setattr(pool, 'services_available', lambda: True)
    queue.update(pool_enabled=False, paused=True)
    saved = vast.save_gpu_preferences(
        RentalPreferences(max_rentals=3, auto_rent=True), session=None)
    state = queue.read()
    assert saved['auto_rent'] is True
    assert state['pool_enabled'] is True
    assert state['paused'] is False
    assert pool.pool_limit(state) == 3
    add_row(queue, 10)
    starts = []
    monkeypatch.setattr(pool, 'select_offer', lambda *args: {'id': 42, 'machine_id': 7})
    monkeypatch.setattr(queue, 'start', lambda *args, **kwargs: starts.append((args, kwargs)) or {'id':'worker'})
    assert pool.pool_step(queue) == {'id':'worker'}
    assert starts[0][1]['pool_authorization_id'] == state['pool_authorization']['id']
    queue.action('end')
    state = queue.read()
    assert state['pool_enabled'] is False
    assert state['pool_authorization'] is None
    assert state['gpu_preferences']['auto_rent'] is False


def test_auto_rent_settings_update_preserves_pause(queue, monkeypatch):
    """Changing pool limits while paused cannot resume paid scheduling."""
    from api import vast
    monkeypatch.setattr(vast, 'CloudQueue', lambda: queue)
    monkeypatch.setattr(pool, 'services_available', lambda: True)
    queue.update(gpu_preferences=RentalPreferences(max_rentals=3, auto_rent=True).model_dump(),
                 pool_enabled=True, paused=True)
    vast.patch_gpu_preferences(
        RentalPreferences(max_rentals=2, auto_rent=True), session=None)
    state = queue.read()
    assert state['paused'] is True
    assert pool.pool_limit(state) == 2


def test_disabling_auto_rent_keeps_existing_rental_but_stops_dispatch(queue, monkeypatch):
    """Turning automation off prevents replacements without killing paid work."""
    from api import vast
    monkeypatch.setattr(vast, 'CloudQueue', lambda: queue)
    worker = add_row(queue, 1, worker=True)
    queue.update(gpu_preferences=RentalPreferences(max_rentals=3, auto_rent=True).model_dump(),
                 pool_enabled=True, paused=False)
    saved = vast.patch_gpu_preferences(RentalPreferences(auto_rent=False), session=None)
    assert saved['auto_rent'] is False
    assert queue.read()['paused'] is True
    assert queue.read()['pool_enabled'] is False
    assert queue.store.read(worker, 'control.json')['stop'] is False


def test_stale_authorization_cannot_create_rental(queue):
    """A previous scheduler request cannot reuse superseded pool consent."""
    add_row(queue, 10)
    with pytest.raises(VastError, match='authorization changed'):
        queue.start({'id':1}, 1, 1, 300, pool_authorization_id='b'*32)
    assert not queue.workers()


def test_fresh_offer_selection_respects_exclusions(queue, monkeypatch):
    """The pool never relaxes hardware requirements or reuses an allocated offer."""
    add_row(queue, 10)
    worker = add_row(queue, 1, worker=True)
    queue.store.update(worker, offer_id=1)
    queue.set_host_block(2, True)
    settings = RentalPreferences(max_rentals=3).model_dump()
    base = dict(gpu_name='RTX 3090', num_gpus=1, cuda_max_good=13, price_hour_usd=.1,
                vram_gb=24, ram_gb=32, cpu_cores=8, tflops=20, disk_gb=40, verified=True)
    offers = [dict(base, id=i, machine_id=i) for i in range(1, 5)]
    offers[2]['cpu_cores'] = 1
    monkeypatch.setattr(pool, 'VastCredentialStore', lambda *args: SimpleNamespace(secrets=lambda: {'api_key':'fake'}))
    monkeypatch.setattr(pool, 'VastClient', lambda *args: object())
    monkeypatch.setattr(pool, 'search_host_offers', lambda *args, **kwargs: offers)
    assert pool.select_offer(queue, settings)['id'] == 4
    queue.set_host_block(4, True)
    with pytest.raises(VastError, match='No available GPU'):
        pool.select_offer(queue, settings)


@pytest.mark.parametrize('operation', ['deadline', 'reserve', 'budget'])
def test_controls_target_selected_lease_not_last_worker(queue, monkeypatch, operation):
    """Editing a job rental never adjusts another concurrently rented GPU."""
    from vast_deadline import request_deadline, request_transfer_reserve, request_budget
    monkeypatch.setattr('vast_deadline.time.time', lambda: 1000)
    selected = add_row(queue, 1, worker=True)
    other = add_row(queue, 2, worker=True)
    queue.update(worker_id=other)
    queue.store.update(selected, rental_state='active', deadline_protocol=2, deadline=8000)
    write_json(queue.store.directory(selected) / 'intent.json', dict(
        id=selected, accepted_at=500, deadline=8000, budget_usd=2, bundle_bytes=1000,
        job_count=1, transfer_reserve_usd=.5,
        offer=dict(download_gb_usd=0, upload_gb_usd=0, price_hour_usd=.1)))
    if operation == 'deadline':
        request_deadline(queue, selected, 8000, 30)
    elif operation == 'reserve':
        request_transfer_reserve(queue, selected, .5, .6)
    else:
        request_budget(queue, selected, 2, 3)
    assert queue.store.read(selected)['deadline_request']['job_id'] == selected
    assert not queue.store.read(other).get('deadline_request')


def test_supervisor_start_failure_restores_previous_pool_state(queue, monkeypatch):
    """A failed settings change cannot replace the previous pool authorization."""
    monkeypatch.setattr(pool, 'services_available', lambda: True)
    def fail(*args):
        """Simulate systemd refusal without starting a process."""
        raise VastError('GPU pool supervisor could not be started', 503)
    monkeypatch.setattr(pool, 'launch_pool', fail)
    previous = queue.read()['pool_authorization']
    with pytest.raises(VastError):
        pool.authorize_pool(queue, RentalPreferences(max_rentals=3).model_dump())
    assert queue.read()['paused'] is False
    assert queue.read()['pool_enabled'] is True
    assert queue.read()['pool_authorization'] == previous


@pytest.mark.parametrize('already_active', [True, False])
def test_supervisor_launch_is_idempotent_and_independent(tmp_path, monkeypatch, already_active):
    """One user service owns scheduling independently of the API process."""
    queue = CloudQueue(JobStore(tmp_path / 'vast'))
    queue.update(pool_error='stale supervisor failure')
    monkeypatch.setattr(pool, 'PROJECT', tmp_path)
    calls = []
    def run(argv, **kwargs):
        """Record service-manager calls without executing them."""
        calls.append(argv)
        return SimpleNamespace(returncode=0 if already_active or argv[0] == 'systemd-run' else 3)
    monkeypatch.setattr(pool.subprocess, 'run', run)
    pool.launch_pool(queue)
    assert queue.read()['pool_error'] is None
    assert len(calls) == (1 if already_active else 2)
    if not already_active:
        assert '--unit' in calls[-1] and 'pbgui-vast-pool' in calls[-1]
        assert '--property=Restart=on-failure' in calls[-1]
        assert calls[-1][-1] == str(tmp_path / 'vast_pool.py')
