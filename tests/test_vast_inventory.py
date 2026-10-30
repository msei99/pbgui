"""Offline corrupt-record isolation, atomic publication and safe decision contracts."""

import json
import errno
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import vast_jobs
from vast_jobs import IMAGE, REVISION, IncompleteJobStore, JobStore, write_json
from vast_integrity_fixtures import freeze_test_proof
from vast_direction_fixtures import direction_config
from vast_provider import VastError
from vast_queue import CloudQueue, worker_step


@pytest.fixture
def inventory(tmp_path):
    """Create only temporary local workers and jobs, with no provider access."""
    store = JobStore(tmp_path / 'vast')
    worker, healthy, bad = 'a' * 32, 'b' * 32, 'c' * 32
    store.publish({'id': worker, 'kind': 'worker', 'status': 'running', 'rental_state': 'active',
                   'created_at': 1, 'deadline': 2000, 'workers': 4, 'idle_seconds': -1},
                  intent={'id': worker, 'image': IMAGE, 'pb8_revision': REVISION, 'offer': {}})
    store.publish({'id': healthy, 'status': 'ready', 'rental_state': 'none', 'created_at': 2})
    directory = store.root / 'jobs' / bad
    directory.mkdir()
    queue = CloudQueue(store)
    queue.update(worker_id=worker)
    return queue, worker, healthy, bad


@pytest.mark.parametrize('payload', [None, b'', b'{', b'[]', b'{}',
    b'{"id":123,"status":"ready"}',
    b'{"id":"cccccccccccccccccccccccccccccccc","status":4}',
    b'{"id":"cccccccccccccccccccccccccccccccc","status":"ready","created_at":null}',
    b'{"id":"cccccccccccccccccccccccccccccccc","status":"ready","created_at":NaN}',
    b'{"id":"dddddddddddddddddddddddddddddddd","status":"ready"}'])
def test_explicit_partial_scan_and_strict_decisions_preserve_corruption(inventory, payload):
    """Only explicit partial reads succeed; existing corrupt bytes and mtime survive."""
    queue, worker, healthy, bad = inventory
    path = queue.store.root / 'jobs' / bad / 'state.json'
    if payload is not None:
        path.write_bytes(payload)
        before = path.stat().st_mtime_ns
    rows, errors = queue.store.list_with_errors()
    assert [row['id'] for row in rows] == [healthy, worker]
    assert errors[0]['id'] == bad
    assert set(errors[0]) == {'id', 'category'}
    for call in (queue.store.list, queue.workers, queue.waiting):
        with pytest.raises(IncompleteJobStore) as caught:
            call()
        assert caught.value.status == 409
        assert caught.value.unreadable == errors
    if payload is not None:
        assert path.read_bytes() == payload
        assert path.stat().st_mtime_ns == before


def test_all_bad_records_and_repair(inventory):
    """No partial result pretends to be a complete empty inventory."""
    queue, worker, healthy, bad = inventory
    for identifier in (worker, healthy):
        (queue.store.directory(identifier) / 'state.json').write_bytes(b'{')
    rows, errors = queue.store.list_with_errors()
    assert rows == [] and len(errors) == 3
    with pytest.raises(IncompleteJobStore):
        queue.store.list()
    for identifier in (worker, healthy, bad):
        write_json(queue.store.directory(identifier) / 'state.json', {'id': identifier, 'status': 'failed'})
    assert len(queue.store.list()) == 3  # Legacy records need no created_at.


def test_global_scan_error_is_not_an_empty_inventory(inventory, monkeypatch):
    """Permission failures for the jobs namespace remain global errors."""
    queue, *_ = inventory
    original = Path.iterdir

    def denied(path):
        """Deny only this temporary jobs namespace."""
        if path == queue.store.root / 'jobs':
            raise PermissionError('synthetic')
        return original(path)

    monkeypatch.setattr(Path, 'iterdir', denied)
    with pytest.raises(VastError) as caught:
        queue.store.list_with_errors()
    assert caught.value.status == 500


@pytest.mark.parametrize('record', ['state.json', 'control.json', 'intent.json'])
def test_publication_is_invisible_until_complete(tmp_path, monkeypatch, record):
    """Readers and deletion recovery ignore an active staging directory."""
    store = JobStore(tmp_path / 'vast')
    original = vast_jobs.write_json
    seen = []

    def inspecting_write(path, value):
        """Inspect each intermediate state without publishing incomplete rows."""
        assert store.list_with_errors() == ([], [])
        assert store.list() == []
        CloudQueue(store).recover_staged_deletions()
        assert path.parent.is_dir()
        seen.append(path.name)
        original(path, value)

    monkeypatch.setattr(vast_jobs, 'write_json', inspecting_write)
    store.publish({'id': 'a' * 32, 'status': 'ready'}, intent={'id': 'a' * 32})
    assert record in seen
    assert store.read('a' * 32, record)


@pytest.mark.parametrize('failed_file', ['state.json', 'control.json', 'intent.json'])
def test_interrupted_publication_removes_only_own_staging(tmp_path, monkeypatch, failed_file):
    """A failed initial write never leaves a visible partial job."""
    store = JobStore(tmp_path / 'vast')
    original = vast_jobs.write_json

    def fail(path, value):
        """Inject one isolated disk-write failure."""
        if path.name == failed_file:
            raise OSError('synthetic write failure')
        original(path, value)

    monkeypatch.setattr(vast_jobs, 'write_json', fail)
    with pytest.raises(OSError):
        store.publish({'id': 'a' * 32, 'status': 'ready'}, intent={})
    assert store.list_with_errors() == ([], [])
    assert list((store.root / 'jobs').iterdir()) == []


def test_publication_never_replaces_even_empty_target(tmp_path):
    """RENAME_NOREPLACE protects an existing empty final directory."""
    store = JobStore(tmp_path / 'vast')
    target = store.root / 'jobs' / ('a' * 32)
    target.mkdir(parents=True)
    before = target.stat().st_ino
    with pytest.raises(FileExistsError):
        store.publish({'id': 'a' * 32, 'status': 'ready'})
    assert target.stat().st_ino == before
    assert list(target.iterdir()) == []
    assert list(target.parent.iterdir()) == [target]


@pytest.mark.parametrize('support', ['missing_symbol', errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP])
@pytest.mark.parametrize('existing', [False, True])
def test_publication_fallback_preserves_no_replace(tmp_path, monkeypatch, support, existing):
    """Unsupported no-replace calls fall back under lock without clobbering targets."""
    store = JobStore(tmp_path / 'vast')
    identifier = 'a' * 32
    target = store.root / 'jobs' / identifier
    if existing:
        target.mkdir(parents=True)
        before = target.stat().st_ino

    class UnsupportedRename:
        """Model a libc entry point whose filesystem cannot accept the flag."""

        def __call__(self, *args):
            """Return the normal ctypes failure representation."""
            vast_jobs.ctypes.set_errno(support)
            return -1

    libc = SimpleNamespace() if support == 'missing_symbol' else SimpleNamespace(renameat2=UnsupportedRename())
    monkeypatch.setattr(vast_jobs.ctypes, 'CDLL', lambda *args, **kwargs: libc)
    if existing:
        with pytest.raises(FileExistsError):
            store.publish({'id': identifier, 'status': 'ready'})
        assert target.stat().st_ino == before
        assert list(target.iterdir()) == []
        assert list(target.parent.iterdir()) == [target]
    else:
        store.publish({'id': identifier, 'status': 'ready'})
        assert store.read(identifier)['status'] == 'ready'
        assert store.read(identifier, 'control.json') == {'stop': False, 'cleanup': False}
        assert list(target.parent.iterdir()) == [target]


@pytest.mark.parametrize('failure', [errno.EACCES, errno.EIO, errno.EEXIST])
def test_publication_does_not_fallback_for_real_rename_errors(tmp_path, monkeypatch, failure):
    """Permission, I/O and collision failures never trigger a second rename."""
    store = JobStore(tmp_path / 'vast')

    class FailingRename:
        """Expose an actual no-replace failure."""

        def __call__(self, *args):
            """Retain the underlying errno for the publishing caller."""
            vast_jobs.ctypes.set_errno(failure)
            return -1

    def unexpected_rename(*args):
        """A retry could conceal a real failure or replace an existing target."""
        pytest.fail('unexpected fallback for a supported rename error')

    monkeypatch.setattr(vast_jobs.ctypes, 'CDLL', lambda *args, **kwargs: SimpleNamespace(renameat2=FailingRename()))
    monkeypatch.setattr(vast_jobs.os, 'rename', unexpected_rename)
    with pytest.raises(OSError) as caught:
        store.publish({'id': 'a' * 32, 'status': 'ready'})
    assert caught.value.errno == failure
    assert list((store.root / 'jobs').iterdir()) == []


@pytest.mark.parametrize('writer', ['preparation', 'worker'])
def test_parallel_reader_sees_no_unpublished_job(tmp_path, monkeypatch, writer):
    """A second thread scans safely while either initial writer is paused."""
    store = JobStore(tmp_path / 'vast')
    queue = CloudQueue(store)
    entered, release = Event(), Event()
    original = vast_jobs.write_json

    def paused_write(path, value):
        """Pause after state exists but before control is written in staging."""
        if path.name == 'control.json':
            entered.set()
            assert release.wait(10)
        original(path, value)

    monkeypatch.setattr(vast_jobs, 'write_json', paused_write)
    monkeypatch.setattr(store, 'start', lambda identifier, *args: store.read(identifier))
    action = (lambda: store.create_preparation('synthetic', 512, 4, False)) if writer == 'preparation' else (
        lambda: queue.start({'id': 42, 'machine_id': 7}, 1, 1, 300, manual=True))
    with ThreadPoolExecutor(max_workers=1) as executor:
        pending = executor.submit(action)
        try:
            assert entered.wait(10)
            assert store.list_with_errors() == ([], [])
            assert store.list() == [] and queue.workers() == [] and queue.waiting() == []
            queue.recover_staged_deletions()
            assert list((store.root / 'jobs').iterdir())
        finally:
            release.set()
        result = pending.result(timeout=10)
    assert store.read(result['id'], 'control.json') == {'stop': False, 'cleanup': False}
    assert len(store.list()) == 1


@pytest.mark.parametrize('link_kind', ['namespace', 'directory', 'state'])
def test_scan_preserves_symlink_boundaries(tmp_path, link_kind):
    """No partial scan reads through a redirected namespace or job record."""
    store = JobStore(tmp_path / 'vast')
    outside = tmp_path / 'outside'
    outside.mkdir()
    identifier = 'a' * 32
    write_json(outside / 'state.json', {'id': identifier, 'status': 'ready'})
    if link_kind == 'namespace':
        store.root.mkdir()
        (store.root / 'jobs').symlink_to(outside, target_is_directory=True)
        with pytest.raises(VastError):
            store.list_with_errors()
    else:
        jobs = store.root / 'jobs'
        jobs.mkdir(parents=True)
        if link_kind == 'directory':
            (jobs / identifier).symlink_to(outside, target_is_directory=True)
        else:
            (jobs / identifier).mkdir()
            (jobs / identifier / 'state.json').symlink_to(outside / 'state.json')
        rows, errors = store.list_with_errors()
        assert rows == [] and errors == [{'id': identifier, 'category': 'record_unreadable'}]
    assert json.loads((outside / 'state.json').read_text()) == {'id': identifier, 'status': 'ready'}


@pytest.mark.parametrize('boundary', ['stop', 'cleanup', 'deadline'])
def test_healthy_worker_cleanup_survives_foreign_bad_record(inventory, boundary):
    """Existing authorized cleanup is reached without claiming any waiting job."""
    queue, worker, healthy, _ = inventory
    if boundary != 'deadline':
        queue.store.control(worker, boundary)
    assert worker_step(queue, worker, now=1760 if boundary == 'deadline' else 1100) is None
    assert queue.store.read(worker, 'control.json')['cleanup'] is True
    assert queue.store.read(healthy)['status'] == 'ready'
    assert not queue.store.read(worker).get('active_job')


def test_incomplete_claim_scan_never_assigns_a_job(inventory):
    """Absence of a readable claim cannot establish free capacity."""
    queue, worker, healthy, _ = inventory
    with pytest.raises(IncompleteJobStore):
        worker_step(queue, worker, now=1100)
    assert not queue.store.read(worker).get('active_job')
    assert not queue.store.read(healthy).get('lease_id')


@pytest.mark.parametrize('boundary', ['running', 'stop', 'deadline'])
@pytest.mark.parametrize('foreign_corruption', [False, True])
def test_existing_running_claim_is_recovered_before_cleanup(inventory, boundary, foreign_corruption):
    """A lost worker pointer cannot bypass collection of an already bound job."""
    from vast_job_runner import guard_step
    queue, worker, healthy, bad = inventory
    if not foreign_corruption:
        queue.store.directory(bad).rmdir()
    queue.store.update(healthy, status='running', lease_id=worker, uploaded=True, final_collected=False)
    intent = {'id': worker, 'label': 'pbgui-vast-' + worker, 'deadline': 2000}
    now = 1760 if boundary == 'deadline' else 1100
    if boundary == 'stop':
        queue.store.control(worker, 'stop')
    waiting = 'd' * 32
    queue.store.publish({'id': waiting, 'status': 'ready', 'rental_state': 'none'})
    before_job = queue.store.read(healthy)

    class Provider:
        """A purely local provider double records any premature rental deletion."""

        def __init__(self):
            """Retain fake calls without credentials or remote access."""
            self.calls = []

        def instances(self, **kwargs):
            """Only this already authorized instance exists in the fake account."""
            return [{'id': 123, 'label': intent['label'], 'actual_status': 'running'}]

        def request(self, method, path, *args):
            """Record mutations without changing any real instance."""
            self.calls.append((method, path))
            return {}

    provider = Provider()
    assert worker_step(queue, worker, now=now) == healthy
    assert queue.store.read(worker)['active_job'] == healthy
    assert queue.store.read(healthy) == before_job
    assert queue.store.read(worker, 'control.json')['cleanup'] is False
    assert not queue.store.read(waiting).get('lease_id')
    assert not guard_step(queue.store, worker, provider, intent, '', now=now)
    assert provider.calls == []

    # Once results are secured, the existing stop/deadline can end the rental.
    queue.store.update(healthy, status='completed', final_collected=True)
    if boundary != 'running':
        assert worker_step(queue, worker, now=now + 1) is None
        assert queue.store.read(worker, 'control.json')['cleanup'] is True


def test_true_deadline_still_deletes_with_recovered_uncollected_job(inventory):
    """Result collection never extends the authorized rental beyond its real deadline."""
    from vast_job_runner import guard_step
    queue, worker, healthy, _ = inventory
    queue.store.update(healthy, status='running', lease_id=worker, uploaded=True, final_collected=False)
    intent = {'id': worker, 'label': 'pbgui-vast-' + worker, 'deadline': 2000}
    assert worker_step(queue, worker, now=1760) == healthy
    calls = []
    provider = SimpleNamespace(
        instances=lambda **kwargs: [{'id': 123, 'label': intent['label']}],
        request=lambda method, path: calls.append((method, path)))
    assert not guard_step(queue.store, worker, provider, intent, '', now=2000)
    assert calls == [('DELETE', '/instances/123/')]


def test_multiple_existing_claims_never_authorize_recovery_or_early_cleanup(inventory):
    """Two known unfinished associations remain an ownership conflict even in partial scans."""
    queue, worker, healthy, _ = inventory
    queue.store.update(healthy, status='running', lease_id=worker)
    queue.store.publish({'id': 'd' * 32, 'status': 'collecting', 'lease_id': worker})
    with pytest.raises(VastError, match='Multiple unfinished jobs'):
        worker_step(queue, worker, now=1760)
    assert not queue.store.read(worker).get('active_job')
    assert queue.store.read(worker, 'control.json')['cleanup'] is False


@pytest.mark.parametrize('operation', ['purge', 'deleted', 'orphaned'])
def test_incomplete_inventory_never_moves_or_deletes_history(inventory, operation):
    """Reference-based deletions require a complete view of all records."""
    queue, worker, healthy, _ = inventory
    queue.store.update(worker, rental_state='deletion_verified')
    before = sorted(path.name for path in (queue.root / 'jobs').iterdir())
    action = {'purge': lambda: queue.purge_job_histories([healthy]),
              'deleted': queue.recover_deleted_job_histories,
              'orphaned': queue.recover_orphaned_workers}[operation]
    with pytest.raises(IncompleteJobStore):
        action()
    assert sorted(path.name for path in (queue.root / 'jobs').iterdir()) == before


def test_verified_worker_completes_healthy_jobs_without_deleting_history(inventory):
    """Guard finalization finishes known jobs while retaining unresolved evidence."""
    from vast_job_runner import finalize_verified_rental
    queue, worker, healthy, _ = inventory
    queue.store.update(worker, rental_state='deletion_verified')
    queue.store.update(healthy, status='running', lease_id=worker)
    finalize_verified_rental(queue.store, worker, now=2000)
    assert queue.store.read(healthy)['status'] == 'failed'
    assert queue.store.read(worker)['status'] == 'completed'
    assert queue.store.read(worker)['inventory_error']
    with pytest.raises(IncompleteJobStore):
        queue.recover_orphaned_workers()


def test_pool_and_gpu_preview_refuse_partial_capacity(inventory):
    """Indirect strict consumers must not interpret missing jobs as free slots."""
    from vast_pool import rental_needed
    from vast_gpu_recommendation import queued_gpu_previews
    queue, *_ = inventory
    queue.update(pool_enabled=True, paused=False, gpu_preferences={'auto_rent': True},
                 pool_authorization={'id': 'fake', 'settings': {'auto_rent': True}})
    with pytest.raises(IncompleteJobStore):
        rental_needed(queue, 'fake')
    with pytest.raises(IncompleteJobStore):
        queued_gpu_previews(queue, {})


def test_store_and_queue_start_never_rent_from_partial_inventory(inventory, monkeypatch):
    """Both rental boundaries reject an incomplete inventory before provider calls."""
    queue, _, healthy, _ = inventory
    monkeypatch.setattr(vast_jobs, 'services_available', lambda: True)
    queue.store.update(healthy, workers=4)
    write_json(queue.store.directory(healthy) / 'input/optimize.json', direction_config())
    freeze_test_proof(queue.store, healthy)
    queue.update(worker_id=None)
    offer = {'id': 42, 'machine_id': 1, 'price_hour_usd': .1,
             'cpu_cores': 4, 'cuda_max_good': 13, 'disk_gb': 40}
    with pytest.raises(IncompleteJobStore):
        queue.store.start(healthy, offer, 1, 1)
    with pytest.raises(IncompleteJobStore):
        queue.start(offer, 1, 1, 300, manual=True)


def test_restart_activity_remains_unknown(inventory, monkeypatch):
    """The existing restart error path never presents partial inventory as idle."""
    import PBApiServer
    queue, *_ = inventory
    monkeypatch.setattr('vast_queue.CloudQueue', lambda: queue)
    assert PBApiServer._vast_restart_activity() == {
        'state': 'unknown', 'active_rentals': None, 'queued_jobs': None}


def test_loop_capacity_and_idempotency_require_complete_inventory(inventory, monkeypatch):
    """A corrupt existing operation never permits a new cloud job or free slot."""
    from api import optimize_v8
    from pb8_loop_backend import LoopBackend
    queue, *_ = inventory
    backend = LoopBackend(queue.root.parent, None)
    monkeypatch.setattr('vast_queue.CloudQueue', lambda: queue)
    record = {'settings': {'execution': 'vast', 'vast': {'max_rentals': 2}, 'parallel': 2}}
    with pytest.raises(IncompleteJobStore):
        backend.capacity(record)
    monkeypatch.setattr(backend, 'authorized', lambda *args, **kwargs: record)
    monkeypatch.setattr('pb8_loop_backend.validate_hsl_policy', lambda *args: None)
    monkeypatch.setattr('pb8_loop_backend.validate_change', lambda *args: None)
    monkeypatch.setattr('pb8_loop_backend.fresh_optimizer_config', lambda config: config)
    monkeypatch.setattr(optimize_v8, '_save_config_bundle', lambda *args, **kwargs: {})
    monkeypatch.setattr(vast_jobs, 'JobStore', lambda: queue.store)
    with pytest.raises(IncompleteJobStore):
        backend.prepare(record, {'kind': 'optimizer', 'config': {}, 'overrides': {},
                                 'name': 'synthetic', 'operation': 'existing-operation'})


def test_preparation_publishes_before_recovery_and_snapshot_work(tmp_path, monkeypatch):
    """Preparation remains recoverable after its atomic namespace publication."""
    store = JobStore(tmp_path / 'vast')
    original = vast_jobs._rename_job_directory

    def inspect(source, target):
        """Both initial files exist while readers still see no job."""
        assert store.list() == []
        assert json.loads((source / 'state.json').read_text())['status'] == 'preparing'
        assert json.loads((source / 'control.json').read_text()) == {'stop': False, 'cleanup': False}
        original(source, target)

    monkeypatch.setattr(vast_jobs, '_rename_job_directory', inspect)
    state = store.create_preparation('synthetic', 512, 4, False)
    assert store.directory(state['id']) == store.root / 'jobs' / state['id']
    assert store.recover_interrupted_preparation(state['id'])['status'] == 'preparing'


def test_queue_publication_completes_before_start_failure(tmp_path, monkeypatch):
    """A supervisor-start error leaves complete records and no queue reference."""
    store = JobStore(tmp_path / 'vast')
    queue = CloudQueue(store)
    original = vast_jobs._rename_job_directory

    def inspect(source, target):
        """No reader sees a worker before all initial records have been written."""
        assert queue.workers() == []
        assert store.list_with_errors() == ([], [])
        assert {p.name for p in source.iterdir()} == {'state.json', 'control.json', 'intent.json'}
        queue.recover_staged_deletions()
        assert source.is_dir()
        original(source, target)

    def fail_start(identifier, *args):
        """The service boundary sees a fully published worker, then fails safely."""
        for filename in ('state.json', 'control.json', 'intent.json'):
            assert store.read(identifier, filename)
        raise VastError('Synthetic supervisor failure', 409)

    monkeypatch.setattr(vast_jobs, '_rename_job_directory', inspect)
    monkeypatch.setattr(store, 'start', fail_start)
    with pytest.raises(VastError, match='Synthetic supervisor'):
        queue.start({'id': 42, 'machine_id': 7}, 1, 1, 300, manual=True)
    rows, errors = store.list_with_errors()
    assert len(rows) == 1 and errors == []
    assert not queue.read().get('worker_id')


@pytest.mark.parametrize('bad_reference', ['foreign', 'selected', 'lease'])
def test_jobs_api_projects_healthy_records_and_clears_warning(inventory, monkeypatch, bad_reference):
    """Use an isolated FastAPI router, never the production application lifespan."""
    from api import vast
    queue, worker, healthy, bad = inventory
    if bad_reference == 'selected':
        queue.update(worker_id=bad)
    if bad_reference == 'lease':
        queue.store.update(healthy, lease_id=bad)
    path = queue.store.directory(bad) / 'state.json'
    path.write_bytes(b'{')
    before = path.stat().st_mtime_ns
    monkeypatch.setattr(vast, 'CloudQueue', lambda: queue)
    monkeypatch.setattr(vast, 'CLOUD_LOG_ROOT', queue.root / 'logs')
    monkeypatch.setattr(vast, 'services_available', lambda: False)
    monkeypatch.setattr('vast_calibration.calibration_worker_available', lambda _: False)
    app = FastAPI()
    app.include_router(vast.router, prefix='/api/vast')
    app.dependency_overrides[vast.require_auth] = lambda: SimpleNamespace()
    with TestClient(app) as client:
        response = client.get('/api/vast/jobs')
        assert response.status_code == 200
        data = response.json()
        assert response.headers['cache-control'] == 'no-store'
        assert [row['id'] for row in data['jobs']] == [healthy]
        assert [row['id'] for row in data['workers']] == [worker]
        assert data['unreadable'] == [{'id': bad, 'category': 'record_unreadable'}]
        assert path.read_bytes() == b'{' and path.stat().st_mtime_ns == before
        if bad_reference == 'lease':
            assert data['jobs'][0]['rental'] is None
        write_json(path, {'id': bad, 'kind': 'worker', 'status': 'completed',
                         'rental_state': 'deletion_verified'})
        write_json(path.parent / 'intent.json', {'id': bad, 'offer': {}})
        assert client.get('/api/vast/jobs').json()['unreadable'] == []
