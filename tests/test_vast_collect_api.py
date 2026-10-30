"""Isolated collect-only publication, ownership, cancellation and API regressions."""
import copy
import fcntl
import json
import os
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import msgpack
import psutil
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api import vast
from api.auth import require_auth
from secure_files import ensure_private_directory
from vast_jobs import JobStore, IMAGE, REVISION, write_json
from vast_provider import VastError
import vast_collect
import vast_transfer


@pytest.fixture
def collection(tmp_path, monkeypatch):
    """Own tiny job/result trees and forbid every real provider or SSH action."""
    store = JobStore(tmp_path / 'cloud')
    identifier, lease = 'a' * 32, 'b' * 32
    for key in (identifier, lease):
        directory = ensure_private_directory(store.root / 'jobs' / key)
        write_json(directory / 'state.json', dict(id=key, kind='optimizer', status='running',
            rental_state='active', uploaded=True, instance_id=123, exact_completed=12,
            lease_id=lease if key == identifier else None, active_job=identifier))
        write_json(directory / 'control.json', dict(stop=False, cleanup=False))
        write_json(directory / 'intent.json', dict(id=key, label='pbgui-vast-' + key,
            image=IMAGE, pb8_revision=REVISION, accepted_at=time.time() - 10,
            deadline=time.time() + 1800, budget_usd=1, offer=dict(id=1),
            results_root=str(tmp_path / 'optimize_results'), source_config_sha256='c' * 64))
        for filename in ('ssh-key', 'known_hosts', 'ssh-attached.json'):
            (directory / filename).write_text('isolated fixture')
    write_json(ensure_private_directory(store.directory(identifier) / 'input') / 'manifest.json', {})
    monkeypatch.setattr('vast_jobs.JobStore', lambda: store)
    monkeypatch.setattr(vast, 'JobStore', lambda: store)
    owner = Mock(return_value=dict(id=123, actual_status='running', public_ipaddr='8.8.8.8',
        ports={'22/tcp': [{'HostPort': '2222'}]}))
    monkeypatch.setattr('vast_job_runner.owned_instance', owner)
    monkeypatch.setattr('vast_job_runner.validate_intent', lambda intent, _key: intent)
    monkeypatch.setattr('vast_deadline.effective_intent', lambda _store, _key, intent: intent)
    provider = Mock()
    monkeypatch.setattr('vast_provider.VastClient', provider)
    monkeypatch.setattr('vast_credentials.VastCredentialStore', lambda _root: SimpleNamespace(secrets=lambda: {'api_key': 'test'}))
    monkeypatch.setattr(vast_collect, 'writer_state', lambda _store, state, cache=None: ('compatible', store.read(state.get('lease_id') or state['id'])))
    monkeypatch.setattr(vast_transfer.WorkerConnection, 'bootstrap', Mock(side_effect=AssertionError('No bootstrap')))
    monkeypatch.setattr(vast_transfer.WorkerConnection, 'operation', Mock(side_effect=AssertionError('No Stop')))

    def download(connection, final, timeout):
        """Publish only an immutable Pareto fixture, never final control state."""
        assert final is False and 0 < timeout <= 90
        native = ensure_private_directory(connection.directory / 'partial-results/optimize_results/run/pareto')
        write_json(native / 'candidate.json', {'config': {}})
        store.update(identifier, last_backup_at=time.time())
    monkeypatch.setattr(vast_transfer.WorkerConnection, 'collect', download)
    app = FastAPI()
    app.include_router(vast.router, prefix='/api/vast')
    app.dependency_overrides[require_auth] = lambda: object()
    vast_collect.startup()
    with TestClient(app) as http:
        yield SimpleNamespace(store=store, identifier=identifier, lease=lease, owner=owner,
            provider=provider, http=http, app=app, url='/api/vast/jobs/' + identifier + '/collect')


@pytest.mark.parametrize('shared', [False, True])
def test_collect_is_idempotent_without_stop_or_finalization(collection, shared):
    """Direct and lease jobs publish one result and retain all lifecycle fields."""
    c = collection
    if not shared:
        c.store.update(c.identifier, lease_id=None)
    before = c.store.read(c.identifier)
    first = c.http.post(c.url)
    assert first.status_code == 200, first.text
    second = c.http.post(c.url)
    assert second.status_code == 200, second.text
    after = second.json()
    assert after['result_path'] == first.json()['result_path']
    assert after['result_partial'] is True and after['pareto_count'] == 1
    for key in ('status', 'rental_state', 'lease_id', 'instance_id'):
        assert after[key] == before[key]
    for key in (c.identifier, c.lease):
        assert c.store.read(key, 'control.json') == dict(stop=False, cleanup=False)
    c.owner.assert_called_with(c.provider.return_value, c.store.read(c.lease if shared else c.identifier, 'intent.json'), fresh=True, expected_id=123)


@pytest.mark.parametrize('change', ['collecting', 'stop_rpc_at', 'final_collection_at', 'deleted_at', 'stop', 'cleanup', 'quota', 'identity', 'offline'])
def test_remote_rejects_unsafe_states_and_instance_mismatch(collection, change):
    """A stale or direct POST cannot bypass ownership, phase or control boundaries."""
    c = collection
    if change == 'collecting':
        c.store.update(c.identifier, status='collecting')
    elif change in {'stop_rpc_at', 'final_collection_at', 'deleted_at'}:
        c.store.update(c.identifier, **{change: time.time()})
    elif change in {'stop', 'cleanup'}:
        write_json(c.store.directory(c.lease) / 'control.json', {change: True})
    elif change == 'quota':
        c.store.update(c.identifier, downloaded_bytes=1024**3)
    elif change == 'identity':
        c.owner.return_value['id'] = 124
    else:
        c.owner.return_value['actual_status'] = 'offline'
    response = c.http.post(c.url)
    assert response.status_code == 409, response.text
    assert 'result_path' not in c.store.read(c.identifier)


@pytest.mark.parametrize('status', ['running', 'collecting', 'completed'])
def test_local_final_after_rental_end_without_provider_or_outcome_change(collection, status):
    """Expired/deleted rentals do not block bounded local final publication."""
    c = collection
    source = ensure_private_directory(c.store.directory(c.identifier) / 'final-results/optimize_results/run/pareto')
    write_json(source / 'candidate.json', {'config': {}})
    (source.parent / 'all_results.bin').write_bytes(msgpack.packb({'config': {}}))
    c.store.update(c.identifier, final_collected=True, status=status, rental_state='deletion_verified')
    c.store.update(c.lease, rental_state='deletion_verified')
    result = c.http.post(c.url)
    assert result.status_code == 200, result.text
    assert result.json()['status'] == status and result.json()['result_partial'] is False
    c.provider.assert_not_called()
    c.owner.assert_not_called()


def test_legacy_writer_is_rejected_before_provider_access(collection, monkeypatch):
    """An old live run cannot bypass missing transaction support."""
    c = collection
    monkeypatch.setattr(vast_collect, 'writer_state', lambda _store, state, cache=None: ('legacy', c.store.read(c.lease)))
    assert c.http.post(c.url).status_code == 409
    c.provider.assert_not_called()


def test_collect_auth_and_invalid_identifier(collection):
    """Keep authentication and namespace validation at the HTTP boundary."""
    c = collection
    assert c.http.post('/api/vast/jobs/unknown/collect').status_code == 422
    c.app.dependency_overrides.clear()
    assert c.http.post(c.url).status_code == 401


def test_manual_stop_during_download_does_not_fail_job(collection, monkeypatch):
    """Control changes cancel only this request and preserve execution status."""
    from vast_snapshot import check_work
    c = collection
    def download(*_args, **_kwargs):
        """Inject a lease Stop after admission, then exercise the real check."""
        write_json(c.store.directory(c.lease) / 'control.json', {'stop': True})
        check_work()
    monkeypatch.setattr(vast_transfer.WorkerConnection, 'collect', download)
    assert c.http.post(c.url).status_code == 409
    assert c.store.read(c.identifier)['status'] == 'running'
    assert not vast_collect._ACTIVE


def test_empty_partial_preserves_published_results(collection, monkeypatch):
    """An empty new front never erases the last browsable partial result."""
    c = collection
    assert c.http.post(c.url).status_code == 200
    result = c.store.read(c.identifier)['result_path']
    previous = {p.name: p.read_bytes() for p in (Path(result) / 'pareto').glob('*')}
    monkeypatch.setattr(vast_transfer.WorkerConnection, 'collect', lambda *_args, **_kwargs: None)
    (c.store.directory(c.identifier) / 'partial-results/optimize_results/run/pareto/candidate.json').unlink()
    assert c.http.post(c.url).status_code == 409
    assert previous == {p.name: p.read_bytes() for p in (Path(result) / 'pareto').glob('*')}


def test_partial_preparation_failure_retains_entire_publication(collection, monkeypatch):
    """Cancellation before commit cannot replace only a prefix of the Pareto front."""
    c = collection
    assert c.http.post(c.url).status_code == 200
    target = Path(c.store.read(c.identifier)['result_path'])
    before = {str(p.relative_to(target)): p.read_bytes() for p in target.rglob('*') if p.is_file()}
    native = c.store.directory(c.identifier) / 'partial-results/optimize_results/run/pareto'
    write_json(native / 'candidate.json', {'config': {'changed': True}})
    write_json(native / 'new.json', {'config': {}})
    calls = 0
    def cancel():
        """Fail after staging has started, using the cooperative importer boundary."""
        nonlocal calls
        calls += 1
        if calls == 15:
            raise VastError('isolated cancellation', 409)
    with pytest.raises(VastError):
        vast_transfer.import_results(c.store, c.identifier, partial=True, check=cancel)
    assert before == {str(p.relative_to(target)): p.read_bytes() for p in target.rglob('*') if p.is_file()}


def test_final_empty_pareto_replaces_previous_front(collection):
    """Only partial empty fronts are rejected; native final evidence is authoritative."""
    c = collection
    assert c.http.post(c.url).status_code == 200
    source = ensure_private_directory(c.store.directory(c.identifier) / 'final-results/optimize_results/run/pareto')
    (source.parent / 'all_results.bin').write_bytes(msgpack.packb({'config': {}}) * 12)
    c.store.update(c.identifier, final_collected=True)
    response = c.http.post(c.url)
    assert response.status_code == 200, response.text
    assert response.json()['pareto_count'] == 0
    assert not list((Path(response.json()['result_path']) / 'pareto').glob('*.json'))


def test_final_import_deadline_preserves_publication(collection, monkeypatch):
    """A large binary expiring before commit leaves publication and metadata intact."""
    c = collection
    assert c.http.post(c.url).status_code == 200
    source = ensure_private_directory(c.store.directory(c.identifier) / 'final-results/optimize_results/run/pareto')
    (source.parent / 'all_results.bin').write_bytes(msgpack.packb({'config': {}}) * 12)
    write_json(source / 'final.json', {})
    c.store.update(c.identifier, final_collected=True, status='collecting')
    before = c.store.read(c.identifier)
    target = Path(before['result_path'])
    files = {str(p.relative_to(target)): p.read_bytes() for p in target.rglob('*') if p.is_file()}
    original = vast_transfer.msgpack if hasattr(vast_transfer, 'msgpack') else None
    clock = [time.monotonic()]
    actual = msgpack.Unpacker
    def unpack(*args, **kwargs):
        """Advance the synthetic work clock while reading real native records."""
        unpacker = actual(*args, **kwargs)
        class Slow:
            """Retain unpacker's byte accounting and expire after one record."""
            def __iter__(self):
                for item in unpacker:
                    clock[0] += 91
                    yield item
            def tell(self):
                """Return native stream consumption."""
                return unpacker.tell()
        return Slow()
    monkeypatch.setattr(msgpack, 'Unpacker', unpack)
    monkeypatch.setattr(vast_collect.time, 'monotonic', lambda: clock[0])
    response = c.http.post(c.url)
    assert response.status_code == 409, response.text
    assert c.store.read(c.identifier) == before
    assert files == {str(p.relative_to(target)): p.read_bytes() for p in target.rglob('*') if p.is_file()}


def test_shutdown_cancels_and_joins_manual_request(collection, monkeypatch):
    """API shutdown waits for request ownership to leave, without stopping a job."""
    import asyncio
    from concurrent.futures import ThreadPoolExecutor
    from vast_snapshot import check_work
    c = collection
    entered = threading.Event()
    def download(*_args, **_kwargs):
        """Remain in a cooperative transport until shutdown cancels this owner."""
        entered.set()
        while True:
            check_work()
            time.sleep(.01)
    monkeypatch.setattr(vast_transfer.WorkerConnection, 'collect', download)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(vast_collect.collect_results, c.store, c.identifier)
        assert entered.wait(2)
        asyncio.run(vast_collect.shutdown())
        with pytest.raises(VastError, match='shutdown'):
            future.result(timeout=2)
    assert not vast_collect._ACTIVE
    assert c.store.read(c.identifier)['status'] == 'running'
    vast_collect.startup()


@pytest.mark.parametrize('interruption', ['deadline', 'shutdown'])
def test_local_publication_state_wait_preserves_results(collection, monkeypatch, interruption):
    """State contention aborts before local file publication, including after rental end."""
    import asyncio
    from concurrent.futures import ThreadPoolExecutor
    from contextlib import contextmanager
    from file_lock import advisory_file_lock
    c = collection
    assert c.http.post(c.url).status_code == 200
    source = ensure_private_directory(c.store.directory(c.identifier) / 'final-results/optimize_results/run/pareto')
    (source.parent / 'all_results.bin').write_bytes(msgpack.packb({'config': {}}) * 12)
    write_json(source / 'new-final.json', {'config': {}})
    c.store.update(c.identifier, final_collected=True, status='collecting')
    before = c.store.read(c.identifier)
    target = Path(before['result_path'])
    files = {str(p.relative_to(target)): p.read_bytes() for p in target.rglob('*') if p.is_file()}
    c.provider.reset_mock()
    held, release, attempted, expired = (threading.Event() for _ in range(4))
    def holder():
        """Own only the temporary job's state lock while the request waits."""
        with advisory_file_lock(c.store.directory(c.identifier) / '.state-lock'):
            held.set()
            release.wait(5)
    original_lock, original_boundary = vast_collect.bounded_lock, vast_collect.work_boundary
    @contextmanager
    def tracked_lock(path, **kwargs):
        """Observe the specific State acquisition before import, not Snapshot admission."""
        if Path(path).name == '.state-lock':
            attempted.set()
        with original_lock(path, **kwargs):
            yield
    @contextmanager
    def short_boundary(check):
        """Accelerate only this request's work clock without changing global clocks."""
        def remaining():
            """Retain the real eligibility checks and inject deterministic budget expiry."""
            if expired.is_set():
                raise VastError('Pareto collection timed out', 409)
            return check()
        with original_boundary(remaining):
            yield
    monkeypatch.setattr(vast_collect, 'bounded_lock', tracked_lock)
    monkeypatch.setattr(vast_collect, 'work_boundary', short_boundary)
    thread = threading.Thread(target=holder)
    thread.start()
    try:
        assert held.wait(2)
        with ThreadPoolExecutor(max_workers=2) as pool:
            future = pool.submit(vast_collect.collect_results, c.store, c.identifier)
            try:
                assert attempted.wait(2)
                if interruption == 'deadline':
                    expired.set()
                    shutdown = None
                else:
                    shutdown = pool.submit(asyncio.run, vast_collect.shutdown())
                with pytest.raises(VastError, match='timed out|shutdown') as error:
                    future.result(timeout=2)
                assert error.value.status == 409
                if shutdown is not None:
                    shutdown.result(timeout=2)
                assert thread.is_alive() and not release.is_set()
                assert c.store.read(c.identifier) == before
                assert files == {str(p.relative_to(target)): p.read_bytes() for p in target.rglob('*') if p.is_file()}
                assert not vast_collect._ACTIVE
                c.provider.assert_not_called()
            finally:
                release.set()
    finally:
        release.set()
        thread.join(3)
        vast_collect.startup()


def test_cancellation_after_file_commit_finishes_matching_metadata(collection, monkeypatch):
    """Once import commits, reentrant State persistence has no new cancellable lock wait."""
    c = collection
    assert c.http.post(c.url).status_code == 200
    source = ensure_private_directory(c.store.directory(c.identifier) / 'final-results/optimize_results/run/pareto')
    (source.parent / 'all_results.bin').write_bytes(msgpack.packb({'config': {}}) * 12)
    write_json(source / 'final.json', {'config': {}})
    c.store.update(c.identifier, final_collected=True, status='collecting')
    original = vast_transfer.import_results
    def publish_then_cancel(*args, **kwargs):
        """Set cancellation exactly after the real file commit, before caller metadata."""
        result = original(*args, **kwargs)
        assert list(vast_collect._ACTIVE.values())
        for cancel, _done in vast_collect._ACTIVE.values():
            cancel.set()
        return result
    monkeypatch.setattr(vast_transfer, 'import_results', publish_then_cancel)
    result = c.http.post(c.url)
    assert result.status_code == 200, result.text
    state = result.json()
    provenance = json.loads((Path(state['result_path']) / '.pbgui_vast.json').read_text())
    assert state['status'] == 'collecting' and state['result_partial'] is False
    assert provenance['partial'] is False and provenance['evaluations'] == state['evaluations'] == 12
    assert state['pareto_count'] == 1 and not vast_collect._ACTIVE


@pytest.mark.parametrize('shared', [False, True])
@pytest.mark.parametrize('capability,identity,expected', [(1, True, 'compatible'), (0, True, 'legacy'), (1, False, 'unknown')])
def test_actual_writer_identity_and_capability(collection, monkeypatch, shared, capability, identity, expected):
    """Read the actual role lock and require both process identity and transaction support."""
    c = collection
    if not shared:
        c.store.update(c.identifier, lease_id=None)
    key = c.lease if shared else c.identifier
    c.store.update(key, supervisor_run={'pid': os.getpid(), 'created_at': 123,
                                      'snapshot_transaction': capability})
    script = str(Path(vast_collect.__file__).with_name('vast_job_runner.py'))
    monkeypatch.setattr(psutil, 'Process', lambda _pid: SimpleNamespace(
        create_time=lambda: 123 if identity else 124,
        cmdline=lambda: ['python', script, 'run', key]))
    # Load the original implementation even though the general fixture stubs it.
    import importlib.util
    spec = importlib.util.spec_from_file_location('isolated_writer', vast_collect.__file__)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    with (c.store.directory(key) / 'run.lock').open('a') as role:
        fcntl.flock(role, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert module.writer_state(c.store, c.store.read(c.identifier))[0] == expected
        if shared:
            c.store.update(key, active_job='c' * 32)
            assert module.writer_state(c.store, c.store.read(c.identifier))[0] == 'inactive'


def test_purge_cannot_mark_or_delete_history_while_snapshot_owned(collection, monkeypatch):
    """Deletion takes every lock before writing tombstones or removing history."""
    from file_lock import advisory_file_lock
    from vast_queue import CloudQueue
    c = collection
    c.store.update(c.identifier, status='completed', final_collected=True)
    c.store.update(c.lease, kind='worker', active_job=None, rental_state='deletion_verified')
    deleted = Mock()
    monkeypatch.setattr('vast_performance.PerformanceHistory.delete_runs', deleted)
    entered, release = threading.Event(), threading.Event()
    def collect_owner():
        """Hold only the job's Snapshot, as a live collector does."""
        with advisory_file_lock(c.store.directory(c.identifier) / '.snapshot-lock'):
            entered.set()
            release.wait(2)
    thread = threading.Thread(target=collect_owner)
    thread.start()
    assert entered.wait(1)
    try:
        with pytest.raises(VastError, match='busy'):
            CloudQueue(c.store).purge_job_histories([c.identifier])
        assert not c.store.read(c.identifier).get('deleted_at')
        deleted.assert_not_called()
    finally:
        release.set()
        thread.join(2)


def test_admission_rechecks_replaced_lease_and_new_collection_phase(collection, monkeypatch):
    """Changes made while acquiring Snapshot cannot use the earlier eligibility result."""
    from contextlib import contextmanager
    c = collection
    original = vast_collect.bounded_lock
    @contextmanager
    def changed(target, **kwargs):
        """Enter the real lock and race a supervisor's ownership transition."""
        with original(target, **kwargs):
            if Path(target).name == '.snapshot-lock':
                c.store.update(c.identifier, lease_id=None)
            yield
    monkeypatch.setattr(vast_collect, 'bounded_lock', changed)
    assert c.http.post(c.url).status_code == 409
    c.provider.assert_not_called()


def test_entire_stop_path_never_reenters_unbounded_finalize_wait(collection):
    """An expired Stop yields to another finalizer instead of blocking in fallback."""
    from file_lock import advisory_file_lock
    from vast_job_runner import stop_and_collect
    c = collection
    c.store.update(c.identifier, final_collected=True, stop_rpc_at=time.time() - 1000,
                   final_collection_at=time.time() - 1000)
    entered, release = threading.Event(), threading.Event()
    def owner():
        """Keep the competing publisher alive beyond the caller's expired budget."""
        with advisory_file_lock(c.store.directory(c.identifier) / '.finalize-lock'):
            entered.set()
            release.wait(2)
    thread = threading.Thread(target=owner)
    thread.start()
    assert entered.wait(1)
    before = time.monotonic()
    try:
        assert stop_and_collect(object(), c.store, c.identifier,
            c.store.read(c.lease, 'intent.json'), lease_id=c.lease) is False
        assert time.monotonic() - before < .5
        assert c.store.read(c.identifier)['status'] == 'running'
        assert c.store.read(c.lease, 'control.json') == {'stop': False, 'cleanup': False}
    finally:
        release.set()
        thread.join(2)


def test_manual_publication_cannot_regress_concurrent_finalizer(collection, monkeypatch):
    """The waiting automatic publisher sees a complete partial transaction, then wins."""
    from concurrent.futures import ThreadPoolExecutor
    from vast_snapshot import snapshot
    from vast_job_runner import finalize_collected_results
    c = collection
    entered, release, waiting = threading.Event(), threading.Event(), threading.Event()
    original = vast_transfer.WorkerConnection.collect
    def download(*args, **kwargs):
        """Hold the manual request inside the production Snapshot transaction."""
        entered.set()
        assert release.wait(3)
        return original(*args, **kwargs)
    monkeypatch.setattr(vast_transfer.WorkerConnection, 'collect', download)
    def automatic():
        """Model verified final download and ordinary finalization with real locks/import."""
        waiting.set()
        with snapshot(c.store, c.identifier):
            partial = c.store.read(c.identifier)
            assert partial['result_partial'] is True
            source = ensure_private_directory(c.store.directory(c.identifier) / 'final-results/optimize_results/run/pareto')
            write_json(source / 'final.json', {'config': {}})
            (source.parent / 'all_results.bin').write_bytes(msgpack.packb({'config': {}}) * 12)
            write_json(source.parent.parent.parent / 'finished.json',
                       {'exit_code': 0, 'finished_at': 123, 'wall_seconds': 12})
            c.store.update(c.identifier, final_collected=True)
        return finalize_collected_results(c.store, c.identifier)
    with ThreadPoolExecutor(max_workers=2) as pool:
        manual = pool.submit(vast_collect.collect_results, c.store, c.identifier)
        assert entered.wait(2)
        final = pool.submit(automatic)
        assert waiting.wait(1)
        assert not final.done()
        release.set()
        first = manual.result(timeout=3)
        last = final.result(timeout=3)
    assert first['result_path'] == last['result_path']
    assert last['result_partial'] is False and last['status'] == 'completed'
    published = Path(last['result_path'])
    assert [p.name for p in (published / 'pareto').glob('*.json')] == ['final.json']
    assert json.loads((published / '.pbgui_vast.json').read_text())['partial'] is False
