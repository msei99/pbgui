"""Offline deadline adjustment, bounds, acknowledgement and retry contracts."""
import json
from types import SimpleNamespace

import pytest

from secure_files import ensure_private_directory
from vast_jobs import JobStore, write_json
from vast_queue import CloudQueue
from vast_provider import VastError
from vast_deadline import effective_intent, request_budget, request_deadline, reconcile_deadline, maximum_deadline
from setup.vast_gpu_benchmark.cloud_worker import apply_deadline_request


def test_legacy_lease_without_changes_keeps_original_intent():
    """Unadjusted legacy rentals need no newly introduced billing metadata."""
    intent = {'deadline': 8000}
    store = SimpleNamespace(read=lambda identifier: {})
    assert effective_intent(store, 'a'*32, intent) is intent


@pytest.mark.parametrize('minutes', [1, 7, 30, 60, -7])
def test_old_worker_upgrades_before_sending_any_minute_step(rental, monkeypatch, minutes):
    """Legacy workers migrate on explicit adjustment, retaining confirmation semantics."""
    queue, identifier, _ = rental
    queue.store.update(identifier, deadline_protocol=1)
    calls = []
    def upgrade(store, worker_id, intent):
        """Record migration without connecting to any real worker."""
        assert not store.read(worker_id).get('deadline_request')
        calls.append('upgrade')
        store.update(worker_id, deadline_protocol=2)
        return object()
    monkeypatch.setattr('vast_guard_migration.upgrade_guard', upgrade)
    monkeypatch.setattr('vast_guard_migration.confirm_migrated_request', lambda *args: False)
    result = request_deadline(queue, identifier, 8000, minutes)
    assert calls == ['upgrade'] and result['pending']
    assert queue.store.read(identifier)['deadline_request']['deadline'] == 8000 + minutes * 60


@pytest.mark.parametrize('control', ['stop', 'cleanup'])
def test_budget_and_reserve_reject_cleanup(rental, control):
    """A budget edit cannot interfere with a rental already being stopped."""
    from vast_deadline import request_transfer_reserve
    queue, identifier, _ = rental
    queue.store.control(identifier, control)
    with pytest.raises(VastError, match='cleanup'):
        request_budget(queue, identifier, 2, 3)
    with pytest.raises(VastError, match='cleanup'):
        request_transfer_reserve(queue, identifier, .5, .6)


@pytest.fixture
def rental(tmp_path, monkeypatch):
    """Provide an isolated, active lease with a capable guard and ample budget."""
    monkeypatch.setattr('vast_deadline.time.time', lambda: 1000)
    store = JobStore(tmp_path)
    identifier = 'a'*32
    folder = ensure_private_directory(store.root / 'jobs' / identifier)
    intent = dict(id=identifier, accepted_at=500, deadline=8000, budget_usd=2,
                  bundle_bytes=1000, job_count=1, transfer_reserve_usd=.5,
                  offer=dict(download_gb_usd=0, upload_gb_usd=0, price_hour_usd=.1))
    write_json(folder/'intent.json', intent)
    write_json(folder/'state.json', dict(id=identifier, status='running', rental_state='active',
               deadline_protocol=2, deadline=8000, generation=0))
    write_json(folder/'control.json', dict(stop=False, cleanup=False))
    queue = CloudQueue(store)
    queue.update(worker_id=identifier)
    return queue, identifier, intent


def test_deadline_request_is_pending_until_confirmed(rental):
    """Request persistence never extends local enforcement before remote confirmation."""
    queue, identifier, intent = rental
    result = request_deadline(queue, identifier, 8000, 30)
    assert result['pending'] and result['deadline'] == 8000
    assert effective_intent(queue.store, identifier, intent)['deadline'] == 8000
    with pytest.raises(VastError, match='awaiting'):
        request_deadline(queue, identifier, 8000, 30)
    request = queue.store.read(identifier)['deadline_request']
    queue.store.update(identifier, deadline_confirmed=request, deadline_request=None)
    assert effective_intent(queue.store, identifier, intent)['deadline'] == 9800
    assert queue.store.read(identifier, 'intent.json')['deadline'] == 8000


@pytest.mark.parametrize('change,expected', [
    ({'deadline_protocol':0}, 'updated'),
    ({'rental_state':'destroy_pending'}, 'active'),
    ({'deadline_request':{'id':'b'*32}}, 'awaiting'),
])
def test_deadline_rejects_unavailable_or_busy_rental(rental, change, expected):
    """Old guards, cleanup and in-flight changes cannot accept another adjustment."""
    queue, identifier, _ = rental
    queue.store.update(identifier, **change)
    with pytest.raises(VastError, match=expected):
        request_deadline(queue, identifier, 8000, 30)


@pytest.mark.parametrize('expected,minutes', [(7000,30),(8000,0),(8000,1441),(8000,True)])
def test_deadline_rejects_stale_or_invalid_steps(rental, expected, minutes):
    """An old browser cannot apply its click to a newer deadline."""
    queue, identifier, _ = rental
    with pytest.raises(VastError):
        request_deadline(queue, identifier, expected, minutes)


def test_deadline_keeps_budget_and_cleanup_margin(rental):
    """Extensions preserve spending allowance and reductions retain collection time."""
    queue, identifier, intent = rental
    intent['budget_usd'] = .27
    write_json(queue.store.directory(identifier)/'intent.json', intent)
    assert maximum_deadline(intent) < 9800
    with pytest.raises(VastError, match='budget'):
        request_deadline(queue, identifier, 8000, 30)
    intent.update(deadline=2500, budget_usd=2)
    write_json(queue.store.directory(identifier)/'intent.json', intent)
    with pytest.raises(VastError, match='10 minutes'):
        request_deadline(queue, identifier, 2500, -30)


@pytest.mark.parametrize('target', [6200, 9800])
def test_worker_applies_adjustment_once(target):
    """Worker acknowledges an absolute target and repeats it idempotently."""
    state = dict(job_id='a'*32, deadline=8000, max_deadline=20000)
    request = dict(id='b'*32, expected=8000, deadline=target, job_id='a'*32)
    updated = apply_deadline_request(state, request, 1000)
    assert updated['deadline'] == target
    assert updated['request_id'] == request['id']
    assert apply_deadline_request(updated, request, 1000) == updated


@pytest.mark.parametrize('change', [dict(deadline=float('nan')), dict(deadline=30000),
    dict(deadline=0), dict(expected=7999), dict(job_id='c'*32), dict(id='invalid')])
def test_worker_rejects_invalid_adjustment(change):
    """Guard limits cannot be bypassed by malformed or stale requests."""
    state = dict(job_id='a'*32, deadline=8000, max_deadline=20000)
    request = dict(id='b'*32, expected=8000, deadline=9800, job_id='a'*32)
    request.update(change)
    assert apply_deadline_request(state, request, 1000)['deadline'] == 8000


def test_reconcile_lost_ack_reuses_request_and_confirms(rental, monkeypatch):
    """A supervisor retry after remote success reads acknowledgement without resending."""
    queue, identifier, intent = rental
    request_deadline(queue, identifier, 8000, 30)
    store = queue.store
    for file in ('known_hosts','ssh-key'):
        (store.directory(identifier)/file).touch()
    request = store.read(identifier)['deadline_request']
    guard = dict(job_id=identifier, instance_id=123, deadline=8000, deadline_protocol=1)
    commands = []

    def command(cmd, **kwargs):
        """Emulate request delivery, with acknowledgement visible on the next poll."""
        commands.append(cmd)
        if cmd.startswith('cat '):
            return json.dumps(guard).encode()
        sent = json.load(kwargs['stdin'])
        guard.update(request_id=sent['id'], deadline=sent['deadline'])
        return b''

    monkeypatch.setattr('vast_transfer.WorkerConnection', lambda *a: SimpleNamespace(command=command))
    reconcile_deadline(store, identifier, None, {'id':123}, intent)
    assert store.read(identifier)['deadline'] == 8000
    reconcile_deadline(store, identifier, None, {'id':123}, intent)
    assert store.read(identifier)['deadline'] == 9800
    assert store.read(identifier)['deadline_request'] is None
    assert len([cmd for cmd in commands if cmd.startswith('umask')]) == 1


def test_deadline_preserves_already_reserved_transfer_budget(rental):
    """An extension cannot spend transfer allowance already committed to other jobs."""
    queue, identifier, _ = rental
    queue.store.update(identifier, transfer_reserved_used=1.9)
    with pytest.raises(VastError, match='budget'):
        request_deadline(queue, identifier, 8000, 30)


def test_budget_request_recalculates_deadline_after_guard_confirmation(rental):
    """Editing the active budget changes its acknowledged deadline without exceeding it."""
    queue, identifier, intent = rental
    result = request_budget(queue, identifier, 2, .7)
    assert result['pending'] and result['requested_deadline'] == pytest.approx(7700)
    request = queue.store.read(identifier)['deadline_request']
    queue.store.update(identifier, deadline_confirmed=request, budget_confirmed=.7, deadline_request=None)
    effective = effective_intent(queue.store, identifier, intent)
    assert effective['budget_usd'] == .7
    assert effective['deadline'] == pytest.approx(7700)
    assert effective['transfer_reserve_usd'] == .5


@pytest.mark.parametrize('restart,maximum', [(False, 20000), (True, 20000), (True, 25000)])
def test_guard_process_publishes_and_restores_confirmed_deadline(tmp_path, monkeypatch, restart, maximum):
    """Exercise the real guard loop without sleeping or calling the provider."""
    from setup.vast_gpu_benchmark import cloud_worker as worker
    monkeypatch.setattr(worker, 'ROOT', tmp_path)
    monkeypatch.setenv('PBGUI_DEADLINE', '8000')
    monkeypatch.setenv('PBGUI_MAX_DEADLINE', '20000')
    monkeypatch.setenv('PBGUI_JOB_ID', 'a'*32)
    monkeypatch.setenv('CONTAINER_ID', '123')
    monkeypatch.setenv('CONTAINER_API_KEY', 'isolated-test-only')
    monkeypatch.setattr(worker.time, 'time', lambda: 1000)
    monkeypatch.setattr(worker.time, 'monotonic', lambda: 5000)
    request = dict(id='b'*32, expected=8000, deadline=9800, job_id='a'*32)
    if restart:
        worker.write_record(tmp_path/'guard.json', dict(instance_id=123, job_id='a'*32,
            deadline=9800, max_deadline=maximum, deadline_protocol=2, request_id='b'*32))
    else:
        worker.write_record(tmp_path/'deadline-request.json', request)

    class Done(Exception):
        """Terminate only this isolated loop after its first publication."""

    def stop_sleep(seconds):
        """End before any real time elapses or deletion path is entered."""
        raise Done()

    monkeypatch.setattr(worker.time, 'sleep', stop_sleep)
    with pytest.raises(Done):
        worker.guard()
    saved = json.loads((tmp_path/'guard.json').read_text())
    assert saved['deadline'] == 9800
    assert saved['request_id'] == 'b'*32


@pytest.mark.parametrize('outcome', ['failure', 'stop', 'confirmed', 'pending'])
def test_migration_owns_restart_blocker_and_preserves_confirmed_deadline(rental, monkeypatch, outcome):
    """Handover failure/cleanup never sends a change; only acknowledgement applies it."""
    import vast_guard_migration as migration
    queue, identifier, intent = rental
    queue.store.update(identifier, deadline_protocol=1)
    sent = []

    def upgrade(store, worker_id, current):
        """Emulate the remote handover while checking restart ownership."""
        assert migration.restart_block_reason()
        if outcome == 'failure':
            raise VastError('Migration failed', 409)
        if outcome == 'stop':
            write_json(store.directory(worker_id) / 'control.json', {'stop': True})
        store.update(worker_id, deadline_protocol=2, deadline_guard_upgraded_at=1000)
        return object()

    def confirm(store, worker_id, connection, request):
        """Acknowledge only when explicitly selected by the test."""
        assert migration.restart_block_reason()
        sent.append(request)
        if outcome == 'confirmed':
            store.update(worker_id, deadline_confirmed=request, deadline_request=None)
            return True
        return False

    monkeypatch.setattr(migration, 'upgrade_guard', upgrade)
    monkeypatch.setattr(migration, 'confirm_migrated_request', confirm)
    if outcome in ('failure', 'stop'):
        with pytest.raises(VastError):
            request_deadline(queue, identifier, 8000, 7)
        assert not sent
        assert not queue.store.read(identifier).get('deadline_request')
    else:
        result = request_deadline(queue, identifier, 8000, 7)
        assert result['pending'] == (outcome == 'pending')
        assert sent[0]['deadline'] == 8420
    assert not migration.restart_block_reason()
    assert effective_intent(queue.store, identifier, intent)['deadline'] == (8420 if outcome == 'confirmed' else 8000)


@pytest.mark.parametrize('response', ['timeout', 'wrong_identity', 'confirmed', 'rejected'])
def test_migrated_confirmation_keeps_pending_on_transport_failure(rental, monkeypatch, response):
    """Transport errors and mismatched identities cannot change local enforcement."""
    from vast_guard_migration import confirm_migrated_request
    queue, identifier, intent = rental
    request = dict(id='d'*32, job_id=identifier, expected=8000, deadline=8420)
    queue.store.update(identifier, deadline_request=request)

    def command(text, **kwargs):
        """Emulate delivery plus a single acknowledgement read."""
        if response == 'timeout':
            raise VastError('SSH timed out', 502)
        if not text.startswith('cat '):
            return ''
        return json.dumps(dict(job_id='wrong' if response == 'wrong_identity' else identifier,
                              instance_id=123, deadline=8420, request_id=request['id'] if response != 'rejected' else '',
                              rejected_request_id=request['id'] if response == 'rejected' else ''))

    connection = SimpleNamespace(command=command, row={'id': 123})
    if response == 'rejected':
        with pytest.raises(VastError, match='rejected'):
            confirm_migrated_request(queue.store, identifier, connection, request)
        assert not queue.store.read(identifier)['deadline_request']
    else:
        assert confirm_migrated_request(queue.store, identifier, connection, request) == (response == 'confirmed')
        assert bool(queue.store.read(identifier)['deadline_request']) == (response != 'confirmed')
    assert effective_intent(queue.store, identifier, intent)['deadline'] == (8420 if response == 'confirmed' else 8000)
