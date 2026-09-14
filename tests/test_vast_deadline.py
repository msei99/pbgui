"""Offline deadline adjustment, bounds, acknowledgement and retry contracts."""
import json
from types import SimpleNamespace

import pytest

from secure_files import ensure_private_directory
from vast_jobs import JobStore, write_json
from vast_queue import CloudQueue
from vast_provider import VastError
from vast_deadline import effective_intent, request_deadline, reconcile_deadline, maximum_deadline
from setup.vast_gpu_benchmark.cloud_worker import apply_deadline_request


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
               deadline_protocol=1, deadline=8000, generation=0))
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


@pytest.mark.parametrize('expected,minutes', [(7000,30),(8000,60),(8000,True)])
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


@pytest.mark.parametrize('restart', [False, True])
def test_guard_process_publishes_and_restores_confirmed_deadline(tmp_path, monkeypatch, restart):
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
            deadline=9800, max_deadline=20000, deadline_protocol=1, request_id='b'*32))
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
