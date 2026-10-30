"""Offline ownership, recoverability and allowance checks for visible rental controls."""
import copy
import asyncio
import json
from types import SimpleNamespace

import pytest

from pb8_loop_rental_controls import change_budget, extend_rental, reconcile_extension, rental_snapshot
from vast_deadline import effective_intent
from vast_provider import VastError
from tests.test_pb8_loop_optimizer import record
from tests.test_vast_deadline import rental


@pytest.fixture
def bound(record, rental):
    """Bind an isolated Loop to one isolated rental with different deadlines."""
    store, row = record
    queue, worker_id, intent = rental
    row = store.update(row['owner'], row['id'], lambda current: current.update(
        status='running', started_at=1000, deadline=7000))
    queue.store.update(worker_id, kind='worker', loop_owner=row['owner'], loop_id=row['id'])
    return store, queue, row, worker_id


def extend(bound, minutes=30, **changes):
    """Submit explicitly reviewed deadlines against temporary stores only."""
    store, queue, row, worker_id = bound
    args = dict(owner=row['owner'], loop_id=row['id'], worker_id=worker_id,
                expected_deadline=8000, expected_loop_deadline=7000, minutes=minutes)
    args.update(changes)
    return extend_rental(store, queue, **args)


def test_use_existing_rental_time_without_remote_request(bound):
    """An already longer rental needs only a Loop allowance update."""
    store, queue, row, worker_id = bound
    result = extend(bound, minutes=10)
    assert result['deadline'] == 7600
    assert result['control_generation'] == row['control_generation'] + 1
    assert not queue.store.read(worker_id).get('deadline_request')
    assert queue.store.read(worker_id, 'intent.json')['deadline'] == 8000


def test_remote_confirmation_before_loop_extension(bound):
    """Pending/rejected requests retain the old deadline; exact confirmation applies once."""
    store, queue, row, worker_id = bound
    before = copy.deepcopy(row['settings'])
    pending = extend(bound)
    request = queue.store.read(worker_id)['deadline_request']
    assert request['deadline'] == 8840
    assert pending['deadline'] == 7000
    assert reconcile_extension(store, queue, pending)['deadline'] == 7000
    queue.store.update(worker_id, deadline_request=None, deadline_confirmed=request, deadline=request['deadline'])
    confirmed = reconcile_extension(store, queue, pending)
    assert confirmed['deadline'] == 8800
    assert not confirmed.get('rental_extension')
    assert confirmed['settings']['max_runs'] == before['max_runs']
    assert queue.store.read(worker_id, 'intent.json')['budget_usd'] == 2
    assert reconcile_extension(store, queue, confirmed) == confirmed


@pytest.mark.parametrize('problem', ['owner', 'worker', 'stale_loop', 'stale_rental', 'stopped', 'cleanup', 'pending', 'budget'])
def test_reject_invalid_or_unaffordable_extension(bound, problem):
    """No control or budget is changed on stale, foreign or unaffordable requests."""
    store, queue, row, worker_id = bound
    changes = {}
    if problem == 'owner':
        queue.store.update(worker_id, loop_owner='b' * 32)
    elif problem == 'worker':
        changes['worker_id'] = 'b' * 32
    elif problem == 'stale_loop':
        changes['expected_loop_deadline'] = 6999
    elif problem == 'stale_rental':
        changes['expected_deadline'] = 7999
    elif problem == 'stopped':
        store.update(row['owner'], row['id'], lambda current: current.update(status='stopping'))
    elif problem == 'cleanup':
        queue.store.control(worker_id, 'cleanup')
    elif problem == 'pending':
        queue.store.update(worker_id, deadline_request={'id': 'pending'})
    else:
        changes['minutes'] = 1440
    with pytest.raises(VastError):
        extend(bound, **changes)
    assert store.read(row['owner'], row['id'])['deadline'] == 7000
    assert not store.read(row['owner'], row['id']).get('rental_extension')
    assert queue.store.read(worker_id, 'intent.json')['budget_usd'] == 2


@pytest.mark.parametrize('problem', ['rejected', 'foreign_confirmation', 'stopped', 'handover'])
def test_failed_confirmation_never_revives_loop(bound, problem):
    """Cleanup, ownership changes and unrelated confirmations cannot grant time."""
    store, queue, row, worker_id = bound
    pending = extend(bound)
    request = queue.store.read(worker_id)['deadline_request']
    if problem == 'foreign_confirmation':
        request = dict(request, expected=7999)
    queue.store.update(worker_id, deadline_request=None,
                       deadline_confirmed=None if problem == 'rejected' else request)
    if problem == 'stopped':
        store.update(row['owner'], row['id'], lambda current: current.update(status='stopping'))
    if problem == 'handover':
        queue.store.update(worker_id, loop_id='b' * 32)
    result = reconcile_extension(store, queue, pending)
    assert result['deadline'] == 7000
    assert result['rental_extension_error']
    assert not result.get('rental_extension')


def test_snapshot_is_public_and_bound_to_owner(bound):
    """The overview contains no unrelated workers or private intent metadata."""
    store, queue, row, worker_id = bound
    snapshot = rental_snapshot(queue, row)
    assert snapshot['worker_id'] == worker_id and snapshot['can_extend']
    assert snapshot['loop_deadline'] == 7000 and snapshot['budget_usd'] == 2
    assert not {'ssh', 'key', 'intent', 'token', 'offer'} & snapshot.keys()
    assert rental_snapshot(queue, dict(row, owner='b' * 32)) is None


def test_budget_edit_waits_for_confirmation_and_preserves_loop_deadline(bound):
    """A larger budget changes the guard request, never the original intent or Loop time."""
    store, queue, row, worker_id = bound
    result = change_budget(store, queue, row['owner'], row['id'], worker_id, 8000, 2, 3)
    assert result['pending']
    assert rental_snapshot(queue, row)['budget_usd'] == 2
    request = queue.store.read(worker_id)['deadline_request']
    queue.store.update(worker_id, deadline_request=None, deadline_confirmed=request,
                       budget_confirmed=3, deadline=request['deadline'])
    snapshot = rental_snapshot(queue, row)
    assert snapshot['budget_usd'] == 3 and not snapshot['pending']
    assert store.read(row['owner'], row['id'])['deadline'] == 7000
    assert queue.store.read(worker_id, 'intent.json')['budget_usd'] == 2
    assert effective_intent(queue.store, worker_id, queue.store.read(worker_id, 'intent.json'))['budget_usd'] == 3


@pytest.mark.parametrize('problem', ['owner', 'worker', 'stale_budget', 'stale_deadline', 'cleanup', 'pending', 'protocol', 'too_low'])
def test_budget_edit_rejects_unreviewed_or_unsafe_requests(bound, problem):
    """Reject foreign, stale, pending and insufficient-budget changes before guard dispatch."""
    store, queue, row, worker_id = bound
    expected_deadline, expected_budget, budget = 8000, 2, 3
    requested_worker = worker_id
    if problem == 'owner':
        queue.store.update(worker_id, loop_owner='b'*32)
    elif problem == 'worker':
        requested_worker = 'b'*32
    elif problem == 'stale_budget':
        expected_budget = 1
    elif problem == 'stale_deadline':
        expected_deadline = 7999
    elif problem == 'cleanup':
        queue.store.control(worker_id, 'cleanup')
    elif problem == 'pending':
        queue.store.update(worker_id, deadline_request={'id': 'previous'})
    elif problem == 'protocol':
        queue.store.update(worker_id, deadline_protocol=1)
    else:
        budget = .6
    before = copy.deepcopy(queue.store.read(worker_id))
    with pytest.raises(VastError):
        change_budget(store, queue, row['owner'], row['id'], requested_worker, expected_deadline, expected_budget, budget)
    assert queue.store.read(worker_id) == before
    assert store.read(row['owner'], row['id'])['deadline'] == 7000


def test_cost_snapshot_distinguishes_estimate_and_provider_actuals(bound):
    """Never present elapsed compute estimates as invoiced total or expose billing metadata."""
    store, queue, row, worker_id = bound
    queue.store.update(worker_id, billing=dict(amount_usd=.044, reported_at=950, error=None,
                                               private_metadata='must not expose'))
    snapshot = rental_snapshot(queue, row)
    assert snapshot['billing'] == dict(amount_usd=.044, reported_at=950, error=None)
    assert snapshot['estimated_compute_usd'] == pytest.approx(500 / 3600 * .1)


def test_billing_endpoint_checks_owner_before_provider_access(bound, monkeypatch):
    """A foreign Loop cannot trigger billing access or disclose another rental's charges."""
    from api import loop_optimizer_v8 as api
    from fastapi import HTTPException
    store, queue, row, worker_id = bound
    calls = []
    def billing(store, lease):
        """Return isolated charges and record which rental was accessed."""
        calls.append(lease)
        return dict(amount_usd=.1, reported_at=950, error=None, private='not public')
    monkeypatch.setattr(api, '_controller', SimpleNamespace(store=store))
    monkeypatch.setattr(api, '_owner', lambda session: row['owner'])
    monkeypatch.setattr('vast_queue.CloudQueue', lambda: queue)
    monkeypatch.setattr('vast_billing.instance_charges', billing)
    response = asyncio.run(api.loop_rental_charges(row['id'], session=None))
    assert json.loads(response.body) == dict(worker_id=worker_id, billing=dict(amount_usd=.1, reported_at=950, error=None))
    monkeypatch.setattr(api, '_owner', lambda session: 'b'*32)
    with pytest.raises(HTTPException) as error:
        asyncio.run(api.loop_rental_charges(row['id'], session=None))
    assert error.value.status_code == 404 and calls == [worker_id]


@pytest.mark.parametrize('replacement', [False, True])
def test_rental_status_and_charges_do_not_hydrate_loop_history(bound, monkeypatch, replacement):
    """Frequent rental reads retain control semantics without loading complete runs."""
    from api import loop_optimizer_v8 as api
    store, queue, row, worker_id = bound
    if replacement:
        row = store.update(row['owner'], row['id'], lambda value: value.update(
            gpu_replacement_id='replace', gpu_replacements={'replace': {'step': 'transfer'}}))
    expected = rental_snapshot(queue, row)
    original = store.read

    def read(owner, loop_id, **kwargs):
        """Reject the expensive full-record path rather than merely timing a tiny fixture."""
        assert kwargs.get('fields') and not {'jobs', 'history', 'initial_config'} & kwargs['fields']
        return original(owner, loop_id, **kwargs)

    monkeypatch.setattr(store, 'read', read)
    monkeypatch.setattr(api, '_controller', SimpleNamespace(store=store))
    monkeypatch.setattr(api, '_owner', lambda session: row['owner'])
    monkeypatch.setattr('vast_queue.CloudQueue', lambda: queue)
    monkeypatch.setattr('vast_billing.instance_charges', lambda *args: None)
    response = asyncio.run(api.loop_rental(row['id'], session=None))
    assert json.loads(response.body) == {'rental': expected}
    response = asyncio.run(api.loop_rental_charges(row['id'], session=None))
    assert json.loads(response.body) == {'worker_id': worker_id, 'billing': None}


def test_budget_endpoint_preserves_conflict_status(bound, monkeypatch):
    """The browser receives an actionable conflict instead of a generic server error."""
    from api import loop_optimizer_v8 as api
    from fastapi import HTTPException
    store, queue, row, worker_id = bound
    monkeypatch.setattr(api, '_controller', SimpleNamespace(store=store))
    monkeypatch.setattr(api, '_owner', lambda session: row['owner'])
    monkeypatch.setattr('vast_queue.CloudQueue', lambda: queue)
    body = api.LoopRentalBudget(worker_id=worker_id, expected_deadline=7999, expected_budget_usd=2, budget_usd=3)
    with pytest.raises(HTTPException) as error:
        asyncio.run(api.loop_rental_budget(row['id'], body, session=None))
    assert error.value.status_code == 409
    assert not queue.store.read(worker_id).get('deadline_request')
