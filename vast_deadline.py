"""Durable, acknowledged deadline changes for a shared Vast rental."""
from __future__ import annotations

import json
import math
import tempfile
import time
import uuid

from file_lock import advisory_file_lock
from vast_jobs import job_id
from vast_provider import VastError

SERVICE = 'VastRunner'


def maximum_deadline(intent: dict) -> float:
    """Retain the original budget and transfer allowance within the 24-hour ceiling."""
    offer = intent['offer']
    if any(type(offer.get(key)) not in (int, float) or not math.isfinite(offer[key]) or offer[key] < 0
           for key in ('download_gb_usd', 'upload_gb_usd', 'price_hour_usd')) or offer['price_hour_usd'] <= 0:
        return intent['deadline']  # Legacy leases without price metadata cannot be extended.
    reserve = max(.05, 2 * intent.get('bundle_bytes', 0) / 1e9 * offer['download_gb_usd']
                  + 3 * offer['upload_gb_usd'] * max(1, intent.get('job_count', 1)))
    return intent['accepted_at'] + min(86400, max(0, intent['budget_usd'] - reserve)
                                      / offer['price_hour_usd'] * 3600)


def effective_intent(store, identifier: str, intent: dict) -> dict:
    """Use only remotely confirmed adjustments; preserve immutable rental authorization."""
    state = store.read(identifier)
    if not state.get('deadline_confirmed') and state.get('budget_confirmed') is None and state.get('transfer_reserve_confirmed') is None:
        return intent
    budget = state.get('budget_confirmed', intent['budget_usd'])
    if type(budget) not in (int, float) or not math.isfinite(budget) or budget < .1:
        raise VastError('Invalid confirmed rental budget', 422)
    current = dict(intent, budget_usd=budget)
    confirmed = state.get('deadline_confirmed')
    deadline = current['deadline'] if not confirmed else confirmed.get('deadline')
    if type(deadline) not in (int, float) or not math.isfinite(deadline) or not current['accepted_at'] < deadline <= maximum_deadline(current):
        raise VastError('Invalid confirmed rental deadline', 422)
    available = current['budget_usd'] - (deadline - current['accepted_at']) / 3600 * current['offer']['price_hour_usd']
    reserve = state.get('transfer_reserve_confirmed', current['transfer_reserve_usd'])
    if type(reserve) not in (int, float) or not math.isfinite(reserve) or reserve < current['transfer_reserve_usd']:
        raise VastError('Invalid confirmed transfer reserve', 422)
    return dict(current, deadline=deadline, transfer_reserve_usd=min(reserve, available))


def request_deadline(queue, identifier: str, expected: float, minutes: int) -> dict:
    """Persist one bounded adjustment without silently applying or starting anything."""
    identifier = job_id(identifier)
    if type(minutes) is not int or not 1 <= abs(minutes) <= 1440:
        raise VastError('Choose a deadline adjustment between 1 and 1,440 minutes', 422)
    with advisory_file_lock(queue.root / '.queue-lock'), advisory_file_lock(queue.store.directory(identifier) / '.deadline-lock'):
        worker = queue.worker_for(identifier)
        if not worker or worker['id'] != identifier or worker.get('rental_state') != 'active':
            raise VastError('This rental is no longer active', 409)
        control = queue.store.read(identifier, 'control.json')
        if control.get('stop') or control.get('cleanup'):
            raise VastError('Rental cleanup has already been requested', 409)
        if worker.get('deadline_protocol') not in (1, 2):
            raise VastError('This worker requires an updated deadline guard; its deadline cannot be changed', 409)
        if worker.get('deadline_request'):
            raise VastError('A deadline change is awaiting worker confirmation', 409)
        intent = effective_intent(queue.store, identifier, queue.store.read(identifier, 'intent.json'))
        current = intent['deadline']
        if expected != current:
            raise VastError('Deadline changed; refresh before adjusting again', 409)
        target = current + minutes * 60
        if min(current, target) < time.time() + 600:
            raise VastError('Keep at least 10 minutes for safe collection and cleanup', 409)
        rental_cost = (target - intent['accepted_at']) / 3600 * intent['offer']['price_hour_usd']
        if target > maximum_deadline(intent) or rental_cost + worker.get('transfer_reserved_used', 0) > intent['budget_usd']:
            raise VastError('Deadline exceeds the existing budget or the 24-hour rental limit', 422)
        request = dict(id=uuid.uuid4().hex, expected=current, deadline=target, job_id=identifier)
        if worker.get('deadline_protocol') == 1 or worker.get('deadline_guard_upgraded_at'):
            from vast_guard_migration import apply_migrated_deadline
            return apply_migrated_deadline(queue, identifier, intent, request)
        queue.store.update(identifier, deadline_request=request, deadline_error=None)
        return {'pending': True, 'deadline': current, 'requested_deadline': target}


def request_transfer_reserve(queue, identifier: str, expected: float, reserve: float) -> dict:
    """Move part of the unused GPU-time budget into acknowledged transfer allowance."""
    identifier = job_id(identifier)
    if any(type(value) not in (int, float) or not math.isfinite(value) for value in (expected, reserve)) or not .05 <= reserve <= 100:
        raise VastError('Choose a valid transfer reserve', 422)
    with advisory_file_lock(queue.root / '.queue-lock'), advisory_file_lock(queue.store.directory(identifier) / '.deadline-lock'):
        worker = queue.worker_for(identifier)
        if not worker or worker['id'] != identifier or worker.get('rental_state') != 'active':
            raise VastError('This rental is no longer active', 409)
        if worker.get('deadline_protocol') != 2:
            raise VastError('This worker needs the updated budget-control guard before changing its transfer reserve', 409)
        control = queue.store.read(identifier, 'control.json')
        if control.get('stop') or control.get('cleanup'):
            raise VastError('Rental cleanup has already been requested', 409)
        if worker.get('deadline_request'):
            raise VastError('A deadline change is awaiting worker confirmation', 409)
        intent = effective_intent(queue.store, identifier, queue.store.read(identifier, 'intent.json'))
        if abs(expected - intent['transfer_reserve_usd']) > 1e-9:
            raise VastError('Transfer reserve changed; refresh before adjusting again', 409)
        if reserve < expected:
            raise VastError('Transfer reserve can only be increased for an active rental', 422)
        delta = reserve - expected
        if delta < 1e-9:
            return {'pending': False, 'transfer_reserve_usd': expected, 'deadline': intent['deadline']}
        rate = intent['offer']['price_hour_usd']
        minutes = math.ceil(delta / rate * 60)
        target = intent['deadline'] - minutes * 60
        if target < time.time() + 600:
            raise VastError('Transfer reserve would leave less than 10 minutes for safe collection and cleanup', 422)
        available = intent['budget_usd'] - (target - intent['accepted_at']) / 3600 * rate
        if reserve > available + 1e-9:
            raise VastError('Transfer reserve exceeds the existing budget', 422)
        request = dict(id=uuid.uuid4().hex, expected=intent['deadline'], deadline=target, job_id=identifier,
                       transfer_reserve_usd=reserve)
        queue.store.update(identifier, deadline_request=request, deadline_error=None)
        return {'pending': True, 'transfer_reserve_usd': reserve, 'deadline': intent['deadline'], 'requested_deadline': target}


def request_budget(queue, identifier: str, expected: float, budget: float) -> dict:
    """Change the budget target by proposing the corresponding safe deadline."""
    identifier = job_id(identifier)
    if any(type(value) not in (int, float) or not math.isfinite(value) for value in (expected, budget)) or not .1 <= budget <= 100:
        raise VastError('Choose a valid budget target', 422)
    with advisory_file_lock(queue.root / '.queue-lock'), advisory_file_lock(queue.store.directory(identifier) / '.deadline-lock'):
        worker = queue.worker_for(identifier)
        if not worker or worker['id'] != identifier or worker.get('rental_state') != 'active':
            raise VastError('This rental is no longer active', 409)
        if worker.get('deadline_protocol') != 2:
            raise VastError('This worker needs the updated budget-control guard before changing its budget', 409)
        control = queue.store.read(identifier, 'control.json')
        if control.get('stop') or control.get('cleanup'):
            raise VastError('Rental cleanup has already been requested', 409)
        if worker.get('deadline_request'):
            raise VastError('A deadline change is awaiting worker confirmation', 409)
        intent = effective_intent(queue.store, identifier, queue.store.read(identifier, 'intent.json'))
        if abs(expected - intent['budget_usd']) > 1e-9:
            raise VastError('Budget changed; refresh before adjusting again', 409)
        used = worker.get('transfer_reserved_used', 0)
        if budget < max(intent['transfer_reserve_usd'], used):
            raise VastError('Budget must cover the transfer reserve already allocated to this rental', 422)
        target = intent['accepted_at'] + min(86400, (budget - intent['transfer_reserve_usd']) / intent['offer']['price_hour_usd'] * 3600)
        if target < time.time() + 600:
            raise VastError('Budget would leave less than 10 minutes for safe collection and cleanup', 422)
        request = dict(id=uuid.uuid4().hex, expected=intent['deadline'], deadline=target, job_id=identifier,
                       budget_usd=budget, max_deadline=target)
        queue.store.update(identifier, deadline_request=request, deadline_error=None)
        return {'pending': True, 'budget_usd': budget, 'deadline': intent['deadline'], 'requested_deadline': target}


def reconcile_deadline(store, identifier: str, client, row: dict, intent: dict) -> None:
    """Let the independent rental guard deliver and verify a persisted adjustment."""
    from vast_transfer import WorkerConnection
    directory = store.directory(identifier)
    # The existing worker controller owns SSH bootstrap. Never race key creation.
    if not (directory / 'known_hosts').is_file() or not (directory / 'ssh-key').is_file():
        return
    with advisory_file_lock(directory / '.deadline-lock'):
        state = store.read(identifier)
        request = state.get('deadline_request')
        if state.get('deadline_protocol') is not None and not request:
            return
        connection = WorkerConnection(store, identifier, row)
        guard = json.loads(connection.command('cat /work/pbgui/guard.json', timeout=10, max_output=8192))
        if guard.get('job_id') != identifier or guard.get('instance_id') != row['id']:
            raise VastError('Deadline guard identity mismatch', 422)
        protocol = guard.get('deadline_protocol')
        supported = protocol in (1, 2)
        store.update(identifier, deadline_protocol=protocol if supported else 0)
        if not request:
            return
        if guard.get('request_id') == request['id'] and guard.get('deadline') == request['deadline']:
            changes = dict(deadline_confirmed=request, deadline=request['deadline'], deadline_request=None, deadline_error=None)
            if 'transfer_reserve_usd' in request:
                changes['transfer_reserve_confirmed'] = request['transfer_reserve_usd']
            if 'budget_usd' in request:
                changes['budget_confirmed'] = request['budget_usd']
            store.update(identifier, **changes)
            return
        control = store.read(identifier, 'control.json')
        if (not supported or control.get('stop') or control.get('cleanup')
                or min(intent['deadline'], request['deadline']) < time.time() + 300):
            store.update(identifier, deadline_request=None, deadline_error='Deadline change could not be confirmed before cleanup; the previous local deadline remains active.')
            return
        if guard.get('rejected_request_id') == request['id']:
            store.update(identifier, deadline_request=None, deadline_error='Worker rejected the deadline change; previous deadline retained.')
            return
        with tempfile.TemporaryFile() as source:
            source.write(json.dumps(request).encode()); source.seek(0)
            connection.command('umask 077; cat > /work/pbgui/deadline-request.tmp && mv /work/pbgui/deadline-request.tmp /work/pbgui/deadline-request.json',
                               stdin=source, timeout=10)
