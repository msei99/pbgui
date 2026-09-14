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
    confirmed = store.read(identifier).get('deadline_confirmed')
    if not confirmed:
        return intent
    deadline = confirmed.get('deadline')
    if type(deadline) not in (int, float) or not math.isfinite(deadline) or not intent['accepted_at'] < deadline <= maximum_deadline(intent):
        raise VastError('Invalid confirmed rental deadline', 422)
    return dict(intent, deadline=deadline, transfer_reserve_usd=min(intent['transfer_reserve_usd'],
                intent['budget_usd'] - (deadline - intent['accepted_at']) / 3600 * intent['offer']['price_hour_usd']))


def request_deadline(queue, identifier: str, expected: float, minutes: int) -> dict:
    """Persist one bounded adjustment without silently applying or starting anything."""
    identifier = job_id(identifier)
    if type(minutes) is not int or minutes not in (-30, 30):
        raise VastError('Choose a 30-minute deadline step', 422)
    with advisory_file_lock(queue.root / '.queue-lock'), advisory_file_lock(queue.store.directory(identifier) / '.deadline-lock'):
        worker = queue.worker()
        if not worker or worker['id'] != identifier or worker.get('rental_state') != 'active':
            raise VastError('This rental is no longer active', 409)
        control = queue.store.read(identifier, 'control.json')
        if control.get('stop') or control.get('cleanup'):
            raise VastError('Rental cleanup has already been requested', 409)
        if worker.get('deadline_protocol') != 1:
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
        queue.store.update(identifier, deadline_request=request, deadline_error=None)
        return {'pending': True, 'deadline': current, 'requested_deadline': target}


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
        supported = guard.get('deadline_protocol') == 1
        store.update(identifier, deadline_protocol=1 if supported else 0)
        if not request:
            return
        if guard.get('request_id') == request['id'] and guard.get('deadline') == request['deadline']:
            store.update(identifier, deadline_confirmed=request, deadline=request['deadline'],
                         deadline_request=None, deadline_error=None)
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
