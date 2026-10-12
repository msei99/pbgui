"""Owner-scoped rental extensions with acknowledged Loop deadline updates."""
from __future__ import annotations

import fcntl
import math
import os
import time

from file_lock import advisory_file_lock
from pb8_loop_store import identifier
from vast_deadline import effective_intent, maximum_deadline, request_budget, request_deadline
from vast_provider import VastError

SERVICE = 'PB8Loop'


def public_billing(billing):
    """Keep provider billing metadata bounded to the displayed public fields."""
    return {key: billing.get(key) for key in ('amount_usd', 'reported_at', 'error')} if billing else None


def rental_snapshot(queue, record):
    """Expose only the active rental bound to this run and its owner."""
    workers = [worker for worker in queue.workers()
               if worker.get('loop_id') == record['id'] and worker.get('loop_owner') == record['owner']]
    if not workers:
        return None
    if len(workers) != 1:
        raise VastError('More than one rental is bound to this loop; use the individual rental controls', 409)
    worker = workers[0]
    intent = effective_intent(queue.store, worker['id'], queue.store.read(worker['id'], 'intent.json'))
    control = queue.store.read(worker['id'], 'control.json')
    controllable = (worker.get('rental_state') == 'active'
                    and not control.get('stop') and not control.get('cleanup')
                    and record['status'] in {'running', 'paused'})
    elapsed = max(0, min(time.time(), intent['deadline']) - intent['accepted_at'])
    return dict(worker_id=worker['id'], instance_id=worker.get('instance_id'),
                state=worker.get('rental_state'), deadline=intent['deadline'],
                loop_deadline=record.get('deadline'), budget_usd=intent['budget_usd'],
                price_hour_usd=intent['offer'].get('price_hour_usd'),
                maximum_deadline=maximum_deadline(intent),
                billing=public_billing(worker.get('billing')),
                estimated_compute_usd=elapsed / 3600 * intent['offer'].get('price_hour_usd', 0),
                can_edit_budget=controllable and worker.get('deadline_protocol') == 2,
                pending=bool(worker.get('deadline_request') or record.get('rental_extension')),
                error=record.get('rental_extension_error') or worker.get('deadline_error'),
                can_extend=controllable and worker.get('deadline_protocol') in (1, 2))


def change_budget(store, queue, owner, loop_id, worker_id, expected_deadline, expected_budget, budget):
    """Change one owned rental's budget without silently changing the Loop allowance."""
    owner, loop_id, worker_id = identifier(owner), identifier(loop_id), identifier(worker_id)
    if any(type(value) not in (int, float) or not math.isfinite(value) for value in (budget, expected_budget)) or not .1 <= budget <= 100:
        raise VastError('Choose a budget between $0.10 and $100.00', 422)
    with advisory_file_lock(queue.root / '.queue-lock'):
        record = store.read(owner, loop_id)
        rental = rental_snapshot(queue, record)
        if not rental or rental['worker_id'] != worker_id or not rental['can_edit_budget']:
            raise VastError('This rental is not available for budget changes by this loop', 409)
        if rental['pending']:
            raise VastError('A rental change is awaiting worker confirmation', 409)
        if rental['deadline'] != expected_deadline:
            raise VastError('Rental deadline changed; review the current values', 409)
        intent = effective_intent(queue.store, worker_id, queue.store.read(worker_id, 'intent.json'))
        # Match the budget guard's deadline calculation before persisting a request.
        target = intent['accepted_at'] + min(86400, (budget - intent['transfer_reserve_usd'])
                                           / intent['offer']['price_hour_usd'] * 3600)
        if target < record['deadline']:
            raise VastError('Budget would end the rental before the current Loop deadline', 422)
        return request_budget(queue, worker_id, expected_budget, budget)


def reconcile_extension(store, queue, record):
    """Apply the Loop allowance only after the exact rental change is confirmed."""
    pending = record.get('rental_extension')
    if not pending:
        return record
    worker = queue.worker_for(pending['worker_id'])
    owned = worker and worker.get('loop_id') == record['id'] and worker.get('loop_owner') == record['owner']
    confirmed = (worker or {}).get('deadline_confirmed') or {}
    acknowledged = (owned and confirmed.get('expected') == pending['expected_deadline']
                    and confirmed.get('deadline') == pending['rental_deadline'])
    if owned and pending['rental_deadline'] == pending['expected_deadline']:
        intent = effective_intent(queue.store, worker['id'], queue.store.read(worker['id'], 'intent.json'))
        acknowledged = intent['deadline'] >= pending['loop_deadline'] and worker.get('rental_state') == 'active'
    if owned and worker.get('deadline_request') and not acknowledged:
        return record
    def apply(current):
        """Never revive stopped work or overwrite a newer Loop control change."""
        if current.get('rental_extension') != pending:
            return
        current.pop('rental_extension', None)
        if (acknowledged and current['status'] in {'running', 'paused'}
                and current['deadline'] == pending['expected_loop_deadline']):
            current['deadline'] = pending['loop_deadline']
            current['settings']['hours'] = max(current['settings']['hours'],
                                             (current['deadline'] - current['started_at']) / 3600)
            current['control_generation'] += 1
            current['rental_extension_error'] = None
        else:
            current['rental_extension_error'] = ((worker or {}).get('deadline_error')
                or 'Rental extension was not confirmed for this active loop; Loop deadline retained.')
    return store.update(record['owner'], record['id'], apply)


def extend_rental(store, queue, owner, loop_id, worker_id, expected_deadline, expected_loop_deadline, minutes):
    """Persist recoverable intent and request an extension within the existing budget."""
    owner, loop_id = identifier(owner), identifier(loop_id)
    identifier(worker_id)
    if type(minutes) is not int or not 1 <= minutes <= 1440:
        raise VastError('Choose an extension between 1 and 1,440 minutes', 422)
    # Match the controller's actual flock, not an advisory helper's .lock suffix.
    path = store.owner_dir(owner) / (loop_id + '.controller')
    fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise VastError('The loop is updating; retry the extension shortly', 409) from None
        with advisory_file_lock(queue.root / '.queue-lock'):
            record = store.read(owner, loop_id)
            rental = rental_snapshot(queue, record)
            if not rental or rental['worker_id'] != worker_id or not rental['can_extend']:
                raise VastError('This rental is not available for extension by this loop', 409)
            if rental['pending']:
                raise VastError('An extension is awaiting worker confirmation', 409)
            if rental['deadline'] != expected_deadline or record['deadline'] != expected_loop_deadline:
                raise VastError('Rental or Loop deadline changed; review the current values', 409)
            target = expected_loop_deadline + minutes * 60
            if target - record['started_at'] > 168 * 3600:
                raise VastError('Loop allowance cannot exceed 168 hours', 422)
            rental_minutes = max(0, math.ceil((target - expected_deadline) / 60))
            if rental_minutes > 1440:
                raise VastError('Required rental extension exceeds 1,440 minutes', 422)
            pending = dict(worker_id=worker_id, expected_deadline=expected_deadline,
                           rental_deadline=expected_deadline + rental_minutes * 60,
                           expected_loop_deadline=expected_loop_deadline, loop_deadline=target)
            def reserve(current):
                """Reserve against concurrent stop or deadline changes atomically."""
                if current['status'] not in {'running', 'paused'} or current['deadline'] != expected_loop_deadline:
                    raise VastError('Loop state changed; review it before extending', 409)
                current.update(rental_extension=pending, rental_extension_error=None)
            record = store.update(owner, loop_id, reserve)
            try:
                if rental_minutes:
                    request_deadline(queue, worker_id, expected_deadline, rental_minutes)
            except Exception:
                store.update(owner, loop_id, lambda row: row.pop('rental_extension', None))
                raise
            return reconcile_extension(store, queue, record)
    finally:
        os.close(fd)
