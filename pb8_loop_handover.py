"""Durable owner-scoped Loop handover without resetting a paid rental's limits."""
from __future__ import annotations

from contextlib import contextmanager, ExitStack
import fcntl
import os
import time

from file_lock import advisory_file_lock
from pb8_loop_store import identifier
from vast_provider import VastError

SERVICE = 'PB8Loop'
FINISHED = {'completed', 'complete', 'failed', 'error', 'cancelled', 'stopped', 'skipped'}


@contextmanager
def controller_claims(loops, owner, *ids):
    """Exclude in-flight controller decisions rather than racing a paid launch."""
    with ExitStack() as stack:
        for loop_id in sorted(set(ids)):
            path = loops.path(identifier(owner), identifier(loop_id)).with_suffix('.controller')
            fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
            stack.callback(os.close, fd)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise VastError('A Loop step is in progress; retry the GPU handover shortly', 409) from exc
        yield


def source_worker(loops, queue, owner, source_id, *, now=None):
    """Validate the single owned live worker without mutating loops or rentals."""
    now = time.time() if now is None else now
    source = loops.read(identifier(owner), identifier(source_id))
    from pb8_loop_gpu_replacement import open_replacement
    if open_replacement(source):
        raise VastError('This Loop has an open GPU replacement; finish or stop it before a Loop handover', 409)
    if source['settings']['execution'] != 'vast' or source['status'] not in {'running', 'finishing', 'paused'}:
        raise VastError('Replacement requires an active or paused Vast Loop', 409)
    workers = [w for w in queue.workers() if w.get('loop_id') == source_id and w.get('loop_owner') == owner]
    if len(workers) != 1:
        raise VastError('Replacement requires exactly one owned live GPU rental', 409)
    worker = workers[0]
    control = queue.store.read(worker['id'], 'control.json')
    if (worker.get('rental_state') != 'active' or worker.get('deadline_request')
            or control.get('stop') or control.get('cleanup') or now >= worker.get('deadline', 0) - 300):
        raise VastError('The current GPU is ending or has insufficient remaining rental time', 409)
    if worker.get('loop_handover', {}).get('status') == 'collecting':
        raise VastError('The GPU already has a pending Loop handover', 409)
    return source, worker


def begin(loops, queue, owner, source_id, target_id, *, expected_worker=None, expected_generation=None):
    """Reserve the existing worker before stopping the old Loop; replay is idempotent."""
    owner, source_id, target_id = map(identifier, (owner, source_id, target_id))
    if source_id == target_id:
        raise VastError('Replacement needs a different queued Loop', 422)
    with controller_claims(loops, owner, source_id, target_id), advisory_file_lock(queue.root / '.queue-lock'):
        target = loops.read(owner, target_id)
        previous = target.get('rental_handover')
        if previous:
            if previous['source_id'] != source_id or (expected_worker and previous['worker_id'] != expected_worker):
                raise VastError('The replacement already belongs to another handover', 409)
            return previous
        # Recover a crash after worker reservation but before either loop write.
        reserved = [w for w in queue.workers() if w.get('loop_owner') == owner
                    and w.get('loop_handover', {}).get('source_id') == source_id
                    and w.get('loop_handover', {}).get('target_id') == target_id
                    and w['loop_handover'].get('status') == 'collecting']
        if reserved:
            if len(reserved) != 1:
                raise VastError('Ambiguous GPU handover reservation', 409)
            worker = reserved[0]
            source = loops.read(owner, source_id)
            handover = worker['loop_handover']
        else:
            source, worker = source_worker(loops, queue, owner, source_id)
            if expected_generation is not None and source['control_generation'] != expected_generation:
                raise VastError('Source Loop controls changed since review', 409)
            handover = {'source_id': source_id, 'target_id': target_id, 'worker_id': worker['id'],
                        'owner': owner, 'status': 'collecting', 'created_at': time.time()}
        if expected_worker and worker['id'] != expected_worker:
            raise VastError('The reviewed GPU rental changed', 409)
        if target['status'] != 'queued' or target['settings']['execution'] != 'vast':
            raise VastError('Replacement must be a queued Vast Loop', 409)
        if target['settings'].get('vast') != source['settings'].get('vast'):
            raise VastError('Replacement must preserve the reviewed Vast rental limits', 409)
        if target['initial_config']['backtest'].get('offline') is not True:
            raise VastError('Replacement requires offline inputs', 422)
        from vast_worker_compatibility import worker_compatibility_errors
        intent = queue.store.read(worker['id'], 'intent.json')
        errors = worker_compatibility_errors(target['initial_config'], intent['pb8_revision'], for_preparation=True)
        if errors:
            raise VastError('Replacement is incompatible with the existing worker: ' + errors[0]['message'], 422)
        # Write-ahead reservation blocks queue dispatch/idle cleanup even if an
        # API restart interrupts the following separate atomic loop writes.
        queue.store.update(worker['id'], loop_handover=handover, idle_since=None)
        loops.update(owner, target_id, lambda r: r.update(rental_handover=handover))
        def stopping(r):
            r.update(status='stopping', reason='Replacing Loop on the existing GPU',
                     cleanup_done=False, control_generation=r['control_generation'] + 1)
        loops.update(owner, source_id, stopping)
        return handover


def advance(loops, queue, target):
    """Reconcile collection and transfer after restarts; never renew budget or deadline."""
    handover = target['rental_handover']
    owner, source_id = target['owner'], handover['source_id']
    with advisory_file_lock(queue.root / '.queue-lock'):
        worker = queue.store.read(handover['worker_id'])
        target = loops.read(owner, target['id'])
        if worker.get('loop_owner') != owner:
            raise VastError('GPU handover ownership changed', 409)
        current = worker.get('loop_handover') or {}
        if any(current.get(k) != handover.get(k) for k in ('source_id', 'target_id', 'worker_id', 'owner')):
            raise VastError('GPU handover reservation changed', 409)
        if worker.get('loop_id') == target['id'] and current.get('status') == 'transferred':
            loops.update(owner, target['id'], lambda r: r.update(rental_handover=current,
                         deadline=min(r['deadline'], worker['deadline'] - 240)))
            return True
        if worker.get('loop_id') != source_id:
            raise VastError('GPU handover source ownership changed', 409)
        control = queue.store.read(worker['id'], 'control.json')
        if target['status'] in {'stopping', 'stopped', 'failed', 'completed', 'unconfirmed'}:
            queue.store.control(worker['id'], 'cleanup')
            loops.update(owner, target['id'], lambda r: r.update(rental_handover={**handover, 'status': 'cancelled'}))
            return False
        if (control.get('stop') or control.get('cleanup') or worker.get('deadline_request')
                or worker.get('rental_state') != 'active' or time.time() >= worker['deadline'] - 240):
            raise VastError('Existing rental ended or reached its limit; no replacement GPU will be rented', 409)
        source = loops.read(owner, source_id)
        # Reconcile an interrupted begin without relying on another HTTP call.
        if source['status'] not in {'stopping', 'stopped'}:
            loops.update(owner, source_id, lambda r: r.update(status='stopping', cleanup_done=False,
                         reason='Replacing Loop on the existing GPU', control_generation=r['control_generation'] + 1))
            return False
        jobs = source['jobs'] + source.get('observer_jobs', [])
        unfinished = any(j.get('status') not in FINISHED for j in jobs)
        native = [j for j in queue.store.list() if j.get('lease_id') == worker['id']
                  and j.get('kind') != 'worker' and j.get('status') not in FINISHED]
        if (source['status'] != 'stopped' or not source.get('cleanup_done') or unfinished or native
                or worker.get('active_job') or target['status'] != 'running'):
            return False
        transferred = {**handover, 'status': 'transferred', 'transferred_at': time.time()}
        # Rental intent, deadline, spend, transfer reserve and controls are untouched.
        queue.store.update(worker['id'], loop_id=target['id'], loop_handover=transferred, idle_since=None)
        loops.update(owner, target['id'], lambda r: r.update(rental_handover=transferred,
                     deadline=min(r['deadline'], worker['deadline'] - 240)))
        return True
