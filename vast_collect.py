"""Collect verified Pareto snapshots without owning optimizer or rental lifecycle."""
from contextlib import contextmanager, ExitStack
from pathlib import Path
from threading import Event, RLock
import asyncio
import fcntl
import os
import time

import psutil

from vast_jobs import job_id
from vast_provider import VastError
from vast_snapshot import bounded_lock, work_boundary

SERVICE = 'VastCollect'
_OWNER_LOCK = RLock()
_ACTIVE = {}
_STOPPING = False
TERMINAL = {'completed', 'cancelled', 'failed'}


def startup():
    """Reopen admission for this API lifespan."""
    global _STOPPING
    with _OWNER_LOCK:
        _STOPPING = False


async def shutdown():
    """Cancel and join only API-owned collection requests, never supervisors."""
    global _STOPPING
    with _OWNER_LOCK:
        _STOPPING = True
        active = list(_ACTIVE.values())
        for cancel, _done in active:
            cancel.set()
    await asyncio.gather(*(asyncio.to_thread(done.wait) for _cancel, done in active), return_exceptions=True)


@contextmanager
def owned_work():
    """Keep synchronous request work registered until all children are reaped."""
    cancel, done = Event(), Event()
    token = object()
    with _OWNER_LOCK:
        if _STOPPING:
            raise VastError('API is shutting down; collection was not started', 409)
        _ACTIVE[token] = (cancel, done)
    try:
        yield cancel
    finally:
        with _OWNER_LOCK:
            _ACTIVE.pop(token, None)
            done.set()


def writer_state(store, state, cache=None):
    """Identify the actual writer using the role lock, PID, start time and argv."""
    identifier = job_id(state['id'])
    owner_id = job_id(state.get('lease_id') or identifier)
    owner = store.read(owner_id)
    if owner_id != identifier and owner.get('active_job') != identifier:
        return 'inactive', owner
    record = owner.get('supervisor_run') or {}
    key = (owner_id, record.get('pid'), record.get('created_at'))
    if cache is not None and key in cache:
        return cache[key], owner
    # A free role lock proves no writer owns this job, even with an old record.
    role = store.directory(owner_id) / 'run.lock'
    try:
        fd = os.open(role, os.O_RDWR | os.O_NOFOLLOW)
    except FileNotFoundError:
        fd = None
    locked = False
    if fd is not None:
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                locked = True
        finally:
            os.close(fd)
    result = 'inactive'
    if locked:
        result = 'unknown'
        try:
            process = psutil.Process(record['pid'])
            args = process.cmdline()
            script = str(Path(__file__).with_name('vast_job_runner.py'))
            if (process.create_time() == record['created_at'] and len(args) == 4
                    and args[1:] == [script, 'run', owner_id]):
                result = 'compatible' if type(record.get('snapshot_transaction')) is int and record['snapshot_transaction'] == 1 else 'legacy'
        except (KeyError, TypeError, psutil.Error):
            pass
    if cache is not None:
        cache[key] = result
    return result, owner


def collection_reason(store, state, *, cache=None, ignore_quota=False):
    """Project local eligibility for the browser; the POST revalidates it."""
    if state.get('deleted_at'):
        return 'This cloud job has been deleted'
    if state.get('kind') in {'worker', 'calibration'}:
        return 'Only optimizer jobs have Pareto results'
    writer, owner = writer_state(store, state, cache)
    if state.get('final_collected'):
        return '' if writer in {'inactive', 'compatible'} else 'Restart the outdated Vast run supervisor before collecting this job'
    if (state.get('status') == 'collecting' or state.get('stop_rpc_at') or state.get('final_collection_at')):
        return 'Stop or final collection already owns this job'
    if not state.get('uploaded') or state.get('status') not in {'running', 'completed', 'failed', 'cancelled'}:
        return 'This optimizer has no running Pareto snapshot yet'
    if writer != 'compatible':
        return 'Restart the Vast run supervisor to enable safe Pareto collection'
    if owner.get('rental_state') != 'active' or state.get('rental_state') not in {'none', 'active'}:
        return 'The job instance is no longer available; previous results are retained'
    if not ignore_quota and int(state.get('downloaded_bytes', 0)) >= 1024**3:
        return 'Partial transfer allowance reached; final collection reserve retained'
    for identifier in {state['id'], owner['id']}:
        control = store.read(identifier, 'control.json')
        if control.get('stop') or control.get('cleanup'):
            return 'Stop or rental cleanup already requested; previous results are retained'
    return ''


def availability(store, state, cache=None):
    """Keep inventory failures local and never expose process details to the GUI."""
    try:
        reason = collection_reason(store, state, cache=cache)
    except (VastError, OSError, ValueError, TypeError, KeyError):
        reason = 'Collection status unavailable; job updates continue automatically'
    return {'collect_available': not reason, 'collect_unavailable_reason': reason}


@contextmanager
def admission(store, identifier, lease_id):
    """Acquire Queue→Finalize?→Snapshot in at most five seconds, then release Queue."""
    until = time.monotonic() + 5
    while True:
        locks = ExitStack()
        try:
            with bounded_lock(store.root / '.queue-lock', timeout=.01):
                state = store.read(identifier)
                if state.get('deleted_at') or (state.get('lease_id') or identifier) != lease_id:
                    raise VastError('Cloud job ownership changed; collection was not started', 409)
                if state.get('final_collected'):
                    locks.enter_context(bounded_lock(store.directory(identifier) / '.finalize-lock', timeout=.01))
                locks.enter_context(bounded_lock(store.directory(identifier) / '.snapshot-lock', timeout=.01))
                current = store.read(identifier)
                if bool(current.get('final_collected')) != bool(state.get('final_collected')):
                    locks.close()
                    continue
                if current.get('deleted_at') or (current.get('lease_id') or identifier) != lease_id:
                    raise VastError('Cloud job ownership changed; collection was not started', 409)
                reason = collection_reason(store, current)
                if reason:
                    raise VastError(reason, 409)
            break
        except VastError as exc:
            locks.close()
            if 'Collection is busy' not in str(exc) or time.monotonic() >= until:
                raise
            time.sleep(.025)
        except BaseException:
            locks.close()
            raise
    try:
        yield current
    finally:
        locks.close()


def collect_results(store, identifier):
    """Publish one current snapshot while preserving all lifecycle controls."""
    from vast_credentials import VastCredentialStore
    from vast_job_runner import validate_intent, owned_instance
    from vast_deadline import effective_intent
    from vast_provider import VastClient
    from vast_transfer import WorkerConnection, import_results

    identifier = job_id(identifier)
    initial = store.read(identifier)
    lease_id = job_id(initial.get('lease_id') or identifier)
    with owned_work() as cancel:
        with admission(store, identifier, lease_id) as state:
            partial = not state.get('final_collected')
            seconds = 90
            if partial:
                intent = validate_intent(store.read(lease_id, 'intent.json'), lease_id)
                intent = effective_intent(store, lease_id, intent)
                seconds = min(90, intent['deadline'] - time.time() - 180)
                if seconds <= 0:
                    raise VastError('Rental final collection reserve reached; previous results are retained', 409)
            deadline = time.monotonic() + seconds

            def remaining():
                """Cancel only manual collection, never the independent supervisor."""
                value = deadline - time.monotonic()
                if cancel.is_set():
                    raise VastError('Pareto collection cancelled during API shutdown; previous results are retained', 409)
                if value <= 0:
                    raise VastError('Pareto collection timed out; previous results are retained', 409)
                current = store.read(identifier)
                if (current.get('lease_id') or identifier) != lease_id or current.get('deleted_at'):
                    raise VastError('Cloud job ownership changed; collection cancelled', 409)
                if partial:
                    reason = collection_reason(store, current, ignore_quota=True)
                    if reason:
                        raise VastError(reason, 409)
                    current_intent = effective_intent(store, lease_id, intent)
                    value = min(value, current_intent['deadline'] - time.time() - 180)
                    if value <= 0:
                        raise VastError('Rental final collection reserve reached', 409)
                return value

            with work_boundary(remaining):
                remaining()
                if partial:
                    rental = store.read(lease_id)
                    client = VastClient(VastCredentialStore(store.root).secrets()['api_key'])
                    row = owned_instance(client, intent, fresh=True, expected_id=rental.get('instance_id'))
                    if row is None or row.get('id') != rental.get('instance_id') or row.get('actual_status') != 'running':
                        raise VastError('The owned job instance is not reachable or running', 409)
                    connection = WorkerConnection(store, identifier, row, lease_id=lease_id)
                    identity = store.directory(lease_id)
                    if not all((identity / name).is_file() and not (identity / name).is_symlink()
                               for name in ('ssh-key', 'known_hosts', 'ssh-attached.json')):
                        raise VastError('The verified worker connection is not ready; try after supervision reconnects', 409)
                    connection.collect(False, timeout=remaining())
                # Only local work remains. Acquire State before publication so
                # its matching metadata cannot encounter an unbounded wait after
                # the file commit. The nested store.update is reentrant; do not
                # add a cancellation point between that commit and its metadata.
                with bounded_lock(store.directory(identifier) / '.state-lock', check=remaining):
                    imported = import_results(store, identifier, partial=partial, check=remaining)
                    return store.update(identifier, **imported)
