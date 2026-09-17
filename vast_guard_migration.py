"""Controller-side upgrade of a legacy rental guard after an explicit deadline request."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import shlex
import tempfile
import threading
import time

from logging_helpers import human_log as _log
from vast_provider import VastError

SERVICE = 'VastRunner'
_ACTIVE_LOCK = threading.RLock()
_ACTIVE_OPERATIONS = 0


def restart_block_reason():
    """Keep API-owned guard handovers alive until acknowledgement or rollback."""
    with _ACTIVE_LOCK:
        return 'Vast deadline guard upgrade is in progress; wait for confirmation' if _ACTIVE_OPERATIONS else ''


def apply_migrated_deadline(queue, identifier, intent, request):
    """Own the synchronous handover and recheck cleanup before submitting changes."""
    global _ACTIVE_OPERATIONS
    with _ACTIVE_LOCK:
        _ACTIVE_OPERATIONS += 1
    try:
        connection = upgrade_guard(queue.store, identifier, intent)
        worker = queue.worker()
        control = queue.store.read(identifier, 'control.json')
        if (not worker or worker['id'] != identifier or worker.get('rental_state') != 'active'
                or control.get('stop') or control.get('cleanup')):
            raise VastError('Rental cleanup has been requested; deadline was not changed', 409)
        if min(request['expected'], request['deadline']) < time.time() + 600:
            raise VastError('Keep at least 10 minutes for safe collection and cleanup; deadline was not changed', 409)
        queue.store.update(identifier, deadline_request=request, deadline_error=None)
        confirmed = confirm_migrated_request(queue.store, identifier, connection, request)
        return {'pending': not confirmed, 'deadline': request['deadline'] if confirmed else request['expected'],
                'requested_deadline': request['deadline']}
    finally:
        with _ACTIVE_LOCK:
            _ACTIVE_OPERATIONS -= 1


def upgrade_guard(store, identifier, intent):
    """Upgrade only the independently owned guard, retaining the confirmed deadline."""
    from vast_credentials import VastCredentialStore
    from vast_job_runner import owned_instance
    from vast_provider import VastClient
    from vast_transfer import WorkerConnection

    directory = store.directory(identifier)
    if not (directory / 'known_hosts').is_file() or not (directory / 'ssh-key').is_file():
        raise VastError('Worker SSH setup is not ready for the deadline guard upgrade; retry after startup', 409)
    client = VastClient(VastCredentialStore(store.root).secrets()['api_key'])
    row = owned_instance(client, intent, fresh=True)
    if row is None:
        raise VastError('Rental is no longer available for a deadline guard upgrade', 409)
    connection = WorkerConnection(store, identifier, row)
    root = Path(__file__).resolve().parent / 'setup/vast_gpu_benchmark'
    source = (root / 'cloud_worker.py').read_text()
    request = dict(job_id=identifier, instance_id=row['id'], expected=intent['deadline'],
                   hard_deadline=intent['accepted_at'] + 86400,
                   source=source, sha256=hashlib.sha256(source.encode()).hexdigest())
    try:
        with tempfile.TemporaryFile() as stream:
            stream.write(json.dumps(request).encode()); stream.seek(0)
            raw = connection.command('/usr/local/bin/python -c ' + shlex.quote((root / 'guard_migration.py').read_text()),
                                     stdin=stream, timeout=40, max_output=8192)
        response = json.loads(raw)
        guard = response.get('guard', {})
        if (response.get('ok') is not True or guard.get('job_id') != identifier
                or guard.get('instance_id') != row['id'] or guard.get('deadline_protocol') != 2
                or guard.get('deadline') != intent['deadline'] or type(guard.get('guard_pid')) is not int):
            raise ValueError('Invalid guard migration acknowledgement')
        store.update(identifier, deadline_protocol=2, deadline_guard_upgraded_at=time.time())
        _log(SERVICE, 'Deadline guard upgraded without changing the rental deadline', level='INFO')
        return connection
    except Exception as exc:
        _log(SERVICE, 'Deadline guard upgrade could not be confirmed: ' + type(exc).__name__, level='WARNING')
        raise VastError('Deadline guard upgrade could not be confirmed. The optimizer was not restarted and the previous local deadline remains active. Retry the adjustment after refreshing.', 409) from None


def confirm_migrated_request(store, identifier, connection, request):
    """Deliver and confirm directly so an older local supervisor cannot reject protocol 2."""
    rejected = False
    try:
        with tempfile.TemporaryFile() as stream:
            stream.write(json.dumps(request).encode()); stream.seek(0)
            connection.command('umask 077; cat > /work/pbgui/deadline-request.tmp && mv /work/pbgui/deadline-request.tmp /work/pbgui/deadline-request.json',
                               stdin=stream, timeout=10)
        until = time.monotonic() + 8
        while time.monotonic() < until:
            guard = json.loads(connection.command('cat /work/pbgui/guard.json', timeout=5, max_output=8192))
            if guard.get('job_id') != identifier or guard.get('instance_id') != connection.row['id']:
                raise ValueError('Guard identity mismatch')
            if guard.get('request_id') == request['id'] and guard.get('deadline') == request['deadline']:
                store.update(identifier, deadline_confirmed=request, deadline=request['deadline'],
                             deadline_request=None, deadline_error=None)
                return True
            if guard.get('rejected_request_id') == request['id']:
                store.update(identifier, deadline_request=None, deadline_error='Worker rejected the deadline change; previous deadline retained.')
                rejected = True
                break
            time.sleep(.25)
    except Exception as exc:
        _log(SERVICE, 'Upgraded guard deadline acknowledgement unavailable: ' + type(exc).__name__, level='WARNING')
    if rejected:
        raise VastError('The upgraded worker rejected the deadline change; previous deadline retained', 409)
    # Retain the same request ID for reconciliation after an interrupted acknowledgement.
    return False
