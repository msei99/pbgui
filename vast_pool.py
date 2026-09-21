"""Durable bounded GPU-pool scheduling; each rental keeps its own guards and budget."""
from __future__ import annotations

import fcntl
import os
import signal
import subprocess
import sys
import time
import uuid
from threading import Event

from file_lock import advisory_file_lock
from logging_helpers import human_log as _log
from secure_files import ensure_private_directory
from vast_credentials import VastCredentialStore
from vast_hosts import offer_priority, search_host_offers
from vast_jobs import PROJECT, TERMINAL, job_id, services_available
from vast_provider import VastClient, VastError, gpu_name_matches
from vast_queue import CloudQueue, blocked_machine_ids, offer_host_allowed

SERVICE = 'VastPool'
MAX_RENTALS = 16


def pool_limit(state: dict) -> int:
    """Validate the persisted authorization instead of widening a damaged limit."""
    authorization = state.get('pool_authorization') or {}
    settings = authorization.get('settings') or {}
    limit = settings.get('max_rentals', 1)
    if type(limit) is not int or not 1 <= limit <= MAX_RENTALS:
        raise VastError('Invalid GPU pool limit; save settings and start the queue again', 422)
    return limit


def rental_needed(queue: CloudQueue, authorization_id: str) -> bool:
    """Count starting/closing leases and idle slots before authorizing more capacity."""
    state = queue.read()
    authorization = state.get('pool_authorization') or {}
    if not state.get('pool_enabled') or state.get('paused') or authorization.get('id') != authorization_id:
        return False
    rows = queue.store.list()
    active = [r for r in rows if r.get('rental_state') not in ('none', 'deletion_verified')]
    if len(active) >= pool_limit(state):
        return False
    waiting = queue.waiting()
    if not waiting:
        return False
    unclaimed = list(waiting)
    for worker in active:
        if worker.get('kind') != 'worker':
            continue
        control = queue.store.read(worker['id'], 'control.json')
        if control.get('stop') or control.get('cleanup') or worker.get('rental_state') == 'destroy_pending':
            continue
        if worker.get('deadline', 0) <= time.time() + 240:
            continue
        busy = any(row.get('lease_id') == worker['id'] and row.get('status') not in TERMINAL
                   and row.get('status') != 'ready' for row in rows)
        if not busy:
            cores = worker.get('allocated_cpus', worker.get('workers', 1))
            candidate = next((row for row in unclaimed
                              if row.get('auto_cpu_workers') or row.get('workers', 1) <= cores), None)
            if candidate is not None:
                unclaimed.remove(candidate)
    return bool(unclaimed)


def select_offer(queue: CloudQueue, settings: dict) -> dict:
    """Recheck fresh provider offers against saved requirements and active exclusions."""
    keys = ('gpu_name', 'max_price', 'min_vram', 'min_ram', 'min_cpu', 'min_tflops', 'disk_gb', 'verified_only')
    preferences = {k: settings[k] for k in keys}
    waiting = queue.waiting()
    preferences['min_cpu'] = max(preferences['min_cpu'], max(
        (1 if row.get('auto_cpu_workers') else row['workers'] for row in waiting), default=1))
    state = queue.read()
    blocked = blocked_machine_ids(state)
    active_offers = {r.get('offer_id') for r in queue.workers()}
    client = VastClient(VastCredentialStore(queue.root).secrets()['api_key'])
    rows = search_host_offers(client, state, **preferences, min_cuda=13,
                             min_duration=settings['hours'] * 3600, excluded_machine_ids=blocked)
    matches = [r for r in rows if r.get('id') not in active_offers and offer_host_allowed(r, blocked)
               and gpu_name_matches(r.get('gpu_name', ''), preferences['gpu_name'])
               and r.get('num_gpus') == 1 and (r.get('cuda_max_good') or 0) >= 13
               and 0 < (r.get('price_hour_usd') or 0) <= preferences['max_price']
               and (r.get('vram_gb') or 0) >= preferences['min_vram']
               and (r.get('ram_gb') or 0) >= preferences['min_ram']
               and (r.get('cpu_cores') or 0) >= preferences['min_cpu']
               and (r.get('tflops') or 0) >= preferences['min_tflops']
               and (r.get('disk_gb') or 0) >= preferences['disk_gb']
               and (not preferences['verified_only'] or r.get('verified') is True)]
    if not matches:
        raise VastError('No available GPU matches the pool requirements; queued jobs are waiting', 409)
    return min(matches, key=lambda row: offer_priority(row, state))


def pool_step(queue: CloudQueue) -> dict | None:
    """Create at most one lease per pass; serialize concurrent scheduler instances."""
    ensure_private_directory(queue.root)
    with advisory_file_lock(queue.root / '.pool-tick-lock'):
        state = queue.read()
        authorization = state.get('pool_authorization') or {}
        identifier = authorization.get('id')
        if not identifier or not rental_needed(queue, job_id(identifier)):
            return None
        settings = authorization['settings']
        offer = select_offer(queue, settings)
        # start rechecks authorization, queue state and available slots under .queue-lock.
        result = queue.start(offer, settings['hours'], settings['budget'], settings['idle_seconds'],
                             pool_authorization_id=identifier)
        queue.update(pool_error=None)
        return result


def authorize_pool(queue: CloudQueue, settings: dict, *, resume: bool = True) -> dict:
    """Record explicit start consent without changing existing rental limits."""
    if not services_available():
        raise VastError('A working user systemd service manager and OpenSSH are required', 409)
    credentials = VastCredentialStore(queue.root).metadata()
    if not credentials['configured']:
        raise VastError('Save a Vast API key first', 409)
    authorization = {'id': uuid.uuid4().hex, 'settings': dict(settings), 'accepted_at': time.time(),
                     'credential_generation': credentials['generation']}
    pool_limit({'pool_authorization': authorization})
    ensure_private_directory(queue.root)
    with advisory_file_lock(queue.root / '.queue-lock'):
        previous = queue.read()
        paused = False if resume else bool(previous.get('paused'))
        queue.update(pool_authorization=authorization, pool_enabled=True, pool_error=None, paused=paused)
        try:
            if resume:
                queue.action('resume')
            else:
                launch_pool(queue)
        except Exception:
            queue.update(pool_authorization=previous.get('pool_authorization'),
                         pool_enabled=bool(previous.get('pool_enabled')),
                         pool_error=previous.get('pool_error'), paused=bool(previous.get('paused')))
            raise
    return {'pool_enabled': True, 'max_rentals': settings['max_rentals']}


def launch_pool(queue: CloudQueue) -> None:
    """Run one independent persistent scheduler; API restart does not kill rentals."""
    ensure_private_directory(queue.root)
    unit = 'pbgui-vast-pool'
    log_root = ensure_private_directory(PROJECT / 'data/logs/vast')
    with advisory_file_lock(queue.root / '.pool-launch-lock'):
        try:
            active = subprocess.run(['systemctl', '--user', 'is-active', '--quiet', unit],
                                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=5)
            if active.returncode == 0:
                if queue.read().get('pool_error') is not None:
                    queue.update(pool_error=None)
                return
            result = subprocess.run(['systemd-run', '--user', '--collect', '--unit', unit,
                '--property=Restart=on-failure', '--property=RestartSec=10', '--property=UMask=0077',
                '--property=StandardOutput=append:' + str(log_root / 'pool.log'),
                '--property=StandardError=inherit', '--working-directory=' + str(PROJECT),
                sys.executable, str(PROJECT / 'vast_pool.py')],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10)
        except (OSError, subprocess.TimeoutExpired):
            raise VastError('GPU pool supervisor could not be started', 503) from None
        if result.returncode:
            raise VastError('GPU pool supervisor could not be started', 503)
        if queue.read().get('pool_error') is not None:
            queue.update(pool_error=None)


def main() -> None:
    """Own the scheduler lock and terminate promptly on systemd shutdown signals."""
    from credential_process_registry import ProcessCapabilityHeartbeat

    os.umask(0o077)
    queue = CloudQueue()
    ensure_private_directory(queue.root)
    stopping = Event()
    signal.signal(signal.SIGTERM, lambda *_: stopping.set())
    signal.signal(signal.SIGINT, lambda *_: stopping.set())
    with ProcessCapabilityHeartbeat(PROJECT, SERVICE):
        with os.fdopen(os.open(queue.root / 'pool.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600), 'r+') as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return
            while not stopping.is_set():
                delay = 5
                try:
                    pool_step(queue)
                except Exception as exc:
                    message = str(exc) if isinstance(exc, VastError) else 'GPU pool scheduling failed; automatic retry pending'
                    _log(SERVICE, message, level='WARNING')
                    fatal = not isinstance(exc, VastError) or exc.status in (400, 401, 403, 404, 422)
                    queue.update(pool_error=message, **({'paused': True} if fatal else {}))
                    delay = 60
                stopping.wait(delay)


if __name__ == '__main__':
    main()
