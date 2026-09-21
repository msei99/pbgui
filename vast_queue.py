"""Persistent shared Vast worker ownership for the PB8 optimizer cloud queue."""
from __future__ import annotations

import json
import time
import uuid
from pathlib import Path

from file_lock import advisory_file_lock
from secure_files import ensure_private_directory, read_regular_file_nofollow
from vast_jobs import JobStore, IMAGE, REVISION, TERMINAL, job_id, write_json
from vast_provider import VastError, positive_id

SERVICE = "VastRunner"


def blocked_machine_ids(state: dict) -> list[int]:
    """Validate persisted exclusions instead of silently ignoring corrupt state."""
    values = state.get('blocked_machine_ids', [])
    if not isinstance(values, list) or any(type(value) is not int or value <= 0 for value in values):
        raise VastError('Blocked GPU hosts cannot be read', 500)
    return sorted(set(values))


def offer_host_allowed(offer: dict, blocked: list[int]) -> bool:
    """Fail closed for unidentified machines when exclusions are active."""
    machine = offer.get('machine_id')
    return not blocked or (type(machine) is int and machine > 0 and machine not in blocked)


def can_remove_job(row: dict, worker: dict | None) -> bool:
    """Keep active work and unfinished standalone rental supervision reachable."""
    if row.get('kind') == 'worker' or (worker and worker.get('active_job') == row.get('id')):
        return False
    if row.get('status') not in TERMINAL | {'ready'}:
        return False
    if row.get('status') == 'ready' and row.get('lease_id'):
        return False
    return row.get('rental_state') in ('none', 'deletion_verified') or bool(row.get('lease_id'))


class CloudQueue:
    """Keep worker selection and queue controls separate from optimizer snapshots."""
    def __init__(self, store: JobStore | None = None):
        """Allow isolated storage in tests without import-time runtime reads."""
        self.store = store or JobStore()
        self.root = self.store.root

    def read(self) -> dict:
        """Read the private queue control record without following links."""
        path = self.root / 'queue.json'
        if not path.exists() and not path.is_symlink():
            return {'worker_id': None, 'selected_offer': None, 'paused': False, 'idle_seconds': -1}
        try:
            value = json.loads(read_regular_file_nofollow(path, self.root))
            if not isinstance(value, dict):
                raise ValueError('invalid')
            if value.get('worker_id'):
                job_id(value['worker_id'])
            return value
        except (OSError, ValueError, RuntimeError):
            raise VastError('Cloud queue state cannot be read', 500) from None

    def update(self, **changes) -> dict:
        """Merge queue controls with cross-process exclusion."""
        ensure_private_directory(self.root)
        with advisory_file_lock(self.root / '.queue-lock'):
            state = self.read()
            state.update(changes)
            write_json(self.root / 'queue.json', state)
            return state

    def worker(self) -> dict | None:
        """Return the current worker, including a verified closed rental."""
        identifier = self.read().get('worker_id')
        return self.store.read(identifier) if identifier else None

    def workers(self, *, active_only: bool = True) -> list[dict]:
        """Discover durable rentals, including legacy workers and pending cleanup."""
        return [row for row in self.store.list() if row.get('kind') == 'worker'
                and (not active_only or row.get('rental_state') not in ('none', 'deletion_verified'))]

    def worker_for(self, identifier: str | None) -> dict | None:
        """Resolve a job's actual rental instead of the last selected worker."""
        if not identifier:
            return self.worker()
        row = self.store.read(job_id(identifier))
        return row if row.get('kind') == 'worker' or self.read().get('worker_id') == identifier else None

    def set_host_block(self, machine_id: int, blocked: bool) -> list[int]:
        """Atomically change one exclusion without overwriting concurrent edits."""
        machine_id = positive_id(machine_id)
        if type(blocked) is not bool:
            raise VastError('Invalid host block action', 422)
        ensure_private_directory(self.root)
        with advisory_file_lock(self.root / '.queue-lock'):
            state = self.read()
            machines = set(blocked_machine_ids(state))
            if blocked:
                machines.add(machine_id)
            else:
                machines.discard(machine_id)
            state['blocked_machine_ids'] = sorted(machines)
            write_json(self.root / 'queue.json', state)
            return state['blocked_machine_ids']

    def waiting(self) -> list[dict]:
        """Use FIFO ordering for prepared cloud jobs, independently of local CPUs."""
        return sorted((row for row in self.store.list() if row.get('kind') != 'worker' and row['status'] == 'ready'),
                      key=lambda row: (row.get('created_at', 0), row['id']))

    def start(self, offer: dict, hours: float, budget: float, idle_seconds: int, *, manual: bool = False,
              pool_authorization_id: str | None = None) -> dict:
        """Rent once for the queue; duplicate starts return the existing worker."""
        if idle_seconds not in (-1, 0, 300, 1800, 3600):
            raise VastError('Choose deadline retention, immediate cleanup or an idle retention period', 422)
        ensure_private_directory(self.root)
        with advisory_file_lock(self.root / '.queue-lock'):
            current = self.worker()
            if pool_authorization_id is not None:
                from vast_pool import rental_needed
                if not rental_needed(self, pool_authorization_id):
                    raise VastError('GPU pool capacity or queue authorization changed', 409)
            elif current and current['rental_state'] not in ('none', 'deletion_verified'):
                return current
            if not offer_host_allowed(offer, blocked_machine_ids(self.read())):
                raise VastError('GPU host is blocked or its machine ID is unavailable; refresh offers', 409)
            jobs = self.waiting()
            if not jobs and not manual:
                raise VastError('Queue a cloud optimizer config first', 409)
            identifier = uuid.uuid4().hex
            directory = ensure_private_directory(self.root / 'jobs' / identifier)
            write_json(directory / 'state.json', {'id': identifier, 'kind': 'worker', 'status': 'ready',
                       'rental_state': 'none', 'workers': max((1 if row.get('auto_cpu_workers') else row['workers'] for row in jobs), default=1),
                       'created_at': time.time(), 'generation': 0, 'idle_seconds': idle_seconds, 'awaiting_queue_start': manual})
            write_json(directory / 'control.json', {'stop': False, 'cleanup': False})
            write_json(directory / 'intent.json', {'id': identifier, 'image': IMAGE, 'pb8_revision': REVISION,
                       'bundle_bytes': sum(row.get('input_bytes', 0) for row in jobs), 'job_count': len(jobs)})
            # Publish the worker before launching so recovery uses the same identity.
            state = self.read()
            state.update(worker_id=identifier, selected_offer=offer, paused=manual, idle_seconds=idle_seconds)
            write_json(self.root / 'queue.json', state)
            return self.store.start(identifier, offer, hours, budget)

    def remove_job(self, identifier: str) -> dict:
        """Remove a queue entry atomically while retaining snapshots and results."""
        identifier = job_id(identifier)
        with advisory_file_lock(self.root / '.queue-lock'):
            row = self.store.read(identifier)
            if row.get('deleted_at'):
                return {'id': identifier, 'deleted': True}
            if not can_remove_job(row, self.worker_for(row.get('lease_id'))):
                raise VastError('Stop the job and finish collection/cleanup before deleting it', 409)
            changes = {'deleted_at': time.time()}
            if row['status'] == 'ready':
                changes['status'] = 'cancelled'
            self.store.update(identifier, **changes)
            return {'id': identifier, 'deleted': True}

    def action(self, action: str) -> dict:
        """Control queue scheduling without implicitly creating a replacement GPU."""
        with advisory_file_lock(self.root / '.queue-lock'):
            if action not in {'pause', 'resume', 'end', 'recover'}:
                raise VastError('Invalid cloud queue action', 422)
            workers = self.workers()
            if action in {'pause', 'resume'}:
                self.update(paused=action == 'pause')
            if action == 'resume':
                # A manual Rent deliberately keeps the worker reserved.  Starting
                # the queue must always release that gate, even if a previous start
                # request had already changed only the queue's paused flag.
                for worker in workers:
                    self.store.update(worker['id'], awaiting_queue_start=False)
            if action == 'end':
                state = self.read()
                preferences = dict(state.get('gpu_preferences') or {})
                preferences['auto_rent'] = False
                self.update(paused=True, pool_enabled=False, pool_authorization=None,
                            pool_error=None, gpu_preferences=preferences)
                for worker in workers:
                    self.store.control(worker['id'], 'stop')
            if action in {'resume', 'recover', 'end'}:
                for worker in workers:
                    self.store.launch_service(worker['id'], 'guard')
                    self.store.launch_service(worker['id'], 'run')
            if action in {'resume', 'recover'} and self.read().get('pool_enabled'):
                from vast_pool import launch_pool
                launch_pool(self)
            return self.read()

    def worker_action(self, identifier: str, action: str) -> dict:
        """Start or release exactly one rental without changing unrelated GPUs."""
        identifier = job_id(identifier)
        if action not in {'start', 'end', 'replace'}:
            raise VastError('Invalid GPU action', 422)
        with advisory_file_lock(self.root / '.queue-lock'):
            worker = self.worker_for(identifier)
            if not worker or worker.get('rental_state') in ('none', 'deletion_verified'):
                raise VastError('GPU rental is no longer active', 409)
            state = self.read()
            if action == 'start':
                self.store.update(identifier, awaiting_queue_start=False, idle_since=None)
                # Legacy manual rental used the pool-wide pause as its start gate.
                if worker.get('awaiting_queue_start') and len(self.workers()) == 1:
                    self.update(paused=False)
            else:
                replacing = action == 'replace'
                preferences = state.get('gpu_preferences') or {}
                if replacing and not (state.get('pool_enabled') and preferences.get('auto_rent')):
                    raise VastError('Enable Auto rent & start before replacing a GPU automatically', 409)
                active_job = worker.get('active_job')
                self.store.update(identifier, replacement_requested=replacing,
                                  replacement_job=active_job if replacing else None)
                self.store.control(identifier, 'stop')
            self.store.launch_service(identifier, 'guard')
            self.store.launch_service(identifier, 'run')
            if action == 'replace' and state.get('pool_enabled'):
                from vast_pool import launch_pool
                launch_pool(self)
            return self.store.read(identifier)


def worker_step(queue: CloudQueue, identifier: str, *, now: float | None = None) -> str | None:
    """Claim the next compatible job or request idle/deadline cleanup atomically."""
    now = time.time() if now is None else now
    store = queue.store
    with advisory_file_lock(queue.root / '.queue-lock'):
        worker = store.read(identifier)
        control = store.read(identifier, 'control.json')
        if worker['rental_state'] == 'deletion_verified':
            missing = worker.get('rental_end_reason') == 'provider_instance_missing'
            replacement_job = worker.get('replacement_job') if worker.get('replacement_requested') else None
            if replacement_job:
                replacement = store.read(job_id(replacement_job))
                if replacement.get('lease_id') == identifier and not replacement.get('final_collected'):
                    try:
                        store.reset_unstarted_for_replacement(replacement_job, identifier)
                    except VastError:
                        # Any existing result remains available and requires an explicit requeue.
                        store.update(replacement_job, error='GPU replaced; existing optimizer progress was preserved. Requeue explicitly to start a fresh run.')
            for job in store.list():
                if job.get('lease_id') != identifier or job['status'] in TERMINAL or job['status'] == 'ready':
                    continue
                stopped = control['stop'] or store.read(job['id'], 'control.json')['stop']
                message = ('Raw results saved locally; result import needs retry' if job.get('final_collected') else
                           'Vast.ai instance disappeared; the last local snapshot remains available' if missing else
                           'Rental ended; the last local snapshot remains available')
                store.update(job['id'], status='cancelled' if stopped and not job.get('final_collected') else 'failed', error=message)
            if missing and not queue.read().get('pool_enabled'):
                queue.update(paused=True)
            store.update(identifier, status='failed' if missing else 'completed', active_job=None,
                         replacement_requested=False, replacement_job=None)
            return None
        active = worker.get('active_job')
        if not active:
            claimed = [row for row in store.list() if row.get('lease_id') == identifier and row['status'] not in TERMINAL and row['status'] != 'ready']
            if len(claimed) > 1:
                raise VastError('Multiple unfinished jobs claim this worker; inspection required', 409)
            if claimed:
                active = claimed[0]['id']
                store.update(identifier, active_job=active)
        if active and store.read(job_id(active))['status'] not in TERMINAL:
            return active
        if active:
            store.update(identifier, active_job=None)
        if control['stop'] or control['cleanup'] or now >= worker['deadline'] - 240:
            store.control(identifier, 'cleanup')
            return None
        state = queue.read()
        candidates = [] if state.get('paused') or worker.get('deadline_request') else queue.waiting()
        if candidates:
            from vast_deadline import effective_intent
            intent = effective_intent(store, identifier, store.read(identifier, 'intent.json'))
            for candidate in candidates:
                if not candidate.get('auto_cpu_workers') and candidate['workers'] > worker.get('allocated_cpus', worker['workers']):
                    store.update(candidate['id'], error='Waiting for a worker with enough allocated CPU cores')
                    continue
                expected = (2 * candidate.get('input_bytes', 0) / 1e9 * intent['offer']['download_gb_usd']
                            + 3 * intent['offer']['upload_gb_usd'])
                used = worker.get('transfer_reserved_used', 0)
                if used + expected > intent['transfer_reserve_usd'] + 1e-9:
                    from vast_deadline import request_transfer_reserve
                    target = max(.05, used + expected)
                    previous = worker.get('auto_reserve_target')
                    if worker.get('deadline_protocol') == 2 and not (worker.get('deadline_error') and previous == target):
                        try:
                            request_transfer_reserve(queue, identifier, intent['transfer_reserve_usd'], target)
                        except VastError as exc:
                            from logging_helpers import human_log
                            error = 'Waiting: transfer reserve cannot be increased: ' + str(exc)
                            if candidate.get('error') != error:
                                human_log(SERVICE, error, level='WARNING')
                            store.update(candidate['id'], error=error)
                        else:
                            store.update(candidate['id'], error='Waiting for worker confirmation of transfer reserve; rental time will shorten within the same budget')
                            store.update(identifier, auto_reserve_target=target, idle_since=None)
                            return None
                    else:
                        store.update(candidate['id'], error='Waiting: insufficient transfer reserve. Adjust Transfer reserve/Budget in the rental card; this worker cannot confirm an automatic adjustment.')
                    continue
                from vast_convergence import DEFAULTS
                preferences = state.get('gpu_preferences', {})
                convergence_config = {key: preferences.get(key, value) for key, value in DEFAULTS.items()}
                store.update(candidate['id'], lease_id=identifier, dispatch_at=now, status='provisioning', error=None,
                             convergence_config=convergence_config, setup_started_at=None)
                store.update(identifier, active_job=candidate['id'], idle_since=None,
                             transfer_reserved_used=used + expected, status='provisioning', awaiting_queue_start=False)
                return candidate['id']
        if worker.get('deadline_request'):
            return None
        if worker.get('awaiting_queue_start'):
            store.update(identifier, status='reserved', idle_since=None)
            return None
        idle_since = worker.get('idle_since')
        if idle_since is None:
            idle_since = now
        store.update(identifier, status='paused' if state.get('paused') else 'idle', idle_since=idle_since)
        idle_seconds = worker.get('idle_seconds', -1)
        if idle_seconds >= 0 and now - idle_since >= idle_seconds:
            store.control(identifier, 'cleanup')
        return None


def worker_loop(store: JobStore, identifier: str) -> None:
    """Reuse one supervised instance across sequential isolated optimizer jobs."""
    from vast_job_runner import run_loop
    queue = CloudQueue(store)
    last_idle_observation = 0.0
    while True:
        next_job = worker_step(queue, identifier)
        worker = store.read(identifier)
        if worker['rental_state'] == 'deletion_verified' or store.read(identifier, 'control.json')['cleanup']:
            return
        if next_job:
            run_loop(store, next_job, lease_id=identifier)
            result = store.read(next_job)
            if result['status'] == 'failed' and not result.get('final_collected'):
                queue.update(paused=True)
        else:
            if worker.get('instance_id') and time.time() - last_idle_observation >= 15:
                from vast_credentials import VastCredentialStore
                from vast_job_runner import owned_instance
                from vast_provider import VastClient
                from vast_runtime_metrics import observe_optimizer, sample_metrics
                from vast_transfer import WorkerConnection
                try:
                    intent = store.read(identifier, 'intent.json')
                    client = VastClient(VastCredentialStore(store.root).secrets()['api_key'])
                    row = owned_instance(client, intent)
                    if row is not None and row.get('actual_status') == 'running':
                        connection = WorkerConnection(store, identifier, row)
                        connection.bootstrap(client)
                        observation = observe_optimizer(connection, store, identifier)
                        if observation is not None:
                            sample_metrics(connection, store, identifier)
                except Exception:
                    import traceback
                    from logging_helpers import human_log
                    human_log(SERVICE, 'Idle GPU optimizer observation temporarily unavailable', level='WARNING',
                              meta={'traceback': traceback.format_exc()})
                last_idle_observation = time.time()
            if worker.get('instance_id') and worker.get('awaiting_queue_start'):
                from vast_credentials import VastCredentialStore
                from vast_provider import VastClient
                from vast_provisioning_log import collect_provisioning_log
                from logging_helpers import human_log
                try:
                    client = VastClient(VastCredentialStore(store.root).secrets()['api_key'])
                    collect_provisioning_log(store, identifier, client, worker['instance_id'])
                except Exception:
                    human_log(SERVICE, 'Reserved GPU provisioning log temporarily unavailable', level='WARNING')
            time.sleep(5)
