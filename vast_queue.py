"""Persistent shared Vast worker ownership for the PB8 optimizer cloud queue."""
from __future__ import annotations

import json
import os
import re
import shutil
import time
import uuid
from pathlib import Path

from file_lock import advisory_file_lock
from secure_files import ensure_private_directory, read_regular_file_nofollow
from vast_jobs import JobStore, IMAGE, PROJECT, REVISION, TERMINAL, job_id, write_json
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


def preflight_local_metadata(store: JobStore, jobs: list[dict]) -> None:
    """Reject a paid rental if its local market or inception cache is incomplete."""
    from vast_inception import load_local_inception, required_markets
    from vast_market_cache import _load_local_markets

    requirements = []
    for row in jobs:
        manifest = store.read(row['id'], 'input/manifest.json')
        required = required_markets(manifest, PROJECT / 'data/coindata')
        if set(required) != set(row.get('exchanges') or []):
            raise VastError('Prepared cloud exchange list changed; prepare the job again', 409)
        cache_times = manifest.get('public_market_cache_mtimes') or {}
        if cache_times:
            if (not isinstance(cache_times, dict) or set(cache_times) != set(required)
                    or any(type(stamp) not in (int, float) or not 0 <= time.time() - stamp < 86400
                           for stamp in cache_times.values())):
                raise VastError('Prepared market cache is invalid or expired; prepare fresh input before renting', 422)
        requirements.append(required)
    snapshots = _load_local_markets(sorted({exchange for required in requirements for exchange in required}))
    for required in requirements:
        for exchange, coins in required.items():
            available = snapshots[exchange]['markets']
            for coin, symbol in coins.items():
                if symbol not in available:
                    raise VastError(
                        f'Local market metadata is missing {exchange}/{coin}; wait for the Market Data update', 422,
                    )
        load_local_inception(required)




def validated_rental_job_profiles(profiles: dict | None, allowed_ids: set[str]) -> dict:
    """Bound and validate job-specific manual GPU overrides against the frozen waiting queue."""
    from vast_gpu_tuning import _validated_rental_gpu_profile
    if profiles is None:
        return {}
    if not isinstance(profiles, dict) or len(profiles) > 100:
        raise VastError('Invalid job GPU profile selection', 422)
    result = {}
    for identifier, profile in profiles.items():
        identifier = job_id(identifier)
        if identifier not in allowed_ids:
            raise VastError('A selected GPU profile job is no longer waiting in the queue', 409)
        if profile is None:
            raise VastError('Invalid job GPU profile selection', 422)
        try:
            result[identifier] = _validated_rental_gpu_profile(profile)
        except ValueError:
            raise VastError('Invalid job GPU profile selection', 422) from None
    return result

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
        return sorted((row for row in self.store.list() if row.get('kind') not in {'worker', 'calibration'} and row['status'] == 'ready'),
                      key=lambda row: (row.get('created_at', 0), row['id']))

    def start(self, offer: dict, hours: float, budget: float, idle_seconds: int, *, manual: bool = False,
              pool_authorization_id: str | None = None, calibration_id: str | None = None,
              calibration_watch_id: str | None = None, rental_gpu_profile: dict | None = None,
              rental_job_gpu_profiles: dict | None = None) -> dict:
        """Rent once for the queue; duplicate starts return the existing worker."""
        if idle_seconds not in (-1, 0, 300, 1800, 3600):
            raise VastError('Choose deadline retention, immediate cleanup or an idle retention period', 422)
        if rental_gpu_profile is not None:
            if not manual:
                raise VastError('Rental GPU overrides require a manual rental', 422)
            from vast_gpu_tuning import _validated_rental_gpu_profile
            try:
                rental_gpu_profile = _validated_rental_gpu_profile(rental_gpu_profile)
            except ValueError:
                raise VastError('Invalid rental GPU profile', 422) from None
        if rental_job_gpu_profiles and not manual:
            raise VastError('Job GPU profile selections require a manual rental', 422)
        ensure_private_directory(self.root)
        with advisory_file_lock(self.root / '.queue-lock'):
            watch_record = self.read().get('calibration_watch')
            if watch_record is not None:
                from vast_calibration_watch import offer_matches, validated_watch
                watch = validated_watch(self)
                if calibration_watch_id != watch['id'] or calibration_id != watch['job_id']:
                    raise VastError('Waiting performance-test authorization changed; cancel it before another rental', 409)
                from vast_credentials import VastCredentialStore
                if VastCredentialStore(self.root).metadata()['generation'] != watch['credential_generation']:
                    raise VastError('Vast credentials changed; authorize the waiting test again', 409)
                if self.workers() or not offer_matches(watch, offer, blocked_machine_ids(self.read())):
                    raise VastError('Selected GPU no longer matches the waiting test', 409)
            elif calibration_watch_id is not None:
                raise VastError('Waiting performance-test authorization was cancelled', 409)
            current = self.worker()
            if pool_authorization_id is not None:
                from vast_pool import rental_needed
                if not rental_needed(self, pool_authorization_id):
                    raise VastError('GPU pool capacity or queue authorization changed', 409)
            elif current and current['rental_state'] not in ('none', 'deletion_verified'):
                return current
            if not offer_host_allowed(offer, blocked_machine_ids(self.read())):
                raise VastError('GPU host is blocked or its machine ID is unavailable; refresh offers', 409)
            if calibration_id is not None:
                calibration_id = job_id(calibration_id)
                calibration = self.store.read(calibration_id)
                if calibration.get('kind') != 'calibration' or calibration.get('status') != 'ready':
                    raise VastError('GPU calibration is no longer ready to start', 409)
                jobs = [calibration]
            else:
                jobs = self.waiting()
            job_profiles = validated_rental_job_profiles(
                rental_job_gpu_profiles, {row['id'] for row in jobs if row.get('kind') != 'calibration'},
            )
            if not jobs and not manual:
                raise VastError('Queue a cloud optimizer config first', 409)
            if jobs:
                preflight_local_metadata(self.store, jobs)
            identifier = uuid.uuid4().hex
            directory = ensure_private_directory(self.root / 'jobs' / identifier)
            write_json(directory / 'state.json', {'id': identifier, 'kind': 'worker', 'status': 'ready',
                       'rental_state': 'none', 'workers': max((1 if row.get('auto_cpu_workers') else row['workers'] for row in jobs), default=1),
                       'created_at': time.time(), 'generation': 0, 'idle_seconds': idle_seconds,
                       'awaiting_queue_start': manual, 'rental_gpu_profile': rental_gpu_profile,
                       'rental_job_gpu_profiles': job_profiles,
                       **({'calibration_job_id': calibration_id} if calibration_id else {})})
            write_json(directory / 'control.json', {'stop': False, 'cleanup': False})
            write_json(directory / 'intent.json', {'id': identifier, 'image': IMAGE, 'pb8_revision': REVISION,
                       'bundle_bytes': sum(row.get('input_bytes', 0) for row in jobs), 'job_count': len(jobs),
                       'rental_gpu_profile': rental_gpu_profile, 'rental_job_gpu_profiles': job_profiles})
            # Publish the worker before launching so recovery uses the same identity.
            state = self.read()
            state.update(worker_id=identifier, selected_offer=offer, paused=manual, idle_seconds=idle_seconds)
            write_json(self.root / 'queue.json', state)
            # Starting the local supervisor is not proof that Vast accepted a
            # rental. Keep a waiting calibration authorized until the worker
            # reports a real contract or a verified absence permits retry.
            return self.store.start(identifier, offer, hours, budget)

    def set_rental_gpu_profile(self, identifier: str, profile: dict | None,
                               job_profiles: dict | None = None) -> dict:
        """Update a reserved manual rental before it claims any queue job."""
        from vast_gpu_tuning import _validated_rental_gpu_profile
        identifier = job_id(identifier)
        try:
            profile = _validated_rental_gpu_profile(profile)
        except ValueError:
            raise VastError('Invalid rental GPU profile', 422) from None
        with advisory_file_lock(self.root / '.queue-lock'):
            worker = self.worker_for(identifier)
            if (not worker or worker.get('kind') != 'worker'
                    or worker.get('rental_state') in ('none', 'deletion_verified')
                    or not worker.get('awaiting_queue_start') or worker.get('active_job')):
                raise VastError('GPU profile can change only before this rental starts a job', 409)
            control = self.store.read(identifier, 'control.json')
            if control.get('stop') or control.get('cleanup'):
                raise VastError('GPU rental is stopping', 409)
            intent = self.store.read(identifier, 'intent.json')
            if not intent.get('offer'):
                raise VastError('Wait for Vast to confirm this rental', 409)
            if job_profiles is not None:
                selected = validated_rental_job_profiles(
                    job_profiles, {row['id'] for row in self.waiting()},
                )
                intent['rental_job_gpu_profiles'] = selected
            intent['rental_gpu_profile'] = profile
            write_json(self.store.directory(identifier) / 'intent.json', intent)
            return self.store.update(identifier, rental_gpu_profile=profile,
                                     rental_job_gpu_profiles=intent.get('rental_job_gpu_profiles', {}))

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

    def purge_job_history(self, identifier: str) -> dict:
        """Permanently remove an inactive job's complete retry lineage and history."""
        result = self.purge_job_histories([identifier])
        return {'id': result['requested_ids'][0], 'deleted': True,
                'purged_ids': result['purged_ids']}

    def purge_job_histories(self, identifiers: list[str]) -> dict:
        """Permanently remove multiple inactive retry lineages in one transaction."""
        requested = list(dict.fromkeys(job_id(identifier) for identifier in identifiers))
        if not requested:
            raise VastError('Choose at least one cloud job to delete', 422)
        with advisory_file_lock(self.root / '.queue-lock'):
            rows = self.store.list()
            by_id = {row['id']: row for row in rows}
            if any(identifier not in by_id or by_id[identifier].get('kind') == 'worker'
                   for identifier in requested):
                raise VastError('Cloud job not found', 404)
            lineage = set(requested)
            changed = True
            while changed:
                changed = False
                for row in rows:
                    parent = row.get('requeue_from')
                    if row.get('kind') != 'worker' and (row['id'] in lineage or parent in lineage):
                        before = len(lineage)
                        lineage.add(row['id'])
                        if parent in by_id and by_id[parent].get('kind') != 'worker':
                            lineage.add(parent)
                        changed = changed or len(lineage) != before
            for job_identifier in lineage:
                row = by_id[job_identifier]
                if not can_remove_job(row, self.worker_for(row.get('lease_id'))):
                    raise VastError('Stop every retry attempt and finish collection/cleanup before deleting its history', 409)
            deleted_at = time.time()
            for job_identifier in lineage:
                row = by_id[job_identifier]
                changes = {'deleted_at': deleted_at}
                if row['status'] == 'ready':
                    changes['status'] = 'cancelled'
                self.store.update(job_identifier, **changes)
            from vast_performance import PerformanceHistory
            PerformanceHistory(self.root).delete_runs(lineage)
            staged = []
            for job_identifier in lineage:
                directory = self.store.directory(job_identifier)
                temporary = directory.with_name(f'.{job_identifier}.delete-{uuid.uuid4().hex}')
                try:
                    os.replace(directory, temporary)
                except OSError as exc:
                    raise VastError('Cloud job history could not be removed completely', 500) from exc
                staged.append(temporary)
        # Snapshot cleanup can take minutes. Readers must never see a partially
        # removed job, and unrelated queue actions must not wait for its files.
        for directory in staged:
            try:
                shutil.rmtree(directory)
            except OSError as exc:
                raise VastError('Cloud job history could not be removed completely', 500) from exc
        return {'requested_ids': requested, 'deleted': True, 'purged_ids': sorted(lineage)}

    def recover_deleted_job_histories(self) -> tuple[list[str], int, list[str]]:
        """Purge legacy tombstones while an unfinished retry still needs its input."""
        staged: list[tuple[str, Path]] = []
        failed: list[str] = []
        with advisory_file_lock(self.root / '.queue-lock'):
            rows = self.store.list()
            by_id = {row['id']: row for row in rows}
            neighbors: dict[str, set[str]] = {identifier: set() for identifier in by_id}
            for row in rows:
                parent = row.get('requeue_from')
                if isinstance(parent, str) and parent in by_id:
                    neighbors[row['id']].add(parent)
                    neighbors[parent].add(row['id'])
            active_job_ids = {row.get('active_job') for row in rows
                              if row.get('kind') == 'worker' and row.get('active_job')}
            protected = {row['id'] for row in rows if row.get('kind') != 'worker' and (
                row.get('status') not in TERMINAL or not can_remove_job(
                    row, {'active_job': row['id']} if row['id'] in active_job_ids else None))}
            pending = list(protected)
            while pending:
                for identifier in neighbors[pending.pop()] - protected:
                    protected.add(identifier)
                    pending.append(identifier)
            candidates = [row['id'] for row in rows if row.get('kind') != 'worker'
                          and row.get('deleted_at') and row['id'] not in protected]
            retained = sum(row.get('kind') != 'worker' and bool(row.get('deleted_at'))
                           and row['id'] in protected for row in rows)
            if candidates:
                from vast_performance import PerformanceHistory
                PerformanceHistory(self.root).delete_runs(candidates)
            for identifier in candidates:
                try:
                    directory = self.store.directory(identifier)
                    temporary = directory.with_name(f'.{identifier}.delete-{uuid.uuid4().hex}')
                    os.replace(directory, temporary)
                except (OSError, VastError):
                    failed.append(identifier)
                else:
                    staged.append((identifier, temporary))
        removed = []
        for identifier, directory in staged:
            try:
                shutil.rmtree(directory)
            except OSError:
                failed.append(identifier)
            else:
                removed.append(identifier)
        return removed, retained, failed

    def recover_orphaned_workers(self) -> tuple[list[str], list[str]]:
        """Remove verified-closed rentals no surviving job needs for its history."""
        staged: list[tuple[str, Path]] = []
        failed: list[str] = []
        with advisory_file_lock(self.root / '.queue-lock'):
            rows = self.store.list()
            referenced = {row.get('lease_id') for row in rows if row.get('kind') != 'worker'}
            current = self.read()
            for row in rows:
                identifier = row['id']
                if (row.get('kind') != 'worker' or row.get('rental_state') != 'deletion_verified'
                        or identifier in referenced):
                    continue
                if current.get('worker_id') == identifier:
                    current.update(worker_id=None, selected_offer=None)
                    write_json(self.root / 'queue.json', current)
                try:
                    directory = self.store.directory(identifier)
                    temporary = directory.with_name(f'.{identifier}.delete-{uuid.uuid4().hex}')
                    os.replace(directory, temporary)
                except (OSError, VastError):
                    failed.append(identifier)
                else:
                    staged.append((identifier, temporary))
        removed = []
        for identifier, directory in staged:
            try:
                shutil.rmtree(directory)
            except OSError:
                failed.append(identifier)
            else:
                removed.append(identifier)
        return removed, failed

    def recover_staged_deletions(self) -> tuple[int, list[str]]:
        """Finish confirmed history deletions left by a stopped API process."""
        folder = self.root / 'jobs'
        if not folder.exists():
            return 0, []
        if folder.is_symlink():
            raise VastError('Invalid cloud jobs directory', 500)
        removed, failed = 0, []
        for path in folder.iterdir():
            if not re.fullmatch(r'\.[0-9a-f]{32}\.delete-(?:[0-9a-f]{32}|interrupted)', path.name):
                continue
            if not path.is_dir() or path.is_symlink():
                failed.append(path.name)
                continue
            try:
                shutil.rmtree(path)
            except OSError:
                failed.append(path.name)
            else:
                removed += 1
        return removed, failed

    def action(self, action: str) -> dict:
        """Control queue scheduling without implicitly creating a replacement GPU."""
        with advisory_file_lock(self.root / '.queue-lock'):
            if action not in {'pause', 'resume', 'end', 'recover'}:
                raise VastError('Invalid cloud queue action', 422)
            workers = self.workers()
            if action == 'resume' and self.read().get('calibration_watch'):
                raise VastError('Cancel the waiting performance test before resuming ordinary GPU rentals', 409)
            if action == 'end' and self.read().get('calibration_watch'):
                from vast_calibration_watch import cancel_watch
                cancel_watch(self, self.read()['calibration_watch']['id'], 'Waiting test cancelled with GPU queue')
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
            from vast_job_runner import uncreated_rental_message
            creation_message = uncreated_rental_message(worker)
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
                           creation_message if creation_message else
                           'Vast.ai instance disappeared; the last local snapshot remains available' if missing else
                           'Rental ended; the last local snapshot remains available')
                store.update(job['id'], status='cancelled' if stopped and not job.get('final_collected') else 'failed', error=message)
            if (missing or creation_message) and not queue.read().get('pool_enabled'):
                queue.update(paused=True)
            store.update(identifier, status='failed' if missing or creation_message else 'completed', active_job=None,
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
        calibration_job_id = worker.get('calibration_job_id')
        if calibration_job_id:
            calibration = store.read(job_id(calibration_job_id))
            if calibration.get('status') in TERMINAL:
                store.control(identifier, 'cleanup')
                return None
            candidates = [calibration] if calibration.get('status') == 'ready' else []
        else:
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
