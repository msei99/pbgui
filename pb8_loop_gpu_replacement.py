"""Durable, generation-bound infrastructure replacement within one PB8 Loop."""
from __future__ import annotations

import copy
import math
import time
import uuid

from file_lock import advisory_file_lock
from logging_helpers import human_log as _log
from pb8_loop_store import digest, identifier
from vast_provider import VastError

SERVICE = 'PB8Loop'
FINISHED = {'complete', 'completed', 'failed', 'error', 'cancelled', 'stopped'}


def open_replacement(record):
    """Find the durable operation independently of the visible pause/stop status."""
    operation = (record.get('gpu_replacements') or {}).get(record.get('gpu_replacement_id'))
    return operation if operation and operation['step'] not in {'done', 'aborted'} else None


def replaced_predecessor(record, job):
    """Only an authorized archived operation can label a predecessor interruption."""
    operation = (record.get('gpu_replacements') or {}).get(job.get('gpu_replacement_id'))
    if (operation and job['operation'] in {operation['predecessor'], *operation.get('interrupted_operations', [])}
            and operation.get('effects', {}).get('stop' if operation['predecessor'] == job['operation']
                else 'stop:' + job['operation']) and job.get('status') in {'cancelled', 'stopped'}):
        return operation
    return None


def attempts_used(record):
    """Dispatched interruptions retain their charge, including pre-optimization work."""
    return sum(bool(job.get('started') or job.get('attempt_charged') or job['status'] not in FINISHED)
               for job in record['jobs'] if job['kind'] == 'optimizer')


def _number(value):
    """Reject unknown/nonfinite cost evidence instead of treating it as zero."""
    return type(value) in (int, float) and math.isfinite(value) and value >= 0


def chain_spending(queue, chain):
    """Reserve transfer cost and all elapsed rental time, or the complete authorization."""
    total = 0.0
    for worker_id in chain:
        worker = queue.store.read(identifier(worker_id))
        intent = queue.store.read(worker_id, 'intent.json')
        price = (intent.get('offer') or {}).get('price_hour_usd')
        accepted = intent.get('accepted_at')
        # Keep every already-bound transfer obligation, even if billing or the
        # acknowledgement of a reserve increase has not arrived yet. Do not
        # clip incurred/reserved transfers to the lease's remaining GPU time.
        reserves = [intent.get('transfer_reserve_usd')]
        reserves.extend(worker[key] for key in ('transfer_reserve_confirmed', 'transfer_reserved_used')
                        if worker.get(key) is not None)
        pending = worker.get('deadline_request')
        if pending:
            if not isinstance(pending, dict):
                reserves.append(None)  # Unreadable obligations require the full budget fallback.
            elif 'transfer_reserve_usd' in pending:
                reserves.append(pending['transfer_reserve_usd'])
        reported = (worker.get('billing') or {}).get('amount_usd')
        end = worker.get('provider_checked_at') if worker.get('rental_state') == 'deletion_verified' else time.time()
        if (all(_number(value) for value in (price, accepted, end, *reserves)) and price > 0
                and (reported is None or _number(reported))):
            cost = max(reserves) + max(0, end - accepted) / 3600 * price
        else:
            cost = intent.get('budget_usd')
            if not _number(cost):
                raise VastError('Rental costs are unknown; review billing before replacing this GPU', 409)
            if _number(worker.get('budget_confirmed')):
                cost = max(cost, worker['budget_confirmed'])
        if _number(reported):
            cost = max(cost, reported)
        total += cost
    return total


def remaining_limits(queue, record, operation):
    """Intersect the original chain authorization with stricter current shared limits."""
    from api.vast import RentalPreferences
    frozen = operation['limits']
    current = RentalPreferences.model_validate(queue.read().get('gpu_preferences') or {}).model_dump()
    limits = copy.deepcopy(frozen['vast'])
    for key in ('max_price', 'max_rentals'):
        limits[key] = min(limits[key], current[key])
    for key in ('min_vram', 'min_ram', 'min_cpu', 'min_tflops', 'min_power_watts', 'min_reliability_pct', 'disk_gb'):
        limits[key] = max(limits.get(key, 0), current.get(key, 0))
    limits['verified_only'] = bool(limits.get('verified_only') or current.get('verified_only'))
    # Conflicting GPU name restrictions cannot be silently relaxed.
    if current.get('gpu_name') and current['gpu_name'] != limits.get('gpu_name'):
        raise VastError('Current GPU selection differs from the approved Loop; review rental authorization', 409)
    deadline = min(frozen['deadline'], record['deadline'])
    budget = max(0, min(frozen['budget'], current['budget']) - chain_spending(queue, operation['chain']))
    limits.update(hours=max(0, min(frozen['vast']['hours'], current['hours'], (deadline - time.time()) / 3600)), budget=budget)
    return limits, deadline


def replacement_gpu_profiles(operation, job_id):
    """Remap only the interrupted job's frozen profile to its reserved successor."""
    profiles = operation.get('gpu_profiles') or {}
    return {'rental_gpu_profile': copy.deepcopy(profiles.get('rental_gpu_profile')),
            'rental_job_gpu_profiles': {job_id: copy.deepcopy(profiles['job_profile'])}
                if profiles.get('job_profile') is not None else {}}


def review(queue, record, worker_id):
    """Freeze a reviewable owner/job/rental snapshot without performing provider actions."""
    worker_id = identifier(worker_id)
    worker = queue.store.read(worker_id)
    if (worker.get('kind') != 'worker' or worker.get('loop_id') != record['id']
            or worker.get('loop_owner') != record['owner']):
        raise VastError('This GPU rental does not belong to the selected Loop', 409)
    if open_replacement(record):
        raise VastError('This Loop already has a GPU replacement in progress', 409)
    if record['status'] not in {'running', 'paused'} or record['phase'] != 'optimize':
        raise VastError('GPU replacement requires a running or paused optimizer round', 409)
    if record.get('rental_extension') or worker.get('deadline_request') or record.get('rental_handover', {}).get('status') == 'collecting':
        raise VastError('Wait for the current rental change or handover before replacing this GPU', 409)
    control = queue.store.read(worker_id, 'control.json')
    if control.get('stop') or control.get('cleanup') or worker.get('rental_state') in {'none', 'deletion_verified', 'destroy_pending'}:
        raise VastError('This rental is already ending; GPU replacement was not authorized', 409)
    jobs = []
    for job in record['jobs']:
        if job['kind'] != 'optimizer' or job['round'] != record['round'] or job['status'] in FINISHED or not job.get('backend'):
            continue
        native = queue.store.read(job['backend']['id']) if job['backend'].get('execution') == 'vast' else {}
        if native.get('lease_id') == worker_id:
            if native.get('status') in FINISHED:
                raise VastError('The attached optimizer already finished; wait for Loop collection before reviewing replacement', 409)
            if native.get('loop_id') != record['id'] or native.get('loop_owner') != record['owner']:
                raise VastError('Attached optimizer ownership changed', 409)
            jobs.append(job)
    if len(jobs) != 1:
        raise VastError('Replacement requires exactly one interrupted optimizer on the selected rental', 409)
    intent = queue.store.read(worker_id, 'intent.json')
    from vast_gpu_tuning import _validated_rental_gpu_profile
    try:
        profiles = {'rental_gpu_profile': _validated_rental_gpu_profile(intent.get('rental_gpu_profile')),
                    'job_profile': _validated_rental_gpu_profile(
                        (intent.get('rental_job_gpu_profiles') or {}).get(jobs[0]['backend']['id']))}
    except (ValueError, AttributeError):
        raise VastError('The selected rental GPU profile is invalid; review its saved settings', 409) from None
    machine = worker.get('host_machine_id') or (intent.get('offer') or {}).get('machine_id')
    if type(machine) is not int or machine <= 0:
        raise VastError('The old machine identity is unavailable; a different machine cannot yet be guaranteed', 409)
    previous = next((op for op in (record.get('gpu_replacements') or {}).values()
                     if op.get('worker_id') == worker_id and op['step'] == 'done'), None)
    chain = [*previous['chain'], worker_id] if previous else [worker_id]
    limits = copy.deepcopy(previous['limits']) if previous else {
        'vast': copy.deepcopy(record['settings']['vast']),
        'budget': min(intent['budget_usd'], record['settings']['vast']['budget']),
        'deadline': min(intent['deadline'], record['deadline'])}
    snapshot = {'loop_id': record['id'], 'owner': record['owner'], 'worker_id': worker_id,
                'generation': record['control_generation'], 'round': record['round'],
                'predecessor': jobs[0]['operation'], 'input_digest': digest([
                    jobs[0]['config'], jobs[0]['overrides'], jobs[0].get('optimizer_start'), jobs[0].get('optimizer_archive')]),
                'limits': limits, 'chain': chain, 'machine': machine, 'gpu_profiles': profiles,
                'rental_deadline': intent['deadline'], 'rental_budget': intent['budget_usd']}
    derived, deadline = remaining_limits(queue, record, snapshot)
    return {'snapshot': snapshot, 'snapshot_digest': digest(snapshot),
            'remaining_budget_usd': derived['budget'], 'deadline': deadline,
            'attempts_used': attempts_used(record), 'max_runs': record['settings']['max_runs'],
            'excluded_machine_ids': sorted({machine, *(previous.get('excluded_machine_ids', []) if previous else [])}),
            'effect': 'Stop and collect only this rental, confirm provider deletion, and restart the preserved optimizer input on a different machine in this same Loop and round. In-flight work is lost; no checkpoint compatibility is assumed. Dispatched attempts still count. No budget, deadline or attempt reset; no global host block or Auto rent change.'}


class GPUReplacement:
    """Reconcile one archived operation under the existing controller ownership lock."""

    def __init__(self, store, queue, backend):
        self.store, self.queue, self.backend = store, queue, backend

    def begin(self, owner, loop_id, worker_id, generation, snapshot_digest, operation_id):
        """Publish intent before the first stop; retry the same durable operation identity."""
        from pb8_loop_handover import controller_claims
        owner, loop_id, operation_id = map(identifier, (owner, loop_id, operation_id))
        with controller_claims(self.store, owner, loop_id), advisory_file_lock(self.queue.root / '.queue-lock'):
            row = self.store.read(owner, loop_id)
            existing = (row.get('gpu_replacements') or {}).get(operation_id)
            if existing:
                if existing['old_worker_id'] != worker_id or existing['review_digest'] != snapshot_digest:
                    raise VastError('Replacement replay does not match its approved rental', 409)
                return row
            data = review(self.queue, row, worker_id)
            if row['control_generation'] != generation or data['snapshot_digest'] != snapshot_digest:
                raise VastError('Loop or rental changed since review; review GPU replacement again', 409)
            snapshot = data['snapshot']
            def reserve(current):
                if current['control_generation'] != generation or open_replacement(current):
                    raise VastError('Loop control changed before GPU replacement', 409)
                operation = dict(snapshot, id=operation_id, old_worker_id=worker_id,
                    review_digest=snapshot_digest, original_status=current['status'], original_phase=current['phase'],
                    excluded_machine_ids=data['excluded_machine_ids'], step='collect', created_at=time.time(),
                    generation=generation + 1, reason='Collecting available artifacts before ending the selected rental',
                    effects={}, successor=None, worker_id=None)
                current.setdefault('gpu_replacements', {})[operation_id] = operation
                current.update(gpu_replacement_id=operation_id,
                    status='paused' if current['status'] == 'paused' else 'replacing_gpu',
                    control_generation=generation + 1, reason=operation['reason'])
                job = next(job for job in current['jobs'] if job['operation'] == operation['predecessor'])
                job.update(gpu_replacement_id=operation_id, attempt_charged=True)
            return self.store.update(owner, loop_id, reserve)

    def change(self, record, function):
        """Replacement transitions cannot use the normal ACTIVE-only commit guard."""
        def guarded(current):
            operation = open_replacement(current)
            if (current['status'] != 'replacing_gpu' or current['control_generation'] != record['control_generation']
                    or not operation or operation['id'] != open_replacement(record)['id']
                    or operation['generation'] != current['control_generation']):
                raise VastError('A newer Loop control invalidated GPU replacement work', 409)
            function(current, operation)
        return self.store.update(record['owner'], record['id'], guarded)

    def note(self, record, reason, **values):
        """Persist a visible wait/error reason and its operation step before returning."""
        def save(current, operation):
            operation.update(reason=reason, updated_at=time.time(), **values)
            current['reason'] = reason
        return self.change(record, save)

    def effect(self, record, name):
        """A durable reservation must precede each replace-related external effect."""
        def reserve(current, operation):
            operation['effects'].setdefault(name, {'at': time.time(), 'generation': current['control_generation']})
        return self.change(record, reserve)

    def local_effect(self, record, name, function, *args):
        """Serialize a reserved local Stop/control effect with newer Pause/Stop actions."""
        record = self.effect(record, name)
        with advisory_file_lock(self.store.path(record['owner'], record['id'])):
            current = self.store.read(record['owner'], record['id'])
            if current['status'] != 'replacing_gpu' or current['control_generation'] != record['control_generation']:
                raise VastError('New Loop control prevented the reserved replacement action', 409)
            function(*args)
        return record

    def step(self, record):
        """Bound each tick; errors retain the operation instead of failing strategy search."""
        try:
            return self._step(record)
        except Exception as exc:
            from pb8_loop_storage import LoopStorageError
            if isinstance(exc, LoopStorageError):
                raise
            _log(SERVICE, f'GPU replacement deferred: {type(exc).__name__}', level='WARNING')
            current = self.store.read(record['owner'], record['id'])
            if current['control_generation'] != record['control_generation'] or current['status'] != 'replacing_gpu':
                return current
            operation = open_replacement(current)
            reason = str(exc)[:1000] if isinstance(exc, VastError) else 'GPU replacement could not complete this step; review the retained artifacts and rental status'
            return self.note(current, reason, error={'step': operation['step'], 'at': time.time(),
                'reason': reason, 'type': type(exc).__name__}, retry_at=time.time() + 30)

    def _monitor_update(self, record, function):
        """Guard existing-job observations independently of replacement advancement or Pause."""
        def guarded(current):
            operation = open_replacement(current)
            if (current['status'] not in {'replacing_gpu', 'paused'}
                    or current['control_generation'] != record['control_generation']
                    or not operation or operation['id'] != open_replacement(record)['id']):
                raise VastError('A newer Loop control invalidated parallel job monitoring', 409)
            function(current, operation)
        return self.store.update(record['owner'], record['id'], guarded)

    def monitor_jobs(self, record):
        """Poll dispatched parallel optimizers and enforce quotas without launching or evaluating."""
        operation = open_replacement(record)
        if not operation or record['status'] not in {'replacing_gpu', 'paused'}:
            return record
        reserved = {operation['predecessor'], operation.get('successor'), *operation.get('interrupted_operations', [])}
        for original in record['jobs']:
            if (original['kind'] != 'optimizer' or original['operation'] in reserved
                    or original['status'] in FINISHED or not original.get('backend')):
                continue
            state = self.backend.poll(record, original)
            if not (original.get('started') or state.get('started')):
                continue  # A prepared/queued input must never become a normal Queue start.
            source = state.get('source') or {}
            started_at = original.get('run_started_at') or source.get('started_at')
            if started_at is None and state['status'] == 'running':
                started_at = time.time()
            proxy = state.get('proxy_evaluations')
            mode, hit = record['settings'].get('run_limit_mode', 'config'), None
            if state['status'] not in FINISHED and started_at is not None:
                if mode == 'hours' and time.time() >= started_at + record['settings']['run_hours'] * 3600:
                    hit = 'hours'
                if mode == 'proxy' and (proxy or 0) >= record['settings']['run_proxy']:
                    hit = 'proxy'
            if (hit or original.get('limit_reached')) and state['status'] not in FINISHED:
                def limited(current, op):
                    job = next(item for item in current['jobs'] if item['operation'] == original['operation'])
                    job.update(limit_reached=job.get('limit_reached') or hit, run_started_at=started_at)
                    op['effects'].setdefault('quota_stop:' + job['operation'],
                        {'at': time.time(), 'generation': current['control_generation']})
                # Native Vast Stop is a local control write. Publish its quota
                # intent first and serialize it with newer user/storage controls.
                with advisory_file_lock(self.store.path(record['owner'], record['id'])):
                    record = self._monitor_update(record, limited)
                    job = next(item for item in record['jobs'] if item['operation'] == original['operation'])
                    self.backend.stop(record, job)
                state = self.backend.poll(record, job)
            def observed(current, op):
                job = next(item for item in current['jobs'] if item['operation'] == original['operation'])
                source = state.get('source') or {}
                job.update(status=state['status'], started=job.get('started') or state.get('started'),
                    last_observed_at=time.time())
                counts = [value for value in (proxy, state.get('proxy_evaluations'), job.get('proxy_evaluations'))
                          if type(value) is int and value >= 0]
                if counts:
                    job['proxy_evaluations'] = max(counts)
                if started_at is not None:
                    job['run_started_at'] = started_at
                if state.get('executed_config'):
                    job.update(executed_config=copy.deepcopy(state['executed_config']), executed_digest=digest(state['executed_config']))
                if state['status'] in FINISHED:
                    job['ended_at'] = job.get('ended_at') or time.time()
                error = state.get('error') or source.get('error')
                if error:
                    job['error'] = str(error)[:1000]
                elif state['status'] in {'complete', 'completed', 'running'} or job.get('limit_reached'):
                    if job.get('error'):
                        job['historical_error'] = job['error']
                    job['error'] = None
                job['runner'] = {key: source.get(key) for key in
                    ('started_at', 'pb8_version', 'pb8_commit', 'lease_id', 'dispatch_at', 'deadline', 'rental_state', 'error')}
            record = self._monitor_update(record, observed)
        return record

    def _step(self, record):
        operation = open_replacement(record)
        if not operation:
            return record
        if time.time() >= min(record['deadline'], operation['limits']['deadline']) - 60:
            def expired(current):
                if (current['control_generation'] == record['control_generation']
                        and current['status'] in {'paused', 'replacing_gpu'} and open_replacement(current)):
                    current.update(status='stopping', reason='Original GPU replacement deadline reached',
                                   control_generation=current['control_generation'] + 1)
            return self.store.update(record['owner'], record['id'], expired)
        record = self.monitor_jobs(record)
        operation = open_replacement(record)
        if record['status'] != 'replacing_gpu':
            return record
        if time.time() < operation.get('retry_at', 0):
            return record
        queue = self.queue
        old = queue.store.read(operation['old_worker_id'])
        if old.get('loop_id') != record['id'] or old.get('loop_owner') != record['owner']:
            raise VastError('Selected rental ownership changed; replacement is waiting for review', 409)
        predecessor = next(job for job in record['jobs'] if job['operation'] == operation['predecessor'])
        if operation['step'] == 'collect':
            # worker_action/end controls only this rental; existing runner owns collection.
            if old['rental_state'] not in {'none', 'deletion_verified'}:
                record = self.local_effect(record, 'stop', queue.worker_action, old['id'], 'end')
            else:
                record = self.effect(record, 'stop')
            state = self.backend.poll(record, predecessor)
            def collected(row, op):
                job = next(job for job in row['jobs'] if job['operation'] == op['predecessor'])
                source = state.get('source') or {}
                job.update(status=state['status'], started=job.get('started') or state.get('started'),
                           error=state.get('error') or source.get('error'))
                count = state.get('proxy_evaluations')
                if type(count) is int and count >= 0:
                    job['proxy_evaluations'] = max(job.get('proxy_evaluations') or 0, count)
                if state.get('executed_config'):
                    job.update(executed_config=copy.deepcopy(state['executed_config']), executed_digest=digest(state['executed_config']))
                job['runner'] = {key: source.get(key) for key in
                    ('started_at', 'pb8_version', 'pb8_commit', 'lease_id', 'dispatch_at', 'deadline', 'rental_state', 'error')}
                if source.get('started_at') is not None:
                    job['run_started_at'] = job.get('run_started_at') or source['started_at']
                if state['status'] in FINISHED:
                    job['ended_at'] = job.get('ended_at') or time.time()
            record = self.change(record, collected)
            operation = open_replacement(record)
            expired = time.time() >= operation['effects']['stop']['at'] + 270
            if state['status'] not in FINISHED and not expired:
                return record
            if old['rental_state'] not in {'none', 'deletion_verified'}:
                record = self.local_effect(record, 'cleanup', queue.store.control, old['id'], 'cleanup')
            incomplete = expired or not (state.get('source') or {}).get('final_collected')
            return self.note(record, 'Waiting for confirmed provider deletion; '
                + ('collection is incomplete or unavailable; any verified local artifacts are retained' if incomplete
                   else 'verified local artifacts are retained'), step='deletion', artifacts_incomplete=incomplete)
        if operation['step'] == 'deletion':
            if old['rental_state'] != 'deletion_verified' or old.get('absent_checks', 0) < 2:
                return record
            return self.note(record, 'Provider deletion confirmed; waiting for eligible replacement capacity', step='capacity')
        if operation['step'] == 'capacity':
            if attempts_used(record) >= record['settings']['max_runs']:
                return self.note(record, 'Optimizer attempt allowance exhausted; no replacement will be rented without additional authorization', retry_at=time.time() + 30)
            limits, deadline = remaining_limits(queue, record, operation)
            if limits['hours'] < .25 or limits['budget'] < .1:
                return self.note(record, 'Remaining rental budget or time cannot cover a replacement; original limits are retained', retry_at=time.time() + 30)
            # Reserve a physical attempt once, retaining every frozen execution input.
            def reserve(row, op):
                token = uuid.uuid4().hex
                successor = {key: copy.deepcopy(predecessor[key]) for key in
                    ('kind', 'round', 'config', 'overrides', 'optimizer_start', 'optimizer_archive', 'reason', 'candidate') if key in predecessor}
                successor.update(operation=token, name='loop_' + row['id'][:8] + '_' + token[:8],
                    status='planned', backend=None, started=False, created_at=time.time(),
                    gpu_predecessor=op.get('retry_predecessor') or predecessor['operation'], gpu_replacement_successor=op['id'])
                row['jobs'].append(successor)
                op.update(successor=token, worker_id=uuid.uuid4().hex, step='prepare')
            return self.change(record, reserve)
        successor = next(job for job in record['jobs'] if job['operation'] == operation['successor'])
        if operation['step'] == 'retry_collection':
            record = self.local_effect(record, 'stop:' + successor['operation'], self.backend.stop, record, successor)
            state = self.backend.poll(record, successor)
            if state['status'] not in FINISHED:
                return record
            def retry_ready(current, op):
                job = next(job for job in current['jobs'] if job['operation'] == op['successor'])
                job.update(status=state['status'], started=job.get('started') or state.get('started'),
                    error=state.get('error') or (state.get('source') or {}).get('error'), ended_at=time.time())
                op.setdefault('interrupted_operations', []).append(job['operation'])
                op['chain'].append(op['worker_id'])
                offer_machine = (op.get('reservation') or {}).get('offer', {}).get('machine_id')
                if type(offer_machine) is int and offer_machine > 0 and offer_machine not in op['excluded_machine_ids']:
                    op['excluded_machine_ids'].append(offer_machine)
                op.update(step='capacity', retry_predecessor=job['operation'], successor=None,
                          worker_id=None, reservation=None)
                current['reason'] = op['reason'] = 'Replacement capacity ended; retrying preserved input only within remaining attempts, budget and original deadline'
            return self.change(record, retry_ready)
        if operation['step'] == 'prepare':
            record = self.effect(record, 'prepare')
            self.backend.verify_replacement_input(record, successor)
            backend = self.backend.prepare(record, successor)
            state = self.backend.poll(record, {**successor, 'backend': backend})
            if state['status'] in {'failed', 'error', 'cancelled', 'stopped'}:
                def preparation_failed(current, op):
                    job = next(job for job in current['jobs'] if job['operation'] == op['successor'])
                    job.update(backend=backend, status=state['status'], started=bool(state.get('started')),
                        attempt_charged=True, ended_at=time.time(),
                        error=state.get('error') or (state.get('source') or {}).get('error')
                            or 'Replacement input preparation did not complete')
                    op.update(step='capacity', retry_predecessor=job['operation'], successor=None,
                        worker_id=None, reservation=None, retry_at=time.time() + 30,
                        reason='Replacement input preparation was interrupted; retry waits within the unchanged attempt allowance')
                    current['reason'] = op['reason']
                return self.change(record, preparation_failed)
            def prepared(row, op):
                job = next(job for job in row['jobs'] if job['operation'] == op['successor'])
                job.update(backend=backend, status='queued')
                op.update(step='rent')
            return self.change(record, prepared)
        if operation['step'] == 'rent':
            existing = next((row for row in queue.store.list() if row['id'] == operation['worker_id']), None)
            if existing and existing['rental_state'] != 'none':
                # The same identity reconstructs accepted/ambiguous Create across API restarts.
                return self.note(record, 'Replacement rental reserved; waiting for provider and worker confirmation', step='resume')
            self.backend.verify_replacement_input(record, successor)
            limits, deadline = remaining_limits(queue, record, operation)
            if limits['hours'] < .25 or limits['budget'] < .1:
                return self.note(record, 'Remaining rental budget or time cannot cover replacement capacity', retry_at=time.time() + 30)
            from vast_queue import CloudQueue, blocked_machine_ids
            from vast_pool import select_offer
            class ScopedQueue(CloudQueue):
                """Only the reserved successor may enter this rental authorization."""
                def waiting(self, loop_id=None):
                    return [job for job in super().waiting(record['id']) if job['id'] == successor['backend']['id']]
                def read(self):
                    value = super().read()
                    value['blocked_machine_ids'] = sorted(set(blocked_machine_ids(value)) | set(operation['excluded_machine_ids']))
                    return value
            scoped = ScopedQueue(queue.store)
            if existing and operation.get('reservation'):
                authorization = operation['reservation']
                validate_rental_authorization(scoped, record, authorization)
                record = self.effect(record, 'rent')
                scoped.start(authorization['offer'], authorization['hours'], authorization['budget'],
                    record['settings']['vast']['idle_seconds'], loop_id=record['id'], loop_owner=record['owner'],
                    loop_limits=record['settings']['vast'], replacement_authorization=authorization,
                    **replacement_gpu_profiles(operation, successor['backend']['id']))
                return self.note(record, 'Reconstructing the reserved replacement rental', step='resume')
            offer = select_offer(scoped, limits)
            if offer.get('machine_id') in operation['excluded_machine_ids']:
                raise VastError('Offer belongs to an excluded machine in this Loop replacement chain', 409)
            authorization = {'operation_id': operation['id'], 'generation': record['control_generation'],
                'worker_id': operation['worker_id'], 'job_id': successor['backend']['id'],
                'budget': limits['budget'], 'hours': limits['hours'], 'deadline': deadline, 'offer': offer,
                **replacement_gpu_profiles(operation, successor['backend']['id'])}
            record = self.change(record, lambda row, op: op.update(reservation=authorization))
            record = self.effect(record, 'rent')
            scoped.start(offer, limits['hours'], limits['budget'], limits['idle_seconds'],
                loop_id=record['id'], loop_owner=record['owner'], loop_limits=record['settings']['vast'],
                replacement_authorization=authorization,
                **replacement_gpu_profiles(operation, successor['backend']['id']))
            return self.note(record, 'Waiting for replacement provider confirmation; no duplicate rental is permitted', step='resume')
        if operation['step'] == 'resume':
            worker = queue.store.read(operation['worker_id'])
            if worker.get('loop_id') != record['id'] or worker.get('loop_owner') != record['owner']:
                raise VastError('Replacement rental ownership changed', 409)
            if worker['rental_state'] == 'deletion_verified':
                if worker.get('absent_checks', 0) < 2:
                    return self.note(record, 'Waiting for two fresh provider absence confirmations for replacement capacity', retry_at=time.time() + 30)
                def retry(current, op):
                    job = next(job for job in current['jobs'] if job['operation'] == op['successor'])
                    job.update(gpu_replacement_id=op['id'], attempt_charged=True)
                    op.update(step='retry_collection', reason='Replacement capacity ended; collecting its reserved attempt before another bounded retry')
                    current['reason'] = op['reason']
                return self.change(record, retry)
            if worker['rental_state'] != 'active':
                waiting = worker.get('authorization_wait_reason') or worker.get('creation_error') or worker.get('cleanup_error')
                if waiting:
                    return self.note(record, str(waiting)[:1000], retry_at=time.time() + 30)
                return record
            machine = worker.get('host_machine_id')
            if type(machine) is not int or machine <= 0 or machine in operation['excluded_machine_ids']:
                return self.note(record, 'Waiting for a confirmed different replacement machine identity', retry_at=time.time() + 30)
            def resumed(row, op):
                op.update(step='done', completed_at=time.time(), reason='Replacement ready; preserved input restarts without a checkpoint')
                row.update(status='running', phase=op['original_phase'], reason=op['reason'], repair_error=None)
            return self.change(record, resumed)
        raise VastError('Unknown GPU replacement operation step; review is required', 409)


def validate_rental_authorization(queue, record, authorization, *, offer=None):
    """Recheck exact reservation, generation, deletion and remaining limits before spending."""
    operation = open_replacement(record)
    if (not operation or record['status'] != 'replacing_gpu'
            or operation['generation'] != record['control_generation']
            or operation.get('reservation') != authorization or operation['id'] != authorization.get('operation_id')
            or operation['worker_id'] != authorization.get('worker_id')
            or time.time() >= min(record['deadline'], authorization['deadline']) - 60):
        raise VastError('GPU replacement reservation is paused, stopped, expired or stale', 409)
    for worker_id in operation['chain']:
        old = queue.store.read(worker_id)
        if (old.get('loop_id') != record['id'] or old.get('loop_owner') != record['owner']
                or old.get('rental_state') != 'deletion_verified' or old.get('absent_checks', 0) < 2):
            raise VastError('Provider deletion is not confirmed for a previous rental in this replacement chain', 409)
    profiles = replacement_gpu_profiles(operation, authorization['job_id'])
    if any(authorization.get(key, {} if key == 'rental_job_gpu_profiles' else None) != value
           for key, value in profiles.items()):
        raise VastError('Replacement GPU profiles differ from the preserved optimizer input', 409)
    limits, deadline = remaining_limits(queue, record, operation)
    from vast_queue import blocked_machine_ids, offer_host_allowed
    from vast_pool import MAX_RENTALS
    others = [worker for worker in queue.store.list() if worker['id'] != operation['worker_id']
              and worker.get('rental_state', 'none') not in {'none', 'deletion_verified'}]
    if len(others) >= min(MAX_RENTALS, limits['max_rentals']):
        raise VastError('Current GPU capacity limit prevents replacement Create; waiting for a free rental slot', 409)
    if not offer_host_allowed(authorization['offer'], blocked_machine_ids(queue.read())):
        raise VastError('Replacement GPU host is blocked or unidentified; review the reserved offer before Create', 409)
    if (authorization['budget'] > limits['budget'] + 1e-9 or authorization['deadline'] > deadline
            or authorization['hours'] > min(operation['limits']['vast']['hours'],
                queue.read().get('gpu_preferences', {}).get('hours', 0))
            or attempts_used(record) > record['settings']['max_runs']):
        raise VastError('Remaining replacement authorization changed; review the current limits', 409)
    selected = authorization['offer'] if offer is None else offer
    if (selected != authorization['offer'] or selected.get('machine_id') in operation['excluded_machine_ids']
            or not _number(selected.get('price_hour_usd')) or not 0 < selected['price_hour_usd'] <= limits['max_price']):
        raise VastError('Replacement offer no longer meets its approved limits', 409)
    from vast_provider import gpu_name_matches
    if (selected.get('num_gpus') != 1 or (selected.get('cuda_max_good') or 0) < 13
            or not gpu_name_matches(selected.get('gpu_name', ''), limits.get('gpu_name', ''))
            or any(not _number(selected.get(field)) or selected[field] < limits.get(requirement, 0)
                   for field, requirement in (('vram_gb', 'min_vram'), ('ram_gb', 'min_ram'), ('cpu_cores', 'min_cpu'),
                     ('tflops', 'min_tflops'), ('gpu_max_power_watts', 'min_power_watts'), ('disk_gb', 'disk_gb')))
            or (selected.get('reliability') or 0) * 100 < limits.get('min_reliability_pct', 0)
            or limits['verified_only'] and selected.get('verified') is not True):
        raise VastError('Replacement offer no longer meets the current GPU requirements', 409)
    return operation, limits
