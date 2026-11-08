"""API-owned autonomous PB8 loop lifecycle over durable jobs and AI decisions."""
from __future__ import annotations

import asyncio
import copy
import fcntl
import os
from pathlib import Path
import time
import traceback
import uuid

from logging_helpers import human_log as _log
from pb8_loop_store import ACTIVE, TERMINAL, LoopStore, LoopDeletionConflict, digest, validate_change, config_changes, apply_optimizer_start, qualified_winner, fresh_optimizer_config, validate_hsl_policy, round_limit, optimizer_execution_evidence
from pb8_loop_ai import LoopAI, LoopAIRetryScheduled, LoopAIRequestFailed, validate_rubric
from pb8_loop_evidence import LoopAIPause, BootstrapWithoutAdvice
from pb8_loop_exploration import evaluated_stagnation, validate_exploration
from pb8_loop_backend import LoopBackend
from pb8_loop_storage import LoopStorageError
from pb8_loop_observer import ObserverJobs, FINISHED as OBSERVER_FINISHED, needs_observer_report

SERVICE = 'PB8Loop'
JOB_FINISHED = {'completed', 'complete', 'failed', 'error', 'cancelled', 'stopped'}


class LoopRestartPending(Exception):
    """Defer a new AI decision until a reserved API restart has completed."""


def failure_details(record, error_type='LoopFailure'):
    """Capture nonsecret failure context before cleanup can change the run state."""
    settings = record['settings']
    retry = record.get('ai_retry') or {}
    pending = record.get('pending_ai') or {}
    return {'at': time.time(), 'phase': record['phase'], 'round': record['round'],
            'stage': pending.get('stage') or retry.get('stage') or record['phase'],
            'error_type': error_type, 'reason': record.get('reason') or record.get('last_error') or error_type,
            'detail': record.get('last_error'),
            'provider': settings.get('provider'), 'model': settings.get('model'), 'effort': settings.get('effort'),
            'attempts': retry.get('attempts') or (1 if pending else None),
            'history': copy.deepcopy(retry.get('history') or [])}


async def owned_thread(function, *args, **kwargs):
    """Join a started filesystem/process operation before honoring task cancellation."""
    task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    try:
        try:
            return await asyncio.wait_for(asyncio.shield(task), timeout=10)
        except TimeoutError:
            if task.done():
                return task.result()
            import sys
            stacks = {}
            for frame in sys._current_frames().values():
                names = []
                while frame is not None and len(names) < 8:
                    names.append(frame.f_code.co_name)
                    frame = frame.f_back
                key = '/'.join(names)
                stacks[key] = stacks.get(key, 0) + 1
            _log(SERVICE, 'Loop operation exceeds ten seconds', level='WARNING',
                 meta={'function': getattr(function, '__qualname__', type(function).__name__),
                       'thread_functions': dict(list(stacks.items())[:16])})
            return await asyncio.shield(task)
    except asyncio.CancelledError:
        await asyncio.gather(task, return_exceptions=True)
        raise


class LoopController:
    """Own replaceable controllers; detached native PB8 jobs survive API shutdown."""
    def __init__(self, root, store=None, backend=None, ai=None):
        self.store = store or LoopStore(root / 'data/loop_optimizer')
        self.backend = backend or LoopBackend(root, self.store)
        self.ai = ai or LoopAI(self.store)
        self.observers = ObserverJobs(self.store, self.backend, owned_thread)
        self._task = None
        self._steps = {}
        self._observer_steps = {}
        self._closing = False
        self._active_decisions = set()
        self._evidence_retention_at = 0
        self._idle_paused = {}
        self.restart_pending = lambda: False

    def start(self):
        if self._task is None or self._task.done():
            self._closing = False
            self._task = asyncio.create_task(self._run(), name='pb8-loop-controller')

    async def close(self):
        self._closing = True
        tasks = [task for task in [self._task, *self._steps.values(), *self._observer_steps.values()] if task is not None]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._idle_paused.clear()
        self._steps.clear()
        self._observer_steps.clear()
        self._task = None

    async def create(self, owner, settings, bundle=None, queued=False):
        round_limit(settings)
        await self.ai.preflight(owner, settings)
        args = (settings['config_name'], settings) if bundle is None else (settings['config_name'], settings, bundle)
        config, overrides, fingerprint, holdouts, comparison = await owned_thread(self.backend.initial, *args)
        exclusions = copy.deepcopy(holdouts)
        validate_change({'settings': settings, 'initial_config': config, 'initial_overrides': overrides, 'holdouts': holdouts, 'training_exclusions': exclusions}, config, overrides)
        record = await owned_thread(self.store.create, owner, settings, config, overrides, fingerprint, holdouts, comparison, queued=queued, training_exclusions=exclusions,
                                    optimizer_research_version=1 if settings['execution'] in {'gpu', 'vast'} else None)
        return record

    def action(self, owner, loop_id, action):
        def change(record):
            if action in {'start', 'resume'}:
                from ai_loop_windows import require_holdout_evidence_policy
                require_holdout_evidence_policy(record)
                from ai_loop_objectives import validate_template_rubric
                validate_template_rubric(record, record['initial_config'], record.get('rubric'))
                from pb8_loop_store import apply_run_limit, validate_cloud_loop_config
                validate_cloud_loop_config(apply_run_limit(record['settings'], record['initial_config']), record['settings']['execution'])
                from ai_loop_search_limits import exposure_ceiling, validate_exposure_ceiling
                validate_exposure_ceiling(record['initial_config'], exposure_ceiling(record['settings']))
            if record['status'] in TERMINAL:
                raise ValueError('The loop has already ended')
            if action == 'start':
                round_limit(record['settings'])
                if record['status'] != 'queued':
                    raise ValueError('Only a queued loop can be started')
                record.update(status='running', started_at=time.time(), deadline=time.time() + record['settings']['hours'] * 3600)
            elif action == 'pause':
                if record['status'] not in ACTIVE | {'replacing_gpu'}:
                    raise ValueError('Only an active loop can be paused')
                record['status'] = 'paused'
            elif action == 'resume':
                round_limit(record['settings'])
                if record['status'] != 'paused':
                    raise ValueError('Only a paused loop can be resumed')
                from pb8_loop_gpu_replacement import open_replacement
                operation = open_replacement(record)
                record['status'] = 'replacing_gpu' if operation else ('finishing' if record['phase'] == 'holdout' else 'running')
                if operation:
                    operation['generation'] = record['control_generation'] + 1
            elif action == 'stop':
                record['status'] = 'stopping'
                record['reason'] = 'Stopped by the user'
            else:
                raise ValueError('Unknown loop action')
            record['control_generation'] += 1
        return self.store.update(owner, loop_id, change)

    def delete(self, owner, loop_id):
        """Exclude concurrent controller work while deleting a collected owned run."""
        descriptors = []
        try:
            for suffix in ('.controller', '.observer-controller'):
                path = self.store.path(owner, loop_id).with_suffix(suffix)
                if path.is_symlink():
                    raise ValueError('Loop controller storage must not contain symlinks')
                fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
                descriptors.append(fd)
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError as exc:
                    raise LoopDeletionConflict('This loop is still processing. Wait for collection to finish before deleting it.') from exc
            self.store.delete(owner, loop_id)
        finally:
            for fd in reversed(descriptors):
                fcntl.flock(fd, fcntl.LOCK_UN)
                os.close(fd)

    async def _run(self):
        while not self._closing:
            try:
                records = await owned_thread(self.store.list_metadata)
                for record in records:
                    key = record['id']
                    observer_pending = (record['observer_pending'] if record.get('_metadata_only')
                                        else self.observers.pending(record))
                    if record['status'] in TERMINAL and record.get('cleanup_done'):
                        if observer_pending:
                            self._schedule_observers(record['owner'], key, allow_launch=True)
                        continue
                    task = self._steps.get(key)
                    if (task is not None and not task.done() and record['status'] in {'paused', 'stopping'}
                            and any(token[0] == key for token in self._active_decisions)):
                        # User control cancels only the owned AI request. Native launch/
                        # collection operations finish before the next cleanup tick.
                        if not task.cancelling():
                            task.cancel()
                        continue
                    if task is None or task.done():
                        if task and not task.cancelled():
                            task.exception()
                        idle_key = (record['owner'], key)
                        idle_stamp = self._idle_paused.get(idle_key)
                        if (record['status'] == 'paused' and idle_stamp is not None
                                and time.time() < record.get('deadline', 0) - 60):
                            path = self.store.path(record['owner'], key)
                            if await owned_thread(self.store._read_stamp, path) == idle_stamp:
                                continue
                        self._idle_paused.pop(idle_key, None)
                        self._steps[key] = asyncio.create_task(self.tick(record['owner'], key, background_reports=True), name='pb8-loop-' + key)
                for key in list(self._steps):
                    if self._steps[key].done():
                        if not self._steps[key].cancelled():
                            self._steps[key].exception()
                        self._steps.pop(key)
                if time.monotonic() >= self._evidence_retention_at:
                    from pb8_loop_evidence import EvidenceStore
                    self._evidence_retention_at = time.monotonic() + 3600
                    for record in records:
                        try:
                            await owned_thread(EvidenceStore(self.store).cleanup, record)
                        except (OSError, ValueError) as exc:
                            _log(SERVICE, 'Evidence retention deferred; loop processing continues', level='WARNING',
                                 meta={'error_type': type(exc).__name__})
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                _log(SERVICE, f'Loop scan failed: {type(exc).__name__}', level='ERROR', meta={'traceback': traceback.format_exc()})
            await asyncio.sleep(2)

    def _schedule_observers(self, owner, loop_id, *, allow_launch):
        """Own one independent report pass so its I/O cannot delay optimizer polling."""
        previous = self._observer_steps.get(loop_id)
        if self._closing or previous is not None and not previous.done():
            return
        task = asyncio.create_task(self._observer_tick(owner, loop_id, allow_launch=allow_launch),
                                   name='pb8-loop-reports-' + loop_id)
        self._observer_steps[loop_id] = task

        def finished(completed):
            """Release the bounded registry entry and retrieve failures deterministically."""
            if self._observer_steps.get(loop_id) is completed:
                self._observer_steps.pop(loop_id, None)
            if not completed.cancelled():
                completed.exception()
        task.add_done_callback(finished)

    async def _observer_tick(self, owner, loop_id, *, allow_launch):
        """Serialize reports across API processes, independently of training ownership."""
        path = self.store.path(owner, loop_id).with_suffix('.observer-controller')
        fd = None
        try:
            fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return
            current = await owned_thread(getattr(self.store, 'read_runtime', self.store.read), owner, loop_id)
            if allow_launch:
                await self.observers.tick(current)
            else:
                await self.observers.tick(current, allow_launch=False)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log(SERVICE, f'User-only evaluation for {loop_id}: {type(exc).__name__}', level='WARNING',
                 meta={'traceback': traceback.format_exc()})
        finally:
            if fd is not None:
                fcntl.flock(fd, fcntl.LOCK_UN)
                os.close(fd)

    async def tick(self, owner, loop_id, *, background_reports=False):
        """Claim nonblocking cross-process ownership for one recoverable state transition."""
        path = self.store.owner_dir(owner) / (loop_id + '.controller')
        if path.is_symlink():
            return
        fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return
            try:
                read_stamp = await owned_thread(self.store._read_stamp, self.store.path(owner, loop_id))
                record = await owned_thread(self._read_execution, owner, loop_id)
            except FileNotFoundError:
                # A queued/finished run may have been deleted after the scan.
                return
            except LoopStorageError as exc:
                await owned_thread(self.store.pause_storage, owner, loop_id, exc)
                _log(SERVICE, f'Loop {loop_id} archive cannot be read: {exc}', level='ERROR')
                return
            phase_before = record.get('phase')
            try:
                await self._tick(record)
            except asyncio.CancelledError:
                raise
            except LoopAIPause as exc:
                def pause(current):
                    """Preserve native jobs and reservations; expose a structured resume path."""
                    if current['status'] not in ACTIVE or current['control_generation'] != record['control_generation']:
                        return
                    if current.get('pending_ai'):
                        current['interrupted_ai'] = current['pending_ai']
                        current['pending_ai'] = None
                    current.update(status='paused', reason=str(exc), last_error=str(exc),
                                   ai_pause={'reason': exc.reason, 'message': str(exc), 'details': exc.details,
                                             'at': time.time(), 'phase': current['phase']},
                                   control_generation=current['control_generation'] + 1)
                await owned_thread(self.store.update, owner, loop_id, pause)
                _log(SERVICE, f'Loop paused: {exc.reason}: {str(exc)}', level='WARNING')
            except LoopAIRetryScheduled:
                # The cooldown is persisted; the normal scan resumes it when due.
                pass
            except LoopRestartPending:
                # Nothing was sent/reserved; the new controller resumes this phase.
                pass
            except LoopStorageError as exc:
                # Storage cannot be repaired by another model call. Keep the
                # already committed decision/evidence root intact for resume.
                await owned_thread(self.store.pause_storage, owner, loop_id, exc)
                _log(SERVICE, f'Loop {loop_id} paused after local storage failure: {exc}', level='ERROR')
            except Exception as exc:
                def failed(current):
                    waiting_handover = (current['status'] == 'queued'
                                        and (current.get('rental_handover') or {}).get('status') == 'collecting')
                    if current['status'] not in ACTIVE and not waiting_handover:
                        return
                    current['errors'] += 1
                    current['last_error'] = (str(exc).strip() or type(exc).__name__)[:1000]
                    # Managed transient requests have their own bounded retry
                    # policy. An interrupted or exhausted call cannot restart it.
                    correcting_proposal = (current.get('optimizer_research_version') == 1
                        and current['phase'] in {'interpret', 'bootstrap', 'select', 'evaluate'}
                        and isinstance(exc, ValueError) and not isinstance(exc, LoopAIRequestFailed))
                    if isinstance(exc, LoopAIRequestFailed) or current.get('pending_ai') or (current['errors'] >= 3 and not correcting_proposal):
                        current['status'] = 'failed'
                        current['reason'] = current['last_error']
                        current['failure'] = failure_details(current, type(exc).__name__)
                        if current['phase'] == 'bootstrap' and current.get('bootstrap'):
                            current['bootstrap'].update(status='failed', reason=current['reason'])
                    else:
                        current.pop('last_ai_decision', None)
                        current['repair_error'] = str(exc)[:1000]
                        if correcting_proposal:
                            current['decision_repair'] = {'active': True, 'attempts': current['errors'],
                                'error': current['repair_error'],
                                'retry_at': time.time() + min(300, 15 * 2 ** min(current['errors'] - 1, 5))}
                updated = await owned_thread(self.store.update, owner, loop_id, failed)
                _log(SERVICE, f"Loop {loop_id}, phase {record['phase']}, round {record['round'] + 1}: "
                     f'{type(exc).__name__}: {str(exc).strip() or type(exc).__name__}', level='ERROR',
                     meta={'failure': updated.get('failure'), 'traceback': traceback.format_exc()})
            # Separate failure boundary: user-only diagnostics never change the Loop's decisions/errors.
            try:
                current = await owned_thread(self._read_execution, owner, loop_id)
                from pb8_loop_research import enabled as research_enabled
                stopping_training = (research_enabled(current) and current.get('phase') == 'optimize'
                    and any(job.get('kind') == 'optimizer' and job.get('round') == current['round']
                        and job.get('status') not in JOB_FINISHED and job.get('limit_reached')
                        for job in current.get('jobs', [])))
                pending_training = (research_enabled(current) and current.get('phase') == 'optimize'
                    and any(job.get('kind') == 'optimizer' and job.get('round') == current['round']
                        and job.get('status') not in JOB_FINISHED
                        and (not job.get('started') or job.get('status') in {'planned', 'queued'})
                        for job in current.get('jobs', [])))
                if (current['status'] in ACTIVE and
                        (stopping_training or research_enabled(current) and current.get('phase') in {'select', 'evaluate'} or
                         phase_before != 'optimize' and current.get('phase') == 'optimize')):
                    # Release this transition's controller claim first. The next
                    # tick evaluates/starts training before expensive report work;
                    # its normal observer pass runs while training is active.
                    return
                if background_reports:
                    self._schedule_observers(owner, loop_id, allow_launch=not pending_training)
                elif pending_training and current['status'] in ACTIVE:
                    # Existing reports may complete while a rental slot is busy;
                    # defer their expensive preparation/launch, not their results.
                    await self.observers.tick(current, allow_launch=False)
                else:
                    await self.observers.tick(current)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                _log(SERVICE, f'User-only evaluation for {loop_id}: {type(exc).__name__}', level='WARNING',
                     meta={'traceback': traceback.format_exc()})
            else:
                if self._can_idle_paused(record):
                    stamp = await owned_thread(self.store._read_stamp, self.store.path(owner, loop_id))
                    if stamp == read_stamp:
                        if len(self._idle_paused) >= 128:
                            self._idle_paused.pop(next(iter(self._idle_paused)))
                        self._idle_paused[(owner, loop_id)] = stamp
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    @staticmethod
    def _can_idle_paused(record):
        """Only unchanged paused runs without collectable work may skip repeated hydration."""
        from pb8_loop_gpu_replacement import open_replacement
        # A paused reservation is durable bookkeeping, not an active request. The
        # inventory loop separately cancels/joins active decisions before another tick.
        if (record['status'] != 'paused' or record.get('rental_extension')
                or (record.get('rental_handover') or {}).get('status') == 'collecting'
                or open_replacement(record)):
            return False
        if any(job['status'] not in JOB_FINISHED and (job.get('backend') or job.get('started'))
               for job in record.get('jobs', [])):
            return False
        return not any(needs_observer_report(job) or (
            job['status'] not in OBSERVER_FINISHED and (job.get('backend') or job.get('started')))
            for job in record.get('observer_jobs', []))

    def _read_execution(self, owner, loop_id):
        """Keep population hydration out of active optimizer polling."""
        return getattr(self.store, "read_execution", self.store.read)(owner, loop_id)

    def commit(self, record, function):
        """Reject decisions made before a newer user pause/stop action."""
        def guarded(current):
            if current['control_generation'] != record['control_generation'] or current['status'] not in ACTIVE:
                raise ValueError('A newer pause or stop invalidated the pending transition')
            phase = current['phase']
            function(current)
            if current.get('decision_repair') and current['phase'] != phase:
                current['decision_repair']['active'] = False
            if current['status'] == 'failed':
                current['failure'] = failure_details(current)
        return self.store.update(record['owner'], record['id'], guarded)

    async def decision(self, record, stage, evidence):
        from ai_loop_windows import require_holdout_evidence_policy
        try:
            require_holdout_evidence_policy(record)
        except ValueError as exc:
            raise LoopAIPause('evidence_recovery', str(exc)) from exc
        reset_at = (await owned_thread(self.store.knowledge, record['owner'])).get('learning_reset_at', 0)
        if reset_at:
            evidence = {**evidence, 'learning_reset_at': reset_at}
        previous = record.get('last_ai_decision') or {}
        if (previous.get('stage') == stage and previous.get('round') == record['round']
                and previous.get('evidence_digest') == digest(evidence)
                and previous.get('repair_error') == record.get('repair_error')):
            return previous['decision']
        if self.restart_pending():
            raise LoopRestartPending()
        token = (record.get('id'), uuid.uuid4().hex)
        self._active_decisions.add(token)
        try:
            return await self.ai.decide(record, stage, evidence)
        finally:
            self._active_decisions.discard(token)

    def restart_block_reason(self):
        """Block managed restarts only while a Loop AI request is in flight."""
        if self._active_decisions:
            return f"{len(self._active_decisions)} AI Loop request(s) still running. Wait for the response before restarting."
        return ''

    def job(self, record, kind, config, overrides, reason, candidate=None, experiment=None):
        """Snapshot an experiment with a fresh population and durable RNG allocation."""
        token = uuid.uuid4().hex
        start = None
        research = None
        from pb8_loop_research import enabled, experiment_plan, resolve_variant
        if kind == 'optimizer' and enabled(record):
            variant = resolve_variant(record, {'config': config, 'overrides': overrides,
                                               'experiment': experiment})
            config, overrides, experiment = variant['config'], variant['overrides'], variant['experiment']
            research = experiment_plan(record, config, overrides, experiment)
        if kind == 'optimizer':
            from pb8_loop_rng import allocate_seed
            seed = allocate_seed(record)
            if research is not None:
                research['seed'] = seed
                research['seed_policy'] = 'controller_random'
            config = fresh_optimizer_config(config, seed)
            config, overrides, start = apply_optimizer_start(record, config, overrides)
            start.update(population='fresh_random', seed=seed, reference_only=True)
        archive = None
        if kind == 'optimizer':
            from pb8_loop_archive import archive_budget
            current = self.store.read(record['owner'], record['id'])
            archive = archive_budget({**record, 'ai_model_profile': current.get('ai_model_profile')}, config)
            config = copy.deepcopy(config)
            config['optimize']['pareto_max_size'] = archive['effective']
        validate_hsl_policy(record, config, overrides)
        if kind in {'optimizer', 'baseline', 'validation'}:
            from ai_loop_search_limits import exposure_ceiling, validate_exposure_ceiling
            validate_exposure_ceiling(config, exposure_ceiling(record['settings']))
        return {'operation': token, 'name': 'loop_' + record['id'][:8] + '_' + token[:8],
                'kind': kind, 'round': record['round'], 'config': copy.deepcopy(config),
                'overrides': copy.deepcopy(overrides), 'reason': str(reason)[:4000],
                'optimizer_start': start, 'optimizer_archive': archive,
                **({'research': research} if research is not None else {}),
                'candidate': candidate, 'status': 'planned', 'backend': None, 'started': False, 'created_at': time.time()}

    async def remember(self, record, evidence_id, entry):
        """A storage failure pauses before committing a winner; never destroys its proof."""
        try:
            await owned_thread(self.store.remember, record, evidence_id, entry)
        except OSError as exc:
            raise LoopAIPause('evidence_storage', 'The full finding could not be archived',
                              error_type=type(exc).__name__) from exc

    @staticmethod
    def pending_validation_retries(record):
        """Return the latest attempt for each candidate lacking a readable CPU result."""
        successful = {o['operation'] for o in record.get('observations') or []
                      if o.get('exact') and o.get('per_report')}
        latest = {}
        completed = set()
        for job in record['jobs']:
            candidate = job.get('candidate') or {}
            if (job['kind'] != 'validation' or job.get('round') != record['round']
                    or job.get('diagnostic_only') or not candidate):
                continue
            key = (candidate.get('optimizer_job'), candidate.get('id'))
            latest[key] = job
            if job['operation'] in successful:
                completed.add(key)
        return [job for key, job in latest.items() if key not in completed]

    async def import_native_training(self, record):
        """Advance from exact Vast results without launching a local validation process."""
        from pb8_loop_native import select, distinct, VERSION, IMPORT_ATTEMPTS
        from pb8_loop_gpu_replacement import replaced_predecessor
        retry = record.get('native_training_retry') or {}
        if retry.get('round') == record['round'] and retry.get('retry_at', 0) > time.time():
            return
        candidates = record['candidates']
        reload_errors = []
        if retry.get('round') == record['round']:
            candidates = []
            for source in record['jobs']:
                if (source['kind'] == 'optimizer' and source['round'] == record['round']
                        and not replaced_predecessor(record, source)):
                    try:
                        candidates.extend(await owned_thread(self.backend.candidates, record, source))
                    except (ValueError, OSError, RuntimeError) as exc:
                        reload_errors.append(str(exc)[:500])
            if not candidates:
                candidates = record['candidates']
        selected, summary = await owned_thread(select, record, candidates)
        if not summary['verified']:
            attempts = retry.get('attempts', 0) + 1 if retry.get('round') == record['round'] else 1
            errors = summary['errors'] + reload_errors + [job['result_error'] for job in record['jobs']
                if job['kind'] == 'optimizer' and job['round'] == record['round'] and job.get('result_error')]
            message = '; '.join(dict.fromkeys(errors))[:1000] or 'No verifiable exact Vast CPU training reports were found'
            if attempts >= IMPORT_ATTEMPTS:
                def fallback(row):
                    row.update(native_training_fallback={'active': True, 'round': row['round'],
                        'attempts': attempts, 'reason': message, 'at': time.time()},
                        native_training_retry=None, native_training_summary=summary,
                        native_training_round=None, candidates=candidates,
                        phase='select' if candidates else 'evaluate', observations=[], repair_error=message)
                updated = await owned_thread(self.commit, record, fallback)
                _log(SERVICE, 'Native CPU import failed; using local validation for this round: ' + message,
                     level='WARNING')
                return updated
            await owned_thread(self.commit, record, lambda row: row.update(
                native_training_retry={'round': row['round'], 'attempts': attempts,
                                       'retry_at': time.time() + 10 * 2 ** (attempts - 1),
                                       'error': message}, native_training_summary=summary))
            _log(SERVICE, 'Native CPU import will retry before local fallback: ' + message, level='WARNING')
            return
        if not summary['ranking_available']:
            # Qualitative or unavailable ranking criteria still use the model, never a local rerun.
            qualified = distinct([candidate for candidate in candidates
                                  if qualified_winner(candidate.get('native_training'))])
            from pb8_loop_native import TOP_COUNT
            decision = await self.decision(record, 'select', {'candidates': qualified,
                'max_candidates': min(TOP_COUNT, len(qualified)),
                'validation_budget': {'remaining': len(qualified), 'max_candidates': min(TOP_COUNT, len(qualified)),
                                      'source': 'vast_cpu_exact'},
                'native_training_summary': summary})
            chosen = decision.get('candidate_ids') or []
            if (len(chosen) != min(TOP_COUNT, len(qualified)) or len(set(chosen)) != len(chosen)
                    or any(value not in {candidate['id'] for candidate in qualified} for value in chosen)):
                raise ValueError('Select five distinct qualified native CPU candidates')
            selected = [candidate for candidate in qualified if candidate['id'] in chosen]
            summary.update(selected=len(selected), selection_reason=decision.get('reason'))
        jobs, observations = [], []
        for candidate in selected:
            candidate = copy.deepcopy(candidate)
            result = candidate.pop('native_training')
            config = await owned_thread(self.backend.validation_config, record, candidate)
            try:
                job = await owned_thread(self.job, record, 'validation', config, candidate['overrides'],
                    'Exact Vast CPU training result reused; no local backtest', candidate=candidate)
            except ValueError as exc:
                await owned_thread(self.commit, record, lambda row: row.update(native_training_retry={
                    'round': row['round'], 'attempts': retry.get('attempts', 0) + 1,
                    'manual_resume': True, 'error': str(exc)[:1000]}))
                raise LoopAIPause('native_training_safety', 'Native CPU candidate safety check failed: ' + str(exc)) from exc
            job.update(name='native_cpu_' + job['operation'][:8], status='complete',
                       native_reused=True, native_observation=result)
            jobs.append(job)
            observations.append({**result, 'operation': job['operation'], 'candidate': candidate, 'round': record['round']})
        def imported(row):
            if retry.get('round') == row['round']:
                for source in row['jobs']:
                    if source['kind'] == 'optimizer' and source['round'] == row['round']:
                        count = sum(candidate.get('optimizer_job') == source['operation'] for candidate in candidates)
                        if count:
                            source.update(results_consumed=True, result_count=count, result_error=None)
            # Retain full exact reports only for the five reusable checkpoints;
            # population evidence already contains native metric statistics.
            population = []
            for candidate in candidates:
                item = {key: value for key, value in candidate.items() if key != 'native_training'}
                if candidate.get('native_training'):
                    item['native_cpu_qualified'] = qualified_winner(candidate['native_training'])
                population.append(item)
            row.update(phase='evaluate', jobs=row['jobs'] + jobs, observations=observations,
                       candidates=population, native_training_round=row['round'], native_training_version=VERSION,
                       native_training_summary=summary, native_training_retry=None, repair_error=None,
                       candidate_selection={'round': row['round'], 'candidate_ids': [c['id'] for c in selected],
                                            'reason': 'Top five distinct qualified native CPU candidates by explained training criteria',
                                            'source': 'vast_cpu_exact', 'ranking': summary})
        await owned_thread(self.commit, record, imported)

    async def recover_validation(self, record):
        """Repair/retry native CPU validation without buying another optimizer round."""
        from ai_loop_search_limits import validation_budget
        recovery = record.get('validation_recovery') or {}
        kind = ('baseline' if record['phase'] == 'baseline' else
                recovery.get('job_kind', 'validation') if record['phase'] == 'recover_validation' else 'validation')
        if (recovery.get('round') != record['round'] or not recovery.get('active')
                or recovery.get('job_kind', 'validation') != kind):
            failures = [j.get('error') or j.get('report_error') for j in record['jobs']
                        if j['kind'] in {'validation', 'baseline'} and j.get('round') == record['round']
                        and (j.get('error') or j.get('report_error'))]
            cause = record.get('repair_error') or '; '.join(failures)[:1000] or 'No completed CPU training validation'
            recovery = {'round': record['round'], 'attempts': 0, 'retry_at': time.time() + 15,
                        'error': cause, 'history': [], 'active': True,
                        'job_kind': kind}
            await owned_thread(self.commit, record, lambda row: row.update(
                phase='recover_validation', validation_recovery=recovery, repair_error=cause, last_error=cause))
            return
        if time.time() < recovery['retry_at']:
            if record['phase'] != 'recover_validation':
                await owned_thread(self.commit, record, lambda row: row.update(phase='recover_validation'))
            return
        if validation_budget(record, 0)['remaining'] <= 0:
            await self.finish(record, 'Validation allowance reached during automatic recovery')
            return
        if kind == 'baseline':
            sources = [j for j in record['jobs'] if j['kind'] == 'baseline' and j.get('candidate')][-1:]
        else:
            sources = self.pending_validation_retries(record)
        sources = sources if recovery['attempts'] < 2 else []
        # One recovery attempt covers the failed batch, bounded by the original
        # CPU allowance. An unchanged probe follows repeated batch failures.
        sources = sources[:validation_budget(record, 0)['remaining']] or [None]
        jobs = []
        try:
            for source in sources:
                candidate = copy.deepcopy(source['candidate'] if source else
                             (record.get('best') or record.get('baseline') or {}).get('candidate') or {
                                 'id': 'recovery_probe', 'optimizer_job': None,
                                 'config': record['initial_config'], 'overrides': record['initial_overrides']})
                config = await owned_thread(self.backend.validation_config, record, candidate)
                job = await owned_thread(self.job, record, kind, config, candidate['overrides'],
                                         'Automatic CPU validation recovery', candidate=candidate)
                job.update(recovery_of=source['operation'] if source else None,
                           diagnostic_only=source is None if kind == 'validation' else False)
                jobs.append(job)
        except (OSError, ValueError) as exc:
            detail = f'{type(exc).__name__}: {str(exc)}'[:1000]
            _log(SERVICE, 'CPU validation preparation will retry automatically', level='WARNING', meta={'error': detail})
            def deferred(row):
                state = copy.deepcopy(recovery)
                state['attempts'] += 1
                state['retry_at'] = time.time() + min(300, 15 * 2 ** min(state['attempts'], 5))
                state['history'] = (state['history'] + [{'at': time.time(), 'error': detail}])[-10:]
                row.update(phase='recover_validation', validation_recovery=state, repair_error=detail, last_error=detail)
            await owned_thread(self.commit, record, deferred)
            return
        def queued(row):
            state = copy.deepcopy(recovery)
            state['attempts'] += 1
            state['history'] = (state['history'] + [{'at': time.time(), 'operations': [job['operation'] for job in jobs],
                                                     'error': row.get('repair_error') or state['error']}])[-10:]
            state['retry_at'] = time.time() + min(300, 15 * 2 ** min(state['attempts'], 5))
            row.update(phase='baseline' if kind == 'baseline' else 'validate', jobs=row['jobs'] + jobs, validation_recovery=state,
                       validation_count=row['validation_count'] + len(jobs))
        await owned_thread(self.commit, record, queued)

    async def _tick(self, record):
        from pb8_loop_gpu_replacement import open_replacement, GPUReplacement
        replacement = open_replacement(record)
        if replacement and record['status'] not in {'stopping', *TERMINAL}:
            from vast_queue import CloudQueue
            await owned_thread(GPUReplacement(self.store, CloudQueue(), self.backend).step, record)
            return
        if record['status'] in ACTIVE:
            try:
                round_limit(record['settings'])
            except ValueError as exc:
                raise LoopAIPause('round_limit', str(exc)) from exc
        if record.get('rental_extension'):
            from pb8_loop_rental_controls import reconcile_extension
            from vast_queue import CloudQueue
            record = await owned_thread(reconcile_extension, self.store, CloudQueue(), record)
        handover = record.get('rental_handover') or {}
        if handover.get('status') == 'collecting':
            if not await owned_thread(self.backend.handover_step, record):
                return
            record = await owned_thread(self.store.read, record['owner'], record['id'])
        if record['status'] == 'queued':
            return
        if record['status'] == 'stopping' or record['status'] in TERMINAL:
            if record['status'] in TERMINAL and record.get('cleanup_done'):
                return
            collecting = False
            for job in record['jobs']:
                if not job.get('backend') and job['status'] not in JOB_FINISHED:
                    def cancelled(current):
                        target = next(item for item in current['jobs'] if item['operation'] == job['operation'])
                        target.update(status='cancelled', ended_at=time.time())
                    await owned_thread(self.store.update, record['owner'], record['id'], cancelled)
                if job.get('backend') and job['status'] not in JOB_FINISHED:
                    await owned_thread(self.backend.stop, record, job)
                    state = await owned_thread(self.backend.poll, record, job)
                    def collected(current):
                        target = next(item for item in current['jobs'] if item['operation'] == job['operation'])
                        target['status'] = state['status']
                        if state['status'] in JOB_FINISHED and not target.get('ended_at'):
                            target['ended_at'] = time.time()
                        target['started'] = target['started'] or state['started']
                    await owned_thread(self.store.update, record['owner'], record['id'], collected)
                    collecting = collecting or state['status'] not in JOB_FINISHED
            await owned_thread(self.backend.cleanup, record)
            if record.get('gpu_replacements'):
                collecting = collecting or await owned_thread(self.backend.replacement_cleanup_pending, record)
            # Stop/poll can discover a start after the tick snapshot was read.
            # Settle reservations from the persisted evidence, never that stale snapshot.
            record = await owned_thread(self.store.read, record['owner'], record['id'])
            if (record.get('final_holdout_policy') == 'final_only' and record.get('holdout_used')
                    and not collecting and record.get('final_validation_status') not in {'passed', 'failed'}):
                evaluated = any(job.get('started') for job in record['jobs'] if job['kind'] == 'holdout')
                await owned_thread(self.store.settle_holdouts, record, evaluated)
            def cleaned(current):
                if current['status'] == 'stopping':
                    current['status'] = 'stopped'
                if (not collecting and current.get('final_holdout_policy') == 'final_only'
                        and current.get('final_validation_status') == 'running'):
                    current['final_validation_status'] = 'incomplete'
                current['cleanup_done'] = not collecting
                if not collecting:
                    from pb8_loop_gpu_replacement import open_replacement
                    operation = open_replacement(current)
                    if operation:
                        operation.update(step='aborted', reason='Loop stopped; replacement rentals are collected', ended_at=time.time())
            await owned_thread(self.store.update, record['owner'], record['id'], cleaned)
            return
        from ai_loop_windows import require_holdout_evidence_policy, filter_historical_candidates
        try:
            require_holdout_evidence_policy(record)
        except ValueError as exc:
            raise LoopAIPause('evidence_recovery', str(exc)) from exc
        if record['status'] in ACTIVE and record['phase'] == 'observer_confirmation':
            await self.finish(record, record.get('stop_reason', 'Training complete'))
            return
        if time.time() >= record['deadline'] - 60:
            await owned_thread(self.store.update, record['owner'], record['id'], lambda row: row.update(status='stopping', reason='Total time allowance reached', control_generation=row['control_generation'] + 1))
            return
        if record['status'] == 'paused':
            await self.sync_jobs(record, launch=False)
            return
        required_rounds = record.get('required_rounds', 0)
        max_rounds = round_limit(record['settings'])
        if type(required_rounds) is not int or not 0 <= required_rounds <= record['settings']['max_runs']:
            raise ValueError('Requested completed rounds must fit the authorized optimizer allowance')
        completed_rounds = {item['round'] for item in record['history'] if item.get('evaluated_at') is not None}
        if len(completed_rounds) >= max_rounds and record['phase'] not in {'holdout', 'observer_confirmation'}:
            await self.finish(record, 'Maximum completed rounds reached')
            return
        if record.get('pending_ai'):
            raise LoopAIPause('evidence_recovery', 'AI request was interrupted; its reservation is retained and no duplicate call was sent')
        if time.time() < (record.get('ai_retry') or {}).get('retry_at', 0):
            return
        if record.get('decision_repair', {}).get('active') and time.time() < record['decision_repair']['retry_at']:
            return
        if record.get('decision_repair', {}).get('active') and not record.get('repair_error'):
            record = await owned_thread(self.commit, record, lambda row: row['decision_repair'].update(active=False))
        phase = record['phase']
        from ai_loop_objectives import validate_template_rubric
        try:
            validate_template_rubric(record, record['initial_config'], record.get('rubric'))
        except ValueError as exc:
            raise LoopAIPause('objective_mismatch', str(exc)) from exc
        if phase == 'recover_validation':
            await self.recover_validation(record)
            return
        if phase == 'interpret':
            decision = await self.decision(record, 'interpret', {'metric_examples': ['gain', 'gain_strategy_eq', 'adg', 'adg_strategy_eq',
                                          'drawdown_worst', 'drawdown_worst_strategy_eq', 'n_fills', 'sharpe_ratio'],
                                          'repair_error': record.get('repair_error')})
            rubric = validate_rubric(decision.get('rubric'), record['settings']['goals'])
            from ai_loop_search_limits import exposure_ceiling
            if exposure_ceiling(record['settings']) is not None and (
                    not isinstance(decision.get('reason'), str) or not decision['reason'].strip()):
                raise ValueError('Explain the initial TWE search interval and any fixed-exposure hypothesis in reason')
            try:
                validate_template_rubric(record, record['initial_config'], rubric)
            except ValueError as exc:
                raise LoopAIPause('objective_mismatch', str(exc)) from exc
            candidates = (await owned_thread(self.backend.existing_candidates, {**record, 'rubric': rubric})
                          if record['settings'].get('historical_advice', True) else [])
            candidates = filter_historical_candidates(record, candidates)
            if candidates:
                sources = sorted({item['source']['run'] for item in candidates})
                await owned_thread(self.commit, record, lambda row: row.update(
                    rubric=rubric, phase='bootstrap', jobs=[], interpretation=decision.get('reason', ''),
                    errors=0, last_error=None, repair_error=None,
                    bootstrap_candidates=candidates,
                    bootstrap={'status': 'evaluating', 'sources': sources, 'candidate_count': len(candidates),
                               'candidates': [{key: item[key] for key in ('id', 'source', 'metrics')} for item in candidates]}))
            else:
                initial = await owned_thread(self.job, {**record, 'rubric': rubric}, 'optimizer', record['initial_config'], record['initial_overrides'], 'Initial optimizer run')
                await owned_thread(self.commit, record, lambda row: row.update(rubric=rubric, phase='optimize', jobs=[initial], interpretation=decision.get('reason', ''),
                    errors=0, last_error=None, repair_error=None,
                    bootstrap={'status': 'no_results', 'candidate_count': 0, 'sources': [], 'changes': [],
                               'reason': 'No usable saved optimizer results were found for this starting config.'}))
            return
        if record.get('baseline_required') and record.get('baseline') is None and phase in {'bootstrap', 'optimize'}:
            candidate = {'id': 'baseline', 'config': record['initial_config'], 'overrides': record['initial_overrides'], 'optimizer_job': None}
            baseline_record = {**record, 'round': -1}
            job = await owned_thread(self.job, baseline_record, 'baseline', self.backend.validation_config(record, candidate), candidate['overrides'], 'Unchanged starting config on fixed comparison windows', candidate=candidate)
            await owned_thread(self.commit, record, lambda row: row.update(phase='baseline', after_baseline=phase, jobs=row['jobs'] + [job], validation_count=row['validation_count'] + 1))
            return
        if phase == 'baseline':
            record = await self.sync_jobs(record)
            if record['status'] not in ACTIVE:
                return
            job = next(item for item in reversed(record['jobs']) if item['kind'] == 'baseline')
            if job['status'] not in JOB_FINISHED:
                return
            if job['status'] not in {'complete', 'completed'}:
                if record.get('optimizer_research_version') == 1:
                    await self.recover_validation(record)
                    return
                await owned_thread(self.commit, record, lambda row: row.update(status='failed', reason='Starting baseline backtest failed', last_error=job.get('error')))
                return
            try:
                result = await owned_thread(self.backend.observations, record, job)
            except (OSError, ValueError) as exc:
                if record.get('optimizer_research_version') != 1:
                    raise
                _log(SERVICE, 'Baseline report unavailable; retrying native CPU validation', level='WARNING', meta={'error_type': type(exc).__name__})
                record = await owned_thread(self.commit, record, lambda row: row.update(repair_error=f'{type(exc).__name__}: {str(exc)}'[:1000]))
                await self.recover_validation(record)
                return
            result.update(operation=job['operation'], candidate={'id': 'baseline', 'optimizer_job': None,
                          'config': copy.deepcopy(record['initial_config']),
                          'overrides': copy.deepcopy(record['initial_overrides'])})
            def baseline_ready(row):
                row.update(baseline=result, phase=row['after_baseline'])
                if row.get('validation_recovery', {}).get('active'):
                    row['validation_recovery']['active'] = False
                    row.update(repair_error=None, last_error=None, errors=0)
            await owned_thread(self.commit, record, baseline_ready)
            return
        if phase == 'bootstrap':
            settings = record['settings']
            maximum = min(settings['max_runs'], settings['parallel'], max(1, await owned_thread(self.backend.capacity, record)))
            evidence = {'existing_results': filter_historical_candidates(record, record['bootstrap_candidates']), 'max_variants': maximum,
                        'baseline': record.get('baseline'), 'exact': False, 'note': 'Historical optimizer guidance; current goals and fixed comparison still require new exact validation.',
                        'jev': record.get('bootstrap_jev_answers') or [], 'repair_error': record.get('repair_error')}
            if required_rounds:
                evidence.update(required_rounds=required_rounds,
                                required_rounds_remaining=max(0, required_rounds - len(completed_rounds)))
            try:
                decision = await self.decision(record, 'bootstrap', evidence)
            except BootstrapWithoutAdvice as fallback:
                initial = await owned_thread(self.job, record, 'optimizer', record['initial_config'], record['initial_overrides'],
                                   'Fresh optimization without historical advice')
                await owned_thread(self.commit, record, lambda row: row.update(phase='optimize', jobs=row['jobs'] + [initial],
                    bootstrap={**row.get('bootstrap', {}), 'status': 'skipped', 'evidence_preview': fallback.preview},
                    repair_error=None, errors=0))
                return
            if decision.get('jev_needed') and settings['jev_enabled'] and not record.get('bootstrap_jev_done'):
                if record.get('bootstrap_jev_attempted'):
                    answer = 'An earlier initial JEV request was interrupted; no duplicate was sent'
                else:
                    await owned_thread(self.commit, record, lambda row: row.update(bootstrap_jev_attempted=True))
                    try:
                        answer = await self.ai.jev(await owned_thread(self.store.read, record['owner'], record['id']),
                                                   [{'id': row['id'], 'metrics': row['metrics']} for row in record['bootstrap_candidates']])
                    except Exception as exc:
                        from ai_jev import OpenRouterDecisionError
                        detail = str(exc)[:500] if isinstance(exc, OpenRouterDecisionError) else type(exc).__name__
                        _log(SERVICE, f'Optional initial JEV failed: {detail}', level='WARNING')
                        answer = 'JEV request failed: ' + detail + '; primary model continues'
                answers = [answer or 'JEV unavailable; primary model continues']
                await owned_thread(self.commit, record, lambda row: row.update(bootstrap_jev_done=True, bootstrap_jev_answers=answers,
                                                           jev_answers=(row.get('jev_answers') or []) + answers))
                return
            variants = decision.get('variants')
            if not isinstance(variants, list) or not 1 <= len(variants) <= maximum:
                raise ValueError('Initial evaluation must provide adjusted configs within the run/parallel allowance')
            jobs = []
            for variant in variants:
                if not isinstance(variant, dict):
                    raise ValueError('Invalid initial optimizer variant')
                if record.get('optimizer_research_version') == 1:
                    from pb8_loop_research import resolve_variant
                    variant = resolve_variant(record, variant)
                config = variant.get('config')
                overrides = variant.get('overrides', record['initial_overrides'])
                validate_change(record, config, overrides)
                job = await owned_thread(self.job, {**record, 'jobs': record['jobs'] + jobs}, 'optimizer', config, overrides, variant.get('reason', decision.get('reason', '')), **({'experiment': variant.get('experiment')} if record.get('optimizer_research_version') == 1 else {}))
                if digest([fresh_optimizer_config(config, 0), overrides]) == digest([
                        fresh_optimizer_config(record['initial_config'], 0), record['initial_overrides']]):
                    raise ValueError('Use the saved results to adjust the config before the first optimizer run')
                jobs.append(job)
            summary = {**record['bootstrap'], 'status': 'applied', 'reason': str(decision.get('reason') or '')[:4000],
                       'knowledge': decision.get('knowledge') or '',
                       'changes': [{'operation': job['operation'], 'fields': config_changes(record['initial_config'], job['config'])} for job in jobs]}
            await self.remember(record, record['id'] + ':bootstrap',
                               {'coins': settings['goals'].get('coins', []), 'goals': settings['goals'],
                                'status': 'historical_optimizer_guidance', 'hypothesis': summary['reason'],
                                'knowledge': summary['knowledge'], 'sources': record['bootstrap']['sources'],
                                'changes': summary['changes'], 'uncertainty': 'Historical optimizer evidence is not a confirmed current improvement.'})
            if len(summary['knowledge']) > 1000:
                summary['knowledge_reference'] = {'evidence_id': record['id'] + ':bootstrap', 'characters': len(summary['knowledge'])}
                summary['knowledge'] = ''
            def applied(row):
                row.update(phase='optimize', jobs=row['jobs'] + jobs, bootstrap=summary, repair_error=None, last_error=None, errors=0)
                if row.get('decision_repair'):
                    row['decision_repair']['active'] = False
                row.pop('bootstrap_candidates', None)
            await owned_thread(self.commit, record, applied)
            return
        if phase in {'optimize', 'validate', 'holdout'}:
            record = await self.sync_jobs(record)
            if record['status'] not in ACTIVE:
                return
            kind = 'optimizer' if phase == 'optimize' else ('holdout' if phase == 'holdout' else 'validation')
            jobs = [job for job in record['jobs'] if job['round'] == record['round'] and job['kind'] == kind]
            if not jobs or any(job['status'] not in JOB_FINISHED for job in jobs):
                return
            if phase == 'optimize':
                candidates = []
                validated_sources = {item.get('candidate', {}).get('optimizer_job') for item in record['jobs']
                                     if item['kind'] == 'validation'}
                # Older controllers could skip usable results after a quota stop.
                # Recover them after the active round finishes, without interrupting it.
                missed = [item for item in record['jobs'] if item['kind'] == 'optimizer'
                          and item['round'] < record['round'] and item['status'] in JOB_FINISHED
                          and item.get('limit_reached') and not item.get('results_consumed')
                          and item['operation'] not in validated_sources]
                results = {}
                for job in jobs + missed:
                    from pb8_loop_gpu_replacement import replaced_predecessor
                    infrastructure = replaced_predecessor(record, job)
                    if (job['status'] in {'complete', 'completed'} or job.get('limit_reached')
                            or (job['status'] == 'failed' and job.get('backend') and job.get('started'))
                            or (infrastructure and job.get('backend') and job.get('started'))):
                        try:
                            found = await owned_thread(self.backend.candidates, record, job)
                            candidates.extend(found)
                            results[job['operation']] = {'results_consumed': True, 'result_count': len(found), 'result_error': None}
                        except ValueError as exc:
                            from pb8_loop_native import enabled as native_training
                            if not native_training(record) and not infrastructure and not job.get('limit_reached') and job['status'] != 'failed':
                                raise
                            _log(SERVICE, f'Stopped optimizer {job["operation"]} has no usable results: {exc}', level='WARNING')
                            results[job['operation']] = {'result_error': str(exc)[:1000]}
                def collected(row):
                    for item in row['jobs']:
                        if item['operation'] in results:
                            item.update(results[item['operation']])
                    from pb8_loop_native import enabled as native_training
                    if candidates or native_training(row):
                        row.update(phase='select', candidates=candidates, repair_error=None)
                    else:
                        from pb8_loop_gpu_replacement import replaced_predecessor
                        failures = [item['error'] for item in jobs if item.get('error') and not replaced_predecessor(row, item)]
                        failures.extend(values['result_error'] for item in jobs + missed
                            if not replaced_predecessor(row, item)
                            and (values := results.get(item['operation'], {})).get('result_error'))
                        if row.get('repair_error'):
                            failures.append(row['repair_error'])
                        row.update(phase='evaluate', observations=[],
                                   repair_error='; '.join(dict.fromkeys(failures))[:1000] or 'All optimizer attempts failed')
                await owned_thread(self.commit, record, collected)
                return
            observations = []
            for job in jobs:
                if job['status'] in {'complete', 'completed'}:
                    try:
                        result = await owned_thread(self.backend.observations, record, job)
                    except (OSError, ValueError) as exc:
                        if phase != 'validate' or record.get('optimizer_research_version') != 1:
                            raise
                        detail = f'{type(exc).__name__}: {str(exc)}'[:1000]
                        _log(SERVICE, 'CPU training report unavailable; automatic recovery scheduled', level='WARNING',
                             meta={'error': detail})
                        def unreadable(row):
                            target = next(j for j in row['jobs'] if j['operation'] == job['operation'])
                            target['report_error'] = detail
                            row.update(repair_error=detail, last_error=detail)
                        record = await owned_thread(self.commit, record, unreadable)
                        continue
                    observations.append({**result, 'candidate': job['candidate'], 'operation': job['operation'], 'round': job['round']})
            if phase == 'holdout':
                before_data = bool(not observations and all([await owned_thread(self.backend.failed_before_data, record, job) for job in jobs])) if hasattr(self.backend, 'failed_before_data') else False
                await owned_thread(self.store.settle_holdouts, record, not before_data)
                if before_data:
                    await owned_thread(self.commit, record, lambda row: row.update(status='unconfirmed', final_validation_status='failed', holdout_used=False, holdout_retryable=True, reason='Final holdout configuration failed before data evaluation; unchanged candidate may be retried', last_error=jobs[0].get('error')))
                    return
                confirmed = bool(record['fingerprint'].get('data') and observations and all(item['assessment']['achieved'] for item in observations))
                for window in record['holdouts']:
                    await self.remember(record, record['id'] + ':holdout:' + digest(window),
                                       {'holdout': window, 'status': 'independently_confirmed' if confirmed else 'unconfirmed',
                                        'coins': record['settings']['goals'].get('coins', []), 'observations': observations})
                await owned_thread(self.commit, record, lambda row: row.update(status='completed' if confirmed else 'unconfirmed',
                                                           final_validation_status='passed' if confirmed else 'failed',
                                                           reason='Final holdout passed' if confirmed else 'Final holdout failed or returned no exact observations',
                                                           final_validation=observations))
                return
            await owned_thread(self.commit, record, lambda row: row.update(phase='evaluate', observations=observations))
            return
        if phase == 'select':
            from pb8_loop_native import enabled as native_training
            if native_training(record):
                fallback = await self.import_native_training(record)
                if fallback is None or fallback['phase'] != 'select':
                    return
                record = fallback
            candidates = record['candidates']
            from ai_loop_search_limits import validation_budget
            budget = validation_budget(record, len(candidates))
            evidence = {'candidates': copy.deepcopy(candidates),
                        'loop_id': record['id'], 'round_number': record['round'] + 1,
                        'jobs': [optimizer_execution_evidence(job, record['settings'], record) for job in record['jobs']
                                 if job['kind'] == 'optimizer' and job['round'] == record['round']],
                        'max_candidates': budget['max_candidates'], 'validation_budget': budget,
                        'jev': record.get('jev_answers') if record.get('jev_round') == record['round'] else [],
                        'native_training_fallback': record.get('native_training_fallback')}
            if evidence['max_candidates'] <= 0:
                await self.finish(record, 'Validation allowance reached')
                return
            decision = await self.decision(record, 'select', evidence)
            chosen = decision.get('candidate_ids')
            if not isinstance(chosen, list) or not 1 <= len(chosen) <= evidence['max_candidates'] or len(set(chosen)) != len(chosen):
                raise ValueError('AI must select a bounded number of exact candidate IDs')
            selected = [row for row in candidates if row['id'] in chosen]
            if len(selected) != len(chosen):
                raise ValueError('AI selected an unknown candidate')
            if not isinstance(decision.get('reason'), str) or not decision['reason'].strip():
                raise ValueError('Explain candidate count, Pareto coverage and CPU budget allocation in reason')
            jev_answers = list(evidence['jev'] or [])
            if not jev_answers and decision.get('jev_needed') and record['settings']['jev_enabled']:
                for optimizer_job in {row['optimizer_job'] for row in candidates}:
                    group = [{'id': row['id'], 'metrics': row['metrics']} for row in candidates if row['optimizer_job'] == optimizer_job]
                    key = str(record['round']) + ':' + optimizer_job
                    current = await owned_thread(self.store.read, record['owner'], record['id'])
                    if key in current.get('jev_attempted', []):
                        jev_answers.append('An earlier JEV request was interrupted; no duplicate was sent')
                        continue
                    await owned_thread(self.commit, record, lambda row: row.update(jev_attempted=row.get('jev_attempted', []) + [key]))
                    try:
                        answer = await self.ai.jev(await owned_thread(self.store.read, record['owner'], record['id']), group)
                        if answer:
                            jev_answers.append(answer)
                    except Exception as exc:
                        from ai_jev import OpenRouterDecisionError
                        detail = str(exc)[:500] if isinstance(exc, OpenRouterDecisionError) else type(exc).__name__
                        _log(SERVICE, f'Optional JEV failed: {detail}', level='WARNING')
                        jev_answers.append('JEV request failed: ' + detail + '; primary model continues')
            if jev_answers and not evidence['jev']:
                record = await owned_thread(self.commit, record, lambda row: row.update(jev_answers=jev_answers, jev_round=row['round']))
                evidence['jev'] = jev_answers
                decision = await self.decision(record, 'select', evidence)
                chosen = decision.get('candidate_ids')
                if not isinstance(chosen, list) or not 1 <= len(chosen) <= evidence['max_candidates'] or len(set(chosen)) != len(chosen):
                    raise ValueError('AI must select bounded known candidates after JEV evaluation')
                selected = [row for row in candidates if row['id'] in chosen]
                if len(selected) != len(chosen):
                    raise ValueError('AI selected an unknown candidate after JEV evaluation')
                if not isinstance(decision.get('reason'), str) or not decision['reason'].strip():
                    raise ValueError('Explain the revised candidate count and CPU budget allocation in reason')
            jobs = [await owned_thread(self.job, record, 'validation', self.backend.validation_config(record, row), row['overrides'],
                             'Fixed comparison validation', candidate=row) for row in selected]
            await owned_thread(self.commit, record, lambda row: row.update(phase='validate', jobs=row['jobs'] + jobs,
                validation_count=row['validation_count'] + len(jobs), jev_answers=jev_answers,
                candidate_selection={'round': row['round'], 'candidate_ids': chosen,
                                     'reason': decision['reason'], 'budget': budget}))
            return
        if phase == 'evaluate':
            observations = record.get('observations') or []
            completed_checks = {job['operation'] for job in record['jobs'] if job['kind'] == 'validation'
                                and job['round'] == record['round'] and job['status'] in {'complete', 'completed'}}
            eligible = [item for item in observations if item['exact'] and item['assessment'].get('comparable', True)
                        and item['assessment'].get('simulation_complete') is True
                        and item.get('operation') in completed_checks
                        and not next((j.get('diagnostic_only') for j in record['jobs'] if j['operation'] == item.get('operation')), False)]
            from pb8_loop_research import enabled as research_enabled
            technical_success = any(o.get('exact') and o.get('per_report') for o in observations)
            recovery = record.get('validation_recovery') or {}
            pending_batch = (recovery.get('active') and recovery.get('job_kind', 'validation') == 'validation'
                             and recovery.get('round') == record['round'] and recovery.get('attempts', 0) < 2
                             and self.pending_validation_retries(record))
            native_round = record.get('native_training_version') == 1 and record.get('native_training_round') == record['round']
            if research_enabled(record) and not native_round and (not technical_success or pending_batch):
                await self.recover_validation(record)
                return
            if research_enabled(record) and record.get('validation_recovery', {}).get('active'):
                record = await owned_thread(self.commit, record, lambda row: row['validation_recovery'].update(active=False))
            if not eligible and not research_enabled(record):
                if observations:
                    await self.remember(record, record['id'] + ':rejected:' + str(record['round']), {'status': 'rejected_exact', 'goals': record['settings']['goals'], 'observations': observations, 'uncertainty': 'Incomplete or incomparable simulations cannot qualify a winner or authorize a next round.'})
                await owned_thread(self.commit, record, lambda row: row.update(rejected_observations=observations, status='failed',
                    reason=('Next cycle blocked: ' + record['repair_error']) if record.get('repair_error') else
                           'Next cycle blocked: no successful comparable validation backtests for this cycle',
                    last_error=record.get('repair_error') or 'Exact comparison backtests are required before continuing'))
                return
            goal_metrics = {metric for rule in record['rubric'] for metric in rule['metrics']}
            current_best = record.get('best')
            from ai_loop_windows import bound_plan
            adaptive = bound_plan(record['settings']) is not None
            training_candidates = copy.deepcopy(record.get('training_candidates') or {}) if adaptive else {}
            if adaptive:
                # Recover old exact training candidates only. Observer/legacy holdout jobs
                # never enter the adaptive comparison pool.
                for job in record['jobs']:
                    if (job['kind'] == 'validation' and job['status'] in {'complete', 'completed'}
                            and job['operation'] not in training_candidates
                            and job['operation'] not in completed_checks):
                        observation = await owned_thread(self.backend.observations, record, job)
                        observation.update(operation=job['operation'], candidate=copy.deepcopy(job['candidate']))
                        training_candidates[job['operation']] = observation
                training_candidates.update({item['operation']: copy.deepcopy(item) for item in eligible})
            options = {}
            for item in [*training_candidates.values(), record.get('baseline'), current_best, *eligible]:
                if qualified_winner(item):
                    operation = item.get('operation', 'baseline')
                    options[operation] = copy.deepcopy(item)
                    if item is record.get('baseline') and not item.get('candidate'):
                        options[operation]['candidate'] = {'id': 'baseline', 'optimizer_job': None,
                            'config': copy.deepcopy(record['initial_config']),
                            'overrides': copy.deepcopy(record['initial_overrides'])}
            settings = record['settings']
            optimizers = [job for job in record['jobs'] if job['kind'] == 'optimizer']
            from pb8_loop_gpu_replacement import attempts_used
            remaining = settings['max_runs'] - attempts_used(record)
            evidence = {'observations': observations, 'best': current_best, 'remaining_runs': remaining,
                        'loop_id': record['id'], 'round_number': record['round'] + 1,
                        'completed_rounds': len(completed_rounds),
                        'completed_rounds_after_evaluation': len(completed_rounds | {record['round']}),
                        'max_rounds': max_rounds,
                        'remaining_rounds': max(0, max_rounds - len(completed_rounds | {record['round']})),
                        'optimizer_attempts': sum(bool(job['started'] or job.get('attempt_charged')) for job in optimizers),
                        'cpu_validation_completed': len(completed_checks),
                        'cpu_validation_pending': sum(job['kind'] == 'validation' and job['round'] == record['round']
                                                      and job['status'] not in JOB_FINISHED for job in record['jobs']),
                        'winner_options': {key: {field: item.get(field) for field in ('metrics', 'assessment', 'per_report')}
                                           | {'candidate_id': item['candidate']['id']} for key, item in options.items()},
                        'free_slots': max(1, await owned_thread(self.backend.capacity, record)),
                        'baseline': record.get('baseline'), 'jev': record.get('jev_answers') or [], 'repair_error': record.get('repair_error'),
                        'candidate_selection': record.get('candidate_selection'),
                        'validation_recovery': record.get('validation_recovery'),
                        'native_training_summary': record.get('native_training_summary') if native_round else None,
                        'jobs': [optimizer_execution_evidence(job, settings, record) for job in optimizers if job['round'] == record['round']]}
            if native_round:
                evidence['native_population'] = record.get('candidates') or []
            if adaptive:
                common_metrics = None
                for option in options.values():
                    for report in option.get('per_report') or []:
                        available = set(report)
                        common_metrics = available if common_metrics is None else common_metrics & available
                evidence['evaluation_policy'] = {
                    'mode': 'adaptive', 'previous': record.get('evaluation_method'),
                    'criteria_format': {'metric': 'exact metric name', 'direction': 'min|max',
                                        'reducer': 'mean|min|max|median', 'reason': 'why this serves the user goal'},
                    'comparison_operations': list(options),
                    'available_metrics_in_every_report': sorted(common_metrics or []),
                    'instruction': 'Return evaluation_criteria and candidate_assessments for every winner option; '
                                   'all candidates are compared with the same criteria. Hard user limits remain binding.'}
            if required_rounds:
                evidence.update(required_rounds=required_rounds,
                    required_rounds_remaining=max(0, required_rounds - len(completed_rounds | {record['round']})))
            from pb8_loop_research import evidence as research_evidence, validate_assessment, trial_records
            if research_enabled(record):
                evidence['optimizer_research'] = research_evidence(record, observations)
            decision = await self.decision(record, 'evaluate', evidence)
            def training_report(operation):
                job = next(j for j in record['jobs'] if j['operation'] == operation and j['kind'] == 'validation'
                           and not j.get('observer_only') and not j.get('diagnostic_only'))
                return self.backend.observations(record, job)
            research_assessment = (await owned_thread(validate_assessment, record, observations,
                                   decision.get('optimizer_assessment'), report_loader=training_report, include_trials=True)
                                   if research_enabled(record) else None)
            research_trials = research_assessment.pop('_trials') if research_assessment is not None else None
            from ai_loop_evaluation import evaluation_comparison
            if adaptive and decision.get('evaluation_criteria') is None:
                raise ValueError('Adaptive evaluation requires explained evaluation_criteria and candidate_assessments')
            comparison = evaluation_comparison(decision, options) if adaptive else None
            if 'winner_operation' not in decision:
                raise ValueError('AI must select winner_operation from qualified exact evidence')
            selected_operation = decision['winner_operation']
            if selected_operation is None:
                if options:
                    raise ValueError('AI must retain or select a qualified exact winner')
                current_best = None
            elif not isinstance(selected_operation, str) or selected_operation not in options:
                raise ValueError('AI selected an unknown, incomplete or hard-target-violating winner')
            else:
                if not isinstance(decision.get('reason'), str) or not decision['reason'].strip():
                    raise ValueError('AI must explain its exact winner selection')
                current_best = copy.deepcopy(options[selected_operation])
                current_best.update(operation=selected_operation, ai_selected=True, selection_reason=decision['reason'])
            previous = record.get('best') or record.get('baseline') or {}
            improved = current_best is not None and selected_operation != previous.get('operation', 'baseline')
            # Validate before committing the evaluation, so repeated invalid proposals
            # cannot reset the repair counter or consume an optimizer attempt.
            next_stagnation = evaluated_stagnation(record, selected_operation)
            continuing = (len(completed_rounds | {record['round']}) < max_rounds and remaining > 0
                          and (len(completed_rounds | {record['round']}) < required_rounds
                               or (not decision.get('finish') and (research_enabled(record) or next_stagnation < settings['patience']))))
            if continuing:
                for index, variant in enumerate(decision.get('variants') or []):
                    if research_enabled(record):
                        from pb8_loop_research import resolve_variant
                        variant = resolve_variant(record, variant)
                        decision['variants'][index] = variant
                    if isinstance(variant, dict) and isinstance(variant.get('config'), dict):
                        validate_change(record, variant['config'], variant.get('overrides', record['initial_overrides']))
                        changes = validate_exploration(record, variant['config'], stagnation_after=next_stagnation)
                        if (changes or adaptive) and (not isinstance(variant.get('reason'), str) or not variant['reason'].strip()):
                            raise ValueError('Explain the search hypothesis in variant.reason')
            cycle_best = current_best
            entry = {'coins': settings['goals'].get('coins', []), 'hypothesis': decision.get('knowledge_hypothesis', ''),
                     'knowledge': decision.get('knowledge', ''), 'contradicts': decision.get('contradicts', []),
                     'goals': settings['goals'], 'comparison': record['comparison'], 'uncertainty': 'Exact comparison evidence; several simultaneous edits do not establish individual causality', 'changes': [{'operation': job['operation'], 'reason': job['reason'], 'config_digest': digest(job['config'])} for job in optimizers if job['round'] == record['round']], 'observations': observations,
                     'status': 'observed_exact' if eligible else 'hypothesis', 'improved': improved,
                     'evaluation_comparison': comparison,
                     'candidate_selection': record.get('candidate_selection'),
                     'controller_facts': {'winner_operation': selected_operation, 'previous_operation': previous.get('operation'),
                                          'incumbent_replaced': improved,
                                          'winner_metrics': {key: value for key, value in (current_best or {}).get('metrics', {}).items() if key in goal_metrics},
                                          'qualified': bool(current_best)}}
            await self.remember(record, record['id'] + ':round:' + str(record['round']), entry)
            def observed(row):
                if row.get('decision_repair'):
                    row['decision_repair']['active'] = False
                if research_assessment is not None:
                    row['optimizer_trials'] = research_trials
                    row['optimizer_assessment'] = research_assessment
                if adaptive:
                    row['training_candidates'] = training_candidates
                if comparison is not None:
                    row['evaluation_method'] = comparison['criteria']
                row['best'] = current_best
                already_observed = any(item['round'] == row['round'] for item in row['history'])
                if not already_observed:
                    row['stagnation'] = 0 if improved else row['stagnation'] + 1
                row['history'] = [item for item in row['history'] if item['round'] != row['round']]
                if not research_enabled(record) or any(not j.get('diagnostic_only') and j['operation'] in {o['operation'] for o in observations if o.get('exact') and o.get('per_report')}
                                                       for j in row['jobs'] if j['kind'] == 'validation'):
                    row.update(errors=0, last_error=None, repair_error=None)
                row['history'].append({'round': row['round'], 'reason': str(decision.get('reason', ''))[:4000],
                                       'evaluation_comparison': comparison,
                                       **({'boundary_responses': [copy.deepcopy(v.get('boundary_response', []))
                                                                 for v in decision.get('variants') or []]}
                                          if research_enabled(record) else {}),
                                       **({'optimizer_assessment': research_assessment} if research_assessment is not None else {}),
                                       'candidate_selection': row.get('candidate_selection'),
                                       'knowledge': decision.get('knowledge', '') if len(decision.get('knowledge', '')) <= 1000 else '',
                                       'knowledge_reference': {'evidence_id': record['id'] + ':round:' + str(record['round']), 'characters': len(decision.get('knowledge', ''))} if len(decision.get('knowledge', '')) > 1000 else None,
                                       'winner_operation': selected_operation,
                                       'winner_candidate_id': current_best['candidate']['id'] if current_best else None,
                                       'best_score': None, 'cycle_score': None,
                                       'assessment': copy.deepcopy(cycle_best['assessment']) if cycle_best else None,
                                       'metrics': {key: value for key, value in cycle_best['metrics'].items() if key in goal_metrics} if cycle_best else {},
                                       'observations': [{'operation': item['operation'], 'candidate_id': item['candidate']['id'],
                                                         'optimizer_job': item['candidate']['optimizer_job'],
                                                         'assessment': copy.deepcopy(item['assessment']),
                                                         'metrics': {key: value for key, value in item['metrics'].items() if key in goal_metrics}}
                                                        for item in eligible],
                                       'evaluated_at': time.time(),
                                       'changes': [{'operation': job['operation'], 'config_digest': digest(job['config']), 'executed_digest': job.get('executed_digest'), 'fields': config_changes(record['initial_config'], job['config'])} for job in optimizers if job['round'] == row['round']]})
            record = await owned_thread(self.commit, record, observed)
            if len({item['round'] for item in record['history'] if item.get('evaluated_at') is not None}) >= max_rounds:
                await owned_thread(self.commit, record, lambda row: row.update(stop_reason='Maximum completed rounds reached'))
                await self.finish(record, 'Maximum completed rounds reached')
                return
            must_continue = len({item['round'] for item in record['history'] if item.get('evaluated_at') is not None}) < required_rounds
            if remaining <= 0 or (not must_continue and (decision.get('finish') or (not research_enabled(record) and record['stagnation'] >= settings['patience']))):
                stop = ('Optimizer allowance reached' if remaining <= 0 else 'No improvement patience reached'
                        if not research_enabled(record) and record['stagnation'] >= settings['patience']
                        else 'AI concluded the search')
                record = await owned_thread(self.commit, record, lambda row: row.update(stop_reason=stop))
                await self.finish(record, str(decision.get('reason') or stop))
                return
            variants = decision.get('variants')
            maximum = min(remaining, settings['parallel'], evidence['free_slots'])
            if not isinstance(variants, list) or not 1 <= len(variants) <= maximum:
                if must_continue:
                    raise ValueError(f'The user explicitly requested {required_rounds} completed loops; '
                                     f'{len(record["history"])} are complete. Return finish=false and a complete next variant within the remaining allowance.')
                raise ValueError('AI must provide complete configurations within the remaining run/parallel allowance')
            jobs = []
            next_record = {**record, 'round': record['round'] + 1}
            for variant in variants:
                if record.get('optimizer_research_version') == 1:
                    from pb8_loop_research import resolve_variant
                    variant = resolve_variant(record, variant)
                config = variant.get('config')
                overrides = variant.get('overrides', record['initial_overrides'])
                validate_change(record, config, overrides)
                jobs.append(await owned_thread(self.job, next_record, 'optimizer', config, overrides, variant.get('reason', ''), **({'experiment': variant.get('experiment')} if record.get('optimizer_research_version') == 1 else {})))
                next_record = {**next_record, 'jobs': record['jobs'] + jobs}
            await owned_thread(self.commit, record, lambda row: row.update(round=next_record['round'], jobs=row['jobs'] + jobs,
                                                       phase='optimize', repair_error=None, last_error=None, errors=0))
            return
        raise ValueError('Unknown persisted loop phase')

    def _update_job_status(self, record, function):
        """Merge fresh job/control state without hydrating unrelated archived evidence."""
        partial = getattr(self.store, 'update_runtime_fields', None)
        if partial is None:
            return self.store.update(record['owner'], record['id'], function)
        changes = partial(record['owner'], record['id'], {'jobs'}, function)
        return {**record, **changes}

    async def sync_jobs(self, record, launch=True):
        """Reconcile persisted launch intent before allocating any additional job."""
        from pb8_loop_gpu_replacement import open_replacement
        if open_replacement(record):
            from pb8_loop_gpu_replacement import GPUReplacement
            return await owned_thread(GPUReplacement(self.store, None, self.backend).monitor_jobs, record)
        for job in record['jobs']:
            if record['phase'] == 'baseline' and job['kind'] != 'baseline':
                continue
            if job['status'] in JOB_FINISHED:
                continue
            current = await owned_thread(self._read_execution, record['owner'], record['id'])
            if current['status'] not in ACTIVE:
                launch = False
            if job['backend'] is None:
                if not launch:
                    continue
                try:
                    backend = await owned_thread(self.backend.prepare, current, job)
                except Exception as exc:
                    from fastapi import HTTPException
                    current = await owned_thread(self._read_execution, record['owner'], record['id'])
                    if current['status'] not in ACTIVE:
                        return current
                    if isinstance(exc, HTTPException) and exc.status_code == 409:
                        continue
                    if isinstance(exc, (HTTPException, ValueError)) or getattr(exc, 'status_code', 0) == 422:
                        def rejected(row):
                            target = next(item for item in row['jobs'] if item['operation'] == job['operation'])
                            target.update(status='failed', error=str(exc)[:1000], ended_at=time.time())
                            row['repair_error'] = str(exc)[:1000]
                        record = await owned_thread(self.store.update, record['owner'], record['id'], rejected)
                        continue
                    raise
                def prepared(row):
                    target = next(item for item in row['jobs'] if item['operation'] == job['operation'])
                    target.update(backend=backend, status='queued')
                    if target.get('research'):
                        target['research_verification'] = {'status': 'verified', 'boundary': 'native_preparation'}
                record = await owned_thread(self._update_job_status, record, prepared)
                job = next(item for item in record['jobs'] if item['operation'] == job['operation'])
            state = await owned_thread(self.backend.poll, record, job)
            if job.get('research') and state.get('executed_config'):
                from pb8_loop_research import execution_check
                try:
                    execution_check(job['config'], state['executed_config'], job['overrides'])
                except ValueError as exc:
                    _log(SERVICE, str(exc), level='ERROR')
                    if state['status'] not in JOB_FINISHED and state.get('started'):
                        await owned_thread(self.backend.stop, record, job)
                    state.update(status='failed', error=str(exc), research_mismatch=True)
            polled_proxy = state.get('proxy_evaluations')
            source = state.get('source') or {}
            started_at = job.get('run_started_at') or source.get('started_at')
            if started_at is None and state['status'] == 'running' and state['started']:
                started_at = time.time()
            mode = current['settings'].get('run_limit_mode', 'config')
            hit = None
            if job['kind'] == 'optimizer' and started_at is not None:
                if mode == 'hours' and state['status'] not in JOB_FINISHED and time.time() >= started_at + current['settings']['run_hours'] * 3600:
                    hit = 'hours'
                if mode == 'proxy' and state['status'] != 'failed' and (state.get('proxy_evaluations') or 0) >= (job.get('research', {}).get('budget', {}).get('value') or current['settings']['run_proxy']):
                    hit = 'proxy'
            if hit and not job.get('limit_reached'):
                def limited(row):
                    target = next(item for item in row['jobs'] if item['operation'] == job['operation'])
                    target.update(limit_reached=hit, run_started_at=started_at)
                current = await owned_thread(self._update_job_status, record, limited)
                job = next(item for item in current['jobs'] if item['operation'] == job['operation'])
            if (hit or job.get('limit_reached')) and state['status'] not in JOB_FINISHED:
                await owned_thread(self.backend.stop, current, job)
                state = await owned_thread(self.backend.poll, current, job)
            if launch and state['status'] == 'queued' and not job.get('limit_reached'):

                if job['kind'] == 'optimizer':
                    capacity_record = copy.deepcopy(record)
                    capacity_record['initial_config'] = job['config']
                    capacity = await owned_thread(self.backend.capacity, capacity_record)
                else:
                    local = copy.deepcopy(record)
                    local['settings']['execution'] = 'cpu'
                    local['initial_config'].setdefault('optimize', {})['n_cpus'] = 1
                    capacity = await owned_thread(self.backend.capacity, local)
                if capacity > 0:
                    try:
                        state = await owned_thread(self.backend.start, await owned_thread(self._read_execution, record['owner'], record['id']), job)
                    except Exception as exc:
                        from fastapi import HTTPException
                        current = await owned_thread(self._read_execution, record['owner'], record['id'])
                        if current['status'] not in ACTIVE:
                            return current
                        if isinstance(exc, HTTPException) and exc.status_code == 409:
                            continue
                        if isinstance(exc, (HTTPException, ValueError)):
                            await owned_thread(self.backend.stop, current, job)
                            state = {'status': 'failed', 'started': state['started']}
                            await owned_thread(self.store.update, record['owner'], record['id'], lambda row: row.update(repair_error=str(exc)[:1000]))
                        else:
                            raise
            def synced(row):
                target = next(item for item in row['jobs'] if item['operation'] == job['operation'])
                target['status'] = state['status']
                target['started'] = target['started'] or state['started']
                proxies = [value for value in (polled_proxy, state.get('proxy_evaluations'))
                           if type(value) is int and value >= 0]
                if job['kind'] == 'optimizer' and proxies:
                    target['proxy_evaluations'] = max(target.get('proxy_evaluations') or 0, *proxies)
                if state.get('research_mismatch'):
                    target['research_verification'] = {'status': 'mismatch'}
                    target['error'] = state['error']
                if target.get('research') and state.get('search_progress'):
                    from pb8_loop_research import progress_sample
                    progress_sample(target, state['search_progress'])
                if state.get('executed_config'):
                    target['executed_config'] = state['executed_config']
                    target['executed_digest'] = digest(state['executed_config'])
                if started_at is not None:
                    target['run_started_at'] = started_at
                elif state['status'] == 'running' and state['started']:
                    target['run_started_at'] = (state.get('source') or {}).get('started_at') or time.time()
                if state['status'] in JOB_FINISHED and not target.get('ended_at'):
                    target['ended_at'] = time.time()
                native_error = state.get('error') or (state.get('source') or {}).get('error')
                if native_error:
                    target['error'] = str(native_error)[:1000]
                elif state['status'] in {'complete', 'completed', 'running'} or target.get('limit_reached'):
                    if target.get('error'):
                        target['historical_error'] = target['error']
                    target['error'] = None
                target['last_observed_at'] = time.time()
                target['runner'] = {key: state.get('source', {}).get(key) for key in ('started_at', 'pb8_version', 'pb8_commit', 'lease_id', 'dispatch_at', 'deadline', 'rental_state', 'error')}
            record = await owned_thread(self._update_job_status, record, synced)
        return record

    async def finish(self, record, reason):
        """Finish training without waiting for or consuming user-only backtests."""
        await owned_thread(self.commit, record, lambda row: row.update(
            status='completed' if row.get('best') else 'unconfirmed', reason=reason,
            final_validation_status='observer_only' if row.get('observer_holdouts') else 'not_requested'))

    async def confirm_observer(self, record):
        """Compatibility entry point; user reports cannot gate training completion."""
        await self.finish(record, record.get('stop_reason', 'Training complete'))
