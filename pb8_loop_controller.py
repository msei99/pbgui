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
from pb8_loop_store import ACTIVE, TERMINAL, LoopStore, LoopDeletionConflict, digest, validate_change, config_changes, apply_optimizer_start
from pb8_loop_ai import LoopAI, LoopAIRetryScheduled, LoopAIRequestFailed, validate_rubric
from pb8_loop_backend import LoopBackend
from pb8_loop_observer import ObserverJobs

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


async def owned_thread(function, *args):
    """Join a started filesystem/process operation before honoring task cancellation."""
    task = asyncio.create_task(asyncio.to_thread(function, *args))
    try:
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
        self._closing = False
        self._active_decisions = set()
        self.restart_pending = lambda: False

    def start(self):
        if self._task is None or self._task.done():
            self._closing = False
            self._task = asyncio.create_task(self._run(), name='pb8-loop-controller')

    async def close(self):
        self._closing = True
        tasks = [task for task in [self._task, *self._steps.values()] if task is not None]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._steps.clear()
        self._task = None

    async def create(self, owner, settings, bundle=None, queued=False):
        await self.ai.preflight(owner, settings)
        args = (settings['config_name'], settings) if bundle is None else (settings['config_name'], settings, bundle)
        config, overrides, fingerprint, holdouts, comparison = await owned_thread(self.backend.initial, *args)
        exclusions = copy.deepcopy(holdouts)
        known = self.store.knowledge(owner)['used_holdouts']
        def used(window):
            return any(isinstance(previous, dict) and previous.get('start_date', '') <= window['end_date']
                       and window['start_date'] <= previous.get('end_date', '') for previous in known)
        holdouts = [window for window in holdouts if not used(window)]
        validate_change({'settings': settings, 'initial_config': config, 'initial_overrides': overrides, 'holdouts': holdouts, 'training_exclusions': exclusions}, config, overrides)
        record = self.store.create(owner, settings, config, overrides, fingerprint, holdouts, comparison, queued=queued, training_exclusions=exclusions)
        return record

    def action(self, owner, loop_id, action):
        def change(record):
            if record['status'] in TERMINAL:
                raise ValueError('The loop has already ended')
            if action == 'start':
                if record['status'] != 'queued':
                    raise ValueError('Only a queued loop can be started')
                record.update(status='running', started_at=time.time(), deadline=time.time() + record['settings']['hours'] * 3600)
            elif action == 'pause':
                if record['status'] not in ACTIVE:
                    raise ValueError('Only an active loop can be paused')
                record['status'] = 'paused'
            elif action == 'resume':
                if record['status'] != 'paused':
                    raise ValueError('Only a paused loop can be resumed')
                record['status'] = 'finishing' if record['phase'] == 'holdout' else 'running'
            elif action == 'stop':
                record['status'] = 'stopping'
                record['reason'] = 'Stopped by the user'
            else:
                raise ValueError('Unknown loop action')
            record['control_generation'] += 1
        return self.store.update(owner, loop_id, change)

    def delete(self, owner, loop_id):
        """Exclude concurrent controller work while deleting a collected owned run."""
        path = self.store.path(owner, loop_id).with_suffix('.controller')
        if path.is_symlink():
            raise ValueError('Loop controller storage must not contain symlinks')
        fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise LoopDeletionConflict('This loop is still processing. Wait for collection to finish before deleting it.') from exc
            self.store.delete(owner, loop_id)
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    async def _run(self):
        while not self._closing:
            try:
                records = await owned_thread(self.store.list)
                for record in records:
                    key = record['id']
                    if record['status'] in TERMINAL and record.get('cleanup_done') and not self.observers.pending(record):
                        continue
                    task = self._steps.get(key)
                    if task is None or task.done():
                        if task and not task.cancelled():
                            task.exception()
                        self._steps[key] = asyncio.create_task(self.tick(record['owner'], key), name='pb8-loop-' + key)
                for key in list(self._steps):
                    if self._steps[key].done():
                        if not self._steps[key].cancelled():
                            self._steps[key].exception()
                        self._steps.pop(key)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                _log(SERVICE, f'Loop scan failed: {type(exc).__name__}', level='ERROR', meta={'traceback': traceback.format_exc()})
            await asyncio.sleep(2)

    async def tick(self, owner, loop_id):
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
                record = self.store.read(owner, loop_id)
            except FileNotFoundError:
                # A queued/finished run may have been deleted after the scan.
                return
            try:
                await self._tick(record)
            except asyncio.CancelledError:
                raise
            except LoopAIRetryScheduled:
                # The cooldown is persisted; the normal scan resumes it when due.
                pass
            except LoopRestartPending:
                # Nothing was sent/reserved; the new controller resumes this phase.
                pass
            except Exception as exc:
                def failed(current):
                    if current['status'] not in ACTIVE:
                        return
                    current['errors'] += 1
                    current['last_error'] = (str(exc).strip() or type(exc).__name__)[:1000]
                    # Managed transient requests have their own bounded retry
                    # policy. An interrupted or exhausted call cannot restart it.
                    if isinstance(exc, LoopAIRequestFailed) or current.get('pending_ai') or current['errors'] >= 3:
                        current['status'] = 'failed'
                        current['reason'] = current['last_error']
                        current['failure'] = failure_details(current, type(exc).__name__)
                        if current['phase'] == 'bootstrap' and current.get('bootstrap'):
                            current['bootstrap'].update(status='failed', reason=current['reason'])
                    else:
                        current.pop('last_ai_decision', None)
                        current['repair_error'] = str(exc)[:1000]
                updated = self.store.update(owner, loop_id, failed)
                _log(SERVICE, f"Loop {loop_id}, phase {record['phase']}, round {record['round'] + 1}: "
                     f'{type(exc).__name__}: {str(exc).strip() or type(exc).__name__}', level='ERROR',
                     meta={'failure': updated.get('failure'), 'traceback': traceback.format_exc()})
            # Separate failure boundary: user-only diagnostics never change the Loop's decisions/errors.
            try:
                await self.observers.tick(self.store.read(owner, loop_id))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                _log(SERVICE, f'User-only evaluation for {loop_id}: {type(exc).__name__}', level='WARNING',
                     meta={'traceback': traceback.format_exc()})
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def commit(self, record, function):
        """Reject decisions made before a newer user pause/stop action."""
        def guarded(current):
            if current['control_generation'] != record['control_generation'] or current['status'] not in ACTIVE:
                raise ValueError('A newer pause or stop invalidated the pending transition')
            function(current)
            if current['status'] == 'failed':
                current['failure'] = failure_details(current)
        return self.store.update(record['owner'], record['id'], guarded)

    async def decision(self, record, stage, evidence):
        previous = record.get('last_ai_decision') or {}
        if previous.get('stage') == stage and previous.get('round') == record['round'] and previous.get('evidence_digest') == digest(evidence):
            return previous['decision']
        if self.restart_pending():
            raise LoopRestartPending()
        token = uuid.uuid4().hex
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

    def job(self, record, kind, config, overrides, reason, candidate=None):
        """Snapshot a job, seeding only new optimizers from retained exact evidence."""
        token = uuid.uuid4().hex
        start = None
        if kind == 'optimizer':
            config, overrides, start = apply_optimizer_start(record, config, overrides)
        return {'operation': token, 'name': 'loop_' + record['id'][:8] + '_' + token[:8],
                'kind': kind, 'round': record['round'], 'config': copy.deepcopy(config),
                'overrides': copy.deepcopy(overrides), 'reason': str(reason)[:4000],
                'optimizer_start': start,
                'candidate': candidate, 'status': 'planned', 'backend': None, 'started': False, 'created_at': time.time()}

    async def _tick(self, record):
        if record['status'] == 'queued':
            return
        if record['status'] == 'stopping' or record['status'] in TERMINAL:
            collecting = False
            for job in record['jobs']:
                if job.get('backend') and job['status'] not in JOB_FINISHED:
                    await owned_thread(self.backend.stop, record, job)
                    state = await owned_thread(self.backend.poll, record, job)
                    def collected(current):
                        target = next(item for item in current['jobs'] if item['operation'] == job['operation'])
                        target['status'] = state['status']
                        if state['status'] in JOB_FINISHED and not target.get('ended_at'):
                            target['ended_at'] = time.time()
                        target['started'] = target['started'] or state['started']
                    self.store.update(record['owner'], record['id'], collected)
                    collecting = collecting or state['status'] not in JOB_FINISHED
            await owned_thread(self.backend.cleanup, record)
            def cleaned(current):
                if current['status'] == 'stopping':
                    current['status'] = 'stopped'
                current['cleanup_done'] = not collecting
            self.store.update(record['owner'], record['id'], cleaned)
            return
        if record['status'] in ACTIVE and record['phase'] == 'observer_confirmation':
            await self.confirm_observer(record)
            return
        if time.time() >= record['deadline'] - 60:
            self.store.update(record['owner'], record['id'], lambda row: row.update(status='stopping', reason='Total time allowance reached', control_generation=row['control_generation'] + 1))
            return
        if record['status'] == 'paused':
            await self.sync_jobs(record, launch=False)
            return
        required_rounds = record.get('required_rounds', 0)
        if type(required_rounds) is not int or not 0 <= required_rounds <= record['settings']['max_runs']:
            raise ValueError('Requested completed rounds must fit the authorized optimizer allowance')
        completed_rounds = {item['round'] for item in record['history'] if item.get('best_score') is not None}
        if record.get('pending_ai'):
            raise ValueError('AI request was interrupted; its reservation is retained and no duplicate call was sent')
        if time.time() < (record.get('ai_retry') or {}).get('retry_at', 0):
            return
        phase = record['phase']
        if phase == 'interpret':
            decision = await self.decision(record, 'interpret', {'metric_examples': ['gain', 'gain_strategy_eq', 'adg', 'adg_strategy_eq',
                                          'drawdown_worst', 'drawdown_worst_strategy_eq', 'n_fills', 'sharpe_ratio'],
                                          'repair_error': record.get('repair_error')})
            rubric = validate_rubric(decision.get('rubric'), record['settings']['goals'])
            candidates = await owned_thread(self.backend.existing_candidates, {**record, 'rubric': rubric})
            if candidates:
                sources = sorted({item['source']['run'] for item in candidates})
                self.commit(record, lambda row: row.update(
                    rubric=rubric, phase='bootstrap', jobs=[], interpretation=decision.get('reason', ''),
                    errors=0, last_error=None, repair_error=None,
                    bootstrap_candidates=candidates,
                    bootstrap={'status': 'evaluating', 'sources': sources, 'candidate_count': len(candidates),
                               'candidates': [{key: item[key] for key in ('id', 'source', 'metrics')} for item in candidates]}))
            else:
                initial = self.job(record, 'optimizer', record['initial_config'], record['initial_overrides'], 'Initial optimizer run')
                self.commit(record, lambda row: row.update(rubric=rubric, phase='optimize', jobs=[initial], interpretation=decision.get('reason', ''),
                    errors=0, last_error=None, repair_error=None,
                    bootstrap={'status': 'no_results', 'candidate_count': 0, 'sources': [], 'changes': [],
                               'reason': 'No usable saved optimizer results were found for this starting config.'}))
            return
        if record.get('baseline_required') and record.get('baseline') is None and phase in {'bootstrap', 'optimize'}:
            candidate = {'id': 'baseline', 'config': record['initial_config'], 'overrides': record['initial_overrides'], 'optimizer_job': None}
            baseline_record = {**record, 'round': -1}
            job = self.job(baseline_record, 'baseline', self.backend.validation_config(record, candidate), candidate['overrides'], 'Unchanged starting config on fixed comparison windows', candidate=candidate)
            self.commit(record, lambda row: row.update(phase='baseline', after_baseline=phase, jobs=row['jobs'] + [job], validation_count=row['validation_count'] + 1))
            return
        if phase == 'baseline':
            record = await self.sync_jobs(record)
            if record['status'] not in ACTIVE:
                return
            job = next(item for item in record['jobs'] if item['kind'] == 'baseline')
            if job['status'] not in JOB_FINISHED:
                return
            if job['status'] not in {'complete', 'completed'}:
                self.commit(record, lambda row: row.update(status='failed', reason='Starting baseline backtest failed', last_error=job.get('error')))
                return
            result = await owned_thread(self.backend.observations, record, job)
            self.commit(record, lambda row: row.update(baseline=result, phase=row['after_baseline']))
            return
        if phase == 'bootstrap':
            settings = record['settings']
            maximum = min(settings['max_runs'], settings['parallel'], max(1, await owned_thread(self.backend.capacity, record)))
            evidence = {'existing_results': record['bootstrap_candidates'], 'max_variants': maximum,
                        'baseline': record.get('baseline'), 'exact': False, 'note': 'Historical optimizer guidance; current goals and fixed comparison still require new exact validation.',
                        'jev': record.get('bootstrap_jev_answers') or [], 'repair_error': record.get('repair_error')}
            if required_rounds:
                evidence.update(required_rounds=required_rounds,
                                required_rounds_remaining=max(0, required_rounds - len(completed_rounds)))
            decision = await self.decision(record, 'bootstrap', evidence)
            if decision.get('jev_needed') and settings['jev_enabled'] and not record.get('bootstrap_jev_done'):
                if record.get('bootstrap_jev_attempted'):
                    answer = 'An earlier initial JEV request was interrupted; no duplicate was sent'
                else:
                    self.commit(record, lambda row: row.update(bootstrap_jev_attempted=True))
                    try:
                        answer = await self.ai.jev(self.store.read(record['owner'], record['id']),
                                                   [{'id': row['id'], 'metrics': row['metrics']} for row in record['bootstrap_candidates']])
                    except Exception as exc:
                        from ai_openrouter import OpenRouterDecisionError
                        detail = str(exc)[:500] if isinstance(exc, OpenRouterDecisionError) else type(exc).__name__
                        _log(SERVICE, f'Optional initial JEV failed: {detail}', level='WARNING')
                        answer = 'JEV request failed: ' + detail + '; primary model continues'
                answers = [answer or 'JEV unavailable; primary model continues']
                self.commit(record, lambda row: row.update(bootstrap_jev_done=True, bootstrap_jev_answers=answers,
                                                           jev_answers=(row.get('jev_answers') or []) + answers))
                return
            variants = decision.get('variants')
            if not isinstance(variants, list) or not 1 <= len(variants) <= maximum:
                raise ValueError('Initial evaluation must provide adjusted configs within the run/parallel allowance')
            jobs = []
            for variant in variants:
                if not isinstance(variant, dict):
                    raise ValueError('Invalid initial optimizer variant')
                config = variant.get('config')
                overrides = variant.get('overrides', record['initial_overrides'])
                validate_change(record, config, overrides)
                job = self.job(record, 'optimizer', config, overrides, variant.get('reason', decision.get('reason', '')))
                if digest([job['config'], overrides]) == digest([record['initial_config'], record['initial_overrides']]):
                    raise ValueError('Use the saved results to adjust the config before the first optimizer run')
                jobs.append(job)
            summary = {**record['bootstrap'], 'status': 'applied', 'reason': str(decision.get('reason') or '')[:4000],
                       'knowledge': str(decision.get('knowledge') or '')[:4000],
                       'changes': [{'operation': job['operation'], 'fields': config_changes(record['initial_config'], job['config'])} for job in jobs]}
            await owned_thread(self.store.remember, record, record['id'] + ':bootstrap',
                               {'coins': settings['goals'].get('coins', []), 'goals': settings['goals'],
                                'status': 'historical_optimizer_guidance', 'hypothesis': summary['reason'],
                                'knowledge': summary['knowledge'], 'sources': record['bootstrap']['sources'],
                                'changes': summary['changes'], 'uncertainty': 'Historical optimizer evidence is not a confirmed current improvement.'})
            def applied(row):
                row.update(phase='optimize', jobs=row['jobs'] + jobs, bootstrap=summary, repair_error=None, last_error=None, errors=0)
                row.pop('bootstrap_candidates', None)
            self.commit(record, applied)
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
                    if (job['status'] in {'complete', 'completed'} or job.get('limit_reached')
                            or (job['status'] == 'failed' and job.get('backend'))):
                        try:
                            found = await owned_thread(self.backend.candidates, record, job)
                            candidates.extend(found)
                            results[job['operation']] = {'results_consumed': True, 'result_count': len(found), 'result_error': None}
                        except ValueError as exc:
                            if not job.get('limit_reached') and job['status'] != 'failed':
                                raise
                            _log(SERVICE, f'Stopped optimizer {job["operation"]} has no usable results: {exc}', level='WARNING')
                            results[job['operation']] = {'result_error': str(exc)[:1000]}
                def collected(row):
                    for item in row['jobs']:
                        if item['operation'] in results:
                            item.update(results[item['operation']])
                    if candidates:
                        row.update(phase='select', candidates=candidates, repair_error=None)
                    else:
                        failures = [item['result_error'] for item in results.values() if item.get('result_error')]
                        failures.extend(item['error'] for item in jobs if item.get('error'))
                        if row.get('repair_error'):
                            failures.append(row['repair_error'])
                        row.update(phase='evaluate', observations=[],
                                   repair_error='; '.join(dict.fromkeys(failures))[:1000] or 'All optimizer attempts failed')
                self.commit(record, collected)
                return
            observations = []
            for job in jobs:
                if job['status'] in {'complete', 'completed'}:
                    result = await owned_thread(self.backend.observations, record, job)
                    observations.append({**result, 'candidate': job['candidate'], 'operation': job['operation']})
            if phase == 'holdout':
                before_data = bool(not observations and all([await owned_thread(self.backend.failed_before_data, record, job) for job in jobs])) if hasattr(self.backend, 'failed_before_data') else False
                await owned_thread(self.store.settle_holdouts, record, not before_data)
                if before_data:
                    self.commit(record, lambda row: row.update(status='unconfirmed', holdout_used=False, holdout_retryable=True, reason='Final holdout configuration failed before data evaluation; unchanged candidate may be retried', last_error=jobs[0].get('error')))
                    return
                confirmed = bool(record['fingerprint'].get('data') and observations and all(item['assessment']['achieved'] for item in observations))
                for window in record['holdouts']:
                    await owned_thread(self.store.remember, record, record['id'] + ':holdout:' + digest(window),
                                       {'holdout': window, 'status': 'independently_confirmed' if confirmed else 'unconfirmed',
                                        'coins': record['settings']['goals'].get('coins', []), 'observations': observations})
                self.commit(record, lambda row: row.update(status='completed' if confirmed else 'unconfirmed',
                                                           reason='Final holdout passed' if confirmed else 'Final holdout failed or returned no exact observations',
                                                           final_validation=observations))
                return
            self.commit(record, lambda row: row.update(phase='evaluate', observations=observations))
            return
        if phase == 'select':
            candidates = record['candidates']
            evidence = {'candidates': [{'id': row['id'], 'optimizer_job': row['optimizer_job'], 'metrics': row['metrics']} for row in candidates],
                        'max_candidates': min(record['settings']['candidates'], record['settings']['max_validations'] - record['validation_count']),
                        'jev': record.get('jev_answers') if record.get('jev_round') == record['round'] else []}
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
            jev_answers = list(evidence['jev'] or [])
            if not jev_answers and decision.get('jev_needed') and record['settings']['jev_enabled']:
                for optimizer_job in {row['optimizer_job'] for row in candidates}:
                    group = [{'id': row['id'], 'metrics': row['metrics']} for row in candidates if row['optimizer_job'] == optimizer_job]
                    key = str(record['round']) + ':' + optimizer_job
                    current = self.store.read(record['owner'], record['id'])
                    if key in current.get('jev_attempted', []):
                        jev_answers.append('An earlier JEV request was interrupted; no duplicate was sent')
                        continue
                    self.commit(record, lambda row: row.update(jev_attempted=row.get('jev_attempted', []) + [key]))
                    try:
                        answer = await self.ai.jev(self.store.read(record['owner'], record['id']), group)
                        if answer:
                            jev_answers.append(answer)
                    except Exception as exc:
                        from ai_openrouter import OpenRouterDecisionError
                        detail = str(exc)[:500] if isinstance(exc, OpenRouterDecisionError) else type(exc).__name__
                        _log(SERVICE, f'Optional JEV failed: {detail}', level='WARNING')
                        jev_answers.append('JEV request failed: ' + detail + '; primary model continues')
            if jev_answers and not evidence['jev']:
                record = self.commit(record, lambda row: row.update(jev_answers=jev_answers, jev_round=row['round']))
                evidence['jev'] = jev_answers
                decision = await self.decision(record, 'select', evidence)
                chosen = decision.get('candidate_ids')
                if not isinstance(chosen, list) or not 1 <= len(chosen) <= evidence['max_candidates'] or len(set(chosen)) != len(chosen):
                    raise ValueError('AI must select bounded known candidates after JEV evaluation')
                selected = [row for row in candidates if row['id'] in chosen]
                if len(selected) != len(chosen):
                    raise ValueError('AI selected an unknown candidate after JEV evaluation')
            jobs = [self.job(record, 'validation', self.backend.validation_config(record, row), row['overrides'],
                             'Fixed comparison validation', candidate=row) for row in selected]
            self.commit(record, lambda row: row.update(phase='validate', jobs=row['jobs'] + jobs,
                                                       validation_count=row['validation_count'] + len(jobs), jev_answers=jev_answers))
            return
        if phase == 'evaluate':
            observations = record.get('observations') or []
            completed_checks = {job['operation'] for job in record['jobs'] if job['kind'] == 'validation'
                                and job['round'] == record['round'] and job['status'] in {'complete', 'completed'}}
            eligible = [item for item in observations if item['exact'] and item['assessment'].get('comparable', True)
                        and item.get('operation') in completed_checks]
            if not eligible:
                if observations:
                    await owned_thread(self.store.remember, record, record['id'] + ':rejected:' + str(record['round']), {'status': 'rejected_exact', 'goals': record['settings']['goals'], 'observations': observations, 'uncertainty': 'Incomplete or incomparable simulations cannot qualify a winner or authorize a next round.'})
                self.commit(record, lambda row: row.update(rejected_observations=observations, status='failed',
                    reason=('Next cycle blocked: ' + record['repair_error']) if record.get('repair_error') else
                           'Next cycle blocked: no successful comparable validation backtests for this cycle',
                    last_error=record.get('repair_error') or 'Exact comparison backtests are required before continuing'))
                return
            rank = lambda item: (item['assessment'].get('hard_targets_met', True), item['assessment']['score'])
            cycle_best = max(eligible, key=rank)
            goal_metrics = {metric for rule in record['rubric'] for metric in rule['metrics']}
            current_best = record.get('best')
            for item in eligible:
                if current_best is None or rank(item) > rank(current_best):
                    current_best = item
            settings = record['settings']
            optimizers = [job for job in record['jobs'] if job['kind'] == 'optimizer']
            remaining = settings['max_runs'] - sum(job['started'] or job['status'] not in JOB_FINISHED for job in optimizers)
            evidence = {'observations': observations, 'best': current_best, 'remaining_runs': remaining,
                        'free_slots': max(1, await owned_thread(self.backend.capacity, record)),
                        'baseline': record.get('baseline'), 'jev': record.get('jev_answers') or [], 'repair_error': record.get('repair_error'),
                        'jobs': [{key: job.get(key) for key in ('operation', 'status', 'reason', 'error', 'config', 'executed_config', 'runner')} for job in optimizers if job['round'] == record['round']]}
            if required_rounds:
                evidence.update(required_rounds=required_rounds,
                    required_rounds_remaining=max(0, required_rounds - len(completed_rounds | {record['round']})))
            decision = await self.decision(record, 'evaluate', evidence)
            improved = current_best is not None and (record['best'] is None or rank(current_best) > rank(record['best']))
            entry = {'coins': settings['goals'].get('coins', []), 'hypothesis': decision.get('reason', ''),
                     'knowledge': decision.get('knowledge', ''), 'contradicts': decision.get('contradicts', []),
                     'goals': settings['goals'], 'comparison': record['comparison'], 'uncertainty': 'Exact comparison evidence; several simultaneous edits do not establish individual causality', 'changes': [{'operation': job['operation'], 'reason': job['reason'], 'config_digest': digest(job['config'])} for job in optimizers if job['round'] == record['round']], 'observations': observations,
                     'status': 'observed_exact' if eligible else 'hypothesis', 'improved': improved}
            await owned_thread(self.store.remember, record, record['id'] + ':round:' + str(record['round']), entry)
            def observed(row):
                row['best'] = current_best
                already_observed = any(item['round'] == row['round'] for item in row['history'])
                if not already_observed:
                    row['stagnation'] = 0 if improved else row['stagnation'] + 1
                row['history'] = [item for item in row['history'] if item['round'] != row['round']]
                row.update(errors=0, last_error=None, repair_error=None)
                row['history'].append({'round': row['round'], 'reason': str(decision.get('reason', ''))[:4000],
                                       'knowledge': str(decision.get('knowledge', ''))[:4000],
                                       'best_score': current_best['assessment']['score'] if current_best else None,
                                       'cycle_score': cycle_best['assessment']['score'],
                                       'assessment': copy.deepcopy(cycle_best['assessment']),
                                       'metrics': {key: value for key, value in cycle_best['metrics'].items() if key in goal_metrics},
                                       'observations': [{'operation': item['operation'], 'candidate_id': item['candidate']['id'],
                                                         'optimizer_job': item['candidate']['optimizer_job'],
                                                         'assessment': copy.deepcopy(item['assessment']),
                                                         'metrics': {key: value for key, value in item['metrics'].items() if key in goal_metrics}}
                                                        for item in eligible],
                                       'evaluated_at': time.time(),
                                       'changes': [{'operation': job['operation'], 'config_digest': digest(job['config']), 'executed_digest': job.get('executed_digest'), 'fields': config_changes(record['initial_config'], job['config'])} for job in optimizers if job['round'] == row['round']]})
            record = self.commit(record, observed)
            must_continue = len({item['round'] for item in record['history'] if item.get('best_score') is not None}) < required_rounds
            if remaining <= 0 or (not must_continue and (decision.get('finish') or record['stagnation'] >= settings['patience'] or (current_best and current_best['assessment']['achieved']))):
                stop = 'Optimizer allowance reached' if remaining <= 0 else 'Goals reached' if current_best and current_best['assessment']['achieved'] else 'No improvement patience reached' if record['stagnation'] >= settings['patience'] else 'AI concluded the search'
                record = self.commit(record, lambda row: row.update(stop_reason=stop))
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
                config = variant.get('config')
                overrides = variant.get('overrides', record['initial_overrides'])
                validate_change(record, config, overrides)
                jobs.append(self.job(next_record, 'optimizer', config, overrides, variant.get('reason', '')))
            self.commit(record, lambda row: row.update(round=next_record['round'], jobs=row['jobs'] + jobs,
                                                       phase='optimize', repair_error=None, errors=0))
            return
        raise ValueError('Unknown persisted loop phase')

    async def sync_jobs(self, record, launch=True):
        """Reconcile persisted launch intent before allocating any additional job."""
        for job in record['jobs']:
            if record['phase'] == 'baseline' and job['kind'] != 'baseline':
                continue
            if job['status'] in JOB_FINISHED:
                continue
            current = self.store.read(record['owner'], record['id'])
            if current['status'] not in ACTIVE:
                launch = False
            if job['backend'] is None:
                if not launch:
                    continue
                try:
                    backend = await owned_thread(self.backend.prepare, current, job)
                except Exception as exc:
                    from fastapi import HTTPException
                    current = self.store.read(record['owner'], record['id'])
                    if current['status'] not in ACTIVE:
                        return current
                    if isinstance(exc, HTTPException) and exc.status_code == 409:
                        continue
                    if isinstance(exc, (HTTPException, ValueError)) or getattr(exc, 'status_code', 0) == 422:
                        def rejected(row):
                            target = next(item for item in row['jobs'] if item['operation'] == job['operation'])
                            target.update(status='failed', error=str(exc)[:1000], ended_at=time.time())
                            row['repair_error'] = str(exc)[:1000]
                        record = self.store.update(record['owner'], record['id'], rejected)
                        continue
                    raise
                def prepared(row):
                    target = next(item for item in row['jobs'] if item['operation'] == job['operation'])
                    target.update(backend=backend, status='queued')
                record = self.store.update(record['owner'], record['id'], prepared)
                job = next(item for item in record['jobs'] if item['operation'] == job['operation'])
            state = await owned_thread(self.backend.poll, record, job)
            source = state.get('source') or {}
            started_at = job.get('run_started_at') or source.get('started_at')
            if started_at is None and state['status'] == 'running' and state['started']:
                started_at = time.time()
            mode = current['settings'].get('run_limit_mode', 'config')
            hit = None
            if job['kind'] == 'optimizer' and started_at is not None and state['status'] not in JOB_FINISHED:
                if mode == 'hours' and time.time() >= started_at + current['settings']['run_hours'] * 3600:
                    hit = 'hours'
                if mode == 'proxy' and (state.get('proxy_evaluations') or 0) >= current['settings']['run_proxy']:
                    hit = 'proxy'
            if (hit or job.get('limit_reached')) and state['status'] not in JOB_FINISHED:
                if hit and not job.get('limit_reached'):
                    def limited(row):
                        target = next(item for item in row['jobs'] if item['operation'] == job['operation'])
                        target.update(limit_reached=hit, run_started_at=started_at)
                    current = self.store.update(record['owner'], record['id'], limited)
                    job = next(item for item in current['jobs'] if item['operation'] == job['operation'])
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
                        state = await owned_thread(self.backend.start, self.store.read(record['owner'], record['id']), job)
                    except Exception as exc:
                        from fastapi import HTTPException
                        current = self.store.read(record['owner'], record['id'])
                        if current['status'] not in ACTIVE:
                            return current
                        if isinstance(exc, HTTPException) and exc.status_code == 409:
                            continue
                        if isinstance(exc, (HTTPException, ValueError)):
                            await owned_thread(self.backend.stop, current, job)
                            state = {'status': 'failed', 'started': state['started']}
                            self.store.update(record['owner'], record['id'], lambda row: row.update(repair_error=str(exc)[:1000]))
                        else:
                            raise
            def synced(row):
                target = next(item for item in row['jobs'] if item['operation'] == job['operation'])
                target['status'] = state['status']
                target['started'] = target['started'] or state['started']
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
            record = self.store.update(record['owner'], record['id'], synced)
        return record

    async def finish(self, record, reason):
        """Freeze selection before checking its isolated existing user Holdout."""
        if record.get('observer_enabled') and record.get('best'):
            self.commit(record, lambda row: row.update(status='finishing', phase='observer_confirmation',
                                                       stop_reason=reason, reason='Checking selected candidate Holdout'))
            return
        remaining = record['settings']['max_validations'] - record['validation_count']
        if record.get('best') and record['holdouts'] and not record['holdout_used'] and remaining > 0 and time.time() < record['deadline'] - 60:
            candidate = record['best']['candidate']
            config = self.backend.validation_config(record, candidate, True)
            if not await owned_thread(self.store.claim_holdouts, record):
                self.commit(record, lambda row: row.update(status='unconfirmed', reason='Final holdout was already reserved or used by another loop'))
                return
            job = self.job(record, 'holdout', config,
                           candidate['overrides'], 'One final unused holdout', candidate=candidate)
            self.commit(record, lambda row: row.update(status='finishing', phase='holdout', holdout_used=True,
                                                       jobs=row['jobs'] + [job], validation_count=row['validation_count'] + 1, reason=reason))
        else:
            self.commit(record, lambda row: row.update(status='unconfirmed', reason=reason + '; no unused final holdout validation available'))

    async def confirm_observer(self, record):
        """Verify selected-candidate evidence after search ends, without another AI decision."""
        from pb8_loop_observer import SUCCESS, FINISHED, needs_observer_report
        best = record.get('best')
        if not best:
            self.commit(record, lambda row: row.update(status='unconfirmed', reason='No exact candidate to check'))
            return
        try:
            candidate = best['candidate']
            expected = self.backend.observer_config(record, candidate, 'observer_holdout')
            sources = {(number, source['id']) for number, source in self.observers.sources(record)}
            matches = [job for job in record.get('observer_jobs', [])
                       if job.get('observer_only') and job['kind'] == 'observer_holdout'
                       and (job['round'], job.get('candidate_id')) in sources
                       and job.get('config') == expected and job.get('overrides') == candidate['overrides']]
            if not matches:
                if self.observers.pending(record) and time.time() < record['deadline'] - 60:
                    return
                raise ValueError('No matching selected-candidate Holdout result is available')
            job = max(matches, key=lambda item: (item.get('candidate_id') == candidate['id'], item['round']))
            if job['status'] not in FINISHED or needs_observer_report(job):
                if time.time() < record['deadline'] - 60 or job.get('started'):
                    return
                raise ValueError('Selected-candidate Holdout did not finish within the allowance')
            if job['status'] not in SUCCESS:
                raise ValueError('Selected-candidate Holdout backtest failed')
            state = await owned_thread(self.backend.poll, record, job)
            if state['status'] not in SUCCESS or not state.get('started'):
                raise ValueError('Selected-candidate Holdout has no completed native runner')
            executed = copy.deepcopy(state.get('executed_config'))
            planned = copy.deepcopy(expected)
            if not isinstance(executed, dict):
                raise ValueError('Selected-candidate Holdout executed snapshot is unavailable')
            # Native queue assigns the output folder and resolves an empty local data root.
            for value in (executed, planned):
                value['backtest'].pop('base_dir', None)
            actual_source = executed['backtest'].get('ohlcv_source_dir')
            if actual_source and not planned['backtest'].get('ohlcv_source_dir'):
                from market_data import get_market_data_root_dir
                if Path(actual_source).resolve() != Path(get_market_data_root_dir()).resolve():
                    raise ValueError('Selected-candidate Holdout used a different market data root')
                planned['backtest']['ohlcv_source_dir'] = actual_source
            if executed != planned:
                raise ValueError('Selected-candidate Holdout executed a different configuration')
            observation = await owned_thread(self.backend.observations, record, job)
            assessment = observation['assessment']
            valid = bool(record['fingerprint'].get('data') and observation.get('exact')
                         and assessment.get('simulation_complete') and assessment.get('comparable')
                         and assessment.get('hard_targets_met')
                         and not any(rule['goal'] == 'custom' and rule.get('target') is None for rule in record['rubric']))
            assessment.update(confirmation_source='observer_holdout', checked_candidate_id=candidate['id'])
            observation.update(operation=job['operation'], observer_only=True)
            self.commit(record, lambda row: row.update(status='completed' if valid else 'unconfirmed',
                reason='Selected candidate Holdout checked; numeric targets met' if valid and any(rule.get('target') is not None for rule in row['rubric'])
                       else 'Selected candidate Holdout checked; no numeric targets specified' if valid
                       else 'Selected candidate Holdout incomplete, targets violated or qualitative goals unconfirmed',
                final_validation=[observation]))
        except Exception as exc:
            _log(SERVICE, f'Selected candidate Holdout check for {record["id"]}: {type(exc).__name__}: {exc}', level='WARNING')
            self.commit(record, lambda row: row.update(status='unconfirmed',
                reason='Selected candidate Holdout check unavailable: ' + str(exc)[:1000]))
