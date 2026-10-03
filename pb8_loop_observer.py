"""Durable user-only Loop backtests, isolated from optimization and learned evidence."""
from __future__ import annotations

import copy
import math
import time
import uuid

from logging_helpers import human_log as _log
from pb8_loop_store import completed_round_observations

FINISHED = {'completed', 'complete', 'failed', 'error', 'cancelled', 'stopped', 'skipped'}
SUCCESS = {'completed', 'complete'}
KINDS = ('observer_holdout', 'observer_full_range')


def display_metrics(observation):
    """Summarize actual scenario metrics with fixed, explicitly displayed reducers."""
    reports = observation.get('per_report') or [observation.get('metrics') or {}]
    definitions = {
        'gain': (('gain_strategy_eq', 'gain_usd', 'gain'), 'mean'),
        'drawdown': (('drawdown_worst_strategy_eq', 'drawdown_worst'), 'max'),
        'recovery_days': (('strategy_eq_recovery_days_max', 'equity_recovery_days_max'), 'max'),
        'adg': (('adg_strategy_eq', 'adg'), 'mean'),
        'sharpe': (('sharpe_ratio_strategy_eq', 'sharpe_ratio'), 'mean'),
        'sortino': (('sortino_ratio_strategy_eq', 'sortino_ratio'), 'mean'),
        'trades': (('n_fills',), 'sum'),
    }
    result = {}
    for name, (aliases, reducer) in definitions.items():
        values = [next((report[key] for key in aliases if isinstance(report.get(key), (int, float))
                        and not isinstance(report[key], bool) and math.isfinite(report[key])), None) for report in reports]
        result[name] = None if not values or any(value is None for value in values) else (
            max(values) if reducer == 'max' else sum(values) if reducer == 'sum' else sum(values) / len(values))
    return result


def comparison_deltas(job, other):
    """Return deltas only for complete simulations under identical reference windows."""
    if not job.get('simulation_complete') or not other or not other.get('simulation_complete'):
        return {}
    return {key: value - other['metrics'][key] for key, value in job.get('metrics', {}).items()
            if value is not None and other.get('metrics', {}).get(key) is not None}


def observer_identity(job):
    """Keep each immutable candidate/period distinct, including prior evaluation jobs."""
    return job['round'], job['kind'], job.get('candidate_id')


def needs_observer_report(job):
    """Recover old Holdout scores without retrying permanently unreadable reports."""
    return job['status'] in SUCCESS and (not job.get('report_done') or (
        job['kind'] == 'observer_holdout' and not job.get('assessment')
        and job.get('report_errors', 0) < 3 and not job.get('selection_error')))


def holdout_selection(record, number):
    """Select a display-only winner after every eligible candidate's Holdout settles."""
    candidates = [candidate for round_number, candidate in ObserverJobs.sources(record) if round_number == number]
    jobs = [job for job in record.get('observer_jobs', [])
            if job['round'] == number and job['kind'] == 'observer_holdout']
    selected = [next((job for job in jobs if job.get('candidate_id') == candidate['id']), None)
                for candidate in candidates]
    settled = sum(job is not None and job['status'] in FINISHED and not needs_observer_report(job) for job in selected)
    summary = {'round': number, 'expected': len(selected), 'completed': settled,
               'status': 'waiting' if settled < len(selected) else 'unavailable'}
    if summary['status'] == 'waiting':
        return summary
    eligible = []
    for job in selected:
        assessment = (job or {}).get('assessment') or {}
        score = assessment.get('score')
        if (job and job['status'] in SUCCESS and job.get('simulation_complete')
                and assessment.get('simulation_complete') and assessment.get('comparable', True)
                and type(score) in (int, float) and math.isfinite(score)):
            eligible.append(job)
    if eligible:
        winner = max(eligible, key=lambda job: (job['assessment'].get('hard_targets_met', True), job['assessment']['score']))
        summary.update(status='selected', candidate_id=winner['candidate_id'],
                       operation=winner['operation'], score=winner['assessment']['score'])
    return summary


def observer_selections(record):
    """Expose only display-selection metadata, separately from optimizer scores."""
    rounds = {number for number, _ in ObserverJobs.sources(record)}
    rounds.update(job['round'] for job in record.get('observer_jobs', []))
    return [holdout_selection(record, number) for number in sorted(rounds)]


class ObserverJobs:
    """Use the existing controller owner/lock and detached native Backtest worker."""
    def __init__(self, store, backend, thread):
        self.store, self.backend, self.thread = store, backend, thread

    @staticmethod
    def sources(record):
        """Keep every successful exact comparison candidate eligible for a Holdout."""
        if not record.get('observer_enabled'):
            return []
        sources = []
        if record.get('baseline'):
            sources.append((-1, {'id': 'starting_config', 'config': record['initial_config'],
                                 'overrides': record['initial_overrides']}))
        by_operation = {job['operation']: job for job in record['jobs'] if job['kind'] == 'validation'}
        rounds = {entry['round'] for entry in record['history']} | {record['round']}
        for number in sorted(rounds):
            eligible = completed_round_observations(record, number)
            sources.extend((number, by_operation[item['operation']]['candidate']) for item in eligible)
        return sources

    @classmethod
    def targets(cls, record):
        """Plan every Holdout first, then only its round winner's Full Time Range."""
        sources = cls.sources(record)
        targets = [(number, candidate, 'observer_holdout') for number, candidate in sources]
        for number in sorted({number for number, _ in sources}):
            selection = holdout_selection(record, number)
            if selection['status'] == 'selected':
                candidate = next(candidate for round_number, candidate in sources
                                 if round_number == number and candidate['id'] == selection['candidate_id'])
                targets.append((number, candidate, 'observer_full_range'))
        return targets

    @classmethod
    def pending(cls, record):
        """Keep collecting user-only jobs after the optimizer has normally finished."""
        jobs = record.get('observer_jobs', [])
        if any(job['status'] not in FINISHED or needs_observer_report(job) for job in jobs):
            return True
        existing = {observer_identity(job) for job in jobs}
        return any((number, kind, candidate['id']) not in existing for number, candidate, kind in cls.targets(record))

    def plan(self, record):
        """Persist immutable round/reference jobs once; never modify optimization evidence."""
        jobs = []
        existing = {observer_identity(job) for job in record.get('observer_jobs', [])}
        for number, candidate, kind in self.targets(record):
            key = (number, kind, candidate['id'])
            if key in existing:
                continue
            existing.add(key)
            token = uuid.uuid4().hex
            job = {'operation': token, 'name': 'loop_' + record['id'][:8] + '_' + token[:8],
                   'kind': kind, 'round': number, 'observer_only': True, 'candidate_id': candidate['id'],
                   'selection_policy': 'all_holdouts_best_full_range',
                   'overrides': copy.deepcopy(candidate['overrides']), 'backend': None,
                   'status': 'planned', 'started': False, 'created_at': time.time(),
                   'reason': 'User-only evaluation; excluded from all optimization decisions'}
            try:
                job['config'] = self.backend.observer_config(record, candidate, kind)
            except ValueError as exc:
                job.update(status='skipped', error=str(exc)[:1000], ended_at=time.time(), report_done=True)
            jobs.append(job)
        if jobs:
            def save(current):
                present = {observer_identity(job) for job in current.get('observer_jobs', [])}
                for job in jobs:
                    if observer_identity(job) not in present:
                        current.setdefault('observer_jobs', []).append(job)
                        present.add(observer_identity(job))
            record = self.store.update(record['owner'], record['id'], save)
        return record

    def update(self, record, job, **values):
        """Update only the separate observer collection under the record transaction lock."""
        def change(current):
            target = next(item for item in current['observer_jobs'] if item['operation'] == job['operation'])
            target.update(values)
        return self.store.update(record['owner'], record['id'], change)

    async def tick(self, record):
        """Reconcile a bounded number of jobs without waiting for their simulations."""
        if not record.get('observer_enabled'):
            return
        record = self.plan(record)
        pending = [job for job in record.get('observer_jobs', [])
                   if job['status'] not in FINISHED or needs_observer_report(job)]
        for original in pending[:4]:
            job = copy.deepcopy(original)
            starting = False
            try:
                current = self.store.read(record['owner'], record['id'])
                cancelling = current['status'] in {'stopping', 'stopped'}
                launch = current['status'] in {'running', 'finishing', 'completed', 'unconfirmed', 'failed'} and time.time() < current['deadline'] - 60
                if cancelling or not launch and current['status'] != 'paused' and not job.get('started'):
                    if job.get('backend') and job['status'] not in FINISHED:
                        await self.thread(self.backend.stop, current, job)
                        state = await self.thread(self.backend.poll, current, job)
                        record = self.update(current, job, status=state['status'], started=job['started'] or state['started'],
                                             **({'ended_at':time.time(), 'report_done':True} if state['status'] in FINISHED else {}))
                    elif not job.get('backend'):
                        record = self.update(current, job, status='cancelled', ended_at=time.time(), report_done=True)
                    elif job['status'] in FINISHED:
                        record = self.update(current, job, report_done=True, selection_error=True)
                    if cancelling:
                        continue
                if job.get('backend') is None:
                    if not launch:
                        continue
                    backend = await self.thread(self.backend.prepare, current, job)
                    record = self.update(current, job, backend=backend, status='queued')
                    job.update(backend=backend, status='queued')
                state = await self.thread(self.backend.poll, current, job)
                if launch and state['status'] == 'queued':
                    local = copy.deepcopy(current)
                    local['settings']['execution'] = 'cpu'
                    local['initial_config'].setdefault('optimize', {})['n_cpus'] = 1
                    if await self.thread(self.backend.capacity, local) > 0:
                        starting = True
                        state = await self.thread(self.backend.start, current, job)
                        starting = False
                values = {'status':state['status'], 'started':job['started'] or state['started'],
                          'error':state.get('error') or (state.get('source') or {}).get('error')}
                if state['started']:
                    values['run_started_at'] = job.get('run_started_at') or (state.get('source') or {}).get('started_at') or time.time()
                if state['status'] in FINISHED:
                    values['ended_at'] = job.get('ended_at') or time.time()
                record = self.update(current, job, **values)
                job.update(values)
                if state['status'] in SUCCESS:
                    observation = await self.thread(self.backend.observations, current, job)
                    record = self.update(current, job, metrics=display_metrics(observation), reports=observation.get('reports'),
                        assessment=copy.deepcopy(observation['assessment']),
                        simulation_complete=observation['assessment'].get('simulation_complete', False), report_done=True)
                elif state['status'] in FINISHED:
                    record = self.update(current, job, report_done=True)
            except Exception as exc:
                _log('PB8Loop', f'User-only backtest {job["operation"]}: {type(exc).__name__}: {exc}', level='WARNING')
                from fastapi import HTTPException
                values = {'error':str(exc)[:1000], 'report_errors':job.get('report_errors', 0) + 1}
                if not job.get('backend') and (isinstance(exc, ValueError) or isinstance(exc, HTTPException) and exc.status_code == 422):
                    values.update(status='failed', ended_at=time.time(), report_done=True)
                elif starting and (isinstance(exc, ValueError) or isinstance(exc, HTTPException) and exc.status_code == 422):
                    # Reconcile a possibly accepted start before cancelling only an owned waiting job.
                    self.update(current, job, **values)
                    state = await self.thread(self.backend.poll, current, job)
                    if state['status'] == 'queued' and not state['started']:
                        await self.thread(self.backend.stop, current, job)
                        state = await self.thread(self.backend.poll, current, job)
                        values.update(status=state['status'], started=state['started'])
                        if state['status'] in FINISHED:
                            values.update(ended_at=time.time(), report_done=True)
                elif job['status'] in SUCCESS and values['report_errors'] >= 3:
                    values['report_done'] = True
                self.update(record, job, **values)
