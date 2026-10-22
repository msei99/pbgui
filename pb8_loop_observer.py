"""Durable user-only Loop backtests, isolated from optimization and learned evidence."""
from __future__ import annotations

import copy
from collections import OrderedDict
import math
import time
import uuid

from logging_helpers import human_log as _log
from pb8_loop_store import completed_round_observations

FINISHED = {'completed', 'complete', 'failed', 'error', 'cancelled', 'stopped', 'skipped'}
SUCCESS = {'completed', 'complete'}
KINDS = ('observer_holdout', 'observer_full_range')
REPLACEMENT_CURSOR_LIMIT = 128
REPLACEMENT_CURSOR_TTL = 600


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
    history = next((entry for entry in record.get('history', []) if entry['round'] == number), {})
    if 'winner_operation' in history or number < 0:
        candidate_id = history.get('winner_candidate_id') if number >= 0 else 'starting_config'
        winner = next((job for job in selected if job
                       and job.get('candidate_id') == candidate_id
                       and job['status'] in SUCCESS and job.get('simulation_complete')), None)
        if winner is None and number >= 0:
            # An unchanged incumbent retains its original validation operation.
            # Follow that immutable origin, never another candidate's Holdout.
            origin = next((job for job in record.get('jobs', [])
                           if job['kind'] == 'validation'
                           and job['operation'] == history.get('winner_operation')
                           and job['round'] < number
                           and (job.get('candidate') or {}).get('id') == candidate_id), None)
            if origin:
                winner = next((job for job in record.get('observer_jobs', [])
                               if job['kind'] == 'observer_holdout'
                               and job['round'] == origin['round']
                               and job.get('candidate_id') == candidate_id
                               and job['status'] in SUCCESS and job.get('simulation_complete')
                               and not needs_observer_report(job)), None)
                if winner:
                    summary['source_round'] = origin['round']
        if winner:
            summary.update(status='selected', candidate_id=winner['candidate_id'],
                           operation=winner['operation'], score=None)
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


def observer_kinds(record):
    """Share the exact scheduling activation rule with display summaries."""
    return KINDS if record.get('observer_holdouts', record.get('training_exclusions', record.get('holdouts'))) else ('observer_full_range',)


def complete_observer_report(job):
    """A terminal process alone does not prove a successfully ingested simulation."""
    return bool(job and job.get('status') in SUCCESS and job.get('report_done')
                and job.get('simulation_complete'))


def observer_display_selections(record):
    """Resolve immutable training winners without waiting for user-only reports."""
    rounds = {number for number, _ in ObserverJobs.sources(record)}
    rounds.update(entry['round'] for entry in record.get('history', []))
    rounds.update(job['round'] for job in record.get('observer_jobs', []))
    selections = []
    for number in sorted(rounds):
        history = next((entry for entry in record.get('history', []) if entry['round'] == number), {})
        modern = 'winner_operation' in history or 'winner_candidate_id' in history
        result = {'round': number, 'status': 'unavailable', 'modern': modern}
        if number == -1 and record.get('baseline'):
            result.update(status='selected', candidate_id='starting_config', source_round=-1)
        elif modern:
            operation, candidate_id = history.get('winner_operation'), history.get('winner_candidate_id')
            if operation and candidate_id == 'baseline':
                baseline = record.get('baseline') or {}
                baseline_jobs = [job for job in record.get('jobs', []) if job['kind'] == 'baseline']
                # Old baselines had no operation; the controller used the literal fallback.
                expected = baseline.get('operation') or (baseline_jobs[0].get('operation') if baseline_jobs else 'baseline')
                valid_jobs = all(job.get('operation') == operation and job.get('round') == -1
                                 and job.get('status') in SUCCESS
                                 and (job.get('candidate') or {}).get('id') == 'baseline'
                                 for job in baseline_jobs)
                if (baseline and operation == expected and valid_jobs
                        and (baseline.get('candidate') or {'id': 'baseline'}).get('id') == 'baseline'
                        and (baseline.get('assessment') or {}).get('simulation_complete', True) is not False):
                    result.update(status='selected', candidate_id='starting_config', source_round=-1)
            elif operation and candidate_id:
                origins = [job for job in record.get('jobs', []) if job['operation'] == operation]
                if len(origins) == 1:
                    origin = origins[0]
                    if (origin['kind'] == 'validation' and 0 <= origin['round'] <= number
                            and origin['status'] in SUCCESS
                            and (origin.get('candidate') or {}).get('id') == candidate_id
                            and any(item['operation'] == operation for item in
                                    completed_round_observations(record, origin['round']))):
                        result.update(status='selected', candidate_id=candidate_id, source_round=origin['round'])
        else:
            # Preserve display-only historical Holdout ranking and readiness.
            result.update(holdout_selection(record, number))
            if result['status'] == 'selected':
                result.setdefault('source_round', number)
        selections.append(result)
    return selections


def observer_summaries(record):
    """Count native targets once per round/kind; aliases never become extra work."""
    sources = {(number, candidate['id']) for number, candidate in ObserverJobs.sources(record)}
    enabled = observer_kinds(record)
    jobs = {}
    for job in record.get('observer_jobs', []):
        jobs.setdefault(observer_identity(job), job)
    summaries = []
    for selection in observer_display_selections(record):
        number = selection['round']
        for kind in KINDS:
            expected = {candidate for source_round, candidate in sources if source_round == number}
            # Retain old recorded reports even if their candidate evidence is no longer available.
            expected.update(candidate for source_round, report_kind, candidate in jobs
                            if source_round == number and report_kind == kind)
            matching = [jobs.get((number, kind, candidate)) for candidate in expected] if kind in enabled else []
            terminal = sum(bool(job and job['status'] in FINISHED) for job in matching)
            successful = sum(complete_observer_report(job) for job in matching)
            pending = sum(bool(job and job['status'] in SUCCESS and not job.get('report_done')) for job in matching)
            failed = sum(bool(job and job['status'] in SUCCESS and job.get('report_done')
                              and not job.get('simulation_complete')) for job in matching)
            chosen = jobs.get((selection.get('source_round'), kind, selection.get('candidate_id')))
            selected_status = None
            if chosen and not complete_observer_report(chosen):
                selected_status = ('report pending' if not chosen.get('report_done') else 'report error') if chosen['status'] in SUCCESS else chosen['status']
            selected_pending = bool(chosen and (chosen['status'] not in FINISHED
                                    or chosen['status'] in SUCCESS and not chosen.get('report_done')))
            status = 'selected' if complete_observer_report(chosen) else (
                'waiting' if terminal < len(matching) or pending or selected_pending else 'unavailable')
            summaries.append({'round': number, 'kind': kind,
                              'expected': len(matching), 'terminal': terminal, 'successful': successful,
                              'report_pending': pending, 'report_failed': failed,
                              'status': status if kind in enabled else 'disabled',
                              'candidate_id': selection.get('candidate_id'),
                              'source_round': selection.get('source_round'),
                              'selected_report_status': selected_status,
                              'operation': chosen['operation'] if chosen else None})
    return summaries


class ObserverJobs:
    """Use the existing controller owner/lock and detached native Backtest worker."""
    def __init__(self, store, backend, thread):
        self.store, self.backend, self.thread = store, backend, thread
        self._replacement_poll_cursors = OrderedDict()

    @staticmethod
    def sources(record):
        """Keep every successful exact comparison candidate eligible for a Holdout."""
        sources = []
        if record.get('baseline'):
            sources.append((-1, {'id': 'starting_config', 'config': record['initial_config'],
                                 'overrides': record['initial_overrides']}))
        by_operation = {job['operation']: job for job in record.get('jobs', []) if job['kind'] == 'validation'}
        rounds = {entry['round'] for entry in record.get('history', [])} | {job['round'] for job in by_operation.values()}
        for number in sorted(rounds):
            eligible = completed_round_observations(record, number)
            sources.extend((number, by_operation[item['operation']]['candidate']) for item in eligible)
        return sources

    @classmethod
    def targets(cls, record):
        """Always schedule both user reports, independently of either result."""
        kinds = observer_kinds(record)
        return [(number, candidate, kind) for number, candidate in cls.sources(record) for kind in kinds]

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
                   'selection_policy': 'all_candidates_both_reports',
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
        previous = next((item for item in record.get('observer_jobs', []) if item['operation'] == job['operation']), None)
        if previous is not None and all(key in previous and previous[key] == value for key, value in values.items()):
            return record
        def change(current):
            target = next(item for item in current['observer_jobs'] if item['operation'] == job['operation'])
            target.update(values)
        return self.store.update(record['owner'], record['id'], change)

    def _poll_batch(self, record, pending):
        """Rotate existing backends during replacement without archiving polling-only state."""
        from pb8_loop_gpu_replacement import open_replacement
        key = (record['owner'], record['id'])
        previous = self._replacement_poll_cursors.pop(key, None)
        if not open_replacement(record):
            return pending[:4]
        # Planned jobs cannot advance while replacement blocks new starts.
        # Queued native jobs still need polling to discover an independent start.
        pending = [job for job in pending if job.get('backend')]
        now = time.monotonic()
        for cached_key, (_, checked_at) in list(self._replacement_poll_cursors.items()):
            if now - checked_at >= REPLACEMENT_CURSOR_TTL:
                self._replacement_poll_cursors.pop(cached_key)
        if not pending:
            return []
        cursor = previous[0] if previous and now - previous[1] < REPLACEMENT_CURSOR_TTL else None
        start = next((index + 1 for index, job in enumerate(pending) if job['operation'] == cursor), 0)
        batch = (pending[start:] + pending[:start])[:4]
        self._replacement_poll_cursors[key] = (batch[-1]['operation'], now)
        while len(self._replacement_poll_cursors) > REPLACEMENT_CURSOR_LIMIT:
            self._replacement_poll_cursors.popitem(last=False)
        return batch

    async def tick(self, record):
        """Reconcile a bounded number of jobs without waiting for their simulations."""
        record = await self.thread(self.plan, record)
        pending = [job for job in record.get('observer_jobs', [])
                   if job['status'] not in FINISHED or needs_observer_report(job)]
        for original in self._poll_batch(record, pending):
            job = copy.deepcopy(original)
            starting = False
            try:
                current = await self.thread(self.store.read, record['owner'], record['id'])
                launch = current['status'] in {'running', 'finishing', 'completed', 'unconfirmed', 'failed', 'stopping', 'stopped'}
                from pb8_loop_gpu_replacement import open_replacement
                if open_replacement(current):
                    launch = False
                if not launch and current['status'] not in {'paused', 'replacing_gpu'} and not open_replacement(current) and not job.get('started'):
                    if job.get('backend') and job['status'] not in FINISHED:
                        await self.thread(self.backend.stop, current, job)
                        state = await self.thread(self.backend.poll, current, job)
                        record = await self.thread(self.update, current, job, status=state['status'], started=job['started'] or state['started'],
                                             **({'ended_at':time.time(), 'report_done':True} if state['status'] in FINISHED else {}))
                    elif not job.get('backend'):
                        record = await self.thread(self.update, current, job, status='cancelled', ended_at=time.time(), report_done=True)
                    elif job['status'] in FINISHED:
                        record = await self.thread(self.update, current, job, report_done=True, selection_error=True)
                if job.get('backend') is None:
                    if not launch:
                        continue
                    backend = await self.thread(self.backend.prepare, current, job)
                    record = await self.thread(self.update, current, job, backend=backend, status='queued')
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
                record = await self.thread(self.update, current, job, **values)
                job.update(values)
                if state['status'] in SUCCESS:
                    observation = await self.thread(self.backend.observations, current, job)
                    record = await self.thread(self.update, current, job, metrics=display_metrics(observation), reports=observation.get('reports'),
                        assessment=copy.deepcopy(observation['assessment']),
                        simulation_complete=observation['assessment'].get('simulation_complete', False), report_done=True)
                elif state['status'] in FINISHED:
                    record = await self.thread(self.update, current, job, report_done=True)
            except Exception as exc:
                _log('PB8Loop', f'User-only backtest {job["operation"]}: {type(exc).__name__}: {exc}', level='WARNING')
                from fastapi import HTTPException
                values = {'error':str(exc)[:1000], 'report_errors':job.get('report_errors', 0) + 1}
                if not job.get('backend') and (isinstance(exc, ValueError) or isinstance(exc, HTTPException) and exc.status_code == 422):
                    values.update(status='failed', ended_at=time.time(), report_done=True)
                elif starting and (isinstance(exc, ValueError) or isinstance(exc, HTTPException) and exc.status_code == 422):
                    # Reconcile a possibly accepted start before cancelling only an owned waiting job.
                    await self.thread(self.update, current, job, **values)
                    state = await self.thread(self.backend.poll, current, job)
                    if state['status'] == 'queued' and not state['started']:
                        await self.thread(self.backend.stop, current, job)
                        state = await self.thread(self.backend.poll, current, job)
                        values.update(status=state['status'], started=state['started'])
                        if state['status'] in FINISHED:
                            values.update(ended_at=time.time(), report_done=True)
                elif job['status'] in SUCCESS and values['report_errors'] >= 3:
                    values['report_done'] = True
                await self.thread(self.update, record, job, **values)
