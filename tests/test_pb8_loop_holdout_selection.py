"""Offline per-candidate Holdouts and a single, independently ranked full-range job."""
import asyncio
import copy

import pytest

from api.loop_optimizer_v8 import _observer_reports, projection
from pb8_loop_controller import owned_thread
from pb8_loop_observer import ObserverJobs, holdout_selection, observer_display_selections, observer_summaries
from tests.test_pb8_loop_observer import Backend, OWNER, record, reference


def candidates(store, row):
    """Create three successful comparison candidates with different immutable inputs."""
    row = reference(store, row)
    first = copy.deepcopy(row['jobs'][0])
    evidence = copy.deepcopy(row['history'][0]['observations'][0])
    jobs, observations = [], []
    for index in range(3):
        job = copy.deepcopy(first)
        job['operation'] = str(index + 1) * 32
        job['candidate']['id'] = 'candidate_' + str(index)
        job['candidate']['overrides']['BTC.json']['test'] = index
        job['candidate']['config']['bot']['long']['risk']['total_wallet_exposure_limit'] = index + 1
        item = copy.deepcopy(evidence)
        item.update(operation=job['operation'])
        item['assessment']['score'] = 30 - index * 10
        jobs.append(job); observations.append(item)
    return store.update(OWNER, row['id'], lambda current: current.update(
        jobs=jobs, baseline=None, history=[{'round': 0, 'observations': observations}]))


def test_independent_progress_counts_targets_and_report_ingestion(record):
    """Count missing work, failures and unread reports independently of selection."""
    store, row = record
    row = candidates(store, row)
    for index in (3, 4):
        job = copy.deepcopy(row['jobs'][0])
        job.update(operation=str(index + 1) * 32)
        job['candidate']['id'] = 'candidate_' + str(index)
        row['jobs'].append(job)
        row['history'][0]['observations'].append({'operation': job['operation'], 'assessment': {'simulation_complete': True}})
    row['history'][0].update(winner_operation=row['jobs'][0]['operation'], winner_candidate_id='candidate_0')
    row = store.update(OWNER, row['id'], lambda current: current.update(row))
    row = ObserverJobs(store, Backend(), owned_thread).plan(row)
    # Work only with this in-memory fixture; never launch workers.
    jobs = {(job['kind'], job['candidate_id']): job for job in row['observer_jobs']}
    for index in range(4):
        jobs['observer_full_range', 'candidate_' + str(index)].update(
            status='complete', report_done=True, simulation_complete=True, metrics={'gain': 7, 'drawdown': .1})
    jobs['observer_holdout', 'candidate_0'].update(status='failed', report_done=True)
    jobs['observer_holdout', 'candidate_1'].update(status='skipped', report_done=True)
    original = copy.deepcopy(row)
    summaries = {item['kind']: item for item in observer_summaries(row) if item['round'] == 0}
    assert (summaries['observer_holdout']['terminal'], summaries['observer_holdout']['successful']) == (2, 0)
    assert (summaries['observer_full_range']['terminal'], summaries['observer_full_range']['successful']) == (4, 4)
    assert summaries['observer_holdout']['expected'] == summaries['observer_full_range']['expected'] == 5
    assert summaries['observer_full_range']['status'] == 'selected'
    assert holdout_selection(row, 0)['status'] == 'waiting'
    selected = [job for job in _observer_reports(row) if job['kind'] == 'observer_full_range' and job['selected_for_full_range']]
    assert len(selected) == 1 and selected[0]['metrics']['gain'] == 7
    assert row == original
    # Missing jobs remain expected. Repeated targets do not inflate the denominator.
    row['observer_jobs'].remove(jobs['observer_full_range', 'candidate_4'])
    row['history'][0]['observations'].append(copy.deepcopy(row['history'][0]['observations'][0]))
    assert next(item for item in observer_summaries(row) if item['kind'] == 'observer_full_range')['expected'] == 5
    source = jobs['observer_full_range', 'candidate_0']
    source.update(report_done=False)
    summary = next(item for item in observer_summaries(row) if item['kind'] == 'observer_full_range')
    assert summary['successful'] == 3 and summary['report_pending'] == 1 and summary['status'] == 'waiting'
    source.update(report_done=True, simulation_complete=False, report_errors=3)
    summary = next(item for item in observer_summaries(row) if item['kind'] == 'observer_full_range')
    assert summary['report_pending'] == 0 and summary['report_failed'] == 1


@pytest.mark.parametrize('field,value,enabled', [
    ('observer_holdouts', [], False), ('observer_holdouts', [{}], True),
    ('training_exclusions', [], False), ('training_exclusions', [{}], True),
    ('holdouts', [], False), ('holdouts', [{}], True)])
def test_summary_activation_matches_targets(record, field, value, enabled):
    """Use the scheduler's precedence even when holdout_available would disagree."""
    store, row = record
    row = candidates(store, row)
    for key in ('observer_holdouts', 'training_exclusions', 'holdouts'):
        row.pop(key, None)
    row[field] = value
    summary = next(item for item in observer_summaries(row) if item['kind'] == 'observer_holdout')
    assert (summary['status'] != 'disabled') == enabled
    assert any(kind == 'observer_holdout' for _, _, kind in ObserverJobs.targets(row)) == enabled
    if not enabled:
        assert summary['expected'] == summary['terminal'] == 0


@pytest.mark.parametrize('operation_mode', ['stored', 'job_only', 'legacy', 'foreign', 'conflicting_job'])
def test_baseline_display_origin_and_aliases(record, operation_mode):
    """Map verified baseline winners across rounds without persisting display aliases."""
    store, row = record
    row = reference(store, row, rounds=3)
    operation = 'b' * 32
    if operation_mode != 'legacy':
        baseline_job = copy.deepcopy(row['jobs'][0])
        baseline_job.update(kind='baseline', round=-1, operation=operation)
        baseline_job['candidate']['id'] = 'baseline'
        row['jobs'].append(baseline_job)
        if operation_mode != 'job_only':
            row['baseline']['operation'] = operation
        if operation_mode == 'conflicting_job':
            baseline_job['operation'] = 'x' * 32
    else:
        operation = 'baseline'
    for history in row['history']:
        history.update(winner_operation='foreign' if operation_mode == 'foreign' else operation,
                       winner_candidate_id='baseline')
    row = store.update(OWNER, row['id'], lambda current: current.update(row))
    row = ObserverJobs(store, Backend(), owned_thread).plan(row)
    for job in row['observer_jobs']:
        if job['kind'] == 'observer_full_range' and job['round'] == -1:
            job.update(status='complete', report_done=True, simulation_complete=True,
                       metrics={'gain': 1.5, 'drawdown': .2})
    original = copy.deepcopy(row)
    selections = observer_display_selections(row)
    reports = _observer_reports(row)
    for number in (0, 1, 2):
        selection = next(item for item in selections if item['round'] == number)
        chosen = [item for item in reports if item['round'] == number and item['selected_for_full_range']]
        if operation_mode in {'foreign', 'conflicting_job'}:
            assert selection['status'] == 'unavailable' and not chosen
            continue
        assert selection['candidate_id'] == 'starting_config' and selection['source_round'] == -1
        full = next(item for item in chosen if item['kind'] == 'observer_full_range')
        assert full['ai_selected'] and full['source_round'] == -1
        assert full['delta_start'] == full['delta_previous'] == {'gain': 0.0, 'drawdown': 0.0}
        summary = next(item for item in observer_summaries(row) if item['round'] == number and item['kind'] == 'observer_full_range')
        assert summary['status'] == 'selected'
        assert summary['expected'] == 1 and summary['terminal'] == 0  # aliases aren't jobs
    assert row == original


@pytest.mark.parametrize('problem', ['empty', 'foreign', 'wrong_candidate', 'future', 'missing_operation'])
def test_modern_invalid_selection_has_no_display_fallback(record, problem):
    """No unrelated complete report may stand in for an invalid modern winner."""
    store, row = record
    _, row = retained_incumbent(store, row)
    history = row['history'][1]
    if problem == 'empty':
        history.update(winner_operation=None, winner_candidate_id=None)
    elif problem == 'foreign':
        history['winner_operation'] = 'unknown'
    elif problem == 'wrong_candidate':
        history['winner_candidate_id'] = 'candidate_1'
    elif problem == 'future':
        row['jobs'][0]['round'] = 2
    else:
        history.pop('winner_operation')
    assert next(item for item in observer_display_selections(row) if item['round'] == 1)['status'] == 'unavailable'
    assert not any(item['selected_for_full_range'] for item in _observer_reports(row) if item['round'] == 1)


def test_retained_full_range_does_not_wait_for_holdout_origin(record):
    """A pending, disabled or unread Holdout never hides complete incumbent evidence."""
    store, row = record
    _, row = retained_incumbent(store, row)
    for job in row['observer_jobs']:
        if job['kind'] == 'observer_holdout':
            job.update(report_done=False, simulation_complete=False)
    row['observer_holdouts'] = []
    reports = _observer_reports(row)
    for number in (1, 2):
        job = next(item for item in reports if item['round'] == number and item['kind'] == 'observer_full_range' and item['selected_for_full_range'])
        assert job['source_round'] == 0 and job['delta_previous'] == {'gain': 0.0, 'drawdown': 0.0}
    assert holdout_selection(row, 1)['status'] != 'selected'


@pytest.mark.parametrize('status', ['failed', 'error', 'cancelled', 'stopped', 'skipped'])
def test_terminal_reports_are_not_successful_simulations(record, status):
    """Terminal failures and skips never contribute to the complete report count."""
    store, row = record
    row = candidates(store, row)
    row = ObserverJobs(store, Backend(), owned_thread).plan(row)
    for job in row['observer_jobs']:
        job.update(status=status, report_done=True, simulation_complete=False)
    for summary in observer_summaries(row):
        assert summary['terminal'] == summary['expected'] == 3
        assert summary['successful'] == summary['report_pending'] == 0
        assert summary['status'] == 'unavailable'


def test_unread_retained_origin_has_no_deltas_and_reports_its_own_pending_state(record):
    """Current-round completion cannot hide pending or failed origin ingestion."""
    store, row = record
    _, row = retained_incumbent(store, row)
    origin = next(job for job in row['observer_jobs'] if job['round'] == 0 and job['kind'] == 'observer_full_range')
    origin['report_done'] = False
    summary = next(item for item in observer_summaries(row) if item['round'] == 1 and item['kind'] == 'observer_full_range')
    assert summary['status'] == 'waiting' and summary['selected_report_status'] == 'report pending'
    projected = next(item for item in _observer_reports(row) if item['round'] == 1 and item['kind'] == 'observer_full_range' and item['selected_for_full_range'])
    assert projected['delta_start'] == projected['delta_previous'] == {}
    origin.update(report_done=True, simulation_complete=False, report_errors=3)
    summary = next(item for item in observer_summaries(row) if item['round'] == 1 and item['kind'] == 'observer_full_range')
    assert summary['status'] == 'unavailable' and summary['selected_report_status'] == 'report error'


@pytest.mark.parametrize('winner', ['candidate', 'baseline'])
def test_archive_roundtrip_preserves_independent_reports_and_display_aliases(record, winner):
    """Hydrated archive reads keep winner reports independent of pending Holdouts."""
    store, row = record
    observer, row = retained_incumbent(store, row)
    source_round = 0
    candidate_id = 'candidate_0'
    if winner == 'baseline':
        source_round, candidate_id = -1, 'starting_config'
        baseline_job = copy.deepcopy(row['jobs'][0])
        baseline_job.update(kind='baseline', round=-1, operation='b' * 32)
        baseline_job['candidate']['id'] = 'baseline'
        row['jobs'].append(baseline_job)
        row['baseline'] = {'operation': baseline_job['operation'],
                           'assessment': {'simulation_complete': True}}
        for history in row['history']:
            history.update(winner_operation=baseline_job['operation'], winner_candidate_id='baseline')
        row = store.update(OWNER, row['id'], lambda current: current.update(row))
        row = observer.plan(row)
    for job in row['observer_jobs']:
        if job['kind'] == 'observer_holdout':
            job.update(status='complete', report_done=False, simulation_complete=False)
        elif job['round'] == source_round and job['candidate_id'] == candidate_id:
            job.update(status='complete', report_done=True, simulation_complete=True,
                       metrics={'gain': 1.5, 'drawdown': .2})
    native_jobs = copy.deepcopy(row['observer_jobs'])
    summaries, reports = observer_summaries(row), _observer_reports(row)
    # The fixture store lives entirely below pytest's temporary directory.
    store.write(row)
    restored = store.read(OWNER, row['id'])
    assert restored['observer_jobs'] == native_jobs
    assert observer_summaries(restored) == summaries
    assert _observer_reports(restored) == reports
    assert projection(restored)['observer_summaries'] == summaries
    metadata = next(item for item in store.list_metadata() if item['id'] == row['id'])
    assert metadata['_metadata_only'] and metadata['observer_pending']
    assert observer.pending(restored)
    for number in (1, 2):
        summary = next(item for item in summaries if item['round'] == number and item['kind'] == 'observer_full_range')
        assert summary['status'] == 'selected' and summary['source_round'] == source_round
        selected = next(item for item in reports if item['round'] == number
                        and item['kind'] == 'observer_full_range' and item['selected_for_full_range'])
        assert selected['source_round'] == source_round
        assert selected['delta_previous'] == {'gain': 0.0, 'drawdown': 0.0}
    assert len(restored['observer_jobs']) == len(native_jobs)  # aliases were never persisted


class RankedBackend(Backend):
    """Complete selected isolated jobs and return native-shaped Holdout assessments."""
    def __init__(self):
        super().__init__()
        self.finished = {'candidate_0', 'candidate_1'}
        self.reads = []

    def poll(self, row, job):
        """Keep one candidate running until the test explicitly completes it."""
        self.done = job.get('candidate_id') in self.finished
        return super().poll(row, job)

    def observations(self, row, job):
        """The comparison winner violates Holdout targets despite its larger score."""
        self.reads.append(job['operation'])
        value = super().observations(row, job)
        index = int(job['candidate_id'][-1])
        value['per_report'][0]['gain_strategy_eq'] = 10 + index
        value['assessment'].update(score=[50, 2, 3][index], hard_targets_met=index != 0)
        return value


def retained_incumbent(store, row):
    """Build three rounds retaining one immutable winner with settled Holdouts."""
    row = reference(store, row, rounds=3)
    row['baseline'] = None
    for entry in row['history']:
        entry.update(winner_operation=row['jobs'][0]['operation'], winner_candidate_id='candidate_0')
    row = store.update(OWNER, row['id'], lambda current: current.update(
        baseline=None, history=row['history']))
    observer = ObserverJobs(store, Backend(), owned_thread)
    row = observer.plan(row)
    def complete(current):
        """Use distinct user-only results to detect accidental candidate substitution."""
        for job in current['observer_jobs']:
            job.update(status='complete', report_done=True, simulation_complete=True,
                       assessment={'simulation_complete': True, 'comparable': True},
                       metrics={'gain': 1.25 + job['round'], 'drawdown': .2})
    row = store.update(OWNER, row['id'], complete)
    row = observer.plan(row)
    row = store.update(OWNER, row['id'], complete)
    return observer, row


def test_retained_incumbent_reuses_original_holdout_and_full_range(record):
    """Later rounds display the same native evidence with correct zero deltas."""
    store, row = record
    observer, row = retained_incumbent(store, row)
    original = copy.deepcopy(row)
    reports = _observer_reports(row)
    for number in (1, 2):
        selection = holdout_selection(row, number)
        assert selection['status'] == 'selected' and selection['source_round'] == 0
        chosen = [job for job in reports if job['round'] == number and job['selected_for_full_range']]
        assert {job['kind'] for job in chosen} == {'observer_holdout', 'observer_full_range'}
        for job in chosen:
            source = next(item for item in row['observer_jobs'] if item['operation'] == job['operation'])
            assert job['source_round'] == source['round'] == 0
            assert job['metrics']['gain'] == 1.25
            assert job['delta_previous'] == {'gain': 0.0, 'drawdown': 0.0}
    assert not observer.pending(row)
    assert observer.plan(row) == original and row == original
    assert len([job for job in row['observer_jobs'] if job['kind'] == 'observer_full_range']) == 3


@pytest.mark.parametrize('problem', ['unknown_operation', 'wrong_candidate', 'future_origin', 'incomplete', 'unread_report'])
def test_retained_incumbent_requires_matching_complete_origin(record, problem):
    """Never borrow unrelated, future, incomplete or unread user-only evidence."""
    store, row = record
    _, row = retained_incumbent(store, row)
    if problem == 'unknown_operation':
        row['history'][1]['winner_operation'] = 'f' * 32
    elif problem == 'wrong_candidate':
        row['history'][1]['winner_candidate_id'] = 'unrelated'
    elif problem == 'future_origin':
        row['jobs'][0]['round'] = 2
    else:
        source = next(job for job in row['observer_jobs'] if job['kind'] == 'observer_holdout' and job['round'] == 0)
        source['simulation_complete' if problem == 'incomplete' else 'report_done'] = False
    assert holdout_selection(row, 1)['status'] == 'unavailable'


def test_new_full_range_follows_ai_training_selection_without_holdout_ranking(record):
    """User-only evidence cannot replace the recorded AI winner, even with larger scores."""
    store, row = record
    row = candidates(store, row)
    def selected(current):
        """Record the AI's immutable training choice in isolated history."""
        current['history'][0].update(winner_operation=current['jobs'][0]['operation'],
                                     winner_candidate_id='candidate_0')
    row = store.update(OWNER, row['id'], selected)
    backend = RankedBackend()
    backend.finished.add('candidate_2')
    observer = ObserverJobs(store, backend, owned_thread)
    row = observer.plan(row)
    for _ in range(3):
        asyncio.run(observer.tick(store.read(OWNER, row['id'])))
    row = store.read(OWNER, row['id'])
    assert holdout_selection(row, 0)['candidate_id'] == 'candidate_0'
    ranges = [job for job in observer.plan(row)['observer_jobs'] if job['kind'] == 'observer_full_range']
    assert {job['candidate_id'] for job in ranges} == {'candidate_0', 'candidate_1', 'candidate_2'}


def test_full_range_is_scheduled_without_waiting_for_holdout(record):
    """An unfinished or bad Holdout never suppresses either candidate's full report."""
    store, row = record
    row = candidates(store, row)
    original = copy.deepcopy(row)
    backend = RankedBackend()
    backend.finished.clear()
    observer = ObserverJobs(store, backend, owned_thread)
    row = observer.plan(row)
    assert len(row['observer_jobs']) == 6
    for candidate in ('candidate_0', 'candidate_1', 'candidate_2'):
        assert {job['kind'] for job in row['observer_jobs'] if job['candidate_id'] == candidate} == {
            'observer_holdout', 'observer_full_range'}
    # Launch both report types while Holdouts are still unfinished.
    asyncio.run(observer.tick(row))
    row = store.read(OWNER, row['id'])
    assert any(job['kind'] == 'observer_full_range' and job['started'] for job in row['observer_jobs'])
    assert holdout_selection(row, 0)['status'] == 'waiting'
    backend.finished.update({'candidate_0', 'candidate_1', 'candidate_2'})
    for _ in range(3):
        asyncio.run(observer.tick(store.read(OWNER, row['id'])))
    row = store.read(OWNER, row['id'])
    assert not observer.pending(row) and len(observer.plan(row)['observer_jobs']) == 6
    assert all(row[key] == original[key] for key in ('history', 'best', 'rubric', 'stagnation', 'errors', 'validation_count'))
    ranges = [job for job in row['observer_jobs'] if job['kind'] == 'observer_full_range']
    assert all(job['status'] == 'complete' for job in ranges)
    assert next(job for job in ranges if job['candidate_id'] == 'candidate_2')['overrides']['BTC.json']['test'] == 2


@pytest.mark.parametrize('problem', ['incomplete', 'incomparable', 'missing_score'])
def test_no_valid_holdout_never_invents_full_range_winner(record, problem):
    """A bad Holdout cannot remove the independently scheduled Full Time Range."""
    store, row = record; row = candidates(store, row)
    observer = ObserverJobs(store, Backend(), owned_thread)
    row = observer.plan(row)
    def invalid(current):
        """Supply settled but unusable reports without launching any worker."""
        for job in current['observer_jobs']:
            job.update(status='complete', report_done=True, simulation_complete=True,
                       assessment={'simulation_complete': problem != 'incomplete',
                                   'comparable': problem != 'incomparable', 'score': None if problem == 'missing_score' else 1})
    row = store.update(OWNER, row['id'], invalid)
    assert holdout_selection(row, 0)['status'] == 'unavailable'
    assert not observer.pending(row) and len(observer.plan(row)['observer_jobs']) == 6


def test_legacy_reports_are_rescored_and_existing_full_range_is_retained(record):
    """Upgrade fills missing scores while retaining completed historical artifacts."""
    store, row = record; row = candidates(store, row)
    backend = RankedBackend(); backend.finished.add('candidate_2')
    observer = ObserverJobs(store, backend, owned_thread)
    row = observer.plan(row)
    legacy = copy.deepcopy(row['observer_jobs'][0])
    legacy.update(operation='f' * 32, kind='observer_full_range', status='complete',
                  report_done=True, simulation_complete=True, metrics={'gain': 99})
    legacy.pop('selection_policy')
    def old(current):
        """Retain an acknowledged old Holdout lacking its separate ranking score."""
        current['observer_jobs'][0].update(status='complete', report_done=True, simulation_complete=True)
        current['observer_jobs'][0].pop('selection_policy')
        current['observer_jobs'].append(legacy)
    row = store.update(OWNER, row['id'], old)
    for _ in range(3):
        asyncio.run(observer.tick(store.read(OWNER, row['id'])))
    row = store.read(OWNER, row['id'])
    assert next(job for job in row['observer_jobs'] if job['operation'] == legacy['operation']) == legacy
    assert holdout_selection(row, 0)['candidate_id'] == 'candidate_2'
    assert not observer.pending(row)
    reports = _observer_reports(row)
    assert next(job for job in reports if job['operation'] == legacy['operation'])['previous_selection']
    assert len([job for job in reports if job['kind'] == 'observer_full_range' and job['selected_for_full_range']]) == 1


@pytest.mark.parametrize('kind', ['observer_holdout', 'observer_full_range'])
@pytest.mark.parametrize('score', [3.5, None, float('nan'), True])
def test_evaluation_score_projection(kind, score):
    """Expose finite per-evaluation scores for display-only Top X ranking."""
    reports = _observer_reports({'observer_jobs':[{'kind':kind, 'round':0, 'status':'complete',
        'assessment':{'score':score, 'hard_targets_met':True}}]})
    expected = 3.5 if type(score) is float and score == 3.5 else None
    assert reports[0]['evaluation_score'] == expected
    assert reports[0]['evaluation_targets_met'] is True
    assert reports[0]['holdout_score'] == (expected if kind == 'observer_holdout' else None)
