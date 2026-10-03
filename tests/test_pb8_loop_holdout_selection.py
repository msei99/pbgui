"""Offline per-candidate Holdouts and a single, independently ranked full-range job."""
import asyncio
import copy

import pytest

from api.loop_optimizer_v8 import _observer_reports, projection
from pb8_loop_controller import owned_thread
from pb8_loop_observer import ObserverJobs, holdout_selection
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


def test_all_holdouts_settle_before_best_holdout_full_range(record):
    """Full range uses Holdout target/score ranking, not the comparison winner."""
    store, row = record; row = candidates(store, row)
    original = copy.deepcopy(row)
    backend = RankedBackend(); observer = ObserverJobs(store, backend, owned_thread)
    row = observer.plan(row)
    assert len(row['observer_jobs']) == 3
    assert all(job['kind'] == 'observer_holdout' for job in row['observer_jobs'])
    assert len(observer.plan(row)['observer_jobs']) == 3
    asyncio.run(observer.tick(row))
    row = store.read(OWNER, row['id'])
    assert holdout_selection(row, 0)['status'] == 'waiting'
    assert not any(job['kind'] == 'observer_full_range' for job in observer.plan(row)['observer_jobs'])
    backend.finished.add('candidate_2')
    asyncio.run(observer.tick(row))
    row = store.read(OWNER, row['id'])
    assert holdout_selection(row, 0)['candidate_id'] == 'candidate_2'
    asyncio.run(observer.tick(row))
    row = store.read(OWNER, row['id'])
    ranges = [job for job in row['observer_jobs'] if job['kind'] == 'observer_full_range']
    assert len(ranges) == 1 and ranges[0]['candidate_id'] == 'candidate_2'
    assert ranges[0]['overrides']['BTC.json']['test'] == 2
    assert ranges[0]['config']['bot']['long']['risk']['total_wallet_exposure_limit'] == 3
    assert not observer.pending(row) and len(observer.plan(row)['observer_jobs']) == 4
    assert all(row[key] == original[key] for key in ('history', 'best', 'rubric', 'stagnation', 'errors', 'validation_count'))
    reports = _observer_reports(row)
    assert [job['metrics']['gain'] for job in reports[:3]] == [10, 11, 12]
    assert [job['comparison_label'] for job in reports[:3]] == ['Comparison backtest 1', 'Comparison backtest 2', 'Comparison backtest 3']
    assert [job['candidate_id'] for job in reports if job['selected_for_full_range']] == ['candidate_2', 'candidate_2']
    assert projection(row)['observer_selections'][0]['candidate_id'] == 'candidate_2'


@pytest.mark.parametrize('problem', ['incomplete', 'incomparable', 'missing_score'])
def test_no_valid_holdout_never_invents_full_range_winner(record, problem):
    """A failed selection terminates diagnostics without using comparison ranking."""
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
    assert not observer.pending(row) and len(observer.plan(row)['observer_jobs']) == 3


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
