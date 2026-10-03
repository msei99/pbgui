"""Offline final Holdout reuse, immutable selection and model-context isolation."""
import asyncio
import copy
from pathlib import Path
import subprocess

import pytest

from pb8_loop_controller import LoopController
from pb8_loop_store import evaluate
from test_pb8_loop_optimizer import OWNER, RULE, record
from test_pb8_loop_observer import Backend, reference


class ProofBackend(Backend):
    """Supply isolated native snapshots and reports without running any real jobs."""
    gain = 3
    complete = True
    wrong_snapshot = False

    def poll(self, row, job):
        """Bind the fake completed runner to its immutable configuration."""
        executed = copy.deepcopy(job['config'])
        executed['backtest']['base_dir'] = 'native-output-folder'
        if self.wrong_snapshot:
            executed['bot']['long']['risk']['n_positions'] = 999
        return {'status': job['status'], 'started': job.get('started', False), 'executed_config': executed}

    def observations(self, row, job):
        """The other candidate has a better Holdout but cannot replace AI selection."""
        gain = self.gain if job['candidate_id'] == 'candidate_0' else 1000
        assessment = evaluate({'gain': gain}, row['rubric'])
        assessment['simulation_complete'] = self.complete
        return {'metrics': {'gain': gain}, 'reports': 1, 'exact': True,
                'per_report': [{'gain': gain}], 'assessment': assessment}


def prepared(record, tmp_path, target=2):
    """Persist two exact candidates and completed isolated Holdout reports."""
    store, row = record
    row = reference(store, row, rounds=2)
    backend = ProofBackend()
    controller = LoopController(tmp_path, backend=backend)
    assert controller.store.root == store.root
    row = store.update(OWNER, row['id'], lambda current: current.update(
        phase='evaluate', best={'candidate': current['jobs'][0]['candidate'],
                               'operation': current['jobs'][0]['operation'], 'assessment': {'score': 1}},
        rubric=[dict(RULE[0], target=target)]))
    row = controller.observers.plan(row)
    def complete(current):
        for job in current['observer_jobs']:
            job.update(status='complete', started=True, report_done=True, simulation_complete=True,
                       assessment={'simulation_complete': True, 'comparable': True,
                                   'score': 1000 if job['candidate_id'] == 'candidate_1' else 1,
                                   'hard_targets_met': True})
    row = store.update(OWNER, row['id'], complete)
    return store, row, controller, backend


@pytest.mark.parametrize('target', [2, None])
def test_selected_holdout_completes_without_new_jobs_or_ai(record, tmp_path, target):
    """Reuse the chosen exact candidate, including targetless preference-only goals."""
    store, row, controller, backend = prepared(record, tmp_path, target)
    before = copy.deepcopy(row)
    asyncio.run(controller.finish(row, 'Requested loops are complete'))
    checking = store.read(OWNER, row['id'])
    assert checking['status'] == 'finishing' and checking['phase'] == 'observer_confirmation'
    asyncio.run(controller._tick(checking))
    after = store.read(OWNER, row['id'])
    assert after['status'] == 'completed'
    result = after['final_validation'][0]
    assert result['assessment']['checked_candidate_id'] == 'candidate_0'
    assert result['metrics']['gain'] == 3
    assert after['best'] == before['best'] and after['history'] == before['history']
    assert after['jobs'] == before['jobs'] and after['observer_jobs'] == before['observer_jobs']
    assert after['validation_count'] == before['validation_count'] and after['ai_calls'] == before['ai_calls']
    assert not backend.starts and store.context(after) == []


@pytest.mark.parametrize('failure', ['targets', 'incomplete', 'snapshot', 'native_failure', 'qualitative'])
def test_failed_selected_check_stays_unconfirmed_without_adaptive_repair(record, tmp_path, failure):
    """A better other-candidate Holdout cannot repair the selected candidate's failure."""
    store, row, controller, backend = prepared(record, tmp_path)
    if failure == 'targets':
        backend.gain = 1
    elif failure == 'incomplete':
        backend.complete = False
    elif failure == 'snapshot':
        backend.wrong_snapshot = True
    elif failure == 'qualitative':
        row = store.update(OWNER, row['id'], lambda current: current['rubric'].append(dict(RULE[0], goal='custom', target=None)))
    else:
        row = store.update(OWNER, row['id'], lambda current: [job.update(status='failed')
            for job in current['observer_jobs'] if job['candidate_id'] == 'candidate_0'])
    asyncio.run(controller.finish(row, 'Search stopped'))
    asyncio.run(controller._tick(store.read(OWNER, row['id'])))
    after = store.read(OWNER, row['id'])
    assert after['status'] == 'unconfirmed' and after['errors'] == 0
    assert after['ai_calls'] == row['ai_calls'] and not after.get('repair_error') and not backend.starts
    assert after['best'] == row['best']


def test_confirmation_waits_for_selected_report_and_survives_reconstruction(record, tmp_path):
    """The frozen selection is checked after background reporting finishes."""
    store, row, controller, backend = prepared(record, tmp_path)
    row = store.update(OWNER, row['id'], lambda current: [job.update(status='running')
        for job in current['observer_jobs'] if job['candidate_id'] == 'candidate_0'])
    asyncio.run(controller.finish(row, 'Search stopped'))
    asyncio.run(controller._tick(store.read(OWNER, row['id'])))
    assert store.read(OWNER, row['id'])['status'] == 'finishing'
    store.update(OWNER, row['id'], lambda current: [job.update(status='complete')
        for job in current['observer_jobs'] if job['candidate_id'] == 'candidate_0'])
    replacement = LoopController(tmp_path, backend=backend)
    asyncio.run(replacement._tick(store.read(OWNER, row['id'])))
    assert store.read(OWNER, row['id'])['status'] == 'completed'


def test_final_evidence_never_enters_future_model_knowledge(record):
    """Legacy final reports and user-only reports are excluded before the context cap."""
    store, row = record
    store.remember(row, 'training', {'coins': [], 'knowledge': 'Training-only finding'})
    for number in range(15):
        store.remember(row, 'holdout-' + str(number), {'coins': [], 'holdout': {'start_date': '2020-02-01'},
            'knowledge': 'SECRET_HOLDOUT_RESULT', 'observations': [{'metrics': {'gain': 99999}}]})
    store.remember(row, 'observer', {'coins': [], 'observer_only': True, 'knowledge': 'SECRET_FULL_RANGE'})
    context = store.context(row)
    assert len(context) == 1 and context[0]['knowledge'] == 'Training-only finding'
    assert len(store.knowledge(OWNER)['entries']) == 17


def test_actual_result_renderer_distinguishes_checked_and_numeric_targets():
    """Render native final evidence without claiming targetless goals were achieved."""
    script = r"""
const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const source=fs.readFileSync('frontend/js/pb8_loop_optimizer.js','utf8');
const body=source.match(/  function resultSummary\([\s\S]*?\n  \}/)[0];
let rendered;
const context=vm.createContext({reportTable(parent,headers,rows){rendered=rows;},
 element(){return {};},goalTable(){},numeric(value){return value;}});
vm.runInContext(body,context);
function outcome(status,source,requirement){
 const row={status,settings:{},final_validation:[{assessment:{confirmation_source:source,goals:[{requirement}]}}]};
 context.resultSummary({appendChild(){}},row);
 return rendered.find(item=>item[0]==='Final confirmation')[1];
}
assert.equal(outcome('completed','observer_holdout',false),'Holdout checked');
assert.equal(outcome('completed','observer_holdout',true),'Holdout checked · numeric targets met');
assert.equal(outcome('unconfirmed','observer_holdout',true),'Unconfirmed — inspect holdout goals');
assert.equal(outcome('completed',undefined,true),'Confirmed');
"""
    result = subprocess.run(['node', '-e', script], cwd=Path(__file__).resolve().parents[1],
                            capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stdout + result.stderr
