"""Offline user-only simulations: immutable periods, lifecycle, UI data and AI isolation."""
import asyncio
import copy
import json
import time

import pytest
from fastapi import HTTPException

from api.loop_optimizer_v8 import _observer_reports
from pb8_loop_ai import LoopAI
from pb8_loop_backend import LoopBackend
from pb8_loop_controller import LoopController, owned_thread
from pb8_loop_observer import ObserverJobs, display_metrics
from pb8_loop_store import authorize_native_job, run_deletable
from test_pb8_loop_optimizer import OWNER, RULE, FakeAI, FakeBackend, config, record


class Backend(FakeBackend):
    """Controlled detached workers; no filesystem/process/provider operations."""
    observer_config = LoopBackend.observer_config
    validation_config = LoopBackend.validation_config

    def __init__(self):
        super().__init__()
        self.done = False
        self.broken = False

    def poll(self, row, job):
        """Keep observers running independently until the test completes them."""
        if not job.get('observer_only'):
            return super().poll(row, job)
        if job['operation'] in self.stops:
            return {'status':'cancelled', 'started':job['operation'] in self.starts}
        return {'status':'complete' if self.done else 'running' if job['operation'] in self.starts else 'queued',
                'started':job['operation'] in self.starts}

    def observations(self, row, job):
        """Return deliberately extreme monitor values, never optimization evidence."""
        if not job.get('observer_only'):
            return super().observations(row, job)
        if self.broken:
            raise ValueError('Observer report unavailable')
        return {'reports':1, 'per_report':[{'gain_strategy_eq':900000, 'drawdown_worst_strategy_eq':.2}],
                'assessment':{'simulation_complete':True,'comparable':True,'score':1,'hard_targets_met':True}}


def reference(store, row, rounds=1):
    """Create fixed comparison evidence and immutable round candidate snapshots."""
    jobs, history = [], []
    for number in range(rounds):
        operation = str(number + 1) * 32
        candidate = {'id':'candidate_'+str(number), 'config':config(), 'overrides':{'BTC.json':{'test':number}}}
        jobs.append({'operation':operation,'kind':'validation','round':number,'candidate':candidate,
                     'status':'complete','backend':None})
        history.append({'round':number,'observations':[{'operation':operation, 'assessment':{
            'comparable':True,'simulation_complete':True,'hard_targets_met':True,'score':1}}]})
    return store.update(OWNER,row['id'],lambda current:current.update(observer_enabled=True, jobs=jobs,
        history=history, baseline={'assessment':{'score':0}}, rubric=RULE,
        observer_holdouts=[{'label':'holdout','start_date':'2020-02-01','end_date':'2020-03-01',
                            'exchanges':['binance'],'starting_balance':1000}], comparison={**config()['backtest'],
        'suite_enabled':True,'scenarios':[{'start_date':'2020-01-01','end_date':'2020-01-10'}]}))


def test_fixed_periods_and_overrides_are_immutable(record):
    """Full range uses original dates; Holdout windows remain fixed even after final use."""
    store,row=record
    row=reference(store,row)
    row['holdouts']=[]
    candidate=copy.deepcopy(row['jobs'][0]['candidate'])
    candidate['config']['backtest']['start_date']='2020-01-09'
    candidate['config']['pbgui']['scenario_template']={'changed':True}
    original=copy.deepcopy(row)
    backend=Backend()
    holdout=backend.observer_config(row,candidate,'observer_holdout')
    full=backend.observer_config(row,candidate,'observer_full_range')
    assert holdout['backtest']['start_date']=='2020-02-01'
    assert holdout['backtest']['starting_balance']==1000
    assert 'starting_balance' not in holdout['backtest']['scenarios'][0]
    assert full['backtest']['start_date']=='2020-01-01'
    assert full['backtest']['end_date']=='2020-02-01'
    assert not full['backtest']['suite_enabled'] and 'scenarios' not in full['backtest']
    assert full['pbgui']=={'execution':'local'}
    assert row==original
    observer=ObserverJobs(store,backend,owned_thread)
    planned=observer.plan(row)
    assert len(planned['observer_jobs'])==2
    assert all(job['overrides']==candidate['overrides'] for job in planned['observer_jobs'] if job['round']==0)
    assert len(observer.plan(planned)['observer_jobs'])==2


def test_missing_holdout_is_explicit_and_does_not_invent_full_range_winner(record):
    """Without a valid Holdout, no Full Time Range winner can be selected."""
    store,row=record
    row=reference(store,row)
    row=store.update(OWNER,row['id'],lambda current:current.update(observer_holdouts=[]))
    backend=Backend();observer=ObserverJobs(store,backend,owned_thread)
    asyncio.run(observer.tick(row))
    jobs=store.read(OWNER,row['id'])['observer_jobs']
    assert [job['status'] for job in jobs if job['kind']=='observer_holdout']==['skipped','skipped']
    assert not backend.starts and not observer.pending(store.read(OWNER,row['id']))


def test_next_optimizer_and_terminal_collection_do_not_wait_for_observers(record, tmp_path):
    """A background monitor cannot block the next round or final normal completion."""
    store,row=record;row=reference(store,row)
    row=store.update(OWNER,row['id'],lambda current:current.update(phase='optimize',round=1,
        jobs=current['jobs']+[{'operation':'f'*32,'name':'next','kind':'optimizer','round':1,
                             'config':config(),'overrides':{},'status':'planned','started':False,'backend':None}]))
    backend=Backend();controller=LoopController(tmp_path,store,backend,FakeAI())
    asyncio.run(controller.tick(OWNER,row['id']))
    assert 'f'*32 in backend.starts
    current=store.read(OWNER,row['id'])
    assert len(current['observer_jobs'])==2
    assert all(job['status']=='running' for job in current['observer_jobs'])
    history=copy.deepcopy(current['history'])
    assert store.knowledge(OWNER)['entries']==[]
    store.update(OWNER,row['id'],lambda value:value.update(status='unconfirmed',cleanup_done=True))
    backend.done=True
    controller=LoopController(tmp_path,store,backend,FakeAI())
    asyncio.run(controller.tick(OWNER,row['id']))
    asyncio.run(controller.observers.tick(store.read(OWNER,row['id'])))
    current=store.read(OWNER,row['id'])
    assert all(job['report_done'] and job['simulation_complete'] for job in current['observer_jobs'])
    assert current['status']=='unconfirmed' and current['history']==history
    assert current['validation_count']==0 and current['best'] is None and current['stagnation']==0
    assert store.knowledge(OWNER)['entries']==[]
    assert not controller.observers.pending(current)


@pytest.mark.parametrize('status', ['paused','stopped'])
def test_pause_and_explicit_stop_guard_observer_launch(record,status):
    """Pause preserves jobs; explicit stop cancels without new workers."""
    store,row=record;row=reference(store,row)
    row=store.update(OWNER,row['id'],lambda current:current.update(status=status))
    backend=Backend();observer=ObserverJobs(store,backend,owned_thread)
    asyncio.run(observer.tick(row))
    current=store.read(OWNER,row['id'])
    assert not backend.starts
    assert all(job['status']==('planned' if status=='paused' else 'cancelled') for job in current['observer_jobs'])
    assert observer.pending(current)==(status=='paused')


def test_failed_ai_evaluation_recovers_current_round_without_changing_history(record):
    """An AI timeout cannot discard completed exact checkpoints or user-only checks."""
    store,row=record;row=reference(store,row,rounds=2)
    item=copy.deepcopy(row['history'][1]['observations'][0])
    item.update(exact=True,candidate=copy.deepcopy(row['jobs'][1]['candidate']))
    row=store.update(OWNER,row['id'],lambda current:current.update(status='failed',phase='evaluate',round=1,
        history=current['history'][:1],observations=[item],pending_ai={'stage':'evaluate'},errors=1,cleanup_done=True))
    original=copy.deepcopy(row)
    backend=Backend();backend.done=True
    observer=ObserverJobs(store,backend,owned_thread)
    assert [number for number,_ in observer.sources(row)]==[-1,0,1]
    for _ in range(3):
        asyncio.run(observer.tick(store.read(OWNER,row['id'])))
    current=store.read(OWNER,row['id'])
    checks=[job for job in current['observer_jobs'] if job['round']==1]
    assert {job['kind'] for job in checks}=={'observer_holdout','observer_full_range'}
    assert all(job['status']=='complete' and job['report_done'] for job in checks)
    assert len(current['observer_jobs'])==6 and not observer.pending(current)
    for key in ('status','history','observations','pending_ai','best','errors','stagnation','validation_count'):
        assert current[key]==original[key]


@pytest.mark.parametrize('rejection', ['not_exact','incomplete','failed_job','foreign_round'])
def test_current_checkpoint_requires_complete_exact_owned_round(record,rejection):
    """Unfinished or unrelated simulations cannot become recovered observer sources."""
    from pb8_loop_store import completed_round_observations
    store,row=record;row=reference(store,row)
    item=copy.deepcopy(row['history'][0]['observations'][0]);item['exact']=True
    row.update(history=[],phase='evaluate',observations=[item])
    if rejection=='not_exact':item['exact']=False
    elif rejection=='incomplete':item['assessment']['simulation_complete']=False
    elif rejection=='failed_job':row['jobs'][0]['status']='failed'
    else:row['jobs'][0]['round']=1
    assert completed_round_observations(row,0)==[]


def test_running_observers_cancel_and_completed_reports_do_not_leave_pending(record):
    """Stopping cancels real workers and resolves a completed but unread report."""
    store,row=record;row=reference(store,row)
    backend=Backend();observer=ObserverJobs(store,backend,owned_thread)
    asyncio.run(observer.tick(row))
    def stop(current):
        current.update(status='stopped',cleanup_done=True)
        current['observer_jobs'][0].update(status='complete',report_done=False)
    row=store.update(OWNER,row['id'],stop)
    assert not run_deletable(row)
    asyncio.run(observer.tick(row))
    current=store.read(OWNER,row['id'])
    assert len(backend.stops)==1 and run_deletable(current)
    assert not observer.pending(current)


def test_deadline_prevents_new_jobs_but_collects_running_ones(record):
    """The budget deadline prevents launching any later user evaluation."""
    store,row=record;row=reference(store,row)
    backend=Backend();observer=ObserverJobs(store,backend,owned_thread)
    asyncio.run(observer.tick(row))
    row=store.update(OWNER,row['id'],lambda current:current.update(deadline=time.time()-1))
    backend.done=True
    asyncio.run(observer.tick(row))
    assert all(job['simulation_complete'] for job in store.read(OWNER,row['id'])['observer_jobs'])
    assert len(backend.starts)==2
    asyncio.run(observer.tick(store.read(OWNER,row['id'])))
    assert len(backend.starts)==2
    assert all(job['status']=='cancelled' for job in store.read(OWNER,row['id'])['observer_jobs'] if job['kind']=='observer_full_range')


def test_report_failure_never_changes_optimizer_state(record, tmp_path):
    """Report retries remain isolated from global errors, repair logic and learning."""
    store,row=record;row=reference(store,row)
    backend=Backend();controller=LoopController(tmp_path,store,backend,FakeAI())
    row=store.update(OWNER,row['id'],lambda current:current.update(status='unconfirmed',cleanup_done=True))
    asyncio.run(controller.tick(OWNER,row['id']))
    backend.done=backend.broken=True
    for _ in range(3):
        asyncio.run(controller.tick(OWNER,row['id']))
    current=store.read(OWNER,row['id'])
    assert current['status']=='unconfirmed' and current['errors']==0
    assert not current.get('repair_error') and current['best'] is None
    assert all(job['report_done'] and job['error']=='Observer report unavailable' for job in current['observer_jobs'])
    assert all(job['display_status']=='report error' for job in _observer_reports(current))
    assert not controller.observers.pending(current)
    assert store.knowledge(OWNER)['entries']==[]


def test_permanent_observer_start_failure_cancels_only_owned_waiting_jobs(record):
    """A rejected start is reconciled and does not retry forever or block later rounds."""
    store,row=record;row=reference(store,row)
    class Rejected(Backend):
        """Simulate a local schema/version rejection before any simulation starts."""
        def start(self,current,job):
            raise ValueError('PB8 version changed')
    backend=Rejected();observer=ObserverJobs(store,backend,owned_thread)
    asyncio.run(observer.tick(row))
    current=store.read(OWNER,row['id'])
    assert len(backend.stops)==2 and not backend.starts
    assert not observer.pending(current) and current['errors']==0
    assert all(job['report_done'] and job['error']=='PB8 version changed' for job in current['observer_jobs'])


def test_delete_waits_for_successful_result_collection(record):
    """Finished simulations cannot be deleted while their metric reports are pending."""
    store,row=record
    row=store.update(OWNER,row['id'],lambda current:current.update(status='unconfirmed',cleanup_done=True,
        observer_jobs=[{'status':'complete','report_done':False}]))
    assert not run_deletable(row)


def test_ai_payload_and_learning_never_include_user_metrics(record,monkeypatch):
    """Inspect the actual serialized AI request after a dramatic user-only result."""
    store,row=record
    marker='PRIVATE_OBSERVER_RESULT'
    row=store.update(OWNER,row['id'],lambda current:current.update(rubric=RULE,
        observer_jobs=[{'name':marker,'metrics':{'gain':987654321}}]))
    ai=LoopAI(store)
    async def preflight(*args):
        return {'output_limit':1000}
    async def request(current,metadata,content,output_tokens):
        payload=json.dumps(content)
        assert marker not in payload and '987654321' not in payload
        assert 'observer_jobs' not in payload
        return '{"finish":false}',{}
    monkeypatch.setattr(ai,'preflight',preflight);monkeypatch.setattr(ai,'_request',request)
    asyncio.run(ai.decide(row,'evaluate',{'observations':[]}))
    assert store.knowledge(OWNER)['entries']==[]


@pytest.mark.parametrize('status',['unconfirmed','completed','failed'])
def test_native_authorization_requires_exact_owned_observer_binding(record,tmp_path,status):
    """Only an owned observer can launch after completion/failure, within its deadline."""
    store,row=record
    data={'loop_id':row['id'],'loop_owner':OWNER,'filename':'native','operation_id':'f'*32,'loop_observer':True}
    row=store.update(OWNER,row['id'],lambda current:current.update(status=status,observer_jobs=[{
        'observer_only':True,'operation':'f'*32,'backend':{'id':'native'}}]))
    authorize_native_job(tmp_path,data,row['id'])
    backend=LoopBackend(tmp_path,store)
    assert backend.authorized(row,observer=True)['status']==status
    with pytest.raises(ValueError):backend.authorized(row)
    for mutation in ({'filename':'foreign'}, {'operation_id':'b'*32}, {'loop_observer':False}, {'loop_owner':'b'*32}):
        with pytest.raises(HTTPException):
            authorize_native_job(tmp_path,{**data,**mutation},row['id'])


def test_reducers_and_delta_projection_are_independent_of_rubric():
    """Incomplete/missing reports cannot claim improvement; reducers are fixed."""
    metrics=display_metrics({'per_report':[{'gain_strategy_eq':2,'drawdown_worst_strategy_eq':.2,'n_fills':2},
                                           {'gain_strategy_eq':4,'drawdown_worst_strategy_eq':.5,'n_fills':3}]})
    assert metrics['gain']==3 and metrics['drawdown']==.5 and metrics['trades']==5
    assert metrics['sharpe'] is None
    jobs=[{'operation':str(number+2)*32,'kind':'observer_holdout','round':number,'status':'complete',
           'metrics':{'gain':number+2,'drawdown':.5-number*.1},'simulation_complete':True,'report_done':True}
          for number in (-1,0,1)]
    reports=_observer_reports({'observer_jobs':jobs})
    assert reports[2]['delta_start']['gain']==2
    assert reports[2]['delta_previous']['gain']==1
    assert reports[2]['delta_previous']['drawdown']==pytest.approx(-.1)
    jobs[2]['simulation_complete']=False
    assert _observer_reports({'observer_jobs':jobs})[2]['display_status']=='incomplete'
    assert _observer_reports({'observer_jobs':jobs})[2]['delta_start']=={}
