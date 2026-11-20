"""Offline PB9 OHLCV adapters and shared helper ownership, without user data."""
import copy
import json
import os
from pathlib import Path
import sys
import threading
import time

import pytest
from fastapi import HTTPException

from api import backtest_v9, pb8_ohlcv_tools as shared, pb9_ohlcv_tools as native
from pb9_config import PB9CancelSignal, PB9ConfigError


@pytest.fixture
def runtime(tmp_path, monkeypatch):
    """Replace source inspection with synthetic roots and isolate controller state."""
    source = tmp_path / 'pb9'; source.mkdir()
    market = tmp_path / 'market'; market.mkdir()
    paths = {'pb9dir':str(source), 'pb9venv':str(tmp_path/'env/bin/python'),
             'pb8dir':str(tmp_path/'pb8'), 'pb7dir':''}
    config = {'config_version':'v9.0.0','bot':{'long':{'enabled':True}},
              'backtest':{'ohlcv_source_dir':str(market)}}
    identity = {'config_schema':'v9.0.0','package_version':'8.2.0.dev0'}
    monkeypatch.setattr(backtest_v9,'_native',lambda config,cancel:(copy.deepcopy(config),identity))
    monkeypatch.setattr(backtest_v9,'_paths',lambda:dict(paths))
    monkeypatch.setattr(shared,'_pbgui_market_data_root',lambda:market)
    for name in ('_START_DATE_JOBS','_START_DATE_THREADS','_START_DATE_PROCESSES'):
        monkeypatch.setattr(shared,name,{})
    class Lease:
        """Exercise ownership without acquiring any production update lock."""
        def release(self):
            """The fake lease owns no external resources."""
    monkeypatch.setattr(shared,'_acquire_runtime_lease',lambda:Lease())
    monkeypatch.setattr(shared,'_log',lambda *args,**kwargs:None)
    yield config, paths, tmp_path
    shared.shutdown_pb8_ohlcv_start_date_jobs()


def wait_until(check, timeout=5):
    """Wait only for an explicitly registered isolated helper condition."""
    deadline = time.monotonic() + timeout
    while not check():
        if time.monotonic() >= deadline:
            pytest.fail('Isolated OHLCV helper did not reach the expected state')
        time.sleep(.01)


def test_plan_uses_only_pb9_interpreter_and_sanitized_environment(runtime, monkeypatch):
    """No PB8 code/runtime or inherited Python injection reaches the PB9 helper."""
    config, paths, _ = runtime
    monkeypatch.setenv('PYTHONPATH','/untrusted')
    monkeypatch.setenv('PBGUI_SECRET_FIXTURE','do-not-forward')
    plan = native.start_date_plan(config)
    work = Path(plan['request']['work_dir'])
    try:
        assert plan['command'][:3] == [paths['pb9venv'],'-I','-B']
        assert plan['cwd'] == paths['pb9dir']
        assert plan['request']['config'] == config
        assert plan['request']['operation'] == 'ohlcv_start_dates'
        assert plan['request']['metadata_roots'] == [paths['pb8dir']]
        assert 'PYTHONPATH' not in plan['env'] and 'PBGUI_SECRET_FIXTURE' not in plan['env']
        assert work.is_dir() and work.stat().st_mode & 0o777 == 0o700
    finally:
        plan['cleanup']()
    assert not work.exists()


@pytest.mark.parametrize('fail',[True,False])
def test_preflight_retains_cancellation_and_always_removes_workspace(runtime,monkeypatch,fail):
    """Cancellation/errors cannot leave helper caches or public-data links owned by nobody."""
    config, paths, _ = runtime
    signal = PB9CancelSignal(); seen = []
    def probe(operation, repo, interpreter, other, **kwargs):
        """Inspect only synthetic transport inputs, never load native code."""
        seen.append(kwargs)
        assert operation == 'ohlcv_preflight' and str(repo) == paths['pb9dir']
        assert kwargs['cancel_event'] is signal
        assert Path(kwargs['probe_context']['work_dir']).exists()
        if fail:
            raise PB9ConfigError('helper_cancelled')
        return {'report':{'summary':{'headline':'Ready'}}}
    monkeypatch.setattr(native,'_config_operation',probe)
    if fail:
        with pytest.raises(PB9ConfigError):native.preflight(config,signal)
    else:
        assert native.preflight(config,signal)['summary']['headline']=='Ready'
    assert not Path(seen[0]['probe_context']['work_dir']).exists()


def test_external_source_is_rejected_before_probe_or_workspace(runtime):
    """The PB8 shared path policy also bounds native PB9 candle reads."""
    config, _, root = runtime
    config['backtest']['ohlcv_source_dir']=str(root/'outside')
    with pytest.raises(HTTPException) as error:
        native.start_date_plan(config)
    assert error.value.status_code == 422


def test_native_job_progress_and_result_are_login_and_runtime_scoped(runtime):
    """Real isolated subprocess transport exercises the existing progress collector."""
    config, _, root = runtime
    helper = root/'helper.py'
    helper.write_text('''import json,sys
request=json.load(sys.stdin)
print('PBGUI_OHLCV_START_PROGRESS {"phase":"resolving_pair","completed":1,"total":1}',file=sys.stderr,flush=True)
print(json.dumps({'ok':True,'result':{'start_date_options':{'earliest':{'available':True,'start_date':'2025-01-05'}}}}))
''')
    cleaned=[]
    def plan(config,cancel):
        """The controller owns only this private helper and cleanup callback."""
        return {'command':[sys.executable,'-I',str(helper)],'cwd':str(root),'env':{'PATH':os.defpath},
                'request':{'config':config},'cleanup':lambda:cleaned.append(True)}
    job = shared.start_pb8_ohlcv_start_date_job(config,runtime='v9',owner='alice',adapter=plan)['job_id']
    wait_until(lambda: native.get(job,'alice')['status']=='completed')
    wait_until(lambda: job not in shared._START_DATE_THREADS)
    result=native.get(job,'alice')
    assert result['progress']['percent']==100
    assert result['result']['start_date_options']['earliest']['start_date']=='2025-01-05'
    assert native.get(job,'bob') is None and native.stop(job,'bob') is None
    assert shared.get_pb8_ohlcv_start_date_job(job) is None
    assert shared.stop_pb8_ohlcv_start_date_job(job) is None
    assert cleaned==[True]


def test_stop_during_native_preparation_is_joined(runtime):
    """Stop is visible before a helper exists and shutdown joins that owner."""
    config, _, _ = runtime
    entered=threading.Event()
    def preparing(config,cancel):
        """Simulate a cancellable native inspection without a subprocess."""
        entered.set()
        wait_until(cancel.is_set)
        raise PB9ConfigError('helper_cancelled')
    job=shared.start_pb8_ohlcv_start_date_job(config,runtime='v9',owner='alice',adapter=preparing)['job_id']
    assert entered.wait(2)
    native.shutdown()
    assert native.get(job,'alice')['status']=='stopped'
    assert shared._START_DATE_THREADS==shared._START_DATE_PROCESSES=={}


@pytest.mark.parametrize('operation',['ohlcv_preflight','ohlcv_start_dates'])
def test_probe_protocol_binds_schema_and_does_not_return_identity_as_ui_data(tmp_path,operation):
    """Native OHLCV output retains the existing helper identity boundary."""
    from pb9_config import _checked_response
    status={'pb9dir':str(tmp_path/'source'),'config_schema':'v9.0.0','package_version':'8.2.0.dev0',
            'commit':None,'commit_available':False}
    result={'config_schema':'v9.0.0','package_version':'8.2.0.dev0',
            'schema_module_file':str(tmp_path/'source/src/config/schema.py'),
            'version_module_file':str(tmp_path/'source/src/passivbot_version.py'),
            'runtime':'pb9','summary':{'headline':'Ready'},'request':{'source_dir':'/approved'}}
    response=_checked_response(json.dumps({'ok':True,'result':result}).encode(),status,operation=operation)
    assert response['report']=={'runtime':'pb9','summary':{'headline':'Ready'},'request':{'source_dir':'/approved'}}
    result['config_schema']='v8.6.0'
    with pytest.raises(PB9ConfigError) as error:
        _checked_response(json.dumps({'ok':True,'result':result}).encode(),status,operation=operation)
    assert error.value.error_codes==('runtime_identity_mismatch',)


@pytest.mark.parametrize('work_mode',['missing','relative','symlink','source'])
def test_probe_context_is_rejected_before_native_imports(tmp_path,work_mode):
    """Only a separate private temporary workspace may receive metadata links."""
    from pb9_config_helper import handle, _HelperError
    source=tmp_path/'source';source.mkdir()
    work=tmp_path/'work';work.mkdir()
    if work_mode=='missing':work=tmp_path/'missing'
    elif work_mode=='relative':work=Path('relative')
    elif work_mode=='symlink':
        link=tmp_path/'link';link.symlink_to(work,target_is_directory=True);work=link
    else:work=source
    request={'operation':'ohlcv_preflight','pb9_dir':str(source),'config_schema':'v9.0.0','package_version':'8.2.0.dev0',
             'config':{'config_version':'v9.0.0'},'work_dir':str(work),'metadata_roots':[]}
    with pytest.raises(_HelperError,match='invalid_request'):
        handle(request)


def test_lookup_routes_refuse_other_owner_runtime_and_query_tokens(runtime):
    """Authenticated route boundaries cannot inspect or stop a different lookup."""
    from types import SimpleNamespace
    job='0123456789ab'
    shared._START_DATE_JOBS[job]={'job_id':job,'status':'completed','_runtime':'v9','_owner':'alice','finished_at':int(time.time()*1000)}
    request=SimpleNamespace(query_params={})
    alice=SimpleNamespace(user_id='alice');bob=SimpleNamespace(user_id='bob')
    assert backtest_v9.get_ohlcv_lookup(job,request,alice)['job_id']==job
    for handler in (backtest_v9.get_ohlcv_lookup,backtest_v9.stop_ohlcv_lookup):
        with pytest.raises(HTTPException) as error:handler(job,request,bob)
        assert error.value.status_code==404
        with pytest.raises(HTTPException) as error:handler(job,SimpleNamespace(query_params={'token':'never-used'}),alice)
        assert error.value.status_code==422
    shared._START_DATE_JOBS[job]['_runtime']='v8'
    with pytest.raises(HTTPException) as error:backtest_v9.get_ohlcv_lookup(job,request,alice)
    assert error.value.status_code==404


def test_runtime_capacity_and_history_do_not_evict_other_generation(runtime,monkeypatch):
    """PB9 inspection cannot consume PB8's lookup slots or terminal retention."""
    class Thread:
        """Admit queued jobs without starting external/native work."""
        def __init__(self,**kwargs):
            """No worker exists in this queue-capacity test."""
        def start(self):
            """Retain the queued state for admission checks."""
        def join(self,timeout=None):
            """The fake thread owns no resources."""
    monkeypatch.setattr(shared.threading,'Thread',Thread)
    config,_,_=runtime
    monkeypatch.setattr(shared,'_MAX_ACTIVE_START_DATE_JOBS',1)
    monkeypatch.setattr(shared,'_MAX_START_DATE_JOBS',2)
    v9=shared.start_pb8_ohlcv_start_date_job(config,runtime='v9',owner='alice',adapter=lambda *_:None)['job_id']
    v8=shared.start_pb8_ohlcv_start_date_job(config)['job_id']
    assert native.get(v9,'alice')['status']==shared.get_pb8_ohlcv_start_date_job(v8)['status']=='queued'
    with pytest.raises(shared.PB8OhlcvBusyError):native.start(config,'alice')
    now=int(time.time()*1000)
    for index in range(3):
        shared._START_DATE_JOBS['history'+str(index)]={'_runtime':'v9','finished_at':now,'status':'completed'}
    shared._cleanup_start_date_jobs_locked()
    assert v8 in shared._START_DATE_JOBS and v9 in shared._START_DATE_JOBS
    shared._START_DATE_JOBS.clear();shared._START_DATE_THREADS.clear()


@pytest.mark.parametrize('fallback,offline',[(False,False),(True,False),(False,True),(True,True)])
def test_online_probe_never_writes_through_published_metadata_links(tmp_path,monkeypatch,fallback,offline):
    """Cached metadata is read-only; online acquisition owns only private files."""
    from contextlib import contextmanager
    from types import SimpleNamespace
    import pb9_backtest_runtime
    import pb9_ohlcv_probe as probe
    work=tmp_path/'work';work.mkdir();public=tmp_path/'public.json';public.write_text('published')
    monkeypatch.setattr(pb9_backtest_runtime,'require_cached_market_metadata',lambda *_:None)
    config={'backtest':{'offline':offline},'bot':{'long':{'enabled':True},'short':{'enabled':False}}}
    policy_state=[];calls=[]
    class OfflineDataError(ValueError):
        """Simulate only a missing cached native metadata fact."""
    @contextmanager
    def policy(cfg):
        """Track the effective native policy without imports from a bot checkout."""
        policy_state.append(cfg['backtest']['offline'])
        try:yield
        finally:policy_state.pop()
    def prepare(work,*args):
        """Reference the published metadata without duplicating it."""
        (work/'metadata.json').symlink_to(public)
    async def build(payload,modules,*,cache_root):
        """An online native write must target a private file, never this link."""
        calls.append(policy_state[-1])
        assert payload['pb8_dir']==str(work)
        assert cache_root == tmp_path/'repo/caches'
        assert modules['approved_sources'](config)=={'long':['HYPE']}
        assert modules['minimum_coin_age_days'](config)==60
        assert modules['coin_minimum_age_ms'](config,['long'])==60*86400000
        if policy_state[-1]:
            assert (work/'metadata.json').is_symlink()
            if fallback:raise OfflineDataError()
        else:
            assert not (work/'metadata.json').exists()
            (work/'metadata.json').write_text('new native metadata')
        return {'summary':{},'request':{},'notes':[]}
    modules={'minimum_coin_age_days':lambda cfg,side:60,'backtest_side_enabled':lambda cfg,side:side=='long',
             'get_side_universe_sources':lambda *args:{'long':['HYPE'],'short':['DISABLED']},
             'simulation_data_policy':policy,'OfflineDataError':OfflineDataError}
    monkeypatch.setattr(probe,'_native_modules',lambda repo:modules)
    monkeypatch.setattr(probe,'_shared_planner',lambda:SimpleNamespace(_build=build))
    monkeypatch.setattr(pb9_backtest_runtime,'prepare_market_metadata',prepare)
    if fallback and offline:
        with pytest.raises(probe.ProbeError) as error:probe.build(config,tmp_path/'repo',work=work)
        assert error.value.error_code=='ohlcv_metadata_missing'
    else:
        assert probe.build(config,tmp_path/'repo',work=work)['runtime']=='pb9'
    assert calls==([True,False] if fallback and not offline else [True])
    assert public.read_text()=='published'


def test_prepared_dataset_is_reported_as_native_validation_not_raw_candle_failure(tmp_path):
    """An expert dataset bypass cannot falsely appear blocked by missing raw data."""
    from pb9_ohlcv_probe import build,ProbeError
    config={'backtest':{'hlcvs_data_dir':'/selected/dataset','offline':True}}
    result=build(config,tmp_path/'repo',work=tmp_path/'work')
    assert result['summary']['overall_status']=='unknown'
    assert result['summary']['headline']=='Prepared dataset selected'
    with pytest.raises(ProbeError) as error:build(config,tmp_path/'repo',work=tmp_path/'work',start_dates=True)
    assert error.value.error_code=='ohlcv_dataset_override'


def test_stop_running_native_transport_reaps_before_workspace_cleanup(runtime,monkeypatch):
    """PB9 Stop cancels the selector client without the legacy reader threads."""
    import pb9_config
    config,paths,root=runtime
    paths['pb9venv']=sys.executable
    helper=root/'waiting_probe.py'
    helper.write_text('''import json,sys,time
json.load(sys.stdin)
print('PBGUI_OHLCV_START_PROGRESS {"phase":"running","completed":1,"total":2}',file=sys.stderr,flush=True)
while True: time.sleep(.1)
''')
    monkeypatch.setattr(pb9_config,'_HELPER_PATH',helper)
    original=native.start_date_plan;workspaces=[]
    def recorded(raw,cancel):
        """Record only this test's parent-owned temporary directory."""
        plan=original(raw,cancel)
        workspaces.append(Path(plan['request']['work_dir']))
        return plan
    monkeypatch.setattr(native,'start_date_plan',recorded)
    job=native.start(config,'alice')['job_id']
    wait_until(lambda:native.get(job,'alice').get('progress',{}).get('phase')=='running')
    assert shared._START_DATE_PROCESSES=={}
    native.stop(job,'alice')
    wait_until(lambda:native.get(job,'alice')['status']=='stopped')
    wait_until(lambda:job not in shared._START_DATE_THREADS)
    assert workspaces and all(not path.exists() for path in workspaces)


@pytest.mark.parametrize('outcome',['success','wrong_schema','selection_error','timeout'])
def test_complete_production_transport_pipeline(runtime,monkeypatch,outcome):
    """Exercise progress, parent identity/error checks and timeout through plan.run."""
    import pb9_config
    config,paths,root=runtime
    paths['pb9venv']=sys.executable
    helper=root/'complete_probe.py'
    helper.write_text('''import json,sys,time
request=json.load(sys.stdin)
print('PBGUI_OHLCV_START_PROGRESS {"phase":"resolving","completed":1,"total":1}',file=sys.stderr,flush=True)
outcome='''+repr(outcome)+'''
if outcome=='timeout':time.sleep(10)
if outcome=='selection_error':
 print(json.dumps({'ok':False,'error_code':'ohlcv_coins_required','detail':'NEVER_EXPOSE_NATIVE_TEXT'}))
else:
 result={'config_schema':'v8.6.0' if outcome=='wrong_schema' else request['config_schema'],
  'package_version':request['package_version'],
  'schema_module_file':request['pb9_dir']+'/src/config/schema.py',
  'version_module_file':request['pb9_dir']+'/src/passivbot_version.py',
  'summary':{},'request':{},'start_date_options':{'earliest':{'available':True,'start_date':'2025-01-05'}}}
 print(json.dumps({'ok':True,'result':result}))
''')
    monkeypatch.setattr(pb9_config,'_HELPER_PATH',helper)
    if outcome=='timeout':monkeypatch.setattr(shared,'_START_DATE_TIMEOUT_SECONDS',.3)
    monkeypatch.setattr(shared,'_collect_start_date_output',lambda *args:pytest.fail('legacy collector used for native PB9'))
    job=native.start(config,'alice')['job_id']
    wait_until(lambda:native.get(job,'alice')['status'] in {'completed','error'})
    wait_until(lambda:job not in shared._START_DATE_THREADS)
    state=native.get(job,'alice')
    if outcome=='success':
        assert state['status']=='completed'
        assert state['result']['start_date_options']['earliest']['start_date']=='2025-01-05'
        assert state['progress']['percent']==100
    else:
        assert state['status']=='error' and state['result'] is None
        assert 'NEVER_EXPOSE_NATIVE_TEXT' not in state['error']
        if outcome=='selection_error':assert 'Select explicit approved coins' in state['error']
        if outcome=='timeout':
            assert state['error_category']=='timeout' and 'timed out' in state['error']


@pytest.mark.parametrize('outcome',['accepted','empty','rejected','failure'])
def test_source_readiness_uses_native_admission_and_restores_manager_range(outcome,tmp_path,monkeypatch):
    """Daily file presence cannot turn a native source-prefix rejection green."""
    import asyncio
    from types import SimpleNamespace
    from contextlib import contextmanager
    from pathlib import Path
    from pb9_ohlcv_probe import check_source_window
    class OfflineError(ValueError):
        """Represent the native loader's strict local-source exception."""
    calls=[]
    work=tmp_path/'work';repo=tmp_path/'native'
    work.mkdir();repo.mkdir();monkeypatch.chdir(work)
    @contextmanager
    def policy(config):
        """Source admission stays offline even when metadata lookup allowed remote I/O."""
        assert config=={'backtest':{'offline':True}}
        yield
    class Manager:
        """Keep this boundary test completely independent of real candle files."""
        start_ts=1733385540000
        end_ts=start_ts+3*86400000
        async def get_ohlcvs(self,coin,**options):
            """Admit data only through the actual loader-shaped interface."""
            calls.append((coin,options))
            assert Path.cwd()==repo
            assert (self.start_ts,self.end_ts)==(1733385540000,1733443140000)
            if outcome=='rejected':raise OfflineError('native prefix rejection')
            if outcome=='failure':raise RuntimeError('unexpected loader failure')
            return SimpleNamespace(empty=outcome=='empty')
        def update_timestamp_range(self,start,end):
            """Restore the owner's original multi-coin planning range."""
            calls.append((start,end))
            self.start_ts,self.end_ts=start,end
    manager=Manager()
    operation=check_source_window(manager,'HYPE',manager.start_ts,manager.end_ts,
                       offline_error=OfflineError,repo=repo,offline_policy=policy)
    if outcome=='failure':
        with pytest.raises(RuntimeError,match='unexpected loader failure'):asyncio.run(operation)
    else:
        report=asyncio.run(operation)
        assert report=={} if outcome=='accepted' else report['status']=='missing_local'
    assert calls[1][1]=={'source_dir_only':True}
    assert calls[-1]==(manager.start_ts,manager.end_ts)
    assert Path.cwd()==work
