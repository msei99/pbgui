"""Offline named AI Loop config, source, queue and immutable history contracts."""
from __future__ import annotations

import asyncio
import copy
import fcntl
import json
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api import loop_optimizer_v8 as api_loop
from api.auth import require_auth
from pb8_loop_controller import LoopController
from pb8_loop_store import LoopStore, apply_run_limit

OWNER = 'a' * 32
OTHER = 'b' * 32


def bundle():
    """Return an isolated PB8 input including a sparse coin override."""
    return {'config': {'backtest': {'start_date':'2020-01-01','end_date':'2020-02-01','exchanges':['binance']},
            'bot': {side:{'risk':{'n_positions':2,'total_wallet_exposure_limit':1}} for side in ('long','short')},
            'live': {'approved_coins':{'long':['BTC'],'short':['BTC']}},
            'optimize': {'backend':'pymoo','bounds':{},'n_cpus':1,'iters':1000}, 'pbgui': {}},
            'override_configs': {'BTC.json': {'bot':{'long':{'risk':{'n_positions':1}}}}}}


def settings():
    """Return saved loop settings without a separate AI selection."""
    value = api_loop.LoopStart(config_name='original', provider='chatgpt', model='shared').model_dump()
    return {key:child for key,child in value.items() if key not in {'authorization','provider','model','profile','effort','service_tier'}}


@pytest.mark.parametrize('name', ['', '.', '..', '../escape', 'x/y', 'x\\y', 'x\x00y', 'x\ny'])
def test_loop_definition_rejects_unsafe_names(tmp_path, name):
    """A configuration name cannot select files outside its owner namespace."""
    store=LoopStore(tmp_path/'loops')
    with pytest.raises(ValueError):
        store.save_definition(OWNER,name,settings(),bundle(),{'kind':'config','id':'original'})


def test_named_definitions_isolate_owners_and_reject_stale_saves(tmp_path):
    """Edit updates the same definition, rename duplicates, and stale writes fail."""
    store=LoopStore(tmp_path/'loops'); source={'kind':'config','id':'original'}
    saved=store.save_definition(OWNER,'My loop',settings(),bundle(),source)
    assert store.definitions(OTHER)==[] and store.list(OWNER)==[]
    changed=settings();changed['hours']=10
    updated=store.save_definition(OWNER,'My loop',changed,bundle(),source,saved['revision'])
    assert updated['revision']==2 and updated['created_at']==saved['created_at']
    with pytest.raises(ValueError,match='changed'):
        store.save_definition(OWNER,'My loop',settings(),bundle(),source,saved['revision'])
    assert store.definition(OWNER,'My loop')['settings']['hours']==10
    store.save_definition(OWNER,'Renamed loop',changed,bundle(),source)
    assert len(store.definitions(OWNER))==2
    with pytest.raises(FileNotFoundError):
        store.definition(OTHER,'My loop')
    store.delete_definition(OWNER,'My loop')
    with pytest.raises(ValueError,match='deleted'):
        store.save_definition(OWNER,'My loop',changed,bundle(),source,updated['revision'])
    assert [row['name'] for row in store.definitions(OWNER)]==['Renamed loop']
    assert store.definition_path(OWNER,'Renamed loop').stat().st_mode & 0o777 == 0o600


def test_named_definition_refuses_symlink_namespace(tmp_path):
    """A redirected owner configuration directory never receives a write."""
    store=LoopStore(tmp_path/'loops'); outside=tmp_path/'outside';outside.mkdir()
    (store.owner_dir(OWNER)/'configs').symlink_to(outside,target_is_directory=True)
    with pytest.raises(ValueError,match='symlink'):
        store.save_definition(OWNER,'Name',settings(),bundle(),{})
    assert list(outside.iterdir())==[]


@pytest.fixture
def workflow(tmp_path,monkeypatch):
    """Serve the real routes with isolated snapshots and no processes or providers."""
    source=bundle();preflights=[];session=SimpleNamespace(user_id=OWNER)
    class Backend:
        """Resolve only the supplied immutable bundle without touching runtime data."""
        def initial(self,name,values,snapshot=None):
            """Apply run settings to an isolated input and return its comparison scope."""
            value=copy.deepcopy(snapshot or source)
            config=apply_run_limit(values,value['config'])
            config['optimize']['backend']='pymoo' if values['execution']=='cpu' else 'gpu'
            config['pbgui']['execution']='vast' if values['execution']=='vast' else 'local'
            return config,value['override_configs'],{'pb8':'test','data':'test','data_status':'verified_files'},[],config['backtest']
    class AI:
        """Record read-only eligibility checks without making an AI request."""
        async def preflight(self,owner,values):
            """Confirm that tests retain the chosen shared model."""
            preflights.append((owner,copy.deepcopy(values)))
    controller=LoopController(tmp_path,store=LoopStore(tmp_path/'loops'),backend=Backend(),ai=AI())
    monkeypatch.setattr(api_loop,'_controller',controller)
    monkeypatch.setattr(api_loop,'_owner',lambda value:value.user_id)
    monkeypatch.setattr(api_loop,'get_ai_chat_service',lambda:SimpleNamespace(get_preferences=lambda owner:{'jev_max_cost_usd':.01}))
    monkeypatch.setattr(api_loop,'_log',lambda *args,**kwargs:None)
    from api import optimize_v8
    import contextlib
    for name in ('_config_lock','_queue_lock','_result_lock'):
        monkeypatch.setattr(optimize_v8,name,lambda:contextlib.nullcontext())
    monkeypatch.setattr(optimize_v8,'get_config',lambda name,session:copy.deepcopy(source))
    monkeypatch.setattr(api_loop,'migrate_loop_bundle',lambda value:copy.deepcopy(value))
    app=FastAPI();app.include_router(api_loop.router,prefix='/loops')
    app.dependency_overrides[require_auth]=lambda:session
    with TestClient(app) as client:
        yield client,controller,source,session,preflights


def save(client,name='Named',values=None,source=None,**extra):
    """Save through the public definition route."""
    return client.put('/loops/configs/'+name,json={'settings':values or settings(),'source':source or {'kind':'config','id':'original'},**extra})


def test_knowledge_sidebar_limit_retains_owner_scope(workflow):
    """Read the bounded central index without crossing accounts or changing evidence."""
    client,controller,source,session,preflights=workflow
    record={'id':'c'*32,'owner':OWNER,'fingerprint':{'pb8':'test'}}
    for number in range(25):
        controller.store.remember(record,f'finding:{number}',{'knowledge':f'Finding {number}','coins':['HYPE'],'status':'observed_exact'})
    controller.store.remember({**record,'owner':OTHER},'other-finding',{'knowledge':'Other account'})
    response=client.get('/loops/knowledge')
    assert response.status_code==200 and response.headers['cache-control']=='no-store'
    assert len(response.json()['entries'])==20
    response=client.get('/loops/knowledge?limit=200')
    assert response.status_code==200 and len(response.json()['entries'])==25
    assert response.json()['entries'][0]['knowledge']=='Finding 0'
    assert len(client.get('/loops/knowledge?limit=1').json()['entries'])==1
    assert client.get('/loops/knowledge?limit=0').status_code==422
    assert client.get('/loops/knowledge?limit=201').status_code==422
    session.user_id=OTHER
    assert [item['knowledge'] for item in client.get('/loops/knowledge?limit=200').json()['entries']]==['Other account']
    assert len(controller.store.knowledge(OWNER)['entries'])==25 and not preflights


def queue(client,name='Named'):
    """Queue with the selection obtained from the shared AI assistant."""
    return client.post('/loops/configs/'+name+'/queue',json={'provider':'chatgpt','model':'shared-current','profile':'default','authorization':True})


@pytest.mark.parametrize('source_kind', ['definition', 'run'])
def test_save_old_snapshot_migrates_before_queue_and_preserves_history(workflow, monkeypatch, source_kind):
    """Raw digest checks precede automatic migration; saved/queued copies use the new schema."""
    import pb8_config
    from pb8_loop_backend import migrate_loop_bundle
    client, controller, source, session, _ = workflow
    source['config']['config_version'] = 'v8.4.0'
    source['config']['coin_overrides'] = {'BTC': {'override_config_path': 'BTC.json'}}
    for side in ('long', 'short'):
        source['config']['bot'][side]['hsl'] = {'enabled': False, 'restart_after_red_policy': 'threshold'}
    original = copy.deepcopy(source)
    definition = controller.store.save_definition(OWNER, 'Named', settings(), source, {'kind':'config','id':'original'})
    history = controller.store.create(OWNER, settings(), source['config'], source['override_configs'], {}, [], source['config']['backtest'], queued=True)
    history_before = controller.store.read(OWNER, history['id'])
    selector = {'kind': source_kind, 'id': 'Named' if source_kind == 'definition' else history['id']}
    opened = client.post('/loops/sources', json=selector).json()
    monkeypatch.setattr(pb8_config, '_cache_config', lambda *args: None)

    def migrated(path, choices):
        """Exercise real snapshot staging without importing a production runtime."""
        config = json.loads(path.read_text())
        assert choices == {}
        config['config_version'] = 'v8.6.0'
        for side in ('long', 'short'):
            config['bot'][side]['hsl']['restart_after_red_policy'] = None
        return {'config': config}

    monkeypatch.setattr(pb8_config, 'preview_pb8_hsl_migration', migrated)
    monkeypatch.setattr(api_loop, 'migrate_loop_bundle', migrate_loop_bundle)
    response = save(client, source=selector, revision=definition['revision'], source_digest=opened['bundle_digest'])
    assert response.status_code == 200, response.text
    stored = controller.store.definition(OWNER, 'Named')
    assert stored['bundle']['config']['config_version'] == 'v8.6.0'
    assert stored['bundle']['override_configs'] == original['override_configs']
    assert stored['revision'] == definition['revision'] + 1
    queued = queue(client)
    assert queued.status_code == 201, queued.text
    actual = controller.store.read(OWNER, queued.json()['id'])
    assert actual['initial_config']['config_version'] == 'v8.6.0'
    assert actual['initial_overrides'] == original['override_configs']
    assert controller.store.read(OWNER, history['id']) == history_before
    assert source == original


def test_failed_migration_does_not_replace_saved_definition(workflow, monkeypatch):
    """Unresolved native policy errors preserve the last saved revision and create no run."""
    client, controller, *_ = workflow
    definition = save(client).json()
    before = controller.store.definition(OWNER, 'Named')

    def unresolved(value):
        """Reject an old enabled threshold instead of choosing a restart policy."""
        raise ValueError('HSL requires an explicit choice of always or never')

    monkeypatch.setattr(api_loop, 'migrate_loop_bundle', unresolved)
    response = save(client, source={'kind':'definition','id':'Named'}, revision=definition['revision'])
    assert response.status_code == 422
    assert 'explicit choice' in response.json()['detail']
    assert controller.store.definition(OWNER, 'Named') == before
    assert controller.store.list(OWNER) == []


def test_source_editor_unresolved_policy_survives_bundle_boundary(workflow):
    """A native editor's pending choice cannot disappear when metadata is stripped."""
    client, controller, source, *_ = workflow
    source['migration_unresolved'] = ['bot.long.hsl.restart_after_red_policy']
    response = save(client)
    assert response.status_code == 422
    assert 'explicit HSL choice' in response.json()['detail']
    assert controller.store.definitions(OWNER) == []
    assert controller.store.list(OWNER) == []


def test_saved_definition_queue_start_and_history_are_separate(workflow):
    """Queue waits without consuming time; editing the definition preserves its run."""
    client,controller,source,session,preflights=workflow
    draft=client.post('/loops/sources',json={'kind':'config','id':'original'})
    assert draft.status_code==200 and controller.store.definitions(OWNER)==[]
    assert draft.json()['execution']=='cpu'
    definition=save(client).json();assert 'provider' not in definition['settings']
    response=queue(client);assert response.status_code==201,response.text
    waiting=response.json();assert waiting['status']=='queued' and waiting['deadline'] is None and waiting['ai_calls']==0
    loop_id=waiting['id'];record=controller.store.read(OWNER,loop_id)
    asyncio.run(controller._tick(record))
    assert controller.store.read(OWNER,loop_id)==record
    source['config']['optimize']['iters']=5000
    values=settings();values['hours']=8
    changed=save(client,values=values,source={'kind':'definition','id':'Named'},revision=definition['revision'])
    assert changed.status_code==200,changed.text
    assert controller.store.read(OWNER,loop_id)==record
    started=client.post('/loops/'+loop_id+'/action',json={'action':'start','selection':{'provider':'chatgpt','model':'new-shared'}})
    assert started.status_code==200,started.text
    row=controller.store.read(OWNER,loop_id)
    assert row['status']=='running' and row['deadline']-row['started_at']==pytest.approx(24*3600,abs=.1)
    assert row['initial_config']['optimize']['iters']==1000 and row['initial_overrides']==bundle()['override_configs']
    assert row['settings']['model']=='new-shared' and row['settings']['hours']==24
    assert len(preflights)==2
    assert client.post('/loops/'+loop_id+'/action',json={'action':'start'}).status_code==422
    assert client.post('/loops/'+loop_id+'/action',json={'action':'pause'}).json()['status']=='paused'
    assert client.post('/loops/'+loop_id+'/action',json={'action':'resume'}).json()['status']=='running'
    assert client.delete('/loops/configs/Named').status_code==200
    assert controller.store.read(OWNER,loop_id)['initial_overrides']==bundle()['override_configs']


def test_queue_edit_refreshes_only_waiting_snapshot(workflow):
    """Normal Edit/Save can update the waiting entry without overwriting executed jobs."""
    client,controller,source,session,_=workflow
    definition=save(client).json();row=queue(client).json();values=settings();values.update(run_limit_mode='iters',run_iters=777,hours=6)
    changed=save(client,values=values,source={'kind':'run','id':row['id']},revision=definition['revision'],queue_id=row['id'])
    assert changed.status_code==200,changed.text
    waiting=controller.store.read(OWNER,row['id'])
    assert waiting['initial_config']['optimize']['iters']==777 and waiting['settings']['hours']==6
    assert waiting['control_generation']==1
    assert client.post('/loops/'+row['id']+'/action',json={'action':'start'}).status_code==200
    executed=controller.store.read(OWNER,row['id'])
    forbidden=save(client,values=settings(),source={'kind':'run','id':row['id']},revision=changed.json()['revision'],queue_id=row['id'])
    assert forbidden.status_code==422
    assert controller.store.read(OWNER,row['id'])==executed
    assert controller.store.definition(OWNER,'Named')['settings']['hours']==6


def test_saved_loop_routes_enforce_owner_and_revision(workflow):
    """Named endpoints and history sources cannot cross accounts or lose edits."""
    client,controller,_,session,_=workflow
    first=save(client).json();loop=queue(client).json()
    assert save(client,values=settings()).status_code==422
    assert controller.store.definition(OWNER,'Named')['revision']==first['revision']
    session.user_id=OTHER
    assert client.get('/loops/configs').json()=={'configs':[]}
    assert client.get('/loops').json()=={'loops':[]}
    assert queue(client).status_code==404
    assert client.post('/loops/sources',json={'kind':'run','id':loop['id']}).status_code==404
    assert client.delete('/loops/configs/Named').status_code==404


def test_delete_waiting_loop_retains_config_and_learned_findings(workflow):
    """Delete removes the queue snapshot without touching definitions or shared evidence."""
    client,controller,_,_,_=workflow
    definition=save(client).json();row=queue(client).json();loop_id=row['id']
    controller.store.remember(controller.store.read(OWNER,loop_id),'prior-finding',{'knowledge':'Useful learning'})
    knowledge=controller.store.knowledge(OWNER)
    assert client.get('/loops/'+loop_id).json()['deletable'] is True
    assert client.delete('/loops/'+loop_id).json()=={'deleted':True}
    assert client.get('/loops').json()=={'loops':[]}
    assert controller.store.definition(OWNER,'Named')['revision']==definition['revision']
    assert controller.store.knowledge(OWNER)==knowledge
    assert list((controller.store.owner_dir(OWNER)/'evidence').glob('*.json'))
    assert client.get('/loops/'+loop_id).status_code==404
    assert client.delete('/loops/'+loop_id).status_code==404
    assert client.post('/loops/'+loop_id+'/action',json={'action':'start'}).status_code==404
    asyncio.run(controller.tick(OWNER,loop_id))
    assert controller.store.list(OWNER)==[]


@pytest.mark.parametrize('status',['running','finishing','paused','stopping'])
def test_delete_refuses_active_loop(workflow,status):
    """Deleting cannot abandon a controller, pending decisions or running native jobs."""
    client,controller,_,_,_=workflow
    save(client);loop_id=queue(client).json()['id']
    before=controller.store.update(OWNER,loop_id,lambda row:row.update(status=status))
    assert client.get('/loops/'+loop_id).json()['deletable'] is False
    assert client.delete('/loops/'+loop_id).status_code==409
    assert controller.store.read(OWNER,loop_id)==before


@pytest.mark.parametrize('status',['completed','failed','unconfirmed','stopped'])
def test_delete_finished_loop_requires_collection(workflow,status):
    """Terminal status alone does not permit deletion before job/rental cleanup."""
    client,controller,_,_,_=workflow
    save(client);loop_id=queue(client).json()['id']
    controller.store.update(OWNER,loop_id,lambda row:row.update(status=status,cleanup_done=False))
    assert client.delete('/loops/'+loop_id).status_code==409
    controller.store.update(OWNER,loop_id,lambda row:row.update(cleanup_done=True))
    assert client.get('/loops/'+loop_id).json()['deletable'] is True
    assert client.delete('/loops/'+loop_id).status_code==200


def test_delete_is_owned_and_excludes_a_controller_transition(workflow):
    """Other owners and a concurrent controller cannot delete this run."""
    client,controller,_,session,_=workflow
    save(client);loop_id=queue(client).json()['id']
    session.user_id=OTHER
    assert client.delete('/loops/'+loop_id).status_code==404
    session.user_id=OWNER
    lock_path=controller.store.path(OWNER,loop_id).with_suffix('.controller')
    with lock_path.open('a+') as handle:
        fcntl.flock(handle.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
        assert client.delete('/loops/'+loop_id).status_code==409
    assert client.delete('/loops/'+loop_id).status_code==200


def test_delete_refuses_unfinished_child_and_symlink(workflow,tmp_path):
    """An unfinished owned backend or redirected controller file prevents removal."""
    from pb8_loop_store import LoopDeletionConflict
    client,controller,_,_,_=workflow
    save(client);loop_id=queue(client).json()['id']
    controller.store.update(OWNER,loop_id,lambda row:row.update(status='failed',cleanup_done=True,jobs=[{'backend':{'id':'native'},'status':'running'}]))
    with pytest.raises(LoopDeletionConflict):
        controller.delete(OWNER,loop_id)
    controller.store.update(OWNER,loop_id,lambda row:row.update(jobs=[]))
    controller_path=controller.store.path(OWNER,loop_id).with_suffix('.controller')
    controller_path.unlink()
    outside=tmp_path/'outside';outside.write_text('keep')
    controller_path.symlink_to(outside)
    assert client.delete('/loops/'+loop_id).status_code==422
    assert outside.read_text()=='keep' and controller.store.read(OWNER,loop_id)['status']=='failed'


@pytest.mark.parametrize('execution',['cpu','gpu','vast'])
def test_native_optimizer_log_target_checks_real_job_ownership(workflow,monkeypatch,tmp_path,execution):
    """Log handoff resolves a real native identity, not the model's operation or job name."""
    from api import optimize_v8
    from pb8_loop_backend import LoopBackend
    import vast_jobs
    client,controller,_,session,_=workflow
    save(client);loop_id=queue(client).json()['id'];operation='1'*32
    native_id='2'*32 if execution=='vast' else '22222222-2222-2222-2222-222222222222'
    native={'loop_id':loop_id,'loop_owner':OWNER,'status':'running'}
    job={'operation':operation,'name':'Actual optimizer run','kind':'optimizer','round':0,'status':'running',
         'started':True,'config':bundle()['config'],'overrides':{},'backend':{'id':native_id,'execution':execution}}
    controller.store.update(OWNER,loop_id,lambda row:row.update(jobs=[job]))
    controller.backend.poll=LoopBackend(tmp_path,controller.store).poll
    if execution=='vast':
        monkeypatch.setattr(vast_jobs,'JobStore',lambda:SimpleNamespace(read=lambda key:native,directory=lambda key:tmp_path/'native-input'))
    else:
        monkeypatch.setattr(optimize_v8,'_queue_file',lambda key:tmp_path/'native-queue.json')
        monkeypatch.setattr(optimize_v8,'_read_json',lambda path:native)
        monkeypatch.setattr(optimize_v8,'_queue_status',lambda value:('running',None))
        monkeypatch.setattr(optimize_v8,'_launch_config_file',lambda key:tmp_path/'missing-input.json')
        monkeypatch.setattr(optimize_v8,'_read_runner_state',lambda key:{})
    endpoint='/loops/'+loop_id+'/log-target?operation='+operation
    assert client.get('/loops/'+loop_id).json()['jobs'][0]['log_available'] is True
    response=client.get(endpoint)
    assert response.status_code==200,response.text
    assert response.json()=={'id':native_id,'cloud':execution=='vast','name':'Actual optimizer run','kind':'optimizer'}
    assert response.headers['cache-control']=='no-store'
    session.user_id=OTHER
    assert client.get(endpoint).status_code==404
    session.user_id=OWNER
    native['loop_owner']=OTHER
    assert client.get(endpoint).status_code==422
    native['loop_owner']=OWNER;native['loop_id']=OTHER
    assert client.get(endpoint).status_code==422
    assert client.get('/loops/'+loop_id+'/log-target?operation='+'9'*32).status_code==404
    assert client.get('/loops/'+loop_id+'/log-target?operation=../escape').status_code==422
    controller.store.update(OWNER,loop_id,lambda row:row['jobs'][0].pop('backend'))
    assert client.get('/loops/'+loop_id).json()['jobs'][0]['log_available'] is False
    assert client.get(endpoint).status_code==404


@pytest.mark.parametrize('module_name',['optimize_v8','backtest_v8'])
def test_normal_queue_projection_excludes_loop_jobs(monkeypatch,module_name):
    """Public native queue views separate jobs while internal controllers retain all."""
    import importlib
    module=importlib.import_module('api.'+module_name)
    rows=[{'filename':'normal','status':'queued'},{'filename':'child','status':'running','loop_id':'a'*32}]
    monkeypatch.setattr(module,'_load_queue',lambda:copy.deepcopy(rows))
    assert module.get_queue(SimpleNamespace())=={'items':[rows[0]]}
    assert module._load_queue()==rows


def test_waiting_vast_run_rechecks_current_rental_limits(workflow,monkeypatch,tmp_path):
    """Rental duration/concurrency changes apply before a waiting loop can start."""
    client,controller,_,_,preflights=workflow
    from api.vast import RentalPreferences
    import vast_queue
    import vast_credentials
    rental=RentalPreferences(hours=14,max_rentals=3).model_dump()
    monkeypatch.setattr(vast_queue,'CloudQueue',lambda:SimpleNamespace(root=tmp_path,read=lambda:{'gpu_preferences':rental}))
    monkeypatch.setattr(vast_credentials,'VastCredentialStore',lambda root:SimpleNamespace(metadata=lambda:{'configured':True}))
    values=settings();values.update(execution='vast',hours=14,parallel=3)
    definition=save(client,values=values).json();queued=queue(client)
    assert queued.status_code==201,queued.text
    row=queued.json();assert row['settings']['hours']==14 and row['settings']['vast']['hours']==14
    rental.update(hours=12,max_rentals=2)
    blocked=client.post('/loops/'+row['id']+'/action',json={'action':'start'})
    assert blocked.status_code==422 and 'Rental & Automation' in blocked.text
    assert controller.store.read(OWNER,row['id'])['status']=='queued' and len(preflights)==1
    values.update(hours=12,parallel=2)
    edited=save(client,values=values,source={'kind':'run','id':row['id']},revision=definition['revision'],queue_id=row['id'])
    assert edited.status_code==200,edited.text
    started=client.post('/loops/'+row['id']+'/action',json={'action':'start'})
    assert started.status_code==200,started.text
    assert started.json()['settings']['vast']['max_rentals']==2
    assert started.json()['deadline']-started.json()['started_at']==pytest.approx(12*3600,abs=.1)


@pytest.mark.parametrize('kind',['queue','result','cloud'])
def test_import_uses_executed_snapshot_and_overrides(workflow,monkeypatch,tmp_path,kind):
    """Create AI Loop resolves the selected job/result, not its mutable source config."""
    client,controller,original,_,_=workflow
    from api import optimize_v8 as opt
    executed=bundle();executed['config']['optimize']['iters']=555
    provenance='executed-source';source_id='job-or-result'
    if kind=='queue':
        path=tmp_path/'queue.json';path.write_text(json.dumps({'name':provenance}))
        monkeypatch.setattr(opt,'_queue_file',lambda name:path)
        monkeypatch.setattr(opt,'get_queue_config',lambda name,session:dict(copy.deepcopy(executed),name=provenance))
    elif kind=='result':
        result=tmp_path/'result';result.mkdir()
        monkeypatch.setattr(opt,'get_result_config',lambda name,session:copy.deepcopy(executed))
        monkeypatch.setattr(opt,'_resolve_result_path',lambda value:result)
        monkeypatch.setattr(opt,'_list_results',lambda:[{'path':source_id,'name':provenance,'result':'result','modified':1}])
        monkeypatch.setattr(controller.backend,'_result_origin',lambda source:(provenance,None),raising=False)
    else:
        import vast_jobs
        import pb8_config
        directory=tmp_path/'cloud';(directory/'input').mkdir(parents=True)
        (directory/'input/optimize.json').write_text('{}')
        monkeypatch.setattr(vast_jobs,'JobStore',lambda:SimpleNamespace(read=lambda value:{'kind':'optimizer','config_name':provenance},directory=lambda value:directory))
        monkeypatch.setattr(pb8_config,'load_pb8_config',lambda path:copy.deepcopy(executed['config']))
        monkeypatch.setattr(opt,'_load_override_payloads',lambda config,directory:copy.deepcopy(executed['override_configs']))
    draft=client.post('/loops/sources',json={'kind':kind,'id':source_id})
    assert draft.status_code==200,draft.text
    assert draft.json()['config_name']==provenance
    assert controller.store.definitions(OWNER)==[] and controller.store.list(OWNER)==[]
    response=save(client,source={'kind':kind,'id':source_id})
    assert response.status_code==200,response.text
    stored=controller.store.definition(OWNER,'Named')
    assert stored['bundle']==executed and stored['source']['kind']==kind
    if kind=='result':
        assert stored['source']['result_path']==source_id
    original['config']['optimize']['iters']=9999
    queued=queue(client)
    assert queued.status_code==201,queued.text
    actual=controller.store.read(OWNER,queued.json()['id'])
    assert actual['initial_config']['optimize']['iters']==555
    assert actual['initial_overrides']==executed['override_configs']


@pytest.mark.parametrize('module_name',['optimize_v8','backtest_v8'])
def test_normal_clear_finished_retains_loop_job_records(tmp_path,monkeypatch,module_name):
    """Cleaning ordinary queues cannot remove hidden evidence needed by a loop."""
    import contextlib
    import importlib
    module=importlib.import_module('api.'+module_name)
    rows=[{'filename':'normal','status':'complete'},{'filename':'child','status':'complete','loop_id':'a'*32}]
    removed=[]
    monkeypatch.setattr(module,'_load_queue',lambda:copy.deepcopy(rows))
    monkeypatch.setattr(module,'_queue_lock',lambda:contextlib.nullcontext())
    if module_name=='optimize_v8':
        monkeypatch.setattr(module,'_remove_queue_item',lambda name,**kwargs:removed.append(name) or True)
    else:
        monkeypatch.setattr(module,'_queue_file',lambda name:tmp_path/name)
        monkeypatch.setattr(module,'_queue_item',lambda path:next(row for row in rows if row['filename']==path.name))
        monkeypatch.setattr(module,'_delete_queue_item_locked',lambda name:removed.append(name))
    assert module.clear_finished(SimpleNamespace())=={'ok':True,'removed':1}
    assert removed==['normal']


def test_source_change_during_edit_is_reported_without_saving(workflow):
    """A draft cannot silently save a different snapshot than the user inspected."""
    client,controller,source,_,_=workflow
    draft=client.post('/loops/sources',json={'kind':'config','id':'original'}).json()
    source['config']['optimize']['iters']=333
    response=save(client,source_digest=draft['bundle_digest'])
    assert response.status_code==409 and 'snapshot changed' in response.text
    assert controller.store.definitions(OWNER)==[]


def test_definition_options_do_not_require_original_config(workflow,monkeypatch,tmp_path):
    """Config previews and GPU eligibility use the saved bundle after source deletion."""
    client,controller,_,_,_=workflow
    definition=save(client).json()
    from api import optimize_v8
    from fastapi import HTTPException
    import vast_queue
    import vast_credentials
    monkeypatch.setattr(optimize_v8,'list_configs',lambda *args,**kwargs:{'configs':[]})
    def unavailable(*args,**kwargs):
        """Model a removed original without any filesystem access."""
        raise HTTPException(404,'Deleted original')
    monkeypatch.setattr(optimize_v8,'get_config',unavailable)
    monkeypatch.setattr(vast_queue,'CloudQueue',lambda:SimpleNamespace(root=tmp_path,read=lambda:{}))
    monkeypatch.setattr(vast_credentials,'VastCredentialStore',lambda root:SimpleNamespace(metadata=lambda:{'configured':False}))
    response=client.get('/loops/options',params={'source_kind':'definition','source_id':'Named','config_name':'original'})
    assert response.status_code==200,response.text
    assert response.json()['configs']==[] and response.json()['config_name']=='original'
    assert response.json()['config_defaults']['iters']==1000
    assert response.json()['bundle_digest']==definition['bundle_digest']
    assert queue(client).status_code==201


def test_validation_errors_do_not_echo_unknown_secret_values(workflow):
    """Invalid settings do not disclose arbitrary credential input in diagnostics."""
    client,_,_,_,_=workflow
    values=settings();values['password']='isolated-secret-sentinel'
    response=save(client,values=values)
    assert response.status_code==422 and 'isolated-secret-sentinel' not in response.text


def test_gpu_snapshot_preflight_uses_captured_overrides_and_cleans_temp(tmp_path,monkeypatch):
    """GPU preflight validates the saved bundle rather than deleted/changed source files."""
    from pb8_loop_backend import LoopBackend
    from api import optimize_v8
    import pb8_config
    import scenario_windows
    from pathlib import Path
    snapshot=bundle();recorded=[];values=dict(settings(),execution='gpu')
    monkeypatch.setattr('pb8_loop_backend.migrate_loop_bundle',lambda value:copy.deepcopy(value))
    backend=LoopBackend(tmp_path,LoopStore(tmp_path/'loops'))
    monkeypatch.setattr(optimize_v8,'_config_file',lambda name:tmp_path/'deleted-source.json')
    monkeypatch.setattr(optimize_v8,'pb8_runtime_status',lambda:{'ready':True})
    monkeypatch.setattr(backend,'fingerprint',lambda value:{'pb8':'isolated'})
    monkeypatch.setattr(scenario_windows,'build_validation_plan',lambda value:None)
    def write(config,path):
        """Write only isolated temporary data instead of invoking PB8."""
        path.write_text(json.dumps(config))
    def validate(config,*,base_config_path):
        """Inspect the exact supplied override while the temporary bundle exists."""
        path=Path(base_config_path);recorded.append(path)
        assert path.is_file()
        assert json.loads((path.parent/'BTC.json').read_text())==snapshot['override_configs']['BTC.json']
        assert config['optimize']['backend']=='gpu'
        return {'valid':True}
    monkeypatch.setattr(pb8_config,'save_prepared_pb8_config',write)
    monkeypatch.setattr(optimize_v8,'_validate_optimize_backend',validate)
    config,overrides,_,_,_=backend.initial('original',values,snapshot)
    assert config['optimize']['backend']=='gpu' and overrides==snapshot['override_configs']
    assert recorded and not recorded[0].exists()
    assert snapshot==bundle()


def test_queue_double_click_and_late_response_preserve_new_navigation():
    """A duplicate click sends one POST and a late response cannot redirect a newer view."""
    import subprocess
    from pathlib import Path
    script=r"""
const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const source=fs.readFileSync('frontend/js/pb8_loop_optimizer.js','utf8');
const body=source.match(/  async function queueDefinitions\([\s\S]*?\n  \}/)[0];
let releaseSelection,releaseQueue,requests=0,polls=0;
const ai={ensureSelection(){return new Promise(resolve=>releaseSelection=resolve);}};
const context=vm.createContext({queueing:false,selected:'previous',state:{navigationSeq:1},
 window:{PBGuiAI:ai},PBGuiAI:ai,syncContext(){},
 request(path,payload){requests++;assert.equal(path,'/configs/Named/queue');assert.equal(payload.authorization,true);return new Promise(resolve=>releaseQueue=resolve);},
 persistWorkflow(){throw Error('stale response persisted selection');},
 selectPanel(){throw Error('stale response redirected navigation');},
 poll(){polls++;return Promise.resolve();}});
vm.runInContext(body,context);
(async()=>{
 const first=context.queueDefinitions(['Named']),duplicate=context.queueDefinitions(['Named']);
 assert.equal(context.queueing,true);releaseSelection({provider:'chatgpt',model:'shared'});
 await new Promise(resolve=>setImmediate(resolve));assert.equal(requests,1);
 context.state.navigationSeq=2;context.selected='new-selection';releaseQueue({id:'a'.repeat(32)});
 await Promise.all([first,duplicate]);assert.equal(requests,1);assert.equal(context.queueing,false);
 assert.equal(context.selected,'new-selection');assert.equal(polls,1);
})().catch(error=>{console.error(error);process.exitCode=1;});
"""
    result=subprocess.run(['node','-e',script],cwd=Path(__file__).resolve().parents[1],capture_output=True,text=True,timeout=10)
    assert result.returncode==0,result.stdout+result.stderr


@pytest.mark.parametrize('selected_round,extra,evaluated', [(None, 10, True), (0, 5, True), (1, 20, False)])
def test_continue_from_best_or_exact_previous_round_keeps_provenance(workflow, monkeypatch, selected_round, extra, evaluated):
    """Continue starts a separate bounded run with winner parameters and optimizer settings."""
    from pb8_loop_store import evaluate
    client, controller, source, session, _ = workflow
    save(client); original = queue(client).json(); loop_id = original['id']
    rubric = [{'goal': 'gain', 'metrics': ['gain'], 'target': 10, 'direction': 'max', 'weight': 1, 'scale': 1}]
    checkpoints = []
    def completed(row):
        row.update(status='unconfirmed', rubric=rubric, jobs=[], history=[])
        for number in (0, 1):
            config = copy.deepcopy(row['initial_config']); config['optimize']['iters'] = 1000 + number
            optimizer = controller.job(dict(row, round=number), 'optimizer', config, row['initial_overrides'], 'Change')
            optimizer.update(status='completed', started=True)
            candidate_config = copy.deepcopy(config); candidate_config['bot']['long']['risk']['total_wallet_exposure_limit'] = .1 + number
            candidate = {'id': 'candidate_' + str(number), 'optimizer_job': optimizer['operation'], 'config': candidate_config, 'overrides': row['initial_overrides'], 'metrics': {'gain': 1 + number}}
            job = controller.job(dict(row, round=number), 'validation', config, row['initial_overrides'], 'Comparison', candidate=candidate)
            job.update(status='completed', started=True)
            assessment = {**evaluate({'gain': 1 + number}, rubric), 'simulation_complete': True}
            item = {'operation': job['operation'], 'candidate': candidate, 'metrics': {'gain': 1 + number}, 'assessment': assessment, 'exact': True}
            checkpoints.append(item); row['jobs'] += [optimizer, job]
            row['history'].append({'round': number, 'assessment': assessment, 'observations': [{key: value for key, value in item.items() if key != 'candidate'}]})
        row['best'] = checkpoints[-1]
        if not evaluated:
            row.update(status='failed', phase='evaluate', round=1, history=row['history'][:1],
                       observations=[copy.deepcopy(checkpoints[-1])], pending_ai={'stage':'evaluate'})
    parent = controller.store.update(OWNER, loop_id, completed)
    monkeypatch.setattr(controller.backend, 'observations', lambda row, job: next(item for item in checkpoints if item['operation'] == job['operation']), raising=False)
    response = client.post('/loops/' + loop_id + '/continue', json={'round': selected_round, 'rounds': extra, 'selection': {'provider': 'chatgpt', 'model': 'shared-current'}, 'authorization': True})
    assert response.status_code == 201, response.text
    result = response.json(); assert result['id'] != loop_id and result['status'] == 'running'
    child = controller.store.read(OWNER, result['id']); index = 1 if selected_round is None else selected_round
    assert child['settings']['max_runs'] == extra and child['settings']['max_validations'] >= extra
    assert child['required_rounds'] == extra
    assert child['initial_config']['bot']['long']['risk']['total_wallet_exposure_limit'] == .1 + index
    assert child['initial_config']['optimize']['iters'] == 1000 + index
    assert child['initial_overrides'] == parent['initial_overrides']
    assert child['continuation']['parent'] == loop_id and child['continuation']['round'] == selected_round
    assert child['phase'] == 'bootstrap' and child['rubric'] == rubric and child['baseline_required']
    assert controller.store.read(OWNER, loop_id) == parent
    session.user_id = OTHER
    assert client.post('/loops/' + loop_id + '/continue', json={'authorization': True}).status_code == 404


def test_continue_rejects_incomplete_and_missing_checkpoints(workflow):
    """Continuation cannot manufacture a candidate from a failed or incomplete round."""
    client, controller, source, session, _ = workflow
    save(client); row = queue(client).json()
    endpoint = '/loops/' + row['id'] + '/continue'
    assert client.post(endpoint, json={'authorization': False}).status_code == 422
    assert client.post(endpoint, json={'round': 0, 'authorization': True}).status_code == 422
    assert len(controller.store.list(OWNER)) == 1


def test_retry_holdout_is_same_candidate_once_and_within_existing_allowance(workflow):
    """The technical retry never extends time/budget or creates an adaptive optimizer job."""
    import time
    client, controller, source, session, _ = workflow
    save(client); original = queue(client).json(); loop_id = original['id']
    def failed(row):
        candidate = {'id': 'exact_candidate', 'config': copy.deepcopy(row['initial_config']), 'overrides': row['initial_overrides']}
        row.update(status='unconfirmed', holdout_retryable=True, best={'candidate': candidate, 'assessment': {'score': 1}, 'metrics': {}},
                   deadline=time.time()+300, holdouts=[{'start_date':'2020-03-01','end_date':'2020-04-01'}])
    row = controller.store.update(OWNER, loop_id, failed)
    controller.backend.validation_config = lambda row, candidate, holdout: copy.deepcopy(candidate['config'])
    response = client.post('/loops/' + loop_id + '/retry-holdout')
    assert response.status_code == 200, response.text
    current = controller.store.read(OWNER, loop_id)
    assert current['status'] == 'finishing' and current['deadline'] == row['deadline']
    assert current['jobs'][-1]['kind'] == 'holdout' and current['jobs'][-1]['candidate'] == row['best']['candidate']
    assert current['validation_count'] == row['validation_count']+1 and current['holdout_retry_count'] == 1
    assert client.post('/loops/' + loop_id + '/retry-holdout').status_code == 422
    assert len(controller.store.read(OWNER, loop_id)['jobs']) == 1


@pytest.mark.parametrize('exhausted', ['time', 'backtests', 'no_proof'])
def test_retry_holdout_rejects_missing_proof_or_exhausted_allowance(workflow, exhausted):
    """No retry may bypass proof of pre-data failure or its original authorization."""
    import time
    client, controller, source, session, _ = workflow
    save(client); original=queue(client).json(); loop_id=original['id']
    def failed(row):
        row.update(status='unconfirmed', holdout_retryable=exhausted!='no_proof', deadline=time.time()+(0 if exhausted=='time' else 300))
        if exhausted=='backtests':
            row['validation_count']=row['settings']['max_validations']
    controller.store.update(OWNER,loop_id,failed)
    assert client.post('/loops/'+loop_id+'/retry-holdout').status_code==422
    assert not controller.store.read(OWNER,loop_id)['jobs']


@pytest.mark.parametrize('kind', ['observer_holdout', 'observer_full_range'])
def test_user_evaluation_log_target_is_native_and_owner_scoped(workflow,monkeypatch,tmp_path,kind):
    """Observer logs resolve their native Backtest file without joining decision jobs."""
    from api import backtest_v8
    from pb8_loop_backend import LoopBackend
    client,controller,_,session,_=workflow
    save(client);loop_id=queue(client).json()['id'];operation='8'*32
    native_id='88888888-8888-8888-8888-888888888888'
    native={'loop_id':loop_id,'loop_owner':OWNER,'status':'complete'}
    job={'operation':operation,'name':'User evaluation','kind':kind,'round':0,'status':'complete',
         'observer_only':True,'report_done':True,'backend':{'id':native_id,'execution':'validation'}}
    controller.store.update(OWNER,loop_id,lambda row:row.update(observer_jobs=[job]))
    controller.backend.poll=LoopBackend(tmp_path,controller.store).poll
    monkeypatch.setattr(backtest_v8,'_queue_file',lambda key:tmp_path/'native.json')
    monkeypatch.setattr(backtest_v8,'_read_json',lambda path:native)
    monkeypatch.setattr(backtest_v8,'_queue_status',lambda value:('complete',None))
    monkeypatch.setattr(backtest_v8,'_snapshot_file',lambda key:tmp_path/'missing.json')
    monkeypatch.setattr(backtest_v8,'_read_runner_state',lambda key:{})
    endpoint='/loops/'+loop_id+'/log-target?operation='+operation
    assert client.get('/loops/'+loop_id).json()['jobs']==[]
    response=client.get(endpoint)
    assert response.status_code==200,response.text
    assert response.json()=={'id':native_id,'cloud':False,'name':'User evaluation','kind':kind}
    native['loop_owner']=OTHER
    assert client.get(endpoint).status_code==422
    native['loop_owner']=OWNER;session.user_id=OTHER
    assert client.get(endpoint).status_code==404
