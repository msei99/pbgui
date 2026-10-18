"""Offline reviewed-window contracts and mandatory isolated background reports."""
import asyncio
import copy
import json
from pathlib import Path
import subprocess
import time
from types import SimpleNamespace

import pytest

from ai_loop_request import current_request, bind_request, validate_request
from ai_loop_windows import MARKER, validate_plan, verify_config, bound_plan, stored_plan
from scenario_windows import preview_windows
from pb8_loop_store import LoopStore, validate_change
from pb8_loop_controller import LoopController
from pb8_loop_observer import ObserverJobs
from tests.test_pb8_loop_optimizer import OWNER, config, settings
from tests.test_ai_loop_tools import environment, approve, CHAT

ROOT = Path(__file__).resolve().parents[1]


def plan(enabled=True):
    """Use small deterministic training/holdout fixtures, with no runtime data."""
    rows = [('training', '2024-01-01', '2024-01-02'), ('training', '2024-01-03', '2024-01-05'),
            ('training', '2024-01-06', '2024-01-07'), ('holdout', '2024-01-08', '2024-01-10')]
    if not enabled:
        rows = [('training', '2024-01-01', '2024-01-03'), ('training', '2024-01-04', '2024-01-06'),
                ('training', '2024-01-07', '2024-01-10')]
    return validate_plan(dict(version=1, start_date='2024-01-01', end_date='2024-01-10',
        training_windows=3, use_holdout=enabled, windows=[dict(id=f'w{i}', label=f'w{i}', role=role,
        start_date=start, end_date=end) for i, (role, start, end) in enumerate(rows)]))


def bundle(value):
    """Apply the existing explicit Scenario contract to a minimal optimizer config."""
    preview = preview_windows(value)
    result = config()
    result['backtest'].update(start_date=value['start_date'], end_date=value['end_date'], suite_enabled=True,
                             scenarios=preview['training_scenarios'], reducer=preview['reducer'])
    result['pbgui']['scenario_template'] = preview['provenance']
    result['optimize']['write_all_results'] = True
    return result


def request(value):
    """Read the same visible authoritative block that the browser submits."""
    text = 'Optimize using reviewed windows.\n' + MARKER + json.dumps(value, separators=(',', ':'))
    return current_request('b' * 32, {'messages': [{'role': 'user', 'content': text}]})


def run_record(tmp_path, enabled=True):
    """Create an isolated durable run with an original-request binding."""
    store = LoopStore(tmp_path / 'loops')
    value = plan(enabled)
    req = request(value)
    opts = settings(max_validations=8)
    opts['goals'] = {**opts['goals'], 'text': req['text'], 'presets': [], 'targets': {}}
    opts['source'] = {'loop_request': bind_request(req, store.instruction_version(OWNER, 'default'))}
    candidate = bundle(value)
    holdouts = preview_windows(value)['holdout_scenarios']
    row = store.create(OWNER, opts, candidate, {}, {'pb8': 'fixture', 'data': 'fixture'}, holdouts, candidate['backtest'])
    row = store.update(OWNER, row['id'], lambda r: r.update(best={'candidate': {'id': 'winner', 'config': candidate, 'overrides': {}}}))
    return store, row


@pytest.mark.parametrize('enabled', [True, False])
def test_reviewed_roundtrip_and_store_policy(tmp_path, enabled):
    """All runs enforce observers and retain the exact original window plan."""
    value = plan(enabled)
    verify_config(value, bundle(value))
    store, row = run_record(tmp_path, enabled)
    assert row['observer_enabled']
    assert ObserverJobs.sources(row) == []
    assert ObserverJobs.targets(row) == []
    assert row['final_holdout_policy'] == 'observer_only'
    assert row['settings']['source']['loop_request']['window_plan'] == value
    legacy = store.create(OWNER, settings(), config(), {}, {}, [], config()['backtest'])
    assert legacy['observer_enabled'] and 'final_holdout_policy' not in legacy


@pytest.mark.parametrize('problem', ['overlap', 'holdout_first', 'count', 'too_many', 'short', 'invalid_date', 'disabled'])
def test_reject_bad_reviewed_windows(problem):
    """Reject invalid reviewed dates and roles without relaxing general Scenario rules."""
    value = plan()
    if problem == 'overlap':
        value['windows'][1]['start_date'] = '2024-01-02'
    elif problem == 'holdout_first':
        value['windows'][0]['role'], value['windows'][-1]['role'] = 'holdout', 'training'
    elif problem == 'count':
        value['training_windows'] = 2
    elif problem == 'too_many':
        value['training_windows'] = 49
    elif problem == 'short':
        value['windows'][0]['end_date'] = '2024-01-01'
    elif problem == 'invalid_date':
        value['windows'][0]['start_date'] = '2024-02-30'
    else:
        value['use_holdout'] = False
    with pytest.raises(ValueError):
        validate_plan(value)


@pytest.mark.parametrize('change', ['training', 'holdout', 'reducer', 'base', 'suite', 'choice', 'binding'])
def test_bound_windows_reject_drift_at_creation_and_start(tmp_path, change):
    """Original text and complete config must agree before saving/starting a run."""
    store, row = run_record(tmp_path)
    candidate = copy.deepcopy(row['initial_config'])
    opts = copy.deepcopy(row['settings'])
    if change == 'training':
        candidate['backtest']['scenarios'][0]['end_date'] = '2024-01-03'
    elif change == 'holdout':
        candidate['pbgui']['scenario_template']['parameters']['windows'][-1]['start_date'] = '2024-01-09'
    elif change == 'reducer':
        candidate['backtest']['reducer'] = {'default': 'max'}
    elif change == 'base':
        candidate['backtest']['end_date'] = '2024-01-11'
    elif change == 'suite':
        candidate['backtest']['suite_enabled'] = False
    elif change == 'choice':
        opts['source']['loop_request']['window_plan']['use_holdout'] = False
    else:
        opts['source']['loop_request'].pop('window_plan')
    with pytest.raises(ValueError):
        validate_request(opts, row['settings']['source']['loop_request'], config=candidate)
    with pytest.raises(ValueError):
        store.create(OWNER, opts, candidate, {}, {}, row['holdouts'], candidate['backtest'])
    if change not in {'choice', 'binding'}:
        with pytest.raises(ValueError):
            validate_change(row, candidate, {})


def test_js_inclusive_split_and_calendar_boundaries():
    """Run production JS arithmetic directly, including leap days and minimum lengths."""
    code = """
require('./frontend/js/ai_loop_windows.js');
const assert=require('node:assert/strict'), w=globalThis.PBGuiAILoopWindows;
assert.deepEqual(w.generate('2024-01-01','2024-01-10',3,true).windows.map(r=>[r.start_date,r.end_date]),
 [['2024-01-01','2024-01-02'],['2024-01-03','2024-01-05'],['2024-01-06','2024-01-07'],['2024-01-08','2024-01-10']]);
assert.equal(w.generate('2024-01-01','2024-01-10',3,false).windows.length,3);
assert.equal(w.generate('2024-02-27','2024-03-01',1,true).windows[1].start_date,'2024-02-29');
assert.throws(()=>w.generate('2024-01-01','2024-01-07',3,true));
assert.throws(()=>w.generate('2024-02-30','2024-03-10',1,true));
assert.throws(()=>w.generate('2024-01-01','2024-12-31',49,false));
assert.equal(w.generate('2024-01-01','2024-12-31',48,true).windows.length,49);
"""
    subprocess.run(['node', '-e', code], cwd=ROOT, check=True, capture_output=True, text=True)


@pytest.mark.parametrize('problem', ['quota', 'time', 'claimed', 'none'])
def test_training_finishes_without_final_holdout_gating(tmp_path, monkeypatch, problem):
    """Time, training quota and another Loop never suppress independent reports."""
    store, row = run_record(tmp_path)
    controller = LoopController(tmp_path, store=store, backend=SimpleNamespace(), ai=SimpleNamespace())
    if problem == 'quota':
        row['validation_count'] = row['settings']['max_validations']
    elif problem == 'time':
        row['deadline'] = time.time() - 1
    def forbidden(*_args):
        """Report scheduling must not consult the historical reservation mechanism."""
        raise AssertionError('Holdout reservations cannot gate user reports')
    monkeypatch.setattr(store, 'claim_holdouts', forbidden)
    asyncio.run(controller.finish(row, 'Training complete'))
    result = store.read(OWNER, row['id'])
    assert result['best'] == row['best']
    assert result['status'] == 'completed' and result['jobs'] == []
    assert result['final_validation_status'] == 'observer_only'


def test_without_holdout_still_schedules_full_range(tmp_path):
    """An explicitly omitted Holdout does not suppress Full Time Range."""
    store, row = run_record(tmp_path, False)
    row['baseline'] = {'assessment': {'simulation_complete': True}}
    targets = ObserverJobs.targets(row)
    assert len(targets) == 1 and targets[0][2] == 'observer_full_range'
    asyncio.run(LoopController(tmp_path, store=store, backend=SimpleNamespace(), ai=SimpleNamespace()).finish(row, 'Training complete'))
    assert store.read(OWNER, row['id'])['status'] == 'completed'


@pytest.mark.parametrize('enabled', [True, False])
def test_old_template_flags_cannot_disable_reports(tmp_path, enabled):
    """Restart derives mandatory reporting even from the former final-only policy."""
    store, row = run_record(tmp_path, enabled)
    row.update(observer_enabled=False, final_holdout_policy='final_only' if enabled else 'disabled',
               baseline={'assessment': {'simulation_complete': True}})
    store.write(row)
    restored = store.read(OWNER, row['id'])
    assert restored['observer_enabled'] and restored['final_holdout_policy'] == 'observer_only'
    assert {kind for _, _, kind in ObserverJobs.targets(restored)} == (
        {'observer_holdout', 'observer_full_range'} if enabled else {'observer_full_range'})
    # Even an in-memory false flag cannot turn scheduling off.
    restored['observer_enabled'] = False
    assert ObserverJobs.targets(restored)


def test_save_proposal_uses_owned_persisted_request_and_explicit_contract(tmp_path, monkeypatch):
    """Approval payloads preserve the dates and reject substituted generation or stale chats."""
    import ai_capabilities
    from ai_capabilities import AICapabilityService, AICapabilityError
    value = plan()
    req = request(value)
    directory = tmp_path/'conversations'/OWNER
    directory.mkdir(parents=True)
    path = directory/('b'*32+'.json')
    def persist(text):
        """Write only this test's synthetic conversation snapshot."""
        path.write_text(json.dumps({'owner':OWNER,'conversation_id':'b'*32,'messages':[{'role':'user','content':text}]}))
    persist(req['text'])
    monkeypatch.setattr(ai_capabilities, 'load_ini_section', lambda section: {})
    async def scenario():
        """Prepare approvals only; all config/runtime hooks are temporary fixtures."""
        service = AICapabilityService(tmp_path/'capabilities')
        monkeypatch.setattr(service,'_current_pb8_bundle',lambda name:(None,{},None))
        monkeypatch.setattr(service,'_validate_pb8_config',lambda name,c:copy.deepcopy(c))
        monkeypatch.setattr(service,'_queue_preview',lambda c,o:{})
        raw = config()
        raw.pop('pbgui')
        args = {'name':'reviewed','config':raw,'scenario_template':dict(value,template='ai_loop_windows')}
        result = await service._propose_pb8_optimizer_config(OWNER,'b'*32,args)
        saved = service.proposals[result['proposal_id']]
        verify_config(value,saved.config)
        assert saved.preview['reviewed_window_plan']==value
        assert saved.preview['generated_scenario_template']['contract_version']==2
        path.write_text(json.dumps({'owner':OWNER,'conversation_id':'b'*32,'messages':[
            {'role':'user','content':req['text']}, {'role':'user','content':'Please use a new configuration name.'}]}))
        broken = copy.deepcopy(args)
        broken['scenario_template']['windows'][0]['end_date']='2024-01-01'
        with pytest.raises(AICapabilityError):
            await service._propose_pb8_optimizer_config(OWNER,'b'*32,broken)
        changed_choice = copy.deepcopy(args)
        changed_choice['scenario_template'] = dict(plan(False), template='ai_loop_windows')
        with pytest.raises(AICapabilityError,match='reviewed Loop windows'):
            await service._propose_pb8_optimizer_config(OWNER,'b'*32,changed_choice)
        with pytest.raises(AICapabilityError,match='reviewed Loop windows'):
            await service._propose_pb8_optimizer_config(OWNER,'b'*32,{'name':'omitted','config':raw})
        persist('A different request')
        from api import optimize_v8
        from contextlib import nullcontext
        monkeypatch.setattr(optimize_v8,'_config_lock',nullcontext)
        with pytest.raises(AICapabilityError,match='changed since'):
            service._execute_proposal(saved)
        await service.shutdown()
    asyncio.run(scenario())


@pytest.mark.parametrize('followup', [False, True])
def test_saved_loop_definition_preserves_plan_and_rejects_changed_source(environment, monkeypatch, followup):
    """Exercise the native reviewed definition save, without queueing or starting jobs."""
    from api import loop_optimizer_v8 as runtime, optimize_v8
    from ai_capabilities import AICapabilityError
    service, store, _, _ = environment
    value = plan()
    messages = [{'role':'user','content':request(value)['text']}]
    if followup:
        messages.append({'role':'user','content':'Use a new definition name and keep the reviewed dates.'})
    runtime.get_ai_chat_service().get_conversation.return_value['messages'] = messages
    source = bundle(value)
    monkeypatch.setattr(optimize_v8,'get_config',lambda *a,**k:{'config':copy.deepcopy(source),'override_configs':{}})
    async def scenario():
        """Save an exact definition, then reject even a one-day source change."""
        result = await service.dispatch(OWNER,CHAT,'propose_ai_loop_config',{
            'name':'reviewed_loop','config_name':'source','settings':{'max_rounds':3}})
        await approve(service,result)
        saved = store.definition(OWNER,'reviewed_loop')
        assert saved['source']['loop_request']['window_plan']==value
        verify_config(value,saved['bundle']['config'])
        assert store.list(OWNER)==[]
        source['backtest']['scenarios'][0]['end_date']='2024-01-03'
        with pytest.raises(AICapabilityError):
            await service.dispatch(OWNER,CHAT,'propose_ai_loop_config',{
                'name':'changed_loop','config_name':'source','settings':{'max_rounds':3}})
    asyncio.run(scenario())


@pytest.mark.parametrize('enabled', [True, False])
def test_marker_free_followup_preserves_original_windows_and_policy(tmp_path, enabled):
    """Follow-ups cannot turn off the save/start checks or silently enable observers."""
    value = plan(enabled)
    original = request(value)['text']
    messages = [{'role':'user','content':original},
                {'role':'user','content':'Please use a new configuration name.'},
                {'role':'assistant','content':MARKER+'invalid assistant data'},
                {'role':'user','content':MARKER+'invalid failed data','failed':True},
                {'role':'user','content':MARKER+'invalid hidden data','hidden':True}]
    req = current_request(CHAT, {'messages':messages})
    assert req['text']=='Please use a new configuration name.'
    assert req['window_request']==original and req['window_plan']==value
    directory = tmp_path/'conversations'/OWNER
    directory.mkdir(parents=True)
    (directory/(CHAT+'.json')).write_text(json.dumps({'owner':OWNER,'conversation_id':CHAT,'messages':messages}))
    assert stored_plan(tmp_path, OWNER, CHAT)==value
    store = LoopStore(tmp_path/'loops')
    opts = settings()
    opts['goals'] = {**opts['goals'], 'text':req['text'], 'presets':[], 'targets':{}}
    opts['source'] = {'loop_request':bind_request(req, store.instruction_version(OWNER,'default'))}
    candidate = bundle(value)
    validate_request(opts, req, config=candidate)
    assert bound_plan(opts)==value
    row = store.create(OWNER,opts,candidate,{}, {}, preview_windows(value)['holdout_scenarios'],candidate['backtest'])
    assert row['observer_enabled']
    assert row['final_holdout_policy']=='observer_only'
    candidate['backtest']['scenarios'][0]['end_date']='2024-01-01'
    with pytest.raises(ValueError):
        validate_request(opts, req, config=candidate)
    with pytest.raises(ValueError):
        validate_change(row, candidate, {})
    old_binding = bind_request(request(value), store.instruction_version(OWNER,'default'))
    with pytest.raises(ValueError,match='current chat request'):
        validate_request({**opts,'source':{'loop_request':old_binding}}, req, config=bundle(value))


def test_new_explicit_plan_supersedes_history_and_malformed_plan_fails():
    """Only an actual visible user plan replaces the protected window contract."""
    messages = [{'role':'user','content':request(plan())['text']},
                {'role':'user','content':request(plan(False))['text']},
                {'role':'user','content':'Use this configuration name.'}]
    req = current_request(CHAT, {'messages':messages})
    assert req['window_plan']==plan(False)
    assert req['window_request']==messages[1]['content']
    messages.append({'role':'user','content':MARKER+'invalid'})
    with pytest.raises(ValueError,match='Invalid reviewed Loop window plan'):
        current_request(CHAT, {'messages':messages})
