"""Offline selected-config deep-search handoff and ownership regressions."""
import asyncio
import copy
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from api import loop_optimizer_v8 as api


def _row():
    """Build a training optimizer and a deliberately different user-only report window."""
    initial = {'bot': {'long': {'value': 1}}, 'live': {'strategy_kind': 'ema_anchor'},
               'optimize': {'bounds': {'x': [0, 2]}, 'limits': [{'metric':'drawdown','value':.5}],
                            'scoring': [{'metric':'adg'}], 'gpu': {'population_size': 2048}, 'seed': 999}}
    search = copy.deepcopy(initial)
    search['optimize']['bounds'] = {'x': [1, 5]}
    report = copy.deepcopy(initial)
    report['bot']['long']['value'] = 4
    report['backtest'] = {'start_date':'holdout-date'}
    return {'initial_config': initial, 'settings': {'config_name':'fixture','execution':'vast'},
            'comparison': {'start_date':'training-date','scenarios':[{'label':'train_01'}]},
            'jobs':[{'operation':'o','kind':'optimizer','config':search},
                    {'kind':'validation','round':12,'candidate':{'id':'selected','optimizer_job':'o'}}],
            'observer_jobs':[{'operation':'a'*32,'kind':'observer_full_range','round':12,
                              'candidate_id':'selected','config':report,'overrides':{'ADA':{'x':1}},
                              'status':'complete','simulation_complete':True,'report_done':True}]}


def test_selected_bot_uses_its_search_domain_and_training_windows():
    """Human selection preserves the bot, bounds, safety, workers and training provenance."""
    row = _row();before = copy.deepcopy(row)
    bundle = api._deep_search_bundle(row,'a'*32)
    config = bundle['config']
    assert config['bot']['long']['value'] == 4
    assert config['optimize']['bounds'] == {'x':[1,5]}
    assert config['optimize']['limits'] == row['initial_config']['optimize']['limits']
    assert config['optimize']['gpu'] == {'population_size':2048}
    assert config['backtest'] == row['comparison']
    assert config['optimize']['seed'] is None
    assert config['pbgui']['optimize_runtime']['convergence_config']['convergence_enabled']
    assert config['pbgui']['optimize_seed_mode'] == 'self'
    assert bundle['override_configs'] == {'ADA':{'x':1}}
    assert row == before


@pytest.mark.parametrize('change',[{'status':'running'},{'simulation_complete':False},{'report_done':False},
                                  {'candidate_id':'missing'},{'kind':'validation'}])
def test_incomplete_or_unresolvable_selection_is_rejected(change):
    """Missing history never silently substitutes another search configuration."""
    row = _row();row['observer_jobs'][0].update(change)
    with pytest.raises(HTTPException) as error:
        api._deep_search_bundle(row,'a'*32)
    assert error.value.status_code == 409


def test_baseline_selection_uses_initial_search_settings():
    """The unchanged start also supports an explicit user-selected deep search."""
    row = _row();row['observer_jobs'][0]['candidate_id'] = 'starting_config'
    assert api._deep_search_bundle(row,'a'*32)['config']['optimize']['bounds'] == {'x':[0,2]}


def test_handoff_reads_only_authenticated_owner(monkeypatch):
    """The route cannot read another owner or create queue/rental state."""
    calls = []
    class Store:
        """Expose only the owner-scoped read needed by this read-only handoff."""
        def read_runtime(self, owner, identifier):
            """Return an isolated synthetic loop."""
            calls.append((owner,identifier));return _row()
    monkeypatch.setattr(api,'_controller',SimpleNamespace(store=Store()))
    monkeypatch.setattr(api,'_owner',lambda session:'owner')
    result = asyncio.run(api.deep_search_config('b'*32,api.LoopDeepSearch(operation='a'*32),object()))
    assert result.status_code == 200
    assert calls == [('owner','b'*32)]


def test_deep_search_toolbar_single_selection_and_native_editor_handoff():
    """Real browser DOM controls retain selection and forward the exact bundle with overrides."""
    from pathlib import Path
    from javascript_checks import run_javascript
    root = Path(__file__).resolve().parents[1]
    source = (root/'frontend/js/pb8_loop_optimizer.js').read_text()
    start = source.index('    var viewKey=run.id+')
    toolbar = source[start:source.index('    if(saved.hide_duplicates)',start)]
    script = r'''
const assert=browserAssert;
function harness(selection,fail){
 const parent=document.createElement('div'),run={id:'loop'},round=null;
 const jobs=[{operation:'a'},{operation:'b'}];
 const workflow={evaluation_views:{'loop:all':{selected:selection}}};
 const calls=[],errors=[];let location={href:'http://pbgui.test/nested/api/optimize-v8/main_page?loop_id=loop'};
 const API_BASE='/nested/api/optimize-v8';
 function persistWorkflow(){}
 function evaluationComparable(){return true;}
 function element(tag,text,cls){const el=document.createElement(tag);if(text)el.textContent=text;if(cls)el.className=cls;return el;}
 function button(parent,text,fn){const b=element('button',text);b.addEventListener('click',fn);parent.appendChild(b);return b;}
 function openEvaluationComparison(){}
 function updateReport(){}
 function toast(message){errors.push(message);}
 async function request(path,body){calls.push({path,body});if(fail)throw new Error('unavailable');return {config:{optimize:{bounds:{x:[1,5]}}},override_configs:{ADA:{x:1}},name:'selected_deep_search'};}
 async function fetch(path,options){calls.push({path,body:JSON.parse(options.body)});return {ok:true,json:async()=>({draft_id:'draft'})};}
''' + toolbar + r'''
 return {parent,calls,errors,location};
}
const empty=harness([]);assert.ok(empty.parent.querySelector('#loop-evaluation-deep-search').disabled);
const multiple=harness(['a','b']);assert.ok(multiple.parent.querySelector('#loop-evaluation-deep-search').disabled);
const single=harness(['a']);assert.ok(!single.parent.querySelector('#loop-evaluation-deep-search').disabled);
single.parent.querySelector('#loop-evaluation-deep-search').click();
await new Promise(resolve=>setTimeout(resolve,0));
assert.equal(single.calls[0].path,'/loop/deep-search-config');assert.deepEqual(single.calls[0].body,{operation:'a'});
assert.equal(single.calls[1].path,'/nested/api/backtest-v8/optimize-draft');
assert.deepEqual(single.calls[1].body.override_configs,{ADA:{x:1}});
const target=new URL(single.location.href);assert.equal(target.searchParams.get('opt_draft_id'),'draft');assert.equal(target.searchParams.get('draft_name'),'selected_deep_search');
const failed=harness(['a'],true);failed.parent.querySelector('#loop-evaluation-deep-search').click();await new Promise(resolve=>setTimeout(resolve,0));
assert.ok(!failed.parent.querySelector('#loop-evaluation-deep-search').disabled);assert.deepEqual(failed.errors,['unavailable']);
'''
    result=run_javascript(['-e','(async function(){'+script+'})()'],cwd=root,capture_output=True,text=True,timeout=15)
    assert result.returncode==0,result.stdout+result.stderr
