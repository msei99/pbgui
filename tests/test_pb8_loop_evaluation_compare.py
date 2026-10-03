"""Offline request ownership and stale navigation checks for Loop comparisons."""
from pathlib import Path
import subprocess


def test_loop_comparison_ignores_stale_owner_and_result_responses():
    """Late replies cannot overwrite a newer panel, run or comparison request."""
    root=Path(__file__).resolve().parents[1]
    source=(root/'frontend/v7_backtest.html').read_text()
    body=source[source.index('var _loopEvaluationComparison ='):source.index('async function openLoopResultComparison(')]
    script=r"""
const assert=require('node:assert/strict');
let location={href:'http://pbgui.test/nested/api/backtest-v8/main_page?panel=results&result_loop='+'a'.repeat(32)+'&result_evaluation=observer_holdout&result_compare=1'};
let window={location,addEventListener(){}},backtestEditorAdapter={isV8:true},API_BASE='/nested/api/backtest-v8';
let _resultsLoadGeneration=0,_resultsEmptyRetryTimer=null,currentPanel='results',results=['old'];
let nodes={'results-list':{textContent:''}},document={getElementById:id=>nodes[id]};
let resolve,requests=0;
function apiFetchFrom(base,path){requests++;assert.equal(base,'/nested/api/optimize-v8/loops');return new Promise(done=>resolve=done);}
function _applyResultsData(){throw Error('stale data applied');}
function toast(){throw Error('stale error shown');}
"""+body+r"""
(async()=>{
 const selection=loopEvaluationSelection();
 assert.equal(selection.run,'a'.repeat(32));
 let first=loadLoopEvaluationResults(selection,'',{});currentPanel='queue';resolve({observer_jobs:[]});
 assert.deepEqual(await first,['old']);assert.equal(requests,1);
 currentPanel='results';first=loadLoopEvaluationResults(selection,'',{});location.href=location.href.replace('a'.repeat(32),'b'.repeat(32));resolve({observer_jobs:[]});
 await first;assert.equal(requests,2);
 location.href=location.href.replace('b'.repeat(32),'a'.repeat(32));first=loadLoopEvaluationResults(selection,'',{});_resultsLoadGeneration++;resolve({observer_jobs:[]});await first;
 for(const value of ['../secrets','a'.repeat(31)]){let url=new URL(location.href);url.searchParams.set('result_loop',value);assert.equal(loopEvaluationSelection(url),null);}
 let url=new URL(location.href);url.searchParams.set('result_evaluation','optimizer');assert.equal(loopEvaluationSelection(url),null);
 backtestEditorAdapter.isV8=false;assert.equal(loopEvaluationSelection(),null);
})().catch(error=>{console.error(error);process.exitCode=1;});
"""
    completed=subprocess.run(['node','-e',script],cwd=root,capture_output=True,text=True,timeout=10)
    assert completed.returncode==0,completed.stdout+completed.stderr


def test_comparison_link_preserves_mount_and_current_loop_context():
    """One click addresses only the selected run and returns to its report tab."""
    root=Path(__file__).resolve().parents[1]
    source=(root/'frontend/js/pb8_loop_optimizer.js').read_text()
    body=source[source.index('  function hasEvaluationResults('):source.index('  function backtestButton(')]
    script=r"""
const assert=require('node:assert/strict');let assigned='';
let location={href:'http://pbgui.test/nested/api/optimize-v8/main_page?loop_id='+'a'.repeat(32)+'&loop_view=run&loop_tab=performance#loops-results',assign:value=>assigned=value};
let state={panel:'loops-results'};function select(id){assert.equal(id,'a'.repeat(32));}
"""+body+r"""
const run={id:'a'.repeat(32),observer_jobs:[{kind:'observer_holdout',simulation_complete:true,status:'complete',log_available:true}]};
openEvaluationComparison(run,'observer_holdout');let url=new URL(assigned);
assert.equal(url.pathname,'/nested/api/backtest-v8/main_page');assert.equal(url.searchParams.get('result_loop'),run.id);
assert.equal(url.searchParams.get('result_evaluation'),'observer_holdout');assert.equal(url.searchParams.get('result_compare'),null);
assert.equal(url.searchParams.get('loop_return'),new URL(location.href).pathname+new URL(location.href).search+'#loops-results');
assigned='';openEvaluationComparison(run,'observer_full_range');assert.equal(assigned,'');
run.observer_jobs[0].simulation_complete=false;assert.equal(hasEvaluationResults(run,'observer_holdout'),false);
"""
    completed=subprocess.run(['node','-e',script],cwd=root,capture_output=True,text=True,timeout=10)
    assert completed.returncode==0,completed.stdout+completed.stderr


def test_all_49_holdouts_load_with_bounded_requests_and_no_reopen_after_close():
    """All jobs are compared without a top-N cap or unrelated-result leakage."""
    root=Path(__file__).resolve().parents[1]
    source=(root/'frontend/v7_backtest.html').read_text()
    body=source[source.index('var _loopEvaluationComparison ='):source.index('async function openLoopResultComparison(')]
    script=r"""
const assert=require('node:assert/strict');
let window={location:{href:'http://pbgui.test/api/backtest-v8/main_page?panel=results&result_loop='+'a'.repeat(32)+'&result_evaluation=observer_holdout&result_compare=1'},addEventListener(){}};
let backtestEditorAdapter={isV8:true},API_BASE='/api/backtest-v8',RESULTS_PAGE_SIZE=100;
let _resultsLoadGeneration=0,_resultsEmptyRetryTimer=null,currentPanel='results',results=[],selected=[];
let active=0,maximum=0,requested=0,plotted=0,chartCurrent,missingResults=false;
let nodes={'results-list':{textContent:''},'compare-chart-area':{style:{}}},document={getElementById:id=>nodes[id]};
let sessionStorage={getItem(){return null;}},jobs=Array.from({length:49},(_,index)=>({name:'job_'+index,kind:'observer_holdout',round:index-1,status:'complete',simulation_complete:true,log_available:true}));
jobs.push({...jobs[0],name:'wrong',kind:'observer_full_range'},{...jobs[0],name:'failed',status:'failed'},{...jobs[0],name:'partial',simulation_complete:false});
async function apiFetchFrom(base,path){
 if(base.endsWith('/loops'))return {observer_jobs:jobs};
 const name=new URL('http://pbgui.test'+path).searchParams.get('name');assert.ok(name.startsWith('job_'));
 requested++;active++;maximum=Math.max(maximum,active);await new Promise(resolve=>setImmediate(resolve));active--;
 if(missingResults&&name==='job_0')return {results:[]};
 return {results:[{config_name:name,path:name+'/result'},{config_name:name,path:name+'/result'},{config_name:'unrelated',path:'unrelated/result'}]};
}
function getSelectedResults(){return selected;}
function setSelectedResults(paths){selected=paths;}
function _applyResultsData(items){results=items;}
function updateResultsCountLabel(){}
function toast(message){throw Error(message);}
async function _compareResultPaths(paths,items,area,id,current){plotted=paths.length;chartCurrent=current;}
"""+body+r"""
(async()=>{
 await loadLoopEvaluationResults(loopEvaluationSelection(),'',{});
 assert.equal(requested,49);assert.equal(results.length,49);assert.equal(selected.length,49);assert.equal(plotted,49);assert.ok(maximum<=4);
 assert.equal(chartCurrent(),true);
 missingResults=true;
 [100,50,30,NaN].forEach((score,index)=>{jobs[index].evaluation_score=score;jobs[index].evaluation_targets_met=index!==1;});
 let topUrl=new URL(window.location.href);topUrl.searchParams.set('result_top','2');window.location.href=topUrl.href;
 await loadLoopEvaluationResults(loopEvaluationSelection(),'',{});
 assert.deepEqual(selected.slice().sort(),['job_1/result','job_2/result']);
 topUrl.searchParams.set('result_top','1');window.location.href=topUrl.href;
 await loadLoopEvaluationResults(loopEvaluationSelection(),'',{});
 assert.deepEqual(selected,['job_2/result']);
 let url=new URL(window.location.href);url.searchParams.delete('result_compare');window.location.href=url.href;
 assert.equal(chartCurrent(),false);
})().catch(error=>{console.error(error);process.exitCode=1;});
"""
    completed=subprocess.run(['node','-e',script],cwd=root,capture_output=True,text=True,timeout=10)
    assert completed.returncode==0,completed.stdout+completed.stderr
