"""Offline request ownership and stale navigation checks for Loop comparisons."""

from javascript_checks import run_javascript
from pathlib import Path


def test_observer_summary_typed_progress_and_legacy_fallback():
    """Exercise production summary helpers with deliberately asymmetric reports."""
    root = Path(__file__).resolve().parents[1]
    source = (root / 'frontend/js/pb8_loop_optimizer.js').read_text()
    helpers = source[source.index('  function observerProgress('):source.index('  function overviewValue(')]
    script = r"""
const assert=browserAssert;
function numeric(value){return String(value);}
""" + helpers + r"""
const run={observer_enabled:true,observer_jobs:[],observer_selections:[{round:0,status:'waiting',completed:2,expected:99}]};
for(const kind of ['observer_holdout','observer_full_range'])for(let i=0;i<5;i++)run.observer_jobs.push({
 round:0,kind,candidate_id:'c'+i,operation:kind+i,status:i<(kind==='observer_holdout'?2:4)?'complete':'running',
 report_done:true,simulation_complete:i<1,metrics:{gain:7,drawdown:.1}});
assert.match(observerSummary(run,0,'observer_holdout'),/Holdouts · 2\/5 recorded terminal · 1 complete/);
assert.match(observerSummary(run,0,'observer_full_range'),/Full Time Range · 4\/5 recorded terminal · 1 complete/);
assert.ok(!observerSummary(run,0,'observer_full_range').includes('99'));
delete run.observer_selections;
assert.equal(selectedObserver(run,0,'observer_full_range'),undefined); // no arbitrary first job
run.observer_jobs.find(j=>j.operation==='observer_full_range0').selected_for_full_range=true;
assert.equal(observerSummary(run,0,'observer_full_range'),'7 × / 10 %');
run.history=[{round:0,winner_operation:null,winner_candidate_id:null}];
assert.equal(selectedObserver(run,0,'observer_full_range'),undefined);
run.history=[{round:0,winner_operation:'train',winner_candidate_id:'c0',observations:[{operation:'train',candidate_id:'c0'}]}];
run.jobs=[{operation:'train',round:0,kind:'validation',status:'complete'}];
assert.equal(observerSummary(run,0,'observer_full_range'),'7 × / 10 %');
run.observer_summaries=[{round:0,kind:'observer_holdout',status:'disabled'},
 {round:0,kind:'observer_full_range',status:'selected',operation:'observer_full_range0',source_round:0}];
assert.equal(observerSummary(run,0,'observer_holdout'),'Holdout disabled');
assert.equal(observerSummary(run,0,'observer_full_range'),'7 × / 10 %');
run.observer_summaries[1]={round:0,kind:'observer_full_range',status:'unavailable',terminal:5,expected:5,successful:3,report_failed:2};
assert.match(observerSummary(run,0,'observer_full_range'),/No complete Full Time Range · 5\/5 terminal · 3 complete · 2 report errors/);
"""
    result = run_javascript(['-e', script], cwd=root, capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stdout + result.stderr


def test_loop_comparison_ignores_stale_owner_and_result_responses():
    """Late replies cannot overwrite a newer panel, run or comparison request."""
    root=Path(__file__).resolve().parents[1]
    source=(root/'frontend/v7_backtest.html').read_text()
    body=source[source.index('var backtestReads='):source.index("window.addEventListener('pagehide'",source.index('var backtestReads='))]+source[source.index('var _loopEvaluationComparison ='):source.index('async function openLoopResultComparison(')]
    script=r"""
const assert=browserAssert;
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
})().catch(error=>{console.error(error);testResult.exitCode=1;});
"""
    completed=run_javascript(['-e',script],cwd=root,capture_output=True,text=True,timeout=10)
    assert completed.returncode==0,completed.stdout+completed.stderr


def test_comparison_link_preserves_mount_and_current_loop_context():
    """One click addresses only the selected run and returns to its report tab."""
    root=Path(__file__).resolve().parents[1]
    source=(root/'frontend/js/pb8_loop_optimizer.js').read_text()
    body=source[source.index('  function leaveForBacktest('):source.index('  function cancelResultTarget(')]+source[source.index('  function hasEvaluationResults('):source.index('  function backtestButton(')]
    script=r"""
const assert=browserAssert;let assigned='';
let location={href:'http://pbgui.test/nested/api/optimize-v8/main_page?loop_id='+'a'.repeat(32)+'&loop_view=run&loop_tab=performance#loops-results',assign:value=>assigned=value};
let history={length:2},sessionStorage={setItem(){}};function saveLoopScroll(){}
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
    completed=run_javascript(['-e',script],cwd=root,capture_output=True,text=True,timeout=10)
    assert completed.returncode==0,completed.stdout+completed.stderr


def test_all_49_holdouts_load_with_bounded_requests_and_no_reopen_after_close():
    """All jobs are compared without a top-N cap or unrelated-result leakage."""
    root=Path(__file__).resolve().parents[1]
    source=(root/'frontend/v7_backtest.html').read_text()
    body=source[source.index('var backtestReads='):source.index("window.addEventListener('pagehide'",source.index('var backtestReads='))]+source[source.index('var _loopEvaluationComparison ='):source.index('async function openLoopResultComparison(')]
    script=r"""
const assert=browserAssert;
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
 jobs[4].ai_selected=true;jobs[5].ai_selected=true;
 await loadLoopEvaluationResults(loopEvaluationSelection(),'',{});
 assert.deepEqual(selected,['job_5/result']);
 let url=new URL(window.location.href);url.searchParams.delete('result_compare');window.location.href=url.href;
 assert.equal(chartCurrent(),false);
})().catch(error=>{console.error(error);testResult.exitCode=1;});
"""
    completed=run_javascript(['-e',script],cwd=root,capture_output=True,text=True,timeout=10)
    assert completed.returncode==0,completed.stdout+completed.stderr
