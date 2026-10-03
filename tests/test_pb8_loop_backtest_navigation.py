"""Exercise native Backtest deep-link navigation without loading runtime data."""
from pathlib import Path
import subprocess


def test_loop_backtest_links_keep_filters_logs_and_newer_navigation():
    """A native result link filters the real loader and does not override newer hash state."""
    source = (Path(__file__).resolve().parents[1] / 'frontend/v7_backtest.html').read_text()
    start = source.index('async function initializeBacktestPageFromUrl(')
    end = source.index('\nfunction getManagedBacktestBaseDir', start)
    function = source[start:end]
    script = '''
const assert=require('node:assert/strict');let panels=[],filters=[],logs=[];
let window={location:{href:''}},backtestEditorAdapter={isV8:true},_resultsLoadGeneration=0;
function selectPanel(...args){panels.push(args);}
async function loadResults(filter){filters.push(filter);}
function showLog(id){logs.push(id);}
function initializeBacktestLoopReturn(){}
async function openInitialBacktestDraftFromUrl(){return false;}
async function openInitialBacktestQueueDraftFromUrl(){return false;}
function editConfig(){}
'''+function+'''
(async()=>{
 window.location.href='http://pbgui.test/api/backtest-v8/main_page?panel=results&result_filter=loop_name&native_log=11111111-1111-1111-1111-111111111111';
 await initializeBacktestPageFromUrl();
 assert.deepEqual(panels,[['results',{deferResultsLoad:true}]]);
 assert.deepEqual(filters,['loop_name']);assert.equal(logs.length,1);
 panels=[];filters=[];
 window.location.href+='\\x23queue';await initializeBacktestPageFromUrl();
 assert.deepEqual(panels,[]);assert.deepEqual(filters,[]);
 backtestEditorAdapter.isV8=false;logs=[];await initializeBacktestPageFromUrl();assert.deepEqual(logs,[]);
})().catch(error=>{console.error(error);process.exit(1);});
'''
    completed = subprocess.run(['node', '-e', script], capture_output=True, text=True, check=False)
    assert completed.returncode == 0, completed.stderr


def test_backtest_return_button_restores_loop_context_and_rejects_untrusted_destinations():
    """Only a same-origin Loop context is offered, including the selected report tab."""
    source = (Path(__file__).resolve().parents[1] / 'frontend/v7_backtest.html').read_text()
    start = source.index('function getBacktestLoopReturnUrl()')
    end = source.index('async function initializeBacktestPageFromUrl(', start)
    script = r'''
const assert=require('node:assert/strict');let assigned='';
let toolbar={hidden:true,style:{}},button={onclick:null};
let document={getElementById:id=>id==='loop-return-toolbar'?toolbar:button};
let window={location:{href:'',assign:value=>assigned=value}},backtestEditorAdapter={isV8:true};
''' + source[start:end] + r'''
const context='/api/optimize-v8/main_page?loop_id='+ 'a'.repeat(32)+'&loop_view=cycle&loop_cycle=3&loop_tab=changes&loop_compare=initial#loops-results';
function arrive(value){window.location.href='http://pbgui.test/api/backtest-v8/main_page?panel=results&loop_return='+encodeURIComponent(value);initializeBacktestLoopReturn();}
arrive(context);assert.equal(toolbar.hidden,false);button.onclick();
assert.equal(assigned,'http://pbgui.test'+context);
// Reload and navigation inside Backtest preserve the originating context.
window.location.href+='&result_filter=changed#results';initializeBacktestLoopReturn();button.onclick();assert.equal(assigned,'http://pbgui.test'+context);
for(const bad of ['https://evil.test'+context,'//evil.test'+context,context.replace('/optimize-v8/','/backtest-v8/'),context.replace('loop_id='+ 'a'.repeat(32),'loop_id=../secret'),context.replace('#loops-results','&token=secret#loops-results')]){
 arrive(bad);assert.equal(toolbar.hidden,true);assert.equal(button.onclick,null);
}
backtestEditorAdapter.isV8=false;arrive(context);assert.equal(toolbar.hidden,true);
'''
    completed = subprocess.run(['node', '-e', script], capture_output=True, text=True, check=False)
    assert completed.returncode == 0, completed.stderr


def test_loop_results_link_carries_current_report_and_main_view():
    """Native result navigation carries an exact non-secret return context."""
    source = (Path(__file__).resolve().parents[1] / 'frontend/js/pb8_loop_optimizer.js').read_text()
    start = source.index('  function backtestButton(')
    end = source.index('  function canContinueRound(', start)
    script = r'''
const assert=require('node:assert/strict');let handler,assigned='';
let location={origin:'http://pbgui.test',href:'http://pbgui.test/api/optimize-v8/main_page?loop_id='+ 'a'.repeat(32)+'&loop_view=cycle&loop_cycle=1&loop_tab=backtests#loops-results'};
location.assign=value=>assigned=value;
let state={panel:'loops-results',navigationSeq:1},selected='a'.repeat(32),resultsGeneration=0,reportView='cycle',reportRound=1,reportTab='backtests';function select(id){assert.equal(id,'a'.repeat(32));}
function button(parent,label,action){handler=action;return {};}
async function request(){return {name:'specific backtest'};}function error(exc){throw exc;}
''' + source[start:end] + r'''
backtestButton({}, {id:'a'.repeat(32)}, {log_available:true,operation:'b'.repeat(32)});
(async()=>{
 await handler();let target=new URL(assigned);assert.equal(target.pathname,'/api/backtest-v8/main_page');assert.equal(target.searchParams.get('result_filter'),'specific backtest');assert.equal(target.searchParams.get('result_compare'),'1');assert.equal(target.searchParams.get('loop_return'),new URL(location.href).pathname+new URL(location.href).search+'#loops-results');
 assigned='';let resolve;request=()=>new Promise(done=>resolve=done);let pending=handler();state.navigationSeq++;resolve({name:'outdated'});await pending;assert.equal(assigned,'');
 let olderResolve,newerResolve;request=()=>new Promise(done=>olderResolve=done);let older=handler();request=()=>new Promise(done=>newerResolve=done);let newer=handler();newerResolve({name:'latest'});await newer;olderResolve({name:'stale'});await older;assert.equal(new URL(assigned).searchParams.get('result_filter'),'latest');
})().catch(error=>{console.error(error);process.exit(1);});
'''
    completed = subprocess.run(['node', '-e', script], capture_output=True, text=True, check=False)
    assert completed.returncode == 0, completed.stderr


def test_native_compare_does_not_reopen_after_handoff_is_obsolete():
    """A delayed equity reply must respect subsequent closing or navigation."""
    source = (Path(__file__).resolve().parents[1] / 'frontend/v7_backtest.html').read_text()
    start = source.index('function _compareResultPaths(')
    end = source.index('function _resolveQueueComparePaths(', start)
    script = r'''
const assert=require('node:assert/strict');let resolve,current=true,plots=0;
let _beCache={},backtestEditorAdapter={version:'v8'};
function fetchCSV(){return new Promise(done=>resolve=done);}
function normalizeBE(){return {time:['2020-01-01'],balance:[1000],equity:[1000]};}
let Plotly={newPlot:()=>plots++};
''' + source[start:end] + r'''
(async()=>{
 let area={style:{display:'none'},innerHTML:''};
 let pending=_compareResultPaths(['specific/result'],[{path:'specific/result',backtest_version:'v8'}],area,'compare-chart-div',()=>current);
 // User closes the chart or leaves Results while the native CSV is loading.
 current=false;area.style.display='none';area.innerHTML='';resolve({});
 assert.equal(await pending,false);assert.equal(plots,0);
 assert.equal(area.style.display,'none');assert.equal(area.innerHTML,'');
})().catch(error=>{console.error(error);process.exit(1);});
'''
    completed = subprocess.run(['node', '-e', script], capture_output=True, text=True, check=False)
    assert completed.returncode == 0, completed.stderr
