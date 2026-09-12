"""Executable regressions for remaining UI errors, draft protection and selections."""

from pathlib import Path
import json
import re
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[2]


def function(page, name):
    """Extract one real inline function, including minified transfer handlers."""
    source = (ROOT / 'frontend' / page).read_text()
    match = re.search(r'^([ \t]*)(?:async )?function ' + re.escape(name) + r'\(', source, re.M)
    assert match, name
    line = source[match.start():source.index('\n', match.start())]
    if line.rstrip().endswith('}'):
        return line + '\n'
    end = source.index('\n' + match[1] + '}', match.end()) + len(match[1]) + 2
    return source[match.start():end] + '\n'


def execute(code):
    """Run isolated handlers with strict assertion failures and a bounded timeout."""
    result = subprocess.run(['node', '-e', code], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize('action', ['deleteJob','retryJob','requeueJob'])
@pytest.mark.parametrize('failure', ['http','network'])
def test_history_action_errors_visible(action, failure):
    """A failed history mutation reports its error and never pretends to refresh success."""
    code = function('hl_data_actions.html','historyJobAction') + function('hl_data_actions.html',action)
    code += function('hl_data_actions.html', 'setHistoryDeleteBusy')
    execute(code + f'const action={json.dumps(action)}, failure={json.dumps(failure)};' + r'''
const assert=require('node:assert/strict'); const nodes={modal:{classList:{add:value=>nodes.shown=value}},'modal-title':{},'modal-body':{}};
const $=id=>nodes[id],API_BASE='/api';const authOptions=x=>x;
const deletingJobIds=new Set(),ROOT={isConnected:true,querySelectorAll:()=>[]};
const window={PBGuiDialogs:{confirm:async()=>true}};
function updateModalViewportHeight(){};console.error=()=>{};
const fetch=async()=>{if(failure==='network')throw new Error('offline');return {ok:false,status:500,json:async()=>({detail:'denied'})};};
(async()=>{await eval(action)('job');assert.equal(nodes.shown,'active');assert.match(nodes['modal-title'].textContent,/failed/);assert.match(nodes['modal-body'].textContent,/denied|offline/);})().catch(e=>{console.error=e=>process.stderr.write(String(e));console.error(e);process.exitCode=1;});
''')


@pytest.mark.parametrize('failure', ['401', '403', '500', '502', 'network'])
def test_logging_get_failure_does_not_apply_fake_rotation_defaults(failure):
    """HTTP and proxy failures reach the error banner without replacing form values."""
    code = function('logging_monitor.html', 'apiGet') + function('logging_monitor.html', 'loadRotationSettings')
    execute(code + 'const failure=' + json.dumps(failure) + ';' + r'''
const assert=require('node:assert/strict');
const nodes={'per-svc-loading':{style:{}},'per-svc-body':{},'def-max-mb':{value:'77'},'def-backup-count':{value:'4'}};
const document={getElementById:id=>nodes[id],createElement:()=>{throw new Error('Should not render success');}};
const API_BASE='/api/logging';let rotationSettingsLoaded=false,rotationSettingsLoading=false;
console.error=()=>{};
const fetch=async()=>{
 if(failure==='network')throw new Error('offline');
 return {ok:false,status:Number(failure),json:async()=>{if(failure==='502')throw new SyntaxError('HTML proxy body');return {detail:'Access failed '+failure};}};
};
(async()=>{loadRotationSettings();await new Promise(setImmediate);
 assert.equal(rotationSettingsLoaded,false);assert.equal(rotationSettingsLoading,false);
 assert.equal(nodes['def-max-mb'].value,'77');assert.equal(nodes['def-backup-count'].value,'4');
 assert.match(nodes['per-svc-loading'].textContent,/Failed to load settings: (Access failed|HTTP 502|offline)/);
 assert.equal(nodes['per-svc-loading'].style.display,'block');
})().catch(e=>{process.stderr.write(String(e));process.exitCode=1;});
''')


def test_logging_get_preserves_success_and_rejects_malformed_success():
    """Valid responses pass through; invalid JSON must not become an empty success."""
    execute(function('logging_monitor.html', 'apiGet') + r'''
const assert=require('node:assert/strict');const API_BASE='/api/logging';let malformed=false;
const payload={default:{max_mb:15,backup_count:2},per_service:{}};
const fetch=async()=>({ok:true,status:200,json:async()=>{if(malformed)throw new SyntaxError('bad JSON');return payload;}});
(async()=>{assert.equal(await apiGet('/rotation'),payload);malformed=true;await assert.rejects(apiGet('/rotation'),/bad JSON/);})()
.catch(e=>{console.error(e);process.exitCode=1;});
''')


@pytest.mark.parametrize('mode', ['accept', 'cancel', 'missing', 'detached', 'parent'])
def test_hl_delete_requires_confirmation_and_blocks_duplicate_actions(mode):
    """Only one confirmed deletion can run; cancellation and unavailable dialogs are safe."""
    code = function('hl_data_actions.html', 'deleteJob') + function('hl_data_actions.html', 'setHistoryDeleteBusy')
    execute(code + 'const mode=' + json.dumps(mode) + ';' + r'''
const assert=require('node:assert/strict');let resolveDialog,resolveDelete,requests=0,prompts=0,refreshes=0;
const button={disabled:false,getAttribute:()=> 'job'},ROOT={isConnected:true,querySelectorAll:()=>[button]};
const deletingJobIds=new Set(),currentTab={dl:'done',build:'failed'};
const nodes={modal:{classList:{add(){}}},'modal-title':{},'modal-body':{}},$=id=>nodes[id];
function updateModalViewportHeight(){};console.error=()=>{};
function loadHistoryTab(){refreshes++;}
const dialogs={confirm:options=>{prompts++;assert.equal(options.confirmText,'Delete');return new Promise(resolve=>resolveDialog=resolve);}};
const window=mode==='missing'?{}:{PBGuiDialogs:dialogs};
if(mode==='parent'){window.parent={PBGuiDialogs:dialogs};window.PBGuiDialogs={confirm:()=>{throw new Error('Must use visible parent dialog');}};}
async function historyJobAction(){requests++;return new Promise(resolve=>resolveDelete=resolve);}
(async()=>{
 const first=deleteJob('job');await deleteJob('job');assert.equal(requests,0);
 if(mode==='missing'){await first;assert.match(nodes['modal-body'].textContent,/Confirmation dialog is unavailable/);}
 else{
  assert.equal(prompts,1);assert.equal(button.disabled,true);
  if(mode==='detached')ROOT.isConnected=false;
  resolveDialog(mode!=='cancel');await new Promise(setImmediate);
  if(mode==='accept'||mode==='parent'){
   assert.equal(requests,1);await deleteJob('job');assert.equal(requests,1);
   resolveDelete(true);await first;assert.equal(refreshes,2);
  }else{await first;assert.equal(requests,0);assert.equal(refreshes,0);}
 }
 assert.equal(button.disabled,false);assert.equal(deletingJobIds.size,0);
})().catch(e=>{process.stderr.write(String(e));process.exitCode=1;});
''')


@pytest.mark.parametrize('kind',['saveManagedScope','saveService'])
@pytest.mark.parametrize('failure',['http','network'])
def test_rotation_save_reports_failure(kind,failure):
    """Failed rotation writes remain visible and do not discard inputs."""
    code=function('logging_monitor.html',kind)
    execute(code+f'const kind={json.dumps(kind)},failure={json.dumps(failure)};'+r'''
const assert=require('node:assert/strict');const msg={style:{},classList:{add:x=>msg.visible=x}},input={value:'250'};
const document={getElementById:id=>id.includes('msg')?msg:input};
function numberOrDefault(v){return Number(v);}
const apiPost=async()=>{if(failure==='network')throw new Error('offline');return {success:false,error:'denied'};};
(async()=>{if(kind==='saveManagedScope')saveManagedScope('scope');else saveService('svc',input,input,msg);await new Promise(setImmediate);assert.match(msg.textContent,/Save failed/);assert.equal(msg.visible,'visible');assert.equal(input.value,'250');})().catch(e=>{console.error(e);process.exitCode=1;});
''')


def test_logging_view_preserves_loaded_settings():
    """Returning to Settings leaves the existing form and its unsaved values alone."""
    execute(function('logging_monitor.html','showView')+r'''
const assert=require('node:assert/strict');let settingsVisible=false,rotationSettingsLoaded=true,rotationSettingsLoading=false;
const document={getElementById:()=>({style:{},classList:{add(){},remove(){}}})};
function loadRotationSettings(){throw new Error('Must not reload the edited form');}
showView('viewer');showView('settings');assert.equal(settingsVisible,true);
''')


def test_coin_data_error_is_handled():
    """The fire-and-forget loader fulfills with null after displaying the failure."""
    execute(function('coin_data.html','loadState')+r'''
const assert=require('node:assert/strict');let loadStateRequestSeq=0,loadStateController=null,messages=[];
function setActionStatus(message){messages.push(message);}function buildStateUrl(){return '/state';}
const fetch=async()=>({ok:false,status:500});
(async()=>{assert.equal(await loadState(),null);assert.match(messages.at(-1),/HTTP 500/);})().catch(e=>{console.error(e);process.exitCode=1;});
''')


def test_service_settings_failure_has_retry():
    """A failed PBData settings load removes the spinner and offers a working retry."""
    execute(function('services_monitor.html','loadSettings')+r'''
const assert=require('node:assert/strict');let _settingsLoaded={},_settingsLoading={},calls=0,applied=0;
const wrap={children:[],replaceChildren(){this.children=[];},append(...children){this.children.push(...children);}};
const document={getElementById:()=>wrap,createElement:()=>({addEventListener(type,fn){this.click=fn;}})};
const API_BASE='',authOptions=()=>({});console.error=()=>{};
const fetch=async()=>{calls++;return {ok:calls>1,json:async()=>({})};};function applySettings(){applied++;}
(async()=>{await loadSettings('pbdata');assert.match(wrap.children[0].textContent,/Failed/);assert.equal(wrap.children[1].textContent,'Retry');wrap.children[1].click();await new Promise(setImmediate);assert.equal(applied,1);assert.equal(_settingsLoaded.pbdata,true);})().catch(e=>{process.stderr.write(String(e));process.exitCode=1;});
''')


@pytest.mark.parametrize('accept',[False,True])
def test_balance_calc_replacement_confirmation(accept):
    """Cancel preserves the editor; accepted loading also protects edits made during fetch."""
    execute(function('balance_calc.html','loadInstanceConfig')+f'const accept={str(accept).lower()};'+r'''
const assert=require('node:assert/strict');let configLoadGeneration=0,lastInstanceSelection='previous',requests=0,resolve;
const configEditor={value:'custom'},btnCalc={},calcStatus={},selExchange={},selInstance={value:'next'},API_BASE='';
const window={PBGuiDialogs:{confirm:async()=>accept}};const authHeaders=()=>({});const showError=()=>{};
const fetch=()=>{requests++;return new Promise(r=>resolve=r);};
(async()=>{const pending=loadInstanceConfig({name:'alice',version:'v8'});await new Promise(setImmediate);assert.equal(configEditor.value,'custom');
if(accept){configEditor.value='typed while loading';resolve({ok:true,json:async()=>({config:{new:true}})});await pending;assert.equal(configEditor.value,'typed while loading');assert.equal(btnCalc.disabled,false);}
else{await pending;assert.equal(requests,0);assert.equal(selInstance.value,'previous');}})().catch(e=>{console.error(e);process.exitCode=1;});
''')


def test_profit_sweep_cancel_preserves_account():
    """Rejected account replacement cannot clear the dirty policy or start loaders."""
    execute(function('profit_sweep.html','selectAccount')+r'''
const assert=require('node:assert/strict');const state={users:[{name:'old'},{name:'new'}],selectedUser:'old',policyBaseline:'saved',record:{draft:true}};
const policyFingerprint=()=> 'changed';const window={PBGuiDialogs:{confirm:async()=>false}};
(async()=>{await selectAccount('new');assert.equal(state.selectedUser,'old');assert.deepEqual(state.record,{draft:true});})().catch(e=>{console.error(e);process.exitCode=1;});
''')


def test_pareto_keeps_custom_z_across_dimensions():
    """A hidden Z selection returns when the user switches back to a 3D plot."""
    code=function('v7_pareto_explorer.html','updatePlaygroundCustomUi')+function('v7_pareto_explorer.html','preserveCurrentCustomMetrics')
    execute(code+r'''
const assert=require('node:assert/strict');const state={playground:{quickView:'Custom...',vizType:'2D Scatter',customZMetric:'chosen'}};
const el=()=>({style:{}}),filterCustomMetrics=x=>x,setSelectOptions=(node,options,value,fallback)=>value||fallback||options[0];
const payload={available_metrics:['fallback','chosen'],metrics:{x_metric:'x',y_metric:'y'}};
updatePlaygroundCustomUi(payload);preserveCurrentCustomMetrics(payload);assert.equal(state.playground.customZMetric,'chosen');
state.playground.vizType='3D Scatter';updatePlaygroundCustomUi(payload);assert.equal(state.playground.customZMetric,'chosen');
''')


def test_retention_fetch_preserves_keystrokes():
    """A delayed settings response cannot overwrite a retention edit or paint it saved."""
    code=function('v7_run.html','fetchRetention')+function('v7_run.html','markRetentionDirty')
    execute(code+r'''
const assert=require('node:assert/strict');let _retSavedVal=50,_retentionGeneration=0,_retentionEditGeneration=0,resolve;
const input={value:'50',style:{}},document={getElementById:()=>input};const apiFetch=()=>new Promise(r=>resolve=r);
(async()=>{fetchRetention();input.value='99';markRetentionDirty();resolve({ok:true,json:async()=>({max_versions:20})});await new Promise(setImmediate);assert.equal(input.value,'99');assert.equal(input.style.color,'#ff9800');})().catch(e=>{console.error(e);process.exitCode=1;});
''')


def test_simulation_workspace_preserves_completed_plot():
    """Re-entering Simulation plots its completed result instead of the base grid."""
    execute(function('v7_strategy_explorer.html','renderSimulationWorkspace')+r'''
const assert=require('node:assert/strict');const sim={fills:[1]},state={simulations:{local:sim},activeSimulationMode:'local'},plots=[];
const document={getElementById:()=>({classList:{contains:()=>true}})};
function renderSideTuning(){}function updateActiveChip(){}function deepGet(){return false;}
function simulationSnapshotForPlot(value){assert.equal(value,sim);return {result:true};}function renderPlot(side,snapshot){plots.push(snapshot);}
renderSimulationWorkspace({base:true});assert.deepEqual(plots,[{result:true},{result:true}]);
''')


@pytest.mark.parametrize('ending',['done','error','network','post_error'])
def test_dbtools_operation_unlock_and_resume(ending):
    """All exits unlock the run button; lost polling resumes the original operation."""
    code=function('db_tools.html','pollOperation')+function('db_tools.html','startOperation')
    execute(code+f'const ending={json.dumps(ending)};'+r'''
const assert=require('node:assert/strict');const activeOperations=Object.create(null),operationButtons={'test':'button'};
const button={disabled:false},el=()=>button;let posts=0,polls=0,callbacks=0,messages=[],recover=false;
function setStatus(id,message){messages.push(message);}function resetProgress(){}function renderProgress(){}
const postJson=async()=>{posts++;if(ending==='post_error')throw new Error('offline');return {operation:{id:'original'}};};
const apiFetch=async path=>{polls++;assert.match(path,/original/);if(ending==='network'&&!recover)throw new Error('lost status');return {operation:{status:ending==='error'?'error':'done',result:{ok:true},error:'failed'}};};
(async()=>{const first=startOperation('/run',{},'test','progress',()=>callbacks++);assert.equal(button.disabled,true);
assert.equal(startOperation('/run',{},'test','progress'),first);await first;
assert.equal(posts,1);assert.equal(button.disabled,false);assert.equal(callbacks,ending==='done'?1:0);
if(ending==='network'){recover=true;await startOperation('/run',{},'test','progress',()=>callbacks++);assert.equal(posts,1);assert.equal(callbacks,1);assert.equal(polls,2);}
})().catch(e=>{console.error(e);process.exitCode=1;});
''')


@pytest.mark.parametrize('action',['closeSyncEditor','newSyncJob','selectSyncJob'])
def test_sync_editor_cancel_preserves_dirty_form(action):
    """Close, New and row selection all leave a dirty draft intact when rejected."""
    code=function('db_tools.html','syncFingerprint')+function('db_tools.html','confirmSyncDiscard')+function('db_tools.html',action)
    execute(code+f'const action={json.dumps(action)};'+r'''
const assert=require('node:assert/strict');let syncBaseline='saved',syncDiscardRequest=0,syncEditorGeneration=0;
const state={syncJobId:'old'},el=()=>({classList:{contains:()=>true}}),syncPayload=()=>({name:'changed'});
const confirmModal=async()=>false;
(async()=>{await eval(action)('new');assert.equal(state.syncJobId,'old');assert.equal(syncEditorGeneration,0);assert.equal(syncBaseline,'saved');})().catch(e=>{console.error(e);process.exitCode=1;});
''')


def test_transfer_refresh_retains_selected_route():
    """Repeated preview rendering prefers the account's selected route over defaults."""
    execute(function('transfers.html','renderPreview')+r'''
const assert=require('node:assert/strict');const route={options:[],value:'',replaceChildren(){this.options=[];},appendChild(option){this.options.push(option);if(option.selected||this.options.length===1)this.value=option.value;}};
const nodes={'route-select':route};const el=id=>nodes[id]||(nodes[id]={});
const state={selected:'alice',users:[{name:'alice'}],routeSelections:{alice:'chosen'},preview:{route:'default',routes:[{id:'default'},{id:'chosen'}]}};
const window={location:{href:'http://local/'}},document={createElement:()=>({}),querySelectorAll:()=>[]};
const text=(v,f)=>v||f,routeLabel=v=>v;function renderPositions(){}function applyRoute(){}
renderPreview();assert.equal(route.value,'chosen');renderPreview();assert.equal(route.value,'chosen');
state.pendingRequest={user:'alice',route:'default'};renderPreview();assert.equal(route.value,'default');
''')


def test_tag_controller_callback_refreshes_ignore_preview():
    """Internal multiselect renders reach Dynamic Ignore through the real callback."""
    source=(ROOT/'frontend/v7_edit.html').read_text()
    start=source.index('var _runMsController = ')
    end=source.index('var msState =',start)
    execute(r'''
const assert=require('node:assert/strict');let callback,structured=0,previews=0;
const window={PBGuiEditorShared:{createMultiselectController:options=>{callback=options.onAfterRender;return {};}}};
const runEditorAdapter={version:7},msCounterpart={},msCoinIds={};
function scheduleStructuredEditorSync(){structured++;}function _scheduleIgnoreRefresh(){previews++;}
'''+source[start:end]+r'''
callback('ms-tags');callback('ms-approved-long');assert.equal(structured,2);assert.equal(previews,1);
''')


def test_delete_dashboard_clears_iframe_handler():
    """Deleting the active dashboard loads about:blank and drops the previous onload."""
    source=(ROOT/'frontend/dashboard_main.html').read_text()
    start=source.index("  document.getElementById('del-ok').addEventListener")
    end=source.index('\n  });',start)+len('\n  });')
    execute(r'''
const assert=require('node:assert/strict');let click,currentDash='old',editMode=true,selectedDashboards=['old'];
const _pendingDelete=['old'],contentFrame={onload:()=>{},classList:{remove(){}}},contentLoading={style:{}},editBanner={classList:{remove(){}}},delDialog={classList:{remove(){}}};
const document={getElementById:()=>({addEventListener:(type,fn)=>click=fn})},apiFetch=async()=>({ok:true});function renderToolbar(){}function refreshList(fn){fn();}
'''+source[start:end]+r'''
(async()=>{click();await new Promise(setImmediate);assert.equal(contentFrame.src,'about:blank');assert.equal(contentFrame.onload,null);assert.equal(currentDash,'');})().catch(e=>{console.error(e);process.exitCode=1;});
''')


@pytest.mark.parametrize('action',['executeDelete','executeForcedMode'])
def test_old_run_action_cannot_close_new_modal(action):
    """Completion only dismisses the initiating modal; duplicate confirms are ignored."""
    execute(function('v7_run.html',action)+f'const action={json.dumps(action)};'+r'''
const assert=require('node:assert/strict');let resolve,requests=0,closed=0;
const root={firstElementChild:{}},confirm={disabled:false},cancel={disabled:false};
const document={getElementById:id=>id==='modal-root'?root:id==='modal-confirm'?confirm:cancel};
const apiFetch=()=>{requests++;return new Promise(r=>resolve=r);};function closeModal(){closed++;}function toast(){}function render(){}function loadInstances(){}
let instances=[],rowMap={};
(async()=>{eval(action)('alice','panic','Panic',1);eval(action)('alice','panic','Panic',1);assert.equal(requests,1);assert.equal(cancel.disabled,true);
root.firstElementChild={newModal:true};resolve({ok:true,json:async()=>({version:2})});await new Promise(setImmediate);assert.equal(closed,0);})().catch(e=>{console.error(e);process.exitCode=1;});
''')


def test_conversion_prevents_duplicate_requests():
    """The conversion guard stays held across the complete request and navigation."""
    execute(function('v7_run.html','convertInstanceToV8')+r'''
const assert=require('node:assert/strict');let conversionBusy=false,requests=0,resolve;
const window={location:{origin:'http://local',href:''},PBGuiDialogs:{alert:async()=>{}}};
function setV8RunConversionStatus(){}function toast(){}function v8RunMigrationDetail(){return '';}
const fetch=()=>{requests++;return new Promise(r=>resolve=r);};
(async()=>{const first=convertInstanceToV8('alice');await convertInstanceToV8('alice');assert.equal(requests,1);
resolve({ok:true,status:200,json:async()=>({name:'alice_v8'})});await first;assert.equal(conversionBusy,false);assert.match(window.location.href,/alice_v8/);})().catch(e=>{console.error(e);process.exitCode=1;});
''')


@pytest.mark.parametrize('first',['runSimulation','runCompare'])
def test_simulation_and_compare_mutual_exclusion(first):
    """Neither execution can start while the other owns the shared action guard."""
    code=function('v7_strategy_explorer.html','runSimulation')+function('v7_strategy_explorer.html','runCompare')
    execute(code+f'const first={json.dumps(first)};'+r'''
const assert=require('node:assert/strict');let simulationActionBusy=false,simulationRequestGeneration=0,compareRequestGeneration=0,simulationProgressId='',compareProgressId='',reject,requests=0;
const state={config:{},simulations:{}},nodes={},document={getElementById:id=>nodes[id]||(nodes[id]={disabled:false})},window={crypto:{randomUUID:()=> 'id'}};
function stopSimulationProgressPolling(){}function stopCompareProgressPolling(){}function simulationOptions(){return {};}function compareOptions(){return {};}
function esc(value){return value;}function updateSimulationButtons(){}function setMessages(){}function setSimulationProgress(){}function setCompareProgress(){}function startSimulationProgressPolling(){}function startCompareProgressPolling(){}
const apiFetch=()=>{requests++;return new Promise((res,rej)=>reject=rej);};
(async()=>{eval(first)('local');runSimulation('local');runCompare();assert.equal(requests,1);assert.equal(nodes['btn-run-compare'].disabled,true);assert.equal(nodes['btn-run-local-sim'].disabled,true);
reject(new Error('offline'));await new Promise(setImmediate);assert.equal(simulationActionBusy,false);assert.equal(nodes['btn-run-compare'].disabled,false);assert.equal(nodes['btn-run-local-sim'].disabled,false);})().catch(e=>{console.error(e);process.exitCode=1;});
''')


@pytest.mark.parametrize('column,values,expected',[
    ('version',[None,0,2,'10'],[None,0,2,'10']),
    ('twe',['L=10.5 S=0','L=2.5 S=0','L=2.10 S=0'],['L=2.10 S=0','L=2.5 S=0','L=10.5 S=0']),
])
def test_run_numeric_sort_is_symmetric(column,values,expected):
    """Missing versions, zero and decimal TWE values retain a total numeric order."""
    source=(ROOT/'frontend/v7_run.html').read_text()
    start=source.index('  filtered.sort(function(a, b) {')
    end=source.index('\n  });',start)+len('\n  });')
    snippet=source[start:end]
    execute('const values='+json.dumps(values)+',expected='+json.dumps(expected)+',column='+json.dumps(column)+';'+r'''
const assert=require('node:assert/strict');const sort={col:column,asc:true};
let filtered=values.map((value,index)=>({[column]:value,status:'active',name:'row'+index}));
'''+snippet+r'''
assert.deepEqual(filtered.map(row=>row[column]),expected);
filtered.reverse();
'''+snippet+r'''
assert.deepEqual(filtered.map(row=>row[column]),expected);
''')


def test_retention_save_preserves_newer_edit():
    """An earlier save response cannot clear edits typed after clicking Save."""
    code=function('v7_run.html','saveRetention')+function('v7_run.html','markRetentionDirty')
    execute(code+r'''
const assert=require('node:assert/strict');let _retSavedVal=50,_retentionGeneration=0,_retentionEditGeneration=0,resolve;
const input={value:'20',style:{}},msg={style:{}},document={getElementById:id=>id==='backup-retention'?input:msg};
const apiFetch=()=>new Promise(r=>resolve=r),setTimeout=()=>{};
(async()=>{saveRetention();input.value='99';markRetentionDirty();resolve({ok:true,json:async()=>({max_versions:20})});await new Promise(setImmediate);assert.equal(input.value,'99');assert.equal(input.style.color,'#ff9800');})().catch(e=>{console.error(e);process.exitCode=1;});
''')


def test_movie_export_missing_controls_uses_defaults():
    """Early export settings reads tolerate missing optional controls."""
    execute(function('v7_strategy_explorer.html','selectedMovieExportOptions')+r'''
const assert=require('node:assert/strict'),document={getElementById:()=>null};function intValue(id,value){return value;}function movieExportFilename(){return 'movie';}
assert.equal(selectedMovieExportOptions(null).codec,'auto');assert.equal(selectedMovieExportOptions(null).preset,'Balanced');
''')


@pytest.mark.parametrize('target',['old','new'])
def test_invalid_policy_still_prompts_before_replacing_account(target):
    """Incomplete numeric input remains a dirty draft, including same-account refresh."""
    execute(function('profit_sweep.html','selectAccount')+'const target='+json.dumps(target)+';'+r'''
const assert=require('node:assert/strict');let prompts=0;
const state={users:[{name:'old'},{name:'new'}],selectedUser:'old',policyBaseline:'saved',record:{draft:true}};
const policyFingerprint=()=>{throw new Error('must be numeric');};const window={PBGuiDialogs:{confirm:async()=>{prompts++;return false;}}};
(async()=>{await selectAccount(target);assert.equal(prompts,1);assert.equal(state.selectedUser,'old');assert.deepEqual(state.record,{draft:true});})().catch(e=>{console.error(e);process.exitCode=1;});
''')
