"""Executable visibility, request ownership and queue update regression tests."""

from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parents[2]


def _functions(path, names, indent=''):
    """Extract real top-level functions while retaining their nested callbacks."""
    source = (ROOT / path).read_text()
    parts = []
    for name in names:
        marker = '\n' + indent + 'async function ' + name + '('
        if marker not in source:
            marker = '\n' + indent + 'function ' + name + '('
        start = source.index(marker) + 1
        end = source.index('\n' + indent + '}', start) + len(indent) + 2
        parts.append(source[start:end])
    return '\n'.join(parts)


def _node(code):
    """Run browser logic with deterministic mocked requests and timers."""
    result = subprocess.run(['node', '-e', code], cwd=ROOT, text=True, capture_output=True)
    assert result.returncode == 0, result.stderr


def test_nav_alerts_pause_guard_overlap_and_reject_stale_responses():
    """Hide/show and failed requests preserve a single current request owner."""
    functions = _functions('frontend/pbgui_nav.js', ['fetchAlerts', 'scheduleAlerts', 'stopAlerts'], '  ')
    _node(r'''
const assert = require('node:assert/strict');
let _alertsRequest=null, _alertsGeneration=0, _alertsTimer=null, _alertsStarted=false, _navAlerts={};
let renders=0, nextTimer=0, timers=new Map(), requests=[];
const document={visibilityState:'visible', getElementById:()=>null};
const _getAppBase=()=>'/mounted'; const authOptions=x=>x;
const updateAlertButton=()=>renders++; const renderAlertOverlay=()=>{};
const setInterval=()=>{const id=++nextTimer; timers.set(id,true);return id;};
const clearInterval=id=>timers.delete(id);
const fetch=(url, options)=>new Promise((resolve,reject)=>requests.push({url,options,resolve,reject}));
''' + functions + r'''
(async()=>{
  scheduleAlerts(); assert.equal(timers.size,1);
  const first=fetchAlerts(); fetchAlerts(); assert.equal(requests.length,1);
  assert.equal(requests[0].url,'/mounted/api/vps/alerts');
  document.visibilityState='hidden'; stopAlerts(); scheduleAlerts(); fetchAlerts();
  assert.equal(timers.size,0); assert.equal(requests.length,1);
  assert.equal(requests[0].options.signal.aborted,true);
  document.visibilityState='visible'; const second=fetchAlerts(); scheduleAlerts();
  requests[0].resolve({ok:true,json:async()=>({old:true})}); await first;
  assert.equal(renders,0); assert.ok(_alertsRequest); fetchAlerts(); assert.equal(requests.length,2);
  requests[1].resolve({ok:true,json:async()=>({new:true})}); await second;
  assert.equal(renders,1); assert.deepEqual(_navAlerts,{new:true}); assert.equal(_alertsRequest,null);
  const failed=fetchAlerts(); requests[2].reject(new Error('offline')); await failed;
  assert.equal(_alertsRequest,null);
  const retry=fetchAlerts(); requests[3].resolve({ok:false}); await retry;
  assert.equal(_alertsRequest,null); stopAlerts(); assert.equal(timers.size,0);
})().catch(e=>{console.error(e);process.exitCode=1;});
''')


def test_queue_updates_deduplicate_defer_hidden_and_preserve_dirty_settings():
    """Repeated pushes do no work, hidden updates coalesce and modal edits survive."""
    functions = _functions('frontend/v7_optimize.html', ['applyOptimizeQueueUpdate', 'hasActiveOptimizeRuns'])
    _node(r'''
const assert=require('node:assert/strict');
let renders=0, refreshes=0, syncs=0;
const document={visibilityState:'visible'};
const state={queue:[], queueUpdateSignature:null, queuePushSeq:0,settingsPushSeq:0,
  settings:{cpu:7,cpu_override:false,use_pbgui_market_data:false},hadActiveOptimizeRuns:false,
  settingsModalCpuDirty:true,settingsModalCpuOverrideDirty:true,settingsModalDirty:true};
const el=()=>({classList:{contains:()=>true}});
const normalizeAutostart=x=>x===true||x==='True';
const normalizeOptimizePositiveInteger=x=>Number(x);
const syncQueueSettingsModalFields=()=>syncs++;
const renderQueueMaybeDeferred=()=>renders++;
const refreshLiveResultsDuringRun=async()=>refreshes++;
const handleError=e=>{throw e;};
''' + functions + r'''
const msg={items:[{filename:'job',status:'running'}],settings:{cpu:2,cpu_override:'True',use_pbgui_market_data:'True'}};
applyOptimizeQueueUpdate(msg);applyOptimizeQueueUpdate(JSON.parse(JSON.stringify(msg)));
assert.equal(renders,1);assert.equal(refreshes,1);assert.equal(state.queuePushSeq,1);
assert.equal(state.settings.cpu,7);assert.equal(state.settings.cpu_override,false);
assert.equal(state.settings.use_pbgui_market_data,false);assert.equal(syncs,0);
document.visibilityState='hidden';
applyOptimizeQueueUpdate({items:[],settings:{}});
applyOptimizeQueueUpdate({items:[{filename:'job',status:'complete'}],settings:{}});
assert.equal(renders,1);assert.equal(refreshes,1);assert.equal(state.queue[0].status,'running');
document.visibilityState='visible';applyOptimizeQueueUpdate(state.pendingQueueUpdate);
assert.equal(state.queue[0].status,'complete');assert.equal(renders,2);assert.equal(refreshes,2);
''')


def test_live_results_advance_without_queue_changes_and_pause_in_background():
    """Result progress has its own timer, with visibility and in-flight guards."""
    functions = _functions('frontend/v7_optimize.html', [
        'refreshLiveResultsDuringRun', 'scheduleOptimizeLiveResults', 'hasActiveOptimizeRuns',
    ])
    _node(r'''
const assert=require('node:assert/strict');
const document={visibilityState:'visible'};
const state={panel:'results',queue:[{status:'running'}],livePanelRefreshTimer:null,livePanelRefreshInFlight:false};
let callback=null, requests=0, release=null, intervals=new Set();
const window={setInterval(fn,ms){assert.equal(ms,8000);callback=fn;intervals.add(fn);return fn;},
  clearInterval(fn){intervals.delete(fn);}};
const refreshCurrentPanel=()=>{requests++;return new Promise(resolve=>release=resolve);};
const handleError=e=>{throw e;};
''' + functions + r'''
(async()=>{
  scheduleOptimizeLiveResults();assert.equal(intervals.size,1);
  const first=refreshLiveResultsDuringRun();callback();assert.equal(requests,1);
  release();await first;assert.equal(state.livePanelRefreshInFlight,false);
  const second=refreshLiveResultsDuringRun();assert.equal(requests,2);release();await second;
  document.visibilityState='hidden';scheduleOptimizeLiveResults();await refreshLiveResultsDuringRun(true);
  assert.equal(intervals.size,0);assert.equal(requests,2);
  document.visibilityState='visible';state.queue=[];scheduleOptimizeLiveResults();await refreshLiveResultsDuringRun();
  assert.equal(requests,2);
  const final=refreshLiveResultsDuringRun(true);assert.equal(requests,3);release();await final;
})().catch(e=>{console.error(e);process.exitCode=1;});
''')


def test_return_refresh_preserves_open_settings_edits():
    """Visibility wakeups merge metadata without replacing unsaved modal fields."""
    functions = _functions('frontend/v7_optimize.html', ['loadSettings'])
    _node(r'''
const assert=require('node:assert/strict');
let modalOpen=true,syncs=0;
const state={settingsLoadSeq:0,settingsPushSeq:0,navigationSeq:0,
  settingsModalDirty:true,settingsModalCpuDirty:true,settingsModalCpuOverrideDirty:false,
  settings:{cpu:7,autostart:false,cpu_override:false,use_pbgui_market_data:false}};
const el=()=>({classList:{contains:()=>modalOpen}});
const apiFetch=async()=>({cpu:2,autostart:true,cpu_override:true,use_pbgui_market_data:true,metadata:'current'});
const handlePb8RuntimeUnavailable=()=>false;
const syncQueueSettingsModalFields=()=>syncs++;
const updateMetaCounts=()=>{};
''' + functions + r'''
(async()=>{
  await loadSettings();assert.equal(syncs,0);assert.equal(state.settings.cpu,7);
  assert.equal(state.settings.autostart,false);assert.equal(state.settings.cpu_override,false);
  assert.equal(state.settings.use_pbgui_market_data,false);assert.equal(state.settings.metadata,'current');
  modalOpen=false;await loadSettings();assert.equal(state.settings.cpu,2);assert.equal(state.settings.autostart,true);
})().catch(e=>{console.error(e);process.exitCode=1;});
''')
