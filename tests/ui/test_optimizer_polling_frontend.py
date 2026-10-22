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
    """Pushes merge each field without changing drafts or validating an initial basis."""
    _node(_settings_harness() + r'''
(async()=>{
  const loading=loadSettings(); requests[0].resolve(saved()); await loading;
  nodes['settings-cpu-value'].value='7'; markQueueSettingsEdited('cpu');
  nodes['settings-autostart'].checked=true; markQueueSettingsEdited('autostart');
  const msg={items:[{filename:'job',status:'running'}],settings:{cpu:2,autostart:false,cpu_override:true,use_pbgui_market_data:true}};
  applyOptimizeQueueUpdate(msg); applyOptimizeQueueUpdate(JSON.parse(JSON.stringify(msg)));
  assert.equal(renders,1); assert.equal(state.queuePushSeq,1);
  assert.equal(nodes['settings-cpu-value'].value,'7'); assert.equal(nodes['settings-autostart'].checked,true);
  assert.equal(nodes['settings-cpu-override'].checked,true); assert.equal(nodes['settings-use-pbgui-market-data'].checked,true);
  document.visibilityState='hidden'; applyOptimizeQueueUpdate({items:[],settings:{cpu:4}});
  assert.equal(renders,1); document.visibilityState='visible'; applyOptimizeQueueUpdate(state.pendingQueueUpdate);
  assert.equal(renders,2); assert.equal(nodes['settings-cpu-value'].value,'7');
  state.settingsLoaded=false; applyOptimizeQueueUpdate({items:[],settings:{cpu:8}});
  assert.equal(state.settingsLoaded,false); assert.equal(nodes['btn-settings-save'].disabled,true);
})().catch(fail);
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


def _settings_harness():
    """Run the production settings controller with isolated DOM, network and clock."""
    functions = _functions('frontend/v7_optimize.html', [
        'queueSettingsFields', 'validateQueueSettings', 'cancelQueueSettingsLoad', 'loadSettings',
        'markQueueSettingsEdited', 'syncQueueSettingsModalFields', 'openQueueSettingsModal',
        'adjustQueueSettingsCpu', 'persistQueueSettings', 'saveQueueSettingsFromModal', 'updateAutostart',
        'applyOptimizeQueueUpdate', 'hasActiveOptimizeRuns',
    ])
    return r'''
const assert=require('node:assert/strict');
const nodes=new Proxy({}, {get(obj,key){return obj[key] ||= {value:'', checked:false, disabled:false,
  textContent:'', hidden:false, classList:{contains:()=>state.panel==='settings'}};}});
const el=id=>nodes[id]; const document={visibilityState:'visible'};
const state={settings:{cpu:1,cpu_max:1},settingsLoadSeq:0,settingsPushSeq:0,settingsLifecycle:0,
  settingsSaveSeq:0,settingsLoaded:false,settingsError:'',settingsDisposed:false,settingsSaving:false,
  settingsRequest:null,settingsDirtyFields:{},settingsEditVersions:{},settingsPushVersions:{},settingsWrittenFields:{},
  navigationSeq:0,panel:'queue',queue:[],queuePushSeq:0};
let renders=0, warnings=0, updates=0, toasts=0, timerId=0;
const requests=[], timers=new Map();
const window={setTimeout(fn,ms){assert.equal(ms,30000);timers.set(++timerId,fn);return timerId;},
  clearTimeout(id){timers.delete(id);}};
const apiFetch=(path,opts={})=>new Promise((resolve,reject)=>requests.push({path,opts,resolve,reject}));
const handlePb8RuntimeUnavailable=e=>{warnings++;return e.status===503;};
const updateMetaCounts=()=>updates++; const toast=()=>toasts++;
const normalizeAutostart=x=>x===true||String(x).toLowerCase()==='true';
const normalizeOptimizePositiveInteger=x=>Number.isSafeInteger(Number(x))&&Number(x)>0?Number(x):null;
const renderQueueMaybeDeferred=()=>renders++; const refreshLiveResultsDuringRun=async()=>{};
const handleError=e=>{throw e;};
const saved=(extra={})=>Object.assign({cpu:8,cpu_max:16,host_cpu_count:16,autostart:false,
  cpu_override:false,use_pbgui_market_data:false},extra);
function setPanel(panel){state.navigationSeq++;state.panel=panel;if(panel==='settings')loadSettings();}
function fail(e){console.error(e);process.exitCode=1;}
''' + functions + '\n'


def test_return_refresh_preserves_open_settings_edits():
    """Returning/reopening revalidates while all four independent drafts survive."""
    _node(_settings_harness() + r'''
(async()=>{
  const first=loadSettings(); requests[0].resolve(saved()); await first;
  state.panel='settings';
  nodes['settings-cpu-value'].value='9';markQueueSettingsEdited('cpu');
  nodes['settings-autostart'].checked=true;markQueueSettingsEdited('autostart');
  nodes['settings-cpu-override'].checked=true;markQueueSettingsEdited('cpu_override');
  nodes['settings-use-pbgui-market-data'].checked=true;markQueueSettingsEdited('use_pbgui_market_data');
  const again=loadSettings(); openQueueSettingsModal();
  assert.equal(requests.length,2); requests[1].resolve(saved({cpu_max:4,host_cpu_count:4})); await again;
  assert.equal(nodes['settings-cpu-value'].value,'9'); assert.equal(nodes['settings-cpu-max'].textContent,'4');
  ['autostart','cpu_override','use_pbgui_market_data'].forEach(k=>assert.equal(nodes[queueSettingsFields()[k]].checked,true));
  await saveQueueSettingsFromModal(); assert.equal(requests.length,2);
  assert.match(nodes['settings-load-status'].textContent,/1 to 4/);
  setPanel('queue');openQueueSettingsModal();requests[2].resolve(saved());await state.settingsRequest.promise;
  assert.equal(nodes['settings-cpu-value'].value,'9'); assert.equal(nodes['btn-settings-save'].disabled,false);
})().catch(fail);
''')


def test_settings_navigation_deduplication_timeout_and_lifecycle():
    """Panel changes retain GETs; timeout/cancellation release ownership and allow recovery."""
    _node(_settings_harness() + r'''
(async()=>{
  openQueueSettingsModal(); const first=loadSettings(); assert.equal(requests.length,1);
  assert.equal(nodes['settings-cpu-value'].value,''); assert.equal(nodes['btn-settings-save'].disabled,true);
  await saveQueueSettingsFromModal(); await updateAutostart(true); assert.equal(requests.length,1);
  setPanel('configs'); requests[0].resolve(saved()); await first;
  assert.equal(state.settingsLoaded,true); assert.equal(state.settings.cpu,8); assert.equal(timers.size,0);
  const late=loadSettings(); timers.values().next().value(); await late;
  assert.match(state.settingsError,/30 seconds/); assert.equal(requests[1].opts.signal.aborted,true);
  assert.equal(nodes['settings-cpu-value'].disabled,true); await updateAutostart(true);assert.equal(requests.length,2);
  openQueueSettingsModal(); const recovery=loadSettings(); requests[2].resolve(saved({cpu:6}));await recovery;
  requests[1].resolve(saved({cpu:1}));await Promise.resolve();assert.equal(state.settings.cpu,6);
  const cancelled=loadSettings();state.settingsDisposed=true;state.settingsLifecycle++;cancelQueueSettingsLoad();
  await cancelled;assert.equal(requests[3].opts.signal.aborted,true);assert.equal(timers.size,0);
  requests[3].resolve(saved({cpu:1}));await Promise.resolve();assert.equal(state.settings.cpu,6);
  state.settingsDisposed=false;const restored=loadSettings();requests[4].resolve(saved());await restored;
  assert.equal(nodes['settings-cpu-value'].value,'8'); assert.equal(toasts,0);
})().catch(fail);
''')


def test_settings_push_http_and_save_ordering():
    """Cached pushes cannot undo Saves; conflicts during GET coalesce one later read."""
    _node(_settings_harness() + r'''
(async()=>{
  const tick=async()=>{await Promise.resolve();await Promise.resolve();await Promise.resolve();};
  const first=loadSettings();applyOptimizeQueueUpdate({settings:{cpu:6},items:[]});
  assert.equal(state.settingsLoaded,false);requests[0].resolve(saved());await first;
  assert.equal(state.settings.cpu,6);assert.equal(state.settings.cpu_max,16);
  nodes['settings-cpu-value'].value='9';markQueueSettingsEdited('cpu');
  const saving=saveQueueSettingsFromModal();assert.equal(requests[1].opts.method,'POST');
  assert.equal(JSON.parse(requests[1].opts.body).cpu,9);
  await saveQueueSettingsFromModal();await updateAutostart(true);await loadSettings();assert.equal(requests.length,2);
  nodes['settings-cpu-value'].value='10';markQueueSettingsEdited('cpu');
  applyOptimizeQueueUpdate({settings:{cpu:8},items:[]});
  requests[1].resolve({});await tick();assert.equal(requests.length,3);
  applyOptimizeQueueUpdate({settings:{cpu:8},items:[{filename:'old-push-during-get'}]});
  assert.equal(requests.length,3);assert.equal(nodes['btn-settings-save'].disabled,true);
  requests[2].resolve(saved({cpu:9}));await tick();assert.equal(requests.length,4);
  requests[3].resolve(saved({cpu:9}));await saving;
  assert.equal(state.settings.cpu,9);assert.equal(nodes['settings-cpu-value'].value,'10');
  assert.equal(state.settingsDirtyFields.cpu,true);
  const get=loadSettings();await saveQueueSettingsFromModal();assert.equal(requests.length,5);
  const newer={settings:{cpu:5},items:[{filename:'other-client-write'}]};
  applyOptimizeQueueUpdate(newer);applyOptimizeQueueUpdate(newer);
  requests[4].resolve(saved({cpu:9}));await tick();assert.equal(requests.length,6);
  requests[5].resolve(saved({cpu:5}));await get;
  assert.equal(state.settings.cpu,5);assert.equal(nodes['settings-cpu-value'].value,'10');
  const second=saveQueueSettingsFromModal();requests[6].resolve({});await tick();
  requests[7].resolve(saved({cpu:10}));await second;
  assert.equal(state.settings.cpu,10);assert.equal(state.settingsDirtyFields.cpu,undefined);
  applyOptimizeQueueUpdate({settings:{cpu:8},items:[{filename:'late-old-push'}]});
  assert.equal(state.settings.cpu,10);const late=state.settingsRequest.promise;
  requests[8].resolve(saved({cpu:10}));await late;assert.equal(state.settings.cpu,10);
})().catch(fail);
''')


def test_settings_invalid_get_and_http_errors_never_enable_save():
    """Metadata, pushes and malformed GETs cannot provide a valid settings baseline."""
    _node(_settings_harness() + r'''
(async()=>{
  for(const payload of [{cpu:8},saved({cpu_max:0}),saved({autostart:'False'})]){
    const pending=loadSettings();requests.at(-1).resolve(payload);await pending;
    assert.equal(state.settingsLoaded,false);assert.equal(nodes['btn-settings-save'].disabled,true);
    await saveQueueSettingsFromModal();assert.equal(requests.at(-1).opts.method,undefined);
  }
  const unavailable=loadSettings();const err=new Error('Run Update PB8.');err.status=503;
  requests.at(-1).reject(err);await unavailable;assert.match(state.settingsError,/Update PB8/);assert.ok(warnings);
  const recovered=loadSettings();requests.at(-1).resolve(saved());await recovered;assert.equal(state.settingsLoaded,true);
})().catch(fail);
''')


def test_settings_failed_save_and_disposed_responses_preserve_drafts():
    """Lost write replies require revalidation; pagehide rejects late POST results."""
    _node(_settings_harness() + r'''
(async()=>{
  const first=loadSettings();requests[0].resolve(saved());await first;
  nodes['settings-cpu-value'].value='9';markQueueSettingsEdited('cpu');
  const failed=saveQueueSettingsFromModal();requests[1].reject(new Error('offline'));
  await assert.rejects(failed,/offline/);assert.equal(state.settingsSaving,false);
  assert.equal(nodes['settings-cpu-value'].value,'9');assert.ok(state.settingsError);
  await saveQueueSettingsFromModal();await updateAutostart(true);assert.equal(requests.length,2);
  const recovery=loadSettings();requests[2].resolve(saved());await recovery;
  const pending=saveQueueSettingsFromModal();state.settingsDisposed=true;
  state.settingsLifecycle++;state.settingsSaveSeq++;state.settingsSaving=false;cancelQueueSettingsLoad();
  requests[3].resolve({});await pending;
  assert.equal(state.settings.cpu,8);assert.equal(toasts,0);assert.equal(requests.length,4);
  state.settingsDisposed=false;const restored=loadSettings();requests[4].resolve(saved({cpu:9}));await restored;
  assert.equal(state.settings.cpu,9);assert.equal(nodes['settings-cpu-value'].value,'9');
  const stale=loadSettings();applyOptimizeQueueUpdate({settings:{cpu:5},items:[]});
  requests[5].reject(new Error('unavailable'));await stale;
  assert.equal(requests.length,6);assert.equal(timers.size,0);
})().catch(fail);
''')
