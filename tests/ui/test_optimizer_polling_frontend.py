"""Executable visibility, request ownership and queue update regression tests."""

from javascript_checks import run_javascript

from pathlib import Path


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
    result = run_javascript(['-e', code], cwd=ROOT, text=True, capture_output=True)
    assert result.returncode == 0, result.stderr


def test_nav_alerts_pause_guard_overlap_and_reject_stale_responses():
    """Hide/show and failed requests preserve a single current request owner."""
    functions = _functions('frontend/pbgui_nav.js', ['fetchAlerts', 'scheduleAlerts', 'stopAlerts'], '  ')
    _node(r'''
const assert = browserAssert;
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
})().catch(e=>{console.error(e);testResult.exitCode=1;});
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
const assert=browserAssert;
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
})().catch(e=>{console.error(e);testResult.exitCode=1;});
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
const assert=browserAssert;
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
function fail(e){console.error(e);testResult.exitCode=1;}
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


def _config_harness():
    """Run actual list/count ownership with controlled requests and time."""
    functions = _functions('frontend/v7_optimize.html', [
        'configCountValue', 'configCountText', 'cancelConfigRequests', 'configCountsVisible',
        'loadConfigCounts', 'loadConfigs', 'scheduleOptimizeConfigs', 'removeConfigsFromState',
        'configNavigationKey', 'persistConfigNavigation', 'restoreConfigNavigation',
        'getConfigSortValue', 'sortConfigs',
    ])
    return r'''
const assert=browserAssert;
const optimizeEditorAdapter={isV8:true};
const document={visibilityState:'visible'};
const state={panel:'configs',configGeneration:0,configs:[],selectedConfigs:new Set(),
 configRequest:null,configCountRequest:null,configsDisposed:false,configsLoadedOnce:false,
 configPollTimer:null,configSearch:'',configSortKey:'backtest_count',configSortDir:'asc',configPollError:''};
let sequence=0, renders=0;
const timers=new Map(), intervals=new Map(), storage=new Map(), requests=[];
const window={setTimeout(fn,ms){assert.ok(ms===10000 || ms===2000);const id=++sequence;timers.set(id,()=>{timers.delete(id);fn();});return id;},
 clearTimeout(id){timers.delete(id);},setInterval(fn,ms){assert.equal(ms,30000);intervals.set(++sequence,fn);return sequence;},
 clearInterval(id){intervals.delete(id);},sessionStorage:{getItem:k=>storage.get(k),setItem:(k,v)=>storage.set(k,v)}};
const input={value:'',focus:true};const el=()=>input;
const renderConfigs=()=>renders++;
const apiFetch=(path,opts={})=>new Promise((resolve,reject)=>requests.push({path,opts,resolve,reject}));
const compareSortValues=(a,b,dir)=>(a===b?0:a<b?-1:1)*(dir==='desc'?-1:1);
const flush=async()=>{for(let i=0;i<8;i++)await Promise.resolve();};
const pending=name=>({name,backtest_count:null,backtest_count_status:'pending'});
const known=(name,n)=>({name,backtest_count:n,backtest_count_status:'known'});
const fail=e=>{console.error(e);testResult.exitCode=1;};
''' + functions + '\n'


def test_config_counts_status_sort_and_navigation():
    """Unknowns sort last in both directions, zero stays known, and PB7/PB8 storage is separate."""
    _node(_config_harness() + r'''
assert.equal(configCountText(pending('p')),'…');
assert.equal(configCountText({name:'u',backtest_count_status:'unknown'}),'—');
assert.equal(configCountText(known('zero',0)),'0');
assert.equal(configCountValue({backtest_count:null}),null);
const rows=[pending('p'),known('two',2),known('zero',0),{name:'u',backtest_count:null,backtest_count_status:'unknown'}];
assert.deepEqual(sortConfigs(rows).map(x=>x.name),['zero','two','p','u']);
state.configSortDir='desc';assert.deepEqual(sortConfigs(rows).map(x=>x.name),['two','zero','p','u']);
state.configSearch='two';state.selectedConfigs.add('two');persistConfigNavigation();
const key=configNavigationKey();state.configSearch='';state.selectedConfigs.clear();restoreConfigNavigation();
assert.equal(state.configSearch,'two');assert.equal(input.value,'two');assert.ok(state.selectedConfigs.has('two'));
optimizeEditorAdapter.isV8=false;assert.notEqual(configNavigationKey(),key);
''')


def test_config_list_coalesces_batches_and_rejects_deleted_or_stale_responses():
    """Usable rows precede counts; list/count generations cannot revive deleted configurations."""
    _node(_config_harness() + r'''
(async()=>{
 const first=loadConfigs(), same=loadConfigs();assert.equal(first,same);assert.equal(requests.length,1);
 assert.match(requests[0].path,/include_result_summary=false&include_backtest_counts=false/);
 requests[0].resolve({configs:Array.from({length:190},(_,i)=>pending('config-'+i))});await first;
 assert.equal(state.configs.length,190);assert.equal(renders,1);assert.equal(requests.length,2);
 assert.equal(JSON.parse(requests[1].opts.body).names.length,16);
 let batches=0;
 while(state.configCountRequest){
  const req=requests.at(-1);const names=JSON.parse(req.opts.body).names;assert.ok(names.length<=16);
  assert.equal(req.path,'/configs/backtest-counts');
  req.resolve({configs:names.map(n=>known(n,0))});batches++;await flush();
 }
 assert.equal(batches,12);assert.ok(state.configs.every(c=>c.backtest_count===0));assert.equal(timers.size,0);
 const stale=loadConfigs();const oldList=requests.at(-1);
 const newer=loadConfigs(true);assert.equal(oldList.opts.signal.aborted,true);
 requests.at(-1).resolve({configs:[pending('new')]});await newer;
 const oldCount=requests.at(-1);removeConfigsFromState(['new']);
 oldList.resolve({configs:[pending('deleted')]});await stale;
 oldCount.resolve({configs:[known('new',99)]});await flush();
 assert.equal(state.configs.length,0);assert.equal(timers.size,0);
 const deleted=loadConfigs();const deletedList=requests.at(-1);removeConfigsFromState(['deleted']);
 deletedList.resolve({configs:[pending('deleted')]});await deleted;assert.equal(state.configs.length,0);
})().catch(fail);
''')


def test_config_poll_resumes_incomplete_count_pass_after_list_changes():
    """Polls refresh metadata, resume unfinished counts, and reject interrupted responses."""
    _node(_config_harness() + r'''
(async()=>{
 scheduleOptimizeConfigs(true);
 const first=state.configRequest.promise;
 const rows=Array.from({length:190},(_,i)=>({...pending('config-'+i),modified:'v1'}));
 requests[0].resolve({configs:rows});
 await first;
 const batch1=requests.at(-1);
 batch1.resolve({configs:JSON.parse(batch1.opts.body).names.map(name=>known(name,1))});
 await flush();
 const interrupted=requests.at(-1);
 Array.from(intervals.values())[0]();
 assert.equal(interrupted.opts.signal.aborted,true);
 const nextList=requests.at(-1);assert.match(nextList.path,/\/configs\?/);
 const changed=rows.filter(cfg=>cfg.name!=='config-1').map(cfg=>({...pending(cfg.name),modified:cfg.modified}));
 changed[0].modified='v2';
 delete changed[1].modified;
 changed.push({...pending('new'),modified:'v1'});
 nextList.resolve({configs:changed});await flush();
 assert.equal(state.configCountProgress.completed.has('config-1'),false);
 const resumed=requests.at(-1);
 assert.deepEqual(JSON.parse(resumed.opts.body).names.slice(0,3),['config-0','config-2','config-16']);
 assert.equal(state.configs.find(cfg=>cfg.name==='config-3').backtest_count,1);
 interrupted.resolve({configs:[known('config-16',999)]});await flush();
 assert.notEqual(state.configs.find(cfg=>cfg.name==='config-16').backtest_count,999);
 let batches=0;
 while(state.configCountRequest){
  const req=requests.at(-1);
  const names=JSON.parse(req.opts.body).names;
  req.resolve({configs:names.map(name=>known(name,1))});
  await flush();batches++;
 }
 assert.equal(batches,12);assert.ok(state.configs.every(cfg=>cfg.backtest_count===1));
 assert.equal(state.configCountProgress,null);
 Array.from(intervals.values())[0]();
 const fresh=requests.at(-1);fresh.resolve({configs:changed.map(cfg=>({...pending(cfg.name),modified:cfg.modified}))});
 await flush();assert.equal(JSON.parse(requests.at(-1).opts.body).names[0],'config-0');
 const active=requests.at(-1);document.visibilityState='hidden';scheduleOptimizeConfigs(false);
 assert.equal(active.opts.signal.aborted,true);assert.equal(state.configCountProgress,null);
})().catch(fail);
''')


def test_config_count_timeout_advances_to_later_batches():
    """A timed-out batch becomes unknown and does not starve subsequent names."""
    _node(_config_harness() + r'''
(async()=>{
 state.configs=Array.from({length:33},(_,i)=>({...known('config-'+i,9),modified:'v1'}));
 const first=loadConfigs();
 requests[0].resolve({configs:Array.from({length:33},(_,i)=>({...pending('config-'+i),modified:'v1'}))});
 await first;
 const slow=requests.at(-1);
 Array.from(timers.values())[0]();assert.equal(slow.opts.signal.aborted,true);
 const error=new Error('timeout');error.name='AbortError';slow.reject(error);await flush();
 assert.ok(state.configs.slice(0,16).every(cfg=>cfg.backtest_count_status==='unknown'));
 const second=requests.at(-1);
 assert.equal(JSON.parse(second.opts.body).names[0],'config-16');
 second.resolve({configs:JSON.parse(second.opts.body).names.map(name=>known(name,2))});await flush();
 const last=requests.at(-1);last.resolve({configs:JSON.parse(last.opts.body).names.map(name=>known(name,2))});await flush();
 assert.equal(state.configs[32].backtest_count,2);assert.equal(state.configCountProgress,null);
})().catch(fail);
''')


def test_pending_count_scans_retry_fairly_for_190_configs():
    """All scans make progress in bounded batches without evicting active cache entries."""
    _node(_config_harness() + r'''
(async()=>{
 scheduleOptimizeConfigs(true);
 state.configs=[{...known('config-0',0),modified:'v1'}];
 const first=state.configRequest.promise;
 const rows=Array.from({length:190},(_,i)=>({...pending('config-'+i),modified:'v1'}));
 requests[0].resolve({configs:rows});await first;
 const active=new Map(), capacity=256;
 let batches=0;
 while(state.configCountRequest){
  const req=requests.at(-1), names=JSON.parse(req.opts.body).names;
  assert.ok(names.length<=16);
  const results=names.map(name=>{
   if(!active.has(name)){assert.ok(active.size<capacity);active.set(name,0);}
   active.set(name,active.get(name)+1);
   return active.get(name)<3 ? pending(name) : known(name,1);
  });
  req.resolve({configs:results});await flush();batches++;
  if(batches===1){
   const interrupted=requests.at(-1);
   Array.from(intervals.values())[0]();assert.equal(interrupted.opts.signal.aborted,true);
   requests.at(-1).resolve({configs:rows.map(cfg=>({...pending(cfg.name),modified:cfg.modified}))});await flush();
   assert.equal(JSON.parse(requests.at(-1).opts.body).names[0],'config-16');
   interrupted.resolve({configs:[known('config-16',999)]});await flush();
  }
 }
 assert.equal(batches,12);assert.equal(active.size,190);
 assert.equal(configCountText(state.configs[0]),'0');
 assert.equal(configCountText(state.configs[1]),'…');
 for(let round=2;round<=3;round++){
  assert.ok(state.configCountRetryTimer);assert.equal(timers.size,1);
  Array.from(timers.values())[0]();await flush();let roundBatches=0;
  while(state.configCountRequest){
   const req=requests.at(-1), names=JSON.parse(req.opts.body).names;
   assert.ok(names.length<=16);
   req.resolve({configs:names.map(name=>{
    active.set(name,active.get(name)+1);
    return active.get(name)<3 ? pending(name) : known(name,1);
   })});await flush();roundBatches++;
  }
  assert.equal(roundBatches,12);
 }
 assert.ok(state.configs.every(cfg=>cfg.backtest_count===1));
 assert.equal(state.configCountProgress,null);assert.equal(timers.size,0);
})().catch(fail);
''')


def test_pending_count_retry_cancelled_on_navigation():
    """A delayed retry cannot revive count requests after leaving the Configs panel."""
    _node(_config_harness() + r'''
(async()=>{
 scheduleOptimizeConfigs(true);
 requests[0].resolve({configs:[{...pending('slow'),modified:'v1'}]});await flush();
 requests.at(-1).resolve({configs:[pending('slow')]});await flush();
 assert.equal(configCountText(state.configs[0]),'…');
 assert.ok(state.configCountRetryTimer);const retry=Array.from(timers.values())[0];
 const before=requests.length;state.panel='queue';scheduleOptimizeConfigs(false);
 assert.equal(timers.size,0);assert.equal(state.configCountRetryTimer,null);
 retry();await flush();assert.equal(requests.length,before);
})().catch(fail);
''')


def test_config_progress_rechecks_a_deleted_and_reintroduced_name():
    """A name absent from one list cannot reuse progress if it reappears later."""
    _node(_config_harness() + r'''
(async()=>{
 scheduleOptimizeConfigs(true);
 const first=state.configRequest.promise;
 const rows=Array.from({length:33},(_,i)=>({...pending('config-'+i),modified:'v1'}));
 requests[0].resolve({configs:rows});await first;
 const done=requests.at(-1);
 done.resolve({configs:JSON.parse(done.opts.body).names.map(name=>known(name,1))});await flush();
 Array.from(intervals.values())[0]();
 requests.at(-1).resolve({configs:rows.filter(cfg=>cfg.name!=='config-0').map(cfg=>({...pending(cfg.name),modified:cfg.modified}))});await flush();
 assert.equal(state.configCountProgress.completed.has('config-0'),false);
 const interrupted=requests.at(-1);
 Array.from(intervals.values())[0]();assert.equal(interrupted.opts.signal.aborted,true);
 requests.at(-1).resolve({configs:rows.map(cfg=>({...pending(cfg.name),modified:cfg.modified}))});await flush();
 const resumed=requests.at(-1);
 assert.ok(JSON.parse(resumed.opts.body).names.includes('config-0'));
 interrupted.resolve({configs:[known('config-0',99)]});await flush();
 assert.equal(state.configs.find(cfg=>cfg.name==='config-0').backtest_count_status,'pending');
 cancelConfigRequests();
})().catch(fail);
''')


def test_config_refresh_preserves_known_counts_and_sort_order_until_replaced():
    """Unchanged rows keep their displayed counts and order through every refresh batch."""
    _node(_config_harness() + r'''
(async()=>{
 const initial=Array.from({length:190},(_,i)=>({
  ...known('config-'+String(i).padStart(3,'0'),189-i),modified:'2025-01-01T00:00:00'
 }));
 state.configs=initial;
 const sorted=()=>sortConfigs(state.configs).map(cfg=>[cfg.name,configCountText(cfg)]);
 const original=sorted();
 const refreshed=loadConfigs();
 requests.at(-1).resolve({configs:initial.map(cfg=>({...pending(cfg.name),modified:cfg.modified}))});
 await refreshed;
 assert.deepEqual(sorted(),original);
 let batches=0;
 while(state.configCountRequest){
  const req=requests.at(-1);const names=JSON.parse(req.opts.body).names;
  req.resolve({configs:names.map(name=>known(name,initial.find(cfg=>cfg.name===name).backtest_count))});
  await flush();batches++;
  assert.deepEqual(sorted(),original);
 }
 assert.equal(batches,12);
 state.configs.push(known('missing-modified',4));
 const changed=loadConfigs();
 requests.at(-1).resolve({configs:[
  {...pending(initial[0].name),modified:initial[0].modified},
  {...pending(initial[1].name),modified:'2025-01-02T00:00:00'},
  {...pending('new'),modified:'2025-01-01T00:00:00'},
  pending('missing-modified')
 ]});
 await changed;
 assert.equal(configCountText(state.configs[0]),'189');
 assert.equal(configCountText(state.configs[1]),'…');
 assert.equal(configCountText(state.configs[2]),'…');
 assert.equal(configCountText(state.configs[3]),'…');
 requests.at(-1).resolve({configs:[known(initial[0].name,0),known(initial[1].name,2),known('new',1),known('missing-modified',4)]});
 await flush();
 assert.deepEqual(sorted(),[[initial[0].name,'0'],['new','1'],[initial[1].name,'2'],['missing-modified','4']]);
})().catch(fail);
''')


def test_config_polling_visibility_timeout_cleanup_and_pb7_compatibility():
    """Polling stays single-owner, aborts on leave/deadline, recovers and never sends PB8 batches for PB7."""
    _node(_config_harness() + r'''
(async()=>{
 scheduleOptimizeConfigs(true);assert.equal(intervals.size,1);assert.equal(requests.length,1);
 const req=requests[0];const task=state.configRequest.promise;
 Array.from(intervals.values())[0]();assert.equal(requests.length,1);
 document.visibilityState='hidden';scheduleOptimizeConfigs(false);
 assert.equal(req.opts.signal.aborted,true);assert.equal(timers.size,0);assert.equal(intervals.size,0);
 req.resolve({configs:[pending('old')]});await task;assert.equal(renders,0);
 document.visibilityState='visible';scheduleOptimizeConfigs(true);
 requests.at(-1).resolve({configs:[pending('current')]});await flush();
 const count=requests.at(-1);Array.from(timers.values())[0]();assert.equal(count.opts.signal.aborted,true);
 const error=new Error('aborted');error.name='AbortError';count.reject(error);await flush();
 assert.equal(state.configs[0].backtest_count_status,'unknown');assert.equal(timers.size,0);
 Array.from(intervals.values())[0]();requests.at(-1).resolve({configs:[pending('current')]});await flush();
 requests.at(-1).resolve({configs:[known('current',3)]});await flush();assert.equal(state.configs[0].backtest_count,3);
 state.configsDisposed=true;cancelConfigRequests();scheduleOptimizeConfigs(false);
 assert.equal(intervals.size,0);assert.equal(timers.size,0);
 state.configsDisposed=false;optimizeEditorAdapter.isV8=false;
 const pb7=loadConfigs();assert.equal(requests.at(-1).path,'/configs');
 const n=requests.length;requests.at(-1).resolve({configs:[{name:'pb7',backtest_count:7}]});await pb7;
 assert.equal(requests.length,n);assert.equal(state.configs[0].backtest_count,7);
})().catch(fail);
''')


def test_config_restored_selection_is_pruned_only_after_success():
    """Initial placeholders and failures retain restored selection; successful snapshots prune it."""
    functions = _functions('frontend/v7_optimize.html', ['updateConfigSelectionUi', 'pruneSelectionSet'])
    _node(_config_harness() + functions + r'''
const filterConfigs=()=>state.configs;
state.selectedConfigs=new Set(['saved','removed']);
updateConfigSelectionUi([]);
assert.deepEqual([...state.selectedConfigs],['saved','removed']);
state.configs=[pending('saved')];state.configsLoadedOnce=true;
updateConfigSelectionUi(state.configs);
assert.deepEqual([...state.selectedConfigs],['saved']);
''')
