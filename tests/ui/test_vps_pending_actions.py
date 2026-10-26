"""Execute pending-action recovery with mocked browser controls and sockets."""

from pathlib import Path
import re
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[2]


def function(source, name):
    """Extract a top-level inline function using its closing indentation."""
    match = re.search(r'^( *)function ' + name + r'\(', source, re.M)
    assert match
    end = source.index('\n' + match[1] + '}', match.end())
    return source[match.start():end + len(match[1]) + 2]


def run(script):
    """Run isolated JavaScript assertions without browser or network access."""
    subprocess.run(['node', '-e', script], check=True, capture_output=True, text=True, timeout=10)


@pytest.mark.parametrize('action', ['kill-instance', 'restart-service'])
@pytest.mark.parametrize('finish', ['result', 'timeout', 'disconnect', 'send-failure'])
def test_monitor_pending_survives_redraw(action, finish):
    """Redrawn buttons stay pending and cannot submit duplicate commands."""
    source = (ROOT / 'frontend/vps_monitor.html').read_text()
    names = ['monitorActionKey', 'finishMonitorAction', 'restoreKillButton', 'restoreRestartButton',
             'resetPendingMonitorActions', 'handleResult', 'killInstance', 'restartService',
             'renderInstanceActions', 'renderServiceRestartButton']
    script = "const action = %r, finish = %r;\n" % (action, finish)
    script += "\n".join(function(source, name) for name in names)
    script += r'''
const assert = require('node:assert/strict');
const pendingMonitorActions = new Map();
let calls = 0, timers = new Map(), nextTimer = 0, buttons = [];
let cpuHistoryTimeout = null;
function setTimeout(fn) { const id = ++nextTimer; timers.set(id, fn); return id; }
function clearTimeout(id) { timers.delete(id); }
function send() { calls++; return finish !== 'send-failure'; }
function escAttr(s) { return String(s); }
const document = {querySelectorAll: () => buttons};
function button() {
 const classes = new Set();
 return {disabled:false, style:{}, classList:{contains:s=>classes.has(s), add:s=>classes.add(s), remove:s=>classes.delete(s)},
 getAttribute: key => ({'data-monitor-action':action, 'data-host':'host', 'data-bot':'bot', 'data-service':'svc'})[key]};
}
const old = button(); buttons = [old];
const start = btn => action === 'kill-instance' ? killInstance('host','bot','8',btn) : restartService('host','svc',btn);
start(old);
if (finish === 'send-failure') {
 assert.equal(pendingMonitorActions.size, 0); assert.equal(old.disabled, false);
} else {
 const html = action === 'kill-instance' ? renderInstanceActions({host:'host', name:'bot', pbVersion:'8'}) : renderServiceRestartButton('host','svc');
 assert.ok(html.includes('disabled'));
 assert.ok(html.includes(action === 'kill-instance' ? 'killing' : 'restarting'));
 const fresh = button(); buttons = [fresh];
 start(fresh); assert.equal(calls, 1);
 if (finish === 'timeout') Array.from(timers.values())[0]();
 else if (finish === 'disconnect') resetPendingMonitorActions('lost');
 else handleResult({cmd:action === 'kill-instance' ? 'kill_instance' : 'restart_service', host:'host', name:'bot', service:'svc', success:true});
 assert.equal(pendingMonitorActions.size, 0);
 assert.equal(fresh.disabled, false);
 start(fresh); assert.equal(calls, 2);
}
'''
    run(script)


def test_manager_disconnect_restores_all_pending_settings():
    """Disconnect clears in-flight hosts and removes queued reads without replay."""
    source = (ROOT / 'frontend/vps_manager.html').read_text()
    script = function(source, 'clearInterruptedSettingsReads') + function(source, 'updateReadSettingsProgress')
    script += r'''
const assert = require('node:assert/strict');
const store = {vps:{one:{readSettingsLoading:true}, two:{readSettingsLoading:false}}, pendingWsMessages:[{cmd:'read_vps_settings'}, {cmd:'refresh'}]};
function ensureVpsUi(host) { return store.vps[host]; }
clearInterruptedSettingsReads();
assert.equal(store.vps.one.readSettingsLoading, false);
assert.equal(store.vps.one.readSettingsProgress.status, 'error');
assert.ok(store.vps.one.readSettingsProgress.label.includes('Reconnect and retry'));
assert.equal(store.vps.two.readSettingsProgress, undefined);
assert.deepEqual(store.pendingWsMessages, [{cmd:'refresh'}]);
'''
    run(script)
    close = source.split('ws.onclose = function (event)', 1)[1].split('function scheduleReconnect', 1)[0]
    assert 'clearInterruptedSettingsReads();' in close
    assert 'forceView: true' in close


def logging_script(real_dialogs=False, api_base='/api/vps-manager'):
    """Load real logging handlers and renderer with isolated DOM/socket adapters."""
    source = (ROOT / 'frontend/vps_manager.html').read_text()
    names = ['defaultVpsLoggingConfig', 'getVpsLoggingConfig', 'setVpsLoggingValue',
             'syncVpsLoggingSettings', 'finishVpsLoggingSave', 'interruptVpsLoggingSave',
             'saveVpsLoggingConfig', 'renderVpsLoggingSettingsView', 'getViewShellSignature',
             'isOverviewSettingsView', 'handleMessage', 'handleResult', 'connectWs',
             'clearInterruptedSettingsReads', 'send', 'queueWsMessage', 'flushPendingWsMessages']
    if real_dialogs:
        names += ['toast', 'openAlertModal', 'openMasterPasswordPrompt']
    script = '\n'.join(function(source, name) for name in names)
    # Execute the actual page-exit registration, rather than invoking cleanup alone.
    script += '\n' + next(line for line in source.splitlines()
                          if "window.addEventListener('pagehide'" in line)
    bootstrap = r'''
const assert = require('node:assert/strict');
let timers = new Map(), nextTimer = 0, messages = [], toasts = [], builds = 0;
let button = null, inputs = [], renderedSignature = '', reconnects = 0;
const events = {};
const window = {addEventListener: (name, fn) => { events[name] = fn; }, location:{replace(){}}};
const WS_BASE = '';
const store = {view:'settings-vps-logging', hostname:'', vps:{}, pendingWsMessages:[],
 vpsLoggingSave:null, vpsLoggingSaveSequence:0, vpsLoggingDrafts:Object.create(null),
 vpsLoggingDraftRevision:0, selectedOverviewHosts:['test-host'],
 state:{config:{vps_logging:{services:[
  {service:'PBRun', max_mb:1, default_max_mb:1},
  {service:'PBRemote', max_mb:2, default_max_mb:2}
 ]}}}};
const document = {activeElement:null, getElementById: id => id === 'save-log-btn' ? button : null,
 querySelectorAll: () => inputs};
const stableStringify = JSON.stringify;
const getSelectedOverviewHosts = () => store.selectedOverviewHosts.slice();
const esc = value => String(value).replaceAll('&','&amp;').replaceAll("'",'&#39;').replaceAll('<','&lt;');
const escAttr = esc, js = JSON.stringify;
const toast = (...args) => toasts.push(args);
function setTimeout(fn, delay) { assert.equal(delay, 30000); const id = ++nextTimer; timers.set(id, fn); return id; }
function clearTimeout(id) { timers.delete(id); }
const refreshLocalInteractiveState = () => {};
const filterDeletedOverviewHosts = () => {}, maybeRefreshCompletedVpsTask = () => {};
const reconcileCurrentVpsContext = () => false;
const renderSummaryBar = () => {}, scheduleReconnect = () => { reconnects++; };
const isVpsContextView = () => false;
class WebSocket {
 static OPEN = 1; static CLOSED = 3; static CLOSING = 2;
 readyState = 1;
 send(payload) { messages.push(JSON.parse(payload)); }
}
function renderUi(options) {
 if (!isOverviewSettingsView()) { button = null; inputs = []; renderedSignature = ''; return; }
 const signature = getViewShellSignature();
 if ((options || {}).forceView || signature !== renderedSignature) {
  renderedSignature = signature;
  const html = renderVpsLoggingSettingsView(); builds++;
  const tag = html.match(/<button id='save-log-btn'[^>]*>([^<]*)<\/button>/);
  assert.ok(tag, 'actual Settings renderer must expose the save button');
  button = {disabled:/\sdisabled(?:\s|>)/.test(tag[0]), textContent:tag[1]};
  assert.equal(button.disabled, !!store.vpsLoggingSave, 'rendered disabled attribute must match pending state');
  assert.equal(button.textContent, store.vpsLoggingSave ? 'Saving...' : 'Save Log Settings');
  inputs = Array.from(html.matchAll(/<input data-vps-logging-service='([^']*)'[^>]*value='([^']*)'/g),
   match => ({value:match[2], getAttribute:() => match[1]}));
 }
 syncVpsLoggingSettings();
}
function reply(id, type='result', data) {
 handleMessage({type, cmd:'save_vps_logging_config', request_id:id, success:true,
  data:data || {services:[{service:'PBRun', max_mb:4}, {service:'PBRemote', max_mb:2}]}});
}
'''
    if real_dialogs:
        bootstrap = 'const API_BASE = ' + repr(api_base) + ';\n' + bootstrap
        bootstrap = bootstrap.replace('const toast = (...args) => toasts.push(args);', '')
        bootstrap = bootstrap.replace('assert.equal(delay, 30000); ', '')
    return bootstrap + script + r'''
connectWs(); renderUi();
''' + (logging_dialog_bootstrap() if real_dialogs else '')


def logging_dialog_bootstrap():
    """Adapt DOM and notifications while retaining the real toast and input dialog."""
    return r'''
const elements = {}, posts = [];
let resets = 0, confirmed = 0, closed = 0;
function element(id) {
 const classes = new Set();
 return {id, style:{}, children:[], value:'', listeners:{}, disabled:false,
  classList:{add:x=>classes.add(x), remove:x=>classes.delete(x), contains:x=>classes.has(x)},
  appendChild(node) { this.children.push(node); node.parent=this; },
  remove() { this.parent.children=this.parent.children.filter(node=>node!==this); },
  focus() { document.activeElement=this; },
  addEventListener(type, callback) { this.listeners[type]=callback; },
  clear() { this.children.forEach(child=>delete elements[child.id]); this.children=[]; },
  set textContent(value) { this.clear(); this.text=value; this.html=''; },
  get textContent() { return this.text || ''; },
  set innerHTML(value) {
   this.clear(); this.html=value;
   for (const match of value.matchAll(/id=['"]([^'"]+)['"]/g)) {
    const child=element(match[1]); elements[child.id]=child; this.children.push(child);
   }
  },
  get innerHTML() { return this.html || ''; }
 };
}
for (const id of ['toastContainer','dialogToastContainer','alertModalOverlay',
 'alertModalTitle','alertModalBody','alertModalBtnRow']) elements[id]=element(id);
document.getElementById = id => id === 'save-log-btn' ? button : elements[id] || null;
document.createElement = () => element('');
function fetch(path, options) { posts.push({path, options}); return Promise.resolve({ok:true}); }
function resetAlertModalWindow() { resets++; }
function isClusterOnboardModalVisible() { return false; }
function isClusterOnboardRunning() { return false; }
function closeAlertModal() { closed++; elements.alertModalOverlay.classList.remove('visible'); }
store.master = {};
function openNewDialog() {
 openMasterPasswordPrompt(() => { confirmed++; }, 'Synthetic fixture action');
 // Run only the actual prompt's focus callback; leave save and toast timers pending.
 const focusId=nextTimer;
 const focus=timers.get(focusId); timers.delete(focusId); focus();
 const input=document.getElementById('master-pw-prompt-input');
 input.value='nonsecret-fixture'; input.focus();
 return {input, body:elements.alertModalBody.innerHTML, buttons:elements.alertModalBtnRow.innerHTML,
  confirm:elements['master-pw-prompt-confirm'], click:elements['master-pw-prompt-confirm'].listeners.click,
  keydown:input.listeners.keydown};
}
'''


@pytest.mark.parametrize('outcome', ['error', 'timeout', 'disconnect', 'send-failure', 'success'])
@pytest.mark.parametrize('open_dialog,api_base,log_path', [
    (True, '/api/vps-manager', '/api/notify_log'),
    (False, '/api/vps-manager', '/api/notify_log'),
    (True, '/mounted/api/vps-manager', '/mounted/api/notify_log'),
])
def test_logging_save_feedback_preserves_real_input_dialog(outcome, open_dialog, api_base, log_path):
    """Late save feedback preserves a real newer prompt; closed-dialog errors keep their modal."""
    run(logging_script(real_dialogs=True, api_base=api_base)
        + 'const logPath = %r;\n' % log_path
        + 'const outcome = %r, openDialog = %s;\n' % (outcome, str(open_dialog).lower()) + r'''
setVpsLoggingValue('PBRun', '4');
let dialog;
if (outcome === 'send-failure') store.ws.send = () => {
 if (openDialog) dialog=openNewDialog();
 throw new Error('Synthetic send failure');
};
saveVpsLoggingConfig();
const pending=store.vpsLoggingSave;
const saveTimer=pending && timers.get(pending.timer);
if (outcome !== 'send-failure' && openDialog) dialog=openNewDialog();
if (outcome === 'error') reply(pending.requestId, 'error');
if (outcome === 'timeout') saveTimer();
if (outcome === 'disconnect') { store.ws.readyState=WebSocket.CLOSED; store.ws.onclose({code:1006}); }
if (outcome === 'success') reply(pending.requestId);
assert.equal(store.vpsLoggingSave, null);
if (pending) assert.equal(timers.has(pending.timer), false, 'save timer must be cancelled');
assert.equal(button.disabled, false);
assert.equal(messages.length, outcome === 'send-failure' ? 0 : 1);
assert.deepEqual(store.pendingWsMessages, []);
assert.equal(confirmed, 0); assert.equal(closed, 0);
assert.equal(posts.length, 1); assert.equal(posts[0].path, logPath);
assert.equal(posts[0].options.credentials, 'same-origin');
assert.ok(!posts[0].options.body.includes('nonsecret-fixture'));
const layer=elements[openDialog ? 'dialogToastContainer' : 'toastContainer'];
const other=elements[openDialog ? 'toastContainer' : 'dialogToastContainer'];
assert.equal(layer.children.length, 1); assert.equal(other.children.length, 0);
assert.equal(layer.children[0].className, 'toast ' + (outcome === 'success' ? 'success' : 'error'));
if (['timeout','disconnect'].includes(outcome)) assert.match(layer.children[0].textContent, /outcome is unknown/);
if (outcome !== 'success') assert.equal(store.vpsLoggingDrafts.PBRun.value, '4');
if (openDialog) {
 assert.equal(resets, 0);
 assert.equal(elements.alertModalTitle.textContent, 'Master sudo password required');
 assert.equal(elements.alertModalBody.innerHTML, dialog.body);
 assert.equal(elements.alertModalBtnRow.innerHTML, dialog.buttons);
 assert.equal(elements['master-pw-prompt-input'], dialog.input);
 assert.equal(dialog.input.value, 'nonsecret-fixture');
 assert.equal(document.activeElement, dialog.input);
 assert.equal(elements['master-pw-prompt-confirm'], dialog.confirm);
 assert.equal(dialog.confirm.listeners.click, dialog.click);
 assert.equal(dialog.input.listeners.keydown, dialog.keydown);
} else if (outcome !== 'success') {
 assert.equal(resets, 1);
 assert.equal(elements.alertModalTitle.textContent, 'Error');
 assert.match(elements.alertModalBtnRow.innerHTML, />Close<\/button>/);
} else assert.equal(resets, 0);
// Old replies/timers cannot create another notice or consume the preserved callback.
if (pending) { reply(pending.requestId); reply(pending.requestId, 'error'); saveTimer(); }
assert.equal(posts.length, 1); assert.equal(confirmed, 0);
for (const [id, callback] of Array.from(timers)) { timers.delete(id); callback(); }
for (const [id, callback] of Array.from(timers)) { timers.delete(id); callback(); }
assert.equal(layer.children.length, 0); assert.equal(timers.size, 0);
if (openDialog) {
 assert.equal(document.activeElement, dialog.input);
 // Only an isolated no-op fixture callback is exercised; no command can run.
 dialog.click(); assert.equal(confirmed, 1); assert.equal(closed, 1);
 assert.equal(messages.length, outcome === 'send-failure' ? 0 : 1);
}
''')


def test_logging_save_locks_real_renderer_across_views_and_correlates_replies():
    """One send survives redraw/navigation; only its reply releases and notifies."""
    run(logging_script() + r'''
setVpsLoggingValue('PBRun', '4');
const originalInput = inputs[0]; document.activeElement = originalInput;
originalInput.value = '4';
const initialBuilds = builds;
saveVpsLoggingConfig(); saveVpsLoggingConfig({silent:true});
assert.equal(messages.length, 1); assert.equal(timers.size, 1);
assert.equal(button.disabled, true); assert.equal(button.textContent, 'Saving...');
assert.equal(builds, initialBuilds); assert.equal(document.activeElement, originalInput);
const first = store.vpsLoggingSave, oldTimer = timers.get(first.timer);
renderUi({forceView:true});
assert.equal(button.disabled, true);
assert.equal(button.textContent, 'Saving...');
store.view = 'overview'; renderUi(); store.view = 'settings-vps-logging'; renderUi();
assert.equal(button.disabled, true); assert.equal(inputs[0].value, '4');
for (const id of ['', 'foreign']) for (const type of ['error', 'result']) reply(id, type);
assert.equal(store.vpsLoggingSave, first); assert.equal(toasts.length, 0);
assert.equal(store.state.config.vps_logging.services[0].max_mb, 1);
reply(first.requestId);
assert.equal(store.vpsLoggingSave, null); assert.equal(timers.size, 0);
assert.equal(button.disabled, false); assert.equal(button.textContent, 'Save Log Settings');
assert.equal(store.state.config.vps_logging.services[0].max_mb, 4);
assert.equal(store.vpsLoggingDrafts.PBRun, undefined); assert.equal(toasts.length, 1);
saveVpsLoggingConfig({silent:true});
const second = store.vpsLoggingSave;
assert.notEqual(first.requestId, second.requestId); assert.equal(messages.length, 2);
oldTimer(); reply(first.requestId); reply(first.requestId, 'error');
assert.equal(store.vpsLoggingSave, second); assert.equal(toasts.length, 1);
reply(second.requestId); assert.equal(toasts.length, 1); assert.equal(timers.size, 0);
''')


@pytest.mark.parametrize('finish', ['error', 'timeout', 'disconnect', 'send-failure', 'send-false', 'leave'])
def test_logging_save_terminal_paths_clean_timers_and_allow_retry(finish):
    """Every terminal path unlocks without replay; stale callbacks cannot end a retry."""
    run(logging_script() + 'const finish = %r;\n' % finish + r'''
setVpsLoggingValue('PBRun', '4');
const originalInput = inputs[0], initialBuilds = builds;
originalInput.value = '4'; document.activeElement = originalInput;
const socket = store.ws;
if (finish === 'send-failure') socket.send = () => { throw new Error('synthetic'); };
if (finish === 'send-false') {
 let checks = 0;
 Object.defineProperty(socket, 'readyState', {configurable:true, get:() => ++checks === 1 ? 1 : 3});
}
saveVpsLoggingConfig();
const first = store.vpsLoggingSave;
const oldTimer = first && timers.get(first.timer);
if (finish === 'error') reply(first.requestId, 'error');
if (finish === 'timeout') oldTimer();
if (finish === 'disconnect') { socket.readyState = WebSocket.CLOSED; socket.onclose({code:1006}); }
if (finish === 'leave') events.pagehide();
assert.equal(store.vpsLoggingSave, null); assert.equal(timers.size, 0);
assert.equal(builds, initialBuilds); assert.equal(document.activeElement, originalInput);
assert.equal(button.disabled, false); assert.equal(button.textContent, 'Save Log Settings');
assert.equal(store.vpsLoggingDrafts.PBRun.value, '4');
assert.equal(store.pendingWsMessages.length, 0);
assert.equal(messages.length, ['send-failure','send-false'].includes(finish) ? 0 : 1);
assert.equal(toasts.length, finish === 'leave' ? 0 : 1);
if (['timeout','disconnect'].includes(finish)) assert.match(toasts[0][0], /outcome is unknown/);
const count = messages.length, toastCount = toasts.length;
if (first) { reply(first.requestId); reply(first.requestId, 'error'); }
assert.equal(toasts.length, toastCount);
connectWs(); saveVpsLoggingConfig();
const second = store.vpsLoggingSave;
assert.equal(messages.length, count + 1); assert.equal(button.disabled, true);
if (oldTimer) oldTimer();
assert.equal(store.vpsLoggingSave, second);
// A replaced socket must not interrupt its successor's save.
socket.onclose({code:1006}); assert.equal(store.vpsLoggingSave, second);
reply(second.requestId); assert.equal(timers.size, 0); assert.equal(button.disabled, false);
''')


def test_logging_save_never_queues_or_replays_while_offline():
    """Logging saves are excluded even from direct queue calls; other commands still queue."""
    run(logging_script() + r'''
store.ws.readyState = WebSocket.CLOSED;
saveVpsLoggingConfig();
assert.equal(store.vpsLoggingSave, null); assert.equal(button.disabled, false);
assert.equal(timers.size, 0); assert.equal(messages.length, 0);
queueWsMessage({cmd:'save_vps_logging_config'});
send({cmd:'save_vps_logging_config'});
assert.deepEqual(store.pendingWsMessages, []);
send({cmd:'refresh'}); assert.deepEqual(store.pendingWsMessages, [{cmd:'refresh'}]);
connectWs(); flushPendingWsMessages();
assert.deepEqual(messages, [{cmd:'refresh'}]);
''')


@pytest.mark.parametrize('edit_after_send', ['none', 'changed', 'changed-back'])
def test_logging_drafts_survive_live_state_and_save_snapshot(edit_after_send):
    """Live values never overwrite edits; acknowledgements clear only unchanged drafts."""
    run(logging_script() + 'const edit = %r;\n' % edit_after_send + r'''
setVpsLoggingValue('PBRun', '4'); inputs[0].value = '4';
const originalInput = inputs[0], initialBuilds = builds;
document.activeElement = originalInput;
saveVpsLoggingConfig(); const pending = store.vpsLoggingSave;
if (edit !== 'none') {
 setVpsLoggingValue('PBRun', '8'); originalInput.value = '8';
 if (edit === 'changed-back') { setVpsLoggingValue('PBRun', '4'); originalInput.value = '4'; }
}
handleMessage({type:'state', data:{config:{vps_logging:{services:[
 {service:'PBRun', max_mb:16, default_max_mb:1},
 {service:'PBRemote', max_mb:32, default_max_mb:2}
]}}}});
const expected = edit === 'changed' ? '8' : '4';
assert.equal(getVpsLoggingConfig().services[0].max_mb, expected);
assert.equal(originalInput.value, expected); assert.equal(inputs[1].value, '32');
assert.equal(builds, initialBuilds); assert.equal(document.activeElement, originalInput);
assert.equal(messages[0].data.services[0].max_mb, 4);
assert.equal(pending.services[0].max_mb, 4);
assert.deepEqual(store.selectedOverviewHosts, ['test-host']);
reply(pending.requestId);
assert.equal(builds, initialBuilds); assert.equal(document.activeElement, originalInput);
if (edit === 'none') {
 assert.equal(store.vpsLoggingDrafts.PBRun, undefined);
 assert.equal(store.state.config.vps_logging.services[0].max_mb, 4);
} else {
 assert.equal(store.vpsLoggingDrafts.PBRun.value, expected);
 assert.equal(store.state.config.vps_logging.services[0].max_mb, 16);
}
store.view='overview'; renderUi(); store.view='settings-vps-logging'; renderUi();
assert.equal(inputs[0].value, expected); assert.equal(button.disabled, false);
''')
