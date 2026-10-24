"""Behavioral regressions for Services Monitor status and worker requests."""

import json
from pathlib import Path
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[2]
PAGE = ROOT / "frontend" / "services_monitor.html"


def _extract_braced(source: str, start: int) -> str:
    """Extract a JavaScript declaration or assignment with one outer function body."""

    brace_start = source.index("{", start)
    depth = 0
    quote: str | None = None
    escaped = False
    for index in range(brace_start, len(source)):
        char = source[index]
        if quote is not None:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
            continue
        if char in ("'", '"', "`"):
            quote = char
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                end = index + 1
                while end < len(source) and source[end] in "; \t":
                    end += 1
                return source[start:end]
    raise AssertionError("Could not extract JavaScript function body")


def _extract_function(source: str, name: str) -> str:
    """Extract a named JavaScript function from the Services Monitor page."""

    start = source.index(f"function {name}(")
    return _extract_braced(source, start)


def _extract_window_function(source: str, name: str) -> str:
    """Extract a function assigned directly to the window object."""

    start = source.index(f"window.{name} = function")
    return _extract_braced(source, start)


def _run_node(script: str) -> None:
    """Run isolated JavaScript assertions when Node is available."""

    node = shutil.which("node")
    if node is None:
        pytest.skip("Node is required for the isolated frontend request test")
    result = subprocess.run(
        [node, "-e", script],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_worker_status_poll_discards_out_of_order_response() -> None:
    """A delayed older worker snapshot must not overwrite the newest snapshot."""

    source = PAGE.read_text(encoding="utf-8")
    code = "\n".join(
        (
            _extract_function(source, "workerResponseJson"),
            _extract_window_function(source, "fetchWorkers"),
            _extract_function(source, "scheduleWorkers"),
            _extract_function(source, "rebuildWorkerIndex"),
        )
    )
    script = """
const assert = require('node:assert/strict');
const window = {};
const API_BASE = '/api/services';
var _workersGeneration = 0;
var _workerActionPending = Object.create(null);
var _workersTimer = null;
var _currentPanelId = 'workers';
var _workers = {counts:{total:0,running:0},groups:[]};
var _workerIndex = {};
const requests = [];
const rendered = [];
function fetch() { return new Promise((resolve, reject) => requests.push({resolve, reject})); }
function authOptions(options) { return options || {}; }
function clearTimeout() {}
function setTimeout() { return 1; }
function updateWorkersSummary() {}
function renderOverviewCards() {}
function renderWorkers() { rendered.push(_workers.counts.running); }
const document = {getElementById: () => null, createElement: () => ({})};
""" + code + """
function response(data) { return {ok:true,status:200,json:() => Promise.resolve(data)}; }
(async () => {
  const older = window.fetchWorkers(false);
  const newer = window.fetchWorkers(false);
  requests[1].resolve(response({counts:{total:1,running:1},groups:[]}));
  await newer;
  assert.equal(_workers.counts.running, 1);
  requests[0].resolve(response({counts:{total:1,running:0},groups:[]}));
  await older;
  assert.equal(_workers.counts.running, 1);
  assert.deepEqual(rendered, [1]);
})().catch(error => { console.error(error); process.exitCode = 1; });
"""

    _run_node(script)


def test_worker_action_suppresses_duplicate_request_while_pending() -> None:
    """Repeated clicks for one worker send only one request and disable its controls."""

    source = PAGE.read_text(encoding="utf-8")
    code = "\n".join(
        (
            _extract_function(source, "workerResponseJson"),
            _extract_function(source, "renderWorkerActionButtons"),
            _extract_function(source, "showWorkerActionError"),
            _extract_window_function(source, "workerAction"),
        )
    )
    script = """
const assert = require('node:assert/strict');
const alerts = [];
const window = {PBGuiDialogs:{alert: options => alerts.push(options)}};
const API_BASE = '/api/services';
var _workersGeneration = 0;
var _workerActionPending = Object.create(null);
var _workersTimer = null;
var _workerIndex = {'worker-1':{id:'worker-1',label:'Worker One'}};
let resolveAction;
let fetchCalls = 0;
let refreshes = 0;
function fetch() {
  fetchCalls++;
  return new Promise(resolve => { resolveAction = resolve; });
}
function authOptions(options) { return options || {}; }
function clearTimeout() {}
function renderWorkers() {}
function scheduleWorkers() {}
function _resultPopup() { throw new Error('Shared dialog should be used'); }
window.fetchWorkers = function () { refreshes++; return Promise.resolve(); };
""" + code + """
(async () => {
  const first = window.workerAction('worker-1', 'start');
  const duplicate = window.workerAction('worker-1', 'start');
  assert.equal(fetchCalls, 1);
  assert.match(renderWorkerActionButtons({id:'worker-1',running:false}, false), /disabled/);
  assert.match(renderWorkerActionButtons({id:'worker-1',running:false}, false), /Starting\\.\\.\\./);
  await duplicate;
  resolveAction({ok:true,status:200,json:() => Promise.resolve({ok:true})});
  await first;
  assert.equal(fetchCalls, 1);
  assert.equal(refreshes, 1);
  assert.equal(_workerActionPending['worker-1'], undefined);
  assert.equal(alerts.length, 0);
})().catch(error => { console.error(error); process.exitCode = 1; });
"""

    _run_node(script)


@pytest.mark.parametrize(
    ("response_script", "expected_detail"),
    (
        (
            "Promise.resolve({ok:false,status:503,json:() => Promise.resolve({})})",
            "Worker action failed. HTTP 503.",
        ),
        ("Promise.reject(new Error('network offline'))", "network offline"),
        (
            "Promise.resolve({ok:true,status:200,json:() => Promise.reject(new SyntaxError('bad json'))})",
            "Worker action failed. Invalid response from server.",
        ),
    ),
    ids=("http", "network", "parse"),
)
def test_worker_action_reports_request_errors(response_script: str, expected_detail: str) -> None:
    """HTTP, network, and response parsing failures use the shared PBGui alert."""

    source = PAGE.read_text(encoding="utf-8")
    code = "\n".join(
        (
            _extract_function(source, "workerResponseJson"),
            _extract_function(source, "showWorkerActionError"),
            _extract_window_function(source, "workerAction"),
        )
    )
    script = """
const assert = require('node:assert/strict');
const alerts = [];
const window = {PBGuiDialogs:{alert: options => alerts.push(options)}};
const API_BASE = '/api/services';
var _workersGeneration = 0;
var _workerActionPending = Object.create(null);
var _workersTimer = null;
var _workerIndex = {'worker-1':{id:'worker-1',label:'Worker One'}};
let refreshes = 0;
function fetch() { return RESPONSE_SCRIPT; }
function authOptions(options) { return options || {}; }
function clearTimeout() {}
function renderWorkers() {}
function scheduleWorkers() {}
function _resultPopup() { throw new Error('Shared dialog should be used'); }
window.fetchWorkers = function () { refreshes++; return Promise.resolve(); };
""".replace("RESPONSE_SCRIPT", response_script) + code + """
(async () => {
  await window.workerAction('worker-1', 'restart');
  assert.equal(alerts.length, 1);
  assert.equal(alerts[0].title, 'Worker action failed');
  assert.equal(alerts[0].message, 'Could not restart worker "Worker One".');
  assert.equal(alerts[0].detail, EXPECTED_DETAIL);
  assert.equal(alerts[0].level, 'error');
  assert.equal(refreshes, 1);
  assert.equal(_workerActionPending['worker-1'], undefined);
})().catch(error => { console.error(error); process.exitCode = 1; });
""".replace("EXPECTED_DETAIL", json.dumps(expected_detail))

    _run_node(script)


def test_settings_load_retries_after_http_failure_and_blocks_duplicate_requests() -> None:
    """Only a successfully applied settings response should satisfy the loaded guard."""

    source = PAGE.read_text(encoding="utf-8")
    code = "var _settingsDrafts = Object.create(null);\n" + "\n".join(
        _extract_function(source, name) for name in ('getSettingsDraft', 'releaseSettingsDraft', 'loadSettings')
    )
    script = """
const assert = require('node:assert/strict');
const API_BASE = '/api/services';
var _settingsLoaded = {};
var _settingsLoading = {};
function focusSettingsTarget() {}
const requests = [];
const applied = [];
function fetch(url) { return new Promise(resolve => requests.push({url, resolve})); }
function authOptions(options) { return options || {}; }
function applySettings(service, data) { applied.push([service, data]); }
""" + code + """
(async () => {
  const failed = loadSettings('api-server');
  const duplicate = loadSettings('api-server');
  assert.equal(requests.length, 1);
  await duplicate;
  requests[0].resolve({ok:false,status:502,json:() => Promise.resolve({})});
  await failed;
  assert.equal(_settingsLoaded['api-server'], undefined);
  assert.equal(_settingsLoading['api-server'], undefined);
  assert.deepEqual(applied, []);

  const retry = loadSettings('api-server');
  assert.equal(requests.length, 2);
  requests[1].resolve({ok:true,status:200,json:() => Promise.resolve({host:'127.0.0.1'})});
  await retry;
  assert.equal(_settingsLoaded['api-server'], true);
  assert.equal(_settingsLoading['api-server'], undefined);
  assert.deepEqual(applied, [['api-server', {host:'127.0.0.1'}]]);
})().catch(error => { console.error(error); process.exitCode = 1; });
"""

    _run_node(script)


def test_api_restart_updates_the_clicked_compact_button() -> None:
    """Compact API restart feedback must target the button that initiated the request."""

    source = PAGE.read_text(encoding="utf-8")
    render_code = _extract_function(source, "renderServiceButtons")
    restart_code = _extract_window_function(source, "restartApiServer")
    script = """
const assert = require('node:assert/strict');
const window = {};
const API_BASE = '/api/services';
var _serviceActionPending = {};
var _statusRequestState = 'ready';
let resolveRestart;
function serviceSkipped() { return false; }
function serviceActionProgressText() { return 'Restarting...'; }
function authOptions(options) { return options || {}; }
function fetch() { return new Promise(resolve => { resolveRestart = resolve; }); }
function _resultPopup() { throw new Error('restart should not fail'); }
""" + render_code + "\n" + restart_code + r"""
(async () => {
  const html = renderServiceButtons({id:'api-server',isApiServer:true}, {running:true}, true);
  assert.match(html, /restartApiServer\(this\);event\.stopPropagation\(\)/);

  const button = {disabled:false,textContent:'Restart',innerHTML:''};
  const restart = window.restartApiServer(button);
  assert.equal(button.disabled, true);
  assert.equal(button.textContent, 'Restarting…');
  resolveRestart({ok:true,status:200,json:() => Promise.resolve({})});
  await restart;
  assert.equal(button.disabled, false);
  assert.equal(button.innerHTML, '&#8635; Restart');
})().catch(error => { console.error(error); process.exitCode = 1; });
"""

    _run_node(script)


def test_escape_closes_visible_services_dialog_without_backdrop_dismissal() -> None:
    """Escape closes the active Services dialog through its existing close action."""
    source = PAGE.read_text(encoding="utf-8")
    start = source.index("  document.addEventListener('keydown', function(e) {\n    if (e.key !== 'Escape'")
    end = source.index("\n  });", start) + len("\n  });")
    handler = source[start:end]
    script = """
const assert = require('node:assert/strict');
const events = {};
const modal = hidden => ({hidden});
const key = modal(false), authority = modal(true);
const result = {removed:false,remove(){this.removed=true;}};
const classes = values => ({contains:value=>values.has(value),remove:value=>values.delete(value)});
const prices = {classList:classes(new Set(['active']))};
const helpOvl = {classList:classes(new Set(['visible']))};
const elements = {'cmc-key-modal':key,'cmc-authority-modal':authority,
  'result-modal':result,'prices-overlay':prices};
const document = {getElementById:id=>elements[id],addEventListener:(key,fn)=>events[key]=fn};
const closed = [];
const window = {closeCmcKeyModal:()=>{closed.push('key');key.hidden=true;},
 closeCmcAuthorityModal:()=>{closed.push('authority');authority.hidden=true;},
 closePricesOverlay:()=>{closed.push('prices');prices.classList.remove('active');}};
""" + handler + """
function press(key='Escape',defaultPrevented=false){
 const event={key,defaultPrevented,prevented:false,preventDefault(){this.prevented=true;}};
 events.keydown(event); return event;
}
assert.equal(press('Enter').prevented,false);
assert.equal(press('Escape',true).prevented,false);
assert.equal(press().prevented,true); assert.deepEqual(closed,['key']);
authority.hidden=false; assert.equal(press().prevented,true); assert.deepEqual(closed,['key','authority']);
assert.equal(press().prevented,true); assert.equal(result.removed,true);
delete elements['result-modal'];
assert.equal(press().prevented,true); assert.deepEqual(closed,['key','authority','prices']);
assert.equal(press().prevented,true); assert.equal(helpOvl.classList.contains('visible'),false);
assert.equal(press().prevented,false);
"""
    _run_node(script)


def test_overview_renders_while_status_request_is_pending() -> None:
    """Show cards before the service request settles without guessing a status."""

    source = PAGE.read_text(encoding="utf-8")
    code = _extract_function(source, "renderOverviewCards")
    script = """
const assert = require('node:assert/strict');
const grid = {innerHTML: ''};
const document = {getElementById: (id) => id === 'overview-grid' ? grid : null};
const SERVICES = [{id: 'pbrun', label: 'PBRun'}];
const _status = {};
const _workers = {counts: {total: 0, running: 0}};
const _migrationStatus = null;
let _statusRequestState = 'loading';
const esc = (value) => String(value);
const migrationStatusMeta = () => ({cls: '', text: 'Not loaded'});
const serviceStatusClass = () => 'running';
const serviceStatusText = () => 'Running';
const serviceStatusTitle = () => '';
const renderServiceButtons = () => '<button>Stop</button>';
""" + code + """
renderOverviewCards();
assert.match(grid.innerHTML, /Loading status/);
assert.doesNotMatch(grid.innerHTML, /<button>Stop<\\/button>/);
_statusRequestState = 'ready';
renderOverviewCards();
assert.match(grid.innerHTML, /Running/);
assert.match(grid.innerHTML, /<button>Stop<\\/button>/);
"""
    _run_node(script)


def _service_status_harness() -> str:
    """Run the actual status renderers and poller against isolated DOM and timers."""
    source = PAGE.read_text(encoding="utf-8")
    registry = source[source.index("  var SERVICES = ["):source.index("\n  ];", source.index("  var SERVICES = [")) + 5]
    functions = (
        "authOptions", "fetchStatus", "scheduleStatus", "serviceSkipped",
        "serviceStatusClass", "serviceStatusText", "serviceStatusTitle",
        "serviceActionProgressText", "renderServiceButtons", "updateStatusUI",
        "workerResponseJson", "renderOverviewCards", "updateWorkersSummary",
        "migrationStatusMeta", "updateMigrationSummary",
    )
    visibility_start = source.index("  document.addEventListener('visibilitychange'")
    visibility_end = source.index("\n  });", visibility_start) + 6
    return r"""
const assert = require('node:assert/strict');
const API_BASE = '/api/services';
const window = {fetchWorkers: () => { workerFetches++; }};
const esc = value => String(value);
const elements = {};
const listeners = {};
const document = {
  hidden: false,
  getElementById: id => elements[id] || null,
  addEventListener: (name, callback) => { listeners[name] = callback; },
};
let _status = {}, _statusRequestState = 'loading';
let _statusInFlight = false, _statusRefreshQueued = false, _statusTimer = null;
let _workersTimer = null, workerFetches = 0;
let _serviceActionPending = {};
const _workerActionPending = {};
const _workers = {counts: {running: 6, total: 7}};
const _migrationStatus = {};
const requests = [], timers = new Map();
let timerId = 0;
function setTimeout(callback, delay) { timers.set(++timerId, {callback, delay}); return timerId; }
function clearTimeout(id) { timers.delete(id); }
function fetch(url, options) {
  return new Promise((resolve, reject) => requests.push({url, options, resolve, reject}));
}
function response(data, ok = true) { return {ok, status: ok ? 200 : 503, json: () => Promise.resolve(JSON.parse(JSON.stringify(data)))}; }
async function settle() { for (let i = 0; i < 30; i++) await Promise.resolve(); }
function runPollTimer() {
  const timer = timers.get(_statusTimer);
  assert.ok(timer, 'Status poll must remain scheduled');
  assert.equal(timer.delay, 5000);
  timers.delete(_statusTimer);
  timer.callback();
}
""" + registry + "\n" + "\n".join(_extract_function(source, name) for name in functions) + "\n" + source[visibility_start:visibility_end] + r"""
SERVICES.forEach(svc => {
  for (const prefix of ['dot-', 'cs-', 'sl-', 'ctrl-btns-']) {
    elements[prefix + svc.id] = {className: 'running', textContent: '', innerHTML: '<button>Old action</button>'};
  }
});
for (const id of ['workers', 'migration']) {
  for (const prefix of ['dot-', 'cs-', 'sl-']) elements[prefix + id] = {};
}
elements['overview-grid'] = {innerHTML: ''};
const healthy = Object.fromEntries(SERVICES.map(svc => [svc.id, {running: svc.isApiServer, can_enable: true, enabled: true}]));
function assertUnknown(state) {
  assert.equal(_statusRequestState, state);
  const label = state === 'loading' ? 'Loading status…' : 'Status unavailable';
  for (const svc of SERVICES) {
    assert.equal(elements['dot-' + svc.id].className.trim(), 'sb-dot');
    assert.equal(elements['cs-' + svc.id].className.trim(), 'status-dot');
    assert.equal(elements['sl-' + svc.id].textContent, label);
    assert.equal(elements['sl-' + svc.id].title, label);
    assert.equal(elements['ctrl-btns-' + svc.id].innerHTML, '');
    assert.equal(renderServiceButtons(svc, healthy[svc.id], true), '');
  }
  assert.equal(elements['overview-grid'].innerHTML.split(label).length - 1, 14);
  assert.doesNotMatch(elements['overview-grid'].innerHTML, /class="card-btn (start|stop|restart|enable|disable)"/);
  assert.equal(elements['sl-workers'].textContent, '6 / 7 running');
  assert.equal(elements['sl-migration'].textContent, 'Ready');
}
function assertRecovered() {
  assert.equal(_statusRequestState, 'ready');
  assert.equal(elements['sl-api-server'].textContent, 'Running · Enabled');
  assert.match(elements['ctrl-btns-api-server'].innerHTML, /Restart/);
  assert.match(elements['overview-grid'].innerHTML, /class="card-btn restart"/);
  assert.equal(elements['sl-pbrun'].textContent, 'Stopped · Enabled');
}
"""


def test_coindata_readiness_status_and_controls() -> None:
    """Background readiness overrides Skipped and keeps labels, classes and controls consistent."""
    source = PAGE.read_text(encoding="utf-8")
    code = '\n'.join(_extract_function(source, name) for name in (
        'serviceSkipped', 'serviceStatusClass', 'serviceStatusText',
        'serviceActionProgressText', 'renderServiceButtons'))
    _run_node("const assert = require('node:assert/strict');\n"
              "let _statusRequestState='ready', _serviceActionPending={};\n" + code + r"""
const svc = {id:'pbcoindata'};
for (const expected of [true,false]) {
  const starting = {actual_status:'starting', running:false, expected, can_enable:true};
  assert.equal(serviceStatusClass(starting), 'starting');
  assert.match(serviceStatusText(starting, svc.id), /^Starting/);
  const buttons = renderServiceButtons(svc, starting, false);
  for (const label of ['Start','Restart','Enable']) assert.ok(buttons.includes('disabled>'+label+'</button>'));
  assert.ok(!buttons.includes('>Stop</button>'));
  starting.can_stop = true;
  assert.ok(renderServiceButtons(svc, starting, true).includes('>Stop</button>'));
}
for (const [actual, text, cls] of [['unknown','Unknown',''],['failed','Failed','stopped'],['running','Running','running'],['stopping','Stopping','starting']]) {
  const item = {actual_status:actual, running:actual==='running', expected:false};
  assert.equal(serviceStatusText(item, svc.id), text);
  assert.equal(serviceStatusClass(item), cls);
  if (actual === 'stopping') {
    const controls = renderServiceButtons(svc, {...item,can_enable:true}, false);
    for (const label of ['Start','Restart','Enable']) assert.ok(controls.includes('disabled>'+label+'</button>'));
    assert.ok(!controls.includes('>Stop</button>'));
  }
}
assert.equal(serviceStatusText({running:false, expected:false}, 'pbdata'), 'Skipped');
""")
    _run_node(_service_status_harness() + r"""
(async () => {
  const starting = {actual_status:'starting', running:false, expected:false, can_enable:true, can_stop:true};
  _statusRequestState = 'ready';
  _status = {...healthy, pbcoindata:starting};
  updateStatusUI();
  assert.match(elements['sl-pbcoindata'].textContent, /^Starting/);
  assert.equal(elements['dot-pbcoindata'].className, 'sb-dot starting');
  assert.ok(elements['overview-grid'].innerHTML.includes('svc-card starting'));
  assert.ok(elements['ctrl-btns-pbcoindata'].innerHTML.includes('disabled>Start'));
  document.hidden = true; listeners.visibilitychange();
  document.hidden = false; listeners.visibilitychange();
  requests.at(-1).resolve(response({...healthy, pbcoindata:starting}));
  await settle();
  assert.match(elements['sl-pbcoindata'].textContent, /^Starting/);
  assert.ok(elements['ctrl-btns-pbcoindata'].innerHTML.includes('disabled>Enable'));
  for (const actual of ['unknown','running','failed','stopping']) {
    _status = {...healthy, pbcoindata:{...starting, actual_status:actual, running:actual==='running', can_stop:false}};
    updateStatusUI();
    assert.ok(elements['sl-pbcoindata'].textContent.startsWith({unknown:'Unknown',running:'Running',failed:'Failed',stopping:'Stopping'}[actual]));
  }
})().catch(error => { console.error(error); process.exitCode=1; });
""")


@pytest.mark.parametrize("after_success", [False, True], ids=["initial", "after-success"])
@pytest.mark.parametrize("failure", ["http", "network", "json"])
def test_service_status_failure_and_automatic_recovery(after_success: bool, failure: str) -> None:
    """Failures neutralize every service surface and preserve the last valid snapshot."""
    script = _service_status_harness() + "\nconst afterSuccess = " + json.dumps(after_success) + ";\nconst failure = " + json.dumps(failure) + r""";
(async () => {
  updateStatusUI(); assertUnknown('loading');
  if (afterSuccess) {
    fetchStatus(); requests.at(-1).resolve(response(healthy)); await settle(); assertRecovered();
  }
  const previous = _status;
  const snapshot = JSON.stringify(_status);
  fetchStatus();
  const request = requests.at(-1);
  if (failure === 'http') request.resolve(response({detail: 'Unavailable'}, false));
  if (failure === 'network') request.reject(new Error('Network failure'));
  if (failure === 'json') request.resolve({ok: true, status: 200, json: () => Promise.reject(new SyntaxError('Invalid JSON'))});
  await settle();
  assertUnknown('unavailable');
  assert.strictEqual(_status, previous); assert.equal(JSON.stringify(_status), snapshot);
  assert.equal(_statusInFlight, false);
  runPollTimer(); requests.at(-1).resolve(response(healthy)); await settle();
  assertRecovered(); assert.ok(timers.has(_statusTimer));
})().catch(error => { console.error(error); process.exitCode = 1; });
"""
    _run_node(script)


def test_service_status_queued_poll_and_visibility_resume() -> None:
    """Queued polls survive failure, hidden tabs pause, and returning resumes once."""
    _run_node(_service_status_harness() + r"""
(async () => {
  fetchStatus(); fetchStatus(); fetchStatus();
  assert.equal(requests.length, 1); assert.equal(_statusRefreshQueued, true);
  requests[0].resolve(response({detail: 'Unavailable'}, false)); await settle();
  assertUnknown('unavailable'); assert.equal(requests.length, 2);
  assert.equal(_statusRefreshQueued, false); assert.equal(_statusInFlight, true);
  requests[1].resolve(response(healthy)); await settle(); assertRecovered();
  document.hidden = true; listeners.visibilitychange();
  assert.equal(_statusTimer, null); assert.equal(timers.size, 0);
  fetchStatus(); requests[2].reject(new Error('Network failure')); await settle();
  assertUnknown('unavailable'); assert.equal(timers.size, 0);
  document.hidden = false; listeners.visibilitychange(); listeners.visibilitychange();
  assert.equal(requests.length, 4); assert.equal(workerFetches, 2);
  requests[3].resolve(response(healthy)); await settle(); assertRecovered();
  runPollTimer(); assert.equal(requests.length, 5);
  requests[4].resolve(response(healthy)); await settle(); assertRecovered();
})().catch(error => { console.error(error); process.exitCode = 1; });
""")


def test_service_action_response_cannot_restore_unavailable_status() -> None:
    """A delayed action response and its finally renderer cannot revive stale controls."""
    source = PAGE.read_text(encoding="utf-8")
    _run_node(_service_status_harness() + "\n" + _extract_window_function(source, "svcAction") + r"""
function _resultPopup() { throw new Error('Unexpected action failure'); }
(async () => {
  fetchStatus(); requests[0].resolve(response(healthy)); await settle(); assertRecovered();
  const action = window.svcAction('pbrun', 'start');
  assert.equal(requests[1].options.method, 'POST');
  fetchStatus(); requests[2].resolve(response({detail: 'Unavailable'}, false)); await settle();
  assertUnknown('unavailable');
  // Check every render from the action response, including the finally branch.
  const originalUpdate = updateStatusUI;
  let actionRenders = 0;
  updateStatusUI = function () { originalUpdate(); assertUnknown('unavailable'); actionRenders++; };
  requests[1].resolve(response({running: true})); await action;
  assert.equal(actionRenders, 2);
  assert.equal(_status.pbrun.running, true); assert.deepEqual(_serviceActionPending, {});
  assert.equal(requests.length, 4); assertUnknown('unavailable');
  updateStatusUI = originalUpdate;
  requests[3].resolve(response(healthy)); await settle(); assertRecovered();
})().catch(error => { console.error(error); process.exitCode = 1; });
""")


@pytest.mark.parametrize(('kind', 'key', 'field'), [
    ('system', 'memory', 'mem_error_server'), ('system', 'swap', 'swap_error_server'),
    ('system', 'disk', 'disk_error_server'), ('system', 'cpu', 'cpu_error_server'),
    ('instance', 'memory', 'mem_error_v7'), ('instance', 'swap', 'swap_error_v7'),
    ('instance', 'cpu', 'cpu_error_v7'), ('instance', 'errors', 'error_error_v7'),
    ('instance', 'tracebacks', 'traceback_error_v7'),
])
def test_alert_threshold_links_use_kind_specific_allowlist(kind, key, field):
    """Only known metrics link to the correct kind's field, retaining the mount prefix."""
    source = (ROOT / 'frontend/pbgui_nav.js').read_text()
    code = _extract_function(source, 'alertThresholdLinks')
    _run_node("""
const assert = require('node:assert/strict');
function _getAppBase() { return '/mounted'; }
function escAttr(text) { return text.replace(/&/g, '&amp;').replace(/"/g, '&quot;'); }
""" + code + "\nconst item = " + json.dumps({'kind': kind, 'triggered_thresholds': [key]}) + ";\n" + """
const html = alertThresholdLinks(item);
assert.ok(html.includes('/mounted/api/services/main_page#api-server/settings/FIELD'));
assert.equal((html.match(/<a /g) || []).length, 1);
assert.ok(!alertThresholdLinks({kind:'offline'}));
assert.ok(!alertThresholdLinks({kind:'__proto__'}));
assert.ok(!alertThresholdLinks({kind:'instance',triggered_thresholds:['<img onerror=x>', '__proto__']}).includes('<img'));
assert.ok(alertThresholdLinks({kind:'instance'}).includes('Threshold settings'));
""".replace('FIELD', field))


def test_settings_request_timeout_abort_stale_response_and_edits():
    """Bound loading and discard cancelled responses while retaining input made during loading."""
    source = PAGE.read_text()
    code = "var _settingsDrafts = Object.create(null);\n" + "\n".join(
        _extract_function(source, name) for name in ('getSettingsDraft', 'releaseSettingsDraft', 'loadSettings')
    )
    _run_node("""
const assert = require('node:assert/strict');
const API_BASE = '/mounted/api/services';
var _settingsLoaded = {}, _settingsLoading = {}, _settingsFieldTarget = {};
const requests = [], applied = [], timers = new Map(), listeners = {};
let timerId = 0;
function setTimeout(fn, ms) { assert.equal(ms,15000); timers.set(++timerId,fn); return timerId; }
function clearTimeout(id) { timers.delete(id); }
const field = {id:'apiserver-host', value:'draft.local', checked:false};
const save = {disabled:false};
const panel = {querySelector: (sel) => sel === '.form-btn.save' ? save : null,
  addEventListener:(name,fn)=>listeners[name]=fn, removeEventListener:(name)=>delete listeners[name]};
const document = {getElementById:id=>id==='apiserver-host'?field:panel};
function authOptions(options) {return options;}
function focusSettingsTarget() {}
function applySettings(svc,data) { applied.push(data.host); field.value = data.host; }
function fetch(url,options) {return new Promise(resolve=>requests.push({url,options,resolve}));}
const console = {error:()=>{}};
""" + code + """
function response(host) {return {ok:true,json:()=>Promise.resolve({host})};}
(async()=>{
  let pending = loadSettings('api-server');
  assert.equal(save.disabled,true);
  assert.equal(requests[0].url, '/mounted/api/services/settings/api-server');
  timers.values().next().value();
  await pending;
  assert.equal(requests[0].options.signal.aborted,true);
  assert.equal(_settingsLoaded['api-server'],undefined);
  assert.equal(timers.size,0);
  assert.equal(save.disabled,true);
  pending = loadSettings('api-server');
  requests[0].resolve(response('obsolete.local'));
  await Promise.resolve(); await Promise.resolve();
  assert.deepEqual(applied,[]);
  listeners.input({target:field});
  requests[1].resolve(response('server.local'));
  await pending;
  assert.deepEqual(applied,['server.local']);
  assert.equal(field.value,'draft.local');
  assert.equal(_settingsFieldTarget,null);
  assert.equal(save.disabled,false);
  assert.equal(timers.size,0);
  delete _settingsLoaded['api-server'];
  pending=loadSettings('api-server');
  const old=_settingsLoading['api-server'];
  delete _settingsLoading['api-server'];
  old.cancel();
  await pending;
  assert.equal(requests[2].options.signal.aborted,true);
  requests[2].resolve(response('after-unload.local'));
  await Promise.resolve(); await Promise.resolve();
  assert.deepEqual(applied,['server.local']);
  assert.equal(timers.size,0);
})().catch(error=>{process.stderr.write(error.stack);process.exitCode=1;});
""")


@pytest.mark.parametrize('missing', [False, True])
def test_settings_same_page_hash_navigation_and_missing_field(missing):
    """Hash changes and reload use a generation guarded field target and safe fallback."""
    source = PAGE.read_text()
    code = '\n'.join(_extract_function(source, name) for name in
                     ('settingsTargetNotice', 'focusSettingsTarget', 'restoreFromHash'))
    _run_node("""
const assert=require('node:assert/strict');
var _settingsLoaded={}, _settingsFieldTarget=null, _settingsNavigationGeneration=0;
var MONITOR_THRESHOLD_FIELDS=['mem_error_v7','cpu_error_v7'];
const notices=[], focused=[];
let currentHash='#api-server/settings/mem_error_v7';
const notice={textContent:''};
const btn={getAttribute:()=> 'settings'};
const panel={querySelectorAll:()=>[btn],querySelector:()=>notice};
const document={getElementById:id=>id==='panel-api-server'||id==='api-server-tab-settings'?panel:
  (MISSING?null:{scrollIntoView:()=>{},focus:()=>focused.push(id)}), createElement:()=>({})};
const window={location:{pathname:'/mounted/api/services/main_page',search:'',get hash(){return currentHash;}},
 switchTab:()=>{++_settingsNavigationGeneration;_settingsFieldTarget=null;currentHash='#api-server/settings';},
 addEventListener:(name,fn)=>{assert.equal(name,'hashchange');window.hashListener=fn;}};
const history={replaceState:(_,__,url)=>{currentHash='#'+url.split('#')[1];}};
function selectPanel(){++_settingsNavigationGeneration;_settingsFieldTarget=null;}
""".replace('MISSING', json.dumps(missing)) + code + "\n" + """
window.addEventListener('hashchange',restoreFromHash);
restoreFromHash();
assert.equal(currentHash,'#api-server/settings/mem_error_v7');
assert.deepEqual(focused,[]);
// A newer destination replaces the pending target before loading finishes.
currentHash='#api-server/settings/cpu_error_v7';
window.hashListener();
_settingsLoaded['api-server']=true;
focusSettingsTarget();
if (MISSING) {
  assert.equal(notice.textContent,'This threshold setting is unavailable.');
  assert.equal(currentHash,'#api-server/settings');
} else {
  assert.deepEqual(focused,['mc-cpu_error_v7']);
  assert.equal(currentHash,'#api-server/settings/cpu_error_v7');
}
currentHash='#api-server/settings/<invalid>';
window.hashListener();
assert.equal(currentHash,'#api-server/settings');
assert.equal(notice.textContent,'This threshold setting is unavailable.');
// A load after leaving the original context must never take focus.
_settingsLoaded['api-server']=false;
currentHash='#api-server/settings/mem_error_v7';restoreFromHash();
++_settingsNavigationGeneration;
_settingsLoaded['api-server']=true;
const before=focused.length;focusSettingsTarget();assert.equal(focused.length,before);
""".replace('MISSING', json.dumps(missing)))
    assert "window.addEventListener('hashchange', restoreFromHash)" in source


@pytest.mark.parametrize('failure', ['timeout', 'http', 'network', 'json'])
def test_settings_draft_survives_failed_load_retry_and_unload(failure):
    """Form-owned drafts survive failed attempts, keep newer edits and are released on success or exit."""
    source = PAGE.read_text()
    functions = '\n'.join(_extract_function(source, name) for name in
                          ('getSettingsDraft', 'releaseSettingsDraft', 'loadSettings'))
    start = source.index("  window.addEventListener('pagehide', function (event) {")
    unload = source[start:source.index('\n  });', start) + 6]
    _run_node("""
const assert = require('node:assert/strict');
const API_BASE = '/api/services';
var _settingsLoaded = {}, _settingsLoading = {}, _settingsDrafts = Object.create(null);
var _settingsFieldTarget = {}, _settingsNavigationGeneration = 0;
const requests = [], timers = new Map(), listeners = new Map();
const host = {id:'apiserver-host', value:'', checked:undefined};
const checkbox = {id:'apiserver-auto-restart', value:'on', checked:false};
const save = {disabled:false};
let timerId = 0, applied = 0, pagehide;
function setTimeout(fn) { timers.set(++timerId,fn); return timerId; }
function clearTimeout(id) { timers.delete(id); }
const panel = {
  querySelector:sel => sel === '.form-btn.save' ? save : null,
  addEventListener:(name, fn)=>{assert.equal(listeners.has(name),false);listeners.set(name,fn);},
  removeEventListener:(name, fn)=>{assert.equal(listeners.get(name),fn);listeners.delete(name);}
};
const document = {getElementById:id=>id===host.id?host:id===checkbox.id?checkbox:panel};
const window = {addEventListener:(name,fn)=>{assert.equal(name,'pagehide');pagehide=fn;}};
function focusSettingsTarget() {}
function authOptions(options) {return options;}
function fetch(url,options) {return new Promise((resolve,reject)=>requests.push({url,options,resolve,reject}));}
function applySettings(svc,data) {applied++;host.value=data.host;checkbox.checked=data.auto_restart;}
const console = {error:()=>{}};
""" + functions + '\n' + unload + '\nconst failure = ' + json.dumps(failure) + ";\n" + """
function response(host) {return {ok:true,json:()=>Promise.resolve({host,auto_restart:true})};}
(async()=>{
  let pending = loadSettings('api-server');
  host.value='before-failure.local';listeners.get('input')({target:host});
  listeners.get('change')({target:checkbox});
  const draft=_settingsDrafts['api-server'];
  if(failure==='timeout') timers.values().next().value();
  if(failure==='http') requests[0].resolve({ok:false});
  if(failure==='network') requests[0].reject(new Error('Offline'));
  if(failure==='json') requests[0].resolve({ok:true,json:()=>Promise.reject(new SyntaxError('Invalid JSON'))});
  await pending;
  assert.equal(applied,0);assert.equal(save.disabled,true);
  assert.equal(listeners.size,2);assert.equal(timers.size,0);
  assert.strictEqual(_settingsDrafts['api-server'],draft);
  // Edits made between the failed attempt and Retry belong to the same draft.
  host.value='after-failure.local';listeners.get('input')({target:host});
  pending=loadSettings('api-server');
  assert.strictEqual(_settingsDrafts['api-server'],draft);
  if(failure==='timeout') requests[0].resolve(response('obsolete.local'));
  await Promise.resolve();await Promise.resolve();
  assert.equal(applied,0);
  requests[1].resolve(response('server.local'));await pending;
  assert.equal(host.value,'after-failure.local');assert.equal(checkbox.checked,false);
  assert.equal(save.disabled,false);assert.equal(applied,1);
  assert.equal(listeners.size,0);assert.equal(draft.edits.size,0);
  assert.equal(_settingsDrafts['api-server'],undefined);assert.equal(timers.size,0);
  delete _settingsLoaded['api-server'];
  pending=loadSettings('api-server');
  host.value='unloaded.local';listeners.get('input')({target:host});
  const abandoned=_settingsDrafts['api-server'];
  pagehide();await pending;
  assert.equal(requests[2].options.signal.aborted,true);
  assert.equal(listeners.size,0);assert.equal(abandoned.edits.size,0);
  assert.equal(_settingsDrafts['api-server'],undefined);assert.equal(timers.size,0);
  requests[2].resolve(response('too-late.local'));
  await Promise.resolve();await Promise.resolve();
  assert.equal(applied,1);assert.equal(host.value,'unloaded.local');
})().catch(error=>{process.stderr.write(error.stack);process.exitCode=1;});
""")


@pytest.mark.parametrize(('state', 'edited'), [
    ('pending', True), ('pending', False), ('failed', True), ('loaded', True), ('inactive', True),
])
def test_settings_cached_return_preserves_draft_and_navigation(state, edited):
    """Cached pages retain drafts and targets, resume only active Settings and reject obsolete jobs."""
    source = PAGE.read_text()
    functions = '\n'.join(_extract_function(source, name) for name in
                          ('getSettingsDraft', 'releaseSettingsDraft', 'loadSettings'))
    lifecycle = []
    for event in ('pagehide', 'pageshow'):
        start = source.index("  window.addEventListener('" + event + "', function (event) {")
        lifecycle.append(source[start:source.index('\n  });', start) + 6])
    _run_node("""
const assert = require('node:assert/strict');
const API_BASE='/api/services';
var _settingsLoaded={}, _settingsLoading={}, _settingsDrafts=Object.create(null);
var _settingsNavigationGeneration=7, _settingsFieldTarget={field:'mem_error_v7',generation:7};
var _currentPanelId='api-server';
const events={}, listeners=new Map(), requests=[], timers=new Map(), focused=[];
const host={id:'apiserver-host',value:'',checked:undefined};
const checkbox={id:'apiserver-auto-restart',value:'on',checked:false};
const save={disabled:false};
let timerId=0, active=true, applied=0;
const pane={
 classList:{contains:name=>name==='active'&&active},
 querySelector:sel=>sel==='.form-btn.save'?save:null,
 addEventListener:(name,fn)=>{assert.equal(listeners.has(name),false);listeners.set(name,fn);},
 removeEventListener:(name,fn)=>{assert.strictEqual(listeners.get(name),fn);listeners.delete(name);}
};
const document={getElementById:id=>id===host.id?host:id===checkbox.id?checkbox:id==='api-server-tab-settings'?pane:null};
const window={addEventListener:(name,fn)=>events[name]=fn};
function setTimeout(fn){timers.set(++timerId,fn);return timerId;}
function clearTimeout(id){timers.delete(id);}
function authOptions(options){return options;}
function fetch(url,options){return new Promise(resolve=>requests.push({url,options,resolve}));}
function applySettings(svc,data){applied++;host.value=data.host;checkbox.checked=data.auto_restart;}
function focusSettingsTarget(){
 if(_settingsFieldTarget&&_settingsFieldTarget.generation===_settingsNavigationGeneration){
   focused.push(_settingsFieldTarget.field);_settingsFieldTarget=null;
 }
}
function restoreFromHash(){throw new Error('A cached page must retain its existing navigation');}
const console={error:()=>{}};
""" + functions + '\n' + '\n'.join(lifecycle) + '\nconst state=' + json.dumps(state) + ';const edited=' + json.dumps(edited) + ";\n" + """
function response(){return {ok:true,json:()=>Promise.resolve({host:'server.local',auto_restart:true})};}
async function settle(){for(let i=0;i<20;i++)await Promise.resolve();}
(async()=>{
 const initial=loadSettings('api-server');
 if(state==='loaded'){requests[0].resolve(response());await initial;focused.length=0;}
 if(edited){
   host.value='unsaved.local';checkbox.checked=false;
   if(listeners.has('input')){
     listeners.get('input')({target:host});listeners.get('change')({target:checkbox});
   }
 }
 const draft=_settingsDrafts['api-server'];
 if(state==='failed'){timers.values().next().value();await initial;}
 if(state==='inactive')active=false;
 events.pagehide({persisted:true});
 await settle();
 assert.equal(timers.size,0);assert.equal(Object.keys(_settingsLoading).length,0);
 assert.equal(_settingsNavigationGeneration,7);
 if(draft){assert.strictEqual(_settingsDrafts['api-server'],draft);assert.equal(listeners.size,2);}
 const appliedBefore=applied;
 events.pageshow({persisted:true});
 if(state==='loaded'){
   assert.equal(requests.length,1);assert.equal(host.value,'unsaved.local');
 }else{
   if(state==='inactive'){
     assert.equal(requests.length,1);active=true;loadSettings('api-server');
   }
   assert.equal(requests.length,2);
   assert.equal(requests[0].options.signal.aborted,true);
   // A second pageshow cannot create duplicate requests or listeners.
   events.pageshow({persisted:true});assert.equal(requests.length,2);
   requests[0].resolve(response());await settle();assert.equal(applied,appliedBefore);
   requests[1].resolve(response());await settle();
   assert.equal(applied,appliedBefore+1);assert.equal(_settingsLoaded['api-server'],true);
   assert.equal(host.value,edited?'unsaved.local':'server.local');
   assert.equal(checkbox.checked,edited?false:true);
   assert.equal(save.disabled,false);assert.equal(listeners.size,0);
   assert.equal(Object.keys(_settingsDrafts).length,0);assert.equal(draft.edits.size,0);
 }
 assert.deepEqual(focused,edited?[]:['mem_error_v7']);
 // A later final exit must discard an abandoned draft instead of retaining it.
 delete _settingsLoaded['api-server'];loadSettings('api-server');
 const abandoned=_settingsDrafts['api-server'];
 events.pagehide({persisted:false});await settle();
 assert.equal(abandoned.edits.size,0);assert.equal(listeners.size,0);
 assert.equal(Object.keys(_settingsDrafts).length,0);assert.equal(timers.size,0);
})().catch(error=>{process.stderr.write(error.stack);process.exitCode=1;});
""")
