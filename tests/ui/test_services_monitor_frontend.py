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
    code = _extract_function(source, "loadSettings")
    script = """
const assert = require('node:assert/strict');
const API_BASE = '/api/services';
var _settingsLoaded = {};
var _settingsLoading = {};
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
