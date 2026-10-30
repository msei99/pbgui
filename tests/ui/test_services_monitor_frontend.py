"""Behavioral regressions for Services Monitor status and worker requests."""

from javascript_checks import run_javascript

import json
import re
from pathlib import Path

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

    match = re.search(rf"window\.{re.escape(name)}\s*=\s*(?:async\s+)?function\b", source)
    assert match is not None, f"Window function {name} not found"
    start = match.start()
    return _extract_braced(source, start)


def _run_node(script: str) -> None:
    """Run isolated JavaScript assertions when the browser is available."""

    result = run_javascript(
        ["-e", script],
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
const assert = browserAssert;
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
})().catch(error => { console.error(error); testResult.exitCode = 1; });
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
const assert = browserAssert;
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
})().catch(error => { console.error(error); testResult.exitCode = 1; });
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
const assert = browserAssert;
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
})().catch(error => { console.error(error); testResult.exitCode = 1; });
""".replace("EXPECTED_DETAIL", json.dumps(expected_detail))

    _run_node(script)


def test_settings_load_retries_after_http_failure_and_blocks_duplicate_requests() -> None:
    """Only a successfully applied settings response should satisfy the loaded guard."""

    source = PAGE.read_text(encoding="utf-8")
    code = "var _settingsDrafts = Object.create(null);\n" + "\n".join(
        _extract_function(source, name) for name in ('getSettingsDraft', 'releaseSettingsDraft', 'loadSettings')
    )
    script = """
const assert = browserAssert;
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
})().catch(error => { console.error(error); testResult.exitCode = 1; });
"""

    _run_node(script)


def test_api_restart_updates_the_clicked_compact_button() -> None:
    """Compact API restart feedback must target the button that initiated the request."""

    source = PAGE.read_text(encoding="utf-8")
    render_code = _extract_function(source, "renderServiceButtons")
    restart_code = _extract_window_function(source, "restartApiServer")
    script = """
const assert = browserAssert;
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
})().catch(error => { console.error(error); testResult.exitCode = 1; });
"""

    _run_node(script)


def test_escape_closes_visible_services_dialog_without_backdrop_dismissal() -> None:
    """Escape closes the active Services dialog through its existing close action."""
    source = PAGE.read_text(encoding="utf-8")
    start = source.index("  document.addEventListener('keydown', function(e) {\n    if (e.key !== 'Escape'")
    end = source.index("\n  });", start) + len("\n  });")
    handler = source[start:end]
    script = """
const assert = browserAssert;
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
const assert = browserAssert;
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
const assert = browserAssert;
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
    _run_node("const assert = browserAssert;\n"
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
})().catch(error => { console.error(error); testResult.exitCode=1; });
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
})().catch(error => { console.error(error); testResult.exitCode = 1; });
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
})().catch(error => { console.error(error); testResult.exitCode = 1; });
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
})().catch(error => { console.error(error); testResult.exitCode = 1; });
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
const assert = browserAssert;
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
const assert = browserAssert;
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
})().catch(error=>{testErrors.write(error.stack);testResult.exitCode=1;});
""")


@pytest.mark.parametrize('missing', [False, True])
def test_settings_same_page_hash_navigation_and_missing_field(missing):
    """Hash changes and reload use a generation guarded field target and safe fallback."""
    source = PAGE.read_text()
    code = '\n'.join(_extract_function(source, name) for name in
                     ('settingsTargetNotice', 'focusSettingsTarget', 'restoreFromHash'))
    _run_node("""
const assert=browserAssert;
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
const assert = browserAssert;
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
})().catch(error=>{testErrors.write(error.stack);testResult.exitCode=1;});
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
const assert = browserAssert;
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
})().catch(error=>{testErrors.write(error.stack);testResult.exitCode=1;});
""")


def _cmc_harness() -> str:
    """Exercise actual CMC handlers with deferred HTTP and a deterministic clock."""
    source = PAGE.read_text(encoding="utf-8")
    names = (
        "cmcFetch", "newCmcOperationId", "sameCmcMutation",
        "clearCmcMutationContext", "cmcMutationOptions", "resolvePendingCmcMutation",
        "cmcMutationFetch", "setCmcMutationBusy", "cmcNumber", "cmcDuration",
        "selectedCmcKey", "updateCmcButtons", "cmcMessage", "renderCmcPool",
        "cmcTimestamp", "cmcViewVisible", "scheduleCmcRefresh", "stopCmcRefresh", "loadCmcPool",
        "queueCmcSnapshot", "resumeCmcSnapshot", "startCmcPoolRead",
    )
    state = source[source.index("  var _cmcPool ="):source.index("  function esc(")]
    # This helper contains quote regexes, outside the braced extractor's grammar.
    escape_helper = source[source.index("  function esc("):source.index("  function authOptions(")]
    lifecycle_start = source.index("  document.addEventListener('visibilitychange'", source.index("  window.loadCmcPool ="))
    lifecycle = source[lifecycle_start:source.index("  window.selectCmcKey =", lifecycle_start)]
    return r"""
const assert = browserAssert;
const API_BASE = '/api/services';
let completed = false;
afterFixture(() => assert.equal(completed, true, 'The asynchronous scenario must finish'));
const timers = new Map(); let timerId = 0;
function setTimeout(fn, delay) { timers.set(++timerId, {fn, delay}); return timerId; }
function clearTimeout(id) { timers.delete(id); }
function fire(delay) {
  const found = [...timers].find(([id, timer]) => timer.delay === delay);
  assert.ok(found, 'Expected timer: ' + delay);
  timers.delete(found[0]); found[1].fn();
}
function timerCount(delay) { return [...timers.values()].filter(t => t.delay === delay).length; }
function deferred() { let resolve, reject; const promise = new Promise((a,b) => {resolve=a;reject=b;}); return {promise, resolve, reject}; }
const requests = []; const dialogs = []; const events = {};
const window = {
  PBGuiDialogs: {confirm() { const d = deferred(); dialogs.push(d); return d.promise; }},
  addEventListener(name, fn) { events[name] = fn; }
};
const scroll = {scrollTop:120, scrollLeft:180};
const poolScroll = {scrollTop:240, scrollLeft:0};
const nodes = new Map();
function element(id) {
  if (!nodes.has(id)) {
    let html = '';
    nodes.set(id, {textContent:'', className:'', disabled:false, hidden:true, value:'',
      get innerHTML() { return html; },
      set innerHTML(value) {html=value; if (id === 'cmc-pool-body') {scroll.scrollTop=0;scroll.scrollLeft=0;poolScroll.scrollTop=0;}}
    });
  }
  return nodes.get(id);
}
const document = {
  hidden:false,
  getElementById:element,
  querySelectorAll(selector) {
    if (selector === '[data-cmc-mutation]') return [element('cmc-key-submit')];
    return selector.includes('.cmc-pool-wrap') ? [scroll, poolScroll] : [scroll];
  },
  addEventListener(name, fn) { events[name] = fn; }
};
function authOptions(options) { return options; }
function fetch(path, options) {
  const d = deferred(); requests.push({...d, path, options}); return d.promise;
}
function reply(index, data, status=200) {
  requests[index].resolve({ok:status<400, status, json:() => Promise.resolve(data)});
}
async function drain() { for (let i=0;i<50;i++) await Promise.resolve(); }
function snapshot(keys=[{id:'a', label:'Test A'}, {id:'b', label:'Test B'}]) { return {keys, ready:true, health:'healthy'}; }
function replyPool(index, keys) { reply(index, snapshot(keys)); reply(index+1, {authority:{lease_count:0}}); }
var _currentPanelId = 'pbcoindata';
""" + state + "\n" + escape_helper + "\n" + "\n".join(_extract_function(source, name) for name in names) + "\n" + lifecycle + "\n" + "\n".join(
        _extract_window_function(source, name) for name in ("selectCmcKey", "deleteSelectedCmcKey", "toggleSelectedCmcKey")
    ) + r"""
_cmcPool = snapshot(); _selectedCmcKeyId = 'a'; _cmcHasSnapshot = true;
"""


@pytest.mark.parametrize("change_selection", [False, True])
def test_cmc_delete_is_locked_through_dialog_mutation_and_fresh_snapshot(change_selection):
    """Rapid calls send one DELETE and keep the lock through a required newer read."""
    _run_node(_cmc_harness() + "\nconst changeSelection = " + json.dumps(change_selection) + r""";
(async () => {
  const older = loadCmcPool();
  const deletion = window.deleteSelectedCmcKey();
  await window.deleteSelectedCmcKey();
  assert.equal(dialogs.length, 1);
  assert.equal(element('cmc-delete-btn').disabled, true);
  renderCmcPool();
  assert.equal(element('cmc-delete-btn').disabled, true);
  dialogs[0].resolve(true); await drain();
  assert.equal(requests.length, 3);
  assert.equal(requests[2].options.method, 'DELETE');
  await window.deleteSelectedCmcKey();
  if (changeSelection) _selectedCmcKeyId = 'b';
  reply(2, {}); await drain();
  assert.equal(_cmcDeleteInFlight, true);
  assert.equal(requests.length, 3, 'The old read must settle before a new pair starts');
  replyPool(0); await older; await drain();
  assert.equal(requests.length, 5, 'Exactly one post-mutation pair is required');
  renderCmcPool();
  assert.equal(element('cmc-delete-btn').disabled, true);
  await window.deleteSelectedCmcKey();
  assert.equal(requests.length, 5);
  replyPool(3, [{id:'b',label:'Test B'}]); await deletion;
  assert.equal(_selectedCmcKeyId, changeSelection ? 'b' : '');
  assert.equal(_cmcDeleteInFlight, false);
  assert.equal(element('cmc-delete-btn').disabled, !changeSelection);
  assert.equal(timerCount(5000), 1);
  assert.equal(timerCount(15000), 0);
  assert.equal(dialogs.length, 1);
})().then(() => {completed=true;}).catch(error => {console.error(error);testResult.exitCode=1;});
""")


@pytest.mark.parametrize("failure", ["cancel", "dialog", "network", "http", "abort"])
def test_cmc_delete_unlocks_after_cancel_or_failure(failure):
    """Cancelled dialogs and failed requests release the local lock without retries."""
    _run_node(_cmc_harness() + "\nconst failure = " + json.dumps(failure) + r""";
(async () => {
  const deletion = window.deleteSelectedCmcKey();
  if (failure === 'cancel') dialogs[0].resolve(false);
  else if (failure === 'dialog') dialogs[0].reject(new Error('dialog failed'));
  else {
    dialogs[0].resolve(true); await drain();
    if (failure === 'http') reply(0, {detail:'HTTP failure'}, 500);
    else requests[0].reject(failure === 'abort' ? new DOMException('cancelled','AbortError') : new Error('network failed'));
  }
  await deletion;
  assert.equal(_cmcDeleteInFlight, false);
  assert.equal(_cmcMutationBusy, false);
  assert.equal(_selectedCmcKeyId, 'a');
  assert.equal(element('cmc-delete-btn').disabled, false);
  assert.equal(timerCount(5000), 1);
  assert.equal(requests.length, ['cancel','dialog'].includes(failure) ? 0 : 1);
  if (['dialog','network','http'].includes(failure)) assert.ok(element('cmc-pool-message').className.includes('error'));
  const retry = window.deleteSelectedCmcKey();
  assert.equal(dialogs.length, 2);
  dialogs[1].resolve(false); await retry;
})().then(() => {completed=true;}).catch(error => {console.error(error);testResult.exitCode=1;});
""")


def test_cmc_delete_respects_shared_toggle_lock_before_and_after_confirmation():
    """A concurrent toggle blocks deletion even if it starts during confirmation."""
    _run_node(_cmc_harness() + r"""
(async () => {
  _cmcMutationBusy = true;
  await window.deleteSelectedCmcKey(); assert.equal(dialogs.length, 0);
  _cmcMutationBusy = false;
  const deletion = window.deleteSelectedCmcKey();
  window.toggleSelectedCmcKey(); window.toggleSelectedCmcKey(); await drain();
  assert.equal(requests.length, 1);
  dialogs[0].resolve(true); await deletion;
  assert.equal(requests.length, 1);
  assert.equal(element('cmc-delete-btn').disabled, true);
  reply(0, {}); await drain();
  assert.equal(requests.length, 3);
  replyPool(1); await drain();
  assert.equal(element('cmc-delete-btn').disabled, false);
  assert.equal(timerCount(5000), 1);
})().then(() => {completed=true;}).catch(error => {console.error(error);testResult.exitCode=1;});
""")


def test_cmc_refresh_coalesces_and_preserves_editing_selection_and_scroll():
    """One nonoverlapping cycle updates status without changing editing containers."""
    _run_node(_cmc_harness() + r"""
(async () => {
  element('cmc-key-modal').hidden = false;
  element('cmc-key-label').value = 'unsaved label';
  document.activeElement = element('cmc-key-label');
  const initial = loadCmcPool();
  assert.equal(loadCmcPool(), initial);
  events.pageshow();
  assert.equal(requests.length, 2);
  assert.equal(timerCount(5000), 0);
  replyPool(0); await initial;
  assert.equal(timerCount(5000), 1);
  fire(5000);
  assert.equal(requests.length, 4);
  assert.equal(timerCount(5000), 0);
  assert.ok(!element('cmc-status-text').textContent.includes('Loading'));
  loadCmcPool(); assert.equal(requests.length, 4);
  replyPool(2); await drain();
  assert.equal(_selectedCmcKeyId, 'a');
  assert.equal(scroll.scrollTop, 120); assert.equal(scroll.scrollLeft, 180);
  assert.equal(poolScroll.scrollTop, 240);
  assert.equal(document.activeElement, element('cmc-key-label'));
  assert.equal(element('cmc-key-label').value, 'unsaved label');
  assert.equal(element('cmc-key-modal').hidden, false);
  assert.equal(timerCount(5000), 1);
  // A later snapshot explains externally removed selection.
  fire(5000); replyPool(4, [{id:'b'}]); await drain();
  assert.equal(_selectedCmcKeyId, '');
  assert.equal(element('cmc-pool-message').textContent, 'The selected CMC key is no longer available.');
})().then(() => {completed=true;}).catch(error => {console.error(error);testResult.exitCode=1;});
""")


@pytest.mark.parametrize("failure", ["timeout", "partial"])
def test_cmc_read_failure_cleans_up_and_recovers(failure):
    """Timeout and partial failure release both reads and schedule an automatic retry."""
    _run_node(_cmc_harness() + "\nconst failure = " + json.dumps(failure) + r""";
(async () => {
  const first = loadCmcPool();
  if (failure === 'timeout') fire(15000);
  else reply(0, {detail:'read failed'}, 503);
  await first;
  assert.equal(requests[0].options.signal.aborted, true);
  assert.equal(requests[1].options.signal.aborted, true);
  assert.equal(timerCount(15000), 0);
  assert.equal(timerCount(5000), 1);
  assert.ok(element('cmc-pool-message').className.includes('error'));
  if (failure === 'timeout') assert.match(element('cmc-status-text').textContent, /15 seconds/);
  fire(5000); replyPool(2); await drain();
  assert.equal(_cmcPool.health, 'healthy');
  assert.ok(!element('cmc-status-bar').className.includes('error'));
  // Completion of an obsolete read must not overwrite recovered state.
  replyPool(0, []); await drain();
  assert.equal(_cmcPool.keys.length, 2);
})().then(() => {completed=true;}).catch(error => {console.error(error);testResult.exitCode=1;});
""")


@pytest.mark.parametrize("leave", ["panel", "hidden", "pagehide"])
def test_cmc_lifecycle_invalidates_reads_without_resetting_new_controllers(leave):
    """Leaving cleans up immediately; stale completion cannot disturb the resumed cycle."""
    _run_node(_cmc_harness() + "\nconst leave = " + json.dumps(leave) + r""";
(async () => {
  const old = loadCmcPool();
  if (leave === 'panel') {_currentPanelId='overview';stopCmcRefresh();}
  else if (leave === 'hidden') {document.hidden=true;events.visibilitychange();}
  else events.pagehide();
  assert.equal(timers.size, 0);
  assert.equal(requests[0].options.signal.aborted, true);
  assert.equal(requests[1].options.signal.aborted, true);
  loadCmcPool(); assert.equal(requests.length, 2);
  _currentPanelId='pbcoindata'; document.hidden=false;
  events.pageshow(); events.visibilitychange();
  assert.equal(requests.length, 4);
  const resumed = _cmcLoadController;
  await old;
  replyPool(0, []); await drain();
  assert.equal(_cmcLoadController, resumed);
  assert.equal(resumed.signal.aborted, false);
  assert.equal(timerCount(15000), 1);
  replyPool(2); await drain();
  assert.equal(_cmcPool.keys.length, 2);
  assert.equal(timerCount(5000), 1);
})().then(() => {completed=true;}).catch(error => {console.error(error);testResult.exitCode=1;});
""")


def test_cmc_hidden_delete_is_not_cancelled_or_retried_and_refreshes_on_return():
    """An in-flight mutation survives pagehide; its snapshot is deferred until return."""
    _run_node(_cmc_harness() + r"""
(async () => {
  const deletion = window.deleteSelectedCmcKey();
  dialogs[0].resolve(true); await drain();
  events.pagehide();
  assert.equal(requests[0].options.signal, undefined);
  reply(0, {}); await deletion;
  assert.equal(requests.length, 1);
  assert.equal(timers.size, 0);
  assert.equal(_cmcSnapshotRequired, true);
  events.pageshow(); events.pageshow();
  assert.equal(requests.length, 3);
  replyPool(1, [{id:'b'}]); await drain();
  assert.equal(_cmcPool.keys.length, 1);
  assert.equal(timerCount(5000), 1);
})().then(() => {completed=true;}).catch(error => {console.error(error);testResult.exitCode=1;});
""")


def test_cmc_refresh_navigation_hooks_and_removed_manual_controls():
    """Panel and Pool entry trigger updates; no manual CMC refresh control remains."""
    source = PAGE.read_text(encoding="utf-8")
    assert 'cmc-refresh-btn' not in source
    assert 'onclick="loadCmcPool()"' not in source
    assert "if (panelId === 'pbcoindata') loadCmcPool();\n    else stopCmcRefresh();" in _extract_function(source, 'selectPanel')
    assert "if (tab === 'pool' && svc === 'pbcoindata') loadCmcPool();" in _extract_window_function(source, 'switchTab')
    for name in ('submitCmcKey', 'submitCmcAuthorityTransfer', 'toggleSelectedCmcKey'):
        assert 'loadCmcPool(true)' in _extract_window_function(source, name)


def test_cmc_delete_snapshot_failure_unlocks_and_polling_recovers():
    """A failed post-delete read releases the lock and retries only reads automatically."""
    _run_node(_cmc_harness() + r"""
(async () => {
  scheduleCmcRefresh();
  assert.equal(timerCount(5000), 1);
  const deletion = window.deleteSelectedCmcKey();
  assert.equal(timerCount(5000), 0);
  await loadCmcPool(); assert.equal(requests.length, 0);
  dialogs[0].resolve(true); await drain();
  _selectedCmcKeyId = 'b';
  reply(0, {}); await drain();
  assert.equal(requests.length, 3);
  assert.equal(_cmcDeleteInFlight, true);
  reply(1, {detail:'snapshot unavailable'}, 503); await deletion;
  assert.equal(requests[2].options.signal.aborted, true);
  assert.equal(_cmcDeleteInFlight, false);
  assert.equal(_selectedCmcKeyId, 'b');
  assert.equal(element('cmc-delete-btn').disabled, false);
  assert.equal(timerCount(5000), 1);
  fire(5000); replyPool(3, [{id:'b'}]); await drain();
  assert.equal(requests.filter(r => r.options.method === 'DELETE').length, 1);
  assert.equal(_cmcPool.keys.length, 1);
  assert.equal(timerCount(5000), 1);
})().then(() => {completed=true;}).catch(error => {console.error(error);testResult.exitCode=1;});
""")


def test_cmc_repeated_fresh_requests_share_one_queued_snapshot():
    """Multiple snapshot waiters share the one follow-up pair, including its completion."""
    _run_node(_cmc_harness() + r"""
(async () => {
  const old = loadCmcPool();
  const fresh = loadCmcPool(true);
  assert.equal(loadCmcPool(true), fresh);
  assert.equal(loadCmcPool(), fresh);
  replyPool(0); await old; await drain();
  assert.equal(requests.length, 4);
  let settled = false;
  fresh.then(() => {settled=true;}); await drain();
  assert.equal(settled, false);
  assert.equal(timerCount(5000), 0);
  replyPool(2, [{id:'b'}]); await fresh;
  assert.equal(settled, true);
  assert.equal(requests.length, 4);
  assert.equal(timerCount(5000), 1);
})().then(() => {completed=true;}).catch(error => {console.error(error);testResult.exitCode=1;});
""")


@pytest.mark.parametrize("outcome", ["success", "http", "network"])
@pytest.mark.parametrize("order", ["read_first", "mutation_first"])
def test_cmc_delete_snapshot_waits_for_intervening_mutation(outcome, order):
    """A queued post-delete read survives another mutation, without early unlock or extra reads."""
    _run_node(_cmc_harness() + '\nconst outcome=' + json.dumps(outcome) + ';const order=' + json.dumps(order) + r""";
(async () => {
  const older = loadCmcPool();
  const deletion = window.deleteSelectedCmcKey();
  dialogs[0].resolve(true); await drain();
  reply(2, {}); await drain();
  _selectedCmcKeyId = 'b';
  window.toggleSelectedCmcKey(); await drain();
  assert.equal(requests.length, 4);
  function completeMutation() {
    if (outcome === 'network') requests[3].reject(new Error('isolated network failure'));
    else reply(3, outcome === 'http' ? {detail:'isolated HTTP failure'} : {}, outcome === 'http' ? 503 : 200);
  }
  if (order === 'read_first') {
    replyPool(0); await older; await drain();
    assert.equal(_cmcDeleteInFlight, true, 'An old read cannot complete the delete snapshot');
    assert.equal(element('cmc-delete-btn').disabled, true);
    assert.equal(requests.length, 4, 'No reads may start during the intervening mutation');
    assert.equal(timerCount(5000), 0);
    completeMutation(); await drain();
  } else {
    completeMutation(); await drain();
    assert.equal(requests.length, 4, 'Do not overlap the old and fresh read pairs');
    assert.equal(_cmcDeleteInFlight, true);
    replyPool(0); await older; await drain();
  }
  assert.equal(requests.length, 6, 'Exactly one fresh pair starts after both prerequisites');
  assert.equal(_cmcDeleteInFlight, true);
  renderCmcPool();
  assert.equal(element('cmc-delete-btn').disabled, true);
  await window.deleteSelectedCmcKey();
  assert.equal(dialogs.length, 1);
  assert.equal(timerCount(5000), 0);
  replyPool(4, [{id:'b',label:'Test B'}]); await deletion; await drain();
  assert.equal(_cmcDeleteInFlight, false);
  assert.equal(element('cmc-delete-btn').disabled, false);
  assert.equal(_selectedCmcKeyId, 'b');
  assert.equal(_cmcPool.keys.some(key=>key.id==='a'), false);
  assert.equal(requests.length, 6);
  assert.equal(requests.filter(r=>r.options.method==='DELETE').length, 1);
  assert.equal(requests.filter(r=>r.options.method==='POST').length, 1);
  assert.equal(timerCount(5000), 1);
  assert.equal(timerCount(15000), 0);
})().then(()=>{completed=true;}).catch(error=>{console.error(error);testResult.exitCode=1;});
""")


@pytest.mark.parametrize("phase", ["waiting", "loading"])
def test_cmc_deferred_snapshot_cleanup_preserves_resumed_owner(phase):
    """Stopping a deferred snapshot releases waiters and never revives its old owner."""
    _run_node(_cmc_harness() + '\nconst phase=' + json.dumps(phase) + r""";
(async () => {
  const older = loadCmcPool();
  const deletion = window.deleteSelectedCmcKey();
  dialogs[0].resolve(true); await drain();
  reply(2, {}); await drain();
  _selectedCmcKeyId='b'; window.toggleSelectedCmcKey(); await drain();
  replyPool(0); await older; await drain();
  if (phase === 'loading') {
    reply(3, {detail:'isolated mutation failure'}, 503); await drain();
    assert.equal(requests.length, 6);
  }
  const abandoned = _cmcQueuedLoad;
  assert.ok(abandoned);
  events.pagehide(); await deletion;
  assert.equal(_cmcQueuedLoad, null);
  assert.equal(timers.size, 0);
  assert.equal(_cmcSnapshotRequired, true);
  if (phase === 'loading') assert.equal(requests[4].options.signal.aborted, true);
  events.pageshow(); await drain();
  let freshIndex;
  if (phase === 'waiting') {
    assert.equal(requests.length, 4);
    assert.notEqual(_cmcQueuedLoad, abandoned);
    reply(3, {detail:'isolated mutation failure'}, 503); await drain();
    assert.equal(requests.length, 6);
    freshIndex=4;
  } else {
    assert.equal(requests.length, 8);
    freshIndex=6;
  }
  const controller = _cmcLoadController;
  if (phase === 'loading') replyPool(4, []);
  await drain();
  assert.equal(_cmcLoadController, controller);
  assert.equal(controller.signal.aborted, false);
  replyPool(freshIndex, [{id:'b'}]); await drain();
  assert.equal(_cmcPool.keys.length, 1);
  assert.equal(_cmcPool.keys[0].id, 'b');
  assert.equal(_cmcQueuedLoad, null);
  assert.equal(timerCount(5000), 1);
  assert.equal(timerCount(15000), 0);
  assert.equal(requests.filter(r=>r.options.method==='DELETE').length, 1);
  assert.equal(requests.filter(r=>r.options.method==='POST').length, 1);
})().then(()=>{completed=true;}).catch(error=>{console.error(error);testResult.exitCode=1;});
""")


def test_cmc_fresh_snapshot_waits_for_shared_lock_without_existing_read():
    """A snapshot requested under the shared lock remains pending and coalesces with success."""
    _run_node(_cmc_harness() + r"""
(async () => {
  window.toggleSelectedCmcKey(); await drain();
  let settled=false;
  const fresh=loadCmcPool(true);
  fresh.then(()=>{settled=true;});
  assert.equal(loadCmcPool(true), fresh);
  await drain();
  assert.equal(settled, false);
  assert.equal(requests.length, 1);
  reply(0, {}); await drain();
  assert.equal(requests.length, 3);
  assert.equal(settled, false);
  replyPool(1); await fresh; await drain();
  assert.equal(settled, true);
  assert.equal(requests.length, 3);
  assert.equal(timerCount(5000), 1);
})().then(()=>{completed=true;}).catch(error=>{console.error(error);testResult.exitCode=1;});
""")


def test_cmc_newer_mutation_keeps_snapshot_waiters_until_newer_read():
    """A later mutation cannot coalesce into a read that predates its completion."""
    _run_node(_cmc_harness() + r"""
(async () => {
  const older=loadCmcPool();
  const deletion=window.deleteSelectedCmcKey();
  dialogs[0].resolve(true); await drain();
  reply(2, {}); await drain();
  _selectedCmcKeyId='b'; window.toggleSelectedCmcKey(); await drain();
  replyPool(0); await older; await drain();
  reply(3, {}); await drain();
  assert.equal(requests.length, 6);
  _cmcPool.keys.push({id:'c',label:'Test C'});
  _selectedCmcKeyId='c'; window.toggleSelectedCmcKey(); await drain();
  assert.equal(requests.length, 7);
  reply(6, {}); await drain();
  assert.equal(requests.length, 7, 'The pending read must finish before another pair starts');
  assert.equal(_cmcDeleteInFlight, true);
  replyPool(4, [{id:'b'}, {id:'c',active:true}]); await drain();
  assert.equal(requests.length, 9, 'The later mutation requires a newer snapshot');
  assert.equal(_cmcDeleteInFlight, true);
  assert.equal(element('cmc-delete-btn').disabled, true);
  assert.equal(timerCount(5000), 0);
  replyPool(7, [{id:'b'}, {id:'c',active:false}]); await deletion; await drain();
  assert.equal(_cmcPool.keys.find(key=>key.id==='c').active, false);
  assert.equal(_selectedCmcKeyId, 'c');
  assert.equal(_cmcDeleteInFlight, false);
  assert.equal(_cmcQueuedLoad, null);
  assert.equal(requests.length, 9);
  assert.equal(requests.filter(r=>r.options.method==='DELETE').length, 1);
  assert.equal(requests.filter(r=>r.options.method==='POST').length, 2);
  assert.equal(timerCount(5000), 1);
})().then(()=>{completed=true;}).catch(error=>{console.error(error);testResult.exitCode=1;});
""")


@pytest.mark.parametrize(
    ("dismiss", "focus_case"),
    [
        ("cancel", "trigger"),
        ("escape", "trigger"),
        ("cancel", "other_input"),
        ("escape", "other_input"),
        ("cancel", "new_dialog"),
        ("cancel", "hidden_view"),
        ("cancel", "disabled_trigger"),
        ("cancel", "removed_trigger"),
        ("cancel", "different_trigger"),
    ],
)
def test_cmc_cancel_restores_focus_without_overriding_new_context(dismiss, focus_case):
    """Actual dialog dismissal restores the enabled trigger without stealing newer focus."""
    browser_api = __import__('browser_driver')
    source = PAGE.read_text(encoding="utf-8")
    state = source[source.index("  var _cmcPool ="):source.index("  function esc(")]
    functions = "\n".join(
        _extract_function(source, name)
        for name in ("selectedCmcKey", "updateCmcButtons", "cmcViewVisible", "cmcMessage")
    )
    script = state + "\n" + functions + "\n" + _extract_window_function(source, "deleteSelectedCmcKey") + """
var _currentPanelId = 'pbcoindata';
_cmcPool = {keys:[{id:'a',label:'Test A'}]};
_selectedCmcKeyId = 'a';
function scheduleCmcRefresh() {} // Polling is covered by the deterministic browser fixtures.
window.requests = [];
window.fetch = function () { window.requests.push('unexpected request'); return Promise.reject(new Error('No HTTP expected')); };
updateCmcButtons();
"""
    with browser_api.sync_browser() as driver:
        browser = driver.chromium.launch(headless=True)
        try:
            page = browser.new_page()
            page.set_default_timeout(3000)
            page.route("**/*", lambda route: route.abort())
            errors = []
            page.on("pageerror", lambda error: errors.append(str(error)))
            page.set_content("""
<button id="cmc-delete-btn" onclick="window.deletion = deleteSelectedCmcKey()">Delete</button>
<input id="other-input" value="unsaved draft">
""")
            page.add_script_tag(content=(ROOT / "frontend/js/pbgui_dialogs.js").read_text(encoding="utf-8"))
            page.add_script_tag(content=script)
            if focus_case == "different_trigger":
                page.locator("#other-input").focus()
                page.evaluate("() => { window.deletion = window.deleteSelectedCmcKey(); }")
            else:
                page.locator("#cmc-delete-btn").focus()
                page.keyboard.press("Enter")
            browser_api.expect(page.locator("#pbgui-dialog-ovl")).to_be_visible()
            browser_api.expect(page.locator("#cmc-delete-btn")).to_be_disabled()

            close = (
                "document.getElementById('pbgui-dialog-cancel').click();"
                if dismiss == "cancel"
                else "document.dispatchEvent(new KeyboardEvent('keydown', {key:'Escape',bubbles:true}));"
            )
            changes = {
                "other_input": "document.getElementById('other-input').focus();",
                "new_dialog": "window.PBGuiDialogs.confirm({title:'New decision',confirmText:'Continue'});",
                "hidden_view": "_currentPanelId = 'overview';",
                "disabled_trigger": "_cmcPool.keys = [];",
                "removed_trigger": "document.getElementById('cmc-delete-btn').remove();",
            }
            if focus_case in changes:
                # Keep close and the context change in one JS stack, before the handler resumes.
                page.evaluate("() => {" + close + changes[focus_case] + "}")
            elif dismiss == "cancel":
                page.locator("#pbgui-dialog-cancel").click()
            else:
                page.keyboard.press("Escape")
            page.evaluate("window.deletion")
            page.evaluate("() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)))")
            focus = page.evaluate("document.activeElement.id || document.activeElement.tagName")
            if focus_case == "trigger":
                assert focus == "cmc-delete-btn"
            elif focus_case in ("other_input", "different_trigger"):
                assert focus == "other-input"
            elif focus_case == "new_dialog":
                assert focus == "pbgui-dialog-accept"
                browser_api.expect(page.locator("#pbgui-dialog-ovl")).to_be_visible()
                browser_api.expect(page.locator("#pbgui-dialog-title")).to_have_text("New decision")
            else:
                assert focus == "BODY"
            if focus_case != "new_dialog":
                browser_api.expect(page.locator("#pbgui-dialog-ovl")).to_be_hidden()
            if focus_case not in ("disabled_trigger", "removed_trigger"):
                browser_api.expect(page.locator("#cmc-delete-btn")).to_be_enabled()
            assert page.locator("#other-input").input_value() == "unsaved draft"
            assert page.evaluate("window.requests") == []
            assert errors == []
        finally:
            browser.close()


@pytest.mark.parametrize("leave", ["panel", "hidden", "pagehide"])
@pytest.mark.parametrize("leave_point", ["mutation", "snapshot"])
@pytest.mark.parametrize("other_selected", [False, True])
def test_cmc_known_deleted_cache_row_stays_blocked_on_return(leave, leave_point, other_selected):
    """Successful deletion stays protected across a deferred or aborted return snapshot."""
    settings = json.dumps(dict(leave=leave, leavePoint=leave_point, otherSelected=other_selected))
    _run_node(_cmc_harness() + '\nconst settings=' + settings + r""";
(async()=>{
  function leaveView() {
    if(settings.leave==='panel'){_currentPanelId='overview';stopCmcRefresh();}
    else if(settings.leave==='hidden'){document.hidden=true;events.visibilitychange();}
    else events.pagehide();
  }
  function returnView() {
    if(settings.leave==='panel'){_currentPanelId='pbcoindata';loadCmcPool();}
    else if(settings.leave==='hidden'){document.hidden=false;events.visibilitychange();}
    else events.pageshow();
  }
  const deletion=window.deleteSelectedCmcKey();
  dialogs[0].resolve(true);await drain();
  if(settings.otherSelected) window.selectCmcKey('b');
  if(settings.leavePoint==='mutation') leaveView();
  reply(0,{});await drain();
  if(settings.leavePoint==='snapshot') {
    assert.equal(requests.length,3);
    leaveView();
  } else assert.equal(requests.length,1);
  await deletion;
  assert.equal(timers.size,0);
  const freshIndex=requests.length;
  returnView();await drain();
  assert.equal(requests.length,freshIndex+2);
  const selected=settings.otherSelected?'b':'';
  window.selectCmcKey('a');
  assert.equal(_selectedCmcKeyId,selected,'A cached deleted row must not replace a different selection');
  assert.equal(element('cmc-delete-btn').disabled,!settings.otherSelected);
  assert.match(element('cmc-pool-body').innerHTML,/data-cmc-deleted="true"/);
  // Direct handler calls must be protected too, even with an obsolete selected ID.
  _selectedCmcKeyId='a';updateCmcButtons();
  assert.equal(selectedCmcKey(),null);
  assert.equal(element('cmc-delete-btn').disabled,true);
  await window.deleteSelectedCmcKey();assert.equal(dialogs.length,1);
  _selectedCmcKeyId=selected;renderCmcPool();
  const controller=_cmcLoadController;
  if(settings.leavePoint==='snapshot') {replyPool(1);await drain();}
  assert.equal(_cmcLoadController,controller);
  assert.equal(controller.signal.aborted,false);
  window.selectCmcKey('a');assert.equal(_selectedCmcKeyId,selected);
  replyPool(freshIndex,[{id:'b',label:'Test B'}]);await drain();
  assert.equal(_selectedCmcKeyId,selected);
  assert.equal(_cmcPool.keys.some(key=>key.id==='a'),false);
  window.selectCmcKey('b');assert.equal(element('cmc-delete-btn').disabled,false);
  assert.equal(requests.filter(request=>request.options.method==='DELETE').length,1);
  assert.equal(timerCount(5000),1);assert.equal(timerCount(15000),0);
})().then(()=>{completed=true;}).catch(error=>{console.error(error);testResult.exitCode=1;});
""")


@pytest.mark.parametrize("failure", ["http", "timeout"])
@pytest.mark.parametrize("current_contains_key", [False, True])
def test_cmc_known_deleted_row_requires_successful_current_snapshot(failure, current_contains_key):
    """Old, failed and timed-out reads cannot restore actions; a successful new snapshot can."""
    settings = json.dumps(dict(failure=failure, containsKey=current_contains_key))
    _run_node(_cmc_harness() + '\nconst settings=' + settings + r""";
(async()=>{
  const older=loadCmcPool();
  const deletion=window.deleteSelectedCmcKey();
  dialogs[0].resolve(true);await drain();reply(2,{});await drain();
  replyPool(0);await older;await drain();
  assert.equal(requests.length,5);
  window.selectCmcKey('a');assert.equal(_selectedCmcKeyId,'');
  if(settings.failure==='timeout') fire(15000);
  else reply(3,{detail:'isolated read failure'},503);
  await deletion;
  window.selectCmcKey('a');assert.equal(_selectedCmcKeyId,'');
  assert.equal(element('cmc-delete-btn').disabled,true);
  await window.deleteSelectedCmcKey();assert.equal(dialogs.length,1);
  fire(5000);assert.equal(requests.length,7);
  replyPool(5,settings.containsKey?[{id:'a'},{id:'b'}]:[{id:'b'}]);await drain();
  window.selectCmcKey('a');
  assert.equal(_selectedCmcKeyId,settings.containsKey?'a':'');
  assert.equal(element('cmc-delete-btn').disabled,!settings.containsKey);
  assert.equal(requests.filter(request=>request.options.method==='DELETE').length,1);
  assert.equal(timerCount(5000),1);assert.equal(timerCount(15000),0);
})().then(()=>{completed=true;}).catch(error=>{console.error(error);testResult.exitCode=1;});
""")


def _render_cmc_services_page(prefix, monkeypatch):
    """Run the actual allowlisted page renderer with synthetic version metadata only."""
    import ast
    import sys
    from types import ModuleType
    from fastapi import Request
    from fastapi.responses import HTMLResponse
    from api.page_templates import render_page_urls, script_json

    path = ROOT / 'api/services.py'
    node = next(node for node in ast.parse(path.read_text(encoding='utf-8')).body
                if isinstance(node, ast.FunctionDef) and node.name == 'get_main_page')
    node.decorator_list = []
    node.returns = None
    for argument in node.args.args:
        argument.annotation = None
    node.args.defaults = [ast.Constant(None) for _ in node.args.defaults]
    pure = ModuleType('pbgui_purefunc')
    pure.PBGUI_VERSION = 'test'
    pure.PBGUI_SERIAL = 'test'
    monkeypatch.setitem(sys.modules, 'pbgui_purefunc', pure)
    namespace = dict(__file__=str(path), Path=Path, HTMLResponse=HTMLResponse,
                     render_page_urls=render_page_urls, script_json=script_json)
    module = ast.fix_missing_locations(ast.Module(body=[node], type_ignores=[]))
    exec(compile(module, str(path), 'exec'), namespace)
    request = Request(dict(type='http', method='GET', scheme='http', path='/api/services/main_page',
                           root_path=prefix, server=('cmc.test', 80), headers=[]))
    return namespace['get_main_page'](request).body


@pytest.mark.parametrize("prefix", ['', '/mounted'])
@pytest.mark.parametrize("leave_point", ['mutation', 'snapshot'])
@pytest.mark.parametrize("other_selected", [False, True])
def test_cmc_real_menu_return_does_not_restore_deleted_cached_action(prefix, leave_point, other_selected, monkeypatch):
    """Actual menu, renderer and rows keep a deleted cached key blocked until the return read."""
    import mimetypes
    from urllib.parse import urlparse

    browser_api = __import__('browser_driver')
    html = _render_cmc_services_page(prefix, monkeypatch)
    pool = {'ready': True, 'health': 'ok', 'keys': [
        {'id': 'qa-alpha', 'label': 'qa-alpha', 'active': True},
        {'id': 'qa-beta', 'label': 'qa-beta', 'active': True},
    ]}
    services = ('pbcluster', 'pbrun', 'pbdata', 'pbcoindata', 'monitor-agent', 'vps-monitor', 'api-server')
    deletes, reads, errors, unexpected = [], [], [], []
    hold_reads = False
    with browser_api.sync_browser() as driver:
        browser = driver.chromium.launch(headless=True)
        try:
            page = browser.new_page(viewport={'width': 1450, 'height': 1000})
            page.set_default_timeout(3000)
            page.clock.install()
            page.on('pageerror', lambda error: errors.append(str(error)))
            page.add_init_script("""
window.WebSocket=class {
  static OPEN=1; constructor(){this.readyState=1;setTimeout(()=>this.onopen?.({}),0);}
  send(){} close(){this.readyState=3;this.onclose?.({});}
};
""")

            def respond(route):
                """Serve local assets and synthetic replies; never forward any request."""
                request = route.request
                url = urlparse(request.url)
                if url.hostname != 'cmc.test' or (prefix and not url.path.startswith(prefix + '/')):
                    unexpected.append(request.url)
                    route.fulfill(status=451, body='Blocked fixture request')
                    return
                path = url.path[len(prefix):] if prefix else url.path
                if request.method == 'DELETE' and path == '/api/services/cmc-pool/keys/qa-alpha':
                    deletes.append(route)
                elif path == '/api/services/cmc-pool':
                    if hold_reads:
                        reads.append(route)
                    else:
                        route.fulfill(json=pool)
                elif path == '/api/services/cmc-pool/leases':
                    route.fulfill(json={'authority': {}, 'leases': []})
                elif path == '/app/start.html':
                    route.fulfill(content_type='text/html', body='<nav id="topnav"></nav><script>window.PBGUI_NAV_CONFIG={current:"welcome",authenticated:true,apiBase:' + json.dumps(prefix + '/api') + '};</script><script src="' + prefix + '/app/pbgui_nav.js"></script>')
                elif path == '/api/services/main_page':
                    route.fulfill(content_type='text/html', body=html)
                elif path.startswith('/app/'):
                    asset = (ROOT / 'frontend' / path.removeprefix('/app/')).resolve()
                    if asset.is_relative_to((ROOT / 'frontend').resolve()) and asset.is_file():
                        route.fulfill(body=asset.read_bytes(), content_type=mimetypes.guess_type(asset)[0] or 'application/octet-stream')
                    else:
                        unexpected.append(path)
                        route.fulfill(status=404, body='Missing local asset')
                elif path == '/api/services/status':
                    route.fulfill(json={name: {'running': True, 'enabled': True, 'can_enable': True} for name in services})
                elif path == '/api/services/workers/summary':
                    route.fulfill(json={'counts': {'total': 7, 'running': 7}})
                elif path == '/api/services/migration/status':
                    route.fulfill(json={'migration_needed': False, 'warnings': []})
                elif path.endswith('/stream'):
                    route.fulfill(status=204, body='')
                elif path in {'/api/server-status', '/api/vps/alerts', '/api/ai/preferences', '/api/token-refresh', '/api/auth/me', '/api/notify_log'}:
                    route.fulfill(json={'ok': True, 'authenticated': True, 'drawer_open': False, 'items': [],
                                        'summary': {'new_count': 0, 'ack_count': 0, 'total_active': 0}})
                else:
                    unexpected.append(path)
                    route.fulfill(status=404, json={'detail': 'Unexpected fixture request'})

            page.route('**/*', respond)

            def wait_routes(routes, count):
                """Drain local route callbacks without advancing the controlled page clock."""
                for _ in range(100):
                    if len(routes) == count:
                        return
                    page.wait_for_timeout(10)
                assert len(routes) == count

            page.goto('http://cmc.test' + prefix + '/app/start.html')
            page.locator('.nav-group-btn', has_text='System').click()
            page.locator('.nav-item[data-page="system_services"]').click()
            page.locator('.sb-btn[data-panel="pbcoindata"]').click()
            page.locator('.tab-btn[data-svc="pbcoindata"][data-tab="pool"]').click()
            alpha = page.locator('#cmc-pool-body [data-key-id="qa-alpha"]')
            browser_api.expect(alpha).to_be_visible()
            hold_reads = True
            alpha.click()
            page.locator('#cmc-delete-btn').click()
            page.locator('#pbgui-dialog-accept').click()
            wait_routes(deletes, 1)
            if other_selected:
                page.locator('#cmc-pool-body [data-key-id="qa-beta"]').click()
            if leave_point == 'mutation':
                page.locator('.sb-btn[data-panel="overview"]').click()
            deletes[0].fulfill(json={'ok': True})
            browser_api.expect(alpha.locator('.cmc-state')).to_have_text('deleted')
            if leave_point == 'snapshot':
                wait_routes(reads, 1)
                page.locator('.sb-btn[data-panel="overview"]').click()
            page.locator('.sb-btn[data-panel="pbcoindata"]').click()
            wait_routes(reads, 2 if leave_point == 'snapshot' else 1)
            browser_api.expect(alpha).to_have_attribute('data-cmc-deleted', 'true')
            alpha.click()
            if other_selected:
                browser_api.expect(page.locator('#cmc-pool-body tr.selected')).to_have_attribute('data-key-id', 'qa-beta')
                browser_api.expect(page.locator('#cmc-delete-btn')).to_be_enabled()
            else:
                browser_api.expect(page.locator('#cmc-pool-body tr.selected')).to_have_count(0)
                browser_api.expect(page.locator('#cmc-delete-btn')).to_be_disabled()
            reads[-1].fulfill(json={**pool, 'keys': [pool['keys'][1]]})
            browser_api.expect(alpha).to_have_count(0)
            page.locator('#cmc-pool-body [data-key-id="qa-beta"]').click()
            trigger = page.locator('#cmc-delete-btn')
            trigger.focus()
            trigger.press('Enter')
            page.keyboard.press('Escape')
            browser_api.expect(trigger).to_be_focused()
            assert len(deletes) == 1
            assert not errors, errors
            assert not unexpected, unexpected
        finally:
            page.unroute_all(behavior='ignoreErrors')
            browser.close()


def _settings_save_script(prefix: str = '/mounted') -> str:
    """Run the real save callers and controller with isolated DOM, transport and clock."""
    source = PAGE.read_text(encoding="utf-8")
    start = source.index('  var pendingSettingsPosts = new Set();')
    end = source.index('\n  function _val(', start)
    callers = '\n'.join(_extract_window_function(source, name) for name in (
        'saveCoinDataSettings', 'savePBDataSettings', 'saveApiServerSettings'))
    return 'const API_BASE = ' + json.dumps(prefix + '/api/services') + ';\n' + r"""
const assert = browserAssert;
const events = {}, requests = [], timers = new Map(), inline = [], reloads = [], logs = [];
let timerId = 0;
const window = {addEventListener:(name, fn)=>events[name]=fn};
var _settingsLoaded = {pbcoindata:true, pbdata:true, 'api-server':true};
var _settingsLoading = {};
class Element {
  constructor() { this.children=[]; this.attrs={}; this.listeners={}; this.hidden=false; this.disabled=false; }
  set innerHTML(value) { throw new Error('Untrusted save results must use textContent'); }
  setAttribute(key,value) { this.attrs[key]=value; }
  addEventListener(name,fn) { this.listeners[name]=fn; }
  append(...nodes) { nodes.forEach(node=>{node.parent=this;this.children.push(node);}); }
  prepend(node) { node.parent=this;this.children.unshift(node); }
  remove() { this.parent.children=this.parent.children.filter(node=>node!==this); }
  get childElementCount() { return this.children.length; }
}
const host = new Element(); host.hidden=true;
const buttons = {}, panels = {}, messages = {}, fields = {};
const ids = {pbcoindata:'coindata-save-msg',pbdata:'pbdata-save-msg','api-server':'apiserver-save-msg'};
for(const svc of Object.keys(ids)) {
  buttons[svc]=new Element();
  panels[svc+'-tab-settings']={querySelector:()=>buttons[svc],hidden:false};
  messages[ids[svc]]={previousElementSibling:buttons[svc]};
}
const document = {
  createElement:()=>new Element(),
  getElementById:id=>id==='settings-save-results'?host:(panels[id]||messages[id]||fields[id]||null),
  querySelectorAll:()=>[]
};
function _val(id) { return fields[id] ? fields[id].value : ''; }
function _flash(id,text,error) { if(messages[id]) inline.push({id,text,error}); }
function collectMonitorConfigFromForm() { return {}; }
function collectAlertRoutingFromForm() { return {}; }
function loadSettings(svc) { reloads.push(svc); }
function authOptions(options) { return options; }
function fetch(url,options) { return new Promise((resolve,reject)=>requests.push({url,options,resolve,reject})); }
function setTimeout(fn,ms) { assert.equal(ms,15000); timers.set(++timerId,fn);return timerId; }
function clearTimeout(id) { timers.delete(id); }
const console = {error:(...args)=>logs.push(args)};
function response(data={ok:true},status=200) { return {ok:status<400,status,json:()=>Promise.resolve(data)}; }
async function settle() { for(let i=0;i<20;i++) await Promise.resolve(); }
function fireTimeout() { assert.equal(timers.size,1);timers.values().next().value(); }
function textAt(index=0) { return host.children[index].children[0].textContent; }
""" + source[start:end] + '\n' + callers + '\n'


@pytest.mark.parametrize('caller,svc', [
    ('saveCoinDataSettings', 'pbcoindata'), ('savePBDataSettings', 'pbdata'),
    ('saveApiServerSettings', 'api-server'),
])
@pytest.mark.parametrize('apply_message', [None, '<img src=x onerror=fail()> Applied on next cycle.'])
def test_settings_save_all_callers_confirm_success(caller, svc, apply_message):
    """All save entry points show safe global results and retain API reload callback."""
    _run_node(_settings_save_script() + '\nconst caller=' + json.dumps(caller)
              + ';const svc=' + json.dumps(svc) + ';const applyMessage=' + json.dumps(apply_message) + r""";
(async()=>{
  fields['coindata-fetch-interval']={value:'12'};
  await window[caller]();
  assert.equal(requests.length,1);
  assert.equal(requests[0].url,API_BASE+'/settings/'+svc);
  assert.equal(requests[0].options.method,'POST');
  if(svc==='pbcoindata') assert.equal(JSON.parse(requests[0].options.body).fetch_interval,12);
  assert.equal(buttons[svc].disabled,true);
  await window[caller](); assert.equal(requests.length,1);
  panels[svc+'-tab-settings'].hidden=true;
  requests[0].resolve(response(applyMessage?{ok:true,apply:{message:applyMessage}}:{ok:true}));
  await settle();
  assert.equal(host.hidden,false); assert.equal(host.children.length,1);
  assert.equal(host.children[0].attrs.role,'status');
  assert.equal(textAt(),({pbcoindata:'PBCoinData',pbdata:'PBData','api-server':'PBAPIServer'})[svc]+': '+(applyMessage||'Settings saved.'));
  assert.equal(inline[0].text,applyMessage||'Settings saved.');
  assert.equal(buttons[svc].disabled,false);
  assert.equal(pendingSettingsPosts.size,0);assert.equal(settingsPostJobs.size,0);assert.equal(timers.size,0);
  assert.deepEqual(reloads,svc==='api-server'?['api-server']:[]);
  assert.equal(fields['coindata-fetch-interval'].value,'12');
  const close=host.children[0].children[1];
  assert.equal(close.type,'button');assert.match(close.attrs['aria-label'],/^Close .* save result$/);
  close.listeners.click();assert.equal(host.hidden,true);
})().catch(error=>{testErrors.write(error.stack);testResult.exitCode=1;});
""")


@pytest.mark.parametrize('failure,expected', [
    ("requests[0].reject(new TypeError('Failed to fetch'))", 'Save unconfirmed.'),
    ("requests[0].reject(Object.assign(new Error('Aborted'),{name:'AbortError'}))", 'Save unconfirmed.'),
    ("requests[0].resolve({ok:true,status:200,json:()=>Promise.reject(new TypeError('Failed to fetch'))})", 'Save unconfirmed.'),
    ("requests[0].resolve({ok:true,status:200,json:()=>Promise.reject(Object.assign(new Error('Aborted'),{name:'AbortError'}))})", 'Save unconfirmed.'),
    ("requests[0].resolve({ok:false,status:502,json:()=>Promise.reject(new SyntaxError('HTML'))})", 'Save failed: HTTP 502'),
    ("requests[0].resolve(response({detail:[{loc:['body','fetch_interval'],msg:'Invalid value'}]},422))", 'Save failed: fetch_interval: Invalid value'),
])
@pytest.mark.parametrize('svc', ['pbcoindata', 'pbdata', 'api-server'])
def test_settings_save_errors_remain_visible_across_panels_without_inline(svc, failure, expected):
    """Network, abort, HTML and validation errors remain visible outside hidden panels."""
    _run_node(_settings_save_script() + '\nconst svc=' + json.dumps(svc) + r""";
(async()=>{
  delete messages[ids[svc]];
  const saving=_post('/settings/'+svc,{},ids[svc],()=>reloads.push(svc));
  panels[svc+'-tab-settings'].hidden=true;
  fields.draft={value:'unsaved draft'};
  FAILURE;
  await saving;
  assert.equal(host.hidden,false);assert.equal(host.children[0].attrs.role,'alert');
  assert.ok(textAt().includes(EXPECTED));assert.equal(timers.size,0);
  assert.equal(buttons[svc].disabled,false);assert.equal(pendingSettingsPosts.size,0);
  assert.equal(inline.length,0);assert.deepEqual(reloads,[]);assert.equal(logs.length,1);
  assert.equal(fields.draft.value,'unsaved draft');
  assert.equal(host.children.length,1); // No timer dismisses errors.
})().catch(error=>{testErrors.write(error.stack);testResult.exitCode=1;});
""".replace('FAILURE', failure).replace('EXPECTED', json.dumps(expected)))


@pytest.mark.parametrize('phase', ['fetch', 'body'])
def test_settings_save_deadline_releases_lock_and_rejects_late_results(phase):
    """The deadline bounds fetch and body; old replies cannot notify or unlock a newer save."""
    _run_node(_settings_save_script() + '\nconst phase=' + json.dumps(phase) + r""";
(async()=>{
  let resolveBody;
  const first=_post('/settings/api-server',{},ids['api-server'],()=>reloads.push('old'));
  if(phase==='body') {
    requests[0].resolve({ok:true,status:200,json:()=>new Promise(resolve=>resolveBody=resolve)});
    await settle();
  }
  fireTimeout();await first;
  assert.equal(requests[0].options.signal.aborted,true);
  assert.match(textAt(),/PBAPIServer: Save unconfirmed.*15 seconds.*server may have saved/);
  assert.equal(timers.size,0);assert.equal(pendingSettingsPosts.size,0);
  assert.equal(buttons['api-server'].disabled,false);
  const second=_post('/settings/api-server',{},ids['api-server'],()=>reloads.push('new'));
  await _post('/settings/api-server',{},ids['api-server']);assert.equal(requests.length,2);
  if(phase==='body') resolveBody({ok:true,apply:{message:'obsolete'}});
  else requests[0].resolve(response({ok:true,apply:{message:'obsolete'}}));
  await settle();
  assert.equal(host.children.length,1);assert.deepEqual(reloads,[]);
  assert.equal(buttons['api-server'].disabled,true);
  assert.equal(pendingSettingsPosts.size,1);assert.equal(timers.size,1);
  requests[1].resolve(response());await second;
  assert.equal(host.children.length,2);assert.deepEqual(reloads,['new']);
  assert.equal(host.children[1].attrs.role,'alert');
  assert.equal(timers.size,0);assert.equal(settingsPostJobs.size,0);
})().catch(error=>{testErrors.write(error.stack);testResult.exitCode=1;});
""")


def test_settings_save_overlapping_services_preserve_independent_errors():
    """Success in one service never replaces an unresolved error from another save."""
    _run_node(_settings_save_script() + r"""
(async()=>{
  const coin=_post('/settings/pbcoindata',{},ids.pbcoindata);
  const data=_post('/settings/pbdata',{},ids.pbdata);
  const api=_post('/settings/api-server',{},ids['api-server']);
  assert.equal(pendingSettingsPosts.size,3);
  requests[0].reject(new TypeError('Failed to fetch'));await coin;
  requests[1].resolve(response());await data;
  requests[2].resolve(response());await api;
  assert.equal(host.children.length,3);assert.equal(host.children[2].attrs.role,'alert');
  assert.match(textAt(2),/^PBCoinData: Save unconfirmed/);
  assert.match(textAt(0),/^PBAPIServer:/);assert.match(textAt(1),/^PBData:/);
  host.children[0].children[1].listeners.click();assert.equal(host.hidden,false);
  assert.equal(host.children[1].attrs.role,'alert');
  assert.equal(pendingSettingsPosts.size,0);assert.equal(timers.size,0);
})().catch(error=>{testErrors.write(error.stack);testResult.exitCode=1;});
""")


@pytest.mark.parametrize('persisted', [False, True])
@pytest.mark.parametrize('phase', ['fetch', 'body'])
@pytest.mark.parametrize('prefix', ['', '/mounted'])
def test_settings_save_pagehide_cleans_captured_button_and_cached_return(persisted, phase, prefix):
    """Leaving queues one unconfirmed result; return preserves drafts and newer owners."""
    _run_node(_settings_save_script(prefix) + '\nconst persisted=' + json.dumps(persisted)
              + ';const phase=' + json.dumps(phase) + ';const prefix=' + json.dumps(prefix) + r""";
(async()=>{
  let resolveBody;
  fields.draft={value:'12'};
  showSettingsSaveResult('PBData','Existing result.',false);
  const oldButton=buttons['api-server'];
  const first=_post('/settings/api-server',{},ids['api-server'],()=>reloads.push('old'));
  assert.equal(requests.length,1);
  assert.equal(requests[0].url,prefix+'/api/services/settings/api-server');
  assert.equal(oldButton.disabled,true);
  if(phase==='body') {
    requests[0].resolve({ok:true,status:200,json:()=>new Promise(resolve=>resolveBody=resolve)});
    await settle();
  }
  delete messages[ids['api-server']];delete panels['api-server-tab-settings'];
  events.pagehide({persisted});
  assert.equal(requests[0].options.signal.aborted,true);
  assert.equal(pendingSettingsPosts.size,0);assert.equal(settingsPostJobs.size,0);
  assert.equal(timers.size,0);assert.equal(oldButton.disabled,false);
  assert.equal(pendingUnconfirmedSaves.length,1);
  assert.equal(host.children.length,1);
  await _post('/settings/api-server',{},ids['api-server']);assert.equal(requests.length,1);
  // Resume immediately, before the cancelled save's catch/finally can execute.
  fields.draft.value='13'; // Preserve edits made after the interrupted save.
  events.pageshow({persisted});
  assert.equal(pendingUnconfirmedSaves.length,0);
  assert.equal(host.children.length,2);assert.equal(host.hidden,false);
  assert.equal(host.children[0].attrs.role,'alert');
  assert.match(textAt(),/^PBAPIServer: Save unconfirmed\..*server may have saved/);
  assert.equal(textAt(1),'PBData: Existing result.');
  events.pageshow({persisted});assert.equal(host.children.length,2);
  assert.equal(requests.length,1);assert.equal(fields.draft.value,'13');
  buttons['api-server']=new Element();
  panels['api-server-tab-settings']={querySelector:()=>buttons['api-server']};
  const second=_post('/settings/api-server',{},ids['api-server'],()=>reloads.push('new'));
  await first;
  if(phase==='body') resolveBody({ok:true});else requests[0].resolve(response());
  await settle();
  assert.equal(host.children.length,2);assert.deepEqual(reloads,[]);
  assert.equal(host.children[0].attrs.role,'alert');
  assert.match(textAt(),/^PBAPIServer: Save unconfirmed\..*server may have saved/);
  assert.equal(buttons['api-server'].disabled,true);
  assert.equal(pendingSettingsPosts.size,1);assert.equal(settingsPostJobs.size,1);assert.equal(timers.size,1);
  requests[1].resolve(response());await second;
  assert.deepEqual(reloads,['new']);assert.equal(host.children.length,3);
  assert.equal(buttons['api-server'].disabled,false);assert.equal(timers.size,0);
})().catch(error=>{testErrors.write(error.stack);testResult.exitCode=1;});
""")


@pytest.mark.parametrize('cleanup', ['success', 'error', 'timeout', 'pagehide'])
@pytest.mark.parametrize('load_state', ['failed', 'loading'])
def test_settings_save_cleanup_preserves_settings_load_button_lock(cleanup, load_state):
    """Save completion or cancellation must not override a settings-load failure/lock."""
    _run_node(_settings_save_script() + '\nconst cleanup=' + json.dumps(cleanup)
              + ';const loadState=' + json.dumps(load_state) + r""";
(async()=>{
  const saving=_post('/settings/pbcoindata',{},ids.pbcoindata);
  if(loadState==='failed') delete _settingsLoaded.pbcoindata;
  else _settingsLoading.pbcoindata={};
  if(cleanup==='success') requests[0].resolve(response());
  if(cleanup==='error') requests[0].reject(new TypeError('Failed to fetch'));
  if(cleanup==='timeout') fireTimeout();
  if(cleanup==='pagehide') events.pagehide({persisted:true});
  await saving;
  assert.equal(buttons.pbcoindata.disabled,true);
  assert.equal(pendingSettingsPosts.size,0);assert.equal(timers.size,0);
})().catch(error=>{testErrors.write(error.stack);testResult.exitCode=1;});
""")


def test_settings_save_feedback_is_outside_switchable_panels():
    """The feedback host precedes service panels; server-rendered entry is never cached."""
    source = PAGE.read_text(encoding='utf-8')
    main = source.index('<div id="main-content">')
    feedback = source.index('<div id="settings-save-results"')
    panel = source.index('<div id="panel-overview"')
    assert main < feedback < panel
    assert 'aria-label="Settings save results" hidden></div>' in source
    assert '/app/js/shared_help_overlay.js?v=12' in source
    services = (ROOT / 'api' / 'services.py').read_text(encoding='utf-8')
    assert 'headers={"Cache-Control": "no-store"}' in services


@pytest.mark.parametrize('prefix', ['', '/mounted'])
@pytest.mark.parametrize('persisted', [False, True])
@pytest.mark.parametrize('outcome', ['success', 'error', 'callback-pending'])
def test_settings_save_completed_result_is_not_reclassified_on_return(prefix, persisted, outcome):
    """Confirmed results stay confirmed, including while a success callback is pending."""
    _run_node(_settings_save_script(prefix) + '\nconst persisted=' + json.dumps(persisted)
              + ';const outcome=' + json.dumps(outcome) + r""";
(async()=>{
  let finishCallback;
  const saving=_post('/settings/api-server',{},ids['api-server'],()=>{
    if(outcome==='callback-pending') return new Promise(resolve=>finishCallback=resolve);
  });
  requests[0].resolve(outcome==='error'?response({detail:'Synthetic failure'},422):response());
  await settle();
  assert.equal(host.children.length,1);
  const originalResult=host.children[0], originalText=textAt();
  if(outcome==='callback-pending') assert.equal(settingsPostJobs.size,1);
  events.pagehide({persisted});events.pageshow({persisted});
  if(finishCallback) finishCallback();
  await saving;
  assert.equal(host.children.length,1);assert.strictEqual(host.children[0],originalResult);
  assert.equal(textAt(),originalText);assert.doesNotMatch(textAt(),/unconfirmed/);
  assert.equal(pendingUnconfirmedSaves.length,0);assert.equal(pendingSettingsPosts.size,0);
  assert.equal(settingsPostJobs.size,0);assert.equal(timers.size,0);
  assert.equal(buttons['api-server'].disabled,false);assert.equal(requests.length,1);
})().catch(error=>{testErrors.write(error.stack);testResult.exitCode=1;});
""")


@pytest.mark.parametrize('prefix', ['', '/mounted'])
@pytest.mark.parametrize('persisted', [False, True])
@pytest.mark.parametrize('outcome', ['pending', 'success', 'error'])
def test_settings_save_real_menu_simulated_lifecycle_feedback(prefix, persisted, outcome, monkeypatch):
    """Real menu and DOM show interrupted saves honestly after synthetic lifecycle events."""
    import mimetypes
    from urllib.parse import urlparse

    browser_api = __import__('browser_driver')
    html = _render_cmc_services_page(prefix, monkeypatch)
    writes, errors, unexpected = [], [], []
    services = ('pbcluster', 'pbrun', 'pbdata', 'pbcoindata', 'monitor-agent', 'vps-monitor', 'api-server')
    with browser_api.sync_browser() as driver:
        browser = driver.chromium.launch(headless=True)
        try:
            page = browser.new_page(viewport={'width': 1450, 'height': 1000})
            page.set_default_timeout(3000)
            page.clock.install()
            page.on('pageerror', lambda error: errors.append(str(error)))
            page.add_init_script("""
window.WebSocket=class {
  static OPEN=1; constructor(){this.readyState=1;setTimeout(()=>this.onopen?.({}),0);}
  send(){} close(){this.readyState=3;this.onclose?.({});}
};
""")

            def respond(route):
                """Serve only local assets and synthetic data; block every other request."""
                request = route.request
                url = urlparse(request.url)
                if url.hostname != 'cmc.test' or (prefix and not url.path.startswith(prefix + '/')):
                    unexpected.append(request.url)
                    route.fulfill(status=451, body='Blocked fixture request')
                    return
                path = url.path[len(prefix):] if prefix else url.path
                if request.method == 'POST' and path == '/api/token-refresh':
                    route.fulfill(json={'ok': True})
                elif request.method == 'POST':
                    if path == '/api/services/settings/pbcoindata':
                        writes.append(route)
                    else:
                        unexpected.append(path)
                        route.fulfill(status=451, body='Unexpected write blocked')
                elif path == '/app/start.html':
                    route.fulfill(content_type='text/html', body='<nav id="topnav"></nav><script>window.PBGUI_NAV_CONFIG={current:"welcome",authenticated:true,apiBase:' + json.dumps(prefix + '/api') + '};</script><script src="' + prefix + '/app/pbgui_nav.js"></script>')
                elif path == '/api/services/main_page':
                    route.fulfill(content_type='text/html', body=html)
                elif path.startswith('/app/'):
                    asset = (ROOT / 'frontend' / path.removeprefix('/app/')).resolve()
                    if asset.is_relative_to((ROOT / 'frontend').resolve()) and asset.is_file():
                        route.fulfill(body=asset.read_bytes(), content_type=mimetypes.guess_type(asset)[0] or 'application/octet-stream')
                    else:
                        unexpected.append(path)
                        route.fulfill(status=404, body='Missing local asset')
                elif path == '/api/services/settings/pbcoindata':
                    route.fulfill(json={'fetch_interval': 24, 'fetch_limit': 5000, 'metadata_interval': 1, 'mapping_interval': 24})
                elif path == '/api/services/status':
                    route.fulfill(json={name: {'running': True, 'enabled': True, 'can_enable': True} for name in services})
                elif path == '/api/services/workers/summary':
                    route.fulfill(json={'counts': {'total': 7, 'running': 7}})
                elif path == '/api/services/migration/status':
                    route.fulfill(json={'migration_needed': False, 'warnings': []})
                elif path == '/api/services/cmc-pool':
                    route.fulfill(json={'ready': True, 'health': 'ok', 'keys': []})
                elif path == '/api/services/cmc-pool/leases':
                    route.fulfill(json={'authority': {}, 'leases': []})
                elif path.endswith('/stream'):
                    route.fulfill(status=204, body='')
                elif path in {'/api/server-status', '/api/vps/alerts', '/api/ai/preferences', '/api/token-refresh', '/api/auth/me', '/api/notify_log'}:
                    route.fulfill(json={'ok': True, 'authenticated': True, 'drawer_open': False, 'items': [],
                                        'summary': {'new_count': 0, 'ack_count': 0, 'total_active': 0}})
                else:
                    unexpected.append(path)
                    route.fulfill(status=404, json={'detail': 'Unexpected fixture request'})

            page.route('**/*', respond)
            page.goto('http://cmc.test' + prefix + '/app/start.html')
            page.locator('.nav-group-btn', has_text='System').click()
            page.locator('.nav-item[data-page="system_services"]').click()
            page.locator('.sb-btn[data-panel="pbcoindata"]').click()
            page.locator('.tab-btn[data-svc="pbcoindata"][data-tab="settings"]').click()
            field = page.locator('#coindata-fetch-interval')
            save = page.locator('#pbcoindata-tab-settings .form-btn.save')
            browser_api.expect(field).to_have_value('24')
            field.fill('12')
            save.click()
            browser_api.expect(save).to_be_disabled()
            for _ in range(100):
                if writes:
                    break
                page.wait_for_timeout(10)
            assert len(writes) == 1
            assert writes[0].request.post_data_json['fetch_interval'] == 12
            results = page.locator('#settings-save-results')
            if outcome != 'pending':
                if outcome == 'success':
                    writes[0].fulfill(json={'ok': True, 'apply': {'message': 'Synthetic settings confirmed'}})
                else:
                    writes[0].fulfill(status=422, json={'detail': 'Synthetic settings rejected'})
                browser_api.expect(results).to_contain_text('Synthetic settings')
                browser_api.expect(save).to_be_enabled()
            field.fill('17')
            page.locator('.tab-btn[data-svc="pbcoindata"][data-tab="pool"]').click()
            page.get_by_role('button', name='Add Key', exact=True).click()
            label = page.locator('#cmc-key-label')
            label.fill('Unsaved synthetic draft')
            page.evaluate("persisted => { window.dispatchEvent(new PageTransitionEvent('pagehide', {persisted})); window.dispatchEvent(new PageTransitionEvent('pageshow', {persisted})); }", persisted)
            if outcome == 'pending':
                writes[0].abort('aborted')
                browser_api.expect(results.locator('[role="alert"]')).to_contain_text('PBCoinData: Save unconfirmed.')
                browser_api.expect(results).to_contain_text('The server may have saved the settings.')
            else:
                browser_api.expect(results).not_to_contain_text('Save unconfirmed')
            browser_api.expect(results.locator('.settings-save-result')).to_have_count(1)
            browser_api.expect(save).to_be_enabled()
            browser_api.expect(field).to_have_value('17')
            browser_api.expect(page.locator('#cmc-key-modal')).to_be_visible()
            browser_api.expect(label).to_have_value('Unsaved synthetic draft')
            browser_api.expect(label).to_be_focused()
            page.locator('#cmc-key-modal').get_by_role('button', name='Cancel', exact=True).click()
            page.locator('.tab-btn[data-svc="pbcoindata"][data-tab="settings"]').click()
            browser_api.expect(results).to_be_visible()
            page.evaluate("persisted => window.dispatchEvent(new PageTransitionEvent('pageshow', {persisted}))", persisted)
            browser_api.expect(results.locator('.settings-save-result')).to_have_count(1)
            page.clock.run_for(16000)
            browser_api.expect(results.locator('.settings-save-result')).to_have_count(1)
            assert len(writes) == 1
            assert not errors, errors
            assert not unexpected, unexpected
        finally:
            page.unroute_all(behavior='ignoreErrors')
            browser.close()
