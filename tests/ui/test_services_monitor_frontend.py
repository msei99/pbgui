"""Behavioral regressions for Services Monitor worker requests."""

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
