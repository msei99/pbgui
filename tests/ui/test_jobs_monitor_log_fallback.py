"""Offline job-monitor regressions using real functions and synthetic responses."""

from javascript_checks import run_javascript

import ast
import json
import re
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest
from starlette.requests import Request

from api.page_templates import render_page_urls


SOURCE = Path(__file__).resolve().parents[2] / "frontend" / "jobs_monitor.html"
RENDERERS = [
    "numberValue", "parseHumanBytes", "parseDownloaderRowsFromLog", "getDownloaderRows",
    "formatCount", "formatBytes", "formatDaysFromMinutes", "renderDownloaderDetails",
    "fmtDayCompact", "formatTimestamp", "formatJobDuration", "formatDurationSeconds",
    "calculateProgress", "formatJsonBlock", "renderDetailRows", "renderJobDetails",
]
BOOTSTRAP = r"""
const escapeHtml = value => String(value == null ? '' : value)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
const escapeAttr = escapeHtml;
const logLines = [
    '2026-10-18 00:00:00 fixture-host segment done coin=BTC pages=2 rows=40 payload=1 KiB minutes_written=40'
];
const job = {id:'fixture-job', type:'bitget_best_1m_distributed', status:'running',
    payload:{distributed_hosts:[{label:'fixture-host'}]}, progress:{}};
"""


def _page_functions(source, names):
    """Extract actual standalone page declarations for bounded offline execution."""
    declarations = []
    for name in names:
        match = re.search(r"^( +)(?:async )?function " + re.escape(name) + r"\([^\n]*\) \{", source, re.M)
        assert match, name
        end = source.index("\n" + match[1] + "}", match.end()) + len(match[1]) + 2
        declarations.append(source[match.start():end])
    return "\n".join(declarations)


def _run_page_script(script):
    """Run JavaScript without a network; subprocess.run kills and waits on timeout."""
    script = "const assert = browserAssert;\n(async () => {\n" + script
    script += "\n})().catch(error => {console.error(error); testResult.exitCode=1;});"
    result = run_javascript(["-e", script], capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("prefix", ["", "/proxy/pbgui", "/my%20gui"])
@pytest.mark.parametrize("protocol", ["http", "https"])
@pytest.mark.parametrize("rendered", [False, True])
@pytest.mark.parametrize("embedded", [False, True])
def test_monitor_entry_urls_filters_and_synthetic_updates(prefix, protocol, rendered, embedded):
    """Rendered/static entries retain same-origin resources, filters and update context."""
    source = SOURCE.read_text(encoding="utf-8")
    if rendered:
        source = render_page_urls(Request({"type": "http", "root_path": prefix.replace("%20", " ")}), source, "/api")
    head_script = re.findall(r"<script>(.*?)</script>", source, re.S)[0]
    init_start = source.index("const urlParams =")
    init_end = source.index("        function isClippingAncestor", init_start)
    functions = _page_functions(source, [
        "matchesExchangeFilter", "logUiNotification", "connectWebSocket", "startPolling", "updateJobs", "loadTab",
    ])
    path = "/api/jobs/main_page" if rendered else "/app/jobs_monitor.html"
    url = f"{protocol}://jobs.test:8443{prefix}{path}?embed={int(embedded)}"
    url += "&exchange=%20BITGET%20&job_type=%20BITGET_BEST_1M%20&api_base=https://evil.test&root_path=//evil.test&token=fixture-secret"
    script = "const window={location:new URL(" + json.dumps(url) + ")};\n"
    script += r"""
const nodes = new Map();
function node(id) {
    if (!nodes.has(id)) nodes.set(id, {innerHTML:'',classList:{values:new Set(),add(value){this.values.add(value);}}});
    return nodes.get(id);
}
const document={getElementById:node,documentElement:node('html'),body:node('body')};
const requests=[], sockets=[];
const fixtureJobs=[
    {id:'matching-fixture',type:'bitget_best_1m',exchange:'bitget',status:'running'},
    {id:'wrong-exchange',type:'bitget_best_1m',exchange:'bybit',status:'running'},
    {id:'wrong-type',type:'bitget_other',exchange:'bitget',status:'running'},
];
const fetch=async(url, options={})=>{requests.push({url,options});return {ok:true,json:async()=>({jobs:fixtureJobs})};};
const WebSocket=class {constructor(url){this.url=url;sockets.push(this);}};
let poll;
const setInterval=callback=>{poll=callback;return 1;};
let ws=null, reconnectAttempts=0, pollingInterval=null, activeJobsSnapshot=[];
let currentTab='done';
const MAX_RECONNECT_ATTEMPTS=5, startingJobIds=new Set(), cancellingJobIds=new Set();
const renderActiveJob=job=>job.id, renderJob=renderActiveJob;
const compareActiveJobs=()=>0, ensureDownloaderLogFallbacks=()=>{};
const escapeHtml=value=>value;
""" + head_script + "\n" + source[init_start:init_end] + "\n" + functions
    script += "\nconst expectedPrefix=" + json.dumps(prefix) + ";\nconst embedded=" + json.dumps(embedded) + ";\n"
    script += r"""
assert.equal(API_BASE,expectedPrefix+'/api');
assert.equal(BASE_PREFIX,expectedPrefix);
assert.equal(WS_URL,(window.location.protocol==='https:'?'wss://':'ws://')+window.location.host+expectedPrefix+'/ws/jobs');
assert.equal(node('jobs-monitor-style').href,expectedPrefix+'/app/css/app.css');
assert.equal(document.body.classList.values.has('is-embedded'),embedded);
assert.equal(document.documentElement.classList.values.has('is-embedded'),embedded);
assert.equal(EXCHANGE_FILTER,'bitget');
assert.equal(JOB_TYPE_FILTER,'bitget_best_1m');
connectWebSocket();
assert.equal(sockets[0].url,WS_URL);
sockets[0].onopen();
assert.equal(node('connection-status').textContent,'Connected');
sockets[0].onmessage({data:JSON.stringify({type:'jobs',data:fixtureJobs})});
assert.equal(node('jobs-running').innerHTML,'matching-fixture');
assert.equal(currentTab,'done');
await loadTab('done');
assert.equal(node('jobs-done').innerHTML,'matching-fixture');
assert.equal(requests[0].url,expectedPrefix+'/api/jobs/?states=done&limit=50');
logUiNotification('Synthetic notice','info');
assert.equal(requests[1].url,expectedPrefix+'/api/notify_log');
assert.equal(requests[1].options.method,'POST');
startPolling();
await poll();
assert.equal(requests[2].url,expectedPrefix+'/api/jobs/?states=pending,running');
assert.equal(currentTab,'done');
for(const url of [...requests.map(request=>request.url),node('jobs-monitor-style').href]) {
    assert.equal(new URL(url,window.location.href).origin,window.location.origin);
}
assert.equal(new URL(WS_URL).host,window.location.host);
assert(!JSON.stringify(requests).includes('fixture-secret'));
assert(!JSON.stringify(requests).includes('evil.test'));
"""
    _run_page_script(script)


@pytest.mark.parametrize("prefix", ["", "/proxy/pbgui"])
def test_services_and_market_data_embed_urls_preserve_prefix_and_frame(prefix):
    """Real embed builders consume synthetic worker data and retain existing frames."""
    services = (SOURCE.parent / "services_monitor.html").read_text(encoding="utf-8")
    market_data = (SOURCE.parent / "market_data_main.html").read_text(encoding="utf-8")
    tree = ast.parse((SOURCE.parents[1] / "api" / "services.py").read_text(encoding="utf-8"))
    worker = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "_get_task_worker_item")
    monitor_path = next(
        keyword.value.value for call in ast.walk(worker) if isinstance(call, ast.Call)
        for keyword in call.keywords if keyword.arg == "monitor_path"
    )
    script = "const monitorPath=" + json.dumps(monitor_path) + ";\n"
    # Read the real template initialization and capability metadata, not copied URL builders.
    rendered_market = render_page_urls(Request({"type": "http", "root_path": prefix}), market_data, "/api/market-data")
    script += re.search(r'var API_BASE = .*?;\s*var BASE_PREFIX = .*?;', rendered_market, re.S)[0]
    script += '\nassert.equal(BASE_PREFIX,' + json.dumps(prefix) + ');\n'
    for name in ("exchangeOptions", "best1mJobMonitorMeta"):
        script += re.search(r"      var " + name + r" = .*?\n      [\]}];", market_data, re.S)[0]
    script += _page_functions(services, ["getWorkerMonitorUrl", "updateWorkerLog"])
    script += _page_functions(market_data, [
        "getExchangeMeta", "buildBest1mJobMonitorUrl", "mountBest1mJobMonitor",
        "buildCopyDataJobMonitorUrl", "mountCopyDataJobMonitor", "mountIntegrityJobMonitor",
    ])
    script += r"""
const PBGUI_SERIAL='test-serial', uiState={contextExchange:'bybit'};
let frame=null, writes=0;
const empty={style:{},classList:{remove(){},add(){}},querySelector(){return frame;},
    set innerHTML(value){
        writes++;
        const src=value.match(/src="([^"]+)"/)[1];
        frame={dataset:{workerId:'market-data-task'},getAttribute(){return src;},setAttribute(){}};
    }};
const nodes={'worker-log-empty':empty,'worker-log-wrap':{style:{}}};
for(const id of ['best1m-job-monitor-card','best1m-job-monitor-frame','copy-data-job-monitor-frame','integrity-job-monitor-frame']){
    nodes[id]={hidden:true,dataset:{},writes:0,set src(value){this.value=value;this.writes++;}};
}
const document={getElementById:id=>nodes[id]};
const _workerLogViewer=null, _workerLogViewerActive=false, esc=value=>value;
const item={id:'market-data-task',label:'Market Data Queue',monitor_path:monitorPath};
assert.equal(getWorkerMonitorUrl(item),BASE_PREFIX+'/app/jobs_monitor.html');
updateWorkerLog(item);
updateWorkerLog({...item,running:true});
assert.equal(writes,1); // Worker polling retains the monitor and its context.
for(const exchange of ['binance','bybit','bitget','okx']) {
    mountBest1mJobMonitor(exchange,false);
    const url=new URL(nodes['best1m-job-monitor-frame'].value,'https://jobs.test');
    assert.equal(url.pathname,BASE_PREFIX+'/app/jobs_monitor.html');
    assert.equal(url.searchParams.get('embed'),'1');
    assert.equal(url.searchParams.get('exchange'),best1mJobMonitorMeta[exchange].exchange);
    assert.equal(url.searchParams.get('job_type'),best1mJobMonitorMeta[exchange].jobType);
    const before=nodes['best1m-job-monitor-frame'].writes;
    mountBest1mJobMonitor(exchange,false);
    assert.equal(nodes['best1m-job-monitor-frame'].writes,before);
}
mountCopyDataJobMonitor(false);
mountIntegrityJobMonitor(false);
for(const [id,exchange,type] of [
    ['copy-data-job-monitor-frame','ohlcv','ohlcv_copy,ohlcv_copy_dry_run'],
    ['integrity-job-monitor-frame','bybit','ohlcv_checksum_reference'],
]) {
    const url=new URL(nodes[id].value,'https://jobs.test');
    assert.equal(url.origin,'https://jobs.test');
    assert.equal(url.pathname,BASE_PREFIX+'/app/jobs_monitor.html');
    assert.equal(url.searchParams.get('exchange'),exchange);
    assert(url.searchParams.get('job_type').includes(type));
    assert.equal(url.searchParams.get('embed'),'1');
    assert.equal(url.searchParams.get('v'),PBGUI_SERIAL);
    assert(!url.searchParams.has('_ts'));
    assert.equal(nodes[id].writes,1);
}
mountCopyDataJobMonitor(false);
mountIntegrityJobMonitor(false);
assert.equal(nodes['copy-data-job-monitor-frame'].writes,1);
assert.equal(nodes['integrity-job-monitor-frame'].writes,1);
"""
    _run_page_script(script)


def run_functions(names, assertions):
    """Execute page declarations in the browser without page startup or network access."""
    source = SOURCE.read_text(encoding="utf-8")
    declarations = []
    for name in names:
        match = re.search(r"^( +)(?:async )?function " + re.escape(name) + r"\([^\n]*\) \{", source, re.M)
        assert match, name
        end = source.index("\n" + match[1] + "}", match.end()) + len(match[1]) + 2
        declarations.append(source[match.start():end])
    script = "const assert = browserAssert;\n(async () => {\n"
    script += BOOTSTRAP + "\n".join(declarations) + "\n" + assertions
    script += "\n})().catch(error => {console.error(error); testResult.exitCode=1;});"
    result = run_javascript(["-e", script], capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("structured_key", ["host_results", "downloaders"])
def test_log_fallback_label_tracks_actual_source(structured_key):
    """Only parsed log statistics carry the excerpt label, even with mixed inputs."""
    run_functions(RENDERERS, r"""
const logRows = getDownloaderRows(job, logLines);
assert.equal(logRows[0].rows, 40);
assert.equal(logRows[0].payloadBytes, 1024);
assert.equal(logRows[0].fromLog, true);
for (const compact of [true, false]) {
    const markup = renderDownloaderDetails(job, {compact, logLines});
    assert(markup.includes('Recent log excerpt'));
    assert(markup.includes('fixture-host'));
    assert(markup.includes('40 rows'));
    assert(!renderDownloaderDetails(job, {compact, logLines:['unrelated text']}).includes('Recent log excerpt'));
    assert(!renderDownloaderDetails(job, {compact}).includes('Recent log excerpt'));
}
assert(!renderDownloaderDetails({progress:{}}, {logLines:['unrelated text']}));
job.progress.last_result = {STRUCTURED_KEY:[{host:'fixture-host', rows:99, pages:7, segments:3, payload_bytes:2048}]};
assert.equal(getDownloaderRows(job, logLines)[0].rows, 99);
assert.equal(getDownloaderRows(job, logLines)[0].fromLog, false);
for (const compact of [true, false]) {
    const markup = renderDownloaderDetails(job, {compact, logLines});
    assert(markup.includes('99 rows'));
    assert(!markup.includes('40 rows'));
    assert(!markup.includes('Recent log excerpt'));
}
// Structured zero values still take precedence over nonzero log contributions.
job.progress.last_result.STRUCTURED_KEY[0].rows = 0;
assert.equal(getDownloaderRows(job, logLines)[0].rows, 0);
assert(!renderDownloaderDetails(job, {logLines}).includes('Recent log excerpt'));
// host_results retains precedence over the older downloaders field as well.
job.progress.last_result = {host_results:[{host:'fixture-host', rows:77}], downloaders:[{host:'fixture-host', rows:88}]};
assert.equal(getDownloaderRows(job, logLines)[0].rows, 77);
""".replace("STRUCTURED_KEY", structured_key))


def test_background_log_tail_updates_without_changing_current_context():
    """The bounded fallback updates open cards, respects its cache and leaves other tabs alone."""
    run_functions(RENDERERS + ["downloaderContributionTotal", "renderActiveJob", "ensureDownloaderLogFallbacks"], r"""
const API_BASE='/mounted/api', authOptions=()=>({credentials:'same-origin'});
const downloaderLogCache=new Map(), downloaderLogFetches=new Set();
const expandedDownloaderJobs=new Set([job.id]), expandedJobs=new Set([job.id]);
const cancellingJobIds=new Set(), startingJobIds=new Set();
let currentTab='running', activeJobsSnapshot=[job], requests=[], writes=0;
let now=100000, release;
Date.now=()=>now;
const container={html:'', set innerHTML(value){writes++;this.html=value;}};
const document={getElementById:id=>{assert.equal(id,'jobs-running');return container;}};
const fetch=(url, options)=>{requests.push({url, options});return new Promise(resolve=>release=resolve);};
const settle=()=>new Promise(resolve=>setImmediate(resolve));
ensureDownloaderLogFallbacks([job]);
ensureDownloaderLogFallbacks([job]);
assert.equal(requests.length,1);
assert.equal(requests[0].url,'/mounted/api/jobs/fixture-job/log?lines=5000');
release({ok:true,json:async()=>({log:logLines,exists:true})});
await settle();
assert.equal(downloaderLogFetches.size,0);
assert.deepEqual(downloaderLogCache.get(job.id).lines,logLines);
assert(container.html.includes('Recent log excerpt'));
assert(container.html.includes('data-downloader-job-id="fixture-job" open'));
assert(expandedDownloaderJobs.has(job.id));
assert(expandedJobs.has(job.id));
assert.equal(currentTab,'running');
ensureDownloaderLogFallbacks([job]);
assert.equal(requests.length,1); // Fresh cache prevents another content request.
now+=6000;
ensureDownloaderLogFallbacks([job]);
release({ok:true,json:async()=>({log:[...logLines,...logLines],exists:true})});
await settle();
assert(container.html.includes('80 rows')); // Background updates need no manual refresh.
assert.equal(writes,2);
now+=6000;
ensureDownloaderLogFallbacks([job]);
currentTab='done';
release({ok:true,json:async()=>({log:logLines,exists:true})});
await settle();
assert.equal(writes,2);
assert.equal(currentTab,'done');
""")


@pytest.mark.parametrize("exists", [False, True])
def test_details_fetches_bounded_log_and_handles_missing_or_empty_files(exists):
    """Details retain their summary and scope when an empty log response arrives."""
    run_functions(RENDERERS + ["showJobDetails"], r"""
const API_BASE='/mounted/api', authOptions=()=>({credentials:'same-origin'});
const content={innerHTML:''}, title={textContent:''}, classes=new Set();
const modal={classList:{add:value=>classes.add(value)}};
const document={getElementById:id=>({'details-modal':modal,'details-content':content,'details-job-id':title}[id])};
let lines=logLines, requests=[];
const fetch=async(url,options)=>{
    requests.push(url);
    if(url.endsWith('/log?lines=5000')) return {ok:true,json:async()=>({log:lines,exists:lines.length > 0 || EXISTS})};
    assert.equal(url,'/mounted/api/jobs/fixture-job');
    return {ok:true,json:async()=>job};
};
await showJobDetails(job.id);
assert.deepEqual(requests,['/mounted/api/jobs/fixture-job','/mounted/api/jobs/fixture-job/log?lines=5000']);
assert.equal(title.textContent,job.id);
assert(classes.has('active'));
assert(content.innerHTML.includes('Summary'));
assert(content.innerHTML.includes('Scope'));
assert(content.innerHTML.includes('Recent log excerpt'));
lines=[];
await showJobDetails(job.id);
assert(classes.has('active'));
assert.equal(title.textContent,job.id);
assert(content.innerHTML.includes('Summary'));
assert(content.innerHTML.includes('Scope'));
assert(!content.innerHTML.includes('Recent log excerpt'));
""".replace("EXISTS", str(exists).lower()))


def test_log_viewer_retains_its_500_line_request():
    """Content fallbacks use 5000 while the separate log viewer keeps its existing tail."""
    source = SOURCE.read_text(encoding="utf-8")
    assert "log?lines=0" not in source
    assert source.count("/log?lines=5000`") == 2
    assert source.count("/log?lines=500`") == 1


@pytest.mark.parametrize("prefix", ["", "/proxy/pbgui"])
def test_browser_log_fallback_and_details_with_synthetic_jobs(prefix):
    """Reach the embedded monitor through the real menu and exercise mocked updates."""
    browser_api = __import__('browser_driver')
    services_html = (SOURCE.parent / "services_monitor.html").read_text(encoding="utf-8")
    services_html = render_page_urls(Request({"type": "http", "root_path": prefix}), services_html, "/api/services")
    for marker, value in {"VERSION": "test",
                          "SERIAL": "1", "NAV_HASH": "test"}.items():
        services_html = services_html.replace("%%" + marker + "%%", value)
    fixture_job = {
        "id": "fixture-job", "type": "bitget_best_1m_distributed", "status": "running",
        "payload": {"distributed_hosts": [{"label": "fixture-host"}]}, "progress": {},
    }
    log_line = "2026-10-18 00:00:00 fixture-host segment done coin=BTC pages=2 rows=1 payload=1 KiB minutes_written=1"
    logs = [log_line] * 6001
    requests, errors = [], []
    with browser_api.sync_browser() as driver:
        browser = driver.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1200, "height": 800})
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.add_init_script("""
            window.WebSocket = class {
                constructor() { window.fixtureSocket = this; }
                close() {}
            };
        """)

        def respond(route):
            """Serve local assets and intercept all API traffic without a running PBGui."""
            url = urlparse(route.request.url)
            if prefix and not url.path.startswith(prefix + "/"):
                errors.append("Missing application prefix: " + url.path)
                route.fulfill(status=404)
                return
            path = url.path.removeprefix(prefix) if prefix else url.path
            if path == "/api/token-refresh" and route.request.method == "POST":
                route.fulfill(json={})
            elif route.request.method != "GET":
                errors.append("Unexpected mutation: " + route.request.method + " " + url.path)
                route.fulfill(status=405)
            elif path == "/app/start.html":
                start_html = '''<nav id="topnav"></nav>
                    <script>window.PBGUI_NAV_CONFIG={current:'/',authenticated:true,apiBase:"%%API_BASE%%"};</script>
                    <script src="/app/pbgui_nav.js?v=test"></script>'''
                route.fulfill(content_type="text/html", body=render_page_urls(
                    Request({"type": "http", "root_path": prefix}), start_html, "/api",
                ))
            elif path == "/api/services/main_page":
                route.fulfill(body=services_html, content_type="text/html")
            elif path == "/app/jobs_monitor.html":
                route.fulfill(body=SOURCE.read_text(encoding="utf-8"), content_type="text/html")
            elif path.startswith("/app/"):
                asset = SOURCE.parent / path.removeprefix("/app/")
                if asset.is_file():
                    mime = "text/css" if asset.suffix == ".css" else "text/javascript"
                    route.fulfill(body=asset.read_bytes(), content_type=mime)
                else:
                    route.fulfill(status=404)
            elif path == "/api/services/workers/status":
                route.fulfill(json={"counts": {"total": 1, "running": 1}, "groups": [{
                    "id": "queue", "label": "Queues", "items": [{
                        "id": "market-data-task", "label": "Market Data Queue", "running": True,
                        "can_start": False, "can_stop": False, "monitor_path": "/app/jobs_monitor.html",
                    }],
                }]})
            elif path == "/api/jobs/fixture-job/log":
                lines = int(parse_qs(url.query)["lines"][0])
                requests.append(lines)
                route.fulfill(json={"log": logs[-min(lines, 5000):] if lines > 0 else [], "exists": True})
            elif path == "/api/jobs/fixture-job":
                route.fulfill(json=fixture_job)
            elif path.startswith("/api/"):
                route.fulfill(json={})
            else:
                errors.append("Unexpected request: " + url.path)
                route.fulfill(status=404)

        page.route("**/*", respond)
        page.set_default_timeout(5000)
        page.goto("http://jobs.test" + prefix + "/app/start.html")
        page.locator(".nav-group-btn", has_text="System").click()
        page.locator('.nav-item[data-page="system_services"]').click()
        page.locator('button[data-panel="workers"]').click()
        page.locator(".worker-card", has_text="Market Data Queue").click()
        frame_element = page.locator("iframe.worker-monitor-frame")
        browser_api.expect(frame_element).to_be_visible()
        assert frame_element.get_attribute("src") == prefix + "/app/jobs_monitor.html"
        page = frame_element.element_handle().content_frame()
        assert page is not None
        page.wait_for_function("window.fixtureSocket")
        push_jobs = "jobs => window.fixtureSocket.onmessage({data:JSON.stringify({type:'jobs',data:jobs})})"
        page.evaluate(push_jobs, [fixture_job])
        card = page.locator("#jobs-running .downloader-details")
        browser_api.expect(card.locator("summary")).to_contain_text("Recent log excerpt")
        assert requests == [5000]
        card.locator("summary").click()
        browser_api.expect(card).to_have_attribute("open", "")
        page.evaluate(push_jobs, [fixture_job])
        browser_api.expect(card).to_have_attribute("open", "")
        browser_api.expect(page.locator(".tab-btn.active")).to_have_text("Active")

        page.locator('#jobs-running [data-job-action="view"]').click()
        details = page.locator("#details-modal")
        browser_api.expect(details).to_have_class("modal active")
        browser_api.expect(details.locator(".downloader-details summary")).to_contain_text("Recent log excerpt")
        browser_api.expect(details.locator("#details-job-id")).to_have_text("fixture-job")
        details.get_by_role("button", name="Close", exact=True).click()

        fixture_job["progress"] = {"last_result": {"host_results": [{"host": "fixture-host", "rows": 99}]}}
        page.evaluate(push_jobs, [fixture_job])
        browser_api.expect(card.locator("summary")).not_to_contain_text("Recent log excerpt")
        browser_api.expect(card.locator("summary")).to_contain_text("99 rows")
        browser_api.expect(card).to_have_attribute("open", "")
        page.locator('#jobs-running [data-job-action="view"]').click()
        browser_api.expect(details.locator(".downloader-details summary")).to_contain_text("99 rows")
        browser_api.expect(details.locator(".downloader-details summary")).not_to_contain_text("Recent log excerpt")
        details.get_by_role("button", name="Close", exact=True).click()
        page.locator('#jobs-running [data-job-action="log"]').click()
        browser_api.expect(page.locator("#log-content")).to_contain_text("segment done")
        assert requests[-1] == 500
        assert all(lines in {500, 5000} for lines in requests)
        assert not errors
        browser.close()
