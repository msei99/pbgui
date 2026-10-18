"""Offline job-monitor regressions using real functions and synthetic responses."""

import re
import subprocess
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest


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


def run_functions(names, assertions):
    """Execute page declarations in Node without page startup or network access."""
    source = SOURCE.read_text(encoding="utf-8")
    declarations = []
    for name in names:
        match = re.search(r"^( +)(?:async )?function " + re.escape(name) + r"\([^\n]*\) \{", source, re.M)
        assert match, name
        end = source.index("\n" + match[1] + "}", match.end()) + len(match[1]) + 2
        declarations.append(source[match.start():end])
    script = "const assert = require('node:assert/strict');\n(async () => {\n"
    script += BOOTSTRAP + "\n".join(declarations) + "\n" + assertions
    script += "\n})().catch(error => {console.error(error); process.exitCode=1;});"
    result = subprocess.run(["node", "-e", script], capture_output=True, text=True, timeout=20)
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


def test_browser_log_fallback_and_details_with_synthetic_jobs():
    """Reach the embedded monitor through the real menu and exercise mocked updates."""
    playwright = pytest.importorskip("playwright.sync_api")
    services_html = (SOURCE.parent / "services_monitor.html").read_text(encoding="utf-8")
    for marker, value in {"API_BASE": "/api/services", "BASE_PREFIX": "", "VERSION": "test",
                          "SERIAL": "1", "NAV_HASH": "test"}.items():
        services_html = services_html.replace("%%" + marker + "%%", value)
    fixture_job = {
        "id": "fixture-job", "type": "bitget_best_1m_distributed", "status": "running",
        "payload": {"distributed_hosts": [{"label": "fixture-host"}]}, "progress": {},
    }
    log_line = "2026-10-18 00:00:00 fixture-host segment done coin=BTC pages=2 rows=1 payload=1 KiB minutes_written=1"
    logs = [log_line] * 6001
    requests, errors = [], []
    with playwright.sync_playwright() as driver:
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
            if url.path == "/api/token-refresh" and route.request.method == "POST":
                route.fulfill(json={})
            elif route.request.method != "GET":
                errors.append("Unexpected mutation: " + route.request.method + " " + url.path)
                route.fulfill(status=405)
            elif url.path == "/app/start.html":
                route.fulfill(content_type="text/html", body='''<nav id="topnav"></nav>
                    <script>window.PBGUI_NAV_CONFIG={current:'/',authenticated:true,apiBase:'/api'};</script>
                    <script src="/app/pbgui_nav.js?v=test"></script>''')
            elif url.path == "/api/services/main_page":
                route.fulfill(body=services_html, content_type="text/html")
            elif url.path == "/app/jobs_monitor.html":
                route.fulfill(body=SOURCE.read_text(encoding="utf-8"), content_type="text/html")
            elif url.path.startswith("/app/"):
                asset = SOURCE.parent / url.path.removeprefix("/app/")
                if asset.is_file():
                    mime = "text/css" if asset.suffix == ".css" else "text/javascript"
                    route.fulfill(body=asset.read_bytes(), content_type=mime)
                else:
                    route.fulfill(status=404)
            elif url.path == "/api/services/workers/status":
                route.fulfill(json={"counts": {"total": 1, "running": 1}, "groups": [{
                    "id": "queue", "label": "Queues", "items": [{
                        "id": "market-data-task", "label": "Market Data Queue", "running": True,
                        "can_start": False, "can_stop": False, "monitor_path": "/app/jobs_monitor.html",
                    }],
                }]})
            elif url.path == "/api/jobs/fixture-job/log":
                lines = int(parse_qs(url.query)["lines"][0])
                requests.append(lines)
                route.fulfill(json={"log": logs[-min(lines, 5000):] if lines > 0 else [], "exists": True})
            elif url.path == "/api/jobs/fixture-job":
                route.fulfill(json=fixture_job)
            elif url.path.startswith("/api/"):
                route.fulfill(json={})
            else:
                errors.append("Unexpected request: " + url.path)
                route.fulfill(status=404)

        page.route("**/*", respond)
        page.set_default_timeout(5000)
        page.goto("http://jobs.test/app/start.html")
        page.locator(".nav-group-btn", has_text="System").click()
        page.locator('.nav-item[data-page="system_services"]').click()
        page.locator('button[data-panel="workers"]').click()
        page.locator(".worker-card", has_text="Market Data Queue").click()
        frame_element = page.locator("iframe.worker-monitor-frame")
        playwright.expect(frame_element).to_be_visible()
        assert frame_element.get_attribute("src") == "/app/jobs_monitor.html"
        page = frame_element.element_handle().content_frame()
        assert page is not None
        page.wait_for_function("window.fixtureSocket")
        push_jobs = "jobs => window.fixtureSocket.onmessage({data:JSON.stringify({type:'jobs',data:jobs})})"
        page.evaluate(push_jobs, [fixture_job])
        card = page.locator("#jobs-running .downloader-details")
        playwright.expect(card.locator("summary")).to_contain_text("Recent log excerpt")
        assert requests == [5000]
        card.locator("summary").click()
        playwright.expect(card).to_have_attribute("open", "")
        page.evaluate(push_jobs, [fixture_job])
        playwright.expect(card).to_have_attribute("open", "")
        playwright.expect(page.locator(".tab-btn.active")).to_have_text("Active")

        page.locator('#jobs-running [data-job-action="view"]').click()
        details = page.locator("#details-modal")
        playwright.expect(details).to_have_class("modal active")
        playwright.expect(details.locator(".downloader-details summary")).to_contain_text("Recent log excerpt")
        playwright.expect(details.locator("#details-job-id")).to_have_text("fixture-job")
        details.get_by_role("button", name="Close", exact=True).click()

        fixture_job["progress"] = {"last_result": {"host_results": [{"host": "fixture-host", "rows": 99}]}}
        page.evaluate(push_jobs, [fixture_job])
        playwright.expect(card.locator("summary")).not_to_contain_text("Recent log excerpt")
        playwright.expect(card.locator("summary")).to_contain_text("99 rows")
        playwright.expect(card).to_have_attribute("open", "")
        page.locator('#jobs-running [data-job-action="view"]').click()
        playwright.expect(details.locator(".downloader-details summary")).to_contain_text("99 rows")
        playwright.expect(details.locator(".downloader-details summary")).not_to_contain_text("Recent log excerpt")
        details.get_by_role("button", name="Close", exact=True).click()
        page.locator('#jobs-running [data-job-action="log"]').click()
        playwright.expect(page.locator("#log-content")).to_contain_text("segment done")
        assert requests[-1] == 500
        assert all(lines in {500, 5000} for lines in requests)
        assert not errors
        browser.close()
