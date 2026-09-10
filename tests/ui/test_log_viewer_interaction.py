"""Full-page keyboard interaction with mocked transport and no production writes."""

from collections import deque
from pathlib import Path
from time import perf_counter
from urllib.parse import urlsplit
import json

import pytest


ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def log_interaction_page(request):
    """Run all API Keys scripts with inert API responses and an isolated socket."""
    playwright = pytest.importorskip("playwright.sync_api")
    requested_size = 50000 if "50000" in request.node.name else 10000
    state_profile = "large_periodic_state" in request.node.name
    if request.node.get_closest_marker("local_runtime"):
        path = ROOT / "data/logs/PBGui.log"
        if not path.exists():
            pytest.skip("PBGui.log is not available for read-only diagnostics")
        with path.open(encoding="utf-8", errors="replace") as handle:
            lines = [line.rstrip("\n") for line in deque(handle, maxlen=requested_size)]
        if requested_size == 50000 and lines:
            lines = (lines * ((requested_size + len(lines) - 1) // len(lines)))[-requested_size:]
    else:
        lines = [
            f"2026-09-09T12:00:00.000 [{'ApiKeys' if i % 7 < 4 else 'Status'}] "
            f"[INFO] record-{i} " + "synthetic message " * (
                2 + i % 3 if requested_size == 50000 else 10 + i % 20
            )
            for i in range(requested_size)
        ]
    if len(lines) != requested_size:
        pytest.skip(f"This interaction requires {requested_size:,} available records")
    matches = ["[apikeys]" in line.lower() for line in lines]
    visible = [False] * len(lines)
    for index, match in enumerate(matches):
        if match:
            for visible_index in range(max(0, index - 5), min(len(lines), index + 6)):
                visible[visible_index] = True
    block_count = sum(value and (index == 0 or not visible[index - 1]) for index, value in enumerate(visible))
    with playwright.sync_playwright() as runner:
        try:
            browser = runner.chromium.launch(headless=True)
        except playwright.Error as error:
            if "Executable doesn't exist" in str(error):
                pytest.skip("Offline Chromium installation is required")
            raise
        try:
            page = browser.new_page(viewport={"width": 1440, "height": 1000})
            page.set_default_timeout(5000)
            error_count = []
            page.on("pageerror", lambda _error: error_count.append(1))
            if "back_releases_50000" in request.node.name:
                page.add_init_script("""(() => {
                    const profile=window.lifecycleProfile={registrations:[],runs:{},fetches:[],
                        eventSource:{messages:0,bytes:0,errors:0},webSocket:{messages:0,bytes:0,types:{}}};
                    for (const name of ['setTimeout','setInterval']) {
                        const original=window[name];
                        window[name]=function(callback,delay,...args) {
                            const label=typeof callback === 'function'
                                ? (callback.name || String(callback).slice(0,80)) : String(callback).slice(0,80);
                            profile.registrations.push({kind:name,delay:Number(delay)||0,label});
                            if (typeof callback !== 'function') return original.call(this,callback,delay,...args);
                            return original.call(this,function(...callbackArgs) {
                                const started=performance.now();
                                try { return callback.apply(this,callbackArgs); }
                                finally {
                                    const key=name+':'+String(Number(delay)||0)+':'+label;
                                    const entry=profile.runs[key]||(profile.runs[key]={count:0,max:0,total:0});
                                    const elapsed=performance.now()-started;
                                    entry.count++; entry.max=Math.max(entry.max,elapsed); entry.total+=elapsed;
                                }
                            },delay,...args);
                        };
                    }
                    const originalFetch=window.fetch;
                    window.fetch=function(input,options) {
                        const started=performance.now();
                        let path='invalid';
                        try { path=new URL(typeof input === 'string' ? input : input.url,location.href).pathname; } catch (_) {}
                        return originalFetch.apply(this,arguments).then(response => {
                            profile.fetches.push({path,status:response.status,ms:performance.now()-started});
                            return response;
                        });
                    };
                    const NativeEventSource=window.EventSource;
                    window.EventSource=class extends NativeEventSource {
                        constructor(...args) {
                            super(...args);
                            this.addEventListener('message',event => {
                                profile.eventSource.messages++; profile.eventSource.bytes+=String(event.data||'').length;
                            });
                            this.addEventListener('error',() => { profile.eventSource.errors++; });
                        }
                    };
                    const NativeWebSocket=window.WebSocket;
                    window.WebSocket=class extends NativeWebSocket {
                        constructor(...args) {
                            super(...args);
                            this.addEventListener('message',event => {
                                const raw=String(event.data||'');
                                profile.webSocket.messages++; profile.webSocket.bytes+=raw.length;
                                let type='invalid'; try { type=JSON.parse(raw).type||'unknown'; } catch (_) {}
                                profile.webSocket.types[type]=(profile.webSocket.types[type]||0)+1;
                            });
                        }
                    };
                })()""")

            def serve(route):
                """Fulfill only local source assets and fixed empty API responses."""
                path = urlsplit(route.request.url).path
                if path == "/api/api-keys/editor":
                    route.fulfill(path=ROOT / "frontend/api_keys_editor.html", content_type="text/html")
                elif path.startswith("/app/"):
                    asset = (ROOT / "frontend" / path.removeprefix("/app/")).resolve()
                    if asset.is_relative_to(ROOT / "frontend") and asset.is_file():
                        route.fulfill(path=asset)
                    else:
                        route.abort()
                else:
                    payload = [] if path in {"/api/api-keys/", "/api/vps/alerts", "/api/docs/index"} else {}
                    route.fulfill(json=payload)

            page.route("**/*", serve)
            pending = []
            subscriptions = []

            def connect(socket):
                """Never connect to a real WebSocket server."""

                def receive(raw):
                    """Defer the large snapshot until the keyboard has focus."""
                    message = json.loads(raw)
                    if message.get("cmd") == "list_local_logs":
                        socket.send(json.dumps({"type": "local_logs_list", "files": ["PBGui.log"]}))
                    elif message.get("cmd") == "subscribe_local_logs":
                        subscriptions.append(message["lines"])
                        if state_profile and message.get("filter"):
                            if message["lines"] >= 1000:
                                pending.append((socket, message))
                            else:
                                selected = lines[-message["lines"]:]
                                compact = [
                                    {"line": line, "line_no": index + 1, "match": True}
                                    for index, line in enumerate(selected)
                                    if "[ApiKeys]" in line
                                ]
                                socket.send(json.dumps({
                                    "type": "local_logs_filtered", "file": "PBGui.log", "sid": message["sid"],
                                    "records": compact, "streaming": True, "source_start": 1,
                                    "source_end": message["lines"], "match_count": len(compact),
                                }))
                            return
                        response = json.dumps({
                            "type": "local_logs", "file": "PBGui.log", "sid": message["sid"],
                            "streaming": True, "lines": lines[-message["lines"]:],
                        })
                        if message["lines"] >= 10000:
                            pending.append((socket, response))
                        else:
                            socket.send(response)
                    elif message.get("cmd") == "get_local_logs" and message.get("purpose") == "download":
                        socket.send(json.dumps({
                            "type": "local_logs", "file": "PBGui.log", "sid": message["sid"],
                            "purpose": "download", "request_id": message["request_id"],
                            "lines": lines[-message["lines"]:],
                        }))

                socket.on_message(receive)

            page.route_web_socket("**/*", connect)
            page.goto("http://pbgui.test/api/api-keys/editor")
            page.wait_for_timeout(300)
            page.locator("#btnLogs").click()
            if state_profile:
                page.wait_for_function(
                    "typeof _logViewer !== 'undefined' && _logViewer._serverFiltered "
                    "&& !_logViewer._fullRenderPending && _logViewer._q('match-count').textContent"
                )
            else:
                page.wait_for_function(
                    "typeof _logViewer !== 'undefined' && _logViewer._lines.length === 200 "
                    "&& !_logViewer._fullRenderPending && _logViewer._q('match-count').textContent"
                )
            search = page.locator("#logViewerTarget-lvp-search")
            assert search.input_value() == "[ApiKeys]"
            assert page.locator("#logViewerTarget-lvp-filter-chk").is_checked()
            assert page.evaluate("_logViewer._searchTerm") == "[ApiKeys]"
            page.evaluate("""() => {
                window.panel = _logViewer;
                window.interaction = {completed:-1, during:0, inputs:0, maxGap:0};
                const original = panel._updateMatchCount;
                panel._updateMatchCount = function(...args) {
                    original.apply(this,args); interaction.completed = this._searchAbort;
                };
                panel._q('search').addEventListener('input', () => {
                    interaction.inputs++;
                    if (panel._fullRenderPending) interaction.during++;
                }, true);
                let last = performance.now();
                window.resetInteractionHeartbeat = () => { last=performance.now(); interaction.maxGap=0; };
                setInterval(() => {
                    const now=performance.now(); interaction.maxGap=Math.max(interaction.maxGap,now-last); last=now;
                },20);
            }""")
            page.evaluate(
                "values => { window.expectedMatches=values.matches; window.expectedBlocks=values.blocks; }",
                {"matches": sum(matches), "blocks": block_count},
            )
            yield page, pending, subscriptions
            assert not error_count, "Unexpected browser errors (payloads intentionally omitted)"
        finally:
            browser.close()


def _exercise_filter_keyboard(page, pending, subscriptions):
    """Type using real key events during rendering and after search completion."""
    field = page.locator("#logViewerTarget-lvp-search")
    field.fill("")
    page.wait_for_timeout(350)
    page.evaluate("interaction.completed=-1; interaction.during=0; interaction.inputs=0; interaction.maxGap=0")
    field.click()
    field.press_sequentially("api", delay=40)
    page.wait_for_timeout(350)
    page.wait_for_function("interaction.completed === panel._searchAbort")
    page.locator("#logViewerTarget-lvp-lines-sel").select_option("10000")
    field.click()
    field.press("End")
    assert len(pending) == 1
    socket, response = pending.pop()
    started = perf_counter()
    socket.send(response)
    field.press_sequentially("keys", delay=80)
    during_ms = (perf_counter() - started) * 1000
    assert field.input_value() == "apikeys"
    assert page.evaluate("interaction.during") > 0, "No keystroke overlapped snapshot rendering"
    page.wait_for_timeout(350)
    page.wait_for_function(
        "panel._lines.length === 10000 && !panel._fullRenderPending && interaction.completed === panel._searchAbort",
        timeout=20000,
    )
    started = perf_counter()
    field.click()
    field.press("ControlOrMeta+A")
    field.press_sequentially("api", delay=40)
    after_ms = (perf_counter() - started) * 1000
    assert field.input_value() == "api"
    page.wait_for_timeout(350)
    page.wait_for_function("interaction.completed === panel._searchAbort", timeout=20000)
    metrics = page.evaluate("({...interaction, rows:panel._q('terminal').childElementCount, modelLines:panel._lines.length})")
    assert metrics["inputs"] == 10, metrics
    assert metrics["modelLines"] == 10000, metrics
    assert metrics["rows"] < 10000, metrics
    assert during_ms < 3000 and after_ms < 1500, {"during_ms": during_ms, "after_ms": after_ms}
    assert metrics["maxGap"] < 1000, metrics
    assert subscriptions == [200, 10000], subscriptions
    return {"during_ms": round(during_ms), "after_ms": round(after_ms), **metrics}


def test_keyboard_edit_during_and_after_10000_line_selection(log_interaction_page):
    """Selecting 10k rows must not disable typing or overwrite typed characters."""
    _exercise_filter_keyboard(*log_interaction_page)


def _exercise_default_filter_50000(page, pending, subscriptions):
    """Load 50k lines under the exact default filter and measure browser responsiveness."""
    field = page.locator("#logViewerTarget-lvp-search")
    assert field.input_value() == "[ApiKeys]"
    session = page.context.new_cdp_session(page)
    session.send("Emulation.setCPUThrottlingRate", {"rate": 4})
    page.set_viewport_size({"width": 4000, "height": 2500})
    page.evaluate("interaction.completed=-1; interaction.during=0; interaction.inputs=0; interaction.maxGap=0")
    page.locator("#logViewerTarget-lvp-lines-sel").select_option("50000")
    assert len(pending) == 1
    socket, response = pending.pop()
    socket.send(response)
    page.evaluate("resetInteractionHeartbeat()")

    latencies = []
    for key in ("End", "x", "Backspace"):
        started = perf_counter()
        field.press(key)
        latencies.append(round((perf_counter() - started) * 1000))
    assert field.input_value() == "[ApiKeys]"
    page.wait_for_timeout(350)
    page.wait_for_function(
        "panel._lines.length === 50000 && !panel._fullRenderPending "
        "&& interaction.completed === panel._searchAbort",
        timeout=30000,
    )
    metrics = page.evaluate("""() => ({...interaction,
        rows:panel._q('terminal').childElementCount,
        modelLines:panel._lines.length,
        matches:panel._filteredMatchCount,
        blocks:panel._filteredBlocks.length})""")
    expected = page.evaluate("""() => {
        const re=panel._getSearchRe();
        const bitmap=Uint8Array.from(panel._lines, line => {
            re.lastIndex=0; return re.test(panel._stripAnsi(line)) ? 1 : 0;
        });
        return {matches:bitmap.reduce((total,value) => total + value,0),
            blocks:panel._computeBlocks(bitmap,bitmap.length,panel._contextLines).blocks.length};
    }""")
    assert metrics["modelLines"] == 50000
    assert metrics["matches"] == expected["matches"]
    assert metrics["blocks"] == expected["blocks"]
    assert metrics["rows"] == (metrics["blocks"] * 2 - 1 if metrics["blocks"] else 0)
    assert metrics["rows"] < 5000, metrics
    assert max(latencies) < 500, latencies
    assert metrics["maxGap"] < 500, metrics

    page.evaluate("resetInteractionHeartbeat()")
    page.locator("#logViewerTarget-lvp-expand-all").click()
    page.wait_for_timeout(1000)
    started = perf_counter()
    field.press("ArrowLeft")
    expand_key_ms = round((perf_counter() - started) * 1000)
    page.locator("#logViewerTarget-lvp-collapse-all").click()
    page.wait_for_function("!panel._fullRenderPending", timeout=10000)
    expansion = page.evaluate("({maxGap:interaction.maxGap, rows:panel._q('terminal').childElementCount})")
    assert expand_key_ms < 500, expand_key_ms
    assert expansion["maxGap"] < 500, expansion
    assert expansion["rows"] == metrics["rows"], expansion
    assert subscriptions == [200, 50000]
    return {"key_latency_ms": latencies, "expand_key_ms": expand_key_ms, "expansion": expansion, **metrics}


def test_default_filter_keeps_50000_line_snapshot_responsive(log_interaction_page):
    """The default API Keys filter must not create a 50k-row intermediate DOM."""
    _exercise_default_filter_50000(*log_interaction_page)


@pytest.mark.local_runtime
def test_readonly_actual_log_50000_default_filter(log_interaction_page):
    """Measure a 50k isolated replay built only from read-only actual log records."""
    metrics = _exercise_default_filter_50000(*log_interaction_page)
    print(json.dumps(metrics))


@pytest.mark.local_runtime
def test_readonly_profile_periodic_50000_updates(log_interaction_page):
    """Profile periodic state, list, and live-line traffic against a 50k filtered model."""
    page, pending, _subscriptions = log_interaction_page
    page.set_viewport_size({"width": 4000, "height": 2500})
    session = page.context.new_cdp_session(page)
    session.send("Emulation.setCPUThrottlingRate", {"rate": 4})
    page.locator("#logViewerTarget-lvp-lines-sel").select_option("50000")
    socket, response = pending.pop()
    socket.send(response)
    page.wait_for_function("panel._lines.length === 50000 && !panel._fullRenderPending", timeout=30000)
    page.evaluate("""() => {
        interaction.maxGap=0;
        window.profileCalls={};
        ['_handleMsg','_updateHostDropdown','_updateFileList','_ingestLines','_appendLines','_renderFilteredFromModel']
            .forEach(name => {
                const original=panel[name];
                panel[name]=function(...args) {
                    profileCalls[name]=(profileCalls[name]||0)+1;
                    return original.apply(this,args);
                };
            });
        window.longTasks=[];
        new PerformanceObserver(list => list.getEntries().forEach(entry => longTasks.push(Math.round(entry.duration))))
            .observe({entryTypes:['longtask']});
        resetInteractionHeartbeat();
    }""")
    session.send("Profiler.enable")
    session.send("Profiler.start")
    sid = page.evaluate("panel._sid")
    key_latencies = []
    for tick in range(32):
        socket.send(json.dumps({"type": "state", "data": {"local_logs": ["PBGui.log"], "connections": {}}}))
        if tick % 5 == 0:
            socket.send(json.dumps({"type": "local_logs_list", "files": ["PBGui.log"]}))
        tag = "ApiKeys" if tick % 8 == 0 else "Status"
        socket.send(json.dumps({
            "type": "local_log_lines", "sid": sid,
            "lines": [f"[INFO] [{tag}] periodic-record-{tick}"],
        }))
        if tick % 4 == 0:
            started = perf_counter()
            page.locator("#logViewerTarget-lvp-search").press("ArrowLeft")
            key_latencies.append(round((perf_counter() - started) * 1000))
        page.wait_for_timeout(1000)
    profile = session.send("Profiler.stop")["profile"]
    metrics = page.evaluate("""() => ({calls:profileCalls, maxGap:interaction.maxGap,
        maxLongTask:longTasks.length ? Math.max(...longTasks) : 0,
        longTaskCount:longTasks.length, rows:panel._q('terminal').childElementCount,
        modelLines:panel._lines.length, matches:panel._filteredMatchCount,
        expectedMatches:panel._lines.reduce((count,line) => count + (panel._stripAnsi(line).includes('[ApiKeys]') ? 1 : 0),0)})""")
    metrics["keyLatencies"] = key_latencies
    sampled = sorted(
        (
            (node.get("hitCount", 0), node.get("callFrame", {}).get("functionName", ""))
            for node in profile.get("nodes", [])
            if node.get("hitCount", 0) and node.get("callFrame", {}).get("functionName")
        ),
        reverse=True,
    )[:8]
    print(json.dumps({"metrics": metrics, "sampledFunctions": sampled}))
    assert metrics["calls"].get("_renderFilteredFromModel", 0) <= 2, metrics
    assert metrics["matches"] == metrics["expectedMatches"], metrics
    assert metrics["maxLongTask"] < 500, metrics
    assert max(key_latencies) < 500, metrics


@pytest.mark.local_runtime
def test_server_filter_keeps_50000_raw_lines_out_of_browser_model(log_interaction_page):
    """Filtered snapshots stay compact, preserve stale display, and restore raw mode on clear."""
    page, pending, _subscriptions = log_interaction_page
    page.locator("#logViewerTarget-lvp-lines-sel").select_option("50000")
    socket, raw_response = pending.pop()
    raw_response = json.loads(raw_response)
    source_lines = raw_response["lines"]
    matches = ["[ApiKeys]" in line for line in source_lines]
    visible = [False] * len(source_lines)
    for index, matched in enumerate(matches):
        if matched:
            for visible_index in range(max(0, index - 5), min(len(source_lines), index + 6)):
                visible[visible_index] = True
    records = [
        {"line": line, "line_no": index + 1, "match": matches[index]}
        for index, line in enumerate(source_lines)
        if visible[index]
    ]
    socket.send(json.dumps({
        "type": "local_logs_filtered", "file": "PBGui.log", "sid": raw_response["sid"],
        "records": records, "streaming": True, "file_size": 123456,
        "source_start": 1, "source_end": 50000, "match_count": sum(matches),
    }))
    page.wait_for_function("panel._serverFiltered && !panel._fullRenderPending")
    compact = page.evaluate("({model:panel._lines.length, records:panel._serverRecords.length, matches:panel._filteredMatchCount})")
    assert compact["model"] == len(records) < 50000
    assert compact["records"] == len(records)
    assert compact["matches"] == sum(matches)

    with page.expect_download() as download_info:
        page.locator("#logViewerTarget-lvp-dl-btn").click()
    downloaded = download_info.value.path()
    assert downloaded is not None
    assert downloaded.stat().st_size == len("\n".join(source_lines).encode("utf-8"))
    assert page.evaluate("panel._serverFiltered && panel._lines.length") == len(records)

    page.locator("#logViewerTarget-lvp-search").fill("")
    page.wait_for_timeout(220)
    assert len(pending) == 1
    assert page.evaluate("panel._serverFiltered && panel._lines.length") == len(records)
    _raw_socket, clear_response = pending.pop()
    _raw_socket.send(clear_response)
    page.wait_for_function("!panel._serverFiltered && panel._lines.length === 50000 && !panel._fullRenderPending", timeout=30000)


@pytest.mark.local_runtime
def test_readonly_actual_log_keyboard_interaction(log_interaction_page):
    """Opt-in actual-log replay emits only aggregate timings, never log contents."""
    metrics = _exercise_filter_keyboard(*log_interaction_page)
    print(json.dumps(metrics))


@pytest.mark.local_runtime
def test_readonly_completed_filter_with_sustained_stream_and_keyboard(log_interaction_page):
    """Replay real logs at high resolution, then type with 4x CPU and live messages."""
    page, pending, subscriptions = log_interaction_page
    page.set_viewport_size({"width": 4000, "height": 2500})
    field = page.locator('#logViewerTarget-lvp-search')
    field.fill('api')
    page.wait_for_timeout(350)
    page.wait_for_function("interaction.completed === panel._searchAbort")
    page.locator('#logViewerTarget-lvp-lines-sel').select_option('10000')
    socket, response = pending.pop()
    socket.send(response)
    page.wait_for_function("panel._lines.length === 10000 && !panel._fullRenderPending && interaction.completed === panel._searchAbort", timeout=20000)
    initial = page.evaluate("""() => ({matches:panel._filteredMatchCount,
        blocks:panel._q('terminal').querySelectorAll('.lvp-group-first').length,
        domRows:panel._q('terminal').childElementCount,
        detailMarks:panel._q('terminal').querySelectorAll('.lvp-grp-detail mark').length})""")
    assert initial['detailMarks'] == 0, initial
    session = page.context.new_cdp_session(page)
    session.send('Emulation.setCPUThrottlingRate', {'rate': 4})
    page.evaluate('interaction.maxGap=0')
    sid = page.evaluate('panel._sid')
    durations = []
    for tick in range(35):
        socket.send(json.dumps({'type':'state', 'data':{'local_logs':['PBGui.log'], 'connections':{}}}))
        socket.send(json.dumps({'type':'local_log_lines', 'sid':sid,
            'lines':[f'[INFO] [ApiKeys] synthetic live record-{tick} ' + 'x' * 700]}))
        if tick % 5 == 0:
            started = perf_counter()
            field.click()
            field.press('End')
            field.press('k')
            assert field.input_value() == 'apik'
            page.wait_for_timeout(400)
            field.press('Backspace')
            assert field.input_value() == 'api'
            durations.append(round((perf_counter()-started)*1000))
        page.wait_for_timeout(1000)
    metrics = page.evaluate("({...interaction, rows:panel._q('terminal').childElementCount, "
                            "modelLines:panel._lines.length, streamOrdered:panel._lines.at(-1).includes('record-34')})")
    assert metrics['modelLines'] == 10000, metrics
    assert metrics['rows'] < metrics['modelLines'], metrics
    assert metrics['streamOrdered'], metrics
    assert metrics['maxGap'] < 1500, metrics
    assert max(durations) < 2500, durations
    assert subscriptions == [200, 10000]
    print(json.dumps({'initial':initial,'steady_state':metrics,'key_roundtrips_ms':durations}))


@pytest.mark.local_runtime
def test_readonly_profile_large_periodic_state_while_lines_select_is_active(log_interaction_page):
    """Keep the Lines control responsive during realistic multi-megabyte state cycles."""
    page, pending, _subscriptions = log_interaction_page
    page.set_viewport_size({"width": 4000, "height": 2500})
    session = page.context.new_cdp_session(page)
    session.send("Emulation.setCPUThrottlingRate", {"rate": 4})
    page.locator("#logViewerTarget-lvp-lines-sel").select_option("25000")
    socket, subscribe_request = pending.pop()
    records = [
        {"line": f"[INFO] [ApiKeys] profile record {index}", "line_no": index * 200 + 1, "match": True}
        for index in range(100)
    ]
    socket.send(json.dumps({
        "type": "local_logs_filtered", "file": "PBGui.log", "sid": subscribe_request["sid"],
        "records": records, "streaming": True, "source_start": 1, "source_end": 25000,
        "match_count": len(records),
    }))
    page.wait_for_function("panel._serverFiltered && !panel._fullRenderPending", timeout=30000)
    page.evaluate("""() => {
        window.stateProfile={calls:0,bytes:0,maxParse:0,maxHandle:0};
        const parse=JSON.parse;
        JSON.parse=function(raw) {
            const started=performance.now();
            const value=parse.call(this,raw);
            const elapsed=performance.now()-started;
            if (value && value.type === 'state') {
                stateProfile.calls++;
                stateProfile.bytes+=String(raw).length;
                stateProfile.maxParse=Math.max(stateProfile.maxParse,elapsed);
            }
            return value;
        };
        const handle=panel._handleMsg;
        panel._handleMsg=function(message) {
            const started=performance.now();
            const result=handle.call(this,message);
            if (message && message.type === 'state')
                stateProfile.maxHandle=Math.max(stateProfile.maxHandle,performance.now()-started);
            return result;
        };
        window.stateLongTasks=[];
        new PerformanceObserver(list => list.getEntries().forEach(entry => stateLongTasks.push(entry.duration)))
            .observe({entryTypes:['longtask']});
    }""")
    synthetic_rows = [
        {
            "u": f"instance-{index}", "p": "8", "c": index % 100,
            "m": [index, 0, 0, 0, 0, 0, 0, 0, 0, index % 4096],
            "detail": "x" * 400,
        }
        for index in range(30000)
    ]
    state_payload = json.dumps({
        "type": "state",
        "data": {
            "connections": {f"host-{index}": {"connected": True} for index in range(64)},
            "system": {}, "instances": {"host-0": synthetic_rows},
            "v7_instances": {}, "v8_instances": {}, "host_meta": {}, "streams": {},
            "services": {}, "bot_logs": {}, "local_logs": ["PBGui.log"], "ui_settings": {},
        },
    })
    select = page.locator("#logViewerTarget-lvp-lines-sel")
    socket_url = page.evaluate("panel._ws.url")
    state_enabled = "state=0" not in socket_url
    select_latencies = []
    transmitted_state_frames = 0
    transmitted_state_bytes = 0
    local_frames = 0
    local_bytes = 0
    for tick in range(16):
        if state_enabled:
            socket.send(state_payload)
            transmitted_state_frames += 1
            transmitted_state_bytes += len(state_payload.encode("utf-8"))
        sid = page.evaluate("panel._sid")
        local_payload = json.dumps({
            "type": "local_log_filtered_lines", "sid": sid, "records": [],
            "source_start": tick + 1, "source_end": 25000 + tick + 1,
            "match_count": len(records),
        })
        socket.send(local_payload)
        local_frames += 1
        local_bytes += len(local_payload.encode("utf-8"))
        started = perf_counter()
        select.click()
        select.press("ArrowDown" if tick % 2 == 0 else "ArrowUp")
        select.press("Enter")
        select_latencies.append(round((perf_counter() - started) * 1000))
        page.wait_for_timeout(50)
        if pending:
            response_socket, request_message = pending.pop()
            response_socket.send(json.dumps({
                "type": "local_logs_filtered", "file": "PBGui.log", "sid": request_message["sid"],
                "records": records, "streaming": True, "source_start": 1,
                "source_end": request_message["lines"], "match_count": len(records),
            }))
        page.wait_for_timeout(2000)
    metrics = page.evaluate("""() => ({...stateProfile,
        maxLongTask:stateLongTasks.length ? Math.max(...stateLongTasks) : 0,
        longTaskCount:stateLongTasks.length})""")
    metrics["frameBytes"] = len(state_payload.encode("utf-8"))
    metrics["stateCycles"] = 16
    metrics["transmittedStateFrames"] = transmitted_state_frames
    metrics["transmittedStateBytes"] = transmitted_state_bytes
    metrics["localFrames"] = local_frames
    metrics["localBytes"] = local_bytes
    metrics["selectLatencies"] = select_latencies
    print(json.dumps(metrics))
    assert "state=0" in socket_url
    assert metrics["calls"] == 0
    assert transmitted_state_frames == 0
    assert transmitted_state_bytes == 0
    assert local_frames == 16 and local_bytes > 0
    assert metrics["maxParse"] < 250, metrics
    assert metrics["maxLongTask"] < 250, metrics
    assert max(select_latencies) < 250, metrics


@pytest.mark.local_runtime
def test_readonly_back_releases_50000_log_viewer_lifecycle(log_interaction_page):
    """Back must release the hidden 50k viewer and remain responsive for eight five-second cycles."""
    page, pending, subscriptions = log_interaction_page
    session = page.context.new_cdp_session(page)
    session.send("Emulation.setCPUThrottlingRate", {"rate": 4})
    page.locator("#logViewerTarget-lvp-lines-sel").select_option("50000")
    socket, response = pending.pop()
    socket.send(response)
    page.wait_for_function("panel._lines.length === 50000 && !panel._fullRenderPending", timeout=30000)
    session.send("HeapProfiler.enable")
    session.send("HeapProfiler.collectGarbage")
    heap_before = session.send("Runtime.getHeapUsage")
    page.evaluate("""() => {
        window.lifecycleLongTasks=[];
        window.afterBackMessages=0;
        const handleMessage=panel._handleMsg;
        panel._handleMsg=function(message) {
            if (!logPanelVisible) afterBackMessages++;
            return handleMessage.call(this,message);
        };
        new PerformanceObserver(list => list.getEntries().forEach(entry => lifecycleLongTasks.push(entry.duration)))
            .observe({entryTypes:['longtask']});
        interaction.maxGap=0; resetInteractionHeartbeat();
    }""")
    page.locator("#logPanel > div:first-child > button").click()
    page.wait_for_function("!logPanelVisible && document.getElementById('userListView').style.display !== 'none'")
    closed = page.evaluate("""() => ({closed:panel._closed, socket:panel._ws === null,
        lines:panel._lines.length, snapshot:panel._filteredSnapshot.length,
        records:panel._serverRecords.length, pending:panel._pending.length,
        dom:panel._q('terminal').childElementCount})""")
    session.send("Profiler.enable")
    session.send("Profiler.start")
    interaction_latencies = []
    user_filter = page.locator("#userFilter")
    for tick in range(8):
        page.wait_for_timeout(5000)
        try:
            socket.send(json.dumps({
                "type": "local_log_filtered_lines", "sid": page.evaluate("panel._sid"),
                "records": [], "source_start": tick + 1, "source_end": 50000 + tick,
                "match_count": 0,
            }))
        except Exception:
            pass
        started = perf_counter()
        user_filter.press("End")
        user_filter.press("x")
        user_filter.press("Backspace")
        interaction_latencies.append(round((perf_counter() - started) * 1000))
    cpu_profile = session.send("Profiler.stop")["profile"]
    session.send("HeapProfiler.collectGarbage")
    heap_after = session.send("Runtime.getHeapUsage")
    metrics = page.evaluate("""() => ({profile:lifecycleProfile,maxGap:interaction.maxGap,
        afterBackMessages,
        maxLongTask:lifecycleLongTasks.length ? Math.max(...lifecycleLongTasks) : 0,
        longTaskCount:lifecycleLongTasks.length})""")
    sampled = sorted(
        (
            (node.get("hitCount", 0), node.get("callFrame", {}).get("functionName", ""))
            for node in cpu_profile.get("nodes", [])
            if node.get("hitCount", 0) and node.get("callFrame", {}).get("functionName")
        ),
        reverse=True,
    )[:10]
    result = {
        "closed": closed,
        "heapBefore": heap_before.get("usedSize", 0),
        "heapAfter": heap_after.get("usedSize", 0),
        "maxHeartbeatGap": metrics["maxGap"],
        "maxLongTask": metrics["maxLongTask"],
        "longTaskCount": metrics["longTaskCount"],
        "afterBackMessages": metrics["afterBackMessages"],
        "interactionLatencies": interaction_latencies,
        "timerDelays": sorted({entry["delay"] for entry in metrics["profile"]["registrations"]}),
        "timerRuns": metrics["profile"]["runs"],
        "fetches": metrics["profile"]["fetches"],
        "eventSource": metrics["profile"]["eventSource"],
        "webSocket": metrics["profile"]["webSocket"],
        "sampledFunctions": sampled,
    }
    print(json.dumps(result))
    assert closed == {"closed": True, "socket": True, "lines": 0, "snapshot": 0,
                      "records": 0, "pending": 0, "dom": 0}, result
    assert metrics["maxLongTask"] < 250, result
    assert metrics["maxGap"] < 250, result
    assert max(interaction_latencies) < 250, result
    assert metrics["afterBackMessages"] == 0, result

    page.locator("#btnLogs").click()
    page.wait_for_timeout(100)
    assert page.locator("#logViewerTarget-lvp-lines-sel").input_value() == "50000"
    assert len(pending) == 1
    reopen_socket, reopen_response = pending.pop()
    reopen_socket.send(reopen_response)
    page.wait_for_function("panel._lines.length === 50000 && !panel._fullRenderPending", timeout=30000)
    page.evaluate("panel._renderFilteredFromModel(); backToList()")
    page.wait_for_timeout(250)
    reopened_closed = page.evaluate("""() => ({closed:panel._closed, socket:panel._ws === null,
        lines:panel._lines.length, snapshot:panel._filteredSnapshot.length,
        records:panel._serverRecords.length, pending:panel._pending.length,
        dom:panel._q('terminal').childElementCount, rendering:panel._fullRenderPending})""")
    assert reopened_closed == {"closed": True, "socket": True, "lines": 0, "snapshot": 0,
                               "records": 0, "pending": 0, "dom": 0, "rendering": False}
    assert subscriptions == [200, 50000, 50000]
