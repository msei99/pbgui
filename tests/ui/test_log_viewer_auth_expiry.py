"""Shared log viewer and cookie-authenticated navigation regressions."""

from __future__ import annotations

import re
import subprocess
import textwrap
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
LOG_VIEWER = ROOT / "frontend" / "js" / "log_viewer_panel.js"
NAV = ROOT / "frontend" / "pbgui_nav.js"


def test_log_viewer_close_4001_is_terminal_and_redirects() -> None:
    """A rejected shared viewer socket must never reconnect, even if reopened."""

    source = LOG_VIEWER.read_text(encoding="utf-8")
    connect_start = source.index("    _connect() {")
    connect_method = source[connect_start:source.index("    _disconnect() {", connect_start)]
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        let sockets = [];
        let timers = 0;
        let redirects = [];
        class FakeWebSocket {{
          static OPEN = 1;
          static CLOSING = 2;
          static CLOSED = 3;
          constructor(url) {{ this.url = url; this.readyState = 0; sockets.push(this); }}
          close() {{ this.readyState = FakeWebSocket.CLOSED; }}
          send() {{}}
        }}
        globalThis.WebSocket = FakeWebSocket;
        globalThis.window = {{location: {{replace: value => redirects.push(value)}}}};
        globalThis.setTimeout = function () {{ timers += 1; return timers; }};
        globalThis.clearTimeout = function () {{}};
        class Panel {{
        {connect_method}
          _disconnect() {{}}
        }}
        const panel = Object.create(Panel.prototype);
        panel._wsBase = 'ws://example.test';
        panel._ws = null;
        panel._closed = false;
        panel._authExpired = false;
        panel._reconnectTimer = 0;
        panel._wsGeneration = 0;
        panel._streaming = false;
        panel._pendingRestartCommand = {{cmd: 'restart_service'}};
        panel._q = () => ({{textContent: ''}});
        panel._updateStreamBtn = () => {{}};
        panel._subscribe = () => {{}};
        panel._flushPendingRestart = () => {{}};
        panel._connect();
        assert.equal(sockets.length, 1);
        sockets[0].onclose({{code: 4001}});
        assert.equal(panel._authExpired, true);
        assert.equal(panel._closed, true);
        assert.equal(panel._pendingRestartCommand, null);
        assert.equal(panel._reconnectTimer, 0);
        assert.deepEqual(redirects, ['/']);
        assert.equal(timers, 0);
        panel._connect();
        assert.equal(sockets.length, 1);
        """
    )
    result = subprocess.run(["node", "-e", script], cwd=ROOT, capture_output=True, text=True, check=False)
    assert result.returncode == 0, f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
    assert "open()  { this._closed = false; if (!this._authExpired) this._connect(); }" in source


def test_replaced_log_viewer_socket_callbacks_cannot_mutate_current_state() -> None:
    """Every callback from a disconnected socket must be inert after replacement."""

    source = LOG_VIEWER.read_text(encoding="utf-8")
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const sockets = [];
        const redirects = [];
        let handled = 0;
        let subscriptions = 0;
        let timers = 0;
        class FakeWebSocket {{
          static OPEN = 1;
          static CLOSING = 2;
          static CLOSED = 3;
          constructor(url) {{ this.url = url; this.readyState = 0; this.sent = []; sockets.push(this); }}
          close() {{ this.readyState = FakeWebSocket.CLOSED; }}
          send(raw) {{ this.sent.push(JSON.parse(raw)); }}
        }}
        globalThis.WebSocket = FakeWebSocket;
        globalThis.window = {{location: {{replace: value => redirects.push(value)}}}};
        globalThis.setTimeout = function () {{ timers += 1; return timers; }};
        globalThis.clearTimeout = function () {{}};
        {source}

        const conn = {{textContent: 'initial'}};
        const panel = Object.create(LogViewerPanel.prototype);
        panel._wsBase = 'ws://example.test';
        panel._ws = null;
        panel._wsGeneration = 0;
        panel._closed = false;
        panel._authExpired = false;
        panel._reconnectTimer = 0;
        panel._restartTimeout = 0;
        panel._streaming = false;
        panel._pendingRestartCommand = null;
        panel._restartAttempt = null;
        panel._q = name => name === 'conn' ? conn : null;
        panel._updateStreamBtn = () => {{}};
        panel._handleMsg = () => {{ handled += 1; }};
        panel._subscribe = () => {{ subscriptions += 1; }};
        panel._flushPendingRestart = () => false;

        panel._connect();
        const oldSocket = sockets[0];
        const oldOpen = oldSocket.onopen;
        const oldMessage = oldSocket.onmessage;
        const oldError = oldSocket.onerror;
        const oldClose = oldSocket.onclose;

        panel._connect();
        const currentSocket = sockets[1];
        assert.equal(oldSocket.onopen, null);
        assert.equal(oldSocket.onmessage, null);
        assert.equal(oldSocket.onerror, null);
        assert.equal(oldSocket.onclose, null);

        oldOpen();
        oldMessage({{data: '{{"type":"state"}}'}});
        oldError();
        oldClose({{code: 4001}});
        assert.equal(panel._ws, currentSocket);
        assert.equal(panel._authExpired, false);
        assert.equal(panel._closed, false);
        assert.equal(panel._streaming, false);
        assert.equal(conn.textContent, 'initial');
        assert.equal(handled, 0);
        assert.equal(subscriptions, 0);
        assert.equal(timers, 0);
        assert.deepEqual(redirects, []);

        currentSocket.readyState = FakeWebSocket.OPEN;
        currentSocket.onopen();
        assert.equal(conn.textContent, 'connected');
        assert.equal(subscriptions, 1);
        assert.deepEqual(currentSocket.sent, [{{cmd: 'list_local_logs'}}]);
        """
    )
    result = subprocess.run(["node", "-e", script], cwd=ROOT, capture_output=True, text=True, check=False)
    assert result.returncode == 0, f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"


def test_log_viewer_buffers_and_dom_keep_the_newest_configured_lines() -> None:
    """Snapshots, RAF queues, and rendered nodes must remain bounded at the newest end."""

    source = LOG_VIEWER.read_text(encoding="utf-8")
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const frames = [];
        globalThis.window = {{}};
        globalThis.WebSocket = {{OPEN: 1, CLOSING: 2, CLOSED: 3}};
        globalThis.requestAnimationFrame = callback => {{ frames.push(callback); return frames.length; }};
        globalThis.document = {{
          createDocumentFragment: () => ({{
            children: [],
            appendChild(node) {{ this.children.push(node); }}
          }})
        }};
        {source}

        const terminal = {{
          children: [],
          scrollTop: 0,
          clientHeight: 10,
          scrollHeight: 10,
          get childElementCount() {{ return this.children.length; }},
          get firstChild() {{ return this.children[0]; }},
          appendChild(node) {{
            if (Array.isArray(node.children)) this.children.push(...node.children);
            else this.children.push(node);
            this.scrollHeight = this.children.length;
          }},
          removeChild(node) {{
            assert.equal(node, this.children[0]);
            this.children.shift();
          }}
        }};
        const panel = Object.create(LogViewerPanel.prototype);
        panel._MAX = 3;
        panel._lines = [];
        panel._lineBase = 0;
        panel._pending = [];
        panel._rafPending = false;
        panel._normalizeIncomingLines = lines => lines.slice();
        panel._buildDiv = (line, num) => ({{line, num}});
        panel._q = name => name === 'terminal' ? terminal : null;
        panel._updateMatchCount = () => {{}};

        panel._replaceLines(['snapshot-1', 'snapshot-2', 'snapshot-3', 'snapshot-4']);
        assert.deepEqual(panel._lines, ['snapshot-2', 'snapshot-3', 'snapshot-4']);
        assert.equal(panel._lineBase, 1);

        panel._lines = [];
        panel._lineBase = 0;
        panel._ingestLines(['line-1', 'line-2', 'line-3', 'line-4', 'line-5']);
        panel._ingestLines(['line-6', 'line-7']);
        assert.deepEqual(panel._lines, ['line-5', 'line-6', 'line-7']);
        assert.deepEqual(panel._pending.map(item => item.line), ['line-5', 'line-6', 'line-7']);
        assert.equal(frames.length, 1);

        frames.shift()();
        assert.deepEqual(terminal.children.map(item => item.line), ['line-5', 'line-6', 'line-7']);
        assert.equal(terminal.childElementCount, 3);

        panel._q = () => ({{value: '999999'}});
        assert.equal(panel._getLines(), LogViewerPanel.MAX_LINES);
        panel._q = () => ({{value: '0'}});
        assert.equal(panel._getLines(), 200);
        """
    )
    result = subprocess.run(["node", "-e", script], cwd=ROOT, capture_output=True, text=True, check=False)
    assert result.returncode == 0, f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
    assert '<option value="50000">Max (50,000)</option>' in source
    assert '<option value="0">All</option>' not in source


def test_chunked_full_render_defers_live_lines_until_frozen_snapshot_finishes() -> None:
    """Lines arriving between RAF chunks remain ordered, unique, and bounded in the DOM."""

    source = LOG_VIEWER.read_text(encoding="utf-8")
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const frames = [];
        globalThis.window = {{}};
        globalThis.WebSocket = {{OPEN: 1, CLOSING: 2, CLOSED: 3}};
        globalThis.requestAnimationFrame = callback => {{ frames.push(callback); return frames.length; }};
        const terminal = {{
          children: [],
          scrollTop: 0,
          clientHeight: 10,
          scrollHeight: 10,
          get childElementCount() {{ return this.children.length; }},
          get firstChild() {{ return this.children[0]; }},
          get innerHTML() {{ return ''; }},
          set innerHTML(_value) {{ this.children = []; }},
          appendChild(node) {{
            const additions = Array.isArray(node.children) ? node.children : [node];
            additions.forEach(child => {{ child.parent = this; this.children.push(child); }});
            this.scrollHeight = this.children.length;
          }},
          insertBefore(node, reference) {{
            const index = this.children.indexOf(reference);
            assert.notEqual(index, -1);
            const additions = Array.isArray(node.children) ? node.children : [node];
            additions.forEach(child => {{ child.parent = this; }});
            this.children.splice(index, 0, ...additions);
          }},
          removeChild(node) {{
            const index = this.children.indexOf(node);
            assert.notEqual(index, -1);
            this.children.splice(index, 1);
          }}
        }};
        globalThis.document = {{
          createDocumentFragment: () => ({{children: [], appendChild(node) {{ this.children.push(node); }}}}),
          createElement: () => ({{
            style: {{}},
            textContent: '',
            parent: null,
            remove() {{ if (this.parent) this.parent.removeChild(this); }}
          }})
        }};
        {source}

        const panel = Object.create(LogViewerPanel.prototype);
        panel._MAX = 5;
        panel._CHUNK = 2;
        panel._lines = ['line-1', 'line-2', 'line-3', 'line-4', 'line-5'];
        panel._lineBase = 0;
        panel._pending = [];
        panel._rafPending = false;
        panel._fullRenderPending = false;
        panel._renderAbort = 0;
        panel._searchAbort = 0;
        panel._searchTerm = '';
        panel._normalizeIncomingLines = lines => lines.slice();
        panel._buildDiv = (line, num) => ({{line, num}});
        panel._q = name => name === 'terminal' ? terminal : null;
        panel._updateMatchCount = () => {{}};

        panel._renderFull();
        assert.equal(frames.length, 1);
        frames.shift()();
        assert.deepEqual(terminal.children.filter(item => item.line).map(item => item.line), ['line-1', 'line-2']);

        panel._ingestLines(['line-6', 'line-7']);
        assert.deepEqual(panel._lines, ['line-3', 'line-4', 'line-5', 'line-6', 'line-7']);
        assert.deepEqual(panel._pending.map(item => item.line), ['line-6', 'line-7']);
        while (frames.length) frames.shift()();

        assert.deepEqual(terminal.children.map(item => item.line), ['line-3', 'line-4', 'line-5', 'line-6', 'line-7']);
        assert.deepEqual(terminal.children.map(item => item.num), [3, 4, 5, 6, 7]);
        """
    )
    result = subprocess.run(["node", "-e", script], cwd=ROOT, capture_output=True, text=True, check=False)
    assert result.returncode == 0, f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"


def test_large_log_filter_renders_from_the_model_without_full_dom() -> None:
    """Large active filters scan the model and retain only collapsed headers in DOM."""

    source = LOG_VIEWER.read_text(encoding="utf-8")
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const frames = [];
        let contentWrites = 0;
        let displayWrites = 0;
        globalThis.window = {{}};
        globalThis.WebSocket = {{OPEN: 1}};
        globalThis.requestAnimationFrame = callback => {{ frames.push(callback); return frames.length; }};

        function attachClassList(node) {{
          node.classList = {{
            add(name) {{
              const names = new Set(String(node.className || '').split(/\\s+/).filter(Boolean));
              names.add(name); node.className = Array.from(names).join(' ');
            }},
            remove(name) {{
              node.className = String(node.className || '').split(/\\s+/).filter(value => value && value !== name).join(' ');
            }},
            toggle(name, force) {{
              const has = this.contains(name);
              const next = force === undefined ? !has : !!force;
              if (next) this.add(name); else this.remove(name);
              return next;
            }},
            contains(name) {{ return String(node.className || '').split(/\\s+/).includes(name); }}
          }};
          return node;
        }}

        function makeLine(index) {{
          let text = 'match-' + index;
          let display = '';
          const node = attachClassList({{
            className: 'lvp-log-info',
            dataset: {{text, level: 'INFO'}},
            firstChild: null,
            querySelector(selector) {{ return selector === '.grp-arrow' ? this.arrow || null : null; }},
            insertBefore(child) {{ this.arrow = child; this.firstChild = child; }},
            appendChild() {{}}
          }});
          Object.defineProperty(node, 'textContent', {{
            get() {{ return text; }},
            set(value) {{ text = String(value); contentWrites += 1; }}
          }});
          Object.defineProperty(node, 'innerHTML', {{
            get() {{ return text; }},
            set(value) {{ text = String(value); contentWrites += 1; }}
          }});
          node.style = {{}};
          Object.defineProperty(node.style, 'display', {{
            get() {{ return display; }},
            set(value) {{ display = value; displayWrites += 1; }}
          }});
          return node;
        }}

        const modelLines = Array.from({{length: 10000}}, (_, index) => 'match-' + index);
        const lines = [];
        const terminal = attachClassList({{
            className: 'lvp-terminal',
            children: lines,
            scrollTop: 0,
            scrollHeight: 1,
            get childElementCount() {{ return this.children.length; }},
          querySelectorAll(selector) {{
            if (selector === '.lvp-separator') return [];
            if (selector === '.lvp-group-first') return lines.filter(line => line.classList.contains('lvp-group-first'));
            if (selector === '.lvp-group-expanded') return lines.filter(line => line.classList.contains('lvp-group-expanded'));
            if (selector === '.lvp-highlight') return lines.filter(line => line.classList.contains('lvp-highlight'));
            return [];
          }},
            insertBefore() {{ throw new Error('One contiguous block needs no separator'); }},
            appendChild(child) {{
              const added = child.fragmentChildren || [child];
              for (const item of added) {{
                item.isConnected = true;
                item.remove = () => {{
                  const index = this.children.indexOf(item);
                  if (index >= 0) this.children.splice(index, 1);
                  item.isConnected = false;
                }};
                this.children.push(item);
              }}
            }}
        }});
        Object.defineProperty(terminal, 'innerHTML', {{
          get() {{ return ''; }},
          set() {{ this.children.splice(0); }}
        }});
        const controls = {{
          'ctx-sel': {{style: {{}}}},
          'grp-actions': {{style: {{}}}},
          'nav-btns': {{style: {{}}}},
          'match-count': {{textContent: ''}}
        }};
        globalThis.document = {{
          createElement: () => makeLine(0),
          createDocumentFragment: () => ({{
            fragmentChildren: [],
            appendChild(child) {{ this.fragmentChildren.push(child); }}
          }})
        }};
        {source}

        const panel = Object.create(LogViewerPanel.prototype);
        panel._searchTerm = 'match';
        panel._searchRegex = false;
        panel._filterMode = true;
        panel._contextLines = 5;
        panel._blocksCollapsed = true;
        panel._searchAbort = 0;
        panel._searchTimer = null;
        panel._lines = modelLines;
        panel._lineBase = 0;
        panel._renderAbort = 0;
        panel._renderMode = 'full';
        panel._filteredSnapshot = [];
        panel._filteredBlocks = [];
        panel._filteredMatchCount = 0;
        panel._expandedBlocks = new Set();
        panel._fullRenderPending = false;
        panel._pending = [];
        panel._rafPending = false;
        panel._matchEls = [];
        panel._matchIdx = -1;
        panel._SCHUNK = 100;
        panel._MAX = 10000;
        panel._visLevels = new Set(['INFO']);
        panel._q = name => name === 'terminal' ? terminal : controls[name];

        panel._applySearch();
        assert.equal(terminal.childElementCount, 1);
        assert.equal(displayWrites, 0);
        assert.equal(frames.length, 1);

        while (frames.length) frames.shift()();

        assert.equal(controls['match-count'].textContent, '10000 matches in 1 blocks');
        assert.equal(terminal.classList.contains('lvp-groups-collapsed'), true);
        assert.equal(terminal.childElementCount, 1);
        assert.equal(displayWrites, 0);
        """
    )
    result = subprocess.run(["node", "-e", script], cwd=ROOT, capture_output=True, text=True, check=False)
    assert result.returncode == 0, f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"


def test_remote_log_info_uses_subscription_sid_to_reject_delayed_metadata() -> None:
    """A delayed size response from an old selection cannot update the active viewer."""

    source = LOG_VIEWER.read_text(encoding="utf-8")
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        globalThis.window = {{}};
        globalThis.WebSocket = {{OPEN: 1}};
        {source}
        const sent = [];
        const sizes = [];
        const panel = Object.create(LogViewerPanel.prototype);
        panel._host = 'host';
        panel._service = 'PBRun';
        panel._sid = 4;
        panel._streaming = false;
        panel._ws = {{readyState: 1, send: raw => sent.push(JSON.parse(raw))}};
        panel._isLocal = () => false;
        panel._unsubscribe = () => {{}};
        panel._clear = () => {{}};
        panel._getLines = () => 200;
        panel._updateStreamBtn = () => {{}};
        panel._showFileSize = size => sizes.push(size);

        const sid = panel._subscribe();
        assert.equal(sid, 5);
        assert.deepEqual(sent[1], {{cmd: 'get_log_info', host: 'host', service: 'PBRun', sid: 5}});
        panel._handleMsg({{type: 'log_info', sid: 4, size: 10}});
        panel._handleMsg({{type: 'log_info', sid: 5, size: 20}});
        assert.deepEqual(sizes, [20]);
        """
    )
    result = subprocess.run(["node", "-e", script], cwd=ROOT, capture_output=True, text=True, check=False)
    assert result.returncode == 0, f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"


def test_server_filtered_local_records_remain_compact_and_drop_expired_blocks() -> None:
    """Compact server records preserve source numbers and remove matchless expired context."""

    source = LOG_VIEWER.read_text(encoding="utf-8")
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        globalThis.window = {{}};
        globalThis.WebSocket = {{OPEN: 1}};
        {source}
        const panel = Object.create(LogViewerPanel.prototype);
        panel._serverRecords = [];
        panel._lines = [];
        panel._searchTerm = 'needle';
        panel._searchRegex = false;
        panel._filterMode = true;
        panel._contextLines = 1;
        panel._searchAbort = 3;
        panel._renderAbort = 0;
        panel._expandedBlocks = new Set();
        panel._blocksCollapsed = true;
        panel._q = () => null;
        panel.renderCalls = 0;
        panel._renderFilteredRows = searchId => {{ panel.renderedSearchId = searchId; panel.renderCalls++; }};

        panel._applyServerFilteredRecords({{
          source_start: 10, match_count: 1,
          records: [
            {{line:'before', line_no:10, match:false}},
            {{line:'needle', line_no:11, match:true}},
            {{line:'after', line_no:12, match:false}},
            {{line:'unrelated', line_no:20, match:false}}
          ]
        }}, false);
        assert.deepEqual(panel._filteredLineNumbers, [10, 11, 12]);
        assert.deepEqual(panel._lines, ['before', 'needle', 'after']);
        assert.equal(panel._filteredBlocks.length, 1);
        assert.equal(panel._filteredBlocks[0].firstMatch, 1);
        assert.equal(panel.renderCalls, 1);

        panel._applyServerFilteredRecords({{source_start:10, match_count:1, records:[]}}, true);
        assert.equal(panel.renderCalls, 1);

        panel._applyServerFilteredRecords({{
          source_start: 11, match_count: 1,
          records: [{{line:'later context', line_no:13, match:false}}]
        }}, true);
        assert.deepEqual(panel._filteredLineNumbers, [11, 12]);

        panel._applyServerFilteredRecords({{source_start:12, match_count:0, records:[]}}, true);
        assert.deepEqual(panel._filteredLineNumbers, []);
        assert.equal(panel._filteredBlocks.length, 0);
        assert.equal(panel._filteredMatchCount, 0);

        panel._applyServerFilteredRecords({{
          source_start:12, match_count:1,
          records:[{{line:'new needle', line_no:14, match:true}}]
        }}, true);
        assert.deepEqual(panel._filteredLineNumbers, [13, 14]);
        assert.deepEqual(panel._lines, ['later context', 'new needle']);
        """
    )
    result = subprocess.run(["node", "-e", script], cwd=ROOT, capture_output=True, text=True, check=False)
    assert result.returncode == 0, f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"


def test_cookie_authenticated_pages_keep_logout_visible_without_a_token() -> None:
    """Explicit page authentication exposes cookie logout without browser token support."""

    nav = NAV.read_text(encoding="utf-8")
    manager = (ROOT / "frontend" / "vps_manager.html").read_text(encoding="utf-8")
    monitor = (ROOT / "frontend" / "vps_monitor.html").read_text(encoding="utf-8")

    assert "authenticated: c.authenticated === true" in nav
    assert "c.authenticated ? 'inline-flex' : 'none'" in nav
    assert "Authorization" not in nav
    assert "opts.credentials = 'same-origin'" in nav
    assert "authenticated: true" in manager
    assert "authenticated: true" in monitor
    for source in (manager, monitor):
        assert "%%TOKEN%%" not in source
        assert "Authorization" not in source
        assert "Bearer" not in source


def test_restart_arms_stream_before_kill_and_keeps_it_after_success() -> None:
    """Restart startup lines follow an acknowledged EOF cursor without resubscribe loss."""
    source = LOG_VIEWER.read_text(encoding="utf-8")
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        globalThis.window = {{}};
        globalThis.WebSocket = {{OPEN: 1, CLOSING: 2, CLOSED: 3}};
        globalThis.setTimeout = () => 1;
        globalThis.clearTimeout = () => {{}};
        {source}
        const sent = [];
        const restartButton = {{disabled: false, textContent: ''}};
        const panel = Object.create(LogViewerPanel.prototype);
        panel._host = 'remote-a';
        panel._service = '/arbitrary/provider/path/live-output';
        panel._file = '';
        panel._vpState = {{}};
        panel._restartServiceProvider = (host, item) => (
          host === 'remote-a' && item === '/arbitrary/provider/path/live-output' ? 'Bot:demo:8' : null
        );
        panel._restartHandler = null;
        panel._ws = {{readyState: 1, send: raw => sent.push(JSON.parse(raw))}};
        panel._sid = 4;
        panel._streaming = true;
        panel._showRestart = true;
        panel._startLocalAtEnd = false;
        panel._pendingRestartCommand = null;
        panel._restartAttempt = null;
        panel._restartGeneration = 0;
        panel._restartTimeout = 0;
        panel._lines = [];
        panel._lineBase = 0;
        panel._q = name => name === 'restart-btn' ? restartButton : (name === 'lines-sel' ? {{value: '200'}} : null);
        panel._unsubscribe = () => {{ panel._streaming = false; }};
        panel._clear = () => {{ panel._lines = []; }};
        panel._updateStreamBtn = () => {{}};
        panel._renderFull = () => {{}};
        panel._normalizeIncomingLines = lines => lines;
        panel._restartBlockerFor = () => '';

        panel._restart();
        const subscribe = sent.find(item => item.cmd === 'subscribe_logs');
        assert.ok(subscribe);
        assert.equal(subscribe.start_at_end, true);
        assert.equal(sent.some(item => item.cmd === 'kill_instance'), false);

        panel._handleMsg({{type: 'logs', sid: subscribe.sid, lines: [], streaming: true}});
        assert.equal(sent.filter(item => item.cmd === 'kill_instance').length, 1);
        const sentBeforeResult = sent.length;

        panel._handleMsg({{type: 'result', cmd: 'kill_instance', success: true}});
        assert.equal(sent.length, sentBeforeResult);
        assert.equal(panel._restartAttempt, null);
        """
    )
    result = subprocess.run(["node", "-e", script], cwd=ROOT, capture_output=True, text=True, check=False)
    assert result.returncode == 0, f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"


def test_every_log_viewer_asset_reference_uses_current_cache_version() -> None:
    """All HTML and dynamic loader references must fetch the fixed shared asset."""

    references: list[tuple[Path, str]] = []
    for path in (ROOT / "frontend").rglob("*.html"):
        source = path.read_text(encoding="utf-8")
        references.extend((path, match.group(0)) for match in re.finditer(r"log_viewer_panel\.js\?v=\d+", source))

    assert references
    assert all(reference.endswith("?v=43") for _path, reference in references), references
    assert "log_viewer_panel.js?v=43" in NAV.read_text(encoding="utf-8")


def test_api_keys_local_viewer_disables_vps_state_transport() -> None:
    """The API Keys log socket opts out before any full VPS state is sent."""
    source = (ROOT / "frontend" / "api_keys_editor.html").read_text(encoding="utf-8")
    assert "needsState  : false" in source
    viewer = LOG_VIEWER.read_text(encoding="utf-8")
    assert "this._needsState = opts.needsState !== false" in viewer
    assert "this._needsState === false ? '?state=0' : ''" in viewer


def test_log_viewer_close_releases_hidden_runtime_state() -> None:
    """Closing a hidden viewer must disconnect and release its retained model and DOM."""
    source = LOG_VIEWER.read_text(encoding="utf-8")
    close_body = source.split("    close() {", 1)[1].split("\n    }", 1)[0]
    assert "this._disconnect();" in close_body
    assert "this._finishRestartAttempt();" in close_body
    assert "this._vpState = null;" in close_body
    assert "this._fileList = [];" in close_body
    assert "this._clear();" in close_body


def test_api_keys_back_and_pagehide_close_log_viewer() -> None:
    """Both in-page Back navigation and page exit must close the API Keys viewer."""
    source = (ROOT / "frontend" / "api_keys_editor.html").read_text(encoding="utf-8")
    back_body = source.split("async function backToList", 1)[1].split("// ── Save user", 1)[0]
    assert "_closeLogWs();" in back_body
    assert 'window.addEventListener("pagehide", _closeLogWs);' in source


def test_remote_default_host_is_rendered_before_vps_state_arrives() -> None:
    """A requested remote host must never be presented as Local while state loads."""

    source = LOG_VIEWER.read_text(encoding="utf-8")
    build_start = source.index("    _build() {")
    build_method = source[build_start:source.index("    _bindEvents() {", build_start)]
    dropdown_start = source.index("    _updateHostDropdown() {")
    dropdown_method = source[dropdown_start:source.index("    _arrayEq(", dropdown_start)]

    assert "if (hostSelect && this._host !== 'local')" in build_method
    assert "hostOption.textContent = this._host" in build_method
    assert "hostSelect.value = this._host" in build_method
    assert "if (this._host !== 'local') hosts.push(this._host)" in dropdown_method
