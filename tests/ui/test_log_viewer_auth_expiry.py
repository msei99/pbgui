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


def test_cookie_authenticated_pages_keep_logout_visible_without_a_token() -> None:
    """Explicit page authentication must expose cookie logout while token pages remain supported."""

    nav = NAV.read_text(encoding="utf-8")
    manager = (ROOT / "frontend" / "vps_manager.html").read_text(encoding="utf-8")
    monitor = (ROOT / "frontend" / "vps_monitor.html").read_text(encoding="utf-8")

    assert "authenticated: c.authenticated === true" in nav
    assert "(TOKEN || c.authenticated) ? 'inline-flex' : 'none'" in nav
    assert "headers.Authorization = 'Bearer ' + token" in nav
    assert "credentials: 'same-origin'" in nav
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
        panel._service = 'Bot:demo:7';
        panel._file = '';
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
    assert all(reference.endswith("?v=28") for _path, reference in references), references
    assert "log_viewer_panel.js?v=28" in NAV.read_text(encoding="utf-8")
