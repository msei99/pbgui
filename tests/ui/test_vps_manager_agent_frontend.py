"""Frontend and authentication contracts for VPS Manager agent status."""

from __future__ import annotations

import inspect
import subprocess
import textwrap
from pathlib import Path
from types import SimpleNamespace

from starlette.requests import Request

from api.auth import authenticate_websocket, require_auth
from api import vps_manager as api_module


ROOT = Path(__file__).resolve().parents[2]
HTML_PATH = ROOT / "frontend" / "vps_manager.html"


def _extract_function(source: str, name: str) -> str:
    """Extract one named inline JavaScript function."""

    marker = f"function {name}("
    start = source.find(marker)
    assert start >= 0, f"Could not find JavaScript function {name!r}"
    brace_start = source.find("{", start)
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
                return source[start:index + 1]
    raise AssertionError(f"Could not extract complete JavaScript function {name!r}")


def test_agent_renderer_shows_states_source_remediation_and_escapes() -> None:
    """Render complete escaped agent diagnostics without converting N/A to zero."""

    source = HTML_PATH.read_text(encoding="utf-8")
    names = ["esc", "escAttr", "js", "formatLatency", "levelTag", "agentStateModel", "formatAgentAge", "effectiveAgentAge", "renderAgentCachePanel"]
    functions = "\n\n".join(_extract_function(source, name) for name in names)
    script = textwrap.dedent(
        fr"""
        const assert = require('node:assert/strict');
        const store = {{ hostname: 'bad-host' }};
        function runMasterWithLog() {{}}
        function runVpsWithLog() {{}}
        {functions}
        const status = {{
          package_status: {{ source: 'agent_cache', state: 'missing', available: false, upgrades: 'N/A', reboot_required: null, error: '<img src=x onerror=1>' }},
          monitor_agent: {{
            source: 'agent_cache', state: 'error', error: '<script>bad</script>', age: 31,
            files: {{ 'service_status.json': {{ state: 'error', age: 12, error: '<b>collector failed</b>' }} }},
            loops: {{ services: {{ state: 'error', age: 12, last_error: '<svg onload=1>' }} }}
          }}
        }};
        const html = renderAgentCachePanel(status, false);
        assert.deepEqual(agentStateModel('ok'), {{ state: 'ok', label: 'OK', tone: 'ok' }});
        assert.deepEqual(agentStateModel('stale'), {{ state: 'stale', label: 'Stale', tone: 'warning' }});
        assert.deepEqual(agentStateModel('missing'), {{ state: 'missing', label: 'Missing', tone: 'error' }});
        assert.deepEqual(agentStateModel('error'), {{ state: 'error', label: 'Error', tone: 'error' }});
        assert.match(html, /Source: agent cache/);
        assert.match(html, />Error</);
        assert.match(html, />Missing</);
        assert.match(html, /N\/A/);
        assert.match(html, /live_metrics\.ndjson/);
        assert.match(html, /collector_status\.json/);
        assert.match(html, /Update PBGui/);
        assert.match(html, /systemctl --user status pbgui-monitor-agent\.service/);
        assert.match(html, /systemctl --user restart pbgui-monitor-agent\.service/);
        assert.match(html, /journalctl --user -u pbgui-monitor-agent\.service/);
        assert.doesNotMatch(html, /<img src=x/);
        assert.doesNotMatch(html, /<script>bad/);
        assert.doesNotMatch(html, /<svg onload/);
        assert.match(html, /&lt;img/);

        const staleHtml = renderAgentCachePanel({{
          package_status: {{ source: 'agent_cache', state: 'stale', available: true, upgrades: 4, reboot_required: true, age: 7300 }},
          monitor_agent: {{ source: 'agent_cache', state: 'stale', files: {{}}, loops: {{}} }}
        }}, true);
        assert.match(staleHtml, /Last-known updates: 4/);
        assert.match(staleHtml, />Stale</);

        assert.equal(effectiveAgentAge({{generated_at: 90, age: 99, checked_at: 95}}, 100), 10);
        assert.equal(effectiveAgentAge({{age: 4, checked_at: 95}}, 100), 9);
        """
    )
    result = subprocess.run(["node", "-e", script], cwd=ROOT, capture_output=True, text=True, check=False)
    assert result.returncode == 0, f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"


def test_overview_provisional_detail_and_sidebar_share_agent_contract() -> None:
    """All page surfaces consume the normalized package and agent objects."""

    source = HTML_PATH.read_text(encoding="utf-8")

    assert "summary.package_status || { source: 'agent_cache'" in source
    assert "summary.monitor_agent || { source: 'agent_cache'" in source
    assert "Source: agent cache ·" in _extract_function(source, "renderOverviewTable")
    assert "renderSidebarAgentStatus(st)" in _extract_function(source, "renderSidebarActions")
    assert "renderAgentCachePanel(status, true)" in _extract_function(source, "renderMasterView")
    assert "renderAgentCachePanel(status, false)" in _extract_function(source, "renderVpsView")
    assert "packageAvailable ? Number.parseInt" in _extract_function(source, "getSystemHeaderModel")
    assert "updatesNum = Number.isFinite(updatesCount) ? updatesCount : 'N/A'" in source


def test_vps_manager_page_is_cookie_only() -> None:
    """The browser page must not contain or configure a bearer/session token."""

    source = HTML_PATH.read_text(encoding="utf-8")

    for forbidden in ("%%TOKEN%%", "window.TOKEN", "Authorization", "Bearer", "token: TOKEN", "const TOKEN"):
        assert forbidden not in source
    assert "credentials: 'same-origin'" in source
    assert "new WebSocket(url)" in source
    assert "new LogViewerPanel({" in source
    assert "authenticated: true" in source


def test_manager_renderers_keep_host_and_bot_payloads_out_of_inline_code() -> None:
    """Host and bot XSS payloads must remain escaped delegated-action data."""

    source = HTML_PATH.read_text(encoding="utf-8")
    functions = "\n\n".join(
        _extract_function(source, name)
        for name in ("esc", "escAttr", "_renderServiceRowCells", "renderOverviewTable")
    )
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const attack = `bad');globalThis.PWNED=1;//<img src=x onerror=1>`;
        const store = {{ overviewSort: {{field: 'name', dir: 'asc'}}, hostname: attack }};
        const OVERVIEW_COLUMNS = [{{key: 'name', label: 'Name', sortable: false, getValue: r => r.name}}];
        function getSortedOverviewRows(rows) {{ return rows; }}
        function getSelectedOverviewHosts() {{ return []; }}
        function isOverviewColumnVisible() {{ return true; }}
        function normalizeOverviewSort(value) {{ return value; }}
        function renderOverviewTaskSummary() {{ return ''; }}
        function levelTag() {{ return ''; }}
        function agentStateModel() {{ return {{state: 'ok', tone: 'ok'}}; }}
        function _historyMetricBubble() {{ return ''; }}
        function _metricTag(value) {{ return '<span>' + String(value || 0) + '</span>'; }}
        function _cpuTag() {{ return ''; }}
        function _cpuSubline() {{ return ''; }}
        function _memSwapTag() {{ return ''; }}
        function _pnlValueHtml() {{ return ''; }}
        {functions}
        const cells = _renderServiceRowCells({{name: attack, start_time: attack}}, attack, '7');
        assert.equal(cells.name.html.includes('onclick='), false);
        assert.equal(cells.errors_today.html.includes('onclick='), false);
        assert.equal(cells.name.html.includes('<img src=x'), false);
        assert.match(cells.name.html, /data-vps-action='open-bot-log'/);
        assert.match(cells.name.html, /&#39;/);

        const overview = renderOverviewTable([{{nav: 'vps', hostname: attack, name: attack}}]);
        const body = overview.slice(overview.indexOf('<tbody>'), overview.indexOf('</tbody>'));
        assert.equal(body.includes('onclick='), false);
        assert.equal(body.includes('<img src=x'), false);
        assert.match(body, /data-vps-action='select-vps'/);
        assert.match(body, /&#39;/);
        """
    )
    result = subprocess.run(["node", "-e", script], cwd=ROOT, capture_output=True, text=True, check=False)
    assert result.returncode == 0, f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"

    assert "${js(selectedHost)}" not in source
    assert "${js(detail.hostname)}" not in source
    assert "openBotLog(${js(host)}" not in source
    assert "fetchBotLogMatches(${js(host)}" not in source
    assert "data-vps-action='toggle-password'" in source
    assert "document.addEventListener('input', handleVpsManagerFieldEvent)" in source


def test_manager_close_4001_permanently_stops_reconnect() -> None:
    """Manager session expiry must clear queued work and never schedule again."""

    source = HTML_PATH.read_text(encoding="utf-8")
    connect = _extract_function(source, "connectWs")
    schedule = _extract_function(source, "scheduleReconnect")

    assert "event.code === 4001" in connect
    assert "store.authExpired = true" in connect
    assert "store.pendingWsMessages = []" in connect
    assert "window.location.replace('/')" in connect
    assert "if (store.authExpired) return" in connect
    assert "if (store.authExpired || store.reconnectTimer) return" in schedule


def test_html_route_requires_auth_without_rendering_session_token() -> None:
    """Keep route authentication server-side and omit the token from HTML."""

    route = next(route for route in api_module.router.routes if getattr(route, "path", "") == "/main_page")
    dependency_calls = [dependency.call for dependency in route.dependant.dependencies]
    assert require_auth in dependency_calls
    request = Request({
        "type": "http",
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": "/api/vps-manager/main_page",
        "raw_path": b"/api/vps-manager/main_page",
        "query_string": b"",
        "headers": [(b"host", b"testserver")],
        "client": ("127.0.0.1", 1234),
        "server": ("testserver", 80),
    })
    secret = "browser-session-secret-must-not-render"

    response = api_module.get_main_page(request, SimpleNamespace(token=secret))
    html = response.body.decode("utf-8")

    assert secret not in html
    assert "%%TOKEN%%" not in html


def test_websocket_route_retains_cookie_authenticator_and_4001_contract() -> None:
    """VPS Manager WebSockets must keep the shared cookie authenticator."""

    source = inspect.getsource(api_module.ws_vps_manager)
    authenticator_source = inspect.getsource(authenticate_websocket)

    assert "await authenticate_websocket(websocket)" in source
    assert "if session is None:" in source
    assert "4001" in authenticator_source
