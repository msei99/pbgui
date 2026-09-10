"""Static and Node-backed VPS Monitor agent frontend regressions."""

from __future__ import annotations

import re
import subprocess
import textwrap
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
HTML_PATH = ROOT / "frontend" / "vps_monitor.html"
EN_GUIDE = ROOT / "docs" / "help" / "29_vps_monitor.md"
DE_GUIDE = ROOT / "docs" / "help_de" / "29_vps_monitor.md"


def _extract_function(source: str, name: str) -> str:
    """Extract one named JavaScript function with balanced braces."""
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


def _run_agent_assertions(assertions: str) -> None:
    """Execute the monitor-agent classifiers and renderer in Node."""
    source = HTML_PATH.read_text(encoding="utf-8")
    names = [
        "telemetryLastUpdate",
        "telemetryAgeSeconds",
        "effectiveAgeSeconds",
        "monitorAgentFile",
        "boundedAgentError",
        "monitorAgentHealth",
        "formatAgentAge",
        "renderMonitorAgentDetails",
    ]
    functions = "\n\n".join(_extract_function(source, name) for name in names)
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const TELEMETRY_STALE_SECONDS = 15;
        const MONITOR_AGENT_COLLECTOR_STALE_SECONDS = 30;
        const MONITOR_AGENT_REQUIRED_FILES = [
          'live_metrics.ndjson', 'instance_snapshot.json',
          'host_meta.json', 'service_status.json', 'package_status.json', 'collector_status.json'
        ];
        function esc(value) {{
          return String(value == null ? '' : value)
            .replace(/&/g, '&amp;').replace(/</g, '&lt;')
            .replace(/>/g, '&gt;').replace(/\"/g, '&quot;');
        }}
        {functions}
        {assertions}
        """
    )
    result = subprocess.run(
        ["node", "-e", script],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, (
        "Node-backed VPS Monitor regression failed\n"
        f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
    )


def test_frontend_is_cookie_only_and_websocket_url_has_no_credentials() -> None:
    """No browser integration may receive or manufacture a bearer credential."""
    source = HTML_PATH.read_text(encoding="utf-8")

    assert "%%TOKEN%%" not in source
    assert "window.TOKEN" not in source
    assert "Authorization" not in source
    assert "Bearer" not in source
    assert not re.search(r"\btoken\s*:", source, re.IGNORECASE)
    assert "WS_BASE + '/ws/vps'" in source
    assert "credentials: 'same-origin'" in source
    assert "PBGuiSharedHelp.open('vps_monitor')" in source
    assert "authenticated: true" in source


def test_compact_desktop_status_keeps_identity_badges_on_one_line() -> None:
    """Wide compact rows reserve enough aligned space without wrapping badges."""
    source = HTML_PATH.read_text(encoding="utf-8")
    media = re.search(
        r"@media \(min-width: 1200px\)\s*\{\s*body\.compact \.vps-title\s*\{(?P<rules>[^}]*)\}",
        source,
    )

    assert media is not None
    assert "min-width: 420px" in media.group("rules")
    assert "width: 420px" in media.group("rules")
    assert "white-space: nowrap" in media.group("rules")


def test_close_4001_stops_reconnect_and_returns_to_login() -> None:
    """Session expiry must terminate reconnect attempts before safe navigation."""
    source = HTML_PATH.read_text(encoding="utf-8")
    connect = _extract_function(source, "connect")
    schedule = _extract_function(source, "scheduleReconnect")

    assert "event.code === 4001" in connect
    assert "authExpired = true" in connect
    assert "clearInterval(reconnectTimer)" in connect
    assert "window.location.replace('/')" in connect
    assert connect.index("event.code === 4001") < connect.index("scheduleReconnect()")
    assert "if (authExpired || reconnectTimer) return" in schedule
    assert "!authExpired" in schedule


def test_disconnected_actions_and_history_clear_pending_states() -> None:
    """Dropped, timed-out, and disconnected requests must restore every pending control."""
    source = HTML_PATH.read_text(encoding="utf-8")
    names = [
        "send",
        "restoreRestartButton",
        "restoreKillButton",
        "failMetricHistory",
        "resetPendingMonitorActions",
        "handleResult",
        "restartService",
        "killInstance",
        "openMetricHistory",
    ]
    functions = "\n\n".join(_extract_function(source, name) for name in names)
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const WebSocket = {{OPEN: 1}};
        const timers = new Map();
        let nextTimer = 0;
        function setTimeout(fn) {{ const id = ++nextTimer; timers.set(id, fn); return id; }}
        function clearTimeout(id) {{ timers.delete(id); }}
        function classList() {{
          const values = new Set();
          return {{
            add(value) {{ values.add(value); }},
            remove(value) {{ values.delete(value); }},
            contains(value) {{ return values.has(value); }}
          }};
        }}
        function button(action, host, bot) {{
          const attrs = {{'data-host': host || '', 'data-bot': bot || ''}};
          return {{disabled: false, textContent: '', title: '', style: {{}}, classList: classList(),
            getAttribute(name) {{ return name === 'data-monitor-action' ? action : attrs[name]; }} }};
        }}
        const body = {{appendChild(element) {{ element.parentElement = body; }}}};
        const elements = {{
          'cpu-history-ovl': {{parentElement: body, classList: classList(), style: {{}}}},
          'cpu-history-title': {{}}, 'cpu-history-sub': {{}},
          'cpu-history-meta': {{innerHTML: ''}}, 'cpu-history-chart': {{innerHTML: '', textContent: ''}}
        }};
        let restartButtons = [];
        let killButtons = [];
        const document = {{
          body,
          getElementById(id) {{ return elements[id] || null; }},
          querySelectorAll(selector) {{ return selector.includes('restart-service') ? restartButtons : killButtons; }}
        }};
        let banner = '';
        function setBanner(value) {{ banner = value; }}
        function prepareCpuHistoryWindow() {{}}
        function metricHistoryTitle() {{ return 'History'; }}
        function metricHistoryMeta() {{ return {{}}; }}
        function metricHistoryLoadingSubtitle() {{ return 'Loading'; }}
        let ws = null;
        let cpuHistoryRequestId = 0;
        let cpuHistoryTimeout = null;
        {functions}

        assert.equal(send({{cmd: 'noop'}}), false);
        ws = {{readyState: WebSocket.OPEN, send() {{ throw new Error('closed during send'); }}}};
        assert.equal(send({{cmd: 'noop'}}), false);
        assert.equal(banner, 'lost');

        ws = null;
        const restart = button('restart-service');
        restartService('host', 'svc', restart);
        assert.equal(restart.disabled, false);
        assert.equal(restart.textContent, 'Restart');
        const kill = button('kill-instance');
        killInstance('host', 'bot', '8', kill);
        assert.equal(kill.disabled, false);
        assert.equal(kill.textContent, '🔄');

        openMetricHistory('host', 'cpu', 'bot');
        assert.match(elements['cpu-history-chart'].textContent, /Connection lost/);
        assert.equal(cpuHistoryTimeout, null);

        const sent = [];
        ws = {{readyState: WebSocket.OPEN, send(payload) {{ sent.push(JSON.parse(payload)); }}}};
        restartService('host', 'svc', restart);
        killInstance('host', 'bot', '8', kill);
        killInstance('host', 'bot', '8', kill);
        assert.equal(sent.length, 2, 'pending kill cannot be submitted twice');
        restartButtons = [restart];
        killButtons = [kill];
        cpuHistoryRequestId = 10;
        cpuHistoryTimeout = setTimeout(function () {{}}, 10000);
        resetPendingMonitorActions('Connection lost. Reconnect and try again.');
        assert.equal(restart.disabled, false);
        assert.equal(kill.disabled, false);
        assert.equal(cpuHistoryTimeout, null);
        assert.match(elements['cpu-history-chart'].textContent, /Connection lost/);

        restartService('host', 'svc', restart);
        timers.get(restart._monitorActionTimer)();
        assert.equal(restart.disabled, false);
        killInstance('host', 'bot', '8', kill);
        timers.get(kill._monitorActionTimer)();
        assert.equal(kill.disabled, false);
        openMetricHistory('host', 'cpu', 'bot');
        timers.get(cpuHistoryTimeout)();
        assert.match(elements['cpu-history-chart'].textContent, /timed out/);
        assert.equal(cpuHistoryTimeout, null);

        const duplicateOne = button('kill-instance', 'host', 'same-name');
        const duplicateTwo = button('kill-instance', 'host', 'same-name');
        duplicateOne.classList.add('killing');
        duplicateTwo.classList.add('killing');
        duplicateOne.disabled = duplicateTwo.disabled = true;
        killButtons = [duplicateOne, duplicateTwo];
        handleResult({{cmd: 'kill_instance', host: 'host', name: 'same-name', success: true}});
        assert.equal(duplicateOne.disabled, false);
        assert.equal(duplicateTwo.disabled, true, 'one response restores only one same-named runtime action');
        """
    )
    result = subprocess.run(["node", "-e", script], cwd=ROOT, capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr or result.stdout
    assert "resetPendingMonitorActions('Connection lost. Reconnect and try again.')" in _extract_function(source, "connect")


def test_agent_classifier_applies_15_and_30_second_policies_to_all_states() -> None:
    """Classify OK, Stale, Missing, Error, and Unknown deterministically."""
    _run_agent_assertions(
        """
        const now = 100;
        assert.equal(effectiveAgeSeconds(500, 99, 90, now), 10);
        assert.equal(effectiveAgeSeconds(5, 99, null, now), 6);
        function base(state) {
          return {
            state: state,
            age: 2,
            checked_at: 99,
            files: {
              'live_metrics.ndjson': {state: 'ok', age: 1, checked_at: 99},
              'collector_status.json': {state: 'ok', age: 2, checked_at: 99}
            },
            collector: {age: 2, checked_at: 99},
            loops: {live_metrics: {last_error: ''}}
          };
        }
        assert.equal(monitorAgentHealth(base('ok'), null, {}, now).state, 'ok');

        const liveStale = base('ok');
        liveStale.files['live_metrics.ndjson'].age = 15;
        assert.equal(monitorAgentHealth(liveStale, null, {}, now).liveAge, 16);
        assert.equal(monitorAgentHealth(liveStale, null, {}, now).state, 'stale');

        const heartbeatStale = base('ok');
        heartbeatStale.collector.age = 30;
        assert.equal(monitorAgentHealth(heartbeatStale, null, {}, now).heartbeatAge, 31);
        assert.equal(monitorAgentHealth(heartbeatStale, null, {}, now).state, 'stale');

        const staleDiagnostic = base('stale');
        staleDiagnostic.error = 'monitor-agent cache stale age=16s: live_metrics.ndjson';
        assert.equal(monitorAgentHealth(staleDiagnostic, null, {}, now).state, 'stale');

        assert.equal(monitorAgentHealth(base('missing'), null, {}, now).state, 'missing');
        const failed = base('ok');
        failed.loops.instances = {last_error: 'collector failed'};
        assert.equal(monitorAgentHealth(failed, null, {}, now).state, 'error');
        const unknown = monitorAgentHealth(null, null, {}, now);
        assert.equal(unknown.state, 'unknown');
        assert.equal(unknown.files.length, MONITOR_AGENT_REQUIRED_FILES.length);
        """
    )


def test_host_and_bot_action_markup_escapes_xss_payloads() -> None:
    """Delegated monitor actions must encode host and bot names as data only."""

    source = HTML_PATH.read_text(encoding="utf-8")
    esc_start = source.index("function escAttr(")
    instance_start = source.index("function renderInstanceActions(")
    service_start = source.index("function renderServiceRestartButton(")
    functions = "\n\n".join([
        source[esc_start:source.index("function toggleCard(", esc_start)],
        source[instance_start:source.index("function instanceCpuCell(", instance_start)],
        source[service_start:source.index("function restartService(", service_start)],
    ])
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        {functions}
        const attack = `bad\"' onclick='globalThis.PWNED=1'><img src=x onerror=1>`;
        const actions = renderInstanceActions({{host: attack, name: attack, pbVersion: attack}});
        const restart = renderServiceRestartButton(attack, attack);
        for (const html of [actions, restart]) {{
          assert.equal(html.includes('" onclick='), false);
          assert.equal(html.includes("' onclick="), false);
          assert.equal(html.includes('<img src=x'), false);
          assert.match(html, /&quot;/);
          assert.match(html, /&#39;/);
        }}
        assert.match(actions, /data-monitor-action="view-instance-log"/);
        assert.match(restart, /data-monitor-action="restart-service"/);
        """
    )
    result = subprocess.run(["node", "-e", script], cwd=ROOT, capture_output=True, text=True, check=False)
    assert result.returncode == 0, (
        f"Node-backed XSS regression failed\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
    )


def test_concrete_monitor_rows_use_delegation_not_inline_names() -> None:
    """Reviewed instance and service paths must contain no generated handlers."""

    source = HTML_PATH.read_text(encoding="utf-8")

    assert "viewInstanceLog('${esc(r.host)}'" not in source
    assert "killInstance('${esc(r.host)}'" not in source
    assert "restartService('${esc(host)}'" not in source
    assert "data-monitor-action=\"view-instance-log\"" in source
    assert "data-monitor-action=\"restart-service\"" in source


def test_pb8_instance_rows_preserve_runtime_and_freshness() -> None:
    """PB8 rows stay distinct from PB7 and stale snapshots cannot invent stopped bots."""

    source = HTML_PATH.read_text(encoding="utf-8")
    functions = "\n\n".join([
        _extract_function(source, "isFreshV8Observation"),
        _extract_function(source, "buildInstanceRows"),
    ])
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        const MONITOR_AGENT_COLLECTOR_STALE_SECONDS = 30;
        {functions}
        const fresh = {{snapshot_generated_at: 95, snapshot_checked_at: 96}};
        const state = {{
          instances: {{host: [
            {{u: 'same', p: '7', m: [], c: 1}},
            {{u: 'same', p: '8', m: [], c: 2}}
          ]}},
          v7_instances: {{host: [{{name: 'same', cv: 2, rv: 2, eo: 'host', running: true}}]}},
          v8_instances: {{host: [
            {{name: 'same', cv: 3, rv: 2, eo: 'host', running: true, ...fresh}},
            {{name: 'stopped', cv: 4, rv: 0, eo: 'host', running: false, ...fresh}},
            {{name: 'disabled', cv: 5, rv: 0, eo: 'disabled', running: false, ...fresh}},
            {{name: 'stale', cv: 6, rv: 0, eo: 'host', running: false,
              snapshot_generated_at: 1, snapshot_checked_at: 1}},
            {{name: 'unknown', cv: 7, rv: 0, eo: 'host', running: false}}
          ]}}
        }};
        const rows = buildInstanceRows(state, 100);
        assert.equal(rows.length, 4);
        assert.deepEqual(
          rows.filter(row => row.name === 'same').map(row => [row.version, row.pbVersion, row.syncStatus]),
          [['V7', '7', 'synced'], ['V8', '8', 'outdated']]
        );
        assert.equal(rows.find(row => row.name === 'stopped').syncStatus, 'stopped');
        assert.equal(rows.find(row => row.name === 'disabled').syncStatus, 'disabled');
        assert.equal(rows.some(row => row.name === 'stale'), false);
        assert.equal(rows.some(row => row.name === 'unknown'), false);
        """
    )
    result = subprocess.run(["node", "-e", script], cwd=ROOT, capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr or result.stdout


def test_agent_details_escape_and_bound_errors_and_show_every_required_file() -> None:
    """Untrusted collector diagnostics must remain bounded text in generated HTML."""
    _run_agent_assertions(
        """
        const attack = '<img src=x onerror=alert(1)>' + 'x'.repeat(500);
        const agent = {
          state: 'ok', age: 1, checked_at: 100,
          files: {'live_metrics.ndjson': {state: 'ok', age: 1, checked_at: 100}},
          collector: {age: 1, checked_at: 100},
          loops: {instances: {last_error: attack}}
        };
        const model = monitorAgentHealth(agent, null, {}, 100);
        assert.equal(model.state, 'error');
        assert.ok(model.errors[0].length <= 240);
        const html = renderMonitorAgentDetails(agent, null, {}, 100);
        assert.equal(html.includes('<img src=x'), false);
        assert.equal(html.includes('&lt;img src=x'), true);
        assert.equal(html.includes('Source:</strong> monitor-agent cache'), true);
        assert.equal(html.includes('healthy through 15s'), true);
        assert.equal(html.includes('healthy through 30s'), true);
        for (const filename of MONITOR_AGENT_REQUIRED_FILES) assert.equal(html.includes(filename), true);
        assert.equal(html.includes('systemctl --user status pbgui-monitor-agent.service'), true);
        assert.equal(html.includes('journalctl --user -u pbgui-monitor-agent.service'), true);
        """
    )


def test_guides_have_parallel_agent_operations_content() -> None:
    """English and German guides must cover the same operational contract."""
    english = EN_GUIDE.read_text(encoding="utf-8")
    german = DE_GUIDE.read_text(encoding="utf-8")
    shared_terms = [
        "PBCluster", "PBRun", "PBData", "PBCoinData", "PBMonitorAgent",
        "data/monitor_agent/live_metrics.ndjson",
        "live_metrics.latest.json", "instance_snapshot.json", "host_meta.json",
        "service_status.json", "package_status.json", "collector_status.json",
        "systemctl --user status pbgui-monitor-agent.service",
        "systemctl --user restart pbgui-monitor-agent.service",
        "journalctl --user -u pbgui-monitor-agent.service",
        "15", "30", "monitor-agent cache",
    ]

    for term in shared_terms:
        assert term in english
        assert term in german
    assert "byte-based retention" in english.lower()
    assert "byte-basierte aufbewahrung" in german.lower()
    assert "no direct collector fallback" in english.lower()
    assert "keinen direkten collector-fallback" in german.lower()
    assert "SSH may remain" in english
    assert "SSH kann verbunden bleiben" in german
    assert len(re.findall(r"^## ", english, re.MULTILINE)) == len(re.findall(r"^## ", german, re.MULTILINE))
