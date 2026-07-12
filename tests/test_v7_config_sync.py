"""Regression tests for legacy sync removal and frontend rendering safety."""

import asyncio
import subprocess
import textwrap
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _read(relative_path: str) -> str:
    """Return a repository file as text."""

    return (ROOT / relative_path).read_text(encoding="utf-8")


def _extract_js_function(source: str, name: str) -> str:
    """Extract one named JavaScript function from an HTML source file."""
    marker = f"function {name}("
    start = source.find(marker)
    assert start >= 0, f"Could not find JavaScript function {name!r}"
    if source[max(0, start - 6):start] == "async ":
        start -= 6
    brace_start = source.find("{", start)
    depth = 0
    quote = None
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


def _run_frontend_node(relative_path: str, function_names: list[str], bootstrap: str, assertions: str) -> None:
    """Run Node assertions against selected inline frontend functions."""
    source = _read(relative_path)
    functions = "\n\n".join(_extract_js_function(source, name) for name in function_names)
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');
        function encodeText(value) {{
          return String(value).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
        }}
        {bootstrap}
        {functions}
        {assertions}
        """
    )
    result = subprocess.run(["node", "-e", script], cwd=ROOT, capture_output=True, text=True, check=False)
    assert result.returncode == 0, f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"


def test_legacy_worker_modules_are_not_present() -> None:
    """Ensure deleted legacy worker modules stay removed."""

    assert not (ROOT / "master" / "file_sync.py").exists()
    assert not (ROOT / "master" / "v7_config_sync.py").exists()
    assert not (ROOT / "frontend" / "js" / "api_sync_status.js").exists()


def test_api_startup_does_not_create_legacy_sync_workers() -> None:
    """Ensure PBApiServer does not initialize deleted sync workers."""

    source = _read("PBApiServer.py")
    forbidden = [
        "FileSyncWorker",
        "V7ConfigSyncWorker",
        "init_file_sync",
        "file_sync.start_watchers",
        "v7_sync.start_watchers",
    ]
    for needle in forbidden:
        assert needle not in source


def test_api_key_ssh_sync_routes_are_removed() -> None:
    """Ensure direct API-key SSH sync endpoints and UI are absent."""

    api_source = _read("api/api_keys.py")
    ui_source = _read("frontend/api_keys_editor.html")
    vps_source = _read("frontend/vps_manager.html")
    for source in (api_source, ui_source, vps_source):
        assert "/sync/push-ssh" not in source
        assert "/sync/ssh-status" not in source
        assert "/sync/ssh-retention" not in source
        assert "Advanced API Sync" not in source
    assert "_file_sync_worker" not in api_source
    assert "createApiSyncStatusController" not in ui_source
    assert "createApiSyncStatusController" not in vps_source


def test_v7_routes_do_not_remote_write_configs() -> None:
    """Ensure V7 save/delete paths no longer perform direct remote writes."""

    source = _read("api/v7_instances.py")
    forbidden = [
        "remote_path_join",
        "remote_shell_path",
        "_open_sftp",
        "SFTP_RETRY_ATTEMPTS",
        "SFTP_RETRY_DELAY",
        "rm -rf",
    ]
    for needle in forbidden:
        assert needle not in source
    assert "cluster_sync" in source


def test_v7_sync_hook_returns_cluster_handoff(tmp_path, monkeypatch) -> None:
    """Ensure the legacy V7 sync hook no longer requires SSH state."""

    import api.v7_instances as v7_instances

    monkeypatch.setattr(v7_instances, "PBGDIR", str(tmp_path))
    instance_dir = tmp_path / "data" / "run_v7" / "demo"
    instance_dir.mkdir(parents=True)
    (instance_dir / "config.json").write_text("{}", encoding="utf-8")

    result = asyncio.run(v7_instances._ssh_sync_instance("demo"))

    assert result["cluster_sync"] is True
    assert result["disabled"] is True
    assert result["hosts"] == {}


def test_api_key_rows_use_data_attributes_instead_of_inline_javascript() -> None:
    """API-key usernames are never compiled as row or button JavaScript."""
    source = _read("frontend/api_keys_editor.html")

    assert 'data-user-name="' in source
    assert 'data-user-action="edit"' in source
    assert 'data-user-action="delete"' in source
    assert 'onclick="editUser(' not in source
    assert 'onclick="event.stopPropagation();editUser(' not in source
    assert 'onclick="event.stopPropagation();confirmDelete(' not in source
    assert 'onkeydown="handleRowKey(' not in source
    assert 'userTableBody.addEventListener("click"' in source
    assert 'userTableBody.addEventListener("keydown"' in source


def test_api_key_attribute_encoding_blocks_context_breakout() -> None:
    """API-key data attributes encode entities, quotes, tags, and backslashes."""
    _run_frontend_node(
        "frontend/api_keys_editor.html",
        ["escapeAttr"],
        "",
        r"""
        const payloads = [
          '<img src=x onerror=alert(1)>',
          '\"><svg onload=alert(1)>',
          "');alert(1);//",
          "\\');alert(1);//",
          '&apos;);alert(1);//',
          'line1\nline2'
        ];
        payloads.forEach(function(payload) {
          const encoded = escapeAttr(payload);
          assert.equal(encoded.includes('<'), false);
          assert.equal(encoded.includes('>'), false);
          assert.equal(encoded.includes('"'), false);
          assert.equal(encoded.includes("'"), false);
        });
        assert.match(escapeAttr('&apos;'), /&amp;apos;/);
        """,
    )


def test_api_key_delegated_actions_preserve_click_and_keyboard_behavior() -> None:
    """Delegated API-key events retain row, edit, delete, and keyboard actions."""
    bootstrap = r"""
        const calls = [];
        const tbody = { contains: function() { return true; } };
        global.document = { getElementById: function() { return tbody; } };
        function editUser(name) { calls.push(['edit', name]); }
        function confirmDelete(name) { calls.push(['delete', name]); }
    """
    assertions = r"""
        const row = { dataset: { userName: "alice');attack();//" } };
        function eventFor(action, key) {
          const actionEl = action ? { dataset: { userAction: action } } : null;
          return {
            key: key || '', stopped: false, prevented: false,
            stopPropagation: function() { this.stopped = true; },
            preventDefault: function() { this.prevented = true; },
            target: {
              nextElementSibling: null, previousElementSibling: null,
              closest: function(selector) {
                if (selector === 'button') return null;
                if (selector === 'tr[data-user-name]') return row;
                if (selector === '[data-user-action]') return actionEl;
                return null;
              }
            }
          };
        }
        handleUserTableClick(eventFor(''));
        handleUserTableClick(eventFor('edit'));
        handleUserTableClick(eventFor('delete'));
        const keyEvent = eventFor('', 'Enter');
        handleUserTableKeydown(keyEvent);
        assert.deepEqual(calls, [
          ['edit', row.dataset.userName], ['edit', row.dataset.userName],
          ['delete', row.dataset.userName], ['edit', row.dataset.userName]
        ]);
        assert.equal(keyEvent.prevented, true);
    """
    _run_frontend_node(
        "frontend/api_keys_editor.html",
        ["handleRowKey", "handleUserTableClick", "handleUserTableKeydown"],
        bootstrap,
        assertions,
    )


def test_hl_expiry_preview_sends_unsaved_key_only_in_post_body() -> None:
    """The browser never places a Hyperliquid private key in the expiry URL."""
    source = _read("frontend/api_keys_editor.html")
    assert "?private_key=" not in source

    bootstrap = r"""
        let unsavedKey = '0x-private-preview-key';
        const calls = [];
        const button = { disabled: false, textContent: '' };
        global.document = { getElementById: function() { return button; } };
        const editingName = 'alice name';
        const hlExpiryData = {};
        function getMaskedFieldValue() { return unsavedKey; }
        async function apiFetch(url, options) { calls.push([url, options || {}]); return { status: 'ok' }; }
        function renderUserTable() {}
        function updateHLExpiryInline() {}
    """
    assertions = r"""
        (async function() {
          await checkSingleHLExpiry();
          unsavedKey = '';
          await checkSingleHLExpiry();
          assert.equal(calls[0][0], '/alice%20name/hl-expiry');
          assert.equal(calls[0][0].includes('private'), false);
          assert.equal(calls[0][1].method, 'POST');
          assert.deepEqual(JSON.parse(calls[0][1].body), { private_key: '0x-private-preview-key' });
          assert.equal(calls[1][0], '/alice%20name/hl-expiry');
          assert.deepEqual(calls[1][1], {});
        }()).catch(function(error) { console.error(error); process.exitCode = 1; });
    """
    _run_frontend_node(
        "frontend/api_keys_editor.html",
        ["checkSingleHLExpiry"],
        bootstrap,
        assertions,
    )


def test_jobs_monitor_escapes_job_data_and_uses_delegated_actions() -> None:
    """Main job cards preserve markup while treating all job fields as text."""
    bootstrap = r"""
        global.document = {
          createElement: function() {
            let text = '';
            return {
              set textContent(value) { text = String(value == null ? '' : value); },
              get innerHTML() { return encodeText(text); }
            };
          }
        };
        const expandedJobs = new Set();
        const expandedDownloaderJobs = new Set();
        const downloaderLogCache = new Map();
        function calculateProgress() { return 42; }
        function formatJobDuration() { return '1m 02s'; }
        function formatBytes(value) { return String(value || 0) + ' B'; }
        function formatTimestamp(value) { return String(value || ''); }
        function fmtDayCompact(value) { return String(value || ''); }
        function renderDownloaderDetails() { return ''; }
    """
    assertions = r"""
        const attack = '<img src=x onerror=globalThis.pwned=true>';
        const idAttack = '&apos;);globalThis.pwned=true;//\\\n' + attack;
        const job = {
          id: idAttack,
          type: attack,
          status: attack,
          updated_ts: attack,
          error: attack,
          payload: { coins: [attack], start_day: attack, end_day: attack },
          progress: {
            coin: attack, chunk_start: attack, chunk_end: attack,
            stage: attack, mode: attack, step: attack, total: 1,
            downloaded_total: 1, skipped_existing_total: 1, failed_total: 1
          }
        };
        const active = renderActiveJob(job);
        const history = renderJob(job);
        [active, history].forEach(function(html) {
          assert.equal(html.includes('<img'), false);
          assert.equal(html.includes('onclick='), false);
          assert.match(html, /data-job-action=/);
          assert.match(html, /&lt;img/);
          assert.match(html, /&amp;apos;/);
        });
    """
    _run_frontend_node(
        "frontend/jobs_monitor.html",
        ["escapeHtml", "escapeAttr", "renderActiveJob", "renderJob"],
        bootstrap,
        assertions,
    )


def test_jobs_monitor_delegation_preserves_all_job_actions() -> None:
    """Delegated job buttons dispatch every existing action with the raw job ID."""
    bootstrap = r"""
        const calls = [];
        function runJob(id) { calls.push(['run', id]); }
        function showJobDetails(id) { calls.push(['view', id]); }
        function showLog(id) { calls.push(['log', id]); }
        function cancelJob(id) { calls.push(['cancel', id]); }
        function retryJob(id) { calls.push(['retry', id]); }
        function requeueJob(id) { calls.push(['requeue', id]); }
        function deleteJob(id) { calls.push(['delete', id]); }
        function toggleExpander(id) { calls.push(['toggle', id]); }
    """
    assertions = r"""
        const id = "job');attack();//";
        ['run','view','log','cancel','retry','requeue','delete','toggle'].forEach(function(action) {
          handleJobActionClick({ target: { closest: function() { return { dataset: { jobAction: action, jobId: id } }; } } });
        });
        assert.deepEqual(calls, [
          ['run',id], ['view',id], ['log',id], ['cancel',id],
          ['retry',id], ['requeue',id], ['delete',id], ['toggle',id]
        ]);
    """
    _run_frontend_node(
        "frontend/jobs_monitor.html",
        ["handleJobActionClick"],
        bootstrap,
        assertions,
    )


def test_hyperliquid_job_monitor_escapes_jobs_and_error_messages() -> None:
    """Embedded job cards and API errors cannot become executable markup."""
    bootstrap = r"""
        function makeElement() {
          let text = '';
          return {
            className: '', style: {},
            set textContent(value) { text = String(value == null ? '' : value); this.innerHTML = encodeText(text); },
            get textContent() { return text; }, innerHTML: '',
            appendChild: function() {}
          };
        }
        const messageElement = makeElement();
        global.document = { createElement: function() { return makeElement(); }, createTextNode: function(value) { return { textContent: String(value) }; } };
        function $(id) { return messageElement; }
        const expandedJobs = { dl: new Set(), build: new Set() };
        const P = '';
        function calcPct() { return 42; }
        function fmtBytes(value) { return String(value || 0) + ' B'; }
        function fmtTS(value) { return String(value || ''); }
        function fmtDay(value) { return String(value || ''); }
        function formatJobDuration() { return '1m 02s'; }
    """
    assertions = r"""
        const attack = '<img src=x onerror=globalThis.pwned=true>';
        const job = {
          id: '&apos;);globalThis.pwned=true;//' + attack,
          type: attack, status: attack, error: attack, updated_ts: attack,
          payload: { coins: [attack], start_day: attack, end_day: attack },
          progress: {
            coin: attack, chunk_start: attack, chunk_end: attack, stage: attack, mode: attack, total: 1,
            downloaded_total: 1, downloaded_bytes_total: attack
          }
        };
        const active = renderActiveJob('dl', job);
        const history = renderHistoryJob('dl', job);
        [active, history].forEach(function(html) {
          assert.equal(html.includes('<img'), false);
          assert.match(html, /&lt;img/);
          assert.match(html, /data-action=/);
        });
        showMsg('dl', 'error', attack);
        assert.equal(messageElement.textContent, attack);
        assert.equal(messageElement.innerHTML.includes('<img'), false);
        assert.match(messageElement.innerHTML, /&lt;img/);
    """
    _run_frontend_node(
        "frontend/hl_data_actions.html",
        ["escHtml", "escAttr", "showMsg", "renderActiveJob", "renderHistoryJob"],
        bootstrap,
        assertions,
    )


def test_hyperliquid_success_message_keeps_existing_structure_without_inner_html() -> None:
    """Queued-job success messages retain strong, break, and small elements safely."""
    source = _extract_js_function(_read("frontend/hl_data_actions.html"), "showQueuedMsg")

    assert "createElement('strong')" in source
    assert "createElement('br')" in source
    assert "createElement('small')" in source
    assert ".innerHTML" not in source


def test_xss_hardening_preserves_job_and_api_key_visual_contract() -> None:
    """Security changes retain the existing classes, labels, and button order."""
    api_keys = _read("frontend/api_keys_editor.html")
    jobs = _extract_js_function(_read("frontend/jobs_monitor.html"), "renderActiveJob")
    history = _extract_js_function(_read("frontend/jobs_monitor.html"), "renderJob")
    hl_jobs = _extract_js_function(_read("frontend/hl_data_actions.html"), "renderActiveJob")

    assert 'class="btn btn-sm btn-info" data-user-action="edit">Edit</button>' in api_keys
    assert 'class="btn btn-sm btn-danger" data-user-action="delete">Delete</button>' in api_keys
    assert jobs.index('>Run</button>') < jobs.index('>View</button>') < jobs.index('>Log</button>') < jobs.index('>Cancel</button>')
    assert history.index('>View</button>') < history.index('>Log</button>') < history.index('>Retry</button>') < history.index('>Requeue</button>') < history.index('>Delete</button>')
    assert 'class="job-card"' in jobs
    assert 'class="progress-bar"' in jobs
    assert 'class="hlda-jc"' in hl_jobs
    assert 'class="hlda-pb"' in hl_jobs
