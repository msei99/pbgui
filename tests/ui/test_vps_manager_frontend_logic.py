"""Regression tests for VPS Manager frontend state handling."""

from __future__ import annotations

import subprocess
import textwrap
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
HTML_PATH = ROOT / "frontend" / "vps_manager.html"
LOG_VIEWER_PATH = ROOT / "frontend" / "js" / "log_viewer_panel.js"


def test_unknown_ssh_host_confirmation_uses_exact_fingerprint() -> None:
    """Display and resubmit the exact SSH host-key fingerprint before trust."""
    source = HTML_PATH.read_text(encoding="utf-8")

    assert "hostKey.needs_confirmation" in source
    assert "accepted_host_key_fingerprint = fingerprint" in source
    assert "accepted_host_key_fingerprint: String(msg.fingerprint || '')" in source
    assert "replace_existing_host_key = changed" in source
    assert "Review changed key" in source


def test_add_vps_offers_pb8_live_only_profile() -> None:
    """Fresh VPS setup offers PB7, PB8-only, and combined runtimes."""
    source = HTML_PATH.read_text(encoding="utf-8")

    assert "runtime_profile: 'pb7'" in source
    assert "PB8 Live only (no PB7)" in source
    assert "PB7 + PB8" in source
    assert "PB8-only setup does not clone or install PB7" in source
    assert "pb8Requested: ['pb8', 'pb7_pb8'].includes(String(store.addForm.runtime_profile || 'pb7'))" in source
    assert "Setup and PB8 Live installation successful." in source


def _extract_function(source: str, name: str) -> str:
    """Extract a named JavaScript function from the inline VPS manager script."""
    marker = f"function {name}("
    start = source.find(marker)
    assert start >= 0, f"Could not find JavaScript function {name!r}"

    brace_start = source.find("{", start)
    assert brace_start >= 0, f"Could not find opening brace for {name!r}"

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
            continue
        if char == "{":
            depth += 1
            continue
        if char == "}":
            depth -= 1
            if depth == 0:
                return source[start : index + 1]

    raise AssertionError(f"Could not extract complete JavaScript function {name!r}")


def _run_node_assertions(function_names: list[str], *, bootstrap: str, assertions: str) -> None:
    """Run focused Node assertions against extracted frontend helpers."""
    source = HTML_PATH.read_text(encoding="utf-8")
    extracted = "\n\n".join(_extract_function(source, name) for name in function_names)
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');

        {bootstrap}

        {extracted}

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
        "Node-backed frontend regression failed\n"
        f"STDOUT:\n{result.stdout}\n"
        f"STDERR:\n{result.stderr}"
    )


class TestVpsManagerFrontendLogic:
    """Lock down VPS Manager form behavior against live metadata refreshes."""

    def test_pb8_install_actions_use_filled_blue_emphasis(self) -> None:
        """Uninstalled PB8 actions stand out while installed and update states retain their colors."""
        source = HTML_PATH.read_text(encoding="utf-8")
        sidebar_source = _extract_function(source, "renderSidebarActions")

        assert ".sb-btn.install {" in source
        assert "background: #3182ce;" in source
        assert "st.pb8_installed ? 'sb-btn ok' : 'sb-btn install'" in sidebar_source
        assert sidebar_source.count("st.pb8_installed ? 'sb-btn ok' : 'sb-btn install'") == 2
        assert "st.pb7_installed ? 'sb-btn ok' : 'sb-btn install'" in sidebar_source
        assert "st.pb7_installed ? 'Update PB7' : 'Install PB7'" in sidebar_source

    def test_pb7_install_is_disabled_when_backend_disk_gate_blocks_it(self) -> None:
        """A low-disk PB8-only VPS exposes the PB7 reason without allowing a click."""
        source = HTML_PATH.read_text(encoding="utf-8")
        sidebar_source = _extract_function(source, "renderSidebarActions")

        assert "const pb7ActionAllowed = st.pb7_action_allowed !== false" in sidebar_source
        assert "pb7ActionAllowed ? '' : 'disabled'" in sidebar_source
        assert "st.pb7_action_reason" in sidebar_source
        signature_source = _extract_function(source, "getSidebarActionsSignature")
        assert "pb7_allowed: st.pb7_action_allowed !== false" in signature_source
        assert "pb7_reason: String(st.pb7_action_reason || '')" in signature_source

    def test_pb8_only_sidebar_uses_pb8_update_matrix(self) -> None:
        """PB8-only and dual-runtime hosts expose the appropriate PBGui/PB8 updates."""
        source = HTML_PATH.read_text(encoding="utf-8")
        sidebar_source = _extract_function(source, "renderSidebarActions")

        assert "const pb8Only = !!st.pb8_installed && !st.pb7_installed" in sidebar_source
        assert "pb8Only ? 'vps-update-pbgui-pb8' : 'vps-update-pb'" in sidebar_source
        assert "pb8Only ? 'Update PBGui and PB8' : 'Update PBGui and PB7'" in sidebar_source
        assert "const masterHasBothRuntimes = !!st.pb7_installed && !!st.pb8_installed" in sidebar_source
        assert "masterHasBothRuntimes ? `<button" in sidebar_source
        assert 'runMasterWithLog("master-update-pbgui-pb8", "Update PBGui and PB8")' in sidebar_source
        assert "const hasBothRuntimes = !!st.pb7_installed && !!st.pb8_installed" in sidebar_source
        assert "hasBothRuntimes ? `<button" in sidebar_source
        assert "data-command='vps-update-pbgui-pb8' data-command-text='Update PBGui and PB8'" in sidebar_source
        assert "st.pb8_installed ? 'Updates the installed PB8 runtime.'" in sidebar_source
        assert "PB8-only leaves PBRun disabled" not in source

    def test_pb8_branch_sidebar_views_and_hash_routes_are_installed_only(self) -> None:
        """Installed PB8 runtimes expose branch views with master and VPS context routes."""
        source = HTML_PATH.read_text(encoding="utf-8")
        sidebar_source = _extract_function(source, "renderSidebarActions")
        view_hash_source = _extract_function(source, "viewHash")
        restore_source = _extract_function(source, "restoreViewFromHash")
        render_view_source = _extract_function(source, "renderView")
        branch_panel_source = _extract_function(source, "renderBranchPanelForCurrentView")
        host_switch_source = _extract_function(source, "selectSidebarVpsHost")

        assert sidebar_source.count("st.pb8_installed ? `<button") == 2
        assert 'selectView("master-pb8-branch")' in sidebar_source
        assert "data-view='vps-pb8-branch'" in sidebar_source
        assert "'#master/pb8-branch'" in view_hash_source
        assert "'/pb8-branch'" in view_hash_source
        assert "selectView('master-pb8-branch')" in restore_source
        assert "selectView('vps-pb8-branch', hostname)" in restore_source
        assert "renderMasterBranchView('pb8')" in render_view_source
        assert "renderVpsBranchView('pb8')" in render_view_source
        assert branch_panel_source.count("(detail.status || {}).pb8_installed") == 2
        assert "store.view === 'vps-pb8-branch' && !row.pb8_installed" in host_switch_source

    def test_pb7_and_pb8_branch_state_and_remote_caches_are_independent(self) -> None:
        """Each runtime keeps distinct branch selections and remote response caches."""
        assertions = """
        const master = defaultMasterUi();
        const vps = defaultVpsUi();

        for (const ui of [master, vps]) {
          ui.pb7Branch = 'v7';
          ui.pb8Branch = 'v8';
          ui.pb7RemoteBranches.push('seven');
          ui.pb8RemoteBranches.push('eight');
          ui.pb7RemoteCommits.seven = [{ full: '777' }];
          ui.pb8RemoteCommits.eight = [{ full: '888' }];

          assert.equal(ui.pb7Branch, 'v7');
          assert.equal(ui.pb8Branch, 'v8');
          assert.deepEqual(ui.pb7RemoteBranches, ['seven']);
          assert.deepEqual(ui.pb8RemoteBranches, ['eight']);
          assert.deepEqual(ui.pb7RemoteCommits.seven, [{ full: '777' }]);
          assert.deepEqual(ui.pb8RemoteCommits.eight, [{ full: '888' }]);
          assert.notEqual(ui.pb7RemoteBranches, ui.pb8RemoteBranches);
          assert.notEqual(ui.pb7RemoteCommits, ui.pb8RemoteCommits);
        }
        """
        _run_node_assertions(
            ["defaultMasterUi", "defaultVpsUi"],
            bootstrap="",
            assertions=assertions,
        )

    def test_remote_branch_responses_follow_the_active_runtime_context(self) -> None:
        """WebSocket responses remain bound to their requesting runtime after navigation."""
        bootstrap = """
        const store = {
          view: 'master-pb8-branch',
          pendingBranchRequests: {
            eight: { runtime: 'pb8', scope: 'master', kind: 'branches', remoteUrl: 'https://example/pb8.git', branch: '' },
            seven: { runtime: 'pb7', scope: 'master', kind: 'commits', remoteUrl: 'https://example/pb7.git', branch: 'v7-dev' }
          },
          master: {
            pb7RemoteBranches: ['keep-seven'],
            pb7RemoteCommits: {},
            pb7RemoteCommitsRequest: 'seven',
            pb8RemoteBranches: [],
            pb8RemoteCommits: {},
            pb8RemoteBranchesRequest: 'eight',
            pb7BranchSelectInteracting: false,
            pb8BranchSelectInteracting: false
          },
          pb7BranchSelectInteracting: false,
          pb8BranchSelectInteracting: false
        };
        let syncs = 0;
        function isMasterContextView() { return true; }
        function isVpsContextView() { return false; }
        function syncBranchViewPanel() { syncs += 1; }
        function ensureVpsUi() { throw new Error('VPS state must not be used'); }
        """
        assertions = """
        handleMessage({ type: 'remote_branches', request_id: 'eight', remote_url: 'https://example/pb8.git', branches: ['v8-dev'] });
        assert.deepEqual(store.master.pb8RemoteBranches, ['v8-dev']);
        assert.deepEqual(store.master.pb7RemoteBranches, ['keep-seven']);
        assert.equal(store.master.pb8RemoteUrl, 'https://example/pb8.git');

        store.view = 'master-pb7-branch';
        handleMessage({ type: 'remote_branch_commits', request_id: 'seven', remote_url: 'https://example/pb7.git', branch: 'v7-dev', commits: [{ full: '777' }] });
        assert.deepEqual(store.master.pb7RemoteCommits['v7-dev'], [{ full: '777' }]);
        assert.deepEqual(store.master.pb8RemoteCommits, {});
        assert.equal(syncs, 2);
        """
        _run_node_assertions(
            ["runtimeBranchKeys", "isPb7BranchView", "isPb8BranchView", "activeRuntimeBranchRepo", "takeRuntimeBranchRequest", "handleMessage"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_remote_head_is_used_without_manual_remote_branch_override(self) -> None:
        """HEAD uses the loaded selected remote instead of stale local branch history."""
        bootstrap = """
        const ui = {
          pb8Branch: 'feature',
          pb8Commit: 'HEAD',
          pb8ManualBranch: '',
          pb8ManualCommit: '',
          pb8RemoteBranches: ['feature'],
          pb8RemoteCommits: { feature: [{ full: 'remote-head' }, { full: 'current' }] }
        };
        const state = {
          current_branch: 'feature',
          current_commit: 'current',
          branches: { feature: [{ full: 'stale-local' }, { full: 'current' }] }
        };
        """
        assertions = """
        const model = pb7BranchModel(state, ui, 'pb8');
        assert.equal(model.effectiveCommit, 'remote-head');
        assert.equal(model.targetCommit, 'remote-head');
        assert.equal(model.isBehindOrigin, true);
        assert.equal(model.commitsBehind, 1);
        assert.equal(model.usingRemoteCommitHistory, true);
        """
        _run_node_assertions(
            ["runtimeBranchKeys", "branchModel", "pb7BranchModel"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_pb8_branch_actions_use_update_commands_and_only_pb8_vars(self) -> None:
        """PB8 branch actions use existing update commands with the contracted payload."""
        bootstrap = """
        const calls = [];
        const pb8Ui = {
          pb8Branch: 'v8-feature',
          pb8Commit: 'HEAD',
          pb8RemoteName: 'fork',
          pb8RemoteUrl: 'https://example/pb8.git',
          pb8ManualBranch: 'v8-feature',
          pb8ManualCommit: 'cafebabe',
          pb8RemoteBranches: ['v8-feature'],
          pb8RemoteCommits: { 'v8-feature': [{ full: 'cafebabe' }] }
        };
        const branchState = {
          current_branch: 'master',
          current_commit: 'deadbeef',
          branches: { master: [{ full: 'deadbeef' }], 'v8-feature': [{ full: 'cafebabe' }] },
          remote_urls: { fork: 'https://example/pb8.git' },
          default_remote_name: 'fork'
        };
        const store = {
          master: Object.assign({}, pb8Ui),
          vps: { runner: Object.assign({}, pb8Ui) },
          detail: { kind: 'master', branches: { pb8: branchState } }
        };
        function ensureVpsUi(hostname) { return store.vps[hostname]; }
        function shortHash(value) { return String(value || '').slice(0, 8); }
        function runMasterWithLog(command, label, extraVars) { calls.push({ scope: 'master', command, label, extraVars }); }
        function runVpsWithLog(hostname, command, label, extraVars) { calls.push({ scope: hostname, command, label, extraVars }); }
        """
        assertions = """
        triggerPb7BranchAction('master', 'pb8');
        store.detail = { kind: 'vps', hostname: 'runner', branches: { pb8: branchState } };
        triggerPb7BranchAction('runner', 'pb8');

        assert.equal(calls[0].command, 'master-update-pb8');
        assert.equal(calls[1].command, 'vps-update-pb8');
        for (const call of calls) {
          assert.deepEqual(Object.keys(call.extraVars).sort(), [
            'pb8_branch',
            'pb8_commit',
            'pb8_remote_name',
            'pb8_remote_url',
            'pb8_source_branch'
          ]);
          assert.equal(call.extraVars.pb8_branch, 'v8-feature');
          assert.equal(call.extraVars.pb8_source_branch, 'v8-feature');
          assert.equal(call.extraVars.pb8_commit, 'cafebabe');
          assert.equal(call.extraVars.pb8_remote_name, 'fork');
          assert.equal(call.extraVars.pb8_remote_url, 'https://example/pb8.git');
          assert.equal(Object.keys(call.extraVars).some(key => key.startsWith('pb7_')), false);
        }
        """
        _run_node_assertions(
            ["runtimeBranchKeys", "branchModel", "pb7BranchModel", "triggerPb7BranchAction"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_pb8_branch_selects_hold_live_rerenders_and_guides_match(self) -> None:
        """PB8 selectors resist live replacement and both guides document explicit tracking."""
        source = HTML_PATH.read_text(encoding="utf-8")
        render_ui_source = _extract_function(source, "renderUi")
        sync_source = _extract_function(source, "syncBranchViewPanel")
        panel_source = _extract_function(source, "renderPb7BranchPanel")
        english = (ROOT / "docs" / "help" / "32_vps_manager.md").read_text(encoding="utf-8")
        german = (ROOT / "docs" / "help_de" / "32_vps_manager.md").read_text(encoding="utf-8")

        assert "holdPb8BranchView" in render_ui_source
        assert "store.pb8BranchSelectInteracting && isPb8BranchView()" in sync_source
        assert panel_source.count("onfocus='store.${interactionField} = true'") >= 5
        assert "PB8 Branch Management" in english
        assert "PB8 Branch Management" in german
        assert "normal **Update PB8**" in english
        assert "normale Aktion **Update PB8**" in german

    def test_combined_update_button_warns_only_when_both_components_need_updates(self) -> None:
        """Combined PBGui/runtime actions must not duplicate one component's warning state."""
        _run_node_assertions(
            ["combinedUpdateButtonClass"],
            bootstrap="",
            assertions="""
              assert.equal(combinedUpdateButtonClass(false, false), 'sb-btn ok');
              assert.equal(combinedUpdateButtonClass(true, false), 'sb-btn ok');
              assert.equal(combinedUpdateButtonClass(false, true), 'sb-btn ok');
              assert.equal(combinedUpdateButtonClass(true, true), 'sb-btn warning');
            """,
        )
        source = HTML_PATH.read_text(encoding="utf-8")
        sidebar_source = _extract_function(source, "renderSidebarActions")
        assert sidebar_source.count("combinedUpdateButtonClass(st.pbgui_update_available") == 4

    def test_manual_refresh_reloads_current_detail_for_update_indicators(self) -> None:
        """A completed git refresh must immediately reload the detail that controls update colors."""
        source = HTML_PATH.read_text(encoding="utf-8")
        refresh_branch = source.split("} else if (cmd === 'refresh') {", 1)[1].split(
            "} else if (cmd === 'read_vps_settings')", 1
        )[0]

        assert "fetchMasterDetail();" in refresh_branch
        assert "fetchVpsDetail(store.hostname);" in refresh_branch

    def test_bulk_runtime_updates_are_profile_aware(self) -> None:
        """Overview bulk actions delegate runtime selection to each host profile."""
        source = HTML_PATH.read_text(encoding="utf-8")
        sidebar_source = _extract_function(source, "renderSidebarActions")

        assert 'deploySelectedVpsAction("vps-update-runtime", "Update Runtime by Profile")' in sidebar_source
        assert 'deploySelectedVpsAction("vps-update-pbgui-runtime", "Update PBGui and Runtime by Profile")' in sidebar_source
        assert 'deploySelectedVpsAction("vps-update-pb7", "Update PB7")' not in sidebar_source

    def test_vps_monitor_combines_pb7_and_pb8_runtime_rows(self) -> None:
        """Detailed and fallback monitor rows retain PB7/PB8 identity in one table."""
        source = HTML_PATH.read_text(encoding="utf-8")
        render_monitor = _extract_function(source, "renderMonitorPanel")
        live_update = _extract_function(source, "_liveUpdateServiceRows")

        assert "const runtimeRows = pb7Rows.concat(pb8Rows)" in render_monitor
        assert "const runtimeFallback = pb7Fallback.concat(pb8Fallback)" in render_monitor
        assert "No running PB7 or PB8 instances reported." in source
        assert "data-bot-key" in source
        assert "var rowKey = pbVersion + ':' + name" in live_update

    def test_firewall_validation_accepts_ipv4_cidr_networks(self) -> None:
        """Allow the IPv4 CIDR sources that remote UFW discovery stores locally."""
        _run_node_assertions(
            ["isValidIPv4", "validateFirewallIps"],
            bootstrap="",
            assertions="""
            assert.equal(validateFirewallIps('198.51.100.1,10.8.0.0/24').ok, true);
            assert.equal(validateFirewallIps('0.0.0.0/0').ok, true);
            assert.equal(validateFirewallIps('10.8.0.0/33').ok, false);
            assert.equal(validateFirewallIps('2001:db8::/32').ok, false);
            """,
        )

    def test_log_viewer_restart_waits_for_websocket_reconnect(self) -> None:
        """Reconnects pre-arm the log cursor before sending a queued restart."""
        source = LOG_VIEWER_PATH.read_text(encoding="utf-8")

        assert "this._pendingRestartCommand = command" in source
        assert "if (!me._flushPendingRestart(ws)) me._subscribe()" in source
        assert "this._prepareRestartStream(cmd)" in source
        assert "this._sendPreparedRestart(msg.sid)" in source
        assert "this._send(attempt.command)" in source
        assert "_sendRestart(" not in source

    def test_vps_manager_send_queues_commands_while_websocket_reconnects(self) -> None:
        """VPS Manager commands must not be dropped while its WebSocket reconnects."""
        source = HTML_PATH.read_text(encoding="utf-8")

        assert "pendingWsMessages: []" in source
        assert "queueWsMessage(payload)" in source
        assert "flushPendingWsMessages()" in source
        assert "store.pendingWsMessages = []" in source

    def test_remote_master_update_switches_to_actual_task_log(self) -> None:
        """Use the backend-selected master log after a VPS update command is remapped."""
        source = HTML_PATH.read_text(encoding="utf-8")
        handle_result = _extract_function(source, "handleResult")

        assert "const actualFile = String(data.file_alias || '')" in handle_result
        assert "if (actualFile) task.file = actualFile" in handle_result
        assert "task.startEmpty = false" in handle_result
        assert "task.runKey = String(Date.now())" in handle_result
        assert "taskLogViewerKey = ''" in handle_result

    def test_new_task_log_stays_empty_until_backend_confirms_rotation(self) -> None:
        """Do not show the prior run, then reload the new file from its beginning."""
        source = HTML_PATH.read_text(encoding="utf-8")
        open_master = _extract_function(source, "openMasterTaskLog")
        open_vps = _extract_function(source, "openVpsTaskLog")
        handle_result = _extract_function(source, "handleResult")

        assert "startEmpty: true" in open_master
        assert "startEmpty: true" in open_vps
        assert "task.startEmpty = false" in handle_result
        assert handle_result.count("taskLogViewerKey = ''") >= 2

    def test_host_detail_context_and_full_hydration_have_separate_generations(self) -> None:
        """Quick socket updates must not cancel same-host background detail hydration."""
        source = HTML_PATH.read_text(encoding="utf-8")
        handle_message = _extract_function(source, "handleMessage")
        send_context = _extract_function(source, "sendContext")
        select_view = _extract_function(source, "selectView")

        assert "contextGeneration: 1" in source
        assert "context_generation: store.contextGeneration" in send_context
        assert "incomingGeneration !== store.contextGeneration" in handle_message
        assert "store.detailAbortController.abort()" not in handle_message
        assert "buildProvisionalVpsDetail(nextHostname)" in select_view

    def test_host_switch_builds_immediate_detail_from_overview_snapshot(self) -> None:
        """A never-visited host renders its local summary without waiting for detail I/O."""
        bootstrap = """
        const store = {};
        function getOverviewRows() {
          return [{
            hostname: 'manibot62',
            online: true,
            ssh_online: true,
            telemetry_fresh: true,
            role: 'vps',
            updates: 3,
            task_command: 'vps-update',
            task_command_text: 'Update Linux',
            task_status: 'running'
          }];
        }
        """
        assertions = """
        const detail = buildProvisionalVpsDetail('manibot62');
        assert.equal(detail.kind, 'vps');
        assert.equal(detail.hostname, 'manibot62');
        assert.equal(detail.provisional, true);
        assert.equal(detail.status.online, true);
        assert.equal(detail.status.pending_updates, 3);
        assert.equal(detail.status.summary_row.role, 'vps');
        assert.equal(detail.progress.command, 'vps-update');
        """
        _run_node_assertions(
            ["buildProvisionalVpsDetail"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_new_vps_setup_context_survives_stale_overview_snapshot(self) -> None:
        """An older overview snapshot cannot eject a newly initialized host from its task log."""
        bootstrap = """
        const store = {
          view: 'vps-task-log',
          hostname: 'fresh-runner',
          detail: null,
          pendingAutoSetupSwitch: { hostname: 'fresh-runner' },
          _pendingView: '',
          _pendingHostname: ''
        };
        const history = { replaceState: function () {} };
        let contextSends = 0;
        function sendContext() { contextSends += 1; }
        function isVpsContextView() { return store.view === 'vps-task-log'; }
        function viewUrl(view) { return '#' + view; }
        """
        assertions = """
        const staleState = { overview: { rows: [] } };
        assert.equal(reconcileCurrentVpsContext(staleState), false);
        assert.equal(store.view, 'vps-task-log');
        assert.equal(store.hostname, 'fresh-runner');
        assert.equal(contextSends, 0);
        store.pendingAutoSetupSwitch = null;
        assert.equal(reconcileCurrentVpsContext(staleState), true);
        assert.equal(store.view, 'overview');
        assert.equal(store.hostname, '');
        assert.equal(contextSends, 1);
        """
        _run_node_assertions(
            ["normalizeOverviewHostname", "hasManagedOverviewHost", "reconcileCurrentVpsContext"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_pb8_setup_completion_uses_the_combined_setup_status(self) -> None:
        """The Add flow no longer waits for a separate PB8 update command."""
        source = HTML_PATH.read_text(encoding="utf-8")
        handle_message = _extract_function(source, "handleMessage")

        assert "Setup and PB8 Live installation successful." in handle_message
        assert "Starting PB8 Live installation" not in handle_message
        assert "activeCommand === 'vps-update-pb8'" not in handle_message
        assert "switchToVpsTaskLog(store.detail.hostname, 'vps-update-pb8'" not in handle_message

    def test_metric_history_reuses_cache_and_rejects_stale_responses(self) -> None:
        """Local history renders cached data immediately and aborts obsolete requests."""
        source = HTML_PATH.read_text(encoding="utf-8")
        open_history = _extract_function(source, "openMetricHistory")

        assert "metricHistoryCache[cacheKey]" in open_history
        assert "cpuHistoryAbortController.abort()" in open_history
        assert "generation !== cpuHistoryRequestGeneration" in open_history
        assert "signal: cpuHistoryAbortController.signal" in open_history

    def test_today_log_matches_show_loading_and_reject_stale_responses(self) -> None:
        """Error and traceback clicks respond immediately while one request is active."""
        source = HTML_PATH.read_text(encoding="utf-8")
        fetch_matches = _extract_function(source, "fetchBotLogMatches")
        handle_message = _extract_function(source, "handleMessage")

        assert "openBotLogMatchesLoadingModal" in fetch_matches
        assert "request_id: requestId" in fetch_matches
        assert "pendingBotLogMatchRequestId" in handle_message

    def test_bot_log_click_preserves_pb_runtime_version(self) -> None:
        """PB7 and PB8 telemetry links open the matching native bot log."""
        bootstrap = """
        const store = { detail: null };
        const ui = { log: {} };
        let openedHost = '';
        function ensureVpsUi() { return ui; }
        function openVpsHostLogs(hostname) { openedHost = hostname; }
        function metricHistoryMeta() { return { label: 'Memory' }; }
        """
        assertions = """
        openBotLog('pb-host', 'pb8_bot', '8');
        assert.equal(ui.log.filename, 'software/pb8/logs/pb8_bot.log');
        assert.equal(openedHost, 'pb-host');
        assert.equal(normalizeVpsLogViewerTarget(ui.log.filename), 'Bot:pb8_bot:8');

        openBotLog('pb-host', 'pb7_bot', '7');
        assert.equal(ui.log.filename, 'software/pb7/logs/pb7_bot.log');
        assert.equal(normalizeVpsLogViewerTarget(ui.log.filename), 'Bot:pb7_bot:7');
        assert.equal(metricHistoryTitle('memory', 'pb-host', '8:pb8_bot'), 'Memory History - pb8_bot @ pb-host');
        """
        _run_node_assertions(
            ["normalizeVpsLogViewerTarget", "openBotLog", "metricHistoryTitle"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_pb8_summary_only_pnl_history_explains_missing_daily_curve(self) -> None:
        """An authoritative PB8 total must not be presented as a fabricated daily series."""

        bootstrap = """
        const METRIC_HISTORY_META = {
          pnl: { pnlMetric: true, empty: 'No bot PNL history available yet.' },
          pnl_fills: { fillsMetric: true, empty: 'No bot fill history available yet.' }
        };
        """
        assertions = """
        const payload = { bot_name: '8:pb8_bot', summary_only: true };
        assert.equal(
          metricHistoryEmpty('pnl', payload),
          'PB8 provides an authoritative total, but no reliable daily net-PNL breakdown.'
        );
        assert.equal(
          metricHistoryEmpty('pnl_fills', payload),
          'PB8 provides an authoritative total, but no reliable daily net-PNL breakdown.'
        );
        """
        _run_node_assertions(
            ["metricHistoryMeta", "metricHistoryEmpty"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_vps_manager_delete_and_remote_purge_are_separate_actions(self) -> None:
        """Deleting a VPS record stays local while remote install purge is explicit."""
        source = HTML_PATH.read_text(encoding="utf-8")

        assert "This only removes the saved VPS Manager record from PBGui" in source
        assert "function confirmPurgeVpsInstall(hostname)" in source
        assert "vpsActionWithPw(hostname, 'vps-purge-install', 'Purge VPS Install')" in source
        assert "data-vps-action='purge-vps'" in source
        assert "data-host='${escAttr(selectedHost)}'>Purge VPS Install</button>" in source

    def test_existing_vps_host_key_can_be_repaired_in_gui(self) -> None:
        """Expose review, exact confirmation, replacement, and reconnect in the VPS sidebar."""
        source = HTML_PATH.read_text(encoding="utf-8")
        sidebar_source = _extract_function(source, "renderSidebarActions")
        main_source = _extract_function(source, "renderVpsView")

        assert "function reviewVpsSshHostKey(hostname)" in source
        assert "function openSshHostKeyReviewModal(data)" in source
        assert "const hostKeyBtnClass = st.host_key_error || (hostKeyStatus !== 'known' && hostKeyStatus !== 'local') ? 'sb-btn warning' : 'sb-btn'" in sidebar_source
        assert "data-vps-action='review-host-key'" in sidebar_source
        assert "data-host='${escAttr(selectedHost)}'>Review SSH Host Key</button>" in sidebar_source
        assert "reviewVpsSshHostKey" not in main_source
        assert "Replace Key & Reconnect" in source
        assert "expected_fingerprint: fingerprint" in source
        assert "replace_existing: status === 'mismatch'" in source
        assert "Monitor reconnect requested" in source
        assert "class='ssh-host-key-targets'" in source
        assert "class='status-sub code ssh-host-key-fingerprint'" in source
        assert "overflow-wrap: anywhere" in source

    def test_existing_vps_import_can_replace_confirmed_changed_host_key(self) -> None:
        """The import preview exposes changed-key replacement with exact fingerprint replay."""

        source = HTML_PATH.read_text(encoding="utf-8")
        preview_source = _extract_function(source, "renderExistingVpsImportPreview")
        trust_source = _extract_function(source, "trustExistingVpsImportHostKey")

        assert "Replace stored key and probe again" in preview_source
        assert "Verify this fingerprint through a trusted channel" in preview_source
        assert "flow.form.accept_unknown_host = true" in trust_source
        assert "flow.form.accepted_host_key_fingerprint = fingerprint" in trust_source

    def test_overview_shows_ssh_host_key_status(self) -> None:
        """Show trusted, unknown, changed, and failed SSH keys in Overview."""
        source = HTML_PATH.read_text(encoding="utf-8")
        table_source = _extract_function(source, "renderOverviewTable")

        assert "{ key: 'ssh_key', label: 'SSH Key', defaultVisible: true" in source
        assert "levelTag('ok', 'Trusted')" in table_source
        assert "levelTag('warning', 'Unknown')" in table_source
        assert "levelTag('error', 'Changed')" in table_source
        assert "levelTag('error', 'Failed')" in table_source

    def test_vps_manager_add_to_cluster_runs_complete_join(self) -> None:
        """VPS Manager exposes the complete guarded Cluster onboarding flow."""

        source = HTML_PATH.read_text(encoding="utf-8")

        assert "function addVpsToCluster(hostname)" in source
        assert "startClusterOnboard(target)" in source
        assert "function renderClusterOnboardProgressModal()" in source
        assert "function pollClusterOnboardProgress(jobId)" in source
        assert "'/cluster-onboard/' + encodeURIComponent(target) + '/start'" in source
        assert "if (isClusterOnboardModalVisible() && isClusterOnboardRunning()) return;" in source
        assert source.count("if (isClusterOnboardModalVisible() && isClusterOnboardRunning()) return;") >= 2
        assert "closeButton.style.display = running ? 'none' : ''" in source
        assert "clusterNodeAction === 'resume' ? 'Resume Cluster Onboarding'" in source
        assert "id='cluster-onboard-retry'>Retry</button>" in source
        assert "retryButton.onclick = function () { startClusterOnboard(String(flow.hostname || '')); };" in source
        assert "install restricted Cluster keys" in source
        assert "verify that no conflicting identity exists" in source
        assert "VPS joined and synchronized with Cluster." in source
        assert "${showClusterNodeBtn ? `<button class='${clusterNodeBtnClass}'" in source

    def test_add_vps_initialize_requires_green_preflight_checks(self) -> None:
        """Initialize stays blocked until required pre-flight checks pass."""
        bootstrap = """
        const store = { addVpsReady: { hostsOk: false, sshOk: true, loading: false } };
        function hasSessionSecret(hostname, field) { return false; }
        function validateFirewallIps(value) { return { ok: true }; }
        """
        assertions = """
        const validForm = {
          ip: '203.0.113.10', hostname: 'manibot40', user: 'mani', user_pw: 'user-pw',
          init_methode: 'root', initial_root_pw: 'root-pw', root_pw: 'new-root-pw',
          swap: '3G', install_dir: '/home/mani/software', firewall_ssh_ips: '198.51.100.1'
        };
        assert.equal(canInitForm(validForm), false);
        store.addVpsReady = { hostsOk: true, sshOk: false, loading: false };
        assert.equal(canInitForm(validForm), false);
        store.addVpsReady = { hostsOk: true, sshOk: true, loading: true };
        assert.equal(canInitForm(validForm), false);
        store.addVpsReady = { hostsOk: true, sshOk: true, loading: false };
        assert.equal(canInitForm(validForm), true);
        """
        _run_node_assertions(
            ["hasInvalidPasswordChars", "addVpsPreflightReady", "canInitForm"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_add_vps_initialize_click_is_guarded(self) -> None:
        """A stale enabled button cannot send init_vps while pre-flight is red."""
        bootstrap = """
        const sent = [];
        const toasts = [];
        const store = {
          addVpsReady: { hostsOk: false, sshOk: true, loading: false },
          master: { debug: false },
          addForm: {
            ip: '203.0.113.10', hostname: 'manibot40', user: 'mani', user_pw: 'user-pw',
            init_methode: 'root', initial_root_pw: 'root-pw', root_pw: 'new-root-pw',
            swap: '3G', install_dir: '/home/mani/software', firewall_ssh_ips: '198.51.100.1'
          }
        };
        function hasSessionSecret(hostname, field) { return false; }
        function validateFirewallIps(value) { return { ok: true }; }
        function toast(message, level) { toasts.push({ message, level }); }
        function send(payload) { sent.push(payload); }
        """
        assertions = """
        initAddVps();
        assert.equal(sent.length, 0);
        assert.equal(toasts[0].level, 'warning');
        store.addVpsReady = { hostsOk: true, sshOk: true, loading: false };
        initAddVps();
        assert.equal(sent.length, 1);
        assert.equal(sent[0].cmd, 'init_vps');
        """
        _run_node_assertions(
            ["hasInvalidPasswordChars", "addVpsPreflightReady", "canInitForm", "initAddVps"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_add_vps_preflight_result_refreshes_init_ready_state(self) -> None:
        """The async pre-flight result must refresh the top Init ready card and button."""
        source = HTML_PATH.read_text(encoding="utf-8")
        marker = "if (msg.type === 'vps_ready_result')"
        start = source.find(marker)
        assert start >= 0
        block = source[start : source.find("if (msg.type === 'public_ip_result')", start)]

        assert "setHtmlIfChanged('add-preflight-checks', renderAddPreflightChecks())" in block
        assert "setHtmlIfChanged('add-status-details', renderAddStatusDetails(store.addForm))" in block
        assert "refreshLocalInteractiveState()" in block

    def test_dirty_optional_fields_survive_live_config_refresh(self) -> None:
        """Cleared optional fields must stay dirty while remote metadata is stale."""
        bootstrap = """
        const store = {
          view: 'vps-setup',
          hostname: 'manibot70',
          vps: {},
          detail: {
            kind: 'vps',
            hostname: 'manibot70',
            config: {
              bucket: 'pbguimani:',
              install_dir: '/home/mani/software',
              swap: '4G'
            },
            log_preview: {},
            branches: {}
          }
        };
        function refreshLocalInteractiveState() {}
        function renderSidebarActions() {}
        function renderStatusFlags() { return ''; }
        function setHtmlIfChanged() {}
        """
        assertions = """
        var ui = ensureVpsUi('manibot70', store.detail);
        assert.equal(canSaveForm(ui.form, ui.savedForm), false);

        setVpsField('manibot70', 'bucket', '');
        assert.equal(ui.form.bucket, '');
        assert.equal(ui.dirtyFields.bucket, true);
        assert.equal(canSaveForm(ui.form, ui.savedForm), true);

        store.detail = Object.assign({}, store.detail, {
          config: {
            bucket: 'pbguimani:',
            install_dir: '/opt/pbgui',
            swap: '8G'
          }
        });
        ensureVpsUi('manibot70', store.detail);

        assert.equal(ui.form.bucket, '');
        assert.equal(ui.savedForm.bucket, 'pbguimani:');
        assert.equal(ui.form.install_dir, '/opt/pbgui');
        assert.equal(ui.form.swap, '8G');
        assert.equal(canSaveForm(ui.form, ui.savedForm), true);
        """
        _run_node_assertions(
            ["defaultVpsUi", "markVpsFieldDirtyState", "syncCurrentVpsFormFromDom", "ensureVpsUi", "setVpsField", "canSaveForm"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_dom_setup_field_survives_focus_triggered_refresh(self) -> None:
        """A focus-triggered refresh must not restore a cleared bucket from stale detail."""
        bootstrap = """
        var domFields = [];
        const store = {
          view: 'vps-setup',
          hostname: 'manibot70',
          vps: {},
          detail: {
            kind: 'vps',
            hostname: 'manibot70',
            config: {
              bucket: 'pbguimani:',
              install_dir: '/home/mani/software',
              swap: '4G'
            },
            log_preview: {},
            branches: {}
          }
        };
        global.document = {
          querySelectorAll: function(selector) {
            if (selector === '[data-vps-field]') return domFields;
            return [];
          }
        };
        function formField(name, value, host) {
          return {
            type: 'text',
            value: value,
            checked: false,
            getAttribute: function(attr) {
              if (attr === 'data-vps-field') return name;
              if (attr === 'data-vps-host') return host;
              return '';
            }
          };
        }
        """
        assertions = """
        var ui = ensureVpsUi('manibot70', store.detail);
        assert.equal(ui.form.bucket, 'pbguimani:');
        assert.equal(canSaveForm(ui.form, ui.savedForm), false);

        domFields = [
          formField('bucket', '', 'manibot70')
        ];
        store.detail = Object.assign({}, store.detail, {
          config: {
            bucket: 'pbguimani:',
            install_dir: '/home/mani/software',
            swap: '4G'
          }
        });
        ensureVpsUi('manibot70', store.detail);

        assert.equal(ui.form.bucket, '');
        assert.equal(ui.dirtyFields.bucket, true);
        assert.equal(ui.savedForm.bucket, 'pbguimani:');
        assert.equal(canSaveForm(ui.form, ui.savedForm), true);
        """
        _run_node_assertions(
            ["defaultVpsUi", "markVpsFieldDirtyState", "syncCurrentVpsFormFromDom", "ensureVpsUi", "canSaveForm"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_read_vps_settings_prompts_when_password_missing(self) -> None:
        """Read VPS settings asks for a password before sending without one."""
        bootstrap = """
        var sent = [];
        var prompts = [];
        var renders = [];
        var domUserPw = '';
        const ui = { form: { user_pw: '' }, readSettingsLoading: false };
        const store = {
          detail: { kind: 'vps', hostname: 'manibot90' },
          suppressVpsDomSyncHost: ''
        };
        global.setTimeout = function(fn, delay) {
          fn();
          return 1;
        };
        global.document = {
          querySelectorAll: function(selector) {
            if (selector !== '[data-vps-field="user_pw"]') return [];
            return [{
              get value() { return domUserPw; },
              set value(next) { domUserPw = String(next || ''); },
              getAttribute: function(attr) {
                if (attr === 'data-vps-host') return 'manibot90';
                return '';
              }
            }];
          }
        };
        function ensureVpsUi(hostname, detail) {
          if (store.suppressVpsDomSyncHost !== hostname) {
            ui.form.user_pw = domUserPw;
          }
          return ui;
        }
        function getEffectiveVpsUserPw(hostname) {
          return ensureVpsUi(hostname, store.detail).form.user_pw;
        }
        function hasSessionSecret(hostname, field) {
          return false;
        }
        function openPasswordPrompt(hostname, onConfirm, options) {
          prompts.push({ hostname: hostname, options: options });
          onConfirm('fresh-password');
        }
        function buildSecretAwareForm(hostname, form) {
          return { user_pw: form.user_pw || '' };
        }
        function renderUi(options) {
          renders.push({ loading: ui.readSettingsLoading, options: options || {} });
        }
        function send(payload) {
          sent.push(payload);
        }
        """
        assertions = """
        refreshVpsSettings('manibot90');

        assert.equal(prompts.length, 1);
        assert.equal(prompts[0].hostname, 'manibot90');
        assert.equal(prompts[0].options.confirmLabel, 'Read Settings');
        assert.equal(sent.length, 1);
        assert.equal(sent[0].cmd, 'read_vps_settings');
        assert.equal(sent[0].hostname, 'manibot90');
        assert.equal(sent[0].form.user_pw, 'fresh-password');
        assert.equal(domUserPw, 'fresh-password');
        assert.equal(ui.readSettingsLoading, true);
        assert.equal(renders.length, 1);
        assert.equal(renders[0].loading, true);
        """
        _run_node_assertions(
            ["setPromptedVpsUserPassword", "refreshVpsSettings"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_save_vps_config_prompts_when_firewall_password_missing(self) -> None:
        """Saving firewall changes asks for a password before sending."""
        bootstrap = """
        var sent = [];
        var prompts = [];
        var passwordAvailable = false;
        const ui = {
          form: {
            user_pw: '',
            firewall: true,
            firewall_ssh_port: '22',
            firewall_ssh_ips: '198.51.100.1'
          },
          savedForm: {
            user_pw: '',
            firewall: false,
            firewall_ssh_port: 22,
            firewall_ssh_ips: ''
          }
        };
        const store = {
          detail: { kind: 'vps', hostname: 'manibot90' }
        };
        function ensureVpsUi(hostname, detail) {
          if (passwordAvailable) ui.form.user_pw = 'fresh-password';
          return ui;
        }
        function getEffectiveVpsUserPw(hostname) {
          return passwordAvailable ? 'fresh-password' : '';
        }
        function hasSessionSecret(hostname, field) {
          return false;
        }
        function openPasswordPrompt(hostname, onConfirm, options) {
          prompts.push({ hostname: hostname, options: options });
          passwordAvailable = true;
          onConfirm('fresh-password');
        }
        function buildSecretAwareForm(hostname, form) {
          return Object.assign({}, form);
        }
        function send(payload) {
          sent.push(payload);
        }
        """
        assertions = """
        saveVpsConfig('manibot90');

        assert.equal(prompts.length, 1);
        assert.equal(prompts[0].hostname, 'manibot90');
        assert.equal(prompts[0].options.confirmLabel, 'Apply Changes');
        assert.equal(sent.length, 1);
        assert.equal(sent[0].cmd, 'save_vps_config');
        assert.equal(sent[0].hostname, 'manibot90');
        assert.equal(sent[0].form.user_pw, 'fresh-password');
        assert.equal(sent[0].form.firewall, true);
        assert.equal(sent[0].form.firewall_ssh_ips, '198.51.100.1');
        """
        _run_node_assertions(
            ["vpsFirewallSettingsChanged", "saveVpsConfig"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_save_vps_config_prompts_when_swap_password_missing(self) -> None:
        """Saving swap changes asks for a password before sending."""
        bootstrap = """
        var sent = [];
        var prompts = [];
        var passwordAvailable = false;
        const ui = {
          form: { user_pw: '', swap: '4G', firewall: false, firewall_ssh_port: 22, firewall_ssh_ips: '' },
          savedForm: { user_pw: '', swap: '2G', firewall: false, firewall_ssh_port: 22, firewall_ssh_ips: '' }
        };
        const store = { detail: { kind: 'vps', hostname: 'manibot90' } };
        function ensureVpsUi(hostname, detail) {
          if (passwordAvailable) ui.form.user_pw = 'fresh-password';
          return ui;
        }
        function getEffectiveVpsUserPw(hostname) {
          return passwordAvailable ? 'fresh-password' : '';
        }
        function hasSessionSecret(hostname, field) { return false; }
        function openPasswordPrompt(hostname, onConfirm, options) {
          prompts.push({ hostname: hostname, options: options });
          passwordAvailable = true;
          onConfirm('fresh-password');
        }
        function buildSecretAwareForm(hostname, form) { return Object.assign({}, form); }
        function send(payload) { sent.push(payload); }
        """
        assertions = """
        saveVpsConfig('manibot90');

        assert.equal(prompts.length, 1);
        assert.equal(prompts[0].options.confirmLabel, 'Apply Changes');
        assert.equal(sent.length, 1);
        assert.equal(sent[0].cmd, 'save_vps_config');
        assert.equal(sent[0].form.user_pw, 'fresh-password');
        assert.equal(sent[0].form.swap, '4G');
        """
        _run_node_assertions(
            ["vpsFirewallSettingsChanged", "vpsSwapSettingsChanged", "saveVpsConfig"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_save_existing_vps_import_prompts_for_local_sudo_when_hosts_missing(self) -> None:
        """Saving an import asks for local sudo before adding /etc/hosts."""
        bootstrap = """
        var prompts = [];
        var posts = [];
        var rendered = 0;
        var closed = 0;
        var toasts = [];
        const store = {
          master: { sudoPw: '' },
          importExistingVps: {
            loading: false,
            saving: false,
            form: {
              hostname: 'manibot90',
              ip: '23.94.74.212',
              user: 'mani',
              user_pw: 'fresh-password',
              install_dir: '/home/mani/software'
            },
            probe: {
              can_save: true,
              local_hosts_update_required: true
            }
          }
        };
        global.setTimeout = function(fn, delay) {
          fn();
          return 1;
        };
        function defaultExistingVpsImportState() { return { form: {}, probe: null }; }
        function openMasterPasswordPrompt(onConfirm, commandText) {
          prompts.push(commandText);
          store.master.sudoPw = 'local-sudo-password';
          onConfirm();
        }
        function renderExistingVpsImportModal() { rendered += 1; }
        function vpsManagerPost(path, payload) {
          posts.push({ path: path, payload: payload });
          return {
            then: function(onResolve) {
              onResolve({ hostname: 'manibot90', message: 'Imported VPS saved.' });
              return { catch: function() {} };
            }
          };
        }
        function closeAlertModal() { closed += 1; }
        function toast(message, kind) { toasts.push({ message: message, kind: kind }); }
        function sendContext() {}
        function scheduleVpsDetailFetch(hostname) {}
        function renderUi(options) {}
        function send(payload) {}
        """
        assertions = """
        saveExistingVpsImport();

        assert.deepEqual(prompts, ['Save Imported VPS']);
        assert.equal(posts.length, 1);
        assert.equal(posts[0].path, '/import/save');
        assert.equal(posts[0].payload.hostname, 'manibot90');
        assert.equal(posts[0].payload.ip, '23.94.74.212');
        assert.equal(posts[0].payload.local_sudo_pw, 'local-sudo-password');
        assert.equal(rendered, 1);
        assert.equal(closed, 1);
        assert.equal(toasts[0].message, 'Imported VPS saved.');
        """
        _run_node_assertions(
            ["existingVpsImportPayload", "saveExistingVpsImport"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_read_vps_settings_progress_updates_steps(self) -> None:
        """Read VPS settings progress events update and render ordered steps."""
        bootstrap = """
        var renders = [];
        const store = {
          view: 'vps-setup',
          hostname: 'manibot90',
          vps: {},
          detail: { kind: 'vps', hostname: 'manibot90' }
        };
        function syncCurrentVpsFormFromDom(hostname, ui) { return false; }
        function isVpsContextView() { return true; }
        function renderUi(options) { renders.push(options || {}); }
        function esc(value) { return String(value || ''); }
        function escAttr(value) { return String(value || ''); }
        """
        assertions = """
        updateReadSettingsProgress('manibot90', { step: 'ssh', label: 'Connecting to VPS', status: 'running' });
        updateReadSettingsProgress('manibot90', { step: 'firewall', label: 'Reading UFW firewall settings', status: 'running' });
        updateReadSettingsProgress('manibot90', { step: 'done', label: 'VPS settings refreshed', status: 'done' });

        const ui = store.vps.manibot90;
        assert.equal(ui.readSettingsLoading, false);
        assert.equal(ui.readSettingsProgress.current, 'done');
        assert.equal(ui.readSettingsProgress.steps.length, 3);
        assert.equal(ui.readSettingsProgress.steps[0].status, 'done');
        assert.equal(ui.readSettingsProgress.steps[1].status, 'done');
        assert.equal(ui.readSettingsProgress.steps[2].status, 'done');
        const html = renderReadSettingsProgress(ui);
        assert.match(html, /Read VPS settings completed/);
        assert.match(html, /read-settings-progress/);
        assert.match(html, /deploy-progress-bar/);
        assert.match(html, /read-settings-task/);
        assert.match(html, /100%/);
        assert.match(html, /Connecting to VPS/);
        assert.match(html, /Reading UFW firewall settings/);
        assert.match(html, /Password/);
        assert.doesNotMatch(html, /Open/);
        assert.equal(renders.length, 3);
        """
        _run_node_assertions(
            ["defaultVpsUi", "ensureVpsUi", "updateReadSettingsProgress", "renderReadSettingsProgress"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_read_vps_settings_updates_detail_before_render(self) -> None:
        """Freshly read VPS settings must survive the forced setup-view render."""
        bootstrap = """
        var renders = [];
        var toasts = [];
        var domFields = [];
        const store = {
          view: 'vps-setup',
          hostname: 'manibot90',
          suppressVpsDomSyncHost: '',
          vps: {
            manibot90: {
              initialized: true,
              readSettingsLoading: true,
              form: {
                user_pw: 'session-password',
                firewall: false,
                firewall_ssh_port: 22,
                firewall_ssh_ips: ''
              },
              savedForm: {
                user_pw: 'session-password',
                firewall: false,
                firewall_ssh_port: 22,
                firewall_ssh_ips: ''
              },
              dirtyFields: {}
            }
          },
          detail: {
            kind: 'vps',
            hostname: 'manibot90',
            config: {
              firewall: false,
              firewall_ssh_port: 22,
              firewall_ssh_ips: ''
            }
          }
        };
        global.document = {
          querySelectorAll: function(selector) {
            if (selector === '[data-vps-field]') return domFields;
            return [];
          }
        };
        function formField(name, value, host, type, checked) {
          return {
            type: type || 'text',
            value: value,
            checked: !!checked,
            getAttribute: function(attr) {
              if (attr === 'data-vps-field') return name;
              if (attr === 'data-vps-host') return host;
              return '';
            }
          };
        }
        function isVpsContextView() { return true; }
        function renderUi(options) {
          ensureVpsUi(store.hostname, store.detail);
          renders.push({
            options: options || {},
            firewall: store.vps.manibot90.form.firewall,
            firewall_ssh_port: store.vps.manibot90.form.firewall_ssh_port,
            firewall_ssh_ips: store.vps.manibot90.form.firewall_ssh_ips
          });
        }
        function toast(message, kind) { toasts.push({ message: message, kind: kind }); }
        """
        assertions = """
        domFields = [
          formField('firewall', '', 'manibot90', 'checkbox', false),
          formField('firewall_ssh_port', '22', 'manibot90'),
          formField('firewall_ssh_ips', '', 'manibot90')
        ];
        handleResult({
          cmd: 'read_vps_settings',
          data: {
            firewall: true,
            firewall_ssh_port: 22,
            firewall_ssh_ips: '198.51.100.1, 203.0.113.7'
          }
        });

        assert.equal(store.detail.config.firewall, true);
        assert.equal(store.detail.config.firewall_ssh_port, 22);
        assert.equal(store.detail.config.firewall_ssh_ips, '198.51.100.1, 203.0.113.7');
        assert.equal(store.vps.manibot90.form.firewall, true);
        assert.equal(store.vps.manibot90.form.firewall_ssh_ips, '198.51.100.1, 203.0.113.7');
        assert.equal(store.vps.manibot90.form.user_pw, 'session-password');
        assert.equal(store.vps.manibot90.readSettingsLoading, false);
        assert.equal(store.vps.manibot90.savedForm.firewall_ssh_ips, '198.51.100.1, 203.0.113.7');
        assert.deepEqual(store.vps.manibot90.dirtyFields, {});
        assert.equal(renders.length, 1);
        assert.equal(renders[renders.length - 1].firewall_ssh_ips, '198.51.100.1, 203.0.113.7');
        assert.equal(store.suppressVpsDomSyncHost, '');
        assert.equal(toasts[0].message, 'VPS settings refreshed.');
        """
        _run_node_assertions(
            ["defaultVpsUi", "markVpsFieldDirtyState", "syncCurrentVpsFormFromDom", "ensureVpsUi", "handleResult"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_active_vps_setup_field_holds_live_view_render(self) -> None:
        """Focused setup inputs should not be replaced by automatic live renders."""
        bootstrap = """
        const store = { view: 'vps-setup', hostname: 'manibot90' };
        function activeField(name, host) {
          return {
            getAttribute: function(attr) {
              if (attr === 'data-vps-field') return name;
              if (attr === 'data-vps-host') return host;
              return '';
            }
          };
        }
        global.document = { activeElement: null };
        """
        assertions = """
        document.activeElement = activeField('firewall_ssh_ips', 'manibot90');
        assert.equal(isActiveVpsSetupFormField(), true);

        document.activeElement = activeField('firewall_ssh_ips', 'manibot91');
        assert.equal(isActiveVpsSetupFormField(), false);

        store.view = 'vps';
        document.activeElement = activeField('firewall_ssh_ips', 'manibot90');
        assert.equal(isActiveVpsSetupFormField(), false);
        """
        _run_node_assertions(
            ["isActiveVpsSetupFormField"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_sidebar_host_switch_keeps_change_vps_view(self) -> None:
        """Switching hosts from Change VPS stays in the Change VPS subview."""
        bootstrap = """
        var selected = [];
        const store = { view: 'vps-setup', hostname: 'manibot90' };
        function selectView(view, hostname) {
          selected.push({ view: view, hostname: hostname });
          store.view = view;
          store.hostname = hostname;
        }
        function getOverviewRows() {
          return [{ hostname: 'manibot90', pb8_installed: true }, { hostname: 'manibot91', pb8_installed: true }];
        }
        """
        assertions = """
        selectSidebarVpsHost('manibot91');

        assert.deepEqual(selected, [{ view: 'vps-setup', hostname: 'manibot91' }]);
        assert.equal(store.view, 'vps-setup');
        assert.equal(store.hostname, 'manibot91');

        store.view = 'vps';
        selectSidebarVpsHost('manibot90');
        assert.equal(selected[1].view, 'vps');
        """
        _run_node_assertions(
            ["selectSidebarVpsHost"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_master_resource_meters_use_master_history_host(self) -> None:
        """Master memory/disk/swap history links use the master hostname, not store.hostname."""
        bootstrap = """
        const store = { view: 'master', hostname: '' };
        function esc(value) { return String(value == null ? '' : value); }
        function escAttr(value) { return String(value == null ? '' : value).replace(/&/g, '&amp;').replace(/'/g, '&#39;').replace(/"/g, '&quot;'); }
        function resourceTone() { return 'ok'; }
        function formatLatency() { return '1s'; }
        function formatCpuTelemetry() {
          return {
            title: 'CPU Utilisation:',
            liveTone: 'ok',
            livePct: 1,
            liveValueText: '1.0%',
            avg60Confirmed: true,
            avg60Tone: 'ok',
            avg60Pct: 1,
            avg60ValueText: '1.0%'
          };
        }
        function renderServiceRows() { return ''; }
        function renderRunningPb7Fallback() { return ''; }
        """
        assertions = """
        const metric = { free_mb: 1, used_mb: 2, total_mb: 3, usage_pct: 4, usage_60s_peak: 5, usage_60s_window: 60 };
        const html = renderMonitorPanel(
          { server: { mem: metric, disk: metric, swap: metric, cpu: 1, cpu_60s: 1, cpu_60s_window: 60 }, v7: [], v7_running: [] },
          true,
          { summary_row: { hostname: 'magicnucpro', name: 'magicnucpro (local)' } }
        );

        assert.equal((html.match(/data-history-host='magicnucpro'/g) || []).length, 4);
        assert.equal(html.includes("data-history-host=''"), false);
        assert.equal(html.includes("data-history-metric='memory'"), true);
        assert.equal(html.includes("data-history-metric='disk'"), true);
        assert.equal(html.includes("data-history-metric='swap'"), true);
        """
        _run_node_assertions(
            ["renderResourceMeter", "renderMonitorPanel"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_systemd_migration_button_shows_running_state(self) -> None:
        """The sidebar distinguishes a running migration from a stale needed preview."""
        bootstrap = """
        """
        assertions = """
        const model = getVpsSystemdMigrationButtonModel(
          'manibot92',
          { status: { systemd_migration: { state: 'running', migration_complete: false, migration_needed: true } } },
          {}
        );

        assert.equal(model.className, 'sb-btn warning');
        assert.equal(model.text, 'Systemd migration running');
        """
        _run_node_assertions(
            ["getVpsSystemdMigrationButtonModel"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_systemd_migration_button_unknown_state_is_neutral(self) -> None:
        """Unknown migration state should be a neutral preview action, not a warning."""
        bootstrap = """
        """
        assertions = """
        const model = getVpsSystemdMigrationButtonModel('manibot91', { status: {} }, {});

        assert.equal(model.className, 'sb-btn');
        assert.equal(model.text, 'Preview systemd migration');
        """
        _run_node_assertions(
            ["getVpsSystemdMigrationButtonModel"],
            bootstrap=bootstrap,
            assertions=assertions,
        )
