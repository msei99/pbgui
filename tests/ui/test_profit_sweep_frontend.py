"""Offline source contracts for the Profit Sweep frontend."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
PAGE = ROOT / "frontend" / "profit_sweep.html"


def _source() -> str:
    """Return the standalone Profit Sweep page source."""

    return PAGE.read_text(encoding="utf-8")


def test_profit_sweep_shell_navigation_and_local_assets() -> None:
    """Use the PBGui standalone shell, expected navigation identity, and local assets."""

    source = _source()

    assert '<nav id="topnav"></nav>' in source
    assert "current: 'system_profit_sweep'" in source
    assert 'window.API_BASE = "%%API_BASE%%"' in source
    assert 'window.PBGUI_VERSION = "%%VERSION%%"' in source
    assert 'window.PBGUI_SERIAL = "%%SERIAL%%"' in source
    assert "/app/pbgui_nav.js?v=%%NAV_HASH%%" in source
    assert "/app/js/pbgui_dialogs.js" in source
    assert "document.getElementById('pbgui-notify-btn')" in source
    assert "sharedLogButton.click()" in source
    assert "new LogViewerPanel({" not in source
    assert "https://" not in source
    assert "http://" not in source


def test_profit_sweep_uses_cookie_only_api_contract() -> None:
    """Call Dry and Live endpoints with same-origin cookies and encoded identifiers."""

    source = _source()

    assert "credentials: 'same-origin'" in source
    for forbidden in (
        "%%TOKEN%%",
        "window.TOKEN",
        "Authorization",
        "Bearer",
        "localStorage",
        "sessionStorage",
        "document.cookie",
    ):
        assert forbidden not in source

    for endpoint in (
        "requestJson('/schema')",
        "requestJson('/users'",
        "'/policies/' + encodeURIComponent(userName)",
        "'/journal/' + encodeURIComponent(userName) + '?limit=100'",
        "requestJson('/health')",
        "'/evaluate/' + encodeURIComponent(userName)",
        "'/baseline/' + encodeURIComponent(userName)",
        "'/intents/' + encodeURIComponent(userName)",
        "'/test-transfers/' + encodeURIComponent(userName)",
        "'/test-transfer/' + encodeURIComponent(userName)",
        "'/test-transfer/' + encodeURIComponent(userName) + '/' + encodeURIComponent(String(prepared.operation.operation_id || '')) + '/signature'",
        "'/live/' + encodeURIComponent(userName)",
        "'/reconcile/' + encodeURIComponent(userName) + '/' + encodeURIComponent(operationId)",
    ):
        assert endpoint in source
    assert "method: 'PUT'" in source
    assert "method: 'DELETE'" in source
    assert "method: 'POST'" in source
    assert "JSON.stringify({ policy: policy })" in source
    assert "var policy = collectPolicy();" in source
    assert "expected_policy_fingerprint: state.record.policy_fingerprint" in source


def test_profit_sweep_stale_account_requests_are_bounded() -> None:
    """Abort old account work and gate every account response by monotonic generation."""

    source = _source()

    assert "accountGeneration: 0" in source
    assert "state.accountGeneration += 1" in source
    assert "state.accountController.abort()" in source
    assert "state.accountController = new AbortController()" in source
    assert "isCurrentAccount(userName, generation)" in source
    assert "signal: state.accountController" in source
    assert "usersGeneration: 0" in source
    assert "state.usersGeneration += 1" in source
    assert "state.usersController.abort()" in source


def test_profit_sweep_test_transfer_roundtrip_uses_shared_safety_dialogs() -> None:
    """Expose supported standard and Vault roundtrips with explicit real-funds consent."""

    source = _source()

    assert 'id="test-transfer" hidden>Test transfer</button>' in source
    assert "[hidden] { display: none !important; }" in source
    assert "var supported = Boolean(user && capability.supported === true && !permissionBlocked)" in source
    assert "Real-funds Vault test (default 5 USDC)" in source
    assert "'Leader Main Perps'" in source
    assert "Leader Main (Unified)" in source
    assert "Transfer back requires at least 5 USDC for the Vault deposit" in source
    assert "window.PBGuiDialogs.prompt({" in source
    assert "defaultValue: user.is_vault ? '5' : '1'" in source
    assert "Test transfer amount must be a positive decimal number." in source
    assert "smaller withdrawals are allowed" in source
    assert "operation.transfer_back_reason" in source
    assert "eip6963:announceProvider" in source
    assert "eip6963:requestProvider" in source
    assert "window.okxwallet" in source
    assert "hasEip6963Providers: false" in source
    assert "item.family === family" in source
    assert "if (state.hasEip6963Providers) return" in source
    assert "Array.isArray(ethereum.providers)" in source
    assert "window.PBGuiDialogs.choose" in source
    assert "walletProvider.request" in source
    assert "eth_requestAccounts" in source
    assert "eth_signTypedData_v4" in source
    assert "JSON.stringify({ signature: signature })" in source
    assert "selected !== expected" in source
    assert "window.PBGuiDialogs.confirm({" in source
    assert "This action moves real funds." in source
    assert "body: JSON.stringify({ amount: amount, asset: asset, operation_id: operationId })" in source
    assert "window.crypto.randomUUID()" in source
    assert "pendingTestOperationId: ''" in source
    assert "operation.asset || 'settlement asset'" in source
    assert "endpoints.source + ' to ' + endpoints.destination" in source
    assert "endpoints.destination + ' back to ' + endpoints.source" in source


def test_profit_sweep_test_operations_are_generation_safe_and_confirmed_only() -> None:
    """Refresh durable operation status without exposing retry or return actions for ambiguity."""

    source = _source()

    assert "testOperations: []" in source
    assert "testActionPending: false" in source
    assert "loadTestTransfersForAccount(userName, generation, signal)" in source
    assert "requestJson('/test-transfers/' + encodeURIComponent(userName), { signal: signal })" in source
    assert "loadTestTransfersForAccount(userName, generation, state.accountController" in source
    assert "operation.status === 'confirmed' && operation.can_transfer_back === true" in source
    assert "operation === latestForward && operation.status === 'confirmed' && operation.can_transfer_back === true" in source
    assert "'Transfer back'" in source
    assert "encodeURIComponent(String(operation.operation_id || '')) + '/back'" in source
    assert "'Retry'" not in source


def test_profit_sweep_renders_untrusted_values_safely() -> None:
    """Build account, status, preview, and journal values with textContent DOM nodes."""

    source = _source()

    assert "document.createElement('button')" in source
    assert "document.createElement('td')" in source
    assert ".textContent =" in source
    assert "replaceChildren()" in source
    assert ".innerHTML" not in source
    assert "onclick=" not in source
    assert "window.alert" not in source
    assert "window.confirm" not in source
    assert "PBGuiDialogs.confirm" in source


def test_profit_sweep_classifies_basic_advanced_and_hidden_policy_fields() -> None:
    """Keep the default UI compact while preserving advanced and future settings."""

    source = _source()
    fields = (
        "operating_mode asset reference_capital baseline_mode trigger_percent sweep_percent minimum_transfer_amount "
        "simulation_minimum_transfer_amount live_minimum_transfer_amount "
        "safety_reserve_mode safety_reserve_amount safety_reserve_percent daily_transfer_limit_enabled "
        "daily_transfer_limit single_transfer_limit_enabled single_transfer_limit trigger_mode "
        "periodic_interval settlement_debounce quiet_period stabilization_interval "
        "successful_transfer_cooldown vault_transfer_cooldown schedule_jitter_percent "
        "maximum_history_age maximum_preflight_age live_activation_baseline_mode "
        "first_live_catchup_limit_enabled first_live_catchup_limit vault_withdraw_mode "
        "vault_destination vault_minimum_transfer_amount retained_leader_equity share_safety_buffer "
        "vault_safety_reserve_mode vault_safety_reserve_amount vault_safety_reserve_percent "
        "vault_conditional_cost_policy main_destination_activity_policy"
    ).split()

    for field in fields:
        assert f"'{field}'" in source
    assert "Object.keys(defaults).forEach" in source
    assert "policy[fieldName] = input.checked" in source
    assert "policy[fieldName] = integer" in source
    assert "policy[fieldName] = decimal" in source
    assert "byId(group + '-fields')" in source
    assert "var HIDDEN_POLICY_FIELDS = new Set([" in source
    assert "var ADVANCED_POLICY_FIELDS = new Set([" in source
    assert 'id="policy-advanced-fields"' in source
    assert 'id="schedule-advanced-fields"' in source
    assert 'id="vault-advanced-fields"' in source
    assert "policy.simulation_minimum_transfer_amount = policy.minimum_transfer_amount" in source
    assert "label: 'Minimum transfer amount'" in source
    assert "Legacy minimum transfer" not in source


def test_profit_sweep_uses_standard_dotted_field_tooltips() -> None:
    """Every generated field label uses the established data-tip hover pattern."""

    source = _source()

    assert "[data-tip] { cursor: help" in source
    assert 'id="data-tip-tooltip"' in source
    assert "label.dataset.tip = meta.help" in source
    assert "event.target.closest('[data-tip]')" in source
    assert "tip.textContent = tooltipText" in source
    assert "tip.style.display = 'block'" in source
    assert "tip.style.display = 'none'" in source
    assert "className = 'field-help'" not in source


def test_profit_sweep_live_actions_and_dry_labels_are_explicit() -> None:
    """Expose Live controls while keeping Dry outcomes unambiguous."""

    source = _source()

    for label in (
        "Keep trading capital",
        "Save changes",
        "Enable Dry",
        "Disable",
        "Evaluate now",
        "Reset baseline",
        "Refresh journal",
        "Refresh intents",
        "Enable Live",
        "Reconcile",
        "Logs",
        "Overview",
        "Policy",
        "Schedule",
        "Exchange / Vault",
        "Journal",
    ):
        assert label in source
    assert "byId('policy-trigger_percent').value = '0'" in source
    assert "byId('policy-sweep_percent').value = '100'" in source
    assert 'id="enable-live">Enable Live</button>' in source
    assert "Live Ready" not in source
    assert "phase-strip" not in source
    assert "Policy generation" not in source
    assert "'Scheduler'" not in source
    assert "'Persistence'" not in source
    assert "state.schema.live_available !== true" in source
    assert "user.is_vault" in source
    assert source.count("WOULD TRANSFER") >= 4
    assert "not a submitted or confirmed transfer" in source
    assert "server rechecks credentials, account mode, history, and snapshot freshness" in source
    assert 'id="panel-logs"' not in source
    assert 'id="log-drawer"' not in source


def test_profit_sweep_live_activation_uses_shared_confirmation_and_saved_settings() -> None:
    """Persist selected activation policy fields before calling the dedicated Live route."""

    source = _source()

    for field_id in (
        "policy-asset",
        "policy-live_activation_baseline_mode",
        "policy-first_live_catchup_limit_enabled",
        "policy-first_live_catchup_limit",
    ):
        assert f'id="{field_id}"' in source
    assert "async function enableLive()" in source
    assert "title: 'Enable Live Profit Sweep'" in source
    assert "var accepted = await window.PBGuiDialogs.confirm({" in source
    assert "policy.operating_mode = currentMode" in source
    assert "JSON.stringify({ policy: policy })" in source
    assert "requestJson('/live/' + encodeURIComponent(userName)" in source
    assert "state.writeCapability = result.capability || null" in source
    assert "await refreshPolicyAndIntents(userName, generation" in source
    assert "expected_generation: state.record ? state.record.generation : null" in source
    assert "title: 'Change Live Profit Sweep'" in source
    assert "confirmed_live_update: confirmedLiveUpdate" in source
    assert "Apply settings that affect future real transfers" in source
    assert "Reset Dry and Live accounting baselines" in source
    assert "all Profit Sweep accounting history" in source
    assert "var liveActive = mode === 'live' || mode === 'paused_unknown'" in source
    assert "state.record.live_state.active_baseline_mode" in source
    assert "title: 'Recalculate Live baseline'" in source
    assert "Previous Dry-period profit may become immediately due" in source
    assert "recalculate_live_baseline: recalculateLiveBaseline" in source
    assert "recover_legacy_dry_generation: recoverLegacyDryGeneration" in source
    assert 'id="apply-live-baseline" hidden>Apply baseline to active Live</button>' in source
    assert "state.baselineRecalculationRequested = true" in source
    assert "recalculateLiveBaseline = state.baselineRecalculationRequested === true" in source
    assert "policy.baseline_mode === 'from_enable'" in source
    assert "Number(entry.generation) < Number(simulation.generation)" in source
    assert "simulation.last_evaluation_at === null" in source
    assert "activeBaseline === selectedBaseline" in source
    assert "previewDecision.sweep_due !== undefined" in source
    assert "decision.state_kind === 'live' ? 'LIVE PREVIEW' : 'DRY PREVIEW'" in source


def test_profit_sweep_renders_live_capability_modes_and_intents_safely() -> None:
    """Show contextual routes, mode badges, intent states, and unknown reconciliation."""

    source = _source()

    for label in (
        "Transfer capability",
        "Transfer route",
        "Unavailable reason",
        "Live Transfer Intents",
        "prepared",
        "submitting",
        "confirmed",
        "failed",
        "unknown",
    ):
        assert label in source
    for route in (
        "perp_to_spot",
        "vault_to_main_perps",
        "unified_to_fund",
        "umfuture_to_funding",
        "usdt_futures_to_spot",
        "uta_to_spot",
    ):
        assert route in source
    assert ".mode-badge.live" in source
    assert ".mode-badge.paused" in source
    assert "mode === 'paused_unknown'" in source
    assert "state.intents.forEach(function (intent)" in source
    assert "if (intent.can_reconcile === true)" in source
    assert "reconcileIntent(String(intent.operation_id || ''))" in source
    assert "encodeURIComponent(operationId)" in source
    assert "renderExchangeContext(user)" in source
    assert "renderAccountBalances(user)" in source
    assert "transferPermissions.internal_transfer === false" in source
    assert "permissionBlocked" in source
    assert "Internal account transfer permission is unavailable." in source
    assert source.count("renderTestTransfers(user);") >= 3
    assert 'id="source-balance-value"' in source
    assert 'id="destination-balance-value"' in source
    assert 'id="transferable-balance-value"' in source
    assert 'id="vault-tvl-value"' in source
    assert 'id="vault-share-value"' in source
    assert "user.is_vault ? 'Your Vault Equity'" in source
    assert "vaultBalances.account_value" in source
    assert "vault.leader_fraction" in source
    assert "function vaultShareText(value)" in source
    assert "state.snapshot = result.snapshot || null" in source
    assert "byId('vault-panel').hidden" in source


def test_profit_sweep_has_one_listener_per_live_action_and_dirty_save() -> None:
    """Avoid duplicate submissions and show Save changes only for modified form values."""

    source = _source()

    assert source.count("byId('enable-live').addEventListener('click', enableLive)") == 1
    assert source.count("byId('refresh-intents').addEventListener('click', refreshIntents)") == 1
    assert 'id="save-policy" hidden>Save changes</button>' in source
    assert "function updatePolicyDirty()" in source
    assert "byId('save-policy').hidden = !dirty || !state.selectedUser" in source


def test_profit_sweep_sidebar_and_mobile_contracts() -> None:
    """Provide safe searchable keyboard selection and a one-column mobile layout."""

    source = _source()

    assert 'id="account-search"' in source
    assert '/app/css/sidebar.css?v=4' in source
    assert '/app/js/sidebar_resize.js?v=1' in source
    for sidebar_id in ('sidebar', 'sidebar-sticky', 'sidebar-header', 'sidebar-toolbar', 'sidebar-inner', 'sidebar-resize'):
        assert f'id="{sidebar_id}"' in source
    assert 'id="sidebar-resize"' in source
    assert "window.PBGuiSidebarResize.init({ sidebarId: 'sidebar', handleId: 'sidebar-resize', minWidth: 220, maxWidth: 520 })" in source
    assert "className = 'account-button'" in source
    assert "button.className = 'account-button' + (user.name === state.selectedUser ? ' selected' : '')" in source
    assert "border-left-color: var(--accent)" in source
    assert "white-space: normal; overflow-wrap: anywhere" in source
    assert "detailRow.className = 'account-detail-row'" in source
    assert "grid-template-columns: 265px" not in source
    assert "#account-sidebar" not in source
    assert "event.key === 'ArrowDown'" in source
    assert "event.key === 'ArrowUp'" in source
    assert "event.key === 'Home'" in source
    assert "event.key === 'End'" in source
    assert "@media (max-width: 720px)" in source
    assert "body.profit-sweep-page { overflow-y: auto; }" in source
    assert '<body class="profit-sweep-page">' in source
    assert "#status-cards, .field-grid, .overview-grid, .key-values, .preview-grid, .balance-grid, .vault-ownership-grid { grid-template-columns: 1fr; }" in source
