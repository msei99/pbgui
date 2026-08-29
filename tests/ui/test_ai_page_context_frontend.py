"""Static contracts for explicitly registered, non-sensitive AI page context."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
FRONTEND = ROOT / "frontend"
NAV = (FRONTEND / "pbgui_nav.js").read_text(encoding="utf-8")


def test_shared_context_boundary_projects_only_the_backend_schema() -> None:
    """The navigation collector should sanitize every page-provided context value."""
    assert "aiContextEntity" in NAV
    assert "aiContextFocusedField" in NAV
    assert "aiContextEvidence" in NAV
    assert "redactAIEvidenceText" in NAV
    assert ".replace(/([\"'])(authorization|password|passwd|secret|token|api[_ -]?key" in NAV
    assert "aiContextSensitiveName" in NAV
    assert "active.type === 'password'" in NAV
    for sensitive in ("password", "secret", "token", "api[_ -]?key", "credential", "session", "cookie", "log", "ssh"):
        assert sensitive in NAV
    assert "PBGUI_AI_PAGE_CONTEXT" in NAV
    assert "PBGUI_AI_PAGE_ACTIONS" in NAV
    assert "registerPageAction" in NAV
    assert "request.type !== 'page.perform_action'" in NAV
    assert "context.entities.some" in NAV
    assert "continuePageAction" in NAV
    assert "FASTAPI_PAGES[String(target.page_key || '')]" in NAV
    assert "pbgui_ai_action" in NAV
    assert "collectAIControls" in NAV
    assert "previousByIdentity" not in NAV
    assert "current[0].descriptor.id = previous[0].descriptor.id" not in NAV
    assert "entry.identity !== identity" in NAV
    assert "entry = { id: 'control_' + _aiControlSequence, identity: identity }" in NAV
    assert "aiControlPageIdentity" in NAV
    assert "aiControlResourceIdentity" in NAV
    assert "aiControlStateIdentity" in NAV
    assert "pageIdentity," in NAV
    assert "options.include_controls === false ? [] : collectAIControls()" in NAV
    assert "entity_kind: 'ui_control'" in NAV
    assert "entity_kind: 'ui_control_label'" in NAV
    assert "id: 'activate'" in NAV
    assert "id: 'set_value'" in NAV
    assert "id: 'activate_by_label'" in NAV
    assert "id: 'set_value_by_label'" in NAV
    assert "resolveAIControlByName" in NAV
    assert "pages: collectAIPages()" in NAV
    assert "aiControlSensitive" in NAV
    assert "type === 'password' || type === 'file'" in NAV
    assert "element.placeholder || element.name || element.id" in NAV
    assert "element.placeholder || element.value" not in NAV
    assert "tryLocalCommand" in NAV
    assert "descriptor.context ? 0 : 1" in NAV
    assert "candidates.slice(0, 2048)" in NAV
    assert "JSON.stringify(context).length > 256 * 1024" in NAV
    assert "rect.bottom >= 0" not in NAV
    assert "descriptor.name = controlContext ? controlContext + ' :: ' + label : label" in NAV
    assert "var selector = 'body *'" in NAV
    assert "element.hasAttribute('onclick')" in NAV
    assert "typeof element.onclick === 'function'" in NAV
    assert "style.cursor === 'pointer'" in NAV
    assert "aiSelectOptionAvailable" in NAV
    assert "!option.disabled && !option.hidden" in NAV
    assert "option.value === value && aiSelectOptionAvailable(option)" in NAV
    assert "#pbgui-dialog-ovl,#pbgui-confirm-ovl,#confirm-ovl" in NAV
    assert "key === '/'" not in NAV
    assert "delete|remove|loesch" in NAV
    assert "confirm|bestaetig" in NAV
    assert "focusedField" in NAV
    assert "context.evidence" in NAV
    page_action_handler = NAV.split("window.addEventListener('pbgui:ai-ui-action'", 1)[1].split("function executeLocalPageAction", 1)[0]
    failed_handler = page_action_handler.split("} catch (error) {", 1)[1]
    assert "request.browser_error = error" in failed_handler
    assert "event.preventDefault()" not in failed_handler
    assert "querySelectorAll('input" not in NAV
    assert "FormData" not in NAV


def test_productive_pages_register_explicit_context_adapters() -> None:
    """Productive pages should expose their own state instead of relying on DOM scraping."""
    pages = (
        "welcome.html",
        "dashboard_main.html",
        "api_keys_editor.html",
        "cluster.html",
        "vps_manager.html",
        "vps_monitor.html",
        "services_monitor.html",
        "db_tools.html",
        "logging_monitor.html",
        "ai_chat.html",
        "market_data_main.html",
        "coin_data.html",
        "balance_calc.html",
        "v7_run.html",
        "v7_edit.html",
        "v7_backtest.html",
        "v7_optimize.html",
        "v7_strategy_explorer.html",
        "v7_pareto_explorer.html",
    )
    for name in pages:
        source = (FRONTEND / name).read_text(encoding="utf-8")
        assert "PBGUI_AI_PAGE_CONTEXT" in source or "registerPageContext" in source, name


def test_sensitive_pages_never_register_sensitive_values() -> None:
    """Credential and logging adapters may expose identity/section context, never values."""
    api_keys = (FRONTEND / "api_keys_editor.html").read_text(encoding="utf-8")
    logging = (FRONTEND / "logging_monitor.html").read_text(encoding="utf-8")
    api_adapter = api_keys.split("PBGUI_AI_PAGE_CONTEXT", 1)[1].split("}());", 1)[0]
    log_adapter = logging.split("registerPageContext", 1)[1].split("});", 1)[0]

    assert "editingName" in api_adapter
    assert "value" not in api_adapter
    assert "tradfiProfileId" not in api_adapter
    assert "apiKey" not in api_adapter
    assert "value" not in log_adapter
    assert "LogViewer" not in log_adapter


def test_services_context_binds_cmc_toolbar_controls_to_selected_key() -> None:
    """Changing the selected CMC key must invalidate delayed toolbar control IDs."""

    services = (FRONTEND / "services_monitor.html").read_text(encoding="utf-8")
    adapter = services.split("window.PBGuiAI.registerPageContext({", 1)[1]

    assert "_currentPanelId === 'pbcoindata' && _selectedCmcKeyId" in adapter
    assert "{ kind: 'cmc_key', name: _selectedCmcKeyId }" in adapter


def test_optimize_context_prefers_the_live_open_editor_config() -> None:
    """Opening or renaming a config after the drawer opens must update turn context."""
    source = (FRONTEND / "v7_optimize.html").read_text(encoding="utf-8")
    adapter = source.split("window.PBGUI_AI_PAGE_CONTEXT", 1)[1].split("window.PBGUI_NAV_CONFIG", 1)[0]

    assert "document.getElementById('editor-name')" in adapter
    assert "editorNameInput.value" in adapter
    assert "state.editingConfig" in adapter
    assert "kind: 'optimizer_config'" in adapter
    assert "state.selectedResultName" in adapter
    assert "selectedRun.pareto_count" in adapter
    assert "selectedRun.modified" in adapter
    assert "kind: 'optimizer_run'" in adapter
    assert "showConfigEntities" in adapter
    assert "state.panel === 'queue' && state.selectedQueue" in adapter
    assert "kind: 'optimizer_queue_item'" in adapter
    assert "name: String(queueItem.filename)" in adapter
    assert "id: 'show_log'" in adapter
    assert "openLogPanel(queueItem.filename, queueItem.name || queueItem.filename)" in adapter
    assert "item.status === 'running' || item.status === 'optimizing'" in adapter
    assert "state.selectedParetos" in adapter
    assert "8 - entities.length" in adapter
    assert "kind: 'pareto_candidate'" in adapter


def test_log_pages_register_one_generic_action_over_existing_viewers() -> None:
    """Optimize, Backtest, and bot editors should reuse their existing log functions."""
    backtest = (FRONTEND / "v7_backtest.html").read_text(encoding="utf-8")
    run_editor = (FRONTEND / "v7_edit.html").read_text(encoding="utf-8")

    assert "kind: 'backtest_queue_item'" in backtest
    assert "entity_kind: 'backtest_queue_item'" in backtest
    assert "showLog(queueItem.filename)" in backtest
    assert "kind: 'log_excerpt'" in run_editor
    assert "visibleRunLogLines(120)" in run_editor
    assert "terminal.children" in run_editor
    assert "line.classList.contains('lvp-hidden')" in run_editor
    assert "rect.bottom > viewportTop && rect.top < viewportBottom" in run_editor
    assert "_runLogViewer._lines.slice" not in run_editor
    assert "item.status === 'running' || item.status === 'backtesting'" in backtest
    assert "entity_kind: 'run_config'" in run_editor
    assert "return openLogPanel()" in run_editor
