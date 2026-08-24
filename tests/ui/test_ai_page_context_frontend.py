"""Static contracts for explicitly registered, non-sensitive AI page context."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
FRONTEND = ROOT / "frontend"
NAV = (FRONTEND / "pbgui_nav.js").read_text(encoding="utf-8")


def test_shared_context_boundary_projects_only_the_backend_schema() -> None:
    """The navigation collector should sanitize every page-provided context value."""
    assert "aiContextEntity" in NAV
    assert "aiContextFocusedField" in NAV
    assert "aiContextSensitiveName" in NAV
    assert "active.type === 'password'" in NAV
    for sensitive in ("password", "secret", "token", "api[_ -]?key", "credential", "session", "cookie", "log", "ssh"):
        assert sensitive in NAV
    assert "PBGUI_AI_PAGE_CONTEXT" in NAV
    assert "focusedField" in NAV
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
    assert "state.panel === 'results' || state.panel === 'paretos' ? [] : names" in adapter
    assert "state.selectedParetos" in adapter
    assert "8 - entities.length" in adapter
    assert "kind: 'pareto_candidate'" in adapter
