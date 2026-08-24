"""Static security and lifecycle contracts for the global AI drawer."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
NAV = (ROOT / "frontend" / "pbgui_nav.js").read_text(encoding="utf-8")
DRAWER = (ROOT / "frontend" / "js" / "ai_drawer.js").read_text(encoding="utf-8")
CSS = (ROOT / "frontend" / "css" / "ai_drawer.css").read_text(encoding="utf-8")


def test_nav_lazy_loads_the_versioned_global_ai_drawer() -> None:
    """Every authenticated top-level page should receive one isolated drawer loader."""
    assert 'id="pbgui-ai-btn"' in NAV
    assert "/app/js/ai_drawer.js?v=24" in NAV
    assert "/app/css/ai_drawer.css?v=11" in NAV
    assert "registerPageContext" in NAV
    assert "collectAIContext" in NAV


def test_drawer_uses_cookie_auth_persistent_history_and_detached_turns() -> None:
    """Navigation must detach polling, not cancel the server-owned turn."""
    assert "credentials: 'same-origin'" in DRAWER
    assert "api('/conversations')" in DRAWER
    assert "'/turns'" in DRAWER
    assert "models/health-refresh" in DRAWER
    assert "navigator.clipboard.writeText" in DRAWER
    assert "'/rewind'" in DRAWER
    assert "buildProposalDiff" in DRAWER
    assert "Reasoning summary" in DRAWER
    assert "renderReasoningSummary" in DRAWER
    assert "renderActivityHistory" in DRAWER
    assert "Thinking" not in DRAWER
    assert "pai-change-card" in DRAWER
    assert "changeKind" in DRAWER
    assert "startContextWatch" in DRAWER
    assert "setInterval(refreshLiveContext, 500)" in DRAWER
    assert "prepareNewSelection" not in DRAWER
    assert "model.addEventListener('change', function () { rebuildEfforts(); })" in DRAWER
    assert "model: root.querySelector('#pai-model').value" in DRAWER
    assert "effort: root.querySelector('#pai-effort').value" in DRAWER
    assert "provider: root.querySelector('#pai-provider').value" in DRAWER
    assert "api('/preferences')" in DRAWER
    assert "method: 'PUT'" in DRAWER
    assert "pagehide" in DRAWER
    pagehide = DRAWER.split("pagehide", 1)[1]
    assert "/cancel" not in pagehide
    assert "localStorage" not in DRAWER
    assert "sessionStorage" not in DRAWER
    assert "Authorization" not in DRAWER
    assert "document.cookie" not in DRAWER
    assert "retryMessages" in DRAWER
    assert "reconcileProposals" in DRAWER
    assert "Review changes" in DRAWER
    assert "Review proposed changes" in DRAWER
    assert "window.PBGuiConfirm" in DRAWER
    assert "Proposal integrity is verified before execution." in DRAWER
    assert "pbgui:ai-action-completed" in DRAWER
    assert "pbgui:ai-ui-action" in DRAWER
    assert "/ui-actions/" in DRAWER
    assert "chat.quick_replies" in DRAWER
    assert "renderQuickReplies" in DRAWER
    assert "proposalReviewText" in DRAWER
    assert "python_analysis" in DRAWER
    assert "proposal.input_data" not in DRAWER
    assert "preview.input_data" in DRAWER
    assert "preview.input_resource" in DRAWER
    assert "appendAnalysisResult" in DRAWER


def test_drawer_context_is_structured_and_never_scrapes_the_page() -> None:
    """Page context should use registrations rather than arbitrary DOM or forms."""
    assert "PBGuiAI.collectContext" in DRAWER
    assert "Include page context" in DRAWER
    assert "pai-context-chip" in DRAWER
    for forbidden in ("innerText", "outerHTML", "FormData", "location.href", "querySelectorAll('input"):
        assert forbidden not in DRAWER


def test_drawer_renders_untrusted_values_as_text_and_has_explicit_close() -> None:
    """Provider and context content must use text nodes and explicit drawer controls."""
    assert ".textContent" in DRAWER
    assert "innerHTML" not in DRAWER
    assert "Collapse AI assistant" in DRAWER
    assert "window.confirm" not in DRAWER
    assert "window.alert" not in DRAWER
    assert "https://" not in DRAWER
    assert "position:fixed" in CSS
    assert "transform:translateX(100%)" in CSS
    assert "@media(max-width:760px)" in CSS
    assert "pai-resize" in CSS
    assert "cursor:ew-resize" in CSS
    assert ".pai-proposal" in CSS
