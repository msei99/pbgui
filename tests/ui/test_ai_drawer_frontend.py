"""Static security and lifecycle contracts for the global AI drawer."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
NAV = (ROOT / "frontend" / "pbgui_nav.js").read_text(encoding="utf-8")
DRAWER = (ROOT / "frontend" / "js" / "ai_drawer.js").read_text(encoding="utf-8")
CSS = (ROOT / "frontend" / "css" / "ai_drawer.css").read_text(encoding="utf-8")


def test_nav_lazy_loads_the_versioned_global_ai_drawer() -> None:
    """Every authenticated top-level page should receive one isolated drawer loader."""
    assert 'id="pbgui-ai-btn"' in NAV
    assert "/app/js/ai_drawer.js?v=33" in NAV
    assert "/app/css/ai_drawer.css?v=12" in NAV
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
    rewind = DRAWER.split("async function rewindMessage", 1)[1].split("async function", 1)[0]
    assert rewind.index("state.selectionDirty = true") < rewind.index("await loadConversation(state.current)")
    assert "buildProposalDiff" in DRAWER
    assert "Reasoning summary" in DRAWER
    assert "renderReasoningSummary" in DRAWER
    assert "renderActivityHistory" in DRAWER
    assert "Thinking" not in DRAWER
    assert "pai-change-card" in DRAWER
    assert "changeKind" in DRAWER
    assert "startContextWatch" in DRAWER
    assert "setInterval(refreshLiveContext, 500)" in DRAWER
    assert "collectDisplayContext" in DRAWER
    assert "context.evidence" in DRAWER
    assert "lineCount" in DRAWER
    assert "collectContext({ include_controls: false })" in DRAWER
    assert "var context = collectDisplayContext();" in DRAWER
    assert "checked ? collectContext() : null" in DRAWER
    assert "prepareNewSelection" not in DRAWER
    assert "state.selectionDirty = true; loadModels();" in DRAWER
    assert "state.selectionDirty = true; rebuildEfforts();" in DRAWER
    assert "if (!state.selectionDirty)" in DRAWER
    assert "await rebuildProviders(conversation.provider, conversation.model)" in DRAWER
    assert "Loading models..." in DRAWER
    assert "Models unavailable" in DRAWER
    assert "state.modelsLoading" in DRAWER
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
    reconcile = DRAWER.split("async function reconcileProposals", 1)[1].split("function proposalActionLabel", 1)[0]
    assert "if (state.busy) { renderProposals([]); return; }" in reconcile
    assert "conversationId !== state.current || state.busy" in reconcile
    assert "Review changes" in DRAWER
    assert "Review proposed changes" in DRAWER
    assert "window.PBGuiConfirm" in DRAWER
    assert "Proposal integrity is verified before execution." in DRAWER
    assert "pbgui:ai-action-completed" in DRAWER
    assert "pbgui:ai-ui-action" in DRAWER
    assert "/ui-actions/" in DRAWER
    assert "Promise.resolve(action.browser_completion).then" in DRAWER
    assert "if (action.browser_error) setStatus" in DRAWER
    assert "pendingPageAction" in DRAWER
    assert "conversation.busy || pendingPageAction" in DRAWER
    assert "tryLocalCommand" in DRAWER
    assert "'/local-action'" in DRAWER
    assert "PBGui completed the action, but could not record it" in DRAWER
    assert "state.resizing" in DRAWER
    assert "pai-resize-shield" in DRAWER
    assert "window.addEventListener('blur', finish)" in DRAWER
    assert "if (!state.resizing) applyWidth(preferences.drawer_width)" in DRAWER
    assert "drawer_open: drawerOpen == null ? state.open : !!drawerOpen" in DRAWER
    assert "saveDrawerPreferences(true)" in DRAWER
    assert "saveDrawerPreferences(false)" in DRAWER
    assert "preferences.drawer_open === true" in NAV
    assert "credentials: 'same-origin'" in NAV
    assert "chat.quick_replies" in DRAWER
    assert "renderQuickReplies" in DRAWER
    assert "proposalReviewText" in DRAWER
    assert "python_analysis" in DRAWER
    assert "proposal.input_data" not in DRAWER
    assert "preview.input_data" in DRAWER
    assert "preview.input_resource" in DRAWER
    assert "appendAnalysisResult" in DRAWER
    assert "Start PB8 optimizer queue jobs" in DRAWER
    assert "exact reviewed PB8 optimizer queue jobs immediately" in DRAWER
    resolve = DRAWER.split("async function resolveProposal", 1)[1].split("async function newConversation", 1)[0]
    assert resolve.index("card.hidden = true") < resolve.index("await api('/proposals/")
    assert "Applying approved action..." in resolve
    assert "card.hidden = false" in resolve
    assert "result.continuation" in DRAWER
    assert "await loadConversation(conversationId)" in DRAWER
    assert "the AI continuation could not start" in DRAWER


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
    assert "width:16px" in CSS
    assert "touch-action:none" in CSS
    assert ".pai-proposal" in CSS
