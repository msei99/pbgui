"""Static security and behavior checks for the minimal AI chat page."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
HTML = (ROOT / "frontend" / "ai_chat.html").read_text(encoding="utf-8")


def test_ai_chat_uses_cookie_only_same_origin_requests() -> None:
    """The page must not expose or persist PBGui authentication material."""
    assert "credentials: 'same-origin'" in HTML
    assert "Authorization" not in HTML
    assert "Bearer" not in HTML
    assert "localStorage" not in HTML
    assert "sessionStorage" not in HTML
    assert "%%TOKEN%%" not in HTML


def test_ai_chat_renders_provider_content_as_text() -> None:
    """Untrusted model text and login values must not enter innerHTML."""
    assert ".textContent = text" in HTML
    assert "login-link').textContent" in HTML
    assert "login-code').textContent" in HTML
    assert "innerHTML" not in HTML
    assert "document.write" not in HTML


def test_ai_chat_clears_unsaved_secret_and_uses_only_ai_action_routes() -> None:
    """The agent UI must clear keys and execute proposals only through the AI API."""
    assert "$('go-key').value = ''" in HTML
    assert "api('/conversations'" in HTML
    assert "/cancel" in HTML
    assert "/providers/chatgpt/browser-login" in HTML
    assert "/providers/chatgpt/device-login" in HTML
    assert "training enabled" in HTML
    assert "['opencode-zen', 'OpenCode Zen']" in HTML
    assert "freeGroup.label = 'Free'" in HTML
    assert "model.free ? ' · Free'" in HTML
    assert "' · Chat only'" in HTML
    assert "cannot inspect installed Passivbot documentation" in HTML
    assert 'href="/api/ai/providers/opencode-go/subscribe"' in HTML
    assert "Get OpenCode Go" in HTML
    assert "Referral link:" not in HTML
    assert "always require your explicit approval" in HTML
    for forbidden in ("/api/optimize", "/api/backtest", "/api/v7", "/api/v8", "/api/jobs"):
        assert forbidden not in HTML


def test_ai_chat_avoids_native_confirmation_dialogs_and_external_assets() -> None:
    """The page should follow PBGui dialog and offline asset policies."""
    assert "window.confirm" not in HTML
    assert "window.alert" not in HTML
    assert 'src="https://' not in HTML
    assert 'href="https://' not in HTML


def test_ai_chat_requires_shared_dialog_approval_for_proposals() -> None:
    """Mutation proposals must use the explicit PBGui modal before approval."""
    assert "/app/js/pbgui_dialogs.js?v=8" in HTML
    assert "window.PBGuiDialogs.confirm" in HTML
    assert "/proposals/" in HTML
    assert "Review & approve" in HTML
    assert "payload_digest: proposal.payload_digest" in HTML
    assert "preview: preview" in HTML
    assert "Review exact code and input" in HTML
    assert "preview.input_data" in HTML
    assert "preview.input_resource" in HTML
    assert "analysisResultText" in HTML
    assert "Start PB8 optimizer queue jobs" in HTML
    assert "exact reviewed PB8 optimizer queue jobs immediately" in HTML


def test_ai_chat_uses_persistent_history_and_detached_turn_polling() -> None:
    """The full page should share persistent detached-turn behavior with the drawer."""
    assert 'id="stop"' in HTML
    assert 'id="conversation-list"' in HTML
    assert 'id="delete-chat"' in HTML
    assert 'id="retry-turn"' in HTML
    assert "retryMessages" in HTML
    assert "loadConversations" in HTML
    assert "loadConversation" in HTML
    assert "startActivityPolling" in HTML
    assert "'/turns'" in HTML
    assert "api('/chat'" not in HTML
    assert "applyConversationSnapshot" in HTML
    assert "chat.quick_replies" in HTML
    assert "renderUiActions" in HTML
    assert "detectedQuickReplies" in HTML
    assert "appendDetectedQuickReplies" in HTML
    assert "reconcileProposals" in HTML
    reconcile = HTML.split("async function reconcileProposals", 1)[1].split("function renderProposals", 1)[0]
    assert "if (state.busy) { renderProposals([]); return; }" in reconcile
    assert "conversationId === state.conversationId && !state.busy" in reconcile
    assert "result.continuation" in HTML
    assert "await loadConversation(conversationId)" in HTML
    assert "the AI continuation could not start" in HTML
    assert "selected effort may take several minutes" in HTML
    assert "stopCurrentTurn" in HTML
    assert 'id="reasoning-summary"' in HTML
    assert 'id="activity-history"' in HTML
    assert "conversation.reasoning_summary" in HTML
    assert HTML.index('id="messages"') < HTML.index('id="chat-status-row"')
    assert "AbortController" not in HTML
    assert "provider: $('provider-select').value" in HTML
    assert "try { await loadModels(); }" in HTML
    assert "await newChat(true); await loadModels();" not in HTML
    pagehide = HTML.split("pagehide", 1)[1]
    assert "/cancel" not in pagehide


def test_ai_chat_builds_model_specific_reasoning_variants() -> None:
    """The effort picker must use each model's metadata instead of a fixed variant list."""
    assert 'id="effort-select"' in HTML
    assert "rebuildEffortOptions" in HTML
    assert "model.reasoning_variants" in HTML
    assert "variant.description" in HTML
    assert "model.default_effort" in HTML
    assert '<option value="low">Low</option>' not in HTML
    assert '<option value="medium">Medium</option>' not in HTML
    assert '<option value="high">High</option>' not in HTML
    assert "effort: effort" in HTML
    assert "models/health-refresh" in HTML
