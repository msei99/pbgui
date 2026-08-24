"""Contract tests for the authenticated minimal AI chat API."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from starlette.requests import Request

import api.ai as ai_api
from api.auth import require_auth


def _request() -> Request:
    """Build a minimal request for direct page rendering."""
    return Request(
        {
            "type": "http",
            "scheme": "http",
            "server": ("testserver", 80),
            "path": "/api/ai/main_page",
            "headers": [],
            "query_string": b"",
        }
    )


def test_ai_routes_require_auth_dependency() -> None:
    """Every AI route must retain shared PBGui authentication."""
    for route in ai_api.router.routes:
        dependency_calls = {dependency.call for dependency in route.dependant.dependencies}
        assert require_auth in dependency_calls


def test_main_page_is_cookie_only_and_no_store() -> None:
    """The AI page must not render or inspect the HttpOnly session token."""
    class CookieOnlySession:
        """Fail if page rendering reads a session field."""

        def __getattr__(self, name):
            raise AssertionError(f"session field accessed: {name}")

    response = ai_api.main_page(_request(), CookieOnlySession())
    html = response.body.decode("utf-8")

    assert response.headers["cache-control"] == "no-store"
    assert "%%API_BASE%%" not in html
    assert "%%TOKEN%%" not in html
    assert "Authorization" not in html
    assert "PBGUI_NAV_CONFIG" in html


def test_status_returns_only_non_secret_provider_projection(monkeypatch) -> None:
    """Provider status must not expose credentials or account email."""
    class FakeService:
        """Return a safe provider projection."""

        async def status(self, owner):
            assert len(owner) == 32
            return {
                "providers": {
                    "chatgpt": {"available": True, "connected": True, "plan": "plus"},
                    "opencode-go": {"available": True, "connected": True},
                }
            }

    monkeypatch.setattr(ai_api, "get_ai_chat_service", lambda: FakeService())
    response = asyncio_run(ai_api.status(SimpleNamespace(user_id="owner")))
    payload = response.body.decode("utf-8")

    assert response.headers["cache-control"] == "no-store"
    assert "api_key" not in payload
    assert "access_token" not in payload
    assert "refresh_token" not in payload
    assert "email" not in payload


def test_chat_forwards_only_validated_fields_to_service(monkeypatch) -> None:
    """The route should pass one bounded text turn to the owner-scoped service."""
    calls = []

    class FakeService:
        """Record one route invocation."""

        async def chat(self, *args):
            calls.append(args)
            return {"conversation_id": "a" * 32, "reply": "hello"}

    monkeypatch.setattr(ai_api, "get_ai_chat_service", lambda: FakeService())
    body = ai_api.ChatRequest(
        provider="opencode-go", model="gpt-5.6-luna", message="hi", effort="high"
    )
    response = asyncio_run(ai_api.chat(body, SimpleNamespace(user_id="owner")))

    assert response.headers["cache-control"] == "no-store"
    assert json_body(response) == {"conversation_id": "a" * 32, "reply": "hello"}
    assert calls[0][1:] == ("opencode-go", "gpt-5.6-luna", "hi", None, "high")


def test_conversation_is_created_before_first_chat_turn(monkeypatch) -> None:
    """The frontend needs a cancellable owner-bound ID before provider work starts."""
    class FakeService:
        """Return a deterministic conversation ID."""

        async def create_conversation(self, owner, provider, model, effort, context):
            assert len(owner) == 32
            assert provider == "chatgpt"
            assert model == "gpt-test"
            assert effort == ""
            assert context is None
            return "b" * 32

    monkeypatch.setattr(ai_api, "get_ai_chat_service", lambda: FakeService())
    body = ai_api.ConversationCreateRequest(provider="chatgpt", model="gpt-test")
    response = asyncio_run(ai_api.create_conversation(body, SimpleNamespace(user_id="owner")))

    assert response.status_code == 201
    assert json_body(response) == {"conversation_id": "b" * 32}


def test_conversation_activity_is_owner_scoped_and_no_store(monkeypatch) -> None:
    """The progress route should delegate through the owner-bound service projection."""
    class FakeService:
        """Return one non-sensitive activity label."""

        async def conversation_activity(self, owner, conversation_id):
            assert len(owner) == 32
            assert conversation_id == "c" * 32
            return {"busy": True, "activity": "Searching documentation", "step": 2}

    monkeypatch.setattr(ai_api, "get_ai_chat_service", lambda: FakeService())
    response = asyncio_run(
        ai_api.conversation_activity("c" * 32, SimpleNamespace(user_id="owner"))
    )

    assert response.headers["cache-control"] == "no-store"
    assert json_body(response) == {
        "busy": True,
        "activity": "Searching documentation",
        "step": 2,
    }


def test_persistent_history_and_detached_turn_routes_are_owner_scoped(monkeypatch) -> None:
    """History snapshots and detached turns should delegate only with the opaque owner key."""
    class FakeService:
        """Record persistent conversation route calls."""

        async def list_conversations(self, owner):
            assert len(owner) == 32
            return [{"conversation_id": "a" * 32, "title": "History"}]

        async def get_conversation(self, owner, conversation_id):
            assert len(owner) == 32 and conversation_id == "a" * 32
            return {"conversation_id": conversation_id, "messages": []}

        async def start_turn(self, owner, conversation_id, message, context, effort, model, provider, internal=False):
            assert len(owner) == 32 and conversation_id == "a" * 32 and message == "Hello"
            assert context is None
            assert effort is None
            assert model is None
            assert provider is None
            assert internal is False
            return {"conversation_id": conversation_id, "turn_id": "b" * 32, "status": "queued"}

        async def acknowledge_ui_action(self, owner, conversation_id, action_id):
            assert len(owner) == 32
            assert conversation_id == "a" * 32
            assert action_id == "c" * 32

    monkeypatch.setattr(ai_api, "get_ai_chat_service", lambda: FakeService())
    session = SimpleNamespace(user_id="owner")

    listed = asyncio_run(ai_api.list_conversations(session))
    detail = asyncio_run(ai_api.get_conversation("a" * 32, session))
    started = asyncio_run(ai_api.start_turn("a" * 32, ai_api.TurnCreateRequest(message="Hello"), session))
    acknowledged = asyncio_run(ai_api.acknowledge_ui_action("a" * 32, "c" * 32, session))

    assert json_body(listed)["conversations"][0]["title"] == "History"
    assert json_body(detail)["messages"] == []
    assert started.status_code == 202
    assert json_body(started)["status"] == "queued"
    assert json_body(acknowledged) == {"status": "acknowledged"}


def test_model_health_refresh_is_owner_scoped_and_async(monkeypatch) -> None:
    """Manual model health refresh should queue work without exposing credentials."""
    class FakeService:
        """Record one owner refresh request."""

        async def request_model_health_refresh(self, owner):
            assert len(owner) == 32

    monkeypatch.setattr(ai_api, "get_ai_chat_service", lambda: FakeService())
    response = asyncio_run(ai_api.refresh_model_health(SimpleNamespace(user_id="owner")))

    assert response.status_code == 202
    assert json_body(response) == {"status": "queued"}


def test_opencode_go_subscription_redirect_uses_public_referral_link() -> None:
    """The provider setup CTA should use the configured referral without exposing credentials."""
    response = ai_api.open_opencode_go_subscription(SimpleNamespace(user_id="owner"))

    assert response.status_code == 307
    assert response.headers["location"] == "https://opencode.ai/go?ref=XPFTXPFZVF"
    assert response.headers["cache-control"] == "no-store"


def test_ai_preferences_are_owner_scoped_and_no_store(monkeypatch) -> None:
    """Drawer width preferences should use authenticated server-side persistence."""
    class FakeService:
        """Return and record one drawer width."""

        def get_preferences(self, owner):
            assert len(owner) == 32
            return {"drawer_width": 540}

        def save_preferences(self, owner, width):
            assert len(owner) == 32 and width == 620
            return {"drawer_width": width}

    monkeypatch.setattr(ai_api, "get_ai_chat_service", lambda: FakeService())
    session = SimpleNamespace(user_id="owner")
    loaded = asyncio_run(ai_api.get_preferences(session))
    saved = asyncio_run(
        ai_api.save_preferences(ai_api.AIPreferencesRequest(drawer_width=620), session)
    )

    assert loaded.headers["cache-control"] == "no-store"
    assert json_body(loaded) == {"drawer_width": 540}
    assert json_body(saved) == {"drawer_width": 620}


def test_rewind_route_is_owner_scoped_and_restores_prompt(monkeypatch) -> None:
    """Rewind should delegate one validated message index to the owner-bound service."""
    class FakeService:
        async def rewind_conversation(self, owner, conversation_id, message_index):
            assert len(owner) == 32 and conversation_id == "a" * 32 and message_index == 2
            return {"conversation_id": conversation_id, "messages": [], "restored_prompt": "retry"}

    monkeypatch.setattr(ai_api, "get_ai_chat_service", lambda: FakeService())
    response = asyncio_run(
        ai_api.rewind_conversation(
            "a" * 32,
            ai_api.ConversationRewindRequest(message_index=2),
            SimpleNamespace(user_id="owner"),
        )
    )

    assert json_body(response)["restored_prompt"] == "retry"


def test_browser_login_returns_only_the_authorization_url(monkeypatch) -> None:
    """Browser OAuth should expose no access or refresh token to the page."""
    class FakeService:
        """Return one safe OAuth URL."""

        async def start_codex_browser_login(self, owner):
            assert len(owner) == 32
            return {"auth_url": "https://auth.openai.com/test"}

    monkeypatch.setattr(ai_api, "get_ai_chat_service", lambda: FakeService())
    response = asyncio_run(ai_api.start_chatgpt_browser_login(SimpleNamespace(user_id="owner")))

    assert json_body(response) == {"auth_url": "https://auth.openai.com/test"}
    assert "token" not in response.body.decode("utf-8")


def test_proposal_approval_is_owner_scoped_and_no_store(monkeypatch) -> None:
    """The approval route should delegate only through the owner-bound capability service."""
    class FakeCapabilities:
        """Record one approval request."""

        async def approve(self, owner, proposal_id, payload_digest, conversation_id):
            assert len(owner) == 32
            assert proposal_id == "a" * 32
            assert payload_digest == "sha256:" + "b" * 64
            assert conversation_id == "c" * 32
            return {"status": "executed", "action": "save"}

    class FakeChat:
        """Record the hidden API-owned continuation turn."""

        async def start_turn(self, owner, conversation_id, message, context, effort, model, provider, internal):
            assert len(owner) == 32
            assert conversation_id == "c" * 32
            assert "approved PBGui action completed" in message
            assert context is None and effort is None and model is None and provider is None
            assert internal is True
            return {"conversation_id": conversation_id, "turn_id": "d" * 32, "status": "queued"}

    monkeypatch.setattr(ai_api, "get_ai_capability_service", lambda: FakeCapabilities())
    monkeypatch.setattr(ai_api, "get_ai_chat_service", lambda: FakeChat())
    response = asyncio_run(
        ai_api.approve_proposal(
            "a" * 32,
            ai_api.ProposalDecisionRequest(
                payload_digest="sha256:" + "b" * 64,
                conversation_id="c" * 32,
            ),
            SimpleNamespace(user_id="owner"),
        )
    )

    assert response.headers["cache-control"] == "no-store"
    assert json_body(response) == {
        "status": "executed",
        "action": "save",
        "continuation": {"conversation_id": "c" * 32, "turn_id": "d" * 32, "status": "queued"},
    }


def test_provider_errors_are_generic_for_unexpected_failures(monkeypatch) -> None:
    """Unexpected provider details must not be returned to the browser."""
    class FailingService:
        """Raise an internal exception without network access."""

        async def status(self, owner):
            raise RuntimeError("secret provider body")

    monkeypatch.setattr(ai_api, "get_ai_chat_service", lambda: FailingService())
    monkeypatch.setattr(ai_api, "_log", lambda *args, **kwargs: None)

    with pytest.raises(ai_api.HTTPException) as exc_info:
        asyncio_run(ai_api.status(SimpleNamespace(user_id="owner")))

    assert exc_info.value.status_code == 500
    assert exc_info.value.detail == "AI provider operation failed"
    assert exc_info.value.headers["Cache-Control"] == "no-store"


def test_capability_registry_and_action_history_are_owner_safe_and_no_store(monkeypatch) -> None:
    """Discovery is public-to-auth while history remains bound to the authenticated owner."""
    class FakeCapabilities:
        """Return path-free discovery and owner audit projections."""

        def capability_registry(self):
            return {"effect_classes": ["read", "write"], "virtual_resources": ["pbgui://draft/{id}"]}

        async def list_action_history(self, owner, limit):
            assert len(owner) == 32
            assert limit == 7
            return [{"proposal_id": "a" * 32, "status": "executed"}]

    monkeypatch.setattr(ai_api, "get_ai_capability_service", lambda: FakeCapabilities())
    session = SimpleNamespace(user_id="owner")

    registry = asyncio_run(ai_api.capability_registry(session))
    history = asyncio_run(ai_api.action_history(7, session))

    assert registry.headers["cache-control"] == "no-store"
    assert json_body(registry)["effect_classes"] == ["read", "write"]
    assert history.headers["cache-control"] == "no-store"
    assert json_body(history)["actions"][0]["status"] == "executed"


def asyncio_run(awaitable):
    """Run one async route directly for a focused unit test."""
    import asyncio

    return asyncio.run(awaitable)


def json_body(response):
    """Decode one JSON response body."""
    import json

    return json.loads(response.body)
