"""Isolated contracts for explicit Jev PBGui data transfer approval."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from ai_capabilities import AICapabilityService
from ai_chat import AIChatError, AIChatService, owner_key
from ai_openrouter import JEV_MODEL, OpenRouterDecisionError, decide_user_jev_request, prepare_user_jev_payload


class _ReadCapabilities:
    """Return one bounded source and expose a write tool for rejection checks."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def capability_registry(self) -> dict:
        """Describe read and write capabilities."""
        return {"capabilities": [
            {"name": "list_backtests", "effect": "read"},
            {"name": "get_optimizer_run_analysis", "effect": "analyze"},
            {"name": "propose_queue_pb8_config", "effect": "write"},
        ]}

    async def dispatch(self, owner: str, conversation_id: str, tool: str, args: dict) -> dict:
        """Return data with a path and token-like field that must be stripped."""
        self.calls.append((tool, args))
        return {"backtests": [{"name": "run-a", "drawdown": 0.12, "api_key": "secret",
                               "path": "/private/result"}]}

    async def reject_conversation(self, owner: str, conversation_id: str) -> None:
        """Allow a new turn to advance the isolated conversation."""
        return None

    _strip_paths = AICapabilityService._strip_paths
    _sanitize_config = AICapabilityService._sanitize_config


def _spec(tool: str = "list_backtests") -> dict:
    """Build an explicit source request."""
    return {
        "state": {"priority": "low drawdown"},
        "sources": [{"name": "recent", "tool": tool, "args": {"version": "v8", "limit": 2}}],
        "questions": {"safe": {"type": "noul", "instructions": "Is the recent result low risk?"}},
    }


def test_jev_preview_is_exact_owner_bound_one_time_and_read_only(tmp_path: Path) -> None:
    """Only a reviewed source payload can start its matching turn."""
    async def scenario() -> None:
        service = AIChatService(tmp_path / "ai")
        service.capabilities = _ReadCapabilities()
        owner = owner_key("alice")
        other = owner_key("bob")
        async def fake_model(*args, **kwargs):
            """Avoid live model discovery."""
            return {"id": JEV_MODEL}
        service._validate_provider_model = fake_model
        conversation_id = await service.create_conversation(owner, "openrouter", JEV_MODEL)
        message = "```jev\n" + json.dumps(_spec()) + "\n```"
        with pytest.raises(AIChatError, match="reviewed preview"):
            await service.start_turn(owner, conversation_id, message)
        with pytest.raises(AIChatError, match="read-only capability"):
            await service.preview_jev_transfer(owner, conversation_id,
                                               json.dumps(_spec("propose_queue_pb8_config")), JEV_MODEL)
        assert service.capabilities.calls == []
        analysis = await service.preview_jev_transfer(
            owner, conversation_id, json.dumps(_spec("get_optimizer_run_analysis")), JEV_MODEL,
        )
        assert analysis["payload"]["state"]["pbgui"]["recent"]["backtests"][0]["name"] == "run-a"
        await service.discard_jev_preview(other, conversation_id, analysis["preview_id"])
        assert analysis["preview_id"] in service.jev_previews
        await service.discard_jev_preview(owner, conversation_id, analysis["preview_id"])
        assert analysis["preview_id"] not in service.jev_previews
        preview = await service.preview_jev_transfer(owner, conversation_id, message, JEV_MODEL)
        payload = preview["payload"]
        assert payload["state"]["pbgui"]["recent"]["backtests"][0] == {"name": "run-a", "drawdown": 0.12}
        assert payload == prepare_user_jev_payload(JEV_MODEL, _spec(), {"recent": {"backtests": [{"name": "run-a", "drawdown": 0.12}]}})
        token = preview["preview_id"]
        with pytest.raises(AIChatError, match="no longer matches"):
            await service.start_turn(owner, conversation_id, message.replace("low drawdown", "high drawdown"), jev_preview_id=token)
        with pytest.raises(AIChatError, match="no longer matches"):
            await service.start_turn(owner, conversation_id, message, model="other", jev_preview_id=token)
        with pytest.raises(AIChatError):
            await service.start_turn(other, conversation_id, message, jev_preview_id=token)
        assert token in service.jev_previews
        captured = []
        async def fake_detached(conversation, clean_message, turn_id, internal, approved_payload):
            """Capture the exact task payload without contacting a provider."""
            captured.append(approved_payload)
        service._run_detached_turn = fake_detached
        started = await service.start_turn(owner, conversation_id, message, jev_preview_id=token)
        await service.active_tasks[conversation_id]
        assert started["status"] == "queued"
        assert captured == [payload]
        assert token not in service.jev_previews
        service.conversations[conversation_id].busy = False
        service.active_tasks.pop(conversation_id, None)
        with pytest.raises(AIChatError, match="no longer matches"):
            await service.start_turn(owner, conversation_id, message, jev_preview_id=token)
    asyncio.run(scenario())


def test_jev_approved_payload_is_the_exact_provider_request() -> None:
    """A source request without approval never reaches OpenRouter."""
    spec = _spec()
    class Response:
        """Minimal provider response."""
        status = 200
        class Content:
            """Bounded response body."""
            async def read(self, size):
                """Return one typed Noul answer."""
                return b'{"answers":{"safe":{"type":"noul","noul":0.7}}}'
        content = Content()
        async def __aenter__(self):
            """Enter response."""
            return self
        async def __aexit__(self, *args):
            """Exit response."""
            return False
    class PricingResponse(Response):
        """Return the pinned model's official per-token pricing shape."""
        class Content:
            """Provide one bounded model-pricing body."""
            async def read(self, size):
                """Return free-output Jev pricing."""
                return b'{"data":{"pricing":{"prompt":"0.000000042","completion":"0"}}}'
        content = Content()
    class Session:
        """Record model-price checks and JSON sent to the provider."""
        def __init__(self):
            self.requests = []
            self.pricing_requests = []
        def get(self, url, **kwargs):
            """Supply the price needed before any billable request."""
            self.pricing_requests.append(url)
            return PricingResponse()
        def post(self, url, **kwargs):
            """Capture provider request."""
            self.requests.append(kwargs["json"])
            return Response()
    session = Session()
    with pytest.raises(OpenRouterDecisionError, match="approved preview"):
        asyncio.run(decide_user_jev_request(session, "key", JEV_MODEL, spec))
    assert session.requests == []
    assert session.pricing_requests == []
    approved = prepare_user_jev_payload(JEV_MODEL, spec, {"recent": {"backtests": [{"name": "run-a"}]}})
    answer = asyncio.run(decide_user_jev_request(session, "key", JEV_MODEL, spec, approved))
    assert "70%" in answer
    assert session.requests == [approved]
    assert len(session.pricing_requests) == 1
