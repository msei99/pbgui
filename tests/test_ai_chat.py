"""Focused offline tests for the minimal AI chat runtime."""

from __future__ import annotations

import asyncio
import copy
import json
import os
from pathlib import Path

import pytest

from ai_chat import (
    AICredentialStore,
    AIChatError,
    AIChatService,
    _COMPARE_KEEP_SOURCE_RISK,
    _COMPARE_PB7_TRAILING_VS_PB8_MARTINGALE,
    _GO_FALLBACK_MODELS,
    _MAX_CAPABILITY_ROUNDS,
    _MAX_HISTORY_CHARS,
    _MAX_PROVIDER_HANDOFF_CHARS,
    _go_instructions,
    owner_key,
)


def test_owner_key_is_stable_and_opaque() -> None:
    """Provider state directories must not expose the PBGui user identifier."""
    first = owner_key("admin@example.test")

    assert first == owner_key("admin@example.test")
    assert first != owner_key("other@example.test")
    assert len(first) == 32
    assert "admin" not in first


def test_go_key_store_is_private_atomic_and_never_returned_as_metadata(tmp_path: Path) -> None:
    """The dedicated provider store should expose only configured state to callers."""
    store = AICredentialStore(tmp_path / "credentials")
    owner = owner_key("owner")
    key = "sk-test-0123456789abcdef"

    store.save_go_key(owner, key)

    path = store.root / f"{owner}.json"
    assert store.configured(owner) is True
    assert store.load_go_key(owner) == key
    assert json.loads(path.read_text(encoding="utf-8"))["api_key"] == key
    if os.name == "posix":
        assert path.stat().st_mode & 0o777 == 0o600
        assert store.root.stat().st_mode & 0o777 == 0o700

    store.delete_go_key(owner)
    assert store.configured(owner) is False


@pytest.mark.parametrize("key", ["", "short", "bad\nkey", "x" * 1025])
def test_go_key_store_rejects_invalid_secrets(tmp_path: Path, key: str) -> None:
    """Malformed provider secrets must be rejected before filesystem writes."""
    store = AICredentialStore(tmp_path / "credentials")

    with pytest.raises(AIChatError):
        store.save_go_key(owner_key("owner"), key)


def test_response_text_extracts_openai_responses_payload(tmp_path: Path) -> None:
    """OpenCode Go Responses output should become plain assistant text."""
    service = AIChatService(tmp_path / "ai")
    payload = {
        "output": [
            {
                "type": "message",
                "content": [
                    {"type": "output_text", "text": "Hello"},
                    {"type": "refusal", "refusal": "ignored"},
                ],
            }
        ]
    }

    assert service._response_text(payload) == "Hello"


def test_response_text_uses_only_last_structured_message(tmp_path: Path) -> None:
    """Responses planning messages must not be concatenated into the visible final answer."""
    service = AIChatService(tmp_path / "ai")
    payload = {
        "output_text": "Planning retry\nFinal answer",
        "output": [
            {"type": "message", "content": [{"type": "output_text", "text": "Planning retry"}]},
            {"type": "reasoning", "summary": [{"type": "summary_text", "text": "Private plan"}]},
            {"type": "message", "content": [{"type": "output_text", "text": "Final answer"}]},
        ],
    }

    assert service._response_text(payload) == "Final answer"


def test_go_fallback_catalog_covers_all_documented_subscription_models() -> None:
    """The offline fallback should cover the current documented OpenCode Go catalog."""
    assert set(_GO_FALLBACK_MODELS) == {
        "deepseek-v4-flash",
        "deepseek-v4-flash-vision-exp",
        "deepseek-v4-pro",
        "glm-5.1",
        "glm-5.2",
        "glm-5.3",
        "gpt-5.6-luna",
        "grok-4.5",
        "hy3",
        "kimi-k2.6",
        "kimi-k2.7-code",
        "kimi-k3",
        "mimo-v2.5",
        "mimo-v2.5-pro",
        "minimax-m2.5",
        "minimax-m2.7",
        "minimax-m3",
        "muse-spark-1.2-contributor",
        "ox-alpha-free",
        "qwen3.6-plus",
        "qwen3.7-max",
        "qwen3.7-plus",
        "qwen3.8-max",
    }


@pytest.mark.parametrize(
    ("model", "endpoint", "protocol", "auth_header"),
    [
        ("gpt-5.6-luna", "responses", "responses", "Authorization"),
        ("glm-5.3", "chat/completions", "chat", "Authorization"),
        ("qwen3.8-max", "messages", "messages", "x-api-key"),
        ("future-chat-model", "chat/completions", "chat", "Authorization"),
    ],
)
def test_go_request_spec_matches_documented_protocols(
    model: str,
    endpoint: str,
    protocol: str,
    auth_header: str,
) -> None:
    """Each model family must use its documented endpoint and auth carrier."""
    history = [{"role": "user", "content": "Hello"}]
    dynamic_protocol = protocol if model == "future-chat-model" else None
    selected_endpoint, headers, body, selected_protocol = AIChatService._go_request_spec(
        model, "sk-test-key", history, dynamic_protocol
    )

    assert selected_endpoint == endpoint
    assert selected_protocol == protocol
    assert auth_header in headers
    assert body["model"] == model
    instructions = body.get("system") or body.get("instructions") or body["messages"][0]["content"]
    assert "No PBGui capability tools are available" in instructions
    assert "# PBGui AI Agent" not in instructions
    if protocol == "messages":
        assert headers["anthropic-version"] == "2023-06-01"
        assert "qwen3.8-max" in body["system"]
    elif protocol == "chat":
        assert body["messages"][0]["role"] == "system"
    else:
        assert body["input"] == history


def test_go_protocol_response_extractors_return_plain_text(tmp_path: Path) -> None:
    """All three Go wire protocols should produce the same plain-text chat contract."""
    service = AIChatService(tmp_path / "ai")

    assert service._chat_completion_text(
        {"choices": [{"message": {"content": "chat text"}}]}
    ) == "chat text"
    assert service._messages_text(
        {"content": [{"type": "text", "text": "message text"}]}
    ) == "message text"
    assert service._response_reasoning_summary(
        {
            "output": [
                {
                    "type": "reasoning",
                    "encrypted_content": "must-not-be-returned",
                    "summary": [{"type": "summary_text", "text": "Provider summary"}],
                }
            ]
        }
    ) == "Provider summary"


def test_chat_only_tool_protocol_leakage_is_detected() -> None:
    """Internal provider tool syntax must never be rendered as an assistant answer."""
    assert AIChatService._contains_unsupported_tool_syntax(
        "I'll look that up. monen_tool: pb_passivbot_docs"
    )
    assert not AIChatService._contains_unsupported_tool_syntax(
        "I cannot inspect the installed documentation in this chat."
    )


@pytest.mark.parametrize(
    ("protocol", "method_name"),
    [("responses", "_go_responses_agent"), ("messages", "_go_messages_agent")],
)
def test_go_chat_routes_tool_capable_protocols_to_native_agents(
    tmp_path: Path, monkeypatch, protocol: str, method_name: str
) -> None:
    """Productive Responses and Messages turns must not fall back to chat-only requests."""
    async def scenario() -> None:
        service = AIChatService(tmp_path / "ai")
        owner = "a" * 32
        service.credentials.save_go_key(owner, "sk-test-0123456789abcdef")
        calls = []

        async def fake_models(provider):
            return [
                {
                    "id": "model-test",
                    "protocol": protocol,
                    "tools": True,
                    "reasoning_variants": [],
                }
            ]

        async def fake_agent(*args):
            calls.append(args)
            return "answer"

        monkeypatch.setattr(service, "_go_models", fake_models)
        monkeypatch.setattr(service, method_name, fake_agent)
        reply = await service._go_chat(
            owner,
            "model-test",
            [{"role": "user", "content": "Hello"}],
            "opencode-go",
            "c" * 32,
        )

        assert reply == "answer"
        assert len(calls) == 1
        await service.shutdown()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("cost", "expected"),
    [
        ({"input": 0, "output": 0}, True),
        ({"input": 0.0, "output": 0.0, "cache_read": 0}, True),
        ({"input": 0, "output": 1}, False),
        ({}, False),
        (None, False),
    ],
)
def test_free_model_detection_uses_live_zero_cost_metadata(cost, expected) -> None:
    """Free labels must come from provider cost metadata instead of model names."""
    assert AIChatService._model_cost_is_free(cost) is expected


def test_reasoning_variants_preserve_model_metadata_order_and_names() -> None:
    """OpenCode effort choices must come from each model instead of a fixed UI list."""
    metadata = {
        "reasoning_options": [
            {"type": "toggle"},
            {"type": "effort", "values": [None, "minimal", "high", "xhigh", "ultra"]},
        ]
    }

    variants = AIChatService._reasoning_variants(metadata, "responses", "model", 32_000)

    assert [item["id"] for item in variants] == ["none", "minimal", "high", "xhigh", "ultra"]
    assert all(item["type"] == "effort" for item in variants)
    assert AIChatService._reasoning_variants({"reasoning_options": []}, "chat", "model", 0) == []
    assert AIChatService._reasoning_variants(
        {"reasoning_options": [{"type": "future"}]}, "chat", "model", 0
    ) == []


def test_reasoning_variants_project_verified_message_presets() -> None:
    """Messages budget and MiniMax toggle metadata should use their protocol presets."""
    budget = AIChatService._reasoning_variants(
        {"reasoning_options": [{"type": "budget_tokens", "min": 1024, "max": 20_000}]},
        "messages",
        "claude-test",
        30_000,
    )
    toggle = AIChatService._reasoning_variants(
        {"reasoning_options": [{"type": "toggle"}]},
        "messages",
        "minimax-m3",
        32_000,
    )

    assert [(item["id"], item["budget_tokens"]) for item in budget] == [
        ("high", 10_000),
        ("max", 20_000),
    ]
    assert [item["id"] for item in toggle] == ["none", "thinking"]


@pytest.mark.parametrize(
    ("protocol", "expected"),
    [
        ("chat", {"reasoning_effort": "xhigh"}),
        (
            "responses",
            {
                "reasoning": {"effort": "xhigh", "summary": "auto"},
                "include": ["reasoning.encrypted_content"],
            },
        ),
        ("messages", {"output_config": {"effort": "xhigh"}}),
    ],
)
def test_reasoning_variant_uses_protocol_correct_wire_shape(protocol, expected) -> None:
    """One advertised effort should map to the correct provider protocol fields."""
    body = {}

    AIChatService._apply_reasoning_variant(
        body,
        protocol,
        "qwen-test",
        {"id": "xhigh", "type": "effort", "value": "xhigh"},
    )

    assert body == expected


@pytest.mark.parametrize(
    ("status", "message", "expected"),
    [
        (400, "Model is unavailable.", "Selected AI model is currently unavailable"),
        (429, "Rate limit exceeded.", "AI provider rate limit reached"),
        (403, "Invalid API key", "AI provider authentication failed"),
        (400, "Region not allowed", "Selected AI model is not available in this region"),
        (
            403,
            "The latest version of this model is only available hosted in China and requires explicit opt in",
            "Selected AI model is not available in this region",
        ),
        (
            403,
            "This model collects data used to improve its quality and requires explicit opt in",
            "Selected AI model requires training-data permission in OpenCode settings",
        ),
        (500, "upstream failed", "AI provider is temporarily unavailable"),
    ],
)
def test_safe_provider_errors_preserve_actionable_category(status, message, expected) -> None:
    """Provider bodies should yield useful categories without returning raw diagnostics."""
    raw = json.dumps({"error": {"type": "server_error", "message": message}}).encode()

    assert AIChatService._safe_provider_error(status, raw) == expected


def test_conversation_owner_and_provider_cannot_be_switched(tmp_path: Path) -> None:
    """Opaque conversation IDs must remain bound to owner and provider."""
    async def scenario() -> None:
        service = AIChatService(tmp_path / "ai")
        conversation = await service._conversation("a" * 32, "opencode-go", "model", None)

        with pytest.raises(AIChatError, match="not found"):
            await service._conversation("b" * 32, "opencode-go", "model", conversation.id)
        with pytest.raises(AIChatError, match="cannot be changed"):
            await service._conversation("a" * 32, "chatgpt", "model", conversation.id)
        with pytest.raises(AIChatError, match="model cannot be changed"):
            await service._conversation("a" * 32, "opencode-go", "other-model", conversation.id)

        await service.shutdown()

    asyncio.run(scenario())


def test_go_chat_keeps_bounded_role_history_without_provider_network(tmp_path: Path, monkeypatch) -> None:
    """A successful Go turn should retain only user and assistant text in memory."""
    async def scenario() -> None:
        service = AIChatService(tmp_path / "ai")
        service.credentials.save_go_key("a" * 32, "sk-test-0123456789abcdef")

        async def fake_go_chat(owner, model, messages, provider, conversation_id, effort):
            assert owner == "a" * 32
            assert model == "gpt-5.6-luna"
            assert provider == "opencode-go"
            assert len(conversation_id) == 32
            assert effort == ""
            assert messages[-1] == {
                "role": "user",
                "content": "Hello",
            }
            return "Hi"

        monkeypatch.setattr(service, "_go_chat", fake_go_chat)
        conversation = await service._conversation(
            "a" * 32, "opencode-go", "gpt-5.6-luna", None
        )
        result = await service.chat(
            "a" * 32,
            "opencode-go",
            "gpt-5.6-luna",
            "Hello",
            conversation.id,
        )
        conversation = service.conversations[result["conversation_id"]]

        assert result["reply"] == "Hi"
        assert conversation.messages == [
            {"role": "user", "content": "Hello", "display_content": "Hello"},
            {"role": "assistant", "content": "Hi"},
        ]
        await service.shutdown()

    asyncio.run(scenario())


def test_go_tool_loop_forces_a_final_answer_after_capability_rounds(
    tmp_path: Path, monkeypatch
) -> None:
    """A tool-happy model must synthesize collected results instead of failing at the round limit."""
    class ResponseContext:
        """Provide the minimal async response context used by the agent loop."""

        async def __aenter__(self):
            return object()

        async def __aexit__(self, exc_type, exc, traceback):
            return False

    class FakeSession:
        """Capture provider request bodies without network access."""

        def __init__(self) -> None:
            self.requests = []

        def post(self, url, **kwargs):
            self.requests.append(json.loads(json.dumps(kwargs["json"])))
            return ResponseContext()

    class FakeCapabilities:
        """Return one bounded result for every unique model request."""

        @staticmethod
        def chat_completion_tools():
            return [{"type": "function", "function": {"name": "list_optimizer_configs"}}]

        @staticmethod
        async def dispatch(owner, conversation_id, name, arguments):
            return {"items": [], "request": arguments["request"]}

    async def scenario() -> None:
        service = AIChatService(tmp_path / "ai")
        session = FakeSession()
        service.capabilities = FakeCapabilities()
        payloads = [
            {
                "choices": [
                    {
                        "message": {
                            "content": None,
                            "reasoning_content": f"reason-{index}",
                            "tool_calls": [
                                {
                                    "id": f"call-{index}",
                                    "function": {
                                        "name": "list_optimizer_configs",
                                        "arguments": json.dumps({"request": index}),
                                    },
                                }
                            ],
                        }
                    }
                ]
            }
            for index in range(_MAX_CAPABILITY_ROUNDS)
        ]
        payloads.append({"choices": [{"message": {"content": "Final answer"}}]})

        async def fake_http_session():
            return session

        async def fake_read_response(response, **kwargs):
            return payloads.pop(0)

        monkeypatch.setattr(service, "_http_session", fake_http_session)
        monkeypatch.setattr(service, "_read_json_response", fake_read_response)
        conversation = await service._conversation("a" * 32, "opencode-go", "mimo-v2.5", None)

        reply = await service._go_chat_completion_agent_inner(
            "a" * 32,
            conversation.id,
            "https://example.test/v1",
            "mimo-v2.5",
            "sk-test",
            [{"role": "user", "content": "Explain this"}],
            "high",
        )

        assert reply == "Final answer"
        assert len(session.requests) == _MAX_CAPABILITY_ROUNDS + 1
        assert all("tools" in request for request in session.requests[:_MAX_CAPABILITY_ROUNDS])
        assert all(request["reasoning_effort"] == "high" for request in session.requests)
        assert session.requests[1]["messages"][-2]["reasoning_content"] == "reason-0"
        final_request = session.requests[_MAX_CAPABILITY_ROUNDS]
        assert "tools" not in final_request
        assert final_request["messages"][-1]["role"] == "system"
        assert "Do not call any more tools" in final_request["messages"][-1]["content"]
        assert conversation.activity == "Preparing the final answer"
        await service.shutdown()

    asyncio.run(scenario())


def test_responses_agent_executes_native_tools_and_replays_results(
    tmp_path: Path, monkeypatch
) -> None:
    """Responses models should receive native function output and return a final answer."""
    class ResponseContext:
        """Provide one asynchronous HTTP response context."""

        async def __aenter__(self):
            return object()

        async def __aexit__(self, exc_type, exc, traceback):
            return False

    class FakeSession:
        """Capture stateless Responses request bodies."""

        def __init__(self) -> None:
            self.requests = []

        def post(self, url, **kwargs):
            assert url.endswith("/responses")
            self.requests.append(json.loads(json.dumps(kwargs["json"])))
            return ResponseContext()

    class FakeCapabilities:
        """Expose one native Responses tool and result."""

        @staticmethod
        def responses_tools():
            return [{"type": "function", "name": "search_passivbot_docs", "parameters": {}}]

        @staticmethod
        async def dispatch(owner, conversation_id, name, arguments):
            return {"commit": "abc", "matches": [{"path": "docs/config.md"}]}

    async def scenario() -> None:
        service = AIChatService(tmp_path / "ai")
        service.capabilities = FakeCapabilities()
        session = FakeSession()
        payloads = [
            {
                "output": [
                    {"type": "reasoning", "encrypted_content": "opaque"},
                    {
                        "type": "function_call",
                        "call_id": "call-1",
                        "name": "search_passivbot_docs",
                        "arguments": '{"version":"v8","query":"EMA anchor"}',
                    },
                ]
            },
            {
                "output": [
                    {
                        "type": "message",
                        "content": [{"type": "output_text", "text": "Installed-source answer"}],
                    }
                ]
            },
        ]

        async def fake_http_session():
            return session

        async def fake_read_response(response, **kwargs):
            return payloads.pop(0)

        monkeypatch.setattr(service, "_http_session", fake_http_session)
        monkeypatch.setattr(service, "_read_json_response", fake_read_response)
        conversation = await service._conversation("a" * 32, "opencode-go", "gpt-5.6-luna", None)

        reply = await service._go_responses_agent_inner(
            "a" * 32,
            conversation.id,
            "https://example.test/v1",
            "gpt-5.6-luna",
            "sk-test",
            [{"role": "user", "content": "What is EMA anchor?"}],
            {"id": "high", "type": "effort", "value": "high"},
        )

        assert reply == "Installed-source answer"
        assert session.requests[0]["tools"][0]["name"] == "search_passivbot_docs"
        assert session.requests[0]["reasoning"]["effort"] == "high"
        assert session.requests[0]["prompt_cache_key"] == conversation.id
        replay = session.requests[1]["input"]
        assert any(item.get("type") == "reasoning" for item in replay)
        result = next(item for item in replay if item.get("type") == "function_call_output")
        assert result["call_id"] == "call-1"
        assert json.loads(result["output"])["success"] is True
        await service.shutdown()

    asyncio.run(scenario())


def test_responses_agent_turns_excess_parallel_calls_into_final_answer(
    tmp_path: Path, monkeypatch
) -> None:
    """Grok-style parallel calls beyond the budget must not abort the whole turn."""
    class ResponseContext:
        async def __aenter__(self):
            return object()

        async def __aexit__(self, exc_type, exc, traceback):
            return False

    class FakeSession:
        def __init__(self) -> None:
            self.requests = []

        def post(self, url, **kwargs):
            self.requests.append(copy.deepcopy(kwargs["json"]))
            return ResponseContext()

    class FakeCapabilities:
        def __init__(self) -> None:
            self.calls = []

        @staticmethod
        def responses_tools():
            return [{"type": "function", "name": "search_passivbot_docs", "parameters": {}}]

        async def dispatch(self, owner, conversation_id, name, arguments):
            self.calls.append(arguments["query"])
            return {"matches": []}

    async def scenario() -> None:
        service = AIChatService(tmp_path / "ai")
        capabilities = FakeCapabilities()
        service.capabilities = capabilities
        session = FakeSession()
        calls = [
            {
                "type": "function_call",
                "call_id": f"call-{index}",
                "name": "search_passivbot_docs",
                "arguments": json.dumps({"version": "v8", "query": f"query-{index}"}),
            }
            for index in range(18)
        ]
        payloads = [
            {"output": calls},
            {"output": [{"type": "message", "content": [{"type": "output_text", "text": "Final answer"}]}]},
        ]

        async def fake_http_session():
            return session

        async def fake_read_response(response, **kwargs):
            return payloads.pop(0)

        monkeypatch.setattr(service, "_http_session", fake_http_session)
        monkeypatch.setattr(service, "_read_json_response", fake_read_response)
        conversation = await service._conversation("a" * 32, "opencode-go", "grok-4.6", None)

        reply = await service._go_responses_agent_inner(
            "a" * 32,
            conversation.id,
            "https://example.test/v1",
            "grok-4.6",
            "sk-test",
            [{"role": "user", "content": "Compare strategies"}],
            None,
        )

        assert reply == "Final answer"
        assert len(capabilities.calls) == 16
        assert "tools" in session.requests[0]
        assert "tools" not in session.requests[1]
        outputs = [item for item in session.requests[1]["input"] if item.get("type") == "function_call_output"]
        assert len(outputs) == 18
        assert all(json.loads(item["output"])["success"] is False for item in outputs[-2:])
        assert all("budget exhausted" in json.loads(item["output"])["error"] for item in outputs[-2:])
        await service.shutdown()

    asyncio.run(scenario())


def test_responses_agent_retries_one_transient_connection_failure(
    tmp_path: Path, monkeypatch
) -> None:
    """A dropped Grok Responses connection should retry once within a bounded request budget."""
    class ResponseContext:
        def __init__(self, fail: bool) -> None:
            self.fail = fail

        async def __aenter__(self):
            if self.fail:
                raise ConnectionResetError("provider disconnected")
            return object()

        async def __aexit__(self, exc_type, exc, traceback):
            return False

    class FakeSession:
        def __init__(self) -> None:
            self.calls = 0

        def post(self, url, **kwargs):
            self.calls += 1
            return ResponseContext(self.calls == 1)

    class FakeCapabilities:
        @staticmethod
        def responses_tools():
            return []

    async def scenario() -> None:
        service = AIChatService(tmp_path / "ai")
        service.capabilities = FakeCapabilities()
        session = FakeSession()

        async def fake_http_session():
            return session

        async def fake_read_response(response, **kwargs):
            return {"output": [{"type": "message", "content": [{"type": "output_text", "text": "Recovered"}]}]}

        monkeypatch.setattr(service, "_http_session", fake_http_session)
        monkeypatch.setattr(service, "_read_json_response", fake_read_response)
        conversation = await service._conversation("a" * 32, "opencode-go", "grok-4.6", None)

        reply = await service._go_responses_agent_inner(
            "a" * 32,
            conversation.id,
            "https://example.test/v1",
            "grok-4.6",
            "sk-test",
            [{"role": "user", "content": "Compare strategies"}],
            None,
        )

        assert reply == "Recovered"
        assert session.calls == 2
        assert conversation.activity_history[-1]["message"] == (
            "OpenCode connection interrupted; retrying model request"
        )
        await service.shutdown()

    asyncio.run(scenario())


def test_messages_agent_executes_native_tools_and_replays_results(
    tmp_path: Path, monkeypatch
) -> None:
    """Messages models should preserve thinking blocks and return native tool results."""
    class ResponseContext:
        """Provide one asynchronous HTTP response context."""

        async def __aenter__(self):
            return object()

        async def __aexit__(self, exc_type, exc, traceback):
            return False

    class FakeSession:
        """Capture native Messages request bodies."""

        def __init__(self) -> None:
            self.requests = []

        def post(self, url, **kwargs):
            assert url.endswith("/messages")
            self.requests.append(json.loads(json.dumps(kwargs["json"])))
            return ResponseContext()

    class FakeCapabilities:
        """Expose one native Messages tool and result."""

        @staticmethod
        def messages_tools():
            return [{"name": "read_passivbot_source", "input_schema": {}}]

        @staticmethod
        async def dispatch(owner, conversation_id, name, arguments):
            return {"commit": "abc", "content": "source lines"}

    async def scenario() -> None:
        service = AIChatService(tmp_path / "ai")
        service.capabilities = FakeCapabilities()
        session = FakeSession()
        payloads = [
            {
                "content": [
                    {"type": "thinking", "thinking": "summary", "signature": "opaque"},
                    {
                        "type": "tool_use",
                        "id": "tool-1",
                        "name": "read_passivbot_source",
                        "input": {"version": "v8", "path": "src/strategy.rs"},
                    },
                ]
            },
            {"content": [{"type": "text", "text": "Source-backed answer"}]},
        ]

        async def fake_http_session():
            return session

        async def fake_read_response(response, **kwargs):
            return payloads.pop(0)

        monkeypatch.setattr(service, "_http_session", fake_http_session)
        monkeypatch.setattr(service, "_read_json_response", fake_read_response)
        conversation = await service._conversation("a" * 32, "opencode-go", "qwen3.8-max", None)

        reply = await service._go_messages_agent_inner(
            "a" * 32,
            conversation.id,
            "https://example.test/v1",
            "qwen3.8-max",
            "sk-test",
            [{"role": "user", "content": "Read the implementation"}],
            None,
        )

        assert reply == "Source-backed answer"
        assert session.requests[0]["tools"][0]["name"] == "read_passivbot_source"
        replay = session.requests[1]["messages"]
        assert replay[-2]["content"][0]["type"] == "thinking"
        tool_result = replay[-1]["content"][0]
        assert tool_result["type"] == "tool_result"
        assert tool_result["tool_use_id"] == "tool-1"
        assert json.loads(tool_result["content"])["success"] is True
        await service.shutdown()

    asyncio.run(scenario())


def test_conversation_activity_is_owner_bound_and_non_sensitive(tmp_path: Path) -> None:
    """Progress polling should expose only a bounded label for the owning user."""
    async def scenario() -> None:
        service = AIChatService(tmp_path / "ai")
        conversation = await service._conversation("a" * 32, "opencode-go", "model", None)
        await service._reserve_conversation(conversation)
        await service._set_activity("a" * 32, conversation.id, "Searching documentation")

        result = await service.conversation_activity("a" * 32, conversation.id)

        assert result == {"busy": True, "activity": "Searching documentation", "step": 1}
        with pytest.raises(AIChatError, match="not found"):
            await service.conversation_activity("b" * 32, conversation.id)
        await service.shutdown()

    asyncio.run(scenario())


def test_persistent_conversation_history_reloads_owner_safe_messages(tmp_path: Path) -> None:
    """Completed display history and page context should survive service recreation."""
    async def scenario() -> None:
        owner = "a" * 32
        first = AIChatService(tmp_path / "ai")
        conversation = await first._conversation(owner, "opencode-go", "model", None)
        conversation.title = "EMA anchor"
        conversation.context = first._validate_page_context(
            {"schema_version": 1, "page_key": "v8_optimize", "guide_topic": "43_pbv8_optimize"}
        )
        conversation.messages = [
            {"role": "user", "content": "hidden context", "display_content": "What is EMA?"},
            {"role": "assistant", "content": "Answer"},
        ]
        conversation.reasoning_summary = "Provider summary"
        conversation.activity_history = [{"timestamp": 1.0, "message": "Analyzing request"}]
        first._persist_conversation(conversation)
        await first.shutdown()

        second = AIChatService(tmp_path / "ai")
        items = await second.list_conversations(owner)
        snapshot = await second.get_conversation(owner, conversation.id)

        assert items[0]["title"] == "EMA anchor"
        assert snapshot["messages"] == [
            {"role": "user", "content": "What is EMA?"},
            {"role": "assistant", "content": "Answer"},
        ]
        assert snapshot["context"]["guide_topic"] == "43_pbv8_optimize"
        assert snapshot["reasoning_summary"] == "Provider summary"
        assert snapshot["activity_history"][0]["message"] == "Analyzing request"
        assert "owner" not in snapshot
        await second.shutdown()

    asyncio.run(scenario())


def test_generic_page_ui_action_is_captured_and_restored(tmp_path: Path) -> None:
    """Typed page actions should remain pending until an advertising page handles them."""
    async def scenario() -> None:
        owner = "a" * 32
        service = AIChatService(tmp_path / "ai")
        conversation = await service._conversation(owner, "opencode-go", "model", None)
        await service._capture_ui_action(
            owner,
            conversation.id,
            {
                "ui_action": {
                    "type": "page.perform_action",
                    "target": {"page_key": "v8_optimize"},
                    "payload": {
                        "action": "show_log",
                        "entity": {"kind": "optimizer_queue_item", "name": "optimize_123"},
                    },
                }
            },
        )

        snapshot = await service.get_conversation(owner, conversation.id)
        restored = AIChatService._restore_ui_actions(snapshot["ui_actions"])

        assert restored[0]["type"] == "page.perform_action"
        assert restored[0]["payload"]["action"] == "show_log"
        await service.shutdown()

    asyncio.run(scenario())


def test_local_browser_action_is_persisted_without_provider_turn(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A browser-completed action should add one durable turn without becoming busy."""
    async def scenario() -> None:
        owner = "a" * 32
        service = AIChatService(tmp_path / "ai")
        conversation = await service._conversation(owner, "opencode-go", "model", None)
        rejected = []

        async def reject_conversation(owner_arg, conversation_id_arg):
            """Capture stale-proposal rejection before the local branch advances."""

            rejected.append((owner_arg, conversation_id_arg))

        monkeypatch.setattr(service.capabilities, "reject_conversation", reject_conversation)

        snapshot = await service.record_local_action(
            owner,
            conversation.id,
            "Close the log window",
            {"schema_version": 1, "page_key": "v8_optimize"},
        )

        assert snapshot["busy"] is False
        assert snapshot["messages"][-2] == {"role": "user", "content": "Close the log window"}
        assert snapshot["messages"][-1]["content"] == "PBGui completed the requested interface action locally."
        assert conversation.id not in service.active_tasks
        assert rejected == [(owner, conversation.id)]
        await service.shutdown()

    asyncio.run(scenario())


def test_detached_turn_completes_without_request_owned_task(tmp_path: Path, monkeypatch) -> None:
    """A server-owned turn should persist its result independently of the start request."""
    async def scenario() -> None:
        owner = "a" * 32
        service = AIChatService(tmp_path / "ai")
        service.credentials.save_go_key(owner, "sk-test-0123456789abcdef")
        conversation = await service._conversation(owner, "opencode-go", "model", None)

        async def fake_go_chat(*args):
            await asyncio.sleep(0)
            return "Detached answer"

        monkeypatch.setattr(service, "_go_chat", fake_go_chat)
        started = await service.start_turn(
            owner,
            conversation.id,
            "Question",
            {"schema_version": 1, "page_key": "v8_optimize", "section": "configs"},
        )
        queued = await service.get_conversation(owner, conversation.id)
        task = service.active_tasks[conversation.id]
        await task
        snapshot = await service.get_conversation(owner, conversation.id)

        assert started["status"] == "queued"
        assert queued["messages"][-1] == {"role": "user", "content": "Question"}
        assert queued["context"]["section"] == "configs"
        assert snapshot["busy"] is False
        assert snapshot["messages"][-1] == {"role": "assistant", "content": "Detached answer"}
        await service.shutdown()

    asyncio.run(scenario())


def test_chatgpt_conversation_reuses_runtime_for_later_turns(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Later ChatGPT turns must use the conversation's existing runtime and thread."""
    async def scenario() -> None:
        owner = "a" * 32
        service = AIChatService(tmp_path / "ai")
        conversation = await service._conversation(owner, "chatgpt", "model", None)
        calls = []

        class FakeRuntime:
            """Minimal live runtime retaining one ephemeral thread."""

            process = object()
            closing = False

            async def start_thread(self, model, tools):
                calls.append(("start", model))
                return "thread-1"

            async def chat(self, thread_id, message, model, effort):
                calls.append(("chat", thread_id, message))
                return "answer"

        runtime = FakeRuntime()
        monkeypatch.setattr(service, "_codex_runtime", lambda owner_arg: runtime)
        monkeypatch.setattr(service, "_ensure_reaper", lambda: None)
        monkeypatch.setattr(service, "_close_idle_codex_runtimes", lambda: asyncio.sleep(0))

        await service.chat(owner, "chatgpt", "model", "First", conversation.id)
        await service.chat(owner, "chatgpt", "model", "Second", conversation.id)

        assert calls == [
            ("start", "model"),
            ("chat", "thread-1", "First"),
            ("chat", "thread-1", "Second"),
        ]
        assert (await service.get_conversation(owner, conversation.id))["messages"][-1] == {
            "role": "assistant",
            "content": "answer",
        }
        await service.shutdown()

    asyncio.run(scenario())


def test_chatgpt_action_promise_gets_one_corrective_tool_turn(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An explicit action request must not finish as an unsupported future promise."""
    async def scenario() -> None:
        owner = "a" * 32
        service = AIChatService(tmp_path / "ai")
        conversation = await service._conversation(owner, "chatgpt", "model", None)
        calls = []

        class FakeCapabilities:
            """Expose an empty proposal catalog for the action-enforcement contract."""

            @staticmethod
            def codex_dynamic_tools():
                return []

            @staticmethod
            async def list_proposals(_owner, _conversation_id):
                return []

        class FakeRuntime:
            """Promise first, then produce tool-confirmed UI progress after correction."""

            process = object()
            closing = False

            async def start_thread(self, model, tools):
                return "thread-1"

            async def chat(self, thread_id, message, model, effort):
                calls.append(message)
                if len(calls) == 1:
                    return "Ich öffne jetzt Results und erstelle danach den Vorschlag."
                conversation.ui_actions.append({
                    "action_id": "action-1",
                    "type": "page.perform_action",
                    "target": {"page_key": "v8_optimize"},
                    "payload": {},
                })
                return "PBGui hat die Results-Aktion an den Browser gesendet."

        runtime = FakeRuntime()
        service.capabilities = FakeCapabilities()
        monkeypatch.setattr(service, "_codex_runtime", lambda owner_arg: runtime)
        monkeypatch.setattr(service, "_ensure_reaper", lambda: None)
        monkeypatch.setattr(service, "_close_idle_codex_runtimes", lambda: asyncio.sleep(0))

        result = await service.chat(
            owner,
            "chatgpt",
            "model",
            "Kannst du meinen Auftrag jetzt ausführen?",
            conversation.id,
        )

        assert len(calls) == 2
        assert calls[0].startswith("Kannst du meinen Auftrag jetzt ausführen?")
        assert "PBGui action-enforcement continuation" in calls[1]
        assert result["reply"] == "PBGui hat die Results-Aktion an den Browser gesendet."
        assert result["proposals"] == []
        await service.shutdown()

    asyncio.run(scenario())


def test_idle_codex_runtime_keeps_persistent_conversations(
    tmp_path: Path,
) -> None:
    """Closing an idle app-server must detach threads without hiding chat history."""
    async def scenario() -> None:
        owner = "a" * 32
        service = AIChatService(tmp_path / "ai")
        conversation = await service._conversation(owner, "chatgpt", "model", None)
        conversation.messages = [{"role": "assistant", "content": "Retained answer"}]

        class FakeRuntime:
            """Idle runtime whose process can be closed deterministically."""

            last_used = 0.0
            active_turn_id = None
            closing = False
            closed = False

            async def close(self):
                self.closed = True

        runtime = FakeRuntime()
        conversation.codex_thread_id = "thread-1"
        conversation.codex_runtime = runtime
        service.codex[owner] = runtime

        await service._close_idle_codex_runtimes()
        snapshot = await service.get_conversation(owner, conversation.id)

        assert runtime.closed is True
        assert conversation.id in service.conversations
        assert conversation.closed is False
        assert conversation.codex_thread_id is None
        assert conversation.codex_runtime is None
        assert snapshot["messages"] == [{"role": "assistant", "content": "Retained answer"}]
        await service.shutdown()

    asyncio.run(scenario())


def test_stopped_codex_runtime_keeps_failed_turn_retryable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A crashed app-server must leave the conversation visible with a retryable error."""
    async def scenario() -> None:
        owner = "a" * 32
        service = AIChatService(tmp_path / "ai")
        conversation = await service._conversation(owner, "chatgpt", "model", None)

        class FakeRuntime:
            """Runtime that stops while processing its existing thread."""

            process = object()
            closing = False

            async def chat(self, thread_id, message, model, effort):
                self.process = None
                self.closing = True
                raise AIChatError("ChatGPT runtime stopped")

        runtime = FakeRuntime()
        conversation.codex_thread_id = "thread-1"
        conversation.codex_runtime = runtime
        service.codex[owner] = runtime
        monkeypatch.setattr(service, "_ensure_reaper", lambda: None)
        monkeypatch.setattr(service, "_close_idle_codex_runtimes", lambda: asyncio.sleep(0))

        await service.start_turn(owner, conversation.id, "Retry me")
        await service.active_tasks[conversation.id]
        snapshot = await service.get_conversation(owner, conversation.id)

        assert conversation.id in service.conversations
        assert snapshot["busy"] is False
        assert snapshot["last_error"] == "ChatGPT runtime stopped"
        assert snapshot["retry_message"] == "Retry me"
        assert conversation.codex_thread_id is None
        assert conversation.codex_runtime is None
        await service.shutdown()

    asyncio.run(scenario())


def test_internal_approval_continuation_is_hidden_from_browser_history(
    tmp_path: Path, monkeypatch
) -> None:
    """Automatic workflow continuation should reach the model without impersonating the user."""
    async def scenario() -> None:
        owner = "a" * 32
        service = AIChatService(tmp_path / "ai")
        service.credentials.save_go_key(owner, "sk-test-0123456789abcdef")
        conversation = await service._conversation(owner, "opencode-go", "model", None)
        conversation.messages = [
            {"role": "user", "content": "Create two proposals", "display_content": "Create two proposals"},
            {"role": "assistant", "content": "Approve the first proposal."},
        ]
        captured = []

        async def fake_go_chat(owner_arg, model, messages, provider, conversation_id, effort):
            captured.extend(messages)
            return "Second proposal prepared."

        monkeypatch.setattr(service, "_go_chat", fake_go_chat)
        await service.start_turn(
            owner,
            conversation.id,
            "Approved action completed. Continue the workflow.",
            internal=True,
        )
        await service.active_tasks[conversation.id]
        snapshot = await service.get_conversation(owner, conversation.id)

        assert captured[-1]["content"].startswith("Approved action completed")
        assert snapshot["messages"] == [
            {"role": "user", "content": "Create two proposals"},
            {"role": "assistant", "content": "Approve the first proposal."},
            {"role": "assistant", "content": "Second proposal prepared."},
        ]
        await service.shutdown()

    asyncio.run(scenario())


def test_detached_turn_can_change_effort_without_losing_history(tmp_path: Path, monkeypatch) -> None:
    """Reasoning effort is a per-turn option and must not fork the conversation."""
    async def scenario() -> None:
        owner = "a" * 32
        service = AIChatService(tmp_path / "ai")
        service.credentials.save_go_key(owner, "sk-test-0123456789abcdef")
        conversation = await service._conversation(owner, "opencode-go", "model", None)
        conversation.messages = [
            {"role": "user", "content": "Earlier", "display_content": "Earlier"},
            {"role": "assistant", "content": "Earlier answer"},
        ]

        async def fake_model(owner_arg, provider, model):
            return {"id": model, "reasoning_variants": [{"id": "xhigh"}]}

        async def fake_go_chat(*args):
            return "xhigh answer"

        monkeypatch.setattr(service, "_validate_provider_model", fake_model)
        monkeypatch.setattr(service, "_go_chat", fake_go_chat)
        await service.start_turn(owner, conversation.id, "Continue", effort="xhigh")
        await service.active_tasks[conversation.id]
        snapshot = await service.get_conversation(owner, conversation.id)

        assert snapshot["conversation_id"] == conversation.id
        assert snapshot["effort"] == "xhigh"
        assert [item["content"] for item in snapshot["messages"]] == [
            "Earlier",
            "Earlier answer",
            "Continue",
            "xhigh answer",
        ]
        await service.shutdown()

    asyncio.run(scenario())


def test_detached_turn_can_change_model_without_losing_history(tmp_path: Path, monkeypatch) -> None:
    """Model selection is per-turn and must retain the persistent conversation."""
    async def scenario() -> None:
        owner = "a" * 32
        service = AIChatService(tmp_path / "ai")
        service.credentials.save_go_key(owner, "sk-test-0123456789abcdef")
        conversation = await service._conversation(owner, "opencode-go", "model-a", None)
        conversation.messages = [
            {"role": "user", "content": "Earlier", "display_content": "Earlier"},
            {"role": "assistant", "content": "Earlier answer"},
        ]

        async def fake_model(owner_arg, provider, model):
            return {"id": model, "reasoning_variants": []}

        async def fake_go_chat(*args):
            return "model-b answer"

        monkeypatch.setattr(service, "_validate_provider_model", fake_model)
        monkeypatch.setattr(service, "_go_chat", fake_go_chat)
        await service.start_turn(owner, conversation.id, "Continue", effort="", model="model-b")
        await service.active_tasks[conversation.id]
        snapshot = await service.get_conversation(owner, conversation.id)

        assert snapshot["conversation_id"] == conversation.id
        assert snapshot["model"] == "model-b"
        assert len(snapshot["messages"]) == 4
        await service.shutdown()

    asyncio.run(scenario())


def test_detached_turn_can_switch_providers_without_losing_history(tmp_path: Path, monkeypatch) -> None:
    """Provider selection is per-turn and hands prior history to a new ChatGPT thread."""
    async def scenario() -> None:
        owner = "a" * 32
        service = AIChatService(tmp_path / "ai")
        service.credentials.save_go_key(owner, "sk-test-0123456789abcdef")
        conversation = await service._conversation(owner, "opencode-go", "model-a", None)
        conversation.messages = [
            {"role": "user", "content": "Earlier question", "display_content": "Earlier question"},
            {"role": "assistant", "content": "Earlier answer"},
        ]
        captured = {}

        class FakeRuntime:
            """Minimal stateful ChatGPT runtime used by the provider-switch contract."""

            process = object()

            async def start_thread(self, model, tools):
                captured["thread_model"] = model
                return "thread-1"

            async def chat(self, thread_id, message, model, effort):
                captured["chatgpt_message"] = message
                return "ChatGPT answer"

            async def unsubscribe(self, thread_id):
                captured["released"] = thread_id
                return True

        runtime = FakeRuntime()

        async def fake_model(owner_arg, provider, model):
            return {"id": model, "reasoning_variants": []}

        async def fake_go_chat(owner_arg, model, messages, provider, conversation_id, effort):
            captured["go_history"] = list(messages)
            return "OpenCode answer"

        monkeypatch.setattr(service, "_validate_provider_model", fake_model)
        monkeypatch.setattr(service, "_codex_runtime", lambda owner_arg: runtime)
        monkeypatch.setattr(service, "_ensure_reaper", lambda: None)
        monkeypatch.setattr(service, "_close_idle_codex_runtimes", lambda: asyncio.sleep(0))
        monkeypatch.setattr(service, "_go_chat", fake_go_chat)

        await service.start_turn(
            owner, conversation.id, "Continue with ChatGPT", effort="", model="model-b", provider="chatgpt"
        )
        await service.active_tasks[conversation.id]
        after_chatgpt = await service.get_conversation(owner, conversation.id)

        assert after_chatgpt["conversation_id"] == conversation.id
        assert after_chatgpt["provider"] == "chatgpt"
        assert "Earlier question" in captured["chatgpt_message"]
        assert "Earlier answer" in captured["chatgpt_message"]

        await service.start_turn(
            owner, conversation.id, "Continue with OpenCode", effort="", model="model-c", provider="opencode-go"
        )
        await service.active_tasks[conversation.id]
        final = await service.get_conversation(owner, conversation.id)

        assert captured["released"] == "thread-1"
        assert final["conversation_id"] == conversation.id
        assert final["provider"] == "opencode-go"
        assert [item["content"] for item in final["messages"]] == [
            "Earlier question",
            "Earlier answer",
            "Continue with ChatGPT",
            "ChatGPT answer",
            "Continue with OpenCode",
            "OpenCode answer",
        ]
        assert len(captured["go_history"]) == 5
        await service.shutdown()

    asyncio.run(scenario())


def test_rewind_removes_selected_turn_and_restores_prompt(tmp_path: Path) -> None:
    """Chat rewind should persistently remove a user turn and everything after it."""
    async def scenario() -> None:
        owner = "a" * 32
        service = AIChatService(tmp_path / "ai")
        conversation = await service._conversation(owner, "opencode-go", "model", None)
        conversation.messages = [
            {"role": "user", "content": "first", "display_content": "first"},
            {"role": "assistant", "content": "first answer"},
            {"role": "user", "content": "second hidden", "display_content": "second"},
            {"role": "assistant", "content": "second answer"},
        ]
        service._persist_conversation(conversation)

        result = await service.rewind_conversation(owner, conversation.id, 2)

        assert result["restored_prompt"] == "second"
        assert [item["content"] for item in result["messages"]] == ["first", "first answer"]
        reloaded = AIChatService(tmp_path / "ai")
        assert len((await reloaded.get_conversation(owner, conversation.id))["messages"]) == 2
        await reloaded.shutdown()
        await service.shutdown()

    asyncio.run(scenario())


def test_failed_detached_turn_persists_prompt_and_retry_after_reload(tmp_path: Path, monkeypatch) -> None:
    """Provider rejection must not remove the submitted question from durable history."""
    async def scenario() -> None:
        owner = "a" * 32
        service = AIChatService(tmp_path / "ai")
        service.credentials.save_go_key(owner, "sk-test-0123456789abcdef")
        conversation = await service._conversation(owner, "opencode-go", "model", None)

        async def reject(*args):
            raise AIChatError("AI provider rejected the selected model request")

        monkeypatch.setattr(service, "_go_chat", reject)
        await service.start_turn(owner, conversation.id, "Why did this fail?")
        await service.active_tasks[conversation.id]
        snapshot = await service.get_conversation(owner, conversation.id)

        assert snapshot["messages"][-1] == {
            "role": "user",
            "content": "Why did this fail?",
            "failed": True,
        }
        assert snapshot["retry_message"] == "Why did this fail?"

        observed = {}

        async def succeed(owner_arg, model, messages, provider, conversation_id, effort):
            observed["messages"] = messages
            return "Recovered"

        monkeypatch.setattr(service, "_go_chat", succeed)
        await service.start_turn(owner, conversation.id, "Try a different question")
        await service.active_tasks[conversation.id]

        assert [item.get("content") for item in observed["messages"]] == [
            "Try a different question"
        ]
        assert all("display_content" not in item and "failed" not in item for item in observed["messages"])
        recovered = await service.get_conversation(owner, conversation.id)
        assert recovered["messages"][0]["failed"] is True
        assert recovered["messages"][-1] == {"role": "assistant", "content": "Recovered"}
        await service.shutdown()

    asyncio.run(scenario())


def test_page_context_is_bounded_and_marked_untrusted() -> None:
    """Page context should improve grounding without becoming instructions or authorization."""
    context = AIChatService._validate_page_context(
        {
            "schema_version": 1,
            "page_key": "v8_optimize",
            "guide_topic": "43_pbv8_optimize",
            "section": "Scoring",
            "pages": [
                {"key": "/", "title": "Welcome"},
                {"key": "v7_backtest", "title": "Backtest"},
                {"key": "v8_optimize", "title": "Optimize"},
            ],
            "entities": [{"kind": "optimizer_config", "version": "v8", "name": "demo"}],
            "evidence": [
                {
                    "kind": "log_excerpt",
                    "title": "Visible Passivbot output",
                    "content": (
                        "INFO healthy\nAuthorization: Bearer must-not-leak\n"
                        "token=must-not-leak either\n"
                        'settings={"api_key": "quoted-must-not-leak"}\n'
                        "metadata={'token': 'al\"so-must-not-leak'}\n"
                        "json={\"password\":\"pa'ssword\"}\n"
                        r'escaped={"api_key":"abc\"LEAK"}' + "\n"
                        'truncated={"password":"unterminated\nSECRET-BODY\nEND'
                    ),
                }
            ],
            "actions": [{"id": "show_log", "entity_kind": "optimizer_queue_item"}],
            "controls": [
                {
                    "id": "control_1",
                    "role": "button",
                    "name": "Optimize log :: Close",
                    "label": "Close",
                    "context": "Optimize log",
                    "operations": ["activate"],
                    "options": [],
                }
            ],
        }
    )

    suffix = AIChatService._context_prompt_suffix(context)

    assert "Untrusted PBGui page context" in suffix
    assert "identifiers and evidence" in suffix
    assert '"guide_topic":"43_pbv8_optimize"' in suffix
    assert context["pages"][0] == {"key": "/", "title": "Welcome"}
    assert context["actions"] == [{"id": "show_log", "entity_kind": "optimizer_queue_item"}]
    assert context["controls"][0]["label"] == "Close"
    assert context["evidence"] == [
        {
            "kind": "log_excerpt",
            "title": "Visible Passivbot output",
            "content": (
                "INFO healthy\nAuthorization: [REDACTED]\n"
                "token=[REDACTED] either\n"
                'settings={"api_key": "[REDACTED]"}\n'
                "metadata={'token': '[REDACTED]'}\n"
                'json={"password":"[REDACTED]"}\n'
                'escaped={"api_key":"[REDACTED]"}\n'
                'truncated={"password":"[REDACTED]'
            ),
        }
    ]
    with pytest.raises(AIChatError, match="Invalid page context"):
        AIChatService._validate_page_context({"secret": "value"})
    with pytest.raises(AIChatError, match="Invalid page context"):
        AIChatService._validate_page_context({"pages": [{"key": "/evil", "title": "Invalid"}]})


def test_new_user_turn_supersedes_pending_proposals(tmp_path: Path, monkeypatch) -> None:
    """A later user request must invalidate approval cards from the earlier branch."""
    async def scenario() -> None:
        owner = "a" * 32
        service = AIChatService(tmp_path / "ai")
        service.credentials.save_go_key(owner, "sk-test-0123456789abcdef")
        conversation = await service._conversation(owner, "opencode-go", "model", None)
        rejected = []

        async def reject_conversation(owner_arg, conversation_id_arg):
            rejected.append((owner_arg, conversation_id_arg))

        async def fake_go_chat(*_args, **_kwargs):
            return "Current answer"

        monkeypatch.setattr(service.capabilities, "reject_conversation", reject_conversation)
        monkeypatch.setattr(service, "_go_chat", fake_go_chat)

        await service.start_turn(owner, conversation.id, "A newer question")
        await service.active_tasks[conversation.id]

        assert rejected == [(owner, conversation.id)]
        await service.shutdown()

    asyncio.run(scenario())


def test_failed_internal_continuation_preserves_successful_action_status(
    tmp_path: Path, monkeypatch
) -> None:
    """A provider timeout after approval must remain a successful action result."""
    async def scenario() -> None:
        owner = "a" * 32
        service = AIChatService(tmp_path / "ai")
        service.credentials.save_go_key(owner, "sk-test-0123456789abcdef")
        conversation = await service._conversation(owner, "opencode-go", "model", None)

        async def failing_go_chat(*_args, **_kwargs):
            raise AIChatError("ChatGPT response timed out")

        monkeypatch.setattr(service, "_go_chat", failing_go_chat)
        await service.start_turn(
            owner,
            conversation.id,
            "Approved action completed. Continue.",
            internal=True,
        )
        await service.active_tasks[conversation.id]
        snapshot = await service.get_conversation(owner, conversation.id)

        assert snapshot["last_error"] == ""
        assert snapshot["messages"] == [{
            "role": "assistant",
            "content": (
                "The approved PBGui action completed successfully. Its optional AI follow-up did not complete: "
                "ChatGPT response timed out. No approved action was rolled back."
            ),
        }]
        await service.shutdown()

    asyncio.run(scenario())


def test_internal_continuation_keeps_large_context_turn_visible(
    tmp_path: Path, monkeypatch
) -> None:
    """Approval follow-ups must not trim the visible turn just because its old context was large."""
    async def scenario() -> None:
        owner = "a" * 32
        service = AIChatService(tmp_path / "ai")
        service.credentials.save_go_key(owner, "sk-test-0123456789abcdef")
        conversation = await service._conversation(owner, "opencode-go", "model", None)
        conversation.messages = [
            {
                "role": "user",
                "content": "Visible question\n\n[Untrusted PBGui page context" + "x" * 100_000,
                "display_content": "Visible question",
            },
            {"role": "assistant", "content": "Visible proposal answer"},
        ]

        async def failing_go_chat(*_args, **_kwargs):
            raise AIChatError("provider timeout")

        monkeypatch.setattr(service, "_go_chat", failing_go_chat)
        await service.start_turn(
            owner,
            conversation.id,
            "Approved action completed. Continue.",
            internal=True,
        )
        await service.active_tasks[conversation.id]
        snapshot = await service.get_conversation(owner, conversation.id)

        assert snapshot["messages"][:2] == [
            {"role": "user", "content": "Visible question"},
            {"role": "assistant", "content": "Visible proposal answer"},
        ]
        assert "No approved action was rolled back" in snapshot["messages"][2]["content"]
        assert conversation.messages[0]["content"] == "Visible question"
        await service.shutdown()

    asyncio.run(scenario())


def test_ai_drawer_preferences_are_private_persistent_merged_and_bounded(tmp_path: Path) -> None:
    """Drawer width and open state should merge server-side without browser storage."""
    service = AIChatService(tmp_path / "ai")
    owner = "a" * 32

    assert service.get_preferences(owner) == {"drawer_width": 460, "drawer_open": False}
    assert service.save_preferences(owner, 612) == {"drawer_width": 612, "drawer_open": False}
    assert service.save_preferences(owner, drawer_open=True) == {"drawer_width": 612, "drawer_open": True}
    assert AIChatService(tmp_path / "ai").get_preferences(owner) == {"drawer_width": 612, "drawer_open": True}
    assert service.save_preferences(owner, 4000) == {"drawer_width": 4000, "drawer_open": True}
    assert service.save_preferences(owner, drawer_open=False) == {"drawer_width": 4000, "drawer_open": False}
    with pytest.raises(AIChatError, match="browser range"):
        service.save_preferences(owner, 100_001)
    with pytest.raises(AIChatError, match="No AI preferences"):
        service.save_preferences(owner)
    if os.name == "posix":
        assert (service.preference_root / f"{owner}.json").stat().st_mode & 0o777 == 0o600


def test_local_agent_failure_does_not_mark_provider_model_unhealthy(
    tmp_path: Path, monkeypatch
) -> None:
    """A PBGui loop failure after successful provider calls is not model-health evidence."""
    async def scenario() -> None:
        service = AIChatService(tmp_path / "ai")
        owner = "a" * 32
        service.credentials.save_go_key(owner, "sk-test-0123456789abcdef")
        conversation = await service._conversation(owner, "opencode-go", "mimo-v2.5", None)
        recorded = []

        async def fail_locally(*args):
            raise AIChatError("OpenCode agent could not produce a final answer")

        monkeypatch.setattr(service, "_go_chat", fail_locally)
        monkeypatch.setattr(service, "_record_model_health", lambda *args: recorded.append(args))

        with pytest.raises(AIChatError, match="could not produce"):
            await service.chat(
                owner,
                "opencode-go",
                "mimo-v2.5",
                "Explain this",
                conversation.id,
            )

        assert recorded == []
        await service.shutdown()

    asyncio.run(scenario())


def test_conversation_reasoning_effort_cannot_change_midstream(tmp_path: Path) -> None:
    """Provider context must not mix different reasoning settings in one conversation."""
    async def scenario() -> None:
        service = AIChatService(tmp_path / "ai")
        conversation = await service._conversation("a" * 32, "opencode-go", "model", None)
        conversation.effort = "high"

        with pytest.raises(AIChatError, match="reasoning effort cannot be changed"):
            await service.chat(
                "a" * 32,
                "opencode-go",
                "model",
                "Hello",
                conversation.id,
                "low",
            )
        await service.shutdown()

    asyncio.run(scenario())


def test_message_validation_blocks_empty_oversized_and_nul_text(tmp_path: Path) -> None:
    """Prompts should remain bounded before entering provider context."""
    service = AIChatService(tmp_path / "ai")

    for value in ("", "x" * 12_001, "bad\x00message"):
        with pytest.raises(AIChatError):
            service._validate_message(value)


def test_action_requests_receive_extended_bounded_capability_rounds() -> None:
    """Config mutations need enough rounds to read, validate, correct, and propose."""
    assert AIChatService._capability_round_limit(
        [{"role": "user", "content": "Passe die aktuelle Config an und speichere sie nach Freigabe"}]
    ) == 10
    assert AIChatService._capability_round_limit(
        [{"role": "user", "content": "Markiere mir die drei stabilsten Pareto-Kandidaten"}]
    ) == 10
    assert AIChatService._capability_round_limit(
        [{"role": "user", "content": "Queue die beiden PB8 Jobs und starte sie direkt"}]
    ) == 10
    assert AIChatService._capability_round_limit(
        [{"role": "user", "content": "Erkläre mir diese Metrik"}]
    ) == 3
    assert "do not repeat searches with minor query variations" in _go_instructions(
        "kimi-k3", tools_enabled=True
    )
    assert "Never propose workspace Python merely because a routine status question" in _go_instructions(
        "kimi-k3", tools_enabled=True
    )


def test_vague_comparison_setup_uses_local_clarification_without_provider(
    tmp_path: Path, monkeypatch
) -> None:
    """A vague comparison confirmation should produce choices without a slow model turn."""
    async def scenario() -> None:
        service = AIChatService(tmp_path / "ai")

        async def fake_models(provider):
            return [{
                "id": "grok-4.6",
                "protocol": "responses",
                "tools": True,
                "reasoning_variants": [],
            }]

        monkeypatch.setattr(service, "_go_models", fake_models)
        monkeypatch.setattr(
            service.credentials,
            "load_go_key",
            lambda owner: (_ for _ in ()).throw(AssertionError("provider must not be called")),
        )
        conversation = await service._conversation("a" * 32, "opencode-go", "grok-4.6", None)
        history = [
            {"role": "assistant", "content": "I can set up a fair comparison if you want."},
            {"role": "user", "content": "yes setup the compare"},
        ]

        reply = await service._go_chat(
            "a" * 32,
            "grok-4.6",
            history,
            "opencode-go",
            conversation.id,
            "",
        )
        snapshot = await service.get_conversation("a" * 32, conversation.id)

        assert reply == "Which comparison should PBGui set up? PB7 and PB8 remain separate runtimes."
        assert snapshot["ui_actions"][0]["type"] == "chat.quick_replies"
        choices = snapshot["ui_actions"][0]["payload"]["choices"]
        assert [item["label"] for item in choices] == [
            "PB7 trailing vs PB8 martingale",
            "PB8 martingale vs PB8 grid",
            "Custom comparison",
        ]
        assert "Do not substitute PB8 trailing_grid_v7" in choices[0]["value"]
        assert service._comparison_setup_clarification([
            {"role": "user", "content": "setup compare configs alpha and beta with TWEL 1.0"}
        ]) is None
        await service.shutdown()

    asyncio.run(scenario())


def test_cross_version_comparison_scope_asks_risk_without_provider(
    tmp_path: Path, monkeypatch
) -> None:
    """A real PB7/PB8 scope should retain both generations before risk alignment."""
    async def scenario() -> None:
        service = AIChatService(tmp_path / "ai")

        async def fake_models(provider):
            return [{"id": "grok-4.6", "protocol": "responses", "tools": True, "reasoning_variants": []}]

        monkeypatch.setattr(service, "_go_models", fake_models)
        monkeypatch.setattr(
            service.credentials,
            "load_go_key",
            lambda owner: (_ for _ in ()).throw(AssertionError("provider must not be called")),
        )
        conversation = await service._conversation("a" * 32, "opencode-go", "grok-4.6", None)

        reply = await service._go_chat(
            "a" * 32,
            "grok-4.6",
            [{"role": "user", "content": _COMPARE_PB7_TRAILING_VS_PB8_MARTINGALE}],
            "opencode-go",
            conversation.id,
            "",
        )
        snapshot = await service.get_conversation("a" * 32, conversation.id)

        assert reply.startswith("For the real PB7 trailing vs PB8 trailing_martingale comparison")
        assert [item["label"] for item in snapshot["ui_actions"][0]["payload"]["choices"]] == [
            "Keep source risk",
            "Normalize risk",
            "Custom values",
        ]
        await service.shutdown()

    asyncio.run(scenario())


def test_cross_version_risk_choice_lists_real_pb7_sources_without_provider(
    tmp_path: Path, monkeypatch
) -> None:
    """A risk choice should list real PB7 sources rather than PB8 compatibility configs."""
    async def scenario() -> None:
        service = AIChatService(tmp_path / "ai")

        async def fake_models(provider):
            return [{
                "id": "grok-4.6",
                "protocol": "responses",
                "tools": True,
                "reasoning_variants": [],
            }]

        monkeypatch.setattr(service, "_go_models", fake_models)
        monkeypatch.setattr(
            service.credentials,
            "load_go_key",
            lambda owner: (_ for _ in ()).throw(AssertionError("provider must not be called")),
        )
        monkeypatch.setattr(
            service.capabilities,
            "_list_optimizer_configs",
            lambda args: {
                "version": "v7",
                "configs": [{"name": "HYPE_v7"}, {"name": "HYPE_v7_safe"}],
                "returned": 2,
            },
        )
        conversation = await service._conversation("a" * 32, "opencode-go", "grok-4.6", None)
        history = [
            {"role": "user", "content": _COMPARE_PB7_TRAILING_VS_PB8_MARTINGALE},
            {"role": "assistant", "content": "How should risk align?"},
            {"role": "user", "content": _COMPARE_KEEP_SOURCE_RISK},
        ]

        reply = await service._go_chat(
            "a" * 32,
            "grok-4.6",
            history,
            "opencode-go",
            conversation.id,
            "",
        )
        snapshot = await service.get_conversation("a" * 32, conversation.id)

        assert reply == "Which PB7 optimizer config should PBGui use as the real V7 trailing source?"
        choices = snapshot["ui_actions"][0]["payload"]["choices"]
        assert [item["label"] for item in choices] == [
            "HYPE_v7",
            "HYPE_v7_safe",
            "Choose another config",
        ]
        assert "real PB7 trailing source" in choices[0]["value"]
        assert "never convert it to PB8 trailing_grid_v7" in choices[0]["value"]
        assert "PB7 mutation, queueing, and starting remain manual" in choices[0]["value"]
        await service.shutdown()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("value", "expected"),
    [("", ""), ("LOW", "LOW"), ("xhigh", "xhigh"), ("focused", "focused")],
)
def test_reasoning_effort_validation_preserves_provider_values(
    tmp_path: Path, value: str, expected: str
) -> None:
    """Provider-defined reasoning names should remain exact but bounded."""
    service = AIChatService(tmp_path / "ai")

    assert service._validate_effort(value) == expected
    with pytest.raises(AIChatError, match="Unsupported reasoning variant"):
        service._validate_effort("bad\nvariant")


def test_model_effort_validation_requires_exact_advertised_variant() -> None:
    """An arbitrary bounded string must not bypass per-model variant metadata."""
    model = {"reasoning_variants": [{"id": "minimal"}, {"id": "ultra"}]}

    AIChatService._validate_model_effort(model, "ultra")
    with pytest.raises(AIChatError, match="does not support"):
        AIChatService._validate_model_effort(model, "medium")


def test_codex_thread_disables_local_and_remote_tools(tmp_path: Path, monkeypatch) -> None:
    """ChatGPT MVP threads must disable tool families before contacting a model."""
    from ai_chat import CodexRuntime

    async def scenario() -> None:
        runtime = CodexRuntime("a" * 32, tmp_path / "codex")
        captured = {}

        async def fake_request(method, params=None, timeout=30):
            captured["method"] = method
            captured["params"] = params
            return {"thread": {"id": "thread-1"}}

        monkeypatch.setattr(runtime, "request", fake_request)
        thread_id = await runtime.start_thread("gpt-test")
        features = captured["params"]["config"]["features"]

        assert thread_id == "thread-1"
        assert captured["method"] == "thread/start"
        assert captured["params"]["sandbox"] == "read-only"
        assert captured["params"]["approvalPolicy"] == "never"
        assert features["shell_tool"] is False
        assert features["unified_exec"] is False
        assert features["standalone_web_search"] is False
        assert features["multi_agent"] is False
        assert features["browser_use"] is False
        assert features["computer_use"] is False
        assert features["plugins"] is False
        assert features["skill_search"] is False
        assert features["tool_suggest"] is False
        assert captured["params"]["config"]["mcp_servers"] == {}
        assert captured["params"]["config"]["web_search"] == "disabled"
        assert captured["params"]["config"]["apps"]["_default"]["enabled"] is False
        assert "gpt-test" in captured["params"]["developerInstructions"]
        assert "# PBGui AI Agent" in captured["params"]["developerInstructions"]
        assert "Mutation tools create proposals only" in captured["params"]["developerInstructions"]
        assert "A text-only JSON suggestion is not a valid completion" in captured["params"]["developerInstructions"]


def test_codex_thread_advertises_only_pbgui_dynamic_tools(tmp_path: Path, monkeypatch) -> None:
    """Productive Codex threads should receive the PBGui namespace and no environment tools."""
    from ai_chat import CodexRuntime

    async def scenario() -> None:
        runtime = CodexRuntime("a" * 32, tmp_path / "codex")
        captured = {}

        async def fake_request(method, params=None, timeout=30):
            captured.update(params)
            return {"thread": {"id": "thread-1"}}

        monkeypatch.setattr(runtime, "request", fake_request)
        dynamic = [{"type": "namespace", "name": "pbgui", "description": "PBGui", "tools": []}]
        await runtime.start_thread("gpt-test", dynamic)

        assert captured["environments"] == []
        assert captured["dynamicTools"] == dynamic
        assert captured["config"]["web_search"] == "disabled"

    asyncio.run(scenario())

    asyncio.run(scenario())


def test_codex_models_preserve_dynamic_reasoning_variants(tmp_path: Path, monkeypatch) -> None:
    """Codex model choices and custom names should pass through in server order."""
    from ai_chat import CodexRuntime

    async def scenario() -> None:
        runtime = CodexRuntime("a" * 32, tmp_path / "codex")

        async def fake_request(method, params=None, timeout=30):
            assert method == "model/list"
            return {
                "data": [
                    {
                        "model": "gpt-test",
                        "displayName": "GPT Test",
                        "supportedReasoningEfforts": [
                            {"reasoningEffort": "minimal", "description": "Quick"},
                            {"reasoningEffort": "ultra", "description": "Deep"},
                            {"reasoningEffort": "focused", "description": "Custom"},
                        ],
                        "defaultReasoningEffort": "ultra",
                    }
                ]
            }

        monkeypatch.setattr(runtime, "request", fake_request)
        models = await runtime.models()

        assert [item["id"] for item in models[0]["reasoning_variants"]] == [
            "minimal",
            "ultra",
            "focused",
        ]
        assert models[0]["reasoning_variants"][2]["description"] == "Custom"
        assert models[0]["default_effort"] == "ultra"

    asyncio.run(scenario())


def test_codex_turn_sends_exact_selected_effort(tmp_path: Path, monkeypatch) -> None:
    """Codex turn/start should receive the exact model-advertised effort string."""
    from ai_chat import CodexRuntime

    async def scenario() -> None:
        runtime = CodexRuntime("a" * 32, tmp_path / "codex")
        captured = {}

        async def fake_request(method, params=None, timeout=30):
            captured.update(params)
            await runtime.notifications.put(
                {
                    "method": "item/agentMessage/delta",
                    "params": {"turnId": "turn-1", "delta": "answer"},
                }
            )
            await runtime.notifications.put(
                {
                    "method": "turn/completed",
                    "params": {"turn": {"id": "turn-1", "status": "completed"}},
                }
            )
            return {"turn": {"id": "turn-1"}}

        monkeypatch.setattr(runtime, "request", fake_request)
        reply = await runtime.chat("thread-1", "Hello", "gpt-test", "ultra")

        assert reply == "answer"
        assert captured["effort"] == "ultra"

    asyncio.run(scenario())


def test_codex_turn_timeout_returns_safe_ai_error(tmp_path: Path, monkeypatch) -> None:
    """The whole-turn deadline should not escape as an unexpected raw TimeoutError."""
    from ai_chat import CodexRuntime

    async def scenario() -> None:
        runtime = CodexRuntime("a" * 32, tmp_path / "codex")

        async def fake_request(method, params=None, timeout=30):
            return {"turn": {"id": "turn-1"}}

        async def fake_interrupt(thread_id, turn_id):
            return None

        monkeypatch.setattr(runtime, "request", fake_request)
        monkeypatch.setattr(runtime, "_interrupt_turn_and_wait", fake_interrupt)
        monkeypatch.setattr("ai_chat._CHAT_TIMEOUT_SECONDS", 0.001)
        monkeypatch.setattr("ai_chat._CODEX_HIGH_EFFORT_TIMEOUT_SECONDS", 0.001)

        with pytest.raises(AIChatError, match="ChatGPT response timed out"):
            await runtime.chat("thread-1", "Hello", "gpt-test", "high")

    asyncio.run(scenario())


def test_codex_tool_updates_activity_after_local_result(tmp_path: Path) -> None:
    """The UI must stop claiming that a completed documentation search is still running."""
    from ai_chat import CodexRuntime

    class FakeCapabilities:
        """Return one bounded documentation search result."""

        @staticmethod
        async def dispatch(owner, conversation_id, tool, arguments):
            return {"matches": [], "returned": 0}

    async def scenario() -> None:
        service = AIChatService(tmp_path / "ai")
        service.capabilities = FakeCapabilities()
        runtime = CodexRuntime("a" * 32, tmp_path / "codex")
        runtime.active_turn_id = "turn-1"
        conversation = await service._conversation("a" * 32, "chatgpt", "gpt-test", None)
        conversation.codex_thread_id = "thread-1"
        conversation.codex_runtime = runtime
        conversation.busy = True

        result = await service._handle_codex_tool(
            "a" * 32,
            runtime,
            {
                "namespace": "pbgui",
                "tool": "search_passivbot_docs",
                "threadId": "thread-1",
                "turnId": "turn-1",
                "arguments": {"version": "v8", "query": "ema"},
            },
        )

        assert result["success"] is True
        assert conversation.activity == "Documentation search complete; model is processing results"
        await service.shutdown()

    asyncio.run(scenario())


def test_codex_tool_budget_caches_repeats_and_stops_only_stalled_analysis(
    tmp_path: Path, monkeypatch
) -> None:
    """Codex should cache repeats while allowing unique calls beyond the soft budget."""
    from ai_chat import CodexRuntime

    class FakeCapabilities:
        """Count only capability calls that pass the Codex turn budget."""

        calls = []

        @classmethod
        async def dispatch(cls, owner, conversation_id, tool, arguments):
            cls.calls.append((tool, arguments))
            return {"tool": tool, "arguments": arguments}

    async def scenario() -> None:
        service = AIChatService(tmp_path / "ai")
        service.capabilities = FakeCapabilities()
        runtime = CodexRuntime("a" * 32, tmp_path / "codex")
        runtime.active_turn_id = "turn-1"
        conversation = await service._conversation("a" * 32, "chatgpt", "gpt-test", None)
        conversation.codex_thread_id = "thread-1"
        conversation.codex_runtime = runtime
        conversation.busy = True
        base = {
            "namespace": "pbgui",
            "tool": "rank_optimizer_run_candidates",
            "threadId": "thread-1",
            "turnId": "turn-1",
            "arguments": {"version": "v8", "resource": "run-1"},
        }

        monkeypatch.setattr("ai_chat._CODEX_TOOL_SOFT_LIMIT", 2)
        monkeypatch.setattr("ai_chat._CODEX_STALL_CALLS", 3)
        first = await service._handle_codex_tool("a" * 32, runtime, base)
        cached = await service._handle_codex_tool("a" * 32, runtime, base)
        compact_cached = await service._handle_codex_tool("a" * 32, runtime, base)
        unique = await service._handle_codex_tool(
            "a" * 32,
            runtime,
            {**base, "tool": "list_optimizer_runs", "arguments": {"version": "v8"}},
        )
        await service._handle_codex_tool("a" * 32, runtime, base)
        await service._handle_codex_tool("a" * 32, runtime, base)
        stalled = await service._handle_codex_tool("a" * 32, runtime, base)

        assert first["success"] is True
        assert cached == first
        assert compact_cached["success"] is True
        assert "result_already_loaded" in compact_cached["contentItems"][0]["text"]
        assert unique["success"] is True
        assert stalled["success"] is False
        assert "No new PBGui information" in stalled["contentItems"][0]["text"]
        assert len(FakeCapabilities.calls) == 2
        assert conversation.activity == "PBGui analysis stalled; model must answer from loaded results"
        await service.shutdown()

    asyncio.run(scenario())


def test_codex_tool_hard_limit_stops_unique_endless_calls(tmp_path: Path, monkeypatch) -> None:
    """Even continuously unique calls must stop at the high end-loop safety limit."""
    from ai_chat import CodexRuntime

    class FakeCapabilities:
        """Return a distinct result for each accepted call."""

        calls = []

        @classmethod
        async def dispatch(cls, owner, conversation_id, tool, arguments):
            cls.calls.append(arguments)
            return {"value": arguments["value"]}

    async def scenario() -> None:
        monkeypatch.setattr("ai_chat._CODEX_TOOL_HARD_LIMIT", 3)
        service = AIChatService(tmp_path / "ai")
        service.capabilities = FakeCapabilities()
        runtime = CodexRuntime("a" * 32, tmp_path / "codex")
        runtime.active_turn_id = "turn-1"
        conversation = await service._conversation("a" * 32, "chatgpt", "gpt-test", None)
        conversation.codex_thread_id = "thread-1"
        conversation.codex_runtime = runtime
        conversation.busy = True

        results = []
        for value in range(4):
            results.append(
                await service._handle_codex_tool(
                    "a" * 32,
                    runtime,
                    {
                        "namespace": "pbgui",
                        "tool": "get_optimizer_run_analysis",
                        "threadId": "thread-1",
                        "turnId": "turn-1",
                        "arguments": {"value": value},
                    },
                )
            )

        assert [item["success"] for item in results] == [True, True, True, False]
        assert "hard capability limit" in results[-1]["contentItems"][0]["text"]
        assert len(FakeCapabilities.calls) == 3
        await service.shutdown()

    asyncio.run(scenario())


def test_codex_soft_limit_detects_distinct_calls_with_identical_results(
    tmp_path: Path, monkeypatch
) -> None:
    """Different tool arguments must still count as stalled when result content never changes."""
    from ai_chat import CodexRuntime

    class FakeCapabilities:
        """Return the same bounded result for every distinct request."""

        @staticmethod
        async def dispatch(owner, conversation_id, tool, arguments):
            return {"matches": [], "returned": 0}

    async def scenario() -> None:
        monkeypatch.setattr("ai_chat._CODEX_TOOL_SOFT_LIMIT", 2)
        monkeypatch.setattr("ai_chat._CODEX_STALL_CALLS", 2)
        service = AIChatService(tmp_path / "ai")
        service.capabilities = FakeCapabilities()
        runtime = CodexRuntime("a" * 32, tmp_path / "codex")
        runtime.active_turn_id = "turn-1"
        conversation = await service._conversation("a" * 32, "chatgpt", "gpt-test", None)
        conversation.codex_thread_id = "thread-1"
        conversation.codex_runtime = runtime
        conversation.busy = True

        results = []
        for query in ("first", "second", "third"):
            results.append(
                await service._handle_codex_tool(
                    "a" * 32,
                    runtime,
                    {
                        "namespace": "pbgui",
                        "tool": "search_pbgui_help",
                        "threadId": "thread-1",
                        "turnId": "turn-1",
                        "arguments": {"query": query},
                    },
                )
            )

        assert [item["success"] for item in results] == [True, True, False]
        assert "no new result content" in results[-1]["contentItems"][0]["text"]
        await service.shutdown()

    asyncio.run(scenario())


def test_codex_parameterless_request_omits_params(tmp_path: Path, monkeypatch) -> None:
    """JSON-RPC methods such as account/logout must not receive an empty params object."""
    from ai_chat import CodexRuntime

    async def scenario() -> None:
        runtime = CodexRuntime("a" * 32, tmp_path / "codex")
        messages = []

        async def fake_write(message):
            messages.append(message)
            runtime.pending[message["id"]].set_result({})

        monkeypatch.setattr(runtime, "_write", fake_write)
        await runtime._request_started("account/logout", None)

        assert "params" not in messages[0]

    asyncio.run(scenario())


def test_codex_browser_login_uses_official_chatgpt_flow(tmp_path: Path, monkeypatch) -> None:
    """Browser login should request the regular ChatGPT OAuth flow, not device auth."""
    from ai_chat import CodexRuntime

    async def scenario() -> None:
        runtime = CodexRuntime("a" * 32, tmp_path / "codex")
        calls = []

        async def fake_request(method, params=None, timeout=30):
            calls.append((method, params, timeout))
            return {
                "type": "chatgpt",
                "loginId": "login-1",
                "authUrl": "https://auth.openai.com/test",
            }

        monkeypatch.setattr(runtime, "request", fake_request)
        result = await runtime.start_browser_login()

        assert result == {"auth_url": "https://auth.openai.com/test"}
        assert calls == [("account/login/start", {"type": "chatgpt"}, 30)]

    asyncio.run(scenario())


def test_history_trimming_preserves_complete_turns(tmp_path: Path) -> None:
    """History limits should remove complete oldest user/assistant pairs."""
    service = AIChatService(tmp_path / "ai")
    messages = []
    for index in range(14):
        messages.extend(
            [
                {"role": "user", "content": f"u{index}"},
                {"role": "assistant", "content": f"a{index}"},
            ]
        )

    service._trim_history(messages, 24)

    assert len(messages) == 24
    assert messages[0]["role"] == "user"
    assert messages[-1] == {"role": "assistant", "content": "a13"}


def test_history_trimming_supports_large_contexts_and_keeps_complete_turns(tmp_path: Path) -> None:
    """Long tool conversations should retain recent complete turns within the larger budget."""
    service = AIChatService(tmp_path / "ai")
    messages = []
    for index in range(4):
        messages.extend(
            [
                {"role": "user", "content": f"u{index}" + "x" * 99_998},
                {"role": "assistant", "content": f"a{index}" + "y" * 49_998},
            ]
        )

    service._trim_history(messages, 24)

    assert sum(len(item["content"]) for item in messages) <= _MAX_HISTORY_CHARS
    assert len(messages) == 6
    assert messages[0]["role"] == "user"
    assert messages[-1]["role"] == "assistant"
    assert _MAX_HISTORY_CHARS == 512_000
    assert _MAX_PROVIDER_HANDOFF_CHARS == 256_000


def test_overlapping_conversation_turn_is_rejected(tmp_path: Path) -> None:
    """Concurrent requests must not queue behind or revive one conversation."""
    async def scenario() -> None:
        service = AIChatService(tmp_path / "ai")
        conversation = await service._conversation("a" * 32, "opencode-go", "model", None)
        await service._reserve_conversation(conversation)

        with pytest.raises(AIChatError, match="busy"):
            await service._reserve_conversation(conversation)

        conversation.busy = False
        await service.shutdown()

    asyncio.run(scenario())


def test_status_does_not_start_codex_without_existing_auth(tmp_path: Path, monkeypatch) -> None:
    """Opening the page must not allocate a Codex process before login is requested."""
    async def scenario() -> None:
        service = AIChatService(tmp_path / "ai")
        monkeypatch.setattr("ai_chat.CodexRuntime.available", lambda: True)

        def fail_runtime(owner):
            raise AssertionError("status started Codex without an auth file")

        monkeypatch.setattr(service, "_codex_runtime", fail_runtime)
        result = await service.status("a" * 32)

        assert result["providers"]["chatgpt"] == {
            "available": True,
            "connected": False,
            "plan": "",
        }
        await service.shutdown()

    asyncio.run(scenario())


def test_codex_auth_detection_hardens_oauth_storage(tmp_path: Path) -> None:
    """ChatGPT auth state must be owner-only before PBGui treats it as configured."""
    service = AIChatService(tmp_path / "ai")
    owner = "a" * 32
    auth_path = service.root / "codex" / owner / "codex-home" / "auth.json"
    auth_path.parent.mkdir(parents=True)
    auth_path.write_text("{}", encoding="utf-8")
    if os.name == "posix":
        auth_path.chmod(0o644)
        auth_path.parent.chmod(0o755)

    assert service._codex_auth_exists(owner) is True
    if os.name == "posix":
        assert auth_path.stat().st_mode & 0o777 == 0o600
        assert auth_path.parent.stat().st_mode & 0o777 == 0o700


def test_health_refresh_and_disconnect_share_one_credential_boundary(
    tmp_path: Path, monkeypatch
) -> None:
    """Disconnect must wait for a probe and then remove queued and persisted health state."""
    async def scenario() -> None:
        owner = "a" * 32
        service = AIChatService(tmp_path / "ai")
        service.credentials.save_go_key(owner, "sk-test-0123456789abcdef")
        probe_started = asyncio.Event()
        release_probe = asyncio.Event()

        async def fake_models(provider):
            return [{"id": "free-model", "protocol": "chat", "free": True}]

        async def fake_probe(*args):
            probe_started.set()
            await release_probe.wait()

        async def fake_cancel(*args):
            return None

        monkeypatch.setattr(service, "_go_models", fake_models)
        monkeypatch.setattr(service, "_probe_opencode_model", fake_probe)
        monkeypatch.setattr(service, "_cancel_provider", fake_cancel)
        service.health_requested.add(owner)
        refresh = asyncio.create_task(service._refresh_free_model_health(owner))
        await probe_started.wait()
        disconnect = asyncio.create_task(service.disconnect_go(owner))
        await asyncio.sleep(0)

        assert service.credentials.configured(owner) is True
        assert disconnect.done() is False
        release_probe.set()
        await refresh
        await disconnect

        assert service.credentials.configured(owner) is False
        assert owner not in service.health_requested
        assert owner not in service.model_health
        assert not service._health_path(owner).exists()
        await service.shutdown()

    asyncio.run(scenario())
