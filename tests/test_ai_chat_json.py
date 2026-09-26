"""Offline regression checks for malformed legacy AI conversation text."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

from ai_chat import AIChatService, owner_key
from api.ai import _json


def test_conversation_list_and_messages_survive_legacy_surrogates(tmp_path: Path) -> None:
    """One malformed stored chat cannot break the complete AI history response."""
    service = AIChatService(tmp_path / "ai")
    owner = owner_key("alice")
    service.credentials.save_openrouter_key(owner, "openrouter-key-123456789")

    async def stored_chat():
        """Persist the same malformed title and message that reached the UI."""
        conversation_id = await service.create_conversation(owner, "openrouter", "typesafe/jev-1.13")
        conversation = service.conversations[conversation_id]
        conversation.title = "Old chat \ud83d"
        conversation.messages = [{"role": "user", "content": "Question \ud83d"}]
        service._persist_conversation(conversation)
        return conversation_id

    conversation_id = asyncio.run(stored_chat())
    reloaded = AIChatService(tmp_path / "ai")

    async def response_payloads():
        """Read the list and one detailed chat after a fresh service load."""
        summaries = await reloaded.list_conversations(owner)
        detail = await reloaded.get_conversation(owner, conversation_id)
        return summaries, detail

    summaries, detail = asyncio.run(response_payloads())
    list_response = _json({"conversations": summaries})
    detail_response = _json(detail)
    assert list_response.headers["Cache-Control"] == "no-store"
    assert json.loads(list_response.body)["conversations"][0]["title"] == "Old chat ?"
    assert json.loads(detail_response.body)["messages"][0]["content"] == "Question ?"
