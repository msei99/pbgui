"""Offline retention contracts for owner-scoped AI conversation history."""

from __future__ import annotations

import asyncio
import time
from pathlib import Path

from ai_chat import AIChatService, Conversation, owner_key
from ai_openrouter import JEV_MODEL


class _Capabilities:
    """Record cleanup without touching real proposal storage."""

    def __init__(self) -> None:
        self.rejected = []

    async def reject_conversation(self, owner, conversation_id):
        """Record one retired conversation."""
        self.rejected.append((owner, conversation_id))


def _stored_chat(service: AIChatService, owner: str, number: int, updated_at: float, *, busy: bool = False) -> Conversation:
    """Write one private synthetic chat below the Pytest temporary root."""
    conversation = Conversation(f"{number:032x}", owner, "openrouter", JEV_MODEL)
    conversation.updated_at = updated_at
    conversation.created_at = updated_at
    conversation.busy = busy
    if busy:
        conversation.active_turn_id = "turn-in-progress"
    service._persist_conversation(conversation)
    return conversation


def test_history_prunes_only_owner_chats_idle_for_thirty_days(tmp_path: Path) -> None:
    """Opening history deletes old idle chats and retains recent or busy chats."""
    service = AIChatService(tmp_path / "ai")
    service.capabilities = _Capabilities()
    alice, bob = owner_key("alice"), owner_key("bob")
    now = time.time()
    old = _stored_chat(service, alice, 1, now - 31 * 86400)
    recent = _stored_chat(service, alice, 2, now - 2 * 86400)
    recent.created_at = now - 40 * 86400
    service._persist_conversation(recent)
    busy = _stored_chat(service, alice, 3, now - 31 * 86400, busy=True)
    other_owner = _stored_chat(service, bob, 4, now - 31 * 86400)
    old_path = service._conversation_path(alice, old.id)
    busy_path = service._conversation_path(alice, busy.id)
    other_path = service._conversation_path(bob, other_owner.id)

    summaries = asyncio.run(service.list_conversations(alice))

    assert {item["conversation_id"] for item in summaries} == {recent.id, busy.id}
    assert not old_path.exists()
    assert busy_path.exists()
    assert other_path.exists()
    assert service.capabilities.rejected == [(alice, old.id)]


def test_new_chat_evicts_oldest_idle_at_one_hundred(tmp_path: Path) -> None:
    """The 101st chat replaces the oldest idle file without deleting a busy turn."""
    service = AIChatService(tmp_path / "ai")
    service.capabilities = _Capabilities()
    owner = owner_key("alice")
    service.credentials.save_openrouter_key(owner, "openrouter-key-123456789")
    now = time.time()
    oldest = _stored_chat(service, owner, 1, now - 100 * 60)
    second_oldest = _stored_chat(service, owner, 2, now - 99 * 60)
    for number in range(3, 101):
        _stored_chat(service, owner, number, now - (101 - number) * 60)

    async def create_with_busy_oldest():
        """Mark the oldest record as an active in-memory turn before creation."""
        await service._ensure_owner_loaded(owner)
        current = service.conversations[oldest.id]
        current.busy = True
        current.active_turn_id = "active-turn"
        service._persist_conversation(current)
        return await service.create_conversation(owner, "openrouter", JEV_MODEL)

    new_id = asyncio.run(create_with_busy_oldest())
    owner_root = service.conversation_root / owner
    assert len(list(owner_root.glob("*.json"))) == 100
    assert (owner_root / f"{oldest.id}.json").exists()
    assert not (owner_root / f"{second_oldest.id}.json").exists()
    assert (owner_root / f"{new_id}.json").exists()
    assert service.capabilities.rejected == [(owner, second_oldest.id)]
