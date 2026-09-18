"""Owner and subscription isolation for persistent ChatGPT profiles, entirely offline."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from ai_chat import AIChatError, AIChatService

OWNER = 'a' * 32
OTHER = 'b' * 32


def test_profile_registry_preserves_default_and_isolates_owners(tmp_path):
    """Legacy auth stays put; other users cannot select or rename the new profile."""
    service = AIChatService(tmp_path / 'ai')
    work = service.save_chatgpt_profile(OWNER, 'Work')
    default_runtime = service._codex_runtime(OWNER)
    work_runtime = service._codex_runtime(OWNER, work['id'])
    assert default_runtime is not work_runtime
    assert default_runtime.codex_home == tmp_path / 'ai/codex' / OWNER / 'codex-home'
    assert work_runtime.codex_home == tmp_path / 'ai/codex' / OWNER / 'profiles' / work['id'] / 'codex-home'
    assert service._codex_runtime(OWNER, work['id']) is work_runtime
    service.save_chatgpt_profile(OWNER, 'Office', work['id'])
    assert service.chatgpt_profiles(OWNER)[1]['name'] == 'Office'
    with pytest.raises(AIChatError):
        service._codex_runtime(OTHER, work['id'])
    with pytest.raises(AIChatError):
        service.save_chatgpt_profile(OTHER, 'Hijack', work['id'])
    assert service.chatgpt_profiles(OTHER) == [{'id': 'default', 'name': 'Default'}]
    assert service._profile_path(OWNER).stat().st_mode & 0o777 == 0o600
    asyncio.run(service.shutdown())


@pytest.mark.parametrize('profile', ['../x', '/tmp', '', 'A' * 32, 'a\x00b'])
def test_profile_identifiers_reject_traversal(tmp_path, profile):
    """Client identifiers cannot escape the owner's profile root."""
    service = AIChatService(tmp_path / 'ai')
    with pytest.raises(AIChatError):
        service._codex_runtime(OWNER, profile)


def test_concurrent_profile_creation_does_not_lose_entries(tmp_path):
    """Registry updates use the reentrant filesystem lock and atomic writes."""
    service = AIChatService(tmp_path / 'ai')
    with ThreadPoolExecutor(max_workers=4) as pool:
        rows = list(pool.map(lambda i: service.save_chatgpt_profile(OWNER, f'Profile {i}'), range(8)))
    assert {row['id'] for row in rows} <= {row['id'] for row in service.chatgpt_profiles(OWNER)}
    assert len(service.chatgpt_profiles(OWNER)) == 9


def test_profile_binding_survives_reload_and_removal_does_not_fallback(tmp_path):
    """Removing Work preserves Default and history, but never reroutes Work chats."""
    async def scenario():
        service = AIChatService(tmp_path / 'ai')
        work = service.save_chatgpt_profile(OWNER, 'Work')['id']
        default = await service._conversation(OWNER, 'chatgpt', 'test-model', None)
        conversation = await service._conversation(OWNER, 'chatgpt', 'test-model', None, profile=work)
        runtime = service._codex_runtime(OWNER, work)
        other_runtime = service._codex_runtime(OWNER)
        runtime.logout = AsyncMock()
        runtime.close = AsyncMock()
        await service.remove_chatgpt_profile(OWNER, work)
        runtime.logout.assert_awaited_once()
        runtime.close.assert_awaited_once()
        assert service.codex[OWNER] is other_runtime
        assert default.chatgpt_profile == 'default'
        assert (await service.get_conversation(OWNER, conversation.id))['chatgpt_profile'] == work
        with pytest.raises(AIChatError, match='removed'):
            service._profile_runtime(OWNER, work)
        await service.shutdown()
        restored = AIChatService(tmp_path / 'ai')
        assert (await restored.get_conversation(OWNER, conversation.id))['chatgpt_profile'] == work
        assert (await restored.get_conversation(OWNER, default.id))['chatgpt_profile'] == 'default'
        await restored.shutdown()
    asyncio.run(scenario())


def test_turns_use_only_the_conversation_profile(tmp_path, monkeypatch):
    """Even sequential chats use their own runtime, with no quota failover."""
    async def scenario():
        service = AIChatService(tmp_path / 'ai')
        profile = service.save_chatgpt_profile(OWNER, 'Work')['id']
        first = await service._conversation(OWNER, 'chatgpt', 'model', None)
        second = await service._conversation(OWNER, 'chatgpt', 'model', None, profile=profile)
        monkeypatch.setattr(service, '_ensure_reaper', lambda: None)
        monkeypatch.setattr(service, '_close_idle_codex_runtimes', AsyncMock())
        runtimes = [service._codex_runtime(OWNER), service._codex_runtime(OWNER, profile)]
        for i, runtime in enumerate(runtimes):
            runtime.process = SimpleNamespace(returncode=None)
            runtime.start_thread = AsyncMock(return_value=f'thread-{i}')
            runtime.chat = AsyncMock(return_value=f'answer-{i}')
            runtime.close = AsyncMock()
        await service.chat(OWNER, 'chatgpt', 'model', 'Hello', first.id)
        await service.chat(OWNER, 'chatgpt', 'model', 'Hello', second.id)
        assert first.messages[-1]['content'] == 'answer-0'
        assert second.messages[-1]['content'] == 'answer-1'
        runtimes[1].chat.side_effect = AIChatError('ChatGPT usage limit reached')
        with pytest.raises(AIChatError, match='usage limit'):
            await service.chat(OWNER, 'chatgpt', 'model', 'Again', second.id)
        assert runtimes[0].chat.await_count == 1
        await service.shutdown()
    asyncio.run(scenario())


def test_unconnected_profile_can_be_removed_without_starting_codex(tmp_path, monkeypatch):
    """Unused metadata can be removed even when no Codex executable is available."""
    async def scenario():
        service = AIChatService(tmp_path / 'ai')
        profile = service.save_chatgpt_profile(OWNER, 'Unused')['id']
        monkeypatch.setattr(service, '_profile_runtime', lambda *args: pytest.fail('Unexpected runtime startup'))
        await service.remove_chatgpt_profile(OWNER, profile)
        assert len(service.chatgpt_profiles(OWNER)) == 1
        await service.shutdown()
    asyncio.run(scenario())


def test_profile_api_routes_preserve_owner_and_selected_profile(tmp_path, monkeypatch):
    """Authenticated endpoint parameters never accept an alternate owner."""
    from api import ai as routes
    from ai_chat import owner_key
    from fastapi import HTTPException
    async def scenario():
        service = AIChatService(tmp_path / 'ai')
        monkeypatch.setattr(routes, 'get_ai_chat_service', lambda: service)
        session = SimpleNamespace(user_id='first-user')
        result = await routes.save_chatgpt_profile(routes.ChatGPTProfileRequest(name='Work'), session)
        import json
        profile = json.loads(result.body)['id']
        assert result.headers['cache-control'] == 'no-store'
        assert service.chatgpt_profiles(owner_key(session.user_id))[1]['id'] == profile
        with pytest.raises(HTTPException) as error:
            await routes.remove_chatgpt_profile(profile, SimpleNamespace(user_id='other-user'))
        assert error.value.status_code == 400
        await routes.remove_chatgpt_profile(profile, session)
        assert len(service.chatgpt_profiles(owner_key(session.user_id))) == 1
        await service.shutdown()
    asyncio.run(scenario())


def test_profile_limits_are_numeric_and_cached(tmp_path, monkeypatch):
    """Usage projection excludes raw provider metadata and avoids repeated reads."""
    from ai_chat import CodexRuntime
    async def scenario():
        runtime = CodexRuntime(OWNER, tmp_path / 'runtime')
        request = AsyncMock(return_value={'rateLimits': {'primary': {
            'usedPercent': 25, 'windowDurationMins': 300, 'resetsAt': 1789728000,
            'accessToken': 'never-return-this'}, 'secondary': {'usedPercent': float('inf')}}})
        monkeypatch.setattr(runtime, 'request', request)
        assert await runtime.profile_limits() == [{'usedPercent': 25, 'windowDurationMins': 300, 'resetsAt': 1789728000}]
        await runtime.profile_limits()
        request.assert_awaited_once_with('account/rateLimits/read', timeout=5)
    asyncio.run(scenario())
