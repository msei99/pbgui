"""Owner and subscription isolation for persistent ChatGPT profiles, entirely offline."""
import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from ai_chat import AIChatError, AIChatService

OWNER = 'a' * 32
OTHER = 'b' * 32


@pytest.mark.parametrize('name', ['Default', ' Default '])
def test_implicit_default_name_collision_does_not_create_registry(tmp_path, name):
    """The synthetic Default profile reserves its name before any registry exists."""
    service = AIChatService(tmp_path / 'ai')
    with pytest.raises(AIChatError, match='already exists'):
        service.save_chatgpt_profile(OWNER, name)
    assert not service._profile_path(OWNER).exists()


@pytest.mark.parametrize('rename', [False, True])
def test_profile_name_collisions_leave_registry_unchanged(tmp_path, rename):
    """Trimmed labels collide on create and rename without changing persisted bytes."""
    service = AIChatService(tmp_path / 'ai')
    service.save_chatgpt_profile(OWNER, 'Trading')
    other = service.save_chatgpt_profile(OWNER, 'Other')
    path = service._profile_path(OWNER)
    before = path.read_bytes()
    with pytest.raises(AIChatError) as error:
        service.save_chatgpt_profile(OWNER, ' Trading ', other['id'] if rename else None)
    assert str(error.value) == 'A profile named "Trading" already exists. Choose a different name.'
    assert path.read_bytes() == before
    assert service.save_chatgpt_profile(OWNER, 'trading')['name'] == 'trading'
    assert service.save_chatgpt_profile(OTHER, 'Trading')['name'] == 'Trading'


@pytest.mark.parametrize('rename', [False, True])
def test_concurrent_profile_name_collisions_are_serialized(tmp_path, rename):
    """Concurrent same-name writes have exactly one winner under the file lock."""
    service = AIChatService(tmp_path / 'ai')
    profiles = [service.save_chatgpt_profile(OWNER, f'Profile {i}')['id'] for i in range(4)] if rename else [None] * 4
    barrier = Barrier(4)

    def save(profile):
        """Start contenders together and retain expected collision failures."""
        barrier.wait(timeout=10)
        try:
            return service.save_chatgpt_profile(OWNER, 'Trading', profile)
        except AIChatError as exc:
            return str(exc)

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(save, profiles))
    assert sum(isinstance(result, dict) for result in results) == 1
    assert results.count('A profile named "Trading" already exists. Choose a different name.') == 3
    rows = service.chatgpt_profiles(OWNER)
    assert sum(row['name'] == 'Trading' for row in rows) == 1
    assert len(rows) == (5 if rename else 2)


def test_legacy_duplicate_names_load_noop_rename_and_remove(tmp_path, monkeypatch):
    """Old duplicates retain IDs, permit unchanged renames without writes and delete normally."""
    from secure_files import atomic_write_private_text

    async def scenario():
        """Exercise only metadata and unused temporary profile directories."""
        service = AIChatService(tmp_path / 'ai')
        rows = [{'id': 'default', 'name': 'Default'},
                {'id': 'c' * 32, 'name': 'Trading'}, {'id': 'd' * 32, 'name': 'Trading'}]
        path = service._profile_path(OWNER)
        atomic_write_private_text(path, json.dumps(rows, indent=4) + '\n')
        before = path.read_bytes()
        assert service.chatgpt_profiles(OWNER) == rows
        with monkeypatch.context() as patch:
            write = Mock(side_effect=AssertionError('Unexpected write'))
            patch.setattr('ai_chat.atomic_write_private_text', write)
            assert service.save_chatgpt_profile(OWNER, ' Trading ', rows[1]['id']) == rows[1]
            write.assert_not_called()
        assert path.read_bytes() == before
        service.save_chatgpt_profile(OWNER, 'Office', rows[1]['id'])
        await service.remove_chatgpt_profile(OWNER, rows[2]['id'])
        assert service.chatgpt_profiles(OWNER) == [rows[0], {'id': rows[1]['id'], 'name': 'Office'}]
        await service.shutdown()

    asyncio.run(scenario())


@pytest.mark.parametrize('foreign', [False, True])
def test_profile_ownership_error_precedes_name_collision(tmp_path, foreign):
    """Unknown or foreign rename targets keep the existing ownership error."""
    service = AIChatService(tmp_path / 'ai')
    profile = service.save_chatgpt_profile(OTHER, 'Elsewhere')['id'] if foreign else 'c' * 32
    with pytest.raises(AIChatError, match='removed or does not belong'):
        service.save_chatgpt_profile(OWNER, 'Default', profile)
    assert not service._profile_path(OWNER).exists()


def test_profile_name_validation_preserves_twenty_profile_limit(tmp_path):
    """Unique creates still stop at twenty while existing profiles can be renamed."""
    service = AIChatService(tmp_path / 'ai')
    for i in range(19):
        service.save_chatgpt_profile(OWNER, f'Profile {i}')
    path = service._profile_path(OWNER)
    before = path.read_bytes()
    with pytest.raises(AIChatError, match='already exists'):
        service.save_chatgpt_profile(OWNER, 'Profile 0')
    with pytest.raises(AIChatError, match='limit reached'):
        service.save_chatgpt_profile(OWNER, 'New profile')
    assert path.read_bytes() == before
    service.save_chatgpt_profile(OWNER, 'Personal', 'default')
    assert len(service.chatgpt_profiles(OWNER)) == 20


def test_profile_collisions_over_http_preserve_status_inventory(tmp_path, monkeypatch):
    """Real authenticated routes reject create/rename collisions and expose one new ID."""
    from fastapi import FastAPI
    from httpx import ASGITransport, AsyncClient
    from api import ai as routes
    from ai_chat import CodexRuntime, owner_key

    async def scenario():
        """Use ASGI transport with synthetic authentication and no provider processes."""
        service = AIChatService(tmp_path / 'ai')
        monkeypatch.setattr(routes, 'get_ai_chat_service', lambda: service)
        monkeypatch.setattr(service, '_ensure_reaper', lambda: None)
        monkeypatch.setattr(service, '_ensure_health_monitor', lambda: None)
        monkeypatch.setattr(CodexRuntime, 'available', staticmethod(lambda: False))
        session = SimpleNamespace(user_id='profile-test-user')
        app = FastAPI()
        app.include_router(routes.router, prefix='/api/ai')
        app.dependency_overrides[routes.require_auth] = lambda: session
        async with AsyncClient(transport=ASGITransport(app=app), base_url='http://test') as client:
            before_status = (await client.get('/api/ai/status')).json()['providers']['chatgpt']['profiles']
            created = await client.post('/api/ai/providers/chatgpt/profiles', json={'name': 'Trading'})
            assert created.status_code == 200
            path = service._profile_path(owner_key(session.user_id))
            before = path.read_bytes()
            for body in [{'name': ' Trading '}, {'name': 'Trading', 'profile': 'default'}]:
                response = await client.post('/api/ai/providers/chatgpt/profiles', json=body)
                assert response.status_code == 400
                assert response.headers['cache-control'] == 'no-store'
                assert response.json()['detail'] == 'A profile named "Trading" already exists. Choose a different name.'
                assert path.read_bytes() == before
            after = (await client.get('/api/ai/status')).json()['providers']['chatgpt']['profiles']
            assert after == before_status + [created.json()]
        await service.shutdown()

    asyncio.run(scenario())


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
