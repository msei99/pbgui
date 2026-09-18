"""Offline regression checks for interrupted and completed ChatGPT logins."""
import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from ai_chat import AIChatService, AIChatError


@pytest.mark.parametrize('method', ['start_codex_login', 'start_codex_browser_login'])
def test_explicit_login_replaces_own_pending_attempt(tmp_path, monkeypatch, method):
    """A lost login link must not permanently block the owner's next attempt."""
    async def run():
        service = AIChatService(tmp_path / 'ai')
        owner = 'a' * 32
        runtime = service._codex_runtime(owner)
        runtime.login_id = 'old-login'
        runtime.cancel_login = AsyncMock()
        runtime.start_device_login = AsyncMock(return_value={'verification_url': 'https://example.test/device'})
        runtime.start_browser_login = AsyncMock(return_value={'auth_url': 'https://example.test/login'})
        monkeypatch.setattr(service, '_ensure_reaper', lambda: None)
        monkeypatch.setattr(service, '_close_idle_codex_runtimes', AsyncMock())
        result = await getattr(service, method)(owner)
        runtime.cancel_login.assert_awaited_once()
        assert result.get('auth_url') or result.get('verification_url')
        other = service._codex_runtime('b' * 32)
        other.login_id = 'other-login'
        other.cancel_login = AsyncMock()
        with pytest.raises(AIChatError, match='Another PBGui user'):
            await getattr(service, method)(owner)
        other.cancel_login.assert_not_awaited()
        await service.shutdown()
    asyncio.run(run())


@pytest.mark.parametrize('event_id,expected', [('current', None), ('old', 'current')])
def test_completion_clears_only_matching_login(tmp_path, event_id, expected):
    """Late completion of a previous login must not clear the replacement attempt."""
    async def run():
        service = AIChatService(tmp_path / 'ai')
        runtime = service._codex_runtime('a' * 32)
        runtime.login_id = 'current'
        stream = asyncio.StreamReader()
        stream.feed_data((json.dumps({'method': 'account/login/completed', 'params': {'loginId': event_id, 'success': True}}) + '\n').encode())
        stream.feed_eof()
        runtime.process = SimpleNamespace(stdout=stream, returncode=None)
        await runtime._read_stdout()
        assert runtime.login_id == expected
        runtime.process = None
        await service.shutdown()
    asyncio.run(run())
