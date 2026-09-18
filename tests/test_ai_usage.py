"""Provider usage normalization and subscription routing, without real credentials."""
import asyncio
from unittest.mock import AsyncMock

from ai_chat import AIChatService


def test_go_usage_projects_only_numeric_windows(tmp_path, monkeypatch):
    """Go quota windows retain percentages and reset dates, excluding unrelated data."""
    async def run():
        service = AIChatService(tmp_path / 'ai')
        monkeypatch.setattr(service.credentials, 'configured', lambda owner: True)
        monkeypatch.setattr(service.credentials, 'load_go_key', lambda owner: 'test-key')
        class Response:
            """Minimal async provider response context."""
            async def __aenter__(self):
                return self
            async def __aexit__(self, *args):
                return False
        class Session:
            """Ensure credentials stay in headers and redirects are disabled."""
            def get(self, url, **kwargs):
                assert url.endswith('/zen/go/v1/usage')
                assert kwargs['allow_redirects'] is False
                assert kwargs['headers']['Authorization'] == 'Bearer test-key'
                return Response()
        monkeypatch.setattr(service, '_http_session', AsyncMock(return_value=Session()))
        monkeypatch.setattr(service, '_read_json_response', AsyncMock(return_value={'usage': {
            'rolling': {'percent': .7, 'resetsAt': '2026-09-19T10:00:00Z'},
            'weekly': {'percent': .9}, 'monthly': {'percent': 3.8}}, 'secret': 'never expose'}))
        result = await service.usage('a' * 32, 'opencode-go')
        assert [item['usedPercent'] for item in result['limits']] == [.7, .9, 3.8]
        assert [item['windowDurationMins'] for item in result['limits']] == [300, 10080, 43200]
        assert result['limits'][0]['resetsAt'] > 0
        assert 'secret' not in str(result)
        await service.shutdown()
    asyncio.run(run())


def test_chatgpt_usage_uses_requested_profile(tmp_path, monkeypatch):
    """Usage and account identity must come from the requested profile together."""
    async def run():
        service = AIChatService(tmp_path / 'ai')
        status = AsyncMock(return_value={'providers': {'chatgpt': {'connected': True, 'email': 'work@example.test', 'limits': []}}})
        monkeypatch.setattr(service, 'status', status)
        result = await service.usage('a' * 32, 'chatgpt', 'b' * 32)
        status.assert_awaited_once_with('a' * 32, 'b' * 32)
        assert result['profile'] == 'b' * 32
        assert result['email'] == 'work@example.test'
        await service.shutdown()
    asyncio.run(run())
