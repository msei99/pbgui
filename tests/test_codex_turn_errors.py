"""Failed turns preserve actionable categories without exposing provider payloads."""
import asyncio

import pytest

from ai_chat import AIChatError, CodexRuntime, _safe_codex_turn_error


@pytest.mark.parametrize('error,expected', [
    ({'codexErrorInfo': 'usageLimitExceeded'}, 'usage limit'),
    ({'message': 'model is not supported for this account'}, 'selected model'),
    ({'message': 'rate limit reached'}, 'rate limit'),
    ({'message': 'token expired'}, 'authentication'),
    ({'codexErrorInfo': 'contextWindowExceeded'}, 'context limit'),
    ({'codexErrorInfo': {'httpConnectionFailed': {'httpStatusCode': 502}}}, 'connection'),
    ({'codexErrorInfo': 'internalServerError'}, 'server error'),
    ({'message': 'private prompt Bearer secret-value'}, 'without a recognized cause'),
])
def test_safe_turn_error_categories(error, expected):
    """Output is fixed safe text, never a copy of raw provider fields."""
    error = {**error, 'additionalDetails': 'secret-value'}
    result = _safe_codex_turn_error(error)
    assert expected in result
    assert 'secret-value' not in result


@pytest.mark.parametrize('notification', [False, True])
def test_failed_turn_exposes_safe_reason(tmp_path, monkeypatch, notification):
    """Both completion errors and preceding turn errors survive the runtime loop."""
    async def scenario():
        runtime = CodexRuntime('a' * 32, tmp_path / 'codex')
        error = {'codexErrorInfo': 'usageLimitExceeded', 'message': 'private details'}

        async def request(method, params=None, timeout=30):
            """Provide a failed turn without contacting an actual provider."""
            if notification:
                await runtime.notifications.put({'method': 'error', 'params': {'turnId': 'turn-1', 'error': error}})
            await runtime.notifications.put({'method': 'turn/completed', 'params': {'turn': {
                'id': 'turn-1', 'status': 'failed', 'error': None if notification else error}}})
            return {'turn': {'id': 'turn-1'}}

        async def interrupt(*args):
            """No live process exists in this fixture."""
            return None

        monkeypatch.setattr(runtime, 'request', request)
        monkeypatch.setattr(runtime, '_interrupt_turn_and_wait', interrupt)
        with pytest.raises(AIChatError, match='usage limit'):
            await runtime.chat('thread-1', 'Hello', 'test-model', 'low')

    asyncio.run(scenario())
