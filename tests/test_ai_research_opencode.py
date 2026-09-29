"""Offline Go/Zen research protocol, credential separation and lifecycle regression tests."""
import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from ai_chat import AIChatError, AIChatService
from ai_research import ResearchService
import ai_research_opencode as adapter

OWNER = 'a' * 32


def fake_chat(protocol='chat'):
    """Expose catalog/credential methods but deliberately no capability dispatch or history."""
    return SimpleNamespace(
        accepting_turns=True, provider_disconnecting=set(),
        credentials=SimpleNamespace(configured=lambda owner: owner == OWNER, load_go_key=lambda owner: 'test-model-secret'),
        _validate_provider_model=AsyncMock(return_value={'id': 'chosen-model', 'protocol': protocol}),
        _validate_model_effort=Mock(), _validate_model_service_tier=Mock(),
        _selected_reasoning_variant=AIChatService._selected_reasoning_variant,
        _apply_reasoning_variant=AIChatService._apply_reasoning_variant)


def model_response(protocol, text, **changes):
    """Produce provider-shaped synthetic assistant text."""
    value = {'model': 'chosen-model'}
    if protocol == 'responses':
        value['output'] = [{'type': 'message', 'content': [{'type': 'output_text', 'text': text}]}]
    elif protocol == 'messages':
        value['content'] = [{'type': 'text', 'text': text}]
    else:
        value['choices'] = [{'finish_reason': 'stop', 'message': {'role': 'assistant', 'content': text}}]
    value.update(changes)
    return json.dumps(value)


def search_response(text='Public evidence https://example.test/report'):
    """Produce the fixed Exa MCP response shape."""
    return json.dumps({'jsonrpc': '2.0', 'id': 1, 'result': {'content': [{'type': 'text', 'text': text}]}})


@pytest.mark.parametrize('provider', ['opencode-go', 'opencode-zen'])
@pytest.mark.parametrize('protocol', ['chat', 'responses', 'messages'])
def test_approved_research_uses_selected_model_and_search_only(tmp_path, monkeypatch, provider, protocol):
    """Same selected model plans and answers; Exa never receives its credentials or UI data."""
    calls, sessions = [], []

    async def post(session, url, headers, body):
        sessions.append(session)
        calls.append((url, headers, body))
        if url == adapter.SEARCH_URL:
            assert 'test-model-secret' not in str(headers) + str(body)
            assert set(headers) == {'Accept'}
            assert body['params']['name'] == 'web_search_exa'
            assert body['params']['arguments']['query'] == 'BTC current security incidents'
            return 'event: message\ndata: ' + search_response() + '\n\n'
        assert 'test-model-secret' in headers.values() or headers.get('Authorization') == 'Bearer test-model-secret'
        assert body['model'] == 'chosen-model' and 'tools' not in body
        assert '/zen/go/v1/' in url if provider == 'opencode-go' else '/zen/v1/' in url
        content = body.get('input', body.get('messages'))[-1]['content']
        data = json.loads(content)
        assert data['prompt'] == 'Review BTC using current web evidence'
        assert not any(key in data for key in ('context', 'history', 'conversation_id'))
        instructions = body.get('instructions') or body.get('system') or body['messages'][0]['content']
        assert 'https://mcp.exa.ai/mcp' in instructions
        if data['stage'] == 'plan':
            assert 'search_results' not in data
            return model_response(protocol, '{"queries":["BTC current security incidents"]}', model='upstream-alias')
        assert 'Public evidence' in data['search_results'][0]['evidence']
        return model_response(protocol, 'Findings: https://example.test/report', model='upstream-alias')

    monkeypatch.setattr(adapter, '_post', post)

    async def run():
        research = ResearchService(fake_chat(protocol))
        preview = await research.preview(OWNER, provider=provider, model='chosen-model', profile='default',
                                         prompt='Review BTC using current web evidence')
        assert not calls
        assert 'Exa' in preview['instructions']
        research.start(OWNER, preview['id'], preview['digest'])
        await research.tasks[preview['id']]
        result = research.get(OWNER, preview['id'])
        assert result['status'] == 'completed', result['error']
        assert result['web_calls'] == 1
        assert 'Findings:' in result['answer']
        assert len(calls) == 3
        assert sessions[0] is sessions[2] and sessions[0] is not sessions[1]
        assert all(session.closed for session in sessions)
        assert all(session.timeout.total is None and session.timeout.sock_read == 180
                   and session.timeout.connect == 30 for session in sessions)
        await research.shutdown()
    asyncio.run(run())


@pytest.mark.parametrize('bad_plan', ['Not JSON', '{"queries":[]}', '{"queries":[123]}', json.dumps({'queries': ['x'] * 9})])
def test_invalid_plan_never_searches(bad_plan):
    """Malformed or excessive model plans cannot create outbound queries."""
    with pytest.raises(AIChatError):
        adapter._queries(bad_plan, 8)


@pytest.mark.parametrize('failure', ['no_sources', 'search_error', 'model_changed', 'tool_call'])
def test_research_failures_never_return_unsearched_answer(monkeypatch, failure):
    """Missing evidence, redirects to another model and unsolicited tool calls fail closed."""
    calls = []
    model_calls = []

    async def post(session, url, headers, body):
        calls.append(url)
        if url == adapter.SEARCH_URL:
            if failure == 'search_error':
                raise AIChatError('Search unavailable')
            return search_response('Public evidence https://example.test/report' if failure == 'model_changed' else 'No matches')
        model_calls.append(url)
        if failure == 'model_changed' and len(model_calls) > 1:
            return model_response('chat', 'Unsearched answer', model='other-model')
        if failure == 'tool_call':
            return model_response('chat', '', choices=[{'message': {'tool_calls': [{'function': {'name': 'python'}}]}}])
        return model_response('chat', '{"queries":["BTC risks"]}')

    monkeypatch.setattr(adapter, '_post', post)

    async def run():
        research = ResearchService(fake_chat())
        preview = await research.preview(OWNER, provider='opencode-go', model='chosen-model', profile='default', prompt='BTC risks')
        research.start(OWNER, preview['id'], preview['digest'])
        await research.tasks[preview['id']]
        result = research.get(OWNER, preview['id'])
        assert result['status'] == 'error'
        assert not result['answer']
        assert len(calls) <= (3 if failure == 'model_changed' else 2)
        await research.shutdown()
    asyncio.run(run())


def test_disconnect_cancels_clients_and_revokes_both_providers(monkeypatch):
    """Provider logout waits for client cleanup and revokes all pending Go/Zen previews."""
    sessions = []

    async def run():
        entered = asyncio.Event()

        async def post(session, url, headers, body):
            sessions.append(session)
            if url == adapter.SEARCH_URL:
                entered.set()
                await asyncio.Event().wait()
            return model_response('chat', '{"queries":["BTC risk"]}')

        monkeypatch.setattr(adapter, '_post', post)
        research = ResearchService(fake_chat())
        first = await research.preview(OWNER, provider='opencode-go', model='chosen-model', profile='default', prompt='BTC risk')
        second = await research.preview(OWNER, provider='opencode-zen', model='chosen-model', profile='default', prompt='ETH risk')
        research.start(OWNER, first['id'], first['digest'])
        await asyncio.wait_for(entered.wait(), 2)
        # A preview may expire while the logout is cancelling a running job.
        research.records[second['id']]['expires'] = 0
        await research.cancel_opencode(OWNER)
        research.records[second['id']]['expires'] = float('inf')
        assert all(session.closed for session in sessions)
        assert research.get(OWNER, first['id'])['status'] == 'cancelled'
        assert research.start(OWNER, second['id'], second['digest'])['status'] == 'cancelled'
        assert not research.tasks
        await research.shutdown()
    asyncio.run(run())


def test_post_does_not_follow_redirects_or_log_provider_body(monkeypatch):
    """An upstream redirect cannot forward credentials to another destination."""
    monkeypatch.setattr("logging_helpers.human_log", lambda *args, **kwargs: None)

    class Response:
        """Return a redirect before any response-body read."""
        status = 302
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            return False

    class Session:
        """Assert the transport-level redirect boundary."""
        def post(self, url, **kwargs):
            assert kwargs['allow_redirects'] is False
            return Response()

    async def run():
        with pytest.raises(AIChatError, match='HTTP 302'):
            await adapter._post(Session(), adapter.SEARCH_URL, {}, {})
    asyncio.run(run())


@pytest.mark.parametrize('protocol', ['chat', 'responses', 'messages'])
@pytest.mark.parametrize('stage', ['plan', 'answer'])
@pytest.mark.parametrize('effort', ['', 'none', 'high'])
def test_research_uses_advertised_output_budget_for_high_reasoning(monkeypatch, protocol, stage, effort):
    """Neither a high-reasoning plan nor a multi-coin report is limited to 4096 tokens."""
    bodies = []
    async def post(session, url, headers, body):
        bodies.append(body)
        return model_response(protocol, 'Complete response')
    monkeypatch.setattr(adapter, '_post', post)
    record = {'id': 'a' * 32, 'provider': 'opencode-go', 'model': 'chosen-model',
              'effort': effort, 'instructions': 'Public evidence only'}
    chat = fake_chat(protocol)
    chat._selected_reasoning_variant = lambda *_: None
    result = asyncio.run(adapter._model_text(None, chat, record,
        {'protocol': protocol, 'output_limit': 24000, 'context': 128000}, 'test-key',
        {'stage': stage, 'prompt': 'Research 39 coins'}))
    assert result == 'Complete response'
    assert bodies[0].get('max_output_tokens', bodies[0].get('max_tokens')) == 24000
    assert 'tools' not in bodies[0]


@pytest.mark.parametrize('protocol', ['chat', 'responses', 'messages'])
def test_token_limited_report_is_preserved_but_not_accepted(monkeypatch, protocol):
    """Partial evidence is visible, never silently promoted to a completed Jev input."""
    text = 'Partial public report with source https://example.test'
    payload = json.loads(model_response(protocol, text))
    if protocol == 'chat':
        payload['choices'][0]['finish_reason'] = 'length'
    elif protocol == 'responses':
        payload.update(status='incomplete', incomplete_details={'reason': 'max_output_tokens'})
    else:
        payload['stop_reason'] = 'max_tokens'
    monkeypatch.setattr(adapter, '_post', AsyncMock(return_value=json.dumps(payload)))
    record = {'id': 'a' * 32, 'provider': 'opencode-go', 'model': 'chosen-model',
              'effort': '', 'instructions': 'Public evidence only'}
    with pytest.raises(AIChatError, match='output limit.*not sent to Jev'):
        asyncio.run(adapter._model_text(None, fake_chat(protocol), record,
            {'protocol': protocol, 'output_limit': 8000}, 'test-key', {'stage': 'answer'}))
    assert record['answer'] == text
    adapter._post.assert_awaited_once()


def test_output_budget_respects_context_and_explicit_thinking(monkeypatch):
    """Reserve input space and reject incompatible thinking before any provider request."""
    post = AsyncMock(return_value=model_response('messages', 'Complete'))
    monkeypatch.setattr(adapter, '_post', post)
    chat = fake_chat('messages')
    chat._selected_reasoning_variant = lambda *_: {'type': 'budget_tokens', 'budget_tokens': 6000}
    record = {'id': 'a' * 32, 'provider': 'opencode-go', 'model': 'chosen-model',
              'effort': 'high', 'instructions': 'Public evidence only'}
    with pytest.raises(AIChatError, match='no output room'):
        asyncio.run(adapter._model_text(None, chat, record,
            {'protocol': 'messages', 'output_limit': 5000, 'context': 128000}, 'test-key', {'stage': 'answer'}))
    post.assert_not_awaited()


@pytest.mark.parametrize('search', [True, False])
@pytest.mark.parametrize('message,reason', [('internal error secret-prompt', 'request failed'), ('max_tokens exceeds limit secret-key', 'token/context budget')])
def test_http_error_identifies_service_without_echoing_body(monkeypatch, search, message, reason):
    """500 diagnostics identify the failing service and never expose upstream payloads."""
    class Content:
        """Yield a fragmented synthetic provider error."""
        async def iter_chunked(self, size):
            raw = json.dumps({'error': {'message': message}}).encode()
            yield raw[:8]
            yield raw[8:]

    class Response:
        """Stand in for an HTTP failure."""
        status = 500
        content = Content()
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            return False

    class Session:
        """Count calls to ensure failures never silently repeat a request."""
        calls = 0
        def post(self, *args, **kwargs):
            self.calls += 1
            return Response()

    logs = []
    monkeypatch.setattr('logging_helpers.human_log', lambda *args, **kwargs: logs.append(str(args)))
    async def run():
        session = Session()
        with pytest.raises(AIChatError) as caught:
            await adapter._post(session, adapter.SEARCH_URL if search else 'https://example.test/chat/completions', {}, {})
        text = str(caught.value)
        assert ('Exa web search' if search else 'OpenCode model') in text
        assert reason in text and 'HTTP 500' in text
        assert 'secret' not in text + str(logs)
        assert session.calls == 1
    asyncio.run(run())
