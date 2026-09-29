"""All productive OpenCode protocols share catalog/context-aware output budgets."""
import asyncio
import json
from unittest.mock import AsyncMock

import pytest

from ai_chat import AIChatError, AIChatService


class Response:
    """Synthetic HTTP response context without network access."""
    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False


class Session:
    """Record exact provider request bodies."""
    def __init__(self):
        self.bodies = []

    def post(self, url, **kwargs):
        self.bodies.append(json.loads(json.dumps(kwargs['json'])))
        return Response()


def response_payload(protocol, limited=False):
    """Provide a complete answer or a truncated tool call in each wire format."""
    if protocol == 'responses':
        return {'status': 'incomplete' if limited else 'completed', 'incomplete_details': {'reason': 'max_output_tokens'}, 'output':
                [{'type': 'function_call', 'call_id': 'partial', 'name': 'propose_web_research', 'arguments': '{}'}] if limited else
                [{'type': 'message', 'content': [{'type': 'output_text', 'text': 'Answer'}]}]}
    if protocol == 'messages':
        return {'stop_reason': 'max_tokens' if limited else 'end_turn', 'content':
                [{'type': 'tool_use', 'id': 'partial', 'name': 'propose_web_research', 'input': {}}] if limited else
                [{'type': 'text', 'text': 'Answer'}]}
    return {'choices': [{'finish_reason': 'length' if limited else 'stop', 'message': {'content': 'Partial' if limited else 'Answer'}}]}


@pytest.mark.parametrize('protocol', ['chat', 'responses', 'messages'])
@pytest.mark.parametrize('with_tools', [False, True])
@pytest.mark.parametrize('limited', [False, True])
@pytest.mark.parametrize('provider', ['opencode-go', 'opencode-zen'])
def test_every_productive_path_uses_catalog_budget(tmp_path, monkeypatch, protocol, with_tools, limited, provider):
    """Real routing/wrappers keep output metadata and never execute truncated tools."""
    async def run():
        service = AIChatService(tmp_path / 'ai')
        session = Session()
        monkeypatch.setattr(service.credentials, 'load_go_key', lambda *_: 'test-key')
        monkeypatch.setattr(service, '_go_models', AsyncMock(return_value=[{
            'id': 'test-model', 'protocol': protocol, 'tools': with_tools,
            'output_limit': 64000, 'context': 1000000, 'reasoning_variants': [],
        }]))
        monkeypatch.setattr(service, '_http_session', AsyncMock(return_value=session))
        monkeypatch.setattr(service, '_read_json_response', AsyncMock(return_value=response_payload(protocol, limited)))
        dispatch = AsyncMock()
        monkeypatch.setattr(service.capabilities, 'dispatch', dispatch)
        conversation = await service._conversation('a' * 32, provider, 'test-model', None)
        try:
            request = service._go_chat('a' * 32, 'test-model', [{'role': 'user', 'content': 'Research coins'}], provider, conversation.id)
            if limited:
                with pytest.raises(AIChatError, match='output limit'):
                    await request
            else:
                assert await request == 'Answer'
            field = 'max_output_tokens' if protocol == 'responses' else 'max_tokens'
            assert session.bodies[0][field] == 64000
            assert len(session.bodies) == 1
            dispatch.assert_not_awaited()
        finally:
            await service.shutdown()
    asyncio.run(run())


@pytest.mark.parametrize('protocol', ['chat', 'responses', 'messages'])
def test_context_and_thinking_reservations(protocol):
    """Count tools and Unicode input, and require output room beyond explicit thinking."""
    body = {'messages': [{'role': 'user', 'content': 'Ä' * 100}], 'tools': [{'name': 'read', 'description': 'x' * 300}], 'thinking': {'budget_tokens': 1000}}
    reserve = len(json.dumps(body, ensure_ascii=False, allow_nan=False).encode('utf-8')) + 1024
    budget = AIChatService._apply_opencode_output_budget(body, protocol, {'output_limit': 64000, 'context': reserve + 2000})
    assert budget == 2000
    with pytest.raises(AIChatError, match='no output room'):
        AIChatService._apply_opencode_output_budget(body, protocol, {'output_limit': 1000})
    with pytest.raises(AIChatError, match='no output room'):
        AIChatService._apply_opencode_output_budget(body, protocol, {'context': reserve})


@pytest.mark.parametrize('limit', [None, 0, -1, True, '64000'])
def test_missing_output_metadata_uses_documented_fallback(limit):
    """Absent or unusable metadata never silently restores the old 4096 ceiling."""
    body = {'max_tokens': 4096, 'max_output_tokens': 8}
    assert AIChatService._apply_opencode_output_budget(body, 'chat', {'output_limit': limit}) == 32768
    assert body == {'max_tokens': 32768}


@pytest.mark.parametrize('protocol', ['chat', 'responses', 'messages'])
def test_connection_probe_stays_small(tmp_path, monkeypatch, protocol):
    """The no-context health probe remains eight tokens, independent of chat budgets."""
    async def run():
        service = AIChatService(tmp_path / 'ai')
        session = Session()
        monkeypatch.setattr(service, '_http_session', AsyncMock(return_value=session))
        monkeypatch.setattr(service, '_read_json_response', AsyncMock(return_value=response_payload(protocol)))
        try:
            await service._probe_opencode_model('opencode-go', 'test-model', protocol, 'test-key', 'test-session')
            assert session.bodies[0]['max_output_tokens' if protocol == 'responses' else 'max_tokens'] == 8
        finally:
            await service.shutdown()
    asyncio.run(run())
