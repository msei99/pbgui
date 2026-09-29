"""Offline OpenCode regression coverage for failed/corrected research tool calls."""

import asyncio
import json
from pathlib import Path

import pytest

from ai_capabilities import AICapabilityError
from ai_chat import AIChatService


@pytest.mark.parametrize('protocol', ['chat', 'responses', 'messages'])
@pytest.mark.parametrize('provider', ['opencode-go', 'opencode-zen'])
def test_failed_research_can_be_corrected_without_duplicate_proposals(tmp_path: Path, monkeypatch, protocol, provider):
    """Replay the real error and successful proposal, not a misleading consumed message."""
    class Response:
        """Stand in for a provider response without any network."""
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

    class Session:
        """Record protocol requests and their returned tool results."""
        def __init__(self):
            self.requests = []

        def post(self, url, **kwargs):
            self.requests.append(json.loads(json.dumps(kwargs['json'])))
            return Response()

    class Capabilities:
        """Require described questions, as proposal validation does."""
        def __init__(self):
            self.calls = []

        def chat_completion_tools(self):
            return [{'type': 'function', 'function': {'name': 'propose_web_research'}}]

        def responses_tools(self):
            return [{'type': 'function', 'name': 'propose_web_research'}]

        def messages_tools(self):
            return [{'name': 'propose_web_research', 'input_schema': {'type': 'object'}}]

        async def dispatch(self, owner, cid, name, arguments):
            self.calls.append(arguments)
            if not arguments.get('jev_questions'):
                raise AICapabilityError('Jev Choice needs 2 to 20 described options')
            return {'research_id': 'single-proposal', 'status': 'awaiting_user_approval'}

    async def scenario():
        service = AIChatService(tmp_path / 'ai')
        capabilities = Capabilities()
        service.capabilities = capabilities
        session = Session()
        invalid = {'prompt': 'Research these coins'}
        valid = {**invalid, 'jev_questions': [{'question': f'Coin {i}', 'options': ['low', 'high']} for i in range(39)]}
        arguments = [invalid, invalid, valid, valid]
        payloads = []
        for i, args in enumerate(arguments):
            if protocol == 'chat':
                payloads.append({'choices': [{'message': {'content': None, 'tool_calls': [{'id': str(i), 'function': {'name': 'propose_web_research', 'arguments': json.dumps(args)}}]}}]})
            elif protocol == 'responses':
                payloads.append({'output': [{'type': 'function_call', 'call_id': str(i), 'name': 'propose_web_research', 'arguments': json.dumps(args)}]})
            else:
                payloads.append({'content': [{'type': 'tool_use', 'id': str(i), 'name': 'propose_web_research', 'input': args}]})
        # Two identical successful calls may appear in one provider round.
        duplicate = payloads.pop()
        if protocol == 'chat':
            payloads[-1]['choices'][0]['message']['tool_calls'].extend(duplicate['choices'][0]['message']['tool_calls'])
        elif protocol == 'responses':
            payloads[-1]['output'].extend(duplicate['output'])
        else:
            payloads[-1]['content'].extend(duplicate['content'])
        if protocol == 'chat':
            payloads.append({'choices': [{'message': {'content': 'Ready for approval'}}]})
        elif protocol == 'responses':
            payloads.append({'output': [{'type': 'message', 'content': [{'type': 'output_text', 'text': 'Ready for approval'}]}]})
        else:
            payloads.append({'content': [{'type': 'text', 'text': 'Ready for approval'}]})

        async def http_session():
            return session

        async def read_response(*args, **kwargs):
            return payloads.pop(0)

        monkeypatch.setattr(service, '_http_session', http_session)
        monkeypatch.setattr(service, '_read_json_response', read_response)
        conversation = await service._conversation('a' * 32, provider, 'deepseek-test', None)
        method = getattr(service, {'chat': '_go_chat_completion_agent_inner', 'responses': '_go_responses_agent_inner', 'messages': '_go_messages_agent_inner'}[protocol])
        try:
            reply = await method('a' * 32, conversation.id, 'https://example.test/v1', 'deepseek-test', 'test-key', [{'role': 'user', 'content': 'Research coins with Jev'}], None)
            assert reply == 'Ready for approval'
            assert capabilities.calls == [invalid, valid]
            request = session.requests[-1]
            if protocol == 'chat':
                results = [json.loads(item['content']) for item in request['messages'] if item['role'] == 'tool']
            elif protocol == 'responses':
                results = [json.loads(item['output']) for item in request['input'] if item.get('type') == 'function_call_output']
            else:
                results = [json.loads(block['content']) for item in request['messages'] if isinstance(item['content'], list) for block in item['content'] if block.get('type') == 'tool_result']
            assert results[0] == results[1] == {'success': False, 'error': 'Jev Choice needs 2 to 20 described options'}
            assert results[2] == results[3] == {'success': True, 'result': {'research_id': 'single-proposal', 'status': 'awaiting_user_approval'}}
        finally:
            await service.shutdown()

    asyncio.run(scenario())


@pytest.mark.parametrize('arguments', [None, [], 'bad-json', 42])
def test_invalid_arguments_never_dispatch(tmp_path, arguments):
    """Malformed provider arguments cannot turn into an executable empty request."""
    async def scenario():
        service = AIChatService(tmp_path / 'ai')
        try:
            cache = {}
            output = await service._agent_capability_result('a' * 32, 'test', 'propose_web_research', arguments, cache)
            assert not output['success']
            assert 'valid JSON object' in output['error']
            assert not cache
        finally:
            await service.shutdown()
    asyncio.run(scenario())


@pytest.mark.parametrize('finish,reasoning,expected', [
    ('length', 'thinking', 'output limit'),
    ('content_filter', '', 'content filter'),
    ('stop', 'thinking', 'reasoning without an answer'),
])
def test_chat_reasoning_failures_never_dispatch_partial_tools(tmp_path, monkeypatch, finish, reasoning, expected):
    """Honor catalog budgets and refuse truncated calls before any capability executes."""
    from ai_chat import AIChatError
    from unittest.mock import AsyncMock

    class Response:
        """Synthetic provider response context."""
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            return False

    class Session:
        """Capture the requested output budget without a provider request."""
        def __init__(self):
            self.bodies = []
        def post(self, url, **kwargs):
            self.bodies.append(kwargs['json'])
            return Response()

    async def scenario():
        service = AIChatService(tmp_path / 'ai')
        session = Session()
        monkeypatch.setattr(service, '_http_session', AsyncMock(return_value=session))
        message = {'content': '', 'reasoning_content': reasoning}
        if finish == 'length':
            message['tool_calls'] = [{'id': 'partial', 'function': {'name': 'propose_web_research', 'arguments': '{}'}}]
        monkeypatch.setattr(service, '_read_json_response', AsyncMock(return_value={'choices': [{'finish_reason': finish, 'message': message}]}))
        dispatch = AsyncMock()
        monkeypatch.setattr(service.capabilities, 'dispatch', dispatch)
        conversation = await service._conversation('a' * 32, 'opencode-go', 'deepseek-test', None)
        try:
            with pytest.raises(AIChatError, match=expected):
                await service._go_chat_completion_agent_inner('a' * 32, conversation.id, 'https://example.test/v1', 'deepseek-test', 'test-key', [{'role': 'user', 'content': 'Research coins'}], 'high', {'output_limit': 65536, 'context': 1000000})
            assert session.bodies[0]['max_tokens'] == 65536
            assert session.bodies[0]['reasoning_effort'] == 'high'
            dispatch.assert_not_awaited()
            assert len(session.bodies) == 1
        finally:
            await service.shutdown()
    asyncio.run(scenario())
