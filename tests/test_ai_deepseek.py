"""Offline DeepSeek credentials, HTTP routing, streaming and Loop boundaries."""
from __future__ import annotations

import asyncio
import copy
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import ai_deepseek
from ai_chat import AICredentialStore, AIChatError, AIChatService, _OPENCODE_PROVIDERS, owner_key

# Reuse the standard isolated native Loop fixture.
from tests.test_pb8_loop_optimizer import record

OWNER = 'a' * 32
KEY = 'sk-deepseek-test-key-only'


class Content:
    """Fragment a synthetic HTTP body at arbitrary byte boundaries."""

    def __init__(self, raw, width=13):
        """Initialize the remaining synthetic bytes and fragment width."""
        self.raw, self.width = raw, width

    async def readany(self):
        """Return small fragments, including splits within UTF-8 characters."""
        chunk, self.raw = self.raw[:self.width], self.raw[self.width:]
        return chunk

    async def iter_chunked(self, size):
        """Expose the ordinary JSON/error response reader contract."""
        while self.raw:
            chunk, self.raw = self.raw[:size], self.raw[size:]
            yield chunk


class Response:
    """An async response context recording deterministic transport cleanup."""

    def __init__(self, payload, status=200, stream=False):
        """Prepare JSON or SSE bytes and the matching HTTP metadata."""
        self.status = status
        self.headers = {'Content-Type': 'text/event-stream' if stream else 'application/json'}
        self.content = Content(payload if isinstance(payload, bytes) else json.dumps(payload).encode())
        self.closed = False

    async def __aenter__(self):
        """Enter the owned HTTP response context."""
        return self

    async def __aexit__(self, *_args):
        """Record cleanup on normal completion, errors and cancellation."""
        self.closed = True


class HTTP:
    """Capture actual request URL, authentication and wire JSON without network."""

    def __init__(self, *responses):
        """Initialize queued responses and captured request metadata."""
        self.responses, self.sent = list(responses), []

    def post(self, url, **kwargs):
        """Return one prepared Chat Completions response."""
        self.sent.append((url, copy.deepcopy(kwargs)))
        return self.responses.pop(0)

    def get(self, url, **kwargs):
        """Use the same capture for the read-only authentication preflight."""
        return self.post(url, **kwargs)


def sse(*deltas, finish='stop', done=True):
    """Build real SSE bytes, including keepalives and separate usage events."""
    events = [b': keep-alive\r\n\r\n']
    for delta in deltas:
        events.append(('data: ' + json.dumps({'choices': [{'delta': delta}]}, ensure_ascii=False) + '\r\n\r\n').encode())
    if finish:
        events.append(('data: ' + json.dumps({'choices': [{'delta': {}, 'finish_reason': finish}]}) + '\n\n').encode())
    events.append(b'data: {"choices":[],"usage":{"prompt_tokens":20,"completion_tokens":10}}\n\n')
    if done:
        events.append(b'data: [DONE]\n\n')
    return b''.join(events)


@pytest.fixture
def service(tmp_path, monkeypatch):
    """Keep every capability, credential, history and log below the temporary root."""
    from ai_capabilities import AICapabilityService
    caps = AICapabilityService(tmp_path / 'capabilities')
    monkeypatch.setattr('ai_chat.get_ai_capability_service', lambda: caps)
    chat = AIChatService(tmp_path / 'ai')
    yield chat
    asyncio.run(chat.shutdown())
    asyncio.run(caps.shutdown())


def test_key_roundtrip_permissions_owner_isolation_and_independent_deletion(tmp_path):
    """DeepSeek files are private, validated and independent of every other provider."""
    store = AICredentialStore(tmp_path / 'credentials')
    other = owner_key('another-user')
    assert not store.deepseek_configured(OWNER)
    with pytest.raises(AIChatError, match='not connected'):
        store.load_deepseek_key(OWNER)
    store.save_go_key(OWNER, 'opencode-test-key-only')
    store.save_openrouter_key(OWNER, 'openrouter-test-key-only')
    store.save_deepseek_key(OWNER, '  ' + KEY + '  ')
    assert store.load_deepseek_key(OWNER) == KEY
    assert store.deepseek_configured(OWNER) and not store.deepseek_configured(other)
    path = store._deepseek_path(OWNER)
    assert path.stat().st_mode & 0o777 == 0o600
    assert store.root.stat().st_mode & 0o777 == 0o700
    assert json.loads(path.read_text())['provider'] == 'deepseek'
    store.delete_deepseek_key(OWNER)
    store.delete_deepseek_key(OWNER)
    assert not store.deepseek_configured(OWNER)
    assert store.load_go_key(OWNER) == 'opencode-test-key-only'
    assert store.load_openrouter_key(OWNER) == 'openrouter-test-key-only'


@pytest.mark.parametrize('key', ['', 'short', 'x'*16+'\x00', 'x'*1025])
def test_invalid_key_is_never_written(tmp_path, key):
    """Reject malformed secrets before creating the owner credential file."""
    store = AICredentialStore(tmp_path / 'credentials')
    with pytest.raises(AIChatError):
        store.save_deepseek_key(OWNER, key)
    assert not store.deepseek_configured(OWNER)


def test_key_rejects_symlinks_wrong_provider_and_traversal(tmp_path):
    """No-follow loading never reads another owner's credential or arbitrary file."""
    store = AICredentialStore(tmp_path / 'credentials')
    other = owner_key('other')
    store.save_deepseek_key(other, KEY)
    path = store._deepseek_path(OWNER)
    path.symlink_to(store._deepseek_path(other))
    assert not store.deepseek_configured(OWNER)
    with pytest.raises(AIChatError):
        store.load_deepseek_key(OWNER)
    store.delete_deepseek_key(OWNER)
    assert store.load_deepseek_key(other) == KEY
    path.write_text(json.dumps({'provider': 'openrouter', 'api_key': KEY}))
    with pytest.raises(AIChatError, match='unavailable'):
        store.load_deepseek_key(OWNER)
    with pytest.raises(ValueError):
        store.save_deepseek_key('../escape', KEY)


def test_provider_config_models_and_existing_provider_configuration(service):
    """Use direct DeepSeek hosts and current IDs plus explicit legacy choices only."""
    assert ai_deepseek.DEEPSEEK_PROVIDER == {'base_url': 'https://api.deepseek.com/v1', 'protocol': 'chat'}
    assert set(_OPENCODE_PROVIDERS) == {'opencode-zen', 'opencode-go'}
    assert 'opencode.ai' in _OPENCODE_PROVIDERS['opencode-go']['base_url']
    with pytest.raises(AIChatError, match='not connected'):
        asyncio.run(service.models(OWNER, 'deepseek'))
    service.credentials.save_deepseek_key(OWNER, KEY)
    models = asyncio.run(service.models(OWNER, 'deepseek'))
    assert {row['id'] for row in models} == {'deepseek-chat','deepseek-reasoner','deepseek-flash','deepseek-v4-pro'}
    assert [row['id'] for row in models if row['default']] == ['deepseek-flash']
    assert all(row['tools'] and row['protocol'] == 'chat' for row in models)
    models[0]['name'] = 'modified'
    assert asyncio.run(service.models(OWNER, 'deepseek'))[0]['name'] != 'modified'
    with pytest.raises(AIChatError, match='unavailable'):
        asyncio.run(service._validate_provider_model(OWNER, 'deepseek', 'not-a-model'))
    assert asyncio.run(service.usage(OWNER, 'deepseek'))['connected']
    assert service._provider_state_key(OWNER, 'deepseek') != service._provider_state_key(OWNER, 'opencode-go')


def test_connect_preflight_and_disconnect_are_owner_scoped(service, monkeypatch):
    """Verify with GET /models, never a billed chat, and delete no unrelated key."""
    response = Response({'data': [{'id': 'deepseek-flash'}]})
    http = HTTP(response)
    monkeypatch.setattr(service, '_http_session', AsyncMock(return_value=http))
    service.credentials.save_openrouter_key(OWNER, 'openrouter-test-key-only')
    asyncio.run(service.connect_deepseek(OWNER, KEY))
    assert response.closed
    url, options = http.sent[0]
    assert url == 'https://api.deepseek.com/v1/models'
    assert options['headers']['Authorization'] == 'Bearer '+KEY and options['allow_redirects'] is False
    assert service.credentials.load_deepseek_key(OWNER) == KEY
    asyncio.run(service.disconnect_deepseek(OWNER))
    assert not service.credentials.deepseek_configured(OWNER)
    assert service.credentials.openrouter_configured(OWNER)


@pytest.mark.parametrize('status', [401, 500])
def test_failed_connection_never_overwrites_a_key(service, monkeypatch, status):
    """A failed authentication preflight retains the previous working credential."""
    service.credentials.save_deepseek_key(OWNER, KEY)
    http = HTTP(Response({'error': {'message': KEY}}, status))
    monkeypatch.setattr(service, '_http_session', AsyncMock(return_value=http))
    with pytest.raises(AIChatError) as caught:
        asyncio.run(service.connect_deepseek(OWNER, 'sk-another-test-key-only'))
    assert KEY not in str(caught.value)
    assert service.credentials.load_deepseek_key(OWNER) == KEY


@pytest.mark.parametrize('model', ['deepseek-chat','deepseek-reasoner','deepseek-flash','deepseek-v4-pro'])
def test_streaming_http_chat_route_happy_path(service, monkeypatch, model):
    """The selected model/key reach DeepSeek and UTF-8 deltas form one final answer."""
    service.credentials.save_deepseek_key(OWNER, KEY)
    response = Response(sse({'reasoning_content': 'private thinking'}, {'content': 'Cześć '}, {'content': 'świat'}), stream=True)
    http = HTTP(response)
    monkeypatch.setattr(service, '_http_session', AsyncMock(return_value=http))
    result = asyncio.run(service._deepseek_chat(OWNER, model, [{'role': 'user', 'content': 'Hello'}]))
    assert result == 'Cześć świat' and response.closed
    url, options = http.sent[0]
    assert url == 'https://api.deepseek.com/v1/chat/completions'
    assert options['headers']['Authorization'] == 'Bearer '+KEY
    assert options['allow_redirects'] is False
    assert options['json']['model'] == model and options['json']['stream'] is True
    assert 'tools' not in options['json']


@pytest.mark.parametrize('status,payload,stream,expected', [
    (401, {'error': {'message': KEY}}, False, 'authentication'),
    (500, {'error': {'message': KEY}}, False, 'temporarily unavailable'),
    (200, {'error': {'message': KEY}}, False, 'DeepSeek API'),
    (200, b'data: {"error":{"message":"'+KEY.encode()+b'"}}\n\n', True, 'DeepSeek API'),
    (200, sse({'content':'partial'}, done=False), True, 'before completing'),
    (200, sse({'content':'partial'}, finish=None), True, 'before completing'),
    (200, sse({'content':'partial'}, finish='length'), True, 'output limit'),
])
def test_http_errors_and_incomplete_streams_do_not_accept_partial_answers(service, monkeypatch, status, payload, stream, expected):
    """No secret-bearing upstream diagnostics or truncated output reaches saved chat."""
    service.credentials.save_deepseek_key(OWNER, KEY)
    response = Response(payload, status, stream)
    http = HTTP(response)
    monkeypatch.setattr(service, '_http_session', AsyncMock(return_value=http))
    with pytest.raises(AIChatError) as caught:
        asyncio.run(service._deepseek_chat(OWNER, 'deepseek-reasoner', [{'role': 'user', 'content':'Hello'}]))
    assert expected.lower() in str(caught.value).lower()
    assert KEY not in str(caught.value) and response.closed
    assert len(http.sent) == 1


def test_tool_fragments_reasoning_history_stream_projection_and_next_turn(service, monkeypatch):
    """Execute only complete tools, retain native reasoning, and hide it from browser history."""
    service.credentials.save_deepseek_key(OWNER, KEY)
    first = Response(sse(
        {'reasoning_content':'Need the tool.'},
        {'tool_calls':[{'index':0,'id':'call-1','function':{'name':'list_optimizer_runs','arguments':'{"version":'}}]},
        {'tool_calls':[{'index':0,'function':{'arguments':'"v8"}'}}]}, finish='tool_calls'), stream=True)
    second = Response(sse({'reasoning_content':'Now answer.'},{'content':'Ready.'}), stream=True)
    third = Response({'choices':[{'finish_reason':'stop','message':{'content':'Again.','reasoning_content':'Second turn.'}}]})
    http = HTTP(first, second, third)
    monkeypatch.setattr(service, '_http_session', AsyncMock(return_value=http))
    dispatched = AsyncMock(return_value={'success':True})
    monkeypatch.setattr(service, '_agent_capability_result', dispatched)
    projections = []
    activity = service._set_activity
    async def inspect_activity(owner, cid, text):
        """Capture reasoning activity and browser-safe live messages mid-turn."""
        await activity(owner, cid, text)
        projections.append(service._conversation_projection(service.conversations[cid], include_messages=True))
    monkeypatch.setattr(service, '_set_activity', inspect_activity)
    async def scenario():
        cid = await service.create_conversation(OWNER, 'deepseek', 'deepseek-reasoner')
        result = await service.chat(OWNER, 'deepseek', 'deepseek-reasoner', 'Hello', cid)
        assert result['reply'] == 'Ready.'
        conversation = service.conversations[cid]
        assert conversation.messages[-1]['reasoning_content'] == 'Now answer.'
        assert 'reasoning_content' not in service._conversation_projection(conversation, include_messages=True)['messages'][-1]
        assert cid not in service.deepseek_live
        await service.chat(OWNER, 'deepseek', 'deepseek-reasoner', 'More', cid)
        return cid
    cid = asyncio.run(scenario())
    assert dispatched.await_count == 1
    assert dispatched.await_args.args[2:4] == ('list_optimizer_runs', {'version':'v8'})
    second_body = http.sent[1][1]['json']
    assert second_body['messages'][-2]['reasoning_content'] == 'Need the tool.'
    assert second_body['messages'][-1]['tool_call_id'] == 'call-1'
    assert http.sent[2][1]['json']['messages'][-2]['reasoning_content'] == 'Now answer.'
    assert any(item['activity'] == 'DeepSeek is reasoning' for item in projections)
    assert all(KEY not in json.dumps(item) for item in projections)
    assert all('reasoning_content' not in json.dumps(item['streaming_messages']) for item in projections)
    assert first.closed and second.closed and third.closed
    assert cid not in service.deepseek_live


def test_deepseek_setup_probe_and_authenticated_credential_routes(service, monkeypatch):
    """Setup accepts DeepSeek and uses the caller's owner with no-store responses."""
    import ai_setup
    import api.ai as api
    probe = AsyncMock(return_value='PBGui connection OK')
    monkeypatch.setattr(service, '_deepseek_chat', probe)
    service.credentials.save_deepseek_key(OWNER, KEY)
    result = asyncio.run(ai_setup.test_model(service, OWNER,
        ai_setup.ModelTestRequest(provider='deepseek', model='deepseek-chat'),
        SimpleNamespace(is_disconnected=AsyncMock(return_value=False))))
    assert result['tools_enabled'] is False and result['conversation_created'] is False
    assert probe.await_args.args == (OWNER, 'deepseek-chat', [{'role':'user','content':'Reply PBGui connection OK.'}])
    connect, disconnect = AsyncMock(), AsyncMock()
    monkeypatch.setattr(api, 'get_ai_chat_service', lambda: SimpleNamespace(connect_deepseek=connect, disconnect_deepseek=disconnect))
    session = SimpleNamespace(user_id='fixture-user')
    response = asyncio.run(api.connect_deepseek(api.GoConnectRequest(api_key=KEY), session))
    assert response.headers['cache-control'] == 'no-store'
    connect.assert_awaited_once_with(owner_key(session.user_id), KEY)
    response = asyncio.run(api.disconnect_deepseek(session))
    disconnect.assert_awaited_once_with(owner_key(session.user_id))
    assert response.headers['cache-control'] == 'no-store' and KEY not in response.body.decode()


def test_deepseek_loop_preflight_accepts_current_models_not_undersized_legacy(service, tmp_path):
    """Adding a provider never bypasses Loop context or explicit job authorization."""
    from api.loop_optimizer_v8 import LoopStart
    from pb8_loop_ai import LoopAI
    from pb8_loop_evidence import LoopAIPause
    service.credentials.save_deepseek_key(OWNER, KEY)
    ai = LoopAI(SimpleNamespace(root=tmp_path), service)
    settings = LoopStart(config_name='fixture', provider='deepseek', model='deepseek-flash', max_rounds=1).model_dump()
    assert settings['authorization'] is False
    metadata = asyncio.run(ai.preflight(OWNER, settings))
    assert metadata['loop_profile']['context'] == 1_000_000 and metadata['loop_profile']['streaming']
    settings['model'] = 'deepseek-reasoner'
    with pytest.raises(LoopAIPause, match='at least 256k'):
        asyncio.run(ai.preflight(OWNER, settings))


def test_loop_streamed_evidence_tool_uses_deepseek_key_and_endpoint(record, service, monkeypatch):
    """The actual Loop adapter retains reasoning and exposes only the evidence reader."""
    from pb8_loop_ai import LoopAI
    from pb8_loop_ai_policy import DecisionLedger
    from pb8_loop_ai_v2 import _ACTIVE, provider_request
    from tests.test_pb8_loop_dialog_v2 import frozen
    store, row = record
    row['settings'].update(provider='deepseek', model='deepseek-flash', effort='')
    service.credentials.save_deepseek_key(OWNER, KEY)
    reader = frozen((store, row))
    metadata = next(item for item in ai_deepseek.models() if item['id'] == 'deepseek-flash')
    ledger = DecisionLedger(store, row, 'select', reader.snapshot['sha256'], metadata)
    runtime = {'context':1_000_000,'page_tokens':5000,'output_tokens':1000,'streaming':True,
               'call_timeout':30,'connect_timeout':10,'first_timeout':10,'idle_timeout':10}
    first = Response(sse({'reasoning_content':'Read exact evidence.'},
        {'tool_calls':[{'index':0,'id':'evidence-1','function':{'name':'loop_evidence',
            'arguments':json.dumps({'path':'/required','offset':0})}}]}, finish='tool_calls'), stream=True)
    second = Response(sse({'reasoning_content':'Evidence is complete.'}, {'content':'{"ok":true}'}), stream=True)
    http = HTTP(first, second)
    monkeypatch.setattr(service, '_http_session', AsyncMock(return_value=http))
    ai = LoopAI(store, service)
    async def scenario():
        """Enter the adapter with the owning evidence, deadline and durable ledger."""
        token = _ACTIVE.set({'profile':runtime,'session':reader,'ledger':ledger,
            'instructions':'Read exact evidence, then answer JSON.','validate':None})
        try:
            return await provider_request(ai, row, metadata, {'stage':'select'}, 1000)
        finally:
            _ACTIVE.reset(token)
    text, usage = asyncio.run(scenario())
    assert json.loads(text) == {'ok':True} and usage['provider'] == 'deepseek'
    assert len(http.sent) == 2 and first.closed and second.closed
    for url, options in http.sent:
        assert url == 'https://api.deepseek.com/v1/chat/completions'
        assert options['headers']['Authorization'] == 'Bearer '+KEY
        assert options['allow_redirects'] is False
        assert [item['function']['name'] for item in options['json']['tools']] == ['loop_evidence']
    assert http.sent[1][1]['json']['messages'][-2]['reasoning_content'] == 'Read exact evidence.'
    assert http.sent[1][1]['json']['messages'][-1]['tool_call_id'] == 'evidence-1'


def test_loaded_key_permissions_are_repaired(tmp_path):
    """Existing credential files cannot remain accidentally world readable."""
    store = AICredentialStore(tmp_path / 'credentials')
    store.save_deepseek_key(OWNER, KEY)
    path = store._deepseek_path(OWNER)
    path.chmod(0o644)
    assert store.load_deepseek_key(OWNER) == KEY
    assert path.stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize('payload', [{'error':{'message':KEY}}, {'data':'invalid'}, []])
def test_connection_rejects_success_status_with_invalid_model_payload(service, monkeypatch, payload):
    """An HTTP 200 error envelope is not evidence of a verified API key."""
    http = HTTP(Response(payload))
    monkeypatch.setattr(service, '_http_session', AsyncMock(return_value=http))
    with pytest.raises(AIChatError, match='invalid model'):
        asyncio.run(service.connect_deepseek(OWNER, KEY))
    assert not service.credentials.deepseek_configured(OWNER)


def test_long_reasoning_does_not_use_the_visible_answer_limit(service, monkeypatch):
    """A valid long R1 reasoning field is separate from its short displayed answer."""
    service.credentials.save_deepseek_key(OWNER, KEY)
    reasoning = 'x' * 50_000
    http = HTTP(Response(sse({'reasoning_content':reasoning}, {'content':'Done.'}), stream=True))
    monkeypatch.setattr(service, '_http_session', AsyncMock(return_value=http))
    async def scenario():
        """Run through the actual tool-enabled chat route and persistent history."""
        cid = await service.create_conversation(OWNER, 'deepseek', 'deepseek-reasoner')
        result = await service.chat(OWNER, 'deepseek', 'deepseek-reasoner', 'Hello', cid)
        assert service.conversations[cid].messages[-1]['reasoning_content'] == reasoning
        assert service._conversation_projection(service.conversations[cid], include_messages=True)['messages'][-1]['content'] == 'Done.'
        return result
    assert asyncio.run(scenario())['reply'] == 'Done.'


def test_disconnect_cancels_owned_stream_and_keeps_other_keys(service, monkeypatch):
    """Disconnect must close an in-flight HTTP response and remove transient stream state."""
    service.credentials.save_deepseek_key(OWNER, KEY)
    service.credentials.save_openrouter_key(OWNER, 'openrouter-test-key-only')
    response = Response(b'', stream=True)
    started = asyncio.Event()
    async def block():
        """Wait for cancellation without network, polling or wall-clock sleep."""
        started.set()
        await asyncio.Future()
    response.content.readany = block
    http = HTTP(response)
    monkeypatch.setattr(service, '_http_session', AsyncMock(return_value=http))
    async def scenario():
        """Cancel through the actual owner/provider lifecycle, not a mocked cancellation."""
        cid = await service.create_conversation(OWNER, 'deepseek', 'deepseek-chat')
        task = asyncio.create_task(service.chat(OWNER, 'deepseek', 'deepseek-chat', 'Hello', cid))
        await asyncio.wait_for(started.wait(), 2)
        await service.disconnect_deepseek(OWNER)
        result = await asyncio.gather(task, return_exceptions=True)
        assert isinstance(result[0], AIChatError) and 'cancelled' in str(result[0])
        assert cid not in service.active_tasks and cid not in service.deepseek_live
    asyncio.run(scenario())
    assert response.closed and not service.credentials.deepseek_configured(OWNER)
    assert service.credentials.openrouter_configured(OWNER)


def test_status_and_saved_selection_do_not_expose_keys(service, monkeypatch):
    """DeepSeek connection state and selected model survive without credential disclosure."""
    service.credentials.save_deepseek_key(OWNER, KEY)
    monkeypatch.setattr(service, '_ensure_reaper', lambda: None)
    monkeypatch.setattr(service, '_ensure_health_monitor', lambda: None)
    monkeypatch.setattr(service.claude, 'status', AsyncMock(return_value={'available':False,'connected':False}))
    monkeypatch.setattr('ai_chat.CodexRuntime.available', staticmethod(lambda:False))
    status = asyncio.run(service.status(OWNER))
    assert status['providers']['deepseek'] == {'available':True,'connected':True}
    assert not status['providers']['opencode-go']['connected']
    assert KEY not in json.dumps(status)
    selection = {'provider':'deepseek','model':'deepseek-reasoner','profile':'default','effort':'','service_tier':''}
    service.save_preferences(OWNER, selection=selection)
    assert service.get_preferences(OWNER)['selection'] == selection
    assert 'selection' not in service.get_preferences('b'*32)


def test_malformed_stream_content_is_a_safe_provider_error(service, monkeypatch):
    """Invalid delta types fail as AIChatError instead of crashing response serialization."""
    service.credentials.save_deepseek_key(OWNER, KEY)
    http = HTTP(Response(sse({'content':123}), stream=True))
    monkeypatch.setattr(service, '_http_session', AsyncMock(return_value=http))
    with pytest.raises(AIChatError, match='invalid stream'):
        asyncio.run(service._deepseek_chat(OWNER, 'deepseek-chat', [{'role':'user','content':'Hello'}]))
