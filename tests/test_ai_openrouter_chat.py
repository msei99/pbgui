"""Offline OpenRouter chat, Jev separation, tool approval and streaming boundaries."""
from __future__ import annotations

import asyncio
import copy
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import ai_openrouter_chat as chat_transport
import ai_setup
from ai_chat import AIChatError, AIChatService
from ai_jev import JEV_MODEL
from ai_model_routing import CHAT_ADAPTER, DECISION_ADAPTER, is_decision_model, model_adapter
from tests.test_ai_deepseek import HTTP, Response

OWNER = 'a' * 32
OTHER = 'b' * 32
KEY = 'sk-or-test-key-only'
NEW_KEY = 'sk-or-replacement-key-only'
MODEL = 'acme/chat-1'
SLUG = 'acme/chat-1-20261001'
TOOLS = ['max_tokens', 'tools', 'tool_choice', 'reasoning', 'include_reasoning']


def catalog_row(model_id=MODEL, **overrides):
    """Build one synthetic catalog row shaped like the documented model object."""
    row = {
        'id': model_id, 'canonical_slug': SLUG if model_id == MODEL else model_id + '-20261001',
        'name': model_id, 'context_length': 200_000,
        'architecture': {'input_modalities': ['text', 'image'], 'output_modalities': ['text']},
        'pricing': {'prompt': '0.000001', 'completion': '0.000002'},
        'top_provider': {'context_length': 200_000, 'max_completion_tokens': 64_000},
        'supported_parameters': TOOLS, 'expiration_date': None,
        'endpoints': ['/api/v1/chat/completions'],
        'reasoning': {'mandatory': False, 'default_enabled': True, 'default_effort': 'medium',
                      'supported_efforts': ['high', 'medium', 'low', 'none']},
    }
    return {**row, **overrides}


def projected(**overrides):
    """Project one synthetic row through the production catalog filter."""
    rows = chat_transport.models({'data': [catalog_row(**overrides)]}, today='2026-11-02')
    return rows[0] if rows else None


def metadata(**overrides):
    """Return validated chat metadata for direct transport calls."""
    return chat_transport.with_tool_status([projected(**overrides)], {})[0]


def event(payload):
    """Encode one SSE data event."""
    return ('data: ' + json.dumps(payload) + '\n\n').encode()


def delta(value, finish=None, model=MODEL):
    """Build one streamed chunk."""
    return {'model': model, 'choices': [{'index': 0, 'delta': value, 'finish_reason': finish}]}


def stream(*deltas, finish='stop', usage=True, done=True, model=MODEL):
    """Build a complete documented stream: terminal chunk, repeated usage chunk, [DONE]."""
    parts = [b': OPENROUTER PROCESSING\n\n']
    parts += [event(delta(item, model=model)) for item in deltas]
    if finish:
        parts.append(event(delta({'content': ''}, finish, model)))
    parts.append(b': OPENROUTER PROCESSING\n\n')
    if usage:
        parts.append(event({**delta({'content': ''}, finish, model), 'usage': {'prompt_tokens': 20, 'completion_tokens': 10}}))
    if done:
        parts.append(b'data: [DONE]\n\n')
    return b''.join(parts)


def tool_call(arguments='{"version":"v8"}', call_id='call-1', name='list_optimizer_runs', index=0):
    """Build one streamed tool-call fragment."""
    return {'tool_calls': [{'index': index, 'id': call_id, 'type': 'function',
                            'function': {'name': name, 'arguments': arguments}}]}


def reply(*deltas, **kwargs):
    """Wrap one complete stream as an HTTP response."""
    return Response(stream(*deltas, **kwargs), stream=True)


def probe_responses(http):
    """Answer a synthetic probe by echoing the nonce from its captured request."""
    async_post = http.post

    def post(url, **kwargs):
        """Serve the first probe step from the nonce that request actually contained."""
        text = kwargs['json']['messages'][1]['content'] if '/chat/completions' in url else ''
        if 'exactly once with nonce' in text and not http.responses:
            if len(kwargs['json']['messages']) == 2:
                nonce = text.split('nonce ')[1].split(',')[0]
                http.responses.append(reply(tool_call(json.dumps({'nonce': nonce}), 'probe-1', chat_transport.PROBE_TOOL), finish='tool_calls'))
            else:
                http.responses.append(reply({'content': 'OK'}))
        return async_post(url, **kwargs)
    http.post = post
    return http


@pytest.fixture
def service(tmp_path, monkeypatch):
    """Keep every capability, credential, history and log below the temporary root."""
    from ai_capabilities import AICapabilityService
    caps = AICapabilityService(tmp_path / 'capabilities')
    monkeypatch.setattr('ai_chat.get_ai_capability_service', lambda: caps)
    chat = AIChatService(tmp_path / 'ai')
    rows = chat_transport.models({'data': [catalog_row(), catalog_row('acme/text-only', architecture={
        'input_modalities': ['text'], 'output_modalities': ['text']}, supported_parameters=['max_tokens'])]}, today='2026-11-02')
    chat.catalog_rows = rows
    monkeypatch.setattr(chat, '_fetch_openrouter_catalog', AsyncMock(side_effect=lambda key: copy.deepcopy(chat.catalog_rows)))
    chat.credentials.save_openrouter_key(OWNER, KEY)
    yield chat
    asyncio.run(chat.shutdown())
    asyncio.run(caps.shutdown())


def use_http(monkeypatch, service, http):
    """Route the service through one captured offline HTTP fixture."""
    monkeypatch.setattr(service, '_http_session', AsyncMock(return_value=http))
    return http


def test_catalog_does_not_probe_model_endpoints(service, monkeypatch):
    """Browsing models reads the catalog without background endpoint or inference requests."""
    http = AsyncMock(side_effect=AssertionError('Catalog must not start endpoint probes'))
    monkeypatch.setattr(service, '_http_session', http)
    rows = asyncio.run(service._openrouter_models(OWNER))
    assert len(rows) == 3
    assert all('no_data_check' not in row and 'data_handling' not in row for row in rows)
    http.assert_not_awaited()


def approve(service, owner=OWNER, key=KEY, model=MODEL, slug=SLUG):
    """Record the state a successful synthetic probe leaves behind."""
    service.credentials.save_openrouter_tool_approval(owner, key, model, slug)


async def finished(service, conversation_id):
    """Wait for one detached turn to reach its terminal state."""
    for _ in range(2000):
        if not service.conversations[conversation_id].busy:
            return
        await asyncio.sleep(0.001)
    raise AssertionError('Turn did not finish')


# --- Adapter mapping and Jev separation ---

def test_adapter_mapping_is_exact_connection_and_model():
    """Only the configured pair is a decision model; catalog text never qualifies."""
    assert model_adapter('openrouter', JEV_MODEL) == DECISION_ADAPTER
    assert is_decision_model('openrouter', JEV_MODEL)
    assert model_adapter('openrouter', MODEL) == CHAT_ADAPTER
    assert model_adapter('deepseek', JEV_MODEL) == CHAT_ADAPTER
    assert model_adapter('openrouter', '~typesafe/jev-latest') == CHAT_ADAPTER


def test_shared_decision_logic_has_no_connection_or_auth():
    """Jev logic receives a transport; OpenRouter URLs and keys stay in its adapter."""
    source = (Path(__file__).resolve().parents[1] / 'ai_jev.py').read_text(encoding='utf-8')
    for forbidden in ('aiohttp', 'openrouter.ai', 'Authorization', 'Bearer', 'api_key', 'session'):
        assert forbidden not in source
    transport = (Path(__file__).resolve().parents[1] / 'ai_openrouter.py').read_text(encoding='utf-8')
    assert 'prepare_user_jev_payload' not in transport and '_format_structured_jev_answers' not in transport


@pytest.mark.parametrize('overrides', [
    {'model_id': JEV_MODEL},
    {'model_id': 'typesafe/jev-router', 'pricing': {'prompt': '-1', 'completion': '-1'}},
    {'model_id': 'openrouter/auto'},
    {'model_id': 'acme/chat-1:batch'},
    {'model_id': '~typesafe/jev-latest'},
    {'alias_target': 'acme/chat-2'},
    {'expiration_date': '2026-11-01'},
    {'endpoints': ['/api/v1/messages']},
    {'architecture': {'input_modalities': ['text'], 'output_modalities': ['decisions']}},
    {'architecture': {'input_modalities': ['text'], 'output_modalities': ['text', 'image']}},
    {'supported_parameters': ['tools']},
])
def test_catalog_excludes_models_that_cannot_be_ordinary_fixed_chat(overrides):
    """Routers, aliases, batches and decision entries never become selectable chat models."""
    assert projected(**overrides) is None


def test_catalog_row_cannot_become_a_decision_model():
    """A catalog entry never sets decision or tool state on its own."""
    row = projected()
    assert 'decision' not in row and row['tools'] is False and 'tool_status' not in row
    assert row['loop_tool_support'] is False and row['image_input'] is True
    assert row['context'] == 200_000 and row['output_limit'] == 64_000
    assert row['cost'] == {'input': 1.0, 'output': 2.0}
    assert 'Zero Data Retention' in row['retention'] and row['retention'].startswith('Provider-dependent')


def test_catalog_failure_keeps_jev_and_reports_the_error(service, monkeypatch):
    """Jev needs no catalog; other selections report the real failure, not unavailability."""
    monkeypatch.setattr(service, '_fetch_openrouter_catalog', AsyncMock(side_effect=AIChatError('Could not load OpenRouter models')))
    async def scenario():
        rows = await service.models(OWNER, 'openrouter')
        assert [row['id'] for row in rows] == [JEV_MODEL]
        assert rows[0]['decision'] is True and rows[0]['default'] is True
        assert (await service._validate_provider_model(OWNER, 'openrouter', JEV_MODEL))['decision'] is True
        with pytest.raises(AIChatError, match='Could not load OpenRouter models'):
            await service._validate_provider_model(OWNER, 'openrouter', MODEL)
    asyncio.run(scenario())


@pytest.mark.parametrize('message', ['{"not": "jev"}', '{broken json', 'Data:\nsome text', '```jev\nnot closed'])
def test_chat_model_messages_never_reach_the_jev_parser(service, monkeypatch, message):
    """Ordinary text shaped like a Jev request stays an ordinary chat message."""
    chat = AsyncMock(return_value='Ordinary answer')
    jev = AsyncMock()
    monkeypatch.setattr(service, '_openrouter_chat', chat)
    monkeypatch.setattr(service, '_openrouter_jev_chat', jev)
    async def scenario():
        cid = await service.create_conversation(OWNER, 'openrouter', MODEL)
        await service.start_turn(OWNER, cid, message)
        await finished(service, cid)
        assert service.conversations[cid].last_error == ''
    asyncio.run(scenario())
    chat.assert_awaited_once()
    jev.assert_not_called()


@pytest.mark.parametrize('message,expected', [('{broken json', 'valid JSON'), ('Data:\nno question', 'Yes/No or Score')])
def test_jev_model_still_parses_structured_requests(service, monkeypatch, message, expected):
    """The same text selected for Jev keeps its structured validation."""
    async def scenario():
        cid = await service.create_conversation(OWNER, 'openrouter', JEV_MODEL)
        with pytest.raises(AIChatError, match=expected):
            await service.start_turn(OWNER, cid, message)
    asyncio.run(scenario())


def test_jev_turn_uses_the_decision_adapter_only(service, monkeypatch):
    """Jev never enters the chat transport."""
    chat = AsyncMock()
    jev = AsyncMock(return_value='Jev chooses')
    monkeypatch.setattr(service, '_openrouter_chat', chat)
    monkeypatch.setattr(service, '_openrouter_jev_chat', jev)
    async def scenario():
        cid = await service.create_conversation(OWNER, 'openrouter', JEV_MODEL)
        await service.start_turn(OWNER, cid, 'Hello')
        await finished(service, cid)
    asyncio.run(scenario())
    jev.assert_awaited_once()
    chat.assert_not_called()


def test_chat_adapter_refuses_decision_models(service):
    """A direct call cannot send Jev through ordinary chat."""
    with pytest.raises(AIChatError, match='decision models'):
        asyncio.run(service._openrouter_chat(OWNER, JEV_MODEL, [{'role': 'user', 'content': 'Hi'}]))


@pytest.mark.parametrize('provider,model', [('openrouter', MODEL), ('deepseek', JEV_MODEL)])
def test_transfer_preview_requires_the_server_side_decision_mapping(service, provider, model):
    """A client cannot obtain a Jev preview for a chat connection or model."""
    async def scenario():
        cid = await service.create_conversation(OWNER, 'openrouter', JEV_MODEL)
        with pytest.raises(AIChatError, match='Select Jev'):
            await service.preview_jev_transfer(OWNER, cid, '{"questions":{}}', model, provider)
    asyncio.run(scenario())


def test_preview_token_is_rejected_for_a_chat_turn(service, monkeypatch):
    """A reviewed Jev preview cannot authorize a turn on the chat adapter."""
    async def scenario():
        cid = await service.create_conversation(OWNER, 'openrouter', MODEL)
        with pytest.raises(AIChatError, match='no longer matches'):
            await service.start_turn(OWNER, cid, 'Hello', jev_preview_id='c' * 32)
    asyncio.run(scenario())


# --- Reasoning metadata ---

def test_reasoning_null_means_all_gateway_efforts():
    """The documented null allowlist is distinct from a missing field."""
    row = projected(reasoning={'mandatory': False, 'supported_efforts': None})
    assert [item['id'] for item in row['reasoning_variants']] == ['none', 'max', 'xhigh', 'high', 'medium', 'low', 'minimal']
    assert row['reasoning_variants'][0]['label'] == 'Off'


def test_reasoning_missing_efforts_offers_only_the_default():
    """No advertised efforts means PBGui never invents selectable values."""
    assert projected(reasoning={'mandatory': False})['reasoning_variants'] == []
    assert projected(reasoning=None)['reasoning_variants'] == []
    assert projected(reasoning=None)['reasoning'] is True


@pytest.mark.parametrize('efforts', [None, ['high', 'none'], ['high']])
def test_mandatory_reasoning_never_offers_off(efforts):
    """A model that cannot disable reasoning has no Off choice."""
    row = projected(reasoning={'mandatory': True, 'default_enabled': True, 'default_effort': 'high', 'supported_efforts': efforts})
    assert 'none' not in [item['id'] for item in row['reasoning_variants']]
    assert row['reasoning_mandatory'] is True and row['default_effort'] == 'high'


@pytest.mark.parametrize('metadata_value,expected', [
    ({'mandatory': False, 'default_enabled': False, 'default_effort': 'medium', 'supported_efforts': ['medium']}, 'off'),
    ({'mandatory': False, 'default_enabled': True, 'default_effort': 'medium', 'supported_efforts': ['medium']}, 'medium'),
    ({'mandatory': False, 'default_effort': 'none', 'supported_efforts': ['none', 'low']}, 'off'),
    ({'mandatory': True, 'default_effort': 'none', 'supported_efforts': ['low']}, ''),
    ({'mandatory': False, 'supported_efforts': ['low']}, ''),
])
def test_reasoning_default_reflects_enabled_state_and_effort(metadata_value, expected):
    """Standard shows what the model will really do by default."""
    assert projected(reasoning=metadata_value)['default_effort'] == expected


def test_reasoning_parameter_is_required_for_selectable_efforts():
    """Efforts stay unavailable when the model does not accept the parameter."""
    row = projected(supported_parameters=['max_tokens'], reasoning={'mandatory': False, 'supported_efforts': ['low']})
    assert row['reasoning_variants'] == [] and row['reasoning'] is False


def test_explicit_effort_requires_a_provider_that_honors_it(service, monkeypatch):
    """A chosen setting is sent with require_parameters and never silently dropped."""
    http = use_http(monkeypatch, service, HTTP(reply({'content': 'Done'})))
    assert asyncio.run(service._openrouter_chat(OWNER, MODEL, [{'role': 'user', 'content': 'Hi'}], effort='low')) == 'Done'
    body = http.sent[0][1]['json']
    assert body['reasoning'] == {'effort': 'low'} and 'reasoning_effort' not in body
    assert body['provider'] == {'data_collection': 'deny', 'require_parameters': True}


def test_default_request_does_not_force_parameters(service, monkeypatch):
    """Without tools or an explicit setting, routing keeps every eligible provider."""
    http = use_http(monkeypatch, service, HTTP(reply({'content': 'Done'})))
    asyncio.run(service._openrouter_chat(OWNER, MODEL, [{'role': 'user', 'content': 'Hi'}]))
    body = http.sent[0][1]['json']
    assert body['provider'] == {'data_collection': 'deny', 'require_parameters': False}
    assert 'reasoning' not in body and 'models' not in body and 'route' not in body


def test_unadvertised_effort_is_rejected_before_any_request(service, monkeypatch):
    """An unsupported saved effort fails explicitly."""
    http = use_http(monkeypatch, service, HTTP())
    with pytest.raises(AIChatError, match='reasoning variant'):
        asyncio.run(service._openrouter_chat(OWNER, MODEL, [{'role': 'user', 'content': 'Hi'}], effort='max'))
    assert http.sent == []


# --- Binding tool approval ---

def test_no_probe_means_no_tools_are_sent_or_run(service, monkeypatch):
    """Without an approval the request has no tools and a tool reply is rejected."""
    http = use_http(monkeypatch, service, HTTP(reply(tool_call(), finish='tool_calls')))
    dispatched = AsyncMock(return_value={'success': True})
    monkeypatch.setattr(service, '_agent_capability_result', dispatched)
    async def scenario():
        cid = await service.create_conversation(OWNER, 'openrouter', MODEL)
        with pytest.raises(AIChatError, match='tools are disabled'):
            await service.chat(OWNER, 'openrouter', MODEL, 'Hello', cid)
    asyncio.run(scenario())
    assert 'tools' not in http.sent[0][1]['json']
    dispatched.assert_not_called()


def test_approved_model_uses_tools_and_keeps_routing(service, monkeypatch):
    """A validated model receives tools on every request of its loop."""
    approve(service)
    http = use_http(monkeypatch, service, HTTP(reply(tool_call(), finish='tool_calls'), reply({'content': 'Ready'})))
    dispatched = AsyncMock(return_value={'success': True})
    monkeypatch.setattr(service, '_agent_capability_result', dispatched)
    async def scenario():
        cid = await service.create_conversation(OWNER, 'openrouter', MODEL)
        return await service.chat(OWNER, 'openrouter', MODEL, 'Hello', cid)
    assert asyncio.run(scenario())['reply'] == 'Ready'
    dispatched.assert_awaited_once()
    for _, kwargs in http.sent:
        assert kwargs['json']['tools'] and kwargs['json']['provider'] == {'data_collection': 'deny', 'require_parameters': True}


def test_approval_is_per_owner_key_and_model_version(service):
    """Another owner, key or canonical model version has no approval."""
    approve(service)
    service.credentials.save_openrouter_key(OTHER, KEY)
    async def tools(owner):
        rows = await service.models(owner, 'openrouter')
        return next(row for row in rows if row['id'] == MODEL)
    assert asyncio.run(tools(OWNER))['tools'] is True
    assert asyncio.run(tools(OWNER))['tool_status'] == 'validated'
    assert asyncio.run(tools(OTHER))['tools'] is False
    assert service.credentials.load_openrouter_tool_approvals(OWNER, NEW_KEY) == {}
    service.catalog_rows[0]['canonical_slug'] = 'acme/chat-1-20261201'
    service.openrouter_catalog.clear()
    row = asyncio.run(tools(OWNER))
    assert row['tools'] is False and row['tool_status'] == 'unvalidated'


def test_model_version_change_blocks_tools_on_the_server(service, monkeypatch):
    """A stale approval cannot pass the runtime gate even with tools in metadata."""
    approve(service, slug='acme/chat-1-20200101')
    with pytest.raises(AIChatError, match='not validated'):
        service._require_openrouter_tools(OWNER, metadata(), KEY)


def test_key_replacement_and_disconnect_invalidate_approvals(service, monkeypatch):
    """Connection changes remove approvals for every model."""
    approve(service)
    http = use_http(monkeypatch, service, HTTP(Response({'data': {'label': 'key'}})))
    asyncio.run(service.connect_openrouter(OWNER, NEW_KEY))
    assert http.sent[0][1]['headers']['Authorization'] == f'Bearer {NEW_KEY}'
    assert not service.credentials._openrouter_tools_path(OWNER).exists()
    assert service.credentials.load_openrouter_tool_approvals(OWNER, KEY) == {}
    approve(service, key=NEW_KEY)
    assert service.credentials._openrouter_tools_path(OWNER).stat().st_mode & 0o777 == 0o600
    asyncio.run(service.disconnect_openrouter(OWNER))
    assert not service.credentials._openrouter_tools_path(OWNER).exists()


def test_approval_file_stores_only_a_key_fingerprint(service):
    """The key itself never reaches the approval file."""
    approve(service)
    raw = service.credentials._openrouter_tools_path(OWNER).read_text()
    assert KEY not in raw and service.credentials._openrouter_key_fingerprint(KEY) in raw


def test_key_swap_during_a_turn_stops_before_the_next_tool(service, monkeypatch):
    """An approval removed mid-turn blocks execution of the pending tool call."""
    approve(service)
    use_http(monkeypatch, service, HTTP(reply(tool_call(), finish='tool_calls')))
    dispatched = AsyncMock(return_value={'success': True})
    monkeypatch.setattr(service, '_agent_capability_result', dispatched)
    original = chat_transport.request
    async def swapped(*args, **kwargs):
        """Replace the key after the provider call has started."""
        result = await original(*args, **kwargs)
        service.credentials.delete_openrouter_tool_approvals(OWNER)
        return result
    monkeypatch.setattr('ai_chat.openrouter_request', swapped)
    async def scenario():
        cid = await service.create_conversation(OWNER, 'openrouter', MODEL)
        with pytest.raises(AIChatError, match='not validated'):
            await service.chat(OWNER, 'openrouter', MODEL, 'Hello', cid)
    asyncio.run(scenario())
    dispatched.assert_not_called()


def test_probe_success_records_approval_with_chat_routing(service, monkeypatch):
    """The probe proves a roundtrip with the same routing and tools shape as chat."""
    http = use_http(monkeypatch, service, probe_responses(HTTP()))
    result = asyncio.run(service.validate_openrouter_tools(OWNER, MODEL))
    assert result['status'] == 'validated'
    first, second = (item[1]['json'] for item in http.sent)
    assert first['provider'] == second['provider'] == {'data_collection': 'deny', 'require_parameters': True}
    assert [item['function']['name'] for item in first['tools']] == [chat_transport.PROBE_TOOL]
    assert 'tools' in second and second['tool_choice'] == 'none'
    assert second['messages'][-1]['role'] == 'tool' and second['messages'][-2]['tool_calls']
    capability_names = {item['function']['name'] for item in service.capabilities.chat_completion_tools()}
    assert chat_transport.PROBE_TOOL not in capability_names
    assert service.credentials.load_openrouter_tool_approvals(OWNER, KEY)[MODEL]['canonical_slug'] == SLUG
    assert service.conversations == {}


def test_probe_forwards_the_selected_effort(service, monkeypatch):
    """An explicit effort is probed exactly as it will later be used."""
    http = use_http(monkeypatch, service, probe_responses(HTTP()))
    asyncio.run(service.validate_openrouter_tools(OWNER, MODEL, 'low'))
    assert all(item[1]['json']['reasoning'] == {'effort': 'low'} for item in http.sent)


@pytest.mark.parametrize('first', [
    {'content': 'I will not call it'},
    tool_call('{"nonce":"wrong"}', 'probe-1', chat_transport.PROBE_TOOL),
    tool_call('{"nonce":', 'probe-1', chat_transport.PROBE_TOOL),
    tool_call('{}', 'probe-1', 'other_tool'),
])
def test_failed_probe_revokes_an_existing_approval(service, monkeypatch, first):
    """A failed compatibility check leaves the model chat only."""
    approve(service)
    finish = 'tool_calls' if 'tool_calls' in first else 'stop'
    use_http(monkeypatch, service, HTTP(reply(first, finish=finish)))
    result = asyncio.run(service.validate_openrouter_tools(OWNER, MODEL))
    assert result['status'] == 'failed'
    assert service.credentials.load_openrouter_tool_approvals(OWNER, KEY) == {}


def test_key_swap_during_probe_never_saves_approval(service, monkeypatch):
    """A probe for the old key cannot approve the new connection."""
    async def probe(*args, **kwargs):
        """Replace the owner's key while the synthetic request is in flight."""
        service.credentials.save_openrouter_key(OWNER, NEW_KEY)
    monkeypatch.setattr('ai_chat.run_openrouter_tool_probe', probe)
    use_http(monkeypatch, service, HTTP())
    result = asyncio.run(service.validate_openrouter_tools(OWNER, MODEL))
    assert result['status'] == 'failed' and 'changed' in result['message']
    assert service.credentials.load_openrouter_tool_approvals(OWNER, NEW_KEY) == {}
    assert not service.credentials._openrouter_tools_path(OWNER).exists()


def test_privacy_permission_is_explicit_scoped_persistent_and_revocable(service, monkeypatch):
    """Real requests use consent for only this owner/key/model version, including after reload."""
    http = use_http(monkeypatch, service, HTTP(*(reply({'content': 'OK'}) for _ in range(3))))
    async def scenario():
        await service._openrouter_chat(OWNER, MODEL, [{'role': 'user', 'content': 'Hello'}])
        approve(service)
        await service.set_openrouter_privacy(OWNER, MODEL, True)
        assert not service.credentials.load_openrouter_tool_approvals(OWNER, KEY)
        rows = await service._openrouter_models(OWNER)
        chosen = next(row for row in rows if row['id'] == MODEL)
        assert chosen['data_collection_allowed'] is True
        assert 'training' in chosen['retention']
        assert next(row for row in rows if row['id'] == 'acme/text-only')['data_collection_allowed'] is False
        assert 'data_collection_allowed' not in rows[0]  # Jev is unchanged.
        from ai_chat import AICredentialStore
        restored = AICredentialStore(service.credentials.root)
        assert restored.load_openrouter_privacy(OWNER, KEY) == {MODEL: SLUG}
        assert restored.load_openrouter_privacy(OWNER, NEW_KEY) == {}
        assert restored.load_openrouter_privacy(OTHER, KEY) == {}
        assert service._openrouter_data_collection(OWNER, KEY, metadata(canonical_slug='new-version')) == 'deny'
        assert service._openrouter_data_collection(OWNER, KEY, {'id': MODEL}) == 'deny'
        await service._openrouter_chat(OWNER, MODEL, [{'role': 'user', 'content': 'Hello'}])
        await service.set_openrouter_privacy(OWNER, MODEL, False)
        await service._openrouter_chat(OWNER, MODEL, [{'role': 'user', 'content': 'Hello'}])
    asyncio.run(scenario())
    assert [sent[1]['json']['provider']['data_collection'] for sent in http.sent] == ['deny', 'allow', 'deny']
    path = service.credentials._openrouter_privacy_path(OWNER)
    assert path.stat().st_mode & 0o777 == 0o600
    assert KEY not in path.read_text()


def test_privacy_exceptions_expire_on_connection_changes(service, monkeypatch):
    """Reconnect and disconnect clear model exceptions; other model saves survive a revoke."""
    store = service.credentials
    store.save_openrouter_privacy(OWNER, KEY, MODEL, SLUG, True)
    store.save_openrouter_privacy(OWNER, KEY, 'other/model', 'other/version', True)
    store.save_openrouter_privacy(OWNER, KEY, MODEL, SLUG, False)
    assert store.load_openrouter_privacy(OWNER, KEY) == {'other/model': 'other/version'}
    use_http(monkeypatch, service, HTTP(Response({'data': {}})))
    asyncio.run(service.connect_openrouter(OWNER, NEW_KEY))
    assert not store._openrouter_privacy_path(OWNER).exists()
    store.save_openrouter_privacy(OWNER, NEW_KEY, MODEL, SLUG, True)
    asyncio.run(service.disconnect_openrouter(OWNER))
    assert not store._openrouter_privacy_path(OWNER).exists()


def test_privacy_rechecked_between_probe_requests(service, monkeypatch):
    """Revoking consent between probe rounds forces the next request back to deny."""
    service.credentials.save_openrouter_privacy(OWNER, KEY, MODEL, SLUG, True)
    http = use_http(monkeypatch, service, probe_responses(HTTP()))
    original = chat_transport.request
    async def revoke_after_first(*args, **kwargs):
        """Model another authenticated tab revoking consent during a provider response."""
        result = await original(*args, **kwargs)
        service.credentials.save_openrouter_privacy(OWNER, KEY, MODEL, SLUG, False)
        return result
    monkeypatch.setattr(chat_transport, 'request', revoke_after_first)
    result = asyncio.run(service.validate_openrouter_tools(OWNER, MODEL))
    assert result['status'] == 'validated'
    assert [sent[1]['json']['provider']['data_collection'] for sent in http.sent] == ['allow', 'deny']


def test_privacy_permission_cannot_be_forged_in_request_body(service, monkeypatch):
    """Provider routing stays server-owned even with injected body or display metadata."""
    http = HTTP(reply({'content': 'OK'}))
    body = {'model': MODEL, 'messages': [], 'provider': {'data_collection': 'allow'}}
    asyncio.run(chat_transport.request(http, KEY, body, {**metadata(), 'data_collection_allowed': True}))
    assert http.sent[0][1]['json']['provider']['data_collection'] == 'deny'
    with pytest.raises(AIChatError, match='chat models'):
        asyncio.run(service.set_openrouter_privacy(OWNER, JEV_MODEL, True))


def test_privacy_api_is_owner_bound_strict_and_no_store(monkeypatch):
    """The dedicated authenticated endpoint accepts an explicit boolean and no owner input."""
    import api.ai as api
    from ai_chat import owner_key
    from pydantic import ValidationError
    save = AsyncMock(return_value={'model': MODEL, 'data_collection_allowed': True})
    monkeypatch.setattr(api, 'get_ai_chat_service', lambda: SimpleNamespace(set_openrouter_privacy=save))
    body = api.OpenRouterPrivacyRequest(model=MODEL, allow_data_collection=True)
    response = asyncio.run(api.save_openrouter_privacy(body, SimpleNamespace(user_id='privacy-user')))
    save.assert_awaited_once_with(owner_key('privacy-user'), MODEL, True)
    assert response.headers['cache-control'] == 'no-store'
    for extra in ({'allow_data_collection': 'true'}, {'owner': OTHER}, {'model': 'bad\nmodel'}):
        with pytest.raises(ValidationError):
            api.OpenRouterPrivacyRequest.model_validate({'model': MODEL, 'allow_data_collection': True, **extra})


def test_model_without_tool_support_is_never_probed(service, monkeypatch):
    """Unsupported models stay chat only without spending credits."""
    http = use_http(monkeypatch, service, HTTP())
    result = asyncio.run(service.validate_openrouter_tools(OWNER, 'acme/text-only'))
    assert result['status'] == 'unsupported' and http.sent == []


def test_setup_test_reports_probe_without_exposing_pbgui_tools(service, monkeypatch):
    """The isolated test creates no conversation and enables no PBGui capability."""
    http = use_http(monkeypatch, service, probe_responses(HTTP(reply({'content': 'PBGui connection OK'}))))
    selection = ai_setup.ModelTestRequest(provider='openrouter', model=MODEL)
    result = asyncio.run(ai_setup.test_model(service, OWNER, selection, SimpleNamespace(is_disconnected=AsyncMock(return_value=False))))
    assert result['tool_probe']['status'] == 'validated'
    assert result['tools_enabled'] is False and result['conversation_created'] is False
    assert 'tools' not in http.sent[0][1]['json'] and service.conversations == {}
    assert not service.active_tasks


# --- Stream completion ---

def run_stream(service, monkeypatch, *responses, history=None):
    """Run one conversation turn on a validated model through the real transport."""
    approve(service)
    http = use_http(monkeypatch, service, HTTP(*responses))
    dispatched = AsyncMock(return_value={'success': True})
    monkeypatch.setattr(service, '_agent_capability_result', dispatched)
    async def scenario():
        cid = await service.create_conversation(OWNER, 'openrouter', MODEL)
        return await service.chat(OWNER, 'openrouter', MODEL, 'Hello', cid)
    return asyncio.run, scenario, http, dispatched


def test_stream_reads_past_finish_to_the_usage_chunk(service, monkeypatch):
    """The repeated finish reason and usage arrive before the reply is accepted."""
    response = reply({'content': 'Complete'})
    http = use_http(monkeypatch, service, HTTP(response))
    result = asyncio.run(chat_transport.request(http, KEY, {'model': MODEL, 'messages': [], 'max_tokens': 10}, metadata()))
    assert result['usage'] == {'prompt_tokens': 20, 'completion_tokens': 10}
    assert result['choices'][0]['message']['content'] == 'Complete'
    assert http.sent[0][1]['json']['stream'] is True and 'stream_options' not in http.sent[0][1]['json']
    assert http.sent[0][0] == 'https://openrouter.ai/api/v1/chat/completions'
    assert http.sent[0][1]['allow_redirects'] is False


@pytest.mark.parametrize('kwargs', [{'done': False}, {'done': False, 'usage': False}, {'finish': None}])
def test_truncated_stream_is_rejected(service, monkeypatch, kwargs):
    """A stream without its final event is incomplete even with a finish reason."""
    http = HTTP(reply({'content': 'Partial'}, **kwargs))
    with pytest.raises(AIChatError, match='stream ended before completing'):
        asyncio.run(chat_transport.request(http, KEY, {'model': MODEL, 'messages': [], 'max_tokens': 10}, metadata()))


@pytest.mark.parametrize('failure', [
    {'error': {'code': 502, 'message': 'SECRET upstream text', 'metadata': {'error_type': 'provider_unavailable'}}},
    delta({'content': ''}, 'error'),
])
def test_error_after_text_is_an_explicit_incomplete_response(failure):
    """Partial output is discarded with a message that says so."""
    raw = event(delta({'content': 'Half an'})) + event(failure) + b'data: [DONE]\n\n'
    http = HTTP(Response(raw, stream=True))
    with pytest.raises(AIChatError, match='after partial output') as caught:
        asyncio.run(chat_transport.request(http, KEY, {'model': MODEL, 'messages': [], 'max_tokens': 10}, metadata()))
    assert 'incomplete response was discarded' in str(caught.value) and 'SECRET' not in str(caught.value)


def test_error_after_finish_rejects_the_finished_text():
    """A failure in the trailing chunk still rejects the reply."""
    raw = event(delta({'content': 'Text'})) + event(delta({'content': ''}, 'stop')) + event({'error': {'code': 503}}) + b'data: [DONE]\n\n'
    with pytest.raises(AIChatError, match='after partial output'):
        asyncio.run(chat_transport.request(HTTP(Response(raw, stream=True)), KEY, {'model': MODEL, 'messages': [], 'max_tokens': 10}, metadata()))


@pytest.mark.parametrize('tail', [
    delta({'content': 'late'}, None),
    delta({'content': ''}, 'length'),
])
def test_output_after_finish_is_rejected(tail):
    """A conflicting trailing chunk cannot silently change a finished stream."""
    raw = event(delta({'content': 'Text'})) + event(delta({'content': ''}, 'stop')) + event(tail) + b'data: [DONE]\n\n'
    with pytest.raises(AIChatError, match='continued after finishing'):
        asyncio.run(chat_transport.request(HTTP(Response(raw, stream=True)), KEY, {'model': MODEL, 'messages': [], 'max_tokens': 10}, metadata()))


def test_stream_abort_during_tool_call_executes_nothing(service, monkeypatch):
    """Tool fragments in a broken stream never reach execution."""
    run, scenario, _http, dispatched = run_stream(service, monkeypatch, reply(tool_call('{"version":'), finish=None, done=False))
    with pytest.raises(AIChatError):
        run(scenario())
    dispatched.assert_not_called()


def test_error_chunk_after_tool_fragments_executes_nothing(service, monkeypatch):
    """A complete-looking call followed by a failure is never executed."""
    raw = event(delta(tool_call())) + event({'error': {'code': 502}}) + b'data: [DONE]\n\n'
    run, scenario, _http, dispatched = run_stream(service, monkeypatch, Response(raw, stream=True))
    with pytest.raises(AIChatError, match='no PBGui tool was run'):
        run(scenario())
    dispatched.assert_not_called()


def test_one_invalid_call_rejects_the_whole_set(service, monkeypatch):
    """The first valid call does not run when a later call is invalid."""
    response = reply(tool_call(), tool_call('{"version":', 'call-2', index=1), finish='tool_calls')
    run, scenario, _http, dispatched = run_stream(service, monkeypatch, response)
    with pytest.raises(AIChatError, match='invalid capability arguments'):
        run(scenario())
    dispatched.assert_not_called()


@pytest.mark.parametrize('call', [
    tool_call('{}', name='not_offered_tool'),
    tool_call('[]'),
    tool_call('{}', call_id=''),
])
def test_unoffered_or_malformed_calls_run_nothing(service, monkeypatch, call):
    """Only offered names with object arguments and IDs may execute."""
    run, scenario, _http, dispatched = run_stream(service, monkeypatch, reply(call, finish='tool_calls'))
    with pytest.raises(AIChatError, match='no PBGui tool was run'):
        run(scenario())
    dispatched.assert_not_called()


@pytest.mark.parametrize('deltas,finish', [
    ([tool_call()], 'stop'),
    ([{'content': 'Text'}], 'tool_calls'),
])
def test_inconsistent_finish_runs_no_tool(service, monkeypatch, deltas, finish):
    """Tool calls require the matching finish reason."""
    run, scenario, _http, dispatched = run_stream(service, monkeypatch, reply(*deltas, finish=finish))
    with pytest.raises(AIChatError, match='inconsistent tool response'):
        run(scenario())
    dispatched.assert_not_called()


@pytest.mark.parametrize('finish,expected', [
    ('length', 'output limit'), ('content_filter', 'content filter'), ('unknown', 'interrupted'),
])
def test_incomplete_finish_reasons_are_rejected(finish, expected):
    """Only stop and tool_calls complete a response."""
    with pytest.raises(AIChatError, match=expected):
        asyncio.run(chat_transport.request(HTTP(reply({'content': 'Text'}, finish=finish)), KEY,
                                           {'model': MODEL, 'messages': [], 'max_tokens': 10}, metadata()))


def test_different_served_model_is_rejected():
    """A response from another model is never accepted silently."""
    with pytest.raises(AIChatError, match='different model'):
        asyncio.run(chat_transport.request(HTTP(reply({'content': 'Text'}, model='other/model')), KEY,
                                           {'model': MODEL, 'messages': [], 'max_tokens': 10}, metadata()))


@pytest.mark.parametrize('served', [MODEL, SLUG, MODEL + ':free'])
def test_served_model_accepts_id_slug_and_variant(served):
    """The same model may be reported by its id, canonical slug or variant."""
    result = asyncio.run(chat_transport.request(HTTP(reply({'content': 'Text'}, model=served)), KEY,
                                                {'model': MODEL, 'messages': [], 'max_tokens': 10}, metadata()))
    assert result['model'] == served


def test_reasoning_details_keep_ids_indexes_and_signatures():
    """Fragments assemble in order and retain every field needed for a follow-up."""
    target = []
    chat_transport.merge_reasoning_details(target, [{'type': 'reasoning.text', 'text': 'Need ', 'format': 'anthropic-claude-v1', 'index': 0, 'id': 'r1'}])
    chat_transport.merge_reasoning_details(target, [{'type': 'reasoning.text', 'text': 'data', 'index': 0}])
    chat_transport.merge_reasoning_details(target, [{'type': 'reasoning.text', 'text': '', 'signature': 'sig-1', 'index': 0}])
    chat_transport.merge_reasoning_details(target, [{'type': 'reasoning.encrypted', 'data': 'AA', 'id': 'enc-1', 'format': 'x', 'index': 1}])
    chat_transport.merge_reasoning_details(target, [{'type': 'reasoning.encrypted', 'data': 'BB', 'index': 1}])
    chat_transport.merge_reasoning_details(target, [{'type': 'reasoning.summary', 'summary': 'Short', 'index': 2}])
    assert target == [
        {'type': 'reasoning.text', 'text': 'Need data', 'format': 'anthropic-claude-v1', 'index': 0, 'id': 'r1', 'signature': 'sig-1'},
        {'type': 'reasoning.encrypted', 'data': 'AABB', 'id': 'enc-1', 'format': 'x', 'index': 1},
        {'type': 'reasoning.summary', 'summary': 'Short', 'index': 2},
    ]


def test_same_index_with_a_new_id_starts_a_new_detail():
    """Distinct reasoning blocks are never merged."""
    target = []
    chat_transport.merge_reasoning_details(target, [{'type': 'reasoning.text', 'text': 'A', 'index': 0, 'id': 'one'}])
    chat_transport.merge_reasoning_details(target, [{'type': 'reasoning.text', 'text': 'B', 'index': 0, 'id': 'two'}])
    assert [item['text'] for item in target] == ['A', 'B']


def test_reasoning_details_are_bounded():
    """Reasoning metadata cannot grow without limit."""
    target = []
    with pytest.raises(AIChatError, match='too large'):
        chat_transport.merge_reasoning_details(target, [{'type': 'reasoning.text', 'text': 'x' * 1_000_001, 'index': 0}])


def test_streamed_reasoning_details_return_in_the_tool_follow_up(service, monkeypatch):
    """The merged sequence is replayed unchanged and never reaches stored history."""
    first = reply({'reasoning_details': [{'type': 'reasoning.text', 'text': 'Think', 'index': 0, 'id': 'r1'}]},
                  {'reasoning_details': [{'type': 'reasoning.text', 'text': '', 'signature': 'sig', 'index': 0}]},
                  tool_call(), finish='tool_calls')
    second = reply({'content': 'Ready'})
    approve(service)
    http = use_http(monkeypatch, service, HTTP(first, second))
    monkeypatch.setattr(service, '_agent_capability_result', AsyncMock(return_value={'success': True}))
    async def scenario():
        cid = await service.create_conversation(OWNER, 'openrouter', MODEL)
        result = await service.chat(OWNER, 'openrouter', MODEL, 'Hello', cid)
        return cid, result
    cid, result = asyncio.run(scenario())
    assert result['reply'] == 'Ready'
    assistant = next(item for item in http.sent[1][1]['json']['messages'] if item.get('tool_calls'))
    assert assistant['reasoning_details'] == [{'type': 'reasoning.text', 'text': 'Think', 'index': 0, 'id': 'r1', 'signature': 'sig'}]
    assert assistant['content'] is None and 'reasoning_content' not in assistant
    assert 'reasoning_details' not in json.dumps(service.conversations[cid].messages)
    assert cid not in service.openrouter_live


def test_plain_reasoning_text_returns_in_the_tool_follow_up(service, monkeypatch):
    """Models without details keep their reasoning string for the required follow-up."""
    first = reply({'reasoning': 'Plain thought'}, tool_call(), finish='tool_calls')
    approve(service)
    http = use_http(monkeypatch, service, HTTP(first, reply({'content': 'Ready'})))
    monkeypatch.setattr(service, '_agent_capability_result', AsyncMock(return_value={'success': True}))
    async def scenario():
        cid = await service.create_conversation(OWNER, 'openrouter', MODEL)
        await service.chat(OWNER, 'openrouter', MODEL, 'Hello', cid)
    asyncio.run(scenario())
    assistant = next(item for item in http.sent[1][1]['json']['messages'] if item.get('tool_calls'))
    assert assistant['reasoning'] == 'Plain thought'


def test_stop_cancels_the_stream_and_stores_no_partial_reply(service, monkeypatch):
    """Stop closes the response, keeps no reply and clears live output."""
    class Blocking:
        """Stream one fragment and then wait until cancelled."""
        def __init__(self):
            self.chunks = [event(delta({'content': 'Partial'}))]
            self.reached = asyncio.Event()
        async def readany(self):
            if self.chunks:
                return self.chunks.pop(0)
            self.reached.set()
            await asyncio.Event().wait()
    async def scenario():
        response = Response(b'', stream=True)
        response.content = Blocking()
        use_http(monkeypatch, service, HTTP(response))
        cid = await service.create_conversation(OWNER, 'openrouter', MODEL)
        await service.start_turn(OWNER, cid, 'Hello')
        await asyncio.wait_for(response.content.reached.wait(), 5)
        projected_live = service._conversation_projection(service.conversations[cid], include_messages=True)
        assert projected_live['streaming_messages'][0]['content'] == 'Partial'
        await service.cancel(OWNER, cid)
        await finished(service, cid)
        assert response.closed
        assert not any(item.get('role') == 'assistant' for item in service.conversations[cid].messages)
        assert cid not in service.openrouter_live
    asyncio.run(scenario())


@pytest.mark.parametrize('status,error_type,expected', [
    (401, None, 'rejected the API key'), (402, None, 'credits are insufficient'),
    (403, None, 'refused this request'), (404, None, 'available endpoint'),
    (429, None, 'rate limit'), (502, None, 'provider is unavailable'),
    (503, None, 'cannot serve this model right now'), (400, 'provider_overloaded', 'overloaded'),
])
def test_provider_errors_are_explicit_and_never_echo_upstream_text(status, error_type, expected):
    """Pre-stream failures use fixed text; flagged input cannot reach the browser."""
    body = {'error': {'code': status, 'message': 'flagged SECRET prompt', 'metadata': {'error_type': error_type, 'flagged_input': 'SECRET'}}}
    http = HTTP(Response(body, status=status))
    with pytest.raises(AIChatError, match=expected) as caught:
        asyncio.run(chat_transport.request(http, KEY, {'model': MODEL, 'messages': [], 'max_tokens': 10}, metadata()))
    assert 'SECRET' not in str(caught.value) and len(http.sent) == 1


@pytest.mark.parametrize('status', [404, 503])
def test_availability_errors_explain_uncertainty_and_next_action(status):
    """Availability failures explain possible restrictions without inventing a cause."""
    raw = json.dumps({'error': {'message': 'SECRET prompt', 'code': status}}).encode()
    message = chat_transport.status_message(status, raw)
    assert f'HTTP {status}' in message
    assert 'exact cause was not confirmed' in message
    assert 'tools' in message and 'reasoning' in message
    assert 'privacy policy' in message
    assert 'another model' in message
    assert 'SECRET' not in message


@pytest.mark.parametrize('message,metadata,expected', [
    ('No endpoints found matching your data policy. Enable prompt training here: SECRET', {}, 'privacy/data-policy filters'),
    ('No endpoints available matching your guardrail restrictions and data policy.', {}, 'privacy/data-policy filters'),
    ('No endpoints found matching your data policy (Free model publication).', {}, 'privacy/data-policy filters'),
    ('No endpoints found matching your data policy (Zero data retention).', {}, 'privacy/data-policy filters'),
    ('No endpoints found that support tool use. SECRET', {}, 'supports the requested tool calls'),
    ('No endpoints found that support all the requested parameters.', {}, 'supports all requested parameters'),
    ('No endpoints for this model found.', {}, 'model has no available endpoints'),
    ('No endpoints found for this model.', {}, 'model has no available endpoints'),
    ('SECRET', {'failed_routing_step': 'Filter by Tool Compatibility'}, 'supports the requested tool calls'),
    ('SECRET', {'failed_routing_step': 'Filter by Data Policy'}, 'privacy/data-policy filters'),
    ('SECRET', {'failed_routing_step': 'unknown SECRET'}, 'exact cause was not confirmed'),
    (None, None, 'exact cause was not confirmed'),
])
@pytest.mark.parametrize('response_kind', ['http', 'json', 'stream'])
def test_routing_diagnostics_use_reported_cause_without_echoing_text(message, metadata, expected, response_kind):
    """All response paths classify actual gateway diagnostics, retaining the redaction boundary."""
    payload = {'error': {'code': 404, 'message': message, 'metadata': metadata}}
    if response_kind == 'http':
        result = chat_transport.status_message(404, json.dumps(payload).encode())
    else:
        with pytest.raises(AIChatError) as caught:
            if response_kind == 'json':
                chat_transport._completion_from_json(payload)
            else:
                chat_transport._CompletionStream().prepare(payload)
        result = str(caught.value)
    assert expected in result
    assert 'SECRET' not in result
    if expected == 'privacy/data-policy filters':
        assert 'https://openrouter.ai/settings/privacy' in result
        assert 'Account restrictions still apply' in result


def test_error_in_a_success_response_is_reported():
    """HTTP 200 can still carry a provider error body."""
    with pytest.raises(AIChatError, match='rate limit'):
        asyncio.run(chat_transport.request(HTTP(Response({'error': {'code': 429}})), KEY,
                                           {'model': MODEL, 'messages': [], 'max_tokens': 10}, metadata()))


def test_malformed_json_reasoning_is_a_safe_provider_error():
    """Invalid reasoning metadata in a non-stream reply never raises a raw error."""
    payload = {'choices': [{'finish_reason': 'stop', 'message': {'content': 'Text', 'reasoning_details': 'bad'}}]}
    with pytest.raises(AIChatError, match='invalid response'):
        asyncio.run(chat_transport.request(HTTP(Response(payload)), KEY, {'model': MODEL, 'messages': [], 'max_tokens': 10}, metadata()))


def test_json_response_keeps_reasoning_and_uses_the_same_validation():
    """A non-stream reply has the same normalized shape as a finished stream."""
    payload = {'model': MODEL, 'choices': [{'finish_reason': 'stop', 'message': {
        'content': 'Text', 'reasoning': 'Thought', 'reasoning_details': [{'type': 'reasoning.text', 'text': 'Thought', 'index': 0}]}}]}
    result = asyncio.run(chat_transport.request(HTTP(Response(payload)), KEY, {'model': MODEL, 'messages': [], 'max_tokens': 10}, metadata()))
    message = result['choices'][0]['message']
    assert message['reasoning_content'] == 'Thought' and message['reasoning_details'][0]['text'] == 'Thought'


# --- Existing limits ---

def test_output_budget_uses_the_shared_cap_and_context(service):
    """The adapter keeps the 32,768 token cap and subtracts input from known context."""
    body = {'model': MODEL, 'messages': [{'role': 'user', 'content': 'x' * 100}]}
    assert AIChatService._apply_opencode_output_budget(body, 'chat', metadata()) == 32768
    small = {**metadata(), 'context': 5000}
    assert AIChatService._apply_opencode_output_budget(dict(body), 'chat', small) < 5000


def test_inline_images_are_reserved_as_tokens_not_base64(service):
    """Large inline images do not exhaust the output budget by their byte size."""
    body = {'model': MODEL, 'messages': [{'role': 'user', 'content': [
        {'type': 'text', 'text': 'Look'}, {'type': 'image_url', 'image_url': {'url': 'data:image/png;base64,' + 'A' * 400_000}}]}]}
    assert AIChatService._apply_opencode_output_budget(body, 'chat', {**metadata(), 'context': 200_000}) == 32768


def test_history_limit_applies_before_any_request(service, monkeypatch):
    """Oversized history fails locally and adds no compaction."""
    http = use_http(monkeypatch, service, HTTP())
    history = [{'role': 'user', 'content': 'x' * 600_000}]
    with pytest.raises(AIChatError, match='exceeds the supported provider payload'):
        asyncio.run(service._openrouter_chat(OWNER, MODEL, history))
    assert http.sent == []


def test_tool_round_limit_ends_with_a_final_answer_request(service, monkeypatch):
    """Tool rounds stay bounded; the last request keeps tools for OpenRouter validation."""
    monkeypatch.setattr(service, '_capability_round_limit', lambda history: 1)
    first = reply(tool_call(), finish='tool_calls')
    second = reply({'content': 'Final'})
    approve(service)
    http = use_http(monkeypatch, service, HTTP(first, second))
    monkeypatch.setattr(service, '_agent_capability_result', AsyncMock(return_value={'success': True}))
    async def scenario():
        cid = await service.create_conversation(OWNER, 'openrouter', MODEL)
        return await service.chat(OWNER, 'openrouter', MODEL, 'Hello', cid)
    assert asyncio.run(scenario())['reply'] == 'Final'
    assert http.sent[1][1]['json']['tool_choice'] == 'none' and http.sent[1][1]['json']['tools']


def test_capability_result_limit_applies(service, monkeypatch):
    """The shared 1 MiB tool-result bound also ends this adapter's turn."""
    first = reply(tool_call(), finish='tool_calls')
    approve(service)
    use_http(monkeypatch, service, HTTP(first))
    monkeypatch.setattr(service, '_agent_capability_result', AsyncMock(return_value={'data': 'x' * (1024 * 1024 + 1)}))
    async def scenario():
        cid = await service.create_conversation(OWNER, 'openrouter', MODEL)
        with pytest.raises(AIChatError, match='results are too large'):
            await service.chat(OWNER, 'openrouter', MODEL, 'Hello', cid)
    asyncio.run(scenario())


def test_turn_deadline_uses_the_direct_api_limit(service, monkeypatch):
    """The agent deadline is finite and names the selected provider."""
    from ai_deepseek import REQUEST_TIMEOUT
    captured = {}
    async def expired(awaitable, timeout):
        """Record the deadline and simulate its expiry."""
        captured['timeout'] = timeout
        awaitable.close()
        raise asyncio.TimeoutError
    monkeypatch.setattr('ai_chat.asyncio.wait_for', expired)
    with pytest.raises(AIChatError, match='OpenRouter agent capability turn timed out'):
        asyncio.run(service._go_chat_completion_agent(OWNER, 'c', 'u', MODEL, KEY, [], '', metadata()))
    assert captured['timeout'] == REQUEST_TIMEOUT


# --- Conversation identity, images and selection ---

def test_image_turns_follow_model_capability(service, monkeypatch):
    """Image input is decided per model, with Jev's rejection unchanged."""
    image = {'media_type': 'image/png', 'data_base64': 'iVBORw0KGgo='}
    chat = AsyncMock(return_value='Seen')
    monkeypatch.setattr(service, '_openrouter_chat', chat)
    async def scenario():
        text_only = await service.create_conversation(OWNER, 'openrouter', 'acme/text-only')
        with pytest.raises(AIChatError, match='does not accept images'):
            await service.start_turn(OWNER, text_only, 'Look', images=[image])
        jev = await service.create_conversation(OWNER, 'openrouter', JEV_MODEL)
        with pytest.raises(AIChatError, match='Jev does not accept images'):
            await service.start_turn(OWNER, jev, 'Look', images=[image])
    asyncio.run(scenario())
    chat.assert_not_called()


def test_selection_and_conversation_identity_survive_reload(service, monkeypatch, tmp_path):
    """A new service restores the selection and the conversation's provider and model."""
    service.save_preferences(OWNER, selection={'provider': 'openrouter', 'model': MODEL, 'effort': 'low'})
    use_http(monkeypatch, service, HTTP(reply({'content': 'Done'})))
    async def scenario():
        cid = await service.create_conversation(OWNER, 'openrouter', MODEL, 'low')
        await service.start_turn(OWNER, cid, 'Hello', effort='low')
        await finished(service, cid)
        return cid
    cid = asyncio.run(scenario())
    restored = AIChatService(tmp_path / 'ai')
    async def reload():
        await restored._ensure_owner_loaded(OWNER)
        return restored.conversations[cid]
    conversation = asyncio.run(reload())
    assert restored.get_preferences(OWNER)['selection']['model'] == MODEL
    assert (conversation.provider, conversation.model, conversation.effort) == ('openrouter', MODEL, 'low')
    assert [item['role'] for item in conversation.messages] == ['user', 'assistant']
    asyncio.run(restored.shutdown())


def test_status_and_models_never_expose_the_key(service):
    """Keys and approvals' fingerprints stay on the server."""
    approve(service)
    rows = asyncio.run(service.models(OWNER, 'openrouter'))
    text = json.dumps(rows)
    assert KEY not in text and service.credentials._openrouter_key_fingerprint(KEY) not in text


# --- Catalog cache ---

def test_catalog_fetches_once_for_concurrent_readers():
    """Concurrent readers share one provider request."""
    calls = []
    async def fetch():
        calls.append(1)
        await asyncio.sleep(0)
        return [{'id': 'a'}]
    async def scenario():
        catalog = chat_transport.ModelCatalog()
        results = await asyncio.gather(*(catalog.get(fetch) for _ in range(5)))
        assert all(item == [{'id': 'a'}] for item in results)
        await catalog.get(fetch)
    asyncio.run(scenario())
    assert len(calls) == 1


def test_catalog_clear_rejects_a_fetch_that_started_before_it():
    """A scan that began before invalidation cannot repopulate the cache."""
    async def scenario():
        catalog = chat_transport.ModelCatalog()
        started, release = asyncio.Event(), asyncio.Event()
        async def fetch():
            started.set()
            await release.wait()
            return ['stale']
        task = asyncio.create_task(catalog.get(fetch))
        await started.wait()
        catalog.clear()
        release.set()
        await task
        assert catalog.rows is None
    asyncio.run(scenario())


def test_catalog_serves_bounded_stale_rows_only_for_failures():
    """Recent rows survive a failed refresh; expired rows do not."""
    async def scenario():
        catalog = chat_transport.ModelCatalog(ttl=0, stale_seconds=100)
        async def good():
            return ['fresh']
        async def bad():
            raise AIChatError('Could not load OpenRouter models')
        await catalog.get(good)
        assert await catalog.get(bad) == ['fresh']
        catalog.stale_seconds = 0
        with pytest.raises(AIChatError):
            await catalog.get(bad)
    asyncio.run(scenario())


def test_catalog_keeps_no_owner_state(service):
    """The shared cache holds provider metadata, never approvals."""
    approve(service)
    asyncio.run(service.models(OWNER, 'openrouter'))
    cached = next(row for row in service.openrouter_catalog.rows if row['id'] == MODEL)
    assert cached['tools'] is False and 'tool_status' not in cached


def test_revoked_approval_between_calls_stops_the_remaining_calls(service, monkeypatch):
    """Revoking approval during one awaited call prevents the next call in its batch."""
    response = reply(tool_call(call_id='first'), tool_call(call_id='second', index=1), finish='tool_calls')
    run, scenario, _http, dispatched = run_stream(service, monkeypatch, response, reply({'content': 'Done'}))
    async def revoke(*args, **kwargs):
        """Model a concurrent disconnect or failed probe while the first tool runs."""
        service.credentials.delete_openrouter_tool_approvals(OWNER)
        return {'success': True}
    dispatched.side_effect = revoke
    with pytest.raises(AIChatError, match='not validated'):
        run(scenario())
    assert dispatched.await_count == 1


def test_replayed_id_rejects_entire_later_round(service, monkeypatch):
    """An ID from an earlier round invalidates a later batch before any of it runs."""
    first = reply(tool_call(call_id='old'), finish='tool_calls')
    second = reply(tool_call(call_id='new'), tool_call(call_id='old', index=1), finish='tool_calls')
    run, scenario, _http, dispatched = run_stream(service, monkeypatch, first, second)
    with pytest.raises(AIChatError, match='repeated a capability call ID'):
        run(scenario())
    assert dispatched.await_count == 1


def test_json_reasoning_blocks_remain_separate_without_indexes():
    """Complete encrypted blocks are not stream fragments and must remain unchanged."""
    details = [{'type': 'reasoning.encrypted', 'data': 'AAAA', 'id': None},
               {'type': 'reasoning.encrypted', 'data': 'BBBB', 'id': None}]
    payload = {'model': MODEL, 'choices': [{'finish_reason': 'stop', 'message': {
        'content': 'OK', 'reasoning_details': copy.deepcopy(details)}}]}
    result = asyncio.run(chat_transport.request(HTTP(Response(payload)), KEY,
                         {'model': MODEL, 'messages': []}, metadata()))
    assert result['choices'][0]['message']['reasoning_details'] == details


@pytest.mark.parametrize('created', [1700000000, None, 'yesterday', True, -1, 253402300800])
def test_catalog_display_metadata_is_bounded_and_typed(created):
    """Descriptions stay text and only finite supported release timestamps reach the UI."""
    text = '<img src=x onerror=alert(1)>\x00' + 'A' * 13000
    row = projected(description=text, created=created)
    assert len(row['description']) <= 12000
    assert row['description'].startswith('<img src=x') and '\x00' not in row['description']
    if type(created) is int and 0 < created <= 253402300799:
        assert row['created'] == created
    else:
        assert 'created' not in row
