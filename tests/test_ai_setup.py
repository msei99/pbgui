"""AI Setup model tests use fixed isolated inputs, owner selection and deterministic cleanup."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from pydantic import ValidationError

import ai_setup


@pytest.mark.parametrize('provider',['chatgpt','opencode-go','opencode-zen','claude-code'])
def test_setup_model_probe_is_isolated_and_does_not_create_conversations(monkeypatch,provider):
    """The fixed probe forwards the chosen model/profile without local diagnostics or tools."""
    reply=AsyncMock(return_value=('PBGui connection OK','fixture','fixture-model'))
    monkeypatch.setattr(ai_setup,'provider_reply',reply)
    chat=SimpleNamespace(accepting_turns=True,active_tasks={})
    selection=ai_setup.ModelTestRequest(provider=provider,profile='b'*32,model='selected-model',effort='high',service_tier='priority' if provider=='chatgpt' else '')
    request=SimpleNamespace(is_disconnected=AsyncMock(return_value=False))
    result=asyncio.run(ai_setup.test_model(chat,'a'*32,selection,request))
    assert result['reply']=='PBGui connection OK' and result['conversation_created'] is False
    assert result['tools_enabled'] is False and result['model']=='selected-model'
    assert reply.call_args.kwargs['selection']==selection.model_dump()
    assert reply.call_args.kwargs['content']=={'connection_test':'Reply PBGui connection OK.'}
    assert 'Do not use tools' in reply.call_args.kwargs['instructions']
    assert not chat.active_tasks


def test_setup_jev_probe_uses_native_decision_and_existing_budget(monkeypatch):
    """Jev is tested through its decision endpoint rather than a text-chat fallback."""
    decide=AsyncMock(return_value='Jev chooses option 1.')
    monkeypatch.setattr('ai_openrouter.decide_general_choice',decide)
    chat=SimpleNamespace(accepting_turns=True,active_tasks={},
        _validate_provider_model=AsyncMock(return_value={}),_validate_model_effort=Mock(),_validate_model_service_tier=Mock(),
        _http_session=AsyncMock(return_value='fixture-session'),credentials=SimpleNamespace(load_openrouter_key=Mock(return_value='FIXTURE_KEY')),
        get_preferences=Mock(return_value={'jev_max_cost_usd':.003}))
    result=asyncio.run(ai_setup.test_model(chat,'a'*32,ai_setup.ModelTestRequest(provider='openrouter',model='typesafe/jev-1.13'),SimpleNamespace(is_disconnected=AsyncMock(return_value=False))))
    assert result['provider']=='openrouter' and decide.call_args.kwargs['max_cost_usd']==.003
    assert decide.call_args.args[4]==['PBGui connection OK','Unrelated option']
    assert not chat.active_tasks


@pytest.mark.parametrize('extra',[{'prompt':'read private files'},{'context':{'secret':'value'}},{'profile':'../other'},{'provider':'foreign'}])
def test_setup_probe_rejects_prompts_context_and_invalid_selections(extra):
    """A connection test cannot be converted into an arbitrary agent request."""
    with pytest.raises(ValidationError):
        ai_setup.ModelTestRequest.model_validate({'provider':'chatgpt','model':'selected-model',**extra})


def test_setup_api_binds_the_authenticated_owner_and_returns_no_store(monkeypatch):
    """The route derives ownership from the session, not the submitted model selection."""
    import json
    import api.ai as api
    from ai_chat import owner_key
    probe=AsyncMock(return_value={'reply':'PBGui connection OK','tools_enabled':False})
    monkeypatch.setattr(ai_setup,'test_model',probe)
    service=object();monkeypatch.setattr(api,'get_ai_chat_service',lambda:service)
    body=ai_setup.ModelTestRequest(provider='chatgpt',model='selected-model')
    request=object()
    result=asyncio.run(api.test_model_connection(body,request,SimpleNamespace(user_id='fixture-user')))
    probe.assert_awaited_once_with(service,owner_key('fixture-user'),body,request)
    assert result.headers['cache-control']=='no-store'
    assert json.loads(result.body)['tools_enabled'] is False


def test_setup_page_preserves_mount_without_trusting_host_headers():
    """The stable setup URL serves local assets/API links under its trusted ASGI mount."""
    from starlette.requests import Request
    from api.ai import main_page
    request=Request({'type':'http','scheme':'http','server':('forged-host.test',80),'root_path':'/mounted',
                     'path':'/api/ai/main_page','headers':[],'query_string':b''})
    page=main_page(request,object()).body.decode()
    assert 'window.API_BASE = "/mounted/api/ai"' in page
    assert '/mounted/app/js/ai_setup.js?v=8' in page
    assert 'href="/mounted/api/ai/providers/opencode-go/subscribe"' in page
    assert 'forged-host.test' not in page and '%%BASE_PREFIX%%' not in page


def test_claude_model_probe_uses_the_same_isolated_request_owner(monkeypatch):
    """Claude connection tests retain shared lifecycle and fixed-input handling."""
    probe=AsyncMock(return_value={'reply':'fixture'})
    monkeypatch.setattr(ai_setup,'draft_for_request',probe)
    request=object(); chat=object()
    result=asyncio.run(ai_setup.test_model(chat,'a'*32,ai_setup.ModelTestRequest(provider='claude-code',model='claude-fixture'),request))
    assert result=={'reply':'fixture'}
    assert probe.call_args.kwargs['selected_provider']=='claude-code'
    assert probe.call_args.kwargs['timeout']==90
