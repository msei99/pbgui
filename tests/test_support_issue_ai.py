"""English issue drafts use isolated providers, strict text schemas and owned cancellation."""
import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import ValidationError

import support_issue_ai as ai
from ai_chat import AIChatError
from api import support_debug as api
from api.auth import require_auth

DRAFT = dict(subject='Improve comparison', description='Compare configurations side by side.', steps='', expected='')


def chat_fixture(provider='opencode-go'):
    """Use synthetic credentials and no runtime files or real provider calls."""
    return SimpleNamespace(accepting_turns=True, active_tasks={},
        get_preferences=Mock(return_value={'selection':dict(provider=provider, model='fixture', profile='default', effort='', service_tier='')}),
        _validate_provider_model=AsyncMock(return_value={'id':'fixture','protocol':'chat'}),
        _validate_model_effort=Mock(), _validate_model_service_tier=Mock(),
        credentials=SimpleNamespace(load_go_key=Mock(return_value='synthetic-key')))


@pytest.mark.parametrize('description', ['Bitte vergleichen', 'Comparar configuraciones', '比较配置', 'Сравнить настройки'])
def test_isolated_text_only_request_always_instructs_english(monkeypatch, description):
    """All input languages use the same English drafting rules and no tools/evidence."""
    action = AsyncMock(return_value=json.dumps(DRAFT))
    monkeypatch.setattr('ai_research_opencode._model_text', action)
    chat = chat_fixture()
    body = ai.IssueDraftRequest(issue_type='feature', description=description)
    result = asyncio.run(ai.generate_draft(chat, 'owner-fixture', body))
    assert result['draft'] == DRAFT
    session, _, record, _, key, content = action.call_args.args
    assert session.closed and key == 'synthetic-key'
    assert record['instructions'] == ai.INSTRUCTIONS
    assert 'Translate every prose field into clear English' in ai.INSTRUCTIONS
    assert 'untrusted' in ai.INSTRUCTIONS and 'without inventing facts' in ai.INSTRUCTIONS
    assert content == {'stage':'answer','untrusted_issue_fields':body.model_dump(exclude={'screenshots'})}
    assert 'screenshots' not in json.dumps(content) and 'error_context' not in json.dumps(content)


@pytest.mark.parametrize('failure', [None, AIChatError, asyncio.CancelledError])
@pytest.mark.parametrize('with_images', [False, True])
def test_codex_is_ephemeral_tool_free_and_closed(tmp_path, monkeypatch, failure, with_images):
    """Use a fresh no-tool runtime without reusing the user's conversation thread."""
    runtime = SimpleNamespace(start_thread=AsyncMock(return_value='thread-fixture'), chat=AsyncMock(return_value=json.dumps(DRAFT)), close=AsyncMock())
    factory = Mock(return_value=runtime)
    monkeypatch.setattr(ai, 'CodexRuntime', factory)
    monkeypatch.setattr(ai, 'IssueImageRuntime', factory)
    chat = chat_fixture('chatgpt')
    chat._profile_runtime = Mock(return_value=SimpleNamespace(root=tmp_path))
    async def inspect_call(*args):
        """Verify private temporary image files while the isolated request is active."""
        if with_images:
            assert len(runtime.issue_image_paths)==1
            image_path=Path(runtime.issue_image_paths[0])
            assert image_path.read_bytes()==b'\x89PNG\r\n\x1a\nfixture'
            assert image_path.stat().st_mode & 0o777 == 0o600
            assert image_path.parent.stat().st_mode & 0o777 == 0o700
        if failure:
            raise failure('Synthetic failure')
        return json.dumps(DRAFT)
    runtime.chat.side_effect=inspect_call
    body=ai.IssueDraftRequest(description='Vergleichen', screenshots=[image_fixture()] if with_images else [])
    if failure:
        with pytest.raises(failure):
            asyncio.run(ai.generate_draft(chat, 'owner-fixture', body))
    else:
        result = asyncio.run(ai.generate_draft(chat, 'owner-fixture', body))
        assert result['draft'] == DRAFT
    assert runtime.research_mode and runtime.research_analysis_only
    assert runtime.loop_instructions == ai.INSTRUCTIONS
    runtime.start_thread.assert_awaited_once_with('fixture')
    runtime.close.assert_awaited_once()
    assert not Path(runtime.research_cwd).exists()
    assert factory.call_args.args == ('owner-fixture', tmp_path)
    assert len(factory.call_args.args) == 2


@pytest.mark.parametrize('reply', ['not json', json.dumps({**DRAFT,'subject':'x'*121}), json.dumps({**DRAFT,'description':''}), json.dumps({**DRAFT,'tools':['shell']}), 'x'*16001])
def test_invalid_model_output_keeps_original_text(reply):
    """Malformed, oversized or extra model fields are never applied."""
    with pytest.raises(AIChatError, match='invalid issue draft'):
        ai.parse_draft(reply)


@pytest.mark.parametrize('extra', ['report','error_context','provider','model'])
def test_draft_schema_rejects_evidence_and_provider_override(extra):
    """A caller cannot silently attach diagnostic evidence or choose another owner/model."""
    with pytest.raises(ValidationError):
        ai.IssueDraftRequest(description='Text', **{extra:'forbidden'})


def test_missing_text_model_does_not_fallback():
    """A structured Jev selection is never silently replaced with a paid text model."""
    with pytest.raises(AIChatError, match='Select a connected'):
        asyncio.run(ai.generate_draft(chat_fixture('openrouter'), 'owner', ai.IssueDraftRequest(description='Text')))


def test_claude_isolated_transport_never_uses_other_providers(monkeypatch):
    """Claude support requests stay tool-free and never use another transport."""
    chat=chat_fixture('claude-code')
    chat._validate_provider_model=AsyncMock(return_value={})
    chat._claude_reply=AsyncMock(return_value='Synthetic reply')
    monkeypatch.setattr(ai,'CodexRuntime',Mock(side_effect=AssertionError('Wrong provider')))
    result=asyncio.run(ai.provider_reply(chat,'fixture-owner',ai.IssueDraftRequest(description='Synthetic issue')))
    assert result[0]=='Synthetic reply' and result[1]=='claude-code'
    assert not chat._claude_reply.call_args.kwargs.get('tools')


@pytest.mark.parametrize('ending', ['success','disconnect','timeout','shutdown'])
def test_request_ownership_and_cleanup(monkeypatch, ending):
    """Disconnect, timeout and service cancellation deterministically release provider work."""
    async def run():
        chat = chat_fixture()
        started = asyncio.Event()
        closed = asyncio.Event()
        async def work(*args):
            """Simulate a provider operation with observable cleanup."""
            started.set()
            try:
                if ending == 'success':
                    return {'draft':DRAFT}
                await asyncio.Event().wait()
            finally:
                closed.set()
        monkeypatch.setattr(ai, 'generate_draft', work)
        monkeypatch.setattr(ai, 'TIMEOUT', .02 if ending == 'timeout' else 5)
        request = SimpleNamespace(is_disconnected=AsyncMock(return_value=ending == 'disconnect'))
        operation = asyncio.create_task(ai.draft_for_request(chat, 'owner', ai.IssueDraftRequest(description='Text'), request))
        await started.wait()
        if ending == 'shutdown':
            for task in list(chat.active_tasks.values()):
                task.cancel()
        if ending in {'disconnect','shutdown'}:
            with pytest.raises(asyncio.CancelledError):
                await operation
        elif ending == 'timeout':
            with pytest.raises(AIChatError, match='timed out'):
                await operation
        else:
            assert await operation == {'draft':DRAFT}
        assert closed.is_set() and not chat.active_tasks
        assert not [task for task in asyncio.all_tasks() if task is not asyncio.current_task() and task.get_name().startswith('support-issue-draft:')]
    asyncio.run(run())


def test_concurrency_and_shutdown_guards():
    """At most one draft per owner and two overall are accepted while serving requests."""
    async def run():
        chat = chat_fixture()
        body = ai.IssueDraftRequest(description='Text')
        request = SimpleNamespace(is_disconnected=AsyncMock(return_value=False))
        chat.active_tasks = {'support-issue-draft:owner':object()}
        with pytest.raises(AIChatError, match='already running'):
            await ai.draft_for_request(chat, 'owner', body, request)
        chat.active_tasks = {'support-issue-draft:a':object(),'support-issue-draft:b':object()}
        with pytest.raises(AIChatError, match='Other issue drafts'):
            await ai.draft_for_request(chat, 'owner', body, request)
        chat.accepting_turns = False
        with pytest.raises(AIChatError, match='shutting down'):
            await ai.draft_for_request(chat, 'owner', body, request)
    asyncio.run(run())


def test_authenticated_no_store_api_and_strict_input(monkeypatch):
    """Only authenticated explicit text requests reach the selected owner-scoped service."""
    action = AsyncMock(return_value={'draft':DRAFT,'provider':'fixture','model':'fixture'})
    monkeypatch.setattr(api, 'draft_for_request', action)
    monkeypatch.setattr(api, 'get_ai_chat_service', lambda: 'chat-fixture')
    monkeypatch.setattr(api, 'owner_key', lambda user: 'owned-'+user)
    app = FastAPI(); app.include_router(api.router, prefix='/api/debug')
    client = TestClient(app)
    assert client.post('/api/debug/issue-draft', json={'description':'Wunsch'}).status_code == 401
    app.dependency_overrides[require_auth] = lambda: SimpleNamespace(user_id='test')
    response = client.post('/api/debug/issue-draft', json={'issue_type':'feature','description':'Wunsch'})
    assert response.status_code == 200 and response.headers['cache-control'] == 'no-store'
    assert response.json()['draft'] == DRAFT
    assert action.call_args.args[:2] == ('chat-fixture','owned-test')
    for invalid in [{'description':' '},{'description':'x','screenshots':[{'url':'http://internal.test'}]},{'description':'x','issue_type':'other'}]:
        assert client.post('/api/debug/issue-draft', json=invalid).status_code == 422
    action.side_effect = AIChatError('Select a connected text model first.')
    assert client.post('/api/debug/issue-draft', json={'description':'x'}).status_code == 400


def image_fixture():
    """Supply synthetic inline PNG bytes, never a captured user image."""
    import base64
    return dict(media_type='image/png', data_base64=base64.b64encode(b'\x89PNG\r\n\x1a\nfixture').decode())


@pytest.mark.parametrize('images', [
    [{'media_type':'image/svg+xml','data_base64':'AAAA'}],
    [{'media_type':'image/png','data_base64':'AAAA'}],
    [{'media_type':'image/png','data_base64':'%%%'}],
    [image_fixture()]*6,
    [{**image_fixture(),'url':'https://external.test/image.png'}],
])
def test_invalid_images_rejected_before_contacting_provider(images):
    """Reject unsupported types, mismatched bytes, bad encoding, paths and excessive counts."""
    with pytest.raises(ValidationError):
        ai.IssueDraftRequest(description='Dashboard', screenshots=images)


def test_native_codex_image_input_body(tmp_path, monkeypatch):
    """Server-generated paths are native localImage blocks, avoiding oversized base64 echoes."""
    action = AsyncMock(return_value={'ok':True})
    monkeypatch.setattr(ai.CodexRuntime, 'request', action)
    runtime = ai.IssueImageRuntime('owner', tmp_path)
    runtime.issue_image_paths = [str(tmp_path/'screenshot-1.png')]
    original = {'input':[{'type':'text','text':'Dashboard'}]}
    asyncio.run(runtime.request('turn/start', original))
    params = action.call_args.args[1]
    assert params['input'] == original['input'] + [{'type':'localImage','path':runtime.issue_image_paths[0]}]
    assert len(original['input']) == 1


@pytest.mark.parametrize('protocol', ['responses','chat','messages'])
def test_opencode_native_image_blocks_and_no_tools(monkeypatch, protocol):
    """All three supported native protocols receive selected image bytes as multimodal content."""
    from ai_chat import AIChatService
    payloads = {
        'responses': {'status':'completed','output':[{'type':'message','content':[{'type':'output_text','text':json.dumps(DRAFT)}]}]},
        'chat': {'choices':[{'finish_reason':'stop','message':{'content':json.dumps(DRAFT)}}]},
        'messages': {'stop_reason':'end_turn','content':[{'type':'text','text':json.dumps(DRAFT)}]},
    }
    action = AsyncMock(return_value=json.dumps(payloads[protocol]))
    monkeypatch.setattr('ai_research_opencode._post', action)
    chat = SimpleNamespace(_selected_reasoning_variant=AIChatService._selected_reasoning_variant,
                           _apply_reasoning_variant=Mock())
    record = dict(model='fixture',provider='opencode-go',effort='',id='synthetic')
    image = ai.IssueScreenshot(**image_fixture())
    result = asyncio.run(ai.opencode_image_draft(None,chat,record,{'protocol':protocol},'synthetic-key',{'stage':'answer'},[image]))
    assert ai.parse_draft(result) == DRAFT
    body = action.call_args.args[3]
    assert 'tools' not in body
    parts = body['input'][0]['content'] if protocol=='responses' else body['messages'][-1]['content']
    assert len(parts)==2
    if protocol=='responses':
        assert parts[1] == {'type':'input_image','image_url':ai.image_url(image)}
    elif protocol=='chat':
        assert parts[1] == {'type':'image_url','image_url':{'url':ai.image_url(image)}}
    else:
        assert parts[1] == {'type':'image','source':{'type':'base64',**image.model_dump(exclude={'data_base64'}),'data':image.data_base64}}


def test_selected_images_reach_provider_once_without_text_fallback(monkeypatch):
    """Screenshots bypass text-only generation and are excluded from the textual JSON."""
    chat = chat_fixture()
    action = AsyncMock(return_value=json.dumps(DRAFT))
    text_only = AsyncMock()
    monkeypatch.setattr(ai, 'opencode_image_draft', action)
    monkeypatch.setattr('ai_research_opencode._model_text', text_only)
    body = ai.IssueDraftRequest(description='Siehe Screenshot',screenshots=[image_fixture()])
    assert asyncio.run(ai.generate_draft(chat,'owner',body))['draft']==DRAFT
    content, images = action.call_args.args[-2:]
    assert 'screenshots' not in content['untrusted_issue_fields'] and images==body.screenshots
    text_only.assert_not_called()
    action.side_effect=AIChatError('Rejected image model')
    with pytest.raises(AIChatError):
        asyncio.run(ai.generate_draft(chat,'owner',body))
    assert action.await_count==2
    text_only.assert_not_called()


def test_api_image_budget_and_safe_model_failure(monkeypatch):
    """Stream limits and image validation run before providers; failures do not echo images."""
    action = AsyncMock(side_effect=AIChatError('Provider echoed sensitive image'))
    monkeypatch.setattr(api,'draft_for_request',action)
    monkeypatch.setattr(api,'get_ai_chat_service',lambda:'chat-fixture')
    app=FastAPI(); app.include_router(api.router,prefix='/api/debug')
    app.dependency_overrides[require_auth]=lambda:SimpleNamespace(user_id='fixture')
    client=TestClient(app)
    response=client.post('/api/debug/issue-draft',json={'description':'Dashboard','screenshots':[image_fixture()]})
    assert response.status_code==400 and 'image-capable model' in response.json()['detail']
    assert 'sensitive' not in response.text
    action.reset_mock()
    monkeypatch.setattr(api,'MAX_REPORT_BYTES',50)
    assert client.post('/api/debug/issue-draft',content=b' '*51).status_code==413
    action.assert_not_called()
