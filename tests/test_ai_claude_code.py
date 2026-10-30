"""Offline Claude subscription ownership, protocol and existing capability checks."""
import asyncio
import json
import sys
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import pytest

from ai_claude_code import ClaudeCodeError, ClaudeRuntime


@pytest.mark.parametrize('response', ['{"result": "quoted \\"value\\""}', '{"result":',
                                     '[]', '{"result": NaN}', '{"result": 1e999}'])
def test_structured_reply_boundary_rejects_invalid_json(tmp_path, monkeypatch, response):
    """Only finite JSON objects cross the structured boundary; ordinary chat stays textual."""
    async def scenario():
        """Replace only the worker response, retaining production reply validation."""
        runtime = ClaudeRuntime(tmp_path / 'claude')
        request = AsyncMock(return_value=response)
        monkeypatch.setattr(runtime, 'models', AsyncMock(return_value=[{'id': 'fixture'}]))
        monkeypatch.setattr(runtime, '_request', request)
        output_schema = {'type': 'object', 'properties': {'result': {'type': 'string'}}, 'required': ['result']}
        try:
            if response.startswith('{"result": "quoted'):
                assert await runtime.reply('a'*32, 'fixture', [], '', output_schema=output_schema) == response
            else:
                with pytest.raises(ClaudeCodeError, match='required structured decision'):
                    await runtime.reply('a'*32, 'fixture', [], '', output_schema=output_schema)
            assert request.await_args.args[1]['output_schema'] == output_schema
            assert await runtime.reply('a'*32, 'fixture', [], '') == response
            assert 'output_schema' not in request.await_args.args[1]
        finally:
            await runtime.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('output_schema', [[], {'type': 'array'}, {'type': 'object', 'bad': float('nan')},
                                          {'type': 'object', 'description': 'x'*131073}])
def test_invalid_output_schema_never_dispatches(tmp_path, monkeypatch, output_schema):
    """Reject malformed or unbounded internal schemas before starting a native request."""
    async def scenario():
        """Verify rejection without processes, credentials or provider calls."""
        runtime = ClaudeRuntime(tmp_path / 'claude')
        request = AsyncMock()
        monkeypatch.setattr(runtime, 'models', AsyncMock(return_value=[{'id': 'fixture'}]))
        monkeypatch.setattr(runtime, '_request', request)
        try:
            with pytest.raises(ClaudeCodeError, match='Invalid Claude structured output schema'):
                await runtime.reply('a'*32, 'fixture', [], '', output_schema=output_schema)
            request.assert_not_awaited()
        finally:
            await runtime.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('loop_name', ['asyncio', 'uvloop'])
def test_native_process_permissions_with_server_event_loop(tmp_path, monkeypatch, loop_name):
    """Both supported event loops must start private children without unsupported kwargs."""
    factory = asyncio.new_event_loop if loop_name == 'asyncio' else pytest.importorskip('uvloop').new_event_loop

    async def scenario():
        """Exercise the production spawn path using a disposable, credential-free child."""
        runtime = ClaudeRuntime(tmp_path / 'claude')
        monkeypatch.setattr(runtime, 'available', lambda: True)
        try:
            process = await runtime._spawn('a' * 32, sys.executable, '-I', '-S', '-c',
                "from pathlib import Path; p=Path('fixture'); p.write_text('test'); print(oct(p.stat().st_mode & 0o777))")
            stdout, stderr = await asyncio.wait_for(process.communicate(), 3)
            assert process.returncode == 0 and stdout.strip() == b'0o600' and not stderr
            assert (runtime.profile('a' * 32) / 'workspace' / 'fixture').is_file()
        finally:
            await runtime.close()
            assert not runtime.processes

    with asyncio.Runner(loop_factory=factory) as runner:
        runner.run(scenario())


def test_private_profiles_and_environment_exclude_host_credentials(tmp_path, monkeypatch):
    """Users cannot share login paths or inherit host API billing and routing."""
    runtime = ClaudeRuntime(tmp_path / 'claude')
    monkeypatch.setenv('ANTHROPIC_API_KEY', 'synthetic-host-key')
    monkeypatch.setenv('CLAUDE_CODE_OAUTH_TOKEN', 'synthetic-host-token')
    monkeypatch.setenv('ANTHROPIC_BASE_URL', 'http://not-used')
    first, second = runtime.environment('a'*32), runtime.environment('b'*32)
    assert first['CLAUDE_CONFIG_DIR'] != second['CLAUDE_CONFIG_DIR']
    assert all(name not in first for name in ('ANTHROPIC_API_KEY', 'CLAUDE_CODE_OAUTH_TOKEN', 'ANTHROPIC_BASE_URL'))
    assert Path(first['CLAUDE_CONFIG_DIR']).stat().st_mode & 0o777 == 0o700
    with pytest.raises(ClaudeCodeError, match='Invalid'):
        runtime.profile('../other')


def test_status_allowlists_metadata_and_rejects_api_billing(tmp_path, monkeypatch):
    """Native status payloads cannot reveal credentials or enable API-key access."""
    async def scenario():
        runtime = ClaudeRuntime(tmp_path / 'claude')
        runtime.profile('a'*32)
        monkeypatch.setattr(runtime, 'available', lambda: True)
        monkeypatch.setattr('ai_claude_code.ClaudeRuntime.available', lambda: True)
        native = AsyncMock(return_value=(0,json.dumps({'loggedIn':True,'authMethod':'api_key','apiKey':'fixture-secret'}).encode()))
        monkeypatch.setattr(runtime, '_native', native)
        result = await runtime.status('a'*32)
        assert not result['connected'] and 'fixture-secret' not in json.dumps(result)
        native.return_value=(0,json.dumps({'loggedIn':True,'authMethod':'claude.ai','email':'fixture@example.test','subscriptionType':'max','accessToken':'fixture-secret'}).encode())
        result = await runtime.status('a'*32)
        assert result['connected'] and result['plan']=='max'
        assert 'fixture-secret' not in json.dumps(result)
        await runtime.close()
    asyncio.run(scenario())


def test_model_catalog_resolves_aliases_and_keeps_owner_caches(tmp_path, monkeypatch):
    """Only SDK-confirmed concrete identities can become saved model choices."""
    async def scenario():
        runtime=ClaudeRuntime(tmp_path/'claude')
        monkeypatch.setattr(runtime,'status',AsyncMock(return_value={'connected':True}))
        query=AsyncMock(return_value=[{'value':'sonnet','resolvedModel':'claude-fixture','displayName':'Fixture','supportedEffortLevels':['high']},
                                     {'value':'default'}, {'value':'fable','resolvedModel':'claude-fable-fixture'}])
        monkeypatch.setattr(runtime,'_request',query)
        first=await runtime.models('a'*32)
        assert [x['id'] for x in first]==['claude-fixture','claude-fable-fixture']
        assert first[0]['tools'] and first[0]['reasoning_variants']==[{'id':'high','label':'High'}]
        await runtime.models('a'*32)
        assert query.await_count==1
        await runtime.models('b'*32)
        assert query.await_count==2
        with pytest.raises(ClaudeCodeError,match='unavailable'):
            await runtime.reply('a'*32,'other-model',[],'fixture')
        assert query.await_count==2
        await runtime.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('default_family', ['opus', 'sonnet'])
def test_complete_sdk_catalog_keeps_fable_and_labels_default_model(tmp_path, monkeypatch, default_family):
    """SDK aliases must not hide Opus, drop Fable or obscure the selected model version."""
    async def scenario():
        runtime = ClaudeRuntime(tmp_path / 'claude')
        monkeypatch.setattr(runtime, 'status', AsyncMock(return_value={'connected': True}))
        rows = [{'value': 'default', 'resolvedModel': f'claude-{default_family}-5-5', 'displayName': 'Default (recommended)'}]
        rows += [{'value': family, 'resolvedModel': f'claude-{family}-{version}', 'displayName': family.title(),
                  'supportedEffortLevels': ['low', 'medium', 'high']} for family, version in
                 [('opus', '5-5'), ('fable', '5-1'), ('sonnet', '5-5'), ('haiku', '5-5')]]
        request = AsyncMock(side_effect=[rows, 'Fable fixture reply'])
        monkeypatch.setattr(runtime, '_request', request)
        try:
            models = await runtime.models('a' * 32)
            assert {model['id']: model['name'] for model in models} == {
                'claude-opus-5-5': 'Opus 5.5', 'claude-fable-5-1': 'Fable 5.1',
                'claude-sonnet-5-5': 'Sonnet 5.5', 'claude-haiku-5-5': 'Haiku 5.5'}
            assert [model['id'] for model in models if model['default']] == [f'claude-{default_family}-5-5']
            assert await runtime.reply('a' * 32, 'claude-fable-5-1', [], 'fixture') == 'Fable fixture reply'
            assert request.await_args.args[1]['model'] == 'claude-fable-5-1'
        finally:
            await runtime.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('url,accepted',[
    ('https://claude.com/cai/oauth/authorize?client_id=fixture&code_challenge=fixture',True),
    ('https://claude.ai/oauth/authorize?client_id=fixture&code_challenge=fixture',True),
    ('https://claude.ai/oauth/authorize?access_token=secret',False),
    ('https://claude.ai.attacker.test/oauth/authorize',False),
    ('http://claude.ai/oauth/authorize',False),
    ('https://user:password@claude.ai/oauth/authorize',False),
])
def test_login_only_exposes_native_authorization_links(url,accepted):
    """Provider output cannot introduce an arbitrary URL or return a token URL."""
    assert bool(ClaudeRuntime._login_url(url)) is accepted


@pytest.mark.parametrize('ending',['complete','failure','cancel'])
def test_worker_protocol_dispatches_streams_and_cleans_processes(tmp_path,monkeypatch,ending):
    """Real offline pipes exercise replay responses, partial failure and cancellation."""
    script=tmp_path/'worker.py'
    script.write_text('''import json,sys,time
json.loads(sys.stdin.readline())
print(json.dumps({'type':'text','text':'Partial'}),flush=True)
print(json.dumps({'type':'tool','id':'request-1','name':'read_fixture','arguments':{'value':1}}),flush=True)
answer=json.loads(sys.stdin.readline())
assert answer['id']=='request-1' and answer['result']=={'success':True}
if sys.argv[1]=='failure':
 print(json.dumps({'type':'error','message':'Claude request failed. Check account access and usage limits.'}),flush=True)
elif sys.argv[1]=='cancel': time.sleep(60)
else: print(json.dumps({'type':'result','result':'Completed'}),flush=True)
''')
    async def scenario():
        runtime=ClaudeRuntime(tmp_path/'claude')
        async def spawn(owner,*args,**kwargs):
            """Replace only the external provider with an isolated protocol process."""
            process=await asyncio.create_subprocess_exec(sys.executable,str(script),ending,
                stdin=asyncio.subprocess.PIPE,stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.PIPE,start_new_session=True)
            runtime.processes.add(process)
            return process
        monkeypatch.setattr(runtime,'_spawn',spawn)
        called=asyncio.Event()
        async def handler(name,args):
            """Confirm typed arguments before returning one backend result."""
            assert name=='read_fixture' and args=={'value':1}
            called.set()
            return {'success':True}
        stream=AsyncMock()
        try:
            if ending=='cancel':
                task=asyncio.create_task(runtime._request('a'*32,{'mode':'chat'},handler,stream))
                await asyncio.wait_for(called.wait(),3)
                task.cancel()
                with pytest.raises(asyncio.CancelledError): await task
            elif ending=='failure':
                with pytest.raises(ClaudeCodeError,match='request failed'):
                    await runtime._request('a'*32,{'mode':'chat'},handler,stream)
            else:
                assert await runtime._request('a'*32,{'mode':'chat'},handler,stream)=='Completed'
            stream.assert_awaited_once_with('Partial')
            assert not runtime.processes
        finally: await runtime.close()
    asyncio.run(scenario())


def test_claude_uses_existing_dispatcher_without_approval_bypass(tmp_path,monkeypatch):
    """Tools and clarification gates remain owned by PBGui, not by the SDK host."""
    from ai_chat import AIChatService,AIChatError
    from ai_capabilities import AICapabilityService
    async def scenario():
        caps=AICapabilityService(tmp_path/'capabilities')
        monkeypatch.setattr('ai_chat.get_ai_capability_service',lambda:caps)
        service=AIChatService(tmp_path/'ai')
        owner='a'*32
        conv=await service._conversation(owner,'claude-code','claude-fixture',None)
        dispatch=AsyncMock(return_value={'fixture':True})
        monkeypatch.setattr(caps,'dispatch',dispatch)
        approval=AsyncMock(side_effect=AssertionError('Human approval must not be called by a provider'))
        monkeypatch.setattr(caps,'approve',approval)
        monkeypatch.setattr(service,'_capture_ui_action',AsyncMock())
        name=caps.messages_tools()[0]['name']
        async def reply(*args,**kwargs):
            """Replay the same request, then try an unknown built-in tool."""
            assert {x['name'] for x in kwargs['tools']}
            conv.busy=True
            conv.activity_history=[{'timestamp':0,'message':'Starting'}]
            await kwargs['on_text']('Before tool')
            first=await kwargs['handler'](name,{})
            assert await kwargs['handler'](name,{})==first
            with pytest.raises(AIChatError,match='unsupported capability'):
                await kwargs['handler']('Bash',{'command':'not-executed'})
            await kwargs['on_text']('Streaming fixture')
            projection=service._conversation_projection(conv,include_messages=True)['streaming_messages']
            assert [entry['content'] for entry in projection]==['Before tool','Streaming fixture']
            conv.busy=False
            return 'Completed'
        monkeypatch.setattr(service.claude,'reply',reply)
        try:
            assert await service._claude_chat(owner,'claude-fixture',[],conv.id)=='Completed'
            assert dispatch.await_count==1 and not approval.called
            conv.ui_actions=[{'type':'chat.quick_replies'}]
            blocked=await service._agent_capability_result(owner,conv.id,'propose_ai_loop_config',{}, {})
            assert not blocked['success'] and 'clarification' in blocked['error'].lower()
        finally:
            await service.shutdown()
            await caps.shutdown()
    asyncio.run(scenario())


def test_claude_detached_turn_stream_stop_and_persistent_history(tmp_path,monkeypatch):
    """Claude uses the real shared turn lifecycle, including cancelled partial replies."""
    from ai_chat import AIChatService
    from ai_capabilities import AICapabilityService
    async def scenario():
        caps=AICapabilityService(tmp_path/'capabilities')
        monkeypatch.setattr('ai_chat.get_ai_capability_service',lambda:caps)
        service=AIChatService(tmp_path/'ai')
        owner='a'*32
        monkeypatch.setattr(service.claude,'models',AsyncMock(return_value=[{'id':'claude-fixture','tools':True}]))
        started=asyncio.Event()
        blocked=False
        async def reply(*args,**kwargs):
            """Emit a real service projection, without any external model request."""
            await kwargs['on_text']('Synthetic partial text')
            started.set()
            if blocked: await asyncio.Event().wait()
            return 'Synthetic completed reply'
        monkeypatch.setattr(service.claude,'reply',reply)
        try:
            cid=await service.create_conversation(owner,'claude-code','claude-fixture')
            await service.start_turn(owner,cid,'Synthetic first question')
            await service.active_tasks[cid]
            saved=await service.get_conversation(owner,cid)
            assert [x['content'] for x in saved['messages']]==['Synthetic first question','Synthetic completed reply']
            service.conversations.clear()
            service.loaded_owners.clear()
            restored=await service.get_conversation(owner,cid)
            assert restored['provider']=='claude-code' and restored['model']=='claude-fixture'
            assert restored['messages']==saved['messages']
            blocked=True
            started.clear()
            await service.start_turn(owner,cid,'Synthetic cancelled question')
            await asyncio.wait_for(started.wait(),2)
            projected=service._conversation_projection(service.conversations[cid],include_messages=True)
            assert projected['streaming_messages'][0]['content']=='Synthetic partial text'
            await service.cancel(owner,cid)
            final=await service.get_conversation(owner,cid)
            assert not final['busy']
            assert all(x.get('content')!='Synthetic partial text' for x in final['messages'])
            assert cid not in service.active_tasks and cid not in service.claude_live
        finally:
            await service.shutdown()
            await caps.shutdown()
    asyncio.run(scenario())


def test_native_login_code_is_transient_and_cancel_cleans_process(tmp_path,monkeypatch):
    """A native-flow double receives the one-time code only through its stdin."""
    script=tmp_path/'login.py'
    script.write_text('''import sys
print('https://claude.com/cai/oauth/authorize?client_id=fixture',flush=True)
assert sys.stdin.readline().strip()=='synthetic-one-time-code'
''')
    async def scenario():
        runtime=ClaudeRuntime(tmp_path/'claude')
        async def spawn(owner,*args,**kwargs):
            """Run only the temporary login fixture with owned standard pipes."""
            p=await asyncio.create_subprocess_exec(sys.executable,str(script),stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.STDOUT,start_new_session=True)
            runtime.processes.add(p)
            return p
        monkeypatch.setattr(runtime,'_spawn',spawn)
        try:
            started = await runtime.start_login('a'*32)
            async with asyncio.timeout(3):
                while not runtime.logins['a'*32]['url']: await asyncio.sleep(.01)
            with pytest.raises(ClaudeCodeError,match='No Claude sign-in'):
                await runtime.login_code('b'*32,'synthetic-one-time-code',started['login_id'])
            with pytest.raises(ClaudeCodeError,match='No Claude sign-in'):
                await runtime.login_code('a'*32,'synthetic-one-time-code','stale-login')
            await runtime.login_code('a'*32,'synthetic-one-time-code',started['login_id'])
            await asyncio.wait_for(runtime.logins['a'*32]['task'],3)
            assert runtime.logins['a'*32]['error']==''
            assert not runtime.processes and not list((tmp_path/'claude').rglob('*'))
            await runtime.start_login('a'*32)
            await runtime.cancel_login('a'*32)
            assert not runtime.processes and not runtime.logins
        finally: await runtime.close()
    asyncio.run(scenario())
