"""Regression coverage for interrupted replies, framing, setup effort and summary fallback."""
import asyncio
import copy
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import ai_deepseek
from ai_chat import AIChatError, AIChatService
from pb8_loop_dialog import IncompleteResponse, read_sse, response_parts
from tests.test_ai_deepseek import OWNER, KEY, HTTP, Response, sse, service, catalog_payload, record


class ManyEvents:
    """Generate realistic framed small deltas without a huge stored wire fixture."""
    def __init__(self,count=20000):
        self.count,self.index=count,0
        item={'id':'','choices':[{'delta':{'reasoning_content':'x'}}]}
        item['id']='x'*(328-len(('data: '+json.dumps(item)+'\n\n').encode()))
        self.chunk=('data: '+json.dumps(item)+'\n\n').encode()
    async def readany(self):
        """Emit more than six MiB of framing for only twenty thousand content characters."""
        self.index+=1
        if self.index<=self.count:return self.chunk
        if self.index==self.count+1:return sse({'content':'OK'})
        return b''


@pytest.mark.parametrize('adapter',['chat','loop'])
def test_small_deltas_do_not_consume_content_budget(adapter):
    """DeepSeek framing has a separate finite budget in Chat and Loop."""
    async def run():
        reader=ManyEvents()
        if adapter=='chat':result=await ai_deepseek._read_stream(reader)
        else:result=await read_sse(reader,'chat',{'first_timeout':5,'idle_timeout':5,'max_stream_bytes':ai_deepseek.stream_limit(),'max_content_chars':ai_deepseek.MAX_CONTENT_CHARS})
        assert result['choices'][0]['message']['content']=='OK'
        assert len(result['choices'][0]['message']['reasoning_content'])==20000
    asyncio.run(run())


@pytest.mark.parametrize('reason',['aborted','insufficient_system_resource'])
def test_interrupted_chat_never_executes_tools_or_saves_an_answer(service, monkeypatch, reason):
    """A terminal SSE marker does not make an interrupted tool response successful."""
    service.credentials.save_deepseek_key(OWNER,KEY)
    response=Response(sse({'content':'partial','tool_calls':[{'index':0,'id':'tool-1','function':{'name':'list_optimizer_runs','arguments':'{}'}}]},finish=reason),stream=True)
    http=HTTP(response)
    monkeypatch.setattr(service,'_http_session',AsyncMock(return_value=http))
    dispatched=AsyncMock()
    monkeypatch.setattr(service,'_agent_capability_result',dispatched)
    async def run():
        cid=await service.create_conversation(OWNER,'deepseek','deepseek-flash')
        await service.start_turn(OWNER,cid,'Synthetic test')
        await service.active_tasks[cid]
        snapshot=await service.get_conversation(OWNER,cid)
        assert snapshot['last_error'] and not any(m['role']=='assistant' for m in snapshot['messages'])
        assert snapshot['messages'][-1]['failed']
    asyncio.run(run())
    dispatched.assert_not_awaited()
    assert response.closed


@pytest.mark.parametrize('reason',['aborted','insufficient_system_resource','future_unknown',None])
def test_deepseek_requires_successful_finish_for_json_and_loop(reason):
    """The DeepSeek send boundary rejects all unsuccessful and missing native finishes."""
    payload={'choices':[{'finish_reason':reason,'message':{'content':'partial'}}]}
    with pytest.raises(AIChatError,match='complet'):
        ai_deepseek.validate_finish(payload)
    if reason in {'aborted','insufficient_system_resource'}:
        with pytest.raises(IncompleteResponse):response_parts('chat',payload)
        with pytest.raises(AIChatError):AIChatService._validate_opencode_completion(payload,'chat',32768)


@pytest.mark.parametrize('reason',['aborted','insufficient_system_resource'])
def test_interrupted_research_is_not_a_report(service, monkeypatch, reason):
    """JSON research answers follow the same successful-finish contract as Chat."""
    import ai_research_opencode as research
    payload={'choices':[{'finish_reason':reason,'message':{'content':'partial'}}]}
    monkeypatch.setattr(research,'_post',AsyncMock(return_value=json.dumps(payload)))
    record={'provider':'deepseek','model':'deepseek-flash','effort':'','instructions':'No tools','id':'f'*32}
    with pytest.raises(AIChatError,match='complet'):
        asyncio.run(research._model_text(None,service,record,ai_deepseek.models(catalog_payload())[0],KEY,{'stage':'answer'}))


@pytest.mark.parametrize('effort',['none','low','high','max'])
def test_setup_forwards_the_chosen_effort(service, monkeypatch, effort):
    """Test the real Setup operation rather than calling the transport directly."""
    import ai_setup
    service.credentials.save_deepseek_key(OWNER,KEY)
    reply=AsyncMock(return_value='PBGui connection OK')
    monkeypatch.setattr(service,'_deepseek_chat',reply)
    async def draft(chat,owner,body,request,**kwargs):
        return await kwargs['operation'](chat,owner,body)
    monkeypatch.setattr(ai_setup,'draft_for_request',draft)
    asyncio.run(ai_setup.test_model(service,OWNER,ai_setup.ModelTestRequest(provider='deepseek',model='deepseek-flash',effort=effort),None))
    assert reply.await_args.kwargs['effort']==effort


def test_summary_failure_falls_back_and_does_not_bill_another_summary(service, monkeypatch):
    """A valid but oversized optional summary must not block a fitting normal chat."""
    service.credentials.save_deepseek_key(OWNER,KEY)
    http=HTTP(Response(sse({'content':'x'*12001}),stream=True),Response(sse({'content':'Normal answer'}),stream=True),Response(sse({'content':'Next answer'}),stream=True))
    monkeypatch.setattr(service,'_http_session',AsyncMock(return_value=http))
    async def run():
        cid=await service.create_conversation(OWNER,'deepseek','deepseek-flash')
        conversation=service.conversations[cid]
        conversation.messages=[{'role':'user' if i%2==0 else 'assistant','content':f'old-{i}'} for i in range(18)]
        assert (await service.chat(OWNER,'deepseek','deepseek-flash','Current',cid))['reply']=='Normal answer'
        assert 'old-0' in json.dumps(http.sent[1][1]['json'])
        retry=conversation.deepseek_state['summary_retry_at']
        service.conversations.pop(cid);service.loaded_owners.discard(OWNER)
        await service.get_conversation(OWNER,cid)
        assert service.conversations[cid].deepseek_state['summary_retry_at']==retry
        assert (await service.chat(OWNER,'deepseek','deepseek-flash','Next',cid))['reply']=='Next answer'
        assert len(http.sent)==3 and 'tools' in http.sent[2][1]['json']
    asyncio.run(run())


def test_summary_image_error_is_a_safe_chat_error(service, monkeypatch):
    """Missing private image files do not leak raw filesystem exceptions."""
    from ai_deepseek_context import prepare
    service.credentials.save_deepseek_key(OWNER,KEY)
    async def run():
        cid=await service.create_conversation(OWNER,'deepseek','deepseek-flash')
        history=[{'role':'user' if i%2==0 else 'assistant','content':'message','context_id':f'{i:032x}'} for i in range(19)]
        history[0]['images']=[{'id':'synthetic'}]
        monkeypatch.setattr(service.image_store,'native_history',lambda *args: (_ for _ in ()).throw(OSError('private path should not appear')))
        with pytest.raises(AIChatError,match='Chat images are unavailable or invalid'):
            await prepare(service,OWNER,service.conversations[cid],history,{'image_input':True},KEY)
    asyncio.run(run())


def test_deepseek_output_budget_matches_adapter_capacity():
    """Keep provider catalog limits truthful but bound each application request."""
    body={'model':'deepseek-flash','messages':[{'role':'user','content':'Hello'}]}
    assert AIChatService._apply_opencode_output_budget(body,'chat',{'provider':'deepseek','context':1048576,'output_limit':393216})==32768
    assert ai_deepseek.stream_limit(393216)==ai_deepseek.stream_limit(32768)
    assert AIChatService._safe_provider_error(402,b'{"error":{"message":"Insufficient Balance"}}')=='AI provider billing or spending limit prevents this request'


def test_deepseek_tool_turn_timeout_is_not_the_opencode_limit(service, monkeypatch):
    """The outer deadline allows deep reasoning while preserving a finite turn budget."""
    deadlines=[]
    async def wait(coro,timeout):
        deadlines.append(timeout)
        return await coro
    monkeypatch.setattr('ai_chat.asyncio.wait_for',wait)
    monkeypatch.setattr(service,'_go_chat_completion_agent_inner',AsyncMock(return_value='OK'))
    assert asyncio.run(service._go_chat_completion_agent(OWNER,'b'*32,'https://api.deepseek.com/v1','deepseek-flash',KEY,[],'',metadata={'provider':'deepseek'}))=='OK'
    assert deadlines==[900]



def test_loop_done_marker_does_not_invent_a_successful_finish():
    """DeepSeek cannot treat a bare terminal marker as a provider-reported stop."""
    async def run():
        response=Response(sse({'content':'partial'},finish=None),stream=True)
        with pytest.raises(IncompleteResponse,match='Premature'):
            await read_sse(response.content,'chat',{'first_timeout':5,'idle_timeout':5})
        from pb8_loop_dialog import StreamAccumulator
        accumulator=StreamAccumulator('chat');accumulator.done=True
        payload=accumulator.finish()
        assert payload['choices'][0]['finish_reason'] is None
        with pytest.raises(AIChatError,match='complet'):
            ai_deepseek.validate_finish(payload)
    asyncio.run(run())


def test_decoded_content_remains_bounded_despite_larger_framing_budget():
    """Enlarging the SSE transport budget does not allow unbounded decoded state."""
    from pb8_loop_dialog import StreamAccumulator
    accumulator=StreamAccumulator('chat')
    accumulator.max_content_chars=10
    with pytest.raises(IncompleteResponse,match='decoded response'):
        accumulator.feed({'choices':[{'delta':{'reasoning_content':'x'*11}}]})


@pytest.mark.parametrize('reason',['length','aborted','insufficient_system_resource','content_filter','future_unknown'])
@pytest.mark.parametrize('transport',['sse','json_fallback','json'])
def test_interrupted_loop_settles_ledger_before_repair(record, service, monkeypatch, reason, transport):
    """Actual provider_request accounts for acknowledged replies before typed repair errors."""
    from unittest.mock import Mock
    from pb8_loop_ai import LoopAI, LoopAIResponseError
    from pb8_loop_ai_policy import DecisionLedger
    from pb8_loop_ai_v2 import _ACTIVE, provider_request
    from tests.test_pb8_loop_dialog_v2 import frozen
    store,row=record
    row['settings'].update(provider='deepseek',model='deepseek-flash',effort='')
    service.credentials.save_deepseek_key(OWNER,KEY)
    reader=frozen((store,row))
    metadata=ai_deepseek.models(catalog_payload())[0]
    ledger=DecisionLedger(store,row,'select',reader.snapshot['sha256'],metadata)
    runtime={'context':1_000_000,'page_tokens':5000,'output_tokens':1000,'streaming':transport!='json',
             'call_timeout':30,'connect_timeout':10,'first_timeout':10,'idle_timeout':10}
    delta={'content':'{"ok":true}','tool_calls':[{'index':0,'id':'read-1','function':{'name':'loop_evidence','arguments':'{"path":"/required","offset":0}'}}]}
    if transport=='sse':response=Response(sse(delta,finish=reason),stream=True)
    else:response=Response({'choices':[{'finish_reason':reason,'message':{'content':'{"ok":true}','tool_calls':[{'id':'read-1','function':{'name':'loop_evidence','arguments':'{"path":"/required","offset":0}'}}]}}], 'usage':{'prompt_tokens':20,'completion_tokens':10}})
    http=HTTP(response)
    monkeypatch.setattr(service,'_http_session',AsyncMock(return_value=http))
    validator=Mock()
    ai=LoopAI(store,service)
    async def run():
        """Enter the same active evidence/ledger scope as productive Loop requests."""
        token=_ACTIVE.set({'profile':runtime,'session':reader,'ledger':ledger,'instructions':'Read evidence','validate':validator})
        try:
            with pytest.raises(LoopAIResponseError) as caught:
                await provider_request(ai,row,metadata,{'stage':'select'},1000)
            assert caught.value.usage['tokens']=={'prompt_tokens':20,'completion_tokens':10}
        finally:_ACTIVE.reset(token)
    asyncio.run(run())
    state=store.read(row['owner'],row['id'])
    assert state['pending_ai'] is None
    assert state['last_usage']['status']=='reported'
    assert state['last_usage']['tokens']=={'prompt_tokens':20,'completion_tokens':10}
    calls=ledger.state()['calls']
    assert len(calls)==1 and calls[0]['settled'] and calls[0]['usage_confirmed']
    assert calls[0]['tokens']==30 and ledger.state().get('tool_calls',0)==0 and reader.total_calls==0
    assert response.closed and len(http.sent)==1
    validator.assert_not_called()


@pytest.mark.parametrize('stage',['plan','answer'])
def test_truncated_deepseek_research_retains_only_partial_report(service, monkeypatch, stage):
    """Length failures retain report text for inspection without accepting it or sending to Jev."""
    import ai_research_opencode as research
    payload={'choices':[{'finish_reason':'length','message':{'content':'Partial sourced report'}}]}
    monkeypatch.setattr(research,'_post',AsyncMock(return_value=json.dumps(payload)))
    record={'provider':'deepseek','model':'deepseek-flash','effort':'','instructions':'No tools','id':'f'*32}
    with pytest.raises(AIChatError,match='output limit'):
        asyncio.run(research._model_text(None,service,record,ai_deepseek.models(catalog_payload())[0],KEY,{'stage':stage}))
    assert record.get('answer')==('Partial sourced report' if stage=='answer' else None)


def test_oversized_decoded_deepseek_stream_has_clear_size_error():
    """A content cap produces a size error rather than an invalid-event diagnostic."""
    response=Response(sse(*[{'reasoning_content':'x'*2000} for _ in range(260)]),stream=True)
    response.content.width=65536
    with pytest.raises(AIChatError,match='decoded response is too large'):
        asyncio.run(ai_deepseek._read_stream(response.content))
