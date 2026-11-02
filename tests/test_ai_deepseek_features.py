"""Offline DeepSeek balances, real usage, private compaction and approved research."""
from __future__ import annotations

import asyncio
import copy
import json
from unittest.mock import AsyncMock

import pytest

from ai_chat import AIChatError
from ai_deepseek_context import observe, prepare, projection, restore
from tests.test_ai_deepseek import OWNER, KEY, HTTP, Response, sse, service, catalog_payload


def test_balance_uses_own_key_and_preserves_provider_amounts(service, monkeypatch):
    """Currency amounts are provider data, with no guessed spend or leaked credentials."""
    service.credentials.save_deepseek_key(OWNER, KEY)
    response = Response({'is_available':True,'balance_infos':[
        {'currency':'USD','total_balance':'12.340001','granted_balance':'2.00','topped_up_balance':'10.340001'},
        {'currency':'CNY','total_balance':'99.50'}]})
    http = HTTP(response)
    monkeypatch.setattr(service,'_http_session',AsyncMock(return_value=http))
    result = asyncio.run(service.usage(OWNER,'deepseek'))
    assert result['balances'][0]['total_balance']=='12.340001'
    assert result['balances'][1]['currency']=='CNY'
    assert KEY not in json.dumps(result) and 'spend' not in result
    url,options = http.sent[0]
    assert url=='https://api.deepseek.com/v1/user/balance'
    assert options['headers']['Authorization']=='Bearer '+KEY and options['allow_redirects'] is False
    assert options['timeout'].total==10 and response.closed
    assert asyncio.run(service.usage('b'*32,'deepseek'))['connected'] is False
    assert len(http.sent)==1


@pytest.mark.parametrize('payload,status', [({},200),({'is_available':True,'balance_infos':[{'currency':'USD','total_balance':'NaN'}]},200),({'error':{'message':KEY}},401)])
def test_missing_balance_is_unavailable_never_zero(service, monkeypatch, payload, status):
    """Bad/failed balance responses do not become a zero balance or echo upstream secrets."""
    service.credentials.save_deepseek_key(OWNER,KEY)
    response=Response(payload,status)
    monkeypatch.setattr(service,'_http_session',AsyncMock(return_value=HTTP(response)))
    result=asyncio.run(service.usage(OWNER,'deepseek'))
    assert result['message']=='DeepSeek balance currently unavailable.'
    assert 'balances' not in result and KEY not in json.dumps(result) and response.closed


def test_usage_validates_real_counters_and_removes_stale_values():
    """Missing telemetry is unknown, while cached/reasoning counters remain provider-reported."""
    state={'summary':'private','covered_ids':['a'*32],'limit_tokens':1000}
    observe(state,{'usage':{'prompt_tokens':100,'completion_tokens':20,'prompt_cache_hit_tokens':50,
                          'completion_tokens_details':{'reasoning_tokens':8}}})
    assert projection(state)=={'limit_tokens':1000,'input_tokens':100,'output_tokens':20,'cached_tokens':50,'reasoning_tokens':8,'used_tokens':120}
    observe(state,{'usage':{'prompt_tokens':True,'completion_tokens':-1}})
    assert projection(state)=={'limit_tokens':1000}
    assert restore({'summary':'x'*20000,'covered_ids':['../bad'],'used_tokens':'200'})=={'summary':'x'*12000,'covered_ids':[]}


@pytest.mark.parametrize('long_turn',[False,True])
def test_context_compaction_is_private_persistent_and_rewindable(service, monkeypatch, long_turn):
    """Productive chat summarizes older turns once and retains visible history and telemetry."""
    service.credentials.save_deepseek_key(OWNER,KEY)
    http=HTTP(Response(sse({'content':'Condensed goals and constraints'}),stream=True),
              Response(sse({'content':'Answer after compaction'}),stream=True),
              Response(sse({'content':'Follow-up answer'}),stream=True))
    monkeypatch.setattr(service,'_http_session',AsyncMock(return_value=http))
    async def run():
        """Exercise real chat, persistence, a follow-up, reload and rewind."""
        cid=await service.create_conversation(OWNER,'deepseek','deepseek-flash')
        conversation=service.conversations[cid]
        count=2 if long_turn else 18
        conversation.messages=[{'role':'user' if i%2==0 else 'assistant','content':f'old-{i}'} for i in range(count)]
        if long_turn:
            conversation.messages[-1]['reasoning_content']='private earlier reasoning '*6000
        original=copy.deepcopy(conversation.messages)
        result=await service.chat(OWNER,'deepseek','deepseek-flash','Current question',cid)
        assert result['reply']=='Answer after compaction'
        assert [m['content'] for m in conversation.messages[:count]]==[m['content'] for m in original]
        assert conversation.deepseek_state['compactions']==1
        projected=await service.get_conversation(OWNER,cid)
        assert projected['context_usage']=={'used_tokens':30,'limit_tokens':1_000_000}
        assert projected['token_usage']['input_tokens']==20 and projected['token_usage']['output_tokens']==10
        assert 'Condensed goals' not in json.dumps(projected) and 'covered_ids' not in json.dumps(projected)
        assert 'context_id' not in json.dumps(http.sent[1][1]['json'])
        assert 'old-0' not in json.dumps(http.sent[1][1]['json'])
        assert 'Untrusted historical summary' in json.dumps(http.sent[1][1]['json'])
        assert 'tools' not in http.sent[0][1]['json'] and 'tools' in http.sent[1][1]['json']
        assert http.sent[0][1]['json']['reasoning_effort']=='none'
        await service.chat(OWNER,'deepseek','deepseek-flash','Next question',cid)
        assert len(http.sent)==3 and conversation.deepseek_state['compactions']==1
        service.conversations.pop(cid);service.loaded_owners.discard(OWNER)
        await service.get_conversation(OWNER,cid)
        assert service.conversations[cid].deepseek_state['compactions']==1
        await service.rewind_conversation(OWNER,cid,0)
        assert service.conversations[cid].deepseek_state=={}
    asyncio.run(run())


@pytest.mark.parametrize('failure',['cancel','empty','length','tool'])
def test_failed_compaction_never_replaces_original_memory(service, monkeypatch, failure):
    """Cancellation, truncation and unsolicited tool calls cannot commit a new summary."""
    import ai_deepseek
    service.credentials.save_deepseek_key(OWNER,KEY)
    async def run():
        """Call the private summary stage without allowing any real provider request."""
        cid=await service.create_conversation(OWNER,'deepseek','deepseek-flash')
        conversation=service.conversations[cid]
        history=[{'role':'user' if i%2==0 else 'assistant','content':f'message {i}','context_id':f'{i:032x}'} for i in range(19)]
        before=copy.deepcopy(history)
        payload={'choices':[{'finish_reason':'length' if failure=='length' else 'stop','message':{'content':'' if failure=='empty' else 'summary'}}]}
        if failure=='tool':payload['choices'][0]['message']['tool_calls']=[{'function':{'name':'delete_user'}}]
        call=AsyncMock(side_effect=asyncio.CancelledError() if failure=='cancel' else None,return_value=payload)
        monkeypatch.setattr('ai_deepseek_context.request',call)
        monkeypatch.setattr(service,'_http_session',AsyncMock(return_value=None))
        if failure=='cancel':
            with pytest.raises(asyncio.CancelledError):
                await prepare(service,OWNER,conversation,history,ai_deepseek.models(catalog_payload())[0],KEY)
            assert conversation.deepseek_state=={}
        else:
            projected=await prepare(service,OWNER,conversation,history,ai_deepseek.models(catalog_payload())[0],KEY)
            assert len(projected)==len(history) and 'summary' not in conversation.deepseek_state
            assert conversation.deepseek_state['summary_retry_at']>0
            await prepare(service,OWNER,conversation,history,ai_deepseek.models(catalog_payload())[0],KEY)
            assert call.await_count==1  # No repeated paid summary inside the cooldown.
        assert history==before
    asyncio.run(run())


def test_deepseek_research_approval_model_pin_and_key_isolation(service, monkeypatch):
    """Only a trusted approval starts bounded public search; Exa never sees the API key."""
    import ai_research_opencode as adapter
    from tests.test_ai_research_opencode import model_response, search_response
    service.credentials.save_deepseek_key(OWNER,KEY)
    calls=[]
    async def post(session,url,headers,body):
        """Answer only synthetic public queries and model messages, never production endpoints."""
        calls.append((url,headers,body))
        if url==adapter.SEARCH_URL:
            assert KEY not in json.dumps(headers)+json.dumps(body)
            assert set(headers)=={'Accept'}
            return search_response()
        assert url=='https://api.deepseek.com/v1/chat/completions'
        assert headers['Authorization']=='Bearer '+KEY
        assert body['model']=='deepseek-flash' and body['reasoning_effort']=='max' and 'tools' not in body
        text=body['messages'][-1]['content']
        assert 'private chat' not in text and 'history' not in json.loads(text)
        stage=json.loads(text)['stage']
        return model_response('chat','{"queries":["public BTC security"]}' if stage=='plan' else 'Findings https://example.test/report',model='deepseek-flash')
    monkeypatch.setattr(adapter,'_post',post)
    async def run():
        """Verify approval boundaries, completed report and revocation of unused previews."""
        research=service.research
        preview=await research.preview(OWNER,provider='deepseek',model='deepseek-flash',profile='default',prompt='Research public BTC security',effort='max')
        assert not calls and 'DeepSeek research workflow' in preview['instructions'] and 'Exa' in preview['instructions']
        with pytest.raises(AIChatError):research.start('b'*32,preview['id'],preview['digest'])
        with pytest.raises(AIChatError):research.start(OWNER,preview['id'],'bad')
        await research.approve(OWNER,preview['id'],preview['digest'])
        await research.tasks[preview['id']]
        result=research.get(OWNER,preview['id'])
        assert result['status']=='completed',result['error']
        assert result['web_calls']==1 and len(calls)==3
        pending=await research.preview(OWNER,provider='deepseek',model='deepseek-flash',profile='default',prompt='Another public query')
        await service.disconnect_deepseek(OWNER)
        assert research.get(OWNER,pending['id'])['status']=='cancelled'
        assert not service.credentials.deepseek_configured(OWNER)
    asyncio.run(run())


def test_disconnect_awaits_running_deepseek_research(service, monkeypatch):
    """Disconnect cancels owned model/search work before deleting the credential."""
    import ai_research_opencode as adapter
    service.credentials.save_deepseek_key(OWNER,KEY)
    async def run():
        """Hold a simulated request and observe deterministic cancellation cleanup."""
        entered,closed=asyncio.Event(),asyncio.Event()
        async def post(*args):
            try:
                entered.set()
                await asyncio.Event().wait()
            finally:
                closed.set()
        monkeypatch.setattr(adapter,'_post',post)
        research=service.research
        preview=await research.preview(OWNER,provider='deepseek',model='deepseek-flash',profile='default',prompt='Public query')
        await research.approve(OWNER,preview['id'],preview['digest'])
        task=research.tasks[preview['id']]
        await asyncio.wait_for(entered.wait(),2)
        await service.disconnect_deepseek(OWNER)
        assert task.done() and closed.is_set()
        assert research.get(OWNER,preview['id'])['status']=='cancelled'
        assert not service.credentials.deepseek_configured(OWNER)
    asyncio.run(run())


def test_deepseek_final_research_analysis_is_tool_free(service, monkeypatch):
    """Optional Jev synthesis stays on the selected DeepSeek model and public evidence only."""
    from ai_research_summary import summarize_research, SUMMARY_INSTRUCTIONS
    service.credentials.save_deepseek_key(OWNER,KEY)
    record={'owner':OWNER,'provider':'deepseek','model':'deepseek-flash','profile':'default','effort':'max',
            'prompt':'Public question','answer':'Public report','jev_answer':'Public classification','id':'f'*32}
    async def model(session,chat,isolated,metadata,key,content):
        assert key==KEY and isolated['instructions']==SUMMARY_INSTRUCTIONS
        assert isolated['provider']=='deepseek' and isolated['model']=='deepseek-flash'
        assert content=={'stage':'answer','approved_prompt':'Public question','untrusted_research':'Public report','untrusted_jev':'Public classification'}
        return 'Final public table'
    monkeypatch.setattr('ai_research_opencode._model_text',model)
    assert asyncio.run(summarize_research(service,record))=='Final public table'


def test_deepseek_usage_renderers_are_safe_and_clear_missing_values():
    """Actual native-browser renderers show real balances/tokens and never parse HTML."""
    from pathlib import Path
    from javascript_checks import run_javascript
    source=(Path(__file__).resolve().parents[1]/'frontend/js/ai_usage.js').read_text()
    script=source+'''
    const assert=browserAssert, box=document.createElement('div');
    window.PBGuiAIUsage.renderDeepSeek(box,{connected:true,is_available:false,balances:[{currency:'USD',total_balance:'12.34'}],message:'<img src=x onerror=alert(1)>'});
    assert.equal(box.querySelectorAll('img').length,0);
    assert.match(box.textContent,/12.34/);assert.match(box.textContent,/Insufficient/);
    window.PBGuiAIUsage.renderDeepSeek(box,{connected:true,message:'Unavailable'});
    assert.equal(box.textContent,'Unavailable');assert.equal(box.textContent.includes('$0'),false);
    window.PBGuiAIUsage.renderTokens(box,{input_tokens:120,output_tokens:40,cached_tokens:20,reasoning_tokens:10,compactions:1});
    assert.equal(box.hidden,false);assert.match(box.textContent,/120 input/);assert.match(box.title,/Context compactions: 1/);
    window.PBGuiAIUsage.renderTokens(box,null);
    assert.equal(box.hidden,true);assert.equal(box.textContent,'');
    '''
    run_javascript(['-e',script],text=True,capture_output=True,check=True)
