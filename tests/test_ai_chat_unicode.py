"""Unicode-safe browser receipts and action retry regressions, with no live provider."""
import asyncio
import json
from pathlib import Path

import pytest

from ai_chat import AIChatService


@pytest.mark.parametrize('text', ['\ud83c', '\udf89', '\ud83c\udf89', '📊 Results', 'abc\ud83c'])
def test_page_context_unicode_is_always_utf8_serializable(text):
    """Truncated browser text cannot break the context persistence or provider prompt."""
    context = {'page_key':'v8_optimize','title':text,
               'controls':[{'id':'control_30','role':'button','name':text,'label':text,
                            'operations':['activate'],'visible_text':text,'tooltip':text,
                            'options':[{'value':text,'label':text}]}],
               'focused_field':{'label':text,'value':text}}
    result = AIChatService._validate_page_context(context)
    json.dumps(result, ensure_ascii=False).encode('utf-8')
    if text == '📊 Results':
        assert result['title'] == text
    elif text == '\ud83c\udf89':
        assert result['title'] == '🎉'
    else:
        assert '\ufffd' in result['title']


def test_browser_ack_with_truncated_emoji_unblocks_waiter(tmp_path):
    """A real receipt completes and persists even when a visible label ends halfway through an emoji."""
    async def scenario():
        service = AIChatService(tmp_path/'ai')
        owner = 'a'*32
        conversation = await service._conversation(owner,'opencode-go','model',None)
        result = {'ui_action':{'type':'page.perform_action','target':{'page_key':'v8_optimize'},
                               'payload':{'action':'activate','entity':{'kind':'ui_control','name':'control_30'}}}}
        task = asyncio.create_task(service._capture_ui_action(owner,conversation.id,result,wait_for_browser=True))
        while not conversation.ui_actions:
            await asyncio.sleep(0)
        action = conversation.ui_actions[0]['action_id']
        await service.acknowledge_ui_action(owner,conversation.id,action,
            context={'page_key':'v8_optimize','section':'results','title':'x'*127+'\ud83c'})
        await asyncio.wait_for(task,1)
        assert result['status'] == 'browser_acknowledged'
        assert not conversation.ui_actions
        assert conversation.context['title'].endswith('\ufffd')
        assert json.loads((service.conversation_root/owner/(conversation.id+'.json')).read_text())['context']['section']=='results'
        await service.shutdown()
    asyncio.run(scenario())


def test_browser_text_truncation_and_ack_retry_never_reclicks():
    """Run production text and receipt helpers in the isolated native browser."""
    from javascript_checks import run_javascript
    root = Path(__file__).resolve().parents[1]
    nav=(root/'frontend/pbgui_nav.js').read_text()
    text=nav[nav.index('  function aiContextText('):nav.index('  function aiContextSensitiveName(')]
    drawer=(root/'frontend/js/ai_drawer.js').read_text()
    actions=drawer[drawer.index('  function acknowledgeHandledUiAction('):drawer.index('  function renderQuickReplies(')]
    script=r'''
const assert=browserAssert;
''' + text + r'''
assert.equal(aiContextText('x'.repeat(159)+'🎉',160),'x'.repeat(159));
assert.equal(aiContextText('📊 Results',160),'📊 Results');
assert.equal(aiContextText('x\ud83c',160),'x\ufffd');
const root=document.body,state={current:'chat',uiActionIds:new Set(),uiActionAcks:new Map()};
let clicks=0,acks=0;
function collectContext(){return {page_key:'v8_optimize'};}
function setStatus(){}
function renderQuickReplies(){}
async function api(){acks++;if(acks===1)throw new Error('temporary acknowledgement failure');return {};}
window.addEventListener('pbgui:ai-ui-action',function(event){clicks++;event.preventDefault();event.detail.browser_completion=Promise.resolve();});
''' + actions + r'''
const action={action_id:'a'.repeat(32),type:'page.perform_action',target:{},payload:{}};
dispatchUiActions('chat',[action],true);
await new Promise(resolve=>setTimeout(resolve,0));
assert.equal(clicks,1);assert.equal(acks,1);
state.uiActionAcks.get(action.action_id).retryAt=0;
dispatchUiActions('chat',[action],true);
await new Promise(resolve=>setTimeout(resolve,0));
assert.equal(clicks,1);assert.equal(acks,2);assert.equal(state.uiActionAcks.size,0);
dispatchUiActions('chat',[action],false);assert.equal(clicks,1);
'''
    result=run_javascript(['-e','(async function(){'+script+'})()'],cwd=root,capture_output=True,text=True,timeout=15)
    assert result.returncode==0,result.stdout+result.stderr
