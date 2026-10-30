"""Real AI drawer menu path supports direct English issue review without Debug export."""
import json
from pathlib import Path
from urllib.parse import urlparse

import pytest
from browser_driver import expect, sync_browser

ROOT=Path(__file__).resolve().parents[2]


@pytest.mark.parametrize('prefix,width',[('',1400),('/mounted',700)])
def test_chat_connect_describe_review_publish_and_restore(prefix,width):
    """Personal credentials bypass chat text; only the reviewed publication calls approve."""
    cid='b'*32; pid='c'*32; review_id='d'*32
    body='### Feature request\nAdd yearly and all-time ranges to the dashboard.\n\nLiteral <img src=x onerror=alert(1)> example.'
    proposal={'proposal_id':pid,'conversation_id':cid,'payload_digest':'fixture-digest','status':'awaiting_approval',
        'preview':{'action':'publish_github_issue','name':'Add dashboard ranges','title':'Add dashboard ranges',
                   'body':body,'login':'fixture-user','target':'msei99/pbgui','review_id':review_id,'encrypted_attachment':False}}
    state={'connected':False,'sent':False,'published':False}; turns=[]; approvals=[]; tokens=[]; urls=[]; errors=[]
    with sync_browser() as driver:
        browser=driver.chromium.launch()
        page=browser.new_page(viewport={'width':width,'height':850})
        page.on('pageerror',lambda error:errors.append(str(error)))
        def respond(route):
            """Serve real shared UI with synthetic native-chat and authenticated support APIs."""
            urls.append(route.request.url)
            path=urlparse(route.request.url).path.removeprefix(prefix)
            if path=='/app/start.html':
                route.fulfill(content_type='text/html',body='<nav id="topnav"></nav><script>window.PBGUI_BASE_PREFIX='+json.dumps(prefix)+';window.PBGUI_NAV_CONFIG={current:"welcome",authenticated:true};</script><script src="'+prefix+'/app/pbgui_nav.js"></script>')
            elif path.startswith('/app/'):
                asset=ROOT/'frontend'/path.removeprefix('/app/')
                route.fulfill(body=asset.read_bytes() if asset.is_file() else b'',content_type='text/css' if asset.suffix=='.css' else 'text/javascript')
            elif path=='/api/debug/github':
                route.fulfill(json={'configured':state['connected'],'login':'fixture-user' if state['connected'] else ''})
            elif path=='/api/debug/github/connect':
                tokens.append(route.request.post_data_json);state['connected']=True
                route.fulfill(json={'configured':True,'login':'fixture-user'})
            elif path=='/api/ai/status':
                route.fulfill(json={'providers':{'opencode-go':{'connected':True}}})
            elif path=='/api/ai/models':
                route.fulfill(json={'models':[{'id':'test-model','name':'Test model','tools':True}]})
            elif path=='/api/ai/conversations':
                route.fulfill(json={'conversations':[{'conversation_id':cid,'title':'Support chat','model':'test-model'}]})
            elif path=='/api/ai/conversations/'+cid:
                route.fulfill(json={'conversation_id':cid,'provider':'opencode-go','model':'test-model','busy':False,'analysis_only':False,
                    'messages':[{'role':'user','content':'Add dashboard ranges'},{'role':'assistant','content':'English issue prepared for review.'}] if state['sent'] else []})
            elif path.endswith('/proposals'):
                route.fulfill(json={'proposals':[proposal] if state['sent'] and not state['published'] else []})
            elif path.endswith('/turns'):
                turns.append(route.request.post_data_json);state['sent']=True
                route.fulfill(json={'turn_id':'fixture','status':'completed'})
            elif path=='/api/ai/proposals/'+pid+'/approve':
                approvals.append(route.request.post_data_json);state['published']=True
                route.fulfill(json={'status':'executed','action':'publish_github_issue','issue_url':'https://github.com/msei99/pbgui/issues/123','review_id':review_id})
            elif path.endswith('/stream'):
                route.fulfill(status=204,body='')
            else:
                route.fulfill(json={})
        page.route('**/*',respond)
        page.goto('http://chat-support.test'+prefix+'/app/start.html')
        page.locator('#pbgui-ai-btn').click()
        expect(page.locator('#pai-model')).to_have_value('test-model')
        expect(page.locator('.pai-support')).to_have_count(0)
        prompt=page.locator('.pai-compose textarea')
        prompt.fill('Erstelle ein Feature Request: Im Dashboard Jahre und Allzeit auswählen.')
        prompt.press('Enter')
        page.get_by_role('button',name='Review changes',exact=True).click()
        expect(page.locator('.pai-review-content')).to_contain_text(body)
        expect(page.locator('.pai-review-content img')).to_have_count(0)
        assert not approvals
        page.get_by_role('button',name='Publish issue',exact=True).click()
        expect(page.get_by_role('link',name='Open published GitHub issue',exact=True)).to_have_attribute('href','https://github.com/msei99/pbgui/issues/123')
        assert approvals==[{'payload_digest':'fixture-digest','conversation_id':cid}]
        assert len(turns)==1 and 'Feature Request' in turns[0]['message']
        assert tokens==[]
        storage=page.evaluate('JSON.stringify({local:{...localStorage},session:{...sessionStorage}})')
        assert 'ghp_SYNTHETIC' not in storage and all('ghp_SYNTHETIC' not in url for url in urls)
        assert not any('/api/debug/main_page' in url or '/api/debug/collect' in url for url in urls)
        page.reload();page.locator('#pbgui-ai-btn').click()
        expect(page.locator('.pai-support')).to_have_count(0)
        assert len(approvals)==1 and not errors
        browser.close()
