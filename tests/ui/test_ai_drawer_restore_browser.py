"""Existing drawer conversations must survive completion of history loading."""
from pathlib import Path
from urllib.parse import urlparse
import time

import pytest
from browser_driver import expect, sync_browser

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize('busy', [False, True])
@pytest.mark.parametrize('provider',['opencode-go'])
def test_existing_conversation_survives_open_reopen_and_poll(busy,provider):
    """A restored transcript stays visible without sending a new user message."""
    continuation = busy
    stopped = False
    held_responses = []
    hold_next = False
    conversation_id = 'a' * 32
    started_at = time.time() - 65
    requests = []
    errors = []
    with sync_browser() as driver:
        browser = driver.chromium.launch()
        page = browser.new_page()
        page.set_default_timeout(5000)
        page.on('pageerror', lambda error: errors.append(str(error)))
        def respond(route):
            """Serve actual navigation/assets with a synthetic saved conversation."""
            nonlocal busy, stopped, hold_next
            path = urlparse(route.request.url).path
            requests.append((route.request.method, path))
            if path == '/app/start.html':
                route.fulfill(content_type='text/html', body='''<nav id="topnav"></nav><script>
                window.PBGUI_NAV_CONFIG={current:'welcome',authenticated:true};
                </script><script src="/app/pbgui_nav.js?v=test"></script>''')
            elif path.startswith('/app/'):
                asset = ROOT / 'frontend' / path.removeprefix('/app/')
                route.fulfill(body=asset.read_bytes() if asset.is_file() else b'', content_type='text/css' if asset.suffix == '.css' else 'text/javascript')
            elif path == '/api/ai/status':
                route.fulfill(json={'providers': {provider: {'connected': True}}})
            elif path == '/api/ai/models':
                route.fulfill(json={'models': [{'id': 'test-model', 'name': 'Test model', 'tools': True}]})
            elif path == '/api/ai/conversations':
                route.fulfill(json={'conversations': [{'conversation_id': conversation_id, 'title': 'Saved conversation', 'model': 'test-model'}]})
            elif path == '/api/ai/conversations/' + conversation_id:
                if hold_next:
                    hold_next=False;held_responses.append(route)
                    page.evaluate('window.claudeConversationHeld=true')
                    return
                route.fulfill(json={'conversation_id': conversation_id, 'provider': provider, 'model': 'test-model',
                    'busy': busy, 'messages': [{'role': 'user', 'content': 'My previous question'},
                                             {'role': 'assistant', 'content': 'My saved answer'}] + ([{'role': 'assistant', 'content': 'My approved continuation answer'}] if continuation and not busy and not stopped else []),
                    'active_turn_id': 'turn-a' if busy else '',
                    'context_usage': {'used_tokens': 25000, 'limit_tokens': 100000},
                    'streaming_messages': [{'timestamp': started_at + 1, 'content': 'Checking **optimizer settings** now.'}] if busy else [],
                    'activity_history': [{'timestamp': started_at, 'message': 'Starting model'}]})
            elif path == '/api/ai/conversations/' + conversation_id + '/cancel':
                assert route.request.method=='POST'
                busy=False;stopped=True;route.fulfill(json={})
            elif path.endswith('/stream'):
                route.fulfill(status=204, body='')
            else:
                route.fulfill(json={})
        page.route('**/*', respond)
        page.goto('http://restore.test/app/start.html')
        page.locator('#pbgui-ai-btn').click()
        expect(page.locator('#pai-model')).to_have_value('test-model')
        # Wait for the complete history/proposal load, including the faulty trailing branch.
        page.wait_for_function("document.querySelector('.pai-history-list button.selected')")
        page.wait_for_timeout(250)
        expect(page.locator('.pai-messages')).to_contain_text('My saved answer')
        expect(page.locator('.pai-empty')).to_have_count(0)
        expect(page.locator('.pai-context-meter')).to_have_text('25%')
        assert page.locator('.pai-context-meter').get_attribute('title') == 'Context: 25,000 / 100,000 tokens (last reported)'
        assert page.locator('.pai-activity').evaluate('(node) => node.open') == busy
        assert page.locator('.pai-activity').evaluate('(node) => node.parentElement.classList.contains("pai-messages")')
        if busy:
            assert page.locator('.pai-activity').evaluate('(node) => node.previousElementSibling.textContent.includes("My saved answer") && !node.nextElementSibling')
        else:
            assert page.locator('.pai-activity').evaluate('(node) => node.previousElementSibling.classList.contains("user") && node.nextElementSibling.classList.contains("assistant")')
        expect(page.locator('.pai-chat > .pai-activity')).to_have_count(0)
        if busy:
            expect(page.locator('.pai-streaming-message')).to_contain_text('Checking optimizer settings now.')
            expect(page.locator('.pai-streaming-message strong')).to_have_text('optimizer settings')
        prompt = page.locator('.pai-compose textarea')
        if not busy:
            prompt.fill('Keep this unfinished message')
        handle = page.get_by_role('separator', name='Resize message input')
        initial_height = prompt.bounding_box()['height']
        box = handle.bounding_box()
        page.mouse.move(box['x'] + box['width'] / 2, box['y'] + box['height'] / 2)
        page.mouse.down()
        page.mouse.move(box['x'] + box['width'] / 2, box['y'] - 120, steps=5)
        page.mouse.up()
        assert prompt.bounding_box()['height'] > initial_height + 100
        if not busy:
            expect(prompt).to_have_value('Keep this unfinished message')
        resized_height = prompt.bounding_box()['height']
        if busy:
            expect(page.locator('.pai-working')).to_be_visible()
            assert page.locator('.pai-working-time').inner_text().startswith('1:')
            before_time = page.locator('.pai-working-time').inner_text()
            before = len([item for item in requests if item[1] == '/api/ai/conversations/' + conversation_id])
            page.wait_for_timeout(1200)
            assert len([item for item in requests if item[1] == '/api/ai/conversations/' + conversation_id]) > before
            assert page.locator('.pai-working-time').inner_text() != before_time
            expect(page.locator('.pai-activity summary')).to_be_hidden()
            if provider=='claude-code':
                hold_next=True
                page.wait_for_function('window.claudeConversationHeld===true')
                page.get_by_role('button',name='Stop',exact=True).click()
                expect(page.locator('.pai-working')).to_be_hidden()
                for route in held_responses:
                    route.fulfill(json={'conversation_id':conversation_id,'provider':provider,'model':'test-model','busy':True,
                                        'messages':[{'role':'assistant','content':'Late interrupted text'}]})
                page.wait_for_timeout(150)
                expect(page.get_by_role('button',name='Stop',exact=True)).to_be_hidden()
                expect(page.locator('.pai-messages')).not_to_contain_text('Late interrupted text')
                assert ('POST','/api/ai/conversations/'+conversation_id+'/cancel') in requests
            else:busy = False
            expect(page.locator('.pai-working')).to_be_hidden()
            expect(page.locator('.pai-activity summary')).to_be_visible()
            expect(page.locator('.pai-streaming-message')).to_have_count(0)
            if provider!='claude-code':
                assert page.locator('.pai-activity').evaluate('(node) => node.previousElementSibling.textContent.includes("My saved answer") && node.nextElementSibling.textContent.includes("My approved continuation answer")')
            else:expect(page.locator('.pai-messages')).not_to_contain_text('My approved continuation answer')
            assert not page.locator('.pai-activity').evaluate('(node) => node.open')
            page.locator('.pai-activity summary').click()
            page.wait_for_timeout(1200)
            assert page.locator('.pai-activity').evaluate('(node) => node.open')
        else:
            expect(page.locator('.pai-working')).to_be_hidden()
        page.locator('#pbgui-ai-drawer .pai-head button', has_text='X').click()
        page.locator('#pbgui-ai-btn').click()
        page.wait_for_timeout(250)
        expect(page.locator('.pai-messages')).to_contain_text('My saved answer')
        assert not any(path.endswith('/turns') for _, path in requests)
        assert abs(prompt.bounding_box()['height'] - resized_height) < 2
        page.reload()
        page.locator('#pbgui-ai-btn').click()
        expect(prompt).to_be_visible()
        assert abs(prompt.bounding_box()['height'] - resized_height) < 2
        handle.focus()
        handle.press('Home')
        assert prompt.bounding_box()['height'] == 44
        handle.press('End')
        page.set_viewport_size({'width': 700, 'height': 500})
        assert prompt.bounding_box()['height'] <= 225
        assert not errors
        browser.close()


@pytest.mark.parametrize('prefix', ['', '/mounted'])
def test_disconnected_claude_history_restores_without_starting_turns(prefix):
    """Disconnected Claude history keeps its identity, selection and live draft without fallback."""
    cid = 'c'*32
    model = 'claude-historical'
    selection = {'provider':'claude-code','model':model,'profile':'default','effort':'','service_tier':''}
    preferences = {'selection':selection}
    requests, errors = [], []
    with sync_browser() as driver:
        browser=driver.chromium.launch()
        page=browser.new_page()
        page.clock.install()
        page.on('pageerror', lambda error:errors.append(str(error)))
        def respond(route):
            """Only synthetic saved data and locally served browser assets are exposed."""
            url=urlparse(route.request.url);path=url.path.removeprefix(prefix)
            requests.append((route.request.method,path))
            if path in ('/app/first.html','/app/second.html'):
                route.fulfill(content_type='text/html',body='<nav id="topnav"></nav><script>window.PBGUI_BASE_PREFIX='+repr(prefix)+';window.PBGUI_NAV_CONFIG={current:"welcome",authenticated:true};</script><script src="'+prefix+'/app/pbgui_nav.js"></script>')
            elif path.startswith('/app/'):
                asset=ROOT/'frontend'/path.removeprefix('/app/')
                route.fulfill(body=asset.read_bytes() if asset.is_file() else b'',content_type='text/css' if asset.suffix=='.css' else 'text/javascript')
            elif path=='/api/ai/preferences':
                if route.request.method=='PUT':preferences.update(route.request.post_data_json)
                route.fulfill(json=preferences)
            elif path=='/api/ai/status':route.fulfill(json={'providers':{'claude-code':{'available':False,'connected':False,'catalog':[],'error':'Subscription sign-in unavailable'},'opencode-go':{'connected':True}}})
            elif path=='/api/ai/usage':route.fulfill(json={'message':'Subscription sign-in unavailable'})
            elif path=='/api/ai/conversations':route.fulfill(json={'conversations':[{'conversation_id':cid,'title':'Synthetic Claude history','model':model}]})
            elif path=='/api/ai/conversations/'+cid:route.fulfill(json={'conversation_id':cid,'provider':'claude-code','model':model,'busy':False,'messages':[{'role':'assistant','content':'Synthetic historical reply'}]})
            elif path.endswith('/stream'):route.fulfill(status=204,body='')
            else:
                assert not path.startswith('/api/ai/models') and not path.endswith('/turns')
                route.fulfill(json={})
        page.route('**/*',respond)
        for action in ('first','reload','second'):
            if action=='reload':page.reload()
            else:page.goto('http://history.test'+prefix+'/app/'+action+'.html')
            page.locator('#pbgui-ai-btn').click()
            page.wait_for_function('window.PBGuiAI && typeof window.PBGuiAI.open === "function"')
            page.evaluate('window.PBGuiAI.open()')
            expect(page.locator('.pai-messages')).to_contain_text('Synthetic historical reply')
            expect(page.locator('#pai-provider')).to_have_value('claude-code')
            expect(page.locator('#pai-model')).to_have_value(model)
            expect(page.locator('#pai-model')).to_be_disabled()
            expect(page.get_by_role('button',name='Send',exact=True)).to_be_disabled()
            expect(page.get_by_role('button',name='New',exact=True)).to_be_disabled()
            page.locator('.pai-compose textarea').fill('Unsent synthetic draft')
            page.locator('.pai-compose textarea').press('Enter')
            expect(page.locator('.pai-compose textarea')).to_have_value('Unsent synthetic draft')
            page.clock.fast_forward(31000)
            expect(page.locator('.pai-compose textarea')).to_have_value('Unsent synthetic draft')
            page.locator('#pbgui-ai-drawer .pai-head button',has_text='X').click()
            page.locator('#pbgui-ai-btn').click()
            expect(page.locator('.pai-compose textarea')).to_have_value('Unsent synthetic draft')
            assert 'Select a connected model' in page.evaluate('window.PBGuiAI.getSelection().then(()=>"unexpected", error=>error.message)')
        assert preferences['selection']==selection
        assert not any(method=='POST' and path.startswith('/api/ai/') for method,path in requests)
        assert not errors
        browser.close()
