"""Exercise profile selection and navigation in the actual offline AI Chat page."""
import json
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[2]


def test_profiles_route_new_chats_and_restore_existing_chats():
    """Selection is explicit; reload and old chat navigation preserve profile binding."""
    html = (ROOT / 'frontend/ai_chat.html').read_text().replace('%%API_BASE%%', '/api/ai')
    work = 'b' * 32
    old_id, new_id = 'c' * 32, 'd' * 32
    profiles = [{'id': 'default', 'name': 'Personal'}, {'id': work, 'name': 'Work'}]
    conversations = {old_id: {'conversation_id': old_id, 'provider': 'chatgpt', 'model': 'model-default',
                            'chatgpt_profile': 'default', 'title': 'Legacy chat', 'messages': [], 'effort': '', 'busy': False}}
    created = []
    errors = []
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page(viewport={'width': 1100, 'height': 900})
        page.on('pageerror', lambda error: errors.append(str(error)))

        def respond(route):
            """Serve fixture APIs without authentication material or external requests."""
            url = urlparse(route.request.url)
            profile = parse_qs(url.query).get('profile', ['default'])[0]
            path = url.path
            if path == '/chat':
                route.fulfill(content_type='text/html', body=html)
            elif path.endswith('pbgui_dialogs.js'):
                route.fulfill(content_type='application/javascript', body='window.PBGuiDialogs={confirm:async()=>true};')
            elif path.endswith('/ai_usage.js'):
                route.fulfill(content_type='application/javascript', body=(Path(__file__).resolve().parents[2] / 'frontend/js/ai_usage.js').read_text())
            elif path.startswith('/app/'):
                route.fulfill(content_type='application/javascript', body='window.PBGuiAI={registerPageContext:()=>{}};')
            elif path == '/api/ai/status':
                route.fulfill(json={'providers': {'chatgpt': {'available': True, 'connected': True,
                    'plan': 'plus' if profile == 'default' else 'pro', 'email': profile + '@example.test',
                    'profile': profile, 'profiles': profiles, 'limits': [{'usedPercent': 25, 'windowDurationMins': 300}]}}})
            elif path == '/api/ai/models':
                route.fulfill(json={'models': [{'id': 'model-' + profile, 'name': 'Model ' + profile, 'tools': True}]})
            elif path == '/api/ai/conversations' and route.request.method == 'POST':
                body = json.loads(route.request.post_data)
                created.append(body)
                conversations[new_id] = {'conversation_id': new_id, 'provider': 'chatgpt', 'model': body['model'],
                    'chatgpt_profile': body['profile'], 'title': 'Work chat', 'messages': [], 'effort': '', 'busy': False}
                route.fulfill(json={'conversation_id': new_id})
            elif path == '/api/ai/conversations':
                route.fulfill(json={'conversations': list(conversations.values())})
            elif path.endswith('/turns'):
                route.fulfill(status=400, json={'detail': 'Fixture stops before contacting a model'})
            elif path.startswith('/api/ai/conversations/'):
                route.fulfill(json=conversations.get(path.rsplit('/', 1)[-1], {}))
            elif path.startswith('/api/ai/proposals'):
                route.fulfill(json={'proposals': []})
            else:
                route.fulfill(json={})

        page.route('**/*', respond)
        page.goto('http://pbgui.test/chat')
        page.wait_for_timeout(300)
        assert not errors, errors
        page.wait_for_function("document.querySelector('#chatgpt-profile').options.length === 2 && !document.querySelector('#chatgpt-profile').disabled")
        assert page.locator('#provider-select option').all_text_contents() == ['ChatGPT · Personal', 'ChatGPT · Work']
        page.locator('#provider-select').select_option('chatgpt:' + work)
        page.wait_for_function("document.querySelector('#chatgpt-status').textContent.includes('pro') && !document.querySelector('#send').disabled")
        assert '75% remaining' in page.locator('#chatgpt-limits').inner_text()
        assert '5-hour usage limit' in page.locator('#chatgpt-limits').inner_text()
        assert page.locator('#chatgpt-limits progress').get_attribute('value') == '75'
        assert 'conversation=new' in page.url
        page.locator('#prompt').fill('Hello')
        page.locator('#send').click()
        page.wait_for_function("document.querySelector('#chat-status').textContent.includes('Fixture stops')")
        assert created[0]['profile'] == work
        assert created[0]['provider'] == 'chatgpt'
        assert created[0]['model'] == 'model-' + work
        page.reload()
        page.wait_for_function("!document.querySelector('#chatgpt-profile').disabled")
        assert page.locator('#chatgpt-profile').input_value() == work
        assert new_id in page.url
        page.get_by_role('button', name='Legacy chat', exact=False).click()
        page.wait_for_function("document.querySelector('#chatgpt-profile').value === 'default' && !document.querySelector('#chatgpt-profile').disabled")
        assert page.locator('#provider-select').input_value() == 'chatgpt:default'
        assert 'Personal' in page.locator('#conversation-list').inner_text()
        page.locator('#chatgpt-profile').select_option(work)
        page.wait_for_function("document.querySelector('#provider-select').value === 'chatgpt:' + 'b'.repeat(32) && !document.querySelector('#send').disabled")
        assert not errors
        browser.close()
