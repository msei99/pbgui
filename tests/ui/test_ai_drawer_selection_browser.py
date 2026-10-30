"""Shared drawer selections persist across pages despite an older conversation."""
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest
from playwright.sync_api import expect, sync_playwright

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize('prefix', ['', '/mounted'])
def test_openrouter_drawer_updates_claude_inventory_without_losing_state(prefix):
    """OpenRouter notices Claude connection changes without losing the conversation or draft."""
    connected = {'claude': False, 'hold': False}
    held, requests, errors = [], [], []
    conversation_id = 'a' * 32
    model = 'typesafe/jev-1.13'
    selection = {'provider': 'openrouter', 'model': model, 'profile': 'default',
                 'effort': '', 'service_tier': ''}
    preferences = {'selection': selection}
    with sync_playwright() as driver:
        browser = driver.chromium.launch()
        page = browser.new_page()
        page.set_default_timeout(7000)
        page.clock.install()
        page.on('pageerror', lambda error: errors.append(str(error)))

        def respond(route):
            """Serve real assets with synthetic provider state and no model requests."""
            url = urlparse(route.request.url)
            path = url.path.removeprefix(prefix)
            requests.append((route.request.method, path))
            if path == '/app/fixture.html':
                route.fulfill(content_type='text/html', body='<nav id="topnav"></nav><script>'
                    'window.PBGUI_BASE_PREFIX=' + repr(prefix) + ';'
                    'window.PBGUI_NAV_CONFIG={current:"welcome",authenticated:true};'
                    '</script><script src="' + prefix + '/app/pbgui_nav.js"></script>')
            elif path.startswith('/app/'):
                asset = ROOT / 'frontend' / path.removeprefix('/app/')
                route.fulfill(body=asset.read_bytes() if asset.is_file() else b'',
                              content_type='text/css' if asset.suffix == '.css' else 'text/javascript')
            elif path == '/api/ai/preferences':
                if route.request.method == 'PUT':
                    preferences.update(route.request.post_data_json)
                route.fulfill(json=preferences)
            elif path == '/api/ai/status':
                page.evaluate('window.fixtureStatusCalls=(window.fixtureStatusCalls || 0)+1')
                data = {'providers': {'openrouter': {'connected': True}, 'claude-code': {
                    'available': False, 'connected': False, 'error': 'Subscription sign-in unavailable',
                    'catalog': [], 'fixture_external_change': connected['claude']}}}
                if connected['hold']:
                    data['providers']['claude-code'].update(available=True, connected=True, catalog=['claude-fixture'])
                    held.append((route, data))
                    page.evaluate('window.fixtureStatusHeld=true')
                else:
                    route.fulfill(json=data)
            elif path == '/api/ai/models':
                route.fulfill(json={'models': [{'id': model, 'name': 'Fixture Jev', 'decision': True}]})
            elif path == '/api/ai/conversations':
                route.fulfill(json={'conversations': [{'conversation_id': conversation_id,
                    'title': 'Fixture OpenRouter chat', 'model': model}]})
            elif path == '/api/ai/conversations/' + conversation_id:
                route.fulfill(json={'conversation_id': conversation_id, 'provider': 'openrouter',
                    'model': model, 'busy': False,
                    'messages': [{'role': 'assistant', 'content': 'Previous synthetic answer'}]})
            elif path.endswith('/stream'):
                route.fulfill(status=204, body='')
            else:
                route.fulfill(json={})

        page.route('**/*', respond)
        page.goto('http://inventory.test' + prefix + '/app/fixture.html')
        page.locator('#pbgui-ai-btn').click()
        expect(page.locator('#pai-model')).to_have_value(model)
        expect(page.locator('.pai-messages')).to_contain_text('Previous synthetic answer')
        page.locator('.pai-compose textarea').fill('Unsent synthetic draft')
        claude = page.locator('#pai-provider option[value="claude-code"]')
        expect(claude).to_have_count(0)

        for available in (True, False):
            before = page.evaluate('window.fixtureStatusCalls || 0')
            connected['claude'] = available
            page.clock.fast_forward(31000)
            page.wait_for_function('(before)=>window.fixtureStatusCalls>before', arg=before)
            expect(claude).to_have_count(0)
            assert any(path == '/api/ai/status' for _method, path in requests)
            expect(page.locator('#pai-provider')).to_have_value('openrouter')
            expect(page.locator('#pai-model')).to_have_value(model)
            expect(page.locator('.pai-compose textarea')).to_have_value('Unsent synthetic draft')
            expect(page.locator('.pai-messages')).to_contain_text('Previous synthetic answer')

        connected.update(claude=True, hold=True)
        page.clock.fast_forward(31000)
        page.wait_for_function('window.fixtureStatusHeld===true')
        connected.update(claude=False, hold=False)
        before = page.evaluate('window.fixtureStatusCalls')
        page.clock.fast_forward(31000)
        page.wait_for_function('(before)=>window.fixtureStatusCalls>before', arg=before)
        for route, data in held:
            route.fulfill(json=data)
        expect(claude).to_have_count(0)
        expect(page.locator('#pai-provider')).to_have_value('openrouter')
        expect(page.locator('.pai-compose textarea')).to_have_value('Unsent synthetic draft')
        assert preferences['selection'] == selection
        assert not any(method == 'POST' and path.startswith('/api/ai/') for method, path in requests)
        assert not errors
        browser.close()


@pytest.mark.parametrize('unavailable', [False, True])
@pytest.mark.parametrize('chosen_provider',['chatgpt'])
def test_gpt_selection_survives_navigation_reload_and_old_deepseek_chat(unavailable,chosen_provider):
    """Real navigation/drawer code uses durable preferences, never a silent fallback."""
    preferences = {}
    writes, errors = [], []
    profile = 'b' * 32
    conversation_id = 'a' * 32
    with sync_playwright() as driver:
        browser = driver.chromium.launch()
        page = browser.new_page()
        page.set_default_timeout(7000)
        page.on('pageerror', lambda error: errors.append(str(error)))

        def respond(route):
            """Serve real shared assets and owner preference round trips without AI calls."""
            url = urlparse(route.request.url)
            path = url.path
            if path in ('/app/first.html', '/app/second.html'):
                route.fulfill(content_type='text/html', body='''<nav id="topnav"></nav><script>
                window.PBGUI_NAV_CONFIG={current:'welcome',authenticated:true};
                </script><script src="/app/pbgui_nav.js?v=test"></script>''')
            elif path.startswith('/app/'):
                asset = ROOT / 'frontend' / path.removeprefix('/app/')
                route.fulfill(body=asset.read_bytes() if asset.is_file() else b'',
                              content_type='text/css' if asset.suffix == '.css' else 'text/javascript')
            elif path == '/api/ai/preferences':
                if route.request.method == 'PUT':
                    body = route.request.post_data_json
                    preferences.update(body)
                    if 'selection' in body:
                        writes.append(body['selection'])
                route.fulfill(json=preferences)
            elif path == '/api/ai/status':
                route.fulfill(json={'providers': {'chatgpt': {'connected': True, 'profiles': [
                    {'id': profile, 'name': 'Work'}]}, 'opencode-go': {'connected': True}, 'claude-code': {'connected':True}}})
            elif path == '/api/ai/models':
                provider = parse_qs(url.query)['provider'][0]
                model = 'deepseek' if provider == 'opencode-go' else 'claude-fixture' if provider=='claude-code' else 'gpt-6.1-sol'
                model_list = [{'id': model, 'name': model, 'default': True,
                               'reasoning_variants': [{'id': 'high', 'label': 'High'}],
                               'service_tiers': [{'id': 'fast', 'label': 'Fast'}]}]
                if unavailable and writes and provider == chosen_provider:
                    model_list = [{'id': 'another-model', 'name': 'Another model', 'default': True}]
                route.fulfill(json={'models': model_list})
            elif path == '/api/ai/conversations':
                route.fulfill(json={'conversations': [{'conversation_id': conversation_id,
                    'title': 'Old DeepSeek chat', 'model': 'deepseek'}]})
            elif path == '/api/ai/conversations/' + conversation_id:
                route.fulfill(json={'conversation_id': conversation_id, 'provider': 'opencode-go',
                    'model': 'deepseek', 'busy': False, 'messages': [
                        {'role': 'assistant', 'content': 'Previous DeepSeek answer'}]})
            elif path.endswith('/stream'):
                route.fulfill(status=204, body='')
            else:
                route.fulfill(json={})

        page.route('**/*', respond)
        page.goto('http://selection.test/app/first.html')
        page.locator('#pbgui-ai-btn').click()
        expect(page.locator('#pai-model')).to_have_value('deepseek')
        provider_value = 'chatgpt:'+profile if chosen_provider=='chatgpt' else chosen_provider
        chosen_model = 'gpt-6.1-sol' if chosen_provider=='chatgpt' else 'claude-fixture'
        page.locator('#pai-provider').select_option(provider_value)
        expect(page.locator('#pai-model')).to_have_value(chosen_model)
        if chosen_provider=='chatgpt':page.locator('#pai-effort').select_option('high')
        if chosen_provider=='chatgpt':page.locator('#pai-speed').select_option('fast')
        selection = page.evaluate('window.PBGuiAI.getSelection()')
        assert selection['provider'] == chosen_provider
        assert selection['model'] == chosen_model
        assert selection['effort'] == ('high' if chosen_provider=='chatgpt' else '')
        assert selection['service_tier'] == ('fast' if chosen_provider=='chatgpt' else '')
        assert preferences['selection'] == selection
        for action in ('navigate', 'reload'):
            if action == 'navigate':
                page.goto('http://selection.test/app/second.html')
            else:
                page.reload()
            expect(page.locator('#pai-provider')).to_have_value(provider_value)
            expect(page.locator('#pai-model')).to_have_value(chosen_model)
            expect(page.locator('.pai-messages')).to_contain_text('Previous DeepSeek answer')
            if unavailable:
                assert 'Unavailable model' in page.locator('#pai-model').inner_text()
                result = page.evaluate('window.PBGuiAI.getSelection().then(() => "wrong", e => e.message)')
                assert 'Select a connected model' in result
            else:
                expect(page.locator('#pai-effort')).to_have_value('high' if chosen_provider=='chatgpt' else '')
                assert page.evaluate('window.PBGuiAI.getSelection()') == selection
        assert writes[-1] == selection
        assert not errors
        browser.close()
