"""Shared drawer selections persist across pages despite an older conversation."""
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest
from playwright.sync_api import expect, sync_playwright

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize('unavailable', [False, True])
def test_gpt_selection_survives_navigation_reload_and_old_deepseek_chat(unavailable):
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
                    {'id': profile, 'name': 'Work'}]}, 'opencode-go': {'connected': True}}})
            elif path == '/api/ai/models':
                provider = parse_qs(url.query)['provider'][0]
                model = 'deepseek' if provider == 'opencode-go' else 'gpt-6.1-sol'
                model_list = [{'id': model, 'name': model, 'default': True,
                               'reasoning_variants': [{'id': 'high', 'label': 'High'}],
                               'service_tiers': [{'id': 'fast', 'label': 'Fast'}]}]
                if unavailable and writes and provider == 'chatgpt':
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
        page.locator('#pai-provider').select_option('chatgpt:' + profile)
        expect(page.locator('#pai-model')).to_have_value('gpt-6.1-sol')
        page.locator('#pai-effort').select_option('high')
        page.locator('#pai-speed').select_option('fast')
        selection = page.evaluate('window.PBGuiAI.getSelection()')
        assert selection['provider'] == 'chatgpt'
        assert selection['model'] == 'gpt-6.1-sol'
        assert selection['effort'] == 'high'
        assert selection['service_tier'] == 'fast'
        assert preferences['selection'] == selection
        for action in ('navigate', 'reload'):
            if action == 'navigate':
                page.goto('http://selection.test/app/second.html')
            else:
                page.reload()
            expect(page.locator('#pai-provider')).to_have_value('chatgpt:' + profile)
            expect(page.locator('#pai-model')).to_have_value('gpt-6.1-sol')
            expect(page.locator('.pai-messages')).to_contain_text('Previous DeepSeek answer')
            if unavailable:
                assert 'Unavailable model' in page.locator('#pai-model').inner_text()
                result = page.evaluate('window.PBGuiAI.getSelection().then(() => "wrong", e => e.message)')
                assert 'Select a connected model' in result
            else:
                expect(page.locator('#pai-effort')).to_have_value('high')
                assert page.evaluate('window.PBGuiAI.getSelection()') == selection
        assert writes[-1] == selection
        assert not errors
        browser.close()
