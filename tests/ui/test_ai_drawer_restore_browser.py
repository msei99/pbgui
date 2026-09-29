"""Existing drawer conversations must survive completion of history loading."""
from pathlib import Path
from urllib.parse import urlparse

import pytest
from playwright.sync_api import expect, sync_playwright

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize('busy', [False, True])
def test_existing_conversation_survives_open_reopen_and_poll(busy):
    """A restored transcript stays visible without sending a new user message."""
    conversation_id = 'a' * 32
    requests = []
    errors = []
    with sync_playwright() as driver:
        browser = driver.chromium.launch()
        page = browser.new_page()
        page.set_default_timeout(5000)
        page.on('pageerror', lambda error: errors.append(str(error)))
        def respond(route):
            """Serve actual navigation/assets with a synthetic saved conversation."""
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
                route.fulfill(json={'providers': {'opencode-go': {'connected': True}}})
            elif path == '/api/ai/models':
                route.fulfill(json={'models': [{'id': 'test-model', 'name': 'Test model', 'tools': True}]})
            elif path == '/api/ai/conversations':
                route.fulfill(json={'conversations': [{'conversation_id': conversation_id, 'title': 'Saved conversation', 'model': 'test-model'}]})
            elif path == '/api/ai/conversations/' + conversation_id:
                route.fulfill(json={'conversation_id': conversation_id, 'provider': 'opencode-go', 'model': 'test-model',
                    'busy': busy, 'messages': [{'role': 'user', 'content': 'My previous question'},
                                             {'role': 'assistant', 'content': 'My saved answer'}]})
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
        if busy:
            before = len([item for item in requests if item[1] == '/api/ai/conversations/' + conversation_id])
            page.wait_for_timeout(1200)
            assert len([item for item in requests if item[1] == '/api/ai/conversations/' + conversation_id]) > before
        page.locator('#pbgui-ai-drawer .pai-head button', has_text='X').click()
        page.locator('#pbgui-ai-btn').click()
        page.wait_for_timeout(250)
        expect(page.locator('.pai-messages')).to_contain_text('My saved answer')
        assert not any(path.endswith('/turns') for _, path in requests)
        assert not errors
        browser.close()
