"""Existing drawer conversations must survive completion of history loading."""
from pathlib import Path
from urllib.parse import urlparse
import time

import pytest
from playwright.sync_api import expect, sync_playwright

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize('busy', [False, True])
def test_existing_conversation_survives_open_reopen_and_poll(busy):
    """A restored transcript stays visible without sending a new user message."""
    continuation = busy
    conversation_id = 'a' * 32
    started_at = time.time() - 65
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
                                             {'role': 'assistant', 'content': 'My saved answer'}] + ([{'role': 'assistant', 'content': 'My approved continuation answer'}] if continuation and not busy else []),
                    'active_turn_id': 'turn-a' if busy else '',
                    'context_usage': {'used_tokens': 25000, 'limit_tokens': 100000},
                    'streaming_messages': [{'timestamp': started_at + 1, 'content': 'Checking **optimizer settings** now.'}] if busy else [],
                    'activity_history': [{'timestamp': started_at, 'message': 'Starting model'}]})
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
            busy = False
            expect(page.locator('.pai-working')).to_be_hidden()
            expect(page.locator('.pai-activity summary')).to_be_visible()
            expect(page.locator('.pai-streaming-message')).to_have_count(0)
            assert page.locator('.pai-activity').evaluate('(node) => node.previousElementSibling.textContent.includes("My saved answer") && node.nextElementSibling.textContent.includes("My approved continuation answer")')
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
