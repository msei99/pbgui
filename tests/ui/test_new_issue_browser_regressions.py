"""Browser regressions for disconnected AI providers and unavailable transfer previews."""

from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest
from playwright.sync_api import expect, sync_playwright

ROOT = Path(__file__).resolve().parents[2]


def _asset(route, path):
    """Serve local dependencies while isolating unrelated navigation services."""
    if path.endswith('/pbgui_nav.js'):
        route.fulfill(content_type='application/javascript', body='window.PBGuiAI={registerPageContext:()=>{}};')
    elif path.startswith('/app/'):
        local = ROOT / 'frontend' / path.removeprefix('/app/')
        assert local.resolve().is_relative_to(ROOT / 'frontend')
        if local.is_file():
            route.fulfill(path=str(local))
        else:
            route.fulfill(body='')
    else:
        route.fulfill(json={})


@pytest.mark.parametrize('connected_provider', ['', 'openrouter'])
def test_ai_chat_disconnected_profile_has_actionable_notice(connected_provider):
    """No doomed ChatGPT request occurs; another connected provider remains usable."""
    html = (ROOT / 'frontend/ai_chat.html').read_text().replace('%%API_BASE%%', '/api/ai')
    model_requests = []
    errors = []
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page()
        page.on('pageerror', lambda error: errors.append(str(error)))

        def respond(route):
            """Provide disconnected ChatGPT and an optional connected alternative."""
            url = urlparse(route.request.url)
            if url.path == '/api/ai/main_page':
                route.fulfill(content_type='text/html', body=html)
            elif url.path == '/api/ai/status':
                route.fulfill(json={'providers': {
                    'chatgpt': {'available': True, 'connected': False, 'profiles': [{'id': 'default', 'name': 'Default'}]},
                    'openrouter': {'connected': connected_provider == 'openrouter'},
                }})
            elif url.path == '/api/ai/models':
                model_requests.append(parse_qs(url.query)['provider'][0])
                route.fulfill(json={'models': [{'id': 'test-model', 'name': 'Test', 'tools': True}]})
            elif url.path == '/api/ai/conversations':
                route.fulfill(json={'conversations': []})
            else:
                _asset(route, url.path)

        page.route('**/*', respond)
        page.goto('http://pbgui.test/api/ai/main_page')
        expect(page.locator('#clear-chat')).to_be_enabled()
        if connected_provider:
            expect(page.locator('#provider-select')).to_have_value(connected_provider)
            expect(page.locator('#prompt')).to_be_enabled()
            assert model_requests == [connected_provider]
        else:
            expect(page.locator('#chat-status')).to_contain_text('Connect the selected provider')
            expect(page.locator('#prompt')).to_be_disabled()
            page.locator('#clear-chat').click()
            expect(page.locator('#chat-status')).to_contain_text('Connect the selected provider')
            assert model_requests == []
        assert errors == []
        browser.close()


@pytest.mark.parametrize('history_failure', [False, True])
def test_transfer_history_survives_preview_failure_and_recovers(history_failure):
    """History and preview fail independently, recover automatically, and preserve edits."""
    html = (ROOT / 'frontend/transfers.html').read_text().replace('%%API_BASE%%', '/api/profit-sweep')
    recovery = {'ready': False}
    errors = []
    operation = {'operation_id': 'existing-operation', 'route': 'perp_to_spot', 'status': 'unknown',
                 'requested_amount': '5', 'asset': 'USDT', 'can_reconcile': True}
    preview = {'user': 'alice', 'asset': 'USDT', 'routes': [
        {'id': 'perp_to_spot', 'source': 'Perps', 'destination': 'Spot', 'asset': 'USDT',
         'minimum_amount': 1, 'source_balance': 100, 'destination_balance': 0, 'max_transferable': 100}]}
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page()
        page.clock.install()
        page.on('pageerror', lambda error: errors.append(str(error)))

        def respond(route):
            """Mock exchange failures independently of the persisted operation records."""
            path = urlparse(route.request.url).path
            if path.endswith('/main_page'):
                route.fulfill(content_type='text/html', body=html)
            elif path.endswith('/transfers/users'):
                route.fulfill(json={'users': [{'name': 'alice', 'exchange': 'bybit'}]})
            elif path.endswith('/transfers/preview/alice'):
                if recovery['ready']:
                    route.fulfill(json=preview)
                else:
                    route.fulfill(status=503, json={'detail': 'Exchange snapshot incomplete: read_failed'})
            elif path.endswith('/transfers/operations/alice'):
                if history_failure and not recovery['ready']:
                    route.fulfill(status=503, json={'detail': 'History temporarily unavailable'})
                else:
                    route.fulfill(json={'operations': [operation]})
            else:
                _asset(route, path)

        page.route('**/*', respond)
        page.goto('http://pbgui.test/api/profit-sweep/transfers/main_page')
        expect(page.locator('#status')).to_contain_text('Exchange snapshot incomplete')
        expect(page.locator('#history')).not_to_contain_text('Loading account data')
        expect(page.locator('#submit')).to_be_disabled()
        if history_failure:
            expect(page.locator('#history')).to_contain_text('Transfer history unavailable')
        else:
            expect(page.locator('#history')).to_contain_text('existing-operation')
            expect(page.locator('#history button')).to_have_text('Reconcile')
        recovery['ready'] = True
        page.clock.fast_forward(5000)
        expect(page.locator('#status')).to_contain_text('updated automatically')
        expect(page.locator('#history')).to_contain_text('existing-operation')
        page.locator('#amount').fill('17')
        page.clock.fast_forward(5000)
        expect(page.locator('#amount')).to_have_value('17')
        expect(page.locator('#route-select')).to_have_value('perp_to_spot')
        expect(page.locator('#submit')).to_be_enabled()
        recovery['ready'] = False
        page.clock.fast_forward(5000)
        expect(page.locator('#submit')).to_be_disabled()
        expect(page.locator('#history')).to_contain_text('existing-operation')
        expect(page.locator('#amount')).to_have_value('17')
        assert errors == []
        browser.close()
