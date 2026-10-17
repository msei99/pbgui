"""Verify that AI Setup displays and cancels browser login with actual local assets."""
from pathlib import Path
from urllib.parse import urlparse
from playwright.sync_api import expect, sync_playwright

ROOT = Path(__file__).resolve().parents[2]


def test_browser_login_shows_link_and_cancel():
    """The real setup page exposes the returned URL without popup dependence."""
    html = (ROOT / 'frontend/ai_chat.html').read_text()
    for marker, value in [('API_BASE', '/api/ai'), ('BASE_PREFIX', ''), ('VERSION', 'fixture'), ('SERIAL', '1'), ('NAV_HASH', 'test')]:
        html = html.replace('%%' + marker + '%%', value)
    cancelled, errors = [], []
    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        page = browser.new_page()
        page.on('pageerror', lambda error: errors.append(str(error)))

        def respond(route):
            """Serve production assets and synthetic provider responses only."""
            path = urlparse(route.request.url).path
            if path == '/chat':
                route.fulfill(content_type='text/html', body=html)
            elif path.startswith('/app/'):
                asset = ROOT / 'frontend' / path.removeprefix('/app/')
                route.fulfill(body=asset.read_bytes() if asset.is_file() else b'', content_type='text/css' if asset.suffix == '.css' else 'text/javascript')
            elif path.endswith('/status'):
                route.fulfill(json={'providers': {'chatgpt': {'available': True, 'connected': False, 'profiles': [{'id': 'default', 'name': 'Default'}]}}})
            elif path.endswith('/browser-login'):
                route.fulfill(json={'auth_url': 'https://example.test/login'})
            elif path.endswith('/login-cancel'):
                cancelled.append(route.request.url)
                route.fulfill(json={})
            else:
                route.fulfill(json={})

        page.route('**/*', respond)
        page.goto('http://pbgui.test/chat?section=chatgpt')
        expect(page.locator('#chatgpt-connect')).to_be_enabled()
        page.locator('#chatgpt-connect').click()
        expect(page.locator('#login-link')).to_be_visible()
        expect(page.locator('#login-link')).to_have_attribute('href', 'https://example.test/login')
        page.locator('#login-cancel').click()
        expect(page.locator('#login-box')).not_to_have_class('visible')
        page.wait_for_function('!document.querySelector("#login-box").classList.contains("visible")')
        assert len(cancelled) == 1 and 'profile=default' in cancelled[0]
        assert not errors
        browser.close()
