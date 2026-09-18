"""Verify that the browser login response produces a visible usable link."""
from pathlib import Path
from urllib.parse import urlparse
from playwright.sync_api import sync_playwright


def test_browser_login_shows_link_and_cancel():
    """The actual page must expose the returned URL without popup dependence."""
    html = (Path(__file__).resolve().parents[2] / 'frontend/ai_chat.html').read_text().replace('%%API_BASE%%', '/api/ai')
    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        page = browser.new_page()
        def respond(route):
            """Serve isolated login fixtures only."""
            path = urlparse(route.request.url).path
            if path == '/chat':
                route.fulfill(content_type='text/html', body=html)
            elif path.endswith('/ai_usage.js'):
                route.fulfill(content_type='application/javascript', body=(Path(__file__).resolve().parents[2] / 'frontend/js/ai_usage.js').read_text())
            elif path.startswith('/app/'):
                route.fulfill(content_type='application/javascript', body='window.PBGuiAI={registerPageContext:()=>{}};')
            elif path.endswith('/status'):
                route.fulfill(json={'providers': {'chatgpt': {'available': True, 'connected': False, 'profiles': [{'id': 'default', 'name': 'Default'}]}}})
            elif path.endswith('/browser-login'):
                route.fulfill(json={'auth_url': 'https://example.test/login'})
            else:
                route.fulfill(json={})
        page.route('**/*', respond)
        page.goto('http://pbgui.test/chat')
        page.locator('#chatgpt-connect').click()
        page.wait_for_function("document.querySelector('#login-box').classList.contains('visible')")
        assert page.locator('#login-link').is_visible()
        assert page.locator('#login-link').get_attribute('href') == 'https://example.test/login'
        assert page.locator('#login-box').get_by_role('button').count() > 0
        browser.close()
