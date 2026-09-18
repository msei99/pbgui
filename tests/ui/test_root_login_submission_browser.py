"""Browser-level regression for duplicate root login submissions."""
from pathlib import Path
from playwright.sync_api import sync_playwright


def test_delayed_login_accepts_one_submission_and_recovers():
    """Rapid form submissions send one request and restore input on rejection."""
    html = (Path(__file__).resolve().parents[2] / 'frontend/root_login.html').read_text()
    html = html.replace('%%API_ORIGIN%%', 'http://pbgui.test')
    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        page = browser.new_page()
        pending = []
        page.route('http://pbgui.test/', lambda route: route.fulfill(content_type='text/html', body=html))
        page.route('**/api/auth/login', lambda route: pending.append(route))
        page.goto('http://pbgui.test/')
        page.locator('#password').fill('isolated-test-value')
        page.evaluate("""() => {
            const form = document.getElementById('login-form');
            form.dispatchEvent(new Event('submit', {cancelable:true}));
            form.dispatchEvent(new Event('submit', {cancelable:true}));
        }""")
        page.wait_for_timeout(150)
        assert len(pending) == 1
        assert page.locator('#password').is_disabled()
        pending[0].fulfill(status=401, json={'detail': 'Invalid password'})
        page.wait_for_function("!document.getElementById('password').disabled")
        assert page.locator('#banner').inner_text() == 'Invalid password'
        browser.close()
