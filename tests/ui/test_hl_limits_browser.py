"""Exercise the Hyperliquid Limits layout and navigation with isolated API responses."""

from pathlib import Path
from urllib.parse import urlparse

import pytest

ROOT = Path(__file__).resolve().parents[2]


def test_limits_sidebar_preserves_filter_selection_and_live_values():
    """The shared shell retains all columns, history and URL state across reloads."""
    playwright = pytest.importorskip('playwright.sync_api')
    html = (ROOT / 'frontend/hl_limits.html').read_text()
    for marker, value in {'API_BASE': '/api/test', 'VERSION': 'test', 'SERIAL': '1', 'NAV_HASH': 'test'}.items():
        html = html.replace('%%' + marker + '%%', value)
    html = html.replace('<nav id="topnav">', '<nav id="topnav" style="height:52px;min-height:52px">')
    accounts = [
        {'users': ['active-wallet'], 'bots': [{'name': 'test-bot', 'host': 'test-vps', 'pb_version': '8'}],
         'hosts': ['test-vps'], 'used': 1234, 'cap': 5000, 'sampled_at': 100, 'state': 'ok'},
        {'users': ['idle-wallet'], 'bots': [], 'hosts': [], 'state': 'idle'},
    ]
    errors = []
    with playwright.sync_playwright() as driver:
        browser = driver.chromium.launch(headless=True)
        page = browser.new_page(viewport={'width': 1440, 'height': 900})
        page.on('pageerror', lambda error: errors.append(str(error)))

        def respond(route):
            """Serve local assets and deterministic data without production network access."""
            path = urlparse(route.request.url).path
            if path == '/limits':
                route.fulfill(body=html, content_type='text/html')
            elif path == '/app/pbgui_nav.js':
                route.fulfill(body='', content_type='text/javascript')
            elif path.startswith('/app/'):
                asset = ROOT / 'frontend' / path.removeprefix('/app/')
                route.fulfill(body=asset.read_bytes(), content_type='text/css' if asset.suffix == '.css' else 'text/javascript')
            elif path == '/api/test/user-rate-limits':
                route.fulfill(json={'accounts': accounts})
            elif path == '/api/test/user-rate-limits/history/active-wallet':
                route.fulfill(json={'samples': [{'sampled_at': 100, 'used': 1234, 'cap': 5000}]})
            else:
                route.fulfill(status=404, body='')

        page.route('**/*', respond)
        page.goto('http://limits.test/limits')
        playwright.expect(page.locator('#rows tr')).to_have_count(2)
        playwright.expect(page.locator('#sidebar #summary')).to_contain_text('2 wallets')
        assert page.locator('thead th').count() == 8
        assert page.locator('#rows tr').first.locator('td').count() == 8
        playwright.expect(page.locator('#rows')).to_contain_text("1'234")
        assert page.locator('thead th').first.evaluate('(e) => getComputedStyle(e).position') == 'sticky'
        page.locator('#sidebar-resize').focus()
        page.keyboard.press('End')
        assert round(page.locator('#sidebar').bounding_box()['width']) == 420
        main = page.locator('#main-content').bounding_box()
        assert round(main['x']) == 420 and round(main['x'] + main['width']) == 1440
        page.locator('#filter').fill('test-vps')
        playwright.expect(page.locator('#rows tr')).to_have_count(1)
        page.locator('#rows tr').click()
        playwright.expect(page.locator('#chart circle')).to_have_count(2)
        assert page.locator('#detail').bounding_box()['y'] < page.locator('.table-wrap').bounding_box()['y']
        page.reload()
        playwright.expect(page.locator('#filter')).to_have_value('test-vps')
        playwright.expect(page.locator('#rows tr.is-selected')).to_have_count(1)
        playwright.expect(page.locator('#chart circle')).to_have_count(2)
        assert round(page.locator('#sidebar').bounding_box()['width']) == 420
        accounts[0]['used'] = 1500
        page.evaluate('load()')
        playwright.expect(page.locator('#rows')).to_contain_text("1'500")
        playwright.expect(page.locator('#filter')).to_have_value('test-vps')
        playwright.expect(page.locator('#rows tr.is-selected')).to_have_count(1)
        page.set_viewport_size({'width': 390, 'height': 800})
        assert round(page.locator('#sidebar').bounding_box()['width']) == 390
        main = page.locator('#main-content').bounding_box()
        assert main['height'] >= 120 and main['y'] + main['height'] <= 801
        assert not errors
        browser.close()
