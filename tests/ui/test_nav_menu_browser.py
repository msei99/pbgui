"""Shared application-menu interaction with isolated browser assets and requests."""
from pathlib import Path
from urllib.parse import urlparse

import pytest


ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize('width', [700, 1280])
def test_open_navigation_menu_tracks_mouse_and_preserves_taps(width):
    """Mouse movement switches open menus; touch, keyboard and outside clicks still work."""
    playwright = pytest.importorskip('playwright.sync_api')
    errors = []
    with playwright.sync_playwright() as driver:
        browser = driver.chromium.launch(headless=True)
        page = browser.new_page(viewport={'width':width, 'height':800}, has_touch=True)
        page.on('pageerror', lambda error: errors.append(str(error)))

        def respond(route):
            """Serve the real shared nav without contacting any running application."""
            path = urlparse(route.request.url).path
            if path.startswith('/app/'):
                asset = ROOT / 'frontend' / path.removeprefix('/app/')
                route.fulfill(body=asset.read_bytes(), content_type='text/javascript' if asset.suffix=='.js' else 'text/css')
            elif path.endswith('/stream'):
                route.fulfill(status=204, body='')
            elif path=='/api/balance-calc/main_page':
                route.fulfill(content_type='text/html', body='''<nav id="topnav"></nav>
                    <main style="padding-top:500px"><input id="draft"></main>
                    <script>window.PBGUI_NAV_CONFIG={authenticated:true,current:'info_balance_calc'};</script>
                    <script src="/app/pbgui_nav.js?v=test"></script>''')
            else:
                route.fulfill(json={})

        page.route('**/*', respond)
        page.goto('http://nav.test/api/balance-calc/main_page')
        draft = page.locator('#draft')
        draft.fill('Keep my edits')
        destination = page.url
        groups = page.locator('#topnav .nav-group.open')

        def menu(name):
            """Locate a shared menu button by its stable group key."""
            return page.locator('.nav-group-btn[data-group="'+name+'"]')

        menu('information').hover()
        playwright.expect(groups).to_have_count(0)
        menu('system').click()
        for name in ['information','pbv7','pbv8','system']:
            menu(name).hover()
            playwright.expect(groups).to_have_count(1)
            playwright.expect(groups.locator('.nav-group-btn')).to_have_attribute('data-group', name)
            playwright.expect(groups.locator('.nav-dropdown')).to_be_visible()
            groups.locator('.nav-item').first.hover()
            playwright.expect(groups).to_have_count(1)
        # Real touch taps must not first hover-open and then click-close the new menu.
        menu('information').tap()
        playwright.expect(groups.locator('.nav-group-btn')).to_have_attribute('data-group', 'information')
        menu('pbv7').tap()
        playwright.expect(groups.locator('.nav-group-btn')).to_have_attribute('data-group', 'pbv7')
        menu('pbv7').tap()
        playwright.expect(groups).to_have_count(0)
        menu('system').focus()
        page.keyboard.press('Enter')
        playwright.expect(groups.locator('.nav-group-btn')).to_have_attribute('data-group', 'system')
        draft.click()
        playwright.expect(groups).to_have_count(0)
        menu('pbv8').hover()
        playwright.expect(groups).to_have_count(0)
        playwright.expect(draft).to_have_value('Keep my edits')
        assert page.url==destination
        assert not errors, errors
        browser.close()
