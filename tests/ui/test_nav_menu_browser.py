"""Shared application-menu interaction with isolated browser assets and requests."""
import json
import mimetypes
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlparse

import pytest


ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(params=['', '/mounted'], ids=['root', 'mounted'])
def dashboard_nav(request):
    """Serve real Dashboard/Services bootstraps, local assets and synthetic API data."""
    browser_api = __import__('browser_driver')
    prefix = request.param
    requests, errors, outside_mount = [], [], []
    with browser_api.sync_browser() as driver:
        browser = driver.chromium.launch(headless=True)
        try:
            context = browser.new_context(viewport={'width': 1280, 'height': 800}, service_workers='block')
            try:
                context.set_default_timeout(5000)
                context.set_default_navigation_timeout(10000)
                context.route_web_socket('**/*', lambda socket: socket.close())

                def respond(route):
                    """Fulfil every request locally; never contact an application or host."""
                    url = urlparse(route.request.url)
                    requests.append(url.path)
                    if url.hostname != 'nav.test' or not url.path.startswith(prefix + '/'):
                        outside_mount.append(url.path)
                        route.fulfill(status=404, body='Outside isolated application mount')
                        return
                    path = url.path.removeprefix(prefix)
                    templates = {
                        '/api/dashboard/main_page': 'dashboard_main.html',
                        '/api/services/main_page': 'services_monitor.html',
                    }
                    if path in templates:
                        html = (ROOT / 'frontend' / templates[path]).read_text()
                        api = prefix + ('/api' if 'dashboard' in path else '/api/services')
                        for marker, value in {
                            'API_BASE': api, 'BASE_PREFIX': prefix, 'VERSION': 'fixture',
                            'SERIAL': '1', 'CURRENT': '', 'NAV_HASH': 'fixture',
                        }.items():
                            html = html.replace('"%%' + marker + '%%"', json.dumps(value))
                            html = html.replace('%%' + marker + '%%', value)
                        html = html.replace('%%DASHBOARDS_JSON%%', '[]')
                        # Equivalent to the handlers' mounted local-asset rewriting.
                        html = html.replace('src="/app/', 'src="' + prefix + '/app/')
                        html = html.replace('href="/app/', 'href="' + prefix + '/app/')
                        assert '%%' not in html
                        route.fulfill(content_type='text/html', body=html)
                    elif path.startswith('/app/'):
                        asset = ROOT / 'frontend' / path.removeprefix('/app/')
                        assert asset.resolve().is_relative_to(ROOT / 'frontend')
                        route.fulfill(body=asset.read_bytes(), content_type=mimetypes.guess_type(asset)[0] or 'application/octet-stream')
                    elif path == '/api/help/index':
                        route.fulfill(json=[{'file': '33_dashboard', 'title': 'Dashboards'},
                                            {'file': '23_services_overview', 'title': 'Services'}])
                    elif path == '/api/help/content':
                        route.fulfill(json={'content': '# Isolated Guide\n\nSynthetic documentation.'})
                    elif path == '/api/dashboards':
                        route.fulfill(json={'dashboards': []})
                    elif path == '/api/services/workers/summary':
                        route.fulfill(json={'counts': {'total': 0, 'running': 0}})
                    elif path.endswith('/stream'):
                        route.fulfill(status=204, body='')
                    else:
                        route.fulfill(json={})

                context.route('**/*', respond)
                page = context.new_page()
                page.on('pageerror', lambda error: errors.append(str(error)))
                yield SimpleNamespace(page=page, prefix=prefix, requests=requests,
                                      errors=errors, outside_mount=outside_mount, expect=browser_api.expect)
            finally:
                context.close()
        finally:
            browser.close()


def test_dashboard_main_menu_preserves_mount(dashboard_nav):
    """Click Information → Dashboards → System → PBGUI Services with real pages."""
    nav = dashboard_nav
    page, prefix = nav.page, nav.prefix
    page.goto('http://nav.test' + prefix + '/api/services/main_page')
    page.locator('.nav-group-btn[data-group="information"]').click()
    page.locator('.nav-item[data-page="dashboards"]').click()
    nav.expect(page).to_have_url('http://nav.test' + prefix + '/api/dashboard/main_page')
    assert page.evaluate('PBGUI_NAV_CONFIG.apiBase') == prefix + '/api'
    assert page.evaluate('typeof window.BASE_PREFIX') == 'undefined'
    assert page.evaluate('typeof window.PBGUI_BASE_PREFIX') == 'undefined'
    assert page.evaluate('typeof window.API_BASE') == 'undefined'
    page.locator('.nav-group-btn[data-group="system"]').click()
    page.locator('.nav-item[data-page="system_services"]').click()
    destination = 'http://nav.test' + prefix + '/api/services/main_page'
    nav.expect(page).to_have_url(destination)
    nav.expect(page.locator('#sidebar')).to_be_visible()
    page.locator('#pbgui-guide-btn').click()
    nav.expect(page.locator('#pbgui-shared-help-content')).to_contain_text('Synthetic documentation.')
    assert page.url == destination
    assert prefix + '/api/help/index' in nav.requests
    assert prefix + '/api/help/content' in nav.requests
    assert prefix + '/app/vendor/marked.min.js' in nav.requests
    assert not nav.outside_mount, nav.outside_mount
    assert not nav.errors, nav.errors


def test_dashboard_guide_retains_mount_without_navigation(dashboard_nav):
    """Guide on the real Dashboard must keep its document, assets and help requests."""
    nav = dashboard_nav
    page, prefix = nav.page, nav.prefix
    destination = 'http://nav.test' + prefix + '/api/dashboard/main_page?filter=isolated'
    page.goto(destination)
    with page.expect_request('**/vendor/marked.min.js?*') as script:
        page.locator('#pbgui-guide-btn').click()
    assert urlparse(script.value.url).path == prefix + '/app/vendor/marked.min.js'
    nav.expect(page.locator('#pbgui-shared-help-content')).to_contain_text('Synthetic documentation.')
    assert page.url == destination
    nav.expect(page.locator('#sidebar-search')).to_have_value('isolated')
    assert prefix + '/api/help/index' in nav.requests
    assert prefix + '/api/help/content' in nav.requests
    assert not nav.outside_mount, nav.outside_mount
    assert not nav.errors, nav.errors


@pytest.mark.parametrize('width', [700, 1280])
def test_open_navigation_menu_tracks_mouse_and_preserves_taps(width):
    """Mouse movement switches open menus; touch, keyboard and outside clicks still work."""
    browser_api = __import__('browser_driver')
    errors = []
    with browser_api.sync_browser() as driver:
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
        browser_api.expect(groups).to_have_count(0)
        menu('system').click()
        for name in ['information','pbv7','pbv8','system']:
            menu(name).hover()
            browser_api.expect(groups).to_have_count(1)
            browser_api.expect(groups.locator('.nav-group-btn')).to_have_attribute('data-group', name)
            browser_api.expect(groups.locator('.nav-dropdown')).to_be_visible()
            groups.locator('.nav-item').first.hover()
            browser_api.expect(groups).to_have_count(1)
        # Real touch taps must not first hover-open and then click-close the new menu.
        menu('information').tap()
        browser_api.expect(groups.locator('.nav-group-btn')).to_have_attribute('data-group', 'information')
        menu('pbv7').tap()
        browser_api.expect(groups.locator('.nav-group-btn')).to_have_attribute('data-group', 'pbv7')
        menu('pbv7').tap()
        browser_api.expect(groups).to_have_count(0)
        menu('system').focus()
        page.keyboard.press('Enter')
        browser_api.expect(groups.locator('.nav-group-btn')).to_have_attribute('data-group', 'system')
        draft.click()
        browser_api.expect(groups).to_have_count(0)
        menu('pbv8').hover()
        browser_api.expect(groups).to_have_count(0)
        browser_api.expect(draft).to_have_value('Keep my edits')
        assert page.url==destination
        assert not errors, errors
        browser.close()
