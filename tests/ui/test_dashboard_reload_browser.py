"""Dashboard navigation survives real browser reloads without stale widget content."""
import json
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest
from playwright.sync_api import expect, sync_playwright

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize('editing', [False, True])
def test_dashboard_menu_reload_restores_selection_and_filter(editing):
    """Use real navigation and shell assets while all dashboard data stays synthetic."""
    dashboards = ['Alpha', 'HL manicpt', 'Beta', 'Gamma']
    errors, loads = [], []
    with sync_playwright() as driver:
        browser = driver.chromium.launch()
        page = browser.new_page()
        page.set_default_timeout(5000)
        page.on('pageerror', lambda error: errors.append(str(error)))

        def respond(route):
            """Return isolated snapshots; every iframe request gets fresh content."""
            url = urlparse(route.request.url)
            path = url.path
            if path == '/app/start.html':
                route.fulfill(content_type='text/html', body='<nav id="topnav"></nav><script>window.PBGUI_NAV_CONFIG={current:"welcome",authenticated:true};</script><script src="/app/pbgui_nav.js"></script>')
            elif path == '/api/dashboard/main_page':
                source = (ROOT / 'frontend/dashboard_main.html').read_text()
                values = {'API_BASE': '/api', 'VERSION': 'test', 'CURRENT': '', 'DASHBOARDS_JSON': json.dumps(dashboards), 'SERIAL': '1', 'NAV_HASH': 'test'}
                for key, value in values.items():
                    source = source.replace('%%' + key + '%%', value)
                route.fulfill(content_type='text/html', body=source)
            elif path == '/api/dashboard/editor_page':
                loads.append(parse_qs(url.query))
                route.fulfill(content_type='text/html', body='<p id="snapshot">Fresh snapshot ' + str(len(loads)) + '</p>')
            elif path == '/api/dashboards':
                route.fulfill(json={'dashboards': dashboards})
            elif path.startswith('/app/'):
                asset = ROOT / 'frontend' / path.removeprefix('/app/')
                route.fulfill(body=asset.read_bytes() if asset.is_file() else b'', content_type='text/css' if asset.suffix == '.css' else 'text/javascript')
            elif path.endswith('/stream'):
                route.fulfill(status=204, body='')
            else:
                route.fulfill(json={})

        page.route('**/*', respond)
        page.goto('http://dashboard.test/app/start.html')
        page.locator('.nav-group-btn', has_text='Information').click()
        page.locator('.nav-item[data-page="dashboards"]').click()
        page.locator('.sb-item[data-name="HL manicpt"]').click()
        expect(page.frame_locator('#content-frame').locator('#snapshot')).to_be_visible()
        page.locator('#sidebar-search').fill('HL')
        if editing:
            page.locator('#sb-edit').click()
        expect(page.locator('#sb-refresh')).to_have_count(0)
        assert parse_qs(urlparse(page.url).query)['current'] == ['HL manicpt']
        before = len(loads)
        page.reload()
        expect(page.frame_locator('#content-frame').locator('#snapshot')).to_contain_text('Fresh snapshot ' + str(before + 1))
        expect(page.locator('#sidebar-search')).to_have_value('HL')
        expect(page.locator('.sb-item.active')).to_have_attribute('data-name', 'HL manicpt')
        assert loads[-1]['name'] == ['HL manicpt']
        assert loads[-1].get('standalone' if editing else 'view_only') == ['1']
        dashboards.remove('HL manicpt')
        page.reload()
        expect(page.locator('#content-loading')).to_contain_text('Dashboard is no longer available')
        assert 'current' not in parse_qs(urlparse(page.url).query)
        assert not errors
        browser.close()
