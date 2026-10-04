"""Standalone Help is a full page; its topic navigation survives browser reloads."""
from pathlib import Path
from urllib.parse import urlparse, parse_qs

import pytest

ROOT = Path(__file__).resolve().parents[2]


def test_help_sidebar_topics_language_search_and_mounted_assets():
    """Exercise actual topic rendering and search with local assets and mocked API data."""
    playwright = pytest.importorskip('playwright.sync_api')
    html = (ROOT / 'frontend/help.html').read_text()
    for marker, value in {'API_BASE': '/prefix/api', 'WS_BASE': '', 'VERSION': 'test', 'SERIAL': '1'}.items():
        html = html.replace('%%' + marker + '%%', value)
    html = html.replace('<nav id="topnav">', '<nav id="topnav" style="height:52px;min-height:52px">')
    errors, unexpected = [], []
    with playwright.sync_playwright() as driver:
        browser = driver.chromium.launch(headless=True)
        page = browser.new_page(viewport={'width': 1200, 'height': 800})
        page.on('pageerror', lambda error: errors.append(str(error)))

        def respond(route):
            """Resolve every browser request locally under a reverse-proxy mount prefix."""
            url = urlparse(route.request.url)
            path = url.path
            if path == '/prefix/app/help.html':
                route.fulfill(body=html, content_type='text/html')
            elif path == '/prefix/app/pbgui_nav.js':
                route.fulfill(body='', content_type='text/javascript')
            elif path.startswith('/prefix/app/'):
                asset = ROOT / 'frontend' / path.removeprefix('/prefix/app/')
                route.fulfill(body=asset.read_bytes(), content_type='text/css' if asset.suffix == '.css' else 'text/javascript')
            elif path == '/prefix/api/help/index':
                route.fulfill(json=[{'file':'00_overview', 'title':'Overview'}, {'file':'beta', 'title':'Beta topic'}])
            elif path == '/prefix/api/help/content':
                query = parse_qs(url.query)
                route.fulfill(json={'content': '# ' + query['file'][0] + '\n\nSample content ' + query['lang'][0]})
            else:
                unexpected.append(path)
                route.fulfill(status=404, body='')

        page.route('**/*', respond)
        page.goto('http://help.test/prefix/app/help.html?topic=beta')
        playwright.expect(page.locator('#help-content h1')).to_have_text('beta')
        assert page.locator('#help-ovl').count() == 0
        assert page.locator('#sidebar .toc-item').count() == 2
        main = page.locator('#main-content').bounding_box()
        assert main['x'] == 240 and main['width'] == 960
        for key in ('Home', 'End'):
            page.locator('#sidebar-resize').focus()
            page.keyboard.press(key)
            buttons = [page.locator('#help-lang-' + lang).bounding_box() for lang in ('en', 'de')]
            assert all(box['width'] == box['height'] == 32 for box in buttons)
            assert buttons[0]['y'] == buttons[1]['y']
            assert page.locator('#sidebar-sticky').evaluate('(e) => e.scrollWidth <= e.clientWidth')
        page.locator('#help-lang-de').click()
        playwright.expect(page.locator('#help-content')).to_contain_text('Sample content DE')
        page.locator('#help-toc-filter').fill('Beta')
        playwright.expect(page.locator('#sidebar .toc-item')).to_have_count(1)
        page.locator('#help-search').fill('Sample')
        playwright.expect(page.locator('#help-content mark')).to_have_count(1)
        page.locator('#sidebar-resize').focus()
        page.keyboard.press('End')
        page.reload()
        playwright.expect(page.locator('#help-content h1')).to_have_text('beta')
        playwright.expect(page.locator('#help-content')).to_contain_text('Sample content DE')
        playwright.expect(page.locator('#help-toc-filter')).to_have_value('Beta')
        playwright.expect(page.locator('#help-content mark')).to_have_count(1)
        assert page.locator('#sidebar').bounding_box()['width'] == 420
        page.locator('#help-search-global').check()
        playwright.expect(page.locator('.gs-item')).to_have_count(2)
        page.reload()
        playwright.expect(page.locator('.gs-item')).to_have_count(2)
        page.set_viewport_size({'width':390, 'height':800})
        main = page.locator('#main-content').bounding_box()
        assert main['height'] >= 120 and main['y'] + main['height'] <= 801
        assert not errors and not unexpected
        browser.close()


def test_information_menu_opens_new_help_page_with_real_navigation():
    """The real menu uses the new cache URL and retains a full-page sidebar after nav starts."""
    playwright = pytest.importorskip('playwright.sync_api')
    html = (ROOT / 'frontend/help.html').read_text()
    for marker, value in {'API_BASE': '/prefix/api', 'WS_BASE': '', 'VERSION': 'test', 'SERIAL': '1'}.items():
        html = html.replace('%%' + marker + '%%', value)
    errors = []
    with playwright.sync_playwright() as driver:
        browser = driver.chromium.launch(headless=True)
        page = browser.new_page(viewport={'width':1200, 'height':800})
        page.on('pageerror', lambda error: errors.append(str(error)))

        def respond(route):
            """Keep both the real menu and destination isolated from production services."""
            url = urlparse(route.request.url)
            path = url.path
            if path == '/prefix/app/start.html':
                route.fulfill(content_type='text/html', body='''<nav id="topnav"></nav>
                  <script>window.PBGUI_NAV_CONFIG={current:'welcome',authenticated:true,apiBase:'/prefix/api'};</script>
                  <script src="/prefix/app/pbgui_nav.js?v=test"></script>''')
            elif path == '/prefix/app/help.html':
                assert parse_qs(url.query).get('v') == ['1770']
                route.fulfill(body=html, content_type='text/html')
            elif path.startswith('/prefix/app/'):
                asset = ROOT / 'frontend' / path.removeprefix('/prefix/app/')
                if asset.is_file():
                    route.fulfill(body=asset.read_bytes(), content_type='text/css' if asset.suffix == '.css' else 'text/javascript')
                else:
                    route.fulfill(status=404, body='')
            elif path == '/prefix/api/help/index':
                route.fulfill(json=[{'file':'00_overview','title':'Overview'}])
            elif path == '/prefix/api/help/content':
                route.fulfill(json={'content':'# Help Overview\n\nFull page documentation.'})
            elif path.endswith('/stream'):
                route.fulfill(status=204, body='')
            else:
                route.fulfill(json={})

        page.route('**/*', respond)
        page.goto('http://help.test/prefix/app/start.html')
        page.locator('.nav-group-btn', has_text='Information').click()
        page.locator('.nav-item[data-page="help"]').click()
        playwright.expect(page.locator('#sidebar')).to_be_visible()
        playwright.expect(page.locator('#help-content h1')).to_have_text('Help Overview')
        assert page.locator('#help-ovl').count() == 0
        assert page.locator('#pbgui-shared-help-ovl.visible').count() == 0
        main = page.locator('#main-content').bounding_box()
        assert main['x'] == 240 and main['width'] == 960
        page.locator('#pbgui-guide-btn').click()
        playwright.expect(page.locator('#pbgui-shared-help-ovl')).to_have_class('visible')
        playwright.expect(page.locator('#sidebar')).to_be_visible()
        assert not errors
        browser.close()
