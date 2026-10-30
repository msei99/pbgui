"""Standalone Help is a full page; its topic navigation survives browser reloads."""
from pathlib import Path
from urllib.parse import urlparse, parse_qs

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize('prefix', ['', '/mounted'])
@pytest.mark.parametrize('area', ['content', 'index'])
@pytest.mark.parametrize('failure', ['http', 'network'])
def test_vps_guide_errors_through_real_menu(prefix, area, failure):
    """Real VPS menu/Guide requests show errors without changing navigation or a draft."""
    __tracebackhide__ = True
    browser_api = __import__('browser_driver')
    html = (ROOT / 'frontend/vps_manager.html').read_text()
    for key, value in {'API_BASE': prefix + '/api/vps-manager',
                       'WS_BASE': 'ws://guide.test' + prefix,
                       'VERSION': 'test', 'SERIAL': '1', 'NAV_HASH': 'test'}.items():
        html = html.replace('%%' + key + '%%', value)
    html = html.replace('/app/', prefix + '/app/')
    state = {'fail': True}
    requested, unexpected, page_errors, logs = [], [], [], []
    with browser_api.sync_browser() as driver:
        browser = driver.chromium.launch(headless=True)
        try:
            page = browser.new_page(viewport={'width': 1400, 'height': 950})
            page.set_default_timeout(5000)
            page.on('pageerror', lambda error: page_errors.append(str(error)))
            page.on('console', lambda message: logs.append(message.text)
                    if message.type == 'error' and message.text.startswith('Error loading help ') else None)
            # Every socket stays local; a synthetic config provides one harmless editable field.
            page.route_web_socket('**/*', lambda socket: socket.on_message(lambda _message: None))

            def respond(route):
                """Serve only versioned local assets and synthetic API replies, including failures."""
                url = urlparse(route.request.url)
                path = url.path
                requested.append(route.request.url)
                if path == prefix + '/app/start.html':
                    route.fulfill(content_type='text/html', body=f'''<nav id="topnav"></nav>
                      <script>window.PBGUI_NAV_CONFIG={{current:'welcome',authenticated:true,apiBase:'{prefix}/api'}};</script>
                      <script src="{prefix}/app/pbgui_nav.js?v=test"></script>''')
                elif path == prefix + '/api/vps-manager/main_page':
                    route.fulfill(body=html, content_type='text/html')
                elif path.startswith(prefix + '/app/'):
                    asset = ROOT / 'frontend' / path.removeprefix(prefix + '/app/')
                    if asset.is_file():
                        route.fulfill(body=asset.read_bytes(), content_type='text/css' if asset.suffix == '.css' else 'text/javascript')
                    else:
                        unexpected.append(path)
                        route.fulfill(status=404, body='')
                elif path in (prefix + '/api/help/index', prefix + '/api/help/content'):
                    is_failure = state['fail'] and path.endswith('/' + area)
                    if is_failure and failure == 'network':
                        route.abort('failed')
                    elif is_failure:
                        route.fulfill(status=500, body='<b>Synthetic error body must stay hidden</b>')
                    elif path.endswith('/index'):
                        route.fulfill(json=[{'file': '32_vps_manager', 'title': 'VPS Manager'},
                                            {'file': '00_overview', 'title': 'Overview'}])
                    else:
                        query = parse_qs(url.query)
                        route.fulfill(json={'content': '# ' + query['file'][0] + '\n\nSynthetic guide ' + query['lang'][0]})
                elif path.endswith('/stream'):
                    route.fulfill(status=204, body='')
                elif path.startswith(prefix + '/api/'):
                    route.fulfill(json={})
                else:
                    unexpected.append(path)
                    route.fulfill(status=404, body='')

            page.route('**/*', respond)
            page.goto('http://guide.test' + prefix + '/app/start.html')
            page.locator('.nav-group-btn', has_text='System').click()
            page.locator('.nav-item[data-page="system_vps_manager_fastapi"]').click()
            browser_api.expect(page.locator('#sidebar')).to_be_visible()
            page.evaluate('''() => {
              store.state = {config:{vps_logging:{services:[
                {service:'FixtureService',max_mb:1,default_max_mb:1}
              ]}}};
            }''')
            page.locator('#sidebar').get_by_role('button', name='Settings', exact=True).click()
            draft = page.locator('[data-vps-logging-service="FixtureService"]')
            draft.fill('2')
            page.evaluate('window.guideDraftNode=document.querySelector("[data-vps-logging-service]")')
            destination = page.url
            assert destination == 'http://guide.test' + prefix + '/api/vps-manager/main_page#settings/vps-logging'
            page.locator('#pbgui-guide-btn').click()
            overlay = page.locator('#pbgui-shared-help-ovl')
            alert = overlay.locator('[role="alert"]')
            expected = 'Failed to load help topics.' if area == 'index' else 'Failed to load help content.'
            browser_api.expect(alert).to_have_text(expected)
            browser_api.expect(alert).to_be_visible()
            assert alert.count() == 1
            assert alert.evaluate('(e)=>getComputedStyle(e).borderTopStyle') == 'solid'
            assert len(logs) == 1
            assert page.url == destination
            browser_api.expect(draft).to_have_value('2')
            assert page.evaluate('guideDraftNode===document.querySelector("[data-vps-logging-service]")')
            assert page.locator('#pbgui-shared-help-content').inner_text() == expected
            assert page.locator('#pbgui-shared-help-content .pbgui-shared-help-loading').count() == 0
            if area == 'index':
                browser_api.expect(page.locator('#pbgui-shared-help-toc-list')).to_have_text(expected)
            # No manual refresh control: normal reopen or topic selection loads recovered data.
            state['fail'] = False
            if area == 'index':
                page.locator('#pbgui-shared-help-close').click()
                page.locator('#pbgui-guide-btn').click()
            else:
                page.locator('.pbgui-shared-help-toc-item', has_text='Overview').click()
            browser_api.expect(overlay.locator('h1')).to_have_text('32_vps_manager' if area == 'index' else '00_overview')
            browser_api.expect(alert).to_have_count(0)
            page.locator('#pbgui-shared-help-lang-de').click()
            browser_api.expect(page.locator('#pbgui-shared-help-content')).to_contain_text('Synthetic guide DE')
            assert page.url == destination
            page.locator('#pbgui-shared-help-close').click()
            browser_api.expect(draft).to_have_value('2')
            assert len(logs) == 1 and not page_errors and not unexpected
            shared_assets = [url for url in requested if '/js/shared_help_overlay.js' in url]
            assert shared_assets
            assert all(urlparse(url).path == prefix + '/app/js/shared_help_overlay.js'
                       and parse_qs(urlparse(url).query).get('v') == ['12'] for url in shared_assets)
            assert any(urlparse(url).path == prefix + '/api/help/' + area for url in requested)
        finally:
            browser.close()


def test_help_sidebar_topics_language_search_and_mounted_assets():
    """Exercise actual topic rendering and search with local assets and mocked API data."""
    __tracebackhide__ = True
    browser_api = __import__('browser_driver')
    html = (ROOT / 'frontend/help.html').read_text()
    for marker, value in {'API_BASE': '/prefix/api', 'WS_BASE': '', 'VERSION': 'test', 'SERIAL': '1'}.items():
        html = html.replace('%%' + marker + '%%', value)
    html = html.replace('<nav id="topnav">', '<nav id="topnav" style="height:52px;min-height:52px">')
    errors, unexpected = [], []
    with browser_api.sync_browser() as driver:
        browser = driver.chromium.launch(headless=True)
        try:
            page = browser.new_page(viewport={'width': 1200, 'height': 800})
            page.on('pageerror', lambda error: errors.append(str(error)))
            page.set_default_timeout(5000)
            page.route_web_socket('**/*', lambda socket: socket.on_message(lambda _message: None))

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
            browser_api.expect(page.locator('#help-content h1')).to_have_text('beta')
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
            browser_api.expect(page.locator('#help-content')).to_contain_text('Sample content DE')
            page.locator('#help-toc-filter').fill('Beta')
            browser_api.expect(page.locator('#sidebar .toc-item')).to_have_count(1)
            page.locator('#help-search').fill('Sample')
            browser_api.expect(page.locator('#help-content mark')).to_have_count(1)
            page.locator('#sidebar-resize').focus()
            page.keyboard.press('End')
            page.reload()
            browser_api.expect(page.locator('#help-content h1')).to_have_text('beta')
            browser_api.expect(page.locator('#help-content')).to_contain_text('Sample content DE')
            browser_api.expect(page.locator('#help-toc-filter')).to_have_value('Beta')
            browser_api.expect(page.locator('#help-content mark')).to_have_count(1)
            assert page.locator('#sidebar').bounding_box()['width'] == 420
            page.locator('#help-search-global').check()
            browser_api.expect(page.locator('.gs-item')).to_have_count(2)
            page.reload()
            browser_api.expect(page.locator('.gs-item')).to_have_count(2)
            page.set_viewport_size({'width':390, 'height':800})
            main = page.locator('#main-content').bounding_box()
            assert main['height'] >= 120 and main['y'] + main['height'] <= 801
            assert not errors and not unexpected
        finally:
            browser.close()


def test_information_menu_opens_new_help_page_with_real_navigation():
    """The real menu uses the new cache URL and retains a full-page sidebar after nav starts."""
    __tracebackhide__ = True
    browser_api = __import__('browser_driver')
    html = (ROOT / 'frontend/help.html').read_text()
    for marker, value in {'API_BASE': '/prefix/api', 'WS_BASE': '', 'VERSION': 'test', 'SERIAL': '1'}.items():
        html = html.replace('%%' + marker + '%%', value)
    errors = []
    with browser_api.sync_browser() as driver:
        browser = driver.chromium.launch(headless=True)
        try:
            page = browser.new_page(viewport={'width':1200, 'height':800})
            page.on('pageerror', lambda error: errors.append(str(error)))
            page.set_default_timeout(5000)
            page.route_web_socket('**/*', lambda socket: socket.on_message(lambda _message: None))

            def respond(route):
                """Keep both the real menu and destination isolated from production services."""
                url = urlparse(route.request.url)
                path = url.path
                if path == '/prefix/app/start.html':
                    route.fulfill(content_type='text/html', body='''<nav id="topnav"></nav>
                      <script>window.PBGUI_NAV_CONFIG={current:'welcome',authenticated:true,apiBase:'/prefix/api'};</script>
                      <script src="/prefix/app/pbgui_nav.js?v=test"></script>''')
                elif path == '/prefix/app/help.html':
                    assert parse_qs(url.query).get('v') == ['1779']
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
            browser_api.expect(page.locator('#sidebar')).to_be_visible()
            browser_api.expect(page.locator('#help-content h1')).to_have_text('Help Overview')
            assert page.locator('#help-ovl').count() == 0
            assert page.locator('#pbgui-shared-help-ovl.visible').count() == 0
            main = page.locator('#main-content').bounding_box()
            assert main['x'] == 240 and main['width'] == 960
            page.locator('#pbgui-guide-btn').click()
            browser_api.expect(page.locator('#pbgui-shared-help-ovl')).to_have_class('visible')
            browser_api.expect(page.locator('#sidebar')).to_be_visible()
            assert not errors
        finally:
            browser.close()
