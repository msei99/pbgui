"""Render the actual Services page using isolated API responses and local assets."""

from pathlib import Path
from urllib.parse import urlparse

import pytest


ROOT = Path(__file__).resolve().parents[2]


def test_overview_uses_fast_worker_summary_and_displays_restart() -> None:
    """Overview loads current cards without requesting expensive worker queue details."""
    playwright = pytest.importorskip('playwright.sync_api')
    html = (ROOT / 'frontend/services_monitor.html').read_text()
    for marker, value in {'API_BASE': '/api/services', 'BASE_PREFIX': '', 'VERSION': 'test',
                          'SERIAL': '1', 'NAV_HASH': 'test'}.items():
        html = html.replace('%%' + marker + '%%', value)
    services = ('pbcluster', 'pbrun', 'pbdata', 'pbcoindata', 'monitor-agent', 'vps-monitor', 'api-server')
    requests = []
    errors = []
    with playwright.sync_playwright() as driver:
        browser = driver.chromium.launch(headless=True)
        page = browser.new_page(viewport={'width': 1440, 'height': 900})
        page.on('pageerror', lambda error: errors.append(str(error)))

        def respond(route):
            """Serve deterministic page resources; never call the production API."""
            path = urlparse(route.request.url).path
            requests.append(path)
            if path == '/api/services/main_page':
                route.fulfill(body=html, content_type='text/html')
            elif path.startswith('/app/'):
                asset = ROOT / 'frontend' / path.removeprefix('/app/')
                if asset.is_file():
                    mime = 'text/javascript' if asset.suffix == '.js' else 'text/css' if asset.suffix == '.css' else 'text/html'
                    route.fulfill(body=asset.read_bytes(), content_type=mime)
                else:
                    route.fulfill(status=404, body='')
            elif path == '/api/services/status':
                route.fulfill(json={name: {'running': True, 'enabled': True, 'can_enable': True} for name in services})
            elif path == '/api/services/workers/summary':
                route.fulfill(json={'counts': {'total': 7, 'running': 7}})
            elif path == '/api/server-status':
                route.fulfill(json={'needs_restart': True, 'restart_services': [
                    {'service': 'PBApiServer', 'label': 'PBGui API Server'}]})
            elif path == '/api/server-status/stream':
                route.fulfill(body=': keepalive\n\n', content_type='text/event-stream')
            else:
                route.fulfill(json={})

        page.route('**/*', respond)
        page.goto('http://pbgui.test/api/services/main_page', wait_until='domcontentloaded')
        playwright.expect(page.locator('#overview-grid .card-status-row').first).to_contain_text('Running', timeout=1000)
        playwright.expect(page.locator('#overview-grid')).to_contain_text('7 / 7 running', timeout=1000)
        playwright.expect(page.locator('#pbgui-restart-btn')).to_be_visible(timeout=1000)
        assert '/api/services/workers/summary' in requests
        assert '/api/services/workers/status' not in requests
        assert not errors
        browser.close()


@pytest.mark.parametrize('initial_failure', [False, True], ids=['after-success', 'first-poll'])
def test_service_poll_failure_recovers_without_losing_context(initial_failure: bool) -> None:
    """All service surfaces recover automatically without disturbing edits or other status."""
    playwright = pytest.importorskip('playwright.sync_api')
    html = (ROOT / 'frontend/services_monitor.html').read_text()
    for marker, value in {'API_BASE': '/api/services', 'BASE_PREFIX': '', 'VERSION': 'test',
                          'SERIAL': '1', 'NAV_HASH': 'test'}.items():
        html = html.replace('%%' + marker + '%%', value)
    services = ('pbcluster', 'pbrun', 'pbdata', 'pbcoindata', 'monitor-agent', 'vps-monitor', 'api-server')
    healthy = {name: {'running': name == 'api-server', 'can_enable': True, 'enabled': True}
               for name in services}
    pending = []
    requests = []
    errors = []
    with playwright.sync_playwright() as driver:
        browser = driver.chromium.launch(headless=True)
        page = browser.new_page(viewport={'width': 1440, 'height': 900})
        # Control the existing five-second timers; all network traffic stays intercepted.
        page.clock.install()
        page.on('pageerror', lambda error: errors.append(str(error)))

        def respond(route):
            """Serve only local assets and synthetic API replies, holding service polls."""
            path = urlparse(route.request.url).path
            requests.append((route.request.method, path))
            if path == '/api/services/main_page':
                route.fulfill(body=html, content_type='text/html')
            elif path.startswith('/app/'):
                asset = ROOT / 'frontend' / path.removeprefix('/app/')
                if asset.is_file():
                    mime = 'text/javascript' if asset.suffix == '.js' else 'text/css' if asset.suffix == '.css' else 'text/html'
                    route.fulfill(body=asset.read_bytes(), content_type=mime)
                else:
                    route.fulfill(status=404, body='')
            elif path == '/api/services/status':
                pending.append(route)
            elif path == '/api/services/workers/summary':
                route.fulfill(json={'counts': {'total': 7, 'running': 6}})
            elif path == '/api/services/migration/status':
                route.fulfill(json={'migration_needed': False, 'warnings': []})
            elif path == '/api/services/settings/api-server':
                route.fulfill(json={'host': '127.0.0.1', 'port': 8000})
            elif path == '/api/server-status/stream':
                route.fulfill(body=': keepalive\n\n', content_type='text/event-stream')
            else:
                route.fulfill(json={})

        def assert_unknown(label):
            """Check hidden detail controls too, rather than just the visible overview."""
            for name in services:
                playwright.expect(page.locator('#sl-' + name)).to_have_text(label)
                playwright.expect(page.locator('#dot-' + name)).to_have_class('sb-dot ')
                playwright.expect(page.locator('#cs-' + name)).to_have_class('status-dot ')
                playwright.expect(page.locator('#ctrl-btns-' + name + ' button')).to_have_count(0)
                card = page.locator('#overview-grid .svc-card').filter(
                    has=page.locator('.card-name', has_text={'api-server': 'PBAPIServer',
                        'pbcluster': 'PBCluster', 'pbrun': 'PBRun', 'pbdata': 'PBData',
                        'pbcoindata': 'PBCoinData', 'monitor-agent': 'PBMonitorAgent',
                        'vps-monitor': 'VPSMonitor'}[name]))
                playwright.expect(card.locator('.card-status-row')).to_have_text(label)
                playwright.expect(card.locator('.card-dot')).to_have_class('card-dot ')
                playwright.expect(card.locator('button')).to_have_count(0)

        def next_poll():
            """Advance the real scheduled poll without calling private page functions."""
            page.clock.run_for(5100)
            page.wait_for_function("true")  # Pump intercepted network callbacks.
            assert len(pending) == 1
            return pending.pop(0)

        def independent_status():
            """Capture worker and migration displays so service failures cannot mask changes."""
            return page.locator('#dot-workers, #cs-workers, #sl-workers, #dot-migration, #cs-migration, #sl-migration').evaluate_all(
                "nodes => nodes.map(node => [node.id, node.className, node.textContent])")

        page.route('**/*', respond)
        try:
            page.goto('http://pbgui.test/api/services/main_page#api-server/settings', wait_until='domcontentloaded')
            assert_unknown('Loading status…')
            playwright.expect(page.locator('#sl-workers')).to_have_text('6 / 7 running')
            playwright.expect(page.locator('#sl-migration')).to_have_text('Ready')
            playwright.expect(page.locator('#apiserver-host')).to_have_value('127.0.0.1')
            page.locator('#apiserver-host').fill('draft.local')
            baseline = independent_status()
            assert len(pending) == 1
            poll = pending.pop(0)
            if not initial_failure:
                poll.fulfill(json=healthy)
                playwright.expect(page.locator('#sl-api-server')).to_have_text('Running · Enabled')
                poll = next_poll()
            poll.fulfill(status=503, json={'detail': 'temporarily unavailable'})
            assert_unknown('Status unavailable')
            assert independent_status() == baseline
            playwright.expect(page.locator('#apiserver-host')).to_have_value('draft.local')
            assert page.url.endswith('#api-server/settings')
            playwright.expect(page.locator('#api-server-tab-settings')).to_have_class('tab-pane active')
            next_poll().fulfill(json=healthy)
            playwright.expect(page.locator('#sl-api-server')).to_have_text('Running · Enabled')
            playwright.expect(page.locator('#ctrl-btns-api-server .restart')).to_have_text('↻ Restart')
            for name in services:
                expected = 'running' if name == 'api-server' else 'stopped'
                playwright.expect(page.locator('#dot-' + name)).to_have_class('sb-dot ' + expected)
                playwright.expect(page.locator('#cs-' + name)).to_have_class('status-dot ' + expected)
                assert page.locator('#ctrl-btns-' + name + ' button').count() > 0
            api_card = page.locator('#overview-grid .svc-card').filter(has=page.get_by_text('PBAPIServer', exact=True))
            playwright.expect(api_card.locator('.restart')).to_contain_text('Restart')
            assert independent_status() == baseline
            playwright.expect(page.locator('#apiserver-host')).to_have_value('draft.local')
            assert page.url.endswith('#api-server/settings')
            # Reload restores the same panel/tab and obtains current service data.
            page.reload(wait_until='domcontentloaded')
            assert len(pending) == 1
            pending.pop(0).fulfill(json=healthy)
            playwright.expect(page.locator('#sl-api-server')).to_have_text('Running · Enabled')
            playwright.expect(page.locator('#api-server-tab-settings')).to_have_class('tab-pane active')
            assert page.url.endswith('#api-server/settings')
            assert not [(method, path) for method, path in requests
                        if method == 'POST' and any(path.startswith('/api/services/' + name + '/')
                                                    for name in services)]
            assert not errors
        finally:
            browser.close()


@pytest.mark.parametrize(('prefix', 'width'), [('', 1280), ('/mounted', 740)])
def test_alert_threshold_navigation_menu_reload_and_edits(prefix, width):
    """Real menu and alert links retain prefix, settings targets, edits and Ack controls."""
    playwright = pytest.importorskip('playwright.sync_api')
    html = (ROOT / 'frontend/services_monitor.html').read_text()
    for marker, value in {'API_BASE': prefix + '/api/services', 'WS_BASE': 'ws://pbgui.test' + prefix,
                          'BASE_PREFIX': prefix, 'VERSION': 'test', 'SERIAL': '1', 'NAV_HASH': 'test'}.items():
        html = html.replace('%%' + marker + '%%', value)
    # Production render_page_urls also prefixes root-relative local assets.
    if prefix:
        html = html.replace('src="/app/', 'src="' + prefix + '/app/').replace('href="/app/', 'href="' + prefix + '/app/')
    entry = (ROOT / 'frontend/help.html').read_text()
    items = [dict(id='instance:test:bot', kind='instance', host='test', name='bot',
                  summary='Instance thresholds exceeded for bot on test: memory, errors',
                  details='Triggered: Memory 600MB > 500MB; Errors today 11 > 10',
                  triggered_thresholds=['memory', 'errors'], acknowledged=False),
             dict(id='system:test:system', kind='system', host='test', name='system',
                  summary='System threshold exceeded on test: memory', details='memory free 64MB <= 128MB',
                  triggered_thresholds=['memory'], acknowledged=False)]
    pending, requests, errors = [], [], []
    settings = {'host': '127.0.0.1', 'port': 8000,
                'monitor_config': {'mem_error_v7': 500, 'error_error_v7': 10, 'mem_error_server': 128}}

    def alert_payload():
        """Return only isolated active alarms and one historical legacy record."""
        return {'items': items, 'history': [dict(kind='instance', summary='Legacy alert', details='Old reading')],
                'summary': {'total_active': 2, 'new_count': sum(not item['acknowledged'] for item in items),
                            'ack_count': sum(item['acknowledged'] for item in items)}}

    with playwright.sync_playwright() as driver:
        browser = driver.chromium.launch(headless=True)
        page = browser.new_page(viewport={'width': width, 'height': 1000})
        page.set_default_timeout(4000)
        page.on('pageerror', lambda error: errors.append(str(error)))

        def respond(route):
            """Intercept all requests; no live host, credentials or service actions are used."""
            raw_path = urlparse(route.request.url).path
            requests.append((route.request.method, raw_path))
            path = raw_path.removeprefix(prefix) if prefix else raw_path
            if path == '/app/help.html':
                route.fulfill(body=entry, content_type='text/html')
            elif path == '/api/services/main_page':
                route.fulfill(body=html, content_type='text/html')
            elif path.startswith('/app/'):
                asset = ROOT / 'frontend' / path.removeprefix('/app/')
                if asset.is_file():
                    mime = 'text/javascript' if asset.suffix == '.js' else 'text/css' if asset.suffix == '.css' else 'text/html'
                    route.fulfill(body=asset.read_bytes(), content_type=mime)
                else:
                    route.fulfill(status=404, body='')
            elif path == '/api/services/settings/api-server':
                pending.append(route)
            elif path.startswith('/api/vps/alerts'):
                if path.endswith('/ack'):
                    selected = route.request.post_data_json['id']
                    next(item for item in items if item['id'] == selected)['acknowledged'] = True
                elif path.endswith('/ack-all'):
                    for item in items:
                        item['acknowledged'] = True
                route.fulfill(json=alert_payload())
            elif path == '/api/docs/index':
                route.fulfill(json=[{'file': '28_pbapiserver_service.md', 'title': 'PBAPIServer'}])
            elif path == '/api/docs/content':
                route.fulfill(json={'content': '# PBAPIServer\nIsolated guide.'})
            elif path == '/api/server-status/stream':
                route.fulfill(body=': keepalive\n\n', content_type='text/event-stream')
            else:
                route.fulfill(json={})

        def open_alerts():
            """Open the actual shared Alerts dialog."""
            page.locator('#pbgui-alert-btn').click()
            playwright.expect(page.locator('#pbgui-alert-ovl')).to_be_visible()

        try:
            page.route('**/*', respond)
            page.goto('http://pbgui.test' + prefix + '/app/help.html', wait_until='domcontentloaded')
            page.locator('.nav-group-btn[data-group="system"]').click()
            page.locator('.nav-item[data-page="system_services"]').click()
            playwright.expect(page).to_have_url('http://pbgui.test' + prefix + '/api/services/main_page')
            assert requests.count(('GET', prefix + '/api/services/main_page')) == 1
            open_alerts()
            playwright.expect(page.locator('#pbgui-alert-list')).to_contain_text('600MB > 500MB')
            page.locator('[data-alert-ack]').first.click()
            playwright.expect(page.locator('#pbgui-alert-summary')).to_contain_text('1 acknowledged')
            page.locator('#pbgui-alert-ack-all').click()
            playwright.expect(page.locator('#pbgui-alert-summary')).to_contain_text('2 acknowledged')
            link = page.locator('#pbgui-alert-list a').filter(has_text='Adjust Memory threshold').first
            assert link.get_attribute('href') == 'http://pbgui.test' + prefix + '/api/services/main_page#api-server/settings/mem_error_v7'
            link.click()
            playwright.expect(page.locator('#api-server-tab-settings')).to_have_class('tab-pane active')
            playwright.expect(page.locator('#pbgui-alert-ovl')).not_to_be_visible()
            assert len(pending) == 1
            assert requests.count(('GET', prefix + '/api/services/main_page')) == 1
            pending.pop().fulfill(json=settings)
            target = page.locator('#mc-mem_error_v7')
            playwright.expect(target).to_be_focused()
            playwright.expect(target).to_have_value('500')
            # Same-page link to another target keeps the unsaved draft.
            target.fill('555')
            open_alerts()
            page.get_by_role('link', name='Adjust Errors threshold', exact=True).click()
            playwright.expect(page.locator('#mc-error_error_v7')).to_be_focused()
            playwright.expect(target).to_have_value('555')
            open_alerts()
            page.get_by_role('link', name='Adjust Errors threshold', exact=True).click()
            playwright.expect(page.locator('#mc-error_error_v7')).to_be_focused()
            assert not pending
            # Independent Guide overlay leaves the field destination unchanged.
            destination = page.url
            page.locator('#pbgui-guide-btn').click()
            playwright.expect(page.locator('#pbgui-shared-help-ovl')).to_be_visible()
            assert page.url == destination
            page.locator('#pbgui-shared-help-close').click()
            # Real shared resize handle: minimum/default/maximum widths and no toolbar overflow.
            if width > 760:
                for sidebar_width in (160, 240, 420):
                    page.locator('#sidebar-resize').focus()
                    page.keyboard.press('Home' if sidebar_width == 160 else 'End' if sidebar_width == 420 else 'ArrowRight')
                    if sidebar_width == 240:
                        for _ in range(7):
                            page.keyboard.press('ArrowRight')
                    assert page.locator('#sidebar').evaluate('(el)=>Math.round(el.getBoundingClientRect().width)') == sidebar_width
                    assert page.locator('#sidebar-inner').evaluate('(el)=>el.scrollWidth <= el.clientWidth')
            page.reload(wait_until='domcontentloaded')
            playwright.expect(page.locator('#api-server-tab-settings')).to_have_class('tab-pane active')
            assert len(pending) == 1
            pending.pop().fulfill(json=settings)
            playwright.expect(page.locator('#mc-error_error_v7')).to_be_focused()
            assert page.url == destination
            if width > 760:
                assert page.locator('#sidebar').evaluate('(el)=>Math.round(el.getBoundingClientRect().width)') == 420
            # System memory uses its own limit, including when settings are already loaded.
            open_alerts()
            page.get_by_role('link', name='Adjust Memory threshold', exact=True).nth(1).click()
            playwright.expect(page.locator('#mc-mem_error_server')).to_be_focused()
            playwright.expect(page.locator('#mc-mem_error_server')).to_have_value('128')
            page.evaluate("location.hash='api-server/settings/not-a-field'")
            playwright.expect(page.locator('.settings-target-notice')).to_have_text('This threshold setting is unavailable.')
            assert page.url.endswith('#api-server/settings')
            # Only isolated acknowledgement and standard session-validation requests may be posted.
            assert all(path in (prefix + '/api/vps/alerts/ack', prefix + '/api/vps/alerts/ack-all', prefix + '/api/token-refresh')
                       for method, path in requests if method == 'POST'), [(method, path) for method, path in requests if method == 'POST']
            # A known field missing in the loaded DOM has the same explicit safe fallback.
            page.locator('#mc-mem_error_v7').evaluate('(el)=>el.remove()')
            page.evaluate("location.hash='api-server/settings/mem_error_v7'")
            playwright.expect(page.locator('.settings-target-notice')).to_have_text('This threshold setting is unavailable.')
            playwright.expect(page).to_have_url('http://pbgui.test' + prefix + '/api/services/main_page#api-server/settings')
            # Dispatch the cached-page lifecycle through the real browser DOM and handlers.
            # Native BFCache eligibility is separate from this deterministic event regression.
            page.clock.install()
            page.reload(wait_until='domcontentloaded')
            assert len(pending) == 1
            page.locator('#apiserver-host').fill('cached-draft.local')
            destination = page.url
            page.evaluate("window.dispatchEvent(new PageTransitionEvent('pagehide', {persisted:true}))")
            page.route('**/api/services/settings/api-server', lambda route: route.fulfill(json=settings), times=1)
            with page.expect_request('**/api/services/settings/api-server'):
                page.evaluate("window.dispatchEvent(new PageTransitionEvent('pageshow', {persisted:true}))")
            playwright.expect(page.locator('#api-server-tab-settings .form-btn.save')).to_be_enabled()
            pending.pop()  # Discard the aborted pre-cache route, without completing it.
            playwright.expect(page.locator('#apiserver-host')).to_have_value('cached-draft.local')
            playwright.expect(page.locator('#apiserver-host')).to_be_focused()
            assert page.url == destination
            playwright.expect(page.locator('#api-server-tab-settings')).to_have_class('tab-pane active')
            playwright.expect(page.locator('#api-server-tab-settings .form-btn.save')).to_be_enabled()
            # A bounded failed load leaves Settings visible and Save unusable; Retry can recover.
            page.reload(wait_until='domcontentloaded')
            assert len(pending) == 1
            page.locator('#apiserver-host').fill('before-timeout.local')
            page.clock.fast_forward(15001)
            playwright.expect(page.locator('.settings-load-error')).to_contain_text('timed out')
            playwright.expect(page.locator('#api-server-tab-settings .form-btn.save')).to_be_disabled()
            page.locator('#apiserver-host').fill('after-timeout.local')
            page.get_by_role('button', name='Retry', exact=True).click()
            playwright.expect(page.locator('#api-server-tab-settings .form-btn.save')).to_be_disabled()
            assert len(pending) == 2
            pending.pop().fulfill(json=settings)
            playwright.expect(page.locator('#api-server-tab-settings .form-btn.save')).to_be_enabled()
            playwright.expect(page.locator('.settings-load-error')).to_have_count(0)
            playwright.expect(page.locator('#apiserver-host')).to_have_value('after-timeout.local')
            assert not errors
        except Exception as error:
            pytest.fail(str(error), pytrace=False)
        finally:
            browser.close()
