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
