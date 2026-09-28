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
