"""Offline browser coverage for real Cluster navigation and credential adoption status."""
from pathlib import Path
import time
from urllib.parse import urlparse

from playwright.sync_api import expect, sync_playwright

ROOT = Path(__file__).resolve().parents[2]


def test_cluster_preview_updates_adoption_without_resetting_navigation():
    """Exercise the real menu, preview status updates, close lifecycle and sidebar state."""
    html = (ROOT/'frontend/cluster.html').read_text()
    for marker, value in {'API_BASE': '/prefix/api/cluster', 'BASE_PREFIX': '/prefix', 'WS_BASE': '', 'VERSION': 'test', 'SERIAL': '1', 'NAV_HASH': 'test'}.items():
        html = html.replace('%%'+marker+'%%', value)
    html = html.replace('/app/', '/prefix/app/')
    node = {'node_id': 'node-b', 'pbname': 'node-b', 'hostname': 'node-b', 'role': 'vps', 'enabled': True, 'state_replica': True}
    phase = {'status': 'pending'}
    reads, errors = [], []
    with sync_playwright() as driver:
        browser = driver.chromium.launch(headless=True)
        page = browser.new_page(viewport={'width': 1200, 'height': 850})
        page.clock.install()
        page.on('pageerror', lambda error: errors.append(str(error)))

        def respond(route):
            """Serve every asset/API locally; no real host or exchange can be contacted."""
            path = urlparse(route.request.url).path
            if path == '/prefix/app/start.html':
                route.fulfill(content_type='text/html', body='''<nav id="topnav"></nav><script>
                  window.PBGUI_NAV_CONFIG={current:'welcome',authenticated:true,apiBase:'/prefix/api'};
                  </script><script src="/prefix/app/pbgui_nav.js?v=test"></script>''')
            elif path == '/prefix/api/cluster/main_page':
                route.fulfill(content_type='text/html', body=html)
            elif path.startswith('/prefix/app/'):
                asset = ROOT/'frontend'/path.removeprefix('/prefix/app/')
                if asset.is_file():
                    route.fulfill(body=asset.read_bytes(), content_type='text/css' if asset.suffix == '.css' else 'text/javascript')
                else:
                    route.fulfill(status=404, body='')
            elif path == '/prefix/api/cluster/status':
                route.fulfill(json={'identity': {'node_id': 'node-a', 'pbname': 'master'}, 'sync_status': {
                    'peers': [{'node_id': 'node-b', 'ok': True, 'status': 'synced', 'last_seen': int(time.time())}]}})
            elif path == '/prefix/api/cluster/nodes':
                route.fulfill(json={'nodes': [node]})
            elif path == '/prefix/api/cluster/remote-status':
                route.fulfill(json={'probes': [{**node, 'ok': True, 'status': 'ok'}]})
            elif path == '/prefix/api/cluster/remote-preview/node-b':
                route.fulfill(json={'hostname': 'node-b', 'state_vector': {}, 'desired_state': {}, 'api_key_materialization': {'status': 'current'}})
            elif path == '/prefix/api/cluster/credential-runtime/node-b':
                reads.append(path)
                route.fulfill(json={'runtimes': {'pb7': {'status': 'key_arrived', 'items': [
                    {'instance': 'bot-a', 'user': 'alice', 'status': phase['status']}]}}})
            elif path == '/prefix/api/help/index':
                route.fulfill(json=[{'file': '39_cluster_sync', 'title': 'Cluster Sync'}])
            elif path == '/prefix/api/help/content':
                route.fulfill(json={'content': '# Cluster Sync\n\nCredential adoption.'})
            elif path.endswith('/stream'):
                route.fulfill(status=204, body='')
            else:
                route.fulfill(json={})

        page.route('**/*', respond)
        page.goto('http://cluster.test/prefix/app/start.html')
        page.locator('.nav-group-btn', has_text='System').click()
        page.locator('.nav-item[data-page="system_cluster"]').click()
        expect(page.locator('#sidebar')).to_be_visible()
        page.locator('[data-cluster-section="nodes"]').click()
        page.locator('#probe-btn').click()
        expect(page.locator('.preview-node-btn')).to_be_visible()
        for key in ('Home', 'End'):
            page.locator('#sidebar-resize').focus()
            page.keyboard.press(key)
            assert page.locator('#sidebar').evaluate('(e)=>e.scrollWidth<=e.clientWidth')
        page.locator('.preview-node-btn').click()
        expect(page.locator('#credential-runtime-status')).to_contain_text('Restart pending')
        phase['status'] = 'applied'
        page.clock.fast_forward(5000)
        expect(page.locator('#credential-runtime-status')).to_contain_text('New credential version adopted')
        page.locator('#preview-close-btn').click()
        before = len(reads)
        page.clock.fast_forward(10000)
        assert len(reads) == before
        page.reload()
        expect(page.locator('[data-cluster-section-panel="nodes"]')).to_have_class('cluster-section is-active')
        assert page.locator('#sidebar').bounding_box()['width'] == 420
        page.locator('#pbgui-guide-btn').click()
        expect(page.locator('#pbgui-shared-help-ovl')).to_have_class('visible')
        assert page.url.endswith('/prefix/api/cluster/main_page#nodes') or 'main_page' in page.url
        assert not errors
        browser.close()
