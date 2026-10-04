"""Real optimizer menu entry, visibility and state preservation with isolated requests."""

import json
from pathlib import Path
from urllib.parse import urlparse

import pytest
from fastapi import Request

from api.page_templates import render_page_urls


ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize('version,width', [('v7', 1280), ('v8', 700)])
def test_optimizer_menu_push_dedup_visibility_and_guide(version, width):
    """Use real menus/templates/assets without contacting PBGui or launching jobs."""
    playwright = pytest.importorskip('playwright.sync_api')
    prefix = '/mounted'
    route_path = prefix + '/api/optimize-' + version + '/main_page'
    request = Request({'type': 'http', 'scheme': 'http', 'server': ('optimizer.test', 80),
                       'path': route_path, 'root_path': prefix, 'headers': []})
    html = render_page_urls(request, (ROOT / 'frontend/v7_optimize.html').read_text(), '/api/optimize-' + version)
    values = {'LIMITS_META': '{}', 'VERSION': 'test', 'SERIAL': '1', 'NAV_HASH': 'test',
              'OPTIMIZE_VERSION': version, 'BACKTEST_VERSION': version,
              'OPTIMIZE_NAV_TITLE': 'Optimize', 'OPTIMIZE_NAV_CURRENT': version + '_optimize'}
    for name, value in values.items():
        html = html.replace('%%' + name + '%%', value)
    queue = [{'filename': 'job', 'name': 'Test job', 'status': 'running', 'exchange': 'binance', 'created': '2026-01-01'}]
    requests, errors = [], []

    with playwright.sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page(viewport={'width': width, 'height': 900})
        page.set_default_timeout(5000)
        page.on('pageerror', lambda error: errors.append(str(error)))
        page.add_init_script('''
          window.testVisibility='visible';
          Object.defineProperty(document,'visibilityState',{get:()=>window.testVisibility});
          const originalInterval=window.setInterval;
          window.setInterval=function(fn,ms){
            if(ms===8000)window.testOptimizerResultsTick=fn;
            if(ms===10000)window.testAlertsTick=fn;
            return originalInterval(fn,ms);
          };
          window.WebSocket=class {
            constructor(url){this.url=url;window.testSocket=this;setTimeout(()=>this.onopen&&this.onopen(),0);}
            close(){} send(){}
          };
        ''')

        def respond(route):
            """Serve repository assets and mock every application/network request."""
            path = urlparse(route.request.url).path.removeprefix(prefix)
            requests.append(path)
            if path.startswith('/app/'):
                asset = ROOT / 'frontend' / path.removeprefix('/app/')
                route.fulfill(body=asset.read_bytes(), content_type={'.js': 'text/javascript', '.css': 'text/css'}.get(asset.suffix, 'application/octet-stream'))
            elif path == '/api/optimize-' + version + '/main_page':
                route.fulfill(body=html, content_type='text/html')
            elif path == '/start':
                route.fulfill(body='<nav id="topnav"></nav><script>window.PBGUI_NAV_CONFIG={authenticated:true,apiBase:"/mounted/api/balance-calc",current:"info_balance_calc"};</script><script src="/mounted/app/pbgui_nav.js?v=test"></script>', content_type='text/html')
            elif path.endswith('/stream'):
                route.fulfill(status=204, body='')
            elif path.endswith('/queue'):
                route.fulfill(json={'items': queue})
            elif path.endswith('/results'):
                route.fulfill(json={'results': []})
            elif path.endswith('/configs'):
                route.fulfill(json={'configs': []})
            elif path.endswith('/settings'):
                route.fulfill(json={'autostart': False, 'cpu': 1})
            elif path == '/api/help/index':
                route.fulfill(json=[{'file': '36_pbv7_optimize.md' if version == 'v7' else '43_pbv8_optimize.md', 'title': 'Optimize'}])
            elif path == '/api/help/content':
                topic = '36_pbv7_optimize.md' if version == 'v7' else '43_pbv8_optimize.md'
                route.fulfill(json={'content': (ROOT / 'docs/help' / topic).read_text()})
            else:
                route.fulfill(json={})

        page.route('**/*', respond)
        page.goto('http://optimizer.test' + prefix + '/start')
        page.locator('.nav-group-btn[data-group="pb' + version + '"]').click()
        page.locator('.nav-item[data-page="' + version + '_optimize"]').click()
        assert urlparse(page.url).path == route_path, (page.url, requests[-10:], errors)
        try:
            page.wait_for_function('typeof state!=="undefined" && !state.initialLoadPending && !!window.testSocket')
        except Exception:
            pytest.fail(f'Optimizer did not initialize: {page.url}, errors={errors}, requests={requests[-12:]}')
        assert page.locator('#page-body #sidebar').count() == 1
        assert page.locator('#page-body #main-content').count() == 1
        if width > 760:
            handle = page.locator('#sidebar-resize')
            for key, expected in [('Home', 160), ('End', 420)]:
                handle.focus()
                handle.press(key)
                assert page.locator('#sidebar').bounding_box()['width'] == expected
                assert page.evaluate('''() => Array.from(document.querySelectorAll('#sidebar-toolbar button')).every(
                  node=>node.scrollWidth<=node.clientWidth)''')
        page.locator('[data-panel="queue"]').click()
        page.evaluate('''() => {
          state.selectedQueue.add('job'); state.settings.cpu=7;
          state.settingsModalCpuDirty=true;
          window.testQueueRenders=0;
          const original=renderQueueMaybeDeferred;
          renderQueueMaybeDeferred=function(){window.testQueueRenders++;return original();};
        }''')
        message = {'type': 'queue_update', 'items': queue, 'settings': {'cpu': 2}}
        page.evaluate('(msg)=>{testSocket.onmessage({data:JSON.stringify(msg)});testSocket.onmessage({data:JSON.stringify(msg)});}', message)
        assert page.evaluate('testQueueRenders') == 1
        assert page.evaluate("state.selectedQueue.has('job')")
        page.evaluate("testVisibility='hidden';document.dispatchEvent(new Event('visibilitychange'));")
        alert_count = requests.count('/api/vps/alerts')
        page.evaluate('testAlertsTick()')
        page.evaluate('(msg)=>testSocket.onmessage({data:JSON.stringify(msg)})',
                      {**message, 'items': [{**queue[0], 'status': 'optimizing'}]})
        assert page.evaluate('testQueueRenders') == 1
        assert requests.count('/api/vps/alerts') == alert_count
        page.evaluate("testVisibility='visible';document.dispatchEvent(new Event('visibilitychange'));")
        page.wait_for_function('state.pendingQueueUpdate===null && testQueueRenders===2')
        page.wait_for_function('!state.returnRefreshInFlight')
        assert page.evaluate("state.selectedQueue.has('job')")
        assert page.url.endswith(route_path + '#queue')
        destination = page.url
        page.locator('#pbgui-guide-btn').click()
        playwright.expect(page.locator('#pbgui-shared-help-ovl')).to_have_class('visible')
        assert page.url == destination
        assert not errors, errors
        browser.close()
