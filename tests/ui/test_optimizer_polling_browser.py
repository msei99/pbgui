"""Real optimizer menu entry, visibility and state preservation with isolated requests."""

import json
from pathlib import Path
from urllib.parse import urlparse

import pytest
from fastapi import Request

from api.page_templates import render_page_urls


ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize('version,width', [('v7', 1280), ('v8', 700), ('v8', 1280)])
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
    configs = [{'name': f'config-{i:03d}', 'exchange': ['binance'], 'strategy': 'ema_anchor',
                'modified': '2026-01-01', 'backtest_count': None if version == 'v8' else 0,
                'backtest_count_status': 'pending' if version == 'v8' else 'known'} for i in range(190)]
    count_value = {'config-000': 0}
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
            if(ms===30000)window.testConfigTick=fn;
            if(ms===10000)window.testAlertsTick=fn;
            return originalInterval(fn,ms);
          };
          window.testDelayCounts=true;window.testReleaseCounts=[];
          const originalFetch=window.fetch;
          window.fetch=function(url,opts){
            if(String(url).endsWith('/configs/backtest-counts') && window.testDelayCounts)
              return new Promise(resolve=>testReleaseCounts.push(()=>resolve(originalFetch(url,opts))));
            return originalFetch(url,opts);
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
            elif path.endswith('/configs/backtest-counts'):
                names = route.request.post_data_json['names']
                assert 1 <= len(names) <= 16
                route.fulfill(json={'configs': [{'name': name,
                    'backtest_count': None if name == 'config-189' else count_value.get(name, 12),
                    'backtest_count_status': 'unknown' if name == 'config-189' else 'known'} for name in names]})
            elif path.endswith('/configs'):
                route.fulfill(json={'configs': configs})
            elif '/configs/config-' in path:
                route.fulfill(json={'config': {'backtest': {'exchanges': ['binance']}, 'live': {'strategy_kind': 'ema_anchor'},
                                               'bot': {'long': {}, 'short': {}}, 'optimize': {}, 'pbgui': {}}, 'param_status': {}})
            elif path.endswith('/settings'):
                route.fulfill(json={'autostart': False, 'cpu': 1, 'cpu_max': 16,
                                    'host_cpu_count': 16, 'cpu_override': False,
                                    'use_pbgui_market_data': False})
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
        page.locator('[data-panel="configs"]').click()
        page.wait_for_function('state.configs.length===190 && !state.configRequest')
        if version == 'v8':
            assert page.locator('#configs-tbody .config-backtest-count').first.text_content() == '…'
            page.locator('#config-search').fill('config-00')
            page.locator('#configs-tbody tr[data-name="config-000"]').click()
            page.locator('#config-search').focus()
            page.evaluate("window.testRow=document.querySelector('#configs-tbody tr[data-name=\"config-000\"]');")
            start = page.locator('tr[data-name="config-001"] td').first.bounding_box()
            end = page.locator('tr[data-name="config-004"] td').first.bounding_box()
            page.mouse.move(start['x'] + 10, start['y'] + 10)
            page.mouse.down()
            page.mouse.move(end['x'] + 10, end['y'] + 10, steps=5)
            page.evaluate('testDelayCounts=false;testReleaseCounts.splice(0).forEach(fn=>fn());')
            page.wait_for_function('state.configs.every(c=>c.backtest_count_status!=="pending")')
            assert page.locator('tr[data-name="config-000"] .config-backtest-count').text_content() == '…'
            page.mouse.up()
            assert page.evaluate('[1,2,3,4].every(i=>state.selectedConfigs.has("config-00"+i))')
            assert page.locator('tr[data-name="config-000"] .config-backtest-count').text_content() == '0'
            assert page.evaluate("testRow===document.querySelector('#configs-tbody tr[data-name=\"config-000\"]')")
            assert page.locator('#config-search').evaluate('(node)=>node===document.activeElement')
            assert page.evaluate('state.selectedConfigs.has("config-000")')
            page.locator('#config-search').fill('')
            page.evaluate("el('configs-list-wrap').scrollTop=400;window.testScroll=el('configs-list-wrap').scrollTop;")
            page.evaluate('renderConfigs()')
            assert page.evaluate("el('configs-list-wrap').scrollTop===testScroll")
            page.locator('#configs-thead [data-sort-key="backtest_count"]').click()
            assert page.locator('#configs-tbody tr').last.get_attribute('data-name') == 'config-189'
            page.locator('#configs-thead [data-sort-key="backtest_count"]').click()
            assert page.locator('#configs-tbody tr').last.get_attribute('data-name') == 'config-189'
            page.locator('#config-search').fill('config-00')
            page.reload()
            page.wait_for_function('!state.initialLoadPending && state.configs.length===190')
            assert page.locator('#config-search').input_value() == 'config-00'
            assert page.evaluate('state.selectedConfigs.has("config-000")')
            assert page.evaluate('state.configSortKey') == 'backtest_count'
            assert page.evaluate('state.configSortDir') == 'asc'
            # A changed server snapshot is observed by the real automatic timer.
            count_value['config-000'] = 3
            configs[0]['strategy'] = 'trailing_martingale'
            page.evaluate('testDelayCounts=false;testReleaseCounts.splice(0).forEach(fn=>fn());')
            page.wait_for_function('!state.configCountRequest')
            page.locator('tr[data-name="config-000"] .cfg-edit').click()
            page.locator('#opted-name').fill('unsaved-test-name')
            page.evaluate('testConfigTick()')
            page.wait_for_function('!state.configRequest && !state.configCountRequest')
            assert page.locator('#opted-name').input_value() == 'unsaved-test-name'
            assert page.evaluate('state.configs.find(c=>c.name==="config-000").backtest_count') == 3
            assert page.evaluate('state.configs.find(c=>c.name==="config-000").strategy') == 'trailing_martingale'
            page.evaluate('closeEditor()')
        else:
            assert '/api/optimize-v7/configs/backtest-counts' not in requests
        page.locator('[data-panel="queue"]').click()
        page.evaluate('''() => {
          state.selectedQueue.add('job');
          el('settings-cpu-value').value='7';markQueueSettingsEdited('cpu');
          window.testQueueRenders=0;
          const original=renderQueueMaybeDeferred;
          renderQueueMaybeDeferred=function(){window.testQueueRenders++;return original();};
        }''')
        message = {'type': 'queue_update', 'items': queue, 'settings': {'cpu': 2}}
        page.evaluate('(msg)=>{testSocket.onmessage({data:JSON.stringify(msg)});testSocket.onmessage({data:JSON.stringify(msg)});}', message)
        assert page.evaluate('testQueueRenders') == 1
        assert page.evaluate("state.selectedQueue.has('job')")
        assert page.locator('#settings-cpu-value').input_value() == '7'
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
        assert page.locator('#settings-cpu-value').input_value() == '7'
        assert page.url.endswith(route_path + '#queue')
        destination = page.url
        page.locator('#pbgui-guide-btn').click()
        playwright.expect(page.locator('#pbgui-shared-help-ovl')).to_have_class('visible')
        assert page.url == destination
        assert not errors, errors
        browser.close()
