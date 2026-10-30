"""Dashboard navigation survives real browser reloads without stale widget content."""
import json
import re
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest
from browser_driver import expect, sync_browser

ROOT = Path(__file__).resolve().parents[2]


def test_dashboard_manage_guide_priority_across_iframe():
    """Keep child dialogs intact when Escape dismisses real parent overlays."""
    try:
        _check_dashboard_manage_guide_priority_across_iframe()
    except Exception as error:
        pytest.fail(str(error), pytrace=False)


def _check_dashboard_manage_guide_priority_across_iframe():
    """Run the browser scenario with compact failures for bounded test reports."""
    from starlette.requests import Request
    from api import dashboard

    name = 'Modal'
    position = {'user': 'test-user', 'symbol': 'BTCUSDT', 'side': 'long', 'size': 1,
                'upnl': 2, 'entry': 100, 'price': 102, 'dca': 0, 'next_dca': 0,
                'next_tp': 0, 'pos_value': 102}
    config = {'name': name, 'rows': 1, 'cols': 1, 'dashboard_type_1_1': 'POSITIONS',
              'dashboard_positions_users_1_1': ['test-user']}
    errors, previews = [], []
    with sync_browser() as driver:
        browser = driver.chromium.launch()
        try:
            page = browser.new_page(viewport={'width': 1280, 'height': 900})
            page.set_default_timeout(5000)
            page.on('pageerror', lambda error: errors.append(str(error)))

            def respond(route):
                """Serve real menu, iframe, renderer and Guide with synthetic data only."""
                path = urlparse(route.request.url).path
                if path == '/app/start.html':
                    route.fulfill(content_type='text/html', body='<nav id="topnav"></nav><script>window.PBGUI_NAV_CONFIG={current:"welcome",authenticated:true};</script><script src="/app/pbgui_nav.js"></script>')
                elif path == '/api/dashboard/main_page':
                    html = (ROOT / 'frontend/dashboard_main.html').read_text()
                    for key, value in {'API_BASE': '/api', 'VERSION': 'test', 'CURRENT': '',
                                       'DASHBOARDS_JSON': json.dumps([name]), 'SERIAL': '1',
                                       'NAV_HASH': 'test'}.items():
                        html = html.replace('%%' + key + '%%', value)
                    route.fulfill(content_type='text/html', body=html)
                elif path == '/api/dashboard/editor_page':
                    response = dashboard.get_editor_page(
                        Request({'type': 'http', 'root_path': ''}), name=name, api_base='',
                        view_only=True, standalone=False, session=object(),
                    )
                    route.fulfill(content_type='text/html', body=response.body)
                elif path == '/api/dashboards':
                    route.fulfill(json={'dashboards': [name]})
                elif path == '/api/dashboards/' + name:
                    route.fulfill(json={'name': name, 'config': config})
                elif path == '/api/dashboard/users':
                    route.fulfill(json={'users': ['test-user']})
                elif path == '/api/dashboard/positions_data':
                    route.fulfill(json={'positions': [position], 'source': 'db'})
                elif path == '/api/dashboard/positions/manage':
                    payload = route.request.post_data_json
                    assert payload['dry_run'] is True
                    previews.append(payload)
                    route.fulfill(json={'dry_run': True, 'config': {'bot': {'long': {}}}})
                elif path == '/api/help/index':
                    route.fulfill(json=[{'file': '33_dashboard.md', 'title': 'Dashboards'}])
                elif path == '/api/help/content':
                    route.fulfill(json={'content': '# Dashboards\n\nIsolated Guide fixture.'})
                elif path.startswith('/app/'):
                    asset = ROOT / 'frontend' / path.removeprefix('/app/')
                    route.fulfill(body=asset.read_bytes() if asset.is_file() else b'',
                                  content_type='text/css' if asset.suffix == '.css' else 'text/javascript')
                elif path.endswith('/stream'):
                    route.fulfill(status=204, body='')
                else:
                    route.fulfill(json={})

            page.route('**/*', respond)
            page.route_web_socket('**/ws/dashboard', lambda socket: None)
            page.goto('http://dashboard.test/app/start.html')
            page.locator('.nav-group-btn', has_text='Information').click()
            page.locator('.nav-item[data-page="dashboards"]').click()
            page.locator('.sb-item[data-name="Modal"]').click()
            frame = page.frame_locator('#content-frame')
            manage = frame.locator('#dp-manage-modal')
            frame.get_by_role('button', name='Manage', exact=True).click()
            close = manage.locator('.dp-modal-close')
            expect(close).to_be_focused()
            guide = page.locator('#pbgui-shared-help-ovl')
            page.wait_for_function('window.PBGuiSharedHelp && window.PBGuiSharedHelp.open')
            page.locator('#pbgui-guide-btn').click()
            expect(guide).to_be_visible()
            close.focus()  # Keyboard focus is inside the real child browsing context.
            page.keyboard.press('Escape')
            expect(guide).to_be_hidden()
            expect(manage).to_be_visible()
            expect(close).to_be_focused()

            # Tab from the child yields focus to the higher parent's close action.
            page.locator('#pbgui-guide-btn').click()
            expect(guide).to_be_visible()
            close.focus()
            page.keyboard.press('Tab')
            expect(page.locator('#pbgui-shared-help-close')).to_be_focused()
            page.keyboard.press('Escape')
            expect(guide).to_be_hidden()
            expect(manage).to_be_visible()

            # A confirmation above Guide must consume exactly one Escape as cancel.
            page.locator('#pbgui-guide-btn').click()
            expect(guide).to_be_visible()
            page.evaluate('''() => { void PBGuiDialogs.confirm({title: 'Isolated test',
                message: 'No action will be executed.'}).then(value => window.testConfirmResult = value); }''')
            confirm = page.locator('#pbgui-dialog-ovl')
            expect(confirm).to_be_visible()
            close.focus()
            page.keyboard.press('Escape')
            expect(confirm).to_be_hidden()
            expect(guide).to_be_visible()
            expect(manage).to_be_visible()
            assert page.evaluate('window.testConfirmResult') is False
            close.focus()
            page.keyboard.press('Escape')
            expect(guide).to_be_hidden()
            expect(manage).to_be_visible()

            manage.get_by_role('button', name='Preview Panic', exact=True).click()
            preview = frame.locator('#dp-preview-modal')
            expect(preview).to_be_visible()
            page.locator('#pbgui-guide-btn').click()
            expect(guide).to_be_visible()
            preview.locator('.dp-modal-close').focus()
            page.keyboard.press('Escape')
            expect(guide).to_be_hidden()
            expect(preview).to_be_visible()
            expect(manage).to_be_visible()
            page.keyboard.press('Escape')
            expect(preview).to_have_count(0)
            expect(manage).to_be_visible()
            expect(manage.get_by_role('button', name='Preview Panic', exact=True)).to_be_focused()
            page.keyboard.press('Escape')
            expect(manage).to_have_count(0)
            expect(frame.get_by_role('button', name='Manage', exact=True)).to_be_focused()
            expect(page.locator('.sb-item.active')).to_have_attribute('data-name', name)
            assert len(previews) == 1
            assert not errors
        finally:
            browser.close()


@pytest.mark.parametrize('editing', [False, True])
def test_dashboard_menu_reload_restores_selection_and_filter(editing):
    """Use real navigation and shell assets while all dashboard data stays synthetic."""
    dashboards = ['Alpha', 'HL manicpt', 'Beta', 'Gamma']
    errors, loads = [], []
    with sync_browser() as driver:
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


@pytest.mark.parametrize('editing', [False, True])
@pytest.mark.parametrize('saved_sum', ['YEAR', 'ALL_TIME'])
def test_ppl_aggregation_controls_persist_and_update(editing, saved_sum):
    """Exercise real menu, editor, Plotly, save/reload and income notifications."""
    from starlette.requests import Request
    from api import dashboard

    name = 'PPL portfolio'
    # The sidebar filter is displayed only for four or more dashboards.
    dashboard_names = ['Alpha', name, 'Beta', 'Gamma']
    config = {
        'name': name, 'rows': 1, 'cols': 1, 'dashboard_type_1_1': 'P+L',
        'dashboard_ppl_users_1_1': ['alice'],
        'dashboard_ppl_period_1_1': 'CUSTOM:2023-01-01:2025-12-31',
        'dashboard_ppl_sum_period_1_1': saved_sum,
    }
    pending, queries, writes, sockets, errors = {}, [], [], [], []
    revision = 0
    labels = {
        'DAY': ['2023-01-01', '2024-01-01', '2024-06-01', '2025-01-01'],
        'WEEK': ['2023-00', '2024-01', '2024-23', '2025-00'],
        'MONTH': ['2023-01', '2024-01', '2024-06', '2025-01'],
        'YEAR': ['2023', '2024', '2025'], 'ALL_TIME': ['ALL_TIME'],
    }
    with sync_browser() as driver:
        browser = driver.chromium.launch()
        page = browser.new_page(viewport={'width': 1280, 'height': 900})
        page.set_default_timeout(10000)
        page.on('pageerror', lambda error: errors.append(str(error)))

        def respond(route):
            """Serve real frontend files and keep every API write in test memory."""
            url = urlparse(route.request.url)
            path, query = url.path, parse_qs(url.query)
            if path == '/app/start.html':
                route.fulfill(content_type='text/html', body='<nav id="topnav"></nav><script>window.PBGUI_NAV_CONFIG={current:"welcome",authenticated:true};</script><script src="/app/pbgui_nav.js"></script>')
            elif path == '/api/dashboard/main_page':
                source = (ROOT / 'frontend/dashboard_main.html').read_text()
                values = {'API_BASE': '/api', 'VERSION': 'test', 'CURRENT': '',
                          'DASHBOARDS_JSON': json.dumps(dashboard_names), 'SERIAL': '1', 'NAV_HASH': 'test'}
                for key, value in values.items():
                    source = source.replace('%%' + key + '%%', value)
                route.fulfill(content_type='text/html', body=source)
            elif path == '/api/dashboard/editor_page':
                response = dashboard.get_editor_page(
                    Request({'type': 'http', 'root_path': ''}), name=name, api_base='',
                    view_only=query.get('view_only') == ['1'],
                    standalone=query.get('standalone') == ['1'], session=object(),
                )
                route.fulfill(content_type='text/html', body=response.body, headers={'Cache-Control': 'no-store'})
            elif path == '/api/dashboards':
                route.fulfill(json={'dashboards': dashboard_names})
            elif path == '/api/dashboards/' + name.replace(' ', '%20') or path == '/api/dashboards/' + name:
                if route.request.method == 'POST':
                    config.update(route.request.post_data_json)
                    writes.append(dict(config))
                    route.fulfill(json={'status': 'ok'})
                else:
                    route.fulfill(json={'name': name, 'config': config})
            elif path == '/api/dashboard/pending_full':
                if route.request.method == 'POST':
                    pending.update(route.request.post_data_json)
                    route.fulfill(json={'status': 'ok'})
                elif route.request.method == 'DELETE':
                    pending.clear()
                    route.fulfill(json={'status': 'ok'})
                else:
                    route.fulfill(json={'found': bool(pending), 'config': pending})
            elif path == '/api/dashboard/users':
                route.fulfill(json={'users': ['alice', 'bob']})
            elif path == '/api/dashboard/ppl_data':
                queries.append(query)
                selected = query['sum_period'][0]
                route.fulfill(json={
                    'sum_period': selected, 'from_date': '2023-01-01', 'to_date': '2025-12-31',
                    'bars': [{'period': label, 'profits': 10 + revision, 'losses': -2}
                             for label in labels[selected]],
                })
            elif path.startswith('/app/'):
                asset = ROOT / 'frontend' / path.removeprefix('/app/')
                route.fulfill(body=asset.read_bytes() if asset.is_file() else b'', content_type='text/css' if asset.suffix == '.css' else 'text/javascript')
            elif path.endswith('/stream'):
                route.fulfill(status=204, body='')
            else:
                route.fulfill(json={})

        page.route('**/*', respond)
        page.route_web_socket('**/ws/dashboard', lambda socket: sockets.append(socket))
        page.goto('http://dashboard.test/app/start.html')
        page.locator('.nav-group-btn', has_text='Information').click()
        page.locator('.nav-item[data-page="dashboards"]').click()
        page.locator('.sb-item[data-name="' + name + '"]').click()
        page.locator('#sidebar-search').fill('PPL')
        if editing:
            page.locator('#sb-edit').click()
        frame = page.frame_locator('#content-frame')
        selector = frame.locator('.dt-meta-controls select').first
        chart = frame.locator('.dt-chart')
        expect(selector).to_have_value(saved_sum)
        assert selector.locator('option').all_text_contents() == list(labels)
        expect(chart).to_have_class('dt-chart js-plotly-plot')

        for aggregation in ['DAY', 'WEEK', 'MONTH', 'YEAR', 'ALL_TIME']:
            if aggregation == 'YEAR':
                chart.evaluate('(chart) => Plotly.relayout(chart, {"xaxis.range": [1, 2], "yaxis.range": [-1, 5]})')
            selector.select_option(aggregation)
            page.wait_for_function(
                '(args) => {const f = document.querySelector("#content-frame").contentDocument;'
                'const chart = f.querySelector(".dt-chart");'
                'return chart && chart.data && JSON.stringify(chart.data[0].x) === JSON.stringify(args);}',
                arg=labels[aggregation],
            )
            assert queries[-1]['sum_period'] == [aggregation]
            assert queries[-1]['users'] == ['alice']
            assert queries[-1]['period'] == ['CUSTOM:2023-01-01:2025-12-31']
            if aggregation == 'ALL_TIME':
                bounds = chart.evaluate('(chart) => chart._fullLayout.xaxis.range')
                assert bounds[0] < 0 < bounds[1], 'ALL_TIME aggregate must remain visible after zoom'

        # Verify both new values after save/reload in both view and edit modes.
        for aggregation in ['YEAR', 'ALL_TIME']:
            selector.select_option(aggregation)
            expect(page.locator('#sb-save' if editing else '#sb-view-save')).to_be_visible()
            previous_writes = len(writes)
            page.locator('#sb-save' if editing else '#sb-view-save').click()
            page.wait_for_function('() => !document.getElementById("sb-view-save")' if not editing else '() => !!document.getElementById("sb-edit")')
            assert len(writes) > previous_writes
            assert config['dashboard_ppl_sum_period_1_1'] == aggregation
            # Re-enter editor if saving returned to view mode.
            if editing:
                page.locator('#sb-edit').click()
            page.reload()
            expect(selector).to_have_value(aggregation)
            expect(page.locator('#sidebar-search')).to_have_value('PPL')
            assert parse_qs(urlparse(page.url).query)['current'] == [name]

            revision += 1
            selector.blur()
            sockets[-1].send(json.dumps({'type': 'income_updated'}))
            page.wait_for_function(
                '(value) => {const chart = document.querySelector("#content-frame").contentDocument.querySelector(".dt-chart");'
                'return chart && chart.data && chart.data[0].y[0] === value;}', arg=10 + revision,
            )
            expect(selector).to_have_value(aggregation)
            assert queries[-1]['sum_period'] == [aggregation]

        assert not errors
        browser.close()


def test_orders_viewport_survives_updates():
    """Real menu, shell and local LWC 5.2.0 retain logical ranges on isolated data."""
    from starlette.requests import Request
    from api import dashboard

    name = 'Orders viewport fixture'
    names = ['Alpha', name, 'Beta', 'Gamma']
    config = {
        'name': name, 'rows': 2, 'cols': 1,
        'dashboard_type_1_1': 'POSITIONS', 'dashboard_type_2_1': 'ORDERS',
        'dashboard_positions_users_1_1': ['alice'],
        'dashboard_orders_2_1': 'view_orders_1_1', 'dashboard_height_2_1': 500,
    }
    position = dict(user='alice', symbol='BTCUSDT', side='long', size=1, upnl=1,
                    entry=100, price=101, dca=0, next_dca=99, next_tp=102, pos_value=101)
    start = 1700000000000

    def candle(index):
        """Leave one interior gap for subsequent history repair."""
        return dict(t=start + index * 60000, o=100, h=102, l=99, c=101, v=10)

    candles = [candle(i) for i in range(180) if i != 40]
    snapshots, history, pending = [], [], {}
    errors, assets = [], []
    with sync_browser() as driver:
        browser = driver.chromium.launch()
        try:
            page = browser.new_page(viewport={'width': 1280, 'height': 1000})
            page.set_default_timeout(7000)
            page.on('pageerror', lambda error: errors.append(str(error)))

            def respond(route):
                """All network responses and saves stay in memory; no runtime files."""
                url = urlparse(route.request.url)
                path, query = url.path, parse_qs(url.query)
                if path == '/app/start.html':
                    route.fulfill(content_type='text/html', body='<nav id="topnav"></nav><script>window.PBGUI_NAV_CONFIG={current:"welcome",authenticated:true};</script><script src="/app/pbgui_nav.js"></script>')
                elif path == '/api/dashboard/main_page':
                    source = (ROOT / 'frontend/dashboard_main.html').read_text()
                    for key, value in {'API_BASE': '/api', 'VERSION': 'test', 'CURRENT': '',
                                       'DASHBOARDS_JSON': json.dumps(names), 'SERIAL': '1', 'NAV_HASH': 'test'}.items():
                        source = source.replace('%%' + key + '%%', value)
                    route.fulfill(content_type='text/html', body=source)
                elif path == '/api/dashboard/editor_page':
                    response = dashboard.get_editor_page(
                        Request({'type': 'http', 'root_path': ''}), name=name, api_base='',
                        view_only=query.get('view_only') == ['1'], standalone=False, session=object())
                    route.fulfill(content_type='text/html', body=response.body)
                elif path == '/api/dashboards':
                    route.fulfill(json={'dashboards': names})
                elif path.startswith('/api/dashboards/'):
                    route.fulfill(json={'name': name, 'config': config})
                elif path == '/api/dashboard/pending_full':
                    if route.request.method == 'POST':
                        pending.update(route.request.post_data_json)
                        route.fulfill(json={'status': 'ok'})
                    elif route.request.method == 'DELETE':
                        pending.clear()
                        route.fulfill(json={'status': 'ok'})
                    else:
                        route.fulfill(json={'found': bool(pending), 'config': pending})
                elif path == '/api/dashboard/users':
                    route.fulfill(json={'users': ['alice']})
                elif path == '/api/dashboard/positions_data':
                    route.fulfill(json={'positions': [position], 'source': 'db'})
                elif path == '/api/dashboard/orders_data':
                    if 'since' in query:
                        history.append(query)
                        route.fulfill(json={'candles': [candle(i) for i in range(-30, 45)]})
                    else:
                        route.fulfill(json={'user': 'alice', 'symbol': 'BTCUSDT', 'candles': candles,
                                            'position': position, 'orders': [], 'current_price': 101})
                elif path == '/api/dashboard/candles_data':
                    snapshots.append(query)
                    route.fulfill(json={'candles': candles})
                elif path == '/api/help/index':
                    route.fulfill(json=[{'file': '33_dashboard.md', 'title': 'Dashboard'}])
                elif path == '/api/help/content':
                    route.fulfill(json={'html': '<h1>Dashboard</h1><p>Orders viewport fixture</p>'})
                elif path.startswith('/app/'):
                    asset = ROOT / 'frontend' / path.removeprefix('/app/')
                    body = asset.read_bytes() if asset.is_file() else b''
                    assets.append(route.request.url)
                    if asset.name == 'dashboard_render.js':
                        # Observe the public controller while retaining the real renderer/library.
                        body += b'\nconst originalBuildOrders=DashRender.buildOrders; DashRender.buildOrders=function(...args){const c=originalBuildOrders(...args);if(c)window.ordersTest=c;return c;};'
                    route.fulfill(body=body, content_type='text/css' if asset.suffix == '.css' else 'text/javascript')
                elif path.endswith('/stream'):
                    route.fulfill(status=204, body='')
                else:
                    route.fulfill(json={})

            page.route('**/*', respond)
            page.route_web_socket('**/ws/**', lambda socket: None)
            page.goto('http://dashboard.test/app/start.html')
            page.locator('.nav-group-btn', has_text='Information').click()
            page.locator('.nav-item[data-page="dashboards"]').click()
            page.locator('.sb-item', has_text=name).click()
            page.locator('#sidebar-search').fill('Orders')
            frame = page.frame_locator('#content-frame')
            frame.locator('.dp-table tbody tr').first.click()
            frame.locator('.do-chart-wrap canvas').first.wait_for(state='visible')
            inner = page.locator('#content-frame').element_handle().content_frame()
            inner.wait_for_function('() => !!window.ordersTest')

            def settle():
                """Include deferred range callbacks and both initial layout passes."""
                inner.evaluate('() => new Promise(resolve => setTimeout(() => requestAnimationFrame(() => requestAnimationFrame(resolve)), 260))')

            def bounds():
                """Read the logical range from the actual chart instance."""
                return inner.evaluate('() => ordersTest.chart.timeScale().getVisibleLogicalRange()')

            def assert_range(expected):
                """Fractional pan/zoom must survive asynchronous chart work."""
                settle()
                assert bounds() == pytest.approx(expected, abs=0.0001)

            def reconcile():
                """Use the production automatic-return path instead of a refresh control."""
                previous = len(snapshots)
                inner.evaluate('() => document.dispatchEvent(new Event("visibilitychange"))')
                expect(frame.locator('.do-chart-wrap canvas').first).to_be_visible()
                settle()
                assert len(snapshots) > previous

            settle()
            assert inner.evaluate('() => LightweightCharts.version()') == '5.2.0'
            assert bounds()['to'] - (len(candles) - 1) >= 8
            orders_source = (ROOT / 'frontend/dashboard_orders.html').read_text(encoding='utf-8')
            versions = re.findall(r"var DR_VERSION\s*=\s*'([^']+)'", orders_source)
            assert len(versions) == 1 and versions[0].strip(), 'Orders must declare one non-empty renderer version'
            renderer_requests = [urlparse(url) for url in assets
                                 if urlparse(url).path == '/app/dashboard_render.js']
            assert renderer_requests, 'The real menu path must load the shared renderer'
            assert all(parse_qs(url.query) == {'v': versions} for url in renderer_requests)
            assert inner.evaluate('() => DashRender.VERSION') == versions[0]
            # Observe queued fitContent/setData behavior on the actual library, independently.
            raw = inner.evaluate('''async () => {
                const div=document.createElement('div'); document.body.appendChild(div);
                const c=LightweightCharts.createChart(div,{width:600,height:300,timeScale:{rightOffset:8}});
                try {
                    const s=c.addSeries(LightweightCharts.CandlestickSeries), ts=c.timeScale();
                    const rows=ordersTest.series.data();
                    const tick=()=>new Promise(r=>requestAnimationFrame(()=>requestAnimationFrame(r)));
                    s.setData(rows); await tick(); const initial=ts.getVisibleLogicalRange();
                    ts.fitContent(); await tick(); const fit=ts.getVisibleLogicalRange();
                    s.setData(rows.slice(0,-1)); await tick(); const replaced=ts.getVisibleLogicalRange();
                    return {initial,fit,replaced};
                } finally {c.remove(); div.remove();}
            }''')
            assert raw['initial']['to'] == pytest.approx(len(candles) - 1 + 8), raw
            assert raw['fit']['to'] == pytest.approx(len(candles) - 1 + 8), raw
            assert raw['replaced']['to'] == pytest.approx(len(candles) - 2 + 8), raw
            # Continuing follow must preserve fractional offsets. Returning from
            # historical panning is tested separately and still restores its margin.
            inner.evaluate('''() => {
                const ts=ordersTest.chart.timeScale(), range=ts.getVisibleLogicalRange();
                ts.setVisibleLogicalRange({from:range.from+.125,to:range.to+.125});
            }''')
            settle()
            base = bounds()
            reconcile()
            assert_range(base)
            candles.append(candle(180))
            reconcile()
            assert bounds()['to'] == pytest.approx(base['to'] + 1)
            # Public WS controller path with the real chart, volume and range callbacks.
            inner.evaluate('(c) => ordersTest.updateCandle([c.t,c.o,c.h,c.l,c.c,c.v])', candle(181))
            settle()
            assert bounds()['to'] == pytest.approx(base['to'] + 2)

            # Start zoomed at the latest position, then exercise real pointer/wheel gestures.
            inner.evaluate('''() => {
                const to=ordersTest.series.data().length-1+8;
                ordersTest.chart.timeScale().setVisibleLogicalRange({from:to-100,to});
            }''')
            settle()
            # Keep the last candle on screen while moving away from the follow threshold.
            box = frame.locator('.do-chart-wrap').bounding_box()
            x, y = box['x'] + box['width'] * 0.5, box['y'] + box['height'] * 0.5
            page.mouse.move(x, y)
            page.mouse.down()
            # Native input must leave the label-dependent follow margin, which
            # can exceed the chart library's eight-bar default after zooming.
            page.mouse.move(x - 80, y, steps=8)
            page.mouse.up()
            page.mouse.wheel(0, -30)
            settle()
            panned = bounds()
            assert abs(panned['to'] - (len(candles) + 8)) > 1
            assert panned['to'] >= len(candles), 'last candle stays visible without resuming follow'
            reconcile()
            assert_range(panned)
            inner.evaluate('(c) => ordersTest.updateCandle([c.t,c.o,c.h,c.l,c.c,c.v])', candle(182))
            assert_range(panned)

            # Fractional historical range, then real onLoadMore with overlaps and an interior gap.
            inner.evaluate('() => ordersTest.chart.timeScale().setVisibleLogicalRange({from:30.25,to:100.75})')
            expected = {'from': 30.25, 'to': 100.75}
            assert_range(expected)
            reconcile()
            assert_range(expected)
            inner.evaluate('() => ordersTest.chart.timeScale().setVisibleLogicalRange({from:10.25,to:80.75})')
            settle()
            assert len(history) == 1
            assert_range({'from': 40.25, 'to': 110.75})
            # Restore was programmatic even though from < 20 in the pre-prepend range.
            inner.evaluate('(rows) => ordersTest.prependData(rows)', [candle(i) for i in range(-30, 45)])
            assert_range({'from': 40.25, 'to': 110.75})
            assert len(history) == 1

            # Returning within one logical bar re-enables follow and retains zoom span.
            inner.evaluate('''() => {
                const to=ordersTest.series.data().length-1+8;
                ordersTest.chart.timeScale().setVisibleLogicalRange({from:to-70.5,to});
            }''')
            settle()
            to = bounds()['to']
            inner.evaluate('(c) => ordersTest.updateCandle([c.t,c.o,c.h,c.l,c.c,c.v])', candle(183))
            assert_range({'from': to + 1 - 70.5, 'to': to + 1})
            inner.evaluate('''(rows) => rows.forEach(c => ordersTest.updateCandle([c.t,c.o,c.h,c.l,c.c,c.v]))''',
                           [candle(i) for i in range(184, 187)])
            assert_range({'from': to + 4 - 70.5, 'to': to + 4})
            # A new gesture must invalidate a follow adjustment queued before LWC's frame.
            inner.evaluate('''() => {
                ordersTest.preserveViewport();
                document.querySelector('.do-chart-wrap').dispatchEvent(new PointerEvent('pointerdown',{bubbles:true}));
                ordersTest.chart.timeScale().setVisibleLogicalRange({from:25.125,to:95.625});
            }''')
            assert_range({'from':25.125,'to':95.625})
            frame.locator('.do-tf-btn', has_text='1m').click()
            settle()
            assert bounds()['to'] - (len(candles) - 1) >= 8

            inner.evaluate('() => ordersTest.chart.timeScale().setVisibleLogicalRange({from:25.25,to:100.75})')
            stable = {'from': 25.25, 'to': 100.75}
            assert_range(stable)
            frame.locator('.do-chart-wrap').hover()
            frame.locator('.do-fs-btn').click()
            inner.wait_for_function('() => !!document.fullscreenElement')
            assert_range(stable)
            inner.evaluate('() => document.exitFullscreen()')
            assert_range(stable)
            # Rapid notifications replace the timeout; a gesture before it fires wins.
            inner.evaluate('''() => {
                document.dispatchEvent(new Event('fullscreenchange'));
                document.dispatchEvent(new Event('webkitfullscreenchange'));
                ordersTest.chart.timeScale().setVisibleLogicalRange({from:35.125,to:110.625});
            }''')
            assert_range({'from': 35.125, 'to': 110.625})

            # Shared sidebar boundaries, narrow/wide layouts and reload restoration.
            for key, width in [('Home', 160), ('End', 420)]:
                page.locator('#sidebar-resize').focus()
                page.keyboard.press(key)
                assert page.locator('#sidebar').evaluate('(el) => el.getBoundingClientRect().width') == width
                assert page.locator('#sidebar-toolbar').evaluate('(el) => el.scrollWidth <= el.clientWidth')
                assert_range({'from': 35.125, 'to': 110.625})
            for width in [740, 1280]:
                page.set_viewport_size({'width': width, 'height': 1000})
                assert page.locator('#sidebar-toolbar').evaluate('(el) => el.scrollWidth <= el.clientWidth')
                assert_range({'from': 35.125, 'to': 110.625})
            url = page.url
            page.locator('#pbgui-guide-btn').click()
            expect(page.locator('#pbgui-shared-help-ovl')).to_be_visible()
            assert page.url == url
            page.locator('#pbgui-shared-help-box .ovl-close').click()
            assert_range({'from': 35.125, 'to': 110.625})
            # A gesture after first paint but before the 200 ms pass must win.
            early = inner.evaluate('''async (rows) => {
                const div=document.createElement('div');
                div.style.cssText='width:700px;height:300px'; document.body.appendChild(div);
                const ctrl=DashRender.renderOrders(div,{candles:rows},{timeframe:'1m'});
                try {
                    await new Promise(r=>requestAnimationFrame(()=>requestAnimationFrame(r)));
                    div.dispatchEvent(new WheelEvent('wheel'));
                    ctrl.chart.timeScale().setVisibleLogicalRange({from:35.125,to:100.625});
                    await new Promise(r=>setTimeout(r,260));
                    return ctrl.chart.timeScale().getVisibleLogicalRange();
                } finally {ctrl.destroy();div.remove();}
            }''', candles)
            assert early == pytest.approx({'from': 35.125, 'to': 100.625}, abs=0.0001)
            # Pending fullscreen work must never access a removed chart.
            inner.evaluate('''() => {
                document.dispatchEvent(new Event('fullscreenchange'));
                ordersTest.destroy(); ordersTest.destroy();
            }''')
            settle()
            page.reload()
            expect(page.locator('.sb-item.active')).to_have_attribute('data-name', name)
            expect(page.locator('#sidebar-search')).to_have_value('Orders')
            assert page.locator('#sidebar').evaluate('(el) => el.getBoundingClientRect().width') == 420
            expect(frame.locator('.dt-title', has_text='Orders')).to_be_visible()
            assert not errors
        finally:
            browser.close()


@pytest.mark.parametrize('width', [1200, 740, 260])
@pytest.mark.parametrize('pixel_ratio', [1, 2])
def test_orders_initial_label_clearance(width, pixel_ratio):
    """Measure real painted badges and candle edges from the first visible chart frame."""
    with sync_browser() as driver:
        browser = driver.chromium.launch()
        try:
            page = browser.new_page(viewport={'width': 1400, 'height': 800}, device_scale_factor=pixel_ratio)
            page.set_default_timeout(5000)
            errors = []
            page.on('pageerror', lambda error: errors.append(str(error)))
            page.route('**/*', lambda route: route.abort())
            page.set_content('<div id="chart" style="width:' + str(width) + 'px;height:580px"></div>')
            page.evaluate('''() => {
                window.orderPaints=[];
                let frame=null;
                const paths=new WeakMap();
                function box(ctx,x,y,w,h) {
                    const m=ctx.getTransform(), r=ctx.canvas.getBoundingClientRect();
                    const points=[[x,y],[x+w,y],[x,y+h],[x+w,y+h]].map(([a,b])=>({
                        x:r.left+(m.a*a+m.c*b+m.e)*r.width/ctx.canvas.width,
                        y:r.top+(m.b*a+m.d*b+m.f)*r.height/ctx.canvas.height}));
                    return {left:Math.min(...points.map(p=>p.x)),right:Math.max(...points.map(p=>p.x)),
                        top:Math.min(...points.map(p=>p.y)),bottom:Math.max(...points.map(p=>p.y))};
                }
                function current() {
                    if(!frame) {
                        frame={rects:[],titles:[]};
                        queueMicrotask(()=>{
                            const f=frame; frame=null;
                            if(!window.ctrl || ctrl.chart.timeScale().timeToCoordinate(lastTime)===null) return;
                            // HiDPI canvases may clear once before drawing any data.
                            // A paint containing candles but missing badges is still recorded.
                            if(!f.titles.length && !f.rects.some(r=>['#48bb78','#f56565'].includes(r.color))) return;
                            const ts=ctrl.chart.timeScale(), root=document.getElementById('chart').getBoundingClientRect();
                            const x=root.left+ts.timeToCoordinate(lastTime);
                            const spacing=ts.options().barSpacing;
                            const candles=f.rects.filter(r=>['#48bb78','#f56565'].includes(r.color)
                                && r.right-r.left<=spacing+2 && Math.abs((r.left+r.right)/2-x)<=1.5);
                            if(!candles.length) throw Error('No painted latest candle edge captured');
                            orderPaints.push({candleRight:Math.max(...candles.map(r=>r.right)),
                                titles:f.titles, range:ts.getVisibleLogicalRange(),width:ts.width(),spacing});
                        });
                    }
                    return frame;
                }
                const proto=CanvasRenderingContext2D.prototype;
                const fillRect=proto.fillRect, roundRect=proto.roundRect, beginPath=proto.beginPath, fill=proto.fill, fillText=proto.fillText;
                proto.fillRect=function(x,y,w,h){current().rects.push({...box(this,x,y,w,h),canvas:this.canvas,color:this.fillStyle});return fillRect.call(this,x,y,w,h);};
                proto.beginPath=function(){paths.delete(this);return beginPath.call(this);};
                proto.roundRect=function(x,y,w,h,...args){paths.set(this,box(this,x,y,w,h));return roundRect.call(this,x,y,w,h,...args);};
                proto.fill=function(...args){if(paths.has(this))current().rects.push({...paths.get(this),canvas:this.canvas,color:this.fillStyle});return fill.apply(this,args);};
                proto.fillText=function(text,x,y,...args){
                    if(text==='Entry'||text==='Price') {
                        const metrics=this.measureText(text), f=current();
                        const bounds=box(this,x-metrics.actualBoundingBoxLeft,y-metrics.actualBoundingBoxAscent,
                            metrics.actualBoundingBoxLeft+metrics.actualBoundingBoxRight,
                            metrics.actualBoundingBoxAscent+metrics.actualBoundingBoxDescent);
                        const backgrounds=f.rects.filter(r=>r.canvas===this.canvas && r.left<=bounds.left+0.5
                            && r.right>=bounds.right-0.5 && r.top<=bounds.top+0.5 && r.bottom>=bounds.bottom-0.5
                            && r.right-r.left<200 && r.bottom-r.top<50);
                        if(!backgrounds.length)throw Error('No actual title badge background captured: '+text);
                        // Include an outer border/background if LWC paints nested shapes.
                        const badge=backgrounds.reduce((a,b)=>a.left<b.left?a:b);
                        f.titles.push({text,textLeft:bounds.left,badgeLeft:badge.left});
                    }
                    return fillText.call(this,text,x,y,...args);
                };
            }''')
            page.add_script_tag(path=str(ROOT / 'frontend/vendor/lightweight-charts.standalone.production.js'))
            page.evaluate('''() => {
                window.orderRangeWrites=0;
                const original=LightweightCharts;
                window.LightweightCharts={...original,createChart(...args){
                    const chart=original.createChart(...args),scale=chart.timeScale();
                    const setRange=scale.setVisibleLogicalRange.bind(scale);
                    scale.setVisibleLogicalRange=function(range){orderRangeWrites++;return setRange(range);};
                    return chart;
                }};
            }''')
            page.add_script_tag(path=str(ROOT / 'frontend/dashboard_render.js'))
            page.evaluate('''() => {
                const candles=Array.from({length:500},(_,i)=>{
                    const p=60000+i*45+2500*Math.sin(i/20);
                    return {t:1700000000000+i*14400000,o:p,h:p+600,l:p-600,c:p+100,v:10};
                });
                candles[499].c=81926; window.lastTime=candles[499].t/1000;
                window.ctrl=DashRender.renderOrders(document.getElementById('chart'),{
                    candles,orders:[],position:{entry:84183,side:'long'},current_price:81926},{timeframe:'4h'});
            }''')
            page.wait_for_function('() => orderPaints.length > 0')
            page.evaluate('() => new Promise(r=>setTimeout(()=>requestAnimationFrame(()=>requestAnimationFrame(r)),350))')
            paints = page.evaluate('() => orderPaints')
            assert page.evaluate('() => orderRangeWrites') <= 4, 'One synchronous fit and at most three initial corrections'
            assert len(paints) >= 2, 'Both the first paint and settled layout must be observed'
            for state in paints:
                assert {t['text'] for t in state['titles']} == {'Entry', 'Price'}, state
                for title in state['titles']:
                    assert title['badgeLeft'] - state['candleRight'] >= 6, (width, pixel_ratio, state)
            # Follow keeps its zoom while recomputing pixel clearance after resizing.
            original_span = page.evaluate('() => {const r=ctrl.chart.timeScale().getVisibleLogicalRange();return r.to-r.from;}')
            resized_width = 740 if width == 260 else 260
            page.locator('#chart').evaluate('(el,w)=>el.style.width=w+"px"', resized_width)
            page.evaluate('() => new Promise(r=>setTimeout(()=>requestAnimationFrame(()=>requestAnimationFrame(r)),180))')
            after_resize = page.evaluate('() => orderPaints.at(-1)')
            assert after_resize['range']['to'] - after_resize['range']['from'] == pytest.approx(
                min(original_span, after_resize['width'] / 0.5 - 1), abs=0.001)
            assert min(t['badgeLeft'] for t in after_resize['titles']) - after_resize['candleRight'] >= 6
            # Font and title changes are owned layout events, preserving following intent.
            page.evaluate('''() => {
                ctrl.chart.applyOptions({layout:{fontSize:16}});
                document.fonts.dispatchEvent(new Event('loadingdone'));
                ctrl.updatePosition({entry:84183,side:'long'});
            }''')
            page.evaluate('() => new Promise(r=>setTimeout(()=>requestAnimationFrame(()=>requestAnimationFrame(r)),180))')
            after_font = page.evaluate('() => orderPaints.at(-1)')
            assert min(t['badgeLeft'] for t in after_font['titles']) - after_font['candleRight'] >= 6
            assert not errors, errors
            page.evaluate('() => ctrl.destroy()')
        finally:
            browser.close()
