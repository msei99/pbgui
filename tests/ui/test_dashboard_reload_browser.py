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
    with sync_playwright() as driver:
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
