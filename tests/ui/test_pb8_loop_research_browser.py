"""Optimizer recommendations use the real Loop report with isolated browser/API fixtures."""
from pathlib import Path
import copy
import json
from urllib.parse import urlparse

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize('prefix,width,sidebar', [('', 1200, 160), ('/nested', 1200, 420), ('', 720, 160)])
def test_research_report_download_reload_and_automatic_update(tmp_path, prefix, width, sidebar):
    """Real navigation, report rendering, safe text, export and state restoration stay offline."""
    browser_api = __import__('browser_driver')
    source = (ROOT / 'frontend/v7_optimize.html').read_text()
    for key, value in {'API_BASE': prefix+'/api/optimize-v8', 'OPTIMIZE_VERSION': 'v8', 'VERSION': 'test',
                       'SERIAL': '1', 'NAV_HASH': 'test', 'BASE_PREFIX': prefix, 'LIMITS_META': '{}',
                       'OPTIMIZE_NAV_TITLE': 'PB8 Optimize', 'OPTIMIZE_NAV_CURRENT': 'v8_optimize'}.items():
        source = source.replace('%%'+key+'%%', value)
    identity, operation = 'a'*32, 'b'*32
    recommendation = {'operation': operation, 'reason': '<img src=x onerror=alert(1)>Measured settings'}
    group = {'operations': [operation], 'trial_count': 2, 'seeds': [0, 1], 'qualified_count': 3,
             'validated_count': 4, 'technical_failure_count': 1, 'measurement_coverage': .5, 'budget': {'mode': 'proxy', 'value': 50000},
             'distribution': [{'mean': 3, 'min': 2, 'max': 4, 'stdev': 1.414}],
             'uncertainty': 'Repeated short trials; deep-search performance remains unverified'}
    run = {'id': identity, 'name': 'Measured optimizer', 'status': 'completed', 'phase': 'evaluate', 'round': 1,
           'created_at': 1000, 'updated_at': 1001, 'ended': True, 'jobs': [], 'history': [], 'cycles': [],
           'observer_jobs': [], 'settings': {'config_name': 'fixture', 'execution': 'gpu'},
           'validation_recovery': {'active': True, 'attempts': 1, 'retry_at': 2000, 'error': 'Temporary training issue'},
           'optimizer_research_version': 1, 'optimizer_assessment': {'criteria': [
               {'metric': 'gain', 'direction': 'max', 'reducer': 'mean', 'reason': 'Total return'}],
               'recommendations': [recommendation], 'groups': [group], 'reason': 'Repeatable short-run quality'}}
    export = {'loop_id': identity, 'deep_search_verified': False,
              'recommendations': [{'operation': operation, 'config': {'optimize': {'scoring': ['fixture']}},
                                    'override_configs': {}}]}
    for index, gain in enumerate((2, 10, -1, None, 10)):
        run['observer_jobs'].append({'operation': str(index+1)*32, 'round': index,
            'kind': 'observer_full_range', 'status': 'complete', 'log_available': True,
            'simulation_complete': True, 'report_done': True, 'run_started_at': 100,
            'ended_at': 100+[2, 10, 90, 3601, 10][index],
            'metrics': {'gain': gain, 'adg': gain/100 if gain is not None else None,
                        'drawdown': .1, 'recovery_days': 2, 'sharpe': gain}})
    original_jobs = copy.deepcopy(run['observer_jobs'])
    errors, downloads = [], []
    with browser_api.sync_browser() as driver:
        browser = driver.chromium.launch(headless=True)
        page = browser.new_page(viewport={'width': width, 'height': 900}, locale='de-DE')
        page.set_default_timeout(7000)
        page.add_init_script("localStorage.setItem('pbgui.sidebar.width', '"+str(sidebar)+"')")
        page.on('pageerror', lambda exc: errors.append(str(exc)))

        def respond(route):
            """All requests are intercepted, including mounted routes and export."""
            path = urlparse(route.request.url).path.removeprefix(prefix) if prefix else urlparse(route.request.url).path
            if path in {'/audit', '/api/optimize-v8/main_page'}:
                route.fulfill(body=source.replace('navCurrent: "v8_optimize"', 'navCurrent: "dashboards"') if path == '/audit' else source, content_type='text/html')
            elif path.startswith('/app/'):
                asset = ROOT / 'frontend' / path.removeprefix('/app/')
                route.fulfill(body=asset.read_bytes() if asset.is_file() else b'',
                              content_type='text/css' if asset.suffix == '.css' else 'text/javascript')
            elif path == '/api/optimize-v8/loops':
                route.fulfill(json={'loops': [copy.deepcopy(run)]})
            elif path == '/api/optimize-v8/loops/'+identity:
                route.fulfill(json=copy.deepcopy(run))
            elif path.endswith('/optimizer-configs'):
                downloads.append(path)
                route.fulfill(json=export)
            elif path.endswith('/loops/configs'):
                route.fulfill(json={'configs': []})
            elif path == '/api/help/index':
                route.fulfill(json=[{'file': '43_pbv8_optimize', 'title': 'PB8 Optimize'}])
            elif path == '/api/help/content':
                route.fulfill(json={'content': '# PB8 Optimize\n\nOptimizer research.'})
            else:
                route.fulfill(json={})

        page.route('**/*', respond)
        page.route_web_socket('**/*', lambda socket: socket.on_message(lambda _: None))
        page.goto('http://pbgui.test'+prefix+'/audit')
        page.locator('.nav-group-btn[data-group="pbv8"]').click()
        page.locator('.nav-item[data-page="v8_optimize"]').click()
        page.wait_for_url('**/api/optimize-v8/main_page')
        location = 'http://pbgui.test'+prefix+'/api/optimize-v8/main_page?loop_id='+identity+'&loop_view=run&loop_tab=overview#loops-results'
        page.goto(location)
        body = page.locator('#loop-report-body')
        browser_api.expect(body).to_contain_text('Optimizer settings for deep search')
        browser_api.expect(body).to_contain_text('gain (mean): 3.00000')
        browser_api.expect(body.get_by_role('columnheader', name='Trials', exact=True)).to_be_visible()
        browser_api.expect(body.get_by_role('columnheader', name='Successful trials', exact=True)).to_be_visible()
        assert 'Trials / seeds' not in body.inner_text()
        assert 'Successful seeds' not in body.inner_text()
        browser_api.expect(body).to_contain_text('Automatic CPU validation recovery')
        browser_api.expect(body).to_contain_text('50%')
        browser_api.expect(body).to_contain_text(recommendation['reason'])
        assert body.locator('img').count() == 0
        with page.expect_download() as pending:
            page.get_by_role('button', name='Download optimizer recommendations').click()
        pending.value.save_as(tmp_path/'optimizer.json')
        assert json.loads((tmp_path/'optimizer.json').read_text()) == export
        assert downloads == ['/api/optimize-v8/loops/'+identity+'/optimizer-configs']
        run['updated_at'] = 1002
        run['validation_recovery']['active'] = False
        run['optimizer_assessment']['reason'] = 'Updated measured recommendation'
        browser_api.expect(body).to_contain_text('Updated measured recommendation', timeout=10000)
        assert 'Automatic CPU validation recovery' not in body.inner_text()
        page.reload()
        browser_api.expect(body).to_contain_text('Updated measured recommendation')
        assert page.url == location
        page.locator('#loop-report-tab-performance').click()
        table = body.locator('table').first

        def order():
            """Read original row identities without depending on localized numeric formatting."""
            return table.locator('tbody tr').evaluate_all('rows=>rows.map(row=>Number(row.dataset.reportIndex))')

        gain_header = table.get_by_role('button', name='Gain', exact=True)
        gain_header.click()
        assert order() == [2, 0, 1, 4, 3]
        table.get_by_role('button', name='Gain ↑', exact=True).press('Enter')
        assert order() == [1, 4, 0, 2, 3]
        browser_api.expect(table.get_by_role('columnheader', name='Gain ↓', exact=True)).to_have_attribute('aria-sort', 'descending')
        table.get_by_role('button', name='Duration', exact=True).click()
        assert order() == [0, 1, 4, 2, 3]
        table.get_by_role('button', name='ADG', exact=True).click()
        assert order() == [2, 0, 1, 4, 3]
        # Force a real automatic update while the chosen sort remains active.
        run['observer_jobs'][0]['metrics']['adg'] = .2
        run['updated_at'] += 1
        page.evaluate("document.dispatchEvent(new Event('visibilitychange'))")
        page.wait_for_function("()=>{const table=document.querySelector('#loop-report-body table');return table&&Array.from(table.tBodies[0].rows).map(r=>r.dataset.reportIndex).join(',')==='2,1,4,0,3';}")
        assert order() == [2, 1, 4, 0, 3]
        page.reload()
        browser_api.expect(page.locator('#loop-report-tab-performance')).to_have_attribute('aria-selected', 'true')
        assert order() == [2, 1, 4, 0, 3]
        # Persisted ordering must not reassign Results actions by the new row position.
        assert table.locator('tbody tr').evaluate_all("rows=>rows.map(row=>row.querySelector('[data-loop-result]').dataset.loopResult)") == [identity+':'+str(i+1)*32 for i in (2,1,4,0,3)]
        page.locator('#loop-report-tab-overview').click()
        page.locator('#loop-report-tab-performance').click()
        assert order() == [2, 1, 4, 0, 3]
        # Sorting never writes back into the source records; only our explicit update did.
        original_jobs[0]['metrics']['adg'] = .2
        assert run['observer_jobs'] == original_jobs
        page.locator('#loop-report-close').click()
        guide_location = page.url
        page.locator('#pbgui-guide-btn').click()
        page.wait_for_selector('#pbgui-shared-help-ovl.visible')
        assert page.url == guide_location
        page.locator('#pbgui-shared-help-close').click()
        if width > 760:
            assert abs(page.locator('#sidebar').bounding_box()['width'] - sidebar) < 2
        assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
        assert not errors
        browser.close()
