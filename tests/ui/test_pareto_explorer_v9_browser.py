"""PB9 Pareto Explorer page boot, navigation and native handoffs in a real browser."""

import json
from pathlib import Path
from urllib.parse import urlsplit, parse_qs

from browser_driver import sync_browser

ROOT = Path(__file__).resolve().parents[2]


def _explorer_html(version='v9', result_path='/pb9/optimize_results/run'):
    """Render the production Explorer template like its FastAPI route."""
    html = (ROOT / 'frontend/v7_pareto_explorer.html').read_text()
    for key, value in {'API_BASE': '/api/pareto-explorer', 'WS_BASE': '', 'RESULT_PATH': result_path,
                       'OPTIMIZE_VERSION': version, 'VERSION': 'test', 'SERIAL': 'test'}.items():
        html = html.replace('"%%' + key + '%%"', json.dumps(value))
    return html.replace('%%OPTIMIZE_LABEL%%', version.upper()).replace('%%NAV_HASH%%', 'test')


def test_v9_explorer_boots_with_pb9_navigation_and_native_actions():
    """A PB9 result opens with PBv9 navigation, no PB8 Strategy Explorer and PB9 Optimize return."""
    errors, calls = [], []
    session = {'ok': True, 'page': {'title': 'Pareto Explorer', 'subtitle': 'PBv9 Pareto Explorer', 'stages': [], 'deep_tabs': []},
               'result': None, 'result_path': '', 'optimize_version': 'v9', 'result_valid': False, 'load': None,
               'defaults': None, 'messages': [], 'actions': {}}

    def serve(route):
        """Serve local assets and isolated Explorer API fixtures only."""
        path = urlsplit(route.request.url).path
        calls.append(route.request.url)
        if path == '/api/pareto-explorer/main_page':
            route.fulfill(content_type='text/html', body=_explorer_html(result_path=''))
        elif path.startswith('/app/'):
            asset = (ROOT / 'frontend' / path.removeprefix('/app/')).resolve()
            assert asset.is_relative_to(ROOT / 'frontend')
            content_type = 'text/javascript' if asset.suffix == '.js' else 'text/css' if asset.suffix == '.css' else 'text/html'
            route.fulfill(status=200 if asset.is_file() else 404, body=asset.read_bytes() if asset.is_file() else b'',
                          content_type=content_type)
        elif path == '/api/pareto-explorer/session':
            assert parse_qs(urlsplit(route.request.url).query)['optimize_version'] == ['v9']
            route.fulfill(json=session)
        elif path.startswith('/api/optimize-v9/main_page'):
            route.fulfill(content_type='text/html', body='<title>PB9 Optimize</title>')
        else:
            route.fulfill(json={})

    with sync_browser() as runner:
        browser = runner.chromium.launch(headless=True)
        try:
            page = browser.new_page(viewport={'width': 1280, 'height': 900})
            page.on('pageerror', lambda error: errors.append(str(error)))
            page.route('**/*', serve)
            page.goto('http://explorer.test/api/pareto-explorer/main_page?optimize_version=v9')
            page.wait_for_function("window.PBGUI_NAV_CONFIG && document.querySelector('#topnav .nav-item.current')")
            assert page.evaluate('window.PBGUI_NAV_CONFIG.current') == 'v9_pareto_explorer'
            assert page.locator('.nav-group-btn.active').inner_text().startswith('PBv9')
            assert 'Pareto Explorer' in page.locator('#topnav .nav-item.current').inner_text()
            assert not page.locator('#btn-open-strategy-explorer').is_visible()
            assert not page.locator('#btn-pin-strategy-baseline').is_visible()
            page.evaluate("document.getElementById('btn-back-optimize').click()")
            page.wait_for_url('**/api/optimize-v9/main_page?panel=results#results')
            assert not errors
        finally:
            browser.close()


def test_v9_explorer_backtest_handoff_keeps_native_sections():
    """Queued PB9 candidates keep config_version, logging and monitor for native validation."""
    source = (ROOT / 'frontend/v7_pareto_explorer.html').read_text()
    sections = source[source.index('  function extractConfigSections(raw)'):source.index('  function suggestDraftName(config)')]
    handlers = source[source.index('  function runSelectedConfigBacktest()'):source.index('  function openSelectedStrategyExplorer()')]
    html = '''<!doctype html><button id="btn-run-backtest">Queue Backtest</button>
    <button id="btn-validate-config">Queue Validation</button>
    <select id="explorer-validation-mode"><option value="full_timerange">Full</option></select>
    <div data-backtest-queue-status></div><div data-backtest-autostart></div><script>
    window.OPTIMIZE_VERSION='v9';
    const el=id=>document.getElementById(id);
    window.messages=[];
    const pushMessage=(type,message)=>{window.messages.push(message);};
    window.state={selectedConfigIndex:4,selectedDetail:{config_index:4,native_config_error:'',full_config:{
      config_version:'v9.0.0',logging:{level:1},monitor:{enabled:false},bot:{global:{}},
      backtest:{start_date:'2025-01-01',end_date:'2025-02-01'}},validation_holdouts:[]}};
    </script><script src="/app/js/backtest_queue_actions.js"></script>
    <script src="/app/js/pareto_validation.js"></script><script>''' + sections + handlers + '''
    el('btn-validate-config').addEventListener('click',validateSelectedConfig);
    el('btn-run-backtest').addEventListener('click',runSelectedConfigBacktest);
    </script>'''
    posts = []

    def route_request(route):
        """Record PB9 queue submissions without any production API."""
        path = urlsplit(route.request.url).path
        if path.startswith('/app/js/'):
            route.fulfill(body=(ROOT / 'frontend/js' / Path(path).name).read_text(), content_type='text/javascript')
        elif route.request.method == 'POST':
            posts.append((path, route.request.post_data_json))
            route.fulfill(json={'filenames': ['x.json'], 'filename': 'x.json', 'items': []})
        elif path.startswith('/api/'):
            route.fulfill(json={'autostart': False} if path.endswith('/settings') else {'items': []})
        else:
            route.fulfill(body=html, content_type='text/html')

    with sync_browser() as runner:
        browser = runner.chromium.launch(headless=True)
        try:
            page = browser.new_page()
            page.route('**/*', route_request)
            page.goto('http://queue.test/')
            status = '[data-backtest-queue-status]'
            page.locator('#btn-run-backtest').click()
            page.wait_for_function(f"document.querySelector('{status}').textContent.includes('jobs added')")
            page.locator('#btn-validate-config').click()
            page.wait_for_function(f"document.querySelector('{status}').textContent.includes('pareto_config_4_full_timerange')")
            assert page.evaluate('window.messages') == []
            assert len(posts) == 2
            assert posts and all(path.startswith('/api/backtest-v9/') for path, _ in posts)
            configs = [item['config'] for _, body in posts for item in (body.get('items') or [body])]
            assert configs and all(config['config_version'] == 'v9.0.0' and config['logging'] == {'level': 1}
                                   and config['monitor'] == {'enabled': False} for config in configs)
            page.evaluate("state.selectedDetail.native_config_error='result is not a PB9 record'")
            page.locator('#btn-run-backtest').click()
            page.wait_for_function("window.messages.some(message => message.includes('result is not a PB9 record'))")
        finally:
            browser.close()


def test_native_explorer_renders_real_reductions_and_switches_views(tmp_path):
    """The complete shared page charts native max metrics and hides unsupported controls."""
    from ParetoDataLoader import ParetoDataLoader
    from api import pareto_explorer as explorer

    loader = ParetoDataLoader(str(tmp_path))
    loader.optimize_version = 'v9'
    growth = 'cagr_fraction_strategy_equity_ew'
    ulcer = 'ulcer_index_fraction_strategy_equity'
    recovery = 'peak_recovery_max_days_strategy_equity'
    for index in range(3):
        config = loader._parse_config(index, {
            'config_version': 'v9.0.0',
            'optimize': {'scoring': [{'metric': growth, 'goal': 'max'},
                                     {'metric': ulcer, 'goal': 'min'},
                                     {'metric': recovery, 'goal': 'min'}]},
            'metrics': {'stats': {growth: {'mean': 0.3 + index / 10},
                                  ulcer: {'max': 0.01 + index / 100},
                                  recovery: {'max': 10 + index}}},
        })
        config.is_pareto = True
        loader.configs.append(config)
    errors = []
    result = {'name': 'Native PB9 fixture', 'pareto_count': 3, 'has_all_results': False}
    session = {'ok': True, 'result_path': '/pb9/optimize_results/run', 'result_valid': True,
               'result': result, 'load': {'summary': {'total_configs': 3, 'pareto_configs': 3}},
               'defaults': {}, 'messages': []}
    detail = {'config_index': 0, 'style': loader.compute_trading_style(loader.configs[0]),
              'risk_profile': loader.compute_risk_profile_score(loader.configs[0]),
              'all_metrics': [{'name': key, 'value': value}
                              for key, value in loader.configs[0].suite_metrics.items()],
              'robustness': None}

    def serve(route):
        """Use production HTML, assets and chart builder with isolated native result fixtures."""
        path = urlsplit(route.request.url).path
        if path == '/api/pareto-explorer/main_page':
            route.fulfill(content_type='text/html', body=_explorer_html())
        elif path == '/static/plotly.min.js':
            import plotly
            route.fulfill(content_type='text/javascript',
                          body=(Path(plotly.__file__).parent / 'package_data/plotly.min.js').read_bytes())
        elif path.startswith('/app/'):
            asset = (ROOT / 'frontend' / path.removeprefix('/app/')).resolve()
            assert asset.is_relative_to(ROOT / 'frontend')
            route.fulfill(status=200 if asset.is_file() else 404,
                          body=asset.read_bytes() if asset.is_file() else b'',
                          content_type='text/css' if asset.suffix == '.css' else 'text/javascript')
        elif path.endswith('/session'):
            route.fulfill(json=session)
        elif path.endswith('/command-center'):
            route.fulfill(json={'champions': [{'config_index': 0}], 'insights': []})
        elif path.endswith('/config-detail'):
            index = route.request.post_data_json.get('config_index', 0)
            route.fulfill(json={'detail': {**detail, 'config_index': index}})
        elif path.endswith('/playground'):
            request = route.request.post_data_json
            route.fulfill(json=explorer._build_playground_payload(
                loader, all_results_loaded=False, perf_weight=80, risk_weight=60, robust_weight=70,
                show_all=False, use_weighted=True, preview_use_weighted=True, preview_show_all=False,
                selected_config_index=0, use_btc=False,
                viz_type=request.get('viz_type', '2D Scatter'),
                quick_view=request.get('quick_view', 'Efficiency'), color_metric_input='None',
                custom_x_metric=request.get('custom_x_metric', ''),
                custom_y_metric=request.get('custom_y_metric', ''),
                custom_z_metric=request.get('custom_z_metric', ''),
            ))
        else:
            route.fulfill(json={})

    with sync_browser() as runner:
        browser = runner.chromium.launch(headless=True)
        try:
            page = browser.new_page(viewport={'width': 1440, 'height': 900})
            page.on('pageerror', lambda error: errors.append(str(error)))
            page.route('**/*', serve)
            page.goto('http://explorer.test/api/pareto-explorer/main_page?optimize_version=v9')
            page.get_by_role('button', name='🎯 Explorer', exact=True).click()
            page.wait_for_function("document.getElementById('playground-chart').data?.[0]?.x?.length === 3")
            assert 'Load a result' not in page.locator('#playground-chart').inner_text()
            assert page.locator('#playground-quick-view').input_value() == 'Growth vs Ulcer'
            assert page.evaluate("document.getElementById('playground-chart').data[0].y") == [0.01, 0.02, 0.03]
            assert not page.locator('#playground-use-btc-wrap').is_visible()
            assert not page.locator('#playground-use-weighted-wrap').is_visible()
            assert page.evaluate("document.getElementById('preview-use-weighted').closest('.check-row').style.display") == 'none'
            assert not page.locator('#playground-robust-weight').is_enabled()
            assert 'Activity classification unavailable' in page.locator('#detail-style-panel').inner_text()
            assert '10' in page.locator('#detail-risk-profile').inner_text()
            assert page.locator('#btn-deep-correlations-refresh').count() == 0
            page.locator('#playground-quick-view').select_option('Growth vs Recovery')
            page.wait_for_function("document.getElementById('playground-chart').data?.[0]?.y?.[0] === 10")
            page.locator('#playground-viz-type').select_option('3D Scatter')
            page.wait_for_function("document.getElementById('playground-chart').data?.[0]?.type === 'scatter3d'")
            assert len(page.evaluate("document.getElementById('playground-chart').data[0].z")) == 3
            page.locator('#playground-quick-view').select_option('Custom...')
            page.locator('#playground-custom-x-metric').select_option(ulcer)
            page.wait_for_function("document.getElementById('playground-chart').data?.[0]?.x?.[0] === 0.01")
            page.wait_for_function("!document.getElementById('detail-title').textContent.startsWith('Loading')")
            selected_before_reload = parse_qs(urlsplit(page.url).query)['selected_config']
            page.reload()
            page.wait_for_function("document.getElementById('playground-chart').data?.[0]?.x?.[0] === 0.01")
            assert page.locator('#playground-viz-type').input_value() == '3D Scatter'
            assert page.locator('#playground-quick-view').input_value() == 'Custom...'
            assert page.locator('#playground-custom-x-metric').input_value() == ulcer
            assert parse_qs(urlsplit(page.url).query)['stage'] == ['pareto_playground']
            assert parse_qs(urlsplit(page.url).query)['selected_config'] == selected_before_reload
            assert not errors
        finally:
            browser.close()
