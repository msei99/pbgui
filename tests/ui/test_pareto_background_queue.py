"""Real Explorer action handlers queue frozen selections while HTTP is pending."""

from pathlib import Path
from urllib.parse import urlsplit

import pytest


def test_explorer_accepts_next_validation_while_first_batch_is_pending():
    """Changing the selected result and mode cannot alter an earlier batch."""
    playwright = pytest.importorskip('playwright.sync_api')
    root = Path(__file__).resolve().parents[2]
    source = (root / 'frontend/v7_pareto_explorer.html').read_text()
    handlers = source[source.index('  function runSelectedConfigBacktest()'):source.index('  function openSelectedStrategyExplorer()')]
    html = '''<!doctype html><button id="btn-run-backtest">Queue Backtest</button>
    <button id="btn-validate-config">Queue Validation</button>
    <select id="explorer-validation-mode"><option value="holdout_and_full_timerange">Both</option>
    <option value="full_timerange">Full</option></select>
    <button data-backtest-open-queue>Open Queue</button><div data-backtest-queue-status></div>
    <div data-backtest-autostart></div><script>
    window.OPTIMIZE_VERSION='v8';
    const el=id=>document.getElementById(id);
    const pushMessage=(type,message)=>{throw new Error(message);};
    const extractConfigSections=config=>config;
    window.state={selectedConfigIndex:1,selectedDetail:{config_index:1,full_config:{
      bot:{value:1},backtest:{start_date:'2020-01-01',end_date:'2021-01-01'}},
      validation_holdouts:[{label:'holdout',start_date:'2020-06-01',end_date:'2021-01-01'}]}};
    </script><script src="/app/js/backtest_queue_actions.js"></script>
    <script src="/app/js/pareto_validation.js"></script><script>''' + handlers + '''
    el('btn-validate-config').addEventListener('click',validateSelectedConfig);
    el('btn-run-backtest').addEventListener('click',runSelectedConfigBacktest);
    </script>'''
    posts, held = [], []

    def route_request(route):
        """Serve local assets and hold only the first isolated queue POST."""
        path = urlsplit(route.request.url).path
        if path.startswith('/app/js/'):
            route.fulfill(body=(root / 'frontend/js' / Path(path).name).read_text(), content_type='text/javascript')
        elif route.request.method == 'POST':
            posts.append(route.request.post_data_json)
            assert path.endswith('/queue/batches')
            if len(posts) == 1:
                held.append(route)
            else:
                route.fulfill(json={'batch_id': 'second', 'status': 'queued', 'total': len(posts[-1]['items']), 'confirmed': 0})
        elif path.startswith('/api/'):
            route.fulfill(json={'autostart': False} if path.endswith('/settings') else {'batches': []} if path.endswith('/batches') else {'items': []})
        else:
            route.fulfill(body=html, content_type='text/html')

    with playwright.sync_playwright() as runner:
        browser = runner.chromium.launch(headless=True)
        try:
            page = browser.new_page()
            page.route('**/*', route_request)
            page.goto('http://queue.test/')
            button = page.locator('#btn-validate-config')
            button.click()
            page.wait_for_function("document.querySelector('[data-backtest-queue-status]').textContent.includes('Preparing 2 jobs')")
            assert button.is_enabled()
            page.evaluate("state.selectedConfigIndex=2; state.selectedDetail.config_index=2; state.selectedDetail.full_config.bot.value=2")
            page.locator('#explorer-validation-mode').select_option('full_timerange')
            button.click()
            page.wait_for_function("document.querySelector('[data-backtest-queue-status]').textContent.includes('1 more batch')")
            assert len(posts) == 1
            held[0].fulfill(json={'batch_id': 'first', 'status': 'queued', 'total': 2, 'confirmed': 0})
            page.wait_for_function("document.querySelector('[data-backtest-queue-status]').textContent.includes('jobs queued by PBGui server') && !document.querySelector('[data-backtest-queue-status]').textContent.includes('waiting')")
            assert len(posts) == 2
            assert [len(post['items']) for post in posts] == [2, 1]
            queued = posts[0]['items'] + posts[1]['items']
            assert [post['name'] for post in queued] == ['pareto_config_1_holdout', 'pareto_config_1_full_timerange', 'pareto_config_2_full_timerange']
            assert [post['config']['bot']['value'] for post in queued] == [1, 1, 2]
            assert queued[0]['config']['pbgui']['backtest_result_group']['id'] == queued[1]['config']['pbgui']['backtest_result_group']['id']
            assert queued[1]['config']['pbgui']['backtest_result_group']['id'] != queued[2]['config']['pbgui']['backtest_result_group']['id']
            assert page.url == 'http://queue.test/'
        finally:
            browser.close()


def test_full_scan_unavailable_until_binary_is_present():
    """Missing cloud binaries cannot trigger scans and availability follows result metadata."""
    playwright = pytest.importorskip('playwright.sync_api')
    root = Path(__file__).resolve().parents[2]
    source = (root / 'frontend/v7_pareto_explorer.html').read_text()
    handlers = source[source.index('  function syncAllResultsAvailability()'):source.index('  function loadParetoOnlyFromSidebar()')]
    with playwright.sync_playwright() as runner:
        browser = runner.chromium.launch(headless=True)
        try:
            page = browser.new_page()
            page.set_content('<button id="btn-load-all-results">Scan all_results</button>')
            page.add_script_tag(content='''
                const el = id => document.getElementById(id);
                const state = {session:{result:{has_all_results:false}},fullLoadPending:false};
            ''' + handlers)
            page.evaluate('syncAllResultsAvailability()')
            button = page.locator('#btn-load-all-results')
            assert button.is_disabled()
            assert 'Vast.ai' in button.get_attribute('title')
            assert 'finished' in button.get_attribute('title')
            page.evaluate('loadAllResultsFromSidebar()')
            assert page.evaluate('state.fullLoadPending') is False
            page.evaluate('state.session.result.has_all_results = true; syncAllResultsAvailability()')
            assert button.is_enabled()
            page.evaluate('state.fullLoadPending = true; syncAllResultsAvailability()')
            assert button.is_disabled()
        finally:
            browser.close()
