"""Exercise actual Vast render paths with incomplete and malformed metrics offline."""

import json
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]


def test_invalid_metrics_do_not_abort_vast_renderers():
    """Offers, selected jobs, rental dashboards and observed runs remain usable."""
    playwright = pytest.importorskip('playwright.sync_api')
    source = (ROOT / 'frontend/js/vast.js').read_text()
    optimizer = (ROOT / 'frontend/v7_optimize.html').read_text()
    helpers = optimizer[optimizer.index('function formatBytesCompact('):
                        optimizer.index('\nasync function refreshOptimizeLogStatus(')]
    log_markup = optimizer[optimizer.index('<div id="opt-log-dashboard">'):
                           optimizer.index('<div id="log-viewer-target"></div>')]
    harness = r'''
      window.__vastMetricsTest = {
        renderOffers, renderJob, renderCloudDashboard, renderObservedOptimizer,
        prepare(job, rental) {
          jobRows = [job]; selectedJobId = job.id;
          workers = [rental]; worker = rental;
          window.state.cloudLogId = job.id;
        },
        stop() { disposed = true; clearTimeout(jobTimer); }
      };
    '''
    source = source.rsplit('})();', 1)[0] + harness + '\n})();'
    shell = '''<!doctype html><meta charset="utf-8">
      <div id="vast-queue-host"></div><div id="vast-offer-parking" hidden></div>
      <div id="opted-vast-offers"></div>
      <select id="opted-execution"><option value="local">Local</option></select>
      <div id="opted-vast-worker"></div><div id="opted-vast-validation"></div>
      <button id="optlog-open-results"></button><button id="optlog-open-pareto-explorer"></button>
    ''' + log_markup + '''<div id="log-viewer-target"></div>
      <script>
        window.OPTIMIZE_VERSION='v8'; window.API_BASE='/api/optimize-v8';
        window.state={}; const el=id=>document.getElementById(id);
      </script><script>''' + helpers + '''</script>
      <script src="/app/js/vast.js"></script>'''
    requests = []

    def serve(route):
        """Fulfil every browser request locally; never contact an API or provider."""
        url = route.request.url
        requests.append((route.request.method, url))
        if url == 'http://pbgui.test/':
            route.fulfill(content_type='text/html', body=shell)
        elif url.endswith('/app/js/vast.js'):
            route.fulfill(content_type='text/javascript', body=source)
        elif url.endswith('/api/vast/fragment'):
            route.fulfill(content_type='text/html', body=(ROOT / 'frontend/vast.html').read_text())
        else:
            payload = {'jobs': [], 'workers': [], 'queue': {}, 'configured': False,
                       'hosts': [], 'profiles': [], 'supervision_available': True}
            route.fulfill(content_type='application/json', body=json.dumps(payload))

    with playwright.sync_playwright() as engine:
        browser = engine.chromium.launch(headless=True)
        try:
            page = browser.new_page()
            errors = []
            page.on('pageerror', lambda error: errors.append(str(error)))
            page.route('**/*', serve)
            page.goto('http://pbgui.test/')
            page.wait_for_function('!!window.__vastMetricsTest')
            page.evaluate(r'''() => {
              const test = window.__vastMetricsTest;
              test.stop();
              const values = [null, undefined, NaN, Infinity, -Infinity, 'abc',
                JSON.parse('{"toString":null,"valueOf":null}'), 0, 12.5, '12.5'];
              for (const value of values) {
                const rental = {id:'b'.repeat(32), rental_state:'active', status:'running',
                  offer:{id:1,machine_id:101,gpu_name:'Test GPU',cuda_max_good:13,
                    vram_gb:value,price_hour_usd:value},transfer_reserve_usd:value};
                const job = {id:'a'.repeat(32),config_name:'Metric regression',status:'running',
                  iterations:512,exact_completed:0,gpu_candidates:0,rental,
                  runtime_metrics:{},throughput:{proxy_per_minute:value,exact_per_minute:value},
                  convergence_config:{},convergence:{},has_log:false};
                test.prepare(job,rental);
                test.renderOffers([rental.offer]);
                test.renderJob();
                test.renderCloudDashboard(job);
                if (!document.querySelector('#offers-body tr')) throw Error('Offers vanished');
                if (!document.getElementById('optlog-rental').textContent.includes('Rental rate'))
                  throw Error('Rental dashboard was not rendered');
                rental.observed_optimizer = {running:true,sampled_at:Date.now()/1000,
                  name:'Observed metrics',exact_completed:0,gpu_candidates:0};
                window.state.cloudLogId = rental.id;
                test.renderObservedOptimizer(rental);
                if (document.getElementById('cloud-job-details').hidden)
                  throw Error('Observed optimizer disappeared');
                if (!document.getElementById('stop-job').disabled)
                  throw Error('Observed optimizer enabled job actions');
              }
            }''')
            assert errors == []
            assert all(method == 'GET' for method, _url in requests)
        finally:
            browser.close()
