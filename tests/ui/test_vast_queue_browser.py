"""Browser integration of the cloud component using an isolated mock HTTP server."""
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def test_cloud_setup_editor_selection_and_queue(tmp_path):
    """Keep GPU preferences in Settings and queueing free of rental calls."""
    playwright = pytest.importorskip('playwright.sync_api')
    calls = []
    jobs = []
    starts = []
    release_start = threading.Event()
    release_stop = threading.Event()
    preferences = {'gpu_name':'','max_price':.5,'min_vram':12,'min_ram':16,'min_cpu':4,'disk_gb':40,'verified_only':True,'hours':1,'budget':1,'idle_seconds':300,'max_rentals':1,'auto_rent':False}
    class Handler(BaseHTTPRequestHandler):
        """Serve repository assets and fake authenticated cloud API responses."""
        def log_message(self, *args):
            """Suppress HTTP logs, including credential request metadata."""

        def do_GET(self):
            """Render the component without loading production application state."""
            route = self.path.split('?')[0]
            content_type = 'application/json'
            if route == '/':
                content_type = 'text/html'
                data = '''<html><head><meta charset="UTF-8"><link rel="stylesheet" href="/app/css/vast.css"></head><body>
                <section id="settings"><div id="vast-queue-host"></div><div id="vast-offer-parking" hidden></div></section>
                <section id="queue"><span id="queue-selection-summary"></span><table><tbody id="queue-tbody"></tbody></table></section><div id="log-panel" hidden><button id="mock-log-close">Close</button><div id="vast-jobs-host" class="vast-component"></div></div>
                <section id="editor"><select id="opted-execution"><option value="local">Local</option><option value="vast">Vast</option></select>
                <p id="opted-vast-worker"></p><div id="opted-vast-validation" hidden></div>
                <button id="btn-cloud-save-queue">Save and Queue</button><button id="btn-editor-save">Save</button>
                <div id="opted-scoring-panel"><table><tbody><tr><td>gain_strategy_eq</td></tr></tbody></table></div>
                <div id="opted-vast-offers" class="vast-component" hidden></div></section>
                <script>window.OPTIMIZE_VERSION='v8';window.API_BASE='/api/optimize-v8';window.WS_BASE='';window.state={editorLastConfig:{}};window.openLogPanel=(id,name,options)=>{PBGuiVast.closeLog();state.cloudLogId=id;document.getElementById('log-panel').hidden=false;};document.getElementById('mock-log-close').onclick=()=>{PBGuiVast.closeLog();document.getElementById('log-panel').hidden=true;};</script>
                <script src="/app/js/vast.js"></script></body></html>'''
            elif route == '/api/vast/fragment':
                content_type = 'text/html'; data = (ROOT/'frontend/vast.html').read_text()
            elif route.startswith('/app/'):
                content_type = 'text/javascript' if route.endswith('.js') else 'text/css'
                data = (ROOT/'frontend'/route.removeprefix('/app/')).read_text()
            elif route == '/api/vast/gpu-preferences':
                data = json.dumps(preferences)
            elif route == '/api/vast/settings':
                data = json.dumps({'configured':False})
            elif route == '/api/vast/jobs':
                data = json.dumps({'jobs':jobs,'worker':None,'queue':{},'supervision_available':True})
            elif route == '/api/vast/offers':
                data = json.dumps({'offers':[{'id':1,'gpu_name':'RTX 3090','disk_gb':40,'price_hour_usd':.15,
                    'cpu_name':'Xeon test','gpu_mem_bw_gbps':805,'pci_gen':3,'gpu_lanes':8,'pcie_bw_gbps':5.5,'disk_name':'NVMe test','disk_bw_mbps':1810,'duration_seconds':172800,
                    'cpu_cores':8,'ram_gb':32,'vram_gb':24,'cuda_max_good':13,'location':'test','verified':True}]})
            else:
                self.send_error(404); return
            self.send_response(200); self.send_header('Content-Type', content_type); self.end_headers(); self.wfile.write(data.encode())

        def do_DELETE(self):
            """Remove only an in-memory mock job."""
            calls.append(self.path)
            jobs[:] = [job for job in jobs if self.path != '/api/vast/jobs/' + job['id']]
            self.send_response(200); self.send_header('Content-Type', 'application/json'); self.end_headers()
            self.wfile.write(b'{"deleted":true}')

        def do_PATCH(self):
            """Merge the requested settings group through the same isolated handler."""
            self.do_POST()

        def do_POST(self):
            """Record mock writes and never contact a cloud provider."""
            payload = json.loads(self.rfile.read(int(self.headers.get('Content-Length',0))) or '{}')
            calls.append(self.path)
            if self.path == '/api/vast/validate-config':
                from vast_config_validation import METRICS, validate_cloud_config
                errors = validate_cloud_config(payload['config'])
                data = {'valid':not errors,'errors':errors,'metrics':sorted(METRICS),'revision':'ee2b7d4','image':'pinned-test-image'}
            elif self.path == '/api/vast/gpu-preferences':
                preferences.update(payload)
                data = dict(preferences)
            elif self.path == '/api/vast/credentials':
                data = {'configured':True}
            elif self.path == '/api/vast/queue/start':
                starts.append(payload)
                release_start.wait(timeout=5)
                data = {'id':'mock-worker'}
            elif self.path.endswith('/stop'):
                release_stop.wait(timeout=5)
                jobs[0]['status'] = 'running'
                jobs[0]['stop_requested'] = True
                data = dict(jobs[0])
            elif self.path.endswith('/requeue'):
                source = jobs[0]
                data = dict(source, id='b'*32, status='ready')
                jobs[:] = [data]
            elif self.path == '/api/vast/jobs/prepare':
                data = {'id':('b' if jobs else 'a')*32,'config_name':payload['config_name'],'status':'ready','rental_state':'none',
                        'iterations':payload['iterations'],'workers':payload['workers'],'input_bytes':100,'has_log':False,'can_delete':True}
                jobs.append(data)
            elif self.path == '/api/vast/jobs/delete':
                removed = set(payload['ids'])
                jobs[:] = [job for job in jobs if job['id'] not in removed]
                data = {'deleted':True, 'purged_ids':sorted(removed)}
            else:
                self.send_error(404); return
            self.send_response(200); self.send_header('Content-Type','application/json'); self.end_headers()
            self.wfile.write(json.dumps(data).encode())
    server = ThreadingHTTPServer(('127.0.0.1',0),Handler)
    thread = threading.Thread(target=server.serve_forever,daemon=True); thread.start()
    try:
        with playwright.sync_playwright() as engine:
            browser = engine.chromium.launch(headless=True)
            try:
                page = browser.new_page()
                errors=[]; page.on('pageerror', lambda error: errors.append(str(error)))
                page.goto('http://127.0.0.1:'+str(server.server_port))
                page.wait_for_function('!!window.PBGuiVast')
                assert page.locator('#settings #vast-setup').count() == 1
                assert page.locator('#registry-token').count() == 0
                assert page.locator('#job-config').count() == 0
                page.locator('[data-vast-view=account]').click()
                page.locator('#api-key').fill('mock-test-key')
                page.locator('#save-key').click()
                page.wait_for_function("document.getElementById('api-key').value === ''")
                page.evaluate("""() => {
                    state.editorLastConfig = {live:{strategy_kind:'ema_anchor',approved_coins:{long:['BTC'],short:[]}},
                        bot:{long:{},short:{}},backtest:{exchanges:['binance']},optimize:{iters:512,n_cpus:4,scoring:[{metric:'gain_strategy_eq',goal:'max'}],limits:[]}};
                    PBGuiVast.openEditor({pbgui:{execution:'vast'}});
                }""")
                page.wait_for_function("document.getElementById('opted-vast-validation').textContent.includes('Unsupported cloud metrics')")
                assert 'Alternatives' not in page.locator('#opted-vast-validation').inner_text()
                assert 'Saving remains available' not in page.locator('#opted-vast-validation').inner_text()
                assert page.locator('#opted-vast-validation button', has_text='Use ADG').get_attribute('data-tip') is None
                assert 'changes the objective' in page.locator('.cloud-issue-title').get_attribute('data-tip')
                assert 'Execution → Local' in page.locator('.cloud-issue-title').get_attribute('data-tip')
                page.evaluate("window.originalIssue = document.querySelector('.cloud-issue-title'); window.originalHeight = document.getElementById('opted-vast-validation').getBoundingClientRect().height; PBGuiVast.scheduleValidation()")
                assert page.evaluate("originalIssue === document.querySelector('.cloud-issue-title')")
                assert page.evaluate("originalHeight === document.getElementById('opted-vast-validation').getBoundingClientRect().height")
                page.wait_for_function("document.getElementById('opted-vast-validation').getAttribute('aria-busy') === 'false'")
                assert page.evaluate("originalIssue === document.querySelector('.cloud-issue-title')")
                assert page.evaluate("originalHeight === document.getElementById('opted-vast-validation').getBoundingClientRect().height")
                assert page.locator('#btn-cloud-save-queue').is_enabled()
                assert page.locator('#btn-editor-save').is_enabled()
                assert page.locator('#opted-scoring-panel .cloud-invalid').count() == 1
                assert not page.evaluate("PBGuiVast.metricAllowed('gain_strategy_eq')")
                assert page.evaluate("PBGuiVast.metricAllowed('adg_strategy_eq')")
                assert page.evaluate("state.editorLastConfig.optimize.scoring[0].metric") == 'gain_strategy_eq'
                page.locator('#opted-vast-validation button', has_text='Use Local').click()
                assert page.locator('#opted-execution').input_value() == 'local'
                assert page.locator('#opted-vast-validation').is_hidden()
                assert page.evaluate("state.editorLastConfig.optimize.scoring[0].metric") == 'gain_strategy_eq'
                page.evaluate("""() => {
                    window.getScoringEntries = () => structuredClone(state.editorLastConfig.optimize.scoring);
                    window.setScoringEntries = entries => { state.editorLastConfig.optimize.scoring = entries; };
                    document.getElementById('opted-execution').value = 'vast';
                    PBGuiVast.updateEditor();
                }""")
                page.locator('#opted-vast-validation button', has_text='Use ADG').click()
                assert page.evaluate("state.editorLastConfig.optimize.scoring[0].metric") == 'adg_strategy_eq'
                assert page.evaluate("state.editorLastConfig.optimize.scoring[0].goal") == 'max'
                assert not any('/configs/' in call or '/jobs/prepare' in call for call in calls)
                page.wait_for_function("document.getElementById('opted-vast-validation').hidden && !document.getElementById('btn-cloud-save-queue').disabled")
                page.evaluate("""() => {
                    state.editorLastConfig.optimize.scoring = [{metric:'adg_pnl',goal:'max'}];
                    state.editorLastConfig.optimize.limits = [{metric:'fills_count',penalize_if:'greater_than',value:1}];
                    window.getLimitEntries = () => structuredClone(state.editorLastConfig.optimize.limits);
                    window.setLimitEntries = entries => {state.editorLastConfig.optimize.limits = entries;};
                    PBGuiVast.scheduleValidation();
                }""")
                page.locator('select[aria-label="GPU metric"]').wait_for()
                assert page.locator('#opted-vast-validation button', has_text='Use Local').count() == 1
                assert page.locator('#opted-vast-validation button[data-tip]').count() == 0
                page.locator('select[aria-label="GPU metric"]').select_option('sortino_ratio_strategy_eq')
                page.locator('button', has_text='Replace metric').click()
                page.wait_for_function("state.editorLastConfig.optimize.scoring[0].metric === 'sortino_ratio_strategy_eq'")
                page.wait_for_function("!document.querySelector('#opted-vast-validation select')")
                page.locator('button', has_text='Remove limit').click()
                assert page.evaluate('state.editorLastConfig.optimize.limits.length') == 0
                assert page.evaluate('state.editorLastConfig.optimize.scoring[0].goal') == 'max'
                page.wait_for_function("document.getElementById('opted-vast-validation').hidden")
                page.evaluate('PBGuiVast.scheduleValidation()')
                assert page.locator('#opted-vast-validation').is_hidden()
                page.wait_for_function("document.getElementById('opted-vast-validation').getAttribute('aria-busy') === 'false'")
                assert page.locator('#opted-vast-validation').is_hidden()
                assert page.locator('#btn-cloud-save-queue').is_enabled()
                assert page.locator('#opted-scoring-panel .cloud-invalid').count() == 0
                assert page.locator('#editor #vast-offers').count() == 0
                page.locator('[data-vast-view=offers]').click()
                assert page.locator('#settings #vast-offers').is_visible()
                assert not page.locator('#show-incompatible').is_checked()
                page.locator('#gpu-model').fill('3090')
                page.locator('#find-offers').click()
                details = page.locator('#offers-body button').first
                details.click()
                assert page.locator('.offer-details').is_visible()
                assert 'PCIe 3.0 ×8 · 5.5 GB/s' in page.locator('.offer-details').inner_text()
                assert 'NVMe test' in page.locator('.offer-details').inner_text()
                assert page.locator('#gpu-model').input_value() == '3090'
                assert 'Xeon test' in page.locator('#offers-body').inner_text()
                assert 'Compatible' in page.locator('#offers-body').inner_text()
                assert '2d 0h' in page.locator('#offers-body').inner_text()
                details.click()
                assert page.locator('.offer-details').is_hidden()
                page.locator('#offers-body tr[data-offer]').click()
                assert page.locator('#gpu-model').input_value() == '3090'
                assert page.locator('#offers-body tr[data-offer]').get_attribute('aria-selected') == 'true'
                assert 'RTX 3090' in page.locator('#selection').inner_text()
                page.locator('#save-gpu-preferences').click()
                page.wait_for_function("document.getElementById('saved-requirements').textContent.includes('3090')")
                page.locator('[data-vast-view=rental]').click()
                page.locator('#convergence-enabled').select_option('true')
                page.locator('#convergence-patience').fill('768')
                page.locator('#save-rental-preferences').click()
                page.wait_for_function("!document.getElementById('save-rental-preferences').disabled")
                page.wait_for_function("document.getElementById('saved-requirements').textContent.includes('3090')")
                assert preferences['convergence_enabled'] is True
                assert preferences['convergence_patience'] == 768
                assert preferences['gpu_name'] == '3090'
                assert 'offer_id' not in preferences
                page.evaluate("window.PBGuiDialogs = {confirm: async options => { window.autoRentConfirmation = options; return true; }}")
                page.locator('#max-rentals').fill('3')
                page.locator('#auto-rent').select_option('true')
                page.locator('#save-rental-preferences').click()
                page.wait_for_function("!document.getElementById('save-rental-preferences').disabled")
                assert preferences['auto_rent'] is True
                assert preferences['max_rentals'] == 3
                assert 'up to $3.00 simultaneously' in page.evaluate('autoRentConfirmation.message')
                page.locator('#auto-rent').select_option('false')
                page.locator('#max-rentals').fill('1')
                page.locator('#save-rental-preferences').click()
                page.wait_for_function("!document.getElementById('save-rental-preferences').disabled")
                assert preferences['auto_rent'] is False
                page.evaluate("PBGuiVast.queue('queued-config',{optimize:{iters:512,n_cpus:4}})")
                assert page.locator('#settings #vast-jobs').count() == 0
                assert page.locator('#cloud-job-details').is_hidden()
                page.evaluate("""() => {
                    window.el = id => document.getElementById(id);
                    Object.assign(window.state, {queue:[{filename:'local-job',name:'Local config',
                        exchange:'binance',status:'queued',created:'2026-09-13'}],selectedQueue:new Set()});
                    window.optimizeEditorAdapter = {isV8:true};
                    window.renderQueueTableHead = () => {};
                    window.renderEmpty = () => {};
                    window.sortQueueItems = items => items;
                    window.escapeHtml = value => String(value || '').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('"','&quot;');
                    window.formatExchange = value => value;
                    window.formatIso = value => value;
                    window.updateQueueStatus = window.updateMetaCounts = () => {};
                }""")
                source = (ROOT/'frontend/v7_optimize.html').read_text()
                start = source.index('function renderQueue() {')
                end = source.index('\n}\n', start) + 2
                page.add_script_tag(content=source[start:end])
                start = source.index('function pruneSelectionSet(')
                end = source.index('\nfunction selectAllConfigs()', start)
                page.add_script_tag(content=source[start:end])
                start = source.index('function updateQueueSelectionUi() {')
                end = source.index('\nfunction updateQueueStatus()', start)
                page.add_script_tag(content=source[start:end])
                start = source.index('async function deleteSelectedQueue() {')
                end = source.index('\nasync function deleteQueueItem(', start)
                page.add_script_tag(content=source[start:end])
                page.evaluate('window.renderQueueMaybeDeferred = () => renderQueue()')
                page.evaluate('renderQueue()')
                assert page.locator('#queue-tbody tr').count() == 2
                assert page.locator('#queue-tbody tr').first.locator('td').nth(5).inner_text() == 'Local'
                row = page.locator('#queue-tbody tr').nth(1)
                cloud_key = 'vast:' + 'a'*32
                assert row.get_attribute('data-filename') == cloud_key
                assert row.locator('td').nth(1).inner_text() == 'queued-config'
                assert row.locator('td').nth(3).inner_text() == 'queued'
                assert row.locator('td').nth(5).inner_text() == 'Vast.ai'
                assert row.locator('button[title="Open log"]').count() == 0
                assert row.locator('button[title="Stop"]').count() == 0
                assert row.locator('button[title="Delete queue item"]').count() == 1
                page.evaluate("initRowSelection('queue-tbody', 'selectedQueue', 'data-filename', null, null)")
                page.locator('#queue-tbody tr').first.locator('td').nth(1).click()
                row.locator('td').nth(1).click()
                assert set(page.evaluate('Array.from(state.selectedQueue)')) == {'local-job', cloud_key}
                page.evaluate('renderQueue()')
                assert page.locator('#queue-tbody tr.selected').count() == 2
                page.evaluate("""() => {
                    const item = PBGuiVast.queueItems()[0];
                    item.cloudJob.status = 'failed';
                    item.cloudJob.error = 'Rental failed (HTTP 400)';
                    renderQueue();
                }""")
                row.locator('button[title="Open log"]').click()
                assert page.locator('#cloud-job-details').is_visible()
                assert page.locator('#requeue-job').is_visible()
                assert page.locator('#job-state').count() == 0
                page.locator('#mock-log-close').click()
                page.evaluate("""() => {
                    const item = PBGuiVast.queueItems()[0];
                    item.cloudJob.status = 'ready';
                    delete item.cloudJob.error;
                    item.cloudJob.has_log = true;
                    window.LogViewerPanel = class {open() {} close() {}};
                    renderQueue();
                }""")
                row.locator('button[title="Open log"]').click()
                assert page.locator('#cloud-job-details').is_visible()
                assert page.locator('#start-job').is_visible()
                page.locator('#mock-log-close').click()
                assert page.locator('#cloud-job-details').is_hidden()
                assert page.locator('#settings #job-hours').is_visible()
                assert page.locator('#queue #job-hours').count() == 0
                assert page.evaluate('window.state.cloudQueueCount') == 1
                page.evaluate('PBGuiVast.closeEditor()')
                page.locator('[data-vast-view=offers]').click()
                assert page.locator('#settings #vast-offers').is_visible()
                page.locator('[data-vast-view=account]').click()
                page.locator('#api-key').fill('unsaved-test-key')
                page.evaluate('PBGuiVast.hideSettings()')
                assert page.locator('#api-key').input_value() == ''
                assert not any('/start' in call for call in calls)
                page.evaluate("window.PBGuiDialogs = {confirm: async () => {throw new Error('Unexpected confirmation');}}")
                page.locator('[data-vast-view=offers]').click()
                page.locator('#gpu-model').fill('unsaved-other-type')
                row.locator('button[title="Start"]').click()
                assert row.locator('td').nth(3).inner_text() == 'Starting…'
                assert row.locator('button[title="Start"]').is_disabled()
                page.evaluate('renderQueue()')
                assert row.locator('td').nth(3).inner_text() == 'Starting…'
                assert row.locator('button[title="Start"]').is_disabled()
                release_start.set()
                page.wait_for_function("document.getElementById('message').textContent === ''")
                assert len(starts) == 1
                assert starts[0] == {'use_saved_settings':True,'accept_rental_and_cleanup':True}
                assert page.locator('#log-panel').is_hidden()
                assert page.locator('#queue #vast-jobs').count() == 0
                assert page.locator('#cloud-rental-controls').count() == 0
                assert 'offer_id' not in starts[0]
                page.evaluate("""() => {
                    PBGuiVast.queueItems()[0].cloudJob.status = 'failed';
                    renderQueue();
                }""")
                assert row.locator('button[title="Requeue"]').count() == 1
                row.locator('button[title="Open log"]').click()
                assert page.locator('#requeue-job').is_visible()
                page.locator('#requeue-job').click()
                page.wait_for_function("PBGuiVast.queueItems()[0]?.cloudJob.id === 'b'.repeat(32)")
                page.wait_for_function("document.querySelector('#queue-tbody tr:nth-child(2) td:nth-child(4)').textContent === 'queued'")
                assert row.locator('td').nth(3).inner_text() == 'queued'
                assert len(jobs) == 1
                assert len(starts) == 1
                assert row.locator('button[title="Requeue"]').count() == 0
                page.wait_for_function("state.cloudLogId === 'b'.repeat(32)")
                assert page.locator('#log-panel').is_visible()
                assert page.locator('#start-job').is_visible()
                assert page.locator('#start-job').is_enabled()
                assert page.locator('#stop-job').is_disabled()
                page.evaluate("""() => {
                    PBGuiVast.queueItems()[0].cloudJob.status = 'running';
                    PBGuiVast.openEditor({});
                }""")
                jobs[0]['status'] = 'running'
                assert page.locator('#stop-job').is_enabled()
                page.evaluate('renderQueue()')
                row.locator('button[title="Open log"]').click()
                page.evaluate("""() => {
                    window.stopDecision = false;
                    window.stopConfirmation = null;
                    window.PBGuiDialogs.confirm = async options => {
                        window.stopConfirmation = options;
                        return window.stopDecision;
                    };
                }""")
                page.locator('#stop-job').click()
                page.wait_for_function('window.stopConfirmation !== null')
                assert page.evaluate('stopConfirmation.title') == 'Stop optimizer?'
                assert page.evaluate('stopConfirmation.confirmText') == 'Stop & collect'
                assert not any(call.endswith('/stop') for call in calls)
                assert page.locator('#stop-job').inner_text() == 'Stop & collect'
                page.evaluate('window.stopDecision = true; window.stopConfirmation = null')
                page.locator('#stop-job').click()
                assert page.locator('#stop-job').inner_text() == 'Stopping…'
                assert page.locator('#stop-job').is_disabled()
                assert row.locator('td').nth(3).inner_text() == 'Stopping…'
                release_stop.set()
                page.wait_for_function("document.getElementById('stop-job').textContent === 'Stop requested'")
                assert page.locator('#stop-job').is_disabled()
                assert row.locator('td').nth(3).inner_text() == 'Stop requested'
                page.evaluate('window.PBGuiDialogs.confirm = async () => true')
                page.evaluate("""() => {
                    const cloudKey = PBGuiVast.queueItems()[0].filename;
                    state.selectedQueue = new Set(['local-job', cloudKey]);
                    window.bulkLocalDelete = null;
                    window.apiFetch = async (path, options) => {
                        bulkLocalDelete = {path, body:JSON.parse(options.body)};
                        return {deleted:true};
                    };
                    window.removeQueueItemsFromState = filenames => {
                        const removed = new Set(filenames);
                        state.queue = state.queue.filter(item => !removed.has(item.filename));
                        removed.forEach(filename => state.selectedQueue.delete(filename));
                    };
                    window.toast = () => {};
                }""")
                page.evaluate('deleteSelectedQueue()')
                page.wait_for_function('state.cloudQueueCount === 0')
                assert page.evaluate('bulkLocalDelete') == {
                    'path':'/queue/delete', 'body':{'filenames':['local-job']}
                }
                assert calls.count('/api/vast/jobs/delete') == 1
                assert page.evaluate('state.selectedQueue.size') == 0
                assert '/api/vast/jobs/' + 'a'*32 + '/requeue' in calls
                assert not errors
                page.screenshot(path=str(tmp_path/'vast-queue.png'),full_page=True)
            finally:
                browser.close()
    finally:
        server.shutdown(); server.server_close(); thread.join(timeout=5)


def test_optimizer_settings_main_area():
    """Exercise real page layout and navigation without production API access."""
    import re
    playwright = pytest.importorskip('playwright.sync_api')
    source = (ROOT / 'frontend/v7_optimize.html').read_text()
    markup = re.sub(r'<script\b[^>]*>.*?</script>', '', source, flags=re.S)
    functions = []
    for name in ('setPanel', 'openQueueSettingsModal', 'saveQueueSettingsFromModal', 'openLogPanel', 'closeLogPanel'):
        start = source.index(('async ' if name.startswith('save') else '') + 'function ' + name + '(')
        end = source.index('\n}\n', start) + 2
        functions.append(source[start:end])
    with playwright.sync_playwright() as engine:
        browser = engine.chromium.launch(headless=True)
        try:
            page = browser.new_page(viewport={'width':1440, 'height':1000})
            page.route('**/*', lambda route: route.fulfill(body='', content_type='text/css'))
            page.set_content(markup)
            page.add_style_tag(content=(ROOT/'frontend/css/sidebar.css').read_text())
            page.evaluate('''() => {
                window.el = id => document.getElementById(id);
                window.state = {panel:'queue',navigationSeq:0,settings:{cpu_max:16}};
                window.PANEL_META = {configs:{},queue:{},settings:{},results:{},paretos:{}};
                window.syncQueueSettingsModalFields = () => { el('settings-cpu-value').value = '8'; };
                window.refreshLiveResultsDuringRun = async () => {};
                window.restoreSelectedOptimizeResultWhenReady = async () => {};
                window.handleError = error => { throw error; };
                window.normalizeOptimizePositiveInteger = value => Number(value);
                window.toast = () => {};
                window.stopOptimizeLogStatusPolling = () => { window.localPolling = null; };
                window.startOptimizeLogStatusPolling = filename => { window.localPolling = filename; };
                window.renderOptimizeLogDashboard = () => {};
                window.optimizeEditorAdapter = {queueLogFile: filename => filename + '.log'};
                window.ensureLogViewer = () => { window.state.logViewer = {open(){},close(){},setHost(){},setFile(file){window.selectedLogFile=file;}}; return window.state.logViewer; };
                window.apiFetch = async (path, options) => { window.savedSettings = JSON.parse(options.body); };
            }''')
            page.add_script_tag(content='\n'.join(functions))
            page.evaluate("openQueueSettingsModal()")
            assert page.locator('#main-content #panel-settings').is_visible()
            assert page.locator('#panel-queue').is_hidden()
            assert page.locator('#panel-settings #vast-queue-host').count() == 0
            assert page.locator('#panel-vast #vast-queue-host').count() == 1
            assert page.locator('#panel-queue #vast-queue-host').count() == 0
            assert page.locator('#settings-modal.modal').count() == 0
            assert page.locator('#modal-backdrop').evaluate("node => getComputedStyle(node).pointerEvents") == 'none'
            assert page.locator('#sidebar').is_visible()
            assert page.locator('#settings-cpu-value').input_value() == '8'
            main = page.locator('#main-content').bounding_box()
            settings = page.locator('#panel-settings').bounding_box()
            assert settings['width'] >= main['width'] * .9
            page.evaluate('saveQueueSettingsFromModal()')
            assert page.evaluate('savedSettings.cpu') == 8
            assert page.locator('#panel-settings').is_visible()
            page.evaluate("setPanel('queue')")
            assert page.locator('#panel-settings').is_hidden()
            assert page.locator('#panel-queue').is_visible()
            page.evaluate("openLogPanel('cloud-id', 'Cloud config', {cloud:true,hasLog:false})")
            assert page.locator('#log-panel').is_visible()
            assert page.locator('#log-panel #vast-jobs-host').count() == 1
            assert page.locator('#panel-queue #vast-jobs-host').count() == 0
            assert page.evaluate('state.cloudLogId') == 'cloud-id'
            assert page.evaluate('window.localPolling') is None
            assert page.locator('#log-viewer-target').is_hidden()
            assert page.locator('#log-waiting').is_visible()
            assert page.evaluate('state.logFile') == ''
            assert page.evaluate('window.selectedLogFile') is None
            page.evaluate('closeLogPanel()')
            assert page.locator('#log-panel').is_hidden()
            page.evaluate("openLogPanel('local-id', 'Local config')")
            assert page.evaluate('state.cloudLogId') is None
            assert page.evaluate('window.localPolling') == 'local-id'
            assert page.locator('#log-viewer-target').is_visible()
            page.evaluate("openLogPanel('cloud-id', 'Cloud config', {cloud:true,hasLog:true})")
            assert page.evaluate('window.selectedLogFile') == 'optimizes_v8/vast_cloud-id.log'
            page.evaluate("openLogPanel('new-id', 'New run', {cloud:true})")
            assert page.locator('#log-viewer-target').is_hidden()
            assert page.locator('#log-waiting').is_visible()
            assert page.evaluate('state.logViewer') is None
            page.evaluate("openLogPanel('new-id', 'New run', {cloud:true,hasProviderLog:true})")
            assert page.evaluate('window.selectedLogFile') == 'optimizes_v8/vast_new-id_provider.log'
            assert page.locator('#log-waiting').is_hidden()
            assert page.evaluate('window.localPolling') is None
        finally:
            browser.close()


def test_convergence_dashboard_progress_and_completion():
    """Render persisted detection progress and hide it for jobs without opt-in."""
    import re
    playwright = pytest.importorskip('playwright.sync_api')
    source = (ROOT / 'frontend/js/vast.js').read_text()
    function = source[source.index('  function renderUtilization(job)'):source.index('  function openCloudLog(job)')]
    shell = (ROOT / 'frontend/v7_optimize.html').read_text()
    shell = re.sub(r'<script\b[^>]*>.*?</script>', '', shell, flags=re.S | re.I)
    component = (ROOT / 'frontend/vast.html').read_text()
    with playwright.sync_playwright() as runner:
        browser = runner.chromium.launch(headless=True)
        try:
            page = browser.new_page()
            page.route('**/*', lambda route: route.abort())
            page.set_content(shell + component)
            page.add_script_tag(content='''
                const el = id => document.getElementById(id);
                window.state = {cloudLogId:'test'};
                const jobRows = [], worker = null, billingLease = null, billingSnapshot = null;
                const fmt = (value, digits) => Number(value).toFixed(digits);
                const renderOptimizeLogDashboard = status => { window.lastDashboard = status; }, renderRentalDetails = () => {};
                window.logViewerOpens = 0;
                window.logViewerFiles = 0;
                window.openLogPanel = (id, name, options) => {
                    window.openedObservedLog = {id, name, options};
                    state.cloudLogId = id;
                    state.logFile = options.hasLog ? 'optimizes_v8/vast_' + id + '.log' : '';
                };
                window.ensureLogViewer = () => ({
                    open: () => { window.logViewerOpens += 1; },
                    setHost: () => {},
                    setFile: () => { window.logViewerFiles += 1; }
                });
            ''' + 'const stoppingJobs = new Map();' + source[source.index('  function stopPhase(job)'):source.index('  let startingQueue')] + source[source.index('  function workerActivity(rental)'):source.index('  function renderJob()')] + function)
            page.evaluate('''() => {
                window.job = {id:'test',status:'running',has_log:true,exact_completed:700,
                    convergence_config:{convergence_enabled:true,convergence_patience:512},
                    convergence:{phase:'tracking',last_improvement_exact:512,stalled_exact:188}};
                renderCloudDashboard(job);
            }''')
            assert page.evaluate('window.logViewerOpens') == 1
            assert page.evaluate('window.logViewerFiles') == 1
            page.evaluate("job.runtime_metrics={available:true,sampled_at:Date.now()/1000,gpu_percent:75,gpu_power_watts:89.5,gpu_power_limit_watts:170,cpu_percent:25,ram_used_bytes:1073741824,ram_total_bytes:4294967296,vram_used_bytes:2147483648,vram_total_bytes:25769803776}; renderCloudDashboard(job)")
            assert page.evaluate('window.logViewerOpens') == 2
            assert page.evaluate('window.logViewerFiles') == 1
            assert page.locator('#cloud-gpu-util').inner_text() == '75.0%'
            assert page.locator('#cloud-gpu-power').inner_text() == '90 / 170 W'
            assert page.locator('#cloud-cpu-util').inner_text() == '25.0%'
            page.evaluate('job.exact_queue={outstanding:16,sampled_at:Date.now()/1000}; renderCloudDashboard(job)')
            assert page.locator('#cloud-exact-queue').inner_text() == '16'
            page.evaluate('job.exact_queue.sampled_at-=60; renderCloudDashboard(job)')
            assert 'last sample' in page.locator('#cloud-exact-queue').inner_text()
            page.evaluate('job.convergence.checked_exact=688; renderCloudDashboard(job)')
            assert '12 newer results' in page.locator('#convergence-sample').inner_text()
            assert '0.25%' in page.locator('#convergence-help').get_attribute('data-tip')
            page.evaluate('job.convergence_config.convergence_tolerance_pct=0.1; renderCloudDashboard(job)')
            assert '0.1%' in page.locator('#convergence-help').get_attribute('data-tip')
            assert '1.00 / 4.00 GiB' in page.locator('#cloud-ram-util').inner_text()
            page.evaluate('job.runtime_metrics.sampled_at-=60; renderCloudDashboard(job)')
            assert page.locator('#cloud-gpu-util').inner_text() == '—'
            assert page.locator('#cloud-gpu-power').inner_text() == '—'
            assert page.locator('#convergence-since').inner_text() == '188 exact evaluations'
            assert page.locator('#convergence-progress').inner_text() == '188 / 512 exact evaluations'
            assert page.locator('#convergence-bar').evaluate('node => node.value') == pytest.approx(188/512*100)
            page.evaluate("job.completion_reason='convergence'; renderCloudDashboard(job)")
            assert 'Completed' in page.locator('#convergence-phase').inner_text()
            page.evaluate("job.status='cancelled'; job.completion_reason='rental_deadline'; job.convergence.final={phase:'tracking',checked_exact:700,last_improvement_exact:700,stalled_exact:0}; renderCloudDashboard(job)")
            assert 'rental time limit' in page.locator('#convergence-phase').inner_text()
            assert 'Final snapshot checked' in page.locator('#convergence-sample').inner_text()
            assert 'awaiting' not in page.locator('#convergence-sample').inner_text()
            assert page.locator('#convergence-progress').inner_text() == '0 / 512 exact evaluations'
            page.evaluate('job.convergence_config.convergence_enabled=false; renderCloudDashboard(job)')
            assert page.locator('#cloud-convergence').evaluate('node => node.hidden')
            page.evaluate('''() => {
                state.cloudLogId = 'worker-observed';
                state.logFile = '';
                renderObservedOptimizer({
                    id:'worker-observed',
                    observed_optimizer:{running:true,name:'perf-halving',activity:'Suite scenario 7/7',exact_completed:0,gpu_candidates:8192,sampled_at:Date.now()/1000},
                    runtime_metrics:{available:true,sampled_at:Date.now()/1000,gpu_percent:100,gpu_power_watts:57,gpu_power_limit_watts:120}
                });
            }''')
            assert page.evaluate('lastDashboard.name') == 'perf-halving'
            assert page.evaluate('lastDashboard.phase') == 'running'
            assert page.evaluate('lastDashboard.progress.proxy_evaluations') == 8192
            assert page.locator('#optlog-activity').inner_text() == 'Suite scenario 7/7'
            assert page.locator('#cloud-gpu-util').inner_text() == '100.0%'
            assert page.locator('#cloud-gpu-power').inner_text() == '57 / 120 W'
            assert page.locator('#stop-job').is_disabled()
            assert page.evaluate("state.logFile") == 'optimizes_v8/vast_worker-observed.log'
            assert page.evaluate("currentObservedOptimizer({observed_optimizer:{running:true,sampled_at:Date.now()/1000-46}})") is None
            page.evaluate('''openObservedOptimizer({
                id:'worker-observed',
                observed_optimizer:{running:true,name:'perf-halving',activity:'Suite scenario 7/7',sampled_at:Date.now()/1000},
                runtime_metrics:{}
            })''')
            assert page.evaluate('openedObservedLog.id') == 'worker-observed'
            assert page.evaluate('openedObservedLog.name') == 'perf-halving'
            assert page.evaluate('openedObservedLog.options.hasLog') is True
        finally:
            browser.close()


def test_vast_result_snapshots_trigger_refresh_without_local_runs():
    """Each newly published snapshot refreshes results once, including completion."""
    playwright = pytest.importorskip('playwright.sync_api')
    source = (ROOT / 'frontend/js/vast.js').read_text()
    function = source[source.index('  async function refreshJobs(preferred)'):source.index('  async function pollJobs()')]
    with playwright.sync_playwright() as runner:
        browser = runner.chromium.launch(headless=True)
        try:
            page = browser.new_page()
            page.set_content('<div id="supervision-status"></div>')
            page.add_script_tag(content='''
                const el = id => document.getElementById(id);
                let jobGeneration=0, disposed=false, jobRows=[], workers=[], worker=null, queueState={}, supervision=false, selectedJobId=null;
                const stoppingJobs = new Map();
                let data={jobs:[{id:'test'}]};
                const request=async () => structuredClone(data);
                const renderJob=()=>{},renderQueueOverview=()=>{};
                const message=msg=>{throw new Error(msg);};
                window.refreshes=[];
                const refreshLiveResultsDuringRun=async force=>refreshes.push(force);
            ''' + function)
            page.evaluate('refreshJobs()')
            assert page.evaluate('refreshes') == []
            page.evaluate("data.jobs[0].result_path='/results/test'; data.jobs[0].last_backup_at=100; data.jobs[0].result_partial=true; refreshJobs()")
            page.evaluate('refreshJobs()')
            assert page.evaluate('refreshes') == [True]
            page.evaluate('data.jobs[0].last_backup_at=160; refreshJobs()')
            page.evaluate('data.jobs[0].result_partial=false; refreshJobs()')
            assert page.evaluate('refreshes') == [True, True, True]
        finally:
            browser.close()


def test_pool_summary_and_explicit_start_consent():
    """Display all leases and gate pool activation behind the aggregate budget consent."""
    playwright = pytest.importorskip('playwright.sync_api')
    source = (ROOT / 'frontend/js/vast.js').read_text()
    activity = source[source.index('  function uploadProgressMetrics(job)'):source.index('  function renderJob()')]
    overview = source[source.index('  function openCloudResults(job)'):source.index('  async function refreshJobs(preferred)')]
    start = source[source.index('  async function startCloudQueue(job)'):source.index("  el('rent-offer').addEventListener")]
    with playwright.sync_playwright() as runner:
        browser = runner.chromium.launch(headless=True)
        try:
            page = browser.new_page()
            page.set_content('''<p id="message"></p><section id="queue-rental">
                <div id="queue-rental-status"></div><div id="queue-worker-cards"></div></section>''')
            page.add_script_tag(content='''
                let workers=[{id:'a',gpu_name:'GPU A',instance_id:101,status:'running',active_job:'job-a'},
                             {id:'b',gpu_name:'GPU B',instance_id:102,status:'provisioning',active_job:'job-b'}];
                let worker=workers[0], jobRows=[
                    {id:'job-a',config_name:'First optimizer',status:'running',result_path:'/results/first',exact_completed:7568,iterations:20000,
                     gpu_candidates:969728,throughput:{sampled_at:Date.now()/1000,proxy_per_minute:25600}},
                    {id:'job-b',config_name:'Second optimizer',status:'provisioning',
                     image_progress:{total:10,downloaded:4,ready:3,download_percent:42.5}},
                    {id:'job-c',config_name:'Uploading optimizer',status:'uploading',input_bytes:54100000,rental:{offer:{inet_down_mbps:1374}},
                     upload_progress:{stage:'sending',bytes:2300000,total:54100000,bytes_per_second:50000}},
                    {id:'job-d',config_name:'Rsync optimizer',status:'uploading',input_bytes:191600000,rental:{offer:{inet_down_mbps:63}},
                     upload_progress:{stage:'sending',transport:'rsync',mode:'files',bytes:0,transferred_bytes:70000000,total:191600000,bytes_per_second:650000}}
                ];
                let queueState={pool_enabled:true,pool_authorization:{settings:{max_rentals:3}}};
                let savedPreferences={max_rentals:3,auto_rent:true,budget:2,hours:1};
                let renting=false,startingQueue=false,workerAction=null,disposed=false,supervision=true,startingJobId=null;
                const deletingJobs=new Set(), workerActions=new Map(), fmt=(n,d)=>Number(n).toFixed(d);
                const el=id=>document.getElementById(id), renderJob=()=>{}, refreshJobs=async()=>{}, message=()=>{};
                const renderQueueMaybeDeferred=()=>{}, openCloudLog=job=>{window.openedLog=job.id};
                const openLogPanelResults=()=>{window.openedResults=window.state.logResultName}; window.state={};
                window.sent=[],window.accept=false,window.confirmation=null;
                const request=async(...args)=>{sent.push(args)};
                window.PBGuiDialogs={confirm:async options=>{window.confirmation=options;return window.accept}};
            ''' + activity + overview + start)
            assert '2 / 3 rentals' in page.evaluate('workerActivity()')
            assert 'First optimizer' in page.evaluate('workerActivity()')
            assert '7,568 / 20,000 exact' in page.evaluate('workerActivity()')
            assert '969,728 proxy (37.8%)' in page.evaluate('workerActivity()')
            assert '25,600 proxy/min' in page.evaluate('workerActivity()')
            assert '102' in page.evaluate('workerActivity()')
            assert 'Downloading worker image · 42.5%' in page.evaluate('workerActivity()')
            assert page.evaluate("compactJobProgress(jobRows[2])") == 'Uploading input · 2.3 / 54.1 MB · 4.3% · 0.4 Mbps · 0.03% of host bandwidth'
            assert page.evaluate("compactJobProgress(jobRows[3])") == 'Checking and synchronizing GPU input cache · 70.0 / 191.6 MB compared'
            page.evaluate('renderQueueOverview()')
            assert page.locator('.queue-gpu-card').count() == 2
            first_card = page.locator('.queue-gpu-card').first
            first_card.get_by_role('button', name='Log').click()
            assert page.evaluate('openedLog') == 'job-a'
            first_card.get_by_role('button', name='Results').click()
            assert page.evaluate('openedResults') == 'first'
            assert page.locator('.queue-gpu-card').nth(1).get_by_role('button', name='Results').is_disabled()
            page.evaluate("queueState.paused=true; jobRows.push({id:'queued',config_name:'Queued optimizer',status:'ready'}); renderQueueOverview()")
            assert page.locator('.queue-pool-state').inner_text() == 'Paused'
            assert page.get_by_role('button', name='Resume auto start').is_visible()
            first_card.get_by_role('button', name='Replace GPU').click()
            page.wait_for_function("confirmation?.title === 'Replace GPU?'")
            assert page.evaluate('sent.length') == 0
            page.evaluate('window.accept=true')
            page.locator('.queue-gpu-card').first.get_by_role('button', name='Replace GPU').click()
            page.wait_for_function('sent.length === 1')
            assert page.evaluate("sent[0][0]") == '/queue/workers/a/replace'
            page.evaluate('sent.length=0; window.accept=false; window.confirmation=null')
            page.get_by_role('button', name='Resume auto start').click()
            assert page.evaluate('sent.length') == 0
            assert '$6.00' in page.evaluate('confirmation.message')
            page.evaluate('window.accept=true; startCloudQueue()')
            assert page.evaluate('sent.length') == 1
            assert page.evaluate('JSON.parse(sent[0][1].body).accept_rental_and_cleanup')
        finally:
            browser.close()
