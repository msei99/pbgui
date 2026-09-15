"""Offline browser regressions for Vast issues 326–340, using real component assets."""
import json
from pathlib import Path
from urllib.parse import urlsplit

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def cloud_page():
    """Run the production component with intercepted HTTP and no provider access."""
    playwright = pytest.importorskip('playwright.sync_api')
    preferences = dict(gpu_name=None, max_price=.5, min_vram=12, min_ram=16,
                       min_cpu=4, disk_gb=40, verified_only=True, hours=1, budget=1,
                       idle_seconds=300)
    job = dict(id='a'*32, config_name='test-run', status='failed', can_delete=True,
               has_log=True, iterations=1000, exact_completed=120, workers=4,
               lease_id='lease-a', rental=dict(offer=dict(gpu_name='RTX 3090'), budget_usd=1))
    data = dict(jobs=[job], worker=dict(rental_state='active', gpu_name='RTX 3090',
                instance_id=123, status='running', price_hour_usd=.15, deadline=2000000000),
                queue=dict(paused=False), supervision_available=True)
    calls, overrides, held = [], {}, []
    html = '''<!doctype html><html><head><meta charset="utf-8"></head><body>
    <div id="vast-queue-host"></div><table><tbody id="rows"></tbody></table>
    <div id="log-panel" hidden><div id="vast-jobs-host"></div><div id="optlog-rental"></div>
    <div id="phase"></div><div id="activity"></div><div id="last-error"></div><div id="progress"></div>
    <div id="optlog-elapsed"></div><div id="optlog-pareto"></div><div id="optlog-cpu"></div>
    <button id="optlog-open-results"></button><button id="optlog-open-pareto-explorer"></button>
    <div id="optlog-progress-fill"></div><div id="optlog-progress-label"></div><div id="optlog-activity"></div>
    <div id="log-waiting"></div><div id="log-viewer-target"></div></div>
    <select id="opted-execution"><option value="local">Local</option><option value="vast">Vast</option></select>
    <div id="opted-vast-worker"></div><div id="opted-vast-validation" hidden></div>
    <button id="btn-cloud-save-queue">Queue</button><button id="btn-editor-save-queue">Save and queue</button>
    <button id="btn-editor-save">Save</button>
    <script>
    window.API_BASE='/api/optimize-v8'; window.WS_BASE=''; window.OPTIMIZE_VERSION='v8';
    window.state={editorLastConfig:{}};
    window.renderQueueMaybeDeferred=()=>{document.getElementById('rows').replaceChildren(...PBGuiVast.queueItems().map(PBGuiVast.queueRow));};
    window.openLogPanel=(id)=>{PBGuiVast.closeLog();state.cloudLogId=id;state.logFile=null;document.getElementById('log-panel').hidden=false;};
    window.renderOptimizeLogDashboard=(d)=>{
      document.getElementById('phase').textContent=d.phase;
      document.getElementById('activity').textContent=d.log.last_line;
      document.getElementById('last-error').textContent=d.log.last_error;
      document.getElementById('progress').textContent=d.progress.exact_evaluations+'/'+d.progress.target_exact_evaluations;
    };
    window.ensureLogViewer=()=>({open(){},setHost(){},setFile(file){document.getElementById('log-viewer-target').textContent=file;}});
    window.PBGuiDialogs={confirm:async()=>true};
    </script><script src="/app/js/vast.js"></script></body></html>'''

    def route_request(route):
        """Supply mock state or hold individual responses for race tests."""
        request = route.request
        path = urlsplit(request.url).path
        calls.append((request.method, request.url))
        if path in overrides:
            override = overrides[path]
            if override == 'hold':
                held.append(route)
                return
            status, body, headers = override
            route.fulfill(status=status, body=body, headers=headers)
            return
        if path == '/':
            route.fulfill(body=html, content_type='text/html')
            return
        if path in ['/app/js/vast.js', '/api/vast/fragment']:
            filename = 'frontend/js/vast.js' if path.endswith('.js') else 'frontend/vast.html'
            route.fulfill(body=(ROOT/filename).read_text(), content_type='text/javascript' if path.endswith('.js') else 'text/html')
            return
        if path == '/api/vast/jobs' and request.method == 'GET':
            payload = data
        elif path == '/api/vast/gpu-preferences':
            payload = preferences
        elif path == '/api/vast/settings':
            payload = dict(configured=False)
        elif path == '/api/vast/validate-config':
            payload = dict(valid=True, errors=[], metrics=[], revision='test1234')
        elif path == '/api/vast/account':
            payload = dict(balance_usd=3, account_id=1)
        elif path.endswith('/charges'):
            payload = dict(billing=dict(amount_usd=.12))
        elif path == '/api/vast/offers':
            payload = dict(offers=[dict(id=1, gpu_name='RTX 3090', price_hour_usd=.15,
                 duration_seconds=86400, cuda_max_good=13, location='test', tflops=35.58)])
        else:
            payload = {}
        route.fulfill(json=payload)

    with playwright.sync_playwright() as p:
        browser = p.chromium.launch()
        context = browser.new_context()
        page = context.new_page()
        page.route('**/*', route_request)
        page.goto('http://vast.test/')
        page.wait_for_function("window.PBGuiVast && PBGuiVast.queueItems().length === 1")
        yield page, data, calls, overrides, held
        context.close()
        browser.close()


def test_worker_action_lock_survives_rerender_and_failure(cloud_page):
    """Worker actions stay mutually exclusive across polling and recover after failure."""
    page, data, calls, overrides, held = cloud_page
    page.locator('[title="Open log"]').click()
    overrides['/api/vast/queue/end'] = 'hold'
    page.locator('#end-worker').click()
    page.wait_for_function("document.getElementById('end-worker').textContent === 'Ending rental…'")
    page.evaluate("document.getElementById('end-worker').click(); document.getElementById('pause-worker').click(); PBGuiVast.openEditor({})")
    assert page.locator('#end-worker').is_disabled()
    assert page.locator('#recover-worker').is_disabled()
    assert len([x for x in calls if '/queue/end' in x[1]]) == 1
    assert not any('/queue/pause' in x[1] for x in calls)
    held.pop().fulfill(status=503, body='<html>upstream failure</html>')
    page.wait_for_function("!document.getElementById('end-worker').disabled")
    assert 'HTTP 503' in page.locator('#queue-message').inner_text()
    assert page.locator('#end-worker').inner_text() == 'End rental'


def test_validation_retry_preserves_save_and_fail_closed_queue(cloud_page):
    """Retry validates the current draft, without allowing unvalidated queue entries."""
    page, _, _, overrides, held = cloud_page
    overrides['/api/vast/validate-config'] = (502, '<html>bad gateway</html>', {})
    page.select_option('#opted-execution', 'vast')
    page.evaluate('PBGuiVast.updateEditor()')
    retry = page.get_by_role('button', name='Retry validation')
    retry.wait_for()
    assert page.locator('#btn-editor-save').is_enabled()
    assert page.locator('#btn-editor-save-queue').is_disabled()
    assert 'HTTP 502' in page.locator('#opted-vast-validation').inner_text()
    overrides['/api/vast/validate-config'] = 'hold'
    retry.click()
    page.wait_for_function("document.getElementById('opted-vast-validation').getAttribute('aria-busy') === 'true'")
    page.wait_for_timeout(400)
    page.select_option('#opted-execution', 'local')
    page.evaluate('PBGuiVast.updateEditor()')
    held.pop().fulfill(status=503, json={'detail':'old failure'})
    assert page.locator('#btn-editor-save-queue').is_enabled()
    assert page.locator('#opted-vast-validation').is_hidden()
    del overrides['/api/vast/validate-config']
    page.select_option('#opted-execution', 'vast')
    page.evaluate('PBGuiVast.updateEditor()')
    page.wait_for_function("!document.getElementById('btn-editor-save-queue').disabled")


@pytest.mark.parametrize('status', [401, 403])
def test_provider_auth_failure_does_not_dispose_pb_gui(cloud_page, status):
    """A rejected provider credential is recoverable; expired PBGui sessions fail closed."""
    page, _, calls, overrides, _ = cloud_page
    overrides['/api/vast/account'] = (status, json.dumps({'detail':'Vast key rejected'}), {'X-PBGui-Error-Source':'vast'})
    page.locator('#refresh-balance').click()
    page.wait_for_function("document.getElementById('message').textContent.includes('Vast key rejected')")
    assert page.locator('#refresh-balance').is_enabled()
    del overrides['/api/vast/account']
    page.locator('#refresh-balance').click()
    page.wait_for_function("document.getElementById('balance').textContent === '$3.00'")
    overrides['/api/vast/account'] = (status, json.dumps({'detail':'Authentication required'}), {})
    page.locator('#api-key').fill('unsaved-test-key')
    page.locator('#refresh-balance').click()
    page.wait_for_function("document.getElementById('message').textContent.includes('PBGui session expired')")
    assert page.locator('#api-key').input_value() == ''
    assert page.locator('#refresh-balance').is_disabled()


def test_offer_feedback_null_model_and_filter_refresh(cloud_page):
    """Empty model stays empty; selecting and changing the filter have visible effects."""
    page, _, calls, _, _ = cloud_page
    assert page.locator('#gpu-model').input_value() == ''
    page.locator('#show-incompatible').check()
    page.locator('#offers-body tr[data-offer]').wait_for()
    assert any('include_incompatible=true' in x[1] for x in calls)
    assert not any('gpu_name=null' in x[1] for x in calls)
    page.locator('#offers-body tr[data-offer]').click()
    assert page.locator('#selection').is_visible()
    assert 'not reserved' in page.locator('#selection').inner_text()


def test_delete_and_requeue_locks_survive_row_replacement(cloud_page):
    """Only one confirmation or mutation may run for a row, even after redraw."""
    page, data, calls, overrides, held = cloud_page
    path = '/api/vast/jobs/' + 'a'*32
    page.evaluate('window.confirmations=0; PBGuiDialogs.confirm=()=>{confirmations++;return new Promise(resolve=>window.confirmDelete=resolve)}; void 0')
    page.locator('[title="Delete queue item"]').click()
    page.evaluate('renderQueueMaybeDeferred()')
    assert page.locator('[title="Delete queue item"]').is_disabled()
    assert page.locator('[title="Requeue"]').is_disabled()
    assert page.evaluate('confirmations') == 1
    assert not any(x[0] == 'DELETE' for x in calls)
    page.evaluate('confirmDelete(false)')
    page.wait_for_function("!document.querySelector('[title=\"Requeue\"]').disabled")
    overrides[path + '/requeue'] = 'hold'
    page.locator('[title="Requeue"]').click()
    page.evaluate('renderQueueMaybeDeferred()')
    assert page.locator('#rows .badge').inner_text() == 'Preparing…'
    assert page.locator('[title="Requeue"]').is_disabled()
    assert page.locator('[title="Delete queue item"]').is_disabled()
    held.pop().fulfill(status=500, json={'detail':'Prepare failed'})
    page.wait_for_function("!document.querySelector('[title=\"Requeue\"]').disabled")
    overrides[path] = 'hold'
    page.locator('[title="Delete queue item"]').click()
    page.evaluate('confirmDelete(true)')
    page.wait_for_timeout(50)
    page.evaluate('renderQueueMaybeDeferred()')
    assert page.locator('[title="Delete queue item"]').is_disabled()
    assert len([x for x in calls if x[0] == 'DELETE']) == 1
    data['jobs'].clear()
    held.pop().fulfill(json={'deleted':True})
    page.wait_for_function('PBGuiVast.queueItems().length === 0')


def test_integrated_dashboard_job_switch_and_billing_failure(cloud_page):
    """Queue log selection exposes telemetry without duplicate hidden legacy controls."""
    page, data, _, overrides, held = cloud_page
    job = data['jobs'][0]
    job['error'] = 'Example worker error'
    # Refresh via the real poll loop.
    page.clock.install()
    page.clock.run_for(10001)
    page.wait_for_function("PBGuiVast.queueItems()[0].cloudJob.error === 'Example worker error'")
    page.locator('[title="Open log"]').click()
    assert page.locator('#cloud-job-details').is_visible()
    assert page.locator('#activity').inner_text().startswith('RTX 3090 · instance 123')
    assert page.locator('#progress').inner_text() == '120/1000'
    assert page.locator('#last-error').inner_text() == 'Example worker error'
    assert 'vast_' + job['id'] + '.log' in page.locator('#log-viewer-target').inner_text()
    page.wait_for_function("document.getElementById('optlog-rental').textContent.includes('$0.1200')")
    path = '/api/vast/jobs/' + job['id'] + '/charges'
    overrides[path] = (503, 'gateway unavailable', {})
    page.evaluate('window.oldNow=Date.now; Date.now=()=>oldNow()+310000')
    page.locator('[title="Open log"]').click()
    page.wait_for_function("document.getElementById('optlog-rental').textContent.includes('(last retrieved)')")
    assert '$0.1200' in page.locator('#optlog-rental').inner_text()
    job2 = dict(job, id='b'*32, lease_id='lease-b', exact_completed=50, has_log=False, has_provider_log=True)
    data['jobs'].append(job2)
    overrides['/api/vast/jobs/' + job2['id'] + '/charges'] = (403, json.dumps({'detail':'Billing permission missing'}), {'X-PBGui-Error-Source':'vast'})
    page.clock.run_for(10001)
    page.locator('#rows tr').nth(1).locator('[title="Open log"]').click()
    page.wait_for_function("document.getElementById('optlog-rental').textContent.includes('Unavailable')")
    assert '$0.1200' not in page.locator('#optlog-rental').inner_text()
    assert page.locator('#progress').inner_text() == '50/1000'
    assert job2['id'] + '_provider.log' in page.locator('#log-viewer-target').inner_text()
    assert page.locator('#cloud-job-details').is_visible()
    for legacy_id in ['job-select', 'job-state', 'worker-state', 'job-log', 'close-job-log', 'vast-log']:
        assert page.locator('#' + legacy_id).count() == 0
    page.evaluate('PBGuiVast.closeLog(); PBGuiVast.openEditor({})')
    assert page.locator('#cloud-job-details').is_hidden()
    data['supervision_available'] = False
    data['jobs'][0]['status'] = 'ready'
    page.clock.run_for(10001)
    page.locator('#settings-supervision-status').wait_for(state='visible')
    assert page.locator('#settings-supervision-status').is_visible()
    assert 'systemd' in page.locator('#settings-supervision-status').inner_text()
    assert page.locator('[title="Start"]').is_disabled()


def test_malformed_success_response_remains_retryable(cloud_page):
    """A 200 HTML response is not treated as successful account data or a logout."""
    page, _, _, overrides, _ = cloud_page
    overrides['/api/vast/account'] = (200, '<html>unexpected login page</html>', {})
    page.locator('#refresh-balance').click()
    page.wait_for_function("document.getElementById('message').textContent.includes('Invalid response from PBGui')")
    assert page.locator('#refresh-balance').is_enabled()
    assert '<html>' not in page.locator('#message').inner_text()
    del overrides['/api/vast/account']
    page.locator('#refresh-balance').click()
    page.wait_for_function("document.getElementById('balance').textContent === '$3.00'")


def test_rent_now_and_queue_release_use_selected_offer(cloud_page):
    """Rent immediately posts the exact offer, then Queue shows and releases the lease."""
    page, data, calls, overrides, held = cloud_page
    page.add_init_script("""document.addEventListener('DOMContentLoaded',()=>{
      const box=document.createElement('div');box.id='queue-rental';
      box.innerHTML='<span id="queue-rental-status"></span><button id="queue-end-rental">End rental</button>';
      document.body.prepend(box);
    });""")
    data['worker'] = None
    page.reload()
    page.wait_for_function('window.PBGuiVast && PBGuiVast.queueItems().length === 1')
    page.locator('#find-offers').click()
    page.locator('tr[data-offer="1"]').click()
    overrides['/api/vast/queue/start'] = 'hold'
    page.locator('#rent-offer').click()
    page.wait_for_function("document.querySelector('#rent-offer').textContent==='Renting…'")
    page.wait_for_timeout(50)
    assert len(held) == 1
    payload = held[0].request.post_data_json
    assert payload['rent_only'] is True
    assert payload['offer_id'] == 1
    assert payload['preferences']['max_price'] == .15
    assert page.locator('#rent-offer').is_disabled()
    data['worker'] = dict(id='lease-a', rental_state='active', gpu_name='RTX 3090', instance_id=123,
                          status='reserved', price_hour_usd=.15, deadline=2000000000, awaiting_queue_start=True)
    data['queue']['paused'] = True
    held.pop().fulfill(json=data['worker'])
    page.wait_for_function("document.querySelector('#queue-rental-status').textContent.includes('Reserved for queue start')")
    assert page.locator('#rent-offer').is_disabled()
    assert 'instance 123' in page.locator('#queue-rental-status').inner_text()
    page.locator('#queue-end-rental').click()
    page.wait_for_timeout(100)
    assert any(method == 'POST' and url.endswith('/queue/end') for method, url in calls)


def test_rent_failure_is_visible_beside_button(cloud_page):
    """A rejected rental stays visible in Settings and permits another selection."""
    page, data, calls, overrides, held = cloud_page
    data['worker'] = None
    page.reload()
    page.wait_for_function('window.PBGuiVast && PBGuiVast.queueItems().length === 1')
    page.locator('#find-offers').click()
    page.locator('tr[data-offer="1"]').click()
    reason = 'The selected GPU is no longer available.'
    overrides['/api/vast/queue/start'] = (409, json.dumps({'detail': reason}), {})
    page.locator('#rent-offer').click()
    page.wait_for_function("document.querySelector('#rent-message').textContent.includes('no longer available')")
    assert page.locator('#rent-message').is_visible()
    assert page.locator('#rent-message').inner_text() == reason
    assert not page.locator('#rent-offer').is_disabled()


def test_offer_tflops_in_list_and_details(cloud_page):
    """Provider compute capacity is readable in the GPU row and expanded details."""
    page, *_ = cloud_page
    page.locator('#find-offers').click()
    row = page.locator('tr[data-offer="1"]')
    assert '35.6 TFLOPS' in row.inner_text()
    row.get_by_role('button', name='Details').click()
    assert '35.58 TFLOPS' in page.locator('.offer-details').inner_text()


@pytest.mark.parametrize('phase', ['preparing', 'ready', 'provisioning', 'uploading', 'running', 'collecting', 'completed', 'failed'])
def test_cloud_log_reopen_and_reload_phase_matrix(cloud_page, phase):
    """Opening/reopening after reload keeps log source and durable progress for every phase."""
    page, data, _, _, _ = cloud_page
    job = data['jobs'][0]
    job.update(status=phase, has_log=phase in {'running', 'collecting', 'completed', 'failed'},
               has_provider_log=phase in {'provisioning', 'uploading'}, lease_id='c'*32,
               upload_progress=dict(stage='sending', bytes=50_000_000, total=100_000_000))
    data['worker'].update(id='c'*32, has_provider_log=True)
    page.reload()
    page.wait_for_function('window.PBGuiVast && PBGuiVast.queueItems().length === 1')
    for _ in range(2):
        page.locator('[title="Open log"]').click()
        expected = ('vast_' + job['id'] + '.log' if job['has_log'] else
                    'vast_' + (job['id'] if job['has_provider_log'] else 'c'*32) + '_provider.log')
        assert expected in page.locator('#log-viewer-target').inner_text()
        if phase == 'uploading':
            assert '50.0 / 100.0 MB verified (50.0%)' in page.locator('#optlog-progress-label').inner_text()
        page.evaluate('PBGuiVast.closeLog()')


def test_cloud_provider_log_switches_to_optimizer_without_reopening(cloud_page):
    """Polling replaces the startup log once the optimizer log becomes available."""
    page, data, _, _, _ = cloud_page
    job = data['jobs'][0]
    job.update(status='uploading', has_log=False, has_provider_log=True,
               upload_progress=dict(stage='checking_cache'))
    page.reload()
    page.wait_for_function('window.PBGuiVast && PBGuiVast.queueItems().length === 1')
    page.locator('[title="Open log"]').click()
    assert '_provider.log' in page.locator('#log-viewer-target').inner_text()
    assert page.locator('#optlog-progress-label').inner_text() == 'Checking cached input data'
    job.update(status='running', has_log=True)
    page.clock.install()
    page.clock.run_for(10001)
    page.wait_for_function("state.logFile === 'optimizes_v8/vast_' + 'a'.repeat(32) + '.log'")


def test_old_job_does_not_borrow_unrelated_rental_log(cloud_page):
    """A terminal job without logs must not display the currently rented GPU's log."""
    page, data, _, _, _ = cloud_page
    data['jobs'][0].update(status='failed', has_log=False, has_provider_log=False, lease_id='old-lease')
    data['worker'].update(id='c'*32, has_provider_log=True)
    page.reload()
    page.wait_for_function('window.PBGuiVast && PBGuiVast.queueItems().length === 1')
    page.locator('[title="Open log"]').click()
    assert page.locator('#log-viewer-target').inner_text() == ''
    assert 'See Last error' in page.locator('#log-waiting').inner_text()


@pytest.mark.parametrize('stage,speed,expected', [
    ('sending', 1_000_000, 'Measured remaining: ~50 s'),
    ('sending', 0, 'Host-rate remaining (theoretical): ~2 s'),
    ('reconnecting', 1_000_000, None),
    ('verifying', 1_000_000, None),
    ('packing', 1_000_000, None),
])
def test_upload_remaining_time(cloud_page, stage, speed, expected):
    """Estimate actual transfer from throughput and label network fallback honestly."""
    page, data, _, _, _ = cloud_page
    job = data['jobs'][0]
    job.update(status='uploading', upload_progress=dict(stage=stage, bytes=50_000_000,
               total=100_000_000, bytes_per_second=speed))
    job['rental']['offer']['inet_down_mbps'] = 275
    page.reload()
    page.wait_for_function('window.PBGuiVast && PBGuiVast.queueItems().length === 1')
    page.locator('[title="Open log"]').click()
    text = page.locator('#optlog-progress-label').inner_text()
    if expected:
        assert expected in text
        assert 'Host: 275 Mbps' in text
        assert 'Host-rate remaining (theoretical): ~2 s' in text
        if speed:
            assert 'Upload: 8.0 Mbps' in text
            assert '2.9% reached' in text
        assert 'MB/s' not in text
    else:
        assert 'Measured remaining' not in text
        assert 'Host-rate remaining' not in text


def test_deadline_controls_wait_for_worker_confirmation(cloud_page):
    """Deadline buttons stay locked until the durable worker acknowledgement arrives."""
    page, data, calls, overrides, held = cloud_page
    lease = 'c'*32
    data['worker'].update(id=lease, deadline_protocol=1)
    data['jobs'][0].update(lease_id=lease)
    data['jobs'][0]['rental'].update(id=lease, deadline_protocol=1, deadline=2_000_000_000)
    page.reload()
    page.wait_for_function('window.PBGuiVast && PBGuiVast.queueItems().length === 1')
    page.locator('[title="Open log"]').click()
    button = page.get_by_role('button', name='Extend deadline by 30 minutes', exact=True)
    assert button.is_enabled()
    overrides['/api/vast/queue/deadline'] = 'hold'
    button.click()
    assert button.is_disabled()
    page.wait_for_timeout(50)
    assert sum('/queue/deadline' in url for _, url in calls) == 1
    data['jobs'][0]['rental']['deadline_pending'] = True
    held.pop().fulfill(json={'pending':True})
    page.wait_for_timeout(100)
    assert button.is_disabled()
    assert 'Awaiting worker confirmation' in page.locator('#optlog-rental').inner_text()
    data['jobs'][0]['rental'].update(deadline_pending=False, deadline=2_000_001_800)
    page.clock.install()
    page.clock.run_for(10001)
    page.wait_for_function("!document.querySelector('[title=\"Extend deadline by 30 minutes\"]').disabled")


def test_old_worker_deadline_controls_are_disabled(cloud_page):
    """An immutable old guard must never offer an ineffective deadline change."""
    page, data, _, _, _ = cloud_page
    data['worker'].update(id='c'*32)
    data['jobs'][0]['rental'].update(id='c'*32, deadline_protocol=0)
    page.reload()
    page.wait_for_function('window.PBGuiVast && PBGuiVast.queueItems().length === 1')
    page.locator('[title="Open log"]').click()
    controls = page.get_by_role('button', name='Requires an updated worker deadline guard', exact=True)
    assert controls.count() == 2
    assert controls.nth(0).is_disabled() and controls.nth(1).is_disabled()


def test_cache_check_counter_survives_log_reopen(cloud_page):
    """File progress is separate from transfer bytes and restored on reopening."""
    page, data, _, _, _ = cloud_page
    data['jobs'][0].update(status='uploading', upload_progress=dict(
        stage='checking_cache', files_checked=1024, files_total=2048,
        bytes=90, total=100))
    for _ in range(2):
        page.reload()
        page.wait_for_function('window.PBGuiVast && PBGuiVast.queueItems().length === 1')
        page.locator('[title="Open log"]').click()
        label = page.locator('#optlog-progress-label').inner_text()
        assert '1,024 / 2,048 files checked (50.0%)' in label
        assert 'MB' not in label and 'remaining' not in label
        assert page.locator('#optlog-progress-fill').evaluate('(el) => el.style.width') == '50%'


def test_rsync_progress_survives_log_reopen(cloud_page):
    """Rsync bytes remain visible without being mislabeled as checksum verified."""
    page, data, _, _, _ = cloud_page
    data['jobs'][0].update(status='uploading', upload_progress=dict(
        transport='rsync', stage='sending', bytes=0, transferred_bytes=50_000_000,
        total=100_000_000, bytes_per_second=1_000_000))
    for _ in range(2):
        page.reload()
        page.wait_for_function('window.PBGuiVast && PBGuiVast.queueItems().length === 1')
        page.locator('[title="Open log"]').click()
        label = page.locator('#optlog-progress-label').inner_text()
        assert '50.0 / 100.0 MB transferred (50.0%)' in label
        assert 'verified' not in label
        assert 'Upload: 8.0 Mbps' in label
        assert page.locator('#optlog-progress-fill').evaluate('(el) => el.style.width') == '50%'


def test_image_preparation_shows_source_and_fetch_age(cloud_page):
    """A zero-layer snapshot is a timed preparation phase, not a false byte percentage."""
    page, data, _, _, _ = cloud_page
    now = page.evaluate('Date.now() / 1000')
    data['jobs'][0].update(status='provisioning', has_provider_log=True,
        dispatch_at=now-180, provider_log_fetched_at=now-20,
        image_progress=dict(total=27, ready=0, downloaded=0, percent=0))
    page.reload()
    page.wait_for_function('window.PBGuiVast && PBGuiVast.queueItems().length === 1')
    page.locator('[title="Open log"]').click()
    text = page.locator('#optlog-progress-label').inner_text()
    assert 'Downloading worker image · 3m ' in text
    assert 'Host log (Extra Debug Logs)' in text
    assert 'Fetched 0m ' in text and '%' not in text
    assert page.locator('#optlog-progress-fill').evaluate('(el) => el.classList.contains("is-indeterminate")')
    page.evaluate('PBGuiVast.closeLog()')
    assert not page.locator('#optlog-progress-fill').evaluate('(el) => el.classList.contains("is-indeterminate")')


def test_requeue_acknowledges_click_before_poll_or_redraw(cloud_page):
    """Immediate feedback must not depend on the table's deferred polling render."""
    page, data, calls, overrides, held = cloud_page
    path = '/api/vast/jobs/' + 'a'*32 + '/requeue'
    overrides[path] = 'hold'
    page.evaluate('window.renderQueueMaybeDeferred=()=>{}')
    page.locator('[title="Requeue"]').click()
    assert page.locator('#rows .badge').inner_text() == 'Preparing…'
    assert page.locator('[title="Requeue"]').is_disabled()
    assert page.locator('[title="Delete queue item"]').is_disabled()
    page.locator('[title="Requeue"]').evaluate('(el)=>{el.click();el.click()}')
    assert sum(url.endswith('/requeue') for method, url in calls) == 1
    page.wait_for_timeout(50)
    held.pop().fulfill(status=500, json={'detail':'Preparation failed'})
    page.wait_for_function("!document.querySelector('[title=\"Requeue\"]').disabled")
    assert page.locator('#rows .badge').inner_text() != 'Preparing…'


def test_direct_file_sync_progress_survives_reopen(cloud_page):
    """Direct synchronization stays distinct from cache scans and archive uploads."""
    page, data, _, _, _ = cloud_page
    data['jobs'][0].update(status='uploading', upload_progress=dict(
        transport='rsync', mode='files', stage='sending', bytes=0,
        transferred_bytes=50_000_000, total=100_000_000, bytes_per_second=1_000_000))
    for _ in range(2):
        page.reload()
        page.wait_for_function('window.PBGuiVast && PBGuiVast.queueItems().length === 1')
        page.locator('[title="Open log"]').click()
        label = page.locator('#optlog-progress-label').inner_text()
        assert 'Synchronizing input files' in label
        assert '50.0 / 100.0 MB processed (50.0%)' in label
        assert 'Rsync: 8.0 Mbps' in label
        assert 'files checked' not in label and 'verified' not in label
        assert 'not measured network traffic' in page.locator('#optlog-progress-label').get_attribute('data-tip')
