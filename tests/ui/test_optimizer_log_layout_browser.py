"""Issue 363: real Optimize assets with isolated jobs, menus and browser geometry."""

import copy
import time
from pathlib import Path
from urllib.parse import urlparse

import pytest
from fastapi import Request

from api.page_templates import render_page_urls

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def log_page(request):
    """Serve only repository assets and mocked responses; never contact PBGui."""
    version = getattr(request, 'param', 'v8')
    prefix = '/mounted'
    html = render_page_urls(Request({'type': 'http', 'root_path': prefix}),
                            (ROOT / 'frontend/v7_optimize.html').read_text(), '/api/optimize-' + version)
    for key, value in {'LIMITS_META': '{}', 'VERSION': 'test', 'SERIAL': '1', 'NAV_HASH': 'test',
                       'OPTIMIZE_VERSION': version, 'BACKTEST_VERSION': version,
                       'OPTIMIZE_NAV_TITLE': 'Optimize', 'OPTIMIZE_NAV_CURRENT': version + '_optimize'}.items():
        html = html.replace('%%' + key + '%%', value)
    rental = dict(id='b' * 32, instance_id=123, rental_state='active', deadline_protocol=2,
                  budget_usd=1, transfer_reserve_usd=.1, deadline=time.time() + 3600,
                  offer=dict(gpu_name='Test GPU', price_hour_usd=.2, vram_gb=24, ram_gb=64, cpu_cores=16))
    job = dict(id='a' * 32, config_name='Test job', status='running', lease_id=rental['id'],
               rental=rental, workers=4, iterations=10000, exact_completed=100, has_log=True,
               convergence_config=dict(convergence_enabled=True), convergence=dict(phase='tracking'),
               runtime_metrics=dict(available=True, sampled_at=time.time(), gpu_percent=75),
               throughput=dict(sampled_at=time.time(), window_seconds=60, proxy_per_minute=200,
                               exact_per_minute=20, proxy_total=2000, exact_total=200))
    data = dict(jobs=[job], workers=[rental], worker=rental, queue={}, supervision_available=True)
    queue = [dict(filename='job', name='Local job', status='running', exchange='binance')]
    errors, calls, held = [], [], []
    control = {'hold_status': False, 'hold_charges': False, 'hold_jobs': False, 'jobs_before_hold': 0, 'jobs_status': 200, 'hold_fragment': False, 'status': {}}
    playwright = pytest.importorskip('playwright.sync_api')
    with playwright.sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page(viewport={'width': 1280, 'height': 900}, locale='en-US')
        page.set_default_timeout(6000)
        page.on('pageerror', lambda error: errors.append(str(error)))
        page.add_init_script('''const originalTimeout=window.setTimeout;
        window.setTimeout=function(fn,ms,...args){if(fn.name==='pollJobs')window.testCloudPoll=fn;return originalTimeout(fn,ms,...args);};
        window.WebSocket=class {
          constructor(){this.readyState=1;setTimeout(()=>this.onopen&&this.onopen(),0);}
          close(){} send(){}
        };''')

        def respond(route):
            """Intercept every URL, including log traffic and external assets."""
            path = urlparse(route.request.url).path.removeprefix(prefix)
            calls.append(path)
            if path.startswith('/app/'):
                asset = ROOT / 'frontend' / path.removeprefix('/app/')
                route.fulfill(body=asset.read_bytes(), content_type={'.js': 'text/javascript', '.css': 'text/css'}.get(asset.suffix, 'application/octet-stream'))
            elif path == '/start':
                route.fulfill(body='<nav id="topnav"></nav><script>window.PBGUI_NAV_CONFIG={authenticated:true,apiBase:"/mounted/api/balance-calc",current:"info_balance_calc"};</script><script src="/mounted/app/pbgui_nav.js?v=test"></script>', content_type='text/html')
            elif path == '/api/optimize-' + version + '/main_page':
                route.fulfill(body=html, content_type='text/html')
            elif path == '/api/vast/fragment':
                if control['hold_fragment']:
                    held.append(('fragment', route))
                else:
                    route.fulfill(body=(ROOT / 'frontend/vast.html').read_text(), content_type='text/html')
            elif path == '/api/vast/jobs':
                if control['hold_jobs'] and control['jobs_before_hold'] <= 0:
                    held.append(('jobs', route))
                else:
                    control['jobs_before_hold'] -= 1
                    route.fulfill(status=control['jobs_status'], json=data)
            elif path.endswith('/charges'):
                if control['hold_charges']:
                    held.append(('charges', route))
                else:
                    route.fulfill(json={'billing': {}})
            elif path.endswith('/queue/job/status'):
                if control['hold_status']:
                    held.append(('status', route))
                else:
                    route.fulfill(json=control['status'])
            elif path.endswith('/queue'):
                route.fulfill(json={'items': queue})
            elif path.endswith('/configs'):
                route.fulfill(json={'configs': []})
            elif path.endswith('/results'):
                route.fulfill(json={'results': []})
            elif path.endswith('/settings'):
                route.fulfill(json={'cpu': 1, 'autostart': False})
            elif path == '/api/help/index':
                route.fulfill(json=[{'file': '43_pbv8_optimize.md', 'title': 'Optimize'}])
            elif path == '/api/help/content':
                route.fulfill(json={'content': (ROOT / 'docs/help/43_pbv8_optimize.md').read_text()})
            elif path.endswith('/stream'):
                route.fulfill(status=204, body='')
            else:
                route.fulfill(json={})

        page.route('**/*', respond)
        page.goto('http://optimizer.test' + prefix + '/start')
        page.locator('.nav-group-btn[data-group="pb' + version + '"]').click()
        page.locator('.nav-item[data-page="' + version + '_optimize"]').click()
        page.wait_for_function('window.PBGuiOptimizeLog && !state.initialLoadPending')
        page.locator('[data-panel="queue"]').click()
        if version == 'v8':
            page.wait_for_function('window.PBGuiVast && PBGuiVast.queueItems().length === 1')
        yield page, data, queue, errors, calls, held, control
        browser.close()


def open_cloud(page):
    """Use the real queue-row action and wait for the measured layout."""
    page.locator('tr[data-cloud-id] [title="Open log"]').click()
    page.wait_for_function('!document.getElementById("optlog-rental-group").hidden && document.getElementById("optlog-details").clientHeight>0')
    page.wait_for_timeout(60)


def boxes(page):
    """Return numeric boxes without taking screenshots or reading log text."""
    return page.evaluate('''() => Object.fromEntries(['log-panel','log-panel-header','optlog-head','optlog-notices','optlog-details','optlog-divider','log-viewer-target','log-waiting'].map(id=>{
      const n=document.getElementById(id),r=n.getBoundingClientRect();
      return [id,{x:r.x,y:r.y,width:r.width,height:n.hidden?0:r.height,bottom:r.bottom}];
    }))''')


def test_closed_grid_actions_and_log_share(log_page):
    """All four closed groups form 2x2; actions precede them and log owns 70%."""
    page, _, _, errors, _, _, _ = log_page
    open_cloud(page)
    assert page.locator('#optlog-details details:visible').count() == 4
    assert page.locator('#optlog-details details[open]').count() == 0
    groups = [page.locator('#optlog-' + key + '-group').bounding_box() for key in ('rental', 'run', 'utilization', 'convergence')]
    assert groups[0]['y'] == groups[1]['y']
    assert groups[2]['y'] == groups[3]['y'] > groups[0]['y']
    assert groups[0]['x'] == groups[2]['x'] < groups[1]['x'] == groups[3]['x']
    b = boxes(page)
    assert b['log-viewer-target']['height'] >= .7 * (b['log-panel']['height'] - 4), b
    assert page.locator('#pause-worker').bounding_box()['y'] < b['optlog-details']['y']
    assert page.locator('#optlog-open-results').bounding_box()['y'] < b['optlog-head']['bottom']
    assert page.locator('#log-panel [id$="fetch-btn"]').is_hidden()
    assert page.locator('#optlog-utilization-hint').inner_text().startswith('GPU 75.0% · Exact/min 20')
    assert not errors


@pytest.mark.parametrize('width,height,panel_width', [(1280,900,1200), (1280,900,640), (1280,900,639), (760,600,640), (390,600,358), (390,599,358)])
def test_open_groups_share_budget_and_breakpoint(log_page, width, height, panel_width):
    """Open details cannot crush the terminal at either side of the breakpoint."""
    page, _, _, errors, _, _, _ = log_page
    page.set_viewport_size({'width': width, 'height': height})
    open_cloud(page)
    page.evaluate('''width=>{document.getElementById('log-panel').style.width=width+'px';
      document.querySelectorAll('#optlog-details details').forEach(n=>n.open=true);}''', panel_width)
    page.wait_for_timeout(100)
    b = boxes(page)
    minimum = 160 if height >= 600 else 120
    assert b['log-viewer-target']['height'] >= minimum - 1, b
    assert page.locator('.lvp-terminal').bounding_box()['height'] >= 48 - 1
    assert b['log-viewer-target']['bottom'] <= b['log-panel']['bottom'] - 1, b
    assert page.evaluate('''() => ['optlog-head','optlog-details'].every(id=>{
      const n=document.getElementById(id);return n.scrollWidth<=n.clientWidth;
    })''')
    rental = page.locator('#optlog-rental-group').bounding_box()
    run = page.locator('#optlog-run-group').bounding_box()
    if panel_width < 640:
        assert run['y'] > rental['y']
    else:
        assert run['y'] == rental['y']
    assert not errors


def test_automatic_details_inputs_polling_and_reload(log_page):
    """Content determines the split while polling/reload preserve drafts and groups."""
    page, _, _, errors, _, _, _ = log_page
    open_cloud(page)
    page.locator('#optlog-rental-group summary').click()
    budget = page.locator('[aria-label="Budget target in USD"]')
    budget.fill('2.50')
    page.evaluate('window.oldLogViewer=state.logViewer;window.testCloudPoll()')
    page.wait_for_timeout(80)
    assert budget.input_value() == '2.50'
    assert budget.evaluate('node=>node===document.activeElement')
    assert page.evaluate('state.logViewer===window.oldLogViewer')
    assert page.locator('#optlog-rental-group').get_attribute('open') is not None
    expanded = boxes(page)
    assert page.locator('#optlog-splitter').count() == 0
    assert page.locator('#optlog-divider').get_attribute('tabindex') is None
    page.locator('#optlog-rental-group summary').click()
    page.wait_for_timeout(80)
    collapsed = boxes(page)
    assert collapsed['log-viewer-target']['y'] < expanded['log-viewer-target']['y'] - 50
    page.locator('#optlog-rental-group summary').click()
    page.wait_for_timeout(80)
    chosen = boxes(page)['optlog-details']['height']
    page.reload()
    page.wait_for_function('state.cloudLogId === "' + 'a' * 32 + '" && !document.getElementById("optlog-rental-group").hidden')
    page.wait_for_timeout(80)
    assert page.locator('#optlog-rental-group').get_attribute('open') is not None
    assert abs(boxes(page)['optlog-details']['height'] - chosen) < 2
    assert page.locator('#panel-queue').is_visible()
    page.locator('#log-panel-close').click()
    assert page.locator('#log-panel').is_hidden()
    assert not errors


@pytest.mark.parametrize('old_height', [0, 9000])
def test_legacy_split_is_ignored(log_page, old_height):
    """Old manual heights cannot hide expanded content or leave an empty gap."""
    page, _, _, errors, _, _, _ = log_page
    open_cloud(page)
    page.locator('#optlog-rental-group summary').click()
    page.wait_for_timeout(80)
    expected = boxes(page)['optlog-details']['height']
    page.evaluate('''height => {
      const key='pbgui.optimize-log.v8.v1';
      const saved=JSON.parse(sessionStorage.getItem(key));
      saved.detailHeight=height;sessionStorage.setItem(key,JSON.stringify(saved));
    }''', old_height)
    page.reload()
    page.wait_for_function('state.cloudLogId === "' + 'a' * 32 + '" && !document.getElementById("optlog-rental-group").hidden')
    page.wait_for_timeout(80)
    assert abs(boxes(page)['optlog-details']['height'] - expected) < 2
    assert page.evaluate("!Object.hasOwn(JSON.parse(sessionStorage.getItem('pbgui.optimize-log.v8.v1')),'detailHeight')")
    assert not errors


@pytest.mark.parametrize('log_page', ['v7', 'v8'], indirect=True)
def test_reset_layout_preserves_log_and_persists_defaults(log_page):
    """Reset centers the window and collapses groups without recreating the log."""
    page, _, _, errors, _, _, _ = log_page
    page.evaluate("openLogPanel('job','Local job');window.viewerBeforeReset=state.logViewer")
    page.locator('#optlog-run-group summary').click()
    page.evaluate('''() => {
      const panel=document.getElementById('log-panel');
      Object.assign(panel.style,{width:'780px',height:'500px',left:'100px',top:'80px'});
    }''')
    reset = page.get_by_role('button', name='Reset to default', exact=True)
    reset.focus()
    reset.press('Enter')
    page.wait_for_timeout(80)
    b = boxes(page)['log-panel']
    assert abs(b['width'] - 1088) < 1
    assert abs(b['height'] - 675) < 1
    assert abs(b['x'] + b['width']/2 - 640) < 1
    assert abs(b['y'] + b['height']/2 - 450) < 1
    assert page.locator('#optlog-details details[open]').count() == 0
    assert page.evaluate("state.logFilename==='job' && state.logViewer===window.viewerBeforeReset")
    page.reload()
    page.wait_for_function("state.logFilename==='job' && document.getElementById('log-panel').classList.contains('visible')")
    page.wait_for_timeout(80)
    restored = boxes(page)['log-panel']
    for key in ('x', 'y', 'width', 'height'):
        assert abs(restored[key] - b[key]) < 1
    assert page.locator('#optlog-details details[open]').count() == 0
    assert not errors


def test_reset_layout_preserves_cloud_draft(log_page):
    """Resetting a cloud window keeps the rental draft and current viewer alive."""
    page, _, _, errors, _, _, _ = log_page
    open_cloud(page)
    page.locator('#optlog-rental-group summary').click()
    budget = page.get_by_role('spinbutton', name='Budget target in USD', exact=True)
    budget.fill('2.50')
    page.evaluate('window.viewerBeforeReset=state.logViewer')
    page.get_by_role('button', name='Reset to default', exact=True).click()
    assert page.locator('#optlog-details details[open]').count() == 0
    assert page.evaluate('state.logViewer===window.viewerBeforeReset')
    page.locator('#optlog-rental-group summary').click()
    assert budget.input_value() == '2.50'
    page.evaluate('window.testCloudPoll()')
    page.wait_for_timeout(80)
    assert budget.input_value() == '2.50'
    assert page.evaluate('state.logViewer===window.viewerBeforeReset')
    assert not errors


def test_expanded_content_has_no_individual_scroll_or_empty_gap(log_page):
    """Tall windows show full detail bodies; collapse gives their space to the log."""
    page, _, _, errors, _, _, _ = log_page
    page.set_viewport_size({'width': 1280, 'height': 1800})
    open_cloud(page)
    closed = boxes(page)
    page.evaluate("document.querySelectorAll('#optlog-details details').forEach(n=>n.open=true)")
    page.wait_for_timeout(100)
    opened = boxes(page)
    assert opened['log-viewer-target']['y'] > closed['log-viewer-target']['y'] + 200
    assert abs(opened['log-viewer-target']['y'] - opened['optlog-details']['bottom'] - opened['optlog-divider']['height']) < 2
    assert page.evaluate('''() => [...document.querySelectorAll('.optlog-accordion-body')].every(n=>n.scrollHeight <= n.clientHeight + 1)''')
    assert page.evaluate("document.getElementById('optlog-details').scrollHeight <= document.getElementById('optlog-details').clientHeight + 1")
    page.evaluate("document.querySelectorAll('#optlog-details details').forEach(n=>n.open=false)")
    page.wait_for_timeout(100)
    assert abs(boxes(page)['log-viewer-target']['y'] - closed['log-viewer-target']['y']) < 2
    assert not errors


@pytest.mark.parametrize('log_page', ['v7', 'v8'], indirect=True)
def test_local_switch_generation_errors_and_backtest(log_page):
    """A→B→A rejects stale status; errors remain visible with closed Run Details."""
    page, _, _, errors, _, held, control = log_page
    control['hold_status'] = True
    page.evaluate("openLogPanel('job','Local job')")
    page.wait_for_function('state.logStatusInFlight')
    page.wait_for_timeout(40)
    old = next(route for kind, route in held if kind == 'status')
    page.evaluate("openLogPanel('other','Other');openLogPanel('job','Local job')")
    old.fulfill(json={'phase': 'stale', 'log': {'last_error': 'Old failure'}})
    page.wait_for_timeout(60)
    assert page.locator('#optlog-phase').inner_text() != 'stale'
    assert 'Old failure' not in page.locator('#optlog-run-error').text_content()
    for kind, route in held[1:]:
        if kind == 'status':
            route.fulfill(json={'phase': 'running', 'log': {'last_error': 'Current failure'}})
    page.wait_for_function('document.getElementById("optlog-run-error").textContent === "Current failure"')
    assert page.locator('#optlog-run-error').is_visible()
    assert page.locator('#optlog-run-group').get_attribute('open') is None
    assert page.locator('#optlog-rental-group').is_hidden()
    page.evaluate("openLogPanel('" + 'c' * 32 + "','Backtest',{backtest:true})")
    page.wait_for_timeout(50)
    assert page.locator('#optlog-details').is_hidden()
    assert page.locator('#optlog-divider').is_hidden()
    assert page.locator('#optlog-cloud-actions').is_hidden()
    assert not errors


def test_window_focus_geometry_guide_and_nonmodal_navigation(log_page):
    """Real menu/Guide, free focus, explicit dismissal and resizing still work."""
    page, _, _, errors, calls, _, _ = log_page
    for key, expected in [('Home',160), ('End',420)]:
        page.locator('#sidebar-resize').focus()
        page.locator('#sidebar-resize').press(key)
        assert page.locator('#sidebar').bounding_box()['width'] == expected
    open_cloud(page)
    b = boxes(page)['log-panel']
    assert abs(b['x'] + b['width']/2 - 640) < 1
    assert abs(b['y'] + b['height']/2 - 450) < 1
    summary = page.locator('#optlog-run-group summary')
    summary.focus()
    summary.press('Enter')
    assert page.locator('#optlog-run-group').get_attribute('open') is not None
    summary.press('Space')
    assert page.locator('#optlog-run-group').get_attribute('open') is None
    page.locator('#optlog-cpu').hover()
    assert page.locator('#data-tip-tooltip').is_visible()
    tip = page.locator('#data-tip-tooltip').bounding_box()
    assert tip['x'] >= 0 and tip['x'] + tip['width'] <= 1280
    page.locator('#optlog-reset-layout').focus()
    page.keyboard.press('Shift+Tab')
    assert page.evaluate('!document.getElementById("log-panel").contains(document.activeElement)')
    page.mouse.click(5,5)
    assert page.locator('#log-panel').is_visible()
    header = page.locator('#log-panel-title').bounding_box()
    page.mouse.move(header['x']+5, header['y']+5)
    page.mouse.down(); page.mouse.move(header['x']+35, header['y']+35); page.mouse.up()
    assert boxes(page)['log-panel']['x'] > b['x']
    handle = page.locator('.lp-resize-se').bounding_box()
    page.mouse.move(handle['x']+2,handle['y']+2)
    page.mouse.down(); page.mouse.move(handle['x']-40,handle['y']-40); page.mouse.up()
    assert boxes(page)['log-panel']['width'] < b['width']
    page.locator('#log-panel-close').focus()
    page.keyboard.press('Escape')
    assert page.locator('#log-panel').is_hidden()
    assert page.evaluate('document.activeElement.closest("tr[data-cloud-id]") !== null')
    assert '/api/optimize-v8/main_page' in calls
    before = page.url
    page.locator('#pbgui-guide-btn').click()
    page.wait_for_function('document.getElementById("pbgui-shared-help-ovl").classList.contains("visible")')
    assert page.url == before
    assert not errors


def test_short_window_errors_and_waiting_log(log_page):
    """Very short windows retain scroll access; first output replaces waiting state."""
    page, data, _, errors, _, _, _ = log_page
    job = data['jobs'][0]
    job.update(status='failed', has_log=False, error='Failure ' * 150)
    job['rental']['deadline_error'] = 'Deadline adjustment rejected'
    page.evaluate('window.testCloudPoll()')
    page.wait_for_timeout(70)
    open_cloud(page)
    assert page.locator('#log-waiting').is_visible()
    assert page.locator('#optlog-run-error').is_visible()
    assert 'Deadline adjustment rejected' in page.locator('#optlog-rental-hint').inner_text()
    page.evaluate('''() => {
      document.documentElement.style.fontSize='200%';
      document.getElementById('log-panel').style.height='260px';
      document.querySelectorAll('#optlog-details details').forEach(n=>n.open=true);
    }''')
    page.wait_for_timeout(80)
    assert page.locator('#log-waiting').bounding_box()['height'] >= 160
    assert page.locator('#optlog-notices').bounding_box()['height'] > 0
    assert page.locator('#optlog-details').bounding_box()['height'] >= 24
    assert page.evaluate('''() => {
      const panel=document.getElementById('log-panel');panel.scrollTop=panel.scrollHeight;
      return document.getElementById('log-waiting').getBoundingClientRect().bottom <= panel.getBoundingClientRect().bottom;
    }''')
    job.update(status='running', has_log=True, error='')
    page.evaluate('window.testCloudPoll()')
    page.wait_for_function('!document.getElementById("log-viewer-target").hidden')
    assert page.locator('#log-waiting').is_hidden()
    assert not errors


def test_stale_samples_groups_and_billing(log_page):
    """Stale hardware is unavailable; historical throughput retains its age and meaning."""
    page, data, _, errors, _, held, control = log_page
    control['hold_charges'] = True
    open_cloud(page)
    old = next(route for kind, route in held if kind == 'charges')
    page.evaluate("openLogPanel('job','Local');PBGuiVast.openJobLog('" + 'a' * 32 + "')")
    old.fulfill(json={'billing': {'amount_usd': 12345}})
    page.wait_for_timeout(70)
    assert '12345' not in page.locator('#optlog-rental').text_content()
    job = data['jobs'][0]
    job['runtime_metrics']['sampled_at'] -= 100
    job['throughput']['sampled_at'] -= 200
    job['convergence_config']['convergence_enabled'] = False
    page.evaluate('window.testCloudPoll()')
    page.wait_for_timeout(80)
    assert page.locator('#optlog-convergence-group').is_hidden()
    hint = page.locator('#optlog-utilization-hint').inner_text()
    assert 'GPU —' in hint and 'Exact/min 20' in hint and 'Last interval' in hint
    job['rental'] = None
    page.evaluate('window.testCloudPoll()')
    page.wait_for_function('document.getElementById("optlog-rental-group").hidden')
    assert not errors


def test_unavailable_target_and_storage_denial(log_page):
    """Unavailable logs explain their removal; storage failure leaves controls usable."""
    page, data, _, errors, _, _, _ = log_page
    open_cloud(page)
    data['jobs'] = []
    page.reload()
    page.wait_for_function('!state.initialLoadPending && window.PBGuiVast')
    page.wait_for_function('!document.getElementById("log-panel").classList.contains("visible")')
    page.wait_for_timeout(100)
    assert page.locator('#panel-queue').is_visible()
    assert 'no longer available' in page.locator('#toast-stack').inner_text()
    page.add_init_script("Object.defineProperty(window,'sessionStorage',{get(){throw new Error('Storage unavailable');}})")
    page.reload()
    page.wait_for_function('window.PBGuiOptimizeLog && !state.initialLoadPending')
    page.evaluate("openLogPanel('job','Local')")
    page.locator('#optlog-run-group summary').click()
    page.locator('#optlog-run-group summary').focus()
    page.keyboard.press('Escape')
    assert page.locator('#log-panel').is_hidden()
    assert not errors


def test_late_fragment_and_close_during_window_drag(log_page):
    """A late component cannot reopen a closed log; window drags end on close."""
    page, _, _, errors, _, held, control = log_page
    control['hold_fragment'] = True
    page.reload()
    page.wait_for_function('window.PBGuiOptimizeLog && !state.initialLoadPending')
    page.evaluate("openLogPanel('job','Local')")
    page.wait_for_timeout(70)
    pos = page.locator('#log-panel-title').bounding_box()
    page.mouse.move(pos['x']+20,pos['y']+4)
    page.mouse.down()
    page.evaluate('closeLogPanel()')
    page.mouse.move(pos['x']+20,pos['y']+100)
    page.mouse.up()
    fragment = next(route for kind, route in held if kind == 'fragment')
    fragment.fulfill(body=(ROOT/'frontend/vast.html').read_text(), content_type='text/html')
    page.wait_for_function('!!window.PBGuiVast')
    assert page.locator('#log-panel').is_hidden()
    assert page.locator('#optlog-backdrop').count() == 0
    assert not errors


def test_nested_confirmation_keeps_log_focus_and_window(log_page):
    """A shared confirmation above the log owns Tab/Escape without closing the log."""
    page, _, _, errors, _, _, _ = log_page
    open_cloud(page)
    page.evaluate("() => {window.dialogResult=null;void PBGuiDialogs.confirm({title:'Test decision',message:'Isolated test',confirmText:'Proceed'}).then(result=>window.dialogResult=result);}")
    assert page.locator('#pbgui-dialog-ovl').is_visible()
    page.locator('#pbgui-dialog-accept').focus()
    page.keyboard.press('Shift+Tab')
    assert page.locator('#pbgui-dialog-cancel').evaluate('n=>n===document.activeElement')
    page.keyboard.press('Tab')
    assert page.locator('#pbgui-dialog-accept').evaluate('n=>n===document.activeElement')
    page.keyboard.press('Escape')
    assert page.locator('#pbgui-dialog-ovl').is_hidden()
    assert page.evaluate('window.dialogResult') is False
    assert page.locator('#log-panel').is_visible()
    assert not errors


def test_background_page_and_result_navigation_stay_interactive(log_page):
    """The log has no backdrop and page controls remain usable while it is open."""
    page, data, _, errors, _, _, _ = log_page
    data['jobs'][0]['result_path'] = 'Test job'
    page.evaluate('window.testCloudPoll()')
    page.wait_for_timeout(80)
    open_cloud(page)
    assert page.locator('#log-panel').get_attribute('aria-modal') == 'false'
    assert page.locator('#optlog-backdrop').count() == 0
    page.locator('[data-panel="configs"]').click(position={'x': 10, 'y': 10})
    assert page.locator('#panel-configs').is_visible()
    assert page.locator('#log-panel').is_visible()
    page.locator('#optlog-open-results').click()
    page.wait_for_function('state.panel === "results"')
    page.locator('[data-panel="queue"]').click(position={'x': 10, 'y': 10})
    assert page.locator('#panel-queue').is_visible()
    page.locator('#optlog-open-pareto-explorer').click()
    page.wait_for_function('state.panel === "results"')
    assert page.locator('#log-panel').is_visible()
    assert not errors


@pytest.mark.parametrize('log_page', ['v7', 'v8'], indirect=True)
def test_persisted_pageshow_revalidates_and_resumes_log(log_page):
    """Exercise pagehide/pageshow handlers with fresh snapshots and resumed polling."""
    page, data, _, errors, calls, _, control = log_page
    cloud = page.evaluate('OPTIMIZE_VERSION === "v8"')
    if cloud:
        open_cloud(page)
        page.locator('#optlog-rental-group summary').click()
        page.locator('[aria-label="Budget target in USD"]').fill('2.50')
        data['jobs'][0]['runtime_metrics']['gpu_percent'] = 42
    else:
        page.evaluate("openLogPanel('job','Local')")
        control['status'] = {'phase': 'completed', 'log': {'last_line': 'Fresh status'}}
    page.evaluate("window.dispatchEvent(new PageTransitionEvent('pagehide',{persisted:true}))")
    assert page.locator('#log-panel').is_hidden()
    previous_requests = len(calls)
    page.evaluate("window.dispatchEvent(new PageTransitionEvent('pageshow',{persisted:true}))")
    page.wait_for_function('document.getElementById("log-panel").classList.contains("visible")')
    if cloud:
        page.wait_for_function('document.getElementById("cloud-gpu-util").textContent === "42.0%"')
        assert page.locator('[aria-label="Budget target in USD"]').input_value() == '2.50'
        data['jobs'][0]['runtime_metrics']['gpu_percent'] = 66
        page.evaluate('window.testCloudPoll()')
        page.wait_for_function('document.getElementById("cloud-gpu-util").textContent === "66.0%"')
    else:
        page.wait_for_function('document.getElementById("optlog-activity").textContent === "Fresh status"')
        assert page.evaluate('!!state.logStatusTimer')
    assert len(calls) > previous_requests
    assert not errors


def test_close_after_pagehide_discards_saved_target(log_page):
    """Closing an inactive layout invalidates the target before a cache return."""
    page, _, _, errors, _, _, _ = log_page
    open_cloud(page)
    page.evaluate("window.dispatchEvent(new PageTransitionEvent('pagehide',{persisted:true}));closeLogPanel();window.dispatchEvent(new PageTransitionEvent('pageshow',{persisted:true}))")
    page.wait_for_timeout(150)
    assert page.locator('#log-panel').is_hidden()
    assert page.evaluate('JSON.parse(sessionStorage.getItem("pbgui.optimize-log.v8.v1")).target') is None
    assert not errors


@pytest.mark.parametrize('action', ['open', 'close', 'navigate'])
def test_slow_restore_cannot_override_user_action(log_page, action):
    """A real delayed cloud lookup cannot overwrite a newer log, close or navigation."""
    page, data, _, errors, _, held, control = log_page
    open_cloud(page)
    original = copy.deepcopy(data['jobs'][0])
    replacement = copy.deepcopy(original)
    replacement['id'] = 'd' * 32
    data['jobs'] = [replacement]
    control.update(hold_jobs=True, jobs_before_hold=1)
    page.reload()
    page.wait_for_function('window.PBGuiVast && !state.initialLoadPending')
    page.wait_for_timeout(100)
    assert held and held[-1][0] == 'jobs'
    if action == 'open':
        page.evaluate("openLogPanel('job','Chosen local log')")
    elif action == 'close':
        page.evaluate('closeLogPanel()')
    else:
        page.locator('[data-panel="configs"]').click()
    data['jobs'] = [original, replacement]
    held[-1][1].fulfill(json=data)
    page.wait_for_timeout(100)
    if action == 'open':
        assert page.evaluate('state.logFilename') == 'job'
        assert page.evaluate('state.cloudLogId') is None
        assert page.evaluate('JSON.parse(sessionStorage.getItem("pbgui.optimize-log.v8.v1")).target.filename') == 'job'
    else:
        assert page.locator('#log-panel').is_hidden()
        if action == 'navigate':
            assert page.locator('#panel-configs').is_visible()
            control['hold_jobs'] = False
            page.evaluate('window.testCloudPoll()')
            page.wait_for_timeout(80)
            assert page.locator('#log-panel').is_hidden()
            assert page.locator('#panel-configs').is_visible()
    assert not errors


@pytest.mark.parametrize('parameter', ['loop_log', 'open_config', 'open_queue_config', 'open_draft', 'migration_draft_id'])
def test_owned_url_context_prevents_stored_log_restore(log_page, parameter):
    """The owning feature's URL takes precedence over an old queue-log target."""
    page, _, _, errors, _, _, _ = log_page
    open_cloud(page)
    page.goto(page.url.split('#')[0] + '?' + parameter + '=test#queue')
    page.wait_for_function('window.PBGuiOptimizeLog && !state.initialLoadPending')
    page.wait_for_timeout(100)
    assert page.evaluate('state.cloudLogId') is None
    assert page.locator('#log-panel').is_hidden()
    assert not errors


@pytest.mark.parametrize('status', [401, 403])
def test_cache_return_cannot_revive_expired_cloud_session(log_page, status):
    """A cache return must not revive cloud polling after authentication failure."""
    page, _, _, errors, calls, _, control = log_page
    open_cloud(page)
    control['jobs_status'] = status
    page.evaluate('window.testCloudPoll()')
    page.wait_for_function('document.getElementById("balance").textContent === "Authentication required"')
    before = calls.count('/api/vast/jobs')
    page.evaluate("window.dispatchEvent(new PageTransitionEvent('pagehide',{persisted:true}));window.dispatchEvent(new PageTransitionEvent('pageshow',{persisted:true}))")
    page.wait_for_timeout(150)
    assert calls.count('/api/vast/jobs') == before
    assert page.locator('#log-panel').is_hidden()
    assert not errors
