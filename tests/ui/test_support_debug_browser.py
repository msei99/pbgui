"""Real shared menu, support page, preview/download and browser-error metadata."""
import json
from pathlib import Path
from urllib.parse import urlparse

import pytest
from browser_driver import expect, sync_browser

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize('width,prefix', [(1400, ''), (700, '/nested')])
def test_universal_debug_menu_preview_download_and_restore(width, prefix, tmp_path):
    """Navigate normally, retain context, inspect exact downloadable JSON and avoid uploads."""
    report = {'format':'pbgui-support-debug-v1','system':{'pbgui_version':'fixture'},'coverage':{'registered_routes':['/api/example'],
              'sources':[{'source':'service.log','truncated':True}], 'omitted':[{'source':'old.json','reason':'file size limit'}]},
              'records':{'selected_loop':{'status':'failed', 'sample_items':list(range(5000)),
                  'diagnostic_text':'<img src=x onerror="window.reportInjection=true">'+'x'*9000+'END_OF_LONG_TEXT'},
                  **{f'queue/job-{index}':{'status':'completed'} for index in range(125)}},
              'logs':{'service.log':['failure']}}
    from starlette.requests import Request
    from api.support_debug import main_page
    from pbgui_purefunc import PBGUI_VERSION, PBGUI_SERIAL
    html = main_page(Request({'type':'http','root_path':prefix,'path':'/api/debug/main_page','headers':[]}), object()).body.decode()
    errors, posts = [], []
    attachments = []
    from support_debug_crypto import encrypt_report
    encrypted_zip = encrypt_report(json.dumps(dict(report, format='pbgui-support-debug-v1')))
    with sync_browser() as driver:
        browser = driver.chromium.launch()
        page = browser.new_page(viewport={'width':width,'height':850}, accept_downloads=True)
        page.on('pageerror',lambda e: errors.append(str(e)))
        def respond(route):
            """Intercept all requests; no debug collector or real state is accessed."""
            raw = urlparse(route.request.url).path
            path = raw.removeprefix(prefix) if prefix else raw
            if path == '/app/start.html':
                route.fulfill(content_type='text/html; charset=utf-8', body=f'''<nav id="topnav"></nav><script>
                window.PBGUI_BASE_PREFIX={prefix!r};window.PBGUI_NAV_CONFIG={{current:'welcome',authenticated:true}};</script>
                <script src="{prefix}/app/pbgui_nav.js?v=test"></script>''')
            elif path == '/api/debug/main_page':
                route.fulfill(content_type='text/html; charset=utf-8',body=html)
            elif path.startswith('/app/'):
                asset = ROOT/'frontend'/path.removeprefix('/app/')
                route.fulfill(body=asset.read_bytes() if asset.is_file() else b'', content_type='text/css' if asset.suffix=='.css' else 'text/javascript; charset=utf-8')
            elif path == '/api/debug/collect':
                posts.append(route.request.post_data_json)
                assert raw == prefix+'/api/debug/collect'
                route.fulfill(json=report)
            elif path == '/api/debug/issue-attachment':
                attachments.append(route.request.post_data_json)
                assert raw == prefix+'/api/debug/issue-attachment'
                route.fulfill(body=encrypted_zip, content_type='application/zip')
            elif path == '/api/help/index':
                route.fulfill(json=[{'file':'50_debug_export','title':'Debug export'}])
            elif path == '/api/help/content':
                route.fulfill(json={'content':'# Debug export\nSupport diagnostics.'})
            else:
                route.fulfill(json={})
        page.route('**/*', respond)
        page.goto('http://debug.test'+prefix+'/app/start.html')
        page.evaluate("window.dispatchEvent(new ErrorEvent('error',{filename:location.origin+'/app/example.js?token=SECRET',lineno:12,message:'password=SECRET'}))")
        page.locator('.nav-group-btn').filter(has_text='Information').click()
        page.locator('[data-page="info_debug_export"]').click()
        expect(page.locator('#debug-collect')).to_be_visible()
        page.locator('#pbgui-about-btn').click()
        expect(page.locator('#pbgui-about-ver')).to_have_text('PBGui ' + PBGUI_VERSION)
        expect(page.locator('#pbgui-about-serial')).to_have_text('API Serial ' + PBGUI_SERIAL)
        page.locator('#pbgui-about-close').click()
        expect(page.locator('#pbgui-about-ovl')).not_to_have_class('visible')
        assert not posts
        destination = page.url
        page.locator('#pbgui-guide-btn').click()
        expect(page.locator('#pbgui-shared-help-ovl')).to_be_visible()
        assert page.url == destination
        page.locator('#pbgui-shared-help-close').click()
        if width > 760:
            for desired in (160,420,240):
                handle=page.locator('#sidebar-resize').bounding_box()
                page.mouse.move(handle['x']+handle['width']/2,handle['y']+80)
                page.mouse.down();page.mouse.move(desired,handle['y']+80);page.mouse.up()
                assert page.locator('#debug-collect').evaluate('(n)=>n.scrollWidth<=n.clientWidth')
        page.locator('#debug-collect').click()
        expect(page.locator('#debug-status')).to_contain_text('Report ready')
        expect(page.locator('#debug-omitted')).to_contain_text('file size limit')
        page.locator('#debug-tab-report').click()
        page.get_by_role('tab', name='Operational records').click()
        expect(page.locator('.support-report-detail')).to_contain_text('selected_loop')
        expect(page.locator('.support-report-detail')).to_contain_text('failed')
        assert page.locator('.support-report-list > button').count() == 40
        page.locator('.support-report-sources').get_by_role('button', name='Next', exact=True).click()
        expect(page.locator('.support-report-sources')).to_contain_text('41–80 of 126')
        page.get_by_role('searchbox', name='Filter report sources').fill('selected_loop')
        assert page.locator('.support-report-list > button').count() == 1
        sample = page.locator('details.support-report-field').filter(has=page.locator('summary').filter(has_text='sample_items'))
        sample.locator('summary').click()
        expect(sample.locator('.support-report-leaf')).to_have_count(40)
        sample.get_by_role('button', name='Next', exact=True).click()
        expect(sample).to_contain_text('41–80 of 5000')
        expect(sample.locator('.support-report-leaf')).to_have_count(40)
        sample.locator('summary').click()
        expect(sample.locator('.support-report-leaf')).to_have_count(0)
        long_text = page.locator('details.support-report-field').filter(has=page.locator('summary').filter(has_text='diagnostic_text'))
        long_text.locator('summary').click()
        expect(long_text.locator('pre')).to_be_visible()
        assert len(long_text.locator('pre').text_content()) == 4000
        long_text.get_by_role('button', name='Next', exact=True).click()
        long_text.get_by_role('button', name='Next', exact=True).click()
        expect(long_text.locator('pre')).to_contain_text('END_OF_LONG_TEXT')
        assert not page.evaluate('Boolean(window.reportInjection)')
        assert page.locator('#main-content *').count() < 450
        assert page.locator('#debug-raw').count() == 0
        height = page.locator('#debug-structured').bounding_box()['height']
        assert height > 350
        assert abs(height - page.locator('#debug-report').bounding_box()['height']) < 2
        preview = page.evaluate('window.PBGuiSupportExport.getReport()')
        result = json.loads(preview)
        assert result['records'] == report['records'] and result['browser']['events'][0]['line'] == 12
        assert 'SECRET' not in preview
        with page.expect_download() as pending:
            page.locator('#debug-download').click()
        pending.value.save_as(tmp_path/'support.json')
        assert (tmp_path/'support.json').read_text() == preview
        assert posts == [{'loop_id':None}]
        assert page.locator('#main-content').evaluate('(n)=>n.scrollWidth<=n.clientWidth')
        page.evaluate("window.supportOpened=[];window.open=(url)=>{window.supportOpened.push(url);return null;}")
        page.locator('#debug-issue').click()
        modal = page.locator('.support-issue-window')
        expect(modal).to_be_visible()
        assert modal.get_by_role('button', name='Collect debug report', exact=True).count() == 0
        assert posts == [{'loop_id': None}]
        page.mouse.click(2, 2)
        expect(modal).to_be_visible()
        rectangle = modal.bounding_box()
        assert abs(rectangle['x'] + rectangle['width']/2 - width/2) < 3
        assert rectangle['x'] >= 15 and rectangle['y'] >= 15
        assert abs(rectangle['width'] - (width-32)) < 2
        assert abs(rectangle['height'] - (850-32)) < 2
        expect(page.get_by_role('button', name='Prepare encrypted attachment')).to_be_visible()
        assert modal.evaluate('(n)=>n.scrollWidth<=n.clientWidth')
        form_box = modal.locator('.support-issue-fields').bounding_box()
        options_box = modal.locator('.support-issue-options').bounding_box()
        if width > 1000:
            assert options_box['x'] > form_box['x'] + form_box['width']
        else:
            assert options_box['y'] > form_box['y'] + form_box['height'] - 1
        page.locator('#support-issue-subject').fill('Local GPU problem <script>')
        page.locator('#support-issue-description').fill('Validation stopped unexpectedly.')
        page.locator('#support-issue-steps').fill('Start a local Loop.')
        page.locator('#support-issue-expected').fill('Complete validation.')
        assert page.locator('#support-issue-report-preview').count() == 0
        assert modal.locator('.support-report-view').count() == 0
        expect(modal.get_by_text('Public issue preview', exact=True)).to_be_visible()
        expect(page.locator('#support-issue-description')).to_have_value('Validation stopped unexpectedly.')
        page.get_by_role('button', name='Prepare encrypted attachment').click()
        expect(page.get_by_role('button', name='Continue to GitHub')).to_be_visible()
        assert attachments == [result]
        with page.expect_download() as encrypted_download:
            page.get_by_role('button', name='Continue to GitHub').click()
        encrypted_download.value.save_as(tmp_path/'encrypted.zip')
        assert (tmp_path/'encrypted.zip').read_bytes() == encrypted_zip
        from urllib.parse import parse_qs
        page.wait_for_function('window.supportOpened.some(value => typeof value === "string" && value.startsWith("https://github.com/"))')
        url = page.evaluate('window.supportOpened.find(value => typeof value === "string" && value.startsWith("https://github.com/"))')
        assert url.startswith('https://github.com/msei99/pbgui/issues/new?')
        query = parse_qs(urlparse(url).query)
        assert query['title'] == ['Local GPU problem <script>']
        assert query['body'][0] == page.locator('#support-issue-public-preview').input_value()
        assert 'service.log' not in url and 'selected_loop' not in url and 'browser' not in url
        expect(page.get_by_role('link', name='Download attachment again')).to_be_visible()
        expect(page.get_by_role('link', name='Open GitHub issue')).to_be_visible()
        page.locator('#support-issue-description').fill('Changed description')
        expect(page.get_by_role('button', name='Continue to GitHub')).to_be_hidden()
        handle = modal.locator('[data-window-resize="se"]').bounding_box()
        page.mouse.move(handle['x']+handle['width']/2, handle['y']+handle['height']/2)
        page.mouse.down(); page.mouse.move(handle['x']-70, handle['y']-70); page.mouse.up()
        resized = modal.bounding_box()
        assert resized['height'] < rectangle['height']
        header = modal.locator('.pbg-modal-head').bounding_box()
        page.mouse.move(header['x']+90, header['y']+20); page.mouse.down()
        page.mouse.move(header['x']+110, header['y']+30); page.mouse.up()
        assert modal.bounding_box()['x'] > resized['x']
        preferred_geometry = modal.bounding_box()
        page.keyboard.press('Escape')
        expect(modal).to_be_hidden()
        expect(page.locator('#debug-issue')).to_be_focused()
        page.locator('#debug-issue').click()
        expect(page.locator('#support-issue-description')).to_have_value('Changed description')
        page.reload()
        expect(modal).to_be_visible()
        restored_geometry = modal.bounding_box()
        for coordinate in ('x','y','width','height'):
            assert abs(restored_geometry[coordinate] - preferred_geometry[coordinate]) < 2
        page.set_viewport_size({'width':width,'height':600})
        expect(page.get_by_role('button', name='Prepare encrypted attachment')).to_be_visible()
        expect(page.get_by_role('button', name='Close issue report')).to_be_visible()
        assert modal.bounding_box()['height'] <= 568
        page.set_viewport_size({'width':width,'height':850})
        expect(page.locator('#support-issue-description')).to_have_value('')
        expect(page.get_by_role('button', name='Prepare encrypted attachment')).to_be_enabled()
        assert len(posts) == 2
        page.get_by_role('button', name='Close issue report').click()
        # Dialog location persists, while sensitive drafts and report snapshots do not.
        page.reload()
        expect(page.locator('#debug-report')).to_be_visible()
        expect(page.locator('#debug-download')).to_be_disabled()
        expect(page.locator('#debug-structured')).to_contain_text('Collect a report')
        page.locator('#debug-collect').click()
        expect(page.locator('#debug-status')).to_contain_text('Report ready')
        expect(page.get_by_role('tab', name='Operational records')).to_have_attribute('aria-selected', 'true')
        expect(page.get_by_role('searchbox', name='Filter report sources')).to_have_value('selected_loop')
        expect(page.locator('.support-report-detail h2')).to_have_text('selected_loop')
        assert not errors
        browser.close()


def test_failed_loop_debug_entry_keeps_owned_context():
    """The real Loop report header opens the universal page with the selected ID."""
    identifier = 'c' * 32
    source = (ROOT / 'frontend/v7_optimize.html').read_text()
    for key, value in {'API_BASE':'/api/optimize-v8','OPTIMIZE_VERSION':'v8','VERSION':'test',
                       'SERIAL':'1','NAV_HASH':'test','BASE_PREFIX':'','LIMITS_META':'{}',
                       'BACKTEST_VERSION':'v8','OPTIMIZE_NAV_TITLE':'PB8 Optimize',
                       'OPTIMIZE_NAV_CURRENT':'v8_optimize'}.items():
        source = source.replace('%%'+key+'%%', value)
    support = (ROOT / 'frontend/support_debug.html').read_text().replace('"%%API_BASE%%"','"/api/debug"').replace('"%%BASE_PREFIX%%"','""').replace('%%NAV_HASH%%','test')
    run = {'id':identifier,'name':'Failed local GPU','status':'failed','phase':'evaluate','round':0,
           'created_at':1700000000,'updated_at':1700000001,'ended':True,'jobs':[], 'history':[],
           'settings':{'execution':'gpu','config_name':'fixture','provider':'chatgpt','model':'test'},
           'fingerprint':{'pb8':'fixture','data_status':'verified_files'},'reason':'No exact validation',
           'last_error':'Exact comparison required','validation_count':0,'ai_calls':0,'best':None}
    errors = []
    with sync_browser() as driver:
        browser = driver.chromium.launch()
        page = browser.new_page()
        page.on('pageerror',lambda e: errors.append(str(e)))
        def respond(route):
            """Provide a failed isolated run and only local application assets."""
            path = urlparse(route.request.url).path
            if path == '/api/optimize-v8/main_page':
                route.fulfill(body=source,content_type='text/html; charset=utf-8')
            elif path == '/api/debug/main_page':
                route.fulfill(body=support,content_type='text/html; charset=utf-8')
            elif path.startswith('/app/'):
                asset=ROOT/'frontend'/path.removeprefix('/app/')
                route.fulfill(body=asset.read_bytes() if asset.is_file() else b'',content_type='text/css' if asset.suffix=='.css' else 'text/javascript; charset=utf-8')
            elif path == '/api/optimize-v8/loops':
                route.fulfill(json={'loops':[run]})
            elif path == '/api/optimize-v8/loops/options':
                route.fulfill(json={'configs':[],'providers':{},'presets':[],'vast':{}})
            elif path == '/api/optimize-v8/loops/configs':
                route.fulfill(json={'configs':[]})
            elif path == '/api/optimize-v8/loops/instructions':
                route.fulfill(json={'active':'default','revision':0,'versions':[{'id':'default','name':'Default'}]})
            elif path.endswith('/instructions/default'):
                route.fulfill(json={'id':'default','text':'Defaults','digest':'0'})
            elif path == '/api/optimize-v8/metadata':
                route.fulfill(json={'runtime_ready':False,'optimize_defaults':{},'bot_defaults':{},'backtest_defaults':{},'live_defaults':{},'optimize_bounds_defaults':{}})
            elif path == '/api/optimize-v8/configs':
                route.fulfill(json={'configs':[]})
            elif path == '/api/optimize-v8/results':
                route.fulfill(json={'results':[]})
            elif path == '/api/optimize-v8/queue':
                route.fulfill(json={'queue':[]})
            else:
                route.fulfill(json={})
        page.route('**/*',respond)
        page.goto('http://debug.test/api/optimize-v8/main_page#loops-results')
        page.locator('#loops-results-table tr[data-key="'+identifier+'"]').get_by_role('button',name='Open',exact=True).click()
        expect(page.locator('#loop-debug-export')).to_be_visible()
        expect(page.locator('#loop-report-problem')).to_be_visible()
        page.evaluate('window.reportedLoop=null;window.PBGuiSupport.reportError=(context)=>{window.reportedLoop=context;}')
        page.locator('#loop-report-problem').click()
        assert page.evaluate('window.reportedLoop.loop_id') == identifier
        assert page.evaluate('window.reportedLoop.message') == 'No exact validation'
        page.locator('#loop-debug-export').click()
        page.wait_for_url('**/api/debug/main_page?loop_id='+identifier+'#summary')
        expect(page.locator('#debug-context')).to_have_text('Selected Loop: '+identifier[:8])
        assert not errors
        browser.close()
