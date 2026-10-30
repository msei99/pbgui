"""Official error handoff and selective encrypted-attachment UX use isolated browser fixtures."""
import json
from pathlib import Path
from urllib.parse import urlparse, parse_qs

import pytest
from browser_driver import sync_browser, expect

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize('width', [1400, 700])
def test_error_dialog_handoff_selection_and_private_context(width):
    """Report an error without moving the source page, then select only relevant diagnostics."""
    identifier = 'e' * 32
    report = {'format':'pbgui-support-debug-v1', 'system':{'pbgui_version':'fixture'},
        'error_context':{'area':'vps_manager','message':'Failure password=[REDACTED]','detail':'fixture'},
        'records':{'vps/status':{'status':'failed'}, 'opt_v8_queue/job':{'status':'OPTIMIZER_ONLY'}},
        'logs':{'PBGui.log':['Core diagnostic'], 'VPS.log':['VPS_DIAGNOSTIC'], 'PBOptimize.log':['OPTIMIZER_DIAGNOSTIC']},
        'databases':[{'source':'history.sqlite','quick_check':['ok']}],
        'coverage':{'registered_routes':[], 'sources':[], 'omitted':[]}}
    html = (ROOT/'frontend/support_debug.html').read_text().replace('"%%API_BASE%%"','"/mounted/api/debug"').replace('"%%BASE_PREFIX%%"','"/mounted"').replace('%%NAV_HASH%%','test')
    html = html.replace('src="/app/', 'src="/mounted/app/').replace('href="/app/', 'href="/mounted/app/')
    contexts, collections, attachments, errors = [], [], [], []
    with sync_browser() as driver:
        browser = driver.chromium.launch()
        page = browser.new_page(viewport={'width':width,'height':850})
        page.on('pageerror',lambda error: errors.append(str(error)))
        def respond(route):
            """Never touch runtime files or send requests to GitHub."""
            path = urlparse(route.request.url).path.removeprefix('/mounted')
            if path == '/start':
                route.fulfill(content_type='text/html',body='''<nav id="topnav"></nav><script>
                window.PBGUI_BASE_PREFIX='/mounted';window.PBGUI_NAV_CONFIG={current:'vps_manager',authenticated:true};
                </script><script src="/mounted/app/pbgui_nav.js"></script><script src="/mounted/app/js/pbgui_dialogs.js"></script>''')
            elif path.startswith('/app/'):
                asset = ROOT/'frontend'/path.removeprefix('/app/')
                route.fulfill(body=asset.read_bytes() if asset.is_file() else b'', content_type='text/css' if asset.suffix=='.css' else 'text/javascript')
            elif path == '/api/debug/error-context':
                contexts.append(route.request.post_data_json); route.fulfill(json={'context_id':identifier})
            elif path == '/api/debug/main_page':
                route.fulfill(body=html,content_type='text/html')
            elif path == '/api/debug/collect':
                collections.append(route.request.post_data_json); route.fulfill(json=report)
            elif path == '/api/debug/issue-attachment':
                attachments.append(route.request.post_data_json); route.fulfill(body=b'PK-fixture',content_type='application/zip')
            else:
                route.fulfill(json={})
        page.route('**/*',respond)
        page.goto('http://support.test/mounted/start')
        page.evaluate("window.reportDestinations=[];window.open=()=>({opener:null,closed:false,close(){},location:{replace(url){window.reportDestinations.push(url)}}})")
        page.evaluate("void window.PBGuiDialogs.alert({title:'Information',message:'Normal notice'})")
        expect(page.locator('#pbgui-dialog-report')).to_be_hidden()
        page.evaluate("void window.PBGuiDialogs.alert({title:'Failed',level:'error',message:'Failure password=TOP_SECRET',detail:'stack fixture'})")
        expect(page.get_by_role('button',name='Report this problem')).to_be_visible()
        expect(page.locator('#pbgui-report-error-btn')).to_be_visible()
        assert page.locator('#topnav').evaluate('(n)=>n.scrollWidth<=n.clientWidth')
        page.get_by_role('button',name='Report this problem').click()
        page.wait_for_function('window.reportDestinations.length === 1')
        assert contexts == [{'area':'vps_manager','message':'Failure password=TOP_SECRET','detail':'stack fixture'}]
        destination = page.evaluate('window.reportDestinations[0]')
        assert page.url.endswith('/mounted/start')
        assert parse_qs(urlparse(destination).query) == {'context':[identifier]}
        assert 'TOP_SECRET' not in destination
        page.goto(destination)
        modal = page.locator('.support-issue-window')
        expect(modal).to_be_visible()
        expect(page.locator('#support-issue-subject')).to_have_value('Problem in VPS Manager')
        assert collections == [{'loop_id':None,'context_id':identifier}]
        selected = page.evaluate('window.PBGuiSupportExport.getData()')
        assert 'vps/status' in selected['records'] and 'opt_v8_queue/job' not in selected['records']
        assert 'VPS.log' in selected['logs'] and 'PBOptimize.log' not in selected['logs']
        assert selected['system'] == report['system'] and selected['error_context'] == report['error_context']
        assert 'TOP_SECRET' not in page.evaluate('JSON.stringify({...sessionStorage,...localStorage})')
        page.locator('#support-issue-description').fill('The operation failed.')
        page.get_by_role('button',name='Prepare encrypted attachment').click()
        expect(page.get_by_role('button',name='Continue to GitHub')).to_be_visible()
        assert attachments == [selected]
        expect(modal.locator('[data-attachment-group="vps"]')).to_be_visible()
        modal.locator('[data-attachment-group="vps"]').click()
        expect(page.get_by_role('button',name='Continue to GitHub')).to_be_hidden()
        smaller = page.evaluate('window.PBGuiSupportExport.getData()')
        assert 'vps/status' not in smaller['records'] and 'VPS.log' not in smaller['logs']
        assert smaller['error_context'] == selected['error_context']
        modal.locator('[data-attachment-group="optimize"]').click()
        expanded = page.evaluate('window.PBGuiSupportExport.getData()')
        assert 'opt_v8_queue/job' in expanded['records'] and 'PBOptimize.log' in expanded['logs']
        assert 'OPTIMIZER_ONLY' not in page.locator('#support-issue-public-preview').input_value()
        assert not errors
        browser.close()
