"""Mounted instruction editor and late-response checks using the real editor module."""
from pathlib import Path
from urllib.parse import urlparse

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize('prefix', ['', '/nested'])
def test_instruction_editor_mount_and_stale_response(prefix):
    """Changing versions rejects delayed old text and polling preserves a dirty editor."""
    playwright = pytest.importorskip('playwright.sync_api')
    custom_id = 'a' * 32
    versions = {'default':{'id':'default','name':'PBGui default','text':'Default text','digest':'0','created_at':None},
                custom_id:{'id':custom_id,'name':'Custom','text':'Custom text','digest':'1','created_at':1}}
    pending, calls, errors = [], [], []
    delay = {'enabled':True}
    base = prefix + '/api/optimize-v8/loops'
    html = '''<div id="sidebar-inner"><hr class="sb-sep"></div><main><div id="panel-configs"></div></main>
    <script>window.PANEL_META={};window.state={panel:'loops-instructions'};
    window.selectPanel=function(panel){state.panel=panel;location.hash=panel;PBGuiLoopInstructions.refresh();};</script>
    <script src="''' + prefix + '''/app/js/pb8_loop_instructions.js"></script>
    <script>PBGuiLoopInstructions.mount("''' + base + '''");PBGuiLoopInstructions.refresh();</script>'''
    with playwright.sync_playwright() as driver:
        browser = driver.chromium.launch(headless=True)
        page = browser.new_page()
        page.on('pageerror', lambda exc: errors.append(str(exc)))
        def respond(route):
            """Serve fixtures only, recording mounted API URLs without external calls."""
            path = urlparse(route.request.url).path
            calls.append(path)
            if path == prefix + '/editor':
                route.fulfill(body=html, content_type='text/html')
            elif path == prefix + '/app/js/pb8_loop_instructions.js':
                route.fulfill(body=(ROOT/'frontend/js/pb8_loop_instructions.js').read_text(), content_type='text/javascript')
            elif path == base + '/instructions':
                route.fulfill(json={'revision':0,'active':'default','versions':[{k:v for k,v in item.items() if k!='text'} for item in versions.values()]})
            elif path == base + '/instructions/default' and delay['enabled']:
                pending.append(route)
            elif path.startswith(base + '/instructions/'):
                route.fulfill(json=versions[path.rsplit('/',1)[1]])
            else:
                route.fulfill(json={}, status=404)
        page.route('**/*', respond)
        page.goto('http://pbgui.test'+prefix+'/editor', wait_until='domcontentloaded')
        page.wait_for_function("document.querySelector('#loop-instruction-version').options.length===2")
        page.locator('#loop-instruction-version').select_option(custom_id)
        page.wait_for_function("document.querySelector('#loop-instruction-text').value==='Custom text'")
        for route in pending:
            route.fulfill(json=versions['default'])
        delay['enabled'] = False
        prompt = page.locator('#loop-instruction-text')
        assert prompt.input_value() == 'Custom text'
        assert prompt.get_attribute('readonly') is not None
        page.locator('#loop-instruction-name').fill('New draft')
        prompt.fill('Unfinished custom edit')
        prompt.focus()
        page.evaluate('PBGuiLoopInstructions.refresh()')
        assert prompt.input_value() == 'Unfinished custom edit'
        assert prompt.evaluate('node=>document.activeElement===node')
        assert 'Unfinished custom edit' not in page.url
        assert page.evaluate('JSON.stringify(localStorage)') == '{}'
        page.reload()
        page.wait_for_function("document.querySelector('#loop-instruction-text').value==='Custom text'")
        assert page.locator('#loop-instruction-version').input_value() == custom_id
        page.locator('#loop-instruction-name').fill('Recovered draft')
        prompt.fill('Keep this unfinished text')
        del versions[custom_id]
        page.evaluate('PBGuiLoopInstructions.refresh()')
        page.wait_for_function("document.querySelector('#loop-instruction-version').selectedOptions[0].textContent.includes('Deleted version')")
        assert prompt.input_value() == 'Keep this unfinished text'
        assert page.get_by_role('button', name='Save & use new version', exact=True).is_enabled()
        assert page.get_by_role('button', name='Delete selected version', exact=True).is_disabled()
        page.reload()
        page.wait_for_function("document.querySelector('#loop-instruction-text').value==='Default text'")
        assert page.locator('#loop-instruction-version').input_value() == 'default'
        assert all(path.startswith(prefix + '/') for path in calls)
        assert not errors, errors
        browser.close()
