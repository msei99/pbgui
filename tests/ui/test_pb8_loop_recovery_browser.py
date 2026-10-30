"""Offline real-renderer regressions for visible AI Loop recovery operations."""
from pathlib import Path
from urllib.parse import urlparse
import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize('outcome', ['success', 'preview_error', 'resume_error', 'selection_error', 'cancel_paid', 'cancel_interrupted', 'lifecycle'])
def test_recovery_feedback_is_immediate_and_scoped(outcome):
    """Pending work, errors and approvals remain visible in the attached report."""
    browser_api = __import__('browser_driver')
    source = (ROOT / 'frontend/v7_optimize.html').read_text()
    for key, value in {'API_BASE':'/api/optimize-v8', 'OPTIMIZE_VERSION':'v8', 'VERSION':'test',
        'SERIAL':'1', 'NAV_HASH':'test', 'BASE_PREFIX':'', 'LIMITS_META':'{}',
        'OPTIMIZE_NAV_TITLE':'PB8 Optimize', 'OPTIMIZE_NAV_CURRENT':'v8_optimize'}.items():
        source = source.replace('%%'+key+'%%', value)
    identity = 'a'*32
    row = {'id':identity, 'name':'Isolated recovery', 'status':'paused', 'phase':'evaluate', 'round':0,
        'ended':False, 'created_at':1000, 'updated_at':1001, 'settings':{'config_name':'fixture',
        'execution':'cpu', 'historical_advice':True, 'ai_decision_token_limit':1000, 'ai_decision_usd_limit':1},
        'jobs':[], 'observer_jobs':[], 'history':[], 'cycles':[], 'rubric':[], 'best':None,
        'fingerprint':{}, 'ai_pause':{'reason':'evidence_recovery','message':'Fixture paused'},
        'has_interrupted_ai':outcome=='cancel_interrupted'}
    preview = {'context':1000000, 'native_archive':{'effective':500}, 'streaming':True,
               'paid':outcome=='cancel_paid', 'maximum_decisions':5, 'maximum_attempts':5}
    previews, actions, errors = [], [], []
    with browser_api.sync_browser() as driver:
        browser = driver.chromium.launch(headless=True)
        page = browser.new_page(viewport={'width':1200,'height':900})
        page.set_default_timeout(5000)
        page.on('pageerror', lambda error: errors.append(str(error)))

        def respond(route):
            """Hold recovery responses while serving only synthetic records and local assets."""
            path = urlparse(route.request.url).path
            if path == '/api/optimize-v8/main_page':
                route.fulfill(body=source, content_type='text/html')
            elif path.startswith('/app/'):
                asset = ROOT / 'frontend' / path.removeprefix('/app/')
                route.fulfill(body=asset.read_bytes() if asset.is_file() else b'',
                    content_type='text/css' if asset.suffix=='.css' else 'text/javascript')
            elif path == '/api/optimize-v8/loops':
                route.fulfill(json={'loops':[row]})
            elif path == '/api/optimize-v8/loops/configs':
                route.fulfill(json={'configs':[]})
            elif path == '/api/optimize-v8/loops/'+identity:
                route.fulfill(json=row)
            elif path.endswith('/ai-preview'):
                previews.append(route)
            elif path.endswith('/action'):
                actions.append(route)
            elif path == '/api/help/index':
                route.fulfill(json=[])
            elif path.endswith('/options'):
                route.fulfill(json={'configs':[], 'vast':{}})
            else:
                route.fulfill(json={})

        page.route('**/*', respond)
        page.route_web_socket('**/*', lambda socket: socket.on_message(lambda message: None))
        selection = "throw new Error('Fixture model unavailable');" if outcome=='selection_error' else "return {provider:'claude-code',model:'fixture'};"
        page.add_init_script("window.PBGuiAI={getSelection:async function(){"+selection+"}};")
        page.goto('http://pbgui.test/api/optimize-v8/main_page?loop_id='+identity+'&loop_view=run&loop_tab=overview#loops-queue')
        form = page.locator('#loop-report-body [data-loop-recovery]')
        try:
            form.wait_for()
        except Exception:
            pytest.fail(str(errors)+'; '+page.locator('#loop-report-body').inner_text()+'; '+page.locator('#loops-queue-message').inner_text())
        field = form.locator('input[type=number]').first
        field.fill('2000')
        status = form.locator('[data-recovery-status]')
        control = form.locator('[data-recovery-action='+('preview' if outcome in {'success','preview_error','selection_error'} else 'resume')+']')
        control.click()
        if outcome=='selection_error':
            page.wait_for_function("document.querySelector('[data-recovery-status]').textContent.includes('Fixture model unavailable')")
            assert status.get_attribute('role') == 'alert'
            assert not previews and not actions
        else:
            page.wait_for_function("document.querySelector('[data-recovery-status]').textContent==='Preparing AI preview…'")
            assert not control.is_enabled() and not field.is_enabled()
            assert len(previews)==1 and not actions
            page.wait_for_timeout(200)
            if outcome=='lifecycle':
                page.evaluate("window.dispatchEvent(new PageTransitionEvent('pagehide',{persisted:true}));window.dispatchEvent(new PageTransitionEvent('pageshow',{persisted:true}));")
                page.wait_for_function("!document.querySelector('[data-recovery-action=resume]').disabled")
                page.wait_for_timeout(250)
                assert not actions and not errors
                assert field.input_value()=='2000'
                browser.close()
                return
            if outcome=='preview_error':
                previews[0].fulfill(status=422,json={'detail':'Fixture preview rejected'})
                page.wait_for_function("document.querySelector('[data-recovery-status]').textContent.includes('Fixture preview rejected')")
                assert status.get_attribute('role') == 'alert'
            else:
                previews[0].fulfill(json=preview)
                if outcome=='success':
                    page.wait_for_function("document.querySelector('[data-recovery-status]').textContent==='AI preview ready.'")
                    assert '1000000' in form.locator('[data-recovery-preview]').inner_text()
                    field.fill('3000')
                    assert 'limits changed' in status.inner_text()
                    assert not form.locator('[data-recovery-preview]').inner_text()
                    form.get_by_role('button',name='Resume with current AI selection').click()
                    page.wait_for_function("document.querySelector('[data-recovery-status]').textContent==='Preparing AI preview…'")
                    assert len(previews)==2
                    previews[1].fulfill(json=preview)
                if outcome.startswith('cancel_'):
                    page.locator('#pbgui-dialog-cancel').click()
                    page.wait_for_function("document.querySelector('[data-recovery-status]').textContent==='Action cancelled.'")
                    assert not actions
                else:
                    page.wait_for_function("document.querySelector('[data-recovery-status]').textContent==='Resuming Loop…'")
                    assert len(actions)==1 and not control.is_enabled()
                    if outcome=='resume_error':
                        actions[0].fulfill(status=409,json={'detail':'Fixture state changed'})
                        page.wait_for_function("document.querySelector('[data-recovery-status]').textContent.includes('Fixture state changed')")
                        assert status.get_attribute('role') == 'alert'
                    else:
                        row.update(status='running', updated_at=1002)
                        actions[0].fulfill(json=row)
                        page.wait_for_function("!document.querySelector('[data-loop-recovery]')")
                        assert 'running' in page.locator('#loop-report-body').inner_text()
        if outcome!='success':
            assert field.input_value()=='2000' and field.is_enabled()
            assert control.is_enabled()
        assert not errors, errors
        browser.close()
