"""Isolated real-page regressions for VPS overview controls and Linux update dialogs."""
import re
from pathlib import Path
from urllib.parse import urlparse

import pytest
from browser_driver import expect, sync_browser

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(params=['', '/mounted'])
def detail_manager(request):
    """Open the real menu at root/mount with fully intercepted traffic and fake hosts."""
    prefix = request.param
    html = (ROOT / 'frontend/vps_manager.html').read_text()
    for key, value in {'API_BASE': prefix + '/api/vps-manager',
                       'WS_BASE': 'ws://manager.test' + prefix,
                       'VERSION': 'test', 'SERIAL': '1', 'NAV_HASH': 'test'}.items():
        html = html.replace('%%' + key + '%%', value)
    html = html.replace('/app/', prefix + '/app/')
    fixture = {'prefix': prefix, 'mode': 'success', 'pending': [], 'posts': [], 'gets': []}
    with sync_browser() as driver:
        browser = driver.chromium.launch(headless=True)
        context = browser.new_context(viewport={'width': 1450, 'height': 1000})
        page = context.new_page()
        page.set_default_timeout(5000)
        errors = []
        page.on('pageerror', lambda error: errors.append(str(error)))
        page.add_init_script('''window.WebSocket=class {
          static OPEN=1; static CONNECTING=0;
          constructor() {this.readyState=1; setTimeout(()=>this.onopen?.(),0);}
          send() {} close() {}
        };''')

        def respond(route):
            """No request reaches real services; detail replies can be released after a prompt opens."""
            path = urlparse(route.request.url).path
            if path == prefix + '/app/start.html':
                route.fulfill(content_type='text/html', body=f'''<nav id="topnav"></nav><script>
                window.PBGUI_NAV_CONFIG={{current:'welcome',authenticated:true,apiBase:'{prefix}/api'}};
                </script><script src="{prefix}/app/pbgui_nav.js?v=test"></script>''')
            elif path == prefix + '/api/vps-manager/main_page':
                route.fulfill(content_type='text/html', body=html)
            elif path.startswith(prefix + '/app/'):
                asset = ROOT / 'frontend' / path.removeprefix(prefix + '/app/')
                route.fulfill(status=200 if asset.is_file() else 404,
                              body=asset.read_bytes() if asset.is_file() else b'',
                              content_type='text/css' if asset.suffix == '.css' else 'text/javascript')
            elif path.startswith(prefix + '/api/vps-manager/detail/'):
                fixture['gets'].append(path)
                if fixture['mode'] == 'hold':
                    fixture['pending'].append(route)
                elif fixture['mode'] == 'missing':
                    route.fulfill(status=404, json={})
                else:
                    route.fulfill(json=fixture['detail'])
            elif path.endswith('/notify_log'):
                fixture['posts'].append((path, route.request.method, route.request.post_data_json))
                route.fulfill(json={})
            elif path == prefix + '/api/help/index':
                route.fulfill(json=[{'file': '32_vps_manager', 'title': 'VPS Manager'}])
            elif path == prefix + '/api/help/content':
                route.fulfill(json={'content': '# VPS Manager\n\nSynthetic guide.'})
            elif path.endswith('/stream'):
                route.fulfill(status=204, body='')
            else:
                route.fulfill(json={})

        page.route('**/*', respond)
        page.goto('http://manager.test' + prefix + '/app/start.html')
        page.locator('.nav-group-btn', has_text='System').click()
        page.locator('.nav-item[data-page="system_vps_manager_fastapi"]').click()
        expect(page.locator('#sidebar')).to_be_visible()
        assert page.url == 'http://manager.test' + prefix + '/api/vps-manager/main_page'
        publish_state(page)
        fixture['detail'] = page.evaluate("buildProvisionalVpsDetail('test-host')")
        fixture['detail'].pop('provisional')
        fixture['detail']['status']['pb8_installed'] = True
        try:
            yield page, fixture
            assert not errors
        finally:
            context.close()
            browser.close()


def test_vps_detail_delayed_failure_preserves_real_dialog(detail_manager):
    """A delayed detail error stays above a real prompt without replacing input or its callback."""
    __tracebackhide__ = True
    page, fixture = detail_manager
    fixture['mode'] = 'hold'
    page.locator('[data-vps-action="select-vps"][data-host="test-host"]').click()
    # Wait for the actual scheduled entry request, without directly invoking the fetch helper.
    page.wait_for_timeout(3200)
    assert len(fixture['pending']) == 1
    page.evaluate('''() => {
      window.confirmed=0;
      openMasterPasswordPrompt(()=>{window.confirmed++;},'Synthetic no-op');
      window.promptInput=document.getElementById('master-pw-prompt-input');
      window.promptBody=document.getElementById('alertModalBody');
      window.promptConfirm=document.getElementById('master-pw-prompt-confirm');
      window.promptButtons=document.getElementById('alertModalBtnRow').innerHTML;
      window.cachedDetail=store.detail; window.cachedVpsUi=store.vps['test-host'];
    }''')
    input_box = page.locator('#master-pw-prompt-input')
    expect(input_box).to_be_focused()
    input_box.fill('nonsecret-fixture')
    fixture['pending'].pop().fulfill(status=500, body='Sensitive fixture body must not appear')
    notice = page.locator('#dialogToastContainer .toast.error')
    expect(notice).to_have_text('Failed to load VPS details. Please try again.')
    expect(input_box).to_have_value('nonsecret-fixture')
    expect(input_box).to_be_focused()
    expect(page.locator('#alertModalTitle')).to_have_text('Master sudo password required')
    assert page.evaluate('''() => promptInput===document.getElementById('master-pw-prompt-input')
      && promptBody===document.getElementById('alertModalBody')
      && promptConfirm===document.getElementById('master-pw-prompt-confirm')
      && promptButtons===document.getElementById('alertModalBtnRow').innerHTML
      && cachedDetail===store.detail && cachedVpsUi===store.vps['test-host'] && confirmed===0''')
    assert page.evaluate('''() => Number(getComputedStyle(document.getElementById('dialogToastContainer')).zIndex)
      > Number(getComputedStyle(document.getElementById('alertModalOverlay')).zIndex)''')
    page.wait_for_timeout(50)
    # Shared navigation also records console.error with level "err".
    toast_posts = [post for post in fixture['posts'] if post[2].get('level') == 'error']
    assert toast_posts == [(fixture['prefix'] + '/api/notify_log', 'POST',
                           {'msg': 'Failed to load VPS details. Please try again.', 'level': 'error'})]
    assert all(post[0] == fixture['prefix'] + '/api/notify_log' for post in fixture['posts'])
    assert all('nonsecret-fixture' not in str(post) for post in fixture['posts'])
    input_box.press('Enter')
    assert page.evaluate('confirmed') == 1
    expect(page.locator('#alertModalOverlay')).not_to_have_class('visible')


def test_vps_detail_error_and_404_reload(detail_manager):
    """Network failure keeps its normal modal; a missing host explains and persists Overview."""
    __tracebackhide__ = True
    page, fixture = detail_manager
    fixture['mode'] = 'hold'
    page.locator('[data-vps-action="select-vps"][data-host="test-host"]').click()
    page.wait_for_timeout(3200)
    assert len(fixture['pending']) == 1
    fixture['pending'].pop().abort('failed')
    expect(page.locator('#alertModalTitle')).to_have_text('Error')
    expect(page.locator('#alertModalBody')).to_have_text('Failed to load VPS details. Please try again.')
    page.locator('#alertModalClose').click()
    fixture['mode'] = 'missing'
    # Returning to a view uses the existing automatic context request.
    page.locator('#sidebar-actions').get_by_role('button', name='Overview', exact=True).click()
    page.locator('[data-vps-action="select-vps"][data-host="test-host"]').click()
    expect(page.locator('#alertModalBody')).to_have_text('This VPS is no longer available. Returning to Overview.')
    assert page.url.endswith('#overview')
    assert page.evaluate('[store.view,store.hostname]') == ['overview', '']
    page.reload()
    publish_state(page)
    expect(page.locator(OVERVIEW)).to_be_visible()
    assert page.url.endswith('#overview')
    assert page.evaluate('[store.view,store.hostname]') == ['overview', '']


def test_vps_detail_automatic_updates_navigation_and_controls(detail_manager):
    """Real renderers retain drafts and branch actions through updates, return and reload."""
    __tracebackhide__ = True
    page, fixture = detail_manager
    page.locator('[data-vps-action="select-vps"][data-host="test-host"]').click()
    page.locator('#sidebar-actions').get_by_role('button', name='PBGui Branch', exact=True).click()
    expect(page.locator('#viewRoot')).to_contain_text('VPS PBGui Branch Management')
    page.wait_for_timeout(3200)
    assert len(fixture['gets']) >= 1
    for repo in ['PBGui', 'PB7', 'PB8']:
        page.locator('#sidebar-actions').get_by_role('button', name=repo + ' Branch', exact=True).click()
        if repo == 'PBGui':
            expect(page.locator('#viewRoot')).to_contain_text('No branch history loaded yet.')
            expect(page.locator('#viewRoot').get_by_role('button', name='+50', exact=True)).to_be_visible()
            expect(page.locator('#viewRoot').get_by_role('button', name='All', exact=True)).to_be_visible()
        else:
            # PB7/PB8 supply their default branch even before history arrives.
            expect(page.locator('#viewRoot')).to_contain_text('VPS ' + repo + ' Branch Management')
            assert page.evaluate('''repo => {
              const detail=JSON.parse(JSON.stringify(store.detail));
              return renderPb7BranchPanel('test-host',detail.branches[repo]||{},repo).includes('Use Refresh');
            }''', repo.lower()) is False
        expect(page.locator('#viewRoot')).not_to_contain_text('Use Refresh')
        expect(page.get_by_role('button', name='Reload', exact=True)).to_have_count(0)
    page.evaluate("store.state.config={swap_options:['0','1G']}")
    page.locator('#sidebar-actions').get_by_role('button', name='Change VPS', exact=True).click()
    expect(page.locator('#viewRoot [data-vps-field="install_dir"]')).to_be_visible()
    page.evaluate('''() => {
      const input=document.querySelector('#viewRoot [data-vps-field="install_dir"]');
      window.editInput=input; input.focus(); input.value='synthetic-unsaved';
      input.dispatchEvent(new Event('input',{bubbles:true}));
      window.editValue=input.value;
      window.fixtureUpdated=JSON.parse(JSON.stringify(store.detail));
      fixtureUpdated.status.online=true;
      handleMessage({type:'detail',context_generation:store.contextGeneration,data:fixtureUpdated});
    }''')
    assert page.evaluate('''() => editInput===document.activeElement && editInput.isConnected
      && editInput.value===editValue && store.view==='vps-setup' && store.hostname==='test-host' ''')
    page.locator('#sidebar-actions').get_by_role('button', name='PBGui Branch', exact=True).click()
    destination = page.url
    for width in [759, 761, 1450]:
        page.set_viewport_size({'width': width, 'height': 1000})
        for key, sidebar_width in [('Home', 160), ('End', 420)]:
            if width >= 760:
                page.locator('#sidebar-resize').focus()
                page.keyboard.press(key)
                assert page.locator('#sidebar').bounding_box()['width'] == sidebar_width
            else:
                # Shared narrow layout stacks the sidebar at the full viewport width.
                assert page.locator('#sidebar').bounding_box()['width'] == width
            for selector in ['#sidebar-actions', '#sidebar-sticky', '#sidebar-header']:
                assert page.locator(selector).evaluate('(e)=>e.scrollWidth<=e.clientWidth')
        page.locator('#pbgui-guide-btn').click()
        expect(page.locator('#pbgui-shared-help-ovl')).to_have_class('visible')
        for label in ['en', 'de']:
            expect(page.locator('#pbgui-shared-help-lang-' + label)).to_be_visible()
        assert page.locator('#pbgui-shared-help-box .lang-pill').evaluate('(e)=>e.scrollWidth<=e.clientWidth')
        assert page.url == destination
        page.locator('#pbgui-shared-help-close').click()
    expect(page.locator('#viewRoot').get_by_role('button', name='Already Selected', exact=True)).to_be_visible()
    page.locator('#pbgui-guide-btn').click()
    expect(page.locator('#pbgui-shared-help-ovl')).to_have_class('visible')
    assert page.url == destination
    page.reload()
    publish_state(page)
    assert page.url == destination
    expect(page.locator('#viewRoot')).to_contain_text('VPS PBGui Branch Management')
    assert page.locator('#sidebar').bounding_box()['width'] == 420
    page.locator('#sidebar-actions').get_by_role('button', name='Overview', exact=True).click()
    expect(page.get_by_role('button', name='Refresh', exact=True)).to_have_count(0)
    page.locator('[data-vps-action="select-vps"][data-host="test-host"]').click()
    page.locator('#sidebar-actions').get_by_role('button', name='PBGui Branch', exact=True).click()
    count = len(fixture['gets'])
    page.wait_for_timeout(3200)
    assert len(fixture['gets']) > count
    assert page.url == destination
    count = len(fixture['gets'])
    page.evaluate('''() => {
      window.previousSocket=store.ws;
      store.ws.readyState=3; store.ws.onclose({code:1006});
    }''')
    page.wait_for_function('store.ws!==previousSocket && store.connected')
    page.wait_for_timeout(3200)
    assert len(fixture['gets']) > count
    assert page.url == destination
    assert page.evaluate('[store.hostname,store.view]') == ['test-host', 'vps-pbgui-branch']


@pytest.fixture
def manager(request):
    """Serve actual navigation/assets with synthetic state and no remote connections."""
    html = (ROOT / 'frontend/vps_manager.html').read_text()
    for key, value in {'API_BASE': '/prefix/api/vps-manager', 'WS_BASE': 'ws://manager.test/prefix',
                       'VERSION': 'test', 'SERIAL': '1', 'NAV_HASH': 'test'}.items():
        html = html.replace('%%' + key + '%%', value)
    html = html.replace('/app/', '/prefix/app/')
    with sync_browser() as driver:
        browser = driver.chromium.launch(headless=True)
        context = browser.new_context(viewport={'width': 1450, 'height': 1000})
        page = context.new_page()
        page.set_default_timeout(5000)
        errors = []
        page.on('pageerror', lambda error: errors.append(str(error)))
        page.add_init_script('''window.WebSocket = class {
          static OPEN=1; static CONNECTING=0;
          constructor() { this.readyState=1; } send() {} close() {}
        };''')
        if getattr(request, 'param', None) == 'reverse':
            page.add_init_script('''window.testKeyListeners = [];
              const add = EventTarget.prototype.addEventListener;
              window.testOriginalAddListener = add;
              EventTarget.prototype.addEventListener = function(type, listener, options) {
                if (type === 'keydown' && (this === window || this === document))
                  window.testKeyListeners.push({target:this,listener,options});
                return add.call(this,type,listener,options);
              };''')

        def respond(route):
            """Intercept all HTTP traffic; no VPS or production API can be contacted."""
            path = urlparse(route.request.url).path
            if path == '/prefix/app/start.html':
                route.fulfill(content_type='text/html', body='''<nav id="topnav"></nav><script>
                window.PBGUI_NAV_CONFIG={current:'welcome',authenticated:true,apiBase:'/prefix/api'};
                </script><script src="/prefix/app/pbgui_nav.js?v=test"></script>''')
            elif path == '/prefix/api/vps-manager/main_page':
                route.fulfill(content_type='text/html', body=html)
            elif path.startswith('/prefix/app/'):
                asset = ROOT / 'frontend' / path.removeprefix('/prefix/app/')
                if asset.is_file():
                    route.fulfill(body=asset.read_bytes(), content_type='text/css' if asset.suffix == '.css' else 'text/javascript')
                else:
                    route.fulfill(status=404, body='')
            elif path == '/prefix/api/help/index':
                route.fulfill(json=[{'file': '32_vps_manager', 'title': 'VPS Manager'}])
            elif path == '/prefix/api/help/content':
                route.fulfill(json={'content': '# VPS Manager\n\nLinux updates.'})
            elif path.endswith('/stream'):
                route.fulfill(status=204, body='')
            else:
                route.fulfill(json={})

        page.route('**/*', respond)
        page.goto('http://manager.test/prefix/app/start.html')
        page.locator('.nav-group-btn', has_text='System').click()
        page.locator('.nav-item[data-page="system_vps_manager_fastapi"]').click()
        expect(page.locator('#sidebar')).to_be_visible()
        try:
            yield page
            assert not errors
        finally:
            context.close()
            browser.close()


def publish_state(page, **changes):
    """Feed a real state-message handler with a complete synthetic package snapshot."""
    status = dict(state='ok', available=True, upgrades=5, installable_updates=5,
                  classification_complete=True, details_complete=True, security_updates=3,
                  kernel_updates=0, routine_updates=2, new_installs=0, removals=0, deferred_updates=0,
                  urgency='security', packages=[dict(name=f'package-{i}', installed_version='1',
                    candidate_version='2', security=i < 3, source='Ubuntu', removed=False) for i in range(5)])
    status.update(changes)
    page.evaluate('''status => handleMessage({type:'state',data:{overview:{rows:[{
      name:'test-host',hostname:'test-host',nav:'vps',role:'slave',online:true,
      package_status:status}]},vps:[],deploys:{}}})''', status)


OVERVIEW = '#overview-table-live'
OVERVIEW_PREFS = 'pbgui_vps_manager_overview_prefs_v1'


def publish_overview(page, revision=0):
    """Send two isolated hosts through the real live handler so sorting is observable."""
    page.evaluate('''revision => handleMessage({type:'state',data:{overview:{rows:[
      {name:'test-alpha',hostname:'test-alpha',nav:'vps',role:'slave',online:true,
       running_bots:revision,pbgui:'1.0',pb7:'7.0',pb8:'8.0'},
      {name:'test-zulu',hostname:'test-zulu',nav:'vps',role:'slave',online:true,
       running_bots:revision+1,pbgui:'1.0',pb7:'7.0',pb8:'8.0'}
    ]},vps:[],deploys:{}}})''', revision)
    expect(page.locator(OVERVIEW + ' tbody tr')).to_have_count(2)


def overview_preferences(page):
    """Read only the non-secret view preferences belonging to this isolated origin."""
    return page.evaluate('(key) => JSON.parse(localStorage.getItem(key))', OVERVIEW_PREFS)


def overview_button_point(button, offset=0):
    """Check actual hit testing after scrolling the real control into its container."""
    button.scroll_into_view_if_needed()
    return button.evaluate('''(button, offset) => {
      const rect = button.getBoundingClientRect();
      const x = rect.x + rect.width / 2 + offset, y = rect.y + rect.height / 2;
      if (document.elementFromPoint(x, y)?.closest('button') !== button)
        throw new Error('Overview button target is clipped or covered');
      return {x, y};
    }''', offset)


def click_overview_button(page, button, offset=0):
    """Use a real pointer click at the verified center or near-center coordinate."""
    point = overview_button_point(button, offset)
    page.mouse.click(point['x'], point['y'])


def assert_overview_targets(page):
    """Check every rendered target, glyph centering, edges and adjacent sort labels."""
    buttons = page.locator(OVERVIEW + ' .th-icon-btn')
    for index in range(buttons.count()):
        button = buttons.nth(index)
        button.scroll_into_view_if_needed()
        metrics = button.evaluate('''button => {
          const rect = button.getBoundingClientRect();
          const range = document.createRange(); range.selectNodeContents(button);
          const glyph = range.getBoundingClientRect();
          const points = [[0,0],[-7,0],[7,0],[-rect.width/2+.5,0],
            [rect.width/2-.5,0],[0,-rect.height/2+.5],[0,rect.height/2-.5]];
          return {width:rect.width,height:rect.height,
            dx:Math.abs((glyph.left+glyph.right-rect.left-rect.right)/2),
            dy:Math.abs((glyph.top+glyph.bottom-rect.top-rect.bottom)/2),
            reachable:points.every(([x,y]) => document.elementFromPoint(
              rect.x+rect.width/2+x,rect.y+rect.height/2+y)?.closest('button') === button)};
        }''')
        assert metrics['width'] >= 24, metrics
        assert metrics['height'] >= 24, metrics
        assert metrics['dx'] <= 1 and metrics['dy'] <= 1, metrics
        assert metrics['reachable'], metrics
    labels = page.locator(OVERVIEW + ' th.sortable .th-sort-main')
    for index in range(labels.count()):
        label = labels.nth(index)
        label.scroll_into_view_if_needed()
        assert label.evaluate('''label => {
          const r = label.getBoundingClientRect();
          return [r.left+1,(r.left+r.right)/2,r.right-1].every(x => {
            const hit = document.elementFromPoint(x,(r.top+r.bottom)/2);
            return hit?.closest('th') === label.closest('th') && !hit.closest('button');
          });
        }'''), 'Enlarged buttons must not cover adjacent sort labels'


@pytest.mark.parametrize('offset', [0, -7, 7])
def test_overview_hide_targets_do_not_sort(manager, offset):
    """All 16 hide controls and reset accept center/±7px clicks without stray sorting."""
    page = manager
    publish_overview(page)
    names = page.locator(OVERVIEW + ' button[title="Hide column"]').evaluate_all(
        '(buttons) => buttons.map(button => button.getAttribute("aria-label"))')
    assert len(names) == 16
    for name in names:
        # Deliberately sort via the label outside the enlarged button target.
        page.locator(OVERVIEW + ' .th-sort-main').first.click(position={'x': 2, 'y': 2})
        assert overview_preferences(page)['sort'] == {'field': 'name', 'dir': 'desc'}
        assert page.locator(OVERVIEW + ' tbody tr').first.get_attribute('data-overview-host') == 'test-zulu'
        button = page.get_by_role('button', name=name, exact=True)
        click_overview_button(page, button, offset)
        expect(button).to_have_count(0)
        preferences = overview_preferences(page)
        assert preferences['sort'] == {'field': 'name', 'dir': 'desc'}
        assert sum(value is False for value in preferences['visible_columns'].values()) == 1
        click_overview_button(page, page.get_by_role('button', name='Reset view', exact=True), offset)
        expect(page.locator(OVERVIEW + ' button[title="Hide column"]')).to_have_count(16)
        assert overview_preferences(page)['sort'] == {'field': 'name', 'dir': 'asc'}
        assert all(overview_preferences(page)['visible_columns'].values())
        assert page.locator(OVERVIEW + ' tbody tr').first.get_attribute('data-overview-host') == 'test-alpha'


@pytest.mark.parametrize('viewport', [1600, 759, 761])
def test_overview_target_layout_preserves_table_width(manager, viewport):
    """Targets remain reachable without widening the table at desktop/mobile sidebar limits."""
    page = manager
    page.set_viewport_size({'width': viewport, 'height': 1000})
    publish_overview(page)
    for width in (160, 240, 420):
        # Use the shared width preference, including on mobile where the handle is hidden.
        page.evaluate('(width) => localStorage.setItem("pbgui.sidebar.width", String(width))', width)
        page.reload()
        publish_overview(page)
        if viewport > 760:
            assert page.locator('#sidebar').bounding_box()['width'] == width
        table = page.locator(OVERVIEW + ' table')
        fixed_width = table.bounding_box()['width']
        old_css = page.add_style_tag(content='''.th-icon-btn {
          display:inline-block; align-items:normal; justify-content:normal; flex-shrink:1;
          min-width:0; min-height:0; padding:0; margin:0;
        }''')
        old_width = table.bounding_box()['width']
        old_css.evaluate('(element) => element.remove()')
        assert abs(fixed_width - old_width) <= .1
        assert page.evaluate('document.documentElement.scrollWidth <= document.documentElement.clientWidth')
        assert page.locator('#sidebar-actions').evaluate('(e) => e.scrollWidth <= e.clientWidth')
        assert_overview_targets(page)
        # Remove every column but Name via real buttons, including the final hide control.
        while page.locator(OVERVIEW + ' button[title="Hide column"]').count():
            click_overview_button(page, page.locator(OVERVIEW + ' button[title="Hide column"]').last)
        expect(page.locator(OVERVIEW + ' th.sortable')).to_have_count(1)
        expect(page.locator(OVERVIEW + ' .th-sort-main')).to_contain_text(['Name'])
        assert_overview_targets(page)
        publish_overview(page, revision=2)
        expect(page.locator(OVERVIEW + ' th.sortable')).to_have_count(1)
        assert_overview_targets(page)
        click_overview_button(page, page.get_by_role('button', name='Reset view', exact=True))


def test_overview_keyboard_reload_live_state_and_guide(manager):
    """Native keyboard activation and reload/live updates preserve the chosen overview state."""
    page = manager
    assert page.url == 'http://manager.test/prefix/api/vps-manager/main_page'
    publish_overview(page)
    assert_overview_targets(page)
    page.locator(OVERVIEW + ' .th-sort-main').first.click(position={'x': 2, 'y': 2})
    for label, key in [('Hide column Name', 'Enter'), ('Hide column PB8 GitHub', 'Space')]:
        button = page.get_by_role('button', name=label, exact=True)
        button.scroll_into_view_if_needed()
        button.focus()
        # A keyboard event enables :focus-visible even after the preceding pointer action.
        page.keyboard.press('Shift')
        assert button.evaluate('''e => {
          const style = getComputedStyle(e);
          return e.matches(':focus-visible') && style.outlineStyle !== 'none'
            && parseFloat(style.outlineWidth) > 0 && style.outlineColor !== 'rgba(0, 0, 0, 0)';
        }''')
        page.keyboard.press(key)
        expect(button).to_have_count(0)
    preferences = overview_preferences(page)
    assert preferences['sort'] == {'field': 'name', 'dir': 'desc'}
    publish_overview(page, revision=9)
    expect(page.locator(OVERVIEW + ' tbody tr').first).to_contain_text('10')
    assert overview_preferences(page) == preferences
    expect(page.get_by_role('button', name='Hide column Name', exact=True)).to_have_count(0)
    expect(page.get_by_role('button', name='Hide column PB8 GitHub', exact=True)).to_have_count(0)
    assert_overview_targets(page)
    page.locator('#pbgui-guide-btn').click()
    expect(page.locator('#pbgui-shared-help-ovl')).to_have_class('visible')
    assert page.url == 'http://manager.test/prefix/api/vps-manager/main_page'
    assert overview_preferences(page) == preferences
    page.reload()
    publish_overview(page, revision=9)
    assert page.url == 'http://manager.test/prefix/api/vps-manager/main_page'
    assert overview_preferences(page) == preferences
    expect(page.get_by_role('button', name='Hide column Name', exact=True)).to_have_count(0)
    expect(page.get_by_role('button', name='Hide column PB8 GitHub', exact=True)).to_have_count(0)
    assert page.locator(OVERVIEW + ' tbody tr').first.get_attribute('data-overview-host') == 'test-zulu'
    assert_overview_targets(page)
    reset = page.get_by_role('button', name='Reset view', exact=True)
    reset.focus()
    page.keyboard.press('Enter')
    expect(page.locator(OVERVIEW + ' button[title="Hide column"]')).to_have_count(16)
    assert overview_preferences(page)['sort'] == {'field': 'name', 'dir': 'asc'}


def test_missing_pb7_is_neutral_and_live_updates_restore_diagnostics(manager):
    """Absent PB7 has no warnings; installed and unknown states retain diagnostics."""
    page = manager
    for installed in (False, True, None, False):
        row = dict(name='test-host', hostname='test-host', nav='vps', online=True,
                   pb7='N/A', pb7_branch='unknown ()', pb7_github='⚠ unknown')
        if installed is not None:
            row['pb7_installed'] = installed
        page.evaluate("""row => handleMessage({type:'state',data:{
          overview:{rows:[row]},vps:[],deploys:{}}})""", row)
        cells = page.locator('#overview-table-live tbody tr td')
        headers = page.locator('#overview-table-live .th-sort-main').all_text_contents()
        for label, diagnostic in [('PB7', 'N/A ⚠'), ('PB7 Branch', 'unknown ()'),
                                  ('PB7 GitHub', '⚠ unknown')]:
            index = next(i for i, text in enumerate(headers)
                         if re.sub(r'[▲▼↕]', '', text).strip() == label)
            expect(cells.nth(index)).to_have_text('-' if installed is False else diagnostic)
            if installed is False:
                expect(cells.nth(index).locator('.tag')).to_have_count(0)


def test_linux_update_x_recovers_after_busy_password_prompt(manager):
    """The reused X button is re-enabled for package details and closes by pointer/keyboard."""
    page = manager
    publish_state(page)
    page.evaluate("setPasswordPromptBusy(true, 'Validating...')")
    expect(page.locator('#alertModalClose')).to_be_disabled()
    page.locator('[data-vps-action="package-updates"]').click()
    expect(page.locator('#alertModalOverlay')).to_have_class('visible')
    expect(page.locator('#alertModalClose')).to_be_enabled()
    page.locator('#alertModalOverlay').click(position={'x': 2, 'y': 2}, force=True)
    expect(page.locator('#alertModalOverlay')).to_have_class('visible')
    page.locator('#alertModalClose').click()
    expect(page.locator('#alertModalOverlay')).not_to_have_class('visible')
    page.locator('[data-vps-action="package-updates"]').click()
    page.locator('#alertModalClose').focus()
    page.keyboard.press('Enter')
    expect(page.locator('#alertModalOverlay')).not_to_have_class('visible')


def open_test_password_prompt(page):
    """Open the real prompt with an inert callback and synthetic password state."""
    page.evaluate('''() => {
      window.testConfirmations = 0; window.testCommands = [];
      send = message => window.testCommands.push(message.cmd);
      openPasswordPrompt('test-host', () => {window.testConfirmations++; return false;},
        {persistToForm:false});
    }''')


@pytest.mark.parametrize('action', ['Escape', 'Cancel', '×'])
def test_password_prompt_cancel_and_reopen(manager, action):
    """Every close path cancels without confirmation or commands across repeated openings."""
    page = manager
    trigger = page.locator('#pbgui-guide-btn')
    for _ in range(3):
        trigger.focus()
        open_test_password_prompt(page)
        expect(page.locator('#pw-prompt-input')).to_be_focused()
        page.locator('#pw-prompt-input').fill('synthetic-password')
        if action == 'Escape':
            page.keyboard.press('Escape')
        else:
            page.locator('#pw-prompt-cancel' if action == 'Cancel' else '#alertModalClose').click()
        expect(page.locator('#alertModalOverlay')).not_to_have_class('visible')
        expect(trigger).to_be_focused()
        assert page.evaluate('[window.testConfirmations, window.testCommands]') == [0, []]


def test_password_prompt_clicks_and_other_alert_users(manager):
    """Backdrop/content clicks do not dismiss; Escape leaves unrelated alert users unchanged."""
    page = manager
    open_test_password_prompt(page)
    page.locator('#alertModalOverlay').click(position={'x': 2, 'y': 2}, force=True)
    page.locator('#alertModalTitle').click()
    expect(page.locator('#alertModalOverlay')).to_have_class('visible')
    assert page.evaluate('window.testConfirmations') == 0
    page.keyboard.press('Escape')
    for script in ("openAlertModal('Test notice', 'Synthetic content')",
                   "openMasterPasswordPrompt(() => window.testConfirmations++, 'test command')",
                   "openConfirmModal('Test confirmation', 'Synthetic content', () => window.testConfirmations++)"):
        page.evaluate(script)
        page.keyboard.press('Escape')
        expect(page.locator('#alertModalOverlay')).to_have_class('visible')
        page.locator('#alertModalClose').click()
    assert page.evaluate('[window.testConfirmations, window.testCommands]') == [0, []]


def test_password_prompt_validation_error_allows_escape(manager):
    """Escape obeys the real validation lock and cancels safely after a synthetic failure."""
    page = manager
    page.evaluate('''() => {
      window.testCommands = [];
      send = message => window.testCommands.push(message.cmd);
      startOverviewDeployPasswordFlow(['test-host'], 'vps-update', 'Update Linux', 'parallel', false, null);
    }''')
    page.locator('#pw-prompt-input').fill('synthetic-invalid-password')
    page.locator('#pw-prompt-confirm').click()
    assert page.evaluate('store.pendingOverviewDeploy.validating') is True
    page.keyboard.press('Escape')
    expect(page.locator('#alertModalOverlay')).to_have_class('visible')
    expect(page.locator('#pw-prompt-error')).to_have_text('Wait until password validation finishes for test-host.')
    expect(page.locator('#pw-prompt-cancel')).to_be_disabled()
    expect(page.locator('#alertModalClose')).to_be_disabled()
    page.evaluate("handleMessage({type:'error',cmd:'validate_and_stage_vps_deploy_host',error:'Synthetic validation failure'})")
    expect(page.locator('#pw-prompt-error')).to_have_text('Synthetic validation failure')
    expect(page.locator('#pw-prompt-cancel')).to_be_enabled()
    expect(page.locator('#alertModalClose')).to_be_enabled()
    commands = page.evaluate('window.testCommands.slice()')
    assert commands.count('validate_and_stage_vps_deploy_host') == 1
    page.keyboard.press('Escape')
    expect(page.locator('#alertModalOverlay')).not_to_have_class('visible')
    assert page.evaluate('store.pendingOverviewDeploy') is None
    assert page.evaluate('window.testCommands') == commands


@pytest.mark.parametrize('manager', ['normal', 'reverse'], indirect=True)
@pytest.mark.parametrize('upper', ['guide', 'legacy-help', 'confirm', 'alert', 'about', 'shared'])
def test_password_prompt_escape_overlay_priority(manager, upper):
    """Real Guide/navigation/shared handlers close only their layer in either listener order."""
    page = manager
    open_test_password_prompt(page)
    scripts = {
        'guide': "document.getElementById('pbgui-guide-btn').click()",
        'legacy-help': "document.getElementById('help-ovl').classList.add('visible')",
        'confirm': "window.PBGuiConfirm({title:'Synthetic confirmation',message:'Test only'})",
        'alert': "document.getElementById('pbgui-alert-ovl').classList.add('visible')",
        'about': "document.getElementById('pbgui-about-ovl').classList.add('visible')",
        'shared': "window.PBGuiDialogs.confirm({title:'Synthetic confirmation',message:'Test only'})",
    }
    ids = {'guide': 'pbgui-shared-help-ovl', 'legacy-help': 'help-ovl',
           'confirm': 'pbgui-confirm-ovl', 'alert': 'pbgui-alert-ovl',
           'about': 'pbgui-about-ovl', 'shared': 'pbgui-dialog-ovl'}
    # Do not return an unresolved dialog Promise from evaluate.
    page.evaluate('() => {' + scripts[upper] + ';}')
    expect(page.locator('#' + ids[upper])).to_have_class('visible')
    page.evaluate('''() => {
      if (!window.testKeyListeners) return;
      const listeners = window.testKeyListeners.slice();
      for (const {target,listener,options} of listeners) target.removeEventListener('keydown',listener,options);
      for (const {target,listener,options} of listeners.reverse())
        window.testOriginalAddListener.call(target,'keydown',listener,options);
    }''')
    page.keyboard.press('Escape')
    expect(page.locator('#' + ids[upper])).not_to_have_class('visible')
    expect(page.locator('#alertModalOverlay')).to_have_class('visible')
    page.keyboard.press('Escape')
    expect(page.locator('#alertModalOverlay')).not_to_have_class('visible')
    assert page.evaluate('[window.testConfirmations, window.testCommands]') == [0, []]


def test_password_prompt_consumed_escape_and_cluster_priority(manager):
    """Already consumed Escape and Cluster onboarding cannot cancel a VPS password flow."""
    page = manager
    open_test_password_prompt(page)
    page.evaluate('''() => {
      const event = new KeyboardEvent('keydown',{key:'Escape',bubbles:true,cancelable:true});
      event.preventDefault(); document.dispatchEvent(event);
    }''')
    expect(page.locator('#alertModalOverlay')).to_have_class('visible')
    page.evaluate("document.getElementById('alertModalTitle').textContent = 'Add to Cluster'")
    page.keyboard.press('Escape')
    expect(page.locator('#alertModalOverlay')).to_have_class('visible')
    assert page.evaluate('window.testConfirmations') == 0


def test_password_prompt_focus_timer_and_single_enter(manager):
    """Closed/replaced prompt timers cannot steal focus or duplicate Enter callbacks."""
    page = manager
    # Count focus attempts on removed inputs too; waiting is bounded to the 100-ms timer.
    page.evaluate('''() => {
      window.testFocusAttempts = []; window.testConfirmations = 0;
      const focus = HTMLElement.prototype.focus;
      HTMLElement.prototype.focus = function(...args) {
        if (this.id === 'pw-prompt-input') window.testFocusAttempts.push(this.dataset.opening);
        return focus.apply(this,args);
      };
      openPasswordPrompt('test-host', () => false, {persistToForm:false});
      document.getElementById('pw-prompt-input').dataset.opening = 'closed';
      closeAlertModal();
      openPasswordPrompt('test-host', () => false, {persistToForm:false});
      document.getElementById('pw-prompt-input').dataset.opening = 'replaced';
      openPasswordPrompt('test-host', () => {window.testConfirmations++; return false;}, {persistToForm:false});
      document.getElementById('pw-prompt-input').dataset.opening = 'current';
    }''')
    expect(page.locator('#pw-prompt-input')).to_be_focused()
    page.wait_for_timeout(150)
    assert page.evaluate('window.testFocusAttempts') == ['current']
    page.locator('#pw-prompt-input').fill('synthetic-password')
    page.keyboard.press('Enter')
    assert page.evaluate('window.testConfirmations') == 1
    expect(page.locator('#alertModalOverlay')).to_have_class('visible')
    page.keyboard.press('Escape')
    page.evaluate('''() => {
      openPasswordPrompt('test-host', () => false, {persistToForm:false});
      document.getElementById('pbgui-guide-btn').click();
    }''')
    page.wait_for_timeout(150)
    assert page.evaluate('window.testFocusAttempts') == ['current']
    assert not page.locator('#pw-prompt-input').evaluate('(e) => e === document.activeElement')


def test_password_prompt_close_preserves_newer_focus(manager):
    """Closing a password prompt does not override a newer page input or shared dialog."""
    page = manager
    for upper in (False, True):
        page.locator('#pbgui-guide-btn').focus()
        open_test_password_prompt(page)
        expect(page.locator('#pw-prompt-input')).to_be_focused()
        if upper:
            page.evaluate("() => {PBGuiDialogs.prompt({title:'Synthetic prompt',label:'Test value'});}")
            expect(page.locator('#pbgui-dialog-input')).to_be_focused()
        else:
            page.locator('#pbgui-guide-btn').focus()
        page.evaluate('closeAlertModal()')
        expect(page.locator('#pbgui-dialog-input' if upper else '#pbgui-guide-btn')).to_be_focused()


def test_security_normal_counts_colors_and_live_updates(manager):
    """Overview shows mutually exclusive counts and preserves context on live changes."""
    page = manager
    publish_state(page)
    counter = page.locator('[data-vps-action="package-updates"]')
    expect(counter).to_have_text('3 / 2')
    security = counter.locator('[data-update-kind="security"]')
    normal = counter.locator('[data-update-kind="normal"]')
    for node, variable in ((security, '--danger'), (normal, '--accent')):
        expected = page.evaluate("""variable => {
          const probe = document.createElement('span'); probe.style.color = 'var(' + variable + ')';
          document.body.append(probe); const color = getComputedStyle(probe).color; probe.remove(); return color;
        }""", variable)
        assert node.evaluate('(e) => getComputedStyle(e).color') == expected
    counter.click()
    expect(page.locator('#alertModalTitle')).to_have_text('Linux Updates — test-host')
    page.locator('#alertModalClose').click()
    publish_state(page, security_updates=4)
    expect(counter).to_have_text('4 / 1')
    for key, width in [('Home', 160), ('End', 420)]:
        page.locator('#sidebar-resize').focus(); page.keyboard.press(key)
        assert page.locator('#sidebar').bounding_box()['width'] == width
        assert page.locator('#sidebar-actions').evaluate('(e)=>e.scrollWidth<=e.clientWidth')
    page.locator('#pbgui-guide-btn').click()
    expect(page.locator('#pbgui-shared-help-ovl')).to_have_class('visible')
    assert '/prefix/api/vps-manager/main_page' in page.url
    page.reload()
    assert page.locator('#sidebar').bounding_box()['width'] == 420


@pytest.mark.parametrize('changes,expected', [
    ({'upgrades': 0, 'security_updates': None, 'classification_complete': False, 'packages': []}, '0 / 0'),
    ({'classification_complete': False, 'deferred_updates': 1}, '? / ?'),
    ({'available': False, 'state': 'missing'}, 'N/A'),
    ({'state': 'stale'}, '3 / 2'),
    ({'security_updates': 0}, '0 / 5'),
    ({'security_updates': 5}, '5 / 0'),
    ({'security_updates': 9}, '? / ?'),
    ({'upgrades': 2, 'installable_updates': 2, 'security_updates': 2, 'new_installs': 1, 'removals': 1,
      'packages': [
          {'name': 'security-upgrade', 'installed_version': '1', 'candidate_version': '2', 'security': True},
          {'name': 'normal-upgrade', 'installed_version': '1', 'candidate_version': '2', 'security': False, 'kernel': True},
          {'name': 'new-package', 'installed_version': '', 'candidate_version': '2', 'security': True},
          {'name': 'removed-package', 'installed_version': '1', 'candidate_version': '1', 'removed': True},
      ]}, '1 / 1'),
])
def test_unknown_zero_and_stale_package_states(manager, changes, expected):
    """Missing or incomplete classification never masquerades as no security updates."""
    page = manager
    publish_state(page, **changes)
    rows = page.locator('tr[data-overview-host="test-host"]')
    expect(rows).to_contain_text(expected)


def test_update_counts_align_with_zero_counts_and_header(manager):
    """Clickable counts retain the same text position as zero counts and the header."""
    page = manager
    publish_state(page)
    counter = page.locator('[data-update-kind="security"]')
    metrics = counter.evaluate('''e => {
      const cell = e.closest('td'); const row = cell.parentElement;
      const header = cell.closest('table').querySelectorAll('th')[cell.cellIndex];
      const range = document.createRange(); range.selectNodeContents(e);
      const rect = range.getBoundingClientRect();
      return {x: rect.x - cell.getBoundingClientRect().x,
        y: rect.y - row.getBoundingClientRect().y,
        header: header.querySelector('.th-sort-main').getBoundingClientRect().x - header.getBoundingClientRect().x};
    }''')
    assert abs(metrics['x'] - metrics['header']) <= 1
    publish_state(page, upgrades=0, security_updates=0, packages=[])
    zero = counter.evaluate('''e => {
      const range = document.createRange(); range.selectNodeContents(e);
      const rect = range.getBoundingClientRect();
      return {x: rect.x - e.closest('td').getBoundingClientRect().x,
        y: rect.y - e.closest('tr').getBoundingClientRect().y};
    }''')
    assert abs(metrics['x'] - zero['x']) <= 1
    assert abs(metrics['y'] - zero['y']) <= 1


def test_deferred_updates_preserve_known_counts(manager):
    """One deferred package must not hide the twenty already classified upgrades."""
    page = manager
    publish_state(page, upgrades=21, installable_updates=20, deferred_updates=1,
                  details_complete=False, classification_complete=False, security_updates=7,
                  packages=[dict(name=f'package-{i}', installed_version='1', security=i < 7)
                            for i in range(20)])
    expect(page.locator('[data-vps-action="package-updates"]')).to_have_text('7 / 13')
    expect(page.locator('[data-vps-action="package-updates"]')).to_have_attribute('title', re.compile('1 deferred update unclassified'))
    expect(page.locator('tr[data-overview-host="test-host"]')).not_to_contain_text('deferred')
    # Missing details must still be shown as unknown, never guessed to be normal.
    publish_state(page, upgrades=21, deferred_updates=1, classification_complete=False, packages=[])
    expect(page.locator('[data-vps-action="package-updates"]')).to_have_text('? / ?')


@pytest.mark.parametrize('deferred', [1, 4])
def test_all_deferred_updates_show_zero_counts_with_hover_details(manager, deferred):
    """Fully deferred batches show no installable updates and retain clickable details."""
    page = manager
    publish_state(page, upgrades=deferred, installable_updates=0, deferred_updates=deferred,
                  details_complete=False, classification_complete=False, packages=[])
    counts = page.locator('[data-vps-action="package-updates"]')
    expect(counts).to_have_text('0 / 0')
    expect(counts).to_have_attribute('title', re.compile(rf'{deferred} deferred update'))
    expect(page.locator('tr[data-overview-host="test-host"]')).not_to_contain_text('deferred')
    counts.click()
    expect(page.locator('#alertModalOverlay')).to_have_class('visible')
    # Automatic incoming state restores real counts without a page reload.
    page.locator('#alertModalClose').click()
    publish_state(page, upgrades=5, deferred_updates=0, classification_complete=True, security_updates=3)
    expect(page.locator('[data-vps-action="package-updates"]')).to_have_text('3 / 2')
