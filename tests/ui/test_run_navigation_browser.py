"""Run navigation restoration using real menus and isolated REST/WebSocket data."""

import json
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse

import pytest
from browser_driver import expect, sync_browser


ROOT = Path(__file__).resolve().parents[2]


def navigation_key(version):
    """Return the version-specific navigation key expected by this contract."""
    return f'pbgui:run:{version}:navigation:v1'


@pytest.fixture
def run_browser():
    """Serve real frontend assets while keeping all data and writes synthetic."""
    with sync_browser() as driver:
        browser = driver.chromium.launch()
        page = browser.new_page(viewport={'width': 1280, 'height': 850})
        page.set_default_timeout(5000)
        state = SimpleNamespace(
            page=page, sockets=[], errors=[], loads=[], pending=[], hold=False,
            writes=[], pending_writes=[], hold_writes=False,
            drop_rest_generation_guard=False,
            rows=[
                {'name': 'gui-442-a', 'status': 'disabled', 'note': 'Alpha'},
                {'name': 'gui-442-c', 'status': 'disabled', 'note': 'Zulu'},
                {'name': 'other', 'status': 'disabled', 'note': 'Middle'},
                {'name': 'gui-442-live', 'status': 'synced', 'note': 'Live'},
            ],
        )
        page.on('pageerror', lambda error: state.errors.append(str(error)))

        def respond(route):
            """Intercept every request; never connect to a running PBGui instance."""
            url = urlparse(route.request.url)
            path = url.path
            if path == '/app/start.html':
                route.fulfill(content_type='text/html', body=(
                    '<nav id="topnav"></nav><script>'
                    'window.PBGUI_NAV_CONFIG={current:"welcome",authenticated:true};'
                    '</script><script src="/app/pbgui_nav.js"></script>'
                ))
            elif path in ('/api/v7/main_page', '/api/v8/main_page'):
                version = path.split('/')[2]
                source = (ROOT / 'frontend/v7_run.html').read_text()
                # Observe entry into the real parsed-REST callback, not a WS counter.
                rest_callback = '  }).then(function(data) {\n    if (generation !== loadGeneration) return;'
                assert source.count(rest_callback) == 1
                source = source.replace(rest_callback, '  }).then(function(data) {\n'
                                        '    window.__restCompletions = (window.__restCompletions || 0) + 1;\n'
                                        '    if (generation !== loadGeneration) return;')
                if state.drop_rest_generation_guard:
                    # Negative control changes only intercepted HTML, never source files.
                    source = source.replace('    if (generation !== loadGeneration) return;', '')
                for key, value in {
                    'API_BASE': f'/api/{version}', 'WS_BASE': 'ws://run.test',
                    'RUN_VERSION': version, 'VERSION': 'test', 'SERIAL': '1',
                    'MASTER_NAME': 'synthetic', 'NAV_HASH': 'test',
                }.items():
                    source = source.replace('%%' + key + '%%', value)
                # Observe the first real render, including controls and header arrows.
                source = source.replace('runListAdapter.configureUi();', '''
                    window.__renders = [];
                    var observedRender = render;
                    render = function() {
                      observedRender();
                      window.__renders.push({
                        keys: Array.from(document.querySelectorAll('#tbody tr[data-key]')).map(r => r.dataset.key),
                        search: document.getElementById('f-search').value,
                        status: document.getElementById('f-status').value,
                        arrow: document.querySelector('th[data-sort="' + sort.col + '"] .sort-arrow').textContent.trim()
                      });
                    };
                    runListAdapter.configureUi();''')
                route.fulfill(content_type='text/html', body=source,
                              headers={'Cache-Control': 'no-store'})
            elif path.startswith(('/api/v7/instances/', '/api/v8/instances/')) and route.request.method in ('POST', 'DELETE'):
                state.writes.append({'path': path, 'method': route.request.method,
                                     'body': route.request.post_data_json})
                if state.hold_writes:
                    state.pending_writes.append(route)
                else:
                    route.fulfill(json={'version': 8})
            elif path in ('/api/v7/instances', '/api/v8/instances'):
                state.loads.append(path)
                if state.hold:
                    state.pending.append(route)
                else:
                    route.fulfill(json={'instances': state.rows})
            elif path == '/api/help/index':
                route.fulfill(json=[{'file': f'{number}_pb{version}_run', 'title': f'PB{version} Run'}
                                    for number, version in [('34', 'v7'), ('44', 'v8')]])
            elif path == '/api/help/content':
                query = parse_qs(url.query)
                directory = 'help_de' if query.get('lang') == ['de'] else 'help'
                route.fulfill(json={'content': (ROOT / 'docs' / directory / (query['file'][0] + '.md')).read_text()})
            elif path.startswith('/app/'):
                asset = ROOT / 'frontend' / path.removeprefix('/app/')
                route.fulfill(body=asset.read_bytes() if asset.is_file() else b'',
                              content_type='text/css' if asset.suffix == '.css' else 'text/javascript')
            elif path.endswith('/stream'):
                route.fulfill(status=204, body='')
            else:
                route.fulfill(json={})

        page.route('**/*', respond)
        page.route_web_socket('**/ws/v*', lambda socket: state.sockets.append(socket))
        page.goto('http://run.test/app/start.html')
        try:
            yield state
        finally:
            browser.close()


def open_run(state, version):
    """Enter the page using the production PBv7/PBv8 → Run menu."""
    page = state.page
    page.locator(f'.nav-group-btn[data-group="pb{version}"]').click()
    page.locator(f'.nav-item[data-page="{version}_run"]').click()
    expect(page).to_have_url(f'http://run.test/api/{version}/main_page')
    page.wait_for_function('ws && ws.readyState === WebSocket.OPEN')


def row_names(page):
    """Read the names of displayed instance rows in their actual order."""
    return page.locator('#tbody tr[data-key]').evaluate_all('(rows) => rows.map(r => r.dataset.key)')


def assert_navigation(page, search='gui-442', status='disabled', col='note', asc=False):
    """Check controls, active sorting and its visible arrow together."""
    expect(page.locator('#f-search')).to_have_value(search)
    expect(page.locator('#f-status')).to_have_value(status)
    expect(page.locator(f'th[data-sort="{col}"] .sort-arrow')).to_have_text('▲' if asc else '▼')
    assert page.evaluate('({col:sort.col,asc:sort.asc})') == {'col': col, 'asc': asc}


@pytest.mark.parametrize('version', ['v7', 'v8'])
@pytest.mark.parametrize('mode', ['search', 'status', 'sort', 'combined'])
def test_run_reload_restores_real_inputs_and_current_rows(run_browser, version, mode):
    """Individual and combined state survive reload with fresh REST data and arrows."""
    state, page = run_browser, run_browser.page
    open_run(state, version)
    expect(page.locator('#tbody tr[data-key]')).to_have_count(4)
    search = 'gui-442' if mode in ('search', 'combined') else ''
    status = 'disabled' if mode in ('status', 'combined') else 'All'
    col, asc = ('note', False) if mode in ('sort', 'combined') else ('name', True)
    if search:
        page.locator('#f-search').fill(search)
    page.locator('#f-status').select_option(status)
    if col == 'note':
        page.locator('th[data-sort="note"]').click()
        expect(page.locator('th[data-sort="note"] .sort-arrow')).to_have_text('▲')
        page.locator('th[data-sort="note"]').click()
    before = row_names(page)
    if col == 'note':
        assert before == (['gui-442-c', 'gui-442-a'] if mode == 'combined'
                          else ['gui-442-live', 'gui-442-c', 'other', 'gui-442-a'])
    saved = page.evaluate('(key) => JSON.parse(sessionStorage.getItem(key))', navigation_key(version))
    assert saved == {'search': search, 'status': status, 'sortCol': col, 'sortAsc': asc}
    state.rows[0]['note'] = 'Alpha fresh'
    loads = len(state.loads)
    page.reload()
    expect(page.locator('tr[data-key="gui-442-a"]')).to_contain_text('Alpha fresh')
    assert len(state.loads) == loads + 1
    assert_navigation(page, search, status, col, asc)
    assert row_names(page) == before
    assert page.evaluate('__renders[0]') == {
        'keys': before, 'search': search, 'status': status, 'arrow': '▲' if asc else '▼',
    }
    assert not state.errors


@pytest.mark.parametrize('version', ['v7', 'v8'])
@pytest.mark.parametrize('guard_enabled', [True, False], ids=['protected', 'negative-control'])
def test_run_websocket_reconnect_and_late_rest_preserve_navigation(run_browser, version, guard_enabled):
    """Reject stale REST before reconnect; verify this probe catches a removed guard."""
    state, page = run_browser, run_browser.page
    state.drop_rest_generation_guard = not guard_enabled
    saved = {'search': 'gui-442', 'status': 'disabled', 'sortCol': 'note', 'sortAsc': False}
    page.evaluate('([key,value]) => sessionStorage.setItem(key, JSON.stringify(value))', [navigation_key(version), saved])
    state.hold = True
    open_run(state, version)
    # Reads/writes of this key must not occur in onmessage or reconnect callbacks.
    page.evaluate('''() => {
      window.__navigationAccesses = [];
      for (const method of ['getItem','setItem']) {
        const original = Storage.prototype[method];
        Storage.prototype[method] = function(key, ...args) {
          if (key.startsWith('pbgui:run:')) window.__navigationAccesses.push(method);
          return original.call(this, key, ...args);
        };
      }
    }''')
    state.rows[0]['note'] = 'Alpha WS'
    state.sockets[-1].send(json.dumps({'type': 'instances', 'data': state.rows}))
    expect(page.locator('tr[data-key="gui-442-a"]')).to_contain_text('Alpha WS')
    assert page.evaluate('__renders[0]') == {
        'keys': ['gui-442-c', 'gui-442-a'], 'search': 'gui-442', 'status': 'disabled', 'arrow': '▼',
    }
    state.pending.pop().fulfill(json={'instances': [
        {'name': 'gui-442-stale', 'status': 'disabled', 'note': 'Stale REST'},
    ]})
    # This marker is set inside the parsed-response callback. Its synchronous
    # generation check and assignment finish before the browser can answer us.
    page.wait_for_function('window.__restCompletions === 1')

    def assert_current_snapshot():
        """Require both current data and the complete visible view before reconnect."""
        assert page.evaluate('instances') == state.rows
        assert row_names(page) == ['gui-442-c', 'gui-442-a']
        assert_navigation(page)
        assert page.evaluate('__renders.every(snapshot => '
                             'JSON.stringify(snapshot.keys) === JSON.stringify(["gui-442-c", "gui-442-a"]))')

    if not guard_enabled:
        with pytest.raises(AssertionError):
            assert_current_snapshot()
        assert row_names(page) == ['gui-442-stale']
        assert not state.errors
        return

    assert_current_snapshot()
    assert len(state.sockets) == 1
    state.sockets[-1].close()
    page.wait_for_function('socketGeneration === 2 && ws.readyState === WebSocket.OPEN')
    state.rows[1]['note'] = 'Zulu reconnect'
    state.sockets[-1].send(json.dumps({'type': 'instances', 'data': state.rows}))
    expect(page.locator('tr[data-key="gui-442-c"]')).to_contain_text('Zulu reconnect')
    assert row_names(page) == ['gui-442-c', 'gui-442-a']
    assert_navigation(page)
    assert_current_snapshot()
    assert page.evaluate('__navigationAccesses') == []
    assert not state.errors


def test_run_version_keys_are_independent_and_clearing_search_persists(run_browser):
    """Switching versions in the same tab preserves each view and an explicitly empty search."""
    state, page = run_browser, run_browser.page
    open_run(state, 'v8')
    page.locator('#f-search').fill('gui-442')
    page.locator('#f-status').select_option('disabled')
    page.locator('th[data-sort="strategy"]').click()
    open_run(state, 'v7')
    assert_navigation(page, '', 'All', 'name', True)
    page.locator('#f-search').fill('other')
    open_run(state, 'v8')
    assert_navigation(page, 'gui-442', 'disabled', 'strategy', True)
    page.locator('#f-search').fill('')
    page.reload()
    expect(page.locator('#tbody tr[data-key]')).to_have_count(3)
    assert_navigation(page, '', 'disabled', 'strategy', True)
    open_run(state, 'v7')
    expect(page.locator('#tbody tr[data-key]')).to_have_count(1)
    assert_navigation(page, 'other', 'All', 'name', True)
    assert not state.errors


@pytest.mark.parametrize('version', ['v7', 'v8'])
@pytest.mark.parametrize('raw,expected', [
    ('{broken', ('', 'All', 'name', True)),
    ('null', ('', 'All', 'name', True)),
    ('[]', ('', 'All', 'name', True)),
    ('{"search":7,"status":false,"sortCol":[],"sortAsc":"false"}', ('', 'All', 'name', True)),
    ('{"search":"gui-442","status":"bogus","sortCol":"_actions","sortAsc":false}', ('gui-442', 'All', 'name', False)),
    ('{"search":null,"status":"disabled","sortCol":"note","sortAsc":null}', ('', 'disabled', 'note', True)),
    ('{"sortCol":"strategy"}', ('', 'All', 'strategy', True)),
])
def test_run_restore_validates_fields_individually(run_browser, version, raw, expected):
    """Malformed containers/types and disallowed columns fall back without losing valid fields."""
    state, page = run_browser, run_browser.page
    page.evaluate('([key,value]) => sessionStorage.setItem(key,value)', [navigation_key(version), raw])
    open_run(state, version)
    expect(page.locator('#tbody tr[data-key]')).not_to_have_count(0)
    if version == 'v7' and expected[2] == 'strategy':
        expected = (*expected[:2], 'name', expected[3])
    assert_navigation(page, *expected)
    page.locator('#f-search').fill('other')
    expect(page.locator('#tbody tr[data-key]')).to_have_count(1)
    assert not state.errors


@pytest.mark.parametrize('version', ['v7', 'v8'])
@pytest.mark.parametrize('method', ['getItem', 'setItem'])
def test_run_storage_failure_keeps_controls_and_rendering_usable(run_browser, version, method):
    """Blocked storage must never prevent data loading, filtering or sorting."""
    state, page = run_browser, run_browser.page
    page.add_init_script('''(function() {
      const method = %s;
      const original = Storage.prototype[method];
      Storage.prototype[method] = function(key, ...args) {
        if (key.startsWith('pbgui:run:')) throw new Error('Synthetic blocked storage');
        return original.call(this, key, ...args);
      };
    })();''' % json.dumps(method))
    open_run(state, version)
    page.locator('#f-search').fill('gui-442')
    page.locator('#f-status').select_option('disabled')
    page.locator('th[data-sort="note"]').click()
    page.locator('th[data-sort="note"]').click()
    assert_navigation(page)
    assert row_names(page) == ['gui-442-c', 'gui-442-a']
    assert not state.errors


@pytest.mark.parametrize('version', ['v7', 'v8'])
def test_run_shared_shell_guide_and_sidebar_preserve_navigation(run_browser, version):
    """Check the real Guide overlay and shared sidebar extremes without production actions."""
    state, page = run_browser, run_browser.page
    open_run(state, version)
    page.locator('#f-search').fill('gui-442')
    page.locator('#f-status').select_option('disabled')
    page.locator('th[data-sort="note"]').click()
    page.locator('th[data-sort="note"]').click()
    destination = page.url
    page.locator('#pbgui-guide-btn').click()
    expect(page.locator('#pbgui-shared-help-box')).to_be_visible()
    expect(page.locator('#pbgui-shared-help-box')).to_contain_text(f'PB{version} Run')
    assert page.url == destination
    page.locator('#pbgui-shared-help-close').click()
    assert_navigation(page)
    for key, width in [('Home', 160), ('End', 420)]:
        page.locator('#sidebar-resize').focus()
        page.keyboard.press(key)
        assert round(page.locator('#sidebar').bounding_box()['width']) == width
        page.reload()
        assert round(page.locator('#sidebar').bounding_box()['width']) == width
        assert_navigation(page)
    for viewport in [700, 800, 1280]:
        page.set_viewport_size({'width': viewport, 'height': 850})
        assert page.locator('#sidebar-toolbar').evaluate('(el) => el.scrollWidth <= el.clientWidth')
        for control in ['#f-search', '#f-status']:
            assert page.locator(control).evaluate('(el) => el.getBoundingClientRect().right <= document.getElementById("sidebar").getBoundingClientRect().right')
    assert not state.errors


@pytest.mark.parametrize('version', ['v7', 'v8'])
@pytest.mark.parametrize('action,label', [
    ('delete', 'Delete'), ('panic', 'Panic'), ('graceful_stop', 'Graceful Stop'),
    ('tp_only', 'Take Profit Only'), ('normal', 'Publish Normal'),
])
def test_run_action_dialog_dismissal_and_focus(run_browser, version, action, label):
    """Real row actions support explicit cancellation, focus trapping and safe repeated opening."""
    state, page = run_browser, run_browser.page
    state.rows[0].update(version=7, forced_mode_long='panic', forced_mode_short='tp_only')
    open_run(state, version)
    page.locator('#f-search').fill('gui-442')
    page.locator('#f-status').select_option('disabled')
    page.locator('th[data-sort="note"]').click()
    page.locator('th[data-sort="note"]').click()
    destination = page.url
    selector = '[data-delete]' if action == 'delete' else f'[data-forced-mode="{action}"]'
    trigger = page.locator('tr[data-key="gui-442-a"]').locator(selector)
    overlay = page.locator('#pbgui-dialog-ovl')
    for dismissal in ['Escape', 'Cancel', 'Close']:
        trigger.click()
        expect(overlay).to_be_visible()
        expect(page.locator('#pbgui-dialog-accept')).to_have_text(label)
        expect(page.locator('#pbgui-dialog-message')).to_contain_text('gui-442-a')
        expect(page.locator('#pbgui-dialog-accept')).to_be_focused()
        # Pointer clicks must not dismiss the dialog or let either Tab
        # direction reach the page behind it. Do not repair focus in the test.
        for target in [overlay, page.locator('#pbgui-dialog-message')]:
            for key in ['Tab', 'Shift+Tab']:
                if target == overlay:
                    target.click(position={'x': 3, 'y': 3})
                else:
                    target.click()
                expect(overlay).to_be_visible()
                page.keyboard.press(key)
                assert page.evaluate('document.getElementById("pbgui-dialog-box").contains(document.activeElement)'), key
        expect(page.locator('#pbgui-dialog-accept')).to_be_focused()
        page.keyboard.press('Tab')
        expect(page.locator('#pbgui-dialog-close')).to_be_focused()
        page.keyboard.press('Shift+Tab')
        expect(page.locator('#pbgui-dialog-accept')).to_be_focused()
        for _ in range(4):
            page.keyboard.press('Tab')
            assert page.evaluate('document.getElementById("pbgui-dialog-box").contains(document.activeElement)')
        if dismissal == 'Escape':
            page.keyboard.press('Escape')
        else:
            page.locator('#pbgui-dialog-' + dismissal.lower()).click()
        expect(overlay).not_to_be_visible()
        expect(trigger).to_be_focused()
        assert not state.writes
        assert page.url == destination
        assert_navigation(page)
    page.reload()
    assert_navigation(page)
    trigger.click()
    page.keyboard.press('Escape')
    expect(trigger).to_be_focused()
    assert not state.writes
    assert not state.errors


@pytest.mark.parametrize('mode', ['confirm', 'alert', 'prompt', 'choose'])
def test_shared_dialog_recovers_keyboard_focus_after_pointer_click(run_browser, mode):
    """All shared modes recover both Tab directions without changing their result contracts."""
    state, page = run_browser, run_browser.page
    open_run(state, 'v7')
    trigger = page.locator('#f-search')
    trigger.focus()
    page.evaluate('''(mode) => {
      window.__dialogResult = 'pending';
      PBGuiDialogs[mode]({
        title: 'Synthetic focus test', message: 'Synthetic message',
        reportable: false, defaultValue: 'Unsaved input',
        actions: [{label: 'First choice', value: 'first'}, {label: 'Last choice', value: 'last'}]
      }).then(value => { window.__dialogResult = value; });
    }''', mode)
    overlay = page.locator('#pbgui-dialog-ovl')
    last = (page.locator('#pbgui-dialog-choices button').last
            if mode == 'choose' else page.locator('#pbgui-dialog-accept'))
    for target in [overlay, page.locator('#pbgui-dialog-message')]:
        for key in ['Tab', 'Shift+Tab']:
            if target == overlay:
                target.click(position={'x': 3, 'y': 3})
            else:
                target.click()
            expect(overlay).to_be_visible()
            assert page.evaluate('__dialogResult') == 'pending'
            page.keyboard.press(key)
            expect(page.locator('#pbgui-dialog-close') if key == 'Tab' else last).to_be_focused()
    if mode == 'prompt':
        expect(page.locator('#pbgui-dialog-input')).to_have_value('Unsaved input')
    page.keyboard.press('Escape')
    expect(overlay).not_to_be_visible()
    expect(trigger).to_be_focused()
    page.wait_for_function('window.__dialogResult !== "pending"')
    assert page.evaluate('__dialogResult') == {'confirm': False, 'alert': True, 'prompt': None, 'choose': None}[mode]
    assert not state.writes
    assert not state.errors


@pytest.mark.parametrize('version', ['v7', 'v8'])
@pytest.mark.parametrize('action', ['delete', 'panic'])
@pytest.mark.parametrize('ending', ['success', 'error'])
def test_run_pending_action_preserves_later_dialog_and_navigation(run_browser, version, action, ending):
    """Delayed synthetic writes stay locked and never close or refocus a subsequent dialog."""
    state, page = run_browser, run_browser.page
    state.rows[0].update(version=7, forced_mode_long='panic')
    state.hold_writes = True
    open_run(state, version)
    page.locator('#f-search').fill('gui-442')
    page.locator('#f-status').select_option('disabled')
    page.locator('th[data-sort="note"]').click()
    page.locator('th[data-sort="note"]').click()
    destination = page.url
    selector = '[data-delete]' if action == 'delete' else '[data-forced-mode="panic"]'
    trigger = page.locator('tr[data-key="gui-442-a"]').locator(selector)
    trigger.click()
    with page.expect_request(lambda request: request.method in ('POST', 'DELETE')
                             and '/instances/gui-442-a' in request.url):
        page.locator('#pbgui-dialog-accept').click()
    expect(page.locator('#pbgui-dialog-ovl')).not_to_be_visible()
    page.wait_for_function('_deleteInFlight || _forcedModeInFlight')
    expect(page.locator('#toast')).not_to_be_visible()
    trigger.click()
    expect(page.locator('#pbgui-dialog-ovl')).not_to_be_visible()
    assert len(state.writes) == 1
    assert state.writes[0] == {
        'path': f'/api/{version}/instances/gui-442-a' + ('' if action == 'delete' else '/forced-mode'),
        'method': 'DELETE' if action == 'delete' else 'POST',
        'body': None if action == 'delete' else {'mode': 'panic'},
    }
    other_selector = '[data-forced-mode="graceful_stop"]' if action == 'delete' else '[data-delete]'
    later_trigger = page.locator('tr[data-key="gui-442-c"]').locator(other_selector)
    later_trigger.click()
    expect(page.locator('#pbgui-dialog-ovl')).to_be_visible()
    expect(page.locator('#pbgui-dialog-accept')).to_be_focused()
    later_title = page.locator('#pbgui-dialog-title').inner_text()
    loads = len(state.loads)
    if ending == 'success':
        if action == 'delete':
            state.rows = [row for row in state.rows if row['name'] != 'gui-442-a']
        else:
            state.rows[0].update(version=8, forced_mode_long='panic', forced_mode_short='panic')
        state.pending_writes.pop().fulfill(json={'version': 8})
    else:
        state.pending_writes.pop().fulfill(status=409, json={'detail': 'Synthetic conflict'})
    page.wait_for_function('!_deleteInFlight && !_forcedModeInFlight')
    if ending == 'success':
        expect(page.locator('#toast')).to_contain_text('deleted' if action == 'delete' else 'published')
        if action == 'delete':
            expect(page.locator('tr[data-key="gui-442-a"]')).to_have_count(0)
        else:
            expect(page.locator('tr[data-key="gui-442-a"]')).to_contain_text('Global: PANIC')
            assert len(state.loads) == loads + 1
    else:
        expect(page.locator('#toast')).to_contain_text('Synthetic conflict')
    expect(page.locator('#pbgui-dialog-ovl')).to_be_visible()
    expect(page.locator('#pbgui-dialog-title')).to_have_text(later_title)
    expect(page.locator('#pbgui-dialog-accept')).to_be_focused()
    assert page.url == destination
    assert_navigation(page)
    page.keyboard.press('Escape')
    expect(later_trigger).to_be_focused()
    # The completed action can be opened again, including after an HTTP error.
    page.locator('tr[data-key="gui-442-c"]').locator(selector).click()
    expect(page.locator('#pbgui-dialog-ovl')).to_be_visible()
    page.keyboard.press('Escape')
    assert len(state.writes) == 1
    assert not state.errors
