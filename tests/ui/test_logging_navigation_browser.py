"""Logging navigation and validation with real assets and intercepted browser requests."""

from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest
from fastapi import Request

from api.page_templates import render_page_urls

ROOT = Path(__file__).resolve().parents[2]
ORIGIN = 'http://logging.test'
PREFIX = '/mounted'
PAGE_PATH = PREFIX + '/api/logging/main_page'


@pytest.fixture
def logging_page():
    """Block writes and external traffic; close every browser resource even on failure."""
    __tracebackhide__ = True
    playwright = pytest.importorskip('playwright.sync_api')
    html = render_page_urls(Request({'type': 'http', 'root_path': PREFIX}),
                            (ROOT / 'frontend/logging_monitor.html').read_text(), '/api/logging')
    for marker, value in {'VERSION': 'test', 'SERIAL': '1', 'NAV_HASH': 'test'}.items():
        html = html.replace('%%' + marker + '%%', value)
    control = {'max_mb': 73, 'rotation_calls': 0, 'hold': False, 'status': 200,
               'posts': [], 'notifications': [], 'post_status': 200, 'hold_posts': False, 'held_posts': [],
               'post_data': {'success': True, 'apply': {'message': 'Applies on next log write'}}}
    errors, blocked, held, calls = [], [], [], []
    with playwright.sync_playwright() as driver:
        browser = driver.chromium.launch(headless=True)
        context = None
        try:
            context = browser.new_context(viewport={'width': 1280, 'height': 900}, service_workers='block')
            page = context.new_page()
            page.set_default_timeout(5000)
            page.set_default_navigation_timeout(10000)
            page.on('pageerror', lambda error: errors.append(str(error)))
            # Keep the real viewer renderer, but prevent any real socket connection.
            page.add_init_script('''window.WebSocket=class {
              static OPEN=1; static CONNECTING=0;
              constructor(){this.readyState=1;setTimeout(()=>this.onopen&&this.onopen(),0);}
              send(){} close(){this.readyState=3;}
            };''')

            def rotation_response(route):
                """Return only synthetic rotation defaults and safe dynamic rows."""
                if control['status'] != 200:
                    route.fulfill(status=control['status'], json={'detail': 'Synthetic rotation unavailable'})
                else:
                    route.fulfill(json={
                        'default': {'max_mb': control['max_mb'], 'backup_count': 0},
                        'managed_scopes': {'jobs': {'label': 'Jobs', 'description': 'Synthetic jobs',
                                                   'max_mb': 12, 'backup_count': 0}},
                        'per_service': {'Fixture': {'max_mb': 16, 'backup_count': 0}},
                    })

            def respond(route):
                """Serve the real menu, page and assets entirely within the synthetic origin."""
                url = urlparse(route.request.url)
                calls.append(url.path)
                if url.netloc == 'logging.test' and url.path == PREFIX + '/api/token-refresh':
                    # The real menu renews its cookie at startup; no real auth state is used.
                    route.fulfill(json={'success': True})
                    return
                if (url.netloc == 'logging.test' and url.path == PREFIX + '/api/notify_log'
                        and route.request.method == 'POST'):
                    control['notifications'].append(route.request.post_data_json)
                    route.fulfill(json={'success': True})
                    return
                if (url.netloc == 'logging.test' and route.request.method == 'POST'
                        and (url.path == PREFIX + '/api/logging/rotation'
                             or url.path == PREFIX + '/api/logging/purge/Fixture.log')):
                    control['posts'].append((url.path, route.request.post_data_json))
                    if control['hold_posts']:
                        control['held_posts'].append(route)
                        return
                    route.fulfill(status=control['post_status'], json=control['post_data'])
                    return
                if (url.netloc != 'logging.test' or not url.path.startswith(PREFIX + '/')
                        or route.request.method != 'GET'):
                    blocked.append((route.request.method, url.path))
                    route.abort()
                    return
                path = url.path.removeprefix(PREFIX)
                if path == '/start':
                    route.fulfill(content_type='text/html', body='''<nav id="topnav"></nav>
                      <script>window.PBGUI_NAV_CONFIG={authenticated:true,apiBase:'/mounted/api/logging',current:'welcome'};</script>
                      <script src="/mounted/app/pbgui_nav.js?v=test"></script>''')
                elif path == '/api/logging/main_page':
                    route.fulfill(body=html, content_type='text/html', headers={'Cache-Control': 'no-store'})
                elif path.startswith('/app/'):
                    asset = ROOT / 'frontend' / path.removeprefix('/app/')
                    assert asset.is_relative_to(ROOT / 'frontend') and asset.is_file(), path
                    route.fulfill(body=asset.read_bytes(), content_type={
                        '.js': 'text/javascript', '.css': 'text/css', '.svg': 'image/svg+xml',
                    }.get(asset.suffix, 'application/octet-stream'))
                elif path == '/api/logging/rotation':
                    control['rotation_calls'] += 1
                    if control['hold']:
                        held.append(route)
                    else:
                        rotation_response(route)
                elif path == '/api/logging/':
                    route.fulfill(json={'rotated': {}})
                elif path == '/api/help/index':
                    route.fulfill(json=[{'file': '31_logging', 'title': 'Logging'}])
                elif path == '/api/help/content':
                    language = parse_qs(url.query).get('lang', ['EN'])[0]
                    route.fulfill(json={'content': (ROOT / 'docs' / (
                        'help_de' if language == 'DE' else 'help') / '31_logging.md').read_text()})
                elif path.endswith('/stream'):
                    route.fulfill(status=204, body='')
                elif path in ('/api/server-status', '/api/vps/alerts', '/api/ai/preferences'):
                    route.fulfill(json={})
                else:
                    blocked.append((route.request.method, url.path))
                    route.abort()

            context.route('**/*', respond)
            yield page, playwright.expect, control, held, rotation_response, calls
            assert not errors, errors
            assert not blocked, blocked
        finally:
            try:
                if context is not None:
                    context.close()
            finally:
                browser.close()


def assert_panel(page, expect, settings):
    """Check both visible panel and active sidebar selection."""
    expect(page.locator('#settings-panel')).to_be_visible() if settings else expect(
        page.locator('#settings-panel')).to_be_hidden()
    expect(page.locator('#log-view')).to_be_hidden() if settings else expect(
        page.locator('#log-view')).to_be_visible()
    expect(page.locator('#btn-settings' if settings else '#btn-logs')).to_have_class('sb-btn active')
    expect(page.locator('#btn-logs' if settings else '#btn-settings')).to_have_class('sb-btn')


def test_settings_reload_and_ordinary_switch_preserve_navigation(logging_page):
    """Reload fetches fresh values while ordinary switches keep drafts and history state."""
    page, expect, control, _, _, _ = logging_page
    page.goto(ORIGIN + PAGE_PATH + '?other=kept#anchor')
    history_length = page.evaluate('history.length')
    page.evaluate("history.replaceState({kept:{value:42}}, '', location.href)")
    page.locator('#btn-settings').click()
    assert_panel(page, expect, True)
    expect(page.locator('#def-max-mb')).to_have_value('73')
    expect(page.locator('#def-backup-count')).to_have_value('0')
    expect(page.locator('#managed-mb-jobs')).to_have_value('12')
    page.locator('#def-max-mb').fill('81')
    page.locator('#btn-logs').click()
    page.locator('#btn-settings').click()
    expect(page.locator('#def-max-mb')).to_have_value('81')
    assert control['rotation_calls'] == 1
    assert page.url == ORIGIN + PAGE_PATH + '?other=kept&view=settings#anchor'
    assert page.evaluate('history.state') == {'kept': {'value': 42}}
    assert page.evaluate('history.length') == history_length
    control['max_mb'] = 94
    page.reload()
    assert_panel(page, expect, True)
    expect(page.locator('#def-max-mb')).to_have_value('94')
    assert control['rotation_calls'] == 2
    assert page.evaluate('history.state') == {'kept': {'value': 42}}
    page.locator('#btn-logs').click()
    page.reload()
    assert_panel(page, expect, False)
    assert page.url == ORIGIN + PAGE_PATH + '?other=kept&view=logs#anchor'
    assert control['rotation_calls'] == 2


@pytest.mark.parametrize('query,settings,normalized', [
    ('', False, ''), ('?view=logs', False, '?view=logs'),
    ('?view=settings', True, '?view=settings'),
    ('?other=kept&view=unknown', False, '?other=kept&view=logs'),
])
def test_direct_view_links(logging_page, query, settings, normalized):
    """Direct links restore allowed panels and explain an unavailable destination."""
    page, expect, control, _, _, _ = logging_page
    page.goto(ORIGIN + PAGE_PATH + query + '#anchor')
    assert_panel(page, expect, settings)
    assert page.url == ORIGIN + PAGE_PATH + normalized + '#anchor'
    if settings:
        expect(page.locator('#def-max-mb')).to_have_value('73')
        assert control['rotation_calls'] == 1
    else:
        assert control['rotation_calls'] == 0
    if 'unknown' in query:
        expect(page.locator('#logging-view-status')).to_have_text('Unknown logging view. Showing Log Viewer.')
        expect(page.locator('#logging-view-status')).to_be_visible()


@pytest.mark.parametrize('first_view', ['settings', 'logs'])
def test_unknown_view_notice_clears_on_panel_choice(logging_page, first_view):
    """Keep the initial fallback notice, then clear it on either explicit panel choice."""
    page, expect, control, _, _, _ = logging_page
    page.goto(ORIGIN + PAGE_PATH + '?other=kept&view=unknown#anchor')
    notice = page.locator('#logging-view-status')
    expect(notice).to_be_visible()
    expect(notice).to_have_text('Unknown logging view. Showing Log Viewer.')
    assert_panel(page, expect, False)
    assert control['rotation_calls'] == 0
    for view in (first_view, 'logs', 'settings'):
        page.locator('#btn-' + view).click()
        assert_panel(page, expect, view == 'settings')
        expect(notice).to_be_hidden()
        expect(notice).to_have_text('')
        assert page.url == ORIGIN + PAGE_PATH + '?other=kept&view=' + view + '#anchor'
    expect(page.locator('#def-max-mb')).to_have_value('73')
    assert control['rotation_calls'] == 1
    page.reload()
    assert_panel(page, expect, True)
    expect(notice).to_be_hidden()
    expect(notice).to_have_text('')
    expect(page.locator('#def-max-mb')).to_have_value('73')
    assert control['rotation_calls'] == 2


@pytest.mark.parametrize('status', [200, 503])
def test_delayed_rotation_never_reselects_settings(logging_page, status):
    """Late success/failure cannot pull the user back; a failed read retries on re-entry."""
    page, expect, control, held, respond, _ = logging_page
    control.update(hold=True, status=status)
    page.goto(ORIGIN + PAGE_PATH + '?view=settings')
    page.wait_for_function('rotationSettingsLoading === true')
    page.wait_for_function("document.getElementById('per-svc-loading').textContent === 'Loading…'")
    page.locator('#btn-logs').click()
    page.locator('#btn-settings').click()
    page.locator('#btn-logs').click()
    assert control['rotation_calls'] == 1
    assert len(held) == 1
    respond(held.pop())
    page.wait_for_function('rotationSettingsLoading === false')
    assert_panel(page, expect, False)
    assert page.url.endswith('?view=logs')
    if status != 200:
        control.update(hold=False, status=200)
        page.locator('#btn-settings').click()
        expect(page.locator('#def-max-mb')).to_have_value('73')
        assert control['rotation_calls'] == 2


def test_failed_restored_settings_shows_error(logging_page):
    """A failed direct Settings load keeps its destination and exposes the synthetic error."""
    page, expect, control, _, _, _ = logging_page
    control['status'] = 503
    page.goto(ORIGIN + PAGE_PATH + '?view=settings')
    expect(page.locator('#per-svc-loading')).to_have_text('Failed to load settings: Synthetic rotation unavailable')
    assert_panel(page, expect, True)
    assert page.url.endswith('?view=settings')
    control['status'] = 200
    page.reload()
    expect(page.locator('#def-max-mb')).to_have_value('73')
    assert_panel(page, expect, True)


def test_real_menu_guide_sidebar_and_mobile(logging_page):
    """Use System → Logging, separate Guide, real resize controls and mobile layout."""
    page, expect, _, _, _, calls = logging_page
    page.goto(ORIGIN + PREFIX + '/start')
    page.locator('.nav-group-btn', has_text='System').click()
    page.locator('.nav-item[data-page="system_logging"]').click()
    expect(page.locator('#btn-logs')).to_be_visible()
    assert page.url == ORIGIN + PAGE_PATH
    assert PAGE_PATH in calls
    assert page.locator('#sidebar').bounding_box()['width'] == 240
    page.locator('#btn-settings').click()
    expect(page.locator('#def-max-mb')).to_have_value('73')
    expect(page.locator('#managed-scope-body button')).to_have_text('Save')
    expect(page.locator('#per-svc-body button')).to_have_text('Save')
    before = page.url
    page.locator('#pbgui-guide-btn').click()
    expect(page.locator('#pbgui-shared-help-ovl')).to_have_class('visible')
    expect(page.locator('#pbgui-shared-help-content')).to_contain_text('Logging')
    assert page.url == before
    assert_panel(page, expect, True)

    for lang in ('en', 'de'):
        expect(page.locator('#pbgui-shared-help-lang-' + lang)).to_be_visible()
    page.locator('#pbgui-shared-help-close').click()
    for key, width in (('Home', 160), ('End', 420)):
        page.locator('#sidebar-resize').focus()
        page.keyboard.press(key)
        assert page.locator('#sidebar').bounding_box()['width'] == width
        assert page.locator('#sidebar-toolbar').evaluate('(e)=>e.scrollWidth <= e.clientWidth')
        expect(page.locator('#btn-settings')).to_be_visible()
    page.reload()
    assert_panel(page, expect, True)
    assert page.locator('#sidebar').bounding_box()['width'] == 420
    for width in (761, 760, 390):
        page.set_viewport_size({'width': width, 'height': 900})
        assert page.locator('#sidebar-toolbar').evaluate('(e)=>e.scrollWidth <= e.clientWidth')
        expect(page.locator('#btn-logs')).to_be_visible()
        expect(page.locator('#btn-settings')).to_be_visible()
        panel = page.locator('#settings-panel').bounding_box()
        assert panel['height'] > 100 and panel['y'] + panel['height'] <= 901
        if width <= 760:
            expect(page.locator('#sidebar-resize')).to_be_hidden()
    # Return to desktop: persisted width and current panel both survive.
    page.set_viewport_size({'width': 1280, 'height': 900})
    assert page.locator('#sidebar').bounding_box()['width'] == 420
    assert_panel(page, expect, True)


def rotation_controls(page, surface):
    """Locate real static controls or the rows created by the production renderer."""
    if surface == 'default':
        row = page.locator('#def-max-mb').locator('..')
        return row.locator('input').nth(0), row.locator('input').nth(1), row.locator('button'), row.locator('#def-msg')
    row = page.locator('#managed-scope-body tr' if surface == 'managed' else '#per-svc-body tr')
    return row.locator('input').nth(0), row.locator('input').nth(1), row.locator('button'), row.locator('.per-svc-msg')


@pytest.mark.parametrize('surface', ['default', 'managed', 'service'])
def test_rotation_save_formats_server_errors_on_real_rows(logging_page, surface):
    """All save surfaces show single, multiple and business errors as safe plain text."""
    page, expect, control, _, _, _ = logging_page
    page.goto(ORIGIN + PAGE_PATH + '?view=settings')
    expect(page.locator('#def-max-mb')).to_have_value('73')
    max_input, copies_input, save, message = rotation_controls(page, surface)
    max_input.fill('10')
    copies_input.fill('1')
    max_error = {'loc': ['body', 'max_mb'], 'msg': 'Input should be less than or equal to 10240',
                 'input': 'DO_NOT_RENDER_INPUT', 'ctx': {'secret': 'DO_NOT_RENDER_CTX'}}
    copies_error = {'loc': ['body', 'backup_count'], 'msg': 'Input should be less than or equal to 20'}
    max_text = 'max_mb ' + max_error['msg']
    copies_text = 'backup_count ' + copies_error['msg']
    cases = [
        (422, {'detail': [max_error]}, max_text),
        (422, {'detail': [copies_error]}, copies_text),
        (422, {'detail': [max_error, copies_error]}, max_text + '; ' + copies_text),
        (400, {'detail': 'Unknown managed log scope'}, 'Unknown managed log scope'),
        (200, {'success': False, 'detail': [max_error, copies_error]}, max_text + '; ' + copies_text),
        (200, {'success': False, 'error': [copies_error]}, copies_text),
        (200, {'success': False, 'error': 'Synthetic save rejected'}, 'Synthetic save rejected'),
        (200, {'success': False, 'detail': {'input': 'DO_NOT_RENDER_INPUT'}}, 'Save failed'),
    ]
    for index, (status, payload, expected) in enumerate(cases, 1):
        control.update(post_status=status, post_data=payload)
        with page.expect_response(lambda response: response.request.method == 'POST' and response.url.endswith('/rotation')):
            save.click()
        prefix = '' if surface == 'default' else 'Save failed: '
        expect(message).to_have_text(prefix + expected)
        expect(message).to_be_visible()
        assert len(control['posts']) == index
        expect(max_input).to_have_value('10')
        expect(copies_input).to_have_value('1')
        assert_panel(page, expect, True)
        assert 'DO_NOT_RENDER' not in message.inner_text()
        assert '[object Object]' not in message.inner_text()


@pytest.mark.parametrize('surface', ['default', 'managed', 'service'])
def test_rotation_invalid_inputs_preserve_drafts_without_post(logging_page, surface):
    """Reject empty, fractional and out-of-range values without clamping or writing."""
    page, expect, control, _, _, _ = logging_page
    page.goto(ORIGIN + PAGE_PATH + '?view=settings')
    expect(page.locator('#def-max-mb')).to_have_value('73')
    max_input, copies_input, save, message = rotation_controls(page, surface)
    for max_mb, copies, fields in [
        ('', '1', ['Max MB']), ('99999', '1', ['Max MB']), ('0', '1', ['Max MB']),
        ('1.5', '1', ['Max MB']), ('1e2', '1', ['Max MB']),
        ('10', '', ['Copies']), ('10', '25', ['Copies']),
        ('10', '-1', ['Copies']), ('10', '1.5', ['Copies']),
        ('99999', '25', ['Max MB', 'Copies']),
    ]:
        max_input.fill(max_mb)
        copies_input.fill(copies)
        save.click()
        expected = '; '.join(field + ' must be a whole number from ' + (
            '1 to 10240' if field == 'Max MB' else '0 to 20') for field in fields)
        expect(message).to_have_text(expected)
        expect(message).to_be_visible()
        expect(max_input).to_have_value(max_mb)
        expect(copies_input).to_have_value(copies)
        page.locator('#btn-logs').click()
        page.locator('#btn-settings').click()
        expect(max_input).to_have_value(max_mb)
        expect(copies_input).to_have_value(copies)
        assert_panel(page, expect, True)
    assert control['posts'] == []
    assert control['rotation_calls'] == 1
    max_input.fill('10')
    copies_input.fill('1')
    with page.expect_response(lambda response: response.request.method == 'POST' and response.url.endswith('/rotation')):
        save.click()
    expect(message).to_have_text('Applies on next log write')
    expect(message).to_be_visible()
    assert len(control['posts']) == 1


@pytest.mark.parametrize('surface,scope', [('default', 'default'), ('managed', 'managed:jobs'), ('service', 'Fixture')])
def test_rotation_valid_boundaries_keep_apply_message(logging_page, surface, scope):
    """Valid limits reach only the mock and keep the server apply message and edits."""
    page, expect, control, _, _, _ = logging_page
    page.goto(ORIGIN + PAGE_PATH + '?view=settings')
    expect(page.locator('#def-max-mb')).to_have_value('73')
    max_input, copies_input, save, message = rotation_controls(page, surface)
    for index, (max_mb, copies) in enumerate([('1', '0'), ('10240', '20')], 1):
        max_input.fill(max_mb)
        copies_input.fill(copies)
        with page.expect_response(lambda response: response.request.method == 'POST' and response.url.endswith('/rotation')):
            save.click()
        expect(message).to_have_text('Applies on next log write')
        expect(message).to_be_visible()
        assert len(control['posts']) == index
        assert control['posts'][-1] == (PREFIX + '/api/logging/rotation', {
            'scope': scope, 'max_mb': int(max_mb), 'backup_count': int(copies)})
        expect(max_input).to_have_value(max_mb)
        expect(copies_input).to_have_value(copies)
    assert control['rotation_calls'] == 1


def track_rotation_saves(page):
    """Keep request promises so assertions run after response handlers, not just HTTP events."""
    page.evaluate('''() => {
      window.loggingSaveRequests = [];
      const originalPost = apiPost;
      apiPost = function(path, body) {
        const request = originalPost(path, body);
        loggingSaveRequests.push(request);
        return request;
      };
    }''')


def settle_rotation_saves(page):
    """Drain both the POST promises and their success/error render callbacks."""
    page.evaluate('''async () => {
      await Promise.allSettled(loggingSaveRequests);
      await Promise.resolve();
      await Promise.resolve();
    }''')


@pytest.mark.parametrize('surface', ['default', 'managed', 'service'])
@pytest.mark.parametrize('outcome', ['success', 'http_error', 'business_error', 'network_error'])
def test_local_validation_supersedes_older_save_responses(logging_page, surface, outcome):
    """A local rejection remains current when an older POST later succeeds or fails."""
    page, expect, control, _, _, _ = logging_page
    page.goto(ORIGIN + PAGE_PATH + '?view=settings')
    expect(page.locator('#def-max-mb')).to_have_value('73')
    track_rotation_saves(page)
    maximum, copies, save, message = rotation_controls(page, surface)
    maximum.fill('10')
    copies.fill('1')
    control['hold_posts'] = True
    with page.expect_request(lambda request: request.method == 'POST' and request.url.endswith('/rotation')):
        save.click()
    assert len(control['held_posts']) == 1
    maximum.fill('99999')
    copies.fill('25')
    save.click()
    expected = 'Max MB must be a whole number from 1 to 10240; Copies must be a whole number from 0 to 20'
    expect(message).to_have_text(expected)
    assert len(control['posts']) == 1
    older = control['held_posts'].pop()
    if outcome == 'success':
        older.fulfill(json={'success': True, 'apply': {'message': 'Obsolete success'}})
    elif outcome == 'http_error':
        older.fulfill(status=422, json={'detail': [{'loc': ['body', 'max_mb'], 'msg': 'Obsolete validation'}]})
    elif outcome == 'business_error':
        older.fulfill(json={'success': False, 'detail': 'Obsolete business error'})
    else:
        older.abort('failed')
    settle_rotation_saves(page)
    expect(message).to_have_text(expected)
    expect(message).to_be_visible()
    expect(message).to_have_css('opacity', '1')
    expect(maximum).to_have_value('99999')
    expect(copies).to_have_value('25')
    assert_panel(page, expect, True)
    assert len(control['posts']) == 1


@pytest.mark.parametrize('surface', ['default', 'managed', 'service'])
def test_newer_valid_save_supersedes_older_response(logging_page, surface):
    """Out-of-order valid save results must leave the newest server status in place."""
    page, expect, control, _, _, _ = logging_page
    page.goto(ORIGIN + PAGE_PATH + '?view=settings')
    expect(page.locator('#def-max-mb')).to_have_value('73')
    track_rotation_saves(page)
    maximum, copies, save, message = rotation_controls(page, surface)
    maximum.fill('10')
    copies.fill('1')
    control['hold_posts'] = True
    with page.expect_request(lambda request: request.method == 'POST' and request.url.endswith('/rotation')):
        save.click()
    assert len(control['held_posts']) == 1
    maximum.fill('11')
    with page.expect_request(lambda request: request.method == 'POST' and request.url.endswith('/rotation')):
        save.click()
    assert len(control['held_posts']) == 2
    older, newer = control['held_posts']
    control['held_posts'].clear()
    newer.fulfill(json={'success': True, 'apply': {'message': 'Applies on next log write'}})
    expect(message).to_have_text('Applies on next log write')
    older.fulfill(json={'success': True, 'apply': {'message': 'Obsolete success'}})
    settle_rotation_saves(page)
    expect(message).to_have_text('Applies on next log write')
    expect(message).to_have_css('opacity', '1')
    expect(maximum).to_have_value('11')
    assert len(control['posts']) == 2


@pytest.mark.parametrize('surface', ['default', 'managed', 'service'])
def test_older_success_timer_cannot_hide_newer_local_error(logging_page, surface):
    """Expire the actual status timer and check opacity, which visibility alone misses."""
    page, expect, control, _, _, _ = logging_page
    page.goto(ORIGIN + PAGE_PATH + '?view=settings')
    expect(page.locator('#def-max-mb')).to_have_value('73')
    maximum, copies, save, message = rotation_controls(page, surface)
    maximum.fill('10')
    copies.fill('1')
    with page.expect_response(lambda response: response.request.method == 'POST' and response.url.endswith('/rotation')):
        save.click()
    expect(message).to_have_text('Applies on next log write')
    maximum.fill('99999')
    save.click()
    expected = 'Max MB must be a whole number from 1 to 10240'
    expect(message).to_have_text(expected)
    # The longest status timer is 2500 ms; table opacity transitions take 300 ms.
    page.wait_for_timeout(3000)
    expect(message).to_have_text(expected)
    expect(message).to_be_visible()
    expect(message).to_have_css('opacity', '1')
    expect(maximum).to_have_value('99999')
    assert len(control['posts']) == 1


def test_get_and_post_share_resilient_error_formatting(logging_page):
    """Both HTTP helpers handle malformed details/JSON without exposing payloads."""
    page, _, _, _, _, _ = logging_page
    page.goto(ORIGIN + PAGE_PATH)
    response = {}

    def respond(route):
        """Intercept the probe for both methods with the exact same response."""
        route.fulfill(**response)

    page.route('**/api/logging/probe', respond)
    cases = [
        ({'detail': 'Unknown managed log scope'}, 'Unknown managed log scope'),
        ({'detail': [{'loc': ['body', 'rules', 0, 'max_mb'], 'msg': '<b>Invalid</b>'},
                     {'loc': ['query', 'backup_count'], 'msg': 'Too many'}]},
         'rules.0.max_mb <b>Invalid</b>; backup_count Too many'),
        ({'detail': [{'loc': ['body'], 'msg': 'Rejected'}]}, 'value Rejected'),
        ({'detail': [None, 'bad', 7, [], {'msg': 'Rejected'}, {'loc': 'bad'},
                     {'loc': ['body', {'secret': 'HIDDEN'}], 'msg': {'secret': 'HIDDEN'}}]},
         'value Rejected; value is invalid; value is invalid'),
        ({'detail': [None, 'bad', []]}, 'HTTP 422'),
        ({'detail': []}, 'HTTP 422'), ({'detail': {}}, 'HTTP 422'),
        ({'detail': ''}, 'HTTP 422'), ({'detail': '  '}, 'HTTP 422'),
        ({'detail': None}, 'HTTP 422'), ({}, 'HTTP 422'), (None, 'HTTP 422'),
    ]
    for payload, expected in cases:
        response.clear()
        response.update(status=422, json=payload)
        messages = page.evaluate('''async () => {
          const results = [];
          for (const request of [() => apiGet('/probe'), () => apiPost('/probe', {})]) {
            try { await request(); results.push('unexpected success'); }
            catch (error) { results.push(error.message); }
          }
          return results;
        }''')
        assert messages == [expected, expected]
    for status in (503, 200):
        response.clear()
        response.update(status=status, content_type='text/html', body='<html>not JSON</html>')
        results = page.evaluate('''async () => {
          const results = [];
          for (const request of [() => apiGet('/probe'), () => apiPost('/probe', {})]) {
            try { await request(); results.push({ok: true}); }
            catch (error) { results.push({message: error.message, name: error.name}); }
          }
          return results;
        }''')
        if status == 503:
            assert [result['message'] for result in results] == ['HTTP 503', 'HTTP 503']
        else:
            assert [result['name'] for result in results] == ['SyntaxError', 'SyntaxError']


@pytest.mark.parametrize('http_error', [True, False])
def test_purge_http_and_business_errors_use_safe_shared_dialog(logging_page, http_error):
    """A synthetic file and fully intercepted purge cover both failure branches."""
    page, expect, control, _, _, _ = logging_page
    detail = [{'loc': ['body', 'filename'], 'msg': '<b>Synthetic rejection</b>', 'input': 'HIDDEN'},
              {'loc': ['body'], 'msg': 'Try later'}]
    control.update(post_status=422 if http_error else 200, post_data={'success': False, 'detail': detail})
    page.goto(ORIGIN + PAGE_PATH)
    page.evaluate("onViewerFileChange('Fixture.log')")
    page.locator('#purge-btn').click()
    expect(page.locator('#pbgui-dialog-title')).to_have_text('Purge log file')
    expect(page.locator('#purge-btn')).to_be_disabled()
    page.locator('#pbgui-dialog-accept').click()
    expect(page.locator('#pbgui-dialog-title')).to_have_text('Purge error' if http_error else 'Purge failed')
    expect(page.locator('#pbgui-dialog-message')).to_have_text('filename <b>Synthetic rejection</b>; value Try later')
    assert page.locator('#pbgui-dialog-message b').count() == 0
    expect(page.locator('#purge-btn')).to_be_enabled()
    assert control['posts'] == [(PREFIX + '/api/logging/purge/Fixture.log', {})]
    assert len(control['notifications']) == 1
    assert control['notifications'][0]['msg'] == 'filename <b>Synthetic rejection</b>; value Try later'
    page.locator('#pbgui-dialog-accept').click()
    assert page.evaluate('purgePending') is False
