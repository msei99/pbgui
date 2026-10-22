"""Logging navigation with real local assets and isolated, read-only browser fixtures."""

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
    playwright = pytest.importorskip('playwright.sync_api')
    html = render_page_urls(Request({'type': 'http', 'root_path': PREFIX}),
                            (ROOT / 'frontend/logging_monitor.html').read_text(), '/api/logging')
    for marker, value in {'VERSION': 'test', 'SERIAL': '1', 'NAV_HASH': 'test'}.items():
        html = html.replace('%%' + marker + '%%', value)
    control = {'max_mb': 73, 'rotation_calls': 0, 'hold': False, 'status': 200}
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
                        'per_service': {'Fixture.log': {'max_mb': 16, 'backup_count': 0}},
                    })

            def respond(route):
                """Serve the real menu, page and assets entirely within the synthetic origin."""
                url = urlparse(route.request.url)
                calls.append(url.path)
                if url.netloc == 'logging.test' and url.path == PREFIX + '/api/token-refresh':
                    # The real menu renews its cookie at startup; no real auth state is used.
                    route.fulfill(json={'success': True})
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
