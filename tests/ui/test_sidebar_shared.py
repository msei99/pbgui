"""Exercise shared sidebars using real page markup, styles and browser input."""

from pathlib import Path
import re
from urllib.parse import urlparse

import pytest

ROOT = Path(__file__).resolve().parents[2]
FRONTEND = ROOT / 'frontend'
SIDEBAR_PAGES = sorted(
    path.name for path in FRONTEND.glob('*.html')
    if re.search(r'<(?:aside|div)\b[^>]*\bid=[\'\"]sidebar[\'\"]', path.read_text())
)


@pytest.fixture(scope='module')
def browser():
    """Launch an isolated browser; every resource request is intercepted below."""
    playwright = pytest.importorskip('playwright.sync_api')
    with playwright.sync_playwright() as driver:
        instance = driver.chromium.launch(headless=True)
        yield instance
        instance.close()


def _open_sidebar(page, name, *, upgrade_backtest=False):
    """Load the actual layout and CSS without application scripts or live API access."""
    source = (FRONTEND / name).read_text()
    source = source.replace('href="./', 'href="/app/').replace('src="./', 'src="/app/')
    html = re.sub(r'<script\b[^>]*>.*?</script>', '', source, flags=re.S | re.I)
    html = html.replace('</head>', '<style>#topnav{height:52px;min-height:52px;flex-shrink:0}</style></head>')
    scripts = ''
    if upgrade_backtest:
        scripts += '''<script src="/app/js/backtest_shell.js"></script><script>
          PBGuiBacktestShell.upgradeLegacy({source:document.getElementById('page-body')});
        </script>'''
    scripts += '<script src="/app/js/sidebar_resize.js"></script>'
    html = html.replace('</body>', scripts + '</body>') if '</body>' in html else html + scripts

    def respond(route):
        """Serve only local assets and empty documents for embedded frames."""
        path = urlparse(route.request.url).path
        if path == '/audit/' + name:
            route.fulfill(body=html, content_type='text/html')
        elif path.startswith('/app/'):
            asset = FRONTEND / path.removeprefix('/app/')
            if asset.is_file():
                mime = 'text/css' if asset.suffix == '.css' else 'text/javascript' if asset.suffix == '.js' else 'text/html'
                route.fulfill(body=asset.read_bytes(), content_type=mime)
            else:
                route.fulfill(status=404, body='')
        else:
            route.fulfill(body='', content_type='text/html')

    page.route('**/*', respond)
    page.goto('http://sidebar.test/audit/' + name)


def test_sidebar_assets_are_loaded_once_in_the_required_order():
    """The shared layout and appearance must load once, after all page-specific styles."""
    assert SIDEBAR_PAGES
    for name in SIDEBAR_PAGES:
        source = (FRONTEND / name).read_text().replace('./css/', '/app/css/').replace('./js/', '/app/js/')
        for asset in ('css/sidebar.css?v=6', 'css/sidebar_layout.css?v=4', 'js/sidebar_resize.js?v=3'):
            assert source.count('/app/' + asset) == 1, name
        assert source.index('/app/css/sidebar_layout.css?v=4') > source.rindex('</style>'), name
        assert re.search(r'\bid=[\'\"]sidebar-resize[\'\"]', source), name


@pytest.mark.parametrize('name', SIDEBAR_PAGES)
def test_actual_page_sidebar_fits_desktop_and_mobile(browser, name):
    """Real sidebar content and main panels remain bounded, including status banners."""
    page = browser.new_page(viewport={'width': 1440, 'height': 900})
    try:
        _open_sidebar(page, name)
        page.locator('#sidebar-resize').focus()
        page.keyboard.press('End')
        for width in (1440, 850, 740, 600, 390):
            page.set_viewport_size({'width': width, 'height': 900})
            state = page.evaluate('''() => {
              const root=document.getElementById('page-body'), sb=document.getElementById('sidebar');
              const main=[...root.children].find(e=>e!==sb && !['SCRIPT','STYLE'].includes(e.tagName));
              const mainRect=main.getBoundingClientRect();
              return {width:sb.getBoundingClientRect().width, mainHeight:mainRect.height,
                mainBottom:mainRect.bottom, mainWidth:mainRect.width,
                handle:getComputedStyle(document.getElementById('sidebar-resize')).display,
                saved:localStorage.getItem('pbgui.sidebar.width')};
            }''')
            assert state['saved'] == '420', (name, width, state)
            assert state['mainHeight'] >= 120, (name, width, state)
            if width <= 760:
                assert round(state['width']) == width, (name, width, state)
                assert state['mainBottom'] <= 901, (name, width, state)
                assert state['handle'] == 'none', (name, width, state)
            else:
                assert round(state['width']) == 420, (name, width, state)
                assert state['mainWidth'] >= 300, (name, width, state)
                assert state['handle'] != 'none', (name, width, state)
    finally:
        page.close()


def test_drag_captures_pointer_and_survives_frames_navigation_and_blur(browser):
    """Dragging over an iframe or losing window focus cannot leave resizing stuck."""
    page = browser.new_page(viewport={'width': 1200, 'height': 800})
    try:
        _open_sidebar(page, 'dashboard_main.html')
        page.evaluate('''() => {
          const frame=document.createElement('iframe');
          frame.srcdoc='<body>Embedded content</body>';
          frame.style.cssText='position:absolute;inset:0;width:100%;height:100%;border:0';
          document.getElementById('main-content').appendChild(frame);
        }''')
        handle = page.locator('#sidebar-resize')
        box = handle.bounding_box()
        x, y = box['x'] + box['width'] / 2, box['y'] + 30
        page.mouse.move(x, y)
        page.mouse.down()
        assert handle.evaluate('(e)=>e.hasPointerCapture(1)')
        page.mouse.move(x + 110, y)
        page.mouse.up()
        assert round(page.locator('#sidebar').bounding_box()['width']) == 350
        assert page.evaluate("localStorage.getItem('pbgui.sidebar.width')") == '350'
        _open_sidebar(page, 'welcome.html')
        page.reload()
        assert round(page.locator('#sidebar').bounding_box()['width']) == 350
        page.set_viewport_size({'width': 390, 'height': 800})
        assert round(page.locator('#sidebar').bounding_box()['width']) == 390
        page.set_viewport_size({'width': 1200, 'height': 800})
        assert round(page.locator('#sidebar').bounding_box()['width']) == 350
        handle = page.locator('#sidebar-resize')
        box = handle.bounding_box()
        x, y = box['x'] + box['width'] / 2, box['y'] + 30
        page.mouse.move(x, y)
        page.mouse.down()
        page.mouse.move(x - 40, y)
        page.evaluate("window.dispatchEvent(new Event('blur'))")
        page.mouse.move(x - 100, y)
        page.mouse.up()
        assert round(page.locator('#sidebar').bounding_box()['width']) == 310
        assert not page.evaluate("document.documentElement.classList.contains('pbgui-sidebar-resizing')")
        assert page.evaluate("localStorage.getItem('pbgui.sidebar.width')") == '310'
    finally:
        page.close()


@pytest.mark.parametrize(('stored', 'expected'), [('invalid', 240), ('', 240), ('-1', 160), ('9000', 420)])
def test_saved_width_is_validated(browser, stored, expected):
    """Malformed or obsolete browser values cannot break the page layout."""
    import json

    page = browser.new_page()
    try:
        page.add_init_script("localStorage.setItem('pbgui.sidebar.width', " + json.dumps(stored) + ')')
        _open_sidebar(page, 'services_monitor.html')
        assert round(page.locator('#sidebar').bounding_box()['width']) == expected
    finally:
        page.close()


def test_backtest_generated_sidebar_uses_the_same_layout(browser):
    """The sidebar created by the PB7/PB8 backtest shell also resizes and fits mobile."""
    page = browser.new_page(viewport={'width': 1200, 'height': 800})
    errors = []
    page.on('pageerror', lambda error: errors.append(str(error)))
    try:
        _open_sidebar(page, 'v7_backtest.html', upgrade_backtest=True)
        handle = page.locator('#sidebar-resize')
        assert handle.get_attribute('data-sidebar-resize-bound') == 'true'
        handle.focus()
        page.keyboard.press('End')
        assert round(page.locator('#sidebar').bounding_box()['width']) == 420
        page.set_viewport_size({'width': 390, 'height': 800})
        main = page.locator('#main-content').bounding_box()
        assert main['height'] >= 120
        assert main['y'] + main['height'] <= 801
        assert not errors
    finally:
        page.close()


@pytest.mark.parametrize('name', SIDEBAR_PAGES)
def test_sidebar_controls_share_spacing_and_states(browser, name):
    """Real controls and dynamically added variants keep the same geometry and state colors."""
    page = browser.new_page(viewport={'width': 1200, 'height': 800})
    try:
        _open_sidebar(page, name)
        # Exercise the classes used by dynamically rendered dashboard/VPS/account lists too.
        page.evaluate('''() => {
          const group = document.createElement('div'); group.id = 'appearance-fixture';
          for (const cls of ['sb-btn', 'sb-section', 'sb-btn primary', 'sb-btn info',
             'sb-btn danger', 'sb-btn warning', 'sb-btn ok', 'sb-btn active',
             'sb-btn save-needed', 'account-button selected', 'side-button', 'sb-item']) {
            const button = document.createElement('button'); button.className = cls;
            button.textContent = 'Sample'; group.appendChild(button);
          }
          const hidden = document.createElement('button'); hidden.className = 'sb-btn';
          hidden.hidden = true; hidden.id = 'hidden-sidebar-action'; group.appendChild(hidden);
          document.getElementById('sidebar').appendChild(group);
        }''')
        for width in (1200, 390):
            page.set_viewport_size({'width': width, 'height': 800})
            controls = page.evaluate('''() => [...document.querySelectorAll(
              '#sidebar .sb-btn, #sidebar .sb-section, #sidebar .account-button, #sidebar .side-button, #sidebar .sb-item'
            )].filter(e => e.getClientRects().length).map(e => {
              const s=getComputedStyle(e);
              return {id:e.id, cls:e.className, compact:!!e.closest(".sb-icon-row"), font:s.fontSize, radius:s.borderRadius,
                padding:s.padding, minHeight:s.minHeight, border:s.borderLeftWidth};
            })''')
            assert controls
            for control in controls:
                assert control['font'] == '13px', (name, width, control)
                assert control['radius'] == '6px', (name, width, control)
                assert control['padding'] == ('0px' if control['compact'] else '6px 10px'), (name, width, control)
                assert control['minHeight'] == '32px', (name, width, control)
                assert control['border'] == '1px', (name, width, control)
            assert not page.locator('#hidden-sidebar-action').is_visible()
        colors = page.locator('#appearance-fixture').evaluate('''e => Object.fromEntries(
          [...e.children].map(b => [b.className, getComputedStyle(b).backgroundColor]))''')
        assert colors['sb-btn'] != colors['sb-btn active']
        assert colors['sb-btn active'] == colors['account-button selected']
        assert colors['sb-btn warning'] == colors['sb-btn save-needed']
        assert len({colors['sb-btn primary'], colors['sb-btn info'], colors['sb-btn danger'],
                    colors['sb-btn warning'], colors['sb-btn ok']}) == 5
        page.set_viewport_size({'width': 1200, 'height': 800})
        regions = page.evaluate('''() => [...document.querySelectorAll(
          '#sidebar-sticky, #sidebar-inner, #sidebar-list, #sidebar-list-wrap, #sidebar-toolbar, #sidebar-actions'
        )].map(e => ({id:e.id, padding:getComputedStyle(e).padding, gap:getComputedStyle(e).gap}))''')
        for region in regions:
            if region['id'] == 'sidebar-sticky':
                assert region['padding'] == '8px 8px 0px', (name, region)
            elif region['id'] in ('sidebar-toolbar', 'sidebar-actions'):
                assert region['padding'] == '0px 0px 8px', (name, region)
                assert region['gap'] == '4px', (name, region)
            else:
                assert region['padding'] == '8px', (name, region)
                assert region['gap'] == '4px', (name, region)
    finally:
        page.close()


def test_long_sidebar_actions_wrap_and_remain_scrollable(browser):
    """Narrow sidebars retain complete labels and access to long dynamic action lists."""
    page = browser.new_page(viewport={'width': 1200, 'height': 700})
    try:
        _open_sidebar(page, 'vps_manager.html')
        page.locator('#sidebar-resize').focus()
        page.keyboard.press('Home')
        page.evaluate('''() => {
          const actions=document.getElementById('sidebar-actions');
          for (let i=0; i<30; i++) {
            const b=document.createElement('button'); b.className='sb-btn';
            b.textContent='Update PBGui and PB8 runtime'; actions.appendChild(b);
          }
        }''')
        state = page.evaluate('''() => {
          const s=document.getElementById('sidebar-sticky');
          const b=document.querySelector('#sidebar-actions button');
          s.scrollTop=s.scrollHeight;
          return {scrollable:s.scrollHeight>s.clientHeight, scrollTop:s.scrollTop,
            height:b.getBoundingClientRect().height, clipped:b.scrollWidth>b.clientWidth,
            bottom:s.getBoundingClientRect().bottom};
        }''')
        assert state['scrollable'] and state['scrollTop'] > 0
        assert state['height'] > 32 and not state['clipped']
        assert state['bottom'] <= 701
    finally:
        page.close()


def test_dashboard_real_toolbar_supports_compact_icons_in_both_modes(browser):
    """Actual view/edit renderers retain compact icons, tooltips and narrow-width wrapping."""
    source = (FRONTEND / 'dashboard_main.html').read_text()
    start = source.index('  function updateViewSaveBtn()')
    end = source.index('  /* ── Build the dashboard list HTML', start)
    renderer = source[start:end]
    page = browser.new_page(viewport={'width': 1200, 'height': 800})
    try:
        _open_sidebar(page, 'dashboard_main.html')
        page.evaluate(r'''renderer => {
          const sidebarToolbar = document.getElementById('sidebar-toolbar');
          let editMode=false, viewDirty=true, currentDash='Example';
          function getDeleteTargets() { return ['Example']; }
          eval(renderer + '\nwindow.renderAuditToolbar = function(edit) {editMode=edit;renderToolbar();};');
        }''', renderer)
        for editing in (False, True):
            page.evaluate('(edit) => window.renderAuditToolbar(edit)', editing)
            buttons = page.locator('#sidebar-toolbar .sb-btn')
            assert buttons.count() == (3 if editing else 5)
            for i in range(buttons.count()):
                button = buttons.nth(i)
                assert button.get_attribute('title')
                box = button.bounding_box()
                assert box['width'] == box['height'] == 32
            assert buttons.nth(0).bounding_box()['y'] == buttons.nth(1).bounding_box()['y']
        page.evaluate('() => window.renderAuditToolbar(false)')
        page.locator('#sidebar-resize').focus()
        page.keyboard.press('Home')
        boxes = page.locator('#sidebar-toolbar .sb-btn').evaluate_all(
            '(nodes) => nodes.map(e => ({x:e.offsetLeft,y:e.offsetTop,width:e.offsetWidth}))')
        assert len({b['y'] for b in boxes}) > 1
        assert all(b['width'] == 32 and b['x'] + 32 <= 160 for b in boxes)
        page.locator('#sb-new').focus()
        assert page.locator('#sb-new').evaluate('(e) => getComputedStyle(e).outlineStyle') == 'solid'
    finally:
        page.close()


def test_balance_calculator_columns_follow_available_content_width(browser):
    """Resizing the sidebar stacks results without losing the edited configuration."""
    page = browser.new_page(viewport={'width': 1200, 'height': 900})
    try:
        _open_sidebar(page, 'balance_calc.html')
        page.locator('#config-editor').fill('{"draft": true}')
        assert page.locator('#sidebar #sel-instance').count() == 1
        assert page.locator('#sidebar #sel-exchange').count() == 1
        assert page.locator('#sidebar #btn-calc').count() == 1
        page.locator('#sidebar-resize').focus()
        page.keyboard.press('Home')
        editor = page.locator('#config-editor').bounding_box()
        results = page.locator('#results-panel').bounding_box()
        assert abs(editor['y'] - results['y']) <= 1
        assert results['x'] > editor['x']
        page.keyboard.press('End')
        editor = page.locator('#config-editor').bounding_box()
        results = page.locator('#results-panel').bounding_box()
        assert results['y'] >= editor['y'] + editor['height']
        assert abs(results['x'] - editor['x']) <= 1
        assert page.locator('#config-editor').input_value() == '{"draft": true}'
    finally:
        page.close()
