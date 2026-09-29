"""Isolated real-page regressions for Linux update counts and reusable dialog closing."""
from pathlib import Path
from urllib.parse import urlparse

import pytest
from playwright.sync_api import expect, sync_playwright

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def manager():
    """Serve actual navigation/assets with synthetic state and no remote connections."""
    html = (ROOT / 'frontend/vps_manager.html').read_text()
    for key, value in {'API_BASE': '/prefix/api/vps-manager', 'WS_BASE': 'ws://manager.test/prefix',
                       'VERSION': 'test', 'SERIAL': '1', 'NAV_HASH': 'test'}.items():
        html = html.replace('%%' + key + '%%', value)
    html = html.replace('/app/', '/prefix/app/')
    with sync_playwright() as driver:
        browser = driver.chromium.launch(headless=True)
        page = browser.new_page(viewport={'width': 1450, 'height': 1000})
        page.set_default_timeout(5000)
        errors = []
        page.on('pageerror', lambda error: errors.append(str(error)))
        page.add_init_script('''window.WebSocket = class {
          static OPEN=1; static CONNECTING=0;
          constructor() { this.readyState=1; } send() {} close() {}
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
        yield page
        assert not errors
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
    expect(page.locator('tr[data-overview-host="test-host"]')).to_contain_text('1 deferred update unclassified')
    # Missing details must still be shown as unknown, never guessed to be normal.
    publish_state(page, upgrades=21, deferred_updates=1, classification_complete=False, packages=[])
    expect(page.locator('[data-vps-action="package-updates"]')).to_have_text('? / ?')
