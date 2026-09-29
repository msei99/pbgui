"""Offline real-menu browser coverage for optional shared Hyperliquid key changes."""
from pathlib import Path
from urllib.parse import urlparse

from playwright.sync_api import expect, sync_playwright
import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def editor():
    """Intercept every request and keep all credential actions inside browser fixtures."""
    html = (ROOT / 'frontend/api_keys_editor.html').read_text()
    for marker, value in {'API_BASE': '/prefix/api/api-keys', 'VERSION': 'test', 'SERIAL': '1', 'NAV_HASH': 'test'}.items():
        html = html.replace('%%' + marker + '%%', value)
    html = html.replace('/app/', '/prefix/app/')
    accounts = [{'name': name, 'wallet_address': '0x' + str(i)*40, 'is_vault': name != 'main'}
                for i, name in enumerate(('vault-a', 'vault-b', 'main'), 1)]
    state = {'accounts': accounts, 'token': 'preview-1', 'conflict': False, 'unavailable': False}
    writes, reads, errors = [], [], []
    with sync_playwright() as driver:
        browser = driver.chromium.launch(headless=True)
        page = browser.new_page(viewport={'width': 1280, 'height': 900})
        page.clock.install()
        page.on('pageerror', lambda error: errors.append(str(error)))

        def respond(route):
            """Serve local page assets and deterministic API responses only."""
            path = urlparse(route.request.url).path
            if path == '/prefix/app/start.html':
                route.fulfill(content_type='text/html', body='''<nav id="topnav"></nav><script>
                window.PBGUI_NAV_CONFIG={current:'welcome',authenticated:true,apiBase:'/prefix/api'};
                </script><script src="/prefix/app/pbgui_nav.js?v=test"></script>''')
            elif path == '/prefix/api/api-keys/main_page':
                route.fulfill(content_type='text/html', body=html)
            elif path.startswith('/prefix/app/'):
                asset = ROOT / 'frontend' / path.removeprefix('/prefix/app/')
                if asset.is_file():
                    route.fulfill(body=asset.read_bytes(), content_type='text/css' if asset.suffix == '.css' else 'text/javascript')
                else:
                    route.fulfill(status=404, body='')
            elif path == '/prefix/api/api-keys/exchanges':
                route.fulfill(json={'exchanges': ['hyperliquid', 'bybit'], 'passphrase_exchanges': [], 'v7_exchanges': ['hyperliquid']})
            elif path == '/prefix/api/api-keys/':
                route.fulfill(json=[dict(account, exchange='hyperliquid', has_private_key=True, in_use=False) for account in accounts])
            elif path == '/prefix/api/api-keys/meta':
                route.fulfill(json={'api_serial': 1, 'capabilities': {'combined_user_update': True}})
            elif path == '/prefix/api/api-keys/hyperliquid/shared-key/preview':
                reads.append(route.request.post_data_json)
                if state['unavailable']:
                    route.fulfill(status=503, json={'detail': 'Preview temporarily unavailable'})
                else:
                    route.fulfill(json={'accounts': state['accounts'], 'preview_token': state['token']})
            elif path.startswith('/prefix/api/api-keys/') and path.rsplit('/', 1)[-1] in ('vault-a', 'vault-b', 'main'):
                name = path.rsplit('/', 1)[-1]
                account = next(account for account in accounts if account['name'] == name)
                if route.request.method == 'PUT':
                    writes.append(route.request.post_data_json)
                    if state['conflict']:
                        state['token'] = 'preview-2'
                        route.fulfill(status=409, json={'detail': 'The shared-key preview changed. Review and confirm again.'})
                    else:
                        route.fulfill(json=dict(account, shared_key_updated_users=[a['name'] for a in state['accounts']]
                                                if 'shared_private_key_preview' in writes[-1] else []))
                else:
                    route.fulfill(json=dict(account, exchange='hyperliquid', private_key_masked='********', in_use=False))
            elif path == '/prefix/api/help/index':
                route.fulfill(json=[{'file': '20_api_keys', 'title': 'API Keys'}])
            elif path == '/prefix/api/help/content':
                route.fulfill(json={'content': '# API Keys\n\nShared private keys.'})
            elif path.endswith('/stream'):
                route.fulfill(status=204, body='')
            else:
                route.fulfill(json={})

        page.route('**/*', respond)
        page.goto('http://keys.test/prefix/app/start.html')
        page.locator('.nav-group-btn', has_text='System').click()
        page.locator('.nav-item[data-page="system_api_keys"]').click()
        expect(page.locator('#userTableBody')).to_contain_text('vault-a')
        page.locator('tr[data-user-name="vault-a"]').click()
        expect(page.locator('#editPrivateKey')).to_be_visible()
        yield page, state, writes, reads, errors
        assert not errors
        browser.close()


def test_group_preview_confirmation_conflict_and_success(editor):
    """Verify default opt-out, explicit confirmation, stale rejection and grouped success."""
    page, state, writes, reads, _ = editor
    page.locator('#editPrivateKey').fill('cd' * 32)
    page.clock.fast_forward(300)
    expect(page.locator('#sharedKeyGroup')).to_be_visible()
    expect(page.locator('#sharedKeyApplyAll')).not_to_be_checked()
    page.locator('#sharedKeyApplyAll').check()
    expect(page.locator('#sharedKeyAccounts')).to_contain_text('Main account')
    expect(page.locator('#sharedKeyAccounts')).to_contain_text('vault-b')
    page.locator('#btnSave').click()
    expect(page.locator('#pbgui-dialog-detail')).to_contain_text('Stopped bots stay stopped')
    page.locator('#pbgui-dialog-ovl').click(position={'x': 2, 'y': 2}, force=True)
    expect(page.locator('#pbgui-dialog-accept')).to_be_visible()
    page.locator('#pbgui-dialog-cancel').click()
    assert not writes
    expect(page.locator('#editPrivateKey')).to_have_value('cd' * 32)
    state['conflict'] = True
    page.locator('#btnSave').click()
    page.locator('#pbgui-dialog-accept').click()
    expect(page.locator('#sharedKeyApplyAll')).to_be_enabled()
    expect(page.locator('#editPanel')).to_be_visible()
    assert len(writes) == 1 and writes[0]['shared_private_key_preview'] == 'preview-1'
    page.locator('#alertModalOverlay button', has_text='OK').click()
    state['conflict'] = False
    page.wait_for_function('sharedKeyPreview && sharedKeyPreview.preview_token === "preview-2"')
    page.locator('#btnSave').click()
    page.locator('#pbgui-dialog-accept').click()
    expect(page.locator('#userListView')).to_be_visible()
    assert len(writes) == 2 and writes[1]['shared_private_key_preview'] == 'preview-2'
    assert writes[1]['private_key'] == 'cd' * 32
    count = len(reads)
    page.clock.fast_forward(10000)
    assert len(reads) == count
    assert page.evaluate('JSON.stringify(localStorage) + JSON.stringify(sessionStorage)').find('cd' * 32) == -1


def test_group_updates_preserve_edits_and_navigation(editor):
    """Auto updates preserve edits, reset consent on membership changes, and restore navigation."""
    page, state, writes, _, _ = editor
    page.locator('#editPrivateKey').fill('cd' * 32)
    page.clock.fast_forward(300)
    expect(page.locator('#sharedKeyApplyAll')).to_be_enabled()
    page.locator('#sharedKeyApplyAll').check()
    page.locator('#editWallet').fill('0xunsaved-wallet')
    state['accounts'] = state['accounts'][:2]
    page.clock.fast_forward(5000)
    expect(page.locator('#sharedKeyApplyAll')).not_to_be_checked()
    expect(page.locator('#sharedKeyLabel')).to_contain_text('(2)')
    expect(page.locator('#editWallet')).to_have_value('0xunsaved-wallet')
    expect(page.locator('#editPrivateKey')).to_have_value('cd' * 32)
    for key, width in [('Home', 160), ('End', 420)]:
        page.locator('#sidebar-resize').focus(); page.keyboard.press(key)
        assert page.locator('#sidebar').bounding_box()['width'] == width
        assert page.locator('#sidebar-toolbar').evaluate('(e) => e.scrollWidth <= e.clientWidth')
    page.locator('#pbgui-guide-btn').click()
    expect(page.locator('#pbgui-shared-help-ovl')).to_have_class('visible')
    assert '#edit/vault-a' in page.url
    page.reload()
    expect(page.locator('#editPanelTitle')).to_contain_text('vault-a')
    expect(page.locator('#editPrivateKey')).to_have_value('')
    expect(page.locator('#sharedKeyGroup')).to_be_hidden()
    assert page.locator('#sidebar').bounding_box()['width'] == 420
    for width in (759, 761):
        page.set_viewport_size({'width': width, 'height': 900})
        assert page.locator('#sidebar-toolbar').evaluate('(e) => e.scrollWidth <= e.clientWidth')
    assert not writes


def test_default_single_save_and_unavailable_selected_preview(editor):
    """A lost preview cannot silently turn a previously selected bulk save into a single save."""
    page, state, writes, _, _ = editor
    page.locator('#editPrivateKey').fill('cd' * 32)
    page.clock.fast_forward(300)
    expect(page.locator('#sharedKeyGroup')).to_be_visible()
    page.locator('#sharedKeyApplyAll').check()
    state['unavailable'] = True
    page.clock.fast_forward(5000)
    expect(page.locator('#sharedKeyLabel')).to_contain_text('unavailable')
    page.locator('#btnSave').click()
    expect(page.locator('#btnSave')).to_be_enabled()
    assert not writes
    page.locator('#alertModalOverlay button', has_text='OK').click()
    state['unavailable'] = False
    page.clock.fast_forward(5000)
    expect(page.locator('#sharedKeyApplyAll')).to_be_enabled()
    page.locator('#sharedKeyApplyAll').uncheck()
    page.locator('#btnSave').click()
    expect(page.locator('#userListView')).to_be_visible()
    assert len(writes) == 1
    assert 'shared_private_key_preview' not in writes[0]


def test_account_runtime_hosts_and_automatic_updates(editor):
    """The real account table shows hosts, polls state and retains deletion protection."""
    page, _, _, _, _ = editor
    users = [dict(name='vault-a', exchange='hyperliquid', in_use=True,
                  bot_runtime=[dict(pb_version=7, running_on=['manibot63'], status='synced'),
                               dict(pb_version=8, running_on=[], status='disabled')]),
             dict(name='vault-b', exchange='hyperliquid', in_use=True,
                  bot_runtime=[dict(pb_version=8, running_on=[], status='disabled')]),
             dict(name='main', exchange='hyperliquid', in_use=False, bot_runtime=[])]
    page.route('**/prefix/api/api-keys/', lambda route: route.fulfill(json=users))
    page.evaluate('backToList()')
    row = page.locator('tr[data-user-name="vault-a"]')
    expect(row).to_contain_text('manibot63')
    expect(row).not_to_contain_text('Disabled')
    expect(row.locator('.badge-in-use')).to_have_count(1)
    expect(row.locator('.badge-in-use')).to_have_attribute('title', 'PB7 running on manibot63')
    disabled = page.locator('tr[data-user-name="vault-b"]')
    expect(disabled).to_contain_text('Disabled')
    expect(disabled.locator('[data-user-action="delete"]')).to_have_count(0)
    expect(page.locator('tr[data-user-name="main"]')).to_contain_text('Unused')
    page.locator('#userFilter').fill('vault')
    users[0]['bot_runtime'][0].update(running_on=[], status='activate_needed', enabled_on='manibot63')
    page.clock.fast_forward(15001)
    expect(row).to_contain_text('Stopped')
    expect(row).not_to_contain_text('manibot63')
    expect(page.locator('#userFilter')).to_have_value('vault')
    users[0]['bot_runtime'][0].update(running_on=['<img src=x onerror=alert(1)>'], status='synced')
    page.clock.fast_forward(15001)
    expect(row).to_contain_text('<img src=x onerror=alert(1)>')
    expect(row.locator('img')).to_have_count(0)
    row.click()
    page.locator('#editWallet').fill('unsaved-value')
    page.clock.fast_forward(30001)
    expect(page.locator('#editWallet')).to_have_value('unsaved-value')
