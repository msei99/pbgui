"""PB9 Run uses the production shared editor with isolated browser responses."""
import copy
import json
import mimetypes
import time
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlparse, parse_qs

import pytest
from browser_driver import sync_browser, expect

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def editor():
    """Intercept every request; never contact PBGui, an exchange or real accounts."""
    metadata = json.loads((ROOT / 'tests/fixtures/pb9_run_metadata.json').read_text())
    config = copy.deepcopy(metadata['template'])
    config['live']['account']['user'] = 'fixture'
    config['pbgui'] = {'runtime': 'pb9', 'version': 0, 'enabled_on': 'disabled', 'note': ''}
    with sync_browser() as driver:
        browser = driver.chromium.launch()
        page = browser.new_page(viewport={'width': 1440, 'height': 1000})
        page.set_default_timeout(5000)
        state = SimpleNamespace(page=page, config=config, saved=[], errors=[], unexpected=[], metadata=metadata,
                                hold_save=False, pending=[], drafts={})
        page.on('pageerror', lambda error: state.errors.append(str(error)))

        def respond(route):
            """Serve real shared pages/assets and explicit synthetic API responses."""
            url = urlparse(route.request.url)
            path = url.path
            if url.hostname != 'run9.test':
                state.unexpected.append(route.request.url)
                route.abort()
            elif path in ('/api/v9/main_page', '/api/v9/edit_page'):
                filename = 'v7_edit.html' if 'edit' in path else 'v7_run.html'
                source = (ROOT / 'frontend' / filename).read_text()
                for key, value in {'API_BASE': '/api/v9', 'WS_BASE': 'ws://run9.test',
                                    'RUN_VERSION': 'v9', 'VERSION': 'fixture', 'SERIAL': '1',
                                    'MASTER_NAME': 'fixture-host', 'NAV_HASH': 'test', 'BASE_PREFIX': '',
                                    'INSTANCE': parse_qs(url.query).get('name', [''])[0],
                                    'IS_NEW': 'true' if 'new=1' in url.query else 'false',
                                    'DRAFT_ID': parse_qs(url.query).get('draft_id', [''])[0]}.items():
                    source = source.replace('"%%' + key + '%%"', json.dumps(value)).replace('%%' + key + '%%', value)
                source = source.replace('%%RUN_LABEL%%', 'PBv9')
                route.fulfill(content_type='text/html', body=source)
            elif path.startswith('/app/'):
                asset = ROOT / 'frontend' / path.removeprefix('/app/')
                route.fulfill(body=asset.read_bytes() if asset.is_file() else b'',
                              content_type=mimetypes.guess_type(asset)[0] or 'text/plain')
            elif path == '/api/v9/editor/metadata':
                route.fulfill(json={**metadata, 'runtime': {'detected': True}})
            elif path == '/api/v9/instances/new-config':
                route.fulfill(json={'config': copy.deepcopy(state.config), 'param_status': {}, 'override_configs': {}})
            elif path.startswith('/api/v9/draft/'):
                route.fulfill(json={'config': copy.deepcopy(state.drafts[path.rsplit('/', 1)[1]]),
                                    'param_status': {}, 'override_configs': {}})
            elif path == '/api/v9/users':
                route.fulfill(json={'users': [{'name': 'fixture', 'exchange': 'bybit'}]})
            elif path == '/api/v9/hosts':
                route.fulfill(json={'hosts': ['disabled', 'fixture-host'], 'host_capabilities': {'fixture-host': {'pb9_capable': True}}})
            elif path.endswith('/next-version'):
                route.fulfill(json={'next_version': 1, 'version': 1})
            elif path.endswith('/config') and route.request.method == 'PUT':
                payload = route.request.post_data_json
                state.saved.append(copy.deepcopy(payload))
                state.config = copy.deepcopy(payload['config'])
                state.config['pbgui']['version'] = len(state.saved)
                result = {'ok': True, 'name': 'fixture', 'version': len(state.saved), 'config': state.config}
                if state.hold_save:
                    state.pending.append((route, copy.deepcopy(result)))
                else:
                    route.fulfill(json=result)
            elif path.endswith('/config'):
                if state.saved:
                    route.fulfill(json={'config': state.config, 'param_status': {}, 'override_configs': {}})
                else:
                    route.fulfill(status=404, json={'detail': 'Not found'})
            elif path == '/api/v9/editor/prepare':
                route.fulfill(json={'config': route.request.post_data_json['config'], 'valid': True, 'param_status': {}})
            elif path == '/api/v9/instances':
                route.fulfill(json={'instances': [{'name': 'fixture', 'status': 'disabled', 'version': 1, 'enabled_on': 'disabled', 'note': 'test'}] if state.saved else []})
            elif path.endswith('/symbols') or path.endswith('/coins/status'):
                route.fulfill(json={'symbols': [], 'catalog': [], 'statuses': {}})
            elif path.endswith('/tags'):
                route.fulfill(json={'tags': []})
            elif path.endswith('/stream'):
                route.fulfill(status=204, body='')
            else:
                route.fulfill(json={})

        page.route('**/*', respond)
        page.route_web_socket('**/ws/v9', lambda socket: None)
        page.goto('http://run9.test/api/v9/edit_page?new=1')
        try:
            yield state
        finally:
            browser.close()


def test_native_form_preserves_sparse_sides_on_note_save(editor):
    """A routine GUI edit does not materialize inherited side risk or V8 fields."""
    page = editor.page
    expect(page.locator('#f-user')).to_have_value('fixture')
    expect(page.locator('[data-config-path="bot.global.risk.max_positions"]')).to_have_count(1)
    page.locator('#f-note').fill('v9 shared editor')
    page.locator('#btn-save').click()
    page.wait_for_function('document.getElementById("f-version").value === "1"')
    assert editor.saved
    config = editor.saved[0]['config']
    assert config['config_version'] == 'v9.0.0'
    assert config['live']['account']['user'] == 'fixture'
    assert config['bot']['long'] == editor.metadata['template']['bot']['long']
    assert 'user' not in config['live']
    assert editor.errors == []
    assert editor.unexpected == []


def test_v9_run_menu_uses_shared_list_and_add_editor(editor):
    """The actual PBv9 menu opens Run and its Add action returns to the form."""
    page = editor.page
    expect(page.locator('#f-user')).to_have_value('fixture')
    page.locator('.nav-group-btn[data-group="pbv9"]').click()
    page.locator('.nav-item[data-page="v9_run"]').click()
    expect(page).to_have_url('http://run9.test/api/v9/main_page')
    page.locator('#add-instance-btn').click()
    expect(page).to_have_url('http://run9.test/api/v9/edit_page?new=1')
    expect(page.locator('[data-config-path="bot.long.enabled"]')).to_have_count(1)
    assert editor.errors == []


def test_newer_typing_survives_delayed_save(editor):
    """A successful response acknowledges its version without replacing later edits."""
    page = editor.page
    expect(page.locator('#f-user')).to_have_value('fixture')
    editor.hold_save = True
    page.locator('#f-note').fill('saved')
    page.locator('#btn-save').click()
    deadline = time.monotonic() + 5
    while not editor.pending:
        assert time.monotonic() < deadline
        page.wait_for_timeout(20)
    page.locator('#f-note').fill('newer unsaved text')
    route, result = editor.pending.pop()
    route.fulfill(json=result)
    expect(page.locator('#f-version')).to_have_value('1')
    expect(page.locator('#f-note')).to_have_value('newer unsaved text')
    assert editor.saved[0]['config']['pbgui']['note'] == 'saved'
    assert page.evaluate('dirty') is True
    assert '?name=fixture' in page.url
    assert editor.errors == []


def test_automatic_return_update_preserves_edits_and_reload_context(editor):
    """Returning updates a clean form; dirty text and open sections survive checks."""
    page = editor.page
    expect(page.locator('#f-user')).to_have_value('fixture')
    page.locator('#btn-save').click()
    expect(page.locator('#f-version')).to_have_value('1')
    editor.config['pbgui']['note'] = 'external note'
    editor.config['pbgui']['version'] = 2
    page.evaluate('document.dispatchEvent(new Event("visibilitychange"))')
    expect(page.locator('#f-note')).to_have_value('external note')
    page.locator('#f-note').fill('local draft')
    editor.config['pbgui']['note'] = 'new remote note'
    page.evaluate('document.dispatchEvent(new Event("visibilitychange"))')
    expect(page.locator('#f-note')).to_have_value('local draft')
    section = page.locator('#run-meta-bot .expander').first
    section.locator('.expander-header').click()
    section_id = section.get_attribute('id')
    expected = section.get_attribute('class')
    page.reload()
    expect(page.locator('#f-note')).to_have_value('new remote note')
    assert page.locator('#' + section_id).get_attribute('class') == expected
    assert 'name=fixture' in page.url
    assert editor.errors == []


def test_backup_draft_save_clears_draft_navigation_before_reload(editor):
    """Reload after applying a backup opens the new saved version, not its old draft."""
    page = editor.page
    expect(page.locator('#f-user')).to_have_value('fixture')
    page.locator('#btn-save').click()
    expect(page.locator('#f-version')).to_have_value('1')
    draft = copy.deepcopy(editor.config)
    draft['pbgui']['note'] = 'backup draft'
    editor.drafts['fixtureDraft'] = draft
    page.goto('http://run9.test/api/v9/edit_page?name=fixture&draft_id=fixtureDraft')
    expect(page.locator('#f-note')).to_have_value('backup draft')
    page.locator('#btn-save').click()
    expect(page.locator('#f-version')).to_have_value('2')
    assert 'draft_id' not in page.url
    page.reload()
    expect(page.locator('#f-version')).to_have_value('2')
    expect(page.locator('#f-note')).to_have_value('backup draft')
    assert editor.errors == []
