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
            elif path == '/api/help/index':
                route.fulfill(json=[{'file': '53_pbv9_run', 'title': 'PBv9 Run'}])
            elif path == '/api/help/content':
                language = parse_qs(url.query).get('lang', ['en'])[0].lower()
                directory = 'help_de' if language == 'de' else 'help'
                route.fulfill(json={'content': (ROOT / 'docs' / directory / '53_pbv9_run.md').read_text()})
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


def test_primary_fields_and_side_json_are_visible_without_expanding(editor):
    """Add PB9 exposes familiar basics while advanced fields remain grouped."""
    page = editor.page
    expect(page.locator('#f-leverage')).to_have_value('10')
    for field in ('f-leverage', 'f-margin-mode', 'f-logging-level', 'f-min-coin-age',
                  'f-v9-risk-lookback', 'f-warmup-ratio', 'f-max-loss-pct',
                  'f-price-dist', 'f-exec-delay', 'f-market-order-threshold',
                  'f-filter-min-cost', 'f-market-orders', 'f-hedge-mode', 'f-auto-gs',
                  'f-long-json', 'f-short-json'):
        assert page.locator('#' + field).is_visible(), field
    assert not page.locator('#f-pnls-lookback').is_visible()
    assert page.locator('#exp-v9-advanced').get_attribute('class') == 'expander'
    for side in ('long', 'short'):
        control = page.locator('[data-config-path="bot.' + side + '.enabled"]')
        assert control.is_visible()
        assert control.evaluate('(el) => el.closest(".form-group").querySelectorAll("input[type=checkbox]").length') == 1
    page.locator('#f-leverage').fill('7')
    page.locator('#f-auto-gs').uncheck()
    page.locator('#btn-save').click()
    expect(page.locator('#f-version')).to_have_value('1')
    config = editor.saved[-1]['config']
    assert config['live']['account']['leverage'] == 7
    assert config['live']['modes']['disapproved_position'] == 'manual'
    assert 'leverage' not in config['live']
    assert config['bot']['long'] == editor.metadata['template']['bot']['long']
    assert editor.errors == []


def test_inherited_side_json_authors_only_changed_values(editor):
    """Editing one resolved leaf keeps other defaults sparse and existing pins intact."""
    page = editor.page
    expect(page.locator('#f-user')).to_have_value('fixture')
    projection = json.loads(page.locator('#f-long-json').input_value())
    assert len(projection) > 2
    projection['risk']['max_positions'] = 4
    page.locator('#f-long-json').fill(json.dumps(projection))
    page.locator('#btn-save').click()
    expect(page.locator('#f-version')).to_have_value('1')
    assert editor.saved[-1]['config']['bot']['long'] == {**editor.metadata['template']['bot']['long'], 'risk': {'max_positions': 4}}
    expect(page.locator('#f-long-npos')).to_have_value('4')
    projection = json.loads(page.locator('#f-long-json').input_value())
    projection['enabled'] = False
    page.locator('#f-long-json').fill(json.dumps(projection))
    page.locator('#btn-save').click()
    expect(page.locator('#f-version')).to_have_value('2')
    assert editor.saved[-1]['config']['bot']['long'] == {'risk': {'max_positions': 4}, 'enabled': False}
    assert editor.errors == []


def test_overrides_json_can_remove_a_pin_and_restore_inheritance(editor):
    """The sparse JSON view provides the same override-removal semantics as fields."""
    page = editor.page
    expect(page.locator('#f-user')).to_have_value('fixture')
    page.locator('#f-long-json-view').select_option('overrides')
    assert json.loads(page.locator('#f-long-json').input_value()) == editor.metadata['template']['bot']['long']
    page.locator('#f-long-json').fill('{"risk":{"max_positions":3}}')
    page.locator('#btn-save').click()
    expect(page.locator('#f-version')).to_have_value('1')
    assert editor.saved[-1]['config']['bot']['long'] == {'risk': {'max_positions': 3}}
    page.locator('#f-long-json').fill('{}')
    page.locator('#btn-save').click()
    expect(page.locator('#f-version')).to_have_value('2')
    assert editor.saved[-1]['config']['bot']['long'] == {}
    page.reload()
    expect(page.locator('#f-user')).to_have_value('fixture')
    expect(page.locator('#f-long-json-view')).to_have_value('overrides')
    expect(page.locator('#f-long-json')).to_have_value('{}')
    assert editor.errors == []


@pytest.mark.parametrize('raw', ['{', '{"constructor":{"polluted":true}}'])
def test_invalid_side_json_survives_blur_and_blocks_save(editor, raw):
    """Incomplete or unsafe JSON is never overwritten by automatic synchronization."""
    page = editor.page
    expect(page.locator('#f-user')).to_have_value('fixture')
    page.locator('#f-long-json').fill(raw)
    page.locator('#f-note').fill('preserve invalid JSON')
    page.wait_for_timeout(250)
    expect(page.locator('#f-long-json')).to_have_value(raw)
    page.locator('#btn-save').click()
    page.wait_for_timeout(250)
    assert editor.saved == []
    expect(page.locator('#f-long-json')).to_have_value(raw)
    assert editor.errors == []


def test_global_edit_updates_side_projection_without_authored_overrides(editor):
    """Structured global changes update both JSON projections and keep sides sparse."""
    page = editor.page
    expect(page.locator('#f-user')).to_have_value('fixture')
    field = page.locator('[data-config-path="bot.global.risk.max_positions"]')
    field.evaluate('(el) => el.closest(".expander").classList.add("open")')
    field.fill('6')
    page.locator('#f-note').fill('inherit six')
    page.wait_for_function('JSON.parse(document.getElementById("f-long-json").value).risk.max_positions === 6')
    page.locator('#btn-save').click()
    expect(page.locator('#f-version')).to_have_value('1')
    config = editor.saved[-1]['config']
    assert config['bot']['global']['risk']['max_positions'] == 6
    assert config['bot']['long'] == editor.metadata['template']['bot']['long']
    assert config['bot']['short'] == editor.metadata['template']['bot']['short']
    assert editor.errors == []


def test_side_json_view_is_navigation_and_invalid_view_change_is_rejected(editor):
    """Changing views keeps a clean document clean and cannot hide invalid edits."""
    page = editor.page
    expect(page.locator('#f-user')).to_have_value('fixture')
    assert page.evaluate('dirty') is False
    page.locator('#f-long-json-view').select_option('overrides')
    assert page.evaluate('dirty') is False
    page.locator('#f-long-json').fill('{')
    page.locator('#f-long-json-view').select_option('inherited')
    expect(page.locator('#f-long-json-view')).to_have_value('overrides')
    expect(page.locator('#f-long-json')).to_have_value('{')
    assert editor.errors == []


def test_side_json_and_fixed_controls_remain_consistent_across_strategy_change(editor):
    """The shared strategy selector updates both JSON views without stale side writes."""
    page = editor.page
    expect(page.locator('#f-user')).to_have_value('fixture')
    page.locator('#f-strategy-kind').select_option('ema_anchor')
    page.wait_for_function('!!JSON.parse(document.getElementById("f-long-json").value).strategy.ema_anchor')
    projection = json.loads(page.locator('#f-long-json').input_value())
    assert 'trailing_martingale' not in projection['strategy']
    page.locator('#f-long-npos').fill('2')
    page.locator('#f-note').fill('two positions')
    page.wait_for_function('JSON.parse(document.getElementById("f-long-json").value).risk.max_positions === 2')
    page.locator('#btn-save').click()
    expect(page.locator('#f-version')).to_have_value('1')
    assert editor.saved[-1]['config']['bot']['long']['risk']['max_positions'] == 2
    assert editor.saved[-1]['config']['bot']['portfolio']['strategy_kind'] == 'ema_anchor'
    assert editor.errors == []


def test_pb9_editor_guide_sidebar_and_responsive_layout(editor):
    """Real navigation, Guide and shared sidebar work with the expanded native form."""
    page = editor.page
    expect(page.locator('#f-user')).to_have_value('fixture')
    destination = page.url
    page.locator('#pbgui-guide-btn').click()
    expect(page.locator('#pbgui-shared-help-box')).to_be_visible()
    expect(page.locator('#pbgui-shared-help-box')).to_contain_text('PB9 Run')
    assert page.url == destination
    page.locator('#pbgui-shared-help-close').click()
    for key, width in [('Home', 160), ('End', 420)]:
        page.locator('#sidebar-resize').focus()
        page.keyboard.press(key)
        assert round(page.locator('#sidebar').bounding_box()['width']) == width
        page.reload()
        expect(page.locator('#f-user')).to_have_value('fixture')
        assert round(page.locator('#sidebar').bounding_box()['width']) == width
    for viewport in [700, 800, 1440]:
        page.set_viewport_size({'width': viewport, 'height': 1000})
        assert page.locator('#sidebar-toolbar').evaluate('(el) => el.scrollWidth <= el.clientWidth')
        assert page.evaluate('document.documentElement.scrollWidth <= document.documentElement.clientWidth')
        assert page.locator('#main-content').evaluate('(el) => el.scrollWidth <= el.clientWidth')
    assert editor.errors == []
