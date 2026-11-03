"""Real drawer and setup code for OpenRouter model search and the decision boundary."""
from pathlib import Path
import json
import pytest
from urllib.parse import urlparse

from browser_driver import expect, sync_browser

ROOT = Path(__file__).resolve().parents[2]
JEV = 'typesafe/jev-1.13'
TOOLS_MODEL = 'acme/tools-model:free'
CHAT_MODEL = 'acme/plain-model-03'
SOURCES = '{"state":"x","questions":{"q":{"type":"noul","instructions":"Is it safe?"}},"sources":[{"name":"recent","tool":"list_backtests","args":{"version":"v8"}}]}'
CONVERSATION = 'a' * 32


def catalog():
    """Build a long synthetic catalog: Jev, one validated model and plain chat models."""
    plain = [{'id': f'acme/plain-model-{index:02}', 'name': f'Acme Plain {index:02}', 'provider': 'openrouter',
              'tools': False, 'tool_status': 'unvalidated', 'image_input': False, 'reasoning_variants': [], 'cost': {'input': 0.15 if index == 3 else round((16-index)*.1, 2), 'output': 0.6}, 'context': (index+1)*4096, 'created': 1700000000 + index*86400, 'description': 'Synthetic chat model <img src=x onerror=alert(1)>. Useful for analysis.'}
             for index in range(16)]
    return [{'id': JEV, 'name': 'Jev 1.13', 'decision': True, 'default': True, 'tools': False},
            {'id': TOOLS_MODEL, 'name': 'Acme Tools', 'provider': 'openrouter', 'tools': True, 'free': True,
             'tool_status': 'validated', 'image_input': True, 'default_effort': 'medium', 'cost': {'input': 0, 'output': 0},
             'reasoning_variants': [{'id': 'low', 'label': 'low', 'type': 'effort', 'value': 'low'}]}, *plain]


def test_drawer_search_and_decision_controls_follow_the_selected_model():
    """Search keeps the selection across reload; only decision models open a Jev preview."""
    preferences = {'selection': {'provider': 'openrouter', 'model': CHAT_MODEL, 'profile': 'default', 'effort': '', 'service_tier': ''}}
    posts, errors, saved = [], [], []
    active = {'model': CHAT_MODEL}
    catalog_rows = catalog()
    with sync_browser() as driver:
        browser = driver.chromium.launch()
        page = browser.new_page()
        page.set_default_timeout(7000)
        page.on('pageerror', lambda error: errors.append(str(error)))

        def respond(route):
            """Serve real assets and synthetic provider state."""
            path = urlparse(route.request.url).path
            if path == '/app/fixture.html':
                route.fulfill(content_type='text/html', body='<meta charset="utf-8"><nav id="topnav"></nav><script>'
                    'window.PBGUI_BASE_PREFIX="";window.PBGUI_NAV_CONFIG={current:"welcome",authenticated:true};'
                    '</script><script src="/app/pbgui_nav.js"></script>')
            elif path.startswith('/app/'):
                asset = ROOT / 'frontend' / path.removeprefix('/app/')
                route.fulfill(body=asset.read_bytes() if asset.is_file() else b'',
                              content_type='text/css' if asset.suffix == '.css' else 'text/javascript')
            elif path == '/api/ai/preferences':
                if route.request.method == 'PUT':
                    preferences.update(route.request.post_data_json)
                route.fulfill(json=preferences)
            elif path == '/api/ai/status':
                route.fulfill(json={'providers': {'openrouter': {'connected': True}}})
            elif path == '/api/ai/models':
                route.fulfill(json={'models': catalog_rows})
            elif path == '/api/ai/conversations' and route.request.method == 'GET':
                route.fulfill(json={'conversations': saved})
            elif path == '/api/ai/conversations':
                saved.append({'conversation_id': CONVERSATION, 'title': 'Fixture chat', 'model': CHAT_MODEL})
                route.fulfill(json={'conversation_id': CONVERSATION, 'provider': 'openrouter', 'model': CHAT_MODEL, 'messages': []})
            elif path.endswith('/jev-preview'):
                posts.append(('preview', route.request.post_data_json))
                page.evaluate('window.fixturePreviews=(window.fixturePreviews || 0)+1')
                route.fulfill(json={'preview_id': 'c' * 32, 'payload': {'model': JEV}})
            elif path.endswith('/turns'):
                posts.append(('turn', route.request.post_data_json))
                active['model'] = posts[-1][1]['model']
                page.evaluate('window.fixtureTurns=(window.fixtureTurns || 0)+1')
                route.fulfill(json={'turn_id': 'd' * 32})
            elif path.startswith('/api/ai/conversations/'):
                route.fulfill(json={'conversation_id': CONVERSATION, 'provider': 'openrouter', 'model': active['model'], 'busy': False, 'messages': []})
            elif path.endswith('/stream'):
                route.fulfill(status=204, body='')
            else:
                route.fulfill(json={})

        page.route('**/*', respond)
        page.goto('http://openrouter.test/app/fixture.html')
        page.locator('#pbgui-ai-btn').click()
        expect(page.locator('#pai-model')).to_have_value(CHAT_MODEL)
        expect(page.locator('#pai-model-cost')).to_have_text('Per 1M tokens: Input USD 0.15 · Output USD 0.6')
        page.locator('.pbg-model-toggle', has_text='Browse models').click()
        search = page.locator('#pai-model-search')
        expect(search).to_be_visible()
        expect(page.locator('#pai-model')).to_be_hidden()
        assert page.locator('.pai-compose textarea').evaluate('node=>node.placeholder').startswith('Type a message')
        assert 'Free' in page.locator(f'#pai-model option[value="{TOOLS_MODEL}"]').evaluate('node=>node.textContent')

        search.fill('tools')
        visible = page.locator('.pbg-model-results').evaluate('node=>Array.from(node.querySelectorAll(".pbg-model-row")).map(o=>o.dataset.modelValue)')
        assert visible == [TOOLS_MODEL]
        expect(page.locator('.pbg-model-current')).to_have_text('Selected: Acme Plain 03')
        expect(page.locator('#pai-model')).to_have_value(CHAT_MODEL)

        page.reload()
        expect(page.locator('#pai-model')).to_have_value(CHAT_MODEL)
        page.locator('.pbg-model-toggle', has_text='Browse models').click()
        expect(page.locator('#pai-model-search')).to_have_value('tools')
        page.set_viewport_size({'width': 380, 'height': 500})
        page.wait_for_function('document.querySelector(".pai-compose .primary").getBoundingClientRect().bottom<=window.innerHeight')
        assert page.locator('.pbg-model-browser').evaluate('node=>node.scrollWidth<=node.clientWidth')
        page.set_viewport_size({'width': 1280, 'height': 720})
        page.locator('.pbg-model-toggle', has_text='Close model browser').click()

        page.wait_for_function('document.getElementById("pbgui-ai-drawer").getBoundingClientRect().right<=window.innerWidth')
        page.locator('.pai-compose textarea').fill(SOURCES)
        page.locator('.pai-compose .primary').click()
        page.wait_for_function('window.fixtureTurns===1')
        assert [kind for kind, _ in posts] == ['turn']
        assert posts[0][1]['model'] == CHAT_MODEL and posts[0][1]['provider'] == 'openrouter'
        assert posts[0][1]['jev_preview_id'] is None

        expect(page.locator('.pai-compose .primary')).to_be_enabled()
        catalog_rows[:] = [row for row in catalog_rows if row['id'] != CHAT_MODEL]
        page.evaluate("window.dispatchEvent(new CustomEvent('pbgui:ai-models-changed'))")
        expect(page.locator(f'#pai-model option[value="{CHAT_MODEL}"]')).to_have_text('Unavailable model: ' + CHAT_MODEL)
        expect(page.locator('#pai-model')).to_have_value(CHAT_MODEL)
        assert [kind for kind, _ in posts] == ['turn']
        page.locator('#pai-model').select_option(JEV)
        expect(page.locator('#pai-model')).to_have_value(JEV)
        expect(page.locator('#pai-model-cost')).to_have_text('Model pricing unavailable')
        assert page.locator('.pai-compose textarea').evaluate('node=>node.placeholder').startswith('Ask Jev')
        page.locator('.pai-compose textarea').fill(SOURCES)
        page.locator('.pai-compose .primary').click()
        page.wait_for_function('window.fixturePreviews===1')
        kind, body = posts[-1]
        assert kind == 'preview' and body == {'message': SOURCES, 'provider': 'openrouter', 'model': JEV}
        assert not errors
        browser.close()


def test_model_table_columns_sort_numerically_and_preserve_state():
    """Headers toggle accessible sorts, keep unknowns last and share persisted list/table order."""
    rows = [
        {'id': 'outlier:free', 'name': 'Selected outlier', 'cost': {'input': 99, 'output': 99}, 'context': 9999, 'created': 1800000000, 'data_collection_allowed': True},
        {'id': 'alpha', 'name': 'Alpha', 'cost': {'input': 10, 'output': 2}, 'context': 100, 'created': 1700000000, 'data_collection_allowed': True},
        {'id': 'beta', 'name': 'Beta', 'cost': {'input': 2, 'output': 10}, 'context': 20, 'created': 1690000000, 'data_collection_allowed': False},
        {'id': 'gamma', 'name': 'Gamma', 'cost': {'input': 0, 'output': 0}, 'context': 300, 'created': 1710000000, 'data_collection_allowed': False},
        {'id': 'unknown', 'name': 'Zulu', 'data_collection_allowed': False},
    ]
    fixture = ('<meta charset="utf-8"><input id="search"><select id="models"></select>'
               '<script src="/app/js/ai_model_search.js"></script><script>const rows=' + json.dumps(rows) + ';'
               'const select=document.querySelector("#models");const metadata={};rows.forEach(row=>{'
               'row.provider="openrouter";metadata[row.id]=row;select.add(new Option(row.name,row.id));});'
               'PBGuiAIModelSearch.attach(select,document.querySelector("#search"),"sorting-test").refresh(metadata);</script>')
    errors = []
    with sync_browser() as driver:
        browser = driver.chromium.launch()
        page = browser.new_page()
        page.on('pageerror', lambda error: errors.append(str(error)))

        def respond(route):
            """Use the real shared renderer and stylesheet without external requests."""
            path = urlparse(route.request.url).path
            if path == '/fixture.html':
                route.fulfill(content_type='text/html', body=fixture)
            else:
                asset = ROOT / 'frontend' / path.removeprefix('/app/')
                route.fulfill(body=asset.read_bytes() if asset.is_file() else b'', content_type='text/css' if asset.suffix == '.css' else 'text/javascript')
        page.route('**/*', respond)
        page.goto('http://models.test/fixture.html')
        page.locator('.pbg-model-control', has_text='Table').click()
        page.locator('select[aria-label="Model variant"]').select_option('standard')
        cases = [
            ('name', 'name-desc', ['alpha', 'beta', 'gamma', 'unknown'], ['unknown', 'gamma', 'beta', 'alpha']),
            ('input-asc', 'input-desc', ['gamma', 'beta', 'alpha', 'unknown'], ['alpha', 'beta', 'gamma', 'unknown']),
            ('output-asc', 'output-desc', ['gamma', 'alpha', 'beta', 'unknown'], ['beta', 'alpha', 'gamma', 'unknown']),
            ('context-asc', 'context', ['beta', 'alpha', 'gamma', 'unknown'], ['gamma', 'alpha', 'beta', 'unknown']),
            ('oldest', 'newest', ['beta', 'alpha', 'gamma', 'unknown'], ['gamma', 'alpha', 'beta', 'unknown']),
        ]
        for ascending, descending, low, high in cases:
            header = page.locator('[data-sort-column="' + ascending + '"]')
            header.click()
            expect(page.locator('select[aria-label="Sort models"]')).to_have_value(ascending)
            assert page.locator('tbody tr').evaluate_all('rows=>rows.map(row=>row.dataset.modelValue)') == low
            expect(page.locator('th[aria-sort="ascending"]')).to_have_count(1)
            assert header.evaluate('node=>node===document.activeElement')
            header.press('Enter')
            expect(page.locator('select[aria-label="Sort models"]')).to_have_value(descending)
            assert page.locator('tbody tr').evaluate_all('rows=>rows.map(row=>row.dataset.modelValue)') == high
            expect(page.locator('th[aria-sort="descending"]')).to_have_count(1)
            expect(page.locator('#models')).to_have_value('outlier:free')
        assert page.locator('.pbg-model-results').evaluate('node=>{node.style.maxHeight="100px";node.scrollTop=40;return node.scrollTop;}') > 0
        page.locator('[data-sort-column="input-asc"]').evaluate('node=>node.click()')
        assert page.locator('.pbg-model-results').evaluate('node=>node.scrollTop') == 0
        page.reload()
        expect(page.locator('select[aria-label="Sort models"]')).to_have_value('input-asc')
        expect(page.locator('#models')).to_have_value('outlier:free')
        expect(page.locator('.pbg-model-current')).to_have_text('Selected: Selected outlier')
        assert page.locator('tbody tr').evaluate_all('rows=>rows.map(row=>row.dataset.modelValue)') == ['gamma', 'beta', 'alpha', 'unknown']
        page.locator('select[aria-label="Sort models"]').select_option('output-desc')
        expect(page.locator('th[aria-sort="descending"] button')).to_have_attribute('data-sort-column', 'output-asc')
        page.locator('.pbg-model-control', has_text='List').click()
        assert page.locator('.pbg-model-row').evaluate_all('rows=>rows.map(row=>row.dataset.modelValue)') == ['beta', 'alpha', 'gamma', 'unknown']
        assert not errors
        browser.close()


@pytest.mark.parametrize('entry', ['connection', 'models'])
@pytest.mark.parametrize('retry_fails', [False, True])
@pytest.mark.parametrize('failure_stage', ['request','tool_probe'])
def test_setup_privacy_permission_requires_consent_and_survives_reload(entry,retry_fails,failure_stage):
    """Real Setup controls confirm consent once, persist its display and revoke without a paid call."""
    allowed, writes, errors, probes = {}, [], [], []
    revision = {'value': 0}
    with sync_browser() as driver:
        browser = driver.chromium.launch()
        page = browser.new_page()
        page.set_default_timeout(7000)
        page.on('pageerror', lambda error: errors.append(str(error)))

        def respond(route):
            """Serve isolated owner policy state and local production assets."""
            path = urlparse(route.request.url).path
            if path == '/app/start.html':
                route.fulfill(content_type='text/html', body='<meta charset="utf-8"><nav id="topnav"></nav><script>window.PBGUI_NAV_CONFIG={current:"welcome",authenticated:true};</script><script src="/app/pbgui_nav.js"></script>')
            elif path in ('/app/ai_chat.html', '/api/ai/main_page'):
                body = (ROOT / 'frontend/ai_chat.html').read_text()
                for key, value in {'API_BASE': '/api/ai', 'BASE_PREFIX': '', 'NAV_HASH': 'test', 'WS_BASE': '/ws'}.items():
                    body = body.replace('%%' + key + '%%', value)
                route.fulfill(content_type='text/html', body=body)
            elif path.startswith('/app/'):
                asset = ROOT / 'frontend' / path.removeprefix('/app/')
                route.fulfill(body=asset.read_bytes() if asset.is_file() else b'',
                              content_type='text/css' if asset.suffix == '.css' else 'text/javascript')
            elif path == '/api/ai/status':
                route.fulfill(json={'providers': {'openrouter': {'connected': True, 'privacy_revision': str(revision['value'])}}})
            elif path == '/api/ai/models':
                route.fulfill(json={'models': [{**row, 'data_collection_allowed': allowed.get(row['id'], False)} for row in catalog()]})
            elif path == '/api/ai/preferences':
                route.fulfill(json={'selection': {'provider': 'openrouter', 'model': CHAT_MODEL, 'profile': 'default'}, 'jev_max_cost_usd': .01})
            elif path == '/api/ai/providers/openrouter/privacy':
                body = route.request.post_data_json
                writes.append(body)
                allowed[body['model']] = body['allow_data_collection']
                revision['value'] += 1
                route.fulfill(json={'model': body['model'], 'data_collection_allowed': body['allow_data_collection']})
            elif path == '/api/ai/test-model':
                probes.append(route.request.post_data_json)
                if not allowed.get(CHAT_MODEL) or retry_fails:
                    message='OpenRouter reports that the privacy/data-policy filters exclude every endpoint for this model.'
                    if failure_stage=='request':
                        route.fulfill(status=400,json={'detail':message})
                    else:
                        route.fulfill(json={'provider':'openrouter','model':CHAT_MODEL,'reply':'OK','elapsed_seconds':0.1,'tool_probe':{'status':'failed','message':message}})
                else:
                    route.fulfill(json={'provider':'openrouter','model':CHAT_MODEL,'reply':'OK','elapsed_seconds':0.1})
            else:
                route.fulfill(json={})
        page.route('**/*', respond)
        page.goto('http://openrouter.test/app/start.html')
        page.locator('.nav-group-btn', has_text='Information').click()
        page.locator('.nav-item[data-page="info_ai_chat"]').click()
        page.locator('[data-section="openrouter"]').click()
        status = page.locator('#openrouter-privacy-status')
        toggle = page.locator('#openrouter-privacy-toggle')
        expect(page.locator('#openrouter-privacy')).to_be_hidden()
        if entry=='models':page.locator('[data-section="models"]').click()
        test_button=page.locator('#openrouter-test' if entry=='connection' else '#test-model')
        test_button.click()
        expect(page.locator('#pbgui-dialog-message')).to_contain_text('images and tool results')
        expect(page.locator('#pbgui-dialog-message')).to_contain_text('training')
        expect(page.locator('#pbgui-dialog-message')).to_contain_text('Acme Plain 03')
        page.locator('#pbgui-dialog-cancel').click()
        assert not writes
        if failure_stage=='tool_probe':
            if entry=='connection':
                expect(page.locator('#openrouter-test-result')).to_contain_text('Connection verified')
                expect(page.locator('#openrouter-test-result')).to_contain_text('PBGui tools disabled:')
                assert not page.locator('#openrouter-test-result').evaluate('node=>node.classList.contains("error")')
            else:
                expect(page.locator('#test-output')).to_contain_text('OK')
                expect(page.locator('#test-output')).to_contain_text('PBGui tools disabled:')
                assert not page.locator('#setup-status').evaluate('node=>node.classList.contains("error")')
        test_button.click()
        page.locator('#pbgui-dialog-accept').click()
        expect(page.locator('#openrouter-privacy-status' if entry=='connection' else '#model-privacy-status')).to_have_text('Provider data collection: allowed')
        assert writes == [{'model': CHAT_MODEL, 'allow_data_collection': True}]
        page._wait(lambda: len(probes)==3,7000)
        page.reload()
        page.locator('[data-section="openrouter"]').click()
        expect(status).to_have_text('Provider data collection: allowed')
        page.locator('[data-section="models"]').click()
        expect(page.locator('#model-privacy-status')).to_have_text('Provider data collection: allowed')
        page.locator('#model-privacy-toggle').click()
        expect(page.locator('#model-privacy')).to_be_hidden()
        assert writes[-1] == {'model': CHAT_MODEL, 'allow_data_collection': False}
        # Another browser changes consent; returning to the tab updates the existing selection.
        allowed[CHAT_MODEL] = True
        revision['value'] += 1
        page.evaluate('document.dispatchEvent(new Event("visibilitychange"))')
        expect(page.locator('#model-privacy-status')).to_have_text('Provider data collection: allowed')
        expect(page.locator('#model-select')).to_have_value(CHAT_MODEL)
        page.locator('#model-select').select_option(JEV)
        expect(page.locator('#model-privacy')).to_be_hidden()
        assert not errors and len(probes)==3
        browser.close()


def test_setup_prices_follow_selection_and_reload():
    """Use the real Setup page to check paid, zero and missing catalog prices."""
    errors = []
    selection = {'provider': 'openrouter', 'model': CHAT_MODEL, 'profile': 'default'}
    with sync_browser() as driver:
        browser = driver.chromium.launch()
        page = browser.new_page()
        page.on('pageerror', lambda error: errors.append(str(error)))
        def respond(route):
            """Serve local assets and synthetic metadata; no external requests are sent."""
            path = urlparse(route.request.url).path
            if path == '/app/start.html':
                route.fulfill(content_type='text/html', body='<meta charset="utf-8"><nav id="topnav"></nav><script>window.PBGUI_NAV_CONFIG={current:"welcome",authenticated:true};</script><script src="/app/pbgui_nav.js"></script>')
            elif path in ('/app/ai_chat.html', '/api/ai/main_page'):
                body = (ROOT / 'frontend/ai_chat.html').read_text()
                for key, value in {'API_BASE': '/api/ai', 'BASE_PREFIX': '', 'NAV_HASH': 'test', 'WS_BASE': '/ws'}.items():
                    body = body.replace('%%' + key + '%%', value)
                route.fulfill(content_type='text/html', body=body)
            elif path.startswith('/app/'):
                asset = ROOT / 'frontend' / path.removeprefix('/app/')
                route.fulfill(body=asset.read_bytes() if asset.is_file() else b'',
                              content_type='text/css' if asset.suffix == '.css' else 'text/javascript')
            elif path == '/api/ai/status':
                route.fulfill(json={'providers': {'openrouter': {'connected': True}}})
            elif path == '/api/ai/models':
                route.fulfill(json={'models': catalog()})
            elif path == '/api/ai/preferences':
                route.fulfill(json={'selection': selection, 'jev_max_cost_usd': .01})
            else:
                route.fulfill(json={})
        page.route('**/*', respond)
        page.goto('http://openrouter.test/app/start.html')
        page.locator('.nav-group-btn', has_text='Information').click()
        page.locator('.nav-item[data-page="info_ai_chat"]').click()
        expect(page.locator('h1')).to_have_text('AI Setup')
        page.locator('[data-section="openrouter"]').click()
        price = page.locator('#openrouter-model-cost')
        expect(price).to_have_text('Per 1M tokens: Input USD 0.15 · Output USD 0.6')
        catalog_view = page.locator('.pbg-model-browser[data-model-select="openrouter-test-model"]')
        assert 'PBGui tools' not in catalog_view.text_content()
        expect(catalog_view.locator('select[aria-label="Sort models"]')).to_have_value('newest')
        assert json.loads(catalog_view.locator('.pbg-model-row').first.get_attribute('data-model-value'))[1] == 'acme/plain-model-15'
        expect(catalog_view.locator('.pbg-model-description').first).to_contain_text('Synthetic chat model <img')
        expect(catalog_view.locator('img')).to_have_count(0)
        catalog_view.locator('.pbg-model-select > button', has_text='All variants').click()
        catalog_view.locator('[role="option"]', has_text='Free').click()
        catalog_view.locator('.pbg-model-row', has_text='Acme Tools').click()
        expect(price).to_have_text('Per 1M tokens: Input USD 0 · Output USD 0')
        catalog_view.locator('button[aria-label="Pin Acme Tools"]').click()
        catalog_view.locator('.pbg-model-control', has_text='Pinned').click()
        expect(catalog_view.locator('.pbg-model-row')).to_have_count(1)
        catalog_view.locator('.pbg-model-select > button', has_text='Newest').click()
        catalog_view.locator('[role="option"]', has_text='Pricing: Low to High').click()
        catalog_view.locator('.pbg-model-control', has_text='Table').click()
        expect(catalog_view.locator('.pbg-model-table')).to_be_visible()
        expect(catalog_view.locator('tbody tr td').nth(1)).to_have_text('$0')
        page.reload()
        expect(price).to_have_text('Per 1M tokens: Input USD 0 · Output USD 0')
        expect(catalog_view.locator('select[aria-label="Sort models"]')).to_have_value('price-asc')
        expect(catalog_view.locator('select[aria-label="Model variant"]')).to_have_value('free')
        expect(catalog_view.locator('.pbg-model-table')).to_be_visible()
        expect(catalog_view.locator('.pbg-model-control', has_text='Pinned')).to_have_attribute('aria-pressed', 'true')
        expect(catalog_view.locator('button[aria-label="Unpin Acme Tools"]')).to_have_attribute('aria-pressed', 'true')
        catalog_view.locator('.pbg-model-control', has_text='Pinned').click()
        catalog_view.locator('select[aria-label="Model variant"]').select_option('')
        catalog_view.locator('select[aria-label="Sort models"]').select_option('context')
        first = catalog_view.locator('.pbg-model-row').first
        assert json.loads(first.get_attribute('data-model-value'))[1] == 'acme/plain-model-15'
        catalog_view.locator('.pbg-model-control', has_text='List').click()
        catalog_view.locator('.pbg-model-tab', has_text='Decisions').click()
        expect(catalog_view.locator('.pbg-model-current')).to_contain_text('Acme Tools')
        catalog_view.locator('.pbg-model-row', has_text='Jev 1.13').focus()
        catalog_view.locator('.pbg-model-row', has_text='Jev 1.13').press('Enter')
        expect(price).to_have_text('Model pricing unavailable')
        page.locator('[data-section="models"]').click()
        expect(page.locator('#model-info')).to_contain_text('Per 1M tokens: Input USD 0.15 · Output USD 0.6')
        main_catalog = page.locator('.pbg-model-browser[data-model-select="model-select"]')
        expect(main_catalog.locator('button[aria-label="Unpin Acme Tools"]')).to_have_attribute('aria-pressed', 'true')
        main_catalog.locator('.pbg-model-control', has_text='Table').click()
        for width in (1200, 700, 380):
            page.set_viewport_size({'width': width, 'height': 700})
            page.wait_for_function('document.documentElement.scrollWidth<=window.innerWidth')
            assert page.locator('.pbg-model-browser[data-model-select="model-select"]').evaluate('node=>node.scrollWidth<=node.clientWidth')
        assert not errors
        browser.close()


@pytest.mark.parametrize('mode', ['lazy', 'empty', 'existing', 'draft', 'draft-send', 'failed'])
def test_setup_test_selects_only_an_empty_or_new_chat(mode):
    """A real Setup test selects its model without modifying old chats or defaults."""
    existing_id, new_id = 'c' * 32, 'd' * 32
    old_messages = [{'role': 'user', 'content': 'Keep this existing conversation'}]
    saved = {existing_id: {'conversation_id': existing_id, 'provider': 'openrouter', 'model': JEV,
                          'messages': old_messages, 'busy': False}} if mode == 'existing' else {}
    creates, preference_writes, errors, turns = [], [], [], []
    with sync_browser() as driver:
        browser = driver.chromium.launch()
        page = browser.new_page()
        page.on('pageerror', lambda error: errors.append(str(error)))
        def respond(route):
            """Use actual page/scripts and only synthetic test/history responses."""
            path = urlparse(route.request.url).path
            if path == '/api/ai/main_page':
                body = (ROOT / 'frontend/ai_chat.html').read_text()
                for key, value in {'API_BASE': '/api/ai', 'BASE_PREFIX': '', 'NAV_HASH': 'test', 'WS_BASE': '/ws'}.items():
                    body = body.replace('%%' + key + '%%', value)
                route.fulfill(content_type='text/html', body=body)
            elif path.startswith('/app/'):
                asset = ROOT / 'frontend' / path.removeprefix('/app/')
                route.fulfill(body=asset.read_bytes() if asset.is_file() else b'',
                              content_type='text/css' if asset.suffix == '.css' else 'text/javascript')
            elif path == '/api/ai/status':
                route.fulfill(json={'providers': {'openrouter': {'connected': True}}})
            elif path == '/api/ai/preferences':
                if route.request.method == 'PUT':
                    preference_writes.append(route.request.post_data_json)
                route.fulfill(json={'selection': {'provider': 'openrouter', 'model': JEV, 'profile': 'default'}})
            elif path == '/api/ai/models':
                route.fulfill(json={'models': catalog()})
            elif path == '/api/ai/test-model':
                assert route.request.post_data_json['model'] == TOOLS_MODEL
                if mode == 'failed':
                    route.fulfill(status=400, json={'detail': 'Synthetic test failed'})
                else:
                    route.fulfill(json={'provider': 'openrouter', 'model': TOOLS_MODEL, 'reply': 'OK',
                                        'elapsed_seconds': .1, 'tool_probe': {'status': 'validated', 'message': 'OK'}})
            elif path == '/api/ai/conversations':
                if route.request.method == 'POST':
                    chosen = route.request.post_data_json
                    creates.append(chosen)
                    created_id = ('d' if len(creates) == 1 else 'e') * 32
                    saved[created_id] = {'conversation_id': created_id, 'messages': [], 'busy': False, **chosen}
                    route.fulfill(json=saved[created_id])
                else:
                    route.fulfill(json={'conversations': list(saved.values())})
            elif path.endswith('/turns'):
                turns.append(route.request.post_data_json)
                cid = path.split('/')[-2]
                saved[cid]['messages'] = [{'role': 'user', 'content': turns[-1]['message']}]
                route.fulfill(json={'turn_id': 'synthetic-turn'})
            elif path.startswith('/api/ai/conversations/'):
                route.fulfill(json=saved.get(path.rsplit('/', 1)[-1], {}))
            elif path.endswith('/stream'):
                route.fulfill(status=204, body='')
            else:
                route.fulfill(json={})
        page.route('**/*', respond)
        page.goto('http://openrouter.test/api/ai/main_page?section=openrouter')
        if mode != 'lazy':
            page.locator('#pbgui-ai-btn').click()
            expect(page.locator('#pai-model')).to_have_value(JEV)
            page.locator('.pai-pin').click()
            page.wait_for_function('document.documentElement.classList.contains("pbgui-ai-drawer-pinned")')
            if mode == 'existing':
                expect(page.locator('.pai-messages')).to_contain_text(old_messages[0]['content'])
            elif mode in ('draft', 'draft-send'):
                page.locator('.pai-compose textarea').fill('Keep this draft')
        picker = page.locator('.pbg-model-browser[data-model-select="openrouter-test-model"]')
        picker.locator('.pbg-model-row', has_text='Acme Tools').click()
        page.locator('#openrouter-test').click()
        expect(page.locator('#openrouter-test-result')).to_contain_text('Synthetic test failed' if mode == 'failed' else 'Connection verified')
        if mode == 'lazy':
            page.locator('#pbgui-ai-btn').click()
        if mode in ('lazy', 'empty'):
            expect(page.locator('#pai-model')).to_have_value(TOOLS_MODEL)
        else:
            expect(page.locator('#pai-model')).to_have_value(JEV)
            if mode == 'existing':
                expect(page.locator('.pai-messages')).to_contain_text(old_messages[0]['content'])
            elif mode in ('draft', 'draft-send'):
                expect(page.locator('.pai-compose textarea')).to_have_value('Keep this draft')
        assert not creates
        provider_box = page.locator('#pai-provider').bounding_box()
        new_box = page.locator('.pai-new').bounding_box()
        assert abs(provider_box['y'] - new_box['y']) < 2
        assert new_box['x'] > provider_box['x']
        if mode == 'draft-send':
            page.locator('.pai-compose .primary').click()
            page.wait_for_function('document.querySelector(".pai-compose .primary").disabled===false')
            page._wait(lambda: len(turns) == 1, 7000)
            assert len(creates) == 1 and creates[0]['model'] == JEV
            assert len(turns) == 1 and turns[0]['model'] == JEV
        if mode in ('existing', 'draft', 'draft-send'):
            page.locator('.pai-new').click()
            expect(page.locator('#pai-model')).to_have_value(TOOLS_MODEL)
            page.wait_for_function('document.querySelector(".pai-new").disabled===false')
            assert len(creates) == (2 if mode == 'draft-send' else 1) and creates[-1]['model'] == TOOLS_MODEL
            if mode == 'existing':
                assert saved[existing_id]['messages'] == old_messages and saved[existing_id]['model'] == JEV
        assert not any('selection' in payload for payload in preference_writes)
        assert not errors
        browser.close()


@pytest.mark.parametrize('compact', [False, True])
@pytest.mark.parametrize('provider', ['deepseek', 'chatgpt'])
def test_hidden_search_never_filters_small_provider_catalog(compact, provider):
    """A saved OpenRouter query remains editable when visible and cannot hide small native catalogs."""
    fixture = '''<meta charset="utf-8"><div WRAPPER><input id="search"><select id="models"></select></div>
    <button id="small">Small provider</button><button id="large">OpenRouter</button>
    <script src="/app/js/ai_model_search.js"></script><script>
    const select=document.querySelector('#models'), search=document.querySelector('#search');
    const browser=PBGuiAIModelSearch.attach(select,search,'hidden-search-regression');
    function show(small){
      const rows=small ? [{id:'first',name:'First'},{id:'second',name:'Second'}] :
        Array.from({length:14},(_,i)=>({id:'model-'+i,name:i===0?'Claude':'Other '+i,provider:'openrouter'}));
      const metadata={};select.replaceChildren();rows.forEach(row=>{metadata[row.id]=row;select.add(new Option(row.name,row.id));});
      browser.refresh(metadata);const url=new URL(location.href);url.searchParams.set('small',String(small));history.replaceState(null,'',url);
    }
    document.querySelector('#small').onclick=()=>show(true);document.querySelector('#large').onclick=()=>show(false);
    show(new URL(location.href).searchParams.get('small')==='true');
    </script>'''.replace('WRAPPER', 'class="pai-toolbar"' if compact else '').replace("name:'First'", "name:'First',provider:'"+provider+"'").replace("name:'Second'", "name:'Second',provider:'"+provider+"'")
    with sync_browser() as driver:
        browser = driver.chromium.launch()
        page = browser.new_page()
        def respond(route):
            """Serve the shared production control and isolated provider fixtures."""
            path = urlparse(route.request.url).path
            if path == '/fixture.html':
                route.fulfill(content_type='text/html', body=fixture)
            else:
                asset = ROOT / 'frontend' / path.removeprefix('/app/')
                route.fulfill(body=asset.read_bytes() if asset.is_file() else b'', content_type='text/css' if asset.suffix=='.css' else 'text/javascript')
        page.route('**/*', respond)
        page.goto('http://search.test/fixture.html')
        if compact:page.locator('.pbg-model-toggle').click()
        page.locator('#search').fill('claude')
        expect(page.locator('.pbg-model-row')).to_have_count(1)
        page.locator('#small').click()
        expect(page.locator('#search')).to_be_hidden()
        assert page.locator('#models option').evaluate_all('rows=>rows.length===2 && rows.every(row=>!row.hidden)')
        page.locator('#models').select_option('second')
        page.reload()
        expect(page.locator('#search')).to_be_hidden()
        assert page.locator('#models option').evaluate_all('rows=>rows.length===2 && rows.every(row=>!row.hidden)')
        page.locator('#large').click()
        if compact and not page.locator('#search').is_visible():page.locator('.pbg-model-toggle').click()
        expect(page.locator('#search')).to_have_value('claude')
        expect(page.locator('.pbg-model-row')).to_have_count(1)
        browser.close()
