"""Real Optimize sidebar template window and chat handoff with isolated APIs."""
from pathlib import Path
from datetime import datetime, timezone
import re
import json
from urllib.parse import urlparse

import pytest
from playwright.sync_api import expect, sync_playwright

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize('width,prefix', [(1400, ''), (700, '/mounted')])
def test_loop_template_sidebar_window_and_send(width, prefix):
    """Use real menus, resize/drag/focus behavior and exactly one explicit chat send."""
    __tracebackhide__ = True
    errors, mutations, turns, conversations = [], [], [], []
    hide_newest_chat = {'enabled': False}
    delayed_markets = {'enabled': False, 'routes': []}
    delayed_dates = {'enabled':False, 'routes':[]}
    date_requests = []
    choices = {'configs': [{'name': 'Saved HYPE', 'start_date': '2025-01-01', 'end_date': '2025-12-31'}],
               'markets': {'binance': ['BTC', 'ETH'], 'hyperliquid': ['BTC', 'HYPE', 'xyz:TSLA']} }
    source = (ROOT / 'frontend/v7_optimize.html').read_text()
    values = {'API_BASE': prefix + '/api/optimize-v8', 'OPTIMIZE_VERSION': 'v8',
              'VERSION': 'test', 'SERIAL': '1', 'NAV_HASH': 'test', 'BASE_PREFIX': prefix,
              'LIMITS_META': '{}', 'BACKTEST_VERSION': 'v8', 'OPTIMIZE_NAV_TITLE': 'PB8 Optimize',
              'OPTIMIZE_NAV_CURRENT': 'v8_optimize'}
    for key, value in values.items():
        source = source.replace('%%' + key + '%%', value)
    source = source.replace('src="/app/', 'src="' + prefix + '/app/').replace('href="/app/', 'href="' + prefix + '/app/')
    with sync_playwright() as driver:
        browser = driver.chromium.launch()
        page = browser.new_page(viewport={'width': width, 'height': 900})
        page.set_default_timeout(7000)
        page.clock.set_fixed_time(datetime(2026,10,16,12,tzinfo=timezone.utc))
        page.on('pageerror', lambda error: errors.append(str(error)))
        def respond(route):
            """All requests are fixtures; AI turns are recorded without provider calls."""
            raw = urlparse(route.request.url).path
            path = raw.removeprefix(prefix) if prefix else raw
            if route.request.method not in ('GET', 'HEAD'):
                mutations.append(path)
            if path == '/app/start.html':
                route.fulfill(content_type='text/html; charset=utf-8', body=f'''<nav id="topnav"></nav>
                <script>window.PBGUI_BASE_PREFIX={prefix!r};window.PBGUI_NAV_CONFIG={{current:'welcome',authenticated:true}};</script>
                <script src="{prefix}/app/pbgui_nav.js?v=test"></script>''')
            elif path == '/api/optimize-v8/main_page':
                route.fulfill(content_type='text/html; charset=utf-8', body=source)
            elif path.startswith('/app/'):
                asset = ROOT / 'frontend' / path.removeprefix('/app/')
                route.fulfill(body=asset.read_bytes() if asset.is_file() else b'',
                              content_type='text/css; charset=utf-8' if asset.suffix == '.css' else 'text/javascript; charset=utf-8')
            elif path == '/api/ai/loop-template-options':
                assert raw.startswith(prefix + '/api/')
                route.fulfill(json=choices)
            elif path == '/api/optimize-v8/configs/new-config':
                route.fulfill(json={'config':{'live':{},'backtest':{},'optimize':{}}})
            elif path.startswith('/api/optimize-v8/configs/'):
                route.fulfill(json={'config':{'backtest':{'start_date':'2025-01-01','end_date':'2025-12-31','suite_enabled':True,
                    'scenarios':[{'label':'train_1','start_date':'2025-01-01','end_date':'2025-04-30'},
                                 {'label':'train_2','start_date':'2025-05-01','end_date':'2025-08-31'}]},
                    'pbgui':{'scenario_template':{'holdout_scenarios':[{'label':'holdout_1','start_date':'2025-09-01','end_date':'2025-12-31'}]}}}})
            elif path == '/api/optimize-v8/ohlcv-start-dates':
                assert raw == prefix + path
                date_requests.append(route.request.post_data_json)
                route.fulfill(json={'job_id':format(len(date_requests),'012x'),'status':'queued'})
            elif path.startswith('/api/optimize-v8/ohlcv-start-dates/'):
                if route.request.method=='DELETE':
                    route.fulfill(json={'status':'stopping'})
                elif delayed_dates['enabled']:
                    delayed_dates['routes'].append(route)
                else:
                    route.fulfill(json={'status':'completed','result':{'start_date_options':{'all_markets':{'available':True,'start_date':'2024-01-01'}}}})
            elif path == '/api/v8/coins/status':
                request = route.request.post_data_json
                if delayed_markets['enabled']:
                    delayed_markets['routes'].append(route)
                    return
                symbols = sorted({coin for exchange in request['exchanges'] for coin in choices['markets'].get(exchange, [])})
                route.fulfill(json={'symbols': symbols,
                                    'catalog': [{'config_id': coin, 'display': coin} for coin in symbols],
                                    'statuses': {coin: {'normalized': coin, 'status': 'valid' if coin in symbols else 'invalid'}
                                                 for coin in request['coins']}})
            elif path == '/api/ai/preferences':
                route.fulfill(json={'drawer_open': False})
            elif path == '/api/ai/status':
                route.fulfill(json={'providers': {'opencode-go': {'connected': True}}})
            elif path == '/api/ai/models':
                route.fulfill(json={'models': [{'id': 'test', 'name': 'Test', 'tools': True}]})
            elif path == '/api/ai/conversations':
                if route.request.method == 'POST':
                    identifier = format(len(conversations) + 1, '032x')
                    conversations.append({'conversation_id': identifier, 'title': 'New loop', 'model': 'test'})
                    route.fulfill(json={'conversation_id': identifier})
                else:
                    route.fulfill(json={'conversations': conversations[:-1] if hide_newest_chat['enabled'] else conversations})
            elif path.endswith('/turns'):
                assert raw.startswith(prefix + '/api/ai/')
                turns.append(route.request.post_data_json)
                route.fulfill(json={})
            elif path.startswith('/api/ai/conversations/'):
                identifier = path.split('/')[4]
                route.fulfill(json={'conversation_id': identifier, 'provider': 'opencode-go', 'model': 'test',
                                    'messages': [{'role': 'user', 'content': t['message']} for t in turns], 'busy': False})
            elif path == '/api/optimize-v8/loops/options':
                route.fulfill(json={'configs': [], 'providers': {}, 'presets': [], 'vast': {}})
            elif path == '/api/optimize-v8/loops':
                route.fulfill(json={'loops': []})
            elif path == '/api/optimize-v8/loops/configs':
                route.fulfill(json={'configs': []})
            elif path == '/api/optimize-v8/loops/instructions':
                route.fulfill(json={'active': 'default', 'revision': 0, 'versions': [{'id': 'default', 'name': 'PBGui default'}]})
            elif path.endswith('/instructions/default'):
                route.fulfill(json={'id': 'default', 'text': 'Defaults', 'digest': '0'})
            elif path == '/api/optimize-v8/metadata':
                route.fulfill(json={'runtime_ready': False, 'runtime_message': 'Fixture', 'optimize_defaults': {},
                                    'bot_defaults': {}, 'backtest_defaults': {}, 'live_defaults': {}, 'optimize_bounds_defaults': {}})
            elif path == '/api/optimize-v8/configs':
                route.fulfill(json={'configs': []})
            elif path == '/api/optimize-v8/queue':
                route.fulfill(json={'queue': []})
            elif path == '/api/optimize-v8/results':
                route.fulfill(json={'results': []})
            elif path == '/api/help/index':
                route.fulfill(json=[{'file': '43_pbv8_optimize', 'title': 'PB8 Optimize'}])
            elif path == '/api/help/content':
                route.fulfill(json={'content': '# PB8 Optimize\n\nTemplate help.'})
            else:
                route.fulfill(json={})
        page.route('**/*', respond)
        page.goto('http://templates.test' + prefix + '/app/start.html')
        page.locator('.nav-group-btn').filter(has_text='PBv8').click()
        page.locator('[data-page="v8_optimize"]').click()
        destination = page.url
        page.locator('#pbgui-guide-btn').click()
        expect(page.locator('#pbgui-shared-help-ovl')).to_be_visible()
        assert page.url == destination
        page.locator('#pbgui-shared-help-close').click()
        if width > 760:
            for sidebar_width in (160, 420, 240):
                handle = page.locator('#sidebar-resize').bounding_box()
                page.mouse.move(handle['x'] + handle['width'] / 2, handle['y'] + 100)
                page.mouse.down(); page.mouse.move(sidebar_width, handle['y'] + 100); page.mouse.up()
                assert page.locator('#btn-ai-loop-template').evaluate('(n)=>n.scrollWidth<=n.clientWidth')
        button = page.locator('#sidebar #btn-ai-loop-template')
        expect(button).to_be_visible()
        # Opening the real sidebar action does not open or load the chat drawer.
        button.click()
        dialog = page.get_by_role('dialog', name='AI Loop Template', exact=True)
        expect(dialog).to_be_visible()
        assert page.locator('#pbgui-ai-drawer').count() == 0
        assert not turns and not conversations
        bounds = dialog.bounding_box()
        assert abs(bounds['x'] + bounds['width'] / 2 - width / 2) < 2
        assert bounds['x'] >= 15 and bounds['y'] >= 15
        assert dialog.locator('.pbg-modal-close').is_visible()
        assert page.locator('.pai-loop-template').evaluate('(n)=>n.scrollWidth<=n.clientWidth')
        assert page.locator('#pai-template-rounds').evaluate('(n)=>n.classList.contains("panel-input") && n.parentElement.classList.contains("form-group")')
        assert dialog.get_by_role('button', name='Reset to defaults', exact=True).evaluate('(n)=>n.classList.contains("btn")')
        assert dialog.locator('.pbg-modal-body').evaluate('(n)=>getComputedStyle(n).backgroundColor === "rgba(0, 0, 0, 0)"')
        # Backdrop clicks do not close it; keyboard focus remains within the window.
        page.mouse.click(2, 2)
        expect(dialog).to_be_visible()
        dialog.locator('.pbg-modal-close').focus()
        page.keyboard.press('Shift+Tab')
        expect(dialog.get_by_role('button', name='Close', exact=True)).to_be_focused()
        page.keyboard.press('Tab')
        expect(dialog.locator('.pbg-modal-close')).to_be_focused()
        if width > 760:
            head = dialog.locator('.pbg-modal-head').bounding_box()
            page.mouse.move(head['x'] + 160, head['y'] + 20)
            page.mouse.down(); page.mouse.move(head['x'] + 185, head['y'] + 40); page.mouse.up()
            moved = dialog.bounding_box()
            assert moved['x'] > bounds['x'] + 15
            handle = dialog.locator('.pbg-window-resize-se').bounding_box()
            page.mouse.move(handle['x'] + 2, handle['y'] + 2)
            page.mouse.down(); page.mouse.move(handle['x'] + 42, handle['y'] + 12); page.mouse.up()
            assert dialog.bounding_box()['width'] > moved['width'] + 20
        # Defaults resolve dates through the existing native lookup, without user input.
        expect(page.locator('#pai-template-windows')).to_have_value('auto')
        initial_coins=page.locator('#pai-template-coins .ms-input')
        initial_coins.click(); initial_coins.fill('ETH')
        page.locator('#pai-template-coins .ms-option[data-val="ETH"]').click()
        expect(page.locator('#pai-template-start')).to_have_value('2024-01-01')
        expect(page.locator('#pai-template-end')).to_have_value('2026-10-15')
        expect(page.locator('.pai-template-error')).to_have_text('')
        expect(page.locator('#pai-template-window-preview input[type="date"]')).to_have_count(18)
        assert date_requests[-1]['config']['live']['approved_coins']=={'long':['ETH'],'short':[]}
        assert date_requests[-1]['config']['backtest']['exchanges']==['binance']
        expect(page.locator('#pai-template-timeframes')).to_have_value('8')
        page.locator('#pai-template-timeframes').fill('4')
        page.locator('#pai-template-start').fill('2025-01-01')
        expect(page.locator('#pai-template-windows')).to_have_value('custom')
        page.locator('#pai-template-coins .ms-x[data-val="ETH"]').click()
        page.locator('#pai-template-end').fill('2025-12-31')
        expect(page.locator('#pai-template-coins .ms-dropdown')).not_to_have_class(re.compile(r'\bopen\b'))
        # Exact shared editor controls, with multiple exchanges and USDC/HIP-3 markets.
        exchange_input = page.locator('#pai-template-exchanges .ms-input')
        exchange_input.fill('hyper')
        page.locator('#pai-template-exchanges .ms-option[data-val="hyperliquid"]').click()
        expect(page.locator('#pai-template-exchanges .ms-tag')).to_have_count(2)
        coin_input = page.locator('#pai-template-coins .ms-input')
        coin_input.fill('BTC')
        page.locator('#pai-template-coins .ms-option[data-val="BTC"]').click()
        coin_input.fill('HYPE')
        page.keyboard.press('Enter')
        coin_input.fill('xyz:')
        expect(page.locator('#pai-template-coins .ms-option[data-val="xyz:TSLA"]')).to_be_visible()
        dialog.locator('.pbg-modal-close').focus()
        expect(page.locator('.pai-template-count')).to_have_text('2 selected')
        # An older automatic reload must not replace a newer exchange selection.
        delayed_markets['enabled'] = True
        page.evaluate('window.dispatchEvent(new Event("focus"))')
        page.wait_for_timeout(100)
        assert len(delayed_markets['routes']) == 1
        delayed_markets['enabled'] = False
        page.locator('#pai-template-exchanges .ms-x[data-val="hyperliquid"]').click()
        expect(page.locator('.pai-template-error')).to_contain_text('unavailable')
        expect(dialog.get_by_role('button', name='Send to AI Chat', exact=True)).to_be_disabled()
        page.locator('#pai-template-coins .ms-x[data-val="HYPE"]').click()
        expect(dialog.get_by_role('button', name='Send to AI Chat', exact=True)).to_be_enabled()
        delayed_markets['routes'].pop().fulfill(json={'symbols': ['STALE'], 'statuses': {}})
        coin_input.fill('STALE')
        expect(page.locator('#pai-template-coins .ms-option')).to_have_count(0)
        # Restore the intended two-exchange selection using the real controls.
        exchange_input.fill('hyper')
        page.locator('#pai-template-exchanges .ms-option[data-val="hyperliquid"]').click()
        expect(page.locator('.pai-template-error')).to_have_text('')
        # Let the shared picker's pending 150 ms blur close finish before refocusing it.
        page.wait_for_timeout(175)
        coin_input.click()
        coin_input.fill('HYPE')
        page.keyboard.press('Enter')
        expect(page.locator('.pai-template-count')).to_have_text('2 selected')
        dialog.locator('.pbg-modal-close').focus()
        page.keyboard.press('Escape')
        expect(dialog).not_to_be_visible()
        expect(button).to_be_focused()
        button.click()
        expect(page.locator('.pai-template-count')).to_have_text('2 selected')
        saved_geometry = dialog.bounding_box()
        page.reload()
        expect(dialog).to_be_visible()
        restored_geometry = dialog.bounding_box()
        for key in ('x', 'y', 'width', 'height'):
            assert abs(saved_geometry[key] - restored_geometry[key]) < 2
        page.set_viewport_size({'width': width, 'height': 500})
        page.wait_for_function('document.querySelector(".pai-template-actions .primary").getBoundingClientRect().bottom <= innerHeight - 15')
        send_box = dialog.get_by_role('button', name='Send to AI Chat', exact=True).bounding_box()
        assert send_box['y'] + send_box['height'] <= 500 - 15
        page.set_viewport_size({'width': width, 'height': 900})
        expect(page.locator('.pai-template-count')).to_have_text('2 selected')
        # A saved source retains its windows; changing objective exposes its threshold.
        page.locator('#pai-template-source').select_option('Saved HYPE')
        expect(page.locator('#pai-template-windows')).to_have_value('source')
        source_holdout = page.locator('#pai-template-window-preview').get_by_role('textbox',name='Holdout start (UTC)',exact=True)
        source_holdout.fill('2025-09-02')
        page.reload()
        expect(dialog).to_be_visible()
        expect(source_holdout).to_have_value('2025-09-02')
        page.locator('#pai-template-validations').fill('137')
        page.locator('#pai-template-objective').select_option('balanced')
        expect(page.locator('#pai-template-drawdown')).to_have_value('25')
        page.get_by_role('button', name='Send to AI Chat', exact=True).click()
        expect(dialog).not_to_be_visible()
        page.wait_for_function('document.querySelector("#pbgui-ai-drawer")?.classList.contains("open")')
        page.wait_for_function('document.querySelector(".pai-messages")?.textContent.includes("Create a new optimization loop")')
        assert len(turns) == len(conversations) == 1
        text = turns[0]['message']
        assert 'using optimizer configuration "Saved HYPE"' in text
        assert 'BTC, HYPE on binance, hyperliquid' in text and 'Allow EMA Anchor and Trailing Martingale' in text
        assert 'max_rounds=25, max_runs=25, max_validations=137' in text
        assert 'Use holdout: enabled.' in text and 'PBGUI_LOOP_WINDOWS_V1:' in text
        assert 'Holdout: 2025-09-02 → 2025-12-31' in text
        assert 'drawdown ≤25%' in text
        assert 'no GPU scoring formula is prescribed' in text
        assert 'arithmetic mean of adg_strategy_eq' not in text
        reviewed = json.loads(text.split('PBGUI_LOOP_WINDOWS_V1:')[1].split('\n')[0])
        assert 'objective' not in reviewed
        assert reviewed['max_twe'] == 8 and reviewed['max_validations'] == 137
        assert 'There is no per-round candidate cap' in text
        assert page.locator('#pbgui-ai-drawer .pai-loop-template').count() == 0
        assert not any('/loops' in p or '/vast' in p or '/configs' in p for p in mutations)
        # Existing draft is preserved; another template starts another new chat.
        page.locator('.pai-compose textarea').fill('Unsent user draft')
        page.evaluate('window.PBGuiAI.close()')
        button.click()
        page.locator('#pai-template-objective').select_option('custom')
        page.locator('#pai-template-custom').fill('Minimize drawdown while preserving gain.')
        page.locator('#pai-template-strategy').select_option('ema_anchor')
        page.get_by_role('button', name='Send to AI Chat', exact=True).click()
        page.wait_for_function('document.querySelector(".pai-messages")?.textContent.includes("Minimize drawdown")')
        assert len(turns) == len(conversations) == 2
        assert 'Use EMA Anchor.' in turns[1]['message'] and 'smoothness' not in turns[1]['message']
        expect(page.locator('.pai-compose textarea')).to_have_value('Unsent user draft')
        # A stale chat catalog must never redirect this request into an older chat.
        page.evaluate('window.PBGuiAI.close()')
        button.click()
        hide_newest_chat['enabled'] = True
        page.get_by_role('button', name='Send to AI Chat', exact=True).click()
        expect(dialog).to_be_visible()
        expect(page.locator('.pai-template-error')).to_contain_text('Could not select the new AI chat')
        assert len(conversations) == 3 and len(turns) == 2
        expect(page.locator('.pai-compose textarea')).to_have_value('Unsent user draft')
        # Manual edits cancel a pending automatic lookup; late replies cannot overwrite them.
        delayed_dates['enabled']=True
        page.locator('#pai-template-source').select_option('')
        expect(page.locator('#pai-template-windows')).to_have_value('auto')
        page.wait_for_function('document.querySelector(".pai-template-date-status").textContent.includes("Determining")')
        for _ in range(60):
            if delayed_dates['routes']:
                break
            page.wait_for_timeout(50)
        assert delayed_dates['routes']
        page.locator('#pai-template-start').fill('2025-01-01')
        page.locator('#pai-template-end').fill('2025-01-16')
        for route in delayed_dates['routes']:
            route.fulfill(json={'status':'completed','result':{'start_date_options':{'all_markets':{'available':True,'start_date':'2024-06-01'}}}})
        expect(page.locator('#pai-template-start')).to_have_value('2025-01-01')
        assert any(path.startswith('/api/optimize-v8/ohlcv-start-dates/') for path in mutations)
        page.locator('#pai-template-holdout').uncheck()
        page.locator('#pai-template-timeframes').fill('49')
        expect(page.get_by_role('button',name='Send to AI Chat',exact=True)).to_be_disabled()
        expect(page.locator('.pai-template-error')).to_contain_text('at least two calendar days')
        page.locator('#pai-template-timeframes').fill('4')
        hide_newest_chat['enabled']=False
        page.get_by_role('button',name='Send to AI Chat',exact=True).click()
        page.wait_for_function('document.querySelector(".pai-messages")?.textContent.includes("Use exactly 4 independent")')
        assert len(turns)==3
        prompt=turns[-1]['message']
        assert '1. 2025-01-01 → 2025-01-04' in prompt
        assert '2. 2025-01-05 → 2025-01-08' in prompt
        assert '3. 2025-01-09 → 2025-01-12' in prompt
        assert '4. 2025-01-13 → 2025-01-16' in prompt
        delayed_dates['enabled'] = False
        # A reset discards the entire draft while keeping automatic dates for the next selection.
        page.evaluate('window.PBGuiAI.close()')
        button.click()
        page.locator('#pai-template-notes').fill('Discard these extra restrictions')
        page.locator('#pai-template-rounds').fill('99')
        dialog.get_by_role('button', name='Reset to defaults', exact=True).click()
        expect(page.locator('#pai-template-source')).to_have_value('')
        expect(page.locator('#pai-template-windows')).to_have_value('auto')
        expect(page.locator('#pai-template-strategy')).to_have_value('both')
        expect(page.locator('#pai-template-rounds')).to_have_value('25')
        expect(page.locator('#pai-template-timeframes')).to_have_value('8')
        expect(page.locator('#pai-template-notes')).to_have_value('')
        expect(page.locator('.pai-template-count')).to_have_text('0 selected')
        expect(page.locator('#pai-template-start')).to_have_value('')
        reset_coins = page.locator('#pai-template-coins .ms-input')
        reset_coins.click(); reset_coins.fill('ETH')
        page.locator('#pai-template-coins .ms-option[data-val="ETH"]').click()
        expect(page.locator('#pai-template-start')).to_have_value('2024-01-01')
        expect(page.locator('#pai-template-end')).to_have_value('2026-10-15')
        # Migrate the old empty manual-mode state shown in the reported screenshot.
        page.evaluate("""sessionStorage.setItem('pbgui.ai.loop-template', JSON.stringify({values:{windows:'custom',start:'',end:''},exchanges:['binance'],coins:['ETH']}))""")
        page.reload()
        expect(dialog).to_be_visible()
        expect(page.locator('#pai-template-windows')).to_have_value('auto')
        expect(page.locator('#pai-template-start')).to_have_value('2024-01-01')
        expect(page.locator('#pai-template-end')).to_have_value('2026-10-15')
        # New manual drafts, including incomplete edits, must survive a browser reload.
        page.locator('#pai-template-start').fill('2025-02-01')
        page.locator('#pai-template-end').fill('')
        dialog.locator('.pbg-modal-head').click()
        page.reload()
        expect(dialog).to_be_visible()
        expect(page.locator('#pai-template-windows')).to_have_value('custom')
        expect(page.locator('#pai-template-start')).to_have_value('2025-02-01')
        expect(page.locator('#pai-template-end')).to_have_value('')
        assert not errors, errors
        browser.close()
