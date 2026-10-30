"""Isolated browser checks for the real Template and shared window editor."""
import json
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import pytest
from browser_driver import expect, sync_browser

ROOT = Path(__file__).resolve().parents[2]


def assert_quiet_success(page):
    """Keep empty live regions rendered without spacing or redundant success copy."""
    dialog = page.get_by_role('dialog', name='AI Loop Template', exact=True)
    for text in ('Common market start including warmup → last complete UTC day.',
                 'Window preview (inclusive UTC dates)',
                 'Windows cover the selected period without gaps.'):
        expect(dialog).not_to_contain_text(text)
    statuses = dialog.locator('.pai-template-date-status')
    expect(statuses).to_have_count(2)
    for status in statuses.all():
        expect(status).to_have_text('')
        expect(status).to_have_attribute('role', 'status')
        assert status.evaluate("""n => {
            const s = getComputedStyle(n);
            return s.display !== 'none' && s.marginTop === '0px' &&
                s.marginBottom === '0px' && n.getBoundingClientRect().height === 0;
        }""")


@pytest.mark.parametrize('width', [700, 1400])
def test_reviewed_holdout_dates_survive_edit_focus_reload_and_send(width):
    """No provider, downloads, saved configs, optimizer jobs or rentals are contacted."""
    __tracebackhide__ = True
    source = (ROOT / 'frontend/v7_optimize.html').read_text()
    for key, value in {'API_BASE':'/api/optimize-v8','OPTIMIZE_VERSION':'v8','VERSION':'test',
                       'SERIAL':'1','NAV_HASH':'test','BASE_PREFIX':'','LIMITS_META':'{}',
                       'BACKTEST_VERSION':'v8','OPTIMIZE_NAV_TITLE':'PB8 Optimize','OPTIMIZE_NAV_CURRENT':'v8_optimize'}.items():
        source = source.replace('%%'+key+'%%', value)
    errors = []
    with sync_browser() as driver:
        browser = driver.chromium.launch()
        page = browser.new_page(viewport={'width':width,'height':900})
        page.set_default_timeout(7000)
        page.on('pageerror', lambda error: errors.append(str(error)))
        def respond(route):
            """Serve versioned local assets and deterministic API fixtures only."""
            path = urlparse(route.request.url).path
            if path == '/api/optimize-v8/main_page':
                route.fulfill(body=source, content_type='text/html')
            elif path.startswith('/app/'):
                asset = ROOT / 'frontend' / path.removeprefix('/app/')
                route.fulfill(body=asset.read_bytes() if asset.is_file() else b'',
                    content_type='text/css' if asset.suffix=='.css' else 'text/javascript')
            elif path == '/api/ai/loop-template-options':
                route.fulfill(json={'configs':[]})
            elif path == '/api/v8/coins/status':
                route.fulfill(json={'symbols':['BTC'],'catalog':[{'config_id':'BTC','display':'BTC'}],
                    'statuses':{'BTC':{'normalized':'BTC','status':'valid'}}})
            elif path.endswith('/metadata'):
                route.fulfill(json={'runtime_ready':False,'optimize_defaults':{},'bot_defaults':{},'live_defaults':{},'backtest_defaults':{}})
            elif path.endswith('/configs/new-config'):
                route.fulfill(json={'config':{'live':{},'backtest':{},'optimize':{}}})
            elif path.endswith('/ohlcv-start-dates'):
                route.fulfill(json={'job_id':'a'*12,'status':'completed','result':{'start_date_options':{'all_markets':{'available':True,'start_date':'2024-01-01'}}}})
            else:
                route.fulfill(json={})
        page.route('**/*', respond)
        page.goto('http://windows.test/api/optimize-v8/main_page')
        page.locator('#btn-ai-loop-template').click()
        dialog = page.get_by_role('dialog',name='AI Loop Template',exact=True)
        holdout = page.locator('#pai-template-holdout')
        expect(holdout).to_be_checked()
        coins = page.locator('#pai-template-coins .ms-input')
        coins.fill('BTC')
        page.locator('#pai-template-coins .ms-option[data-val="BTC"]').click()
        page.locator('#pai-template-windows').select_option('custom')
        page.locator('#pai-template-timeframes').fill('3')
        page.locator('#pai-template-start').fill('2024-01-01')
        page.locator('#pai-template-end').fill('2024-01-10')
        preview = page.locator('#pai-template-window-preview')
        expect(preview.get_by_role('textbox',name='Training 1 end (UTC)',exact=True)).to_have_value('2024-01-02')
        expect(preview.get_by_role('textbox',name='Holdout start (UTC)',exact=True)).to_have_value('2024-01-08')
        expect(preview.locator('input[type="date"]')).to_have_count(8)
        send = dialog.get_by_role('button',name='Send to AI Chat',exact=True)
        # Overlapping inclusive dates block submission without splitting or adding windows.
        preview.get_by_role('textbox',name='Training 2 start (UTC)',exact=True).fill('2024-01-02')
        expect(send).to_be_disabled()
        expect(page.locator('.pai-template-error')).to_contain_text('overlap')
        preview.get_by_role('textbox',name='Training 2 start (UTC)',exact=True).fill('2024-01-03')
        preview.get_by_role('textbox',name='Holdout start (UTC)',exact=True).fill('2024-01-09')
        expect(page.locator('.pai-template-error')).to_have_text('')
        expect(send).to_be_enabled()
        expect(preview.locator('input[type="date"]')).to_have_count(8)
        page.evaluate('window.dispatchEvent(new Event("focus"))')
        expect(preview.get_by_role('textbox',name='Holdout start (UTC)',exact=True)).to_have_value('2024-01-09')
        page.reload()
        expect(dialog).to_be_visible()
        expect(holdout).to_be_checked()
        expect(preview.get_by_role('textbox',name='Holdout start (UTC)',exact=True)).to_have_value('2024-01-09')
        assert page.locator('.pai-loop-template').evaluate('(n)=>n.scrollWidth<=n.clientWidth')
        page.evaluate('window.PBGuiAI={ensureSelection:async()=>{},isBusy:()=>false,sendLoopPrompt:async text=>{window.sent=text}}')
        send.click()
        page.wait_for_function('typeof window.sent==="string"')
        text = page.evaluate('window.sent')
        value = json.loads(text.split('PBGUI_LOOP_WINDOWS_V1:')[1].split('\n')[0])
        assert value['use_holdout'] and value['windows'][-1]['start_date']=='2024-01-09'
        assert 'objective' not in value
        assert value['max_twe'] == 8 and value['max_validations'] == 100
        assert 'no fixed evaluation metric is prescribed' in text
        assert 'arithmetic mean of adg_strategy_eq' not in text
        assert 'independent user-only background reports' in text and 'Use exactly 3 independent' in text
        page.locator('#btn-ai-loop-template').click()
        holdout.uncheck()
        expect(preview.locator('input[type="date"]')).to_have_count(6)
        expect(preview.get_by_role('textbox',name='Training 3 end (UTC)',exact=True)).to_have_value('2024-01-10')
        send.click()
        page.wait_for_function('window.sent.includes("Use holdout: disabled")')
        page.locator('#btn-ai-loop-template').click()
        # An invalid draft must not leave stale error/status text after Reset.
        page.locator('#pai-template-timeframes').fill('49')
        expect(page.locator('.pai-template-error')).to_contain_text('at least two calendar days')
        dialog.get_by_role('button',name='Reset to defaults',exact=True).click()
        expect(holdout).to_be_checked()
        expect(page.locator('.pai-template-error')).to_have_text('')
        expect(page.locator('#pai-template-window-status')).not_to_contain_text('Use valid UTC calendar dates.')
        expect(preview.locator('input[type="date"]')).to_have_count(0)
        expect(page.locator('#pai-template-start')).to_have_value('')
        expect(page.locator('#pai-template-end')).to_have_value('')
        # Legacy drafts have no holdout flag and migrate to enabled.
        page.evaluate("sessionStorage.setItem('pbgui.ai.loop-template',JSON.stringify({values:{windows:'custom',timeframes:'3',start:'2024-01-01',end:'2024-01-10'},coins:['BTC'],exchanges:['binance']}))")
        page.reload()
        expect(holdout).to_be_checked()
        expect(preview.locator('input[type="date"]')).to_have_count(8)
        assert not errors
        browser.close()


@pytest.fixture
def automatic_lookup_page():
    """Open the real Template with HYPE markets and fully isolated, deferred jobs."""
    source = (ROOT / 'frontend/v7_optimize.html').read_text()
    for key, value in {'API_BASE':'/api/optimize-v8','OPTIMIZE_VERSION':'v8','VERSION':'test',
                       'SERIAL':'1','NAV_HASH':'test','BASE_PREFIX':'','LIMITS_META':'{}',
                       'BACKTEST_VERSION':'v8','OPTIMIZE_NAV_TITLE':'PB8 Optimize','OPTIMIZE_NAV_CURRENT':'v8_optimize'}.items():
        source = source.replace('%%'+key+'%%', value)
    state = {'jobs': [], 'stops': [], 'polls': [], 'errors': [], 'market_routes': [], 'hold_markets': False}
    with sync_browser() as driver:
        browser = driver.chromium.launch()
        page = browser.new_page(viewport={'width':1400,'height':900})
        page.set_default_timeout(5000)
        page.clock.install(time=datetime(2026, 10, 18, 12, tzinfo=timezone.utc))
        page.on('pageerror', lambda error: state['errors'].append(str(error)))
        page.add_init_script("""if (!sessionStorage.getItem('pbgui.ai.loop-template')) {
          sessionStorage.setItem('pbgui.ai.loop-template', JSON.stringify({schema_version:3,
          values:{windows:'auto',timeframes:'3',holdout:'on',source:''},
          exchanges:['hyperliquid','bybit'],coins:['HYPE']})); }""")

        def respond(route):
            """Never contact a real API, exchange, AI provider or saved runtime data."""
            path = urlparse(route.request.url).path
            if path == '/api/optimize-v8/main_page':
                route.fulfill(body=source, content_type='text/html')
            elif path.startswith('/app/'):
                asset = ROOT / 'frontend' / path.removeprefix('/app/')
                route.fulfill(body=asset.read_bytes() if asset.is_file() else b'',
                              content_type='text/css' if asset.suffix == '.css' else 'text/javascript')
            elif path == '/api/ai/loop-template-options':
                route.fulfill(json={'configs': []})
            elif path == '/api/v8/coins/status':
                if state['hold_markets']:
                    state['market_routes'].append(route)
                else:
                    route.fulfill(json={'symbols':['HYPE','BTC'], 'statuses':{}, 'catalog':[]})
            elif path.endswith('/configs/new-config'):
                route.fulfill(json={'config':{'live':{},'backtest':{},'optimize':{}}})
            elif path.endswith('/ohlcv-start-dates'):
                state['jobs'].append(route.request.post_data_json)
                route.fulfill(json={'job_id':format(len(state['jobs']),'012x'), 'status':'queued'})
            elif '/ohlcv-start-dates/' in path:
                if route.request.method == 'DELETE':
                    state['stops'].append(path)
                    route.fulfill(json={'status':'stopped'})
                else:
                    state['polls'].append(route)
            else:
                route.fulfill(json={})

        page.route('**/*', respond)
        page.goto('http://lookup.test/api/optimize-v8/main_page')
        page.locator('#btn-ai-loop-template').click()
        _wait_for_date_poll(page, state)
        yield page, state
        assert not state['errors']
        browser.close()


def _wait_for_date_poll(page, state):
    """Wait briefly for an intercepted poll, without real network or long deadlines."""
    for _ in range(100):
        if state['polls']:
            return
        page.wait_for_timeout(50)
    pytest.fail('No date lookup poll arrived')


def _complete_date_poll(state, start='2024-01-01'):
    """Publish the documented terminal lookup payload from an isolated fixture."""
    state['polls'].pop(0).fulfill(json={'status':'completed','result':{'start_date_options':{
        'all_markets':{'available':True,'start_date':start}}}})


def test_automatic_lookup_finalizes_without_focus_cancellation(automatic_lookup_page):
    """Progress success is waiting; terminal success and reviewed edits survive refresh."""
    page, state = automatic_lookup_page
    state['polls'].pop(0).fulfill(json={'status':'running','progress':{
        'phase':'complete','message':'OHLCV start dates resolved'}})
    _wait_for_date_poll(page, state)
    expect(page.locator('.pai-template-date-status').first).to_have_text('Finalizing available training dates…')
    expect(page.locator('.pai-template-error')).to_have_text('')
    expect(page.locator('#pai-template-window-status')).not_to_contain_text('Use valid UTC calendar dates.')
    send = page.get_by_role('button', name='Send to AI Chat', exact=True)
    expect(send).to_be_disabled()
    page.evaluate("window.dispatchEvent(new Event('focus')); document.dispatchEvent(new Event('visibilitychange'))")
    page.wait_for_timeout(150)
    assert len(state['jobs']) == 1 and not state['stops']
    _complete_date_poll(state)
    expect(page.locator('#pai-template-start')).to_have_value('2024-01-01')
    expect(page.locator('#pai-template-end')).to_have_value('2026-10-17')
    preview = page.locator('#pai-template-window-preview')
    expect(preview.locator('input[type="date"]')).to_have_count(8)
    expect(send).to_be_enabled()
    expect(page.locator('.pai-template-error')).to_have_text('')
    holdout = preview.get_by_role('textbox', name='Holdout start (UTC)', exact=True)
    assert_quiet_success(page)
    holdout.fill('2026-02-06')
    expect(send).to_be_enabled()
    page.evaluate("window.dispatchEvent(new Event('focus')); document.dispatchEvent(new Event('visibilitychange'))")
    expect(holdout).to_have_value('2026-02-06')
    page.reload()
    expect(page.get_by_role('dialog',name='AI Loop Template',exact=True)).to_be_visible()
    expect(holdout).to_have_value('2026-02-06')
    expect(send).to_be_enabled()
    assert len(state['jobs']) == 1
    page.locator('#pai-template-holdout').uncheck()
    expect(preview.locator('input[type="date"]')).to_have_count(6)
    assert_quiet_success(page)
    # Editing windows cannot authorize dates from a different coin context.
    page.locator('#pai-template-coins .ms-x[data-val="HYPE"]').click()
    page.locator('#pai-template-coins .ms-input').fill('BTC')
    page.locator('#pai-template-coins .ms-option[data-val="BTC"]').click()
    _wait_for_date_poll(page, state)
    assert len(state['jobs']) == 2
    expect(send).to_be_disabled()


@pytest.mark.parametrize('problem', ['overlap', 'reversed', 'outside', 'invalid'])
def test_quiet_success_and_warning_recovery(automatic_lookup_page, problem):
    """Recalculation and recovery preserve live status, roles and reviewed dates."""
    page, state = automatic_lookup_page
    _complete_date_poll(state)
    send = page.get_by_role('button', name='Send to AI Chat', exact=True)
    expect(send).to_be_enabled()
    assert_quiet_success(page)
    page.locator('#pai-template-timeframes').fill('4')
    page.locator('#pai-template-timeframes').blur()
    expect(page.locator('#pai-template-window-preview input[type="date"]')).to_have_count(10)
    assert_quiet_success(page)
    page.locator('#pai-template-timeframes').fill('3')
    page.locator('#pai-template-timeframes').blur()
    preview = page.locator('#pai-template-window-preview')
    start = preview.get_by_role('textbox', name='Training 2 start (UTC)', exact=True)
    original = start.input_value()
    previous_end = preview.get_by_role('textbox', name='Training 1 end (UTC)', exact=True).input_value()
    next_day = datetime.fromisoformat(original).replace(tzinfo=timezone.utc).timestamp() + 86400
    gap_start = datetime.fromtimestamp(next_day, timezone.utc).date().isoformat()
    start.fill(gap_start)
    warning = page.locator('#pai-template-window-status')
    expect(warning).to_have_text('1 unused calendar days between or outside windows.')
    expect(warning).to_be_visible()
    expect(send).to_be_enabled()
    bad = {'overlap': previous_end, 'reversed': '2026-10-17',
           'outside': '2023-12-31', 'invalid': ''}[problem]
    start.fill(bad)
    expect(send).to_be_disabled()
    expect(page.locator('.pai-template-error')).not_to_have_text('')
    expect(page.locator('.pai-template-date-status').first).not_to_have_text('')
    start.fill(original)
    expect(send).to_be_enabled()
    assert_quiet_success(page)
    page.evaluate("window.dispatchEvent(new Event('focus'))")
    expect(start).to_have_value(original)
    assert_quiet_success(page)
    page.get_by_role('dialog', name='AI Loop Template', exact=True).get_by_role('button', name='Close', exact=True).click()
    page.locator('#btn-ai-loop-template').click()
    expect(send).to_be_enabled()
    assert_quiet_success(page)
    page.reload()
    expect(send).to_be_enabled()
    expect(start).to_have_value(original)
    assert_quiet_success(page)


@pytest.mark.parametrize('failure', ['missing', 'invalid', 'short', 'request_timeout', 'lookup_timeout'])
def test_automatic_lookup_failure_and_retry(automatic_lookup_page, failure):
    """Every failure clears waiting, stays visible on focus and can recover safely."""
    page, state = automatic_lookup_page
    if failure == 'request_timeout':
        page.clock.fast_forward(15010)
        expected = 'request timed out'
    elif failure == 'lookup_timeout':
        # Keep polling responsive while the actual helper never finishes.
        for _ in range(15):
            state['polls'].pop(0).fulfill(json={'status':'running','progress':{'phase':'complete'}})
            page.clock.fast_forward(14000)
            page.wait_for_timeout(20)
        expected = 'did not finish within 210 seconds'
    elif failure == 'missing':
        state['polls'].pop(0).fulfill(json={'status':'completed','result':{'start_date_options':{
            'all_markets':{'available':False,'detail':'No common date.'},
            'missing_market_pairs':[{'exchange':'bybit','coin':'HYPE'}],
            'unknown_start_pairs':[{'exchange':'hyperliquid','coin':'HYPE'}]}}})
        expected = 'bybit/HYPE'
    else:
        _complete_date_poll(state, '2024-02-30' if failure == 'invalid' else '2026-10-16')
        expected = 'valid UTC calendar dates' if failure == 'invalid' else 'at least two calendar days'
    footer = page.locator('.pai-template-error')
    expect(footer).to_contain_text(expected)
    expect(page.locator('#pai-template-window-status')).to_contain_text(expected)
    expect(page.get_by_role('button',name='Send to AI Chat',exact=True)).to_be_disabled()
    expect(page.locator('#pai-template-start')).to_have_value('')
    if failure.endswith('timeout'):
        assert state['stops']
    # A late response cannot revive a timed-out generation.
    while state['polls']:
        _complete_date_poll(state)
    page.evaluate("window.dispatchEvent(new Event('focus'))")
    page.wait_for_timeout(100)
    expect(footer).to_contain_text(expected)
    assert len(state['jobs']) == 1
    page.get_by_role('button',name='Retry date lookup',exact=True).click()
    _wait_for_date_poll(page, state)
    _complete_date_poll(state)
    expect(footer).to_have_text('')
    expect(page.locator('#pai-template-start')).to_have_value('2024-01-01')
    expect(page.locator('#pai-template-window-preview input[type="date"]')).to_have_count(8)
    expect(page.get_by_role('button',name='Send to AI Chat',exact=True)).to_be_enabled()
    assert_quiet_success(page)


@pytest.mark.parametrize('action', ['manual', 'close', 'selection'])
def test_late_automatic_lookup_cannot_revive_cancelled_context(automatic_lookup_page, action):
    """Old completions cannot overwrite manual dates or a newer selection/closed dialog."""
    page, state = automatic_lookup_page
    old = state['polls'].pop(0)
    if action == 'manual':
        page.locator('#pai-template-start').fill('2025-01-01')
        page.locator('#pai-template-end').fill('2025-12-31')
    elif action == 'close':
        page.get_by_role('dialog',name='AI Loop Template',exact=True).get_by_role('button',name='Close',exact=True).click()
    else:
        page.locator('#pai-template-coins .ms-input').fill('BTC')
        page.locator('#pai-template-coins .ms-option[data-val="BTC"]').click()
        _wait_for_date_poll(page, state)
    old.fulfill(json={'status':'completed','result':{'start_date_options':{
        'all_markets':{'available':True,'start_date':'2024-06-01'}}}})
    page.wait_for_timeout(50)
    assert state['stops']
    if action == 'manual':
        expect(page.locator('#pai-template-start')).to_have_value('2025-01-01')
    elif action == 'close':
        expect(page.get_by_role('dialog',name='AI Loop Template',exact=True)).not_to_be_visible()
    else:
        expect(page.locator('#pai-template-start')).to_have_value('')
        _complete_date_poll(state)
        expect(page.locator('#pai-template-start')).to_have_value('2024-01-01')
        page.get_by_role('button',name='Clear coins',exact=True).click()
        expect(page.get_by_role('button',name='Send to AI Chat',exact=True)).to_be_disabled()


@pytest.mark.parametrize('edited', [False, True])
def test_automatic_utc_day_rollover_preserves_only_reviewed_edits(automatic_lookup_page, edited):
    """Automatic ranges follow the UTC day while explicit reviewed boundaries survive."""
    page, state = automatic_lookup_page
    _complete_date_poll(state)
    expect(page.locator('#pai-template-end')).to_have_value('2026-10-17')
    holdout = page.locator('#pai-template-window-preview').get_by_role('textbox',name='Holdout start (UTC)',exact=True)
    if edited:
        holdout.fill('2026-02-06')
    page.clock.set_fixed_time(datetime(2026, 10, 19, 12, tzinfo=timezone.utc))
    page.evaluate("window.dispatchEvent(new Event('focus'))")
    if edited:
        expect(holdout).to_have_value('2026-02-06')
        expect(page.locator('#pai-template-end')).to_have_value('2026-10-17')
        page.wait_for_timeout(100)
        assert len(state['jobs']) == 1
    else:
        _wait_for_date_poll(page, state)
        assert len(state['jobs']) == 2
        _complete_date_poll(state)
        expect(page.locator('#pai-template-end')).to_have_value('2026-10-18')


def test_market_focus_timeout_recovers_without_new_lookup(automatic_lookup_page):
    """A stuck prerequisite request cannot permanently block an already reviewed plan."""
    page, state = automatic_lookup_page
    _complete_date_poll(state)
    send = page.get_by_role('button',name='Send to AI Chat',exact=True)
    expect(send).to_be_enabled()
    state['hold_markets'] = True
    page.evaluate("window.dispatchEvent(new Event('focus'))")
    page.wait_for_timeout(50)
    assert state['market_routes']
    page.clock.fast_forward(15010)
    expect(page.locator('.pai-template-error')).to_contain_text('Market availability request timed out')
    expect(send).to_be_disabled()
    state['hold_markets'] = False
    for route in state['market_routes']:
        route.fulfill(json={'symbols':['STALE'], 'statuses':{}})
    page.clock.fast_forward(1000)
    expect(send).to_be_enabled()
    expect(page.locator('.pai-template-error')).to_have_text('')
    expect(page.locator('#pai-template-start')).to_have_value('2024-01-01')
    assert len(state['jobs']) == 1 and not state['stops']


@pytest.mark.parametrize('control', ['timeframes', 'holdout'])
def test_restored_date_basis_cannot_be_rebound_during_market_check(automatic_lookup_page, control):
    """Window edits cannot authorize old OHLCV dates for newly selected exchanges."""
    page, state = automatic_lookup_page
    _complete_date_poll(state)
    send = page.get_by_role('button',name='Send to AI Chat',exact=True)
    expect(send).to_be_enabled()
    page.reload()
    expect(send).to_be_enabled()
    state['hold_markets'] = True
    page.locator('#pai-template-exchanges .ms-input').fill('binance')
    page.locator('#pai-template-exchanges .ms-option[data-val="binance"]').click()
    page.wait_for_timeout(100)
    assert state['market_routes']
    if control == 'timeframes':
        page.locator('#pai-template-timeframes').fill('4')
        page.locator('#pai-template-timeframes').blur()
    else:
        page.locator('#pai-template-holdout').uncheck()
    expect(page.locator('#pai-template-start')).to_have_value('')
    expect(send).to_be_disabled()
    state['hold_markets'] = False
    for route in state['market_routes']:
        route.fulfill(json={'symbols':['HYPE','BTC'],'statuses':{},'catalog':[]})
    _wait_for_date_poll(page, state)
    assert len(state['jobs']) == 2
    assert set(state['jobs'][-1]['config']['backtest']['exchanges']) == {'hyperliquid','bybit','binance'}
    expect(send).to_be_disabled()
    _complete_date_poll(state, '2025-01-01')
    expect(page.locator('#pai-template-start')).to_have_value('2025-01-01')
    expect(page.locator('#pai-template-window-preview input[type="date"]')).to_have_count(10 if control == 'timeframes' else 6)
    expect(send).to_be_enabled()


def test_legacy_reviewed_windows_revalidate_dates_without_discarding_edits(automatic_lookup_page):
    """Older saved drafts need fresh coverage evidence, retaining valid reviewed boundaries."""
    page, state = automatic_lookup_page
    _complete_date_poll(state)
    holdout = page.locator('#pai-template-window-preview').get_by_role('textbox',name='Holdout start (UTC)',exact=True)
    expect(holdout).to_be_visible()
    holdout.fill('2026-02-06')
    page.evaluate("""const key='pbgui.ai.loop-template'; const saved=JSON.parse(sessionStorage.getItem(key));
      saved.schema_version=3; delete saved.date_basis; sessionStorage.setItem(key,JSON.stringify(saved));""")
    page.reload()
    _wait_for_date_poll(page, state)
    expect(page.get_by_role('button',name='Send to AI Chat',exact=True)).to_be_disabled()
    assert len(state['jobs']) == 2
    _complete_date_poll(state)
    expect(holdout).to_have_value('2026-02-06')
    expect(page.get_by_role('button',name='Send to AI Chat',exact=True)).to_be_enabled()
