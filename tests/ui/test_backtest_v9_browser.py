"""Native PB9 handoff on the production shared Backtest page, entirely offline."""
import copy
import json
import mimetypes
from pathlib import Path
from urllib.parse import urlparse, parse_qs
from types import SimpleNamespace

import pytest
from browser_driver import sync_browser, expect

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def backtest():
    """Serve synthetic PB9 data and real shared assets with no production access."""
    metadata = json.loads((ROOT / 'tests/fixtures/pb9_run_metadata.json').read_text())
    config = copy.deepcopy(metadata['template'])
    config['bot']['long'] = {'enabled': True}
    with sync_browser() as driver:
        browser = driver.chromium.launch()
        page = browser.new_page(viewport={'width': 1440, 'height': 1000})
        page.set_default_timeout(5000)
        state = SimpleNamespace(page=page, config=config, saved=[], queued=[], errors=[], unexpected=[], metadata=metadata)
        page.on('pageerror', lambda error: state.errors.append(str(error)))

        def respond(route):
            """Every request is explicit; exchanges and real PBGui remain unreachable."""
            url = urlparse(route.request.url)
            path = url.path
            if url.hostname != 'backtest9.test':
                state.unexpected.append(route.request.url); route.abort()
            elif path in ('/api/backtest-v9/main_page', '/api/v9/main_page'):
                source = (ROOT / 'frontend' / ('v7_run.html' if path == '/api/v9/main_page' else 'v7_backtest.html')).read_text()
                values = {'API_BASE':'/api/backtest-v9', 'WS_BASE':'ws://backtest9.test',
                          'VERSION':'test', 'SERIAL':'1', 'MASTER_NAME':'fixture', 'NAV_HASH':'test', 'BASE_PREFIX':'',
                          'BACKTEST_VERSION':'v9', 'BACKTEST_LABEL':'V9', 'BACKTEST_SUBTITLE':'PBv9 BACKTEST',
                          'BACKTEST_NAV_CURRENT':'v9_backtest', 'RUN_VERSION':'v9', 'RUN_LABEL':'PB9'}
                for key, value in values.items():
                    source = source.replace('"%%'+key+'%%"',json.dumps(value)).replace('%%'+key+'%%',value)
                route.fulfill(content_type='text/html',body=source)
            elif path.startswith('/app/'):
                asset = ROOT / 'frontend' / path.removeprefix('/app/')
                route.fulfill(body=asset.read_bytes() if asset.is_file() else b'', content_type=mimetypes.guess_type(asset)[0] or 'text/plain')
            elif path.endswith('/editor/metadata'):
                route.fulfill(json=metadata)
            elif path.endswith('/settings'):
                route.fulfill(json={'exchange_options':['binance','bybit'],'cpu':1,'autostart':False})
            elif path.endswith('/optimize-draft/fixture-draft'):
                route.fulfill(json={'config':copy.deepcopy(config),'param_status':{}})
            elif path.endswith('/configs/prepare'):
                route.fulfill(json={'config':route.request.post_data_json['config'],'param_status':{}})
            elif path.endswith('/configs'):
                route.fulfill(json={'configs':[]})
            elif '/configs/' in path and route.request.method == 'PUT':
                state.saved.append(route.request.post_data_json)
                route.fulfill(json={'ok':True})
            elif '/configs/' in path:
                route.fulfill(json={'config':state.saved[-1]['config'] if state.saved else config, 'name':path.rsplit('/',1)[1]})
            elif path.endswith('/queue') and route.request.method == 'POST':
                state.queued.append(route.request.post_data_json)
                route.fulfill(json={'ok':True,'filename':'11111111-1111-4111-8111-111111111111'})
            elif path.endswith('/queue'):
                route.fulfill(json={'queue':[],'items':[]})
            elif path.endswith('/results'):
                route.fulfill(json={'results':[],'pagination':{'has_more':False,'total':0}})
            elif '/api/backtest-v7/' in path or '/api/backtest-v8/' in path or '/api/v7/' in path or '/api/v8/' in path:
                state.unexpected.append(path); route.abort()
            elif path == '/api/help/index':
                route.fulfill(json=[{'file':'54_pbv9_backtest','title':'PB9 Backtest'}])
            elif path == '/api/help/content':
                route.fulfill(json={'content':'# PB9 Backtest\nNative isolated test guide.'})
            else:
                route.fulfill(json={})

        page.route('**/*',respond)
        page.goto('http://backtest9.test/api/backtest-v9/main_page?opt_draft_id=fixture-draft&draft_name=fixture')
        yield state
        browser.close()


def test_native_handoff_roundtrip_and_queue(backtest):
    """The shared editor keeps native sparse source and queues the current dates/balance."""
    page = backtest.page
    expect(page.locator('#cfg-name')).to_have_value('fixture')
    expect(page.locator('#cfg-starting-balance')).to_have_value(str(backtest.config['backtest']['starting_balance']))
    page.locator('#cfg-start-date').fill('2025-01-01')
    page.locator('#cfg-end-date').fill('2025-01-03')
    page.locator('#cfg-starting-balance').fill('2500')
    page.locator('#sidebar-editor button[onclick="saveAndQueue()"]').click()
    page.wait_for_function('currentPanel === "queue"')
    assert len(backtest.saved) == len(backtest.queued) == 1
    source = backtest.saved[0]['config']
    assert source['bot']['long'] == {'enabled':True}
    assert source['bot']['short'] == {}
    assert source['backtest']['starting_balance'] == 2500
    assert source['backtest']['start_date'] == '2025-01-01'
    assert source['backtest']['end_date'] == '2025-01-03'
    assert 'btc_collateral_cap' not in source['backtest']
    assert 'hsl_signal_mode' not in source['live']
    assert backtest.queued[0]['config'] == source
    assert backtest.errors == [], "\n".join(backtest.errors)
    assert backtest.unexpected == []


def test_native_editor_reload_retains_owned_draft_and_invalid_json(backtest):
    """Reload returns to the draft; invalid JSON never saves or silently disappears."""
    page = backtest.page
    expect(page.locator('#cfg-name')).to_have_value('fixture')
    assert 'opt_draft_id=' in page.url
    page.reload()
    expect(page.locator('#cfg-name')).to_have_value('fixture')
    page.locator('#cfg-bot-long').fill('{')
    page.locator('#sidebar-editor button[onclick="saveEditor()"]').click()
    assert backtest.saved == []
    expect(page.locator('#cfg-bot-long')).to_have_value('{')
    assert backtest.errors == [], "\n".join(backtest.errors)


def test_native_menu_and_results_stay_v9(backtest):
    """Menu, result filters and API requests cannot fall back to v7/v8."""
    page = backtest.page
    expect(page.locator('#cfg-name')).to_have_value('fixture')
    page.locator('.nav-group-btn[data-group="pbv9"]').click()
    page.locator('.nav-item[data-page="v9_run"]').click()
    page.locator('.nav-group-btn[data-group="pbv9"]').click()
    page.locator('.nav-item[data-page="v9_backtest"]').click()
    assert urlparse(page.url).path == '/api/backtest-v9/main_page'
    page.goto('http://backtest9.test/api/backtest-v9/main_page?opt_draft_id=fixture-draft&draft_name=fixture')
    expect(page.locator('#cfg-name')).to_have_value('fixture')
    page.locator('#sidebar-editor button[onclick="closeEditor()"]').click()
    page.locator('#sidebar [data-panel="results"]').click()
    expect(page.locator('#results-version-filter')).to_have_value('v9')
    assert backtest.unexpected == []
    assert backtest.errors == [], "\n".join(backtest.errors)


def test_native_guide_and_sidebar_use_shared_shell(backtest):
    """Native Backtest retains the shared Guide overlay and bounded responsive sidebar."""
    page=backtest.page
    expect(page.locator('#cfg-name')).to_have_value('fixture')
    destination=page.url
    page.locator('#pbgui-guide-btn').click()
    expect(page.locator('#pbgui-shared-help-box')).to_be_visible()
    expect(page.locator('#pbgui-shared-help-box')).to_contain_text('PB9 Backtest')
    assert page.url==destination
    page.locator('#pbgui-shared-help-close').click()
    for key,width in [('Home',160),('End',420)]:
        page.locator('#sidebar-resize').focus();page.keyboard.press(key)
        assert round(page.locator('#sidebar').bounding_box()['width'])==width
    for width in [700,800,1440]:
        page.set_viewport_size({'width':width,'height':1000})
        assert page.locator('#sidebar').evaluate('(el)=>el.scrollWidth<=el.clientWidth')
        assert page.evaluate('document.documentElement.scrollWidth<=document.documentElement.clientWidth')
    assert backtest.errors==[], '\n'.join(backtest.errors)


def test_native_equity_display_keeps_missing_balance_as_a_gap():
    """The shared chart adapter retains native UTC times and missing ledger samples."""
    from javascript_checks import run_javascript
    source=(ROOT/'frontend/v7_backtest.html').read_text()
    start=source.index('function normalizeBE(')
    function=source[start:source.index('\nfunction ',start+1)]
    script='''const assert=browserAssert;
    '''+function+'''
    const actual=normalizeBE({headers:['timestamp','usd_total_balance','usd_total_equity'],rows:[
      {timestamp:'2025-01-01T00:00:00+00:00',usd_total_balance:'',usd_total_equity:'1000'},
      {timestamp:'2025-01-01T00:01:00+00:00',usd_total_balance:'1001',usd_total_equity:'1002'}
    ]},{backtest_version:'v9'});
    assert.deepEqual(actual.time,['2025-01-01T00:00:00+00:00','2025-01-01T00:01:00+00:00']);
    assert.deepEqual(actual.balance,[null,1001]);assert.deepEqual(actual.equity,[1000,1002]);
    '''
    result=run_javascript(['-e',script],cwd=ROOT,capture_output=True,text=True,check=False)
    assert result.returncode==0,result.stderr or result.stdout


def _open_pb8_conversion(backtest, *, panel='configs', accept_status=200):
    """Keep production PB8/PB9 pages/assets; replace only isolated API responses."""
    page = backtest.page
    backtest.migration_calls = []
    report = {'mode':'best_effort','source_version':'v8.6.0','target_version':'v9.0.0',
              'changes':[{'path':'bot.global.risk','reason':'<img src=x onerror=alert(1)>'}],
              'warnings':['Review inferred risk memory']}
    def respond(route):
        """Record the two-phase handoff while exposing no real account or runtime data."""
        path=urlparse(route.request.url).path
        if path.endswith('/main_page'):
            source=(ROOT/'frontend/v7_backtest.html').read_text()
            values={'API_BASE':'/api/backtest-v8','WS_BASE':'ws://backtest9.test','VERSION':'test','SERIAL':'1',
                    'MASTER_NAME':'fixture','NAV_HASH':'test','BASE_PREFIX':'','BACKTEST_VERSION':'v8',
                    'BACKTEST_LABEL':'V8','BACKTEST_SUBTITLE':'PBv8 BACKTEST','BACKTEST_NAV_CURRENT':'v8_backtest'}
            for key,value in values.items():source=source.replace('"%%'+key+'%%"',json.dumps(value)).replace('%%'+key+'%%',value)
            route.fulfill(content_type='text/html',body=source)
        elif path.endswith('/configs'):
            route.fulfill(json={'configs':[{'name':'example','exchanges':['binance'],'strategy':'trailing_martingale',
                     'coins':1,'twe_long':1,'twe_short':0,'results':1,'modified':1}]})
        elif path.endswith('/results'):
            route.fulfill(json={'results':[{'path':'example/binance/run','config_name':'example',
                     'result_name':'run','backtest_version':'v8','exchange_dir':'binance','strategy':'trailing_martingale'}],
                     'pagination':{'has_more':False,'total':1}})
        else:route.fulfill(json={'queue':[],'items':[],'cpu':1,'autostart':False,'exchange_options':['binance']})
    def migrate(route):
        """An accepted proposal returns an owned draft, never saved data or queue work."""
        kind=urlparse(route.request.url).path.rsplit('/',1)[1]
        backtest.migration_calls.append((kind,route.request.post_data_json))
        if kind=='preview':route.fulfill(json={'status':'ready','review_id':'review','suggested_name':'example_v9','report':report})
        else:route.fulfill(status=accept_status,json={'draft_id':'migrated','suggested_name':'example_v9'} if accept_status==200 else {'detail':'PB8 source changed; create a new migration preview'})
    page.route('**/api/backtest-v8/**',respond)
    page.route('**/api/backtest-v9/migrate-v8/**',migrate)
    page.route('**/api/backtest-v9/optimize-draft/migrated',lambda route:route.fulfill(json={'config':backtest.config,'migration_report':report}))
    page.goto('http://backtest9.test/api/backtest-v8/main_page#'+panel)
    return page


def test_pb8_conversion_cancel_and_adopt_to_native_draft(backtest):
    """Review is explicit, inert text; adoption/reload opens a native unsaved editor."""
    page=_open_pb8_conversion(backtest)
    button=page.locator('[data-migrate-pb9="config"]')
    button.click()
    expect(page.locator('#pbgui-dialog-title')).to_have_text('Review PB8 → PB9 migration')
    expect(page.locator('#pbgui-dialog-detail')).to_contain_text('Review inferred risk memory')
    assert page.locator('#pbgui-dialog-detail img').count()==0
    page.locator('#pbgui-dialog-cancel').click()
    assert [kind for kind,_ in backtest.migration_calls]==['preview']
    assert '/backtest-v8/' in page.url
    button.click();page.locator('#pbgui-dialog-accept').click()
    expect(page.locator('#cfg-name')).to_have_value('example_v9')
    expect(page.locator('#bt9-migration-report')).to_contain_text('Review inferred risk memory')
    assert [kind for kind,_ in backtest.migration_calls]==['preview','preview','accept']
    assert backtest.saved==backtest.queued==[]
    page.reload()
    expect(page.locator('#cfg-name')).to_have_value('example_v9')
    expect(page.locator('#bt9-migration-report')).to_contain_text('best_effort')
    assert page.locator('#bt9-migration-report img').count()==0
    assert backtest.errors==[]


def test_pb8_result_conversion_and_stale_source_error(backtest):
    """Result actions send the original result path and stay in context on conflict."""
    page=_open_pb8_conversion(backtest,panel='results',accept_status=409)
    page.locator('[data-migrate-pb9="result"]').click()
    page.locator('#pbgui-dialog-accept').click()
    expect(page.locator('#toast')).to_contain_text('PB8 source changed')
    assert backtest.migration_calls[0]==('preview',{'source_type':'result','source':'example/binance/run'})
    assert '/backtest-v8/' in page.url and '#results' in page.url
    assert backtest.saved==backtest.queued==[]
    assert backtest.errors==[]
