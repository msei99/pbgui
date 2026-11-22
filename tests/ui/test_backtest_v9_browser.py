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
        state = SimpleNamespace(page=page, config=config, saved=[], queued=[], run_drafts=[], balance_requests=[], errors=[], unexpected=[], metadata=metadata, hold_symbols=False, pending_symbols=[], queue_fail_once=False,
                                preflight_requests=[], hold_preflight=False, pending_preflight=[], lookups=[], lookup_stops=[], hold_lookup=False,
                                preload_supported=False,preload_status='running',preload_gets=0,
                                settings={'exchange_options':['binance','bybit'],'cpu':1,'cpu_max':4,'autostart':False}, settings_saved=[], archives=[], archive_results=[], archived=[], archive_queued=[], schedules=[], schedule_actions=[])
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
            elif path == '/api/v9/symbols':
                if state.hold_symbols:
                    state.pending_symbols.append(route)
                else:
                    route.fulfill(json={'symbols':['BTC','HYPE']})
            elif path == '/api/v9/tags':
                route.fulfill(json={'tags':['defi']})
            elif path == '/api/v9/coins/filter':
                route.fulfill(json={'approved':['HYPE'],'ignored':['BTC']})
            elif path == '/api/v9/draft':
                state.run_drafts.append(route.request.post_data_json)
                route.fulfill(json={'draft_id':'native-run-draft'})
            elif path == '/api/balance-calc/calculate':
                state.balance_requests.append(route.request.post_data_json)
                route.fulfill(json={'balance_long':[{'coin':'HYPE','balance':123}], 'balance_short':[]})
            elif path == '/api/backtest-v9/ohlcv-preflight':
                state.preflight_requests.append(route.request.post_data_json)
                payload={'runtime':'pb9','summary':{'overall_status':'legacy','headline':'Configured source ready','detail':'Local candles available','counts':{'legacy_importable':1},'preload_supported':state.preload_supported},
                         'request':{'source_dir':str(ROOT/'data/ohlcv')},'universe':{'coin_count':1},'exchanges':[],'notes':[]}
                if state.hold_preflight:
                    state.pending_preflight.append((route,payload))
                else:
                    route.fulfill(json=payload)
            elif path == '/api/backtest-v9/ohlcv-preload':
                # Synthetic preload capability exercises the shared PB7/PB8
                # controller; the real PB9 report always keeps this disabled.
                route.fulfill(json={'job_id':'preload-fixture','status':'running'})
            elif path == '/api/backtest-v9/ohlcv-preload/preload-fixture':
                state.preload_gets+=1
                route.fulfill(json={'job_id':'preload-fixture','status':state.preload_status})
            elif path == '/api/backtest-v9/ohlcv-start-dates':
                state.lookups.append(route.request.post_data_json)
                route.fulfill(json={'job_id':'0123456789ab','status':'queued','progress':{'percent':0}})
            elif path == '/api/backtest-v9/ohlcv-start-dates/0123456789ab':
                if route.request.method == 'DELETE':
                    state.lookup_stops.append(path)
                    route.fulfill(json={'job_id':'0123456789ab','status':'stopped'})
                elif state.hold_lookup:
                    route.fulfill(json={'job_id':'0123456789ab','status':'running','progress':{'percent':50,'message':'Resolving HYPE'}})
                else:
                    route.fulfill(json={'job_id':'0123456789ab','status':'completed','result':{'start_date_options':{
                        'earliest':{'available':True,'start_date':'2025-01-05'},'all_markets':{'available':True,'start_date':'2025-02-01'},'pair_count':1,'known_pair_count':1}}})
            elif '/archives' in path:
                if path.endswith('/archives'):
                    route.fulfill(json={'archives':state.archives,'my_archive':'own'})
                elif path.endswith('/archives/settings'):
                    route.fulfill(json={'my_archive':'own','has_access_token':False,'username':'','email':''})
                elif path.endswith('/results/rebacktest'):
                    state.archive_queued.append((path,route.request.post_data_json))
                    route.fulfill(json={'ok':True,'queued':1})
                elif path.endswith('/results/retest-replace'):
                    state.archive_queued.append((path,route.request.post_data_json))
                    route.fulfill(json={'ok':True,'queued':1})
                elif path.endswith('/add-config'):
                    state.archived.append(route.request.post_data_json)
                    route.fulfill(json={'ok':True})
                elif path.endswith('/results'):
                    route.fulfill(json={'results':state.archive_results})
                elif path.endswith('/optimize-configs'):
                    route.fulfill(json={'configs':[]})
                elif path.endswith('/retest-schedules'):
                    if route.request.method == 'POST':
                        payload=route.request.post_data_json
                        state.schedule_actions.append((path,payload))
                        state.schedules.append(dict(payload,id='schedule-fixture',enabled=True,backtest_versions=['v9'],targets=[{'relative_path':'native-result','backtest_version':'v9'}]))
                    route.fulfill(json={'schedules':state.schedules,'runs':[]})
                elif '/retest-schedules/' in path:
                    state.schedule_actions.append((path,route.request.method))
                    if path.endswith('/toggle'):
                        state.schedules[0]['enabled']=not state.schedules[0]['enabled']
                    elif route.request.method == 'DELETE':
                        state.schedules.clear()
                    route.fulfill(json={'ok':True,'queued':1})
                else:
                    route.fulfill(json={})
            elif path.endswith('/settings'):
                if route.request.method == 'POST':
                    payload=route.request.post_data_json
                    state.settings_saved.append(payload)
                    state.settings.update(payload)
                    route.fulfill(json={'ok':True})
                else:
                    route.fulfill(json=state.settings)
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
                if state.queue_fail_once:
                    state.queue_fail_once=False
                    route.fulfill(status=503,json={'detail':'Synthetic lost enqueue response'})
                    return
                route.fulfill(json={'ok':True,'filename':'11111111-1111-4111-8111-111111111111'})
            elif path.endswith('/queue'):
                route.fulfill(json={'queue':[],'items':[]})
            elif path.endswith('/results'):
                route.fulfill(json={'results':[],'pagination':{'has_more':False,'total':0}})
            elif '/api/backtest-v7/' in path or '/api/backtest-v8/' in path or '/api/v7/' in path or '/api/v8/' in path:
                state.unexpected.append(path); route.abort()
            elif path == '/api/help/index':
                route.fulfill(json=[{'file':'35_pbv7_backtest','title':'PB7 Backtest'},
                                    {'file':'42_pbv8_backtest','title':'PB8 Backtest'},
                                    {'file':'54_pbv9_backtest','title':'PB9 Backtest'}])
            elif path == '/api/help/content':
                title = 'PB9 Backtest' if parse_qs(url.query).get('file') == ['54_pbv9_backtest'] else 'Wrong version guide'
                route.fulfill(json={'content':'# '+title+'\nNative isolated test guide.'})
            else:
                route.fulfill(json={})

        page.route('**/*',respond)
        page.goto('http://backtest9.test/api/backtest-v9/main_page?opt_draft_id=fixture-draft&draft_name=fixture')
        yield state
        browser.close()


def test_native_exchange_options_include_unselected_supported_venues(backtest):
    """Backtest choices and source routing use capabilities rather than template selections."""
    page=backtest.page
    expect(page.locator('#cfg-name')).to_have_value('fixture')
    field=page.locator('#ms-cfg-exchanges-input');field.fill('hyperliquid')
    page.locator('#ms-cfg-exchanges-dd .ms-option[data-val="hyperliquid"]').click()
    assert 'hyperliquid' in page.evaluate("cfgGetMs('ms-cfg-exchanges')")
    assert 'okx' in page.locator('#kv-cs-exchange option').all_text_contents()
    assert not backtest.errors


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


def test_queue_retry_reuses_operation_id_and_double_click_is_single_flight(backtest):
    """An uncertain enqueue response may be retried without duplicating execution."""
    backtest.queue_fail_once=True
    page=backtest.page
    expect(page.locator('#cfg-name')).to_have_value('fixture')
    page.evaluate('saveAndQueue();saveAndQueue();')
    page.wait_for_function('document.body.textContent.includes("Synthetic lost enqueue response")')
    assert len(backtest.saved)==len(backtest.queued)==1
    page.locator('#sidebar-editor button[onclick="saveAndQueue()"]').click()
    page.wait_for_function('currentPanel === "queue"')
    assert len(backtest.queued)==2
    assert backtest.queued[0]['operation_id']==backtest.queued[1]['operation_id']
    assert backtest.errors==backtest.unexpected==[]


def test_additional_parameters_preserve_types_defaults_and_managed_path(backtest):
    """The existing PB8 fallback edits future PB9 fields without pinning defaults."""
    page=backtest.page
    future={'future_number':0.125,'future_bool':False,'future_text':'<svg onload="bad()">',
            'future_null':None,'future_list':[1,'two'],'future_object':{'nested':True}}
    backtest.config['backtest'].update(future)
    backtest.metadata['prototype']['backtest']['future_default']=42
    backtest.metadata['fields'].append({'path':['backtest','future_registered'],'types':['float'],'default':3.5,'nullable':False})
    page.reload()
    expect(page.locator('#cfg-name')).to_have_value('fixture')
    page.locator('#exp-extra-bt > .expander-header').click()
    for width in [700,1440]:
        page.set_viewport_size({'width':width,'height':1000})
        assert page.evaluate('document.documentElement.scrollWidth <= document.documentElement.clientWidth')
    expect(page.locator('#extra-bt-future_default')).to_have_value('42')
    expect(page.locator('#extra-bt-future_registered')).to_have_value('3.5')
    expect(page.locator('#extra-bt-future_text')).to_have_value(future['future_text'])
    assert page.locator('#exp-extra-bt svg').count() == 0
    assert page.locator('#managed-bt-base_dir').get_attribute('readonly') is not None
    page.locator('#cfg-name').fill('renamed')
    expect(page.locator('#managed-bt-base_dir')).to_have_value('data/bt_v9_results/renamed/{job-id}')
    page.locator('#extra-bt-future_number').fill('2.75')
    page.locator('#extra-bt-future_bool').check()
    page.locator('#extra-bt-future_null').fill('false')
    page.locator('#extra-bt-future_list').fill('[3,"new"]')
    page.locator('#sidebar-editor button[onclick="saveAndQueue()"]' ).click()
    page.wait_for_function('currentPanel === "queue"')
    saved=backtest.saved[-1]['config']['backtest']
    assert saved['future_number']==2.75 and saved['future_bool'] is True
    assert saved['future_null'] is False and saved['future_list']==[3,'new']
    assert saved['future_object']==future['future_object'] and saved['future_text']==future['future_text']
    assert 'future_default' not in saved and 'future_registered' not in saved
    assert saved==backtest.queued[-1]['config']['backtest']
    assert backtest.errors==[], '\n'.join(backtest.errors)


def test_additional_json_errors_block_save_and_future_bot_field_is_visible(backtest):
    """Invalid fallback JSON stays editable; newly discovered bot leaves are usable."""
    page=backtest.page
    backtest.config['backtest']['future_object']={'value':1}
    backtest.config['bot']['long']['future_policy']={'threshold':0.25}
    backtest.config['bot']['future_namespace']={'mode':'preview'}
    page.reload()
    expect(page.locator('#cfg-name')).to_have_value('fixture')
    page.locator('#bt9-field-section-advanced > .expander-header').click()
    expect(page.locator('[data-config-path="bot.long.future_policy"]')).to_be_visible()
    expect(page.locator('[data-config-path="bot.future_namespace"]')).to_be_visible()
    page.locator('#exp-extra-bt > .expander-header').click()
    page.locator('#extra-bt-future_object').fill('{')
    page.locator('#sidebar-editor button[onclick="saveAndQueue()"]' ).click()
    assert backtest.saved==backtest.queued==[]
    expect(page.locator('#extra-bt-future_object')).to_have_value('{')
    assert backtest.errors==[]


def test_source_only_bot_override_can_return_to_inheritance(backtest):
    """Discovery never invents an inherited default from the user's own override."""
    page=backtest.page
    backtest.config['bot']['long']['future_policy']={'threshold':0.25}
    page.reload()
    expect(page.locator('#cfg-name')).to_have_value('fixture')
    page.locator('#bt9-field-section-advanced > .expander-header').click()
    field=page.locator('[data-meta-path="bot.long.future_policy"]')
    field.locator('.meta-explicit').uncheck()
    expect(field.locator('[data-config-path]')).to_have_value('')
    page.locator('#sidebar-editor button[onclick="saveAndQueue()"]' ).click()
    page.wait_for_function('currentPanel === "queue"')
    assert 'future_policy' not in backtest.saved[-1]['config']['bot']['long']
    assert backtest.errors==[]


def test_raw_json_adds_and_removes_fallback_controls(backtest):
    """Future keys follow raw edits automatically and cannot survive their removal."""
    page=backtest.page
    expect(page.locator('#cfg-name')).to_have_value('fixture')
    page.locator('#exp-raw > .expander-header').click()
    raw=page.locator('#cfg-raw-json')
    config=json.loads(raw.input_value())
    config['backtest']['raw_future']=1.5
    raw.fill(json.dumps(config))
    expect(page.locator('#extra-bt-raw_future')).to_have_value('1.5')
    del config['backtest']['raw_future']
    raw.fill(json.dumps(config))
    expect(page.locator('#extra-bt-raw_future')).to_have_count(0)
    page.locator('#sidebar-editor button[onclick="saveAndQueue()"]' ).click()
    page.wait_for_function('currentPanel === "queue"')
    assert 'raw_future' not in backtest.saved[-1]['config']['backtest']
    assert backtest.errors==[], '\n'.join(backtest.errors)


@pytest.mark.parametrize('order', ['raw_first', 'form_first'])
@pytest.mark.parametrize('immediate_save', [False, True])
def test_raw_and_form_edits_merge_without_waiting(backtest, order, immediate_save):
    """Actual input events preserve both views even before their debounce timers."""
    page = backtest.page
    expect(page.locator('#cfg-name')).to_have_value('fixture')
    page.evaluate("""order => {
      const raw = document.getElementById('cfg-raw-json');
      const rawEdit = () => {
        const config = JSON.parse(raw.value);
        config.backtest.starting_balance = 98765;
        raw.value = JSON.stringify(config);
        raw.dispatchEvent(new Event('input', {bubbles:true}));
      };
      const formEdit = () => {
        const input = document.getElementById('cfg-start-date');
        input.value = '2025-03-01';
        input.dispatchEvent(new Event('input', {bubbles:true}));
      };
      if (order === 'raw_first') { rawEdit(); formEdit(); }
      else { formEdit(); rawEdit(); }
    }""", order)
    if not immediate_save:
        page.wait_for_function("JSON.parse(document.getElementById('cfg-raw-json').value).backtest.start_date === '2025-03-01' && document.getElementById('cfg-starting-balance').value === '98765'")
        # A later ordinary edit must not replay the original form balance.
        page.locator('#cfg-start-date').fill('2025-03-02')
    page.locator('#sidebar-editor button[onclick="saveEditor()"]' ).click()
    page.wait_for_function("document.getElementById('cfg-starting-balance').value === '98765'")
    assert backtest.saved[-1]['config']['backtest']['starting_balance'] == 98765
    assert backtest.saved[-1]['config']['backtest']['start_date'] == ('2025-03-01' if immediate_save else '2025-03-02')
    assert backtest.errors == []


@pytest.mark.parametrize('raw_last', [False, True])
def test_same_field_edits_follow_input_order(backtest, raw_last):
    """Timers cannot reverse the user's last edit of the same primary field."""
    page = backtest.page
    expect(page.locator('#cfg-name')).to_have_value('fixture')
    page.evaluate("""rawLast => {
      const raw=document.getElementById('cfg-raw-json'), input=document.getElementById('cfg-start-date');
      function rawEdit() {const config=JSON.parse(raw.value);config.backtest.start_date='2025-02-01';raw.value=JSON.stringify(config);raw.dispatchEvent(new Event('input',{bubbles:true}));}
      function formEdit() {input.value='2025-03-01';input.dispatchEvent(new Event('input',{bubbles:true}));}
      if (rawLast) {formEdit();rawEdit();} else {rawEdit();formEdit();}
    }""", raw_last)
    page.locator('#sidebar-editor button[onclick="saveEditor()"]' ).click()
    assert backtest.saved[-1]['config']['backtest']['start_date'] == ('2025-02-01' if raw_last else '2025-03-01')


def test_invalid_raw_preserves_pending_form_edit(backtest):
    """An incomplete raw document blocks Save, retaining valid form changes for repair."""
    page = backtest.page
    expect(page.locator('#cfg-name')).to_have_value('fixture')
    page.evaluate("""() => {
      window.reviewRaw=document.getElementById('cfg-raw-json').value;
      const raw=document.getElementById('cfg-raw-json');raw.value='{';raw.dispatchEvent(new Event('input',{bubbles:true}));
      const input=document.getElementById('cfg-start-date');input.value='2025-03-01';input.dispatchEvent(new Event('input',{bubbles:true}));
    }""")
    page.locator('#sidebar-editor button[onclick="saveEditor()"]' ).click()
    assert backtest.saved == []
    page.evaluate("""() => {
      const raw=document.getElementById('cfg-raw-json');const config=JSON.parse(reviewRaw);config.backtest.starting_balance=98765;
      raw.value=JSON.stringify(config);raw.dispatchEvent(new Event('input',{bubbles:true}));
    }""")
    page.locator('#sidebar-editor button[onclick="saveEditor()"]' ).click()
    assert backtest.saved[-1]['config']['backtest']['start_date'] == '2025-03-01'
    assert backtest.saved[-1]['config']['backtest']['starting_balance'] == 98765
    assert backtest.errors == []


def test_raw_sync_preserves_exact_text_and_caret(backtest):
    """A valid intermediate numeric spelling remains editable after form synchronization."""
    page = backtest.page
    expect(page.locator('#cfg-name')).to_have_value('fixture')
    page.locator('#exp-raw > .expander-header').click()
    before = page.evaluate("""() => {
      const raw=document.getElementById('cfg-raw-json'), config=JSON.parse(raw.value);
      config.backtest.starting_balance=1234;
      raw.value=JSON.stringify(config,null,2).replace('"starting_balance": 1234','"starting_balance": 1234.0');
      raw.focus();const position=raw.value.indexOf('1234.0')+6;raw.setSelectionRange(position,position);
      raw.dispatchEvent(new Event('input',{bubbles:true}));return {text:raw.value,position};
    }""")
    page.wait_for_function("document.getElementById('cfg-starting-balance').value === '1234'")
    after = page.evaluate("const raw=document.getElementById('cfg-raw-json');({text:raw.value,position:raw.selectionStart,focused:document.activeElement===raw})")
    assert after == {**before, 'focused': True}
    assert backtest.errors == []


def test_save_blocks_when_sync_owner_is_not_initialized(backtest):
    """An unavailable editor snapshot cannot silently fall through to a stale save."""
    page = backtest.page
    expect(page.locator('#cfg-name')).to_have_value('fixture')
    page.evaluate("window._btJsonSync=PBGuiEditorShared.createJsonSyncController({trackChanges:true,validateRaw:validateRawJson})")
    page.locator('#sidebar-editor button[onclick="saveEditor()"]' ).click()
    assert backtest.saved == []


def test_future_market_setting_survives_changes_to_known_rows(backtest):
    """A newly discovered root field is not overwritten by the shared row editor."""
    page=backtest.page
    backtest.config['backtest']['market_settings']={'future_policy':1.25,'overrides':{'HYPE':{'min_cost':5}}}
    page.reload()
    expect(page.locator('#cfg-name')).to_have_value('fixture')
    page.locator('#bt9-field-section-advanced > .expander-header').click()
    page.locator('[data-config-path="backtest.market_settings.future_policy"]').fill('2.5')
    page.locator('#exp-market-settings > .expander-header').click()
    page.locator('#exp-market-settings input[oninput*="min_cost"]').fill('6')
    page.locator('#sidebar-editor button[onclick="saveAndQueue()"]' ).click()
    page.wait_for_function('currentPanel === "queue"')
    saved=backtest.saved[-1]['config']['backtest']['market_settings']
    assert saved['future_policy']==2.5
    assert saved['overrides']['HYPE']['min_cost']==6
    assert backtest.errors==[]


def test_native_editor_run_handoff_keeps_edits_and_gui_metadata(backtest):
    """Run receives current native form values and filters through its own draft API."""
    page=backtest.page
    backtest.config['pbgui']={'note':'keep note','tags':['defi'],'market_cap':100}
    page.goto('http://backtest9.test/api/backtest-v9/main_page?config=fixture#configs')
    expect(page.locator('#cfg-starting-balance')).to_have_value(str(backtest.config['backtest']['starting_balance']))
    page.locator('#cfg-starting-balance').fill('3500')
    page.locator('#sb-btn-add-to-run').click()
    page.wait_for_url('**/api/v9/edit_page?new=1&draft_id=native-run-draft')
    sent=backtest.run_drafts[-1]['config']
    assert sent['backtest']['starting_balance']==3500
    assert sent['pbgui']=={'note':'keep note','tags':['defi'],'market_cap':100,'runtime':'pb9','enabled_on':'disabled'}
    assert backtest.saved==backtest.queued==[]
    assert backtest.unexpected==[]


def test_unsupported_editor_tools_do_not_forward_to_legacy_runtimes(backtest):
    """Only connected native capabilities are actionable in the editor sidebar."""
    page=backtest.page
    expect(page.locator('#cfg-name')).to_have_value('fixture')
    for action in ['goStrategyExplorer()']:
        expect(page.locator('#sidebar-editor button[onclick="'+action+'"]')).to_be_hidden()
    for action in ['goBalanceCalc()','goCalcBalance()']:
        expect(page.locator('#sidebar-editor button[onclick="'+action+'"]')).to_be_visible()
    expect(page.locator('#sb-btn-ohlcv-preflight')).to_be_visible()
    assert backtest.unexpected==[]


def test_native_ohlcv_readiness_updates_and_restores_without_manual_refresh(backtest):
    """Readiness follows current native edits and retains the open panel on reload."""
    page=backtest.page
    expect(page.locator('#cfg-name')).to_have_value('fixture')
    page.locator('#sb-btn-ohlcv-preflight').click()
    expect(page.locator('#ohlcv-panel-body')).to_contain_text('Configured source ready')
    assert 'legacy importable' not in page.locator('#ohlcv-panel-body').inner_text()
    assert len(backtest.preflight_requests)==1
    assert backtest.preflight_requests[0]['config']['config_version']=='v9.0.0'
    expect(page.locator('#ohlcv-panel-body [data-action="refresh"]')).to_have_count(0)
    expect(page.locator('#ohlcv-panel-body [data-action="preload"]')).to_be_disabled()
    page.locator('#cfg-starting-balance').fill('3500')
    page.wait_for_function('!document.getElementById("ohlcv-panel-body").textContent.includes("Updating OHLCV")')
    assert backtest.preflight_requests[-1]['config']['backtest']['starting_balance']==3500
    page.reload()
    expect(page.locator('#cfg-name')).to_have_value('fixture')
    expect(page.locator('#ohlcv-panel')).to_be_visible()
    expect(page.locator('#ohlcv-panel-body')).to_contain_text('Configured source ready')
    assert backtest.saved==backtest.queued==[]
    assert backtest.errors==backtest.unexpected==[]


def test_readiness_ignores_response_from_older_native_inputs(backtest):
    """A late reply cannot label newer editor values as already checked."""
    page=backtest.page
    expect(page.locator('#cfg-name')).to_have_value('fixture')
    backtest.hold_preflight=True
    page.locator('#sb-btn-ohlcv-preflight').click()
    page.wait_for_function('document.getElementById("ohlcv-panel-body").textContent.includes("Checking OHLCV")')
    page.locator('#cfg-starting-balance').fill('4800')
    assert len(backtest.pending_preflight)==1
    route,payload=backtest.pending_preflight.pop()
    payload['summary']['headline']='OBSOLETE RESPONSE'
    backtest.hold_preflight=False;route.fulfill(json=payload)
    expect(page.locator('#ohlcv-panel-body')).to_contain_text('Configured source ready')
    assert 'OBSOLETE RESPONSE' not in page.locator('#ohlcv-panel-body').inner_text()
    assert backtest.preflight_requests[-1]['config']['backtest']['starting_balance']==4800


def test_native_first_and_all_use_current_config_and_keep_sparse_sides(backtest):
    """The existing date controls call PB9, never a PB8 planning endpoint."""
    page=backtest.page
    expect(page.locator('#cfg-name')).to_have_value('fixture')
    page.locator('[aria-label="Set earliest OHLCV start date"]').click()
    expect(page.locator('#cfg-start-date')).to_have_value('2025-01-05')
    page.locator('[aria-label="Set common OHLCV start date"]').click()
    expect(page.locator('#cfg-start-date')).to_have_value('2025-02-01')
    assert len(backtest.lookups)==2
    assert backtest.lookups[0]['config']['bot']['long']=={'enabled':True}
    assert backtest.lookups[0]['config']['config_version']=='v9.0.0'
    for width in (700,1440):
        page.set_viewport_size({'width':width,'height':1000})
        assert page.evaluate('document.documentElement.scrollWidth <= document.documentElement.clientWidth')
    assert backtest.saved==backtest.queued==[]
    assert backtest.errors==backtest.unexpected==[]


def test_native_date_lookup_stop_and_changed_input_guard(backtest):
    """Stop and stale-result protection are the same shared workflow as PB8."""
    page=backtest.page
    expect(page.locator('#cfg-name')).to_have_value('fixture')
    backtest.hold_lookup=True
    page.locator('[aria-label="Set earliest OHLCV start date"]').click()
    expect(page.locator('#cfg-ohlcv-start-progress')).to_be_visible()
    original=page.locator('#cfg-start-date').input_value()
    page.locator('#cfg-starting-balance').fill('5200')
    backtest.hold_lookup=False
    expect(page.locator('#cfg-ohlcv-start-status')).to_contain_text('inputs changed')
    assert page.locator('#cfg-start-date').input_value()==original
    backtest.hold_lookup=True
    page.locator('[aria-label="Set earliest OHLCV start date"]').click()
    expect(page.locator('#cfg-ohlcv-start-progress')).to_be_visible()
    page.locator('#cfg-ohlcv-start-stop').click()
    expect(page.locator('#cfg-ohlcv-start-status')).to_contain_text('Lookup stopped')
    assert len(backtest.lookup_stops)==1
    assert backtest.errors==backtest.unexpected==[]


@pytest.mark.parametrize('terminal',['completed','stopped'])
def test_shared_preload_terminal_status_rechecks_once_and_stops_polling(backtest,terminal):
    """Existing preload-capable runtimes need no Refresh and no terminal poll loop."""
    page=backtest.page
    expect(page.locator('#cfg-name')).to_have_value('fixture')
    backtest.preload_supported=True
    page.locator('#sb-btn-ohlcv-preflight').click()
    expect(page.locator('#ohlcv-panel-body')).to_contain_text('Configured source ready')
    page.locator('#ohlcv-panel-body [data-action="preload"]').click()
    backtest.preload_status=terminal
    expect(page.locator('#ohlcv-panel-body')).to_contain_text('Completed' if terminal=='completed' else 'Stopped')
    page.wait_for_timeout(1800)
    assert len(backtest.preflight_requests)==2
    assert backtest.preload_gets==2
    assert backtest.errors==backtest.unexpected==[]


def test_native_balance_tool_reuses_shared_calculator_without_saving(backtest):
    """Native side inheritance is projected only into calculator analysis inputs."""
    backtest.config['bot']['global']['risk']={'max_positions':3,'total_wallet_exposure_limit_ratio':2}
    backtest.config['bot']['global']['universe']['approved_coins']=['HYPE']
    page=backtest.page;page.reload()
    expect(page.locator('#cfg-name')).to_have_value('fixture')
    page.locator('#sidebar-editor button[onclick="goCalcBalance()"]').click()
    expect(page.locator('#bcalc-res')).to_contain_text('123.00')
    payload=backtest.balance_requests[-1]
    assert payload['config']['bot']['long']['risk']=={'n_positions':3,'total_wallet_exposure_limit':2}
    assert payload['config']['live']['approved_coins']['long']==['HYPE']
    assert payload['config']['bot']['short']['risk']['n_positions']==0
    assert backtest.saved==backtest.queued==[]
    assert backtest.errors==backtest.unexpected==[]


def test_nested_registered_fields_have_one_author_and_preserve_siblings(backtest):
    """The additional root JSON cannot overwrite an edited registry child."""
    backtest.config['backtest']['future_object']={'threshold':2,'other':4}
    backtest.metadata['fields'].append({'path':['backtest','future_object','threshold'],'types':['float'],'default':1})
    page=backtest.page;page.reload()
    expect(page.locator('#cfg-name')).to_have_value('fixture')
    expect(page.locator('#extra-bt-future_object')).to_have_count(0)
    page.locator('#bt9-field-section-advanced > .expander-header').click()
    page.locator('[data-config-path="backtest.future_object.threshold"]').fill('3.5')
    page.locator('#sidebar-editor button[onclick="saveEditor()"]').click()
    page.wait_for_function('window.editingConfig === "fixture"')
    assert backtest.saved[-1]['config']['backtest']['future_object']=={'threshold':3.5,'other':4}


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


@pytest.mark.parametrize('entry', ['draft', 'saved'])
def test_native_registry_maps_and_compact_override_controls(backtest, entry):
    """Real registry-shaped maps stay editable without literal wildcard fields."""
    state = backtest
    state.metadata['fields'].extend([
        {'path':['backtest','balance_sample_divider'], 'types':['integer']},
        {'path':['backtest','compress_cache'], 'types':['boolean']},
        {'path':['backtest','scenarios'], 'types':['array']},
        {'path':['backtest','coin_sources'], 'types':['object']},
        {'path':['backtest','reducer','*'], 'types':['string'],
         'domain':{'choices':['mean','max']}},
        {'path':['backtest','coin_sources','*'], 'types':['string']},
        {'path':['backtest','scenarios','*','coin_sources','*'], 'types':['string']},
        {'path':['backtest','scenarios','*','start_date'], 'types':['string']},
    ])
    state.config['backtest']['coin_sources'] = {'HYPE':'bybit'}
    state.config['backtest']['reducer']['custom_metric'] = 'mean'
    state.config['backtest']['scenarios'] = [{'label':'sample','start_date':'2025-01-01'}]
    page = state.page
    if entry == 'saved':
        page.goto('http://backtest9.test/api/backtest-v9/main_page?config=fixture#configs')
    else:
        page.reload()
    expect(page.locator('#cfg-name')).to_have_value('fixture')
    assert page.locator('[data-meta-path*="*"]').count() == 0
    expect(page.locator('#kv-cs')).to_contain_text('HYPE')
    page.locator('#exp-suite > .expander-header').click()
    page.locator('#suite-enabled').check()
    page.locator('#exp-suite-agg > .expander-header').click()
    reducer = page.locator('#suite-agg-default')
    expect(reducer).to_have_value('mean')
    for toggle in page.locator('#bt9-native-fields .meta-explicit').all():
        if toggle.is_visible():
            box = toggle.bounding_box()
            assert box['width'] == box['height'] == 14
    reducer.select_option('max')
    boolean = page.locator('[data-config-path="backtest.compress_cache"]').bounding_box()
    assert boolean['width'] == boolean['height'] == 16
    page.locator('[data-config-path="backtest.compress_cache"]').uncheck()
    page.locator('#sidebar-editor button[onclick="saveAndQueue()"]').click()
    page.wait_for_function('currentPanel === "queue"')
    saved = state.saved[0]['config']['backtest']
    assert saved['reducer'] == {'default':'max','custom_metric':'mean'}
    assert saved['coin_sources'] == state.config['backtest']['coin_sources']
    assert saved['scenarios'] == state.config['backtest']['scenarios']
    assert saved['compress_cache'] is False
    assert saved == state.queued[0]['config']['backtest']
    assert '*' not in json.dumps(saved)
    assert state.errors == [], '\n'.join(state.errors)
    assert state.unexpected == []


def test_pb9_layout_primary_fields_coins_and_sparse_full_json(backtest):
    """The PB8-shaped editor shares controls while preserving native inheritance."""
    page = backtest.page
    expect(page.locator('#cfg-name')).to_have_value('fixture')
    for selector in ['#cfg-starting-balance','#cfg-maker-fee','#cfg-compress',
                     '#ms-cfg-app-long','#ms-cfg-app-short','#cfg-twe-long',
                     '#cfg-npos-short','#cfg-bot-long','#cfg-bot-short']:
        expect(page.locator(selector)).to_be_visible()
    assert not page.locator('#bt9-field-section-advanced').get_attribute('class').endswith('open')
    assert page.locator('#bt9-balance-fields .meta-explicit').count() == 0
    assert page.locator('#bt9-long-fields .num-stepper').count() == 2
    before=json.loads(page.locator('#cfg-bot-long').input_value())
    before['risk']['max_positions']=6
    page.locator('#cfg-bot-long').fill(json.dumps(before))
    page.locator('#cfg-bot-long').dispatch_event('change')
    expect(page.locator('#cfg-npos-long')).to_have_value('6')
    # Applying the shared filter picker writes native side-owned universes.
    page.locator('#sidebar-editor button[onclick="cfgApplyFilters()"]' ).click()
    expect(page.locator('#ms-cfg-app-long')).to_contain_text('HYPE')
    page.locator('#sidebar-editor button[onclick="saveAndQueue()"]' ).click()
    page.wait_for_function('currentPanel === "queue"')
    saved=backtest.saved[0]['config']
    assert saved['bot']['long']['risk'] == {'max_positions':6}
    assert saved['bot']['long']['universe']['approved_coins'] == ['HYPE']
    assert saved['bot']['short']['universe']['ignored_coins'] == ['BTC']
    assert 'approved_coins' not in saved.get('live',{})
    assert 'strategy' not in saved['bot']['long']
    assert saved == backtest.queued[0]['config']
    assert backtest.errors == [], '\n'.join(backtest.errors)


def test_pb9_editor_sections_and_json_view_survive_reload(backtest):
    """Editor navigation is restored independently from authored config values."""
    page=backtest.page
    expect(page.locator('#cfg-bot-long-view')).to_have_value('effective')
    page.locator('#bt9-field-section-advanced > .expander-header').click()
    page.locator('#cfg-bot-long-view').select_option('overrides')
    page.reload()
    expect(page.locator('#cfg-bot-long-view')).to_have_value('overrides')
    expect(page.locator('#bt9-field-section-advanced')).to_have_class('expander open')
    expect(page.locator('#cfg-bot-long')).to_have_value(json.dumps({'enabled':True},indent=2))
    assert 'opt_draft_id=fixture-draft' in page.url
    assert backtest.saved == backtest.queued == []
    assert backtest.errors == []


def test_catalog_response_preserves_new_coin_selection(backtest):
    """A late shared-catalog response cannot resurrect a removed approved coin."""
    import time
    page=backtest.page
    expect(page.locator('#cfg-name')).to_have_value('fixture')
    page.wait_for_function('_marketSettingsCoins.includes("BTC")')
    backtest.config['bot']['long']['universe']={'approved_coins':['HYPE']}
    backtest.hold_symbols=True
    page.reload()
    expect(page.locator('#ms-cfg-app-long')).to_contain_text('HYPE')
    deadline=time.monotonic()+5
    while not backtest.pending_symbols:
        assert time.monotonic()<deadline
        page.wait_for_timeout(20)
    page.locator('#ms-cfg-app-long .ms-x').click()
    backtest.hold_symbols=False
    for route in backtest.pending_symbols:
        route.fulfill(json={'symbols':['BTC','HYPE']})
    page.wait_for_function('_marketSettingsCoins.includes("BTC")')
    page.locator('#sidebar-editor button[onclick="saveEditor()"]' ).click()
    page.wait_for_function('editingConfig !== "__new__"')
    assert backtest.saved[-1]['config']['bot']['long']['universe']['approved_coins'] == []
    assert backtest.errors == []


def test_shared_filter_numbers_do_not_render_metadata_as_html(backtest):
    """Imported GUI filter metadata is escaped before the shared form is built."""
    page=backtest.page
    backtest.config['pbgui']={'market_cap':'"><img src=x onerror="window.layoutInjection=1">'}
    page.reload()
    expect(page.locator('#cfg-market-cap')).to_be_visible()
    assert page.locator('#configs-editor img').count() == 0
    assert page.evaluate('window.layoutInjection || 0') == 0
    assert backtest.errors == []


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


def test_native_source_control_reuses_pbgui_path_and_preserves_explicit_opt_out(backtest):
    """PBGui Data, a custom source and clearing the path round-trip through the real editor."""
    page=backtest.page
    page.route('**/api/backtest-v9/pbgui_data_path',lambda route:route.fulfill(json={'path':'/shared/pbgui/ohlcv'}))
    expect(page.locator('#cfg-ohlcv-dir')).to_be_visible()
    expect(page.locator('#cfg-candle-interval')).to_be_visible()
    expect(page.locator('#cfg-gap-tol')).to_be_visible()
    page.locator('button[onclick="fillPbguiDataPath()"]').click()
    expect(page.locator('#cfg-ohlcv-dir')).to_have_value('/shared/pbgui/ohlcv')
    page.locator('#sidebar-editor button[onclick="saveEditor()"]').click()
    page.wait_for_function('editingConfig !== "__new__"')
    assert backtest.saved[-1]['config']['backtest']['ohlcv_source_dir']=='/shared/pbgui/ohlcv'
    page.locator('#cfg-ohlcv-dir').fill('')
    with page.expect_response(lambda response: '/configs/fixture' in response.url and response.request.method == 'PUT'):
        page.locator('#sidebar-editor button[onclick="saveEditor()"]').click()
    assert backtest.saved[-1]['config']['backtest']['ohlcv_source_dir'] is None
    assert backtest.errors==[]


def test_combined_native_result_shows_individual_venues_and_close_price(backtest):
    """The actual chart uses native price routes and keeps the overlay when switching venue."""
    page=backtest.page;requested=[]
    def respond(route):
        """Return synthetic result data; real production code renders Plotly and selector."""
        url=urlparse(route.request.url);path=url.path
        if path.endswith('/results'):
            route.fulfill(json={'results':[{'path':'native/combined/result','config_name':'native',
                'result_name':'result','backtest_version':'v9','exchange_dir':'bybit, hyperliquid',
                'exchanges':['bybit','hyperliquid'],'coins':['HYPE'],'strategy':'ema_anchor',
                'start_date':'2025-07-01','end_date':'2025-07-02'}],
                'pagination':{'has_more':False,'total':1}})
        elif path.endswith('/equity'):
            route.fulfill(content_type='text/csv',body='timestamp,usd_total_balance,usd_total_equity\n2025-07-01T00:00:00Z,1000,1000\n2025-07-02T00:00:00Z,1001,1002\n')
        elif path.endswith('/price'):
            query=parse_qs(url.query);exchange=query['exchange'][0];requested.append(exchange)
            assert exchange in ['bybit','hyperliquid']
            route.fulfill(json={'available':True,'exchange':exchange,'coin':'HYPE',
                'time':['2025-07-01T00:00:00Z','2025-07-02T00:00:00Z'],'close':[40,42],
                'coverage_start':'2025-07-01T00:00:00Z','coverage_end':'2025-07-02T00:00:00Z','coverage_complete':True})
        elif path.endswith('/fills'):
            route.fulfill(content_type='text/csv',body='timestamp,symbol,pnl,fee_paid,balance\n')
        else:route.fulfill(json={})
    page.route('**/api/backtest-v9/results**',respond)
    page.locator('#sidebar-editor button[onclick="closeEditor()"]').click()
    page.locator('#sidebar [data-panel="results"]').click()
    page.locator('.icon-btn[data-action="view"]').click()
    expect(page.locator('#price-market-0 option')).to_have_count(2)
    expect(page.locator('#price-status-0')).to_contain_text('full chart coverage')
    page.wait_for_function('document.getElementById("be-chart-0").data.some(trace=>trace.name==="bybit / HYPE close")')
    page.locator('#price-market-0').select_option('hyperliquid|HYPE')
    page.wait_for_function('document.getElementById("be-chart-0").data.some(trace=>trace.name==="hyperliquid / HYPE close")')
    page.reload()
    expect(page.locator('#price-market-0')).to_have_value('hyperliquid|HYPE')
    expect(page.locator('#price-status-0')).to_contain_text('full chart coverage')
    assert set(requested)=={'bybit','hyperliquid'}
    assert backtest.unexpected==[] and backtest.errors==[]


def _comparison_fixture(backtest, *, linked=True):
    """Supply version-owned summaries/CSV to the unchanged production Compare renderer."""
    page=backtest.page;calls=[]
    older={'path':'/fixture/pb8/combined/source','backtest_version':'v8','config_name':'original',
           'result_name':'source','exchange_dir':'combined','exchanges':['bybit'],'coins':['HYPE'],
           'start_date':'2025-01-01','end_date':'2025-01-02'}
    newer={'path':'/fixture/pb9/combined/converted','backtest_version':'v9','config_name':'converted',
           'result_name':'converted','exchange_dir':'bybit','exchanges':['bybit'],'coins':['HYPE'],
           'start_date':'2025-01-01','end_date':'2025-01-02'}
    if linked:newer['comparison_reference']={'version':'v8','path':older['path']}
    def respond(route):
        """Only explicit read endpoints are allowed; no backtest or persistence action runs."""
        url=urlparse(route.request.url);path=url.path;calls.append(path)
        if path.endswith('/results/compare-source'):route.fulfill(json={'results':[older,newer]})
        elif path.endswith('/results'):
            version=path.split('/')[2].removeprefix('backtest-')
            route.fulfill(json={'results':{'v7':[],'v8':[older],'v9':[newer]}[version],
                                'pagination':{'has_more':False,'total':0 if version=='v7' else 1}})
        elif path.endswith('/results/equity'):
            native='/backtest-v9/' in path
            csv=('timestamp,usd_total_balance,usd_total_equity\n2025-01-01T00:00:00Z,1000,1000\n2025-01-02T00:00:00Z,1200,1300\n' if native else
                 'minute,balance,equity\n0,1000,1000\n1440,1100,1150\n')
            route.fulfill(content_type='text/csv',body=csv)
        else:route.fulfill(json={})
    for version in ['v7','v8','v9']:page.route('**/api/backtest-'+version+'/results**',respond)
    expect(page.locator('#cfg-name')).to_have_value('fixture')
    page.locator('#sidebar-editor button[onclick="closeEditor()"]').click()
    page.locator('#sidebar [data-panel="results"]').click()
    return page,calls,older,newer


def test_source_reference_opens_existing_compare_and_survives_reload(backtest):
    """One row action uses the existing four-trace chart and each version's own CSV adapter."""
    page,calls,older,newer=_comparison_fixture(backtest)
    page.locator('#results-filter').fill('converted')
    page.locator('#results-list tr[data-path="'+newer['path']+'"]').click()
    page.locator('button[title="Compare with PB8 source"]').click()
    expect(page.locator('#compare-chart-div .legendtext')).to_have_count(4)
    expect(page.locator('#results-list tr.selected')).to_have_count(2)
    assert '/api/backtest-v8/results/equity' in calls and '/api/backtest-v9/results/equity' in calls
    assert page.evaluate('Date.parse(document.getElementById("compare-chart-div").data[0].x[0])')==page.evaluate('Date.parse(document.getElementById("compare-chart-div").data[2].x[0])')
    assert 'compare_source=' in page.url
    page.reload()
    expect(page.locator('#compare-chart-div .legendtext')).to_have_count(4)
    expect(page.locator('#results-list tr.selected')).to_have_count(2)
    page.locator('#sidebar button[onclick="compareSelected()"]').click()
    expect(page.locator('#compare-chart-area')).not_to_be_visible()
    page.reload()
    expect(page.locator('#results-list tr.selected')).to_have_count(2)
    assert page.locator('#compare-chart-div').count()==0
    assert backtest.errors==[] and backtest.unexpected==[]


def test_manual_compare_includes_existing_unlinked_pb8_and_pb9_results(backtest):
    """Older PB9 runs remain comparable through the ordinary version selector and row selection."""
    page,calls,older,newer=_comparison_fixture(backtest,linked=False)
    page.locator('#results-version-filter').select_option('both')
    expect(page.locator('#results-list tr[data-path]')).to_have_count(2)
    page.locator('#results-list tr[data-path="'+older['path']+'"]').click()
    page.locator('#results-list tr[data-path="'+newer['path']+'"]').click()
    page.locator('#results-list th').nth(0).click()
    expect(page.locator('#results-list tr.selected')).to_have_count(2)
    page.locator('#sidebar button[onclick="compareSelected()"]').click()
    expect(page.locator('#compare-chart-div .legendtext')).to_have_count(4)
    assert '/api/backtest-v7/results' in calls
    page.reload()
    expect(page.locator('#results-version-filter')).to_have_value('both')
    expect(page.locator('#compare-chart-div .legendtext')).to_have_count(4)
    assert backtest.errors==[] and backtest.unexpected==[]


def test_queue_settings_reuses_pb8_dialog_and_preserves_dirty_edits(backtest):
    """PB9 exposes CPU/autostart, saves only native controls and restores the Queue view."""
    page = backtest.page
    expect(page.locator('#cfg-name')).to_have_value('fixture')
    page.locator('#sidebar-editor button[onclick="closeEditor()"]').click()
    page.locator('#sidebar [data-panel="queue"]').click()
    page.locator('button[onclick="openSettingsModal()"]').click()
    expect(page.locator('#set-cpu-val')).to_have_value('1')
    expect(page.locator('#set-autostart')).not_to_be_checked()
    expect(page.locator('#legacy-queue-settings')).to_be_hidden()
    page.locator('button[onclick="settingsAdjustCpu(1)"]').click()
    page.locator('#set-autostart').check()
    page.evaluate("settings.cpu=4; settings.autostart=false; syncOpenSettingsModal()")
    expect(page.locator('#set-cpu-val')).to_have_value('2')
    expect(page.locator('#set-autostart')).to_be_checked()
    page.locator('.modal-btn-primary').filter(has_text='Save').click()
    expect(page.locator('#set-cpu-val')).to_be_hidden()
    assert backtest.settings_saved == [{'cpu': 2, 'autostart': True}]
    page.reload()
    expect(page.locator('#sidebar button[data-panel="queue"]')).to_have_class(__import__('re').compile('active'))
    page.locator('button[onclick="openSettingsModal()"]').click()
    expect(page.locator('#set-cpu-val')).to_have_value('2')
    expect(page.locator('#set-autostart')).to_be_checked()
    assert backtest.errors == backtest.unexpected == []


def test_native_archive_reuses_shared_view_charts_and_version_owned_requeue(backtest):
    """Native archive rows reuse charts, retain navigation and requeue through PB9 only."""
    page = backtest.page
    path = '/archive-fixture/own/pbgui/configs/v9.0.0/backtests/native-archived/bybit/result'
    backtest.archives = [{'name':'own','is_own':True,'results':1}]
    backtest.archive_results = [{'path':path,'backtest_version':'v9','config_version':'v9.0.0',
        'config_name':'native-archived','result_name':'result','coins':['HYPE'],'coins_text':'HYPE',
        'strategy':'ema_anchor','exchange_dir':'bybit','exchanges':['bybit'],
        'start_date':'2025-01-01','end_date':'2025-01-02','gain':1.3,'final_balance':1200}]
    reads = []
    def results(route):
        """Only the native result API may serve this archived row's numeric artifacts."""
        url=urlparse(route.request.url);reads.append(url.path)
        if url.path.endswith('/config'):
            cfg=copy.deepcopy(backtest.config)
            cfg['backtest'].update(start_date='2025-01-01',end_date='2025-01-02',exchanges=['bybit'])
            route.fulfill(json=cfg)
        elif url.path.endswith('/equity'):
            route.fulfill(content_type='text/csv',body='timestamp,usd_total_balance,usd_total_equity\n2025-01-01T00:00:00Z,1000,1000\n2025-01-02T00:00:00Z,1200,1300\n')
        elif url.path.endswith('/price'):
            route.fulfill(json={'available':False,'reason':'No fixture candles'})
        elif url.path.endswith('/fills'):
            route.fulfill(content_type='text/csv',body='timestamp,symbol,pnl,fee_paid,balance\n')
        else:
            route.fulfill(json={})
    page.route('**/api/backtest-v9/results/**',results)
    expect(page.locator('#cfg-name')).to_have_value('fixture')
    page.locator('#sidebar-editor button[onclick="closeEditor()"]').click()
    page.locator('#sidebar [data-panel="archive"]').click()
    page.locator('#archive-list-container tbody tr').dblclick()
    expect(page.locator('#archive-results-table')).to_contain_text('native-archived')
    expect(page.locator('#arc-tab-optimize')).to_be_hidden()
    expect(page.locator('#arc-tab-schedules')).to_be_visible()
    expect(page.locator('#archive-btn-retest-replace')).to_be_visible()
    page.locator('#archive-results-table .icon-btn[data-action="view"]').click()
    page.wait_for_function('document.querySelector("#archive-charts .js-plotly-plot") !== null')
    assert '/api/backtest-v9/results/equity' in reads
    page.reload()
    expect(page.locator('#archive-results-table')).to_contain_text('native-archived')
    expect(page.locator('#sidebar [data-panel="archive"]')).to_have_class(__import__('re').compile('active'))
    page.locator('#archive-results-table tbody tr').first.click()
    page.locator('#archive-btn-rebacktest').click()
    expect(page.locator('#rbt-start')).to_have_value('2025-01-01')
    page.locator('#modal-btn-0').click()
    page.wait_for_function('document.getElementById("modal-root").classList.contains("open") === false')
    assert len(backtest.archive_queued) == 1
    assert backtest.archive_queued[0][0] == '/api/backtest-v9/archives/own/results/rebacktest'
    assert backtest.archive_queued[0][1]['paths'] == [path]
    assert backtest.errors == backtest.unexpected == []


def test_native_archive_retest_schedule_uses_shared_controls_and_owned_execution(backtest):
    """Create/run/pause/delete and reload use the PB8 controls with native execution routing."""
    page=backtest.page
    path='/archive-fixture/own/pbgui/configs/v9.0.0/backtests/native-archived/bybit/result'
    backtest.archives=[{'name':'own','is_own':True,'results':1}]
    backtest.archive_results=[{'path':path,'backtest_version':'v9','config_name':'native-archived','exchange_dir':'bybit','exchanges':['bybit'],'coins':['HYPE']}]
    page.route('**/api/backtest-v9/results/config?*',lambda route:route.fulfill(json=backtest.config))
    expect(page.locator('#cfg-name')).to_have_value('fixture')
    page.locator('#sidebar-editor button[onclick="closeEditor()"]').click()
    page.locator('#sidebar [data-panel="archive"]').click()
    page.locator('#archive-list-container tbody tr').dblclick()
    page.locator('#archive-results-table tbody tr').first.click()
    page.locator('#archive-btn-retest-replace').click()
    expect(page.locator('#arr-cadence')).to_have_value('daily')
    page.locator('#arr-cadence').select_option('weekly')
    expect(page.locator('#arr-weekday-wrap')).to_be_visible()
    page.locator('#arr-weekday').select_option('2')
    page.locator('#arr-time').fill('04:10')
    page.locator('#modal-btn-1').click()
    expect(page.locator('#archive-results-table')).to_contain_text('Weekly Wed 04:10')
    assert backtest.schedule_actions[0][0]=='/api/backtest-v7/archives/own/retest-schedules'
    assert backtest.schedule_actions[0][1]['paths']==[path]
    page.reload()
    expect(page.locator('#archive-results-table')).to_contain_text('Weekly Wed 04:10')
    assert page.evaluate('location.hash').endswith(':schedules')
    page.locator('.archive-sched-run').click()
    page.wait_for_function('document.querySelector(".archive-sched-run") !== null')
    page.locator('.archive-sched-toggle').click()
    expect(page.locator('#archive-results-table')).to_contain_text('Disabled')
    assert any(action[0]=='/api/backtest-v9/archives/own/retest-schedules/schedule-fixture/run' for action in backtest.schedule_actions)
    page.locator('.archive-sched-delete').click()
    page.locator('#modal-btn-1').click()
    expect(page.locator('#archive-results-table')).to_contain_text('No retest schedules')
    page.locator('#arc-tab-backtests').click()
    page.locator('#arc-btn-deselect').click()
    page.locator('#archive-results-table tbody tr').first.click()
    page.locator('#archive-btn-retest-replace').click()
    page.locator('#modal-btn-0').click()
    page.wait_for_function('document.getElementById("modal-root").classList.contains("open") === false')
    assert backtest.archive_queued[-1][0]=='/api/backtest-v9/archives/own/results/retest-replace'
    assert backtest.errors==backtest.unexpected==[]


def test_native_result_adds_to_existing_archive_endpoint(backtest):
    """Add to Archive sends a native generation selector through the shared archive action."""
    page = backtest.page
    result={'path':'native/local/result','backtest_version':'v9','config_name':'native-local','result_name':'result'}
    page.route('**/api/backtest-v9/results?*',lambda route:route.fulfill(json={'results':[result],'pagination':{'total':1,'has_more':False}}))
    page.route('**/api/backtest-v9/results',lambda route:route.fulfill(json={'results':[result],'pagination':{'total':1,'has_more':False}}))
    expect(page.locator('#cfg-name')).to_have_value('fixture')
    page.locator('#sidebar-editor button[onclick="closeEditor()"]').click()
    page.locator('#sidebar [data-panel="results"]').click()
    expect(page.locator('#results-list')).to_contain_text('native-local')
    page.locator('#results-list tbody tr').click()
    page.locator('#btn-result-add-archive').click()
    expect(page.locator('#btn-result-add-archive')).to_be_enabled()
    assert backtest.archived == [{'results':[{'source_path':'native/local/result','backtest_version':'v9'}]}]
    assert backtest.errors == backtest.unexpected == []


def test_mixed_archive_requeue_reports_partial_success_and_skips_completed_retry(backtest):
    """A retry of a partial cross-version action cannot enqueue its successful group twice."""
    page=backtest.page
    backtest.archives=[{'name':'own','is_own':True,'results':2}]
    backtest.archive_results=[{'path':'/archive/native','backtest_version':'v9','config_name':'native','result_name':'result'},
                              {'path':'/archive/legacy','backtest_version':'v8','config_name':'legacy','result_name':'result'}]
    calls={'native':[],'legacy':[]}
    def native_queue(route):
        """Fail the first native request while the legacy group succeeds."""
        calls['native'].append(route.request.post_data_json)
        if len(calls['native'])==1:
            route.fulfill(status=503,json={'detail':'fixture native busy'})
        else:
            route.fulfill(json={'ok':True,'queued':1})
    def legacy_queue(route):
        """Record a successful legacy group, which must never be resent by retry."""
        calls['legacy'].append(route.request.post_data_json)
        route.fulfill(json={'ok':True,'queued':1})
    page.route('**/api/backtest-v9/archives/own/results/rebacktest',native_queue)
    page.route('**/api/backtest-v7/archives/own/results/rebacktest',legacy_queue)
    page.route('**/results/config?*',lambda route:route.fulfill(json=backtest.config))
    expect(page.locator('#cfg-name')).to_have_value('fixture')
    page.locator('#sidebar-editor button[onclick="closeEditor()"]').click()
    page.locator('#sidebar [data-panel="archive"]').click()
    page.locator('#archive-list-container tbody tr').dblclick()
    expect(page.locator('#archive-results-table tbody tr')).to_have_count(2)
    page.locator('#arc-btn-select-all').click()
    page.locator('#archive-btn-rebacktest').click()
    page.locator('#modal-btn-0').click()
    page.wait_for_function('!!_archiveRequeueAttempt && !!_archiveRequeueAttempt.completed.legacy')
    page.locator('#archive-btn-rebacktest').click()
    page.locator('#modal-btn-0').click()
    page.wait_for_function('_archiveRequeueAttempt === null')
    assert len(calls['native'])==2 and len(calls['legacy'])==1
    assert calls['native'][0]['operation_id']==calls['native'][1]['operation_id']
    assert backtest.errors==backtest.unexpected==[]


def test_archive_reload_restores_search_coin_config_and_selected_row(backtest):
    """The real shared archive keeps its filters and selection when data returns after reload."""
    page=backtest.page
    backtest.archives=[{'name':'own','is_own':True,'results':2}]
    backtest.archive_results=[{'path':'/archive/hype','backtest_version':'v9','config_name':'hype-native',
        'result_name':'run','coins':['HYPE'],'coins_text':'HYPE'},
        {'path':'/archive/btc','backtest_version':'v8','config_name':'btc-old','result_name':'run','coins':['BTC'],'coins_text':'BTC'}]
    expect(page.locator('#cfg-name')).to_have_value('fixture')
    page.locator('#sidebar-editor button[onclick="closeEditor()"]').click()
    page.locator('#sidebar [data-panel="archive"]').click()
    page.locator('#archive-list-container tbody tr').dblclick()
    expect(page.locator('#archive-results-table tbody tr')).to_have_count(2)
    page.locator('#arc-results-filter').fill('hype')
    page.locator('#arc-results-config-filter').select_option('hype-native')
    page.locator('#arc-results-coin-filter').select_option('HYPE')
    page.locator('#archive-results-table tbody tr').click()
    page.reload()
    expect(page.locator('#arc-results-filter')).to_have_value('hype')
    expect(page.locator('#arc-results-config-filter')).to_have_value('hype-native')
    expect(page.locator('#arc-results-coin-filter')).to_have_value('HYPE')
    expect(page.locator('#archive-results-table tbody tr.selected')).to_have_count(1)
    page.locator('#arc-btn-deselect').click()
    page.reload()
    expect(page.locator('#archive-results-table tbody tr.selected')).to_have_count(0)
    expect(page.locator('#arc-results-filter')).to_have_value('hype')
    assert backtest.errors==backtest.unexpected==[]


def test_native_result_opens_shared_native_optimize_draft(backtest):
    """Use the real result action and draft contract without a legacy optimizer hop."""
    page, _, _, result = _comparison_fixture(backtest, linked=False)
    submitted=[]
    page.route('**/api/backtest-v9/results/config?*',lambda route:route.fulfill(json=backtest.config))
    def draft(route):
        """Capture exactly the authored native config sent through the existing API."""
        submitted.append(route.request.post_data_json)
        route.fulfill(json={'draft_id':'native-optimize-draft'})
    page.route('**/api/backtest-v9/optimize-draft',draft)
    page.route('**/api/optimize-v9/main_page?*',lambda route:route.fulfill(body='<html><body>Native Optimize draft</body></html>',content_type='text/html'))
    page.locator('#results-version-filter').select_option('both')
    expect(page.locator('#results-list tr[data-path]')).to_have_count(2)
    page.locator('#results-list tr[data-path="'+result['path']+'"]').click()
    page.locator('button[onclick="optimizeFromResult()"]').click()
    page.wait_for_url('**/api/optimize-v9/main_page?*')
    assert parse_qs(urlparse(page.url).query)['opt_draft_id']==['native-optimize-draft']
    assert submitted==[{'config':backtest.config}]
    assert backtest.unexpected==[]


@pytest.mark.parametrize('action', ['run', 'backtest', 'balance', 'calc', 'archive'])
def test_native_result_sidebar_actions_work_under_all_filter(backtest, action):
    """Click real selected rows and sidebar buttons; no mocked selection bypasses gating."""
    page, calls, _, result = _comparison_fixture(backtest, linked=False)
    requests = []
    def config_read(route):
        """Record the selected generation and source, retaining native authored fields."""
        requests.append(route.request.url)
        route.fulfill(json=backtest.config)
    page.route('**/api/backtest-v9/results/config?*', config_read)
    page.route('**/api/v9/edit_page?*', lambda route: route.fulfill(content_type='text/html', body='<p>Run draft</p>'))
    balance_drafts = []
    def balance_draft(route):
        """Only the shared balance projection leaves the native result boundary."""
        balance_drafts.append(route.request.post_data_json)
        route.fulfill(json={'draft_id': 'balance-fixture'})
    page.route('**/api/balance-calc/draft', balance_draft)
    page.route('**/api/balance-calc/main_page?*', lambda route: route.fulfill(content_type='text/html', body='<p>Balance draft</p>'))
    page.locator('#results-version-filter').select_option('both')
    expect(page.locator('#results-list tr[data-path]')).to_have_count(2)
    page.locator('#results-list tr[data-path="'+result['path']+'"]').click()
    actions = {'run': 'addToRun()', 'backtest': 'rebacktestSelected()',
               'balance': 'goBalanceCalculatorFromResult()', 'calc': 'calcBalanceFromResult()',
               'archive': 'addResultToArchive()'}
    page.locator('#ctx-results button[onclick="'+actions[action]+'"]').click()
    if action == 'run':
        page.wait_for_url('**/api/v9/edit_page?*')
        assert backtest.run_drafts[0]['config']['pbgui']['runtime'] == 'pb9'
        assert backtest.run_drafts[0]['config']['pbgui']['enabled_on'] == 'disabled'
        assert backtest.run_drafts[0]['config']['config_version'] == 'v9.0.0'
    elif action == 'backtest':
        expect(page.locator('#cfg-starting-balance')).to_have_value(str(backtest.config['backtest']['starting_balance']))
        assert page.evaluate('editingConfig') == '__new__'
    elif action == 'balance':
        page.wait_for_url('**/api/balance-calc/main_page?*')
        assert len(balance_drafts) == 1
    elif action == 'calc':
        expect(page.locator('#bcalc-res')).to_contain_text('123.00')
        assert len(backtest.balance_requests) == 1
    else:
        page.wait_for_function("document.getElementById('btn-result-add-archive').getAttribute('aria-busy') === null")
        assert backtest.archived == [{'results': [{'source_path': result['path'], 'backtest_version': 'v9'}]}]
    if action != 'archive':
        assert len(requests) == 1 and parse_qs(urlparse(requests[0]).query)['path'] == [result['path']]
    assert backtest.saved == backtest.queued == []
    assert backtest.errors == backtest.unexpected == []


def test_result_action_gating_tracks_selection_sort_reload_and_mixed_versions(backtest):
    """All view uses row ownership, restores selection, and keeps unsupported tools hidden."""
    page, _, foreign, native = _comparison_fixture(backtest, linked=False)
    page.locator('#results-version-filter').select_option('both')
    expect(page.locator('#results-list tr[data-path]')).to_have_count(2)
    button = page.locator('#ctx-results button[onclick="addToRun()"]')
    expect(button).to_be_disabled()
    page.locator('#results-list tr[data-path="'+native['path']+'"]').click()
    expect(button).to_be_enabled()
    page.locator('#results-list th').nth(0).click()
    expect(button).to_be_enabled()
    page.reload()
    expect(page.locator('#results-version-filter')).to_have_value('both')
    expect(button).to_be_enabled()
    page.locator('#results-list tr[data-path="'+foreign['path']+'"]').click()
    expect(button).to_be_disabled()
    expect(page.locator('#ctx-results button[onclick="compareSelected()"]')).to_be_enabled()
    page.locator('#results-list tr[data-path="'+native['path']+'"]').click()
    expect(button).to_be_disabled()
    page.get_by_role('button', name='Deselect', exact=True).click()
    expect(button).to_be_disabled()
    page.get_by_role('button', name='Select All', exact=True).click()
    expect(button).to_be_disabled()
    page.locator('#results-version-filter').select_option('v9')
    expect(page.locator('#results-list tr[data-path]')).to_have_count(1)
    expect(button).to_be_disabled()
    page.locator('#results-list tr[data-path="'+native['path']+'"]').click()
    expect(button).to_be_enabled()
    for action in ('strategyExplorerFromResult()', 'optimizePresetFromResult()'):
        hidden = page.locator('#ctx-results button[onclick="'+action+'"]')
        expect(hidden).to_be_hidden()
        expect(hidden).to_be_disabled()
    assert backtest.errors == backtest.unexpected == []


@pytest.mark.parametrize('action', ['analysis', 'config', 'plot', 'fills'])
def test_native_result_row_artifact_buttons_use_native_endpoints(backtest, action):
    """Real row icons render native JSON and image artifacts through their owning API."""
    import base64
    page, _, _, result = _comparison_fixture(backtest, linked=False)
    reads = []
    def artifact(route):
        """Supply only isolated artifacts, recording every requested native endpoint."""
        path = urlparse(route.request.url).path
        reads.append(path)
        if path.endswith('/analysis'):
            route.fulfill(json={'sharpe_ratio_strategy_equity': 2.5})
        elif path.endswith('/config'):
            route.fulfill(json=backtest.config)
        elif path.endswith('/files'):
            route.fulfill(json={'files': ['balance_and_equity.png', 'fills_plots/HYPE.png']})
        else:
            route.fulfill(content_type='image/png', body=base64.b64decode(
                'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+a3ioAAAAASUVORK5CYII='))
    for endpoint in ('analysis', 'config', 'files', 'image'):
        page.route('**/api/backtest-v9/results/'+endpoint+'?*', artifact)
    page.locator('#results-list tr[data-path="'+result['path']+'"] button[data-action="'+action+'"]').click()
    if action == 'analysis':
        expect(page.locator('#results-charts')).to_contain_text('sharpe_ratio_strategy_equity')
        assert '/api/backtest-v9/results/analysis' in reads
    elif action == 'config':
        expect(page.locator('#results-charts')).to_contain_text('v9.0.0')
        assert '/api/backtest-v9/results/config' in reads
    else:
        expect(page.locator('#results-charts img')).to_have_count(2 if action == 'plot' else 1)
        page.wait_for_function("Array.from(document.querySelectorAll('#results-charts img')).every(image => image.complete && image.naturalWidth > 0)")
        assert '/api/backtest-v9/results/files' in reads
        assert '/api/backtest-v9/results/image' in reads
    assert backtest.errors == backtest.unexpected == []


def test_native_result_delete_uses_owning_endpoint_and_updates_list(backtest):
    """Delete only an intercepted synthetic result and refresh the current All view automatically."""
    page, _, _, result = _comparison_fixture(backtest, linked=False)
    deleted = []
    def native_results(route):
        """Keep deletion entirely inside this browser fixture."""
        if route.request.method == 'DELETE':
            deleted.append(parse_qs(urlparse(route.request.url).query)['path'][0])
            route.fulfill(json={'ok': True})
        else:
            route.fulfill(json={'results': [] if deleted else [result], 'pagination': {'has_more': False, 'total': 0 if deleted else 1}})
    page.route('**/api/backtest-v9/results', native_results)
    page.route('**/api/backtest-v9/results?*', native_results)
    page.locator('#results-version-filter').select_option('both')
    expect(page.locator('#results-list tr[data-path]')).to_have_count(2)
    page.locator('#results-list tr[data-path="'+result['path']+'"]').click()
    page.locator('#ctx-results button[onclick="deleteSelectedResults()"]').click()
    page.locator('#modal-root').get_by_role('button', name='Delete', exact=True).click()
    expect(page.locator('#results-list tr[data-path]')).to_have_count(1)
    assert deleted == [result['path']]
    expect(page.locator('#results-version-filter')).to_have_value('both')
    expect(page.locator('#ctx-results button[onclick="addToRun()"]')).to_be_disabled()
    assert backtest.errors == backtest.unexpected == []


def test_all_results_load_in_bounded_pages_without_resetting_native_selection(backtest):
    """An unfinished PB8 page cannot block a PB9 selection, chart or Run handoff."""
    page, _, old, native = _comparison_fixture(backtest, linked=False)
    page.wait_for_function('!_resultsReadController')
    page.evaluate("""() => {
      const fetch = window.fetch.bind(window);
      window.__lateResultRequests = [];
      window.fetch = function(url, options) {
        if(String(url).includes('/api/backtest-v8/results?') && String(url).includes('offset=100')) {
          window.__lateResultRequests.push({signal:options.signal,generation:_resultsLoadGeneration});
        }
        return fetch(url, options);
      };
    }""")
    rows = [dict(old, path=old['path']+'-'+str(i), config_name='older-'+str(i)) for i in range(120)]
    pending, requested = [], []
    def older_page(route):
        """Hold the second PB8 page while the real UI remains usable."""
        query = parse_qs(urlparse(route.request.url).query)
        offset, limit = int(query.get('offset', ['0'])[0]), int(query.get('limit', ['0'])[0])
        requested.append((offset, limit, query.get('snapshot', [''])[0]))
        assert limit == 100
        if offset:
            pending.append(route)
        else:
            route.fulfill(json={'results': rows[:100], 'pagination': {
                'total': 120, 'has_more': True, 'next_offset': 100, 'snapshot': 'a'*32}})
    page.route('**/api/backtest-v8/results?*', older_page)
    page.route('**/api/backtest-v9/results/analysis?*', lambda route: route.fulfill(json={'sharpe_ratio_strategy_equity': 2.5}))
    page.locator('#results-version-filter').select_option('both')
    expect(page.locator('#results-list tr[data-path]')).to_have_count(101)
    row = page.locator('#results-list tr[data-path="'+native['path']+'"]')
    row.click()
    row.locator('button[data-action="analysis"]').click()
    expect(page.locator('#results-charts')).to_contain_text('sharpe_ratio_strategy_equity')
    expect(page.locator('#ctx-results button[onclick="addToRun()"]')).to_be_enabled()
    page.locator('#results-filter').fill('converted')
    expect(page.locator('#results-list tr[data-path]')).to_have_count(1)
    page.evaluate("window.__retainedResultPanel=document.getElementById('results-charts').firstElementChild")
    assert 1 <= len(pending) <= 2, requested
    signals = page.evaluate('window.__lateResultRequests.map(item=>({aborted:item.signal.aborted,generation:item.generation}))')
    assert len(signals) == len(pending)
    assert all(item['aborted'] for item in signals[:-1]), signals
    pending.pop().fulfill(json={'results': rows[100:], 'pagination': {
        'total': 120, 'has_more': False, 'next_offset': 120, 'snapshot': 'a'*32}})
    expect(page.locator('#results-count-label')).to_contain_text('121')
    assert page.evaluate("document.getElementById('results-charts').firstElementChild===window.__retainedResultPanel"), 'Paging must preserve the existing chart/JSON panel DOM'
    expect(page.locator('#results-filter')).to_have_value('converted')
    expect(page.locator('#results-list tr.selected')).to_have_count(1)
    assert row.locator('button[data-action="analysis"]').evaluate("element=>element.classList.contains('active')")
    expect(page.locator('#results-charts')).to_contain_text('sharpe_ratio_strategy_equity')
    expect(page.locator('#ctx-results button[onclick="addToRun()"]')).to_be_enabled()
    assert requested[:2] == [(0, 100, 'new'), (100, 100, 'a'*32)]
    # A focus/visibility update may start one fresh load; every page stays bounded
    # and uses its discovery snapshot rather than switching to an unbounded read.
    assert len(requested) <= 4
    assert all(limit == 100 and (offset == 0 or snapshot == 'a'*32)
               for offset, limit, snapshot in requested)
    # A cleared panel must be rebuilt even if its saved source/action signature
    # still matches; preservation applies only while the existing panel exists.
    page.evaluate("document.getElementById('results-charts').replaceChildren();window.PBGuiBacktestV9.restoreResults()")
    expect(page.locator('#results-charts')).to_contain_text('sharpe_ratio_strategy_equity')
    assert backtest.errors == backtest.unexpected == []


@pytest.mark.parametrize('is_new',[False,True])
def test_unsaved_backtest_reload_preserves_name_values_and_sections(backtest,is_new):
    """Refresh restores the same native editor and its latest unsaved form values."""
    page=backtest.page
    expect(page.locator('#cfg-name')).to_have_value('fixture')
    if is_new:
        page.evaluate('closeEditor()')
        page.get_by_role('button',name='+ New Config',exact=True).click()
        expect(page.locator('#cfg-name')).to_have_value('')
    page.locator('#cfg-name').fill('Unsaved native edits')
    page.locator('#cfg-starting-balance').fill('12345')
    page.evaluate("if(!document.getElementById('exp-extra-bt').classList.contains('open'))toggleExpander('exp-extra-bt')")
    page.reload()
    expect(page.locator('#cfg-name')).to_have_value('Unsaved native edits')
    expect(page.locator('#cfg-starting-balance')).to_have_value('12345')
    assert page.locator('#exp-extra-bt').evaluate("el=>el.classList.contains('open')")
    assert backtest.saved==[] and backtest.queued==[] and backtest.unexpected==[]


def test_backtest_recovery_never_stores_credential_bearing_raw_config(backtest):
    """Custom credential fields are rejected by the same guard used by Optimize."""
    page=backtest.page
    expect(page.locator('#cfg-name')).to_have_value('fixture')
    result=page.evaluate("""() => {
      var raw=document.getElementById('cfg-raw-json');
      var config=JSON.parse(raw.value);config.api_key='synthetic-secret';
      raw.value=JSON.stringify(config);configEditRevision++;
      captureBacktestDraftForRefresh();
      return sessionStorage.getItem(backtestDraftStorageKey());
    }""")
    assert result is None and backtest.saved==[]
