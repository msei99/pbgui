"""Native PB9 Optimize uses the production shared page with isolated browser APIs."""
import copy
import hashlib
import json
import mimetypes
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest
from browser_driver import sync_browser

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def native_page(tmp_path):
    """Serve only public assets and fixture metadata, never production configs/jobs."""
    metadata = json.loads((ROOT/'tests/fixtures/pb9_optimize_metadata.json').read_text())
    from pb9_parameter_help import populate_pb9_field_help
    docs = tmp_path/'docs'
    docs.mkdir()
    (docs/'configuration.md').write_text('## Shared Bot Policy\n\n- **bot.portfolio.hedge_mode**: Original hedge policy.\n'
        '## Bot Settings\n\nLocal HSL settings inherit from `bot.global.hsl`, with optional directional pins.\n')
    populate_pb9_field_help(tmp_path, metadata['fields'])
    config = copy.deepcopy(metadata['template'])
    config['bot']['global']['universe']['approved_coins'] = ['HYPE']
    config['backtest'].update(starting_balance=1000, start_date='2025-01-01', end_date='2025-02-01')
    config['pbgui'] = {'custom_fixture': {'preserve': True}}
    template = (ROOT/'frontend/v7_optimize.html').read_text()
    for key,value in {'API_BASE':'/api/optimize-v9','BASE_PREFIX':'','WS_BASE':'ws://native.test/api/optimize-v9',
        'LIMITS_META':'{}','VERSION':'test','SERIAL':'test','OPTIMIZE_VERSION':'v9',
        'OPTIMIZE_LABEL':'V9','BACKTEST_VERSION':'v9','OPTIMIZE_NAV_TITLE':'PBv9 OPTIMIZE',
        'OPTIMIZE_NAV_CURRENT':'v9_optimize','NAV_HASH':'test'}.items():
        template=template.replace('%%'+key+'%%',value)
    errors, calls, saved = [], [], []
    def serve(route):
        """Fulfil every request without outbound network or runtime-data access."""
        path=urlsplit(route.request.url).path
        calls.append(path)
        if path=='/api/optimize-v9/main_page':
            route.fulfill(content_type='text/html',body=template)
        elif path.startswith('/app/'):
            asset=(ROOT/'frontend'/path.removeprefix('/app/')).resolve()
            assert asset.is_relative_to(ROOT/'frontend')
            route.fulfill(status=200 if asset.is_file() else 404,body=asset.read_bytes() if asset.is_file() else b'',
                content_type=mimetypes.guess_type(asset.name)[0] or 'application/octet-stream')
        elif path.endswith('/metadata'):
            route.fulfill(json=metadata)
        elif path=='/api/optimize-v9/settings':
            route.fulfill(json={'exchange_options':metadata['exchange_options']['optimize'],
                'cpu':1,'cpu_max':4,'host_cpu_count':4,'autostart':False,'cpu_override':True,
                'use_pbgui_market_data':True})
        elif path.endswith('/configs/new-config'):
            route.fulfill(json={'config':config})
        elif path == '/api/optimize-v9/preparation-manifests':
            body = route.request.post_data_json
            digest = hashlib.sha256(body['text'].encode()).hexdigest()
            route.fulfill(json={'manifest':{'name':body['name'],'sha256':digest,'artifact':digest}})
        elif path.endswith('/coins/status'):
            route.fulfill(json={'symbols':['BTC','HYPE'],'catalog':[
                {'config_id':coin,'coin':coin,'display':coin,
                 'resolutions':[{'exchange':'bybit','symbol':coin+'/USDT:USDT'}]}
                for coin in ('BTC','HYPE')],'statuses':{}})
        elif path.endswith('/configs/parity') and route.request.method=='PUT':
            saved.append(route.request.post_data_json)
            route.fulfill(json={'ok':True})
        elif path=='/api/vast-v9/fragment':
            route.fulfill(content_type='text/html',body=(ROOT/'frontend/vast.html').read_text())
        elif path=='/api/vast-v9/jobs':
            route.fulfill(json={'jobs':[],'workers':[],'worker':None,'queue':{},
                'profile':{'runtime':'pb9','published':False,'reason':'Native PB9 worker image has not been published.'}})
        else:
            route.fulfill(json={})
    with sync_browser() as runner:
        browser=runner.chromium.launch(headless=True)
        try:
            page=browser.new_page(viewport={'width':1440,'height':1000})
            page.set_default_timeout(10000)
            page.on('pageerror',lambda error: errors.append(str(error)))
            page.route('**/*',serve)
            page.route_web_socket('**/*',lambda socket:socket.on_message(lambda message:None))
            page.goto('http://native.test/api/optimize-v9/main_page')
            page.wait_for_function('!state.initialLoadPending')
            yield page,config,errors,calls,saved
        finally:
            browser.close()


def test_native_editor_roundtrip_and_latest_edits(native_page):
    """Viewing native inherited policy does not create retired keys or pin defaults."""
    page,source,errors,calls,saved=native_page
    page.locator('#btn-new-config').click()
    page.wait_for_function("document.getElementById('opted-starting-balance')?.value==='1000'")
    initial=page.evaluate('collectEditorConfig().config')
    # PBGui may manage output paths and preferences, never backfill every inherited bot field.
    assert initial['config_version']==source['config_version']
    assert initial['bot']==source['bot']
    assert initial['optimize']==source['optimize'], _differences(source['optimize'],initial['optimize'])
    assert 'strategy_kind' not in initial['live']
    assert 'approved_coins' not in initial['live']
    page.locator('#opted-starting-balance').fill('2345')
    page.locator('#opted-starting-balance').press('Tab')
    assert page.evaluate('collectEditorConfig().config.backtest.starting_balance')==2345
    assert page.evaluate("JSON.parse(document.getElementById('opted-raw-json').value).backtest.starting_balance")==2345
    assert not page.evaluate('document.documentElement.scrollWidth>innerWidth')
    assert not errors


def test_native_template_saves_without_suite_and_preserves_null_scenarios(native_page):
    """Save the official non-Suite scoring defaults through the real button path."""
    page,source,errors,calls,saved=native_page
    assert source['backtest']['suite_enabled'] is False
    assert any('scenario' in item and item['scenario'] is None for item in source['optimize']['scoring'])
    page.locator('#btn-new-config').click()
    page.wait_for_function("document.getElementById('opted-starting-balance')?.value==='1000'")
    page.locator('#opted-name').fill('parity')
    page.locator('#btn-cloud-save').click()
    page.wait_for_function("document.getElementById('btn-cloud-save')?.disabled===false")
    assert len(saved)==1, page.locator('#opted-ohlcv-start-status').text_content()
    assert saved[0]['config']['optimize']['scoring']==source['optimize']['scoring']
    assert saved[0]['config']['optimize']['limits']==source['optimize']['limits']
    assert saved[0]['config']['backtest'].get('suite_enabled',False) is False
    assert not errors


def test_native_launch_controls_import_and_save_without_raw_json(native_page):
    """All new CLI options are editable on the shared page, with exact report bytes."""
    page, source, errors, calls, saved = native_page
    source['backtest'].update(suite_enabled=True, scenarios=[{'label':'train','start_date':'2025-01-01','end_date':'2025-01-15'},
        {'label':'holdout','start_date':'2025-01-16','end_date':'2025-02-01'}])
    page.locator('#btn-new-config').click()
    page.wait_for_function("document.getElementById('opted-starting-balance')?.value==='1000'")
    page.locator('#opted-name').fill('parity')
    page.locator('[data-run-scenario="train"]').check()
    page.locator('#opt9-filter-starting-configs').check()
    page.locator('#opt9-compress-starting-configs').fill('8')
    report = json.dumps({'schema_version':1, 'kind':'prepared-inputs', 'consumer':'optimize',
        'suite':{}, 'datasets':[{'manifest_present':True,'manifest_sha256':'a'*64}], 'cases':[{}]})
    page.locator('#opt9-preparation-manifest').set_input_files({'name':'prepared.json','mimeType':'application/json','buffer':report.encode()})
    page.wait_for_function("collectEditorConfig().config.pbgui.optimize_runtime.preparation_manifest?.artifact")
    options = page.evaluate('collectEditorConfig().config.pbgui.optimize_runtime')
    assert options['scenarios'] == ['train']
    assert options['filter_starting_configs'] is True
    assert options['compress_starting_configs'] == 8
    digest = hashlib.sha256(report.encode()).hexdigest()
    assert options['preparation_manifest'] == {'name':'prepared.json', 'sha256':digest, 'artifact':digest}
    assert report not in json.dumps(page.evaluate('collectEditorConfig().config'))
    # A report is never a path into the server filesystem, and removal is explicit.
    page.locator('#opt9-remove-preparation-manifest').click()
    assert page.evaluate('collectEditorConfig().config.pbgui.optimize_runtime.preparation_manifest') is None
    assert not page.evaluate('document.documentElement.scrollWidth>innerWidth')
    assert not errors


@pytest.mark.parametrize('entries,limits,objective,message',[
    ([{'scenario':None}],[],None,None),
    ([{'scenario':None,'reducer':'median'}],[{'scenario':None}],None,None),
    ([{'reducer':'mean'}],[],None,None),
    ([{'scenario':'bull'}],[],None,'Scoring Scenario'),
    ([],[{'scenario':'bull'}],None,'Limit Scenario'),
    ([],[],'bull','Objective Scenario'),
    ([{'scenario':''}],[],None,'cannot be empty'),
])
def test_native_non_suite_scenario_validation(native_page,entries,limits,objective,message):
    """Null/default bases are valid; named scenarios still require Suite mode."""
    page,*_=native_page
    result=page.evaluate("""(args)=>{try {
        validatePb8ScenarioBases(args.entries,args.limits,args.objective,false,[]);
        return null;
    } catch(error) { return error.message; }}""",dict(entries=entries,limits=limits,objective=objective))
    if message is None:assert result is None
    else:assert message in result


@pytest.mark.parametrize('mode,edit_during_lookup',[
    ('earliest',False),('all_markets',False),('earliest',True),('all_markets',True)])
def test_unsaved_hype_lookup_uses_latest_form_without_false_stale_result(native_page,mode,edit_during_lookup):
    """Lookup a named unsaved draft after editing exchanges and clearing Short coins."""
    page,source,errors,calls,saved=native_page
    submitted=[]
    release={'result':not edit_during_lookup}
    def lookup(route):
        """Return a deterministic helper result while using the production UI lifecycle."""
        if route.request.method=='POST':
            submitted.append(route.request.post_data_json['config'])
            route.fulfill(json={'job_id':'draft-lookup','status':'queued'})
        elif not release['result']:
            route.fulfill(json={'job_id':'draft-lookup','status':'running'})
        else:
            option={'available':True,'start_date':'2025-01-16'}
            route.fulfill(json={'job_id':'draft-lookup','status':'completed','result':{
                'start_date_options':{'earliest':option,'all_markets':option,'known_pair_count':2,'pair_count':2}}})
    page.route('**/api/optimize-v9/ohlcv-start-dates**',lookup)
    page.locator('#btn-new-config').click()
    page.wait_for_function("document.getElementById('opted-starting-balance')?.value==='1000'")
    page.locator('#opted-name').fill('HYPE_PB9')
    page.locator('#ms-opt-exchanges .ms-x[data-val="binance"]').click()
    page.locator('#ms-opt-exchanges-input').fill('hyperliquid')
    page.locator('#ms-opt-exchanges-dd .ms-option[data-val="hyperliquid"]').click()
    page.locator('#ms-opt-app-short .ms-x[data-val="HYPE"]').click()
    page.locator('#opted-ohlcv-source-dir').fill('/fixture/ohlcv')
    page.locator('#opted-ohlcv-source-dir').press('Tab')
    name='Set earliest OHLCV start date' if mode=='earliest' else 'Set common OHLCV start date'
    page.get_by_role('button',name=name,exact=True).click()
    if edit_during_lookup:
        page.wait_for_function("!!_optOhlcvStartDateRun?.jobId")
        page.locator('#opted-starting-balance').fill('2345')
        page.locator('#opted-starting-balance').press('Tab')
        release['result']=True
    page.wait_for_function('_optOhlcvStartDateRun===null')
    assert page.locator('#opted-start-date').input_value()==('2025-01-01' if edit_during_lookup else '2025-01-16'), {
        'status':page.locator('#opted-ohlcv-start-status').text_content(),
        'drift':_differences(submitted[0],page.evaluate('collectEditorConfig().config'),'config') if submitted else []}
    assert submitted[0]['backtest']['exchanges']==['bybit','hyperliquid']
    if edit_during_lookup:
        assert 'editor inputs changed' in page.locator('#opted-ohlcv-start-status').text_content()
        assert page.locator('#opted-starting-balance').input_value()=='2345'
    assert submitted[0]['bot']['global']['universe']['approved_coins']==['HYPE']
    assert 'universe' not in submitted[0]['bot']['long']
    assert submitted[0]['bot']['short']['universe']['approved_coins']==[]
    assert not saved
    assert not errors


def test_side_json_coin_edit_converges_with_picker_without_save(native_page):
    """Editing the duplicate JSON view updates the picker and stays stable on collection."""
    page,source,errors,calls,saved=native_page
    page.locator('#btn-new-config').click()
    page.wait_for_function("document.getElementById('opted-starting-balance')?.value==='1000'")
    node=page.locator('#opted-bot-short')
    changed=json.loads(node.input_value())
    changed['universe']['approved_coins']=[]
    node.fill(json.dumps(changed));node.press('Tab')
    page.wait_for_function("optGetMs('ms-opt-app-short').length===0")
    for _ in range(3):
        result=page.evaluate('collectEditorConfig().config')
        assert result['bot']['short']['universe']['approved_coins']==[]
        assert result['bot']['long']==source['bot']['long']
    assert not saved
    assert not errors


def test_native_editor_navigation_restore_and_bounds(native_page):
    """The shared sections, canonical bounds and latest draft survive browser reload."""
    page,source,errors,calls,saved=native_page
    page.locator('#btn-new-config').click()
    page.wait_for_function("document.getElementById('opted-starting-balance')?.value==='1000'")
    shared = page.locator('[data-native-bound-section="global"]')
    portfolio = page.locator('[data-native-bound-section="portfolio"]')
    assert 'Global bounds (49)' in shared.locator('summary').inner_text()
    assert 'Portfolio bounds (1)' in portfolio.locator('summary').inner_text()
    assert page.get_by_text('Long bound overrides (0)',exact=True).is_visible()
    assert page.get_by_text('Short bound overrides (0)',exact=True).is_visible()
    assert not shared.locator('.optimize-bound-row').first.is_visible()
    assert not page.locator('#opt9-policy-portfolio').is_visible()
    assert page.locator('[data-remove-bound-override^="long."], [data-remove-bound-override^="short."]').count()==0
    shared.locator('summary').press('Enter')
    search = page.get_by_role('searchbox', name='Search global bounds')
    search.fill('min_duration_minutes')
    assert shared.locator('.optimize-bound-row:not([hidden])').count()==1
    assert shared.locator('.optimize-bound-row:not([hidden])').is_visible()
    search.fill('no visible match')
    assert shared.get_by_text('No matching bounds.').is_visible()
    search.fill('min_duration_minutes')
    assert page.evaluate('collectEditorConfig().config.optimize.bounds')==source['optimize']['bounds']
    page.locator('#pbgui-guide-btn').click()
    guide = page.locator('#pbgui-shared-help-ovl')
    guide.wait_for(state='visible')
    page.keyboard.press('Escape')
    assert not guide.is_visible()
    assert search.is_visible() and search.input_value()=='min_duration_minutes'
    page.locator('#opted-starting-balance').fill('3456')
    page.locator('#opted-starting-balance').press('Tab')
    page.wait_for_timeout(450)
    location=page.url
    page.reload()
    page.wait_for_function("document.getElementById('opted-starting-balance')?.value==='3456'")
    assert page.url==location
    assert search.is_visible() and search.input_value()=='min_duration_minutes'
    assert shared.locator('.optimize-bound-row:not([hidden])').count()==1
    assert page.evaluate('collectEditorConfig().config.bot')==source['bot']
    assert page.evaluate('collectEditorConfig().config.optimize.bounds')==source['optimize']['bounds']
    assert not errors


def _differences(a,b,path='optimize'):
    """Report only changed fixture paths, keeping failures small and actionable."""
    if a==b:return []
    if isinstance(a,dict) and isinstance(b,dict):
        return [diff for key in a.keys()|b.keys() for diff in _differences(a.get(key),b.get(key),path+'.'+key)]
    return [(path,a,b)]


def _open_native_policy(page):
    """Reveal policy controls through the real disclosure if it is closed."""
    section = page.locator('#opt9-policy-disclosure')
    if not section.evaluate('node=>node.open'):
        section.locator('summary').click()


def test_native_policy_matches_pb8_checkbox_layout_and_label_help(native_page):
    """Compare real shared PB8 markup and native policy geometry at two widths."""
    page, source, errors, calls, saved = native_page
    page.locator('#btn-new-config').click()
    page.wait_for_function("document.getElementById('opt9-policy-bot__portfolio__hedge_mode')")
    page.locator('#opted-bounds-panel').evaluate("node => {const details=node.closest('details'); if(details)details.open=true; node.closest('.expander')?.classList.add('open')}")
    _open_native_policy(page)
    for width in (1440, 1000):
        page.set_viewport_size({'width': width, 'height': 1000})
        geometry = page.evaluate('''() => {
            const control=document.getElementById('opt9-policy-bot__portfolio__hedge_mode');
            const native=control.closest('.meta-field');
            const label=native.querySelector('label');
            const text=label.querySelector('[data-tip]');
            const reference=document.createElement('div');
            reference.style.width=native.getBoundingClientRect().width+'px';
            reference.innerHTML=fieldCheckboxInline('pb8-reference',text.textContent,false,'Original hedge policy.');
            document.body.appendChild(reference);
            const field=reference.firstElementChild, check=field.querySelector('input'), help=field.querySelector('[data-tip]');
            const rect=node=>node.getBoundingClientRect();
            const result={nativeGap:rect(control).top-rect(text).bottom,
                pb8Gap:rect(check).top-rect(help).bottom,
                nativeHeight:rect(control).height,pb8Height:rect(check).height,
                leftGap:rect(control).left-rect(text).left,
                underline:getComputedStyle(text).textDecorationStyle,
                focusable:text.tabIndex,tip:text.dataset.tip};
            reference.remove(); return result;
        }''')
        assert abs(geometry['nativeGap'] - geometry['pb8Gap']) <= 1, geometry
        assert geometry['nativeHeight'] == geometry['pb8Height'] == 16
        assert abs(geometry['leftGap']) <= 1
        assert geometry['underline'] == 'dotted'
        assert geometry['focusable'] == 0
        assert 'Original hedge policy' in geometry['tip'] and 'docs/configuration.md' in geometry['tip']
        policy = page.evaluate('''() => {
          const rect = scope => document.getElementById('opt9-policy-' + scope)
            .closest('.bounds-side-card').getBoundingClientRect();
          const [portfolio, shared, long, short] = ['portfolio','global','long','short'].map(rect);
          return {sharedBelow: shared.top >= portfolio.bottom, sameWidth: shared.width === portfolio.width,
                  directionalBelow: long.top >= shared.bottom && short.top >= shared.bottom,
                  paired: long.top === short.top && long.left < short.left};
        }''')
        assert policy == {'sharedBelow': True, 'sameWidth': True, 'directionalBelow': True, 'paired': width == 1440}
    label = page.locator('[data-meta-path="bot.portfolio.hedge_mode"] label span')
    label.scroll_into_view_if_needed()
    label.focus()
    page.evaluate("document.dispatchEvent(new Event('scroll'))")
    page.wait_for_function("!document.getElementById('data-tip-tooltip').hidden && document.getElementById('data-tip-tooltip').textContent.includes('Original hedge policy')")
    assert 'docs/configuration.md' in page.locator('#data-tip-tooltip').inner_text()
    page.keyboard.press('Escape')
    assert page.locator('#data-tip-tooltip').get_attribute('hidden') is not None
    assert page.evaluate('collectEditorConfig().config.bot') == source['bot']
    assert not errors


def test_native_policy_and_gpu_fields_update_the_authoritative_json(native_page):
    """Native dynamic controls participate in the same immediate form/raw synchronization."""
    page,source,errors,calls,saved=native_page
    page.locator('#btn-new-config').click()
    page.wait_for_function("document.getElementById('opted-starting-balance')?.value==='1000'")
    _open_native_policy(page)
    page.locator('#opt9-policy-bot__global__hsl__enabled').check()
    assert page.evaluate('collectEditorConfig().config.bot.global.hsl.enabled') is True
    assert page.evaluate("JSON.parse(document.getElementById('opted-raw-json').value).bot.global.hsl.enabled") is True
    assert page.evaluate('collectEditorConfig().config.bot.short')==source['bot']['short']
    page.locator('#opted-opt-backend').select_option('gpu')
    field=page.locator('#opt9-field-optimize__gpu__checkpoint_interval_seconds')
    field.fill('12');field.press('Tab')
    assert page.evaluate('collectEditorConfig().config.optimize.gpu.checkpoint_interval_seconds')==12
    assert not errors


def test_native_strategy_change_and_scenario_recipe_keep_native_metric_names(native_page):
    """Shared strategy/scenario actions cannot inject retired PB8 paths or scoring names."""
    page,source,errors,calls,saved=native_page
    page.locator('#btn-new-config').click()
    page.wait_for_function("document.getElementById('opted-starting-balance')?.value==='1000'")
    _open_native_policy(page)
    page.locator('#opt9-policy-bot__global__hsl__enabled').check()
    page.locator('#opted-strategy-kind').select_option('ema_anchor')
    assert page.locator('#opt9-policy-bot__global__hsl__enabled').is_checked()
    assert page.locator('#opted-runtime-overrides-panel .bounds-side-card').count()==4
    page.evaluate("applyOptimizeSweepPreset({template:'all',contract_version:1})")
    config=page.evaluate('collectEditorConfig().config')
    assert config['bot']['portfolio']['strategy_kind']=='ema_anchor'
    assert 'strategy_kind' not in config['live']
    assert set(config['optimize']['bounds'])=={'global','portfolio'}
    assert config['optimize']['bounds']['global']['strategy']['ema_anchor']
    assert page.locator('[data-remove-bound-override^="long."], [data-remove-bound-override^="short."]').count()==0
    assert all('strategy_eq' not in item['metric'] or 'strategy_equity' in item['metric'] for item in config['optimize']['scoring'])
    assert {item['metric'] for item in config['optimize']['limits']}=={'drawdown_max_fraction_strategy_equity','backtest_completion_fraction'}
    assert not errors


def test_native_explicit_side_bounds_remain_independent_when_switching_strategies(native_page):
    """Show the authored Long override without inventing a matching Short range."""
    page,source,errors,calls,saved=native_page
    source['optimize']['bounds']['long']={'entry_cooldown': {'min_duration_minutes': [2, 6]}}
    page.locator('#btn-new-config').click()
    page.wait_for_function("document.getElementById('opted-starting-balance')?.value==='1000'")
    strategy=page.locator('#opted-strategy-kind')
    strategy.select_option('ema_anchor')
    assert page.locator('[data-remove-bound-override^="long."]').count()==1
    assert page.locator('[data-remove-bound-override^="short."]').count()==0
    strategy.select_option('trailing_martingale')
    bounds=page.evaluate('collectEditorConfig().config.optimize.bounds')
    assert bounds['long']==source['optimize']['bounds']['long']
    assert 'short' not in bounds
    assert bounds['global']['strategy']['trailing_martingale']==source['optimize']['bounds']['global']['strategy']['trailing_martingale']
    assert not errors


def test_native_can_add_and_remove_short_overrides_for_shared_ranges(native_page):
    """A range with no native Short recommendation can still be added, edited and removed."""
    page, source, errors, calls, saved = native_page
    source['bot']['short']['entry_cooldown'] = {'min_duration_minutes': 2}
    page.locator('#btn-new-config').click()
    page.wait_for_function("document.getElementById('opted-starting-balance')?.value==='1000'")
    summary = page.locator('#opted-bounds-summary')
    if not page.locator('[data-bound-override-source="short"]').is_visible():
        summary.click()
    key = 'entry_cooldown.min_duration_minutes'
    global_range = source['optimize']['bounds']['global']['entry_cooldown']['min_duration_minutes']
    short_picker = page.locator('[data-bound-override-source="short"]')
    assert short_picker.locator('optgroup[label="Pinned side values"] option[value="short.' + key + '"]').count()==1
    assert page.locator('[data-bound-override-source="long"] optgroup[label="Inherited global ranges"] option[value="long.' + key + '"]').count()==1
    short_picker.select_option('short.' + key)
    assert 'short' not in page.evaluate('collectEditorConfig().config.optimize.bounds')
    page.get_by_role('button', name='Add Short bound override').click()
    row = page.locator('#opt-bound-short\\.' + key.replace('.', '\\.'))
    row.locator('.optimize-bound-step-input').fill('1')
    row.locator('.optimize-bound-step-input').press('Tab')
    row.locator('.optimize-bound-fixed input').check()
    current = page.evaluate('collectEditorConfig().config.optimize')
    assert current['bounds']['short'] == {'entry_cooldown': {'min_duration_minutes': global_range[:2] + [1]}}
    assert current['bounds']['global'] == source['optimize']['bounds']['global']
    assert 'long' not in current['bounds']
    assert 'bot.short.' + key in current['fixed_params']
    page.wait_for_function("JSON.parse(document.getElementById('opted-raw-json').value).optimize.bounds.short?.entry_cooldown?.min_duration_minutes?.length === 3")
    page.reload()
    page.locator('[data-remove-bound-override="short.' + key + '"]').wait_for(state='visible')
    page.locator('[data-remove-bound-override="short.' + key + '"]').click()
    current = page.evaluate('collectEditorConfig().config.optimize')
    assert 'short' not in current['bounds']
    assert 'bot.short.' + key not in current['fixed_params']
    assert page.get_by_text('No Short bound overrides.', exact=True).is_visible()
    assert not saved
    assert not errors


def test_native_can_add_portfolio_and_unseeded_side_bounds(native_page):
    """Offer the native range catalog beyond the fields seeded in the template."""
    page, source, errors, calls, saved = native_page
    page.locator('#btn-new-config').click()
    page.wait_for_function("document.getElementById('opted-starting-balance')?.value==='1000'")
    if not page.get_by_text('Short bound overrides (0)', exact=True).is_visible():
        page.locator('#opted-bounds-summary').click()
    portfolio = page.locator('[data-native-bound-section="portfolio"]')
    portfolio.locator('summary').click()
    portfolio.get_by_role('combobox', name='Portfolio bound parameter').select_option(
        'portfolio.loss_gate.memory_ema_span_minutes'
    )
    portfolio.get_by_role('button', name='Add Portfolio bound').click()
    portfolio.get_by_role('button', name='Remove portfolio.loss_gate.memory_ema_span_minutes').wait_for()
    assert page.evaluate('collectEditorConfig().config.optimize.bounds.portfolio.loss_gate.memory_ema_span_minutes')
    assert page.evaluate('collectEditorConfig().config.optimize.bounds.portfolio.unstuck') == source['optimize']['bounds']['portfolio']['unstuck']
    side_picker = page.get_by_role('combobox', name='Short bound parameter')
    side_picker.select_option('short.entry_cooldown.max_duration_minutes')
    page.get_by_role('button', name='Add Short bound override').click()
    assert page.evaluate('collectEditorConfig().config.optimize.bounds.short.entry_cooldown.max_duration_minutes') == [360, 360]
    assert portfolio.locator('option[value^="portfolio.hsl."]').count() == 0
    page.locator('#opted-hsl-signal-mode').select_option('portfolio')
    assert portfolio.locator('option[value^="portfolio.hsl."]').count() == 4
    assert not errors


def test_native_additional_parameters_reuse_pb8_fields_and_save(native_page):
    """List only unmapped authored optimize keys and preserve their types on save."""
    page, source, errors, calls, saved = native_page
    source['optimize'].update(future_number=3, future_switch=False, future_object={'original': 1})
    page.locator('#btn-new-config').click()
    page.wait_for_function("document.getElementById('opted-starting-balance')?.value==='1000'")
    extras = page.locator('#exp-opt-extra')
    extras.locator('.expander-header').click()
    assert extras.locator('input[type="search"], select').count() == 0
    assert set(extras.locator('[data-extra-param-key]').evaluate_all(
        '(nodes) => nodes.map(node => node.dataset.extraParamKey)'
    )) == {'future_number', 'future_switch', 'future_object'}
    assert extras.locator('[data-extra-param-key="prefilter"]').count() == 0

    extras.locator('[data-extra-param-key="future_number"]').fill('7.25')
    extras.locator('[data-extra-param-key="future_switch"]').check()
    object_field = extras.locator('[data-extra-param-key="future_object"]')
    object_field.fill('{')
    page.locator('#opted-name').fill('parity')
    page.locator('#btn-cloud-save').click()
    assert not saved
    assert 'invalid' in page.locator('#cfg-editor-status').text_content().lower()
    object_field.fill('{"new_child": [1, false]}')
    page.locator('#btn-cloud-save').click()
    page.wait_for_function("document.getElementById('btn-cloud-save')?.disabled===false")
    assert saved, page.locator('#cfg-editor-status').text_content()
    result = saved[0]['config']['optimize']
    assert result['future_number'] == 7.25
    assert result['future_switch'] is True
    assert result['future_object'] == {'new_child': [1, False]}
    assert result['prefilter'] == source['optimize']['prefilter']
    assert not errors


def test_native_additional_parameters_do_not_materialize_missing_defaults(native_page):
    """The expanded runtime prototype does not add an extra to a sparse draft."""
    page, source, errors, calls, saved = native_page
    del source['optimize']['prefilter']
    page.locator('#btn-new-config').click()
    page.wait_for_function("document.getElementById('opted-starting-balance')?.value==='1000'")
    extras = page.locator('#exp-opt-extra')
    extras.locator('.expander-header').click()
    assert extras.get_by_text('No additional optimize parameters detected for this config.').is_visible()
    assert extras.locator('[data-extra-param-key]').count() == 0
    assert not page.evaluate("Object.hasOwn(collectEditorConfig().config.optimize, 'prefilter')")
    assert not errors


def test_native_prefilter_checks_save_without_json_and_can_be_removed(native_page):
    """Author a native check with typed controls and remove a synchronized rule."""
    page, source, errors, calls, saved = native_page
    page.locator('#btn-new-config').click()
    page.wait_for_function("document.getElementById('opted-starting-balance')?.value==='1000'")
    form = page.locator('#opt9-prefilter')
    assert form.locator('textarea').count() == 0
    assert page.evaluate('collectEditorConfig().config.optimize.prefilter') == source['optimize']['prefilter']
    form.get_by_role('button', name='Add check', exact=True).click()
    check = form.locator('[data-prefilter-check]').first
    check.locator('[data-config-path="check.metric"]').select_option(label='grid_span_fraction')
    check.locator('[data-config-path="check.min"]').fill('0.1')
    check.locator('[data-config-path="check.max"]').fill('0.35')
    check.locator('[data-config-path="check.enabled"]').check()
    check.get_by_role('checkbox', name='Short', exact=True).uncheck()
    page.wait_for_function("state.editorLastConfig.optimize.prefilter.checks?.[0]?.enabled === true")
    page.locator('#opted-name').fill('parity')
    page.locator('#btn-cloud-save').click()
    page.wait_for_function("document.getElementById('btn-cloud-save')?.disabled===false")
    assert saved, page.locator('#cfg-editor-status').text_content()
    result = saved[0]['config']['optimize']['prefilter']
    assert result['checks'] == [{'metric':'grid_span_fraction', 'enabled':True, 'min':0.1, 'max':0.35, 'sides':['long']}]
    assert result['exposure_probe_fractions'] == source['optimize']['prefilter']['exposure_probe_fractions']
    assert not errors


def test_native_prefilter_rejects_invalid_checks_and_preserves_sparse_values(native_page):
    """Require real limits for enabled checks and preserve omitted defaults."""
    page, source, errors, calls, saved = native_page
    del source['optimize']['prefilter']
    page.locator('#btn-new-config').click()
    page.wait_for_function("document.getElementById('opted-starting-balance')?.value==='1000'")
    form = page.locator('#opt9-prefilter')
    assert not page.evaluate("Object.hasOwn(collectEditorConfig().config.optimize, 'prefilter')")
    form.get_by_role('button', name='Add check', exact=True).click()
    check = form.locator('[data-prefilter-check]').first
    check.locator('[data-config-path="check.enabled"]').check()
    page.locator('#opted-name').fill('parity')
    page.locator('#btn-cloud-save').click()
    page.wait_for_function("document.getElementById('btn-cloud-save')?.disabled===false")
    assert not saved
    assert 'minimum or maximum' in form.text_content()
    check.locator('[data-config-path="check.max"]').fill('0.4')
    page.wait_for_function("state.editorLastConfig.optimize.prefilter?.checks?.length === 1")
    check.get_by_role('button', name='Remove check').click()
    page.wait_for_function("state.editorLastConfig.optimize.prefilter?.checks?.length === 0")
    assert not errors


def test_native_overrides_and_gpu_scenarios_use_structured_controls(native_page):
    """Expose every installed override and edit scenario labels without a JSON textarea."""
    page, source, errors, calls, saved = native_page
    page.locator('#btn-new-config').click()
    page.wait_for_function("document.getElementById('opted-starting-balance')?.value==='1000'")
    _open_native_policy(page)
    for key in ('mirror_short_from_long', 'couple_unstuck_ema_spans', 'lossless_close_trailing'):
        page.locator('[data-pb8-enable-override="' + key + '"]').check()
    assert set(page.evaluate('collectEditorConfig().config.optimize.enable_overrides')) == {
        'mirror_short_from_long', 'couple_unstuck_ema_spans', 'lossless_close_trailing'
    }
    page.locator('#opted-opt-backend').select_option('gpu')
    assert 'Enable Suite mode' in page.locator('#opt9-gpu-screening-scenarios').inner_text()
    assert page.locator('#opt9-gpu-fields textarea').count() == 0
    assert not errors


def test_native_gpu_screening_scenarios_select_suite_labels(native_page):
    """Screening scenarios are chosen from Suite labels; stale labels block saving."""
    page, source, errors, calls, saved = native_page
    source['backtest']['suite_enabled'] = True
    source['optimize'].setdefault('gpu', {}).setdefault('screening', {})['scenarios'] = ['gone']
    _open_new_native_draft(page)
    page.locator('#opted-opt-backend').select_option('gpu')
    scenarios = page.locator('#opt9-gpu-screening-scenarios')
    labels = [item['label'] for item in source['backtest']['scenarios']]
    assert scenarios.locator('input[type=checkbox]').count() == len(labels) + 1
    assert 'gone (not in Suite)' in scenarios.inner_text()
    assert 'must be Suite labels: gone' in page.locator('#opt9-gpu-fields').inner_text()
    scenarios.locator('[data-screening-label="gone"]').uncheck()
    scenarios.locator('[data-screening-label="subset_2"]').check()
    scenarios.locator('[data-screening-label="base"]').check()
    assert page.evaluate('collectEditorConfig().config.optimize.gpu.screening.scenarios') == ['base', 'subset_2']
    assert 'must be Suite labels' not in page.locator('#opt9-gpu-fields').inner_text()
    scenarios.locator('[data-screening-label="base"]').uncheck()
    scenarios.locator('[data-screening-label="subset_2"]').uncheck()
    assert page.evaluate('collectEditorConfig().config.optimize.gpu.screening.scenarios') == []
    assert not errors


def test_native_vast_reuses_every_shared_settings_view(native_page):
    """Native cloud UI uses the common five views and never requests the legacy runtime API."""
    page,source,errors,calls,saved=native_page
    page.locator('#nav-vast').click()
    assert page.locator('#vast-settings-nav').is_visible()
    assert page.locator('#vast-settings-nav [data-vast-view]').evaluate_all(
        '(buttons)=>buttons.map(button=>button.dataset.vastView).sort()')==['account','hosts','offers','performance','rental']
    for view,target in [('rental','vast-rental'),('hosts','vast-hosts-filter'),
            ('performance','vast-performance'),('account','vast-setup')]:
        page.locator('[data-vast-view='+view+']').click()
        assert page.locator('#'+target).is_visible()
    assert any(path.startswith('/api/vast-v9/') for path in calls)
    assert not any(path.startswith('/api/vast/') for path in calls)
    assert page.locator('#performance-refresh').count()==0
    assert not errors


def test_native_exchange_picker_offers_unselected_supported_venues(native_page):
    """Template defaults select two venues without restricting the historical capability list."""
    page,source,errors,calls,saved=native_page
    page.locator('#btn-new-config').click()
    page.wait_for_function("document.getElementById('opted-starting-balance')?.value==='1000'")
    choices=json.loads((ROOT/'tests/fixtures/pb9_optimize_metadata.json').read_text())['exchange_options']['optimize']
    for exchange in choices:
        if exchange in source['backtest']['exchanges']:continue
        field=page.locator('#ms-opt-exchanges-input');field.fill(exchange)
        page.locator('#ms-opt-exchanges-dd .ms-option[data-val="'+exchange+'"]').click()
    assert set(page.evaluate('collectEditorConfig().config.backtest.exchanges'))==set(choices)
    assert source['backtest']['exchanges']==['binance','bybit']
    assert not errors


def test_native_coin_picker_uses_one_normal_name_per_coin(native_page):
    """The shared picker displays canonical native IDs without duplicate venue entries."""
    page,source,errors,calls,saved=native_page
    page.locator('#btn-new-config').click()
    page.wait_for_function("document.getElementById('opted-starting-balance')?.value==='1000'")
    page.locator('#ms-opt-app-long-input').fill('BTC')
    option=page.locator('#ms-opt-app-long-dd .ms-option[data-val="BTC"]')
    assert option.count()==1
    assert option.text_content().strip()=='BTC'
    assert page.locator('#ms-opt-app-long-dd .ms-option[data-val*="::"]').count()==0
    option.click()
    assert page.evaluate("collectEditorConfig().config.bot.long.universe.approved_coins")==['HYPE','BTC']
    assert not saved
    assert not errors


@pytest.mark.parametrize('query,expected',[
    ('hype',['HYPE','HYPER']),('stx',['hyperliquid::19']),
    ('19',['hyperliquid::19']),('hyperliquid::19',['hyperliquid::19']),
    ('hyperliquid',[]),
])
def test_coin_search_matches_coin_name_not_exchange_prefix(native_page,query,expected):
    """HYPE search cannot surface STX merely because its native venue is Hyperliquid."""
    page,source,errors,calls,saved=native_page
    def catalog(route):
        """Supply a genuine-style collision ID through the production catalog route."""
        route.fulfill(json={'symbols':['HYPE','HYPER','hyperliquid::19'],
            'catalog':[{'config_id':value,'coin':coin,'display':display,
                        'resolutions':[{'exchange':'hyperliquid','symbol':coin+'/USDC:USDC'}]}
                       for value,coin,display in [('HYPE','HYPE','HYPE'),('HYPER','HYPER','HYPER'),
                                                  ('hyperliquid::19','STX','STX (hyperliquid: 19)')]],
            'statuses':{}})
    page.route('**/coins/status',catalog)
    page.locator('#btn-new-config').click()
    page.wait_for_function("document.getElementById('opted-starting-balance')?.value==='1000'")
    page.wait_for_function("_optMarketLabels['hyperliquid::19']?.startsWith('STX')")
    page.locator('#ms-opt-app-short-input').fill(query)
    assert page.locator('#ms-opt-app-short-dd .ms-option').evaluate_all(
        '(options)=>options.map(option=>option.dataset.val)')==expected
    if query=='stx':
        page.locator('#ms-opt-app-short-dd .ms-option[data-val="hyperliquid::19"]').click()
        assert page.evaluate('collectEditorConfig().config.bot.short.universe.approved_coins')==['HYPE','hyperliquid::19']
    page.locator('#ms-opt-exchanges-input').fill('hype')
    assert page.locator('#ms-opt-exchanges-dd .ms-option[data-val="hyperliquid"]').is_visible()
    assert not saved
    assert not errors


@pytest.mark.parametrize('action,count', [('backtest', 1), ('backtest', 2), ('validation', 1)])
def test_native_result_to_backtest_uses_pb9_queue_and_open_queue(native_page, action, count):
    """Actual result and Pareto controls submit native snapshots and open only PB9."""
    page, source, errors, calls, _saved = native_page
    posted, queue_calls = [], []
    result_path = '/fixture/native-result'
    paretos = [{'name': 'candidate-' + str(index),
                'path': result_path + '/pareto/candidate-' + str(index) + '.json',
                'summary': {'cagr': 0.2}, 'modified': '2026-01-01T12:00:00'}
               for index in range(count)]

    def results(route):
        """Supply one result through the real result-list loading path."""
        route.fulfill(json={'results': [{'name': 'Native result', 'result': 'native-result',
            'path': result_path, 'pareto_count': count, 'has_pareto': True,
            'has_config': True, 'modified': '2026-01-01T12:00:00'}]})

    def candidate_list(route):
        """Advertise native candidates without touching real optimizer results."""
        route.fulfill(json={'paretos': paretos, 'meta': {'mode': 'single',
            'available_metrics': ['cagr'], 'default_metrics': ['cagr'],
            'available_statistics': ['mean'], 'objectives': []}})

    def queue(route):
        """Capture queue requests and prevent every real enqueue or process start."""
        path = urlsplit(route.request.url).path
        queue_calls.append(path)
        assert path.startswith('/api/backtest-v9/'), path
        if route.request.method == 'POST':
            assert path == '/api/backtest-v9/queue'
            posted.append(route.request.post_data_json)
            route.fulfill(json={'ok': True, 'filename': 'fixture-' + str(len(posted))})
        elif path.endswith('/settings'):
            route.fulfill(json={'autostart': False})
        elif path.endswith('/main_page'):
            route.fulfill(content_type='text/html', body='<title>PB9 fixture queue</title>')
        else:
            route.fulfill(json={'items': [{'status': 'queued'} for _ in posted]})

    page.route('**/api/backtest-*', queue)
    page.route('**/api/optimize-v9/results', results)
    page.route('**/api/optimize-v9/paretos?*', candidate_list)
    page.route('**/api/optimize-v9/paretos/file?*', lambda route: route.fulfill(
        json={'config': source, 'override_configs': {}}))
    if action == 'backtest' and count == 1:
        page.route('**/queue-menu-fixture', lambda route: route.fulfill(
            content_type='text/html', body='''<!doctype html><nav id="topnav"></nav>
            <script>window.API_BASE='/api/dashboard';window.PBGUI_NAV_CONFIG={
            current:'dashboard',subtitle:'Queue navigation test',apiBase:'/api/dashboard'};</script>
            <script src="/app/pbgui_nav.js"></script>'''))
        page.goto('http://native.test/queue-menu-fixture')
        page.locator('.nav-group-btn[data-group="pbv9"]').click()
        page.locator('.nav-item[data-page="v9_optimize"]').click()
        assert urlsplit(page.url).path == '/api/optimize-v9/main_page'
    else:
        page.reload()
    page.wait_for_function('!state.initialLoadPending')
    assert page.locator('script[src*="backtest_queue_actions.js"]').get_attribute('src').endswith('?v=4')
    page.locator('.sb-section[data-panel="results"]').click()
    page.locator('#results-tbody .result-paretos').click()
    for candidate in paretos:
        page.locator('#paretos-tbody tr[data-path="' + candidate['path'] + '"]').click()
    if action == 'validation':
        page.locator('#holdout-validation-mode').select_option('full_timerange')
        page.locator('#btn-holdout-selected-paretos').click()
    else:
        page.locator('#btn-backtest-selected-paretos').click()
    page.wait_for_function("document.querySelector('[data-backtest-queue-status]').textContent.includes('jobs added')")
    assert len(posted) == count
    for candidate in posted:
        assert candidate['config']['config_version'] == 'v9.0.0'
        assert candidate['config']['backtest']['exchanges'] == source['backtest']['exchanges']
        assert candidate['config']['backtest']['start_date'] == source['backtest']['start_date']
        assert candidate['config']['backtest']['end_date'] == source['backtest']['end_date']
    assert page.evaluate('state.selectedParetos.size') == count
    assert urlsplit(page.url).fragment == 'paretos'
    assert not any('/backtest-v7/' in path or '/backtest-v8/' in path for path in calls + queue_calls)
    page.locator('[data-backtest-open-queue]').click()
    page.wait_for_function("location.pathname==='/api/backtest-v9/main_page' && location.search==='?panel=queue'")
    assert '/api/backtest-v9/main_page' in queue_calls
    assert not errors


def _open_new_native_draft(page):
    """Open the fixture template through the real New button."""
    page.locator('#btn-new-config').click()
    page.wait_for_function("document.getElementById('opted-starting-balance')?.value==='1000'")


def _edit_side_json(page, side, change):
    """Edit one effective Side JSON view through its real textarea."""
    node = page.locator('#opted-bot-' + side)
    value = json.loads(node.input_value())
    change(value)
    node.fill(json.dumps(value))
    node.press('Tab')


def test_side_json_policy_boolean_edit_is_not_reverted_by_policy_panel(native_page):
    """A Side JSON boolean edit survives repeated collection and reaches the panel."""
    page, source, errors, calls, saved = native_page
    _open_new_native_draft(page)
    _edit_side_json(page, 'long', lambda value: value['hsl'].update(enabled=True))
    page.wait_for_function("document.getElementById('opt9-policy-bot__long__hsl__enabled')?.checked===true")
    for _ in range(3):
        result = page.evaluate('collectEditorConfig().config')
        assert result['bot']['long']['hsl']['enabled'] is True
        assert result['bot']['global']['hsl']['enabled'] is False
    assert page.evaluate("JSON.parse(document.getElementById('opted-raw-json').value).bot.long.hsl.enabled") is True
    assert not errors


def test_policy_panel_edit_updates_side_json_and_later_side_edit_wins(native_page):
    """Panel and Side JSON edits of the same path converge without losing the newer one."""
    page, source, errors, calls, saved = native_page
    _open_new_native_draft(page)
    _open_native_policy(page)
    page.locator('#opt9-policy-bot__global__hsl__enabled').check()
    page.wait_for_function("JSON.parse(document.getElementById('opted-bot-long').value).hsl.enabled===true")
    _edit_side_json(page, 'long', lambda value: value['hsl'].update(enabled=False))
    page.wait_for_function("document.getElementById('opt9-policy-bot__long__hsl__enabled')?.checked===false")
    result = page.evaluate('collectEditorConfig().config')
    assert result['bot']['global']['hsl']['enabled'] is True
    assert result['bot']['long']['hsl']['enabled'] is False
    assert page.locator('#opt9-policy-bot__global__hsl__enabled').is_checked()
    assert not errors


def test_hsl_mode_change_keeps_native_policy_fields(native_page):
    """Changing HSL scope and bounds data keeps the native policy panel intact."""
    page, source, errors, calls, saved = native_page
    _open_new_native_draft(page)
    _open_native_policy(page)
    page.locator('#opted-hsl-signal-mode').select_option('portfolio')
    assert page.locator('#opt9-policy-bot__portfolio__hedge_mode').count() == 1
    assert 'No runtime overrides' not in page.locator('#opted-runtime-overrides-panel').inner_text()
    page.locator('#opt9-policy-bot__global__hsl__enabled').check()
    assert page.evaluate('collectEditorConfig().config.bot.portfolio.hsl.scope') == 'portfolio'
    assert page.evaluate('collectEditorConfig().config.bot.global.hsl.enabled') is True
    page.evaluate("setOptimizeBoundsData(state.optimizeBounds.map(entry=>[entry.key,[entry.low,entry.high]]).reduce((a,[k,v])=>(a[k]=v,a),{}), state.optimizeFixedParams, {})")
    assert page.locator('#opt9-policy-bot__portfolio__hedge_mode').count() == 1
    assert not errors


SCENARIO_COIN_FORMS = [
    {'label': 'file', 'coins': 'configs/approved_coins.json', 'ignored_coins': ['BTC']},
    {'label': 'sides', 'coins': {'long': ['HYPE'], 'short': 'configs/short.json'},
     'ignored_coins': {'long': None, 'short': ['BTC']}},
    {'label': 'list', 'coins': ['HYPE', 'BTC']},
]


@pytest.mark.parametrize('index', range(len(SCENARIO_COIN_FORMS)))
def test_suite_scenario_coin_forms_survive_open_and_save(native_page, index):
    """String, list and {long, short} coin values keep their authored form."""
    page, source, errors, calls, saved = native_page
    source['backtest']['suite_enabled'] = True
    source['backtest']['scenarios'] = copy.deepcopy(SCENARIO_COIN_FORMS)
    _open_new_native_draft(page)
    page.locator('#suite-container').evaluate("node=>node.closest('details')?.setAttribute('open','')")
    page.evaluate("document.getElementById('exp-suite')?.classList.add('open')")
    page.locator('#suite-container button[title="Edit"]').nth(index).click()
    page.wait_for_function("document.getElementById('suite-sc-label')?.value===%s" % json.dumps(SCENARIO_COIN_FORMS[index]['label']))
    for _ in range(2):
        scenarios = page.evaluate('collectEditorConfig().config.backtest.scenarios')
        assert scenarios == SCENARIO_COIN_FORMS
    assert not errors


def test_suite_scenario_coin_form_switch_writes_selected_form(native_page):
    """Switching a list to Long/Short and a text value back to a list uses real controls."""
    page, source, errors, calls, saved = native_page
    source['backtest']['suite_enabled'] = True
    source['backtest']['scenarios'] = copy.deepcopy(SCENARIO_COIN_FORMS)
    _open_new_native_draft(page)
    page.evaluate("document.getElementById('exp-suite')?.classList.add('open')")
    page.locator('#suite-container button[title="Edit"]').nth(2).click()
    page.locator('#suite-sc-coins-form').select_option('sides')
    page.wait_for_selector('#suite-sc-coins-short-ms')
    page.locator('#suite-sc-coins-short-ms .ms-x[data-val="BTC"]').click()
    scenario = page.evaluate('collectEditorConfig().config.backtest.scenarios[2]')
    assert scenario['coins'] == {'long': ['HYPE', 'BTC'], 'short': ['HYPE']}
    page.locator('#suite-container button[title="Edit"]').nth(0).click()
    page.locator('#suite-sc-coins-text').fill('configs/other.json')
    page.locator('#suite-sc-coins-text').press('Tab')
    assert page.evaluate('collectEditorConfig().config.backtest.scenarios[0].coins') == 'configs/other.json'
    assert not errors


EMPTY_COIN_FORMS = [
    {'label': 'empty_list', 'start_date': '2024-01-01', 'coins': [], 'ignored_coins': []},
    {'label': 'empty_sides', 'start_date': '2024-01-01', 'coins': {'long': [], 'short': ['HYPE']},
     'ignored_coins': {'long': '', 'short': []}},
    {'label': 'empty_object', 'start_date': '2024-01-01', 'coins': {}, 'ignored_coins': ''},
]


def _open_suite_scenario(page, source, scenarios, index):
    """Open one authored Suite scenario through its real Edit button."""
    source['backtest']['suite_enabled'] = True
    source['backtest']['scenarios'] = copy.deepcopy(scenarios)
    _open_new_native_draft(page)
    page.evaluate("document.getElementById('exp-suite')?.classList.add('open')")
    page.locator('#suite-container button[title="Edit"]').nth(index).click()
    page.wait_for_function("document.getElementById('suite-sc-label')?.value===%s" % json.dumps(scenarios[index]['label']))


@pytest.mark.parametrize('index', range(len(EMPTY_COIN_FORMS)))
def test_suite_explicit_empty_coin_values_survive_open_other_edits_and_save(native_page, index):
    """[] / '' / {} and empty sides are explicit overrides, not inheritance."""
    page, source, errors, calls, saved = native_page
    _open_suite_scenario(page, source, EMPTY_COIN_FORMS, index)
    assert page.evaluate('collectEditorConfig().config.backtest.scenarios') == EMPTY_COIN_FORMS
    page.locator('#suite-sc-start').fill('2024-02-01')
    page.locator('#suite-sc-start').press('Tab')
    page.locator('#suite-sc-label').fill('renamed')
    page.locator('#suite-sc-label').press('Tab')
    expected = copy.deepcopy(EMPTY_COIN_FORMS)
    expected[index].update(label='renamed', start_date='2024-02-01')
    assert page.evaluate('collectEditorConfig().config.backtest.scenarios') == expected
    page.locator('#opted-name').fill('parity')
    page.locator('#btn-cloud-save').click()
    page.wait_for_function("document.getElementById('btn-cloud-save')?.disabled===false")
    assert len(saved) == 1
    assert saved[0]['config']['backtest']['scenarios'] == expected
    assert not errors


def test_suite_coin_inheritance_and_side_removal_are_explicit_controls(native_page):
    """Inherit removes the override, Not set removes one side, and clearing keeps an empty list."""
    page, source, errors, calls, saved = native_page
    _open_suite_scenario(page, source, SCENARIO_COIN_FORMS, 2)
    page.locator('label:has(+ #suite-sc-coins-ms) .ms-clear-btn').click()
    assert page.evaluate('collectEditorConfig().config.backtest.scenarios[2].coins') == []
    page.locator('#suite-sc-coins-form').select_option('inherit')
    scenario = page.evaluate('collectEditorConfig().config.backtest.scenarios[2]')
    assert 'coins' not in scenario
    assert page.locator('#suite-sc-coins-ms').count() == 0
    page.locator('#suite-container button[title="Edit"]').nth(1).click()
    page.wait_for_function("document.getElementById('suite-sc-label')?.value==='sides'")
    page.locator('#suite-sc-coins-short-mode').select_option('absent')
    page.locator('#suite-sc-ign-long-mode').select_option('list')
    scenario = page.evaluate('collectEditorConfig().config.backtest.scenarios[1]')
    assert scenario['coins'] == {'long': ['HYPE']}
    assert scenario['ignored_coins'] == {'long': [], 'short': ['BTC']}
    page.locator('#suite-sc-coins-form').select_option('inherit')
    page.locator('#suite-sc-ign-form').select_option('list')
    scenario = page.evaluate('collectEditorConfig().config.backtest.scenarios[1]')
    assert 'coins' not in scenario
    assert scenario['ignored_coins'] == ['BTC']
    assert not errors


def test_native_gpu_backend_keeps_population_and_pymoo_settings(native_page):
    """PB9 GPU uses optimize.population_size and optimize.pymoo like CPU orchestration."""
    page, source, errors, calls, saved = native_page
    _open_new_native_draft(page)
    page.locator('#opted-opt-backend').select_option('gpu')
    assert page.locator('#optimize-pymoo-section').is_visible()
    assert page.locator('#opted-pymoo-algorithm').is_visible()
    result = page.evaluate('collectEditorConfig().config.optimize')
    assert result['backend'] == 'gpu'
    assert result['population_size'] == source['optimize']['population_size']
    assert result['pymoo'] == source['optimize']['pymoo']
    page.locator('#opted-pymoo-population-mode').select_option('value')
    page.locator('#opted-pymoo-population-size').fill('96')
    page.locator('#opted-pymoo-population-size').press('Tab')
    page.locator('#opted-pymoo-crossover-eta').fill('25')
    page.locator('#opted-pymoo-crossover-eta').press('Tab')
    result = page.evaluate('collectEditorConfig().config.optimize')
    assert result['population_size'] == 96
    assert result['pymoo']['shared']['crossover_distribution_index'] == 25
    assert 'population_size' not in result['gpu']
    assert not errors


def test_native_result_opens_pb9_pareto_explorer(native_page):
    """The PB9 Results action navigates to the Explorer with the PB9 generation."""
    page, source, errors, calls, saved = native_page
    page.evaluate("openFastApiParetoExplorer('/pb9/optimize_results/run')")
    page.wait_for_url('**/api/pareto-explorer/main_page?**')
    query = parse_qs(urlsplit(page.url).query)
    assert query == {'result_path': ['/pb9/optimize_results/run'], 'optimize_version': ['v9']}
    assert not errors


def test_native_sweep_preset_uses_cagr_and_pb9_position_bounds(native_page):
    """PB9 Sweep keeps native CAGR for window gains and sizes PB9 position bounds."""
    page, source, errors, calls, saved = native_page
    _open_new_native_draft(page)
    page.evaluate("applyOptimizeSweepPreset({template:'sweep_cycles',contract_version:1,parameters:{sweep_policy:{starting_balance:1000}}})")
    config = page.evaluate('collectEditorConfig().config')
    assert config['optimize']['scoring'][0]['metric'] == 'cagr_fraction_strategy_equity'
    risk = config['optimize']['bounds']['global']['risk']
    assert risk['max_positions'][:2] == [1, 1]
    assert risk['total_wallet_exposure_limit_ratio'][:2] == [6, 10]
    assert 'bot.global.risk.max_positions' in config['optimize']['fixed_params']
    assert 'bot.global.forager.score_weights.volume' in config['optimize']['fixed_params']
    assert not any(key.endswith(('n_positions', 'total_wallet_exposure_limit')) for key in config['optimize']['fixed_params'])
    assert not errors


def test_fixed_bound_shows_the_effective_value_it_freezes(native_page):
    """Fixed shared bounds show the inherited config value Passivbot will keep."""
    page, source, errors, calls, saved = native_page
    _open_new_native_draft(page)
    page.locator('#opted-bounds-panel').evaluate("node => {const details=node.closest('details'); if(details)details.open=true; node.closest('.expander')?.classList.add('open')}")
    shared = page.locator('[data-native-bound-section="global"]')
    shared.locator('summary').click()
    page.get_by_role('searchbox', name='Search global bounds').fill('base_duration_minutes')
    row = shared.locator('.optimize-bound-row:not([hidden])')
    value = row.locator('[data-fixed-value-for]')
    assert value.get_attribute('hidden') is not None
    row.locator('.optimize-bound-fixed input').check()
    expected = source['bot']['global']['entry_cooldown']['base_duration_minutes']
    assert value.is_visible() and value.inner_text() == 'uses: ' + json.dumps(expected)
    row.locator('.optimize-bound-fixed input').uncheck()
    assert value.get_attribute('hidden') is not None
    assert not errors


def test_side_override_can_be_reset_to_inherited_value(native_page):
    """A pinned Long value is listed and its Reset restores inheritance from Shared defaults."""
    page, source, errors, calls, saved = native_page
    source['bot']['long']['risk'] = {'max_positions': 3}
    _open_new_native_draft(page)
    _open_native_policy(page)
    overrides = page.locator('#opt9-side-overrides-long')
    assert 'Long overrides (2)' in overrides.inner_text()
    overrides.locator('[data-reset-side-override="long.risk.max_positions"]').click()
    page.wait_for_function("!document.querySelector('[data-reset-side-override=\"long.risk.max_positions\"]')")
    config = page.evaluate('collectEditorConfig().config')
    assert 'risk' not in config['bot']['long']
    assert config['bot']['global']['risk']['max_positions'] == source['bot']['global']['risk']['max_positions']
    assert json.loads(page.locator('#opted-bot-long').input_value())['risk']['max_positions'] == source['bot']['global']['risk']['max_positions']
    assert 'Long overrides (1)' in page.locator('#opt9-side-overrides-long').inner_text()
    assert 'Short overrides (1)' in page.locator('#opt9-side-overrides-short').inner_text()
    assert not errors


def test_score_hysteresis_margin_is_a_regular_policy_field(native_page):
    """Non-optimizable numeric policy values are edited as normal fields."""
    page, source, errors, calls, saved = native_page
    _open_new_native_draft(page)
    _open_native_policy(page)
    field = page.locator('#opt9-policy-bot__global__forager__score_hysteresis_margin')
    field.fill('0.05')
    field.press('Tab')
    assert page.evaluate('collectEditorConfig().config.bot.global.forager.score_hysteresis_margin') == 0.05
    assert page.locator('#opt9-policy-bot__portfolio__warmup__max_minutes').count() == 1
    assert page.locator('#opt9-policy-bot__global__risk__max_positions').count() == 0
    assert not errors
