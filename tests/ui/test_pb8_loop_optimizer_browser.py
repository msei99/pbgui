"""Real optimizer shell and Loop tab against isolated authenticated fixture APIs."""
from pathlib import Path
import json
import copy
import re
from urllib.parse import urlparse, parse_qs, parse_qs

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize('viewport_width,sidebar_width', [(1200, 160), (1200, 420), (720, 160)])
def test_gpu_replacement_review_wait_reload_and_guide(viewport_width, sidebar_width):
    """Real menu, explicit review, automatic state updates and preserved navigation with fixture-only rentals."""
    browser_api = __import__('browser_driver')
    source = (ROOT / 'frontend/v7_optimize.html').read_text()
    for key, value in {'API_BASE':'/api/optimize-v8','OPTIMIZE_VERSION':'v8','VERSION':'test',
            'SERIAL':'1','NAV_HASH':'test','BASE_PREFIX':'','LIMITS_META':'{}',
            'BACKTEST_VERSION':'v8','OPTIMIZE_NAV_TITLE':'PB8 Optimize','OPTIMIZE_NAV_CURRENT':'v8_optimize'}.items():
        source = source.replace('%%'+key+'%%', value)
    loop_id, worker_id = 'a' * 32, 'b' * 32
    run = dict(id=loop_id, name='Isolated replacement', status='running', phase='optimize', round=0,
        created_at=1000, settings={'config_name':'fixture','execution':'vast'}, jobs=[],
        history=[], observer_enabled=False, ended=False, cycles=[], control_generation=0)
    rental = dict(worker_id=worker_id, deadline=2000007200, loop_deadline=2000000000, budget_usd=2,
        price_hour_usd=.08, maximum_deadline=2000010000, can_extend=True, can_edit_budget=True)
    mutations, errors = [], []
    workers = []
    with browser_api.sync_browser() as driver:
        browser = driver.chromium.launch(headless=True)
        try:
            page = browser.new_page(viewport={'width':viewport_width,'height':900})
            page.set_default_timeout(7000)
            page.add_init_script("localStorage.setItem('pbgui.sidebar.width', '"+str(sidebar_width)+"')")
            page.on('pageerror', lambda error: errors.append(str(error)))
            def respond(route):
                """Every API response is local synthetic data; no production mutations are possible."""
                path = urlparse(route.request.url).path
                if path in {'/audit', '/api/optimize-v8/main_page'}:
                    html = source.replace('navCurrent: "v8_optimize"', 'navCurrent: "dashboards"') if path == '/audit' else source
                    route.fulfill(body=html, content_type='text/html')
                elif path.startswith('/app/'):
                    asset = ROOT / 'frontend' / path.removeprefix('/app/')
                    route.fulfill(body=asset.read_bytes() if asset.is_file() else b'',
                        content_type='text/css' if asset.suffix=='.css' else 'text/javascript')
                elif path == '/api/optimize-v8/loops':
                    route.fulfill(json={'loops':[run]})
                elif path == '/api/optimize-v8/loops/configs':
                    route.fulfill(json={'configs':[]})
                elif path == '/api/optimize-v8/loops/options':
                    route.fulfill(json={'configs':['fixture'],'profiles':[],'providers':{},'presets':[]})
                elif path == '/api/optimize-v8/loops/'+loop_id+'/rental':
                    route.fulfill(json={'rental':rental})
                elif path.endswith('/rental/replace-preview'):
                    route.fulfill(json={'snapshot':{'generation':0}, 'snapshot_digest':'c'*64,
                        'operation_id':'d'*32, 'excluded_machine_ids':[101], 'attempts_used':1,
                        'max_runs':4, 'remaining_budget_usd':1.5, 'deadline':2000000000})
                elif path.endswith('/rental/replace'):
                    mutations.append(route.request.post_data_json)
                    run.update(status='replacing_gpu', control_generation=1,
                        gpu_replacement={'step':'deletion','reason':'Waiting for confirmed provider deletion'})
                    route.fulfill(status=202, json=run)
                elif path == '/api/help/index':
                    route.fulfill(json=[{'file':'43_pbv8_optimize','title':'PB8 Optimize'}])
                elif path == '/api/help/content':
                    route.fulfill(json={'content':'# PB8 Optimize\n\n## Replacing a GPU within the same AI Loop\nFixture guide.'})
                elif path == '/api/vast/fragment':
                    route.fulfill(body=(ROOT/'frontend/vast.html').read_text(), content_type='text/html')
                elif path == '/api/vast/jobs':
                    route.fulfill(json={'jobs':[],'workers':workers,'queue':{},'supervision_available':True})
                else:
                    route.fulfill(json={})
            page.route('**/*', respond)
            page.route_web_socket('**/*', lambda socket: socket.on_message(lambda _message: None))
            page.goto('http://pbgui.test/audit')
            page.locator('.nav-group-btn[data-group="pbv8"]').click()
            page.locator('.nav-item[data-page="v8_optimize"]').click()
            page.wait_for_url('**/api/optimize-v8/main_page')
            page.locator('.sb-section[data-panel="loops-queue"]').click()
            page.get_by_role('textbox', name='Search AI Loops queue').fill('Isolated')
            main = '#loops-queue-table tr[data-key="'+loop_id+'"]'
            page.locator(main+' td').nth(2).click()
            browser_api.expect(page.locator(main)).to_have_class('selected')
            page.locator('#loop-rental-minutes').fill('17')
            replace = page.locator('#loop-rental-replace')
            browser_api.expect(replace).to_be_enabled()
            replace.click()
            browser_api.expect(page.locator('#pbgui-dialog-title')).to_have_text('Replace GPU and continue loop?')
            message = page.locator('#pbgui-dialog-message').text_content()
            assert all(text in message for text in ('1/4','in-flight work is lost','$1.5000','101','Deadline:'))
            page.mouse.click(2,2)
            browser_api.expect(page.locator('#pbgui-dialog-ovl')).to_have_class('visible')
            page.locator('#pbgui-dialog-cancel').click()
            assert not mutations
            replace.click()
            page.locator('#pbgui-dialog-accept').click()
            browser_api.expect(page.locator('#loop-rental-status')).to_contain_text('Waiting for confirmed provider deletion')
            browser_api.expect(replace).to_be_disabled()
            assert len(mutations) == 1 and mutations[0]['authorization'] is True
            assert mutations[0]['worker_id'] == worker_id
            browser_api.expect(page.locator('#loop-rental-minutes')).to_have_value('17')
            browser_api.expect(page.locator(main)).to_have_class('selected')
            if viewport_width > 760:
                assert abs(page.locator('#sidebar').bounding_box()['width'] - sidebar_width) < 2
            assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
            before = page.url
            page.reload()
            browser_api.expect(page.locator('#loop-rental-status')).to_contain_text('Waiting for confirmed provider deletion')
            assert page.url == before
            browser_api.expect(page.get_by_role('textbox', name='Search AI Loops queue')).to_have_value('Isolated')
            browser_api.expect(page.locator('#loop-rental-minutes')).to_have_value('17')
            browser_api.expect(page.locator(main)).to_have_class('selected')
            page.locator('#pbgui-guide-btn').click()
            page.wait_for_selector('#pbgui-shared-help-ovl.visible')
            assert page.url == before
            page.locator('#pbgui-shared-help-close').click()
            run['gpu_replacement'].update(step='capacity', reason='Optimizer attempt allowance exhausted')
            browser_api.expect(page.locator('#loop-rental-status')).to_contain_text('Optimizer attempt allowance exhausted')
            assert len(mutations) == 1 and not errors
            workers.append(dict(id=worker_id, loop_id=loop_id, loop_owner='fixture', kind='worker',
                status='running', rental_state='active', instance_id=11, deadline=2000007200,
                budget_usd=2, price_hour_usd=.08, gpu_name='Fixture GPU', host_machine_id=101))
            page.locator('.sb-section[data-panel="queue"]').click()
            native_replace = page.get_by_role('button', name='Replace GPU and continue loop', exact=True)
            browser_api.expect(native_replace).to_be_visible()
            native_replace.click()
            browser_api.expect(page.locator('#pbgui-dialog-title')).to_have_text('Replace GPU and continue loop?')
            page.locator('#pbgui-dialog-cancel').click()
            page.get_by_role('button', name='End rental', exact=True).click()
            browser_api.expect(page.locator('#pbgui-dialog-message')).to_contain_text('The Loop can fail')
            page.locator('#pbgui-dialog-cancel').click()
            assert len(mutations) == 1 and not errors
        finally:
            browser.close()


def test_pb8_observer_progress_columns_and_automatic_updates():
    """Real menu/shell with isolated asymmetric reports, reload and failure states."""
    browser_api = __import__('browser_driver')
    source = (ROOT / 'frontend/v7_optimize.html').read_text()
    observer_asset = re.search(r'<script\s+src="(/app/js/pb8_loop_optimizer\.js\?v=\d+)"', source)
    assert observer_asset, 'The optimizer page must include the versioned observer script'
    for key, value in {'API_BASE':'/api/optimize-v8','OPTIMIZE_VERSION':'v8','VERSION':'test',
                       'SERIAL':'1','NAV_HASH':'test','BASE_PREFIX':'','LIMITS_META':'{}',
                       'BACKTEST_VERSION':'v8','OPTIMIZE_NAV_TITLE':'PB8 Optimize',
                       'OPTIMIZE_NAV_CURRENT':'v8_optimize'}.items():
        source = source.replace('%%'+key+'%%', value)
    run = {'id':'a'*32,'name':'Isolated report progress','status':'running','phase':'evaluate','round':0,
           'created_at':1000,'settings':{'config_name':'fixture','execution':'cpu'},'jobs':[],
           'history':[],'observer_enabled':True,'ended':False,'cycles':[{'round':0,'status':'validated',
           'ai_evaluated':True,'winner_candidate_id':'fixture_4','winner_operation':'training',
           'goals':[],'backtests':5,'completed_backtests':5}], 'observer_jobs':[], 'observer_summaries':[]}
    for kind,terminal in [('observer_holdout',2),('observer_full_range',4)]:
        run['observer_summaries'].append({'round':0,'kind':kind,'status':'waiting',
            'expected':5,'terminal':terminal,'successful':0 if kind=='observer_holdout' else 4,
            'report_pending':0,'report_failed':0,'candidate_id':'fixture_4','source_round':0})
        for index in range(5):
            run['observer_jobs'].append({'round':0,'kind':kind,'candidate_id':'fixture_'+str(index),
                'operation':kind+str(index),'status':('failed' if index==0 else 'skipped') if kind=='observer_holdout' and index<terminal else 'complete' if index<terminal else 'running',
                'report_done':index<terminal,'simulation_complete':kind=='observer_full_range' and index<terminal,
                'metrics':{'gain':7,'drawdown':.1}})
    requests, errors = [], []
    with browser_api.sync_browser() as driver:
        # Use the normal browser/action deadlines. The approved ten-second
        # limit applies to new Node helper subprocesses, not to browser startup.
        browser = driver.chromium.launch(headless=True)
        try:
            page = browser.new_page(viewport={'width':1200,'height':900})
            page.on('pageerror', lambda error: errors.append(str(error)))

            def respond(route):
                """Serve local assets and fixture JSON; block every real request."""
                path = urlparse(route.request.url).path
                requests.append(route.request.url)
                if path in {'/audit','/api/optimize-v8/main_page'}:
                    html = source.replace('navCurrent: "v8_optimize"', 'navCurrent: "dashboards"') if path == '/audit' else source
                    route.fulfill(body=html,content_type='text/html')
                elif path.startswith('/app/'):
                    asset = ROOT / 'frontend' / path.removeprefix('/app/')
                    route.fulfill(body=asset.read_bytes() if asset.is_file() else b'',content_type='text/css' if asset.suffix=='.css' else 'text/javascript')
                elif path == '/api/optimize-v8/loops':
                    route.fulfill(json={'loops':[run]})
                elif path == '/api/optimize-v8/loops/configs':
                    route.fulfill(json={'configs':[]})
                elif path == '/api/optimize-v8/loops/options':
                    route.fulfill(json={'configs':['fixture'],'profiles':[],'providers':{},'presets':[]})
                elif path == '/api/help/index':
                    route.fulfill(json=[{'file':'43_pbv8_optimize','title':'PB8 Optimize'}])
                elif path == '/api/help/content':
                    route.fulfill(json={'content':'# PB8 Optimize\n\nIsolated report guide.'})
                else:
                    route.fulfill(json={})

            page.route('**/*', respond)
            page.route_web_socket('**/*', lambda socket: socket.on_message(lambda _message: None))
            page.goto('http://pbgui.test/audit')
            page.locator('.nav-group-btn[data-group="pbv8"]').click()
            page.locator('.nav-item[data-page="v8_optimize"]').click()
            page.wait_for_url('**/api/optimize-v8/main_page')
            page.locator('.sb-section[data-panel="loops-queue"]').click()
            main = '#loops-queue-table tr[data-key="'+'a'*32+'"]'
            cycle = '#loops-queue-table tr[data-key="'+'a'*32+':0"]'
            page.locator(main+' .loop-disclosure').click()

            def assert_cells(holdout, full):
                """Wait on real polling rather than invoking renderer helpers."""
                page.evaluate("document.dispatchEvent(new Event('visibilitychange'))")
                for selector in (main,cycle):
                    try:
                        page.wait_for_function("([selector,holdout,full])=>{const row=document.querySelector(selector);return row&&row.cells[9].textContent.includes(holdout)&&row.cells[10].textContent.includes(full);}",arg=[selector,holdout,full])
                    except browser_api.TimeoutError:
                        cells = page.locator(selector+' td').all_text_contents()
                        message = page.locator('#loops-queue-message').text_content()
                        pytest.fail(f'Report cells did not reach {holdout!r} / {full!r}; '
                                    f'row={cells!r}; queue_message={message!r}; page_errors={errors!r}')

            assert_cells('Holdouts · 2/5 terminal · 0 complete','Full Time Range · 4/5 terminal · 4 complete')
            url = page.url
            page.reload()
            assert_cells('Holdouts · 2/5 terminal','Full Time Range · 4/5 terminal')
            assert page.url == url and page.locator(cycle).is_visible()
            winner = run['observer_jobs'][-1]
            winner.update(status='complete',report_done=True,simulation_complete=True)
            run['observer_summaries'][1].update(status='selected',operation=winner['operation'],terminal=5,successful=5)
            assert_cells('Holdouts · 2/5 terminal','Gain 7 ×')
            run['observer_summaries'][0]['status']='disabled'
            assert_cells('Holdout disabled','Gain 7 ×')
            winner['simulation_complete']=False
            run['observer_summaries'][1].update(status='unavailable',successful=3,report_failed=2)
            assert_cells('Holdout disabled','No complete Full Time Range · 5/5 terminal · 3 complete · 2 report errors')
            run.pop('observer_summaries')
            run['cycles'][0].update(ai_evaluated=False,score=1)
            run['observer_selections']=[{'round':0,'status':'waiting','completed':2,'expected':99}]
            assert_cells('Holdouts · 2/5 recorded terminal','No complete Full Time Range · 5/5 recorded terminal')
            run.pop('observer_selections')
            assert_cells('Holdouts · 2/5 recorded terminal','No complete Full Time Range · 5/5 recorded terminal')
            for width in (160,420):
                page.evaluate('(width)=>document.getElementById("sidebar").style.width=width+"px"',width)
                assert page.locator('#sidebar').evaluate('node=>node.scrollWidth<=node.clientWidth+1')
            for width in (700,1920):
                page.set_viewport_size({'width':width,'height':900})
                assert page.locator('#sidebar').evaluate('node=>node.scrollWidth<=node.clientWidth+1')
            assert 'http://pbgui.test' + observer_asset.group(1) in requests
            destination = page.url
            page.locator('#pbgui-guide-btn').click()
            page.wait_for_selector('#pbgui-shared-help-ovl.visible')
            assert page.url == destination
            page.locator('#pbgui-shared-help-close').click()
            assert not errors, errors
        finally:
            browser.close()


def test_pb8_loop_navigation_setup_polling_and_restore(viewport_width=1200, capability_only=False):
    """Exercise real menu navigation, dynamic settings and preserved draft inputs."""
    browser_api = __import__('browser_driver')
    source = (ROOT / 'frontend/v7_optimize.html').read_text()
    replacements = {'API_BASE': '/api/optimize-v8', 'OPTIMIZE_VERSION': 'v8', 'VERSION': 'test', 'SERIAL': '1', 'NAV_HASH': 'test', 'BASE_PREFIX':'', 'LIMITS_META':'{}', 'BACKTEST_VERSION':'v8', 'OPTIMIZE_NAV_TITLE':'PB8 Optimize', 'OPTIMIZE_NAV_CURRENT':'v8_optimize'}
    for key, value in replacements.items():
        source = source.replace('%%' + key + '%%', value)
    loops, starts, errors, diff_queries, continuations = [], [], [], [], []
    diff_fixture = {'delay':False, 'pending':[]}
    continuation_delay = {'enabled': False, 'pending': []}
    connection_fixture = {'fail': False, 'action_fail': False, 'unverified': False, 'action_requests': 0}
    knowledge_fixture = {'entries': [], 'fail': False}
    knowledge_queries = []
    instruction_versions = {'default':{'id':'default','name':'PBGui default','text':'Default Loop instructions','digest':'default-hash','created_at':None}}
    instruction_catalog = {'active':'default','revision':0}
    cloud_jobs, log_targets, log_subscriptions, native_status_requests = [], [], [], []
    definitions = {}
    rental_extensions, rental_budgets = [], []
    rental_fixture = dict(worker_id='f'*32, state='active', deadline=2000007200,
                          loop_deadline=2000000000, budget_usd=2, price_hour_usd=.08,
                          maximum_deadline=2000010000, pending=False, error=None, can_extend=True,
                          can_edit_budget=True, estimated_compute_usd=.15,
                          billing={'amount_usd':.1234,'reported_at':2000000000,'error':None})
    from api.vast import RentalPreferences
    rental_preferences = RentalPreferences(hours=12, budget=5, max_rentals=3, idle_seconds=300).model_dump()
    options = {'configs':['fixture','alternative'], 'providers': {'chatgpt': {'connected':True}}, 'profiles':[{'id':'default','name':'Default'}], 'gpu_available':True,'gpu_reason':'', 'vast_connected':True, 'vast':{'hours':12,'gpu_name':'RTX 3060','budget':5,'max_rentals':3}, 'presets':['gain','drawdown','uptrend']}
    with browser_api.sync_browser() as driver:
        browser = driver.chromium.launch(headless=True)
        page = browser.new_page(viewport={'width':viewport_width,'height':900})
        page.on('pageerror', lambda exc: errors.append(str(exc)))
        def respond(route):
            """Intercept every request; no provider, process or production data access."""
            path = urlparse(route.request.url).path
            if path in {'/audit', '/api/optimize-v8/main_page'}:
                html = source.replace('navCurrent: "v8_optimize"', 'navCurrent: "dashboards"') if path == '/audit' else source
                route.fulfill(body=html, content_type='text/html')
            elif path.startswith('/app/'):
                asset = ROOT / 'frontend' / path.removeprefix('/app/')
                route.fulfill(body=asset.read_bytes() if asset.is_file() else b'', content_type='text/css' if asset.suffix=='.css' else 'text/javascript')
            elif path == '/api/vast/fragment':
                route.fulfill(body=(ROOT / 'frontend/vast.html').read_text(),content_type='text/html')
            elif path == '/api/vast/gpu-preferences':
                if route.request.method == 'PATCH':
                    rental_preferences.update(json.loads(route.request.post_data))
                route.fulfill(json=rental_preferences)
            elif path == '/api/vast/jobs':
                route.fulfill(json={'jobs':cloud_jobs,'workers':[],'worker':None,'queue':{},'supervision_available':True})
            elif path == '/api/ai/preferences':
                route.fulfill(json={'drawer_width':460,'drawer_open':False,'drawer_pinned':False,'jev_max_cost_usd':.01})
            elif path == '/api/ai/status':
                route.fulfill(json={'providers':{'chatgpt':{'connected':True,'profiles':[{'id':'default','name':'Default'}]}}})
            elif path == '/api/ai/models':
                route.fulfill(json={'models':[{'id':'pinned','name':'Shared model','reasoning_variants':[{'id':'high','label':'High','type':'effort','value':'high'}],'service_tiers':[{'id':'priority','label':'Fast'}]}]})
            elif path == '/api/ai/conversations':
                route.fulfill(json={'conversations':[]})
            elif path == '/api/optimize-v8/loops/options':
                name = parse_qs(urlparse(route.request.url).query).get('config_name',['fixture'])[0]
                defaults = {'coins':{'long':['BTC','ETH'],'short':['ETH']},'ignored_coins':{'long':['DOGE'],'short':[]},
                            'direction':'both','positions':{'long':[2,4],'short':[3,3]},'total_positions':[5,7],'iters':4000}
                if name == 'alternative':
                    defaults = {'coins':{'long':[],'short':['SOL']},'ignored_coins':{'long':[],'short':[]},
                                'direction':'short','positions':{'long':[0,0],'short':[6,6]},'total_positions':[6,6],'iters':8000}
                route.fulfill(json={**options,'config_name':name,'config_defaults':defaults})

            elif path == '/api/optimize-v8/loops/models':
                route.fulfill(json={'models':[{'id':'pinned','name':'Pinned model'}]})
            elif path == '/api/optimize-v8/loops/instructions':
                if route.request.method == 'POST':
                    payload=json.loads(route.request.post_data)
                    assert payload['revision']==instruction_catalog['revision']
                    identifier=str(instruction_catalog['revision']+1)*32
                    version={'id':identifier,'name':payload['name'],'text':payload['text'],'digest':identifier,'created_at':1000}
                    instruction_versions[identifier]=version
                    instruction_catalog.update(active=identifier,revision=instruction_catalog['revision']+1)
                    route.fulfill(json=version,status=201)
                else:
                    route.fulfill(json={**instruction_catalog,'versions':[{key:value for key,value in item.items() if key!='text'} for item in instruction_versions.values()]})
            elif path == '/api/optimize-v8/loops/instructions/active':
                payload=json.loads(route.request.post_data)
                assert payload['revision']==instruction_catalog['revision']
                instruction_catalog.update(active=payload['version_id'],revision=instruction_catalog['revision']+1)
                route.fulfill(json=instruction_versions[payload['version_id']])
            elif path.startswith('/api/optimize-v8/loops/instructions/'):
                identifier=path.rsplit('/',1)[1]
                if route.request.method=='DELETE':
                    assert identifier!='default'
                    assert route.request.post_data_json['revision']==instruction_catalog['revision']
                    del instruction_versions[identifier]
                    instruction_catalog['revision']+=1
                    if instruction_catalog['active']==identifier:
                        instruction_catalog['active']='default'
                    route.fulfill(json=instruction_catalog)
                else:
                    route.fulfill(json=instruction_versions[identifier])
            elif path.endswith('/instructions') and path.startswith('/api/optimize-v8/loops/'):
                route.fulfill(json={**instruction_versions['default'],'text':'Frozen instructions of this run','legacy':False})
            elif path == '/api/optimize-v8/loops/configs':
                route.fulfill(json={'configs':list(definitions.values())})
            elif path == '/api/optimize-v8/loops/knowledge':
                knowledge_queries.append(parse_qs(urlparse(route.request.url).query))
                route.fulfill(json={'detail':'Knowledge temporarily unavailable'} if knowledge_fixture['fail'] else {'entries':knowledge_fixture['entries'],'used_holdouts':[]},status=503 if knowledge_fixture['fail'] else 200)
            elif path == '/api/optimize-v8/loops/sources':
                request=json.loads(route.request.post_data)
                saved=next((row for row in loops if row['id']==request['id']),None) if request['kind']=='run' else None
                name=saved['settings']['config_name'] if saved else 'fixture'
                defaults={'coins':{'long':['BTC','ETH'],'short':['ETH']},'ignored_coins':{},'direction':'both','positions':{'long':[2,4],'short':[3,3]},'total_positions':[5,7],'iters':4000}
                route.fulfill(json={'config_name':name,'source':request,'settings':saved['settings'] if saved else None,'execution':saved['settings']['execution'] if saved else 'vast','config_defaults':defaults})
            elif path.startswith('/api/optimize-v8/loops/configs/'):
                from urllib.parse import unquote
                name=unquote(path.split('/configs/')[1].removesuffix('/queue'))
                if route.request.method=='PUT':
                    payload=json.loads(route.request.post_data)
                    assert 'provider' not in payload['settings'] and 'model' not in payload['settings']
                    from api.loop_optimizer_v8 import LoopDefinitionSave, LoopStart
                    LoopDefinitionSave.model_validate(payload)
                    LoopStart.model_validate(dict(payload['settings'],provider='chatgpt',model='shared'))
                    definitions[name]={'name':name,'settings':payload['settings'],'source':payload['source'],'revision':definitions.get(name,{}).get('revision',0)+1,'created_at':1790805652,'updated_at':1790805652,'config_defaults':options.get('config_defaults')}
                    route.fulfill(json=definitions[name])
                elif path.endswith('/queue'):
                    payload=dict(definitions[name]['settings'],**json.loads(route.request.post_data));starts.append(payload)
                    row={'id':('a' if len(starts)==1 else 'c')*32,'name':name,'definition_name':name,'created_at':1790805652,'updated_at':1790805652,'status':'queued','phase':'interpret','round':0,'settings':payload,'jobs':[],'ai_calls':0,'ai_tokens_reserved':0,'ai_usd_reserved':0,'jev_usd_reserved':0,'validation_count':0,'holdout_used':False,'deadline':None,'fingerprint':{'pb8':'commit','data_status':'verified_files'},'best':None,'history':[],'rubric':[],'ended':False,'usd_status':'subscription'}
                    row['deletable']=True
                    loops.append(row);route.fulfill(json=row)
                elif route.request.method=='DELETE':
                    definitions.pop(name);route.fulfill(json={'deleted':True})
            elif path.endswith('/ai-preview') and path.startswith('/api/optimize-v8/loops/'):
                if connection_fixture['unverified']:
                    route.fulfill(status=409, json={'detail': {'reason': 'context_budget',
                        'message': 'Model context must be known and at least 256k'}})
                    return
                preview={'paid':False,'context':256000,'context_pending':connection_fixture.get('stale_context',False),'native_archive':{'effective':100},'maximum_decisions':42}
                next(row for row in loops if row['id']==path.split('/')[-2])['ai_preview']=preview
                route.fulfill(json=preview)
            elif path.endswith('/action') and path.startswith('/api/optimize-v8/loops/'):
                connection_fixture['action_requests'] += 1
                if connection_fixture['action_fail']:
                    route.fulfill(status=409,json={'detail':'Fixture action rejected'})
                    return
                payload=json.loads(route.request.post_data);row=next(row for row in loops if row['id']==path.split('/')[-2])
                row.update(status={'start':'running','pause':'paused','resume':'running','stop':'stopped'}[payload['action']],phase='optimize',deadline=2000000000)
                row['deletable']=False
                route.fulfill(json=row)
            elif path.endswith('/continue') and path.startswith('/api/optimize-v8/loops/'):
                payload=json.loads(route.request.post_data);continuations.append(payload)
                parent=next(row for row in loops if row['id']==path.split('/')[-2])
                child=copy.deepcopy(parent);child.update(id='e'*32, name='Continued fixture', status='running', ended=False, phase='baseline', jobs=[], cycles=[], best=None)
                child['settings']['max_runs']=payload['rounds'];child['continuation']={'parent':parent['id'],'round':payload['round']}
                if continuation_delay['enabled']:
                    continuation_delay['pending'].append((route, child))
                else:
                    loops.append(child);route.fulfill(json=child,status=201)
            elif path.endswith('/rental/budget') and path.startswith('/api/optimize-v8/loops/'):
                payload=json.loads(route.request.post_data)
                rental_budgets.append(payload)
                rental_fixture['pending']=True
                route.fulfill(json={'pending':True},status=202)
            elif path.endswith('/rental/charges') and path.startswith('/api/optimize-v8/loops/'):
                route.fulfill(json={'worker_id':rental_fixture['worker_id'],'billing':rental_fixture['billing']})
            elif path.endswith('/rental/extend') and path.startswith('/api/optimize-v8/loops/'):
                payload=json.loads(route.request.post_data)
                rental_extensions.append(payload)
                rental_fixture['pending']=True
                route.fulfill(json={'pending':True},status=202)
            elif path.endswith('/rental') and path.startswith('/api/optimize-v8/loops/'):
                route.fulfill(json={'rental':rental_fixture})
            elif path.endswith('/diff') and path.startswith('/api/optimize-v8/loops/'):
                query=parse_qs(urlparse(route.request.url).query);diff_queries.append(query)
                payload={'fields':[{'path':'optimize.limits.2.value','before':None,'after':.25}] +
                         [{'path':'fixture.parameter_'+str(index),'before':index,'after':index+1} for index in range(80)],'truncated':False}
                if diff_fixture['delay']:
                    diff_fixture['pending'].append((route,payload))
                else:
                    route.fulfill(json=payload)
            elif path.endswith('/log-target') and path.startswith('/api/optimize-v8/loops/'):
                operation=parse_qs(urlparse(route.request.url).query)['operation'][0]
                log_targets.append(operation)
                route.fulfill(json={'id':'d'*32 if operation=='2'*32 else '11111111-1111-1111-1111-111111111111','name':'Native optimizer' if operation in {'1'*32,'2'*32} else 'Specific backtest','kind':'optimizer' if operation in {'1'*32,'2'*32} else 'validation','cloud':operation=='2'*32})
            elif path.startswith('/api/optimize-v8/queue/') and path.endswith('/status'):
                native_status_requests.append(path.split('/')[-2])
                route.fulfill(json={'name':'Native optimizer','phase':'running','progress':{},'runtime':{},'queue':{},'log':{}})
            elif path == '/api/optimize-v8/loops':
                if connection_fixture['fail']:
                    route.abort('failed')
                    return
                route.fulfill(json={'loops':loops})
            elif path.startswith('/api/optimize-v8/loops/') and route.request.method=='DELETE':
                loop_id=path.split('/')[-1]
                row=next(row for row in loops if row['id']==loop_id)
                assert row['deletable']
                loops.remove(row);route.fulfill(json={'deleted':True})
            elif path == '/api/optimize-v8/metadata':
                route.fulfill(json={'runtime_ready':False,'runtime_message':'Isolated fixture','optimize_defaults':{},'bot_defaults':{},'backtest_defaults':{},'live_defaults':{},'optimize_bounds_defaults':{}})
            elif path == '/api/optimize-v8/configs':
                route.fulfill(json={'configs':[{'name':'fixture','exchange':'binance','modified':'2026-10-01'}]})
            elif path == '/api/optimize-v8/queue':
                route.fulfill(json={'queue':[]})
            elif path == '/api/optimize-v8/results':
                route.fulfill(json={'results':[]})
            elif path == '/api/help/index':
                route.fulfill(json=[{'file':'43_pbv8_optimize','title':'PB8 Optimize'}])
            elif path == '/api/help/content':
                route.fulfill(json={'content':'# PB8 Optimize\n\nAI Loops fixture guide.'})
            else:
                route.fulfill(json={})
        page.route('**/*', respond)
        def connect_log(socket):
            """Intercept log streams and provide fixture output without any live server."""
            def receive(raw):
                """Serve the requested native file and retain its actual identity."""
                message=json.loads(raw)
                if message.get('cmd')=='list_local_logs':
                    socket.send(json.dumps({'type':'local_logs_list','files':['optimizes_v8/vast_'+'d'*32+'.log','optimizes_v8/11111111-1111-1111-1111-111111111111.log','backtests_v8/11111111-1111-1111-1111-111111111111.log','backtests_v8/unrelated.log']}))
                elif message.get('cmd') in {'subscribe_local_logs','get_local_logs'}:
                    log_subscriptions.append(message['file'])
                    socket.send(json.dumps({'type':'local_logs','file':message['file'],'sid':message['sid'],'streaming':True,'lines':['Native optimizer fixture output']}))
            socket.on_message(receive)
        page.route_web_socket('**/*',connect_log)
        page.goto('http://pbgui.test/audit')
        page.locator('.nav-group-btn[data-group="pbv8"]').click()
        page.locator('.nav-item[data-page="v8_optimize"]').click()
        page.wait_for_url('**/api/optimize-v8/main_page')
        page.wait_for_timeout(300)
        assert not errors, errors
        assert page.evaluate('()=>({version:window.OPTIMIZE_VERSION,adapter:window.optimizeEditorAdapter&&optimizeEditorAdapter.isV8,loop:!!window.PBGuiLoopOptimizer})')['loop']
        page.wait_for_function('!!window.PBGuiVast')
        page.locator('#nav-vast').click()
        assert page.locator('#vast-settings-nav').is_visible()
        page.locator('[data-vast-view=rental]').click()
        assert page.locator('#vast-rental').is_visible()
        page.reload()
        page.wait_for_function('!!window.PBGuiVast')
        assert page.url.endswith('#vast-rental')
        assert page.locator('#vast-rental').is_visible()
        assert page.locator('#nav-vast').evaluate('(button)=>button.classList.contains("active")')
        page.locator('.sb-section[data-panel="queue"]').click()
        assert page.locator('#vast-settings-nav').is_hidden()
        assert page.locator('#nav-vast').is_visible()
        # Real sidebar instruction editor preserves text/focus during polling and saved version on reload.
        instruction_nav=page.locator('.sb-section[data-panel="loops-instructions"]')
        instruction_nav.click()
        page.wait_for_function("document.querySelector('#loop-instruction-text').value==='Default Loop instructions'")
        assert page.url.endswith('#loops-instructions')
        prompt=page.locator('#loop-instruction-text')
        save_prompt=page.get_by_role('button',name='Save & use new version',exact=True)
        delete_prompt=page.get_by_role('button',name='Delete selected version',exact=True)
        assert prompt.get_attribute('readonly') is not None
        assert save_prompt.is_disabled() and delete_prompt.is_disabled()
        assert float(save_prompt.evaluate('node=>getComputedStyle(node).opacity')) < 1
        page.locator('#loop-instruction-name').fill('PBGui default')
        assert save_prompt.is_disabled() and prompt.get_attribute('readonly') is not None
        page.locator('#loop-instruction-name').fill('My targeted search')
        assert save_prompt.is_enabled() and prompt.get_attribute('readonly') is None
        prompt.fill('Change bounds deliberately. <script>unsafe()</script>')
        prompt.focus()
        page.evaluate("document.dispatchEvent(new Event('visibilitychange'))")
        page.wait_for_timeout(300)
        assert prompt.input_value()=='Change bounds deliberately. <script>unsafe()</script>'
        assert prompt.evaluate('node=>document.activeElement===node')
        assert page.locator('#panel-loops-instructions script').count()==0
        page.get_by_role('button',name='Save & use new version',exact=True).click()
        page.wait_for_function("document.querySelector('#loop-instruction-version').value==='"+'1'*32+"'")
        assert instruction_catalog['active']=='1'*32
        page.reload()
        page.wait_for_function("document.querySelector('#panel-loops-instructions').classList.contains('active') && document.querySelector('#loop-instruction-text').value.includes('Change bounds deliberately')")
        page.locator('#loop-instruction-version').select_option('default')
        page.wait_for_function("document.querySelector('#loop-instruction-text').value==='Default Loop instructions'")
        page.get_by_role('button',name='Use selected version',exact=True).click()
        page.wait_for_function("document.querySelector('#panel-loops-instructions .muted-line').textContent.includes('Active for new runs: PBGui default')")
        assert instruction_versions['1'*32]['text'].startswith('Change bounds')
        page.locator('#loop-instruction-name').fill(' my targeted SEARCH ')
        assert save_prompt.is_disabled()
        page.locator('#loop-instruction-name').fill('')
        page.locator('#loop-instruction-version').select_option('1'*32)
        page.wait_for_function("document.querySelector('#loop-instruction-text').value.includes('Change bounds')")
        assert save_prompt.is_disabled() and delete_prompt.is_enabled()
        page.get_by_role('button',name='Use selected version',exact=True).click()
        page.wait_for_function("document.querySelector('#panel-loops-instructions .muted-line').textContent.includes('Active for new runs: My targeted search')")
        delete_prompt.click()
        page.wait_for_selector('#pbgui-dialog-ovl.visible')
        assert 'New runs will use PBGui default' in page.locator('#pbgui-dialog-body').inner_text()
        page.locator('#pbgui-dialog-ovl').click(position={'x':2,'y':2})
        assert page.locator('#pbgui-dialog-ovl').is_visible()
        page.locator('#pbgui-dialog-box').get_by_role('button',name='Cancel',exact=True).click()
        assert '1'*32 in instruction_versions
        delete_prompt.click()
        page.locator('#pbgui-dialog-box').get_by_role('button',name='Delete',exact=True).click()
        page.wait_for_function("document.querySelector('#loop-instruction-version').value==='default' && document.querySelector('#loop-instruction-text').value==='Default Loop instructions'")
        assert '1'*32 not in instruction_versions and instruction_catalog['active']=='default'
        assert delete_prompt.is_disabled() and save_prompt.is_disabled()
        destination=page.url
        page.locator('#pbgui-guide-btn').click()
        page.wait_for_selector('#pbgui-shared-help-ovl.visible')
        assert page.url==destination
        page.locator('#pbgui-shared-help-close').click()
        # Central findings are a real sidebar destination, restored on browser reload.
        knowledge_nav=page.locator('.sb-section[data-panel="loops-knowledge"]')
        knowledge_nav.click()
        page.wait_for_function("document.querySelector('#loop-knowledge-list').textContent.includes('No saved findings yet.')")
        assert page.url.endswith('#loops-knowledge')
        assert not page.locator('#loop-report-overlay').evaluate("node=>node.classList.contains('is-open')")
        assert knowledge_nav.evaluate('node=>node.scrollWidth<=node.clientWidth')
        knowledge_fixture['entries'].append({'id':'a'*32+':round:0','loop':'a'*32,'knowledge':'Lower exposure reduced drawdown.','hypothesis':'Compare sizing bounds.','status':'observed_exact','created_at':1000,'coins':['HYPE'],'fingerprint':{'pb8':'fixture-revision'},'comparison':{'start_date':'2020-01-01','end_date':'2020-02-01'},'observations':[{'operation':'3'*32,'assessment':{'score':1.25,'hard_targets_met':True}}]})
        page.evaluate("document.dispatchEvent(new Event('visibilitychange'))")
        page.wait_for_function("document.querySelector('#loop-knowledge-list').textContent.includes('Lower exposure reduced drawdown.')")
        learned=page.locator('.loop-knowledge-entry').first
        learned.get_by_text('Evidence and source',exact=True).click()
        assert 'fixture-revision' in learned.inner_text() and '1.25' in learned.inner_text()
        learned.evaluate('node=>window.learnedCard=node')
        search=page.get_by_role('searchbox',name='Search learned findings')
        search.fill('exposure')
        search.evaluate('node=>window.knowledgeSearch=node')
        knowledge_fixture['entries'].append({'id':'b'*32+':bootstrap','loop':'b'*32,'knowledge':'Recovery remains uncertain. <img src=x onerror=alert(1)>','status':'historical_optimizer_guidance','created_at':2000,'coins':[]})
        page.evaluate("document.dispatchEvent(new Event('visibilitychange'))")
        page.wait_for_function("document.querySelector('#loop-nav-count-knowledge').textContent==='2'")
        assert search.evaluate('node=>node===window.knowledgeSearch && node===document.activeElement')
        assert learned.evaluate('node=>node===window.learnedCard')
        assert learned.locator('details').first.get_attribute('open') is not None
        page.reload()
        page.wait_for_function("document.querySelector('#panel-loops-knowledge').classList.contains('active') && document.querySelector('.loop-knowledge-entry')")
        assert search.input_value()=='exposure'
        assert page.locator('.loop-knowledge-entry details').first.get_attribute('open') is not None
        search.fill('')
        assert page.locator('.loop-knowledge-entry').count()==2
        assert page.locator('.loop-knowledge-entry img').count()==0
        knowledge_fixture['fail']=True
        page.evaluate("document.dispatchEvent(new Event('visibilitychange'))")
        page.wait_for_function("document.querySelector('#loop-knowledge-status').textContent.includes('Knowledge temporarily unavailable')")
        assert page.locator('.loop-knowledge-entry').count()==2
        knowledge_fixture['fail']=False
        page.evaluate("document.dispatchEvent(new Event('visibilitychange'))")
        page.wait_for_function("document.querySelector('#loop-knowledge-status').textContent===''")
        assert knowledge_queries and all(query.get('limit')==['200'] for query in knowledge_queries)
        assert page.locator('#panel-loops-config').get_by_text('Saved knowledge for future loops',exact=True).count()==0
        page.locator('.sb-section[data-panel="configs"]').click()
        # Enter through the ordinary PB8 Config table and its standard sidebar action.
        page.locator('#configs-tbody tr[data-name="fixture"]').click()
        page.locator('[data-create-ai-loop="configs"]').click()
        page.wait_for_function("document.querySelector('#loop-form') && !document.querySelector('#loop-form').hidden")
        assert page.url.endswith('#loops-config')
        assert not definitions and not starts, 'Create AI Loop only opens a draft'
        assert not page.locator('#loop-strategy').is_checked()
        page.locator('#loop-strategy').check()
        assert page.locator('#loop-execution').input_value()=='vast'
        page.locator('#loop-name').fill('My named loop')
        # A normal New action opens an editable source config rather than a snapshot selector.
        page.locator('#loop-new-config').click()
        page.wait_for_function("!document.querySelector('#loop-form').hidden && document.querySelector('#loop-name').value==='' && !document.querySelector('#loop-config').disabled")
        assert not page.locator('#loop-strategy').is_checked()
        page.locator('#loop-strategy').check()
        page.locator('#loop-name').fill('My named loop')
        assert page.locator('.loop-section > h3').all_text_contents() == ['Starting configuration', 'Coins and positions', 'Result goals', 'Run limits', 'AI evidence and limits']
        layout = page.evaluate("""()=>{
          const panel=document.getElementById('panel-loops-config'), form=panel.querySelector('form');
          return {width:form.clientWidth,available:panel.clientWidth-parseFloat(getComputedStyle(panel).paddingRight),overflow:panel.scrollWidth-panel.clientWidth,columns:getComputedStyle(panel.querySelector('.loop-grid')).gridTemplateColumns.split(' ').length};
        }""")
        assert abs(layout['width'] - layout['available']) <= 2
        assert layout['overflow'] <= 1
        if viewport_width == 1920:
            assert layout['width'] > 1500 and layout['columns'] == 8
        assert page.locator('#loop-model, #loop-provider, #loop-calls, #loop-budget, #loop-jev, .loop-form .loop-advanced').count() == 0
        assert page.locator('#loop-trades-target').is_disabled()
        page.locator('#loop-trades').check()
        assert page.locator('#loop-trades-target').is_enabled()
        page.locator('#loop-trades').uncheck()
        assert page.locator('#loop-hsl-mode').input_value() == 'auto'
        assert page.locator('#loop-hsl-mode option').all_text_contents() == ['Off', 'On', 'Auto · AI decides']
        page.locator('#loop-hsl-mode').select_option('off')
        if viewport_width == 1920:
            tops = page.locator('#loop-coins, #loop-direction, #loop-positions').evaluate_all('nodes=>nodes.map(node=>node.getBoundingClientRect().top)')
            assert max(tops) - min(tops) <= 1
        assert page.locator('#loop-direction').input_value() == 'config'
        assert page.locator('#loop-direction option').all_text_contents() == ['Use config · Long and short', 'Long and short', 'Long', 'Short']
        assert page.locator('#loop-position-split').is_hidden()
        assert 'Long: BTC, ETH' in page.locator('#loop-coins-config').inner_text()
        assert 'Short: ETH' in page.locator('#loop-coins-config').inner_text()
        assert 'Ignored: Long: DOGE' in page.locator('#loop-coins-config').inner_text()
        assert page.locator('#loop-coins').input_value() == ''
        assert page.locator('#loop-positions').input_value() == ''
        assert page.locator('#loop-positions').get_attribute('placeholder') == 'From config: 5–7'
        assert '4,000 iters' in page.locator('#loop-run-mode option[value=config]').inner_text()
        page.locator('#loop-config').select_option('alternative')
        page.wait_for_function("document.getElementById('loop-coins-config').textContent.includes('Short: SOL')")
        assert page.locator('#loop-direction option[value=config]').inner_text() == 'Use config · Short'
        assert page.locator('#loop-positions').get_attribute('placeholder') == 'From config: 6'
        page.locator('#loop-coins').fill('BTC')
        page.locator('#loop-positions').fill('4')
        page.locator('#loop-config').select_option('fixture')
        page.wait_for_function("document.getElementById('loop-direction').options[0].textContent === 'Use config · Long and short'")
        assert page.locator('#loop-coins').input_value() == 'BTC'
        assert page.locator('#loop-positions').input_value() == '4'
        assert page.locator('#loop-coins-config').is_hidden()
        page.locator('#loop-coins').fill('')
        page.locator('#loop-positions').fill('')
        assert page.locator('#loop-coins-config').is_visible()
        assert page.locator('#loop-positions-config').is_visible()
        page.locator('#loop-direction').select_option('long')

        assert page.locator('#loop-position-split').is_hidden()
        page.locator('#loop-direction').select_option('both')
        assert page.locator('#loop-position-split').is_visible()
        page.locator('#loop-coins').fill('BTC, ETH')
        page.locator('#loop-positions').fill('4')
        page.locator('#loop-long').fill('3'); page.locator('#loop-short').fill('1')
        assert page.locator('#loop-run-mode').input_value() == 'config'
        assert page.locator('#loop-run-hours').is_hidden()
        assert page.locator('#loop-run-mode option[value="proxy"]').is_disabled()
        page.locator('#loop-run-mode').select_option('iters')
        assert page.locator('#loop-run-iters').is_visible()
        page.locator('#loop-run-iters').fill('500')
        page.locator('#loop-execution').select_option('gpu')
        page.locator('#loop-run-mode').select_option('proxy')
        assert page.locator('#loop-run-proxy').is_visible()
        page.locator('#loop-run-proxy').fill('1000')
        page.locator('#loop-run-mode').select_option('hours')
        page.locator('#loop-run-hours').fill('.25')
        assert page.locator('#loop-hours').input_value() == '12'
        page.locator('#loop-hours').fill('24')
        page.locator('#loop-execution').select_option('vast')
        assert page.locator('#loop-hours').input_value() == '12'
        assert page.locator('#loop-hours').get_attribute('max') == '12'
        page.locator('#loop-hours').fill('24')
        assert not page.locator('#loop-hours').evaluate('(node)=>node.checkValidity()')
        page.locator('#loop-hours').fill('8')
        page.locator('#loop-rounds').fill('5')
        page.locator('#loop-parallel').fill('3')
        page.locator('#loop-goals').fill('Prefer stable equity')
        if viewport_width == 700:
            shared = page.evaluate('async()=>await PBGuiAI.ensureSelection()')
            assert shared['model'] == 'pinned'
            assert page.locator('#pbgui-ai-drawer').get_attribute('aria-hidden') == 'true'
        page.locator('#pbgui-ai-btn').click()
        page.wait_for_function("document.querySelector('#pai-model')?.value === 'pinned'")
        page.locator('#pai-effort').select_option('high')
        page.locator('#pai-speed').select_option('priority')
        page.get_by_role('button', name='Collapse AI assistant').click()
        page.locator('#loop-start').click()
        page.wait_for_function("document.querySelector('#loops-queue-table tr[data-key]')")
        assert len(starts)==1 and starts[0]['execution']=='vast'
        assert starts[0]['hsl_mode']=='off'
        assert starts[0]['model']=='pinned' and starts[0]['effort']=='high' and starts[0]['service_tier']=='priority'
        assert page.url.endswith('#loops-queue')
        assert loops[0]['status']=='queued' and loops[0]['ai_calls']==0
        if capability_only:
            connection_fixture['unverified'] = True
            page.locator('#loop-queue-start').click()
            page.wait_for_function("document.querySelector('#loops-queue-message').textContent.includes('at least 256k')")
            assert connection_fixture['action_requests'] == 0
            assert loops[0]['status'] == 'queued' and loops[0]['ai_calls'] == 0 and not loops[0]['jobs']
            assert page.url.endswith('#loops-queue')
            assert page.locator('#loop-queue-start').is_enabled()
            connection_fixture['unverified'] = False
            connection_fixture['stale_context'] = True
            page.locator('#loops-queue-table tr[data-key]').first.get_by_role('button', name='Open', exact=True).click()
            page.locator('#loop-report-body').get_by_role('button', name='Preview AI limits', exact=True).click()
            page.wait_for_function("document.querySelector('#loop-report-body').textContent.includes('Reported by normal response')")
            assert connection_fixture['action_requests'] == 0 and loops[0]['status'] == 'queued'
            assert not errors, errors
            browser.close()
            return
        # Simulate an API restart: keep rows/navigation and clear only the
        # polling failure when the automatic next request succeeds.
        visible_queue_rows = page.locator('#loops-queue-table tr[data-key]').count()
        connection_fixture['fail'] = True
        page.wait_for_function("document.querySelector('#loops-queue-message').textContent.includes('Retrying automatically')")
        assert page.locator('#loops-queue-table tr[data-key]').count() == visible_queue_rows
        assert page.url.endswith('#loops-queue')
        connection_fixture['fail'] = False
        page.wait_for_function("document.querySelector('#loops-queue-message').textContent === ''")
        # Successful background polling must retain a separate action failure.
        connection_fixture['action_fail'] = True
        page.locator('#loop-queue-start').click()
        page.wait_for_function("document.querySelector('#loops-queue-message').textContent === 'Fixture action rejected'")
        page.wait_for_timeout(3300)
        assert page.locator('#loops-queue-message').inner_text() == 'Fixture action rejected'
        connection_fixture['action_fail'] = False
        knowledge_nav.click()
        source_card=page.locator('.loop-knowledge-entry').filter(has_text='Lower exposure reduced drawdown.')
        page.wait_for_function("document.querySelector('#panel-loops-knowledge').classList.contains('active') && document.querySelector('#loop-knowledge-list').textContent.includes('My named loop')")
        search.fill('My named loop')
        assert page.locator('.loop-knowledge-entry').count()==1
        search.fill('')
        source_card.get_by_role('button',name='Open run',exact=True).click()
        assert page.url.endswith('#loops-queue') and 'loop_id='+('a'*32) in page.url
        assert page.locator('#loop-report-title').inner_text()=='Loop details'
        page.locator('#loop-report-close').click()
        assert page.locator('#loop-queue-delete').is_enabled()
        assert page.locator('#loops-queue-table tr[data-key]').first.get_by_role('button',name='Log',exact=True).is_disabled()
        page.locator('#loops-queue-table tr[data-key]').first.click()
        page.locator('#loop-queue-start').click()
        page.wait_for_function("document.querySelector('#loops-queue-table').textContent.includes('running')")
        assert page.locator('#loop-queue-delete').is_disabled()
        page.wait_for_function("document.querySelector('#loop-rental-status').textContent.includes('Budget $2.00')")
        assert 'Spent on this rental: $0.1234' in page.locator('#loop-rental-cost').inner_text()
        budget=page.locator('#loop-rental-budget')
        budget.fill('3')
        budget.focus()
        page.wait_for_timeout(3300)
        assert budget.input_value()=='3' and budget.evaluate('(node)=>node===document.activeElement')
        page.locator('#loop-rental-budget-save').click()
        page.wait_for_function("document.querySelector('#loop-rental-status').textContent.includes('Awaiting worker confirmation')")
        assert page.locator('#loop-rental-budget-save').is_disabled()
        assert rental_budgets[-1]==dict(worker_id='f'*32,expected_deadline=2000007200,expected_budget_usd=2,budget_usd=3)
        assert 'Budget $2.00' in page.locator('#loop-rental-status').inner_text()
        rental_fixture.update(pending=False,budget_usd=3)
        page.wait_for_function("document.querySelector('#loop-rental-status').textContent.includes('Budget $3.00')")
        assert budget.input_value()=='3'
        minutes=page.locator('#loop-rental-minutes')
        minutes.fill('90')
        minutes.focus()
        page.wait_for_timeout(3300)
        assert minutes.input_value()=='90'
        assert minutes.evaluate('(node)=>node===document.activeElement')
        assert page.locator('#loop-rental-extend').inner_text()=='Extend loop'
        page.locator('#loop-rental-extend').click()
        page.wait_for_function("document.querySelector('#loop-rental-status').textContent.includes('Awaiting worker confirmation')")
        assert page.locator('#loop-rental-extend').is_disabled()
        assert rental_extensions[-1]==dict(worker_id='f'*32,expected_deadline=2000007200,
                                           expected_loop_deadline=2000000000,minutes=90)
        rental_fixture.update(pending=False,loop_deadline=2000005400)
        page.wait_for_function("!document.querySelector('#loop-rental-extend').disabled")
        assert minutes.input_value()=='90'
        assert page.locator('#loop-rental-extend').inner_text()=='Extend rental & loop'
        assert page.locator('#loop-rental').evaluate('(node)=>node.getBoundingClientRect().right<=innerWidth')
        loops[0]['bootstrap']={'status':'applied','candidate_count':1,'sources':['saved_vast_run'],
                              'reason':'Historical assessment <script>unsafe()</script>','knowledge':'Requires exact validation'}
        first,second,check1,check2=[digit*32 for digit in ('1','2','3','4')]
        loops[0]['jobs']=[
            {'operation':first,'name':'run_first','kind':'optimizer','round':0,'status':'completed','started':True,'reason':'Initial run','changes':[],'native_iters':10000000,'created_at':1000,'run_started_at':1010,'ended_at':1050},
            {'operation':second,'name':'run_second','kind':'optimizer','round':1,'status':'completed','started':True,'reason':'Reduce recovery duration <script>unsafe()</script>',
             'native_iters':10000000,'changes':[{'path':'optimize.limits.2.value','before':None,'after':.25}],'created_at':1100,'run_started_at':1110,'ended_at':1150},
            {'operation':check1,'name':'validate_first','kind':'validation','round':0,'status':'completed','candidate_optimizer':first},
            {'operation':check2,'name':'validate_second','kind':'validation','round':1,'status':'completed','candidate_optimizer':second}]
        loops[0]['initial_snapshot_digest']='initial-content'
        for job in loops[0]['jobs']:
            if job['kind']=='optimizer':
                job['snapshot_digest']=job['operation']+'-content'
        loops[0]['validation_count']=2
        for job in loops[0]['jobs']:
            job['log_available']=True
        loops[0]['observer_enabled']=True
        loops[0]['observer_jobs']=[{
            'operation':digit*32,'name':'user_evaluation_'+digit,'kind':kind,'round':number,
            'status':'complete','display_status':'complete','simulation_complete':True,'report_done':True,
            'selected_for_full_range':True,
            'log_available':True,'metrics':{'gain':2+number,'drawdown':.3,'recovery_days':.25,'adg':.01},
            'delta_start':{'gain':number+1,'drawdown':-.1},'delta_previous':{'gain':1,'drawdown':-.1},
            'created_at':1000,'run_started_at':1010,'ended_at':1050}
            for number,digit,kind in [(-1,'6','observer_holdout'),(0,'7','observer_holdout'),
                                       (1,'8','observer_holdout'),(1,'9','observer_full_range')]]
        loops[0]['observer_jobs'][-1].update(status='running',display_status='running',simulation_complete=False,
                                             report_done=False,metrics={},ended_at=None)
        cloud_jobs.append({'id':'d'*32,'kind':'optimizer','loop_id':loops[0]['id'],'config_name':'Native optimizer',
                           'status':'running','has_log':True,'has_provider_log':False,'iterations':1000,'workers':2,
                           'exact_completed':5,'gpu_candidates':40,'created_at':1000,'started_at':1010})
        loops[0]['comparison']={'start_date':'2020-01-01','end_date':'2020-02-01','exchanges':['binance']}
        loops[0]['history']=[{'round':0,'reason':'Fixed comparison available','knowledge':'Verified first candidate','best_score':.5}]
        loops[0]['cycles']=[{'round':number,'status':'validated','started_at':1000+100*number,'ended_at':1100+120*number,
            'duration_seconds':100+20*number,'duration_estimated':False,'active':False,'backtests':1,'completed_backtests':1,
            'score':.5+.7*number,'score_delta':None if number==0 else .7,
            'goals':[{'goal':'gain','metrics':['gain'],'target':2,'direction':'max','value':.5+.7*number,
                      'previous':None if number==0 else .5,'delta':None if number==0 else .7,'improved':None if number==0 else True,'confirmed':False}],
            'reason':'Exact comparison confirms the values','knowledge':'Improvement measured under fixed conditions',
            'observations':[{'candidate_id':'candidate_'+str(number),'assessment':{'comparable':True,'simulation_complete':True,'goals':[]}}]} for number in (0,1)]
        for number,cycle in enumerate(loops[0]['cycles']):
            cycle.update(winner_operation=check1 if number == 0 else check2,
                         winner_candidate_id='candidate_'+str(number))
            cycle['goals'].append({'goal':'drawdown','metrics':['drawdown_worst_strategy_eq'],'direction':'min',
                'value':.3 if number==0 else .5,'previous':None if number==0 else .3,
                'delta':None if number==0 else .2,'confirmed':False})
            cycle['observations'][0].update(operation=check1 if number==0 else check2,
                assessment={'comparable':True,'simulation_complete':True,'goals':[
                    {'goal':goal['goal'],'metric':goal['metrics'][0],'direction':goal['direction'],'value':goal['value']}
                    for goal in cycle['goals']]})
        loops[0].update(phase='optimize', round=1)
        loops[0]['jobs'][1].update(proxy_evaluations=50000, proxy_limit=100000)
        page.evaluate("document.dispatchEvent(new Event('visibilitychange'))")
        page.wait_for_function("document.querySelector('#loops-queue-table').textContent.includes('Loop 2')")
        assert 'Reduce recovery duration' not in page.locator('#loops-queue-table').inner_text()
        progress=page.locator('#loops-queue-table tr[data-key="'+'a'*32+':1"]')
        assert 'Gain 1.2 ×' in progress.inner_text() and 'Drawdown 50 %' in progress.inner_text()
        assert progress.locator('.loop-proxy-progress').inner_text() == 'Proxy 50,000 / 100,000 · 50%'
        main_proxy=page.locator('#loops-queue-table tr[data-key="'+'a'*32+'"] .loop-proxy-progress')
        assert main_proxy.inner_text() == 'Proxy 50,000 / 100,000 · 50%'
        loops[0]['jobs'][1]['proxy_evaluations']=104448
        page.evaluate("document.dispatchEvent(new Event('visibilitychange'))")
        page.wait_for_function("document.querySelector('.loop-proxy-progress')?.textContent==='Proxy 104,448 / 100,000 · 104.4%'")
        assert progress.locator('.loop-proxy-progress').inner_text() == 'Proxy 104,448 / 100,000 · 104.4%'
        assert progress.locator('.loop-activity').inner_text() == 'GPU optimizing'
        loops[0]['phase']='evaluate'
        page.evaluate("document.dispatchEvent(new Event('visibilitychange'))")
        page.wait_for_function("document.querySelector('.loop-activity')?.textContent==='AI evaluating results'")
        assert progress.locator('.loop-activity').inner_text() == 'AI evaluating results'
        assert page.locator('#loops-queue-table tr[data-key=\"'+'a'*32+'\"]').locator('.loop-activity').inner_text() == 'AI evaluating results'

        assert '↑ +0.7 ×' in progress.locator('.loop-metric-change.is-better').all_text_contents()
        assert '↓ +20 pp' in progress.locator('.loop-metric-change.is-worse').all_text_contents()
        assert '↑ -10 pp' in progress.locator('.loop-metric-change.is-better').all_text_contents()
        first_comparison=page.locator('#loops-queue-table tr[data-key="'+'a'*32+':0"] td').nth(5)
        assert first_comparison.locator('.loop-metric-change.is-better, .loop-metric-change.is-worse').count()==0
        assert progress.locator('.loop-metric-change.is-better').first.get_attribute('title').endswith('vs Loop 1 (best exact comparison)')
        # Parallel variants require an explicit job choice instead of guessing a log target.
        variant=copy.deepcopy(loops[0]['jobs'][1]);variant.update(operation='5'*32,name='Other parallel optimizer')
        loops[0]['jobs'].append(variant)
        page.locator('#loops-queue-table tr[data-key="'+'a'*32+':1"] .loop-disclosure').click()
        page.evaluate("document.dispatchEvent(new Event('visibilitychange'))")
        page.wait_for_function("document.querySelector('#loops-queue-table').textContent.includes('Optimizer Variant 2')")
        assert page.locator('#loops-queue-table tr[data-key="'+'a'*32+'"]').get_by_role('button',name='Log',exact=True).is_disabled()
        assert page.locator('#loops-queue-table tr[data-key="'+'a'*32+':1"]').get_by_role('button',name='Log',exact=True).is_disabled()
        assert page.locator('#loops-queue-table tr[data-key="'+'a'*32+':1:'+second+'"]').get_by_role('button',name='Log',exact=True).is_enabled()
        assert page.locator('#loops-queue-table tr[data-key="'+'a'*32+':1:'+second+'"] .loop-proxy-progress').inner_text() == 'Proxy 104,448 / 100,000 · 104.4%'
        assert page.locator('#loops-queue-table tr[data-key="'+'a'*32+':1:'+'5'*32+'"]').get_by_role('button',name='Log',exact=True).is_enabled()
        loops[0]['jobs'].remove(variant)
        page.evaluate("document.dispatchEvent(new Event('visibilitychange'))")
        page.wait_for_function("(id)=>{const row=Array.from(document.querySelectorAll('#loops-queue-table tr[data-key]')).find(row=>row.dataset.key===id);return row && Array.from(row.querySelectorAll('button')).some(button=>button.textContent==='Log' && !button.disabled);}",arg='a'*32)
        # Main and round shortcuts open the exact existing local/cloud queue log window.
        page.locator('#loops-queue-table tr[data-key="'+'a'*32+'"]').get_by_role('button',name='Log',exact=True).click()
        page.wait_for_function("state.cloudLogId==='d'.repeat(32) && document.querySelector('#log-panel').classList.contains('visible')")
        page.wait_for_function("state.logFile==='optimizes_v8/vast_'+ 'd'.repeat(32)+'.log'")
        assert log_targets[-1]==second and page.url.endswith('#loops-queue')
        assert 'Native optimizer' in page.locator('#log-panel-title').inner_text()
        page.reload()
        page.wait_for_function("state.cloudLogId==='d'.repeat(32) && document.querySelector('#log-panel').classList.contains('visible')")
        page.locator('#log-panel-close').click()
        page.wait_for_function("!new URL(location.href).searchParams.has('loop_log')")
        # Each optimizer variant has its own native Log action; backtests stay distinct.
        round_one=page.locator('#loops-queue-table tr[data-key="'+'a'*32+':0"]')
        round_one.locator('.loop-disclosure').click()
        optimizer=page.locator('#loops-queue-table tr[data-key="'+'a'*32+':0:'+first+'"]')
        optimizer.get_by_role('button',name='Log',exact=True).click()
        page.wait_for_function("state.logFilename==='11111111-1111-1111-1111-111111111111' && document.querySelector('#log-panel').classList.contains('visible')")
        assert log_targets[-1]==first
        assert native_status_requests[-1]=='11111111-1111-1111-1111-111111111111'
        page.wait_for_function("state.logFile==='optimizes_v8/11111111-1111-1111-1111-111111111111.log'")
        page.locator('#log-panel-close').click()
        assert page.locator('#loops-queue-table tr[data-key="'+'a'*32+':0:'+check1+'"]').get_by_role('button',name='Log',exact=True).count()==1
        individual=page.locator('#loops-queue-table tr[data-key="'+'a'*32+':1:'+check2+'"]')
        assert 'Gain 1.2 ×' in individual.inner_text()
        assert '↓ +20 pp' in individual.locator('.loop-metric-change.is-worse').all_text_contents()
        # Incomplete individual simulations retain values, but cannot claim comparable improvement.
        loops[0]['cycles'][1]['observations'][0]['assessment']['simulation_complete']=False
        page.evaluate("document.dispatchEvent(new Event('visibilitychange'))")
        page.wait_for_function("(key)=>{const row=Array.from(document.querySelectorAll('#loops-queue-table tr[data-key]')).find(row=>row.dataset.key===key);return row&&row.cells[5].querySelectorAll('.is-better,.is-worse').length===0;}",arg='a'*32+':1:'+check2)
        assert progress.locator('td').nth(7).locator('.is-better,.is-worse').count()==0
        loops[0]['cycles'][1]['observations'][0]['assessment']['simulation_complete']=True
        page.evaluate("document.dispatchEvent(new Event('visibilitychange'))")
        page.wait_for_function("(key)=>{const row=Array.from(document.querySelectorAll('#loops-queue-table tr[data-key]')).find(row=>row.dataset.key===key);return row&&row.cells[5].querySelector('.is-worse');}",arg='a'*32+':1:'+check2)
        assert log_subscriptions and all(file.startswith('optimizes_v8/') for file in log_subscriptions)
        # Backtests open only their own native file, without leaving the Loop page.
        before_status=len(native_status_requests)
        before_logs=len(log_subscriptions)
        page.locator('#loops-queue-table tr[data-key="'+'a'*32+':0:'+check1+'"]').get_by_role('button',name='Log',exact=True).click()
        page.wait_for_function("state.logFile==='backtests_v8/11111111-1111-1111-1111-111111111111.log' && state.logViewer._fileList.length===1")
        assert '/api/backtest-v8/' not in page.url
        assert page.locator('#log-panel-title').inner_text()=='Backtest log - Specific backtest'
        assert not page.locator('#opt-log-dashboard').is_visible()
        assert len(native_status_requests)==before_status
        page.wait_for_function("new URL(location.href).searchParams.get('loop_log')==='"+check1+"'")
        page.reload()
        page.wait_for_function("state.logFile==='backtests_v8/11111111-1111-1111-1111-111111111111.log' && state.logViewer._fileList.length===1")
        assert len(native_status_requests)==before_status
        assert log_subscriptions[before_logs:] and set(log_subscriptions[before_logs:])=={'backtests_v8/11111111-1111-1111-1111-111111111111.log'}
        page.locator('#log-panel-close').click()
        row=page.locator('#loops-queue-table tr[data-key="'+'a'*32+':1"]')
        row.focus();row.press('Enter')
        assert page.locator('#loop-report-overlay').evaluate("node=>node.classList.contains('is-open')")
        assert 'Round 2' in page.locator('#loop-report-title').inner_text()
        assert page.get_by_role('button',name='Continue',exact=True).count()==1
        page.get_by_role('button',name='10 more',exact=True).click()
        assert page.get_by_role('spinbutton',name='Additional optimizer runs').input_value()=='10'
        # Background changes must leave the actual number input attached, even mid-edit.
        count=page.get_by_role('spinbutton',name='Additional optimizer runs')
        count.fill('')
        count.evaluate('node=>window.loopCountInput=node')
        changing=next(item for item in loops if item['id']=='a'*32)
        changing['cycles'][1]['status']='Focus update one'
        page.evaluate("document.dispatchEvent(new Event('visibilitychange'))")
        page.wait_for_function("document.querySelector('#loop-report-title').textContent.includes('Focus update one')")
        assert count.evaluate('node=>node===window.loopCountInput && node===document.activeElement')
        assert count.input_value()==''
        count.press('2')
        changing['cycles'][1]['status']='Focus update two'
        page.evaluate("document.dispatchEvent(new Event('visibilitychange'))")
        page.wait_for_function("document.querySelector('#loop-report-title').textContent.includes('Focus update two')")
        assert count.evaluate('node=>node===window.loopCountInput && node===document.activeElement')
        assert count.input_value()=='2'
        count.press('0')
        assert count.input_value()=='20'
        changing['cycles'][1]['status']='validated'
        page.get_by_role('button',name='10 more',exact=True).click()
        geometry=page.locator('#loop-report-dialog').evaluate("(node)=>{let r=node.getBoundingClientRect();return {x:r.x,y:r.y,w:r.width,h:r.height,vw:innerWidth,vh:innerHeight,bg:getComputedStyle(node).backgroundColor};}")
        assert abs(geometry['x']+geometry['w']/2-geometry['vw']/2)<2
        assert abs(geometry['y']+geometry['h']/2-geometry['vh']/2)<2
        assert geometry['x']>=15 and geometry['y']>=15
        assert page.locator('#loop-report-dialog').get_attribute('role')=='dialog'
        assert page.locator('#loop-report-close').is_visible()
        assert page.locator('#loop-report-close').evaluate('(node)=>node.getBoundingClientRect().height')>=30
        assert page.locator('#loop-report-tab-goals').evaluate('(node)=>node.getBoundingClientRect().height')>=30
        assert page.locator('#loop-report-dialog .pbg-window-resize').count()==8
        # Exercise the real shared resize and title-bar handlers, not synthetic CSS.
        before=page.locator('#loop-report-dialog').bounding_box()
        handle=page.locator('#loop-report-dialog .pbg-window-resize-se').bounding_box()
        x,y=handle['x']+handle['width']/2,handle['y']+handle['height']/2
        page.mouse.move(x,y)
        page.mouse.down();page.mouse.move(x-60,y-20,steps=5);page.mouse.up()
        resized=page.locator('#loop-report-dialog').bounding_box()
        assert resized['width']==pytest.approx(before['width']-60,abs=1)
        assert resized['height']<=before['height']-15
        page.mouse.move(resized['x']+35,resized['y']+25)
        page.mouse.down();page.mouse.move(resized['x']+65,resized['y']+45,steps=5);page.mouse.up()
        moved=page.locator('#loop-report-dialog').bounding_box()
        assert moved['x']==pytest.approx(resized['x']+30,abs=1)
        assert moved['y']==pytest.approx(resized['y']+20,abs=1)
        assert moved['width']==resized['width'] and moved['height']==resized['height']

        page.locator('#loop-report-close').focus();page.keyboard.press('Shift+Tab')
        assert page.evaluate("document.querySelector('#loop-report-dialog').contains(document.activeElement)")
        page.keyboard.press('Tab')
        assert page.locator('#loop-report-close').evaluate('(node)=>node===document.activeElement')

        assert '0h 2m 0s' in page.locator('#loop-report-body').inner_text()
        page.mouse.click(3,3)
        assert page.locator('#loop-report-overlay').evaluate("(node)=>node.classList.contains('is-open')"), 'Outside clicks cannot close reports'
        page.locator('#loop-report-tab-goals').click()
        assert 'improved' in page.locator('#loop-report-body').inner_text()
        assert '50 %' in page.locator('#loop-report-body').inner_text() and '120 %' in page.locator('#loop-report-body').inner_text()
        assert '+70 %' in page.locator('#loop-report-body').inner_text()
        assert page.locator('#loop-report-dialog').bounding_box()==moved
        page.locator('#loop-report-tab-changes').click()
        page.wait_for_function("document.querySelector('#loop-report-body').textContent.includes('optimize.limits.2.value')")
        assert diff_queries[-1]['baseline']==[first]
        # Completed historical comparisons survive later-round polling and keep live controls attached.
        before_diff=len(diff_queries)
        page.locator('#loop-diff-operation').focus()
        page.evaluate("""()=>{window.diffSelect=document.querySelector('#loop-diff-operation');
            window.diffTable=document.querySelector('#loop-report-body .loop-diff-selectors').nextElementSibling.firstElementChild;
            document.querySelector('#loop-report-body').scrollTop=140;}""")
        diff_scroll=page.locator('#loop-report-body').evaluate('node=>node.scrollTop')
        assert diff_scroll>0
        for tick in range(3):
            changing['ai_calls']=100+tick
            changing['cycles'][1]['status']='Diff poll '+str(tick)
            page.evaluate("document.dispatchEvent(new Event('visibilitychange'))")
            page.wait_for_function("text=>document.querySelector('#loop-report-title').textContent.includes(text)",arg='Diff poll '+str(tick))
            assert len(diff_queries)==before_diff
            assert page.evaluate("window.diffSelect===document.activeElement && window.diffTable===document.querySelector('#loop-report-body .loop-diff-selectors').nextElementSibling.firstElementChild")
            assert page.locator('#loop-report-body').evaluate('node=>node.scrollTop')==diff_scroll
            assert 'Loading parameter diff' not in page.locator('#loop-report-body').text_content()
        third,later='5'*32,'a'*32
        changing['jobs'].extend([
            {'operation':third,'name':'fixture_variant','kind':'optimizer','round':1,'status':'completed','snapshot_digest':'third-content'},
            {'operation':later,'name':'fixture_later','kind':'optimizer','round':2,'status':'running','snapshot_digest':'later-content'}])
        changing['cycles'].append({'round':2,'status':'running','goals':[]})
        page.evaluate("document.dispatchEvent(new Event('visibilitychange'))")
        page.wait_for_function("document.querySelector('#loop-diff-operation').options.length===2 && document.querySelector('#loop-diff-baseline').options.length===4")
        assert len(diff_queries)==before_diff
        assert page.evaluate('window.diffSelect===document.activeElement')
        # An actual relevant content revision reloads once, while polling reuses the in-flight request.
        diff_fixture['delay']=True
        changing['jobs'][1]['snapshot_digest']='second-revised'
        page.evaluate("document.dispatchEvent(new Event('visibilitychange'))")
        page.wait_for_function("document.querySelector('#loop-report-body').textContent.includes('Loading parameter diff')")
        page.wait_for_timeout(100)
        assert len(diff_queries)==before_diff+1 and len(diff_fixture['pending'])==1
        changing['cycles'][1]['status']='Pending diff poll'
        page.evaluate("document.dispatchEvent(new Event('visibilitychange'))")
        page.wait_for_function("document.querySelector('#loop-report-title').textContent.includes('Pending diff poll')")
        assert len(diff_queries)==before_diff+1
        page.locator('#loop-diff-operation').select_option(third)
        page.wait_for_function("new URL(location.href).searchParams.get('loop_variant')==='5'.repeat(32)")
        page.locator('#loop-diff-baseline').select_option('initial')
        page.wait_for_function("new URL(location.href).searchParams.get('loop_compare')==='initial'")
        page.wait_for_timeout(100)
        assert len(diff_queries)==before_diff+3
        assert diff_queries[-1]=={'operation':[third],'baseline':['initial']}
        newest_route,newest_payload=diff_fixture['pending'].pop()
        newest_payload['fields'].append({'path':'fixture.current_comparison','before':0,'after':1})
        newest_route.fulfill(json=newest_payload)
        page.wait_for_function("document.querySelector('#loop-report-body').textContent.includes('fixture.current_comparison')")
        while diff_fixture['pending']:
            stale_route,stale_payload=diff_fixture['pending'].pop(0)
            stale_payload['fields']=[{'path':'fixture.stale_response','before':0,'after':1}]
            stale_route.fulfill(json=stale_payload)
        assert 'fixture.stale_response' not in page.locator('#loop-report-body').text_content()
        # Switching tabs and closing discard pending responses; reopening reads the current comparison.
        page.locator('#loop-diff-baseline').select_option(first)
        page.wait_for_timeout(100)
        page.locator('#loop-report-tab-goals').click()
        while diff_fixture['pending']:
            delayed_route,payload=diff_fixture['pending'].pop(0)
            delayed_route.fulfill(json=payload)
        assert page.locator('#loop-diff-operation').count()==0
        page.locator('#loop-report-tab-changes').click()
        page.wait_for_timeout(100)
        page.locator('#loop-report-close').click()
        while diff_fixture['pending']:
            delayed_route,payload=diff_fixture['pending'].pop(0)
            delayed_route.fulfill(json=payload)
        assert page.locator('#loop-report-overlay').get_attribute('aria-hidden')=='true'
        diff_fixture['delay']=False
        changing['cycles'].pop()
        changing['cycles'][1]['status']='validated'
        changing['jobs']=changing['jobs'][:-2]
        row.get_by_role('button',name='Open',exact=True).click()
        page.locator('#loop-report-tab-changes').click()
        page.wait_for_function("document.querySelector('#loop-report-body').textContent.includes('optimize.limits.2.value')")
        page.locator('#loop-diff-baseline').select_option('initial')
        page.wait_for_function("new URL(location.href).searchParams.get('loop_compare')==='initial'")
        assert '0.25' in page.locator('#loop-report-body').inner_text()
        assert page.locator('#loop-report-dialog').evaluate('(node)=>node.scrollWidth <= node.clientWidth+1')
        stored_geometry=page.locator('#loop-report-dialog').bounding_box()
        page.reload()
        page.wait_for_function("document.querySelector('#loop-report-overlay')?.classList.contains('is-open') && document.querySelector('#loop-diff-baseline')?.value==='initial'")
        assert page.locator('#loop-report-tab-changes').get_attribute('aria-selected')=='true'
        assert page.locator('#loop-report-dialog').bounding_box()==stored_geometry
        page.locator('#loop-report-tab-backtests').click()
        assert 'validate_second' in page.locator('#loop-report-body').inner_text()
        assert '2020-01-01' in page.locator('#loop-report-body').inner_text()
        page.locator('#loop-report-tab-analysis').click()
        assert 'Reduce recovery duration' in page.locator('#loop-report-body').inner_text()
        assert page.locator('#loop-report-body script').count()==0
        if viewport_width==1200:
            # User-only metrics have a separate report, native log and exact Compare handoff.
            page.locator('#loop-report-tab-performance').click()
            assert '3 ×' in page.locator('#loop-report-body').inner_text()
            assert '30 %' in page.locator('#loop-report-body').inner_text()
            assert 'Change vs start' in page.locator('#loop-report-body').text_content()
            assert 'Waiting for complete simulation results.' in page.locator('#loop-report-body').inner_text()
            before_status=len(native_status_requests)
            page.locator('#loop-report-body tbody tr').filter(has_text='Loop 2 · Holdout').get_by_role('button',name='Log',exact=True).click()
            page.wait_for_function("state.logFile==='backtests_v8/11111111-1111-1111-1111-111111111111.log'")
            assert log_targets[-1]=='8'*32 and len(native_status_requests)==before_status
            page.reload()
            page.wait_for_function("state.logFile==='backtests_v8/11111111-1111-1111-1111-111111111111.log'")
            page.locator('#log-panel-close').click()
            assert page.locator('#loop-report-tab-performance').get_attribute('aria-selected')=='true'
            page.locator('#loop-report-body tbody tr').filter(has_text='Loop 2 · Holdout').get_by_role('button',name='Results',exact=True).click()
            page.wait_for_url('**/api/backtest-v8/main_page?**')
            target=parse_qs(urlparse(page.url).query)
            assert target['panel']==['results'] and target['result_compare']==['1']
            assert target['result_filter']==['Specific backtest']
            assert 'loop_tab=performance' in target['loop_return'][0]
            page.goto('http://pbgui.test'+target['loop_return'][0])
            page.wait_for_selector('#loop-report-tab-performance[aria-selected="true"]')
            for label,kind in [('Compare all Holdouts','observer_holdout'),('Compare all Full Time Ranges','observer_full_range')]:
                control=page.locator('#loop-report-body').get_by_role('button',name=label,exact=True)
                if kind=='observer_full_range':
                    assert control.is_disabled()
                    loops[0]['observer_jobs'][-1].update(status='complete',display_status='complete',simulation_complete=True,
                                                        report_done=True,metrics={'gain':3,'drawdown':.3},ended_at=1050)
                    page.evaluate("document.dispatchEvent(new Event('visibilitychange'))")
                    page.wait_for_function("Array.from(document.querySelectorAll('#loop-report-body button')).some(button=>button.textContent==='Compare all Full Time Ranges'&&!button.disabled)")
                assert control.is_enabled()
                control.click()
                page.wait_for_url('**/api/backtest-v8/main_page?**')
                compare_target=parse_qs(urlparse(page.url).query)
                assert compare_target['result_loop']==['a'*32]
                assert compare_target['result_evaluation']==[kind]
                assert 'result_compare' not in compare_target
                page.goto('http://pbgui.test'+compare_target['loop_return'][0])
                page.wait_for_selector('#loop-report-tab-performance[aria-selected="true"]')
            page.locator('#loop-report-close').click()
            page.locator('#loop-queue-compare-observer_holdout').click()
            page.wait_for_url('**/api/backtest-v8/main_page?**')
            sidebar_target=parse_qs(urlparse(page.url).query)
            assert sidebar_target['result_evaluation']==['observer_holdout']
            assert 'result_compare' not in sidebar_target
            page.goto('http://pbgui.test'+sidebar_target['loop_return'][0])
            page.locator('#loops-queue-table tr[data-key="'+'a'*32+'"]').get_by_role('button',name='Open',exact=True).click()
            page.locator('#loop-report-tab-analysis').click()
        # Every Holdout row displays its own candidate; the summary uses the winner.
        original_observers=copy.deepcopy(loops[0]['observer_jobs'])
        first_holdout=loops[0]['observer_jobs'][2]
        first_holdout.update(candidate_id='holdout_1',comparison_label='Comparison backtest 1',holdout_score=1)
        for index,gain in [(2,5),(3,4)]:
            holdout=copy.deepcopy(first_holdout)
            holdout.update(operation='0'*31+str(index),candidate_id='holdout_'+str(index),
                           comparison_label='Comparison backtest '+str(index),holdout_score=5-index,
                           selected_for_full_range=index==2,metrics=dict(first_holdout['metrics'],gain=gain))
            loops[0]['observer_jobs'].append(holdout)
        loops[0]['observer_jobs'][3].update(candidate_id='holdout_2',comparison_label='Comparison backtest 2',selected_for_full_range=True)
        loops[0]['observer_selections']=[{'round':1,'status':'selected','expected':3,'completed':3,
                                          'operation':'0'*31+'2','candidate_id':'holdout_2','score':3}]
        page.locator('#loop-report-tab-performance').click()
        page.evaluate("document.dispatchEvent(new Event('visibilitychange'))")
        page.wait_for_function("document.querySelector('#loop-report-body').textContent.includes('Comparison backtest 3')")
        for index,gain in [(1,3),(2,5),(3,4)]:
            holdout_row=page.locator('#loop-report-body tbody tr').filter(has_text='Loop 2 · Holdout · Comparison backtest '+str(index))
            assert str(gain)+' ×' in holdout_row.inner_text()
        assert 'Selected candidate' in page.locator('#loop-report-body tbody tr').filter(has_text='Loop 2 · Holdout · Comparison backtest 2').inner_text()
        assert 'Legacy score' in page.locator('#loop-report-body').text_content()
        page.reload()
        page.wait_for_function("document.querySelector('#loop-report-body')?.textContent.includes('Comparison backtest 3')")
        assert page.locator('#loop-report-dialog').evaluate('(node)=>node.scrollWidth <= node.clientWidth+1')
        page.locator('#loop-report-close').click()
        page.locator('#loops-queue-table tr[data-key="'+'a'*32+':observer:'+'0'*31+'2'+'"]').wait_for()
        assert '5 ×' in page.locator('#loops-queue-table tr[data-key="'+'a'*32+':observer:'+'0'*31+'2'+'"]').inner_text()
        assert '3 ×' in page.locator('#loops-queue-table tr[data-key="'+'a'*32+':observer:'+'8'*32+'"]').inner_text()
        loops[0]['observer_jobs']=original_observers
        loops[0].pop('observer_selections')
        page.evaluate("document.dispatchEvent(new Event('visibilitychange'))")
        page.locator('#loops-queue-table tr[data-key="'+'a'*32+':1'+'"]').get_by_role('button',name='Open',exact=True).click()
        page.locator('#loop-report-tab-analysis').click()
        # Long content scrolls inside the window; the title and close remain reachable.
        page.set_viewport_size({'width':viewport_width,'height':430})
        page.locator('#loop-report-body').evaluate("node=>{const p=document.createElement('p');p.textContent='Long report '.repeat(3000);node.appendChild(p);}")
        assert page.locator('#loop-report-body').evaluate('(node)=>node.scrollHeight>node.clientHeight')
        assert page.locator('#loop-report-close').is_visible()
        assert page.locator('#loop-report-dialog').evaluate('(node)=>{let r=node.getBoundingClientRect();return r.top>=15&&r.bottom<=innerHeight-15;}')
        page.set_viewport_size({'width':viewport_width,'height':1000})

        loops[0]['ai_calls']=2
        page.evaluate("document.dispatchEvent(new Event('visibilitychange'))")
        page.wait_for_timeout(3500)
        assert page.locator('#loop-report-tab-analysis').get_attribute('aria-selected')=='true'
        page.locator('#loop-report-close').click()
        assert page.url.endswith('#loops-queue')
        # Returning to Config and editing never mutates previously executed evidence.
        snapshot=copy.deepcopy(loops[0])
        page.locator('.sb-section[data-panel="loops-config"]').click()
        page.locator('#loop-config-list tbody tr').click()
        page.locator('#loop-edit-config').click()
        page.wait_for_function("!document.querySelector('#loop-form').hidden")
        assert page.locator('#loop-hsl-mode').input_value() == 'off'
        # Same-name Save has no waiting queue ID and must send null, not an empty string.
        revision=definitions['My named loop']['revision']
        page.locator('#loop-save').click()
        page.wait_for_function("document.querySelector('#loop-form').hidden")
        assert definitions['My named loop']['revision']==revision+1 and loops[0]==snapshot
        page.locator('#loop-edit-config').click()
        page.wait_for_function("!document.querySelector('#loop-form').hidden")
        page.locator('#loop-name').fill('Second named loop')
        page.locator('#loop-goals').fill('Preserve this unsaved edit')
        page.wait_for_timeout(3300)
        assert page.locator('#loop-goals').input_value()=='Preserve this unsaved edit'
        assert page.locator('#loop-queue-config').is_disabled(), 'A hidden list selection cannot queue a different config while editing'
        knowledge_nav.click()
        page.wait_for_function("document.querySelector('#panel-loops-knowledge').classList.contains('active')")
        page.locator('.sb-section[data-panel="loops-config"]').click()
        assert page.locator('#loop-goals').input_value()=='Preserve this unsaved edit'
        page.locator('.sb-section[data-panel="loops-queue"]').click()
        page.reload()
        page.wait_for_function("document.querySelector('#loop-goals')?.value==='Preserve this unsaved edit' && document.querySelector('#panel-loops-queue').classList.contains('active')")
        assert page.url.endswith('#loops-queue'), 'Restoring a draft must preserve the active destination'
        assert page.locator('#loop-hsl-mode').input_value() == 'off'
        page.locator('.sb-section[data-panel="loops-config"]').click()
        assert page.locator('#loop-goals').input_value()=='Preserve this unsaved edit'
        page.locator('#loop-save').click()
        page.wait_for_function("document.querySelectorAll('#loop-config-list tbody tr').length===2")
        assert loops[0]==snapshot
        config_rows=page.locator('#loop-config-list tbody tr')
        config_rows.nth(0).click();config_rows.nth(1).click(modifiers=['Control'])
        assert page.locator('#loop-config-list tbody tr.selected').count()==2
        config_rows.nth(1).click(modifiers=['Control'])
        assert page.locator('#loop-config-list tbody tr.selected').count()==1
        first_box=config_rows.nth(0).bounding_box();last_box=config_rows.nth(1).bounding_box()
        page.mouse.move(first_box['x']+30,first_box['y']+first_box['height']/2);page.mouse.down()
        page.mouse.move(last_box['x']+30,last_box['y']+last_box['height']/2,steps=1);page.mouse.up()
        assert page.locator('#loop-config-list tbody tr.selected').count()==2
        # Finished runs belong to Results, with ordinary Edit Selected to reuse settings.
        loops[0].update(status='failed',ended=True,deletable=True,reason='Missing goal ID',last_error='Missing goal ID')
        other=copy.deepcopy(loops[0]);other.update(id='b'*32,name='Other finished loop');loops.append(other)
        page.locator('.sb-section[data-panel="loops-results"]').click()
        page.wait_for_function("document.querySelector('#loops-results-table tr[data-key]')")
        page.locator('#loops-results-table tr[data-key]').first.click()
        page.locator('#loops-results-table tr[data-key]').first.get_by_role('button',name='Open',exact=True).click()
        page.locator('#loop-report-tab-analysis').click()
        assert page.locator('#loop-report-body').inner_text().count('Missing goal ID')==1
        assert 'Termination details' in page.locator('#loop-report-body').inner_text()
        assert 'Not recorded' in page.locator('#loop-report-body').inner_text()
        # New failure evidence updates the open report and survives browser reload.
        failure_reason='AI provider request timed out after 180 seconds. Failed after 3 attempts.'
        loops[0].update(reason=failure_reason,last_error=failure_reason,stop_reason='Proxy limit reached',failure={
            'at':1700000300,'phase':'evaluate','stage':'evaluate','round':1,'error_type':'LoopAIRequestFailed',
            'reason':failure_reason,'detail':failure_reason,'provider':'chatgpt','model':'gpt-sol-6.1','effort':'high','attempts':3,
            'history':[{'attempt':n,'at':1700000000+n*90,'error':'Timeout after 180 seconds <script>test-only</script>',
                        'retry_delay':30 if n==1 else 60 if n==2 else None} for n in range(1,4)]})
        page.evaluate("document.dispatchEvent(new Event('visibilitychange'))")
        page.wait_for_function("document.querySelector('#loop-report-body').textContent.includes('gpt-sol-6.1')")
        assert 'Failed request attempts' in page.locator('#loop-report-body').inner_text()
        assert '60 seconds' in page.locator('#loop-report-body').inner_text()
        assert page.locator('#loop-report-body script').count()==0
        page.reload()
        page.wait_for_function("document.querySelector('#loop-report-body')?.textContent.includes('gpt-sol-6.1')")
        assert page.locator('#loop-report-tab-analysis').get_attribute('aria-selected')=='true'
        page.locator('#loop-report-tab-overview').click()
        assert failure_reason in page.locator('#loop-report-body').inner_text()
        assert 'Proxy limit reached' not in page.locator('#loop-report-body').inner_text()
        assert page.locator('#loop-report-dialog').evaluate('(node)=>node.scrollWidth <= node.clientWidth+1')
        page.locator('#loop-report-close').click()
        page.locator('#loops-results-table tr[data-key="'+'b'*32+'"]').click()
        assert page.locator('#loops-results-table tbody tr.selected').get_attribute('data-key')=='b'*32
        page.locator('#loops-results-table tr[data-key="'+'a'*32+'"]').click()
        page.locator('#loop-results-edit').click()
        page.wait_for_function("!document.querySelector('#loop-form').hidden")
        assert page.locator('#loop-execution').input_value()=='vast'
        assert page.locator('#loop-name').input_value()=='My named loop'
        assert page.locator('#loop-goals').input_value()=='Prefer stable equity'
        assert page.locator('#loop-strategy').is_checked()
        # Reuse a finished result with Save & Queue under its existing config name.
        old_result=copy.deepcopy(loops[0])
        page.locator('#loop-start').click()
        page.wait_for_function("(id)=>document.querySelector('#panel-loops-queue').classList.contains('active') && Array.from(document.querySelectorAll('#loops-queue-table tr[data-key]')).some(row=>row.dataset.key===id)",arg='c'*32)
        assert len(starts)==2 and loops[0]==old_result
        assert loops[-1]['status']=='queued'
        for width in (160,420):
            page.evaluate('(width)=>document.getElementById("sidebar").style.width=width+"px"', width)
            assert page.evaluate('()=>Array.from(document.querySelectorAll("#sidebar .sb-section, #sidebar .sb-btn")).filter(node=>node.offsetParent).every(node=>node.scrollWidth<=node.clientWidth+1)')
        destination=page.url
        page.locator('#pbgui-guide-btn').click()
        page.wait_for_selector('#pbgui-shared-help-ovl.visible')
        assert page.url==destination
        page.locator('#pbgui-shared-help-close').click()
        # Run deletion uses the shared explicit confirmation and preserves configs.
        page.locator('.sb-section[data-panel="loops-results"]').click()
        page.locator('#loops-results-table tr[data-key="'+'a'*32+'"]').click()
        assert page.locator('#loop-results-delete').is_enabled()
        page.locator('#loop-results-delete').click()
        page.wait_for_selector('#pbgui-dialog-ovl.visible')
        page.mouse.click(3,3)
        assert page.locator('#pbgui-dialog-ovl').is_visible()
        page.locator('#pbgui-dialog-cancel').click()
        assert len(loops)==3
        page.locator('#loops-results-table tr[data-key="'+'b'*32+'"]').click(modifiers=['Control'])
        assert page.locator('#loops-results-table tbody tr.selected').count()==2
        page.locator('#loop-results-delete').click()
        page.locator('#pbgui-dialog-accept').click()
        page.wait_for_function("document.querySelector('#loops-results-table').textContent.includes('No finished loops.')")
        assert len(loops)==1 and loops[0]['id']=='c'*32 and len(definitions)==2
        assert 'loop_id' not in parse_qs(urlparse(page.url).query)
        page.reload()
        page.wait_for_selector('#loops-results-table')
        assert page.url.endswith('#loops-results') and page.locator('#loop-results-delete').is_disabled()
        # Waiting entries are removable from Queue without starting jobs.
        page.locator('.sb-section[data-panel="loops-queue"]').click()
        page.wait_for_function("document.querySelector('#loops-queue-table tr[data-key]')")
        page.locator('#loops-queue-table tr[data-key="'+'c'*32+'"]').click()
        page.locator('#loop-queue-delete').click()
        page.locator('#pbgui-dialog-accept').click()
        page.wait_for_function("document.querySelector('#loops-queue-table').textContent.includes('No queued or active loops.')")
        assert not loops and len(definitions)==2
        # Continue from a result is one direct authorized action, using shared AI settings.
        parent=copy.deepcopy(old_result);parent.update(id='f'*32,status='unconfirmed',ended=True,deletable=True,
            best={'round':1,'assessment':{'score':1.2,'hard_targets_met':True,'goals':[{'goal':'gain','metric':'gain','value':1.2,'target':2,'confirmed':False,'direction':'max'}]}})
        loops.append(parent)
        for identifier,status in [('1'*32,'failed'),('2'*32,'stopped')]:
            without_continue=copy.deepcopy(parent)
            without_continue.update(id=identifier,name='ema_anchor_05_hype_bybit_hype',status=status,best=None)
            loops.append(without_continue)
        page.locator('.sb-section[data-panel="loops-results"]').click()
        page.wait_for_function("document.querySelector('#loops-results-table').textContent.includes('My named loop')")
        # The real result rows keep Open/Log aligned even when Continue is present.
        open_positions,log_positions=[],[]
        for identifier in ['1'*32,'2'*32,'f'*32]:
            result_row=page.locator('#loops-results-table tr[data-key="'+identifier+'"]')
            actions=result_row.locator('td').first.locator('.loop-row-actions')
            assert actions.count()==1
            labels=actions.locator('button').all_text_contents()
            assert labels[1:3]==['Open','Log']
            assert ('Continue' in labels)==(identifier=='f'*32)
            if identifier=='f'*32:
                assert labels[-1]=='Continue'
            geometry=actions.locator('button').evaluate_all("nodes=>nodes.map(node=>{const rect=node.getBoundingClientRect();return {x:rect.x,y:rect.y,right:rect.right,float:getComputedStyle(node).float};})")
            assert all(item['float']=='none' for item in geometry)
            assert max(item['y'] for item in geometry)-min(item['y'] for item in geometry)<1
            assert all(left['right']<=right['x'] for left,right in zip(geometry,geometry[1:]))
            open_positions.append(geometry[1]['x'])
            log_positions.append(geometry[2]['x'])
        assert max(open_positions)-min(open_positions)<1
        assert max(log_positions)-min(log_positions)<1
        # Long result lists must scroll with the wheel, including after automatic updates.
        original_loops=list(loops)
        for index in range(24):
            scroll_run=copy.deepcopy(parent)
            scroll_run.update(id=f'{256+index:032x}',name=f'Scroll fixture {index}',jobs=[],cycles=[],best=None)
            loops.append(scroll_run)
        page.evaluate("document.dispatchEvent(new Event('visibilitychange'))")
        page.wait_for_function("document.querySelector('#loops-results-table').textContent.includes('Scroll fixture 23')")
        scroll_panel=page.locator('#panel-loops-results')
        assert scroll_panel.evaluate("node=>getComputedStyle(node).overflowY")=='auto'
        assert scroll_panel.evaluate('node=>node.scrollHeight>node.clientHeight')
        panel_box=scroll_panel.bounding_box()
        page.mouse.move(panel_box['x']+panel_box['width']/2,panel_box['y']+panel_box['height']/2)
        page.mouse.wheel(0,10000)
        page.wait_for_function("(()=>{const node=document.querySelector('#panel-loops-results');return node.scrollTop>0 && node.scrollTop+node.clientHeight>=node.scrollHeight-2;})()")
        last_row=page.locator('#loops-results-table tr[data-key="'+f'{279:032x}'+'"]')
        assert last_row.bounding_box()['y']+last_row.bounding_box()['height']<=panel_box['y']+panel_box['height']+2
        last_row.click()
        scroll_position=scroll_panel.evaluate('node=>node.scrollTop')
        loops[-1]['name']='Updated scroll fixture'
        page.evaluate("document.dispatchEvent(new Event('visibilitychange'))")
        page.wait_for_function("document.querySelector('#loops-results-table').textContent.includes('Updated scroll fixture')")
        assert abs(scroll_panel.evaluate('node=>node.scrollTop')-scroll_position)<2
        assert last_row.evaluate("node=>node.classList.contains('selected')")
        loops[:]=original_loops
        page.evaluate("document.dispatchEvent(new Event('visibilitychange'))")
        page.wait_for_function("!document.querySelector('#loops-results-table').textContent.includes('Updated scroll fixture')")
        scroll_panel.evaluate('node=>node.scrollTop=0')
        parent_row=page.locator('#loops-results-table tr[data-key="'+'f'*32+'"]')
        if parent_row.locator('.loop-disclosure').get_attribute('aria-expanded')=='false':
            parent_row.locator('.loop-disclosure').click()
        selected_round=page.locator('#loops-results-table tr[data-key="'+'f'*32+':0"]')
        selected_round.get_by_role('button',name='Continue',exact=True).click()
        assert page.locator('#loop-report-title').inner_text().startswith('Round 1')
        assert page.get_by_role('spinbutton',name='Additional optimizer runs').evaluate('node=>node===document.activeElement')
        page.get_by_role('button',name='5 more',exact=True).click()
        assert page.get_by_role('spinbutton',name='Additional optimizer runs').input_value()=='5'
        page.locator('#loop-report-close').click()
        parent_row.get_by_role('button',name='Continue',exact=True).click()
        page.locator('#loop-report-tab-overview').click()
        page.get_by_role('button',name='10 more',exact=True).click()
        continuation_delay['enabled']=True
        with page.expect_request('**/api/optimize-v8/loops/'+'f'*32+'/continue'):
            page.locator('#loop-report-body').get_by_role('button',name='Continue',exact=True).click()
        starting=page.locator('#loop-report-body').get_by_role('button',name='Starting…',exact=True)
        assert starting.is_disabled()
        assert page.get_by_role('spinbutton',name='Additional optimizer runs').is_disabled()
        assert page.get_by_role('button',name='5 more',exact=True).is_disabled()
        assert page.locator('#loop-report-body [role="status"]').inner_text()=='Starting the linked run…'
        starting.evaluate('node=>window.loopStartingButton=node')
        parent['ai_calls']+=1
        parent['name']='Pending continuation update'
        page.evaluate("document.dispatchEvent(new Event('visibilitychange'))")
        page.wait_for_function("document.querySelector('#loops-results-table').textContent.includes('Pending continuation update')")
        assert starting.evaluate('node=>node===window.loopStartingButton') and starting.is_disabled()
        page.locator('#loop-report-tab-analysis').click()
        assert page.locator('#loop-report-tab-analysis').get_attribute('aria-selected')=='true'
        assert starting.evaluate('node=>node===window.loopStartingButton') and starting.is_disabled()
        starting.evaluate("node=>node.dispatchEvent(new Event('click'))")
        assert len(continuation_delay['pending'])==1 and len(continuations)==1
        pending_route,child=continuation_delay['pending'].pop()
        pending_route.fulfill(json={'detail':'Checkpoint verification temporarily unavailable'},status=503)
        page.wait_for_function("document.querySelector('#loop-report-body [role=\"status\"]').textContent.includes('Checkpoint verification temporarily unavailable')")
        assert page.locator('#loop-report-body').get_by_role('button',name='Continue',exact=True).is_enabled()
        count=page.get_by_role('spinbutton',name='Additional optimizer runs')
        assert count.is_enabled() and count.input_value()=='10'
        count.fill('20')
        with page.expect_request('**/api/optimize-v8/loops/'+'f'*32+'/continue'):
            page.locator('#loop-report-body').get_by_role('button',name='Continue',exact=True).click()
        assert len(continuation_delay['pending'])==1 and len(continuations)==2
        pending_route,child=continuation_delay['pending'].pop()
        loops.append(child);pending_route.fulfill(json=child,status=201)
        page.wait_for_function("document.querySelector('#panel-loops-queue').classList.contains('active') && document.querySelector('#loops-queue-table').textContent.includes('Continued fixture')")
        assert continuations[-1]['rounds']==20 and continuations[-1]['round'] is None
        assert continuations[-1]['authorization'] and continuations[-1]['selection']['model']=='pinned'
        assert loops[-1]['continuation']['parent']=='f'*32
        assert 'loop_id='+('e'*32) in page.url
        page.locator('#loops-queue-table tr[data-key="'+'e'*32+'"]').get_by_role('button',name='Open',exact=True).click()
        page.locator('#loop-report-tab-analysis').click()
        page.get_by_role('button',name='View instructions',exact=True).click()
        page.wait_for_function("document.querySelector('#loop-instruction-text').value==='Frozen instructions of this run'")
        assert not page.locator('#loop-report-overlay').evaluate("node=>node.classList.contains('is-open')")
        page.reload()
        page.wait_for_function("document.querySelector('#panel-loops-instructions').classList.contains('active') && document.querySelector('#loop-instruction-text').value==='Frozen instructions of this run'")
        assert parse_qs(urlparse(page.url).query)['loop_instructions_run']==['e'*32]
        assert not errors,errors
        browser.close()


def test_pb8_loop_navigation_setup_polling_and_restore_mobile():
    """Run the complete observer/compare workflow independently at mobile width."""
    test_pb8_loop_navigation_setup_polling_and_restore(700)


def test_pb8_loop_navigation_setup_polling_and_restore_wide():
    """Run the complete observer/compare workflow independently at wide width."""
    test_pb8_loop_navigation_setup_polling_and_restore(1920)


@pytest.mark.parametrize('viewport_width', [700, 1200])
def test_pb8_unverified_model_is_visible_before_start(viewport_width):
    """Only insufficient/unknown context blocks start, without a model allowlist."""
    test_pb8_loop_navigation_setup_polling_and_restore(viewport_width, capability_only=True)


@pytest.fixture
def loop_sort_browser():
    """Serve the real shell, all assets and mutable synthetic records with no network access."""
    __tracebackhide__ = True
    browser_api = __import__('browser_driver')
    source = (ROOT / 'frontend/v7_optimize.html').read_text()
    for key, value in {'API_BASE':'/api/optimize-v8','OPTIMIZE_VERSION':'v8','VERSION':'test',
            'SERIAL':'1','NAV_HASH':'test','BASE_PREFIX':'','LIMITS_META':'{}','BACKTEST_VERSION':'v8',
            'OPTIMIZE_NAV_TITLE':'PB8 Optimize','OPTIMIZE_NAV_CURRENT':'v8_optimize'}.items():
        source = source.replace('%%'+key+'%%', value)
    definitions, runs, diffs, errors, mutations = [], [], [], [], []
    for index, number in enumerate((2, 10)):
        goals = [{'goal':'gain','metric':'gain','value':number,'direction':'max'}]
        definitions.append(dict(name=f'Config {number}',updated_at=1790892000+index*1468800,
            settings={'config_name':f'Start {number}','execution':'cpu' if index==0 else 'gpu'}))
        for ended in (False, True):
            run_id = ('abcd'[index + (2 if ended else 0)])*32
            run = dict(id=run_id,name=f'Run {number}',created_at=1790892000+index*1468800,
                started_at=1000,deadline=2000000000,ended=ended,status='completed' if ended else 'running',
                phase='optimize',round=1,settings={'config_name':'fixture','execution':'cpu','goals':{}},
                jobs=[],observer_jobs=[],history=[],cycles=[],observer_enabled=True,
                best={'round':1,'candidate_id':f'Candidate {number}','assessment':{'goals':goals}})
            for round_number in range(2):
                run['cycles'].append(dict(round=round_number,status='complete',started_at=1000+round_number*100,
                    duration_seconds=2 if round_number==0 else 10,completed_backtests=number+round_number,
                    backtests=number+round_number+1,goals=[dict(goals[0],value=number+round_number)],
                    score=number+round_number,score_delta=number+round_number,observations=[]))
                for variant in range(2):
                    run['jobs'].append(dict(round=round_number,kind='optimizer',operation=f'opt{round_number}{variant}',
                        name=f'Optimizer {variant+1}',status='complete',created_at=1000+round_number*100+variant*10,
                        ended_at=1200+round_number*100+variant*10,log_available=False))
                run['jobs'].append(dict(round=round_number,kind='validation',operation=f'check{round_number}',
                    name='Comparison',status='complete',created_at=1020+round_number*100,ended_at=1250+round_number*100))
                run['observer_jobs'].append(dict(round=round_number,kind='observer_holdout',operation=f'obs{round_number}',
                    name='Holdout',status='complete',created_at=1030+round_number*100,run_started_at=1040+round_number*100,
                    ended_at=1300+round_number*100,simulation_complete=True,selected_for_full_range=True,
                    metrics={'gain':number,'drawdown':number/100}))
            runs.append(run)
    with browser_api.sync_browser() as driver:
        browser = driver.chromium.launch(headless=True)
        try:
            page = browser.new_page(viewport={'width':1200,'height':900})
            page.set_default_timeout(7000)
            page.on('pageerror', lambda error: errors.append(str(error)))

            def respond(route):
                """Intercept every request, including actions; all input data belongs to this fixture."""
                parsed = urlparse(route.request.url)
                path = parsed.path
                if path.startswith('/api/optimize-v8/loops') and route.request.method not in ('GET', 'HEAD'):
                    mutations.append(path)
                if path in {'/audit','/api/optimize-v8/main_page'}:
                    route.fulfill(body=source.replace('navCurrent: "v8_optimize"','navCurrent: "dashboards"') if path=='/audit' else source,content_type='text/html')
                elif path.startswith('/app/'):
                    asset = ROOT/'frontend'/path.removeprefix('/app/')
                    content = asset.read_text() if asset.is_file() else ''
                    if asset.name=='pb8_loop_optimizer.js':
                        # Fail immediately if any display sort mutates the original arrays.
                        content = content.replace('  function renderWorkflow() {', '''  function renderWorkflow() {
    rows.forEach(function(run){Object.freeze(run.jobs);Object.freeze(run.observer_jobs);Object.freeze(run.cycles);});Object.freeze(rows);
''')
                    route.fulfill(body=content,content_type='text/css' if asset.suffix=='.css' else 'text/javascript')
                elif path=='/api/optimize-v8/loops':
                    route.fulfill(json={'loops':runs})
                elif path=='/api/optimize-v8/loops/configs':
                    route.fulfill(json={'configs':definitions})
                elif path=='/api/optimize-v8/loops/options':
                    route.fulfill(json={'configs':['fixture'],'profiles':[],'providers':{},'presets':[]})
                elif path.endswith('/diff'):
                    diffs.append(parse_qs(parsed.query))
                    route.fulfill(json={'fields':[],'truncated':False})
                elif path=='/api/help/index':
                    route.fulfill(json=[{'file':'43_pbv8_optimize','title':'PB8 Optimize'}])
                elif path=='/api/help/content':
                    route.fulfill(json={'content':'# PB8 Optimize\n\nIsolated sorting guide.'})
                else:
                    route.fulfill(json={})
            page.route('**/*', respond)
            page.route_web_socket('**/*', lambda socket: socket.on_message(lambda _message: None))
            page.goto('http://pbgui.test/audit')
            page.locator('.nav-group-btn[data-group="pbv8"]').click()
            page.locator('.nav-item[data-page="v8_optimize"]').click()
            page.wait_for_url('**/api/optimize-v8/main_page')
            yield page, definitions, runs, diffs
            assert not errors, errors
            assert not mutations, mutations
        finally:
            browser.close()


def test_loop_config_sort_headers_filters_reload_and_layout(loop_sort_browser):
    """Check all configuration headers, persistent independent state, real Guide and shared sidebar bounds."""
    __tracebackhide__ = True
    page, definitions, _, _ = loop_sort_browser
    page.locator('.sb-section[data-panel="loops-config"]').click()
    selector = '#loop-config-list'
    page.wait_for_selector(selector+' tbody tr[data-selection]')

    def names():
        """Read identities from the actual renderer, excluding empty-state rows."""
        return page.locator(selector+' tbody tr[data-selection]').evaluate_all('rows=>rows.map(row=>row.dataset.selection)')
    assert names()==['Config 10','Config 2']
    for key in ('name','config_name','execution','updated_at'):
        control = page.locator(selector+f' [data-sort="{key}"]')
        control.click()
        direction = control.locator('..').get_attribute('aria-sort')
        expected = ['Config 2','Config 10'] if direction=='ascending' else ['Config 10','Config 2']
        assert names()==expected
        control.press('Enter')
        assert names()==expected[::-1]
        assert control.evaluate('node=>node===document.activeElement')
    page.locator(selector+' [data-sort="name"]').click()
    page.locator(selector+' tr[data-selection="Config 2"] td').first.click()
    search = page.get_by_role('textbox', name='Search AI Loops config')
    search.fill('Config 2')
    assert names()==['Config 2']
    search.fill('')
    before = names()
    definitions[0]['updated_at'] += 2000000
    page.evaluate("document.dispatchEvent(new Event('visibilitychange'))")
    page.wait_for_function("stamp=>document.querySelector('#loop-config-list').dataset.signature.includes(String(stamp))",arg=definitions[0]['updated_at'])
    assert page.locator(selector+' th[aria-sort="ascending"] [data-sort="name"]').count()==1
    assert names()==before
    page.reload()
    page.wait_for_selector(selector+' tr[data-selection="Config 2"]')
    assert page.locator(selector+' tr[data-selection="Config 2"]').evaluate('node=>node.classList.contains("selected")'), page.evaluate("JSON.parse(localStorage.getItem('pbgui.pb8.loops.workflow')).config_selection")
    assert names()==before
    assert page.locator(selector+' th[aria-sort="ascending"] [data-sort="name"]').count()==1
    for viewport, width in ((1200,160),(1200,420),(700,160)):
        page.set_viewport_size({'width':viewport,'height':900})
        page.evaluate('(width)=>document.getElementById("sidebar").style.width=width+"px"',width)
        assert page.locator('#sidebar').evaluate('node=>node.scrollWidth<=node.clientWidth+1')
        assert page.locator(selector+' .loop-table-scroll').evaluate('node=>node.getBoundingClientRect().right<=innerWidth')
    destination = page.url
    page.locator('#pbgui-guide-btn').click()
    page.wait_for_selector('#pbgui-shared-help-ovl.visible')
    assert page.url==destination
    page.locator('#pbgui-shared-help-close').click()


@pytest.mark.parametrize('panel', ['loops-queue','loops-results'])
def test_loop_tree_sort_headers_groups_variants_and_actions(loop_sort_browser, panel):
    """Sort all tree headers through real controls and retain parents, references, expansion and variant targets."""
    __tracebackhide__ = True
    page, _, runs, diffs = loop_sort_browser
    page.locator(f'.sb-section[data-panel="{panel}"]').click()
    host = '#'+panel+'-table'
    ids = [run['id'] for run in runs if run['ended']==(panel=='loops-results')]
    page.wait_for_selector(host+' tr[data-selection]')
    assert page.locator(host+' tr[data-selection]').evaluate_all('rows=>rows.map(row=>row.dataset.selection)')==ids[::-1]
    selected = host+' tr[data-key="'+ids[1]+'"]'
    page.locator(selected+' td').nth(2).click()
    page.locator(selected+' .loop-disclosure').click()
    for number in (0,1):
        page.locator(host+f' tr[data-key="{ids[1]}:{number}"] .loop-disclosure').click()
    original = copy.deepcopy(runs)

    def keys():
        """Read the flattened display order while keeping model identities visible."""
        return page.locator(host+' tr[data-key]').evaluate_all('rows=>rows.map(row=>row.dataset.key)')
    displayed = keys()
    assert displayed.index(ids[1]+':1')<displayed.index(ids[1]+':0')
    assert [key for key in displayed if key.startswith(ids[1]+':1:')]==[
        ids[1]+':1:check1',ids[1]+':1:opt11',ids[1]+':1:opt10']
    assert displayed.index(ids[1]+':observer:obs1')<displayed.index(ids[1]+':1:check1')
    for key in ('name','status','execution','duration','backtests','metrics','selection','decision','created_at','holdout','full_range'):
        control = page.locator(host+f' [data-sort="{key}"]')
        control.click()
        direction = control.locator('..').get_attribute('aria-sort')
        control.press('Space')
        assert control.locator('..').get_attribute('aria-sort')!=direction
        assert control.evaluate('node=>node===document.activeElement')
        assert page.locator(selected).evaluate('node=>node.classList.contains("selected")')
        assert page.locator(host+' .loop-disclosure[aria-expanded="true"]').count()==3
        displayed = keys()
        start = displayed.index(ids[1])
        end = displayed.index(ids[0]) if displayed.index(ids[0])>start else len(displayed)
        assert all(key.startswith(ids[1]+':') for key in displayed[start+1:end])
        for number in (0,1):
            group_start = displayed.index(ids[1]+':'+str(number))
            child_indices = [displayed.index(ids[1]+f':{number}:{operation}') for operation in (f'check{number}',f'opt{number}0',f'opt{number}1')]
            child_indices.append(displayed.index(ids[1]+f':observer:obs{number}'))
            assert min(child_indices)>group_start
            assert max(child_indices)-min(child_indices)==3
    assert runs==original
    # The display order must not change the prior-round comparison or the original variant index.
    round_two = page.locator(host+f' tr[data-key="{ids[1]}:1"]')
    assert 'vs Loop 1' in round_two.locator('td').nth(5).locator('.loop-metric-change').get_attribute('title')
    variant = page.locator(host+f' tr[data-key="{ids[1]}:1:opt11"]')
    assert 'Optimizer Variant 2' in variant.locator('td').first.inner_text()
    variant.get_by_role('button',name='Open',exact=True).click()
    page.wait_for_selector('#loop-diff-operation')
    assert page.locator('#loop-diff-operation').input_value()=='opt11'
    page.wait_for_function("()=>!document.querySelector('#loop-report-body').textContent.includes('Loading parameter diff')")
    assert diffs[-1]['operation']==['opt11'] and diffs[-1]['baseline']==['opt01']
    page.locator('#loop-report-close').click()
    round_two.get_by_role('button',name='Open',exact=True).click()
    assert page.locator('#loop-report-title').inner_text().startswith('Round 2')
    page.locator('#loop-report-close').click()


def test_loop_duration_auto_sort_focus_selection_and_reload(loop_sort_browser):
    """Prove live duration reordering and clock-only DOM stability, filters, drag selection and reload."""
    __tracebackhide__ = True
    page, _, runs, _ = loop_sort_browser
    page.locator('.sb-section[data-panel="loops-queue"]').click()
    host = '#loops-queue-table'
    run = next(run for run in runs if run['id']=='b'*32)
    parent = host+f' tr[data-key="{run["id"]}"]'
    page.wait_for_selector(parent)
    page.locator(parent+' td').nth(2).click()
    page.locator(parent+' .loop-disclosure').click()
    round_selector = host+f' tr[data-key="{run["id"]}:0"]'
    page.locator(round_selector+' .loop-disclosure').click()
    page.locator(host+' [data-sort="duration"]').click()
    page.locator(round_selector+' .loop-disclosure').focus()
    page.locator(host+' .loop-table-scroll').evaluate('node=>node.scrollLeft=200')
    run['cycles'][0]['duration_seconds']=20
    page.wait_for_function("(id)=>{const rows=[...document.querySelectorAll('#loops-queue-table tr[data-key]')];return rows.findIndex(row=>row.dataset.key===id+':0')<rows.findIndex(row=>row.dataset.key===id+':1');}",arg=run['id'])
    assert page.locator(round_selector+' .loop-disclosure').evaluate('node=>node===document.activeElement')
    assert page.locator(host+' .loop-table-scroll').evaluate('node=>node.scrollLeft')==200
    assert page.locator(parent+'.selected').count()==1
    page.locator(host+' [data-sort="created_at"]').click()
    page.locator(round_selector+' .loop-disclosure').focus()
    page.locator(round_selector).evaluate('node=>window.clockRow=node')
    run['cycles'][0]['duration_seconds']=30
    page.wait_for_function("()=>document.querySelector('[data-clock=\"'+ 'b'.repeat(32)+':0\"]').textContent.includes('30')")
    assert page.locator(round_selector).evaluate('node=>node===window.clockRow')
    assert page.locator(round_selector+' .loop-disclosure').evaluate('node=>node===document.activeElement')
    search = page.get_by_role('textbox',name='Search AI Loops queue')
    search.fill('Run 10')
    page.locator('#panel-loops-queue select[aria-label="Status filter"]').select_option('running')
    page.locator(host+' [data-sort="backtests"]').click()
    page.locator('.sb-section[data-panel="loops-results"]').click()
    assert page.locator('#loops-results-table th[aria-sort="descending"] [data-sort="created_at"]').count()==1
    page.locator('#loops-results-table [data-sort="name"]').click()
    page.locator('.sb-section[data-panel="loops-queue"]').click()
    page.reload()
    page.wait_for_selector(parent+'.selected')
    assert search.input_value()=='Run 10'
    assert page.locator('#panel-loops-queue select[aria-label="Status filter"]').input_value()=='running'
    assert page.locator(host+' th[aria-sort="ascending"] [data-sort="backtests"]').count()==1
    assert page.locator(round_selector+' .loop-disclosure').get_attribute('aria-expanded')=='true'
    search.fill('')
    # Drag selection follows the current visible order, and polling cannot rebuild during the drag.
    selection = page.locator(host+' tr[data-selection]')
    selection.first.dispatch_event('pointerdown',{'button':0})
    signature = page.locator(host).get_attribute('data-signature')
    run['cycles'][0]['duration_seconds']=40
    selection.last.dispatch_event('pointerenter')
    assert page.locator(host+' tr[data-selection].selected').count()==2
    assert page.locator(host).get_attribute('data-signature')==signature
    page.dispatch_event('body','pointerup')
    page.locator('.sb-section[data-panel="loops-results"]').click()
    assert page.locator('#loops-results-table th[aria-sort="ascending"] [data-sort="name"]').count()==1


@pytest.mark.parametrize('first_read', ['delayed', 'network', 'http'])
def test_loop_config_actions_wait_for_loaded_selection(loop_sort_browser, first_read):
    """Keep restored selection while disabling actions during a delayed or failed first list read."""
    __tracebackhide__ = True
    browser_api = __import__('browser_driver')
    page, definitions, _, _ = loop_sort_browser
    page.locator('.sb-section[data-panel="loops-config"]').click()
    selected = '#loop-config-list tr[data-selection="Config 2"]'
    page.locator(selected+' td').first.click()
    page.locator('#loop-config-list [data-sort="name"]').click()
    search = page.get_by_role('textbox', name='Search AI Loops config')
    search.fill('Config 2')
    state = {'hold':True, 'pending':[]}

    def hold_definitions(route):
        """Delay only the isolated definition list; automatic retries use the same local fixture."""
        if state['hold']:
            state['pending'].append(route)
        else:
            route.fulfill(json={'configs':definitions})

    try:
        page.route('**/api/optimize-v8/loops/configs', hold_definitions)
        page.reload(wait_until='domcontentloaded')
        controls = [page.locator('#loop-'+name+'-config') for name in ('edit','duplicate','queue','delete')]
        browser_api.expect(controls[0]).to_be_visible()
        for control in controls:
            browser_api.expect(control).to_be_disabled()
        assert page.evaluate("JSON.parse(localStorage.getItem('pbgui.pb8.loops.workflow')).config_selection")==['Config 2']
        assert state['pending']
        # Programmatic dispatch must also be harmless if a previously scheduled callback runs.
        for control in controls:
            control.dispatch_event('click')
        assert page.locator('#pbgui-dialog-ovl.visible').count()==0
        assert not page.locator('#loop-message').inner_text()
        initial = state['pending'].pop(0)
        if first_read!='delayed':
            if first_read=='network':
                initial.abort('failed')
                message = 'Connection interrupted. Retrying automatically'
            else:
                initial.fulfill(status=503, json={'detail':'Isolated definition list unavailable'})
                message = 'Isolated definition list unavailable'
            browser_api.expect(page.locator('#loop-message')).to_contain_text(message)
            for control in controls:
                browser_api.expect(control).to_be_disabled()
            assert page.evaluate("JSON.parse(localStorage.getItem('pbgui.pb8.loops.workflow')).config_selection")==['Config 2']
            state['hold'] = False
            if first_read=='http':
                # Existing non-transient HTTP errors retry on returning to the view.
                page.locator('.sb-section[data-panel="loops-results"]').click()
                page.locator('.sb-section[data-panel="loops-config"]').click()
            # Transient network failures recover through the existing automatic poll.
        else:
            state['hold'] = False
            initial.fulfill(json={'configs':definitions})
        while state['pending']:
            state['pending'].pop(0).fulfill(json={'configs':definitions})
        browser_api.expect(page.locator(selected)).to_have_class(re.compile(r'\bselected\b'))
        for control in controls:
            browser_api.expect(control).to_be_enabled()
        assert search.input_value()=='Config 2'
        assert page.locator('#loop-config-list th[aria-sort="ascending"] [data-sort="name"]').count()==1
        browser_api.expect(page.locator('#loop-message')).to_be_empty()
    finally:
        state['hold'] = False
        page.unroute('**/api/optimize-v8/loops/configs', hold_definitions)
        while state['pending']:
            state['pending'].pop(0).abort()
