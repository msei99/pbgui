"""Real optimizer shell and Loop tab against isolated authenticated fixture APIs."""
from pathlib import Path
import json
import copy
from urllib.parse import urlparse, parse_qs, parse_qs

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize("viewport_width", [700, 1200, 1920])
def test_pb8_loop_navigation_setup_polling_and_restore(viewport_width):
    """Exercise real menu navigation, dynamic settings and preserved draft inputs."""
    playwright = pytest.importorskip('playwright.sync_api')
    source = (ROOT / 'frontend/v7_optimize.html').read_text()
    replacements = {'API_BASE': '/api/optimize-v8', 'OPTIMIZE_VERSION': 'v8', 'VERSION': 'test', 'SERIAL': '1', 'NAV_HASH': 'test', 'BASE_PREFIX':'', 'LIMITS_META':'{}', 'BACKTEST_VERSION':'v8', 'OPTIMIZE_NAV_TITLE':'PB8 Optimize', 'OPTIMIZE_NAV_CURRENT':'v8_optimize'}
    for key, value in replacements.items():
        source = source.replace('%%' + key + '%%', value)
    loops, starts, errors, diff_queries, continuations = [], [], [], [], []
    continuation_delay = {'enabled': False, 'pending': []}
    knowledge_fixture = {'entries': [], 'fail': False}
    knowledge_queries = []
    instruction_versions = {'default':{'id':'default','name':'PBGui default','text':'Default Loop instructions','digest':'default-hash','created_at':None}}
    instruction_catalog = {'active':'default','revision':0}
    cloud_jobs, log_targets, log_subscriptions, native_status_requests = [], [], [], []
    definitions = {}
    from api.vast import RentalPreferences
    rental_preferences = RentalPreferences(hours=12, budget=5, max_rentals=3, idle_seconds=300).model_dump()
    options = {'configs':['fixture','alternative'], 'providers': {'chatgpt': {'connected':True}}, 'profiles':[{'id':'default','name':'Default'}], 'gpu_available':True,'gpu_reason':'', 'vast_connected':True, 'vast':{'hours':12,'gpu_name':'RTX 3060','budget':5,'max_rentals':3}, 'presets':['gain','drawdown','uptrend']}
    with playwright.sync_playwright() as driver:
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
            elif path.endswith('/action') and path.startswith('/api/optimize-v8/loops/'):
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
            elif path.endswith('/diff') and path.startswith('/api/optimize-v8/loops/'):
                query=parse_qs(urlparse(route.request.url).query);diff_queries.append(query)
                route.fulfill(json={'fields':[{'path':'optimize.limits.2.value','before':None,'after':.25}],'truncated':False})
            elif path.endswith('/log-target') and path.startswith('/api/optimize-v8/loops/'):
                operation=parse_qs(urlparse(route.request.url).query)['operation'][0]
                log_targets.append(operation)
                route.fulfill(json={'id':'d'*32 if operation=='2'*32 else '11111111-1111-1111-1111-111111111111','name':'Native optimizer' if operation in {'1'*32,'2'*32} else 'Specific backtest','kind':'optimizer' if operation in {'1'*32,'2'*32} else 'validation','cloud':operation=='2'*32})
            elif path.startswith('/api/optimize-v8/queue/') and path.endswith('/status'):
                native_status_requests.append(path.split('/')[-2])
                route.fulfill(json={'name':'Native optimizer','phase':'running','progress':{},'runtime':{},'queue':{},'log':{}})
            elif path == '/api/optimize-v8/loops':
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
        assert page.locator('.loop-section > h3').all_text_contents() == ['Starting configuration', 'Coins and positions', 'Result goals', 'Run limits']
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
        assert starts[0]['model']=='pinned' and starts[0]['effort']=='high' and starts[0]['service_tier']=='priority'
        assert page.url.endswith('#loops-queue')
        assert loops[0]['status']=='queued' and loops[0]['ai_calls']==0
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
        loops[0]['bootstrap']={'status':'applied','candidate_count':1,'sources':['saved_vast_run'],
                              'reason':'Historical assessment <script>unsafe()</script>','knowledge':'Requires exact validation'}
        first,second,check1,check2=[digit*32 for digit in ('1','2','3','4')]
        loops[0]['jobs']=[
            {'operation':first,'name':'run_first','kind':'optimizer','round':0,'status':'completed','started':True,'reason':'Initial run','changes':[],'native_iters':10000000,'created_at':1000,'run_started_at':1010,'ended_at':1050},
            {'operation':second,'name':'run_second','kind':'optimizer','round':1,'status':'completed','started':True,'reason':'Reduce recovery duration <script>unsafe()</script>',
             'native_iters':10000000,'changes':[{'path':'optimize.limits.2.value','before':None,'after':.25}],'created_at':1100,'run_started_at':1110,'ended_at':1150},
            {'operation':check1,'name':'validate_first','kind':'validation','round':0,'status':'completed','candidate_optimizer':first},
            {'operation':check2,'name':'validate_second','kind':'validation','round':1,'status':'completed','candidate_optimizer':second}]
        loops[0]['validation_count']=2
        for job in loops[0]['jobs']:
            job['log_available']=True
        loops[0]['observer_enabled']=True
        loops[0]['observer_jobs']=[{
            'operation':digit*32,'name':'user_evaluation_'+digit,'kind':kind,'round':number,
            'status':'complete','display_status':'complete','simulation_complete':True,'report_done':True,
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
            cycle['goals'].append({'goal':'drawdown','metrics':['drawdown_worst_strategy_eq'],'direction':'min',
                'value':.3 if number==0 else .5,'previous':None if number==0 else .3,
                'delta':None if number==0 else .2,'confirmed':False})
            cycle['observations'][0].update(operation=check1 if number==0 else check2,
                assessment={'comparable':True,'simulation_complete':True,'goals':[
                    {'goal':goal['goal'],'metric':goal['metrics'][0],'direction':goal['direction'],'value':goal['value']}
                    for goal in cycle['goals']]})
        page.evaluate("document.dispatchEvent(new Event('visibilitychange'))")
        page.wait_for_function("document.querySelector('#loops-queue-table').textContent.includes('Loop 2')")
        assert 'Reduce recovery duration' not in page.locator('#loops-queue-table').inner_text()
        progress=page.locator('#loops-queue-table tr[data-key="'+'a'*32+':1"]')
        assert 'Gain 1.2 ×' in progress.inner_text() and 'Drawdown 50 %' in progress.inner_text()
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
        page.mouse.move(before['x']+before['width']-3,before['y']+before['height']-3)
        page.mouse.down();page.mouse.move(before['x']+before['width']-63,before['y']+before['height']-23,steps=5);page.mouse.up()
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
        assert 'Best Holdout' in page.locator('#loop-report-body tbody tr').filter(has_text='Loop 2 · Holdout · Comparison backtest 2').inner_text()
        assert 'Holdout score' in page.locator('#loop-report-body').inner_text()
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
