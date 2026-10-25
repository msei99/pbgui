"""Exercise the complete native Backtest startup for AI Loop result links offline."""
from pathlib import Path
import json
from urllib.parse import urlencode, urlparse, parse_qs

import pytest


ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize('stored_panel,result_count,missing_equity', [('queue',3,False), ('configs',3,False), ('results',3,False), ('queue',1,False), ('queue',0,False), ('queue',3,True)])
def test_loop_result_handoff_overrides_stored_panel_and_preserves_return(stored_panel, result_count, missing_equity):
    """An explicit result link wins on arrival; later navigation wins on reload."""
    playwright = pytest.importorskip('playwright.sync_api')
    source = (ROOT / 'frontend/v7_backtest.html').read_text()
    replacements = {'API_BASE':'/api/backtest-v8', 'BACKTEST_VERSION':'v8',
                    'BACKTEST_LABEL':'PB8', 'BASE_PREFIX':'', 'VERSION':'test',
                    'SERIAL':'1', 'NAV_HASH':'test'}
    for key, value in replacements.items():
        source = source.replace('%%'+key+'%%', value)
    errors, filters, equity_paths = [], [], []
    curve_count=result_count-(1 if missing_equity else 0)
    context = '/api/optimize-v8/main_page?loop_id='+'a'*32+'&loop_view=cycle&loop_cycle=4&loop_tab=performance#loops-results'
    with playwright.sync_playwright() as driver:
        browser = driver.chromium.launch(headless=True)
        page = browser.new_page(viewport={'width':1200,'height':900})
        page.on('pageerror', lambda error: errors.append(str(error)))

        def respond(route):
            """Serve real local browser assets and isolated API data only."""
            parsed = urlparse(route.request.url)
            if parsed.path == '/api/backtest-v8/main_page':
                route.fulfill(body=source, content_type='text/html')
            elif parsed.path.startswith('/app/'):
                asset = ROOT / 'frontend' / parsed.path.removeprefix('/app/')
                route.fulfill(body=asset.read_bytes() if asset.is_file() else b'',
                              content_type='text/css' if asset.suffix=='.css' else 'text/javascript')
            elif parsed.path == '/api/backtest-v8/results':
                filters.append(parse_qs(parsed.query).get('name', [''])[0])
                offset=int(parse_qs(parsed.query).get('offset',['0'])[0])
                indices=range(result_count) if result_count<2 else ([0] if offset==0 else range(1,result_count))
                route.fulfill(json={'results':[{'path':'loop_fixture/result_'+str(index), 'config_name':'loop_fixture',
                    'exchange':'binance','name':'loop_fixture', 'start_date':'2020-01-01',
                    'end_date':'2020-02-01','analysis':{}} for index in indices] + [
                    {'path':'other_job/result','config_name':'other_job','exchange':'binance','analysis':{}}],
                    'pagination':{'has_more':result_count>1 and offset==0,'next_offset':1}})
            elif parsed.path == '/api/backtest-v8/results/equity':
                equity_paths.append(parse_qs(parsed.query)['path'][0])
                if missing_equity and equity_paths[-1]=='loop_fixture/result_2':
                    route.fulfill(status=404,body='Fixture equity is unavailable')
                    return
                route.fulfill(body='datetime,usd_total_balance,usd_total_equity\n2020-01-01,1000,1000\n2020-01-02,1100,1090\n', content_type='text/csv')
            elif parsed.path == '/api/help/index':
                route.fulfill(json=[])
            elif parsed.path == '/api/optimize-v8/main_page':
                route.fulfill(body='<main>Returned to AI Loop</main>',content_type='text/html')
            else:
                route.fulfill(json={})

        page.route('**/*',respond)
        page.route_web_socket('**/*',lambda socket: socket.on_message(lambda message: None))
        page.add_init_script("if(!localStorage.getItem('pbgui:v8_backtest:view_state'))localStorage.setItem('pbgui:v8_backtest:view_state',JSON.stringify({panel:"+repr(stored_panel)+"}));")
        page.goto('http://pbgui.test/api/backtest-v8/main_page?'+urlencode({'panel':'results','result_filter':'loop_fixture','result_compare':'1','loop_return':context}))
        page.wait_for_function("currentPanel==='results' && document.querySelector('#compare-chart-area').style.display!== 'none'",timeout=5000)
        if result_count:
            page.wait_for_function("(count)=>document.querySelector('#compare-chart-div')?.data?.length===count*2",arg=curve_count)
            assert page.evaluate('getSelectedResults().length')==result_count
            if missing_equity:
                assert page.locator('#compare-chart-area [role="status"]').inner_text()=='1 of 3 selected results could not load equity data.'
            assert set(equity_paths)=={'loop_fixture/result_'+str(index) for index in range(result_count)}
            assert page.locator('#results-config-filter').input_value()=='loop_fixture'
            assert len(filters)==(2 if result_count>1 else 1),'exactly one initial filtered page sequence'
            measures=page.evaluate("performance.getEntriesByType('measure').filter(x=>x.name.startsWith('pbgui.backtest.')).map(x=>({name:x.name,duration:x.duration}))")
            assert {'pbgui.backtest.filtered-results','pbgui.backtest.document-results','pbgui.backtest.equity-prepare','pbgui.backtest.chart'}<=set(x['name'] for x in measures)
            assert all(x['duration']>=0 for x in measures)
            print('Isolated Backtest timings (ms):',measures)
        else:
            assert 'No results are available for this backtest job.' in page.locator('#compare-chart-area').inner_text()
            assert not equity_paths
        assert page.locator('#panel-results').is_visible()
        assert not page.locator('#panel-queue').is_visible()
        assert page.get_by_role('button',name='Back to AI Loop').is_visible()
        assert filters and set(filters)=={'loop_fixture'}
        page.reload()
        page.wait_for_function("currentPanel==='results' && document.querySelector('#compare-chart-area').style.display!=='none'")
        if result_count:
            page.wait_for_function("(count)=>document.querySelector('#compare-chart-div')?.data?.length===count*2",arg=curve_count)
            page.evaluate('compareSelected()')
            page.reload()
            page.wait_for_function("currentPanel==='results' && document.querySelector('#results-config-filter').value==='loop_fixture'")
            assert not page.locator('#compare-chart-area').is_visible()
        page.evaluate("selectPanel('queue')")
        page.reload()
        page.wait_for_function("currentPanel==='queue'")
        assert page.get_by_role('button',name='Back to AI Loop').is_visible()
        page.get_by_role('button',name='Back to AI Loop').click()
        page.wait_for_url('**/api/optimize-v8/main_page?**')
        assert page.url == 'http://pbgui.test'+context
        page.goto('http://pbgui.test/api/backtest-v8/main_page')
        page.wait_for_function("currentPanel==='queue'")
        assert not page.get_by_role('button',name='Back to AI Loop',include_hidden=True).is_visible()
        assert not errors, errors
        browser.close()


@pytest.mark.parametrize('kind,prefix', [('observer_holdout',''),('observer_full_range',''),('observer_holdout','/nested')])
def test_all_loop_evaluations_compare_native_results_and_restore(kind, prefix):
    """Compare every complete candidate, preserve selection and restore the Loop context."""
    playwright = pytest.importorskip('playwright.sync_api')
    source = (ROOT / 'frontend/v7_backtest.html').read_text()
    for key,value in {'API_BASE':prefix+'/api/backtest-v8','BACKTEST_VERSION':'v8','BACKTEST_LABEL':'PB8',
                      'BASE_PREFIX':prefix,'VERSION':'test','SERIAL':'1','NAV_HASH':'test'}.items():
        source=source.replace('%%'+key+'%%',value)
    run_id='a'*32
    jobs=[{'name':'evaluation_'+str(index),'kind':kind,'round':index-1,'candidate_id':str(index),
           'comparison_label':'Comparison backtest '+str(index),'status':'complete','simulation_complete':True,
           'log_available':True,'evaluation_score':[1,100,3][index],'evaluation_targets_met':index!=1} for index in range(3)]
    jobs += [dict(jobs[0],name='wrong_kind',kind='observer_full_range' if kind=='observer_holdout' else 'observer_holdout'),
             dict(jobs[0],name='incomplete',simulation_complete=False),dict(jobs[0],name='failed',status='failed')]
    errors,requested,equity=[],[],[]
    context=prefix+'/api/optimize-v8/main_page?loop_id='+run_id+'&loop_view=run&loop_tab=performance#loops-results'
    with playwright.sync_playwright() as driver:
        browser=driver.chromium.launch(headless=True)
        page=browser.new_page(viewport={'width':1200,'height':900})
        page.on('pageerror',lambda error:errors.append(str(error)))
        def respond(route):
            """Route every request to in-memory fixtures and real local assets."""
            parsed=urlparse(route.request.url)
            path=parsed.path.removeprefix(prefix) if prefix else parsed.path
            query=parse_qs(parsed.query)
            if path=='/api/backtest-v8/main_page':
                route.fulfill(body=source,content_type='text/html')
            elif path.startswith('/app/'):
                asset=ROOT/'frontend'/path.removeprefix('/app/')
                route.fulfill(body=asset.read_bytes() if asset.is_file() else b'',content_type='text/css' if asset.suffix=='.css' else 'text/javascript')
            elif path=='/api/optimize-v8/loops/'+run_id:
                route.fulfill(json={'id':run_id,'observer_jobs':jobs})
            elif path=='/api/backtest-v8/results':
                name=query.get('name',[''])[0];offset=int(query.get('offset',['0'])[0]);requested.append(name)
                assert name.startswith('evaluation_'), 'Do not scan unrelated results'
                route.fulfill(json={'results':[{'path':name+'/result_'+str(offset),'config_name':name,'exchange':'bybit' if offset else 'hyperliquid',
                    'exchange_dir':'bybit' if offset else 'hyperliquid','start_date':'2020-01-01','end_date':'2020-02-01','analysis':{}}],
                    'pagination':{'has_more':offset==0,'next_offset':offset+1}})
            elif path=='/api/backtest-v8/results/equity':
                equity.append(query['path'][0])
                route.fulfill(body='datetime,usd_total_balance,usd_total_equity\n2020-01-01,1000,1000\n2020-01-02,1100,1090\n',content_type='text/csv')
            elif path=='/api/help/index':route.fulfill(json=[])
            elif path=='/api/optimize-v8/main_page':route.fulfill(body='<main>Returned</main>',content_type='text/html')
            else:route.fulfill(json={})
        page.route('**/*',respond)
        page.route_web_socket('**/*',lambda socket:socket.on_message(lambda message:None))
        page.goto('http://pbgui.test'+prefix+'/api/backtest-v8/main_page?'+urlencode({'panel':'results','result_loop':run_id,'result_evaluation':kind,'loop_return':context}))
        page.wait_for_function('results.length===6')
        assert set(requested)=={'evaluation_0','evaluation_1','evaluation_2'}
        assert len(requested)==6,'each evaluation page is initially requested once'
        assert not page.locator('#compare-chart-area').is_visible()
        assert equity==[]
        page.evaluate("loadResults('',{keepVisible:true})")
        assert equity==[]
        page.reload()
        page.wait_for_function('results.length===6')
        assert equity==[] and not page.locator('#compare-chart-area').is_visible()
        page.locator('#loop-evaluation-mode').select_option('top')
        page.locator('#loop-evaluation-count').fill('1')
        assert equity==[]
        page.get_by_role('button',name='Apply comparison',exact=True).click()
        page.wait_for_function("document.querySelector('#compare-chart-div')?.data?.length===4")
        assert set(equity)=={'evaluation_2/result_0','evaluation_2/result_1'}
        jobs.append(dict(jobs[2],name='evaluation_3',round=2,evaluation_score=9))
        page.evaluate("loadResults('',{keepVisible:true})")
        page.wait_for_function("getSelectedResults().every(path=>path.startsWith('evaluation_3/'))")
        mode=page.locator('#loop-evaluation-mode')
        count=page.locator('#loop-evaluation-count')
        apply=page.get_by_role('button',name='Apply comparison',exact=True)
        assert mode.is_visible() and count.is_enabled()
        for width in [700,900,1200]:
            page.set_viewport_size({'width':width,'height':900})
            assert page.locator('#loop-evaluation-controls').evaluate('node=>node.scrollWidth<=node.clientWidth')
            assert apply.is_visible()
        handle=page.locator('#sidebar-resize').bounding_box()
        if handle:
            for target in [420,160]:
                handle=page.locator('#sidebar-resize').bounding_box()
                page.mouse.move(handle['x']+handle['width']/2,handle['y']+20)
                page.mouse.down();page.mouse.move(target,handle['y']+20,steps=5);page.mouse.up()
                assert page.locator('#loop-evaluation-controls').evaluate('node=>node.scrollWidth<=node.clientWidth')
        mode.select_option('top');count.fill('1');apply.click()
        page.wait_for_function("getSelectedResults().length===2 && getSelectedResults().every(path=>path.startsWith('evaluation_3/'))")
        # Hard-target compliance outranks the higher score of candidate 1.
        page.wait_for_function("document.querySelector('#compare-chart-div')?.data?.length===4")
        assert parse_qs(urlparse(page.url).query)['result_top']==['1']
        jobs[0]['evaluation_score']=20
        count.focus()
        page.evaluate("loadResults('',{keepVisible:true})")
        page.wait_for_function("getSelectedResults().every(path=>path.startsWith('evaluation_0/'))")
        assert count.evaluate('node=>document.activeElement===node')
        count.fill('2');apply.click()
        page.wait_for_function("getSelectedResults().length===4")
        page.reload()
        page.wait_for_function("document.querySelector('#compare-chart-div')?.data?.length===8")
        assert count.input_value()=='2' and mode.input_value()=='top'
        assert set(path.split('/')[0] for path in page.evaluate('getSelectedResults()'))=={'evaluation_0','evaluation_3'}
        mode.select_option('all');apply.click()
        page.wait_for_function("getSelectedResults().length===8 && document.querySelector('#compare-chart-div')?.data?.length===16")
        assert 'result_top' not in parse_qs(urlparse(page.url).query)
        # Automatic updates preserve a user's narrowed selection.
        page.evaluate("setSelectedResults(getSelectedResults().slice(0,2))")
        page.evaluate("loadResults('',{keepVisible:true})")
        assert page.evaluate('getSelectedResults().length')==2
        page.reload()
        page.wait_for_function("document.querySelector('#compare-chart-div')?.data?.length===4")
        assert page.evaluate('getSelectedResults().length')==2
        page.evaluate('compareSelected()')
        page.reload()
        page.wait_for_function('results.length===8')
        assert not page.locator('#compare-chart-area').is_visible()
        assert page.get_by_role('button',name='Back to AI Loop').is_visible()
        page.get_by_role('button',name='Back to AI Loop').click()
        page.wait_for_url('http://pbgui.test'+context)
        assert not errors,errors
        browser.close()


@pytest.mark.parametrize('kind,prefix,ended', [('observer_holdout','',False),('observer_full_range','/nested',True)])
def test_loop_round_trip_keeps_report_and_restarts_cancelled_handoff(kind,prefix,ended):
    """Real menu, delayed options/rental, rerender click lock, BFCache lifecycle and cold return."""
    playwright=pytest.importorskip('playwright.sync_api')
    optimize=(ROOT/'frontend/v7_optimize.html').read_text()
    backtest=(ROOT/'frontend/v7_backtest.html').read_text()
    values={'API_BASE':prefix+'/api/optimize-v8','OPTIMIZE_VERSION':'v8','VERSION':'test',
        'SERIAL':'1','NAV_HASH':'test','BASE_PREFIX':prefix,'LIMITS_META':'{}','BACKTEST_VERSION':'v8',
        'OPTIMIZE_NAV_TITLE':'PB8 Optimize','OPTIMIZE_NAV_CURRENT':'v8_optimize','BACKTEST_LABEL':'PB8'}
    for key,value in values.items():
        optimize=optimize.replace('%%'+key+'%%',value)
        backtest=backtest.replace('%%'+key+'%%',prefix+'/api/backtest-v8' if key=='API_BASE' else value)
    run_id='a'*32;panel='loops-results' if ended else 'loops-queue'
    jobs=[{'operation':format(index+1,'032x'),'name':'evaluation_'+str(index),'round':1,'kind':kind,
        'status':'complete','simulation_complete':True,'report_done':True,'log_available':True,'metrics':{}}
        for index in range(16)]
    run={'id':run_id,'name':'Isolated navigation','status':'completed' if ended else 'running','phase':'evaluate',
        'round':1,'ended':ended,'created_at':1000,'settings':{'config_name':'fixture','execution':'vast'},
        'jobs':[],'history':[],'observer_jobs':jobs,'observer_enabled':True,
        'cycles':[{'round':1,'status':'validated','ai_evaluated':True,'goals':[],'backtests':0,'completed_backtests':0}]}
    loops=[run];held_targets=[];held_options=[];held_rentals=[];errors=[];target_count=[];target_timings=[]
    with playwright.sync_playwright() as driver:
        # This part deliberately exercises full navigation; persisted lifecycle is simulated separately below.
        browser=driver.chromium.launch(headless=True,args=['--disable-features=BackForwardCache'])
        page=browser.new_page(viewport={'width':1200,'height':900})
        page.set_default_timeout(7000)
        page.add_init_script("window.addEventListener('pagehide',()=>console.info('loop-target-timing:'+JSON.stringify(performance.getEntriesByName('pbgui.loop.target').map(x=>x.duration))))")
        page.on('console',lambda message:target_timings.extend(json.loads(message.text.removeprefix('loop-target-timing:'))) if message.text.startswith('loop-target-timing:') else None)
        page.on('pageerror',lambda error:errors.append(str(error)))
        def respond(route):
            """All data and pending responses are fixture-owned, including mounted assets."""
            parsed=urlparse(route.request.url);path=parsed.path.removeprefix(prefix) if prefix else parsed.path
            query=parse_qs(parsed.query)
            if path in {'/audit','/api/optimize-v8/main_page'}:
                route.fulfill(body=optimize.replace('navCurrent: "v8_optimize"','navCurrent: "dashboards"') if path=='/audit' else optimize,content_type='text/html')
            elif path=='/api/backtest-v8/main_page':
                route.fulfill(body=backtest,content_type='text/html')
            elif path.startswith('/app/'):
                asset=ROOT/'frontend'/path.removeprefix('/app/')
                route.fulfill(body=asset.read_bytes() if asset.is_file() else b'',content_type='text/css' if asset.suffix=='.css' else 'text/javascript')
            elif path=='/api/optimize-v8/loops':route.fulfill(json={'loops':loops})
            elif path=='/api/optimize-v8/loops/configs':route.fulfill(json={'configs':[]})
            elif path=='/api/optimize-v8/loops/options':held_options.append(route)
            elif path.endswith('/rental'):held_rentals.append(route)
            elif path.endswith('/log-target'):
                target_count.append(query['operation'][0]);held_targets.append(route)
            elif path=='/api/backtest-v8/results':
                route.fulfill(json={'results':[{'path':'evaluation/result','config_name':'evaluation_0','exchange':'binance','analysis':{}}]})
            elif path=='/api/backtest-v8/results/equity':
                route.fulfill(body='datetime,usd_total_balance,usd_total_equity\n2020-01-01,1000,1000\n2020-01-02,1100,1090\n',content_type='text/csv')
            elif path=='/api/help/index':route.fulfill(json=[])
            else:route.fulfill(json={})
        page.route('**/*',respond)
        page.route_web_socket('**/*',lambda socket:socket.on_message(lambda _message:None))
        page.goto('http://pbgui.test'+prefix+'/audit')
        page.locator('.nav-group-btn[data-group="pbv8"]').click()
        page.locator('.nav-item[data-page="v8_optimize"]').click()
        page.wait_for_url('**/api/optimize-v8/main_page')
        page.locator('.sb-section[data-panel="'+panel+'"]').click()
        parent='#'+panel+'-table tr[data-key="'+run_id+'"]'
        cycle='#'+panel+'-table tr[data-key="'+run_id+':1"]'
        page.locator(parent+' .loop-disclosure').click()
        page.locator(cycle).dblclick()
        page.locator('#loop-report-tab-performance').click()
        page.wait_for_selector('#loop-report-body [data-loop-result]')
        assert held_options,'primary report does not wait for options'
        if not ended:assert held_rentals,'primary report does not wait for rental'
        page.locator('#loop-report-body [data-loop-result]').first.click()
        page.wait_for_function("document.querySelector('#loop-report-body [aria-busy=true]')!==null")
        assert page.locator('#loop-report-body [data-loop-result]').first.inner_text()=='Opening…'
        # Poll/render while resolving must retain the click lock.
        run['reason']='fixture changed'
        page.evaluate("document.dispatchEvent(new Event('visibilitychange'))")
        page.wait_for_function("[...document.querySelectorAll('#loop-report-body [data-loop-result]')].every(node=>node.disabled)")
        assert len(target_count)==1
        # Simulate BFCache suspension while the destination request is pending.
        page.evaluate("window.dispatchEvent(new PageTransitionEvent('pagehide',{persisted:true}));window.dispatchEvent(new PageTransitionEvent('pageshow',{persisted:true}));")
        page.wait_for_function("document.querySelector('#loop-report-body [data-loop-result]')?.disabled===false")
        page.locator('#loop-report-body').evaluate('node=>node.scrollTop=180')
        # Use dispatch_event so Playwright does not scroll the fixture report to the first row.
        page.locator('#loop-report-body [data-loop-result]').first.dispatch_event('click')
        page.wait_for_function("document.querySelector('#loop-report-body [aria-busy=true]')!==null")
        page.wait_for_function('true')
        assert len(target_count)==2,'cancelled handoff is restartable'
        scroll=page.locator('#loop-report-body').evaluate('node=>node.scrollTop')
        assert scroll>0
        held_targets[-1].fulfill(json={'name':'evaluation_0','id':'1'*32,'cloud':False,'kind':kind})
        page.wait_for_url('**/api/backtest-v8/main_page?**')
        scroll=page.evaluate("JSON.parse(sessionStorage.getItem('pbgui.loop.scroll')).report")
        page.wait_for_function("document.querySelector('#compare-chart-div')?.data?.length===2")
        assert urlparse(page.url).path==prefix+'/api/backtest-v8/main_page'
        assert target_timings and target_timings[-1]>=0
        print('Isolated click to target resolution (ms):',target_timings)
        print('Isolated Backtest phases (ms):',page.evaluate("performance.getEntriesByType('measure').filter(x=>x.name.startsWith('pbgui.backtest.')).map(x=>({name:x.name,duration:x.duration}))"))
        page.get_by_role('button',name='Back to AI Loop').click()
        page.wait_for_url('**/api/optimize-v8/main_page?**')
        page.wait_for_function("document.querySelector('#loop-report-tab-performance')?.getAttribute('aria-selected')==='true'")
        page.wait_for_selector('#loop-report-body [data-loop-result]')
        assert 'Round 2' in page.locator('#loop-report-title').inner_text()
        assert page.locator(cycle).is_visible()
        assert page.locator(parent).evaluate("node=>node.classList.contains('selected')")
        try:
            page.wait_for_function('(top)=>document.querySelector("#loop-report-body").scrollTop===top',arg=scroll,timeout=2000)
        except playwright.TimeoutError:
            pytest.fail('Scroll restoration: '+str(page.evaluate("({url:location.href,actual:document.querySelector('#loop-report-body').scrollTop,saved:sessionStorage.getItem('pbgui.loop.scroll')})"))+' expected='+str(scroll))
        print('Isolated cold return to Loop report (ms):',page.evaluate("performance.getEntriesByName('pbgui.loop.return-report').map(x=>x.duration)"))
        page.reload()
        page.wait_for_function("document.querySelector('#loop-report-tab-performance')?.getAttribute('aria-selected')==='true'")
        try:
            page.wait_for_function('(top)=>document.querySelector("#loop-report-body").scrollTop===top',arg=scroll,timeout=2000)
        except playwright.TimeoutError:
            pytest.fail('Scroll restoration: '+str(page.evaluate("({url:location.href,actual:document.querySelector('#loop-report-body').scrollTop,saved:sessionStorage.getItem('pbgui.loop.scroll')})"))+' expected='+str(scroll))
        timings=page.evaluate("performance.getEntriesByType('measure').filter(x=>x.name==='pbgui.loop.return-report').map(x=>x.duration)")
        assert timings and timings[0]>=0
        print('Isolated Loop document to report (ms):',timings)
        older_options=held_options[-1]
        with page.expect_request('**/loops/options?**'):
            page.evaluate("document.dispatchEvent(new Event('visibilitychange'))")
        page.wait_for_function('true')
        latest_options=held_options[-1]
        assert latest_options is not older_options
        latest_options.fulfill(json={'configs':['current'],'profiles':[],'providers':{},'presets':[]})
        page.wait_for_function("document.querySelector('#loop-config').value==='current'")
        older_options.fulfill(json={'configs':['stale'],'profiles':[],'providers':{},'presets':[]})
        page.wait_for_function('true')
        assert page.locator('#loop-config').input_value()=='current','stale options cannot overwrite newer choices'
        loops.clear()
        page.evaluate("document.dispatchEvent(new Event('visibilitychange'))")
        page.wait_for_function("document.querySelector('#'+location.hash.slice(1)+'-message')?.textContent.includes('no longer available')")
        assert not page.locator('#loop-report-overlay').evaluate("node=>node.classList.contains('is-open')")
        assert not errors,errors
        browser.close()


@pytest.mark.parametrize('version', ['v7','v8'])
def test_ordinary_backtest_startup_and_later_panel_navigation(version):
    """The shared template keeps ordinary PB7/PB8 panels and lazy configs/archive requests usable."""
    playwright=pytest.importorskip('playwright.sync_api')
    source=(ROOT/'frontend/v7_backtest.html').read_text()
    for key,value in {'API_BASE':'/api/backtest-'+version,'BACKTEST_VERSION':version,'BACKTEST_LABEL':'PB'+version[1:],
        'BASE_PREFIX':'','VERSION':'test','SERIAL':'1','NAV_HASH':'test'}.items():
        source=source.replace('%%'+key+'%%',value)
    requests=[];errors=[]
    with playwright.sync_playwright() as driver:
        browser=driver.chromium.launch(headless=True)
        page=browser.new_page(viewport={'width':1200,'height':900})
        page.on('pageerror',lambda error:errors.append(str(error)))
        def respond(route):
            """Serve the ordinary template with isolated native result fixtures."""
            path=urlparse(route.request.url).path;requests.append(path)
            if path=='/api/backtest-'+version+'/main_page':route.fulfill(body=source,content_type='text/html')
            elif path.startswith('/app/'):
                asset=ROOT/'frontend'/path.removeprefix('/app/')
                route.fulfill(body=asset.read_bytes() if asset.is_file() else b'',content_type='text/css' if asset.suffix=='.css' else 'text/javascript')
            elif path.endswith('/results'):
                route.fulfill(json={'results':[{'path':'fixture/result','config_name':'fixture','exchange':'binance','analysis':{}}]})
            elif path=='/api/help/index':route.fulfill(json=[])
            else:route.fulfill(json={})
        page.route('**/*',respond)
        page.route_web_socket('**/*',lambda socket:socket.on_message(lambda _message:None))
        page.add_init_script("localStorage.setItem('pbgui:"+version+"_backtest:view_state',JSON.stringify({panel:'queue'}))")
        # PB7 ignores PB8-only arrival parameters and restores the stored panel.
        query='?panel=results&result_filter=fixture&result_compare=1' if version=='v7' else ''
        page.goto('http://pbgui.test/api/backtest-'+version+'/main_page'+query)
        page.wait_for_function("currentPanel==='queue'")
        assert not any(path.endswith('/results') for path in requests)
        page.locator('.sb-section[data-panel="results"]').click()
        page.wait_for_function('results.length===1')
        assert page.locator('#panel-results').is_visible()
        page.locator('.sb-section[data-panel="configs"]').click()
        page.wait_for_function("currentPanel==='configs'")
        page.wait_for_function('true')
        assert '/api/backtest-'+version+'/configs' in requests
        assert any('/archives' in path for path in requests),'ordinary Results still loads archive controls'
        assert not errors,errors
        browser.close()
