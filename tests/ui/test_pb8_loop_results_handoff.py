"""Exercise the complete native Backtest startup for AI Loop result links offline."""
from pathlib import Path
from urllib.parse import urlencode, urlparse, parse_qs

import pytest


ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize('stored_panel,result_count', [('queue',3), ('configs',3), ('results',3), ('queue',1), ('queue',0)])
def test_loop_result_handoff_overrides_stored_panel_and_preserves_return(stored_panel, result_count):
    """An explicit result link wins on arrival; later navigation wins on reload."""
    playwright = pytest.importorskip('playwright.sync_api')
    source = (ROOT / 'frontend/v7_backtest.html').read_text()
    replacements = {'API_BASE':'/api/backtest-v8', 'BACKTEST_VERSION':'v8',
                    'BACKTEST_LABEL':'PB8', 'BASE_PREFIX':'', 'VERSION':'test',
                    'SERIAL':'1', 'NAV_HASH':'test'}
    for key, value in replacements.items():
        source = source.replace('%%'+key+'%%', value)
    errors, filters, equity_paths = [], [], []
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
                route.fulfill(json={'results':[{'path':'loop_fixture/result_'+str(index), 'config_name':'loop_fixture',
                    'exchange':'binance','name':'loop_fixture', 'start_date':'2020-01-01',
                    'end_date':'2020-02-01','analysis':{}} for index in range(result_count)] + [
                    {'path':'other_job/result','config_name':'other_job','exchange':'binance','analysis':{}}]})
            elif parsed.path == '/api/backtest-v8/results/equity':
                equity_paths.append(parse_qs(parsed.query)['path'][0])
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
            page.wait_for_function("(count)=>document.querySelector('#compare-chart-div')?.data?.length===count*2",arg=result_count)
            assert page.evaluate('getSelectedResults().length')==result_count
            assert set(equity_paths)=={'loop_fixture/result_'+str(index) for index in range(result_count)}
            assert page.locator('#results-config-filter').input_value()=='loop_fixture'
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
            page.wait_for_function("(count)=>document.querySelector('#compare-chart-div')?.data?.length===count*2",arg=result_count)
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
