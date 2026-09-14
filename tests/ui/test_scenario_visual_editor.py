"""Real browser checks for explicit scenario window interactions."""
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize('width', [920, 1500])
def test_drag_resize_split_and_apply(width):
    """Dragging at different viewport widths, undo and explicit holdout splitting work."""
    playwright = pytest.importorskip('playwright.sync_api')
    with playwright.sync_playwright() as runner:
        browser = runner.chromium.launch(headless=True)
        try:
            page = browser.new_page(viewport={'width': width, 'height': 1100})
            page.set_content('<div id="editor"></div>')
            page.add_style_tag(path=str(ROOT / 'frontend/css/scenario_visual_editor.css'))
            page.add_script_tag(path=str(ROOT / 'frontend/js/scenario_visual_editor.js'))
            page.evaluate("""() => {
              window.changed=[];window.applied=null;
              PBGuiScenarioVisual.mount(document.getElementById('editor'),{
                context:{start_date:'2024-01-01',end_date:'2024-04-09',exchanges:[]},
                windows:[{id:'a',label:'train',role:'training',start_date:'2024-01-01',end_date:'2024-01-20',scenario:{coins:['ETH']}}],
                change:w=>changed=w,apply:async w=>{applied=w;}
              });
            }""")
            svg = page.locator('.scenario-canvas svg').bounding_box()
            bar = page.locator('[data-id="a"][data-part="move"]').bounding_box()
            center = bar['x'] + bar['width']/2
            page.mouse.move(center, bar['y']+13)
            page.mouse.down()
            page.mouse.move(center + (svg['width']-120)*.10, bar['y']+13, steps=5)
            page.mouse.up()
            assert page.evaluate('changed[0].start_date') == '2024-01-11'
            assert page.evaluate('changed[0].end_date') == '2024-01-30'
            page.get_by_role('button', name='Undo', exact=True).click()
            assert page.evaluate('changed[0].start_date') == '2024-01-01'
            edge = page.locator('[data-id="a"][data-part="right"]').bounding_box()
            page.mouse.move(edge['x']+3, edge['y']+13)
            page.mouse.down()
            page.mouse.move(edge['x']+3+(svg['width']-120)*.05, edge['y']+13, steps=5)
            page.mouse.up()
            assert page.evaluate('changed[0].end_date') == '2024-01-25'
            _draw_dates(page, 'holdout', 9, 15, 100)
            assert page.get_by_role('button', name='Exclude holdouts from training').count()==0
            assert page.get_by_role('button', name='Check & Apply windows').is_enabled(), page.evaluate('({windows:changed, text:document.body.innerText})')
            page.get_by_role('button', name='Check & Apply windows').click()
            windows = page.evaluate('applied')
            assert [(w['start_date'], w['end_date']) for w in windows if w['role']=='training'] == [('2024-01-01','2024-01-09'),('2024-01-16','2024-01-25')]
            assert all(w['scenario']['coins']==['ETH'] for w in windows if w['role']=='training')
        finally:
            browser.close()


def test_suite_visual_round_trip_preserves_scoring_and_holdouts():
    """Applying graphical windows saves metadata without invoking preset scoring reset."""
    playwright = pytest.importorskip('playwright.sync_api')
    with playwright.sync_playwright() as runner:
        browser = runner.chromium.launch(headless=True)
        try:
            page = browser.new_page()
            page.set_content('<div id="suite"></div>')
            page.add_script_tag(path=str(ROOT / 'frontend/js/suite_editor.js'))
            result = page.evaluate("""async () => {
              window.esc=s=>String(s);window.toast=()=>{};
              window._suiteRender=()=>{};window._suiteNotifyStructuredSync=()=>{};
              window._suiteScenarioContext=()=>({start_date:'2024-01-01',end_date:'2024-12-31',exchanges:[]});
              _suiteState.editIdx=-1;_suiteState.onApplyScenarioPreview=()=>{throw Error('Scoring changed');};
              const holdout={label:'h',start_date:'2024-04-01',end_date:'2024-04-30'};
              _suiteState.scenarioPreview={contract_version:2,training_scenarios:[{label:'t',start_date:'2024-01-01',end_date:'2024-03-31'}],reducer:{default:'median'},provenance:{contract_version:2,template:'custom_windows',holdout_scenarios:[holdout]}};
              _suiteState.scenarioPreviewContext=_suiteScenarioContextSignature(_suiteScenarioContext());
              await _suiteApplyScenarioPreview();
              return {scenarios:_suiteState.scenarios,aggregate:_suiteState.aggregate,holdouts:_suiteState.scenarioTemplate.holdout_scenarios};
            }""")
            assert result['aggregate'] == {'default': 'median'}
            assert len(result['scenarios']) == len(result['holdouts']) == 1
        finally:
            browser.close()


def test_full_suite_editor_apply_save_reload():
    """The real shared editor round-trips visual metadata and retains it on reducer edits."""
    from scenario_templates import generate_scenario_template
    playwright = pytest.importorskip('playwright.sync_api')
    with playwright.sync_playwright() as runner:
        browser = runner.chromium.launch(headless=True)
        try:
            page = browser.new_page(viewport={'width': 1400, 'height': 1100})
            def request(route):
                """Serve only local test assets and validate previews against the backend function."""
                if route.request.url.endswith('/preview'):
                    route.fulfill(json=generate_scenario_template(route.request.post_data_json))
                else:
                    route.fulfill(body='<div id="suite"></div>', content_type='text/html')
            page.route('**/*', request)
            page.goto('http://scenario.test/')
            page.add_script_tag(path=str(ROOT / 'frontend/js/scenario_visual_editor.js'))
            page.add_script_tag(path=str(ROOT / 'frontend/js/suite_editor.js'))
            page.evaluate("""() => {
              window.esc=s=>String(s).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('"','&quot;');
              window.toast=()=>{};window.scheduleStructuredEditorSync=()=>{};
              window.apiFetch=(path,opts)=>path==='/bot-params'?Promise.resolve({params:[]}):fetch('/api'+path,opts).then(r=>r.json());
              suiteInit('suite',{version:'v8',scenarioGenerator:true,getScenarioContext:()=>({start_date:'2024-01-01',end_date:'2024-12-31',exchanges:[]})});
              suiteLoad({backtest:{suite_enabled:true,scenarios:[{label:'train',start_date:'2024-01-01',end_date:'2024-06-30'}],aggregate:{default:'median'}}});
            }""")
            _draw_dates(page, 'holdout', 182, 244, 366)
            page.get_by_role('button', name='Check & Apply windows').click()
            page.wait_for_function("_suiteState.scenarioTemplate?.contract_version === 2")
            assert page.get_by_role('button', name='Apply Training Scenarios', exact=True).count() == 0
            assert page.get_by_text('Preview:', exact=False).count() == 0
            assert page.get_by_role('button', name='Generate windows', exact=True).count() == 1
            saved = page.evaluate("""() => {
              const s=suiteCollect();
              const cfg={backtest:{suite_enabled:s.suite_enabled,scenarios:s.scenarios,aggregate:s.aggregate},pbgui:{scenario_template:s.scenario_template,scenario_generator:s.scenario_generator}};
              suiteLoad(cfg);
              _suiteState.aggregate.default='max';_suiteNotifyStructuredSync();
              return suiteCollect();
            }""")
            assert saved['aggregate'] == {'default': 'max'}
            assert saved['scenario_template']['holdout_scenarios'][0]['start_date'] == '2024-07-01'
            assert len(saved['scenarios']) == 1
            assert page.locator('.scenario-canvas [data-part=move]').count() == 2
            assert page.get_by_label('Selected window', exact=True).count() == 0
        finally:
            browser.close()


@pytest.mark.parametrize('days,stride', [(200,200),(400,400)])
def test_new_windows_use_settings_and_native_geometry(days, stride):
    """Window settings remain authoritative even at wide viewports and after zoom."""
    from datetime import date, timedelta
    playwright = pytest.importorskip('playwright.sync_api')
    with playwright.sync_playwright() as runner:
        browser = runner.chromium.launch(headless=True)
        try:
            page = browser.new_page(viewport={'width':1800,'height':1100})
            page.set_content('<div id="editor"></div>')
            native = (ROOT / 'frontend/v7_optimize.html').read_text().split('<style>',1)[1].split('</style>',1)[0]
            page.add_style_tag(content=native)
            page.add_style_tag(path=str(ROOT / 'frontend/css/scenario_visual_editor.css'))
            page.add_script_tag(path=str(ROOT / 'frontend/js/scenario_visual_editor.js'))
            start = date(2020,1,1)
            page.evaluate("""args=>{
              window.changed=[];
              PBGuiScenarioVisual.mount(document.getElementById('editor'),{
                context:{start_date:'2020-01-01',end_date:'2026-01-01',exchanges:[]},
                settings:()=>({window_days:args.days,stride_days:args.stride}),
                windows:[{id:'a',label:'existing',role:'training',start_date:'2020-01-01',end_date:args.end,scenario:{}}],
                change:w=>changed=w,apply:async()=>{}
              });
            }""", {'days':days,'stride':stride,'end':str(start+timedelta(days=days-1))})
            _draw_dates(page, 'training', stride, stride+days, (date(2026,1,1)-start).days+1)
            added=page.evaluate('changed[1]')
            assert added['start_date']==str(start+timedelta(days=stride))
            assert added['end_date']==str(start+timedelta(days=stride+days-1))
            assert page.evaluate("Math.abs(document.querySelector('.scenario-canvas svg').viewBox.baseVal.width-document.querySelector('.scenario-canvas svg').clientWidth)<2")
            assert page.get_by_label('Window start', exact=True).count() == 0
            if days==200:
                page.screenshot(path='/tmp/pbgui-scenario-editor-corrected.png',full_page=True)
        finally:
            browser.close()


def test_price_reference_prefers_optimizer_coin_and_keeps_gap_strip_small():
    """An alphabetical 0G source cannot replace ETH; missing history does not cover the chart."""
    playwright=pytest.importorskip('playwright.sync_api')
    with playwright.sync_playwright() as runner:
        browser=runner.chromium.launch(headless=True)
        try:
            page=browser.new_page(viewport={'width':1600,'height':1100})
            requested=[]
            def route_request(route):
                """Provide local reference metadata and a small deterministic chart."""
                from urllib.parse import urlparse, parse_qs
                url=urlparse(route.request.url)
                if url.path.endswith('/sources'):
                    route.fulfill(json={'sources':[{'coin':'0G_USDT:USDT','base_coin':'0G','dataset':'1m'}, {'coin':'ETH_USDT:USDT','base_coin':'ETH','dataset':'1m'}]})
                elif url.path.endswith('/chart'):
                    requested.append(parse_qs(url.query)['coin'][0])
                    route.fulfill(json={'candles':[{'date':'2024-01-02','open':10,'high':12,'low':8,'close':11},{'date':'2024-01-03','open':11,'high':14,'low':9,'close':13}], 'missing_days':['2024-01-01'],'incomplete_days':[]})
                else:
                    route.fulfill(body='<div id="editor"></div>',content_type='text/html')
            page.route('**/*',route_request)
            page.goto('http://scenario.test/')
            native=(ROOT/'frontend/v7_optimize.html').read_text().split('<style>',1)[1].split('</style>',1)[0]
            page.add_style_tag(content=native)
            page.add_style_tag(path=str(ROOT/'frontend/css/scenario_visual_editor.css'))
            page.add_script_tag(path=str(ROOT/'frontend/js/scenario_visual_editor.js'))
            page.evaluate("""()=>{
              window.options={apiBase:'/api',context:{start_date:'2024-01-01',end_date:'2024-01-10',exchanges:['binance'],coins:['ETH']},windows:[],change:()=>{},apply:async()=>{}};
              PBGuiScenarioVisual.mount(document.getElementById('editor'),options);
            }""")
            page.wait_for_function("document.querySelector('.scenario-feedback').textContent.includes(' days · ')")
            assert requested == ['ETH_USDT:USDT']
            gaps=page.locator('.scenario-canvas svg rect[fill="rgba(240,160,50,.5)"]')
            assert gaps.count()==1
            assert gaps.first.get_attribute('height')=='8'
            assert page.get_by_label('Price chart style').count()==0
            assert page.locator('.scenario-canvas svg rect[fill="#49c99a"]').count()==2
            assert '0G' not in page.get_by_label('Reference coin and dataset').text_content()
            page.get_by_role('button',name='Full range',exact=True).click()
            assert page.locator('.scenario-canvas svg').text_content().count('2024-01-01')>=1
            page.get_by_label('Reference coin and dataset').select_option('0')
            page.wait_for_function("document.querySelector('.scenario-feedback').textContent.includes(' days · ')")
            page.evaluate("PBGuiScenarioVisual.mount(document.getElementById('editor'),options)")
            page.wait_for_function("document.querySelector('.scenario-feedback').textContent.includes(' days · ')")
            assert requested[-1]=='ETH_USDT:USDT'
        finally:
            browser.close()


def test_trash_click_drop_undo_and_connections():
    """Trash accepts captured pointer drops and connection markers respect inclusive dates."""
    playwright = pytest.importorskip('playwright.sync_api')
    with playwright.sync_playwright() as runner:
        browser = runner.chromium.launch(headless=True)
        try:
            page = browser.new_page(viewport={'width': 1500, 'height': 1100})
            page.set_content('<div id="editor"></div>')
            page.add_style_tag(path=str(ROOT / 'frontend/css/scenario_visual_editor.css'))
            page.add_script_tag(path=str(ROOT / 'frontend/js/scenario_visual_editor.js'))
            page.evaluate("""() => {
              window.changed=[];
              PBGuiScenarioVisual.mount(document.getElementById('editor'),{
                context:{start_date:'2024-01-01',end_date:'2024-04-09',exchanges:[]},
                windows:[['a','01','10'],['b','11','20'],['c','23','30'],['d','25','28']].map(([id,a,b])=>({
                  id,label:id,role:'training',start_date:'2024-01-'+a,end_date:'2024-01-'+b,scenario:{}})),
                change:w=>changed=w,apply:async()=>{}
              });
            }""")
            assert page.locator('[data-connection="adjacent"]').count() == 1
            assert '2 days' in page.locator('[data-connection="gap"] title').text_content()
            assert '4 days' in page.locator('[data-connection="overlap"] title').text_content()
            trash = page.get_by_role('button', name='Delete selected window', exact=True)
            assert page.get_by_role('button', name='Delete', exact=True).count() == 0
            trash.click()
            assert page.locator('[data-id="a"]').count() == 0
            page.get_by_role('button', name='Undo', exact=True).click()
            original = page.evaluate('changed')
            bar = page.locator('[data-id="d"][data-part="move"]').bounding_box()
            target = trash.bounding_box()
            page.mouse.move(bar['x']+bar['width']/2, bar['y']+13)
            page.mouse.down()
            page.mouse.move(target['x']+target['width']/2,target['y']+target['height']/2,steps=10)
            assert 'is-drop-target' in trash.get_attribute('class')
            page.mouse.up()
            assert page.locator('[data-id="d"]').count() == 0
            assert [w for w in original if w['id'] != 'd'] == page.evaluate('changed')
            page.get_by_role('button', name='Undo', exact=True).click()
            assert page.evaluate('changed') == original
        finally:
            browser.close()


def test_default_drawing_and_mouse_zoom():
    """Empty-space dragging creates windows without a mode button; zoom never edits dates."""
    playwright=pytest.importorskip('playwright.sync_api')
    with playwright.sync_playwright() as runner:
        browser=runner.chromium.launch(headless=True)
        try:
            page=browser.new_page(viewport={'width':1500,'height':1100})
            page.set_content('<div id="editor"></div>')
            page.add_style_tag(path=str(ROOT/'frontend/css/scenario_visual_editor.css'))
            page.add_script_tag(path=str(ROOT/'frontend/js/scenario_visual_editor.js'))
            page.evaluate("""()=>{window.changed=[];PBGuiScenarioVisual.mount(document.getElementById('editor'),{
              context:{start_date:'2024-01-01',end_date:'2024-04-09',exchanges:[]},windows:[],change:w=>changed=w,apply:async()=>{}
            });}""")
            assert page.get_by_role('button',name='Draw window',exact=True).count()==0
            box=page.locator('.scenario-canvas svg').bounding_box()
            page.mouse.move(box['x']+200,box['y']+180)
            page.mouse.down()
            page.mouse.move(box['x']+400,box['y']+180,steps=5)
            page.mouse.up()
            original=page.evaluate('changed')
            assert len(original)==1 and original[0]['role']=='training'
            before=page.locator('[data-part="move"]').bounding_box()['width']
            page.mouse.wheel(0,-400)
            page.wait_for_timeout(100)
            assert page.locator('[data-part="move"]').bounding_box()['width']>before
            assert page.evaluate('changed')==original
            page.mouse.dblclick(box['x']+500,box['y']+180)
            assert page.evaluate('changed')==original
        finally:
            browser.close()


def _draw_window(page, role):
    """Draw a window on empty price or holdout space using the public mouse interaction."""
    box=page.locator('.scenario-canvas svg').bounding_box()
    y=box['y']+(405 if role=='holdout' else 180)
    left=box['x']+100+(box['width']-120)*(.8 if role=='holdout' else .02)
    page.mouse.move(left,y)
    page.mouse.down()
    page.mouse.move(left+40,y,steps=4)
    page.mouse.up()


def test_drag_between_roles_and_four_icons():
    """Vertical drops change roles without changing dates and remain undoable."""
    playwright=pytest.importorskip('playwright.sync_api')
    with playwright.sync_playwright() as runner:
        browser=runner.chromium.launch(headless=True)
        try:
            page=browser.new_page(viewport={'width':1500,'height':1100})
            page.set_content('<div id="editor"></div>')
            page.add_style_tag(path=str(ROOT/'frontend/css/scenario_visual_editor.css'))
            page.add_script_tag(path=str(ROOT/'frontend/js/scenario_visual_editor.js'))
            page.evaluate("""()=>{window.changed=[];PBGuiScenarioVisual.mount(document.getElementById('editor'),{
              context:{start_date:'2024-01-01',end_date:'2024-04-09',exchanges:[]},
              windows:[{id:'a',label:'train',role:'training',start_date:'2024-01-10',end_date:'2024-01-20',scenario:{}},
                {id:'b',label:'overlapping',role:'training',start_date:'2024-01-01',end_date:'2024-01-25',scenario:{}}],
              change:w=>changed=w,apply:async()=>{}
            });}""")
            icons=page.locator('.scenario-icon')
            assert icons.count()==5
            assert icons.all_text_contents()==['','','','','']
            for target,role in [(405,'holdout'),(355,'training')]:
                box=page.locator('.scenario-canvas svg').bounding_box()
                bar=page.locator('[data-id="a"][data-part="move"]').bounding_box()
                x=bar['x']+bar['width']/2
                page.mouse.move(x,bar['y']+13)
                page.mouse.down()
                lane=page.locator('[data-lane="'+role+'"]').bounding_box()
                page.mouse.move(x,lane['y']+lane['height']/2,steps=6)
                assert page.locator('.scenario-drag-ghost').get_attribute('data-role')==role
                assert page.locator('[data-id="a"][data-part="move"]').count()==0
                page.mouse.up()
                result=page.evaluate('changed[0]')
                assert result['role']==role
                assert result['start_date']=='2024-01-10'
                assert result['end_date']=='2024-01-20'
            page.get_by_role('button',name='Undo',exact=True).click()
            assert page.evaluate('changed[0].role')=='holdout'
        finally:
            browser.close()


def test_magnet_can_snap_and_be_disabled():
    """Snapping closes a small gap while disabling the magnet preserves the pointer dates."""
    playwright=pytest.importorskip('playwright.sync_api')
    with playwright.sync_playwright() as runner:
        browser=runner.chromium.launch(headless=True)
        try:
            page=browser.new_page(viewport={'width':1500,'height':1100})
            page.set_content('<div id="editor"></div>')
            page.add_style_tag(path=str(ROOT/'frontend/css/scenario_visual_editor.css'))
            page.add_script_tag(path=str(ROOT/'frontend/js/scenario_visual_editor.js'))
            page.evaluate("""()=>{window.changed=[];PBGuiScenarioVisual.mount(document.getElementById('editor'),{
              context:{start_date:'2024-01-01',end_date:'2024-12-31',exchanges:[]},
              windows:[{id:'a',label:'a',role:'training',start_date:'2024-01-01',end_date:'2024-01-20'},
              {id:'b',label:'b',role:'training',start_date:'2024-02-01',end_date:'2024-02-20'}],
              change:w=>changed=w,apply:async()=>{}
            });}""")
            for enabled in [True,False]:
                if not enabled:
                    page.get_by_role('button',name='Undo',exact=True).click()
                    page.get_by_role('button',name='Snap windows',exact=True).click()
                box=page.locator('.scenario-canvas svg').bounding_box()
                bar=page.locator('[data-id="b"][data-part="move"]').bounding_box()
                x=bar['x']+bar['width']/2
                page.mouse.move(x,bar['y']+13)
                page.mouse.down()
                page.mouse.move(x-10*(box['width']-120)/366,bar['y']+13,steps=5)
                page.mouse.up()
                result=page.evaluate('changed.find(w=>w.id==="b")')
                assert result['start_date']==('2024-01-21' if enabled else '2024-01-22')
        finally:
            browser.close()


def test_holdout_drawing_splits_training_as_one_undoable_edit():
    """An interior holdout removes training dates immediately and Undo restores the full window."""
    playwright=pytest.importorskip('playwright.sync_api')
    with playwright.sync_playwright() as runner:
        browser=runner.chromium.launch(headless=True)
        try:
            page=browser.new_page(viewport={'width':1500,'height':1100})
            page.set_content('<div id="editor"></div>')
            page.add_style_tag(path=str(ROOT/'frontend/css/scenario_visual_editor.css'))
            page.add_script_tag(path=str(ROOT/'frontend/js/scenario_visual_editor.js'))
            page.evaluate("""()=>{window.original=[{id:'a',label:'train',role:'training',start_date:'2024-01-01',end_date:'2024-01-31',scenario:{coins:['ETH']}}];window.changed=[];
              PBGuiScenarioVisual.mount(document.getElementById('editor'),{
                context:{start_date:'2024-01-01',end_date:'2024-04-09',exchanges:[]},windows:original,change:w=>changed=w,apply:async()=>{}
              });}""")
            box=page.locator('.scenario-canvas svg').bounding_box()
            lane=page.locator('[data-lane="holdout"]').bounding_box()
            y=lane['y']+lane['height']/2
            page.mouse.move(box['x']+100+(box['width']-120)*.09,y)
            page.mouse.down()
            page.mouse.move(box['x']+100+(box['width']-120)*.14,y,steps=5)
            page.mouse.up()
            windows=page.evaluate('changed')
            training=[w for w in windows if w['role']=='training']
            assert [(w['start_date'],w['end_date']) for w in training]==[('2024-01-01','2024-01-09'),('2024-01-16','2024-01-31')]
            assert all(w['scenario']=={'coins':['ETH']} for w in training)
            page.get_by_role('button',name='Undo',exact=True).click()
            assert page.evaluate('changed')==page.evaluate('original')
            page.get_by_role('button',name='Redo',exact=True).click()
            assert page.evaluate('changed')==windows
        finally:
            browser.close()


@pytest.mark.parametrize('right_role',['training','holdout'])
def test_shared_boundary_preserves_adjacency_and_outer_dates(right_role):
    """Moving the shared grip resizes both windows and undoes as one edit, across roles too."""
    playwright=pytest.importorskip('playwright.sync_api')
    with playwright.sync_playwright() as runner:
        browser=runner.chromium.launch(headless=True)
        try:
            page=browser.new_page(viewport={'width':1500,'height':1100})
            page.set_content('<div id="editor"></div>')
            page.add_style_tag(path=str(ROOT/'frontend/css/scenario_visual_editor.css'))
            page.add_script_tag(path=str(ROOT/'frontend/js/scenario_visual_editor.js'))
            page.evaluate("""role=>{window.original=[{id:'a',label:'a',role:'training',start_date:'2024-01-01',end_date:'2024-01-20'},
              {id:'b',label:'b',role,start_date:'2024-01-21',end_date:'2024-02-09'}];window.changed=[];
              PBGuiScenarioVisual.mount(document.getElementById('editor'),{
                context:{start_date:'2024-01-01',end_date:'2024-04-09',exchanges:[]},windows:original,change:w=>changed=w,apply:async()=>{}
              });}""",right_role)
            box=page.locator('.scenario-canvas svg').bounding_box()
            handle=page.locator('[data-part="joint"]').bounding_box()
            page.mouse.move(handle['x']+5,handle['y']+13)
            page.mouse.down()
            page.mouse.move(handle['x']+5+(box['width']-120)*.05,handle['y']+13,steps=5)
            page.mouse.up()
            windows=page.evaluate('changed')
            assert windows[0]['start_date']=='2024-01-01'
            assert windows[0]['end_date']=='2024-01-25'
            assert windows[1]['start_date']=='2024-01-26'
            assert windows[1]['end_date']=='2024-02-09'
            page.get_by_role('button',name='Undo',exact=True).click()
            assert page.evaluate('changed')==page.evaluate('original')
            page.get_by_role('button',name='Redo',exact=True).click()
            assert page.evaluate('changed')==windows
        finally:
            browser.close()


@pytest.mark.parametrize('side',['left','right'])
def test_adjacent_windows_can_be_separated_with_side_grips(side):
    """Independent grips open a gap without moving the neighboring window."""
    playwright=pytest.importorskip('playwright.sync_api')
    with playwright.sync_playwright() as runner:
        browser=runner.chromium.launch(headless=True)
        try:
            page=browser.new_page(viewport={'width':1500,'height':1100})
            page.set_content('<div id="editor"></div>')
            page.add_style_tag(path=str(ROOT/'frontend/css/scenario_visual_editor.css'))
            page.add_script_tag(path=str(ROOT/'frontend/js/scenario_visual_editor.js'))
            page.evaluate("""()=>{window.original=[{id:'a',label:'a',role:'training',start_date:'2024-01-01',end_date:'2024-01-20'},
              {id:'b',label:'b',role:'training',start_date:'2024-01-21',end_date:'2024-02-09'}];window.changed=[];
              PBGuiScenarioVisual.mount(document.getElementById('editor'),{context:{start_date:'2024-01-01',end_date:'2024-04-09',exchanges:[]},windows:original,change:w=>changed=w,apply:async()=>{}});
            }""")
            box=page.locator('.scenario-canvas svg').bounding_box()
            label='Resize left window end only' if side=='left' else 'Resize right window start only'
            grip=page.get_by_role('button',name=label,exact=True).bounding_box()
            x=grip['x']+4;y=grip['y']+13
            page.mouse.move(x,y);page.mouse.down()
            page.mouse.move(x+(box['width']-120)*(-.05 if side=='left' else .05),y,steps=5)
            page.mouse.up()
            actual=page.evaluate('changed');original=page.evaluate('original')
            if side=='left':
                assert actual[0]['end_date']=='2024-01-15'
                assert actual[1]==original[1]
            else:
                assert actual[1]['start_date']=='2024-01-26'
                assert actual[0]==original[0]
            page.get_by_role('button',name='Undo',exact=True).click()
            assert page.evaluate('changed')==original
        finally:
            browser.close()


def _draw_dates(page, role, start, end, total):
    """Draw a dated window through the chart rather than redundant form inputs."""
    svg = page.locator('.scenario-canvas svg').bounding_box()
    y = svg['y'] + (405 if role == 'holdout' else 180)
    page.mouse.move(svg['x']+100+(svg['width']-120)*start/total, y)
    page.mouse.down()
    page.mouse.move(svg['x']+100+(svg['width']-120)*(end-1)/total, y, steps=5)
    page.mouse.up()
