"""Executable contracts for the shared PB8 8.6 controls and HSL observations."""

from pathlib import Path
import json
import subprocess
import textwrap
from urllib.parse import urlparse

import pytest
from playwright.sync_api import sync_playwright


ROOT = Path(__file__).resolve().parents[2]


def _node(script):
    """Run browser-independent JavaScript against the actual shared modules."""
    result = subprocess.run(["node", "-e", textwrap.dedent(script)], cwd=ROOT, capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr or result.stdout


def test_current_optimizer_metadata_does_not_reintroduce_retired_hsl_parameters():
    """Current defaults keep explicit two-policy HSL and adaptive optimizer bounds."""
    _node("""
      const assert = require('node:assert/strict'); const fs = require('node:fs'); global.window = {};
      eval(fs.readFileSync('frontend/js/optimize_editor_adapter.js', 'utf8'));
      const adapter = window.PBGuiOptimizeEditorAdapter.create('v8', {});
      const metadata = adapter.normalizeMetadata({template: {config_version:'v8.6.0', bot: {
        long: {hsl:{enabled:true}}, short:{hsl:{enabled:false}}
      }, optimize:{fixed_runtime_overrides:{'bot.long.hsl.restart_after_red_policy':'never'}}},
        active_bounds:{ema_anchor:{long:{entry_cooldown:{weights_minutes:{exposure_ratio:[0,30,1]}},
          forager:{score_weights:{unilateralness:[0,1,.1]}}}}}});
      assert(!metadata.runtimeOverrides.some(f => f.key.includes('no_restart_drawdown_threshold')));
      assert.deepEqual(metadata.runtimeOverrides.find(f=>f.key.endsWith('restart_after_red_policy')).choices,['always','never']);
      assert.deepEqual(metadata.strategyBounds.ema_anchor['long.entry_cooldown.weights_minutes.exposure_ratio'],[0,30,1]);
      assert.equal(adapter.canonicalFixedParam('hsl.red_threshold'),'bot.hsl.red_threshold');
      assert.equal(adapter.activeHslPath('bot.long.hsl.enabled','unified'),false);
      assert.equal(adapter.activeHslPath('bot.hsl.enabled','unified'),true);
      assert.equal(adapter.activeHslPath('bot.hsl.enabled','coin'),false);
    """)



def test_native_report_preserves_per_coin_signals_and_transition_sequence():
    """The chart uses native values, including equal-timestamp fill transitions."""
    _node("""
      const assert=require('node:assert/strict'); const fs=require('node:fs');global.window={};
      eval(fs.readFileSync('frontend/js/pb8_hsl_report.js','utf8'));
      const report={schema_version:1,engine:'hsl',mode:'coin',detailed:true,coins:['HYPE'],
        scopes:[{side:0,coin:0,policy:{red_threshold:.2}}],
        samples:[{side:0,coin:0,timestamp:1000,sequence:1,raw:.4,ema:.21,action:'panic'},
          {side:0,coin:0,timestamp:1000,sequence:3,raw:.1,ema:.1,action:'normal'}],
        events:[{side:0,coin:0,observed_at:1000,sequence:2,kind:'flat'}]};
      const data=window.PB8HslReport.traces(report);
      assert.deepEqual(data[0].y,[.4,.1]);assert.deepEqual(data[1].y,[.21,.1]);
      assert.deepEqual(data[3].y,[1,1,0]);assert(!data.some(t=>/yellow|orange/i.test(t.name)));
      assert.deepEqual(window.PB8HslReport.traces({...report,detailed:false}),[]);
      assert.throws(()=>window.PB8HslReport.traces({...report,engine:'legacy'}),/schema/);
    """)


def test_cooldown_override_parser_accepts_null_and_finite_numbers():
    """Sparse nullable ceilings stay typed rather than becoming string values."""
    _node("""
      const assert=require('node:assert/strict');const fs=require('node:fs');
      eval(fs.readFileSync('frontend/js/coin_overrides_editor.js','utf8'));
      const metadata={type:'number_or_null',default:null};
      assert.equal(_covParseParamValue('null',metadata,'entry_cooldown.max_duration_minutes'),null);
      assert.equal(_covParseParamValue('0',metadata,'entry_cooldown.max_duration_minutes'),0);
      assert.equal(_covParseParamValue('25.5',metadata,'entry_cooldown.max_duration_minutes'),25.5);
      assert.throws(()=>_covParseParamValue('Infinity',metadata,'entry_cooldown.max_duration_minutes'),/number/);
    """)


@pytest.mark.parametrize("page_name", ["edit", "backtest", "optimize"])
def test_existing_json_highlighter_marks_only_changed_hsl_lines(page_name):
    """Migration marks cannot spill into cooldown, forager or unchanged HSL leaves."""
    source = (ROOT / f'frontend/v7_{page_name}.html').read_text()
    start = source.index('function _applyBotJsonHighlight(')
    function = source[start:source.index('\n}', start) + 2]
    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        page = browser.new_page()
        page.set_content('<div><textarea id="side" rows="12"></textarea></div>')
        page.add_script_tag(path=str(ROOT / 'frontend/js/editor_shared.js'))
        page.add_script_tag(content=function)
        marked = page.evaluate("""() => {
          const ta = document.querySelector('textarea');
          ta.value = JSON.stringify({entry_cooldown:{base_duration_minutes:10.4},
            forager:{score_weights:{volume:.39}},hsl:{enabled:true,red_threshold:.141,
              restart_after_red_policy:null,panic_close_order_type:'limit'},
            risk:{n_positions:1}}, null, 2);
          _applyBotJsonHighlight(ta, {
            'hsl.restart_after_red_policy':'review_line',
            'hsl.no_restart_drawdown_threshold':{status:'removed',before:1},
            'hsl.orange_tier_mode':{status:'removed',before:'<img src=x onerror=alert(1)>'},
            'hsl.tier_ratios.orange':{status:'removed',before:.75},
            'hsl.tier_ratios.yellow':{status:'removed',before:.5}
          });
          return Array.from(ta._botHighlightPre.children).filter(el=>el.style.background)
            .map(el=>el.textContent.trim());
        }""")
        assert marked == ['"restart_after_red_policy": null,']
        assert page.locator('.hsl-migration-removals pre').inner_text().splitlines() == [
            '- "hsl.no_restart_drawdown_threshold": 1',
            '- "hsl.orange_tier_mode": "<img src=x onerror=alert(1)>"',
            '- "hsl.tier_ratios.orange": 0.75',
            '- "hsl.tier_ratios.yellow": 0.5',
        ]
        assert page.locator('.hsl-migration-removals img').count() == 0
        assert page.locator('#side').evaluate('el=>JSON.parse(el.value).hsl.no_restart_drawdown_threshold === undefined')
        page.evaluate("""() => {
          const ta=document.querySelector('textarea');
          ta.focus();ta.setSelectionRange(4,4);
          _applyBotJsonHighlight(ta, {'hsl.red_threshold':'review_line'});
        }""")
        assert page.locator('#side').evaluate('el=>el.selectionStart') == 4
        assert page.locator('#side').evaluate('el=>document.activeElement===el')
        assert page.locator('#side').evaluate("el=>Array.from(el._botHighlightPre.children).filter(x=>x.style.background).map(x=>x.textContent.trim())") == ['"red_threshold": 0.141,']
        assert page.locator('.hsl-migration-removals').count() == 0
        browser.close()


@pytest.mark.parametrize("page_name", ["edit", "backtest", "optimize"])
def test_raw_and_structured_migration_marks_use_exact_paths_and_preserve_edits(page_name):
    """All editors retain full changes without confusing sides or altering JSON."""
    source = (ROOT / f'frontend/v7_{page_name}.html').read_text()
    start = source.index('function syncRawJsonHighlightOverlay(')
    sync_function = source[start:source.index('\n}', start) + 2]
    raw_id = 'opted-raw-json' if page_name == 'optimize' else 'cfg-raw-json'
    overlay_id = 'opted-raw-json-highlight' if page_name == 'optimize' else 'cfg-raw-json-highlight'
    root_id = 'main-content' if page_name == 'edit' else 'configs-editor'
    status_name = {'edit':'paramStatus','backtest':'_cfgBotParamStatus','optimize':'_optBotParamStatus'}[page_name]
    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        page = browser.new_page()
        page.set_content(f'''<main id="{root_id}">
          <div class="form-group"><label>pnls_max_lookback_days</label><input id="f-pnls-lookback" value="35"></div>
          <div class="form-group"><label>hsl_engine</label><input id="f-hsl-engine" value="legacy"></div>
          <div class="form-group"><label>Long TWE</label><input id="cfg-twe-long" value="2"></div>
          <div class="form-group"><label>Short TWE</label><input id="cfg-twe-short" value="2"></div>
          <div class="form-group"><label>offline</label><input id="extra-bt-offline" type="checkbox"></div>
          <div class="form-group"><label>drift_objective_tolerance</label><input id="opted-gpu-drift-objective-tolerance" value="0.000001"></div>
          <div class="optimize-bound-row"><span class="bound-key-text">long.entry_cooldown.base_duration_minutes</span><input type="range"></div>
          <div class="optimize-bound-row"><span class="bound-key-text">long.forager.score_weights.volume</span><label class="optimize-bound-fixed"><input id="fixed-volume" type="checkbox" checked></label></div>
          <div style="position:relative"><pre id="{overlay_id}"></pre><textarea id="{raw_id}" rows="8"></textarea></div>
        </main>''')
        page.add_script_tag(path=str(ROOT / 'frontend/js/editor_shared.js'))
        page.add_script_tag(path=str(ROOT / 'frontend/js/pb8_parameter_help.js'))
        setter = {'backtest':'setCfgBotParamStatus','optimize':'setOptBotParamStatus'}.get(page_name)
        setter_function = ''
        if setter:
            setter_start = source.index('function ' + setter + '(')
            setter_function = source[setter_start:source.index('\n}', setter_start) + 2]
        assign = f'{setter}(value)' if setter else f'{status_name}=value'
        page.add_script_tag(content=f'var {status_name} = {{}};\nfunction el(id){{return document.getElementById(id)}}\n'
                            + setter_function + f'\nfunction setMigrationTestStatus(value){{{assign};}}\n' + sync_function)
        page.evaluate('''({rawId, statusName}) => {
          const config={config_version:'v8.6.0',
            live:{pnls_max_lookback_days:35,note:'literal { braces } and "quoted" values'},
            backtest:{offline:false},
            bot:{long:{entry_cooldown:{base_duration_minutes:10.4},risk:{total_wallet_exposure_limit:2}},
              short:{entry_cooldown:{base_duration_minutes:10.4},risk:{total_wallet_exposure_limit:2}}},
            optimize:{gpu:{drift_objective_tolerance:.000001},
              fixed_params:['bot.long.forager.score_weights.volume','literal "quoted" selector'],
              bounds:{long:{entry_cooldown:{base_duration_minutes:[0,60,.1]}}}}};
          const paths=['config_version','live.pnls_max_lookback_days','backtest.offline',
            'bot.long.entry_cooldown.base_duration_minutes','bot.long.risk.total_wallet_exposure_limit',
            'optimize.gpu.drift_objective_tolerance','optimize.fixed_params',
            'optimize.bounds.long.entry_cooldown.base_duration_minutes'];
          const changes=Object.fromEntries(paths.map(path=>[path,{removed:false,added:true}]));
          changes['live.hsl_engine']={removed:true,before:'legacy',after:null};
          changes['optimize.fixed_params'].before=['bot.long.forager.score_weights_volume','literal "quoted" selector'];
          changes['optimize.fixed_params'].after=config.optimize.fixed_params;
          setMigrationTestStatus({config_changes:changes});
          const raw=document.getElementById(rawId);
          raw.value=JSON.stringify(config,null,2);window.originalRaw=raw.value;
          raw.focus();raw.setSelectionRange(7,13);raw.scrollTop=40;
          window.originalScroll=raw.scrollTop;
          syncRawJsonHighlightOverlay(null,null);
        }''', {'rawId':raw_id,'statusName':status_name})
        assert page.locator('#f-pnls-lookback').get_attribute('data-config-migration') == 'live.pnls_max_lookback_days'
        assert page.locator('#f-hsl-engine').get_attribute('data-config-migration') == 'live.hsl_engine'
        assert page.locator('#cfg-twe-long').get_attribute('data-config-migration') == 'bot.long.risk.total_wallet_exposure_limit'
        assert page.locator('#cfg-twe-short').get_attribute('data-config-migration') is None
        assert page.locator('#extra-bt-offline').get_attribute('data-config-migration') == 'backtest.offline'
        assert page.locator('#opted-gpu-drift-objective-tolerance').get_attribute('data-config-migration') == 'optimize.gpu.drift_objective_tolerance'
        assert page.locator('.optimize-bound-row').first.get_attribute('data-config-migration') == 'optimize.bounds.long.entry_cooldown.base_duration_minutes'
        assert page.locator('#fixed-volume').get_attribute('data-config-migration') == 'optimize.fixed_params'
        marked = page.locator(f'#{overlay_id} span').evaluate_all('nodes=>nodes.filter(x=>x.style.background).map(x=>x.textContent.trim())')
        assert sum('"base_duration_minutes": 10.4' in line for line in marked) == 1
        assert sum('"total_wallet_exposure_limit": 2' in line for line in marked) == 1
        assert '"literal \\"quoted\\" selector"' in marked
        assert '60,' in marked
        assert not any('note' in line for line in marked)
        assert page.locator(f'#{raw_id}').evaluate('el=>el.value===window.originalRaw && el.selectionStart===7 && el.selectionEnd===13 && el.scrollTop===window.originalScroll && document.activeElement===el')
        page.evaluate('syncRawJsonHighlightOverlay(new Error("test"),{line:2})')
        assert page.locator(f'#{overlay_id} span').nth(1).evaluate('el=>el.style.background') == 'rgba(255, 75, 75, 0.16)'
        page.evaluate('setMigrationTestStatus({});syncRawJsonHighlightOverlay(null,null)')
        assert page.locator('[data-config-migration]').count() == 0
        assert page.locator(f'#{overlay_id}').is_hidden()
        assert page.locator(f'#{raw_id}').evaluate('el=>el.value===window.originalRaw')
        browser.close()


@pytest.mark.parametrize("width", [700, 1600])
@pytest.mark.parametrize("migrated", [False, True])
def test_real_pb8_run_editor_loads_current_fields_without_retired_controls(width, migrated):
    """Exercise the real Run edit route, shared assets, raw sync and responsive layout."""
    source = (ROOT / 'frontend/v7_edit.html').read_text()
    for key, value in {'API_BASE':'/api/v8', 'WS_BASE':'ws://pbgui.test', 'VERSION':'test',
                       'SERIAL':'1', 'INSTANCE':'alice', 'IS_NEW':'false', 'DRAFT_ID':'',
                       'RUN_VERSION':'v8', 'MASTER_NAME':'test', 'NAV_HASH':'test'}.items():
        source = source.replace('%%' + key + '%%', value)
    side = {'risk':{'n_positions':1,'total_wallet_exposure_limit':1}, 'strategy':{'ema_anchor':{}},
            'hsl':{'enabled':True,'red_threshold':.15,'ema_span_minutes':720,'cooldown_minutes_after_red':2160,
                   'restart_after_red_policy':'never','panic_close_order_type':'limit'},
            'entry_cooldown':{'base_duration_minutes':.05,'min_duration_minutes':0,'max_duration_minutes':None,
                              'weights_minutes':{'exposure_ratio':0,'adverse_directionality':0}},
            'forager':{'score_weights':{'unilateralness':0},'unilateralness_ema_span_1m':60}}
    config = {'config_version':'v8.6.0','bot':{'long':side,'short':side},
              'live':{'user':'alice','strategy_kind':'ema_anchor','hsl_signal_mode':'pside','pnls_max_lookback_days':35},
              'pbgui':{'version':2,'enabled_on':'disabled'}}
    if migrated:
        side['hsl']['restart_after_red_policy'] = None
    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        page = browser.new_page(viewport={'width':width,'height':900})
        errors = []
        page.on('pageerror', lambda error: errors.append(str(error)))

        def route(request):
            path = urlparse(request.request.url).path
            if path == '/api/v8/edit_page':
                request.fulfill(body=source, content_type='text/html')
            elif path.startswith('/app/'):
                asset = ROOT / 'frontend' / path.removeprefix('/app/')
                request.fulfill(body=asset.read_bytes() if asset.is_file() else b'',
                                content_type='text/css' if asset.suffix == '.css' else 'text/javascript')
            elif path == '/api/v8/editor/metadata':
                request.fulfill(json={'strategies':['ema_anchor'],'params':{'live':{'hsl_signal_mode':{'type':'string'}}}})
            elif path == '/api/v8/instances/alice/config':
                request.fulfill(json={'config':config,'param_status':{'long': {
                    'hsl.restart_after_red_policy':'review_line',
                    'hsl.no_restart_drawdown_threshold':{'status':'removed','before':1},
                    'hsl.orange_tier_mode':{'status':'removed','before':'tp_only_with_active_entry_cancellation'},
                    'hsl.tier_ratios.orange':{'status':'removed','before':.75},
                    'hsl.tier_ratios.yellow':{'status':'removed','before':.5},
                }, 'config_changes': {
                    'live.pnls_max_lookback_days': {'before':30,'after':35,'removed':False,'added':False},
                    'bot.long.entry_cooldown.base_duration_minutes': {'before':None,'after':.05,'removed':False,'added':True},
                    'bot.long.risk.total_wallet_exposure_limit': {'before':None,'after':1,'removed':False,'added':True},
                }} if migrated else {},'override_configs':{},
                                      'migration_changes':[{'path':'bot.long.hsl.restart_after_red_policy','before':'threshold','after':None,'requires_choice':True}] if migrated else []})
            elif path == '/api/v8/users':
                request.fulfill(json={'users':[{'name':'alice','exchange':'hyperliquid'}]})
            elif path == '/api/v8/hosts':
                request.fulfill(json={'hosts':['disabled'],'host_capabilities':{}})
            else:
                request.fulfill(json={})

        page.route('**/*', route)
        page.goto('http://pbgui.test/api/v8/edit_page?name=alice')
        page.wait_for_function("document.getElementById('f-long-json').value.includes('panic_close_order_type')")
        assert page.locator('#f-hsl-engine').is_hidden()
        assert page.locator('#f-hsl-grace').is_hidden()
        assert page.locator('[data-pb8-bot-fields], [data-pb8-portfolio], [data-pb8-migration-changes]').count() == 0
        if migrated:
            marked = page.locator('#f-long-json').evaluate("el=>Array.from(el._botHighlightPre.children).filter(x=>x.style.background).map(x=>x.textContent.trim())")
            assert marked == ['"restart_after_red_policy": null,']
            assert page.locator('.hsl-migration-removals').count() == 1
            assert 'tp_only_with_active_entry_cancellation' in page.locator('.hsl-migration-removals').inner_text()
            assert len(page.locator('.hsl-migration-removals pre').inner_text().splitlines()) == 4
            assert page.locator('#f-pnls-lookback').get_attribute('data-config-migration') == 'live.pnls_max_lookback_days'
            assert page.locator('#f-long-twe').get_attribute('data-config-migration') == 'bot.long.risk.total_wallet_exposure_limit'
            assert page.locator('#f-short-twe').get_attribute('data-config-migration') is None
            raw_marked = page.locator('#cfg-raw-json-highlight span').evaluate_all("nodes=>nodes.filter(x=>x.style.background).map(x=>x.textContent.trim())")
            assert '"base_duration_minutes": 0.05,' in raw_marked
            assert any(line.startswith('"pnls_max_lookback_days": 35') for line in raw_marked)
            assert len([line for line in raw_marked if 'base_duration_minutes' in line]) == 1
        page.locator('#f-long-json').evaluate("""el=>{
          const side=JSON.parse(el.value);
          side.hsl.restart_after_red_policy='never';
          side.entry_cooldown.base_duration_minutes=4.25;
          el.value=JSON.stringify(side,null,2);el.dispatchEvent(new Event('input',{bubbles:true}));
        }""")
        page.wait_for_function('collectConfig().bot.long.entry_cooldown.base_duration_minutes === 4.25')
        assert page.evaluate('collectConfig().bot.long.hsl.restart_after_red_policy') == 'never'
        assert page.evaluate('collectConfig().bot.long.entry_cooldown.max_duration_minutes') is None
        assert page.evaluate('collectConfig().bot.long.hsl.no_restart_drawdown_threshold === undefined')
        page.evaluate('populateForm()')
        assert page.locator('[data-pb8-bot-fields], [data-pb8-portfolio], [data-pb8-migration-changes]').count() == 0
        assert page.locator('.hsl-migration-removals').count() == (1 if migrated else 0)
        assert page.evaluate('document.documentElement.scrollWidth <= window.innerWidth')
        assert not errors, errors
        browser.close()
