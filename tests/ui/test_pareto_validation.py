"""Executable validation-draft contracts without runtime data or network access."""
from pathlib import Path
import subprocess


def test_validation_periods_overrides_and_source_immutability():
    """All modes preserve overrides and dates while producing standalone grouped jobs."""
    source = Path('frontend/js/pareto_validation.js').read_text()
    script = "const window={};\n" + source + r'''
const assert=require('node:assert/strict');
const config={backtest:{start_date:'2020-01-01',end_date:'2025-01-01',exchanges:['bybit'],suite_enabled:true,scenarios:[{label:'train',start_date:'2020-01-01',end_date:'2021-01-01'}]},bot:{long:{}},pbgui:{scenario_template:{template:'sweep_cycles'}}};
const detail={full_config:config,validation_holdouts:[{label:'holdout',start_date:'2024-01-01',end_date:'2025-01-01'}],override_configs:{'ETH.json':{bot:{long:{x:1}}}}};
const before=JSON.stringify(detail);
for(const [mode,count] of [['holdout_only',1],['full_timerange',1],['holdout_and_full_timerange',2],['all_timeranges',3]]) {
 const items=window.PBGuiParetoValidation.buildItems(detail,mode,'candidate','group');
 assert.equal(items.length,count);
 for(const item of items){
  assert.equal(item.preserve_timerange,true);
  assert.equal(item.preserve_exchanges,true);
  assert.deepEqual(item.override_configs,detail.override_configs);
  assert.equal(item.config.backtest.scenarios,undefined);
  assert.equal(item.config.pbgui.scenario_template,undefined);
  assert.equal(item.config.pbgui.backtest_result_group.id,'group');
 }
}
assert.equal(JSON.stringify(detail),before);
const items=window.PBGuiParetoValidation.buildItems(detail,'all_timeranges','c','g');
assert.equal(items[0].config.backtest.end_date,'2021-01-01');
assert.equal(items[1].config.backtest.start_date,'2024-01-01');
assert.equal(items[2].config.backtest.start_date,'2020-01-01');
assert.throws(()=>window.PBGuiParetoValidation.buildItems({...detail,validation_holdouts:[]},'holdout_only','c','g'),/Holdout/);
assert.throws(()=>window.PBGuiParetoValidation.buildItems({...detail,override_error:'missing'},'full_timerange','c','g'),/Override/);
assert.throws(()=>window.PBGuiParetoValidation.buildItems(detail,'invalid','c','g'),/mode/);
'''
    subprocess.run(['node','-e',script],check=True,capture_output=True,text=True)
