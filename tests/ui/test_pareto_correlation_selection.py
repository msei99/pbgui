"""Executable correlation selection and detail visibility regression checks."""
from pathlib import Path
import subprocess


def test_radar_selection_and_detail_visibility():
    """Polar clicks select their config and Correlations keeps shared details visible."""
    page = Path('frontend/v7_pareto_explorer.html').read_text()
    functions = page[page.index('  function bindPlotClick('):page.index('  function refreshPlaygroundFromSettings(')]
    functions += page[page.index('  function applyDeepTabUi('):page.index('  function loadCommandCenterData(')]
    script = r'''
const assert=require('node:assert/strict');
const nodes={};
const el=id=>nodes[id]||(nodes[id]={style:{}});
const state={stage:'deep_intelligence',allResultsLoaded:false,deepEvolution:{requires_full_mode:true}};
const document={querySelectorAll:()=>[]};
const window={requestAnimationFrame:f=>f()};
const resizePlotlyNodes=()=>{},scheduleDeepPlotResize=()=>{},updateLocationState=()=>{};
'''+functions+r'''
applyDeepTabUi('correlations',false);
assert.equal(el('selected-config-detail').style.display,'block');
assert.equal(el('selected-config-section').style.display,'grid');
let callback, selected;
const node={removeAllListeners:()=>{},on:(event,fn)=>callback=fn};
bindPlotClick(node,index=>selected=index);
callback({points:[{customdata:[3365]}]});
assert.equal(selected,3365);
callback({points:[{data:{meta:{config_index:913}}}]});
assert.equal(selected,913);
applyDeepTabUi('parameters',false);
assert.equal(el('selected-config-detail').style.display,'none');
applyDeepTabUi('correlations',false);
assert.equal(el('selected-config-detail').style.display,'block');
'''
    subprocess.run(['node','-e',script],check=True,capture_output=True,text=True)
