"""Node checks for the PB8 editor's explicit Vast GPU sizing controls."""

from pathlib import Path
import subprocess

import pytest


def test_auto_gpu_fields_are_optional_for_vast_queue():
    """Vast requires the GPU backend but accepts automatic or partial sizing."""
    source = Path('frontend/js/vast.js').read_text()
    start = source.index('  function manualGpuErrors(')
    end = source.index('\n  function renderGpuRecommendation', start)
    code = r'''
const assert=require('assert');
const auto={pbgui:{execution:'vast'},optimize:{backend:'gpu',gpu:{
 auto_lean_parallelism:true,population_size:null,batch_size:null,max_dispatch_candidate_bars:null}}};
assert.deepEqual(manualGpuErrors(auto),[]);
auto.optimize.gpu.population_size=7168;
assert.deepEqual(manualGpuErrors(auto),[]);
auto.optimize.backend='pymoo';
assert.deepEqual(manualGpuErrors(auto).map(e=>e.path),['optimize.backend']);
auto.pbgui.execution='local';
assert.deepEqual(manualGpuErrors(auto),[]);
'''
    result = subprocess.run(['node', '-e', source[start:end] + code],
                            capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr


def test_measured_button_applies_explicit_triple_only_for_selected_offer():
    """One click writes all three fields and disables automatic sizing."""
    source = Path('frontend/js/vast.js').read_text()
    start = source.index('  function applyMeasuredGpuRecommendation(')
    end = source.index('\n  document.addEventListener', start)
    code = r'''
const assert=require('assert');
let selectedOffer={id:123},syncs=0,validations=0;
const values={'opted-gpu-auto-lean':{checked:true}};
for(const id of ['opted-gpu-population-size','opted-gpu-batch-size','opted-gpu-max-dispatch-bars']) values[id]={value:''};
const el=id=>values[id];
const scheduleStructuredEditorSync=()=>syncs++;
const scheduleValidation=()=>validations++;
let gpuRecommendation={offerId:124,result:{suggestion:{profile:{population_size:7168,batch_size:7168,
 max_dispatch_candidate_bars:130170880000}}}};
applyMeasuredGpuRecommendation();
assert.equal(syncs,0);
gpuRecommendation.offerId=123;
applyMeasuredGpuRecommendation();
assert.equal(values['opted-gpu-population-size'].value,'7168');
assert.equal(values['opted-gpu-batch-size'].value,'7168');
assert.equal(values['opted-gpu-max-dispatch-bars'].value,'130170880000');
assert.equal(values['opted-gpu-auto-lean'].checked,false);
assert.equal(syncs,1);assert.equal(validations,1);
'''
    result = subprocess.run(['node', '-e', source[start:end] + code],
                            capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr


def test_vast_execution_is_next_to_gpu_sizing_and_validation():
    """Cloud execution, optional sizing and validation stay together in the editor."""
    page = Path('frontend/v7_optimize.html').read_text()
    positions = [page.index(marker) for marker in (
        "'<div id=\"suite-container\"",
        "fieldSelect2('opted-execution'",
        "fieldSelect2('opted-opt-backend'",
        "'<div id=\"optimize-gpu-section\"",
        "fieldNumber2('opted-gpu-max-dispatch-bars'",
        "'<div id=\"opted-vast-validation\"",
    )]
    assert positions == sorted(positions)


def test_auto_gpu_sizing_passes_queue_validation():
    """Blank automatic GPU fields pass both background and queue validation."""
    source = Path('frontend/js/vast.js').read_text()
    manual = source[source.index('  function manualGpuErrors('):source.index('\n  function renderGpuRecommendation', source.index('  function manualGpuErrors('))]
    validate = source[source.index('  async function validateEditorConfig('):source.index('\n  function scheduleValidation', source.index('  async function validateEditorConfig('))]
    code = r'''
const assert=require('assert');
let validationGeneration=0,validationTimer=null,cloudMetrics=null,disposed=false,requests=0;
const paints=[];
const clearTimeout=()=>{},setQueueBlocked=()=>{},showValidation=(errors)=>paints.push(errors.map(error=>error.path));
const refreshGpuRecommendation=async()=>{};
const request=async()=>{requests++;return {valid:true,errors:[],metrics:[]};};
const config={pbgui:{execution:'vast'},optimize:{backend:'gpu',gpu:{
 auto_lean_parallelism:true,population_size:null,batch_size:null,max_dispatch_candidate_bars:null}}};
(async()=>{
 assert.equal(await validateEditorConfig(config),true);
 assert.equal(await validateEditorConfig(config,{forQueue:true}),true);
 assert.equal(requests,2);
 assert.deepEqual(paints.at(-1),[]);
 assert.equal(config.optimize.gpu.population_size,null);
 assert.equal(config.optimize.gpu.batch_size,null);
 assert.equal(config.optimize.gpu.max_dispatch_candidate_bars,null);
})().catch(error=>{console.error(error);process.exitCode=1});
'''
    result = subprocess.run(['node', '-e', manual + '\n' + validate + code],
                            capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr


def test_gpu_auto_stepper_reaches_blank_from_one():
    """Only nullable GPU steppers map the lower boundary back to Auto."""
    page = Path('frontend/v7_optimize.html').read_text()
    start = page.index('function stepNullablePositiveInteger(')
    end = page.index('\nfunction fieldOptimizeCpuCount2(', start)
    code = r'''
const assert=require('assert');
const inputs={gpu:{value:'2'}, batch:{value:''}};
const el=id=>inputs[id];
const esc=value=>String(value);
const fmtGroupSpan=(id,label,html)=>html;
stepNullablePositiveInteger('gpu',-1); assert.equal(inputs.gpu.value,'1');
stepNullablePositiveInteger('gpu',-1); assert.equal(inputs.gpu.value,'');
stepNullablePositiveInteger('gpu',-1); assert.equal(inputs.gpu.value,'');
stepNullablePositiveInteger('gpu',1); assert.equal(inputs.gpu.value,'1');
inputs.gpu.value='0'; stepNullablePositiveInteger('gpu',-1);
assert.equal(inputs.gpu.value,'');
stepNullablePositiveInteger('batch',1); assert.equal(inputs.batch.value,'1');
const nullable=fieldNumber2('gpu','population_size','',1,'',1,'auto after rent',true);
const ordinary=fieldNumber2('other','exact_workers',0,1,'',1);
assert(nullable.includes("stepNullablePositiveInteger('gpu', -1)"));
assert(nullable.includes("stepNullablePositiveInteger('gpu', 1)"));
assert(!ordinary.includes('stepNullablePositiveInteger'));
'''
    result = subprocess.run(['node', '-e', page[start:end] + code],
                            capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr

def test_queued_job_gpu_preview_uses_billions_and_keeps_exact_override():
    """A card preview exposes each job's Auto values and stores exact custom bars."""
    playwright = pytest.importorskip('playwright.sync_api')
    source = Path('frontend/js/vast.js').read_text()
    start = source.index('  const offerJobSelections = new Map()')
    end = source.index('  function setRentalGpuFieldMode(', start)
    with playwright.sync_playwright() as runner:
        browser = runner.chromium.launch(headless=True)
        try:
            page = browser.new_page()
            page.set_content('<div id="preview"></div>')
            page.add_script_tag(content='const el=id=>document.getElementById(id);' + source[start:end])
            result = page.evaluate('''() => {
              const assert = (condition, message) => { if (!condition) throw Error(message); };
              const first='a'.repeat(32), second='b'.repeat(32);
              const auto={population_size:8192,batch_size:8192,max_dispatch_candidate_bars:159252480000};
              const previews={jobs:[
                {id:first,name:'41-coin EMA',mode:'auto',auto,estimated_coin_candles:136080000,
                 candidate_bars_per_largest_scenario:19440000,measurements:[{
                   population_size:7168,batch_size:7168,max_dispatch_candidate_bars:130170880000,
                   measured_power_limit_watts:170,offered_power_limit_watts:160,
                   candidates_per_second:3.565,peak_vram_gib:6.62}]},
                {id:second,name:'small EMA',mode:'auto',auto:{
                  population_size:8192,batch_size:8192,max_dispatch_candidate_bars:1000000000},
                 estimated_coin_candles:3000000,candidate_bars_per_largest_scenario:1000000,measurements:[]}
              ],total_jobs:2};
              const selections=new Map();
              renderGpuJobPreviews('preview',previews,selections,false,()=>{});
              const cards=document.querySelectorAll('.rental-gpu-job');
              assert(cards.length===2,'both queued jobs must be shown');
              assert(cards[0].textContent.includes('159.25248 billion candidate-bars'),'readable work limit');
              const selector=cards[0].querySelector('select');
              selector.value='measured:0';selector.dispatchEvent(new Event('change'));
              assert(gpuJobOverrides(previews,selections)[first].max_dispatch_candidate_bars===130170880000,
                     'measurement must retain exact integer');
              document.querySelector('.rental-gpu-job select').value='custom';
              document.querySelector('.rental-gpu-job select').dispatchEvent(new Event('change'));
              const input=document.querySelector('.rental-gpu-job .fields label:last-child input');
              input.value='130.17088';input.dispatchEvent(new Event('input'));
              const selected=gpuJobOverrides(previews,selections);
              assert(selected[first].max_dispatch_candidate_bars===130170880000,'custom billions conversion');
              assert(!(second in selected),'other job remains Auto');
              return selected[first].max_dispatch_candidate_bars;
            }''')
            assert result == 130_170_880_000
        finally:
            browser.close()
