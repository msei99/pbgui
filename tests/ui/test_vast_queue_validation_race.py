"""Queue validation must not be cancelled by a later background draft check."""
from pathlib import Path
import subprocess


def test_save_and_queue_failure_waits_for_acknowledgement_without_saving():
    """A failed cloud check retains the draft and blocks writes until the dialog closes."""
    source = Path('frontend/v7_optimize.html').read_text()
    start = source.index('async function saveEditor(andQueue) {')
    end = source.index('\nasync function saveAndQueue()', start)
    code = r'''
const assert=require('node:assert/strict');
const draft={pbgui:{execution:'vast'}};
let state={editorSaving:false},dialog,dismiss,calls=[],failure=null;
const setOptimizeEditorSaving=value=>{state.editorSaving=value};
const setPageEditorStatus=()=>{};
const editorVisible=()=>true;
const ensureRawJsonValidForSave=()=>true;
const ensureStructuredJsonFieldsValidForSave=()=>true;
const collectEditorConfig=()=>({name:'unsaved',config:draft});
const optimizeEditorAdapter={isV8:true};
const el=()=>({textContent:'Short enabledness must remain fixed.'});
const handleError=error=>calls.push(['error',error.message]);
const apiFetch=async()=>{calls.push(['save']);throw Error('Save unavailable');};
const window={
 PBGuiVast:{validateConfig:async()=>{if(failure)throw failure;return false;}},
 PBGuiDialogs:{alert:options=>{dialog=options;return new Promise(resolve=>{dismiss=resolve});}}
};
(async()=>{
 const pending=saveEditor(true);
 await new Promise(resolve=>setImmediate(resolve));
 assert.equal(dialog.title,'Cannot queue configuration');
 assert.equal(dialog.confirmText,'OK');
 assert.match(dialog.detail,/Short enabledness/);
 assert.equal(state.editorSaving,true);
 assert.equal(calls.filter(row=>row[0]==='save').length,0);
 assert.deepEqual(draft,{pbgui:{execution:'vast'}});
 dismiss();await pending;
 assert.equal(state.editorSaving,false);
 failure=Error('Compatibility service unavailable');
 const network=saveEditor(true);
 await new Promise(resolve=>setImmediate(resolve));
 assert.equal(dialog.message,failure.message);
 assert.equal(dialog.detail,'');
 dismiss();await network;
 dialog=null;
 await saveEditor(false);
 assert.equal(dialog,null);
 assert.equal(calls.filter(row=>row[0]==='save').length,1);
})();
'''
    result = subprocess.run(['node', '-e', source[start:end] + code], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr


def test_queue_validation_survives_background_generation_without_repainting():
    """A validated snapshot can queue; stale UI results stay suppressed and errors survive."""
    source = Path('frontend/js/vast.js').read_text()
    start = source.index('  async function validateEditorConfig(')
    end = source.index('\n  function scheduleValidation', start)
    code = r'''
const assert=require('assert');
let validationGeneration=0,validationTimer=null,cloudMetrics=null,disposed=false,resolve,reject,paint=0,sizingSubmitted=false;
const refreshGpuRecommendation=async()=>{};
const manualGpuErrors=()=>[];
const clearTimeout=()=>{},setQueueBlocked=()=>{},showValidation=()=>paint++;
const request=()=>new Promise((a,b)=>{resolve=a;reject=b});
(async()=>{
 const queued=validateEditorConfig({name:'captured'},{forQueue:true});
 validationGeneration++;
 resolve({valid:true,errors:[]});
 assert.equal(await queued,true);assert.equal(paint,1);
 const stale=validateEditorConfig({name:'background'});
 validationGeneration++;resolve({valid:true,errors:[]});assert.equal(await stale,false);
 const invalid=validateEditorConfig({},{forQueue:true});
 validationGeneration++;resolve({valid:false,errors:[]});assert.equal(await invalid,false);
 const failed=validateEditorConfig({},{forQueue:true});reject(Error('Service unavailable'));
 await assert.rejects(failed,/Service unavailable/);
})();
'''
    result = subprocess.run(['node', '-e', source[start:end] + code], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr


def test_direction_hints_are_nonblocking_and_not_marked_as_errors():
    """Execute the real renderer with an isolated minimal DOM, without Chromium."""
    source = Path('frontend/js/vast.js').read_text()
    start = source.index('  function showValidation(')
    end = source.index('\n  function manualGpuErrors', start)
    code = r'''
const assert=require('assert');
class Node {
 constructor(){this.children=[];this.dataset={};this.classes=new Set();this.hidden=true;this.attributes={};
  this.classList={add:name=>this.classes.add(name),remove:name=>this.classes.delete(name),toggle:(name,on)=>on?this.classes.add(name):this.classes.delete(name)};}
 appendChild(node){this.children.push(node);return node;}
 append(...nodes){nodes.forEach(node=>this.appendChild(node));}
 replaceChildren(...nodes){this.children=nodes;}
 setAttribute(name,value){this.attributes[name]=value;}
 removeAttribute(name){delete this.attributes[name];}
 querySelector(){return null;}
 get childElementCount(){return this.children.length;}
 get content(){return this.textContent||this.children.map(node=>node.content).join(' ');}
}
const box=new Node(),field=new Node(),exposure=new Node(),positions=new Node();
const el=id=>id==='opted-vast-validation'?box:id==='opted-iters'?field:
 id==='bound-short.risk.total_wallet_exposure_limit'?exposure:id==='bound-short.risk.n_positions'?positions:null;
const document={querySelectorAll:()=>[field,exposure,positions].filter(node=>node.classes.has('cloud-invalid')),createDocumentFragment:()=>new Node(),createElement:()=>new Node()};
const window={state:{},getOptimizeBoundDomId:key=>'bound-'+key};
const pendingCount={path:'bot.long.risk.n_positions',message:'Unknown prepared coin count',suggestions:[],pending_coin_count:true};
showValidation([],false,{revision:'00ce7d0',warnings:[pendingCount]});
assert.equal(box.hidden,true);
showValidation([],false,{revision:'00ce7d0',warnings:[{path:'optimize.iters',message:'Other nonblocking hint',suggestions:[]}]});
assert.equal(box.hidden,false);
assert(!box.classes.has('cloud-validation-error'));
assert(!field.classes.has('cloud-invalid'));
assert(box.content.includes('GPU direction hints (1)'));
assert(box.content.includes('Hint: Other nonblocking hint'));
showValidation([{path:'optimize.iters',message:'Blocking failure',bound_keys:['short.risk.total_wallet_exposure_limit']}],false,{revision:'00ce7d0',warnings:[pendingCount]});
assert(box.classes.has('cloud-validation-error'));
assert(field.classes.has('cloud-invalid'));
assert(!box.content.includes('Unknown prepared coin count'));
assert(exposure.classes.has('cloud-invalid'));
assert.equal(exposure.attributes['aria-invalid'],'true');
assert(!positions.classes.has('cloud-invalid'));
showValidation([{path:'bot.short.risk.n_positions',message:'Invalid positions',bound_keys:['short.risk.n_positions']}],false,{revision:'00ce7d0',warnings:[]});
assert(!exposure.classes.has('cloud-invalid'));
assert(positions.classes.has('cloud-invalid'));
showValidation([],false,{revision:'00ce7d0',warnings:[]});
assert.equal(box.hidden,true);
assert(!positions.classes.has('cloud-invalid'));
assert.equal(positions.attributes['aria-invalid'],undefined);
'''
    result = subprocess.run(['node', '-e', source[start:end] + code], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr
