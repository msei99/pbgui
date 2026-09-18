"""Deterministic restart UI regressions without restarting real services."""
from pathlib import Path
import subprocess


def test_restart_waits_for_new_instance_and_keeps_button_busy():
    """Status races cannot unlock restart or mistake the old API for a completed restart."""
    source = Path('frontend/pbgui_nav.js').read_text()
    functions = []
    for name in ('showRestartOverlay', 'updateRestartButtonState'):
        start = source.index('  function ' + name + '(')
        end = source.index('\n  }', start) + 4
        functions.append(source[start:end])
    code = r'''
const assert=require('assert');
let _restartInFlight=true,_restartStatus={},reloads=0,timers=[],requests=0;
const nodes={};
function node(){return {style:{},classList:{add(){},toggle(){}},setAttribute(){},getAttribute(){return ''},remove(){},appendChild(){},addEventListener(){}};}
nodes['pbgui-restart-btn']=node();nodes['pbgui-restart-status']=node();
const document={getElementById:id=>nodes[id],createElement:node,body:{appendChild(n){nodes[n.id]=n}}};
const window={location:{origin:'http://test',reload(){reloads++}}};
const updateAuthModeState=()=>{}, updateMasterName=()=>{}, authOptions=x=>x;
const setTimeout=(fn)=>timers.push(fn);
let status={needs_restart:false,api_instance_id:'old'};
const fetch=async()=>{requests++;return {ok:true,json:async()=>status}};
const flush=()=>new Promise(r=>setImmediate(r));
(async()=>{
  updateRestartButtonState({needs_restart:false});
  assert.equal(nodes['pbgui-restart-btn'].disabled,true);
  assert.match(nodes['pbgui-restart-btn'].innerHTML,/Restarting/);
  showRestartOverlay('http://test',[],'old',true);
  assert.equal(timers.length,0);assert.equal(requests,0);
  assert.match(nodes['pbgui-restart-status'].textContent,/Requesting/);
  showRestartOverlay('http://test',[],'old');
  timers.shift()();await flush();assert.equal(reloads,0);
  status={needs_restart:false,api_instance_id:'new',restart_inspection_error:'unavailable'};
  timers.shift()();await flush();assert.equal(reloads,0);
  status={needs_restart:false,api_instance_id:'new'};
  timers.shift()();await flush();assert.equal(reloads,1);
})();
'''
    result = subprocess.run(['node', '-e', '\n'.join(functions) + code], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr


def test_confirmed_restart_is_visible_during_delayed_request_and_sent_once():
    """Duplicate clicks are ignored and rejected/lost replies have distinct recovery."""
    source = Path('frontend/pbgui_nav.js').read_text()
    block = source[source.index('    /* Restart button */'):source.index('    function startRestartStatusWatch()')]
    code = r'''
const assert=require('assert');
let _restartInFlight=false,_restartConfirmPending=false,_restartStatus={api_instance_id:'old'};
let click,confirm,reply,requests=0,overlays=[],errors=[],removed=0;
const btn={getAttribute(){return ''},classList:{add(){},remove(){}},addEventListener(_,fn){click=fn}};
const document={getElementById(id){return id==='pbgui-restart-btn'?btn:{remove(){removed++}}}};
const showNavConfirm=opts=>{
 if(opts.title==='Restart failed'){errors.push(opts);return Promise.resolve(true)}
 return new Promise(r=>confirm=r);
};
const _getAppBase=()=>'/prefix',authOptions=x=>x,fetchRestartStatus=()=>{};
const showRestartOverlay=(...args)=>overlays.push(args);
const fetch=()=>{requests++;return new Promise((resolve,reject)=>reply={resolve,reject})};
const flush=()=>new Promise(r=>setImmediate(r));
INSTALL
(async()=>{
 click();const firstConfirm=confirm;click();assert.equal(confirm,firstConfirm);
 confirm(true);await flush();
 assert.equal(requests,1);assert.equal(overlays[0][3],true);
 click();assert.equal(requests,1);
 reply.resolve({ok:false,json:async()=>({detail:'Protected job running'})});await flush();
 assert.equal(_restartInFlight,false);assert.equal(btn.disabled,false);assert.equal(removed,1);
 assert.equal(errors[0].detail,'Protected job running');
 click();confirm(true);await flush();
 reply.reject(new Error('Connection closed'));await flush();
 assert.equal(requests,2);assert.equal(_restartInFlight,true);
 assert.equal(errors.length,1);assert.equal(overlays.at(-1)[2],'old');
})();
'''.replace('INSTALL', block)
    result = subprocess.run(['node', '-e', code], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr
