"""Execute the real automatic coordinator with a deterministic network and clock."""

from javascript_checks import run_javascript

from pathlib import Path


def test_coordinator_retains_jobs_after_lost_responses_and_stops_on_exit():
    """Lost Ensure/poll responses never abandon jobs or create overlapping requests."""
    source = Path("frontend/coin_data.html").read_text()
    lifecycle = source[source.index("    function showBusy("):source.index("    function toggleButtonState(")]
    coordinator = source[source.index("    function requestJson("):source.index("    function renderWarnings(")]
    script = r"""
const assert=browserAssert;
var autoTimer=0,autoController=null,autoGeneration=0,autoStopped=false,autoInFlight=false;
var autoAgain=false,autoFailures=0,autoJobs={},autoStatus='',autoDismissed=false;
var autoResults=[],autoStatusLevel=false;
var loadStateController=null,loadStateRequestSeq=0,filterInputDebounceTimer=0;
let requests=[],timers=new Map(),timerId=0,visible=false,status='',snapshots=0;
const overlay={classList:{add(){visible=true},remove(){visible=false}}};
const statusNode={set textContent(value){status=value},style:{}};
const document={hidden:false,getElementById:id=>id==='action-status'?statusNode:
 id==='busy-overlay'?overlay:{textContent:''},addEventListener(){}};
const window={setTimeout(fn,delay){timers.set(++timerId,{fn,delay});return timerId},
 clearTimeout(id){timers.delete(id)},addEventListener(){}};
const API_BASE='/api/coin-data',jsonHeaders=()=>({}),buildRefreshPayload=()=>({exchange:'bybit'});
const buildRefreshJobUrl=id=>'/job/'+id,updateBusyProgress=()=>{};
const loadState=async()=>{snapshots++};
const fetch=(url,options)=>new Promise((resolve,reject)=>requests.push({url,options,resolve,reject}));
const flush=async()=>{for(let i=0;i<30;i++)await Promise.resolve()};
const reply=async(i,body,status=200)=>{requests[i].resolve({ok:status===200,status,
 json:async()=>body});await flush()};
const tick=()=>{assert.equal(timers.size,1);const [id,t]=[...timers][0];timers.delete(id);t.fn()};
""" + lifecycle + coordinator + r"""
(async()=>{
 runAutomaticUpdate();runAutomaticUpdate();assert.equal(requests.length,1);
 requests[0].reject(new TypeError('lost Ensure response'));await flush();
 assert.equal([...timers.values()][0].delay,2000);assert.equal(snapshots,0);
 tick();await reply(1,{job_ids:['a','b'],cmc_pool:{active_credentials:1}});
 assert.equal(requests[2].url,'/job/a');assert.equal(requests.length,3);
 await reply(2,{job:{status:'running',message:'A running'}});
 assert.equal(requests[3].url,'/job/b');
 await reply(3,{job:{status:'running',message:'B running'}});
 assert.equal(snapshots,1);assert.equal(visible,true);
 dismissBusy();assert.equal(visible,false);assert.equal(Object.keys(autoJobs).length,2);
 tick();await reply(4,{job_ids:[],cmc_pool:{active_credentials:1}});
 requests[5].reject(new TypeError('poll disconnected'));await flush();
 assert.deepEqual(Object.keys(autoJobs),['a','b']);assert.equal(visible,false);
 assert.equal([...timers.values()][0].delay,2000);
 tick();await reply(6,{job_ids:['a','b'],cmc_pool:{active_credentials:1}});
 await reply(7,{job:{status:'completed',message:'A done',state:{stale:true}}});
 await reply(8,{job:{status:'error',message:'B failed'}});
 assert.equal(Object.keys(autoJobs).length,0);assert.equal(visible,false);
 assert.equal(snapshots,2);
 tick();requests[9].resolve({ok:true,json:async()=>{throw new SyntaxError('bad JSON')}});await flush();
 assert.equal(autoFailures,1);assert.equal(snapshots,2);
 tick();await reply(10,{job_ids:['c'],cmc_pool:{active_credentials:1}});
 stopAutomaticUpdates();assert.equal(requests[11].options.signal.aborted,true);
 await reply(11,{job:{status:'completed',message:'stale callback'}});
 assert.equal(snapshots,2);assert.equal(timers.size,0);
})().catch(e=>{console.error(e);testResult.exitCode=1});
"""
    result = run_javascript(["-e", script], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stdout + result.stderr
