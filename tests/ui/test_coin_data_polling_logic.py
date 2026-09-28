"""Execute Coin Data's real polling functions against a deterministic fake clock."""

from pathlib import Path
import subprocess


def test_coin_data_polling_is_sequential_adaptive_and_generation_guarded():
    """Slow requests, dismissal, hidden tabs, replacement and completion remain safe."""
    source = Path('frontend/coin_data.html').read_text()
    lifecycle = source[source.index('    function stopBusyPolling('):source.index('    function setActionStatus(')]
    polling = source[source.index('    function pollRefreshJob('):source.index('    var refreshPending')]
    script = r'''
const assert = require('node:assert/strict');
let timers = new Map(), timerId = 0, requests = [], hidden = false, visible = true;
let messages = [], applied = [], progress = [];
var busyJobPollTimer = 0, busyJobId = '', busyPollGeneration = 0, busyPollController = null;
const overlay = {classList:{contains:()=>visible,remove:()=>{visible=false},add:()=>{visible=true}}};
const document = {get hidden(){return hidden}, getElementById:()=>overlay};
const window = {setTimeout:(fn,delay)=>{timers.set(++timerId,{fn,delay});return timerId},
 clearTimeout:id=>timers.delete(id),addEventListener:()=>{}};
const buildRefreshJobUrl = id=>id;
const setActionStatus = (...args)=>messages.push(args);
const updateBusyProgress = (...args)=>progress.push(args);
const applyServerState = data=>applied.push(data);
const refreshJobMessage = job=>job.message;
const fetch = (url,options)=>new Promise(resolve=>requests.push({url,options,resolve}));
const flush = async()=>{for(let i=0;i<10;i++)await Promise.resolve()};
const respond = async(index,job)=>{requests[index].resolve({ok:true,json:async()=>({job})});await flush()};
const tick = ()=>{assert.equal(timers.size,1);const [id,timer]=[...timers][0];timers.delete(id);timer.fn();};
''' + lifecycle + polling + r'''
(async()=>{
 startRefreshJobPolling('a','done');
 assert.equal(requests.length,1); assert.equal(timers.size,0);
 await respond(0,{status:'running',percent:1});
 assert.equal([...timers.values()][0].delay,750);
 dismissBusy(); tick();
 assert.equal(requests.length,2);assert.equal(timers.size,0);
 await respond(1,{status:'running',percent:2});
 assert.equal([...timers.values()][0].delay,2500);
 hidden=true;tick();await respond(2,{status:'running',percent:3});
 assert.equal([...timers.values()][0].delay,5000);
 tick();startRefreshJobPolling('a','done');
 assert.equal(requests[3].options.signal.aborted,true);
 const priorProgress=progress.length;
 await respond(3,{status:'completed',state:{stale:true}});
 assert.equal(progress.length,priorProgress);assert.deepEqual(applied,[]);
 assert.equal(busyJobId,'a');assert.equal(timers.size,0);
 await respond(4,{status:'completed',state:{fresh:true}});
 assert.deepEqual(applied,[{fresh:true}]);assert.equal(busyJobId,'');assert.equal(timers.size,0);
 startRefreshJobPolling('b','done');stopBusyPolling();
 await respond(5,{status:'running',percent:99});
 assert.equal(timers.size,0);assert.equal(requests[5].options.signal.aborted,true);
})().catch(error=>{console.error(error);process.exitCode=1});
'''
    result = subprocess.run(['node', '-e', script], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stdout + result.stderr
