"""Queue-in-place behavior with isolated requests and no production state."""
from pathlib import Path
import subprocess


def test_queue_stays_put_retries_and_keeps_validation_group():
    """Partial retries reuse operations, preserve grouping and do not navigate/start."""
    source = Path('frontend/js/backtest_queue_actions.js').read_text()
    script = r'''
const assert=require('node:assert/strict');
const crypto=require('node:crypto').webcrypto;
const status={textContent:''}, auto={textContent:''}, open={textContent:'',addEventListener:(event,fn)=>open.click=fn};
const document={readyState:'complete',querySelectorAll:selector=>selector.includes('status')?[status]:selector.includes('autostart')?[auto]:[open]};
const window={OPTIMIZE_VERSION:'v7',location:{href:'explorer'}};
const posts=[];
let fail=true;
async function fetch(url,opts){
 if(!opts || !opts.method) return {ok:true,json:async()=>url.endsWith('/settings')?{autostart:true}:{items:[{status:'queued'},{status:'complete'}]}};
 assert.equal(url,'/api/backtest-v7/queue');
 const body=JSON.parse(opts.body);posts.push(body);
 if(body.name==='second'&&fail){fail=false;return {ok:false,text:async()=> 'temporary failure'};}
 return {ok:true,json:async()=>({filename:body.name})};
}
'''+source+r'''
(async()=>{
 const client=window.PBGuiBacktestQueue;
 const item=name=>({name,config:{bot:{long:{a:1}},pbgui:{backtest_result_group:{id:'first-attempt',kind:'optimize_validate'}}},override_configs:{'ETH.json':{bot:{long:{b:2}}}}});
 const items=[item('first'),item('second')];
 const button={textContent:'Queue Validation',disabled:false};
 await assert.rejects(client.perform(button,()=>client.submit(items)),/1 jobs added/);
 assert.equal(button.disabled,false);
 items.forEach(item=>item.config.pbgui.backtest_result_group.id='retry-attempt');
 const result=await client.perform(button,()=>client.submit(items));
 assert.deepEqual(result,{added:1,skipped:1});
 assert.equal(posts.length,3);
 assert.equal(posts[1].operation_id,posts[2].operation_id);
 assert.equal(posts[0].config.pbgui.backtest_result_group.id,posts[2].config.pbgui.backtest_result_group.id);
 assert.deepEqual(posts[0].override_configs,items[0].override_configs);
 await client.submit(items);
 assert.equal(posts.length,3);
 assert.equal(window.location.href,'explorer');
 assert.match(status.textContent,/already queued/);
 assert.equal(open.textContent,'Open Queue (1)');
 assert.match(auto.textContent,/Autostart ON/);
 open.click();
 assert.equal(window.location.href,'/api/backtest-v7/main_page?panel=queue');
 let release; const waiting=new Promise(resolve=>release=resolve);
 let calls=0;const first=client.perform(button,()=>{calls++;return waiting;});
 assert.equal(button.textContent,'Queue Validation');
 assert.equal(button.disabled,false);
 await client.perform(button,()=>calls++);release();await first;
 assert.equal(calls,2);
})().catch(error=>{console.error(error);process.exit(1);});
'''
    subprocess.run(['node','-e',script],check=True,text=True,capture_output=True)


def test_background_batches_snapshot_selection_and_continue_after_failure():
    """Pending submissions accept more selections without duplicate POSTs or mutation."""
    source = Path('frontend/js/backtest_queue_actions.js').read_text()
    script = r'''
const assert = require('node:assert/strict');
const crypto = require('node:crypto').webcrypto;
const status={textContent:''}, auto={textContent:''};
const open={textContent:'',addEventListener:(event,fn)=>open.click=fn};
const document={readyState:'complete',querySelectorAll:s=>s.includes('status')?[status]:s.includes('autostart')?[auto]:[open]};
const tabs=[];
const window={OPTIMIZE_VERSION:'v7',location:{href:'explorer'},open:(...args)=>tabs.push(args)};
let release, started, fail=true;
const posting=new Promise(resolve=>started=resolve);
const gate=new Promise(resolve=>release=resolve);
const posts=[];
async function fetch(url,opts) {
 if (!opts || !opts.method) return {ok:true,json:async()=>url.endsWith('/settings')?{autostart:false}:{items:[]}};
 const body=JSON.parse(opts.body); posts.push(body);
 if (body.name==='first') {started(); await gate;}
 if (body.name==='fails' && fail) {fail=false;return {ok:false,text:async()=>'temporary failure'};}
 return {ok:true,json:async()=>({filename:body.name})};
}
''' + source + r'''
(async()=>{
 const client=window.PBGuiBacktestQueue;
 const button={textContent:'Queue Validation',disabled:false};
 const item=name=>({name,config:{bot:{value:name}},override_configs:{}});
 const first=client.perform(button,()=>client.submit([item('first')]));
 await posting;
 const secondItems=[item('second')];
 const second=client.perform(button,()=>client.submit(secondItems));
 secondItems[0].config.bot.value='changed selection';
 secondItems[0].name='changed name';
 const duplicate=client.perform(button,()=>client.submit([item('second')]));
 const failure=client.perform(button,()=>client.submit([item('fails')]));
 const expectedFailure=assert.rejects(failure,/temporary failure/);
 const last=client.perform(button,()=>client.submit([item('last')]));
 assert.equal(button.disabled,false);
 assert.equal(button.textContent,'Queue Validation');
 assert.match(status.textContent,/4 more batch/);
 assert.equal(posts.length,1);
 open.click();
 assert.deepEqual(tabs,[['/api/backtest-v7/main_page?panel=queue','_blank','noopener']]);
 assert.equal(window.location.href,'explorer');
 release();
 await Promise.all([first,second,duplicate,expectedFailure,last]);
 assert.deepEqual(posts.map(p=>p.name),['first','second','fails','last']);
 assert.equal(posts[1].config.bot.value,'second');
 assert.deepEqual(await duplicate,{added:0,skipped:1});
 assert.doesNotMatch(status.textContent,/batch\(es\) waiting/);
 await client.perform(button,()=>client.submit([item('fails')]));
 assert.equal(posts[2].operation_id,posts[4].operation_id);
 open.click();
 assert.equal(window.location.href,'/api/backtest-v7/main_page?panel=queue');
})().catch(error=>{console.error(error);process.exit(1);});
'''
    subprocess.run(['node', '-e', script], check=True, text=True, capture_output=True)


def test_pb8_uses_one_durable_backend_batch_and_restores_progress():
    """PB8 submits one command and reads progress from the server."""
    source = Path('frontend/js/backtest_queue_actions.js').read_text()
    script = r'''
const assert=require('node:assert/strict');
const crypto=require('node:crypto').webcrypto;
const status={textContent:''}, auto={textContent:''};
const open={textContent:'',addEventListener:(event,fn)=>open.click=fn};
const document={readyState:'complete',querySelectorAll:s=>s.includes('status')?[status]:s.includes('autostart')?[auto]:[open]};
const window={OPTIMIZE_VERSION:'v8',location:{href:'explorer'}};
const posts=[];
let confirmed=0;
async function fetch(url,opts) {
  if(opts && opts.method) {
    assert.equal(url,'/api/backtest-v8/queue/batches');
    const body=JSON.parse(opts.body);
    posts.push(body);
    return {ok:true,json:async()=>({batch_id:'batch-1',status:'queued',total:body.items.length,confirmed:0})};
  }
  if(url.endsWith('/settings')) return {ok:true,json:async()=>({autostart:true})};
  if(url.endsWith('/batches')) return {ok:true,json:async()=>({batches:[{status:'running',total:90,confirmed}]})};
  return {ok:true,json:async()=>({items:[]})};
}
''' + source + r'''
(async()=>{
  const item=i=>({name:'candidate-'+i,config:{bot:{index:i},pbgui:{backtest_result_group:{id:'group'}}},override_configs:{}});
  const result=await window.PBGuiBacktestQueue.submit(Array.from({length:90},(_,i)=>item(i)));
  assert.equal(posts.length,1);
  assert.equal(posts[0].items.length,90);
  assert.equal(result.queued,90);
  assert.equal(window.location.href,'explorer');
  confirmed=10;
  await window.PBGuiBacktestQueue.refresh();
  assert.match(status.textContent,/10 \/ 90 jobs queued by PBGui server/);
})().catch(error=>{console.error(error);process.exit(1)});
'''
    subprocess.run(['node', '-e', script], check=True, text=True, capture_output=True)
