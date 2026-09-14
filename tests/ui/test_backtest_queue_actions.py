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
const window={OPTIMIZE_VERSION:'v8',location:{href:'explorer'}};
const posts=[];
let fail=true;
async function fetch(url,opts){
 if(!opts || !opts.method) return {ok:true,json:async()=>url.endsWith('/settings')?{autostart:true}:{items:[{status:'queued'},{status:'complete'}]}};
 assert.equal(url,'/api/backtest-v8/queue');
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
 assert.equal(window.location.href,'/api/backtest-v8/main_page?panel=queue');
 let release; const waiting=new Promise(resolve=>release=resolve);
 let calls=0;const first=client.perform(button,()=>{calls++;return waiting;});
 assert.equal(button.textContent,'Adding…');
 await client.perform(button,()=>calls++);release();await first;
 assert.equal(calls,1);
})().catch(error=>{console.error(error);process.exit(1);});
'''
    subprocess.run(['node','-e',script],check=True,text=True,capture_output=True)
