"""Cancel drains dashboard draft writes before deleting the draft and closing."""

from pathlib import Path
import subprocess

import pytest


@pytest.mark.parametrize('failure', [False, True])
def test_cancel_orders_pending_writes_and_delete(failure):
    """No delayed autosave can recreate the discarded draft after the editor closes."""
    source=(Path(__file__).resolve().parents[2]/'frontend/dashboard_editor.html').read_text()
    sync=source[source.index('  function scheduleSync()'):source.index('  function markViewDirty()')]
    cancel=source[source.index('  async function doCancel()'):source.index('  /* ── Listen for messages from parent')]
    script='const failure=%s;\n' % str(failure).lower()+sync+cancel+r'''
const assert=require('node:assert/strict');
let cancelling=false,syncInFlight=Promise.resolve(),syncTimer=null,state={draft:'changed'},ORIG_NAME='saved';
const requests=[],messages=[],statuses=[];
const window={parent:{postMessage:message=>messages.push(message)}},location={origin:'http://local'};
function setStatus(message){statuses.push(message);}
function apiFetch(url,options){return new Promise((resolve,reject)=>requests.push({options,resolve,reject}));}
async function flush(){await new Promise(resolve=>setImmediate(resolve));}
(async()=>{
 doSync();await flush();assert.equal(requests[0].options.method,'POST');
 scheduleSync();const cancel=doCancel();doSync();scheduleSync();
 await flush();assert.equal(requests.length,1);assert.equal(messages.length,0);
 requests[0].resolve({ok:true});await flush();
 assert.equal(requests.length,2);assert.equal(requests[1].options.method,'DELETE');
 requests[1].resolve({ok:!failure});await cancel;
 assert.equal(messages.length,failure?0:1);assert.equal(cancelling,!failure);
 if(failure){assert.match(statuses.at(-1),/Could not discard/);}
})().catch(error=>{console.error(error);process.exitCode=1;});
'''
    subprocess.run(['node','-e',script],check=True,capture_output=True,text=True,timeout=10)
