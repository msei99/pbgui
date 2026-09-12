"""Dashboard batch creation carries each user and generates distinct names."""

from pathlib import Path
import subprocess

import pytest


@pytest.mark.parametrize('users,name,expected', [
    (['alice','bob'], 'Trading', ['Trading_alice','Trading_bob']),
    (['alice','bob'], '', ['alice','bob']),
    (['alice'], 'Trading', ['Trading']),
])
@pytest.mark.parametrize('exists', [False, True])
def test_batch_template_payloads(users, name, expected, exists):
    """Run the page click handler including both overwrite and create branches."""
    import json

    source = (Path(__file__).resolve().parents[2] / 'frontend/dashboard_templates.html').read_text()
    start = source.index("createBtn.addEventListener('click', async function() {")
    end = source.index('\n      });', start) + len('\n      });')
    script = 'const users=%s, name=%s, expected=%s, exists=%s;\n' % tuple(map(json.dumps, [users,name,expected,exists]))
    script += r'''
const assert = require('node:assert/strict');
let handler; const posted = [];
const createBtn = {addEventListener:(event, fn)=>handler=fn};
const document = {getElementById:id=>({value:id === 'tpl-select' ? 'source' : name})};
function getEffectiveUsers() {return users;}
async function confirmDialog() {return true;}
function showMsg() {}
const window = {parent:{postMessage:()=>{}}}, location = {origin:'http://localhost'};
function apiGet(path, cb) { cb(exists ? {config:{}} : {}); }
function apiPost(path, payload, cb) {posted.push(payload); cb({status:'ok'});}
'''
    script += source[start:end]
    script += r'''
(async()=>{
 await handler();
 await new Promise(resolve=>setImmediate(resolve));
 assert.deepEqual(posted.map(p=>p.name),expected);
 assert.deepEqual(posted.map(p=>p.user),users);
 assert.equal(createBtn.disabled,false);
})().catch(error=>{console.error(error);process.exitCode=1;});
'''
    subprocess.run(['node','-e',script],check=True,capture_output=True,text=True,timeout=10)
