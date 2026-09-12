"""Offline regressions for stalled and superseded Profit Sweep previews."""

import json
import subprocess
from pathlib import Path

import pytest

PAGE = Path(__file__).resolve().parents[2] / "frontend/profit_sweep.html"


def function(name):
    """Extract one top-level inline page function for isolated Node execution."""
    source = PAGE.read_text()
    import re
    match = re.search(r"^    (?:async )?function " + name + r"\(", source, re.M)
    end = source.index("\n    }", match.start()) + len("\n    }")
    return source[match.start():end]


def execute(code):
    """Run mocked JavaScript without browser, network, or runtime data."""
    result = subprocess.run(["node", "-e", code], capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("ending", ["timeout", "switch", "success", "preaborted"])
def test_preview_deadline_and_account_abort_cleanup(ending):
    """Deadline reports a failure while account changes retain cancellation semantics."""
    execute(function("requestPreview") + "const ending=" + json.dumps(ending) + r"""
const assert = require('assert');
let timer, cleared = 0, removed = 0;
const window = {setTimeout(fn, ms) {assert.equal(ms,60000); timer=fn; return 1;},
 clearTimeout(id) {assert.equal(id,1); cleared++;}};
const account = new AbortController();
const remove=account.signal.removeEventListener.bind(account.signal);
account.signal.removeEventListener=(...args)=>{removed++;remove(...args);};
function requestJson(path, options) {
 assert.equal(path,'/evaluate/a%2Fb');
 return new Promise((resolve,reject)=>{
  const abort=()=>reject(Object.assign(new Error('aborted'),{name:'AbortError'}));
  options.signal.addEventListener('abort',abort,{once:true});
  if(options.signal.aborted) abort();
  else if(ending==='success') resolve({ok:true});
 });
}
(async()=>{
 if(ending==='preaborted')account.abort();
 const result=requestPreview('a/b',{},account.signal);
 if(ending==='timeout')timer();
 if(ending==='switch')account.abort();
 if(ending==='success') assert.deepEqual(await result,{ok:true});
 else await assert.rejects(result,e=>ending==='timeout'?e.message.includes('60 seconds'):e.name==='AbortError');
 assert.equal(cleared,1);assert.equal(removed,1);
})().catch(e=>{console.error(e);process.exit(1);});
""")


@pytest.mark.parametrize("vault", [False, True])
def test_balance_error_replaces_loading_and_recovers(vault):
    """Failure is visible in Exchange / Vault, and recovery removes the error."""
    execute(function("renderAccountBalances") + "const vault=" + json.dumps(vault) + r"""
const assert=require('assert');
const nodes={};function byId(id){return nodes[id] ||= {};}
const state={snapshot:null,previewError:'Rate limited <test>'};
const text=(value,fallback)=>value==null?fallback:String(value);
const balanceText=b=>b.amount+' USDC';const vaultShareText=v=>String(v);
renderAccountBalances({is_vault:vault});
assert.equal(nodes['exchange-read-error'].hidden,false);
assert.ok(nodes['exchange-read-error'].textContent.includes('Rate limited <test>'));
for(const id of ['source','destination','transferable'])assert.equal(nodes[id+'-balance-value'].textContent,'Unavailable');
if(vault)assert.equal(nodes['vault-tvl-value'].textContent,'Unavailable');
state.previewError='';state.snapshot={account_balances:{source:{amount:5},destination:{amount:1},max_transferable:3}};
renderAccountBalances({is_vault:false});
assert.equal(nodes['exchange-read-error'].hidden,true);
assert.equal(nodes['source-balance-value'].textContent,'5 USDC');
""")


def test_failed_preview_releases_loading_and_renders_exchange_error():
    """A rejected snapshot releases the busy state and schedules a retry."""
    execute(function("evaluateNow") + r"""
const assert=require('assert');
const state={selectedUser:'a',accountGeneration:1,previewLoading:false,users:[{name:'a'}],schema:{defaults:{}}};
const isCurrentAccount=(user,generation)=>user===state.selectedUser&&generation===state.accountGeneration;
function renderPreview(){}let rendered=0,retry;
function renderAccountBalances(){rendered++;assert.equal(state.previewLoading,false);assert.equal(state.previewError,'failed');}
function scheduleAutomaticPreview(delay){retry=delay;}
async function requestPreview(){throw new Error('failed');}
(async()=>{await evaluateNow();assert.equal(rendered,1);assert.equal(retry,15000);assert.equal(state.previewLoading,false);})().catch(e=>{console.error(e);process.exit(1);});
""")
