"""Offline JavaScript regressions for overview request ownership and authentication."""

from pathlib import Path
import shutil
import subprocess

import pytest


@pytest.mark.parametrize("ending", ["switch", "auth", "success", "failure"])
def test_overview_ignores_stale_responses_and_stops_after_auth_loss(ending):
    """Execute the actual overview controller with deferred, isolated requests."""
    node = shutil.which("node")
    if not node:
        pytest.skip("Node is required for the isolated frontend regression")
    source = (Path(__file__).resolve().parents[2] / "frontend/js/profit_sweep_overview.js").read_text()
    code = r"""
const assert = require('assert');
const elements = {};
let clears = 0, renders = 0, pending, timerId = 0;
const timers = new Map();
global.document = {hidden: false, addEventListener(){}, removeEventListener(){},
 getElementById(id) { return elements[id] ||= {textContent:'', replaceChildren(){clears++;},
 classList:{add(){},remove(){}}, setAttribute(){}}; }};
global.window = {setTimeout(fn, delay) {timers.set(++timerId,{fn,delay}); return timerId;},
 clearTimeout(id) {timers.delete(id);}};
""" + source + r"""
(async () => {
 const overview = new window.ProfitSweepOverview(() => new Promise((resolve,reject)=>{pending={resolve,reject};}),()=>{});
 overview.render = () => {renders++;};
 overview.active = true;
 overview.rows = [{name:'old'}];
 const request = overview.refresh();
 if (ENDING === 'switch') {overview.hide(); pending.resolve({accounts:[{name:'stale'}]});}
 if (ENDING === 'auth') pending.reject({status:401});
 if (ENDING === 'success') pending.resolve({accounts:[{name:'fresh'}]});
 if (ENDING === 'failure') pending.reject(new Error('offline'));
 await request;
 if (ENDING === 'switch') {assert.equal(renders,0);assert.equal(overview.rows[0].name,'old');assert.equal(timers.size,0);}
 if (ENDING === 'auth') {assert.equal(overview.stopped,true);assert.equal(overview.rows.length,0);assert.equal(timers.size,0);assert.equal(clears,2);}
 if (ENDING === 'success') {assert.equal(overview.rows[0].name,'fresh');assert.equal(renders,1);assert.equal(timers.size,1);}
 if (ENDING === 'failure') {assert.equal(overview.rows[0].stale,true);assert.equal(renders,1);assert.equal(timers.size,1);}
 overview.destroy();assert.equal(timers.size,0);
})().catch(error=>{console.error(error);process.exit(1);});
"""
    result = subprocess.run([node, "-e", "const ENDING=" + repr(ending) + ";\n" + code], capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stdout + result.stderr


def test_summary_groups_currency_simulation_unique_targets_and_missing_values():
    """Execute exact totals without combining assets, simulations, or duplicate wallets."""
    node = shutil.which("node")
    if not node:
        pytest.skip("Node is required for the isolated frontend regression")
    source = (Path(__file__).resolve().parents[2] / "frontend/js/profit_sweep_overview.js").read_text()
    code = "global.window={};\n" + source + r"""
const assert=require('assert');
const sum=window.ProfitSweepOverview.summaryGroups;
const rows=[
 {asset:'USDT',mode:'live',balance:'0.105',swept:'2',target_key:'shared',target_balance:'7',updated_at:10},
 {asset:'USDT',mode:'live',balance:'0.105',swept:'3',target_key:'shared',target_balance:'8',updated_at:20},
 {asset:'USDT',mode:'dry',balance:null,swept:'99',target_key:null,target_balance:'12'},
 {asset:'USDC',mode:'live',balance:'10',swept:'4',target_key:'other',target_balance:'5'}
];
const balances=sum(rows,'balance');
assert.equal(balances.find(x=>x.label==='USDT').amount,'0.21');
assert.equal(balances.find(x=>x.label==='USDT').missing,1);
assert.equal(balances.find(x=>x.label==='USDC').amount,'10.00');
const swept=sum(rows,'swept');
assert.equal(swept.find(x=>x.label==='USDT').amount,'5.00');
assert.equal(swept.find(x=>x.label==='USDT · sim').amount,'99.00');
const target=sum(rows,'target_balance').find(x=>x.label==='USDT');
assert.equal(target.amount,'8.00');assert.equal(target.missing,1);
assert.equal(sum([{asset:'USDC',mode:'live',balance:'-0.005'}],'balance')[0].amount,'-0.01');
"""
    result = subprocess.run([node, "-e", code], capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stdout + result.stderr
