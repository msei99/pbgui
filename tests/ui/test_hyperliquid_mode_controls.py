"""Execute account-mode and exact-amount UI controls offline in Node."""

import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def page_function(page, name):
    """Extract the actual inline function, retaining all production behavior."""
    source = (ROOT / "frontend" / page).read_text()
    match = re.search(r"^    (?:async )?function " + name + r"\(", source, re.M)
    if page == "transfers.html":
        return source[match.start():source.index("\n", match.start())]
    return source[match.start():source.index("\n    }", match.start()) + 6]


def run_js(code):
    """Run without browser storage, exchange network, or production files."""
    result = subprocess.run(["node", "-e", code], capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stdout + result.stderr


def test_max_uses_route_cap_and_blocks_unsafe_states():
    """Max remains unavailable without a valid cap or with a retained operation."""
    run_js(page_function("transfers.html", "updateSubmit") + r"""
const assert=require('assert');
const nodes={'amount':{value:'99.842907'}};
const el=id=>nodes[id] ||= {};
const state={selected:'alice',busy:false,preview:{}};
let route={id:'spot_to_perp',minimum_amount:'0.000001',max_transferable:'99.842907'};
const selectedRoute=()=>route;
updateSubmit();assert.equal(el('max-amount').disabled,false);assert.equal(el('submit').disabled,false);
state.busy=true;updateSubmit();assert.equal(el('max-amount').disabled,true);
state.busy=false;state.pendingRequest={user:'alice',route:'spot_to_perp',amount:'99.842907'};
updateSubmit();assert.equal(el('max-amount').disabled,true);
state.pendingRequest=null;route.max_transferable='0';updateSubmit();assert.equal(el('max-amount').disabled,true);
route.max_transferable='99.842907';state.preview.blocked_reason='Unified';
updateSubmit();assert.equal(el('max-amount').disabled,true);assert.equal(el('submit').disabled,true);
state.preview={};route=null;updateSubmit();assert.equal(el('max-amount').disabled,true);
""")


def test_profit_sweep_mode_change_disables_and_restores_actions():
    """Unified is visible and blocked; refreshed Standard and Vaults recover controls."""
    run_js(page_function("profit_sweep.html", "detectedAccountMode") + page_function("profit_sweep.html", "renderAccountMode") + r"""
const assert=require('assert');const nodes={};const byId=id=>nodes[id] ||= {};
const liveActivations=new Map();function renderAccounts(){}function setAccountActionsEnabled(){}
const state={schema:{live_available:true},selectedUser:'alice',preview:{account_mode:{label:'Unified Account',unsupported:true}},snapshot:{account:{mode:'unified'}}};
const user={name:'alice',exchange:'hyperliquid',is_vault:false};
renderAccountMode(user);assert.equal(byId('enable-live').disabled,true);assert.equal(byId('enable-dry').disabled,true);
assert.ok(byId('account-subtitle').textContent.includes('Unified Account'));
state.preview={};state.snapshot.account.mode='standard_manual';renderAccountMode(user);
assert.equal(byId('enable-live').disabled,false);assert.equal(byId('enable-dry').disabled,false);
assert.ok(byId('account-subtitle').textContent.includes('Standard / Manual'));
state.preview={account_mode:{label:'Unified Account',unsupported:true}};
user.is_vault=true;renderAccountMode(user);assert.equal(byId('enable-live').disabled,false);
""")


def test_api_keys_shows_shared_balance_and_mode_guidance():
    """Connection testing renders Unified USDC and replaces it after switching mode."""
    run_js(page_function("api_keys_editor.html", "testConnection") + r"""
const assert=require('assert');const nodes={};
const document={getElementById(id){return nodes[id] ||= {value:'',style:{}}}};
const editMode='edit',editingName='alice';
const captureEditorRequest=()=>({});const isCurrentEditorRequest=()=>true;
const showToast=()=>{};const getMaskedFieldValue=()=>'';
const escapeHtml=x=>String(x).replaceAll('<','&lt;').replaceAll('>','&gt;');
let result={success:true,balance_futures:null,balance_unified:'99.842907',account_mode_label:'Unified Account',guidance:'Switch to Manual (Standard) <test>'};
const apiFetch=async()=>result;
document.getElementById('editExchange').value='hyperliquid';
(async()=>{
 await testConnection();let html=nodes.balanceDisplay.innerHTML;
 assert.ok(html.includes('Shared USDC Balance'));assert.ok(html.includes('99.842907'));
 assert.ok(html.includes('Unified Account'));assert.ok(html.includes('Manual (Standard)'));
 assert.ok(!html.includes('Futures Balance'));assert.ok(!html.includes('<test>'));
 result={success:true,balance_futures:100,account_mode_label:'Standard / Manual'};
 await testConnection();html=nodes.balanceDisplay.innerHTML;
 assert.ok(html.includes('Futures Balance'));assert.ok(!html.includes('Unified Account'));
})().catch(e=>{console.error(e);process.exit(1)});
""")


def test_mode_change_is_external_only():
    """Account guidance links to Hyperliquid without wallet signing or mutation routes."""
    from api import api_keys
    source = (ROOT / "frontend/api_keys_editor.html").read_text()
    assert 'href="https://app.hyperliquid.xyz/portfolio"' in source
    assert 'Account Type → Manual (Standard)' in source
    assert "switchStandardMode" not in source
    assert "browser_wallet.js" not in source
    assert "eth_signTypedData" not in source
    assert not any("standard-mode" in route.path for route in api_keys.router.routes)
