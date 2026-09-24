"""Hyperliquid account-limit display and purchase guards in the API-key editor."""

from pathlib import Path
import subprocess


PAGE = Path(__file__).resolve().parents[2] / "frontend" / "api_keys_editor.html"


def test_hl_limit_uses_apostrophes_and_only_offers_confirmed_main_account_purchase() -> None:
    """Formatting, vault gating, confirmation, and a single POST work without exchange calls."""
    source = PAGE.read_text(encoding="utf-8")
    assert '<div id="accountBalanceRow" class="balance-display">' in source
    assert '<div id="hlRateLimitInline" class="balance-box"' in source
    start = source.index("    function formatHlRequestCount(")
    end = source.index("    function showCreateForm()", start)
    functions = source[start:end]
    script = r"""
const assert = require('node:assert/strict');
const nodes = new Map();
const document = { getElementById(id) {
    if (!nodes.has(id)) nodes.set(id, {hidden: true, value: '', textContent: '', disabled: false, dataset: {}});
    return nodes.get(id);
}};
const element = id => document.getElementById(id);
element('editExchange').value = 'hyperliquid';
element('editExchange').dataset.originalExchange = 'hyperliquid';
let editMode = 'edit', editingName = 'main', editorGeneration = 1;
let hlLimitLastSample = null, hlLimitSavedIsVault = false;
let hlCreditPurchasePending = false, hlCreditPurchaseLocked = false;
const captureEditorRequest = name => ({name, generation: editorGeneration});
const isCurrentEditorRequest = context => context.name === editingName && context.generation === editorGeneration;
let confirmed = false, confirmations = [], posts = [];
const window = { PBGuiDialogs: { confirm: async options => { confirmations.push(options); return confirmed; } } };
const API_BASE = '/api/api-keys';
const fetch = async (url, options) => {
    posts.push({url, options});
    return {ok: true, json: async () => ({verified: true, used: 105, cap: 6100, sampled_at: 100, cost_usdc: '3.0000'})};
};
""" + functions + r"""
(async () => {
    assert.equal(formatHlRequestCount(2209548), "2'209'548");
    renderHlRateLimitForEditor({used: 109510, cap: 2209548, remaining: 2100038, sampled_at: 100});
    assert.equal(element('hlRateLimitInlineValue').textContent, "Used 109'510 / Cap 2'209'548");
    assert.match(element('hlRateLimitInlineMeta').textContent, /Remaining 2'100'038/);
    assert.equal(element('hlCreditPurchasePanel').hidden, true);

    renderHlRateLimitForEditor({used: 105, cap: 100, remaining: 0, sampled_at: 100});
    assert.equal(element('hlCreditPurchasePanel').hidden, false);
    assert.equal(element('hlCreditPurchaseForm').hidden, false);
    hlLimitSavedIsVault = true;
    refreshHlCreditOffer();
    assert.equal(element('hlCreditPurchaseForm').hidden, true);
    assert.equal(element('hlCreditVaultNote').hidden, false);
    element('hlCreditAmount').value = '6000';
    await buyHlRequestCredits();
    assert.equal(posts.length, 0);

    hlLimitSavedIsVault = false;
    refreshHlCreditOffer();
    updateHlCreditCost();
    assert.equal(element('hlCreditCost').textContent, 'Cost: 3.0000 USDC from Perps balance');
    assert.equal(element('hlCreditBuyButton').disabled, false);
    await buyHlRequestCredits();
    assert.equal(posts.length, 0);
    assert.equal(confirmations.length, 1);

    confirmed = true;
    element('hlCreditPurchasePanel').hidden = true;
    await buyHlRequestCredits();
    assert.equal(posts.length, 0);
    refreshHlCreditOffer();
    window.PBGuiDialogs.confirm = async () => { editorGeneration += 1; return true; };
    await buyHlRequestCredits();
    assert.equal(posts.length, 0);
    editorGeneration -= 1;
    window.PBGuiDialogs.confirm = async () => true;
    await buyHlRequestCredits();
    assert.equal(posts.length, 1);
    assert.equal(posts[0].url, '/api/vps-manager/user-rate-limits/credits');
    assert.deepEqual(JSON.parse(posts[0].options.body), {user_name: 'main', credits: 6000});
    assert.equal(element('hlCreditPurchasePanel').hidden, true);
    assert.equal(element('hlRateLimitInlineValue').textContent, "Used 105 / Cap 6'100");
    await buyHlRequestCredits();
    assert.equal(posts.length, 1);
})().catch(error => {console.error(error); process.exitCode = 1;});
"""
    result = subprocess.run(["node", "-e", script], capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr


def test_hl_limit_overview_and_vps_manager_use_apostrophe_counters() -> None:
    """The two other request-limit views render large counts without periods."""
    root = PAGE.parents[1]
    overview = (root / "frontend" / "hl_limits.html").read_text(encoding="utf-8")
    vps_manager = (root / "frontend" / "vps_manager.html").read_text(encoding="utf-8")
    overview_formatter = next(line for line in overview.splitlines() if "const formatRequestCount =" in line)
    start = vps_manager.index("    function formatHlRequestCount(")
    end = vps_manager.index("    function updateHlCreditCost()", start)
    vps_formatter = vps_manager[start:end]
    script = "\n".join([
        "const assert = require('node:assert/strict');",
        overview_formatter,
        vps_formatter,
        "assert.equal(formatRequestCount(2209548), \"2'209'548\");",
        "assert.equal(formatHlRequestCount(109510), \"109'510\");",
    ])
    result = subprocess.run(["node", "-e", script], capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr
    assert "nf.format(" not in overview
    assert "used.toLocaleString()" not in vps_manager[vps_manager.index("function renderHlRateLimitHistoryMeta"):vps_manager.index("function renderHlRateLimitHistoryChart")]
    assert "cap.toLocaleString()" not in vps_manager[vps_manager.index("function _hlLimitCell"):vps_manager.index("function _cpuTag")]
