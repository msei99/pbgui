"""VPS Manager offers request-credit purchases only to depleted main accounts."""

from pathlib import Path
import subprocess


PAGE = Path(__file__).resolve().parents[2] / "frontend" / "vps_manager.html"


def test_hl_credit_purchase_visibility_and_guard() -> None:
    """History rendering and purchase action reject spare quota and vault accounts."""
    source = PAGE.read_text(encoding="utf-8")
    helpers = source[source.index("    function hlCreditLimitReached("):source.index("    function formatHlRequestCount(")]
    renderer = source[source.index("    function renderMetricHistoryPayload("):source.index("    function renderHlRateLimitHistoryMeta(")]
    purchase = source[source.index("    async function buyHlRequestCredits("):source.index("    document.getElementById('hlCreditAmount').addEventListener", source.index("    async function buyHlRequestCredits("))]
    script = r"""
const assert = require('node:assert/strict');
const nodes = new Map();
const document = {getElementById(id) {
  if (!nodes.has(id)) nodes.set(id, {hidden: true, value: '', textContent: '', innerHTML: '', disabled: false});
  return nodes.get(id);
}};
const element = id => document.getElementById(id);
let cpuHistoryLastPayload = null, hlCreditPurchasePending = false, posts = 0;
const metricHistorySeriesPoints = () => [];
const metricHistoryTitle = () => 'History';
const metricHistorySubtitle = () => '24h';
const renderHlRateLimitHistoryMeta = () => '';
const renderCpuHistoryMeta = () => '';
const renderHlRateLimitHistoryChart = () => '';
const renderCpuHistoryChart = () => '';
const window = {PBGuiDialogs: {confirm: async () => true}};
const API_BASE = '/api/vps-manager';
const fetch = async () => {posts++; return {ok: true, json: async () => ({})};};
const formatHlRequestCount = value => String(value);
const updateHlCreditCost = () => {
  const valid = element('hlCreditAmount').value === '6000';
  element('hlCreditBuyButton').disabled = !valid || element('hlCreditPurchasePanel').hidden;
  return valid ? 6000 : null;
};
""" + helpers + renderer + purchase + r"""
(async () => {
  const sample = (used, cap) => ({samples: [{used, cap, sampled_at: 100}]});
  element('hlCreditAmount').value = '6000';
  renderMetricHistoryPayload({...sample(1000, 10000), is_vault: false}, 'host', 'main', 'hl_requests');
  assert.equal(element('hlCreditPurchasePanel').hidden, true);
  assert.equal(element('hlCreditBuyButton').disabled, true);
  await buyHlRequestCredits();
  assert.equal(posts, 0);

  renderMetricHistoryPayload({...sample(10000, 10000), is_vault: true}, 'host', 'vault', 'hl_requests');
  assert.equal(element('hlCreditPurchasePanel').hidden, true);
  assert.equal(element('hlCreditVaultNote').hidden, false);
  await buyHlRequestCredits();
  assert.equal(posts, 0);

  renderMetricHistoryPayload(sample(10000, 10000), 'host', 'unknown', 'hl_requests');
  assert.equal(element('hlCreditPurchasePanel').hidden, true);

  renderMetricHistoryPayload({...sample(10000, 10000), is_vault: false}, 'host', 'main', 'hl_requests');
  assert.equal(element('hlCreditPurchasePanel').hidden, false);
  assert.equal(element('hlCreditBuyButton').disabled, false);
  assert.equal(element('hlCreditVaultNote').hidden, true);

  renderMetricHistoryPayload({...sample(1000, 10000), is_vault: false}, 'host', 'main', 'cpu');
  assert.equal(element('hlCreditPurchasePanel').hidden, true);
})().catch(error => {console.error(error); process.exitCode = 1;});
"""
    result = subprocess.run(["node", "-e", script], capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr
