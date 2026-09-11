"""Execute bootstrap submission recovery with mocked requests and DOM controls."""

import json
from pathlib import Path
import subprocess

import pytest


@pytest.mark.parametrize("scenario", ["failure", "locked", "complete"])
def test_bootstrap_controls_recover_from_current_plan(scenario):
    """Failures allow retries while refreshed empty or locked plans stay disabled."""
    html = (Path(__file__).resolve().parents[2] / "frontend/cluster.html").read_text()
    apply = "function runBootstrapApply" + html.split("function runBootstrapApply", 1)[1].split("function nodeLabel", 1)[0]
    render = "function renderBootstrap" + html.split("function renderBootstrap", 1)[1].split("        if (!table) return;", 1)[0] + "}"
    script = r"""
const assert = require('node:assert/strict');
const btn = {disabled: false, textContent: 'Apply Bootstrap', style: {}};
const document = {getElementById: id => id === 'bootstrap-apply-btn' ? btn : null};
let lastBootstrapPlan = {generation: 1, counts: {add: 1}};
let clusterHasExistingState = false, bootstrapUnlocked = false;
let calls = 0, rejectRequest, resolveRequest;
function setMessage() {}
function loadCluster() { return Promise.resolve(); }
function fetchJson() { calls++; return new Promise((resolve, reject) => { rejectRequest = reject; resolveRequest = resolve; }); }
""" + render + apply + r"""
(async () => {
  const pending = runBootstrapApply(1);
  assert.equal(btn.disabled, true);
  runBootstrapApply(1);
  assert.equal(calls, 1);
  if (scenario === 'locked') clusterHasExistingState = true;
  if (scenario === 'complete') resolveRequest({after: {counts: {}}, result: {counts: {applied: 1}}});
  else rejectRequest(new Error('request failed'));
  await pending;
  assert.equal(btn.disabled, scenario !== 'failure');
  assert.equal(btn.textContent, scenario === 'locked' ? 'Bootstrap Locked' : 'Apply Bootstrap');
  if (scenario === 'failure') {
    const retry = runBootstrapApply(1);
    assert.equal(calls, 2);
    rejectRequest(new Error('still offline'));
    await retry;
    assert.equal(btn.disabled, false);
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
"""
    subprocess.run(["node", "-e", "const scenario = " + json.dumps(scenario) + ";\n" + script], check=True, capture_output=True, text=True, timeout=10)
