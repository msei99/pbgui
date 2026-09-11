"""Execute real inline chat functions against a minimal DOM and failed requests."""

import json
from pathlib import Path
import subprocess

import pytest


@pytest.mark.parametrize("scenario", ["retry", "finished", "switched", "generation"])
def test_stop_failure_recovers_current_composer(scenario):
    """Failures restore current controls without modifying a newer chat view."""
    html = (Path(__file__).resolve().parents[2] / "frontend/ai_chat.html").read_text()
    stop = "async function stopCurrentTurn" + html.split("async function stopCurrentTurn", 1)[1].split("async function deleteCurrentConversation", 1)[0]
    composer = "function updateComposer" + html.split("function updateComposer", 1)[1].split("function updateEffortControl", 1)[0]
    script = r"""
const assert = require('node:assert/strict');
const state = {busy: true, transitioning: false, conversationId: 'one', chatGeneration: 1};
const nodes = {};
function $(id) { return nodes[id] ||= {value: 'selected', options: [1], disabled: false}; }
function updateEffortControl() {}
let notice = '';
function setNotice(text) { notice = text; }
let rejectRequest;
function api() { return new Promise((resolve, reject) => { rejectRequest = reject; }); }
async function loadConversation() { throw new Error('unexpected load'); }
""" + composer + stop + r"""
(async () => {
  updateComposer();
  const pending = stopCurrentTurn();
  assert.equal($('stop').disabled, true);
  if (scenario === 'finished') state.busy = false;
  if (scenario === 'switched') state.conversationId = 'two';
  if (scenario === 'generation') state.chatGeneration++;
  rejectRequest(new Error('cancel failed'));
  await pending;
  if (scenario === 'switched' || scenario === 'generation') {
    assert.equal(notice, 'Stopping current response...');
    assert.equal($('stop').disabled, true);
  } else {
    assert.equal(notice, 'cancel failed');
    assert.equal($('stop').disabled, !state.busy);
    assert.equal($('send').hidden, state.busy);
    assert.equal($('send').disabled, state.busy);
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
"""
    subprocess.run(["node", "-e", "const scenario = " + json.dumps(scenario) + ";\n" + script], check=True, capture_output=True, text=True, timeout=10)
