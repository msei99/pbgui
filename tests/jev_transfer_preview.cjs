/* Verify Jev transfer review sends exactly the previewed payload or discards it. */
const fs = require('fs');
const vm = require('vm');
const assert = require('assert');
const source = fs.readFileSync('frontend/js/jev_transfer_preview.js', 'utf8');
const window = {};
vm.runInNewContext(source, { window, JSON, String, Array, encodeURIComponent });
const helper = window.PBGuiJevTransferPreview;
const message = '```jev\n{"state":"x","questions":{"yes":{"type":"noul","instructions":"?"}},"sources":[{"name":"recent","tool":"list_backtests","args":{"version":"v8"}}]}\n```';
assert(helper.requestedSources(message));
const calls = [];
const payload = { model: 'jev', state: { input: 'x', pbgui: { recent: { total: 1 } } }, questions: {} };
const api = async (path, options) => {
  calls.push({ path, method: options.method });
  return options.method === 'POST' ? { preview_id: 'a'.repeat(32), payload } : {};
};
(async () => {
  const options = { api, conversationId: 'conversation', provider: 'openrouter', model: 'jev', message };
  const denied = await helper.review({ ...options, confirm: async info => {
    assert.strictEqual(info.detail, JSON.stringify(payload, null, 2));
    return false;
  } });
  assert.strictEqual(denied.cancelled, true);
  assert.deepStrictEqual(calls.map(x => x.method), ['POST', 'DELETE']);
  calls.length = 0;
  const allowed = await helper.review({ ...options, confirm: async () => true });
  assert.strictEqual(allowed.previewId, 'a'.repeat(32));
  assert.deepStrictEqual(calls.map(x => x.method), ['POST']);
})().catch(error => { console.error(error); process.exitCode = 1; });
