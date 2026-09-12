"""Backtest Add to Run must navigate only after successful draft creation."""

from pathlib import Path
import subprocess

import pytest


@pytest.mark.parametrize('flow', ['addConfigToRunByName', 'addToRun', 'addToRunFromArchive', 'addToRunFromLegacy'])
@pytest.mark.parametrize('response', ['success', 'http400', 'http500', 'html', 'missing', 'blank', 'network'])
def test_add_to_run_draft_response(flow, response):
    """Execute all four actual flows with mocked fetch and observe navigation/toasts."""
    source = (Path(__file__).resolve().parents[2] / 'frontend/v7_backtest.html').read_text()
    functions = []
    for name in [flow, 'apiFetchFrom', 'apiHttpError', 'requestHeaders']:
        start = source.index('function ' + name + '(')
        end = source.index('\n}', start)
        functions.append(source[start:end+2])
    script = 'const flow = %r, response = %r;\n' % (flow, response)
    script += '\n'.join(functions)
    script += r'''
const assert = require('node:assert/strict');
const API_BASE = '/api/backtest-v7';
const window = {location:{href:'backtest'}};
const notices = [];
function toast(message, kind) { notices.push([message,kind]); }
function esc(value) { return value; }
function getSelectedResults() { return ['result']; }
function getSelectedArchiveResults() { return ['archive']; }
function getSelectedLegacyResults() { return ['legacy']; }
function archiveResultByPath() { return {backtest_version:'v7'}; }
function apiFetch() { const cfg={live:{user:'alice'}}; return Promise.resolve(flow === 'addConfigToRunByName' ? {config:cfg} : cfg); }
function archiveResultApiFetch() { return apiFetch(); }
let draftCalls = 0;
async function fetch(url, opts) {
 if (!url.endsWith('/draft')) return {json:async()=>({instances:[],users:[{name:'alice'}]})};
 draftCalls++;
 assert.equal(opts.method,'POST'); assert.equal(opts.credentials,'same-origin');
 assert.equal(JSON.parse(opts.body).config.live.user,'alice');
 if (response === 'network') throw new Error('offline');
 const status = response === 'http400' ? 400 : response === 'http500' || response === 'html' ? 500 : 200;
 const payload = response === 'success' ? {draft_id:'draft/a'} : response === 'blank' ? {draft_id:' '} : response === 'missing' ? {} : {detail:'Draft rejected'};
 return {ok:status===200,status,statusText:'Server error',text:async()=>response==='html'?'upstream unavailable':JSON.stringify(payload)};
}
(async()=>{
 globalThis[flow];
 eval(flow + '("config")');
 await new Promise(resolve=>setImmediate(resolve));
 assert.equal(draftCalls,1);
 if (response === 'success') {
  assert.equal(window.location.href,'/api/v7/edit_page?new=1&draft_id=draft%2Fa');
  assert.equal(notices.length,0);
 } else {
  assert.equal(window.location.href,'backtest');
  assert.equal(notices.length,1); assert.equal(notices[0][1],'err');
  const expected = response.startsWith('http') ? 'Draft rejected' : response === 'html' ? 'upstream unavailable' : response === 'network' ? 'offline' : 'no draft ID';
  assert.ok(notices[0][0].includes(expected));
 }
})().catch(error=>{console.error(error);process.exitCode=1;});
'''
    subprocess.run(['node','-e',script],check=True,capture_output=True,text=True,timeout=10)
