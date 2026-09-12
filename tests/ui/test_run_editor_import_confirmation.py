"""Run-editor imports must confirm before replacing unsaved form values."""

from pathlib import Path
import subprocess

import pytest


@pytest.mark.parametrize('runtime', ['7', '8'])
@pytest.mark.parametrize('decision', ['accept', 'cancel', 'unavailable', 'invalid'])
def test_import_preserves_draft_until_confirmed(runtime, decision):
    """Execute the shared PB7/PB8 import handler with isolated preparation and DOM."""
    source = (Path(__file__).resolve().parents[2] / 'frontend/v7_edit.html').read_text()
    start = source.index('async function doImport()')
    end = source.index('\n}', start)
    script = 'const runtime = %r, decision = %r;\n' % (runtime, decision)
    script += source[start:end+2]
    script += r'''
const assert = require('node:assert/strict');
const original = {draft:'unsaved'};
let cfg = original, paramStatus = {draft:true}, renders = 0, closed = 0, confirms = 0, failure = '';
const nodes = {'import-json':{value:'import text'}, 'import-error':{}, 'import-user':{value:'alice'}};
const document = {getElementById:id=>nodes[id]};
const allUsers = [{name:'alice'}], IS_NEW = 'false', INSTANCE_NAME = 'alice';
const runEditorAdapter = {isV8:runtime === '8'};
const window = {PBGuiEditorShared:{setInlineStatusError:(el, status)=>failure=status.message}};
if (decision !== 'unavailable') window.PBGuiDialogs = {confirm:async options=>{
 confirms++; assert.equal(cfg,original); assert.equal(renders,0); assert.equal(closed,0);
 assert.match(options.message,/unsaved/); return decision === 'accept';
}};
function validateJsonFieldTextarea() { return {parsed:{live:{}}}; }
function getInt() { return 5; }
async function prepareConfigForRunEditor(parsed) {
 if (decision === 'invalid') throw new Error('invalid config');
 return {config:parsed,param_status:{ready:true}};
}
async function fetchNextInstanceVersion() { return 6; }
function populateForm() { renders++; }
function queueSymbolsAndTagsLoad() {}
function closeImportModal() { closed++; }
function toast() {}
(async()=>{
 await doImport();
 if (decision === 'accept') {
  assert.notEqual(cfg,original); assert.equal(cfg.live.user,'alice');
  assert.equal(cfg.pbgui.version,runtime === '8' ? 5 : 6);
  assert.equal(renders,1); assert.equal(closed,1);
 } else {
  assert.equal(cfg,original); assert.deepEqual(paramStatus,{draft:true});
  assert.equal(renders,0); assert.equal(closed,0);
  assert.equal(nodes['import-json'].value,'import text');
 }
 assert.equal(confirms,decision === 'accept' || decision === 'cancel' ? 1 : 0);
 if (decision === 'unavailable') assert.match(failure,/Confirmation dialog unavailable/);
 if (decision === 'invalid') assert.equal(failure,'invalid config');
})().catch(error=>{console.error(error);process.exitCode=1;});
'''
    subprocess.run(['node','-e',script],check=True,capture_output=True,text=True,timeout=10)
