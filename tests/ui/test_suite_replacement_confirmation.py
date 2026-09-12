"""Suite replacements must not silently discard the active scenario draft."""

from pathlib import Path
import re
import subprocess

import pytest


@pytest.mark.parametrize('action', ['template', 'reset', 'preview'])
@pytest.mark.parametrize('decision', ['cancel', 'accept', 'unavailable', 'closed', 'changed'])
def test_scenario_replacement_confirmation(action, decision):
    """Run real replacement functions with draft state and mocked shared dialogs."""
    source = (Path(__file__).resolve().parents[2] / 'frontend/js/suite_editor.js').read_text()
    names = ['_suiteConfirmScenarioReplacement', '_suiteApplyScenarioPreview', '_suiteApplyTemplate', '_suiteResetToBase']
    functions = []
    for name in names:
        match = re.search(r'^(?:async )?function ' + name + r'\(', source, re.M)
        assert match
        end = source.index('\n}', match.end())
        functions.append(source[match.start():end+2])
    script = "const action = %r, decision = %r;\n" % (action, decision)
    script += '\n'.join(functions)
    script += r'''
const assert = require('node:assert/strict');
const original = [{label:'original'}];
const draft = {value:'unsaved draft'};
const _suiteState = {scenarios:original, editIdx:decision === 'closed' ? -1 : 0,
 scenarioPreview:{training_scenarios:[{label:'generated'}]}, scenarioPreviewContext:'context'};
const _suiteTemplates = {test:{scenarios:[{label:'template'}], aggregate:{default:'mean'}}};
const document = {getElementById:()=>draft};
let confirms = 0, renders = 0, syncs = 0;
const window = decision === 'unavailable' ? {} : {PBGuiDialogs:{confirm:async options=>{
 confirms++; assert.match(options.detail, /Unsaved/);
 if (decision === 'changed') _suiteState.scenarios = [{label:'newer'}];
 return decision !== 'cancel';
}}};
function toast() {}
function _suiteScenarioContext() { return 'context'; }
function _suiteScenarioContextSignature(value) { return value; }
function _suiteRender() { renders++; }
function _suiteNotifyStructuredSync() { syncs++; }
(async()=>{
 if (action === 'template') await _suiteApplyTemplate('test');
 else if (action === 'reset') await _suiteResetToBase();
 else await _suiteApplyScenarioPreview();
 if (decision === 'accept' || decision === 'closed') {
  assert.equal(renders,1); assert.equal(syncs,1); assert.equal(_suiteState.editIdx,-1);
  assert.equal(_suiteState.scenarios[0].label, {template:'template',reset:'base',preview:'generated'}[action]);
 } else {
  assert.equal(renders,0); assert.equal(syncs,0); assert.equal(draft.value,'unsaved draft');
  assert.equal(_suiteState.scenarios[0].label,decision === 'changed' ? 'newer' : 'original');
 }
 assert.equal(confirms,decision === 'closed' || decision === 'unavailable' ? 0 : 1);
})().catch(error=>{console.error(error);process.exitCode=1;});
'''
    subprocess.run(['node','-e',script],check=True,capture_output=True,text=True,timeout=10)
