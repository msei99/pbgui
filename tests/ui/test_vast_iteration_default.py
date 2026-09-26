"""New Vast defaults preserve existing configurations and deliberate edits."""

import subprocess
from pathlib import Path


def test_new_vast_iteration_default_preserves_explicit_values():
    """Exercise new-template marking and execution changes together."""
    root = Path(__file__).resolve().parents[2]
    page = (root / 'frontend/v7_optimize.html').read_text()
    cloud = (root / 'frontend/js/vast.js').read_text()
    new_config = page[page.index('async function openNewConfig() {'):page.index('\nasync function openConfigEditor')]
    update = cloud[cloud.index('    updateEditor: function () {'):cloud.index('    closeEditor:')]
    script = '''
const assert = require('node:assert/strict');
let fields, input, backendRow;
const state = {};
const optimizeEditorAdapter = {isV8:true};
const apiFetch = async () => ({});
const el = id => fields[id];
function openEditorWithConfig() {
  backendRow = {style:{}};
  fields = {'opted-execution':{value:'local'},
    'opted-opt-backend':{value:'pymoo',options:[{value:'gpu'}],disabled:false,dispatchEvent:()=>{},closest:()=>backendRow}, 'opted-iters':{
    value:'200000', dataset:{}, addEventListener:(name, fn)=>{input=fn;}
  }};
}
function scheduleValidation() {}
''' + new_config + '\nconst controller = {\n' + update + '};\n' + '''
(async () => {
  await openNewConfig();
  controller.updateEditor();
  assert.equal(fields['opted-iters'].value, '200000');
  assert.equal(backendRow.style.display, '');
  fields['opted-execution'].value = 'vast';
  controller.updateEditor();
  assert.equal(fields['opted-opt-backend'].value, 'gpu');
  assert.equal(fields['opted-opt-backend'].disabled, true);
  assert.equal(backendRow.style.display, 'none');
  assert.equal(fields['opted-iters'].value, '20000');
  fields['opted-iters'].value = '12345';
  controller.updateEditor();
  assert.equal(fields['opted-iters'].value, '12345');
  await openNewConfig();
  input(); // Even an explicit edit back to the original value is preserved.
  fields['opted-execution'].value = 'vast';
  controller.updateEditor();
  assert.equal(fields['opted-iters'].value, '200000');
  await openNewConfig();
  fields['opted-iters'].value = '9999'; // e.g. JSON editor changed the value
  fields['opted-execution'].value = 'vast';
  controller.updateEditor();
  assert.equal(fields['opted-iters'].value, '9999');
  openEditorWithConfig(); // Existing config: never marked as a new template.
  fields['opted-execution'].value = 'vast';
  controller.updateEditor();
  assert.equal(fields['opted-iters'].value, '200000');
})().catch(error => {console.error(error); process.exit(1);});
'''
    result = subprocess.run(['node'], input=script, text=True, capture_output=True)
    assert result.returncode == 0, result.stderr
