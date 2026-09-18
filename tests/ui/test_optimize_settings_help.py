"""Contextual Optimize help links preserve explicit topic anchors."""

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_settings_help_targets_local_or_vast_setup_without_changing_explicit_help():
    """Resolve Guide on every click so changing panels cannot leave a stale topic."""
    page = (ROOT / 'frontend/v7_optimize.html').read_text()
    start = page.index('  window._openOptimizeHelp = function(anchor) {')
    end = page.index('\n  };', start) + len('\n  };')
    script = '''
const assert = require('node:assert/strict');
const calls = [];
const window = {PBGuiSharedHelp:{open:(...args)=>calls.push(args)}};
const state = {panel:'settings'};
const optimizeEditorAdapter = {isV8:true};
''' + page[start:end] + '''
window._openOptimizeHelp();
assert.deepEqual(calls.pop(), ['43_pbv8_optimize', {anchor:'optimizer-settings'}]);
state.panel = 'vast';
window._openOptimizeHelp();
assert.deepEqual(calls.pop(), ['48_vast_gpu', {anchor:'optimizer-settings'}]);
window._openOptimizeHelp('scenario-generator');
assert.deepEqual(calls.pop(), ['43_pbv8_optimize', {anchor:'scenario-generator'}]);
state.panel = 'queue';
window._openOptimizeHelp();
assert.deepEqual(calls.pop(), ['43_pbv8_optimize', {anchor:''}]);
optimizeEditorAdapter.isV8 = false;
state.panel = 'settings';
window._openOptimizeHelp();
assert.deepEqual(calls.pop(), ['36_pbv7_optimize', {anchor:'optimizer-settings'}]);
state.panel = 'configs';
window._openOptimizeHelp();
assert.deepEqual(calls.pop(), ['36_pbv7_optimize', {anchor:''}]);
'''
    result = subprocess.run(['node'], input=script, text=True, capture_output=True, check=False)
    assert result.returncode == 0, result.stderr
    for language in ('help', 'help_de'):
        for topic in ('36_pbv7_optimize', '48_vast_gpu'):
            assert '## Optimizer Settings' in (ROOT / 'docs' / language / (topic + '.md')).read_text()
        guide = (ROOT / 'docs' / language / '48_vast_gpu.md').read_text()
        for permission in ('user_read', 'misc', 'instance_read', 'instance_write', 'billing_read'):
            assert f'| `{permission}` |' in guide
