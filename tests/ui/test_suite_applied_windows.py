"""Save guard catches stale applied windows without changing workload estimates."""
from pathlib import Path
import subprocess


def test_saved_training_must_match_visual_windows_and_current_dates():
    """The 2026 draft cannot silently queue old 250-day training scenarios."""
    source = Path('frontend/js/suite_editor.js').read_text()
    source = source[source.index('function suiteValidateAppliedWindows('):]
    code = r'''
const assert=require('assert');
const context={start_date:'2026-01-01',end_date:'2026-09-18'};
const suite={suite_enabled:true,scenario_generator:{windows:[
 {role:'training',start_date:'2026-01-06',end_date:'2026-02-06'},
 {role:'holdout',start_date:'2026-02-07',end_date:'2026-03-10'}]},
 scenarios:[{start_date:'2020-07-16',end_date:'2021-03-22'}]};
assert.throws(()=>suiteValidateAppliedWindows(suite,context),/Check & Apply windows/);
suite.scenarios=[{label:'custom',start_date:'2026-01-06',end_date:'2026-02-06'}];
suite.scenario_template={holdout_scenarios:[{start_date:'2026-02-07',end_date:'2026-03-10'}]};
assert.doesNotThrow(()=>suiteValidateAppliedWindows(suite,context));
assert.throws(()=>suiteValidateAppliedWindows(suite,{...context,start_date:'2026-02-01'}),/outside/);
suite.suite_enabled=false;assert.doesNotThrow(()=>suiteValidateAppliedWindows(suite,context));
'''
    result = subprocess.run(['node', '-e', source + code], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr
