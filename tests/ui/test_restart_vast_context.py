"""Restart confirmation distinguishes local Vast controllers from GPU rentals."""

from pathlib import Path
import subprocess


NAV = Path(__file__).resolve().parents[2] / "frontend" / "pbgui_nav.js"


def test_vast_restart_context_reports_local_activity_without_hiding_stale_service() -> None:
    """Idle, queued, active and unknown states receive accurate restart context."""
    source = NAV.read_text(encoding="utf-8")
    assert "visibleRestartServices(_restartStatus, restartServices)" in source
    assert "visibleRestartServices(state, services)" in source
    helper = source[source.index("  function visibleRestartServices("):source.index("  function updateRestartButtonState(")]
    helper += source[source.index("    function vastRestartContext("):source.index("    /* Restart button */")]
    script = "const assert = require('node:assert/strict');\n" + helper + r"""
const services = [{service: 'VastPool', label: 'Vast GPU Pool'}];
assert.deepEqual(visibleRestartServices({vast_activity: {state: 'idle'}}, services), []);
assert.equal(visibleRestartServices({vast_activity: {state: 'active'}}, services).length, 1);
assert.equal(vastRestartContext({vast_activity: {state: 'idle'}}, []), '');
assert.match(vastRestartContext({vast_activity: {state: 'idle'}}, services), /no active GPU rentals or queued cloud jobs/);
assert.match(vastRestartContext({vast_activity: {state: 'queued'}}, services), /remain queued/);
assert.match(vastRestartContext({vast_activity: {state: 'active'}}, services), /continue on their remote hosts/);
assert.match(vastRestartContext({vast_activity: {state: 'unknown'}}, services), /could not be verified/);
assert.match(vastRestartContext({}, services), /not a GPU rental/);
"""
    result = subprocess.run(["node", "-e", script], capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr
