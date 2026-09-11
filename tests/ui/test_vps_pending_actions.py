"""Execute pending-action recovery with mocked browser controls and sockets."""

from pathlib import Path
import re
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[2]


def function(source, name):
    """Extract a top-level inline function using its closing indentation."""
    match = re.search(r'^( *)function ' + name + r'\(', source, re.M)
    assert match
    end = source.index('\n' + match[1] + '}', match.end())
    return source[match.start():end + len(match[1]) + 2]


def run(script):
    """Run isolated JavaScript assertions without browser or network access."""
    subprocess.run(['node', '-e', script], check=True, capture_output=True, text=True, timeout=10)


@pytest.mark.parametrize('action', ['kill-instance', 'restart-service'])
@pytest.mark.parametrize('finish', ['result', 'timeout', 'disconnect', 'send-failure'])
def test_monitor_pending_survives_redraw(action, finish):
    """Redrawn buttons stay pending and cannot submit duplicate commands."""
    source = (ROOT / 'frontend/vps_monitor.html').read_text()
    names = ['monitorActionKey', 'finishMonitorAction', 'restoreKillButton', 'restoreRestartButton',
             'resetPendingMonitorActions', 'handleResult', 'killInstance', 'restartService',
             'renderInstanceActions', 'renderServiceRestartButton']
    script = "const action = %r, finish = %r;\n" % (action, finish)
    script += "\n".join(function(source, name) for name in names)
    script += r'''
const assert = require('node:assert/strict');
const pendingMonitorActions = new Map();
let calls = 0, timers = new Map(), nextTimer = 0, buttons = [];
let cpuHistoryTimeout = null;
function setTimeout(fn) { const id = ++nextTimer; timers.set(id, fn); return id; }
function clearTimeout(id) { timers.delete(id); }
function send() { calls++; return finish !== 'send-failure'; }
function escAttr(s) { return String(s); }
const document = {querySelectorAll: () => buttons};
function button() {
 const classes = new Set();
 return {disabled:false, style:{}, classList:{contains:s=>classes.has(s), add:s=>classes.add(s), remove:s=>classes.delete(s)},
 getAttribute: key => ({'data-monitor-action':action, 'data-host':'host', 'data-bot':'bot', 'data-service':'svc'})[key]};
}
const old = button(); buttons = [old];
const start = btn => action === 'kill-instance' ? killInstance('host','bot','8',btn) : restartService('host','svc',btn);
start(old);
if (finish === 'send-failure') {
 assert.equal(pendingMonitorActions.size, 0); assert.equal(old.disabled, false);
} else {
 const html = action === 'kill-instance' ? renderInstanceActions({host:'host', name:'bot', pbVersion:'8'}) : renderServiceRestartButton('host','svc');
 assert.ok(html.includes('disabled'));
 assert.ok(html.includes(action === 'kill-instance' ? 'killing' : 'restarting'));
 const fresh = button(); buttons = [fresh];
 start(fresh); assert.equal(calls, 1);
 if (finish === 'timeout') Array.from(timers.values())[0]();
 else if (finish === 'disconnect') resetPendingMonitorActions('lost');
 else handleResult({cmd:action === 'kill-instance' ? 'kill_instance' : 'restart_service', host:'host', name:'bot', service:'svc', success:true});
 assert.equal(pendingMonitorActions.size, 0);
 assert.equal(fresh.disabled, false);
 start(fresh); assert.equal(calls, 2);
}
'''
    run(script)


def test_manager_disconnect_restores_all_pending_settings():
    """Disconnect clears in-flight hosts and removes queued reads without replay."""
    source = (ROOT / 'frontend/vps_manager.html').read_text()
    script = function(source, 'clearInterruptedSettingsReads') + function(source, 'updateReadSettingsProgress')
    script += r'''
const assert = require('node:assert/strict');
const store = {vps:{one:{readSettingsLoading:true}, two:{readSettingsLoading:false}}, pendingWsMessages:[{cmd:'read_vps_settings'}, {cmd:'refresh'}]};
function ensureVpsUi(host) { return store.vps[host]; }
clearInterruptedSettingsReads();
assert.equal(store.vps.one.readSettingsLoading, false);
assert.equal(store.vps.one.readSettingsProgress.status, 'error');
assert.ok(store.vps.one.readSettingsProgress.label.includes('Reconnect and retry'));
assert.equal(store.vps.two.readSettingsProgress, undefined);
assert.deepEqual(store.pendingWsMessages, [{cmd:'refresh'}]);
'''
    run(script)
    close = source.split('ws.onclose = function (event)', 1)[1].split('function scheduleReconnect', 1)[0]
    assert 'clearInterruptedSettingsReads();' in close
    assert 'forceView: true' in close
