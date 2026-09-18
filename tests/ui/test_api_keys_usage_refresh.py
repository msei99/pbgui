"""Account usage refresh keeps newer list responses authoritative."""
import subprocess
from pathlib import Path


def test_usage_refresh_discards_stale_response():
    """A stale In Use response cannot hide Delete after a fresh usage check."""
    source = (Path(__file__).resolve().parents[2] / 'frontend/api_keys_editor.html').read_text()
    start = source.index('    async function loadUsers(')
    function = source[start:source.index('\n    function renderUserTable()', start)]
    script = r'''
const assert = require('assert');
let usersRequestGeneration = 0, currentUsers = [], rendered = [];
const nodes = new Map();
const document = {getElementById: id => {if (!nodes.has(id)) nodes.set(id, {style:{}}); return nodes.get(id);}};
const pending = [];
const apiFetch = (path, options) => {assert.equal(options.cache, 'no-store'); return new Promise(resolve => pending.push(resolve));};
const renderUserTable = () => rendered.push(currentUsers[0].in_use);
const loadApiMeta = () => {};
const escapeHtml = text => text;
''' + function + r'''
(async () => {
const old = loadUsers(true), fresh = loadUsers(true);
pending[1]([{name:'test-account',in_use:false}]); await fresh;
pending[0]([{name:'test-account',in_use:true}]); await old;
assert.deepEqual(rendered, [false]);
assert.equal(currentUsers[0].in_use, false);
})().catch(error => {console.error(error);process.exitCode=1;});
'''
    result = subprocess.run(['node', '-e', script], capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr
    assert 'title="Refresh account usage"' not in source
    assert 'list.style.display !== "none") loadUsers(true)' in source
