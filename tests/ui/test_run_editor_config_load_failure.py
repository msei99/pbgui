"""Run-editor must surface failed instance config loads and block saving empty configs."""

from pathlib import Path
import subprocess

import pytest

SOURCE = (Path(__file__).resolve().parents[2] / 'frontend/v7_edit.html').read_text()


def _extract(start_marker: str, end_marker: str) -> str:
    start = SOURCE.index(start_marker)
    end = SOURCE.index(end_marker, start)
    return SOURCE[start:end]


@pytest.mark.parametrize('failure', ['json_detail', 'non_json', 'ok'])
def test_config_load_failure_is_reported_and_blocks_blank_form(failure):
    """A failed /config load must throw with the server detail instead of rendering an empty form."""
    branch = _extract(
        'var cfgResponse = await apiFetch(',
        '// User change → reload symbols/tags',
    )
    script = 'const failure = %r;\n' % failure
    script += 'async function loadExistingConfig() {\n' + branch.rstrip() + '\n'
    script += r'''
const assert = require('node:assert/strict');
let cfg = {pbgui: {kept: true}}, paramStatus = {kept: true}, draftOverrideConfigs = {kept: true};
let renders = 0, throws = '';
const document = {getElementById: () => null};
const INSTANCE_NAME = 'alice', API_BASE = '', runEditorAdapter = {isV8: false};
let allHosts = [], _fromBacktestConfig = '';
function populateHosts() {}
function coinOvInit() {}
function coinOvSetConfigName() {}
function populateForm() { renders++; }
function coinOvSetOverrideConfigs() {}
function queueSymbolsAndTagsLoad() {}
const apiFetch = async () => {
  if (failure === 'ok') {
    return {ok: true, status: 200, json: async () => ({config: {live: {user: 'alice'}}, param_status: {ready: true}})};
  }
  if (failure === 'json_detail') {
    return {ok: false, status: 500, json: async () => ({detail: 'config file unreadable'})};
  }
  return {ok: false, status: 404, json: async () => { throw new Error('not json'); }};
};
(async () => {
  if (failure === 'ok') {
    await loadExistingConfig();
    assert.equal(cfg.live.user, 'alice');
    assert.equal(paramStatus.ready, true);
    assert.equal(renders, 1);
  } else {
    await assert.rejects(loadExistingConfig(), error => {
      return failure === 'json_detail'
        ? error.message === 'config file unreadable'
        : error.message === 'Could not load config: HTTP 404';
    });
    assert.deepEqual(cfg, {pbgui: {kept: true}});
    assert.deepEqual(paramStatus, {kept: true});
    assert.equal(renders, 0);
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
'''
    subprocess.run(['node', '-e', script], check=True, capture_output=True, text=True, timeout=10)


@pytest.mark.parametrize('blocked', [True, False])
def test_save_blocked_after_failed_config_load(blocked):
    """saveConfig must refuse to save when the instance config never loaded."""
    fn = _extract('async function saveConfig() {', '/* ─── Copy To User')
    script = 'const blocked = %s;\n' % ('true' if blocked else 'false')
    script += fn
    script += r'''
const assert = require('node:assert/strict');
let _configLoadFailed = blocked;
let _editorInitPromise = null, _symbolsAndTagsLoadPromise = Promise.resolve();
let toasts = [], rawChecked = 0, btnDisabled = null;
const document = {getElementById: () => ({set disabled(value) { btnDisabled = value; }, get disabled() { return btnDisabled; }, set innerHTML(value) {}, get innerHTML() { return ''; }})};
function toast(message) { toasts.push(message); }
function ensureRawJsonValidForSave() { rawChecked++; return false; }
(async () => {
  await saveConfig();
  if (blocked) {
    assert.match(toasts[0], /Config was not loaded/);
    assert.equal(rawChecked, 0);
  } else {
    assert.equal(rawChecked, 1);
    assert.equal(toasts.length, 0);
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
'''
    subprocess.run(['node', '-e', script], check=True, capture_output=True, text=True, timeout=10)
