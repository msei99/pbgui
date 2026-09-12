"""Exercise initialization failure and saves racing initialization in the run editor."""

from pathlib import Path
import json
import subprocess

import pytest

SOURCE = (Path(__file__).resolve().parents[2] / 'frontend/v7_edit.html').read_text()


def run_node(script):
    """Run isolated real editor code without network or production data."""
    subprocess.run(['node', '-e', script], check=True, capture_output=True, text=True, timeout=10)


@pytest.mark.parametrize('payload', [None, {}, {'config': None}, {'config': {}}, {'config': []}, {'config': 'bad'}, 'bad', []])
def test_success_response_requires_nonempty_config(payload):
    """HTTP success cannot turn invalid data into an editable default configuration."""
    start = SOURCE.index('      var cfgResponse = await apiFetch(')
    end = SOURCE.index('      paramStatus = cfgResp.param_status', start)
    script = 'const payload = ' + json.dumps(payload) + ';\n'
    script += 'async function load() {\n' + SOURCE[start:end] + '\n}\n'
    script += '''
const assert = require('node:assert/strict');
let cfg = {original:true}; const INSTANCE_NAME = 'alice';
const apiFetch = async()=>({ok:true,json:async()=>payload});
(async()=>{await assert.rejects(load(), /invalid or empty/); assert.deepEqual(cfg,{original:true});})()
.catch(error=>{console.error(error);process.exitCode=1;});
'''
    run_node(script)


@pytest.mark.parametrize('is_new', ['false', 'true'])
def test_real_init_catch_sets_existing_instance_guard(is_new):
    """Execute init itself so string-valued IS_NEW cannot bypass the failure guard."""
    start = SOURCE.index('async function init() {')
    end = SOURCE.index('async function requestHostCapabilities()', start)
    script = 'const IS_NEW = ' + json.dumps(is_new) + ';\n' + SOURCE[start:end]
    script += '''
const assert = require('node:assert/strict');
let _configLoadFailed=false; const button={disabled:false}, messages=[];
const document={getElementById:()=>button};
const runEditorAdapter={configureUi:()=>{throw new Error('load unavailable');}};
function toast(message){messages.push(message);}
console.error=()=>{};
(async()=>{await init(); assert.equal(_configLoadFailed,IS_NEW !== 'true');
assert.equal(button.disabled,IS_NEW !== 'true'); assert.match(messages[0],/load unavailable/);})()
.catch(error=>{process.stderr.write(String(error));process.exitCode=1;});
'''
    run_node(script)


def test_early_save_rechecks_failed_init_and_stays_disabled():
    """A save already awaiting init must stop after init sets the failure flag."""
    start = SOURCE.index('async function saveConfig() {')
    end = SOURCE.index('/* ─── Copy To User', start)
    script = SOURCE[start:end] + '''
const assert = require('node:assert/strict');
let _configLoadFailed=false, release;
const _editorInitPromise=new Promise(resolve=>release=resolve);
const _symbolsAndTagsLoadPromise=Promise.resolve();
const button={disabled:false,innerHTML:''}, messages=[];
const document={getElementById:()=>button};
function toast(message){messages.push(message);}
function ensureRawJsonValidForSave(){throw new Error('Save must not reach validation or persistence');}
(async()=>{const saving=saveConfig(); assert.equal(button.disabled,true);
_configLoadFailed=true; release(); await saving;
assert.equal(button.disabled,true); assert.match(messages[0],/Config was not loaded/);
assert.equal(messages.length,1);})().catch(error=>{console.error(error);process.exitCode=1;});
'''
    run_node(script)


def test_invalid_json_success_is_rejected():
    """A successful response with an unreadable body fails closed."""
    start = SOURCE.index('      var cfgResponse = await apiFetch(')
    end = SOURCE.index('      paramStatus = cfgResp.param_status', start)
    script = 'async function load() {\n' + SOURCE[start:end] + '\n}\n'
    script += '''
const assert=require('node:assert/strict'); let cfg={original:true}; const INSTANCE_NAME='alice';
const apiFetch=async()=>({ok:true,json:async()=>{throw new SyntaxError('bad json');}});
(async()=>{await assert.rejects(load(),/invalid or empty/); assert.deepEqual(cfg,{original:true});})()
.catch(error=>{console.error(error);process.exitCode=1;});
'''
    run_node(script)
