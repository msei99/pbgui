"""Offline behavioral checks for independent Live activation across sidebar navigation."""

from pathlib import Path
import shutil
import subprocess

import pytest


def test_live_activation_survives_account_changes_and_reconciles_failures():
    """Defer saves and activation responses while selecting and activating other accounts."""
    node = shutil.which('node')
    if node is None:
        pytest.skip('Node is required')
    source = (Path(__file__).resolve().parents[2] / 'frontend/profit_sweep.html').read_text()
    action = 'async function enableLive()' + source.split('async function enableLive()', 1)[1].split('async function reconcileIntent', 1)[0]
    load = 'async function loadPolicyForAccount' + source.split('async function loadPolicyForAccount', 1)[1].split('async function loadIntentsForAccount', 1)[0]
    script = r'''
const assert = require('node:assert/strict');
const liveActivations = new Map(), livePolicyRevisions = new Map();
const state = {selectedUser:'alice',accountGeneration:1,record:record('dry'),users:[{name:'alice'},{name:'bob'}]};
function record(mode) {return {policy:{operating_mode:mode},generation:1,policy_fingerprint:'fp',live_state:{sweep_due:'2'}};}
const window = {PBGuiDialogs:{confirm:async()=>true},addEventListener(){},removeEventListener(){}};
function collectPolicy() {return {asset:'USDC'};}
function isCurrentAccount(name,gen) {return state.selectedUser===name && state.accountGeneration===gen;}
function setMessage() {} function setAccountActionsEnabled() {} function renderAccounts() {}
function renderSelectedAccount() {} function scheduleAutomaticPreview() {} function syncSelectedUserSummary() {}
const calls=[];
function requestJson(path,options) {return new Promise((resolve,reject)=>calls.push({path,options,resolve,reject}));}
const flush = () => new Promise(resolve=>setImmediate(resolve));
function select(name) {state.selectedUser=name;state.accountGeneration++;state.record=record('dry');}
__ACTION__
__LOAD__
(async()=>{
 const a=enableLive();await flush();
 assert.equal(calls[0].path,'/policies/alice');
 select('bob');const b=enableLive();await flush();
 assert.equal(calls[1].path,'/policies/bob');
 await enableLive();assert.equal(calls.length,2);
 assert(!calls[0].options.signal.aborted);
 calls[0].resolve(record('dry'));await flush();
 assert.equal(calls[2].path,'/live/alice');
 calls[1].resolve(record('dry'));await flush();
 assert.equal(calls[3].path,'/live/bob');
 calls[2].resolve({policy:record('live')});await a;
 assert.equal(state.users[0].operating_mode,'live');
 assert.equal(state.record.policy.operating_mode,'dry');
 assert(liveActivations.get('bob').pending);
 select('alice');
 calls[3].reject(Object.assign(new Error('preflight failed'),{status:409}));await flush();
 assert.equal(calls[4].path,'/policies/bob');
 calls[4].resolve(record('dry'));await b;
 assert.equal(state.users[1].operating_mode,'dry');
 assert.equal(liveActivations.get('bob').error,'preflight failed');
 // Returning to an account during activation: late old reads must not restore Dry.
 select('bob');const retry=enableLive();await flush();
 calls[5].resolve(record('dry'));await flush();
 const oldRead=loadPolicyForAccount('bob',state.accountGeneration,new AbortController().signal);
 calls[6].resolve({policy:record('live')});await retry;
 calls[7].resolve(record('dry'));await oldRead;
 assert.equal(state.record.policy.operating_mode,'live');
 assert.equal(state.users[1].operating_mode,'live');
 assert(!liveActivations.has('bob'));
})().catch(e=>{console.error(e);process.exit(1);});
'''.replace('__ACTION__', action).replace('__LOAD__', load)
    result = subprocess.run([node, '-e', script], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr
