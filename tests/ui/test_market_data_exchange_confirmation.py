"""Exchange switching preserves unsaved Market Data settings until confirmed."""

from pathlib import Path
import subprocess

import pytest


@pytest.mark.parametrize('mode', ['clean', 'accept', 'cancel', 'same', 'stale'])
def test_exchange_switch_confirmation(mode):
    """Execute the real switch handler with deferred confirmation and isolated DOM."""
    source = (Path(__file__).resolve().parents[2] / 'frontend/market_data_main.html').read_text()
    start = source.index('      var contextExchangeRequestId = 0;')
    end = source.index('      function renderSettingsPayload()', start)
    script = 'const mode = %r;\n' % mode + source[start:end] + r'''
const assert = require('node:assert/strict');
const uiState = {contextExchange:'binance'}, settingsState = {isDirty:mode !== 'clean'};
const selector = {value:'bybit'}, loaded = [], stored = [], confirmations = [];
const document = {getElementById:id=>id === 'page-exchange' ? selector : {classList:{contains:()=>false}}};
const window = {localStorage:{setItem:(key,value)=>stored.push(value)}};
const contextExchangeKey = 'exchange';
function getExchangeMeta(key) {return {key};}
function showConfirmDialog(options) {assert.match(options.message,/unsaved/); return new Promise(resolve=>confirmations.push(resolve));}
function loadSettings(key) { loaded.push(key); settingsState.isDirty = false; }
function updateStatusPanel() {}
function updateSidebarLegacyShortcuts() {}
function syncInventorySubsectionVisibility() {}
(async()=>{
 const pending = setContextExchange(mode === 'same' ? 'binance' : 'bybit');
 if (mode === 'clean') {
  await pending; assert.equal(confirmations.length,0);
 } else if (mode === 'same') {
  await pending; assert.equal(confirmations.length,0);
 } else {
  assert.equal(uiState.contextExchange,'binance'); assert.equal(selector.value,'binance');
  assert.deepEqual(loaded,[]); assert.deepEqual(stored,[]); assert.equal(settingsState.isDirty,true);
  if (mode === 'stale') {
   const newer = setContextExchange('okx');
   confirmations[0](true); await pending;
   assert.deepEqual(loaded,[]);
   confirmations[1](true); await newer;
  } else { confirmations[0](mode === 'accept'); await pending; }
 }
 const expected = mode === 'stale' ? 'okx' : ['accept','clean'].includes(mode) ? 'bybit' : 'binance';
 assert.equal(uiState.contextExchange,expected); assert.equal(selector.value,expected);
 assert.deepEqual(loaded,expected === 'binance' ? [] : [expected]);
 assert.equal(settingsState.isDirty,expected === 'binance');
})().catch(error=>{console.error(error);process.exitCode=1;});
'''
    subprocess.run(['node', '-e', script], check=True, capture_output=True, text=True, timeout=10)


@pytest.mark.parametrize('stale_failure', [False, True])
def test_settings_responses_cannot_restore_previous_exchange(stale_failure):
    """Resolve exchange requests out of order and suppress stale data and errors."""
    source = (Path(__file__).resolve().parents[2] / 'frontend/market_data_main.html').read_text()
    start = source.index('      var settingsLoadRequestId = 0;')
    end = source.index('      function collectSettingsRequest()', start)
    script = 'const staleFailure = %s;\n' % str(stale_failure).lower() + source[start:end] + r'''
const assert = require('node:assert/strict');
const uiState = {contextExchange:'binance'}, settingsState = {}, pending = [], renders = [], errors = [];
const document = {getElementById:()=>({value:''})};
function fetchJson() {return new Promise((resolve,reject)=>pending.push({resolve,reject}));}
function renderSettingsPayload() {renders.push(settingsState.exchange);}
function setSettingsBaseline() {}
function showToast(error) {errors.push(error);}
(async()=>{
 const first = loadSettings('binance');
 uiState.contextExchange = 'bybit';
 const second = loadSettings('bybit');
 pending[1].resolve({exchange:'bybit',enabled_coins:['BTC']}); await second;
 if (staleFailure) pending[0].reject(new Error('old failure'));
 else pending[0].resolve({exchange:'binance',enabled_coins:['ETH']});
 await first;
 assert.equal(settingsState.exchange,'bybit');
 assert.deepEqual([...settingsState.selectedCoins],['BTC']);
 assert.deepEqual(renders,['bybit']); assert.deepEqual(errors,[]);
})().catch(error=>{console.error(error);process.exitCode=1;});
'''
    subprocess.run(['node','-e',script],check=True,capture_output=True,text=True,timeout=10)
