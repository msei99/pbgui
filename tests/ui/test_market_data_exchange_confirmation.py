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
    from tests.ui.test_market_data_tiingo_frontend import _run_settings_js
    _run_settings_js(["loadSettings", "stopSettingsSync", "settingsSyncVisible", "scheduleSettingsSync", "resumeSettingsSync", "settingsSyncMessage", "restoreSettingsCoinFilter", "applySettingsDynamicPayload"], r"""
      let pending = [];
      global.fetchSettingsJson = () => {const item=deferred();pending.push(item);return item.promise;};
      uiState.contextExchange='binance';
      const first=loadSettings('binance');
      uiState.contextExchange='bybit';
      const second=loadSettings('bybit');
      pending[1].resolve({...payload('bybit'),enabled_coins:['BTC']});await second;
      if(STALE_FAILURE)pending[0].reject(new Error('old failure'));
      else pending[0].resolve({...payload('binance'),enabled_coins:['ETH']});
      await first;
      assert.equal(settingsState.exchange,'bybit');
      assert.deepEqual([...settingsState.selectedCoins],['BTC']);
      assert.equal(renders,1);assert.deepEqual(toasts,[]);
      assert.equal(document.getElementById('settings-sync-error').textContent,'');
    """.replace('STALE_FAILURE',str(stale_failure).lower()))


def test_settings_save_deduplicates_requests_and_keeps_edits_made_during_save():
    """A second Save cannot POST while the first is pending or erase newer input."""
    from tests.ui.test_market_data_tiingo_frontend import _run_settings_js
    _run_settings_js(["saveSettings", "settingsRequestsEqual", "updateSaveSettingsButton", "setSettingsBaseline", "syncSettingsDirtyState", "stopSettingsSync", "scheduleSettingsSync", "settingsSyncVisible", "applySettingsDynamicPayload"], r"""
      let current='BTC',pending=[];
      collectSettingsRequest=()=>({enabled_coins:[current]});
      global.fetchSettingsJson=(path,options)=>{const item=deferred();pending.push(item);requests.push({path,options});return item.promise;};
      settingsState.payload=payload();settingsState.isDirty=true;
      const first=saveSettings();
      assert.equal(document.getElementById('btn-save-settings-sidebar').disabled,true);
      await saveSettings();assert.equal(requests.length,1);
      assert.deepEqual(JSON.parse(requests[0].options.body),{enabled_coins:['BTC']});
      current='ETH';settingsEditRevision++;
      pending[0].resolve({success:true,settings:payload()});await first;
      assert.equal(renders,0); // unchanged availability must not repaint the draft
      assert.equal(current,'ETH');assert.equal(settingsState.isDirty,true);
      const second=saveSettings();assert.equal(requests.length,2);
      pending[1].resolve({success:true,settings:{...payload(),enabled_coins:['ETH']}});await second;
      assert.equal(settingsState.isDirty,false);assert.equal(settingsState.saveBusy,false);
      assert.equal(toasts.length,2);
    """)
