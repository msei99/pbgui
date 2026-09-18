"""Offline regressions for the open High-priority UI issues."""

import json

import pytest

from test_remaining_issue_regressions import ROOT, execute, function


def test_stop_before_conversation_creation_invalidates_pending_send():
    """A late conversation ID cannot send the prompt after local Stop (#297)."""
    execute(function('ai_chat.html', 'selectedProvider') + function('ai_chat.html', 'sendMessage') + function('ai_chat.html', 'stopCurrentTurn') + r'''
const assert=require('node:assert/strict');
const nodes={prompt:{value:'hello'},'provider-select':{value:'test',selectedOptions:[{textContent:'Test'}]},'model-select':{value:'model'},'effort-select':{value:''},stop:{},'retry-turn':{}};
const $=id=>nodes[id]; const state={conversationId:'',chatGeneration:1,retryMessages:{},busy:false};
let finish, turns=0, composed=0;
const api=path=>{if(path==='/conversations')return new Promise(resolve=>finish=resolve);turns++;};
const updateComposer=()=>composed++;const setNotice=()=>{};const stopActivityPolling=()=>{};
const loadConversationSummary=async()=>{};const renderPendingMessage=()=>{};
(async()=>{const sending=sendMessage();await stopCurrentTurn();assert.equal(state.busy,false);
finish({conversation_id:'late'});await sending;assert.equal(turns,0);assert.equal(state.conversationId,'');assert.equal(state.pendingMessage,'');assert(composed>=2);
})().catch(e=>{console.error(e);process.exitCode=1;});
''')


@pytest.mark.parametrize('payload,allowed', [
    ({'override_configs': {'TOKEN': {'bot': {'long': {'value': 1}}}}}, True),
    ({'override_configs': {'TOKEN': {'api_key': 'blocked'}}}, False),
    ({'config': {'token': 'blocked'}}, False),
    ({'override_configs': {'TOKEN': {'nested': {'session_token': 'blocked'}}}}, False),
    ({'override_configs': {'TOKEN': 'blocked'}}, False),
])
def test_draft_symbol_keys_do_not_hide_nested_credentials(payload, allowed):
    """Allow TOKEN override objects while rejecting credential values (#344)."""
    execute(function('v7_optimize.html', 'storeOptimizeDraft') +
            'const payload=' + json.dumps(payload) + '; const allowed=' + json.dumps(allowed) + ';' + r'''
const assert=require('node:assert/strict');let stored=null;
const window={sessionStorage:{setItem:(_,v)=>stored=v,removeItem:()=>stored=null}};
const optimizeDraftStorageKey=()=> 'draft';const setPageEditorStatus=()=>{};
assert.equal(storeOptimizeDraft(payload),allowed);assert.equal(stored!==null,allowed);
''')


def test_account_switch_releases_old_action_without_unlocking_new_action():
    """Existing account-generation reset already protects cancellation (#346)."""
    execute(function('profit_sweep.html', 'selectAccount') + function('profit_sweep.html', 'cancelPreparedTest') + r'''
const assert=require('node:assert/strict');
const state={selectedUser:'old',accountGeneration:1,users:[{name:'old'},{name:'new'}],testActionPending:false};
const isCurrentAccount=(name,generation)=>state.selectedUser===name&&state.accountGeneration===generation;
const stopAutomaticPreview=()=>{};const overview={hide(){},show(){}};
const renderAccounts=()=>{};const renderAccount=()=>{};const setMessage=()=>{};
const loadAccountData=async()=>{};const loadAccount=async()=>{};const scheduleAutomaticPreview=()=>{};
const renderAccountLoading=()=>{};const renderAccountLoadError=()=>{};
const loadPolicyForAccount=async()=>{},loadJournalForAccount=async()=>{},loadIntentsForAccount=async()=>{},loadTestTransfersForAccount=async()=>{};
const renderSelectedAccount=()=>{};
const renderTestTransfers=()=>{};const refreshTestTransfersAfterAction=async()=>{};
let finish;const requestJson=()=>new Promise(resolve=>finish=resolve);
(async()=>{
const pending=cancelPreparedTest({operation_id:'old-op',can_cancel:true});assert.equal(state.testActionPending,true);
await selectAccount('new');assert.equal(state.selectedUser,'new');assert.equal(state.testActionPending,false);
state.testActionPending=true;finish({});await pending;assert.equal(state.testActionPending,true);
})().catch(e=>{console.error(e);process.exitCode=1;});
''')


def test_scenario_apply_uses_latest_base_context_and_preserves_windows():
    """Applying after date/exchange edits validates the retained draft (#347)."""
    execute(function('js/suite_editor.js', '_suiteMountVisual') + r'''
const assert=require('node:assert/strict');let mounted,payload;
let context={start_date:'2024-01-01',end_date:'2024-12-31',exchanges:['binance']};
const _suiteState={apiBase:'/api/test',aggregate:{},scenarioRequestId:0,scenarios:[]};
const _suiteScenarioContext=()=>structuredClone(context),_suiteScenarioContextSignature=JSON.stringify;
const windows=[{id:'keep',start_date:'2024-04-01',end_date:'2024-05-01'}];
const _suiteVisualWindows=()=>windows,_suiteCaptureScenarioGeneratorDraft=()=>({windows});
const window={PBGuiScenarioVisual:{mount:(_,opts)=>mounted=opts}};
const document={querySelector:()=>null};
const apiFetch=async(_,opts)=>{payload=JSON.parse(opts.body);return {training_scenarios:[{label:'retained'}]};};
const _suiteApplyScenarioPreview=async()=>{_suiteState.scenarios=_suiteState.scenarioPreview.training_scenarios;_suiteState.scenarioTemplate={};};
(async()=>{_suiteMountVisual({});context.end_date='2025-01-01';context.exchanges=['bybit'];await mounted.apply(windows);
assert.equal(payload.end_date,context.end_date);assert.deepEqual(payload.exchanges,['bybit']);assert.deepEqual(payload.windows,windows);
})().catch(e=>{console.error(e);process.exitCode=1;});
''')


@pytest.mark.parametrize('failure', [False, True])
def test_delete_by_date_single_flight_and_visible_failure(failure):
    """Confirmation and deletion are locked, and errors dismiss the covering modal (#355)."""
    execute(function('market_data_main.html', 'runInventoryDeleteOlder') + 'const failure=' + json.dumps(failure) + ';' + r'''
const assert=require('node:assert/strict');const inventoryState={};const uiState={contextExchange:'binance'};
const viewState={olderCutoffDay:'2024-01-01',payload:{},selectedRowIds:['BTC']};
const getCurrentInventoryViewState=()=>viewState,getResolvedInventoryView=()=> 'ohlcv';
const getInventoryOlderScopeCoins=()=>['BTC'],getInventoryCoinDisplayNames=x=>x,getExchangeMeta=()=>({});
let confirm,complete,requests=0,closed=0;const button={};
const document={getElementById:()=>button};const showConfirmDialog=()=>new Promise(resolve=>confirm=resolve);
const showToast=()=>{};const setInventoryBox=()=>{};const closeInventoryDeleteOlderDialog=()=>closed++;
const renderInventoryOlderPreview=()=>{button.disabled=false;};const loadInventoryPanel=async()=>{};
const fetchJson=()=>{requests++;return new Promise((resolve,reject)=>complete=()=>failure?reject(Error('failed')):resolve({success:true}));};
(async()=>{const pending=runInventoryDeleteOlder();await runInventoryDeleteOlder();assert.equal(button.disabled,true);
confirm(true);await new Promise(setImmediate);await runInventoryDeleteOlder();assert.equal(requests,1);
complete();await pending;assert.equal(inventoryState.deleteOlderPending,false);assert.equal(button.disabled,false);assert.equal(closed,1);
})().catch(e=>{console.error(e);process.exitCode=1;});
''')


@pytest.mark.parametrize('failure', [False, True])
def test_status_actions_lock_and_handle_html_errors(failure):
    """Running refreshes cannot be requeued; proxy errors release the action lock (#354)."""
    execute(function('market_data_status.html', 'callAPI') + 'const failure=' + json.dumps(failure) + ';' + r'''
const assert=require('node:assert/strict');let actionPending=false,destroyed=false;
const currentStatus={running:true,queued:false},API_BASE='/api';let finish,calls=0,notices=[];
const updateUI=()=>{};const showToast=msg=>notices.push(msg);
const fetch=()=>{calls++;return new Promise(resolve=>finish=resolve);};
(async()=>{await callAPI('/market-data/refresh-now',{},'ok');assert.equal(calls,0);
currentStatus.running=false;const pending=callAPI('/market-data/refresh-now',{},'ok');await callAPI('/market-data/refresh-now',{},'ok');assert.equal(calls,1);assert.equal(actionPending,true);
finish({ok:!failure,status:failure?502:200,json:async()=>{if(failure)throw Error('html');return {success:true};}});
await pending;assert.equal(actionPending,false);assert.equal(currentStatus.queued,!failure);if(failure)assert.match(notices[0],/HTTP 502/);
})().catch(e=>{console.error(e);process.exitCode=1;});
''')


def test_visual_context_refresh_in_browser_preserves_drawn_windows():
    """Date/exchange edits remount the real chart with the same explicit windows (#347)."""
    playwright = pytest.importorskip('playwright.sync_api')
    with playwright.sync_playwright() as runner:
        browser = runner.chromium.launch(headless=True)
        try:
            page = browser.new_page()
            page.route('**/*', lambda route: route.fulfill(json={'sources': []}))
            page.goto('http://scenario.test/')
            page.set_content('<div id="suite"></div>')
            page.add_script_tag(path=str(ROOT / 'frontend/js/scenario_visual_editor.js'))
            page.add_script_tag(path=str(ROOT / 'frontend/js/suite_editor.js'))
            page.evaluate('''() => {
              window.esc = s => String(s); window.toast = () => {};
              window.apiFetch = async () => ({params: []});
              window.scheduleStructuredEditorSync = () => {};
              window.context = {start_date:'2024-01-01',end_date:'2024-12-31',exchanges:['binance']};
              suiteInit('suite', {version:'v8',scenarioGenerator:true,getScenarioContext:()=>context});
              suiteLoad({backtest:{suite_enabled:true,scenarios:[{label:'train',start_date:'2024-03-01',end_date:'2024-04-01'}]}});
              _suiteState.scenarioGeneratorDraft = {windows:_suiteVisualWindows()};
              window.before = JSON.stringify(_suiteState.scenarioGeneratorDraft.windows);
              context = {start_date:'2023-01-01',end_date:'2025-12-31',exchanges:['bybit']};
              suiteRefreshVisualContext();
            }''')
            assert page.get_by_label('Reference exchange', exact=True).input_value() == 'bybit'
            assert page.evaluate('JSON.stringify(_suiteState.scenarioGeneratorDraft.windows) === before')
            assert page.locator('.scenario-canvas [data-part=move]').count() == 1
            assert page.evaluate('_suiteState.visualContextSignature === _suiteScenarioContextSignature(context)')
        finally:
            browser.close()
