"""Exercise real frontend handlers against delayed requests and failure responses."""
import json
import re
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def function(page, name):
    """Extract one page-local declaration without running unrelated page startup."""
    source = (ROOT / 'frontend' / (page + '.html')).read_text()
    match = re.search(r'^( +)(?:async )?function ' + name + r'\([^\n]*\) \{', source, re.M)
    assert match, name
    end = source.index('\n' + match[1] + '}', match.end()) + len(match[1]) + 2
    return source[match.start():end]


def run_js(code):
    """Run deterministic JavaScript with no browser network or production state."""
    result = subprocess.run(['node', '-e', "const assert = require('assert');\n" + code], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize('action', ['deleteJob', 'retryJob', 'requeueJob'])
def test_job_monitor_deduplicates_and_reports_html_errors(action):
    """Duplicate actions send one request and preserve a proxy failure's HTTP status."""
    run_js(function('jobs_monitor', action) + '''
const pendingJobActions = new Set(); let release, requests=0, notices=[];
const showActionDialog=async()=>true, setJobActionBusy=()=>{}, authOptions=x=>x;
const API_BASE='/api', currentTab='done', loadTab=()=>{};
const showActionNotice=async x=>notices.push(x.message);
const fetch=()=>{requests++;return new Promise(r=>release=r)};
(async()=>{
 const first=ACTION('job'); await Promise.resolve();
 const second=ACTION('job'); await Promise.resolve();
 assert.equal(requests,1);
 release({ok:false,status:502,statusText:'Bad Gateway',json:async()=>{throw Error('HTML')}});
 await Promise.all([first,second]);
 assert(notices[0].includes('502'));assert.equal(pendingJobActions.size,0);
})().catch(e=>{console.error(e);process.exitCode=1});
'''.replace('ACTION', action))


def test_login_blocks_duplicate_submissions_and_recovers():
    """Rapid Enter presses cannot consume multiple failed-login attempts."""
    run_js(function('root_login', 'submitLogin') + '''
let loginSubmitting=false, requests=0, reject;
const input={value:'fixture',disabled:false,focus(){}};
const document={getElementById:()=>input}, API_ORIGIN='', setBanner=()=>{}, redirectToWelcome=()=>{};
const fetchJson=()=>{requests++;return new Promise((r,j)=>reject=j)};
(async()=>{const event={preventDefault(){}};const a=submitLogin(event),b=submitLogin(event);
assert.equal(requests,1);assert(input.disabled);reject(Error('Denied'));await Promise.all([a,b]);assert(!input.disabled);assert(!loginSubmitting);
})().catch(e=>{console.error(e);process.exitCode=1});
''')


def test_setup_save_single_request_and_unlock():
    """Setup writes cannot overlap and the button recovers after a server failure."""
    run_js(function('welcome', 'saveSetup') + '''
let requests=0,reject;const button={disabled:false,value:'fixture'};
const document={getElementById:()=>button},API_ORIGIN='',authHeaders=()=>({}),render=()=>{},setBanner=()=>{},focusSection=()=>{};
const fetchJson=()=>{requests++;return new Promise((r,j)=>reject=j)};
(async()=>{const a=saveSetup(),b=saveSetup();assert.equal(requests,1);assert(button.disabled);reject(Error('Failed'));await Promise.all([a,b]);assert(!button.disabled);})().catch(e=>{console.error(e);process.exitCode=1});
''')


@pytest.mark.parametrize('action', ['runInventoryDeleteSelected', 'runInventoryClearDataset'])
def test_inventory_locks_during_confirmation_and_keeps_reviewed_target(action):
    """Repeated destructive actions and context changes cannot change the reviewed dataset."""
    run_js(function('market_data_main', action) + '''
let inventoryMutationPending=false, confirm,finish,requests=[];
const uiState={contextExchange:'bybit'};let view='ohlcv';const original={payload:{},selectedRowIds:['BTC']};
const getCurrentInventoryViewState=()=>original,getResolvedInventoryView=()=>view,getInventorySelectedCoins=()=>['BTC'];
const getInventoryCoinDisplayNames=x=>x,getExchangeMeta=()=>({}),showToast=()=>{},setInventoryBox=()=>{},renderInventorySidebarActions=()=>{};
const showConfirmDialog=()=>new Promise(r=>confirm=r),loadInventoryPanel=async()=>{};
const fetchJson=(url,options)=>{requests.push([url,JSON.parse(options.body)]);return new Promise(r=>finish=r)};
(async()=>{const a=ACTION(),b=ACTION();assert(inventoryMutationPending);uiState.contextExchange='binance';view='other';confirm(true);await Promise.resolve();
assert.equal(requests.length,1);assert(requests[0][0].includes('/bybit/'));assert.equal(requests[0][1].view,'ohlcv');finish({success:true});await Promise.all([a,b]);assert(!inventoryMutationPending);
})().catch(e=>{console.error(e);process.exitCode=1});
'''.replace('ACTION', action))


def test_services_validation_message_and_save_lock():
    """Settings display 422 field errors and reject overlapping writes."""
    run_js(function('services_monitor','_post') + '''
const pendingSettingsPosts=new Set(),button={disabled:false};let release,requests=0,message='';
const document={getElementById:()=>({previousElementSibling:button})},API_BASE='',authOptions=x=>x,_flash=(id,text)=>message=text;
const fetch=()=>{requests++;return new Promise(r=>release=r)};
(async()=>{const a=_post('/settings',{},'save'),b=_post('/settings',{},'save');assert.equal(requests,1);assert(button.disabled);
release({ok:false,status:422,json:async()=>({detail:[{loc:['body','port'],msg:'Invalid integer'}]})});await Promise.all([a,b]);assert(message.includes('port: Invalid integer'));assert(!button.disabled);
})().catch(e=>{console.error(e);process.exitCode=1});
''')


@pytest.mark.parametrize('status,payload,expected', [(422,{'detail':[{'loc':['body','coin'],'msg':'Required'}]},'Required'),(502,None,'502'),(200,{},'job ID')])
def test_heatmap_queue_errors_preserve_reason(status,payload,expected):
    """Malformed and failed queue responses never read an absent job identifier."""
    run_js(function('gap_heatmap','readQueueResponse') + f'''
(async()=>{{const response={{ok:{str(status==200).lower()},status:{status},json:async()=>{{const value={json.dumps(payload)};if(value===null)throw Error('HTML');return value;}}}};
await assert.rejects(readQueueResponse(response),new RegExp({json.dumps(expected)}));}})().catch(e=>{{console.error(e);process.exitCode=1}});
''')


def test_delete_ai_chat_stops_polling_and_unlocks_composer():
    """Successful deletion invalidates old responses and clears active turn state."""
    run_js(function('ai_chat','deleteCurrentConversation') + '''
const state={conversationId:'old',transitioning:false,retryMessages:{old:'x'},busy:true,pendingMessage:'x',chatGeneration:1};let stopped=0;
const window={PBGuiDialogs:{confirm:async()=>true}},setTransitioning=x=>state.transitioning=x,api=async()=>{},stopActivityPolling=()=>stopped++,rememberProfile=()=>{},loadConversations=async()=>{},setNotice=()=>{};
(async()=>{await deleteCurrentConversation();assert.equal(stopped,1);assert(!state.busy);assert.equal(state.pendingMessage,'');assert.equal(state.conversationId,'');assert.equal(state.chatGeneration,2);})().catch(e=>{console.error(e);process.exitCode=1});
''')


def test_approval_cancel_preserves_other_proposal_cards():
    """Only the reviewed proposal is hidden; cancelling restores it."""
    run_js(function('ai_chat','resolveProposal') + '''
const state={chatGeneration:1,conversationId:'chat',resolvingProposalIds:new Set()};let confirm,clears=0;
const window={PBGuiDialogs:{confirm:()=>new Promise(r=>confirm=r)}},proposalActionLabel=()=>'',reconcileProposals=async()=>{},renderProposals=()=>clears++;
const button={disabled:false},card={hidden:false,querySelectorAll:()=>[button]};
(async()=>{const task=resolveProposal({proposal_id:'a'},true,card);assert.equal(clears,0);confirm(false);await task;assert(!card.hidden);assert(!button.disabled);})().catch(e=>{console.error(e);process.exitCode=1});
''')


def test_hl_cancel_updates_running_lists_and_deduplicates():
    """Cancellation automatically reloads running namespaces after one request."""
    run_js(function('hl_data_actions','cancelJob') + '''
const pendingJobActions=new Set(),currentTab={dl:'running',build:'running'};let release,requests=0,reloaded=[];
const setJobActionBusy=()=>{},API_BASE='',authOptions=x=>x,loadRunning=async ns=>reloaded.push(ns);
const fetch=()=>{requests++;return new Promise(r=>release=r)};
(async()=>{const a=cancelJob('j'),b=cancelJob('j');assert.equal(requests,1);release({ok:true});await Promise.all([a,b]);assert.deepEqual(reloaded,['dl','build']);assert.equal(pendingJobActions.size,0);})().catch(e=>{console.error(e);process.exitCode=1});
''')


def test_db_delete_locks_and_uses_confirmed_job():
    """A changed editor selection cannot redirect a pending delete to another job."""
    source=(ROOT/'frontend/db_tools.html').read_text()
    start=source.index("    el('sync-delete').onclick = async function")
    end=source.index('\n    };',start)+7
    run_js('''
const button={disabled:false};const el=()=>button;let syncDeletePending=false,confirm,requests=[];
const state={syncJobId:'old'},setStatus=()=>{},confirmModal=()=>new Promise(r=>confirm=r),apiFetch=async url=>requests.push(url),closeSyncEditor=()=>{},loadSyncJobs=async()=>{};
'''+source[start:end]+'''
(async()=>{const a=button.onclick(),b=button.onclick();assert(button.disabled);state.syncJobId='new';confirm(true);await Promise.all([a,b]);assert.deepEqual(requests,['/sync/jobs/old']);assert.equal(state.syncJobId,'new');assert(!button.disabled);})().catch(e=>{console.error(e);process.exitCode=1});
''')
