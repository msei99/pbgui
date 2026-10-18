"""Offline source contracts and executable polling tests for Transfers."""

from pathlib import Path
import json
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[2]
PAGE = ROOT / "frontend" / "transfers.html"
NAV = (ROOT / "frontend" / "pbgui_nav.js").read_text(encoding="utf-8")


def test_transfers_page_uses_shared_shell_and_cookie_auth() -> None:
    """Transfers must remain a local authenticated FastAPI page."""

    source = PAGE.read_text(encoding="utf-8")

    assert "current:'system_transfers'" in source
    assert '/app/css/sidebar.css?v=6' in source
    assert '/app/js/sidebar_resize.js?v=3' in source
    assert '/app/js/pbgui_dialogs.js?v=11' in source
    assert "credentials:'same-origin'" in source
    assert "Authorization" not in source
    assert "document.cookie" not in source
    assert "localStorage" not in source
    assert "pbgui:transfers:pending-top-up:v1" in source
    assert "sessionStorage.setItem(PENDING_KEY" in source
    assert "pendingRequest:loadPendingRequest()" in source
    assert "button.disabled=state.busy" in source
    assert "if(state.busy&&!force)return" in source
    for sensitive in ("password", "private_key", "api_key", "signature", "credential", "token"):
        assert f"pending.{sensitive}" not in source
    assert "https://" not in source
    for sidebar_id in (
        "sidebar", "sidebar-sticky", "sidebar-header", "sidebar-toolbar",
        "sidebar-inner", "sidebar-resize",
    ):
        assert f'id="{sidebar_id}"' in source


def test_transfers_page_has_fixed_route_review_and_reconciliation() -> None:
    """The browser may choose an advertised route and amount, never a free destination."""

    source = PAGE.read_text(encoding="utf-8")

    assert "Main Perps → Vault" in source
    assert "Vault → Main Perps" in source
    assert "Main Perps → Main Spot" in source
    assert "Main Spot → Main Perps" in source
    assert "Spot → Perps" in source
    assert "Funding → Unified" in source
    assert "Funding → USD-M Futures" in source
    assert "Spot → USDT Futures" in source
    assert "Spot → UTA" in source
    assert "el('destination').textContent=text(route&&route.destination)" in source
    assert 'id="route-select" disabled' in source
    assert 'id="amount" type="number" min="0.000001" step="any" inputmode="decimal" value="5"' in source
    assert 'placeholder="5"' not in source
    assert "Review transfer" in source
    transfer_form = source.split('<div class="transfer-form">', 1)[1].split('</div><div id="status"', 1)[0]
    assert transfer_form.index('for="route-select"') < transfer_form.index('for="amount"') < transfer_form.index('id="submit"')
    assert "el('route-note').textContent=routeNote" in source
    assert "window.PBGuiDialogs.confirm" in source
    assert "crypto.randomUUID" in source
    assert "operation_id:pending.operation_id" in source
    assert "route:route.id" in source
    assert "'/transfers/execute/'" in source
    assert "same operation ID will be reused" in source
    assert "max_transferable" in source
    assert "Your Vault Equity" in source
    assert "Vault Account Value" in source
    assert "Your Max Withdrawable" in source
    assert "Open Vault Positions" in source
    assert "renderPositions(value)" in source
    for column in ("Coin", "Side", "Size", "Position value", "Entry", "Unrealized PnL", "Liquidation", "Leverage"):
        assert column in source
    assert "may affect Passivbot wallet-exposure sizing" in source
    assert "el('amount').addEventListener('input',updateSubmit)" in source
    assert "pendingOperationId=''" not in source
    assert "error.status=response.status" in source
    assert "Number(error.status)>=400&&Number(error.status)<500" in source
    assert "(?:[eE][+-]?\\d+)?" in source
    assert "Unknown operations can only be reconciled" in source
    assert "'/reconcile'" in source
    assert "innerHTML" not in source
    assert "textContent" in source
    assert "Profit Sweep Due" in source


def test_transfers_navigation_and_profit_sweep_handoff_are_registered() -> None:
    """System navigation and the contextual Vault shortcut target the same page."""

    profit_sweep = (ROOT / "frontend" / "profit_sweep.html").read_text(encoding="utf-8")

    assert "{ page: 'system_transfers'" in NAV
    assert "'system_transfers':  '/api/profit-sweep/transfers/main_page'" in NAV
    assert "'system_transfers':             '47_transfers'" in NAV
    assert 'id="fund-account" hidden>Fund account</button>' in profit_sweep
    assert "byId('fund-account').addEventListener('click', openFundAccount)" in profit_sweep
    assert "'/transfers/main_page?user=' + encodeURIComponent(state.selectedUser) + '&route=main_perps_to_vault'" in profit_sweep


# Execute the actual page script, including renderers and action handlers. Only
# browser primitives, time and fetch are replaced; no production data is read.
_NODE_HARNESS = r"""
const assert = require('node:assert/strict');
const vm = require('node:vm');
const fs = require('node:fs');
let now = Date.UTC(2026, 0, 1), origin = now, random = 0;
let timers = new Map(), timerId = 0, calls = [], handler;
const documentListeners = new Map(), windowListeners = new Map(), nodes = new Map();
class Element {
  constructor(tag='div') {this.tag=tag;this.children=[];this._value='';this.textContent='';this.disabled=false;this.listeners={};}
  appendChild(node) {this.children.push(node);return node;}
  replaceChildren(...children) {this.children=children;this.textContent='';if(this.tag==='select')this._value='';}
  setAttribute(name,value) {this[name]=value;}
  addEventListener(name,fn) {this.listeners[name]=fn;}
  get options() {return this.children;}
  get value() {
    if(this.tag!=='select')return this._value;
    return this._value || (this.children.find(x=>x.selected)||this.children[0]||{}).value || '';
  }
  set value(value) {this._value=String(value);}
}
function el(id) {
  if(!nodes.has(id))nodes.set(id,new Element(id==='route-select'?'select':'div'));
  return nodes.get(id);
}
el('amount').value='5';
const document={visibilityState:'visible',getElementById:el,createElement:tag=>new Element(tag),querySelectorAll:()=>[],
 addEventListener:(name,fn)=>documentListeners.set(name,fn),
 removeEventListener:(name,fn)=>{if(documentListeners.get(name)===fn)documentListeners.delete(name)}};
const storage=new Map();
const window={API_BASE:'/api/profit-sweep',location:{href:'http://offline.test/api/profit-sweep/transfers/main_page'},
 addEventListener:(name,fn)=>windowListeners.set(name,fn),PBGuiSidebarResize:{init(){}},PBGuiDialogs:{confirm:async()=>true}};
window.crypto={randomUUID:()=> '123e4567-e89b-42d3-a456-426614174000'};
class ClockDate extends Date {constructor(...args){super(...(args.length?args:[now]));}static now(){return now;}}
function preview(user='alice') {return {user_name:user,routes:[
 {id:'perp_to_spot',asset:'USDC',source:'Perps',destination:'Spot',minimum_amount:'1',max_transferable:'100',source_balance:'100',destination_balance:'0'},
 {id:'spot_to_perp',asset:'USDC',source:'Spot',destination:'Perps',minimum_amount:'1',max_transferable:'100',source_balance:'100',destination_balance:'0'}]};}
function response(status,payload,headers={}) {return {ok:status>=200&&status<300,status,headers:{get:name=>headers[name]??null},json:async()=>payload};}
function deferred() {let resolve,reject;const promise=new Promise((a,b)=>{resolve=a;reject=b});return {promise,resolve,reject};}
const context=vm.createContext({window,document,URL,AbortController,Date:ClockDate,Math:Object.assign(Object.create(Math),{random:()=>random}),
 sessionStorage:{getItem:key=>storage.get(key)||null,setItem:(k,v)=>storage.set(k,v),removeItem:key=>storage.delete(key)},
 crypto:{randomUUID:()=> '123e4567-e89b-42d3-a456-426614174000'},
 setTimeout:(fn,delay)=>{const id=++timerId;timers.set(id,{fn,at:now+delay});return id},clearTimeout:id=>timers.delete(id),
 fetch:(path,options)=>{
   const call={path,options,at:now-origin};calls.push(call);
   if(handler)return Promise.resolve().then(()=>handler(call));
   return Promise.resolve(response(200,path.includes('/preview/')?preview():path.includes('/operations/')?{operations:[]}:{status:'confirmed'}));
 }});
let source=fs.readFileSync(0,'utf8').split('<script>').at(-1).split('</script>')[0];
source=source.replace("window.PBGuiSidebarResize.init();loadUsers().catch(function(error){if(!stopped)setStatus(error.message,'error')});",
 "window.__transfers={state,selectAccount,loadAccountData,loadUsers,accountPolling,scheduleAccountUpdate,submitTopUp,reconcile,setBusy,parseRetryAfter};");
vm.runInContext(source,context);
const t=window.__transfers;
assert.ok(t,'Test hook must execute inside the real page script');
t.state.users=[{name:'alice',exchange:'hyperliquid'},{name:'bob',exchange:'bybit'}];
async function flush(){for(let i=0;i<40;i++)await Promise.resolve();}
async function advance(ms){const target=now+ms;let guard=0;while(true){
 const entry=[...timers.entries()].filter(([id,timer])=>timer.at<=target).sort((a,b)=>a[1].at-b[1].at)[0];
 if(!entry)break;assert.ok(++guard<10000,'Scheduler must not spin');
 timers.delete(entry[0]);now=entry[1].at;entry[1].fn();await flush();
}now=target;await flush();}
function times(kind){return calls.filter(c=>c.path.includes('/'+kind+'/')).map(c=>c.at);}
function content(node){return node.textContent+node.children.map(content).join(' ');}
function visibility(value){document.visibilityState=value;documentListeners.get('visibilitychange')?.();}
function exit(){windowListeners.get('pagehide')();}
"""


def _run_page(code: str) -> None:
    """Run the real page with fake time, DOM and fetch in an offline Node VM."""
    result = subprocess.run(
        ["node", "-e", _NODE_HARNESS + "\n(async()=>{\n" + code +
         "\nconsole.log('TRANSFERS_TEST_COMPLETE');})().catch(error=>{console.error(error);process.exitCode=1});"],
        input=PAGE.read_text(encoding="utf-8"), text=True, capture_output=True,
        timeout=20, cwd=ROOT,
    )
    assert result.returncode == 0, result.stderr
    assert "TRANSFERS_TEST_COMPLETE" in result.stdout, "Test must finish every asynchronous assertion"


@pytest.mark.parametrize("failure", ("503", "429", "403", "404", "409", "422", "network"))
def test_failed_preview_backs_off_while_history_keeps_updating(failure: str) -> None:
    """HTTP and network failures back off without permanently stopping data updates."""
    _run_page("const failure=" + repr(failure) + r""";
handler=call=>{
 if(call.path.includes('/preview/')){
   if(failure==='network')throw new TypeError('Network unavailable');
   return response(Number(failure),{detail:'Exchange snapshot incomplete: read_failed'});
 }
 return response(200,{operations:[{operation_id:'history-row',status:'unknown',asset:'USDC',requested_amount:'2',can_reconcile:true}]});
};
await t.selectAccount('alice');await advance(610000);
assert.deepEqual(times('preview'),[0,10000,30000,70000,150000,310000,610000]);
assert.equal(times('operations').length,123);
assert.deepEqual(times('operations').slice(0,5),[0,5000,10000,15000,20000]);
assert.match(el('status').textContent,/Balances unavailable:.*Next automatic attempt/);
assert.ok(content(el('history')).includes('Reconcile'));
assert.equal(el('submit').disabled,true);
assert.equal(t.state.preview,null);
exit();
""")


def test_jitter_and_success_reset_use_completion_time() -> None:
    """Backoff includes bounded jitter; success restores the five-second cadence."""
    _run_page(r"""
random=.5;let previews=0;
handler=call=>call.path.includes('/preview/')&&previews++<2?response(503,{detail:'offline'}):response(200,call.path.includes('/preview/')?preview():{operations:[]});
await t.selectAccount('alice');await advance(10500);await advance(20500);
assert.deepEqual(times('preview'),[0,10500,31000]);
assert.equal(t.accountPolling('alice').preview.failures,0);
await advance(5000);assert.deepEqual(times('preview'),[0,10500,31000,36000]);
assert.equal(el('submit').disabled,false);
assert.doesNotMatch(el('status').textContent,/unavailable/);
random=.999;
handler=call=>call.path.includes('/preview/')?response(503,{detail:'offline'}):response(200,{operations:[]});
await advance(5000);
for(const delay of [10999,20999,40999,80999,160999,300000,300000]){
 const previous=times('preview').at(-1);
 await advance(delay-1);assert.equal(times('preview').at(-1),previous);
 await advance(1);assert.equal(times('preview').at(-1)-previous,delay);
}
exit();
""")


@pytest.mark.parametrize("header", ("120", "Thu, 01 Jan 2026 00:02:00 GMT"))
def test_retry_after_deadlines_survive_account_selection_and_visibility(header: str) -> None:
    """Delta and HTTP-date Retry-After apply even after an explicit selection."""
    _run_page("const header=" + repr(header) + r""";
handler=call=>call.path.includes('/preview/')?response(429,{detail:'wait'},{'Retry-After':header}):response(200,{operations:[]});
await t.selectAccount('alice');
await advance(1000);await t.selectAccount('bob');await t.selectAccount('alice');
assert.deepEqual(times('preview'),[0,1000]); // one attempt for each account
visibility('hidden');await advance(50000);visibility('visible');await advance(0);
assert.equal(times('preview').length,2);
await advance(69000);
assert.deepEqual(times('preview'),[0,1000,120000]);
assert.match(el('status').textContent,/Next automatic attempt/);
exit();
""")


@pytest.mark.parametrize("header", (None, "", "invalid", "Wed, 01 Jan 2020 00:00:00 GMT"))
def test_missing_invalid_and_past_retry_after_fall_back_to_local_delay(header: str | None) -> None:
    """Malformed headers cannot disable retries or bypass the local backoff."""
    _run_page("const header=" + json.dumps(header) + r""";
handler=call=>call.path.includes('/preview/')?response(503,{detail:'offline'},{'Retry-After':header}):response(200,{operations:[]});
await t.selectAccount('alice');await advance(9999);assert.equal(times('preview').length,1);
await advance(1);assert.deepEqual(times('preview'),[0,10000]);exit();
""")


def test_slow_preview_does_not_block_history_or_overlap_requests() -> None:
    """History renders while the initial preview waits; repeat triggers coalesce."""
    _run_page(r"""
const slow=deferred();
handler=call=>call.path.includes('/preview/')?slow.promise:response(200,{operations:[{operation_id:'fast-history',status:'confirmed',asset:'USDC',requested_amount:'2'}]});
const initial=t.selectAccount('alice');await flush();
assert.ok(content(el('history')).includes('fast-history'));
assert.equal(t.state.preview,null);assert.equal(el('submit').disabled,true);
await t.loadAccountData('alice',t.state.generation);t.scheduleAccountUpdate();
await advance(15000);assert.equal(times('preview').length,1);assert.equal(times('operations').length,4);
slow.resolve(response(200,preview()));await initial;await flush();
assert.equal(t.accountPolling('alice').preview.nextAt,origin+20000);
assert.equal(el('submit').disabled,false);exit();
""")


def test_account_switch_rejects_late_responses_and_finally_callbacks() -> None:
    """An aborted request cannot overwrite a newer account or its scheduler."""
    _run_page(r"""
const old=deferred();
handler=call=>call.path.includes('/preview/alice')?old.promise:response(200,call.path.includes('/preview/')?preview('bob'):{operations:[]});
const initial=t.selectAccount('alice');await flush();
const oldRequest=calls[0], oldTimer=[...timers.values()][0].fn;await t.selectAccount('bob');
const callCount=calls.length;oldTimer();await flush();assert.equal(calls.length,callCount);
assert.equal(oldRequest.options.signal.aborted,true);
const before=[...timers.values()].map(x=>x.at);
old.resolve(response(503,{detail:'stale failure'}));await initial;await flush();
assert.equal(t.state.selected,'bob');assert.equal(t.state.preview.user_name,'bob');
assert.equal(t.accountPolling('alice').preview.failures,0);
assert.deepEqual([...timers.values()].map(x=>x.at),before);
await advance(5000);assert.equal(calls.filter(x=>x.path.includes('/preview/bob')).length,2);exit();
""")


def test_hidden_tab_preserves_deadlines_and_pagehide_disposes_requests() -> None:
    """Visibility never bypasses backoff; pagehide aborts both owned requests."""
    _run_page(r"""
handler=call=>call.path.includes('/preview/')?response(503,{detail:'offline'}):response(200,{operations:[]});
await t.selectAccount('alice');visibility('hidden');await advance(6000);
assert.equal(calls.length,2);visibility('visible');await advance(0);
assert.equal(times('preview').length,1);assert.equal(times('operations').length,2);
await advance(4000);assert.deepEqual(times('preview'),[0,10000]);
const pending=deferred();handler=()=>pending.promise;
const selection=t.selectAccount('bob');await flush();
const active=calls.slice(-2);assert.ok(active.every(c=>!c.options.signal.aborted));
const staleTimer=[...timers.values()][0]?.fn;
exit();assert.equal(timers.size,0);assert.ok(active.every(c=>c.options.signal.aborted));
assert.equal(documentListeners.has('visibilitychange'),false);
if(staleTimer)staleTimer();
pending.resolve(response(200,preview('bob')));await selection;await flush();
assert.equal(t.state.preview,null);assert.equal(timers.size,0);
windowListeners.get('pageshow')({persisted:true});await advance(0);
assert.equal(documentListeners.has('visibilitychange'),true);assert.ok(calls.length>active.length);
exit();
""")


def test_history_errors_do_not_throttle_preview_or_lose_edits() -> None:
    """Independent errors preserve input, route and pending submission identity."""
    _run_page(r"""
await t.selectAccount('alice');
el('amount').value='7.25';el('route-select').value='spot_to_perp';
t.state.pendingRequest={user:'alice',route:'spot_to_perp',amount:'7.25',operation_id:'pending-id'};
handler=call=>call.path.includes('/preview/')?response(200,preview()):response(503,{detail:'history offline'});
await advance(20000);
assert.deepEqual(times('preview'),[0,5000,10000,15000,20000]);
assert.deepEqual(times('operations'),[0,5000,15000]);
assert.equal(el('amount').value,'7.25');assert.equal(el('route-select').value,'spot_to_perp');
assert.equal(t.state.pendingRequest.operation_id,'pending-id');
assert.match(el('status').textContent,/Transfer history unavailable/);
handler=call=>call.path.includes('/preview/')?response(503,{detail:'preview offline'}):response(200,{operations:[]});
await advance(5000);assert.equal(el('submit').disabled,true);
assert.equal(el('amount').value,'7.25');
handler=call=>response(200,call.path.includes('/preview/')?preview():{operations:[]});
await advance(20000);
assert.equal(el('route-select').value,'spot_to_perp');assert.equal(el('amount').value,'7.25');
assert.equal(t.state.pendingRequest.operation_id,'pending-id');exit();
""")


def test_explicit_selection_resets_local_backoff_but_actions_do_not() -> None:
    """Follow-up loads respect failures and release busy state without resubmission."""
    _run_page(r"""
await t.selectAccount('alice');
el('amount').value='7';
const action=deferred();handler=call=>{
 if(call.options.method==='POST')return action.promise;
 return call.path.includes('/preview/')?response(503,{detail:'offline'}):response(200,{operations:[]});
};
const submit=t.submitTopUp();await flush();assert.equal(t.state.busy,true);
action.resolve(response(200,{status:'confirmed'}));await submit;await advance(0);
assert.equal(t.state.busy,false);assert.equal(t.accountPolling('alice').preview.failures,1);
assert.equal(calls.filter(c=>c.options.method==='POST').length,1);
const deadline=t.accountPolling('alice').preview.nextAt;
await t.reconcile('recorded-id');await advance(0);
assert.equal(t.state.busy,false);assert.equal(t.accountPolling('alice').preview.nextAt,deadline);
assert.equal(times('preview').length,2);
await advance(10000);assert.equal(times('preview').length,3);
assert.equal(calls.filter(c=>c.options.method==='POST').length,2);
await t.selectAccount('alice');assert.equal(times('preview').length,4);
assert.equal(el('amount').value,'7');exit();
""")


def test_busy_actions_and_abort_do_not_revive_old_generations() -> None:
    """Late GET success during a POST and AbortError never change retry counters."""
    _run_page(r"""
const old=deferred();handler=()=>old.promise;
const selection=t.selectAccount('alice');await flush();
t.setBusy(true);const generation=t.state.generation;
old.resolve(response(200,preview()));await selection;await flush();
assert.equal(t.state.preview,null);assert.equal(t.state.generation,generation);assert.equal(timers.size,0);
t.setBusy(false);
handler=()=>{const error=new Error('cancelled');error.name='AbortError';throw error};
await advance(0);assert.equal(t.accountPolling('alice').preview.failures,0);
assert.equal(t.accountPolling('alice').history.failures,0);
assert.ok([...timers.values()].every(x=>x.at>now));exit();
""")


@pytest.mark.parametrize("kind", ("preview", "operations"))
@pytest.mark.parametrize("status", (403, 404, 409, 422))
def test_read_errors_recover_automatically_without_reselection(kind: str, status: int) -> None:
    """Both sources revalidate rejected reads and recover without user intervention."""
    _run_page("const kind=" + repr(kind) + ";const status=" + str(status) + r""";
let failures=0;
handler=call=>{
 if(call.path.includes('/'+kind+'/')&&failures++<2)return response(status,{detail:'account configuration unavailable'});
 return response(200,call.path.includes('/preview/')?preview():{operations:[]});
};
await t.selectAccount('alice');
assert.match(el('status').textContent,/Next automatic attempt/);
await advance(30000);
assert.deepEqual(times(kind),[0,10000,30000]);
assert.equal(times(kind==='preview'?'operations':'preview').length,7);
assert.doesNotMatch(el('status').textContent,/unavailable/);
assert.equal(t.accountPolling('alice')[kind==='preview'?'preview':'history'].failures,0);
assert.equal(el('submit').disabled,false);
await advance(5000);assert.deepEqual(times(kind),[0,10000,30000,35000]);
assert.ok(calls.every(call=>!call.options.method||call.options.method==='GET'));
exit();
""")


def test_unauthorized_response_is_left_to_shared_login_flow() -> None:
    """401 remains owned by the shared nav authentication interceptor."""
    _run_page(r"""
handler=call=>call.path.includes('/preview/')?response(401,{detail:'not available'}):response(200,{operations:[]});
await t.selectAccount('alice');await advance(60000);
assert.equal(times('preview').length,1);assert.equal(times('operations').length,13);
assert.doesNotMatch(el('status').textContent,/Next automatic attempt/);exit();
""")


@pytest.mark.parametrize("action_name", ("submit", "reconcile"))
def test_pagehide_cancels_actions_without_late_mutations(action_name: str) -> None:
    """An action detached by pagehide cannot clear retained IDs or revive polling."""
    _run_page("const actionName=" + repr(action_name) + r""";
await t.selectAccount('alice');
const late=deferred();handler=()=>late.promise;
const action=actionName==='submit'?t.submitTopUp():t.reconcile('existing-operation');await flush();
const request=calls.at(-1);assert.equal(request.options.method,'POST');
const pending=t.state.pendingRequest;const status=el('status').textContent;
exit();assert.equal(request.options.signal.aborted,true);assert.equal(timers.size,0);
late.resolve(response(200,{status:'confirmed'}));await action;await flush();
assert.equal(t.state.pendingRequest,pending);assert.equal(el('status').textContent,status);
assert.equal(t.state.busy,false);assert.equal(timers.size,0);
windowListeners.get('pageshow')({persisted:true});
handler=call=>response(200,call.path.includes('/preview/')?preview():{operations:[]});
await advance(5000);assert.equal(calls.filter(c=>c.options.method==='POST').length,1);exit();
""")


@pytest.mark.parametrize("width,prefix", ((1400, ""), (700, "/mounted")))
def test_transfers_real_menu_browser_backoff_and_state(width: int, prefix: str) -> None:
    """Open the actual navigation destination using only local assets and fixture APIs."""
    from datetime import datetime, timezone
    from urllib.parse import urlparse

    playwright = pytest.importorskip("playwright.sync_api")
    source = PAGE.read_text(encoding="utf-8")
    for key, value in {"API_BASE": prefix + "/api/profit-sweep", "BASE_PREFIX": prefix,
                       "VERSION": "test", "SERIAL": "1", "NAV_HASH": "test"}.items():
        source = source.replace("%%" + key + "%%", value)
    source = source.replace('src="/app/', 'src="' + prefix + '/app/').replace('href="/app/', 'href="' + prefix + '/app/')
    mode = {"offline": False}
    requests, errors = [], []
    routes = [{"id": route, "asset": "USDC", "source": "Perps", "destination": "Spot",
               "minimum_amount": "1", "max_transferable": "100", "source_balance": "100", "destination_balance": "0"}
              for route in ("perp_to_spot", "spot_to_perp")]
    with playwright.sync_playwright() as driver:
        browser = driver.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": width, "height": 900})
        page.clock.install(time=datetime(2026, 1, 1, tzinfo=timezone.utc))
        page.add_init_script("Math.random=()=>0")
        page.on("pageerror", lambda error: errors.append(str(error)))

        def respond(route):
            """Intercept every request, with no exchange, application server or secrets."""
            raw = urlparse(route.request.url).path
            path = raw.removeprefix(prefix) if prefix else raw
            if path == "/app/start.html":
                route.fulfill(content_type="text/html", body=f'''<nav id="topnav"></nav>
                <script>window.PBGUI_BASE_PREFIX={prefix!r};window.PBGUI_NAV_CONFIG={{current:'welcome',authenticated:true}};</script>
                <script src="{prefix}/app/pbgui_nav.js?v=test"></script>''')
            elif path == "/api/profit-sweep/transfers/main_page":
                route.fulfill(content_type="text/html", body=source)
            elif path.startswith("/app/"):
                asset = ROOT / "frontend" / path.removeprefix("/app/")
                route.fulfill(body=asset.read_bytes() if asset.is_file() else b"",
                              content_type="text/css" if asset.suffix == ".css" else "text/javascript")
            elif path == "/api/profit-sweep/transfers/users":
                route.fulfill(json={"users": [{"name": "alice", "exchange": "hyperliquid"}]})
            elif path.startswith("/api/profit-sweep/transfers/"):
                assert raw.startswith(prefix + "/api/")
                assert route.request.method == "GET", "Browser smoke test must never submit transfers"
                requests.append(path)
                if "/preview/" in path:
                    route.fulfill(status=503 if mode["offline"] else 200,
                                  json={"detail": "Exchange snapshot incomplete: read_failed"} if mode["offline"] else {"routes": routes})
                else:
                    route.fulfill(json={"operations": [{"operation_id": "fixture-only", "status": "unknown", "asset": "USDC",
                                                         "requested_amount": "2", "can_reconcile": True}]})
            elif path == "/api/help/index":
                route.fulfill(json=[{"file": "47_transfers", "title": "Transfers"}])
            elif path == "/api/help/content":
                route.fulfill(json={"content": "# Transfers\n\nOffline fixture."})
            else:
                route.fulfill(json={})

        page.route("**/*", respond)
        page.goto("http://transfers.test" + prefix + "/app/start.html")
        page.locator('.nav-group-btn[data-group="system"]').click()
        page.locator('[data-page="system_transfers"]').click()
        playwright.expect(page).to_have_url("http://transfers.test" + prefix + "/api/profit-sweep/transfers/main_page")
        playwright.expect(page.locator("#route-select option")).to_have_count(2)
        page.locator("#amount").fill("7.25")
        page.locator("#route-select").select_option("spot_to_perp")
        mode["offline"] = True
        page.clock.run_for(5000)
        playwright.expect(page.locator("#status")).to_contain_text("Next automatic attempt")
        playwright.expect(page.locator("#submit")).to_be_disabled()
        playwright.expect(page.locator("#history")).to_contain_text("Reconcile")
        preview_count = sum("/preview/" in path for path in requests)
        history_count = sum("/operations/" in path for path in requests)
        page.clock.run_for(5000)
        playwright.expect(page.locator("#status")).to_contain_text("Balances unavailable")
        assert sum("/preview/" in path for path in requests) == preview_count
        assert sum("/operations/" in path for path in requests) > history_count
        page.clock.run_for(5000)
        playwright.expect(page.locator("#status")).to_contain_text("Next automatic attempt")
        assert sum("/preview/" in path for path in requests) == preview_count + 1
        mode["offline"] = False
        page.clock.run_for(20000)
        playwright.expect(page.locator("#submit")).to_be_enabled()
        playwright.expect(page.locator("#amount")).to_have_value("7.25")
        playwright.expect(page.locator("#route-select")).to_have_value("spot_to_perp")
        if width > 760:
            assert page.locator("#sidebar").bounding_box()["width"] == 240
            for key, expected in (("Home", 160), ("End", 420)):
                page.locator("#sidebar-resize").focus()
                page.keyboard.press(key)
                assert page.locator("#sidebar").bounding_box()["width"] == expected
        assert page.locator("#sidebar-inner").evaluate("node=>node.scrollWidth<=node.clientWidth")
        destination = page.url
        page.locator("#pbgui-guide-btn").click()
        playwright.expect(page.locator("#pbgui-shared-help-ovl")).to_be_visible()
        assert page.url == destination
        for language in ("en", "de"):
            playwright.expect(page.locator("#pbgui-shared-help-lang-" + language)).to_be_visible()
        page.locator("#pbgui-shared-help-close").click()
        assert not errors
        browser.close()


def test_late_account_list_is_aborted_and_cannot_revive_the_page() -> None:
    """Initial account discovery has an owner and is disposed just like source reads."""
    _run_page(r"""
const late=deferred();handler=()=>late.promise;
const loading=t.loadUsers();await flush();
const request=calls[0];const users=t.state.users;const status=el('status').textContent;
exit();assert.equal(request.options.signal.aborted,true);
late.resolve(response(200,{users:[{name:'stale-user',exchange:'hyperliquid'}]}));
await loading;await flush();
assert.equal(t.state.users,users);assert.equal(el('status').textContent,status);
assert.equal(t.state.selected,'');assert.equal(timers.size,0);
""")
