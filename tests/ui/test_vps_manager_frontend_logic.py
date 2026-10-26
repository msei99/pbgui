"""Regression tests for VPS Manager frontend state handling."""

from __future__ import annotations

import json
import subprocess
import textwrap
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]
HTML_PATH = ROOT / "frontend" / "vps_manager.html"
LOG_VIEWER_PATH = ROOT / "frontend" / "js" / "log_viewer_panel.js"


@pytest.mark.parametrize("command,scope,has_pb8,notice", [
    ("master-update-pb8", "master", True, True),
    ("master-update-pbgui-pb8", "master", True, True),
    ("master-update-pb7", "master", True, False),
    ("vps-update-pb8", "vps", True, True),
    ("vps-update-pbgui-pb8", "vps", True, True),
    ("vps-update-pb7", "vps", True, False),
    ("vps-update-runtime", "bulk", True, True),
    ("vps-update-pbgui-runtime", "bulk", True, True),
    ("vps-update-runtime", "bulk", False, False),
    ("vps-update-pbgui-runtime", "bulk", False, False),
    ("vps-update", "bulk", True, False),
])
def test_pb8_update_notice_preserves_actions(command, scope, has_pb8, notice):
    """Only PB8 updates wait for one notice; cancel sends nothing and retains state."""
    bootstrap = """
        const caseData = %s;
        const calls = [], dialogs = [];
        let resolveDialog;
        const window = { PBGuiDialogs: { confirm(options) {
          dialogs.push(options);
          return new Promise(resolve => { resolveDialog = resolve; });
        } } };
        const store = { state: { config: { vps_deploy: { action: 'old' } } } };
        const selected = ['vps1', 'vps2'];
        const getManagedRows = () => [{hostname:'vps1', pb8_installed:caseData.has_pb8},
                                      {hostname:'vps2', pb8_installed:false}];
        const getSelectedOverviewHosts = () => selected.slice();
        const getVpsDeployConfig = () => ({mode:'parallel',debug:true});
        const overviewDeployNeedsPassword = () => false;
        const toast = (...args) => calls.push(['toast',...args]);
        const selectView = (...args) => calls.push(['view',...args]);
        const send = payload => calls.push(['send',payload]);
        const openMasterTaskLog = (...args) => calls.push(['master-log',...args]);
        const runMaster = (...args) => calls.push(['master-run',...args]);
        const openVpsTaskLog = (...args) => calls.push(['vps-log',...args]);
        const runVps = (...args) => calls.push(['vps-run',...args]);
    """ % json.dumps(dict(command=command, scope=scope, has_pb8=has_pb8, notice=notice))
    assertions = r"""
        (async () => {
          const extra = {branch:'master',commit:'abc'};
          const action = () => caseData.scope === 'master'
            ? runMasterWithLog(caseData.command,'Update',extra)
            : caseData.scope === 'vps'
              ? runVpsWithLog('vps1',caseData.command,'Update',extra,false)
              : deploySelectedVpsAction(caseData.command,'Update');
          let pending = action();
          if (caseData.notice) {
            assert.equal(dialogs.length,1);
            assert.equal(calls.length,0);
            assert.equal(store.state.config.vps_deploy.action,'old');
            assert.match(dialogs[0].message,/click Save/);
            assert.match(dialogs[0].detail,/VPS with no PB8 bots first/);
            resolveDialog(false);
            await pending;
            assert.equal(calls.length,0);
            assert.equal(store.state.config.vps_deploy.action,'old');
            pending = action();
            assert.equal(dialogs.length,2); // One dialog per attempted update, never per host.
            resolveDialog(true);
          } else {
            assert.equal(dialogs.length,0);
          }
          await pending;
          if (caseData.scope === 'master') {
            assert.deepEqual(calls,[['master-log',caseData.command,'Update'],
                                    ['master-run',caseData.command,'Update',extra]]);
          } else if (caseData.scope === 'vps') {
            assert.deepEqual(calls,[['vps-log','vps1',caseData.command,'Update'],
                                    ['vps-run','vps1',caseData.command,'Update',extra,false]]);
          } else {
            assert.deepEqual(calls,[['view','deploys-vps-logging'],['send',{
              cmd:'run_vps_deploy',hostnames:['vps1','vps2'],command:caseData.command,
              mode:'parallel',extra_vars:caseData.command === 'vps-update'
                ? {reboot:false,reboot_requested:false} : null,debug:true
            }]]);
          }
        })().catch(error => { console.error(error); process.exitCode=1; });
    """
    _run_node_assertions(
        ["withPb8UpdateNotice", "runMasterWithLog", "runVpsWithLog", "deploySelectedVpsAction"],
        bootstrap=bootstrap, assertions=assertions,
    )


def test_unknown_ssh_host_confirmation_uses_exact_fingerprint() -> None:
    """Display and resubmit the exact SSH host-key fingerprint before trust."""
    source = HTML_PATH.read_text(encoding="utf-8")

    assert "hostKey.needs_confirmation" in source
    assert "accepted_host_key_fingerprint = fingerprint" in source
    assert "accepted_host_key_fingerprint: String(msg.fingerprint || '')" in source
    assert "replace_existing_host_key = changed" in source
    assert "Review changed key" in source
    assert "String(flow.command_text || 'the operation')" in source
    assert "and continue with Update Linux?" not in source


def test_add_vps_offers_pb8_live_only_profile() -> None:
    """Fresh VPS setup offers PB7, PB8-only, and combined runtimes."""
    source = HTML_PATH.read_text(encoding="utf-8")

    assert "runtime_profile: 'pb7'" in source
    assert "PB8 Live only (no PB7)" in source
    assert "PB7 + PB8" in source
    assert "PB8-only setup does not clone or install PB7" in source
    assert "pb8Requested: ['pb8', 'pb7_pb8'].includes(String(store.addForm.runtime_profile || 'pb7'))" in source
    assert "Setup and PB8 Live installation successful." in source


def _extract_function(source: str, name: str) -> str:
    """Extract a named JavaScript function from the inline VPS manager script."""
    marker = f"function {name}("
    start = source.find(marker)
    assert start >= 0, f"Could not find JavaScript function {name!r}"

    brace_start = source.find("{", start)
    assert brace_start >= 0, f"Could not find opening brace for {name!r}"

    depth = 0
    quote: str | None = None
    escaped = False

    for index in range(brace_start, len(source)):
        char = source[index]
        if quote is not None:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
            continue

        if char in ("'", '"', "`"):
            quote = char
            continue
        if char == "{":
            depth += 1
            continue
        if char == "}":
            depth -= 1
            if depth == 0:
                return source[start : index + 1]

    raise AssertionError(f"Could not extract complete JavaScript function {name!r}")


def _run_node_assertions(
    function_names: list[str], *, bootstrap: str, assertions: str, timeout: float | None = None
) -> None:
    """Run focused Node assertions against extracted frontend helpers."""
    source = HTML_PATH.read_text(encoding="utf-8")
    extracted = "\n\n".join(_extract_function(source, name) for name in function_names)
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');

        {bootstrap}

        {extracted}

        {assertions}
        """
    )
    try:
        result = subprocess.run(
            ["node", "-e", script],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        pytest.fail(f"Node-backed frontend regression exceeded {timeout} seconds", pytrace=False)
    assert result.returncode == 0, (
        "Node-backed frontend regression failed\n"
        f"STDOUT:\n{result.stdout}\n"
        f"STDERR:\n{result.stderr}"
    )


def _run_hl_history_assertions(assertions: str) -> None:
    """Exercise automatic history updates with synthetic responses and fake time."""
    _run_node_assertions(
        ["refreshOpenHlRateLimitHistory", "renderMetricHistoryPayload", "updateHlCreditCost"],
        timeout=30,
        bootstrap=r"""
        const API_BASE = '/api/vps-manager';
        const cached = { samples: [{ sampled_at: 10, used: 100, cap: 100 }], is_vault: false };
        const fresh = { samples: [{ sampled_at: 20, used: 100, cap: 100 }], is_vault: false };
        const cacheKey = 'test-host::hl_requests::test-bot';
        let cpuHistoryLastPayload = { data: cached, host: 'test-host', botName: 'test-bot', metric: 'hl_requests' };
        const originalContext = cpuHistoryLastPayload;
        let cpuHistoryRequestGeneration = 0;
        let cpuHistoryAbortController = null;
        const metricHistoryCache = { [cacheKey]: cached };
        let hlCreditPurchasePending = false;
        const store = { authExpired: false, hlRateLimits: { accounts: [{ users: ['test-bot'], sampled_at: 20 }] } };
        const overlay = { visible: true, classList: { contains: () => overlay.visible } };
        const elements = {
          cpuHistoryOverlay: overlay,
          cpuHistorySubtitle: { textContent: 'Normal subtitle' },
          cpuHistoryTitle: { textContent: 'Original title' },
          cpuHistoryBody: { innerHTML: 'Original chart' },
          cpuHistoryMeta: { innerHTML: 'Original metadata' },
          hlCreditAmount: { value: '123', selectionStart: 1 },
          hlCreditCost: { textContent: 'Original cost' },
          hlCreditBuyButton: { disabled: false },
          hlCreditPurchasePanel: { hidden: false },
          hlCreditVaultNote: { hidden: true }
        };
        const document = { getElementById: id => elements[id], activeElement: elements.hlCreditAmount };
        function metricHistorySeriesPoints() { return []; }
        function metricHistoryTitle() { return 'History title'; }
        function metricHistorySubtitle() { return 'Normal subtitle'; }
        function hlCreditLimitReached() { return true; }
        function canBuyHlCredits() { return true; }
        function renderHlRateLimitHistoryMeta() { return 'Fresh metadata'; }
        function renderHlRateLimitHistoryChart() { return 'Fresh chart'; }
        const logged = [];
        console.error = (...args) => logged.push(args);
        const timers = new Map();
        let timerId = 0;
        function setTimeout(callback, delay) {
          assert.equal(delay, 15000);
          timers.set(++timerId, callback);
          return timerId;
        }
        function clearTimeout(id) { timers.delete(id); }
        function expire() {
          assert.equal(timers.size, 1);
          const [id, callback] = timers.entries().next().value;
          timers.delete(id);
          callback();
        }
        class AbortController {
          constructor() {
            const listeners = new Set();
            this.signal = {
              aborted: false, listeners,
              addEventListener: (type, fn) => { assert.equal(type, 'abort'); listeners.add(fn); },
              removeEventListener: (type, fn) => listeners.delete(fn)
            };
          }
          abort() {
            if (this.signal.aborted) return;
            this.signal.aborted = true;
            for (const fn of [...this.signal.listeners]) fn();
          }
        }
        function deferred() {
          let resolve, reject;
          const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
          return { promise, resolve, reject };
        }
        let fetchImpl;
        const requests = [];
        function fetch(url, options) {
          assert.equal(url, API_BASE + '/user-rate-limits/history/test-bot');
          assert.equal(options.credentials, 'same-origin');
          assert.equal(options.cache, 'no-store');
          requests.push(options);
          return fetchImpl();
        }
        async function flush() { for (let i = 0; i < 12; i++) await Promise.resolve(); }
        function assertPreserved() {
          assert.equal(cpuHistoryLastPayload, originalContext);
          assert.equal(metricHistoryCache[cacheKey], cached);
          assert.equal(elements.cpuHistoryBody.innerHTML, 'Original chart');
          assert.equal(elements.cpuHistoryMeta.innerHTML, 'Original metadata');
          assert.equal(elements.cpuHistoryTitle.textContent, 'Original title');
          assert.equal(elements.hlCreditAmount.value, '123');
          assert.equal(elements.hlCreditAmount.selectionStart, 1);
          assert.equal(document.activeElement, elements.hlCreditAmount);
          assert.equal(elements.hlCreditCost.textContent, 'Original cost');
          assert.equal(elements.hlCreditPurchasePanel.hidden, false);
          assert.equal(elements.hlCreditBuyButton.disabled, false);
        }
        function assertClean(controller) {
          assert.equal(timers.size, 0);
          assert.equal(controller.signal.listeners.size, 0);
          assert.equal(cpuHistoryAbortController, null);
        }
        """,
        assertions="(async () => {\n" + assertions + "\n})().catch(error => { process.stderr.write(error.stack); process.exitCode = 1; });",
    )


def _run_master_detail_assertions(assertions: str) -> None:
    """Exercise detail requests with controlled responses and unchanged edit state."""
    _run_node_assertions(
        ["fetchMasterDetail", "isMasterContextView"],
        bootstrap=r"""
        const API_BASE = '/api/vps-manager';
        const original = {kind:'master', marker:'last loaded'};
        const store = {
          view:'master', hostname:'', contextGeneration:1, detailRequestGeneration:0,
          masterDetailErrorContextGeneration:null, detail:original,
          detailCache:{master:original}, master:{draft:'unsaved'}, selection:['fixture']
        };
        const requests = [], logs = [], toasts = [], renders = [], ensured = [];
        let handlerFailure = '';
        const console = {error:(...args)=>logs.push(args)};
        function fetch(path, options) {
          let resolve, reject;
          const promise = new Promise((ok, fail)=>{resolve=ok; reject=fail;});
          requests.push({path, options, resolve, reject});
          return promise;
        }
        function toast(...args) {toasts.push(args);}
        function ensureMasterUi(data) {
          if (handlerFailure === 'ensure') throw new Error('Synthetic ensure failure');
          ensured.push(data);
        }
        function renderUi(options) {
          if (handlerFailure === 'render') throw new Error('Synthetic render failure');
          renders.push(options);
        }
        function success(request, data={kind:'master', marker:'fresh'}) {
          request.resolve({ok:true, json:()=>Promise.resolve(data)});
        }
        function unchanged() {
          assert.equal(store.detail, original);
          assert.equal(store.detailCache.master, original);
          assert.deepEqual(store.master,{draft:'unsaved'});
          assert.deepEqual(store.selection,['fixture']);
          assert.equal(store.view,'master');
          assert.equal(store.hostname,'');
          assert.equal(renders.length,0);
          assert.equal(ensured.length,0);
        }
        """,
        assertions="(async () => {\n" + assertions
        + "\n})().catch(error => {process.stderr.write(String(error.stack)); process.exitCode=1;});",
    )


@pytest.mark.parametrize("failure", ["network", "http500", "invalid_json", "ensure", "render"])
def test_master_detail_reports_current_errors(failure: str) -> None:
    """Fetch, parsing and success-handler errors reach the console and notification."""
    _run_master_detail_assertions("const failure = " + json.dumps(failure) + ";\n" + r"""
        const pending = fetchMasterDetail();
        assert.equal(requests[0].path,'/api/vps-manager/detail-master');
        assert.equal(requests[0].options.credentials,'same-origin');
        assert.equal(requests[0].options.cache,'no-store');
        if (failure === 'network') requests[0].reject(new TypeError('Synthetic offline'));
        else if (failure === 'http500') requests[0].resolve({ok:false,status:500});
        else if (failure === 'invalid_json') requests[0].resolve({
          ok:true,json:()=>Promise.reject(new SyntaxError('Synthetic invalid JSON'))
        });
        else {handlerFailure=failure; success(requests[0]);}
        await pending;
        assert.equal(logs.length,1);
        assert.equal(logs[0][0],'Failed to load master details:');
        assert.ok(logs[0][1] instanceof Error);
        assert.deepEqual(toasts,[['Failed to load master details. Please try again.',
          'error',{preserveOpenDialog:true}]]);
        assert.equal(store.masterDetailErrorContextGeneration,1);
        if (!['ensure','render'].includes(failure)) unchanged();
    """)


@pytest.mark.parametrize("reason,response", [
    ("abort", "failure"), ("request", "failure"), ("request", "success"),
    ("context", "failure"), ("context", "success"),
    ("view", "failure"), ("view", "success"),
])
def test_master_detail_ignores_cancelled_or_obsolete_requests(reason: str, response: str) -> None:
    """Obsolete callbacks cannot log, render, notify or reset a newer error phase."""
    _run_master_detail_assertions("const reason = " + json.dumps(reason)
        + "; const response = " + json.dumps(response) + ";\n" + r"""
        store.masterDetailErrorContextGeneration = 91;
        const pending = fetchMasterDetail();
        if (reason === 'request') {
          fetchMasterDetail();
          assert.equal(requests[0].options.signal.aborted,true);
        }
        if (reason === 'context') store.contextGeneration++;
        if (reason === 'view') store.view='overview';
        if (response === 'success') success(requests[0]);
        else requests[0].reject(reason === 'abort'
          ? new DOMException('Synthetic cancellation','AbortError') : new Error('Synthetic failure'));
        await pending;
        assert.equal(logs.length,0);
        assert.equal(toasts.length,0);
        assert.equal(store.masterDetailErrorContextGeneration,91);
        if (reason === 'view') store.view='master';
        unchanged();
    """)


def test_master_detail_deduplicates_until_valid_recovery_or_context_change() -> None:
    """Only current validated detail success resets notification suppression."""
    _run_master_detail_assertions(r"""
        async function fail() {
          const pending=fetchMasterDetail();
          requests.at(-1).reject(new Error('Synthetic offline')); await pending;
        }
        await fail(); await fail();
        assert.equal(logs.length,2);
        assert.equal(toasts.length,1);
        unchanged();
        for (const invalid of [null, {}, {kind:'vps'}]) {
          const pending=fetchMasterDetail(); success(requests.at(-1),invalid); await pending;
          assert.equal(store.masterDetailErrorContextGeneration,1);
        }
        const obsolete=fetchMasterDetail(), oldRequest=requests.at(-1);
        await fail();
        success(oldRequest); await obsolete;
        assert.equal(store.masterDetailErrorContextGeneration,1);
        unchanged();
        const fresh={kind:'master',marker:'recovered'};
        const recovered=fetchMasterDetail(); success(requests.at(-1),fresh); await recovered;
        assert.equal(store.masterDetailErrorContextGeneration,null);
        assert.equal(store.detail,fresh);
        assert.equal(store.detailCache.master,fresh);
        assert.deepEqual(ensured,[fresh]);
        assert.deepEqual(renders,[undefined]);
        await fail();
        assert.equal(toasts.length,2);
        store.contextGeneration++;
        await fail();
        assert.equal(toasts.length,3);
        assert.equal(store.masterDetailErrorContextGeneration,2);
        assert.equal(logs.length,5);
    """)


def test_master_detail_first_success_forces_view_render() -> None:
    """Initial valid data populates detail and cache and replaces the loading view."""
    _run_master_detail_assertions(r"""
        store.detail=null;
        const pending=fetchMasterDetail(); success(requests[0]); await pending;
        assert.equal(store.detail.marker,'fresh');
        assert.equal(store.detailCache.master,store.detail);
        assert.deepEqual(ensured,[store.detail]);
        assert.deepEqual(renders,[{forceView:true}]);
        assert.equal(toasts.length,0);
        assert.equal(logs.length,0);
    """)


@pytest.mark.parametrize("kind", ["error", "warning"])
@pytest.mark.parametrize("protected_dialog", [True, False])
def test_master_detail_toast_preserves_real_sudo_dialog(kind: str, protected_dialog: bool) -> None:
    """Real toast/dialog handlers preserve sudo content, callbacks, focus and timers."""
    _run_node_assertions(
        ["toast", "openAlertModal", "openMasterPasswordPrompt", "promptForMasterSudoAction",
         "runPendingLocalSudoAction", "fetchMasterDetail", "isMasterContextView"],
        bootstrap="const kind = " + json.dumps(kind) + "; const protectedDialog = "
        + json.dumps(protected_dialog) + ";\n" + r"""
        const API_BASE='/api/vps-manager';
        const store={master:{},pendingOverviewDeploy:{marker:'pending overview'},
          contextGeneration:1,detailRequestGeneration:0,detailCache:{},view:'master'};
        const elements={}, timers=[], posts=[], logs=[], runs=[];
        let resets=0;
        function element(id) {
          const classes=new Set();
          return {id,style:{},children:[],className:'',textContent:'',value:'',listeners:{},
            classList:{add:x=>classes.add(x),contains:x=>classes.has(x)},
            appendChild(node) {this.children.push(node); node.parent=this;},
            remove() {this.parent.children=this.parent.children.filter(node=>node!==this);},
            focus() {document.activeElement=this;},
            addEventListener(type,callback) {this.listeners[type]=callback;},
            set innerHTML(value) {
              for (const child of this.children) delete elements[child.id];
              this.children=[]; this.html=value;
              for (const match of value.matchAll(/id=['"]([^'"]+)['"]/g)) {
                const child=element(match[1]); elements[child.id]=child; this.children.push(child);
              }
            },
            get innerHTML() {return this.html;}
          };
        }
        const document={activeElement:null,getElementById:id=>elements[id]||null,
          createElement:()=>element('')};
        for (const id of ['toastContainer','dialogToastContainer','alertModalOverlay',
          'alertModalTitle','alertModalBody','alertModalBtnRow']) elements[id]=element(id);
        const console={error:(...args)=>logs.push(args)};
        const setTimeout=(callback,delay)=>{timers.push({callback,delay}); return timers.length;};
        function fetch(path,options) {
          if (path.endsWith('/detail-master')) return Promise.reject(new Error('Synthetic offline'));
          posts.push({path,options}); return Promise.resolve({ok:true});
        }
        function resetAlertModalWindow() {resets++;}
        function isClusterOnboardModalVisible() {return false;}
        function isClusterOnboardRunning() {return false;}
        function esc(value) {return String(value);}
        function closeAlertModal() {throw new Error('Must not close the dialog');}
        function runMasterWithLog(...args) {runs.push(args);}
        function ensureMasterUi() {throw new Error('Must not update data');}
        function renderUi() {throw new Error('Must not render');}
        """,
        assertions=r"""
        (async () => {
          promptForMasterSudoAction('fixture-command','Fixture action',{fixture:true});
          timers.shift().callback(); // The actual prompt's delayed focus.
          const input=elements['master-pw-prompt-input']; input.value='synthetic-test-password';
          const pending=store.pendingLocalSudoAction, overview=store.pendingOverviewDeploy;
          const body=elements.alertModalBody.innerHTML, buttons=elements.alertModalBtnRow.innerHTML;
          const confirm=elements['master-pw-prompt-confirm'];
          const confirmAction=confirm.listeners.click, keyAction=input.listeners.keydown;
          if (protectedDialog && kind === 'error') await fetchMasterDetail();
          else toast('Synthetic failure',kind,protectedDialog ? {preserveOpenDialog:true} : undefined);
          assert.equal(posts.length,1);
          assert.equal(posts[0].path,'/api/notify_log');
          assert.equal(posts[0].options.method,'POST');
          assert.equal(posts[0].options.credentials,'same-origin');
          assert.equal(JSON.parse(posts[0].options.body).level,kind);
          assert.equal(store.pendingLocalSudoAction,pending);
          assert.equal(store.pendingOverviewDeploy,overview);
          assert.equal(runs.length,0);
          const layer=elements[protectedDialog ? 'dialogToastContainer' : 'toastContainer'];
          assert.equal(layer.children.length,1);
          assert.equal(elements[protectedDialog ? 'toastContainer' : 'dialogToastContainer'].children.length,0);
          const node=layer.children[0];
          assert.equal(node.className,'toast '+kind);
          if (protectedDialog) {
            assert.equal(resets,0);
            assert.equal(elements.alertModalTitle.textContent,'Master sudo password required');
            assert.equal(elements.alertModalBody.innerHTML,body);
            assert.equal(elements.alertModalBtnRow.innerHTML,buttons);
            assert.equal(elements['master-pw-prompt-input'],input);
            assert.equal(elements['master-pw-prompt-confirm'],confirm);
            assert.equal(input.value,'synthetic-test-password');
            assert.equal(document.activeElement,input);
            assert.equal(confirm.listeners.click,confirmAction);
            assert.equal(input.listeners.keydown,keyAction);
            if (kind === 'error') {
              assert.equal(logs.length,1);
              assert.equal(node.textContent,'Failed to load master details. Please try again.');
            }
          } else {
            assert.equal(resets,1);
            assert.equal(elements.alertModalTitle.textContent,kind === 'error' ? 'Error' : 'Warning');
            assert.match(elements.alertModalBtnRow.innerHTML,/>Close<\/button>/);
          }
          assert.equal(timers[0].delay,kind === 'error' ? 4200 : 2600);
          timers.shift().callback();
          assert.equal(node.style.opacity,'0');
          assert.equal(timers[0].delay,220);
          timers.shift().callback();
          assert.equal(layer.children.length,0);
          assert.equal(timers.length,0);
          if (protectedDialog) assert.equal(document.activeElement,input);
          else {
            // The option retains the normal error modal when no dialog is open.
            elements.alertModalOverlay=element('alertModalOverlay');
            toast('Closed dialog failure',kind,{preserveOpenDialog:true});
            assert.equal(resets,2);
            assert.equal(elements.toastContainer.children.length,1);
            assert.equal(elements.dialogToastContainer.children.length,0);
          }
        })().catch(error=>{process.stderr.write(String(error.stack)); process.exitCode=1;});
        """,
    )


def test_master_detail_toast_layer_and_menu_delivery_contract() -> None:
    """The opt-in layer clears the modal and pointer rules; the menu serves uncached HTML."""
    source = HTML_PATH.read_text(encoding="utf-8")
    layer = source.rsplit("#dialogToastContainer {", 1)[1].split("}", 1)[0]
    assert "z-index: 6500" in layer
    common = source.split("#toastContainer,", 1)[1].split("}", 1)[0]
    assert "position: fixed" in common
    assert "pointer-events: none" in common
    child = source.split("#dialogToastContainer .toast {", 1)[1].split("}", 1)[0]
    assert "pointer-events: none" in child
    assert "pointer-events: auto" in source.split("    .toast {", 1)[1].split("}", 1)[0]
    assert "<div id='dialogToastContainer' role='status' aria-live='polite'></div>" in source
    assert source.index("<div id='toastContainer'></div>") < source.index("<div id='dialogToastContainer'")
    assert source.index("<div id='dialogToastContainer'") < source.index("<div id='alertModalOverlay'>")
    nav = (ROOT / "frontend" / "pbgui_nav.js").read_text(encoding="utf-8")
    assert "{ id: 'system', label: 'System'" in nav
    assert "page: 'system_vps_manager_fastapi', icon: '&#128421;', label: 'VPS Manager'" in nav
    assert "'system_vps_manager_fastapi': '/api/vps-manager/main_page'" in nav
    route = (ROOT / "api" / "vps_manager.py").read_text(encoding="utf-8")
    page = route.split('html_path = ', 1)[1].split('@router.get(', 1)[0]
    assert '"vps_manager.html"' in page
    assert 'headers={"Cache-Control": "no-store"}' in page


def _run_vps_detail_assertions(assertions: str) -> None:
    """Run current VPS requests and navigation with isolated deferred HTTP responses."""
    _run_node_assertions(
        ['fetchVpsDetail', 'isVpsContextView', 'isMasterContextView', 'selectView',
         'detailContextKey', 'viewUrl', 'viewHash', 'openVpsHostLogs', 'restoreViewFromHash'],
        bootstrap=r'''
        const API_BASE='/api/vps-manager';
        const original={kind:'vps',hostname:'test-host',marker:'cached'};
        const store={view:'vps',hostname:'test-host',contextGeneration:1,detailRequestGeneration:0,
          vpsDetailErrorHost:null,detail:original,detailCache:{'vps:test-host':original},
          vps:{'test-host':{draft:'unsaved'}},selection:['test-host']};
        const requests=[],logs=[],toasts=[],renders=[],ensured=[],contexts=[],scheduled=[];
        const console={error:(...args)=>logs.push(args)};
        const location={pathname:'/api/vps-manager/main_page',search:'',hash:'#vps/test-host'};
        const window={location};
        const history={replaceState:(a,b,url)=>{location.hash=url.slice(url.indexOf('#'));}};
        function fetch(path,options) {
          let resolve,reject; const promise=new Promise((ok,fail)=>{resolve=ok;reject=fail;});
          requests.push({path,options,resolve,reject}); return promise;
        }
        function toast(...args) {toasts.push(args);}
        function renderUi(options) {renders.push(options);}
        function ensureVpsUi(host,data) {ensured.push({host,data});}
        function sendContext() {contexts.push([store.view,store.hostname]);}
        function defaultTaskLog() {return {};}
        function buildProvisionalVpsDetail() {return null;}
        function scheduleVpsDetailFetch(host) {scheduled.push(host);}
        function scheduleMasterDetailFetch() {}
        function success(request,data={kind:'vps',hostname:'test-host',marker:'fresh'}) {
          request.resolve({ok:true,json:()=>Promise.resolve(data)});
        }
        function unchanged() {
          assert.equal(store.detail,original); assert.equal(store.detailCache['vps:test-host'],original);
          assert.deepEqual(store.vps,{'test-host':{draft:'unsaved'}});
          assert.deepEqual(store.selection,['test-host']);
          assert.equal(renders.length,0); assert.equal(ensured.length,0);
        }
        async function fail() {
          const pending=fetchVpsDetail(store.hostname);
          requests.at(-1).reject(new TypeError('Synthetic sensitive diagnostic')); await pending;
        }
        ''',
        assertions='(async()=>{\n' + assertions
        + '\n})().catch(error=>{process.stderr.write(String(error.stack));process.exitCode=1;});',
    )


@pytest.mark.parametrize('failure', ['network', 'http500', 'invalid_json'])
def test_vps_detail_reports_current_failures(failure: str) -> None:
    """A genuine failure logs controlled text and preserves cache, selection and drafts."""
    _run_vps_detail_assertions('const failure=' + json.dumps(failure) + ';\n' + r'''
        const pending=fetchVpsDetail('test-host');
        assert.equal(requests[0].path,'/api/vps-manager/detail/test-host');
        assert.equal(requests[0].options.credentials,'same-origin');
        assert.equal(requests[0].options.cache,'no-store');
        if (failure==='network') requests[0].reject(new TypeError('Sensitive fixture diagnostic'));
        if (failure==='http500') requests[0].resolve({ok:false,status:500});
        if (failure==='invalid_json') requests[0].resolve({ok:true,json:()=>Promise.reject(new SyntaxError('Sensitive fixture body'))});
        await pending;
        assert.deepEqual(logs,[['Failed to load VPS details.']]);
        assert.deepEqual(toasts,[['Failed to load VPS details. Please try again.','error',{preserveOpenDialog:true}]]);
        assert.equal(store.vpsDetailErrorHost,'test-host'); unchanged();
    ''')


@pytest.mark.parametrize('reason,response', [('abort', 'failure')] + [
    (reason, response) for reason in ['request', 'context', 'host', 'view']
    for response in ['failure', 'http500', 'http404', 'success', 'json']
])
def test_vps_detail_ignores_obsolete_requests(reason: str, response: str) -> None:
    """Late headers, JSON and failures cannot affect another host, context or request."""
    _run_vps_detail_assertions('const reason=' + json.dumps(reason)
        + ', response=' + json.dumps(response) + ';\n' + r'''
        store.vpsDetailErrorHost='existing-phase';
        const pending=fetchVpsDetail('test-host');
        let resolveJson;
        if (response==='json') {
          requests[0].resolve({ok:true,json:()=>new Promise(resolve=>{resolveJson=resolve;})});
          await Promise.resolve(); await Promise.resolve();
        }
        if (reason==='request') {fetchVpsDetail('test-host'); assert.ok(requests[0].options.signal.aborted);}
        if (reason==='context') store.contextGeneration++;
        if (reason==='host') store.hostname='test-other';
        if (reason==='view') store.view='overview';
        if (response==='failure') requests[0].reject(reason==='abort'
          ? new DOMException('Synthetic abort','AbortError') : new Error('Synthetic failure'));
        if (response==='http500' || response==='http404') requests[0].resolve({ok:false,status:response==='http404'?404:500});
        if (response==='success') success(requests[0]);
        if (response==='json') resolveJson({kind:'vps',hostname:'test-host'});
        await pending;
        assert.equal(logs.length,0); assert.equal(toasts.length,0);
        assert.equal(store.vpsDetailErrorHost,'existing-phase');
        assert.equal(contexts.length,0); assert.equal(location.hash,'#vps/test-host'); unchanged();
    ''')


def test_vps_detail_deduplicates_and_recovers() -> None:
    """Only a valid current success or host-context change starts another error phase."""
    _run_vps_detail_assertions(r'''
        await fail(); await fail(); assert.equal(toasts.length,1); unchanged();
        for (const data of [null,{}, {kind:'master'}, {kind:'vps',hostname:'wrong'}]) {
          const pending=fetchVpsDetail('test-host'); success(requests.at(-1),data); await pending;
          assert.equal(store.vpsDetailErrorHost,'test-host');
        }
        const pending=fetchVpsDetail('test-host'); success(requests.at(-1)); await pending;
        assert.equal(store.vpsDetailErrorHost,null); assert.equal(store.detail.marker,'fresh');
        assert.equal(store.detailCache['vps:test-host'],store.detail);
        assert.deepEqual(renders,[undefined]);
        await fail(); assert.equal(toasts.length,2);
        selectView('vps-setup','test-host');
        assert.equal(store.vpsDetailErrorHost,'test-host');
        await fail(); assert.equal(toasts.length,2);
        selectView('overview',''); selectView('vps','test-host');
        assert.equal(store.vpsDetailErrorHost,null);
        await fail(); assert.equal(toasts.length,3);
        openVpsHostLogs('test-other');
        assert.equal(store.vpsDetailErrorHost,null); assert.equal(store.hostname,'test-other');
        assert.equal(store.view,'vps-host-logs');
        requests.at(-1).reject(new Error('Synthetic failure'));
        await new Promise(resolve=>setImmediate(resolve));
        assert.equal(toasts.length,4); assert.equal(store.vpsDetailErrorHost,'test-other');
        assert.deepEqual(store.vps,{'test-host':{draft:'unsaved'}});
    ''')


def test_vps_detail_404_navigates_and_restores_overview() -> None:
    """A current missing host uses the normal navigation path and survives reload."""
    _run_vps_detail_assertions(r'''
        const pending=fetchVpsDetail('test-host'); requests[0].resolve({ok:false,status:404}); await pending;
        assert.deepEqual(toasts,[['This VPS is no longer available. Returning to Overview.','warning',{preserveOpenDialog:true}]]);
        assert.equal(store.view,'overview'); assert.equal(store.hostname,''); assert.equal(store.detail,null);
        assert.equal(location.hash,'#overview'); assert.deepEqual(contexts,[['overview','']]);
        assert.equal(store.detailCache['vps:test-host'],original);
        store.view='overview'; store.hostname=''; restoreViewFromHash();
        assert.equal(store.view,'overview'); assert.equal(store.hostname,'');
        assert.equal(logs.length,0);
    ''')


def test_vps_detail_host_logs_switch_ignores_old_404() -> None:
    """Direct host-log navigation invalidates the former host's pending missing response."""
    _run_vps_detail_assertions(r'''
        const old=fetchVpsDetail('test-host');
        store.vpsDetailErrorHost='test-host';
        openVpsHostLogs('test-other');
        assert.equal(requests[0].options.signal.aborted,true);
        requests[0].resolve({ok:false,status:404}); await old;
        assert.equal(store.view,'vps-host-logs'); assert.equal(store.hostname,'test-other');
        assert.equal(store.vpsDetailErrorHost,null);
        assert.equal(toasts.length,0); assert.equal(logs.length,0);
        assert.deepEqual(contexts,[['vps-host-logs','test-other']]);
        assert.equal(store.detailCache['vps:test-host'],original);
    ''')


def _run_cluster_onboard_assertions(assertions: str) -> None:
    """Exercise real onboarding handlers/renderers with deferred offline requests."""
    _run_node_assertions(
        ["stopClusterOnboardProgressPolling", "isClusterOnboardRunning",
         "isClusterOnboardModalVisible", "renderClusterOnboardProgressModal",
         "isCurrentClusterOnboard", "pollClusterOnboardProgress", "startClusterOnboard",
         "resumeClusterOnboardProgress", "addVpsToCluster", "openConfirmModal"],
        bootstrap=r"""
        const store = { clusterOnboard: {generation:0, resumeChecked:{}} };
        const posts = [], gets = [], timers = [], cleared = [], toasts = [], details = [];
        const deferred = () => {
          let resolve, reject;
          const promise = new Promise((ok, fail) => {resolve=ok; reject=fail;});
          return {promise, resolve, reject};
        };
        const request = (list, path) => {
          const item = {...deferred(), path}; list.push(item); return item.promise;
        };
        const vpsManagerPost = path => request(posts, path);
        const vpsManagerGet = path => request(gets, path);
        const settle = async () => { for (let i=0; i<6; i++) await Promise.resolve(); };
        const window = {
          innerWidth:1200, innerHeight:900,
          setTimeout(callback, delay) {timers.push({callback,delay}); return timers.length;},
          clearTimeout(id) {cleared.push(id);},
          requestAnimationFrame(callback) {callback();}
        };
        const elements = {};
        for (const id of ['alertModalTitle','alertModalBody','alertModalOverlay',
                          'cluster-onboard-progress-log','close']) elements[id] = {style:{}};
        const visible = new Set();
        elements.alertModalOverlay.classList = {
          add:value=>visible.add(value), contains:value=>visible.has(value)
        };
        elements.alertModalBtnRow = {
          set innerHTML(value) {
            this.html = value;
            delete elements['cluster-onboard-retry']; delete elements['confirm-modal-ok'];
            for (const id of ['cluster-onboard-retry','confirm-modal-ok']) {
              if (value.includes("id='"+id+"'")) elements[id] = {
                onclick:null, addEventListener(type, callback) {this[type]=callback;}
              };
            }
          },
          get innerHTML() {return this.html;}
        };
        const document = {
          getElementById:id=>elements[id] || null,
          querySelector:selector=>selector === '#alertModalHead .close-btn' ? elements.close : null
        };
        const esc = value => String(value).replaceAll('&','&amp;').replaceAll('<','&lt;')
          .replaceAll('>','&gt;').replaceAll("'",'&#39;').replaceAll('"','&quot;');
        const escAttr = esc;
        const clusterImportProgressClass = status => status;
        const prepareAlertModalWindow = () => {};
        const closeAlertModal = () => visible.delete('visible');
        const toast = (...args) => toasts.push(args);
        const scheduleVpsDetailFetch = host => details.push(host);
        const assertLocked = () => {
          assert.match(elements.alertModalBtnRow.innerHTML, /<button[^>]* disabled>Onboarding in progress/);
          assert.equal(document.getElementById('cluster-onboard-retry'),null);
          assert.equal(elements.close.style.display,'none');
        };
        """,
        assertions="(async () => {\n" + assertions
        + "\n})().catch(error => {console.error(error); process.exitCode=1;});",
    )


@pytest.mark.parametrize("failure", ["request", "missing", "empty", "blank", "non_string"])
def test_cluster_onboard_start_and_retry_lock(failure: str) -> None:
    """Repeated Confirm/Retry and other hosts cannot reset queued/running work."""
    _run_cluster_onboard_assertions("const failure = " + json.dumps(failure) + ";\n" + r"""
        const checked = store.clusterOnboard.resumeChecked;
        checked['fixture-node'] = true;
        addVpsToCluster('fixture-node');
        assert.equal(elements.alertModalTitle.textContent,'Add to Cluster');
        assert.match(elements.alertModalBtnRow.innerHTML, />Confirm<\/button>/);
        const confirm = elements['confirm-modal-ok'].click;
        confirm();
        const queued = store.clusterOnboard;
        for (let i=0; i<5; i++) {
          confirm(); startClusterOnboard('other-fixture'); addVpsToCluster('other-fixture');
        }
        assert.equal(posts.length,1);
        assert.equal(store.clusterOnboard,queued);
        assert.equal(queued.resumeChecked,checked);
        assert.equal(cleared.length,0);
        assertLocked();
        assert.match(posts[0].path,/\/fixture-node\/start$/);
        if (failure === 'request') posts[0].reject(new Error('Synthetic start failure'));
        else posts[0].resolve(failure === 'missing' ? {} : {
          job_id: {empty:'',blank:'  ',non_string:42}[failure], status:'queued'
        });
        await settle();
        assert.equal(store.clusterOnboard.status,'error');
        assert.match(elements.alertModalBody.innerHTML,
          failure === 'request' ? /Synthetic start failure/ : /no valid job ID/);
        assert.equal(gets.length,0);
        assert.equal(posts.length,1); // No automatic POST retry.
        const retry = elements['cluster-onboard-retry'].onclick;
        retry();
        const retryFlow = store.clusterOnboard;
        for (let i=0; i<5; i++) { retry(); startClusterOnboard('other-fixture'); }
        assert.equal(posts.length,2);
        assert.equal(store.clusterOnboard,retryFlow);
        assert.equal(retryFlow.resumeChecked,checked);
        assertLocked();
        posts[1].resolve({job_id:'fixture-job',status:'queued'});
        await settle();
        assert.equal(gets.length,1);
        gets[0].resolve({status:'running',percent:36,label:'Synthetic progress'});
        await settle();
        const flow = store.clusterOnboard, timer = flow.progressTimer;
        const clearedBefore = cleared.length;
        assert.equal(timers[timer-1].delay,500);
        for (let i=0; i<5; i++) {
          retry(); startClusterOnboard('other-fixture'); addVpsToCluster('other-fixture');
          resumeClusterOnboardProgress('other-fixture');
        }
        assert.equal(store.clusterOnboard,flow);
        assert.equal(flow.progressTimer,timer);
        assert.equal(cleared.length,clearedBefore);
        assert.equal(posts.length,2);
        assert.equal(gets.length,1);
        assertLocked();
        assert.match(elements.alertModalBody.innerHTML,/36%/);
        timers[timer-1].callback();
        assert.equal(gets.length,2);
        gets[1].resolve({status:'successful',percent:100});
        await settle();
        assert.equal(flow.status,'successful');
        assert.match(elements.alertModalBtnRow.innerHTML,/>Close<\/button>/);
        assert.equal(elements['cluster-onboard-retry'],undefined);
        assert.equal(elements.close.style.display,'');
        assert.deepEqual(details,['fixture-node']);
        assert.equal(toasts.length,1);
    """)


def test_cluster_onboard_job_failure_allows_explicit_retry() -> None:
    """A terminal job error shows Retry and only an explicit click sends a POST."""
    _run_cluster_onboard_assertions(r"""
        startClusterOnboard('fixture-node');
        posts[0].resolve({job_id:'failed-job',status:'running'});
        await settle();
        gets[0].resolve({status:'error',label:'Synthetic job failure',error:'Fixture failure'});
        await settle();
        assert.equal(store.clusterOnboard.status,'error');
        assert.match(elements.alertModalBody.innerHTML,/Fixture failure/);
        assert.equal(elements.close.style.display,'');
        assert.equal(timers.length,0);
        assert.equal(toasts.length,0);
        assert.equal(details.length,0);
        assert.equal(posts.length,1);
        const retry = elements['cluster-onboard-retry'].onclick;
        for (let i=0; i<5; i++) retry();
        assert.equal(posts.length,2);
        assert.match(posts[1].path,/\/fixture-node\/start$/);
        assert.equal(store.clusterOnboard.jobId,'');
        assertLocked();
    """)


@pytest.mark.parametrize("response", ["success", "error"])
def test_cluster_onboard_ignores_stale_start(response: str) -> None:
    """A superseded start response cannot overwrite or poll a newer generation."""
    _run_cluster_onboard_assertions("const response = " + json.dumps(response) + ";\n" + r"""
        startClusterOnboard('fixture-node');
        const old = store.clusterOnboard;
        // Model a replacement flow, including reuse of the same host.
        store.clusterOnboard = {...old,status:'error'};
        startClusterOnboard('fixture-node');
        const current = store.clusterOnboard;
        const html = elements.alertModalBody.innerHTML;
        if (response === 'success') posts[0].resolve({job_id:'obsolete',status:'queued'});
        else posts[0].reject(new Error('obsolete failure'));
        await settle();
        assert.equal(store.clusterOnboard,current);
        assert.equal(current.status,'queued');
        assert.equal(current.jobId,'');
        assert.equal(elements.alertModalBody.innerHTML,html);
        assert.equal(gets.length,0);
        assertLocked();
        posts[1].resolve({job_id:'current',status:'running'});
        await settle();
        assert.equal(current.jobId,'current');
        assert.equal(gets.length,1);
    """)


@pytest.mark.parametrize("target", ["fixture-node", "other-fixture"])
def test_cluster_onboard_resume_cannot_replace_start(target: str) -> None:
    """Resume requested before Start stays obsolete and resumeChecked survives."""
    _run_cluster_onboard_assertions("const target = " + json.dumps(target) + ";\n" + r"""
        store.clusterOnboard.resumeChecked['checked-fixture'] = true;
        resumeClusterOnboardProgress('fixture-node');
        const checked = store.clusterOnboard.resumeChecked;
        startClusterOnboard(target);
        const current = store.clusterOnboard;
        gets[0].resolve({active:true,job_id:'obsolete',status:'running'});
        await settle();
        assert.equal(store.clusterOnboard,current);
        assert.equal(current.hostname,target);
        assert.equal(current.jobId,'');
        assert.equal(current.resumeChecked,checked);
        assert.equal(checked['checked-fixture'],true);
        assert.equal(checked['fixture-node'],undefined); // A superseded lookup never completed.
        assert.equal(current.resumeAttempt,null);
        assert.equal(gets.length,1);
        assertLocked();
        posts[0].resolve({job_id:'current',status:'running'});
        await settle();
        assert.equal(current.jobId,'current');
        assert.equal(gets.length,2);
    """)


@pytest.mark.parametrize("response", ["active", "inactive", "error"])
@pytest.mark.parametrize("arrival", ["before_return", "after_return"])
def test_cluster_onboard_rechecks_superseded_resume_on_return(response: str, arrival: str) -> None:
    """Returning A/B/A rechecks A, and obsolete replies cannot complete its new lookup."""
    _run_cluster_onboard_assertions(
        "const response = " + json.dumps(response) + "; const arrival = " + json.dumps(arrival) + ";\n" + r"""
        const checked = store.clusterOnboard.resumeChecked;
        checked['checked-fixture'] = true;
        resumeClusterOnboardProgress('fixture-a');
        resumeClusterOnboardProgress('fixture-a');
        assert.equal(gets.length,1); // Detail updates share the pending lookup.
        resumeClusterOnboardProgress('fixture-b');
        gets[1].resolve({active:false,hostname:'fixture-b'});
        await settle();
        assert.equal(checked['fixture-b'],true);
        const finishObsolete = async () => {
          if (response === 'error') gets[0].reject(new Error('Obsolete lookup failure'));
          else gets[0].resolve({active:response === 'active',hostname:'fixture-a',
            job_id:'obsolete-a',status:'running'});
          await settle();
        };
        if (arrival === 'before_return') await finishObsolete();
        resumeClusterOnboardProgress('fixture-a');
        assert.equal(gets.length,3,'Returning to A must recheck its active job');
        const flow = store.clusterOnboard, attempt = flow.resumeAttempt;
        const snapshot = JSON.stringify(flow);
        if (arrival === 'after_return') await finishObsolete();
        assert.equal(JSON.stringify(flow),snapshot);
        assert.equal(flow.resumeAttempt,attempt);
        assert.equal(checked['fixture-a'],undefined);
        resumeClusterOnboardProgress('fixture-a');
        assert.equal(gets.length,3);
        assert.equal(timers.length,0);
        assert.equal(isClusterOnboardModalVisible(),false);
        gets[2].resolve({active:true,hostname:'fixture-a',job_id:'current-a',
          status:'running',percent:47});
        await settle();
        assert.equal(flow.jobId,'current-a');
        assert.equal(flow.hostname,'fixture-a');
        assert.equal(flow.resumeChecked,checked);
        assert.equal(checked['checked-fixture'],true);
        assert.equal(checked['fixture-a'],true);
        assert.equal(flow.resumeAttempt,null);
        assert.equal(gets.length,4); // Only the current job is polled.
        assert.match(gets[3].path,/\/jobs\/current-a$/);
        assert.equal(posts.length,0);
        assertLocked();
    """)


def test_cluster_onboard_checked_host_supersedes_pending_resume() -> None:
    """A previously checked destination also invalidates the host being left."""
    _run_cluster_onboard_assertions(r"""
        store.clusterOnboard.resumeChecked['fixture-b'] = true;
        resumeClusterOnboardProgress('fixture-a');
        resumeClusterOnboardProgress('fixture-b');
        assert.equal(gets.length,1); // No redundant lookup for a completed check.
        gets[0].resolve({active:true,job_id:'obsolete-a',status:'running'});
        await settle();
        assert.equal(store.clusterOnboard.jobId || '','');
        assert.equal(isClusterOnboardModalVisible(),false);
        assert.equal(store.clusterOnboard.resumeChecked['fixture-a'],undefined);
        resumeClusterOnboardProgress('fixture-a');
        assert.equal(gets.length,2);
        gets[1].resolve({active:true,job_id:'current-a',status:'running'});
        await settle();
        assert.equal(store.clusterOnboard.jobId,'current-a');
        assert.equal(gets.length,3);
        assert.equal(posts.length,0);
        assertLocked();
    """)


def test_cluster_onboard_latest_resume_and_reload() -> None:
    """Latest host resume wins; a fresh page restores progress without a POST."""
    _run_cluster_onboard_assertions(r"""
        resumeClusterOnboardProgress('old-fixture');
        resumeClusterOnboardProgress('fixture-node');
        gets[1].resolve({active:true,job_id:'current',status:'running',percent:50});
        await settle();
        gets[0].resolve({active:true,job_id:'obsolete',status:'running'});
        await settle();
        assert.equal(store.clusterOnboard.hostname,'fixture-node');
        assert.equal(store.clusterOnboard.jobId,'current');
        assert.equal(gets.length,3);
        assertLocked();
        // A reload creates a fresh frontend store; the API job is still active.
        store.clusterOnboard = {generation:0,resumeChecked:{}};
        resumeClusterOnboardProgress('fixture-node');
        gets[3].resolve({active:true,job_id:'current',status:'running',percent:65});
        await settle();
        assert.equal(posts.length,0);
        assert.equal(store.clusterOnboard.jobId,'current');
        assert.equal(gets.length,5);
        assert.match(elements.alertModalBody.innerHTML,/65%/);
        assertLocked();
    """)


@pytest.mark.parametrize("response", ["success", "error"])
@pytest.mark.parametrize("replacement", ["generation", "host", "job"])
def test_cluster_onboard_ignores_stale_poll(response: str, replacement: str) -> None:
    """Stale polling success/failure cannot mutate state or revive its timers."""
    _run_cluster_onboard_assertions(
        "const response = " + json.dumps(response)
        + "; const replacement = " + json.dumps(replacement) + ";\n" + r"""
        startClusterOnboard('fixture-node');
        posts[0].resolve({job_id:'fixture-job',status:'running'});
        await settle();
        const old = store.clusterOnboard;
        const current = store.clusterOnboard = {...old,
          generation:old.generation + (replacement === 'generation' ? 1 : 0),
          hostname:replacement === 'host' ? 'other-fixture' : old.hostname,
          jobId:replacement === 'job' ? 'other-job' : old.jobId
        };
        const snapshot = JSON.stringify(current), html = elements.alertModalBody.innerHTML;
        if (response === 'success') gets[0].resolve({status:'successful',percent:100});
        else gets[0].reject(new Error('obsolete poll'));
        await settle();
        assert.equal(JSON.stringify(current),snapshot);
        assert.equal(elements.alertModalBody.innerHTML,html);
        assert.equal(timers.length,0);
        assert.equal(toasts.length,0);
        assert.equal(details.length,0);
    """)


@pytest.mark.parametrize("response", ["running", "error"])
def test_cluster_onboard_stale_timer_cannot_poll(response: str) -> None:
    """Both progress and transient-error timers verify ownership before a GET."""
    _run_cluster_onboard_assertions("const response = " + json.dumps(response) + ";\n" + r"""
        startClusterOnboard('fixture-node');
        posts[0].resolve({job_id:'fixture-job',status:'running'});
        await settle();
        if (response === 'running') gets[0].resolve({status:'running',percent:25});
        else gets[0].reject(new Error('transient network failure'));
        await settle();
        const old = store.clusterOnboard, timer = old.progressTimer;
        assert.equal(timers[timer-1].delay,response === 'running' ? 500 : 1000);
        assertLocked();
        // An already-dispatched callback can run even after clearTimeout.
        store.clusterOnboard = {...old,status:'error'};
        startClusterOnboard('fixture-node');
        assert.deepEqual(cleared,[timer]);
        posts[1].resolve({job_id:'fixture-job',status:'running'});
        await settle();
        const current = store.clusterOnboard;
        const snapshot = JSON.stringify(current);
        timers[timer-1].callback();
        assert.equal(gets.length,2);
        assert.equal(JSON.stringify(current),snapshot);
    """)


class TestVpsManagerFrontendLogic:
    """Lock down VPS Manager form behavior against live metadata refreshes."""

    def test_confirm_modal_uses_plain_title_text_and_preserves_message_lines(self) -> None:
        """Text titles must not double escape and multiline messages remain readable."""
        bootstrap = """
        const elements = {
          alertModalTitle: { textContent: '' },
          alertModalBody: { className: '', innerHTML: '' },
          alertModalBtnRow: { innerHTML: '' },
          alertModalOverlay: { classList: { add: function() {} } },
          'confirm-modal-ok': { onclick: null, addEventListener: function() {} }
        };
        const document = { getElementById: function(id) { return elements[id]; } };
        function esc(value) {
          return String(value).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
        }
        function closeAlertModal() {}
        """
        assertions = r"""
        openConfirmModal('PBGui & Linux', 'Replace A & B\nContinue?', function() {});
        assert.equal(elements.alertModalTitle.textContent, 'PBGui & Linux');
        assert.match(elements.alertModalBody.innerHTML, /white-space:pre-line/);
        assert.match(elements.alertModalBody.innerHTML, /Replace A &amp; B\nContinue\?/);
        """
        _run_node_assertions(
            ["openConfirmModal"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_rollout_action_change_persists_silently(self) -> None:
        """Changing the rollout task should save it like the other deploy settings."""
        bootstrap = """
        const saves = [];
        const store = {
          view: 'overview',
          state: { config: { vps_deploy: {
            action: 'vps-deploy-logging', action_text: 'Deploy Settings',
            actions: [{command:'vps-update',command_text:'Update Linux'}]
          } } }
        };
        function defaultVpsDeployConfig() {
          return {action:'vps-deploy-logging',action_text:'Deploy Settings',mode:'parallel',debug:false,reboot_requested:false,selected_hosts:[],actions:[],modes:[]};
        }
        function renderUi() {}
        function refreshLocalInteractiveState() {}
        function isOverviewSettingsView() { return false; }
        function saveVpsDeploySettings(options) { saves.push(options); }
        """
        assertions = """
        setVpsDeployAction('vps-update');
        assert.equal(store.state.config.vps_deploy.action, 'vps-update');
        assert.equal(store.state.config.vps_deploy.action_text, 'Update Linux');
        assert.deepEqual(saves, [{silent:true}]);
        """
        _run_node_assertions(
            ["getVpsDeployConfig", "setVpsDeployAction"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_cluster_import_reload_preserves_passwords_and_apply_failure_unlocks(self) -> None:
        """Preview reloads retain password drafts and failed starts return to an actionable preview."""
        bootstrap = """
        const renders = [];
        const preview = {can_apply:true,items:[{hostname:'node-a',action:'add',hosts_action:'none'}]};
        const store = {
          clusterNodesImport: {
            loading:false, applying:false, error:'', localSudoPw:'',
            passwords:{'node-a':'secret'}, preview:preview, jobId:'', progress:null,
            progressTimer:0, result:null
          },
          master: {sudoPw:''}
        };
        function stopClusterNodesImportProgressPolling() {}
        function renderClusterNodesImportModal() {
          renders.push({
            applying:store.clusterNodesImport.applying,
            progress:store.clusterNodesImport.progress,
            error:store.clusterNodesImport.error
          });
        }
        function vpsManagerGet() { return Promise.resolve(preview); }
        function vpsManagerPost() { return Promise.reject(new Error('network offline')); }
        function openMasterPasswordPrompt() { throw new Error('sudo prompt not expected'); }
        """
        assertions = """
        (async function() {
          await loadClusterNodesImportPreview();
          assert.deepEqual(store.clusterNodesImport.passwords, {'node-a':'secret'});
          assert.equal(store.clusterNodesImport.preview, preview);

          await applyClusterNodesImport();
          assert.equal(store.clusterNodesImport.applying, false);
          assert.equal(store.clusterNodesImport.progress, null);
          assert.equal(store.clusterNodesImport.error, 'network offline');
          assert.equal(store.clusterNodesImport.preview, preview);
        }()).catch(function(error) { console.error(error); process.exitCode = 1; });
        """
        _run_node_assertions(
            ["defaultClusterNodesImportState", "loadClusterNodesImportPreview", "applyClusterNodesImport"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_linux_update_checks_support_host_and_overview_selection(self) -> None:
        """Manual package checks expose both scopes and clear their busy state on every response."""
        source = HTML_PATH.read_text(encoding="utf-8")
        sidebar_source = _extract_function(source, "renderSidebarActions")
        action_source = _extract_function(source, "handleVpsManagerAction")
        result_source = _extract_function(source, "handleResult")
        message_source = _extract_function(source, "handleMessage")

        assert "data-vps-action='check-package'" in sidebar_source
        assert "checkLinuxUpdates(getSelectedOverviewHosts())" in sidebar_source
        assert "checkLinuxUpdates([host])" in action_source
        assert "cmd === 'check_package_status'" in result_source
        assert "store.packageCheckHosts = []" in result_source
        assert "msg.cmd === 'check_package_status'" in message_source
        assert "store.packageCheckHosts = []" in message_source

    def test_pb8_install_actions_use_filled_blue_emphasis(self) -> None:
        """Uninstalled PB8 actions stand out while installed and update states retain their colors."""
        source = HTML_PATH.read_text(encoding="utf-8")
        sidebar_source = _extract_function(source, "renderSidebarActions")

        assert ".sb-btn.install {" in source
        assert "background: #3182ce;" in source
        assert "st.pb8_installed ? 'sb-btn ok' : 'sb-btn install'" in sidebar_source
        assert sidebar_source.count("st.pb8_installed ? 'sb-btn ok' : 'sb-btn install'") == 2
        assert "st.pb7_installed ? 'sb-btn ok' : 'sb-btn install'" in sidebar_source
        assert "st.pb7_installed ? 'Update PB7' : 'Install PB7'" in sidebar_source

    def test_pb8_runtime_blocker_is_visible_and_emphasizes_repair_action(self) -> None:
        """Failed PB8 updates remain visible in overview, detail, and sidebar repair controls."""
        source = HTML_PATH.read_text(encoding="utf-8")
        banner_source = _extract_function(source, "renderPb8RuntimeBanner")
        header_model_source = _extract_function(source, "getSystemHeaderModel")
        header_source = _extract_function(source, "renderSystemHeader")
        sync_header_source = _extract_function(source, "syncSystemHeader")
        sidebar_source = _extract_function(source, "renderSidebarActions")
        signature_source = _extract_function(source, "getSidebarActionsSignature")
        overview_source = _extract_function(source, "renderOverviewTable")

        assert "PB8 Update Required" in banner_source
        assert "esc(reason)" in banner_source
        assert "pb8-runtime-banner" in banner_source
        assert "const pb8Tone = pb8Blocked ? 'error'" in header_model_source
        assert "runtime-error" in header_source
        assert "model.pb8Reason" in header_source
        assert "'system-metric' + (model.pb8Blocked ? ' runtime-error' : '')" in sync_header_source
        assert "masterPb8RuntimeBlocked || st.pb8_update_available" in sidebar_source
        assert "pb8RuntimeBlocked || st.pb8_update_available" in sidebar_source
        assert "pb8_runtime_reason" in sidebar_source
        assert "pb8_blocked: !!st.pb8_runtime_blocked" in signature_source
        assert "row.pb8_runtime_blocked, row.pb8_runtime_reason" in overview_source
        assert ".system-metric.runtime-error" in source

    def test_pb7_install_is_disabled_when_backend_disk_gate_blocks_it(self) -> None:
        """A low-disk PB8-only VPS exposes the PB7 reason without allowing a click."""
        source = HTML_PATH.read_text(encoding="utf-8")
        sidebar_source = _extract_function(source, "renderSidebarActions")

        assert "const pb7ActionAllowed = st.pb7_action_allowed !== false" in sidebar_source
        assert "pb7ActionAllowed ? '' : 'disabled'" in sidebar_source
        assert "st.pb7_action_reason" in sidebar_source
        signature_source = _extract_function(source, "getSidebarActionsSignature")
        assert "pb7_allowed: st.pb7_action_allowed !== false" in signature_source
        assert "pb7_reason: String(st.pb7_action_reason || '')" in signature_source

    def test_pb8_only_sidebar_uses_pb8_update_matrix(self) -> None:
        """PB8-only and dual-runtime hosts expose the appropriate PBGui/PB8 updates."""
        source = HTML_PATH.read_text(encoding="utf-8")
        sidebar_source = _extract_function(source, "renderSidebarActions")

        assert "const pb8Only = !!st.pb8_installed && !st.pb7_installed" in sidebar_source
        assert "pb8Only ? 'vps-update-pbgui-pb8' : 'vps-update-pb'" in sidebar_source
        assert "pb8Only ? 'Update PBGui and PB8' : 'Update PBGui and PB7'" in sidebar_source
        assert "const masterHasBothRuntimes = !!st.pb7_installed && !!st.pb8_installed" in sidebar_source
        assert "masterHasBothRuntimes ? `<button" in sidebar_source
        assert 'runMasterWithLog("master-update-pbgui-pb8", "Update PBGui and PB8")' in sidebar_source
        assert "const hasBothRuntimes = !!st.pb7_installed && !!st.pb8_installed" in sidebar_source
        assert "hasBothRuntimes ? `<button" in sidebar_source
        assert "data-command='vps-update-pbgui-pb8' data-command-text='Update PBGui and PB8'" in sidebar_source
        assert "st.pb8_installed ? 'Updates the installed PB8 runtime.'" in sidebar_source
        assert "PB8-only leaves PBRun disabled" not in source

    def test_pb8_branch_sidebar_views_and_hash_routes_are_installed_only(self) -> None:
        """Installed PB8 runtimes expose branch views with master and VPS context routes."""
        source = HTML_PATH.read_text(encoding="utf-8")
        sidebar_source = _extract_function(source, "renderSidebarActions")
        view_hash_source = _extract_function(source, "viewHash")
        restore_source = _extract_function(source, "restoreViewFromHash")
        render_view_source = _extract_function(source, "renderView")
        branch_panel_source = _extract_function(source, "renderBranchPanelForCurrentView")
        host_switch_source = _extract_function(source, "selectSidebarVpsHost")

        assert sidebar_source.count("st.pb8_installed ? `<button") == 2
        assert 'selectView("master-pb8-branch")' in sidebar_source
        assert "data-view='vps-pb8-branch'" in sidebar_source
        assert "'#master/pb8-branch'" in view_hash_source
        assert "'/pb8-branch'" in view_hash_source
        assert "selectView('master-pb8-branch')" in restore_source
        assert "selectView('vps-pb8-branch', hostname)" in restore_source
        assert "renderMasterBranchView('pb8')" in render_view_source
        assert "renderVpsBranchView('pb8')" in render_view_source
        assert branch_panel_source.count("(detail.status || {}).pb8_installed") == 2
        assert "store.view === 'vps-pb8-branch' && !row.pb8_installed" in host_switch_source

    def test_pb7_and_pb8_branch_state_and_remote_caches_are_independent(self) -> None:
        """Each runtime keeps distinct branch selections and remote response caches."""
        assertions = """
        const master = defaultMasterUi();
        const vps = defaultVpsUi();

        for (const ui of [master, vps]) {
          ui.pb7Branch = 'v7';
          ui.pb8Branch = 'v8';
          ui.pb7RemoteBranches.push('seven');
          ui.pb8RemoteBranches.push('eight');
          ui.pb7RemoteCommits.seven = [{ full: '777' }];
          ui.pb8RemoteCommits.eight = [{ full: '888' }];

          assert.equal(ui.pb7Branch, 'v7');
          assert.equal(ui.pb8Branch, 'v8');
          assert.deepEqual(ui.pb7RemoteBranches, ['seven']);
          assert.deepEqual(ui.pb8RemoteBranches, ['eight']);
          assert.deepEqual(ui.pb7RemoteCommits.seven, [{ full: '777' }]);
          assert.deepEqual(ui.pb8RemoteCommits.eight, [{ full: '888' }]);
          assert.notEqual(ui.pb7RemoteBranches, ui.pb8RemoteBranches);
          assert.notEqual(ui.pb7RemoteCommits, ui.pb8RemoteCommits);
        }
        """
        _run_node_assertions(
            ["defaultMasterUi", "defaultVpsUi"],
            bootstrap="",
            assertions=assertions,
        )

    def test_remote_branch_responses_follow_the_active_runtime_context(self) -> None:
        """WebSocket responses remain bound to their requesting runtime after navigation."""
        bootstrap = """
        const store = {
          view: 'master-pb8-branch',
          pendingBranchRequests: {
            eight: { runtime: 'pb8', scope: 'master', kind: 'branches', remoteUrl: 'https://example/pb8.git', branch: '' },
            seven: { runtime: 'pb7', scope: 'master', kind: 'commits', remoteUrl: 'https://example/pb7.git', branch: 'v7-dev' }
          },
          master: {
            pb7RemoteBranches: ['keep-seven'],
            pb7RemoteCommits: {},
            pb7RemoteCommitsRequest: 'seven',
            pb8RemoteBranches: [],
            pb8RemoteCommits: {},
            pb8RemoteBranchesRequest: 'eight',
            pb7BranchSelectInteracting: false,
            pb8BranchSelectInteracting: false
          },
          pb7BranchSelectInteracting: false,
          pb8BranchSelectInteracting: false
        };
        let syncs = 0;
        function isMasterContextView() { return true; }
        function isVpsContextView() { return false; }
        function syncBranchViewPanel() { syncs += 1; }
        function ensureVpsUi() { throw new Error('VPS state must not be used'); }
        """
        assertions = """
        handleMessage({ type: 'remote_branches', request_id: 'eight', remote_url: 'https://example/pb8.git', branches: ['v8-dev'] });
        assert.deepEqual(store.master.pb8RemoteBranches, ['v8-dev']);
        assert.deepEqual(store.master.pb7RemoteBranches, ['keep-seven']);
        assert.equal(store.master.pb8RemoteUrl, 'https://example/pb8.git');

        store.view = 'master-pb7-branch';
        handleMessage({ type: 'remote_branch_commits', request_id: 'seven', remote_url: 'https://example/pb7.git', branch: 'v7-dev', commits: [{ full: '777' }] });
        assert.deepEqual(store.master.pb7RemoteCommits['v7-dev'], [{ full: '777' }]);
        assert.deepEqual(store.master.pb8RemoteCommits, {});
        assert.equal(syncs, 2);
        """
        _run_node_assertions(
            ["runtimeBranchKeys", "isPb7BranchView", "isPb8BranchView", "activeRuntimeBranchRepo", "takeRuntimeBranchRequest", "handleMessage"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_remote_head_is_used_without_manual_remote_branch_override(self) -> None:
        """HEAD uses the loaded selected remote instead of stale local branch history."""
        bootstrap = """
        const ui = {
          pb8Branch: 'feature',
          pb8Commit: 'HEAD',
          pb8ManualBranch: '',
          pb8ManualCommit: '',
          pb8RemoteBranches: ['feature'],
          pb8RemoteCommits: { feature: [{ full: 'remote-head' }, { full: 'current' }] }
        };
        const state = {
          current_branch: 'feature',
          current_commit: 'current',
          branches: { feature: [{ full: 'stale-local' }, { full: 'current' }] }
        };
        """
        assertions = """
        const model = pb7BranchModel(state, ui, 'pb8');
        assert.equal(model.effectiveCommit, 'remote-head');
        assert.equal(model.targetCommit, 'remote-head');
        assert.equal(model.isBehindOrigin, true);
        assert.equal(model.commitsBehind, 1);
        assert.equal(model.usingRemoteCommitHistory, true);
        """
        _run_node_assertions(
            ["runtimeBranchKeys", "branchModel", "pb7BranchModel"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_pb8_branch_actions_use_update_commands_and_only_pb8_vars(self) -> None:
        """PB8 branch actions use existing update commands with the contracted payload."""
        bootstrap = """
        const calls = [];
        const pb8Ui = {
          pb8Branch: 'v8-feature',
          pb8Commit: 'HEAD',
          pb8RemoteName: 'fork',
          pb8RemoteUrl: 'https://example/pb8.git',
          pb8ManualBranch: 'v8-feature',
          pb8ManualCommit: 'cafebabe',
          pb8RemoteBranches: ['v8-feature'],
          pb8RemoteCommits: { 'v8-feature': [{ full: 'cafebabe' }] }
        };
        const branchState = {
          current_branch: 'master',
          current_commit: 'deadbeef',
          branches: { master: [{ full: 'deadbeef' }], 'v8-feature': [{ full: 'cafebabe' }] },
          remote_urls: { fork: 'https://example/pb8.git' },
          default_remote_name: 'fork'
        };
        const store = {
          master: Object.assign({}, pb8Ui),
          vps: { runner: Object.assign({}, pb8Ui) },
          detail: { kind: 'master', branches: { pb8: branchState } }
        };
        function ensureVpsUi(hostname) { return store.vps[hostname]; }
        function shortHash(value) { return String(value || '').slice(0, 8); }
        function runMasterWithLog(command, label, extraVars) { calls.push({ scope: 'master', command, label, extraVars }); }
        function runVpsWithLog(hostname, command, label, extraVars) { calls.push({ scope: hostname, command, label, extraVars }); }
        """
        assertions = """
        triggerPb7BranchAction('master', 'pb8');
        store.detail = { kind: 'vps', hostname: 'runner', branches: { pb8: branchState } };
        triggerPb7BranchAction('runner', 'pb8');

        assert.equal(calls[0].command, 'master-update-pb8');
        assert.equal(calls[1].command, 'vps-update-pb8');
        for (const call of calls) {
          assert.deepEqual(Object.keys(call.extraVars).sort(), [
            'pb8_branch',
            'pb8_commit',
            'pb8_remote_name',
            'pb8_remote_url',
            'pb8_source_branch'
          ]);
          assert.equal(call.extraVars.pb8_branch, 'v8-feature');
          assert.equal(call.extraVars.pb8_source_branch, 'v8-feature');
          assert.equal(call.extraVars.pb8_commit, 'cafebabe');
          assert.equal(call.extraVars.pb8_remote_name, 'fork');
          assert.equal(call.extraVars.pb8_remote_url, 'https://example/pb8.git');
          assert.equal(Object.keys(call.extraVars).some(key => key.startsWith('pb7_')), false);
        }
        """
        _run_node_assertions(
            ["runtimeBranchKeys", "branchModel", "pb7BranchModel", "triggerPb7BranchAction"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_pb8_branch_select_handler_survives_html_attribute_parsing(self) -> None:
        """Rendered PB8 onchange code must remain complete and update the selected branch."""
        bootstrap = """
        const calls = [];
        const branchState = {
          current_branch: 'master',
          current_commit: 'current',
          branches: {
            master: [{ full: 'current' }],
            'codex/gpu-tma-market-foundation': [{ full: 'gpu-head' }]
          },
          known_remotes: ['origin'],
          remote_urls: { origin: 'https://example.test/passivbot.git' },
          branch_tracking_remotes: {},
          default_remote_name: 'origin'
        };
        const store = {
          master: {
            pb8Branch: 'master', pb8Commit: 'HEAD', pb8ManualBranch: '', pb8ManualCommit: '',
            pb8RemoteName: 'origin', pb8RemoteUrl: 'https://example.test/passivbot.git',
            pb8RemoteBranches: [], pb8RemoteCommits: {}
          },
          detail: { kind: 'master', branches: { pb8: branchState } }
        };
        function ensureVpsUi() { throw new Error('not expected'); }
        function js(value) { return JSON.stringify(value == null ? '' : String(value)); }
        function esc(value) { return String(value == null ? '' : value); }
        function escAttr(value) { return esc(value); }
        function shortHash(value) { return String(value || '').slice(0, 8); }
        function syncBranchViewPanel() {}
        function renderBranchMessage() { return ''; }
        function renderMissingCommitsPanel() { return ''; }
        function renderCommitDetails() { return ''; }
        function setPb7LocalBranch(scope, value, runtime) { calls.push({ scope, value, runtime }); }
        """
        assertions = """
        const html = renderPb7BranchPanel('master', branchState, 'pb8');
        const handlers = Array.from(html.matchAll(/onchange='([^']*)'/g), match => match[1]);
        const handler = handlers.find(value => value.includes('setPb7LocalBranch'));
        assert.ok(handler, 'complete local branch onchange handler is present');
        const run = new Function('store', 'setPb7LocalBranch', 'syncBranchViewPanel', handler);
        run.call({ value: 'codex/gpu-tma-market-foundation' }, store, setPb7LocalBranch, syncBranchViewPanel);
        assert.deepEqual(calls, [{ scope: 'master', value: 'codex/gpu-tma-market-foundation', runtime: 'pb8' }]);
        """
        _run_node_assertions(
            ["runtimeBranchKeys", "branchModel", "pb7BranchModel", "renderPb7BranchPanel"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_pb8_branch_selects_hold_live_rerenders_and_guides_match(self) -> None:
        """PB8 selectors resist live replacement and both guides document explicit tracking."""
        source = HTML_PATH.read_text(encoding="utf-8")
        render_ui_source = _extract_function(source, "renderUi")
        sync_source = _extract_function(source, "syncBranchViewPanel")
        panel_source = _extract_function(source, "renderPb7BranchPanel")
        english = (ROOT / "docs" / "help" / "32_vps_manager.md").read_text(encoding="utf-8")
        german = (ROOT / "docs" / "help_de" / "32_vps_manager.md").read_text(encoding="utf-8")

        assert "holdPb8BranchView" in render_ui_source
        assert "store.pb8BranchSelectInteracting && isPb8BranchView()" in sync_source
        assert panel_source.count("onfocus='store.${interactionField} = true'") >= 5
        assert "PB8 Branch Management" in english
        assert "PB8 Branch Management" in german
        assert "normal **Update PB8**" in english
        assert "normale Aktion **Update PB8**" in german

    def test_combined_update_button_warns_only_when_both_components_need_updates(self) -> None:
        """Combined PBGui/runtime actions must not duplicate one component's warning state."""
        _run_node_assertions(
            ["combinedUpdateButtonClass"],
            bootstrap="",
            assertions="""
              assert.equal(combinedUpdateButtonClass(false, false), 'sb-btn ok');
              assert.equal(combinedUpdateButtonClass(true, false), 'sb-btn ok');
              assert.equal(combinedUpdateButtonClass(false, true), 'sb-btn ok');
              assert.equal(combinedUpdateButtonClass(true, true), 'sb-btn warning');
            """,
        )
        source = HTML_PATH.read_text(encoding="utf-8")
        sidebar_source = _extract_function(source, "renderSidebarActions")
        assert sidebar_source.count("combinedUpdateButtonClass(st.pbgui_update_available") == 4

    def test_manual_refresh_reloads_current_detail_for_update_indicators(self) -> None:
        """A completed git refresh must immediately reload the detail that controls update colors."""
        source = HTML_PATH.read_text(encoding="utf-8")
        refresh_branch = source.split("} else if (cmd === 'refresh') {", 1)[1].split(
            "} else if (cmd === 'read_vps_settings')", 1
        )[0]

        assert "fetchMasterDetail();" in refresh_branch
        assert "fetchVpsDetail(store.hostname);" in refresh_branch

    def test_bulk_runtime_updates_are_profile_aware(self) -> None:
        """Overview bulk actions delegate runtime selection to each host profile."""
        source = HTML_PATH.read_text(encoding="utf-8")
        sidebar_source = _extract_function(source, "renderSidebarActions")

        assert 'deploySelectedVpsAction("vps-update-runtime", "Update Passivbot (PB7/PB8)")' in sidebar_source
        assert 'deploySelectedVpsAction("vps-update-pbgui-runtime", "Update PBGui + Passivbot (PB7/PB8)")' in sidebar_source
        assert "Updates PB7, PB8, or both according to what is installed on each selected VPS." in sidebar_source
        assert 'deploySelectedVpsAction("vps-update-pb7", "Update PB7")' not in sidebar_source

    def test_vps_monitor_combines_pb7_and_pb8_runtime_rows(self) -> None:
        """Detailed and fallback monitor rows retain PB7/PB8 identity in one table."""
        source = HTML_PATH.read_text(encoding="utf-8")
        render_monitor = _extract_function(source, "renderMonitorPanel")
        live_update = _extract_function(source, "_liveUpdateServiceRows")

        assert "const runtimeRows = pb7Rows.concat(pb8Rows)" in render_monitor
        assert "const runtimeFallback = pb7Fallback.concat(pb8Fallback)" in render_monitor
        assert "No running PB7 or PB8 instances reported." in source
        assert "data-bot-key" in source
        assert "var rowKey = pbVersion + ':' + name" in live_update

    def test_firewall_validation_accepts_ipv4_cidr_networks(self) -> None:
        """Allow the IPv4 CIDR sources that remote UFW discovery stores locally."""
        _run_node_assertions(
            ["isValidIPv4", "validateFirewallIps"],
            bootstrap="",
            assertions="""
            assert.equal(validateFirewallIps('198.51.100.1,10.8.0.0/24').ok, true);
            assert.equal(validateFirewallIps('0.0.0.0/0').ok, true);
            assert.equal(validateFirewallIps('10.8.0.0/33').ok, false);
            assert.equal(validateFirewallIps('2001:db8::/32').ok, false);
            """,
        )

    def test_log_viewer_restart_waits_for_websocket_reconnect(self) -> None:
        """Reconnects pre-arm the log cursor before sending a queued restart."""
        source = LOG_VIEWER_PATH.read_text(encoding="utf-8")

        assert "this._pendingRestartCommand = command" in source
        assert "if (!me._flushPendingRestart(ws)) me._subscribe()" in source
        assert "this._prepareRestartStream(cmd)" in source
        assert "this._sendPreparedRestart(msg.sid)" in source
        assert "this._send(attempt.command)" in source
        assert "_sendRestart(" not in source

    def test_vps_manager_send_queues_commands_while_websocket_reconnects(self) -> None:
        """VPS Manager commands must not be dropped while its WebSocket reconnects."""
        source = HTML_PATH.read_text(encoding="utf-8")

        assert "pendingWsMessages: []" in source
        assert "queueWsMessage(payload)" in source
        assert "flushPendingWsMessages()" in source
        assert "store.pendingWsMessages = []" in source

    def test_remote_master_update_switches_to_actual_task_log(self) -> None:
        """Use the backend-selected master log after a VPS update command is remapped."""
        source = HTML_PATH.read_text(encoding="utf-8")
        handle_result = _extract_function(source, "handleResult")

        assert "const actualFile = String(data.file_alias || '')" in handle_result
        assert "if (actualFile) task.file = actualFile" in handle_result
        assert "task.startEmpty = false" in handle_result
        assert "task.runKey = String(Date.now())" in handle_result
        assert "taskLogViewerKey = ''" in handle_result

    def test_new_task_log_stays_empty_until_backend_confirms_rotation(self) -> None:
        """Do not show the prior run, then reload the new file from its beginning."""
        source = HTML_PATH.read_text(encoding="utf-8")
        open_master = _extract_function(source, "openMasterTaskLog")
        open_vps = _extract_function(source, "openVpsTaskLog")
        handle_result = _extract_function(source, "handleResult")

        assert "startEmpty: true" in open_master
        assert "startEmpty: true" in open_vps
        assert "task.startEmpty = false" in handle_result
        assert handle_result.count("taskLogViewerKey = ''") >= 2

    def test_host_detail_context_and_full_hydration_have_separate_generations(self) -> None:
        """Quick socket updates must not cancel same-host background detail hydration."""
        source = HTML_PATH.read_text(encoding="utf-8")
        handle_message = _extract_function(source, "handleMessage")
        send_context = _extract_function(source, "sendContext")
        select_view = _extract_function(source, "selectView")

        assert "contextGeneration: 1" in source
        assert "context_generation: store.contextGeneration" in send_context
        assert "incomingGeneration !== store.contextGeneration" in handle_message
        assert "store.detailAbortController.abort()" not in handle_message
        assert "buildProvisionalVpsDetail(nextHostname)" in select_view

    def test_host_switch_builds_immediate_detail_from_overview_snapshot(self) -> None:
        """A never-visited host renders its local summary without waiting for detail I/O."""
        bootstrap = """
        const store = {};
        function getOverviewRows() {
          return [{
            hostname: 'manibot62',
            online: true,
            ssh_online: true,
            telemetry_fresh: true,
            role: 'vps',
            updates: 3,
            task_command: 'vps-update',
            task_command_text: 'Update Linux',
            task_status: 'running'
          }];
        }
        """
        assertions = """
        const detail = buildProvisionalVpsDetail('manibot62');
        assert.equal(detail.kind, 'vps');
        assert.equal(detail.hostname, 'manibot62');
        assert.equal(detail.provisional, true);
        assert.equal(detail.status.online, true);
        assert.equal(detail.status.pending_updates, 3);
        assert.equal(detail.status.summary_row.role, 'vps');
        assert.equal(detail.progress.command, 'vps-update');
        """
        _run_node_assertions(
            ["buildProvisionalVpsDetail"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_new_vps_setup_context_survives_stale_overview_snapshot(self) -> None:
        """An older overview snapshot cannot eject a newly initialized host from its task log."""
        bootstrap = """
        const store = {
          view: 'vps-task-log',
          hostname: 'fresh-runner',
          detail: null,
          pendingAutoSetupSwitch: { hostname: 'fresh-runner' },
          _pendingView: '',
          _pendingHostname: ''
        };
        const history = { replaceState: function () {} };
        let contextSends = 0;
        function sendContext() { contextSends += 1; }
        function isVpsContextView() { return store.view === 'vps-task-log'; }
        function viewUrl(view) { return '#' + view; }
        """
        assertions = """
        const staleState = { overview: { rows: [] } };
        assert.equal(reconcileCurrentVpsContext(staleState), false);
        assert.equal(store.view, 'vps-task-log');
        assert.equal(store.hostname, 'fresh-runner');
        assert.equal(contextSends, 0);
        store.pendingAutoSetupSwitch = null;
        assert.equal(reconcileCurrentVpsContext(staleState), true);
        assert.equal(store.view, 'overview');
        assert.equal(store.hostname, '');
        assert.equal(contextSends, 1);
        """
        _run_node_assertions(
            ["normalizeOverviewHostname", "hasManagedOverviewHost", "reconcileCurrentVpsContext"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_pb8_setup_completion_uses_the_combined_setup_status(self) -> None:
        """The Add flow no longer waits for a separate PB8 update command."""
        source = HTML_PATH.read_text(encoding="utf-8")
        handle_message = _extract_function(source, "handleMessage")

        assert "Setup and PB8 Live installation successful." in handle_message
        assert "Starting PB8 Live installation" not in handle_message
        assert "activeCommand === 'vps-update-pb8'" not in handle_message
        assert "switchToVpsTaskLog(store.detail.hostname, 'vps-update-pb8'" not in handle_message

    def test_metric_history_reuses_cache_and_rejects_stale_responses(self) -> None:
        """Local history renders cached data immediately and aborts obsolete requests."""
        source = HTML_PATH.read_text(encoding="utf-8")
        open_history = _extract_function(source, "openMetricHistory")

        assert "metricHistoryCache[cacheKey]" in open_history
        assert "cpuHistoryAbortController.abort()" in open_history
        assert "generation !== cpuHistoryRequestGeneration" in open_history
        assert "signal: cpuHistoryAbortController.signal" in open_history

    @pytest.mark.parametrize("failure", ["network", "http_404", "http_500", "json", "timeout", "json_timeout"])
    def test_hl_history_refresh_errors_preserve_data_and_recover(self, failure: str) -> None:
        """Current failures retain chart/edit state; a later automatic update recovers."""
        _run_hl_history_assertions(
            "const failure = " + json.dumps(failure) + ";\n" + r"""
            const body = deferred();
            const networkError = new Error('Synthetic network failure');
            fetchImpl = () => {
              if (failure === 'network') return Promise.reject(networkError);
              if (failure.startsWith('http_')) return Promise.resolve({ ok: false, status: Number(failure.slice(5)),
                json: () => { throw new Error('HTTP failure must not parse JSON'); } });
              if (failure === 'json') return Promise.resolve({ ok: true, json: () => Promise.reject(new SyntaxError('Synthetic JSON failure')) });
              if (failure === 'json_timeout') return Promise.resolve({ ok: true, json: () => body.promise });
              return body.promise;
            };
            const pending = refreshOpenHlRateLimitHistory();
            const controller = cpuHistoryAbortController;
            await flush();
            if (failure.includes('timeout')) {
              assert.equal(logged.length, 0);
              expire();
            }
            await pending;
            assert.equal(logged.length, 1);
            assert.equal(logged[0][0], 'Rate limit history fetch failed:');
            assert.ok(logged[0][1] instanceof Error);
            if (failure === 'network') assert.equal(logged[0][1], networkError);
            if (failure.includes('timeout')) {
              assert.equal(controller.signal.aborted, true);
              assert.match(logged[0][1].message, /timed out/);
            }
            assert.equal(elements.cpuHistorySubtitle.textContent,
              'Failed to update rate limit history. Displaying cached data.');
            assertPreserved();
            assertClean(controller);
            // A non-abortable body settling after the deadline cannot update cache/UI.
            body.resolve(fresh);
            await flush();
            assertPreserved();
            assert.equal(logged.length, 1);

            fetchImpl = () => Promise.resolve({ ok: true, json: () => Promise.resolve(fresh) });
            const recovery = refreshOpenHlRateLimitHistory();
            const recoveryController = cpuHistoryAbortController;
            await recovery;
            assert.equal(metricHistoryCache[cacheKey], fresh);
            assert.equal(cpuHistoryLastPayload.data, fresh);
            assert.equal(elements.cpuHistorySubtitle.textContent, 'Normal subtitle');
            assert.equal(elements.cpuHistoryBody.innerHTML, 'Fresh chart');
            assert.equal(elements.hlCreditAmount.value, '123');
            assert.equal(elements.hlCreditAmount.selectionStart, 1);
            assert.equal(document.activeElement, elements.hlCreditAmount);
            assert.equal(logged.length, 1);
            assertClean(recoveryController);
            // No redundant update without a newer sample; forced updates still work.
            assert.equal(refreshOpenHlRateLimitHistory(), undefined);
            assert.equal(requests.length, 2);
            await refreshOpenHlRateLimitHistory(true);
            assert.equal(requests.length, 3);
            """
        )

    @pytest.mark.parametrize("context", ["closed", "host", "bot", "metric", "cleared", "auth", "generation", "abort", "abort_error"])
    @pytest.mark.parametrize("outcome", ["failure", "success", "timeout"])
    def test_hl_history_refresh_ignores_obsolete_results(self, context: str, outcome: str) -> None:
        """Closed, superseded, unauthenticated and cancelled requests never alter UI."""
        _run_hl_history_assertions(
            "const context = " + json.dumps(context) + ";\n"
            "const outcome = " + json.dumps(outcome) + ";\n" + r"""
            const response = deferred();
            fetchImpl = () => response.promise;
            const pending = refreshOpenHlRateLimitHistory();
            const controller = cpuHistoryAbortController;
            if (context === 'closed') overlay.visible = false;
            if (context === 'host') cpuHistoryLastPayload = { ...originalContext, host: 'another-host' };
            if (context === 'bot') cpuHistoryLastPayload = { ...originalContext, botName: 'another-bot' };
            if (context === 'metric') cpuHistoryLastPayload = { ...originalContext, metric: 'cpu' };
            if (context === 'cleared') cpuHistoryLastPayload = null;
            if (context === 'auth') store.authExpired = true;
            if (context === 'generation') cpuHistoryRequestGeneration++;
            if (context === 'abort') {
              controller.abort();
              assert.equal(timers.size, 0); // Cleared immediately, even if fetch never settles.
            }
            if (context === 'abort_error') {
              response.reject(Object.assign(new Error('Intentional abort'), { name: 'AbortError' }));
            } else if (outcome === 'timeout' && context !== 'abort') {
              expire();
            } else if (outcome === 'success') {
              response.resolve({ ok: true, json: () => Promise.resolve(fresh) });
            } else if (outcome === 'failure') {
              response.reject(new Error('Obsolete error'));
            }
            await pending;
            const changedContext = cpuHistoryLastPayload;
            cpuHistoryLastPayload = originalContext;
            assertPreserved();
            cpuHistoryLastPayload = changedContext;
            assert.equal(elements.cpuHistorySubtitle.textContent, 'Normal subtitle');
            assert.equal(logged.length, 0);
            assertClean(controller);
            // Late completion after timeout/abort must stay silent as well.
            response.resolve({ ok: true, json: () => Promise.resolve(fresh) });
            await flush();
            assert.equal(metricHistoryCache[cacheKey], cached);
            assert.equal(cpuHistoryLastPayload, changedContext);
            assert.equal(elements.cpuHistorySubtitle.textContent, 'Normal subtitle');
            assert.equal(logged.length, 0);
            """
        )

    def test_hl_history_refresh_cleanup_preserves_new_request_owner(self) -> None:
        """Old abort/cleanup and late responses cannot clear a replacement controller."""
        _run_hl_history_assertions(r"""
            const first = deferred();
            const second = deferred();
            fetchImpl = () => first.promise;
            const obsolete = refreshOpenHlRateLimitHistory();
            const oldController = cpuHistoryAbortController;
            fetchImpl = () => second.promise;
            const active = refreshOpenHlRateLimitHistory();
            const newController = cpuHistoryAbortController;
            assert.notEqual(oldController, newController);
            assert.equal(oldController.signal.aborted, true);
            await obsolete;
            assert.equal(oldController.signal.listeners.size, 0);
            assert.equal(cpuHistoryAbortController, newController);
            assert.equal(timers.size, 1);
            assert.equal(newController.signal.aborted, false);
            first.resolve({ ok: true, json: () => Promise.resolve({ samples: [] }) });
            await flush();
            assertPreserved();
            assert.equal(logged.length, 0);
            assert.equal(cpuHistoryAbortController, newController);
            second.resolve({ ok: true, json: () => Promise.resolve(fresh) });
            await active;
            assert.equal(metricHistoryCache[cacheKey], fresh);
            assert.equal(elements.cpuHistorySubtitle.textContent, 'Normal subtitle');
            assertClean(newController);
        """)

    def test_today_log_matches_show_loading_and_reject_stale_responses(self) -> None:
        """Error and traceback clicks respond immediately while one request is active."""
        source = HTML_PATH.read_text(encoding="utf-8")
        fetch_matches = _extract_function(source, "fetchBotLogMatches")
        handle_message = _extract_function(source, "handleMessage")

        assert "openBotLogMatchesLoadingModal" in fetch_matches
        assert "request_id: requestId" in fetch_matches
        assert "pendingBotLogMatchRequestId" in handle_message

    def test_bot_log_click_preserves_pb_runtime_version(self) -> None:
        """PB7 and PB8 telemetry links open the matching native bot log."""
        bootstrap = """
        const store = { detail: null };
        const ui = { log: {} };
        let openedHost = '';
        function ensureVpsUi() { return ui; }
        function openVpsHostLogs(hostname) { openedHost = hostname; }
        function metricHistoryMeta() { return { label: 'Memory' }; }
        """
        assertions = """
        openBotLog('pb-host', 'pb8_bot', '8');
        assert.equal(ui.log.filename, 'software/pb8/logs/pb8_bot.log');
        assert.equal(openedHost, 'pb-host');
        assert.equal(normalizeVpsLogViewerTarget(ui.log.filename), 'Bot:pb8_bot:8');

        openBotLog('pb-host', 'pb7_bot', '7');
        assert.equal(ui.log.filename, 'software/pb7/logs/pb7_bot.log');
        assert.equal(normalizeVpsLogViewerTarget(ui.log.filename), 'Bot:pb7_bot:7');
        assert.equal(metricHistoryTitle('memory', 'pb-host', '8:pb8_bot'), 'Memory History - pb8_bot @ pb-host');
        """
        _run_node_assertions(
            ["normalizeVpsLogViewerTarget", "openBotLog", "metricHistoryTitle"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_pb8_summary_only_pnl_history_explains_missing_daily_curve(self) -> None:
        """An authoritative PB8 total must not be presented as a fabricated daily series."""

        bootstrap = """
        const METRIC_HISTORY_META = {
          pnl: { pnlMetric: true, empty: 'No bot PNL history available yet.' },
          pnl_fills: { fillsMetric: true, empty: 'No bot fill history available yet.' }
        };
        """
        assertions = """
        const payload = { bot_name: '8:pb8_bot', summary_only: true };
        assert.equal(
          metricHistoryEmpty('pnl', payload),
          'PB8 provides an authoritative total, but no reliable daily net-PNL breakdown.'
        );
        assert.equal(
          metricHistoryEmpty('pnl_fills', payload),
          'PB8 provides an authoritative total, but no reliable daily net-PNL breakdown.'
        );
        """
        _run_node_assertions(
            ["metricHistoryMeta", "metricHistoryEmpty"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_vps_manager_delete_and_remote_purge_are_separate_actions(self) -> None:
        """Deleting a VPS record stays local while remote install purge is explicit."""
        source = HTML_PATH.read_text(encoding="utf-8")

        assert "This only removes the saved VPS Manager record from PBGui" in source
        assert "function confirmPurgeVpsInstall(hostname)" in source
        assert "vpsActionWithPw(hostname, 'vps-purge-install', 'Purge VPS Install')" in source
        assert "data-vps-action='purge-vps'" in source
        assert "data-host='${escAttr(selectedHost)}'>Purge VPS Install</button>" in source

    def test_existing_vps_host_key_can_be_repaired_in_gui(self) -> None:
        """Expose review, exact confirmation, replacement, and reconnect in the VPS sidebar."""
        source = HTML_PATH.read_text(encoding="utf-8")
        sidebar_source = _extract_function(source, "renderSidebarActions")
        main_source = _extract_function(source, "renderVpsView")

        assert "function reviewVpsSshHostKey(hostname)" in source
        assert "function openSshHostKeyReviewModal(data)" in source
        assert "const hostKeyBtnClass = st.host_key_error || (hostKeyStatus !== 'known' && hostKeyStatus !== 'local') ? 'sb-btn warning' : 'sb-btn'" in sidebar_source
        assert "data-vps-action='review-host-key'" in sidebar_source
        assert "data-host='${escAttr(selectedHost)}'>Review SSH Host Key</button>" in sidebar_source
        assert "reviewVpsSshHostKey" not in main_source
        assert "Replace Key & Reconnect" in source
        assert "expected_fingerprint: fingerprint" in source
        assert "replace_existing: status === 'mismatch'" in source
        assert "Monitor reconnect requested" in source
        assert "class='ssh-host-key-targets'" in source
        assert "class='status-sub code ssh-host-key-fingerprint'" in source
        assert "overflow-wrap: anywhere" in source

    def test_existing_vps_import_can_replace_confirmed_changed_host_key(self) -> None:
        """The import preview exposes changed-key replacement with exact fingerprint replay."""

        source = HTML_PATH.read_text(encoding="utf-8")
        preview_source = _extract_function(source, "renderExistingVpsImportPreview")
        trust_source = _extract_function(source, "trustExistingVpsImportHostKey")

        assert "Replace stored key and probe again" in preview_source
        assert "Verify this fingerprint through a trusted channel" in preview_source
        assert "flow.form.accept_unknown_host = true" in trust_source
        assert "flow.form.accepted_host_key_fingerprint = fingerprint" in trust_source

    def test_overview_shows_ssh_host_key_status(self) -> None:
        """Show trusted, unknown, changed, and failed SSH keys in Overview."""
        source = HTML_PATH.read_text(encoding="utf-8")
        table_source = _extract_function(source, "renderOverviewTable")

        assert "{ key: 'ssh_key', label: 'SSH Key', defaultVisible: true" in source
        assert "levelTag('ok', 'Trusted')" in table_source
        assert "levelTag('warning', 'Unknown')" in table_source
        assert "levelTag('error', 'Changed')" in table_source
        assert "levelTag('error', 'Failed')" in table_source

    def test_vps_manager_add_to_cluster_runs_complete_join(self) -> None:
        """VPS Manager exposes the complete guarded Cluster onboarding flow."""

        source = HTML_PATH.read_text(encoding="utf-8")

        assert "function addVpsToCluster(hostname)" in source
        assert "startClusterOnboard(target)" in source
        assert "function renderClusterOnboardProgressModal()" in source
        assert "function pollClusterOnboardProgress(jobId, generation)" in source
        assert "'/cluster-onboard/' + encodeURIComponent(target) + '/start'" in source
        assert "if (isClusterOnboardModalVisible() && isClusterOnboardRunning()) return;" in source
        assert source.count("if (isClusterOnboardModalVisible() && isClusterOnboardRunning()) return;") >= 2
        assert "closeButton.style.display = running ? 'none' : ''" in source
        assert "clusterNodeAction === 'resume' ? 'Resume Cluster Onboarding'" in source
        assert "id='cluster-onboard-retry'>Retry</button>" in source
        assert "retryButton.onclick = function () { startClusterOnboard(String(flow.hostname || '')); };" in source
        assert "install restricted Cluster keys" in source
        assert "verify that no conflicting identity exists" in source
        assert "VPS joined and synchronized with Cluster." in source
        assert "${showClusterNodeBtn ? `<button class='${clusterNodeBtnClass}'" in source

    def test_add_vps_initialize_requires_green_preflight_checks(self) -> None:
        """Initialize stays blocked until required pre-flight checks pass."""
        bootstrap = """
        const store = { addVpsReady: { hostsOk: false, sshOk: true, loading: false } };
        function hasSessionSecret(hostname, field) { return false; }
        function validateFirewallIps(value) { return { ok: true }; }
        """
        assertions = """
        const validForm = {
          ip: '203.0.113.10', hostname: 'manibot40', user: 'mani', user_pw: 'user-pw',
          init_methode: 'root', initial_root_pw: 'root-pw', root_pw: 'new-root-pw',
          swap: '3G', install_dir: '/home/mani/software', firewall_ssh_ips: '198.51.100.1'
        };
        assert.equal(canInitForm(validForm), false);
        store.addVpsReady = { hostsOk: true, sshOk: false, loading: false };
        assert.equal(canInitForm(validForm), false);
        store.addVpsReady = { hostsOk: true, sshOk: true, loading: true };
        assert.equal(canInitForm(validForm), false);
        store.addVpsReady = { hostsOk: true, sshOk: true, loading: false };
        assert.equal(canInitForm(validForm), true);
        """
        _run_node_assertions(
            ["hasInvalidPasswordChars", "addVpsPreflightReady", "canInitForm"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_add_vps_initialize_click_is_guarded(self) -> None:
        """A stale enabled button cannot send init_vps while pre-flight is red."""
        bootstrap = """
        const sent = [];
        const toasts = [];
        const store = {
          addVpsReady: { hostsOk: false, sshOk: true, loading: false },
          master: { debug: false },
          addForm: {
            ip: '203.0.113.10', hostname: 'manibot40', user: 'mani', user_pw: 'user-pw',
            init_methode: 'root', initial_root_pw: 'root-pw', root_pw: 'new-root-pw',
            swap: '3G', install_dir: '/home/mani/software', firewall_ssh_ips: '198.51.100.1'
          }
        };
        function hasSessionSecret(hostname, field) { return false; }
        function validateFirewallIps(value) { return { ok: true }; }
        function toast(message, level) { toasts.push({ message, level }); }
        function send(payload) { sent.push(payload); }
        """
        assertions = """
        initAddVps();
        assert.equal(sent.length, 0);
        assert.equal(toasts[0].level, 'warning');
        store.addVpsReady = { hostsOk: true, sshOk: true, loading: false };
        initAddVps();
        assert.equal(sent.length, 1);
        assert.equal(sent[0].cmd, 'init_vps');
        """
        _run_node_assertions(
            ["hasInvalidPasswordChars", "addVpsPreflightReady", "canInitForm", "initAddVps"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_add_vps_preflight_result_refreshes_init_ready_state(self) -> None:
        """The async pre-flight result must refresh the top Init ready card and button."""
        source = HTML_PATH.read_text(encoding="utf-8")
        marker = "if (msg.type === 'vps_ready_result')"
        start = source.find(marker)
        assert start >= 0
        block = source[start : source.find("if (msg.type === 'public_ip_result')", start)]

        assert "setHtmlIfChanged('add-preflight-checks', renderAddPreflightChecks())" in block
        assert "setHtmlIfChanged('add-status-details', renderAddStatusDetails(store.addForm))" in block
        assert "refreshLocalInteractiveState()" in block

    def test_dirty_optional_fields_survive_live_config_refresh(self) -> None:
        """Cleared optional fields must stay dirty while remote metadata is stale."""
        bootstrap = """
        const store = {
          view: 'vps-setup',
          hostname: 'manibot70',
          vps: {},
          detail: {
            kind: 'vps',
            hostname: 'manibot70',
            config: {
              bucket: 'pbguimani:',
              install_dir: '/home/mani/software',
              swap: '4G'
            },
            log_preview: {},
            branches: {}
          }
        };
        function refreshLocalInteractiveState() {}
        function renderSidebarActions() {}
        function renderStatusFlags() { return ''; }
        function setHtmlIfChanged() {}
        """
        assertions = """
        var ui = ensureVpsUi('manibot70', store.detail);
        assert.equal(canSaveForm(ui.form, ui.savedForm), false);

        setVpsField('manibot70', 'bucket', '');
        assert.equal(ui.form.bucket, '');
        assert.equal(ui.dirtyFields.bucket, true);
        assert.equal(canSaveForm(ui.form, ui.savedForm), true);

        store.detail = Object.assign({}, store.detail, {
          config: {
            bucket: 'pbguimani:',
            install_dir: '/opt/pbgui',
            swap: '8G'
          }
        });
        ensureVpsUi('manibot70', store.detail);

        assert.equal(ui.form.bucket, '');
        assert.equal(ui.savedForm.bucket, 'pbguimani:');
        assert.equal(ui.form.install_dir, '/opt/pbgui');
        assert.equal(ui.form.swap, '8G');
        assert.equal(canSaveForm(ui.form, ui.savedForm), true);
        """
        _run_node_assertions(
            ["defaultVpsUi", "markVpsFieldDirtyState", "syncCurrentVpsFormFromDom", "ensureVpsUi", "setVpsField", "canSaveForm"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_dom_setup_field_survives_focus_triggered_refresh(self) -> None:
        """A focus-triggered refresh must not restore a cleared bucket from stale detail."""
        bootstrap = """
        var domFields = [];
        const store = {
          view: 'vps-setup',
          hostname: 'manibot70',
          vps: {},
          detail: {
            kind: 'vps',
            hostname: 'manibot70',
            config: {
              bucket: 'pbguimani:',
              install_dir: '/home/mani/software',
              swap: '4G'
            },
            log_preview: {},
            branches: {}
          }
        };
        global.document = {
          querySelectorAll: function(selector) {
            if (selector === '[data-vps-field]') return domFields;
            return [];
          }
        };
        function formField(name, value, host) {
          return {
            type: 'text',
            value: value,
            checked: false,
            getAttribute: function(attr) {
              if (attr === 'data-vps-field') return name;
              if (attr === 'data-vps-host') return host;
              return '';
            }
          };
        }
        """
        assertions = """
        var ui = ensureVpsUi('manibot70', store.detail);
        assert.equal(ui.form.bucket, 'pbguimani:');
        assert.equal(canSaveForm(ui.form, ui.savedForm), false);

        domFields = [
          formField('bucket', '', 'manibot70')
        ];
        store.detail = Object.assign({}, store.detail, {
          config: {
            bucket: 'pbguimani:',
            install_dir: '/home/mani/software',
            swap: '4G'
          }
        });
        ensureVpsUi('manibot70', store.detail);

        assert.equal(ui.form.bucket, '');
        assert.equal(ui.dirtyFields.bucket, true);
        assert.equal(ui.savedForm.bucket, 'pbguimani:');
        assert.equal(canSaveForm(ui.form, ui.savedForm), true);
        """
        _run_node_assertions(
            ["defaultVpsUi", "markVpsFieldDirtyState", "syncCurrentVpsFormFromDom", "ensureVpsUi", "canSaveForm"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_read_vps_settings_prompts_when_password_missing(self) -> None:
        """Read VPS settings asks for a password before sending without one."""
        bootstrap = """
        var sent = [];
        var prompts = [];
        var renders = [];
        var domUserPw = '';
        const ui = { form: { user_pw: '' }, readSettingsLoading: false };
        const store = {
          detail: { kind: 'vps', hostname: 'manibot90' },
          suppressVpsDomSyncHost: ''
        };
        global.setTimeout = function(fn, delay) {
          fn();
          return 1;
        };
        global.document = {
          querySelectorAll: function(selector) {
            if (selector !== '[data-vps-field="user_pw"]') return [];
            return [{
              get value() { return domUserPw; },
              set value(next) { domUserPw = String(next || ''); },
              getAttribute: function(attr) {
                if (attr === 'data-vps-host') return 'manibot90';
                return '';
              }
            }];
          }
        };
        function ensureVpsUi(hostname, detail) {
          if (store.suppressVpsDomSyncHost !== hostname) {
            ui.form.user_pw = domUserPw;
          }
          return ui;
        }
        function getEffectiveVpsUserPw(hostname) {
          return ensureVpsUi(hostname, store.detail).form.user_pw;
        }
        function hasSessionSecret(hostname, field) {
          return false;
        }
        function openPasswordPrompt(hostname, onConfirm, options) {
          prompts.push({ hostname: hostname, options: options });
          onConfirm('fresh-password');
        }
        function buildSecretAwareForm(hostname, form) {
          return { user_pw: form.user_pw || '' };
        }
        function renderUi(options) {
          renders.push({ loading: ui.readSettingsLoading, options: options || {} });
        }
        function send(payload) {
          sent.push(payload);
        }
        """
        assertions = """
        refreshVpsSettings('manibot90');

        assert.equal(prompts.length, 1);
        assert.equal(prompts[0].hostname, 'manibot90');
        assert.equal(prompts[0].options.confirmLabel, 'Read Settings');
        assert.equal(sent.length, 1);
        assert.equal(sent[0].cmd, 'read_vps_settings');
        assert.equal(sent[0].hostname, 'manibot90');
        assert.equal(sent[0].form.user_pw, 'fresh-password');
        assert.equal(domUserPw, 'fresh-password');
        assert.equal(ui.readSettingsLoading, true);
        assert.equal(renders.length, 1);
        assert.equal(renders[0].loading, true);
        """
        _run_node_assertions(
            ["setPromptedVpsUserPassword", "refreshVpsSettings"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_save_vps_config_prompts_when_firewall_password_missing(self) -> None:
        """Saving firewall changes asks for a password before sending."""
        bootstrap = """
        var sent = [];
        var prompts = [];
        var passwordAvailable = false;
        const ui = {
          form: {
            user_pw: '',
            firewall: true,
            firewall_ssh_port: '22',
            firewall_ssh_ips: '198.51.100.1'
          },
          savedForm: {
            user_pw: '',
            firewall: false,
            firewall_ssh_port: 22,
            firewall_ssh_ips: ''
          }
        };
        const store = {
          detail: { kind: 'vps', hostname: 'manibot90' }
        };
        function ensureVpsUi(hostname, detail) {
          if (passwordAvailable) ui.form.user_pw = 'fresh-password';
          return ui;
        }
        function getEffectiveVpsUserPw(hostname) {
          return passwordAvailable ? 'fresh-password' : '';
        }
        function hasSessionSecret(hostname, field) {
          return false;
        }
        function openPasswordPrompt(hostname, onConfirm, options) {
          prompts.push({ hostname: hostname, options: options });
          passwordAvailable = true;
          onConfirm('fresh-password');
        }
        function buildSecretAwareForm(hostname, form) {
          return Object.assign({}, form);
        }
        function send(payload) {
          sent.push(payload);
        }
        """
        assertions = """
        saveVpsConfig('manibot90');

        assert.equal(prompts.length, 1);
        assert.equal(prompts[0].hostname, 'manibot90');
        assert.equal(prompts[0].options.confirmLabel, 'Apply Changes');
        assert.equal(sent.length, 1);
        assert.equal(sent[0].cmd, 'save_vps_config');
        assert.equal(sent[0].hostname, 'manibot90');
        assert.equal(sent[0].form.user_pw, 'fresh-password');
        assert.equal(sent[0].form.firewall, true);
        assert.equal(sent[0].form.firewall_ssh_ips, '198.51.100.1');
        """
        _run_node_assertions(
            ["vpsFirewallSettingsChanged", "saveVpsConfig"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_save_vps_config_prompts_when_swap_password_missing(self) -> None:
        """Saving swap changes asks for a password before sending."""
        bootstrap = """
        var sent = [];
        var prompts = [];
        var passwordAvailable = false;
        const ui = {
          form: { user_pw: '', swap: '4G', firewall: false, firewall_ssh_port: 22, firewall_ssh_ips: '' },
          savedForm: { user_pw: '', swap: '2G', firewall: false, firewall_ssh_port: 22, firewall_ssh_ips: '' }
        };
        const store = { detail: { kind: 'vps', hostname: 'manibot90' } };
        function ensureVpsUi(hostname, detail) {
          if (passwordAvailable) ui.form.user_pw = 'fresh-password';
          return ui;
        }
        function getEffectiveVpsUserPw(hostname) {
          return passwordAvailable ? 'fresh-password' : '';
        }
        function hasSessionSecret(hostname, field) { return false; }
        function openPasswordPrompt(hostname, onConfirm, options) {
          prompts.push({ hostname: hostname, options: options });
          passwordAvailable = true;
          onConfirm('fresh-password');
        }
        function buildSecretAwareForm(hostname, form) { return Object.assign({}, form); }
        function send(payload) { sent.push(payload); }
        """
        assertions = """
        saveVpsConfig('manibot90');

        assert.equal(prompts.length, 1);
        assert.equal(prompts[0].options.confirmLabel, 'Apply Changes');
        assert.equal(sent.length, 1);
        assert.equal(sent[0].cmd, 'save_vps_config');
        assert.equal(sent[0].form.user_pw, 'fresh-password');
        assert.equal(sent[0].form.swap, '4G');
        """
        _run_node_assertions(
            ["vpsFirewallSettingsChanged", "vpsSwapSettingsChanged", "saveVpsConfig"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_save_existing_vps_import_prompts_for_local_sudo_when_hosts_missing(self) -> None:
        """Saving an import asks for local sudo before adding /etc/hosts."""
        bootstrap = """
        var prompts = [];
        var posts = [];
        var rendered = 0;
        var closed = 0;
        var toasts = [];
        const store = {
          master: { sudoPw: '' },
          importExistingVps: {
            loading: false,
            saving: false,
            form: {
              hostname: 'manibot90',
              ip: '23.94.74.212',
              user: 'mani',
              user_pw: 'fresh-password',
              install_dir: '/home/mani/software'
            },
            probe: {
              can_save: true,
              local_hosts_update_required: true
            }
          }
        };
        global.setTimeout = function(fn, delay) {
          fn();
          return 1;
        };
        function defaultExistingVpsImportState() { return { form: {}, probe: null }; }
        function openMasterPasswordPrompt(onConfirm, commandText) {
          prompts.push(commandText);
          store.master.sudoPw = 'local-sudo-password';
          onConfirm();
        }
        function renderExistingVpsImportModal() { rendered += 1; }
        function vpsManagerPost(path, payload) {
          posts.push({ path: path, payload: payload });
          return {
            then: function(onResolve) {
              onResolve({ hostname: 'manibot90', message: 'Imported VPS saved.' });
              return { catch: function() {} };
            }
          };
        }
        function closeAlertModal() { closed += 1; }
        function toast(message, kind) { toasts.push({ message: message, kind: kind }); }
        function sendContext() {}
        function scheduleVpsDetailFetch(hostname) {}
        function renderUi(options) {}
        function send(payload) {}
        """
        assertions = """
        saveExistingVpsImport();

        assert.deepEqual(prompts, ['Save Imported VPS']);
        assert.equal(posts.length, 1);
        assert.equal(posts[0].path, '/import/save');
        assert.equal(posts[0].payload.hostname, 'manibot90');
        assert.equal(posts[0].payload.ip, '23.94.74.212');
        assert.equal(posts[0].payload.local_sudo_pw, 'local-sudo-password');
        assert.equal(rendered, 1);
        assert.equal(closed, 1);
        assert.equal(toasts[0].message, 'Imported VPS saved.');
        """
        _run_node_assertions(
            ["existingVpsImportPayload", "saveExistingVpsImport"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_read_vps_settings_progress_updates_steps(self) -> None:
        """Read VPS settings progress events update and render ordered steps."""
        bootstrap = """
        var renders = [];
        const store = {
          view: 'vps-setup',
          hostname: 'manibot90',
          vps: {},
          detail: { kind: 'vps', hostname: 'manibot90' }
        };
        function syncCurrentVpsFormFromDom(hostname, ui) { return false; }
        function isVpsContextView() { return true; }
        function renderUi(options) { renders.push(options || {}); }
        function esc(value) { return String(value || ''); }
        function escAttr(value) { return String(value || ''); }
        """
        assertions = """
        updateReadSettingsProgress('manibot90', { step: 'ssh', label: 'Connecting to VPS', status: 'running' });
        updateReadSettingsProgress('manibot90', { step: 'firewall', label: 'Reading UFW firewall settings', status: 'running' });
        updateReadSettingsProgress('manibot90', { step: 'done', label: 'VPS settings refreshed', status: 'done' });

        const ui = store.vps.manibot90;
        assert.equal(ui.readSettingsLoading, false);
        assert.equal(ui.readSettingsProgress.current, 'done');
        assert.equal(ui.readSettingsProgress.steps.length, 3);
        assert.equal(ui.readSettingsProgress.steps[0].status, 'done');
        assert.equal(ui.readSettingsProgress.steps[1].status, 'done');
        assert.equal(ui.readSettingsProgress.steps[2].status, 'done');
        const html = renderReadSettingsProgress(ui);
        assert.match(html, /Read VPS settings completed/);
        assert.match(html, /read-settings-progress/);
        assert.match(html, /deploy-progress-bar/);
        assert.match(html, /read-settings-task/);
        assert.match(html, /100%/);
        assert.match(html, /Connecting to VPS/);
        assert.match(html, /Reading UFW firewall settings/);
        assert.match(html, /Password/);
        assert.doesNotMatch(html, /Open/);
        assert.equal(renders.length, 3);
        """
        _run_node_assertions(
            ["defaultVpsUi", "ensureVpsUi", "updateReadSettingsProgress", "renderReadSettingsProgress"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_read_vps_settings_updates_detail_before_render(self) -> None:
        """Freshly read VPS settings must survive the forced setup-view render."""
        bootstrap = """
        var renders = [];
        var toasts = [];
        var domFields = [];
        const store = {
          view: 'vps-setup',
          hostname: 'manibot90',
          suppressVpsDomSyncHost: '',
          vps: {
            manibot90: {
              initialized: true,
              readSettingsLoading: true,
              form: {
                user_pw: 'session-password',
                firewall: false,
                firewall_ssh_port: 22,
                firewall_ssh_ips: ''
              },
              savedForm: {
                user_pw: 'session-password',
                firewall: false,
                firewall_ssh_port: 22,
                firewall_ssh_ips: ''
              },
              dirtyFields: {}
            }
          },
          detail: {
            kind: 'vps',
            hostname: 'manibot90',
            config: {
              firewall: false,
              firewall_ssh_port: 22,
              firewall_ssh_ips: ''
            }
          }
        };
        global.document = {
          querySelectorAll: function(selector) {
            if (selector === '[data-vps-field]') return domFields;
            return [];
          }
        };
        function formField(name, value, host, type, checked) {
          return {
            type: type || 'text',
            value: value,
            checked: !!checked,
            getAttribute: function(attr) {
              if (attr === 'data-vps-field') return name;
              if (attr === 'data-vps-host') return host;
              return '';
            }
          };
        }
        function isVpsContextView() { return true; }
        function renderUi(options) {
          ensureVpsUi(store.hostname, store.detail);
          renders.push({
            options: options || {},
            firewall: store.vps.manibot90.form.firewall,
            firewall_ssh_port: store.vps.manibot90.form.firewall_ssh_port,
            firewall_ssh_ips: store.vps.manibot90.form.firewall_ssh_ips
          });
        }
        function toast(message, kind) { toasts.push({ message: message, kind: kind }); }
        """
        assertions = """
        domFields = [
          formField('firewall', '', 'manibot90', 'checkbox', false),
          formField('firewall_ssh_port', '22', 'manibot90'),
          formField('firewall_ssh_ips', '', 'manibot90')
        ];
        handleResult({
          cmd: 'read_vps_settings',
          data: {
            firewall: true,
            firewall_ssh_port: 22,
            firewall_ssh_ips: '198.51.100.1, 203.0.113.7'
          }
        });

        assert.equal(store.detail.config.firewall, true);
        assert.equal(store.detail.config.firewall_ssh_port, 22);
        assert.equal(store.detail.config.firewall_ssh_ips, '198.51.100.1, 203.0.113.7');
        assert.equal(store.vps.manibot90.form.firewall, true);
        assert.equal(store.vps.manibot90.form.firewall_ssh_ips, '198.51.100.1, 203.0.113.7');
        assert.equal(store.vps.manibot90.form.user_pw, 'session-password');
        assert.equal(store.vps.manibot90.readSettingsLoading, false);
        assert.equal(store.vps.manibot90.savedForm.firewall_ssh_ips, '198.51.100.1, 203.0.113.7');
        assert.deepEqual(store.vps.manibot90.dirtyFields, {});
        assert.equal(renders.length, 1);
        assert.equal(renders[renders.length - 1].firewall_ssh_ips, '198.51.100.1, 203.0.113.7');
        assert.equal(store.suppressVpsDomSyncHost, '');
        assert.equal(toasts[0].message, 'VPS settings refreshed.');
        """
        _run_node_assertions(
            ["defaultVpsUi", "markVpsFieldDirtyState", "syncCurrentVpsFormFromDom", "ensureVpsUi", "handleResult"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_active_vps_setup_field_holds_live_view_render(self) -> None:
        """Focused setup inputs should not be replaced by automatic live renders."""
        bootstrap = """
        const store = { view: 'vps-setup', hostname: 'manibot90' };
        function activeField(name, host) {
          return {
            getAttribute: function(attr) {
              if (attr === 'data-vps-field') return name;
              if (attr === 'data-vps-host') return host;
              return '';
            }
          };
        }
        global.document = { activeElement: null };
        """
        assertions = """
        document.activeElement = activeField('firewall_ssh_ips', 'manibot90');
        assert.equal(isActiveVpsSetupFormField(), true);

        document.activeElement = activeField('firewall_ssh_ips', 'manibot91');
        assert.equal(isActiveVpsSetupFormField(), false);

        store.view = 'vps';
        document.activeElement = activeField('firewall_ssh_ips', 'manibot90');
        assert.equal(isActiveVpsSetupFormField(), false);
        """
        _run_node_assertions(
            ["isActiveVpsSetupFormField"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_sidebar_host_switch_keeps_change_vps_view(self) -> None:
        """Switching hosts from Change VPS stays in the Change VPS subview."""
        bootstrap = """
        var selected = [];
        const store = { view: 'vps-setup', hostname: 'manibot90' };
        function selectView(view, hostname) {
          selected.push({ view: view, hostname: hostname });
          store.view = view;
          store.hostname = hostname;
        }
        function getOverviewRows() {
          return [{ hostname: 'manibot90', pb8_installed: true }, { hostname: 'manibot91', pb8_installed: true }];
        }
        """
        assertions = """
        selectSidebarVpsHost('manibot91');

        assert.deepEqual(selected, [{ view: 'vps-setup', hostname: 'manibot91' }]);
        assert.equal(store.view, 'vps-setup');
        assert.equal(store.hostname, 'manibot91');

        store.view = 'vps';
        selectSidebarVpsHost('manibot90');
        assert.equal(selected[1].view, 'vps');
        """
        _run_node_assertions(
            ["selectSidebarVpsHost"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_master_resource_meters_use_master_history_host(self) -> None:
        """Master memory/disk/swap history links use the master hostname, not store.hostname."""
        bootstrap = """
        const store = { view: 'master', hostname: '' };
        function esc(value) { return String(value == null ? '' : value); }
        function escAttr(value) { return String(value == null ? '' : value).replace(/&/g, '&amp;').replace(/'/g, '&#39;').replace(/"/g, '&quot;'); }
        function resourceTone() { return 'ok'; }
        function formatLatency() { return '1s'; }
        function formatCpuTelemetry() {
          return {
            title: 'CPU Utilisation:',
            liveTone: 'ok',
            livePct: 1,
            liveValueText: '1.0%',
            avg60Confirmed: true,
            avg60Tone: 'ok',
            avg60Pct: 1,
            avg60ValueText: '1.0%'
          };
        }
        function renderServiceRows() { return ''; }
        function renderRunningPb7Fallback() { return ''; }
        function renderHlAccountLimits() { return ''; }
        """
        assertions = """
        const metric = { free_mb: 1, used_mb: 2, total_mb: 3, usage_pct: 4, usage_60s_peak: 5, usage_60s_window: 60 };
        const html = renderMonitorPanel(
          { server: { mem: metric, disk: metric, swap: metric, cpu: 1, cpu_60s: 1, cpu_60s_window: 60 }, v7: [], v7_running: [] },
          true,
          { summary_row: { hostname: 'magicnucpro', name: 'magicnucpro (local)' } }
        );

        assert.equal((html.match(/data-history-host='magicnucpro'/g) || []).length, 4);
        assert.equal(html.includes("data-history-host=''"), false);
        assert.equal(html.includes("data-history-metric='memory'"), true);
        assert.equal(html.includes("data-history-metric='disk'"), true);
        assert.equal(html.includes("data-history-metric='swap'"), true);
        """
        _run_node_assertions(
            ["renderResourceMeter", "renderMonitorPanel"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_systemd_migration_button_shows_running_state(self) -> None:
        """The sidebar distinguishes a running migration from a stale needed preview."""
        bootstrap = """
        """
        assertions = """
        const model = getVpsSystemdMigrationButtonModel(
          'manibot92',
          { status: { systemd_migration: { state: 'running', migration_complete: false, migration_needed: true } } },
          {}
        );

        assert.equal(model.className, 'sb-btn warning');
        assert.equal(model.text, 'Systemd migration running');
        """
        _run_node_assertions(
            ["getVpsSystemdMigrationButtonModel"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_systemd_migration_button_unknown_state_is_neutral(self) -> None:
        """Unknown migration state should be a neutral preview action, not a warning."""
        bootstrap = """
        """
        assertions = """
        const model = getVpsSystemdMigrationButtonModel('manibot91', { status: {} }, {});

        assert.equal(model.className, 'sb-btn');
        assert.equal(model.text, 'Preview systemd migration');
        """
        _run_node_assertions(
            ["getVpsSystemdMigrationButtonModel"],
            bootstrap=bootstrap,
            assertions=assertions,
        )


def test_config_apply_log_opens_exact_run_from_start() -> None:
    """A completed apply response retains early output and selects its own run."""
    _run_node_assertions(
        ["handleResult", "switchToVpsTaskLog", "getVpsTaskLogFile"],
        bootstrap="""
            const store = {hostname: 'manibot52', vps: {}};
            function toast() {}
            function scheduleVpsDetailFetch() {}
            function sendContext() {}
            function renderUi() {}
        """,
        assertions="""
            handleResult({cmd: 'save_vps_config', data: {remote_apply: {
                started: true, command: 'vps-apply-config',
                filename: 'vps-apply-config--run-123.log'
            }}});
            assert.equal(store.taskLog.file, 'VPSAction:manibot52:vps-apply-config--run-123.log');
            assert.equal(store.taskLog.startEmpty, false);
            assert.equal(store.view, 'vps-task-log');
            switchToVpsTaskLog('manibot52', 'vps-setup', 'Setup VPS');
            assert.equal(store.taskLog.file, 'VPSAction:manibot52:vps-setup');
            assert.equal(store.taskLog.startEmpty, true);
        """,
    )


def test_setup_reload_waits_for_saved_config_before_rendering_fields() -> None:
    """Provisional defaults must never become edits over the saved swap size."""
    _run_node_assertions(
        ["renderVpsSetupView", "ensureVpsUi", "defaultVpsUi",
         "syncCurrentVpsFormFromDom", "markVpsFieldDirtyState"],
        bootstrap="""
            const store = {view: 'vps-setup', hostname: 'manibot52', vps: {},
                state: {config: {swap_options: ['0', '2G', '4G']}},
                detail: {kind: 'vps', hostname: 'manibot52', provisional: true, config: {}}};
            let domFields = [];
            const document = {querySelectorAll: () => domFields};
            function sendContext() {}
            function esc(value) { return String(value); }
            const escAttr = esc;
            function canSetupForm() { return false; }
            function canSaveForm() { return false; }
            function canApplyVpsConfig() { return true; }
            function renderVpsStatusFields() { return ''; }
            function renderStatusFlags() { return ''; }
            function renderVpsRemoteServices() { return ''; }
            function renderVisibilityInput() { return ''; }
            function renderSecretHint() { return ''; }
            function renderReadSettingsProgress() { return ''; }
            function mount(html) {
                domFields = [];
                if (html.includes("data-vps-field='swap'")) {
                    const selected = html.match(/<option value='([^']+)' selected>/);
                    domFields.push({type: 'select-one', value: selected ? selected[1] : '0',
                        getAttribute: name => name === 'data-vps-field' ? 'swap' : 'manibot52'});
                }
            }
        """,
        assertions="""
            mount(renderVpsSetupView());
            assert.equal(domFields.length, 0);
            store.detail = {kind: 'vps', hostname: 'manibot52',
                config: {swap: '2G', install_dir: '/home/mani/software'}};
            ensureVpsUi('manibot52', store.detail);
            const html = renderVpsSetupView();
            assert.ok(html.includes("value='2G' selected"));
            assert.ok(html.includes("value='/home/mani/software'"));
            mount(html);
            const ui = ensureVpsUi('manibot52', store.detail);
            assert.equal(ui.form.swap, '2G');
            assert.equal(ui.dirtyFields.swap, undefined);
            domFields[0].value = '4G';
            ensureVpsUi('manibot52', store.detail);
            assert.equal(ui.form.swap, '4G');
            assert.equal(ui.dirtyFields.swap, true);
            store.state = null;
            assert.ok(!renderVpsSetupView().includes("data-vps-field='swap'"));
        """,
    )


def test_apply_button_allows_swap_change_without_password() -> None:
    """A swap edit enables Apply before the password prompt is confirmed."""
    _run_node_assertions(
        ["canApplyVpsConfig", "refreshLocalInteractiveState", "saveVpsConfig",
         "vpsFirewallSettingsChanged", "vpsSwapSettingsChanged"],
        bootstrap="""
            const ui = {
                form: {swap: '4G', user_pw: '', install_dir: '/home/mani/software'},
                savedForm: {swap: '2G', user_pw: ''}
            };
            const store = {view: 'vps-setup', hostname: 'manibot52', detail: null};
            const button = {disabled: true};
            const document = {getElementById: id => id === 'vps-save-btn' ? button : null};
            let confirmPassword;
            const sent = [];
            function ensureVpsUi() { return ui; }
            function validateFirewallIps() { return {ok: true}; }
            function syncCurrentVpsSecretsFromDom() { return false; }
            function getEffectiveVpsUserPw() { return ui.form.user_pw; }
            function hasSessionSecret() { return false; }
            function openPasswordPrompt(host, callback) {
                assert.equal(host, 'manibot52');
                confirmPassword = callback;
            }
            function buildSecretAwareForm(host, form) { return {...form}; }
            function send(message) { sent.push(message); }
        """,
        assertions="""
            refreshLocalInteractiveState();
            assert.equal(button.disabled, false);
            saveVpsConfig('manibot52');
            assert.equal(typeof confirmPassword, 'function');
            assert.equal(sent.length, 0);
            refreshLocalInteractiveState();
            assert.equal(button.disabled, false);
            ui.form.user_pw = 'test-password';
            confirmPassword();
            assert.equal(sent.length, 1);
            assert.equal(sent[0].cmd, 'save_vps_config');
            assert.equal(sent[0].form.swap, '4G');
            assert.equal(sent[0].form.user_pw, 'test-password');
            assert.equal(sent[0].form.check_remote_swap, true);
            ui.form.user_pw = '';
            ui.savedForm = {...ui.form};
            sent.length = 0;
            confirmPassword = null;
            refreshLocalInteractiveState();
            assert.equal(button.disabled, false);
            saveVpsConfig('manibot52');
            assert.equal(typeof confirmPassword, 'function');
            assert.equal(sent.length, 0);
            ui.form.user_pw = 'test-password';
            confirmPassword();
            assert.equal(sent.length, 1);
            assert.equal(sent[0].form.check_remote_swap, true);
        """,
    )
