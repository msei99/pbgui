"""Frontend contracts for secure Market Data Tiingo credential updates."""

from javascript_checks import run_javascript

from pathlib import Path
import textwrap
from types import SimpleNamespace
from urllib.parse import urlparse

import pytest


ROOT = Path(__file__).resolve().parents[2]
PAGE = ROOT / "frontend" / "market_data_main.html"


def _extract_function(source: str, name: str) -> str:
    """Extract one named JavaScript function from the embedded page."""
    start = source.index(f"function {name}(")
    async_start = source.rfind("async ", max(0, start - 12), start)
    if async_start >= 0 and not source[async_start + len("async ") : start].strip():
        start = async_start
    brace_start = source.index("{", start)
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
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return source[start : index + 1]
    raise AssertionError(f"Could not extract JavaScript function {name!r}")


def _run_settings_js(names, script):
    """Exercise actual inline helpers with isolated browser state and fake timers."""
    source = PAGE.read_text(encoding="utf-8")
    functions = "\n".join(_extract_function(source, name) for name in names)
    prelude = r"""
    const assert = browserAssert;
    const elements = new Map();
    const document = {hidden: false, activeElement: null, getElementById(id) {
      if (!elements.has(id)) elements.set(id, {value: '', textContent: '', hidden: false, dataset: {},
        classList: {toggle() {}}, getAttribute() { return null; }});
      return elements.get(id);
    }};
    const storage = new Map();
    const window = {localStorage: {getItem(key) { return storage.get(key) || null; }, setItem(key, val) { storage.set(key, val); }}};
    let timers = new Map(), nextTimer = 0;
    function setTimeout(callback, delay) { timers.set(++nextTimer, {callback, delay}); return nextTimer; }
    function clearTimeout(id) { timers.delete(id); }
    let settingsState = {exchange: 'okx', payload: null, selectedCoins: new Set(['BTC']), autoEnableNewCoins: false,
      baselineRequest: 'original-baseline', isDirty: false, saveBusy: false, tiingoProfileId: '', tiingoConfigured: false};
    let settingsEditRevision = 0, settingsSaveRevision = 0, settingsLoadRequestId = 0;
    let settingsReadController = null, settingsReadPromise = null, settingsReadExchange = '', settingsSyncTimer = null;
    let settingsAuthLost = false, settingsPageLeft = false;
    let settingsFormLoading = false;
    let settingsResetExchange = '', settingsLoadDeferred = false;
    let uiState = {contextExchange: 'okx'}, renders = 0, clears = 0, requests = [], toasts = [];
    function renderSettingsPayload() { renders++; }
    function setSettingsBaseline() { settingsState.baselineRequest = 'initialized'; }
    function updateSaveSettingsButton() {}
    function setSettingsLoading(value) { settingsFormLoading = value; }
    function syncSettingsDirtyState() { settingsState.isDirty = true; }
    function renderCoinOptions() { renders++; }
    function clearTiingoRevealedToken() { clears++; }
    function renderTiingoUsage() {}
    function showToast(message) { toasts.push(message); }
    function apiUrl(path) { return '/mount/api/market-data' + path; }
    function readNumberValue() { return 1; }
    function readFieldValue(id) { return document.getElementById(id).value; }
    function deferred() { let resolve, reject; const promise = new Promise((a,b) => {resolve=a;reject=b;}); return {promise, resolve, reject}; }
    function payload(exchange='okx', coins=['BTC']) { return {exchange, coin_options: coins, enabled_coins: ['BTC'], missing_saved_coins: [], coin_data_available: true,
      auto_enable_new_coins: false, settings: {interval_seconds: 99, aws_secret_access_key: 'server-old-secret'}}; }
    """
    program = prelude + "\n" + functions + "\n(async () => {\n" + script + "\n})().catch(error => { console.error(error); finishTest(1); });"
    run_javascript(["-e", program], cwd=ROOT, check=True, capture_output=True, text=True)


def test_settings_wrapper_errors_timeout_and_authentication():
    """503/200 errors are rejected before application; reads time out and saves do not."""
    _run_settings_js(["fetchSettingsJson", "settingsSyncMessage", "stopSettingsSync"], r"""
      let response = {ok: false, status: 503, json: async () => ({detail: 'Local mapping unavailable'})};
      global.fetch = async (path, options) => { requests.push({path, options}); return response; };
      await assert.rejects(fetchSettingsJson('/settings/okx'), /Local mapping unavailable/);
      response = {ok: true, status: 200, json: async () => ({success: false, error: 'Synthetic failure'})};
      await assert.rejects(fetchSettingsJson('/settings/okx'), /Synthetic failure/);
      response = {ok: false, status: 500, json: async () => ({detail: {unsafe: 'object'}})};
      await assert.rejects(fetchSettingsJson('/settings/okx'), /HTTP 500/);
      global.fetch = (path, options) => new Promise((resolve,reject) => {
        options.signal.addEventListener('abort', () => reject(new DOMException('Aborted', 'AbortError')));
      });
      const reading = fetchSettingsJson('/settings/okx');
      const timeout = Array.from(timers.values()).find(timer => timer.delay === 5000);
      assert.ok(timeout); timeout.callback();
      await assert.rejects(reading, {name: 'AbortError'});
      assert.equal(timers.size, 0);
      global.fetch = async (path, options) => {
        assert.equal(timers.size, 0, 'A save must not imply rollback after a read timeout');
        return {ok: true, status: 200, json: async () => ({success: true})};
      };
      await fetchSettingsJson('/settings/okx', {method: 'POST', body: '{}'});
      global.fetch = async () => ({ok: false, status: 401, json: async () => ({detail: 'Expired'})});
      await assert.rejects(fetchSettingsJson('/settings/okx'), /Expired/);
      assert.equal(settingsAuthLost, true); assert.equal(clears, 1); assert.equal(timers.size, 0);
      assert.match(document.getElementById('settings-sync-error').textContent, /Authentication lost/);
      assert.equal(requests[0].path, '/mount/api/market-data/settings/okx');
    """)


def test_settings_load_revisions_coalescing_and_cleanup():
    """Exchange/save generations reject stale reads, preserving last good state on errors."""
    _run_settings_js(["loadSettings", "stopSettingsSync", "settingsSyncVisible", "scheduleSettingsSync", "resumeSettingsSync", "settingsSyncMessage", "restoreSettingsCoinFilter", "applySettingsDynamicPayload"], r"""
      let pending = [];
      global.fetchSettingsJson = (path, options) => {const value = deferred(); pending.push(value); requests.push({path,options}); return value.promise;};
      storage.set('market_data_coin_filter_okx', 'BT');
      let a = loadSettings('okx'); let joined = loadSettings('okx');
      assert.equal(pending.length, 1);
      pending[0].resolve(payload()); await Promise.all([a, joined]);
      assert.equal(renders, 1); assert.equal(settingsState.coinFilter, 'BT');
      const baseline = settingsState.baselineRequest;
      document.getElementById('settings-aws-secret-access-key').value = 'unsaved-draft';
      settingsState.selectedCoins.add('OLD');
      a = loadSettings('okx');
      pending[1].resolve(payload('okx', ['ETH'])); await a;
      assert.equal(settingsState.selectedCoins.has('OLD'), true);
      assert.equal(settingsState.baselineRequest, baseline);
      assert.equal(document.getElementById('settings-aws-secret-access-key').value, 'unsaved-draft');
      a = loadSettings('okx'); uiState.contextExchange = 'bybit';
      const b = loadSettings('bybit'); pending[3].resolve(payload('bybit', ['SOL'])); await b;
      pending[2].resolve(payload('okx', ['STALE'])); await a;
      assert.equal(settingsState.payload.exchange, 'bybit');
      assert.deepEqual(settingsState.payload.coin_options, ['SOL']);
      a = loadSettings('bybit'); settingsSaveRevision++;
      pending[4].resolve(payload('bybit', ['PRE_SAVE'])); await a;
      assert.deepEqual(settingsState.payload.coin_options, ['SOL']);
      a = loadSettings('bybit'); pending[5].reject(new Error('503 mapping')); await a;
      assert.deepEqual(settingsState.payload.coin_options, ['SOL']);
      assert.equal(document.getElementById('settings-sync-error').textContent, '503 mapping');
      assert.equal(Array.from(timers.values()).filter(timer => timer.delay === 15000).length, 1);
      a = loadSettings('bybit'); const signal = requests.at(-1).options.signal;
      document.hidden = true; resumeSettingsSync();
      assert.equal(signal.aborted, true); assert.equal(timers.size, 0);
      pending[6].resolve(payload('bybit', ['HIDDEN'])); await a;
      assert.deepEqual(settingsState.payload.coin_options, ['SOL']);
      document.hidden = false; settingsAuthLost = true; await loadSettings('bybit');
      assert.equal(pending.length, 7);
    """)


def test_settings_dynamic_updates_wait_for_drag_selection():
    """An in-flight drag keeps its DOM until the latest deferred update is safe."""
    _run_settings_js(["applySettingsDynamicPayload", "resetSettingsCoinDragSelection"], r"""
      settingsState.payload = payload(); settingsState.dragStart = {coin: 'BTC'};
      applySettingsDynamicPayload(payload('okx', ['ETH']));
      applySettingsDynamicPayload(payload('okx', ['SOL']));
      assert.deepEqual(settingsState.payload.coin_options, ['BTC']);
      assert.equal(renders, 0);
      resetSettingsCoinDragSelection();
      assert.deepEqual(settingsState.payload.coin_options, ['SOL']);
      assert.equal(renders, 1);
      assert.equal(settingsState.pendingDynamicPayload, null);
    """)


def test_best_1m_mapping_errors_preserve_last_valid_view():
    """Snapshot failures keep same-exchange selections and never leave a loading list."""
    _run_settings_js(["loadBest1mInfo"], r"""
      global.best1mState = {exchange: 'okx', enabledCoins: ['BTC'], selectedCoins: new Set(['BTC']), distributedHosts: ['saved-host']};
      uiState.best1mRequestId = 0;
      let feedback = '';
      global.clearBest1mFeedback = () => { feedback = ''; };
      global.setBest1mFeedback = message => { feedback = message; };
      global.renderBest1mDistributedHosts = () => {};
      global.renderBest1mGeneric = () => { throw new Error('Error payload must never be rendered'); };
      global.fetchJson = async () => ({success: false, error: 'Local mapping unreadable'});
      document.getElementById('best1m-coin-picker').innerHTML = '<button>BTC selected</button>';
      await loadBest1mInfo('okx');
      assert.equal(document.getElementById('best1m-coin-picker').innerHTML, '<button>BTC selected</button>');
      assert.deepEqual(Array.from(best1mState.selectedCoins), ['BTC']);
      assert.deepEqual(best1mState.distributedHosts, ['saved-host']);
      assert.equal(feedback, 'Local mapping unreadable');
      assert.equal(document.getElementById('btn-best1m-queue').disabled, true);
      await loadBest1mInfo('bybit');
      assert.match(document.getElementById('best1m-coin-picker').innerHTML, /Build options unavailable/);
      assert.equal(document.getElementById('best1m-coin-count').textContent, 'Unavailable');
    """)


def test_settings_auto_enable_without_mapping_retains_saved_request():
    """Unknown and valid empty options cannot make auto-enable send an empty deletion."""
    _run_settings_js(["collectSettingsRequest", "getAllCoins"], r"""
      settingsState.autoEnableNewCoins = true;
      settingsState.payload = Object.assign(payload(), {coin_options: [], coin_data_available: false});
      assert.deepEqual(collectSettingsRequest().enabled_coins, ['BTC']);
      settingsState.payload.coin_data_available = true;
      assert.deepEqual(collectSettingsRequest().enabled_coins, ['BTC']);
      settingsState.payload.coin_options = ['ETH'];
      assert.deepEqual(collectSettingsRequest().enabled_coins, ['ETH']);
      settingsState.autoEnableNewCoins = false;
      settingsState.selectedCoins.clear();
      assert.deepEqual(collectSettingsRequest().enabled_coins, []);
    """)


def test_settings_save_preserves_edits_made_during_request():
    """An older Save response updates only availability and keeps the newer draft baseline."""
    _run_settings_js(["saveSettings", "settingsRequestsEqual", "collectSettingsRequest", "getAllCoins", "stopSettingsSync", "scheduleSettingsSync", "settingsSyncVisible", "applySettingsDynamicPayload"], r"""
      settingsState.payload = payload();
      const pending = deferred();
      global.fetchSettingsJson = (path, options) => {requests.push({path,options}); return pending.promise;};
      let saving = saveSettings();
      const sent = requests[0].options.body;
      settingsEditRevision++; settingsState.selectedCoins.add('ETH');
      document.getElementById('settings-aws-secret-access-key').value = 'newer-secret-draft';
      pending.resolve({success: true, settings: payload('okx', ['BTC','ETH']), message: 'Saved'});
      await saving;
      assert.equal(settingsState.selectedCoins.has('ETH'), true);
      assert.equal(document.getElementById('settings-aws-secret-access-key').value, 'newer-secret-draft');
      assert.equal(settingsState.baselineRequest, sent);
      assert.equal(settingsState.saveBusy, false);
      assert.equal(settingsState.isDirty, true);
      assert.equal(timers.size, 1);
    """)


@pytest.mark.parametrize("save_success", [True, False])
def test_settings_save_exchange_change_waits_for_latest_form(save_success):
    """A delayed save cannot admit another save for a superseded exchange."""
    _run_settings_js(["saveSettings", "settingsRequestsEqual", "collectSettingsRequest", "getAllCoins", "stopSettingsSync", "scheduleSettingsSync", "settingsSyncVisible", "loadSettings", "applySettingsDynamicPayload", "restoreSettingsCoinFilter", "settingsSyncMessage", "setSettingsBaseline", "updateSaveSettingsButton"], r"""
      settingsState.payload = payload();
      setSettingsBaseline(); settingsState.isDirty = true;
      const firstSave = deferred(), nextRead = deferred(), lastSave = deferred();
      global.fetchSettingsJson = (path, options) => {
        requests.push({path,options});
        if (!options.method) return nextRead.promise;
        return requests.filter(item => item.options.method === 'POST').length === 1 ? firstSave.promise : lastSave.promise;
      };
      const saving = saveSettings();
      uiState.contextExchange = 'bybit';
      await loadSettings('bybit', {resetForm: true});
      assert.equal(settingsFormLoading, true);
      uiState.contextExchange = 'bitget';
      await loadSettings('bitget', {resetForm: true});
      assert.equal(requests.length, 1, 'Reads wait for save completion');
      firstSave.resolve(SAVE_RESULT);
      await saving;
      assert.deepEqual(requests.map(item => item.path), ['/settings/okx', '/settings/bitget']);
      assert.equal(settingsFormLoading, true);
      assert.equal(document.getElementById('btn-save-settings-sidebar').disabled, true);
      await saveSettings(); assert.equal(requests.length, 2);
      // Even direct calls cannot bypass exchange ownership by clearing loading.
      settingsFormLoading = false;
      updateSaveSettingsButton();
      assert.equal(document.getElementById('btn-save-settings-sidebar').disabled, true);
      await saveSettings(); assert.equal(requests.length, 2);
      settingsFormLoading = true;
      const reading = settingsReadPromise;
      nextRead.resolve(payload('bitget', ['SOL'])); await reading;
      assert.equal(settingsState.exchange, 'bitget');
      assert.equal(settingsState.payload.exchange, 'bitget');
      assert.equal(settingsFormLoading, false);
      assert.equal(settingsState.isDirty, false);
      settingsState.isDirty = true;
      const secondSave = saveSettings();
      assert.equal(requests.at(-1).path, '/settings/bitget');
      lastSave.resolve({success: true, settings: payload('bitget', ['SOL'])}); await secondSave;
    """.replace("SAVE_RESULT", "{success: true, settings: payload()}" if save_success else "{success: false, error: 'Synthetic write failure'}"))


def test_deferred_settings_reset_survives_hidden_document_and_retry():
    """A deferred same-exchange reset remains mandatory after hide, abort or failure."""
    _run_settings_js(["loadSettings", "stopSettingsSync", "scheduleSettingsSync", "settingsSyncVisible", "resumeSettingsSync", "applySettingsDynamicPayload", "restoreSettingsCoinFilter", "settingsSyncMessage"], r"""
      settingsState.payload = payload();
      settingsState.saveBusy = true;
      await loadSettings('okx', {resetForm: true});
      assert.equal(settingsResetExchange, 'okx');
      settingsState.saveBusy = false; document.hidden = true;
      stopSettingsSync();
      await loadSettings('okx');
      assert.equal(settingsFormLoading, true);
      let pending = deferred();
      global.fetchSettingsJson = () => pending.promise;
      document.hidden = false;
      let reading = loadSettings('okx');
      pending.reject(new Error('Synthetic retry')); await reading;
      assert.equal(settingsResetExchange, 'okx');
      assert.equal(settingsFormLoading, true);
      pending = deferred(); reading = loadSettings('okx');
      pending.resolve(payload()); await reading;
      assert.equal(settingsResetExchange, '');
      assert.equal(settingsFormLoading, false);
      assert.equal(settingsState.baselineRequest, 'initialized', 'Full reset rather than a dynamic-only sync');
    """)


@pytest.mark.parametrize("auto", [True, False])
def test_settings_availability_changes_do_not_dirty_or_rebase_form(auto):
    """Availability is derived in auto mode; manual selection and real edits stay meaningful."""
    _run_settings_js(["settingsRequestsEqual", "collectSettingsRequest", "getAllCoins", "setSettingsBaseline", "syncSettingsDirtyState", "applySettingsDynamicPayload"], r"""
      settingsState.payload = payload();
      settingsState.autoEnableNewCoins = AUTO;
      let interval = 1;
      readNumberValue = id => id === 'settings-interval-seconds' ? interval : 1;
      setSettingsBaseline();
      const baseline = settingsState.baselineRequest;
      applySettingsDynamicPayload(payload('okx', ['BTC', 'ETH']));
      syncSettingsDirtyState();
      assert.equal(settingsState.isDirty, false);
      assert.equal(settingsState.baselineRequest, baseline);
      interval = 2; syncSettingsDirtyState(); assert.equal(settingsState.isDirty, true);
      interval = 1; syncSettingsDirtyState(); assert.equal(settingsState.isDirty, false);
      assert.deepEqual(collectSettingsRequest().enabled_coins, AUTO ? ['BTC','ETH'] : ['BTC']);
      settingsState.autoEnableNewCoins = !AUTO;
      syncSettingsDirtyState(); assert.equal(settingsState.isDirty, true);
      settingsState.autoEnableNewCoins = AUTO;
      syncSettingsDirtyState(); assert.equal(settingsState.isDirty, false);
      if (!AUTO) {
        settingsState.selectedCoins.add('ETH'); syncSettingsDirtyState(); assert.equal(settingsState.isDirty, true);
        settingsState.selectedCoins.delete('ETH'); syncSettingsDirtyState(); assert.equal(settingsState.isDirty, false);
      }
      assert.equal(settingsState.baselineRequest, baseline);
    """.replace("AUTO", "true" if auto else "false"))


@pytest.fixture
def market_settings_browser():
    """Intercept the real menu/page/assets with synthetic settings and no live server."""
    from browser_driver import sync_browser

    with sync_browser() as driver:
        browser = driver.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 900})
        page.set_default_timeout(5000)
        page.clock.install()
        page.add_init_script("""(() => {
          const original = window.fetch;
          window.settingsReadsPending = 0;
          window.fetch = async function(url, options) {
            const tracked = /\\/settings\\/[^/]+$/.test(String(url)) && !(options && options.method);
            if (!tracked) return original.apply(this, arguments);
            window.settingsReadsPending++;
            let response;
            try { response = await original.apply(this, arguments); }
            catch (error) { window.settingsReadsPending--; throw error; }
            const json = response.json.bind(response);
            response.json = async function() { try { return await json(); } finally { window.settingsReadsPending--; } };
            return response;
          };
        })();""")
        state = SimpleNamespace(page=page, errors=[], reads=[], posts=[], pending=[], hold=False, error=0,
                                coins={"hyperliquid": ["TESTBTC", "TESTETH"], "okx": ["OKXBTC", "OKXETH"]}, available=True,
                                saved={}, post_exchanges=[], hold_save=False, pending_saves=[])
        page.on("pageerror", lambda error: state.errors.append(str(error)))

        def data(exchange):
            """Return a harmless exchange-specific settings payload."""
            coins = state.coins.get(exchange, [exchange.upper() + "BTC"])
            saved = state.saved.get(exchange, {})
            auto = saved.get("auto_enable_new_coins", False)
            selected = saved.get("enabled_coins", coins[:1])
            enabled = (coins if auto else [coin for coin in selected if coin in coins]) if coins else selected
            missing = [coin for coin in selected if coin not in coins] if coins and not auto else []
            return {"exchange": exchange, "coin_options": coins, "enabled_coins": enabled,
                    "missing_saved_coins": missing, "coin_data_available": state.available, "auto_enable_new_coins": auto,
                    "settings": {"interval_seconds": 1800, "coin_pause_seconds": .5, "api_timeout_seconds": 30,
                                 "min_lookback_days": 2, "max_lookback_days": 4, "aws_profile": "test-only",
                                 "aws_region": "us-east-1", "aws_access_key_id": "", "aws_secret_access_key": "",
                                 "l2book_scan_timeout_s": 5, "l2book_scan_workers": 8,
                                 "tiingo_configured": False, "tiingo_profile_id": "", "tiingo_usage": {},
                                 **saved.get("settings", {})}}

        def respond(route):
            """No request, including external or background traffic, leaves the fixture."""
            path = urlparse(route.request.url).path
            if path == "/app/start.html":
                route.fulfill(content_type="text/html", body='<nav id="topnav"></nav><script>window.PBGUI_NAV_CONFIG={current:"welcome",authenticated:true};</script><script src="/app/pbgui_nav.js"></script>')
            elif path == "/api/market-data/main_page":
                source = PAGE.read_text(encoding="utf-8")
                for key, value in {"BASE_PREFIX":"", "API_BASE": "/api/market-data", "VERSION": "test", "SERIAL": "1", "NAV_HASH": "test"}.items():
                    source = source.replace("%%" + key + "%%", value)
                route.fulfill(content_type="text/html", body=source, headers={"Cache-Control": "no-store"})
            elif path.startswith("/api/market-data/settings/") and path.count("/") == 4:
                exchange = path.rsplit("/", 1)[-1]
                if route.request.method == "POST":
                    request = route.request.post_data_json
                    state.posts.append(request)
                    state.post_exchanges.append(exchange)
                    state.saved[exchange] = request
                    result = data(exchange)
                    response = {"success": True, "settings": result}
                    if state.hold_save:
                        state.pending_saves.append((route, response))
                    else:
                        route.fulfill(json=response)
                else:
                    state.reads.append(exchange)
                    if state.error:
                        route.fulfill(status=state.error if state.error != 200 else 200,
                                      json={"success": False, "detail": "Synthetic mapping read failed", "error": "Synthetic mapping read failed"})
                    elif state.hold:
                        state.pending.append((route, data(exchange)))
                    else:
                        route.fulfill(json=data(exchange))
            elif path == "/api/help/index":
                route.fulfill(json=[{"file": "26_market_data", "title": "Market Data"}])
            elif path == "/api/help/content":
                route.fulfill(json={"content": (ROOT / "docs/help/26_market_data.md").read_text()})
            elif path.startswith("/app/"):
                asset = ROOT / "frontend" / path.removeprefix("/app/")
                route.fulfill(body=asset.read_bytes() if asset.is_file() else b"",
                              content_type="text/css" if asset.suffix == ".css" else "text/javascript")
            elif path.endswith("/stream"):
                route.fulfill(status=204, body="")
            else:
                route.fulfill(json={})

        page.route("**/*", respond)
        page.goto("http://settings.test/app/start.html")
        yield state
        browser.close()


def _open_market_settings(state):
    """Use the production Information → Market Data menu destination."""
    from browser_driver import expect

    page = state.page
    page.locator('.nav-group-btn[data-group="information"]').click()
    page.locator('.nav-item[data-page="info_market_data_fastapi"]').click()
    assert page.url.startswith("http://settings.test/api/market-data/main_page")
    expect(page.locator("#settings-enabled-coins button")).to_have_count(2)


def _advance_settings_poll(page):
    """Complete each response before advancing another 15 seconds of virtual time."""
    page.wait_for_function("window.settingsReadsPending === 0")
    with page.expect_response(lambda response: "/settings/" in response.url and response.url.count("/") == 6):
        page.clock.run_for(15050)
    page.wait_for_function("window.settingsReadsPending === 0")


def test_settings_real_menu_sync_drafts_errors_and_reload(market_settings_browser):
    """Automatic updates preserve actual controls, focus, selected missing coins and navigation."""
    from browser_driver import expect

    state, page = market_settings_browser, market_settings_browser.page
    _open_market_settings(state)
    page.locator("#settings-coin-filter").fill("TEST")
    page.locator("#settings-interval-seconds").fill("90")
    page.locator("#settings-interval-seconds").focus()
    state.coins["hyperliquid"] = ["TESTETH", "TESTSOL"]
    _advance_settings_poll(page)
    expect(page.locator('[data-settings-coin-row="TESTBTC"]')).to_contain_text("unavailable")
    expect(page.locator('[data-settings-coin-row="TESTBTC"]')).to_have_class("coin-picker-row coin-picker-button settings-coin-row selected")
    expect(page.locator("#settings-interval-seconds")).to_have_value("90")
    expect(page.locator("#settings-coin-filter")).to_have_value("TEST")
    assert page.evaluate("document.activeElement.id") == "settings-interval-seconds"
    page.locator("#btn-settings-subsection-aws").click()
    page.locator("#settings-aws-secret-access-key").fill("synthetic-unsaved-draft")
    page.locator("#settings-aws-secret-access-key").focus()
    _advance_settings_poll(page)
    expect(page.locator("#settings-aws-secret-access-key")).to_have_value("synthetic-unsaved-draft")
    assert page.evaluate("document.activeElement.id") == "settings-aws-secret-access-key"
    page.locator("#btn-settings-subsection-normal").click()
    state.error = 503
    _advance_settings_poll(page)
    expect(page.locator("#settings-sync-error")).to_contain_text("Synthetic mapping read failed")
    expect(page.locator('[data-settings-coin-row="TESTBTC"]')).to_be_visible()
    state.error = 200
    _advance_settings_poll(page)
    expect(page.locator("#settings-sync-error")).to_contain_text("Synthetic mapping read failed")
    state.error = 0
    _advance_settings_poll(page)
    expect(page.locator("#settings-sync-error")).to_be_hidden()
    page.locator("#page-exchange").select_option("okx")
    page.locator("#btn-confirm-accept").click()
    expect(page.locator('[data-settings-coin-row="OKXBTC"]')).to_be_visible()
    page.locator("#settings-coin-filter").fill("OKXBTC")
    page.reload()
    expect(page.locator("#page-exchange")).to_have_value("okx")
    expect(page.locator("#settings-coin-filter")).to_have_value("OKXBTC")
    expect(page.locator("#settings-panel")).to_be_visible()
    expect(page.locator("#btn-save-settings-sidebar")).to_be_disabled()
    # Empty availability must retain the saved selection in the actual Save request.
    state.available = False
    state.coins["okx"] = []
    _advance_settings_poll(page)
    expect(page.locator("#settings-missing-coins")).to_contain_text("saved selection is retained")
    page.locator("#settings-auto-enable-new-coins").check()
    page.locator("#settings-interval-seconds").fill("120")
    page.locator("#btn-save-settings-sidebar").click()
    expect(page.locator("#btn-save-settings-sidebar")).to_be_disabled()
    assert state.posts[-1]["enabled_coins"] == ["OKXBTC"]
    assert not state.errors


def test_settings_delayed_save_exchange_switch_owns_next_save(market_settings_browser):
    """The real Exchange selector cannot leave an old editable form after a delayed POST."""
    from browser_driver import expect

    state, page = market_settings_browser, market_settings_browser.page
    _open_market_settings(state)
    state.hold_save = True
    page.locator("#settings-interval-seconds").fill("90")
    page.locator("#btn-save-settings-sidebar").click()
    page.locator("#page-exchange").select_option("okx")
    page.locator("#btn-confirm-accept").click()
    expect(page.locator("#page-exchange")).to_have_value("okx")
    expect(page.locator("#settings-panel .settings-layout")).to_have_js_property("inert", True)
    expect(page.locator("#btn-save-settings-sidebar")).to_be_disabled()
    assert len(state.pending_saves) == 1
    # Completing the previous POST must start the latest GET immediately, while
    # the old form remains inert until that GET is actually applied.
    state.hold = True
    state.hold_save = False
    route, response = state.pending_saves.pop()
    with page.expect_request(lambda request: request.url.endswith("/settings/okx") and request.method == "GET"):
        route.fulfill(json=response)
    expect(page.locator("#settings-panel .settings-layout")).to_have_js_property("inert", True)
    expect(page.locator("#btn-save-settings-sidebar")).to_be_disabled()
    state.hold = False
    for route, response in state.pending:
        assert response["exchange"] == "okx"
        route.fulfill(json=response)
    state.pending.clear()
    expect(page.locator('[data-settings-coin-row="OKXBTC"]')).to_be_visible()
    expect(page.locator("#settings-panel .settings-layout")).to_have_js_property("inert", False)
    expect(page.locator("#settings-interval-seconds")).to_have_value("1800")
    expect(page.locator("#btn-save-settings-sidebar")).to_be_disabled()
    page.locator("#settings-interval-seconds").fill("120")
    with page.expect_request(lambda request: request.url.endswith("/settings/okx") and request.method == "POST"):
        page.locator("#btn-save-settings-sidebar").click()
    assert state.post_exchanges == ["hyperliquid", "okx"]
    assert not state.errors


def test_settings_auto_enable_sync_and_reverted_edit_stay_clean(market_settings_browser):
    """Actual auto-selection updates cannot invent a draft or a discard prompt."""
    from browser_driver import expect

    state, page = market_settings_browser, market_settings_browser.page
    _open_market_settings(state)
    page.locator("#settings-auto-enable-new-coins").check()
    page.locator("#btn-save-settings-sidebar").click()
    page.wait_for_function("!document.getElementById('btn-save-settings-sidebar').classList.contains('save-needed')")
    state.coins["hyperliquid"].append("TESTSOL")
    _advance_settings_poll(page)
    expect(page.locator('[data-settings-coin-row="TESTSOL"]')).to_have_attribute("aria-pressed", "true")
    expect(page.locator("#btn-save-settings-sidebar")).to_be_disabled()
    page.locator("#settings-interval-seconds").fill("1830")
    expect(page.locator("#btn-save-settings-sidebar")).to_be_enabled()
    page.locator("#settings-interval-seconds").fill("1800")
    expect(page.locator("#btn-save-settings-sidebar")).to_be_disabled()
    page.locator("#page-exchange").select_option("okx")
    expect(page.locator('[data-settings-coin-row="OKXBTC"]')).to_be_visible()
    expect(page.locator("#confirm-ovl")).not_to_have_class("visible")
    assert not state.errors


def test_settings_real_menu_guide_sidebar_and_pending_reads(market_settings_browser):
    """Guide, shared widths, returning to Settings and late responses retain their context."""
    from browser_driver import expect

    state, page = market_settings_browser, market_settings_browser.page
    _open_market_settings(state)
    for width in [160, 420]:
        page.evaluate("width => { localStorage.setItem('pbgui.sidebar.width', String(width)); location.reload(); }", width)
        expect(page.locator("#settings-enabled-coins button")).to_have_count(2)
        assert abs(page.locator("#sidebar").bounding_box()["width"] - width) <= 2
        assert page.locator("#sidebar-toolbar").evaluate("el => el.scrollWidth <= el.clientWidth")
    page.locator("#pbgui-guide-btn").click()
    expect(page.locator("#pbgui-shared-help-ovl")).to_have_class("visible")
    assert page.url.startswith("http://settings.test/api/market-data/main_page")
    page.keyboard.press("Escape")
    for width in [740, 780]:
        page.set_viewport_size({"width": width, "height": 900})
        assert page.locator("#sidebar-toolbar").evaluate("el => el.scrollWidth <= el.clientWidth")
    page.set_viewport_size({"width": 1280, "height": 900})
    state.hold = True
    page.clock.run_for(15050)
    page.wait_for_function("document.querySelector('#settings-panel').hidden === false")
    assert len(state.pending) == 1
    page.locator("#page-exchange").select_option("okx")
    page.wait_for_function("document.querySelector('#page-exchange').value === 'okx'")
    expect(page.locator("#settings-panel .settings-layout")).to_have_js_property("inert", True)
    expect(page.locator("#btn-save-settings-sidebar")).to_be_disabled()
    # The earlier request was aborted; fulfilling it cannot restore its old exchange.
    old_route, old_data = state.pending.pop(0)
    try:
        old_route.fulfill(json=old_data)
    except Exception:
        pass  # Chromium may have already discarded the explicitly aborted request.
    state.hold = False
    page.clock.run_for(10)
    for route, data in state.pending:
        route.fulfill(json=data)
    state.pending.clear()
    expect(page.locator('[data-settings-coin-row="OKXBTC"]')).to_be_visible()
    expect(page.locator("#settings-panel .settings-layout")).to_have_js_property("inert", False)
    before = len(state.reads)
    page.locator("#btn-panel-copy-data").click()
    page.clock.run_for(16000)
    assert len(state.reads) == before
    page.locator("#btn-panel-settings").click()
    expect(page.locator('[data-settings-coin-row="OKXBTC"]')).to_be_visible()
    assert len(state.reads) > before
    assert not state.errors


def test_tiingo_settings_link_to_official_token_and_usage_pages() -> None:
    """Tiingo setup should expose its token and authoritative usage destinations."""
    source = PAGE.read_text(encoding="utf-8")

    assert 'href="https://www.tiingo.com/account/api/token"' in source
    assert 'href="https://www.tiingo.com/account/api/usage"' in source
    assert source.count('target="_blank" rel="noopener noreferrer"') >= 2


def test_tiingo_token_save_reuses_profile_and_clears_secret_input() -> None:
    """Direct configuration should update the active vault profile without retaining the token."""
    source = PAGE.read_text(encoding="utf-8")
    function = _extract_function(source, "saveTiingoToken")
    script = textwrap.dedent(
        f"""
        const assert = browserAssert;
        const input = {{value: 'new-vault-token', type: 'password', disabled: false, dataset: {{}}}};
        const button = {{disabled: false}};
        const document = {{getElementById(id) {{
          if (id === 'settings-tiingo-token') return input;
          if (id === 'btn-save-tiingo-token') return button;
          return null;
        }}}};
        const settingsState = {{
          tiingoConfigured: true,
          tiingoProfileId: 'tradfi_tiingo_1',
          tiingoSaveGeneration: 0
        }};
        const requests = [];
        const toasts = [];
        let settingsReloads = 0;
        async function fetchApiKeysJson(path, options) {{
          requests.push({{path, options: options || null}});
          if (path === '/tradfi/profiles') return {{profiles: [{{
            id: 'tradfi_tiingo_1', provider: 'tiingo', label: 'Existing Tiingo',
            active: true, pending: false, shared: true
          }}]}};
          return {{status: 'saved', profile: {{id: 'tradfi_tiingo_1', has_api_key: true}}}};
        }}
        async function loadSettings(exchange) {{
          assert.equal(exchange, 'hyperliquid');
          settingsReloads += 1;
        }}
        function showToast(message, level) {{ toasts.push({{message, level}}); }}
        {function}
        (async () => {{
          await saveTiingoToken();
          assert.equal(requests.length, 2);
          assert.equal(requests[1].path, '/tradfi/config');
          assert.equal(requests[1].options.method, 'PUT');
          assert.deepEqual(JSON.parse(requests[1].options.body), {{
            profile_id: 'tradfi_tiingo_1',
            provider: 'tiingo',
            label: 'Existing Tiingo',
            active: true,
            shared: true,
            api_key: 'new-vault-token',
            create_new: false
          }});
          assert.equal(input.value, '');
          assert.equal(input.disabled, false);
          assert.equal(button.disabled, false);
          assert.equal(settingsReloads, 1);
          assert.deepEqual(toasts, [{{
            message: 'Tiingo token saved to the credential vault.', level: 'success'
          }}]);
        }})().catch(error => {{ console.error(error); finishTest(1); }});
        """
    )

    run_javascript(["-e", script], cwd=ROOT, check=True, capture_output=True, text=True)


def test_tiingo_token_reveal_is_explicit_and_clears_when_hidden() -> None:
    """The eye action should POST one profile ID and remove the revealed value on hide."""
    source = PAGE.read_text(encoding="utf-8")
    clear_function = _extract_function(source, "clearTiingoRevealedToken")
    toggle_function = _extract_function(source, "toggleTiingoTokenVisible")
    script = textwrap.dedent(
        f"""
        const assert = browserAssert;
        const button = {{
          disabled: false,
          textContent: '',
          classList: {{contains(value) {{ return value === 'pw-eye-btn'; }}}}
        }};
        const input = {{value: '', type: 'password', dataset: {{}}, nextElementSibling: button}};
        const document = {{getElementById(id) {{
          return id === 'settings-tiingo-token' ? input : null;
        }}}};
        const settingsState = {{
          tiingoConfigured: true,
          tiingoProfileId: 'tradfi_tiingo_1',
          tiingoRevealGeneration: 0
        }};
        const requests = [];
        const toasts = [];
        async function fetchApiKeysJson(path, options) {{
          requests.push({{path, options}});
          return {{value: 'stored-vault-token'}};
        }}
        function showToast(message, level) {{ toasts.push({{message, level}}); }}
        {clear_function}
        {toggle_function}
        (async () => {{
          await toggleTiingoTokenVisible(button);
          assert.equal(requests.length, 1);
          assert.equal(requests[0].path, '/tradfi/reveal');
          assert.equal(requests[0].options.method, 'POST');
          assert.deepEqual(JSON.parse(requests[0].options.body), {{profile_id: 'tradfi_tiingo_1'}});
          assert.equal(input.value, 'stored-vault-token');
          assert.equal(input.type, 'text');
          assert.equal(input.dataset.revealed, 'true');
          assert.equal(button.disabled, false);

          await toggleTiingoTokenVisible(button);
          assert.equal(input.value, '');
          assert.equal(input.type, 'password');
          assert.equal(input.dataset.revealed, undefined);
          assert.deepEqual(toasts, []);
        }})().catch(error => {{ console.error(error); finishTest(1); }});
        """
    )

    run_javascript(["-e", script], cwd=ROOT, check=True, capture_output=True, text=True)
