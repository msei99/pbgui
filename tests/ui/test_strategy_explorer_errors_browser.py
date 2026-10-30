"""Real shared Explorer shell/menu with intercepted, synthetic API failures."""

import json
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlparse

import pytest
from browser_driver import expect, sync_browser

ROOT = Path(__file__).resolve().parents[2]
REASON = "bot.long.ema_span_0 is a legacy flat strategy field. Run passivbot tool migrate-config-v7."


def _snapshot(config):
    """Return a safe snapshot without bot processes, files or exchange requests."""
    return {
        "ok": True, "source": "posted", "title": "Synthetic recovery", "config": config,
        "market": {"exchange": "binance", "coin": "BTC", "reference_price": 100,
                   "engine_status": "Synthetic engine", "metadata": {}, "ohlcv_status": "Candles disabled"},
        "sides": {}, "candles": [], "messages": [],
    }


@pytest.fixture
def explorer_browser():
    """Serve real local assets while intercepting every browser HTTP request."""
    with sync_browser() as driver:
        browser = driver.chromium.launch()
        context = browser.new_context(viewport={"width": 1280, "height": 900})
        page = context.new_page()
        page.set_default_timeout(8000)
        state = SimpleNamespace(page=page, errors=[], snapshot_error=False, requests=[],
                                probe_status=503, probe_body="", bootstrap_status=422, snapshot_status=422,
                                hold_snapshots=False, pending_snapshots=[], hold_markets=False, pending_markets=[],
                                bootstrap_snapshot=None, snapshot_detail=REASON)
        page.on("pageerror", lambda error: state.errors.append(str(error)))

        def respond(route):
            """Prevent all network/runtime access, including menu background calls."""
            path = urlparse(route.request.url).path
            state.requests.append((route.request.method, path))
            if path == "/app/start.html":
                route.fulfill(content_type="text/html", body=(
                    '<nav id="topnav"></nav><script>window.PBGUI_NAV_CONFIG={current:"welcome",authenticated:true};'
                    '</script><script src="/app/pbgui_nav.js"></script>'
                ))
            elif path.endswith("/main_page") and "/strategy-explorer" in path:
                source = (ROOT / "frontend/v7_strategy_explorer.html").read_text()
                values = {"API_BASE": path.removesuffix("/main_page"), "WS_BASE": "ws://explorer.test",
                          "VERSION": "test", "SERIAL": "1", "NAV_HASH": "test", "DRAFT_ID": "", "RESULT_PATH": ""}
                for key, value in values.items():
                    source = source.replace("%%" + key + "%%", value)
                route.fulfill(content_type="text/html", body=source, headers={"Cache-Control": "no-store"})
            elif "/strategy-explorer" in path and path.endswith("/session"):
                runtime = "PB8" if "-v8/" in path else "Passivbot V7"
                reason = REASON if state.bootstrap_status == 422 else f"{runtime} runtime unavailable: synthetic missing runtime"
                if state.bootstrap_status == 200:
                    config = {"backtest": {"starting_balance": 1000}, "bot": {"long": {}, "short": {}}, "live": {}}
                    snapshot = state.bootstrap_snapshot if state.bootstrap_snapshot is not None else _snapshot(config)
                    route.fulfill(json={"snapshot": snapshot, "page": {}, "movie": {}, "messages": snapshot.get("messages", [])})
                else:
                    route.fulfill(status=state.bootstrap_status, json={"detail": reason})
            elif "/strategy-explorer" in path and path.endswith("/snapshot"):
                if state.hold_snapshots:
                    state.pending_snapshots.append(route)
                    return
                if state.snapshot_error:
                    route.fulfill(status=state.snapshot_status, json={"detail": state.snapshot_detail})
                else:
                    config = route.request.post_data_json["config"]
                    route.fulfill(json=_snapshot(config))
            elif "/strategy-explorer" in path and path.endswith("/markets"):
                if state.hold_markets:
                    state.pending_markets.append(route)
                    return
                route.fulfill(json={"ok": True, "exchanges": ["binance"], "coins_by_exchange": {"binance": ["BTC"]}})
            elif "/strategy-explorer" in path and path.endswith(("/simulate", "/compare", "/movie/frames")):
                route.fulfill(status=422, json={"detail": REASON})
            elif path.endswith("/probe"):
                route.fulfill(status=state.probe_status, body=state.probe_body)
            elif path == "/api/help/index":
                route.fulfill(json=[])
            elif path == "/static/plotly.min.js":
                route.fulfill(content_type="text/javascript", body=(ROOT / "frontend/plotly.min.js").read_bytes())
            elif path.startswith("/app/"):
                asset = ROOT / "frontend" / path.removeprefix("/app/")
                route.fulfill(body=asset.read_bytes() if asset.is_file() else b"",
                              content_type="text/css" if asset.suffix == ".css" else "text/javascript")
            elif path.endswith("/stream"):
                route.abort()
            else:
                route.fulfill(json={})

        state.respond = respond
        page.route("**/*", respond)
        page.goto("http://explorer.test/app/start.html")
        yield state
        browser.close()


@pytest.mark.parametrize("version,sidebar_width", [("v7", 160), ("v8", 420)])
@pytest.mark.parametrize("bootstrap_status", [422, 503])
def test_menu_bootstrap_failure_raw_recovery_and_followup_errors(
    explorer_browser, version, sidebar_width, bootstrap_status
):
    """A failed bootstrap ends Loading; editing recovers without leaving the stage."""
    state = explorer_browser
    page = state.page
    state.bootstrap_status = bootstrap_status
    page.locator(f'.nav-group-btn[data-group="pb{version}"]').click()
    page.locator(f'.nav-item[data-page="{version}_strategy_explorer"]').click()
    suffix = "-v8" if version == "v8" else ""
    assert page.url == f"http://explorer.test/api/strategy-explorer{suffix}/main_page"
    expect(page.locator("#source-chip")).to_have_text("Session unavailable")
    expect(page.locator("#engine-chip")).to_have_text("Engine unavailable")
    expect(page.locator("#messages")).to_contain_text(REASON if bootstrap_status == 422 else "synthetic missing runtime")
    assert '{"detail"' not in page.locator("#messages").inner_text()
    page.locator('[data-stage="raw"]').click()
    page.evaluate("width => { document.getElementById('sidebar').style.width = width + 'px'; }", sidebar_width)
    assert page.locator("#sidebar").evaluate("el => el.scrollWidth <= el.clientWidth")
    config = {"backtest": {"starting_balance": 1000}, "bot": {"long": {}, "short": {}},
              "live": {"approved_coins": {"long": ["BTC"], "short": ["BTC"]}},
              "pbgui": {"note": "Synthetic recovery"}}
    editor = page.locator("#raw-config-json")
    editor.fill(json.dumps(config))
    expect(page.locator("#raw-config-status")).to_have_text("JSON valid. Snapshot updated.")
    expect(page.locator("#source-chip")).to_have_text("Source: Synthetic recovery")
    expect(page.locator('[data-stage="raw"]')).to_have_class("sb-section active")
    assert not page.locator("#messages").is_visible()
    assert page.evaluate("state.config.pbgui.note") == "Synthetic recovery"

    # Rejected follow-up edits stay in the editor and do not reset navigation.
    state.snapshot_error = True
    config["pbgui"]["note"] = "Synthetic rejected edit"
    editor.fill(json.dumps(config))
    expect(page.locator("#raw-config-status")).to_contain_text(REASON)
    expect(editor).to_contain_text("Synthetic rejected edit")
    expect(page.locator('[data-stage="raw"]')).to_have_class("sb-section active")
    state.snapshot_error = False
    config["pbgui"]["note"] = "Synthetic corrected edit"
    editor.fill(json.dumps(config))
    expect(page.locator("#raw-config-status")).to_have_text("JSON valid. Snapshot updated.")

    for stage, button in [("simulation", "#btn-run-local-sim"), ("compare", "#btn-run-compare"), ("movie", "#btn-build-movie")]:
        page.locator(f'[data-stage="{stage}"]').click()
        page.locator(button).click()
        expect(page.locator("#messages")).to_contain_text(REASON)
        expect(page.locator(button)).to_be_enabled()
        expect(page.locator(f'[data-stage="{stage}"]')).to_have_class("sb-section active")
    for endpoint in ("/simulate", "/compare", "/movie/frames"):
        assert ("POST", f"/api/strategy-explorer{suffix}" + endpoint) in state.requests
    assert state.errors == []


def test_api_fetch_text_status_fallback_and_json_detail(explorer_browser):
    """Non-JSON and empty failures retain useful fallback text/status."""
    state = explorer_browser
    page = state.page
    page.locator('.nav-group-btn[data-group="pbv7"]').click()
    page.locator('.nav-item[data-page="v7_strategy_explorer"]').click()
    expect(page.locator("#source-chip")).to_have_text("Session unavailable")
    for body, expected in [("synthetic unavailable", "synthetic unavailable"), ("", "HTTP 503"),
                           (json.dumps({"detail": REASON}), REASON)]:
        state.probe_body = body
        result = page.evaluate("() => apiFetch('/probe').catch(error => error.message)")
        assert result == expected
    assert state.errors == []


def test_pb8_reload_503_preserves_cached_edit_context_and_recovers(explorer_browser):
    """Exercise the real menu, reload, separate Guide and recovery using only intercepted data."""
    state = explorer_browser
    page = state.page
    state.bootstrap_status = 200
    page.locator('.nav-group-btn[data-group="pbv8"]').click()
    page.locator('.nav-item[data-page="v8_strategy_explorer"]').click()
    expect(page.locator('#source-chip')).to_have_text('Source: Synthetic recovery')
    assert page.url.endswith('/api/strategy-explorer-v8/main_page')
    expect(page.locator('#topnav')).to_be_visible()
    expect(page.locator('#main-content')).to_be_visible()
    page.locator('[data-stage="raw"]').click()
    config = {"config_version": 8, "backtest": {"starting_balance": 4321},
              "bot": {"long": {}, "short": {}}, "live": {}}
    page.locator('#raw-config-json').fill(json.dumps(config))
    expect(page.locator('#raw-config-status')).to_have_text('JSON valid. Snapshot updated.')
    # A nondefault stage and controls must survive a failed recovery snapshot.
    page.locator('[data-stage="movie"]').click()
    page.locator('#movie-frames-input').fill('37')
    page.locator('[data-stage="raw"]').click()
    saved = page.evaluate('sessionStorage.getItem(STRATEGY_REFRESH_CACHE_KEY)')
    assert json.loads(saved)['config'] == config
    key = page.evaluate('STRATEGY_REFRESH_CACHE_KEY')
    page.add_init_script("""
      window.cacheWrites = [];
      window.savedAtDocumentStart = sessionStorage.getItem(""" + json.dumps(key) + """);
      const originalSetItem = Storage.prototype.setItem;
      Storage.prototype.setItem = function(key, value) {
        if (this === sessionStorage) window.cacheWrites.push(key);
        return originalSetItem.call(this, key, value);
      };
    """)
    state.snapshot_error = True
    state.snapshot_status = 503
    page.reload()
    expect(page.locator('#messages')).to_contain_text('Failed to load Strategy Explorer session:')
    expect(page.locator('#source-chip')).to_have_text('Session unavailable')
    expect(page.locator('[data-stage="raw"]')).to_have_class('sb-section active')
    expect(page.locator('#raw-config-json')).to_contain_text('4321')
    expect(page.locator('#movie-frames-input')).to_have_value('37')
    expect(page.locator('#exchange-select')).to_have_value('binance')
    expect(page.locator('#coin-select')).to_have_value('BTC')
    assert page.evaluate('state.snapshot') is None
    # beforeunload legitimately updates saved_at on the old successful page.
    initial_saved = page.evaluate('window.savedAtDocumentStart')
    assert json.loads(initial_saved)['config'] == json.loads(saved)['config']
    saved = initial_saved
    assert page.evaluate('window.cacheWrites') == []
    assert page.evaluate('sessionStorage.getItem(STRATEGY_REFRESH_CACHE_KEY)') == saved
    # Even navigation and the unload persistence hook cannot overwrite it.
    page.locator('[data-stage="movie"]').click()
    assert page.evaluate('sessionStorage.getItem(STRATEGY_REFRESH_CACHE_KEY)') == saved
    state.snapshot_error = False
    page.reload()
    expect(page.locator('#source-chip')).to_have_text('Source: Synthetic recovery')
    expect(page.locator('[data-stage="raw"]')).to_have_class('sb-section active')
    assert page.evaluate('state.config') == config
    expect(page.locator('#movie-frames-input')).to_have_value('37')
    assert page.evaluate('Object.keys(explorerRequests).length') == 0
    # Successful automatic recalculation also resumes persistence after a failed
    # restore, without requiring another browser reload or resetting the stage.
    state.snapshot_error = True
    page.reload()
    expect(page.locator('#source-chip')).to_have_text('Session unavailable')
    state.snapshot_error = False
    config['backtest']['starting_balance'] = 5432
    page.locator('#raw-config-json').fill(json.dumps(config))
    expect(page.locator('#raw-config-status')).to_have_text('JSON valid. Snapshot updated.')
    expect(page.locator('[data-stage="raw"]')).to_have_class('sb-section active')
    assert page.evaluate('strategyRefreshRecoveryPending') is False
    assert json.loads(page.evaluate('sessionStorage.getItem(STRATEGY_REFRESH_CACHE_KEY)'))['config'] == config
    # Guide is an overlay on the same destination, with explicit close.
    destination = page.url
    page.locator('#pbgui-guide-btn').click()
    expect(page.locator('#pbgui-shared-help-ovl')).to_be_visible()
    assert page.url == destination
    page.locator('#pbgui-shared-help-close').click()
    expect(page.locator('#pbgui-shared-help-ovl')).not_to_be_visible()
    for viewport, width in [(1280, 240), (1280, 160), (1280, 420), (740, None), (780, 420)]:
        page.set_viewport_size({"width": viewport, "height": 900})
        if width is not None:
            page.evaluate("width => { document.getElementById('sidebar').style.width = width + 'px'; }", width)
        assert page.locator('#sidebar').evaluate('el => el.scrollWidth <= el.clientWidth')
        expect(page.locator('[data-stage="movie"]')).to_be_visible()
    assert state.errors == []


def test_pb8_parallel_tabs_load_synthetic_sessions(explorer_browser):
    """Multiple browser tabs retain independent state while sharing the real Explorer entry point."""
    state = explorer_browser
    page = state.page
    state.bootstrap_status = 200
    # The fixture's route applies to this page. Copy the same intercepted handler
    # to additional pages; no request can escape to a runtime or exchange.
    context = page.context
    extra = [context.new_page() for _ in range(2)]
    try:
        for other in extra:
            other.route('**/*', state.respond)
        for other in [page, *extra]:
            other.goto('http://explorer.test/api/strategy-explorer-v8/main_page', wait_until='domcontentloaded')
        for other in [page, *extra]:
            expect(other.locator('#source-chip')).to_have_text('Source: Synthetic recovery')
    finally:
        for other in extra:
            other.close()


@pytest.mark.parametrize('snapshot_status', [200, 503])
def test_pb8_edits_during_delayed_restore_and_market_response(explorer_browser, snapshot_status):
    """Keep current stage/Frames through delayed success or 503 and a later market response."""
    state = explorer_browser
    page = state.page
    state.bootstrap_status = 200
    page.locator('.nav-group-btn[data-group="pbv8"]').click()
    page.locator('.nav-item[data-page="v8_strategy_explorer"]').click()
    expect(page.locator('#source-chip')).to_have_text('Source: Synthetic recovery')
    page.locator('[data-stage="raw"]').click()
    config = {"config_version": 8, "backtest": {"starting_balance": 4321},
              "bot": {"long": {}, "short": {}}, "live": {}}
    page.locator('#raw-config-json').fill(json.dumps(config))
    expect(page.locator('#raw-config-status')).to_have_text('JSON valid. Snapshot updated.')
    page.locator('[data-stage="movie"]').click()
    page.locator('#movie-frames-input').fill('37')
    page.locator('[data-stage="raw"]').click()
    state.hold_snapshots = True
    page.reload(wait_until='domcontentloaded')
    page.wait_for_function("Object.prototype.hasOwnProperty.call(explorerRequests, '/snapshot')")
    assert len(state.pending_snapshots) == 1
    expect(page.locator('#movie-frames-input')).to_have_value('37')
    stored = page.evaluate('sessionStorage.getItem(STRATEGY_REFRESH_CACHE_KEY)')
    assert json.loads(stored)['controls']['movie_frames'] == '37'
    page.locator('[data-stage="movie"]').click()
    page.locator('#movie-frames-input').fill('99')
    state.hold_snapshots = False
    snapshot_route = state.pending_snapshots.pop()
    if snapshot_status == 503:
        snapshot_route.fulfill(status=503, json={'detail': 'Synthetic delayed overload'})
        expect(page.locator('#source-chip')).to_have_text('Session unavailable')
        expect(page.locator('#messages')).to_contain_text('Synthetic delayed overload')
        expect(page.locator('#movie-frames-input')).to_have_value('99')
        expect(page.locator('[data-stage="movie"]')).to_have_class('sb-section active')
        assert page.evaluate('state.snapshot') is None
        assert page.evaluate('sessionStorage.getItem(STRATEGY_REFRESH_CACHE_KEY)') == stored
        assert page.evaluate('state.config') == config
        # After the temporary fault, a valid edit saves the current controls,
        # including edits that were deliberately not persisted during the 503.
        page.locator('[data-stage="raw"]').click()
        config['backtest']['starting_balance'] = 5432
        page.locator('#raw-config-json').fill(json.dumps(config))
        expect(page.locator('#raw-config-status')).to_have_text('JSON valid. Snapshot updated.')
        page.locator('[data-stage="movie"]').click()
        expect(page.locator('#movie-frames-input')).to_have_value('99')
        saved = json.loads(page.evaluate('sessionStorage.getItem(STRATEGY_REFRESH_CACHE_KEY)'))
        assert saved['config'] == config
        assert saved['controls']['movie_frames'] == '99'
        page.reload()
        expect(page.locator('[data-stage="movie"]')).to_have_class('sb-section active')
        expect(page.locator('#movie-frames-input')).to_have_value('99')
    else:
        state.hold_markets = True
        snapshot_route.fulfill(json=_snapshot(snapshot_route.request.post_data_json['config']))
        page.wait_for_function("Object.prototype.hasOwnProperty.call(explorerRequests, '/markets')")
        assert len(state.pending_markets) == 1
        page.locator('#movie-frames-input').fill('123')
        page.locator('#movie-visible-input').fill('75')
        state.hold_markets = False
        state.pending_markets.pop().fulfill(json={
            'ok': True, 'exchanges': ['binance'], 'coins_by_exchange': {'binance': ['BTC']}
        })
        expect(page.locator('#source-chip')).to_have_text('Source: Synthetic recovery')
        expect(page.locator('#movie-frames-input')).to_have_value('123')
        expect(page.locator('#movie-visible-input')).to_have_value('75')
        expect(page.locator('[data-stage="movie"]')).to_have_class('sb-section active')
        page.wait_for_function('strategyRefreshRecoveryPending === false')
        saved = json.loads(page.evaluate('sessionStorage.getItem(STRATEGY_REFRESH_CACHE_KEY)'))
        assert saved['config'] == config
        assert saved['controls']['stage'] == 'movie'
        assert saved['controls']['movie_frames'] == '123'
        assert saved['controls']['movie_visible'] == '75'
        page.reload()
        expect(page.locator('[data-stage="movie"]')).to_have_class('sb-section active')
        expect(page.locator('#movie-frames-input')).to_have_value('123')
    assert state.errors == []


def test_pb8_missing_ema_tabs_show_native_failure_and_preserve_cached_context(explorer_browser):
    """Synthetic native 422 is distinct from overload and missing candles in cold and cached tabs."""
    from api import strategy_explorer_v8 as api

    state, detail = explorer_browser, 'orchestrator compute_ideal_orders failed: MissingEma { symbol_idx: 0 }'
    page = state.page
    config = {'backtest': {'exchanges': ['binance'], 'starting_balance': 4321},
              'bot': {'long': {}, 'short': {}}, 'live': {'approved_coins': {'long': ['BTC'], 'short': []}}}
    state.bootstrap_status = 200
    state.bootstrap_snapshot = api._unavailable_snapshot(config, {}, detail)
    page.locator('.nav-group-btn[data-group="pbv8"]').click()
    page.locator('.nav-item[data-page="v8_strategy_explorer"]').click()
    extra = [page.context.new_page() for _ in range(2)]
    try:
        for other in extra:
            other.route('**/*', state.respond)
            other.goto('http://explorer.test/api/strategy-explorer-v8/main_page')
        for other in [page, *extra]:
            expect(other.locator('#engine-chip')).to_have_text('PB8 snapshot unavailable')
            expect(other.locator('#engine-chip')).to_have_class('chip err')
            expect(other.locator('#messages')).to_contain_text('MissingEma')
            expect(other.locator('#ohlcv-chip')).to_have_attribute('title', 'Candle availability unknown: native snapshot failed')
            assert other.evaluate('state.snapshot.ok') is False
            assert other.evaluate('state.config') == config
            assert 'No valid local PB8 candles' not in other.locator('body').inner_text()
        # Retain a previously successful edited config when the next restoration
        # fails, even though the bootstrap response contains default values.
        config['backtest']['starting_balance'] = 5432
        page.locator('[data-stage="raw"]').click()
        page.locator('#raw-config-json').fill(json.dumps(config))
        expect(page.locator('#raw-config-status')).to_have_text('JSON valid. Snapshot updated.')
        page.locator('[data-stage="movie"]').click()
        page.locator('#movie-frames-input').fill('37')
        page.locator('[data-stage="raw"]').click()
        key = page.evaluate('STRATEGY_REFRESH_CACHE_KEY')
        page.add_init_script("""
          window.savedAtStart = sessionStorage.getItem(""" + json.dumps(key) + """);
          window.cacheWrites = [];
          const originalSet = Storage.prototype.setItem;
          Storage.prototype.setItem = function(key, value) {
            if (this === sessionStorage) window.cacheWrites.push(key);
            return originalSet.call(this, key, value);
          };
        """)
        state.snapshot_error, state.snapshot_status, state.snapshot_detail = True, 422, detail
        page.reload()
        expect(page.locator('#source-chip')).to_have_text('Session unavailable')
        expect(page.locator('#engine-chip')).to_have_class('chip err')
        expect(page.locator('#messages')).to_contain_text('MissingEma')
        expect(page.locator('[data-stage="raw"]')).to_have_class('sb-section active')
        expect(page.locator('#raw-config-json')).to_contain_text('5432')
        expect(page.locator('#movie-frames-input')).to_have_value('37')
        expect(page.locator('#exchange-select')).to_have_value('binance')
        expect(page.locator('#coin-select')).to_have_value('BTC')
        assert page.evaluate('state.snapshot') is None
        assert page.evaluate('state.config') == config
        assert page.evaluate('sessionStorage.getItem(STRATEGY_REFRESH_CACHE_KEY) === window.savedAtStart')
        assert page.evaluate('window.cacheWrites') == []
        assert state.errors == []
    finally:
        for other in extra:
            other.close()
