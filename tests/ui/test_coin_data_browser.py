"""Coin Data lifecycle through the real menu, with every request intercepted."""

from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse

import pytest
from playwright.sync_api import expect, sync_playwright
from fastapi import HTTPException
from api.coin_data import _select_quotes


ROOT = Path(__file__).resolve().parents[2]


def snapshot(exchange="bybit", market_cap=0):
    """Return safe, synthetic GUI-406 rows and complete view metadata."""
    rows = [{"coin": coin, "symbol": coin + "USDT", "ccxt_symbol": coin + "/USDT:USDT",
             "quote": "USDT", "cmc_id": index, "market_cap": index * 1000000,
             "volume_24h": 1000, "vol_mcap": 0.01, "price_last": 1,
             "tags": ["synthetic"], "notices": [], "copy_trading": True}
            for index, coin in enumerate(["GUI406A", "GUI406B"], 1)]
    return {"cmc_pool": {"ready": True, "active_credentials": 1},
            "filters": {"exchange": exchange, "market_cap": market_cap, "vol_mcap": 10,
                        "tags": [], "quotes": ["USDT"], "only_cpt": False},
            "options": {"exchanges": ["bybit", "binance", "hyperliquid"], "tags": ["synthetic"],
                        "available_quotes": ["USDT"], "vol_mcap_values": [1, 10]},
            "available_quotes": ["USDT"], "selected_quotes": ["USDT"],
            "meta": {"cmc_line": "CMC current", "exchange_line": "Exchange current",
                     "cmc_line_detail": "Synthetic CMC", "exchange_line_detail": "Synthetic exchange"},
            "counts": {"main": 2, "unmatched_visible": 0, "unmatched_all": 0, "hip3": 0},
            "sections": {"main_title": "Filtered symbols (2)", "unmatched_title": "CMC unmatched (0)",
                         "hip3_title": "HIP-3 symbols (0)"},
            "warnings": [], "rows": rows, "unmatched_rows": [], "hip3_rows": []}


@pytest.fixture
def coin_browser():
    """Serve actual HTML/assets and synthetic API responses without any real PBGui."""
    with sync_playwright() as driver:
        browser = driver.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 850})
        page.set_default_timeout(5000)
        page.clock.install()
        state = SimpleNamespace(page=page, errors=[], requests=[], ensure_calls=0, polls=[],
                                jobs={"gui406-a": "running", "gui406-b": "running"},
                                lost_ensure=False, poll_error=False, unauthorized=False,
                                snapshots=[], hold_snapshot=False, pending=[], extra_tags=False,
                                mapping_missing=False, quotes=["USDT"], hip3_dexes=[], snapshot_error=None)
        page.on("pageerror", lambda error: state.errors.append(str(error)))

        def respond(route):
            """All browser traffic terminates here, including simulated failures."""
            url = urlparse(route.request.url)
            path = url.path
            state.requests.append((route.request.method, path))
            if path == "/app/start.html":
                route.fulfill(content_type="text/html", body=(
                    '<nav id="topnav"></nav><script>window.PBGUI_NAV_CONFIG={current:"welcome",authenticated:true};'
                    '</script><script src="/app/pbgui_nav.js"></script>'
                ))
            elif path == "/api/coin-data/main_page":
                source = (ROOT / "frontend/coin_data.html").read_text()
                for key, value in {"API_BASE": "/api/coin-data", "WS_BASE": "ws://coin.test",
                                   "VERSION": "test", "SERIAL": "1", "NAV_HASH": "test"}.items():
                    source = source.replace("%%" + key + "%%", value)
                route.fulfill(content_type="text/html", body=source, headers={"Cache-Control": "no-store"})
            elif path == "/api/coin-data/ensure":
                state.ensure_calls += 1
                if state.unauthorized:
                    route.fulfill(status=401, json={"detail": "Synthetic session expired"})
                elif state.lost_ensure:
                    state.lost_ensure = False
                    route.abort()
                else:
                    route.fulfill(json={"job_ids": [key for key, status in state.jobs.items() if status == "running"],
                                        "cmc_pool": {"ready": True, "active_credentials": 1}, "retry_at": 0})
            elif path.startswith("/api/coin-data/refresh/jobs/"):
                job_id = path.rsplit("/", 1)[-1]
                state.polls.append(job_id)
                if state.poll_error:
                    route.abort()
                elif job_id not in state.jobs:
                    route.fulfill(status=404, json={"detail": "Synthetic expired job"})
                else:
                    status = state.jobs[job_id]
                    route.fulfill(json={"job": {"status": status, "percent": 40 if status == "running" else 100,
                                               "message": "Synthetic " + job_id + " " + status,
                                               "state": snapshot("binance", 999)}})
            elif path == "/api/coin-data/state":
                if state.snapshot_error:
                    route.fulfill(status=422, json={"detail": state.snapshot_error})
                    return
                query = parse_qs(url.query)
                data = snapshot(query.get("exchange", ["bybit"])[0], float(query.get("market_cap", [0])[0]))
                available = [] if state.mapping_missing else state.quotes
                try:
                    selected = _select_quotes(data["filters"]["exchange"], available, query.get("quotes"))
                except HTTPException as error:
                    route.fulfill(status=error.status_code, json={"detail": error.detail})
                    return
                data["filters"]["quotes"] = selected
                data["selected_quotes"] = selected
                data["available_quotes"] = available
                data["options"]["available_quotes"] = available
                if state.mapping_missing:
                    data["rows"] = []
                    data["counts"]["main"] = 0
                if data["filters"]["exchange"] == "hyperliquid":
                    data["hip3_rows"] = [dict(data["rows"][0], dex=dex, is_hip3=True,
                                             ccxt_symbol=dex + ":GUI406/USDT:USDT")
                                         for dex in state.hip3_dexes if data["rows"]]
                    data["counts"]["hip3"] = len(data["hip3_rows"])
                if state.extra_tags:
                    data["options"]["tags"].append("new-synthetic-tag")
                state.snapshots.append(data)
                if state.hold_snapshot:
                    state.pending.append((route, data))
                else:
                    route.fulfill(json=data)
            elif path == "/api/help/index":
                route.fulfill(json=[{"file": "27_coin_data", "title": "Coin Data"}])
            elif path == "/api/help/content":
                query = parse_qs(url.query)
                directory = "help_de" if query.get("lang") == ["de"] else "help"
                route.fulfill(json={"content": (ROOT / "docs" / directory / "27_coin_data.md").read_text()})
            elif path.startswith("/app/"):
                asset = ROOT / "frontend" / path.removeprefix("/app/")
                route.fulfill(body=asset.read_bytes() if asset.is_file() else b"",
                              content_type="text/css" if asset.suffix == ".css" else "text/javascript")
            elif path.endswith("/stream"):
                route.fulfill(status=204, body="")
            else:
                route.fulfill(json={})

        page.route("**/*", respond)
        page.goto("http://coin.test/app/start.html")
        yield state
        browser.close()


def open_coin(state):
    """Use the production Information → Coin Data destination."""
    page = state.page
    page.locator('.nav-group-btn[data-group="information"]').click()
    page.locator('.nav-item[data-page="info_coin_data"]').click()
    assert page.url.startswith("http://coin.test/api/coin-data/main_page")
    expect(page.locator("#main-body tr.data-row")).to_have_count(2)
    page.clock.run_for(10)
    page.wait_for_function("!autoInFlight && Object.keys(autoJobs).length === 2")


def test_background_jobs_disconnect_reload_and_authentication(coin_browser):
    """Dismiss/Escape, lost responses, reload and expiry preserve or stop observation."""
    state, page = coin_browser, coin_browser.page
    open_coin(state)
    assert page.locator('[id^="btn-refresh-"]').count() == 0
    expect(page.locator("#action-status")).to_have_attribute("role", "status")
    expect(page.locator("#action-status")).to_have_attribute("aria-live", "polite")
    expect(page.locator("#busy-overlay")).to_have_class("visible")
    page.locator("#btn-busy-dismiss").click()
    expect(page.locator("#busy-overlay")).not_to_have_class("visible")
    previous_polls = len(state.polls)
    page.clock.run_for(2600)
    page.wait_for_function("!autoInFlight")
    assert len(state.polls) > previous_polls
    state.poll_error = True
    page.clock.run_for(2600)
    expect(page.locator("#action-status")).to_contain_text("reconnecting")
    assert page.evaluate("Object.keys(autoJobs).length") == 2
    state.poll_error = False
    state.lost_ensure = True
    page.clock.run_for(2100)
    expect(page.locator("#action-status")).to_contain_text("reconnecting")
    page.clock.run_for(4100)
    page.wait_for_function("!autoInFlight && autoFailures === 0")
    page.reload()
    page.clock.run_for(10)
    page.wait_for_function("!autoInFlight && Object.keys(autoJobs).length === 2")
    expect(page.locator("#busy-overlay")).to_have_class("visible")
    page.keyboard.press("Escape")
    expect(page.locator("#busy-overlay")).not_to_have_class("visible")
    state.unauthorized = True
    page.clock.run_for(2600)
    expect(page.locator("#action-status")).to_contain_text("Authentication lost")
    count = state.ensure_calls
    page.clock.run_for(60000)
    assert state.ensure_calls == count
    assert not any(method == "POST" and path.startswith("/api/coin-data/refresh/") for method, path in state.requests)
    assert not state.errors


def test_completion_preserves_filters_selection_sort_drafts_and_navigation(coin_browser):
    """Terminal job snapshots cannot replace current filters or rebuild focused controls."""
    state, page = coin_browser, coin_browser.page
    open_coin(state)
    page.locator("#btn-busy-dismiss").click()
    page.locator('th[data-table="main"][data-key="coin"]').click()
    page.locator("#main-body tr.data-row").first.click()
    selected = page.evaluate("uiState.selectedKey")
    sort = page.evaluate("sortState")
    page.locator("#filter-market-cap").fill("0.25")
    page.locator("#filter-market-cap").focus()
    state.jobs = {key: "completed" for key in state.jobs}
    page.clock.run_for(2700)
    page.wait_for_function("!autoInFlight && Object.keys(autoJobs).length === 0")
    expect(page.locator("#filter-market-cap")).to_have_value("0.25")
    assert page.evaluate("document.activeElement.id") == "filter-market-cap"
    assert page.evaluate("filters.exchange") == "bybit"
    assert page.evaluate("uiState.selectedKey") == selected
    assert page.evaluate("sortState") == sort
    page.reload()
    page.clock.run_for(10)
    expect(page.locator("#main-body tr.selected")).to_have_count(1)
    assert page.evaluate("uiState.selectedKey") == selected
    assert page.evaluate("sortState") == sort
    assert not state.errors


def test_late_snapshot_and_return_do_not_restore_old_exchange(coin_browser):
    """A superseded snapshot is rejected; returning rediscovers the same jobs."""
    state, page = coin_browser, coin_browser.page
    page.add_init_script("""(() => {
      const originalFetch = window.fetch;
      window.fetch = (url, options) => originalFetch(url,
        String(url).includes('/state?') ? {...options, signal: undefined} : options);
    })();""")
    open_coin(state)
    page.locator("#btn-busy-dismiss").click()
    state.hold_snapshot = True
    page.evaluate("() => {loadState(true);}")
    page.wait_for_timeout(20)
    assert len(state.pending) == 1
    state.hold_snapshot = False
    page.locator("#filter-exchange").select_option("binance")
    page.wait_for_function("serverState.filters.exchange === 'binance'")
    route, old_data = state.pending.pop()
    route.fulfill(json=old_data)
    page.wait_for_timeout(20)
    assert page.evaluate("filters.exchange") == "binance"
    assert page.evaluate("serverState.filters.exchange") == "binance"
    assert not state.errors


def test_return_unknown_jobs_and_tag_draft_are_safe(coin_browser):
    """Unknown job IDs reconcile; changed tag options never clear a focused draft."""
    state, page = coin_browser, coin_browser.page
    open_coin(state)
    page.locator("#btn-busy-dismiss").click()
    page.locator("#tag-filter-input").fill("syn")
    state.extra_tags = True
    del state.jobs["gui406-a"]
    page.clock.run_for(2600)
    page.wait_for_function("!autoInFlight")
    expect(page.locator("#tag-filter-input")).to_have_value("syn")
    assert page.evaluate("document.activeElement.id") == "tag-filter-input"
    assert page.evaluate("Object.keys(autoJobs)") == ["gui406-b"]
    page.evaluate("window.dispatchEvent(new Event('pagehide')); window.dispatchEvent(new Event('pageshow'))")
    page.clock.run_for(10)
    page.wait_for_function("!autoInFlight && !autoStopped")
    assert page.evaluate("Object.keys(autoJobs)") == ["gui406-b"]
    assert not state.errors


def test_missing_mapping_with_saved_quote_keeps_monitoring_after_reload(coin_browser):
    """The real 422 quote guard cannot orphan running jobs while a cache is rebuilt."""
    state, page = coin_browser, coin_browser.page
    open_coin(state)
    state.mapping_missing = True
    page.reload()
    page.clock.run_for(10)
    expect(page.locator("#action-status")).to_contain_text("Mapping is not available yet")
    page.locator("#btn-busy-dismiss").click()
    prior_polls = len(state.polls)
    page.clock.run_for(6000)
    page.wait_for_function("!autoInFlight")
    assert len(state.polls) > prior_polls
    assert not page.evaluate("autoStopped")
    assert page.evaluate("filters.quotes") == ["USDT"]
    assert page.evaluate("Object.keys(autoJobs)") == ["gui406-a", "gui406-b"]
    state.mapping_missing = False
    state.jobs = {key: "completed" for key in state.jobs}
    page.clock.run_for(2600)
    page.wait_for_function("!autoInFlight && !!serverState && Object.keys(autoJobs).length === 0")
    expect(page.locator("#main-body tr.data-row")).to_have_count(2)
    expect(page.locator("#action-status")).not_to_contain_text("Mapping is not available yet")
    assert page.evaluate("filters.quotes") == ["USDT"]
    assert not page.evaluate("autoStopped")
    assert not state.errors


def test_invalid_view_parameters_do_not_stop_or_repeatedly_retry_snapshots(coin_browser):
    """A permanent view rejection is retried only after context changes; jobs continue."""
    state, page = coin_browser, coin_browser.page
    open_coin(state)
    page.locator("#btn-busy-dismiss").click()
    state.snapshot_error = "Synthetic invalid view parameter"
    page.clock.run_for(2600)
    expect(page.locator("#action-status")).to_contain_text("Coin Data view unavailable")
    snapshots = sum(path == "/api/coin-data/state" for _, path in state.requests)
    polls = len(state.polls)
    page.clock.run_for(6000)
    page.wait_for_function("!autoInFlight")
    assert len(state.polls) > polls
    assert sum(path == "/api/coin-data/state" for _, path in state.requests) == snapshots
    assert not page.evaluate("autoStopped")
    state.snapshot_error = None
    page.locator("#filter-market-cap").fill("250")
    page.clock.run_for(300)
    page.wait_for_function("serverState.filters.market_cap === 250")
    expect(page.locator("#action-status")).not_to_contain_text("Coin Data view unavailable")
    assert not state.errors


@pytest.mark.parametrize("retain_valid_subset", [False, True])
def test_disappeared_quotes_are_reconciled_without_weakening_validation(coin_browser, retain_valid_subset):
    """Keep remaining valid quotes, or explain the fallback to the exchange default."""
    state, page = coin_browser, coin_browser.page
    if retain_valid_subset:
        state.quotes = ["USDT", "USDC", "USD"]
    open_coin(state)
    page.locator("#btn-busy-dismiss").click()
    if retain_valid_subset:
        page.locator('[data-quote="USDC"]').click()
        page.wait_for_function("filters.quotes.length === 2 && !autoInFlight")
    state.quotes = ["USDC", "USD"] if retain_valid_subset else ["USDC"]
    page.clock.run_for(2700)
    page.wait_for_function("!autoInFlight && filters.quotes.join(',') === 'USDC'")
    expect(page.locator("#action-status")).to_contain_text("previous quote selection is no longer available")
    expect(page.locator('[data-quote="USDC"]')).to_have_attribute("aria-pressed", "true")
    if retain_valid_subset:
        expect(page.locator('[data-quote="USD"]')).to_have_attribute("aria-pressed", "false")
    assert not page.evaluate("autoStopped")
    assert page.evaluate("filters.exchange") == "bybit"
    assert not state.errors


@pytest.mark.parametrize("selected_dex_remains", [False, True])
def test_hip3_dex_options_follow_background_snapshots(coin_browser, selected_dex_remains):
    """Add current options, preserve a valid selection, or explain All DEXes fallback."""
    state, page = coin_browser, coin_browser.page
    state.hip3_dexes = ["gui406-dex-a"]
    open_coin(state)
    page.locator("#btn-busy-dismiss").click()
    page.locator("#filter-exchange").select_option("hyperliquid")
    page.wait_for_function("serverState.filters.exchange === 'hyperliquid'")
    page.locator("#btn-view-hip3").click()
    page.locator("#filter-hip3-dex").select_option("gui406-dex-a")
    page.locator("#filter-hip3-dex").focus()
    state.hip3_dexes = (["gui406-dex-a"] if selected_dex_remains else []) + ["gui406-dex-b"]
    page.clock.run_for(2700)
    page.wait_for_function("!autoInFlight && serverState.hip3_rows.some(row => row.dex === 'gui406-dex-b')")
    options = page.locator("#filter-hip3-dex").evaluate("el => Array.from(el.options).map(option => option.value)")
    assert options == [""] + state.hip3_dexes
    expect(page.locator("#filter-hip3-dex")).to_have_value("gui406-dex-a" if selected_dex_remains else "")
    expect(page.locator("#hip3-body tr.data-row")).to_have_count(1)
    assert page.evaluate("document.activeElement.id") == "filter-hip3-dex"
    assert page.evaluate("uiState.activeView") == "hip3"
    if not selected_dex_remains:
        expect(page.locator("#action-status")).to_contain_text("Showing All DEXes")
        assert "hip3_dex=" not in page.url
    assert not page.evaluate("autoStopped")
    assert not state.errors


@pytest.mark.parametrize("width", [160, 420])
def test_sidebar_and_guide_keep_the_current_destination(coin_browser, width):
    """Shared sidebar sizes and the real Guide overlay leave Coin Data in place."""
    state, page = coin_browser, coin_browser.page
    open_coin(state)
    page.locator("#btn-busy-dismiss").click()
    page.evaluate("(width) => {localStorage.setItem('pbgui.sidebar.width', String(width)); location.reload();}", width)
    page.clock.run_for(10)
    page.wait_for_function("!!serverState")
    assert page.locator("#sidebar").evaluate("el => el.scrollWidth <= el.clientWidth")
    assert abs(page.locator("#sidebar").bounding_box()["width"] - width) <= 2
    page.keyboard.press("Escape")
    page.locator("#pbgui-guide-btn").click()
    page.wait_for_function("!!document.getElementById('pbgui-shared-help-ovl')")
    assert page.url.startswith("http://coin.test/api/coin-data/main_page")
    assert not state.errors
