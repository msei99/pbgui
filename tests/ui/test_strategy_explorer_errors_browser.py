"""Real shared Explorer shell/menu with intercepted, synthetic API failures."""

import json
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlparse

import pytest
from playwright.sync_api import expect, sync_playwright

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
    with sync_playwright() as driver:
        browser = driver.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 900})
        page.set_default_timeout(8000)
        state = SimpleNamespace(page=page, errors=[], snapshot_error=False, requests=[],
                                probe_status=503, probe_body="", bootstrap_status=422)
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
                route.fulfill(status=state.bootstrap_status, json={"detail": reason})
            elif "/strategy-explorer" in path and path.endswith("/snapshot"):
                if state.snapshot_error:
                    route.fulfill(status=422, json={"detail": REASON})
                else:
                    config = route.request.post_data_json["config"]
                    route.fulfill(json=_snapshot(config))
            elif "/strategy-explorer" in path and path.endswith("/markets"):
                route.fulfill(json={"ok": True, "exchanges": ["binance"], "coins_by_exchange": {"binance": ["BTC"]}})
            elif "/strategy-explorer" in path and path.endswith(("/simulate", "/compare", "/movie/frames")):
                route.fulfill(status=422, json={"detail": REASON})
            elif path.endswith("/probe"):
                route.fulfill(status=state.probe_status, body=state.probe_body)
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
