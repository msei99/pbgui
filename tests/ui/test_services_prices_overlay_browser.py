"""Exercise the actual Prices overlay code with isolated browser data and timers."""

from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]


def test_prices_preserve_dom_selection_and_pause_hidden_tab():
    """Age/data ticks retain nodes and selection; hiding/closing invalidates pending work."""
    playwright = pytest.importorskip('playwright.sync_api')
    source = (ROOT / 'frontend/services_monitor.html').read_text()
    start = source.index('  (function () {', source.index('/* ── Prices Overlay'))
    end = source.index('  }());', start) + len('  }());')
    with playwright.sync_playwright() as driver:
        browser = driver.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_content('''<div id="prices-overlay"><div id="prices-overlay-title"></div>
            <input id="po-search"><div id="prices-overlay-body" style="height:60px;overflow:auto"></div></div>''')
        page.evaluate('''() => {
            window.API_BASE = '/api/services';
            window.authOptions = options => options || {};
            window.hidden = false;
            Object.defineProperty(document, 'hidden', {get: () => window.hidden});
            window.timers = new Map(); window.timerId = 0;
            window.setInterval = (fn, ms) => {timers.set(++timerId, {fn, ms}); return timerId;};
            window.clearInterval = id => timers.delete(id);
            window.requests = [];
            window.fetch = (url, options) => new Promise(resolve => requests.push({resolve, options}));
            window.respond = (index, rows) => requests[index].resolve({ok:true, json:async () => ({rows})});
            window.rows = Array.from({length:20}, (_, i) => ({symbol:'COIN' + i, exchange:'bybit', price:12, ts:Math.floor(Date.now()/1000)-59}));
        }''')
        page.add_script_tag(content=source[start:end])
        page.evaluate('openPricesOverlay(); respond(0, rows);')
        page.wait_for_function("document.querySelectorAll('.po-age-cell').length === 20")
        page.evaluate('''() => {
            window.table = document.querySelector('table');
            window.firstRow = table.tBodies[0].rows[0];
            window.symbolText = firstRow.cells[1].firstChild;
            var range = document.createRange(); range.selectNodeContents(firstRow.cells[1]);
            getSelection().removeAllRanges(); getSelection().addRange(range);
            document.getElementById('prices-overlay-body').scrollTop = 80;
            window.scrollTopBefore = document.getElementById('prices-overlay-body').scrollTop;
            window.ageCell = firstRow.cells[4];
            ageCell.dataset.ts = String(Math.floor(Date.now()/1000)-61);
            Array.from(timers.values()).find(t => t.ms === 1000).fn();
        }''')
        assert page.locator('.po-age-cell').first.inner_text() == '1m'
        page.evaluate("Array.from(timers.values()).find(t => t.ms === 5000).fn(); respond(1, rows);")
        page.wait_for_function('requests.length === 2 && document.querySelectorAll(".po-age-cell").length === 20')
        # Flush the complete promise chain before checking the in-place data update.
        page.evaluate('async () => { await new Promise(resolve => setTimeout(resolve, 0)); }')
        assert page.evaluate('''() => document.querySelector('table') === table && table.tBodies[0].rows[0] === firstRow
            && firstRow.cells[1].firstChild === symbolText && getSelection().toString() === 'COIN0'
            && document.getElementById('prices-overlay-body').scrollTop === scrollTopBefore''')
        page.evaluate('loadPricesOverlay({silent:true}); hidden = true; document.dispatchEvent(new Event("visibilitychange"));')
        assert page.evaluate('timers.size') == 0
        assert page.evaluate('requests[2].options.signal.aborted')
        page.evaluate('respond(2, [{symbol:"STALE", exchange:"bybit", price:1, ts:1}]); loadPricesOverlay({silent:true});')
        page.evaluate('async () => { await new Promise(resolve => setTimeout(resolve, 0)); }')
        assert page.evaluate('requests.length') == 3
        assert page.locator('tbody tr').first.locator('td').nth(1).inner_text() == 'COIN0'
        page.evaluate('hidden = false; document.dispatchEvent(new Event("visibilitychange"));')
        assert page.evaluate('requests.length') == 4
        assert page.evaluate('timers.size') == 2
        page.evaluate('respond(3, rows.map(r => ({...r, price:99})));')
        page.wait_for_function("document.querySelector('tbody tr').cells[3].textContent === '99'")
        page.evaluate('''() => {
            document.getElementById('po-search').value = 'COIN19';
            filterPricesOverlay();
        }''')
        assert page.locator('tbody tr').count() == 1
        page.evaluate('loadPricesOverlay({silent:true}); closePricesOverlay(); openPricesOverlay();')
        assert page.evaluate('requests[4].options.signal.aborted')
        page.evaluate('respond(5, rows); respond(4, [{symbol:"OLD", exchange:"bybit", price:1, ts:1}]);')
        page.wait_for_function("document.querySelectorAll('tbody tr').length === 20")
        assert page.locator('tbody tr').first.locator('td').nth(1).inner_text() == 'COIN0'
        page.evaluate('closePricesOverlay();')
        assert page.evaluate('timers.size') == 0
        browser.close()
