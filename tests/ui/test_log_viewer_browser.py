"""Offline Chromium layout regressions using only synthetic log messages."""

from pathlib import Path
import re

import pytest


ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def log_page():
    """Load the real API Keys shell without application scripts or network access."""
    playwright = pytest.importorskip("playwright.sync_api")
    with playwright.sync_playwright() as runner:
        try:
            browser = runner.chromium.launch(headless=True)
        except playwright.Error as error:
            if "Executable doesn't exist" in str(error):
                pytest.skip("Offline Chromium installation is required")
            raise
        try:
            page = browser.new_page()
            page.route("**/*", lambda route: route.abort())
            errors = []
            page.on("pageerror", lambda error: errors.append(str(error)))
            shell = (ROOT / "frontend/api_keys_editor.html").read_text(encoding="utf-8")
            shell = re.sub(r"<script\b[^>]*>.*?</script>", "", shell, flags=re.S | re.I)
            shell = re.sub(r"<link\b[^>]*>", "", shell, flags=re.I)
            page.set_content(shell)
            page.add_script_tag(path=ROOT / "frontend/js/log_viewer_panel.js")
            page.evaluate("""() => {
                document.getElementById('userListView').style.display = 'none';
                document.getElementById('logPanel').style.display = 'flex';
                window.panel = new LogViewerPanel({containerId:'logViewerTarget',
                    defaultFile:'PBGui.log', height:'calc(100vh - 300px)'});
                window.syntheticLines = Array.from({length:10000}, (_, i) => {
                    const block = Math.floor(i / 800), offset = i % 800;
                    const match = block < 12 && offset < (block < 3 ? 143 : 142);
                    return '[INFO] ' + (match ? 'api ' : 'other ') + i + ' ' + 'x'.repeat(64);
                });
                for (let b=0; b<12; b++) syntheticLines[b*800] =
                    '[INFO] api ' + 'api response '.repeat(50000) + ' tail-only-needle';
            }""")
            assert page.locator("#logViewerTarget-lvp-search").input_value() == ""
            assert page.locator("#logViewerTarget-lvp-filter-chk").is_checked()
            yield page, errors
            assert not errors
        finally:
            browser.close()


@pytest.mark.parametrize("width", [1200, 390])
def test_large_filtered_log_layout_and_long_line_preservation(log_page, width):
    """10k rows/1707 matches/12 blocks remain interactive, including long headers."""
    page, _errors = log_page
    page.set_viewport_size({"width": width, "height": 900})
    if width < 600:
        # Exercise the shared viewer at mobile width without the page's fixed sidebar.
        page.evaluate("""() => {
            document.getElementById('sidebar').style.display = 'none';
            panel._toggleSidebar();
        }""")
    page.evaluate("""() => {
        panel._q('lines-sel').value = '10000';
        panel._q('lines-sel').dispatchEvent(new Event('change'));
        panel._searchTerm = 'api'; panel._q('search').value = 'api';
        window.gaps = []; let last = performance.now();
        window.heartbeat = setInterval(() => {
            const now = performance.now(); gaps.push(now-last); last=now;
        }, 20);
        window.started = performance.now();
        panel._handleMsg({type:'local_logs', lines:syntheticLines});
    }""")
    page.wait_for_function("panel._q('match-count').textContent === '1707 matches in 12 blocks'", timeout=15000)
    page.locator("#logViewerTarget-lvp-expand-all").click(timeout=3000)
    page.wait_for_function("!panel._fullRenderPending")
    page.evaluate("""() => {
        const term = panel._q('terminal');
        term.querySelector('.lvp-group-first').click();
    }""")
    page.wait_for_function("!panel._fullRenderPending")
    assert page.evaluate("!panel._q('terminal').querySelector('.lvp-grp-detail[data-blk=\"0\"]')")
    page.locator("#logViewerTarget-lvp-expand-all").click(timeout=3000)
    page.wait_for_function("!panel._fullRenderPending")
    assert page.evaluate("""() => getComputedStyle(panel._q('terminal')
        .querySelector('.lvp-grp-detail[data-blk="0"]')).display""") != "none"
    page.locator("#logViewerTarget-lvp-collapse-all").click(timeout=3000)
    page.wait_for_function("!panel._fullRenderPending")
    metrics = page.evaluate(r"""() => {
        clearInterval(heartbeat);
        const term = panel._q('terminal');
        return {maxGap:Math.max(...gaps), elapsed:performance.now()-started,
            marks:term.querySelectorAll('mark').length,
            preview:term.firstElementChild.textContent.length,
            original:term.firstElementChild.dataset.text.length,
            preserved:panel._lines.join('\n') === syntheticLines.join('\n')};
    }""")
    assert metrics["maxGap"] < 1000, metrics
    assert metrics["elapsed"] < 10000, metrics
    assert metrics["marks"] < 10000, metrics
    assert metrics["preview"] < 4200 < metrics["original"], metrics
    assert metrics["preserved"], metrics
    print(f"Chromium width={width}: {metrics}")

    # Matches beyond the preview must not disappear from search or downloads.
    page.locator("#logViewerTarget-lvp-search").fill("tail-only-needle")
    page.wait_for_function("panel._q('match-count').textContent === '12 matches in 12 blocks'")
    assert page.evaluate(r"""async () => {
        let captured;
        URL.createObjectURL = blob => { captured = blob; return 'blob:synthetic'; };
        URL.revokeObjectURL = () => {};
        HTMLAnchorElement.prototype.click = function() {};
        panel._download();
        return await captured.text() === syntheticLines.join('\n');
    }""")


@pytest.mark.parametrize("action", ["clear", "close", "replace", "search"])
def test_pending_search_cancellation_and_invalid_pattern(log_page, action):
    """Queued work cannot resurrect cleared/replaced lines or overwrite a new search."""
    page, _errors = log_page
    page.evaluate("""() => {
        panel._MAX = 10000; panel._lines = syntheticLines;
        panel._searchTerm = 'api'; panel._renderFull();
    }""")
    page.wait_for_function("panel._q('match-count').textContent === '1707 matches in 12 blocks'")
    page.evaluate("""action => {
        panel._applySearch();
        if (action === 'clear') panel._clear();
        if (action === 'close') panel.close();
        if (action === 'replace') {
            panel._replaceLines(['[INFO] replacement']); panel._renderFull();
        }
        if (action === 'search') {
            panel._q('search').value = 'no-such-term'; panel._onSearchInput();
        }
        window.afterCancel = panel._q('match-count').textContent;
    }""", action)
    page.wait_for_timeout(1200)
    if action == "clear":
        assert page.evaluate("panel._q('terminal').childElementCount") == 0
        assert page.evaluate("panel._q('match-count').textContent") == "0 matches"
    elif action == "close":
        assert page.evaluate("panel._q('match-count').textContent === afterCancel")
    elif action == "replace":
        assert page.evaluate("panel._q('terminal').childElementCount") == 0
        assert page.evaluate("panel._q('match-count').textContent") == "0 matches"
    else:
        assert page.evaluate("panel._q('match-count').textContent") == "0 matches"
        page.evaluate("""() => {
            panel._searchTerm = '['; panel._searchRegex = true; panel._applySearch();
        }""")
        page.wait_for_timeout(1200)
        assert page.evaluate("panel._q('match-count').textContent") == "0 matches"


def test_search_changed_and_cleared_during_full_render(log_page):
    """Transient filters during snapshot rendering cannot leave stale hidden rows."""
    page, _errors = log_page
    page.evaluate("""() => {
        panel._MAX = 10000;
        panel._lines = syntheticLines;
        panel._renderFull();
        requestAnimationFrame(() => {
            panel._q('search').value = 'api'; panel._onSearchInput();
            requestAnimationFrame(() => {
                panel._q('search').value = ''; panel._onSearchInput();
            });
        });
    }""")
    page.wait_for_function("!panel._fullRenderPending")
    page.wait_for_function("""() => {
        const term = panel._q('terminal');
        return term.childElementCount === 10000 &&
            !term.querySelector('.lvp-highlight, .lvp-hidden, .lvp-grp-detail');
    }""")


def test_collapsed_results_only_materialize_headers_and_expand_lazily(log_page):
    """5430 matches retain searchable text but only 71 headers need highlight DOM."""
    page, _errors = log_page
    page.evaluate("""() => {
        panel._MAX = 10000; panel._searchTerm = 'api';
        panel._lines = Array.from({length:10000}, (_, i) => {
            const b=Math.floor(i/140), offset=i%140;
            const match=b<71 && offset>=10 && offset<86+(b<34 ? 1 : 0);
            return '[INFO] ['+(match ? 'ApiKeys' : 'Status')+'] record-'+i+' '+ 'payload '.repeat(80);
        });
        panel._renderFull();
    }""")
    page.wait_for_function("panel._q('match-count').textContent === '5430 matches in 71 blocks'")
    assert page.evaluate("panel._q('terminal').querySelectorAll('mark').length") == 71
    assert page.evaluate("panel._q('terminal').childElementCount") == 141
    assert page.evaluate("!panel._q('terminal').querySelector('.lvp-grp-detail,.lvp-hidden')")
    page.evaluate("panel._q('terminal').querySelector('.lvp-group-first').click()")
    page.wait_for_function("!panel._fullRenderPending")
    page.wait_for_function("""() => [...panel._q('terminal').querySelectorAll('.lvp-grp-detail[data-blk="0"]')]
        .every(row => row.textContent.includes('payload'))""")
    assert page.evaluate("""() => JSON.stringify([...panel._q('terminal').querySelectorAll('[data-blk="0"]')]
        .map(row => Number(row.dataset.ln)))""") == str(list(range(6, 93))).replace(" ", "")
    page.evaluate("panel._q('terminal').querySelector('.lvp-group-first').click()")
    page.wait_for_function("!panel._fullRenderPending && !panel._q('terminal').querySelector('.lvp-grp-detail')")
    page.locator('#logViewerTarget-lvp-expand-all').click()
    page.wait_for_function("!panel._fullRenderPending", timeout=15000)
    page.wait_for_function("""() => [...panel._q('terminal').querySelectorAll('.lvp-grp-detail')]
        .every(row => row.textContent.includes('payload'))""", timeout=15000)
    page.locator('#logViewerTarget-lvp-collapse-all').click()
    page.wait_for_function("!panel._fullRenderPending && !panel._q('terminal').querySelector('.lvp-grp-detail')", timeout=15000)
    # An unfinished expansion must follow current collapse state, not resurrect details.
    page.evaluate("panel._toggleAllGroups(true); panel._toggleAllGroups(false)")
    page.wait_for_timeout(2000)
    assert page.evaluate("!panel._q('terminal').querySelector('.lvp-grp-detail')")
    page.evaluate("""() => {
        panel._ingestLines(['[INFO] api live-one', '[INFO] api live-two']);
    }""")
    page.wait_for_function("panel._q('match-count').textContent === '5432 matches in 72 blocks'")
    assert page.evaluate("JSON.stringify(panel._lines.slice(-2))") == '["[INFO] api live-one","[INFO] api live-two"]'
    assert page.evaluate("panel._q('terminal').childElementCount") == 143
    page.evaluate("panel._toggleAllGroups(true); panel._clear()")
    page.wait_for_timeout(300)
    assert page.evaluate("panel._q('terminal').childElementCount") == 0
