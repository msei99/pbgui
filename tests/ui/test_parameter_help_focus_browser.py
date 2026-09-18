"""PB8 documentation appears over labels, never over the editable date field."""
from pathlib import Path
from playwright.sync_api import sync_playwright


def test_date_input_focus_does_not_open_parameter_help():
    """Pointer editing and keyboard focus stay clear while label hover still works."""
    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        page = browser.new_page()
        page.route('**/api/optimize-v8/parameter-help', lambda route: route.fulfill(json={}))
        page.route('**/api/v8/parameter-help', lambda route: route.fulfill(json={'entries': {
            'backtest.start_date': {'text': 'Start date of backtest.', 'source': 'configuration.md'}}}))
        page.route('http://pbgui.test/', lambda route: route.fulfill(content_type='text/html', body='''
            <div class="form-group"><label><span data-tip="Start date">start_date</span></label>
            <input id="opted-start-date" value="2026-10-10"></div>
            <script>var API_BASE='/api/optimize-v8';</script>'''))
        page.goto('http://pbgui.test/')
        page.add_script_tag(path=str(Path('frontend/js/pb8_parameter_help.js').resolve()))
        page.wait_for_timeout(100)
        field = page.locator('input')
        tip = page.locator('#pb8-parameter-tooltip')
        field.click()
        assert tip.is_hidden()
        page.keyboard.press('ArrowLeft')
        page.keyboard.type('1')
        assert tip.is_hidden()
        field.evaluate('(element) => element.blur()')
        page.locator('[data-tip]').hover()
        assert tip.is_visible()
        field.focus()
        assert tip.is_hidden()
        field.hover()
        assert tip.is_hidden()
        browser.close()
