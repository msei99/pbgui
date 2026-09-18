"""Exercise real editor date renderers through the common browser component."""

from pathlib import Path
import re

import pytest
from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[2]


def function_source(source, name):
    """Extract a top-level page renderer without executing the application."""
    start = source.index('function ' + name + '(')
    return source[start:source.index('\n}', start) + 2]


@pytest.mark.parametrize('filename,renderer', [
    ('v7_backtest.html', 'fieldDate2'),
    ('v7_backtest.html', 'backtestDialogDateInputHtml'),
    ('v7_optimize.html', 'fieldDate2'),
    ('v7_optimize.html', 'fieldOptimizeStartDate2'),
])
def test_editors_share_overwrite_and_calendar(filename, renderer):
    """Both editors and re-backtest dialogs load and use the same input code."""
    source = (ROOT / 'frontend' / filename).read_text()
    assert re.search(r'<script[^>]+src="/app/js/date_overwrite.js\?v=2"', source)
    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        page = browser.new_page()
        page.set_content('<main></main><input id="other">')
        page.add_script_tag(path=str(ROOT / 'frontend/js/date_overwrite.js'))
        page.evaluate('''() => {
            window.fmtGroupSpan = (id, label, input) => input;
            window.__dp = {show(id, button) { window.calendarTarget = [id, button.dataset.dp]; }};
        }''')
        if renderer == 'fieldOptimizeStartDate2':
            page.add_script_tag(content=function_source(source, 'normalizeOptimizeDateOnlyValue'))
        page.add_script_tag(content=function_source(source, renderer))
        page.evaluate('''name => {
            const args = name === 'backtestDialogDateInputHtml'
                ? ['date', '2026-10-10'] : ['date', 'start_date', '2026-10-10', '', 1];
            document.querySelector('main').innerHTML = window[name](...args);
        }''', renderer)
        field = page.locator('#date')
        field.evaluate("el => { el.dataset.semanticValue = 'now'; el.focus(); el.setSelectionRange(0, 0); }")
        page.keyboard.type('20270102')
        assert field.input_value() == '2027-01-02'
        page.locator('#other').focus()
        assert field.get_attribute('data-prev') == '2027-01-02'
        if filename == 'v7_backtest.html' and renderer == 'fieldDate2':
            assert field.get_attribute('data-semantic-value') == ''
        page.get_by_title('Open calendar', exact=True).click()
        assert page.evaluate('window.calendarTarget') == ['date', 'date']
        assert page.locator('input[data-date-overwrite]').count() == 1
        browser.close()


def test_shared_renderer_escapes_values_and_ids():
    """Rendering untrusted attributes cannot inject event handlers or markup."""
    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        page = browser.new_page()
        page.set_content('<main></main>')
        page.add_script_tag(path=str(ROOT / 'frontend/js/date_overwrite.js'))
        value = '\"><img src=x onerror="window.injected=true">'
        page.evaluate('value => document.querySelector("main").innerHTML = PBGuiDateInput.render(value, value)', value)
        assert page.locator('img').count() == 0
        assert page.locator('input').input_value() == value
        assert page.locator('input').get_attribute('id') == value
        browser.close()
