"""Browser regression for overwrite typing in optimizer ISO date fields."""
from pathlib import Path
from playwright.sync_api import sync_playwright


def test_date_digits_overwrite_and_selection_remains_editable():
    """Typing skips separators, never appends a fifth year digit, and allows replacement."""
    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        page = browser.new_page()
        page.set_content('<input data-date-overwrite value="2026-10-10"><input id="other">')
        page.add_script_tag(path=str(Path(__file__).resolve().parents[2] / 'frontend/js/date_overwrite.js'))
        field = page.locator('[data-date-overwrite]')
        field.focus()
        field.evaluate('(el) => el.setSelectionRange(0, 0)')
        page.keyboard.type('20270102')
        assert field.input_value() == '2027-01-02'
        page.keyboard.type('0')
        assert field.input_value() == '2027-01-02'
        field.evaluate('(el) => el.setSelectionRange(5, 7)')
        page.keyboard.type('12')
        assert field.input_value() == '2027-12-02'
        field.select_text()
        page.keyboard.type('2026-10-10')
        assert field.input_value() == '2026-10-10'
        field.evaluate('(el) => el.setSelectionRange(4, 4)')
        page.keyboard.type('09')
        assert field.input_value() == '2026-09-10'
        browser.close()
