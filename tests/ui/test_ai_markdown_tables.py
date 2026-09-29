"""Offline checks for conservative model-table repair and inert Markdown rendering."""
from pathlib import Path

import pytest
from playwright.sync_api import expect, sync_playwright

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope='module')
def renderer():
    """Load only the exact local production assets; perform no external requests."""
    with sync_playwright() as driver:
        browser = driver.chromium.launch()
        page = browser.new_page()
        page.route('**/*', lambda route: route.abort())
        page.set_content('<div id="output"></div>')
        for asset in ['vendor/marked.min.js', 'vendor/purify.min.js', 'js/ai_message_view.js']:
            page.add_script_tag(path=str(ROOT / 'frontend' / asset))
        yield page
        browser.close()


@pytest.mark.parametrize('source,headers,rows', [
    ('Intro. | Sort order | Coin | Risk | Confidence |\n|---|---|---|---|\n|1|PEPE|very_high|48%|\nAfterword.', 4, 1),
    ('| Coin | Risk |\n|---|---|\n|BTC|Moderate|', 2, 1),
    ('Coin | Risk\n--- | ---\nBTC | Moderate', 2, 1),
    ('Intro. | Coin | Risk |\n|:---|---:|\n|A\\|B|High|', 2, 1),
    ('```markdown\nIntro. | Coin | Risk |\n|---|---|\n|BTC|High|\n```', 0, 0),
    ('~~~\n| Coin | Risk |\n|---|---|\n|BTC|High|\n~~~', 0, 0),
    ('    Intro. | Coin | Risk |\n    |---|---|\n    |BTC|High|', 0, 0),
    ('Intro. | Coin | Risk | Extra |\n|---|---|\n|BTC|High|', 0, 0),
])
def test_table_boundaries_preserve_valid_text_and_code(renderer, source, headers, rows):
    """Only unambiguous table boundaries outside code are repaired."""
    renderer.evaluate('(s) => PBGuiAIMessageView.render(document.getElementById("output"), s)', source)
    expect(renderer.locator('#output th')).to_have_count(headers)
    expect(renderer.locator('#output tbody tr')).to_have_count(rows)
    if 'Afterword.' in source:
        expect(renderer.locator('#output > p').last).to_have_text('Afterword.')
    if 'A\\|B' in source:
        expect(renderer.locator('#output td').first).to_have_text('A|B')
    if not headers and (source.startswith('```') or source.startswith('~~~') or source.startswith('    ')):
        expect(renderer.locator('#output pre code')).to_contain_text('|---|---|')
