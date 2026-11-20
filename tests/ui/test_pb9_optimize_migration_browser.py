"""PB8 Optimize -> PB9 migration uses the shared review dialog and opens a PB9 Optimize draft."""

from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest
from browser_driver import sync_browser

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize('kind,attribute,source', [
    ('config', 'data-config-name="saved_opt"', 'saved_opt'),
    ('pareto', 'data-path="/pb8/optimize_results/run/pareto/a.json"', '/pb8/optimize_results/run/pareto/a.json'),
])
def test_optimize_migration_reviews_and_opens_pb9_optimize_draft(kind, attribute, source):
    """The reviewed proposal is accepted only after confirmation and opens PB9 Optimize."""
    html = ('<!doctype html><button data-migrate-pb9-optimize="' + kind + '" ' + attribute + '>V9</button>'
            '<script>window.toasts=[];window.toast=(m,t)=>window.toasts.push([m,t]);'
            'window.dialogs=[];window.PBGuiDialogs={confirm:async o=>{window.dialogs.push(o);return true;}};</script>'
            '<script src="/app/js/backtest_v9_migration.js"></script>')
    posts = []

    def serve(route):
        """Serve the production script and isolated migration responses."""
        path = urlsplit(route.request.url).path
        if path.startswith('/app/js/'):
            route.fulfill(body=(ROOT / 'frontend/js' / Path(path).name).read_text(), content_type='text/javascript')
        elif path == '/api/optimize-v9/migrate-v8/preview':
            posts.append(('preview', route.request.post_data_json))
            route.fulfill(json={'status': 'ready', 'review_id': 'r1', 'suggested_name': 'saved_opt_v9',
                                'report': {'changes': [], 'warnings': ['run settings']}})
        elif path == '/api/optimize-v9/migrate-v8/accept':
            posts.append(('accept', route.request.post_data_json))
            route.fulfill(json={'draft_id': 'd1', 'suggested_name': 'saved_opt_v9'})
        elif path == '/api/optimize-v9/main_page':
            route.fulfill(body='<title>PB9 Optimize</title>', content_type='text/html')
        else:
            route.fulfill(body=html, content_type='text/html')

    with sync_browser() as runner:
        browser = runner.chromium.launch(headless=True)
        try:
            page = browser.new_page()
            page.route('**/*', serve)
            page.goto('http://pb8.test/api/optimize-v8/main_page')
            page.locator('[data-migrate-pb9-optimize]').click()
            page.wait_for_url('**/api/optimize-v9/main_page?**')
            assert posts == [('preview', {'source_type': kind, 'source': source}), ('accept', {'review_id': 'r1'})]
            query = parse_qs(urlsplit(page.url).query)
            assert query == {'opt_draft_id': ['d1'], 'draft_name': ['saved_opt_v9']}
            assert urlsplit(page.url).fragment == 'configs'
        finally:
            browser.close()


def test_pb8_optimize_page_offers_v9_migration_only_for_pb8():
    """Only the PB8 page renders the V9 actions; PB7 keeps its V8 conversion."""
    source = (ROOT / 'frontend/v7_optimize.html').read_text()
    assert source.count("optimizeEditorAdapter.isV8 && !optimizeEditorAdapter.isV9 ? '<button class=\"icon-btn\" data-migrate-pb9-optimize=") == 2
    assert '/app/js/backtest_v9_migration.js?v=2' in source
