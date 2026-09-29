"""Browser approval tests with real PBGui assets and no live accounts or filesystem mutations."""
from pathlib import Path
from urllib.parse import urlparse

import pytest
from playwright.sync_api import sync_playwright, expect

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def review_page():
    """Serve inert owner-bound proposals and intercept every HTTP request."""
    cid, pid = 'a' * 32, 'b' * 32
    conversation = {'conversation_id': cid, 'title': 'Research analysis', 'provider': 'opencode-go',
                    'model': 'selected-model', 'analysis_only': True, 'busy': False,
                    'messages': [{'role': 'assistant', 'content': 'Review the proposed coin-list change.'}]}
    proposal = {'proposal_id': pid, 'conversation_id': cid, 'status': 'awaiting_approval',
        'payload_digest': 'sha256:' + 'd' * 64,
        'preview': {'action': 'reviewed_config_change', 'name': 'test', 'version': 'v8', 'kind': 'backtest',
            'mode': 'save', 'effect': 'Save config only', 'changed_count': 1,
            'changes': [{'path': 'live.approved_coins.long', 'kind': 'removed', 'before': 'ETH'}]}}
    pending = [proposal]
    calls, errors = [], []
    with sync_playwright() as driver:
        browser = driver.chromium.launch()
        page = browser.new_page(viewport={'width': 1450, 'height': 1000})
        page.set_default_timeout(5000)
        page.on('pageerror', lambda error: errors.append(str(error)))
        def route_handler(route):
            """Provide real UI code and synthetic API contracts; external network is impossible."""
            path = urlparse(route.request.url).path
            if path == '/app/start.html':
                route.fulfill(content_type='text/html', body='<nav id="topnav"></nav><script>window.PBGUI_NAV_CONFIG={current:"welcome",authenticated:true,apiBase:"/api/start"};</script><script src="/app/pbgui_nav.js"></script>')
            elif path == '/api/ai/main_page':
                html = (ROOT / 'frontend/ai_chat.html').read_text()
                for key, value in {'API_BASE': '/api/ai', 'VERSION': 'test', 'SERIAL': '1', 'NAV_HASH': 'test'}.items():
                    html = html.replace('%%' + key + '%%', value)
                route.fulfill(content_type='text/html', body=html)
            elif path.startswith('/app/'):
                asset = ROOT / 'frontend' / path.removeprefix('/app/')
                route.fulfill(body=asset.read_bytes() if asset.is_file() else b'', content_type='text/css' if asset.suffix == '.css' else 'text/javascript')
            elif path.endswith('/ai/status'):
                route.fulfill(json={'providers': {'opencode-go': {'connected': True, 'available': True}}})
            elif path.endswith('/models'):
                route.fulfill(json={'models': [{'id': 'selected-model', 'name': 'Selected model', 'tools': True}]})
            elif path.endswith('/conversations'):
                route.fulfill(json={'conversations': [conversation]})
            elif path.endswith('/conversations/' + cid):
                route.fulfill(json=conversation)
            elif path.endswith('/proposals'):
                route.fulfill(json={'proposals': pending})
            elif path.endswith('/review'):
                calls.append(('review', route.request.post_data_json))
                route.fulfill(json={'proposal': proposal, 'review_token': 'e' * 64, 'expires_in': 120})
            elif path.endswith('/approve'):
                body = route.request.post_data_json
                calls.append(('approve', body))
                assert body['review_token'] == 'e' * 64
                pending.clear()
                route.fulfill(json={'proposal_id': pid, 'status': 'executed', 'action': 'reviewed_config_change',
                                    'mode': 'save', 'kind': 'backtest', 'version': 'v8', 'name': 'test'})
            elif path.endswith('/turns'):
                calls.append(('turn', route.request.post_data_json))
                route.fulfill(json={})
            elif path.endswith('/stream'):
                route.fulfill(status=204, body='')
            else:
                route.fulfill(json={})
        page.route('**/*', route_handler)
        page.goto('http://review.test/app/start.html')
        yield page, calls
        assert not errors
        browser.close()


@pytest.mark.parametrize('surface', ['drawer', 'page'])
def test_only_trusted_review_and_explicit_apply_can_cross_bridge(review_page, surface):
    """Synthetic clicks cannot obtain tickets or approve; real clicks show exact changes first."""
    page, calls = review_page
    if surface == 'drawer':
        page.locator('#pbgui-ai-btn').click()
        review = page.get_by_role('button', name='Review changes', exact=True)
    else:
        page.locator('.nav-group-btn', has_text='Information').click()
        page.locator('.nav-item[data-page="info_ai_chat"]').click()
        review = page.get_by_role('button', name='Review & approve', exact=True)
    expect(review).to_be_visible()
    review.evaluate('(button) => button.click()')
    assert calls == []
    review.click()
    if surface == 'drawer':
        dialog = page.locator('.pai-review-dialog')
        expect(dialog).to_contain_text('ETH')
        expect(dialog).to_contain_text('Save config only')
        approve = dialog.get_by_role('button', name='Approve', exact=True)
    else:
        expect(page.locator('.proposal-review').first).to_have_attribute('open', '')
        expect(page.locator('.proposal-review').first).to_contain_text('ETH')
        approve = page.get_by_role('button', name='Apply reviewed changes', exact=True)
    assert [kind for kind, _ in calls] == ['review']
    approve.evaluate('(button) => button.click()')
    assert [kind for kind, _ in calls] == ['review']
    approve.click()
    expect(page.locator('#pbgui-dialog-ovl')).to_be_visible()
    assert [kind for kind, _ in calls] == ['review']
    page.locator('#pbgui-dialog-ovl').get_by_role('button', name='Approve', exact=True).click()
    expect(page.locator('#pbgui-dialog-ovl')).not_to_be_visible()
    expect(review).to_have_count(0)
    assert [kind for kind, _ in calls] == ['review', 'approve']
    assert 'e' * 64 not in page.evaluate('JSON.stringify(sessionStorage) + JSON.stringify(localStorage)')
