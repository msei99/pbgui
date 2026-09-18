"""Offline browser checks of the inline fixed-route transfer controls."""
import json
from pathlib import Path

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[2]


def test_inline_transfer_confirmation_and_recovery():
    """Max is exact; cancellation, stale editors and lost replies cannot duplicate writes."""
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page()
        writes = []
        preview = {"routes": [{"id": "spot_to_perp", "asset": "USDC", "source": "Spot", "destination": "Perps", "minimum_amount": "0.000001", "max_transferable": "99.842907"}]}

        def respond(route):
            """Serve only local fixtures and simulate a lost execution response."""
            if route.request.method == "POST":
                writes.append(json.loads(route.request.post_data))
                route.abort()
            elif '/preview/' in route.request.url:
                route.fulfill(json=preview)
            else:
                route.fulfill(content_type='text/html', body='<div id="host"></div>')

        page.route('**/*', respond)
        page.goto('http://pbgui.test/')
        page.add_script_tag(path=str(ROOT / 'frontend/js/internal_transfer_panel.js'))
        page.evaluate("""() => {
            window.valid = true; window.accept = false;
            window.PBGuiDialogs = {confirm: async () => window.accept};
            window.mountPanel = () => {
                document.querySelector('#host').replaceChildren();
                PBGuiInternalTransferPanel.mount(document.querySelector('#host'), {
                    user: 'test-account', base: '/api/profit-sweep', isCurrent: () => window.valid,
                    onConfirmed: async () => { window.refreshed = true; }
                });
            }; mountPanel();
        }""")
        page.locator('summary').click()
        page.get_by_role('button', name='Max', exact=True).wait_for()
        page.wait_for_function("!document.querySelector('button').disabled")
        page.get_by_role('button', name='Max', exact=True).click()
        assert page.locator('input').input_value() == '99.842907'
        page.get_by_role('button', name='Review transfer').click()
        page.wait_for_function("!document.querySelector('input').disabled")
        assert writes == []
        page.evaluate('window.valid = false; window.accept = true')
        page.get_by_role('button', name='Review transfer').click()
        assert writes == []
        page.evaluate('window.valid = true; mountPanel()')
        page.locator('summary').click()
        page.wait_for_function("!document.querySelector('button').disabled")
        page.get_by_role('button', name='Max', exact=True).click()
        page.get_by_role('button', name='Review transfer').click()
        page.wait_for_function("document.querySelector('[role=status]').textContent.includes('pending operation')")
        assert len(writes) == 1
        assert writes[0]['amount'] == '99.842907'
        assert writes[0]['route'] == 'spot_to_perp'
        assert page.get_by_role('button', name='Review transfer').is_disabled()
        assert page.evaluate("JSON.parse(sessionStorage.getItem('pbgui:transfers:pending-top-up:v1')).operation_id") == writes[0]['operation_id']
        browser.close()


def test_inline_transfer_automatically_resolves_pending_operation():
    """Polling updates balances and clears a confirmed ID without a transfer POST."""
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page()
        writes = []

        def respond(route):
            """Provide an already confirmed history record and current balances."""
            if route.request.method == 'POST':
                writes.append(route.request.url)
                route.abort()
            elif '/operations/' in route.request.url:
                route.fulfill(json={'operations': [{'operation_id': 'test-id', 'status': 'confirmed'}]})
            elif '/preview/' in route.request.url:
                route.fulfill(json={'routes': [{'id': 'spot_to_perp', 'asset': 'USDC', 'source_balance': '0', 'destination_balance': '99.842907', 'max_transferable': '0', 'minimum_amount': '0.000001'}]})
            else:
                route.fulfill(content_type='text/html', body='<div id="host"></div>')

        page.route('**/*', respond)
        page.goto('http://pbgui.test/')
        page.evaluate('window.setInterval = callback => { window.pollTransfer = callback; return 1; }')
        page.add_script_tag(path=str(ROOT / 'frontend/js/internal_transfer_panel.js'))
        page.evaluate("""() => {
            PBGuiInternalTransferPanel.mount(document.querySelector('#host'), {
                user:'test-account',base:'/api/profit-sweep',isCurrent:()=>true,
                onBalances: value => {window.balances = value;},
                onConfirmed: async () => {window.confirmed = true;}
            });
        }""")
        page.evaluate('async () => { await window.pollTransfer(); }')
        assert page.evaluate('window.balances.routes[0].destination_balance') == '99.842907'
        page.evaluate("sessionStorage.setItem('pbgui:transfers:pending-top-up:v1', JSON.stringify({user:'test-account',operation_id:'test-id'}))")
        page.evaluate('async () => { await window.pollTransfer(); }')
        assert page.evaluate('window.confirmed') is True
        assert page.evaluate("sessionStorage.getItem('pbgui:transfers:pending-top-up:v1')") is None
        assert writes == []
        browser.close()
