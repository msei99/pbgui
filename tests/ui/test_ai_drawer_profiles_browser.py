"""Offline browser coverage for profile routing in the compact AI drawer."""
from pathlib import Path
from urllib.parse import urlparse, parse_qs
from playwright.sync_api import sync_playwright


def test_drawer_profile_selection_routes_models_and_chats():
    """Both profiles are selectable and requests use the selected subscription."""
    source = (Path(__file__).resolve().parents[2] / 'frontend/js/ai_drawer.js').read_text()
    created = []
    models = []
    errors = []
    work = 'b' * 32
    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        page = browser.new_page()
        page.on('pageerror', lambda error: errors.append(str(error)))

        def route_request(route):
            """Mock all requests without contacting any provider."""
            url = urlparse(route.request.url)
            path = url.path
            if path == '/':
                route.fulfill(content_type='text/html', body='<html><body></body></html>')
            elif path.endswith('/status'):
                route.fulfill(json={'providers': {'chatgpt': {'connected': True, 'profiles': [
                    {'id': 'default', 'name': 'Personal'}, {'id': work, 'name': 'Work'}]}}})
            elif path.endswith('/usage'):
                profile = parse_qs(url.query).get('profile', ['default'])[0]
                route.fulfill(json={'email': profile + '@example.test', 'limits': [{'usedPercent': 3.8, 'windowDurationMins': 43200}]})
            elif path.endswith('/models'):
                profile = parse_qs(url.query)['profile'][0]
                models.append(profile)
                route.fulfill(json={'models': [{'id': 'model-' + profile, 'name': 'Test model'}]})
            elif path.endswith('/conversations') and route.request.method == 'POST':
                created.append(route.request.post_data_json)
                route.fulfill(status=400, json={'detail': 'Fixture stops before creating a real chat'})
            elif path.endswith('/conversations'):
                route.fulfill(json={'conversations': []})
            else:
                route.fulfill(json={})

        page.route('**/*', route_request)
        page.goto('http://pbgui.test/')
        page.add_script_tag(content=(Path(__file__).resolve().parents[2] / 'frontend/js/ai_usage.js').read_text())
        page.add_script_tag(content=source)
        page.evaluate('window.PBGuiAI.open()')
        page.wait_for_function("document.querySelector('#pai-model').value === 'model-default'")
        assert page.locator('#pai-provider option').all_text_contents() == ['ChatGPT · Personal', 'ChatGPT · Work']
        page.locator('#pai-provider').select_option('chatgpt:' + work)
        page.wait_for_function("document.querySelector('#pai-model').value === 'model-' + 'b'.repeat(32)")
        page.get_by_role('button', name='New', exact=True).click()
        page.wait_for_function("document.body.textContent.includes('Fixture stops')")
        page.wait_for_function("document.querySelector('#pai-usage').textContent.includes('96.2% remaining')")
        assert work + '@example.test' in page.locator('#pai-usage').inner_text()
        assert created[0]['provider'] == 'chatgpt'
        assert created[0]['profile'] == work
        assert models[-1] == work
        assert not errors
        browser.close()
