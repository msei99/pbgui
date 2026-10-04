"""Real shared navigation restores drawer width and confirms AI menu navigation."""
import json
from pathlib import Path
from urllib.parse import urlparse

from playwright.sync_api import expect, sync_playwright

ROOT = Path(__file__).resolve().parents[2]


def test_delayed_preferences_and_ai_menu_navigation():
    """Opening never saves default width; menu actions are acknowledged at destination."""
    conversation_id = 'a' * 32
    action_id = 'b' * 32
    writes, acknowledgements, errors = [], [], []
    pending = []
    with sync_playwright() as driver:
        browser = driver.chromium.launch()
        page = browser.new_page(viewport={'width': 1400, 'height': 900})
        page.on('pageerror', lambda error: errors.append(str(error)))

        def respond(route):
            """Intercept every browser request without touching production runtime."""
            path = urlparse(route.request.url).path
            if path in ('/app/start.html', '/api/optimize-v8/main_page'):
                key = 'v8_run' if path == '/app/start.html' else 'v8_optimize'
                route.fulfill(content_type='text/html', body=f'''<nav id="topnav"></nav>
                <script>window.PBGUI_NAV_CONFIG={{current:{json.dumps(key)},authenticated:true}};
                window.PBGUI_AI_PAGE_CONTEXT=()=>({{section:'test-destination'}});</script>
                <script src="/app/pbgui_nav.js?v=test"></script>''')
            elif path.startswith('/app/'):
                asset = ROOT / 'frontend' / path.removeprefix('/app/')
                route.fulfill(body=asset.read_bytes() if asset.is_file() else b'', content_type='text/css' if asset.suffix == '.css' else 'text/javascript')
            elif path == '/api/ai/preferences':
                if route.request.method == 'PUT':
                    writes.append(route.request.post_data_json)
                    route.fulfill(json={})
                else:
                    route.fulfill(json={'drawer_width': 850, 'drawer_open': True, 'drawer_pinned': True})
            elif path == '/api/ai/status':
                route.fulfill(json={'providers': {'opencode-go': {'connected': True}}})
            elif path == '/api/ai/models':
                route.fulfill(json={'models': [{'id': 'test', 'name': 'Test', 'tools': True}]})
            elif path == '/api/ai/conversations':
                route.fulfill(json={'conversations': [{'conversation_id': conversation_id, 'model': 'test'}]})
            elif path.endswith('/ack'):
                acknowledgements.append(route.request.post_data_json)
                pending.clear()
                route.fulfill(json={})
            elif path == '/api/ai/conversations/' + conversation_id:
                route.fulfill(json={'conversation_id': conversation_id, 'provider': 'opencode-go', 'model': 'test',
                    'busy': True, 'messages': [{'role': 'user', 'content': 'Open Optimize'}], 'ui_actions': list(pending)})
            elif path.endswith('/stream'):
                route.fulfill(status=204, body='')
            else:
                route.fulfill(json={})

        page.add_init_script("""const originalFetch=window.fetch;
          window.fetch=async function(url,options){
            if(String(url).endsWith('/api/ai/preferences') && (!options || !options.method))
              await new Promise(resolve=>setTimeout(resolve,200));
            return originalFetch.call(this,url,options);
          };""")
        page.route('**/*', respond)
        page.goto('http://navigation.test/app/start.html')
        expect(page.locator('#pbgui-ai-drawer')).to_have_class(__import__('re').compile('open'))
        page.wait_for_function("document.querySelector('#pbgui-ai-drawer').getBoundingClientRect().width >= 850 && document.querySelector('#pbgui-ai-drawer').getBoundingClientRect().width <= 851")
        assert all(item.get('drawer_width', 850) == 850 for item in writes)
        # Open the genuine PBv8 dropdown and target its actual advertised menu item.
        page.locator('.nav-group-btn').filter(has_text='PBv8').click()
        controls = page.evaluate('window.PBGuiAI.collectContext().controls')
        control = next(item for item in controls if 'Optimize' in item.get('label', ''))
        pending.append({'action_id': action_id, 'type': 'page.perform_action',
            'target': {'page_key': 'v8_run'}, 'payload': {'action': 'activate',
                'entity': {'kind': 'ui_control', 'name': control['id']}}})
        page.wait_for_url('**/api/optimize-v8/main_page*')
        page.wait_for_function("document.querySelector('#pbgui-ai-drawer')?.classList.contains('open')")
        page.wait_for_timeout(1200)
        assert len(acknowledgements) == 1
        assert acknowledgements[0]['context']['page_key'] == 'v8_optimize'
        assert page.evaluate("sessionStorage.getItem('pbgui.ai.navigation-receipt')") is None
        assert 850 <= page.locator('#pbgui-ai-drawer').evaluate('(n)=>n.getBoundingClientRect().width') <= 851
        assert not errors
        browser.close()


def test_navigation_ids_survive_counter_and_entity_updates():
    """Background counters cannot invalidate navigation; resource actions still change IDs."""
    with sync_playwright() as driver:
        browser = driver.chromium.launch()
        page = browser.new_page()

        def respond(route):
            """Serve the real collector against isolated navigation and resource controls."""
            path = urlparse(route.request.url).path
            if path == '/test':
                route.fulfill(content_type='text/html', body='''<nav id="topnav"></nav>
                <button class="sb-section" data-panel="loops-config">AI Loops Config<span class="sb-count">1</span></button>
                <button id="resource-action">Edit selected config</button>
                <script>window.PBGUI_NAV_CONFIG={current:'v8_optimize',authenticated:true};
                window.currentConfigs=[{kind:'config',name:'first'}];
                window.PBGUI_AI_PAGE_CONTEXT=()=>({section:'loops-config',entities:window.currentConfigs});
                </script><script src="/app/pbgui_nav.js?v=test"></script>''')
            elif path == '/app/pbgui_nav.js':
                route.fulfill(body=(ROOT / 'frontend/pbgui_nav.js').read_bytes(), content_type='text/javascript')
            else:
                route.fulfill(json={})

        page.route('**/*', respond)
        page.goto('http://navigation.test/test')
        first = page.evaluate('window.PBGuiAI.collectContext().controls')
        navigation = next(item for item in first if item['label'] == 'AI Loops Config')
        resource = next(item for item in first if item['label'] == 'Edit selected config')
        page.evaluate("document.querySelector('.sb-count').textContent='2'; currentConfigs.push({kind:'config',name:'second'});")
        second = page.evaluate('window.PBGuiAI.collectContext().controls')
        assert next(item for item in second if item['label'] == 'AI Loops Config')['id'] == navigation['id']
        assert next(item for item in second if item['label'] == 'Edit selected config')['id'] != resource['id']
        action = {'type': 'page.perform_action', 'action_id': 'c' * 32, 'target': {'page_key': 'v8_optimize'},
                  'payload': {'action': 'activate', 'entity': {'kind': 'ui_control', 'name': navigation['id']}}}
        assert page.evaluate('''action=>{window.navClicks=0;document.querySelector('.sb-section').onclick=()=>navClicks++;
          const event=new CustomEvent('pbgui:ai-ui-action',{cancelable:true,detail:action});
          window.dispatchEvent(event);return event.defaultPrevented && navClicks===1;}''', action)
        browser.close()
