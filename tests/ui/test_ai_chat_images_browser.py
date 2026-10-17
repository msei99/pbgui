"""Real drawer image paste/drop, draft recovery and provider payloads with mocked APIs."""
from pathlib import Path
from urllib.parse import urlparse
import base64

import pytest
from playwright.sync_api import expect, sync_playwright

ROOT = Path(__file__).resolve().parents[2]
PNG = base64.b64decode('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aX1kAAAAASUVORK5CYII=')


@pytest.mark.parametrize('fresh', [False, True])
@pytest.mark.parametrize('width,prefix', [(1440, ''), (430, '/mounted')])
def test_paste_drop_and_send_images_without_upload_button(width, prefix, fresh):
    """Pasted and dropped bytes reach the turn while failed submissions preserve drafts."""
    conversation_id = 'a' * 32
    sent, errors, messages = [], [], []
    fail = True
    created = not fresh
    with sync_playwright() as driver:
        browser = driver.chromium.launch()
        page = browser.new_page(viewport={'width': width, 'height': 900})
        page.set_default_timeout(5000)
        page.on('pageerror', lambda error: errors.append(str(error)))

        def respond(route):
            """Serve local production assets and an isolated owner conversation."""
            nonlocal fail, created
            path = urlparse(route.request.url).path.removeprefix(prefix)
            if path == '/app/start.html':
                route.fulfill(content_type='text/html', body=f'''<nav id="topnav"></nav><script>
                window.PBGUI_BASE_PREFIX={prefix!r}; window.BASE_PREFIX={prefix!r};
                window.PBGUI_NAV_CONFIG={{current:'welcome',authenticated:true}};
                </script><script src="{prefix}/app/pbgui_nav.js?v=test"></script>''')
            elif path.startswith('/app/'):
                asset = ROOT / 'frontend' / path.removeprefix('/app/')
                route.fulfill(body=asset.read_bytes() if asset.is_file() else b'', content_type='text/css' if asset.suffix == '.css' else 'text/javascript')
            elif path == '/api/ai/status':
                route.fulfill(json={'providers': {'opencode-go': {'connected': True}}})
            elif path == '/api/ai/models':
                route.fulfill(json={'models': [{'id': 'test-model', 'name': 'Image model', 'tools': True}]})
            elif path == '/api/ai/conversations':
                if route.request.method == 'POST':
                    created = True
                    route.fulfill(json={'conversation_id': conversation_id})
                else:
                    route.fulfill(json={'conversations': [{'conversation_id': conversation_id, 'title': 'Image chat', 'model': 'test-model'}] if created else []})
            elif path.endswith('/turns'):
                payload = route.request.post_data_json
                sent.append(payload)
                if fail:
                    route.fulfill(status=400, json={'detail': 'Fixture rejected submission'})
                else:
                    messages.extend([{'role': 'user', 'content': payload['message'], 'images': [{'id': 'b' * 64 + '.png', 'media_type': 'image/png', 'size': len(PNG)}]}, {'role': 'assistant', 'content': 'The screenshot shows the Dashboard.'}])
                    route.fulfill(status=202, json={'status': 'queued'})
            elif '/images/' in path:
                route.fulfill(body=PNG, content_type='image/png')
            elif path == '/api/ai/conversations/' + conversation_id:
                route.fulfill(json={'conversation_id': conversation_id, 'provider': 'opencode-go', 'model': 'test-model', 'busy': False, 'messages': messages})
            elif path.endswith('/stream'):
                route.fulfill(status=204, body='')
            else:
                route.fulfill(json={})

        page.route('**/*', respond)
        page.goto('http://images.test' + prefix + '/app/start.html')
        page.locator('#pbgui-ai-btn').click()
        expect(page.locator('#pai-model')).to_have_value('test-model')
        if not fresh:
            page.wait_for_function("document.querySelector('.pai-history-list button.selected')")
        else:
            expect(page.locator('.pai-empty')).to_be_visible()
        assert page.locator('.pai-compose input[type=file]').count() == 0
        assert page.locator('.pai-compose button', has_text='Attach').count() == 0
        inject = '''({kind,count=1,type='image/png',size=0}) => {
          const bytes=Uint8Array.from(atob('%s'), c=>c.charCodeAt(0));
          const transfer=new DataTransfer();
          for(let i=0;i<count;i++) transfer.items.add(new File([size ? new Uint8Array(size) : bytes], 'screenshot.png', {type}));
          const target=document.querySelector(kind==='paste' ? '.pai-compose textarea' : '.pai-chat');
          target.dispatchEvent(kind==='paste' ? new ClipboardEvent('paste',{clipboardData:transfer,bubbles:true,cancelable:true}) : new DragEvent('drop',{dataTransfer:transfer,bubbles:true,cancelable:true}));
        }''' % base64.b64encode(PNG).decode()
        page.evaluate(inject, {'kind': 'paste'})
        expect(page.locator('.pai-image-draft')).to_have_count(1)
        page.evaluate(inject, {'kind': 'drop'})
        expect(page.locator('.pai-image-draft')).to_have_count(2)
        page.get_by_role('button', name='Remove image 1', exact=True).click()
        expect(page.locator('.pai-image-draft')).to_have_count(1)
        page.evaluate(inject, {'kind': 'drop', 'count': 5})
        expect(page.locator('.pai-status')).to_contain_text('at most five')
        expect(page.locator('.pai-image-draft')).to_have_count(1)
        page.evaluate(inject, {'kind': 'drop', 'type': 'image/svg+xml'})
        expect(page.locator('.pai-status')).to_contain_text('PNG, JPEG or WebP')
        page.evaluate(inject, {'kind': 'paste', 'size': 4 * 1024 * 1024 + 1})
        expect(page.locator('.pai-image-draft')).to_have_count(1)
        prompt = page.locator('.pai-compose textarea')
        prompt.fill('Hier fehlt eine Jahresauswahl, siehe Bild.')
        page.locator('.pai-compose .primary').click()
        expect(page.locator('.pai-status')).to_contain_text('Fixture rejected')
        expect(prompt).to_have_value('Hier fehlt eine Jahresauswahl, siehe Bild.')
        expect(page.locator('.pai-image-draft')).to_have_count(1)
        expect(page.get_by_role('button', name='Remove image 1', exact=True)).to_be_enabled()
        assert sent[0]['images'][0]['data_base64'] == base64.b64encode(PNG).decode()
        fail = False
        page.locator('.pai-compose .primary').click()
        expect(page.locator('.pai-image-draft')).to_have_count(0)
        expect(page.locator('.pai-chat-image')).to_have_count(1)
        expect(page.locator('.pai-messages')).to_contain_text('Dashboard')
        assert sent[-1]['images'][0]['media_type'] == 'image/png'
        # Image-only sends carry explicit fallback text, not an invalid empty turn.
        page.evaluate(inject, {'kind': 'paste'})
        expect(page.locator('.pai-image-draft')).to_have_count(1)
        prompt.fill('')
        page.locator('.pai-compose .primary').click()
        expect(page.locator('.pai-image-draft')).to_have_count(0)
        assert sent[-1]['message'] == 'Please inspect the attached images.'
        assert not errors
        browser.close()
