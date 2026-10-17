"""Local screenshot drafts are previewed and encrypted without publishing or browser storage."""
import base64
import json
import struct
import zlib
from pathlib import Path
from urllib.parse import urlparse

import pytest
from playwright.sync_api import sync_playwright, expect

ROOT = Path(__file__).resolve().parents[2]


def sample_png():
    """Generate a one-pixel test image without capturing the user's screen."""
    def chunk(name, data):
        """Encode a PNG chunk with its checksum."""
        return struct.pack('!I',len(data)) + name + data + struct.pack('!I',zlib.crc32(name+data))
    return b'\x89PNG\r\n\x1a\n' + chunk(b'IHDR',struct.pack('!2I5B',1,1,8,6,0,0,0)) + chunk(b'IDAT',zlib.compress(b'\x00\xff\x00\x00\xff')) + chunk(b'IEND',b'')


@pytest.mark.parametrize('width',[1400,700])
def test_screenshot_picker_paste_drop_removal_and_encrypted_snapshot(width):
    """Files, paste and drop enter only the encrypted snapshot; rejected images stay out."""
    png = sample_png()
    encoded = base64.b64encode(png).decode()
    report = {'format':'pbgui-support-debug-v1','system':{'pbgui_version':'fixture'},'records':{},'logs':{},
              'coverage':{'registered_routes':[],'sources':[],'omitted':[]}}
    html = (ROOT/'frontend/support_debug.html').read_text().replace('"%%API_BASE%%"','"/api/debug"').replace('"%%BASE_PREFIX%%"','""').replace('%%NAV_HASH%%','test')
    attachments, errors, requests = [], [], []
    with sync_playwright() as driver:
        browser = driver.chromium.launch()
        page = browser.new_page(viewport={'width':width,'height':850})
        page.on('pageerror',lambda error: errors.append(str(error)))
        def respond(route):
            """Keep every request offline and use no runtime state."""
            path = urlparse(route.request.url).path; requests.append(route.request.url)
            if path == '/api/debug/main_page':
                route.fulfill(body=html,content_type='text/html')
            elif path.startswith('/app/'):
                asset = ROOT/'frontend'/path.removeprefix('/app/')
                route.fulfill(body=asset.read_bytes() if asset.is_file() else b'',content_type='text/css' if asset.suffix=='.css' else 'text/javascript')
            elif path == '/api/debug/collect':
                route.fulfill(json=report)
            elif path == '/api/debug/issue-attachment':
                attachments.append(route.request.post_data_json); route.fulfill(body=b'PK-fixture',content_type='application/zip')
            else:
                route.fulfill(json={})
        page.route('**/*',respond)
        page.goto('http://screenshots.test/api/debug/main_page')
        page.locator('#debug-issue').click()
        page.locator('#support-issue-subject').fill('Display problem')
        page.locator('#support-issue-description').fill('A panel looks wrong.')
        chooser = page.get_by_label('Choose screenshots')
        chooser.set_input_files({'name':'private-account.png','mimeType':'image/png','buffer':png})
        expect(page.locator('.support-screenshot-card')).to_have_count(1)
        expect(page.locator('.support-screenshot-card img')).to_be_visible()
        assert page.locator('.support-screenshot-card img').evaluate('(n)=>n.naturalWidth') == 1
        assert page.locator('.support-screenshot-card img').get_attribute('src').startswith('blob:')
        page.get_by_role('button',name='Prepare encrypted attachment').click()
        expect(page.get_by_role('button',name='Continue to GitHub')).to_be_visible()
        assert base64.b64decode(attachments[-1]['screenshots'][0]['data_base64']) == png
        assert attachments[-1]['screenshots'][0]['name'] == 'private-account.png'
        public = page.locator('#support-issue-public-preview').input_value()
        assert 'private-account' not in public and encoded not in public and 'blob:' not in public
        assert 'Screenshots: 1' in public
        assert encoded not in page.evaluate('JSON.stringify({...sessionStorage,...localStorage})')
        chooser.set_input_files({'name':'unsafe.svg','mimeType':'image/svg+xml','buffer':b'<svg></svg>'})
        expect(page.locator('.support-screenshots .support-issue-error')).to_contain_text('Use PNG')
        expect(page.locator('.support-screenshot-card')).to_have_count(1)
        assert not any('github.com' in request for request in requests)
        chooser.set_input_files({'name':'fake.png','mimeType':'image/png','buffer':b'<svg></svg>'})
        expect(page.locator('.support-screenshots .support-issue-error')).to_contain_text('does not match')
        chooser.set_input_files({'name':'large.png','mimeType':'image/png','buffer':b'x'*(4*1024*1024+1)})
        expect(page.locator('.support-screenshots .support-issue-error')).to_contain_text('4 MiB')
        page.evaluate('''encoded => {
          const file = new File([Uint8Array.from(atob(encoded),c=>c.charCodeAt(0))], 'pasted.png', {type:'image/png'});
          const transfer = new DataTransfer(); transfer.items.add(file);
          document.querySelector('.support-issue-window').dispatchEvent(new ClipboardEvent('paste',{clipboardData:transfer,bubbles:true,cancelable:true}));
        }''',encoded)
        expect(page.locator('.support-screenshot-card')).to_have_count(2)
        expect(page.get_by_role('button',name='Continue to GitHub')).to_be_hidden()
        page.evaluate('''encoded => {
          const file = new File([Uint8Array.from(atob(encoded),c=>c.charCodeAt(0))], 'dropped.png', {type:'image/png'});
          const transfer = new DataTransfer(); transfer.items.add(file);
          document.querySelector('.support-issue-window').dispatchEvent(new DragEvent('drop',{dataTransfer:transfer,bubbles:true,cancelable:true}));
        }''',encoded)
        expect(page.locator('.support-screenshot-card')).to_have_count(3)
        page.get_by_role('button',name='Remove screenshot 2',exact=True).click()
        expect(page.locator('.support-screenshot-card')).to_have_count(2)
        page.get_by_role('button',name='Close issue report').click()
        page.locator('#debug-issue').click()
        expect(page.locator('.support-screenshot-card')).to_have_count(2)
        page.get_by_role('button',name='Prepare encrypted attachment').click()
        expect(page.get_by_role('button',name='Continue to GitHub')).to_be_visible()
        assert [item['name'] for item in attachments[-1]['screenshots']] == ['private-account.png','dropped.png']
        page.reload()
        expect(page.locator('.support-screenshot-card')).to_have_count(0)
        assert not errors
        browser.close()


@pytest.mark.parametrize('width,prefix', [(1400, ''), (700, '/mounted')])
def test_feature_requests_optional_diagnostics_and_private_screenshots(width, prefix):
    """Feature drafts use matching headings and can submit without diagnostic uploads."""
    from urllib.parse import parse_qs
    report = {'format':'pbgui-support-debug-v1','system':{'pbgui_version':'fixture'},
              'records':{'private-record':{'value':42}},'logs':{},'coverage':{}}
    html = (ROOT/'frontend/support_debug.html').read_text().replace('"%%API_BASE%%"',json.dumps(prefix+'/api/debug')).replace('"%%BASE_PREFIX%%"',json.dumps(prefix)).replace('%%NAV_HASH%%','test')
    attachments, collections, errors = [], [], []
    with sync_playwright() as driver:
        browser = driver.chromium.launch()
        page = browser.new_page(viewport={'width':width,'height':850})
        page.on('pageerror',lambda error: errors.append(str(error)))
        def respond(route):
            """Use only isolated local fixtures, including mounted attachment requests."""
            raw = urlparse(route.request.url).path
            path = raw.removeprefix(prefix) if prefix else raw
            if path == '/api/debug/main_page':
                route.fulfill(body=html,content_type='text/html')
            elif path.startswith('/app/'):
                asset = ROOT/'frontend'/path.removeprefix('/app/')
                route.fulfill(body=asset.read_bytes() if asset.is_file() else b'',content_type='text/css' if asset.suffix=='.css' else 'text/javascript')
            elif path == '/api/debug/collect':
                collections.append(route.request.post_data_json); route.fulfill(json=report)
            elif path == '/api/debug/issue-attachment':
                assert raw == prefix+'/api/debug/issue-attachment'
                attachments.append(route.request.post_data_json); route.fulfill(body=b'PK-fixture',content_type='application/zip')
            else:
                route.fulfill(json={})
        page.route('**/*',respond)
        page.goto('http://features.test'+prefix+'/api/debug/main_page')
        page.evaluate('window.opened=[];window.open=(url)=>{window.opened.push(url);return null;}')
        page.locator('#debug-issue').click()
        expect(page.get_by_role('button',name='Prepare encrypted attachment')).to_be_enabled()
        page.get_by_label('Issue type',exact=True).select_option('feature')
        page.get_by_label('Title',exact=True).fill('Improve the editor')
        page.get_by_label('What would you like and why?',exact=True).fill('Make comparing configurations easier.')
        page.get_by_label('Suggested solution (optional)',exact=True).fill('Show a side-by-side comparison.')
        page.get_by_label('Alternatives considered (optional)',exact=True).fill('Manual comparison.')
        expect(page.get_by_label('Diagnostic attachment',exact=True)).to_have_value('none')
        expect(page.locator('.support-issue-window .support-attachment-choices')).to_be_hidden()
        page.get_by_role('button',name='Continue to GitHub',exact=True).click()
        page.wait_for_function('window.opened.some(url => typeof url === "string" && url.startsWith("https://github.com/"))')
        body = parse_qs(urlparse(page.evaluate('window.opened.find(url => typeof url === "string" && url.startsWith("https://github.com/"))')).query)['body'][0]
        assert '### Feature request' in body and '### Suggested solution' in body
        assert '### Alternatives considered' in body and 'What happened?' not in body
        assert 'Attach the downloaded' not in body and not attachments
        expect(page.get_by_role('link',name='Open GitHub issue')).to_be_visible()
        page.get_by_label('Choose screenshots').set_input_files({'name':'private.png','mimeType':'image/png','buffer':sample_png()})
        expect(page.locator('.support-screenshot-card')).to_have_count(1)
        page.get_by_role('button',name='Prepare encrypted attachment').click()
        expect(page.get_by_role('button',name='Continue to GitHub',exact=True)).to_be_visible()
        assert set(attachments[-1]) == {'format','system','screenshots'}
        assert attachments[-1]['screenshots'][0]['name'] == 'private.png'
        assert 'private.png' not in page.locator('#support-issue-public-preview').input_value()
        page.get_by_label('Diagnostic attachment',exact=True).select_option('debug')
        expect(page.get_by_role('button',name='Prepare encrypted attachment')).to_be_visible()
        page.get_by_role('button',name='Prepare encrypted attachment').click()
        expect(page.get_by_role('button',name='Continue to GitHub',exact=True)).to_be_visible()
        assert attachments[-1]['records'] == report['records']
        page.get_by_label('Issue type',exact=True).select_option('problem')
        expect(page.get_by_label('What happened?',exact=True)).to_have_value('Make comparing configurations easier.')
        expect(page.get_by_role('button',name='Prepare encrypted attachment')).to_be_visible()
        page.get_by_label('Issue type',exact=True).select_option('feature')
        page.get_by_label('Diagnostic attachment',exact=True).select_option('none')
        page.get_by_role('button',name='Remove screenshot 1',exact=True).click()
        expect(page.get_by_role('button',name='Continue to GitHub',exact=True)).to_be_visible()
        before = len(collections)
        page.reload()
        expect(page.get_by_label('Issue type',exact=True)).to_have_value('feature')
        expect(page.get_by_label('Title',exact=True)).to_have_value('')
        assert len(collections) == before
        page.goto('http://features.test'+prefix+'/api/debug/main_page?context='+'a'*32)
        expect(page.get_by_label('Issue type',exact=True)).to_have_value('problem')
        assert not errors
        browser.close()
