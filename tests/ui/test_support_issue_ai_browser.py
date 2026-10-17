"""English issue drafting is explicit, reviewable, text-only and safe against stale responses."""
import json
import base64
import struct
import zlib
from pathlib import Path
from urllib.parse import urlparse

import pytest
from playwright.sync_api import expect, sync_playwright

ROOT = Path(__file__).resolve().parents[2]


def synthetic_image():
    """Generate one PNG pixel, without taking a screenshot of any real surface."""
    def chunk(name, data):
        """Encode a valid fixture PNG chunk."""
        return struct.pack('!I',len(data))+name+data+struct.pack('!I',zlib.crc32(name+data))
    return b'\x89PNG\r\n\x1a\n'+chunk(b'IHDR',struct.pack('!2I5B',1,1,8,6,0,0,0))+chunk(b'IDAT',zlib.compress(b'\x00\xff\x00\x00\xff'))+chunk(b'IEND',b'')


@pytest.mark.parametrize('kind,width,prefix', [('problem',1400,''),('feature',700,'/mounted')])
def test_english_drafting_review_apply_errors_and_stale_results(kind, width, prefix):
    """Drafting never uploads diagnostics or overwrites edits before explicit application."""
    report = {'format':'pbgui-support-debug-v1','system':{'pbgui_version':'fixture'},
              'records':{'private-record':{'error':'PRIVATE_DIAGNOSTIC'}},'logs':{},'coverage':{}}
    draft = dict(subject='Compare configurations',description='Please add a configuration comparison.',steps='Show differences side by side.',expected='')
    html = (ROOT/'frontend/support_debug.html').read_text().replace('"%%API_BASE%%"',json.dumps(prefix+'/api/debug')).replace('"%%BASE_PREFIX%%"',json.dumps(prefix)).replace('%%NAV_HASH%%','test')
    posts, errors = [], []
    outcome = {'status':200}
    with sync_playwright() as driver:
        browser = driver.chromium.launch()
        page = browser.new_page(viewport={'width':width,'height':850})
        page.on('pageerror',lambda error: errors.append(str(error)))
        def respond(route):
            """All provider and diagnostic requests use local synthetic responses."""
            raw = urlparse(route.request.url).path
            path = raw.removeprefix(prefix) if prefix else raw
            if path == '/api/debug/main_page':
                route.fulfill(body=html,content_type='text/html')
            elif path.startswith('/app/'):
                asset = ROOT/'frontend'/path.removeprefix('/app/')
                route.fulfill(body=asset.read_bytes() if asset.is_file() else b'',content_type='text/css' if asset.suffix=='.css' else 'text/javascript')
            elif path == '/api/debug/collect':
                route.fulfill(json=report)
            elif path == '/api/debug/issue-draft':
                assert raw == prefix+'/api/debug/issue-draft'
                posts.append(route.request.post_data_json)
                route.fulfill(status=outcome['status'],json={'draft':draft,'provider':'fixture','model':'fixture'} if outcome['status']==200 else {'detail':'Select a connected text model in AI Chat first.'})
            else:
                route.fulfill(json={})
        page.route('**/*',respond)
        page.goto('http://draft.test'+prefix+'/api/debug/main_page')
        page.locator('#debug-issue').click()
        page.get_by_label('Issue type',exact=True).select_option(kind)
        assert not posts
        page.get_by_role('button',name='Draft with AI',exact=True).click()
        source = page.get_by_label('Describe your problem or idea (any language)',exact=True)
        expect(source).to_be_visible()
        expect(source).to_be_focused()
        expect(page.get_by_role('button',name='Generate English draft',exact=True)).to_be_disabled()
        expect(page.locator('.support-issue-ai [role="alert"]')).to_be_empty()
        assert not posts
        assert page.locator('.support-issue-ai').bounding_box()['y'] < page.locator('#support-issue-subject').bounding_box()['y']
        source.fill('Bitte einen Vergleich einbauen. PRIVATER_ENTWURF')
        page.get_by_role('button',name='Generate English draft',exact=True).click()
        expect(page.get_by_role('button',name='Apply English draft',exact=True)).to_be_visible()
        assert posts == [dict(issue_type=kind,subject='',description='Bitte einen Vergleich einbauen. PRIVATER_ENTWURF',steps='',expected='')]
        expect(page.locator('#support-issue-description')).to_have_value('')
        assert draft['description'] in page.get_by_label('English AI draft',exact=True).input_value()
        assert 'PRIVATE_DIAGNOSTIC' not in json.dumps(posts)
        assert 'PRIVATER_ENTWURF' not in page.evaluate('JSON.stringify({...sessionStorage,...localStorage})')
        page.get_by_role('button',name='Apply English draft',exact=True).click()
        expect(page.locator('#support-issue-subject')).to_have_value(draft['subject'])
        expect(page.locator('#support-issue-description')).to_have_value(draft['description'])
        outcome['status'] = 200
        png = synthetic_image()
        page.get_by_label('Choose screenshots').set_input_files([
            {'name':'private-one.png','mimeType':'image/png','buffer':png},
            {'name':'private-two.png','mimeType':'image/png','buffer':png}])
        expect(page.locator('.support-ai-image')).to_have_count(2)
        for index in [1,2]:
            expect(page.get_by_role('button',name=f'Use screenshot {index} for AI')).to_have_attribute('aria-pressed','false')
        expect(page.get_by_text('Select screenshots for AI analysis.',exact=False)).to_be_visible()
        page.get_by_role('button',name='Generate English draft',exact=True).click()
        expect(page.get_by_role('button',name='Apply English draft',exact=True)).to_be_visible()
        assert 'screenshots' not in posts[-1]
        page.get_by_role('button',name='Use screenshot 2 for AI').click()
        expect(page.get_by_role('button',name='Apply English draft',exact=True)).to_be_hidden()
        page.get_by_role('button',name='Generate English draft',exact=True).click()
        expect(page.get_by_role('button',name='Apply English draft',exact=True)).to_be_visible()
        assert posts[-1]['screenshots'] == [dict(media_type='image/png',data_base64=base64.b64encode(png).decode())]
        assert 'private-two.png' not in json.dumps(posts[-1]) and 'PRIVATE_DIAGNOSTIC' not in json.dumps(posts[-1])
        page.get_by_role('button',name='Use screenshot 2 for AI').click()
        page.get_by_role('button',name='Generate English draft',exact=True).click()
        expect(page.get_by_role('button',name='Apply English draft',exact=True)).to_be_visible()
        assert 'screenshots' not in posts[-1]
        expect(page.locator('.support-screenshot-card')).to_have_count(2)
        assert 'Screenshots: 2' in page.locator('#support-issue-public-preview').input_value()
        assert draft['description'] in page.locator('#support-issue-public-preview').input_value()
        source.fill('')
        expect(page.get_by_role('button',name='Apply English draft',exact=True)).to_be_hidden()
        page.get_by_role('button',name='Draft with AI',exact=True).click()
        expect(source).to_have_value(draft['description'])
        outcome['status'] = 400
        page.get_by_role('button',name='Generate English draft',exact=True).click()
        expect(page.locator('.support-issue-ai [role="alert"]')).to_contain_text('Select a connected')
        expect(page.locator('#support-issue-description')).to_have_value(draft['description'])
        page.evaluate('''() => {
          const original = window.fetch;
          window.fetch = function(url, options) {
            if (String(url).endsWith('/issue-draft')) {
              window.draftSignal = options.signal;
              return new Promise(resolve => { window.resolveDraft = resolve; });
            }
            return original.apply(this, arguments);
          };
        }''')
        for action in ['edit','source-edit','cancel','close','type','image-choice','image-remove']:
            page.get_by_role('button',name='Generate English draft',exact=True).click()
            expect(page.get_by_role('button',name='Cancel AI draft',exact=True)).to_be_visible()
            if action == 'edit':
                page.locator('#support-issue-description').fill('Keep my newer text')
            elif action == 'source-edit':
                source.fill('New source text')
            elif action == 'cancel':
                page.get_by_role('button',name='Cancel AI draft',exact=True).click()
            elif action == 'close':
                page.get_by_role('button',name='Close issue report').click()
                page.locator('#debug-issue').click()
            elif action == 'type':
                page.get_by_label('Issue type',exact=True).select_option('feature' if kind=='problem' else 'problem')
            elif action == 'image-choice':
                page.get_by_role('button',name='Use screenshot 1 for AI').click()
            else:
                page.get_by_role('button',name='Remove screenshot 1',exact=True).click()
            assert page.evaluate('window.draftSignal.aborted')
            page.evaluate('(draft)=>window.resolveDraft({ok:true,json:async()=>({draft})})',draft)
            expect(page.get_by_role('button',name='Apply English draft',exact=True)).to_be_hidden()
            expect(page.locator('#support-issue-description')).to_have_value('Keep my newer text')
            expect(page.get_by_role('button',name='Generate English draft',exact=True)).to_be_enabled()
        assert not errors
        assert page.locator('.support-issue-window').evaluate('(n)=>n.scrollWidth<=n.clientWidth')
        browser.close()
