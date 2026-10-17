"""The real support form investigates read-only evidence and publishes only its final reviewed payload."""
import base64
import json
import struct
import zlib
from pathlib import Path
from urllib.parse import urlparse

import pytest
from playwright.sync_api import expect,sync_playwright

ROOT=Path(__file__).resolve().parents[2]


def png_pixel(red):
    """Generate a distinguishable test PNG without capturing a real screen."""
    def chunk(name,data):
        """Encode a valid PNG fixture chunk."""
        return struct.pack('!I',len(data))+name+data+struct.pack('!I',zlib.crc32(name+data))
    return b'\x89PNG\r\n\x1a\n'+chunk(b'IHDR',struct.pack('!2I5B',1,1,8,6,0,0,0))+chunk(b'IDAT',zlib.compress(bytes([0,red,0,255-red,255])))+chunk(b'IEND',b'')


@pytest.mark.parametrize('width,prefix',[(1400,''),(700,'/mounted')])
def test_diagnose_apply_connect_review_public_image_choice_and_publish(width,prefix):
    """Diagnosis can stop without an issue; personal publication requires an exact final review."""
    report={'format':'pbgui-support-debug-v1','system':{'pbgui_version':'fixture'},'records':{'services':{'status':'failed'}},'logs':{'PBGui.log':['HTTP 503']},'coverage':{}}
    draft={'subject':'Handle failed service status polls','description':'Services appear stopped after a failed status poll.','steps':'Return HTTP 503.','expected':'Show status unavailable.'}
    analysis='### Evidence\nE1: frontend/services_monitor.html:2\n### Root cause / hypotheses\nLikely unchecked HTTP status.\n### Proposed fix\nCheck response.ok.\n### Validation still required\nNo tests were executed.'
    html=(ROOT/'frontend/support_debug.html').read_text().replace('"%%API_BASE%%"',json.dumps(prefix+'/api/debug')).replace('"%%BASE_PREFIX%%"',json.dumps(prefix)).replace('%%NAV_HASH%%','test')
    diagnose_posts=[]; review_posts=[]; publish_posts=[]; requests=[]; errors=[]
    state={'connected':False,'reviews':{},'tokens':[]}
    first,second=png_pixel(255),png_pixel(0)
    with sync_playwright() as driver:
        browser=driver.chromium.launch()
        page=browser.new_page(viewport={'width':width,'height':850})
        page.on('pageerror',lambda error:errors.append(str(error)))
        def respond(route):
            """Keep AI, credential, review and GitHub operations local and fully synthetic."""
            url=route.request.url;requests.append(url)
            path=urlparse(url).path.removeprefix(prefix) if prefix else urlparse(url).path
            if path=='/api/debug/main_page':
                route.fulfill(body=html,content_type='text/html')
            elif path.startswith('/app/'):
                asset=ROOT/'frontend'/path.removeprefix('/app/')
                route.fulfill(body=asset.read_bytes() if asset.is_file() else b'',content_type='text/css' if asset.suffix=='.css' else 'text/javascript')
            elif path=='/api/debug/collect':
                route.fulfill(json=report)
            elif path=='/api/debug/diagnose':
                diagnose_posts.append(route.request.post_data_json)
                route.fulfill(json={'draft':draft,'analysis_markdown':analysis,'questions':['Which version?'],'confidence':'likely','investigation':{'read_only':True,'tests_executed':False}})
            elif path=='/api/debug/github':
                route.fulfill(json={'configured':state['connected'],'login':'fixture-user' if state['connected'] else ''})
            elif path=='/api/debug/github/connect':
                state['tokens'].append(route.request.post_data_json);state['connected']=True
                route.fulfill(json={'configured':True,'login':'fixture-user'})
            elif path=='/api/debug/github/review':
                payload=route.request.post_data_json;review_posts.append(payload)
                identifier=format(len(review_posts),'032x')
                body=payload['body']+'\n\n[Encrypted debug report](https://github.com/fixture-user/pbgui-support-assets/releases/download/report-'+identifier+'/debug-report.zip)'
                if payload['public_images']:
                    body+='\n\n![Screenshot](https://github.com/fixture-user/pbgui-support-assets/releases/download/report-'+identifier+'/screenshot-1.png)'
                value={'review_id':identifier,'title':payload['title'],'body':body,'created_at':1,'status':'prepared','phase':'prepared','login':'fixture-user','target':'msei99/pbgui','assets_repository':'fixture-user/pbgui-support-assets','public_image_count':len(payload['public_images']),'encrypted_attachment':True,'issue_url':None,'error':''}
                state['reviews'][identifier]=value;route.fulfill(json=value)
            elif path.startswith('/api/debug/github/reviews/'):
                parts=path.split('/');identifier=parts[5]
                if route.request.method=='DELETE':
                    state['reviews'].pop(identifier,None);route.fulfill(json={'discarded':True})
                elif '/images/' in path:
                    route.fulfill(body=first,content_type='image/png')
                else:
                    route.fulfill(json=state['reviews'][identifier])
            elif path=='/api/debug/github/publish':
                payload=route.request.post_data_json;publish_posts.append(payload)
                value=state['reviews'][payload['review_id']]
                value.update(status='published',issue_url='https://github.com/msei99/pbgui/issues/422')
                route.fulfill(json=value)
            else:
                route.fulfill(json={})
        page.route('**/*',respond)
        page.goto('http://assistant.test'+prefix+'/api/debug/main_page')
        page.locator('#debug-issue').click()
        page.get_by_label('Choose screenshots').set_input_files([{'name':'private-red.png','mimeType':'image/png','buffer':first},{'name':'private-blue.png','mimeType':'image/png','buffer':second}])
        expect(page.locator('.support-screenshot-card')).to_have_count(2)
        page.get_by_role('button',name='Draft with AI',exact=True).click()
        page.get_by_label('Describe your problem or idea (any language)',exact=True).fill('Alle Dienste werden als gestoppt angezeigt. Warum?')
        page.get_by_role('button',name='Use screenshot 2 for AI').click()
        page.get_by_label('Diagnostic data for AI',exact=True).select_option('selected')
        page.get_by_role('button',name='Investigate with AI',exact=True).click()
        expect(page.get_by_role('button',name='Apply English draft',exact=True)).to_be_visible()
        assert diagnose_posts[0]['inspect_source'] is True
        assert diagnose_posts[0]['diagnostics']['records']==report['records']
        assert diagnose_posts[0]['screenshots'][0]['data_base64']==base64.b64encode(second).decode()
        assert not review_posts and not publish_posts
        expect(page.locator('#support-issue-subject')).to_have_value('')
        assert 'Root cause' in page.get_by_label('English AI draft',exact=True).input_value()
        page.get_by_role('button',name='Apply English draft',exact=True).click()
        expect(page.get_by_label('Analysis and proposed fix (public)',exact=True)).to_have_value(analysis)
        assert analysis in page.locator('#support-issue-public-preview').input_value()
        page.get_by_role('button',name='Review GitHub publication',exact=True).click()
        token=page.get_by_label('GitHub personal access token',exact=True)
        expect(token).to_be_focused()
        secret='ghp_SYNTHETIC_PRIVATE_TOKEN_123456789'
        token.fill(secret)
        page.get_by_role('button',name='Connect GitHub',exact=True).click()
        expect(page.get_by_role('button',name='Publish reviewed issue',exact=True)).to_be_visible()
        assert state['tokens']==[{'token':secret}]
        expect(token).to_have_value('')
        assert review_posts[-1]['public_images']==[] and not publish_posts
        assert '![' not in page.locator('#support-issue-public-preview').input_value()
        page.get_by_role('button',name='Publish screenshot 1 publicly').click()
        expect(page.get_by_role('button',name='Publish reviewed issue',exact=True)).to_be_hidden()
        page.get_by_role('button',name='Prepare publication review',exact=True).click()
        expect(page.get_by_role('button',name='Publish reviewed issue',exact=True)).to_be_enabled()
        expect(page.get_by_alt_text('Reviewed public screenshot 1')).to_be_visible()
        assert page.get_by_alt_text('Reviewed public screenshot 1').get_attribute('src').startswith('blob:')
        assert review_posts[-1]['public_images']==[0]
        assert review_posts[-1]['report']['screenshots'][0]['data_base64']==base64.b64encode(first).decode()
        assert '![Screenshot]' in page.locator('#support-issue-public-preview').input_value()
        assert not publish_posts
        page.get_by_role('button',name='Publish reviewed issue',exact=True).click()
        expect(page.get_by_role('link',name='Open published issue',exact=True)).to_be_visible()
        assert publish_posts==[{'review_id':format(2,'032x')}]
        assert all(secret not in url for url in requests)
        stored=page.evaluate('JSON.stringify({...sessionStorage,...localStorage})')
        assert secret not in stored and base64.b64encode(first).decode() not in stored
        assert not any(url.startswith('https://github.com/') for url in requests)
        page.reload()
        page.get_by_role('button',name='Review GitHub publication',exact=True).click()
        expect(page.get_by_role('link',name='Open published issue',exact=True)).to_be_visible()
        assert len(publish_posts)==1
        assert page.locator('.support-issue-window').evaluate('(n)=>n.scrollWidth<=n.clientWidth')
        assert not errors,errors
        browser.close()
