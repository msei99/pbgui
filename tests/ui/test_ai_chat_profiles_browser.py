"""Real AI Setup menu path configures profiles/models and GitHub without a second chat."""
import json
from pathlib import Path
from urllib.parse import parse_qs,urlparse

import pytest
from playwright.sync_api import expect,sync_playwright

ROOT=Path(__file__).resolve().parents[2]


@pytest.mark.parametrize('prefix,width',[('',1400),('/mounted',700)])
def test_setup_profiles_models_probe_github_and_restoration(prefix,width):
    """Setup preserves location and model edits, shares credentials and never starts chat jobs."""
    work='b'*32
    state={'github':False,'probe_error':None,'profiles':[{'id':'default','name':'Personal'},{'id':work,'name':'Work'}],
           'preferences':{'selection':{'provider':'chatgpt','profile':'default','model':'model-default','effort':'','service_tier':''},'jev_max_cost_usd':.01}}
    tests=[];mutations=[];requests=[];errors=[];permission_checks=[]
    with sync_playwright() as driver:
        browser=driver.chromium.launch()
        page=browser.new_page(viewport={'width':width,'height':900})
        page.on('pageerror',lambda error:errors.append(str(error)))
        def respond(route):
            """All provider/GitHub calls are synthetic and every asset is local."""
            url=urlparse(route.request.url);path=url.path.removeprefix(prefix);query=parse_qs(url.query)
            requests.append((route.request.method,path))
            if path=='/app/start.html':
                route.fulfill(content_type='text/html',body='<nav id="topnav"></nav><script>window.PBGUI_BASE_PREFIX='+json.dumps(prefix)+';window.PBGUI_NAV_CONFIG={current:"welcome",authenticated:true};</script><script src="'+prefix+'/app/pbgui_nav.js"></script>')
            elif path=='/api/ai/main_page':
                html=(ROOT/'frontend/ai_chat.html').read_text()
                html=html.replace('"%%API_BASE%%"',json.dumps(prefix+'/api/ai')).replace('"%%BASE_PREFIX%%"',json.dumps(prefix)).replace('%%NAV_HASH%%','test').replace('%%VERSION%%','test').replace('%%SERIAL%%','1')
                route.fulfill(content_type='text/html',body=html.replace('src="/app/','src="'+prefix+'/app/').replace('href="/app/','href="'+prefix+'/app/'))
            elif path.startswith('/app/'):
                asset=ROOT/'frontend'/path.removeprefix('/app/')
                route.fulfill(body=asset.read_bytes() if asset.is_file() else b'',content_type='text/css' if asset.suffix=='.css' else 'text/javascript')
            elif path=='/api/ai/status':
                profile=query.get('profile',['default'])[0]
                route.fulfill(json={'providers':{'chatgpt':{'available':True,'connected':True,'profiles':state['profiles'],'email':profile+'@example.test'},'opencode-go':{'connected':True},'opencode-zen':{'connected':True},'openrouter':{'connected':True}}})
            elif path=='/api/ai/models':
                profile=query.get('profile',['default'])[0]
                model='typesafe/jev-1.13' if query.get('provider')==['openrouter'] else 'model-'+profile
                route.fulfill(json={'models':[{'id':model,'name':'Selected model '+profile,'tools':True,'reasoning_variants':[{'id':'high','label':'High'}],'service_tiers':[{'id':'priority','label':'Fast'}]}]})
            elif path=='/api/ai/preferences':
                if route.request.method=='PUT':state['preferences'].update(route.request.post_data_json);mutations.append((path,route.request.post_data_json))
                route.fulfill(json=state['preferences'])
            elif path=='/api/ai/test-model':
                chosen=route.request.post_data_json;tests.append(chosen)
                if state['probe_error']:route.fulfill(status=400,json={'detail':state['probe_error']})
                else:route.fulfill(json={**chosen,'reply':'PBGui connection OK','elapsed_seconds':.2,'tools_enabled':False,'conversation_created':False})
            elif path=='/api/debug/github':
                if route.request.method=='DELETE':state['github']=False
                route.fulfill(json={'configured':state['github'],'login':'fixture-user' if state['github'] else ''})
            elif path=='/api/debug/github/connect':
                mutations.append((path,route.request.post_data_json));state['github']=True
                route.fulfill(json={'configured':True,'login':'fixture-user'})
            elif path=='/api/debug/github/check-permissions':
                permission_checks.append(route.request.method)
                route.fulfill(json={'login':'fixture-user','ready':True,'permissions':'verified','issue_target_available':True,'write_test_performed':False})
            elif path=='/api/ai/providers/chatgpt/profiles':
                chosen=route.request.post_data_json
                item=next(item for item in state['profiles'] if item['id']==chosen['profile']);item['name']=chosen['name']
                route.fulfill(json=item)
            elif path=='/api/help/index':route.fulfill(json=[{'file':'45_ai_chat','title':'AI Setup and Chat'}])
            elif path=='/api/help/content':route.fulfill(json={'content':'# AI Setup and Chat\nModels, connections and support.'})
            elif path=='/api/ai/conversations':route.fulfill(json={'conversations':[]})
            else:route.fulfill(json={})
        page.route('**/*',respond)
        page.goto('http://setup.test'+prefix+'/app/start.html')
        page.locator('.nav-group-btn',has_text='Information').click()
        expect(page.locator('[data-page="info_ai_chat"]')).to_contain_text('AI Setup')
        page.locator('[data-page="info_ai_chat"]').click()
        expect(page.locator('h1')).to_have_text('AI Setup')
        expect(page.locator('#model-select')).to_have_value('model-default')
        expect(page.locator('#prompt')).to_have_count(0)
        expect(page.locator('details.field-help')).to_have_count(0)
        page.get_by_role('button',name='Test model',exact=True).hover()
        expect(page.get_by_role('tooltip')).to_be_hidden()
        page.get_by_role('button',name='Test model',exact=True).focus()
        expect(page.get_by_role('tooltip')).to_be_hidden()
        assert not tests
        page.keyboard.press('Escape');expect(page.get_by_role('tooltip')).to_be_hidden()
        page.locator('[data-section="opencode"]').click()
        test_label=page.locator('label:has(#opencode-test-model) > span')
        test_label.hover()
        expect(page.get_by_role('tooltip')).to_contain_text('fixed synthetic request')
        page.keyboard.press('Escape')
        help_label=page.locator('[data-view="opencode"] .provider-name[data-tip]')
        help_label.focus();expect(page.get_by_role('tooltip')).to_contain_text('One OpenCode key')
        page.keyboard.press('Escape');expect(page.get_by_role('tooltip')).to_be_hidden()
        help_label.click();expect(page.get_by_role('tooltip')).to_contain_text('One OpenCode key')
        assert page.get_by_role('tooltip').evaluate('(node)=>{const r=node.getBoundingClientRect();return r.left>=0 && r.right<=innerWidth && r.top>=0 && r.bottom<=innerHeight;}')
        help_label.evaluate('(node)=>node.dataset.tip="<img src=x onerror=alert(1)> plain text"')
        help_label.focus();help_label.click()
        expect(page.get_by_role('tooltip')).to_contain_text('<img src=x onerror=alert(1)>')
        expect(page.get_by_role('tooltip').locator('img')).to_have_count(0)
        page.keyboard.press('Escape')
        page.locator('#opencode-test-model').select_option(json.dumps(['opencode-zen','model-default'],separators=(',',':')))
        page.locator('#opencode-test').click()
        expect(page.locator('#opencode-test-result')).to_contain_text('Connection verified: opencode-zen')
        page.locator('#opencode-test-model').select_option(json.dumps(['opencode-go','model-default'],separators=(',',':')))
        page.locator('#opencode-test').click()
        expect(page.locator('#opencode-test-result')).to_contain_text('Connection verified: opencode-go')
        expect(page.get_by_role('tooltip')).to_be_hidden()
        assert tests[-1]=={'provider':'opencode-go','profile':'default','model':'model-default','effort':'','service_tier':''}
        page.locator('[data-section="models"]').click()
        page.locator('#provider-select').select_option('chatgpt:'+work)
        expect(page.locator('#model-select')).to_have_value('model-'+work)
        page.locator('#effort-select').select_option('high');page.locator('#speed-select').select_option('priority')
        page.get_by_role('button',name='Test model',exact=True).click()
        expect(page.locator('#test-output')).to_have_text('PBGui connection OK')
        assert tests[-1]=={'provider':'chatgpt','profile':work,'model':'model-'+work,'effort':'high','service_tier':'priority'}
        assert state['preferences']['selection']['profile']=='default'
        page.get_by_role('button',name='Use for new chats',exact=True).click()
        expect(page.locator('#setup-status')).to_contain_text('Default model saved')
        assert state['preferences']['selection']==tests[-1]
        page.locator('[data-section="chatgpt"]').click()
        expect(page.locator('#chatgpt-profile')).to_have_value(work)
        profile_actions=page.locator('#chatgpt-profile-add').locator('..')
        expect(profile_actions.locator('#chatgpt-disconnect')).to_be_visible()
        button_tops=[page.locator('#'+name).bounding_box()['y'] for name in ['chatgpt-profile-add','chatgpt-profile-rename','chatgpt-profile-remove','chatgpt-disconnect']]
        assert max(button_tops)-min(button_tops)<2
        page.locator('#chatgpt-profile-name').fill('Renamed work')
        page.get_by_role('button',name='Rename',exact=True).click()
        expect(page.locator('#chatgpt-profile-name')).to_have_value('Renamed work')
        page.locator('#chatgpt-test').click()
        expect(page.locator('#chatgpt-test-result')).to_contain_text('Connection verified: chatgpt')
        assert tests[-1]['profile']==work and tests[-1]['model']=='model-'+work
        assert state['preferences']['selection']['effort']=='high'
        page.locator('[data-section="openrouter"]').click()
        page.locator('#openrouter-test').click()
        expect(page.locator('#openrouter-test-result')).to_contain_text('Connection verified: openrouter')
        assert tests[-1]['model']=='typesafe/jev-1.13'
        state['probe_error']='Provider rejected this test request.'
        page.locator('#openrouter-test').click()
        expect(page.locator('#openrouter-test-result')).to_contain_text('Provider rejected this test request')
        expect(page.locator('#openrouter-test')).to_be_enabled()
        state['probe_error']=None
        page.locator('[data-section="github"]').click()
        token_box=page.locator('#github-token').bounding_box()
        button_box=page.locator('#github-connect').bounding_box()
        if width>760:
            assert abs(token_box['y']+token_box['height']-button_box['y']-button_box['height'])<2
        else:
            assert button_box['y']>=token_box['y']+token_box['height']
        assert page.locator('#github-connect-row').evaluate('(node)=>node.scrollWidth<=node.clientWidth')
        secret='ghp_SYNTHETIC_PRIVATE_TOKEN_123456789'
        page.get_by_label('GitHub personal access token',exact=True).fill(secret)
        page.get_by_role('button',name='Connect GitHub',exact=True).click()
        expect(page.locator('#github-status')).to_contain_text('fixture-user')
        expect(page.locator('#github-token')).to_have_value('')
        page.get_by_role('button',name='Check permissions',exact=True).click()
        expect(page.locator('#github-check-result')).to_contain_text('Required public_repo permission and issue repository verified')
        expect(page.locator('#github-check-result')).to_contain_text('No issue or attachment was created')
        assert permission_checks==['POST']
        destination=page.url
        page.locator('#pbgui-guide-btn').click();expect(page.locator('#pbgui-shared-help-ovl')).to_be_visible()
        assert page.url==destination
        page.locator('#pbgui-shared-help-close').click()
        if width>760:
            handle=page.get_by_role('separator',name='Resize sidebar')
            handle.press('Home');assert page.locator('#sidebar').bounding_box()['width']==160
            handle.press('End');assert page.locator('#sidebar').bounding_box()['width']==420
        assert page.locator('#main-content').evaluate('(node)=>node.scrollWidth<=node.clientWidth')
        page.reload()
        expect(page.locator('[data-view="github"]')).to_be_visible()
        expect(page.locator('#github-status')).to_contain_text('fixture-user')
        page.locator('[data-section="models"]').click()
        expect(page.locator('#model-select')).to_have_value('model-'+work)
        expect(page.locator('#effort-select')).to_have_value('high');expect(page.locator('#speed-select')).to_have_value('priority')
        assert not any(method=='POST' and ('/conversations' in path or '/turns' in path) for method,path in requests)
        storage=page.evaluate('JSON.stringify({local:{...localStorage},session:{...sessionStorage}})')
        assert secret not in storage and secret not in page.url
        page.locator('[data-section="opencode"]').click()
        expect(page.locator('#opencode-test-model')).to_have_value(json.dumps(['opencode-go','model-default'],separators=(',',':')))
        page.get_by_role('button',name='Open AI Chat',exact=True).click()
        expect(page.locator('#pbgui-ai-drawer')).to_have_class('open')
        expect(page.locator('.pai-support')).to_have_count(0)
        assert not errors
        browser.close()
