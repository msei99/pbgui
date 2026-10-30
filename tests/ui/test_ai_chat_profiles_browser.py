"""Real AI Setup menu path configures profiles/models and GitHub without a second chat."""
import json
from pathlib import Path
from urllib.parse import parse_qs,urlparse

import pytest
from browser_driver import expect,sync_browser

ROOT=Path(__file__).resolve().parents[2]


@pytest.mark.parametrize('prefix,width',[('',1400),('/mounted',761),('/mounted',760),('',700)])
def test_claude_setup_real_menu_connection_probe_and_navigation(prefix,width):
    """Native subscription controls preserve the real menu, Guide, model and mount."""
    requests,errors=[],[]
    selection={'provider':'claude-code','model':'claude-fixture','profile':'default','effort':'','service_tier':''}
    account={'connected':False,'login':False}
    with sync_browser() as driver:
        browser=driver.chromium.launch()
        page=browser.new_page(viewport={'width':width,'height':900})
        page.clock.install()
        page.on('pageerror',lambda error:errors.append(str(error)))
        def respond(route):
            """Use synthetic native login metadata and no external provider traffic."""
            url=urlparse(route.request.url);path=url.path.removeprefix(prefix)
            requests.append((route.request.method,path,url.query))
            if path=='/app/start.html':
                route.fulfill(content_type='text/html',body='<nav id="topnav"></nav><script>window.PBGUI_BASE_PREFIX='+json.dumps(prefix)+';window.PBGUI_NAV_CONFIG={current:"welcome",authenticated:true};</script><script src="'+prefix+'/app/pbgui_nav.js"></script>')
            elif path=='/api/ai/main_page':
                from api.ai import main_page
                from starlette.requests import Request
                request=Request({'type':'http','scheme':'http','server':('fixture',80),'root_path':prefix,'path':'/api/ai/main_page','headers':[],'query_string':b''})
                route.fulfill(content_type='text/html',body=main_page(request,object()).body)
            elif path.startswith('/app/'):
                asset=ROOT/'frontend'/path.removeprefix('/app/')
                route.fulfill(body=asset.read_bytes() if asset.is_file() else b'',content_type='text/css' if asset.suffix=='.css' else 'text/javascript')
            elif path=='/api/ai/preferences':route.fulfill(json={'selection':selection})
            elif path=='/api/ai/status':route.fulfill(json={'providers':{'claude-code':{'available':True,'connected':account['connected'],'catalog':['claude-fixture'] if account['connected'] else [],'login':{'id':'fixture-login','active':account['login'],'url':'https://claude.com/cai/oauth/authorize?client_id=fixture' if account['login'] else ''}}}})
            elif path=='/api/ai/providers/claude-code/connect':
                account['login']=True;route.fulfill(json={'started':True})
            elif path=='/api/ai/providers/claude-code/login-code':
                assert route.request.post_data_json=={'code':'synthetic-one-time-code','login_id':'fixture-login'}
                account.update(connected=True,login=False);route.fulfill(json={'submitted':True})
            elif path=='/api/ai/models':route.fulfill(json={'models':[{'id':'claude-fixture','name':'Claude fixture','tools':True}]})
            elif path=='/api/ai/test-model':
                assert route.request.post_data_json['provider']=='claude-code'
                route.fulfill(json={'provider':'claude-code','model':'claude-fixture','reply':'PBGui connection OK','elapsed_seconds':.1,'tools_enabled':False})
            elif path=='/api/ai/usage':route.fulfill(json={'message':'Subscription usage limits unavailable'})
            elif path=='/api/ai/conversations':route.fulfill(json={'conversations':[]})
            elif path=='/api/help/index':route.fulfill(json=[{'file':'45_ai_chat','title':'AI Setup and Chat'}])
            elif path=='/api/help/content':route.fulfill(json={'content':'# AI Setup and Chat\nClaude subscription.'})
            elif path.endswith('/stream'):route.fulfill(status=204,body='')
            else:route.fulfill(json={})
        page.route('**/*',respond)
        page.goto('http://claude-setup.test'+prefix+'/app/start.html')
        page.locator('.nav-group-btn',has_text='Information').click()
        page.locator('[data-page="info_ai_chat"]').click()
        page.locator('[data-section="claude-code"]').click()
        expect(page.locator('#claude-code-status')).to_have_text('Not connected')
        expect(page.get_by_label('Anthropic API key',exact=True)).to_have_count(0)
        page.locator('#claude-code-connect').click()
        expect(page.locator('#claude-code-login-link')).to_have_attribute('href','https://claude.com/cai/oauth/authorize?client_id=fixture')
        page.locator('#claude-code-login-code').fill('synthetic-one-time-code')
        page.locator('#claude-code-login-submit').click()
        expect(page.locator('#claude-code-status')).to_have_text('Connected')
        expect(page.locator('#claude-code-login-code')).to_have_value('')
        page.locator('#claude-code-test').click()
        expect(page.locator('#claude-code-test-result')).to_contain_text('PBGui connection OK')
        destination=page.url
        page.locator('#pbgui-guide-btn').click()
        expect(page.locator('#pbgui-shared-help-ovl')).to_be_visible()
        assert page.url==destination
        page.locator('#pbgui-shared-help-close').click()
        if width>760:
            handle=page.get_by_role('separator',name='Resize sidebar')
            for key,size in [('Home',160),('End',420)]:
                handle.press(key)
                assert page.locator('#sidebar').bounding_box()['width']==size
                assert page.locator('#sidebar-inner').evaluate('(n)=>n.scrollWidth<=n.clientWidth')
        assert page.locator('#main-content').evaluate('(n)=>n.scrollWidth<=n.clientWidth')
        page.reload()
        expect(page.locator('[data-view="claude-code"]')).to_be_visible()
        expect(page.locator('#claude-code-status')).to_have_text('Connected')
        page.locator('[data-section="models"]').click()
        expect(page.locator('#provider-select')).to_have_value('claude-code')
        expect(page.locator('#model-select')).to_have_value('claude-fixture')
        expect(page.locator('#save-model')).to_be_enabled()
        page.locator('#open-assistant').click()
        expect(page.locator('#pai-provider')).to_have_value('claude-code')
        expect(page.locator('#pai-model')).to_have_value('claude-fixture')
        expect(page.get_by_role('button',name='Send',exact=True)).to_be_enabled()
        assert not any('synthetic-one-time-code' in query for _,_,query in requests)
        assert not errors
        browser.close()


@pytest.mark.parametrize('prefix,width',[('',1400),('/mounted',700)])
def test_setup_profiles_models_probe_github_and_restoration(prefix,width):
    """Setup preserves location and model edits, shares credentials and never starts chat jobs."""
    work='b'*32
    state={'github':False,'probe_error':None,'profiles':[{'id':'default','name':'Personal'},{'id':work,'name':'Work'}],
           'preferences':{'selection':{'provider':'chatgpt','profile':'default','model':'model-default','effort':'','service_tier':''},'jev_max_cost_usd':.01}}
    tests=[];mutations=[];requests=[];errors=[];permission_checks=[]
    held_saves=[];held_status=[]
    with sync_browser() as driver:
        browser=driver.chromium.launch()
        page=browser.new_page(viewport={'width':width,'height':900})
        page.clock.install()
        page.on('pageerror',lambda error:errors.append(str(error)))

        def save_profile(route):
            """Model registry responses, including stale-client conflicts, without real files."""
            chosen=route.request.post_data_json;name=chosen['name'].strip()
            item=next((item for item in state['profiles'] if item['id']==chosen['profile']),None)
            if item and item['name'].strip()==name:
                route.fulfill(json=item);return
            if any(row['name'].strip()==name and row is not item for row in state['profiles']):
                route.fulfill(status=400,json={'detail':'A profile named "'+name+'" already exists. Choose a different name.'});return
            if item:item['name']=name
            else:
                item={'id':format(len(state['profiles']),'032x'),'name':name};state['profiles'].append(item)
            route.fulfill(json=item)

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
                payload=json.loads(json.dumps({'providers':{'chatgpt':{'available':True,'connected':True,'profiles':state['profiles'],'email':profile+'@example.test'},'opencode-go':{'connected':True},'opencode-zen':{'connected':True},'openrouter':{'connected':True}}}))
                if state.pop('hold_status',False):
                    held_status.append((route,payload));page.evaluate('window.profileStatusHeld=true')
                else:route.fulfill(json=payload)
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
                if state.pop('hold_save',False):
                    held_saves.append(route);page.evaluate('window.profileSaveHeld=true')
                else:save_profile(route)
            elif path.startswith('/api/ai/providers/chatgpt/profiles/') and route.request.method=='DELETE':
                state['profiles']=[row for row in state['profiles'] if row['id']!=path.rsplit('/',1)[1]]
                route.fulfill(json={'success':True})
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

        # Exercise registry mutations through the real menu/page, with no provider calls.
        name_input=page.locator('#chatgpt-profile-name')
        add=page.get_by_role('button',name='Add profile',exact=True)
        rename=page.get_by_role('button',name='Rename',exact=True)
        status=page.locator('#setup-status')

        def post_count():
            """Count only profile writes, not model/preferences requests."""
            return requests.count(('POST','/api/ai/providers/chatgpt/profiles'))

        def hold_next_status():
            """Delay a normal visibility-triggered update until the test releases it."""
            state['hold_status']=True
            page.evaluate('window.profileStatusHeld=false;document.dispatchEvent(new Event("visibilitychange"))')
            page.wait_for_function('window.profileStatusHeld')

        def release_status():
            """Deliver the captured snapshot, which may now be stale."""
            route,payload=held_status.pop(0);route.fulfill(json=payload)

        def start_delayed_save(button,label):
            """Allow edits while the server response is pending."""
            state['hold_save']=True;page.evaluate('window.profileSaveHeld=false')
            name_input.fill(label);button.click()
            page.wait_for_function('window.profileSaveHeld')
            expect(add).to_be_disabled()

        count_before=len(state['profiles']);posts=post_count()
        name_input.fill('Trading');add.click()
        expect(name_input).to_have_value('');expect(add).to_be_enabled()
        trading=state['profiles'][-1]['id']
        expect(page.locator('#chatgpt-profile')).to_have_value(trading)
        assert len(state['profiles'])==count_before+1 and post_count()==posts+1
        # Delayed automatic status must preserve both intentional emptiness and newer edits.
        hold_next_status();release_status();expect(name_input).to_have_value('')
        with page.expect_response('**/api/ai/status?*'):
            page.clock.run_for(15001)
        expect(name_input).to_have_value('')
        add.click();expect(status).to_have_text('Enter a profile name.')
        assert post_count()==posts+1
        inventory=page.evaluate('async (url)=>(await (await fetch(url)).json()).providers.chatgpt.profiles',prefix+'/api/ai/status')
        assert len(inventory)==count_before+1 and sum(row['name']=='Trading' for row in inventory)==1
        name_input.fill(' Trading ');add.click()
        expect(status).to_have_text('A profile named "Trading" already exists. Choose a different name.')
        assert post_count()==posts+1
        name_input.fill('Renamed work');rename.click()
        expect(status).to_have_text('A profile named "Renamed work" already exists. Choose a different name.')
        assert post_count()==posts+1

        # The server can learn about another profile before this browser does.
        state['profiles'].append({'id':'e'*32,'name':'Remote'})
        name_input.fill('Remote');add.click()
        expect(add).to_be_enabled()
        expect(page.locator('#chatgpt-profile option[value="'+('e'*32)+'"]')).to_have_count(1)
        expect(status).to_have_text('A profile named "Remote" already exists. Choose a different name.')
        expect(name_input).to_have_value('Remote')
        assert post_count()==posts+2 and len(state['profiles'])==count_before+2

        hold_next_status();name_input.fill('Status draft');release_status()
        expect(name_input).to_have_value('Status draft')
        start_delayed_save(add,'Delayed')
        name_input.fill('Newer add draft');save_profile(held_saves.pop(0))
        expect(add).to_be_enabled();expect(name_input).to_have_value('Newer add draft')
        delayed=state['profiles'][-1]['id']
        expect(page.locator('#chatgpt-profile')).to_have_value(delayed)
        start_delayed_save(rename,'Renamed delayed')
        name_input.fill('Newer rename draft');save_profile(held_saves.pop(0))
        expect(add).to_be_enabled();expect(name_input).to_have_value('Newer rename draft')
        assert next(row for row in state['profiles'] if row['id']==delayed)['name']=='Renamed delayed'

        # Both profile selectors must reset the post-Add empty state.
        name_input.fill('Switch test');add.click()
        expect(add).to_be_enabled();expect(name_input).to_have_value('')
        page.locator('#chatgpt-profile').select_option(work)
        expect(name_input).to_have_value('Renamed work')
        name_input.fill('Provider switch test');add.click()
        expect(add).to_be_enabled();expect(name_input).to_have_value('')
        page.locator('[data-section="models"]').click()
        page.locator('#provider-select').select_option('chatgpt:'+work)
        page.locator('[data-section="chatgpt"]').click()
        expect(page.locator('#chatgpt-profile')).to_have_value(work)
        expect(name_input).to_have_value('Renamed work')
        name_input.fill('Remove test');add.click()
        expect(add).to_be_enabled();expect(name_input).to_have_value('')
        page.get_by_role('button',name='Remove',exact=True).click()
        page.get_by_role('button',name='Remove profile',exact=True).click()
        expect(name_input).to_have_value('Personal')

        # Pre-existing duplicate labels remain distinct, selectable and removable by ID.
        legacy_ids=['c'*32,'d'*32]
        state['profiles'].extend({'id':identifier,'name':'Legacy'} for identifier in legacy_ids)
        hold_next_status();release_status()
        expect(page.locator('#chatgpt-profile option',has_text='Legacy')).to_have_count(2)
        page.locator('#chatgpt-profile').select_option(legacy_ids[0])
        expect(name_input).to_have_value('Legacy')
        rename.click();expect(add).to_be_enabled()
        expect(status).to_have_text('Profile saved.')
        page.get_by_role('button',name='Remove',exact=True).click()
        page.get_by_role('button',name='Remove profile',exact=True).click()
        expect(page.locator('#chatgpt-profile option',has_text='Legacy')).to_have_count(1)
        assert any(row['id']==legacy_ids[1] for row in state['profiles'])
        page.locator('#chatgpt-profile').select_option(work)
        expect(name_input).to_have_value('Renamed work')
        page.locator('[data-section="models"]').click()
        page.locator('#effort-select').select_option('high');page.locator('#speed-select').select_option('priority')
        page.locator('[data-section="chatgpt"]').click()
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
