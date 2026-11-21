"""Exercise the real shared installer page without executing any install job."""
from urllib.parse import urlsplit

import pytest
from browser_driver import sync_browser
from setup.installer import web


@pytest.mark.parametrize('installed', [False, True])
def test_pb9_installer_mode_uses_separate_status_and_payload(tmp_path, monkeypatch, installed):
    """PB9 remains actionable with a dirty PB8 slot and submits only its mode."""
    status={'installed':True,'pb8_installed':True,'pb9_installed':installed,
        'pbgui_dir':str(tmp_path/'pbgui'),'maintenance_errors':['PB8 fixture has local changes'],
        'pb9_maintenance_errors':[],'update_all_errors':['PB8 fixture has local changes']}
    monkeypatch.setattr(web,'local_prerequisite_status',lambda:{'missing':[]})
    monkeypatch.setattr(web,'inspect_local_master_install',lambda _:dict(status))
    monkeypatch.setattr(web,'default_local_install_dir',lambda:str(tmp_path))
    monkeypatch.setattr(web,'default_local_master_name',lambda:'fixture')
    monkeypatch.setattr(web,'generate_pbgui_password',lambda:'fixture-not-a-credential')
    html=web._html()
    submitted=[];errors=[]
    def serve(route):
        """Fulfil only deterministic local fixture responses."""
        path=urlsplit(route.request.url).path
        if path=='/':route.fulfill(content_type='text/html',body=html)
        elif path=='/api/local-master-status':route.fulfill(json=status)
        elif path=='/api/install':
            submitted.append(route.request.post_data_json)
            route.fulfill(json={'job_id':'fixture'})
        elif path=='/api/status':route.fulfill(json={'id':'fixture','status':'running','logs':[]})
        elif path=='/api/public-ip':route.fulfill(json={'ip':'192.0.2.1'})
        else:route.fulfill(status=404,body='')
    with sync_browser() as runner:
        browser=runner.chromium.launch(headless=True)
        try:
            page=browser.new_page(viewport={'width':1280,'height':900})
            page.route('**/*',serve)
            page.on('pageerror',lambda error:errors.append(str(error)))
            page.goto('http://installer.test/')
            page.locator('#install-mode').select_option('local-pb9')
            button=page.get_by_role('button',name='Update PB9' if installed else 'Install PB9',exact=True)
            assert button.is_enabled()
            button.click()
            page.wait_for_function("document.getElementById('install-mode').value==='local-pb9'")
            assert len(submitted)==1
            assert submitted[0]['install_mode']=='local-pb9'
            assert submitted[0]['install_dir']==str(tmp_path)
            assert not errors
        finally:
            browser.close()
