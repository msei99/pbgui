"""Official GitHub publication uses owner-bound immutable reviews and conservative retry recovery."""
import asyncio
import base64
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import support_github as github

OWNER='a'*32
OTHER='b'*32
TOKEN='ghp_'+'SYNTHETIC_TOKEN_'*3


@pytest.fixture
def service(tmp_path,monkeypatch):
    """Store only synthetic test credentials and use no ambient GitHub authentication."""
    store=github.SupportGitHubStore(tmp_path/'vault')
    store.write(OWNER,{'login':'fixture-user','token':TOKEN})
    monkeypatch.setattr(github,'encrypt_report',lambda raw:b'PK-ENCRYPTED-FIXTURE')
    return github.SupportGitHubService(store)


def report():
    """Provide synthetic PNG signature bytes, never an actual user screenshot."""
    return {'format':'pbgui-support-debug-v1','screenshots':[{'name':'private-account.png','media_type':'image/png','data_base64':base64.b64encode(b'\x89PNG\r\n\x1a\nimage').decode()}]}


def test_vault_is_private_encrypted_owner_bound_and_not_revealable(service):
    """Tokens and report bodies never appear in plaintext storage or status responses."""
    status=asyncio.run(service.status(OWNER))
    assert status=={'configured':True,'login':'fixture-user'}
    for path in service.store.root.iterdir():
        assert path.stat().st_mode & 0o777==0o600
        assert TOKEN.encode() not in path.read_bytes()
    assert service.store.root.stat().st_mode & 0o777==0o700
    assert asyncio.run(service.status(OTHER))=={'configured':False,'login':''}
    with pytest.raises(github.GitHubSupportError):
        service.store.read('../escape')


def test_prepare_is_local_review_first_and_public_images_are_explicit(service,monkeypatch):
    """Preparation performs zero GitHub writes and shows exact public attachment URLs."""
    remote=AsyncMock(); monkeypatch.setattr(github,'github_request',remote)
    review=asyncio.run(service.prepare(OWNER,'A problem','English description',report(),[]))
    assert review['status']=='prepared' and review['public_image_count']==0
    assert 'Encrypted debug report' in review['body'] and '![' not in review['body']
    assert 'private-account.png' not in json.dumps(review) and TOKEN not in json.dumps(review)
    stored=service.store.read(OWNER,review['review_id'])
    assert len(stored['assets'])==1
    assert 'English description' not in service.store.path(OWNER,review['review_id']).read_bytes().decode(errors='ignore')
    remote.assert_not_called()
    assert service.store.read(OTHER,review['review_id']) is None
    public=asyncio.run(service.prepare(OWNER,'A problem','English description',report(),[0]))
    assert public['public_image_count']==1 and '![Screenshot]' in public['body']
    assert public['body'].endswith('<!-- pbgui-support:'+public['review_id']+' -->')


def test_publish_immutable_review_exactly_once_without_assets(service,monkeypatch):
    """Repeated approval uses the same receipt and never posts a duplicate issue."""
    remote=AsyncMock(return_value={'number':422})
    monkeypatch.setattr(github,'github_request',remote)
    async def run():
        review=await service.prepare(OWNER,'Reviewed title','Reviewed body')
        result=await service.publish(OWNER,review['review_id'])
        assert result['issue_url']=='https://github.com/msei99/pbgui/issues/422'
        assert result['status']=='published'
        assert await service.publish(OWNER,review['review_id'])==result
        assert remote.await_count==1
        assert remote.call_args.args[2:]==('POST','/repos/msei99/pbgui/issues')
        assert remote.call_args.kwargs['payload']=={'title':'Reviewed title','body':review['body']}
    asyncio.run(run())


@pytest.mark.parametrize('found',[True,False])
def test_uncertain_issue_creation_reconciles_without_reposting(service,monkeypatch,found):
    """A lost issue response is checked by its marker, even with a new service instance."""
    posts=[]
    async def remote(session,token,method,path,**kwargs):
        """Simulate GitHub creating an issue while its response is lost."""
        if method=='POST':
            posts.append(kwargs['payload'])
            raise github.GitHubSupportError('Lost response',502)
        return [{'number':99,'body':posts[0]['body']}] if found else []
    monkeypatch.setattr(github,'github_request',remote)
    async def run():
        review=await service.prepare(OWNER,'Title','Body')
        with pytest.raises(github.GitHubSupportError):
            await service.publish(OWNER,review['review_id'])
        assert service.store.read(OWNER,review['review_id'])['status']=='uncertain'
        resumed=github.SupportGitHubService(service.store)
        if found:
            assert (await resumed.publish(OWNER,review['review_id']))['issue_url'].endswith('/99')
        else:
            with pytest.raises(github.GitHubSupportError,match='remains unconfirmed'):
                await resumed.publish(OWNER,review['review_id'])
        assert len(posts)==1
    asyncio.run(run())


def test_release_assets_use_official_upload_and_resume_without_duplicate_binaries(service,monkeypatch):
    """Encrypted ZIP and approved public PNG are release assets, never Git commits or issue blobs."""
    calls=[]; uploaded=[]; state={'repo':False,'release':False,'failed':False}
    async def remote(session,token,method,path,**kwargs):
        """Model a partial upload followed by a safe resumed publication."""
        calls.append((method,path,kwargs))
        if path=='/user/repos':
            state['repo']=True
            assert kwargs['payload']['private'] is False
            return {'private':False,'full_name':'fixture-user/pbgui-support-assets'}
        if path=='/repos/fixture-user/pbgui-support-assets':
            return {'private':False,'full_name':'fixture-user/pbgui-support-assets'} if state['repo'] else None
        if '/releases/tags/' in path:
            return {'id':1} if state['release'] else None
        if path.endswith('/releases'):
            state['release']=True;return {'id':1,'upload_url':'https://evil.test/not-used'}
        if path.endswith('/assets?per_page=100'):
            return uploaded
        if kwargs.get('upload'):
            name=path.split('name=')[1]
            if name.endswith('.png') and not state['failed']:
                state['failed']=True;raise github.GitHubSupportError('Upload unavailable',502)
            data=kwargs['binary'];uploaded.append({'name':name,'size':len(data),'digest':'sha256:'+hashlib.sha256(data).hexdigest()})
            return {'id':len(uploaded)}
        if path.endswith('/issues'):
            return {'number':100}
        raise AssertionError((method,path))
    monkeypatch.setattr(github,'github_request',remote)
    async def run():
        review=await service.prepare(OWNER,'Title','Body',report(),[0])
        with pytest.raises(github.GitHubSupportError):
            await service.publish(OWNER,review['review_id'])
        result=await service.publish(OWNER,review['review_id'])
        assert result['status']=='published'
        assert len([c for c in calls if 'name=debug-report.zip' in c[1]])==1
        assert sorted(item['name'] for item in uploaded)==['debug-report.zip','screenshot-1.png']
        assert all('evil.test' not in path for _,path,_ in calls)
        assert service.store.read(OWNER,review['review_id'])['assets']==[]
    asyncio.run(run())


def test_private_asset_repository_never_becomes_public_implicitly(service,monkeypatch):
    """Existing private repositories stop the publication; no visibility change is attempted."""
    remote=AsyncMock(return_value={'private':True,'full_name':'fixture-user/pbgui-support-assets'})
    monkeypatch.setattr(github,'github_request',remote)
    async def run():
        review=await service.prepare(OWNER,'Title','Body',report())
        with pytest.raises(github.GitHubSupportError,match='must be public'):
            await service.publish(OWNER,review['review_id'])
        assert remote.await_count==1
    asyncio.run(run())


def test_claim_discard_owner_and_capacity_guards(service):
    """Cross-process claiming and local discard preserve pending publication receipts."""
    async def run():
        reviews=[await service.prepare(OWNER,'Title','Body') for _ in range(4)]
        with pytest.raises(github.GitHubSupportError,match='capacity'):
            await service.prepare(OWNER,'Title','Body')
        identifier=reviews[0]['review_id']
        with pytest.raises(github.GitHubSupportError):
            service.store.claim(OTHER,identifier)
        service.store.claim(OWNER,identifier)
        with pytest.raises(github.GitHubSupportError,match='already being published'):
            service.store.claim(OWNER,identifier)
        with pytest.raises(github.GitHubSupportError):
            service.store.discard(OWNER,identifier)
        service.store.discard(OWNER,reviews[1]['review_id'])
        assert service.store.read(OWNER,reviews[1]['review_id']) is None
    asyncio.run(run())


def test_dead_api_publisher_and_expired_uncertain_receipt_can_be_checked(service,monkeypatch):
    """A hard restart never leaves the UI polling a dead publisher or reposting an old issue."""
    async def run():
        review=await service.prepare(OWNER,'Title','Body')
        row=service.store.read(OWNER,review['review_id'])
        row.update(status='publishing',phase='issue',pid=99999999,created_at=0)
        service.store.write(OWNER,row,review['review_id'])
        monkeypatch.setattr(github,'process_alive',lambda pid:False)
        recovered=service.store.read(OWNER,review['review_id'])
        assert recovered['status']=='uncertain'
        remote=AsyncMock(return_value=[{'number':123,'body':review['body']}])
        monkeypatch.setattr(github,'github_request',remote)
        assert (await service.publish(OWNER,review['review_id']))['status']=='published'
        assert all(call.args[2]=='GET' for call in remote.call_args_list)
    asyncio.run(run())


def test_missing_vault_key_is_not_silently_replaced(service):
    """Losing the private key requires recovery rather than destroying access to saved tokens."""
    (service.store.root/'vault.key').unlink()
    with pytest.raises(github.GitHubSupportError,match='key is missing'):
        service.store.read(OWNER)
    assert not (service.store.root/'vault.key').exists()


def test_authenticated_api_review_and_publish_owner_binding(service,monkeypatch):
    """Only the authenticated owner can inspect/approve an immutable prepared payload."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from api import support_debug as api
    from api.auth import require_auth
    remote=AsyncMock(return_value={'number':200})
    monkeypatch.setattr(github,'github_request',remote)
    monkeypatch.setattr(github,'get_support_github_service',lambda:service)
    chat=SimpleNamespace(active_tasks={},accepting_turns=True)
    monkeypatch.setattr(api,'get_ai_chat_service',lambda:chat)
    monkeypatch.setattr(api,'owner_key',lambda user:OWNER if user=='first' else OTHER)
    app=FastAPI();app.include_router(api.router,prefix='/api/debug')
    client=TestClient(app)
    assert client.get('/api/debug/github').status_code==401
    active={'user':'first'}
    app.dependency_overrides[require_auth]=lambda:SimpleNamespace(user_id=active['user'])
    assert client.get('/api/debug/github').json()=={'configured':True,'login':'fixture-user'}
    response=client.post('/api/debug/github/review',json={'title':'Title','body':'Body'})
    assert response.status_code==200 and response.headers['cache-control']=='no-store'
    review=response.json();identifier=review['review_id']
    assert TOKEN not in response.text and 'assets' not in review
    active['user']='other'
    assert client.get('/api/debug/github/reviews/'+identifier).status_code==404
    assert client.post('/api/debug/github/publish',json={'review_id':identifier}).status_code==404
    remote.assert_not_called()
    active['user']='first'
    assert client.post('/api/debug/github/publish',json={'review_id':identifier,'body':'tampered'}).status_code==422
    assert client.post('/api/debug/github/review',json={'title':'Title','body':'Body','public_images':[True]}).status_code==422
    result=client.post('/api/debug/github/publish',json={'review_id':identifier})
    assert result.status_code==200 and result.json()['issue_url'].endswith('/200')
    assert client.post('/api/debug/github/publish',json={'review_id':identifier}).status_code==200
    assert remote.await_count==1
    assert not chat.active_tasks


def test_owned_public_image_preview_requires_auth_and_returns_exact_bytes(service,monkeypatch):
    """Review images remain authenticated local previews until explicit publication."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from api import support_debug as api
    from api.auth import require_auth
    monkeypatch.setattr(github,'get_support_github_service',lambda:service)
    monkeypatch.setattr(api,'owner_key',lambda user:OWNER if user=='first' else OTHER)
    review=asyncio.run(service.prepare(OWNER,'Title','Body',report(),[0]))
    app=FastAPI();app.include_router(api.router,prefix='/api/debug')
    client=TestClient(app)
    url='/api/debug/github/reviews/'+review['review_id']+'/images/0'
    assert client.get(url).status_code==401
    active={'user':'first'}
    app.dependency_overrides[require_auth]=lambda:SimpleNamespace(user_id=active['user'])
    response=client.get(url)
    assert response.content==b'\x89PNG\r\n\x1a\nimage' and response.headers['cache-control']=='no-store'
    assert response.headers['content-type']=='image/png'
    active['user']='other'
    assert client.get(url).status_code==404


def test_disconnect_invalidates_inflight_token_verification(service,monkeypatch):
    """An older network response cannot resurrect credentials after an explicit disconnect."""
    async def run():
        started=asyncio.Event();release=asyncio.Event()
        async def verify(*args,**kwargs):
            """Hold an account verification until after the local disconnect."""
            started.set();await release.wait();return {'login':'new-user'}
        monkeypatch.setattr(github,'github_request',verify)
        task=asyncio.create_task(service.connect(OWNER,TOKEN))
        await started.wait()
        service.store.disconnect(OWNER)
        release.set()
        with pytest.raises(github.GitHubSupportError,match='replaced or disconnected'):
            await task
        assert await service.status(OWNER)=={'configured':False,'login':''}
        assert 'token' not in service.store.read(OWNER)
    asyncio.run(run())


def test_browser_disconnect_does_not_cancel_approved_owned_publication(monkeypatch):
    """The API owns approved work after its HTTP caller disconnects, then releases it."""
    async def run():
        started=asyncio.Event();release=asyncio.Event()
        async def publish(*args):
            """Hold synthetic publication without any remote mutations."""
            started.set();await release.wait();return {'status':'published'}
        service=SimpleNamespace(publish=publish)
        monkeypatch.setattr(github,'get_support_github_service',lambda:service)
        chat=SimpleNamespace(active_tasks={},accepting_turns=True)
        caller=asyncio.create_task(github.publish_owned(chat,OWNER,'c'*32))
        await started.wait()
        caller.cancel()
        with pytest.raises(asyncio.CancelledError):
            await caller
        assert len(chat.active_tasks)==1
        worker=next(iter(chat.active_tasks.values()))
        release.set();assert await worker=={'status':'published'}
        assert not chat.active_tasks
    asyncio.run(run())


@pytest.mark.parametrize('identifier',['account','../account','x'*32,'a'*31])
def test_review_operations_cannot_address_account_credentials(service,identifier):
    """A reserved credential-record name is never a review resource."""
    for operation in [service.store.review,service.store.discard,service.store.claim]:
        with pytest.raises(github.GitHubSupportError):
            operation(OWNER,identifier)
    assert service.store.read(OWNER)['token']==TOKEN


@pytest.mark.parametrize('scopes,available,expected',[
    (['public_repo'],True,'verified'),(['repo'],True,'verified'),([],True,'missing'),
    (['read:user'],True,'missing'),(None,True,'unknown'),(['public_repo'],False,'verified')])
def test_permission_check_uses_get_only_and_never_claims_unknown_scopes(service,monkeypatch,scopes,available,expected):
    """Authentication, token scopes and public issue target are checked without any write."""
    before=service.store.read(OWNER)
    remote=AsyncMock(side_effect=[({'login':'fixture-user'},scopes),
        {'full_name':github.TARGET,'private':False,'has_issues':available,'archived':False}])
    monkeypatch.setattr(github,'github_request',remote)
    result=asyncio.run(service.check_permissions(OWNER))
    assert result['permissions']==expected
    assert result['ready']==(expected=='verified' and available)
    assert result['write_test_performed'] is False
    assert all(call.args[2]=='GET' for call in remote.call_args_list)
    assert remote.call_args_list[0].kwargs=={'with_scopes':True}
    assert [call.args[3] for call in remote.call_args_list]==['/user','/repos/'+github.TARGET]
    assert TOKEN not in json.dumps(result) and service.store.read(OWNER)==before


def test_permission_check_rejects_unconnected_owner_without_network(service,monkeypatch):
    """Another PBGui owner cannot borrow the saved GitHub token."""
    remote=AsyncMock();monkeypatch.setattr(github,'github_request',remote)
    with pytest.raises(github.GitHubSupportError,match='Connect'):
        asyncio.run(service.check_permissions(OTHER))
    remote.assert_not_called()


def test_permission_check_rejects_revoked_token_and_changed_connection(service,monkeypatch):
    """An expired token or a concurrently disconnected account cannot yield a successful check."""
    remote=AsyncMock(side_effect=github.GitHubSupportError('GitHub rejected the operation (HTTP 401).',401))
    monkeypatch.setattr(github,'github_request',remote)
    with pytest.raises(github.GitHubSupportError,match='401'):
        asyncio.run(service.check_permissions(OWNER))

    async def changed(session,token,method,path,**kwargs):
        """Disconnect this synthetic account while the remote read is in flight."""
        if path=='/user':return {'login':'fixture-user'},['public_repo']
        service.store.disconnect(OWNER)
        return {'full_name':github.TARGET,'private':False,'has_issues':True}
    monkeypatch.setattr(github,'github_request',changed)
    with pytest.raises(github.GitHubSupportError,match='changed during'):
        asyncio.run(service.check_permissions(OWNER))


@pytest.mark.parametrize('with_scopes',[False,True])
def test_github_transport_exposes_only_requested_scope_metadata(with_scopes):
    """Optional metadata parsing preserves existing payloads and never returns auth headers."""
    class Content:
        """Stream a synthetic GitHub JSON response."""
        async def iter_chunked(self,size):
            """Yield a single bounded response."""
            yield b'{"login":"fixture-user"}'
    class Response:
        """Provide a minimal async HTTP response and deliberately unrelated header."""
        status=200
        headers={'X-OAuth-Scopes':'public_repo, read:user','Authorization':'DO_NOT_EXPOSE'}
        content=Content()
        async def __aenter__(self):
            """Enter the request context."""
            return self
        async def __aexit__(self,*args):
            """Release the request context."""
    class Session:
        """Keep all traffic in memory and inspect the outgoing auth boundary."""
        def request(self,method,url,**kwargs):
            """Require headers-only credentials and official fixed URL."""
            assert method=='GET' and url=='https://api.github.com/user'
            assert kwargs['headers']['Authorization']=='Bearer '+TOKEN
            assert kwargs['allow_redirects'] is False
            return Response()
    result=asyncio.run(github.github_request(Session(),TOKEN,'GET','/user',with_scopes=with_scopes))
    assert result==({'login':'fixture-user'},['public_repo','read:user']) if with_scopes else result=={'login':'fixture-user'}
    assert 'DO_NOT_EXPOSE' not in str(result) and TOKEN not in str(result)


def test_permission_check_api_requires_auth_and_uses_only_session_owner(service,monkeypatch):
    """The check endpoint cannot borrow another user's connection or return credentials."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from api import support_debug as api
    from api.auth import require_auth
    monkeypatch.setattr(github,'get_support_github_service',lambda:service)
    monkeypatch.setattr(api,'owner_key',lambda user:OWNER if user=='first' else OTHER)
    remote=AsyncMock(side_effect=[({'login':'fixture-user'},['public_repo']),{'full_name':github.TARGET,'private':False,'has_issues':True}])
    monkeypatch.setattr(github,'github_request',remote)
    app=FastAPI();app.include_router(api.router,prefix='/api/debug')
    with TestClient(app) as client:
        assert client.post('/api/debug/github/check-permissions').status_code==401
        active={'user':'other'}
        app.dependency_overrides[require_auth]=lambda:SimpleNamespace(user_id=active['user'])
        assert client.post('/api/debug/github/check-permissions',json={'owner':OWNER}).status_code==409
        remote.assert_not_called()
        active['user']='first'
        response=client.post('/api/debug/github/check-permissions')
        assert response.status_code==200 and response.json()['ready'] is True
        assert response.headers['cache-control']=='no-store' and TOKEN not in response.text
