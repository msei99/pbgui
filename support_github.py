"""Owner-scoped official GitHub reporting with encrypted local credentials and durable receipts."""
from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import re
import time
import uuid
from pathlib import Path

import aiohttp
from cryptography.fernet import Fernet, InvalidToken

from file_lock import advisory_file_lock
from logging_helpers import human_log as _log
from secure_files import atomic_write_private_bytes, ensure_private_directory, read_regular_file_nofollow
from support_debug_crypto import decoded_screenshots, encrypt_report

SERVICE = 'SupportDebug'
TARGET = 'msei99/pbgui'
ASSET_REPO = 'pbgui-support-assets'
REVIEW_TTL = 1800
ROOT = Path(__file__).resolve().parent/'data'/'support'/'github'
ID = re.compile(r'^[a-f0-9]{32}$')
LOGIN = re.compile(r'^[A-Za-z0-9](?:[A-Za-z0-9-]{0,37}[A-Za-z0-9])?$')


def process_alive(pid):
    """Check a persisted publisher PID without sending an operational signal."""
    if type(pid) is not int or pid<=0:
        return False
    try:
        os.kill(pid,0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


class GitHubSupportError(RuntimeError):
    """A safe error that never contains credentials or an upstream response body."""
    def __init__(self, message, status=400):
        """Carry the user-facing status separately from untrusted remote content."""
        super().__init__(message)
        self.status=status


class SupportGitHubStore:
    """Encrypt account credentials, review payloads and receipts at rest with owner-only files."""
    def __init__(self, root=None):
        """Delay all filesystem access until an authenticated operation; allow isolated tests."""
        self.root=Path(root) if root is not None else ROOT

    def path(self, owner, identifier='account'):
        """Validate identifiers before constructing any persisted filename."""
        if not ID.fullmatch(owner) or (identifier!='account' and not ID.fullmatch(identifier)):
            raise GitHubSupportError('Invalid support record identifier.')
        return self.root/f'{owner}-{identifier}.vault'

    def cipher(self):
        """Load/create the vault key under the same cross-process transaction lock."""
        ensure_private_directory(self.root)
        path=self.root/'vault.key'
        if not path.exists():
            if any(self.root.glob('*.vault')):
                raise GitHubSupportError('Support vault key is missing. Restore its private backup before reconnecting.',500)
            atomic_write_private_bytes(path,Fernet.generate_key())
        return Fernet(read_regular_file_nofollow(path,self.root))

    def read_unlocked(self, path):
        """Read only a bounded encrypted regular record beneath the private vault root."""
        if not path.exists() and not path.is_symlink():
            return None
        if path.stat().st_size>48*1024*1024:
            raise GitHubSupportError('Support vault record exceeds its size limit.',500)
        try:
            result=json.loads(self.cipher().decrypt(read_regular_file_nofollow(path,self.root)))
        except GitHubSupportError:
            raise
        except (InvalidToken,ValueError,OSError,RuntimeError) as exc:
            raise GitHubSupportError('Support vault cannot be read.',500) from exc
        if not isinstance(result,dict):
            raise GitHubSupportError('Support vault record is invalid.',500)
        return result

    def write_unlocked(self, path, value):
        """Atomically encrypt a record while its caller holds the vault lock."""
        raw=json.dumps(value,ensure_ascii=False,indent=4).encode()
        if len(raw)>32*1024*1024:
            raise GitHubSupportError('Support payload exceeds its storage budget.',413)
        atomic_write_private_bytes(path,self.cipher().encrypt(raw))

    def read(self, owner, identifier='account'):
        """Read one authenticated owner's record under a reentrant process lock."""
        path=self.path(owner,identifier)
        if not self.root.exists() and not self.root.is_symlink():
            return None
        ensure_private_directory(self.root)
        with advisory_file_lock(self.root/'.write'):
            row=self.read_unlocked(path)
            if row and identifier!='account' and row.get('status')=='publishing' and not process_alive(row.get('pid')):
                row.update(status='uncertain' if row.get('phase')=='issue' else 'interrupted',error='The previous API process stopped. Check or resume this reviewed operation.')
                self.write_unlocked(path,row)
            return row

    def write(self, owner, value, identifier='account'):
        """Persist one record without lost concurrent updates to key creation."""
        path=self.path(owner,identifier)
        ensure_private_directory(self.root)
        with advisory_file_lock(self.root/'.write'):
            self.write_unlocked(path,value)

    def review(self, owner, identifier):
        """Review endpoints cannot address the separate account credential record."""
        if not isinstance(identifier,str) or not ID.fullmatch(identifier):
            raise GitHubSupportError('Invalid publication review identifier.',404)
        return self.read(owner,identifier)

    def disconnect(self, owner):
        """Remove only this account credential; retain publication receipts for recovery."""
        path=self.path(owner)
        if self.root.exists():
            ensure_private_directory(self.root)
            with advisory_file_lock(self.root/'.write'):
                self.write_unlocked(path,{'login':'','pending_connection':uuid.uuid4().hex})

    def begin_connection(self, owner):
        """Give a replacement connection a generation before its network verification."""
        ensure_private_directory(self.root)
        with advisory_file_lock(self.root/'.write'):
            path=self.path(owner); row=self.read_unlocked(path) or {}
            identifier=uuid.uuid4().hex; row['pending_connection']=identifier
            self.write_unlocked(path,row)
            return identifier

    def finish_connection(self, owner, identifier, login, token):
        """A disconnect or newer connection invalidates any older in-flight verification."""
        with advisory_file_lock(self.root/'.write'):
            path=self.path(owner); row=self.read_unlocked(path) or {}
            if row.get('pending_connection')!=identifier:
                raise GitHubSupportError('This GitHub connection was replaced or disconnected. Connect again if needed.',409)
            self.write_unlocked(path,{'login':login,'token':token})

    def discard(self, owner, identifier):
        """Remove a local inactive review only; uncertain remote writes retain their receipt."""
        if not isinstance(identifier,str) or not ID.fullmatch(identifier):
            raise GitHubSupportError('Invalid publication review identifier.',404)
        path=self.path(owner,identifier)
        if not self.root.exists():
            return
        ensure_private_directory(self.root)
        with advisory_file_lock(self.root/'.write'):
            row=self.read_unlocked(path)
            if row and (row.get('status') in {'publishing','published','uncertain'} or row.get('phase')=='issue'):
                raise GitHubSupportError('This publication receipt must be retained. Check its GitHub status instead.',409)
            path.unlink(missing_ok=True)

    def prepare(self, owner, record):
        """Bound retained large reviews; never remove an in-flight publication."""
        ensure_private_directory(self.root)
        with advisory_file_lock(self.root/'.write'):
            active=[]; completed=[]
            for path in self.root.glob('*.vault'):
                if path.name.endswith('-account.vault'):
                    continue
                row=self.read_unlocked(path)
                if not row:
                    continue
                if row.get('status')=='published' or row.get('phase')=='issue' and row.get('status')=='uncertain':
                    completed.append((row.get('created_at',0),path))
                elif time.time()-row.get('created_at',0)>REVIEW_TTL and row.get('status')!='publishing':
                    path.unlink(missing_ok=True)
                else:
                    active.append(row)
            for _,path in sorted(completed)[:-128]:
                path.unlink(missing_ok=True)
            if len(active)>=4:
                raise GitHubSupportError('Support review capacity is full. Finish or discard a previous review.',429)
            self.write_unlocked(self.path(owner,record['review_id']),record)

    def claim(self, owner, identifier):
        """Atomically claim a review; a dead API process can be recovered conservatively."""
        if not isinstance(identifier,str) or not ID.fullmatch(identifier):
            raise GitHubSupportError('Invalid publication review identifier.',404)
        with advisory_file_lock(self.root/'.write'):
            path=self.path(owner,identifier)
            row=self.read_unlocked(path)
            if not row:
                raise GitHubSupportError('Review expired or unavailable. Prepare a new review.',404)
            if row.get('status')=='published':
                return row
            if time.time()-row.get('created_at',0)>REVIEW_TTL and row.get('phase')!='issue':
                raise GitHubSupportError('Review expired. Prepare a new review.',409)
            if row.get('status')=='publishing':
                if process_alive(row.get('pid')):
                    raise GitHubSupportError('This reviewed issue is already being published.',409)
            row.update(status='publishing',pid=os.getpid(),error='')
            self.write_unlocked(path,row)
            return row


def safe_status(row):
    """Return public review metadata without tokens, binary assets or diagnostic payloads."""
    return {key:row.get(key) for key in ('review_id','created_at','status','phase','title','body','login','target','assets_repository','public_image_count','encrypted_attachment','issue_url','error')}


async def github_request(session, token, method, path, *, payload=None, binary=None, media_type=None, allow_404=False, upload=False, with_scopes=False):
    """Use fixed official hosts, bounded responses and headers-only authentication; no redirects."""
    if not path.startswith('/') or any(char in path for char in ('\r','\n','\\')):
        raise GitHubSupportError('Invalid GitHub API operation.')
    base='https://uploads.github.com' if upload else 'https://api.github.com'
    headers={'Authorization':'Bearer '+token,'Accept':'application/vnd.github+json','X-GitHub-Api-Version':'2022-11-28','User-Agent':'PBGui-Support'}
    if binary is not None:
        headers['Content-Type']=media_type or 'application/octet-stream'
    try:
        async with session.request(method,base+path,headers=headers,json=payload if binary is None else None,data=binary,allow_redirects=False) as response:
            if allow_404 and response.status==404:
                return None
            if not 200<=response.status<300:
                raise GitHubSupportError(f'GitHub rejected the operation (HTTP {response.status}). Check account permissions and token expiry.',response.status)
            if response.status==204:
                return {}
            raw=bytearray()
            async for chunk in response.content.iter_chunked(65536):
                raw.extend(chunk)
                if len(raw)>2*1024*1024:
                    raise GitHubSupportError('GitHub response exceeded its size limit.',502)
            data=json.loads(raw)
            if with_scopes:
                header=response.headers.get('X-OAuth-Scopes')
                scopes=None if header is None else [scope.strip() for scope in header[:4000].split(',') if re.fullmatch(r'[a-z_:]+',scope.strip())][:100]
                return data,scopes
            return data
    except GitHubSupportError:
        raise
    except (aiohttp.ClientError,asyncio.TimeoutError,ValueError) as exc:
        raise GitHubSupportError('GitHub response was not confirmed. No automatic write retry was performed.',502) from exc


class SupportGitHubService:
    """Prepare immutable reviews and publish only an owner-approved review ID."""
    def __init__(self, store=None):
        """Keep clients request-scoped; durable encrypted receipts own publication state."""
        self.store=store or SupportGitHubStore()

    async def connect(self, owner, token):
        """Verify the personal account read-only before securely saving its token."""
        if not isinstance(token,str) or not re.fullmatch(r'[A-Za-z0-9_]{20,255}',token):
            raise GitHubSupportError('Enter a valid GitHub personal access token.')
        identifier=await asyncio.to_thread(self.store.begin_connection,owner)
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30),trust_env=False,cookie_jar=aiohttp.DummyCookieJar()) as session:
            user=await github_request(session,token,'GET','/user')
        login=user.get('login','') if isinstance(user,dict) else ''
        if not LOGIN.fullmatch(login):
            raise GitHubSupportError('GitHub did not return a valid account.')
        await asyncio.to_thread(self.store.finish_connection,owner,identifier,login,token)
        return {'configured':True,'login':login}

    async def status(self, owner):
        """Expose only safe connection metadata, never token fragments."""
        row=await asyncio.to_thread(self.store.read,owner)
        return {'configured':bool(row and row.get('token')),'login':(row or {}).get('login','')}

    async def check_permissions(self, owner):
        """Verify the saved account, classic token scope and issue target using GET only."""
        account=await asyncio.to_thread(self.store.read,owner)
        if not account or not account.get('token'):
            raise GitHubSupportError('Connect your GitHub account first.',409)
        async with asyncio.timeout(45):
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30,connect=15),trust_env=False,cookie_jar=aiohttp.DummyCookieJar()) as session:
                user,scopes=await github_request(session,account['token'],'GET','/user',with_scopes=True)
                if not isinstance(user,dict) or user.get('login')!=account.get('login'):
                    raise GitHubSupportError('The token account does not match the saved connection. Reconnect GitHub.',409)
                target=await github_request(session,account['token'],'GET','/repos/'+TARGET)
        current=await asyncio.to_thread(self.store.read,owner)
        if current!=account:
            raise GitHubSupportError('The GitHub connection changed during the check. Check again.',409)
        permission='unknown' if scopes is None else 'verified' if {'public_repo','repo'}.intersection(scopes) else 'missing'
        target_available=isinstance(target,dict) and target.get('full_name')==TARGET and not target.get('private',True) and target.get('has_issues',False) and not target.get('archived',False) and not target.get('disabled',False)
        return {'login':account['login'],'authenticated':True,'permissions':permission,'required_scope':'public_repo',
                'issue_target_available':bool(target_available),'ready':permission=='verified' and bool(target_available),
                'checked_at':time.time(),'write_test_performed':False}

    async def prepare(self, owner, title, body, report=None, public_images=None):
        """Encrypt selected diagnostics and construct exact public links before approval."""
        account=await asyncio.to_thread(self.store.read,owner)
        if not account or not account.get('token'):
            raise GitHubSupportError('Connect your GitHub account first.',409)
        if not title.strip() or len(title)>120 or not body.strip() or len(body)>50000:
            raise GitHubSupportError('Enter an issue title and a public description within the size limits.')
        identifier=uuid.uuid4().hex; login=account['login']; tag='report-'+identifier
        assets=[]; public_images=public_images or []
        if len(public_images)>5 or len(set(public_images))!=len(public_images) or any(type(index) is not int or index<0 or index>4 for index in public_images):
            raise GitHubSupportError('Invalid public screenshot selection.')
        if report is not None:
            raw=json.dumps(report,ensure_ascii=False)
            archive=await asyncio.to_thread(encrypt_report,raw)
            assets.append({'name':'debug-report.zip','media_type':'application/zip','data_base64':base64.b64encode(archive).decode()})
            images=decoded_screenshots(report)
            for index in public_images:
                if index>=len(images):
                    raise GitHubSupportError('A selected public screenshot is unavailable.')
                suffix,data=images[index]
                assets.append({'name':f'screenshot-{index+1}{suffix}','media_type':{'png':'image/png','jpg':'image/jpeg','webp':'image/webp'}[suffix[1:]],'data_base64':base64.b64encode(data).decode()})
        elif public_images:
            raise GitHubSupportError('Selected screenshots must be included in the reviewed attachment.')
        base=f'https://github.com/{login}/{ASSET_REPO}/releases/download/{tag}/'
        public_body=body.rstrip()
        if assets:
            public_body+='\n\n### Support attachments\n[Encrypted debug report]('+base+'debug-report.zip) — only PBGui support can decrypt it.'
            for asset in assets[1:]:
                public_body+='\n\n![Screenshot]('+base+asset['name']+')'
        public_body+=f'\n\n<!-- pbgui-support:{identifier} -->'
        if len(public_body)>60000:
            raise GitHubSupportError('The reviewed issue body is too long.')
        row=dict(review_id=identifier,created_at=time.time(),status='prepared',phase='prepared',title=title.strip(),body=public_body,
                 login=login,target=TARGET,assets_repository=f'{login}/{ASSET_REPO}' if assets else None,tag=tag,assets=assets,
                 public_image_count=len(public_images),encrypted_attachment=bool(assets),issue_url=None,error='')
        await asyncio.to_thread(self.store.prepare,owner,row)
        return safe_status(row)

    async def publish(self, owner, identifier):
        """Resume idempotent asset stages and never blindly repeat uncertain issue creation."""
        row=await asyncio.to_thread(self.store.claim,owner,identifier)
        if row['status']=='published':
            return safe_status(row)
        try:
            account=await asyncio.to_thread(self.store.read,owner)
            if not account or account.get('login')!=row['login']:
                raise GitHubSupportError('The connected GitHub account changed. Prepare a new review.',409)
            token=account['token']
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60,connect=15),trust_env=False,cookie_jar=aiohttp.DummyCookieJar()) as session:
                if row['phase']=='issue':
                    found=await self.find_issue(session,token,row)
                    if not found:
                        raise GitHubSupportError('Issue creation remains unconfirmed. Check GitHub before creating another issue; retry only checks for the existing issue.',409)
                    row['issue_url']=found
                else:
                    if row['assets']:
                        await self.upload_assets(session,token,owner,row)
                    row['phase']='issue'
                    await asyncio.to_thread(self.store.write,owner,row,identifier)
                    try:
                        result=await github_request(session,token,'POST',f'/repos/{TARGET}/issues',payload={'title':row['title'],'body':row['body']})
                    except GitHubSupportError as exc:
                        if 400<=exc.status<500:
                            row['phase']='assets'
                        raise
                    number=result.get('number') if isinstance(result,dict) else None
                    if type(number) is not int or number<=0:
                        raise GitHubSupportError('GitHub did not confirm the created issue.',502)
                    row['issue_url']=f'https://github.com/{TARGET}/issues/{number}'
            row.update(status='published',assets=[],error='')
        except BaseException as exc:
            row.update(status='uncertain' if row['phase']=='issue' else 'interrupted',error=str(exc) if isinstance(exc,GitHubSupportError) else 'Publication interrupted. The reviewed operation can be checked again.')
            if row['phase']=='issue':
                row['assets']=[]
            await asyncio.shield(asyncio.to_thread(self.store.write,owner,row,identifier))
            if isinstance(exc,(asyncio.CancelledError,GitHubSupportError)):
                raise
            raise GitHubSupportError('GitHub publication failed. Check the retained review status before retrying.',502) from exc
        await asyncio.to_thread(self.store.write,owner,row,identifier)
        return safe_status(row)

    async def find_issue(self, session, token, row):
        """Recover an ambiguous POST by its unique public marker, without posting a duplicate."""
        marker='<!-- pbgui-support:'+row['review_id']+' -->'
        for page in range(1,11):
            items=await github_request(session,token,'GET',f'/repos/{TARGET}/issues?state=all&creator={row["login"]}&per_page=100&page={page}')
            if not isinstance(items,list):
                raise GitHubSupportError('Cannot reconcile the previous issue creation.',502)
            for item in items:
                number=item.get('number')
                if marker in str(item.get('body') or '') and type(number) is int and number>0:
                    return f'https://github.com/{TARGET}/issues/{number}'
            if len(items)<100:
                return None
        raise GitHubSupportError('Issue reconciliation exceeded its page limit. Check GitHub manually.',409)

    async def upload_assets(self, session, token, owner, row):
        """Use a dedicated public user repository and release assets rather than bloating Git history."""
        repository=row['assets_repository']
        repo=await github_request(session,token,'GET',f'/repos/{repository}',allow_404=True)
        if repo is None:
            repo=await github_request(session,token,'POST','/user/repos',payload={'name':ASSET_REPO,'description':'Encrypted PBGui support bundles and explicitly public screenshots','private':False,'auto_init':True})
        if not isinstance(repo,dict) or repo.get('private') is not False or str(repo.get('full_name','')).lower()!=repository.lower():
            raise GitHubSupportError('The support asset repository must be public and owned by your connected account. No repository visibility was changed.',409)
        release=await github_request(session,token,'GET',f'/repos/{repository}/releases/tags/{row["tag"]}',allow_404=True)
        if release is None:
            release=await github_request(session,token,'POST',f'/repos/{repository}/releases',payload={'tag_name':row['tag'],'name':'PBGui support report','body':'Diagnostic ZIP is encrypted for PBGui support. Any screenshots here were explicitly approved for public sharing.','draft':False,'prerelease':False})
        release_id=release.get('id') if isinstance(release,dict) else None
        if type(release_id) is not int or release_id<=0:
            raise GitHubSupportError('GitHub returned an invalid release identifier.',502)
        existing=await github_request(session,token,'GET',f'/repos/{repository}/releases/{release_id}/assets?per_page=100')
        if not isinstance(existing,list):
            raise GitHubSupportError('GitHub returned invalid asset metadata.',502)
        row['phase']='assets'
        await asyncio.to_thread(self.store.write,owner,row,row['review_id'])
        for asset in row['assets']:
            data=base64.b64decode(asset['data_base64'],validate=True)
            found=next((item for item in existing if item.get('name')==asset['name']),None)
            if found:
                digest=found.get('digest')
                if found.get('size')!=len(data) or (digest and digest!='sha256:'+hashlib.sha256(data).hexdigest()):
                    raise GitHubSupportError('An existing support asset does not match the reviewed file; it was not overwritten.',409)
                continue
            await github_request(session,token,'POST',f'/repos/{repository}/releases/{release_id}/assets?name={asset["name"]}',binary=data,media_type=asset['media_type'],upload=True)


_SERVICE=None


def get_support_github_service():
    """Return the local support publication service without opening files at import time."""
    global _SERVICE
    if _SERVICE is None:
        _SERVICE=SupportGitHubService()
    return _SERVICE


async def publish_owned(chat, owner, identifier):
    """Keep approved publication alive across browser disconnects; AI shutdown owns cleanup."""
    key='support-github-publish:'+owner
    if not chat.accepting_turns or key in chat.active_tasks:
        raise GitHubSupportError('A publication is already running or the API is shutting down.',409)
    async def operation():
        """Retain a durable receipt on interruption and always release task registration."""
        try:
            async with asyncio.timeout(300):
                return await get_support_github_service().publish(owner,identifier)
        finally:
            chat.active_tasks.pop(key,None)
    task=asyncio.create_task(operation(),name=key)
    chat.active_tasks[key]=task
    def consume_result(done):
        """Observe failures even if the originating browser closes after explicit approval."""
        if not done.cancelled() and done.exception() is not None:
            _log(SERVICE,'Reviewed GitHub publication did not complete; durable receipt retained.',level='WARNING')
    task.add_done_callback(consume_result)
    return await asyncio.shield(task)
