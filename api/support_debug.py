"""Authenticated universal support diagnostics, without jobs or remote actions."""
from pathlib import Path
import threading
import traceback
import time
import uuid

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.responses import Response
from starlette.concurrency import run_in_threadpool
from pydantic import BaseModel, ConfigDict, Field
from pydantic import ValidationError, StrictInt

from api.auth import SessionToken, require_auth
from api.page_templates import render_page_urls, script_json
from ai_chat import AIChatError, get_ai_chat_service, owner_key
from support_issue_ai import IssueDraftRequest, draft_for_request
from logging_helpers import human_log as _log
from pbgui_purefunc import PBGDIR, pb7dir, pb8dir
from support_debug import collect, text
from support_debug_crypto import MAX_REPORT_BYTES, encrypt_report

SERVICE = 'SupportDebug'
router = APIRouter()
_slots = threading.BoundedSemaphore(2)
_context_lock = threading.RLock()
_error_contexts = {}
CONTEXT_TTL = 600
MAX_CONTEXTS = 128


@router.post('/issue-draft')
async def issue_draft(request: Request, session: SessionToken = Depends(require_auth)):
    """Draft English issue text only after an explicit authenticated user request."""
    try:
        chunks, size = [], 0
        async for chunk in request.stream():
            size += len(chunk)
            if size > MAX_REPORT_BYTES:
                raise HTTPException(413, 'Issue draft exceeds the 12 MiB input limit')
            chunks.append(chunk)
        try:
            body = IssueDraftRequest.model_validate_json(b''.join(chunks))
        except ValidationError as exc:
            raise HTTPException(422, 'Invalid issue text or screenshots', headers={'Cache-Control':'no-store'}) from exc
        result = await draft_for_request(get_ai_chat_service(), owner_key(session.user_id), body, request)
        return JSONResponse(result, headers={'Cache-Control':'no-store'})
    except AIChatError as exc:
        _log(SERVICE, f'Issue drafting failed: {type(exc).__name__}', level='WARNING')
        detail = ('The selected model could not process the screenshots. Choose an image-capable model in AI Chat or deselect the images. Your text was kept.'
                  if body.screenshots else str(exc))
        raise HTTPException(400, detail, headers={'Cache-Control':'no-store'}) from exc
    except HTTPException:
        raise
    except Exception as exc:
        _log(SERVICE, f'Issue drafting failed: {type(exc).__name__}', level='ERROR')
        raise HTTPException(500, 'AI issue drafting failed. Your text was kept.', headers={'Cache-Control':'no-store'}) from exc


class ErrorContextRequest(BaseModel):
    """Explicitly submitted local UI error; never transported in browser URLs/storage."""
    model_config = ConfigDict(extra='forbid')
    area: str = Field(default='unknown', pattern=r'^[a-zA-Z0-9_-]{1,80}$')
    message: str = Field(default='', max_length=4000)
    detail: str = Field(default='', max_length=8000)
    loop_id: str | None = Field(default=None, pattern=r'^[a-f0-9]{32}$')


def _prune_contexts(reserve=False):
    """Expire and bound this process-owned, short-lived handoff cache under its lock."""
    now = time.monotonic()
    for key, (created, _, _) in list(_error_contexts.items()):
        if now - created >= CONTEXT_TTL:
            _error_contexts.pop(key, None)
    while len(_error_contexts) > MAX_CONTEXTS - int(reserve):
        _error_contexts.pop(next(iter(_error_contexts)))


def _get_context(identifier, owner):
    """Only the originating authenticated owner may consume an error context."""
    with _context_lock:
        _prune_contexts()
        item = _error_contexts.get(identifier)
        if item is None or item[1] != owner:
            raise HTTPException(404, 'Error context expired or unavailable. Report the error again.')
        return dict(item[2])


@router.post('/error-context')
def create_error_context(body: ErrorContextRequest, session: SessionToken = Depends(require_auth)):
    """Keep redacted error evidence for ten minutes; return only an opaque identifier."""
    identifier = uuid.uuid4().hex
    context = {'area': body.area, 'message': text(body.message), 'detail': text(body.detail),
               'loop_id': body.loop_id, 'reported_at': time.time()}
    with _context_lock:
        _prune_contexts(reserve=True)
        _error_contexts[identifier] = (time.monotonic(), owner_key(session.user_id), context)
    return JSONResponse({'context_id': identifier}, headers={'Cache-Control': 'no-store'})


class DebugRequest(BaseModel):
    """Optional owner-bound context; collection always covers all subsystems."""
    model_config = ConfigDict(extra='forbid')
    loop_id: str | None = Field(default=None, pattern=r'^[a-f0-9]{32}$')
    context_id: str | None = Field(default=None, pattern=r'^[a-f0-9]{32}$')


@router.get('/main_page', response_class=HTMLResponse)
def main_page(request: Request, session: SessionToken = Depends(require_auth)):
    """Render the shared-shell support page using mounted same-origin URLs."""
    root = Path(__file__).resolve().parent.parent
    html = (root / 'frontend/support_debug.html').read_text()
    from pbgui_purefunc import PBGUI_VERSION, PBGUI_SERIAL
    html = html.replace('"%%VERSION%%"', script_json(PBGUI_VERSION))
    html = html.replace('"%%SERIAL%%"', script_json(PBGUI_SERIAL))
    html = render_page_urls(request, html, '/api/debug')
    html = html.replace('%%NAV_HASH%%', str((root / 'frontend/pbgui_nav.js').stat().st_mtime_ns))
    return HTMLResponse(html, headers={'Cache-Control': 'no-store'})


@router.post('/collect')
def collect_debug(body: DebugRequest, request: Request, session: SessionToken = Depends(require_auth)):
    """Collect in FastAPI's worker thread; do not persist support files on the server."""
    if not _slots.acquire(blocking=False):
        raise HTTPException(429, 'Another debug collection is running. Try again shortly.')
    try:
        context = _get_context(body.context_id, owner_key(session.user_id)) if body.context_id else None
        roots = {kind: value for kind, value in [('pb7', pb7dir()), ('pb8', pb8dir())] if value}
        result = collect(PBGDIR, owner_key(session.user_id),
                         [route.path for route in request.app.routes if hasattr(route, 'path')],
                         loop_id=body.loop_id or (context or {}).get('loop_id'), runtime_roots=roots)
        if context:
            result['error_context'] = context
        return JSONResponse(result, headers={'Cache-Control': 'no-store'})
    except HTTPException:
        raise
    except FileNotFoundError:
        raise HTTPException(404, 'Loop not found for this user')
    except Exception as exc:
        _log(SERVICE, f'Debug collection failed: {type(exc).__name__}', level='ERROR', meta={'traceback': traceback.format_exc()})
        raise HTTPException(500, 'Debug collection failed; see PBGui service log') from exc
    finally:
        _slots.release()


@router.post('/issue-attachment')
async def issue_attachment(request: Request, session: SessionToken = Depends(require_auth)):
    """Encrypt a bounded, explicitly reviewed snapshot without saving or uploading it."""
    if not _slots.acquire(blocking=False):
        raise HTTPException(429, 'Another debug operation is running. Try again shortly.')
    try:
        chunks, size = [], 0
        async for chunk in request.stream():
            size += len(chunk)
            if size > MAX_REPORT_BYTES:
                raise HTTPException(413, 'Report exceeds the 12 MiB input limit')
            chunks.append(chunk)
        try:
            reviewed = b''.join(chunks).decode('utf-8')
            attachment = await run_in_threadpool(encrypt_report, reviewed)
        except (ValueError, UnicodeError) as exc:
            raise HTTPException(422, 'Invalid or oversized PBGui debug report') from exc
        return Response(attachment, media_type='application/zip', headers={
            'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff',
            'Content-Disposition': 'attachment; filename="pbgui-debug-encrypted.zip"'})
    except HTTPException:
        raise
    except Exception as exc:
        _log(SERVICE, f'Debug attachment encryption failed: {type(exc).__name__}', level='ERROR')
        raise HTTPException(500, 'Cannot encrypt debug attachment; see PBGui service log') from exc
    finally:
        _slots.release()


async def _support_body(request, model, maximum=MAX_REPORT_BYTES):
    """Bound request bytes before parsing; validation never echoes submitted secrets/images."""
    if request.headers.get('content-type','').split(';',1)[0].strip().lower()!='application/json':
        raise HTTPException(415,'Support requests must use application/json')
    raw=bytearray()
    async for chunk in request.stream():
        raw.extend(chunk)
        if len(raw)>maximum:
            raise HTTPException(413,'Support request exceeds its input limit')
    try:
        return model.model_validate_json(raw)
    except ValidationError as exc:
        raise HTTPException(422,'Invalid support request',headers={'Cache-Control':'no-store'}) from exc


@router.post('/diagnose')
async def diagnose_support(request: Request, session: SessionToken = Depends(require_auth)):
    """Investigate consented evidence without running tests, changing files or publishing."""
    from support_diagnosis import DiagnosisRequest, diagnose
    body=await _support_body(request,DiagnosisRequest)
    try:
        async def operation(chat,owner,value):
            """Keep the installed source root server-owned, never supplied by the caller."""
            return await diagnose(chat,owner,value,Path(PBGDIR))
        result=await draft_for_request(get_ai_chat_service(),owner_key(session.user_id),body,request,operation=operation,timeout=300)
        return JSONResponse(result,headers={'Cache-Control':'no-store'})
    except HTTPException:
        raise
    except AIChatError as exc:
        _log(SERVICE,'Read-only support investigation did not complete.',level='WARNING')
        raise HTTPException(400,str(exc),headers={'Cache-Control':'no-store'}) from exc
    except Exception as exc:
        _log(SERVICE,f'Support investigation failed: {type(exc).__name__}',level='ERROR')
        raise HTTPException(500,'Support investigation failed. Your text was kept.',headers={'Cache-Control':'no-store'}) from exc


class GitHubConnectRequest(BaseModel):
    """Submit a personal token once in an authenticated body, never URLs or storage."""
    model_config=ConfigDict(extra='forbid')
    token: str=Field(min_length=20,max_length=255)


class GitHubReviewRequest(BaseModel):
    """Exact public text and selected encrypted snapshot for final publication review."""
    model_config=ConfigDict(extra='forbid')
    title: str=Field(min_length=1,max_length=120)
    body: str=Field(min_length=1,max_length=50000)
    report: dict | None=None
    public_images: list[StrictInt]=Field(default_factory=list,max_length=5)


class GitHubPublishRequest(BaseModel):
    """Publish only an authenticated owner's immutable reviewed payload."""
    model_config=ConfigDict(extra='forbid')
    review_id: str=Field(pattern=r'^[a-f0-9]{32}$')


def _github_error(exc):
    """Log safe operation failures without tokens, binary assets or upstream response bodies."""
    from support_github import GitHubSupportError
    if isinstance(exc,HTTPException):
        return exc
    _log(SERVICE,f'GitHub support operation failed: {type(exc).__name__}',level='WARNING')
    return HTTPException(exc.status if isinstance(exc,GitHubSupportError) else 500,
                         str(exc) if isinstance(exc,GitHubSupportError) else 'GitHub support operation failed.',headers={'Cache-Control':'no-store'})


@router.get('/github')
async def github_status(session: SessionToken = Depends(require_auth)):
    """Return connection metadata only; GitHub credentials are never revealable."""
    from support_github import get_support_github_service
    try:
        return JSONResponse(await get_support_github_service().status(owner_key(session.user_id)),headers={'Cache-Control':'no-store'})
    except Exception as exc:
        raise _github_error(exc) from exc


@router.post('/github/connect')
async def github_connect(request: Request, session: SessionToken = Depends(require_auth)):
    """Verify and save an owner-scoped GitHub connection read-only."""
    from support_github import get_support_github_service
    body=await _support_body(request,GitHubConnectRequest,2048)
    owner=owner_key(session.user_id)
    if 'support-github-publish:'+owner in get_ai_chat_service().active_tasks:
        raise HTTPException(409,'Wait for your current publication to finish.')
    try:
        result=await get_support_github_service().connect(owner,body.token)
        return JSONResponse(result,headers={'Cache-Control':'no-store'})
    except Exception as exc:
        raise _github_error(exc) from exc


@router.post('/github/check-permissions')
async def github_check_permissions(session: SessionToken = Depends(require_auth)):
    """Check the authenticated owner's saved token without creating remote support artifacts."""
    from support_github import get_support_github_service
    try:
        result=await get_support_github_service().check_permissions(owner_key(session.user_id))
        return JSONResponse(result,headers={'Cache-Control':'no-store'})
    except Exception as exc:
        raise _github_error(exc) from exc


@router.delete('/github')
async def github_disconnect(session: SessionToken = Depends(require_auth)):
    """Forget local credentials without deleting published issues, assets or receipts."""
    from support_github import get_support_github_service
    owner=owner_key(session.user_id)
    if 'support-github-publish:'+owner in get_ai_chat_service().active_tasks:
        raise HTTPException(409,'Wait for your current publication to finish.')
    try:
        import asyncio
        await asyncio.to_thread(get_support_github_service().store.disconnect,owner)
        return JSONResponse({'configured':False,'login':''},headers={'Cache-Control':'no-store'})
    except Exception as exc:
        raise _github_error(exc) from exc


@router.post('/github/review')
async def github_review(request: Request, session: SessionToken = Depends(require_auth)):
    """Prepare/encrypt everything before the user's final publish approval; no external writes."""
    from support_github import get_support_github_service
    body=await _support_body(request,GitHubReviewRequest)
    try:
        result=await get_support_github_service().prepare(owner_key(session.user_id),body.title,body.body,body.report,body.public_images)
        return JSONResponse(result,headers={'Cache-Control':'no-store'})
    except (ValueError,UnicodeError):
        raise HTTPException(422,'Invalid or oversized debug attachment') from None
    except Exception as exc:
        raise _github_error(exc) from exc


@router.get('/github/reviews/{review_id}')
async def github_review_status(review_id: str, session: SessionToken = Depends(require_auth)):
    """Restore an owner-bound public review/receipt after browser reload or an interrupted call."""
    from support_github import get_support_github_service,safe_status
    import asyncio
    try:
        row=await asyncio.to_thread(get_support_github_service().store.review,owner_key(session.user_id),review_id)
        if not row:
            raise HTTPException(404,'Review expired or unavailable.')
        return JSONResponse(safe_status(row),headers={'Cache-Control':'no-store'})
    except HTTPException:
        raise
    except Exception as exc:
        raise _github_error(exc) from exc


@router.delete('/github/reviews/{review_id}')
async def github_discard_review(review_id: str, session: SessionToken = Depends(require_auth)):
    """Discard only a non-published local review; no remote deletion is performed."""
    from support_github import get_support_github_service
    import asyncio
    try:
        await asyncio.to_thread(get_support_github_service().store.discard,owner_key(session.user_id),review_id)
        return JSONResponse({'discarded':True},headers={'Cache-Control':'no-store'})
    except Exception as exc:
        raise _github_error(exc) from exc


@router.get('/github/reviews/{review_id}/images/{index}')
async def github_review_image(review_id: str, index: int, session: SessionToken = Depends(require_auth)):
    """Preview exactly the selected public image from the immutable owner-bound review."""
    from support_github import get_support_github_service
    import asyncio
    import base64
    try:
        row=await asyncio.to_thread(get_support_github_service().store.review,owner_key(session.user_id),review_id)
        if not row or not 0<=index<5:
            raise HTTPException(404,'Reviewed screenshot unavailable.')
        images=[asset for asset in row.get('assets',[]) if asset.get('media_type') in {'image/png','image/jpeg','image/webp'}]
        if index>=len(images):
            raise HTTPException(404,'Reviewed screenshot unavailable.')
        asset=images[index]
        return Response(base64.b64decode(asset['data_base64'],validate=True),media_type=asset['media_type'],headers={'Cache-Control':'no-store','X-Content-Type-Options':'nosniff'})
    except HTTPException:
        raise
    except Exception as exc:
        raise _github_error(exc) from exc


@router.post('/github/publish')
async def github_publish(request: Request, session: SessionToken = Depends(require_auth)):
    """Explicit final approval publishes exactly the stored review, surviving browser disconnect."""
    from support_github import publish_owned
    body=await _support_body(request,GitHubPublishRequest,2048)
    try:
        result=await publish_owned(get_ai_chat_service(),owner_key(session.user_id),body.review_id)
        return JSONResponse(result,headers={'Cache-Control':'no-store'})
    except Exception as exc:
        raise _github_error(exc) from exc
