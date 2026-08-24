"""Authenticated FastAPI routes for persistent PBGui AI conversations."""

from __future__ import annotations

import json
from pathlib import Path
import traceback

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from pydantic import BaseModel, Field

from ai_chat import AIChatError, get_ai_chat_service, owner_key
from ai_capabilities import AICapabilityError, get_ai_capability_service
from api.auth import SessionToken, require_auth
from logging_helpers import human_log as _log


SERVICE = "AIChat"
_OPENCODE_GO_REFERRAL_URL = "https://opencode.ai/go?ref=XPFTXPFZVF"

router = APIRouter()


@router.get("/providers/opencode-go/subscribe")
def open_opencode_go_subscription(
    session: SessionToken = Depends(require_auth),
) -> RedirectResponse:
    """Open the public OpenCode Go subscription page with the PBGui referral code."""
    return RedirectResponse(
        url=_OPENCODE_GO_REFERRAL_URL,
        status_code=307,
        headers={"Cache-Control": "no-store"},
    )


class GoConnectRequest(BaseModel):
    """OpenCode Go subscription key submitted once for server-side storage."""

    api_key: str = Field(min_length=16, max_length=1024)


class ChatRequest(BaseModel):
    """One legacy synchronous AI chat turn."""

    provider: str = Field(min_length=1, max_length=32)
    model: str = Field(default="", max_length=128)
    message: str = Field(min_length=1, max_length=12_000)
    conversation_id: str | None = Field(default=None, max_length=32)
    effort: str = Field(default="", max_length=64)


class ConversationCreateRequest(BaseModel):
    """Provider and model selected before the first cancellable chat turn."""

    provider: str = Field(min_length=1, max_length=32)
    model: str = Field(default="", max_length=128)
    effort: str = Field(default="", max_length=64)
    context: dict | None = None


class TurnCreateRequest(BaseModel):
    """One message submitted to an existing persistent conversation."""

    message: str = Field(min_length=1, max_length=12_000)
    context: dict | None = None
    effort: str | None = Field(default=None, max_length=64)
    model: str | None = Field(default=None, max_length=128)
    provider: str | None = Field(default=None, min_length=1, max_length=32)


class AIPreferencesRequest(BaseModel):
    """Bounded owner-scoped AI drawer preferences."""

    drawer_width: int = Field(ge=180, le=100_000)


class ConversationRewindRequest(BaseModel):
    """One persistent user-message rewind point."""

    message_index: int = Field(ge=0, le=1000)


class ProposalDecisionRequest(BaseModel):
    """Bind an approval decision to the displayed proposal and conversation."""

    payload_digest: str = Field(min_length=71, max_length=71)
    conversation_id: str = Field(min_length=32, max_length=32)


def _owner(session: SessionToken) -> str:
    """Return the opaque AI owner key for one authenticated PBGui user."""
    return owner_key(session.user_id)


def _json(payload: object, status_code: int = 200) -> JSONResponse:
    """Return a no-store JSON response."""
    return JSONResponse(payload, status_code=status_code, headers={"Cache-Control": "no-store"})


def _provider_error(operation: str, exc: Exception) -> HTTPException:
    """Log a provider failure and return a safe HTTP exception."""
    if isinstance(exc, (AIChatError, AICapabilityError)):
        _log(SERVICE, f"{operation} failed: {exc}", level="WARNING")
        return HTTPException(
            status_code=400,
            detail=str(exc),
            headers={"Cache-Control": "no-store"},
        )
    _log(
        SERVICE,
        f"{operation} failed: {type(exc).__name__}",
        level="ERROR",
        meta={"traceback": traceback.format_exc(), "operation": operation},
    )
    return HTTPException(
        status_code=500,
        detail="AI provider operation failed",
        headers={"Cache-Control": "no-store"},
    )


@router.get("/main_page", response_class=HTMLResponse)
def main_page(request: Request, session: SessionToken = Depends(require_auth)) -> HTMLResponse:
    """Serve the cookie-authenticated full AI chat page."""
    del session
    html_path = Path(__file__).parent.parent / "frontend" / "ai_chat.html"
    html = html_path.read_text(encoding="utf-8")
    scheme = request.url.scheme
    host = request.url.hostname or "127.0.0.1"
    port = request.url.port
    origin = f"{scheme}://{host}" + (f":{port}" if port else "")
    html = html.replace('"%%API_BASE%%"', json.dumps(origin + "/api/ai"))

    from pbgui_purefunc import PBGUI_SERIAL, PBGUI_VERSION

    html = html.replace('"%%VERSION%%"', json.dumps(PBGUI_VERSION))
    html = html.replace('"%%SERIAL%%"', json.dumps(PBGUI_SERIAL))
    nav_js = Path(__file__).parent.parent / "frontend" / "pbgui_nav.js"
    nav_hash = str(int(nav_js.stat().st_mtime)) if nav_js.exists() else PBGUI_VERSION
    html = html.replace("%%NAV_HASH%%", nav_hash)
    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})


@router.get("/status")
async def status(session: SessionToken = Depends(require_auth)) -> JSONResponse:
    """Return non-secret AI provider status."""
    try:
        return _json(await get_ai_chat_service().status(_owner(session)))
    except Exception as exc:
        raise _provider_error("status", exc) from exc


@router.get("/preferences")
async def get_preferences(session: SessionToken = Depends(require_auth)) -> JSONResponse:
    """Return owner-scoped AI UI preferences."""
    try:
        return _json(get_ai_chat_service().get_preferences(_owner(session)))
    except Exception as exc:
        raise _provider_error("get_preferences", exc) from exc


@router.put("/preferences")
async def save_preferences(
    body: AIPreferencesRequest,
    session: SessionToken = Depends(require_auth),
) -> JSONResponse:
    """Persist owner-scoped AI UI preferences."""
    try:
        return _json(
            get_ai_chat_service().save_preferences(_owner(session), body.drawer_width)
        )
    except Exception as exc:
        raise _provider_error("save_preferences", exc) from exc


@router.post("/providers/opencode-go/connect")
async def connect_go(
    body: GoConnectRequest,
    session: SessionToken = Depends(require_auth),
) -> JSONResponse:
    """Verify and store an OpenCode Go subscription key."""
    try:
        await get_ai_chat_service().connect_go(_owner(session), body.api_key)
        return _json({"success": True})
    except Exception as exc:
        raise _provider_error("connect_go", exc) from exc


@router.delete("/providers/opencode-go/connection")
async def disconnect_go(session: SessionToken = Depends(require_auth)) -> JSONResponse:
    """Remove the current user's OpenCode Go connection."""
    try:
        await get_ai_chat_service().disconnect_go(_owner(session))
        return _json({"success": True})
    except Exception as exc:
        raise _provider_error("disconnect_go", exc) from exc


@router.post("/providers/chatgpt/device-login")
async def start_chatgpt_login(session: SessionToken = Depends(require_auth)) -> JSONResponse:
    """Start the official ChatGPT device-code login flow."""
    try:
        return _json(await get_ai_chat_service().start_codex_login(_owner(session)))
    except Exception as exc:
        raise _provider_error("start_chatgpt_login", exc) from exc


@router.post("/providers/chatgpt/browser-login")
async def start_chatgpt_browser_login(session: SessionToken = Depends(require_auth)) -> JSONResponse:
    """Start the official ChatGPT browser OAuth flow."""
    try:
        return _json(await get_ai_chat_service().start_codex_browser_login(_owner(session)))
    except Exception as exc:
        raise _provider_error("start_chatgpt_browser_login", exc) from exc


@router.post("/providers/chatgpt/login-cancel")
async def cancel_chatgpt_login(session: SessionToken = Depends(require_auth)) -> JSONResponse:
    """Cancel a pending ChatGPT device login."""
    try:
        await get_ai_chat_service().cancel_codex_login(_owner(session))
        return _json({"success": True})
    except Exception as exc:
        raise _provider_error("cancel_chatgpt_login", exc) from exc


@router.delete("/providers/chatgpt/connection")
async def disconnect_chatgpt(session: SessionToken = Depends(require_auth)) -> JSONResponse:
    """Log the current user out from ChatGPT."""
    try:
        await get_ai_chat_service().logout_codex(_owner(session))
        return _json({"success": True})
    except Exception as exc:
        raise _provider_error("disconnect_chatgpt", exc) from exc


@router.get("/models")
async def models(
    provider: str = Query(min_length=1, max_length=32),
    session: SessionToken = Depends(require_auth),
) -> JSONResponse:
    """List account-visible models supported by one native adapter."""
    try:
        return _json({"models": await get_ai_chat_service().models(_owner(session), provider)})
    except Exception as exc:
        raise _provider_error("models", exc) from exc


@router.post("/models/health-refresh")
async def refresh_model_health(session: SessionToken = Depends(require_auth)) -> JSONResponse:
    """Queue a serial free-model health refresh for the current owner."""
    try:
        await get_ai_chat_service().request_model_health_refresh(_owner(session))
        return _json({"status": "queued"}, status_code=202)
    except Exception as exc:
        raise _provider_error("refresh_model_health", exc) from exc


@router.post("/chat")
async def chat(body: ChatRequest, session: SessionToken = Depends(require_auth)) -> JSONResponse:
    """Run one legacy synchronous turn with controlled PBGui tools."""
    try:
        result = await get_ai_chat_service().chat(
            _owner(session),
            body.provider,
            body.model,
            body.message,
            body.conversation_id,
            body.effort,
        )
        return _json(result)
    except Exception as exc:
        raise _provider_error("chat", exc) from exc


@router.post("/conversations")
async def create_conversation(
    body: ConversationCreateRequest,
    session: SessionToken = Depends(require_auth),
) -> JSONResponse:
    """Create an owner-bound conversation before its first provider request."""
    try:
        conversation_id = await get_ai_chat_service().create_conversation(
            _owner(session), body.provider, body.model, body.effort, body.context
        )
        return _json({"conversation_id": conversation_id}, status_code=201)
    except Exception as exc:
        raise _provider_error("create_conversation", exc) from exc


@router.get("/conversations")
async def list_conversations(session: SessionToken = Depends(require_auth)) -> JSONResponse:
    """List persistent owner-bound AI conversations."""
    try:
        conversations = await get_ai_chat_service().list_conversations(_owner(session))
        return _json({"conversations": conversations})
    except Exception as exc:
        raise _provider_error("list_conversations", exc) from exc


@router.get("/conversations/{conversation_id}")
async def get_conversation(
    conversation_id: str,
    session: SessionToken = Depends(require_auth),
) -> JSONResponse:
    """Return one persistent owner-bound conversation snapshot."""
    try:
        return _json(
            await get_ai_chat_service().get_conversation(_owner(session), conversation_id)
        )
    except Exception as exc:
        raise _provider_error("get_conversation", exc) from exc


@router.post("/conversations/{conversation_id}/turns")
async def start_turn(
    conversation_id: str,
    body: TurnCreateRequest,
    session: SessionToken = Depends(require_auth),
) -> JSONResponse:
    """Start one API-owned turn and return immediately."""
    try:
        result = await get_ai_chat_service().start_turn(
            _owner(session), conversation_id, body.message, body.context, body.effort, body.model, body.provider
        )
        return _json(result, status_code=202)
    except Exception as exc:
        raise _provider_error("start_turn", exc) from exc


@router.post("/conversations/{conversation_id}/ui-actions/{action_id}/ack")
async def acknowledge_ui_action(
    conversation_id: str,
    action_id: str,
    session: SessionToken = Depends(require_auth),
) -> JSONResponse:
    """Acknowledge one browser action after an allowlisted page applied it."""
    try:
        await get_ai_chat_service().acknowledge_ui_action(_owner(session), conversation_id, action_id)
        return _json({"status": "acknowledged"})
    except Exception as exc:
        raise _provider_error("acknowledge_ui_action", exc) from exc


@router.post("/conversations/{conversation_id}/rewind")
async def rewind_conversation(
    conversation_id: str,
    body: ConversationRewindRequest,
    session: SessionToken = Depends(require_auth),
) -> JSONResponse:
    """Rewind one owner-bound conversation to before a selected user message."""
    try:
        result = await get_ai_chat_service().rewind_conversation(
            _owner(session), conversation_id, body.message_index
        )
        return _json(result)
    except Exception as exc:
        raise _provider_error("rewind_conversation", exc) from exc


@router.get("/conversations/{conversation_id}/activity")
async def conversation_activity(
    conversation_id: str,
    session: SessionToken = Depends(require_auth),
) -> JSONResponse:
    """Return non-sensitive progress for one active owner-bound conversation."""
    try:
        result = await get_ai_chat_service().conversation_activity(
            _owner(session), conversation_id
        )
        return _json(result)
    except Exception as exc:
        raise _provider_error("conversation_activity", exc) from exc


@router.post("/conversations/{conversation_id}/cancel")
async def cancel_chat(
    conversation_id: str,
    session: SessionToken = Depends(require_auth),
) -> JSONResponse:
    """Cancel one active chat turn."""
    try:
        await get_ai_chat_service().cancel(_owner(session), conversation_id)
        return _json({"success": True})
    except Exception as exc:
        raise _provider_error("cancel_chat", exc) from exc


@router.delete("/conversations/{conversation_id}")
async def delete_conversation(
    conversation_id: str,
    session: SessionToken = Depends(require_auth),
) -> JSONResponse:
    """Delete one persistent owner-bound conversation."""
    try:
        await get_ai_chat_service().delete_conversation(_owner(session), conversation_id)
        return _json({"success": True})
    except Exception as exc:
        raise _provider_error("delete_conversation", exc) from exc


@router.get("/proposals")
async def list_proposals(
    conversation_id: str = Query(default="", max_length=32),
    session: SessionToken = Depends(require_auth),
) -> JSONResponse:
    """List pending owner-bound action proposals."""
    try:
        proposals = await get_ai_capability_service().list_proposals(
            _owner(session), conversation_id
        )
        return _json({"proposals": proposals})
    except Exception as exc:
        raise _provider_error("list_proposals", exc) from exc


@router.get("/capabilities")
async def capability_registry(session: SessionToken = Depends(require_auth)) -> JSONResponse:
    """Return the current effect-aware path-free capability registry."""
    del session
    try:
        return _json(get_ai_capability_service().capability_registry())
    except Exception as exc:
        raise _provider_error("capability_registry", exc) from exc


@router.get("/actions/history")
async def action_history(
    limit: int = Query(default=50, ge=1, le=200),
    session: SessionToken = Depends(require_auth),
) -> JSONResponse:
    """Return durable owner-bound proposal decisions and execution outcomes."""
    try:
        records = await get_ai_capability_service().list_action_history(_owner(session), limit)
        return _json({"actions": records})
    except Exception as exc:
        raise _provider_error("action_history", exc) from exc


@router.post("/proposals/{proposal_id}/approve")
async def approve_proposal(
    proposal_id: str,
    body: ProposalDecisionRequest,
    session: SessionToken = Depends(require_auth),
) -> JSONResponse:
    """Execute one exact proposal after explicit browser approval."""
    try:
        result = await get_ai_capability_service().approve(
            _owner(session), proposal_id, body.payload_digest, body.conversation_id
        )
        if result.get("status") == "executed":
            continuation_result = {
                key: result.get(key)
                for key in ("proposal_id", "status", "action", "name", "queued_count", "template")
                if result.get(key) is not None
            }
            continuation_message = (
                "An approved PBGui action completed successfully. Continue the user's existing "
                "requested workflow now. If no requested step remains, briefly confirm completion "
                "without creating another proposal. Approved action result:\n"
                + json.dumps(continuation_result, allow_nan=False, separators=(",", ":"))
            )
            try:
                continuation = await get_ai_chat_service().start_turn(
                    _owner(session),
                    body.conversation_id,
                    continuation_message,
                    None,
                    None,
                    None,
                    None,
                    True,
                )
                result["continuation"] = continuation
            except AIChatError as exc:
                _log(SERVICE, f"Approved action continuation was not started: {type(exc).__name__}", level="WARNING")
                result["continuation"] = {"status": "not_started"}
        return _json(result)
    except Exception as exc:
        raise _provider_error("approve_proposal", exc) from exc


@router.post("/proposals/{proposal_id}/reject")
async def reject_proposal(
    proposal_id: str,
    body: ProposalDecisionRequest,
    session: SessionToken = Depends(require_auth),
) -> JSONResponse:
    """Reject one proposal without changing PBGui state."""
    try:
        result = await get_ai_capability_service().reject(
            _owner(session), proposal_id, body.payload_digest, body.conversation_id
        )
        return _json(result)
    except Exception as exc:
        raise _provider_error("reject_proposal", exc) from exc
