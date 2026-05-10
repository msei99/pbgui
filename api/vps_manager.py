from __future__ import annotations

import asyncio
import json
import traceback
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse

from api.auth import SessionToken, require_auth, validate_token
from api.vps import get_bot_log_matches
from logging_helpers import human_log as _log
from vps_manager_service import VPSManagerService

SERVICE = "VPSManagerApi"

router = APIRouter()

_service: VPSManagerService | None = None


def _get_service() -> VPSManagerService:
    global _service
    if _service is None:
        _service = VPSManagerService()
    return _service


@router.get("/main_page", response_class=HTMLResponse)
def get_main_page(
    request: Request,
    st_base: str = Query(default="", description="Browser-visible Streamlit base URL"),
    session: SessionToken = Depends(require_auth),
) -> HTMLResponse:
    html_path = Path(__file__).resolve().parent.parent / "frontend" / "vps_manager.html"
    html = html_path.read_text(encoding="utf-8")

    scheme = request.url.scheme
    host = request.url.hostname or "127.0.0.1"
    port = request.url.port
    origin = f"{scheme}://{host}" + (f":{port}" if port else "")
    api_base = origin + "/api/vps-manager"
    ws_base = origin.replace("http://", "ws://").replace("https://", "wss://")

    if not st_base:
        st_base = f"http://{host}:8501"

    html = html.replace('"%%TOKEN%%"', json.dumps(session.token))
    html = html.replace('"%%API_BASE%%"', json.dumps(api_base))
    html = html.replace('"%%WS_BASE%%"', json.dumps(ws_base))
    html = html.replace('"%%ST_BASE%%"', json.dumps(st_base))

    from pbgui_func import PBGUI_VERSION
    from pbgui_purefunc import PBGUI_SERIAL

    html = html.replace('"%%VERSION%%"', json.dumps(PBGUI_VERSION))
    html = html.replace("%%VERSION%%", PBGUI_VERSION)
    html = html.replace('"%%SERIAL%%"', json.dumps(PBGUI_SERIAL))
    html = html.replace("%%SERIAL%%", PBGUI_SERIAL)

    nav_js = Path(__file__).resolve().parent.parent / "frontend" / "pbgui_nav.js"
    nav_hash = str(int(nav_js.stat().st_mtime)) if nav_js.exists() else PBGUI_VERSION
    html = html.replace("%%NAV_HASH%%", nav_hash)

    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})


@router.get("/detail/{hostname}")
def get_vps_detail(
    hostname: str,
    session: SessionToken = Depends(require_auth),
) -> JSONResponse:
    try:
        detail = _get_service().build_vps_detail(session.token, hostname)
        return JSONResponse(content=detail)
    except Exception as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.get("/detail-master")
def get_master_detail(
    session: SessionToken = Depends(require_auth),
) -> JSONResponse:
    try:
        detail = _get_service().build_master_detail()
        return JSONResponse(content=detail)
    except Exception as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.websocket("/ws")
async def ws_vps_manager(websocket: WebSocket):
    token = websocket.query_params.get("token", "")
    if not validate_token(token):
        await websocket.close(code=4001)
        return

    await websocket.accept()
    service = _get_service()
    context: dict[str, str] = {"view": "overview", "hostname": "", "token": token}
    push_task = asyncio.create_task(_push_loop(websocket, service, context), name="vps-manager-push")
    try:
        async for raw in websocket.iter_text():
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                await websocket.send_json({"type": "error", "error": "Invalid JSON"})
                continue

            cmd = str(msg.get("cmd") or "").strip()
            try:
                if cmd == "set_context":
                    context["view"] = str(msg.get("view") or "overview")
                    context["hostname"] = str(msg.get("hostname") or "")
                    _log(SERVICE, f"set_context view={context['view']} hostname={context['hostname']}", level="INFO")
                    await _send_current_context_detail(websocket, service, context)
                elif cmd == "refresh":
                    await asyncio.to_thread(service.refresh, force=True)
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True})
                elif cmd == "sync_api":
                    await service.start_api_sync()
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True})
                elif cmd == "save_vps":
                    data = await asyncio.to_thread(service.save_vps, token, msg.get("form") or {})
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True, "data": data})
                elif cmd == "prepare_import":
                    data = await asyncio.to_thread(service.prepare_import, msg.get("hostname") or "")
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True, "data": data})
                elif cmd == "save_vps_config":
                    data = await asyncio.to_thread(
                        service.save_vps_config,
                        token,
                        str(msg.get("hostname") or ""),
                        msg.get("form") or {},
                    )
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True, "data": data})
                elif cmd == "init_vps":
                    data = await asyncio.to_thread(service.init_vps, token, msg.get("form") or {}, debug=bool(msg.get("debug")))
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True, "data": data})
                elif cmd == "setup_vps":
                    data = await asyncio.to_thread(
                        service.setup_vps,
                        token,
                        str(msg.get("hostname") or ""),
                        msg.get("form") or {},
                        debug=bool(msg.get("debug")),
                    )
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True, "data": data})
                elif cmd == "delete_vps":
                    await asyncio.to_thread(service.delete_vps, str(msg.get("hostname") or ""))
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True})
                elif cmd == "read_vps_settings":
                    data = await asyncio.to_thread(service.read_vps_settings, token, str(msg.get("hostname") or ""))
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True, "data": data})
                elif cmd == "reveal_secret":
                    data = await asyncio.to_thread(
                        service.reveal_session_secret,
                        token,
                        str(msg.get("hostname") or ""),
                        str(msg.get("field") or ""),
                    )
                    await websocket.send_json({"type": "secret_value", "cmd": cmd, "success": True, "data": data})
                elif cmd == "fetch_vps_log":
                    data = await asyncio.to_thread(
                        service.fetch_vps_log,
                        str(msg.get("hostname") or ""),
                        filename=str(msg.get("filename") or ""),
                        size_kb=int(msg.get("size_kb") or 50),
                        reverse=bool(msg.get("reverse", True)),
                        debug=bool(msg.get("debug")),
                    )
                    await websocket.send_json({"type": "log_preview", "data": data, "hostname": str(msg.get("hostname") or "")})
                elif cmd == "load_more_commits":
                    await asyncio.to_thread(service.load_more_commits, str(msg.get("repo") or ""), str(msg.get("branch") or ""), int(msg.get("limit") or 50))
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True})
                elif cmd == "load_remote_branches":
                    branches = await asyncio.to_thread(service.load_remote_branches, str(msg.get("remote_url") or ""))
                    await websocket.send_json({"type": "remote_branches", "remote_url": str(msg.get("remote_url") or ""), "branches": branches})
                elif cmd == "run_master_command":
                    await asyncio.to_thread(
                        service.run_master_command,
                        command=str(msg.get("command") or ""),
                        command_text=str(msg.get("command_text") or ""),
                        debug=bool(msg.get("debug")),
                        sudo_pw=str(msg.get("sudo_pw") or "") or None,
                        extra_vars=msg.get("extra_vars") or None,
                    )
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True})
                elif cmd == "run_vps_command":
                    await asyncio.to_thread(
                        service.run_vps_command,
                        token=token,
                        hostname=str(msg.get("hostname") or ""),
                        command=str(msg.get("command") or ""),
                        command_text=str(msg.get("command_text") or ""),
                        debug=bool(msg.get("debug")),
                        extra_vars=msg.get("extra_vars") or None,
                    )
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True})
                elif cmd == "fetch_bot_log_matches":
                    bucket = str(msg.get("bucket") or "").strip()
                    if bucket not in {"today", "yesterday"}:
                        await websocket.send_json({"type": "error", "error": "bucket must be today or yesterday", "cmd": cmd})
                        continue
                    lines = await get_bot_log_matches(
                        str(msg.get("hostname") or ""),
                        str(msg.get("bot_name") or ""),
                        pb_version=str(msg.get("pb_version") or "") or None,
                        kind=str(msg.get("kind") or "tracebacks"),
                        bucket=bucket,
                        expected_count=int(msg.get("expected_count")) if msg.get("expected_count") is not None else None,
                        lines=int(msg.get("lines") or 5000),
                    )
                    await websocket.send_json({
                        "type": "bot_log_matches",
                        "hostname": str(msg.get("hostname") or ""),
                        "bot_name": str(msg.get("bot_name") or ""),
                        "kind": str(msg.get("kind") or "tracebacks"),
                        "bucket": bucket,
                        "expected_count": int(msg.get("expected_count")) if msg.get("expected_count") is not None else None,
                        "lines": lines,
                    })
                else:
                    await websocket.send_json({"type": "error", "error": f"Unknown command: {cmd}"})
            except Exception as exc:
                _log(SERVICE, f"command {cmd} failed: {exc}", level="WARNING", meta={"traceback": traceback.format_exc()})
                await websocket.send_json({"type": "error", "error": str(exc), "cmd": cmd})
    except WebSocketDisconnect:
        pass
    finally:
        push_task.cancel()
        try:
            await push_task
        except asyncio.CancelledError:
            pass


async def _push_loop(websocket: WebSocket, service: VPSManagerService, context: dict[str, str]) -> None:
    last_state = ""
    last_detail = ""
    try:
        while True:
            state = await asyncio.to_thread(service.build_state)
            encoded_state = json.dumps(state, sort_keys=True, default=str)
            if encoded_state != last_state:
                await websocket.send_json({"type": "state", "data": state})
                last_state = encoded_state

            detail = await asyncio.to_thread(_build_quick_detail_for_context, service, context)
            if detail is not None:
                encoded_detail = json.dumps(detail, sort_keys=True, default=str)
                if encoded_detail != last_detail:
                    await websocket.send_json({"type": "detail", "data": detail})
                    last_detail = encoded_detail
            else:
                last_detail = ""

            await asyncio.sleep(1)
    except asyncio.CancelledError:
        pass
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        _log(SERVICE, f"push loop failed: {exc}", level="WARNING", meta={"traceback": traceback.format_exc()})


def _build_detail_for_context(service: VPSManagerService, context: dict[str, str]):
    view = str(context.get("view") or "overview")
    if view == "master":
        return service.build_master_detail()
    if view == "vps":
        hostname = str(context.get("hostname") or "")
        if hostname:
            return service.build_vps_detail(str(context.get("token") or ""), hostname)
    return None


def _build_quick_detail_for_context(service: VPSManagerService, context: dict[str, str]):
    view = str(context.get("view") or "overview")
    if view == "master":
        return service.build_master_detail_quick()
    if view == "vps":
        hostname = str(context.get("hostname") or "")
        if hostname:
            return service.build_vps_detail(str(context.get("token") or ""), hostname, quick=True)
    return None


async def _send_current_context_detail(websocket: WebSocket, service: VPSManagerService, context: dict[str, str]) -> None:
    detail = await asyncio.to_thread(_build_quick_detail_for_context, service, context)
    if detail is not None:
        try:
            await websocket.send_json({"type": "detail", "data": detail})
            _log(SERVICE, f"detail sent for {context.get('view')}/{context.get('hostname')}", level="INFO")
        except Exception:
            _log(SERVICE, f"detail send failed for {context.get('view')}/{context.get('hostname')}", level="WARNING")
    else:
        _log(SERVICE, f"detail is None for {context.get('view')}/{context.get('hostname')}", level="INFO")
