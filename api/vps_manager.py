from __future__ import annotations

import asyncio
import json
import traceback
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

from api.auth import SessionToken, require_auth, validate_token
from api.vps import get_bot_log_matches
from logging_helpers import human_log as _log
from vps_manager_service import UnknownHostKeyError, VPSManagerService

DetailPayload = dict[str, object]

SERVICE = "VPSManagerApi"

router = APIRouter()

_service: VPSManagerService | None = None


class ExistingVpsImportRequest(BaseModel):
    hostname: str = ""
    ip: str = ""
    user: str = ""
    user_pw: str = ""
    local_sudo_pw: str = ""
    install_dir: str = ""
    accept_unknown_host: bool = False
    accepted_host_key_fingerprint: str = ""


class ClusterNodesImportRequest(BaseModel):
    local_sudo_pw: str = ""
    passwords: dict[str, str] = {}

MASTER_CONTEXT_VIEWS = {
    "master",
    "master-task-log",
    "master-host-logs",
    "master-pbgui-branch",
    "master-pb7-branch",
    "master-ufw",
}
VPS_CONTEXT_VIEWS = {
    "vps",
    "vps-task-log",
    "vps-host-logs",
    "vps-setup",
    "vps-pbgui-branch",
    "vps-pb7-branch",
    "vps-ufw",
}


def _get_service() -> VPSManagerService:
    global _service
    if _service is None:
        _service = VPSManagerService()
    return _service


def get_service_instance() -> VPSManagerService:
    return _get_service()


@router.get("/main_page", response_class=HTMLResponse)
def get_main_page(
    request: Request,
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

    html = html.replace('"%%TOKEN%%"', json.dumps(session.token))
    html = html.replace('"%%API_BASE%%"', json.dumps(api_base))
    html = html.replace('"%%WS_BASE%%"', json.dumps(ws_base))

    from pbgui_purefunc import PBGUI_VERSION
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


@router.get("/cpu-history/{hostname}")
def get_cpu_history(
    hostname: str,
    bot_name: str = Query(default="", description="Optional bot name for bot CPU history"),
    session: SessionToken = Depends(require_auth),
) -> JSONResponse:
    try:
        payload = _get_service().get_cpu_history(hostname, bot_name=bot_name)
        return JSONResponse(content=payload)
    except Exception as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.get("/metric-history/{hostname}")
def get_metric_history(
    hostname: str,
    metric: str = Query(default="cpu", description="Metric key: cpu, memory, disk, swap"),
    bot_name: str = Query(default="", description="Optional bot name for bot CPU history"),
    session: SessionToken = Depends(require_auth),
) -> JSONResponse:
    try:
        payload = _get_service().get_metric_history(hostname, bot_name=bot_name, metric=metric)
        return JSONResponse(content=payload)
    except Exception as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.get("/import/resolve-host")
def resolve_existing_vps_import_host(
    hostname: str = Query(default="", description="Hostname to resolve from local /etc/hosts"),
    session: SessionToken = Depends(require_auth),
) -> JSONResponse:
    del session
    try:
        data = _get_service().resolve_existing_vps_import_host(hostname)
        return JSONResponse(content=data)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.post("/import/probe")
def probe_existing_vps_import(
    payload: ExistingVpsImportRequest,
    session: SessionToken = Depends(require_auth),
) -> JSONResponse:
    del session
    try:
        data = _get_service().probe_existing_vps_import(payload.dict())
        return JSONResponse(content=data)
    except Exception as exc:
        _log(SERVICE, f"existing VPS import probe failed: {exc}", level="WARNING", meta={"traceback": traceback.format_exc()})
        raise HTTPException(status_code=400, detail=str(exc))


@router.post("/import/save")
def save_existing_vps_import(
    payload: ExistingVpsImportRequest,
    session: SessionToken = Depends(require_auth),
) -> JSONResponse:
    try:
        data = _get_service().save_existing_vps_import(session.token, payload.dict())
        return JSONResponse(content=data)
    except Exception as exc:
        _log(SERVICE, f"existing VPS import save failed: {exc}", level="WARNING", meta={"traceback": traceback.format_exc()})
        raise HTTPException(status_code=400, detail=str(exc))


@router.get("/cluster-import/preview")
def preview_cluster_nodes_import(
    session: SessionToken = Depends(require_auth),
) -> JSONResponse:
    del session
    try:
        data = _get_service().preview_cluster_nodes_import()
        return JSONResponse(content=data)
    except Exception as exc:
        _log(SERVICE, f"Cluster node import preview failed: {exc}", level="WARNING", meta={"traceback": traceback.format_exc()})
        raise HTTPException(status_code=400, detail=str(exc))


@router.post("/cluster-import/apply")
def apply_cluster_nodes_import(
    payload: ClusterNodesImportRequest | None = None,
    session: SessionToken = Depends(require_auth),
) -> JSONResponse:
    try:
        data = _get_service().start_cluster_nodes_import(session.token, payload.dict() if payload else {})
        return JSONResponse(content=data)
    except Exception as exc:
        _log(SERVICE, f"Cluster node import apply failed: {exc}", level="WARNING", meta={"traceback": traceback.format_exc()})
        raise HTTPException(status_code=400, detail=str(exc))


@router.get("/cluster-import/progress/{job_id}")
def get_cluster_nodes_import_progress(
    job_id: str,
    session: SessionToken = Depends(require_auth),
) -> JSONResponse:
    del session
    try:
        data = _get_service().get_cluster_nodes_import_progress(job_id)
        return JSONResponse(content=data)
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
                elif cmd == "probe_vps_host_key":
                    data = await asyncio.to_thread(
                        service.probe_vps_host_key,
                        str(msg.get("hostname") or ""),
                    )
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True, "data": data})
                elif cmd == "trust_vps_host_key":
                    data = await asyncio.to_thread(
                        service.trust_vps_host_key,
                        str(msg.get("hostname") or ""),
                        str(msg.get("expected_fingerprint") or ""),
                        replace_existing=bool(msg.get("replace_existing")),
                    )
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True, "data": data})
                elif cmd == "save_vps_logging_config":
                    data = await asyncio.to_thread(service.save_vps_logging_config, msg.get("data") or {})
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True, "data": data})
                elif cmd == "save_vps_deploy_settings":
                    data = await asyncio.to_thread(service.save_vps_deploy_settings, msg.get("data") or {})
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True, "data": data})
                elif cmd == "init_vps":
                    data = await asyncio.to_thread(service.init_vps, token, msg.get("form") or {}, debug=bool(msg.get("debug")))
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True, "data": data})
                elif cmd == "check_cmc_api_key":
                    data = await asyncio.to_thread(service.check_cmc_api_key, str(msg.get("api_key") or ""))
                    await websocket.send_json({"type": "cmc_check_result", "data": data})
                elif cmd == "detect_public_ip":
                    data = await asyncio.to_thread(service.detect_public_ip)
                    await websocket.send_json({"type": "public_ip_result", "data": data})
                elif cmd == "setup_vps":
                    data = await asyncio.to_thread(
                        service.setup_vps,
                        token,
                        str(msg.get("hostname") or ""),
                        msg.get("form") or {},
                        debug=bool(msg.get("debug")),
                    )
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True, "data": data})
                elif cmd == "add_vps_to_cluster":
                    data = await asyncio.to_thread(
                        service.add_vps_to_cluster,
                        str(msg.get("hostname") or ""),
                    )
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True, "data": data})
                elif cmd == "preview_vps_systemd_migration":
                    data = await asyncio.to_thread(
                        service.preview_vps_systemd_migration,
                        token,
                        str(msg.get("hostname") or ""),
                        msg.get("form") or {},
                    )
                    await websocket.send_json({"type": "vps_systemd_migration_preview", "cmd": cmd, "success": True, "data": data})
                elif cmd == "delete_vps":
                    await asyncio.to_thread(service.delete_vps, str(msg.get("hostname") or ""))
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True})
                elif cmd == "read_vps_settings":
                    hostname = str(msg.get("hostname") or "")
                    loop = asyncio.get_running_loop()

                    def send_read_progress(step: str, label: str, status: str = "running") -> None:
                        payload = {
                            "type": "vps_read_settings_progress",
                            "cmd": cmd,
                            "hostname": hostname,
                            "step": step,
                            "label": label,
                            "status": status,
                        }
                        future = asyncio.run_coroutine_threadsafe(websocket.send_json(payload), loop)
                        try:
                            future.result(timeout=3)
                        except Exception:
                            pass

                    data = await asyncio.to_thread(
                        service.read_vps_settings,
                        token,
                        hostname,
                        msg.get("form") or {},
                        send_read_progress,
                    )
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
                elif cmd == "load_remote_branch_commits":
                    remote_url = str(msg.get("remote_url") or "")
                    branch_name = str(msg.get("branch") or "")
                    limit = int(msg.get("limit") or 50)
                    commits = await asyncio.to_thread(service.load_remote_branch_commits, remote_url, branch_name, limit)
                    await websocket.send_json({
                        "type": "remote_branch_commits",
                        "remote_url": remote_url,
                        "branch": branch_name,
                        "limit": limit,
                        "commits": commits,
                    })
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
                elif cmd == "deploy_vps_logging":
                    data = await asyncio.to_thread(
                        service.deploy_vps_logging,
                        token,
                        msg.get("hostnames") or [],
                        debug=bool(msg.get("debug")),
                    )
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True, "data": data})
                elif cmd == "run_vps_deploy":
                    data = await asyncio.to_thread(
                        service.run_vps_deploy,
                        token,
                        msg.get("hostnames") or [],
                        command=str(msg.get("command") or ""),
                        mode=str(msg.get("mode") or ""),
                        debug=bool(msg.get("debug")),
                        extra_vars=msg.get("extra_vars") or None,
                    )
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True, "data": data})
                elif cmd == "validate_and_stage_vps_deploy_host":
                    try:
                        data = await asyncio.to_thread(
                            service.validate_and_stage_vps_deploy_host,
                            token,
                            hostnames=msg.get("hostnames") or [],
                            hostname=str(msg.get("hostname") or ""),
                            password=str(msg.get("password") or ""),
                            command=str(msg.get("command") or ""),
                            mode=str(msg.get("mode") or ""),
                            debug=bool(msg.get("debug")),
                            extra_vars=msg.get("extra_vars") or None,
                            entry_id=str(msg.get("entry_id") or "") or None,
                            accept_unknown_host=bool(msg.get("accept_unknown_host")),
                            accepted_host_key_fingerprint=str(msg.get("accepted_host_key_fingerprint") or ""),
                        )
                        await websocket.send_json({"type": "result", "cmd": cmd, "success": True, "data": data})
                    except UnknownHostKeyError as exc:
                        await websocket.send_json({
                            "type": "confirm_unknown_host_key",
                            "cmd": cmd,
                            "hostname": exc.hostname,
                            "ssh_host": exc.ssh_host,
                            "ip": exc.ip,
                            "key_type": exc.key_type,
                            "fingerprint": exc.fingerprint,
                            "error": str(exc),
                        })
                elif cmd == "finalize_vps_deploy_session":
                    data = await asyncio.to_thread(service.finalize_vps_deploy_session, str(msg.get("entry_id") or ""))
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True, "data": data})
                elif cmd == "fetch_bot_log_matches":
                    bucket = str(msg.get("bucket") or "").strip()
                    if bucket != "today":
                        await websocket.send_json({"type": "error", "error": "bucket must be today", "cmd": cmd})
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
                elif cmd == "get_cpu_history":
                    data = await asyncio.to_thread(
                        service.get_cpu_history,
                        str(msg.get("hostname") or ""),
                        bot_name=str(msg.get("bot_name") or ""),
                    )
                    await websocket.send_json({
                        "type": "cpu_history",
                        "cmd": cmd,
                        "success": True,
                        "hostname": str(msg.get("hostname") or ""),
                        "bot_name": str(msg.get("bot_name") or ""),
                        "data": data,
                    })
                elif cmd == "browse_files":
                    path = str(msg.get("path") or "")
                    data = await asyncio.to_thread(service.browse_files, path)
                    await websocket.send_json({"type": "browse_result", "data": data})
                elif cmd == "check_vps_ready":
                    data = await asyncio.to_thread(service.check_vps_ready, dict(msg.get("form") or {}))
                    await websocket.send_json({"type": "vps_ready_result", "data": data})
                elif cmd == "write_hosts_entry":
                    data = await asyncio.to_thread(
                        service.write_hosts_entry,
                        str(msg.get("ip") or ""),
                        str(msg.get("hostname") or ""),
                        str(msg.get("sudo_pw") or ""),
                    )
                    await websocket.send_json({"type": "write_hosts_result", "data": data})
                elif cmd == "read_ufw_rules":
                    data = await asyncio.to_thread(
                        service.read_ufw_rules,
                        str(msg.get("hostname") or ""),
                        str(msg.get("sudo_pw") or "") or None,
                    )
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True, "data": data})
                elif cmd == "preview_ufw_rules":
                    data = await asyncio.to_thread(
                        service.preview_ufw_rules,
                        str(msg.get("hostname") or ""),
                        msg.get("payload") or {},
                        str(msg.get("sudo_pw") or "") or None,
                    )
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True, "data": data})
                elif cmd == "apply_ufw_rules":
                    data = await asyncio.to_thread(
                        service.apply_ufw_rules,
                        str(msg.get("hostname") or ""),
                        msg.get("payload") or {},
                        str(msg.get("sudo_pw") or "") or None,
                    )
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True, "data": data})
                elif cmd == "validate_local_sudo_password":
                    data = await asyncio.to_thread(
                        service.validate_local_sudo_password,
                        str(msg.get("sudo_pw") or ""),
                    )
                    await websocket.send_json({"type": "local_sudo_validation_result", "data": data})
                elif cmd == "get_metric_history":
                    data = await asyncio.to_thread(
                        service.get_metric_history,
                        str(msg.get("hostname") or ""),
                        bot_name=str(msg.get("bot_name") or ""),
                        metric=str(msg.get("metric") or "cpu"),
                    )
                    await websocket.send_json({
                        "type": "metric_history",
                        "cmd": cmd,
                        "success": True,
                        "hostname": str(msg.get("hostname") or ""),
                        "bot_name": str(msg.get("bot_name") or ""),
                        "metric": str(msg.get("metric") or "cpu"),
                        "data": data,
                    })
                else:
                    await websocket.send_json({"type": "error", "error": f"Unknown command: {cmd}"})
            except Exception as exc:
                _log(SERVICE, f"command {cmd} failed: {exc}", level="WARNING", meta={"traceback": traceback.format_exc()})
                if cmd == "read_vps_settings":
                    await websocket.send_json({
                        "type": "vps_read_settings_progress",
                        "cmd": cmd,
                        "hostname": str(msg.get("hostname") or ""),
                        "step": "error",
                        "label": str(exc),
                        "status": "error",
                    })
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


def _build_detail_for_context(service: VPSManagerService, context: dict[str, str]) -> DetailPayload | None:
    view = str(context.get("view") or "overview")
    if view in MASTER_CONTEXT_VIEWS:
        return service.build_master_detail()
    if view in VPS_CONTEXT_VIEWS:
        hostname = str(context.get("hostname") or "")
        if hostname:
            try:
                return service.build_vps_detail(str(context.get("token") or ""), hostname)
            except ValueError as exc:
                if str(exc).startswith("Unknown VPS:"):
                    context["view"] = "overview"
                    context["hostname"] = ""
                    return None
                raise
    return None


def _build_quick_detail_for_context(service: VPSManagerService, context: dict[str, str]) -> DetailPayload | None:
    view = str(context.get("view") or "overview")
    if view in MASTER_CONTEXT_VIEWS:
        return service.build_master_detail_quick()
    if view in VPS_CONTEXT_VIEWS:
        hostname = str(context.get("hostname") or "")
        if hostname:
            try:
                return service.build_vps_detail(str(context.get("token") or ""), hostname, quick=True)
            except ValueError as exc:
                if str(exc).startswith("Unknown VPS:"):
                    context["view"] = "overview"
                    context["hostname"] = ""
                    return None
                raise
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
