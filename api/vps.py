"""
FastAPI WebSocket router for VPS monitoring.

Replaces ``master/ws_server.py`` — single endpoint ``/ws/vps`` that pushes
full state on every change and handles client commands (log fetch, service
restart, instance kill, …).

All operations are async.  No threads, no Paramiko.
"""

from __future__ import annotations

import asyncio
import json
import time
import traceback
from typing import Optional

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from api.auth import validate_token
from pbgui_purefunc import save_ini
from logging_helpers import human_log as _log
from master.async_monitor import VPSMonitor
from master.async_logs import (
    AsyncLogStreamer, LocalLogSub, resolve_bot_log_path,
    local_logs_dir, tail_file,
)

SERVICE = "VPSMonitor"

router = APIRouter()

# ── Module-level state (initialized by startup hook) ────────

_monitor: Optional[VPSMonitor] = None
_streamer: Optional[AsyncLogStreamer] = None

# Connected clients
_clients: set[WebSocket] = set()

# Push intervals
STATE_PUSH_INTERVAL = 1.0    # max rate for full-state push
LOG_PUSH_INTERVAL = 0.15     # ~150ms for log line push
LOCAL_LOG_PUSH_INTERVAL = 0.15


def init(monitor: VPSMonitor, streamer: AsyncLogStreamer):
    """Called once at FastAPI startup to inject shared objects."""
    global _monitor, _streamer
    _monitor = monitor
    _streamer = streamer


# ── Allowed UI setting keys (whitelist) ──────────────────────

_UI_SETTINGS_KEYS = {"compact"}


# ── WebSocket endpoint ───────────────────────────────────────

@router.websocket("/ws/vps")
async def ws_vps(websocket: WebSocket):
    """
    Main VPS monitoring WebSocket.

    Query params: ``?token=xxx``

    Push messages (server → client):
        - ``{"type": "state", "data": {…}}`` — full state
        - ``{"type": "log_lines", …}`` — incremental remote log lines
        - ``{"type": "local_log_lines", …}`` — incremental local log lines

    Command messages (client → server):
        - ``{"cmd": "restart_service", "host": …, "service": …}``
        - ``{"cmd": "get_logs", "host": …, "service": …, "lines": 200}``
        - ``{"cmd": "subscribe_logs", "host": …, "service": …}``
        - ``{"cmd": "unsubscribe_logs"}``
        - ``{"cmd": "kill_instance", "host": …, "name": …}``
        - etc.
    """
    await websocket.accept()

    # ── Auth ──
    token = websocket.query_params.get("token")
    session = validate_token(token) if token else None
    if not session:
        await websocket.send_json({"error": "Invalid or missing token"})
        await websocket.close(code=1008)
        return

    _clients.add(websocket)
    _log(SERVICE, f"[ws] Client connected: {websocket.client}")

    # Per-client subscriptions
    log_stream_id: Optional[str] = None
    log_sid: Optional[str] = None
    local_sub: Optional[LocalLogSub] = None

    # Background push tasks for this client
    push_state_task = asyncio.create_task(_push_state_loop(websocket))
    push_log_task = asyncio.create_task(
        _push_log_loop(websocket, lambda: log_stream_id, lambda: log_sid)
    )
    push_local_log_task = asyncio.create_task(
        _push_local_log_loop(websocket, lambda: local_sub)
    )

    try:
        # Send initial full state
        await _send_full_state(websocket)

        # Process incoming commands
        async for raw in websocket.iter_text():
            try:
                request = json.loads(raw)
            except json.JSONDecodeError:
                await websocket.send_json({"type": "error",
                                           "error": "Invalid JSON"})
                continue

            cmd = request.get("cmd", "")

            # ── restart_service ──
            if cmd == "restart_service":
                result = await _cmd_restart_service(request)
                await websocket.send_json(result)

            # ── get_logs (one-shot) ──
            elif cmd == "get_logs":
                result = await _cmd_get_logs(request)
                await websocket.send_json(result)

            # ── get_log_info ──
            elif cmd == "get_log_info":
                result = await _cmd_get_log_info(request)
                await websocket.send_json(result)

            # ── subscribe_logs (remote) ──
            elif cmd == "subscribe_logs":
                # Stop previous sub
                if log_stream_id and _streamer:
                    _streamer.stop_stream(log_stream_id)
                log_stream_id, log_sid = await _cmd_subscribe_logs(
                    websocket, request
                )

            # ── unsubscribe_logs ──
            elif cmd == "unsubscribe_logs":
                if log_stream_id and _streamer:
                    _streamer.stop_stream(log_stream_id)
                log_stream_id = None
                log_sid = None

            # ── kill_instance ──
            elif cmd == "kill_instance":
                result = await _cmd_kill_instance(request)
                await websocket.send_json(result)

            # ── set_setting ──
            elif cmd == "set_setting":
                _cmd_set_setting(request)

            # ── list_local_logs ──
            elif cmd == "list_local_logs":
                files = _streamer.list_local_logs() if _streamer else []
                await websocket.send_json({
                    "type": "local_logs_list", "files": files,
                })

            # ── get_local_logs (one-shot) ──
            elif cmd == "get_local_logs":
                result = _cmd_get_local_logs(request)
                await websocket.send_json(result)

            # ── subscribe_local_logs ──
            elif cmd == "subscribe_local_logs":
                local_sub = await _cmd_subscribe_local_logs(
                    websocket, request
                )

            # ── unsubscribe_local_logs ──
            elif cmd == "unsubscribe_local_logs":
                local_sub = None

            else:
                await websocket.send_json({
                    "type": "error",
                    "error": f"Unknown command: {cmd}",
                })

    except WebSocketDisconnect:
        pass
    except Exception as e:
        _log(SERVICE, f"[ws] Client error: {e}", level="WARNING",
             meta={'traceback': traceback.format_exc()})
    finally:
        _clients.discard(websocket)
        push_state_task.cancel()
        push_log_task.cancel()
        push_local_log_task.cancel()
        # Cleanup remote log subscription
        if log_stream_id and _streamer:
            _streamer.stop_stream(log_stream_id)
        _log(SERVICE, f"[ws] Client disconnected: {websocket.client}")


# ── Push loops ───────────────────────────────────────────────

async def _push_state_loop(ws: WebSocket):
    """Push full state whenever store.changed fires (event-based, not polling)."""
    try:
        while True:
            if _monitor and _monitor.store:
                # Wait for change event
                _monitor.store.changed.clear()
                await _monitor.store.changed.wait()
                # Throttle to avoid flooding
                await asyncio.sleep(STATE_PUSH_INTERVAL)
                await _send_full_state(ws)
            else:
                await asyncio.sleep(STATE_PUSH_INTERVAL)
    except (asyncio.CancelledError, WebSocketDisconnect):
        pass
    except Exception as e:
        _log(SERVICE, f"[ws] State push error: {e}", level="WARNING")


async def _push_log_loop(ws: WebSocket,
                         get_stream_id, get_sid):
    """Push buffered remote log lines at ~150ms intervals."""
    try:
        while True:
            await asyncio.sleep(LOG_PUSH_INTERVAL)
            stream_id = get_stream_id()
            if not stream_id or not _streamer:
                continue
            lines = _streamer.read_stream(stream_id, max_lines=50)
            if not lines:
                continue
            status = _streamer.get_stream_status(stream_id)
            msg: dict = {
                "type": "log_lines",
                "lines": lines,
                "host": status.get("hostname", "") if status else "",
                "service": status.get("log_path", "") if status else "",
            }
            sid = get_sid()
            if sid is not None:
                msg["sid"] = sid
            await ws.send_json(msg)
    except (asyncio.CancelledError, WebSocketDisconnect):
        pass
    except Exception as e:
        _log(SERVICE, f"[ws] Log push error: {e}", level="WARNING")


async def _push_local_log_loop(ws: WebSocket, get_sub):
    """Push new local log lines at ~150ms intervals."""
    try:
        while True:
            await asyncio.sleep(LOCAL_LOG_PUSH_INTERVAL)
            sub: Optional[LocalLogSub] = get_sub()
            if not sub or not _streamer:
                continue
            new_lines = _streamer.read_local_log_delta(sub)
            if not new_lines:
                continue
            msg: dict = {
                "type": "local_log_lines",
                "file": sub.name,
                "lines": new_lines,
            }
            if sub.sid is not None:
                msg["sid"] = sub.sid
            await ws.send_json(msg)
    except (asyncio.CancelledError, WebSocketDisconnect):
        pass
    except Exception as e:
        _log(SERVICE, f"[ws] Local log push error: {e}", level="WARNING")


# ── Helpers ──────────────────────────────────────────────────

async def _send_full_state(ws: WebSocket):
    """Build and send the complete state snapshot."""
    if not _monitor:
        return
    conn_summary = _monitor.pool.get_status_summary()
    local_logs = _streamer.list_local_logs() if _streamer else []
    state = _monitor.store.get_full_state(conn_summary, local_logs)
    await ws.send_json({"type": "state", "data": state})


# ── Command handlers ─────────────────────────────────────────

async def _cmd_restart_service(request: dict) -> dict:
    host = request.get("host", "")
    service = request.get("service", "")
    if not host or not service or not _monitor:
        return {"type": "error", "error": "host and service required"}
    success = await _monitor._restart_service(host, service)
    return {
        "type": "result", "cmd": "restart_service",
        "host": host, "service": service, "success": success,
    }


async def _cmd_get_logs(request: dict) -> dict:
    host = request.get("host", "")
    service = request.get("service", "")
    lines_n = request.get("lines", 200)
    sid = request.get("sid")
    if not host or not service or not _streamer:
        return {"type": "error", "error": "host and service required"}

    if service.startswith("Bot:"):
        parts = service[4:].strip().split(":")
        bot_name = parts[0]
        pb_version = parts[1] if len(parts) > 1 else None
        content = await _streamer.get_bot_log(
            host, bot_name, lines_n, pb_version
        )
    else:
        content = await _streamer.get_recent_logs(host, service, lines_n)

    resp: dict = {
        "type": "logs", "host": host, "service": service,
        "lines": (content or "").splitlines(),
    }
    if sid is not None:
        resp["sid"] = sid
    return resp


async def _cmd_get_log_info(request: dict) -> dict:
    host = request.get("host", "")
    service = request.get("service", "")
    if not host or not service or not _streamer:
        return {"type": "error", "error": "host and service required"}
    info = await _streamer.get_log_info(host, service)
    return {
        "type": "log_info", "host": host, "service": service,
        "size": info["size"] if info else None,
    }


async def _cmd_subscribe_logs(ws: WebSocket,
                              request: dict) -> tuple[Optional[str], Optional[str]]:
    """Start remote log stream + send initial chunk. Returns (stream_id, sid)."""
    host = request.get("host", "")
    service = request.get("service", "")
    sid = request.get("sid")
    if not host or not service or not _streamer:
        await ws.send_json({"type": "error",
                            "error": "host and service required"})
        return None, None

    # Resolve bot log path if needed
    resolved_service = service
    if service.startswith("Bot:"):
        parts = service[4:].strip().split(":")
        bot_name = parts[0]
        pb_version = parts[1] if len(parts) > 1 else None
        resolved_service = resolve_bot_log_path(
            bot_name, pb_version or "7"
        )

    stream_id = await _streamer.start_stream(host, resolved_service)
    if not stream_id:
        await ws.send_json({
            "type": "error",
            "error": f"Failed to start log stream for {service} on {host}",
        })
        return None, None

    # Send initial chunk
    if service.startswith("Bot:"):
        parts = service[4:].strip().split(":")
        bot_name = parts[0]
        pb_version = parts[1] if len(parts) > 1 else None
        content = await _streamer.get_bot_log(host, bot_name, 100, pb_version)
    else:
        content = await _streamer.get_recent_logs(host, service, 100)

    resp: dict = {
        "type": "logs", "host": host, "service": service,
        "lines": (content or "").splitlines(), "streaming": True,
    }
    if sid is not None:
        resp["sid"] = sid
    await ws.send_json(resp)

    # Drain any lines the tail -f worker buffered during the initial fetch
    # to prevent duplicates (tail -f starts immediately, initial fetch is
    # a separate SSH command that may overlap).
    if _streamer:
        _streamer.read_stream(stream_id, max_lines=9999)

    return stream_id, sid


async def _cmd_kill_instance(request: dict) -> dict:
    host = request.get("host", "")
    name = request.get("name", "")
    pb_version = request.get("pb_version", "")
    if not host or not name or not _monitor:
        return {"type": "error", "error": "host and name required"}
    result = await _monitor.kill_instance(host, name, pb_version)
    return {
        "type": "result", "cmd": "kill_instance",
        "host": host, "name": name,
        "success": result["success"], "pid": result["pid"],
    }


def _cmd_set_setting(request: dict):
    key = request.get("key", "")
    value = request.get("value", "")
    if key in _UI_SETTINGS_KEYS:
        save_ini("vps_monitor_ui", key, str(value))
        _log(SERVICE, f"[setting] {key} = {value}")


def _cmd_get_local_logs(request: dict) -> dict:
    filename = request.get("file", "")
    lines_n = int(request.get("lines", 200))
    sid = request.get("sid")
    if not _streamer:
        return {"type": "error", "error": "streamer not available"}
    content, file_size = _streamer.get_local_logs(filename, lines_n)
    resp: dict = {
        "type": "local_logs", "file": filename,
        "lines": content, "file_size": file_size,
    }
    if sid is not None:
        resp["sid"] = sid
    return resp


async def _cmd_subscribe_local_logs(ws: WebSocket,
                                    request: dict) -> Optional[LocalLogSub]:
    """Subscribe to local log streaming. Returns LocalLogSub."""
    filename = request.get("file", "")
    lines_n = int(request.get("lines", 200))
    sid = request.get("sid")
    fp = local_logs_dir() / filename
    logs_root = local_logs_dir().resolve()

    if not filename or not fp.resolve().is_relative_to(logs_root):
        await ws.send_json({
            "type": "error", "error": "Local log file not found",
        })
        return None

    content = tail_file(fp, lines_n) if fp.exists() else []
    try:
        file_size = fp.stat().st_size
    except Exception:
        file_size = 0

    resp: dict = {
        "type": "local_logs", "file": filename,
        "lines": content, "streaming": True, "file_size": file_size,
    }
    if sid is not None:
        resp["sid"] = sid
    await ws.send_json(resp)

    try:
        pos = fp.stat().st_size
    except Exception:
        pos = 0
    return LocalLogSub(file=fp, name=filename, pos=pos, sid=sid)
