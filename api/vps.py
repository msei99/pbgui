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
import re
import time
import traceback
from collections import deque
from dataclasses import dataclass
from datetime import date, datetime, timezone
from time import mktime
from typing import Any, Optional

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends, Query, Request, HTTPException
from fastapi.responses import HTMLResponse

from api.auth import SessionToken, authenticate_websocket, require_auth
from MonitorConfig import MonitorConfig
from pbgui_purefunc import load_ini, save_ini
from logging_helpers import human_log as _log
from master.async_monitor import VPSMonitor
from master.async_logs import (
    AsyncLogStreamer, LocalLogSub, MAX_REMOTE_LOG_LINES, resolve_bot_log_path,
    local_logs_dir, normalize_remote_log_lines, tail_file,
    resolve_local_log_path,
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
MAX_LOCAL_FILTER_LENGTH = 256
MAX_LOCAL_FILTER_TERMS = 8
MAX_LOCAL_FILTER_TERM_LENGTH = 64
_ANSI_ESCAPE_RE = re.compile(r"\x1b(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")


def _normalize_log_request_lines(value: object, default: int = 200) -> int:
    """Return a bounded viewer request, including legacy ``All`` value zero."""
    lines = normalize_remote_log_lines(value, default=default)
    return MAX_REMOTE_LOG_LINES if lines == 0 else lines


@dataclass
class _RemoteLogSubscription:
    """Track one WebSocket's atomically replaceable remote stream ownership."""

    generation: int = 0
    stream_id: Optional[str] = None
    sid: Optional[str] = None
    ready: bool = False

    def invalidate(self) -> tuple[int, Optional[str]]:
        """Clear the active pair and return the new generation plus prior stream."""
        self.generation += 1
        old_stream_id = self.stream_id
        self.stream_id = None
        self.sid = None
        self.ready = False
        return self.generation, old_stream_id

    def register(self, generation: int, stream_id: str, sid: Optional[str]) -> bool:
        """Claim a created stream only if this operation still owns the generation."""
        if generation != self.generation:
            return False
        self.stream_id = stream_id
        self.sid = sid
        self.ready = False
        return True

    def activate(self, generation: int, stream_id: str) -> None:
        """Expose an owned stream to the push loop after snapshot handoff."""
        if generation == self.generation and stream_id == self.stream_id:
            self.ready = True

    def current(self) -> tuple[Optional[str], Optional[str]]:
        """Return the active stream/SID pair as one state read."""
        return (self.stream_id, self.sid) if self.ready else (None, None)


@dataclass
class _LocalLogFilterState:
    """Bounded search state attached to one local log cursor."""

    mode: str
    values: tuple[str, ...]
    context: int
    limit: int
    source_matches: deque[bool]
    pending: deque[tuple[int, str]]
    source_base: int = 0
    next_line_no: int = 1
    match_count: int = 0
    last_sent_line_no: int = 0
    trailing: int = 0


def _parse_local_log_filter(value: object) -> Optional[tuple[str, tuple[str, ...], int]]:
    """Validate a literal or bounded OR-term filter supplied by the viewer."""
    if not isinstance(value, dict):
        return None
    try:
        context = int(value.get("context", 5))
    except (TypeError, ValueError) as exc:
        raise ValueError("Invalid local log filter context") from exc
    if context < 0 or context > 20:
        raise ValueError("Local log filter context must be between 0 and 20")
    mode = str(value.get("mode") or "literal")
    if mode == "literal":
        query = str(value.get("query") or "").strip()
        if not query:
            return None
        if len(query) > MAX_LOCAL_FILTER_LENGTH or any(ord(char) < 32 for char in query):
            raise ValueError("Invalid local log filter query")
        return mode, (query.casefold(),), context
    if mode == "terms":
        raw_terms = value.get("terms")
        if not isinstance(raw_terms, list) or not 1 <= len(raw_terms) <= MAX_LOCAL_FILTER_TERMS:
            raise ValueError("Invalid local log filter terms")
        terms: list[str] = []
        for raw_term in raw_terms:
            term = str(raw_term or "").strip()
            if not term or len(term) > MAX_LOCAL_FILTER_TERM_LENGTH or any(ord(char) < 32 for char in term):
                raise ValueError("Invalid local log filter term")
            terms.append(term.casefold())
        return mode, tuple(terms), context
    raise ValueError("Invalid local log filter mode")


def _local_filter_matches(state: _LocalLogFilterState, line: str) -> bool:
    """Match one ANSI-stripped line without evaluating client-provided regex."""
    text = _ANSI_ESCAPE_RE.sub("", line).casefold()
    if state.mode == "literal":
        return state.values[0] in text
    return any(term in text for term in state.values)


def _initialize_local_filter(
    lines: list[str],
    limit: int,
    config: tuple[str, tuple[str, ...], int],
) -> tuple[_LocalLogFilterState, list[dict[str, object]]]:
    """Build compact match/context records from the newest source-line window."""
    mode, values, context = config
    state = _LocalLogFilterState(
        mode=mode,
        values=values,
        context=context,
        limit=limit,
        source_matches=deque(maxlen=limit),
        pending=deque(maxlen=max(1, context)),
        next_line_no=len(lines) + 1,
    )
    matches = [_local_filter_matches(state, line) for line in lines]
    state.source_matches.extend(matches)
    state.match_count = sum(matches)
    visible = bytearray(len(lines))
    for index, matched in enumerate(matches):
        if not matched:
            continue
        start = max(0, index - context)
        end = min(len(lines), index + context + 1)
        visible[start:end] = b"\x01" * (end - start)
    records = [
        {"line": lines[index], "line_no": index + 1, "match": matches[index]}
        for index in range(len(lines))
        if visible[index]
    ]
    if context:
        state.pending.extend(
            (index + 1, lines[index])
            for index in range(max(0, len(lines) - context), len(lines))
        )
        last_match = max((index for index, matched in enumerate(matches) if matched), default=-1)
        if last_match >= 0:
            state.trailing = max(0, context - (len(lines) - last_match - 1))
    if records:
        state.last_sent_line_no = int(records[-1]["line_no"])
    return state, records


def _filter_local_log_delta(
    state: _LocalLogFilterState,
    lines: list[str],
) -> tuple[list[dict[str, object]], int]:
    """Filter live records while retaining enough history for leading context."""
    records: list[dict[str, object]] = []
    for line in lines:
        line_no = state.next_line_no
        state.next_line_no += 1
        matched = _local_filter_matches(state, line)
        if len(state.source_matches) == state.limit:
            if state.source_matches.popleft():
                state.match_count -= 1
            state.source_base += 1
        state.source_matches.append(matched)
        if matched:
            state.match_count += 1
            for pending_no, pending_line in state.pending:
                if pending_no > state.last_sent_line_no:
                    records.append({"line": pending_line, "line_no": pending_no, "match": False})
                    state.last_sent_line_no = pending_no
            if line_no > state.last_sent_line_no:
                records.append({"line": line, "line_no": line_no, "match": True})
                state.last_sent_line_no = line_no
            state.trailing = state.context
        else:
            if state.trailing and line_no > state.last_sent_line_no:
                records.append({"line": line, "line_no": line_no, "match": False})
                state.last_sent_line_no = line_no
                state.trailing -= 1
        if state.context:
            state.pending.append((line_no, line))
    source_start = state.source_base + 1
    while state.pending and state.pending[0][0] < source_start:
        state.pending.popleft()
    return records, source_start


def _local_optional_service_blocker(service: str) -> str:
    return ""


def _host_usage_threshold(total_bytes: float, free_threshold_mb: float) -> Optional[float]:
    total = float(total_bytes or 0)
    free_mb = float(free_threshold_mb or 0)
    if total <= 0:
        return None
    free_bytes = max(0.0, free_mb * 1024 * 1024)
    usage_pct = 100.0 - ((free_bytes / total) * 100.0)
    return max(0.0, min(100.0, usage_pct))


def _metric_thresholds(hostname: str, metric: str, *, bot_name: str = "") -> dict[str, Optional[float]]:
    cfg = MonitorConfig()
    metric = str(metric or "cpu").strip().lower()
    bot_name = str(bot_name or "").strip()
    if bot_name:
        if metric == "errors":
            return {
                "warning_threshold": float(cfg.error_warning_v7),
                "error_threshold": float(cfg.error_error_v7),
            }
        if metric == "tracebacks":
            return {
                "warning_threshold": float(cfg.traceback_warning_v7),
                "error_threshold": float(cfg.traceback_error_v7),
            }
        if metric == "pnl":
            return {"warning_threshold": None, "error_threshold": None}
        if metric == "memory":
            return {
                "warning_threshold": float(cfg.mem_warning_v7),
                "error_threshold": float(cfg.mem_error_v7),
            }
        if metric == "swap":
            return {
                "warning_threshold": float(cfg.swap_warning_v7),
                "error_threshold": float(cfg.swap_error_v7),
            }
        return {
            "warning_threshold": float(cfg.cpu_warning_v7),
            "error_threshold": float(cfg.cpu_error_v7),
        }

    if metric == "cpu":
        return {
            "warning_threshold": float(cfg.cpu_warning_server),
            "error_threshold": float(cfg.cpu_error_server),
        }

    metrics = (_monitor.store.system.get(hostname) if _monitor else None)
    if metric == "memory":
        total = float(getattr(metrics, "mem_total", 0) or 0)
        return {
            "warning_threshold": _host_usage_threshold(total, cfg.mem_warning_server),
            "error_threshold": _host_usage_threshold(total, cfg.mem_error_server),
        }
    if metric == "disk":
        total = float(getattr(metrics, "disk_total", 0) or 0)
        return {
            "warning_threshold": _host_usage_threshold(total, cfg.disk_warning_server),
            "error_threshold": _host_usage_threshold(total, cfg.disk_error_server),
        }
    if metric == "swap":
        total = float(getattr(metrics, "swap_total", 0) or 0)
        return {
            "warning_threshold": _host_usage_threshold(total, cfg.swap_warning_server),
            "error_threshold": _host_usage_threshold(total, cfg.swap_error_server),
        }
    return {"warning_threshold": None, "error_threshold": None}


def init(monitor: VPSMonitor, streamer: AsyncLogStreamer):
    """Called once at FastAPI startup to inject shared objects."""
    global _monitor, _streamer
    _monitor = monitor
    _streamer = streamer


def get_monitor() -> Optional[VPSMonitor]:
    """Return the shared VPSMonitor instance if startup has initialized it."""
    return _monitor


def _bot_service_parts(service: str) -> tuple[str, str]:
    """Return a bot name and runtime from the allowlisted service identifier."""
    raw = str(service or "").strip()
    name, separator, version = raw.rpartition(":")
    if separator and version.lower() in {"7", "v7", "pb7", "8", "v8", "pb8"}:
        normalized = "8" if version.lower() in {"8", "v8", "pb8"} else "7"
        return name, normalized
    return raw, "7"


async def get_bot_log_matches(hostname: str, bot_name: str, *, pb_version: str | None = None,
                              kind: str = "tracebacks", bucket: str,
                              expected_count: int | None = None, lines: int = 5000) -> list[str]:
    """Return filtered bot-log lines for popup display.

    Uses existing SSH log-tail reads only. It must not launch the old remote
    instance collector script because periodic monitor data now comes from the
    local monitor-agent cache.
    """
    if not _streamer or not hostname or not bot_name or bucket != "today":
        return []
    expected = max(int(expected_count or 0), 0)
    line_limit = max(int(lines or 0), expected * (100 if kind == "tracebacks" else 20), 500)
    line_limit = min(line_limit, 10_000)
    today_prefix = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def is_today(line: str) -> bool:
        return str(line or "").startswith(today_prefix)

    version = "8" if str(pb_version or "7").strip().lower() in {"8", "v8", "pb8"} else "7"
    discovered = []
    if _monitor:
        host_logs = _monitor.store.bot_logs.get(hostname) or {}
        discovered = list(host_logs.get(f"{version}:{bot_name}") or [])
        if not discovered and version == "7":
            discovered = list(host_logs.get(bot_name) or [])

    if kind == "tracebacks":
        paths = [
            path for path in discovered
            if str(path).endswith(("/passivbot_err.log", "/passivbot_err.log.old"))
        ]
        if not paths:
            paths = [
                f"data/run_v{version}/{bot_name}/passivbot_err.log.old",
                f"data/run_v{version}/{bot_name}/passivbot_err.log",
            ]
        output = await _streamer.get_recent_log_files(hostname, paths, line_limit)
        if output is None:
            return []
        matches: list[str] = []
        block: list[str] = []
        block_today = False

        def flush_block() -> None:
            if block_today and any("Traceback" in line for line in block):
                if matches:
                    matches.extend(["", "-----", ""])
                matches.extend(block)

        for raw_line in output.splitlines():
            line = raw_line.rstrip("\n")
            if re.match(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", line):
                flush_block()
                block = [line]
                block_today = is_today(line)
            elif block:
                block.append(line)
        flush_block()
        return matches[-line_limit:] if line_limit > 0 else matches

    paths = [path for path in discovered if f"/pb{version}/logs/" in f"/{str(path).lstrip('/')}" ]
    if not paths:
        paths = [f"software/pb{version}/logs/{bot_name}.log"]
    paths = list(dict.fromkeys(paths))
    matches = []
    # The monitor RPC accepts at most 32 files per request. Scan every batch,
    # including newer sessions beyond the first page of archived bot logs.
    for offset in range(0, len(paths), 32):
        output = await _streamer.get_recent_log_files(
            hostname, paths[offset:offset + 32], line_limit, contains=" ERROR ",
        )
        if output is None:
            return []
        matches.extend(line for line in output.splitlines() if is_today(line) and " ERROR " in line)
        matches = matches[-line_limit:]
    return matches


async def get_bot_log_tail(hostname: str, bot_name: str, *, pb_version: str = "7", lines: int = 500) -> str:
    """Return a bounded remote bot log tail through the shared SSH streamer."""

    if not _streamer or not hostname or not bot_name:
        return ""
    version = "8" if str(pb_version).strip().lower() in {"8", "v8", "pb8"} else "7"
    line_limit = max(1, min(int(lines or 500), 10_000))
    discovered: list[str] = []
    if _monitor:
        host_logs = _monitor.store.bot_logs.get(hostname) or {}
        discovered = list(host_logs.get(f"{version}:{bot_name}") or [])
        if not discovered and version == "7":
            discovered = list(host_logs.get(bot_name) or [])
    paths = [
        path for path in discovered
        if str(path).endswith((".log", ".log.old"))
    ]
    if not paths:
        paths = [
            f"data/run_v{version}/{bot_name}/passivbot_err.log.old",
            f"data/run_v{version}/{bot_name}/passivbot_err.log",
            f"software/pb{version}/logs/{bot_name}.log",
        ]
    output = await _streamer.get_recent_log_files(hostname, list(dict.fromkeys(paths)), line_limit)
    return str(output or "")


def get_monitor_state_snapshot() -> dict:
    """Return the same full-state snapshot used by the VPS Monitor WebSocket."""
    if not _monitor:
        return {
            "connections": {"total": 0, "connected": 0, "disconnected": 0, "auth_failed": 0, "connections": {}},
            "system": {},
            "instances": {},
            "v7_instances": {},
            "v8_instances": {},
            "host_meta": {},
            "streams": {},
            "services": {},
            "local_logs": [],
            "timestamp": time.time(),
            "ui_settings": {},
        }
    conn_summary = _monitor.pool.get_status_summary()
    local_logs = _streamer.list_local_logs() if _streamer else []
    return _monitor.store.get_full_state(conn_summary, local_logs)


def get_alert_snapshot() -> dict[str, Any]:
    if not _monitor:
        return {"items": [], "history": [], "summary": {"new_count": 0, "ack_count": 0, "total_active": 0}}
    return {
        "items": _monitor.list_active_alerts(gui_only=True),
        "history": _monitor.list_alert_history(gui_only=True),
        "summary": _monitor.get_alert_summary(),
    }


def get_metric_history_snapshot(hostname: str, *, bot_name: str = "", metric: str = "cpu") -> dict:
    """Return on-demand 24h metric history for a host or bot."""
    bot_name = str(bot_name or "").strip()
    metric = str(metric or "cpu").strip().lower()
    if bot_name and metric == "disk":
        metric = "cpu"
    if metric not in {"cpu", "memory", "disk", "swap", "errors", "tracebacks", "pnl"}:
        metric = "cpu"
    thresholds = _metric_thresholds(hostname, metric, bot_name=bot_name)
    if not _monitor:
        return {
            "available": False,
            "scope": "bot" if bot_name else "host",
            "metric": metric,
            "hostname": str(hostname or ""),
            "bot_name": str(bot_name or ""),
            "source": "cpu_60s" if metric == "cpu" else ("rss_mb" if metric == "memory" and bot_name else ("swap_mb" if metric == "swap" and bot_name else ("passivbot.log" if metric in {"errors", "pnl"} else ("passivbot_err.log" if metric == "tracebacks" else f"{metric}_percent")))),
            "step_seconds": 3600 if metric in {"errors", "tracebacks"} else (86400 if metric == "pnl" else 60),
            **({"window_hours": 24 * 28} if metric in {"errors", "tracebacks"} else ({"start_day": 0, "end_day": 0, "days": [], "cumulative_points": [], "fills_points": [], "total_pnl": 0.0, "total_fills": 0, "last_fill_ts": 0} if metric == "pnl" else {"window_minutes": 24 * 60})),
            "warning_threshold": thresholds["warning_threshold"],
            "error_threshold": thresholds["error_threshold"],
            **({"unit": "MB"} if bot_name and metric in {"memory", "swap"} else {"resolution_pct": 0.5}),
            **({"timezone_basis": "UTC", "daily_points": [], "daily_step_seconds": 86400} if metric in {"errors", "tracebacks"} else ({"timezone_basis": "UTC"} if metric == "pnl" else {})),
            "points": [],
        }
    if bot_name:
        payload = _monitor.get_bot_metric_history(hostname, bot_name, metric)
    else:
        payload = _monitor.get_host_metric_history(hostname, metric)
    payload["warning_threshold"] = thresholds["warning_threshold"]
    payload["error_threshold"] = thresholds["error_threshold"]
    return payload


def get_cpu_history_snapshot(hostname: str, *, bot_name: str = "") -> dict:
    """Backward-compatible CPU-only history accessor."""
    return get_metric_history_snapshot(hostname, bot_name=bot_name, metric="cpu")


@router.get("/api/vps/alerts")
def get_active_alerts(session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    return get_alert_snapshot()


@router.post("/api/vps/alerts/ack")
async def acknowledge_alert(request: Request, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    if not _monitor:
        raise HTTPException(status_code=503, detail="monitor not available")
    body = await request.json()
    alert_id = str(body.get("id") or "").strip()
    if not alert_id:
        raise HTTPException(status_code=400, detail="alert id required")
    ok = await asyncio.to_thread(_monitor.acknowledge_alert, alert_id)
    if not ok:
        raise HTTPException(status_code=404, detail="alert not found")
    snapshot = await asyncio.to_thread(get_alert_snapshot)
    return {"ok": True, **snapshot}


@router.post("/api/vps/alerts/ack-all")
def acknowledge_all_alerts(session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    if not _monitor:
        raise HTTPException(status_code=503, detail="monitor not available")
    updated = _monitor.acknowledge_all_alerts()
    payload = get_alert_snapshot()
    payload["ok"] = True
    payload["updated"] = updated
    return payload


# ── Allowed UI setting keys (whitelist) ──────────────────────

_UI_SETTINGS_KEYS = {"compact"}
# Keys stored in [vps_monitor] ini section (not [vps_monitor_ui])
_VPS_SETTINGS_KEYS = {"debug_logging"}


# ── Standalone page ──────────────────────────────────────────

@router.get("/api/vps/main_page", response_class=HTMLResponse)
def get_main_page(
    request: Request,
    session: SessionToken = Depends(require_auth),
) -> HTMLResponse:
    """Serve the standalone VPS Monitor page using cookie authentication."""
    from pathlib import Path as _P

    html_path = _P(__file__).parent.parent / "frontend" / "vps_monitor.html"
    html = html_path.read_text(encoding="utf-8")

    scheme = request.url.scheme
    host = request.url.hostname or "127.0.0.1"
    port = request.url.port
    origin = f"{scheme}://{host}" + (f":{port}" if port else "")
    ws_base = origin.replace("http://", "ws://").replace("https://", "wss://")

    html = html.replace('"%%WS_BASE%%"', json.dumps(ws_base))

    from pbgui_purefunc import PBGUI_VERSION
    from pbgui_purefunc import PBGUI_SERIAL
    html = html.replace('"%%VERSION%%"', json.dumps(PBGUI_VERSION))
    html = html.replace("%%VERSION%%", PBGUI_VERSION)
    html = html.replace('"%%SERIAL%%"', json.dumps(PBGUI_SERIAL))
    html = html.replace("%%SERIAL%%", PBGUI_SERIAL)

    nav_js = _P(__file__).parent.parent / "frontend" / "pbgui_nav.js"
    nav_hash = str(int(nav_js.stat().st_mtime)) if nav_js.exists() else PBGUI_VERSION
    html = html.replace("%%NAV_HASH%%", nav_hash)

    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})


# ── WebSocket endpoint ───────────────────────────────────────

@router.websocket("/ws/vps")
async def ws_vps(websocket: WebSocket):
    """
    Main VPS monitoring WebSocket.

    Authentication uses the HttpOnly session cookie.

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
    if await authenticate_websocket(websocket) is None:
        return

    state_updates = getattr(websocket, "query_params", {}).get("state", "1") != "0"
    _clients.add(websocket)
    _log(SERVICE, f"[ws] Client connected: {websocket.client}")

    # Per-client subscriptions
    remote_sub = _RemoteLogSubscription()
    local_sub: Optional[LocalLogSub] = None

    # Background push tasks for this client
    push_state_task = (
        asyncio.create_task(_push_state_loop(websocket))
        if state_updates else None
    )
    push_log_task = asyncio.create_task(
        _push_log_loop(websocket, remote_sub.current)
    )
    push_local_log_task = asyncio.create_task(
        _push_local_log_loop(websocket, lambda: local_sub)
    )

    try:
        if state_updates:
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
                generation, old_stream_id = remote_sub.invalidate()
                if old_stream_id and _streamer:
                    await _stop_remote_stream(old_stream_id)
                await _cmd_subscribe_logs(
                    websocket, request, remote_sub, generation
                )

            # ── unsubscribe_logs ──
            elif cmd == "unsubscribe_logs":
                _generation, old_stream_id = remote_sub.invalidate()
                if old_stream_id and _streamer:
                    await _stop_remote_stream(old_stream_id)

            # ── kill_instance ──
            elif cmd == "kill_instance":
                result = await _cmd_kill_instance(request)
                await websocket.send_json(result)

            # ── get_cpu_history ──
            elif cmd == "get_cpu_history":
                result = await _cmd_get_cpu_history(request)
                await websocket.send_json(result)

            elif cmd == "get_metric_history":
                result = await _cmd_get_metric_history(request)
                await websocket.send_json(result)

            # ── set_setting ──
            elif cmd == "set_setting":
                await _cmd_set_setting(request)

            # ── list_local_logs ──
            elif cmd == "list_local_logs":
                files = _streamer.list_local_logs() if _streamer else []
                await websocket.send_json({
                    "type": "local_logs_list", "files": files,
                })

            # ── get_local_logs (one-shot) ──
            elif cmd == "get_local_logs":
                result = await asyncio.to_thread(_cmd_get_local_logs, request)
                await websocket.send_json(result)

            # ── subscribe_local_logs ──
            elif cmd == "subscribe_local_logs":
                if local_sub:
                    local_sub.reset_cursor()
                    local_sub = None
                local_sub = await _cmd_subscribe_local_logs(
                    websocket, request
                )

            # ── unsubscribe_local_logs ──
            elif cmd == "unsubscribe_local_logs":
                if local_sub:
                    local_sub.reset_cursor()
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
        push_tasks = tuple(task for task in (
            push_state_task, push_log_task, push_local_log_task,
        ) if task is not None)
        for task in push_tasks:
            task.cancel()
        await asyncio.gather(*push_tasks, return_exceptions=True)
        # Daemon-backed streams use a bounded idle lease so an API restart does
        # not tear down their SSH channel before the browser reconnects.
        _generation, log_stream_id = remote_sub.invalidate()
        if log_stream_id and _streamer:
            detach = getattr(_streamer, "detach_stream", None)
            if callable(detach):
                detach(log_stream_id)
            else:
                _streamer.stop_stream(log_stream_id)
        if local_sub:
            local_sub.reset_cursor()
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


async def _push_log_loop(ws: WebSocket, get_subscription):
    """Push buffered remote log lines at ~150ms intervals."""
    try:
        while True:
            await asyncio.sleep(LOG_PUSH_INTERVAL)
            stream_id, sid = get_subscription()
            if not stream_id or not _streamer:
                continue
            read_async = getattr(_streamer, "read_stream_async", None)
            if callable(read_async):
                lines = await read_async(stream_id, max_lines=50)
            else:
                lines = _streamer.read_stream(stream_id, max_lines=50)
            if (stream_id, sid) != get_subscription():
                continue
            if not lines:
                continue
            status_async = getattr(_streamer, "get_stream_status_async", None)
            if callable(status_async):
                status = await status_async(stream_id)
            else:
                status = _streamer.get_stream_status(stream_id)
            if (stream_id, sid) != get_subscription():
                continue
            msg: dict = {
                "type": "log_lines",
                "lines": lines,
                "host": status.get("hostname", "") if status else "",
                "service": status.get("log_path", "") if status else "",
            }
            if sid is not None:
                msg["sid"] = sid
            if (stream_id, sid) != get_subscription():
                continue
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
            new_lines = await asyncio.to_thread(_streamer.read_local_log_delta, sub)
            if sub is not get_sub():
                continue
            if not new_lines:
                continue
            new_lines = new_lines[-MAX_REMOTE_LOG_LINES:]
            filter_state = getattr(sub, "_filter_state", None)
            if isinstance(filter_state, _LocalLogFilterState):
                records, source_start = await asyncio.to_thread(
                    _filter_local_log_delta, filter_state, new_lines,
                )
                msg: dict = {
                    "type": "local_log_filtered_lines",
                    "file": sub.name,
                    "records": records,
                    "source_start": source_start,
                    "source_end": filter_state.next_line_no - 1,
                    "match_count": filter_state.match_count,
                }
            else:
                msg = {
                    "type": "local_log_lines",
                    "file": sub.name,
                    "lines": new_lines,
                }
            if sub.sid is not None:
                msg["sid"] = sub.sid
            if sub is not get_sub():
                continue
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

async def _stop_remote_stream(stream_id: str) -> None:
    """Stop one remote log stream without blocking the API event loop."""
    if not _streamer:
        return
    stop_async = getattr(_streamer, "stop_stream_async", None)
    if callable(stop_async):
        await stop_async(stream_id)
    else:
        _streamer.stop_stream(stream_id)


async def _cmd_restart_service(request: dict) -> dict:
    host = request.get("host", "")
    service = request.get("service", "")
    if not host or not service:
        return {"type": "error", "error": "host and service required"}

    # ── Local restart via services API ──
    if host == "local":
        return await _local_restart_service(service)

    if not _monitor:
        return {"type": "error", "error": "monitor not available"}
    success = await _monitor._restart_service(host, service)
    return {
        "type": "result", "cmd": "restart_service",
        "host": host, "service": service, "success": success,
    }


# ── Service name mapping: LogViewer name → services API id ──
_SVC_NAME_MAP = {
    "PBRun": "pbrun", "PBCoinData": "pbcoindata",
    "PBData": "pbdata",
    "pbrun": "pbrun", "pbcoindata": "pbcoindata",
    "pbdata": "pbdata",
}


async def _local_restart_service(service: str) -> dict:
    """Restart a local service using the same stop/start mechanism as api/services.py."""
    from api.services import _service_action, _SERVICES
    svc_id = _SVC_NAME_MAP.get(service)
    if not svc_id or svc_id not in _SERVICES:
        return {"type": "result", "cmd": "restart_service",
                "host": "local", "service": service, "success": False,
                "error": f"Unknown local service: {service}"}
    blocker = _local_optional_service_blocker(service)
    if blocker:
        return {"type": "result", "cmd": "restart_service",
                "host": "local", "service": service, "success": False,
                "error": blocker}
    try:
        result = await asyncio.to_thread(_service_action, svc_id, "restart")
        _log(SERVICE, f"[local] Restarted service {service} ({svc_id})")
        return {"type": "result", "cmd": "restart_service",
                "host": "local", "service": service,
                "success": bool(result.get("running", True))}
    except Exception as e:
        _log(SERVICE, f"[local] Failed to restart {service}: {e}",
             level="ERROR", meta={"traceback": traceback.format_exc()})
        return {"type": "result", "cmd": "restart_service",
                "host": "local", "service": service, "success": False,
                "error": str(e)}


async def _local_kill_instance(name: str, pb_version: str) -> dict:
    """Kill a local bot instance via PBRun."""
    try:
        from pathlib import Path

        from PBRun import PBRun, RunV7, RunV8
        name = str(name or "").strip()
        if (not name or len(name) > 255 or name in {".", ".."}
                or "/" in name or "\\" in name or "\x00" in name
                or any(ord(char) < 32 or ord(char) == 127 for char in name)):
            return {"type": "result", "cmd": "kill_instance",
                    "host": "local", "name": name, "success": False, "pid": None}
        raw_version = str(pb_version or "7").strip().lower()
        if raw_version not in {"7", "v7", "pb7", "8", "v8", "pb8"}:
            return {"type": "result", "cmd": "kill_instance",
                    "host": "local", "name": name, "success": False, "pid": None}
        version = "8" if raw_version in {"8", "v8", "pb8"} else "7"
        pbrun = PBRun()
        runner = RunV8() if version == "8" else RunV7()
        runner.path = str(Path(pbrun.v8_path if version == "8" else pbrun.v7_path) / name)
        runner.user = name
        runner.name = pbrun.name
        runner.pbgdir = pbrun.pbgdir
        if version == "8":
            runner.pb8dir = pbrun.pb8dir
            runner.pb8venv = pbrun.pb8venv
        else:
            runner.pb7dir = pbrun.pb7dir
            runner.pb7venv = pbrun.pb7venv

        # Stopping must also work when the config no longer permits a start.
        process = runner.pid()
        if process is None:
            return {"type": "result", "cmd": "kill_instance",
                    "host": "local", "name": name,
                    "success": False, "pid": None}

        pid = process.pid
        runner.stop()
        _log(SERVICE, f"[local] Stopped PB{version} bot {name} (pid={pid})")
        return {"type": "result", "cmd": "kill_instance",
                "host": "local", "name": name,
                "success": True, "pid": pid}
    except Exception as e:
        _log(SERVICE, f"[local] Failed to kill {name}: {e}",
             level="ERROR", meta={"traceback": traceback.format_exc()})
        return {"type": "result", "cmd": "kill_instance",
                "host": "local", "name": name,
                "success": False, "pid": None}


async def _cmd_get_logs(request: dict) -> dict:
    host = request.get("host", "")
    service = request.get("service", "")
    sid = request.get("sid")
    if not host or not service or not _streamer:
        return {"type": "error", "error": "host and service required"}
    try:
        lines_n = _normalize_log_request_lines(request.get("lines"), default=200)
    except ValueError as exc:
        return {"type": "error", "error": str(exc)}

    try:
        if service.startswith("Bot:"):
            bot_name, pb_version = _bot_service_parts(service[4:])
            content = await _streamer.get_bot_log(
                host, bot_name, lines_n, pb_version
            )
        else:
            content = await _streamer.get_recent_logs(host, service, lines_n)
    except ValueError as exc:
        return {"type": "error", "error": str(exc)}

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
    sid = request.get("sid")
    if not host or not service or not _streamer:
        return {"type": "error", "error": "host and service required"}
    try:
        pb_version = None
        if service.startswith("Bot:"):
            _bot_name, pb_version = _bot_service_parts(service[4:])
        info = await _streamer.get_log_info(host, service, pb_version)
    except ValueError as exc:
        return {"type": "error", "error": str(exc)}
    response = {
        "type": "log_info", "host": host, "service": service,
        "size": info["size"] if info else None,
    }
    if sid is not None:
        response["sid"] = sid
    return response


async def _cmd_subscribe_logs(ws: WebSocket,
                              request: dict,
                              subscription: _RemoteLogSubscription,
                              generation: int) -> tuple[Optional[str], Optional[str]]:
    """Start remote log stream + send initial chunk. Returns (stream_id, sid)."""
    host = request.get("host", "")
    service = request.get("service", "")
    sid = request.get("sid")
    if not host or not service or not _streamer:
        await ws.send_json({"type": "error",
                            "error": "host and service required"})
        return None, None
    try:
        lines_n = _normalize_log_request_lines(request.get("lines"), default=200)
    except ValueError as exc:
        await ws.send_json({"type": "error", "error": str(exc)})
        return None, None

    try:
        # Resolve bot log path if needed
        resolved_service = service
        if service.startswith("Bot:"):
            bot_name, pb_version = _bot_service_parts(service[4:])
            resolved_service = resolve_bot_log_path(
                bot_name, pb_version
            )

        stream_id = await _streamer.start_stream(host, resolved_service)
    except ValueError as exc:
        await ws.send_json({"type": "error", "error": str(exc)})
        return None, None
    if not stream_id:
        await ws.send_json({
            "type": "error",
            "error": f"Failed to start log stream for {service} on {host}",
        })
        return None, None
    if not subscription.register(generation, stream_id, sid):
        await _stop_remote_stream(stream_id)
        return None, None

    # Send initial chunk
    start_at_end = bool(request.get("start_at_end"))
    if service.startswith("Bot:"):
        if start_at_end:
            content = ""
        else:
            bot_name, pb_version = _bot_service_parts(service[4:])
            content = await _streamer.get_bot_log(host, bot_name, lines_n, pb_version)
    else:
        content = "" if start_at_end else await _streamer.get_recent_logs(host, service, lines_n)

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
        read_async = getattr(_streamer, "read_stream_async", None)
        if callable(read_async):
            await read_async(stream_id, max_lines=9999)
        else:
            _streamer.read_stream(stream_id, max_lines=9999)

    subscription.activate(generation, stream_id)
    return stream_id, sid


async def _cmd_kill_instance(request: dict) -> dict:
    host = request.get("host", "")
    name = request.get("name", "")
    pb_version = request.get("pb_version", "")
    if not host or not name:
        return {"type": "error", "error": "host and name required"}

    # ── Local kill ──
    if host == "local":
        return await _local_kill_instance(name, pb_version)

    if not _monitor:
        return {"type": "error", "error": "monitor not available"}
    result = await _monitor.kill_instance(host, name, pb_version)
    return {
        "type": "result", "cmd": "kill_instance",
        "host": host, "name": name,
        "success": result["success"], "pid": result["pid"],
    }


async def _cmd_set_setting(request: dict):
    key = request.get("key", "")
    value = request.get("value", "")
    if key in _UI_SETTINGS_KEYS:
        await asyncio.to_thread(save_ini, "vps_monitor_ui", key, str(value))
        if _monitor:
            _monitor.store.set_ui_setting(key, str(value))
        _log(SERVICE, f"[setting] {key} = {value}")
    elif key in _VPS_SETTINGS_KEYS:
        await asyncio.to_thread(save_ini, "vps_monitor", key, str(value))
        if _monitor:
            _monitor.store.set_ui_setting(key, str(value))
            # Apply live — avoids waiting for next ini-watcher cycle
            if key == "debug_logging":
                _monitor._debug_logging = (str(value).lower() == "true")
        _log(SERVICE, f"[setting] {key} = {value}")


def _cmd_get_local_logs(request: dict) -> dict:
    filename = request.get("file", "")
    sid = request.get("sid")
    if not _streamer:
        return {"type": "error", "error": "streamer not available"}
    try:
        lines_n = _normalize_log_request_lines(request.get("lines"), default=200)
    except ValueError as exc:
        return {"type": "error", "error": str(exc)}
    content, file_size = _streamer.get_local_logs(filename, lines_n)
    resp: dict = {
        "type": "local_logs", "file": filename,
        "lines": content, "file_size": file_size,
    }
    if sid is not None:
        resp["sid"] = sid
    if request.get("purpose") == "download":
        resp["purpose"] = "download"
        resp["request_id"] = request.get("request_id")
    return resp


async def _cmd_get_cpu_history(request: dict) -> dict:
    host = str(request.get("host") or "").strip()
    bot_name = str(request.get("bot_name") or "").strip()
    sid = request.get("sid")
    if not host:
        return {"type": "error", "error": "host required", "cmd": "get_cpu_history"}
    payload = await asyncio.to_thread(get_cpu_history_snapshot, host, bot_name=bot_name)
    resp: dict = {
        "type": "cpu_history",
        "cmd": "get_cpu_history",
        "host": host,
        "bot_name": bot_name,
        "data": payload,
    }
    if sid is not None:
        resp["sid"] = sid
    return resp


async def _cmd_get_metric_history(request: dict) -> dict:
    host = str(request.get("host") or "").strip()
    bot_name = str(request.get("bot_name") or "").strip()
    metric = str(request.get("metric") or "cpu").strip().lower()
    sid = request.get("sid")
    if not host:
        return {"type": "error", "error": "host required", "cmd": "get_metric_history"}
    payload = await asyncio.to_thread(
        get_metric_history_snapshot,
        host,
        bot_name=bot_name,
        metric=metric,
    )
    resp: dict = {
        "type": "metric_history",
        "cmd": "get_metric_history",
        "host": host,
        "bot_name": bot_name,
        "metric": metric,
        "data": payload,
    }
    if sid is not None:
        resp["sid"] = sid
    return resp


async def _cmd_subscribe_local_logs(ws: WebSocket,
                                    request: dict) -> Optional[LocalLogSub]:
    """Subscribe to local log streaming. Returns LocalLogSub."""
    filename = request.get("file", "")
    sid = request.get("sid")
    start_at_end = bool(request.get("start_at_end"))
    try:
        lines_n = _normalize_log_request_lines(request.get("lines"), default=200)
        filter_config = _parse_local_log_filter(request.get("filter"))
    except ValueError as exc:
        await ws.send_json({"type": "error", "error": str(exc)})
        return None
    fp = resolve_local_log_path(filename) if filename else None

    if fp is None:
        await ws.send_json({
            "type": "error", "error": "Local log file not found",
        })
        return None

    content, file_size, sub = await asyncio.to_thread(
        AsyncLogStreamer.initialize_local_log_subscription,
        fp, filename, lines_n, sid, start_at_end=start_at_end,
    )

    if filter_config is not None:
        filter_state, records = await asyncio.to_thread(
            _initialize_local_filter, content, lines_n, filter_config,
        )
        sub._filter_state = filter_state
        resp: dict = {
            "type": "local_logs_filtered", "file": filename,
            "records": records, "streaming": True, "file_size": file_size,
            "source_start": 1, "source_end": len(content),
            "match_count": filter_state.match_count,
        }
    else:
        resp = {
            "type": "local_logs", "file": filename,
            "lines": content, "streaming": True, "file_size": file_size,
        }
    if sid is not None:
        resp["sid"] = sid
    await ws.send_json(resp)
    return sub
