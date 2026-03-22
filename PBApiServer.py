"""
PBApiServer — FastAPI REST + WebSocket server for PBGui.

Provides:
- REST API for job operations (list, cancel, delete, retry, requeue)
- WebSocket endpoint for real-time job updates
- Static file serving for Vanilla JS frontend
- Token-based authentication

Runs as a background daemon, following the same pattern as PBRun etc.

Usage (same pattern as all other PB services):
    python PBApiServer.py          # start the server directly
    uvicorn PBApiServer:app        # for development with reload
"""

import asyncio
import json
import logging
import os
import subprocess
import sys
import warnings
from contextlib import asynccontextmanager
from pathlib import Path, PurePath
from time import sleep

import psutil
import uvicorn
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from api.dashboard import router as dashboard_router
from api.dashboards import router as dashboards_router
from api.jobs import router as jobs_router
from api.market_data import router as market_data_router
from api.heatmap import router as heatmap_router
from api.vps import router as vps_router
from logging_helpers import human_log as _log
from pbgui_purefunc import PBGDIR, load_ini, save_ini

SERVICE = "PBApiServer"


# ── Route uvicorn logs through human_log ─────────────────────

class _UvicornLogHandler(logging.Handler):
    """Bridges Python logging → human_log for uvicorn access/error logs."""

    _LEVEL_MAP = {
        logging.DEBUG: "DEBUG",
        logging.INFO: "INFO",
        logging.WARNING: "WARNING",
        logging.ERROR: "ERROR",
        logging.CRITICAL: "CRITICAL",
    }

    def emit(self, record: logging.LogRecord):
        try:
            msg = self.format(record)
            level = self._LEVEL_MAP.get(record.levelno, "INFO")
            _log(SERVICE, msg, level=level)
        except Exception:
            pass


def _setup_uvicorn_logging():
    """Replace uvicorn's default handlers with our human_log bridge."""
    handler = _UvicornLogHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access", "py.warnings"):
        logger = logging.getLogger(name)
        logger.handlers = [handler]
        logger.propagate = False
    logging.captureWarnings(True)


# ── VPS monitoring lifecycle ─────────────────────────────────

_vps_monitor = None

# ── Task-worker watchdog ──────────────────────────────────────

_WATCHDOG_INTERVAL_S = 60  # check every 60 seconds


async def _worker_watchdog_loop() -> None:
    """Periodically restart task_worker if it's dead but jobs are waiting.

    This prevents the queue from silently stalling when the worker process
    crashes or is killed without cleanly transitioning its jobs back to
    pending state (e.g. SIGKILL). The fix mirrors the auto-restart logic
    that exists in the old Streamlit polling panel but runs at the API-server
    level so it fires regardless of which UI is open.
    """
    from task_queue import list_jobs, read_worker_pid, is_pid_running, clear_worker_pid
    _log(SERVICE, "[watchdog] task-worker watchdog started", level="INFO")
    while True:
        try:
            await asyncio.sleep(_WATCHDOG_INTERVAL_S)
            active = list_jobs(states=["pending", "running"], limit=1)
            if not active:
                continue
            pid = read_worker_pid()
            if pid and is_pid_running(int(pid)):
                continue
            # Worker is dead but jobs are queued — restart it.
            try:
                clear_worker_pid()
                subprocess.Popen(
                    [sys.executable, str(Path(__file__).resolve().parent / "task_worker.py")],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    close_fds=True,
                )
                _log(
                    SERVICE,
                    "[watchdog] auto-restarted task_worker (worker dead, active jobs found)",
                    level="WARNING",
                )
            except Exception as e:
                _log(SERVICE, f"[watchdog] failed to restart task_worker: {e}", level="ERROR")
        except asyncio.CancelledError:
            _log(SERVICE, "[watchdog] task-worker watchdog stopped", level="INFO")
            return
        except Exception as e:
            _log(SERVICE, f"[watchdog] unexpected error: {e}", level="ERROR")


@asynccontextmanager
async def _lifespan(app: FastAPI):
    """FastAPI lifespan: startup and graceful shutdown of VPS monitoring."""
    global _vps_monitor
    from master.async_monitor import VPSMonitor
    from master.async_logs import AsyncLogStreamer
    from api.vps import init as vps_init

    monitor = VPSMonitor()
    streamer = AsyncLogStreamer(monitor.pool)
    vps_init(monitor, streamer)
    _vps_monitor = monitor
    await monitor.start()

    watchdog_task = asyncio.create_task(_worker_watchdog_loop(), name="worker-watchdog")

    yield  # app runs here

    watchdog_task.cancel()
    try:
        await watchdog_task
    except asyncio.CancelledError:
        pass
    if _vps_monitor:
        await _vps_monitor.stop()


# ── FastAPI app ───────────────────────────────────────────────

app = FastAPI(
    lifespan=_lifespan,
    title="PBGui API",
    description=(
        "REST API + WebSocket for PBGui backend services.\n\n"
        "## Authentication\n"
        "All endpoints require an API token, passed as `?token=xxx` query param "
        "or `Authorization: Bearer xxx` header.\n\n"
        "---\n\n"
        "## WebSocket — VPS Monitor (`/ws/vps`)\n"
        "Real-time VPS monitoring and log streaming.\n\n"
        "**Server → Client push messages:**\n"
        "- `{\"type\": \"state\", \"data\": {…}}` — full VPS state (connections, system metrics, instances, services)\n"
        "- `{\"type\": \"log_lines\", \"lines\": [...]}` — incremental remote log lines\n"
        "- `{\"type\": \"local_log_lines\", \"lines\": [...]}` — incremental local log lines\n\n"
        "**Client → Server commands:**\n"
        "- `{\"cmd\": \"get_logs\", \"host\": …, \"service\": …, \"lines\": 200}`\n"
        "- `{\"cmd\": \"subscribe_logs\", \"host\": …, \"service\": …}` / `unsubscribe_logs`\n"
        "- `{\"cmd\": \"restart_service\", \"host\": …, \"service\": …}`\n"
        "- `{\"cmd\": \"kill_instance\", \"host\": …, \"name\": …}`\n"
        "- `{\"cmd\": \"list_local_logs\"}` / `get_local_logs` / `subscribe_local_logs` / `unsubscribe_local_logs`\n\n"
        "---\n\n"
        "## WebSocket — Jobs (`/ws/jobs`)\n"
        "Pushes job queue state every 2 s. Auth via `?token=xxx`.\n\n"
        "**Server → Client:** `{\"jobs\": [...]}` — full list of jobs (pending, running, done, failed).\n\n"
        "---\n\n"
        "## WebSocket — Market Data (`/ws/market-data`)\n"
        "Pushes per-exchange status every 2 s. Auth via `?token=xxx`.\n\n"
        "**Query params:** `exchange` (required).\n\n"
        "**Server → Client:** `{\"status\": {…}}` — daemon status, progress, cycle info.\n\n"
        "---\n\n"
        "## WebSocket — Heatmap Watch (`/ws/heatmap-watch`)\n"
        "Watches data file mtimes and notifies when heatmap data changes. Auth via `?token=xxx`.\n\n"
        "**Query params:** `exchange`, `dataset`, `coin` (all required).\n\n"
        "**Server → Client:** `{\"type\": \"updated\", \"mtime\": …}` — sent when the underlying data files change (polls every 5 s).\n"
    ),
    version="1.65",
    openapi_tags=[
        {"name": "jobs", "description": "Background job queue — list, cancel, retry, delete, bulk-delete, view logs"},
        {"name": "market-data", "description": "Market data pipeline — status, trigger refresh, cancel, stop"},
        {"name": "heatmap", "description": "Gap / coverage heatmap — info, overview (with SSE progress streaming), minute-detail, mtime check"},
        {"name": "vps", "description": "VPS monitoring WebSocket (`/ws/vps`) — live metrics, log streaming, service control"},
    ],
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(dashboard_router, prefix="/api/dashboard", tags=["dashboard"])
app.include_router(dashboards_router, prefix="/api/dashboards", tags=["dashboards"])
app.include_router(jobs_router, prefix="/api/jobs", tags=["jobs"])
app.include_router(market_data_router, prefix="/api", tags=["market-data"])
app.include_router(heatmap_router, prefix="/api/heatmap", tags=["heatmap"])
app.include_router(vps_router, tags=["vps"])

frontend_dir = Path(__file__).parent / "frontend"
if frontend_dir.exists():
    app.mount("/app", StaticFiles(directory=str(frontend_dir), html=True), name="frontend")


# ── WebSocket endpoints ───────────────────────────────────────

active_connections: list[WebSocket] = []

# Set of active /ws/dashboard connections (notified by PBData via internal POST)
dashboard_ws_clients: set[WebSocket] = set()


@app.websocket("/ws/jobs")
async def websocket_jobs(websocket: WebSocket):
    """WebSocket endpoint for real-time job updates."""
    from api.auth import validate_token
    token = websocket.query_params.get("token", "")
    if not validate_token(token):
        await websocket.close(code=4001)
        return
    await websocket.accept()
    active_connections.append(websocket)
    try:
        while True:
            from task_queue import list_jobs
            jobs = list_jobs(states=["pending", "running"], limit=50)
            await websocket.send_json({
                "type": "jobs",
                "data": jobs,
                "timestamp": asyncio.get_event_loop().time()
            })
            await asyncio.sleep(2)
    except WebSocketDisconnect:
        if websocket in active_connections:
            active_connections.remove(websocket)
    except Exception as e:
        _log(SERVICE, f"[ws/jobs] Error: {e}", level="ERROR")
        if websocket in active_connections:
            active_connections.remove(websocket)


@app.websocket("/ws/market-data")
async def websocket_market_data(websocket: WebSocket):
    """WebSocket endpoint for real-time market data status updates."""
    from api.auth import validate_token
    token = websocket.query_params.get("token", "")
    if not validate_token(token):
        await websocket.close(code=4001)
        return
    await websocket.accept()
    exchange = websocket.query_params.get("exchange", "").lower().strip()
    if not exchange:
        await websocket.send_json({"error": "Missing exchange parameter"})
        await websocket.close(code=1008)
        return
    active_connections.append(websocket)
    try:
        while True:
            from api.market_data import _get_exchange_status_key, _get_exchange_flag_prefix, _load_market_data_status
            from datetime import datetime
            status_key = _get_exchange_status_key(exchange)
            flag_prefix = _get_exchange_flag_prefix(exchange)
            if status_key and flag_prefix:
                all_status = _load_market_data_status()
                exchange_status = all_status.get(status_key, {})
                pbgdir = Path(__file__).parent
                flag_path = pbgdir / "data" / "logs" / f"{flag_prefix}_run_now.flag"
                queued = flag_path.exists()
                running = bool(exchange_status.get("running", False))
                coins_data = exchange_status.get("coins", {})
                interval_s = int(exchange_status.get("interval_seconds", 0))
                coin_rows = []
                if isinstance(coins_data, dict):
                    now = datetime.now()
                    for coin, cst in sorted(coins_data.items()):
                        if not isinstance(cst, dict):
                            continue
                        last_fetch = str(cst.get("last_fetch") or "")
                        next_run = ""
                        if interval_s and last_fetch:
                            try:
                                last_fetch_clean = last_fetch.replace(" ", "T") if " " in last_fetch else last_fetch
                                last_dt = datetime.fromisoformat(last_fetch_clean)
                                next_run = max(0, int(interval_s - (now - last_dt).total_seconds()))
                            except Exception:
                                pass
                        api_res = cst.get("api_result", {})
                        coin_rows.append({
                            "coin": coin,
                            "last_fetch": last_fetch,
                            "result": cst.get("result", ""),
                            "lookback_days": cst.get("lookback_days", ""),
                            "minutes_written": api_res.get("minutes_written", "") if isinstance(api_res, dict) else "",
                            "newest_day": cst.get("newest_day", ""),
                            "next_run_in_s": next_run,
                            "note": cst.get("note") or cst.get("error") or "",
                        })
                await websocket.send_json({
                    "type": "market_data_status",
                    "exchange": exchange,
                    "running": running,
                    "queued": queued,
                    "coins_done": int(exchange_status.get("coins_done", 0)),
                    "coins_total": int(exchange_status.get("coins_total", 0)),
                    "current_coin": exchange_status.get("current_coin", ""),
                    "interval_seconds": interval_s,
                    "coin_rows": coin_rows,
                    "timestamp": asyncio.get_event_loop().time()
                })
            else:
                await websocket.send_json({"error": f"Unknown exchange: {exchange}"})
            await asyncio.sleep(2)
    except WebSocketDisconnect:
        if websocket in active_connections:
            active_connections.remove(websocket)
    except Exception as e:
        _log(SERVICE, f"[ws/market-data] Error: {e}", level="ERROR")
        if websocket in active_connections:
            active_connections.remove(websocket)


@app.websocket("/ws/heatmap-watch")
async def websocket_heatmap_watch(websocket: WebSocket):
    """WebSocket endpoint: sends {type: 'updated', mtime: float} when data files change."""
    from api.auth import validate_token
    from api.heatmap import _latest_mtime
    token = websocket.query_params.get("token", "")
    if not validate_token(token):
        await websocket.close(code=4001)
        return
    exchange = websocket.query_params.get("exchange", "")
    dataset = websocket.query_params.get("dataset", "")
    coin = websocket.query_params.get("coin", "")
    await websocket.accept()
    last_mtime: float = 0.0
    try:
        while True:
            current = _latest_mtime(exchange, dataset, coin)
            if current != last_mtime:
                last_mtime = current
                await websocket.send_json({"type": "updated", "mtime": current})
            await asyncio.sleep(5)
    except WebSocketDisconnect:
        pass
    except Exception as e:
        _log(SERVICE, f"[ws/heatmap-watch] Error: {e}", level="ERROR")


@app.websocket("/ws/dashboard")
async def websocket_dashboard(websocket: WebSocket):
    """WebSocket endpoint for dashboard live updates.

    Clients receive {"type": "balance_updated"} whenever PBData writes
    fresh balance/position data to the database.
    """
    from api.auth import validate_token
    token = websocket.query_params.get("token", "")
    if not validate_token(token):
        await websocket.close(code=4001)
        return
    await websocket.accept()
    dashboard_ws_clients.add(websocket)
    try:
        while True:
            await asyncio.sleep(60)  # keep-alive; real pushes come from /api/internal/notify/balance
    except WebSocketDisconnect:
        pass
    except Exception as e:
        _log(SERVICE, f"[ws/dashboard] Error: {e}", level="ERROR")
    finally:
        dashboard_ws_clients.discard(websocket)


@app.websocket("/ws/candles")
async def websocket_candles(websocket: WebSocket):
    """WebSocket endpoint for live chart updates (Phase 2).

    Clients subscribe by connecting with ?token=...&user=...&symbol=...&tf=...
    They receive pre-formatted messages:
      {"type": "candle",   "candle":   [t,o,h,l,c,v]}
      {"type": "position", "position": {entry,size,upnl,side} | null}
      {"type": "orders",   "orders":   [{price,amount,side}, ...]}
    Data comes from ccxt.pro live streams when available, with polling fallback.
    """
    from api.auth import validate_token
    from api.dashboard import (register_chart_client, unregister_chart_client,
                                _set_event_loop)
    token = websocket.query_params.get("token", "")
    if not validate_token(token):
        await websocket.close(code=4001)
        return

    user = websocket.query_params.get("user", "")
    symbol = websocket.query_params.get("symbol", "")
    tf = websocket.query_params.get("tf", "4h")

    if not user or not symbol:
        await websocket.close(code=4002)
        return

    await websocket.accept()

    # Store event loop reference for the polling thread
    _set_event_loop(asyncio.get_running_loop())

    # One unified asyncio.Queue receives candle, position, and order messages
    q: asyncio.Queue = asyncio.Queue(maxsize=200)
    await register_chart_client(user, symbol, tf, q)

    try:
        while True:
            try:
                msg = await asyncio.wait_for(q.get(), timeout=30)
                await websocket.send_json(msg)
            except asyncio.TimeoutError:
                # No data for 30s — send heartbeat to detect dead connections
                try:
                    await asyncio.wait_for(
                        websocket.send_json({"type": "ping"}), timeout=5
                    )
                except Exception:
                    break
    except WebSocketDisconnect:
        pass
    except Exception as e:
        _log(SERVICE, f"[ws/candles] Error: {e}", level="ERROR")
    finally:
        await unregister_chart_client(user, symbol, tf, q)


@app.post("/api/internal/notify/balance")
async def internal_notify_balance(request: Request):
    """Internal endpoint called by PBData after writing balance/position data.

    Broadcasts {"type": "balance_updated"} to all connected /ws/dashboard clients.
    Only accepts requests from localhost.
    """
    client_host = request.client.host if request.client else ""
    if client_host not in ("127.0.0.1", "::1", "localhost"):
        raise HTTPException(status_code=403, detail="Internal endpoint")
    dead: set[WebSocket] = set()
    for ws in list(dashboard_ws_clients):
        try:
            await ws.send_json({"type": "balance_updated"})
        except Exception:
            dead.add(ws)
    dashboard_ws_clients.difference_update(dead)
    return {"ok": True, "notified": len(dashboard_ws_clients)}


@app.post("/api/internal/notify/income")
async def internal_notify_income(request: Request):
    """Internal endpoint called by PBData after writing history/income data.

    Broadcasts {"type": "income_updated", "user": "<name>"} to all /ws/dashboard clients.
    Clients filter by their configured user list — only reload when relevant.
    Only accepts requests from localhost.
    """
    client_host = request.client.host if request.client else ""
    if client_host not in ("127.0.0.1", "::1", "localhost"):
        raise HTTPException(status_code=403, detail="Internal endpoint")
    try:
        body = await request.json()
        user_name = body.get("user", "") if isinstance(body, dict) else ""
    except Exception:
        user_name = ""
    dead: set[WebSocket] = set()
    for ws in list(dashboard_ws_clients):
        try:
            await ws.send_json({"type": "income_updated", "user": user_name})
        except Exception:
            dead.add(ws)
    dashboard_ws_clients.difference_update(dead)
    return {"ok": True, "notified": len(dashboard_ws_clients)}


@app.post("/api/nav/request")
async def nav_request(request: Request):
    """Universal navigation bridge.

    Widget iframes POST ``{page: "...", params: {...}}`` here.
    The payload is broadcast as ``{type: "nav_request", ...}`` to all
    ``/ws/dashboard`` WebSocket clients.  A tiny Streamlit component
    (``nav_bridge``) listens for this message type and calls
    ``st.switch_page()`` on the Python side.
    """
    from api.auth import validate_token as _vt

    # Accept token from header or body
    auth = request.headers.get("Authorization", "")
    token = auth.replace("Bearer ", "") if auth.startswith("Bearer ") else ""
    body: dict = {}
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")
    if not token:
        token = body.get("token", "")
    if not _vt(token):
        raise HTTPException(status_code=401, detail="Unauthorized")

    page = body.get("page", "")
    action = body.get("action", "")
    params = body.get("params", {})
    if not page and not action:
        raise HTTPException(status_code=400, detail="Missing 'page' or 'action'")

    if action:
        _valid_actions = {
            'select_dashboard', 'new_dashboard', 'edit_dashboard',
            'save_dashboard', 'cancel_edit', 'delete_dashboard', 'refresh',
        }
        if action not in _valid_actions:
            raise HTTPException(status_code=400, detail="Invalid action")
        payload = {"type": "dashboard_action", "action": action, "params": params}
    else:
        payload = {"type": "nav_request", "page": page, "params": params}
    dead: set[WebSocket] = set()
    for ws in list(dashboard_ws_clients):
        try:
            await ws.send_json(payload)
        except Exception:
            dead.add(ws)
    dashboard_ws_clients.difference_update(dead)
    return {"ok": True, "notified": len(dashboard_ws_clients)}


# ── Docs endpoints ────────────────────────────────────────────

@app.get("/api/docs/index")
async def docs_index(lang: str = "EN", token: str = ""):
    """Return the list of help topics for the given language.

    Returns ``[{title: str, file: str}, ...]`` where ``file`` is the bare
    filename (e.g. ``00_overview.md``) and ``title`` is the first ``#``
    heading or the filename.
    """
    from api.auth import validate_token
    if not validate_token(token):
        raise HTTPException(status_code=401, detail="Unauthorized")

    ln = str(lang or "EN").strip().upper()
    root = Path(__file__).parent / "docs"
    folder = "help_de" if ln == "DE" else "help"
    docs_dir = root / folder
    if not docs_dir.is_dir():
        return []

    result = []
    for p in sorted(docs_dir.glob("*.md")):
        title = p.name
        try:
            with open(p, "r", encoding="utf-8") as f:
                first = f.readline().strip()
            if first.startswith("#"):
                title = first.lstrip("#").strip() or p.name
        except Exception:
            pass
        result.append({"title": title, "file": p.name})
    return result


@app.get("/api/docs/content")
async def docs_content(file: str, lang: str = "EN", token: str = ""):
    """Return the raw Markdown text for a help file.

    ``file`` must be a bare filename (no path separators), ``*.md`` only,
    and must exist in the appropriate ``docs/help[_de]/`` directory.
    """
    from api.auth import validate_token
    if not validate_token(token):
        raise HTTPException(status_code=401, detail="Unauthorized")

    # Reject path traversal and non-markdown files
    safe_name = Path(file).name
    if safe_name != file or not safe_name.endswith(".md") or "/" in file or "\\" in file:
        raise HTTPException(status_code=400, detail="Invalid file parameter")

    ln = str(lang or "EN").strip().upper()
    root = Path(__file__).parent / "docs"
    folder = "help_de" if ln == "DE" else "help"
    full_path = (root / folder / safe_name).resolve()

    # Must remain within the docs dir (additional safety)
    if not str(full_path).startswith(str((root / folder).resolve())):
        raise HTTPException(status_code=400, detail="Invalid file parameter")

    if not full_path.is_file():
        raise HTTPException(status_code=404, detail="File not found")

    try:
        content = full_path.read_text(encoding="utf-8")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Read error: {e}")

    return {"content": content}


# ── REST endpoints ────────────────────────────────────────────

@app.get("/static/plotly.min.js")
def serve_plotly_js():
    """Serve Plotly.js from the local Python package (no CDN needed)."""
    import plotly as _plotly
    path = Path(_plotly.__file__).parent / "package_data" / "plotly.min.js"
    return FileResponse(str(path), media_type="application/javascript")


@app.get("/")
def root():
    """API root endpoint."""
    return {
        "service": "PBGui API",
        "version": "1.65",
        "endpoints": {
            "jobs": "/api/jobs/",
            "websocket": "ws://localhost:8000/ws/jobs",
            "frontend": "/app/jobs_monitor.html"
        }
    }


@app.get("/health")
def health():
    """Health check endpoint."""
    return {
        "status": "ok",
        "websocket_clients": len(active_connections)
    }


class PBApiServer:
    """
    FastAPI server daemon for PBGui.

    Lifecycle (same pattern as other services):
        api = PBApiServer()
        api.run()        # Start daemon in background
        api.stop()       # Stop daemon
        api.is_running() # Check if running
        api.restart()    # Stop + start
    """

    def __init__(self):
        self.piddir = Path(f'{PBGDIR}/data/pid')
        if not self.piddir.exists():
            self.piddir.mkdir(parents=True)
        self.pidfile = Path(f'{self.piddir}/api_server.pid')
        self.my_pid = None

        # Configuration (persisted in pbgui.ini)
        self._host = None
        self._port = None

    # ── Configuration properties (persisted in pbgui.ini) ──

    @property
    def host(self) -> str:
        """Bind address (0.0.0.0 = all interfaces, 127.0.0.1 = localhost only)."""
        if self._host is None:
            val = load_ini("api_server", "host")
            self._host = val.strip() if val and val.strip() else "0.0.0.0"
        return self._host

    @host.setter
    def host(self, value: str):
        if self._host != value:
            self._host = value.strip() if value else "0.0.0.0"
            save_ini("api_server", "host", self._host)

    @property
    def port(self) -> int:
        """API server port (default: 8000)."""
        if self._port is None:
            val = load_ini("api_server", "port")
            self._port = int(val) if val and val.isdigit() else 8000
        return self._port

    @port.setter
    def port(self, value: int):
        if self._port != value:
            self._port = max(1024, min(65535, value))
            save_ini("api_server", "port", str(self._port))

    # ── PID management (same pattern as PBRun, PBData, etc.) ──

    def load_pid(self):
        """Load PID from pidfile into self.my_pid."""
        if self.pidfile.exists():
            with open(self.pidfile) as f:
                pid = f.read().strip()
                try:
                    self.my_pid = int(pid) if pid.isnumeric() else None
                except ValueError:
                    self.my_pid = None

    def save_pid(self):
        """Write current process PID to pidfile. Called from the daemon process."""
        self.my_pid = os.getpid()
        tmp_path = self.pidfile.with_suffix(self.pidfile.suffix + '.tmp')
        with tmp_path.open('w', encoding='utf-8') as f:
            f.write(str(self.my_pid))
        tmp_path.replace(self.pidfile)

    # ── Daemon lifecycle ──

    def is_running(self) -> bool:
        """Check if the API server daemon is running."""
        self.load_pid()
        try:
            if self.my_pid and psutil.pid_exists(self.my_pid) and any(
                sub.lower().endswith("pbapiserver.py") for sub in psutil.Process(self.my_pid).cmdline()
            ):
                return True
        except psutil.NoSuchProcess:
            pass
        return False

    def run(self):
        """Start the API server daemon in the background."""
        if not self.is_running():
            # Small delay + re-check to avoid double-spawn on rapid Streamlit reruns
            sleep(0.3)
            if self.is_running():
                return
            pbgdir = Path.cwd()
            venv_python = self._get_venv_python()
            cmd = [venv_python, '-u', str(PurePath(f'{pbgdir}/PBApiServer.py'))]

            # Set environment variables for config
            env = os.environ.copy()
            env["PBGUI_API_HOST"] = self.host
            env["PBGUI_API_PORT"] = str(self.port)

            subprocess.Popen(
                cmd,
                stdout=None,
                stderr=None,
                cwd=pbgdir,
                text=True,
                env=env,
                start_new_session=True,
            )
            count = 0
            while True:
                if count > 5:
                    _log(SERVICE, 'Error: Can not start API server', level='ERROR')
                    break
                sleep(2)
                if self.is_running():
                    break
                count += 1

    def stop(self):
        """Stop the API server daemon."""
        if self.is_running():
            _log(SERVICE, 'Stop: API server', level='INFO')
            try:
                psutil.Process(self.my_pid).terminate()
                psutil.Process(self.my_pid).wait(timeout=5)
            except psutil.TimeoutExpired:
                try:
                    psutil.Process(self.my_pid).kill()
                except psutil.NoSuchProcess:
                    pass
            except psutil.NoSuchProcess:
                pass
            self.pidfile.unlink(missing_ok=True)

    def restart(self):
        """Restart the API server daemon (stop if running, then start)."""
        if self.is_running():
            self.stop()
        self.run()

    def _get_venv_python(self) -> str:
        """Get the path to the virtual environment Python executable."""
        venv_candidates = [
            Path(f"{PBGDIR}/../venv_pbgui/bin/python"),
            Path(f"{PBGDIR}/../venv_pbgui312/bin/python"),
            Path(f"{PBGDIR}/../venv/bin/python"),
        ]
        for venv_py in venv_candidates:
            if venv_py.exists():
                # Return the venv path (not .resolve()) so that the
                # venv's site-packages are on sys.path at runtime.
                return str(venv_py)
        return sys.executable


if __name__ == "__main__":
    server = PBApiServer()
    if server.is_running():
        _log(SERVICE, 'Already running — exit', level='INFO')
        sys.exit(0)
    server.save_pid()

    host = os.getenv("PBGUI_API_HOST", server.host)
    port = int(os.getenv("PBGUI_API_PORT", str(server.port)))

    _log(SERVICE, f"Starting PBGui API Server on {host}:{port}")
    _log(SERVICE, f"Docs: http://{host}:{port}/docs")
    _log(SERVICE, f"Frontend: http://{host}:{port}/app/jobs_monitor.html")

    _setup_uvicorn_logging()

    uvicorn.run(
        app,
        host=host,
        port=port,
        log_level="info",
        log_config=None,
    )
