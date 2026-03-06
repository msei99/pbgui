"""
FastAPI Server for PBGui.

Proof-of-Concept: Job monitoring without Streamlit reruns.

Usage:
    python api_server.py
    # or with reload:
    uvicorn api_server:app --reload --host 127.0.0.1 --port 8000
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import asyncio
import json
import logging
import warnings
from pathlib import Path

from api.jobs import router as jobs_router
from api.market_data import router as market_data_router
from api.heatmap import router as heatmap_router
from api.vps import router as vps_router
from logging_helpers import human_log as _log

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
    """Replace uvicorn's default handlers with our human_log bridge.

    Also captures Python warnings (DeprecationWarning etc.) so they
    appear in PBApiServer.log instead of raw stderr.
    """
    handler = _UvicornLogHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access", "py.warnings"):
        logger = logging.getLogger(name)
        logger.handlers = [handler]
        logger.propagate = False
    # Redirect all warnings.warn() calls (incl. DeprecationWarning) through logging
    logging.captureWarnings(True)


# ── VPS monitoring lifecycle ─────────────────────────────────

_vps_monitor = None


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

    yield  # app runs here

    if _vps_monitor:
        await _vps_monitor.stop()


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

# CORS: Allow Streamlit (port 8501) to call FastAPI (port 8000)
# CORS: Allow all origins for remote access (Streamlit can be on any host/IP)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow remote access from any host
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API Routes
app.include_router(jobs_router, prefix="/api/jobs", tags=["jobs"])
app.include_router(market_data_router, prefix="/api", tags=["market-data"])
app.include_router(heatmap_router, prefix="/api/heatmap", tags=["heatmap"])
app.include_router(vps_router, tags=["vps"])

# Static files (Vanilla JS frontend)
frontend_dir = Path(__file__).parent / "frontend"
if frontend_dir.exists():
    app.mount("/app", StaticFiles(directory=str(frontend_dir), html=True), name="frontend")


# WebSocket: Job live updates
active_connections: list[WebSocket] = []


@app.websocket("/ws/jobs")
async def websocket_jobs(websocket: WebSocket):
    """WebSocket endpoint for real-time job updates.
    
    Requires token in query param: ws://localhost:8000/ws/jobs?token=xxx
    Pushes job list every 2 seconds while connected.
    """
    await websocket.accept()
    
    # Validate token
    from api.auth import validate_token
    token = websocket.query_params.get("token")
    session = validate_token(token) if token else None
    
    if not session:
        await websocket.send_json({"error": "Invalid or missing token"})
        await websocket.close(code=1008)  # Policy violation
        return
    
    active_connections.append(websocket)
    
    try:
        while True:
            from task_queue import list_jobs
            
            # Get active jobs (pending + running)
            jobs = list_jobs(states=["pending", "running"], limit=50)
            
            # Send to client
            await websocket.send_json({
                "type": "jobs",
                "data": jobs,
                "timestamp": asyncio.get_event_loop().time()
            })
            
            # Wait 2s before next push
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
    """WebSocket endpoint for real-time market data status updates.
    
    Requires token + exchange in query params: ws://localhost:8000/ws/market-data?token=xxx&exchange=binanceusdm
    Pushes status updates every 2 seconds while connected.
    """
    await websocket.accept()
    
    # Validate token
    from api.auth import validate_token
    token = websocket.query_params.get("token")
    session = validate_token(token) if token else None
    
    if not session:
        await websocket.send_json({"error": "Invalid or missing token"})
        await websocket.close(code=1008)  # Policy violation
        return
    
    # Get exchange parameter
    exchange = websocket.query_params.get("exchange", "").lower().strip()
    if not exchange:
        await websocket.send_json({"error": "Missing exchange parameter"})
        await websocket.close(code=1008)
        return
    
    active_connections.append(websocket)
    
    try:
        while True:
            from api.market_data import _get_exchange_status_key, _get_exchange_flag_prefix, _load_market_data_status
            from pathlib import Path
            from datetime import datetime
            
            status_key = _get_exchange_status_key(exchange)
            flag_prefix = _get_exchange_flag_prefix(exchange)
            
            if status_key and flag_prefix:
                # Load status
                all_status = _load_market_data_status()
                exchange_status = all_status.get(status_key, {})
                
                # Check flags
                pbgdir = Path(__file__).parent
                flag_path = pbgdir / "data" / "logs" / f"{flag_prefix}_run_now.flag"
                queued = flag_path.exists()
                running = bool(exchange_status.get("running", False))
                
                # Build coin rows with calculated next_run times
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
                                # Handle both ISO format and space-separated format
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
                
                # Send to client
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
                await websocket.send_json({
                    "error": f"Unknown exchange: {exchange}"
                })
            
            # Wait 2s before next push
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
    """WebSocket endpoint: sends {type: 'updated', mtime: float} when data files change.

    Query params: token, exchange, dataset, coin
    """
    from api.auth import verify_token
    from api.heatmap import _latest_mtime

    token = websocket.query_params.get("token", "")
    try:
        verify_token(token)
    except Exception:
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
        "version": "0.1.0",
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


if __name__ == "__main__":
    import os
    import sys
    from PBApiServer import PBApiServer

    server = PBApiServer()
    if server.is_running():
        _log(SERVICE, 'Error: API server already started', level='ERROR')
        sys.exit(1)
    server.save_pid()

    # Bind to 0.0.0.0 for remote access (configurable via env var)
    host = os.getenv("PBGUI_API_HOST", "0.0.0.0")
    port = int(os.getenv("PBGUI_API_PORT", "8000"))

    _log(SERVICE, f"Starting PBGui API Server on {host}:{port}")
    _log(SERVICE, f"Docs: http://{host}:{port}/docs")
    _log(SERVICE, f"Frontend: http://{host}:{port}/app/jobs_monitor.html")

    _setup_uvicorn_logging()

    uvicorn.run(
        app,
        host=host,
        port=port,
        log_level="info",
        log_config=None,  # Prevent uvicorn from overriding our handlers
    )
