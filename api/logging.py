"""
FastAPI router: Logging Monitor endpoints.

Provides REST API for:
- Listing log files with sizes and rotated variants
- Getting / saving log rotation settings (defaults + per-service)
- Purging a log file
- Serving the standalone Logging Monitor page
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException, Depends, Query, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from api.auth import require_auth, SessionToken
from logging_helpers import (
    human_log as _log,
    purge_log_to_rotated,
    get_rotate_defaults,
    set_rotate_defaults,
    get_rotate_settings,
    set_rotate_settings,
)

SERVICE = "ApiLogging"

router = APIRouter()

_LOGS_DIR = Path(__file__).parent.parent / "data" / "logs"


# ── Pydantic models ───────────────────────────────────────────

class RotationSaveIn(BaseModel):
    """Body for POST /rotation — saves one rotation rule."""
    scope: str       # "default" or service/log-stem name
    max_mb: int
    backup_count: int


# ── Endpoints ─────────────────────────────────────────────────

@router.get("/")
def list_log_files(session: SessionToken = Depends(require_auth)) -> dict:
    """List all .log files with sizes and rotated variants."""
    if not _LOGS_DIR.exists():
        return {"files": [], "sizes": {}, "rotated": {}}

    files = sorted(p.name for p in _LOGS_DIR.glob("*.log") if p.is_file())
    sizes: dict[str, int] = {}
    rotated: dict[str, list[str]] = {}

    for name in files:
        p = _LOGS_DIR / name
        try:
            sizes[name] = p.stat().st_size
        except Exception:
            sizes[name] = 0

        variants: list[str] = []
        for i in range(1, 20):
            rp = _LOGS_DIR / f"{name}.{i}"
            if rp.is_file():
                variants.append(rp.name)
                try:
                    sizes[rp.name] = rp.stat().st_size
                except Exception:
                    sizes[rp.name] = 0
            else:
                break
        old_p = _LOGS_DIR / f"{name}.old"
        if old_p.is_file():
            variants.append(old_p.name)
            try:
                sizes[old_p.name] = old_p.stat().st_size
            except Exception:
                pass
        if variants:
            rotated[name] = variants

    return {"files": files, "sizes": sizes, "rotated": rotated}


@router.get("/rotation")
def get_rotation(session: SessionToken = Depends(require_auth)) -> dict:
    """Get rotation settings: default and per-log-file overrides."""
    default_max_bytes, default_backup_count = get_rotate_defaults()

    per_service: dict[str, dict] = {}
    if _LOGS_DIR.exists():
        for p in sorted(_LOGS_DIR.glob("*.log")):
            service = p.stem
            max_bytes, backup_count = get_rotate_settings(service=service)
            per_service[service] = {
                "max_mb": max(1, max_bytes // (1024 * 1024)),
                "backup_count": backup_count,
            }

    return {
        "default": {
            "max_mb": max(1, default_max_bytes // (1024 * 1024)),
            "backup_count": default_backup_count,
        },
        "per_service": per_service,
    }


@router.post("/rotation")
def save_rotation(
    body: RotationSaveIn,
    session: SessionToken = Depends(require_auth),
) -> dict:
    """Save rotation settings for 'default' or a specific service/log-stem."""
    max_bytes = max(1, int(body.max_mb)) * 1024 * 1024
    backup_count = max(1, int(body.backup_count))

    if body.scope == "default":
        set_rotate_defaults(max_bytes, backup_count)
        _log(SERVICE, f"Default rotation updated: max={body.max_mb} MB, files={backup_count}", level="INFO")
    else:
        set_rotate_settings(body.scope, max_bytes, backup_count)
        _log(SERVICE, f"Rotation for '{body.scope}' updated: max={body.max_mb} MB, files={backup_count}", level="INFO")

    return {"success": True}


@router.post("/purge/{filename}")
def purge_logfile(
    filename: str,
    session: SessionToken = Depends(require_auth),
) -> dict:
    """Purge (rotate-and-truncate) the specified log file."""
    # Security: reject path traversal attempts
    if "/" in filename or "\\" in filename or ".." in filename:
        raise HTTPException(status_code=400, detail="Invalid filename")
    if not (filename.endswith(".log") or ".log." in filename):
        raise HTTPException(status_code=400, detail="Only .log files are allowed")

    path = _LOGS_DIR / filename
    if not path.exists():
        raise HTTPException(status_code=404, detail="Log file not found")

    success, msg = purge_log_to_rotated(str(path), 10 * 1024 * 1024)
    if not success:
        raise HTTPException(status_code=500, detail=msg)

    _log(SERVICE, f"Purged log file '{filename}': {msg}", level="INFO")
    return {"success": True, "message": msg}


@router.get("/main_page", response_class=HTMLResponse)
def get_main_page(
    request: Request,
    st_base: str = Query(default="", description="Browser-visible Streamlit base URL"),
    session: SessionToken = Depends(require_auth),
) -> HTMLResponse:
    """Serve the standalone Logging Monitor page with token injected server-side."""
    html_path = Path(__file__).parent.parent / "frontend" / "logging_monitor.html"
    html = html_path.read_text(encoding="utf-8")

    # Derive API origin from the actual request URL
    scheme = request.url.scheme
    host = request.url.hostname or "127.0.0.1"
    port = request.url.port
    origin = f"{scheme}://{host}" + (f":{port}" if port else "")
    api_logging_base = origin + "/api/logging"

    html = html.replace('"%%TOKEN%%"', json.dumps(session.token))
    html = html.replace('"%%API_BASE%%"', json.dumps(api_logging_base))

    if not st_base:
        st_base = f"http://{host}:8501"
    html = html.replace('"%%ST_BASE%%"', json.dumps(st_base))

    from pbgui_func import PBGUI_VERSION  # local import to avoid circular
    from pbgui_purefunc import PBGUI_SERIAL
    html = html.replace('"%%VERSION%%"', json.dumps(PBGUI_VERSION))
    html = html.replace("%%VERSION%%", PBGUI_VERSION)
    html = html.replace('"%%SERIAL%%"', json.dumps(PBGUI_SERIAL))
    html = html.replace("%%SERIAL%%", PBGUI_SERIAL)

    nav_js = Path(__file__).parent.parent / "frontend" / "pbgui_nav.js"
    nav_hash = str(int(nav_js.stat().st_mtime)) if nav_js.exists() else PBGUI_VERSION
    html = html.replace("%%NAV_HASH%%", nav_hash)

    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})
