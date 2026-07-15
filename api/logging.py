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
import re
import traceback
from typing import Optional

from fastapi import APIRouter, HTTPException, Depends, Query, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from api.auth import require_auth, SessionToken
import logging_helpers
from ini_settings import apply_metadata
from logging_helpers import (
    human_log as _log,
    purge_log_to_rotated,
    get_rotate_defaults,
    set_rotate_defaults,
    get_rotate_settings,
    set_rotate_settings,
    get_managed_scope_settings,
    set_managed_scope_settings,
)

SERVICE = "ApiLogging"

router = APIRouter()

_BASE_LOG_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*\.log$")
_RETIRED_LOG_STEMS = {
    "PBRemote", "PBMon", "sync", "FastAPI", "FileSync", "PBStat",
    "V7ConfigSync", "config_archives", "Auth", "LiveSession",
    "ApiKeyState", "User",
}


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
    if not logging_helpers.LOG_ROOT.exists():
        return {"files": [], "sizes": {}, "rotated": {}}

    files = sorted(p.name for p in logging_helpers.LOG_ROOT.glob("*.log") if p.is_file())
    sizes: dict[str, int] = {}
    rotated: dict[str, list[str]] = {}

    for name in files:
        p = logging_helpers.LOG_ROOT / name
        try:
            sizes[name] = p.stat().st_size
        except Exception:
            sizes[name] = 0

        variants: list[str] = []
        _, backup_count = get_rotate_settings(logfile=str(p))
        for i in range(1, backup_count + 1):
            rp = logging_helpers.LOG_ROOT / f"{name}.{i}"
            if rp.is_file():
                variants.append(rp.name)
                try:
                    sizes[rp.name] = rp.stat().st_size
                except Exception:
                    sizes[rp.name] = 0
        old_p = logging_helpers.LOG_ROOT / f"{name}.old"
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
    if logging_helpers.LOG_ROOT.exists():
        for p in sorted(logging_helpers.LOG_ROOT.glob("*.log")):
            if logging_helpers.resolve_managed_log_scope(p):
                continue
            service = p.stem
            if service in _RETIRED_LOG_STEMS:
                continue
            max_bytes, backup_count = get_rotate_settings(logfile=str(p))
            per_service[service] = {
                "max_mb": max(1, max_bytes // (1024 * 1024)),
                "backup_count": backup_count,
            }

    managed_scopes = {}
    for scope_id, definition in logging_helpers.MANAGED_LOG_SCOPES.items():
        max_bytes, backup_count = get_managed_scope_settings(scope_id)
        managed_scopes[scope_id] = {
            "label": definition["label"],
            "description": definition["description"],
            "max_mb": max(1, max_bytes // (1024 * 1024)),
            "backup_count": backup_count,
        }
    return {
        "default": {
            "max_mb": max(1, default_max_bytes // (1024 * 1024)),
            "backup_count": default_backup_count,
        },
        "per_service": per_service,
        "managed_scopes": managed_scopes,
        "apply": apply_metadata("logging_rotation"),
    }


@router.post("/rotation")
def save_rotation(
    body: RotationSaveIn,
    session: SessionToken = Depends(require_auth),
) -> dict:
    """Save rotation settings for 'default' or a specific service/log-stem."""
    max_bytes = max(1, int(body.max_mb)) * 1024 * 1024
    backup_count = max(0, int(body.backup_count))

    if body.scope == "default":
        set_rotate_defaults(max_bytes, backup_count)
        _log(SERVICE, f"Default rotation updated: max={body.max_mb} MB, files={backup_count}", level="INFO")
    elif body.scope.startswith("managed:"):
        scope_id = body.scope.removeprefix("managed:")
        try:
            set_managed_scope_settings(scope_id, max_bytes, backup_count)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        _log(SERVICE, f"Managed rotation for '{scope_id}' updated: max={body.max_mb} MB, files={backup_count}", level="INFO")
    else:
        set_rotate_settings(body.scope, max_bytes, backup_count)
        _log(SERVICE, f"Rotation for '{body.scope}' updated: max={body.max_mb} MB, files={backup_count}", level="INFO")

    return {"success": True, "apply": apply_metadata("logging_rotation")}


@router.post("/purge/{filename}")
def purge_logfile(
    filename: str,
    session: SessionToken = Depends(require_auth),
) -> dict:
    """Purge (rotate-and-truncate) the specified log file."""
    try:
        if not _BASE_LOG_NAME_RE.fullmatch(filename):
            raise HTTPException(status_code=400, detail="Invalid log filename")

        root = logging_helpers.LOG_ROOT.resolve()
        path = (root / filename).resolve()
        if path.parent != root:
            raise HTTPException(status_code=400, detail="Invalid log filename")
        if not path.is_file():
            raise HTTPException(status_code=404, detail="Log file not found")

        max_bytes, _backup_count = get_rotate_settings(logfile=str(path))
        success, msg = purge_log_to_rotated(str(path), max_bytes)
        if not success:
            raise HTTPException(status_code=500, detail=msg)

        _log(SERVICE, f"Purged log file '{filename}': {msg}", level="INFO")
        return {"success": True, "message": msg}
    except HTTPException:
        raise
    except Exception as exc:
        _log(
            SERVICE,
            f"Failed to purge log file '{filename}': {exc}",
            level="ERROR",
            meta={"traceback": traceback.format_exc(), "operation": "purge_log"},
        )
        raise HTTPException(status_code=500, detail="Failed to purge log file") from exc


@router.get("/main_page", response_class=HTMLResponse)
def get_main_page(
    request: Request,
    session: SessionToken = Depends(require_auth),
) -> HTMLResponse:
    """Serve the standalone Logging Monitor page using cookie authentication."""
    html_path = Path(__file__).parent.parent / "frontend" / "logging_monitor.html"
    html = html_path.read_text(encoding="utf-8")

    # Derive API origin from the actual request URL
    scheme = request.url.scheme
    host = request.url.hostname or "127.0.0.1"
    port = request.url.port
    origin = f"{scheme}://{host}" + (f":{port}" if port else "")
    api_logging_base = origin + "/api/logging"

    html = html.replace('"%%API_BASE%%"', json.dumps(api_logging_base))

    from pbgui_purefunc import PBGUI_VERSION
    from pbgui_purefunc import PBGUI_SERIAL
    html = html.replace('"%%VERSION%%"', json.dumps(PBGUI_VERSION))
    html = html.replace("%%VERSION%%", PBGUI_VERSION)
    html = html.replace('"%%SERIAL%%"', json.dumps(PBGUI_SERIAL))
    html = html.replace("%%SERIAL%%", PBGUI_SERIAL)

    nav_js = Path(__file__).parent.parent / "frontend" / "pbgui_nav.js"
    nav_hash = str(int(nav_js.stat().st_mtime)) if nav_js.exists() else PBGUI_VERSION
    html = html.replace("%%NAV_HASH%%", nav_hash)

    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})
