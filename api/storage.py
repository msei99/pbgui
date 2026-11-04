"""Storage page: disk usage, cleanup, retention rules and system information."""

from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Any, Callable

from fastapi import APIRouter, Body, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from api.auth import SessionToken, require_auth
from api.page_templates import render_page_urls, script_json
from logging_helpers import human_log as _log
from pbgui_purefunc import PBGDIR, load_ini
import storage_controller
from storage_controller import StorageController, StorageError, live_mounts
import system_stats

SERVICE = "Storage"

router = APIRouter()

_controller: StorageController | None = None
_cache: dict[str, tuple[float, Any]] = {}
_cache_locks: dict[str, asyncio.Lock] = {}


class PreviewRequest(BaseModel):
    """Categories and filters to preview."""

    selections: list[dict[str, Any]]


class CleanupRequest(BaseModel):
    """A previously created preview to execute, minus deselected entries."""

    plan_id: str
    exclude: dict[str, list[str]] | None = None


def startup() -> None:
    """Create the storage controller for this API lifespan."""
    global _controller
    system_stats.prime()
    if _controller is None:
        controller = StorageController(Path(PBGDIR))
        controller.start()
        _controller = controller


async def shutdown() -> None:
    """Stop the scheduler and any running storage worker."""
    global _controller
    controller, _controller = _controller, None
    _cache.clear()
    if controller is not None:
        await controller.shutdown()


def _ctrl() -> StorageController:
    if _controller is None:
        raise HTTPException(status_code=503, detail="Storage service is not running")
    return _controller


def _http(exc: StorageError) -> HTTPException:
    return HTTPException(status_code=exc.status, detail=str(exc))


async def _cached(name: str, ttl: float, producer: Callable[[], Any]) -> Any:
    """Run a blocking producer in a thread with a single-flight TTL cache."""
    stamp, value = _cache.get(name, (0.0, None))
    if value is not None and time.monotonic() - stamp < ttl:
        return value
    lock = _cache_locks.setdefault(name, asyncio.Lock())
    async with lock:
        stamp, value = _cache.get(name, (0.0, None))
        if value is not None and time.monotonic() - stamp < ttl:
            return value
        value = await asyncio.to_thread(producer)
        _cache[name] = (time.monotonic(), value)
        return value


async def _mounts(controller: StorageController) -> list[dict[str, Any]]:
    return await _cached("mounts", 5.0, lambda: live_mounts(controller))


@router.get("/main_page", response_class=HTMLResponse)
def get_main_page(request: Request, session: SessionToken = Depends(require_auth)) -> HTMLResponse:
    """Serve the Storage page."""
    del session
    html_path = Path(__file__).parent.parent / "frontend" / "storage.html"
    html = html_path.read_text(encoding="utf-8")
    html = render_page_urls(request, html, "/api/storage")
    from pbgui_purefunc import PBGUI_VERSION

    html = html.replace('"%%VERSION%%"', script_json(PBGUI_VERSION))
    nav_js = Path(__file__).parent.parent / "frontend" / "pbgui_nav.js"
    nav_hash = str(int(nav_js.stat().st_mtime)) if nav_js.exists() else PBGUI_VERSION
    html = html.replace("%%NAV_HASH%%", nav_hash)
    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})


@router.get("/status")
async def get_status(viewing: bool = False, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Return worker progress, snapshot generation and live disk usage."""
    del session
    controller = _ctrl()
    await controller.ensure_loaded()
    if viewing:
        await controller.request_scan(max_age=storage_controller.VIEW_MAX_AGE_S)
    return {**controller.status(), "mounts": await _mounts(controller)}


@router.get("/snapshot")
async def get_snapshot(session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Return directory trees and category summaries of the latest scan."""
    del session
    controller = _ctrl()
    await controller.ensure_loaded()
    return controller.snapshot_view()


@router.get("/categories/{category_id}/items")
async def get_category_items(category_id: str, days: int = 0, keep: int = 0, limit: int | None = None,
                             session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Return one category's entries and which ones match a filter."""
    del session
    controller = _ctrl()
    await controller.ensure_loaded()
    try:
        return controller.category_items(category_id, days=days, keep=keep, limit=limit)
    except StorageError as exc:
        raise _http(exc) from None


@router.post("/preview")
async def post_preview(payload: PreviewRequest, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Create a short-lived cleanup plan for confirmation."""
    del session
    controller = _ctrl()
    await controller.ensure_loaded()
    try:
        return controller.preview(payload.selections)
    except StorageError as exc:
        raise _http(exc) from None


@router.post("/cleanup")
async def post_cleanup(payload: CleanupRequest, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Start deleting a previewed plan in the storage worker."""
    controller = _ctrl()
    try:
        job = await controller.start_cleanup(payload.plan_id, payload.exclude)
    except StorageError as exc:
        raise _http(exc) from None
    del session
    return {"job": job}


@router.post("/cancel")
async def post_cancel(session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Stop the running scan or cleanup after the current file."""
    del session
    return {"stopping": await _ctrl().cancel()}


@router.get("/settings")
async def get_settings(session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Return scan interval and automatic cleanup rules."""
    del session
    controller = _ctrl()
    settings = await asyncio.to_thread(controller.load_settings)
    return {"settings": settings, "auto_state": await asyncio.to_thread(controller.auto_state),
            "categories": [c.public() for c in storage_controller.registry.CATEGORIES if c.auto_allowed]}


@router.put("/settings")
async def put_settings(payload: dict[str, Any] = Body(...), session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Validate and save scan interval and automatic cleanup rules."""
    del session
    controller = _ctrl()
    try:
        settings = await asyncio.to_thread(controller.save_settings, payload)
    except StorageError as exc:
        raise _http(exc) from None
    _log(SERVICE, f"settings saved: auto cleanup {'enabled' if settings['auto']['enabled'] else 'disabled'}", level="INFO")
    return {"settings": settings}


@router.get("/history")
async def get_history(session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Return daily disk usage points."""
    del session
    return {"history": await asyncio.to_thread(_ctrl().history)}


def _python_versions() -> dict[str, str]:
    import platform

    return {
        "PBGui": f"Python {platform.python_version()}",
        "PB7": system_stats.python_version(str(load_ini("main", "pb7venv") or "").strip()),
        "PB8": system_stats.python_version(str(load_ini("main", "pb8venv") or "").strip()),
    }


@router.get("/system")
async def get_system(session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Return CPU, memory, GPU, Python versions and PBGui processes."""
    del session
    controller = _ctrl()
    live, processes, gpus, pythons, mounts = await asyncio.gather(
        _cached("live", 2.0, system_stats.collect_live),
        _cached("processes", 3.0, lambda: system_stats.collect_processes(Path(PBGDIR))),
        _cached("gpus", system_stats.GPU_TTL_S, system_stats.collect_gpus),
        _cached("pythons", 300.0, _python_versions),
        _mounts(controller),
    )
    return {"system": live, "processes": processes, "gpus": gpus, "pythons": pythons, "mounts": mounts}


@router.get("/summary")
async def get_summary(session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Return the compact system and storage summary for the Welcome page."""
    del session
    controller = _ctrl()
    await controller.ensure_loaded()
    live, processes, mounts = await asyncio.gather(
        _cached("live", 2.0, system_stats.collect_live),
        _cached("processes", 3.0, lambda: system_stats.collect_processes(Path(PBGDIR))),
        _mounts(controller),
    )
    status = controller.status()
    pbgui_mount = next((m for m in mounts if "pbgui" in (m.get("roots") or [])), mounts[0] if mounts else None)
    return {
        "system": {key: live.get(key) for key in ("cpu_percent", "cpu_logical", "cpu_physical", "load", "memory",
                                                  "swap", "uptime_s")},
        "processes": {"count": processes["count"], "rss": processes["rss"]},
        "disk": pbgui_mount,
        "mounts": mounts,
        "scanned_at": status["scanned_at"],
        "busy": status["busy"],
        "reclaimable": controller.reclaimable(),
        "warnings": controller.warnings(mounts, live),
    }
