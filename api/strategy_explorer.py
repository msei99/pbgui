"""FastAPI router for the Strategy Explorer page."""

from __future__ import annotations

import copy
import json
import secrets
import time
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, Response

from api.auth import SessionToken, require_auth
from api.strategy_explorer_movie import movie_builder_status
from api.strategy_explorer_sim import simulation_modes
from pbgui_purefunc import PBGUI_SERIAL, PBGUI_VERSION

SERVICE = "StrategyExplorer"

router = APIRouter()

_DRAFT_TTL = 600
_MAX_DRAFT_BYTES = 2 * 1024 * 1024
_MAX_DRAFTS = 128
_draft_store: dict[str, tuple[float, dict[str, Any]]] = {}
_movie_progress: dict[str, dict[str, Any]] = {}
_compare_progress: dict[str, dict[str, Any]] = {}
_simulation_progress: dict[str, dict[str, Any]] = {}


def _safe_download_filename(value: str, default: str = "movie.mp4") -> str:
    """Return a safe download filename without path separators."""
    name = str(value or "").strip() or default
    name = name.replace("/", "_").replace("\\", "_").replace("\x00", "_")
    name = "".join(ch if ch.isalnum() or ch in {".", "_", "-"} else "_" for ch in name)
    if name in {"", ".", ".."}:
        name = default
    if not name.lower().endswith(".mp4"):
        name += ".mp4"
    return name


def _clean_drafts() -> None:
    """Remove expired cross-page config drafts."""
    now = time.time()
    expired = [key for key, (ts, _) in _draft_store.items() if now - ts > _DRAFT_TTL]
    for key in expired:
        _draft_store.pop(key, None)
    if len(_draft_store) >= _MAX_DRAFTS:
        for key, _entry in sorted(_draft_store.items(), key=lambda item: item[1][0])[: len(_draft_store) - _MAX_DRAFTS + 1]:
            _draft_store.pop(key, None)


def _clean_movie_progress() -> None:
    """Remove stale Movie Builder progress entries."""
    now = time.time()
    expired = [key for key, value in _movie_progress.items() if now - float(value.get("updated", 0.0) or 0.0) > 3600]
    for key in expired:
        _movie_progress.pop(key, None)


def _clean_compare_progress() -> None:
    """Remove stale Compare progress entries."""
    now = time.time()
    expired = [key for key, value in _compare_progress.items() if now - float(value.get("updated", 0.0) or 0.0) > 3600]
    for key in expired:
        _compare_progress.pop(key, None)


def _clean_simulation_progress() -> None:
    """Remove stale Simulation progress entries."""
    now = time.time()
    expired = [key for key, value in _simulation_progress.items() if now - float(value.get("updated", 0.0) or 0.0) > 3600]
    for key in expired:
        _simulation_progress.pop(key, None)


def _set_movie_progress(progress_id: str, *, progress: float, message: str, done: bool = False, error: str = "") -> None:
    """Store Movie Builder progress for frontend polling."""
    if not progress_id:
        return
    _movie_progress[str(progress_id)] = {
        "ok": not bool(error),
        "progress": max(0.0, min(1.0, float(progress))),
        "message": str(message or ""),
        "done": bool(done),
        "error": str(error or ""),
        "updated": time.time(),
        "cancelled": bool((_movie_progress.get(str(progress_id)) or {}).get("cancelled")),
    }


def _set_compare_progress(progress_id: str, *, progress: float, message: str, done: bool = False, error: str = "") -> None:
    """Store Compare progress for frontend polling."""
    if not progress_id:
        return
    _compare_progress[str(progress_id)] = {
        "ok": not bool(error),
        "progress": max(0.0, min(1.0, float(progress))),
        "message": str(message or ""),
        "done": bool(done),
        "error": str(error or ""),
        "updated": time.time(),
    }


def _set_simulation_progress(progress_id: str, *, progress: float, message: str, done: bool = False, error: str = "") -> None:
    """Store Simulation progress for frontend polling."""
    if not progress_id:
        return
    _simulation_progress[str(progress_id)] = {
        "ok": not bool(error),
        "progress": max(0.0, min(1.0, float(progress))),
        "message": str(message or ""),
        "done": bool(done),
        "error": str(error or ""),
        "updated": time.time(),
    }


def _get_draft_config(draft_id: str) -> dict[str, Any] | None:
    """Return a draft config by id, or None when missing/expired."""
    _clean_drafts()
    entry = _draft_store.get(str(draft_id or ""))
    if entry is None:
        return None
    ts, config = entry
    _draft_store[str(draft_id)] = (time.time(), config)
    return copy.deepcopy(config)


def _page_context(request: Request, session: SessionToken) -> dict[str, str]:
    """Build replacement values for the standalone HTML page."""
    scheme = request.url.scheme
    host = request.url.hostname or "127.0.0.1"
    port = request.url.port
    origin = f"{scheme}://{host}" + (f":{port}" if port else "")
    return {
        "token": session.token,
        "api_base": origin + "/api/strategy-explorer",
        "ws_base": origin.replace("http://", "ws://").replace("https://", "wss://"),
    }


@router.get("/main_page", response_class=HTMLResponse)
def main_page(
    request: Request,
    draft_id: str = Query(default="", description="Optional Strategy Explorer config draft id"),
    result_path: str = Query(default="", description="Optional source backtest result path"),
    session: SessionToken = Depends(require_auth),
) -> HTMLResponse:
    """Serve the standalone Strategy Explorer page."""
    html_path = Path(__file__).resolve().parent.parent / "frontend" / "v7_strategy_explorer.html"
    if not html_path.exists():
        raise HTTPException(404, "v7_strategy_explorer.html not found")

    html = html_path.read_text(encoding="utf-8")
    ctx = _page_context(request, session)
    replacements = {
        '"%%TOKEN%%"': json.dumps(ctx["token"]),
        '"%%API_BASE%%"': json.dumps(ctx["api_base"]),
        '"%%WS_BASE%%"': json.dumps(ctx["ws_base"]),
        '"%%DRAFT_ID%%"': json.dumps(str(draft_id or "")),
        '"%%RESULT_PATH%%"': json.dumps(str(result_path or "")),
        '"%%VERSION%%"': json.dumps(PBGUI_VERSION),
        '"%%SERIAL%%"': json.dumps(PBGUI_SERIAL),
    }
    for token, value in replacements.items():
        html = html.replace(token, value)
    html = html.replace("%%VERSION%%", PBGUI_VERSION)
    html = html.replace("%%SERIAL%%", PBGUI_SERIAL)

    nav_js = Path(__file__).resolve().parent.parent / "frontend" / "pbgui_nav.js"
    nav_hash = str(int(nav_js.stat().st_mtime)) if nav_js.exists() else PBGUI_VERSION
    html = html.replace("%%NAV_HASH%%", nav_hash)

    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})


@router.get("/session")
def get_session(
    draft_id: str = Query(default="", description="Optional Strategy Explorer config draft id"),
    result_path: str = Query(default="", description="Optional source backtest result path"),
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """Return page bootstrap data and the initial strategy snapshot."""
    from api.strategy_explorer_calc import backtest_result_handoff_options, build_strategy_snapshot, default_strategy_config
    from api.strategy_explorer_core import _resolve_safe_backtest_dir
    from api.pb7_bridge import get_hsl_signal_modes
    from pb7_config import load_pb7_config

    config = _get_draft_config(draft_id) if draft_id else None
    source = "draft" if config is not None else "default"
    messages: list[dict[str, str]] = []
    result_dir = None
    if result_path:
        safe_result_path = _resolve_safe_backtest_dir(str(result_path or ""))
        if safe_result_path:
            result_dir = Path(safe_result_path)
        else:
            messages.append({"level": "warning", "text": "Invalid or unsupported Strategy Explorer backtest result path."})
    if config is None and result_dir is not None and (result_dir / "config.json").is_file():
        try:
            loaded = load_pb7_config(result_dir / "config.json", neutralize_added=True)
            if isinstance(loaded, dict):
                config = loaded
                source = "result"
        except Exception as exc:
            messages.append({"level": "warning", "text": f"Failed to load Strategy Explorer result config: {exc}"})
    if config is None:
        config = default_strategy_config()
    handoff = backtest_result_handoff_options(config, str(result_dir)) if result_dir else {"options": {"load_candles": True}, "messages": [], "meta": {}}
    options = dict(handoff.get("options") or {"load_candles": True})
    snapshot = build_strategy_snapshot(config, source=source, options=options)
    messages.extend(list(handoff.get("messages") or []))
    messages.extend(list(snapshot.get("messages") or []))
    if draft_id and source != "draft":
        messages.insert(0, {"level": "warning", "text": "Strategy Explorer draft expired or was not found. Loaded defaults."})
    return {
        "ok": True,
        "page": {
            "title": "Strategy Explorer",
            "subtitle": "Strategy Explorer",
            "stages": [
                {"key": "analysis", "label": "Analysis"},
                {"key": "exchange-state", "label": "Exchange / State"},
                {"key": "raw", "label": "Raw Config"},
                {"key": "simulation", "label": "Simulation"},
                {"key": "compare", "label": "Compare"},
                {"key": "movie", "label": "Movie Builder"},
            ],
            "simulation_modes": simulation_modes(),
            "hsl_signal_modes": get_hsl_signal_modes() or ["pside", "unified"],
        },
        "draft_id": str(draft_id or ""),
        "result_path": str(result_dir or ""),
        "snapshot": snapshot,
        "handoff": handoff.get("meta") or {},
        "movie": movie_builder_status(),
        "messages": messages,
    }


@router.post("/draft")
def create_draft(body: dict[str, Any], session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Store a temporary config draft for cross-page handoff."""
    config = body.get("config") if isinstance(body, dict) else None
    if not isinstance(config, dict):
        raise HTTPException(400, "config must be an object")
    try:
        if len(json.dumps(config, separators=(",", ":"), ensure_ascii=False).encode("utf-8")) > _MAX_DRAFT_BYTES:
            raise HTTPException(413, "config draft is too large")
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(400, "config draft could not be serialized")
    _clean_drafts()
    draft_id = secrets.token_urlsafe(18)
    _draft_store[draft_id] = (time.time(), copy.deepcopy(config))
    return {"ok": True, "draft_id": draft_id, "ttl": _DRAFT_TTL}


@router.get("/draft/{draft_id}")
def get_draft(draft_id: str, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Return a stored Strategy Explorer draft config."""
    config = _get_draft_config(draft_id)
    if config is None:
        raise HTTPException(404, "draft not found")
    return {"ok": True, "draft_id": draft_id, "config": config}


@router.get("/markets")
def get_markets(session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Return available Strategy Explorer exchanges and coins."""
    from api.strategy_explorer_calc import market_options

    return market_options()


@router.post("/markets")
def post_markets(body: dict[str, Any], session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Return Strategy Explorer exchanges/coins for the posted config and OHLCV source."""
    from api.strategy_explorer_calc import market_options

    config = body.get("config") if isinstance(body, dict) else None
    options = body.get("options") if isinstance(body.get("options"), dict) else {}
    return market_options(config if isinstance(config, dict) else None, options)


@router.post("/snapshot")
def build_snapshot(body: dict[str, Any], session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Build a Strategy Explorer snapshot from a posted config."""
    from api.strategy_explorer_calc import build_strategy_snapshot

    config = body.get("config") if isinstance(body, dict) else None
    if not isinstance(config, dict):
        raise HTTPException(400, "config must be an object")
    options = body.get("options") if isinstance(body.get("options"), dict) else {}
    return build_strategy_snapshot(config, source="posted", options=options)


@router.post("/simulate")
def run_simulation(body: dict[str, Any], session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Run a Strategy Explorer simulation for the posted config."""
    from api.strategy_explorer_calc import build_strategy_simulation

    config = body.get("config") if isinstance(body, dict) else None
    if not isinstance(config, dict):
        raise HTTPException(400, "config must be an object")
    options = body.get("options") if isinstance(body.get("options"), dict) else {}
    mode = str(body.get("mode") or "local_simulation")
    if mode not in {"local_simulation", "pb7_engine"}:
        raise HTTPException(400, "invalid simulation mode")
    progress_id = str(body.get("progress_id") or options.get("progress_id") or "").strip()
    if progress_id:
        _clean_simulation_progress()
        _set_simulation_progress(progress_id, progress=0.0, message="Starting Simulation...")

        def _progress_cb(progress: float, message: str) -> None:
            _set_simulation_progress(progress_id, progress=progress, message=message)

        options = dict(options)
        options["_progress_cb"] = _progress_cb
    try:
        result = build_strategy_simulation(config, mode=mode, options=options)
        if progress_id:
            _set_simulation_progress(
                progress_id,
                progress=1.0,
                message=str(result.get("message") or "Simulation finished."),
                done=True,
                error="" if result.get("ok", True) else str(result.get("message") or "Simulation failed."),
            )
        return result
    except Exception as exc:
        if progress_id:
            _set_simulation_progress(progress_id, progress=1.0, message=str(exc), done=True, error=str(exc))
        raise


@router.get("/simulate/progress/{progress_id}")
def get_simulation_progress(progress_id: str, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Return current Simulation progress for frontend polling."""
    _clean_simulation_progress()
    item = _simulation_progress.get(str(progress_id or ""))
    if not item:
        return {"ok": False, "progress": 0.0, "message": "No Simulation progress found.", "done": True}
    return {k: v for k, v in item.items() if k != "updated"}


@router.post("/compare")
def run_compare(body: dict[str, Any], session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Run Strategy Explorer PB7/B/C compare for the posted config."""
    from api.strategy_explorer_calc import build_strategy_compare

    config = body.get("config") if isinstance(body, dict) else None
    if not isinstance(config, dict):
        raise HTTPException(400, "config must be an object")
    options = body.get("options") if isinstance(body.get("options"), dict) else {}
    progress_id = str(body.get("progress_id") or options.get("progress_id") or "").strip()
    if progress_id:
        _clean_compare_progress()
        _set_compare_progress(progress_id, progress=0.0, message="Starting Compare...")

        def _progress_cb(progress: float, message: str) -> None:
            _set_compare_progress(progress_id, progress=progress, message=message)

        options = dict(options)
        options["_progress_cb"] = _progress_cb
    try:
        result = build_strategy_compare(config, options=options)
        if progress_id:
            _set_compare_progress(
                progress_id,
                progress=1.0,
                message=str(result.get("message") or "Compare finished."),
                done=True,
                error="" if result.get("ok", True) else str(result.get("message") or "Compare failed."),
            )
        return result
    except Exception as exc:
        if progress_id:
            _set_compare_progress(progress_id, progress=1.0, message=str(exc), done=True, error=str(exc))
        raise


@router.get("/compare/progress/{progress_id}")
def get_compare_progress(progress_id: str, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Return current Compare progress for frontend polling."""
    _clean_compare_progress()
    item = _compare_progress.get(str(progress_id or ""))
    if not item:
        return {"ok": False, "progress": 0.0, "message": "No Compare progress found.", "done": True}
    return {k: v for k, v in item.items() if k != "updated"}


@router.post("/movie/frames")
def get_movie_frames(body: dict[str, Any], session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Build Strategy Explorer Movie Builder replay frames."""
    from api.strategy_explorer_calc import build_movie_frames

    config = body.get("config") if isinstance(body, dict) else None
    if not isinstance(config, dict):
        raise HTTPException(400, "config must be an object")
    options = body.get("options") if isinstance(body.get("options"), dict) else {}
    progress_id = str(body.get("progress_id") or options.get("progress_id") or "").strip()
    if progress_id:
        _clean_movie_progress()
        _set_movie_progress(progress_id, progress=0.0, message="Generating movie...")

        def _progress_cb(progress: float, message: str) -> None:
            _set_movie_progress(progress_id, progress=progress, message=message)

        def _cancel_cb() -> bool:
            return bool((_movie_progress.get(progress_id) or {}).get("cancelled"))

        options = dict(options)
        options["_progress_cb"] = _progress_cb
        options["_cancel_cb"] = _cancel_cb
    try:
        result = build_movie_frames(config, options=options)
        if progress_id:
            _set_movie_progress(
                progress_id,
                progress=1.0,
                message=str(result.get("message") or "Movie Builder finished."),
                done=True,
                error="" if result.get("ok", True) else str(result.get("message") or "Movie Builder failed."),
            )
        return result
    except Exception as exc:
        if progress_id:
            _set_movie_progress(progress_id, progress=1.0, message=str(exc), done=True, error=str(exc))
        raise


@router.get("/movie/export/options")
def get_movie_export_options(session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Return Movie Builder MP4 export presets and codec choices."""
    from api.strategy_explorer_export import movie_export_options

    return movie_export_options()


@router.post("/movie/export")
def export_movie(body: dict[str, Any], session: SessionToken = Depends(require_auth)) -> Response:
    """Export a posted Movie Builder Plotly animation as MP4."""
    from api.strategy_explorer_export import export_plotly_animation_to_mp4

    figure = body.get("figure") if isinstance(body, dict) else None
    if not isinstance(figure, dict):
        raise HTTPException(400, "figure must be an object")
    options = body.get("options") if isinstance(body.get("options"), dict) else {}
    progress_id = str(body.get("progress_id") or options.get("progress_id") or "").strip()
    if progress_id:
        _clean_movie_progress()
        _set_movie_progress(progress_id, progress=0.0, message="Starting Movie export...")

        def _progress_cb(progress: float, message: str) -> None:
            _set_movie_progress(progress_id, progress=progress, message=message)

        def _cancel_cb() -> bool:
            return bool((_movie_progress.get(progress_id) or {}).get("cancelled"))

    else:
        _progress_cb = None
        _cancel_cb = None
    try:
        mp4_bytes, meta = export_plotly_animation_to_mp4(figure, options=options, progress_cb=_progress_cb, cancel_cb=_cancel_cb)
        filename = _safe_download_filename(str(options.get("filename") or "movie.mp4"))
        if progress_id:
            codec = str((meta or {}).get("codec") or "")
            suffix = f" ({codec})" if codec else ""
            _set_movie_progress(progress_id, progress=1.0, message=f"Movie export ready{suffix}.", done=True)
        return Response(
            content=mp4_bytes,
            media_type="video/mp4",
            headers={
                "Cache-Control": "no-store",
                "Content-Disposition": f'attachment; filename="{filename}"',
            },
        )
    except Exception as exc:
        if progress_id:
            _set_movie_progress(progress_id, progress=1.0, message=str(exc), done=True, error=str(exc))
        raise HTTPException(500, str(exc)) from exc


@router.get("/movie/progress/{progress_id}")
def get_movie_progress(progress_id: str, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Return current Movie Builder progress for frontend polling."""
    _clean_movie_progress()
    item = _movie_progress.get(str(progress_id or ""))
    if not item:
        return {"ok": False, "progress": 0.0, "message": "No Movie Builder progress found.", "done": True}
    return {k: v for k, v in item.items() if k != "updated"}


@router.post("/movie/progress/{progress_id}/cancel")
def cancel_movie_progress(progress_id: str, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Mark a Movie Builder job as cancelled."""
    item = _movie_progress.setdefault(str(progress_id or ""), {})
    item["cancelled"] = True
    item["updated"] = time.time()
    return {"ok": True, "cancelled": True}
