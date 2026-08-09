"""FastAPI backend for the isolated Passivbot V8 Strategy Explorer."""

from __future__ import annotations

import asyncio
import copy
import csv
import datetime as dt
import hashlib
import json
import secrets
import threading
import time
import traceback
import uuid
from pathlib import Path
from typing import Any, Callable

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, Response

from api.auth import SessionToken, require_auth
from logging_helpers import human_log as _log
from pbgui_purefunc import PBGUI_SERIAL, PBGUI_VERSION, pb8_runtime_status
from api.pb8_ohlcv_tools import (
    PB8OhlcvError,
    PB8OhlcvUnavailableError,
    resolve_pb8_ohlcv_paths,
)
import pb8_strategy_explorer as explorer

SERVICE = "StrategyExplorerV8"
router = APIRouter()

_DRAFT_TTL_SECONDS = 600.0
_MAX_DRAFTS = 128
_MAX_DRAFT_BYTES = 2 * 1024 * 1024
_PROGRESS_TTL_SECONDS = 3600.0
_MAX_PROGRESS = 256
_MAX_MOVIE_EXPORT_PAYLOAD_BYTES = 16 * 1024 * 1024
_MAX_MOVIE_EXPORT_OUTPUT_BYTES = 512 * 1024 * 1024
_STORE_LOCK = threading.RLock()
_draft_store: dict[str, dict[str, Any]] = {}
_progress_store: dict[str, dict[str, Any]] = {}
_started = False


def _owner(session: SessionToken) -> str:
    """Return a non-reversible process-local ownership digest for a session token."""
    return hashlib.sha256(session.token.encode("utf-8")).hexdigest()


def _clean_stores(*, reserve_draft: bool = False, reserve_progress: bool = False) -> None:
    """Expire and bound owner-scoped drafts and progress records under one lock."""
    now = time.time()
    with _STORE_LOCK:
        for key in [key for key, item in _draft_store.items() if now - float(item["touched_at"]) > _DRAFT_TTL_SECONDS]:
            _draft_store.pop(key, None)
        for key in [key for key, item in _progress_store.items() if now - float(item["updated_at"]) > _PROGRESS_TTL_SECONDS]:
            _progress_store.pop(key, None)
        draft_excess = len(_draft_store) - _MAX_DRAFTS + (1 if reserve_draft else 0)
        if draft_excess > 0:
            oldest = sorted(_draft_store.items(), key=lambda item: item[1]["touched_at"])[:draft_excess]
            for key, _item in oldest:
                _draft_store.pop(key, None)
        progress_excess = len(_progress_store) - _MAX_PROGRESS + (1 if reserve_progress else 0)
        if progress_excess > 0:
            oldest = sorted(_progress_store.items(), key=lambda item: item[1]["updated_at"])[:progress_excess]
            for key, _item in oldest:
                _progress_store.pop(key, None)


def _json_size(value: Any) -> int:
    """Return strict compact JSON size or raise a browser-safe validation error."""
    try:
        return len(json.dumps(value, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8"))
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="Strategy Explorer payload is not valid JSON") from exc


def _script_json(value: Any) -> str:
    """Serialize one inline-script value without permitting an HTML end tag."""
    return (
        json.dumps(value)
        .replace("&", "\\u0026")
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
    )


def _draft(draft_id: str, session: SessionToken, *, touch: bool = True) -> dict[str, Any]:
    """Return an owner-bound draft without revealing cross-owner existence."""
    _clean_stores()
    key = str(draft_id or "").strip()
    with _STORE_LOCK:
        item = _draft_store.get(key)
        if item is None or item["owner"] != _owner(session):
            raise HTTPException(status_code=404, detail="draft not found")
        if touch:
            item["touched_at"] = time.time()
        return copy.deepcopy(item)


def _validate_result_path(value: Any) -> str:
    """Validate one result through the PB8 backtest router's managed-root boundary."""
    if not str(value or "").strip():
        return ""
    from api.backtest_v8 import _resolve_result_dir

    return str(_resolve_result_dir(str(value), allow_archives=True))


def _canonical_config(config: Any) -> dict[str, Any]:
    """Canonicalize a draft config in the isolated PB8 helper."""
    if not isinstance(config, dict):
        raise HTTPException(status_code=400, detail="config must be an object")
    result = _call("canonicalize config", explorer.capabilities, config)
    canonical = result.get("canonical_config")
    if not isinstance(canonical, dict):
        raise HTTPException(status_code=500, detail="PB8 did not return a canonical configuration")
    return canonical


def _result_initial_options(result_path: str, config: dict[str, Any]) -> dict[str, str]:
    """Derive one valid initial market and timestamp from PB8 result artifacts."""
    if not result_path:
        return {}
    from api.backtest_v8 import _read_json, _resolve_result_file

    result_dir = Path(result_path)
    try:
        dataset = _read_json(_resolve_result_file(result_dir, "dataset.json"))
    except (HTTPException, RuntimeError) as exc:
        _log(SERVICE, f"PB8 result has no usable dataset metadata: {exc}", level="WARNING")
        return {}

    backtest = config.get("backtest") if isinstance(config.get("backtest"), dict) else {}
    live = config.get("live") if isinstance(config.get("live"), dict) else {}
    configured_exchanges = backtest.get("exchanges")
    if isinstance(configured_exchanges, str):
        configured_exchanges = [configured_exchanges]
    allowed_exchanges = {
        str(value).strip().lower()
        for value in configured_exchanges or []
        if str(value or "").strip()
    }
    approved = live.get("approved_coins")
    if isinstance(approved, list):
        approved = {"long": approved, "short": approved}
    approved = approved if isinstance(approved, dict) else {}
    allowed_coins = {
        str(value).strip().upper()
        for side in ("long", "short")
        for value in (approved.get(side) if isinstance(approved.get(side), list) else [])
        if str(value or "").strip()
    }

    coin = ""
    timestamp = ""
    try:
        fills_path = _resolve_result_file(result_dir, "fills.csv")
        with fills_path.open("r", encoding="utf-8", newline="") as handle:
            for scanned, fill in enumerate(csv.DictReader(handle), start=1):
                if scanned > 1_000_000:
                    break
                candidate = str(fill.get("coin") or "").strip().upper()
                if candidate in allowed_coins:
                    coin = candidate
                    timestamp = str(fill.get("timestamp") or "").strip()
                    break
    except (HTTPException, OSError, UnicodeError, csv.Error):
        pass

    dataset_coins = dataset.get("coins") if isinstance(dataset.get("coins"), list) else []
    if coin not in allowed_coins:
        coin = next(
            (
                str(value).strip().upper()
                for value in dataset_coins
                if str(value).strip().upper() in allowed_coins
            ),
            "",
        )
    if not coin:
        return {}

    preparation = dataset.get("preparation") if isinstance(dataset.get("preparation"), dict) else {}
    selections = preparation.get("source_selection") if isinstance(preparation.get("source_selection"), dict) else {}
    selection = selections.get(coin) if isinstance(selections.get(coin), dict) else {}
    exchange = str(selection.get("selected_exchange") or "").strip().lower()
    if exchange not in allowed_exchanges:
        dataset_exchange = str(dataset.get("exchange") or "").strip().lower()
        exchange = dataset_exchange if dataset_exchange in allowed_exchanges else ""
    if not exchange:
        return {}

    selected_dt: dt.datetime | None = None
    if timestamp:
        try:
            selected_dt = dt.datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        except ValueError:
            selected_dt = None
    quality = selection.get("selected_quality") if isinstance(selection.get("selected_quality"), dict) else {}
    if selected_dt is None:
        try:
            selected_dt = dt.datetime.fromtimestamp(
                float(quality.get("first_ts")) / 1000.0,
                tz=dt.timezone.utc,
            )
        except (TypeError, ValueError, OverflowError, OSError):
            return {}
    if selected_dt.tzinfo is None:
        selected_dt = selected_dt.replace(tzinfo=dt.timezone.utc)
    selected_dt = selected_dt.astimezone(dt.timezone.utc)
    return {
        "exchange": exchange,
        "coin": coin,
        "start_date": selected_dt.date().isoformat(),
        "start_time": selected_dt.strftime("%H:%M"),
    }


def _validated_ohlcv_config(config: dict[str, Any]) -> dict[str, Any]:
    """Normalize PB8 OHLCV paths below approved runtime or PBGui data roots."""
    backtest = config.get("backtest") if isinstance(config.get("backtest"), dict) else {}
    if not str(backtest.get("ohlcv_source_dir") or "").strip():
        return config
    try:
        validated, _source_dir, _catalog = resolve_pb8_ohlcv_paths(
            config, pb8_runtime_status()
        )
        return validated
    except PB8OhlcvUnavailableError as exc:
        _log(SERVICE, f"PB8 OHLCV source validation unavailable: {exc}", level="WARNING")
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except PB8OhlcvError as exc:
        _log(SERVICE, f"Rejected PB8 Strategy Explorer OHLCV source: {exc}", level="WARNING")
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _merge_sparse_overrides(config: Any, overrides: Any) -> dict[str, Any]:
    """Merge validated in-memory sparse override payloads into their referenced coins."""
    if not isinstance(config, dict):
        raise HTTPException(status_code=400, detail="config must be an object")
    if overrides is None:
        overrides = {}
    if not isinstance(overrides, dict):
        raise HTTPException(status_code=400, detail="override_configs must be an object")
    from api.backtest_v8 import _validate_override_payloads

    validated = _validate_override_payloads(config, overrides)
    merged = copy.deepcopy(config)
    coin_overrides = merged.get("coin_overrides")
    if not isinstance(coin_overrides, dict):
        coin_overrides = {}
        merged["coin_overrides"] = coin_overrides

    def merge(target: dict[str, Any], source: dict[str, Any]) -> None:
        for key, value in source.items():
            if isinstance(value, dict) and isinstance(target.get(key), dict):
                merge(target[key], value)
            else:
                target[key] = copy.deepcopy(value)

    for coin, descriptor in list(coin_overrides.items()):
        if not isinstance(descriptor, dict):
            continue
        filename = str(descriptor.get("override_config_path") or "")
        if not filename:
            continue
        payload = validated.get(filename)
        if not isinstance(payload, dict):
            raise HTTPException(status_code=422, detail=f"Override config not found: {filename}")
        inline = copy.deepcopy(descriptor)
        inline.pop("override_config_path", None)
        effective = copy.deepcopy(payload)
        merge(effective, inline)
        coin_overrides[coin] = effective
    return merged


def _safe_filename(value: Any) -> str:
    """Return a safe MP4 download filename without path components."""
    name = str(value or "movie.mp4").strip().replace("/", "_").replace("\\", "_").replace("\x00", "_")
    name = "".join(char if char.isalnum() or char in {".", "_", "-"} else "_" for char in name)
    if name in {"", ".", ".."}:
        name = "movie.mp4"
    if not name.lower().endswith(".mp4"):
        name += ".mp4"
    return name[:160]


def _call(label: str, function: Callable[..., dict[str, Any]], *args, **kwargs) -> dict[str, Any]:
    """Map isolated PB8 failures to logged browser-safe HTTP errors."""
    try:
        return function(*args, **kwargs)
    except HTTPException:
        raise
    except explorer.PB8StrategyExplorerBusyError as exc:
        _log(SERVICE, f"{label} busy: {exc}", level="WARNING")
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except explorer.PB8StrategyExplorerCancelledError as exc:
        _log(SERVICE, f"{label} cancelled: {exc}", level="INFO")
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except explorer.PB8StrategyExplorerError as exc:
        _log(SERVICE, f"{label} failed: {exc}", level="WARNING")
        raise HTTPException(status_code=int(getattr(exc, "status_code", 422)), detail=str(exc)) from exc
    except Exception as exc:
        _log(SERVICE, f"{label} failed unexpectedly: {exc}", level="ERROR", meta={"traceback": traceback.format_exc()})
        raise HTTPException(status_code=500, detail=f"{label} failed") from exc


def _progress_id(body: dict[str, Any], options: dict[str, Any]) -> str:
    """Read and validate one browser polling id."""
    value = str(body.get("progress_id") or options.get("progress_id") or "").strip()
    if len(value) > 128 or any(ord(char) < 33 or ord(char) > 126 for char in value):
        raise HTTPException(status_code=400, detail="invalid progress id")
    return value


def _progress_begin(progress_id: str, kind: str, session: SessionToken) -> str:
    """Create an owner-bound bounded progress record and return its internal process id."""
    operation_id = uuid.uuid4().hex
    if not progress_id:
        return operation_id
    _clean_stores(reserve_progress=True)
    owner = _owner(session)
    with _STORE_LOCK:
        existing = _progress_store.get(progress_id)
        if existing and existing["owner"] != owner:
            raise HTTPException(status_code=409, detail="progress id is already in use")
        if existing and not existing.get("done"):
            raise HTTPException(status_code=409, detail="progress id already has an active operation")
        _progress_store[progress_id] = {
            "owner": owner,
            "kind": kind,
            "operation_id": operation_id,
            "ok": True,
            "progress": 0.0,
            "message": f"Starting {kind}...",
            "done": False,
            "error": "",
            "cancelled": False,
            "updated_at": time.time(),
        }
    return operation_id


def _progress_update(
    progress_id: str,
    *,
    operation_id: str,
    progress: float,
    message: str,
    done: bool = False,
    error: str = "",
) -> None:
    """Update one progress record while preserving cancellation and ownership fields."""
    if not progress_id:
        return
    with _STORE_LOCK:
        item = _progress_store.get(progress_id)
        if item is None or item.get("operation_id") != operation_id:
            return
        item.update(
            {
                "ok": not bool(error),
                "progress": max(0.0, min(1.0, float(progress))),
                "message": str(message or ""),
                "done": bool(done),
                "error": str(error or "")[-2000:],
                "updated_at": time.time(),
            }
        )


def _progress_result(progress_id: str, kind: str, session: SessionToken) -> dict[str, Any]:
    """Return only an owner's matching progress record."""
    _clean_stores()
    with _STORE_LOCK:
        item = _progress_store.get(str(progress_id or ""))
        if item is None or item["owner"] != _owner(session) or item["kind"] != kind:
            return {"ok": False, "progress": 0.0, "message": f"No {kind} progress found.", "done": True}
        return {key: value for key, value in item.items() if key not in {"owner", "kind", "operation_id", "updated_at"}}


def _request_parts(
    body: dict[str, Any],
    session: SessionToken,
    *,
    require_draft: bool = False,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any] | None]:
    """Resolve posted config/options and optional owner-bound draft provenance."""
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="request body must be an object")
    options = dict(body.get("options")) if isinstance(body.get("options"), dict) else {}
    config = body.get("config")
    draft_id = str(body.get("draft_id") or options.get("draft_id") or "").strip()
    entry = None
    if draft_id:
        try:
            entry = _draft(draft_id, session)
        except HTTPException as exc:
            if exc.status_code != 404 or require_draft or not isinstance(config, dict):
                raise
            options.pop("draft_id", None)
    if not isinstance(config, dict) and entry is not None:
        config = entry["config"]
    if not isinstance(config, dict):
        raise HTTPException(status_code=400, detail="config must be an object")
    blocked_paths = [body.get("result_path"), options.get("result_path"), options.get("pb7_backtest_dir")]
    if any(str(value or "").strip() for value in blocked_paths) and entry is None:
        raise HTTPException(status_code=400, detail="result provenance must be supplied through an owner-bound draft_id")
    return _validated_ohlcv_config(config), dict(options), entry


def _unavailable_snapshot(
    config: dict[str, Any], capabilities: dict[str, Any], detail: str, options: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Return a renderable config snapshot when native market data is unavailable."""
    options = options if isinstance(options, dict) else {}
    backtest = config.get("backtest") if isinstance(config.get("backtest"), dict) else {}
    live = config.get("live") if isinstance(config.get("live"), dict) else {}
    bot = config.get("bot") if isinstance(config.get("bot"), dict) else {}
    exchanges = backtest.get("exchanges") if isinstance(backtest.get("exchanges"), list) else []
    approved = live.get("approved_coins")
    if isinstance(approved, list):
        approved = {"long": approved, "short": approved}
    approved = approved if isinstance(approved, dict) else {}
    coins = []
    for side in ("long", "short"):
        for coin in approved.get(side) if isinstance(approved.get(side), list) else []:
            value = str(coin or "").strip().upper()
            if value and value not in coins:
                coins.append(value)
    strategy = capabilities.get("strategy") if isinstance(capabilities.get("strategy"), dict) else {}
    sides = {}
    for side in ("long", "short"):
        side_config = bot.get(side) if isinstance(bot.get(side), dict) else {}
        risk = side_config.get("risk") if isinstance(side_config.get("risk"), dict) else {}
        n_positions = int(risk.get("n_positions") or 0)
        total_limit = float(risk.get("total_wallet_exposure_limit") or 0.0)
        sides[side] = {
            "active": n_positions > 0 and total_limit > 0.0,
            "params": copy.deepcopy(side_config),
            "visual_params": copy.deepcopy(side_config),
            "modes": {"entry": "Unavailable", "close": "Unavailable"},
            "orders": {
                "entries": [],
                "closes": [],
                "normal_entries": [],
                "gridonly_entries": [],
                "gridonly_closes": [],
            },
            "summary": {
                "entry_orders": 0,
                "close_orders": 0,
                "entry_avg_price": 0.0,
                "entry_grid_pct": 0.0,
                "wallet_exposure_limit_per_position": (
                    total_limit / max(1, n_positions) if n_positions > 0 else 0.0
                ),
            },
            "debug": {"exchange_params": {}, "state_params": {}},
        }
    return {
        "ok": False,
        "source": "posted",
        "title": f"PB8 {str(live.get('strategy_kind') or 'strategy')}",
        "market": {
            "exchange": str(options.get("exchange") or (exchanges[0] if exchanges else "")),
            "coin": str(options.get("coin") or (coins[0] if coins else "")),
            "reference_price": 100.0,
            "engine_status": "PB8 snapshot unavailable",
            "ohlcv_status": "No valid local PB8 candles for the selected config window",
            "metadata": {
                "ohlcv": {
                    "rows": 0,
                    "selected_start": str(options.get("start_date") or backtest.get("start_date") or ""),
                }
            },
        },
        "labels": {"engine": "PB8 Backtest Engine", "snapshot": "Unavailable"},
        "param_groups": strategy.get("param_groups") or [],
        "param_field_meta": strategy.get("param_field_meta") or {},
        "sides": sides,
        "candles": [],
        "config": copy.deepcopy(config),
        "options": copy.deepcopy(options),
        "messages": [
            {
                "level": "error",
                "text": f"PB8 snapshot unavailable: {str(detail or 'no valid local market data')}",
            }
        ],
    }


def startup() -> None:
    """Initialize process-local stores; repeated startup calls are harmless."""
    global _started
    with _STORE_LOCK:
        if _started:
            return
        _started = True
    explorer.startup()
    _clean_stores()


async def shutdown() -> None:
    """Stop API-owned helpers and clear stores; repeated shutdown calls are harmless."""
    global _started
    await asyncio.to_thread(explorer.shutdown)
    with _STORE_LOCK:
        _draft_store.clear()
        _progress_store.clear()
        _started = False


@router.get("/main_page", response_class=HTMLResponse)
def main_page(
    request: Request,
    draft_id: str = Query(default="", description="Optional owner-bound Strategy Explorer draft id"),
    session: SessionToken = Depends(require_auth),
) -> HTMLResponse:
    """Serve the exact existing Strategy Explorer HTML with cookie-only authentication."""
    del session
    html_path = Path(__file__).resolve().parent.parent / "frontend" / "v7_strategy_explorer.html"
    if not html_path.is_file():
        raise HTTPException(status_code=404, detail="v7_strategy_explorer.html not found")
    html = html_path.read_text(encoding="utf-8")
    origin = str(request.base_url).rstrip("/")
    route_base = request.url.path.rsplit("/main_page", 1)[0]
    api_base = origin + route_base
    ws_base = origin.replace("http://", "ws://", 1).replace("https://", "wss://", 1)
    replacements = {
        '"%%TOKEN%%"': _script_json(""),
        '"%%API_BASE%%"': _script_json(api_base),
        '"%%WS_BASE%%"': _script_json(ws_base),
        '"%%DRAFT_ID%%"': _script_json(str(draft_id or "")),
        '"%%RESULT_PATH%%"': _script_json(""),
        '"%%VERSION%%"': _script_json(PBGUI_VERSION),
        '"%%SERIAL%%"': _script_json(PBGUI_SERIAL),
    }
    for placeholder, value in replacements.items():
        html = html.replace(placeholder, value)
    html = html.replace("%%VERSION%%", PBGUI_VERSION).replace("%%SERIAL%%", PBGUI_SERIAL)
    nav = Path(__file__).resolve().parent.parent / "frontend" / "pbgui_nav.js"
    nav_hash = str(int(nav.stat().st_mtime)) if nav.is_file() else PBGUI_VERSION
    html = html.replace("%%NAV_HASH%%", nav_hash)
    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})


@router.get("/session")
def get_session(
    draft_id: str = Query(default="", description="Optional owner-bound Strategy Explorer draft id"),
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """Return PB7-compatible bootstrap data without exposing filesystem provenance."""
    entry = _draft(draft_id, session) if draft_id else None
    if entry is None:
        capabilities = _call("load capabilities", explorer.capabilities)
        config = capabilities.get("canonical_config")
        source = "default"
    else:
        capabilities = _call("load capabilities", explorer.capabilities, entry["config"])
        config = entry["config"]
        source = "draft"
    if not isinstance(config, dict):
        raise HTTPException(status_code=500, detail="PB8 default configuration is unavailable")
    initial_options = (
        entry.get("initial_options")
        if entry and isinstance(entry.get("initial_options"), dict)
        else {}
    )
    try:
        snapshot = _call("build session snapshot", explorer.snapshot, config, initial_options)
    except HTTPException as exc:
        if exc.status_code != 422:
            raise
        snapshot = _unavailable_snapshot(config, capabilities, str(exc.detail), initial_options)
    snapshot["source"] = source
    strategy = capabilities.get("strategy") if isinstance(capabilities.get("strategy"), dict) else {}
    return {
        "ok": True,
        "page": {
            "title": "Strategy Explorer",
            "subtitle": "PB8 Strategy Explorer",
            "strategy_label": "PB8",
            "stages": [
                {"key": "analysis", "label": "Analysis"},
                {"key": "exchange-state", "label": "Exchange / State"},
                {"key": "raw", "label": "Raw Config"},
                {"key": "simulation", "label": "Simulation"},
                {"key": "compare", "label": "Compare"},
                {"key": "movie", "label": "Movie Builder"},
            ],
            "simulation_modes": capabilities.get("simulation_modes") or [],
            "hsl_signal_modes": ["pside", "unified"],
            "strategy_kinds": strategy.get("supported_kinds") or [],
        },
        "draft_id": str(draft_id or ""),
        "snapshot": snapshot,
        "handoff": {
            "provenance_available": bool(entry and entry.get("result_path")),
            "compare_available": bool(entry and isinstance(entry.get("compare_config"), dict)),
        },
        "movie": {"available": True, "message": "Build replay frames with the PB8 native backtest engine."},
        "messages": snapshot.get("messages") or [],
    }


@router.post("/draft")
def create_draft(body: dict[str, Any], session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Store one canonical owner-bound cross-page handoff for ten minutes."""
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="request body must be an object")
    result_path = _validate_result_path(body.get("result_path"))
    overrides = body.get("override_configs") if "override_configs" in body else None
    if overrides is None and result_path:
        from api.backtest_v8 import _load_override_payloads

        raw_config = body.get("config")
        if not isinstance(raw_config, dict):
            raise HTTPException(status_code=400, detail="config must be an object")
        overrides = _load_override_payloads(raw_config, Path(result_path))
    if overrides is None:
        overrides = {}
    effective_config = _merge_sparse_overrides(body.get("config"), overrides)
    canonical = _canonical_config(effective_config)
    initial_options = _result_initial_options(result_path, canonical)
    provenance = body.get("provenance") or {}
    if not isinstance(overrides, dict):
        raise HTTPException(status_code=400, detail="override_configs must be an object")
    if not isinstance(provenance, dict):
        raise HTTPException(status_code=400, detail="provenance must be an object")
    stored = {
        "config": canonical,
        "compare_config": (
            _canonical_config(
                _merge_sparse_overrides(
                    body.get("compare_config"),
                    body.get("compare_override_configs") or {},
                )
            )
            if isinstance(body.get("compare_config"), dict)
            else None
        ),
        "result_path": result_path,
        "initial_options": initial_options,
        "override_configs": copy.deepcopy(overrides),
        "provenance": copy.deepcopy(provenance),
    }
    size_probe = {
        "owner": "0" * 64,
        "created_at": time.time(),
        "touched_at": time.time(),
        **stored,
    }
    if _json_size(size_probe) > _MAX_DRAFT_BYTES:
        raise HTTPException(status_code=413, detail="Strategy Explorer draft exceeds the 2 MiB limit")
    _clean_stores(reserve_draft=True)
    draft_id = secrets.token_urlsafe(24)
    now = time.time()
    with _STORE_LOCK:
        _draft_store[draft_id] = {"owner": _owner(session), "created_at": now, "touched_at": now, **stored}
    return {"ok": True, "draft_id": draft_id, "ttl": int(_DRAFT_TTL_SECONDS)}


@router.get("/draft/{draft_id}")
def get_draft(draft_id: str, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Return an owner's draft config and overrides without filesystem provenance."""
    item = _draft(draft_id, session)
    return {
        "ok": True,
        "draft_id": draft_id,
        "config": item["config"],
        "override_configs": item["override_configs"],
        "provenance": item["provenance"],
        "compare_available": isinstance(item.get("compare_config"), dict),
    }


@router.post("/markets")
def post_markets(body: dict[str, Any], session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Return config-approved PB8 markets from local metadata only."""
    config, options, _entry = _request_parts(body, session)
    return _call("load markets", explorer.markets, config, options)


@router.post("/snapshot")
def build_snapshot(body: dict[str, Any], session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Build a PB7-compatible native PB8 ideal-order snapshot."""
    config, options, _entry = _request_parts(body, session)
    return _call("build snapshot", explorer.snapshot, config, options)


def _run_progress_operation(
    *,
    kind: str,
    progress_id: str,
    operation_id: str,
    call: Callable[[], dict[str, Any]],
) -> dict[str, Any]:
    """Run one synchronous helper operation while publishing lifecycle progress."""
    _progress_update(progress_id, operation_id=operation_id, progress=0.1, message=f"Running PB8 {kind}...")
    try:
        result = call()
    except HTTPException as exc:
        _progress_update(
            progress_id,
            operation_id=operation_id,
            progress=1.0,
            message=str(exc.detail),
            done=True,
            error=str(exc.detail),
        )
        raise
    message = str(result.get("message") or f"PB8 {kind} finished.")
    _progress_update(
        progress_id,
        operation_id=operation_id,
        progress=1.0,
        message=message,
        done=True,
        error="" if result.get("ok", True) else message,
    )
    return result


@router.post("/simulate")
def run_simulation(body: dict[str, Any], session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Run the requested legacy mode through the single PB8 native replay engine."""
    config, options, _entry = _request_parts(body, session)
    if str(options.get("sim_start_state") or "flat").strip().lower() == "manual":
        raise HTTPException(
            status_code=422,
            detail="PB8 Native Replay does not support manual starting positions; use the flat native state.",
        )
    progress_id = _progress_id(body, options)
    operation_id = _progress_begin(progress_id, "Simulation", session)
    return _run_progress_operation(
        kind="Simulation",
        progress_id=progress_id,
        operation_id=operation_id,
        call=lambda: _call("run simulation", explorer.replay, config, options, operation_id=operation_id),
    )


@router.get("/simulate/progress/{progress_id}")
def get_simulation_progress(progress_id: str, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Return owner-bound native replay progress."""
    return _progress_result(progress_id, "Simulation", session)


@router.post("/compare")
def run_compare(body: dict[str, Any], session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Compare a fresh PB8 replay with owner-bound provenance or another replay."""
    config, options, entry = _request_parts(body, session, require_draft=True)
    progress_id = _progress_id(body, options)
    operation_id = _progress_begin(progress_id, "Compare", session)
    compare_config = body.get("compare_config") if isinstance(body.get("compare_config"), dict) else None
    if compare_config is None and isinstance((entry or {}).get("compare_config"), dict):
        compare_config = entry["compare_config"]
    result_path = str((entry or {}).get("result_path") or "")
    if compare_config is not None:
        compare_config = _validated_ohlcv_config(compare_config)
    if not result_path and compare_config is None:
        raise HTTPException(
            status_code=422,
            detail="Compare requires a stored-result handoff or a distinct pinned PB8 baseline config.",
        )
    return _run_progress_operation(
        kind="Compare",
        progress_id=progress_id,
        operation_id=operation_id,
        call=lambda: _call(
            "run compare",
            explorer.compare,
            config,
            options,
            result_path=result_path,
            compare_config=compare_config,
            operation_id=operation_id,
        ),
    )


@router.get("/compare/progress/{progress_id}")
def get_compare_progress(progress_id: str, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Return owner-bound compare progress."""
    return _progress_result(progress_id, "Compare", session)


@router.post("/movie/frames")
def get_movie_frames(body: dict[str, Any], session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Build bounded movie frames from PB8 native replay candles and fills."""
    config, options, _entry = _request_parts(body, session)
    progress_id = _progress_id(body, options)
    operation_id = _progress_begin(progress_id, "Movie Builder", session)
    return _run_progress_operation(
        kind="Movie Builder",
        progress_id=progress_id,
        operation_id=operation_id,
        call=lambda: _call("build movie frames", explorer.movie, config, options, operation_id=operation_id),
    )


@router.get("/movie/export/options")
def get_movie_export_options(session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Return the existing local-only Movie Builder encoder options."""
    del session
    from api.strategy_explorer_export import movie_export_options

    return movie_export_options()


@router.post("/movie/export")
def export_movie(body: dict[str, Any], session: SessionToken = Depends(require_auth)) -> Response:
    """Export a posted Plotly animation through the existing local MP4 exporter."""
    if not isinstance(body, dict) or not isinstance(body.get("figure"), dict):
        raise HTTPException(status_code=400, detail="figure must be an object")
    options = body.get("options") if isinstance(body.get("options"), dict) else {}
    if _json_size({"figure": body["figure"], "options": options}) > _MAX_MOVIE_EXPORT_PAYLOAD_BYTES:
        raise HTTPException(status_code=413, detail="Movie export payload exceeds the 16 MiB limit")
    progress_id = _progress_id(body, options)
    from api.strategy_explorer_export import (
        MovieExportBusyError,
        MovieExportTooLargeError,
        export_plotly_animation_to_mp4,
    )

    operation_id = _progress_begin(progress_id, "Movie Builder", session)

    def progress_cb(progress: float, message: str) -> None:
        _progress_update(progress_id, operation_id=operation_id, progress=progress, message=message)

    def cancel_cb() -> bool:
        if not progress_id:
            return False
        with _STORE_LOCK:
            item = _progress_store.get(progress_id)
            return bool(item is None or item.get("operation_id") != operation_id or item.get("cancelled"))

    try:
        content, meta = export_plotly_animation_to_mp4(
            body["figure"], options=options, progress_cb=progress_cb, cancel_cb=cancel_cb
        )
        if len(content) > _MAX_MOVIE_EXPORT_OUTPUT_BYTES:
            raise HTTPException(status_code=413, detail="Movie export output exceeds the 512 MiB limit")
    except MovieExportBusyError as exc:
        _progress_update(
            progress_id,
            operation_id=operation_id,
            progress=1.0,
            message=str(exc),
            done=True,
            error=str(exc),
        )
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except MovieExportTooLargeError as exc:
        _progress_update(
            progress_id,
            operation_id=operation_id,
            progress=1.0,
            message=str(exc),
            done=True,
            error=str(exc),
        )
        raise HTTPException(status_code=413, detail=str(exc)) from exc
    except HTTPException as exc:
        _progress_update(
            progress_id,
            operation_id=operation_id,
            progress=1.0,
            message=str(exc.detail),
            done=True,
            error=str(exc.detail),
        )
        raise
    except Exception as exc:
        _log(SERVICE, f"Movie export failed: {exc}", level="WARNING")
        _progress_update(
            progress_id,
            operation_id=operation_id,
            progress=1.0,
            message=str(exc),
            done=True,
            error=str(exc),
        )
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    codec = str((meta or {}).get("codec") or "")
    _progress_update(
        progress_id,
        operation_id=operation_id,
        progress=1.0,
        message=f"Movie export ready{f' ({codec})' if codec else ''}.",
        done=True,
    )
    return Response(
        content=content,
        media_type="video/mp4",
        headers={"Cache-Control": "no-store", "Content-Disposition": f'attachment; filename="{_safe_filename(options.get("filename"))}"'},
    )


@router.get("/movie/progress/{progress_id}")
def get_movie_progress(progress_id: str, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Return owner-bound movie generation or export progress."""
    return _progress_result(progress_id, "Movie Builder", session)


@router.post("/movie/progress/{progress_id}/cancel")
def cancel_movie_progress(progress_id: str, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """Cancel only the requesting owner's active Movie Builder helper/export."""
    with _STORE_LOCK:
        item = _progress_store.get(str(progress_id or ""))
        if item is None or item["owner"] != _owner(session) or item["kind"] != "Movie Builder":
            raise HTTPException(status_code=404, detail="Movie Builder progress not found")
        item["cancelled"] = True
        item["message"] = "Movie Builder cancellation requested."
        item["updated_at"] = time.time()
        operation_id = str(item.get("operation_id") or "")
    explorer.cancel(operation_id)
    return {"ok": True, "cancelled": True}


__all__ = ["router", "startup", "shutdown"]
