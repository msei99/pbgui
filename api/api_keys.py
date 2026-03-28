"""
FastAPI router: API-Keys management endpoints.

Provides CRUD operations for API key management, connection testing,
Hyperliquid key expiry checking (with local persistence), and
top-level comment field management in api-keys.json.
All endpoints require auth (Bearer token).
"""
from __future__ import annotations

import asyncio
import json
import threading
import time
import traceback
from pathlib import Path as _Path
from typing import Any, Optional

import httpx
from fastapi import APIRouter, Body, Depends, HTTPException, Path as PathParam, Query, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel, Field

from api.auth import SessionToken, require_auth
from logging_helpers import human_log as _log
from pbgui_purefunc import PBGDIR as _PBGDIR

SERVICE = "ApiKeys"

router = APIRouter()

# ── FileSyncWorker (injected by PBApiServer at startup) ──────

_file_sync_worker = None


def init_file_sync(worker):
    """Called by PBApiServer._lifespan() to inject the FileSyncWorker."""
    global _file_sync_worker
    _file_sync_worker = worker

# ── Pydantic models ───────────────────────────────────────────

class UserSummary(BaseModel):
    name: str
    exchange: str
    has_key: bool = False
    has_secret: bool = False
    has_wallet: bool = False
    has_private_key: bool = False
    is_vault: bool = False
    in_use: bool = False
    hl_valid_until: Optional[int] = None
    hl_valid_until_iso: Optional[str] = None
    hl_days_remaining: Optional[int] = None
    hl_expiry_status: Optional[str] = None


class UserDetail(BaseModel):
    name: str
    exchange: str
    key: Optional[str] = None
    secret_masked: Optional[str] = None
    passphrase_masked: Optional[str] = None
    wallet_address: Optional[str] = None
    private_key_masked: Optional[str] = None
    is_vault: bool = False
    quote: Optional[str] = None
    options: Optional[dict] = None
    extra: Optional[dict] = None
    in_use: bool = False
    hl_valid_until: Optional[int] = None
    hl_valid_until_iso: Optional[str] = None
    hl_days_remaining: Optional[int] = None
    hl_expiry_status: Optional[str] = None
    bybit_expires_at_iso: Optional[str] = None
    bybit_days_remaining: Optional[int] = None
    bybit_expiry_status: Optional[str] = None


class UserCreateUpdate(BaseModel):
    exchange: str
    key: Optional[str] = None
    secret: Optional[str] = None
    passphrase: Optional[str] = None
    wallet_address: Optional[str] = None
    private_key: Optional[str] = None
    is_vault: bool = False
    quote: Optional[str] = None
    options: Optional[dict] = None
    extra: Optional[dict] = None


class TestResult(BaseModel):
    success: bool
    balance_futures: Optional[float] = None
    balance_spot: Optional[float] = None
    error: Optional[str] = None


class TestOverride(BaseModel):
    """Optional credential overrides for connection test (unsaved values)."""
    key: Optional[str] = None
    secret: Optional[str] = None
    passphrase: Optional[str] = None
    wallet_address: Optional[str] = None
    private_key: Optional[str] = None


class HLExpiryInfo(BaseModel):
    name: str
    agent_address: Optional[str] = None
    valid_until: Optional[int] = None
    valid_until_iso: Optional[str] = None
    days_remaining: Optional[int] = None
    status: str = "unknown"  # "ok" (>30d), "expiring_soon" (7-30d), "critical" (<7d), "expired", "no_expiry", "error"
    is_vault: bool = False
    error: Optional[str] = None


class BybitExpiryInfo(BaseModel):
    name: str
    expires_at_iso: Optional[str] = None   # ISO date if not IP-bound; "no_expiry" if IP-bound
    days_remaining: Optional[int] = None
    status: str = "unknown"  # "ok" | "expiring_soon" | "critical" | "expired" | "no_expiry" | "error"
    ips: list = []           # live only – never persisted to disk
    error: Optional[str] = None


class BackupEntry(BaseModel):
    filename: str
    ts: str          # ISO datetime (from file mtime)
    size_kb: float
    target: str      # "pb7" or "pb6"


class DiffRequest(BaseModel):
    filename1: str
    filename2: str


# Keys stored by the backend in user.extra that users must not see or edit.
# These are preserved automatically across saves.
_SYSTEM_EXTRA_KEYS: frozenset[str] = frozenset({"hl_valid_until", "bybit_expires_at"})

# ── Helpers ───────────────────────────────────────────────────

def _mask(value: Optional[str], show_chars: int = 4) -> Optional[str]:
    if not value:
        return None
    if len(value) <= show_chars * 2:
        return "*" * len(value)
    return value[:show_chars] + "*" * (len(value) - show_chars * 2) + value[-show_chars:]


def _get_users():
    """Instantiate a fresh Users object to avoid stale state."""
    from User import Users
    return Users()


def _is_user_in_use(user_name: str) -> bool:
    """Check if a user is referenced by any live instance."""
    return user_name in _get_in_use_names()


def _get_in_use_names() -> set[str]:
    """Collect all user names referenced by any instance (built once).

    Multi and V7 instance directories are named after the user directly,
    so we scan the filesystem to stay Streamlit-session-state-free.
    Instances (PB6) still use the Instance loader (directory names include
    symbol, so we need the parsed 'user' field).
    """
    names: set[str] = set()

    # PB6 single instances — directory name is {user}_{symbol}_{type}
    try:
        from Instance import Instances
        for inst in Instances().instances:
            if inst.user:
                names.add(inst.user)
    except Exception:
        pass

    # Multi instances — directory name IS the user name
    try:
        import glob as _glob
        from pathlib import Path as _Path
        for p in _glob.glob(str(_Path.cwd() / "data" / "multi" / "*")):
            name = _Path(p).name
            if name:
                names.add(name)
    except Exception:
        pass

    # V7 instances — directory name IS the user name
    try:
        import glob as _glob
        from pathlib import Path as _Path
        for p in _glob.glob(str(_Path.cwd() / "data" / "run_v7" / "*")):
            name = _Path(p).name
            if name:
                names.add(name)
    except Exception:
        pass

    return names


def _hl_expiry_from_extra(user) -> dict:
    """Extract HL expiry info from the user's extra dict (locally stored)."""
    from datetime import datetime, timezone
    result = {
        "hl_valid_until": None,
        "hl_valid_until_iso": None,
        "hl_days_remaining": None,
        "hl_expiry_status": None,
    }
    if user.exchange != "hyperliquid":
        return result
    extra = user.extra if isinstance(user.extra, dict) else {}
    vu = extra.get("hl_valid_until")
    if vu is None:
        return result
    try:
        vu = int(vu)
        result["hl_valid_until"] = vu
        expiry_dt = datetime.fromtimestamp(vu / 1000, tz=timezone.utc)
        result["hl_valid_until_iso"] = expiry_dt.isoformat()
        days = (expiry_dt - datetime.now(tz=timezone.utc)).days
        result["hl_days_remaining"] = days
        if days < 0:
            result["hl_expiry_status"] = "expired"
        elif days < 7:
            result["hl_expiry_status"] = "critical"
        elif days <= 30:
            result["hl_expiry_status"] = "expiring_soon"
        else:
            result["hl_expiry_status"] = "ok"
    except (ValueError, TypeError, OSError):
        pass
    return result


def _bybit_expiry_from_extra(user) -> dict:
    """Extract Bybit expiry info from the user's extra dict (locally stored date only)."""
    from datetime import datetime, timezone
    result = {
        "bybit_expires_at_iso": None,
        "bybit_days_remaining": None,
        "bybit_expiry_status": None,
    }
    if user.exchange != "bybit":
        return result
    extra = user.extra if isinstance(user.extra, dict) else {}
    eat = extra.get("bybit_expires_at")
    if eat is None:
        return result
    if eat == "no_expiry":
        result["bybit_expiry_status"] = "no_expiry"
        return result
    try:
        expiry_dt = datetime.fromisoformat(eat.replace("Z", "+00:00"))
        result["bybit_expires_at_iso"] = eat
        days = (expiry_dt - datetime.now(tz=timezone.utc)).days
        result["bybit_days_remaining"] = days
        if days < 0:
            result["bybit_expiry_status"] = "expired"
        elif days < 7:
            result["bybit_expiry_status"] = "critical"
        elif days <= 30:
            result["bybit_expiry_status"] = "expiring_soon"
        else:
            result["bybit_expiry_status"] = "ok"
    except (ValueError, TypeError):
        pass
    return result


def _user_to_summary(user, in_use: bool) -> UserSummary:
    hl = _hl_expiry_from_extra(user)
    bybit = _bybit_expiry_from_extra(user)
    return UserSummary(
        name=user.name,
        exchange=user.exchange,
        has_key=bool(user.key),
        has_secret=bool(user.secret),
        has_wallet=bool(user.wallet_address),
        has_private_key=bool(user.private_key),
        is_vault=user.is_vault,
        in_use=in_use,
        **hl,
        **bybit,
    )


def _user_to_detail(user, in_use: bool) -> UserDetail:
    hl = _hl_expiry_from_extra(user)
    bybit = _bybit_expiry_from_extra(user)
    # Strip system-managed keys from the user-editable extra dict so they are
    # never shown in the frontend Extra textarea and cannot be accidentally wiped.
    user_extra = (
        {k: v for k, v in user.extra.items() if k not in _SYSTEM_EXTRA_KEYS}
        if isinstance(user.extra, dict) else None
    ) or None
    return UserDetail(
        name=user.name,
        exchange=user.exchange,
        key=user.key,
        secret_masked=_mask(user.secret),
        passphrase_masked=_mask(user.passphrase),
        wallet_address=user.wallet_address,
        private_key_masked=_mask(user.private_key),
        is_vault=user.is_vault,
        quote=user.quote,
        options=user.options if isinstance(user.options, dict) else None,
        extra=user_extra,
        in_use=in_use,
        **hl,
        **bybit,
    )


# ── Hyperliquid expiry cache ─────────────────────────────────

_hl_expiry_cache: dict[str, HLExpiryInfo] = {}
_hl_expiry_cache_ts: float = 0.0
_hl_expiry_cache_lock = threading.Lock()
_HL_EXPIRY_CACHE_TTL = 300  # 5 minutes


def _query_hl_info(payload: dict) -> Any:
    """POST to Hyperliquid info API."""
    resp = httpx.post(
        "https://api.hyperliquid.xyz/info",
        json=payload,
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def _get_agent_address(private_key: str) -> Optional[str]:
    """Derive the agent/wallet address from a private key using ccxt."""
    try:
        import ccxt
        pk = private_key
        if pk.startswith("0x"):
            pk = pk[2:]
        hl = ccxt.hyperliquid()
        return hl.privateKeyToAddress(pk)
    except Exception:
        return None


def _check_hl_expiry_single(user, users_obj=None) -> HLExpiryInfo:
    """Check expiry for a single Hyperliquid user via HL API and persist to extra."""
    from datetime import datetime, timezone

    info = HLExpiryInfo(name=user.name, is_vault=user.is_vault)

    if not user.private_key:
        info.status = "no_expiry"
        info.error = "No private key configured"
        return info

    agent_addr = _get_agent_address(user.private_key)
    if not agent_addr:
        info.status = "error"
        info.error = "Could not derive agent address from private key"
        return info

    info.agent_address = agent_addr

    try:
        if user.is_vault and user.wallet_address:
            vault_details = _query_hl_info({
                "type": "vaultDetails",
                "vaultAddress": user.wallet_address,
            })
            leader = vault_details.get("leader")
            if not leader:
                info.status = "error"
                info.error = "Could not determine vault leader"
                return info
            agents = _query_hl_info({
                "type": "extraAgents",
                "user": leader,
            })
        else:
            wallet = user.wallet_address or agent_addr
            agents = _query_hl_info({
                "type": "extraAgents",
                "user": wallet,
            })

        matched = None
        for agent in (agents if isinstance(agents, list) else []):
            if isinstance(agent, dict):
                addr = agent.get("address", "")
                if addr.lower() == agent_addr.lower():
                    matched = agent
                    break

        if not matched:
            # Agent address not found in extraAgents — could be a legacy key
            # (no expiry ever registered) OR a revoked key (replaced by a new one).
            # We cannot distinguish the two cases, so we do NOT wipe the
            # existing stored expiry date — that would corrupt the persisted
            # data when the user checks before saving a replacement key.
            info.status = "no_expiry"
            return info

        valid_until = matched.get("validUntil")
        if valid_until is None:
            info.status = "no_expiry"
            return info

        info.valid_until = int(valid_until)
        expiry_dt = datetime.fromtimestamp(int(valid_until) / 1000, tz=timezone.utc)
        info.valid_until_iso = expiry_dt.isoformat()
        now = datetime.now(tz=timezone.utc)
        days_remaining = (expiry_dt - now).days
        info.days_remaining = days_remaining

        if days_remaining < 0:
            info.status = "expired"
        elif days_remaining < 7:
            info.status = "critical"
        elif days_remaining <= 30:
            info.status = "expiring_soon"
        else:
            info.status = "ok"

        # Persist to user.extra → api-keys.json
        _persist_hl_expiry(user, int(valid_until), users_obj)

        return info

    except Exception as e:
        info.status = "error"
        info.error = str(e)
        _log(SERVICE, f"HL expiry check failed for {user.name}: {e}",
             level="WARNING", meta={"traceback": traceback.format_exc()})
        return info


def _persist_hl_expiry(user, valid_until_ms: Optional[int], users_obj=None):
    """Save HL expiry to user.extra and persist to api-keys.json."""
    if not isinstance(user.extra, dict):
        user.extra = {}
    if valid_until_ms is not None:
        user.extra["hl_valid_until"] = valid_until_ms
    else:
        user.extra.pop("hl_valid_until", None)
    if users_obj is not None:
        try:
            users_obj.save()
        except Exception as e:
            _log(SERVICE, f"Failed to persist HL expiry for {user.name}: {e}", level="WARNING")


def _refresh_hl_expiry_cache(users_obj=None) -> dict[str, HLExpiryInfo]:
    """Fetch HL expiry from exchange API for all HL users and persist."""
    global _hl_expiry_cache, _hl_expiry_cache_ts

    now = time.time()
    with _hl_expiry_cache_lock:
        if now - _hl_expiry_cache_ts < _HL_EXPIRY_CACHE_TTL and _hl_expiry_cache:
            return dict(_hl_expiry_cache)

    if users_obj is None:
        users_obj = _get_users()

    result: dict[str, HLExpiryInfo] = {}
    changed = False
    for user in users_obj:
        if user.exchange == "hyperliquid":
            # Capture old value BEFORE _check_hl_expiry_single mutates user.extra in-place
            old_vu = (user.extra or {}).get("hl_valid_until") if isinstance(user.extra, dict) else None
            info = _check_hl_expiry_single(user, users_obj=None)  # don't save per-user
            result[user.name] = info
            # Detect change by comparing against the pre-call value
            new_vu = (user.extra or {}).get("hl_valid_until") if isinstance(user.extra, dict) else None
            if new_vu != old_vu:
                changed = True
            elif info.status == "no_expiry":
                if isinstance(user.extra, dict) and "hl_valid_until" in user.extra:
                    user.extra.pop("hl_valid_until", None)
                    changed = True

    if changed:
        try:
            users_obj.save()
            _log(SERVICE, "Persisted HL expiry data to api-keys.json")
        except Exception as e:
            _log(SERVICE, f"Failed to persist HL expiry data: {e}", level="WARNING")

    with _hl_expiry_cache_lock:
        _hl_expiry_cache = result
        _hl_expiry_cache_ts = time.time()

    return result


# ── Bybit expiry cache ────────────────────────────────────────

_bybit_expiry_cache: dict[str, "BybitExpiryInfo"] = {}
_bybit_expiry_cache_ts: float = 0.0
_bybit_expiry_cache_lock = threading.Lock()
_BYBIT_EXPIRY_CACHE_TTL = 300  # 5 minutes


def _check_bybit_expiry_single(user, users_obj=None) -> "BybitExpiryInfo":
    """Check Bybit API key expiry via GET /v5/user/query-api and persist date to extra."""
    from datetime import datetime, timezone
    import ccxt as _ccxt

    info = BybitExpiryInfo(name=user.name)

    if not user.key or not user.secret:
        info.status = "error"
        info.error = "No API key/secret configured"
        return info

    try:
        ex = _ccxt.bybit({"apiKey": user.key, "secret": user.secret})
        result = ex.privateGetV5UserQueryApi({})
        r = result.get("result", {})

        expires_at = r.get("expiredAt", "")
        ips_raw = r.get("ips", [])
        info.ips = ips_raw if isinstance(ips_raw, list) else []

        # IP-bound keys → expiredAt is epoch "1970-01-01T00:00:00Z" → treat as no_expiry
        if not expires_at or expires_at.startswith("1970-01-01"):
            info.status = "no_expiry"
            _persist_bybit_expiry(user, "no_expiry", users_obj)
            return info

        info.expires_at_iso = expires_at
        expiry_dt = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
        days_remaining = (expiry_dt - datetime.now(tz=timezone.utc)).days
        info.days_remaining = days_remaining

        if days_remaining < 0:
            info.status = "expired"
        elif days_remaining < 7:
            info.status = "critical"
        elif days_remaining <= 30:
            info.status = "expiring_soon"
        else:
            info.status = "ok"

        _persist_bybit_expiry(user, expires_at, users_obj)
        return info

    except Exception as e:
        info.status = "error"
        info.error = str(e)
        import ccxt as _ccxt_err
        if isinstance(e, _ccxt_err.AuthenticationError):
            # Invalid/revoked key — expected condition, no traceback needed
            _log(SERVICE, f"Bybit expiry check failed for {user.name}: {e}",
                 level="WARNING")
        else:
            _log(SERVICE, f"Bybit expiry check failed for {user.name}: {e}",
                 level="WARNING", meta={"traceback": traceback.format_exc()})
        return info


def _persist_bybit_expiry(user, expires_at_value: Optional[str], users_obj=None):
    """Save Bybit expiry date to user.extra and optionally flush api-keys.json."""
    if not isinstance(user.extra, dict):
        user.extra = {}
    if expires_at_value is not None:
        user.extra["bybit_expires_at"] = expires_at_value
    else:
        user.extra.pop("bybit_expires_at", None)
    if users_obj is not None:
        try:
            users_obj.save()
        except Exception as e:
            _log(SERVICE, f"Failed to persist Bybit expiry for {user.name}: {e}", level="WARNING")


def _refresh_bybit_expiry_cache(users_obj=None) -> dict[str, "BybitExpiryInfo"]:
    """Fetch Bybit expiry from exchange API for all Bybit users and persist."""
    global _bybit_expiry_cache, _bybit_expiry_cache_ts

    now = time.time()
    with _bybit_expiry_cache_lock:
        if now - _bybit_expiry_cache_ts < _BYBIT_EXPIRY_CACHE_TTL and _bybit_expiry_cache:
            return dict(_bybit_expiry_cache)

    if users_obj is None:
        users_obj = _get_users()

    result: dict[str, "BybitExpiryInfo"] = {}
    changed = False
    for user in users_obj:
        if user.exchange == "bybit":
            old_val = (user.extra or {}).get("bybit_expires_at") if isinstance(user.extra, dict) else None
            info = _check_bybit_expiry_single(user, users_obj=None)
            result[user.name] = info
            new_val = (user.extra or {}).get("bybit_expires_at") if isinstance(user.extra, dict) else None
            if new_val != old_val:
                changed = True

    if changed:
        try:
            users_obj.save()
            _log(SERVICE, "Persisted Bybit expiry data to api-keys.json")
        except Exception as e:
            _log(SERVICE, f"Failed to persist Bybit expiry data: {e}", level="WARNING")

    with _bybit_expiry_cache_lock:
        _bybit_expiry_cache = result
        _bybit_expiry_cache_ts = time.time()

    return result


# ── Standalone page ───────────────────────────────────────────

@router.get("/main_page", response_class=HTMLResponse)
def get_main_page(
    request: Request,
    st_base: str = Query(default="", description="Browser-visible Streamlit base URL"),
    session: SessionToken = Depends(require_auth),
) -> HTMLResponse:
    """Serve the standalone API Keys editor page with token injected server-side."""
    html_path = _Path(__file__).parent.parent / "frontend" / "api_keys_editor.html"
    html = html_path.read_text(encoding="utf-8")

    # Derive API base from the actual request URL
    scheme = request.url.scheme
    host   = request.url.hostname or "127.0.0.1"
    port   = request.url.port
    origin = f"{scheme}://{host}" + (f":{port}" if port else "")
    api_base = origin + "/api/api-keys"

    html = html.replace('"%%TOKEN%%"',    json.dumps(session.token))
    html = html.replace('"%%API_BASE%%"', json.dumps(api_base))

    # st_base may arrive empty when DOMPurify XML-escapes '&' in the redirect
    # URL (turning &st_base= into amp;st_base=, which FastAPI ignores).
    # Fallback: derive from the request hostname + Streamlit default port.
    if not st_base:
        st_base = f"http://{host}:8501"
    html = html.replace('"%%ST_BASE%%"',  json.dumps(st_base))

    from pbgui_func import PBGUI_VERSION  # noqa: PLC0415 – local import to avoid circular
    html = html.replace('"%%VERSION%%"',  json.dumps(PBGUI_VERSION))
    html = html.replace('%%VERSION%%',    PBGUI_VERSION)

    # Cache-bust pbgui_nav.js with file mtime so browser always loads latest
    nav_js = _Path(__file__).parent.parent / "frontend" / "pbgui_nav.js"
    nav_hash = str(int(nav_js.stat().st_mtime)) if nav_js.exists() else PBGUI_VERSION
    html = html.replace('%%NAV_HASH%%', nav_hash)

    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})


# ── REST endpoints ────────────────────────────────────────────

@router.get("/")
def list_users(
    session: SessionToken = Depends(require_auth),
) -> list[UserSummary]:
    """List all API key users with summary info."""
    users = _get_users()
    in_use_names = _get_in_use_names()
    result = []
    for user in users:
        result.append(_user_to_summary(user, user.name in in_use_names))
    return result


@router.get("/meta")
def get_meta(
    session: SessionToken = Depends(require_auth),
) -> dict:
    """Return api-keys.json editor metadata: serial, timestamp, author."""
    users = _get_users()
    return users.api_meta


@router.get("/exchanges")
def list_exchanges(
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """List available exchanges and their capabilities."""
    from Exchange import Exchanges, Spot, Passphrase, V7
    return {
        "exchanges": Exchanges.list(),
        "spot_exchanges": Spot.list(),
        "passphrase_exchanges": Passphrase.list(),
        "v7_exchanges": V7.list(),
    }


@router.get("/hl-expiry")
def get_hl_expiry_all(
    force: bool = False,
    session: SessionToken = Depends(require_auth),
) -> list[HLExpiryInfo]:
    """Get Hyperliquid API key expiry info for all HL users.
    
    Pass force=true to bypass the 5-minute cache.
    """
    global _hl_expiry_cache_ts

    if force:
        with _hl_expiry_cache_lock:
            _hl_expiry_cache_ts = 0.0

    result = _refresh_hl_expiry_cache()
    return list(result.values())


@router.get("/bybit-expiry")
def get_bybit_expiry_all(
    force: bool = False,
    session: SessionToken = Depends(require_auth),
) -> list[BybitExpiryInfo]:
    """Get Bybit API key expiry info for all Bybit users.

    IPs are included in the response but never persisted to api-keys.json.
    Pass force=true to bypass the 5-minute cache.
    """
    global _bybit_expiry_cache_ts

    if force:
        with _bybit_expiry_cache_lock:
            _bybit_expiry_cache_ts = 0.0

    result = _refresh_bybit_expiry_cache()
    return list(result.values())


# ── Backup / Restore ──────────────────────────────────────────

@router.get("/backups")
def list_backups(
    session: SessionToken = Depends(require_auth),
) -> list[BackupEntry]:
    """List all api-keys backup files from data/api-keys/, newest first.

    The first entries are virtual '_current_pb7' / '_current_pb6' sentinels
    pointing at the live api-keys.json files (no Restore, but diffable).
    """
    from datetime import datetime
    from pbgui_purefunc import is_pb7_installed, is_pb_installed, pb7dir, pbdir

    backup_dir = _Path(_PBGDIR) / "data" / "api-keys"
    backups: list[BackupEntry] = []
    if backup_dir.exists():
        for f in backup_dir.glob("*.json"):
            name = f.name
            if name.startswith("api-keys7_"):
                target = "pb7"
            elif name.startswith("api-keys_"):
                target = "pb6"
            else:
                continue
            stat = f.stat()
            size_kb = round(stat.st_size / 1024, 1)
            ts_iso = datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds")
            backups.append(BackupEntry(filename=name, ts=ts_iso, size_kb=size_kb, target=target))
        backups.sort(key=lambda x: x.ts, reverse=True)

    current: list[BackupEntry] = []
    for sentinel, is_inst_fn, get_dir_fn, tgt in [
        ("_current_pb7", is_pb7_installed, pb7dir, "pb7"),
        ("_current_pb6", is_pb_installed, pbdir, "pb6"),
    ]:
        try:
            if is_inst_fn():
                live = _Path(get_dir_fn()) / "api-keys.json"
                if live.exists():
                    stat = live.stat()
                    current.append(BackupEntry(
                        filename=sentinel,
                        ts=datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds"),
                        size_kb=round(stat.st_size / 1024, 1),
                        target=tgt,
                    ))
        except Exception:
            pass

    return current + backups


@router.post("/backups/restore")
def restore_backup(
    filename: str = Body(..., embed=True),
    session: SessionToken = Depends(require_auth),
) -> dict:
    """Restore api-keys.json from a backup file. Creates a pre-restore snapshot first."""
    import shutil
    from datetime import datetime

    # Security: prevent path traversal and unknown file patterns
    if "/" in filename or "\\" in filename or ".." in filename:
        raise HTTPException(status_code=400, detail="Invalid backup filename")
    if not (filename.startswith("api-keys7_") or filename.startswith("api-keys_")):
        raise HTTPException(status_code=400, detail="Invalid backup filename")
    if not filename.endswith(".json"):
        raise HTTPException(status_code=400, detail="Invalid backup filename")

    backup_dir = _Path(_PBGDIR) / "data" / "api-keys"
    backup_file = backup_dir / filename
    if not backup_file.exists():
        raise HTTPException(status_code=404, detail=f"Backup file not found: {filename}")

    is_pb7_backup = filename.startswith("api-keys7_")
    date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    restored_to = []

    from pbgui_purefunc import is_pb7_installed, is_pb_installed, pb7dir, pbdir

    if is_pb7_backup:
        if not is_pb7_installed():
            raise HTTPException(status_code=400, detail="pb7 is not installed/configured")
        target_path = _Path(pb7dir()) / "api-keys.json"
        if target_path.exists():
            shutil.copy(target_path, backup_dir / f"api-keys7_pre-restore_{date}.json")
        shutil.copy(backup_file, target_path)
        restored_to.append("pb7")
    else:
        if not is_pb_installed():
            raise HTTPException(status_code=400, detail="pb6 is not installed/configured")
        target_path = _Path(pbdir()) / "api-keys.json"
        if target_path.exists():
            shutil.copy(target_path, backup_dir / f"api-keys_pre-restore_{date}.json")
        shutil.copy(backup_file, target_path)
        restored_to.append("pb6")

    _log(SERVICE, f"Restored api-keys.json ({restored_to[0]}) from backup: {filename}")
    return {"restored_to": restored_to, "filename": filename}


@router.post("/backups/diff")
def diff_backups(
    req: DiffRequest,
    session: SessionToken = Depends(require_auth),
) -> dict:
    """Return SequenceMatcher opcodes + line arrays for two backup files.

    filename1/filename2 may be '_current_pb7' or '_current_pb6' to compare
    against the live api-keys.json.
    """
    import difflib as _difflib
    import json as _json_

    _CURRENT_SENTINELS = {"_current_pb7", "_current_pb6"}

    for fn in [req.filename1, req.filename2]:
        if fn in _CURRENT_SENTINELS:
            continue
        if "/" in fn or "\\" in fn or ".." in fn:
            raise HTTPException(status_code=400, detail="Invalid backup filename")
        if not (fn.startswith("api-keys7_") or fn.startswith("api-keys_")):
            raise HTTPException(status_code=400, detail="Invalid backup filename")
        if not fn.endswith(".json"):
            raise HTTPException(status_code=400, detail="Invalid backup filename")

    backup_dir = _Path(_PBGDIR) / "data" / "api-keys"

    def read_lines(filename: str) -> list:
        if filename in _CURRENT_SENTINELS:
            from pbgui_purefunc import pb7dir, pbdir
            live = _Path(pb7dir() if filename == "_current_pb7" else pbdir()) / "api-keys.json"
            if not live.exists():
                raise HTTPException(status_code=404, detail=f"Live api-keys.json not found for {filename}")
            try:
                text = _json_.dumps(
                    _json_.loads(live.read_text(encoding="utf-8")),
                    indent=4, ensure_ascii=False,
                )
            except Exception:
                text = live.read_text(encoding="utf-8")
            return text.splitlines()
        path = backup_dir / filename
        if not path.exists():
            raise HTTPException(status_code=404, detail=f"Backup not found: {filename}")
        try:
            text = _json_.dumps(
                _json_.loads(path.read_text(encoding="utf-8")),
                indent=4, ensure_ascii=False,
            )
        except Exception:
            text = path.read_text(encoding="utf-8")
        return text.splitlines()

    lines1 = read_lines(req.filename1)
    lines2 = read_lines(req.filename2)
    matcher = _difflib.SequenceMatcher(None, lines1, lines2, autojunk=False)
    opcodes = [[tag, i1, i2, j1, j2] for tag, i1, i2, j1, j2 in matcher.get_opcodes()]

    return {
        "filename1": req.filename1,
        "filename2": req.filename2,
        "lines1": lines1,
        "lines2": lines2,
        "opcodes": opcodes,
    }


@router.get("/{name}")
def get_user(
    name: str = PathParam(..., description="User name"),
    session: SessionToken = Depends(require_auth),
) -> UserDetail:
    """Get details for a single API key user (secrets masked)."""
    users = _get_users()
    user = users.find_user(name)
    if not user:
        raise HTTPException(status_code=404, detail=f"User '{name}' not found")
    in_use = _is_user_in_use(name)
    return _user_to_detail(user, in_use)


@router.post("/")
def create_user(
    name: str = Body(..., embed=True),
    data: UserCreateUpdate = Body(..., embed=True),
    session: SessionToken = Depends(require_auth),
) -> UserDetail:
    """Create a new API key user."""
    from User import User
    from Exchange import Exchanges

    if not name or not name.strip():
        raise HTTPException(status_code=400, detail="Username is required")
    name = name.strip()

    if data.exchange not in Exchanges.list():
        raise HTTPException(status_code=400, detail=f"Unknown exchange: {data.exchange}")

    is_hl = data.exchange == "hyperliquid"
    if not is_hl:
        if not data.key:
            raise HTTPException(status_code=400, detail="API Key is required")
        if not data.secret:
            raise HTTPException(status_code=400, detail="API Secret is required")
    else:
        if not data.wallet_address:
            raise HTTPException(status_code=400, detail="Wallet Address is required for Hyperliquid")
        if not data.private_key:
            raise HTTPException(status_code=400, detail="Private Key is required for Hyperliquid")

    users = _get_users()
    if users.find_user(name):
        raise HTTPException(status_code=409, detail=f"User '{name}' already exists")

    user = User()
    user.name = name
    user.exchange = data.exchange
    user.key = data.key
    user.secret = data.secret
    user.passphrase = data.passphrase
    user.wallet_address = data.wallet_address
    user.private_key = data.private_key
    user.is_vault = data.is_vault
    user.quote = data.quote
    user.options = data.options
    user.extra = data.extra or {}

    users.users.append(user)
    users.save()

    _log(SERVICE, f"Created API key user: {name} ({data.exchange})")
    return _user_to_detail(user, False)


@router.put("/{name}")
def update_user(
    data: UserCreateUpdate,
    name: str = PathParam(..., description="User name"),
    session: SessionToken = Depends(require_auth),
) -> UserDetail:
    """Update an existing API key user."""
    from Exchange import Exchanges

    if data.exchange not in Exchanges.list():
        raise HTTPException(status_code=400, detail=f"Unknown exchange: {data.exchange}")

    is_hl = data.exchange == "hyperliquid"
    if not is_hl and not data.key:
        raise HTTPException(status_code=400, detail="API Key is required")
    if is_hl and not data.wallet_address:
        raise HTTPException(status_code=400, detail="Wallet Address is required for Hyperliquid")

    users = _get_users()
    user = users.find_user(name)
    if not user:
        raise HTTPException(status_code=404, detail=f"User '{name}' not found")

    user.exchange = data.exchange
    # Detect actual credential changes BEFORE overwriting (compare new vs stored)
    key_changed = data.key is not None and data.key != user.key
    secret_changed = data.secret is not None and data.secret != user.secret
    private_key_changed = data.private_key is not None and data.private_key != user.private_key
    user.key = data.key
    # Masked fields: null means "leave unchanged" (frontend "••• leave blank to keep" UX)
    if data.secret is not None:
        user.secret = data.secret
    if data.passphrase is not None:
        user.passphrase = data.passphrase
    user.wallet_address = data.wallet_address
    if data.private_key is not None:
        user.private_key = data.private_key
    user.is_vault = data.is_vault
    user.quote = data.quote
    user.options = data.options
    # Merge user-supplied extra with the preserved system-managed keys.
    # System keys (hl_valid_until, bybit_expires_at) must survive a save even if
    # the user clears or never edits the Extra textarea.
    existing_system = {
        k: v for k, v in (user.extra or {}).items() if k in _SYSTEM_EXTRA_KEYS
    }
    # When credentials change, stale expiry data is no longer valid — clear it
    # so the UI shows "—" instead of a wrong cached value until re-checked.
    if private_key_changed and user.exchange == "hyperliquid":
        existing_system.pop("hl_valid_until", None)
    if (key_changed or secret_changed) and user.exchange == "bybit":
        existing_system.pop("bybit_expires_at", None)
    user.extra = {**(data.extra or {}), **existing_system}

    users.save()

    # Invalidate expiry caches when credentials changed
    if private_key_changed and user.exchange == "hyperliquid":
        with _hl_expiry_cache_lock:
            _hl_expiry_cache.pop(name, None)
            _hl_expiry_cache_ts = 0.0
    if (key_changed or secret_changed) and user.exchange == "bybit":
        with _bybit_expiry_cache_lock:
            _bybit_expiry_cache.pop(name, None)
            _bybit_expiry_cache_ts = 0.0

    _log(SERVICE, f"Updated API key user: {name} ({data.exchange})")
    in_use = _is_user_in_use(name)
    return _user_to_detail(user, in_use)


class RenameRequest(BaseModel):
    new_name: str


@router.patch("/{name}/rename")
def rename_user(
    data: RenameRequest,
    name: str = PathParam(..., description="Current user name"),
    session: SessionToken = Depends(require_auth),
) -> UserDetail:
    """Rename an API key user. Fails if the user is in use."""
    new_name = data.new_name.strip()
    if not new_name:
        raise HTTPException(status_code=400, detail="New name must not be empty")
    if new_name == name:
        users = _get_users()
        user = users.find_user(name)
        if not user:
            raise HTTPException(status_code=404, detail=f"User '{name}' not found")
        return _user_to_detail(user, _is_user_in_use(name))

    users = _get_users()
    user = users.find_user(name)
    if not user:
        raise HTTPException(status_code=404, detail=f"User '{name}' not found")
    if _is_user_in_use(name):
        raise HTTPException(
            status_code=409,
            detail=f"User '{name}' is in use by active instances and cannot be renamed",
        )
    if users.find_user(new_name):
        raise HTTPException(status_code=409, detail=f"A user named '{new_name}' already exists")

    user.name = new_name
    users.save()
    _log(SERVICE, f"Renamed API key user: {name} → {new_name}")
    return _user_to_detail(user, False)


@router.delete("/{name}")
def delete_user(
    name: str = PathParam(..., description="User name"),
    session: SessionToken = Depends(require_auth),
) -> dict:
    """Delete an API key user. Fails if the user is in use by any instance."""
    users = _get_users()
    user = users.find_user(name)
    if not user:
        raise HTTPException(status_code=404, detail=f"User '{name}' not found")

    if _is_user_in_use(name):
        raise HTTPException(
            status_code=409,
            detail=f"User '{name}' is in use by active instances and cannot be deleted",
        )

    users.remove_user(name)
    _log(SERVICE, f"Deleted API key user: {name}")
    return {"deleted": name}


# ── Reveal endpoints (must be before /{name}/… catch-alls to avoid shadowing) ──

@router.get("/tradfi/reveal")
def tradfi_reveal(
    field: str = Query(..., description="api_key or api_secret"),
    session: SessionToken = Depends(require_auth),
) -> dict:
    """Return the real (unmasked) TradFi credential for the eye-toggle."""
    if field not in ("api_key", "api_secret"):
        raise HTTPException(status_code=400, detail="field must be api_key or api_secret")
    users = _get_users()
    tradfi = users.tradfi or {}
    value = tradfi.get(field) or ""
    return {"value": value}


@router.get("/{name}/reveal")
def reveal_user_field(
    name: str = PathParam(..., description="User name"),
    field: str = Query(..., description="secret | passphrase | private_key"),
    session: SessionToken = Depends(require_auth),
) -> dict:
    """Return the real (unmasked) credential for the eye-toggle."""
    if field not in ("secret", "passphrase", "private_key"):
        raise HTTPException(status_code=400, detail="field must be secret, passphrase or private_key")
    users = _get_users()
    user = users.find_user(name)
    if not user:
        raise HTTPException(status_code=404, detail=f"User '{name}' not found")
    value = getattr(user, field, None) or ""
    return {"value": value}


# ── TradFi test (must be registered BEFORE /{name}/test to avoid shadowing) ──

class TradFiTestRequest(BaseModel):
    provider: str
    api_key: str = ""
    api_secret: str = ""


@router.post("/tradfi/test")
def tradfi_test_connection(
    req: TradFiTestRequest,
    session: SessionToken = Depends(require_auth),
) -> dict:
    """Test a TradFi data provider connection.

    If api_key is empty the stored config is used, so the user can test
    without re-entering credentials that are already saved.

    NOTE: This route must be registered BEFORE @router.post("/{name}/test")
    because /{name}/test would otherwise shadow /tradfi/test.
    """
    py, pb7dir = _get_pb7_paths()
    if not py or not pb7dir:
        raise HTTPException(status_code=400, detail="pb7 venv/dir not configured")
    api_key = req.api_key
    api_secret = req.api_secret
    if not api_key:
        users = _get_users()
        stored = users.tradfi or {}
        if stored.get("provider") == req.provider:
            api_key = stored.get("api_key", "")
            api_secret = stored.get("api_secret", "")
    # yfinance needs no API key — allow test without one
    if not api_key and req.provider != "yfinance":
        raise HTTPException(status_code=400, detail="No API key provided and none stored for this provider")
    ok, msg = _run_tradfi_test(py, pb7dir, req.provider, api_key, api_secret)
    return {"success": ok, "message": msg}


@router.post("/{name}/test")
def test_connection(
    name: str = PathParam(..., description="User name"),
    override: Optional[TestOverride] = Body(None),
    session: SessionToken = Depends(require_auth),
) -> TestResult:
    """Test API key connection by fetching balance.

    If override fields are supplied they replace the persisted credentials for
    this test only (not saved to disk), allowing users to verify new credentials
    before committing to Save.
    """
    from Exchange import Exchange, Spot
    import copy

    users = _get_users()
    user = users.find_user(name)
    if not user:
        raise HTTPException(status_code=404, detail=f"User '{name}' not found")

    # Apply unsaved overrides if provided
    if override and any(v is not None for v in (override.key, override.secret,
                                                 override.passphrase, override.wallet_address,
                                                 override.private_key)):
        user = copy.copy(user)
        if override.key is not None:
            user.key = override.key
        if override.secret is not None:
            user.secret = override.secret
        if override.passphrase is not None:
            user.passphrase = override.passphrase
        if override.wallet_address is not None:
            user.wallet_address = override.wallet_address
        if override.private_key is not None:
            user.private_key = override.private_key

    exchange = Exchange(user.exchange, user)
    result = TestResult(success=False)

    try:
        balance_futures = exchange.fetch_balance("swap")
        if isinstance(balance_futures, (int, float)):
            result.balance_futures = float(balance_futures)
            result.success = True
        else:
            result.error = str(balance_futures)
    except Exception as e:
        result.error = str(e)
    finally:
        exchange.close()

    # Try spot if exchange supports it
    if result.success and user.exchange in Spot.list():
        try:
            exchange2 = Exchange(user.exchange, user)
            balance_spot = exchange2.fetch_balance("spot")
            if isinstance(balance_spot, (int, float)):
                result.balance_spot = float(balance_spot)
            exchange2.close()
        except Exception:
            pass

    return result


@router.get("/{name}/hl-expiry")
def get_hl_expiry_single(
    name: str = PathParam(..., description="User name"),
    private_key: Optional[str] = Query(None, description="Override private key (unsaved, not persisted)"),
    session: SessionToken = Depends(require_auth),
) -> HLExpiryInfo:
    """Fetch Hyperliquid API key expiry from exchange API.

    If private_key is supplied it overrides the persisted key and the result
    is NOT written to disk (preview-only for verifying a new key before saving).
    """
    users = _get_users()
    user = users.find_user(name)
    if not user:
        raise HTTPException(status_code=404, detail=f"User '{name}' not found")
    if user.exchange != "hyperliquid":
        raise HTTPException(status_code=400, detail=f"User '{name}' is not a Hyperliquid user")

    if private_key:
        # Temporarily override the key for this check only — do NOT persist
        import copy
        user_copy = copy.copy(user)
        user_copy.extra = dict(user.extra) if isinstance(user.extra, dict) else {}
        user_copy.private_key = private_key
        return _check_hl_expiry_single(user_copy, users_obj=None)

    return _check_hl_expiry_single(user, users_obj=users)


@router.get("/{name}/bybit-expiry")
def get_bybit_expiry_single(
    name: str = PathParam(..., description="User name"),
    session: SessionToken = Depends(require_auth),
) -> BybitExpiryInfo:
    """Fetch Bybit API key expiry from exchange API.

    Persists the expiry date (not IPs) to user.extra in api-keys.json.
    IPs are returned in the response but never stored.
    """
    users = _get_users()
    user = users.find_user(name)
    if not user:
        raise HTTPException(status_code=404, detail=f"User '{name}' not found")
    if user.exchange != "bybit":
        raise HTTPException(status_code=400, detail=f"User '{name}' is not a Bybit user")

    return _check_bybit_expiry_single(user, users_obj=users)


# ── Comment fields (top-level _comment_* in api-keys.json) ───

class CommentField(BaseModel):
    key: str
    value: str


@router.get("/comments/list")
def list_comments(
    session: SessionToken = Depends(require_auth),
) -> list[CommentField]:
    """List all top-level _comment_* fields from api-keys.json."""
    users = _get_users()
    extras = users._top_level_extras if isinstance(users._top_level_extras, dict) else {}
    result = []
    for k, v in sorted(extras.items()):
        if str(k).startswith("_comment_"):
            result.append(CommentField(key=k, value=str(v) if v is not None else ""))
    return result


@router.post("/comments/list")
def create_comment(
    field: CommentField,
    session: SessionToken = Depends(require_auth),
) -> CommentField:
    """Create a new _comment_* field."""
    key = field.key.strip()
    if not key.startswith("_comment_"):
        key = "_comment_" + key
    # Validate key: only alphanumeric, underscore, dash
    import re
    if not re.match(r"^_comment_[a-zA-Z0-9_-]+$", key):
        raise HTTPException(status_code=400, detail="Invalid comment key. Use only letters, digits, underscore, dash.")

    users = _get_users()
    if not isinstance(users._top_level_extras, dict):
        users._top_level_extras = {}
    if key in users._top_level_extras:
        raise HTTPException(status_code=409, detail=f"Comment '{key}' already exists")

    users._top_level_extras[key] = field.value
    users.save()
    _log(SERVICE, f"Created comment field: {key}")
    return CommentField(key=key, value=field.value)


@router.put("/comments/list/{key}")
def update_comment(
    key: str = PathParam(..., description="Comment key"),
    value: str = Body(..., embed=True),
    session: SessionToken = Depends(require_auth),
) -> CommentField:
    """Update an existing _comment_* field."""
    users = _get_users()
    extras = users._top_level_extras if isinstance(users._top_level_extras, dict) else {}
    if key not in extras or not str(key).startswith("_comment_"):
        raise HTTPException(status_code=404, detail=f"Comment '{key}' not found")

    users._top_level_extras[key] = value
    users.save()
    _log(SERVICE, f"Updated comment field: {key}")
    return CommentField(key=key, value=value)


@router.delete("/comments/list/{key}")
def delete_comment(
    key: str = PathParam(..., description="Comment key"),
    session: SessionToken = Depends(require_auth),
) -> dict:
    """Delete a _comment_* field."""
    users = _get_users()
    extras = users._top_level_extras if isinstance(users._top_level_extras, dict) else {}
    if key not in extras or not str(key).startswith("_comment_"):
        raise HTTPException(status_code=404, detail=f"Comment '{key}' not found")

    del users._top_level_extras[key]
    users.save()
    _log(SERVICE, f"Deleted comment field: {key}")
    return {"deleted": key}


# ── HL Expiry warning config ─────────────────────────────────

class HLExpiryConfig(BaseModel):
    telegram_warning_days: int = 7


@router.get("/hl-expiry/config")
def get_hl_expiry_config(
    session: SessionToken = Depends(require_auth),
) -> HLExpiryConfig:
    """Get HL expiry Telegram warning config."""
    from pbgui_purefunc import load_ini
    days_str = load_ini("hl_expiry", "telegram_warning_days")
    days = 7
    if days_str:
        try:
            days = int(days_str)
        except ValueError:
            pass
    return HLExpiryConfig(telegram_warning_days=days)


@router.put("/hl-expiry/config")
def update_hl_expiry_config(
    config: HLExpiryConfig,
    session: SessionToken = Depends(require_auth),
) -> dict:
    """Update HL expiry Telegram warning config."""
    from pbgui_purefunc import save_ini
    if config.telegram_warning_days < 1:
        raise HTTPException(status_code=400, detail="Warning days must be >= 1")
    save_ini("hl_expiry", "telegram_warning_days", str(config.telegram_warning_days))
    _log(SERVICE, f"Updated HL expiry warning days: {config.telegram_warning_days}")
    return {"status": "saved", "telegram_warning_days": config.telegram_warning_days}


# ── TradFi provider endpoints ────────────────────────────────

class TradFiConfig(BaseModel):
    provider: str = ""
    api_key: Optional[str] = None
    api_secret: Optional[str] = None


@router.get("/tradfi/config")
def get_tradfi_config(
    session: SessionToken = Depends(require_auth),
) -> TradFiConfig:
    """Get current TradFi data provider config."""
    users = _get_users()
    tradfi = users.tradfi or {}
    return TradFiConfig(
        provider=tradfi.get("provider", ""),
        api_key=_mask(tradfi.get("api_key")),
        api_secret=_mask(tradfi.get("api_secret")),
    )


@router.put("/tradfi/config")
def update_tradfi_config(
    config: TradFiConfig,
    session: SessionToken = Depends(require_auth),
) -> dict:
    """Update TradFi data provider config.

    If api_key / api_secret is an empty string the existing stored value is
    preserved (allows saving provider change without re-entering credentials).
    """
    users = _get_users()
    existing = users.tradfi or {}
    effective_key = config.api_key if config.api_key else existing.get("api_key", "")
    effective_secret = config.api_secret if config.api_secret else existing.get("api_secret", "")
    if config.provider:
        new_tradfi: dict = {"provider": config.provider, "api_key": effective_key}
        if effective_secret:
            new_tradfi["api_secret"] = effective_secret
        users.tradfi = new_tradfi
    else:
        users.tradfi = {}
    users.save()
    _log(SERVICE, f"Updated TradFi config: provider={config.provider}")
    if config.provider and effective_key:
        _save_tradfi_profile(config.provider, effective_key, effective_secret or "")
    return {"status": "saved"}


@router.delete("/tradfi/config")
def clear_tradfi_config(
    session: SessionToken = Depends(require_auth),
) -> dict:
    """Clear TradFi data provider config from api-keys.json."""
    users = _get_users()
    users.tradfi = {}
    users.save()
    _log(SERVICE, "Cleared TradFi config")
    return {"status": "cleared"}


# ── TradFi helpers ────────────────────────────────────────────

TRADFI_PROVIDERS = ["alpaca", "polygon", "finnhub", "alphavantage"]
TRADFI_PROVIDER_NOTES = {
    "alpaca": "Free (IEX feed, 15-min delay — fine for backtesting), 5+ years of 1-minute data. Requires API key + secret. Recommended.",
    "polygon": "Free tier: 2 years of 1-minute data. Paid plans offer extended history.",
    "finnhub": "Free tier does NOT support 1-minute intraday — unusable for backtesting.",
    "alphavantage": "Free tier: 25 calls/day, very limited for backtesting.",
}
TRADFI_NEEDS_SECRET = {"alpaca"}
TRADFI_PROVIDER_LINKS = {
    "alpaca": ("https://app.alpaca.markets/paper-trading/overview", "Get free Alpaca API key"),
    "polygon": ("https://polygon.io/dashboard/signup", "Sign up for Polygon.io"),
    "finnhub": ("https://finnhub.io/register", "Sign up for Finnhub (free)"),
    "alphavantage": ("https://www.alphavantage.co/support/#api-key", "Get free Alpha Vantage API key"),
}


def _get_pb7_paths() -> tuple[Optional[str], Optional[str]]:
    """Return (pb7venv, pb7dir) from pbgui.ini."""
    from pbgui_purefunc import load_ini
    return load_ini("main", "pb7venv"), load_ini("main", "pb7dir")


def _load_tradfi_profiles() -> dict[str, dict[str, str]]:
    """Load all TradFi provider profiles from pbgui.ini."""
    import configparser
    from pathlib import Path
    ini = Path("pbgui.ini")
    parser = configparser.ConfigParser()
    if ini.exists():
        parser.read(ini)
    out: dict[str, dict[str, str]] = {}
    for p in TRADFI_PROVIDERS:
        out[p] = {
            "api_key": parser.get("tradfi_profiles", f"{p}_api_key", fallback=""),
            "api_secret": parser.get("tradfi_profiles", f"{p}_api_secret", fallback=""),
        }
    return out


def _save_tradfi_profile(provider: str, key: str, secret: str) -> None:
    """Save a TradFi provider profile to pbgui.ini."""
    import configparser, os, tempfile
    from pathlib import Path
    ini = Path("pbgui.ini")
    parser = configparser.ConfigParser()
    if ini.exists():
        parser.read(ini)
    if not parser.has_section("tradfi_profiles"):
        parser.add_section("tradfi_profiles")
    parser.set("tradfi_profiles", f"{provider}_api_key", key or "")
    parser.set("tradfi_profiles", f"{provider}_api_secret", secret or "")
    tmp = ini.with_suffix(".ini.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        parser.write(f)
    os.replace(tmp, ini)


def _run_tradfi_test(py: str, pb7_dir: str, provider: str,
                     api_key: str = "", api_secret: str = "") -> tuple[bool, str]:
    """Run TradFi connection test in pb7 venv. Returns (success, message)."""
    import subprocess, tempfile, os, re as _re

    kwargs_parts = []
    if api_key:
        kwargs_parts.append(f"api_key={repr(api_key)}")
    if api_secret:
        kwargs_parts.append(f"api_secret={repr(api_secret)}")
    kw = (", " + ", ".join(kwargs_parts)) if kwargs_parts else ""

    # Finnhub free tier does NOT support resolution=1 (1-min) for US stocks —
    # use the /quote endpoint instead which is available on all plans.
    if provider == "finnhub":
        script = f"""\
import sys, asyncio, aiohttp
async def _test():
    url = "https://finnhub.io/api/v1/quote"
    params = {{"symbol": "AAPL", "token": {repr(api_key)}}}
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as resp:
            if resp.status == 403:
                print("FAIL:403 Forbidden — check your Finnhub API key")
                return
            resp.raise_for_status()
            data = await resp.json()
    # "c" is current price — present if key is valid
    if data.get("c") is not None and data["c"] != 0:
        print(f'OK:AAPL price={{data["c"]}}')
    else:
        print(f'FAIL:unexpected response: {{data}}')
asyncio.run(_test())
"""
    # Alpha Vantage TIME_SERIES_INTRADAY is premium — use GLOBAL_QUOTE (free tier)
    elif provider == "alphavantage":
        script = f"""\
import sys, asyncio, aiohttp
async def _test():
    url = "https://www.alphavantage.co/query"
    params = {{"function": "GLOBAL_QUOTE", "symbol": "AAPL", "apikey": {repr(api_key)}}}
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as resp:
            resp.raise_for_status()
            data = await resp.json()
    if "Note" in data or "Information" in data:
        msg = data.get("Note") or data.get("Information") or "rate limit"
        print(f"FAIL:{{msg}}")
        return
    quote = data.get("Global Quote", {{}})
    price = quote.get("05. price")
    if price:
        print(f"OK:AAPL price={{price}}")
    else:
        print(f"FAIL:unexpected response: {{data}}")
asyncio.run(_test())
"""
    else:
        script = f"""\
import sys, asyncio
sys.path.insert(0, {repr(str(pb7_dir) + '/src')})
from tradfi_data import get_provider
from datetime import datetime, timedelta, timezone

async def _test():
    p = get_provider({repr(provider)}{kw})
    async with p:
        end = datetime.now(timezone.utc) - timedelta(days=1)
        start = end - timedelta(days=7)
        c = await p.fetch_1m_candles(
            'AAPL',
            int(start.timestamp() * 1000),
            int(end.timestamp() * 1000),
        )
        print(f'OK:{{len(c)}}')

asyncio.run(_test())
"""
    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False, encoding="utf-8") as tf:
            tf.write(script)
            tmp = tf.name
        result = subprocess.run([py, tmp], capture_output=True, text=True, timeout=30)
        os.unlink(tmp)
        out = (result.stdout or "").strip()
        err = (result.stderr or "").strip()
        if result.returncode == 0 and out.startswith("OK:"):
            payload = out[3:]
            # Numeric candle count or string (Finnhub quote)
            if payload.isdigit():
                n = int(payload)
                if n > 0:
                    return True, f"Connection OK — {n} candles received (AAPL, last 7 days)."
                err_hint = err.splitlines()[-1] if err else "unknown reason"
                return False, f"0 candles — {err_hint}"
            # Finnhub quote result
            return True, f"Connection OK — {payload}"
        if result.returncode == 0 and out.startswith("FAIL:"):
            return False, out[5:]
        if result.returncode == 0 and "OK:" in out:
            m = _re.search(r"OK:(\d+)", out)
            if m:
                n = int(m.group(1))
                if n > 0:
                    return True, f"Connection OK — {n} candles received (AAPL, last 7 days)."
            err_hint = err.splitlines()[-1] if err else "unknown reason"
            return False, f"0 candles — {err_hint}"
        msg = err.splitlines()[-1] if err else out or "Unknown error"
        return False, f"Test failed: {msg}"
    except subprocess.TimeoutExpired:
        return False, "Test timed out (30s)."
    except Exception as e:
        return False, f"Test error: {e}"


@router.get("/tradfi/yfinance/status")
def tradfi_yfinance_status(
    session: SessionToken = Depends(require_auth),
) -> dict:
    """Check if yfinance is installed in the pb7 venv."""
    import subprocess
    py, _ = _get_pb7_paths()
    if not py:
        return {"installed": False, "version": "", "error": "pb7 venv not configured"}
    try:
        r = subprocess.run(
            [py, "-c", "import yfinance; print(yfinance.__version__)"],
            capture_output=True, text=True, timeout=10,
        )
        if r.returncode == 0:
            return {"installed": True, "version": r.stdout.strip()}
        return {"installed": False, "version": ""}
    except Exception as e:
        return {"installed": False, "version": "", "error": str(e)}


@router.post("/tradfi/yfinance/install")
def tradfi_yfinance_install(
    session: SessionToken = Depends(require_auth),
) -> dict:
    """Install yfinance in the pb7 venv."""
    import subprocess
    py, _ = _get_pb7_paths()
    if not py:
        raise HTTPException(status_code=400, detail="pb7 venv not configured")
    try:
        r = subprocess.run(
            [py, "-m", "pip", "install", "yfinance"],
            capture_output=True, text=True, timeout=120,
        )
        if r.returncode == 0:
            _log(SERVICE, "Installed yfinance in pb7 venv")
            return {"success": True, "message": "yfinance installed successfully"}
        msg = r.stderr.splitlines()[-1] if r.stderr else r.stdout or "Unknown error"
        return {"success": False, "message": f"Install failed: {msg}"}
    except subprocess.TimeoutExpired:
        return {"success": False, "message": "Install timed out (120s)"}
    except Exception as e:
        return {"success": False, "message": str(e)}


@router.post("/tradfi/yfinance/uninstall")
def tradfi_yfinance_uninstall(
    session: SessionToken = Depends(require_auth),
) -> dict:
    """Uninstall yfinance from the pb7 venv."""
    import subprocess
    py, _ = _get_pb7_paths()
    if not py:
        raise HTTPException(status_code=400, detail="pb7 venv not configured")
    try:
        r = subprocess.run(
            [py, "-m", "pip", "uninstall", "yfinance", "-y"],
            capture_output=True, text=True, timeout=60,
        )
        if r.returncode == 0:
            _log(SERVICE, "Uninstalled yfinance from pb7 venv")
            return {"success": True, "message": "yfinance uninstalled"}
        msg = r.stderr.splitlines()[-1] if r.stderr else r.stdout or "Unknown error"
        return {"success": False, "message": f"Uninstall failed: {msg}"}
    except Exception as e:
        return {"success": False, "message": str(e)}


class TradFiTestRequest(BaseModel):
    provider: str
    api_key: str = ""
    api_secret: str = ""

# (tradfi/test route registered earlier, before /{name}/test — see above)


@router.get("/tradfi/profiles")
def tradfi_get_profiles(
    session: SessionToken = Depends(require_auth),
) -> dict:
    """Get saved TradFi provider profiles from pbgui.ini."""
    profiles = _load_tradfi_profiles()
    # Mask secrets
    for p in profiles.values():
        p["api_key"] = _mask(p.get("api_key")) if p.get("api_key") else ""
        p["api_secret"] = _mask(p.get("api_secret")) if p.get("api_secret") else ""
    return {
        "providers": TRADFI_PROVIDERS,
        "provider_notes": TRADFI_PROVIDER_NOTES,
        "provider_links": {p: {"url": url, "label": lbl} for p, (url, lbl) in TRADFI_PROVIDER_LINKS.items()},
        "needs_secret": list(TRADFI_NEEDS_SECRET),
        "profiles": profiles,
    }


# ── API Sync (rclone / VPS distribution) ─────────────────────

def _get_pbremote() -> Any:
    """Instantiate PBRemote. Returns the instance (may have .error set)."""
    from PBRemote import PBRemote
    return PBRemote()


@router.get("/sync/status")
def get_sync_status(
    session: SessionToken = Depends(require_auth),
) -> dict:
    """Check whether api-keys.json is in sync with all remote (VPS) servers.

    Returns:
      - configured: False when rclone / PBRemote is not set up
      - synced: True if all online servers have the same API key MD5
      - unsynced_servers: names of servers that are out of sync
      - total_servers: count of known remote servers
    """
    try:
        pbremote = _get_pbremote()
        if pbremote.error:
            return {"configured": False, "synced": True, "unsynced_servers": [], "total_servers": 0, "error": pbremote.error}
        total = len(pbremote.remote_servers)
        unsynced = [
            s.name for s in pbremote.remote_servers
            if s.is_online() and not s.is_api_md5_same(pbremote.api_md5)
        ]
        return {
            "configured": True,
            "synced": len(unsynced) == 0,
            "unsynced_servers": unsynced,
            "total_servers": total,
        }
    except Exception as e:
        _log(SERVICE, f"sync/status error: {e}", level="WARNING", meta={"traceback": traceback.format_exc()})
        return {"configured": False, "synced": True, "unsynced_servers": [], "total_servers": 0, "error": str(e)}


@router.post("/sync/push")
def push_sync(
    session: SessionToken = Depends(require_auth),
) -> dict:
    """Push current api-keys.json to rclone remote storage for VPS distribution.

    Calls sync_api_up() (copies api-keys to data/cmd/) then sync('up','cmd')
    to upload via rclone immediately. Poll GET /sync/status to confirm delivery.
    """
    try:
        pbremote = _get_pbremote()
        if pbremote.error:
            raise HTTPException(status_code=400, detail=f"PBRemote not configured: {pbremote.error}")
        pbremote.sync_api_up()
        pbremote.sync("up", "cmd")
        _log(SERVICE, "API keys pushed to rclone remote storage")
        return {"success": True}
    except HTTPException:
        raise
    except Exception as e:
        _log(SERVICE, f"sync/push error: {e}", level="ERROR", meta={"traceback": traceback.format_exc()})
        raise HTTPException(status_code=500, detail=str(e))


# ── SSH API Sync (direct SFTP push via AsyncSSHPool) ─────────

class SSHPushRequest(BaseModel):
    hostnames: Optional[list[str]] = None
    dry_run: bool = False
    no_propagate: bool = False


class RetentionRequest(BaseModel):
    hostname: str
    backup_retention_days: int = Field(ge=1, le=3650)
    backup_min_versions: int = Field(ge=1, le=1000)


@router.post("/sync/push-ssh")
async def push_ssh(
    req: SSHPushRequest,
    session: SessionToken = Depends(require_auth),
) -> dict:
    """Push api-keys.json to VPS(es) via SSH/SFTP.

    Backs up remote file, pushes with MD5 verify, cleans retention,
    and kills affected bots (PBRun auto-restarts them).
    """
    if not _file_sync_worker:
        raise HTTPException(status_code=503,
                            detail="FileSyncWorker not initialized")
    try:
        results = await _file_sync_worker.push_api_keys(
            hostnames=req.hostnames,
            dry_run=req.dry_run,
            no_propagate=req.no_propagate,
        )
        # push_api_keys returns {"error": "..."} for pre-flight failures
        # (no local file, no connected VPS). Raise 400 so JS catch handles it.
        if isinstance(results, dict) and "error" in results and len(results) == 1:
            raise HTTPException(status_code=400, detail=results["error"])
        return {"results": results}
    except Exception as e:
        _log(SERVICE, f"sync/push-ssh error: {e}", level="ERROR",
             meta={"traceback": traceback.format_exc()})
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/sync/ssh-status")
async def get_ssh_sync_status(
    session: SessionToken = Depends(require_auth),
) -> dict:
    """Get SSH sync status: connected hosts, watchers, serial."""
    if not _file_sync_worker:
        raise HTTPException(status_code=503,
                            detail="FileSyncWorker not initialized")
    return _file_sync_worker.get_status()


@router.get("/sync/ssh-status/stream")
async def stream_ssh_sync_status(
    session: SessionToken = Depends(require_auth),
) -> StreamingResponse:
    """SSE stream that pushes serial-update events when VPS watchers detect changes."""
    if not _file_sync_worker:
        raise HTTPException(status_code=503,
                            detail="FileSyncWorker not initialized")
    q = _file_sync_worker.subscribe_sse()

    async def event_gen():
        import json
        try:
            # Send initial state so the client can populate all serial cells
            status = _file_sync_worker.get_status()
            yield f"data: {json.dumps({'type': 'init', **status})}\n\n"
            while True:
                try:
                    msg = await asyncio.wait_for(q.get(), timeout=25)
                    msg["type"] = "serial_update"
                    yield f"data: {json.dumps(msg)}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        except (GeneratorExit, asyncio.CancelledError):
            pass
        finally:
            _file_sync_worker.unsubscribe_sse(q)

    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/sync/ssh-retention/{hostname}")
async def get_ssh_retention(
    hostname: str = PathParam(...),
    session: SessionToken = Depends(require_auth),
) -> dict:
    """Read backup retention settings from a VPS's pbgui.ini."""
    if not _file_sync_worker:
        raise HTTPException(status_code=503,
                            detail="FileSyncWorker not initialized")
    return await _file_sync_worker.get_retention_settings(hostname)


@router.put("/sync/ssh-retention")
async def set_ssh_retention(
    req: RetentionRequest,
    session: SessionToken = Depends(require_auth),
) -> dict:
    """Update backup retention settings on a VPS's pbgui.ini."""
    if not _file_sync_worker:
        raise HTTPException(status_code=503,
                            detail="FileSyncWorker not initialized")
    ok = await _file_sync_worker.set_retention_settings(
        req.hostname, req.backup_retention_days, req.backup_min_versions)
    if not ok:
        raise HTTPException(status_code=500,
                            detail=f"Failed to write retention to {req.hostname}")
    return {
        "success": True,
        "hostname": req.hostname,
        "backup_retention_days": req.backup_retention_days,
        "backup_min_versions": req.backup_min_versions,
    }

