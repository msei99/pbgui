"""
FastAPI router: API-Keys management endpoints.

Provides CRUD operations for API key management, connection testing,
Hyperliquid/Bybit key expiry checking (with local state-file persistence), and
top-level comment field management in api-keys.json.
All endpoints require auth (Bearer token).
"""
from __future__ import annotations

import json
from functools import wraps
import os
import re
import subprocess
import threading
import time
import traceback
import uuid
from pathlib import Path as _Path
from typing import Any, Optional

import httpx
from fastapi import APIRouter, Body, Depends, HTTPException, Path as PathParam, Query, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from api.auth import SessionToken, require_auth
from api_key_state import (
    RUNTIME_STATE_KEYS,
    clear_user_state,
    delete_user_state,
    get_user_state,
    rename_user_state,
    strip_runtime_extra,
    update_user_state,
)
from cluster_credential_publisher import ClusterCredentialPublisher
from credential_store import CredentialNotFoundError, CredentialStore, credential_mutation_lock
from credential_reconciler import reconcile_pending_credentials
from logging_helpers import human_log as _log
from file_lock import advisory_file_lock
from master.cluster_state import default_cluster_root, ensure_local_identity, read_local_identity, rebuild_materialized_state
from pb7_api_keys import PB7ApiKeysMergeWriter, project_active_tradfi_profiles
import pbgui_purefunc
from pbgui_purefunc import PBGDIR as _PBGDIR

SERVICE = "ApiKeys"

router = APIRouter()


def _serialized_api_keys_write(func):
    """Hold the API-key transaction lock across load, validation, and save."""
    @wraps(func)
    def wrapped(*args, **kwargs):
        lock_target = _Path(_PBGDIR) / "data" / "api-keys" / ".write"
        with advisory_file_lock(lock_target):
            return func(*args, **kwargs)

    return wrapped

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
    error: Optional[str] = None


class TestOverride(BaseModel):
    """Optional credential overrides for connection test (unsaved values)."""
    key: Optional[str] = None
    secret: Optional[str] = None
    passphrase: Optional[str] = None
    wallet_address: Optional[str] = None
    private_key: Optional[str] = None


class HLExpiryOverride(BaseModel):
    """Optional unsaved private key used only for an expiry preview."""
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
    ips: list[str] = Field(default_factory=list)
    error: Optional[str] = None


class BackupEntry(BaseModel):
    filename: str
    ts: str          # ISO datetime (from file mtime)
    size_kb: float
    target: str      # "pb7"


class DiffRequest(BaseModel):
    filename1: str
    filename2: str


# Legacy runtime keys that may still exist in older api-keys.json files.
# They are hidden from the editor and stripped automatically on the next save.
_LEGACY_RUNTIME_EXTRA_KEYS: frozenset[str] = RUNTIME_STATE_KEYS

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


def _credential_store() -> CredentialStore:
    """Return the local owner-only credential vault."""

    from credential_rolling_bootstrap import bootstrap_local_legacy_credentials

    bootstrap_local_legacy_credentials(_Path(_PBGDIR))
    return CredentialStore(_Path(_PBGDIR) / "data" / "credentials")


def _cluster_credential_publisher(store: CredentialStore) -> ClusterCredentialPublisher:
    """Return a publisher for the local master, creating its identity if needed."""

    cluster_root = default_cluster_root(_Path(_PBGDIR))
    snapshot = pbgui_purefunc.load_ini_snapshot()
    pbname = snapshot.get("main", "pbname", fallback="").strip() or os.uname().nodename
    ensure_local_identity(cluster_root, role="master", pbname=pbname)
    identity = read_local_identity(cluster_root)
    if str(identity.get("role") or "").strip().lower() != "master":
        raise HTTPException(status_code=409, detail="TradFi credentials can only be managed on a master")
    return ClusterCredentialPublisher(cluster_root, store)


def _project_local_tradfi(
    store: CredentialStore,
    pending_profile_id: str | None = None,
) -> dict[str, Any]:
    """Project active vault profiles into the reserved PB7 TradFi subtree."""

    _py, pb7_dir = _get_pb7_paths()
    if not pb7_dir:
        raise HTTPException(status_code=409, detail="PB7 directory is not configured")
    return project_active_tradfi_profiles(
        store,
        _Path(pb7_dir) / "api-keys.json",
        pending_profile_id=pending_profile_id,
    )


def _is_user_in_use(user_name: str) -> bool:
    """Check if a user is referenced by any live instance."""
    return user_name in _get_in_use_names()


def _get_in_use_names() -> set[str]:
    """Collect all user names referenced by any instance (built once).

    V7 instance directories are named after the user directly,
    so we scan the filesystem to stay Streamlit-session-state-free.
    """
    names: set[str] = set()

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


def _hl_expiry_from_state(user) -> dict:
    """Extract HL expiry info from the local runtime state file."""
    from datetime import datetime, timezone
    result = {
        "hl_valid_until": None,
        "hl_valid_until_iso": None,
        "hl_days_remaining": None,
        "hl_expiry_status": None,
    }
    if user.exchange != "hyperliquid":
        return result
    state = get_user_state(user.name)
    vu = state.get("hl_valid_until")
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


def _bybit_expiry_from_state(user) -> dict:
    """Extract Bybit expiry info from the local runtime state file."""
    from datetime import datetime, timezone
    result = {
        "bybit_expires_at_iso": None,
        "bybit_days_remaining": None,
        "bybit_expiry_status": None,
    }
    if user.exchange != "bybit":
        return result
    state = get_user_state(user.name)
    eat = state.get("bybit_expires_at")
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
    hl = _hl_expiry_from_state(user)
    bybit = _bybit_expiry_from_state(user)
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
    hl = _hl_expiry_from_state(user)
    bybit = _bybit_expiry_from_state(user)
    user_extra = strip_runtime_extra(user.extra) or None
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
    """Check expiry for a single Hyperliquid user and persist to the local state file."""
    from datetime import datetime, timezone

    info = HLExpiryInfo(name=user.name, is_vault=user.is_vault)

    if not user.private_key:
        info.status = "no_expiry"
        info.error = "No private key configured"
        clear_user_state(user.name, ("hl_valid_until",))
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
            info.status = "no_expiry"
            clear_user_state(user.name, ("hl_valid_until",))
            return info

        valid_until = matched.get("validUntil")
        if valid_until is None:
            info.status = "no_expiry"
            clear_user_state(user.name, ("hl_valid_until",))
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

        update_user_state(user.name, hl_valid_until=int(valid_until))

        return info

    except Exception as e:
        info.status = "error"
        info.error = str(e)
        _log(SERVICE, f"HL expiry check failed for {user.name}: {e}",
             level="WARNING", meta={"traceback": traceback.format_exc()})
        return info
def _refresh_hl_expiry_cache(users_obj=None) -> dict[str, HLExpiryInfo]:
    """Fetch HL expiry from exchange API for all HL users and persist to local state."""
    global _hl_expiry_cache, _hl_expiry_cache_ts

    now = time.time()
    with _hl_expiry_cache_lock:
        if now - _hl_expiry_cache_ts < _HL_EXPIRY_CACHE_TTL and _hl_expiry_cache:
            return dict(_hl_expiry_cache)

    if users_obj is None:
        users_obj = _get_users()

    result: dict[str, HLExpiryInfo] = {}
    for user in users_obj:
        if user.exchange == "hyperliquid":
            info = _check_hl_expiry_single(user, users_obj=None)
            result[user.name] = info

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
    """Check Bybit API key expiry and persist date/IPs to the local state file."""
    from datetime import datetime, timezone
    import ccxt as _ccxt

    info = BybitExpiryInfo(name=user.name)

    if not user.key or not user.secret:
        info.status = "error"
        info.error = "No API key/secret configured"
        clear_user_state(user.name, ("bybit_expires_at", "bybit_ips"))
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
            update_user_state(user.name, bybit_expires_at="no_expiry", bybit_ips=info.ips)
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

        update_user_state(user.name, bybit_expires_at=expires_at, bybit_ips=info.ips)
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
def _refresh_bybit_expiry_cache(users_obj=None) -> dict[str, "BybitExpiryInfo"]:
    """Fetch Bybit expiry from exchange API for all Bybit users and persist to local state."""
    global _bybit_expiry_cache, _bybit_expiry_cache_ts

    now = time.time()
    with _bybit_expiry_cache_lock:
        if now - _bybit_expiry_cache_ts < _BYBIT_EXPIRY_CACHE_TTL and _bybit_expiry_cache:
            return dict(_bybit_expiry_cache)

    if users_obj is None:
        users_obj = _get_users()

    result: dict[str, "BybitExpiryInfo"] = {}
    for user in users_obj:
        if user.exchange == "bybit":
            info = _check_bybit_expiry_single(user, users_obj=None)
            result[user.name] = info

    with _bybit_expiry_cache_lock:
        _bybit_expiry_cache = result
        _bybit_expiry_cache_ts = time.time()

    return result


# ── Standalone page ───────────────────────────────────────────

@router.get("/main_page", response_class=HTMLResponse)
def get_main_page(
    request: Request,
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

    from pbgui_purefunc import PBGUI_VERSION
    from pbgui_purefunc import PBGUI_SERIAL
    html = html.replace('"%%VERSION%%"',  json.dumps(PBGUI_VERSION))
    html = html.replace('%%VERSION%%',    PBGUI_VERSION)
    html = html.replace('"%%SERIAL%%"',   json.dumps(PBGUI_SERIAL))
    html = html.replace('%%SERIAL%%',     PBGUI_SERIAL)

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
    from Exchange import Exchanges, Passphrase, V7
    return {
        "exchanges": Exchanges.list(),
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

    IPs are included in the response and cached in the local state file, never in api-keys.json.
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

    The first entry is a virtual '_current_pb7' sentinel
    pointing at the live api-keys.json file (no Restore, but diffable).
    """
    from datetime import datetime
    from pbgui_purefunc import is_pb7_installed, pb7dir

    backup_dir = _Path(_PBGDIR) / "data" / "api-keys"
    backups: list[BackupEntry] = []
    if backup_dir.exists():
        for f in backup_dir.glob("*.json"):
            name = f.name
            if name.startswith("api-keys7_"):
                target = "pb7"
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
@_serialized_api_keys_write
def restore_backup(
    filename: str = Body(..., embed=True),
    session: SessionToken = Depends(require_auth),
) -> dict:
    """Restore api-keys.json from a backup file. Creates a pre-restore snapshot first."""
    from datetime import datetime
    from secure_files import ensure_private_directory, secure_private_file

    # Security: prevent path traversal and unknown file patterns
    if "/" in filename or "\\" in filename or ".." in filename:
        raise HTTPException(status_code=400, detail="Invalid backup filename")
    if not filename.startswith("api-keys7_"):
        raise HTTPException(status_code=400, detail="Invalid backup filename")
    if not filename.endswith(".json"):
        raise HTTPException(status_code=400, detail="Invalid backup filename")

    backup_dir = _Path(_PBGDIR) / "data" / "api-keys"
    ensure_private_directory(backup_dir)
    backup_file = backup_dir / filename
    if backup_file.is_symlink():
        raise HTTPException(status_code=400, detail="Refusing symlinked backup file")
    if not backup_file.exists():
        raise HTTPException(status_code=404, detail=f"Backup file not found: {filename}")
    secure_private_file(backup_file)
    try:
        backup_payload = json.loads(backup_file.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=400, detail="Backup file is not valid JSON") from exc
    if not isinstance(backup_payload, dict):
        raise HTTPException(status_code=400, detail="Backup file must contain a JSON object")

    is_pb7_backup = filename.startswith("api-keys7_")
    date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    restored_to = []

    from pbgui_purefunc import is_pb7_installed, pb7dir

    if is_pb7_backup:
        if not is_pb7_installed():
            raise HTTPException(status_code=400, detail="pb7 is not installed/configured")
        target_path = _Path(pb7dir()) / "api-keys.json"
        store = _credential_store()
        writer = PB7ApiKeysMergeWriter(target_path, store.root / "pb7_projection.json")
        with credential_mutation_lock(store.root):
            writer.restore_exchange_and_project(
                backup_payload,
                store,
                backup_path=(
                    backup_dir / f"api-keys7_pre-restore_{date}.json"
                    if target_path.exists()
                    else None
                ),
            )
        restored_to.append("pb7")

    _log(SERVICE, f"Restored api-keys.json ({restored_to[0]}) from backup: {filename}")
    return {"restored_to": restored_to, "filename": filename}


@router.post("/backups/diff")
def diff_backups(
    req: DiffRequest,
    session: SessionToken = Depends(require_auth),
) -> dict:
    """Return SequenceMatcher opcodes + line arrays for two backup files.

    filename1/filename2 may be '_current_pb7' to compare
    against the live api-keys.json.
    """
    import difflib as _difflib
    import json as _json_

    _CURRENT_SENTINELS = {"_current_pb7"}

    for fn in [req.filename1, req.filename2]:
        if fn in _CURRENT_SENTINELS:
            continue
        if "/" in fn or "\\" in fn or ".." in fn:
            raise HTTPException(status_code=400, detail="Invalid backup filename")
        if not fn.startswith("api-keys7_"):
            raise HTTPException(status_code=400, detail="Invalid backup filename")
        if not fn.endswith(".json"):
            raise HTTPException(status_code=400, detail="Invalid backup filename")

    backup_dir = _Path(_PBGDIR) / "data" / "api-keys"

    def read_lines(filename: str) -> list:
        if filename in _CURRENT_SENTINELS:
            from pbgui_purefunc import pb7dir
            live = _Path(pb7dir()) / "api-keys.json"
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
@_serialized_api_keys_write
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
    user.extra = strip_runtime_extra(data.extra)

    users.users.append(user)
    users.save()
    delete_user_state(name)

    _log(SERVICE, f"Created API key user: {name} ({data.exchange})")
    return _user_to_detail(user, False)


@router.put("/{name}")
@_serialized_api_keys_write
def update_user(
    data: UserCreateUpdate,
    name: str = PathParam(..., description="User name"),
    session: SessionToken = Depends(require_auth),
) -> UserDetail:
    """Update an existing API key user."""
    from Exchange import Exchanges
    global _hl_expiry_cache_ts, _bybit_expiry_cache_ts

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

    old_exchange = user.exchange
    old_wallet_address = user.wallet_address
    old_is_vault = user.is_vault
    # Detect actual credential changes BEFORE overwriting (compare new vs stored)
    key_changed = data.key is not None and data.key != user.key
    secret_changed = data.secret is not None and data.secret != user.secret
    private_key_changed = data.private_key is not None and data.private_key != user.private_key
    wallet_changed = data.wallet_address != old_wallet_address
    vault_changed = data.is_vault != old_is_vault
    exchange_changed = data.exchange != old_exchange
    user.exchange = data.exchange
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
    user.extra = strip_runtime_extra(data.extra)

    users.save()

    if exchange_changed:
        clear_user_state(name)
    else:
        if user.exchange == "hyperliquid" and (private_key_changed or wallet_changed or vault_changed):
            clear_user_state(name, ("hl_valid_until",))
        if user.exchange == "bybit" and (key_changed or secret_changed):
            clear_user_state(name, ("bybit_expires_at", "bybit_ips"))

    # Invalidate expiry caches when credentials changed
    if exchange_changed or old_exchange == "hyperliquid" or user.exchange == "hyperliquid":
        with _hl_expiry_cache_lock:
            _hl_expiry_cache.pop(name, None)
            _hl_expiry_cache_ts = 0.0
    if exchange_changed or old_exchange == "bybit" or user.exchange == "bybit":
        with _bybit_expiry_cache_lock:
            _bybit_expiry_cache.pop(name, None)
            _bybit_expiry_cache_ts = 0.0

    _log(SERVICE, f"Updated API key user: {name} ({data.exchange})")
    in_use = _is_user_in_use(name)
    return _user_to_detail(user, in_use)


class RenameRequest(BaseModel):
    new_name: str


@router.patch("/{name}/rename")
@_serialized_api_keys_write
def rename_user(
    data: RenameRequest,
    name: str = PathParam(..., description="Current user name"),
    session: SessionToken = Depends(require_auth),
) -> UserDetail:
    """Rename an API key user. Fails if the user is in use."""
    global _hl_expiry_cache_ts, _bybit_expiry_cache_ts
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
    rename_user_state(name, new_name)
    with _hl_expiry_cache_lock:
        _hl_expiry_cache.pop(name, None)
        _hl_expiry_cache.pop(new_name, None)
        _hl_expiry_cache_ts = 0.0
    with _bybit_expiry_cache_lock:
        _bybit_expiry_cache.pop(name, None)
        _bybit_expiry_cache.pop(new_name, None)
        _bybit_expiry_cache_ts = 0.0
    _log(SERVICE, f"Renamed API key user: {name} → {new_name}")
    return _user_to_detail(user, False)


@router.delete("/{name}")
@_serialized_api_keys_write
def delete_user(
    name: str = PathParam(..., description="User name"),
    session: SessionToken = Depends(require_auth),
) -> dict:
    """Delete an API key user. Fails if the user is in use by any instance."""
    global _hl_expiry_cache_ts, _bybit_expiry_cache_ts
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
    delete_user_state(name)
    with _hl_expiry_cache_lock:
        _hl_expiry_cache.pop(name, None)
        _hl_expiry_cache_ts = 0.0
    with _bybit_expiry_cache_lock:
        _bybit_expiry_cache.pop(name, None)
        _bybit_expiry_cache_ts = 0.0
    _log(SERVICE, f"Deleted API key user: {name}")
    return {"deleted": name}


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
    profile_id: Optional[str] = None
    provider: str = ""
    api_key: Optional[str] = None
    api_secret: Optional[str] = None


@router.post("/tradfi/test")
def tradfi_test_connection(
    req: TradFiTestRequest,
    session: SessionToken = Depends(require_auth),
) -> dict:
    """Test a TradFi data provider connection.

    A saved profile ID and one-time request-body credentials are mutually
    exclusive so stored secrets never need to round-trip through the browser.

    NOTE: This route must be registered BEFORE @router.post("/{name}/test")
    because /{name}/test would otherwise shadow /tradfi/test.
    """
    py, pb7_dir = _get_pb7_paths()
    if not py or not pb7_dir:
        raise HTTPException(status_code=400, detail="pb7 venv/dir not configured")
    has_unsaved_secret = req.api_key is not None or req.api_secret is not None
    if req.profile_id and has_unsaved_secret:
        raise HTTPException(status_code=400, detail="Use either profile_id or one-time credentials")

    provider = req.provider.strip().lower()
    api_key = str(req.api_key or "").strip()
    api_secret = str(req.api_secret or "").strip()
    if req.profile_id:
        store = _credential_store()
        try:
            record = store.get_tradfi(req.profile_id)
            credentials = store.load_tradfi_credentials(req.profile_id)
        except (CredentialNotFoundError, ValueError) as exc:
            raise HTTPException(status_code=404, detail="TradFi profile not found") from exc
        provider = str(record.get("provider") or "").strip().lower()
        api_key = _tradfi_api_key(credentials)
        api_secret = _tradfi_api_secret(credentials)
    elif not has_unsaved_secret and provider != "yfinance":
        raise HTTPException(status_code=400, detail="Provide a stored profile_id or one-time credentials")

    if provider not in {*TRADFI_PROVIDERS, "yfinance"}:
        raise HTTPException(status_code=400, detail="Unknown TradFi provider")
    if not api_key and provider != "yfinance":
        raise HTTPException(status_code=400, detail="TradFi API key is required")
    ok, msg = _run_tradfi_test(py, pb7_dir, provider, api_key, api_secret)
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
    from Exchange import Exchange
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

    return result


def _get_hl_expiry_for_user(name: str, private_key: Optional[str] = None) -> HLExpiryInfo:
    """Check one user's saved key or an unsaved preview override."""
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


@router.get("/{name}/hl-expiry")
def get_hl_expiry_single(
    name: str = PathParam(..., description="User name"),
    session: SessionToken = Depends(require_auth),
) -> HLExpiryInfo:
    """Fetch Hyperliquid API key expiry using the persisted private key."""
    return _get_hl_expiry_for_user(name)


@router.post("/{name}/hl-expiry")
def preview_hl_expiry_single(
    override: Optional[HLExpiryOverride] = Body(None),
    name: str = PathParam(..., description="User name"),
    session: SessionToken = Depends(require_auth),
) -> HLExpiryInfo:
    """Preview Hyperliquid expiry with an unsaved private key in the request body."""
    return _get_hl_expiry_for_user(name, override.private_key if override else None)


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
@_serialized_api_keys_write
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
@_serialized_api_keys_write
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
@_serialized_api_keys_write
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


class HLExpiryConfigResponse(HLExpiryConfig):
    configured: bool = False


@router.get("/hl-expiry/config")
def get_hl_expiry_config(
    session: SessionToken = Depends(require_auth),
) -> HLExpiryConfigResponse:
    """Get HL expiry Telegram warning config and whether it is explicitly set."""
    from pbgui_purefunc import load_ini
    days_str = load_ini("hl_expiry", "telegram_warning_days")
    days = 7
    configured = False
    if days_str:
        try:
            parsed_days = int(days_str)
            if parsed_days >= 1:
                days = parsed_days
                configured = True
        except ValueError:
            pass
    return HLExpiryConfigResponse(telegram_warning_days=days, configured=configured)


@router.put("/hl-expiry/config")
def update_hl_expiry_config(
    config: HLExpiryConfig,
    session: SessionToken = Depends(require_auth),
) -> HLExpiryConfigResponse:
    """Update HL expiry Telegram warning config."""
    from pbgui_purefunc import save_ini
    if config.telegram_warning_days < 1:
        raise HTTPException(status_code=400, detail="Warning days must be >= 1")
    save_ini("hl_expiry", "telegram_warning_days", str(config.telegram_warning_days))
    _log(SERVICE, f"Updated HL expiry warning days: {config.telegram_warning_days}")
    return HLExpiryConfigResponse(telegram_warning_days=config.telegram_warning_days, configured=True)


# ── TradFi provider endpoints ────────────────────────────────

class TradFiConfig(BaseModel):
    profile_id: Optional[str] = None
    provider: str = ""
    label: str = ""
    active: bool = True
    shared: bool = True
    api_key: Optional[str] = None
    api_secret: Optional[str] = None
    operation_id: Optional[str] = Field(default=None, min_length=1, max_length=128)
    create_new: bool = False


class TradFiProfileMetadata(BaseModel):
    id: Optional[str] = None
    provider: str = ""
    label: str = ""
    active: bool = False
    shared: bool = False
    generation: int = 0
    configured: bool = False
    has_api_key: bool = False
    has_api_secret: bool = False
    origin: str = ""
    pending: bool = False
    pending_delete: bool = False
    pending_stage: str = ""
    pending_operation_id: str = ""
    last_operation_id: str = ""
    replicated_active: bool = False
    activation_generation: int = 0
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class TradFiProjectionRetry(BaseModel):
    operation_id: Optional[str] = Field(default=None, min_length=1, max_length=128)


def _tradfi_api_key(credentials: dict[str, str]) -> str:
    """Return a provider API key from supported vault field aliases."""

    for key in ("api_key", "token", "key"):
        value = str(credentials.get(key) or "").strip()
        if value:
            return value
    return ""


def _tradfi_api_secret(credentials: dict[str, str]) -> str:
    """Return a provider secret from supported vault field aliases."""

    for key in ("api_secret", "secret"):
        value = str(credentials.get(key) or "").strip()
        if value:
            return value
    return ""


def _tradfi_profile_metadata(
    store: CredentialStore,
    record: dict[str, Any],
    replicated_profiles: dict[str, dict[str, Any]] | None = None,
) -> TradFiProfileMetadata:
    """Convert one vault record to metadata and presence booleans only."""

    credentials = store.load_tradfi_credentials(str(record["id"]))
    replicated = (replicated_profiles or {}).get(str(record.get("provider") or "")) or {}
    replicated_active = str(replicated.get("profile_id") or "") == str(record["id"])
    return TradFiProfileMetadata(
        id=str(record["id"]),
        provider=str(record.get("provider") or ""),
        label=str(record.get("label") or ""),
        active=bool(record.get("active", True)) and not bool(record.get("pending")),
        shared=bool(record.get("shared", False)),
        generation=int(record.get("generation") or 0),
        configured=bool(credentials),
        has_api_key=bool(_tradfi_api_key(credentials)),
        has_api_secret=bool(_tradfi_api_secret(credentials)),
        origin=str(record.get("origin") or ""),
        pending=bool(record.get("pending")),
        pending_delete=bool(record.get("pending_delete")),
        pending_stage=str(record.get("pending_stage") or ""),
        pending_operation_id=str(record.get("pending_operation_id") or ""),
        last_operation_id=str(record.get("last_operation_id") or ""),
        replicated_active=replicated_active,
        activation_generation=int(replicated.get("activation_generation") or 0) if replicated_active else 0,
        created_at=str(record.get("created_at") or "") or None,
        updated_at=str(record.get("updated_at") or "") or None,
    )


def _tradfi_replicated_selection() -> dict[str, dict[str, Any]]:
    """Return the exact secret-free replicated active profile selection."""

    try:
        desired = rebuild_materialized_state(
            default_cluster_root(_Path(_PBGDIR)), write=False
        ).get("desired_state") or {}
    except Exception:
        return {}
    selections = desired.get("tradfi_active_profiles")
    if not isinstance(selections, dict):
        return {}
    return {
        str(provider): {
            "provider": str(provider),
            "profile_id": str(item.get("profile_id") or "") or None,
            "activation_generation": int(item.get("activation_generation") or 0),
            "conflicted": bool(item.get("conflicted")),
            "updated_at": item.get("updated_at"),
        }
        for provider, item in sorted(selections.items())
        if isinstance(item, dict)
    }


def _tradfi_projection_status(store: CredentialStore) -> dict[str, Any]:
    """Return the stable secret-free PB7 projection retry state."""

    _py, pb7_dir = _get_pb7_paths()
    if not pb7_dir:
        return {
            "status": "pending",
            "desired_generation": 0,
            "applied_generation": 0,
            "attempts": 0,
            "last_error": "PB7 directory is not configured",
        }
    status = PB7ApiKeysMergeWriter(
        _Path(pb7_dir) / "api-keys.json",
        store.root / "pb7_projection.json",
    ).projection_status()
    return {
        "status": str(status.get("status") or "pending"),
        "desired_generation": int(status.get("desired_generation") or 0),
        "applied_generation": int(status.get("applied_generation") or 0),
        "attempts": int(status.get("attempts") or 0),
        "last_attempt_at": status.get("last_attempt_at"),
        "applied_at": status.get("applied_at"),
        "last_error": str(status.get("last_error") or "") or None,
    }


@router.get("/tradfi/config")
def get_tradfi_config(
    session: SessionToken = Depends(require_auth),
) -> TradFiProfileMetadata:
    """Get the deterministic explicitly active TradFi profile without secrets."""

    store = _credential_store()
    publisher = _cluster_credential_publisher(store)
    reconcile_pending_credentials(
        _PBGDIR,
        store=store,
        tradfi_projector=lambda current, pending: _project_local_tradfi(current, pending),
        publisher=publisher,
    )
    records = sorted(store.list_tradfi(active_only=True), key=lambda item: str(item.get("id") or ""))
    return _tradfi_profile_metadata(store, records[0]) if records else TradFiProfileMetadata()


@router.put("/tradfi/config")
def update_tradfi_config(
    config: TradFiConfig,
    session: SessionToken = Depends(require_auth),
) -> dict:
    """Create or version one vault profile, publish it, and project active profiles."""

    provider = config.provider.strip().lower()
    if provider not in TRADFI_PROVIDERS:
        raise HTTPException(status_code=400, detail="Unknown TradFi provider")
    store = _credential_store()
    publisher = _cluster_credential_publisher(store)
    operation_id = config.operation_id or uuid.uuid4().hex
    try:
        with credential_mutation_lock(store.root):
            existing = store.get_tradfi(config.profile_id) if config.profile_id else None
            if existing is None and not config.create_new:
                active_matches = [
                    record
                    for record in store.list_tradfi(active_only=True)
                    if str(record.get("provider") or "").strip().lower() == provider
                ]
                matches = active_matches or [
                    record
                    for record in store.list_tradfi()
                    if str(record.get("provider") or "").strip().lower() == provider
                ]
                if matches:
                    existing = sorted(matches, key=lambda item: str(item.get("id") or ""))[0]

            credentials = (
                store.load_tradfi_credentials(str(existing["id"]))
                if existing is not None
                else {}
            )
            if config.api_key is not None:
                credentials["api_key"] = config.api_key.strip()
            if config.api_secret is not None:
                credentials["api_secret"] = config.api_secret.strip()
            if not _tradfi_api_key(credentials):
                raise HTTPException(status_code=400, detail="TradFi API key is required")
            if provider in TRADFI_NEEDS_SECRET and not _tradfi_api_secret(credentials):
                raise HTTPException(status_code=400, detail=f"{provider} API secret is required")

            if existing is None:
                record = store.create_tradfi(
                    provider,
                    credentials,
                    label=config.label,
                    active=config.active,
                    shared=config.shared,
                    pending=True,
                    operation_id=operation_id,
                )
            else:
                record = store.update_tradfi(
                    str(existing["id"]),
                    provider=provider,
                    credentials=credentials,
                    label=config.label,
                    active=config.active,
                    shared=config.shared,
                    pending=True,
                    operation_id=operation_id,
                )

            profile_id = str(record["id"])
            reconciliation = reconcile_pending_credentials(
                _PBGDIR,
                store=store,
                tradfi_projector=lambda current, pending: _project_local_tradfi(current, pending),
                publisher=publisher,
            )
            item = next(
                (
                    item for item in reconciliation.get("items") or []
                    if item.get("kind") == "tradfi"
                    and item.get("credential_id") == profile_id
                    and item.get("operation_id") == operation_id
                ),
                {"status": "active"},
            )
            record = store.get_tradfi(profile_id)
    except HTTPException as exc:
        if isinstance(exc.detail, dict) and exc.detail.get("operation_id"):
            raise
        raise HTTPException(
            status_code=exc.status_code,
            detail={"message": exc.detail, "operation_id": operation_id},
        ) from exc
    except (CredentialNotFoundError, ValueError) as exc:
        raise HTTPException(
            status_code=400,
            detail={"message": str(exc), "operation_id": operation_id},
        ) from exc
    except Exception as exc:
        _log(
            SERVICE,
            f"TradFi vault update failed for provider={provider}: {type(exc).__name__}",
            level="ERROR",
            meta={"traceback": traceback.format_exc()},
        )
        raise HTTPException(
            status_code=500,
            detail={
                "message": "TradFi profile could not be published or projected",
                "operation_id": operation_id,
            },
        ) from exc

    _log(SERVICE, f"Updated TradFi vault profile: id={record['id']} provider={provider}")
    return {
        "status": "saved" if item.get("status") == "active" else "pending",
        "operation_id": operation_id,
        "profile": _tradfi_profile_metadata(store, record).model_dump(),
        "reconciliation": item,
    }


@router.delete("/tradfi/config")
def clear_tradfi_config(
    profile_id: Optional[str] = Query(None, description="Vault profile ID"),
    session: SessionToken = Depends(require_auth),
) -> dict:
    """Tombstone one vault profile and reproject the remaining active profiles."""

    store = _credential_store()
    publisher = _cluster_credential_publisher(store)
    try:
        with credential_mutation_lock(store.root):
            if profile_id:
                record = store.get_tradfi(profile_id)
            else:
                records = store.list_tradfi(active_only=True)
                if not records:
                    raise HTTPException(status_code=404, detail="No active TradFi profile found")
                record = sorted(records, key=lambda item: str(item.get("id") or ""))[0]
            selected_id = str(record["id"])
            operation_id = uuid.uuid4().hex
            store.begin_tradfi_delete(selected_id, operation_id)
            reconciliation = reconcile_pending_credentials(
                _PBGDIR,
                store=store,
                tradfi_projector=lambda current, pending: _project_local_tradfi(current, pending),
                publisher=publisher,
            )
            item = next(
                item for item in reconciliation.get("items") or []
                if item.get("credential_id") == selected_id
                and item.get("operation_id") == operation_id
            )
    except HTTPException:
        raise
    except (CredentialNotFoundError, ValueError) as exc:
        raise HTTPException(status_code=404, detail="TradFi profile not found") from exc
    except Exception as exc:
        _log(
            SERVICE,
            f"TradFi vault delete failed: {type(exc).__name__}",
            level="ERROR",
            meta={"traceback": traceback.format_exc()},
        )
        raise HTTPException(status_code=500, detail="TradFi profile could not be tombstoned or projected") from exc

    _log(SERVICE, f"Deleted TradFi vault profile: id={selected_id}")
    return {
        "status": "cleared" if item.get("status") == "deleted" else "pending_delete",
        "operation_id": operation_id,
        "deleted": selected_id,
        "reconciliation": item,
        "tombstone": {"status": item.get("tombstone_status")},
        "projection": {"status": item.get("projection_status")},
    }


# ── TradFi helpers ────────────────────────────────────────────

TRADFI_PROVIDERS = ["alpaca", "polygon", "finnhub", "alphavantage", "tiingo"]
TRADFI_PROVIDER_NOTES = {
    "alpaca": "Free (IEX feed, 15-min delay — fine for backtesting), 5+ years of 1-minute data. Requires API key + secret. Recommended.",
    "polygon": "Free tier: 2 years of 1-minute data. Paid plans offer extended history.",
    "finnhub": "Free tier does NOT support 1-minute intraday — unusable for backtesting.",
    "alphavantage": "Free tier: 25 calls/day, very limited for backtesting.",
    "tiingo": "IEX and FX data used by PBGui Market Data for local stock-perp archives.",
}
TRADFI_NEEDS_SECRET = {"alpaca"}
TRADFI_PROVIDER_LINKS = {
    "alpaca": ("https://app.alpaca.markets/paper-trading/overview", "Get free Alpaca API key"),
    "polygon": ("https://polygon.io/dashboard/signup", "Sign up for Polygon.io"),
    "finnhub": ("https://finnhub.io/register", "Sign up for Finnhub (free)"),
    "alphavantage": ("https://www.alphavantage.co/support/#api-key", "Get free Alpha Vantage API key"),
    "tiingo": ("https://www.tiingo.com/account/api/token", "Get a Tiingo API token"),
}


def _get_pb7_paths() -> tuple[Optional[str], Optional[str]]:
    """Return (pb7venv, pb7dir) from pbgui.ini."""
    snapshot = pbgui_purefunc.load_ini_snapshot()
    pb7venv = snapshot.get("main", "pb7venv") if snapshot.has_option("main", "pb7venv") else None
    pb7dir = snapshot.get("main", "pb7dir") if snapshot.has_option("main", "pb7dir") else None
    return pb7venv, pb7dir


def _run_tradfi_test(py: str, pb7_dir: str, provider: str,
                     api_key: str = "", api_secret: str = "") -> tuple[bool, str]:
    """Run a PB7 provider test with secrets supplied only through the environment."""

    # Finnhub free tier does not support resolution=1 for US stocks;
    # use the /quote endpoint instead which is available on all plans.
    if provider == "finnhub":
        script = """\
import os, asyncio, aiohttp
async def _test():
    url = "https://finnhub.io/api/v1/quote"
    params = {"symbol": "AAPL", "token": os.environ["PBGUI_TRADFI_API_KEY"]}
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as resp:
            if resp.status == 403:
                print("FAIL:403 Forbidden - check your Finnhub API key")
                return
            resp.raise_for_status()
            data = await resp.json()
    if data.get("c") is not None and data["c"] != 0:
        print(f'OK:AAPL price={data["c"]}')
    else:
        print(f'FAIL:unexpected response: {data}')
asyncio.run(_test())
"""
    # Alpha Vantage TIME_SERIES_INTRADAY is premium; GLOBAL_QUOTE is free.
    elif provider == "alphavantage":
        script = """\
import os, asyncio, aiohttp
async def _test():
    url = "https://www.alphavantage.co/query"
    params = {"function": "GLOBAL_QUOTE", "symbol": "AAPL", "apikey": os.environ["PBGUI_TRADFI_API_KEY"]}
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as resp:
            resp.raise_for_status()
            data = await resp.json()
    if "Note" in data or "Information" in data:
        msg = data.get("Note") or data.get("Information") or "rate limit"
        print(f"FAIL:{msg}")
        return
    quote = data.get("Global Quote", {})
    price = quote.get("05. price")
    if price:
        print(f"OK:AAPL price={price}")
    else:
        print(f"FAIL:unexpected response: {data}")
asyncio.run(_test())
"""
    else:
        script = """\
import os, sys, asyncio
sys.path.insert(0, os.path.join(os.environ["PBGUI_PB7_DIR"], "src"))
from tradfi_data import get_provider
from datetime import datetime, timedelta, timezone

async def _test():
    kwargs = {}
    if os.environ.get("PBGUI_TRADFI_API_KEY"):
        kwargs["api_key"] = os.environ["PBGUI_TRADFI_API_KEY"]
    if os.environ.get("PBGUI_TRADFI_API_SECRET"):
        kwargs["api_secret"] = os.environ["PBGUI_TRADFI_API_SECRET"]
    p = get_provider(os.environ["PBGUI_TRADFI_PROVIDER"], **kwargs)
    async with p:
        end = datetime.now(timezone.utc) - timedelta(days=1)
        start = end - timedelta(days=7)
        c = await p.fetch_1m_candles(
            'AAPL',
            int(start.timestamp() * 1000),
            int(end.timestamp() * 1000),
        )
        print(f'OK:{len(c)}')

asyncio.run(_test())
"""
    env = dict(os.environ)
    env.update({
        "PBGUI_TRADFI_PROVIDER": provider,
        "PBGUI_TRADFI_API_KEY": api_key,
        "PBGUI_TRADFI_API_SECRET": api_secret,
        "PBGUI_PB7_DIR": str(pb7_dir),
    })
    try:
        result = subprocess.run(
            [py, "-c", script],
            capture_output=True,
            text=True,
            timeout=30,
            env=env,
        )
        out = _redact_tradfi_diagnostic((result.stdout or "").strip(), api_key, api_secret)
        err = _redact_tradfi_diagnostic((result.stderr or "").strip(), api_key, api_secret)
        if result.returncode == 0 and out.startswith("OK:"):
            payload = out[3:]
            if payload.isdigit():
                n = int(payload)
                if n > 0:
                    return True, f"Connection OK - {n} candles received (AAPL, last 7 days)."
                err_hint = err.splitlines()[-1] if err else "unknown reason"
                return False, f"0 candles - {err_hint}"
            return True, f"Connection OK - {payload}"
        if result.returncode == 0 and out.startswith("FAIL:"):
            return False, out[5:]
        if result.returncode == 0 and "OK:" in out:
            m = re.search(r"OK:(\d+)", out)
            if m:
                n = int(m.group(1))
                if n > 0:
                    return True, f"Connection OK - {n} candles received (AAPL, last 7 days)."
            err_hint = err.splitlines()[-1] if err else "unknown reason"
            return False, f"0 candles - {err_hint}"
        msg = err.splitlines()[-1] if err else out or "Unknown error"
        return False, f"Test failed: {msg}"
    except subprocess.TimeoutExpired:
        return False, "Test timed out (30s)."
    except Exception as exc:
        return False, f"Test error: {type(exc).__name__}"


def _redact_tradfi_diagnostic(value: str, *secrets: str) -> str:
    """Remove credentials and provider URLs from subprocess diagnostics."""

    result = str(value or "")
    for secret in secrets:
        if secret:
            result = result.replace(secret, "<redacted>")
    return re.sub(r"https?://\S+", "<provider-url>", result)


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
# (tradfi/test route registered earlier, before /{name}/test — see above)


@router.get("/tradfi/profiles")
def tradfi_get_profiles(
    session: SessionToken = Depends(require_auth),
) -> dict:
    """Get vault profile metadata and credential-presence booleans only."""

    store = _credential_store()
    replicated = _tradfi_replicated_selection()
    profiles = [
        _tradfi_profile_metadata(store, record, replicated).model_dump()
        for record in store.list_tradfi()
    ]
    return {
        "providers": TRADFI_PROVIDERS,
        "provider_notes": TRADFI_PROVIDER_NOTES,
        "provider_links": {p: {"url": url, "label": lbl} for p, (url, lbl) in TRADFI_PROVIDER_LINKS.items()},
        "needs_secret": list(TRADFI_NEEDS_SECRET),
        "profiles": profiles,
        "replicated_active_profiles": replicated,
        "projection": _tradfi_projection_status(store),
    }


@router.post("/tradfi/projection/retry")
def retry_tradfi_projection(
    body: TradFiProjectionRetry,
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """Retry pending publication/projection and return only durable status metadata."""

    operation_id = body.operation_id or uuid.uuid4().hex
    store = _credential_store()
    try:
        with credential_mutation_lock(store.root):
            reconciliation = reconcile_pending_credentials(
                _PBGDIR,
                store=store,
                tradfi_projector=lambda current, pending: _project_local_tradfi(current, pending),
                publisher=_cluster_credential_publisher(store),
            )
            if not any(item.get("kind") == "tradfi" for item in reconciliation.get("items") or []):
                _project_local_tradfi(store)
            projection = _tradfi_projection_status(store)
        return {
            "ok": projection.get("status") == "current",
            "operation_id": operation_id,
            "projection": projection,
            "reconciliation": reconciliation,
        }
    except HTTPException:
        raise
    except Exception as exc:
        _log(
            SERVICE,
            f"TradFi projection retry failed: {type(exc).__name__}",
            level="ERROR",
            meta={"operation_id": operation_id, "traceback": traceback.format_exc()},
        )
        raise HTTPException(
            status_code=409,
            detail={
                "message": "TradFi PB7 projection retry failed",
                "operation_id": operation_id,
                "projection": _tradfi_projection_status(store),
            },
        ) from exc
