"""Authentication, welcome page, and setup helpers for FastAPI endpoints."""

import asyncio
from collections import deque
from dataclasses import dataclass, field
import hmac
import json
import math
import re
import threading
import time
import toml
from pathlib import Path
from typing import Optional
from uuid import uuid4

from fastapi import APIRouter, Cookie, Depends, Header, HTTPException, Query, Request, WebSocket
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from pydantic import BaseModel
from starlette.websockets import WebSocketState

from logging_helpers import human_log as _log
from pbgui_purefunc import (
    PBGDIR,
    PBGUI_SERIAL,
    PBGUI_VERSION,
    legacy_auth_secrets_path,
    load_ini,
    pb7_runtime_status,
    pbgui_auth_secrets_path,
    save_ini,
)
from secure_files import atomic_write_private_text, ensure_private_directory

router = APIRouter()
SERVICE = "Auth"
_DEFAULT_PASSWORD = "PBGui$Bot!"
SESSION_COOKIE_NAME = "pbgui_session"
_websocket_sessions: dict[str, set[WebSocket]] = {}
_websocket_watchdogs: dict[WebSocket, asyncio.Task] = {}
_LOGIN_FAILURE_WINDOW_SECONDS = 5 * 60
_LOGIN_FAILURE_LIMIT = 5
_LOGIN_INITIAL_LOCK_SECONDS = 30
_LOGIN_MAX_LOCK_SECONDS = 15 * 60
_LOGIN_STATE_TTL_SECONDS = 60 * 60
_LOGIN_STATE_MAX_ENTRIES = 4096
_PASSWORDLESS_SESSION_LIMIT = 4096
_LOGIN_BLOCK_LOG_RE = re.compile(
    r"^(?P<timestamp>\S+) \[Auth\] \[WARNING\] Login temporarily blocked for client "
    r"(?P<client>.+?) after repeated failures; retry in (?P<retry>\d+)s"
    r"(?:; event (?P<event_id>[a-f0-9]+))?$"
)


@dataclass
class _LoginAttemptState:
    """Process-local failed-login history for one direct client address."""

    failures: deque[float] = field(default_factory=deque)
    lock_until: float = 0.0
    lock_level: int = 0
    last_seen: float = 0.0


_login_attempts: dict[str, _LoginAttemptState] = {}
_login_attempts_lock = threading.Lock()
_login_block_count = 0
_login_last_block: dict[str, object] | None = None
_login_security_history_loaded = False
_passwordless_sessions_lock = threading.Lock()


def _login_now() -> float:
    """Return the wall-clock timestamp used by login-throttle calculations."""
    return time.time()


def _prune_login_attempts(now: float) -> None:
    """Remove inactive throttle entries while the registry lock is held."""
    expired = [
        host for host, state in _login_attempts.items()
        if now - state.last_seen >= _LOGIN_STATE_TTL_SECONDS
    ]
    for host in expired:
        _login_attempts.pop(host, None)


def _ensure_login_security_history() -> None:
    """Load retained login-lock history once while the throttle lock is held."""
    global _login_block_count, _login_last_block, _login_security_history_loaded
    if _login_security_history_loaded:
        return
    count = 0
    last_block = None
    log_dir = Path(PBGDIR) / "data" / "logs"
    for path in (log_dir / "Auth.log.1", log_dir / "Auth.log"):
        try:
            with path.open("r", encoding="utf-8", errors="replace") as handle:
                for line in handle:
                    match = _LOGIN_BLOCK_LOG_RE.match(line.strip())
                    if not match:
                        continue
                    count += 1
                    last_block = {
                        "blocked_at": match.group("timestamp"),
                        "client": match.group("client"),
                        "retry_seconds": int(match.group("retry")),
                    }
                    if match.group("event_id"):
                        last_block["event_id"] = match.group("event_id")
        except OSError:
            continue
    _login_block_count = count
    _login_last_block = last_block
    _login_security_history_loaded = True


def _login_security_ack_path() -> Path:
    """Return the private persisted login-security acknowledgement path."""
    return Path(PBGDIR) / "data" / "auth" / "login_security_ack.json"


def _load_login_security_ack() -> dict[str, object] | None:
    """Load the last acknowledged block event, logging malformed state."""
    path = _login_security_ack_path()
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        event = data.get("last_block") if isinstance(data, dict) else None
        return dict(event) if isinstance(event, dict) else None
    except (OSError, json.JSONDecodeError) as exc:
        _log(SERVICE, f"Unable to read login security acknowledgement: {exc}", level="WARNING")
        return None


def _login_security_event_key(event: dict[str, object] | None) -> tuple[object, ...] | None:
    """Return a restart-stable identity for a retained login block event."""
    if not event:
        return None
    if event.get("event_id"):
        return ("event_id", event["event_id"])
    return (
        "legacy",
        event.get("blocked_at"),
        event.get("client"),
        event.get("retry_seconds"),
    )


def _login_security_status(now: float | None = None) -> dict[str, object]:
    """Return authenticated login-lock status from memory and retained Auth logs."""
    current_time = _login_now() if now is None else now
    with _login_attempts_lock:
        _ensure_login_security_history()
        _prune_login_attempts(current_time)
        active_blocks = sum(1 for state in _login_attempts.values() if state.lock_until > current_time)
        last_block = dict(_login_last_block) if _login_last_block else None
        return {
            "active_blocks": active_blocks,
            "blocked_attempts": _login_block_count,
            "last_block": last_block,
            "acknowledged": last_block is None or (
                _login_security_event_key(_load_login_security_ack()) == _login_security_event_key(last_block)
            ),
        }


def _login_retry_after(client_host: str, now: float) -> int:
    """Return remaining lock seconds for a client, or zero when login may proceed."""
    with _login_attempts_lock:
        _prune_login_attempts(now)
        state = _login_attempts.get(client_host)
        if state is None:
            return 0
        state.last_seen = now
        if state.lock_until > now:
            return max(1, math.ceil(state.lock_until - now))
        state.lock_until = 0.0
        return 0


def _record_login_failure(client_host: str, now: float) -> int:
    """Record a failed login and return lock seconds when the threshold is reached."""
    global _login_block_count, _login_last_block
    with _login_attempts_lock:
        _ensure_login_security_history()
        _prune_login_attempts(now)
        if client_host not in _login_attempts and len(_login_attempts) >= _LOGIN_STATE_MAX_ENTRIES:
            oldest_host = min(_login_attempts, key=lambda host: _login_attempts[host].last_seen)
            _login_attempts.pop(oldest_host, None)
        state = _login_attempts.setdefault(client_host, _LoginAttemptState())
        state.last_seen = now
        if state.lock_until > now:
            return max(1, math.ceil(state.lock_until - now))
        cutoff = now - _LOGIN_FAILURE_WINDOW_SECONDS
        while state.failures and state.failures[0] <= cutoff:
            state.failures.popleft()
        state.failures.append(now)
        if len(state.failures) < _LOGIN_FAILURE_LIMIT:
            return 0
        lock_seconds = min(
            _LOGIN_INITIAL_LOCK_SECONDS * (2 ** min(state.lock_level, 5)),
            _LOGIN_MAX_LOCK_SECONDS,
        )
        state.lock_level += 1
        state.lock_until = now + lock_seconds
        _login_block_count += 1
        event_id = uuid4().hex
        _login_last_block = {
            "blocked_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now)),
            "client": client_host,
            "retry_seconds": lock_seconds,
            "event_id": event_id,
        }
        _log(
            SERVICE,
            f"Login temporarily blocked for client {client_host} after repeated failures; "
            f"retry in {lock_seconds}s; event {event_id}",
            level="WARNING",
        )
        return lock_seconds


def _reset_login_attempts(client_host: str) -> None:
    """Forget all failures and lock escalation for one client."""
    with _login_attempts_lock:
        _login_attempts.pop(client_host, None)


def _raise_login_throttled(retry_after: int) -> None:
    """Raise the standard HTTP response for a temporarily blocked login."""
    raise HTTPException(
        status_code=429,
        detail=f"Too many login attempts. Try again in {retry_after} seconds.",
        headers={"Retry-After": str(retry_after)},
    )


def _clear_vps_manager_secrets(token: str) -> None:
    try:
        from api.vps_manager import _get_service as _get_vps_manager_service

        _get_vps_manager_service().clear_session_secrets(token)
    except Exception:
        pass


def _prune_vps_manager_secrets(valid_tokens: set[str]) -> None:
    try:
        from api.vps_manager import _get_service as _get_vps_manager_service

        _get_vps_manager_service().prune_session_secrets(valid_tokens)
    except Exception:
        pass


class SessionToken(BaseModel):
    """Session token data structure."""
    token: str
    user_id: str
    created_at: float
    expires_at: float


class LoginRequest(BaseModel):
    password: str = ""


class PasswordChangeRequest(BaseModel):
    current_password: str = ""
    new_password: str = ""
    disable_auth: bool = False


class SetupConfigRequest(BaseModel):
    pb7dir: str = ""
    pb7venv: str = ""
    pbname: str = ""
    role: str = "slave"


def _resolve_browse_path(raw_path: str) -> tuple[Path, str]:
    candidate = Path(str(raw_path or "").strip() or str(Path.home())).expanduser().resolve(strict=False)
    selected_path = ""

    if candidate.exists():
        if candidate.is_dir():
            return candidate, str(candidate)
        return candidate.parent, str(candidate)

    current_dir = candidate.parent if candidate.parent != candidate else Path.home().resolve()
    while not current_dir.exists() and current_dir.parent != current_dir:
        current_dir = current_dir.parent

    if not current_dir.exists():
        current_dir = Path.home().resolve()

    return current_dir, selected_path


def get_tokens_dir() -> Path:
    """Return directory where API tokens are stored."""
    return ensure_private_directory(Path(PBGDIR) / "data" / "api_tokens")


def _write_auth_secrets_toml(secrets: dict) -> None:
    secrets_path = pbgui_auth_secrets_path()
    ensure_private_directory(secrets_path.parent)
    atomic_write_private_text(secrets_path, toml.dumps(secrets))


def _ensure_auth_secrets_file() -> Path:
    secrets_path = pbgui_auth_secrets_path()
    ensure_private_directory(secrets_path.parent)
    if not secrets_path.exists():
        legacy_path = legacy_auth_secrets_path()
        if legacy_path.exists():
            atomic_write_private_text(secrets_path, legacy_path.read_text(encoding="utf-8"))
        else:
            _write_auth_secrets_toml({"auth_mode": "password", "password": _DEFAULT_PASSWORD})
    return secrets_path


def _load_auth_secrets() -> tuple[dict, str | None]:
    secrets_path = _ensure_auth_secrets_file()
    try:
        return toml.load(secrets_path), None
    except toml.TomlDecodeError as exc:
        return {}, f"Invalid secrets.toml: {exc}"


def _password_state() -> dict:
    secrets, error = _load_auth_secrets()
    password_value = str(secrets.get("password", "")) if not error else ""
    configured_mode = str(secrets.get("auth_mode", "")).strip().lower() if not error else ""
    if not configured_mode:
        configured_mode = "password" if password_value else "disabled"
    if configured_mode not in {"password", "disabled"}:
        error = f"Invalid authentication mode: {configured_mode}"
    if configured_mode == "password" and not password_value and not error:
        error = "Password authentication is enabled but no password is configured"
    auth_mode = configured_mode if not error else "error"
    password_required = auth_mode == "password"
    password_missing = auth_mode == "disabled"
    try:
        bind_host = str(load_ini("api_server", "host") or "0.0.0.0").strip()
    except Exception:
        bind_host = "0.0.0.0"
    uses_legacy_default = password_value == _DEFAULT_PASSWORD and not error
    wildcard_bind = bind_host in {"", "0.0.0.0", "::", "[::]"}
    security_warnings = []
    if auth_mode == "disabled":
        if wildcard_bind:
            security_warnings.append(
                "Authentication is disabled while PBGui listens on all network interfaces. "
                "Anyone who can reach the API port has full PBGui access; verify VPN and firewall restrictions."
            )
        else:
            security_warnings.append(
                f"Authentication is disabled. Anyone who can reach PBGui on {bind_host} has full access."
            )
    elif uses_legacy_default and wildcard_bind:
        security_warnings.append(
            "PBGui listens on all network interfaces and still uses the known legacy default password. "
            "Verify that the API port is restricted to VPN or trusted networks, or set an individual password."
        )
    return {
        "error": error,
        "mode": auth_mode,
        "required": password_required,
        "missing": password_missing,
        "password": password_value,
        "bind_host": bind_host,
        "wildcard_bind": wildcard_bind,
        "security_warnings": security_warnings,
    }


def auth_runtime_status() -> dict[str, object]:
    """Return non-secret authentication mode details for global UI status."""
    state = _password_state()
    return {
        "mode": state["mode"],
        "disabled": state["mode"] == "disabled",
        "bind_host": state["bind_host"],
        "wildcard_bind": state["wildcard_bind"],
        "error": state["error"],
    }


def _frontend_template_path(name: str) -> Path:
    return Path(__file__).parent.parent / "frontend" / name


def _request_origin(request: Request) -> str:
    scheme = request.url.scheme
    host = request.url.hostname or "127.0.0.1"
    port = request.url.port
    return f"{scheme}://{host}" + (f":{port}" if port else "")


def _main_page_url() -> str:
    return "/api/auth/main_page"


def _root_url() -> str:
    return "/"


def build_root_entry_response(
    request: Request,
    session: SessionToken | None = None,
) -> HTMLResponse | RedirectResponse:
    """Return the public root response.

    If a password is configured and no valid token is present, show the small
    root login page first. Otherwise jump straight to the Welcome page.
    """
    auth_state = _password_state()

    if auth_state["error"] or session is not None or not auth_state["required"]:
        return RedirectResponse(url=_main_page_url())

    html = _frontend_template_path("root_login.html").read_text(encoding="utf-8")
    html = html.replace('"%%API_ORIGIN%%"', json.dumps(_request_origin(request)))
    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})


def _bootstrap_payload(session: SessionToken | None = None) -> dict:
    auth_state = _password_state()
    return {
        "version": PBGUI_VERSION,
        "serial": PBGUI_SERIAL,
        "auth": {
            "authenticated": session is not None,
            "auth_mode": auth_state["mode"],
            "password_required": auth_state["required"],
            "password_missing": auth_state["missing"],
            "error": auth_state["error"],
            "security_warnings": auth_state["security_warnings"] if session is not None else [],
            "login_security": _login_security_status() if session is not None else {},
            "token": session.token if session else "",
            "user_id": session.user_id if session else "",
            "expires_at": session.expires_at if session else 0,
        },
        "setup": pb7_runtime_status(),
    }


def generate_token(user_id: str, expires_in_seconds: int = 86400) -> SessionToken:
    """Generate a new API token for a user.
    
    Args:
        user_id: User identifier
        expires_in_seconds: Token lifetime (default: 24 hours)
        
    Returns:
        SessionToken with token string and metadata
    """
    token = str(uuid4())
    now = time.time()
    
    session = SessionToken(
        token=token,
        user_id=user_id,
        created_at=now,
        expires_at=now + expires_in_seconds
    )
    
    # Save token to file
    token_file = get_tokens_dir() / f"{token}.json"
    atomic_write_private_text(
        token_file,
        json.dumps(session.model_dump(), indent=2),
    )
    
    return session


def _get_or_create_passwordless_session(client_host: str) -> SessionToken:
    """Reuse one bounded passwordless session per direct client address."""
    user_id = f"passwordless:{client_host}"
    with _passwordless_sessions_lock:
        now = time.time()
        sessions: list[SessionToken] = []
        for token_file in get_tokens_dir().glob("*.json"):
            try:
                session = SessionToken(**json.loads(token_file.read_text(encoding="utf-8")))
            except (OSError, json.JSONDecodeError, ValueError):
                continue
            if not session.user_id.startswith("passwordless:"):
                continue
            if session.expires_at <= now:
                revoke_token(session.token)
                continue
            sessions.append(session)

        matching = [session for session in sessions if session.user_id == user_id]
        if matching:
            keep = max(matching, key=lambda session: session.expires_at)
            for duplicate in matching:
                if duplicate.token != keep.token:
                    revoke_token(duplicate.token)
            return keep

        if len(sessions) >= _PASSWORDLESS_SESSION_LIMIT:
            oldest = min(sessions, key=lambda session: session.created_at)
            revoke_token(oldest.token)

        session = generate_token(user_id, expires_in_seconds=86400)
        _log(SERVICE, f"Passwordless session issued for client {client_host}", level="INFO")
        return session


async def _revoke_all_sessions() -> int:
    """Revoke all persisted sessions and close their active WebSockets."""
    tokens = [path.stem for path in get_tokens_dir().glob("*.json")]
    for token in tokens:
        revoke_token(token)
        await close_websocket_sessions(token)
    return len(tokens)


def validate_token(token: str) -> Optional[SessionToken]:
    """Validate an API token and return session if valid.
    
    Args:
        token: Token string to validate
        
    Returns:
        SessionToken if valid and not expired, None otherwise
    """
    if not token or not token.strip():
        return None
    
    token_file = get_tokens_dir() / f"{token.strip()}.json"
    
    if not token_file.exists():
        return None
    
    try:
        data = json.loads(token_file.read_text(encoding="utf-8"))
        session = SessionToken(**data)
        
        # Check expiration
        if session.expires_at < time.time():
            # Delete expired token
            token_file.unlink(missing_ok=True)
            _clear_vps_manager_secrets(token.strip())
            return None
        
        return session
        
    except Exception:
        return None


def revoke_token(token: str) -> bool:
    """Revoke (delete) a token.
    
    Args:
        token: Token to revoke
        
    Returns:
        True if token was found and deleted, False otherwise
    """
    token_file = get_tokens_dir() / f"{token.strip()}.json"
    _clear_vps_manager_secrets(token.strip())
    if token_file.exists():
        token_file.unlink()
        return True
    return False


def set_session_cookie(response: Response, request: Request, session: SessionToken) -> None:
    """Set the browser session cookie without exposing the token to JavaScript URLs."""
    max_age = max(0, int(session.expires_at - time.time()))
    response.set_cookie(
        SESSION_COOKIE_NAME,
        session.token,
        max_age=max_age,
        httponly=True,
        secure=request.url.scheme == "https",
        samesite="strict",
        path="/",
    )


def unauthenticated_page_redirect(request: Request, status_code: int) -> Optional[RedirectResponse]:
    """Redirect unauthenticated browser page loads to login without altering API 401s."""
    accepts_html = "text/html" in request.headers.get("accept", "").lower()
    if request.method == "GET" and status_code == 401 and accepts_html:
        return RedirectResponse(url="/", status_code=303, headers={"Cache-Control": "no-store"})
    return None


def _unregister_websocket(websocket: WebSocket, token: str) -> None:
    """Remove one WebSocket from the token-scoped active-session registry."""
    sockets = _websocket_sessions.get(token)
    if sockets is not None:
        sockets.discard(websocket)
        if not sockets:
            _websocket_sessions.pop(token, None)
    _websocket_watchdogs.pop(websocket, None)


async def _websocket_auth_watchdog(websocket: WebSocket, token: str) -> None:
    """Close an accepted WebSocket when its backing session is revoked or expires."""
    try:
        while websocket.client_state == WebSocketState.CONNECTED:
            await asyncio.sleep(1)
            if validate_token(token) is None:
                await websocket.close(code=4001, reason="Session expired or revoked")
                break
    except (asyncio.CancelledError, RuntimeError):
        pass
    finally:
        _unregister_websocket(websocket, token)


async def authenticate_websocket(websocket: WebSocket) -> Optional[SessionToken]:
    """Authenticate and track a browser WebSocket through its HttpOnly cookie."""
    token = str(websocket.cookies.get(SESSION_COOKIE_NAME) or "").strip()
    session = validate_token(token)
    if session is None:
        await websocket.close(code=4001)
        return None
    await websocket.accept()
    _websocket_sessions.setdefault(token, set()).add(websocket)
    _websocket_watchdogs[websocket] = asyncio.create_task(
        _websocket_auth_watchdog(websocket, token),
        name="websocket-auth-watchdog",
    )
    return session


async def close_websocket_sessions(token: str) -> None:
    """Immediately close every active WebSocket authenticated by *token*."""
    sockets = list(_websocket_sessions.pop(str(token or "").strip(), set()))
    tasks: list[asyncio.Task] = []
    for websocket in sockets:
        try:
            await websocket.close(code=4001, reason="Session logged out")
        except RuntimeError:
            pass
        task = _websocket_watchdogs.pop(websocket, None)
        if task is not None:
            task.cancel()
            tasks.append(task)
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


async def shutdown() -> None:
    """Close authenticated WebSockets and await every session watchdog."""
    sockets = {websocket for group in _websocket_sessions.values() for websocket in group}
    sockets.update(_websocket_watchdogs)
    _websocket_sessions.clear()
    tasks = list(_websocket_watchdogs.values())
    _websocket_watchdogs.clear()
    for websocket in sockets:
        try:
            await websocket.close(code=1001, reason="API shutting down")
        except RuntimeError:
            pass
    for task in tasks:
        if not task.done():
            task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


def cleanup_expired_tokens() -> int:
    """Delete all expired tokens.
    
    Returns:
        Number of tokens deleted
    """
    deleted = 0
    now = time.time()
    valid_tokens: set[str] = set()

    for token_file in get_tokens_dir().glob("*.json"):
        try:
            data = json.loads(token_file.read_text(encoding="utf-8"))
            if data.get("expires_at", 0) < now:
                token_file.unlink()
                _clear_vps_manager_secrets(token_file.stem)
                deleted += 1
            else:
                valid_tokens.add(token_file.stem)
        except Exception:
            # Delete corrupt token files
            token_file.unlink()
            _clear_vps_manager_secrets(token_file.stem)
            deleted += 1

    _prune_vps_manager_secrets(valid_tokens)

    return deleted


def refresh_token(token: str, extends_seconds: int = 86400) -> Optional[SessionToken]:
    """Extend an existing token's expiry by the given duration.

    Returns the updated SessionToken, or None if the token is invalid/expired.
    """
    if not token or not token.strip():
        return None

    token_file = get_tokens_dir() / f"{token.strip()}.json"
    if not token_file.exists():
        return None

    try:
        data = json.loads(token_file.read_text(encoding="utf-8"))
        session = SessionToken(**data)

        if session.expires_at < time.time():
            token_file.unlink(missing_ok=True)
            return None

        session.expires_at = time.time() + extends_seconds
        atomic_write_private_text(token_file, json.dumps(session.model_dump(), indent=4))
        return session
    except Exception:
        return None


# ── FastAPI Dependencies ──

def get_token_from_request(
    authorization: Optional[str] = Header(None, description="Bearer token"),
    session_cookie: Optional[str] = Cookie(None, alias=SESSION_COOKIE_NAME),
) -> str:
    """Extract a token from a Bearer header or the HttpOnly browser cookie."""
    if authorization and authorization.startswith("Bearer "):
        return authorization.replace("Bearer ", "").strip()
    if session_cookie:
        return session_cookie.strip()
    raise HTTPException(
        status_code=401,
        detail="Missing authentication token. Provide an Authorization Bearer header or session cookie."
    )


def require_auth(token_str: str = Depends(get_token_from_request)) -> SessionToken:
    """FastAPI dependency that requires valid authentication.
    
    Usage:
        @router.get("/protected")
        def protected_endpoint(session: SessionToken = Depends(require_auth)):
            return {"user": session.user_id}
    
    Raises:
        HTTPException(401) if token is missing or invalid
    """
    session = validate_token(token_str)
    
    if not session:
        raise HTTPException(
            status_code=401,
            detail="Invalid or expired token"
        )
    
    return session


def optional_auth(
    authorization: Optional[str] = Header(None),
    session_cookie: Optional[str] = Cookie(None, alias=SESSION_COOKIE_NAME),
) -> Optional[SessionToken]:
    """FastAPI dependency for optional authentication.
    
    Returns SessionToken if valid token provided, None otherwise.
    Does not raise exception for missing/invalid tokens.
    """
    # Try to extract token
    token_str = None
    if authorization and authorization.startswith("Bearer "):
        token_str = authorization.replace("Bearer ", "").strip()
    elif session_cookie:
        token_str = session_cookie.strip()
    if not token_str:
        return None
    
    return validate_token(token_str)


@router.get("/bootstrap")
def bootstrap(session: Optional[SessionToken] = Depends(optional_auth)) -> dict:
    """Return welcome-page bootstrap data for auth and PB7 runtime status."""
    return _bootstrap_payload(session)


@router.post("/login-security/ack")
def acknowledge_login_security(session: SessionToken = Depends(require_auth)) -> dict:
    """Acknowledge the latest retained login-lock event for all browsers."""
    del session
    with _login_attempts_lock:
        _ensure_login_security_history()
        last_block = dict(_login_last_block) if _login_last_block else None
    if last_block is not None:
        path = _login_security_ack_path()
        try:
            ensure_private_directory(path.parent)
            atomic_write_private_text(
                path,
                json.dumps(
                    {
                        "last_block": last_block,
                        "acknowledged_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    },
                    indent=4,
                ),
            )
        except OSError as exc:
            _log(SERVICE, f"Unable to save login security acknowledgement: {exc}", level="ERROR")
            raise HTTPException(status_code=500, detail="Unable to save login security acknowledgement") from exc
    return {"ok": True, "login_security": _login_security_status()}


@router.post("/login")
def login(payload: LoginRequest, request: Request, response: Response) -> dict:
    """Authenticate the welcome page and return a fresh API token."""
    auth_state = _password_state()
    if auth_state["error"]:
        raise HTTPException(status_code=500, detail=auth_state["error"])

    client_host = request.client.host if request.client else "local"
    now = _login_now()
    if auth_state["required"]:
        retry_after = _login_retry_after(client_host, now)
        if retry_after:
            _raise_login_throttled(retry_after)
        password_matches = hmac.compare_digest(
            payload.password.encode("utf-8"),
            auth_state["password"].encode("utf-8"),
        )
        if not password_matches:
            retry_after = _record_login_failure(client_host, now)
            if retry_after:
                _raise_login_throttled(retry_after)
            raise HTTPException(status_code=401, detail="Password incorrect")

    _reset_login_attempts(client_host)
    session = generate_token(f"welcome:{client_host}", expires_in_seconds=86400)
    set_session_cookie(response, request, session)
    result = _bootstrap_payload(session)
    result["message"] = "Authenticated"
    return result


@router.post("/logout")
async def logout(response: Response, session: SessionToken = Depends(require_auth)) -> dict:
    """Revoke the current API token."""
    revoke_token(session.token)
    response.delete_cookie(SESSION_COOKIE_NAME, path="/", httponly=True, samesite="strict")
    await close_websocket_sessions(session.token)
    return {"ok": True}


@router.post("/change-password")
async def change_password(
    payload: PasswordChangeRequest,
    request: Request,
    response: Response,
    session: SessionToken = Depends(require_auth),
) -> dict:
    """Change password authentication mode and rotate every active session."""
    del session
    auth_state = _password_state()
    if auth_state["error"]:
        raise HTTPException(status_code=500, detail=auth_state["error"])

    if payload.disable_auth and payload.new_password:
        raise HTTPException(status_code=400, detail="New password must be empty when disabling authentication")
    if not payload.disable_auth and not payload.new_password:
        raise HTTPException(status_code=400, detail="Enter a new password or explicitly disable authentication")
    if auth_state["required"] and not hmac.compare_digest(
        payload.current_password.encode("utf-8"),
        auth_state["password"].encode("utf-8"),
    ):
        raise HTTPException(status_code=400, detail="Current password is incorrect")

    secrets, error = _load_auth_secrets()
    if error:
        raise HTTPException(status_code=500, detail=error)

    new_mode = "disabled" if payload.disable_auth else "password"
    secrets["auth_mode"] = new_mode
    secrets["password"] = "" if payload.disable_auth else payload.new_password
    _write_auth_secrets_toml(secrets)

    revoked_count = await _revoke_all_sessions()
    client_host = request.client.host if request.client else "local"
    new_session = generate_token(f"welcome:{client_host}", expires_in_seconds=86400)
    set_session_cookie(response, request, new_session)
    if new_mode == "disabled":
        _log(
            SERVICE,
            f"Authentication disabled by client {client_host}; revoked {revoked_count} existing sessions",
            level="WARNING",
        )
        message = "Authentication disabled"
    else:
        action = "Password authentication enabled" if auth_state["mode"] == "disabled" else "Password updated"
        _log(
            SERVICE,
            f"{action} by client {client_host}; revoked {revoked_count} existing sessions",
            level="INFO",
        )
        message = action

    result = _bootstrap_payload(new_session)
    result["message"] = message
    return result


@router.post("/setup")
def save_setup(payload: SetupConfigRequest, session: SessionToken = Depends(require_auth)) -> dict:
    """Persist PB7 path/interpreter and host identity from the welcome page."""
    role = "master" if payload.role == "master" else "slave"
    pbname = payload.pbname.strip() or pb7_runtime_status()["pbname"]

    save_ini("main", "pb7dir", payload.pb7dir.strip())
    save_ini("main", "pb7venv", payload.pb7venv.strip())
    save_ini("main", "pbname", pbname)
    save_ini("main", "role", role)

    response = _bootstrap_payload(session)
    response["message"] = "Setup saved"
    return response


@router.get("/browse")
def browse_files(
    path: str = Query(default="", description="Filesystem path to open"),
    mode: str = Query(default="directory", description="directory or python"),
    session: SessionToken = Depends(require_auth),
) -> dict:
    """Browse server-side paths for the Welcome setup page."""
    browse_mode = "python" if mode == "python" else "directory"
    current_dir, selected_path = _resolve_browse_path(path)

    try:
        children = sorted(
            current_dir.iterdir(),
            key=lambda child: (not child.is_dir(), child.name.lower()),
        )
    except OSError as exc:
        raise HTTPException(status_code=400, detail=f"Cannot open directory: {exc}") from exc

    entries: list[dict[str, object]] = []
    for child in children:
        is_dir = child.is_dir()
        is_file = child.is_file()

        if browse_mode == "directory" and not is_dir:
            continue
        if browse_mode == "python" and not is_dir and not child.name.startswith("python"):
            continue

        entries.append(
            {
                "name": child.name,
                "path": str(child),
                "is_dir": is_dir,
                "is_file": is_file,
            }
        )

    parent_path = current_dir.parent if current_dir.parent != current_dir else current_dir
    return {
        "current_path": str(current_dir),
        "selected_path": selected_path,
        "parent_path": str(parent_path),
        "mode": browse_mode,
        "entries": entries,
    }


@router.get("/main_page", response_class=HTMLResponse)
def main_page(
    request: Request,
    session: Optional[SessionToken] = Depends(optional_auth),
) -> Response:
    """Serve the standalone Welcome/Login page."""
    auth_state = _password_state()
    active_session = session
    if active_session is None and auth_state["required"] and not auth_state["error"]:
        return RedirectResponse(url=_root_url())
    if active_session is None and not auth_state["required"] and not auth_state["error"]:
        client_host = request.client.host if request.client else "local"
        active_session = _get_or_create_passwordless_session(client_host)

    html_path = _frontend_template_path("welcome.html")
    html = html_path.read_text(encoding="utf-8")

    html = html.replace('"%%TOKEN%%"', json.dumps(active_session.token if active_session else ""))
    html = html.replace('"%%API_ORIGIN%%"', json.dumps(_request_origin(request)))
    html = html.replace('"%%VERSION%%"', json.dumps(PBGUI_VERSION))
    html = html.replace('%%VERSION%%', PBGUI_VERSION)
    html = html.replace('"%%SERIAL%%"', json.dumps(PBGUI_SERIAL))
    html = html.replace('%%SERIAL%%', PBGUI_SERIAL)

    nav_js = _frontend_template_path("pbgui_nav.js")
    nav_hash = str(int(nav_js.stat().st_mtime)) if nav_js.exists() else PBGUI_VERSION
    html = html.replace('%%NAV_HASH%%', nav_hash)

    response = HTMLResponse(
        content=html,
        headers={"Cache-Control": "no-store", "Referrer-Policy": "no-referrer"},
    )
    if active_session is not None:
        set_session_cookie(response, request, active_session)
    return response
