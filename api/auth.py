"""Authentication, welcome page, and setup helpers for FastAPI endpoints."""

import json
import os
import time
import toml
from pathlib import Path
from typing import Optional
from urllib.parse import urlencode
from uuid import uuid4

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from pydantic import BaseModel

from pbgui_purefunc import (
    PBGDIR,
    PBGUI_SERIAL,
    PBGUI_VERSION,
    pb7_runtime_status,
    save_ini,
    streamlit_secrets_path,
)

router = APIRouter()
_DEFAULT_PASSWORD = "PBGui$Bot!"


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
    tokens_dir = Path(PBGDIR) / "data" / "api_tokens"
    tokens_dir.mkdir(parents=True, exist_ok=True)
    return tokens_dir


def _write_secrets_toml(secrets: dict) -> None:
    secrets_path = streamlit_secrets_path()
    secrets_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = secrets_path.with_suffix(".tmp")
    with open(tmp_path, "w", encoding="utf-8") as handle:
        toml.dump(secrets, handle)
    os.replace(str(tmp_path), str(secrets_path))


def _ensure_secrets_file() -> Path:
    secrets_path = streamlit_secrets_path()
    secrets_path.parent.mkdir(parents=True, exist_ok=True)
    if not secrets_path.exists():
        _write_secrets_toml({"password": _DEFAULT_PASSWORD})
    return secrets_path


def _load_streamlit_secrets() -> tuple[dict, str | None]:
    secrets_path = _ensure_secrets_file()
    try:
        return toml.load(secrets_path), None
    except toml.TomlDecodeError as exc:
        return {}, f"Invalid secrets.toml: {exc}"


def _password_state() -> dict:
    secrets, error = _load_streamlit_secrets()
    password_value = str(secrets.get("password", "")) if not error else ""
    password_required = bool(password_value) and not error
    password_missing = (not password_required) and not error
    return {
        "error": error,
        "required": password_required,
        "missing": password_missing,
        "password": password_value,
    }


def _frontend_template_path(name: str) -> Path:
    return Path(__file__).parent.parent / "frontend" / name


def _request_origin(request: Request) -> str:
    scheme = request.url.scheme
    host = request.url.hostname or "127.0.0.1"
    port = request.url.port
    return f"{scheme}://{host}" + (f":{port}" if port else "")


def _effective_st_base(request: Request, st_base: str) -> str:
    if st_base:
        return st_base
    host = request.url.hostname or "127.0.0.1"
    return f"http://{host}:8501"


def _main_page_url(token: str = "", st_base: str = "") -> str:
    params: dict[str, str] = {}
    if token:
        params["token"] = token
    if st_base:
        params["st_base"] = st_base
    query = urlencode(params)
    return "/api/auth/main_page" + (f"?{query}" if query else "")


def _root_url(st_base: str = "") -> str:
    params: dict[str, str] = {}
    if st_base:
        params["st_base"] = st_base
    query = urlencode(params)
    return "/" + (f"?{query}" if query else "")


def build_root_entry_response(
    request: Request,
    st_base: str = "",
    session: SessionToken | None = None,
) -> HTMLResponse | RedirectResponse:
    """Return the public root response.

    If a password is configured and no valid token is present, show the small
    root login page first. Otherwise jump straight to the Welcome page.
    """
    auth_state = _password_state()
    effective_st_base = _effective_st_base(request, st_base)

    if auth_state["error"] or session is not None or not auth_state["required"]:
        token = session.token if session else ""
        return RedirectResponse(url=_main_page_url(token=token, st_base=effective_st_base))

    html = _frontend_template_path("root_login.html").read_text(encoding="utf-8")
    html = html.replace('"%%API_ORIGIN%%"', json.dumps(_request_origin(request)))
    html = html.replace('"%%ST_BASE%%"', json.dumps(effective_st_base))
    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})


def _bootstrap_payload(session: SessionToken | None = None) -> dict:
    auth_state = _password_state()
    return {
        "version": PBGUI_VERSION,
        "serial": PBGUI_SERIAL,
        "auth": {
            "authenticated": session is not None,
            "password_required": auth_state["required"],
            "password_missing": auth_state["missing"],
            "error": auth_state["error"],
            "token": session.token if session else "",
            "user_id": session.user_id if session else "",
            "expires_at": session.expires_at if session else 0,
        },
        "setup": pb7_runtime_status(),
    }


def generate_token(user_id: str, expires_in_seconds: int = 86400) -> SessionToken:
    """Generate a new API token for a user.
    
    Args:
        user_id: User identifier (from Streamlit session)
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
    token_file.write_text(
        json.dumps(session.model_dump(), indent=2),
        encoding="utf-8"
    )
    
    return session


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
        token_file.write_text(
            json.dumps(session.model_dump(), indent=2),
            encoding="utf-8",
        )
        return session
    except Exception:
        return None


# ── FastAPI Dependencies ──

def get_token_from_request(
    token: Optional[str] = Query(None, description="API token"),
    authorization: Optional[str] = Header(None, description="Bearer token")
) -> str:
    """Extract token from request (query param or Authorization header).
    
    Supports both:
    - ?token=xxx (for iframe URLs)
    - Authorization: Bearer xxx (for API calls)
    """
    # Try query param first (iframe)
    if token:
        return token.strip()
    
    # Try Authorization header
    if authorization and authorization.startswith("Bearer "):
        return authorization.replace("Bearer ", "").strip()
    
    raise HTTPException(
        status_code=401,
        detail="Missing authentication token. Provide ?token=xxx or Authorization: Bearer xxx"
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
    token: Optional[str] = Query(None),
    authorization: Optional[str] = Header(None)
) -> Optional[SessionToken]:
    """FastAPI dependency for optional authentication.
    
    Returns SessionToken if valid token provided, None otherwise.
    Does not raise exception for missing/invalid tokens.
    """
    # Try to extract token
    token_str = None
    if token:
        token_str = token.strip()
    elif authorization and authorization.startswith("Bearer "):
        token_str = authorization.replace("Bearer ", "").strip()
    
    if not token_str:
        return None
    
    return validate_token(token_str)


@router.get("/bootstrap")
def bootstrap(session: Optional[SessionToken] = Depends(optional_auth)) -> dict:
    """Return welcome-page bootstrap data for auth and PB7 runtime status."""
    return _bootstrap_payload(session)


@router.post("/login")
def login(payload: LoginRequest, request: Request) -> dict:
    """Authenticate the welcome page and return a fresh API token."""
    auth_state = _password_state()
    if auth_state["error"]:
        raise HTTPException(status_code=500, detail=auth_state["error"])

    if auth_state["required"] and payload.password != auth_state["password"]:
        raise HTTPException(status_code=401, detail="Password incorrect")

    client_host = request.client.host if request.client else "local"
    session = generate_token(f"welcome:{client_host}", expires_in_seconds=86400)
    response = _bootstrap_payload(session)
    response["message"] = "Authenticated"
    return response


@router.post("/logout")
def logout(session: SessionToken = Depends(require_auth)) -> dict:
    """Revoke the current API token."""
    revoke_token(session.token)
    return {"ok": True}


@router.post("/change-password")
def change_password(payload: PasswordChangeRequest, session: SessionToken = Depends(require_auth)) -> dict:
    """Change the Streamlit password from the FastAPI welcome page."""
    auth_state = _password_state()
    if auth_state["error"]:
        raise HTTPException(status_code=500, detail=auth_state["error"])

    if auth_state["required"] and payload.current_password != auth_state["password"]:
        raise HTTPException(status_code=400, detail="Current password is incorrect")

    secrets, error = _load_streamlit_secrets()
    if error:
        raise HTTPException(status_code=500, detail=error)

    secrets["password"] = payload.new_password
    _write_secrets_toml(secrets)

    response = _bootstrap_payload(session)
    response["message"] = "Password updated"
    return response


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
    st_base: str = Query(default="", description="Browser-visible Streamlit base URL"),
    session: Optional[SessionToken] = Depends(optional_auth),
) -> Response:
    """Serve the standalone Welcome/Login page."""
    auth_state = _password_state()
    active_session = session
    if active_session is None and auth_state["required"] and not auth_state["error"]:
        return RedirectResponse(url=_root_url(st_base=st_base))
    if active_session is None and not auth_state["required"] and not auth_state["error"]:
        client_host = request.client.host if request.client else "local"
        active_session = generate_token(f"welcome:{client_host}", expires_in_seconds=86400)

    html_path = _frontend_template_path("welcome.html")
    html = html_path.read_text(encoding="utf-8")

    html = html.replace('"%%TOKEN%%"', json.dumps(active_session.token if active_session else ""))
    html = html.replace('"%%API_ORIGIN%%"', json.dumps(_request_origin(request)))
    html = html.replace('"%%ST_BASE%%"', json.dumps(_effective_st_base(request, st_base)))
    html = html.replace('"%%VERSION%%"', json.dumps(PBGUI_VERSION))
    html = html.replace('%%VERSION%%', PBGUI_VERSION)
    html = html.replace('"%%SERIAL%%"', json.dumps(PBGUI_SERIAL))
    html = html.replace('%%SERIAL%%', PBGUI_SERIAL)

    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})
