"""
Authentication and authorization for FastAPI endpoints.

Token-based auth: Streamlit generates tokens on login, FastAPI validates them.
Tokens are stored in data/api_tokens/{token}.json with expiration.
"""

import json
import time
from pathlib import Path
from typing import Optional
from uuid import uuid4

from fastapi import HTTPException, Query, Header, Depends
from pydantic import BaseModel

from pbgui_purefunc import PBGDIR


class SessionToken(BaseModel):
    """Session token data structure."""
    token: str
    user_id: str
    created_at: float
    expires_at: float


def get_tokens_dir() -> Path:
    """Return directory where API tokens are stored."""
    tokens_dir = Path(PBGDIR) / "data" / "api_tokens"
    tokens_dir.mkdir(parents=True, exist_ok=True)
    return tokens_dir


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
    
    for token_file in get_tokens_dir().glob("*.json"):
        try:
            data = json.loads(token_file.read_text(encoding="utf-8"))
            if data.get("expires_at", 0) < now:
                token_file.unlink()
                deleted += 1
        except Exception:
            # Delete corrupt token files
            token_file.unlink()
            deleted += 1
    
    return deleted


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
