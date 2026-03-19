"""
FastAPI router: Dashboard configuration CRUD.

Provides REST endpoints for loading, saving, listing, and deleting
dashboard configuration files stored in {PBGDIR}/data/dashboards/.

All endpoints require auth (Bearer token).
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from api.auth import SessionToken, require_auth

router = APIRouter()


# --------------------------------------------------------------------------- helpers

def _dashboards_dir() -> Path:
    from pbgui_func import PBGDIR
    d = Path(f"{PBGDIR}/data/dashboards")
    d.mkdir(parents=True, exist_ok=True)
    return d


def _dashboard_file(name: str) -> Path:
    return _dashboards_dir() / f"{name}.json"


def _valid_name(name: str) -> bool:
    """Reject names that could escape the dashboards directory."""
    return bool(name) and "/" not in name and "\\" not in name and name != ".."


# --------------------------------------------------------------------------- /users

@router.get("/users")
def list_users(
    session: SessionToken = Depends(require_auth),
) -> dict[str, list[str]]:
    """Return the sorted list of available user names."""
    from User import Users
    u = Users()
    u.load()
    names = sorted(u.list(), key=str.lower)
    return {"users": names}


# --------------------------------------------------------------------------- /

@router.get("")
def list_dashboards(
    session: SessionToken = Depends(require_auth),
) -> dict[str, list[str]]:
    """Return a sorted list of all dashboard names."""
    d = _dashboards_dir()
    names = sorted(f.stem for f in d.glob("*.json"))
    return {"dashboards": names}


# --------------------------------------------------------------------------- /{name}

@router.get("/{name}")
def get_dashboard(
    name: str,
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """Load and return a dashboard config by name."""
    if not _valid_name(name):
        raise HTTPException(status_code=400, detail="Invalid dashboard name")
    f = _dashboard_file(name)
    if not f.exists():
        raise HTTPException(status_code=404, detail=f"Dashboard '{name}' not found")
    with f.open() as fh:
        config: dict[str, Any] = json.load(fh)
    return {"name": name, "config": config}


@router.post("/{name}")
def save_dashboard(
    name: str,
    payload: dict[str, Any],
    session: SessionToken = Depends(require_auth),
) -> dict[str, str]:
    """
    Save a dashboard config.  Body must be the raw config dict (same format
    as the JSON files written by Dashboard.save()).
    """
    if not _valid_name(name):
        raise HTTPException(status_code=400, detail="Invalid dashboard name")
    if "rows" not in payload or "cols" not in payload:
        raise HTTPException(status_code=422, detail="Config must contain 'rows' and 'cols'")
    f = _dashboard_file(name)
    # Atomic write: tmp → rename
    tmp = f.with_suffix(".tmp")
    try:
        with tmp.open("w") as fh:
            json.dump(payload, fh, indent=4)
        tmp.replace(f)
    except Exception as exc:
        if tmp.exists():
            tmp.unlink(missing_ok=True)
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {"status": "ok", "name": name}


@router.delete("/{name}")
def delete_dashboard(
    name: str,
    session: SessionToken = Depends(require_auth),
) -> dict[str, str]:
    """Delete a dashboard config file."""
    if not _valid_name(name):
        raise HTTPException(status_code=400, detail="Invalid dashboard name")
    f = _dashboard_file(name)
    if not f.exists():
        raise HTTPException(status_code=404, detail=f"Dashboard '{name}' not found")
    f.unlink()
    return {"status": "ok", "name": name}
