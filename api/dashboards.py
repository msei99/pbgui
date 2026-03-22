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

import re

router = APIRouter()

_VALID_DASHBOARD_NAME = re.compile(r'^[\w\- ]+$')


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
    return bool(name) and bool(_VALID_DASHBOARD_NAME.match(name))


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


# --------------------------------------------------------------------------- /templates
# NOTE: These routes MUST be registered before /{name} to avoid FastAPI
#       routing "templates" as a {name} path parameter.

def _templates_dir() -> Path:
    from pbgui_func import PBGDIR
    d = Path(f"{PBGDIR}/data/dashboards/templates")
    d.mkdir(parents=True, exist_ok=True)
    return d


def _template_file(name: str) -> Path:
    return _templates_dir() / f"{name}.json"


_USER_KEY_RE = re.compile(r'^(dashboard_\w+_users_)\d+_\d+$')


@router.get("/templates")
def list_templates(
    session: SessionToken = Depends(require_auth),
) -> dict[str, list[str]]:
    """Return a sorted list of all template names."""
    names = sorted(f.stem for f in _templates_dir().glob("*.json"))
    return {"templates": names}


@router.post("/templates/{name}")
def save_template(
    name: str,
    payload: dict[str, Any],
    session: SessionToken = Depends(require_auth),
) -> dict[str, str]:
    """Save the given dashboard config as a template."""
    if not _valid_name(name):
        raise HTTPException(status_code=400, detail="Invalid template name")
    if "rows" not in payload or "cols" not in payload:
        raise HTTPException(status_code=422, detail="Config must contain 'rows' and 'cols'")
    f = _template_file(name)
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


@router.delete("/templates/{name}")
def delete_template(
    name: str,
    session: SessionToken = Depends(require_auth),
) -> dict[str, str]:
    """Delete a template."""
    if not _valid_name(name):
        raise HTTPException(status_code=400, detail="Invalid template name")
    f = _template_file(name)
    if not f.exists():
        raise HTTPException(status_code=404, detail=f"Template '{name}' not found")
    f.unlink()
    return {"status": "ok", "name": name}


@router.patch("/templates/{name}")
def rename_template(
    name: str,
    payload: dict[str, Any],
    session: SessionToken = Depends(require_auth),
) -> dict[str, str]:
    """Rename a template (payload: {new_name: str})."""
    new_name = (payload.get("new_name") or "").strip()
    if not _valid_name(name):
        raise HTTPException(status_code=400, detail="Invalid template name")
    if not _valid_name(new_name):
        raise HTTPException(status_code=400, detail="Invalid new template name")
    src = _template_file(name)
    if not src.exists():
        raise HTTPException(status_code=404, detail=f"Template '{name}' not found")
    dst = _template_file(new_name)
    if dst.exists():
        raise HTTPException(status_code=409, detail=f"Template '{new_name}' already exists")
    src.rename(dst)
    return {"status": "ok", "name": new_name}


# --------------------------------------------------------------------------- /from_template


@router.post("/from_template")
def dashboards_from_template(
    payload: dict[str, Any],
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """
    Create a dashboard from a template.

    Simple mode (free name):
        {"template": "my_template", "name": "my_new_dashboard"}

    Bulk mode (prefix + users, legacy):
        {"template": "my_template", "users": ["bybit_main"], "prefix": "trade"}
    """
    template_name: str = payload.get("template", "")
    free_name: str = payload.get("name", "").strip()
    users: list[str] = payload.get("users", [])
    prefix: str = payload.get("prefix", "").strip()

    if not _valid_name(template_name):
        raise HTTPException(status_code=400, detail="Invalid template name")

    tf = _template_file(template_name)
    if not tf.exists():
        raise HTTPException(status_code=404, detail=f"Template '{template_name}' not found")

    with tf.open() as fh:
        template_config: dict[str, Any] = json.load(fh)

    # ── Simple mode: single dashboard with free name ──────────────────────
    if free_name:
        if not _valid_name(free_name):
            raise HTTPException(status_code=400, detail="Invalid dashboard name")
        config = json.loads(json.dumps(template_config))
        f = _dashboard_file(free_name)
        tmp = f.with_suffix(".tmp")
        try:
            with tmp.open("w") as fh:
                json.dump(config, fh, indent=4)
            tmp.replace(f)
        except Exception:
            if tmp.exists():
                tmp.unlink(missing_ok=True)
            return {"status": "error", "created": [], "skipped": [free_name]}
        return {"status": "ok", "created": [free_name], "skipped": []}

    # ── Bulk mode: prefix + users ──────────────────────────────────────────
    if not users:
        raise HTTPException(status_code=422, detail="No users specified")
    if not prefix:
        raise HTTPException(status_code=422, detail="prefix is required")
    if not _valid_name(prefix):
        raise HTTPException(status_code=400, detail="Invalid prefix")

    created: list[str] = []
    skipped: list[str] = []

    for user in users:
        dash_name = f"{prefix}_{user}"
        if not _valid_name(dash_name):
            skipped.append(dash_name)
            continue

        config = json.loads(json.dumps(template_config))  # deep copy

        # Replace all user selections with this single user
        for key in list(config.keys()):
            if _USER_KEY_RE.match(key):
                config[key] = [user]

        f = _dashboard_file(dash_name)
        tmp = f.with_suffix(".tmp")
        try:
            with tmp.open("w") as fh:
                json.dump(config, fh, indent=4)
            tmp.replace(f)
            created.append(dash_name)
        except Exception:
            if tmp.exists():
                tmp.unlink(missing_ok=True)
            skipped.append(dash_name)

    return {"status": "ok", "created": created, "skipped": skipped}


# --------------------------------------------------------------------------- /{name}
# NOTE: Parameterised catch-all routes are listed LAST so that the specific
#       static paths above (/templates, /from_template) are matched first.

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
