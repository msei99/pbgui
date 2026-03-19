"""
FastAPI router: Dashboard data endpoints.

Provides data for Vanilla JS dashboard components (balance, positions, …).
All endpoints require auth (Bearer token).
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, Body, Depends, Query, Request
from fastapi.responses import HTMLResponse

from api.auth import SessionToken, require_auth

router = APIRouter()

# In-memory storage for JS→Python selection sync (keyed by position string)
_balance_user_selections: dict[str, list[str]] = {}

# In-memory storage for JS grid editor → save() sync
# Stores the latest pending grid config from the grid editor component.
_grid_pending_config: dict[str, Any] = {}

# Full pending dashboard config per named dashboard (for the full JS grid editor)
# Key = original dashboard name (or "" for a new dashboard)
_pending_full_configs: dict[str, dict[str, Any]] = {}


# --------------------------------------------------------------------------- helpers

def _get_db():
    from Database import Database
    return Database()


def _get_users():
    from User import Users
    u = Users()
    u.load()
    return u


# --------------------------------------------------------------------------- /balance

@router.get("/balance")
def get_balance(
    users: str = Query(default="ALL", description="Comma-separated user names, or ALL"),
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """
    Return balance data for the given users, including TWE and uPnl per user,
    plus overall totals. Used by the dashboard_balance.html component.
    """
    db = _get_db()
    all_users = _get_users()

    # Resolve user list
    if not users or users.strip().upper() == "ALL":
        users_selected = all_users.list()
    else:
        requested = [u.strip() for u in users.split(",") if u.strip()]
        users_selected = [u for u in requested if u in all_users.list()]

    if not users_selected:
        return {"rows": [], "totals": {"balance": 0, "upnl": 0, "we": 0}}

    balances = db.fetch_balances(users_selected)
    if not balances:
        return {"rows": [], "totals": {"balance": 0, "upnl": 0, "we": 0}}

    rows = []
    total_balance = 0.0
    total_upnl = 0.0
    all_pprices = 0.0

    for row in balances:
        # row: (id, date_ms, balance, user)
        row_id, date_ms, balance, user_name = row[0], row[1], row[2], row[3]

        # format date
        try:
            dt = datetime.utcfromtimestamp(date_ms / 1000)
            date_str = dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            date_str = str(date_ms)

        # fetch positions for TWE / uPnl
        user_obj = all_users.find_user(user_name)
        positions = db.fetch_positions(user_obj) if user_obj else []

        upnl = 0.0
        pprices = 0.0
        for pos in (positions or []):
            pprices += pos[3] * pos[5]
            upnl += pos[4]

        all_pprices += pprices
        twe = (100 / balance * pprices) if balance and pprices else 0.0

        total_balance += balance
        total_upnl += upnl

        rows.append({
            "id": row_id,
            "user": user_name,
            "date": date_str,
            "balance": round(balance, 2),
            "upnl": round(upnl, 2),
            "we": round(twe, 2),
        })

    total_twe = (100 / total_balance * all_pprices) if total_balance and all_pprices else 0.0

    return {
        "rows": rows,
        "totals": {
            "balance": round(total_balance, 2),
            "upnl": round(total_upnl, 2),
            "we": round(total_twe, 2),
        },
    }


# ---------------------------------------------------------------- /balance/selection

@router.post("/balance/selection")
def set_balance_selection(
    payload: dict[str, Any],
    session: SessionToken = Depends(require_auth),
) -> dict[str, str]:
    """Store user selection from JS dropdown so save() can read it."""
    position = payload.get("position", "")
    users = payload.get("users", ["ALL"])
    if position:
        _balance_user_selections[position] = users
    return {"status": "ok"}


@router.get("/balance/selection")
def get_balance_selection_endpoint(
    position: str = Query(description="Dashboard position key, e.g. 1_1"),
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """Return the stored user selection for a dashboard position."""
    users = _balance_user_selections.get(position)
    if users is None:
        return {"found": False, "users": []}
    return {"found": True, "users": users}


# ---------------------------------------------------------------- /grid/pending

@router.post("/grid/pending")
def set_grid_pending(
    payload: dict[str, Any],
    session: SessionToken = Depends(require_auth),
) -> dict[str, str]:
    """
    Store the pending grid config from the JS grid editor.
    Called on every change (debounced by the JS side).
    Payload: { name, rows, cols, dashboard_type_1_1, ... }
    """
    _grid_pending_config.clear()
    _grid_pending_config.update(payload)
    return {"status": "ok"}


@router.get("/grid/pending")
def get_grid_pending(
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """Return the latest pending grid config from the JS editor."""
    if not _grid_pending_config:
        return {"found": False}
    return {"found": True, **_grid_pending_config}


# ---------------------------------------------------------------- /users

@router.get("/users")
def get_dashboard_users(
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """Return list of available user names for cell config dropdowns."""
    try:
        users = _get_users()
        return {"users": users.list()}
    except Exception:
        return {"users": []}


# ---------------------------------------------------------------- /pending_full

@router.get("/pending_full")
def get_pending_full(
    name: str = Query(default="", description="Dashboard name (original)"),
    session: SessionToken = Depends(require_auth),
) -> dict[str, Any]:
    """Return the full pending dashboard config for a named dashboard."""
    cfg = _pending_full_configs.get(name)
    if cfg is None:
        return {"found": False, "config": {}}
    return {"found": True, "config": cfg}


@router.post("/pending_full")
def set_pending_full(
    payload: dict[str, Any],
    name: str = Query(default="", description="Dashboard name (original)"),
    session: SessionToken = Depends(require_auth),
) -> dict[str, str]:
    """Store the full pending dashboard config for a named dashboard."""
    _pending_full_configs[name] = payload
    return {"status": "ok"}


# ---------------------------------------------------------------- /editor_page

@router.get("/editor_page", response_class=HTMLResponse)
def get_editor_page(
    name: str = Query(default="", description="Dashboard name"),
    api_base: str = Query(default="", description="API base URL"),
    session: SessionToken = Depends(require_auth),
) -> HTMLResponse:
    """Serve the full dashboard grid editor HTML page."""
    import json as _json
    from pathlib import Path as _P
    html_path = _P(__file__).parent.parent / "frontend" / "dashboard_editor.html"
    html = html_path.read_text(encoding="utf-8")
    html = html.replace("%%TOKEN%%", session.token)
    html = html.replace("%%API_BASE%%", api_base)
    html = html.replace("%%DASHBOARD_NAME%%", _json.dumps(name))
    return HTMLResponse(content=html)


# ---------------------------------------------------------------- /balance_page

@router.get("/balance_page", response_class=HTMLResponse)
def get_balance_page(
    request: Request,
    users: str = Query(default="ALL", description="Comma-separated user names"),
    position: str = Query(default="preview", description="Position key"),
    session: SessionToken = Depends(require_auth),
) -> HTMLResponse:
    """Serve the balance component as a standalone HTML page (for iframe embedding)."""
    import json as _json
    import uuid as _uuid
    from pathlib import Path as _P
    html_path = _P(__file__).parent.parent / "frontend" / "dashboard_balance.html"
    html = html_path.read_text(encoding="utf-8")
    # Wrap in a full HTML document so the iframe renders correctly
    html = (
        "<!DOCTYPE html>\n"
        '<html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        "</head><body style=\"margin:0;background:#0e1117\">\n"
        + html
        + "\n</body></html>"
    )
    # Determine API base from request host header
    req_host = request.headers.get("host", "127.0.0.1:8000")
    api_base = f"http://{req_host}/api"
    api_host = req_host
    # Parse users list
    user_list = [u.strip() for u in users.split(",") if u.strip()]
    if not user_list:
        user_list = ["ALL"]
    # Get all available users
    try:
        all_users_list = ['ALL'] + sorted(_get_users().list(), key=str.lower)
    except Exception:
        all_users_list = ['ALL']
    instance_id = _uuid.uuid4().hex[:8]
    html = html.replace('"%%TOKEN%%"',        f'"{session.token}"')
    html = html.replace('"%%API_BASE%%"',    f'"{api_base}"')
    html = html.replace('"%%API_HOST%%"',    f'"{api_host}"')
    html = html.replace('%%USERS%%',          _json.dumps(user_list))
    html = html.replace('%%ALL_USERS%%',      _json.dumps(all_users_list))
    html = html.replace('%%EDIT_MODE%%',      'false')
    html = html.replace('"%%INSTANCE_ID%%"', f'"{instance_id}"')
    html = html.replace('"%%POSITION%%"',    f'"{position}"')
    return HTMLResponse(content=html)
