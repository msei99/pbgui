"""
FastAPI router: Dashboard data endpoints.

Provides data for Vanilla JS dashboard components (balance, positions, …).
All endpoints require auth (Bearer token).
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, Query

from api.auth import SessionToken, require_auth

router = APIRouter()

# In-memory storage for JS→Python selection sync (keyed by position string)
_balance_user_selections: dict[str, list[str]] = {}


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