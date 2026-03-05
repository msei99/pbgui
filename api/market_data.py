"""
FastAPI endpoints for market data status monitoring.
"""

from fastapi import APIRouter, Depends
from pathlib import Path
from typing import Any
from datetime import datetime
import json

from .auth import require_auth, SessionToken

router = APIRouter(prefix="/market-data", tags=["market-data"])

PBGDIR = Path(__file__).resolve().parent.parent


def _load_market_data_status() -> dict:
    """Load market data status from status file."""
    status_path = PBGDIR / "data" / "logs" / "market_data_status.json"
    if not status_path.exists():
        return {}
    try:
        with open(status_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _get_exchange_status_key(exchange: str) -> str:
    """Map exchange name to status key."""
    exchange = exchange.lower().strip()
    if exchange in ("binance", "binanceusdm"):
        return "binance_latest_1m"
    elif exchange == "bybit":
        return "bybit_latest_1m"
    elif exchange == "hyperliquid":
        return "latest_1m"
    return ""


def _get_exchange_flag_prefix(exchange: str) -> str:
    """Map exchange name to flag file prefix."""
    exchange = exchange.lower().strip()
    if exchange in ("binance", "binanceusdm"):
        return "binance_latest_1m"
    elif exchange == "bybit":
        return "bybit_latest_1m"
    elif exchange == "hyperliquid":
        return "hyperliquid_latest_1m"
    return ""


@router.get("/status/{exchange}")
def get_market_data_status(exchange: str, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """
    Get market data daemon status for a specific exchange.
    
    Returns:
        {
            "exchange": "binanceusdm",
            "running": bool,
            "queued": bool,
            "status_key": "binance_latest_1m",
            "status": {...},  # Full status from status file
            "coins_done": int,
            "coins_total": int,
            "current_coin": str,
            "coins": {...}  # Per-coin details
        }
    """
    exchange_clean = exchange.lower().strip()
    status_key = _get_exchange_status_key(exchange_clean)
    flag_prefix = _get_exchange_flag_prefix(exchange_clean)
    
    if not status_key or not flag_prefix:
        return {
            "exchange": exchange_clean,
            "error": "Unknown exchange",
            "running": False,
            "queued": False,
        }
    
    # Load status
    all_status = _load_market_data_status()
    exchange_status = all_status.get(status_key, {})
    
    # Check flags
    flag_path = PBGDIR / "data" / "logs" / f"{flag_prefix}_run_now.flag"
    queued = flag_path.exists()
    running = bool(exchange_status.get("running", False))
    
    return {
        "exchange": exchange_clean,
        "status_key": status_key,
        "running": running,
        "queued": queued,
        "coins_done": int(exchange_status.get("coins_done", 0)),
        "coins_total": int(exchange_status.get("coins_total", 0)),
        "current_coin": exchange_status.get("current_coin", ""),
        "interval_seconds": int(exchange_status.get("interval_seconds", 0)),
        "coins": exchange_status.get("coins", {}),
        "status": exchange_status,
    }


@router.post("/refresh-now")
def trigger_refresh_now(request: dict, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """
    Trigger immediate refresh by creating flag file.
    
    Request body:
        {
            "exchange": "binanceusdm"|"bybit"|"hyperliquid"
        }
    """
    exchange = request.get("exchange", "").lower().strip()
    flag_prefix = _get_exchange_flag_prefix(exchange)
    
    if not flag_prefix:
        return {"success": False, "error": "Unknown exchange"}
    
    flag_path = PBGDIR / "data" / "logs" / f"{flag_prefix}_run_now.flag"
    
    try:
        flag_path.touch()
        return {
            "success": True,
            "message": "Refresh triggered — cycle will start within seconds.",
            "exchange": exchange,
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.post("/cancel-refresh")
def cancel_refresh(request: dict, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """
    Cancel queued refresh by removing flag file.
    
    Request body:
        {
            "exchange": "binanceusdm"|"bybit"|"hyperliquid"
        }
    """
    exchange = request.get("exchange", "").lower().strip()
    flag_prefix = _get_exchange_flag_prefix(exchange)
    
    if not flag_prefix:
        return {"success": False, "error": "Unknown exchange"}
    
    flag_path = PBGDIR / "data" / "logs" / f"{flag_prefix}_run_now.flag"
    
    try:
        flag_path.unlink(missing_ok=True)
        return {
            "success": True,
            "message": "Queued refresh cancelled.",
            "exchange": exchange,
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.post("/stop-run")
def stop_current_run(request: dict, session: SessionToken = Depends(require_auth)) -> dict[str, Any]:
    """
    Stop current run by creating stop flag file.
    
    Request body:
        {
            "exchange": "binanceusdm"|"bybit"|"hyperliquid"
        }
    """
    exchange = request.get("exchange", "").lower().strip()
    flag_prefix = _get_exchange_flag_prefix(exchange)
    
    if not flag_prefix:
        return {"success": False, "error": "Unknown exchange"}
    
    stop_path = PBGDIR / "data" / "logs" / f"{flag_prefix}_stop.flag"
    
    try:
        stop_path.touch()
        return {
            "success": True,
            "message": "Stop signal sent — run will abort after current coin.",
            "exchange": exchange,
        }
    except Exception as e:
        return {"success": False, "error": str(e)}
