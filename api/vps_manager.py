from __future__ import annotations

import asyncio
import json
import re
import threading
import time
import traceback
import uuid
from decimal import Decimal
from pathlib import Path

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field, StrictInt

from api.auth import SessionToken, authenticate_websocket, require_auth
from api.page_templates import render_page_urls, script_json
from api.vps import get_bot_log_matches
from file_lock import advisory_file_lock
from secure_files import atomic_write_private_text, ensure_private_directory, read_regular_file_nofollow
from logging_helpers import human_log as _log
from vps_manager_service import UnknownHostKeyError, VPSManagerService

DetailPayload = dict[str, object]

SERVICE = "VPSManagerApi"

router = APIRouter()

_service: VPSManagerService | None = None
_CLUSTER_ONBOARD_TASKS: dict[str, asyncio.Task[dict[str, object]]] = {}
_CLUSTER_ONBOARD_JOBS: dict[str, dict[str, object]] = {}
_CLUSTER_ONBOARD_ACTIVE: dict[str, str] = {}
_CLUSTER_ONBOARD_JOB_TTL_SECONDS = 3600
_HL_RATE_LIMIT_INTERVAL_SECONDS = 300
_HL_RATE_LIMIT_IMPORT_INTERVAL_SECONDS = 30
_HL_RATE_LIMIT_HISTORY_SECONDS = 24 * 60 * 60
_HL_RATE_LIMIT_HISTORY_DIR = Path(__file__).resolve().parent.parent / "data" / "vpsmanager" / "hl_rate_limit_history"
_HL_RATE_LIMIT_HISTORY_FILE = _HL_RATE_LIMIT_HISTORY_DIR / "samples.json"
_HL_WALLET_PATTERN = re.compile(r"0x[0-9a-fA-F]{40}\Z")
_HL_CREDIT_PRICE_USDC = Decimal("0.0005")
_HL_MAX_CREDIT_PURCHASE = 100_000
_hl_credit_purchase_lock = threading.Lock()
_hl_rate_limit_task: asyncio.Task[None] | None = None
_hl_rate_limit_accounts: dict[str, dict[str, object]] = {}
_hl_rate_limit_accounts_lock = threading.RLock()


class ExistingVpsImportRequest(BaseModel):
    hostname: str = ""
    ip: str = ""
    user: str = ""
    user_pw: str = ""
    local_sudo_pw: str = ""
    install_dir: str = ""
    accept_unknown_host: bool = False
    accepted_host_key_fingerprint: str = ""


class ClusterNodesImportRequest(BaseModel):
    local_sudo_pw: str = ""
    passwords: dict[str, str] = {}


class HlCreditPurchaseRequest(BaseModel):
    """A confirmed, one-time purchase for a saved Hyperliquid account."""

    user_name: str
    credits: StrictInt = Field(ge=1, le=_HL_MAX_CREDIT_PURCHASE)

MASTER_CONTEXT_VIEWS = {
    "master",
    "master-task-log",
    "master-host-logs",
    "master-pbgui-branch",
    "master-pb7-branch",
    "master-pb8-branch",
    "master-ufw",
}
VPS_CONTEXT_VIEWS = {
    "vps",
    "vps-task-log",
    "vps-host-logs",
    "vps-setup",
    "vps-pbgui-branch",
    "vps-pb7-branch",
    "vps-pb8-branch",
    "vps-ufw",
}


def _get_service() -> VPSManagerService:
    global _service
    if _service is None:
        _service = VPSManagerService()
    return _service


def get_service_instance() -> VPSManagerService:
    return _get_service()


def startup() -> None:
    """Prepare an existing VPS Manager service for a new API lifespan."""
    global _hl_rate_limit_task
    if _service is not None:
        _service.prepare_startup()
    if _hl_rate_limit_task is None or _hl_rate_limit_task.done():
        _hl_rate_limit_task = asyncio.get_running_loop().create_task(
            _poll_hl_rate_limits(), name="vps-manager-hl-rate-limits"
        )


async def shutdown() -> None:
    """Join API-owned deploy controllers and Hyperliquid polling."""
    global _hl_rate_limit_task
    task = _hl_rate_limit_task
    _hl_rate_limit_task = None
    if task is not None:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    tasks = list(_CLUSTER_ONBOARD_TASKS.values())
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    _CLUSTER_ONBOARD_TASKS.clear()
    if _service is not None:
        await asyncio.to_thread(_service.shutdown)


def _configured_hl_wallets() -> dict[str, list[str]]:
    """Group saved Hyperliquid account names by validated public wallet address."""
    from User import Users

    wallets: dict[str, list[str]] = {}
    for user in Users():
        if str(getattr(user, "exchange", "") or "").lower() != "hyperliquid":
            continue
        address = str(getattr(user, "wallet_address", "") or "").strip()
        if not _HL_WALLET_PATTERN.fullmatch(address):
            continue
        wallets.setdefault(address.lower(), []).append(str(user.name))
    return {address: sorted(set(names)) for address, names in wallets.items()}



def restart_block_reason() -> str:
    """Keep API restart from interrupting a paid exchange action."""
    return "A Hyperliquid request-credit purchase is in progress." if _hl_credit_purchase_lock.locked() else ""


def _read_hl_rate_limit_now(address: str) -> tuple[int, int]:
    """Read current account action counters after a purchase."""
    response = httpx.post(
        "https://api.hyperliquid.xyz/info",
        json={"type": "userRateLimit", "user": address},
        timeout=10.0,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("Unexpected userRateLimit response")
    used, cap = payload.get("nRequestsUsed"), payload.get("nRequestsCap")
    if any(type(value) is not int or value < 0 for value in (used, cap)):
        raise ValueError("Invalid userRateLimit counters")
    return used, cap


def _read_hl_rate_limit_history() -> dict[str, list[list[int]]]:
    """Read account samples from the private on-disk history."""
    if not _HL_RATE_LIMIT_HISTORY_FILE.exists():
        return {}
    raw = read_regular_file_nofollow(_HL_RATE_LIMIT_HISTORY_FILE, _HL_RATE_LIMIT_HISTORY_DIR)
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError("Invalid Hyperliquid rate-limit history")
    return payload


def _append_hl_rate_limit_sample(address: str, sampled_at: int, used: int, cap: int) -> None:
    """Persist one sample per wallet, pruning old samples under a process lock."""
    ensure_private_directory(_HL_RATE_LIMIT_HISTORY_DIR)
    with advisory_file_lock(_HL_RATE_LIMIT_HISTORY_FILE):
        history = _read_hl_rate_limit_history()
        cutoff = sampled_at - _HL_RATE_LIMIT_HISTORY_SECONDS
        history = {
            wallet: [sample for sample in samples
                     if isinstance(sample, list) and len(sample) == 3
                     and isinstance(sample[0], int) and sample[0] >= cutoff][-300:]
            for wallet, samples in history.items()
            if _HL_WALLET_PATTERN.fullmatch(wallet) and isinstance(samples, list)
        }
        points = history.setdefault(address, [])
        points[:] = [sample for sample in points if sample[0] != sampled_at]
        points.append([sampled_at, used, cap])
        points.sort(key=lambda sample: sample[0])
        history[address] = points[-300:]
        atomic_write_private_text(_HL_RATE_LIMIT_HISTORY_FILE, json.dumps(history, indent=4))


def _get_hl_rate_limit_history(address: str) -> list[dict[str, int]]:
    """Return recent counter samples for one validated wallet."""
    ensure_private_directory(_HL_RATE_LIMIT_HISTORY_DIR)
    with advisory_file_lock(_HL_RATE_LIMIT_HISTORY_FILE):
        history = _read_hl_rate_limit_history()
    cutoff = int(time.time()) - _HL_RATE_LIMIT_HISTORY_SECONDS
    samples = history.get(address, [])
    if not isinstance(samples, list):
        raise ValueError("Invalid Hyperliquid rate-limit account history")
    return [
        {"sampled_at": sample[0], "used": sample[1], "cap": sample[2]}
        for sample in samples
        if isinstance(sample, list) and len(sample) == 3
        and all(isinstance(value, int) and not isinstance(value, bool) and value >= 0 for value in sample)
        and sample[0] >= cutoff
    ]


async def _poll_hl_rate_limits() -> None:
    """Import the latest VPS-agent samples; masters never poll Hyperliquid here."""
    global _hl_rate_limit_accounts
    from api.vps import get_monitor_state_snapshot

    while True:
        cycle_started = time.monotonic()
        try:
            wallets = await asyncio.to_thread(_configured_hl_wallets)
            snapshot = await asyncio.to_thread(get_monitor_state_snapshot)
            now = int(time.time())
            wallet_by_user = {user: address for address, names in wallets.items() for user in names}
            observed: dict[str, dict[str, object]] = {
                address: {"users": names, "hosts": set(), "bots": set(), "sample": None}
                for address, names in wallets.items()
            }
            for version, field in (("7", "v7_instances"), ("8", "v8_instances")):
                by_host = snapshot.get(field) if isinstance(snapshot, dict) else None
                if not isinstance(by_host, dict):
                    continue
                for host, rows in by_host.items():
                    if not isinstance(host, str) or not isinstance(rows, list):
                        continue
                    for row in rows:
                        if not isinstance(row, dict) or row.get("running") is not True:
                            continue
                        name = str(row.get("name") or "").strip()
                        user = str(row.get("user") or "").strip()
                        address = wallet_by_user.get(user)
                        if not name or not address:
                            continue
                        item = observed[address]
                        item["hosts"].add(host)
                        item["bots"].add((name, host, version))
                        sample = row.get("hl_rate_limit")
                        if not isinstance(sample, dict):
                            continue
                        used, cap, sampled_at = (sample.get(key) for key in ("used", "cap", "sampled_at"))
                        if any(type(value) is not int or value < 0 for value in (used, cap, sampled_at)):
                            continue
                        if sampled_at > now + 300 or sampled_at < now - 86400:
                            continue
                        current = item["sample"]
                        if current is None or sampled_at > current["sampled_at"]:
                            item["sample"] = {"used": used, "cap": cap, "sampled_at": sampled_at}
            with _hl_rate_limit_accounts_lock:
                previous = dict(_hl_rate_limit_accounts)
            next_accounts: dict[str, dict[str, object]] = {}
            for address, item in observed.items():
                bots = [{"name": name, "host": host, "pb_version": version}
                        for name, host, version in sorted(item["bots"])]
                entry: dict[str, object] = {
                    "users": item["users"], "hosts": sorted(item["hosts"]), "bots": bots,
                    "state": "pending" if bots else "idle",
                }
                sample = item["sample"]
                if sample is not None:
                    entry.update(sample)
                    entry["state"] = "ok" if now - sample["sampled_at"] <= 2 * _HL_RATE_LIMIT_INTERVAL_SECONDS else "stale"
                    prior_sampled_at = int(previous.get(address, {}).get("sampled_at") or 0)
                    if sample["sampled_at"] != prior_sampled_at:
                        try:
                            await asyncio.to_thread(_append_hl_rate_limit_sample, address, sample["sampled_at"], sample["used"], sample["cap"])
                        except (OSError, ValueError, RuntimeError) as exc:
                            _log(SERVICE, f"Hyperliquid VPS sample history write failed: {type(exc).__name__}", level="WARNING")
                elif not bots and previous.get(address, {}).get("source") == "on_demand":
                    prior = previous[address]
                    entry.update({key: prior[key] for key in ("used", "cap", "sampled_at") if key in prior})
                    entry["state"] = "on_demand"
                    entry["source"] = "on_demand"
                next_accounts[address] = entry
            with _hl_rate_limit_accounts_lock:
                for address, entry in next_accounts.items():
                    current = _hl_rate_limit_accounts.get(address, {})
                    if (current.get("source") in {"on_demand", "purchase"}
                            and int(current.get("sampled_at") or 0) >= int(entry.get("sampled_at") or 0)):
                        entry.update({key: current[key] for key in ("used", "cap", "sampled_at") if key in current})
                        entry["state"] = "on_demand"
                        entry["source"] = current["source"]
                _hl_rate_limit_accounts = next_accounts
        except Exception as exc:
            _log(SERVICE, f"Hyperliquid VPS sample import failed: {type(exc).__name__}", level="WARNING")
        await asyncio.sleep(max(1.0, _HL_RATE_LIMIT_IMPORT_INTERVAL_SECONDS - (time.monotonic() - cycle_started)))


@router.get("/user-rate-limits")
def get_hl_user_rate_limits(session: SessionToken = Depends(require_auth)) -> JSONResponse:
    """Return cached address-limit counters without triggering exchange traffic."""
    del session
    with _hl_rate_limit_accounts_lock:
        accounts = sorted((dict(item) for item in _hl_rate_limit_accounts.values()), key=lambda item: item.get("users") or [])
    return JSONResponse(
        content={"accounts": accounts, "interval_seconds": _HL_RATE_LIMIT_INTERVAL_SECONDS},
        headers={"Cache-Control": "no-store"},
    )


@router.get("/user-rate-limits/live/{user_name}")
def get_hl_user_rate_limit_live(
    user_name: str,
    session: SessionToken = Depends(require_auth),
) -> JSONResponse:
    """Read one saved wallet once when its API-key editor is opened."""
    del session
    if not user_name or user_name in {".", ".."} or any(char in user_name for char in ("/", "\\", "\x00")) or any(ord(char) < 32 for char in user_name):
        raise HTTPException(status_code=400, detail="Invalid account name")
    wallets = _configured_hl_wallets()
    address = next((wallet for wallet, names in wallets.items() if user_name in names), None)
    if address is None:
        raise HTTPException(status_code=404, detail="Hyperliquid account not found")
    try:
        used, cap = _read_hl_rate_limit_now(address)
    except (httpx.HTTPError, ValueError) as exc:
        _log(SERVICE, f"Hyperliquid live userRateLimit read failed: {type(exc).__name__}", level="WARNING")
        raise HTTPException(status_code=502, detail="Hyperliquid account limit unavailable") from exc
    sampled_at = int(time.time())
    with _hl_rate_limit_accounts_lock:
        prior = _hl_rate_limit_accounts.get(address, {})
        _hl_rate_limit_accounts[address] = {
            **prior, "users": wallets[address], "used": used, "cap": cap,
            "sampled_at": sampled_at, "state": "on_demand", "source": "on_demand",
        }
    return JSONResponse(
        content={"account": user_name, "used": used, "cap": cap, "remaining": max(0, cap - used),
                 "sampled_at": sampled_at, "source": "Hyperliquid userRateLimit"},
        headers={"Cache-Control": "no-store"},
    )


@router.post("/user-rate-limits/credits")
def purchase_hl_request_credits(
    request: HlCreditPurchaseRequest,
    session: SessionToken = Depends(require_auth),
) -> JSONResponse:
    """Reserve the exact confirmed action count using the saved agent key."""
    del session
    name = request.user_name
    if not name or name in {".", ".."} or any(char in name for char in ("/", "\\", "\x00")) or any(ord(char) < 32 for char in name):
        raise HTTPException(status_code=400, detail="Invalid account name")
    if not _hl_credit_purchase_lock.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="A Hyperliquid credit purchase is already in progress")
    try:
        from User import Users
        import ccxt

        user = Users().find_user(name)
        if user is None or str(getattr(user, "exchange", "") or "").lower() != "hyperliquid":
            raise HTTPException(status_code=404, detail="Hyperliquid account not found")
        if bool(getattr(user, "is_vault", False)):
            raise HTTPException(status_code=400, detail="Request-credit purchase is available for main accounts only")
        address = str(getattr(user, "wallet_address", "") or "").strip()
        private_key = str(getattr(user, "private_key", "") or "").strip()
        if not _HL_WALLET_PATTERN.fullmatch(address) or not private_key:
            raise HTTPException(status_code=400, detail="Saved Hyperliquid wallet address and private key are required")

        cost = _HL_CREDIT_PRICE_USDC * request.credits
        client = ccxt.hyperliquid({
            "walletAddress": address,
            "privateKey": private_key,
            "enableRateLimit": True,
            "timeout": 15000,
        })
        try:
            result = client.reserve_request_weight(request.credits)
        except ccxt.InsufficientFunds as exc:
            _log(SERVICE, f"Hyperliquid credit purchase lacked Perps balance for {name}", level="WARNING")
            raise HTTPException(status_code=409, detail="Insufficient Hyperliquid Perps USDC balance") from exc
        except ccxt.NetworkError as exc:
            _log(SERVICE, f"Hyperliquid credit purchase result uncertain for {name}: {type(exc).__name__}", level="WARNING")
            raise HTTPException(
                status_code=502,
                detail="Exchange response unavailable. Check the account limit and Perps balance before purchasing again.",
            ) from exc
        except ccxt.BaseError as exc:
            _log(SERVICE, f"Hyperliquid credit purchase rejected for {name}: {type(exc).__name__}", level="WARNING")
            raise HTTPException(status_code=502, detail="Hyperliquid rejected the credit purchase; check the account and Perps balance") from exc


        if not isinstance(result, dict) or result.get("status") != "ok" or not isinstance(result.get("response"), dict) or result["response"].get("type") != "default":
            _log(SERVICE, f"Hyperliquid credit purchase response unconfirmed for {name}", level="WARNING")
            raise HTTPException(status_code=502, detail="Exchange result unconfirmed. Check the account limit and Perps balance before purchasing again.")

        sampled_at = int(time.time())
        verified = False
        used = cap = None
        try:
            used, cap = _read_hl_rate_limit_now(address)
            verified = True
            with _hl_rate_limit_accounts_lock:
                prior = _hl_rate_limit_accounts.get(address.lower(), {})
                _hl_rate_limit_accounts[address.lower()] = {
                    **prior, "users": sorted(set((prior.get("users") or []) + [name])),
                    "used": used, "cap": cap, "sampled_at": sampled_at,
                    "state": "ok" if prior.get("bots") else "on_demand",
                    "source": "purchase" if prior.get("bots") else "on_demand",
                }
            if prior.get("bots"):
                try:
                    _append_hl_rate_limit_sample(address.lower(), sampled_at, used, cap)
                except (OSError, ValueError, RuntimeError) as exc:
                    _log(SERVICE, f"Hyperliquid post-purchase history write failed: {type(exc).__name__}", level="WARNING")
        except (httpx.HTTPError, ValueError) as exc:
            _log(SERVICE, f"Hyperliquid post-purchase limit read failed: {type(exc).__name__}", level="WARNING")

        _log(SERVICE, f"Reserved {request.credits} Hyperliquid request credits for {name}", level="INFO")
        return JSONResponse(
            content={
                "account": name, "credits": request.credits, "cost_usdc": str(cost),
                "verified": verified, "used": used, "cap": cap, "sampled_at": sampled_at,
            },
            headers={"Cache-Control": "no-store"},
        )
    except HTTPException:
        raise
    except Exception as exc:
        _log(SERVICE, f"Hyperliquid credit purchase failed for {name}: {type(exc).__name__}", level="ERROR")
        raise HTTPException(status_code=500, detail="Credit purchase unavailable") from exc
    finally:
        _hl_credit_purchase_lock.release()


@router.get("/user-rate-limits/history/{user_name}")
def get_hl_user_rate_limit_history(
    user_name: str,
    session: SessionToken = Depends(require_auth),
) -> JSONResponse:
    """Return one account's stored history without exposing its wallet address."""
    del session
    if not user_name or user_name in {".", ".."} or any(char in user_name for char in ("/", "\\", "\x00")) or any(ord(char) < 32 for char in user_name):
        raise HTTPException(status_code=400, detail="Invalid account name")
    wallets = _configured_hl_wallets()
    address = next((wallet for wallet, names in wallets.items() if user_name in names), None)
    if address is None:
        raise HTTPException(status_code=404, detail="Hyperliquid account not found")
    try:
        points = _get_hl_rate_limit_history(address)
    except (OSError, ValueError, RuntimeError) as exc:
        _log(SERVICE, f"Hyperliquid rate-limit history read failed: {type(exc).__name__}", level="WARNING")
        raise HTTPException(status_code=500, detail="Account history unavailable") from exc
    return JSONResponse(
        content={"account": user_name, "source": "Hyperliquid userRateLimit", "window_seconds": _HL_RATE_LIMIT_HISTORY_SECONDS,
                 "step_seconds": _HL_RATE_LIMIT_INTERVAL_SECONDS, "samples": points},
        headers={"Cache-Control": "no-store"},
    )


def _public_cluster_onboard_job(job: dict[str, object]) -> dict[str, object]:
    """Return one secret-free Cluster onboarding progress record."""

    return {
        key: value
        for key, value in job.items()
        if key in {"job_id", "hostname", "status", "phase", "label", "percent", "events", "result", "error", "created_at", "updated_at"}
    }


def _prune_cluster_onboard_jobs() -> None:
    """Bound retained terminal onboarding jobs by age and count."""

    cutoff = int(time.time()) - _CLUSTER_ONBOARD_JOB_TTL_SECONDS
    removable = [
        job_id
        for job_id, job in _CLUSTER_ONBOARD_JOBS.items()
        if str(job.get("status") or "") not in {"queued", "running"}
        and int(job.get("updated_at") or 0) < cutoff
    ]
    for job_id in removable:
        _CLUSTER_ONBOARD_JOBS.pop(job_id, None)
    terminal = sorted(
        (
            (int(job.get("updated_at") or 0), job_id)
            for job_id, job in _CLUSTER_ONBOARD_JOBS.items()
            if str(job.get("status") or "") not in {"queued", "running"}
        )
    )
    for _updated_at, job_id in terminal[:-50]:
        _CLUSTER_ONBOARD_JOBS.pop(job_id, None)


def _update_cluster_onboard_job(job_id: str, update: dict[str, object]) -> dict[str, object]:
    """Apply one bounded progress update to an onboarding job."""

    job = _CLUSTER_ONBOARD_JOBS.get(job_id)
    if not job:
        return {}
    phase = str(update.get("phase") or job.get("phase") or "running")
    label = str(update.get("label") or job.get("label") or "Adding VPS to Cluster...")
    previous_phase = str(job.get("phase") or "")
    job.update(update)
    job["phase"] = phase
    job["label"] = label
    job["percent"] = max(0, min(100, int(job.get("percent") or 0)))
    job["updated_at"] = int(time.time())
    if phase != previous_phase or str(update.get("status") or "") in {"successful", "error"}:
        events = list(job.get("events") or [])
        events.append({"phase": phase, "label": label, "status": str(update.get("status") or "running")})
        job["events"] = events[-30:]
    return _public_cluster_onboard_job(job)


async def _run_cluster_onboard_job(
    job_id: str,
    service: VPSManagerService,
    token: str,
    hostname: str,
) -> dict[str, object]:
    """Run one tracked Cluster onboarding operation."""

    _update_cluster_onboard_job(job_id, {
        "status": "running",
        "phase": "starting",
        "label": "Starting Cluster onboarding...",
        "percent": 1,
    })

    def progress(update: dict[str, object]) -> None:
        _update_cluster_onboard_job(job_id, {"status": "running", **update})

    try:
        result = await service.add_vps_to_cluster(token, hostname, progress_callback=progress)
    except asyncio.CancelledError:
        _update_cluster_onboard_job(job_id, {
            "status": "error",
            "phase": "interrupted",
            "label": "Cluster onboarding was interrupted by API shutdown.",
            "error": "API shutdown interrupted Cluster onboarding; retry Add to Cluster.",
        })
        raise
    except Exception as exc:
        _update_cluster_onboard_job(job_id, {
            "status": "error",
            "phase": "error",
            "label": str(exc),
            "error": str(exc),
        })
        raise
    _update_cluster_onboard_job(job_id, {
        "status": "successful",
        "phase": "complete",
        "label": "VPS joined and synchronized with Cluster.",
        "percent": 100,
        "result": result,
        "error": "",
    })
    return result


def _cluster_onboard_done(hostname: str, job_id: str, task: asyncio.Task[dict[str, object]]) -> None:
    """Release one completed onboarding task and record detached failures."""

    if _CLUSTER_ONBOARD_TASKS.get(hostname) is task:
        _CLUSTER_ONBOARD_TASKS.pop(hostname, None)
    if _CLUSTER_ONBOARD_ACTIVE.get(hostname) == job_id:
        _CLUSTER_ONBOARD_ACTIVE.pop(hostname, None)
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        _log(SERVICE, f"Cluster onboarding failed for {hostname}: {exc}", level="WARNING")


def _start_cluster_onboard(service: VPSManagerService, token: str, hostname: str) -> tuple[dict[str, object], asyncio.Task[dict[str, object]]]:
    """Create or return one active host onboarding job."""

    target = str(hostname or "").strip()
    if not target or target in {".", ".."} or any(ch in target for ch in ("/", "\\", "\x00")) or any(ord(ch) < 32 for ch in target):
        raise HTTPException(status_code=400, detail="Invalid VPS hostname")
    _prune_cluster_onboard_jobs()
    task = _CLUSTER_ONBOARD_TASKS.get(target)
    active_job_id = _CLUSTER_ONBOARD_ACTIVE.get(target, "")
    if task is not None and not task.done() and active_job_id in _CLUSTER_ONBOARD_JOBS:
        return _public_cluster_onboard_job(_CLUSTER_ONBOARD_JOBS[active_job_id]), task

    now = int(time.time())
    job_id = uuid.uuid4().hex
    job: dict[str, object] = {
        "job_id": job_id,
        "hostname": target,
        "status": "queued",
        "phase": "queued",
        "label": "Queued Cluster onboarding...",
        "percent": 0,
        "events": [],
        "result": None,
        "error": "",
        "created_at": now,
        "updated_at": now,
    }
    _CLUSTER_ONBOARD_JOBS[job_id] = job
    _CLUSTER_ONBOARD_ACTIVE[target] = job_id
    task = asyncio.create_task(_run_cluster_onboard_job(job_id, service, token, target))
    _CLUSTER_ONBOARD_TASKS[target] = task
    task.add_done_callback(
        lambda finished, host=target, tracked_job_id=job_id: _cluster_onboard_done(host, tracked_job_id, finished)
    )
    return _public_cluster_onboard_job(job), task


async def _await_cluster_onboard(service: VPSManagerService, token: str, hostname: str) -> dict[str, object]:
    """Await one host onboarding task independently of its requesting WebSocket."""

    _job, task = _start_cluster_onboard(service, token, hostname)
    return await asyncio.shield(task)


@router.get("/main_page", response_class=HTMLResponse)
def get_main_page(
    request: Request,
    session: SessionToken = Depends(require_auth),
) -> HTMLResponse:
    del session
    html_path = Path(__file__).resolve().parent.parent / "frontend" / "vps_manager.html"
    html = html_path.read_text(encoding="utf-8")

    scheme = request.url.scheme
    host = request.url.hostname or "127.0.0.1"
    port = request.url.port
    origin = f"{scheme}://{host}" + (f":{port}" if port else "")
    api_base = origin + "/api/vps-manager"
    ws_base = origin.replace("http://", "ws://").replace("https://", "wss://")

    html = html.replace('"%%API_BASE%%"', json.dumps(api_base))
    html = html.replace('"%%WS_BASE%%"', json.dumps(ws_base))

    from pbgui_purefunc import PBGUI_VERSION
    from pbgui_purefunc import PBGUI_SERIAL

    html = html.replace('"%%VERSION%%"', json.dumps(PBGUI_VERSION))
    html = html.replace("%%VERSION%%", PBGUI_VERSION)
    html = html.replace('"%%SERIAL%%"', json.dumps(PBGUI_SERIAL))
    html = html.replace("%%SERIAL%%", PBGUI_SERIAL)

    nav_js = Path(__file__).resolve().parent.parent / "frontend" / "pbgui_nav.js"
    nav_hash = str(int(nav_js.stat().st_mtime)) if nav_js.exists() else PBGUI_VERSION
    html = html.replace("%%NAV_HASH%%", nav_hash)

    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})


@router.get("/hyperliquid-limits/main_page", response_class=HTMLResponse)
def get_hl_limits_page(
    request: Request,
    session: SessionToken = Depends(require_auth),
) -> HTMLResponse:
    """Serve the central account-limit overview."""
    del session
    html_path = Path(__file__).resolve().parent.parent / "frontend" / "hl_limits.html"
    html = html_path.read_text(encoding="utf-8")
    from pbgui_purefunc import PBGUI_VERSION, PBGUI_SERIAL

    nav_js = html_path.parent / "pbgui_nav.js"
    nav_hash = str(int(nav_js.stat().st_mtime)) if nav_js.exists() else PBGUI_VERSION
    html = html.replace("%%NAV_HASH%%", nav_hash)
    html = render_page_urls(request, html, "/api/vps-manager")
    html = html.replace('"%%VERSION%%"', script_json(PBGUI_VERSION))
    html = html.replace('"%%SERIAL%%"', script_json(PBGUI_SERIAL))
    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})


@router.get("/detail/{hostname}")
def get_vps_detail(
    hostname: str,
    session: SessionToken = Depends(require_auth),
) -> JSONResponse:
    try:
        detail = _get_service().build_vps_detail(session.token, hostname)
        return JSONResponse(content=detail)
    except Exception as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.get("/detail-master")
def get_master_detail(
    session: SessionToken = Depends(require_auth),
) -> JSONResponse:
    try:
        detail = _get_service().build_master_detail()
        return JSONResponse(content=detail)
    except Exception as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.get("/cpu-history/{hostname}")
def get_cpu_history(
    hostname: str,
    bot_name: str = Query(default="", description="Optional bot name for bot CPU history"),
    session: SessionToken = Depends(require_auth),
) -> JSONResponse:
    try:
        payload = _get_service().get_cpu_history(hostname, bot_name=bot_name)
        return JSONResponse(content=payload)
    except Exception as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.get("/metric-history/{hostname}")
async def get_metric_history(
    hostname: str,
    metric: str = Query(default="cpu", description="Metric key: cpu, memory, disk, swap"),
    bot_name: str = Query(default="", description="Optional bot name for bot CPU history"),
    session: SessionToken = Depends(require_auth),
) -> JSONResponse:
    try:
        payload = _get_service().get_metric_history(hostname, bot_name=bot_name, metric=metric)
        return JSONResponse(content=payload)
    except Exception as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.get("/import/resolve-host")
def resolve_existing_vps_import_host(
    hostname: str = Query(default="", description="Hostname to resolve from local /etc/hosts"),
    session: SessionToken = Depends(require_auth),
) -> JSONResponse:
    del session
    try:
        data = _get_service().resolve_existing_vps_import_host(hostname)
        return JSONResponse(content=data)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.post("/import/probe")
def probe_existing_vps_import(
    payload: ExistingVpsImportRequest,
    session: SessionToken = Depends(require_auth),
) -> JSONResponse:
    del session
    try:
        data = _get_service().probe_existing_vps_import(payload.dict())
        return JSONResponse(content=data)
    except Exception as exc:
        _log(SERVICE, f"existing VPS import probe failed: {exc}", level="WARNING", meta={"traceback": traceback.format_exc()})
        raise HTTPException(status_code=400, detail=str(exc))


@router.post("/import/save")
def save_existing_vps_import(
    payload: ExistingVpsImportRequest,
    session: SessionToken = Depends(require_auth),
) -> JSONResponse:
    try:
        data = _get_service().save_existing_vps_import(session.token, payload.dict())
        return JSONResponse(content=data)
    except Exception as exc:
        _log(SERVICE, f"existing VPS import save failed: {exc}", level="WARNING", meta={"traceback": traceback.format_exc()})
        raise HTTPException(status_code=400, detail=str(exc))


@router.get("/cluster-import/preview")
def preview_cluster_nodes_import(
    session: SessionToken = Depends(require_auth),
) -> JSONResponse:
    del session
    try:
        data = _get_service().preview_cluster_nodes_import()
        return JSONResponse(content=data)
    except Exception as exc:
        _log(SERVICE, f"Cluster node import preview failed: {exc}", level="WARNING", meta={"traceback": traceback.format_exc()})
        raise HTTPException(status_code=400, detail=str(exc))


@router.post("/cluster-import/apply")
def apply_cluster_nodes_import(
    payload: ClusterNodesImportRequest | None = None,
    session: SessionToken = Depends(require_auth),
) -> JSONResponse:
    try:
        data = _get_service().start_cluster_nodes_import(session.token, payload.dict() if payload else {})
        return JSONResponse(content=data)
    except Exception as exc:
        _log(SERVICE, f"Cluster node import apply failed: {exc}", level="WARNING", meta={"traceback": traceback.format_exc()})
        raise HTTPException(status_code=400, detail=str(exc))


@router.get("/cluster-import/progress/{job_id}")
def get_cluster_nodes_import_progress(
    job_id: str,
    session: SessionToken = Depends(require_auth),
) -> JSONResponse:
    del session
    try:
        data = _get_service().get_cluster_nodes_import_progress(job_id)
        return JSONResponse(content=data)
    except Exception as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.post("/cluster-onboard/{hostname}/start")
async def start_cluster_onboard(
    hostname: str,
    session: SessionToken = Depends(require_auth),
) -> JSONResponse:
    """Start or resume one tracked VPS Cluster onboarding job."""

    job, _task = _start_cluster_onboard(_get_service(), session.token, hostname)
    return JSONResponse(content=job)


@router.get("/cluster-onboard/jobs/{job_id}")
def get_cluster_onboard_job(
    job_id: str,
    session: SessionToken = Depends(require_auth),
) -> JSONResponse:
    """Return one tracked VPS Cluster onboarding job."""

    del session
    _prune_cluster_onboard_jobs()
    job = _CLUSTER_ONBOARD_JOBS.get(str(job_id or "").strip())
    if not job:
        raise HTTPException(status_code=404, detail="Cluster onboarding job not found")
    return JSONResponse(content=_public_cluster_onboard_job(job))


@router.get("/cluster-onboard/{hostname}/active")
def get_active_cluster_onboard_job(
    hostname: str,
    session: SessionToken = Depends(require_auth),
) -> JSONResponse:
    """Return the active onboarding job for one VPS hostname, if any."""

    del session
    target = str(hostname or "").strip()
    job_id = _CLUSTER_ONBOARD_ACTIVE.get(target, "")
    job = _CLUSTER_ONBOARD_JOBS.get(job_id)
    if not job or str(job.get("status") or "") not in {"queued", "running"}:
        return JSONResponse(content={"active": False, "hostname": target})
    return JSONResponse(content={"active": True, **_public_cluster_onboard_job(job)})


@router.websocket("/ws")
async def ws_vps_manager(websocket: WebSocket):
    session = await authenticate_websocket(websocket)
    if session is None:
        return

    token = session.token
    service = _get_service()
    context: dict[str, Any] = {"view": "overview", "hostname": "", "token": token, "generation": 0}
    push_task = asyncio.create_task(_push_loop(websocket, service, context), name="vps-manager-push")
    try:
        async for raw in websocket.iter_text():
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                await websocket.send_json({"type": "error", "error": "Invalid JSON"})
                continue

            cmd = str(msg.get("cmd") or "").strip()
            try:
                if cmd == "set_context":
                    context["view"] = str(msg.get("view") or "overview")
                    context["hostname"] = str(msg.get("hostname") or "")
                    context["generation"] = max(int(msg.get("context_generation") or 0), 0)
                    _log(SERVICE, f"set_context view={context['view']} hostname={context['hostname']}", level="INFO")
                    await _send_current_context_detail(websocket, service, context)
                elif cmd == "refresh":
                    await asyncio.to_thread(service.refresh, force=True)
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True})
                elif cmd == "check_package_status":
                    data = await asyncio.to_thread(
                        service.check_vps_package_status,
                        msg.get("hostnames") or [],
                    )
                    await websocket.send_json({
                        "type": "result",
                        "cmd": cmd,
                        "request_id": str(msg.get("request_id") or ""),
                        "success": True,
                        "data": data,
                    })
                elif cmd == "save_vps":
                    data = await asyncio.to_thread(service.save_vps, token, msg.get("form") or {})
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True, "data": data})
                elif cmd == "prepare_import":
                    data = await asyncio.to_thread(service.prepare_import, msg.get("hostname") or "")
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True, "data": data})
                elif cmd == "save_vps_config":
                    data = await asyncio.to_thread(
                        service.save_vps_config,
                        token,
                        str(msg.get("hostname") or ""),
                        msg.get("form") or {},
                    )
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True, "data": data})
                elif cmd == "probe_vps_host_key":
                    data = await asyncio.to_thread(
                        service.probe_vps_host_key,
                        str(msg.get("hostname") or ""),
                    )
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True, "data": data})
                elif cmd == "trust_vps_host_key":
                    data = await asyncio.to_thread(
                        service.trust_vps_host_key,
                        str(msg.get("hostname") or ""),
                        str(msg.get("expected_fingerprint") or ""),
                        replace_existing=bool(msg.get("replace_existing")),
                    )
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True, "data": data})
                elif cmd == "save_vps_logging_config":
                    data = await asyncio.to_thread(service.save_vps_logging_config, msg.get("data") or {})
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True, "data": data})
                elif cmd == "save_vps_deploy_settings":
                    data = await asyncio.to_thread(service.save_vps_deploy_settings, msg.get("data") or {})
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True, "data": data})
                elif cmd == "init_vps":
                    data = await asyncio.to_thread(service.init_vps, token, msg.get("form") or {}, debug=bool(msg.get("debug")))
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True, "data": data})
                elif cmd == "detect_public_ip":
                    data = await asyncio.to_thread(service.detect_public_ip)
                    await websocket.send_json({"type": "public_ip_result", "data": data})
                elif cmd == "setup_vps":
                    data = await asyncio.to_thread(
                        service.setup_vps,
                        token,
                        str(msg.get("hostname") or ""),
                        msg.get("form") or {},
                        debug=bool(msg.get("debug")),
                    )
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True, "data": data})
                elif cmd == "add_vps_to_cluster":
                    data = await _await_cluster_onboard(
                        service,
                        token,
                        str(msg.get("hostname") or ""),
                    )
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True, "data": data})
                elif cmd == "preview_vps_systemd_migration":
                    data = await asyncio.to_thread(
                        service.preview_vps_systemd_migration,
                        token,
                        str(msg.get("hostname") or ""),
                        msg.get("form") or {},
                    )
                    await websocket.send_json({"type": "vps_systemd_migration_preview", "cmd": cmd, "success": True, "data": data})
                elif cmd == "delete_vps":
                    await asyncio.to_thread(service.delete_vps, str(msg.get("hostname") or ""))
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True})
                elif cmd == "read_vps_settings":
                    hostname = str(msg.get("hostname") or "")
                    loop = asyncio.get_running_loop()

                    def send_read_progress(step: str, label: str, status: str = "running") -> None:
                        payload = {
                            "type": "vps_read_settings_progress",
                            "cmd": cmd,
                            "hostname": hostname,
                            "step": step,
                            "label": label,
                            "status": status,
                        }
                        future = asyncio.run_coroutine_threadsafe(websocket.send_json(payload), loop)
                        try:
                            future.result(timeout=3)
                        except Exception:
                            pass

                    data = await asyncio.to_thread(
                        service.read_vps_settings,
                        token,
                        hostname,
                        msg.get("form") or {},
                        send_read_progress,
                    )
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True, "data": data})
                elif cmd == "reveal_secret":
                    data = await asyncio.to_thread(
                        service.reveal_session_secret,
                        token,
                        str(msg.get("hostname") or ""),
                        str(msg.get("field") or ""),
                    )
                    await websocket.send_json({"type": "secret_value", "cmd": cmd, "success": True, "data": data})
                elif cmd == "fetch_vps_log":
                    data = await asyncio.to_thread(
                        service.fetch_vps_log,
                        str(msg.get("hostname") or ""),
                        filename=str(msg.get("filename") or ""),
                        size_kb=int(msg.get("size_kb") or 50),
                        reverse=bool(msg.get("reverse", True)),
                        debug=bool(msg.get("debug")),
                    )
                    await websocket.send_json({"type": "log_preview", "data": data, "hostname": str(msg.get("hostname") or "")})
                elif cmd == "load_more_commits":
                    await asyncio.to_thread(service.load_more_commits, str(msg.get("repo") or ""), str(msg.get("branch") or ""), int(msg.get("limit") or 50))
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True})
                elif cmd == "load_remote_branches":
                    branches = await asyncio.to_thread(service.load_remote_branches, str(msg.get("remote_url") or ""))
                    await websocket.send_json({
                        "type": "remote_branches",
                        "request_id": str(msg.get("request_id") or ""),
                        "remote_url": str(msg.get("remote_url") or ""),
                        "branches": branches,
                    })
                elif cmd == "load_remote_branch_commits":
                    remote_url = str(msg.get("remote_url") or "")
                    branch_name = str(msg.get("branch") or "")
                    limit = int(msg.get("limit") or 50)
                    commits = await asyncio.to_thread(service.load_remote_branch_commits, remote_url, branch_name, limit)
                    await websocket.send_json({
                        "type": "remote_branch_commits",
                        "request_id": str(msg.get("request_id") or ""),
                        "remote_url": remote_url,
                        "branch": branch_name,
                        "limit": limit,
                        "commits": commits,
                    })
                elif cmd == "run_master_command":
                    await asyncio.to_thread(
                        service.run_master_command,
                        command=str(msg.get("command") or ""),
                        command_text=str(msg.get("command_text") or ""),
                        debug=bool(msg.get("debug")),
                        sudo_pw=str(msg.get("sudo_pw") or "") or None,
                        extra_vars=msg.get("extra_vars") or None,
                    )
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True})
                elif cmd == "run_vps_command":
                    data = await asyncio.to_thread(
                        service.run_vps_command,
                        token=token,
                        hostname=str(msg.get("hostname") or ""),
                        command=str(msg.get("command") or ""),
                        command_text=str(msg.get("command_text") or ""),
                        debug=bool(msg.get("debug")),
                        extra_vars=msg.get("extra_vars") or None,
                    )
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True, "data": data})
                elif cmd == "deploy_vps_logging":
                    data = await asyncio.to_thread(
                        service.deploy_vps_logging,
                        token,
                        msg.get("hostnames") or [],
                        debug=bool(msg.get("debug")),
                    )
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True, "data": data})
                elif cmd == "run_vps_deploy":
                    data = await asyncio.to_thread(
                        service.run_vps_deploy,
                        token,
                        msg.get("hostnames") or [],
                        command=str(msg.get("command") or ""),
                        mode=str(msg.get("mode") or ""),
                        debug=bool(msg.get("debug")),
                        extra_vars=msg.get("extra_vars") or None,
                    )
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True, "data": data})
                elif cmd == "validate_and_stage_vps_deploy_host":
                    try:
                        data = await asyncio.to_thread(
                            service.validate_and_stage_vps_deploy_host,
                            token,
                            hostnames=msg.get("hostnames") or [],
                            hostname=str(msg.get("hostname") or ""),
                            password=str(msg.get("password") or ""),
                            command=str(msg.get("command") or ""),
                            mode=str(msg.get("mode") or ""),
                            debug=bool(msg.get("debug")),
                            extra_vars=msg.get("extra_vars") or None,
                            entry_id=str(msg.get("entry_id") or "") or None,
                            accept_unknown_host=bool(msg.get("accept_unknown_host")),
                            accepted_host_key_fingerprint=str(msg.get("accepted_host_key_fingerprint") or ""),
                        )
                        await websocket.send_json({"type": "result", "cmd": cmd, "success": True, "data": data})
                    except UnknownHostKeyError as exc:
                        await websocket.send_json({
                            "type": "confirm_unknown_host_key",
                            "cmd": cmd,
                            "hostname": exc.hostname,
                            "ssh_host": exc.ssh_host,
                            "ip": exc.ip,
                            "key_type": exc.key_type,
                            "fingerprint": exc.fingerprint,
                            "error": str(exc),
                        })
                elif cmd == "finalize_vps_deploy_session":
                    data = await asyncio.to_thread(service.finalize_vps_deploy_session, str(msg.get("entry_id") or ""))
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True, "data": data})
                elif cmd == "fetch_bot_log_matches":
                    bucket = str(msg.get("bucket") or "").strip()
                    if bucket not in {"today", "yesterday"}:
                        await websocket.send_json({"type": "error", "error": "bucket must be today or yesterday", "cmd": cmd})
                        continue
                    lines = await get_bot_log_matches(
                        str(msg.get("hostname") or ""),
                        str(msg.get("bot_name") or ""),
                        pb_version=str(msg.get("pb_version") or "") or None,
                        kind=str(msg.get("kind") or "tracebacks"),
                        bucket=bucket,
                        expected_count=int(msg.get("expected_count")) if msg.get("expected_count") is not None else None,
                        lines=int(msg.get("lines") or 5000),
                    )
                    await websocket.send_json({
                        "type": "bot_log_matches",
                        "request_id": str(msg.get("request_id") or ""),
                        "hostname": str(msg.get("hostname") or ""),
                        "bot_name": str(msg.get("bot_name") or ""),
                        "kind": str(msg.get("kind") or "tracebacks"),
                        "bucket": bucket,
                        "expected_count": int(msg.get("expected_count")) if msg.get("expected_count") is not None else None,
                        "lines": lines,
                    })
                elif cmd == "get_cpu_history":
                    data = await asyncio.to_thread(
                        service.get_cpu_history,
                        str(msg.get("hostname") or ""),
                        bot_name=str(msg.get("bot_name") or ""),
                    )
                    await websocket.send_json({
                        "type": "cpu_history",
                        "cmd": cmd,
                        "success": True,
                        "hostname": str(msg.get("hostname") or ""),
                        "bot_name": str(msg.get("bot_name") or ""),
                        "data": data,
                    })
                elif cmd == "browse_files":
                    path = str(msg.get("path") or "")
                    data = await asyncio.to_thread(service.browse_files, path)
                    await websocket.send_json({"type": "browse_result", "data": data})
                elif cmd == "check_vps_ready":
                    data = await asyncio.to_thread(service.check_vps_ready, dict(msg.get("form") or {}))
                    await websocket.send_json({"type": "vps_ready_result", "data": data})
                elif cmd == "write_hosts_entry":
                    data = await asyncio.to_thread(
                        service.write_hosts_entry,
                        str(msg.get("ip") or ""),
                        str(msg.get("hostname") or ""),
                        str(msg.get("sudo_pw") or ""),
                    )
                    await websocket.send_json({"type": "write_hosts_result", "data": data})
                elif cmd == "read_ufw_rules":
                    data = await asyncio.to_thread(
                        service.read_ufw_rules,
                        str(msg.get("hostname") or ""),
                        str(msg.get("sudo_pw") or "") or None,
                    )
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True, "data": data})
                elif cmd == "preview_ufw_rules":
                    data = await asyncio.to_thread(
                        service.preview_ufw_rules,
                        str(msg.get("hostname") or ""),
                        msg.get("payload") or {},
                        str(msg.get("sudo_pw") or "") or None,
                    )
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True, "data": data})
                elif cmd == "apply_ufw_rules":
                    data = await asyncio.to_thread(
                        service.apply_ufw_rules,
                        str(msg.get("hostname") or ""),
                        msg.get("payload") or {},
                        str(msg.get("sudo_pw") or "") or None,
                    )
                    await websocket.send_json({"type": "result", "cmd": cmd, "success": True, "data": data})
                elif cmd == "validate_local_sudo_password":
                    data = await asyncio.to_thread(
                        service.validate_local_sudo_password,
                        str(msg.get("sudo_pw") or ""),
                    )
                    await websocket.send_json({"type": "local_sudo_validation_result", "data": data})
                elif cmd == "get_metric_history":
                    data = await asyncio.to_thread(
                        service.get_metric_history,
                        str(msg.get("hostname") or ""),
                        bot_name=str(msg.get("bot_name") or ""),
                        metric=str(msg.get("metric") or "cpu"),
                    )
                    await websocket.send_json({
                        "type": "metric_history",
                        "cmd": cmd,
                        "success": True,
                        "hostname": str(msg.get("hostname") or ""),
                        "bot_name": str(msg.get("bot_name") or ""),
                        "metric": str(msg.get("metric") or "cpu"),
                        "data": data,
                    })
                else:
                    await websocket.send_json({"type": "error", "error": f"Unknown command: {cmd}"})
            except Exception as exc:
                _log(SERVICE, f"command {cmd} failed: {exc}", level="WARNING", meta={"traceback": traceback.format_exc()})
                if cmd == "read_vps_settings":
                    await websocket.send_json({
                        "type": "vps_read_settings_progress",
                        "cmd": cmd,
                        "hostname": str(msg.get("hostname") or ""),
                        "step": "error",
                        "label": str(exc),
                        "status": "error",
                    })
                await websocket.send_json({
                    "type": "error",
                    "error": str(exc),
                    "cmd": cmd,
                    "request_id": str(msg.get("request_id") or ""),
                })
    except WebSocketDisconnect:
        pass
    finally:
        push_task.cancel()
        try:
            await push_task
        except asyncio.CancelledError:
            pass


async def _push_loop(websocket: WebSocket, service: VPSManagerService, context: dict[str, Any]) -> None:
    last_state = ""
    last_detail = ""
    try:
        while True:
            context_snapshot = dict(context)
            detail = await asyncio.to_thread(_build_quick_detail_for_context, service, context_snapshot)
            if detail is not None:
                encoded_detail = json.dumps(detail, sort_keys=True, default=str)
                if encoded_detail != last_detail and context_snapshot.get("generation") == context.get("generation"):
                    await websocket.send_json({
                        "type": "detail",
                        "data": detail,
                        "context_generation": int(context_snapshot.get("generation") or 0),
                    })
                    last_detail = encoded_detail
            else:
                last_detail = ""

            state = await asyncio.to_thread(service.build_state)
            encoded_state = json.dumps(state, sort_keys=True, default=str)
            if encoded_state != last_state:
                await websocket.send_json({"type": "state", "data": state})
                last_state = encoded_state

            await asyncio.to_thread(service.refresh, force=False)

            await asyncio.sleep(1)
    except asyncio.CancelledError:
        pass
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        _log(SERVICE, f"push loop failed: {exc}", level="WARNING", meta={"traceback": traceback.format_exc()})


def _build_detail_for_context(service: VPSManagerService, context: dict[str, Any]) -> DetailPayload | None:
    view = str(context.get("view") or "overview")
    if view in MASTER_CONTEXT_VIEWS:
        return service.build_master_detail()
    if view in VPS_CONTEXT_VIEWS:
        hostname = str(context.get("hostname") or "")
        if hostname:
            try:
                return service.build_vps_detail(str(context.get("token") or ""), hostname)
            except ValueError as exc:
                if str(exc).startswith("Unknown VPS:"):
                    context["view"] = "overview"
                    context["hostname"] = ""
                    return None
                raise
    return None


def _build_quick_detail_for_context(service: VPSManagerService, context: dict[str, Any]) -> DetailPayload | None:
    view = str(context.get("view") or "overview")
    if view in MASTER_CONTEXT_VIEWS:
        return service.build_master_detail_quick()
    if view in VPS_CONTEXT_VIEWS:
        hostname = str(context.get("hostname") or "")
        if hostname:
            try:
                return service.build_vps_detail(str(context.get("token") or ""), hostname, quick=True)
            except ValueError as exc:
                if str(exc).startswith("Unknown VPS:"):
                    context["view"] = "overview"
                    context["hostname"] = ""
                    return None
                raise
    return None


async def _send_current_context_detail(websocket: WebSocket, service: VPSManagerService, context: dict[str, Any]) -> None:
    context_snapshot = dict(context)
    detail = await asyncio.to_thread(_build_quick_detail_for_context, service, context_snapshot)
    if detail is not None and context_snapshot.get("generation") == context.get("generation"):
        try:
            await websocket.send_json({
                "type": "detail",
                "data": detail,
                "context_generation": int(context_snapshot.get("generation") or 0),
            })
            _log(SERVICE, f"detail sent for {context_snapshot.get('view')}/{context_snapshot.get('hostname')}", level="INFO")
        except Exception:
            _log(SERVICE, f"detail send failed for {context.get('view')}/{context.get('hostname')}", level="WARNING")
    else:
        _log(SERVICE, f"detail is None for {context.get('view')}/{context.get('hostname')}", level="INFO")
