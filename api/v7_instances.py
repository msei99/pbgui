"""
FastAPI router for v7 instance list + SSH activate.

Endpoints:
    GET    /instances                   → list all v7 instances with sync status
    POST   /activate/{name}             → SSH-push config + activate_cmd to all VPS
    POST   /activate-all                → SSH-push all instances that need activation
    DELETE /instances/{name}             → backup + delete instance locally + on VPS
    GET    /backups                      → list all instance backups
    POST   /restore/{name}/{timestamp}   → restore instance from backup + SSH activate
    DELETE /backups/{name}/{timestamp}    → delete a specific backup
    GET    /main_page                    → serve the standalone HTML page
"""

from __future__ import annotations

import asyncio
import configparser
import json
import platform
import shutil
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

from api.auth import SessionToken, require_auth, validate_token
from logging_helpers import human_log as _log
from master.async_pool import SFTP_RETRY_ATTEMPTS, SFTP_RETRY_DELAY, _is_transient_error
from pbgui_purefunc import (PBGDIR, STATUS_V7_FILE, SYNC_EXCLUDE_FILES,
                             update_status_v7 as _update_status_v7,
                             get_syncable_files as _get_syncable_files)

SERVICE = "V7Instances"

router = APIRouter()

# Remote pbgui data dir (relative to home on VPS)
REMOTE_PBGUI_DIR = "software/pbgui"



# ── Injected at startup ─────────────────────────────────────

_monitor = None  # VPSMonitor
_v7_sync = None  # V7ConfigSyncWorker


def init(monitor, v7_sync=None):
    """Called by PBApiServer lifespan to inject shared objects."""
    global _monitor, _v7_sync
    _monitor = monitor
    _v7_sync = v7_sync


# ── Helpers ──────────────────────────────────────────────────

def _get_master_hostname() -> str:
    """Get the hostname of this master (from pbgui.ini or platform.node())."""
    pb_config = configparser.ConfigParser()
    pb_config.read(Path(PBGDIR) / "pbgui.ini")
    if pb_config.has_option("main", "pbname"):
        return pb_config.get("main", "pbname")
    return platform.node()


def _load_local_running_v7() -> dict[str, dict]:
    """Read PBRun's status_v7.json for locally running instances.

    Returns: {name: {running: bool, rv: int, cv: int, eo: str}}
    """
    status_file = Path(PBGDIR) / "data" / "cmd" / "status_v7.json"
    if not status_file.is_file():
        return {}
    try:
        with open(status_file, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}

    result = {}
    for name, info in data.get("instances", {}).items():
        if not info.get("running"):
            continue
        # Read running_version.txt for actual running version
        rv_file = Path(PBGDIR) / "data" / "run_v7" / name / "running_version.txt"
        rv = 0
        if rv_file.is_file():
            try:
                rv = int(rv_file.read_text().strip())
            except (ValueError, OSError):
                pass
        result[name] = {
            "running": True,
            "rv": rv,
            "cv": info.get("version", 0),
            "eo": info.get("enabled_on", ""),
        }
    return result


def _load_local_instances() -> list[dict]:
    """Read all v7 instance configs from local disk."""
    run_dir = Path(f"{PBGDIR}/data/run_v7")
    if not run_dir.is_dir():
        return []
    instances = []
    for d in sorted(run_dir.iterdir()):
        cfg_file = d / "config.json"
        if not cfg_file.is_file():
            continue
        try:
            with open(cfg_file, "r", encoding="utf-8") as f:
                cfg = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue
        live = cfg.get("live", {})
        pbgui = cfg.get("pbgui", {})
        bot = cfg.get("bot", {})
        long_cfg = bot.get("long", {})
        short_cfg = bot.get("short", {})

        # TWE string
        l_twe = round(long_cfg.get("total_wallet_exposure_limit", 0), 2)
        l_n = long_cfg.get("n_positions", 0)
        s_twe = round(short_cfg.get("total_wallet_exposure_limit", 0), 2)
        s_n = short_cfg.get("n_positions", 0)
        parts = []
        if l_n > 0:
            parts.append(f"L={l_twe}")
        if s_n > 0:
            parts.append(f"S={s_twe}")
        twe_str = " | ".join(parts)

        instances.append({
            "name": d.name,
            "user": live.get("user", d.name),
            "enabled_on": pbgui.get("enabled_on", "disabled"),
            "version": pbgui.get("version", 0),
            "note": pbgui.get("note", ""),
            "twe": twe_str,
        })
    return instances


def _enrich_with_vps_data(instances: list[dict]) -> list[dict]:
    """Merge VPSMonitor v7_instances data + local PBRun status."""
    store = _monitor.store if _monitor else None
    v7_data = store.v7_instances if store else {}  # {hostname: [{name, running, cv, rv, eo}, ...]}
    # If no VPS host has reported yet, we're still in initial collection phase
    any_vps_data = bool(v7_data)

    # Build lookup: name → best match across all hosts
    # "best" = the host where enabled_on matches, or any running host
    # has_data: True if we received collect data from at least one VPS host
    # for this instance (even if running=False). Used to distinguish
    # "confirmed not running" from "no data yet" — the latter must not
    # show "disabled" when the bot might still be running.
    vps_info = {}  # name → {running_on: [...], rv, cv_remote, has_data}
    for host, items in v7_data.items():
        for item in items:
            name = item.get("name", "")
            if name not in vps_info:
                vps_info[name] = {"running_on": [], "rv": 0, "cv_remote": 0, "has_data": False}
            vps_info[name]["has_data"] = True
            if item.get("running"):
                vps_info[name]["running_on"].append(host)
                vps_info[name]["rv"] = item.get("rv", 0)
            vps_info[name]["cv_remote"] = max(
                vps_info[name]["cv_remote"], item.get("cv", 0)
            )

    # Include locally running instances (PBRun on this master)
    master_host = _get_master_hostname()
    local_running = _load_local_running_v7()
    for name, info in local_running.items():
        if name not in vps_info:
            vps_info[name] = {"running_on": [], "rv": 0, "cv_remote": 0, "has_data": False}
        vps_info[name]["has_data"] = True
        if master_host not in vps_info[name]["running_on"]:
            vps_info[name]["running_on"].append(master_host)
        vps_info[name]["rv"] = max(vps_info[name]["rv"], info["rv"])
        vps_info[name]["cv_remote"] = max(
            vps_info[name]["cv_remote"], info["cv"]
        )

    for inst in instances:
        name = inst["name"]
        info = vps_info.get(name)
        if info:
            inst["running_on"] = info["running_on"]
            inst["running_version"] = info["rv"]
            inst["config_version_remote"] = info["cv_remote"]
        else:
            inst["running_on"] = []
            inst["running_version"] = 0
            inst["config_version_remote"] = 0

        # Compute sync status
        enabled = inst["enabled_on"]
        running_on = inst["running_on"]
        version = inst["version"]
        rv = inst["running_version"]
        has_data = info.get("has_data", False) if info else False

        if enabled == "disabled":
            if running_on:
                inst["status"] = "stop_needed"
            else:
                inst["status"] = "disabled"
        elif not any_vps_data and not running_on:
            # No VPS host has reported yet (server just restarted) — don't guess
            inst["status"] = "collecting"
        elif enabled in running_on and version == rv:
            inst["status"] = "synced"
        elif running_on:
            inst["status"] = "outdated"
        else:
            inst["status"] = "activate_needed"

    return instances


# ── SSH Sync ─────────────────────────────────────────────────

async def _ssh_sync_instance(name: str) -> dict:
    """Update status_v7.json + push config files via SFTP to all VPS.

    This replaces the old activate_*.cmd mechanism. PBRun on VPS polls
    status_v7.json for mtime changes and rescans accordingly.
    """
    config_path = Path(f"{PBGDIR}/data/run_v7/{name}/config.json")
    if not config_path.is_file():
        return {"name": name, "error": f"Config not found: {name}"}

    # 1) Update local status_v7.json (bumps per-instance activate_ts)
    _update_status_v7(name)

    # 2) Gather all syncable config files for this instance
    sync_files = _get_syncable_files(name)
    if not sync_files:
        return {"name": name, "error": "No config files to sync"}

    # 3) Read status_v7.json content for pushing
    status_content = STATUS_V7_FILE.read_bytes() if STATUS_V7_FILE.is_file() else None

    if not _monitor or not _monitor.pool:
        return {"name": name, "local": True,
                "hosts": {}, "ok": 0, "failed": 0}

    pool = _monitor.pool
    connected = pool.connected_hosts()
    if not connected:
        return {"name": name, "local": True,
                "hosts": {}, "ok": 0, "failed": 0}

    results = {}

    async def push_to_host(hostname: str):
        for attempt in range(1, SFTP_RETRY_ATTEMPTS + 1):
            sftp = await pool._open_sftp(hostname)
            if not sftp:
                return {"success": False, "error": "SFTP failed"}

            try:
                remote_inst_dir = f"{REMOTE_PBGUI_DIR}/data/run_v7/{name}"
                remote_cmd_dir = f"{REMOTE_PBGUI_DIR}/data/cmd"

                # Ensure dirs exist
                try:
                    await sftp.makedirs(remote_inst_dir, exist_ok=True)
                except Exception:
                    pass
                try:
                    await sftp.makedirs(remote_cmd_dir, exist_ok=True)
                except Exception:
                    pass

                # Write all config files
                for filename, content in sync_files:
                    async with sftp.open(
                            f"{remote_inst_dir}/{filename}", "wb") as f:
                        await f.write(content)

                # Write status_v7.json
                if status_content:
                    async with sftp.open(
                            f"{remote_cmd_dir}/status_v7.json", "wb") as f:
                        await f.write(status_content)

                return {"success": True}
            except Exception as e:
                if attempt < SFTP_RETRY_ATTEMPTS and _is_transient_error(e):
                    _log(SERVICE, f"SSH sync {name} → {hostname} "
                         f"failed (attempt {attempt}): {e} — retrying",
                         level="WARNING")
                    await asyncio.sleep(SFTP_RETRY_DELAY)
                    continue
                _log(SERVICE, f"SSH sync {name} → {hostname} failed: {e}",
                     level="ERROR", meta={"traceback": traceback.format_exc()})
                return {"success": False, "error": str(e)}
            finally:
                sftp.exit()
        return {"success": False, "error": "All retry attempts failed"}

    # Push to all connected hosts in parallel
    tasks = {h: push_to_host(h) for h in connected}
    raw = await asyncio.gather(*tasks.values(), return_exceptions=True)
    for hostname, result in zip(tasks.keys(), raw):
        if isinstance(result, Exception):
            results[hostname] = {"success": False, "error": str(result)}
        else:
            results[hostname] = result

    ok = sum(1 for r in results.values() if r.get("success"))
    fail = len(results) - ok
    _log(SERVICE, f"SSH sync '{name}': {ok}/{len(results)} hosts OK"
         + (f", {fail} failed" if fail else ""), level="INFO")

    # Schedule a delayed collect on the enabled_on host as fallback
    if ok > 0 and _monitor:
        try:
            cfg = json.loads(config_path.read_bytes())
            enabled_on = cfg.get("pbgui", {}).get("enabled_on", "")
        except (json.JSONDecodeError, ValueError):
            enabled_on = ""
        if enabled_on and enabled_on != "disabled":
            async def _delayed_collect(host: str):
                await asyncio.sleep(8)
                try:
                    await _monitor.collect_instances_now(host)
                except Exception:
                    pass
            asyncio.create_task(
                _delayed_collect(enabled_on),
                name=f"v7-delayed-collect-{enabled_on}",
            )

    return {"name": name, "local": True,
            "hosts": results, "ok": ok, "failed": fail}


# ── Endpoints ────────────────────────────────────────────────

@router.get("/instances")
def get_instances(session: SessionToken = Depends(require_auth)):
    """List all v7 instances with sync status from VPS data."""
    instances = _load_local_instances()
    instances = _enrich_with_vps_data(instances)
    return {"instances": instances}


@router.post("/activate/{name}")
async def activate_instance(
    name: str,
    session: SessionToken = Depends(require_auth),
):
    """Sync config files + status_v7.json to all VPS for a single instance."""
    config_path = Path(f"{PBGDIR}/data/run_v7/{name}/config.json")
    if not config_path.is_file():
        raise HTTPException(status_code=404, detail=f"Instance '{name}' not found")
    result = await _ssh_sync_instance(name)
    return result


@router.post("/activate-all")
async def activate_all(session: SessionToken = Depends(require_auth)):
    """SSH-push all instances that need activation."""
    instances = _load_local_instances()
    instances = _enrich_with_vps_data(instances)

    to_activate = [
        inst for inst in instances
        if inst["status"] in ("outdated", "activate_needed", "stop_needed")
    ]

    if not to_activate:
        return {"activated": 0, "results": [], "message": "All instances in sync"}

    results = []
    for inst in to_activate:
        r = await _ssh_sync_instance(inst["name"])
        results.append(r)

    ok = sum(1 for r in results if r.get("ok", 0) > 0)
    return {"activated": len(to_activate), "ok": ok, "results": results}


@router.delete("/instances/{name}")
async def delete_instance(
    name: str,
    session: SessionToken = Depends(require_auth),
):
    """Delete a v7 instance locally and on all connected VPS hosts."""
    # Sanitise name — must be a plain directory name, no path traversal
    if not name or "/" in name or "\\" in name or name in (".", ".."):
        raise HTTPException(status_code=400, detail="Invalid instance name")

    instance_dir = Path(PBGDIR) / "data" / "run_v7" / name
    if not instance_dir.is_dir():
        raise HTTPException(status_code=404, detail=f"Instance '{name}' not found")

    # Check if running on any VPS or locally
    instances = _load_local_instances()
    instances = _enrich_with_vps_data(instances)
    inst = next((i for i in instances if i["name"] == name), None)
    if inst and inst.get("running_on"):
        hosts = ", ".join(inst["running_on"])
        raise HTTPException(
            status_code=409,
            detail=f"Instance '{name}' is running on {hosts} — stop it first",
        )

    # 1) Backup locally before delete
    backup_ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    backup_dir = Path(PBGDIR) / "data" / "backup" / "v7" / name / backup_ts
    try:
        backup_dir.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(instance_dir, backup_dir)
        _log(SERVICE, f"Backed up '{name}' → {backup_dir}")
    except OSError as e:
        _log(SERVICE, f"Backup failed for '{name}': {e}", level="WARNING")
        # Continue with delete even if backup fails — log the warning

    # 2) Delete locally
    try:
        shutil.rmtree(instance_dir)
    except OSError as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete locally: {e}")

    _log(SERVICE, f"Deleted instance '{name}' locally")

    # 3) Remove from status_v7.json and push to all VPS
    _update_status_v7(name, remove=True)
    status_content = STATUS_V7_FILE.read_bytes() if STATUS_V7_FILE.is_file() else None

    vps_results = {}
    if _monitor and _monitor.pool:
        pool = _monitor.pool
        connected = pool.connected_hosts()
        remote_dir = f"{REMOTE_PBGUI_DIR}/data/run_v7/{name}"

        async def delete_on_host(hostname: str):
            for attempt in range(1, SFTP_RETRY_ATTEMPTS + 1):
                try:
                    # Remove instance directory
                    result = await pool.run(
                        hostname,
                        f"rm -rf ~/{remote_dir}",
                        timeout=15,
                    )
                    if result is None:
                        return {"success": False, "error": "Not connected"}

                    # Push updated status_v7.json (instance removed)
                    if status_content:
                        sftp = await pool._open_sftp(hostname)
                        if sftp:
                            try:
                                remote_cmd_dir = f"{REMOTE_PBGUI_DIR}/data/cmd"
                                try:
                                    await sftp.makedirs(
                                        remote_cmd_dir, exist_ok=True)
                                except Exception:
                                    pass
                                async with sftp.open(
                                        f"{remote_cmd_dir}/status_v7.json",
                                        "wb") as f:
                                    await f.write(status_content)
                            finally:
                                sftp.exit()

                    return {"success": True}
                except Exception as e:
                    if attempt < SFTP_RETRY_ATTEMPTS and _is_transient_error(e):
                        _log(SERVICE, f"SSH delete {name} → {hostname} "
                             f"attempt {attempt} failed: {e} — retrying",
                             level="WARNING")
                        await asyncio.sleep(SFTP_RETRY_DELAY)
                        continue
                    _log(SERVICE, f"SSH delete {name} → {hostname} failed: {e}",
                         level="ERROR", meta={"traceback": traceback.format_exc()})
                    return {"success": False, "error": str(e)}
            return {"success": False, "error": "All retry attempts failed"}

        tasks = {h: delete_on_host(h) for h in connected}
        raw = await asyncio.gather(*tasks.values(), return_exceptions=True)
        for hostname, result in zip(tasks.keys(), raw):
            if isinstance(result, Exception):
                vps_results[hostname] = {"success": False, "error": str(result)}
            else:
                vps_results[hostname] = result

        ok = sum(1 for r in vps_results.values() if r.get("success"))
        fail = len(vps_results) - ok
        _log(SERVICE, f"SSH delete '{name}': {ok}/{len(vps_results)} hosts OK"
             + (f", {fail} failed" if fail else ""), level="INFO")

        # 4) Delayed collect so UI refreshes
        if ok > 0:
            async def _delayed_collect():
                await asyncio.sleep(3)
                for h in connected:
                    try:
                        await _monitor.collect_instances_now(h)
                    except Exception:
                        pass
            asyncio.create_task(
                _delayed_collect(),
                name=f"v7-delete-collect-{name}",
            )

    return {
        "ok": True,
        "name": name,
        "hosts": vps_results,
    }


# ── Backup / Restore ────────────────────────────────────────

def _validate_name(name: str) -> None:
    """Raise 400 if name contains path traversal characters."""
    if not name or "/" in name or "\\" in name or name in (".", ".."):
        raise HTTPException(status_code=400, detail="Invalid name")


@router.get("/backups")
def list_backups(session: SessionToken = Depends(require_auth)):
    """List all v7 instance backups grouped by instance name."""
    backup_root = Path(PBGDIR) / "data" / "backup" / "v7"
    if not backup_root.is_dir():
        return {"backups": []}
    result = []
    for inst_dir in sorted(backup_root.iterdir()):
        if not inst_dir.is_dir():
            continue
        timestamps = sorted(
            [d.name for d in inst_dir.iterdir() if d.is_dir()],
            reverse=True,
        )
        if timestamps:
            # Check if instance currently exists in run_v7
            exists = (Path(PBGDIR) / "data" / "run_v7" / inst_dir.name).is_dir()
            result.append({
                "name": inst_dir.name,
                "timestamps": timestamps,
                "currently_exists": exists,
            })
    return {"backups": result}


@router.post("/restore/{name}/{timestamp}")
async def restore_instance(
    name: str,
    timestamp: str,
    session: SessionToken = Depends(require_auth),
):
    """Restore a v7 instance from backup."""
    _validate_name(name)
    _validate_name(timestamp)

    backup_dir = Path(PBGDIR) / "data" / "backup" / "v7" / name / timestamp
    if not backup_dir.is_dir():
        raise HTTPException(
            status_code=404,
            detail=f"Backup '{name}/{timestamp}' not found",
        )

    instance_dir = Path(PBGDIR) / "data" / "run_v7" / name
    if instance_dir.is_dir():
        raise HTTPException(
            status_code=409,
            detail=f"Instance '{name}' already exists — delete it first or choose a different name",
        )

    # Copy backup to run_v7
    try:
        shutil.copytree(backup_dir, instance_dir)
    except OSError as e:
        raise HTTPException(status_code=500, detail=f"Restore failed: {e}")

    _log(SERVICE, f"Restored '{name}' from backup {timestamp}")

    # Push restored config to all VPS
    result = await _ssh_sync_instance(name)
    return {
        "ok": True,
        "name": name,
        "timestamp": timestamp,
        "activate": result,
    }


@router.delete("/backups/{name}/{timestamp}")
def delete_backup(
    name: str,
    timestamp: str,
    session: SessionToken = Depends(require_auth),
):
    """Delete a specific backup."""
    _validate_name(name)
    _validate_name(timestamp)

    backup_dir = Path(PBGDIR) / "data" / "backup" / "v7" / name / timestamp
    if not backup_dir.is_dir():
        raise HTTPException(
            status_code=404,
            detail=f"Backup '{name}/{timestamp}' not found",
        )

    try:
        shutil.rmtree(backup_dir)
    except OSError as e:
        raise HTTPException(status_code=500, detail=f"Delete failed: {e}")

    # Clean up parent dir if empty
    parent = backup_dir.parent
    if parent.is_dir() and not any(parent.iterdir()):
        parent.rmdir()

    _log(SERVICE, f"Deleted backup '{name}/{timestamp}'")
    return {"ok": True, "name": name, "timestamp": timestamp}


# ── Instance Config (Edit) ──────────────────────────────────

@router.get("/instances/{name}/config")
def get_instance_config(
    name: str,
    session: SessionToken = Depends(require_auth),
):
    """Load the full config.json for a v7 instance via ConfigV7.

    Using ConfigV7.load_config() ensures all setters run (including
    normalize_symbol in ApprovedCoins/IgnoredCoins) — identical to Streamlit.
    """
    from Config import ConfigV7
    _validate_name(name)
    config_path = Path(PBGDIR) / "data" / "run_v7" / name / "config.json"
    if not config_path.is_file():
        raise HTTPException(status_code=404, detail=f"Instance '{name}' not found")
    cv7 = ConfigV7(file_name=config_path)
    cv7.load_config()
    return {"name": name, "config": cv7.config}


@router.put("/instances/{name}/config")
async def save_instance_config(
    name: str,
    request: Request,
    session: SessionToken = Depends(require_auth),
):
    """Save config.json for a v7 instance via ConfigV7.

    Applies the same logic as RunV7.save():
      - Passes config through ConfigV7 setters (normalization, conversions)
      - Increments pbgui.version
      - Sets backtest.exchange from user→exchange mapping
      - Updates status_v7.json
      - Triggers SSH sync to all VPS
    """
    from Config import ConfigV7
    _validate_name(name)
    body = await request.json()
    cfg = body.get("config")
    if not isinstance(cfg, dict):
        raise HTTPException(status_code=400, detail="Missing or invalid 'config' in body")

    # Pass through ConfigV7 so all setters (including normalization) run
    instance_dir = Path(PBGDIR) / "data" / "run_v7" / name
    instance_dir.mkdir(parents=True, exist_ok=True)
    config_path = instance_dir / "config.json"

    cv7 = ConfigV7(file_name=config_path)
    cv7.config = cfg

    # Increment version
    cv7.pbgui.version = cv7.pbgui.version + 1

    # Set backtest.exchange from user→exchange mapping
    live_user = cv7.live.user
    from User import Users
    users = Users()
    exchange = users.find_exchange(live_user)
    if exchange:
        bt_exchange = exchange
        if bt_exchange in ("bitget", "okx", "hyperliquid"):
            bt_exchange = "binance"
        cv7.backtest.backtest["exchange"] = bt_exchange
    cv7.backtest.backtest["base_dir"] = f"backtests/pbgui/{live_user}"

    cv7.save_config()

    version = cv7.pbgui.version

    # Update status_v7.json
    _update_status_v7(name)

    # Trigger SSH sync
    sync_result = await _ssh_sync_instance(name)

    _log(SERVICE, f"Saved config for '{name}' (v{version})")
    return {
        "ok": True,
        "name": name,
        "version": version,
        "sync": sync_result,
    }


@router.get("/users")
def get_users(session: SessionToken = Depends(require_auth)):
    """List all v7-compatible users with their exchanges."""
    from User import Users
    users = Users()
    result = []
    for name in users.list_v7():
        exchange = users.find_exchange(name)
        result.append({"name": name, "exchange": exchange or ""})
    return {"users": result}


@router.get("/hosts")
def get_hosts(session: SessionToken = Depends(require_auth)):
    """List available hosts for the 'enabled_on' dropdown."""
    master = _get_master_hostname()
    hosts = ["disabled", master]
    if _monitor and _monitor.pool:
        for h in sorted(_monitor.enabled_hosts):
            if h != master and h not in hosts:
                hosts.append(h)
    return {"hosts": hosts}


@router.get("/symbols")
def get_symbols(
    exchange: str = Query(..., description="Exchange ID (e.g. 'binance')"),
    session: SessionToken = Depends(require_auth),
):
    """Return normalized base coin names for a given exchange (active USDT linear perps).

    Uses the same CoinData.filter_mapping() call as the Streamlit UI so that
    normalization logic (multiplier prefixes, quote suffixes) stays in one place
    and cannot diverge between the two frontends.
    """
    from PBCoinData import CoinData
    cd = CoinData()
    approved, ignored = cd.filter_mapping(
        exchange=exchange,
        market_cap_min_m=0,
        vol_mcap_max=float("inf"),
        only_cpt=False,
        notices_ignore=False,
        tags=[],
        quote_filter=None,
        active_only=True,
        use_cache=True,
    )
    # Return all active coins (approved + ignored by filter, but present on exchange)
    symbols = sorted(set(approved) | set(ignored))
    return {"symbols": symbols}


@router.get("/tags")
def get_tags(
    exchange: str = Query(..., description="Exchange ID"),
    session: SessionToken = Depends(require_auth),
):
    """Return available filter tags for a given exchange."""
    from PBCoinData import CoinData
    cd = CoinData()
    tags = cd.get_mapping_tags(exchange=exchange, use_cache=True)
    return {"tags": tags}


@router.get("/coins/filter")
def filter_coins(
    exchange: str = Query(...),
    market_cap: int = Query(0),
    vol_mcap: float = Query(10.0),
    only_cpt: bool = Query(False),
    notices_ignore: bool = Query(False),
    tags: str = Query("", description="Comma-separated tags"),
    session: SessionToken = Depends(require_auth),
):
    """Preview dynamic-ignore filter results."""
    from PBCoinData import CoinData
    cd = CoinData()
    tag_list = [t.strip() for t in tags.split(",") if t.strip()] if tags else []
    approved, ignored = cd.filter_mapping(
        exchange=exchange,
        market_cap_min_m=market_cap,
        vol_mcap_max=vol_mcap,
        only_cpt=only_cpt,
        notices_ignore=notices_ignore,
        tags=tag_list,
        quote_filter=None,
        use_cache=True,
    )
    return {"approved": approved, "ignored": ignored}


@router.get("/log/{name}")
def get_instance_log(
    name: str,
    lines: int = Query(500, ge=1, le=10000),
    session: SessionToken = Depends(require_auth),
):
    """Read the passivbot.log for an instance (tail N lines)."""
    _validate_name(name)
    log_path = Path(PBGDIR) / "data" / "run_v7" / name / "passivbot.log"
    if not log_path.is_file():
        return {"name": name, "log": ""}
    try:
        with open(log_path, "r", encoding="utf-8") as f:
            all_lines = f.readlines()
        tail = all_lines[-lines:] if len(all_lines) > lines else all_lines
        return {"name": name, "log": "".join(reversed(tail))}
    except OSError:
        return {"name": name, "log": ""}


@router.get("/last-active-host/{name}")
def get_last_active_host(
    name: str,
    session: SessionToken = Depends(require_auth),
):
    """Find the last VPS host where a bot was active by scanning backups.

    Scans data/backup/v7/{name}/*/config.json in reverse order (newest first)
    looking for enabled_on != 'disabled'.
    Returns {name, host, version} or {name, host: ''} if none found.
    """
    _validate_name(name)
    master = _get_master_hostname()
    backup_root = Path(PBGDIR) / "data" / "backup" / "v7" / name
    if not backup_root.is_dir():
        return {"name": name, "host": "", "master": master}
    # Sort backup dirs by mtime descending (newest first)
    dirs = sorted(
        [d for d in backup_root.iterdir() if d.is_dir()],
        key=lambda d: d.stat().st_mtime,
        reverse=True,
    )
    for d in dirs:
        cfg_file = d / "config.json"
        if not cfg_file.is_file():
            continue
        try:
            with open(cfg_file, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            host = cfg.get("pbgui", {}).get("enabled_on", "disabled") or "disabled"
            if host != "disabled":
                return {"name": name, "host": host, "version": d.name, "master": master}
        except (json.JSONDecodeError, OSError):
            continue
    return {"name": name, "host": "", "master": master}


@router.get("/log-smart/{name}")
def get_instance_log_smart(
    name: str,
    lines: int = Query(500, ge=1, le=10000),
    session: SessionToken = Depends(require_auth),
):
    """Return passivbot.log with smart fallback.

    - enabled_on != 'disabled': read live log from data/run_v7/{name}/passivbot.log
    - disabled: find most recent backup in data/backup/v7/{name}/ that contains passivbot.log
    Returns {name, log, source, source_label} where source is 'live' or 'backup:{timestamp}'.
    """
    _validate_name(name)

    # Determine enabled_on from saved config
    cfg_path = Path(PBGDIR) / "data" / "run_v7" / name / "config.json"
    enabled_on = "disabled"
    if cfg_path.is_file():
        try:
            with open(cfg_path, "r", encoding="utf-8") as f:
                cfg_data = json.load(f)
            enabled_on = cfg_data.get("pbgui", {}).get("enabled_on", "disabled") or "disabled"
        except (json.JSONDecodeError, OSError):
            pass

    def _read_log(log_path: Path) -> str:
        try:
            with open(log_path, "r", encoding="utf-8") as f:
                all_lines = f.readlines()
            tail = all_lines[-lines:] if len(all_lines) > lines else all_lines
            return "".join(reversed(tail))
        except OSError:
            return ""

    # Try live log first (always preferred if the file exists and bot is not disabled)
    live_log_path = Path(PBGDIR) / "data" / "run_v7" / name / "passivbot.log"
    if enabled_on != "disabled" and live_log_path.is_file():
        return {
            "name": name,
            "log": _read_log(live_log_path),
            "source": "live",
            "source_label": f"live ({enabled_on})",
        }

    # Fallback: most recent backup with passivbot.log
    backup_root = Path(PBGDIR) / "data" / "backup" / "v7" / name
    if backup_root.is_dir():
        timestamps = sorted(
            [d.name for d in backup_root.iterdir() if d.is_dir()],
            reverse=True,
        )
        for ts in timestamps:
            backup_log = backup_root / ts / "passivbot.log"
            if backup_log.is_file():
                return {
                    "name": name,
                    "log": _read_log(backup_log),
                    "source": f"backup:{ts}",
                    "source_label": f"last active (backup {ts})",
                }

    # No log found at all — try live path as last resort even if disabled
    if live_log_path.is_file():
        return {
            "name": name,
            "log": _read_log(live_log_path),
            "source": "live",
            "source_label": "local log",
        }

    return {"name": name, "log": "", "source": "none", "source_label": "no log found"}


@router.get("/instances/{name}/coin-config/{symbol}")
def get_coin_config(
    name: str,
    symbol: str,
    session: SessionToken = Depends(require_auth),
):
    """Load per-coin override config (bot section) from {symbol}.json."""
    _validate_name(name)
    config_dir = Path(PBGDIR) / "data" / "run_v7" / name
    config_file = config_dir / f"{symbol}.json"
    if config_file.is_file():
        try:
            with open(config_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            return {"bot": data.get("bot", {})}
        except (json.JSONDecodeError, OSError):
            return {"bot": {}}
    return {"bot": {}}


@router.put("/instances/{name}/coin-config/{symbol}")
def save_coin_config(
    name: str,
    symbol: str,
    body: dict = Body(...),
    session: SessionToken = Depends(require_auth),
):
    """Save per-coin override config (bot section) to {symbol}.json."""
    _validate_name(name)
    config_dir = Path(PBGDIR) / "data" / "run_v7" / name
    config_file = config_dir / f"{symbol}.json"
    # Load existing to preserve non-bot sections
    full = {}
    if config_file.is_file():
        try:
            with open(config_file, "r", encoding="utf-8") as f:
                full = json.load(f)
        except (json.JSONDecodeError, OSError):
            full = {}
    full["bot"] = body.get("bot", {})
    tmp = config_file.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(full, f, indent=4)
    tmp.rename(config_file)
    return {"ok": True}


@router.delete("/instances/{name}/coin-config/{symbol}")
def delete_coin_config(
    name: str,
    symbol: str,
    session: SessionToken = Depends(require_auth),
):
    """Delete per-coin override config file."""
    _validate_name(name)
    config_dir = Path(PBGDIR) / "data" / "run_v7" / name
    config_file = config_dir / f"{symbol}.json"
    config_file.unlink(missing_ok=True)
    return {"ok": True}


@router.get("/main_page", response_class=HTMLResponse)
def get_main_page(
    request: Request,
    st_base: str = Query(default="", description="Streamlit base URL"),
    session: SessionToken = Depends(require_auth),
) -> HTMLResponse:
    """Serve the standalone v7 Run page."""
    html_path = Path(__file__).parent.parent / "frontend" / "v7_run.html"
    html = html_path.read_text(encoding="utf-8")

    scheme = request.url.scheme
    host = request.url.hostname or "127.0.0.1"
    port = request.url.port
    origin = f"{scheme}://{host}" + (f":{port}" if port else "")
    api_base = origin + "/api/v7"
    ws_base = origin.replace("http://", "ws://").replace("https://", "wss://")

    html = html.replace('"%%TOKEN%%"', json.dumps(session.token))
    html = html.replace('"%%API_BASE%%"', json.dumps(api_base))
    html = html.replace('"%%WS_BASE%%"', json.dumps(ws_base))

    if not st_base:
        st_base = f"http://{host}:8501"
    html = html.replace('"%%ST_BASE%%"', json.dumps(st_base))

    from pbgui_func import PBGUI_VERSION
    from pbgui_purefunc import PBGUI_SERIAL
    html = html.replace('"%%VERSION%%"', json.dumps(PBGUI_VERSION))
    html = html.replace("%%VERSION%%", PBGUI_VERSION)
    html = html.replace('"%%SERIAL%%"', json.dumps(PBGUI_SERIAL))
    html = html.replace("%%SERIAL%%", PBGUI_SERIAL)

    nav_js = Path(__file__).parent.parent / "frontend" / "pbgui_nav.js"
    nav_hash = str(int(nav_js.stat().st_mtime)) if nav_js.exists() else PBGUI_VERSION
    html = html.replace("%%NAV_HASH%%", nav_hash)

    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})


@router.get("/edit_page", response_class=HTMLResponse)
def get_edit_page(
    request: Request,
    name: str = Query(default="", description="Instance name to edit"),
    new: str = Query(default="", description="Set to '1' for new instance"),
    st_base: str = Query(default="", description="Streamlit base URL"),
    session: SessionToken = Depends(require_auth),
) -> HTMLResponse:
    """Serve the standalone v7 Edit page."""
    html_path = Path(__file__).parent.parent / "frontend" / "v7_edit.html"
    html = html_path.read_text(encoding="utf-8")

    scheme = request.url.scheme
    host = request.url.hostname or "127.0.0.1"
    port = request.url.port
    origin = f"{scheme}://{host}" + (f":{port}" if port else "")
    api_base = origin + "/api/v7"
    ws_base = origin.replace("http://", "ws://").replace("https://", "wss://")

    html = html.replace('"%%TOKEN%%"', json.dumps(session.token))
    html = html.replace('"%%API_BASE%%"', json.dumps(api_base))
    html = html.replace('"%%WS_BASE%%"', json.dumps(ws_base))

    if not st_base:
        st_base = f"http://{host}:8501"
    html = html.replace('"%%ST_BASE%%"', json.dumps(st_base))

    is_new = "true" if new == "1" else "false"
    html = html.replace('"%%INSTANCE%%"', json.dumps(name))
    html = html.replace('"%%IS_NEW%%"', json.dumps(is_new))

    from pbgui_func import PBGUI_VERSION
    from pbgui_purefunc import PBGUI_SERIAL
    html = html.replace('"%%VERSION%%"', json.dumps(PBGUI_VERSION))
    html = html.replace("%%VERSION%%", PBGUI_VERSION)
    html = html.replace('"%%SERIAL%%"', json.dumps(PBGUI_SERIAL))
    html = html.replace("%%SERIAL%%", PBGUI_SERIAL)

    nav_js = Path(__file__).parent.parent / "frontend" / "pbgui_nav.js"
    nav_hash = str(int(nav_js.stat().st_mtime)) if nav_js.exists() else PBGUI_VERSION
    html = html.replace("%%NAV_HASH%%", nav_hash)

    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})


# ── WebSocket ────────────────────────────────────────────────

V7_WS_PUSH_INTERVAL = 1.0  # throttle: max 1 push/sec

_ws_clients: set[WebSocket] = set()


@router.websocket("/ws/v7")
async def ws_v7(websocket: WebSocket):
    """WebSocket for real-time v7 instance updates.

    Query param: ``?token=xxx``
    Push: ``{"type": "instances", "data": [...]}`` on every store change.
    """
    token = websocket.query_params.get("token", "")
    if not validate_token(token):
        await websocket.close(code=4001)
        return
    await websocket.accept()
    _ws_clients.add(websocket)
    _log(SERVICE, f"[ws] v7 client connected: {websocket.client}")

    push_task = asyncio.create_task(
        _v7_push_loop(websocket), name="v7-ws-push")

    try:
        while True:
            # Keep connection alive; ignore client messages
            await websocket.receive_text()
    except (WebSocketDisconnect, asyncio.CancelledError):
        pass
    except Exception as e:
        _log(SERVICE, f"[ws] v7 client error: {e}", level="WARNING")
    finally:
        _ws_clients.discard(websocket)
        push_task.cancel()
        _log(SERVICE, f"[ws] v7 client disconnected: {websocket.client}")


async def _v7_push_loop(ws: WebSocket):
    """Push v7 instance state whenever the VPS store changes."""
    try:
        # Send initial state immediately
        await _send_v7_state(ws)
        while True:
            if _monitor and _monitor.store:
                _monitor.store.changed.clear()
                await _monitor.store.changed.wait()
                await asyncio.sleep(V7_WS_PUSH_INTERVAL)
                await _send_v7_state(ws)
            else:
                await asyncio.sleep(V7_WS_PUSH_INTERVAL)
    except (asyncio.CancelledError, WebSocketDisconnect):
        pass
    except Exception as e:
        _log(SERVICE, f"[ws] v7 push error: {e}", level="WARNING")


async def _send_v7_state(ws: WebSocket):
    """Build and push the v7 instance list."""
    instances = _load_local_instances()
    instances = _enrich_with_vps_data(instances)
    await ws.send_json({"type": "instances", "data": instances})
