"""
FastAPI router for v7 instance list + SSH activate.

Endpoints:
    GET  /instances          → list all v7 instances with sync status
    POST /activate/{name}    → SSH-push config + activate_cmd to all VPS
    POST /activate-all       → SSH-push all instances that need activation
    GET  /main_page          → serve the standalone HTML page
"""

from __future__ import annotations

import asyncio
import configparser
import json
import platform
import time
import traceback
import uuid
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

from api.auth import SessionToken, require_auth, validate_token
from logging_helpers import human_log as _log
from master.async_pool import SFTP_RETRY_ATTEMPTS, SFTP_RETRY_DELAY, _is_transient_error
from pbgui_purefunc import PBGDIR

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
            elif has_data:
                # We have confirmed VPS data and the bot is not running
                inst["status"] = "disabled"
            else:
                # No VPS data yet — cannot confirm bot stopped; be conservative
                inst["status"] = "stop_needed"
        elif enabled in running_on and version == rv:
            inst["status"] = "synced"
        elif running_on:
            inst["status"] = "outdated"
        else:
            inst["status"] = "activate_needed"

    return instances


# ── SSH Activate ─────────────────────────────────────────────

async def _ssh_activate_single(name: str) -> dict:
    """Write local activate cmd for PBRun + push config via SFTP to VPS."""
    # Read local config
    config_path = Path(f"{PBGDIR}/data/run_v7/{name}/config.json")
    if not config_path.is_file():
        return {"name": name, "error": f"Config not found: {name}"}

    config_content = config_path.read_bytes()

    # Build activate cmd
    activate_payload = json.dumps({
        "instance": name,
        "multi": False,
        "version": "7",
    })

    # Always write local activate cmd so PBRun picks it up
    # (PBRun checks enabled_on itself and starts or stops accordingly)
    local_activated = False
    try:
        local_cmd_dir = Path(PBGDIR) / "data" / "cmd"
        local_cmd_dir.mkdir(parents=True, exist_ok=True)
        local_cmd_file = local_cmd_dir / f"activate_{uuid.uuid4()}.cmd"
        local_cmd_file.write_text(activate_payload, encoding="utf-8")
        local_activated = True
    except OSError as e:
        _log(SERVICE, f"Failed to write local activate cmd for '{name}': {e}",
             level="ERROR")

    # Push to VPS via SSH
    if not _monitor:
        return {"name": name, "local": local_activated,
                "hosts": {}, "ok": 0, "failed": 0}

    pool = _monitor.pool
    connected = pool.connected_hosts()
    if not connected:
        return {"name": name, "local": local_activated,
                "hosts": {}, "ok": 0, "failed": 0}

    activate_cmd_bytes = activate_payload.encode("utf-8")
    cmd_filename = f"activate_{uuid.uuid4()}.cmd"
    results = {}

    async def push_to_host(hostname: str):
        for attempt in range(1, SFTP_RETRY_ATTEMPTS + 1):
            entry = pool.get_connection(hostname)
            if not entry:
                return {"success": False, "error": "Not connected"}

            sftp = await pool._open_sftp(hostname)
            if not sftp:
                return {"success": False, "error": "SFTP failed"}

            try:
                remote_config = f"{REMOTE_PBGUI_DIR}/data/run_v7/{name}/config.json"
                remote_cmd = f"{REMOTE_PBGUI_DIR}/data/cmd/{cmd_filename}"

                # Ensure dirs exist
                try:
                    await sftp.makedirs(f"{REMOTE_PBGUI_DIR}/data/run_v7/{name}", exist_ok=True)
                except Exception:
                    pass
                try:
                    await sftp.makedirs(f"{REMOTE_PBGUI_DIR}/data/cmd", exist_ok=True)
                except Exception:
                    pass

                # Write config
                async with sftp.open(remote_config, "wb") as f:
                    await f.write(config_content)

                # Write activate cmd
                async with sftp.open(remote_cmd, "wb") as f:
                    await f.write(activate_cmd_bytes)

                return {"success": True}
            except Exception as e:
                if attempt < SFTP_RETRY_ATTEMPTS and _is_transient_error(e):
                    _log(SERVICE, f"SSH activate {name} → {hostname} "
                         f"failed (attempt {attempt}): {e} — retrying",
                         level="WARNING")
                    await asyncio.sleep(SFTP_RETRY_DELAY)
                    continue
                _log(SERVICE, f"SSH activate {name} → {hostname} failed: {e}",
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
    _log(SERVICE, f"SSH activate '{name}': {ok}/{len(results)} hosts OK"
         + (f", {fail} failed" if fail else ""), level="INFO")

    # Note: no watcher restart needed — persistent streaming watchers
    # already detect config.json + running_version.txt changes in
    # existing instance dirs.  The watchdog handles discovery of
    # entirely new instance directories every WATCHDOG_INTERVAL.

    # Schedule a delayed collect on the enabled_on host as fallback —
    # PBRun needs a few seconds to process the activate_cmd and write
    # running_version.txt.  The inotify watcher should fire first, but
    # this covers edge cases (watcher down, inotify race, etc.).
    if ok > 0 and _monitor:
        try:
            cfg = json.loads(config_content)
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

    return {"name": name, "local": local_activated,
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
    """SSH-push config + activate command to all VPS for a single instance."""
    config_path = Path(f"{PBGDIR}/data/run_v7/{name}/config.json")
    if not config_path.is_file():
        raise HTTPException(status_code=404, detail=f"Instance '{name}' not found")
    result = await _ssh_activate_single(name)
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
        r = await _ssh_activate_single(inst["name"])
        results.append(r)

    ok = sum(1 for r in results if r.get("ok", 0) > 0)
    return {"activated": len(to_activate), "ok": ok, "results": results}


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
