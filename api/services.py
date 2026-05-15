"""FastAPI router for Services management (start/stop/settings for all PBGui daemons)."""

from __future__ import annotations

import asyncio
from collections import Counter
import glob
import json
import importlib
import os
import signal
import subprocess
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from api.auth import require_auth, SessionToken
from api.vps import get_monitor_state_snapshot
from pbgui_purefunc import PBGDIR, load_ini, save_ini
from logging_helpers import human_log as _log

SERVICE = "Services"

router = APIRouter()

_SERVICES = ["pbrun", "pbremote", "pbmon", "pbdata", "pbcoindata", "api-server"]


def _get_service(name: str):
    """Instantiate and return the service object for the given name."""
    if name == "pbrun":
        from PBRun import PBRun
        return PBRun()
    if name == "pbremote":
        from PBRemote import PBRemote
        return PBRemote()
    if name == "pbmon":
        from PBMon import PBMon
        return PBMon()
    if name == "pbdata":
        from PBData import PBData
        obj = PBData.__new__(PBData)
        obj.piddir = Path(f'{PBGDIR}/data/pid')
        obj.pidfile = Path(f'{PBGDIR}/data/pid/pbdata.pid')
        obj.my_pid = None
        return obj
    if name == "pbcoindata":
        from PBCoinData import CoinData
        return CoinData()
    if name == "api-server":
        # Lazy import to avoid circular import (PBApiServer.py imports api/services.py)
        mod = importlib.import_module("PBApiServer")
        return mod.PBApiServer()
    raise ValueError(f"Unknown service: {name}")


def _task_active(task: Any) -> bool:
    return bool(task and not task.done())


def _worker_stat(label: str, value: Any) -> dict[str, str]:
    return {"label": str(label), "value": str(value)}


def _worker_item(
    *,
    worker_id: str,
    label: str,
    group: str,
    worker_type: str,
    running: bool,
    summary: str,
    description: str,
    note: str = "",
    stats: list[dict[str, str]] | None = None,
    log_file: str | None = None,
    monitor_path: str | None = None,
    available: bool = True,
    can_start: bool | None = None,
    can_stop: bool | None = None,
) -> dict[str, Any]:
    if can_start is None:
        can_start = available
    if can_stop is None:
        can_stop = available
    return {
        "id": worker_id,
        "label": label,
        "group": group,
        "type": worker_type,
        "running": bool(running),
        "summary": summary,
        "description": description,
        "note": note,
        "stats": stats or [],
        "log_file": log_file,
        "monitor_path": monitor_path,
        "available": bool(available),
        "can_start": bool(can_start),
        "can_stop": bool(can_stop),
    }


def _get_task_worker_item() -> dict[str, Any]:
    from task_queue import list_jobs, read_worker_pid, is_pid_running, clear_worker_pid

    jobs = list_jobs(states=["pending", "running", "done", "failed"], limit=0)
    counts = Counter(str(job.get("status") or "unknown").strip().lower() for job in jobs)
    pid = read_worker_pid()
    running = bool(pid and is_pid_running(int(pid)))
    if pid and not running:
        clear_worker_pid()
        pid = None

    pending = counts.get("pending", 0)
    active = counts.get("running", 0) + counts.get("cancelling", 0)
    done = counts.get("done", 0)
    failed = counts.get("failed", 0)
    summary = f"{pending} pending, {active} active"
    return _worker_item(
        worker_id="market-data-task",
        label="Market Data Queue",
        group="queue",
        worker_type="process worker",
        running=running,
        summary=summary,
        description="Processes queued Market Data and Heatmap jobs from the shared task queue.",
        note="Stop sends SIGTERM to the worker process. If pending jobs remain, the PBAPIServer watchdog may start it again.",
        stats=[
            _worker_stat("PID", pid or "-"),
            _worker_stat("Pending", pending),
            _worker_stat("Active", active),
            _worker_stat("Done", done),
            _worker_stat("Failed", failed),
        ],
        monitor_path="/app/jobs_monitor.html",
        log_file=None,
    )


async def _get_backtest_worker_item() -> dict[str, Any]:
    import api.backtest_v7 as bt7

    await bt7._store.refresh_from_disk()
    items = list(bt7._store.items.values())
    counts = Counter(str(item.get("status") or "unknown").strip().lower() for item in items)
    settings = bt7._read_ini_section()
    cpu = settings.get("cpu", "1")
    autostart = settings.get("autostart", "False").lower() == "true"
    running = _task_active(getattr(bt7._worker, "_task", None))
    summary = f"{counts.get('queued', 0)} queued, {counts.get('running', 0) + counts.get('backtesting', 0)} active"
    return _worker_item(
        worker_id="backtest-queue",
        label="Backtest Queue",
        group="queue",
        worker_type="scheduler task",
        running=running,
        summary=summary,
        description="Schedules queued PB7 backtests and launches detached PB7 subprocesses.",
        note="Stopping the worker pauses queue processing only. Already launched PB7 backtest subprocesses continue running.",
        stats=[
            _worker_stat("Queued", counts.get("queued", 0)),
            _worker_stat("Running", counts.get("running", 0)),
            _worker_stat("Backtesting", counts.get("backtesting", 0)),
            _worker_stat("Complete", counts.get("complete", 0)),
            _worker_stat("Error", counts.get("error", 0)),
            _worker_stat("Autostart", "On" if autostart else "Off"),
            _worker_stat("CPU limit", cpu),
        ],
        log_file="BacktestV7API.log",
    )


async def _get_optimize_worker_item() -> dict[str, Any]:
    import api.optimize_v7 as opt7

    await opt7._store.refresh_from_disk()
    items = list(opt7._store.items.values())
    counts = Counter(str(item.get("status") or "unknown").strip().lower() for item in items)
    settings = opt7._read_ini_section()
    autostart = settings.get("autostart", "False").lower() == "true"
    cpu_override = settings.get("cpu_override", "True").lower() == "true"
    cpu = settings.get("cpu", "1")
    running = _task_active(getattr(opt7._worker, "_task", None))
    summary = f"{counts.get('queued', 0)} queued, {counts.get('running', 0) + counts.get('optimizing', 0)} active"
    return _worker_item(
        worker_id="optimize-queue",
        label="Optimize Queue",
        group="queue",
        worker_type="scheduler task",
        running=running,
        summary=summary,
        description="Schedules queued PB7 optimize jobs and launches detached PB7 optimizer subprocesses.",
        note="Stopping the worker pauses queue processing only. Already launched PB7 optimize subprocesses continue running.",
        stats=[
            _worker_stat("Queued", counts.get("queued", 0)),
            _worker_stat("Running", counts.get("running", 0)),
            _worker_stat("Optimizing", counts.get("optimizing", 0)),
            _worker_stat("Complete", counts.get("complete", 0)),
            _worker_stat("Error", counts.get("error", 0)),
            _worker_stat("Autostart", "On" if autostart else "Off"),
            _worker_stat("Autostart CPU", cpu),
            _worker_stat("CPU override", "On" if cpu_override else "Off"),
        ],
        log_file="OptimizeV7API.log",
    )


def _get_archive_sync_worker_item() -> dict[str, Any]:
    import api.backtest_v7 as bt7

    running = _task_active(getattr(bt7._archive_sync_worker, "_task", None))
    return _worker_item(
        worker_id="archive-sync",
        label="Archive Sync",
        group="internal",
        worker_type="periodic task",
        running=running,
        summary="Auto-pulls configured backtest archives",
        description="Keeps configured backtest archives up to date in the background.",
        note="This worker is tied to the Backtest subsystem and is usually only needed for archive maintenance.",
        stats=[
            _worker_stat("Running", "Yes" if running else "No"),
            _worker_stat("Auto pull", bt7._read_auto_pull_interval()),
        ],
        log_file="ArchiveSync.log",
    )


def _get_hlcvs_cleanup_worker_item() -> dict[str, Any]:
    import api.backtest_v7 as bt7

    running = _task_active(getattr(bt7._hlcvs_cleanup_worker, "_task", None))
    targets = []
    try:
        targets = bt7._cleanup_cache_targets()
    except Exception:
        targets = []
    return _worker_item(
        worker_id="hlcvs-cleanup",
        label="HLCVS Cleanup",
        group="internal",
        worker_type="periodic task",
        running=running,
        summary=f"Maintains {len(targets)} cache target(s)",
        description="Periodically removes expired PB7 cache materialization data from configured cleanup targets.",
        stats=[
            _worker_stat("Running", "Yes" if running else "No"),
            _worker_stat("Targets", len(targets)),
        ],
        log_file="HLCVSCleanup.log",
    )


def _get_file_sync_worker_item() -> dict[str, Any]:
    from api.api_keys import _file_sync_worker

    if not _file_sync_worker:
        return _worker_item(
            worker_id="file-sync",
            label="API Key File Sync",
            group="sync",
            worker_type="watcher + watchdog",
            running=False,
            summary="Worker not initialized",
            description="Pushes api-keys.json updates and watches remote VPS copies for changes.",
            note="Available only while PBAPIServer has initialized the FileSyncWorker.",
            available=False,
            can_start=False,
            can_stop=False,
            log_file="FileSync.log",
        )

    status = _file_sync_worker.get_status()
    hosts = status.get("hosts", {})
    connected = status.get("connected_hosts", [])
    watcher_active = sum(1 for item in hosts.values() if item.get("watcher_active"))
    in_sync = sum(1 for item in hosts.values() if item.get("in_sync") is True)
    watchdog_active = _task_active(getattr(_file_sync_worker, "_watchdog", None))
    running = bool(watchdog_active or watcher_active)
    return _worker_item(
        worker_id="file-sync",
        label="API Key File Sync",
        group="sync",
        worker_type="watcher + watchdog",
        running=running,
        summary=f"{watcher_active}/{len(connected)} watcher(s) active",
        description="Pushes api-keys.json updates and watches remote VPS copies for changes.",
        note="Start/stop controls the inotify watchers and watchdog inside PBAPIServer.",
        stats=[
            _worker_stat("Connected hosts", len(connected)),
            _worker_stat("Watchers active", watcher_active),
            _worker_stat("Hosts in sync", in_sync),
            _worker_stat("Watchdog", "On" if watchdog_active else "Off"),
        ],
        log_file="FileSync.log",
    )


def _get_v7_sync_worker_item() -> dict[str, Any]:
    from api.v7_instances import _v7_sync

    if not _v7_sync:
        return _worker_item(
            worker_id="v7-config-sync",
            label="V7 Config Sync",
            group="sync",
            worker_type="watcher + watchdog",
            running=False,
            summary="Worker not initialized",
            description="Watches remote V7 instance state and synchronizes config changes between masters.",
            note="Available only while PBAPIServer has initialized the V7ConfigSyncWorker.",
            available=False,
            can_start=False,
            can_stop=False,
            log_file="V7ConfigSync.log",
        )

    watchers = getattr(_v7_sync, "_watchers", {})
    connected = _v7_sync.pool.connected_hosts()
    watcher_active = sum(1 for task in watchers.values() if _task_active(task))
    watchdog_active = _task_active(getattr(_v7_sync, "_watchdog", None))
    running = bool(watchdog_active or watcher_active)
    return _worker_item(
        worker_id="v7-config-sync",
        label="V7 Config Sync",
        group="sync",
        worker_type="watcher + watchdog",
        running=running,
        summary=f"{watcher_active}/{len(connected)} watcher(s) active",
        description="Watches remote V7 instance state and synchronizes config changes between masters.",
        note="Start/stop controls the inotify watchers and watchdog inside PBAPIServer.",
        stats=[
            _worker_stat("Connected hosts", len(connected)),
            _worker_stat("Watchers active", watcher_active),
            _worker_stat("Watchdog", "On" if watchdog_active else "Off"),
        ],
        log_file="V7ConfigSync.log",
    )


async def _collect_worker_groups() -> list[dict[str, Any]]:
    groups = [
        {
            "id": "queue",
            "label": "Queue Workers",
            "items": [
                _get_task_worker_item(),
                await _get_backtest_worker_item(),
                await _get_optimize_worker_item(),
            ],
        },
        {
            "id": "sync",
            "label": "Sync / Watchers",
            "items": [
                _get_file_sync_worker_item(),
                _get_v7_sync_worker_item(),
            ],
        },
        {
            "id": "internal",
            "label": "Internal Helpers",
            "items": [
                _get_archive_sync_worker_item(),
                _get_hlcvs_cleanup_worker_item(),
            ],
        },
    ]
    return groups


async def _find_worker(worker_id: str) -> dict[str, Any] | None:
    groups = await _collect_worker_groups()
    for group in groups:
        for item in group.get("items", []):
            if item.get("id") == worker_id:
                return item
    return None


def _spawn_task_worker() -> None:
    from task_queue import clear_worker_pid

    clear_worker_pid()
    subprocess.Popen(
        [sys.executable, str(Path(__file__).resolve().parents[1] / "task_worker.py")],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        close_fds=True,
    )


async def _start_worker(worker_id: str) -> None:
    if worker_id == "market-data-task":
        _spawn_task_worker()
        return
    if worker_id == "backtest-queue":
        import api.backtest_v7 as bt7
        bt7._worker.start()
        return
    if worker_id == "optimize-queue":
        import api.optimize_v7 as opt7
        opt7._worker.start()
        return
    if worker_id == "archive-sync":
        import api.backtest_v7 as bt7
        bt7._archive_sync_worker.start()
        return
    if worker_id == "hlcvs-cleanup":
        import api.backtest_v7 as bt7
        bt7._hlcvs_cleanup_worker.start()
        return
    if worker_id == "file-sync":
        from api.api_keys import _file_sync_worker
        if not _file_sync_worker:
            raise HTTPException(status_code=409, detail="FileSyncWorker not initialized")
        await _file_sync_worker.start_watchers()
        _file_sync_worker.start_watchdog()
        return
    if worker_id == "v7-config-sync":
        from api.v7_instances import _v7_sync
        if not _v7_sync:
            raise HTTPException(status_code=409, detail="V7ConfigSyncWorker not initialized")
        await _v7_sync.start_watchers()
        _v7_sync.start_watchdog()
        return
    raise HTTPException(status_code=404, detail=f"Unknown worker: {worker_id}")


async def _stop_worker(worker_id: str) -> None:
    if worker_id == "market-data-task":
        from task_queue import read_worker_pid, is_pid_running, clear_worker_pid

        pid = read_worker_pid()
        if pid and is_pid_running(int(pid)):
            os.kill(int(pid), signal.SIGTERM)
            await asyncio.sleep(0.4)
        pid = read_worker_pid()
        if pid and not is_pid_running(int(pid)):
            clear_worker_pid()
        return
    if worker_id == "backtest-queue":
        import api.backtest_v7 as bt7
        bt7._worker.stop()
        return
    if worker_id == "optimize-queue":
        import api.optimize_v7 as opt7
        opt7._worker.stop()
        return
    if worker_id == "archive-sync":
        import api.backtest_v7 as bt7
        bt7._archive_sync_worker.stop()
        return
    if worker_id == "hlcvs-cleanup":
        import api.backtest_v7 as bt7
        bt7._hlcvs_cleanup_worker.stop()
        return
    if worker_id == "file-sync":
        from api.api_keys import _file_sync_worker
        if not _file_sync_worker:
            raise HTTPException(status_code=409, detail="FileSyncWorker not initialized")
        await _file_sync_worker.stop_watchers()
        _file_sync_worker.stop_watchdog()
        return
    if worker_id == "v7-config-sync":
        from api.v7_instances import _v7_sync
        if not _v7_sync:
            raise HTTPException(status_code=409, detail="V7ConfigSyncWorker not initialized")
        await _v7_sync.stop_watchers()
        _v7_sync.stop_watchdog()
        return
    raise HTTPException(status_code=404, detail=f"Unknown worker: {worker_id}")


# ── Status ───────────────────────────────────────────────────

@router.get("/status")
def get_status(session: SessionToken = Depends(require_auth)) -> Dict[str, Any]:
    """Return running status for all services."""
    result = {}
    for svc in _SERVICES:
        try:
            obj = _get_service(svc)
            result[svc] = {"running": bool(obj.is_running())}
        except Exception as e:
            _log(SERVICE, f"status check failed for {svc}: {e}", level="WARNING")
            result[svc] = {"running": False, "error": str(e)}
    return result


@router.get("/workers/status")
async def get_workers_status(session: SessionToken = Depends(require_auth)) -> Dict[str, Any]:
    groups = await _collect_worker_groups()
    total = sum(len(group.get("items", [])) for group in groups)
    running = sum(
        1
        for group in groups
        for item in group.get("items", [])
        if item.get("running")
    )
    return {
        "updated_ts": int(time.time()),
        "counts": {"total": total, "running": running},
        "groups": groups,
    }


# ── Start / Stop ─────────────────────────────────────────────

@router.post("/{service}/start")
def start_service(service: str, session: SessionToken = Depends(require_auth)) -> Dict[str, Any]:
    if service not in _SERVICES:
        raise HTTPException(status_code=404, detail=f"Unknown service: {service}")
    try:
        obj = _get_service(service)
        if not obj.is_running():
            obj.run()
        return {"running": bool(obj.is_running())}
    except Exception as e:
        _log(SERVICE, f"start {service} failed: {e}\n{traceback.format_exc()}", level="ERROR")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{service}/stop")
def stop_service(service: str, session: SessionToken = Depends(require_auth)) -> Dict[str, Any]:
    if service not in _SERVICES:
        raise HTTPException(status_code=404, detail=f"Unknown service: {service}")
    try:
        obj = _get_service(service)
        if obj.is_running():
            obj.stop()
        return {"running": bool(obj.is_running())}
    except Exception as e:
        _log(SERVICE, f"stop {service} failed: {e}\n{traceback.format_exc()}", level="ERROR")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/workers/{worker_id}/{action}")
async def worker_action(worker_id: str, action: str, session: SessionToken = Depends(require_auth)) -> Dict[str, Any]:
    normalized_action = str(action or "").strip().lower()
    if normalized_action not in {"start", "stop"}:
        raise HTTPException(status_code=404, detail=f"Unknown action: {action}")

    try:
        if normalized_action == "start":
            await _start_worker(worker_id)
        else:
            await _stop_worker(worker_id)
        await asyncio.sleep(0.1)
        item = await _find_worker(worker_id)
        if item is None:
            raise HTTPException(status_code=404, detail=f"Unknown worker: {worker_id}")
        return {"ok": True, "worker": item}
    except HTTPException:
        raise
    except Exception as e:
        _log(SERVICE, f"worker action failed ({worker_id}/{normalized_action}): {e}\n{traceback.format_exc()}", level="ERROR")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api-server/restart")
def restart_api_server(session: SessionToken = Depends(require_auth)) -> Dict[str, Any]:
    """Trigger in-process restart of the API server.

    Returns 200 immediately; the actual stop+restart happens 300 ms later in a
    daemon thread so the HTTP response has time to reach the browser before the
    process exits.
    """
    import os
    import signal
    import subprocess
    import sys
    import threading
    import time

    try:
        pbgdir = Path(PBGDIR)
        venv_python: Optional[str] = None
        for candidate in [
            pbgdir.parent / "venv_pbgui" / "bin" / "python",
            pbgdir.parent / "venv_pbgui312" / "bin" / "python",
            pbgdir.parent / "venv" / "bin" / "python",
        ]:
            if candidate.exists():
                venv_python = str(candidate)
                break
        if not venv_python:
            venv_python = sys.executable

        pid_file = pbgdir / "data" / "pid" / "api_server.pid"

        def _do_restart() -> None:
            time.sleep(0.3)  # let HTTP response reach the browser first
            pid_file.unlink(missing_ok=True)
            env = os.environ.copy()
            env["PBGUI_RESTART_DELAY"] = "3"
            subprocess.Popen(
                [venv_python, str(pbgdir / "PBApiServer.py")],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                close_fds=True,
                cwd=str(pbgdir),
                env=env,
            )
            os.kill(os.getpid(), signal.SIGTERM)

        _log(SERVICE, "[restart] restart requested by user", level="WARNING")
        threading.Thread(target=_do_restart, daemon=True).start()
        return {"ok": True, "message": "Restarting\u2026"}
    except Exception as e:
        _log(SERVICE, f"restart api-server failed: {e}\n{traceback.format_exc()}", level="ERROR")
        raise HTTPException(status_code=500, detail=str(e))


# ── PBRemote info + api-sync ─────────────────────────────────

@router.get("/pbremote/info")
def get_pbremote_info(session: SessionToken = Depends(require_auth)) -> Dict[str, Any]:
    """Return PBRemote config plus remote host state using worker-based API sync status."""
    try:
        from PBRemote import PBRemote
        obj = PBRemote()
        if obj.error:
            return {"error": obj.error, "configured": False}
        api_keys_mod = __import__("api.api_keys", fromlist=["_file_sync_worker"])
        fs_worker = getattr(api_keys_mod, "_file_sync_worker", None)
        if fs_worker is None:
            return {"error": "FileSyncWorker not initialized", "configured": False}
        sync_status = fs_worker.get_status()
        monitor_state = get_monitor_state_snapshot()
        connections = ((monitor_state.get("connections") or {}).get("connections") or {})
        system_state = monitor_state.get("system") or {}
        host_meta = monitor_state.get("host_meta") or {}
        sync_hosts = sync_status.get("hosts") or {}
        servers = []
        connected_hosts = set(sync_status.get("connected_hosts") or [])
        api_synced = True
        for hostname in sorted({*connections.keys(), *host_meta.keys(), *connected_hosts, *sync_hosts.keys()}):
            conn = connections.get(hostname) or {}
            meta = host_meta.get(hostname) or {}
            system = system_state.get(hostname) or {}
            status = str(conn.get("status") or "")
            online = status == "connected"
            sync_info = sync_hosts.get(hostname) or {}
            in_sync = sync_info.get("in_sync")
            if hostname in connected_hosts and in_sync is False:
                api_synced = False
            last_seen_ts = float(system.get("timestamp") or 0)
            servers.append({
                "name": hostname,
                "role": str(meta.get("role") or "unknown"),
                "online": online,
                "last_seen_s": (max(int(time.time() - last_seen_ts), 0) if last_seen_ts else None),
                "pbgui_version": str(meta.get("pbgv") or "N/A"),
                "api_in_sync": in_sync,
            })
        return {
            "configured": True,
            "error": None,
            "role": obj.role,
            "bucket": obj.bucket or "",
            "api_synced": api_synced,
            "servers": servers,
        }
    except Exception as e:
        _log(SERVICE, f"pbremote info failed: {e}", level="WARNING")
        return {"error": str(e), "configured": False}


@router.post("/pbremote/api-sync")
async def trigger_pbremote_api_sync(session: SessionToken = Depends(require_auth)) -> Dict[str, Any]:
    """Push local api-keys.json to connected VPS via SSH and report worker-based sync state."""
    try:
        api_keys_mod = __import__("api.api_keys", fromlist=["_file_sync_worker"])
        fs_worker = getattr(api_keys_mod, "_file_sync_worker", None)
        if fs_worker is None:
            return {"ok": False, "configured": False, "api_synced": True, "error": "FileSyncWorker not initialized"}
        results = await fs_worker.push_api_keys(dry_run=False, no_propagate=False)
        if isinstance(results, dict) and "error" in results and len(results) == 1:
            raise HTTPException(status_code=400, detail=results["error"])
        status = fs_worker.get_status()
        connected_hosts = status.get("connected_hosts") or []
        synced = all((status.get("hosts") or {}).get(hostname, {}).get("in_sync") is not False for hostname in connected_hosts)
        return {"ok": True, "configured": True, "api_synced": synced}
    except HTTPException:
        raise
    except Exception as e:
        _log(SERVICE, f"pbremote api-sync failed: {e}", level="ERROR")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/pbremote/instances")
def get_pbremote_instances(session: SessionToken = Depends(require_auth)) -> Dict[str, Any]:
    """Return the current instance list and system metrics per host from the VPS monitor store."""
    try:
        from api.vps import _monitor
        if _monitor is None:
            return {"instances": {}, "system": {}, "available": False}
        instances = dict(_monitor.store.instances)
        system = {h: m.to_dict() for h, m in _monitor.store.system.items()}
        return {"instances": instances, "system": system, "available": True}
    except Exception as e:
        _log(SERVICE, f"pbremote/instances failed: {e}", level="WARNING")
        return {"instances": {}, "system": {}, "available": False, "error": str(e)}


@router.get("/settings/pbremote/buckets")
def get_pbremote_buckets(session: SessionToken = Depends(require_auth)) -> Dict[str, Any]:
    """List available rclone remotes (buckets) and currently selected bucket."""
    try:
        from PBRemote import PBRemote
        obj = PBRemote()
        obj.fetch_buckets()
        return {
            "buckets": obj.buckets or [],
            "rclone_available": obj.rclone_installed,
            "selected": obj.bucket or "",
        }
    except Exception as e:
        return {"buckets": [], "rclone_available": False, "selected": "", "error": str(e)}


@router.get("/settings/pbremote/bucket-config")
def get_bucket_config(
    bucket: str = Query(..., description="Bucket name (e.g. 'mybucket:')"),
    session: SessionToken = Depends(require_auth),
) -> Dict[str, Any]:
    """Fetch rclone config for a specific bucket."""
    try:
        from PBRemote import PBRemote
        obj = PBRemote()
        obj.bucket = bucket
        cfg = obj.fetch_bucket_config()
        if cfg is None:
            return {"ok": False, "error": "Could not load bucket config"}
        return {
            "ok": True,
            "region": obj.bucket_region or "",
            "endpoint": obj.bucket_endpoint or "",
            "access_key_id": obj.bucket_access_key_id or "",
            "secret_access_key": obj.bucket_secret_access_key or "",
        }
    except Exception as e:
        _log(SERVICE, f"get bucket config: {e}", level="ERROR")
        return {"ok": False, "error": str(e)}


class BucketSaveRequest(BaseModel):
    bucket_name: str
    region: str = ""
    endpoint: str = ""
    access_key_id: str = ""
    secret_access_key: str = ""


@router.post("/settings/pbremote/bucket-save")
def save_bucket_config(
    body: BucketSaveRequest, session: SessionToken = Depends(require_auth)
) -> Dict[str, Any]:
    """Save (create/update) an rclone bucket config."""
    try:
        from PBRemote import PBRemote
        obj = PBRemote()
        name = body.bucket_name.strip()
        if not name:
            raise HTTPException(status_code=400, detail="Bucket name is required")
        obj.bucket = name + ":" if not name.endswith(":") else name
        obj.bucket_region = body.region
        obj.bucket_endpoint = body.endpoint
        obj.bucket_access_key_id = body.access_key_id
        obj.bucket_secret_access_key = body.secret_access_key
        ok, result = obj.save_bucket_config()
        if ok:
            # Update selected bucket in ini
            save_ini("pbremote", "bucket", obj.bucket)
            return {"ok": True, "message": result}
        return {"ok": False, "error": result}
    except HTTPException:
        raise
    except Exception as e:
        _log(SERVICE, f"save bucket config: {e}", level="ERROR")
        raise HTTPException(status_code=500, detail=str(e))


class BucketActionRequest(BaseModel):
    bucket: str


class BucketTestRequest(BaseModel):
    bucket: str
    region: str = ""
    endpoint: str = ""
    access_key_id: str = ""
    secret_access_key: str = ""


@router.post("/settings/pbremote/bucket-test")
def test_bucket(
    body: BucketTestRequest, session: SessionToken = Depends(require_auth)
) -> Dict[str, Any]:
    """Test rclone bucket connection using the credentials currently in the form."""
    import os, subprocess, tempfile
    try:
        bucket = body.bucket if body.bucket.endswith(':') else body.bucket + ':'
        bucket_name = bucket.rstrip(':')
        config_lines = [
            f'[{bucket_name}]',
            'type = s3',
            'provider = Synology',
            f'region = {body.region}',
            f'endpoint = {body.endpoint}',
            'no_check_bucket = true',
            f'access_key_id = {body.access_key_id}',
            f'secret_access_key = {body.secret_access_key}',
        ]
        with tempfile.NamedTemporaryFile(mode='w', suffix='.conf', delete=False) as tmp:
            tmp.write('\n'.join(config_lines) + '\n')
            tmp_path = tmp.name
        try:
            result = subprocess.run(
                ['rclone', '--config', tmp_path, 'ls', bucket],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=30
            )
            if result.returncode == 0:
                return {"ok": True, "message": result.stdout}
            return {"ok": False, "message": result.stderr}
        finally:
            os.unlink(tmp_path)
    except Exception as e:
        _log(SERVICE, f"test bucket: {e}", level="ERROR")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/settings/pbremote/bucket-delete")
def delete_bucket(
    body: BucketActionRequest, session: SessionToken = Depends(require_auth)
) -> Dict[str, Any]:
    """Delete an rclone bucket config."""
    try:
        from PBRemote import PBRemote
        obj = PBRemote()
        obj.bucket = body.bucket
        ok, result = obj.delete_bucket()
        if ok:
            # Clear selected bucket if it was the deleted one
            current = load_ini("pbremote", "bucket")
            if current and current.strip() == body.bucket.strip():
                save_ini("pbremote", "bucket", "")
        return {"ok": ok, "message": result}
    except Exception as e:
        _log(SERVICE, f"delete bucket: {e}", level="ERROR")
        raise HTTPException(status_code=500, detail=str(e))


# ── Monitor config ───────────────────────────────────────────

_MC_FIELDS = [
    'mem_warning_server', 'mem_error_server', 'swap_warning_server', 'swap_error_server',
    'disk_warning_server', 'disk_error_server', 'cpu_warning_server', 'cpu_error_server',
    'mem_warning_v7', 'mem_error_v7', 'swap_warning_v7', 'swap_error_v7',
    'cpu_warning_v7', 'cpu_error_v7', 'error_warning_v7', 'error_error_v7',
    'traceback_warning_v7', 'traceback_error_v7',
]


def _load_monitor_config_values() -> Dict[str, float]:
    from MonitorConfig import MonitorConfig

    mc = MonitorConfig()
    return {field: getattr(mc, field) for field in _MC_FIELDS}


def _save_monitor_config_values(values: Dict[str, float]) -> None:
    from MonitorConfig import MonitorConfig

    mc = MonitorConfig()
    for field in _MC_FIELDS:
        if field in values:
            setattr(mc, field, float(values[field]))
    mc.save_monitor_config()


@router.get("/settings/monitor-config")
def get_monitor_config(session: SessionToken = Depends(require_auth)) -> Dict[str, Any]:
    """Return all monitor threshold values."""
    return _load_monitor_config_values()


@router.post("/settings/monitor-config")
def save_monitor_config(
    body: Dict[str, float], session: SessionToken = Depends(require_auth)
) -> Dict[str, Any]:
    """Save monitor threshold values."""
    try:
        _save_monitor_config_values(body)
        return {"ok": True}
    except Exception as e:
        _log(SERVICE, f"save monitor config: {e}", level="ERROR")
        raise HTTPException(status_code=500, detail=str(e))


# ── Settings: PBMon ──────────────────────────────────────────

@router.get("/settings/pbmon")
def get_pbmon_settings(session: SessionToken = Depends(require_auth)) -> Dict[str, Any]:
    from PBMon import PBMon
    obj = PBMon()
    return {
        "telegram_token": obj.telegram_token or "",
        "telegram_chat_id": obj.telegram_chat_id or "",
    }


class PBMonSettings(BaseModel):
    telegram_token: str = ""
    telegram_chat_id: str = ""


@router.post("/settings/pbmon")
def save_pbmon_settings(
    body: PBMonSettings, session: SessionToken = Depends(require_auth)
) -> Dict[str, Any]:
    try:
        from PBMon import PBMon
        obj = PBMon()
        obj.telegram_token = body.telegram_token
        obj.telegram_chat_id = body.telegram_chat_id
        return {"ok": True}
    except Exception as e:
        _log(SERVICE, f"save pbmon settings: {e}", level="ERROR")
        raise HTTPException(status_code=500, detail=str(e))


# ── Settings: PBCoinData ─────────────────────────────────────

@router.get("/settings/pbcoindata/key-status")
def get_pbcoindata_key_status(session: SessionToken = Depends(require_auth)) -> Dict[str, Any]:
    """Call CMC /v1/key/info and return usage stats."""
    try:
        from PBCoinData import CoinData
        obj = CoinData()
        if not obj.api_key:
            return {"ok": False, "error": "No API key configured"}
        ok = obj.fetch_api_status()
        if ok:
            return {
                "ok": True,
                "credit_limit_monthly": getattr(obj, "credit_limit_monthly", None),
                "credits_used_day": getattr(obj, "credits_used_day", None),
                "credits_used_month": getattr(obj, "credits_used_month", None),
                "credits_left": getattr(obj, "credits_left", None),
                "credit_limit_monthly_reset_timestamp": getattr(obj, "credit_limit_monthly_reset_timestamp", None),
            }
        return {"ok": False, "error": getattr(obj, "api_error", "unknown error")}
    except Exception as e:
        _log(SERVICE, f"pbcoindata key-status: {e}", level="ERROR")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/settings/pbcoindata")
def get_pbcoindata_settings(session: SessionToken = Depends(require_auth)) -> Dict[str, Any]:
    from PBCoinData import CoinData
    obj = CoinData()
    return {
        "api_key": obj.api_key or "",
        "fetch_limit": obj.fetch_limit,
        "fetch_interval": obj.fetch_interval,
        "metadata_interval": obj.metadata_interval,
        "mapping_interval": obj.mapping_interval,
    }


class PBCoinDataSettings(BaseModel):
    api_key: str = ""
    fetch_limit: int = 5000
    fetch_interval: int = 24
    metadata_interval: int = 1
    mapping_interval: int = 24


@router.post("/settings/pbcoindata")
def save_pbcoindata_settings(
    body: PBCoinDataSettings, session: SessionToken = Depends(require_auth)
) -> Dict[str, Any]:
    try:
        from PBCoinData import CoinData
        obj = CoinData()
        obj.api_key = body.api_key
        obj.fetch_limit = body.fetch_limit
        obj.fetch_interval = body.fetch_interval
        obj.metadata_interval = body.metadata_interval
        obj.mapping_interval = body.mapping_interval
        obj.save_config()
        return {"ok": True}
    except Exception as e:
        _log(SERVICE, f"save pbcoindata settings: {e}", level="ERROR")
        raise HTTPException(status_code=500, detail=str(e))


# ── Settings: PBAPIServer ────────────────────────────────────

def _available_vps_hosts() -> List[str]:
    vps_dir = Path(f"{PBGDIR}/data/vpsmanager/hosts")
    hostnames: list[str] = []
    pattern = str(vps_dir / "*" / "*.json")
    for filepath in sorted(glob.glob(pattern)):
        try:
            with open(filepath, "r") as f:
                config = json.load(f)
            hostname = config.get("_hostname")
            if hostname:
                hostnames.append(hostname)
        except Exception:
            pass
    return sorted(set(hostnames))


@router.get("/settings/api-server")
def get_api_server_settings(session: SessionToken = Depends(require_auth)) -> Dict[str, Any]:
    mod = importlib.import_module("PBApiServer")
    obj = mod.PBApiServer()

    auto_restart_val = load_ini("vps_monitor", "auto_restart")
    auto_restart = auto_restart_val.lower() == "true" if auto_restart_val else True

    enabled_hosts_val = load_ini("vps_monitor", "enabled_hosts")
    enabled_hosts: list[str] = []
    if enabled_hosts_val and enabled_hosts_val.strip():
        enabled_hosts = [h.strip() for h in enabled_hosts_val.split(",") if h.strip()]

    return {
        "host": obj.host,
        "port": obj.port,
        "auto_restart": auto_restart,
        "enabled_hosts": enabled_hosts,
        "available_hosts": _available_vps_hosts(),
        "monitor_config": _load_monitor_config_values(),
    }


class APIServerSettings(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8000
    auto_restart: bool = True
    enabled_hosts: List[str] = []
    monitor_config: Dict[str, float] = Field(default_factory=dict)


@router.post("/settings/api-server")
def save_api_server_settings(
    body: APIServerSettings, session: SessionToken = Depends(require_auth)
) -> Dict[str, Any]:
    try:
        mod = importlib.import_module("PBApiServer")
        obj = mod.PBApiServer()
        obj.host = body.host
        obj.port = body.port
        save_ini("vps_monitor", "auto_restart", str(body.auto_restart))
        save_ini("vps_monitor", "enabled_hosts", ",".join(sorted(body.enabled_hosts)))
        _save_monitor_config_values(body.monitor_config)
        return {"ok": True}
    except Exception as e:
        _log(SERVICE, f"save api-server settings: {e}", level="ERROR")
        raise HTTPException(status_code=500, detail=str(e))


# ── Settings: PBData ─────────────────────────────────────────

def _read_ini_int(section: str, key: str, default: int) -> int:
    try:
        v = load_ini(section, key)
        s = str(v).strip() if v is not None else ""
        return int(float(s)) if s else default
    except Exception:
        return default


def _read_ini_float(section: str, key: str, default: float) -> float:
    try:
        v = load_ini(section, key)
        s = str(v).strip() if v is not None else ""
        return float(s) if s else default
    except Exception:
        return default


@router.get("/settings/pbdata")
def get_pbdata_settings(session: SessionToken = Depends(require_auth)) -> Dict[str, Any]:
    from Exchange import MAX_PRIVATE_WS_GLOBAL
    from User import Users
    import ast as _ast

    try:
        users = Users()
        all_users = users.list()
        valid = set(all_users)
    except Exception:
        all_users = []
        valid = set()

    # Read fetch_users and trades_users directly from ini (no PBData() instantiation)
    def _read_ini_list(key: str) -> list:
        try:
            raw = load_ini('pbdata', key)
            if not raw or not str(raw).strip():
                return []
            users_list = _ast.literal_eval(str(raw).strip())
            if not isinstance(users_list, list):
                return []
            return [u for u in users_list if u in valid]
        except Exception:
            return []

    fetch_users = _read_ini_list('fetch_users')
    trades_users = _read_ini_list('trades_users')

    # per-exchange overrides: read JSON from ini, merge with defaults
    default_by_ex = {'hyperliquid': 3.0, 'bybit': 3.0}
    try:
        raw = load_ini('pbdata', 'shared_rest_pause_by_exchange_json') or ''
        overrides = json.loads(raw) if raw.strip() else {}
        if isinstance(overrides, dict):
            default_by_ex.update({str(k): float(v) for k, v in overrides.items() if v is not None})
    except Exception:
        pass

    return {
        "fetch_users": fetch_users,
        "trades_users": trades_users,
        "all_users": all_users,
        "log_level": load_ini("pbdata", "log_level") or "INFO",
        "ws_max": _read_ini_int("pbdata", "ws_max", MAX_PRIVATE_WS_GLOBAL),
        "pollers_delay_seconds": _read_ini_int("pbdata", "pollers_delay_seconds", 60),
        "poll_interval_combined_seconds": _read_ini_int("pbdata", "poll_interval_combined_seconds", 90),
        "poll_interval_balance_seconds": _read_ini_int("pbdata", "poll_interval_balance_seconds", 300),
        "poll_interval_positions_seconds": _read_ini_int("pbdata", "poll_interval_positions_seconds", 300),
        "poll_interval_orders_seconds": _read_ini_int("pbdata", "poll_interval_orders_seconds", 60),
        "poll_interval_history_seconds": _read_ini_int("pbdata", "poll_interval_history_seconds", 300),
        "poll_interval_executions_seconds": _read_ini_int("pbdata", "poll_interval_executions_seconds", 1800),
        "shared_rest_user_pause_seconds": _read_ini_float("pbdata", "shared_rest_user_pause_seconds", 0.75),
        "shared_rest_pause_by_exchange": default_by_ex,
        "latest_1m_coin_pause_seconds": _read_ini_float("pbdata", "latest_1m_coin_pause_seconds", 2.0),
    }


class PBDataSettings(BaseModel):
    fetch_users: List[str] = []
    trades_users: List[str] = []
    log_level: str = "INFO"
    ws_max: int = 10
    pollers_delay_seconds: int = 60
    poll_interval_combined_seconds: int = 90
    poll_interval_balance_seconds: int = 300
    poll_interval_positions_seconds: int = 300
    poll_interval_orders_seconds: int = 60
    poll_interval_history_seconds: int = 300
    poll_interval_executions_seconds: int = 1800
    shared_rest_user_pause_seconds: float = 0.75
    shared_rest_pause_by_exchange: Dict[str, float] = {}
    latest_1m_coin_pause_seconds: float = 2.0


@router.post("/settings/pbdata")
def save_pbdata_settings(
    body: PBDataSettings, session: SessionToken = Depends(require_auth)
) -> Dict[str, Any]:
    try:
        from PBData import PBData
        # Use a lightweight PBData instance only for save_fetch_users/save_trades_users
        # (those methods write fetch_users/trades_users back to pbgui.ini).
        # Suppress _load_settings() side-effects by deferring until after ini writes.
        obj = PBData.__new__(PBData)
        # Minimal init state needed for save_fetch_users / save_trades_users
        obj._fetch_users = []
        obj._trades_users = []
        from User import Users
        try:
            obj.users = Users()
        except Exception:
            obj.users = None
        obj.fetch_users = body.fetch_users
        obj.trades_users = body.trades_users
        save_ini("pbdata", "log_level", "" if body.log_level == "NONE" else body.log_level)
        save_ini("pbdata", "ws_max", str(body.ws_max))
        save_ini("pbdata", "pollers_delay_seconds", str(body.pollers_delay_seconds))
        save_ini("pbdata", "poll_interval_combined_seconds", str(body.poll_interval_combined_seconds))
        save_ini("pbdata", "poll_interval_balance_seconds", str(body.poll_interval_balance_seconds))
        save_ini("pbdata", "poll_interval_positions_seconds", str(body.poll_interval_positions_seconds))
        save_ini("pbdata", "poll_interval_orders_seconds", str(body.poll_interval_orders_seconds))
        save_ini("pbdata", "poll_interval_history_seconds", str(body.poll_interval_history_seconds))
        save_ini("pbdata", "poll_interval_executions_seconds", str(body.poll_interval_executions_seconds))
        save_ini("pbdata", "shared_rest_user_pause_seconds", str(body.shared_rest_user_pause_seconds))
        save_ini("pbdata", "latest_1m_coin_pause_seconds", str(body.latest_1m_coin_pause_seconds))
        # Only store exchanges that differ from the global pause (overrides only)
        global_pause = body.shared_rest_user_pause_seconds
        overrides = {
            ex: v for ex, v in body.shared_rest_pause_by_exchange.items()
            if abs(v - global_pause) > 1e-9
        }
        save_ini("pbdata", "shared_rest_pause_by_exchange_json", json.dumps(overrides) if overrides else "{}")
        return {"ok": True}
    except Exception as e:
        _log(SERVICE, f"save pbdata settings: {e}", level="ERROR")
        raise HTTPException(status_code=500, detail=str(e))


# ── Fetch summary (PBData) ───────────────────────────────────

@router.get("/fetch-summary")
def get_fetch_summary(session: SessionToken = Depends(require_auth)) -> Dict[str, Any]:
    try:
        p = Path(f"{PBGDIR}/data/logs/fetch_summary.json")
        if p.exists():
            return json.loads(p.read_text())
        return {}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/prices-snapshot")
def get_prices_snapshot(session: SessionToken = Depends(require_auth)) -> Dict[str, Any]:
    """Return latest price per (symbol, exchange) from the prices DB table, filtered to active symbols."""
    import sqlite3 as _sqlite3
    try:
        # Build user→exchange map from api-keys
        user_exchange: Dict[str, str] = {}
        try:
            from User import Users as _Users
            _u = _Users()
            _u.load()
            for _usr in _u:
                if _usr.name and _usr.exchange:
                    user_exchange[_usr.name] = _usr.exchange
        except Exception:
            pass

        # Load active symbol list from fetch_summary.
        # 1. symbol_list present → filter by (symbol, exchange) pairs
        # 2. symbols>0 but no symbol_list (old PBData) → top-N most-recently-updated
        # 3. fetch_summary absent or symbols=0 → return empty
        active_symbols: Optional[List[str]] = None
        allowed_pairs: Optional[set] = None          # set of (symbol, exchange)
        top_n: Optional[int] = None
        fs_path = Path(f"{PBGDIR}/data/logs/fetch_summary.json")
        if fs_path.exists():
            try:
                fs = json.loads(fs_path.read_text())
                prices = fs.get("prices", {})
                total_active_count = sum(exd.get("symbols", 0) for exd in prices.values())
                sym_set: set = set()
                pair_set: set = set()
                has_symbol_list = False
                for exch_name, exch_data in prices.items():
                    if "symbol_list" in exch_data and exch_data["symbol_list"]:
                        has_symbol_list = True
                        for s in exch_data["symbol_list"]:
                            sym_set.add(s)
                            pair_set.add((s, exch_name))
                if has_symbol_list:
                    active_symbols = sorted(sym_set)
                    allowed_pairs = pair_set
                elif total_active_count == 0:
                    return {"rows": []}
                else:
                    top_n = total_active_count
            except Exception:
                pass
        else:
            return {"rows": []}

        db_path = Path(f"{PBGDIR}/data/pbgui.db")
        if not db_path.exists():
            return {"rows": []}
        with _sqlite3.connect(str(db_path), timeout=5) as conn:
            conn.row_factory = _sqlite3.Row
            cur = conn.cursor()
            if active_symbols:
                placeholders = ",".join("?" * len(active_symbols))
                cur.execute(
                    f"SELECT symbol, user, price, MAX(timestamp) AS ts FROM prices WHERE symbol IN ({placeholders}) GROUP BY symbol, user ORDER BY symbol, user",
                    active_symbols,
                )
            elif top_n:
                cur.execute(
                    "SELECT symbol, user, price, MAX(timestamp) AS ts FROM prices GROUP BY symbol, user ORDER BY ts DESC LIMIT ?",
                    (top_n,),
                )
            else:
                cur.execute(
                    "SELECT symbol, user, price, MAX(timestamp) AS ts FROM prices GROUP BY symbol, user ORDER BY symbol, user"
                )
            raw = [{"symbol": r["symbol"], "user": r["user"], "price": r["price"], "ts": r["ts"]} for r in cur.fetchall()]

        # Collapse to best price per (symbol, exchange) — keep MAX(ts)
        best: Dict[str, Dict] = {}
        for row in raw:
            exch = user_exchange.get(row["user"], "")
            key = row["symbol"] + "\x00" + exch
            if key not in best or row["ts"] > best[key]["ts"]:
                best[key] = {"symbol": row["symbol"], "exchange": exch, "price": row["price"], "ts": row["ts"]}

        # Filter to allowed (symbol, exchange) pairs if available
        if allowed_pairs:
            best = {k: v for k, v in best.items() if (v["symbol"], v["exchange"]) in allowed_pairs}

        rows = sorted(best.values(), key=lambda x: (x["symbol"], x["exchange"]))
        return {"rows": rows}
    except Exception as e:
        _log(SERVICE, f"prices-snapshot failed: {e}", level="WARNING")
        raise HTTPException(status_code=500, detail=str(e))


# ── Poller metrics (PBData) ──────────────────────────────────

@router.get("/poller-metrics")
def get_poller_metrics(session: SessionToken = Depends(require_auth)) -> Dict[str, Any]:
    try:
        p = Path(f"{PBGDIR}/data/logs/poller_metrics.json")
        if p.exists():
            return json.loads(p.read_text())
        return {}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Main page ────────────────────────────────────────────────

@router.get("/main_page", response_class=HTMLResponse)
def get_main_page(
    request: Request,
    st_base: str = Query(default="", description="Browser-visible Streamlit base URL"),
    session: SessionToken = Depends(require_auth),
) -> HTMLResponse:
    """Serve the standalone Services Monitor page with token injected server-side."""
    html_path = Path(__file__).parent.parent / "frontend" / "services_monitor.html"
    html = html_path.read_text(encoding="utf-8")

    scheme = request.url.scheme
    host = request.url.hostname or "127.0.0.1"
    port = request.url.port
    origin = f"{scheme}://{host}" + (f":{port}" if port else "")
    api_services_base = origin + "/api/services"
    ws_base = origin.replace("http://", "ws://").replace("https://", "wss://")

    html = html.replace('"%%TOKEN%%"', json.dumps(session.token))
    html = html.replace('"%%API_BASE%%"', json.dumps(api_services_base))
    html = html.replace('"%%WS_BASE%%"', json.dumps(ws_base))

    if not st_base:
        st_base = f"http://{host}:8501"
    html = html.replace('"%%ST_BASE%%"', json.dumps(st_base))

    from pbgui_func import PBGUI_VERSION  # local import to avoid circular
    from pbgui_purefunc import PBGUI_SERIAL
    html = html.replace('"%%VERSION%%"', json.dumps(PBGUI_VERSION))
    html = html.replace("%%VERSION%%", PBGUI_VERSION)
    html = html.replace('"%%SERIAL%%"', json.dumps(PBGUI_SERIAL))
    html = html.replace("%%SERIAL%%", PBGUI_SERIAL)

    nav_js = Path(__file__).parent.parent / "frontend" / "pbgui_nav.js"
    nav_hash = str(int(nav_js.stat().st_mtime)) if nav_js.exists() else PBGUI_VERSION
    html = html.replace("%%NAV_HASH%%", nav_hash)

    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})
