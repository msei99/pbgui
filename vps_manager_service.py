from __future__ import annotations

import asyncio
import getpass
import json
import re
import shlex
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from api.vps import get_monitor, get_monitor_state_snapshot
from logging_helpers import human_log as _log
from MonitorConfig import MonitorConfig
from PBCoinData import CoinData
from PBRemote import PBRemote
from pbgui_purefunc import get_git_remote_url, list_git_remotes, list_remote_git_branches, load_ini, save_ini
from vps_manager_core import PBGDIR, VPS, VPSManager, _install_dir_from_remote_pbgui_dir

SERVICE = "VPSManagerApi"

PB7_UPSTREAM_REMOTE_NAME = "origin"
PB7_UPSTREAM_REMOTE_URL = "https://github.com/enarjord/passivbot.git"
SWAP_OPTIONS = ["0", "1G", "1.5G", "2G", "2.5G", "3G", "4G", "5G", "6G", "8G"]
INIT_METHODS = ["root", "password", "private_key"]
SLAVE_HOST_LOGFILES = [
    "logs/PBCoinData.log",
    "logs/PBRun.log",
    "logs/PBRemote.log",
    "logs/PBData.log",
    "logs/PBMon.log",
    "logs/sync.log",
]

BASE_HOST_LOGFILES = [
    "logs/PBCoinData.log",
    "logs/PBRun.log",
    "logs/PBRemote.log",
    "logs/PBData.log",
    "logs/PBMon.log",
    "logs/PBGui.log",
    "logs/PBApiServer.log",
    "logs/FastAPI.log",
    "logs/VPSMonitor.log",
    "logs/VPSManagerApi.log",
    "logs/sync.log",
]


def _now_ts() -> int:
    return round(datetime.now().timestamp())


def _today_start_ts() -> int:
    return round(datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp())


def _valid_ipv4(value: str) -> bool:
    if not value:
        return False
    return bool(
        re.match(
            r"^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$",
            str(value).strip(),
        )
    )


def _short_commit(value: str | None) -> str:
    return str(value or "")[:7]


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _status_running(status: str | None) -> bool:
    normalized = str(status or "").strip().lower()
    return normalized not in {"", "none", "successful", "failed", "error", "timeout", "canceled", "cancelled"}


def _metric_level(value: float, warning: float, error: float, *, inverse: bool = False) -> str:
    if inverse:
        if value <= error:
            return "error"
        if value <= warning:
            return "warning"
        return "ok"
    if value >= error:
        return "error"
    if value >= warning:
        return "warning"
    return "ok"


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


class VPSManagerService:
    def __init__(self):
        self.vpsmanager = VPSManager()
        self.pbremote: PBRemote | None = None
        self.coindata: CoinData | None = None
        self.monitor_config = MonitorConfig()
        self._first_refresh_done = False
        self._api_sync_task: asyncio.Task | None = None
        self._vps_package_status_cache: dict[str, dict[str, Any]] = {}
        self.api_sync_state: dict[str, Any] = {
            "running": False,
            "remaining": 0,
            "unsynced_hosts": [],
            "started_at": 0,
            "deadline": 0,
            "error": "",
            "success": False,
        }

    def _ensure_pbremote(self, *, force_reinit: bool = False) -> PBRemote:
        if self.pbremote is None or force_reinit:
            self.pbremote = PBRemote()
        return self.pbremote

    def _ensure_coindata(self) -> CoinData:
        if self.coindata is None:
            self.coindata = CoinData()
        return self.coindata

    def _sync_vps_inventory(self) -> None:
        pattern = str(Path(f"{PBGDIR}/data/vpsmanager/hosts/*/*.json"))
        host_files = sorted(Path(path) for path in __import__("glob").glob(pattern, recursive=False))
        current = {item.hostname: item for item in self.vpsmanager.vpss if item.hostname}
        next_items: list[VPS] = []
        existing_hosts: set[str] = set()
        for host_file in host_files:
            loaded = VPS()
            loaded.load(str(host_file))
            if not loaded.hostname:
                continue
            existing_hosts.add(loaded.hostname)
            current_item = current.get(loaded.hostname)
            if current_item is None:
                next_items.append(loaded)
                continue
            if not (
                _status_running(current_item.init_status)
                or _status_running(current_item.setup_status)
                or _status_running(current_item.update_status)
            ):
                current_item.load(str(host_file))
            next_items.append(current_item)
        for item in self.vpsmanager.vpss:
            if item.hostname and item.hostname not in existing_hosts:
                if _status_running(item.init_status) or _status_running(item.setup_status) or _status_running(item.update_status):
                    next_items.append(item)
        self.vpsmanager.vpss = sorted(next_items, key=lambda entry: entry.hostname or "")
        if not _status_running(self.vpsmanager.update_status):
            self.vpsmanager.load_master()

    def refresh(self, *, force: bool = False) -> None:
        self._sync_vps_inventory()
        pbremote = self._ensure_pbremote(force_reinit=force and self.pbremote is not None and bool(self.pbremote.error))
        local_run = getattr(pbremote, "local_run", None)
        if local_run is None:
            self._first_refresh_done = True
            return

        try:
            local_run.load_versions()
            local_run.load_git_commits()
            if hasattr(local_run, "update_pb7_python_version"):
                local_run.update_pb7_python_version(force=force)
        except Exception as exc:
            _log(SERVICE, f"refresh local versions failed: {exc}", level="WARNING")

        stale = (_now_ts() - int(getattr(pbremote, "systemts", 0) or 0)) > 3600
        full_refresh = force or stale or not self._first_refresh_done
        if full_refresh:
            try:
                local_run.load_git_origin()
            except Exception as exc:
                _log(SERVICE, f"refresh git origin failed: {exc}", level="WARNING")
            try:
                local_run.load_versions_origin()
            except Exception as exc:
                _log(SERVICE, f"refresh origin versions failed: {exc}", level="WARNING")
            try:
                local_run.load_versions()
                local_run.load_git_commits()
            except Exception as exc:
                _log(SERVICE, f"refresh local commit data failed: {exc}", level="WARNING")
            try:
                if hasattr(local_run, "load_git_branches_history"):
                    local_run.load_git_branches_history()
                if hasattr(local_run, "load_pb7_branches_history"):
                    local_run.load_pb7_branches_history()
            except Exception as exc:
                _log(SERVICE, f"refresh branch history failed: {exc}", level="WARNING")
            try:
                local_run.has_upgrades()
                local_run.has_reboot()
            except Exception as exc:
                _log(SERVICE, f"refresh package status failed: {exc}", level="WARNING")
            pbremote.systemts = _now_ts()

        try:
            pbremote.update_remote_servers()
        except Exception as exc:
            _log(SERVICE, f"refresh remote servers failed: {exc}", level="WARNING")
        self._first_refresh_done = True

    def _get_monitor_state(self) -> dict[str, Any]:
        try:
            return get_monitor_state_snapshot()
        except Exception as exc:
            _log(SERVICE, f"monitor snapshot failed: {exc}", level="WARNING")
            return {
                "connections": {"connections": {}},
                "system": {},
                "instances": {},
                "v7_instances": {},
                "host_meta": {},
                "streams": {},
            }

    def _get_host_telemetry(self, monitor_state: dict[str, Any], hostname: str) -> dict[str, Any]:
        connections = ((monitor_state.get("connections") or {}).get("connections") or {})
        return {
            "hostname": hostname,
            "connection": connections.get(hostname) or {},
            "system": (monitor_state.get("system") or {}).get(hostname) or {},
            "instances": (monitor_state.get("instances") or {}).get(hostname) or [],
            "v7_instances": (monitor_state.get("v7_instances") or {}).get(hostname) or [],
            "meta": (monitor_state.get("host_meta") or {}).get(hostname) or {},
            "stream": (monitor_state.get("streams") or {}).get(hostname) or {},
        }

    def _host_online(self, host_state: dict[str, Any] | None) -> bool:
        if not host_state:
            return False
        status = str((host_state.get("connection") or {}).get("status") or "")
        return status == "connected"

    def _host_meta(self, host_state: dict[str, Any] | None) -> dict[str, Any]:
        return (host_state or {}).get("meta") or {}

    async def _refresh_remote_api_md5s(self, hostnames: list[str] | None = None) -> None:
        monitor = get_monitor()
        if not monitor:
            return
        targets = [name for name in (hostnames or [item.hostname for item in self.vpsmanager.vpss if item.hostname]) if name]
        if not targets:
            return
        results = await asyncio.gather(
            *(monitor.collect_host_meta_now(hostname) for hostname in targets),
            return_exceptions=True,
        )
        for hostname, result in zip(targets, results):
            if isinstance(result, Exception):
                _log(SERVICE, f"API sync metadata refresh failed for {hostname}: {result}", level="WARNING")

    async def start_api_sync(self) -> None:
        pbremote = self._ensure_pbremote()
        if self.api_sync_state.get("running"):
            return
        if pbremote.error:
            self.api_sync_state = {
                "running": False,
                "remaining": 0,
                "unsynced_hosts": [],
                "started_at": _now_ts(),
                "deadline": 0,
                "error": str(pbremote.error),
                "success": False,
            }
            return
        await asyncio.to_thread(pbremote.sync_api_up)
        await self._refresh_remote_api_md5s()
        started = _now_ts()
        self.api_sync_state = {
            "running": True,
            "remaining": len(self._unsynced_api_hosts(pbremote, self._get_monitor_state())),
            "unsynced_hosts": self._unsynced_api_hosts(pbremote),
            "started_at": started,
            "deadline": started + 180,
            "error": "",
            "success": False,
        }
        self._api_sync_task = asyncio.create_task(self._api_sync_loop(), name="vps-manager-api-sync")

    async def _api_sync_loop(self) -> None:
        try:
            while self.api_sync_state.get("running"):
                pbremote = self._ensure_pbremote()
                targets = list(self.api_sync_state.get("unsynced_hosts") or [])
                await self._refresh_remote_api_md5s(targets)
                synced = await asyncio.to_thread(pbremote.check_if_api_synced)
                monitor_state = self._get_monitor_state()
                unsynced_hosts = self._unsynced_api_hosts(pbremote, monitor_state)
                remaining = len(unsynced_hosts)
                self.api_sync_state["remaining"] = remaining
                self.api_sync_state["unsynced_hosts"] = unsynced_hosts
                if synced:
                    self.api_sync_state["running"] = False
                    self.api_sync_state["success"] = True
                    return
                if _now_ts() >= int(self.api_sync_state.get("deadline") or 0):
                    self.api_sync_state["running"] = False
                    self.api_sync_state["success"] = False
                    self.api_sync_state["error"] = "API sync timed out"
                    return
                await asyncio.sleep(1)
        finally:
            self._api_sync_task = None

    def _unsynced_api_hosts(self, pbremote: PBRemote | None = None,
                            monitor_state: dict[str, Any] | None = None) -> list[str]:
        pbremote = pbremote or self._ensure_pbremote()
        monitor_state = monitor_state or self._get_monitor_state()
        hosts: list[str] = []
        try:
            for item in self.vpsmanager.vpss:
                hostname = str(item.hostname or "")
                if not hostname:
                    continue
                host_state = self._get_host_telemetry(monitor_state, hostname)
                if not self._host_online(host_state):
                    continue
                api_md5 = str(self._host_meta(host_state).get("api_md5") or "")
                if api_md5 and api_md5 != pbremote.api_md5:
                    hosts.append(hostname)
        except Exception as exc:
            _log(SERVICE, f"unsynced host check failed: {exc}", level="WARNING")
        return hosts

    def build_state(self) -> dict[str, Any]:
        self.refresh(force=self._first_refresh_done)
        pbremote = self._ensure_pbremote()
        monitor_state = self._get_monitor_state()
        overview_rows = self._build_overview_rows(pbremote, monitor_state)
        import_candidates = self._build_import_candidates(pbremote)
        return {
            "config": {
                "master_name": getattr(pbremote, "name", "local"),
                "local_user": getpass.getuser(),
                "swap_options": SWAP_OPTIONS,
                "init_methods": INIT_METHODS,
            },
            "errors": self._build_errors(pbremote),
            "overview": {
                "rows": overview_rows,
                "import_candidates": import_candidates,
                "api_sync": self._build_api_sync_state(pbremote, monitor_state),
            },
        }

    def build_master_detail(self) -> dict[str, Any]:
        self.refresh(force=False)
        pbremote = self._ensure_pbremote()
        coindata = self._ensure_coindata()
        coindata_ok = False
        try:
            coindata_ok = coindata.fetch_api_status()
        except Exception:
            coindata_ok = False
        return {
            "kind": "master",
            "status": self._build_master_status(pbremote, coindata_ok),
            "branches": {
                "pbgui": self._build_master_pbgui_branch_state(pbremote),
                "pb7": self._build_master_pb7_branch_state(pbremote),
            },
            "monitor": self._build_monitor_payload(pbremote),
            "progress": self._build_master_progress(include_log=True),
        }

    def build_master_detail_quick(self) -> dict[str, Any]:
        pbremote = self._ensure_pbremote()
        return {
            "kind": "master",
            "status": self._build_master_status(pbremote, False),
            "branches": {
                "pbgui": self._build_master_pbgui_branch_state(pbremote),
                "pb7": self._build_master_pb7_branch_state(pbremote),
            },
            "monitor": self._build_monitor_payload(pbremote),
            "progress": self._build_master_progress(include_log=True),
        }

    def build_vps_detail(self, hostname: str, *, quick: bool = False) -> dict[str, Any]:
        if not quick:
            self.refresh(force=False)
        pbremote = self._ensure_pbremote()
        vps = self._require_vps(hostname)
        monitor_state = self._get_monitor_state()
        host_state = self._get_host_telemetry(monitor_state, hostname)
        coindata_ok = False
        if not quick:
            try:
                coindata = self._ensure_coindata()
                if vps.coinmarketcap_api_key:
                    old_key = coindata.api_key
                    coindata.api_key = vps.coinmarketcap_api_key
                    coindata_ok = coindata.fetch_api_status()
                    coindata.api_key = old_key
            except Exception:
                coindata_ok = False

        logfiles = list(BASE_HOST_LOGFILES)
        role = str(self._host_meta(host_state).get("role") or "slave")
        if role == "slave":
            logfiles = [f for f in logfiles if f in SLAVE_HOST_LOGFILES]
        monitor_payload = self._build_monitor_payload(host_state, hostname=hostname)
        logfiles.extend(monitor_payload.get("logfiles", []))
        return {
            "kind": "vps",
            "hostname": hostname,
            "status": self._build_vps_status(vps, host_state, pbremote, coindata_ok),
            "config": self._build_vps_config(vps),
            "branches": {
                "pbgui": self._build_vps_pbgui_branch_state(pbremote, host_state),
                "pb7": self._build_vps_pb7_branch_state(pbremote, host_state, hostname),
            },
            "monitor": monitor_payload,
            "progress": self._build_vps_progress(vps, include_logs=not quick),
            "logfiles": sorted(dict.fromkeys(logfiles)),
            "log_preview": {
                "filename": vps.logfilename or (logfiles[0] if logfiles else ""),
                "size_kb": int(vps.logsize or 50),
                "content": "" if quick else (vps.logfile or ""),
            },
        }

    def _build_errors(self, pbremote: PBRemote) -> list[str]:
        out: list[str] = []
        if pbremote.error:
            out.append(str(pbremote.error))
        if self.api_sync_state.get("error"):
            out.append(str(self.api_sync_state["error"]))
        return out

    def _build_api_sync_state(self, pbremote: PBRemote,
                              monitor_state: dict[str, Any]) -> dict[str, Any]:
        unsynced_hosts = self._unsynced_api_hosts(pbremote, monitor_state)
        in_sync = len(unsynced_hosts) == 0 and not self.api_sync_state.get("running")
        return {
            "running": bool(self.api_sync_state.get("running")),
            "in_sync": in_sync,
            "remaining": int(self.api_sync_state.get("remaining") or len(unsynced_hosts)),
            "unsynced_hosts": unsynced_hosts if not self.api_sync_state.get("running") else list(self.api_sync_state.get("unsynced_hosts") or []),
            "started_at": int(self.api_sync_state.get("started_at") or 0),
            "deadline": int(self.api_sync_state.get("deadline") or 0),
            "error": str(self.api_sync_state.get("error") or ""),
            "success": bool(self.api_sync_state.get("success")),
        }

    def _build_import_candidates(self, pbremote: PBRemote) -> list[dict[str, Any]]:
        existing = {item.hostname for item in self.vpsmanager.vpss if item.hostname}
        out: list[dict[str, Any]] = []
        default_user = getpass.getuser()
        for server in sorted(pbremote.remote_servers, key=lambda item: item.name):
            if server.role in ("slave", "master") and server.name not in existing:
                temp = VPS()
                temp.hostname = server.name
                out.append({
                    "hostname": server.name,
                    "ip_from_hosts": temp.fetch_vps_ip_from_hosts(),
                    "online": server.is_online(),
                    "role": server.role or "slave",
                    "default_user": default_user,
                })
        return out

    def _build_overview_rows(self, pbremote: PBRemote,
                             monitor_state: dict[str, Any]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = [self._build_master_overview_row(pbremote)]
        managed_hostnames: set[str] = set()
        for vps in sorted(self.vpsmanager.vpss, key=lambda item: item.hostname or ""):
            hostname = str(vps.hostname or "")
            if not hostname:
                continue
            managed_hostnames.add(hostname)
            host_state = self._get_host_telemetry(monitor_state, hostname)
            rows.append(self._build_vps_overview_row(pbremote, hostname, host_state))
        return rows

    def _build_master_overview_row(self, pbremote: PBRemote) -> dict[str, Any]:
        local_run = pbremote.local_run
        master_branch, master_commit = local_run.get_current_pbgui_status()
        master_branch = master_branch or "unknown"
        master_commit = master_commit or getattr(local_run, "pbgui_commit", "")
        master_pb7_branch, master_pb7_commit = local_run.get_current_pb7_status()
        master_pb7_branch = master_pb7_branch or "unknown"
        master_pb7_commit = master_pb7_commit or getattr(local_run, "pb7_commit", "")
        return {
            "name": f"{pbremote.name} (local)",
            "hostname": pbremote.name,
            "nav": "master",
            "online": pbremote.is_running(),
            "role": "master",
            "role_icon": "🧠",
            "start": datetime.fromtimestamp(getattr(pbremote, "boot", 0)).strftime("%Y-%m-%d %H:%M:%S"),
            "reboot_required": bool(getattr(local_run, "reboot", False)),
            "updates": getattr(local_run, "upgrades", "N/A"),
            "pbgui": f"{pbremote.pbgui_version}{'' if getattr(pbremote, 'pbgui_python', 'N/A') in (None, '', 'N/A') else ' /' + str(getattr(pbremote, 'pbgui_python'))}",
            "pbgui_branch": f"{master_branch} ({_short_commit(master_commit)})",
            "pbgui_github": self._build_master_pbgui_github_status(pbremote, master_branch, master_commit),
            "pb7": f"{pbremote.pb7_version}{'' if getattr(pbremote, 'pb7_python', 'N/A') in (None, '', 'N/A') else ' /' + str(getattr(pbremote, 'pb7_python'))}",
            "pb7_branch": f"{master_pb7_branch} ({_short_commit(master_pb7_commit)})",
            "pb7_github": self._build_master_pb7_github_status(pbremote, master_pb7_branch, master_pb7_commit),
            "api_sync": "✅",
        }

    def _build_vps_overview_row(self, pbremote: PBRemote,
                                hostname: str,
                                host_state: dict[str, Any]) -> dict[str, Any]:
        online = self._host_online(host_state)
        meta = self._host_meta(host_state)
        role = str(meta.get("role") or "slave")
        if role == "master":
            role_icon = "🧠"
        else:
            role_icon = "💻"
        api_md5 = str(meta.get("api_md5") or "")
        if api_md5 and api_md5 == pbremote.api_md5:
            api_sync = "✅"
        elif api_md5:
            api_sync = "❌"
        else:
            api_sync = "-"
        boot = _safe_int(meta.get("boot"))
        return {
            "name": hostname,
            "hostname": hostname,
            "nav": "vps",
            "online": online,
            "role": role,
            "role_icon": role_icon,
            "start": datetime.fromtimestamp(boot).strftime("%Y-%m-%d %H:%M:%S") if boot else "",
            "reboot_required": bool(meta.get("reboot", False)),
            "updates": meta.get("upgrades", "N/A"),
            "pbgui": f"{meta.get('pbgv', 'N/A')}{'' if meta.get('pbgpy', 'N/A') in (None, '', 'N/A') else ' /' + str(meta.get('pbgpy'))}",
            "pbgui_branch": f"{meta.get('pbgb', 'unknown')} ({_short_commit(meta.get('pbgc'))})",
            "pbgui_github": self._build_remote_pbgui_github_status(pbremote, host_state),
            "pb7": f"{meta.get('pb7v', 'N/A')}{'' if meta.get('pb7py', 'N/A') in (None, '', 'N/A') else ' /' + str(meta.get('pb7py'))}",
            "pb7_branch": f"{meta.get('pb7b', 'unknown')} ({_short_commit(meta.get('pb7c'))})",
            "pb7_github": self._build_remote_pb7_github_status(pbremote, host_state),
            "api_sync": api_sync,
            "rtd": min(self._build_remote_rtd(host_state), 9999),
        }

    def _build_peer_master_overview_row(self, pbremote: PBRemote, server) -> dict[str, Any]:
        host_state = {
            "meta": {
                "role": getattr(server, "role", "master"),
                "api_md5": getattr(server, "api_md5", ""),
                "reboot": bool(getattr(server, "reboot", False)),
                "upgrades": getattr(server, "upgrades", "N/A"),
                "pbgv": getattr(server, "pbgui_version", "N/A"),
                "pbgc": getattr(server, "pbgui_commit", ""),
                "pbgb": getattr(server, "pbgui_branch", "unknown"),
                "pbgpy": getattr(server, "pbgui_python", "N/A"),
                "pb7v": getattr(server, "pb7_version", "N/A"),
                "pb7c": getattr(server, "pb7_commit", ""),
                "pb7b": getattr(server, "pb7_branch", "unknown"),
                "pb7py": getattr(server, "pb7_python", "N/A"),
            }
        }
        api_md5 = str(getattr(server, "api_md5", "") or "")
        if api_md5 and server.is_api_md5_same(pbremote.api_md5):
            api_sync = "✅"
        elif api_md5:
            api_sync = "❌"
        else:
            api_sync = "-"
        return {
            "name": server.name,
            "hostname": server.name,
            "nav": "none",
            "online": bool(server.is_online()),
            "role": "master",
            "role_icon": "🧠",
            "start": datetime.fromtimestamp(server.boot).strftime("%Y-%m-%d %H:%M:%S") if getattr(server, "boot", 0) else "",
            "reboot_required": bool(getattr(server, "reboot", False)),
            "updates": getattr(server, "upgrades", "N/A"),
            "pbgui": f"{getattr(server, 'pbgui_version', 'N/A')}{'' if getattr(server, 'pbgui_python', 'N/A') in (None, '', 'N/A') else ' /' + str(getattr(server, 'pbgui_python'))}",
            "pbgui_branch": f"{getattr(server, 'pbgui_branch', 'unknown')} ({_short_commit(getattr(server, 'pbgui_commit', ''))})",
            "pbgui_github": self._build_remote_pbgui_github_status(pbremote, host_state),
            "pb7": f"{getattr(server, 'pb7_version', 'N/A')}{'' if getattr(server, 'pb7_python', 'N/A') in (None, '', 'N/A') else ' /' + str(getattr(server, 'pb7_python'))}",
            "pb7_branch": f"{getattr(server, 'pb7_branch', 'unknown')} ({_short_commit(getattr(server, 'pb7_commit', ''))})",
            "pb7_github": self._build_remote_pb7_github_status(pbremote, host_state),
            "api_sync": api_sync,
            "rtd": min(int(getattr(server, 'rtd', 9999) or 9999), 9999),
        }

    def _build_master_pbgui_github_status(self, pbremote: PBRemote, current_branch: str, current_commit: str) -> str:
        local_run = pbremote.local_run
        branches = getattr(local_run, "pbgui_branches_data", {}) or {}
        if current_branch != "unknown" and current_branch in branches and branches[current_branch]:
            origin_commit = branches[current_branch][0]["full"]
            if current_commit == origin_commit:
                return "✅"
            return f"❌ {local_run.pbgui_version} ({_short_commit(origin_commit)})"
        if current_branch == "main":
            if local_run.pbgui_version == getattr(local_run, "pbgui_version_origin", None) and current_commit == getattr(local_run, "pbgui_commit_origin", None):
                return "✅"
            return f"❌ {getattr(local_run, 'pbgui_version_origin', 'N/A')} ({_short_commit(getattr(local_run, 'pbgui_commit_origin', ''))})"
        return f"⚠️ {local_run.pbgui_version}"

    def _build_master_pb7_github_status(self, pbremote: PBRemote, current_branch: str, current_commit: str) -> str:
        local_run = pbremote.local_run
        branches = getattr(local_run, "pb7_branches_data", {}) or {}
        if current_branch in branches and branches[current_branch]:
            origin_commit = branches[current_branch][0]["full"]
            if current_commit == origin_commit:
                return "✅"
            return f"❌ {pbremote.pb7_version} ({_short_commit(origin_commit)})"
        if current_branch == "master":
            if local_run.pb7_version == getattr(local_run, "pb7_version_origin", None) and current_commit == getattr(local_run, "pb7_commit_origin", None):
                return "✅"
            return f"❌ {getattr(local_run, 'pb7_version_origin', 'N/A')} ({_short_commit(getattr(local_run, 'pb7_commit_origin', ''))})"
        return "⚠️ version"

    def _build_remote_pbgui_github_status(self, pbremote: PBRemote,
                                          host_state: dict[str, Any]) -> str:
        meta = self._host_meta(host_state)
        server_branch = str(meta.get("pbgb") or "unknown")
        server_commit = str(meta.get("pbgc") or "")
        server_version = str(meta.get("pbgv") or "N/A")
        branches = getattr(pbremote.local_run, "pbgui_branches_data", {}) or {}
        if server_branch != "unknown" and server_branch in branches and branches[server_branch]:
            origin_commit = branches[server_branch][0]["full"]
            if server_commit == origin_commit:
                return "✅"
            return f"❌ {server_version} ({_short_commit(origin_commit)})"
        if server_branch == "main":
            if server_version == pbremote.local_run.pbgui_version_origin and server_commit == pbremote.local_run.pbgui_commit_origin:
                return "✅"
            return f"❌ {pbremote.local_run.pbgui_version_origin} ({_short_commit(pbremote.local_run.pbgui_commit_origin)})"
        return f"⚠️ {server_version}"

    def _build_remote_pb7_github_status(self, pbremote: PBRemote,
                                        host_state: dict[str, Any]) -> str:
        meta = self._host_meta(host_state)
        server_branch = str(meta.get("pb7b") or "unknown")
        server_commit = str(meta.get("pb7c") or "")
        server_version = str(meta.get("pb7v") or "N/A")
        branches = getattr(pbremote.local_run, "pb7_branches_data", {}) or {}
        if server_branch != "unknown" and server_branch in branches and branches[server_branch]:
            origin_commit = branches[server_branch][0]["full"]
            if server_commit == origin_commit:
                return "✅"
            return f"❌ {server_version} ({_short_commit(origin_commit)})"
        if server_branch == "master":
            if server_version == pbremote.local_run.pb7_version_origin and server_commit == pbremote.local_run.pb7_commit_origin:
                return "✅"
            return f"❌ {pbremote.local_run.pb7_version_origin} ({_short_commit(pbremote.local_run.pb7_commit_origin)})"
        return f"⚠️ {server_version}"

    def _build_master_status(self, pbremote: PBRemote, coindata_ok: bool) -> dict[str, Any]:
        summary_row = self._build_master_overview_row(pbremote)
        local_coindata = getattr(pbremote.local_run, "coindata", None)
        return {
            "name": pbremote.name,
            "online": pbremote.is_online(),
            "rclone_ok": bool(pbremote.bucket),
            "coindata_ok": coindata_ok,
            "update_ok": self.vpsmanager.update_status == "successful",
            "update_ready": True,
            "pending_updates": summary_row.get("updates", "N/A"),
            "cmc_credits": getattr(local_coindata, "credits_left", None),
            "last_command": self.vpsmanager.command_text,
            "last_update": self.vpsmanager.last_update,
            "summary_row": summary_row,
        }

    def _build_vps_status(self, vps: VPS, host_state: dict[str, Any],
                          pbremote: PBRemote, coindata_ok: bool) -> dict[str, Any]:
        summary_row = self._build_vps_overview_row(pbremote, vps.hostname, host_state)
        live_package_status = self._get_live_vps_package_status(vps, host_state)
        if live_package_status:
            summary_row = dict(summary_row)
            if live_package_status.get("upgrades") not in (None, ""):
                summary_row["updates"] = live_package_status.get("upgrades")
            summary_row["reboot_required"] = bool(live_package_status.get("reboot", False))
        server = pbremote.find_server(vps.hostname)
        pbgui_github = self._build_remote_pbgui_github_status(pbremote, host_state)
        pb7_github = self._build_remote_pb7_github_status(pbremote, host_state)
        return {
            "hosts_ok": vps.is_vps_in_hosts(),
            "ssh_ok": vps.is_vps_ssh_open(),
            "init_ok": vps.init_status == "successful",
            "setup_ok": vps.setup_status == "successful",
            "update_ok": vps.update_status == "successful",
            "update_ready": bool(vps.user_pw),
            "pending_updates": summary_row.get("updates", "N/A"),
            "rclone_ok": bool(getattr(pbremote, "bucket", None)),
            "coindata_ok": coindata_ok,
            "cmc_credits": getattr(server, "cmc_credits", None) if server else None,
            "online": self._host_online(host_state),
            "last_command": vps.command_text,
            "last_update": vps.last_update,
            "last_setup": vps.last_setup,
            "last_init": vps.last_init,
            "install_dir": _install_dir_from_remote_pbgui_dir(vps.remote_pbgui_dir, vps.user),
            "summary_row": summary_row,
            "pbgui_update_available": pbgui_github.startswith("\u274c"),
            "pb7_update_available": pb7_github.startswith("\u274c"),
            "server_metrics": self._build_remote_server_metrics(vps.hostname, host_state),
        }

    def _get_live_vps_package_status(self, vps: VPS, host_state: dict[str, Any]) -> dict[str, Any] | None:
        hostname = str(vps.hostname or "")
        if not hostname:
            return None
        cached = self._vps_package_status_cache.get(hostname)
        fingerprint = (
            str(vps.command or ""),
            str(vps.update_status or ""),
            str(vps.last_update or ""),
        )
        now = time.time()
        if cached:
            cached_fingerprint = tuple(cached.get("fingerprint") or ())
            age = now - float(cached.get("checked_at") or 0)
            if cached_fingerprint == fingerprint and age < 120:
                return cached.get("data") or None

        if not self._host_online(host_state):
            return (cached or {}).get("data") or None
        if not getattr(vps, "user_pw", None):
            return (cached or {}).get("data") or None
        if _status_running(vps.init_status) or _status_running(vps.setup_status) or _status_running(vps.update_status):
            return (cached or {}).get("data") or None

        live = vps.fetch_package_status()
        if live is None:
            return (cached or {}).get("data") or None
        self._vps_package_status_cache[hostname] = {
            "fingerprint": fingerprint,
            "checked_at": now,
            "data": live,
        }
        return live

    def _build_remote_rtd(self, host_state: dict[str, Any]) -> int:
        system = (host_state or {}).get("system") or {}
        timestamp = _safe_float(system.get("timestamp"), 0.0)
        if not timestamp:
            return 0
        return max(int((time.time() - timestamp) * 1000), 0)

    def _build_remote_server_metrics(self, hostname: str,
                                     host_state: dict[str, Any]) -> dict[str, Any] | None:
        system = (host_state or {}).get("system") or {}
        meta = self._host_meta(host_state)
        if not system:
            return None
        boot = _safe_int(meta.get("boot"))
        return {
            "rtd": self._build_remote_rtd(host_state),
            "boot": datetime.fromtimestamp(boot).strftime("%Y-%m-%d %H:%M:%S") if boot else "",
            "cpu": _safe_float(system.get("cpu")),
            "mem": {
                "total_mb": _safe_int(_safe_float(system.get("mem_total")) / 1024 / 1024),
                "free_mb": _safe_int(_safe_float(system.get("mem_available")) / 1024 / 1024),
                "used_mb": _safe_int(_safe_float(system.get("mem_used")) / 1024 / 1024),
                "usage_pct": _safe_int(system.get("mem_percent")),
            },
            "disk": {
                "total_mb": _safe_int(_safe_float(system.get("disk_total")) / 1024 / 1024),
                "used_mb": _safe_int(_safe_float(system.get("disk_used")) / 1024 / 1024),
                "free_mb": _safe_int(_safe_float(system.get("disk_free")) / 1024 / 1024),
                "usage_pct": _safe_int(system.get("disk_percent")),
            },
            "swap": {
                "total_mb": _safe_int(_safe_float(system.get("swap_total")) / 1024 / 1024),
                "used_mb": _safe_int(_safe_float(system.get("swap_used")) / 1024 / 1024),
                "free_mb": _safe_int(_safe_float(system.get("swap_free")) / 1024 / 1024),
                "usage_pct": min(_safe_int(system.get("swap_percent")), 100),
            },
        }

    def _build_server_metrics(self, server) -> dict[str, Any] | None:
        if not server or not getattr(server, "mem", None) or not getattr(server, "disk", None) or not getattr(server, "swap", None):
            return None
        return {
            "rtd": int(getattr(server, "rtd", 0) or 0),
            "boot": datetime.fromtimestamp(server.boot).strftime("%Y-%m-%d %H:%M:%S") if getattr(server, "boot", 0) else "",
            "cpu": _safe_float(server.cpu),
            "mem": {
                "total_mb": _safe_int(server.mem[0] / 1024 / 1024),
                "free_mb": _safe_int(server.mem[1] / 1024 / 1024),
                "used_mb": _safe_int(server.mem[3] / 1024 / 1024),
                "usage_pct": _safe_int(server.mem[2]),
            },
            "disk": {
                "total_mb": _safe_int(server.disk[0] / 1024 / 1024),
                "used_mb": _safe_int(server.disk[1] / 1024 / 1024),
                "free_mb": _safe_int(server.disk[2] / 1024 / 1024),
                "usage_pct": _safe_int(server.disk[3]),
            },
            "swap": {
                "total_mb": _safe_int(server.swap[0] / 1024 / 1024),
                "used_mb": _safe_int(server.swap[1] / 1024 / 1024),
                "free_mb": _safe_int(server.swap[2] / 1024 / 1024),
                "usage_pct": min(_safe_int(server.swap[3]), 100),
            },
        }

    def _build_master_pbgui_branch_state(self, pbremote: PBRemote) -> dict[str, Any]:
        local_run = pbremote.local_run
        current_branch, current_commit = local_run.get_current_pbgui_status()
        return {
            "current_branch": current_branch or getattr(local_run, "pbgui_branch", "unknown"),
            "current_commit": current_commit or getattr(local_run, "pbgui_commit", ""),
            "branches": getattr(local_run, "pbgui_branches_data", {}) or {},
        }

    def _build_master_pb7_branch_state(self, pbremote: PBRemote) -> dict[str, Any]:
        local_run = pbremote.local_run
        current_branch, current_commit = local_run.get_current_pb7_status()
        repo_dir = getattr(local_run, "pb7dir", "")
        known_remotes = list_git_remotes(repo_dir) if repo_dir else []
        for opt in ("origin", "fork"):
            if opt not in known_remotes:
                known_remotes.append(opt)
        remote_urls = {name: get_git_remote_url(repo_dir, name) for name in known_remotes if repo_dir}
        default_remote_name = "fork" if "fork" in known_remotes else ("origin" if "origin" in known_remotes else (known_remotes[0] if known_remotes else ""))
        return {
            "current_branch": current_branch or getattr(local_run, "pb7_branch", "unknown"),
            "current_commit": current_commit or getattr(local_run, "pb7_commit", ""),
            "branches": getattr(local_run, "pb7_branches_data", {}) or {},
            "known_remotes": known_remotes,
            "remote_urls": remote_urls,
            "default_remote_name": default_remote_name,
            "upstream_remote_name": PB7_UPSTREAM_REMOTE_NAME,
            "upstream_remote_url": PB7_UPSTREAM_REMOTE_URL,
        }

    def _build_vps_pbgui_branch_state(self, pbremote: PBRemote,
                                      host_state: dict[str, Any]) -> dict[str, Any]:
        meta = self._host_meta(host_state)
        return {
            "current_branch": str(meta.get("pbgb") or "unknown"),
            "current_commit": str(meta.get("pbgc") or ""),
            "branches": getattr(pbremote.local_run, "pbgui_branches_data", {}) or {},
        }

    def _build_vps_pb7_branch_state(self, pbremote: PBRemote,
                                    host_state: dict[str, Any],
                                    hostname: str) -> dict[str, Any]:
        local_run = pbremote.local_run
        meta = self._host_meta(host_state)
        repo_dir = getattr(local_run, "pb7dir", "")
        known_remotes = list_git_remotes(repo_dir) if repo_dir else []
        for opt in ("origin", "fork"):
            if opt not in known_remotes:
                known_remotes.append(opt)
        remote_urls = {name: get_git_remote_url(repo_dir, name) for name in known_remotes if repo_dir}
        return {
            "hostname": hostname,
            "current_branch": str(meta.get("pb7b") or "unknown"),
            "current_commit": str(meta.get("pb7c") or ""),
            "branches": getattr(local_run, "pb7_branches_data", {}) or {},
            "known_remotes": known_remotes,
            "remote_urls": remote_urls,
            "default_remote_name": "origin" if "origin" in known_remotes else (known_remotes[0] if known_remotes else ""),
            "upstream_remote_name": PB7_UPSTREAM_REMOTE_NAME,
            "upstream_remote_url": PB7_UPSTREAM_REMOTE_URL,
        }

    def _build_master_progress(self, *, include_log: bool = False) -> dict[str, Any]:
        return {
            "command": self.vpsmanager.command,
            "command_text": self.vpsmanager.command_text,
            "status": self.vpsmanager.update_status,
            "last_update": self.vpsmanager.last_update,
            "log": self.vpsmanager.get_update_log_text() if include_log else "",
        }

    def _build_vps_progress(self, vps: VPS, *, include_logs: bool = False) -> dict[str, Any]:
        return {
            "hostname": vps.hostname,
            "command": vps.command,
            "command_text": vps.command_text,
            "init_status": vps.init_status,
            "setup_status": vps.setup_status,
            "update_status": vps.update_status,
            "last_init": vps.last_init,
            "last_setup": vps.last_setup,
            "last_update": vps.last_update,
            "init_log": vps.get_init_log_text() if include_logs else "",
            "setup_log": vps.get_setup_log_text() if include_logs else "",
            "update_log": vps.get_update_log_text() if include_logs else "",
        }

    def _build_vps_config(self, vps: VPS) -> dict[str, Any]:
        return {
            "hostname": vps.hostname,
            "ip": vps.ip or "",
            "user": vps.user or "",
            "user_pw": vps.user_pw or "",
            "swap": vps.swap or "0",
            "bucket": vps.bucket or "",
            "coinmarketcap_api_key": vps.coinmarketcap_api_key or "",
            "firewall": bool(vps.firewall),
            "firewall_ssh_port": int(vps.firewall_ssh_port or 22),
            "firewall_ssh_ips": vps.firewall_ssh_ips or "",
            "init_methode": vps.init_methode or "root",
            "remove_user": bool(vps.remove_user),
            "initial_root_pw": vps.initial_root_pw or "",
            "root_pw": vps.root_pw or "",
            "private_key_user": vps.private_key_user or "",
            "private_key_file": vps.private_key_file or "",
            "user_sudo": vps.user_sudo or "",
            "user_sudo_pw": vps.user_sudo_pw or "",
        }

    def _build_monitor_payload(self, server, hostname: str | None = None) -> dict[str, Any]:
        if hostname is not None:
            return self._build_remote_monitor_payload(hostname, server)
        payload = {"server": self._build_server_metrics(server), "v7": [], "v7_running": [], "multi": [], "single": [], "logfiles": []}
        if not server or not getattr(server, "monitor", None):
            payload["v7_running"] = self._build_running_v7_payload(server)
            for item in payload["v7_running"]:
                payload["logfiles"].append(f"run_v7/{item['name']}/passivbot.log")
            return payload
        cfg = self.monitor_config
        for monitor in server.monitor:
            swap_value = monitor["m"][9] / 1024 / 1024 if len(monitor["m"]) == 10 else 0.0
            start_ts = _safe_int(monitor["st"])
            item = {
                "server": server.name,
                "version": getattr(server, "pb7_version", "N/A"),
                "name": monitor["u"],
                "pb_version": monitor["p"],
                "start_time": datetime.fromtimestamp(start_ts).strftime("%Y-%m-%d %H:%M:%S") if start_ts else "",
                "memory_mb": round(monitor["m"][0] / 1024 / 1024, 2),
                "swap_mb": round(swap_value, 2),
                "cpu": round(_safe_float(monitor["c"]), 2),
                "pnls_today": _safe_int(monitor["ct"]),
                "pnl_today": _safe_float(monitor["pt"]),
                "pnls_yesterday": _safe_int(monitor["cy"]),
                "pnl_yesterday": _safe_float(monitor["py"]),
                "last_info": monitor["i"],
                "infos_today": _safe_int(monitor["it"]),
                "infos_yesterday": _safe_int(monitor["iy"]),
                "last_error": monitor["e"],
                "errors_today": _safe_int(monitor["et"]),
                "errors_yesterday": _safe_int(monitor["ey"]),
                "last_traceback": monitor["t"],
                "tracebacks_today": _safe_int(monitor["tt"]),
                "tracebacks_yesterday": _safe_int(monitor["ty"]),
            }
            if item["pb_version"] == "7":
                item["levels"] = {
                    "cpu": _metric_level(item["cpu"], cfg.cpu_warning_v7, cfg.cpu_error_v7),
                    "memory": _metric_level(item["memory_mb"], cfg.mem_warning_v7, cfg.mem_error_v7),
                    "swap": _metric_level(item["swap_mb"], cfg.swap_warning_v7, cfg.swap_error_v7),
                    "errors": _metric_level(item["errors_today"], cfg.error_warning_v7, cfg.error_error_v7),
                    "tracebacks": _metric_level(item["tracebacks_today"], cfg.traceback_warning_v7, cfg.traceback_error_v7),
                }
                payload["v7"].append(item)
                payload["logfiles"].append(f"run_v7/{item['name']}/passivbot.log")
            elif item["pb_version"] == "6":
                item["levels"] = {
                    "cpu": _metric_level(item["cpu"], cfg.cpu_warning_multi, cfg.cpu_error_multi),
                    "memory": _metric_level(item["memory_mb"], cfg.mem_warning_multi, cfg.mem_error_multi),
                    "swap": _metric_level(item["swap_mb"], cfg.swap_warning_multi, cfg.swap_error_multi),
                    "errors": _metric_level(item["errors_today"], cfg.error_warning_multi, cfg.error_error_multi),
                    "tracebacks": _metric_level(item["tracebacks_today"], cfg.traceback_warning_multi, cfg.traceback_error_multi),
                }
                payload["multi"].append(item)
                payload["logfiles"].append(f"multi/{item['name']}/passivbot.log")
            elif item["pb_version"] == "s":
                item["levels"] = {
                    "cpu": _metric_level(item["cpu"], cfg.cpu_warning_single, cfg.cpu_error_single),
                    "memory": _metric_level(item["memory_mb"], cfg.mem_warning_single, cfg.mem_error_single),
                    "swap": _metric_level(item["swap_mb"], cfg.swap_warning_single, cfg.swap_error_single),
                    "errors": _metric_level(item["errors_today"], cfg.error_warning_single, cfg.error_error_single),
                    "tracebacks": _metric_level(item["tracebacks_today"], cfg.traceback_warning_single, cfg.traceback_error_single),
                }
                payload["single"].append(item)
                payload["logfiles"].append(f"instances/{item['name']}/passivbot.log")
        existing_v7_names = {item["name"] for item in payload["v7"] if item.get("name")}
        payload["v7_running"] = self._build_running_v7_payload(server, existing_v7_names)
        for item in payload["v7_running"]:
            payload["logfiles"].append(f"run_v7/{item['name']}/passivbot.log")
        return payload

    def _build_remote_monitor_payload(self, hostname: str,
                                      host_state: dict[str, Any]) -> dict[str, Any]:
        payload = {
            "server": self._build_remote_server_metrics(hostname, host_state),
            "v7": [],
            "v7_running": [],
            "multi": [],
            "single": [],
            "logfiles": [],
        }
        cfg = self.monitor_config
        meta = self._host_meta(host_state)
        for monitor in (host_state or {}).get("instances") or []:
            metrics = monitor.get("m") or []
            swap_value = metrics[9] / 1024 / 1024 if len(metrics) == 10 else 0.0
            start_ts = _safe_int(monitor.get("st"))
            item = {
                "server": hostname,
                "version": meta.get("pb7v", "N/A"),
                "name": monitor.get("u", ""),
                "pb_version": monitor.get("p", ""),
                "start_time": datetime.fromtimestamp(start_ts).strftime("%Y-%m-%d %H:%M:%S") if start_ts else "",
                "memory_mb": round(_safe_float(metrics[0]) / 1024 / 1024, 2) if metrics else 0.0,
                "swap_mb": round(swap_value, 2),
                "cpu": round(_safe_float(monitor.get("c")), 2),
                "pnls_today": _safe_int(monitor.get("ct")),
                "pnl_today": _safe_float(monitor.get("pt")),
                "pnls_yesterday": _safe_int(monitor.get("cy")),
                "pnl_yesterday": _safe_float(monitor.get("py")),
                "last_info": monitor.get("i", ""),
                "infos_today": _safe_int(monitor.get("it")),
                "infos_yesterday": _safe_int(monitor.get("iy")),
                "last_error": monitor.get("e", ""),
                "errors_today": _safe_int(monitor.get("et")),
                "errors_yesterday": _safe_int(monitor.get("ey")),
                "last_traceback": monitor.get("t", ""),
                "tracebacks_today": _safe_int(monitor.get("tt")),
                "tracebacks_yesterday": _safe_int(monitor.get("ty")),
            }
            if item["pb_version"] == "7":
                item["levels"] = {
                    "cpu": _metric_level(item["cpu"], cfg.cpu_warning_v7, cfg.cpu_error_v7),
                    "memory": _metric_level(item["memory_mb"], cfg.mem_warning_v7, cfg.mem_error_v7),
                    "swap": _metric_level(item["swap_mb"], cfg.swap_warning_v7, cfg.swap_error_v7),
                    "errors": _metric_level(item["errors_today"], cfg.error_warning_v7, cfg.error_error_v7),
                    "tracebacks": _metric_level(item["tracebacks_today"], cfg.traceback_warning_v7, cfg.traceback_error_v7),
                }
                payload["v7"].append(item)
                if item["name"]:
                    payload["logfiles"].append(f"run_v7/{item['name']}/passivbot.log")

        existing_v7_names = {item["name"] for item in payload["v7"] if item.get("name")}
        payload["v7_running"] = self._build_running_v7_payload_from_telemetry(host_state, existing_v7_names)
        for item in payload["v7_running"]:
            if item.get("name"):
                payload["logfiles"].append(f"run_v7/{item['name']}/passivbot.log")
        return payload

    def _build_running_v7_payload_from_telemetry(self, host_state: dict[str, Any],
                                                 existing_names: set[str] | None = None) -> list[dict[str, Any]]:
        known_names = existing_names or set()
        items: list[dict[str, Any]] = []
        for instance in (host_state or {}).get("v7_instances") or []:
            if not _truthy(instance.get("running")):
                continue
            name = str(instance.get("name") or "")
            if not name or name in known_names:
                continue
            items.append(
                {
                    "name": name,
                    "version": _safe_int(instance.get("cv")),
                    "enabled_on": str(instance.get("eo") or ""),
                    "activate_ts": "",
                }
            )
        items.sort(key=lambda item: item["name"])
        return items

    def _build_running_v7_payload(self, server, existing_names: set[str] | None = None) -> list[dict[str, Any]]:
        status_v7 = getattr(server, "instances_status_v7", None)
        if not status_v7:
            return []
        known_names = existing_names or set()
        items: list[dict[str, Any]] = []
        for instance in getattr(status_v7, "instances", []) or []:
            if not getattr(instance, "running", False):
                continue
            name = str(getattr(instance, "name", "") or "")
            if not name or name in known_names:
                continue
            activate_ts = _safe_int(getattr(instance, "activate_ts", 0))
            items.append(
                {
                    "name": name,
                    "version": _safe_int(getattr(instance, "version", 0)),
                    "enabled_on": str(getattr(instance, "enabled_on", "") or ""),
                    "activate_ts": datetime.fromtimestamp(activate_ts).strftime("%Y-%m-%d %H:%M:%S") if activate_ts else "",
                }
            )
        items.sort(key=lambda item: item["name"])
        return items

    def _require_vps(self, hostname: str) -> VPS:
        vps = self.vpsmanager.find_vps_by_hostname(hostname)
        if not vps:
            raise ValueError(f"Unknown VPS: {hostname}")
        return vps

    def load_more_commits(self, repo: str, branch_name: str, limit: int) -> None:
        pbremote = self._ensure_pbremote()
        if repo == "pbgui":
            pbremote.local_run.load_more_commits(branch_name, int(limit))
        elif repo == "pb7":
            pbremote.local_run.load_more_pb7_commits(branch_name, int(limit))
        else:
            raise ValueError(f"Unknown repo: {repo}")

    def load_remote_branches(self, remote_url: str) -> list[str]:
        if not remote_url:
            return []
        return list_remote_git_branches(remote_url)

    def run_master_command(self, *, command: str, command_text: str, debug: bool = False, sudo_pw: str | None = None, extra_vars: dict[str, Any] | None = None) -> None:
        self.vpsmanager.command = command
        self.vpsmanager.command_text = command_text
        self.vpsmanager.update_master(debug=debug, sudo_pw=sudo_pw, extra_vars=extra_vars)

    def run_vps_command(self, *, hostname: str, command: str, command_text: str, debug: bool = False, extra_vars: dict[str, Any] | None = None) -> None:
        vps = self._require_vps(hostname)
        vps.command = command
        vps.command_text = command_text
        self.vpsmanager.update_vps(vps, debug=debug, extra_vars=extra_vars)

    def delete_vps(self, hostname: str) -> None:
        vps = self._require_vps(hostname)
        vps.delete()
        self.vpsmanager.vpss = [item for item in self.vpsmanager.vpss if item.hostname != hostname]
        self._set_vps_monitor_enabled(hostname, enabled=False)

    def read_vps_settings(self, hostname: str) -> dict[str, Any]:
        pbremote = self._ensure_pbremote()
        vps = self._require_vps(hostname)
        if not vps.can_login_ssh():
            raise ValueError("Cannot login via SSH. Please check username and password.")
        vps.bucket = pbremote.bucket
        info = vps.fetch_vps_info()
        vps.coinmarketcap_api_key = info["coinmarketcap"]
        vps.swap = info.get("swap", "0") if info.get("swap") in SWAP_OPTIONS else "0"
        vps.firewall, vps.firewall_ssh_ips = vps.fetch_ufw_settings()
        vps.save()
        return self._build_vps_config(vps)

    def save_vps(self, form: dict[str, Any]) -> dict[str, Any]:
        vps, is_new = self._hydrate_vps_from_form(form, allow_create=True)
        self._apply_vps_setup_form(vps, form)
        vps.save()
        self._set_vps_monitor_enabled(vps.hostname, enabled=True)
        if is_new:
            self.vpsmanager.vpss.append(vps)
            self.vpsmanager.vpss.sort(key=lambda item: item.hostname or "")
        return self._build_vps_config(vps)

    def save_vps_config(self, hostname: str, form: dict[str, Any]) -> dict[str, Any]:
        vps = self._require_vps(hostname)
        self._apply_vps_setup_form(vps, form)
        vps.save()
        self._set_vps_monitor_enabled(vps.hostname, enabled=True)
        return self._build_vps_config(vps)

    def init_vps(self, form: dict[str, Any], *, debug: bool = False) -> dict[str, Any]:
        vps, is_new = self._hydrate_vps_from_form(form, allow_create=True)
        if not vps.has_init_parameters():
            raise ValueError("Init parameters are incomplete.")
        self._set_vps_monitor_enabled(vps.hostname, enabled=True)
        self.vpsmanager.init_vps(vps, debug=debug)
        if is_new:
            self.vpsmanager.vpss.append(vps)
            self.vpsmanager.vpss.sort(key=lambda item: item.hostname or "")
        return self._build_vps_progress(vps, include_logs=True)

    def _set_vps_monitor_enabled(self, hostname: str, *, enabled: bool) -> None:
        hostname = str(hostname or "").strip()
        if not hostname:
            return
        current = str(load_ini("vps_monitor", "enabled_hosts") or "")
        hosts = {item.strip() for item in current.split(",") if item.strip()}
        if enabled:
            hosts.add(hostname)
        else:
            hosts.discard(hostname)
        save_ini("vps_monitor", "enabled_hosts", ",".join(sorted(hosts)))

    def setup_vps(self, hostname: str, form: dict[str, Any], *, debug: bool = False) -> dict[str, Any]:
        vps = self._require_vps(hostname)
        self._apply_vps_setup_form(vps, form)
        if not vps.has_setup_parameters():
            raise ValueError("Setup parameters are incomplete.")
        self.vpsmanager.setup_vps(vps, debug=debug)
        return self._build_vps_progress(vps, include_logs=True)

    def fetch_vps_log(self, hostname: str, *, filename: str, size_kb: int, reverse: bool = True, debug: bool = False) -> dict[str, Any]:
        vps = self._require_vps(hostname)
        vps.logfilename = filename
        vps.logsize = int(size_kb)
        vps.command = "vps-fetch-logfile"
        vps.command_text = f"Fetch logfile {filename}"
        self.vpsmanager.fetch_log(vps, debug=debug)
        content = vps.logfile or ""
        if reverse:
            content = "\n".join(content.splitlines()[::-1])
        return {"filename": filename, "size_kb": int(size_kb), "content": content}

    def _hydrate_vps_from_form(self, form: dict[str, Any], *, allow_create: bool) -> tuple[VPS, bool]:
        hostname = str(form.get("hostname") or "").strip()
        if not hostname:
            raise ValueError("Hostname is required.")
        vps = self.vpsmanager.find_vps_by_hostname(hostname)
        is_new = vps is None
        if is_new:
            if not allow_create:
                raise ValueError(f"Unknown VPS: {hostname}")
            vps = VPS()
            vps.hostname = hostname
        self._apply_vps_full_form(vps, form, is_new=is_new)
        return vps, is_new

    def _apply_vps_full_form(self, vps: VPS, form: dict[str, Any], *, is_new: bool) -> None:
        master_name = str(self._ensure_pbremote().name or "").strip()
        ip = str(form.get("ip") or "").strip()
        if ip and not _valid_ipv4(ip):
            raise ValueError("IP address is not valid.")
        hostname = str(form.get("hostname") or vps.hostname or "").strip()
        if hostname == master_name:
            raise ValueError("Hostname is equal to master, use another hostname.")
        if is_new and hostname in self.vpsmanager.list():
            raise ValueError("Hostname already exists.")
        init_methode = str(form.get("init_methode") or "root").strip()
        if init_methode not in INIT_METHODS:
            raise ValueError("Invalid init method.")

        for field_name in ("root_pw", "user_sudo_pw", "user_pw"):
            value = str(form.get(field_name) or "")
            if value and ("{{" in value or "}}" in value):
                raise ValueError(f"{field_name} contains '{{{{' or '}}}}'.")

        vps.hostname = hostname
        vps.ip = ip or vps.ip
        vps.init_methode = init_methode
        vps.remove_user = _truthy(form.get("remove_user"))
        vps.initial_root_pw = str(form.get("initial_root_pw") or "")
        vps.root_pw = str(form.get("root_pw") or "")
        vps.private_key_user = str(form.get("private_key_user") or "")
        vps.private_key_file = str(form.get("private_key_file") or "")
        vps.user_sudo = str(form.get("user_sudo") or "")
        vps.user_sudo_pw = str(form.get("user_sudo_pw") or "")
        vps.user = str(form.get("user") or vps.user or "")
        vps.user_pw = str(form.get("user_pw") or "")

    def _apply_vps_setup_form(self, vps: VPS, form: dict[str, Any]) -> None:
        user_pw = str(form.get("user_pw") or vps.user_pw or "")
        if user_pw and ("{{" in user_pw or "}}" in user_pw):
            raise ValueError("user_pw contains '{{' or '}}'.")
        swap = str(form.get("swap") or vps.swap or "0")
        if swap not in SWAP_OPTIONS:
            raise ValueError("Invalid swap size.")
        firewall_ips = str(form.get("firewall_ssh_ips") or "").strip()
        if firewall_ips:
            for ip in [part.strip() for part in firewall_ips.split(",") if part.strip()]:
                if not _valid_ipv4(ip):
                    raise ValueError("IP-Addresses to allow contains an invalid IPv4 address.")
        vps.user_pw = user_pw
        vps.swap = swap
        vps.bucket = str(form.get("bucket") or vps.bucket or "")
        vps.coinmarketcap_api_key = str(form.get("coinmarketcap_api_key") or vps.coinmarketcap_api_key or "")
        vps.firewall = _truthy(form.get("firewall"))
        vps.firewall_ssh_port = _safe_int(form.get("firewall_ssh_port"), 22)
        vps.firewall_ssh_ips = firewall_ips
        vps.save()
