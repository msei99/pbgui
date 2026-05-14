from __future__ import annotations

import asyncio
import getpass
import json
import os
import re
import shlex
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from api.vps import get_monitor, get_monitor_state_snapshot, get_metric_history_snapshot
from logging_helpers import human_log as _log
from master.async_monitor import INSTANCE_COLLECT_SCRIPT, MONITOR_CACHE_VERSION
from MonitorConfig import MonitorConfig
from PBCoinData import CoinData
from PBRemote import PBRemote
from pbgui_purefunc import get_git_remote_url, list_git_remotes, list_remote_git_branch_commits, list_remote_git_branches, load_ini, save_ini
from vps_manager_core import PBGDIR, VPS, VPSManager, _install_dir_from_remote_pbgui_dir

SERVICE = "VPSManagerApi"

PB7_UPSTREAM_REMOTE_NAME = "origin"
PB7_UPSTREAM_REMOTE_URL = "https://github.com/enarjord/passivbot.git"
SWAP_OPTIONS = ["0", "1G", "1.5G", "2G", "2.5G", "3G", "4G", "5G", "6G", "8G"]
INIT_METHODS = ["root", "password", "private_key"]
SESSION_SECRET_TTL_SECONDS = 15 * 60
# Guardrail: every field listed here is sensitive bootstrap/auth material and
# must never be written to host JSON or included in normal config/detail payloads.
SECRET_FIELDS = (
    "user_pw",
    "initial_root_pw",
    "root_pw",
    "user_sudo",
    "user_sudo_pw",
    "private_key_user",
    "private_key_file",
)

ROLLING_PEAK_WINDOW_SECONDS = 60.0


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


def _get_file_sync_worker():
    try:
        from api.api_keys import _file_sync_worker
        return _file_sync_worker
    except Exception:
        return None


class VPSManagerService:
    def __init__(self):
        self.vpsmanager = VPSManager()
        self.pbremote: PBRemote | None = None
        self.coindata: CoinData | None = None
        self.monitor_config = MonitorConfig()
        self._first_refresh_done = False
        self._api_sync_task: asyncio.Task | None = None
        self._vps_package_status_cache: dict[str, dict[str, Any]] = {}
        # Quick detail is pushed every second. Any status that requires a slower
        # validation step must reuse the last full-detail result instead of
        # falling back to a weaker default on the next quick push.
        self._master_coindata_ok_cache: bool = False
        self._master_monitor_payload_cache: dict[str, Any] | None = None
        self._master_monitor_cache: dict[str, Any] = {"_version": MONITOR_CACHE_VERSION}
        self._master_bot_cpu_history: dict[str, dict[str, Any]] = {}
        self._master_server_metric_history: dict[str, list[tuple[float, float]]] = {
            "memory": [],
            "disk": [],
            "swap": [],
        }
        self._vps_coindata_status_cache: dict[str, bool] = {}
        self._vps_ssh_ok_cache: dict[str, bool] = {}
        self._session_secrets: dict[str, dict[str, dict[str, dict[str, Any]]]] = {}
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

    def clear_session_secrets(self, token: str) -> None:
        token = str(token or "").strip()
        if token:
            self._session_secrets.pop(token, None)

    def prune_session_secrets(self, valid_tokens: set[str] | None = None) -> None:
        now = _now_ts()
        next_store: dict[str, dict[str, dict[str, dict[str, Any]]]] = {}
        for token, host_map in self._session_secrets.items():
            if valid_tokens is not None and token not in valid_tokens:
                continue
            next_host_map: dict[str, dict[str, dict[str, Any]]] = {}
            for hostname, field_map in host_map.items():
                next_field_map: dict[str, dict[str, Any]] = {}
                for field_name, payload in field_map.items():
                    if payload.get("value") and int(payload.get("expires_at") or 0) > now:
                        next_field_map[field_name] = payload
                if next_field_map:
                    next_host_map[hostname] = next_field_map
            if next_host_map:
                next_store[token] = next_host_map
        self._session_secrets = next_store

    def _get_secret_bucket(self, token: str, hostname: str, *, create: bool = False) -> dict[str, dict[str, Any]] | None:
        token = str(token or "").strip()
        hostname = str(hostname or "").strip()
        if not token or not hostname:
            return None
        self.prune_session_secrets()
        host_map = self._session_secrets.get(token)
        if host_map is None:
            if not create:
                return None
            host_map = {}
            self._session_secrets[token] = host_map
        bucket = host_map.get(hostname)
        if bucket is None and create:
            bucket = {}
            host_map[hostname] = bucket
        return bucket

    def _session_secret_payload(self, value: str) -> dict[str, Any]:
        now = _now_ts()
        return {
            "value": value,
            "stored_at": now,
            "expires_at": now + SESSION_SECRET_TTL_SECONDS,
        }

    def _store_session_secrets(self, token: str, hostname: str, form: dict[str, Any]) -> None:
        bucket = self._get_secret_bucket(token, hostname, create=True)
        if bucket is None:
            return
        # Only the token-scoped in-memory store may retain these fields.
        # Do not mirror them into persisted VPS config or generic API payloads.
        for field_name in SECRET_FIELDS:
            if field_name not in form:
                continue
            value = str(form.get(field_name) or "")
            if value:
                bucket[field_name] = self._session_secret_payload(value)
            else:
                bucket.pop(field_name, None)
        if not bucket:
            host_map = self._session_secrets.get(str(token or "").strip()) or {}
            host_map.pop(str(hostname or "").strip(), None)

    def _secret_entry(self, token: str, hostname: str, field_name: str) -> dict[str, Any] | None:
        bucket = self._get_secret_bucket(token, hostname, create=False)
        if not bucket:
            return None
        entry = bucket.get(field_name)
        if not entry:
            return None
        if int(entry.get("expires_at") or 0) <= _now_ts():
            bucket.pop(field_name, None)
            if not bucket:
                host_map = self._session_secrets.get(str(token or "").strip()) or {}
                host_map.pop(str(hostname or "").strip(), None)
            return None
        return entry

    def _session_secret_value(self, token: str, hostname: str, field_name: str) -> str:
        entry = self._secret_entry(token, hostname, field_name)
        return str(entry.get("value") or "") if entry else ""

    def _session_secret_meta(self, token: str, hostname: str) -> dict[str, Any]:
        hostname = str(hostname or "")
        now = _now_ts()
        out: dict[str, Any] = {}
        for field_name in SECRET_FIELDS:
            entry = self._secret_entry(token, hostname, field_name)
            expires_at = int(entry.get("expires_at") or 0) if entry else 0
            out[field_name] = {
                "stored": entry is not None,
                "expires_at": expires_at,
                "remaining_seconds": max(expires_at - now, 0),
            }
        return out

    def reveal_session_secret(self, token: str, hostname: str, field_name: str) -> dict[str, Any]:
        if field_name not in SECRET_FIELDS:
            raise ValueError("Unsupported secret field.")
        meta = self._session_secret_meta(token, hostname).get(field_name) or {}
        return {
            "hostname": str(hostname or ""),
            "field": field_name,
            "value": self._session_secret_value(token, hostname, field_name),
            "stored": bool(meta.get("stored")),
            "expires_at": int(meta.get("expires_at") or 0),
            "remaining_seconds": int(meta.get("remaining_seconds") or 0),
        }

    def _apply_session_secrets_to_vps(self, token: str, vps: VPS) -> None:
        hostname = str(vps.hostname or "")
        for field_name in SECRET_FIELDS:
            value = self._session_secret_value(token, hostname, field_name)
            setattr(vps, field_name, value or None)

    def _require_user_password(self, token: str, hostname: str) -> str:
        value = self._session_secret_value(token, hostname, "user_pw")
        if not value:
            raise ValueError("VPS user password expired or missing. Please enter it again.")
        return value

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
        file_sync_worker = _get_file_sync_worker()
        if file_sync_worker is None:
            self.api_sync_state = {
                "running": False,
                "remaining": 0,
                "unsynced_hosts": [],
                "started_at": _now_ts(),
                "deadline": 0,
                "error": "FileSyncWorker not initialized",
                "success": False,
            }
            return
        results = await file_sync_worker.push_api_keys(dry_run=False, no_propagate=False)
        if isinstance(results, dict) and "error" in results and len(results) == 1:
            self.api_sync_state = {
                "running": False,
                "remaining": 0,
                "unsynced_hosts": [],
                "started_at": _now_ts(),
                "deadline": 0,
                "error": str(results["error"]),
                "success": False,
            }
            return
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
                targets = list(self.api_sync_state.get("unsynced_hosts") or [])
                await self._refresh_remote_api_md5s(targets)
                monitor_state = self._get_monitor_state()
                unsynced_hosts = self._unsynced_api_hosts(monitor_state=monitor_state)
                remaining = len(unsynced_hosts)
                self.api_sync_state["remaining"] = remaining
                self.api_sync_state["unsynced_hosts"] = unsynced_hosts
                if remaining == 0:
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
        local_api_md5 = str(pbremote.api_md5 or "")
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
                if api_md5 and api_md5 != local_api_md5:
                    hosts.append(hostname)
        except Exception as exc:
            _log(SERVICE, f"unsynced host check failed: {exc}", level="WARNING")
        return hosts

    def build_state(self) -> dict[str, Any]:
        self.refresh(force=False)
        pbremote = self._ensure_pbremote()
        monitor_state = self._get_monitor_state()
        overview_rows = self._build_overview_rows(pbremote, monitor_state)
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
        self._master_coindata_ok_cache = bool(coindata_ok)
        master_monitor = self._build_local_master_monitor_payload(pbremote, refresh=True)
        return {
            "kind": "master",
            "status": self._build_master_status(pbremote, coindata_ok),
            "branches": {
                "pbgui": self._build_master_pbgui_branch_state(pbremote),
                "pb7": self._build_master_pb7_branch_state(pbremote),
            },
            "monitor": master_monitor,
            "progress": self._build_master_progress(include_log=True),
        }

    def build_master_detail_quick(self) -> dict[str, Any]:
        pbremote = self._ensure_pbremote()
        return {
            "kind": "master",
            # Quick detail must not overwrite validated full-detail status with
            # a cheap fallback such as a hardcoded False.
            "status": self._build_master_status(pbremote, self._master_coindata_ok_cache),
            "branches": {
                "pbgui": self._build_master_pbgui_branch_state(pbremote),
                "pb7": self._build_master_pb7_branch_state(pbremote),
            },
            "monitor": self._build_local_master_monitor_payload(pbremote, refresh=False),
            "progress": self._build_master_progress(include_log=True),
        }

    def build_vps_detail(self, token: str, hostname: str, *, quick: bool = False) -> dict[str, Any]:
        if not quick:
            self.refresh(force=False)
        pbremote = self._ensure_pbremote()
        vps = self._require_vps(hostname)
        self._apply_session_secrets_to_vps(token, vps)
        monitor_state = self._get_monitor_state()
        host_state = self._get_host_telemetry(monitor_state, hostname)
        # Quick detail may be less fresh, but it must not regress fields that
        # were already validated by the full-detail path.
        coindata_ok = bool(self._vps_coindata_status_cache.get(hostname, False)) if quick else False
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
            self._vps_coindata_status_cache[hostname] = bool(coindata_ok)

        logfiles: list[str] = []
        monitor_payload = self._build_monitor_payload(host_state, hostname=hostname)
        logfiles.extend(monitor_payload.get("logfiles", []))
        available_logs = ((self._host_meta(host_state).get("available_logs") or []) if host_state else [])
        if isinstance(available_logs, list):
            logfiles.extend(available_logs)
        # add old bot log files
        bot_logs = (monitor_state.get("bot_logs") or {}).get(hostname, {})
        for log_list in bot_logs.values():
            logfiles.extend(log_list)
        return {
            "kind": "vps",
            "hostname": hostname,
            "status": self._build_vps_status(vps, host_state, pbremote, coindata_ok, quick=quick),
            "config": self._build_vps_config(token, vps),
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

    def get_cpu_history(self, hostname: str, *, bot_name: str = "") -> dict[str, Any]:
        return self.get_metric_history(hostname, bot_name=bot_name, metric="cpu")

    def get_metric_history(self, hostname: str, *, bot_name: str = "", metric: str = "cpu") -> dict[str, Any]:
        hostname = str(hostname or "").strip()
        bot_name = str(bot_name or "").strip()
        if not hostname:
            raise ValueError("Hostname is required.")
        if hostname != self._ensure_pbremote().name:
            self._require_vps(hostname)
        return get_metric_history_snapshot(hostname, bot_name=bot_name, metric=metric)

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
                          pbremote: PBRemote, coindata_ok: bool, *, quick: bool = False) -> dict[str, Any]:
        hostname = str(vps.hostname or "")
        summary_row = self._build_vps_overview_row(pbremote, vps.hostname, host_state)
        live_package_status = None
        if quick:
            # Keep the last full package probe visible between quick pushes.
            cached_package_status = self._vps_package_status_cache.get(hostname) or {}
            live_package_status = cached_package_status.get("data") or None
        else:
            live_package_status = self._get_live_vps_package_status(vps, host_state)
        if live_package_status:
            summary_row = dict(summary_row)
            if live_package_status.get("upgrades") not in (None, ""):
                summary_row["updates"] = live_package_status.get("upgrades")
            summary_row["reboot_required"] = bool(live_package_status.get("reboot", False))
        pbgui_github = self._build_remote_pbgui_github_status(pbremote, host_state)
        pb7_github = self._build_remote_pb7_github_status(pbremote, host_state)
        if quick:
            if not self._host_online(host_state):
                ssh_ok = False
            elif hostname in self._vps_ssh_ok_cache:
                # Keep the last full SSH validation result while the host stays online.
                ssh_ok = bool(self._vps_ssh_ok_cache[hostname])
            else:
                ssh_ok = True
        else:
            ssh_ok = vps.is_vps_ssh_open()
            self._vps_ssh_ok_cache[hostname] = bool(ssh_ok)
        return {
            "hosts_ok": vps.is_vps_in_hosts(),
            "ssh_ok": ssh_ok,
            "init_ok": vps.init_status == "successful",
            "setup_ok": vps.setup_status == "successful",
            "update_ok": vps.update_status == "successful",
            "update_ready": bool(vps.user_pw),
            "pending_updates": summary_row.get("updates", "N/A"),
            "rclone_ok": bool(getattr(pbremote, "bucket", None)),
            "coindata_ok": coindata_ok,
            "cmc_credits": self._host_meta(host_state).get("cmc_credits"),
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

    def build_vps_status_with_session(self, token: str, hostname: str, *, quick: bool = False) -> dict[str, Any]:
        vps = self._require_vps(hostname)
        self._apply_session_secrets_to_vps(token, vps)
        monitor_state = self._get_monitor_state()
        pbremote = self._ensure_pbremote()
        host_state = self._get_host_telemetry(monitor_state, hostname)
        coindata_ok = bool(self._vps_coindata_status_cache.get(hostname, False)) if quick else False
        return self._build_vps_status(vps, host_state, pbremote, coindata_ok, quick=quick)

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
            "cpu_60s": _safe_float(system.get("cpu_60s")),
            "cpu_60s_window": _safe_float(system.get("cpu_60s_window")),
            "cpu_60s_confirmed": _safe_float(system.get("cpu_60s_window")) >= 60,
            "mem": {
                "total_mb": _safe_int(_safe_float(system.get("mem_total")) / 1024 / 1024),
                "free_mb": _safe_int(_safe_float(system.get("mem_available")) / 1024 / 1024),
                "used_mb": _safe_int(_safe_float(system.get("mem_used")) / 1024 / 1024),
                "usage_pct": _safe_int(system.get("mem_percent")),
                "usage_60s_peak": _safe_float(system.get("mem_60s_peak")),
                "usage_60s_window": _safe_float(system.get("mem_60s_window")),
            },
            "disk": {
                "total_mb": _safe_int(_safe_float(system.get("disk_total")) / 1024 / 1024),
                "used_mb": _safe_int(_safe_float(system.get("disk_used")) / 1024 / 1024),
                "free_mb": _safe_int(_safe_float(system.get("disk_free")) / 1024 / 1024),
                "usage_pct": _safe_int(system.get("disk_percent")),
                "usage_60s_peak": _safe_float(system.get("disk_60s_peak")),
                "usage_60s_window": _safe_float(system.get("disk_60s_window")),
            },
            "swap": {
                "total_mb": _safe_int(_safe_float(system.get("swap_total")) / 1024 / 1024),
                "used_mb": _safe_int(_safe_float(system.get("swap_used")) / 1024 / 1024),
                "free_mb": _safe_int(_safe_float(system.get("swap_free")) / 1024 / 1024),
                "usage_pct": min(_safe_int(system.get("swap_percent")), 100),
                "usage_60s_peak": _safe_float(system.get("swap_60s_peak")),
                "usage_60s_window": _safe_float(system.get("swap_60s_window")),
            },
        }

    def _build_server_metrics(self, server) -> dict[str, Any] | None:
        if not server or not getattr(server, "mem", None) or not getattr(server, "disk", None) or not getattr(server, "swap", None):
            return None
        memory_peak, memory_window = self._update_master_server_metric_peak("memory", _safe_float(server.mem[2]))
        disk_peak, disk_window = self._update_master_server_metric_peak("disk", _safe_float(server.disk[3]))
        swap_total = _safe_float(server.swap[0])
        swap_peak, swap_window = self._update_master_server_metric_peak("swap", _safe_float(server.swap[3]), enabled=swap_total > 0)
        return {
            "rtd": int(getattr(server, "rtd", 0) or 0),
            "boot": datetime.fromtimestamp(server.boot).strftime("%Y-%m-%d %H:%M:%S") if getattr(server, "boot", 0) else "",
            "cpu": _safe_float(server.cpu),
            "cpu_60s": _safe_float(getattr(server, "cpu_60s", 0)),
            "cpu_60s_window": _safe_float(getattr(server, "cpu_60s_window", 0)),
            "cpu_60s_confirmed": _safe_float(getattr(server, "cpu_60s_window", 0)) >= 60,
            "mem": {
                "total_mb": _safe_int(server.mem[0] / 1024 / 1024),
                "free_mb": _safe_int(server.mem[1] / 1024 / 1024),
                "used_mb": _safe_int(server.mem[3] / 1024 / 1024),
                "usage_pct": _safe_int(server.mem[2]),
                "usage_60s_peak": memory_peak,
                "usage_60s_window": memory_window,
            },
            "disk": {
                "total_mb": _safe_int(server.disk[0] / 1024 / 1024),
                "used_mb": _safe_int(server.disk[1] / 1024 / 1024),
                "free_mb": _safe_int(server.disk[2] / 1024 / 1024),
                "usage_pct": _safe_int(server.disk[3]),
                "usage_60s_peak": disk_peak,
                "usage_60s_window": disk_window,
            },
            "swap": {
                "total_mb": _safe_int(server.swap[0] / 1024 / 1024),
                "used_mb": _safe_int(server.swap[1] / 1024 / 1024),
                "free_mb": _safe_int(server.swap[2] / 1024 / 1024),
                "usage_pct": min(_safe_int(server.swap[3]), 100),
                "usage_60s_peak": swap_peak,
                "usage_60s_window": swap_window,
            },
        }

    def _update_master_server_metric_peak(self, metric: str, value: float, *, enabled: bool = True) -> tuple[float, float]:
        metric = str(metric or "").strip().lower()
        history = self._master_server_metric_history.get(metric)
        if history is None:
            return 0.0, 0.0
        if not enabled:
            history.clear()
            return 0.0, 0.0
        now = time.time()
        history.append((now, max(0.0, float(value))))
        cutoff = now - (ROLLING_PEAK_WINDOW_SECONDS + 2.0)
        history[:] = [sample for sample in history if sample[0] >= cutoff]
        if not history:
            return 0.0, 0.0
        peak = round(max(sample[1] for sample in history), 1)
        window = round(now - history[0][0], 1)
        return peak, window

    def _empty_monitor_payload(self) -> dict[str, Any]:
        return {"server": None, "v7": [], "v7_running": [], "multi": [], "single": [], "logfiles": []}

    def _master_bot_cpu_60s(self, name: str, pid: int, ticks: int, now: float) -> tuple[float, float]:
        entry = self._master_bot_cpu_history.get(name)
        if not isinstance(entry, dict) or _safe_int(entry.get("pid")) != pid:
            entry = {"pid": pid, "history": []}
            self._master_bot_cpu_history[name] = entry
        history = entry.get("history")
        if not isinstance(history, list):
            history = []
            entry["history"] = history
        history.append((now, ticks))
        cutoff = now - 62
        history[:] = [sample for sample in history if sample[0] >= cutoff]
        base_sample = None
        for sample in history:
            if now - sample[0] >= 60:
                base_sample = sample
            else:
                break
        if base_sample is not None:
            dt_sec = now - base_sample[0]
            if dt_sec > 0:
                return round((ticks - base_sample[1]) / (dt_sec * 100), 2), round(dt_sec, 1)
        if history:
            return 0.0, round(now - history[0][0], 1)
        return 0.0, 0.0

    def _collect_local_master_live_bot_stats(self) -> dict[str, dict[str, float]]:
        stats: dict[str, dict[str, float]] = {}
        now = time.time()
        seen_names: set[str] = set()
        try:
            result = subprocess.run(
                ["ps", "auxw"],
                capture_output=True,
                text=True,
                timeout=5,
                cwd=PBGDIR,
            )
        except Exception as exc:
            _log(SERVICE, f"local master ps probe failed: {exc}", level="WARNING")
            return stats
        if result.returncode != 0:
            return stats
        for raw_line in (result.stdout or "").splitlines():
            if "main.py" not in raw_line or "config_run.json" not in raw_line:
                continue
            parts = raw_line.split(None, 10)
            if len(parts) < 11:
                continue
            try:
                pid = int(parts[1])
            except Exception:
                continue
            cmdline = parts[10]
            bot_name = ""
            try:
                for arg in shlex.split(cmdline):
                    if arg.endswith("/config_run.json") or arg.endswith("\\config_run.json"):
                        bot_name = Path(arg).parent.name
                        break
            except Exception:
                continue
            if not bot_name:
                continue
            seen_names.add(bot_name)
            cpu_60s = 0.0
            cpu_60s_window = 0.0
            try:
                stat_path = Path(f"/proc/{pid}/stat")
                if stat_path.exists():
                    stat_parts = stat_path.read_text(encoding="utf-8", errors="ignore").split()
                    ticks = _safe_int(stat_parts[13]) + _safe_int(stat_parts[14])
                    cpu_60s, cpu_60s_window = self._master_bot_cpu_60s(bot_name, pid, ticks, now)
            except Exception:
                cpu_60s = 0.0
                cpu_60s_window = 0.0
            swap_mb = 0.0
            try:
                status_path = Path(f"/proc/{pid}/status")
                if status_path.exists():
                    for line in status_path.read_text(encoding="utf-8", errors="ignore").splitlines():
                        if line.startswith("VmSwap:"):
                            swap_mb = round(_safe_float(line.split()[1]) / 1024, 2)
                            break
            except Exception:
                swap_mb = 0.0
            stats[bot_name] = {
                "cpu": round(_safe_float(parts[2]), 2),
                "cpu_60s": cpu_60s,
                "cpu_60s_window": cpu_60s_window,
                "rss_mb": round(_safe_float(parts[5]) / 1024, 2),
                "swap_mb": swap_mb,
            }
        for name in list(self._master_bot_cpu_history.keys()):
            if name not in seen_names:
                self._master_bot_cpu_history.pop(name, None)
        return stats

    def _collect_local_master_monitor_snapshot(self) -> dict[str, Any]:
        env = dict(os.environ)
        env.update({
            "PBGUI_CACHE_VERSION": str(MONITOR_CACHE_VERSION),
            "PBGUI_CACHE": json.dumps(self._master_monitor_cache),
        })
        try:
            result = subprocess.run(
                INSTANCE_COLLECT_SCRIPT,
                shell=True,
                capture_output=True,
                text=True,
                timeout=30,
                cwd=PBGDIR,
                env=env,
            )
        except Exception as exc:
            _log(SERVICE, f"local master monitor collect failed: {exc}", level="WARNING")
            return {"monitors": [], "v7": [], "bot_logs": {}}
        if result.returncode != 0 or not result.stdout:
            stderr = str(result.stderr or "").strip()
            if stderr:
                _log(SERVICE, f"local master monitor collect failed: {stderr}", level="WARNING")
            return {"monitors": [], "v7": [], "bot_logs": {}}
        try:
            parsed = json.loads(result.stdout.strip())
        except json.JSONDecodeError as exc:
            _log(SERVICE, f"local master monitor JSON parse failed: {exc}", level="WARNING")
            return {"monitors": [], "v7": [], "bot_logs": {}}
        if not isinstance(parsed, dict):
            return {"monitors": [], "v7": [], "bot_logs": {}}
        new_cache = parsed.get("cache")
        if isinstance(new_cache, dict):
            self._master_monitor_cache = new_cache
        return {
            "monitors": parsed.get("monitors") if isinstance(parsed.get("monitors"), list) else [],
            "v7": parsed.get("v7") if isinstance(parsed.get("v7"), list) else [],
            "bot_logs": parsed.get("bot_logs") if isinstance(parsed.get("bot_logs"), dict) else {},
        }

    def _bot_count_total(self, hostname: str, bot_name: str, metric: str) -> int:
        monitor = get_monitor()
        if not monitor or not hostname or not bot_name:
            return 0
        try:
            payload = monitor.get_bot_metric_history(hostname, bot_name, metric)
        except Exception:
            return 0
        return _safe_int((payload or {}).get("total_count"))

    def _bot_pnl_total(self, hostname: str, bot_name: str) -> tuple[float, int]:
        monitor = get_monitor()
        if not monitor or not hostname or not bot_name:
            return 0.0, 0
        try:
            payload = monitor.get_bot_metric_history(hostname, bot_name, "pnl")
        except Exception:
            return 0.0, 0
        return _safe_float((payload or {}).get("total_pnl")), _safe_int((payload or {}).get("total_fills"))

    def _build_local_running_v7_payload(self, v7_rows: list[dict[str, Any]], existing_names: set[str] | None = None) -> list[dict[str, Any]]:
        known_names = existing_names or set()
        items: list[dict[str, Any]] = []
        for instance in v7_rows:
            if not _truthy(instance.get("running", True)):
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

    def _build_local_master_monitor_payload(self, pbremote: PBRemote, *, refresh: bool) -> dict[str, Any]:
        if not refresh and self._master_monitor_payload_cache is not None:
            return self._master_monitor_payload_cache
        payload = self._empty_monitor_payload()
        payload["server"] = self._build_server_metrics(pbremote)
        snapshot = self._collect_local_master_monitor_snapshot()
        live_stats = self._collect_local_master_live_bot_stats()
        cfg = self.monitor_config
        for monitor in snapshot.get("monitors") or []:
            name = str(monitor.get("u") or "")
            live = live_stats.get(name) or {}
            start_ts = _safe_int(monitor.get("st"))
            pnl_hist_total, pnls_hist_total = self._bot_pnl_total(pbremote.name, name)
            item = {
                "server": pbremote.name,
                "version": getattr(pbremote, "pb7_version", "N/A"),
                "name": name,
                "pb_version": "7",
                "start_time": datetime.fromtimestamp(start_ts).strftime("%Y-%m-%d %H:%M:%S") if start_ts else "",
                "memory_mb": round(_safe_float(live.get("rss_mb")), 2),
                "swap_mb": round(_safe_float(live.get("swap_mb")), 2),
                "cpu": round(_safe_float(live.get("cpu")), 2),
                "cpu_60s": round(_safe_float(live.get("cpu_60s")), 2),
                "cpu_60s_window": round(_safe_float(live.get("cpu_60s_window")), 1),
                "cpu_60s_confirmed": _safe_float(live.get("cpu_60s_window")) >= 60,
                "pnls_today": _safe_int(monitor.get("ct")),
                "pnl_today": _safe_float(monitor.get("pt")),
                "pnls_hist_total": pnls_hist_total,
                "pnl_hist_total": pnl_hist_total,
                "errors_today": _safe_int(monitor.get("et")),
                "errors_4w": self._bot_count_total(pbremote.name, name, "errors"),
                "tracebacks_today": _safe_int(monitor.get("tt")),
                "tracebacks_4w": self._bot_count_total(pbremote.name, name, "tracebacks"),
            }
            item["levels"] = {
                "cpu": _metric_level(item["cpu"], cfg.cpu_warning_v7, cfg.cpu_error_v7),
                "memory": _metric_level(item["memory_mb"], cfg.mem_warning_v7, cfg.mem_error_v7),
                "swap": _metric_level(item["swap_mb"], cfg.swap_warning_v7, cfg.swap_error_v7),
                "errors": _metric_level(item["errors_today"], cfg.error_warning_v7, cfg.error_error_v7),
                "tracebacks": _metric_level(item["tracebacks_today"], cfg.traceback_warning_v7, cfg.traceback_error_v7),
            }
            payload["v7"].append(item)
            if name:
                payload["logfiles"].append(f"run_v7/{name}/passivbot.log")
        existing_v7_names = {item["name"] for item in payload["v7"] if item.get("name")}
        payload["v7_running"] = self._build_local_running_v7_payload(snapshot.get("v7") or [], existing_v7_names)
        for item in payload["v7_running"]:
            if item.get("name"):
                payload["logfiles"].append(f"run_v7/{item['name']}/passivbot.log")
        payload["logfiles"] = sorted(dict.fromkeys(payload["logfiles"]))
        self._master_monitor_payload_cache = payload
        return payload

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

    def _build_vps_config(self, token: str, vps: VPS) -> dict[str, Any]:
        secret_status = self._session_secret_meta(token, str(vps.hostname or ""))
        # Keep detail/config payloads secret-free. The frontend only gets
        # presence/TTL metadata and must explicitly request an on-demand reveal.
        return {
            "hostname": vps.hostname,
            "ip": vps.ip or "",
            "user": vps.user or "",
            "swap": vps.swap or "0",
            "bucket": vps.bucket or "",
            "coinmarketcap_api_key": vps.coinmarketcap_api_key or "",
            "firewall": bool(vps.firewall),
            "firewall_ssh_port": int(vps.firewall_ssh_port or 22),
            "firewall_ssh_ips": vps.firewall_ssh_ips or "",
            "init_methode": vps.init_methode or "root",
            "remove_user": bool(vps.remove_user),
            "secret_status": secret_status,
        }

    def _build_monitor_payload(self, host_state: dict[str, Any], hostname: str | None = None) -> dict[str, Any]:
        if hostname is None:
            return self._empty_monitor_payload()
        return self._build_remote_monitor_payload(hostname, host_state)

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
            bot_name = str(monitor.get("u") or "")
            pnl_hist_total, pnls_hist_total = self._bot_pnl_total(hostname, bot_name)
            item = {
                "server": hostname,
                "version": meta.get("pb7v", "N/A"),
                "name": bot_name,
                "pb_version": "7",
                "start_time": datetime.fromtimestamp(start_ts).strftime("%Y-%m-%d %H:%M:%S") if start_ts else "",
                "memory_mb": round(_safe_float(metrics[0]) / 1024 / 1024, 2) if metrics else 0.0,
                "swap_mb": round(swap_value, 2),
                "cpu": round(_safe_float(monitor.get("c")), 2),
                "cpu_60s": round(_safe_float(monitor.get("cpu_60s")), 2),
                "cpu_60s_window": round(_safe_float(monitor.get("cpu_60s_window")), 1),
                "cpu_60s_confirmed": _safe_float(monitor.get("cpu_60s_window")) >= 60,
                "pnls_today": _safe_int(monitor.get("ct")),
                "pnl_today": _safe_float(monitor.get("pt")),
                "pnls_hist_total": pnls_hist_total,
                "pnl_hist_total": pnl_hist_total,
                "errors_today": _safe_int(monitor.get("et")),
                "errors_4w": self._bot_count_total(hostname, bot_name, "errors"),
                "tracebacks_today": _safe_int(monitor.get("tt")),
                "tracebacks_4w": self._bot_count_total(hostname, bot_name, "tracebacks"),
            }
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

    def load_remote_branch_commits(self, remote_url: str, branch_name: str, limit: int = 50) -> list[dict[str, Any]]:
        if not remote_url or not branch_name:
            return []
        return list_remote_git_branch_commits(remote_url, branch_name, limit=int(limit))

    def run_master_command(self, *, command: str, command_text: str, debug: bool = False, sudo_pw: str | None = None, extra_vars: dict[str, Any] | None = None) -> None:
        self.vpsmanager.command = command
        self.vpsmanager.command_text = command_text
        self.vpsmanager.update_master(debug=debug, sudo_pw=sudo_pw, extra_vars=extra_vars)

    def run_vps_command(self, *, token: str, hostname: str, command: str, command_text: str, debug: bool = False, extra_vars: dict[str, Any] | None = None) -> None:
        vps = self._require_vps(hostname)
        self._apply_session_secrets_to_vps(token, vps)
        vps.command = command
        vps.command_text = command_text
        self.vpsmanager.update_vps(vps, debug=debug, extra_vars=extra_vars)

    def delete_vps(self, hostname: str) -> None:
        vps = self._require_vps(hostname)
        vps.delete()
        self.vpsmanager.vpss = [item for item in self.vpsmanager.vpss if item.hostname != hostname]
        self._set_vps_monitor_enabled(hostname, enabled=False)

    def read_vps_settings(self, token: str, hostname: str) -> dict[str, Any]:
        pbremote = self._ensure_pbremote()
        vps = self._require_vps(hostname)
        vps.user_pw = self._require_user_password(token, hostname)
        if not vps.can_login_ssh():
            raise ValueError("Cannot login via SSH. Please check username and password.")
        vps.bucket = pbremote.bucket
        info = vps.fetch_vps_info()
        vps.coinmarketcap_api_key = info["coinmarketcap"]
        vps.swap = info.get("swap", "0") if info.get("swap") in SWAP_OPTIONS else "0"
        vps.firewall, vps.firewall_ssh_ips = vps.fetch_ufw_settings()
        vps.save()
        return self._build_vps_config(token, vps)

    def save_vps(self, token: str, form: dict[str, Any]) -> dict[str, Any]:
        vps, is_new = self._hydrate_vps_from_form(token, form, allow_create=True)
        self._apply_vps_setup_form(token, vps, form)
        vps.save()
        self._set_vps_monitor_enabled(vps.hostname, enabled=True)
        if is_new:
            self.vpsmanager.vpss.append(vps)
            self.vpsmanager.vpss.sort(key=lambda item: item.hostname or "")
        return self._build_vps_config(token, vps)

    def prepare_import(self, hostname: Any) -> dict[str, Any]:
        hostname = str(hostname or "").strip()
        if not hostname:
            raise ValueError("Hostname is required.")
        if hostname in self.vpsmanager.list():
            raise ValueError("Hostname already exists.")
        temp = VPS()
        temp.hostname = hostname
        ip = str(temp.fetch_vps_ip_from_hosts() or "").strip()
        if not ip:
            raise ValueError("Hostname is not available in local /etc/hosts.")
        if not _valid_ipv4(ip):
            raise ValueError("Hostname in local /etc/hosts does not resolve to a valid IPv4 address.")
        return {
            "hostname": hostname,
            "ip": ip,
            "user": getpass.getuser(),
        }

    def save_vps_config(self, token: str, hostname: str, form: dict[str, Any]) -> dict[str, Any]:
        vps = self._require_vps(hostname)
        self._apply_vps_setup_form(token, vps, form)
        vps.save()
        self._set_vps_monitor_enabled(vps.hostname, enabled=True)
        return self._build_vps_config(token, vps)

    def init_vps(self, token: str, form: dict[str, Any], *, debug: bool = False) -> dict[str, Any]:
        vps, is_new = self._hydrate_vps_from_form(token, form, allow_create=True)
        self._apply_session_secrets_to_vps(token, vps)
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

    def setup_vps(self, token: str, hostname: str, form: dict[str, Any], *, debug: bool = False) -> dict[str, Any]:
        vps = self._require_vps(hostname)
        self._apply_vps_setup_form(token, vps, form)
        self._apply_session_secrets_to_vps(token, vps)
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

    def _hydrate_vps_from_form(self, token: str, form: dict[str, Any], *, allow_create: bool) -> tuple[VPS, bool]:
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
        self._apply_vps_full_form(token, vps, form, is_new=is_new)
        return vps, is_new

    def _apply_vps_full_form(self, token: str, vps: VPS, form: dict[str, Any], *, is_new: bool) -> None:
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
        vps.user = str(form.get("user") or vps.user or "")
        self._store_session_secrets(token, hostname, form)

    def _apply_vps_setup_form(self, token: str, vps: VPS, form: dict[str, Any]) -> None:
        hostname = str(vps.hostname or "")
        self._store_session_secrets(token, hostname, form)
        user_pw = str(form.get("user_pw") or self._session_secret_value(token, hostname, "user_pw") or "")
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
        vps.user_pw = user_pw or None
        vps.swap = swap
        vps.bucket = str(form.get("bucket") or vps.bucket or "")
        vps.coinmarketcap_api_key = str(form.get("coinmarketcap_api_key") or vps.coinmarketcap_api_key or "")
        vps.firewall = _truthy(form.get("firewall"))
        vps.firewall_ssh_port = _safe_int(form.get("firewall_ssh_port"), 22)
        vps.firewall_ssh_ips = firewall_ips
        vps.save()
