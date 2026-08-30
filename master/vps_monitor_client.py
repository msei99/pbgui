"""Unix RPC client and API-side compatibility proxies for VPS monitoring."""

from __future__ import annotations

import asyncio
import copy
import json
import socket
import threading
import time
import uuid
from dataclasses import fields
from pathlib import Path
from typing import Any, Callable

from master.async_logs import AsyncLogStreamer
from master.async_pool import AsyncSSHPool
from master.async_store import SystemMetrics
from logging_helpers import human_log as _log
from master.vps_monitor_rpc import (
    MAX_FRAME_BYTES,
    MAX_RESPONSE_FRAME_BYTES,
    PROTOCOL_VERSION,
    default_socket_path,
    encode_frame,
    verify_socket_permissions,
)


SERVICE = "VPSMonitor"


class VPSMonitorRPCClient:
    """Perform one synchronous request per owner-only Unix connection."""

    def __init__(self, socket_path: Path | None = None, *, timeout: float = 5.0) -> None:
        self.socket_path = Path(socket_path) if socket_path is not None else default_socket_path()
        self.timeout = float(timeout)

    def call(self, method: str, params: dict[str, Any] | None = None) -> Any:
        """Send one request and return its result or raise a safe RuntimeError."""
        verify_socket_permissions(self.socket_path)
        request_id = uuid.uuid4().hex
        frame = encode_frame({
            "version": PROTOCOL_VERSION,
            "id": request_id,
            "method": str(method),
            "params": params or {},
        }, maximum=MAX_FRAME_BYTES)
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            sock.settimeout(self.timeout)
            sock.connect(str(self.socket_path))
            sock.sendall(frame)
            raw = self._read_response(sock)
        except (FileNotFoundError, ConnectionRefusedError, TimeoutError, socket.timeout, OSError) as exc:
            raise RuntimeError("VPS monitor daemon is unavailable") from exc
        finally:
            sock.close()
        try:
            response = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RuntimeError("VPS monitor daemon returned an invalid response") from exc
        if not isinstance(response, dict) or response.get("id") != request_id or not isinstance(response.get("ok"), bool):
            raise RuntimeError("VPS monitor daemon returned an invalid response")
        if not response["ok"]:
            error = response.get("error") if isinstance(response.get("error"), dict) else {}
            message = str(error.get("message") or "VPS monitor RPC request failed")[:240]
            raise RuntimeError(message)
        return response.get("result")

    def _read_response(self, sock: socket.socket) -> bytes:
        """Read exactly one bounded response frame."""
        chunks: list[bytes] = []
        size = 0
        while True:
            chunk = sock.recv(min(65536, MAX_RESPONSE_FRAME_BYTES + 1 - size))
            if not chunk:
                raise RuntimeError("VPS monitor daemon closed the connection")
            chunks.append(chunk)
            size += len(chunk)
            if size > MAX_RESPONSE_FRAME_BYTES:
                raise RuntimeError("VPS monitor daemon response is too large")
            if b"\n" in chunk:
                raw = b"".join(chunks)
                line, remainder = raw.split(b"\n", 1)
                if remainder:
                    raise RuntimeError("VPS monitor daemon returned multiple frames")
                return line

    async def call_async(self, method: str, params: dict[str, Any] | None = None) -> Any:
        """Run one blocking Unix request without blocking the event loop."""
        return await asyncio.to_thread(self.call, method, params)


class VPSStoreProxy:
    """Hydrated read-compatible view of the daemon-owned monitor store."""

    _DICT_FIELDS = (
        "instances", "v7_instances", "v8_instances", "host_meta", "services", "streams", "bot_logs"
    )

    def __init__(self, set_ui_callback: Callable[[str, str], None]) -> None:
        self.system: dict[str, SystemMetrics] = {}
        for name in self._DICT_FIELDS:
            setattr(self, name, {})
        self.local_logs: list[str] = []
        self._ui_settings: dict[str, str] = {}
        self._timestamp = 0.0
        self.monitor_available = False
        self.changed = asyncio.Event()
        self._set_ui_callback = set_ui_callback

    def hydrate(self, snapshot: dict[str, Any]) -> None:
        """Replace cached dictionaries with one complete daemon snapshot."""
        metric_fields = {item.name for item in fields(SystemMetrics)}
        raw_system = snapshot.get("system") if isinstance(snapshot.get("system"), dict) else {}
        system: dict[str, SystemMetrics] = {}
        for hostname, raw in raw_system.items():
            if isinstance(raw, dict):
                system[str(hostname)] = SystemMetrics(**{key: value for key, value in raw.items() if key in metric_fields})
        dictionaries: dict[str, dict[str, Any]] = {}
        for name in self._DICT_FIELDS:
            value = snapshot.get(name)
            dictionaries[name] = value if isinstance(value, dict) else {}
        local_logs = snapshot.get("local_logs")
        hydrated_logs = list(local_logs) if isinstance(local_logs, list) else []
        settings = snapshot.get("ui_settings")
        hydrated_settings = dict(settings) if isinstance(settings, dict) else {}
        timestamp = float(snapshot.get("timestamp") or time.time())

        self.system = system
        for name, value in dictionaries.items():
            setattr(self, name, value)
        self.local_logs = hydrated_logs
        self._ui_settings = hydrated_settings
        self._timestamp = timestamp
        self.changed.set()

    def get_full_state(self, connection_summary: dict, local_logs: list[str]) -> dict[str, Any]:
        """Build the legacy full-state shape from the latest cached snapshot."""
        return {
            "connections": connection_summary,
            "system": {host: metrics.to_dict() for host, metrics in self.system.items()},
            **{name: getattr(self, name) for name in self._DICT_FIELDS},
            "local_logs": list(local_logs),
            "timestamp": self._timestamp,
            "monitor_available": self.monitor_available,
            "ui_settings": dict(self._ui_settings),
        }

    def set_ui_setting(self, key: str, value: str) -> None:
        """Update the cache immediately and forward the live setting."""
        self._ui_settings[str(key)] = str(value)
        self.changed.set()
        self._set_ui_callback(str(key), str(value))


class PoolProxy:
    """Combine daemon status with a lazy API-local operational SSH pool."""

    def __init__(self, pool_factory: Callable[[], AsyncSSHPool] = AsyncSSHPool) -> None:
        self._status = self._empty_status()
        self._pool_factory = pool_factory
        self._operational_pool: AsyncSSHPool | None = None
        self._available = False
        self._lock = threading.RLock()

    @staticmethod
    def _empty_status() -> dict[str, Any]:
        """Return the canonical empty pool status."""
        return {"total": 0, "connected": 0, "disconnected": 0, "auth_failed": 0, "connections": {}}

    def hydrate(self, status: dict[str, Any]) -> None:
        """Replace the daemon-owned status snapshot."""
        self._status = dict(status) if isinstance(status, dict) else self._empty_status()

    def set_available(self, available: bool) -> None:
        """Mark whether daemon connection state is currently authoritative."""
        self._available = bool(available)

    def get_status_summary(self) -> dict[str, Any]:
        """Return the latest daemon connection status."""
        if self._available:
            return copy.deepcopy(self._status)
        status = copy.deepcopy(self._status)
        connections = status.get("connections") if isinstance(status.get("connections"), dict) else {}
        for item in connections.values():
            if isinstance(item, dict):
                item["status"] = "disconnected"
                item["last_error"] = "VPS monitor daemon unavailable"
        status["connected"] = 0
        status["disconnected"] = len(connections)
        status["auth_failed"] = 0
        status["total"] = len(connections)
        return status

    def hostnames(self) -> list[str]:
        """Return hostnames known to the daemon snapshot."""
        connections = self._status.get("connections")
        return sorted(connections) if isinstance(connections, dict) else []

    def connected_hosts(self) -> list[str]:
        """Return hosts marked connected in the daemon snapshot."""
        if not self._available:
            return []
        connections = self._status.get("connections")
        if not isinstance(connections, dict):
            return []
        return sorted(host for host, item in connections.items() if isinstance(item, dict) and item.get("status") == "connected")

    def _ensure_pool(self) -> AsyncSSHPool:
        """Create and inventory-load the API-local pool once."""
        with self._lock:
            if self._operational_pool is None:
                pool = self._pool_factory()
                pool.load_vps_configs()
                self._operational_pool = pool
            return self._operational_pool

    def get_connection(self, hostname: str) -> Any:
        """Return API-local connection metadata for direct compatibility callers."""
        return self._ensure_pool().get_connection(hostname)

    async def disconnect_all(self) -> None:
        """Disconnect only the API-local operational pool."""
        pool = self._operational_pool
        if pool is not None:
            await pool.disconnect_all()

    def reload_inventory(self) -> None:
        """Refresh an already-created operational pool after host changes."""
        pool = self._operational_pool
        if pool is not None:
            pool.load_vps_configs()

    def __getattr__(self, name: str) -> Any:
        """Lazily delegate operational run, SFTP, process, and INI methods."""
        if name.startswith("__"):
            raise AttributeError(name)
        return getattr(self._ensure_pool(), name)


class _HostHistoryWriterProxy:
    """Compatibility writer for API-produced local-master history samples."""

    def __init__(self, owner: "VPSMonitorProxy", metric: str) -> None:
        self.owner = owner
        self.metric = metric

    def record(self, hostname: str, **sample: Any) -> None:
        """Forward one metric record to the daemon, ignoring an outage."""
        params = {"hostname": hostname, "metric": self.metric, **sample}
        try:
            self.owner._rpc_sync("history.record_host_sample", params)
        except RuntimeError:
            return

    def maybe_flush(self, *args: Any, **kwargs: Any) -> None:
        """No-op because each daemon-side record performs its own bounded flush."""
        del args, kwargs


class VPSMonitorProxy:
    """Poll daemon state while preserving the production monitor interface."""

    def __init__(
        self,
        socket_path: Path | None = None,
        *,
        client: VPSMonitorRPCClient | Any | None = None,
        poll_interval: float = 1.0,
        pool_factory: Callable[[], AsyncSSHPool] = AsyncSSHPool,
    ) -> None:
        self.client = client or VPSMonitorRPCClient(socket_path)
        self.poll_interval = max(float(poll_interval), 0.05)
        self.store = VPSStoreProxy(self._set_ui_setting)
        self.pool = PoolProxy(pool_factory)
        self.loop: asyncio.AbstractEventLoop | None = None
        self.enabled_hosts: set[str] = set()
        self.boot_id = ""
        self.revision = 0
        self.available = False
        self.upstream_releases: dict[str, Any] = {}
        self.upstream_release_capability: bool | None = None
        self._alert_settings: dict[str, Any] = {}
        self._alerts_cache = {"items": [], "history": [], "summary": self._empty_alert_summary()}
        self._debug_logging_value = False
        self._poll_task: asyncio.Task | None = None
        self._host_metric_history = {
            metric: _HostHistoryWriterProxy(self, metric) for metric in ("cpu", "memory", "disk", "swap")
        }

    @staticmethod
    def _empty_alert_summary() -> dict[str, int]:
        """Return a fresh empty alert summary."""
        return {"new_count": 0, "ack_count": 0, "total_active": 0}

    async def _rpc_async(self, method: str, params: dict[str, Any] | None = None) -> Any:
        """Call either the production client or a small test fake asynchronously."""
        if hasattr(self.client, "call_async"):
            return await self.client.call_async(method, params)
        return await asyncio.to_thread(self.client.call, method, params)

    def _rpc_sync(self, method: str, params: dict[str, Any] | None = None) -> Any:
        """Call the synchronous one-request client."""
        return self.client.call(method, params)

    async def start(self) -> None:
        """Start reconnecting state polling without owning daemon lifecycle."""
        if self._poll_task is not None and not self._poll_task.done():
            return
        self.loop = asyncio.get_running_loop()
        await self._poll_once()
        self._poll_task = asyncio.create_task(self._poll_loop(), name="vps-monitor-proxy-poll")

    async def stop(self) -> None:
        """Stop polling and disconnect only the lazy API-local SSH pool."""
        task, self._poll_task = self._poll_task, None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await self.pool.disconnect_all()
        self.loop = None

    async def _poll_loop(self) -> None:
        """Reconnect forever and retain the latest valid state during outages."""
        while True:
            try:
                await asyncio.sleep(self.poll_interval)
                await self._poll_once()
            except asyncio.CancelledError:
                return

    async def _poll_once(self) -> bool:
        """Fetch and hydrate one revisioned state snapshot."""
        try:
            state = await self._rpc_async("state.get")
        except RuntimeError:
            self._mark_unavailable()
            return False
        try:
            if not isinstance(state, dict):
                raise TypeError("state response must be an object")
            boot_id = str(state.get("boot_id") or "")
            revision = int(state.get("revision") or 0)
            changed = boot_id != self.boot_id or revision != self.revision
            self.available = True
            self.pool.set_available(True)
            self.store.monitor_available = True
            self.upstream_release_capability = "upstream_releases" in state
            if not changed:
                return True
            store = state.get("store")
            self.store.hydrate(store if isinstance(store, dict) else {})
            pool = state.get("pool")
            self.pool.hydrate(pool if isinstance(pool, dict) else {})
            enabled = state.get("enabled_hosts")
            self.enabled_hosts = {str(item) for item in enabled} if isinstance(enabled, list) else set()
            settings = state.get("alert_settings")
            self._alert_settings = dict(settings) if isinstance(settings, dict) else {}
            releases = state.get("upstream_releases")
            self.upstream_releases = dict(releases) if isinstance(releases, dict) else {}
            self._debug_logging_value = str(self.store._ui_settings.get("debug_logging", "false")).lower() == "true"
            self.boot_id = boot_id
            self.revision = revision
            return True
        except (TypeError, ValueError) as exc:
            self._mark_unavailable()
            _log(SERVICE, f"Invalid daemon state response: {type(exc).__name__}", level="WARNING")
            return False

    def _mark_unavailable(self) -> None:
        """Expose a daemon outage without discarding the last valid snapshot."""
        self.available = False
        self.pool.set_available(False)
        self.store.monitor_available = False
        self.store.changed.set()

    @property
    def _debug_logging(self) -> bool:
        """Expose the legacy directly-assigned debug flag."""
        return self._debug_logging_value

    @_debug_logging.setter
    def _debug_logging(self, value: bool) -> None:
        self._debug_logging_value = bool(value)

    @property
    def debug_logging(self) -> bool:
        """Return the cached debug logging flag."""
        return self._debug_logging_value

    @debug_logging.setter
    def debug_logging(self, value: bool) -> None:
        self._debug_logging = value

    def _set_ui_setting(self, key: str, value: str) -> None:
        """Rely on the daemon INI watcher after the API persists the setting."""
        del key, value

    def get_alert_settings(self) -> dict[str, Any]:
        """Return the last daemon-provided alert settings."""
        return dict(self._alert_settings)

    def save_alert_settings(self, settings: dict[str, Any]) -> None:
        """Persist alert settings through the daemon owner."""
        self._rpc_sync("config.set_runtime", {"key": "alert_settings", "value": settings})
        self._alert_settings.update(settings)

    def _refresh_alert_cache(self) -> dict[str, Any]:
        """Fetch alerts or retain the previous snapshot during an outage."""
        try:
            result = self._rpc_sync("alerts.get", {"gui_only": True, "history_limit": 0})
            if isinstance(result, dict):
                self._alerts_cache = result
        except RuntimeError:
            pass
        return self._alerts_cache

    def list_active_alerts(self, *, gui_only: bool = False) -> list[dict[str, Any]]:
        """Return active alerts using the synchronous compatibility contract."""
        try:
            result = self._rpc_sync("alerts.get", {"gui_only": gui_only, "history_limit": 0})
            if isinstance(result, dict):
                self._alerts_cache = result
        except RuntimeError:
            pass
        return list(self._alerts_cache.get("items") or [])

    def list_alert_history(self, *, gui_only: bool = False, limit: int = 0) -> list[dict[str, Any]]:
        """Return resolved alert history from the daemon or cache."""
        try:
            result = self._rpc_sync("alerts.get", {"gui_only": gui_only, "history_limit": int(limit)})
            if isinstance(result, dict):
                self._alerts_cache = result
        except RuntimeError:
            pass
        history = list(self._alerts_cache.get("history") or [])
        return history[:limit] if limit > 0 else history

    def get_alert_summary(self) -> dict[str, int]:
        """Return the current alert summary or a cached empty summary."""
        result = self._refresh_alert_cache().get("summary")
        return dict(result) if isinstance(result, dict) else self._empty_alert_summary()

    def acknowledge_alert(self, alert_id: str) -> bool:
        """Acknowledge one alert, returning false while unavailable."""
        try:
            return bool(self._rpc_sync("alerts.ack", {"id": alert_id}))
        except RuntimeError:
            return False

    def acknowledge_all_alerts(self) -> int:
        """Acknowledge all alerts, returning zero while unavailable."""
        try:
            return int(self._rpc_sync("alerts.ack_all") or 0)
        except RuntimeError:
            return 0

    def get_host_metric_history(self, hostname: str, metric: str) -> dict[str, Any]:
        """Return host history or a compatible unavailable payload."""
        try:
            result = self._rpc_sync("history.get", {"hostname": hostname, "metric": metric, "bot_name": ""})
            return result if isinstance(result, dict) else {}
        except RuntimeError:
            return {"available": False, "scope": "host", "hostname": hostname, "metric": metric, "points": []}

    def get_host_cpu_history(self, hostname: str) -> dict[str, Any]:
        """Return host CPU history."""
        return self.get_host_metric_history(hostname, "cpu")

    def get_bot_metric_history(self, hostname: str, bot_name: str, metric: str) -> dict[str, Any]:
        """Return bot history or a compatible unavailable payload."""
        try:
            result = self._rpc_sync(
                "history.get", {"hostname": hostname, "bot_name": bot_name, "metric": metric}
            )
            return result if isinstance(result, dict) else {}
        except RuntimeError:
            return {
                "available": False,
                "scope": "bot",
                "hostname": hostname,
                "bot_name": bot_name,
                "metric": metric,
                "points": [],
            }

    def get_bot_cpu_history(self, hostname: str, bot_name: str) -> dict[str, Any]:
        """Return bot CPU history."""
        return self.get_bot_metric_history(hostname, bot_name, "cpu")

    async def refresh_enabled_host(self, hostname: str) -> bool:
        """Ask the daemon to reload and reconnect one monitored host."""
        self.pool.reload_inventory()
        try:
            return bool(await self._rpc_async("host.refresh", {"hostname": hostname}))
        except RuntimeError:
            return False

    async def collect_host_meta_now(self, hostname: str, *, include_package_status: bool = False) -> None:
        """Ask the daemon for an immediate metadata collection."""
        try:
            await self._rpc_async(
                "host.collect_meta",
                {"hostname": hostname, "include_package_status": bool(include_package_status)},
            )
        except RuntimeError:
            return None

    async def refresh_package_status(self, hostname: str) -> bool:
        """Refresh package metadata, returning false while unavailable."""
        try:
            return bool(await self._rpc_async("host.refresh_package", {"hostname": hostname}))
        except RuntimeError:
            return False

    def get_upstream_release_status(self) -> dict[str, Any]:
        """Return the latest daemon-owned upstream snapshot."""
        return copy.deepcopy(self.upstream_releases)

    def request_upstream_release_refresh(self) -> None:
        """Ask the daemon to refresh release heads without waiting for Git."""
        try:
            self._rpc_sync("releases.refresh")
        except RuntimeError:
            return

    async def _restart_service(self, hostname: str, service_name: str) -> bool:
        """Restart a monitored remote service through the daemon."""
        try:
            return bool(await self._rpc_async(
                "service.restart", {"hostname": hostname, "service": service_name}
            ))
        except RuntimeError:
            return False

    async def restart_service(self, hostname: str, service_name: str) -> bool:
        """Public alias for compatibility with older callers."""
        return await self._restart_service(hostname, service_name)

    async def kill_instance(self, hostname: str, name: str, pb_version: str = "") -> dict[str, Any]:
        """Stop a remote instance through the daemon."""
        try:
            result = await self._rpc_async(
                "instance.kill",
                {"hostname": hostname, "name": name, "pb_version": pb_version or "7"},
            )
            return result if isinstance(result, dict) else {"success": False, "pid": ""}
        except RuntimeError:
            return {"success": False, "pid": ""}


class RemoteLogStreamerProxy:
    """Keep local log reads local and route remote streams to the daemon."""

    list_local_logs = staticmethod(AsyncLogStreamer.list_local_logs)
    get_local_logs = staticmethod(AsyncLogStreamer.get_local_logs)
    read_local_log_delta = staticmethod(AsyncLogStreamer.read_local_log_delta)
    resolve_local_log_path = staticmethod(AsyncLogStreamer.resolve_local_log_path)

    def __init__(self, client: VPSMonitorRPCClient | Any) -> None:
        self.client = client

    async def _call_async(self, method: str, params: dict[str, Any]) -> Any:
        """Use the client's async wrapper or adapt a test fake."""
        if hasattr(self.client, "call_async"):
            return await self.client.call_async(method, params)
        return await asyncio.to_thread(self.client.call, method, params)

    async def get_recent_logs(self, hostname: str, service_or_path: str, lines: int = 100) -> str | None:
        """Fetch recent remote log text."""
        try:
            return await self._call_async(
                "logs.get_recent", {"hostname": hostname, "service_or_path": service_or_path, "lines": lines}
            )
        except RuntimeError:
            return None

    async def get_recent_log_files(
        self, hostname: str, paths: list[str], lines: int = 5000, *, contains: str | None = None
    ) -> str | None:
        """Fetch grouped remote log files."""
        try:
            return await self._call_async(
                "logs.get_recent_files",
                {"hostname": hostname, "paths": paths, "lines": lines, "contains": contains},
            )
        except RuntimeError:
            return None

    async def get_bot_log(
        self, hostname: str, instance_name: str, lines: int = 100, pb_version: str | None = None
    ) -> str | None:
        """Fetch one remote bot log."""
        try:
            return await self._call_async(
                "logs.get_bot",
                {
                    "hostname": hostname,
                    "instance_name": instance_name,
                    "lines": lines,
                    "pb_version": pb_version or "",
                },
            )
        except RuntimeError:
            return None

    async def get_log_info(
        self, hostname: str, service_or_path: str, pb_version: str | None = None
    ) -> dict[str, Any] | None:
        """Fetch remote log metadata."""
        try:
            result = await self._call_async(
                "logs.info",
                {"hostname": hostname, "service_or_path": service_or_path, "pb_version": pb_version or ""},
            )
            return result if isinstance(result, dict) else None
        except RuntimeError:
            return None

    async def start_stream(self, hostname: str, service_or_path: str) -> str | None:
        """Start a daemon-owned remote log stream."""
        try:
            result = await self._call_async(
                "logs.start", {"hostname": hostname, "service_or_path": service_or_path}
            )
            return str(result) if result else None
        except RuntimeError:
            return None

    def stop_stream(self, stream_id: str) -> None:
        """Stop one daemon-owned remote stream."""
        try:
            self.client.call("logs.stop", {"stream_id": stream_id})
        except RuntimeError:
            return

    async def stop_stream_async(self, stream_id: str) -> None:
        """Stop one daemon-owned remote stream without blocking FastAPI."""
        try:
            await self._call_async("logs.stop", {"stream_id": stream_id})
        except RuntimeError:
            return

    def detach_stream(self, stream_id: str) -> None:
        """Leave a stream on its bounded daemon lease during API disconnect."""
        del stream_id

    def read_stream(self, stream_id: str, max_lines: int = 100) -> list[str]:
        """Drain buffered daemon-owned stream lines."""
        try:
            result = self.client.call("logs.read", {"stream_id": stream_id, "max_lines": max_lines})
            return list(result) if isinstance(result, list) else []
        except RuntimeError:
            return []

    async def read_stream_async(self, stream_id: str, max_lines: int = 100) -> list[str]:
        """Drain stream lines without blocking FastAPI."""
        try:
            result = await self._call_async(
                "logs.read", {"stream_id": stream_id, "max_lines": max_lines}
            )
            return list(result) if isinstance(result, list) else []
        except RuntimeError:
            return []

    def get_stream_status(self, stream_id: str) -> dict[str, Any] | None:
        """Return daemon-owned stream status."""
        try:
            result = self.client.call("logs.status", {"stream_id": stream_id})
            return result if isinstance(result, dict) else None
        except RuntimeError:
            return None

    async def get_stream_status_async(self, stream_id: str) -> dict[str, Any] | None:
        """Return stream status without blocking FastAPI."""
        try:
            result = await self._call_async("logs.status", {"stream_id": stream_id})
            return result if isinstance(result, dict) else None
        except RuntimeError:
            return None

    def cleanup_stopped(self) -> None:
        """No-op because the daemon removes stopped streams itself."""
        return None

    def stop_all_streams(self) -> None:
        """Preserve daemon-owned streams across API proxy shutdown."""
        return None
