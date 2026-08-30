"""Owner process for persistent VPS monitoring and remote log streams."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import stat
import time
import uuid
from pathlib import Path
from typing import Any

from logging_helpers import human_log as _log
from master.async_logs import AsyncLogStreamer
from master.async_monitor import VPSMonitor
from master.async_store import SystemMetrics
from master.vps_monitor_rpc import (
    MAX_FRAME_BYTES,
    MAX_RPC_CLIENTS,
    PROTOCOL_VERSION,
    RPC_REQUEST_TIMEOUT_SECONDS,
    RPCError,
    default_socket_path,
    encode_frame,
    error_response,
    prepare_socket_path,
    read_frame,
    require_bool,
    require_int,
    require_string,
    validate_request,
    verify_peer_uid,
)


SERVICE = "VPSMonitor"
REMOTE_STREAM_IDLE_SECONDS = 60.0
MAX_REMOTE_STREAMS = 128


class VPSMonitorRPCDaemon:
    """Serve an allowlisted RPC surface around one real monitor owner."""

    def __init__(
        self,
        socket_path: Path | None = None,
        *,
        monitor: VPSMonitor | None = None,
        streamer: AsyncLogStreamer | None = None,
        expected_uid: int | None = None,
    ) -> None:
        self.socket_path = Path(socket_path) if socket_path is not None else default_socket_path()
        self.monitor = monitor or VPSMonitor()
        self.streamer = streamer or AsyncLogStreamer(self.monitor.pool)
        self.expected_uid = os.getuid() if expected_uid is None else int(expected_uid)
        self.boot_id = uuid.uuid4().hex
        self.revision = 0
        self._state_digest = ""
        self._server: asyncio.AbstractServer | None = None
        self._socket_identity: tuple[int, int] | None = None
        self._started_monitor = False
        self._client_tasks: set[asyncio.Task] = set()
        self._client_writers: set[asyncio.StreamWriter] = set()
        self._stream_last_access: dict[str, float] = {}
        self._stream_keys: dict[tuple[str, str], str] = {}
        self._stream_cleanup_task: asyncio.Task | None = None

    async def start(self) -> None:
        """Start the private RPC listener and the owned monitor."""
        if self._server is not None:
            return
        prepare_socket_path(self.socket_path)
        try:
            self._server = await asyncio.start_unix_server(
                self._handle_connection,
                path=str(self.socket_path),
                limit=MAX_FRAME_BYTES,
            )
            os.chmod(self.socket_path, 0o600)
            info = self.socket_path.lstat()
            if not stat.S_ISSOCK(info.st_mode):
                raise RuntimeError("RPC endpoint was not created as a Unix socket")
            self._socket_identity = (info.st_dev, info.st_ino)
            self._started_monitor = True
            await self.monitor.start()
            self._stream_cleanup_task = asyncio.create_task(
                self._stream_cleanup_loop(), name="vps-monitor-log-lease-cleanup"
            )
            _log(SERVICE, f"RPC daemon listening on {self.socket_path}", level="INFO")
        except BaseException:
            await self.stop()
            raise

    async def stop(self) -> None:
        """Stop all daemon-owned resources in deterministic order."""
        server, self._server = self._server, None
        if server is not None:
            server.close()
            await server.wait_closed()
        for writer in list(self._client_writers):
            writer.close()
        tasks = [task for task in self._client_tasks if task is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        cleanup_task, self._stream_cleanup_task = self._stream_cleanup_task, None
        if cleanup_task is not None:
            cleanup_task.cancel()
            await asyncio.gather(cleanup_task, return_exceptions=True)
        try:
            self.streamer.stop_all_streams()
        except Exception as exc:
            _log(SERVICE, f"Failed to stop remote log streams: {type(exc).__name__}", level="WARNING")
        if self._started_monitor:
            self._started_monitor = False
            try:
                await self.monitor.stop()
            except Exception as exc:
                _log(SERVICE, f"Failed to stop monitor: {type(exc).__name__}", level="ERROR")
        self._unlink_owned_socket()

    def _unlink_owned_socket(self) -> None:
        """Remove only the exact socket inode created by this daemon."""
        if self._socket_identity is None:
            return
        try:
            info = self.socket_path.lstat()
            if stat.S_ISSOCK(info.st_mode) and (info.st_dev, info.st_ino) == self._socket_identity:
                self.socket_path.unlink()
        except FileNotFoundError:
            pass
        finally:
            self._socket_identity = None

    async def _stream_cleanup_loop(self) -> None:
        """Bound detached remote tails while allowing API restart handoff."""
        try:
            while True:
                await asyncio.sleep(10)
                cutoff = time.monotonic() - REMOTE_STREAM_IDLE_SECONDS
                for stream_id, last_access in list(self._stream_last_access.items()):
                    if last_access >= cutoff:
                        continue
                    self.streamer.stop_stream(stream_id)
                    self._stream_last_access.pop(stream_id, None)
                    self._forget_stream_key(stream_id)
        except asyncio.CancelledError:
            return

    def _forget_stream_key(self, stream_id: str) -> None:
        """Remove any deduplication key currently mapped to a stream."""
        for key, mapped_id in list(self._stream_keys.items()):
            if mapped_id == stream_id:
                self._stream_keys.pop(key, None)

    async def _handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Process exactly one request and close the connection."""
        task = asyncio.current_task()
        if task is not None:
            self._client_tasks.add(task)
        self._client_writers.add(writer)
        request_id: str | int | None = None
        try:
            if len(self._client_tasks) > MAX_RPC_CLIENTS:
                raise RPCError("server_busy", "VPS monitor RPC client limit reached")
            verify_peer_uid(writer, self.expected_uid)
            try:
                payload = await asyncio.wait_for(read_frame(reader), timeout=RPC_REQUEST_TIMEOUT_SECONDS)
            except TimeoutError as exc:
                raise RPCError("request_timeout", "RPC request timed out") from exc
            request_id, method, params = validate_request(payload)
            result = await self.dispatch(method, params)
            response = {"id": request_id, "ok": True, "result": result}
            try:
                frame = encode_frame(response)
            except RPCError as exc:
                frame = encode_frame(error_response(request_id, exc))
        except Exception as exc:
            if not isinstance(exc, RPCError):
                _log(SERVICE, f"RPC request failed: {type(exc).__name__}", level="WARNING")
            frame = encode_frame(error_response(request_id, exc))
        try:
            writer.write(frame)
            await writer.drain()
        except (ConnectionError, BrokenPipeError):
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, BrokenPipeError):
                pass
            self._client_writers.discard(writer)
            if task is not None:
                self._client_tasks.discard(task)

    def _state_snapshot(self) -> dict[str, Any]:
        """Build a revisioned, serializable monitor and pool snapshot."""
        pool = self.monitor.pool.get_status_summary()
        local_logs = self.streamer.list_local_logs()
        store = self.monitor.store.get_full_state(pool, local_logs)
        store = dict(store)
        store.pop("connections", None)
        alert_settings = self.monitor.get_alert_settings()
        stable = {
            "store": {key: value for key, value in store.items() if key != "timestamp"},
            "pool": pool,
            "enabled_hosts": sorted(self.monitor.enabled_hosts),
            "alert_settings": alert_settings,
            "upstream_releases": (
                self.monitor.get_upstream_release_status()
                if hasattr(self.monitor, "get_upstream_release_status")
                else {}
            ),
            "boot_id": self.boot_id,
        }
        encoded = json.dumps(stable, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        digest = hashlib.sha256(encoded).hexdigest()
        if digest != self._state_digest:
            self._state_digest = digest
            self.revision += 1
        store["timestamp"] = float(store.get("timestamp") or time.time())
        return {**stable, "store": store, "revision": self.revision}

    async def dispatch(self, method: str, params: dict[str, Any]) -> Any:
        """Validate and dispatch one allowlisted RPC method."""
        if method == "hello":
            return {
                "service": SERVICE,
                "protocol_version": PROTOCOL_VERSION,
                "boot_id": self.boot_id,
                "revision": self.revision,
            }
        if method == "state.get":
            return self._state_snapshot()
        if method == "alerts.get":
            gui_only = require_bool(params, "gui_only", default=True)
            limit = require_int(params, "history_limit", default=0, maximum=10_000)
            return {
                "items": self.monitor.list_active_alerts(gui_only=gui_only),
                "history": self.monitor.list_alert_history(gui_only=gui_only, limit=limit),
                "summary": self.monitor.get_alert_summary(),
                "settings": self.monitor.get_alert_settings(),
            }
        if method == "alerts.ack":
            return self.monitor.acknowledge_alert(require_string(params, "id"))
        if method == "alerts.ack_all":
            return self.monitor.acknowledge_all_alerts()
        if method == "history.get":
            hostname = require_string(params, "hostname")
            metric = require_string(params, "metric", optional=True) or "cpu"
            bot_name = require_string(params, "bot_name", optional=True)
            if bot_name:
                return self.monitor.get_bot_metric_history(hostname, bot_name, metric)
            return self.monitor.get_host_metric_history(hostname, metric)
        if method == "host.refresh":
            return await self.monitor.refresh_enabled_host(require_string(params, "hostname"))
        if method == "host.collect_meta":
            hostname = require_string(params, "hostname")
            include_packages = require_bool(params, "include_package_status", default=False)
            await self.monitor.collect_host_meta_now(hostname, include_package_status=include_packages)
            return True
        if method == "host.refresh_package":
            return await self.monitor.refresh_package_status(require_string(params, "hostname"))
        if method == "releases.refresh":
            if hasattr(self.monitor, "request_upstream_release_refresh"):
                self.monitor.request_upstream_release_refresh()
            return True
        if method == "service.restart":
            return await self.monitor._restart_service(
                require_string(params, "hostname"), require_string(params, "service")
            )
        if method == "instance.kill":
            return await self.monitor.kill_instance(
                require_string(params, "hostname"),
                require_string(params, "name"),
                require_string(params, "pb_version", optional=True) or "7",
            )
        if method == "history.record_host_sample":
            return self._record_host_sample(params)
        if method == "config.set_runtime":
            return self._set_runtime_config(params)
        if method == "logs.get_recent":
            return await self.streamer.get_recent_logs(
                require_string(params, "hostname"),
                require_string(params, "service_or_path"),
                require_int(params, "lines", default=100, maximum=50_000),
            )
        if method == "logs.get_recent_files":
            paths = params.get("paths")
            if not isinstance(paths, list) or len(paths) > 32 or not all(isinstance(item, str) for item in paths):
                raise RPCError("invalid_params", "paths must be a string list")
            contains = params.get("contains")
            if contains is not None and (not isinstance(contains, str) or len(contains) > 4096):
                raise RPCError("invalid_params", "contains must be a string")
            return await self.streamer.get_recent_log_files(
                require_string(params, "hostname"),
                paths,
                require_int(params, "lines", default=5000, maximum=50_000),
                contains=contains,
            )
        if method == "logs.get_bot":
            return await self.streamer.get_bot_log(
                require_string(params, "hostname"),
                require_string(params, "instance_name"),
                require_int(params, "lines", default=100, maximum=50_000),
                require_string(params, "pb_version", optional=True) or None,
            )
        if method == "logs.info":
            return await self.streamer.get_log_info(
                require_string(params, "hostname"),
                require_string(params, "service_or_path"),
                require_string(params, "pb_version", optional=True) or None,
            )
        if method == "logs.start":
            hostname = require_string(params, "hostname")
            service_or_path = require_string(params, "service_or_path")
            stream_key = (hostname, service_or_path)
            existing_id = self._stream_keys.get(stream_key)
            if existing_id:
                existing_status = self.streamer.get_stream_status(existing_id)
                if isinstance(existing_status, dict) and existing_status.get("active"):
                    self._stream_last_access[existing_id] = time.monotonic()
                    return existing_id
                self._stream_keys.pop(stream_key, None)
                self._stream_last_access.pop(existing_id, None)
            if len(self._stream_last_access) >= MAX_REMOTE_STREAMS:
                oldest = min(self._stream_last_access, key=self._stream_last_access.get)
                self.streamer.stop_stream(oldest)
                self._stream_last_access.pop(oldest, None)
                self._forget_stream_key(oldest)
            stream_id = await self.streamer.start_stream(
                hostname, service_or_path
            )
            if stream_id:
                self._stream_last_access[str(stream_id)] = time.monotonic()
                self._stream_keys[stream_key] = str(stream_id)
            return stream_id
        if method == "logs.stop":
            stream_id = require_string(params, "stream_id")
            self.streamer.stop_stream(stream_id)
            self._stream_last_access.pop(stream_id, None)
            self._forget_stream_key(stream_id)
            return True
        if method == "logs.read":
            stream_id = require_string(params, "stream_id")
            self._stream_last_access[stream_id] = time.monotonic()
            return self.streamer.read_stream(
                stream_id,
                require_int(params, "max_lines", default=100, maximum=10_000),
            )
        if method == "logs.status":
            stream_id = require_string(params, "stream_id")
            self._stream_last_access[stream_id] = time.monotonic()
            return self.streamer.get_stream_status(stream_id)
        raise RPCError("method_not_found", "RPC method is not supported")

    def _record_host_sample(self, params: dict[str, Any]) -> bool:
        """Record one API-produced host metric sample in the daemon store."""
        hostname = require_string(params, "hostname")
        serialized_metrics = params.get("metrics")
        if serialized_metrics is not None:
            if not isinstance(serialized_metrics, dict):
                raise RPCError("invalid_params", "metrics must be an object")
            allowed = set(SystemMetrics.__dataclass_fields__)
            if set(serialized_metrics) - allowed:
                raise RPCError("invalid_params", "metrics contain unsupported fields")
            try:
                metrics = SystemMetrics(**serialized_metrics)
            except (TypeError, ValueError) as exc:
                raise RPCError("invalid_params", "metrics are invalid") from exc
            self.monitor._record_host_metric_history(hostname, metrics)
            for store in self.monitor._host_metric_history.values():
                store.maybe_flush()
            return True
        metric = require_string(params, "metric")
        if metric not in {"cpu", "memory", "disk", "swap"}:
            raise RPCError("invalid_params", "metric is not supported")
        minute = require_int(params, "minute", maximum=10**12)
        value = params.get("value")
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise RPCError("invalid_params", "value must be numeric")
        confirmed = require_bool(params, "confirmed", default=False)
        mode = require_string(params, "same_minute_mode", optional=True)
        if mode not in {"", "replace", "peak"}:
            raise RPCError("invalid_params", "same_minute_mode is invalid")
        store = self.monitor._host_metric_history.get(metric)
        if store is None:
            return False
        kwargs: dict[str, Any] = {"minute": minute, "value": float(value), "confirmed": confirmed}
        if mode:
            kwargs["same_minute_mode"] = mode
        store.record(hostname, **kwargs)
        store.maybe_flush()
        return True

    def _set_runtime_config(self, params: dict[str, Any]) -> bool:
        """Apply the small compatibility subset of live monitor settings."""
        key = require_string(params, "key")
        if key == "debug_logging":
            value = require_bool(params, "value")
            self.monitor._debug_logging = value
            self.monitor.store.set_ui_setting("debug_logging", "true" if value else "false")
            return True
        if key == "ui_setting":
            setting = require_string(params, "setting")
            value = params.get("value")
            if setting != "compact" or not isinstance(value, str) or len(value) > 128:
                raise RPCError("invalid_params", "UI setting is invalid")
            self.monitor.store.set_ui_setting(setting, value)
            return True
        if key == "alert_settings":
            settings = params.get("value")
            if not isinstance(settings, dict):
                raise RPCError("invalid_params", "alert settings must be an object")
            self.monitor.save_alert_settings(settings)
            return True
        raise RPCError("invalid_params", "Runtime setting is not supported")
