"""Focused tests for the private VPS monitor RPC daemon and compatibility proxy."""

from __future__ import annotations

import asyncio
import json
import stat
import sys
import types
from pathlib import Path
from typing import Any

import pytest

# The focused protocol tests never open SSH. Keep collection independent from
# the optional runtime package when the lightweight test interpreter lacks it.
try:
    import asyncssh  # noqa: F401
except ImportError:
    sys.modules["asyncssh"] = types.ModuleType("asyncssh")

from master.async_store import SystemMetrics, VPSStore
from master.vps_monitor_client import VPSMonitorProxy, VPSMonitorRPCClient
from master.vps_monitor_daemon import VPSMonitorRPCDaemon
from master.vps_monitor_rpc import MAX_FRAME_BYTES, PROTOCOL_VERSION, prepare_socket_path


class FakeHistoryStore:
    """Capture history writes without touching runtime data."""

    def __init__(self) -> None:
        self.records: list[tuple[str, dict[str, Any]]] = []
        self.flushes = 0

    def record(self, hostname: str, **sample: Any) -> None:
        """Capture one sample."""
        self.records.append((hostname, sample))

    def maybe_flush(self, *args: Any, **kwargs: Any) -> None:
        """Capture a flush request."""
        del args, kwargs
        self.flushes += 1


class FakePool:
    """Provide a deterministic pool status."""

    def get_status_summary(self) -> dict[str, Any]:
        """Return one connected fake host."""
        return {
            "total": 1,
            "connected": 1,
            "disconnected": 0,
            "auth_failed": 0,
            "connections": {"vps-1": {"status": "connected", "ip": "192.0.2.1"}},
        }


class FakeMonitor:
    """Implement the daemon-facing monitor contract entirely in memory."""

    def __init__(self) -> None:
        self.pool = FakePool()
        self.store = VPSStore()
        self.store.update_system("vps-1", SystemMetrics(timestamp=1.0, cpu=12.5, mem_total=100))
        self.enabled_hosts = {"vps-1"}
        self._debug_logging = False
        self._host_metric_history = {name: FakeHistoryStore() for name in ("cpu", "memory", "disk", "swap")}
        self.started = 0
        self.stopped = 0
        self.calls: list[tuple[str, Any]] = []
        self.release_status = {
            "schema_version": 1,
            "repositories": {"pb8": {"state": "ok", "target_commit": "a" * 40}},
        }

    async def start(self) -> None:
        """Capture daemon startup."""
        self.started += 1

    async def stop(self) -> None:
        """Capture daemon shutdown."""
        self.stopped += 1

    def get_alert_settings(self) -> dict[str, Any]:
        """Return harmless fake alert settings."""
        return {"offline_gui": True}

    def save_alert_settings(self, settings: dict[str, Any]) -> None:
        """Capture alert setting updates."""
        self.calls.append(("settings", settings))

    def list_active_alerts(self, *, gui_only: bool = False) -> list[dict[str, Any]]:
        """Return one fake alert."""
        return [{"id": "alert-1", "gui_only": gui_only}]

    def list_alert_history(self, *, gui_only: bool = False, limit: int = 0) -> list[dict[str, Any]]:
        """Return one fake history item."""
        del gui_only, limit
        return [{"id": "old-1"}]

    def get_alert_summary(self) -> dict[str, int]:
        """Return a fake summary."""
        return {"new_count": 1, "ack_count": 0, "total_active": 1}

    def acknowledge_alert(self, alert_id: str) -> bool:
        """Capture one acknowledgement."""
        self.calls.append(("ack", alert_id))
        return alert_id == "alert-1"

    def acknowledge_all_alerts(self) -> int:
        """Return one acknowledged alert."""
        return 1

    def get_host_metric_history(self, hostname: str, metric: str) -> dict[str, Any]:
        """Return fake host history."""
        return {"available": True, "hostname": hostname, "metric": metric, "points": [[1, 2]]}

    def get_bot_metric_history(self, hostname: str, bot_name: str, metric: str) -> dict[str, Any]:
        """Return fake bot history."""
        return {"available": True, "hostname": hostname, "bot_name": bot_name, "metric": metric}

    async def refresh_enabled_host(self, hostname: str) -> bool:
        """Capture a host refresh."""
        self.calls.append(("refresh", hostname))
        return True

    async def collect_host_meta_now(self, hostname: str, *, include_package_status: bool = False) -> None:
        """Capture metadata collection."""
        self.calls.append(("meta", (hostname, include_package_status)))

    async def refresh_package_status(self, hostname: str) -> bool:
        """Capture package refresh."""
        self.calls.append(("package", hostname))
        return True

    def get_upstream_release_status(self) -> dict[str, Any]:
        """Return fake daemon-owned release state."""
        return self.release_status

    def request_upstream_release_refresh(self) -> None:
        """Capture a release refresh request."""
        self.calls.append(("release-refresh", None))

    async def _restart_service(self, hostname: str, service: str) -> bool:
        """Capture service restart."""
        self.calls.append(("restart", (hostname, service)))
        return True

    async def kill_instance(self, hostname: str, name: str, pb_version: str) -> dict[str, Any]:
        """Capture instance stop."""
        self.calls.append(("kill", (hostname, name, pb_version)))
        return {"success": True, "pid": "123"}


class FakeStreamer:
    """Implement remote log operations without files or SSH."""

    def __init__(self) -> None:
        self.stopped_all = 0
        self.stopped: list[str] = []

    @staticmethod
    def list_local_logs() -> list[str]:
        """Return a fake local log list."""
        return ["VPSMonitor.log"]

    def stop_all_streams(self) -> None:
        """Capture daemon cleanup."""
        self.stopped_all += 1

    async def get_recent_logs(self, hostname: str, path: str, lines: int) -> str:
        """Return deterministic recent text."""
        return f"{hostname}:{path}:{lines}"

    async def get_recent_log_files(self, hostname: str, paths: list[str], lines: int, *, contains=None) -> str:
        """Return deterministic grouped text."""
        return f"{hostname}:{len(paths)}:{lines}:{contains}"

    async def get_bot_log(self, hostname: str, name: str, lines: int, version: str | None) -> str:
        """Return deterministic bot text."""
        return f"{hostname}:{name}:{lines}:{version}"

    async def get_log_info(self, hostname: str, path: str, version: str | None) -> dict[str, int]:
        """Return fake file information."""
        del hostname, path, version
        return {"size": 42}

    async def start_stream(self, hostname: str, path: str) -> str:
        """Return a fake stream id."""
        return f"{hostname}:{path}:1"

    def stop_stream(self, stream_id: str) -> None:
        """Capture a stopped stream."""
        self.stopped.append(stream_id)

    def read_stream(self, stream_id: str, max_lines: int) -> list[str]:
        """Return one fake stream line."""
        return [f"{stream_id}:{max_lines}"]

    def get_stream_status(self, stream_id: str) -> dict[str, Any]:
        """Return fake stream status."""
        return {"stream_id": stream_id, "active": True}


def test_rpc_framing_errors_and_owner_only_permissions(tmp_path: Path) -> None:
    """The server enforces strict framing and owner-only filesystem modes."""
    async def scenario() -> None:
        socket_path = tmp_path / "runtime" / "vps-monitor.sock"
        monitor = FakeMonitor()
        daemon = VPSMonitorRPCDaemon(socket_path, monitor=monitor, streamer=FakeStreamer())
        await daemon.start()
        try:
            assert stat.S_IMODE(socket_path.parent.stat().st_mode) == 0o700
            assert stat.S_IMODE(socket_path.stat().st_mode) == 0o600
            client = VPSMonitorRPCClient(socket_path)
            hello = await client.call_async("hello")
            assert hello["protocol_version"] == PROTOCOL_VERSION
            socket_path.chmod(0o666)
            with pytest.raises(RuntimeError, match="not owner-only"):
                await client.call_async("hello")
            socket_path.chmod(0o600)

            reader, writer = await asyncio.open_unix_connection(str(socket_path))
            writer.write(b'{"version":1,"id":"bad","method":"hello","params":{},"token":"secret"}\n')
            await writer.drain()
            response = json.loads(await reader.readline())
            assert response == {
                "id": None,
                "ok": False,
                "error": {"code": "invalid_request", "message": "RPC request contains unsupported fields"},
            }
            writer.close()
            await writer.wait_closed()

            reader, writer = await asyncio.open_unix_connection(str(socket_path))
            writer.write((b"x" * MAX_FRAME_BYTES) + b"\n")
            await writer.drain()
            response = json.loads(await reader.readline())
            assert response["ok"] is False
            assert response["error"]["code"] == "frame_too_large"
            writer.close()
            await writer.wait_closed()
        finally:
            await daemon.stop()
        assert monitor.started == 1
        assert monitor.stopped == 1
        assert not socket_path.exists()

    asyncio.run(scenario())


def test_socket_path_rejects_symlink_and_regular_file_collisions(tmp_path: Path) -> None:
    """Security-sensitive socket collisions are never unlinked implicitly."""
    parent = tmp_path / "runtime"
    parent.mkdir()
    socket_path = parent / "vps-monitor.sock"
    socket_path.write_text("not a socket", encoding="utf-8")
    with pytest.raises(RuntimeError, match="non-socket collision"):
        prepare_socket_path(socket_path)
    socket_path.unlink()
    socket_path.symlink_to(tmp_path / "missing")
    with pytest.raises(RuntimeError, match="non-socket collision"):
        prepare_socket_path(socket_path)


def test_daemon_dispatches_monitor_log_and_history_methods(tmp_path: Path) -> None:
    """Allowlisted dispatch reaches the real-owner interfaces with validated arguments."""
    async def scenario() -> None:
        monitor = FakeMonitor()
        streamer = FakeStreamer()
        daemon = VPSMonitorRPCDaemon(tmp_path / "rpc" / "sock", monitor=monitor, streamer=streamer)
        assert await daemon.dispatch("alerts.ack", {"id": "alert-1"}) is True
        assert await daemon.dispatch("host.refresh", {"hostname": "vps-1"}) is True
        assert await daemon.dispatch(
            "service.restart", {"hostname": "vps-1", "service": "PBRun"}
        ) is True
        assert await daemon.dispatch(
            "instance.kill", {"hostname": "vps-1", "name": "bot", "pb_version": "8"}
        ) == {"success": True, "pid": "123"}
        assert await daemon.dispatch(
            "logs.get_recent", {"hostname": "vps-1", "service_or_path": "PBRun", "lines": 10}
        ) == "vps-1:PBRun:10"
        assert await daemon.dispatch(
            "history.record_host_sample",
            {"hostname": "master", "metric": "cpu", "minute": 12, "value": 4.5, "confirmed": True},
        ) is True
        history = monitor._host_metric_history["cpu"]
        assert history.records == [("master", {"minute": 12, "value": 4.5, "confirmed": True})]
        assert history.flushes == 1
        state_1 = await daemon.dispatch("state.get", {})
        state_2 = await daemon.dispatch("state.get", {})
        assert state_2["revision"] == state_1["revision"]
        monitor.store.instances["vps-1"] = [{"name": "new-bot"}]
        state_3 = await daemon.dispatch("state.get", {})
        assert state_3["revision"] == state_2["revision"] + 1
        assert state_3["pool"]["connections"]["vps-1"]["status"] == "connected"
        with pytest.raises(RuntimeError, match="not supported"):
            await daemon.dispatch("daemon.stop", {})

    asyncio.run(scenario())


class StateClient:
    """Return mutable state snapshots and record all proxy requests."""

    def __init__(self, state: dict[str, Any]) -> None:
        self.state = state
        self.calls: list[str] = []

    def call(self, method: str, params: dict[str, Any] | None = None) -> Any:
        """Return fake RPC responses synchronously."""
        del params
        self.calls.append(method)
        if method == "state.get":
            return self.state
        return True

    async def call_async(self, method: str, params: dict[str, Any] | None = None) -> Any:
        """Return fake RPC responses asynchronously."""
        return self.call(method, params)


class LocalPoolFake:
    """Track API-local pool loading and disconnection."""

    def __init__(self) -> None:
        self.loaded = 0
        self.disconnected = 0

    def load_vps_configs(self) -> list[str]:
        """Capture inventory loading."""
        self.loaded += 1
        return []

    async def disconnect_all(self) -> None:
        """Capture local pool shutdown."""
        self.disconnected += 1

    def get_connection(self, hostname: str) -> None:
        """Return no local connection."""
        del hostname
        return None


def test_pb8_snapshot_stamps_survive_rpc_and_proxy_without_retimestamping(tmp_path: Path) -> None:
    """Rows and their freshness travel together through the actual daemon/proxy packet."""
    from master.vps_monitor_rpc import encode_frame

    monitor = FakeMonitor()
    source = [{"name": "alice", "running": False, "nested": {"value": 1},
               "snapshot_generated_at": 9999, "snapshot_checked_at": 9999}]
    monitor.store.update_v8_instances("vps-1", source,
                                      snapshot_generated_at=100, snapshot_checked_at=101)
    expected = [{"name": "alice", "running": False, "nested": {"value": 1},
                 "snapshot_generated_at": 100, "snapshot_checked_at": 101}]
    published = monitor.store.v8_instances["vps-1"]
    source[0]["running"] = True
    source[0]["nested"]["value"] = 2
    assert published == expected
    daemon = VPSMonitorRPCDaemon(tmp_path / "unused.sock", monitor=monitor, streamer=FakeStreamer())

    def packet() -> dict[str, Any]:
        """Serialize a full state response exactly as RPC does, without opening a socket."""
        return json.loads(encode_frame({"id": "test", "ok": True, "result": daemon._state_snapshot()}))["result"]

    client = StateClient(packet())
    proxy = VPSMonitorProxy(client=client)

    async def exercise() -> None:
        """Later diagnostics and transport polling cannot refresh the row's timestamps."""
        assert await proxy._poll_once()
        assert proxy.store.v8_instances["vps-1"] == expected
        monitor.store.update_stream_info("vps-1", {"monitor_agent": {"files": {
            "instance_snapshot.json": {"state": "ok", "generated_at": 9999, "checked_at": 9999},
        }}})
        client.state = packet()
        assert await proxy._poll_once()
        assert proxy.store.v8_instances["vps-1"] == expected
        monitor.store.update_v8_instances("vps-1", published)
        assert published == expected
        client.state = packet()
        assert await proxy._poll_once()
        assert proxy.store.v8_instances["vps-1"] == [{"name": "alice", "running": False, "nested": {"value": 1}}]

    asyncio.run(exercise())


def test_pb8_cache_hydration_preserves_generation_and_legacy_unknown() -> None:
    """Cache restart preserves exact stamps and never upgrades unstamped legacy rows."""
    from master.async_monitor import VPSMonitor

    tagged = {"name": "alice", "running": False, "snapshot_generated_at": 100, "snapshot_checked_at": 101}
    monitor = object.__new__(VPSMonitor)
    monitor.store = VPSStore()
    monitor._monitor_cache = {
        "vps-1": {"v8_instances": [tagged]},
        "legacy": {"v8_instances": [{"name": "bob", "running": False}]},
    }
    monitor._hydrate_monitor_cache()
    assert monitor.store.v8_instances["vps-1"] == [tagged]
    assert monitor.store.v8_instances["legacy"] == [{"name": "bob", "running": False}]
    monitor.store.update_v8_instances("vps-1", monitor.store.v8_instances["vps-1"])
    assert monitor._monitor_cache["vps-1"]["v8_instances"] == [tagged]


def test_proxy_hydrates_system_metrics_only_for_new_revision() -> None:
    """Polling turns serialized metrics back into SystemMetrics and honors revisions."""
    state = {
        "boot_id": "boot-1",
        "revision": 4,
        "enabled_hosts": ["vps-1"],
        "alert_settings": {"offline_gui": True},
        "upstream_releases": {"repositories": {"pb7": {"target_commit": "b" * 40}}},
        "pool": {"total": 1, "connected": 1, "connections": {"vps-1": {"status": "connected"}}},
        "store": {
            "system": {"vps-1": {"timestamp": 10.0, "cpu": 22.5, "mem_total": 100}},
            "instances": {"vps-1": [{"name": "bot"}]},
            "ui_settings": {"debug_logging": "true"},
            "local_logs": [],
        },
    }
    client = StateClient(state)
    proxy = VPSMonitorProxy(client=client)

    async def scenario() -> None:
        assert await proxy._poll_once() is True
        assert isinstance(proxy.store.system["vps-1"], SystemMetrics)
        assert proxy.store.system["vps-1"].cpu == 22.5
        assert proxy.enabled_hosts == {"vps-1"}
        assert proxy._debug_logging is True
        assert proxy.get_upstream_release_status()["repositories"]["pb7"]["target_commit"] == "b" * 40
        assert proxy.upstream_release_capability is True
        proxy.store.changed.clear()
        state["store"]["system"]["vps-1"]["cpu"] = 99.0
        assert await proxy._poll_once() is True
        assert proxy.store.system["vps-1"].cpu == 22.5
        assert proxy.store.changed.is_set() is False
        state["revision"] = 5
        assert await proxy._poll_once() is True
        assert proxy.store.system["vps-1"].cpu == 99.0
        assert proxy.store.changed.is_set() is True

    asyncio.run(scenario())


def test_proxy_marks_legacy_daemon_without_release_snapshot_capability() -> None:
    """A valid old daemon state is distinguishable from a temporary RPC outage."""
    state = {
        "boot_id": "legacy-boot",
        "revision": 1,
        "enabled_hosts": [],
        "alert_settings": {},
        "pool": {},
        "store": {},
    }
    proxy = VPSMonitorProxy(client=StateClient(state))

    assert asyncio.run(proxy._poll_once()) is True
    assert proxy.available is True
    assert proxy.upstream_release_capability is False


def test_daemon_exposes_and_refreshes_upstream_release_snapshot() -> None:
    """Release state survives behind the daemon RPC contract and has an explicit wake action."""
    monitor = FakeMonitor()
    daemon = VPSMonitorRPCDaemon(monitor=monitor, streamer=FakeStreamer())

    state = daemon._state_snapshot()
    refreshed = asyncio.run(daemon.dispatch("releases.refresh", {}))

    assert state["upstream_releases"] == monitor.release_status
    assert refreshed is True
    assert ("release-refresh", None) in monitor.calls


def test_api_proxy_stop_never_requests_daemon_shutdown() -> None:
    """Proxy shutdown cancels polling and disconnects only its lazy local pool."""
    state = {
        "boot_id": "boot-1",
        "revision": 1,
        "enabled_hosts": [],
        "alert_settings": {},
        "pool": {"total": 0, "connected": 0, "connections": {}},
        "store": {},
    }
    client = StateClient(state)
    local_pool = LocalPoolFake()
    proxy = VPSMonitorProxy(client=client, poll_interval=60, pool_factory=lambda: local_pool)

    async def scenario() -> None:
        await proxy.start()
        assert proxy.pool.get_connection("vps-1") is None
        await proxy.stop()

    asyncio.run(scenario())
    assert local_pool.loaded == 1
    assert local_pool.disconnected == 1
    assert "daemon.stop" not in client.calls
    assert "shutdown" not in client.calls


def test_real_daemon_survives_sequential_api_proxy_lifecycles(tmp_path: Path) -> None:
    """Stopping and recreating API proxies preserves one daemon monitor generation."""

    async def scenario() -> None:
        socket_path = tmp_path / "runtime" / "vps-monitor.sock"
        monitor = FakeMonitor()
        daemon = VPSMonitorRPCDaemon(socket_path, monitor=monitor, streamer=FakeStreamer())
        await daemon.start()
        try:
            first = VPSMonitorProxy(socket_path=socket_path, poll_interval=60, pool_factory=LocalPoolFake)
            await first.start()
            boot_id = first.boot_id
            await first.stop()
            assert monitor.started == 1
            assert monitor.stopped == 0

            second = VPSMonitorProxy(socket_path=socket_path, poll_interval=60, pool_factory=LocalPoolFake)
            await second.start()
            assert second.boot_id == boot_id
            assert monitor.started == 1
            assert monitor.stopped == 0
            await second.stop()
        finally:
            await daemon.stop()
        assert monitor.stopped == 1

    asyncio.run(scenario())


def test_daemon_deduplicates_detached_remote_log_streams(tmp_path: Path) -> None:
    """A reconnecting API reuses the existing daemon-owned SSH tail."""

    class CountingStreamer(FakeStreamer):
        """Count physical remote stream starts."""

        def __init__(self) -> None:
            super().__init__()
            self.starts = 0

        async def start_stream(self, hostname: str, path: str) -> str:
            self.starts += 1
            return await super().start_stream(hostname, path)

    async def scenario() -> None:
        streamer = CountingStreamer()
        daemon = VPSMonitorRPCDaemon(tmp_path / "rpc" / "sock", monitor=FakeMonitor(), streamer=streamer)
        first = await daemon.dispatch("logs.start", {"hostname": "vps-1", "service_or_path": "PBRun"})
        second = await daemon.dispatch("logs.start", {"hostname": "vps-1", "service_or_path": "PBRun"})
        assert first == second
        assert streamer.starts == 1

    asyncio.run(scenario())


def test_proxy_marks_cached_connections_unavailable_and_reloads_inventory() -> None:
    """Daemon outages are not shown as connected and host edits refresh the lazy pool."""

    state = {
        "boot_id": "boot-1",
        "revision": 1,
        "enabled_hosts": ["vps-1"],
        "alert_settings": {},
        "pool": {"total": 1, "connected": 1, "disconnected": 0, "auth_failed": 0, "connections": {"vps-1": {"status": "connected"}}},
        "store": {},
    }
    client = StateClient(state)
    local_pool = LocalPoolFake()
    proxy = VPSMonitorProxy(client=client, pool_factory=lambda: local_pool)

    async def scenario() -> None:
        await proxy._poll_once()
        proxy.pool.get_connection("vps-1")
        assert local_pool.loaded == 1
        await proxy.refresh_enabled_host("vps-1")
        assert local_pool.loaded == 2
        proxy.pool.set_available(False)
        status = proxy.pool.get_status_summary()
        assert status["connected"] == 0
        assert status["connections"]["vps-1"]["status"] == "disconnected"

    asyncio.run(scenario())


def test_proxy_recovers_after_malformed_state_response() -> None:
    """Version-skewed state cannot permanently terminate proxy polling."""

    state = {
        "boot_id": "boot-1",
        "revision": "invalid",
        "enabled_hosts": [],
        "alert_settings": {},
        "pool": {},
        "store": {},
    }
    client = StateClient(state)
    proxy = VPSMonitorProxy(client=client)

    async def scenario() -> None:
        assert await proxy._poll_once() is False
        assert proxy.available is False
        state["revision"] = 1
        assert await proxy._poll_once() is True
        assert proxy.available is True

    asyncio.run(scenario())
