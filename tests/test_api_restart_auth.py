"""Regression tests for shared API restart authentication."""

import asyncio
from pathlib import Path
import subprocess
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from fastapi import HTTPException
from fastapi.routing import APIRoute

import PBApiServer
import credential_process_registry
from api.auth import require_auth


def test_server_restart_uses_shared_auth_dependency() -> None:
    """The restart route must accept the HttpOnly browser session cookie."""
    route = next(
        route
        for route in PBApiServer.app.routes
        if isinstance(route, APIRoute)
        and route.path == "/api/server-restart"
        and "POST" in route.methods
    )

    assert any(dependency.call is require_auth for dependency in route.dependant.dependencies)


def test_shared_nav_does_not_send_an_undefined_bearer_token() -> None:
    """Cookie-only pages must not manufacture a Bearer undefined header."""
    source = Path("frontend/pbgui_nav.js").read_text(encoding="utf-8")
    restart_block = source[source.index("fetch(origin2 + '/api/server-restart'"):source.index("showRestartOverlay(origin2", source.index("fetch(origin2 + '/api/server-restart'"))]

    assert "authOptions({ method: 'POST' })" in restart_block
    assert "Authorization" not in restart_block
    assert "JSON.stringify({ token: c2.token })" not in restart_block


def test_shared_nav_releases_restart_watchers_on_pagehide() -> None:
    """Repeated page navigation must not retain SSE streams or polling timers."""
    source = Path("frontend/pbgui_nav.js").read_text(encoding="utf-8")

    assert "window.addEventListener('pagehide', stopRestartStatusWatch);" in source
    assert "_restartEventSource.close();" in source
    assert "clearTimeout(_restartRetryTimer);" in source
    assert "clearInterval(_restartPollTimer);" in source
    assert "if (event && event.persisted) startRestartStatusWatch();" in source


def test_server_status_stream_closes_before_api_restart(monkeypatch) -> None:
    """The persistent nav SSE must not consume Uvicorn's graceful-shutdown timeout."""
    monkeypatch.setattr(
        PBApiServer,
        "_runtime_service_restart_state",
        lambda: {"current_serial": "1", "stale_services": [], "inspection_error": ""},
    )

    async def scenario() -> None:
        response = await PBApiServer.server_status_stream(session=object())
        iterator = response.body_iterator
        first = await anext(iterator)
        assert str(first).startswith("data:")

        PBApiServer._close_server_status_streams()

        try:
            await anext(iterator)
        except StopAsyncIteration:
            pass
        else:
            raise AssertionError("Server status stream stayed open after restart signal")

    asyncio.run(scenario())


def test_runtime_service_restart_state_detects_only_active_stale_daemons(monkeypatch) -> None:
    """Managed daemon startup serials are compared without including detached workers."""

    processes = [
        {"pid": 10, "create_time": 1.0, "service": "PBRun"},
        {"pid": 11, "create_time": 2.0, "service": "PBData"},
        {"pid": 12, "create_time": 3.0, "service": "Market Data worker"},
        {"pid": 13, "create_time": 4.0, "service": "VastPool"},
    ]
    monkeypatch.setattr(PBApiServer, "_read_serial", lambda: 2052)
    monkeypatch.setattr(PBApiServer, "_vps_monitor", None)
    monkeypatch.setattr(credential_process_registry, "running_relevant_processes", lambda _root: processes)
    monkeypatch.setattr(
        credential_process_registry,
        "process_barrier_readiness",
        lambda _root, processes: {
            "services": [
                {"service": "PBRun", "code_serial": "2052"},
                {"service": "PBData", "code_serial": "2051"},
                {"service": "Market Data worker", "code_serial": "2051"},
                {"service": "VastPool", "code_serial": "2050"},
            ],
        },
    )

    state = PBApiServer._runtime_service_restart_state()

    assert [item["service"] for item in state["stale_services"]] == ["VastPool", "PBData"]
    assert state["stale_services"][0]["unit"] == "pbgui-vast-pool.service"
    assert state["stale_services"][1]["unit"] == "pbgui-pbdata.service"


def test_server_status_includes_stale_managed_daemons(monkeypatch) -> None:
    """The shared restart button remains visible when only another daemon is stale."""

    monkeypatch.setattr(PBApiServer, "_startup_serial", 2052)
    monkeypatch.setattr(PBApiServer, "_read_serial", lambda: 2052)
    monkeypatch.setattr(PBApiServer, "_runtime_restart_reasons", [])
    monkeypatch.setattr(PBApiServer, "_restart_block_state", lambda: asyncio.sleep(0, result=(False, "")))
    monkeypatch.setattr(
        PBApiServer,
        "_runtime_service_restart_state",
        lambda: {
            "current_serial": "2052",
            "inspection_error": "",
            "stale_services": [{
                "service": "PBRun",
                "label": "PBRun",
                "unit": "pbgui-pbrun.service",
                "running_serial": "2051",
                "current_serial": "2052",
                "reason": "outdated code serial",
            }],
        },
    )

    payload = asyncio.run(PBApiServer.server_status(session=object()))

    assert payload["needs_restart"] is True
    assert payload["api_restart_required"] is False
    assert payload["service_restart_required"] is True
    assert [item["service"] for item in payload["restart_services"]] == ["PBRun"]


def test_missing_monitor_unit_keeps_restart_prompt_visible(monkeypatch) -> None:
    """Compatibility mode requests the one-time systemd monitor handoff."""

    monkeypatch.setattr(PBApiServer, "_startup_serial", 2154)
    monkeypatch.setattr(PBApiServer, "_read_serial", lambda: 2154)
    monkeypatch.setattr(PBApiServer, "_runtime_restart_reasons", ["VPS Monitor systemd migration required"])
    monkeypatch.setattr(PBApiServer, "_restart_block_state", lambda: asyncio.sleep(0, result=(False, "")))
    monkeypatch.setattr(
        PBApiServer,
        "_runtime_service_restart_state",
        lambda: {"current_serial": "2154", "inspection_error": "", "stale_services": []},
    )

    payload = asyncio.run(PBApiServer.server_status(session=object()))

    assert payload["needs_restart"] is True
    assert payload["api_restart_required"] is True
    assert "VPS Monitor systemd migration required" in payload["runtime_restart_reasons"]


@pytest.mark.parametrize("platform", ["darwin", "win32"])
def test_systemd_ownership_skips_non_linux_platforms(monkeypatch, platform) -> None:
    """Non-Linux API processes need neither systemctl nor a user-manager lookup."""
    run = Mock(side_effect=AssertionError("systemctl must not run"))
    env = Mock(side_effect=AssertionError("user environment must not be inspected"))
    monkeypatch.setattr(PBApiServer.sys, "platform", platform)
    monkeypatch.setattr(PBApiServer.subprocess, "run", run)
    monkeypatch.setattr(PBApiServer, "_systemd_user_env", env)

    assert PBApiServer._systemd_owns_current_api() is False
    run.assert_not_called()
    env.assert_not_called()


@pytest.mark.parametrize(
    ("returncode", "stdout", "expected"),
    [
        (1, "", False),
        (0, "", False),
        (0, "malformed response", False),
        (0, "ActiveState=active\n", False),
        (0, "MainPID={pid}\n", False),
        (0, "ActiveState=inactive\nMainPID={pid}\n", False),
        (0, "ActiveState=active\nMainPID=0\n", False),
        (0, "ActiveState=active\nMainPID=invalid\n", False),
        (0, "ActiveState=active\nMainPID=\n", False),
        (0, "ActiveState=active\nMainPID={foreign_pid}\n", False),
        (0, "ActiveState=active\nMainPID={pid}\n", True),
    ],
)
def test_systemd_ownership_requires_active_unit_with_own_pid(
    monkeypatch, returncode, stdout, expected,
) -> None:
    """Only a successful, active API unit with this MainPID permits migration."""
    pid = PBApiServer.os.getpid()
    run = Mock(return_value=subprocess.CompletedProcess(
        [], returncode, stdout=stdout.format(pid=pid, foreign_pid=pid + 1), stderr="",
    ))
    env = {"XDG_RUNTIME_DIR": "/isolated/user-runtime"}
    monkeypatch.setattr(PBApiServer.sys, "platform", "linux")
    monkeypatch.setattr(PBApiServer.subprocess, "run", run)
    monkeypatch.setattr(PBApiServer, "_systemd_user_env", lambda: env)

    assert PBApiServer._systemd_owns_current_api() is expected
    run.assert_called_once_with(
        ["systemctl", "--user", "show", "pbgui-api.service", "-p", "ActiveState", "-p", "MainPID"],
        check=False, capture_output=True, text=True, timeout=5, env=env,
    )


@pytest.mark.parametrize(
    "error",
    [FileNotFoundError("systemctl"), PermissionError("denied"), OSError("unavailable"),
     subprocess.TimeoutExpired("systemctl", 5), RuntimeError("unexpected failure")],
)
def test_systemd_ownership_handles_unavailable_manager(monkeypatch, error) -> None:
    """Failed bounded inspections cannot create an actionable migration reason."""
    run = Mock(side_effect=error)
    log = Mock()
    monkeypatch.setattr(PBApiServer.sys, "platform", "linux")
    monkeypatch.setattr(PBApiServer.subprocess, "run", run)
    monkeypatch.setattr(PBApiServer, "_systemd_user_env", lambda: {})
    monkeypatch.setattr(PBApiServer, "_log", log)

    assert PBApiServer._systemd_owns_current_api() is False
    run.assert_called_once()
    if isinstance(error, RuntimeError):
        assert log.call_args.kwargs["level"] == "ERROR"
        assert "RuntimeError" in log.call_args.kwargs["meta"]["traceback"]
    else:
        log.assert_not_called()


@pytest.fixture
def isolated_monitor_lifespan(monkeypatch, tmp_path):
    """Replace every productive startup/shutdown boundary with isolated doubles."""
    import api.auth
    import api.v7_instances
    import api.v8_instances
    import api.vps
    import pbcoindata_process

    home = tmp_path / "home"
    monkeypatch.setattr(Path, "home", classmethod(lambda _cls: home))
    monkeypatch.setattr(PBApiServer, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(PBApiServer, "_PBGUI_ROOT", tmp_path)
    heartbeat = SimpleNamespace(__enter__=Mock(), close=Mock())
    monkeypatch.setattr(credential_process_registry, "ProcessCapabilityHeartbeat", Mock(return_value=heartbeat))
    for name in (
        "harden_sensitive_paths", "_setup_api_logging", "bootstrap_local_legacy_credentials",
        "run_credential_migration", "persist_credential_migration_error", "db_tools_init",
        "bt7_startup", "bt8_startup", "opt7_startup", "opt8_startup", "coin_data_startup",
        "ohlcv_preload_startup", "vps_manager_startup", "vast_startup", "market_data_startup",
        "strategy_explorer_v8_startup", "profit_sweep_startup", "loop8_startup",
    ):
        monkeypatch.setattr(PBApiServer, name, Mock())
    monkeypatch.setattr(PBApiServer, "load_ini", Mock(return_value=None))
    monkeypatch.setattr(PBApiServer, "run_startup_migrations", Mock(return_value={"skipped": True, "completed": []}))
    monkeypatch.setattr(PBApiServer, "reconcile_pending_credentials", Mock(return_value={}))
    for name in (
        "ai_capabilities_startup", "_worker_watchdog_loop", "_serial_watcher_loop",
        "loop8_shutdown", "auth_shutdown", "ai_chat_shutdown", "live_shutdown",
        "dashboard_shutdown", "heatmap_shutdown", "pareto_explorer_shutdown",
        "strategy_explorer_v8_shutdown", "coin_data_shutdown", "market_data_shutdown",
        "profit_sweep_shutdown", "vps_manager_shutdown", "vast_shutdown", "cluster_shutdown",
        "db_tools_shutdown", "bt7_shutdown", "bt8_shutdown", "opt7_shutdown", "opt8_shutdown",
    ):
        monkeypatch.setattr(PBApiServer, name, AsyncMock())
    for module in (api.vps, api.v7_instances, api.v8_instances):
        monkeypatch.setattr(module, "init", Mock())
    monkeypatch.setattr(api.auth, "cleanup_expired_tokens", Mock(return_value=0))
    monkeypatch.setattr(pbcoindata_process, "reap_children", Mock())
    monitor = SimpleNamespace(pool=object(), client=object(), start=AsyncMock(), stop=AsyncMock())
    in_process = Mock(return_value=monitor)
    proxy = Mock(return_value=monitor)
    monkeypatch.setitem(PBApiServer.sys.modules, "master.async_monitor", SimpleNamespace(VPSMonitor=in_process))
    monkeypatch.setitem(PBApiServer.sys.modules, "master.async_logs", SimpleNamespace(AsyncLogStreamer=Mock()))
    monkeypatch.setitem(PBApiServer.sys.modules, "master.vps_monitor_client", SimpleNamespace(
        VPSMonitorProxy=proxy, RemoteLogStreamerProxy=Mock(),
    ))
    read_serial = Mock(return_value=500)
    monkeypatch.setattr(PBApiServer, "_read_serial", read_serial)
    monkeypatch.setattr(PBApiServer, "_startup_serial", 0)
    monkeypatch.setattr(PBApiServer, "_needs_restart", False)
    monkeypatch.setattr(PBApiServer, "_runtime_restart_reasons", [])
    monkeypatch.setattr(PBApiServer, "_vps_monitor", None)
    monkeypatch.setattr(PBApiServer, "_vps_monitor_in_process", False)
    monkeypatch.setattr(PBApiServer, "_runtime_service_restart_state", Mock(return_value={"stale_services": []}))
    log = Mock()
    monkeypatch.setattr(PBApiServer, "_log", log)
    monkeypatch.setattr(PBApiServer, "_systemd_user_env", Mock(return_value={}))
    run_threads = []

    def fake_run(*_args, **_kwargs):
        """Record inspection placement without contacting a real user manager."""
        run_threads.append(threading.get_ident())
        return subprocess.CompletedProcess(
            [], 0, stdout=f"ActiveState=active\nMainPID={PBApiServer.os.getpid()}\n", stderr="",
        )

    run = Mock(side_effect=fake_run)
    monkeypatch.setattr(PBApiServer.subprocess, "run", run)
    return SimpleNamespace(
        home=home, monitor=monitor, in_process=in_process, proxy=proxy,
        run=run, run_threads=run_threads, read_serial=read_serial, log=log, heartbeat=heartbeat,
    )


@pytest.mark.parametrize("mode", ["darwin", "win32", "linux-unmanaged", "linux-owned", "proxy"])
@pytest.mark.parametrize("serial_changed", [False, True])
def test_lifespan_only_requests_actionable_monitor_migration(
    monkeypatch, isolated_monitor_lifespan, mode, serial_changed,
) -> None:
    """Real startup branching leaves normal in-process mode current and serial checks intact."""
    state = isolated_monitor_lifespan
    monkeypatch.setattr(PBApiServer.sys, "platform", mode if mode in {"darwin", "win32"} else "linux")
    if mode == "linux-unmanaged":
        state.run.side_effect = None
        state.run.return_value = subprocess.CompletedProcess([], 1, stdout="", stderr="No user manager")
    if mode == "proxy":
        unit = state.home / ".config/systemd/user/pbgui-vps-monitor.service"
        unit.parent.mkdir(parents=True)
        unit.write_text("# isolated test unit\n", encoding="utf-8")

    async def scenario() -> None:
        """Exercise the actual lifespan with only temporary paths and mock resources."""
        async with PBApiServer._lifespan(PBApiServer.app):
            assert PBApiServer._vps_monitor is state.monitor
            assert PBApiServer._vps_monitor_in_process is (mode != "proxy")
            if serial_changed:
                state.read_serial.return_value += 1
            payload = PBApiServer._restart_status_payload()
            migration = mode == "linux-owned"
            assert payload["runtime_restart_reasons"] == (
                ["VPS Monitor systemd migration required"] if migration else []
            )
            assert payload["serial_restart_required"] is serial_changed
            assert payload["api_restart_required"] is (migration or serial_changed)
            assert payload["needs_restart"] is (migration or serial_changed)
            assert [item["label"] for item in payload["restart_services"]] == (
                ["PBGui API Server"] if migration or serial_changed else []
            )

    asyncio.run(scenario())
    state.monitor.stop.assert_awaited_once()
    state.heartbeat.close.assert_called_once()
    if mode == "proxy":
        state.proxy.assert_called_once()
        state.in_process.assert_not_called()
    else:
        state.in_process.assert_called_once()
        state.proxy.assert_not_called()
    if mode in {"darwin", "win32", "proxy"}:
        state.run.assert_not_called()
    else:
        state.run.assert_called_once()
    if mode == "linux-owned":
        assert state.run_threads and all(ident != threading.get_ident() for ident in state.run_threads)
    if mode in {"darwin", "win32", "linux-unmanaged"}:
        assert any(
            call.args[1] == "[lifespan] using in-process VPS monitor" and call.kwargs["level"] == "INFO"
            for call in state.log.call_args_list
        )


def test_restart_rechecks_systemd_ownership_before_handoff(monkeypatch) -> None:
    """A restart must inspect current ownership again rather than trust startup state."""
    ownership = Mock(side_effect=[True, False])
    prepare = Mock(return_value=(True, "prepared"))
    queue = Mock(return_value=(True, "queued"))
    monkeypatch.setattr(PBApiServer, "_systemd_owns_current_api", ownership)
    monkeypatch.setattr(PBApiServer, "_vps_monitor_in_process", True)
    monkeypatch.setattr(PBApiServer, "_prepare_vps_monitor_systemd_unit", prepare)
    monkeypatch.setattr(PBApiServer, "_queue_current_api_systemd_restart", queue)
    monkeypatch.setattr(PBApiServer, "_log", Mock())

    assert PBApiServer._restart_current_api_systemd_unit() is True
    assert PBApiServer._restart_current_api_systemd_unit() is False
    assert ownership.call_count == 2
    prepare.assert_called_once()
    queue.assert_called_once()


def test_shared_nav_restarts_all_reported_services_and_waits_for_serials() -> None:
    """The nav dialog lists stale daemons and waits for the combined status to clear."""

    source = Path("frontend/pbgui_nav.js").read_text(encoding="utf-8")

    assert "Restart all PBGui services running outdated code?" in source
    assert "Local services running outdated code: " in source
    assert "Restarting PBGui Services" in source
    assert "fetch(apiBase + '/api/server-status'" in source


def test_restart_overlay_follows_up_once_for_services_discovered_by_new_api() -> None:
    """An upgrade restart should finish daemons unknown to the pre-update API process."""
    source = Path("frontend/pbgui_nav.js").read_text(encoding="utf-8")

    assert "var remainingRestartRequested = false;" in source
    assert "data && Array.isArray(data.restart_services) ? data.restart_services : []" in source
    assert "var requestedRestartServices = {};" in source
    assert "!requestedRestartServices[label]" in source
    assert "data.service_restart_required && !data.api_restart_required" in source
    assert "newlyDiscovered.length" in source
    assert "remainingRestartRequested = true;" in source
    assert "fetch(apiBase + '/api/server-restart'" in source
    assert "attempts = 0;" in source
    assert "closeButton.id = 'pbgui-restart-close';" in source
    assert "pbgui-restart-reload" not in source


def test_persistent_vps_monitor_is_an_allowlisted_managed_restart_target() -> None:
    """A version-skewed monitor can join a coordinated restart without broad unit access."""

    units = {str(item["unit"]) for item in PBApiServer._RUNTIME_SYSTEMD_SERVICES}

    assert "pbgui-vps-monitor.service" in units


def test_vast_pool_is_an_allowlisted_managed_restart_target() -> None:
    """The scheduler reloads changed rental code without ending active rentals."""

    units = {str(item["unit"]) for item in PBApiServer._RUNTIME_SYSTEMD_SERVICES}

    assert "pbgui-vast-pool.service" in units


def test_missing_release_capability_marks_only_persistent_monitor_stale(monkeypatch) -> None:
    """An old daemon is restartable after Git pull while an unpolled proxy remains neutral."""
    monkeypatch.setattr(PBApiServer, "_read_serial", lambda: 2052)
    monkeypatch.setattr(credential_process_registry, "running_relevant_processes", lambda _root: [])
    monkeypatch.setattr(
        credential_process_registry,
        "process_barrier_readiness",
        lambda _root, processes: {"services": []},
    )
    monkeypatch.setattr(
        PBApiServer,
        "_vps_monitor",
        SimpleNamespace(upstream_release_capability=False),
    )

    stale = PBApiServer._runtime_service_restart_state()["stale_services"]

    assert stale == [{
        "service": "VPSMonitor",
        "label": "VPS Monitor",
        "unit": "pbgui-vps-monitor.service",
        "running_serial": "legacy",
        "current_serial": "2052",
        "reason": "upstream release capability missing",
    }]


def test_missing_package_check_capability_marks_persistent_monitor_stale(monkeypatch) -> None:
    """The shared restart includes a daemon too old for explicit package checks."""
    monkeypatch.setattr(PBApiServer, "_read_serial", lambda: 2052)
    monkeypatch.setattr(credential_process_registry, "running_relevant_processes", lambda _root: [])
    monkeypatch.setattr(
        credential_process_registry,
        "process_barrier_readiness",
        lambda _root, processes: {"services": []},
    )
    monkeypatch.setattr(
        PBApiServer,
        "_vps_monitor",
        SimpleNamespace(upstream_release_capability=True, package_check_capability=False),
    )

    stale = PBApiServer._runtime_service_restart_state()["stale_services"]

    assert stale[0]["service"] == "VPSMonitor"
    assert stale[0]["unit"] == "pbgui-vps-monitor.service"
    assert stale[0]["reason"] == "package check capability missing"


def test_blocked_restart_releases_master_update_reservation(monkeypatch) -> None:
    """Restart reserves against new updates and releases that reservation when another blocker wins."""
    class Lease:
        released = False

        def release(self) -> None:
            self.released = True

    lease = Lease()
    monkeypatch.setattr(PBApiServer, "acquire_master_update_lock", lambda _path: lease)
    monkeypatch.setattr(PBApiServer, "_restart_block_state", lambda: asyncio.sleep(0, result=(True, "busy")))

    with pytest.raises(HTTPException) as error:
        asyncio.run(PBApiServer.server_restart(session=object()))

    assert error.value.status_code == 409
    assert lease.released is True


def test_root_restart_spawn_ack_failure_returns_error_before_success(monkeypatch) -> None:
    """The root async route does not claim success when direct child preparation fails."""
    class Lease:
        """Track release and detach operations for a restart reservation."""

        def __init__(self) -> None:
            self.releases = 0

        def release(self) -> None:
            self.releases += 1

        def detach(self) -> None:
            return None

    leases = (Lease(), Lease(), Lease())
    threaded: list[str] = []

    async def to_thread(function, *args, **kwargs):
        threaded.append(function.__name__)
        return function(*args, **kwargs)

    monkeypatch.setattr(PBApiServer.asyncio, "to_thread", to_thread)
    monkeypatch.setattr(PBApiServer, "_acquire_api_restart_leases", lambda: leases)
    monkeypatch.setattr(PBApiServer, "_restart_block_state", lambda: asyncio.sleep(0, result=(False, "")))
    monkeypatch.setattr(
        PBApiServer,
        "_restart_status_payload",
        lambda: {"restart_services": [{"unit": "pbgui-api.service", "label": "PBGui API Server"}]},
    )
    monkeypatch.setattr(PBApiServer, "_restart_current_api_systemd_unit", lambda _units: False)
    monkeypatch.setattr(
        PBApiServer,
        "_prepare_direct_api_replacement",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("ack failed")),
    )
    monkeypatch.setattr(PBApiServer, "_log", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(PBApiServer, "_api_restart_lease", None)

    with pytest.raises(HTTPException) as error:
        asyncio.run(PBApiServer.server_restart(session=object()))

    assert error.value.status_code == 500
    assert "ack failed" in str(error.value.detail)
    assert [lease.releases for lease in leases] == [1, 1, 1]
    assert PBApiServer._api_restart_lease is None
    assert threaded == [
        "<lambda>",
        "<lambda>",
        "<lambda>",
        "<lambda>",
        "_release_api_restart_leases",
    ]


def test_root_restart_lock_order_is_global_service_then_update(monkeypatch) -> None:
    """Root restart uses one stable order shared with individual service actions."""
    events: list[str] = []

    class Lease:
        """Minimal ordered lease double."""

        def release(self) -> None:
            events.append("release")

    monkeypatch.setattr(
        PBApiServer,
        "acquire_service_lifecycle_lock",
        lambda _root, service, _action, timeout: events.append(service) or Lease(),
    )
    monkeypatch.setattr(
        PBApiServer,
        "acquire_master_update_lock",
        lambda _root: events.append("master-update") or Lease(),
    )

    leases = PBApiServer._acquire_api_restart_leases()

    assert events == ["all-services", "api-server", "master-update"]
    PBApiServer._release_api_restart_leases(leases)


def test_restart_blockers_keep_local_registry_reads_on_event_loop(monkeypatch) -> None:
    """Only the blocking VPS deployment inspection is delegated to a worker thread."""
    from api import cluster, coin_data, dashboard, db_tools, pareto_explorer, vps_manager, loop_optimizer_v8

    event_loop_thread = threading.get_ident()
    local_threads: list[int] = []
    external_threads: list[int] = []

    def local_reason(*_args) -> str:
        local_threads.append(threading.get_ident())
        return ""

    for module in (cluster, coin_data, dashboard, db_tools, pareto_explorer):
        monkeypatch.setattr(module, "restart_block_reason", local_reason)
    monkeypatch.setattr(PBApiServer, "profit_sweep_restart_block_reason", local_reason)
    monkeypatch.setattr(PBApiServer, "ai_restart_block_reason", local_reason)
    monkeypatch.setattr(PBApiServer, "credential_migration_restart_block_reason", local_reason)
    monkeypatch.setattr(loop_optimizer_v8, "restart_block_reason", local_reason)

    def inspect_deploys() -> dict[str, bool]:
        external_threads.append(threading.get_ident())
        return {"active": False}

    monkeypatch.setattr(
        vps_manager,
        "get_service_instance",
        lambda: SimpleNamespace(active_vps_deploy_summary=inspect_deploys),
    )

    assert asyncio.run(PBApiServer._restart_block_state()) == (False, "")
    assert local_threads == [event_loop_thread] * 9
    assert len(external_threads) == 1
    assert external_threads[0] != event_loop_thread


def test_guard_handover_blocks_api_restart(monkeypatch) -> None:
    """A synchronous guard migration participates in the actual API restart check."""
    import vast_guard_migration
    from api import cluster, coin_data, dashboard, db_tools, pareto_explorer

    for module in (cluster, coin_data, dashboard, db_tools, pareto_explorer):
        monkeypatch.setattr(module, 'restart_block_reason', lambda: '')
    for name in ('profit_sweep_restart_block_reason', 'ai_restart_block_reason',
                 'credential_migration_restart_block_reason'):
        monkeypatch.setattr(PBApiServer, name, lambda *args: '')
    monkeypatch.setattr(vast_guard_migration, '_ACTIVE_OPERATIONS', 1)
    blocked, reason = asyncio.run(PBApiServer._restart_block_state())
    assert blocked
    assert 'Vast deadline guard upgrade' in reason


def test_restart_status_identifies_running_api_instance(monkeypatch):
    """Identical serials do not conceal whether the old or replacement API responds."""
    monkeypatch.setattr(PBApiServer, '_read_serial', lambda: 1)
    monkeypatch.setattr(PBApiServer, '_refresh_restart_state', lambda: False)
    monkeypatch.setattr(PBApiServer, '_runtime_service_restart_state', lambda: {})
    monkeypatch.setattr(PBApiServer, '_api_instance_id', 'old-instance')
    assert PBApiServer._restart_status_payload()['api_instance_id'] == 'old-instance'
    monkeypatch.setattr(PBApiServer, '_api_instance_id', 'replacement-instance')
    assert PBApiServer._restart_status_payload()['api_instance_id'] == 'replacement-instance'


@pytest.fixture(autouse=True)
def isolated_cloud_supervisor_discovery(monkeypatch):
    """Existing API tests must not inspect actual user rental processes."""
    monkeypatch.setattr(PBApiServer, '_vast_supervisor_restart_state', lambda: [])


@pytest.mark.parametrize('stream', [False, True])
def test_restart_inspection_does_not_block_other_api_requests(monkeypatch, stream) -> None:
    """Slow daemon inspection must yield the event loop for ordinary status requests."""
    import time

    def slow_status():
        """Simulate a slow synchronous process or file inspection."""
        time.sleep(0.2)
        return {'needs_restart': True}

    async def unblocked():
        """Supply an isolated restart guard result."""
        return False, ''

    monkeypatch.setattr(PBApiServer, '_restart_status_payload', slow_status)
    monkeypatch.setattr(PBApiServer, '_restart_block_state', unblocked)
    monkeypatch.setattr(PBApiServer, 'auth_runtime_status', lambda: {})
    monkeypatch.setattr(PBApiServer, '_local_master_name', lambda: 'test')

    async def scenario():
        iterator = None
        if stream:
            response = await PBApiServer.server_status_stream(session=object())
            iterator = response.body_iterator
            task = asyncio.create_task(anext(iterator))
        else:
            task = asyncio.create_task(PBApiServer.server_status(session=object()))
        try:
            await asyncio.sleep(0.02)
            assert not task.done(), 'Restart inspection blocked the event loop'
            await task
        finally:
            await asyncio.gather(task, return_exceptions=True)
            if iterator is not None:
                await iterator.aclose()

    asyncio.run(scenario())
