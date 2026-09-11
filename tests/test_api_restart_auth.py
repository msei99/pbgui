"""Regression tests for shared API restart authentication."""

import asyncio
from pathlib import Path
import threading
from types import SimpleNamespace

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
            ],
        },
    )

    state = PBApiServer._runtime_service_restart_state()

    assert [item["service"] for item in state["stale_services"]] == ["PBData"]
    assert state["stale_services"][0]["unit"] == "pbgui-pbdata.service"


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


def test_shared_nav_restarts_all_reported_services_and_waits_for_serials() -> None:
    """The nav dialog lists stale daemons and waits for the combined status to clear."""

    source = Path("frontend/pbgui_nav.js").read_text(encoding="utf-8")

    assert "Restart all PBGui services running outdated code?" in source
    assert "Outdated services: " in source
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
    assert "reloadButton.id = 'pbgui-restart-reload';" in source


def test_persistent_vps_monitor_is_an_allowlisted_managed_restart_target() -> None:
    """A version-skewed monitor can join a coordinated restart without broad unit access."""

    units = {str(item["unit"]) for item in PBApiServer._RUNTIME_SYSTEMD_SERVICES}

    assert "pbgui-vps-monitor.service" in units


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
    from api import cluster, coin_data, dashboard, db_tools, pareto_explorer, vps_manager

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

    def inspect_deploys() -> dict[str, bool]:
        external_threads.append(threading.get_ident())
        return {"active": False}

    monkeypatch.setattr(
        vps_manager,
        "get_service_instance",
        lambda: SimpleNamespace(active_vps_deploy_summary=inspect_deploys),
    )

    assert asyncio.run(PBApiServer._restart_block_state()) == (False, "")
    assert local_threads == [event_loop_thread] * 8
    assert len(external_threads) == 1
    assert external_threads[0] != event_loop_thread
