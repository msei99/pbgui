"""Regression tests for shared API restart authentication."""

import asyncio
from pathlib import Path
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

    assert "authOptions(c2.token" in restart_block
    assert "'Authorization': 'Bearer ' + c2.token" not in restart_block
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
