"""Regression tests for shared API restart authentication."""

import asyncio
from pathlib import Path

import pytest
from fastapi import HTTPException
from fastapi.routing import APIRoute

import PBApiServer
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


def test_server_status_stream_closes_before_api_restart() -> None:
    """The persistent nav SSE must not consume Uvicorn's graceful-shutdown timeout."""
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
