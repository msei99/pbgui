"""Regression tests for shared API restart authentication."""

from pathlib import Path

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
