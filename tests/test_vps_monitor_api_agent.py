"""Cookie-authentication and monitor-agent API regressions for VPS Monitor."""

from __future__ import annotations

import asyncio
from pathlib import Path

from fastapi.routing import APIRoute
from starlette.requests import Request

from api.auth import require_auth
import api.vps as vps_api


def _request() -> Request:
    """Build a minimal same-origin request for direct page rendering."""
    return Request({
        "type": "http",
        "scheme": "http",
        "server": ("testserver", 80),
        "path": "/api/vps/main_page",
        "headers": [],
        "query_string": b"",
    })


def test_main_page_never_reads_or_renders_session_fields() -> None:
    """The VPS page must use only the HttpOnly cookie after authentication."""
    class CookieOnlySession:
        """Reject every attempt to inspect the authenticated session object."""

        def __getattribute__(self, name: str):
            if name in {"__class__", "__dict__"}:
                return object.__getattribute__(self, name)
            raise AssertionError(f"session field accessed: {name}")

    response = vps_api.get_main_page(_request(), CookieOnlySession())
    html = response.body.decode("utf-8")

    assert "%%TOKEN%%" not in html
    assert "Authorization" not in html
    assert "Bearer" not in html
    assert "window.TOKEN" not in html
    assert '"%%WS_BASE%%"' not in html
    assert "authenticated: true" in html


def test_main_page_retains_shared_auth_dependency() -> None:
    """The standalone page must remain protected by require_auth."""
    route = next(
        route
        for route in vps_api.router.routes
        if isinstance(route, APIRoute) and route.path == "/api/vps/main_page"
    )

    assert any(dependency.call is require_auth for dependency in route.dependant.dependencies)


def test_websocket_authentication_happens_before_socket_use(monkeypatch) -> None:
    """A rejected socket must return before registration or property access."""
    calls = []

    class AuthOnlySocket:
        """Fail if ws_vps touches the socket after authentication rejects it."""

        def __getattribute__(self, name: str):
            if name in {"__class__", "__dict__"}:
                return object.__getattribute__(self, name)
            raise AssertionError(f"socket accessed before authentication: {name}")

    socket = AuthOnlySocket()

    async def reject(candidate):
        calls.append(candidate)
        return None

    monkeypatch.setattr(vps_api, "authenticate_websocket", reject)
    asyncio.run(vps_api.ws_vps(socket))

    assert calls == [socket]
    assert socket not in vps_api._clients


def test_websocket_source_keeps_authentication_first() -> None:
    """Guard the authentication call ordering against future setup changes."""
    source = Path(vps_api.__file__).read_text(encoding="utf-8")
    start = source.index("async def ws_vps")
    endpoint = source[start:source.index("# ── Push loops", start)]

    assert endpoint.index("await authenticate_websocket(websocket)") < endpoint.index("_clients.add(websocket)")
