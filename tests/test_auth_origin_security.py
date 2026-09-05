"""Isolated HTTP/ASGI regressions for exact-origin CSRF and WebSocket boundaries."""

import ast
import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from httpx import Headers as HTTPHeaders
from httpx import ASGITransport, AsyncClient
from fastapi import Depends, FastAPI, HTTPException, Request, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from fastapi.staticfiles import StaticFiles
from fastapi.testclient import TestClient
from starlette.datastructures import Headers, URL
from starlette.websockets import WebSocketDisconnect
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware

from api import auth


ORIGIN = "https://gui.example.test"
BAD_ORIGINS = [
    "https://evil.example.test", "https://gui.example.test.evil.test",
    "http://gui.example.test", "https://gui.example.test:8443", "null", "",
    "https://gui.example.test/", "https://gui.example.test/path",
    "https://gui.example.test?", "https://gui.example.test#",
    "https://user@gui.example.test", "https://user:secret@gui.example.test",
    "https://gui.example.test@evil.test", "https://gui.example.test:bad",
    "https://gui.example.test:", "https://gui.example.test:65536",
    "https://gui.example.test,https://evil.test",
    "https://gui.example.test https://evil.test", " https://gui.example.test",
    "https://gui.example.test\t", "https://gui.example.test\n",
    "https://gui.example.test\\@evil.test", "https://gui%2eexample.test",
    "//gui.example.test", "file://gui.example.test", "https://[invalid]",
]


@pytest.fixture
def security_app(monkeypatch, tmp_path):
    """Mock all authentication persistence and side effects; never start real services."""
    session = auth.SessionToken(token="test-session", user_id="test", created_at=1, expires_at=9999999999)
    state = {"error": None, "required": True, "mode": "password", "password": "secret"}
    monkeypatch.setattr(auth, "PBGDIR", tmp_path)
    monkeypatch.setattr(auth, "_log", Mock())
    monkeypatch.setattr(auth, "_websocket_sessions", {})
    monkeypatch.setattr(auth, "_websocket_watchdogs", {})
    monkeypatch.setattr(auth, "validate_token", Mock(side_effect=lambda token: session if token == session.token else None))
    monkeypatch.setattr(auth, "_password_state", Mock(return_value=state))
    monkeypatch.setattr(auth, "_bootstrap_payload", lambda current: {"auth": {"authenticated": current is not None}})
    monkeypatch.setattr(auth, "_login_retry_after", Mock(return_value=0))
    monkeypatch.setattr(auth, "_record_login_failure", Mock(return_value=0))
    monkeypatch.setattr(auth, "_reset_login_attempts", Mock())
    monkeypatch.setattr(auth, "generate_token", Mock(return_value=session))
    monkeypatch.setattr(auth, "revoke_token", Mock(return_value=True))
    monkeypatch.setattr(auth, "_get_or_create_passwordless_session", Mock(return_value=session))
    monkeypatch.setattr(auth, "_write_auth_secrets_toml", Mock())
    monkeypatch.setattr(auth, "_load_auth_secrets", Mock(return_value=({"password": "secret"}, None)))
    monkeypatch.setattr(auth, "_revoke_all_sessions", AsyncMock(return_value=1))
    monkeypatch.setattr(auth, "save_ini_section", Mock())
    monkeypatch.setattr(auth, "pb7_runtime_status", Mock(return_value={"pbname": "test"}))
    monkeypatch.setattr(auth, "pb8_runtime_status", Mock(return_value={}))
    effects = Mock()
    app = FastAPI()
    app.add_middleware(
        CORSMiddleware, allow_origins=["https://evil.example.test"],
        allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
    )
    app.add_middleware(auth.BrowserOriginMiddleware)
    app.include_router(auth.router, prefix="/api/auth")

    def side_effect():
        """Represent a dependency that must not run for rejected browser requests."""
        effects()

    @app.api_route("/mutation", methods=["POST", "PUT", "PATCH", "DELETE"])
    def mutation(_side_effect=Depends(side_effect), session=Depends(auth.require_auth)):
        """Exercise the global guard before dependency ordering can cause side effects."""
        return {"user": session.user_id}

    @app.post("/conflict")
    def conflict(session=Depends(auth.require_auth)):
        """Retain a representative restart-blocking response after valid authentication."""
        raise HTTPException(status_code=409, detail="Job blocks restart")

    @app.websocket("/socket")
    async def socket(websocket: WebSocket):
        """Send privileged data only after the real shared authenticator succeeds."""
        if await auth.authenticate_websocket(websocket) is None:
            return
        try:
            await websocket.send_json({"private": True})
            await websocket.receive_text()
        finally:
            await auth.close_websocket_sessions(session.token)

    return SimpleNamespace(app=app, session=session, state=state, effects=effects)


def _client(env, *, cookie=True, base_url=ORIGIN, app=None):
    """Create a cookie browser without running the production application lifespan."""
    client = TestClient(app or env.app, base_url=base_url)
    if cookie:
        client.cookies.set(auth.SESSION_COOKIE_NAME, env.session.token)
    return client


@pytest.mark.parametrize("origin", BAD_ORIGINS)
@pytest.mark.parametrize("method", ["POST", "PUT", "PATCH", "DELETE"])
def test_unsafe_origins_fail_before_dependencies(security_app, origin, method):
    """Same-site attackers and malformed origins cannot reach any unsafe HTTP method."""
    with _client(security_app) as client:
        response = client.request(method, "/mutation", headers={"Origin": origin})
    assert response.status_code == 403
    assert response.json() == {"detail": "Same-origin browser request required"}
    assert response.headers["cache-control"] == "no-store"
    assert "access-control-allow-origin" not in response.headers
    security_app.effects.assert_not_called()
    auth.validate_token.assert_not_called()
    assert auth._log.call_args.args == (auth.SERVICE, "Rejected browser request: same-origin validation failed")


@pytest.mark.parametrize("headers", [
    {}, {"Sec-Fetch-Site": "same-site"}, {"Sec-Fetch-Site": "cross-site"},
    {"Sec-Fetch-Site": "none"}, {"Referer": "https://evil.example.test/page"},
    {"Origin": "null", "Sec-Fetch-Site": "same-origin", "Referer": ORIGIN + "/page"},
    {"Origin": ORIGIN, "Sec-Fetch-Site": "same-site"},
    {"Origin": ORIGIN, "Referer": "https://evil.example.test/page"},
    {"Sec-Fetch-Site": "same-origin", "Referer": "https://evil.example.test/page"},
    {"Sec-Fetch-Site": "same-origin", "Referer": "https://user@gui.example.test/page"},
    [("Origin", ORIGIN), ("Origin", ORIGIN)],
    [("Origin", ORIGIN), ("Origin", "https://evil.example.test")],
    [("Origin", ORIGIN), ("Host", "gui.example.test"), ("Host", "evil.example.test")],
    [("Sec-Fetch-Site", "same-origin"), ("Sec-Fetch-Site", "same-origin")],
    [("Referer", ORIGIN), ("Referer", ORIGIN)],
])
def test_missing_duplicate_or_conflicting_evidence_fails(security_app, headers):
    """Fallbacks must never override bad Origin, duplicate headers, or contradictory evidence."""
    with _client(security_app) as client:
        assert client.post("/mutation", headers=headers).status_code == 403
    security_app.effects.assert_not_called()


@pytest.mark.parametrize("headers", [
    {"Origin": ORIGIN}, {"Origin": "https://GUI.EXAMPLE.TEST:443"},
    {"Sec-Fetch-Site": "same-origin"}, {"Referer": ORIGIN + "/app/page?view=1"},
    {"Referer": ORIGIN + "/page", "Sec-Fetch-Site": "same-origin"},
    {"Origin": ORIGIN, "Referer": ORIGIN + "/page", "Sec-Fetch-Site": "same-origin"},
])
def test_same_origin_and_safe_missing_origin_fallbacks_succeed(security_app, headers):
    """Same-origin metadata or an exact-origin Referer supports browsers omitting Origin."""
    with _client(security_app) as client:
        assert client.post("/mutation", headers=headers).json() == {"user": "test"}
    security_app.effects.assert_called_once()


@pytest.mark.parametrize("base_url,origin", [
    ("http://gui.example.test", "http://gui.example.test:80"),
    ("https://gui.example.test:8443", "https://gui.example.test:8443"),
    ("http://192.0.2.8:9000", "http://192.0.2.8:9000"),
])
def test_arbitrary_hosts_ports_and_ipv6_work(security_app, base_url, origin):
    """Origin equality uses parsed hosts and default ports rather than localhost allowlists."""
    with _client(security_app, base_url=base_url) as client:
        assert client.post("/mutation", headers={"Origin": origin}).status_code == 200


def test_ipv6_origin_uses_preserved_host_not_internal_server(security_app):
    """Supply the real IPv6 authority through Host, bypassing TestClient's broken IPv6 transport parser."""
    with _client(security_app) as client:
        assert client.post("/mutation", headers={
            "Host": "[2001:db8::1]:8443", "Origin": "https://[2001:0db8:0:0:0:0:0:1]:8443",
        }).status_code == 200


@pytest.mark.parametrize("authorization", ["", "Basic secret", "Bearer", "Bearer ", "Bearer invalid", "bearer invalid", "Bearer bad extra"])
def test_invalid_authorization_never_falls_back_to_cookie(security_app, authorization):
    """Explicit but invalid credentials cannot use cookies or skip protection through a fake Bearer."""
    with _client(security_app) as client:
        headers = {"Authorization": authorization, "Origin": ORIGIN}
        response = client.post("/mutation", headers=headers)
        assert response.status_code == 401
        assert client.get("/api/auth/bootstrap", headers=headers).json()["auth"]["authenticated"] is False
        assert client.post("/api/auth/login", json={"password": "secret"}, headers=headers).status_code == 401
    security_app.effects.assert_not_called()
    auth.generate_token.assert_not_called()


def test_scoped_cookie_also_requires_origin_and_authenticates_websockets(security_app):
    """The current host-scoped cookie is protected just like the shipped legacy cookie."""
    with _client(security_app, cookie=False) as client:
        client.cookies.set(auth._session_cookie_name("gui.example.test"), "test-session")
        assert client.post("/mutation").status_code == 403
        security_app.effects.assert_not_called()
        assert client.post("/mutation", headers={"Origin": ORIGIN}).status_code == 200
        with client.websocket_connect("wss://gui.example.test/socket", headers={"Origin": ORIGIN}) as websocket:
            assert websocket.receive_json() == {"private": True}
            websocket.send_text("close")
    assert auth._websocket_watchdogs == {}


def test_duplicate_authorization_cannot_select_cookie_or_first_bearer(security_app):
    """Multiple credentials are ambiguous even when the first one is valid."""
    headers = [("Authorization", "Bearer test-session"), ("Authorization", "Bearer bad")]
    with _client(security_app) as client:
        assert client.post("/mutation", headers=headers).status_code == 401
        assert client.get("/api/auth/bootstrap", headers=headers).json()["auth"]["authenticated"] is False
    security_app.effects.assert_not_called()


@pytest.mark.parametrize("cookie", [False, True])
@pytest.mark.parametrize("origin", [None, "https://external-client.test"])
def test_explicit_valid_bearer_clients_do_not_require_browser_headers(security_app, cookie, origin):
    """Nonambient valid Bearer authority, including case-insensitive schemes, stays supported."""
    headers = {"Authorization": "bEaReR test-session"}
    if origin:
        headers["Origin"] = origin
    with _client(security_app, cookie=cookie) as client:
        assert client.post("/mutation", headers=headers).status_code == 200
        assert client.post("/conflict", headers=headers).status_code == 409
        assert client.get("/api/auth/bootstrap", headers=headers).json()["auth"]["authenticated"] is True


@pytest.mark.parametrize("path,payload,cookie", [
    ("/api/auth/login", {"password": "secret"}, False),
    ("/api/auth/login/", {"password": "secret"}, False),
    ("/api/auth/logout", {}, True),
    ("/api/auth/change-password", {"current_password": "secret", "new_password": "replacement"}, True),
    ("/api/auth/setup", {"pbname": "attacker"}, True),
])
@pytest.mark.parametrize("headers", [{}, {"Origin": "https://evil.example.test", "Sec-Fetch-Site": "same-site"}])
def test_auth_mutations_are_guarded_before_side_effects(security_app, path, payload, cookie, headers):
    """Login CSRF and same-site logout/setup/password changes fail before credential or config access."""
    with _client(security_app, cookie=cookie) as client:
        response = client.post(path, json=payload, headers=headers)
    assert response.status_code == 403
    assert "set-cookie" not in response.headers
    for spy in (auth.generate_token, auth.revoke_token, auth._password_state, auth._write_auth_secrets_toml, auth.save_ini_section):
        spy.assert_not_called()


def test_valid_auth_requests_keep_cookies_and_expected_errors(security_app):
    """Valid browser traffic preserves login, bad-password, logout, and restart-conflict behavior."""
    with _client(security_app, cookie=False) as client:
        headers = {"Origin": ORIGIN}
        response = client.post("/api/auth/login", json={"password": "secret"}, headers=headers)
        assert response.status_code == 200
        assert "HttpOnly" in response.headers["set-cookie"]
        assert "Secure" in response.headers["set-cookie"]
        assert "test-session" not in response.text
        assert client.post("/api/auth/login", json={"password": "wrong"}, headers=headers).status_code == 401
        assert client.post("/api/auth/change-password", json={"current_password": "wrong", "new_password": "new"}, headers=headers).status_code == 400
        assert client.post("/conflict", headers=headers).json() == {"detail": "Job blocks restart"}
        assert client.post("/api/auth/logout", headers=headers).json() == {"ok": True}
    auth.revoke_token.assert_called_once_with("test-session")


@pytest.mark.parametrize("headers", [
    {}, {"Sec-Fetch-Site": "same-site", "Sec-Fetch-Mode": "navigate", "Sec-Fetch-Dest": "document"},
    {"Origin": "https://evil.example.test"},
    {"Sec-Fetch-Site": "none", "Sec-Fetch-Mode": "navigate", "Sec-Fetch-Dest": "document"},
    {"Sec-Fetch-Site": "same-origin"}, {"Referer": ORIGIN + "/"},
])
def test_passwordless_welcome_requires_safe_navigation(security_app, headers):
    """Every cookieless navigation now serves only a non-issuing shell, safe even without origin evidence."""
    security_app.state.update(required=False, mode="disabled")
    with _client(security_app, cookie=False) as client:
        response = client.get("/api/auth/main_page", headers=headers)
    assert response.status_code == 200
    assert "set-cookie" not in response.headers
    assert response.headers["content-security-policy"] == "frame-ancestors 'self'"
    assert response.headers["x-frame-options"] == "SAMEORIGIN"
    auth._get_or_create_passwordless_session.assert_not_called()


@pytest.mark.parametrize("origin", BAD_ORIGINS + [None])
@pytest.mark.parametrize("middleware", [False, True])
def test_passwordless_post_explicit_origin_is_enforced_without_middleware(security_app, origin, middleware):
    """The route independently blocks bad origins before optional auth, config reads, or session issuance."""
    app = security_app.app if middleware else FastAPI()
    if not middleware:
        app.include_router(auth.router, prefix="/api/auth")
    headers = {"Origin": origin} if origin is not None else {}
    with _client(security_app, cookie=False, app=app) as client:
        response = client.post("/api/auth/passwordless-session", headers=headers)
    assert response.status_code == 403
    assert "set-cookie" not in response.headers
    auth._password_state.assert_not_called()
    auth.validate_token.assert_not_called()
    auth._get_or_create_passwordless_session.assert_not_called()


@pytest.mark.parametrize("headers", [
    {"Sec-Fetch-Site": "same-origin"}, {"Referer": ORIGIN + "/page"},
    {"Sec-Fetch-Site": "same-origin", "Authorization": "Bearer test-session"},
    [("Origin", ORIGIN), ("Origin", ORIGIN)],
])
def test_passwordless_post_never_uses_http_origin_fallbacks(security_app, headers):
    """Even an authenticated browser POST must explicitly provide one valid Origin for issuance."""
    security_app.state.update(required=False, mode="disabled")
    with _client(security_app) as client:
        assert client.post("/api/auth/passwordless-session", headers=headers).status_code == 403
    auth._get_or_create_passwordless_session.assert_not_called()


@pytest.mark.parametrize("cookie", [False, True])
@pytest.mark.parametrize("mode,required,error,status", [
    ("password", True, None, 401), ("error", False, "Invalid state", 500),
    ("password", False, None, 401), ("disabled", True, None, 401),
])
def test_passwordless_post_rechecks_authentication_mode(security_app, cookie, mode, required, error, status):
    """A stale public bootstrap cannot issue a session once passwords are required or auth is broken."""
    security_app.state.update(mode=mode, required=required, error=error)
    with _client(security_app, cookie=cookie) as client:
        response = client.post("/api/auth/passwordless-session", headers={"Origin": ORIGIN})
    assert response.status_code == status
    assert "set-cookie" not in response.headers
    auth._get_or_create_passwordless_session.assert_not_called()


@pytest.mark.parametrize("authorization", ["", "Basic invalid", "Bearer", "Bearer invalid"])
def test_passwordless_post_invalid_bearer_cannot_reuse_cookie_without_middleware(security_app, authorization):
    """Standalone route registration must retain the no-cookie-fallback boundary."""
    security_app.state.update(required=False, mode="disabled")
    app = FastAPI()
    app.include_router(auth.router, prefix="/api/auth")
    with _client(security_app, app=app) as client:
        response = client.post("/api/auth/passwordless-session", headers={
            "Origin": ORIGIN, "Authorization": authorization,
        })
    assert response.status_code == 401
    assert "set-cookie" not in response.headers
    auth._get_or_create_passwordless_session.assert_not_called()


def test_cors_preflight_and_unauthenticated_errors_are_preserved(security_app):
    """Preflight remains side-effect free, and missing nonbrowser credentials still return 401."""
    with _client(security_app, cookie=False) as client:
        assert client.post("/conflict").status_code == 401
        assert client.get("/api/auth/bootstrap").status_code == 200
        response = client.options("/mutation", headers={
            "Origin": "https://evil.example.test", "Access-Control-Request-Method": "POST",
        })
        assert response.status_code == 200
    security_app.effects.assert_not_called()


@pytest.mark.parametrize("trusted", [False, True])
def test_proxy_origin_uses_effective_url_not_raw_forwarded_headers(security_app, trusted):
    """Only ASGI proxy processing may supply the external TLS scheme; Host must be preserved."""
    app = ProxyHeadersMiddleware(security_app.app, trusted_hosts=["testclient"] if trusted else [])
    with _client(security_app, app=app, base_url="http://gui.example.test") as client:
        response = client.post("/mutation", headers={
            "Origin": ORIGIN, "X-Forwarded-Proto": "https",
            "X-Forwarded-Host": "evil.example.test", "Forwarded": "proto=https;host=evil.example.test",
        })
    assert response.status_code == (200 if trusted else 403)


def test_forwarded_host_cannot_override_request_host(security_app):
    """Untrusted forwarding headers cannot authorize an attacker's different authority."""
    with _client(security_app) as client:
        assert client.post("/mutation", headers={
            "Origin": "https://evil.example.test", "X-Forwarded-Host": "evil.example.test",
            "Forwarded": "proto=https;host=evil.example.test",
        }).status_code == 403


@pytest.mark.parametrize("host", ["[broken", "gui.example.test:", "user@gui.example.test", "gui.example.test..", "gui.example.test:65536"])
def test_malformed_target_authority_fails_closed(security_app, host):
    """Malformed Host must produce a secret-free 403, not a URL parser 500 or an origin match."""
    with _client(security_app) as client:
        assert client.post("/mutation", headers={"Origin": ORIGIN, "Host": host}).status_code == 403
    security_app.effects.assert_not_called()


def test_mount_prefix_does_not_bypass_cookieless_login_guard(security_app):
    """ASGI root_path and trailing-slash redirects must not hide a session-issuing route."""
    parent = FastAPI()
    parent.mount("/pbgui", security_app.app)
    with _client(security_app, cookie=False, app=parent) as client:
        response = client.post("/pbgui/api/auth/login/", json={"password": "secret"})
        assert response.status_code == 403
    auth.generate_token.assert_not_called()


def test_authenticated_welcome_navigation_needs_no_csrf_headers(security_app):
    """Ordinary safe navigation with an existing session still works on plaintext HTTP browsers."""
    with _client(security_app, base_url="http://gui.example.test") as client:
        assert client.get("/api/auth/main_page").status_code == 200
    auth._get_or_create_passwordless_session.assert_not_called()


def test_login_then_password_rotation_accepts_valid_bearer_without_origin(security_app):
    """Cookie-issuing endpoints stay usable by explicitly authenticated external API clients."""
    with _client(security_app, cookie=False) as client:
        headers = {"Authorization": "Bearer test-session"}
        assert client.post("/api/auth/login", headers=headers, json={"password": "secret"}).status_code == 200
        response = client.post("/api/auth/change-password", headers=headers, json={
            "current_password": "secret", "new_password": "replacement",
        })
        assert response.status_code == 200
        assert "HttpOnly" in response.headers["set-cookie"]
    auth._write_auth_secrets_toml.assert_called_once_with({"auth_mode": "password", "password": "replacement"})
    auth._revoke_all_sessions.assert_awaited_once()


@pytest.mark.parametrize("origin", BAD_ORIGINS + [None])
def test_websocket_rejects_bad_origin_before_accept_or_token_access(security_app, origin):
    """The shared authenticator denies data, watchdogs, and session tracking to attacker sockets."""
    websocket = Mock()
    websocket.scope = {"type": "websocket"}
    websocket.url = URL("wss://gui.example.test/socket")
    websocket.headers = Headers({"host": "gui.example.test", **({"origin": origin} if origin is not None else {})})
    websocket.cookies = {auth.SESSION_COOKIE_NAME: "test-session"}
    websocket.accept = AsyncMock()
    websocket.close = AsyncMock()
    assert asyncio.run(auth.authenticate_websocket(websocket)) is None
    websocket.close.assert_awaited_once_with(code=1008, reason="Same-origin browser request required")
    websocket.accept.assert_not_awaited()
    auth.validate_token.assert_not_called()
    assert auth._websocket_sessions == {}
    assert auth._websocket_watchdogs == {}


@pytest.mark.parametrize("headers,code", [
    ({"Origin": "https://evil.example.test"}, 1008),
    ({"Sec-Fetch-Site": "same-origin", "Referer": ORIGIN + "/page"}, 1008),
    ([("Origin", ORIGIN), ("Origin", ORIGIN)], 1008),
    ({"Origin": ORIGIN, "Sec-Fetch-Site": "same-site"}, 1008),
])
def test_real_websocket_handshake_denies_cross_origin_cookie(security_app, headers, code):
    """A real ASGI handshake cannot accept or receive the private message when origin is invalid."""
    with _client(security_app) as client:
        with pytest.raises(WebSocketDisconnect) as error:
            with client.websocket_connect("wss://gui.example.test/socket", headers=HTTPHeaders(headers)):
                pytest.fail("Untrusted socket was accepted")
    assert error.value.code == code
    assert auth._websocket_sessions == {}
    assert auth._websocket_watchdogs == {}


def test_real_websocket_same_origin_lifecycle_and_bad_session(security_app):
    """Valid sockets receive data, register watchdogs, and still enforce missing-session code 4001."""
    with _client(security_app) as client:
        with client.websocket_connect("wss://gui.example.test/socket", headers={"Origin": ORIGIN + ":443"}) as websocket:
            assert websocket.receive_json() == {"private": True}
            assert auth._websocket_sessions
            assert auth._websocket_watchdogs
            websocket.send_text("close")
            with pytest.raises(WebSocketDisconnect) as error:
                websocket.receive_json()
            assert error.value.code == 4001
        client.cookies.clear()
        with pytest.raises(WebSocketDisconnect) as error:
            with client.websocket_connect("wss://gui.example.test/socket", headers={"Origin": ORIGIN}):
                pytest.fail("Unauthenticated socket was accepted")
        assert error.value.code == 4001
    assert auth._websocket_sessions == {}
    assert auth._websocket_watchdogs == {}


def test_cross_origin_logout_cannot_close_an_active_browser_socket(security_app):
    """A same-site POST must neither revoke the cookie nor disconnect its active privileged socket."""
    with _client(security_app) as client:
        with client.websocket_connect("wss://gui.example.test/socket", headers={"Origin": ORIGIN}) as websocket:
            assert websocket.receive_json() == {"private": True}
            assert client.post("/api/auth/logout", headers={
                "Origin": "https://evil.example.test", "Sec-Fetch-Site": "same-site",
            }).status_code == 403
            auth.revoke_token.assert_not_called()
            assert auth._websocket_sessions
            assert auth._websocket_watchdogs
            websocket.send_text("close")
    assert auth._websocket_sessions == {}
    assert auth._websocket_watchdogs == {}


def test_server_installs_guard_outside_cors_and_all_browser_sockets_use_auth():
    """Check real server wiring without importing modules that inspect production INI/runtime state."""
    root = Path(__file__).resolve().parents[1]
    source = (root / "PBApiServer.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    registrations = [
        node.value.args[0].id for node in tree.body
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call)
        and isinstance(node.value.func, ast.Attribute) and node.value.func.attr == "add_middleware"
    ]
    assert registrations.index("CORSMiddleware") < registrations.index("BrowserOriginMiddleware")
    guard = next(node for node in tree.body if isinstance(node, ast.Expr)
                 and isinstance(node.value, ast.Call) and node.value.args
                 and isinstance(node.value.args[0], ast.Name) and node.value.args[0].id == "BrowserOriginMiddleware")
    logging_middleware = next(node for node in tree.body if isinstance(node, ast.AsyncFunctionDef)
                              and node.name == "redirect_unauthenticated_page")
    assert guard.lineno > logging_middleware.end_lineno
    sockets = []
    for path in [root / "PBApiServer.py", *(root / "api").glob("*.py")]:
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.AsyncFunctionDef) and any(
                isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Attribute)
                and decorator.func.attr == "websocket" for decorator in node.decorator_list
            ):
                sockets.append(node.name)
                assert "authenticate_websocket(websocket)" in ast.unparse(node), (path.name, node.name)
    assert len(sockets) >= 13


@pytest.mark.parametrize("page", ["welcome.html", "dashboard_main.html", "dashboard_editor.html"])
def test_real_static_html_get_head_and_cached_response_are_frame_protected(security_app, page):
    """The actual frontend files receive framing headers through the ASGI static mount, not a page handler."""
    frontend = Path(__file__).resolve().parents[1] / "frontend"
    security_app.app.mount("/app", StaticFiles(directory=frontend, html=True))
    with _client(security_app, cookie=False) as client:
        response = client.get(f"/app/{page}")
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/html")
        assert len(response.content) == int(response.headers["content-length"])
        for result in (
            response, client.head(f"/app/{page}"),
            client.get(f"/app/{page}", headers={"If-None-Match": response.headers["etag"]}),
        ):
            assert result.headers.get_list("content-security-policy") == ["frame-ancestors 'self'"]
            assert result.headers["x-frame-options"] == "SAMEORIGIN"
        assert result.status_code == 304
        assert result.content == b""


@pytest.mark.parametrize("existing", [
    [], [("content-security-policy", "frame-ancestors 'self'")],
    [("content-security-policy", "default-src 'none'; script-src 'nonce-test'; frame-ancestors 'none'"),
     ("x-frame-options", "DENY")],
    [("content-security-policy", "default-src 'self'"),
     ("content-security-policy", "frame-ancestors *; object-src 'none'")],
    [("content-security-policy-report-only", "frame-ancestors 'none'; script-src 'none'")],
    [("content-security-policy", "frame-ancestors https://other.example.test"),
     ("x-frame-options", "ALLOW-FROM https://other.example.test")],
    [("x-frame-options", "SAMEORIGIN"), ("x-frame-options", "deny")],
])
@pytest.mark.parametrize("media_type", ["text/html", "application/xhtml+xml"])
def test_html_framing_policy_intersects_existing_csp_without_replacing_it(existing, media_type):
    """Separate enforced policies retain every original directive and any stronger framing denial."""
    app = FastAPI()
    app.add_middleware(auth.BrowserOriginMiddleware)

    @app.get("/page")
    def page():
        """Return HTML with arbitrary pre-existing security headers."""
        response = Response("<html>unchanged</html>", media_type=media_type)
        response.raw_headers.extend((key.encode(), value.encode()) for key, value in existing)
        return response

    with TestClient(app) as client:
        response = client.get("/page")
    original = [value for key, value in existing if key == "content-security-policy"]
    expected = original if "frame-ancestors 'self'" in original else [*original, "frame-ancestors 'self'"]
    assert response.headers.get_list("content-security-policy") == expected
    assert response.headers.get_list("content-security-policy-report-only") == [
        value for key, value in existing if key == "content-security-policy-report-only"
    ]
    deny = any(key == "x-frame-options" and value.upper() == "DENY" for key, value in existing)
    assert response.headers["x-frame-options"] == ("DENY" if deny else "SAMEORIGIN")
    assert response.text == "<html>unchanged</html>"


@pytest.mark.parametrize("status", [200, 403, 404, 500])
def test_streamed_html_is_protected_without_buffering_or_changing_messages(status):
    """The send wrapper changes only response headers, not streaming chunks or response status."""
    messages = []

    async def application(scope, receive, send):
        """Emit two HTML body chunks with an explicit start message."""
        await send({"type": "http.response.start", "status": status, "headers": [(b"content-type", b"text/html; charset=utf-8")]})
        await send({"type": "http.response.body", "body": b"<html>", "more_body": True})
        assert len(messages) == 2
        await send({"type": "http.response.body", "body": b"</html>", "more_body": False})

    asyncio.run(auth.BrowserOriginMiddleware(application)(
        {"type": "http", "method": "GET", "path": "/page", "headers": []}, AsyncMock(), AsyncMock(side_effect=messages.append),
    ))
    assert messages[0]["status"] == status
    assert dict(messages[0]["headers"])[b"content-security-policy"] == b"frame-ancestors 'self'"
    assert messages[1:] == [
        {"type": "http.response.body", "body": b"<html>", "more_body": True},
        {"type": "http.response.body", "body": b"</html>", "more_body": False},
    ]


@pytest.mark.parametrize("origin", [
    "http://[2001:db8::1]:8000", "http://[::1]", "https://[2001:db8::1]:8443", "http://gui_lan:8000",
])
def test_literal_ipv6_and_lan_urls_render_usable_login_and_welcome_origins(security_app, origin):
    """Real ASGI HTTP URLs keep brackets/ports in generated JS and can complete passwordless bootstrap."""
    @security_app.app.get("/")
    def root(request: Request):
        """Exercise the actual Login renderer without server startup."""
        return auth.build_root_entry_response(request)

    async def scenario():
        """Use HTTPX ASGITransport, avoiding TestClient's IPv6 netloc parsing bug."""
        async with AsyncClient(transport=ASGITransport(app=security_app.app), base_url=origin) as client:
            login = await client.get("/")
            assert login.status_code == 200
            assert f'var API_ORIGIN = "{origin}";' in login.text
            assert login.headers["content-security-policy"] == "frame-ancestors 'self'"
            security_app.state.update(required=False, mode="disabled")
            welcome = await client.get("/api/auth/main_page")
            assert welcome.status_code == 200
            assert f'var API_ORIGIN = "{origin}";' in welcome.text
            assert "set-cookie" not in welcome.headers
            issued = await client.post("/api/auth/passwordless-session", headers={"Origin": origin}, json={})
            assert issued.status_code == 200
            assert "HttpOnly" in issued.headers["set-cookie"]
            assert issued.json()["auth"]["authenticated"] is True

    asyncio.run(scenario())


def test_underscore_lan_host_retains_exact_origin_checks_for_http_and_websocket(security_app):
    """Accept browser-valid LAN underscores without allowing different schemes, ports, or authorities."""
    origin = "http://gui_lan:8000"
    with _client(security_app, base_url=origin) as client:
        assert client.post("/mutation", headers={"Origin": origin}).status_code == 200
        for invalid in ("null", "http://gui_lan:8001", "https://gui_lan:8000", "http://gui_lan.evil:8000",
                        "http://user@gui_lan:8000", "http://gui_lan:bad", origin + "," + origin):
            assert client.post("/mutation", headers={"Origin": invalid}).status_code == 403
        assert client.post("/mutation", headers=[("Origin", origin), ("Origin", origin)]).status_code == 403
        with client.websocket_connect("ws://gui_lan:8000/socket", headers={"Origin": origin}) as websocket:
            assert websocket.receive_json() == {"private": True}
            websocket.send_text("close")
    assert auth._websocket_watchdogs == {}


@pytest.mark.parametrize("host", ["user@gui_lan:8000", "[invalid", "gui_lan:bad", "gui_lan:65536"])
def test_request_origin_rejects_invalid_authority_without_echoing_it(host, monkeypatch):
    """Invalid page authority must fail closed rather than enter generated JavaScript."""
    monkeypatch.setattr(auth, "_log", Mock())
    request = Request({"type": "http", "scheme": "http", "path": "/", "headers": [(b"host", host.encode())]})
    with pytest.raises(HTTPException) as error:
        auth._request_origin(request)
    assert error.value.status_code == 400
    assert error.value.detail == "Invalid request origin"
    assert auth._log.call_args.args == (auth.SERVICE, "Rejected page request: invalid origin authority")
