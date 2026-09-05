"""Offline regressions for auth token boundaries and issues #153 through #156."""

import json
from unittest.mock import Mock
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.requests import Request

from api import auth


@pytest.fixture
def isolated_auth(monkeypatch, tmp_path):
    """Keep token files, auth state, logs, and runtime lookups in isolated test data."""
    monkeypatch.setattr(auth, "PBGDIR", tmp_path)
    monkeypatch.setattr(auth, "_clear_vps_manager_secrets", Mock())
    monkeypatch.setattr(auth, "_log", Mock())
    monkeypatch.setattr(auth, "pb7_runtime_status", lambda: {})
    monkeypatch.setattr(auth, "pb8_runtime_status", lambda: {})
    monkeypatch.setattr(auth, "_login_security_status", lambda: {})
    state = {
        "error": None, "required": False, "mode": "disabled", "missing": True,
        "password": "", "security_warnings": [],
    }
    monkeypatch.setattr(auth, "_password_state", lambda: state)
    return state


@pytest.fixture
def auth_app(isolated_auth):
    """Serve only the auth router without production application startup."""
    app = FastAPI()
    app.include_router(auth.router, prefix="/api/auth")
    return app


def _request(method="GET", accept="text/html"):
    """Build an HTTPS request without filesystem or network dependencies."""
    return Request({
        "type": "http", "method": method, "scheme": "https",
        "server": ("testserver", 443), "client": ("127.0.0.1", 1),
        "path": "/", "query_string": b"",
        "headers": [(b"host", b"testserver"), (b"accept", accept.encode())],
    })


@pytest.mark.parametrize("operation", [auth.validate_token, auth.revoke_token, auth.refresh_token])
@pytest.mark.parametrize("raw", [
    None, "", " ", "../outside", "../../outside", "/tmp/outside",
    "..\\outside", "bad\x00token", "bad\ntoken", "not-a-uuid",
    "12345678-1234-1234-9234-123456789abc",
    "12345678-1234-4234-9234-123456789ABC",
    "12345678123442349234123456789abc",
])
def test_invalid_token_never_accesses_store(operation, raw, monkeypatch, isolated_auth):
    """Malformed identifiers must fail before any directory lookup or secret cleanup."""
    store = Mock(side_effect=AssertionError("Invalid token accessed token store"))
    monkeypatch.setattr(auth, "get_tokens_dir", store)
    assert not operation(raw)
    store.assert_not_called()
    auth._clear_vps_manager_secrets.assert_not_called()


@pytest.mark.parametrize("operation", [auth.validate_token, auth.revoke_token, auth.refresh_token])
@pytest.mark.parametrize("expired", [False, True])
def test_traversal_cannot_read_modify_or_delete_outside_json(operation, expired, isolated_auth, tmp_path):
    """An attacker-controlled traversal cannot reach even a valid session-shaped file."""
    tokens_dir = auth.get_tokens_dir()
    victim = tokens_dir.parent / "outside.json"
    contents = json.dumps({
        "token": str(uuid4()), "user_id": "victim", "created_at": 1,
        "expires_at": 1 if expired else 9999999999,
    })
    victim.write_text(contents, encoding="utf-8")
    assert not operation("../outside")
    assert victim.read_text(encoding="utf-8") == contents
    auth._clear_vps_manager_secrets.assert_not_called()


@pytest.mark.parametrize("operation", [auth.validate_token, auth.revoke_token, auth.refresh_token])
def test_token_symlink_cannot_reach_outside_json(operation, isolated_auth, tmp_path):
    """A syntactically valid token filename must not follow a persisted symlink."""
    token = str(uuid4())
    victim = tmp_path / "outside.json"
    contents = json.dumps({"token": token, "user_id": "victim", "created_at": 1, "expires_at": 9999999999})
    victim.write_text(contents, encoding="utf-8")
    alias = auth.get_tokens_dir() / f"{token}.json"
    alias.symlink_to(victim)
    assert not operation(token)
    assert alias.is_symlink()
    assert victim.read_text(encoding="utf-8") == contents
    auth._clear_vps_manager_secrets.assert_not_called()


def test_generated_token_lifecycle_still_works(isolated_auth):
    """Generated and persisted UUID sessions remain usable by existing callers."""
    session = auth.generate_token("test", expires_in_seconds=60)
    assert auth.validate_token(session.token) == session
    assert auth.validate_token(f" {session.token} ") == session
    refreshed = auth.refresh_token(session.token, extends_seconds=120)
    assert refreshed.token == session.token
    assert refreshed.expires_at > session.expires_at
    assert auth.validate_token(session.token) == refreshed
    assert auth.revoke_token(session.token) is True
    assert auth.validate_token(session.token) is None
    assert auth.revoke_token(session.token) is False


@pytest.mark.parametrize("operation", [auth.validate_token, auth.refresh_token])
def test_expired_tokens_are_removed_only_inside_store(operation, isolated_auth):
    """Rejecting traversal must retain ordinary expiry cleanup for real UUID tokens."""
    session = auth.generate_token("test", expires_in_seconds=-1)
    assert operation(session.token) is None
    assert not (auth.get_tokens_dir() / f"{session.token}.json").exists()


@pytest.mark.parametrize("operation", [auth.validate_token, auth.refresh_token])
def test_persisted_token_must_match_filename(operation, isolated_auth):
    """Corrupt persisted data cannot substitute another session's identity."""
    session = auth.generate_token("test")
    alias = auth.get_tokens_dir() / f"{uuid4()}.json"
    alias.write_text(session.model_dump_json(), encoding="utf-8")
    assert operation(alias.stem) is None
    assert auth.validate_token(session.token) == session


@pytest.mark.parametrize("transport", ["bearer", "cookie"])
def test_request_auth_rejects_traversal_without_touching_victim(transport, auth_app):
    """Both public authentication transports enforce the same token-path boundary."""
    victim = auth.get_tokens_dir().parent / "outside.json"
    contents = json.dumps({"token": "../outside", "user_id": "victim", "created_at": 1, "expires_at": 9999999999})
    victim.write_text(contents, encoding="utf-8")
    with TestClient(auth_app, base_url="https://testserver") as client:
        headers = {"Authorization": "Bearer ../outside"} if transport == "bearer" else {}
        if transport == "cookie":
            client.cookies.set(auth._session_cookie_name("testserver"), "../outside")
        assert client.get("/api/auth/bootstrap", headers=headers).json()["auth"]["authenticated"] is False
        assert client.post("/api/auth/logout", headers=headers).status_code == 401
    assert victim.read_text(encoding="utf-8") == contents


def test_passwordless_browsers_behind_same_proxy_are_isolated(auth_app, monkeypatch):
    """Two cookie jars on the same host get independent sessions and logout state."""
    with TestClient(auth_app, base_url="https://testserver") as first, TestClient(
        auth_app, base_url="https://testserver"
    ) as second:
        assert first.get("/api/auth/main_page").status_code == 200
        assert not first.cookies
        response = first.post("/api/auth/passwordless-session", headers={"Origin": "https://testserver"})
        assert response.status_code == 200
        assert "HttpOnly" in response.headers["set-cookie"]
        first_token = first.cookies.get(auth._session_cookie_name("testserver"))
        assert second.post("/api/auth/passwordless-session", headers={"Origin": "https://testserver"}).status_code == 200
        second_token = second.cookies.get(auth._session_cookie_name("testserver"))
        assert first_token != second_token
        issue_session = Mock(side_effect=AssertionError("Cookie reuse must not scan or issue sessions"))
        monkeypatch.setattr(auth, "_get_or_create_passwordless_session", issue_session)
        assert first.post("/api/auth/passwordless-session", headers={"Origin": "https://testserver"}).status_code == 200
        assert first.get("/api/auth/main_page").status_code == 200
        assert first.cookies.get(auth._session_cookie_name("testserver")) == first_token
        assert len(list(auth.get_tokens_dir().glob("*.json"))) == 2
        assert first.post("/api/auth/logout").status_code == 200
        assert auth.validate_token(first_token) is None
        assert auth.validate_token(second_token) is not None
        assert second.get("/api/auth/bootstrap").json()["auth"]["authenticated"] is True
        issue_session.assert_not_called()


@pytest.mark.parametrize("middleware", [False, True])
def test_plain_http_lan_passwordless_bootstrap_issues_only_on_post(auth_app, middleware):
    """An HTTP LAN browser can GET without metadata, then POST its Origin and reuse its own cookie."""
    if middleware:
        auth_app.add_middleware(auth.BrowserOriginMiddleware)
    origin = "http://192.0.2.8:8000"
    with TestClient(auth_app, base_url=origin) as client:
        response = client.get("/api/auth/main_page")
        assert response.status_code == 200
        assert "set-cookie" not in response.headers
        bootstrap = client.get("/api/auth/bootstrap")
        assert bootstrap.json()["auth"]["password_required"] is False
        assert bootstrap.json()["auth"]["authenticated"] is False
        assert "set-cookie" not in bootstrap.headers
        assert not list(auth.get_tokens_dir().glob("*.json"))
        issued = client.post("/api/auth/passwordless-session", headers={"Origin": origin}, json={})
        assert issued.status_code == 200
        assert issued.headers["cache-control"] == "no-store"
        assert "HttpOnly" in issued.headers["set-cookie"]
        assert "SameSite=strict" in issued.headers["set-cookie"]
        assert "Secure" not in issued.headers["set-cookie"]
        assert issued.json()["auth"]["authenticated"] is True
        token = client.cookies.get(auth._session_cookie_name("192.0.2.8:8000"))
        session = auth.validate_token(token)
        assert session is not None
        assert token not in issued.text
        assert "token" not in issued.json()["auth"]
        assert client.get("/api/auth/bootstrap").json()["auth"]["authenticated"] is True
        repeated = client.post("/api/auth/passwordless-session", headers={"Origin": origin})
        assert repeated.status_code == 200
        assert client.cookies.get(auth._session_cookie_name("192.0.2.8:8000")) == token
        assert auth.validate_token(token) == session
        assert len(list(auth.get_tokens_dir().glob("*.json"))) == 1


def test_passwordless_issuance_retains_bounded_storage(monkeypatch, isolated_auth):
    """Independent cookieless visitors must not remove the existing session-store cap."""
    monkeypatch.setattr(auth, "_PASSWORDLESS_SESSION_LIMIT", 1)
    first = auth._get_or_create_passwordless_session("127.0.0.1")
    second = auth._get_or_create_passwordless_session("127.0.0.1")
    assert first.token != second.token
    assert auth.validate_token(first.token) is None
    assert auth.validate_token(second.token) is not None
    assert len(list(auth.get_tokens_dir().glob("*.json"))) == 1


@pytest.mark.parametrize("passwordless", [False, True])
def test_welcome_html_and_bootstrap_never_expose_session_token(passwordless, isolated_auth, auth_app):
    """Existing and freshly issued browser sessions are delivered only as HttpOnly cookies."""
    isolated_auth["required"] = not passwordless
    with TestClient(auth_app, base_url="https://testserver") as client:
        if not passwordless:
            session = auth.generate_token("test")
            client.cookies.set(auth._session_cookie_name("testserver"), session.token, domain="testserver.local")
        else:
            issued = client.post("/api/auth/passwordless-session", headers={"Origin": "https://testserver"})
            assert issued.status_code == 200
            assert "token" not in issued.json()["auth"]
        response = client.get("/api/auth/main_page", headers={"Sec-Fetch-Site": "same-origin"})
        token = client.cookies.get(auth._session_cookie_name("testserver"))
        assert token
        assert response.status_code == 200
        assert token not in response.text
        assert "%%TOKEN%%" not in response.text
        assert response.headers["cache-control"] == "no-store"
        assert response.headers["referrer-policy"] == "no-referrer"
        bootstrap = client.get("/api/auth/bootstrap")
        assert token not in bootstrap.text
        assert "token" not in bootstrap.json()["auth"]
        assert bootstrap.json()["auth"]["authenticated"] is True


@pytest.mark.parametrize("required,error,authenticated", [
    (False, None, False), (True, None, True), (True, "Invalid auth config", False),
])
def test_root_redirects_are_private_303_responses(required, error, authenticated, isolated_auth):
    """Every root redirect branch uses explicit auth-boundary headers and semantics."""
    isolated_auth.update(required=required, error=error)
    session = auth.generate_token("test") if authenticated else None
    response = auth.build_root_entry_response(_request(), session)
    assert response.status_code == 303
    assert response.headers["location"] == "/api/auth/main_page"
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["referrer-policy"] == "no-referrer"


def test_welcome_unauthenticated_redirect_is_private_303(isolated_auth, auth_app):
    """An unauthenticated Welcome visit redirects without caching or forwarding referrers."""
    isolated_auth["required"] = True
    with TestClient(auth_app) as client:
        response = client.get("/api/auth/main_page", follow_redirects=False)
    assert response.status_code == 303
    assert response.headers["location"] == "/"
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["referrer-policy"] == "no-referrer"


@pytest.mark.parametrize("method,accept,status,redirects", [
    ("GET", "text/html", 401, True), ("GET", "application/json", 401, False),
    ("POST", "text/html", 401, False), ("GET", "text/html", 403, False),
])
def test_generic_page_redirect_does_not_change_api_errors(method, accept, status, redirects):
    """Adding referrer protection must retain the HTML-GET-only redirect boundary."""
    response = auth.unauthenticated_page_redirect(_request(method, accept), status)
    if redirects:
        assert response.status_code == 303
        assert response.headers["cache-control"] == "no-store"
        assert response.headers["referrer-policy"] == "no-referrer"
    else:
        assert response is None
