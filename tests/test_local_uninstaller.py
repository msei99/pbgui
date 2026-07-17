"""Regression tests for master installer safety and uninstall paths."""

import asyncio
import configparser
from concurrent.futures import ThreadPoolExecutor
import inspect
import json
from pathlib import Path
import re
import stat
import subprocess
import sys
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

from fastapi import HTTPException, Response
from starlette.requests import Request
from starlette.websockets import WebSocketState

from api import api_keys, auth, dashboards
import market_data
import pbgui_purefunc
from secure_files import harden_sensitive_paths
from setup.installer import core
from setup.installer import web
import task_queue


def _write_unit(home: Path, unit: str, pbgui_dir: Path) -> Path:
    """Write a minimal pbgui systemd user unit for tests."""
    unit_dir = home / ".config" / "systemd" / "user"
    unit_dir.mkdir(parents=True, exist_ok=True)
    unit_path = unit_dir / unit
    unit_path.write_text(
        "[Service]\n"
        f"WorkingDirectory={pbgui_dir}\n"
        f"ExecStart={pbgui_dir.parent / 'venv_pbgui' / 'bin' / 'python'} -u {pbgui_dir / 'PBApiServer.py'}\n",
        encoding="utf-8",
    )
    return unit_path


def test_extract_pbgui_dir_from_unit_text_uses_working_directory() -> None:
    """The unit parser detects the checkout path from WorkingDirectory."""
    unit_text = "[Service]\nWorkingDirectory=/srv/one/pbgui\nExecStart=/venv/bin/python -u /srv/one/pbgui/PBRun.py\n"

    assert core._extract_pbgui_dir_from_unit_text(unit_text) == Path("/srv/one/pbgui")


def test_default_local_install_dir_prefers_existing_systemd_unit(tmp_path: Path, monkeypatch) -> None:
    """The installer defaults to the parent referenced by existing pbgui units."""
    home = tmp_path / "home"
    detected_parent = tmp_path / "detected"
    _write_unit(home, "pbgui-api.service", detected_parent / "pbgui")
    monkeypatch.setattr(core.Path, "home", staticmethod(lambda: home))

    assert core.default_local_install_dir() == str(detected_parent)


def test_local_systemd_units_for_install_skips_other_install(tmp_path: Path, monkeypatch) -> None:
    """Unit cleanup only selects units that point to the chosen install parent."""
    home = tmp_path / "home"
    selected_parent = tmp_path / "selected"
    other_parent = tmp_path / "other"
    _write_unit(home, "pbgui-api.service", other_parent / "pbgui")
    monkeypatch.setattr(core.Path, "home", staticmethod(lambda: home))
    logs: list[str] = []

    assert core._local_systemd_units_for_install(selected_parent, logs.append) == []
    assert any("points to" in line and "not selected" in line for line in logs)


def test_run_local_master_uninstall_keeps_other_install_units(tmp_path: Path, monkeypatch) -> None:
    """Uninstalling one parent does not stop or remove units for another parent."""
    home = tmp_path / "home"
    selected_parent = tmp_path / "selected"
    other_parent = tmp_path / "other"
    for parent in (selected_parent, other_parent):
        (parent / "pbgui").mkdir(parents=True)
    selected_unit = _write_unit(home, "pbgui-api.service", selected_parent / "pbgui")
    other_unit = _write_unit(home, "pbgui-pbrun.service", other_parent / "pbgui")
    monkeypatch.setattr(core.Path, "home", staticmethod(lambda: home))
    calls: list[list[str]] = []
    monkeypatch.setattr(core, "_run_user_systemctl_best_effort", lambda args, log: calls.append(args))
    logs: list[str] = []

    result = core.run_local_master_uninstall(
        core.LocalUninstallConfig(install_dir=str(selected_parent), confirm=True),
        logs.append,
    )

    assert result["ok"] is True
    assert ["stop", "pbgui-api.service"] in calls
    assert ["disable", "pbgui-api.service"] in calls
    assert ["stop", "pbgui-pbrun.service"] not in calls
    assert ["disable", "pbgui-pbrun.service"] not in calls
    assert not selected_unit.exists()
    assert other_unit.exists()
    assert not (selected_parent / "pbgui").exists()
    assert (other_parent / "pbgui").exists()


def test_fresh_install_configs_generate_individual_passwords() -> None:
    """Empty installer payloads never fall back to the known legacy password."""
    local_config = core.LocalMasterConfig.from_mapping({})
    remote_config = core.RemoteMasterConfig.from_mapping({"remote_host": "example.invalid"})

    assert len(local_config.pbgui_password) >= 20
    assert len(remote_config.pbgui_password) >= 20
    assert local_config.pbgui_password != remote_config.pbgui_password
    assert local_config.pbgui_password != "PBGui$Bot!"
    assert remote_config.pbgui_password != "PBGui$Bot!"


def test_browser_installer_displays_generated_password(monkeypatch) -> None:
    """The browser installer shows its generated password before installation."""
    generated = "generated-install-password"
    monkeypatch.setattr(web, "generate_pbgui_password", lambda: generated)
    monkeypatch.setattr(web, "local_prerequisite_status", lambda: {"missing": [], "sudo_password_useful": False})

    page = web._html()

    assert f'value="{generated}"' in page
    assert 'value="PBGui$Bot!"' not in page
    assert "Generated uniquely for this installation" in page


def test_remote_installer_limits_pbgui_port_to_vpn() -> None:
    """The remote bootstrap exposes the configured PBGui port only to VPN clients."""
    script = Path("setup/installer/scripts/remote_master_bootstrap.sh").read_text(encoding="utf-8")

    assert 'ufw allow from "$VPN_CIDR" to any port "$PBG_PORT" proto tcp' in script
    assert not re.search(r'^ufw allow ["$]*PBG_PORT', script, flags=re.MULTILINE)
    assert "PBGui$Bot!" not in script


def test_remote_installer_restricts_pbgui_credentials() -> None:
    """Remote installs restrict config, auth directory, and password file modes."""
    script = Path("setup/installer/scripts/remote_master_bootstrap.sh").read_text(encoding="utf-8")

    assert 'chmod 600 "$INSTALL_DIR/pbgui/pbgui.ini"' in script
    assert 'chmod 700 "$INSTALL_DIR/pbgui/data/auth"' in script
    assert 'chmod 600 "$INSTALL_DIR/pbgui/data/auth/secrets.toml"' in script
    assert 'auth_mode = "password"' in script


def test_auth_warns_for_wildcard_bind_with_legacy_password(monkeypatch) -> None:
    """Existing wildcard listeners surface the legacy-password exposure risk."""
    monkeypatch.setattr(auth, "_load_auth_secrets", lambda: ({"password": "PBGui$Bot!"}, None))
    monkeypatch.setattr(auth, "load_ini", lambda section, key: "0.0.0.0")

    state = auth._password_state()

    assert state["security_warnings"]
    assert "known legacy default password" in state["security_warnings"][0]


def test_auth_omits_exposure_warning_for_local_bind(monkeypatch) -> None:
    """A loopback-only listener does not report possible public exposure."""
    monkeypatch.setattr(auth, "_load_auth_secrets", lambda: ({"password": "PBGui$Bot!"}, None))
    monkeypatch.setattr(auth, "load_ini", lambda section, key: "127.0.0.1")

    assert auth._password_state()["security_warnings"] == []


def test_legacy_empty_password_remains_explicit_passwordless_mode(monkeypatch) -> None:
    """Existing empty-password installations retain intentional no-login behavior."""
    monkeypatch.setattr(auth, "_load_auth_secrets", lambda: ({"password": ""}, None))
    monkeypatch.setattr(auth, "load_ini", lambda section, key: "0.0.0.0")

    state = auth._password_state()

    assert state["mode"] == "disabled"
    assert state["required"] is False
    assert state["missing"] is True
    assert "Authentication is disabled" in state["security_warnings"][0]


def test_explicit_password_mode_rejects_missing_password(monkeypatch) -> None:
    """A malformed explicit password mode fails closed instead of becoming no-login."""
    monkeypatch.setattr(
        auth,
        "_load_auth_secrets",
        lambda: ({"auth_mode": "password", "password": ""}, None),
    )
    monkeypatch.setattr(auth, "load_ini", lambda section, key: "127.0.0.1")

    state = auth._password_state()

    assert state["mode"] == "error"
    assert "no password is configured" in state["error"]


def test_bootstrap_exposes_security_warning_only_after_authentication(monkeypatch) -> None:
    """The bootstrap reports security details only to authenticated sessions."""
    warning = "legacy exposure warning"
    monkeypatch.setattr(
        auth,
        "_password_state",
        lambda: {
            "mode": "password",
            "required": True,
            "missing": False,
            "error": None,
            "password": "PBGui$Bot!",
            "security_warnings": [warning],
        },
    )
    monkeypatch.setattr(auth, "pb7_runtime_status", lambda: {})
    session = auth.SessionToken(token="test", user_id="test", created_at=1, expires_at=2)

    assert auth._bootstrap_payload()["auth"]["security_warnings"] == []
    assert auth._bootstrap_payload(session)["auth"]["security_warnings"] == [warning]


def test_welcome_page_renders_authenticated_security_warnings() -> None:
    """The Welcome issue list includes authenticated security warnings safely."""
    source = Path("frontend/welcome.html").read_text(encoding="utf-8")

    assert "auth.security_warnings" in source
    assert "item.textContent = message" in source
    assert "renderStatus(auth, setup)" in source


def test_local_installer_writes_owner_only_credentials(tmp_path: Path) -> None:
    """Local installer config and password files are private from creation."""
    pbgui_dir = tmp_path / "pbgui"
    pbgui_dir.mkdir()
    config = core.LocalMasterConfig(
        install_dir=str(tmp_path),
        master_name="test-master",
        pbgui_password="test-password",
    )

    core._write_pbgui_config(config, tmp_path, pbgui_dir)
    core._write_auth_secret(config, pbgui_dir)

    assert stat.S_IMODE((pbgui_dir / "pbgui.ini").stat().st_mode) == 0o600
    assert stat.S_IMODE((pbgui_dir / "data" / "auth").stat().st_mode) == 0o700
    assert stat.S_IMODE((pbgui_dir / "data" / "auth" / "secrets.toml").stat().st_mode) == 0o600


def test_auth_tokens_and_ini_remain_owner_only(tmp_path: Path, monkeypatch) -> None:
    """Auth and INI rewrites preserve private permissions."""
    ini_path = tmp_path / "pbgui.ini"
    monkeypatch.setattr(pbgui_purefunc, "pbgui_ini_path", lambda: ini_path)
    monkeypatch.setattr(auth, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(auth, "pbgui_auth_secrets_path", lambda: tmp_path / "data" / "auth" / "secrets.toml")

    pbgui_purefunc.save_ini("main", "role", "master")
    auth._write_auth_secrets_toml({"password": "secret"})
    session = auth.generate_token("test-user", expires_in_seconds=60)
    token_file = tmp_path / "data" / "api_tokens" / f"{session.token}.json"

    assert stat.S_IMODE(ini_path.stat().st_mode) == 0o600
    assert stat.S_IMODE((tmp_path / "data" / "auth").stat().st_mode) == 0o700
    assert stat.S_IMODE((tmp_path / "data" / "auth" / "secrets.toml").stat().st_mode) == 0o600
    assert stat.S_IMODE(token_file.parent.stat().st_mode) == 0o700
    assert stat.S_IMODE(token_file.stat().st_mode) == 0o600

    token_file.chmod(0o644)
    assert auth.refresh_token(session.token, extends_seconds=120) is not None
    assert stat.S_IMODE(token_file.stat().st_mode) == 0o600


def test_sensitive_path_migration_repairs_existing_modes(tmp_path: Path) -> None:
    """Startup migration repairs existing credential trees without changing data."""
    pb7_root = tmp_path / "pb7"
    aws_root = tmp_path / ".aws"
    private_files = [
        tmp_path / "pbgui.ini",
        tmp_path / "data" / "auth" / "secrets.toml",
        tmp_path / "data" / "api_tokens" / "token.json",
        tmp_path / "data" / "api-keys" / "backup.json",
        tmp_path / "data" / "cluster" / "secret_blobs" / "sha256" / "aa" / "secret.json",
        tmp_path / "data" / "vpsmanager" / "hosts" / "test-vps" / "test-vps.json",
        pb7_root / "api-keys.json",
        aws_root / "credentials",
        aws_root / "config",
    ]
    for path in private_files:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("secret", encoding="utf-8")
        path.chmod(0o644)
    private_dirs = [
        tmp_path / "data" / "auth",
        tmp_path / "data" / "api_tokens",
        tmp_path / "data" / "api-keys",
        tmp_path / "data" / "cluster" / "secret_blobs",
        tmp_path / "data" / "cluster" / "secret_blobs" / "sha256",
        tmp_path / "data" / "cluster" / "secret_blobs" / "sha256" / "aa",
        tmp_path / "data" / "vpsmanager",
        tmp_path / "data" / "vpsmanager" / "hosts",
        tmp_path / "data" / "vpsmanager" / "hosts" / "test-vps",
        aws_root,
    ]
    for path in private_dirs:
        path.chmod(0o755)

    harden_sensitive_paths(tmp_path, pb7_root, aws_root)

    assert all(path.read_text(encoding="utf-8") == "secret" for path in private_files)
    assert all(stat.S_IMODE(path.stat().st_mode) == 0o600 for path in private_files)
    assert all(stat.S_IMODE(path.stat().st_mode) == 0o700 for path in private_dirs)


def test_login_sets_secure_httponly_session_cookie(monkeypatch) -> None:
    """Browser login stores the session in a protected cookie instead of a URL."""
    session = auth.SessionToken(token="cookie-token", user_id="test", created_at=1, expires_at=9999999999)
    monkeypatch.setattr(
        auth,
        "_password_state",
        lambda: {
            "error": None,
            "mode": "password",
            "required": True,
            "missing": False,
            "password": "secret",
            "security_warnings": [],
        },
    )
    monkeypatch.setattr(auth, "generate_token", lambda *args, **kwargs: session)
    request = Request(
        {
            "type": "http",
            "scheme": "https",
            "server": ("pbgui.test", 443),
            "path": "/api/auth/login",
            "query_string": b"",
            "headers": [(b"host", b"pbgui.test")],
            "client": ("127.0.0.1", 1),
        }
    )
    response = Response()

    result = auth.login(auth.LoginRequest(password="secret"), request, response)

    cookie = response.headers.get("set-cookie", "")
    assert result["auth"]["token"] == "cookie-token"
    assert f"{auth.SESSION_COOKIE_NAME}=cookie-token" in cookie
    assert "HttpOnly" in cookie
    assert "Secure" in cookie
    assert "SameSite=strict" in cookie


def _login_request(client_host: str) -> Request:
    """Build an isolated login request for a direct client address."""
    return Request(
        {
            "type": "http",
            "scheme": "https",
            "server": ("pbgui.test", 443),
            "path": "/api/auth/login",
            "query_string": b"",
            "headers": [(b"host", b"pbgui.test")],
            "client": (client_host, 1),
        }
    )


def _prepare_login_throttle_test(monkeypatch, clock: dict[str, float]) -> None:
    """Replace login dependencies so throttle tests never touch runtime files or logs."""
    session = auth.SessionToken(token="test-token", user_id="test", created_at=1, expires_at=9999999999)
    monkeypatch.setattr(auth, "_login_attempts", {})
    monkeypatch.setattr(auth, "_login_block_count", 0)
    monkeypatch.setattr(auth, "_login_last_block", None)
    monkeypatch.setattr(auth, "_login_security_history_loaded", True)
    monkeypatch.setattr(auth, "_login_now", lambda: clock["now"])
    monkeypatch.setattr(auth, "_log", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        auth,
        "_password_state",
        lambda: {
            "error": None,
            "mode": "password",
            "required": True,
            "missing": False,
            "password": "secret",
            "security_warnings": [],
        },
    )
    monkeypatch.setattr(auth, "generate_token", lambda *args, **kwargs: session)
    monkeypatch.setattr(auth, "_bootstrap_payload", lambda current: {"auth": {"token": current.token}})


def _attempt_login(password: str, client_host: str) -> dict | HTTPException:
    """Return either a successful login payload or the raised HTTP error."""
    try:
        return auth.login(auth.LoginRequest(password=password), _login_request(client_host), Response())
    except HTTPException as exc:
        return exc


def test_login_throttle_blocks_fifth_failure_and_honors_retry_after(monkeypatch) -> None:
    """Five failures temporarily block even a correct password until the lock expires."""
    clock = {"now": 1000.0}
    _prepare_login_throttle_test(monkeypatch, clock)

    for _ in range(4):
        error = _attempt_login("wrong", "198.51.100.10")
        assert isinstance(error, HTTPException)
        assert error.status_code == 401

    blocked = _attempt_login("wrong", "198.51.100.10")
    assert isinstance(blocked, HTTPException)
    assert blocked.status_code == 429
    assert blocked.headers == {"Retry-After": "30"}

    correct_while_blocked = _attempt_login("secret", "198.51.100.10")
    assert isinstance(correct_while_blocked, HTTPException)
    assert correct_while_blocked.status_code == 429

    clock["now"] += 31
    success = _attempt_login("secret", "198.51.100.10")
    assert isinstance(success, dict)
    assert success["auth"]["token"] == "test-token"
    assert "198.51.100.10" not in auth._login_attempts


def test_login_throttle_escalates_and_isolates_client_addresses(monkeypatch) -> None:
    """Repeated lockouts escalate without blocking a different direct client address."""
    clock = {"now": 2000.0}
    _prepare_login_throttle_test(monkeypatch, clock)

    for _ in range(5):
        first_lock = _attempt_login("wrong", "198.51.100.20")
    assert isinstance(first_lock, HTTPException)
    assert first_lock.headers == {"Retry-After": "30"}

    other_client = _attempt_login("secret", "198.51.100.21")
    assert isinstance(other_client, dict)

    clock["now"] += 31
    second_lock = _attempt_login("wrong", "198.51.100.20")
    assert isinstance(second_lock, HTTPException)
    assert second_lock.status_code == 429
    assert second_lock.headers == {"Retry-After": "60"}


def test_successful_login_resets_failure_window(monkeypatch) -> None:
    """A valid password clears earlier failures instead of carrying them into a later attempt."""
    clock = {"now": 3000.0}
    _prepare_login_throttle_test(monkeypatch, clock)

    for _ in range(3):
        assert _attempt_login("wrong", "198.51.100.30").status_code == 401
    assert isinstance(_attempt_login("secret", "198.51.100.30"), dict)

    for _ in range(4):
        error = _attempt_login("wrong", "198.51.100.30")
        assert isinstance(error, HTTPException)
        assert error.status_code == 401


def test_login_throttle_registry_evicts_oldest_client(monkeypatch) -> None:
    """Random source addresses cannot grow the process-local throttle registry without bound."""
    clock = {"now": 4000.0}
    _prepare_login_throttle_test(monkeypatch, clock)
    monkeypatch.setattr(auth, "_LOGIN_STATE_MAX_ENTRIES", 2)

    assert _attempt_login("wrong", "198.51.100.40").status_code == 401
    clock["now"] += 1
    assert _attempt_login("wrong", "198.51.100.41").status_code == 401
    clock["now"] += 1
    assert _attempt_login("wrong", "198.51.100.42").status_code == 401

    assert set(auth._login_attempts) == {"198.51.100.41", "198.51.100.42"}


def test_concurrent_login_failures_do_not_skip_lock_levels(monkeypatch) -> None:
    """Failures already in flight when a lock starts must reuse that lock duration."""
    clock = {"now": 5000.0}
    _prepare_login_throttle_test(monkeypatch, clock)

    retry_values = [auth._record_login_failure("198.51.100.50", clock["now"]) for _ in range(10)]

    assert retry_values[:4] == [0, 0, 0, 0]
    assert retry_values[4:] == [30, 30, 30, 30, 30, 30]
    assert auth._login_attempts["198.51.100.50"].lock_level == 1


def test_login_security_status_loads_retained_auth_log(monkeypatch, tmp_path) -> None:
    """Authenticated status includes retained lockouts and the latest blocked client."""
    log_dir = tmp_path / "data" / "logs"
    log_dir.mkdir(parents=True)
    (log_dir / "Auth.log").write_text(
        "2026-07-11T18:24:37.680 [Auth] [WARNING] Login temporarily blocked for client 127.0.0.1 after repeated failures; retry in 30s\n"
        "2026-07-11T18:26:05.563 [Auth] [WARNING] Login temporarily blocked for client 127.0.0.1 after repeated failures; retry in 60s\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(auth, "PBGDIR", tmp_path)
    monkeypatch.setattr(auth, "_login_attempts", {})
    monkeypatch.setattr(auth, "_login_block_count", 0)
    monkeypatch.setattr(auth, "_login_last_block", None)
    monkeypatch.setattr(auth, "_login_security_history_loaded", False)

    status = auth._login_security_status(now=5000.0)

    assert status == {
        "active_blocks": 0,
        "blocked_attempts": 2,
        "last_block": {
            "blocked_at": "2026-07-11T18:26:05.563",
            "client": "127.0.0.1",
            "retry_seconds": 60,
        },
        "acknowledged": False,
    }


def test_bootstrap_hides_login_security_from_unauthenticated_clients(monkeypatch) -> None:
    """Client addresses and lock history are exposed only after authentication."""
    monkeypatch.setattr(
        auth,
        "_password_state",
        lambda: {
            "error": None,
            "mode": "password",
            "required": True,
            "missing": False,
            "password": "secret",
            "security_warnings": [],
        },
    )
    monkeypatch.setattr(auth, "pb7_runtime_status", lambda: {})
    monkeypatch.setattr(
        auth,
        "_login_security_status",
        lambda: {"active_blocks": 1, "blocked_attempts": 2, "last_block": {"client": "198.51.100.1"}},
    )
    session = auth.SessionToken(token="token", user_id="test", created_at=1, expires_at=9999999999)

    assert auth._bootstrap_payload(None)["auth"]["login_security"] == {}
    assert auth._bootstrap_payload(session)["auth"]["login_security"]["last_block"]["client"] == "198.51.100.1"


def test_welcome_page_renders_login_security_status() -> None:
    """The authenticated Welcome overview visibly renders login lockout history."""
    source = Path("frontend/welcome.html").read_text(encoding="utf-8")

    assert 'label: "Login security"' in source
    assert "auth.login_security || {}" in source
    assert "temporary login lockout" in source
    assert '["status-detail", row.detail]' in source


def test_empty_password_cannot_implicitly_disable_authentication(monkeypatch) -> None:
    """Password updates require either a non-empty password or explicit disable flag."""
    monkeypatch.setattr(
        auth,
        "_password_state",
        lambda: {"error": None, "mode": "password", "required": True, "password": "secret"},
    )

    try:
        asyncio.run(
            auth.change_password(
                auth.PasswordChangeRequest(current_password="secret", new_password=""),
                _login_request("127.0.0.1"),
                Response(),
                auth.SessionToken(token="old", user_id="test", created_at=1, expires_at=2),
            )
        )
    except HTTPException as exc:
        assert exc.status_code == 400
        assert "explicitly disable authentication" in exc.detail
    else:
        raise AssertionError("Empty password unexpectedly disabled authentication")


def test_explicit_disable_rotates_sessions_and_persists_mode(monkeypatch) -> None:
    """Intentional no-login mode revokes old sessions and returns a fresh protected cookie."""
    written = {}
    new_session = auth.SessionToken(token="new-token", user_id="welcome:test", created_at=1, expires_at=9999999999)
    monkeypatch.setattr(
        auth,
        "_password_state",
        lambda: {"error": None, "mode": "password", "required": True, "password": "secret"},
    )
    monkeypatch.setattr(auth, "_load_auth_secrets", lambda: ({"auth_mode": "password", "password": "secret"}, None))
    monkeypatch.setattr(auth, "_write_auth_secrets_toml", lambda secrets: written.update(secrets))
    revoke_all = AsyncMock(return_value=3)
    monkeypatch.setattr(auth, "_revoke_all_sessions", revoke_all)
    monkeypatch.setattr(auth, "generate_token", lambda *args, **kwargs: new_session)
    monkeypatch.setattr(
        auth,
        "_bootstrap_payload",
        lambda session: {"auth": {"auth_mode": "disabled", "token": session.token}},
    )
    monkeypatch.setattr(auth, "_log", lambda *args, **kwargs: None)
    response = Response()

    result = asyncio.run(
        auth.change_password(
            auth.PasswordChangeRequest(current_password="secret", disable_auth=True),
            _login_request("127.0.0.1"),
            response,
            auth.SessionToken(token="old", user_id="test", created_at=1, expires_at=2),
        )
    )

    assert written == {"auth_mode": "disabled", "password": ""}
    revoke_all.assert_awaited_once()
    assert result["auth"]["token"] == "new-token"
    assert result["message"] == "Authentication disabled"
    assert f"{auth.SESSION_COOKIE_NAME}=new-token" in response.headers["set-cookie"]


def test_password_can_reenable_authentication_without_current_password(monkeypatch) -> None:
    """Intentional no-login users can restore password auth while rotating open sessions."""
    written = {}
    new_session = auth.SessionToken(token="secured-token", user_id="welcome:test", created_at=1, expires_at=9999999999)
    monkeypatch.setattr(
        auth,
        "_password_state",
        lambda: {"error": None, "mode": "disabled", "required": False, "password": ""},
    )
    monkeypatch.setattr(auth, "_load_auth_secrets", lambda: ({"auth_mode": "disabled", "password": ""}, None))
    monkeypatch.setattr(auth, "_write_auth_secrets_toml", lambda secrets: written.update(secrets))
    revoke_all = AsyncMock(return_value=4)
    monkeypatch.setattr(auth, "_revoke_all_sessions", revoke_all)
    monkeypatch.setattr(auth, "generate_token", lambda *args, **kwargs: new_session)
    monkeypatch.setattr(
        auth,
        "_bootstrap_payload",
        lambda session: {"auth": {"auth_mode": "password", "token": session.token}},
    )
    monkeypatch.setattr(auth, "_log", lambda *args, **kwargs: None)

    result = asyncio.run(
        auth.change_password(
            auth.PasswordChangeRequest(new_password="new-secret"),
            _login_request("127.0.0.1"),
            Response(),
            auth.SessionToken(token="open", user_id="test", created_at=1, expires_at=2),
        )
    )

    assert written == {"auth_mode": "password", "password": "new-secret"}
    revoke_all.assert_awaited_once()
    assert result["message"] == "Password authentication enabled"


def test_passwordless_sessions_are_reused_per_direct_client(monkeypatch, tmp_path) -> None:
    """Repeated no-login page loads from one client do not create token-file floods."""
    monkeypatch.setattr(auth, "PBGDIR", tmp_path)
    monkeypatch.setattr(auth, "_log", lambda *args, **kwargs: None)

    first = auth._get_or_create_passwordless_session("198.51.100.10")
    second = auth._get_or_create_passwordless_session("198.51.100.10")

    assert second.token == first.token
    assert len(list((tmp_path / "data" / "api_tokens").glob("*.json"))) == 1


def test_passwordless_session_registry_evicts_oldest_client(monkeypatch, tmp_path) -> None:
    """Distinct no-login clients cannot grow persisted token state without a bound."""
    monkeypatch.setattr(auth, "PBGDIR", tmp_path)
    monkeypatch.setattr(auth, "_PASSWORDLESS_SESSION_LIMIT", 1)
    monkeypatch.setattr(auth, "_log", lambda *args, **kwargs: None)

    first = auth._get_or_create_passwordless_session("198.51.100.11")
    second = auth._get_or_create_passwordless_session("198.51.100.12")

    assert auth.validate_token(first.token) is None
    assert auth.validate_token(second.token) is not None
    assert len(list((tmp_path / "data" / "api_tokens").glob("*.json"))) == 1


def test_welcome_requires_explicit_no_login_confirmation() -> None:
    """The Welcome page separates password changes from intentional auth disabling."""
    source = Path("frontend/welcome.html").read_text(encoding="utf-8")

    assert 'id="disable-auth-btn"' in source
    assert "window.PBGuiDialogs.confirm" in source
    assert "disable_auth: disableAuth" in source
    assert "Leave the new password empty" not in source


def test_global_nav_shows_non_dismissible_no_login_status() -> None:
    """Every standalone page can show the persistent authentication-disabled pill."""
    nav_source = Path("frontend/pbgui_nav.js").read_text(encoding="utf-8")
    server_source = Path("PBApiServer.py").read_text(encoding="utf-8")

    assert 'id="pbgui-auth-mode-pill"' in nav_source
    assert "NO LOGIN" in nav_source
    assert "updateAuthModeState" in nav_source
    assert '"auth": auth_runtime_status()' in server_source


def test_global_nav_allows_httponly_cookie_navigation() -> None:
    """Navigation must not require a browser-readable session token."""

    nav_source = Path("frontend/pbgui_nav.js").read_text(encoding="utf-8")

    assert "var canNavigate = !!c.token;" not in nav_source
    assert "disabled aria-disabled=\"true\"" not in nav_source


def test_login_security_acknowledgement_persists_and_new_event_realerts(monkeypatch, tmp_path) -> None:
    """Acknowledgement is global and only suppresses the event that was confirmed."""
    last_block = {
        "blocked_at": "2026-07-11T18:26:05.563",
        "client": "127.0.0.1",
        "retry_seconds": 60,
        "event_id": "abc123",
    }
    monkeypatch.setattr(auth, "PBGDIR", tmp_path)
    monkeypatch.setattr(auth, "_login_attempts", {})
    monkeypatch.setattr(auth, "_login_block_count", 2)
    monkeypatch.setattr(auth, "_login_last_block", last_block)
    monkeypatch.setattr(auth, "_login_security_history_loaded", True)
    session = auth.SessionToken(token="token", user_id="test", created_at=1, expires_at=9999999999)

    response = auth.acknowledge_login_security(session)

    ack_path = tmp_path / "data" / "auth" / "login_security_ack.json"
    saved = json.loads(ack_path.read_text(encoding="utf-8"))
    assert response["login_security"]["acknowledged"] is True
    assert saved["last_block"] == last_block
    assert ack_path.stat().st_mode & 0o777 == 0o600

    auth._login_last_block = {
        "blocked_at": "2026-07-11T18:30:00Z",
        "client": "198.51.100.8",
        "retry_seconds": 120,
        "event_id": "def456",
    }
    auth._login_block_count = 3

    status = auth._login_security_status(now=5000.0)

    assert status["acknowledged"] is False
    assert status["blocked_attempts"] == 3


def test_login_security_ack_survives_log_timestamp_reformat(monkeypatch, tmp_path) -> None:
    """Stable event IDs keep an acknowledgement valid after history reload."""
    ack_path = tmp_path / "data" / "auth" / "login_security_ack.json"
    ack_path.parent.mkdir(parents=True)
    ack_path.write_text(
        json.dumps(
            {
                "last_block": {
                    "blocked_at": "2026-07-11T18:26:05Z",
                    "client": "127.0.0.1",
                    "retry_seconds": 60,
                    "event_id": "abc123",
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(auth, "PBGDIR", tmp_path)
    monkeypatch.setattr(auth, "_login_attempts", {})
    monkeypatch.setattr(auth, "_login_block_count", 1)
    monkeypatch.setattr(
        auth,
        "_login_last_block",
        {
            "blocked_at": "2026-07-11T18:26:05.563",
            "client": "127.0.0.1",
            "retry_seconds": 60,
            "event_id": "abc123",
        },
    )
    monkeypatch.setattr(auth, "_login_security_history_loaded", True)

    assert auth._login_security_status(now=5000.0)["acknowledged"] is True


def test_welcome_login_security_ack_uses_authenticated_endpoint() -> None:
    """The Welcome warning exposes an explicit authenticated acknowledgement action."""
    source = Path("frontend/welcome.html").read_text(encoding="utf-8")

    assert 'acknowledgeButton.textContent = "Acknowledge"' in source
    assert '"/api/auth/login-security/ack"' in source
    assert "headers: authHeaders(false)" in source
    assert 'setBanner("Login security alert acknowledged.", "success")' in source


def test_http_auth_rejects_query_tokens_and_accepts_cookie_or_bearer() -> None:
    """Session authentication has no query parameter while API Bearer auth remains."""
    assert "token" not in inspect.signature(auth.get_token_from_request).parameters
    assert auth.get_token_from_request(authorization="Bearer api-token", session_cookie=None) == "api-token"
    assert auth.get_token_from_request(authorization=None, session_cookie="cookie-token") == "cookie-token"


def test_unauthenticated_page_redirect_preserves_api_401_responses() -> None:
    """Only top-level HTML navigation is redirected to login after session loss."""
    page_request = Request(
        {
            "type": "http",
            "method": "GET",
            "scheme": "https",
            "server": ("pbgui.test", 443),
            "path": "/api/v7/main_page",
            "query_string": b"token=legacy-token",
            "headers": [(b"accept", b"text/html,application/xhtml+xml")],
        }
    )
    api_request = Request(
        {
            "type": "http",
            "method": "GET",
            "scheme": "https",
            "server": ("pbgui.test", 443),
            "path": "/api/v7/instances",
            "query_string": b"",
            "headers": [(b"accept", b"application/json")],
        }
    )

    redirect = auth.unauthenticated_page_redirect(page_request, 401)

    assert redirect is not None
    assert redirect.status_code == 303
    assert redirect.headers["location"] == "/"
    assert auth.unauthenticated_page_redirect(api_request, 401) is None
    assert auth.unauthenticated_page_redirect(page_request, 404) is None


def test_logout_immediately_closes_token_websockets(monkeypatch) -> None:
    """Logout revokes the token cookie and closes every active privileged socket."""
    session = auth.SessionToken(token="socket-token", user_id="test", created_at=1, expires_at=9999999999)
    websocket = MagicMock()
    websocket.cookies = {auth.SESSION_COOKIE_NAME: session.token}
    websocket.client_state = WebSocketState.CONNECTED
    websocket.state = SimpleNamespace()
    websocket.accept = AsyncMock()
    websocket.close = AsyncMock()
    monkeypatch.setattr(auth, "validate_token", lambda token: session if token == session.token else None)
    monkeypatch.setattr(auth, "revoke_token", lambda token: token == session.token)

    async def scenario() -> Response:
        """Authenticate one socket, then exercise the asynchronous logout path."""
        assert await auth.authenticate_websocket(websocket) == session
        response = Response()
        assert await auth.logout(response, session) == {"ok": True}
        await asyncio.sleep(0)
        return response

    response = asyncio.run(scenario())

    websocket.accept.assert_awaited_once()
    websocket.close.assert_awaited_once_with(code=4001, reason="Session logged out")
    assert session.token not in auth._websocket_sessions
    assert f"{auth.SESSION_COOKIE_NAME}=\"\"" in response.headers.get("set-cookie", "")


def test_expired_session_closes_active_websocket(monkeypatch) -> None:
    """The watchdog closes a privileged socket as soon as its session expires."""
    session = auth.SessionToken(token="expiring-token", user_id="test", created_at=1, expires_at=2)
    websocket = MagicMock()
    websocket.cookies = {auth.SESSION_COOKIE_NAME: session.token}
    websocket.client_state = WebSocketState.CONNECTED
    websocket.state = SimpleNamespace()
    websocket.accept = AsyncMock()
    websocket.close = AsyncMock()
    validation_results = iter((session, None))
    real_sleep = asyncio.sleep

    async def immediate_sleep(_delay: float) -> None:
        """Yield immediately so the watchdog test does not wait one second."""

    monkeypatch.setattr(auth, "validate_token", lambda _token: next(validation_results))
    monkeypatch.setattr(auth.asyncio, "sleep", immediate_sleep)

    async def scenario() -> None:
        """Authenticate one socket and allow its watchdog task to run."""
        assert await auth.authenticate_websocket(websocket) == session
        await real_sleep(0)

    asyncio.run(scenario())

    websocket.close.assert_awaited_once_with(code=4001, reason="Session expired or revoked")
    assert session.token not in auth._websocket_sessions


def test_session_tokens_are_absent_from_browser_and_backend_urls() -> None:
    """PBGui page, SSE, WebSocket, and proxy URLs must never contain sessions."""
    frontend_root = Path("frontend")
    forbidden = re.compile(
        r"(?:[?&]token=|(?:searchParams|params)\.set\(['\"]token|"
        r"(?:[A-Za-z_$][\w$]*|new\s+URLSearchParams\([^)]*\))\.get\(['\"]token['\"]\))"
    )
    offenders = []
    for path in sorted(frontend_root.rglob("*")):
        if path.suffix not in {".html", ".js"}:
            continue
        if forbidden.search(path.read_text(encoding="utf-8")):
            offenders.append(str(path))
    assert offenders == []

    jobs_monitor = Path("frontend/jobs_monitor.html").read_text(encoding="utf-8")
    assert "No authentication token provided" not in jobs_monitor
    assert "Bearer ${API_TOKEN" not in jobs_monitor

    nav_source = Path("frontend/pbgui_nav.js").read_text(encoding="utf-8")
    help_source = Path("frontend/help.html").read_text(encoding="utf-8")
    assert "document.head.appendChild(script)" in nav_source
    assert "window.PBGuiSharedHelp.open('overview'" in nav_source
    assert "window.history.back()" in help_source

    auth_source = Path("api/auth.py").read_text(encoding="utf-8")
    websocket_sources = [Path("PBApiServer.py"), *Path("api").glob("*.py")]
    assert "token: Optional[str] = Query" not in auth_source
    assert all(
        "websocket.query_params.get(\"token\"" not in path.read_text(encoding="utf-8")
        for path in websocket_sources
    )


def test_concurrent_ini_writes_preserve_all_keys(tmp_path: Path, monkeypatch) -> None:
    """Independent concurrent INI updates must not overwrite each other."""
    ini_path = tmp_path / "pbgui.ini"
    monkeypatch.setattr(pbgui_purefunc, "pbgui_ini_path", lambda: ini_path)
    original_write = pbgui_purefunc._write_ini_config

    def slow_write(config: configparser.ConfigParser) -> None:
        """Widen the race window while the transaction lock is held."""
        time.sleep(0.005)
        original_write(config)

    monkeypatch.setattr(pbgui_purefunc, "_write_ini_config", slow_write)
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda index: pbgui_purefunc.save_ini("race", f"key_{index}", str(index)), range(16)))

    config = configparser.ConfigParser()
    config.read(ini_path)
    assert dict(config.items("race")) == {f"key_{index}": str(index) for index in range(16)}


def test_concurrent_api_key_transactions_preserve_comments(tmp_path: Path, monkeypatch) -> None:
    """API-key route transactions must reload after prior writers complete."""
    api_file = tmp_path / "api-keys.json"
    api_file.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(api_keys, "_PBGDIR", str(tmp_path))

    class FakeUsers:
        """Minimal file-backed Users stand-in exposing comment extras."""

        def __init__(self) -> None:
            self._top_level_extras = json.loads(api_file.read_text(encoding="utf-8"))

        def save(self) -> None:
            """Persist through a shared temp name to expose concurrent writers."""
            time.sleep(0.005)
            tmp = api_file.with_suffix(".tmp")
            tmp.write_text(json.dumps(self._top_level_extras), encoding="utf-8")
            tmp.replace(api_file)

    monkeypatch.setattr(api_keys, "_get_users", FakeUsers)
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(
            pool.map(
                lambda index: api_keys.create_comment(
                    api_keys.CommentField(key=f"field_{index}", value=str(index))
                ),
                range(12),
            )
        )

    saved = json.loads(api_file.read_text(encoding="utf-8"))
    assert saved == {f"_comment_field_{index}": str(index) for index in range(12)}


def test_concurrent_dashboard_writes_remain_atomic(tmp_path: Path, monkeypatch) -> None:
    """Concurrent saves to one dashboard must not collide on its temp file."""
    dashboard_dir = tmp_path / "dashboards"
    dashboard_dir.mkdir()
    monkeypatch.setattr(dashboards, "_dashboards_dir", lambda: dashboard_dir)

    def save(index: int) -> dict[str, str]:
        """Save one valid revision of the same dashboard."""
        return dashboards.save_dashboard("main", {"rows": 1, "cols": 1, "revision": index})

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(save, range(16)))

    payload = json.loads((dashboard_dir / "main.json").read_text(encoding="utf-8"))
    assert all(result == {"status": "ok", "name": "main"} for result in results)
    assert payload["revision"] in range(16)


def test_concurrent_task_updates_preserve_all_fields(tmp_path: Path, monkeypatch) -> None:
    """Worker and API mutations to one job must share one queue transaction."""
    tasks_root = tmp_path / "tasks"
    monkeypatch.setattr(task_queue, "get_tasks_root_dir", lambda: tasks_root)
    task_queue.ensure_task_dirs()
    job_path = task_queue.get_task_state_dir("running") / "job.json"
    task_queue._atomic_write_json(job_path, {"id": "job", "status": "running"})

    def update(index: int) -> None:
        """Add one field while widening the read-modify-write race window."""
        def mutate(job: dict) -> None:
            time.sleep(0.005)
            job[f"field_{index}"] = index

        task_queue.update_job_file(job_path, mutate=mutate)

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(update, range(12)))

    saved = json.loads(job_path.read_text(encoding="utf-8"))
    assert all(saved[f"field_{index}"] == index for index in range(12))


def test_task_updates_are_serialized_across_processes(tmp_path: Path, monkeypatch) -> None:
    """Independent API/worker processes must not lose task-file updates."""
    tasks_root = tmp_path / "tasks"
    monkeypatch.setattr(task_queue, "get_tasks_root_dir", lambda: tasks_root)
    task_queue.ensure_task_dirs()
    job_path = task_queue.get_task_state_dir("running") / "job.json"
    task_queue._atomic_write_json(job_path, {"id": "job", "status": "running"})

    start_file = tmp_path / "start"
    fields = [f"process_{index}" for index in range(6)]
    script = """
import sys
import time
from pathlib import Path
import task_queue

tasks_root = Path(sys.argv[1])
field = sys.argv[2]
start_file = Path(sys.argv[3])
task_queue.get_tasks_root_dir = lambda: tasks_root
job_path = task_queue.get_task_state_dir("running") / "job.json"
deadline = time.monotonic() + 10
while not start_file.exists() and time.monotonic() < deadline:
    time.sleep(0.005)
if not start_file.exists():
    raise TimeoutError("process start signal was not created")
def mutate(job):
    time.sleep(0.02)
    job[field] = field
task_queue.update_job_file(job_path, mutate=mutate)
"""
    processes = [
        subprocess.Popen(
            [sys.executable, "-c", script, str(tasks_root), field, str(start_file)],
            cwd=Path.cwd(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for field in fields
    ]
    try:
        start_file.touch()
        results = [process.communicate(timeout=15) for process in processes]
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)

    assert [(process.returncode, stderr) for process, (_, stderr) in zip(processes, results)] == [
        (0, "") for _ in processes
    ]
    saved = json.loads(job_path.read_text(encoding="utf-8"))
    assert all(saved[field] == field for field in fields)


def test_concurrent_market_data_updates_preserve_exchanges(tmp_path: Path, monkeypatch) -> None:
    """Per-exchange market-data changes must merge into the latest config."""
    config_path = tmp_path / "market_data.json"
    monkeypatch.setattr(market_data, "get_market_data_config_path", lambda: config_path)
    original_write = market_data._atomic_write_text

    def slow_write(path: Path, payload: str) -> None:
        """Widen the stale-read window while preserving the real atomic writer."""
        time.sleep(0.005)
        original_write(path, payload)

    monkeypatch.setattr(market_data, "_atomic_write_text", slow_write)
    exchanges = ["binance", "bybit", "bitget", "okx", "hyperliquid"]
    with ThreadPoolExecutor(max_workers=5) as pool:
        list(pool.map(lambda exchange: market_data.set_enabled_coins(exchange, ["BTC"]), exchanges))

    saved = market_data.load_market_data_config()
    assert all(saved.enabled_coins[exchange] == ["BTC"] for exchange in exchanges)
