"""Regression tests for PB8 Welcome status and session-expiry navigation."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
import sys
from types import SimpleNamespace

from fastapi import HTTPException
from starlette.requests import Request
from starlette.responses import Response

import PBApiServer
import api.auth as auth
import pbgui_purefunc


def _ready_pb8(tmp_path: Path) -> tuple[Path, Path]:
    """Create a static PB8 artifact layout without executing external code."""
    pb8_dir = tmp_path / "pb8"
    src_dir = pb8_dir / "src"
    schema_file = src_dir / "config" / "schema.py"
    schema_file.parent.mkdir(parents=True)
    (src_dir / "passivbot_version.py").write_text('__version__ = "8.0.0"\n', encoding="utf-8")
    schema_file.write_text('CONFIG_SCHEMA_VERSION = "v8.0.0"\n', encoding="utf-8")
    python_file = tmp_path / "venv_pb8" / "bin" / "python"
    python_file.parent.mkdir(parents=True)
    python_file.symlink_to(Path(sys.executable))
    cli_file = python_file.parent / "passivbot"
    cli_file.write_text("#!/bin/sh\n", encoding="utf-8")
    cli_file.chmod(0o700)
    rust_dir = python_file.parent.parent / "lib" / "python3.12" / "site-packages" / "passivbot_rust"
    rust_dir.mkdir(parents=True)
    (rust_dir / "passivbot_rust.abi3.so").write_bytes(b"")
    return pb8_dir, python_file


def test_pb8_runtime_status_reports_installer_artifacts_ready(tmp_path: Path, monkeypatch) -> None:
    """Welcome recognizes the Rust artifact installed in PB8's editable virtualenv."""
    pb8_dir, python_file = _ready_pb8(tmp_path)
    monkeypatch.setattr(pbgui_purefunc, "pb8dir", lambda: str(pb8_dir))
    monkeypatch.setattr(pbgui_purefunc, "pb8venv", lambda: str(python_file))

    status = pbgui_purefunc.pb8_runtime_status()

    assert status["version"] == "8.0.0"
    assert status["config_schema"] == "v8.0.0"
    assert status["source_ready"] is True
    assert status["config_ready"] is True
    assert status["python_ready"] is True
    assert status["cli_ready"] is True
    assert status["rust_ready"] is True
    assert status["rust_file"].endswith("site-packages/passivbot_rust/passivbot_rust.abi3.so")
    assert status["ready"] is True
    assert status["errors"] == []


def test_pb8_runtime_status_rejects_wrong_schema_without_importing(tmp_path: Path, monkeypatch) -> None:
    """A non-V8 schema remains blocked without importing PB8 into the API process."""
    pb8_dir, python_file = _ready_pb8(tmp_path)
    (pb8_dir / "src" / "config" / "schema.py").write_text(
        'CONFIG_SCHEMA_VERSION = "v7.0.0"\n', encoding="utf-8"
    )
    monkeypatch.setattr(pbgui_purefunc, "pb8dir", lambda: str(pb8_dir))
    monkeypatch.setattr(pbgui_purefunc, "pb8venv", lambda: str(python_file))

    status = pbgui_purefunc.pb8_runtime_status()

    assert status["config_ready"] is False
    assert status["ready"] is False
    assert any("unexpected version" in error for error in status["errors"])


def test_pb8_runtime_status_rejects_incomplete_update_marker(tmp_path: Path, monkeypatch) -> None:
    """A failed or active PB8 update must keep new runtime consumers disabled."""
    pb8_dir, python_file = _ready_pb8(tmp_path)
    marker = tmp_path / "data" / "locks" / "pb8-runtime-invalid"
    marker.parent.mkdir(parents=True)
    marker.write_text("update failed\n", encoding="utf-8")
    monkeypatch.setattr(pbgui_purefunc, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(pbgui_purefunc, "pb8dir", lambda: str(pb8_dir))
    monkeypatch.setattr(pbgui_purefunc, "pb8venv", lambda: str(python_file))

    status = pbgui_purefunc.pb8_runtime_status()

    assert status["ready"] is False
    assert any("did not complete" in error for error in status["errors"])


def test_bootstrap_adds_nested_pb8_status_without_changing_pb7_fields(monkeypatch) -> None:
    """The Welcome bootstrap extends the established flat PB7 setup contract additively."""
    monkeypatch.setattr(
        auth,
        "_password_state",
        lambda: {"mode": "password", "required": True, "missing": False, "error": None, "security_warnings": []},
    )
    monkeypatch.setattr(auth, "pb7_runtime_status", lambda: {"ready": True, "master": True, "pb7dir": "/pb7"})
    monkeypatch.setattr(auth, "pb8_runtime_status", lambda: {"ready": True, "pb8dir": "/pb8"})

    setup = auth._bootstrap_payload()["setup"]

    assert setup["pb7dir"] == "/pb7"
    assert setup["ready"] is True
    assert setup["pb8"] == {"ready": True, "pb8dir": "/pb8", "required": True}


def test_pb8_only_setup_save_applies_on_next_operation(monkeypatch) -> None:
    """PB8 path saves use one INI transaction and do not request a service restart."""
    previous = {"pb7dir": "/pb7", "pb7venv": "/venv7/bin/python", "pbname": "master", "role": "master"}
    previous_pb8 = {"pb8dir": "/old/pb8", "pb8venv": "/old/venv/bin/python"}
    current = {
        **previous,
        "master": True,
        "pb8": {"pb8dir": "/new/pb8", "pb8venv": "/new/venv/bin/python"},
    }
    saved: list[tuple[str, dict[str, str]]] = []
    monkeypatch.setattr(auth, "pb7_runtime_status", lambda: dict(previous))
    monkeypatch.setattr(auth, "pb8_runtime_status", lambda: dict(previous_pb8))
    monkeypatch.setattr(auth, "save_ini_section", lambda section, values: saved.append((section, values)))
    monkeypatch.setattr(auth, "_bootstrap_payload", lambda _session: {"setup": current})

    result = auth.save_setup(
        auth.SetupConfigRequest(
            pb7dir="/pb7",
            pb7venv="/venv7/bin/python",
            pb8dir="/new/pb8",
            pb8venv="/new/venv/bin/python",
            pbname="master",
            role="master",
        ),
        SimpleNamespace(),
    )

    assert saved[0][1]["pb8dir"] == "/new/pb8"
    assert saved[0][1]["pb8venv"] == "/new/venv/bin/python"
    assert result["apply"]["timing"] == "next_operation"
    assert result["apply"]["restart_required"] is False


def test_pb8_setup_rejects_relative_or_control_character_paths() -> None:
    """New PB8 filesystem identifiers are validated at the authenticated boundary."""
    for value in ("relative/pb8", "/tmp/pb8\nother"):
        try:
            auth._validated_setup_path(value, "Passivbot V8 path")
        except HTTPException as exc:
            assert exc.status_code == 400
        else:
            raise AssertionError(f"Unsafe PB8 path was accepted: {value!r}")


def test_token_refresh_renews_the_httponly_cookie(monkeypatch) -> None:
    """Active browser keepalive keeps cookie and persisted token expiry aligned."""
    session = auth.SessionToken(token="session-token", user_id="test", created_at=1, expires_at=2)
    updated = auth.SessionToken(token="session-token", user_id="test", created_at=1, expires_at=9999999999)
    monkeypatch.setattr(auth, "refresh_token", lambda token: updated if token == session.token else None)
    request = Request(
        {
            "type": "http",
            "method": "POST",
            "scheme": "https",
            "server": ("pbgui.test", 443),
            "path": "/api/token-refresh",
            "query_string": b"",
            "headers": [],
        }
    )
    response = Response()

    result = asyncio.run(PBApiServer.token_refresh(request, response, session))

    cookie = response.headers["set-cookie"]
    assert result["expires_at"] == updated.expires_at
    assert f"{auth._request_session_cookie_name(request)}=session-token" in cookie
    assert "HttpOnly" in cookie
    assert "SameSite=strict" in cookie
    assert "Secure" in cookie


def test_login_and_shared_nav_escape_iframes_with_one_top_level_redirect() -> None:
    """Session expiry cannot render Login or Welcome inside a Dashboard iframe."""
    login_source = Path("frontend/root_login.html").read_text(encoding="utf-8")
    nav_source = Path("frontend/pbgui_nav.js").read_text(encoding="utf-8")

    assert "window.self !== window.top" in login_source
    assert "window.top.location.replace(url)" in login_source
    assert "window.top.location.replace(url)" in nav_source
    assert "var _authRedirecting = false" in nav_source
    assert "if (_authRedirecting) return" in nav_source


def test_welcome_frontend_renders_and_saves_pb8_runtime() -> None:
    """Overview and Setup visibly expose the PB8 status contract."""
    source = Path("frontend/welcome.html").read_text(encoding="utf-8")

    assert 'id="summary-pb8"' in source
    assert 'id="sb-pb8-state"' in source
    assert 'id="pb8dir"' in source
    assert 'id="pb8venv"' in source
    assert 'label: "PB8 CLI / Rust"' in source
    assert 'pb8dir: document.getElementById("pb8dir").value' in source
    assert 'pb8venv: document.getElementById("pb8venv").value' in source
