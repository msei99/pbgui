"""Focused tests for the authenticated Logging Monitor API."""

from pathlib import Path
from types import SimpleNamespace

import pytest
from starlette.requests import Request

from api.auth import require_auth
import api.logging as logging_api
import logging_helpers


@pytest.fixture
def isolated_log_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Redirect Logging Monitor filesystem access away from runtime logs."""
    log_root = tmp_path / "logs"
    log_root.mkdir()
    monkeypatch.setattr(logging_helpers, "LOG_ROOT", log_root)
    return log_root


def _request() -> Request:
    """Build a minimal request for direct page rendering tests."""
    return Request({
        "type": "http",
        "scheme": "http",
        "server": ("testserver", 80),
        "path": "/",
        "headers": [],
        "query_string": b"",
    })


def test_main_page_does_not_read_or_render_session_token():
    """Rendered monitor HTML should rely on its HttpOnly cookie only."""
    class CookieOnlySession:
        """Fail if page rendering attempts to read any session field."""

        def __getattr__(self, name):
            raise AssertionError(f"session field accessed: {name}")

    response = logging_api.get_main_page(_request(), CookieOnlySession())
    html = response.body.decode()

    assert "%%TOKEN%%" not in html
    assert "TOKEN" not in html
    assert "Authorization" not in html
    assert "PBGUI_NAV_CONFIG" in html


def test_logging_routes_require_auth_dependency():
    """Every monitor route should retain the shared auth dependency."""
    for route in logging_api.router.routes:
        dependency_calls = {dependency.call for dependency in route.dependant.dependencies}
        assert require_auth in dependency_calls


def test_sparse_rotations_are_listed_in_numeric_order(isolated_log_root, monkeypatch):
    """Missing generations must not hide later configured rotations."""
    base = isolated_log_root / "PBGui.log"
    base.write_text("current", encoding="utf-8")
    (isolated_log_root / "PBGui.log.1").write_text("one", encoding="utf-8")
    (isolated_log_root / "PBGui.log.3").write_text("three", encoding="utf-8")
    monkeypatch.setattr(logging_api, "get_rotate_settings", lambda **kwargs: (1024, 3))

    result = logging_api.list_log_files(SimpleNamespace())

    assert result["rotated"] == {"PBGui.log": ["PBGui.log.1", "PBGui.log.3"]}
    assert result["sizes"]["PBGui.log.3"] == 5


@pytest.mark.parametrize(
    "filename",
    ["PBGui.log.1", "PBGui.log.old", "../PBGui.log", "sub/PBGui.log", "sub\\PBGui.log", "bad\n.log", ".log"],
)
def test_purge_rejects_non_base_or_unsafe_names(isolated_log_root, filename):
    """Purge should accept only strict, separator-free base log names."""
    with pytest.raises(logging_api.HTTPException) as exc_info:
        logging_api.purge_logfile(filename, SimpleNamespace())

    assert exc_info.value.status_code == 400


def test_purge_uses_effective_configured_rotation(isolated_log_root, monkeypatch):
    """Purge should pass the physical log's effective size and count."""
    logfile = isolated_log_root / "PBGui.log"
    logfile.write_text("content", encoding="utf-8")
    calls = {}

    def fake_settings(**kwargs):
        calls["settings"] = kwargs
        return 4321, 7

    def fake_purge(path, max_bytes, backup_count):
        calls["purge"] = (path, max_bytes, backup_count)
        return True, "purged"

    monkeypatch.setattr(logging_api, "get_rotate_settings", fake_settings)
    monkeypatch.setattr(logging_api, "purge_log_to_rotated", fake_purge)
    monkeypatch.setattr(logging_api, "_log", lambda *args, **kwargs: None)

    result = logging_api.purge_logfile("PBGui.log", SimpleNamespace())

    assert result == {"success": True, "message": "purged"}
    assert calls["settings"] == {"logfile": str(logfile.resolve())}
    assert calls["purge"] == (str(logfile.resolve()), 4321, 7)


def test_purge_failure_is_logged_and_returns_generic_detail(isolated_log_root, monkeypatch):
    """Operational purge failures are logged without exposing helper details."""
    logfile = isolated_log_root / "PBGui.log"
    logfile.write_text("content", encoding="utf-8")
    events = []
    monkeypatch.setattr(logging_api, "get_rotate_settings", lambda **kwargs: (4321, 2))
    monkeypatch.setattr(logging_api, "purge_log_to_rotated", lambda *args: (False, "sanitized failure"))
    monkeypatch.setattr(logging_api, "_log", lambda *args, **kwargs: events.append((args, kwargs)))

    with pytest.raises(logging_api.HTTPException) as exc_info:
        logging_api.purge_logfile("PBGui.log", SimpleNamespace())

    assert exc_info.value.status_code == 500
    assert exc_info.value.detail == "Failed to purge log file"
    assert events[0][1]["level"] == "ERROR"
    assert events[0][1]["meta"] == {"operation": "purge_log"}


def test_rotation_settings_resolve_by_physical_logfile(isolated_log_root, monkeypatch):
    """Grouped PBGui settings should be requested by physical logfile stem."""
    logfile = isolated_log_root / "PBGui.log"
    logfile.touch()
    requested = []

    def fake_settings(**kwargs):
        requested.append(kwargs)
        return 2 * 1024 * 1024, 4

    monkeypatch.setattr(logging_api, "get_rotate_defaults", lambda: (1024 * 1024, 1))
    monkeypatch.setattr(logging_api, "get_rotate_settings", fake_settings)

    result = logging_api.get_rotation(SimpleNamespace())

    assert requested == [{"logfile": str(logfile)}]
    assert result["per_service"]["PBGui"] == {"max_mb": 2, "backup_count": 4}
    assert set(result["managed_scopes"]) == set(logging_helpers.MANAGED_LOG_SCOPES)


def test_managed_top_level_logs_are_not_duplicated_as_per_log_rows(isolated_log_root):
    """Special managed logs should have one unambiguous settings row."""
    (isolated_log_root / "PBApiServer.console.log").write_text("console\n", encoding="utf-8")
    (isolated_log_root / "PBRun.log").write_text("service\n", encoding="utf-8")

    result = logging_api.get_rotation(SimpleNamespace())

    assert "PBApiServer.console" not in result["per_service"]
    assert "PBRun" in result["per_service"]


def test_retired_service_logs_are_not_offered_as_rotation_settings(isolated_log_root):
    """Files without an active writer must not receive editable rotation rules."""
    for stem in (
        "PBRemote", "PBMon", "sync", "FastAPI", "FileSync", "PBStat",
        "V7ConfigSync", "config_archives", "Auth", "LiveSession",
    ):
        (isolated_log_root / f"{stem}.log").write_text("legacy\n", encoding="utf-8")

    result = logging_api.get_rotation(SimpleNamespace())

    assert {
        "PBRemote", "PBMon", "sync", "FastAPI", "FileSync", "PBStat",
        "V7ConfigSync", "config_archives", "Auth", "LiveSession",
    }.isdisjoint(result["per_service"])


def test_managed_rotation_save_validates_scope(monkeypatch):
    """Managed requests cannot create arbitrary INI setting names."""
    monkeypatch.setattr(logging_api, "_log", lambda *args, **kwargs: None)
    with pytest.raises(logging_api.HTTPException) as exc_info:
        logging_api.save_rotation(logging_api.RotationSaveIn(scope="managed:unknown", max_mb=2, backup_count=1), SimpleNamespace())
    assert exc_info.value.status_code == 400


def test_logging_ui_builds_managed_rows_without_html_interpolation():
    """Managed payload labels and paths must be rendered through textContent."""
    html = (Path(logging_api.__file__).parent.parent / "frontend" / "logging_monitor.html").read_text(encoding="utf-8")
    managed_block = html[html.index("var managedBody"):html.index("var services")]
    assert "textContent" in managed_block
    assert "innerHTML" not in managed_block
    assert "managed:" in html
