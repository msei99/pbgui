"""Offline Node-backed dashboard lifecycle and same-origin security regressions."""

import json
import subprocess
from pathlib import Path

import pytest


@pytest.mark.parametrize("case", ["creation", "templates", "cancel", "resize", "charts", "messages", "assets"])
def test_dashboard_frontend_regressions(case: str) -> None:
    """Execute the real frontend handlers against isolated DOM and HTTP doubles."""
    script = Path(__file__).with_name("dashboard_frontend_regressions.cjs")
    result = subprocess.run(
        ["node", str(script), case], capture_output=True, text=True, timeout=30
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("page", ["editor_page", "templates_page", "main_page"])
@pytest.mark.parametrize("root_path", ["", "/pbgui", "/team/pbgui"])
@pytest.mark.parametrize("api_base", [
    "https://evil.test/api",
    "//evil.test/api",
    "https://testserver.evil.test/api",
    "https://testserver@evil.test/api",
    "javascript:alert(1)",
    '</script><script src="https://evil.test/payload.js"></script>',
    "/api",
    "http://testserver/api",
    "",
])
def test_dashboard_page_ignores_api_base(monkeypatch, tmp_path, page, api_base, root_path):
    """Real HTML routes and JS functions never use query input for fetch/scripts/WS."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    import pbgui_purefunc
    from api import dashboard

    monkeypatch.setattr(pbgui_purefunc, "PBGDIR", tmp_path)
    app = FastAPI(root_path=root_path)
    app.include_router(dashboard.router, prefix="/api/dashboard")
    app.dependency_overrides[dashboard.require_auth] = lambda: object()
    with TestClient(app) as client:
        response = client.get(
            f"{root_path}/api/dashboard/{page}",
            params={"api_base": api_base, "root_path": "//evil.test"},
        )
    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    assert "evil.test" not in response.text
    script = Path(__file__).with_name("dashboard_frontend_regressions.cjs")
    result = subprocess.run(
        ["node", str(script), "page"],
        input=json.dumps({"html": response.text, "page": page, "prefix": root_path}),
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("root_path,prefix", [
    ("/", ""), ("/pbgui/", "/pbgui"), ("/pbgui space", "/pbgui%20space"),
    ('/pbgui\"<>', "/pbgui%22%3C%3E"),
])
def test_dashboard_mount_path_normalized(root_path, prefix):
    """Trusted mount paths are encoded for URL and inline HTML/script contexts."""
    from starlette.requests import Request
    from api import dashboard

    response = dashboard.get_editor_page(
        Request({"type": "http", "root_path": root_path}), name="", api_base="//evil.test",
        view_only=False, standalone=False, session=object(),
    )
    result = subprocess.run(
        ["node", str(Path(__file__).with_name("dashboard_frontend_regressions.cjs")), "page"],
        input=json.dumps({"html": response.body.decode(), "page": "editor_page", "prefix": prefix}),
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("root_path", ["//evil.test", "https://evil.test", "javascript:bad", "/\\evil.test", "/pbgui/..", "/pbgui\n"])
def test_dashboard_mount_path_rejects_unsafe_prefix(root_path):
    """Invalid mount configuration cannot produce external or traversal URLs."""
    from fastapi import HTTPException
    from starlette.requests import Request
    from api import dashboard

    with pytest.raises(HTTPException) as error:
        dashboard.get_editor_page(
            Request({"type": "http", "root_path": root_path}), name="", api_base="",
            view_only=False, standalone=False, session=object(),
        )
    assert error.value.status_code == 500
