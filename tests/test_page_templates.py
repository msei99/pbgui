"""Offline contracts for shared script serialization and mounted page URLs."""

import json
import re

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from starlette.requests import Request

from api.page_templates import render_page_urls, script_json


@pytest.mark.parametrize("value", [
    '</script><script>bad()</script>&%%API_BASE%%',
    '"\\\n\u2028\u2029',
    {"nested": ["<>&%", None, True, 1.5]},
])
def test_script_json_preserves_values_without_html_or_template_delimiters(value):
    """Escaped values round-trip while remaining inert across template passes."""
    encoded = script_json(value)
    assert json.loads(encoded) == value
    assert not any(char in encoded for char in "<>&%\u2028\u2029")


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_script_json_rejects_non_json_numbers(value):
    """Non-finite numbers cannot enter page initialization state."""
    with pytest.raises(ValueError):
        script_json(value)


@pytest.mark.parametrize(("root", "expected"), [
    ("", ""), ("/", ""), ("/pbgui/", "/pbgui"),
    ('/a b/<>&"%/\u00e4', "/a%20b/%3C%3E%26%22%25/%C3%A4"),
    ("/literal%2f", "/literal%252f"),
])
def test_page_urls_use_only_asgi_prefix_and_rewrite_assets(root, expected):
    """API-only pages need no authority and ignore legacy URL query inputs."""
    request = Request({"type": "http", "root_path": root,
                       "query_string": b"root_path=//evil.test&api_base=https://evil.test"})
    html = ('"%%API_BASE%%"\n"%%BASE_PREFIX%%"\n'
            '<script src="/app/a.js?v=1"></script><link href="/app/a.css">'
            "<script src='/app/b.js'></script><a href='/other'>x</a>")
    rendered = render_page_urls(request, html, "/api/test")
    api, prefix, assets = rendered.split("\n")
    assert json.loads(api) == expected + "/api/test"
    assert json.loads(prefix) == expected
    assert f'src="{expected}/app/a.js?v=1"' in assets
    assert f'href="{expected}/app/a.css"' in assets
    assert f"src='{expected}/app/b.js'" in assets
    assert "href='/other'" in assets
    assert "evil.test" not in rendered


def test_page_url_attribute_markers_preserve_encoded_mount_paths() -> None:
    """HTML data attributes receive URL text rather than JavaScript escape sequences."""
    request = Request({"type": "http", "root_path": "/my gui"})
    html = 'data-api="%%API_BASE_ATTR%%" data-prefix="%%BASE_PREFIX_ATTR%%"'
    rendered = render_page_urls(request, html, "/api")
    assert rendered == 'data-api="/my%20gui/api" data-prefix="/my%20gui"'
    assert "\\u0025" not in rendered


@pytest.mark.parametrize("root", ["relative", "//evil.test", "//", "/../x", "/./x", "/x/../", "/a\\b", "/a\n", "/a\x7f", 123, None])
def test_page_urls_reject_invalid_asgi_mounts(root):
    """Malformed deployment prefixes fail closed before page serialization."""
    with pytest.raises(HTTPException) as exc:
        render_page_urls(Request({"type": "http", "root_path": root}), '"%%API_BASE%%"', "/api")
    assert exc.value.status_code == 500


@pytest.mark.parametrize("api_path", ["https://evil.test", "//evil.test", "relative", "/a/../b", "/api?x=1", "/api#x", "/api\\x", None])
def test_page_urls_reject_non_local_api_paths(api_path):
    """The shared helper only accepts a root-relative internal API route prefix."""
    with pytest.raises(HTTPException) as exc:
        render_page_urls(Request({"type": "http"}), '"%%API_BASE%%"', api_path)
    assert exc.value.status_code == 500


def test_websocket_origin_is_browser_derived_and_uses_mount_prefix():
    """WebSocket URLs ignore request authorities and use the loaded page origin."""
    request = Request({
        "type": "http",
        "root_path": "/pbgui",
        "headers": [(b"host", b"attacker.example")],
    })
    rendered = render_page_urls(request, 'var ws = "%%WS_BASE%%";', "/api")
    assert rendered == (
        "var ws = (window.location.protocol === 'https:' ? 'wss://' : 'ws://')"
        ' + window.location.host + "/pbgui";'
    )
    assert "attacker.example" not in rendered


@pytest.fixture
def versioned_page_client(tmp_path, monkeypatch):
    """Serve actual page handlers with isolated metadata and result roots."""
    import pbgui_purefunc
    from api import dashboard, optimize_v7, optimize_v8, pareto_explorer, v7_instances, v8_instances, vps, vps_manager
    from api.auth import require_auth

    monkeypatch.setattr(v7_instances, "_get_master_hostname", lambda: "test-master")
    monkeypatch.setattr(v8_instances, "_master_hostname", lambda: "test-master")
    monkeypatch.setattr(pbgui_purefunc, "PBGDIR", tmp_path)
    monkeypatch.setattr(optimize_v7, "get_optimize_limits_meta_payload", lambda: {})
    monkeypatch.setattr(optimize_v8, "get_pb8_optimize_metadata", lambda **_kwargs: {"limits": {}})

    roots = [(version, tmp_path / version / "optimize_results") for version in ("v7", "v8")]
    results = {}
    for version, root in roots:
        result = root / "test_result"
        (result / "pareto").mkdir(parents=True)
        results[version] = str(result)
    monkeypatch.setattr(pareto_explorer, "_optimize_result_roots_by_version", lambda: roots)

    app = FastAPI()
    app.dependency_overrides[require_auth] = lambda: None
    for module, prefix in (
        (optimize_v7, "/api/optimize-v7"), (optimize_v8, "/api/optimize-v8"),
        (v7_instances, "/api/v7"), (v8_instances, "/api/v8"),
        (pareto_explorer, "/api/pareto-explorer"),
        (dashboard, "/api/dashboard"), (vps, ""), (vps_manager, "/api/vps-manager"),
    ):
        app.include_router(module.router, prefix=prefix)
    with TestClient(app) as client:
        yield client, results


def _assert_initial_page_title(response, expected):
    """Check the server response before any browser JavaScript can correct it."""
    assert response.status_code == 200
    titles = re.findall(r"<title>(.*?)</title>", response.text, flags=re.DOTALL)
    assert len(titles) == 1
    assert "%%" not in titles[0]
    assert titles[0] == expected
    assert response.headers["cache-control"] == "no-store"


@pytest.mark.parametrize(("path", "expected"), [
    ("/api/optimize-v7/main_page", "PBGui - V7 Optimize"),
    ("/api/optimize-v8/main_page", "PBGui - V8 Optimize"),
    ("/api/v7/main_page", "PBv7 Run"),
    ("/api/v8/main_page", "PBv8 Run"),
    ("/api/v7/edit_page?new=1", "PBv7 Edit"),
    ("/api/v8/edit_page?new=1", "PBv8 Edit"),
])
def test_versioned_pages_render_correct_initial_title(versioned_page_client, path, expected):
    """PB7/PB8 handlers, including the PB8 renderer, deliver exact initial titles."""
    client, _results = versioned_page_client
    _assert_initial_page_title(client.get(path), expected)


@pytest.mark.parametrize(("version", "expected"), [
    (None, "V7"),
    ("v7", "V7"),
    ("v8", "V8"),
    (" V8 ", "V8"),
    (" V7 ", "V7"),
    ("invalid", "V7"),
    ("", "V7"),
])
def test_pareto_initial_title_uses_normalized_version(versioned_page_client, version, expected):
    """Empty explorer pages use the existing version default and normalization."""
    client, _results = versioned_page_client
    params = {} if version is None else {"optimize_version": version}
    response = client.get("/api/pareto-explorer/main_page", params=params)
    _assert_initial_page_title(response, f"PBGui - {expected} Pareto Explorer")
    assert f'window.OPTIMIZE_VERSION = "{expected.lower()}";' in response.text


@pytest.mark.parametrize(("result_version", "parameter_version"), [
    ("v7", "v8"),
    ("v8", "v7"),
])
def test_pareto_initial_title_prioritizes_selected_result(
    versioned_page_client, result_version, parameter_version,
):
    """Real result resolution overrides a conflicting query version in both directions."""
    client, results = versioned_page_client
    response = client.get("/api/pareto-explorer/main_page", params={
        "result_path": results[result_version],
        "optimize_version": parameter_version,
    })
    _assert_initial_page_title(response, f"PBGui - {result_version.upper()} Pareto Explorer")
    assert f'window.OPTIMIZE_VERSION = "{result_version}";' in response.text


AFFECTED_PAGES = [
    ("/api/dashboard/main_page", "/api", "dashboard_main.html"),
    ("/api/dashboard/editor_page", "/api", "dashboard_editor.html"),
    ("/api/dashboard/templates_page", "/api", "dashboard_templates.html"),
    ("/api/v7/main_page", "/api/v7", "v7_run.html"),
    ("/api/v7/edit_page", "/api/v7", "v7_edit.html"),
    ("/api/v8/main_page", "/api/v8", "v7_run.html"),
    ("/api/v8/edit_page", "/api/v8", "v7_edit.html"),
    ("/api/pareto-explorer/main_page", "/api/pareto-explorer", "v7_pareto_explorer.html"),
    ("/api/vps/main_page", "/api/vps", "vps_monitor.html"),
    ("/api/vps-manager/main_page", "/api/vps-manager", "vps_manager.html"),
]


def _initial_json(html, name):
    """Read one JSON-valued initialization assignment without executing the page."""
    match = re.search(r'\b' + re.escape(name) + r'\s*=\s*("(?:[^"\\]|\\.)*")\s*;', html)
    assert match is not None, name
    return json.loads(match[1])


@pytest.mark.parametrize(("path", "api_path", "filename"), AFFECTED_PAGES)
@pytest.mark.parametrize(("root", "prefix"), [
    ("", ""), ("/", ""), ("/team/pbgui", "/team/pbgui"),
    ("/team/pbgui/", "/team/pbgui"),
    ('/a b/<>&"%/ä', "/a%20b/%3C%3E%26%22%25/%C3%A4"),
    ("/literal%2f", "/literal%252f"),
])
def test_affected_page_mounts_and_complete_markers(
    versioned_page_client, path, api_path, filename, root, prefix,
):
    """Every real route renders all markers and mounted URLs independent of authority."""
    from pathlib import Path

    client, _ = versioned_page_client
    client.app.root_path = root
    response = client.get(path, params={"api_base": "https://attacker.example", "root_path": "//attacker.example"},
                          headers={"host": "attacker.example", "x-forwarded-host": "attacker.example"})
    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    html = response.text
    assert _initial_json(html, "API_BASE") == prefix + api_path
    assert not re.search(r"%%[A-Z_]+%%", html)
    assert "attacker.example" not in html
    template = (Path(__file__).resolve().parents[1] / "frontend" / filename).read_text(encoding="utf-8")
    for asset in re.findall(r'''\b(?:src|href)\s*=\s*["'](/app/[^"']+)["']''', template):
        if "%%NAV_HASH%%" not in asset:
            assert prefix + asset in html
    if "%%NAV_HASH%%" in template:
        assert re.search(re.escape(prefix + "/app/pbgui_nav.js?v=") + r"[^\"']+", html)
    if "%%WS_BASE%%" in template:
        assert '"%%WS_BASE%%"' in template
        assert ("(window.location.protocol === 'https:' ? 'wss://' : 'ws://')"
                " + window.location.host + " + script_json(prefix)) in html
    if "%%BASE_PREFIX%%" in template:
        assert _initial_json(html, "BASE_PREFIX") == prefix
    if filename == "vps_monitor.html":
        assert "apiBase: API_BASE" in html


@pytest.mark.parametrize(("path", "_api_path", "_filename"), AFFECTED_PAGES)
@pytest.mark.parametrize("root", ["//attacker.example", "/x/../y", "/x\\y", "/x\n"])
def test_affected_pages_reject_unsafe_mounts(versioned_page_client, path, _api_path, _filename, root):
    """All page entry points fail closed on malformed deployment prefixes."""
    client, _ = versioned_page_client
    route = next(route for route in client.app.routes if route.path == path)
    # Starlette may parse root_path as a regex before dispatch. Exercise the
    # registered renderer directly so invalid escapes reach our own boundary.
    kwargs = {field.name: field.default for field in route.dependant.query_params}
    kwargs.update({dependency.name: None for dependency in route.dependant.dependencies})
    with pytest.raises(HTTPException) as error:
        route.endpoint(Request({"type": "http", "root_path": root}), **kwargs)
    assert error.value.status_code == 500
    assert error.value.detail == "Invalid page mount path"


@pytest.mark.parametrize(("path", "_api_path", "_filename"), AFFECTED_PAGES)
def test_affected_page_initial_values_are_inert_and_preserved(
    versioned_page_client, monkeypatch, path, _api_path, _filename,
):
    """Names and draft paths survive later replacement passes without HTML injection."""
    from api import v7_instances, v8_instances

    value = 'fixture<>&%%VERSION%%-%%MASTER_NAME%%-%%RUN_LABEL%%-%%SERIAL%%-%%NAV_HASH%%'
    monkeypatch.setattr(v7_instances, "_get_master_hostname", lambda: value)
    monkeypatch.setattr(v8_instances, "_master_hostname", lambda: value)
    client, _ = versioned_page_client
    response = client.get(path, params={"name": value, "current": value, "draft_id": value, "result_path": value, "new": "1"})
    assert response.status_code == 200
    html = response.text
    assert value not in html
    assert not re.search(r"%%[A-Z_]+%%", html)
    if "/v7/" in path or "/v8/" in path:
        assert _initial_json(html, "MASTER_NAME") == value
        if path.endswith("edit_page"):
            assert _initial_json(html, "INSTANCE_NAME") == value
            assert _initial_json(html, "DRAFT_ID") == value
            assert _initial_json(html, "IS_NEW") == "true"
    elif "pareto-explorer" in path:
        assert _initial_json(html, "INIT_RESULT_PATH") == value
    elif "dashboard" in path:
        key = "ORIG_NAME" if path.endswith("editor_page") else "CURRENT"
        assert _initial_json(html, key) == value
