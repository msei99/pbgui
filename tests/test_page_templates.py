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
    from api import optimize_v7, optimize_v8, pareto_explorer, v7_instances, v8_instances
    from api.auth import require_auth

    monkeypatch.setattr(v7_instances, "_get_master_hostname", lambda: "test-master")
    monkeypatch.setattr(v8_instances, "_master_hostname", lambda: "test-master")
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
