"""Offline contracts for shared script serialization and mounted page URLs."""

import json

import pytest
from fastapi import HTTPException
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
