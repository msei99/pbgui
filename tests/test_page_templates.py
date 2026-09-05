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


@pytest.mark.parametrize(("scheme", "host", "expected"), [
    ("https", "example.test", "wss://example.test/pbgui"),
    ("http", "example.test:8080", "ws://example.test:8080/pbgui"),
    ("https", "[2001:db8::1]:8443", "wss://[2001:db8::1]:8443/pbgui"),
])
def test_websocket_origin_preserves_scheme_ipv6_port_and_mount(scheme, host, expected):
    """WebSocket URLs use the same validated visible authority as auth pages."""
    request = Request({"type": "http", "scheme": scheme, "path": "/page", "root_path": "/pbgui",
                       "query_string": b"", "headers": [(b"host", host.encode())]})
    assert json.loads(render_page_urls(request, '"%%WS_BASE%%"', "/api")) == expected


@pytest.mark.parametrize("hosts", [[b"evil.test@safe.test"], [b"safe.test:bad"], [b"safe.test\\evil"], [b"safe.test", b"other.test"]])
def test_websocket_origin_rejects_malformed_or_duplicate_authorities(hosts):
    """A WebSocket marker must not relax the established origin validation."""
    request = Request({"type": "http", "scheme": "https", "path": "/page", "query_string": b"",
                       "headers": [(b"host", host) for host in hosts]})
    with pytest.raises(HTTPException) as exc:
        render_page_urls(request, '"%%WS_BASE%%"', "/api")
    assert exc.value.status_code == 400
