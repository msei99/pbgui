"""Offline wire-level regressions for the production HTTP middleware wiring."""

import ast
import asyncio
import gzip
import http.client
import re
import socket
import threading
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlsplit
from uuid import uuid4

import plotly
import pytest
import uvicorn
from fastapi import FastAPI, HTTPException, Request, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, StreamingResponse
from fastapi.testclient import TestClient
from starlette.datastructures import Headers

from api import auth
from api.http_compression import ConditionalStaticFiles, HTTPCompressionMiddleware
from logging_helpers import logging_context

ROOT = Path(__file__).resolve().parents[1]
ORIGIN = "http://testserver"
REAL_PLOTLY_MODULE = plotly.__file__


async def _raw_async(app, path, encoding="gzip", method="GET", headers=None, observe=None):
    """Capture ASGI bytes without HTTP client automatic content decoding."""
    url = urlsplit(path)
    request_headers = [("host", "testserver")]
    if isinstance(encoding, list):
        request_headers.extend(("accept-encoding", value) for value in encoding)
    else:
        request_headers.append(("accept-encoding", encoding))
    request_headers.extend(headers.items() if isinstance(headers, dict) else (headers or []))
    scope = {
        "type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1",
        "method": method, "scheme": "http", "path": url.path,
        "raw_path": url.path.encode(), "query_string": url.query.encode(),
        "root_path": "", "server": ("testserver", 80), "client": ("127.0.0.1", 1234),
        "headers": [(name.encode(), value.encode()) for name, value in request_headers],
    }
    messages = []
    delivered = False
    disconnected = asyncio.Event()

    async def receive():
        """Keep the connection open until StreamingResponse cancels its listener."""
        nonlocal delivered
        if not delivered:
            delivered = True
            return {"type": "http.request", "body": b"", "more_body": False}
        await disconnected.wait()
        return {"type": "http.disconnect"}

    async def send(message):
        """Snapshot headers and expose each body chunk immediately to observers."""
        messages.append({**message, **({"headers": list(message["headers"])} if "headers" in message else {})})
        if observe:
            observe(message)

    await asyncio.wait_for(app(scope, receive, send), 10)
    start = next(message for message in messages if message["type"] == "http.response.start")
    chunks = [message.get("body", b"") for message in messages if message["type"] == "http.response.body"]
    return SimpleNamespace(status=start["status"], headers=Headers(raw=start["headers"]),
                           body=b"".join(chunks), chunks=chunks)


def _raw(app, path, encoding="gzip", **kwargs):
    """Run one isolated wire request."""
    return asyncio.run(_raw_async(app, path, encoding, **kwargs))


@pytest.fixture
def server(tmp_path, monkeypatch):
    """Execute real server wiring without importing its production startup module."""
    frontend = tmp_path / "frontend"
    frontend.mkdir()
    asset = b"const label = 'offline asset';\n" * 4000
    (frontend / "asset.js").write_bytes(asset)
    (frontend / "page.html").write_bytes(b"<html>" + b"x" * 3000 + b"</html>")
    package = tmp_path / "package_data"
    package.mkdir()
    (package / "plotly.min.js").write_bytes(asset)
    monkeypatch.setattr(plotly, "__file__", str(tmp_path / "__init__.py"))
    entries = []

    @contextmanager
    def record_context(**kwargs):
        """Record entry to actual request logging context for origin-boundary tests."""
        entries.append(kwargs)
        with logging_context(**kwargs):
            yield

    app = FastAPI()
    namespace = {
        "app": app, "CORSMiddleware": CORSMiddleware,
        "HTTPCompressionMiddleware": HTTPCompressionMiddleware,
        "ConditionalStaticFiles": ConditionalStaticFiles,
        "BrowserOriginMiddleware": auth.BrowserOriginMiddleware,
        "_cors_origins": ["https://consumer.example"], "_cors_allow_credentials": True,
        "Request": Request, "Path": Path, "uuid4": uuid4,
        "_REQUEST_ID_RE": re.compile(r"^[A-Za-z0-9._-]{1,128}$"),
        "logging_context": record_context,
        "unauthenticated_page_redirect": auth.unauthenticated_page_redirect,
        "frontend_dir": frontend,
    }
    tree = ast.parse((ROOT / "PBApiServer.py").read_text())
    selected = []
    for node in tree.body:
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call):
            function = node.value.func
            if isinstance(function, ast.Attribute) and function.attr == "add_middleware":
                selected.append(node)
        elif isinstance(node, ast.AsyncFunctionDef) and node.name in {"redirect_unauthenticated_page", "serve_plotly_js"}:
            selected.append(node)
        elif isinstance(node, ast.If) and "app.mount('/app'" in ast.unparse(node):
            selected.append(node)
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(ROOT / "PBApiServer.py"), "exec"), namespace)
    assert [middleware.cls.__name__ for middleware in app.user_middleware] == [
        "BrowserOriginMiddleware", "BaseHTTPMiddleware", "HTTPCompressionMiddleware", "CORSMiddleware",
    ]

    @app.get("/json")
    def json_payload():
        """Represent a large authenticated JSON response without runtime reads."""
        return {"payload": "offline data" * 5000}

    @app.get("/private")
    def private():
        """Exercise the real browser login redirect helper."""
        raise HTTPException(401, "Authentication required")

    @app.post("/mutation")
    def mutation():
        """Record routing, which must not happen for rejected Origins."""
        entries.append("routed")
        return {"ok": True}

    @app.websocket("/socket")
    async def socket_echo(websocket: WebSocket):
        """Exercise ASGI WebSocket passthrough without runtime authentication state."""
        await websocket.accept()
        await websocket.send_text(await websocket.receive_text())
        await websocket.close()

    return SimpleNamespace(app=app, frontend=frontend, asset=asset, entries=entries, package=package)


@pytest.mark.parametrize("path", ["/app/asset.js", "/static/plotly.min.js", "/json"])
def test_encoded_roundtrip_and_cors(server, path):
    """Real mounts, routes and middleware retain CORS and request identifiers."""
    identity = _raw(server.app, path, "identity")
    encoded = _raw(server.app, path, headers={"origin": "https://consumer.example", "x-request-id": "wire-1"})
    assert encoded.status == identity.status == 200
    assert gzip.decompress(encoded.body) == identity.body
    assert len(encoded.body) < len(identity.body) / 2
    assert encoded.headers["content-encoding"] == "gzip"
    assert encoded.headers["access-control-allow-origin"] == "https://consumer.example"
    assert encoded.headers["x-request-id"] == "wire-1"
    if "last-modified" in identity.headers:
        assert encoded.headers["last-modified"] == identity.headers["last-modified"]
    if path == "/json":
        assert "etag" not in encoded.headers and "etag" not in identity.headers
    assert {"origin", "accept-encoding"} <= {item.strip().lower() for item in encoded.headers["vary"].split(",")}
    assert "accept-encoding" in identity.headers["vary"].lower()
    if "content-length" in encoded.headers:
        assert int(encoded.headers["content-length"]) == len(encoded.body)
    assert not ("content-length" in encoded.headers and "transfer-encoding" in encoded.headers)


def test_actual_plotly_size(server, monkeypatch):
    """Read only the installed public Plotly asset and verify the expected reduction."""
    monkeypatch.setattr(plotly, "__file__", REAL_PLOTLY_MODULE)
    response = _raw(server.app, "/static/plotly.min.js")
    original = (Path(REAL_PLOTLY_MODULE).parent / "package_data" / "plotly.min.js").read_bytes()
    assert gzip.decompress(response.body) == original
    assert 1_300_000 < len(response.body) < 1_500_000
    assert "content-length" not in response.headers


@pytest.mark.parametrize("path", ["/api/coin-data/state", "/api/v8/parameter-help"])
def test_real_large_json_routes(server, monkeypatch, path):
    """Exercise the issue's real JSON routes while replacing only runtime data reads."""
    payload = {"entries": {str(index): "offline description" * 100 for index in range(200)}}
    if path == "/api/coin-data/state":
        from api import coin_data

        monkeypatch.setattr(coin_data, "_build_state", lambda **kwargs: payload)
        server.app.include_router(coin_data.router, prefix="/api/coin-data")
    else:
        import pb8_parameter_help
        from api import v8_instances

        monkeypatch.setattr(pb8_parameter_help, "get_pb8_parameter_help", lambda: payload)
        server.app.include_router(v8_instances.router, prefix="/api/v8")
    server.app.dependency_overrides[auth.require_auth] = lambda: object()
    identity = _raw(server.app, path, "identity")
    encoded = _raw(server.app, path)
    assert encoded.status == identity.status == 200
    assert encoded.headers["content-encoding"] == "gzip"
    assert gzip.decompress(encoded.body) == identity.body
    assert len(identity.body) > 137_774
    assert len(encoded.body) < len(identity.body) / 2


@pytest.mark.parametrize("path", ["/app/asset.js", "/static/plotly.min.js"])
@pytest.mark.parametrize("encoding,next_encoding", [("gzip", "identity"), ("identity", "gzip")])
def test_static_validators(server, path, encoding, next_encoding):
    """Both static paths use shared weak validators with correct precedence."""
    first = _raw(server.app, path, encoding)
    etag = first.headers["etag"]
    assert etag.startswith('W/"')
    other = _raw(server.app, path, next_encoding)
    assert other.headers["etag"] == etag
    for condition in [etag, etag.removeprefix("W/"), f'"unrelated", {etag}', "*"]:
        for method in ["GET", "HEAD"]:
            cached = _raw(server.app, path, next_encoding, method=method, headers={"if-none-match": condition})
            assert cached.status == 304
            assert cached.body == b""
            assert cached.headers["etag"] == etag
            assert "accept-encoding" in cached.headers["vary"].lower()
    mismatch = _raw(server.app, path, headers={
        "if-none-match": 'W/"old-version"', "if-modified-since": first.headers["last-modified"],
    })
    assert mismatch.status == 200
    assert gzip.decompress(mismatch.body) == server.asset
    since = _raw(server.app, path, headers={"if-modified-since": first.headers["last-modified"]})
    assert since.status == 304
    for head_encoding in [encoding, next_encoding]:
        head = _raw(server.app, path, head_encoding, method="HEAD")
        assert head.status == 200 and head.body == b""
        assert head.headers["etag"] == etag
        assert "accept-encoding" in head.headers["vary"].lower()
        if head_encoding == "gzip":
            assert "content-length" not in head.headers
        else:
            assert int(head.headers["content-length"]) == len(server.asset)
    target = server.frontend / "asset.js" if path.startswith("/app") else server.package / "plotly.min.js"
    target.write_bytes(server.asset + b"changed")
    changed = _raw(server.app, path, headers={"if-none-match": etag})
    assert changed.status == 200
    assert changed.headers["etag"] != etag


@pytest.mark.parametrize("path", ["/app/asset.js", "/static/plotly.min.js", "/etag-json"])
@pytest.mark.parametrize("method", ["GET", "HEAD"])
@pytest.mark.parametrize("encoding", ["gzip", "identity"])
def test_split_if_none_match_headers(server, path, method, encoding):
    """Split field lines and comma lists must yield the same conditional response."""
    @server.app.api_route("/etag-json", methods=["GET", "HEAD"])
    def etag_json(request: Request):
        """Exercise middleware revalidation independently of static-file checks."""
        return Response(
            b"" if request.method == "HEAD" else b'{"value":"current"}',
            media_type="application/json", headers={"ETag": '"current-json"'},
        )

    first = _raw(server.app, path, encoding)
    etag = first.headers["etag"]
    for validator in [etag, etag.removeprefix("W/")]:
        combined = _raw(server.app, path, encoding, method=method, headers={
            "if-none-match": f'"unrelated", {validator}',
        })
        assert combined.status == 304 and combined.body == b""
        for values in [('"unrelated"', validator), (validator, '"unrelated"')]:
            split = _raw(server.app, path, encoding, method=method, headers=[
                ("if-none-match", value) for value in values
            ])
            assert split.status == combined.status
            assert split.body == b""
            assert split.headers["etag"] == combined.headers["etag"] == etag
            assert "accept-encoding" in split.headers["vary"].lower()
    mismatch = _raw(server.app, path, encoding, method=method, headers=[
        ("if-none-match", '"unrelated"'), ("if-none-match", 'W/"also-unrelated"'),
        ("if-modified-since", first.headers.get("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT")),
    ])
    assert mismatch.status == 200
    if method == "HEAD":
        assert mismatch.body == b""


@pytest.mark.parametrize("encoding,expected", [
    ("", None), ("identity", None), ("br", None), ("gzip;q=0, *;q=1", None),
    ("GZip; q=0.5", "gzip"), ("*;q=0.5", "gzip"),
    ("gzip;q=0.2, identity;q=1", None), ("gzip;q=1, identity;q=0.1", "gzip"),
    (["gzip;q=0.2", "identity;q=1"], None),
    (["*;q=1", "gzip;q=0"], None),
    ("gzip ;q=0, *;q=1", None),
])
def test_negotiation(server, encoding, expected):
    """Respect explicit prohibitions and preferences through the registered chain."""
    result = _raw(server.app, "/json", encoding)
    assert result.status == 200
    assert result.headers.get("content-encoding") == expected


@pytest.mark.parametrize("size", [0, 31, 1023, 1024, 3000])
@pytest.mark.parametrize("chunked", [False, True])
def test_threshold_and_exclusive_gzip(server, size, chunked):
    """Small streamed bodies remain identity unless identity is explicitly forbidden."""
    payload = b"x" * size

    @server.app.get("/threshold")
    def threshold():
        """Return the same content as one body or a sequence of tiny chunks."""
        if chunked:
            return StreamingResponse(iter([payload[i:i + 17] for i in range(0, size, 17)]), media_type="application/json")
        return Response(payload, media_type="application/json")

    normal = _raw(server.app, "/threshold")
    assert normal.headers.get("content-encoding") == ("gzip" if size >= 1024 else None)
    assert (gzip.decompress(normal.body) if size >= 1024 else normal.body) == payload
    forced = _raw(server.app, "/threshold", "gzip;q=1, identity;q=0")
    assert forced.headers["content-encoding"] == "gzip"
    assert gzip.decompress(forced.body) == payload


@pytest.mark.parametrize("path", ["/app/asset.js", "/static/plotly.min.js", "/conditional-json"])
@pytest.mark.parametrize("method", ["GET", "HEAD"])
@pytest.mark.parametrize("condition", ["star", "etag", "modified_since"])
@pytest.mark.parametrize("encoding", ["gzip;q=0, identity;q=0", "*;q=0"])
def test_unacceptable_encoding_precedes_cache_conditions(server, path, method, condition, encoding):
    """No matching cache condition can replace a negotiation failure with 304."""
    modified = "Wed, 01 Jan 2025 00:00:00 GMT"

    @server.app.api_route("/conditional-json", methods=["GET", "HEAD"])
    def conditional_json(request: Request):
        """Model a JSON endpoint that also supplies its own bodyless cached reply."""
        metadata = {"ETag": '"json-version"', "Last-Modified": modified, "Vary": "Accept-Encoding"}
        if request.headers.get("if-modified-since") == modified:
            return Response(status_code=304, headers=metadata)
        return Response(
            b"" if request.method == "HEAD" else b'{"value":"current"}',
            media_type="application/json", headers=metadata,
        )

    first = _raw(server.app, path, "identity")
    conditions = {
        "star": {"if-none-match": "*"},
        "etag": {"if-none-match": first.headers["etag"]},
        "modified_since": {"if-modified-since": first.headers["last-modified"]},
    }[condition]
    accepted = _raw(server.app, path, "gzip", method=method, headers=conditions)
    assert accepted.status == 304 and accepted.body == b""
    result = _raw(server.app, path, encoding, method=method, headers={
        **conditions, "origin": "https://consumer.example", "x-request-id": "rejected-encoding",
    })
    assert result.status == 406 and result.body == b""
    assert result.headers["content-length"] == "0"
    assert result.headers["cache-control"] == "no-store"
    assert "etag" not in result.headers and "last-modified" not in result.headers
    assert "content-encoding" not in result.headers
    assert result.headers["access-control-allow-origin"] == "https://consumer.example"
    assert result.headers["x-request-id"] == "rejected-encoding"
    assert {"origin", "accept-encoding"} <= {value.strip().lower() for value in result.headers["vary"].split(",")}


def test_unacceptable_and_security_errors(server):
    """406 never replaces authentication errors, origin rejections or redirects."""
    result = _raw(server.app, "/json", "gzip;q=0, identity;q=0")
    assert result.status == 406 and result.body == b""
    assert result.headers["content-length"] == "0"
    assert "etag" not in result.headers
    private = _raw(server.app, "/private", "*;q=0", headers={"if-none-match": "*"})
    assert private.status == 401
    redirect = _raw(server.app, "/private", "*;q=0", headers={"accept": "text/html", "if-none-match": "*"})
    assert redirect.status == 303 and redirect.body == b""
    assert redirect.headers["location"] == "/"
    assert redirect.headers["cache-control"] == "no-store"
    assert "x-request-id" in redirect.headers
    server.entries.clear()
    denied = _raw(server.app, "/mutation", method="POST", headers={"origin": "https://evil.example"})
    assert denied.status == 403
    assert "content-encoding" not in denied.headers and "x-request-id" not in denied.headers
    assert server.entries == []


def test_html_security_headers_and_bodyless_statuses(server):
    """Framing protections survive compression and bodyless statuses pass through."""
    html = _raw(server.app, "/app/page.html")
    assert html.headers["x-frame-options"] == "SAMEORIGIN"
    assert "frame-ancestors 'self'" in html.headers["content-security-policy"]
    cached = _raw(server.app, "/app/page.html", headers={"if-none-match": html.headers["etag"]})
    assert cached.status == 304 and cached.body == b""
    assert cached.headers["x-frame-options"] == "SAMEORIGIN"
    assert "frame-ancestors 'self'" in cached.headers["content-security-policy"]
    for status in [204, 304]:
        server.app.add_api_route(f"/empty/{status}", lambda status=status: Response(status_code=status))
        empty = _raw(server.app, f"/empty/{status}")
        assert empty.status == status and empty.body == b""
        assert "content-encoding" not in empty.headers


@pytest.mark.parametrize("media_type", ["text/event-stream", "application/x-ndjson"])
def test_event_stream_first_chunk_is_immediate(server, media_type):
    """The second event waits until the first traverses the whole middleware chain."""
    async def exercise():
        """Use an event rather than timing a fully buffered HTTP client."""
        first_received = asyncio.Event()

        async def events():
            """Do not produce the remainder until the consumer sees the first event."""
            yield b"data: first\n\n"
            await asyncio.wait_for(first_received.wait(), 1)
            yield b"data: last\n\n"

        server.app.add_api_route("/events", lambda: StreamingResponse(events(), media_type=media_type))

        def observe(message):
            """A real outward body message releases the producer."""
            if b"data: first" in message.get("body", b""):
                first_received.set()

        response = await _raw_async(server.app, "/events", observe=observe)
        assert response.body == b"data: first\n\ndata: last\n\n"
        assert "content-encoding" not in response.headers

    asyncio.run(exercise())


def test_websocket_passthrough(server):
    """HTTP compression does not intercept the WebSocket scope or messages."""
    with TestClient(server.app) as client:
        with client.websocket_connect("/socket") as websocket:
            websocket.send_text("unchanged")
            assert websocket.receive_text() == "unchanged"


def test_encoded_json_is_not_encoded_twice(server):
    """Existing Content-Encoding overrides even the otherwise eligible JSON MIME."""
    payload = gzip.compress(server.asset)
    server.app.add_api_route("/encoded", lambda: Response(payload, media_type="application/json", headers={
        "Content-Encoding": "gzip", "Cache-Control": "max-age=3600",
    }))
    result = _raw(server.app, "/encoded", "gzip;q=1, identity;q=0")
    assert result.body == payload
    assert result.headers.getlist("content-encoding") == ["gzip"]
    assert result.headers["cache-control"] == "max-age=3600"


def test_static_first_chunk_arrives_before_second_read(server, monkeypatch):
    """A file cannot advance to its second read until the first gzip chunk is sent."""
    import anyio

    async def exercise():
        """Synchronize file reads with the outward ASGI send callback."""
        sent = asyncio.Event()
        read_count = 0
        original_read = anyio.AsyncFile.read

        async def read(self, size=-1):
            """Gate the second read, detecting full-file buffering as a timeout."""
            nonlocal read_count
            read_count += 1
            if read_count > 1:
                await asyncio.wait_for(sent.wait(), 1)
            return await original_read(self, size)

        def observe(message):
            """Only a body leaving the complete middleware chain releases the file."""
            if message.get("body"):
                sent.set()

        monkeypatch.setattr(anyio.AsyncFile, "read", read)
        result = await _raw_async(server.app, "/app/asset.js", observe=observe)
        assert read_count > 1
        assert gzip.decompress(result.body) == server.asset

    asyncio.run(exercise())


@pytest.mark.parametrize("suffix", [".gz", ".tgz", ".GZ", ".TGZ"])
def test_archive_path_bypasses_even_textual_mime(server, suffix):
    """Prove the path guard separately from the MIME allowlist and encoding header."""
    server.app.add_api_route("/archive" + suffix, lambda: Response(server.asset, media_type="application/json"))
    response = _raw(server.app, "/archive" + suffix + "?path=ignored")
    assert response.body == server.asset
    assert "content-encoding" not in response.headers


@pytest.mark.parametrize("suffix", [".gz", ".tgz", ".GZ", ".TGZ"])
@pytest.mark.parametrize("encoding", ["gzip", "identity"])
def test_pb8_real_archive_route(server, tmp_path, monkeypatch, suffix, encoding):
    """Exercise only the real PB8 results/{filename}?path= archive download route."""
    from api import backtest_v8

    monkeypatch.setattr(backtest_v8, "_resolve_result_dir", lambda path: tmp_path)
    server.app.dependency_overrides[auth.require_auth] = lambda: object()
    server.app.include_router(backtest_v8.router, prefix="/api/backtest-v8")
    filename = "artifact.json" + suffix
    payload = gzip.compress(server.asset)
    (tmp_path / filename).write_bytes(payload)
    response = _raw(server.app, f"/api/backtest-v8/results/{filename}?path=isolated", encoding)
    assert response.status == 200 and response.body == payload
    assert "content-encoding" not in response.headers


@pytest.mark.parametrize("version", [7, 8])
@pytest.mark.parametrize("kind,filename", [("equity", "balance_and_equity.csv.gz"), ("fills", "fills.csv.gz")])
def test_existing_gzip_csv_routes(server, tmp_path, monkeypatch, version, kind, filename):
    """Preserve encoded PB7/PB8 CSV bytes, encoding and caching on suffixless routes."""
    from api import backtest_v7, backtest_v8

    module = backtest_v7 if version == 7 else backtest_v8
    monkeypatch.setattr(module, "_resolve_result_dir", lambda path: tmp_path)
    server.app.dependency_overrides[auth.require_auth] = lambda: object()
    server.app.include_router(module.router, prefix=f"/api/backtest-v{version}")
    payload = gzip.compress(b"time,value\n" + b"1,2\n" * 4000)
    (tmp_path / filename).write_bytes(payload)
    response = _raw(server.app, f"/api/backtest-v{version}/results/{kind}?path=isolated")
    assert response.status == 200 and response.body == payload
    assert response.headers.getlist("content-encoding") == ["gzip"]
    assert response.headers["cache-control"] == "max-age=3600"


def test_uvicorn_stream_framing_and_bounded_file_reads(server, monkeypatch):
    """Use a temporary loopback server and raw HTTP for real streaming framing."""
    read_sizes = []
    original_read = __import__("anyio").AsyncFile.read

    async def observe_read(self, size=-1):
        """Record that FileResponse keeps bounded reads rather than loading the asset."""
        read_sizes.append(size)
        return await original_read(self, size)

    monkeypatch.setattr(__import__("anyio").AsyncFile, "read", observe_read)
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    runner = uvicorn.Server(uvicorn.Config(server.app, lifespan="off", log_config=None, access_log=False))
    runner.install_signal_handlers = lambda: None
    thread = threading.Thread(target=runner.run, kwargs={"sockets": [listener]})
    thread.start()
    connection = None
    try:
        import time

        deadline = time.monotonic() + 5
        while not runner.started and thread.is_alive() and time.monotonic() < deadline:
            threading.Event().wait(0.01)
        assert runner.started
        connection = http.client.HTTPConnection("127.0.0.1", listener.getsockname()[1], timeout=5)
        connection.request("GET", "/app/asset.js", headers={"Accept-Encoding": "gzip"})
        response = connection.getresponse()
        encoded = response.read()
        assert response.status == 200
        assert response.getheader("Content-Encoding") == "gzip"
        assert response.getheader("Transfer-Encoding") == "chunked"
        assert response.getheader("Content-Length") is None
        assert gzip.decompress(encoded) == server.asset
        assert read_sizes and all(0 < size <= 65536 for size in read_sizes)
    finally:
        if connection:
            connection.close()
        runner.should_exit = True
        thread.join(timeout=5)
        listener.close()
        assert not thread.is_alive()
