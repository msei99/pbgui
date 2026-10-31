"""Bounded HTTP gzip encoding and shared semantic static-file validators."""

import re
import zlib
from email.utils import parsedate

from starlette.datastructures import Headers, MutableHeaders
from starlette.exceptions import HTTPException
from starlette.responses import FileResponse
from starlette.staticfiles import NotModifiedResponse, StaticFiles

SERVICE = "HTTPCompression"
_CONTENT_TYPES = frozenset({
    "text/html", "application/xhtml+xml", "text/css", "text/javascript",
    "application/javascript", "application/json",
})


def _compressible(headers):
    """Limit encoding to textual product assets and JSON, never encoded bodies."""
    media_type = headers.get("content-type", "").split(";", 1)[0].strip().lower()
    return "content-encoding" not in headers and media_type in _CONTENT_TYPES


def _archive(scope):
    """Protect archive bytes even when FileResponse guesses a textual MIME type."""
    return scope.get("path", "").lower().endswith((".gz", ".tgz"))


def _vary(headers):
    """Merge encoding negotiation without replacing CORS cache dimensions."""
    values = {value.strip().lower() for value in headers.get("vary", "").split(",")}
    if "*" not in values and "accept-encoding" not in values:
        headers.add_vary_header("Accept-Encoding")


def _semantic_headers(headers):
    """Use the same weak resource validator across identity and gzip variants."""
    _vary(headers)
    if "etag" in headers and not headers["etag"].startswith("W/"):
        headers["etag"] = "W/" + headers["etag"]


def _etag_matches(value, etag):
    """Compare entity-tag lists weakly, retaining commas inside opaque tags."""
    tags = re.findall(r'(?:W/)?"[^"\r\n]*"|\*', value)
    return "*" in tags or any(tag.removeprefix("W/") == etag.removeprefix("W/") for tag in tags)


def _encoding_quality(value):
    """Return gzip/identity qualities with explicit exclusions overriding '*'."""
    qualities = {}
    for entry in value.split(","):
        coding, *parameters = entry.strip().lower().split(";")
        coding = coding.strip()
        if not coding:
            continue
        quality = 1.0
        for parameter in parameters:
            name, _, raw = parameter.strip().partition("=")
            if name == "q":
                try:
                    quality = float(raw)
                except ValueError:
                    quality = 0.0
                if not 0 <= quality <= 1:
                    quality = 0.0
        # Conservatively retain an explicit prohibition in duplicate entries.
        qualities[coding] = min(quality, qualities.get(coding, 1.0))
    gzip_quality = qualities.get("gzip", qualities.get("*", 0.0))
    identity_quality = qualities.get("identity", 0.0 if qualities.get("*") == 0 else 1.0)
    prefer_gzip = gzip_quality > 0 and ("identity" not in qualities or gzip_quality >= identity_quality)
    return gzip_quality, identity_quality, prefer_gzip


class ConditionalStaticFiles(StaticFiles):
    """Stateless static serving with correct conditional precedence and weak ETags."""

    def __init__(self, *args, allowed_html=None, **kwargs):
        """Optionally restrict HTML to exact relative paths at an asset mount."""
        self._allowed_html = None if allowed_html is None else frozenset(allowed_html)
        super().__init__(*args, **kwargs)

    async def get_response(self, path, scope):
        """Reject raw templates before file delivery or conditional cache handling."""
        if (self._allowed_html is not None and path.lower().endswith(".html")
                and path not in self._allowed_html):
            raise HTTPException(status_code=404)
        return await super().get_response(path, scope)

    def is_not_modified(self, response_headers, request_headers):
        """If-None-Match takes precedence, including nonmatching tags and '*'."""
        if "if-none-match" in request_headers:
            conditional = ", ".join(request_headers.getlist("if-none-match"))
            return _etag_matches(conditional, response_headers.get("etag", ""))
        since = parsedate(request_headers.get("if-modified-since", ""))
        modified = parsedate(response_headers.get("last-modified", ""))
        return since is not None and modified is not None and since >= modified

    def file_response(self, full_path, stat_result, scope, status_code=200):
        """Set representation metadata before deciding whether to return 304."""
        response = FileResponse(full_path, stat_result=stat_result, status_code=status_code)
        if not _archive(scope) and _compressible(response.headers):
            _semantic_headers(response.headers)
        if self.is_not_modified(response.headers, Headers(scope=scope)):
            return NotModifiedResponse(response.headers)
        return response


class HTTPCompressionMiddleware:
    """Negotiate gzip without buffering files or delaying event-stream messages."""

    def __init__(self, app, minimum_size=1024, compresslevel=6):
        """Own no background resources; compression state belongs to one request."""
        self.app = app
        self.minimum_size = minimum_size
        self.compresslevel = compresslevel

    async def __call__(self, scope, receive, send):
        """Inspect response headers once, then stream or buffer below the threshold."""
        if scope["type"] != "http" or _archive(scope):
            await self.app(scope, receive, send)
            return

        request_headers = Headers(scope=scope)
        gzip_quality, identity_quality, use_gzip = _encoding_quality(
            ",".join(request_headers.getlist("accept-encoding"))
        )
        start = None
        mode = "pass"
        pending = bytearray()
        compressor = None

        async def send_encoded(body, more_body, buffered=False):
            """Emit gzip bytes incrementally, retaining length only for a final body."""
            nonlocal compressor
            if compressor is None:
                compressor = zlib.compressobj(self.compresslevel, zlib.DEFLATED, 31)
            encoded = compressor.compress(body)
            encoded += compressor.flush(zlib.Z_SYNC_FLUSH if more_body else zlib.Z_FINISH)
            if buffered:
                headers = MutableHeaders(scope=start)
                headers["Content-Encoding"] = "gzip"
                del headers["Transfer-Encoding"]
                if more_body:
                    del headers["Content-Length"]
                else:
                    headers["Content-Length"] = str(len(encoded))
                await send(start)
            await send({"type": "http.response.body", "body": encoded, "more_body": more_body})

        async def send_response(message):
            """Preserve bypasses, validators and framing before forwarding bytes."""
            nonlocal start, mode
            if message["type"] == "http.response.start":
                start = message
                headers = MutableHeaders(scope=start)
                status = message["status"]
                eligible = _compressible(headers)
                if status == 304:
                    # Static serving supplies Vary even though 304 omits Content-Type.
                    varied = {value.strip().lower() for value in headers.get("vary", "").split(",")}
                    if "content-encoding" in headers or not (eligible or "accept-encoding" in varied):
                        await send(start)
                        return
                elif not eligible or not 200 <= status < 300 or status in {204, 205, 206}:
                    await send(start)
                    return
                _semantic_headers(headers)
                # Negotiation failure takes precedence over all cache preconditions,
                # including a 304 already produced by the static-file application.
                if gzip_quality == 0 and identity_quality == 0:
                    start["status"] = 406
                    for name in ("Content-Type", "ETag", "Last-Modified", "Transfer-Encoding"):
                        del headers[name]
                    headers["Content-Length"] = "0"
                    headers["Cache-Control"] = "no-store"
                    mode = "discard"
                    await send(start)
                    return
                if status == 304:
                    await send(start)
                    return
                conditional_values = request_headers.getlist("if-none-match")
                conditional = ", ".join(conditional_values) if conditional_values else None
                if scope["method"] in {"GET", "HEAD"} and conditional is not None and (
                    conditional.strip() == "*" or (
                        "etag" in headers and _etag_matches(conditional, headers["etag"])
                    )
                ):
                    start["status"] = 304
                    for name in ("Content-Length", "Content-Type", "Content-Encoding"):
                        del headers[name]
                    mode = "discard"
                    await send(start)
                    return
                if scope["method"] == "HEAD":
                    if use_gzip and (identity_quality == 0 or int(headers.get("content-length", "0")) >= self.minimum_size):
                        headers["Content-Encoding"] = "gzip"
                        del headers["Content-Length"]
                        del headers["Transfer-Encoding"]
                    await send(start)
                    return
                if use_gzip:
                    mode = "buffer"
                else:
                    await send(start)
            elif message["type"] == "http.response.body":
                body = message.get("body", b"")
                more_body = message.get("more_body", False)
                if mode == "discard":
                    if not more_body:
                        await send({"type": "http.response.body", "body": b"", "more_body": False})
                elif mode == "buffer":
                    if identity_quality == 0 or len(pending) + len(body) >= self.minimum_size:
                        # Pending is always below the threshold, regardless of file size.
                        body = bytes(pending) + body
                        pending.clear()
                        mode = "gzip"
                        await send_encoded(body, more_body, buffered=True)
                    else:
                        pending.extend(body)
                        if not more_body:
                            await send(start)
                            await send({"type": "http.response.body", "body": bytes(pending), "more_body": False})
                elif mode == "gzip":
                    await send_encoded(body, more_body)
                else:
                    await send(message)
            else:
                await send(message)

        await self.app(scope, receive, send_response)
