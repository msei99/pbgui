"""Safe inline JSON and same-origin URLs for server-rendered pages."""

import json
import re
from urllib.parse import quote

from fastapi import HTTPException, Request

from logging_helpers import human_log as _log

SERVICE = "Auth"


def script_json(value: object) -> str:
    """Serialize JSON without HTML delimiters or replaceable template markers."""
    return (json.dumps(value, allow_nan=False)
            .replace("<", "\\u003c").replace(">", "\\u003e")
            .replace("&", "\\u0026").replace("%", "\\u0025"))


def render_page_urls(request: Request, html: str, api_path: str) -> str:
    """Bind page URLs and assets to the trusted ASGI mount, not query inputs."""
    root_path = request.scope.get("root_path", "")
    if not isinstance(root_path, str) or (root_path and (
        not root_path.startswith("/") or root_path.startswith("//")
        or "\\" in root_path or any(ord(char) < 32 or ord(char) == 127 for char in root_path)
        or any(part in {".", ".."} for part in root_path.split("/"))
    )):
        _log(SERVICE, "Invalid page ASGI root_path", level="ERROR")
        raise HTTPException(status_code=500, detail="Invalid page mount path")
    if (not isinstance(api_path, str) or not api_path.startswith("/") or api_path.startswith("//")
            or any(char in api_path for char in "\\?#%")
            or any(ord(char) <= 32 or ord(char) >= 127 for char in api_path)
            or any(part in {".", ".."} for part in api_path.split("/"))):
        _log(SERVICE, "Invalid page API path", level="ERROR")
        raise HTTPException(status_code=500, detail="Invalid page API path")
    prefix = quote(root_path.rstrip("/"), safe="/")
    html = html.replace('"%%API_BASE%%"', script_json(prefix + api_path))
    html = html.replace('"%%BASE_PREFIX%%"', script_json(prefix))
    if '"%%WS_BASE%%"' in html:
        from api.auth import _request_origin

        origin = _request_origin(request)
        ws_origin = ("wss://" if origin.startswith("https://") else "ws://") + origin.split("://", 1)[1]
        html = html.replace('"%%WS_BASE%%"', script_json(ws_origin + prefix))
    return re.sub(
        r"(\b(?:src|href)\s*=\s*)([\"'])/app/",
        lambda match: match[1] + match[2] + prefix + "/app/",
        html,
        flags=re.IGNORECASE,
    )
