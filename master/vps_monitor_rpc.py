"""Shared protocol and Unix-socket helpers for the VPS monitor RPC service."""

from __future__ import annotations

import asyncio
import json
import os
import socket
import stat
import struct
import sys
from pathlib import Path
from typing import Any


SERVICE = "VPSMonitor"
PROTOCOL_VERSION = 1
MAX_REQUEST_FRAME_BYTES = 1024 * 1024
MAX_RESPONSE_FRAME_BYTES = 16 * 1024 * 1024
MAX_FRAME_BYTES = MAX_REQUEST_FRAME_BYTES
MAX_ERROR_CHARS = 240
RPC_REQUEST_TIMEOUT_SECONDS = 5.0
MAX_RPC_CLIENTS = 128


class RPCError(RuntimeError):
    """A bounded, client-safe protocol error."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(str(message)[:MAX_ERROR_CHARS])
        self.code = str(code)[:64]


def default_socket_path() -> Path:
    """Return the owner-runtime VPS monitor socket path."""
    runtime = os.environ.get("XDG_RUNTIME_DIR", "").strip()
    root = Path(runtime) if runtime else Path("/run/user") / str(os.getuid())
    if not root.is_absolute():
        raise RuntimeError("XDG_RUNTIME_DIR must be an absolute path")
    return root / "pbgui" / "vps-monitor.sock"


def _check_owned_directory(path: Path) -> None:
    """Require an existing directory to be real and owned by this UID."""
    info = path.lstat()
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise RuntimeError(f"Runtime path is not a directory: {path}")
    if hasattr(os, "getuid") and info.st_uid != os.getuid():
        raise RuntimeError(f"Runtime path is not owned by the current user: {path}")


def prepare_socket_path(socket_path: Path) -> Path:
    """Secure the socket parent and remove only a stale owned socket."""
    path = Path(socket_path)
    if not path.is_absolute():
        raise RuntimeError("VPS monitor socket path must be absolute")
    if len(os.fsencode(path)) >= 104:
        raise RuntimeError("VPS monitor socket path is too long")

    parent = path.parent
    if parent.exists() or parent.is_symlink():
        _check_owned_directory(parent)
    else:
        parent.mkdir(parents=True, mode=0o700)
        _check_owned_directory(parent)
    os.chmod(parent, 0o700)

    try:
        info = path.lstat()
    except FileNotFoundError:
        return path
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISSOCK(info.st_mode):
        raise RuntimeError(f"Refusing non-socket collision at {path}")
    if hasattr(os, "getuid") and info.st_uid != os.getuid():
        raise RuntimeError(f"Refusing socket not owned by the current user: {path}")

    probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        probe.settimeout(0.1)
        if probe.connect_ex(str(path)) == 0:
            raise RuntimeError(f"VPS monitor socket is already active: {path}")
    finally:
        probe.close()
    path.unlink()
    return path


def verify_socket_permissions(socket_path: Path) -> None:
    """Require an owner-only, owner-held Unix socket and parent directory."""
    path = Path(socket_path)
    try:
        parent_info = path.parent.lstat()
        socket_info = path.lstat()
    except FileNotFoundError as exc:
        raise RuntimeError("VPS monitor daemon is unavailable") from exc
    if stat.S_ISLNK(parent_info.st_mode) or not stat.S_ISDIR(parent_info.st_mode):
        raise RuntimeError("VPS monitor runtime directory is invalid")
    if stat.S_IMODE(parent_info.st_mode) & 0o077:
        raise RuntimeError("VPS monitor runtime directory is not owner-only")
    if stat.S_ISLNK(socket_info.st_mode) or not stat.S_ISSOCK(socket_info.st_mode):
        raise RuntimeError("VPS monitor endpoint is not a Unix socket")
    if stat.S_IMODE(socket_info.st_mode) & 0o177:
        raise RuntimeError("VPS monitor socket is not owner-only")
    if hasattr(os, "getuid") and (
        parent_info.st_uid != os.getuid() or socket_info.st_uid != os.getuid()
    ):
        raise RuntimeError("VPS monitor endpoint has the wrong owner")


def verify_peer_uid(writer: asyncio.StreamWriter, expected_uid: int | None = None) -> None:
    """Require Linux Unix-socket peers to have the daemon owner's UID."""
    if not sys.platform.startswith("linux"):
        return
    peer_socket = writer.get_extra_info("socket")
    if peer_socket is None or not hasattr(socket, "SO_PEERCRED"):
        raise RPCError("peer_credentials", "Peer credentials are unavailable")
    try:
        credentials = peer_socket.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i"))
        _pid, uid, _gid = struct.unpack("3i", credentials)
    except (OSError, struct.error) as exc:
        raise RPCError("peer_credentials", "Peer credentials could not be verified") from exc
    owner_uid = os.getuid() if expected_uid is None else int(expected_uid)
    if uid != owner_uid:
        raise RPCError("peer_credentials", "Peer user is not authorized")


async def read_frame(reader: asyncio.StreamReader) -> dict[str, Any]:
    """Read one bounded newline-delimited JSON object."""
    try:
        raw = await reader.readline()
    except (ValueError, asyncio.LimitOverrunError) as exc:
        raise RPCError("frame_too_large", "RPC frame exceeds the size limit") from exc
    if not raw:
        raise RPCError("empty_frame", "RPC request is empty")
    if len(raw) > MAX_FRAME_BYTES or not raw.endswith(b"\n"):
        raise RPCError("frame_too_large", "RPC frame exceeds the size limit")
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RPCError("invalid_json", "RPC request is not valid JSON") from exc
    if not isinstance(payload, dict):
        raise RPCError("invalid_request", "RPC request must be an object")
    return payload


def encode_frame(payload: dict[str, Any], *, maximum: int = MAX_RESPONSE_FRAME_BYTES) -> bytes:
    """Encode one bounded compact NDJSON frame."""
    try:
        encoded = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8") + b"\n"
    except (TypeError, ValueError) as exc:
        raise RPCError("serialization_error", "RPC response could not be serialized") from exc
    if len(encoded) > maximum:
        raise RPCError("frame_too_large", "RPC response exceeds the size limit")
    return encoded


def validate_request(payload: dict[str, Any]) -> tuple[str | int, str, dict[str, Any]]:
    """Validate and unpack a strict versioned request envelope."""
    if set(payload) - {"version", "id", "method", "params"}:
        raise RPCError("invalid_request", "RPC request contains unsupported fields")
    if payload.get("version") != PROTOCOL_VERSION:
        raise RPCError("protocol_version", "Unsupported RPC protocol version")
    request_id = payload.get("id")
    if isinstance(request_id, bool) or not isinstance(request_id, (str, int)):
        raise RPCError("invalid_request", "RPC request id must be a string or integer")
    if isinstance(request_id, str) and (not request_id or len(request_id) > 128):
        raise RPCError("invalid_request", "RPC request id is invalid")
    method = payload.get("method")
    if not isinstance(method, str) or not method or len(method) > 128:
        raise RPCError("invalid_request", "RPC method is invalid")
    params = payload.get("params", {})
    if not isinstance(params, dict):
        raise RPCError("invalid_params", "RPC params must be an object")
    return request_id, method, params


def error_response(request_id: str | int | None, error: Exception) -> dict[str, Any]:
    """Build a bounded response without exception details or sensitive input."""
    if isinstance(error, RPCError):
        code = error.code
        message = str(error)[:MAX_ERROR_CHARS]
    elif isinstance(error, (TypeError, ValueError)):
        code = "invalid_params"
        message = "RPC parameters are invalid"
    else:
        code = "request_failed"
        message = "RPC request failed"
    return {"id": request_id, "ok": False, "error": {"code": code, "message": message}}


def require_string(params: dict[str, Any], key: str, *, optional: bool = False) -> str:
    """Return a bounded string parameter without coercing other types."""
    value = params.get(key)
    if optional and value is None:
        return ""
    if not isinstance(value, str) or (not optional and not value.strip()) or len(value) > 4096:
        raise RPCError("invalid_params", f"{key} must be a string")
    return value.strip()


def require_bool(params: dict[str, Any], key: str, *, default: bool | None = None) -> bool:
    """Return a strict boolean parameter."""
    value = params.get(key, default)
    if not isinstance(value, bool):
        raise RPCError("invalid_params", f"{key} must be a boolean")
    return value


def require_int(
    params: dict[str, Any], key: str, *, default: int | None = None, minimum: int = 0, maximum: int
) -> int:
    """Return a strict bounded integer parameter."""
    value = params.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum or value > maximum:
        raise RPCError("invalid_params", f"{key} must be a bounded integer")
    return value
