"""
IPC Client for communicating with the PBMaster daemon.

Provides a simple synchronous ``query()`` function that connects to the
PBMaster Unix socket, sends a JSON command, and returns the parsed response.

Used by:
    - The Streamlit UI pages (for actions like restart_service)
    - The WebSocket proxy (to fetch real-time data for push)

Example::

    from master.ipc_client import query

    # Get full status
    status = query("get_status")

    # Get logs
    logs = query("get_logs", host="manibot71", service="PBRun", lines=200)

    # Restart a service
    result = query("restart_service", host="manibot71", service="PBRun")
"""

from __future__ import annotations

import json
import socket
from pathlib import Path
from typing import Any, Optional

from pbgui_purefunc import PBGDIR

SOCKET_PATH = Path(f"{PBGDIR}/data/pbmaster.sock")

# Connection and read timeout (seconds)
TIMEOUT = 10.0


class IPCError(Exception):
    """Raised when the IPC call fails."""
    pass


def query(cmd: str, **params) -> Any:
    """
    Send a command to the PBMaster daemon and return the result.

    Parameters
    ----------
    cmd : str
        Command name (e.g. "get_status", "restart_service").
    **params
        Additional parameters for the command.

    Returns
    -------
    Any
        The ``data`` field from the daemon's response.

    Raises
    ------
    IPCError
        If the socket is not available, the daemon is not running,
        or the command returns an error.
    """
    if not SOCKET_PATH.exists():
        raise IPCError("PBMaster socket not found — is the daemon running?")

    request = {"cmd": cmd, **params}
    request_bytes = json.dumps(request).encode("utf-8") + b"\n"

    try:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(TIMEOUT)
        sock.connect(str(SOCKET_PATH))
        sock.sendall(request_bytes)
        # Half-close write side so server sees EOF
        sock.shutdown(socket.SHUT_WR)

        # Read response
        chunks = []
        while True:
            chunk = sock.recv(65536)
            if not chunk:
                break
            chunks.append(chunk)
        sock.close()

    except ConnectionRefusedError:
        raise IPCError("PBMaster daemon is not accepting connections")
    except socket.timeout:
        raise IPCError("PBMaster daemon did not respond in time")
    except OSError as e:
        raise IPCError(f"Socket error: {e}")

    raw = b"".join(chunks)
    if not raw:
        raise IPCError("Empty response from daemon")

    try:
        response = json.loads(raw.decode("utf-8").strip())
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        raise IPCError(f"Invalid response: {e}")

    if not response.get("ok"):
        raise IPCError(response.get("error", "Unknown error"))

    return response.get("data")


def is_daemon_reachable() -> bool:
    """Quick check: can we connect to the daemon socket?"""
    if not SOCKET_PATH.exists():
        return False
    try:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(2.0)
        sock.connect(str(SOCKET_PATH))
        sock.close()
        return True
    except (OSError, socket.timeout):
        return False
