"""
IPC Server for PBMaster daemon.

Runs a Unix Domain Socket server in a background thread inside the
PBMaster daemon process.  The Streamlit UI (and the WebSocket proxy)
connect to this socket to query real-time data and issue commands.

Protocol
--------
- Client sends a JSON line terminated by ``\\n``.
- Server replies with a JSON line terminated by ``\\n``.
- Connection is closed after each request/response (short-lived).

Request format::

    {"cmd": "<command>", ...params}

Response format::

    {"ok": true, "data": ...}   # success
    {"ok": false, "error": "..."}  # failure

Supported commands
------------------
    get_status      — Connection summary + stream info
    get_system      — System metrics for one or all hosts
    get_instances   — Bot instances for one or all hosts
    get_services    — Last service-check results
    get_logs        — Fetch recent log lines (one-shot)
    restart_service — Restart a service on a VPS
"""

from __future__ import annotations

import json
import os
import select
import socket
import threading
import time
import traceback
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pbgui_purefunc import PBGDIR

if TYPE_CHECKING:
    from PBMaster import PBMaster

SOCKET_PATH = Path(f"{PBGDIR}/data/pbmaster.sock")

# Maximum request size (64 KB should be plenty)
MAX_REQUEST_SIZE = 65536


class IPCServer:
    """
    Unix-socket IPC server running inside the PBMaster daemon.

    Usage::

        server = IPCServer(pbmaster_instance)
        server.start()   # spawns background thread
        ...
        server.stop()     # clean shutdown
    """

    def __init__(self, pbmaster: "PBMaster"):
        self._pbmaster = pbmaster
        self._sock: socket.socket | None = None
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        # Cache last service-check results (written by main loop)
        self._last_services: dict = {}

    # ── public ──────────────────────────────────────────────

    def start(self):
        """Start the IPC server in a background thread."""
        self._stop_event.clear()
        self._ensure_socket()
        self._thread = threading.Thread(
            target=self._serve_loop,
            name="IPCServer",
            daemon=True,
        )
        self._thread.start()

    def stop(self):
        """Shut down the server."""
        self._stop_event.set()
        if self._sock:
            try:
                self._sock.close()
            except OSError:
                pass
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3)
        _cleanup_socket()

    def update_services(self, results: dict):
        """Cache latest service-check results (called from main loop)."""
        self._last_services = results

    # ── socket setup ────────────────────────────────────────

    def _ensure_socket(self):
        _cleanup_socket()
        SOCKET_PATH.parent.mkdir(parents=True, exist_ok=True)
        self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._sock.bind(str(SOCKET_PATH))
        self._sock.listen(5)
        self._sock.settimeout(1.0)  # so we can check stop_event
        # Make socket accessible
        os.chmod(str(SOCKET_PATH), 0o660)

    # ── serve loop ──────────────────────────────────────────

    def _serve_loop(self):
        """Accept connections and dispatch commands."""
        while not self._stop_event.is_set():
            try:
                conn, _ = self._sock.accept()
            except socket.timeout:
                continue
            except OSError:
                if self._stop_event.is_set():
                    break
                continue

            # Handle each connection in-line (fast enough for IPC)
            try:
                conn.settimeout(5.0)
                self._handle_connection(conn)
            except Exception:
                pass
            finally:
                try:
                    conn.close()
                except OSError:
                    pass

    def _handle_connection(self, conn: socket.socket):
        """Read one JSON request, dispatch, send response."""
        raw = b""
        while True:
            chunk = conn.recv(4096)
            if not chunk:
                break
            raw += chunk
            if b"\n" in raw or len(raw) >= MAX_REQUEST_SIZE:
                break

        if not raw:
            return

        try:
            request = json.loads(raw.decode("utf-8").strip())
        except (json.JSONDecodeError, UnicodeDecodeError):
            self._send_response(conn, {"ok": False, "error": "invalid JSON"})
            return

        cmd = request.get("cmd", "")
        try:
            result = self._dispatch(cmd, request)
            self._send_response(conn, {"ok": True, "data": result})
        except Exception as e:
            self._send_response(conn, {"ok": False, "error": str(e)})

    def _send_response(self, conn: socket.socket, response: dict):
        """Send JSON response terminated by newline."""
        try:
            data = json.dumps(response, default=str).encode("utf-8") + b"\n"
            conn.sendall(data)
        except (OSError, BrokenPipeError):
            pass

    # ── command dispatch ────────────────────────────────────

    def _dispatch(self, cmd: str, request: dict) -> Any:
        handler = {
            "get_status": self._cmd_get_status,
            "get_system": self._cmd_get_system,
            "get_instances": self._cmd_get_instances,
            "get_services": self._cmd_get_services,
            "get_logs": self._cmd_get_logs,
            "restart_service": self._cmd_restart_service,
        }.get(cmd)
        if not handler:
            raise ValueError(f"unknown command: {cmd}")
        return handler(request)

    # ── command handlers ────────────────────────────────────

    def _cmd_get_status(self, _request: dict) -> dict:
        """Return connections + stream info + system metrics (all hosts)."""
        pb = self._pbmaster
        conn_summary = pb.pool.get_status_summary() if pb.pool else {}
        stream_info = pb.realtime.get_stream_info() if pb.realtime else {}

        system = {}
        if pb.realtime:
            for h in pb.pool.hostnames():
                m = pb.realtime.get_system(h)
                if m:
                    system[h] = {
                        "cpu": m.cpu,
                        "mem_total": m.mem_total,
                        "mem_available": m.mem_available,
                        "mem_percent": m.mem_percent,
                        "mem_used": m.mem_used,
                        "disk_total": m.disk_total,
                        "disk_used": m.disk_used,
                        "disk_free": m.disk_free,
                        "disk_percent": m.disk_percent,
                        "swap_total": m.swap_total,
                        "swap_used": m.swap_used,
                        "swap_free": m.swap_free,
                        "swap_percent": m.swap_percent,
                    }

        instances = {}
        if pb.realtime:
            instances = pb.realtime.get_all_instances() or {}

        return {
            "connections": conn_summary,
            "system": system,
            "instances": instances,
            "streams": stream_info,
            "services": self._last_services,
        }

    def _cmd_get_system(self, request: dict) -> dict:
        """Return system metrics for a single host or all hosts."""
        host = request.get("host")
        pb = self._pbmaster
        if not pb.realtime:
            return {}
        if host:
            m = pb.realtime.get_system(host)
            if not m:
                return {}
            return {
                "cpu": m.cpu, "mem_total": m.mem_total,
                "mem_available": m.mem_available, "mem_percent": m.mem_percent,
                "mem_used": m.mem_used,
                "disk_total": m.disk_total, "disk_used": m.disk_used,
                "disk_free": m.disk_free, "disk_percent": m.disk_percent,
                "swap_total": m.swap_total, "swap_used": m.swap_used,
                "swap_free": m.swap_free, "swap_percent": m.swap_percent,
            }
        # All hosts
        result = {}
        for h in pb.pool.hostnames():
            m = pb.realtime.get_system(h)
            if m:
                result[h] = {
                    "cpu": m.cpu, "mem_total": m.mem_total,
                    "mem_available": m.mem_available, "mem_percent": m.mem_percent,
                    "mem_used": m.mem_used,
                    "disk_total": m.disk_total, "disk_used": m.disk_used,
                    "disk_free": m.disk_free, "disk_percent": m.disk_percent,
                    "swap_total": m.swap_total, "swap_used": m.swap_used,
                    "swap_free": m.swap_free, "swap_percent": m.swap_percent,
                }
        return result

    def _cmd_get_instances(self, request: dict) -> dict:
        """Return bot instances for one host or all hosts."""
        host = request.get("host")
        pb = self._pbmaster
        if not pb.realtime:
            return {}
        if host:
            return {host: pb.realtime.get_instances(host)}
        return pb.realtime.get_all_instances() or {}

    def _cmd_get_services(self, _request: dict) -> dict:
        """Return cached service-check results."""
        return self._last_services

    def _cmd_get_logs(self, request: dict) -> dict:
        """Fetch recent lines from a log file on a VPS."""
        host = request.get("host", "")
        service = request.get("service", "")
        lines = request.get("lines", 200)
        if not host or not service:
            raise ValueError("host and service required")
        pb = self._pbmaster
        if not pb.streamer:
            raise ValueError("log streamer not available")
        if service.startswith("Bot:"):
            bot_name = service[4:].strip()
            content = pb.streamer.get_bot_log(host, bot_name, lines=lines)
        else:
            content = pb.streamer.get_recent_logs(host, service, lines=lines)
        return {"lines": content or ""}

    def _cmd_restart_service(self, request: dict) -> dict:
        """Restart a service on a VPS."""
        host = request.get("host", "")
        service = request.get("service", "")
        if not host or not service:
            raise ValueError("host and service required")
        pb = self._pbmaster
        if not pb.monitor:
            raise ValueError("service monitor not available")
        success = pb.monitor.restart_service(host, service)
        return {"success": success}


def _cleanup_socket():
    """Remove stale socket file."""
    try:
        if SOCKET_PATH.exists():
            SOCKET_PATH.unlink()
    except OSError:
        pass
