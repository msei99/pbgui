"""
WebSocket Server for PBMaster daemon.

Runs an asyncio WebSocket server in a daemon thread inside the PBMaster
process.  Browser clients (Custom Component) connect directly and receive
real-time push updates — no Streamlit reruns, no polling.

Architecture
------------
    PBMaster Daemon (same process)
        ├── Main loop (threading, 15s)
        ├── RealtimeCollector (SSH streams, 1s metrics)
        └── WSServer (asyncio, this module)
                ├── _push_loop: reads in-memory data every 2s, pushes to clients
                └── _handler: receives commands, sends responses

Protocol (JSON over WebSocket)
------------------------------
Server → Client (push):
    {"type": "state",    "data": { connections, system, instances, streams, services }}
    {"type": "logs",     "host": "x", "lines": ["...", ...]}
    {"type": "result",   "cmd": "restart_service", "host": "x", "service": "y", "success": true}

Client → Server (commands):
    {"cmd": "restart_service", "host": "x", "service": "y"}
    {"cmd": "get_logs",        "host": "x", "service": "y", "lines": 200}
    {"cmd": "get_log_info",    "host": "x", "service": "y"}
    {"cmd": "subscribe_logs",  "host": "x", "service": "y"}
    {"cmd": "unsubscribe_logs"}

Binding
-------
Default: 127.0.0.1:8765 (configurable via pbgui.ini: pbmaster/ws_port).
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
import traceback
from dataclasses import asdict
from typing import TYPE_CHECKING, Any, Optional

from pbgui_purefunc import load_ini, save_ini
from logging_helpers import human_log as _log

if TYPE_CHECKING:
    from PBMaster import PBMaster

SERVICE = "PBMaster"

DEFAULT_WS_HOST = "127.0.0.1"
DEFAULT_WS_PORT = 8765

# How often to push state to all connected clients (seconds)
PUSH_INTERVAL = 1.0

# How often to push log lines to subscribed clients (seconds)
LOG_PUSH_INTERVAL = 0.15


class WSServer:
    """
    WebSocket server embedded in the PBMaster daemon process.

    Usage::

        server = WSServer(pbmaster_instance)
        server.start()   # spawns background thread with asyncio loop
        ...
        server.stop()     # clean shutdown
    """

    def __init__(self, pbmaster: "PBMaster"):
        self._pbmaster = pbmaster
        self._thread: Optional[threading.Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._stop_event = asyncio.Event()
        self._clients: set = set()  # set of websocket connections
        self._host = DEFAULT_WS_HOST
        self._port = self._load_port()

        # Per-client log subscriptions: ws -> stream_id
        self._log_subs: dict = {}
        # Per-client session ids for log dedup: ws -> sid
        self._log_sids: dict = {}

        # Cached service results (set by PBMaster main loop)
        self._last_services: dict = {}

    # ── Configuration ───────────────────────────────────────

    @staticmethod
    def _load_port() -> int:
        val = load_ini("pbmaster", "ws_port")
        if val and val.isdigit():
            port = int(val)
            if 1024 <= port <= 65535:
                return port
        return DEFAULT_WS_PORT

    @property
    def port(self) -> int:
        return self._port

    # ── Public API (called from PBMaster) ───────────────────

    def start(self):
        """Start the WebSocket server in a background thread."""
        self._thread = threading.Thread(
            target=self._run_loop,
            name="WSServer",
            daemon=True,
        )
        self._thread.start()
        _log(SERVICE, f"[ws] WebSocket server starting on "
             f"{self._host}:{self._port}")

    def stop(self):
        """Shut down the server."""
        if self._loop and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(self._stop_event.set)
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        _log(SERVICE, "[ws] WebSocket server stopped")

    def update_services(self, results: dict):
        """Cache latest service-check results (called from main loop)."""
        self._last_services = results

    @property
    def client_count(self) -> int:
        return len(self._clients)

    # ── Background thread entry ─────────────────────────────

    def _run_loop(self):
        """Entry point for the background thread — runs the asyncio loop."""
        try:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            self._stop_event = asyncio.Event()
            self._loop.run_until_complete(self._serve())
        except Exception as e:
            _log(SERVICE, f"[ws] Server thread error: {e}", level="ERROR")
            traceback.print_exc()
        finally:
            if self._loop and not self._loop.is_closed():
                self._loop.close()

    async def _serve(self):
        """Start the WebSocket server and push loop."""
        import websockets
        import websockets.asyncio.server

        try:
            async with websockets.asyncio.server.serve(
                self._handler,
                self._host,
                self._port,
                ping_interval=20,
                ping_timeout=20,
                max_size=2**20,  # 1 MB max message
            ) as server:
                _log(SERVICE, f"[ws] Listening on ws://{self._host}:{self._port}")
                push_task = asyncio.create_task(self._push_loop())
                log_task = asyncio.create_task(self._log_push_loop())

                # Wait until stop is requested
                await self._stop_event.wait()

                push_task.cancel()
                log_task.cancel()
                server.close()
                await server.wait_closed()

        except OSError as e:
            _log(SERVICE, f"[ws] Cannot bind to {self._host}:{self._port}: {e}",
                 level="ERROR")

    # ── Client handler ──────────────────────────────────────

    async def _handler(self, websocket):
        """Handle a single WebSocket client connection."""
        self._clients.add(websocket)
        remote = websocket.remote_address
        _log(SERVICE, f"[ws] Client connected: {remote}")

        try:
            # Send initial full state immediately
            state = self._get_full_state()
            await websocket.send(json.dumps({
                "type": "state",
                "data": state,
            }, default=str))

            # Process incoming commands
            async for message in websocket:
                try:
                    request = json.loads(message)
                    await self._handle_command(websocket, request)
                except json.JSONDecodeError:
                    await websocket.send(json.dumps({
                        "type": "error",
                        "error": "Invalid JSON",
                    }))
                except Exception as e:
                    await websocket.send(json.dumps({
                        "type": "error",
                        "error": str(e),
                    }))

        except Exception:
            pass  # Client disconnected
        finally:
            self._clients.discard(websocket)
            # Clean up log subscription
            stream_id = self._log_subs.pop(id(websocket), None)
            self._log_sids.pop(id(websocket), None)
            if stream_id and self._pbmaster.streamer:
                self._pbmaster.streamer.stop_stream(stream_id)
            _log(SERVICE, f"[ws] Client disconnected: {remote}")

    # ── Command handling ────────────────────────────────────

    async def _handle_command(self, websocket, request: dict):
        """Dispatch a client command."""
        cmd = request.get("cmd", "")
        handler = {
            "restart_service": self._cmd_restart_service,
            "get_logs": self._cmd_get_logs,
            "get_log_info": self._cmd_get_log_info,
            "subscribe_logs": self._cmd_subscribe_logs,
            "unsubscribe_logs": self._cmd_unsubscribe_logs,
            "kill_instance": self._cmd_kill_instance,
            "set_setting": self._cmd_set_setting,
        }.get(cmd)

        if not handler:
            await websocket.send(json.dumps({
                "type": "error",
                "error": f"Unknown command: {cmd}",
            }))
            return

        await handler(websocket, request)

    async def _cmd_restart_service(self, websocket, request: dict):
        """Restart a service on a VPS."""
        host = request.get("host", "")
        service = request.get("service", "")
        if not host or not service:
            await websocket.send(json.dumps({
                "type": "error", "error": "host and service required",
            }))
            return

        pb = self._pbmaster
        if not pb.monitor:
            await websocket.send(json.dumps({
                "type": "error", "error": "Service monitor not available",
            }))
            return

        # Run blocking SSH command in thread pool
        loop = asyncio.get_event_loop()
        success = await loop.run_in_executor(
            None, pb.monitor.restart_service, host, service
        )

        await websocket.send(json.dumps({
            "type": "result",
            "cmd": "restart_service",
            "host": host,
            "service": service,
            "success": success,
        }))

    async def _cmd_get_logs(self, websocket, request: dict):
        """Fetch recent log lines (one-shot)."""
        host = request.get("host", "")
        service = request.get("service", "")
        lines = request.get("lines", 200)
        sid = request.get("sid")
        if not host or not service:
            await websocket.send(json.dumps({
                "type": "error", "error": "host and service required",
            }))
            return

        pb = self._pbmaster
        if not pb.streamer:
            await websocket.send(json.dumps({
                "type": "error", "error": "Log streamer not available",
            }))
            return

        loop = asyncio.get_event_loop()
        if service.startswith("Bot:"):
            # Format: "Bot:name" or "Bot:name:version"
            parts = service[4:].strip().split(":")
            bot_name = parts[0]
            pb_version = parts[1] if len(parts) > 1 else None
            content = await loop.run_in_executor(
                None, pb.streamer.get_bot_log, host, bot_name, lines, pb_version
            )
        else:
            content = await loop.run_in_executor(
                None, pb.streamer.get_recent_logs, host, service, lines
            )

        resp = {
            "type": "logs",
            "host": host,
            "service": service,
            "lines": (content or "").splitlines(),
        }
        if sid is not None:
            resp["sid"] = sid
        await websocket.send(json.dumps(resp))

    async def _cmd_get_log_info(self, websocket, request: dict):
        """Get log file info (size)."""
        host = request.get("host", "")
        service = request.get("service", "")
        if not host or not service:
            await websocket.send(json.dumps({
                "type": "error", "error": "host and service required",
            }))
            return

        pb = self._pbmaster
        if not pb.streamer:
            await websocket.send(json.dumps({
                "type": "error", "error": "Log streamer not available",
            }))
            return

        loop = asyncio.get_event_loop()
        info = await loop.run_in_executor(
            None, pb.streamer.get_log_info, host, service
        )

        await websocket.send(json.dumps({
            "type": "log_info",
            "host": host,
            "service": service,
            "size": info["size"] if info else None,
        }))

    async def _cmd_subscribe_logs(self, websocket, request: dict):
        """Start live log streaming for this client."""
        host = request.get("host", "")
        service = request.get("service", "")
        sid = request.get("sid")
        if not host or not service:
            await websocket.send(json.dumps({
                "type": "error", "error": "host and service required",
            }))
            return

        pb = self._pbmaster
        if not pb.streamer:
            await websocket.send(json.dumps({
                "type": "error", "error": "Log streamer not available",
            }))
            return

        # Stop previous subscription for this client
        ws_id = id(websocket)
        old_stream = self._log_subs.pop(ws_id, None)
        if old_stream:
            pb.streamer.stop_stream(old_stream)

        # Start new stream — resolve bot log path if needed
        resolved_service = service
        if service.startswith("Bot:"):
            from master.log_streamer import LogStreamer
            parts = service[4:].strip().split(":")
            bot_name = parts[0]
            pb_version = parts[1] if len(parts) > 1 else None
            resolved_service = LogStreamer.resolve_bot_log_path(bot_name, pb_version)

        loop = asyncio.get_event_loop()
        stream_id = await loop.run_in_executor(
            None, pb.streamer.start_stream, host, resolved_service
        )

        if stream_id:
            self._log_subs[ws_id] = stream_id
            self._log_sids[ws_id] = sid  # Store session id for log_lines push
            # Send initial log chunk
            if service.startswith("Bot:"):
                parts = service[4:].strip().split(":")
                bot_name = parts[0]
                pb_version = parts[1] if len(parts) > 1 else None
                content = await loop.run_in_executor(
                    None, pb.streamer.get_bot_log, host, bot_name, 100, pb_version
                )
            else:
                content = await loop.run_in_executor(
                    None, pb.streamer.get_recent_logs, host, service, 100
                )
            await websocket.send(json.dumps({
                "type": "logs",
                "host": host,
                "service": service,
                "lines": (content or "").splitlines(),
                "streaming": True,
                **(({"sid": sid}) if sid is not None else {}),
            }))
        else:
            await websocket.send(json.dumps({
                "type": "error",
                "error": f"Failed to start log stream for {service} on {host}",
            }))

    async def _cmd_unsubscribe_logs(self, websocket, _request: dict):
        """Stop live log streaming for this client."""
        ws_id = id(websocket)
        stream_id = self._log_subs.pop(ws_id, None)
        self._log_sids.pop(ws_id, None)
        if stream_id and self._pbmaster.streamer:
            self._pbmaster.streamer.stop_stream(stream_id)

    async def _cmd_kill_instance(self, websocket, request: dict):
        """Kill a bot instance on a VPS. PBRun will auto-restart it."""
        host = request.get("host", "")
        name = request.get("name", "")
        pb_version = request.get("pb_version", "")
        if not host or not name:
            await websocket.send(json.dumps({
                "type": "error", "error": "host and name required",
            }))
            return

        pb = self._pbmaster
        if not pb.executor:
            await websocket.send(json.dumps({
                "type": "error", "error": "Command executor not available",
            }))
            return

        # Build kill command based on version
        # V7: main.py with instance name in config path
        # V6 Multi: passivbot_multi.py with instance name
        # V6 Single: passivbot.py with instance name
        if pb_version == "7":
            grep_pattern = f"main.py.*{name}"
        elif pb_version == "6":
            grep_pattern = f"passivbot_multi.py.*{name}"
        elif pb_version == "s":
            grep_pattern = f"passivbot.py.*{name}"
        else:
            grep_pattern = f"(main.py|passivbot_multi.py|passivbot.py).*{name}"

        # Find and kill the process
        kill_cmd = (
            f"pid=$(ps aux | grep -E '{grep_pattern}' | grep -v grep "
            f"| awk '{{print $2}}' | head -1) && "
            f"[ -n \"$pid\" ] && kill $pid && echo \"killed:$pid\" || echo \"not_found\""
        )

        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None, pb.executor.execute, host, kill_cmd, 15
        )

        success = result.success and "killed:" in result.stdout
        killed_pid = ""
        if success:
            killed_pid = result.stdout.split("killed:")[1].strip()

        _log(SERVICE,
             f"[cmd] Kill instance {name} on {host}: "
             f"{'OK pid=' + killed_pid if success else 'not found or failed'}",
             level="INFO" if success else "WARNING")

        await websocket.send(json.dumps({
            "type": "result",
            "cmd": "kill_instance",
            "host": host,
            "name": name,
            "success": success,
            "pid": killed_pid,
        }))

    # ── UI Settings ───────────────────────────────────────

    # Allowed keys that clients can persist (whitelist for safety)
    _UI_SETTINGS_KEYS = {"compact"}

    def _load_ui_settings(self) -> dict:
        """Load UI settings from pbgui.ini [pbmaster_ui] section."""
        result = {}
        for key in self._UI_SETTINGS_KEYS:
            val = load_ini("pbmaster_ui", key)
            if val:
                result[key] = val
        return result

    async def _cmd_set_setting(self, websocket, request: dict):
        """Persist a UI setting to pbgui.ini."""
        key = request.get("key", "")
        value = request.get("value", "")
        if key not in self._UI_SETTINGS_KEYS:
            await websocket.send(json.dumps({
                "type": "error",
                "error": f"Unknown setting: {key}",
            }))
            return
        save_ini("pbmaster_ui", key, str(value))
        _log(SERVICE, f"[setting] {key} = {value}")

    # ── Push loops ──────────────────────────────────────────

    async def _push_loop(self):
        """Push full state to all connected clients every PUSH_INTERVAL."""
        while not self._stop_event.is_set():
            try:
                await asyncio.sleep(PUSH_INTERVAL)
                if not self._clients:
                    continue

                state = self._get_full_state()
                msg = json.dumps({"type": "state", "data": state}, default=str)

                # Send to all clients, ignore individual failures
                dead = []
                for ws in list(self._clients):
                    try:
                        await ws.send(msg)
                    except Exception:
                        dead.append(ws)
                for ws in dead:
                    self._clients.discard(ws)

            except asyncio.CancelledError:
                break
            except Exception as e:
                _log(SERVICE, f"[ws] Push loop error: {e}", level="WARNING")
                await asyncio.sleep(PUSH_INTERVAL)

    async def _log_push_loop(self):
        """Push live log lines to subscribed clients."""
        while not self._stop_event.is_set():
            try:
                await asyncio.sleep(LOG_PUSH_INTERVAL)
                if not self._log_subs:
                    continue

                pb = self._pbmaster
                if not pb.streamer:
                    continue

                dead_subs = []
                for ws_id, stream_id in list(self._log_subs.items()):
                    # Find the websocket by id
                    ws = None
                    for client in list(self._clients):
                        if id(client) == ws_id:
                            ws = client
                            break

                    if not ws:
                        dead_subs.append(ws_id)
                        continue

                    # Read buffered lines
                    lines = pb.streamer.read_stream(stream_id, max_lines=50)
                    if lines:
                        try:
                            status = pb.streamer.get_stream_status(stream_id)
                            msg = {
                                "type": "log_lines",
                                "lines": lines,
                                "host": status.get("hostname", "") if status else "",
                                "service": status.get("log_path", "") if status else "",
                            }
                            sid = self._log_sids.get(ws_id)
                            if sid is not None:
                                msg["sid"] = sid
                            await ws.send(json.dumps(msg))
                        except Exception:
                            dead_subs.append(ws_id)

                for ws_id in dead_subs:
                    stream_id = self._log_subs.pop(ws_id, None)
                    self._log_sids.pop(ws_id, None)
                    if stream_id:
                        pb.streamer.stop_stream(stream_id)

            except asyncio.CancelledError:
                break
            except Exception as e:
                _log(SERVICE, f"[ws] Log push error: {e}", level="WARNING")
                await asyncio.sleep(LOG_PUSH_INTERVAL)

    # ── State serialization ─────────────────────────────────

    def _get_full_state(self) -> dict:
        """Build the complete state dict from PBMaster's in-memory data."""
        pb = self._pbmaster

        # Connection status
        conn_summary = {}
        if pb.pool:
            conn_summary = pb.pool.get_status_summary()

        # System metrics (from realtime collector, updated every ~1s)
        system = {}
        if pb.realtime:
            for h in (pb.pool.hostnames() if pb.pool else []):
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

        # Bot instances (from realtime collector, updated every ~30s)
        instances = {}
        if pb.realtime:
            instances = pb.realtime.get_all_instances() or {}

        # Stream info
        streams = {}
        if pb.realtime:
            streams = pb.realtime.get_stream_info()
            # Add system data timestamps for age calculation
            for h in streams:
                m = pb.realtime.get_system(h)
                if m:
                    streams[h]["last_update"] = m.timestamp

        # Services (cached from last service check)
        services = self._last_services

        return {
            "connections": conn_summary,
            "system": system,
            "instances": instances,
            "streams": streams,
            "services": services,
            "timestamp": time.time(),
            "ui_settings": self._load_ui_settings(),
        }
