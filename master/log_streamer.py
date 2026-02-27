"""
Log Streamer for PBMaster.

Provides live log streaming and recent log fetching from remote VPS servers
via SSH. Supports streaming service logs and individual bot instance logs.
"""

import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Callable
from collections import deque

from master.connection_pool import SSHConnectionPool
from master.service_monitor import REMOTE_PBGUI_DIR
from logging_helpers import human_log as _log


SERVICE = "PBMaster"


@dataclass
class LogStream:
    """Represents an active log stream."""
    stream_id: str
    hostname: str
    log_path: str
    active: bool = True
    thread: Optional[threading.Thread] = field(default=None, repr=False)
    buffer: deque = field(default_factory=lambda: deque(maxlen=1000))
    last_activity: Optional[datetime] = None
    error: Optional[str] = None


class LogStreamer:
    """
    Streams logs from remote VPS servers in real-time.

    Supports two modes:
    1. get_recent_logs() — fetch last N lines (one-shot)
    2. start_stream() / stop_stream() — continuous tail -f streaming

    Usage:
        streamer = LogStreamer(pool)

        # One-shot: get last 100 lines of PBRun log
        lines = streamer.get_recent_logs("myvps", "PBRun", lines=100)

        # Streaming: start tailing a bot log
        stream_id = streamer.start_stream("myvps", "data/logs/PBRun.log")
        new_lines = streamer.read_stream(stream_id)  # Get buffered lines
        streamer.stop_stream(stream_id)
    """

    # Known log file locations (relative to PBGUI dir)
    SERVICE_LOGS = {
        "PBRun": "data/logs/PBRun.log",
        "PBRemote": "data/logs/PBRemote.log",
        "PBCoinData": "data/logs/PBCoinData.log",
        "PBData": "data/logs/PBData.log",
        "PBMon": "data/logs/PBMon.log",
    }

    def __init__(self, pool: SSHConnectionPool):
        self._pool = pool
        self._streams: dict[str, LogStream] = {}
        self._lock = threading.Lock()
        self._stream_counter = 0

    def get_recent_logs(self, hostname: str, service_or_path: str,
                        lines: int = 100) -> Optional[str]:
        """
        Fetch the last N lines of a log file (one-shot, not streaming).

        Args:
            hostname: VPS hostname
            service_or_path: Service name ("PBRun") or relative log path
            lines: Number of lines to fetch

        Returns:
            Log content as string, or None on error
        """
        log_path = self._resolve_log_path(service_or_path)
        full_path = f"~/{REMOTE_PBGUI_DIR}/{log_path}"

        client = self._pool.get_or_reconnect(hostname)
        if not client:
            _log(SERVICE, f"[log] Cannot fetch logs from {hostname}: no connection",
                 level="WARNING")
            return None

        try:
            if lines == 0:
                cmd = f"cat {full_path} 2>/dev/null"
            else:
                cmd = f"tail -n {lines} {full_path} 2>/dev/null"
            stdin, stdout, stderr = client.exec_command(cmd, timeout=30)
            output = stdout.read().decode('utf-8', errors='replace')
            exit_code = stdout.channel.recv_exit_status()

            if exit_code != 0:
                err = stderr.read().decode('utf-8', errors='replace')
                _log(SERVICE, f"[log] Error fetching logs from {hostname}: {err}",
                     level="WARNING")
                return None

            return output

        except Exception as e:
            _log(SERVICE, f"[log] Failed to fetch logs from {hostname}: {e}",
                 level="ERROR")
            return None

    def get_bot_log(self, hostname: str, instance_name: str,
                    lines: int = 100, pb_version: str = None) -> Optional[str]:
        """
        Fetch recent log lines for a specific bot instance.
        Searches common log locations on the VPS.

        Args:
            hostname: VPS hostname
            instance_name: Bot instance name (directory name)
            lines: Number of lines to fetch
            pb_version: "7", "6", or "s" — used to optimize search order
        """
        # Build search paths based on version
        if pb_version == "7":
            possible_paths = [
                f"~/{REMOTE_PBGUI_DIR}/data/run_v7/{instance_name}/passivbot.log",
                f"~/{REMOTE_PBGUI_DIR}/data/logs/{instance_name}.log",
            ]
        elif pb_version == "6":
            possible_paths = [
                f"~/{REMOTE_PBGUI_DIR}/data/multi/{instance_name}/passivbot.log",
            ]
        elif pb_version == "s":
            possible_paths = [
                f"~/{REMOTE_PBGUI_DIR}/data/instances/{instance_name}/passivbot.log",
            ]
        else:
            # Unknown version — search all locations
            possible_paths = [
                f"~/{REMOTE_PBGUI_DIR}/data/run_v7/{instance_name}/passivbot.log",
                f"~/{REMOTE_PBGUI_DIR}/data/multi/{instance_name}/passivbot.log",
                f"~/{REMOTE_PBGUI_DIR}/data/instances/{instance_name}/passivbot.log",
                f"~/{REMOTE_PBGUI_DIR}/data/logs/{instance_name}.log",
            ]

        client = self._pool.get_or_reconnect(hostname)
        if not client:
            return None

        for path in possible_paths:
            try:
                if lines == 0:
                    cmd = f"test -f {path} && cat {path} 2>/dev/null"
                else:
                    cmd = f"test -f {path} && tail -n {lines} {path} 2>/dev/null"
                stdin, stdout, stderr = client.exec_command(cmd, timeout=30)
                output = stdout.read().decode('utf-8', errors='replace')
                if output.strip():
                    return output
            except Exception:
                continue

        _log(SERVICE, f"[log] No log found for bot {instance_name} on {hostname}",
             level="DEBUG")
        return None

    def list_bot_logs(self, hostname: str) -> list[str]:
        """
        List available bot log files on a VPS.
        Returns list of log file paths (relative to PBGUI dir).
        """
        client = self._pool.get_or_reconnect(hostname)
        if not client:
            return []

        try:
            cmd = (
                f"find ~/{REMOTE_PBGUI_DIR}/data/logs/ "
                f"-name '*.log' -type f 2>/dev/null | sort"
            )
            stdin, stdout, stderr = client.exec_command(cmd, timeout=10)
            output = stdout.read().decode('utf-8', errors='replace')
            return [line.strip() for line in output.strip().split('\n') if line.strip()]
        except Exception as e:
            _log(SERVICE, f"[log] Failed to list logs on {hostname}: {e}",
                 level="ERROR")
            return []

    def start_stream(self, hostname: str, service_or_path: str,
                     callback: Callable[[str], None] = None) -> Optional[str]:
        """
        Start a live log stream (tail -f) in a background thread.

        Args:
            hostname: VPS hostname
            service_or_path: Service name or log path
            callback: Optional callback called with each new line

        Returns:
            stream_id for managing the stream, or None on failure
        """
        log_path = self._resolve_log_path(service_or_path)
        full_path = f"~/{REMOTE_PBGUI_DIR}/{log_path}"

        client = self._pool.get_or_reconnect(hostname)
        if not client:
            return None

        with self._lock:
            self._stream_counter += 1
            stream_id = f"{hostname}:{log_path}:{self._stream_counter}"

        stream = LogStream(
            stream_id=stream_id,
            hostname=hostname,
            log_path=log_path,
        )

        # Start tail -f in a background thread
        thread = threading.Thread(
            target=self._stream_worker,
            args=(stream, full_path, callback),
            daemon=True,
            name=f"log-stream-{stream_id}",
        )
        stream.thread = thread

        with self._lock:
            self._streams[stream_id] = stream

        thread.start()
        _log(SERVICE, f"[log] Started stream {stream_id}")
        return stream_id

    def stop_stream(self, stream_id: str):
        """Stop an active log stream."""
        with self._lock:
            stream = self._streams.get(stream_id)
            if stream:
                stream.active = False
                _log(SERVICE, f"[log] Stopping stream {stream_id}")

    def stop_all_streams(self):
        """Stop all active log streams."""
        with self._lock:
            for stream in self._streams.values():
                stream.active = False
        _log(SERVICE, "[log] Stopping all streams")

    def read_stream(self, stream_id: str, max_lines: int = 100) -> list[str]:
        """
        Read buffered lines from a stream.
        Returns up to max_lines lines and removes them from the buffer.
        """
        with self._lock:
            stream = self._streams.get(stream_id)
            if not stream:
                return []

        lines = []
        try:
            while len(lines) < max_lines and stream.buffer:
                lines.append(stream.buffer.popleft())
        except IndexError:
            pass
        return lines

    def get_stream_status(self, stream_id: str) -> Optional[dict]:
        """Get status info for a stream."""
        with self._lock:
            stream = self._streams.get(stream_id)
            if not stream:
                return None
            return {
                "stream_id": stream.stream_id,
                "hostname": stream.hostname,
                "log_path": stream.log_path,
                "active": stream.active,
                "buffered_lines": len(stream.buffer),
                "last_activity": stream.last_activity.isoformat() if stream.last_activity else None,
                "error": stream.error,
            }

    def active_streams(self) -> list[str]:
        """Return list of active stream IDs."""
        with self._lock:
            return [sid for sid, s in self._streams.items() if s.active]

    def cleanup_stopped(self):
        """Remove stopped streams from the internal registry."""
        with self._lock:
            stopped = [sid for sid, s in self._streams.items()
                       if not s.active and (s.thread is None or not s.thread.is_alive())]
            for sid in stopped:
                del self._streams[sid]

    def _stream_worker(self, stream: LogStream, full_path: str,
                       callback: Callable[[str], None] = None):
        """Background worker thread for tail -f streaming."""
        try:
            client = self._pool.get_or_reconnect(stream.hostname)
            if not client:
                stream.error = "No SSH connection"
                stream.active = False
                return

            transport = client.get_transport()
            if not transport:
                stream.error = "No SSH transport"
                stream.active = False
                return

            channel = transport.open_session()
            channel.exec_command(f"tail -f {full_path} 2>/dev/null")
            channel.settimeout(1.0)  # Non-blocking reads with 1s timeout

            _log(SERVICE, f"[log] Stream worker started for {stream.stream_id}")

            while stream.active:
                try:
                    if channel.recv_ready():
                        data = channel.recv(16384).decode('utf-8', errors='replace')
                        if data:
                            for line in data.splitlines():
                                stream.buffer.append(line)
                                if callback:
                                    try:
                                        callback(line)
                                    except Exception:
                                        pass
                            stream.last_activity = datetime.now()
                    else:
                        time.sleep(0.05)

                    # Check if channel is still open
                    if channel.exit_status_ready():
                        stream.error = "Remote tail process ended"
                        break

                except Exception as e:
                    if stream.active:
                        # Socket timeout is expected — we set 1s timeout
                        if "timed out" not in str(e).lower():
                            stream.error = str(e)
                            _log(SERVICE,
                                 f"[log] Stream error for {stream.stream_id}: {e}",
                                 level="WARNING")
                            break
                    else:
                        break

            channel.close()

        except Exception as e:
            stream.error = str(e)
            _log(SERVICE, f"[log] Stream worker failed for {stream.stream_id}: {e}",
                 level="ERROR")
        finally:
            stream.active = False
            _log(SERVICE, f"[log] Stream worker ended for {stream.stream_id}")

    def get_log_info(self, hostname: str, service_or_path: str,
                     pb_version: str = None) -> Optional[dict]:
        """
        Get log file info (size in bytes) from a VPS.

        Args:
            hostname: VPS hostname
            service_or_path: Service name or "Bot:name:version"
            pb_version: Used for bot log path resolution

        Returns:
            dict with 'size' key, or None on error
        """
        if service_or_path.startswith("Bot:"):
            parts = service_or_path[4:].strip().split(":")
            bot_name = parts[0]
            pv = parts[1] if len(parts) > 1 else pb_version
            log_path = self.resolve_bot_log_path(bot_name, pv or "7")
        else:
            log_path = self._resolve_log_path(service_or_path)
        full_path = f"~/{REMOTE_PBGUI_DIR}/{log_path}"

        client = self._pool.get_or_reconnect(hostname)
        if not client:
            return None

        try:
            cmd = f"stat -c '%s' {full_path} 2>/dev/null"
            stdin, stdout, stderr = client.exec_command(cmd, timeout=10)
            output = stdout.read().decode('utf-8', errors='replace').strip()
            exit_code = stdout.channel.recv_exit_status()
            if exit_code == 0 and output.isdigit():
                return {"size": int(output)}
            return None
        except Exception as e:
            _log(SERVICE, f"[log] Failed to get log info from {hostname}: {e}",
                 level="ERROR")
            return None

    def _resolve_log_path(self, service_or_path: str) -> str:
        """Resolve a service name to a log file path, or return as-is."""
        if service_or_path in self.SERVICE_LOGS:
            return self.SERVICE_LOGS[service_or_path]
        return service_or_path

    @staticmethod
    def resolve_bot_log_path(instance_name: str, pb_version: str) -> str:
        """Resolve a bot instance to its relative log file path."""
        if pb_version == "7":
            return f"data/run_v7/{instance_name}/passivbot.log"
        elif pb_version == "6":
            return f"data/multi/{instance_name}/passivbot.log"
        elif pb_version == "s":
            return f"data/instances/{instance_name}/passivbot.log"
        else:
            return f"data/run_v7/{instance_name}/passivbot.log"
