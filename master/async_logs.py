"""
Async Log Streamer — replaces master/log_streamer.py.

All operations are async coroutines running on the FastAPI event loop.
Uses ``AsyncSSHPool`` for remote SSH commands (no threads, no paramiko).
"""

from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

from master.async_pool import AsyncSSHPool, REMOTE_PBGUI_DIR
from logging_helpers import human_log as _log

SERVICE = "VPSMonitor"


# ── Known log file locations ─────────────────────────────────

SERVICE_LOGS: dict[str, str] = {
    "PBRun":     "data/logs/PBRun.log",
    "PBRemote":  "data/logs/PBRemote.log",
    "PBCoinData": "data/logs/PBCoinData.log",
    "PBData":    "data/logs/PBData.log",
    "PBMon":     "data/logs/PBMon.log",
}


def _resolve_log_path(service_or_path: str) -> str:
    """Resolve a service name to a relative log file path, or return as-is."""
    return SERVICE_LOGS.get(service_or_path, service_or_path)


def resolve_bot_log_path(instance_name: str, pb_version: str) -> str:
    """Resolve a bot instance to its relative log file path."""
    if pb_version == "7":
        return f"data/run_v7/{instance_name}/passivbot.log"
    elif pb_version == "6":
        return f"data/multi/{instance_name}/passivbot.log"
    elif pb_version == "s":
        return f"data/instances/{instance_name}/passivbot.log"
    return f"data/run_v7/{instance_name}/passivbot.log"


# ── Local log helpers ─────────────────────────────────────────

def local_logs_dir() -> Path:
    """Return the local data/logs directory."""
    return Path(__file__).resolve().parent.parent / "data" / "logs"


def tail_file(path: Path, n: int) -> list[str]:
    """Return the last *n* lines of *path* (all lines if n <= 0)."""
    try:
        size = path.stat().st_size
        if size == 0:
            return []
        with open(path, "rb") as f:
            chunk = min(size, max(n * 200, 65536)) if n > 0 else size
            f.seek(max(0, size - chunk))
            data = f.read()
        text = data.decode("utf-8", errors="replace")
        lines = text.splitlines()
        return lines[-n:] if n > 0 else lines
    except Exception:
        return []


# ── Active stream data ────────────────────────────────────────

@dataclass
class LogStream:
    """Represents an active remote log stream (tail -f via SSH)."""
    stream_id: str
    hostname: str
    log_path: str
    active: bool = True
    task: Optional[asyncio.Task] = field(default=None, repr=False)
    buffer: deque = field(default_factory=lambda: deque(maxlen=1000))
    last_activity: Optional[datetime] = None
    error: Optional[str] = None


@dataclass
class LocalLogSub:
    """Tracks a local log file subscription (file position for tail)."""
    file: Path
    name: str
    pos: int = 0
    sid: Optional[str] = None


class AsyncLogStreamer:
    """
    Async log streamer for remote VPS and local log files.

    Supports:
    - One-shot remote log fetch (``get_recent_logs``, ``get_bot_log``)
    - Live remote log streaming (``start_stream`` / ``stop_stream``)
    - Local log listing and tailing (``list_local_logs``, ``get_local_logs``)
    """

    def __init__(self, pool: AsyncSSHPool):
        self._pool = pool
        self._streams: dict[str, LogStream] = {}
        self._stream_counter = 0

    # ── One-shot remote logs ──────────────────────────────────

    async def get_recent_logs(self, hostname: str, service_or_path: str,
                              lines: int = 100) -> Optional[str]:
        """Fetch the last N lines of a remote log file."""
        log_path = _resolve_log_path(service_or_path)
        full_path = f"~/{REMOTE_PBGUI_DIR}/{log_path}"

        if lines == 0:
            cmd = f"cat {full_path} 2>/dev/null"
        else:
            cmd = f"tail -n {lines} {full_path} 2>/dev/null"

        result = await self._pool.run(hostname, cmd, timeout=30)
        if result is None:
            _log(SERVICE, f"[log] Cannot fetch logs from {hostname}: "
                 "no connection", level="WARNING")
            return None
        if result.exit_status != 0:
            return None
        return result.stdout or ""

    async def get_bot_log(self, hostname: str, instance_name: str,
                          lines: int = 100,
                          pb_version: str = None) -> Optional[str]:
        """Fetch recent log lines for a specific bot instance."""
        if pb_version == "7":
            paths = [
                f"~/{REMOTE_PBGUI_DIR}/data/run_v7/{instance_name}/passivbot.log",
                f"~/{REMOTE_PBGUI_DIR}/data/logs/{instance_name}.log",
            ]
        elif pb_version == "6":
            paths = [
                f"~/{REMOTE_PBGUI_DIR}/data/multi/{instance_name}/passivbot.log",
            ]
        elif pb_version == "s":
            paths = [
                f"~/{REMOTE_PBGUI_DIR}/data/instances/{instance_name}/passivbot.log",
            ]
        else:
            paths = [
                f"~/{REMOTE_PBGUI_DIR}/data/run_v7/{instance_name}/passivbot.log",
                f"~/{REMOTE_PBGUI_DIR}/data/multi/{instance_name}/passivbot.log",
                f"~/{REMOTE_PBGUI_DIR}/data/instances/{instance_name}/passivbot.log",
                f"~/{REMOTE_PBGUI_DIR}/data/logs/{instance_name}.log",
            ]

        for path in paths:
            cmd = (f"test -f {path} && "
                   f"{'cat' if lines == 0 else f'tail -n {lines}'} "
                   f"{path} 2>/dev/null")
            result = await self._pool.run(hostname, cmd, timeout=30)
            if result and result.exit_status == 0 and (result.stdout or "").strip():
                return result.stdout
        return None

    async def get_log_info(self, hostname: str, service_or_path: str,
                           pb_version: str = None) -> Optional[dict]:
        """Get log file info (size in bytes)."""
        if service_or_path.startswith("Bot:"):
            parts = service_or_path[4:].strip().split(":")
            bot_name = parts[0]
            pv = parts[1] if len(parts) > 1 else pb_version
            log_path = resolve_bot_log_path(bot_name, pv or "7")
        else:
            log_path = _resolve_log_path(service_or_path)
        full_path = f"~/{REMOTE_PBGUI_DIR}/{log_path}"

        result = await self._pool.run(
            hostname, f"stat -c '%s' {full_path} 2>/dev/null", timeout=10
        )
        if result and result.exit_status == 0:
            output = (result.stdout or "").strip()
            if output.isdigit():
                return {"size": int(output)}
        return None

    # ── Live remote log streaming ─────────────────────────────

    async def start_stream(self, hostname: str,
                           service_or_path: str) -> Optional[str]:
        """Start a live log stream (tail -f) as an async task."""
        log_path = _resolve_log_path(service_or_path)
        full_path = f"~/{REMOTE_PBGUI_DIR}/{log_path}"

        self._stream_counter += 1
        stream_id = f"{hostname}:{log_path}:{self._stream_counter}"

        stream = LogStream(
            stream_id=stream_id,
            hostname=hostname,
            log_path=log_path,
        )

        task = asyncio.create_task(
            self._stream_worker(stream, full_path),
            name=f"log-stream-{stream_id}",
        )
        stream.task = task
        self._streams[stream_id] = stream

        _log(SERVICE, f"[log] Started stream {stream_id}")
        return stream_id

    def stop_stream(self, stream_id: str):
        """Stop an active log stream."""
        stream = self._streams.get(stream_id)
        if stream:
            stream.active = False
            if stream.task and not stream.task.done():
                stream.task.cancel()
            _log(SERVICE, f"[log] Stopping stream {stream_id}")

    def stop_all_streams(self):
        """Stop all active log streams."""
        for stream in self._streams.values():
            stream.active = False
            if stream.task and not stream.task.done():
                stream.task.cancel()
        _log(SERVICE, "[log] Stopping all streams")

    def read_stream(self, stream_id: str,
                    max_lines: int = 100) -> list[str]:
        """Read buffered lines from a stream (non-blocking)."""
        stream = self._streams.get(stream_id)
        if not stream:
            return []
        lines: list[str] = []
        try:
            while len(lines) < max_lines and stream.buffer:
                lines.append(stream.buffer.popleft())
        except IndexError:
            pass
        return lines

    def get_stream_status(self, stream_id: str) -> Optional[dict]:
        """Get status info for a stream."""
        stream = self._streams.get(stream_id)
        if not stream:
            return None
        return {
            "stream_id": stream.stream_id,
            "hostname": stream.hostname,
            "log_path": stream.log_path,
            "active": stream.active,
            "buffered_lines": len(stream.buffer),
            "last_activity": (stream.last_activity.isoformat()
                              if stream.last_activity else None),
            "error": stream.error,
        }

    def cleanup_stopped(self):
        """Remove stopped streams from the internal registry."""
        stopped = [
            sid for sid, s in self._streams.items()
            if not s.active and (s.task is None or s.task.done())
        ]
        for sid in stopped:
            del self._streams[sid]

    async def _stream_worker(self, stream: LogStream, full_path: str):
        """Async worker that reads tail -f output from SSH.

        Retries automatically after transient connection drops (e.g. TCP
        timeout between VPS hosts).  On each failed attempt it waits up to
        60 s for the SSH pool to re-establish the connection before giving up.
        """
        _MAX_RETRIES = 5
        _RETRY_WAIT_S = 60   # max seconds to wait for reconnect per attempt
        _RETRY_PAUSE_S = 5   # pause between poll checks

        attempt = 0
        try:
            while stream.active and attempt <= _MAX_RETRIES:
                proc = await self._pool.start_process(
                    stream.hostname, f"tail -f -n 0 {full_path} 2>/dev/null"
                )
                if not proc:
                    attempt += 1
                    if attempt > _MAX_RETRIES:
                        stream.error = "No SSH connection (retries exhausted)"
                        stream.active = False
                        _log(SERVICE,
                             f"[log] Stream {stream.stream_id}: no connection "
                             f"after {_MAX_RETRIES} retries — giving up",
                             level="WARNING")
                        return
                    stream.error = f"No SSH connection (retry {attempt}/{_MAX_RETRIES})"
                    _log(SERVICE,
                         f"[log] Stream {stream.stream_id}: no connection, "
                         f"waiting for reconnect (attempt {attempt}/{_MAX_RETRIES})",
                         level="WARNING")
                    # Wait for the pool to reconnect, polling every few seconds
                    waited = 0
                    while stream.active and waited < _RETRY_WAIT_S:
                        await asyncio.sleep(_RETRY_PAUSE_S)
                        waited += _RETRY_PAUSE_S
                        # Check if connection is back
                        test = await self._pool.start_process(
                            stream.hostname, "echo ok"
                        )
                        if test:
                            try:
                                await test.wait_closed()
                            except Exception:
                                pass
                            break  # connection is back, retry tail
                    if not stream.active:
                        return
                    continue  # retry outer while loop

                stream.error = None
                attempt = 0  # reset on successful connect
                _log(SERVICE, f"[log] Stream worker started for "
                     f"{stream.stream_id}")

                async for line in proc.stdout:
                    if not stream.active:
                        break
                    line = line.rstrip("\n")
                    if line:
                        stream.buffer.append(line)
                        stream.last_activity = datetime.now()

                # Process ended — check if it was an intentional stop
                if not stream.active:
                    break
                # Unexpected end (e.g. connection drop) — retry
                attempt += 1
                stream.error = f"Remote tail ended — retrying ({attempt}/{_MAX_RETRIES})"
                _log(SERVICE,
                     f"[log] Stream {stream.stream_id}: tail process ended "
                     f"unexpectedly, retrying ({attempt}/{_MAX_RETRIES})",
                     level="WARNING")
                await asyncio.sleep(_RETRY_PAUSE_S)

        except asyncio.CancelledError:
            pass
        except Exception as e:
            stream.error = str(e)
            _log(SERVICE, f"[log] Stream error for {stream.stream_id}: {e}",
                 level="WARNING")
        finally:
            stream.active = False
            _log(SERVICE, f"[log] Stream worker ended for "
                 f"{stream.stream_id}")

    # ── Local logs ────────────────────────────────────────────

    @staticmethod
    def list_local_logs() -> list[str]:
        """Return sorted list of *.log filenames in data/logs/."""
        d = local_logs_dir()
        if not d.exists():
            return []
        return sorted(p.name for p in d.glob("*.log") if p.is_file())

    @staticmethod
    def get_local_logs(filename: str, lines: int = 200) -> tuple[list[str], int]:
        """Fetch last N lines from a local log file.

        Returns:
            (lines, file_size) tuple
        """
        fp = local_logs_dir() / filename
        logs_root = local_logs_dir().resolve()
        if not filename or not fp.resolve().is_relative_to(logs_root):
            return [], 0
        content = tail_file(fp, lines) if fp.exists() else []
        try:
            file_size = fp.stat().st_size
        except Exception:
            file_size = 0
        return content, file_size

    @staticmethod
    def read_local_log_delta(sub: LocalLogSub,
                             max_bytes: int = 65536) -> list[str]:
        """Read new lines since last position (for streaming).

        Updates ``sub.pos`` in-place.  Handles log rotation (truncation).
        """
        try:
            size = sub.file.stat().st_size
        except Exception:
            return []

        # Log rotation detection
        if size < sub.pos:
            sub.pos = 0

        if size <= sub.pos:
            return []

        try:
            with open(sub.file, "r", encoding="utf-8", errors="replace") as f:
                f.seek(sub.pos)
                new_text = f.read(max_bytes)
                sub.pos = f.tell()
            return new_text.splitlines()
        except Exception:
            return []
