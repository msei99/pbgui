"""
Async Log Streamer — replaces master/log_streamer.py.

All operations are async coroutines running on the FastAPI event loop.
Uses ``AsyncSSHPool`` for remote SSH commands (no threads, no paramiko).
"""

from __future__ import annotations

import asyncio
import json
import re
import shlex
import subprocess
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Optional

from master.async_pool import AsyncSSHPool, remote_path_join, remote_shell_path
from logging_helpers import human_log as _log
from pbgui_purefunc import pb7dir

SERVICE = "VPSMonitor"


# ── Known log file locations ─────────────────────────────────

SERVICE_LOGS: dict[str, str] = {
    "PBCluster": "data/logs/PBCluster.log",
    "PBRun":     "data/logs/PBRun.log",
    "PBCoinData": "data/logs/PBCoinData.log",
    "PBData":    "data/logs/PBData.log",
    "PBGui":     "data/logs/PBGui.log",
    "PBApiServer": "data/logs/PBApiServer.log",
    "VPSMonitor": "data/logs/VPSMonitor.log",
    "VPSManagerApi": "data/logs/VPSManagerApi.log",
}

MAX_REMOTE_LOG_LINES = 50_000
_REMOTE_LOG_FILE_RE = re.compile(r"^[^/\\\x00]+\.log(?:\.\d+)?$")


def normalize_remote_log_lines(value: object, default: int = 200) -> int:
    """Return a safe line count for remote shell log commands."""
    raw = default if value is None else value
    if isinstance(raw, bool):
        raise ValueError("lines must be an integer")
    if isinstance(raw, int):
        lines = raw
    elif isinstance(raw, str) and raw.strip().isdecimal():
        lines = int(raw.strip())
    else:
        raise ValueError("lines must be an integer")
    if lines < 0 or lines > MAX_REMOTE_LOG_LINES:
        raise ValueError(f"lines must be between 0 and {MAX_REMOTE_LOG_LINES}")
    return lines


def _validate_remote_log_instance_name(value: object) -> str:
    """Return a bot instance name which cannot escape its log directory."""
    name = str(value or "").strip()
    if (not name or len(name) > 255 or name in {".", ".."}
            or "/" in name or "\\" in name or "\x00" in name
            or any(ord(char) < 32 or ord(char) == 127 for char in name)):
        raise ValueError("Invalid bot instance name")
    return name


def _validate_remote_log_path(value: object) -> str:
    """Return a normalized log path restricted to supported remote log roots."""
    raw = str(value or "").strip()
    if not raw or raw.startswith(("/", "~")) or "\\" in raw or "\x00" in raw:
        raise ValueError("Invalid remote log path")
    raw_parts = raw.split("/")
    if any(part in {"", ".", ".."} for part in raw_parts):
        raise ValueError("Invalid remote log path")

    parts = PurePosixPath(raw).parts
    filename = parts[-1] if parts else ""
    valid_root = (
        (len(parts) == 3 and parts[:2] == ("data", "logs"))
        or (len(parts) == 4 and parts[:2] == ("data", "run_v7")
            and filename in {"passivbot_err.log", "passivbot_err.log.old"})
        or (len(parts) == 3 and parts[:2] == ("pb7", "logs"))
        or (len(parts) == 4 and parts[:3] == ("software", "pb7", "logs"))
    )
    valid_filename = bool(_REMOTE_LOG_FILE_RE.fullmatch(filename)) or filename == "passivbot_err.log.old"
    if not valid_root or not valid_filename:
        raise ValueError("Remote log path is not allowed")
    if parts[:2] == ("data", "run_v7"):
        _validate_remote_log_instance_name(parts[2])
    return "/".join(parts)


def _resolve_log_path(service_or_path: str) -> str:
    """Resolve a service name to a validated remote log path."""
    return _validate_remote_log_path(SERVICE_LOGS.get(service_or_path, service_or_path))


def _is_home_relative_log_path(log_path: str) -> bool:
    """Return True for remote paths rooted from HOME, not remote pbgui dir."""
    normalized = str(log_path or "").lstrip("./")
    return normalized.startswith("software/") or normalized.startswith("pb7/logs/")


def resolve_bot_log_path(instance_name: str, pb_version: str) -> str:
    """Resolve a bot instance to its remote log file path (passivbot's own log)."""
    name = _validate_remote_log_instance_name(instance_name)
    return _validate_remote_log_path(f"software/pb7/logs/{name}.log")


def _bot_log_path_from_pb7dir(pb7dir_value: str | None, instance_name: str) -> str:
    """Return the remote bot log path using cached pb7dir when available."""
    instance_name = _validate_remote_log_instance_name(instance_name)
    if pb7dir_value:
        return remote_path_join(pb7dir_value, "logs", f"{instance_name}.log")
    return resolve_bot_log_path(instance_name, "7")


def _pb7dir_for_host(pool: AsyncSSHPool, hostname: str) -> str:
    """Return cached remote pb7dir for a host, if the ini cache has it."""
    entry = pool.get_connection(hostname)
    return str((entry.data or {}).get("pb7dir") or "") if entry else ""


def _remote_log_shell_path(pool: AsyncSSHPool, hostname: str, log_path: str) -> str:
    """Resolve a remote log path to a shell-safe absolute/HOME expression."""
    if log_path.startswith("software/pb7/logs/") or log_path.startswith("pb7/logs/"):
        pb7dir_value = _pb7dir_for_host(pool, hostname)
        if pb7dir_value:
            return remote_shell_path(remote_path_join(pb7dir_value, "logs", Path(log_path).name))
        if log_path.startswith("pb7/logs/"):
            return remote_shell_path(remote_path_join("software", log_path))
    return remote_shell_path(log_path)


def resolve_local_bot_log_path(instance_name: str) -> Path:
    """Resolve a local bot instance to the native passivbot log path."""
    configured_pb7dir = str(pb7dir() or "").strip()
    if configured_pb7dir:
        return Path(configured_pb7dir) / "logs" / f"{instance_name}.log"
    return Path.home() / "software" / "pb7" / "logs" / f"{instance_name}.log"


def local_pb7_logs_dir() -> Path:
    """Return the configured local PB7 logs directory."""
    configured_pb7dir = str(pb7dir() or "").strip()
    if configured_pb7dir:
        return Path(configured_pb7dir) / "logs"
    return Path.home() / "software" / "pb7" / "logs"


def resolve_local_bot_err_log_path(instance_name: str) -> Path:
    """Resolve a local bot instance to the legacy stderr log path."""
    return _project_root() / "data" / "run_v7" / instance_name / "passivbot_err.log"


# ── Local log helpers ─────────────────────────────────────────

def _project_root() -> Path:
    """Return the project root directory."""
    return Path(__file__).resolve().parent.parent


def local_logs_dir() -> Path:
    """Return the local data/logs directory."""
    return _project_root() / "data" / "logs"


_TASK_LOG_ALIAS_RE = re.compile(r"^(?P<stem>[A-Za-z0-9_-]+)(?:\.(?P<history>\d+))?$")
_TASK_LOG_FILE_RE = re.compile(r"^(?P<stem>[A-Za-z0-9_-]+)\.log(?:\.(?P<history>\d+))?$")
_PB7_ARCHIVE_LOG_RE = re.compile(r"^\d{8}_\d{6}__.*_config_run\..*\.log$")


def _task_log_filename_from_action(action: str, prefix: str,
                                   legacy_action_files: dict[str, str]) -> Optional[str]:
    raw_action = action.strip()
    if raw_action in legacy_action_files:
        return legacy_action_files[raw_action]
    if raw_action.endswith('.log'):
        return raw_action
    match = _TASK_LOG_ALIAS_RE.fullmatch(raw_action)
    if not match:
        return None
    stem = match.group("stem") or ""
    history = match.group("history")
    legacy_stems = {Path(item).stem for item in legacy_action_files.values()}
    if not (stem.startswith(prefix) or stem.startswith(prefix.replace("-", "_")) or stem in legacy_stems):
        return None
    filename = f"{stem}.log"
    if history is not None:
        filename += f".{history}"
    return filename


def _task_action_from_filename(filename: str) -> Optional[str]:
    match = _TASK_LOG_FILE_RE.fullmatch(filename)
    if not match:
        return None
    stem = match.group("stem") or ""
    history = match.group("history")
    return f"{stem}.{history}" if history is not None else stem


def _list_vps_task_log_aliases() -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    project = _project_root()
    canonical = project / "data" / "logs" / "vps-manager"
    legacy = project / "data" / "vpsmanager"
    for root in (canonical / "master", legacy):
        if not root.exists():
            continue
        for fp in sorted(root.glob("*.log*")):
            if not fp.is_file():
                continue
            action = _task_action_from_filename(fp.name)
            alias = f"MasterAction:{action}" if action else ""
            if alias and alias not in seen:
                seen.add(alias)
                result.append(alias)
    host_names = sorted({
        host_dir.name
        for hosts_root in (canonical / "hosts", legacy / "hosts") if hosts_root.exists()
        for host_dir in hosts_root.iterdir() if host_dir.is_dir()
    })
    for host_name in host_names:
        for host_dir in (canonical / "hosts" / host_name, legacy / "hosts" / host_name):
            if not host_dir.exists():
                continue
            for fp in sorted(host_dir.glob("*.log*")):
                if not fp.is_file():
                    continue
                action = _task_action_from_filename(fp.name)
                alias = f"VPSAction:{host_name}:{action}" if action else ""
                if alias and alias not in seen:
                    seen.add(alias)
                    result.append(alias)
    return result


def _resolve_vps_action_log_path(filename: str) -> Optional[Path]:
    root = _project_root()
    if filename.startswith("VPSAction:"):
        parts = filename.split(":", 2)
        if len(parts) != 3:
            return None
        action_files = {
            "init": "vps_init.log",
            "setup": "vps_setup.log",
            "update": "vps_update.log",
        }
        hostname = parts[1].strip()
        action = parts[2].strip()
        resolved_name = _task_log_filename_from_action(action, "vps-", action_files)
        if not resolved_name:
            resolved_name = _task_log_filename_from_action(action, "master-", {})
        if not resolved_name and action.endswith(".log"):
            resolved_name = action
        if not hostname or not resolved_name:
            return None
        if not re.fullmatch(r"[A-Za-z0-9._-]+", hostname) or hostname in {".", ".."}:
            return None
        candidates = (
            root / "data" / "logs" / "vps-manager" / "hosts" / hostname / resolved_name,
            root / "data" / "vpsmanager" / "hosts" / hostname / resolved_name,
        )
        return next((path for path in candidates if path.is_file()), candidates[0])
    if filename.startswith("MasterAction:"):
        action = filename.split(":", 1)[1].strip()
        action_files = {"update": "vps_update.log"}
        resolved_name = _task_log_filename_from_action(action, "master-", action_files)
        if not resolved_name:
            return None
        candidates = (
            root / "data" / "logs" / "vps-manager" / "master" / resolved_name,
            root / "data" / "vpsmanager" / resolved_name,
        )
        return next((path for path in candidates if path.is_file()), candidates[0])
    return None


def resolve_local_log_path(filename: str) -> Optional[Path]:
    """Resolve a local log identifier to its absolute path.

    Handles regular log files (e.g. 'PBRun.log'), native bot logs
    (e.g. 'Bot:bybit_SANDUSDT'), and legacy bot stderr logs
    (e.g. 'BotErr:bybit_SANDUSDT').
    Returns None if the resolved path escapes allowed directories.
    """
    root = _project_root()
    action_log = _resolve_vps_action_log_path(filename)
    if action_log is not None:
        return action_log
    if filename.startswith("Bot:"):
        instance_name = filename[4:]
        fp = resolve_local_bot_log_path(instance_name)
        logs_root = local_pb7_logs_dir().resolve()
        if not fp.resolve().is_relative_to(logs_root):
            return None
        return fp
    if filename.startswith("pb7/logs/") or filename.startswith("software/pb7/logs/"):
        relative_name = Path(filename).name
        fp = local_pb7_logs_dir() / relative_name
        if not fp.resolve().is_relative_to(local_pb7_logs_dir().resolve()):
            return None
        return fp
    if filename.startswith("BotErr:"):
        instance_name = filename[7:]
        fp = resolve_local_bot_err_log_path(instance_name)
        run_v7_root = (root / "data" / "run_v7").resolve()
        if not fp.resolve().is_relative_to(run_v7_root):
            return None
        return fp
    else:
        fp = local_logs_dir() / filename
        if not fp.resolve().is_relative_to(local_logs_dir().resolve()):
            return None
        return fp


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
        lines = normalize_remote_log_lines(lines, default=100)
        log_path = _resolve_log_path(service_or_path)
        result = None

        # Only HOME-rooted paths like software/pb7/logs/... bypass the remote
        # pbgui base dir. Relative paths like data/logs/PBRun.log must still be
        # resolved below each remote pbgui candidate.
        if _is_home_relative_log_path(log_path):
            full_path = _remote_log_shell_path(self._pool, hostname, log_path)
            if lines == 0:
                cmd = f"cat {full_path} 2>/dev/null"
            else:
                cmd = f"tail -n {lines} {full_path} 2>/dev/null"
            result = await self._pool.run(hostname, cmd, timeout=30)
            if result is None:
                _log(SERVICE, f"[log] Cannot fetch logs from {hostname}: no connection", level="WARNING")
                return None
            if result.exit_status == 0:
                return (result.stdout or "")
            return ""

        for base_dir in self._pool.get_remote_pbgui_dirs(hostname):
            full_path = remote_shell_path(remote_path_join(base_dir, log_path))
            if lines == 0:
                cmd = f"cat {full_path} 2>/dev/null"
            else:
                cmd = f"tail -n {lines} {full_path} 2>/dev/null"
            result = await self._pool.run(hostname, cmd, timeout=30)
            if result is None:
                _log(SERVICE, f"[log] Cannot fetch logs from {hostname}: no connection", level="WARNING")
                return None
            if result.exit_status == 0 and (result.stdout or '').strip():
                return result.stdout or ""
        return ""

    async def get_recent_log_files(
        self,
        hostname: str,
        paths: list[str],
        lines: int = 5000,
        *,
        contains: str | None = None,
    ) -> Optional[str]:
        """Fetch multiple monitor-discovered log files in one SSH round trip."""

        line_limit = normalize_remote_log_lines(lines, default=5000)
        commands: list[str] = []
        seen: set[str] = set()
        for raw_path in paths[:32]:
            try:
                log_path = _resolve_log_path(raw_path)
            except ValueError:
                continue
            if log_path in seen:
                continue
            seen.add(log_path)
            if _is_home_relative_log_path(log_path):
                candidates = [_remote_log_shell_path(self._pool, hostname, log_path)]
            else:
                candidates = [
                    remote_shell_path(remote_path_join(base_dir, log_path))
                    for base_dir in self._pool.get_remote_pbgui_dirs(hostname)
                ]
            branches: list[tuple[str, str]] = []
            for full_path in candidates:
                if contains is None:
                    read_command = f"tail -n {line_limit} {full_path} 2>/dev/null"
                else:
                    read_command = f"grep -hF -- {shlex.quote(contains)} {full_path} 2>/dev/null"
                branches.append((f"[ -f {full_path} ]", read_command))
            if branches:
                command = f"if {branches[0][0]}; then {branches[0][1]}"
                for condition, read_command in branches[1:]:
                    command += f"; elif {condition}; then {read_command}"
                commands.append(command + "; fi")
        if not commands:
            return ""
        command = "{ " + "; ".join(commands) + "; }"
        if line_limit > 0:
            command += f" | tail -n {line_limit}"
        result = await self._pool.run(hostname, command, timeout=30)
        if result is None:
            _log(SERVICE, f"[log] Cannot fetch grouped logs from {hostname}: no connection", level="WARNING")
            return None
        if result.exit_status == 0:
            return result.stdout or ""
        return ""

    async def get_bot_log(self, hostname: str, instance_name: str,
                          lines: int = 100,
                          pb_version: str = None) -> Optional[str]:
        """Fetch the most recent *lines* from the bot's own passivbot log.

        Passivbot writes its log to ``~/software/pb7/logs/{name}.log`` and
        manages rotation internally; the stable filename always points to the
        current run.
        """
        lines = normalize_remote_log_lines(lines, default=100)
        log_path = remote_shell_path(
            _bot_log_path_from_pb7dir(_pb7dir_for_host(self._pool, hostname), instance_name)
        )
        if lines == 0:
            cmd = f"cat {log_path} 2>/dev/null"
        else:
            cmd = f"tail -n {lines} {log_path} 2>/dev/null"
        result = await self._pool.run(hostname, cmd, timeout=30)
        if result and result.exit_status == 0 and (result.stdout or "").strip():
            return result.stdout.rstrip("\n")
        return None

    async def get_log_info(self, hostname: str, service_or_path: str,
                           pb_version: str = None) -> Optional[dict]:
        """Get log file info (size in bytes)."""
        if service_or_path.startswith("Bot:"):
            parts = service_or_path[4:].strip().split(":")
            bot_name = parts[0]
            log_path = _bot_log_path_from_pb7dir(_pb7dir_for_host(self._pool, hostname), bot_name)
            full_path = remote_shell_path(log_path)
            result = await self._pool.run(
                hostname, f"stat -c '%s' {full_path} 2>/dev/null", timeout=10
            )
            if result and result.exit_status == 0:
                output = (result.stdout or "").strip()
                if output.isdigit():
                    return {"size": int(output)}
            return None
        else:
            log_path = _resolve_log_path(service_or_path)

        # Only HOME-rooted paths like software/pb7/logs/... bypass the remote
        # pbgui base dir. Relative paths like data/logs/PBRun.log must still be
        # resolved below each remote pbgui candidate.
        if _is_home_relative_log_path(log_path):
            full_path = _remote_log_shell_path(self._pool, hostname, log_path)
            result = await self._pool.run(
                hostname, f"stat -c '%s' {full_path} 2>/dev/null", timeout=10
            )
            if result and result.exit_status == 0:
                output = (result.stdout or "").strip()
                if output.isdigit():
                    return {"size": int(output)}
            return None

        for base_dir in self._pool.get_remote_pbgui_dirs(hostname):
            full_path = remote_shell_path(remote_path_join(base_dir, log_path))
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
        full_path = None

        # Only HOME-rooted paths like software/pb7/logs/... bypass the remote
        # pbgui base dir. Relative paths like data/logs/PBRun.log must still be
        # resolved below each remote pbgui candidate.
        if _is_home_relative_log_path(log_path):
            full_path = _remote_log_shell_path(self._pool, hostname, log_path)
        else:
            for base_dir in self._pool.get_remote_pbgui_dirs(hostname):
                candidate = remote_shell_path(remote_path_join(base_dir, log_path))
                result = await self._pool.run(hostname, f"test -f {candidate}", timeout=10)
                if result and result.exit_status == 0:
                    full_path = candidate
                    break
            if full_path is None:
                full_path = remote_shell_path(
                    remote_path_join(self._pool.get_remote_pbgui_dir(hostname), log_path)
                )

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
        stream = self._streams.pop(stream_id, None)
        if stream:
            stream.active = False
            if stream.task and not stream.task.done():
                stream.task.cancel()
            _log(SERVICE, f"[log] Stopping stream {stream_id}")

    def stop_all_streams(self):
        """Stop all active log streams."""
        streams = list(self._streams.values())
        self._streams.clear()
        for stream in streams:
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
        proc = None
        try:
            while stream.active and attempt <= _MAX_RETRIES:
                proc = await self._pool.start_process(
                    stream.hostname, f"tail -F -n 0 {full_path} 2>/dev/null"
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
            if proc is not None:
                try:
                    proc.close()
                except Exception:
                    pass
            stream.active = False
            if self._streams.get(stream.stream_id) is stream:
                self._streams.pop(stream.stream_id, None)
            _log(SERVICE, f"[log] Stream worker ended for "
                 f"{stream.stream_id}")

    # ── Local logs ────────────────────────────────────────────

    @staticmethod
    def list_local_logs() -> list[str]:
        """Return sorted list of log identifiers.

        Includes daemon logs from data/logs/, native instance logs from
        pb7/logs/*.log, and legacy instance stderr logs from
        data/run_v7/*/passivbot_err.log.
        """
        result: list[str] = []
        d = local_logs_dir()
        if d.exists():
            result.extend(
                sorted(p.name for p in d.glob("*.log") if p.is_file())
            )
        result.extend(_list_vps_task_log_aliases())
        pb7_logs = local_pb7_logs_dir()
        if pb7_logs.exists():
            for p in sorted(pb7_logs.glob("*.log")):
                if not p.is_file():
                    continue
                if _PB7_ARCHIVE_LOG_RE.match(p.name):
                    result.append(f"pb7/logs/{p.name}")
                else:
                    result.append(f"Bot:{p.stem}")
        run_v7 = _project_root() / "data" / "run_v7"
        if run_v7.exists():
            result.extend(sorted(
                f"BotErr:{p.parent.name}"
                for p in run_v7.glob("*/passivbot_err.log")
                if p.is_file()
            ))
        return list(dict.fromkeys(result))

    resolve_local_log_path = staticmethod(resolve_local_log_path)

    @staticmethod
    def get_local_logs(filename: str, lines: int = 200) -> tuple[list[str], int]:
        """Fetch last N lines from a local log file.

        Returns:
            (lines, file_size) tuple
        """
        if not filename:
            return [], 0
        fp = resolve_local_log_path(filename)
        if fp is None:
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
