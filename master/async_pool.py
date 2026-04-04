"""
Async SSH Connection Pool for VPS monitoring.

Manages persistent asyncssh connections to all registered VPS servers.
Auto-reconnects with exponential backoff, reports status changes.

Fully async — no threads, no paramiko.  Runs on the FastAPI event loop.
"""

from __future__ import annotations

import asyncio
import configparser
import glob
import io
import json
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional

import asyncssh

from pbgui_purefunc import PBGDIR
from logging_helpers import human_log as _log

SERVICE = "VPSMonitor"

# Remote PBGui directory on VPS (relative to home)
REMOTE_PBGUI_DIR = "software/pbgui"


class ConnectionStatus(Enum):
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    AUTH_FAILED = "auth_failed"


@dataclass
class VPSConfig:
    """Configuration for a single VPS."""
    hostname: str
    ip: str
    user: str
    ssh_port: int = 22


@dataclass
class VPSConnection:
    """Holds state for a single VPS SSH connection."""
    config: VPSConfig
    conn: Optional[asyncssh.SSHClientConnection] = field(default=None, repr=False)
    status: ConnectionStatus = ConnectionStatus.DISCONNECTED
    last_connected: Optional[datetime] = None
    last_disconnect: Optional[datetime] = None
    last_error: Optional[str] = None
    reconnect_attempts: int = 0
    data: dict = field(default_factory=dict)  # Cached per-host info (ini, paths)


# ── Constants ───────────────────────────────────────────────

CONNECT_TIMEOUT = 10        # seconds
KEEPALIVE_INTERVAL = 10     # seconds — lower = faster dead-connection detection (~30s worst-case)
RECONNECT_COOLDOWN = 30     # seconds
MAX_RECONNECT_ATTEMPTS = 5
BACKOFF_MULTIPLIER = 60     # seconds
SFTP_RETRY_ATTEMPTS = 2    # total attempts for transient SFTP errors
SFTP_RETRY_DELAY = 0.5     # seconds between retries


def _is_transient_error(e: Exception) -> bool:
    """Check if an error is transient (worth retrying)."""
    return isinstance(e, (
        ConnectionError, TimeoutError, OSError, BrokenPipeError,
        asyncssh.ConnectionLost, asyncssh.DisconnectError,
    ))


class AsyncSSHPool:
    """
    Async SSH connection pool.

    Usage::

        pool = AsyncSSHPool()
        pool.load_vps_configs()
        await pool.connect("myvps")
        result = await pool.run("myvps", "uptime")
        await pool.disconnect_all()
    """

    def __init__(self):
        self._connections: dict[str, VPSConnection] = {}
        # Async callbacks fired after every successful connect/reconnect.
        # Signature: async callback(hostname: str) -> None
        self._on_connect_callbacks: list[callable] = []

    def add_on_connect_callback(self, callback) -> None:
        """Register a callback invoked after every successful SSH connect.

        The callback receives the hostname as its only argument.
        It is called as a fire-and-forget asyncio task so it never blocks
        the connect() call itself.  Multiple callbacks can be registered.
        """
        self._on_connect_callbacks.append(callback)

    # ── Config loading ──────────────────────────────────────

    def load_vps_configs(self) -> list[str]:
        """Load VPS configs from data/vpsmanager/hosts/*/*.json.

        Returns list of hostnames found.
        Does NOT connect — call connect() or connect_enabled() afterwards.
        """
        vps_dir = Path(f'{PBGDIR}/data/vpsmanager/hosts')
        pattern = str(vps_dir / '*' / '*.json')
        hostnames: list[str] = []

        for filepath in sorted(glob.glob(pattern)):
            try:
                with open(filepath, 'r') as f:
                    config = json.load(f)
                hostname = config.get('_hostname')
                ip = config.get('ip')
                user = config.get('user')
                ssh_port = config.get('firewall_ssh_port', 22)

                if not hostname or not ip or not user:
                    _log(SERVICE, f"Skipping VPS config {filepath}: "
                         "missing hostname/ip/user", level="WARNING")
                    continue

                cfg = VPSConfig(
                    hostname=hostname,
                    ip=ip,
                    user=user,
                    ssh_port=ssh_port,
                )

                if hostname in self._connections:
                    # Update existing entry (IP/user/port may have changed)
                    self._connections[hostname].config = cfg
                else:
                    self._connections[hostname] = VPSConnection(config=cfg)

                hostnames.append(hostname)
                _log(SERVICE, f"Loaded VPS config: {hostname} ({ip})",
                     level="DEBUG")
            except Exception as e:
                _log(SERVICE, f"Error loading VPS config {filepath}: {e}",
                     level="ERROR")

        _log(SERVICE, f"Loaded {len(hostnames)} VPS configurations")
        return hostnames

    # ── Connect / disconnect ────────────────────────────────

    async def connect(self, hostname: str) -> bool:
        """Establish asyncssh connection to a single VPS.

        Returns True on success.
        """
        entry = self._connections.get(hostname)
        if not entry:
            _log(SERVICE, f"Unknown VPS: {hostname}", level="ERROR")
            return False

        if entry.status == ConnectionStatus.CONNECTED and self._is_alive(entry):
            return True

        entry.status = ConnectionStatus.CONNECTING
        cfg = entry.config
        try:
            conn = await asyncio.wait_for(
                asyncssh.connect(
                    host=cfg.ip,
                    port=cfg.ssh_port,
                    username=cfg.user,
                    known_hosts=None,  # Accept any host key (same as Paramiko AutoAddPolicy)
                    keepalive_interval=KEEPALIVE_INTERVAL,
                ),
                timeout=CONNECT_TIMEOUT,
            )
            # Close previous connection if lingering
            if entry.conn:
                entry.conn.close()

            entry.conn = conn
            entry.status = ConnectionStatus.CONNECTED
            entry.last_connected = datetime.now()
            entry.last_error = None
            entry.reconnect_attempts = 0
            # Auto-read remote pbgui.ini and cache paths
            await self._cache_remote_ini(entry)
            _log(SERVICE, f"SSH connected to {hostname} ({cfg.ip})")
            # Fire on-connect callbacks (e.g. start inotifywait watchers)
            for _i, _cb in enumerate(self._on_connect_callbacks):
                asyncio.create_task(
                    _cb(hostname),
                    name=f"on-connect-{_i}-{hostname}",
                )
            return True

        except asyncssh.PermissionDenied as e:
            entry.status = ConnectionStatus.AUTH_FAILED
            entry.last_error = f"Authentication failed: {e}"
            entry.reconnect_attempts += 1
            _log(SERVICE, f"SSH auth failed for {hostname}: {e}", level="ERROR")
            return False

        except Exception as e:
            entry.status = ConnectionStatus.DISCONNECTED
            entry.last_error = str(e)
            entry.last_disconnect = datetime.now()
            entry.reconnect_attempts += 1
            _log(SERVICE, f"SSH connect failed for {hostname}: {e}",
                 level="ERROR")
            return False

    async def connect_enabled(self, enabled_hosts: set[str]) -> dict[str, bool]:
        """Connect to all enabled hosts concurrently.

        Returns {hostname: success}.
        """
        targets = [h for h in self._connections if h in enabled_hosts]
        if not targets:
            return {}
        results = await asyncio.gather(
            *(self.connect(h) for h in targets),
            return_exceptions=True,
        )
        return {
            h: (r is True)
            for h, r in zip(targets, results)
        }

    async def disconnect(self, hostname: str):
        """Close SSH connection to a single VPS."""
        entry = self._connections.get(hostname)
        if not entry:
            return
        if entry.conn:
            entry.conn.close()
            entry.conn = None
        entry.status = ConnectionStatus.DISCONNECTED
        entry.last_disconnect = datetime.now()
        _log(SERVICE, f"SSH disconnected from {hostname}")

    async def disconnect_all(self):
        """Close all SSH connections."""
        for hostname in list(self._connections):
            await self.disconnect(hostname)

    def remove_host(self, hostname: str):
        """Remove a host from the pool entirely."""
        entry = self._connections.pop(hostname, None)
        if entry and entry.conn:
            entry.conn.close()

    # ── Command execution ───────────────────────────────────

    async def run(self, hostname: str, command: str,
                  timeout: Optional[int] = 30, check: bool = False
                  ) -> Optional[asyncssh.SSHCompletedProcess]:
        """Run a command on a VPS.

        Returns SSHCompletedProcess or None on connection error.
        If check=True, raises ProcessError on nonzero exit.
        timeout=None means no timeout (wait indefinitely).
        """
        entry = self._connections.get(hostname)
        if not entry or not entry.conn:
            _log(SERVICE, f"[cmd] Cannot run on {hostname}: no connection",
                 level="WARNING")
            return None
        try:
            coro = entry.conn.run(command, check=check)
            if timeout is not None:
                result = await asyncio.wait_for(coro, timeout=timeout)
            else:
                result = await coro
            return result
        except asyncssh.ProcessError:
            raise  # Let caller handle
        except Exception as e:
            _log(SERVICE, f"[cmd] {hostname}: '{command}' failed: {e}",
                 level="ERROR")
            return None

    async def start_process(self, hostname: str, command: str
                            ) -> Optional[asyncssh.SSHClientProcess]:
        """Start a long-running process (returns SSHClientProcess for streaming).

        Caller is responsible for reading stdout and closing.
        """
        entry = self._connections.get(hostname)
        if not entry or not entry.conn:
            return None
        try:
            return await entry.conn.create_process(command)
        except Exception as e:
            _log(SERVICE, f"[cmd] {hostname}: start_process failed: {e}",
                 level="ERROR")
            return None

    # ── Health checks ───────────────────────────────────────

    def _is_alive(self, entry: VPSConnection) -> bool:
        """Check if connection is still open."""
        if not entry.conn:
            return False
        # asyncssh: connection object has no explicit is_active(); but
        # the transport will be None / closed on disconnect.
        transport = getattr(entry.conn, '_transport', None)
        if transport is None:
            return False
        return not transport.is_closing()

    def health_check(self) -> dict[str, ConnectionStatus]:
        """Check all connections; update status for dead ones.

        Does NOT reconnect — call reconnect_lost() for that.
        """
        results: dict[str, ConnectionStatus] = {}
        for hostname, entry in self._connections.items():
            if entry.status == ConnectionStatus.CONNECTED:
                if not self._is_alive(entry):
                    entry.status = ConnectionStatus.DISCONNECTED
                    entry.last_disconnect = datetime.now()
                    entry.last_error = "Connection lost"
                    _log(SERVICE, f"SSH connection lost to {hostname}",
                         level="WARNING")
            results[hostname] = entry.status
        return results

    def should_reconnect(self, hostname: str) -> bool:
        """Whether a reconnect attempt should be made (respects backoff)."""
        entry = self._connections.get(hostname)
        if not entry:
            return False
        if entry.status in (ConnectionStatus.CONNECTED,
                            ConnectionStatus.AUTH_FAILED):
            return False
        if entry.reconnect_attempts >= MAX_RECONNECT_ATTEMPTS:
            backoff = BACKOFF_MULTIPLIER * (
                entry.reconnect_attempts - MAX_RECONNECT_ATTEMPTS + 1
            )
            if entry.last_disconnect:
                elapsed = (datetime.now() - entry.last_disconnect).total_seconds()
                return elapsed >= backoff
            return True
        if entry.last_disconnect:
            elapsed = (datetime.now() - entry.last_disconnect).total_seconds()
            return elapsed >= RECONNECT_COOLDOWN
        return True

    async def reconnect_lost(self, enabled_hosts: set[str]) -> dict[str, bool]:
        """Reconnect all lost connections (respecting backoff).

        Returns {hostname: success} for attempted reconnections.
        """
        results: dict[str, bool] = {}
        for hostname, entry in self._connections.items():
            if hostname not in enabled_hosts:
                continue
            if entry.status in (ConnectionStatus.DISCONNECTED,
                                ConnectionStatus.CONNECTING):
                if self.should_reconnect(hostname):
                    results[hostname] = await self.connect(hostname)
        return results

    # ── Status for UI ───────────────────────────────────────

    def get_status_summary(self) -> dict:
        """Build status summary dict for the WebSocket state push."""
        summary = {
            "total": len(self._connections),
            "connected": 0,
            "disconnected": 0,
            "auth_failed": 0,
            "connections": {},
        }
        for hostname, entry in self._connections.items():
            if entry.status == ConnectionStatus.CONNECTED:
                summary["connected"] += 1
            elif entry.status == ConnectionStatus.AUTH_FAILED:
                summary["auth_failed"] += 1
            else:
                summary["disconnected"] += 1
            summary["connections"][hostname] = {
                "status": entry.status.value,
                "ip": entry.config.ip,
                "last_connected": (entry.last_connected.isoformat()
                                   if entry.last_connected else None),
                "last_disconnect": (entry.last_disconnect.isoformat()
                                    if entry.last_disconnect else None),
                "last_error": entry.last_error,
                "reconnect_attempts": entry.reconnect_attempts,
            }
        return summary

    def hostnames(self) -> list[str]:
        """Return sorted list of all known VPS hostnames."""
        return sorted(self._connections.keys())

    def connected_hosts(self) -> list[str]:
        """Return sorted list of currently connected VPS hostnames."""
        return sorted(
            h for h, e in self._connections.items()
            if e.status == ConnectionStatus.CONNECTED
        )

    def get_connection(self, hostname: str) -> Optional[VPSConnection]:
        """Get the VPSConnection entry (for diagnostics)."""
        return self._connections.get(hostname)

    # ── SFTP file operations ────────────────────────────────

    async def _open_sftp(self, hostname: str) -> Optional[asyncssh.SFTPClient]:
        """Get an SFTP client for a connected host."""
        entry = self._connections.get(hostname)
        if not entry or not entry.conn:
            _log(SERVICE, f"[sftp] Cannot open SFTP to {hostname}: no connection",
                 level="WARNING")
            return None
        try:
            return await entry.conn.start_sftp_client()
        except Exception as e:
            _log(SERVICE, f"[sftp] Failed to open SFTP to {hostname}: {e}",
                 level="ERROR")
            return None

    async def push_file(self, hostname: str, local_path: Path,
                        remote_path: str) -> bool:
        """Push a local file to a remote path via SFTP (with retry)."""
        for attempt in range(1, SFTP_RETRY_ATTEMPTS + 1):
            sftp = await self._open_sftp(hostname)
            if not sftp:
                return False
            try:
                await sftp.put(str(local_path), remote_path)
                _log(SERVICE, f"[sftp] Pushed {local_path.name} → "
                     f"{hostname}:{remote_path}", level="DEBUG")
                return True
            except Exception as e:
                if attempt < SFTP_RETRY_ATTEMPTS and _is_transient_error(e):
                    _log(SERVICE, f"[sftp] Push to {hostname}:{remote_path} "
                         f"failed (attempt {attempt}): {e} — retrying",
                         level="WARNING")
                    await asyncio.sleep(SFTP_RETRY_DELAY)
                    continue
                _log(SERVICE, f"[sftp] Push to {hostname}:{remote_path} "
                     f"failed: {e}", level="ERROR")
                return False
            finally:
                sftp.exit()
        return False

    async def pull_file(self, hostname: str, remote_path: str,
                        local_path: Path) -> bool:
        """Pull a remote file to a local path via SFTP (with retry)."""
        for attempt in range(1, SFTP_RETRY_ATTEMPTS + 1):
            sftp = await self._open_sftp(hostname)
            if not sftp:
                return False
            try:
                local_path.parent.mkdir(parents=True, exist_ok=True)
                await sftp.get(remote_path, str(local_path))
                _log(SERVICE, f"[sftp] Pulled {hostname}:{remote_path} → "
                     f"{local_path}", level="DEBUG")
                return True
            except Exception as e:
                if attempt < SFTP_RETRY_ATTEMPTS and _is_transient_error(e):
                    _log(SERVICE, f"[sftp] Pull from {hostname}:{remote_path} "
                         f"failed (attempt {attempt}): {e} — retrying",
                         level="WARNING")
                    await asyncio.sleep(SFTP_RETRY_DELAY)
                    continue
                _log(SERVICE, f"[sftp] Pull from {hostname}:{remote_path} "
                     f"failed: {e}", level="ERROR")
                return False
            finally:
                sftp.exit()
        return False

    async def read_remote_file(self, hostname: str,
                               remote_path: str) -> Optional[bytes]:
        """Read a remote file's contents into memory (with retry)."""
        for attempt in range(1, SFTP_RETRY_ATTEMPTS + 1):
            sftp = await self._open_sftp(hostname)
            if not sftp:
                return None
            try:
                async with sftp.open(remote_path, 'rb') as f:
                    return await f.read()
            except asyncssh.SFTPNoSuchFile:
                return None
            except Exception as e:
                if attempt < SFTP_RETRY_ATTEMPTS and _is_transient_error(e):
                    _log(SERVICE, f"[sftp] Read {hostname}:{remote_path} "
                         f"failed (attempt {attempt}): {e} — retrying",
                         level="WARNING")
                    await asyncio.sleep(SFTP_RETRY_DELAY)
                    continue
                _log(SERVICE, f"[sftp] Read {hostname}:{remote_path} failed: "
                     f"{type(e).__name__}: {e}", level="ERROR")
                return None
            finally:
                sftp.exit()
        return None

    async def list_remote_dir(self, hostname: str,
                              remote_path: str) -> list[str]:
        """List filenames in a remote directory."""
        sftp = await self._open_sftp(hostname)
        if not sftp:
            return []
        try:
            return await sftp.listdir(remote_path)
        except asyncssh.SFTPNoSuchFile:
            # Directory doesn't exist yet — not an error (e.g. backup dir
            # on first push before any backup has been created).
            return []
        except Exception as e:
            _log(SERVICE, f"[sftp] Listdir {hostname}:{remote_path} "
                 f"failed: {e}", level="ERROR")
            return []
        finally:
            sftp.exit()

    async def remove_remote_file(self, hostname: str,
                                 remote_path: str) -> bool:
        """Delete a single remote file."""
        sftp = await self._open_sftp(hostname)
        if not sftp:
            return False
        try:
            await sftp.remove(remote_path)
            return True
        except Exception as e:
            _log(SERVICE, f"[sftp] Remove {hostname}:{remote_path} "
                 f"failed: {e}", level="ERROR")
            return False
        finally:
            sftp.exit()

    async def makedirs_remote(self, hostname: str,
                              remote_path: str) -> bool:
        """Create remote directory tree (like mkdir -p)."""
        sftp = await self._open_sftp(hostname)
        if not sftp:
            return False
        try:
            await sftp.makedirs(remote_path, exist_ok=True)
            return True
        except Exception as e:
            _log(SERVICE, f"[sftp] Makedirs {hostname}:{remote_path} "
                 f"failed: {e}", level="ERROR")
            return False
        finally:
            sftp.exit()

    async def stat_remote(self, hostname: str,
                          remote_path: str) -> Optional[asyncssh.SFTPAttrs]:
        """Get file attributes (size, mtime, etc.) for a remote path."""
        sftp = await self._open_sftp(hostname)
        if not sftp:
            return None
        try:
            return await sftp.stat(remote_path)
        except Exception:
            return None
        finally:
            sftp.exit()

    async def start_file_watcher(self, hostname: str,
                                 remote_path: str
                                 ) -> Optional[asyncssh.SSHClientProcess]:
        """Start inotifywait process watching a remote file for changes.

        Returns the SSHClientProcess for the caller to read stdout from.
        """
        cmd = (f"inotifywait -m -e close_write --format '%w%f' "
               f"'{remote_path}' 2>/dev/null")
        return await self.start_process(hostname, cmd)

    # ── Remote pbgui.ini access ─────────────────────────────

    async def _cache_remote_ini(self, entry: VPSConnection):
        """Read remote pbgui.ini on connect and cache paths in entry.data."""
        hostname = entry.config.hostname
        try:
            ini = await self._read_ini_internal(entry)
            entry.data['ini'] = ini
            entry.data['pb7dir'] = ini.get('main', 'pb7dir', fallback=None)
            entry.data['pbname'] = ini.get('main', 'pbname', fallback=hostname)
            _log(SERVICE, f"[ini] Cached remote config for {hostname} "
                 f"(pb7dir={entry.data['pb7dir']})", level="DEBUG")
        except Exception as e:
            _log(SERVICE, f"[ini] Failed to read remote ini for {hostname}: "
                 f"{e}", level="WARNING")
            entry.data['ini'] = None
            entry.data['pb7dir'] = None
            entry.data['pbname'] = hostname

    async def _read_ini_internal(self,
                                 entry: VPSConnection
                                 ) -> configparser.ConfigParser:
        """Read and parse remote pbgui.ini via SFTP (uses entry.conn directly)."""
        remote_path = f"{REMOTE_PBGUI_DIR}/pbgui.ini"
        sftp = await entry.conn.start_sftp_client()
        async with sftp.open(remote_path, 'r') as f:
            content = await f.read()
        cfg = configparser.ConfigParser()
        cfg.read_string(content if isinstance(content, str)
                        else content.decode('utf-8'))
        return cfg

    async def read_remote_ini(self, hostname: str
                              ) -> Optional[configparser.ConfigParser]:
        """Read remote pbgui.ini. Returns cached version if available."""
        entry = self._connections.get(hostname)
        if not entry or not entry.conn:
            return None
        cached = entry.data.get('ini')
        if cached is not None:
            return cached
        try:
            ini = await self._read_ini_internal(entry)
            entry.data['ini'] = ini
            return ini
        except Exception as e:
            _log(SERVICE, f"[ini] Read failed for {hostname}: {e}",
                 level="ERROR")
            return None

    async def write_remote_ini(self, hostname: str,
                               config: configparser.ConfigParser) -> bool:
        """Write a full ConfigParser back to remote pbgui.ini via SFTP."""
        entry = self._connections.get(hostname)
        if not entry or not entry.conn:
            return False
        remote_path = f"{REMOTE_PBGUI_DIR}/pbgui.ini"
        try:
            buf = io.StringIO()
            config.write(buf)
            content = buf.getvalue()
            sftp = await entry.conn.start_sftp_client()
            async with sftp.open(remote_path, 'w') as f:
                await f.write(content)
            # Refresh cache
            entry.data['ini'] = config
            _log(SERVICE, f"[ini] Wrote remote ini for {hostname}")
            return True
        except Exception as e:
            _log(SERVICE, f"[ini] Write failed for {hostname}: {e}",
                 level="ERROR")
            return False

    async def get_remote_ini_value(self, hostname: str, section: str,
                                   key: str,
                                   fallback=None) -> Optional[str]:
        """Read a single value from the cached remote pbgui.ini."""
        ini = await self.read_remote_ini(hostname)
        if ini is None:
            return fallback
        return ini.get(section, key, fallback=fallback)

    async def set_remote_ini_value(self, hostname: str, section: str,
                                   key: str, value: str) -> bool:
        """Set a single value in the remote pbgui.ini (read-modify-write)."""
        entry = self._connections.get(hostname)
        if not entry or not entry.conn:
            return False
        try:
            ini = await self._read_ini_internal(entry)
            if not ini.has_section(section):
                ini.add_section(section)
            ini.set(section, key, value)
            success = await self.write_remote_ini(hostname, ini)
            if success:
                # Also update convenience keys if relevant
                entry.data['pb7dir'] = ini.get('main', 'pb7dir', fallback=None)
            return success
        except Exception as e:
            _log(SERVICE, f"[ini] set_remote_ini_value failed for "
                 f"{hostname}: {e}", level="ERROR")
            return False
