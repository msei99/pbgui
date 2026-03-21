"""
Async SSH Connection Pool for VPS monitoring.

Manages persistent asyncssh connections to all registered VPS servers.
Auto-reconnects with exponential backoff, reports status changes.

Fully async — no threads, no paramiko.  Runs on the FastAPI event loop.
"""

from __future__ import annotations

import asyncio
import glob
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


# ── Constants ───────────────────────────────────────────────

CONNECT_TIMEOUT = 10        # seconds
KEEPALIVE_INTERVAL = 10     # seconds — lower = faster dead-connection detection (~30s worst-case)
RECONNECT_COOLDOWN = 30     # seconds
MAX_RECONNECT_ATTEMPTS = 5
BACKOFF_MULTIPLIER = 60     # seconds


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
            _log(SERVICE, f"SSH connected to {hostname} ({cfg.ip})")
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
                  timeout: int = 30, check: bool = False
                  ) -> Optional[asyncssh.SSHCompletedProcess]:
        """Run a command on a VPS.

        Returns SSHCompletedProcess or None on connection error.
        If check=True, raises ProcessError on nonzero exit.
        """
        entry = self._connections.get(hostname)
        if not entry or not entry.conn:
            _log(SERVICE, f"[cmd] Cannot run on {hostname}: no connection",
                 level="WARNING")
            return None
        try:
            result = await asyncio.wait_for(
                entry.conn.run(command, check=check),
                timeout=timeout,
            )
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
