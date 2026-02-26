"""
SSH Connection Pool for PBMaster.

Manages persistent SSH connections to all registered VPS servers.
Auto-reconnects on connection loss, sends keepalive packets,
and reports connection status changes for alerting.
"""

import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from datetime import datetime
from typing import Optional

import paramiko

from pbgui_purefunc import PBGDIR
from logging_helpers import human_log as _log


SERVICE = "PBMaster"


class ConnectionStatus(Enum):
    """Status of a single SSH connection."""
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    AUTH_FAILED = "auth_failed"
    UNKNOWN = "unknown"


@dataclass
class ConnectionInfo:
    """Holds state for a single VPS SSH connection."""
    hostname: str
    ip: str
    user: str
    ssh_port: int = 22
    status: ConnectionStatus = ConnectionStatus.DISCONNECTED
    client: Optional[paramiko.SSHClient] = field(default=None, repr=False)
    last_connected: Optional[datetime] = None
    last_disconnect: Optional[datetime] = None
    last_error: Optional[str] = None
    reconnect_attempts: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)


class SSHConnectionPool:
    """
    Manages persistent SSH connections to all VPS servers.

    Usage:
        pool = SSHConnectionPool()
        pool.load_vps_configs()       # Load VPS data from data/vpsmanager/hosts/
        pool.connect_all()            # Establish SSH to all VPS
        client = pool.get("myvps")    # Get active paramiko.SSHClient
        pool.health_check()           # Check all connections, reconnect if needed
        pool.disconnect_all()         # Clean shutdown
    """

    # SSH keepalive interval (seconds)
    KEEPALIVE_INTERVAL = 15
    # Max time to wait for SSH connection (seconds)
    CONNECT_TIMEOUT = 10
    # How long before we attempt reconnect (seconds)
    RECONNECT_COOLDOWN = 30
    # Max consecutive reconnect attempts before backing off
    MAX_RECONNECT_ATTEMPTS = 5
    # Backoff multiplier (seconds) after max attempts
    BACKOFF_MULTIPLIER = 60

    def __init__(self):
        self._connections: dict[str, ConnectionInfo] = {}
        self._lock = threading.Lock()
        self._vps_dir = Path(f'{PBGDIR}/data/vpsmanager/hosts')

    @property
    def connections(self) -> dict[str, ConnectionInfo]:
        """Read-only access to connection info dict."""
        return dict(self._connections)

    def load_vps_configs(self) -> list[str]:
        """
        Load VPS configurations from data/vpsmanager/hosts/*/*.json.
        Returns list of hostnames found.

        Does NOT establish connections — call connect_all() after this.
        """
        import json
        import glob

        hostnames = []
        pattern = str(self._vps_dir / '*' / '*.json')
        for filepath in sorted(glob.glob(pattern)):
            try:
                with open(filepath, 'r') as f:
                    config = json.load(f)
                hostname = config.get('_hostname')
                ip = config.get('ip')
                user = config.get('user')
                ssh_port = config.get('firewall_ssh_port', 22)

                if not hostname or not ip or not user:
                    _log(SERVICE, f"Skipping VPS config {filepath}: missing hostname/ip/user",
                         level="WARNING")
                    continue

                with self._lock:
                    if hostname not in self._connections:
                        self._connections[hostname] = ConnectionInfo(
                            hostname=hostname,
                            ip=ip,
                            user=user,
                            ssh_port=ssh_port,
                        )
                    else:
                        # Update IP/user if changed
                        info = self._connections[hostname]
                        info.ip = ip
                        info.user = user
                        info.ssh_port = ssh_port

                hostnames.append(hostname)
                _log(SERVICE, f"Loaded VPS config: {hostname} ({ip})", level="DEBUG")
            except Exception as e:
                _log(SERVICE, f"Error loading VPS config {filepath}: {e}", level="ERROR")

        _log(SERVICE, f"Loaded {len(hostnames)} VPS configurations")
        return hostnames

    def _create_ssh_client(self, info: ConnectionInfo) -> paramiko.SSHClient:
        """Create and configure a paramiko SSH client."""
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            hostname=info.ip,
            port=info.ssh_port,
            username=info.user,
            timeout=self.CONNECT_TIMEOUT,
            banner_timeout=self.CONNECT_TIMEOUT,
            auth_timeout=self.CONNECT_TIMEOUT,
            allow_agent=True,
            look_for_keys=True,
        )
        # Enable keepalive
        transport = client.get_transport()
        if transport:
            transport.set_keepalive(self.KEEPALIVE_INTERVAL)
        return client

    def connect(self, hostname: str) -> bool:
        """
        Establish SSH connection to a single VPS.
        Returns True if connected successfully.
        """
        info = self._connections.get(hostname)
        if not info:
            _log(SERVICE, f"Unknown VPS: {hostname}", level="ERROR")
            return False

        with info.lock:
            if info.status == ConnectionStatus.CONNECTED and self._is_alive(info):
                return True

            info.status = ConnectionStatus.CONNECTING
            try:
                # Close existing client if any
                if info.client:
                    try:
                        info.client.close()
                    except Exception:
                        pass

                info.client = self._create_ssh_client(info)
                info.status = ConnectionStatus.CONNECTED
                info.last_connected = datetime.now()
                info.last_error = None
                info.reconnect_attempts = 0
                _log(SERVICE, f"SSH connected to {hostname} ({info.ip})")
                return True

            except paramiko.AuthenticationException as e:
                info.status = ConnectionStatus.AUTH_FAILED
                info.last_error = f"Authentication failed: {e}"
                info.reconnect_attempts += 1
                _log(SERVICE, f"SSH auth failed for {hostname}: {e}", level="ERROR")
                return False

            except Exception as e:
                info.status = ConnectionStatus.DISCONNECTED
                info.last_error = str(e)
                info.last_disconnect = datetime.now()
                info.reconnect_attempts += 1
                _log(SERVICE, f"SSH connect failed for {hostname}: {e}", level="ERROR")
                return False

    def connect_all(self) -> dict[str, bool]:
        """
        Connect to all loaded VPS servers.
        Returns dict of {hostname: success_bool}.
        """
        results = {}
        for hostname in list(self._connections.keys()):
            results[hostname] = self.connect(hostname)
        return results

    def disconnect(self, hostname: str):
        """Close SSH connection to a single VPS."""
        info = self._connections.get(hostname)
        if not info:
            return
        with info.lock:
            if info.client:
                try:
                    info.client.close()
                except Exception:
                    pass
                info.client = None
            info.status = ConnectionStatus.DISCONNECTED
            info.last_disconnect = datetime.now()
            _log(SERVICE, f"SSH disconnected from {hostname}")

    def disconnect_all(self):
        """Close all SSH connections."""
        for hostname in list(self._connections.keys()):
            self.disconnect(hostname)

    def get(self, hostname: str) -> Optional[paramiko.SSHClient]:
        """
        Get an active SSH client for a hostname.
        Returns None if not connected. Does NOT auto-reconnect.
        """
        info = self._connections.get(hostname)
        if not info:
            return None
        with info.lock:
            if info.status == ConnectionStatus.CONNECTED and self._is_alive(info):
                return info.client
        return None

    def get_or_reconnect(self, hostname: str) -> Optional[paramiko.SSHClient]:
        """
        Get an active SSH client, attempting reconnect if disconnected.
        Returns None if reconnect also fails.
        """
        client = self.get(hostname)
        if client:
            return client
        if self.connect(hostname):
            return self.get(hostname)
        return None

    def _is_alive(self, info: ConnectionInfo) -> bool:
        """Check if an SSH connection is still alive."""
        if not info.client:
            return False
        transport = info.client.get_transport()
        if not transport:
            return False
        return transport.is_active()

    def health_check(self) -> dict[str, ConnectionStatus]:
        """
        Check all connections. Returns dict of {hostname: status}.
        Detects lost connections and updates status accordingly.
        Does NOT auto-reconnect — caller should handle reconnection.
        """
        results = {}
        for hostname, info in self._connections.items():
            with info.lock:
                if info.status == ConnectionStatus.CONNECTED:
                    if not self._is_alive(info):
                        info.status = ConnectionStatus.DISCONNECTED
                        info.last_disconnect = datetime.now()
                        info.last_error = "Connection lost (keepalive failed)"
                        _log(SERVICE, f"SSH connection lost to {hostname}",
                             level="WARNING")
                results[hostname] = info.status
        return results

    def should_reconnect(self, hostname: str) -> bool:
        """
        Determine if a reconnect attempt should be made, considering
        cooldown and backoff.
        """
        info = self._connections.get(hostname)
        if not info:
            return False

        if info.status == ConnectionStatus.CONNECTED:
            return False

        if info.status == ConnectionStatus.AUTH_FAILED:
            return False  # Don't retry auth failures automatically

        if info.reconnect_attempts >= self.MAX_RECONNECT_ATTEMPTS:
            # Exponential-ish backoff
            backoff = self.BACKOFF_MULTIPLIER * (info.reconnect_attempts - self.MAX_RECONNECT_ATTEMPTS + 1)
            if info.last_disconnect:
                elapsed = (datetime.now() - info.last_disconnect).total_seconds()
                return elapsed >= backoff
            return True

        if info.last_disconnect:
            elapsed = (datetime.now() - info.last_disconnect).total_seconds()
            return elapsed >= self.RECONNECT_COOLDOWN

        return True

    def reconnect_lost(self) -> dict[str, bool]:
        """
        Attempt to reconnect all lost connections (respecting cooldown/backoff).
        Returns dict of {hostname: success_bool} for attempted reconnections.
        """
        results = {}
        for hostname, info in self._connections.items():
            if info.status in (ConnectionStatus.DISCONNECTED, ConnectionStatus.UNKNOWN):
                if self.should_reconnect(hostname):
                    results[hostname] = self.connect(hostname)
        return results

    def get_status_summary(self) -> dict:
        """Return a summary of all connection states for monitoring/UI."""
        summary = {
            "total": len(self._connections),
            "connected": 0,
            "disconnected": 0,
            "auth_failed": 0,
            "connections": {},
        }
        for hostname, info in self._connections.items():
            if info.status == ConnectionStatus.CONNECTED:
                summary["connected"] += 1
            elif info.status == ConnectionStatus.AUTH_FAILED:
                summary["auth_failed"] += 1
            else:
                summary["disconnected"] += 1
            summary["connections"][hostname] = {
                "status": info.status.value,
                "ip": info.ip,
                "last_connected": info.last_connected.isoformat() if info.last_connected else None,
                "last_disconnect": info.last_disconnect.isoformat() if info.last_disconnect else None,
                "last_error": info.last_error,
                "reconnect_attempts": info.reconnect_attempts,
            }
        return summary

    def hostnames(self) -> list[str]:
        """Return sorted list of all known VPS hostnames."""
        return sorted(self._connections.keys())
