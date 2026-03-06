"""
Async in-memory store for VPS monitoring data.

Central data hub: monitoring tasks write here, WebSocket clients read.
Uses asyncio.Event to wake WebSocket push loops instantly on data change
(no polling interval needed).
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class SystemMetrics:
    """Latest system metrics from a VPS."""
    timestamp: float = 0.0
    cpu: float = 0.0
    mem_total: int = 0
    mem_available: int = 0
    mem_percent: float = 0.0
    mem_used: int = 0
    disk_total: int = 0
    disk_used: int = 0
    disk_free: int = 0
    disk_percent: float = 0.0
    swap_total: int = 0
    swap_used: int = 0
    swap_free: int = 0
    swap_percent: float = 0.0

    @classmethod
    def from_json(cls, data: dict) -> "SystemMetrics":
        """Parse from the remote agent's JSON output."""
        mem = data.get("mem", [0, 0, 0, 0])
        disk = data.get("disk", [0, 0, 0, 0])
        swap = data.get("swap", [0, 0, 0, 0])
        return cls(
            timestamp=data.get("ts", 0.0),
            cpu=data.get("cpu", 0.0),
            mem_total=mem[0] if len(mem) > 0 else 0,
            mem_available=mem[1] if len(mem) > 1 else 0,
            mem_percent=mem[2] if len(mem) > 2 else 0.0,
            mem_used=mem[3] if len(mem) > 3 else 0,
            disk_total=disk[0] if len(disk) > 0 else 0,
            disk_used=disk[1] if len(disk) > 1 else 0,
            disk_free=disk[2] if len(disk) > 2 else 0,
            disk_percent=disk[3] if len(disk) > 3 else 0.0,
            swap_total=swap[0] if len(swap) > 0 else 0,
            swap_used=swap[1] if len(swap) > 1 else 0,
            swap_free=swap[2] if len(swap) > 2 else 0,
            swap_percent=swap[3] if len(swap) > 3 else 0.0,
        )

    def to_dict(self) -> dict:
        return {
            "cpu": self.cpu,
            "mem_total": self.mem_total,
            "mem_available": self.mem_available,
            "mem_percent": self.mem_percent,
            "mem_used": self.mem_used,
            "disk_total": self.disk_total,
            "disk_used": self.disk_used,
            "disk_free": self.disk_free,
            "disk_percent": self.disk_percent,
            "swap_total": self.swap_total,
            "swap_used": self.swap_used,
            "swap_free": self.swap_free,
            "swap_percent": self.swap_percent,
        }


class VPSStore:
    """
    Thread-safe in-memory store for all VPS monitoring data.

    All monitor tasks write here; WebSocket clients read via get_full_state().
    The ``changed`` event is set on every write — WebSocket push loops
    await it for instant delivery.
    """

    def __init__(self):
        # Per-host data
        self.system: dict[str, SystemMetrics] = {}
        self.instances: dict[str, list[dict]] = {}
        self.services: dict[str, dict] = {}
        self.streams: dict[str, dict] = {}  # stream diagnostics per host

        # asyncio.Event — set on every data update, cleared by readers
        self.changed = asyncio.Event()

        # UI settings (persisted in pbgui.ini)
        self._ui_settings: dict[str, str] = {}

    # ── Writers (called by monitor tasks) ───────────────────

    def update_system(self, hostname: str, metrics: SystemMetrics):
        """Update system metrics for a host."""
        self.system[hostname] = metrics
        self.changed.set()

    def update_instances(self, hostname: str, data: list[dict]):
        """Update bot instance data for a host."""
        self.instances[hostname] = data
        self.changed.set()

    def update_services(self, results: dict):
        """Update service check results (all hosts at once)."""
        self.services = results
        self.changed.set()

    def update_stream_info(self, hostname: str, info: dict):
        """Update stream diagnostics for a host."""
        self.streams[hostname] = info
        self.changed.set()

    def remove_host(self, hostname: str):
        """Remove all data for a host."""
        self.system.pop(hostname, None)
        self.instances.pop(hostname, None)
        self.streams.pop(hostname, None)
        # Don't clear services — they're host-keyed inside the dict
        self.services.pop(hostname, None)
        self.changed.set()

    # ── UI settings ─────────────────────────────────────────

    def set_ui_setting(self, key: str, value: str):
        self._ui_settings[key] = value

    def load_ui_settings(self):
        """Load UI settings from pbgui.ini."""
        from pbgui_purefunc import load_ini
        for key in ("compact",):
            val = load_ini("vps_monitor_ui", key)
            if val:
                self._ui_settings[key] = val

    # ── Reader (called by WebSocket push) ───────────────────

    def get_full_state(self, connection_summary: dict,
                       local_logs: list[str]) -> dict:
        """Build the complete state dict, matching the old WSServer format.

        Parameters
        ----------
        connection_summary : dict
            Output of ``AsyncSSHPool.get_status_summary()``.
        local_logs : list[str]
            Log file names in `data/logs/`.
        """
        return {
            "connections": connection_summary,
            "system": {
                h: m.to_dict() for h, m in self.system.items()
            },
            "instances": self.instances,
            "streams": self.streams,
            "services": self.services,
            "local_logs": local_logs,
            "timestamp": time.time(),
            "ui_settings": dict(self._ui_settings),
        }
