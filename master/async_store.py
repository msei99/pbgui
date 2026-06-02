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
    cpu_60s: float = 0.0
    cpu_60s_window: float = 0.0
    cpu_60s_samples: int = 0
    mem_total: int = 0
    mem_available: int = 0
    mem_percent: float = 0.0
    mem_60s_peak: float = 0.0
    mem_60s_window: float = 0.0
    mem_used: int = 0
    disk_total: int = 0
    disk_used: int = 0
    disk_free: int = 0
    disk_percent: float = 0.0
    disk_60s_peak: float = 0.0
    disk_60s_window: float = 0.0
    swap_total: int = 0
    swap_used: int = 0
    swap_free: int = 0
    swap_percent: float = 0.0
    swap_60s_peak: float = 0.0
    swap_60s_window: float = 0.0

    @classmethod
    def from_json(cls, data: dict) -> "SystemMetrics":
        """Parse from the remote agent's JSON output."""
        mem = data.get("mem", [0, 0, 0, 0])
        disk = data.get("disk", [0, 0, 0, 0])
        swap = data.get("swap", [0, 0, 0, 0])
        return cls(
            timestamp=data.get("ts", 0.0),
            cpu=data.get("cpu", 0.0),
            cpu_60s=data.get("cpu_60s", 0.0),
            cpu_60s_window=data.get("cpu_60s_window", 0.0),
            cpu_60s_samples=data.get("cpu_60s_samples", 0),
            mem_total=mem[0] if len(mem) > 0 else 0,
            mem_available=mem[1] if len(mem) > 1 else 0,
            mem_percent=mem[2] if len(mem) > 2 else 0.0,
            mem_60s_peak=data.get("mem_60s_peak", 0.0),
            mem_60s_window=data.get("mem_60s_window", 0.0),
            mem_used=mem[3] if len(mem) > 3 else 0,
            disk_total=disk[0] if len(disk) > 0 else 0,
            disk_used=disk[1] if len(disk) > 1 else 0,
            disk_free=disk[2] if len(disk) > 2 else 0,
            disk_percent=disk[3] if len(disk) > 3 else 0.0,
            disk_60s_peak=data.get("disk_60s_peak", 0.0),
            disk_60s_window=data.get("disk_60s_window", 0.0),
            swap_total=swap[0] if len(swap) > 0 else 0,
            swap_used=swap[1] if len(swap) > 1 else 0,
            swap_free=swap[2] if len(swap) > 2 else 0,
            swap_percent=swap[3] if len(swap) > 3 else 0.0,
            swap_60s_peak=data.get("swap_60s_peak", 0.0),
            swap_60s_window=data.get("swap_60s_window", 0.0),
        )

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "cpu": self.cpu,
            "cpu_60s": self.cpu_60s,
            "cpu_60s_window": self.cpu_60s_window,
            "cpu_60s_samples": self.cpu_60s_samples,
            "mem_total": self.mem_total,
            "mem_available": self.mem_available,
            "mem_percent": self.mem_percent,
            "mem_60s_peak": self.mem_60s_peak,
            "mem_60s_window": self.mem_60s_window,
            "mem_used": self.mem_used,
            "disk_total": self.disk_total,
            "disk_used": self.disk_used,
            "disk_free": self.disk_free,
            "disk_percent": self.disk_percent,
            "disk_60s_peak": self.disk_60s_peak,
            "disk_60s_window": self.disk_60s_window,
            "swap_total": self.swap_total,
            "swap_used": self.swap_used,
            "swap_free": self.swap_free,
            "swap_percent": self.swap_percent,
            "swap_60s_peak": self.swap_60s_peak,
            "swap_60s_window": self.swap_60s_window,
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
        self.v7_instances: dict[str, list[dict]] = {}  # v7 config details per host
        self.host_meta: dict[str, dict] = {}
        self.services: dict[str, dict] = {}
        self.streams: dict[str, dict] = {}  # stream diagnostics per host
        self.bot_logs: dict[str, dict[str, list[str]]] = {}  # old bot log files per host

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

    def update_instances_live(self, hostname: str, bots: list[dict]):
        """Merge live CPU/RSS/Swap from the metrics stream into existing instance entries.

        Only overwrites cpu (``c``), 60s cpu, rss (``m[0]``) and swap (``m[9]``);
        preserves the log-derived counters already collected into the instance snapshot.
        """
        existing = self.instances.get(hostname, [])
        if not existing:
            return
        merge_map = {b["name"]: b for b in bots if b.get("name")}
        for inst in existing:
            name = inst.get("u") or inst.get("name")
            live = merge_map.get(name)
            if not live:
                continue
            inst["c"] = live.get("cpu", inst.get("c", 0))
            inst["cpu_60s"] = live.get("cpu_60s", inst.get("cpu_60s", 0))
            inst["cpu_60s_window"] = live.get("cpu_60s_window", inst.get("cpu_60s_window", 0))
            rss = live.get("rss_mb", 0)
            if rss > 0 and "m" in inst and isinstance(inst["m"], list) and len(inst["m"]) >= 1:
                inst["m"][0] = int(rss * 1024 * 1024)
            swap = live.get("swap_mb", 0)
            if "m" in inst and isinstance(inst["m"], list) and len(inst["m"]) >= 10:
                inst["m"][9] = int(swap * 1024 * 1024)
        self.changed.set()

    def update_v7_instances(self, hostname: str, data: list[dict]):
        """Update v7 instance details (config_version, running_version, enabled_on)."""
        self.v7_instances[hostname] = data
        self.changed.set()

    def update_bot_logs(self, hostname: str, data: dict[str, Any]):
        """Update bot log file listings for a host.

        The collector may return either the legacy flat list format or a
        structured mapping with `errors`/`tracebacks`/`sidebar` groups.
        Store a flat sidebar-friendly view here; callers that need the richer
        structure should keep using the raw collector payload directly.
        """
        normalized: dict[str, list[str]] = {}
        for bot_name, payload in (data or {}).items():
            if isinstance(payload, list):
                normalized[str(bot_name)] = [str(item) for item in payload]
                continue
            if isinstance(payload, dict):
                sidebar = payload.get("sidebar")
                if isinstance(sidebar, list):
                    normalized[str(bot_name)] = [str(item) for item in sidebar]
                    continue
                merged: list[str] = []
                for key in ("errors", "tracebacks"):
                    values = payload.get(key)
                    if isinstance(values, list):
                        merged.extend(str(item) for item in values)
                normalized[str(bot_name)] = merged
        self.bot_logs[hostname] = normalized
        self.changed.set()

    def update_host_meta(self, hostname: str, data: dict):
        """Merge host metadata collected via SSH for a host."""
        current = dict(self.host_meta.get(hostname, {}))
        current.update(data)
        self.host_meta[hostname] = current
        self.changed.set()

    def update_services(self, results: dict):
        """Update service check results (all hosts at once)."""
        self.services = results
        self.changed.set()

    def update_stream_info(self, hostname: str, info: dict):
        """Update stream diagnostics for a host."""
        current = dict(self.streams.get(hostname, {}))
        current.update(info)
        self.streams[hostname] = current
        self.changed.set()

    def remove_host(self, hostname: str):
        """Remove all data for a host."""
        self.system.pop(hostname, None)
        self.instances.pop(hostname, None)
        self.v7_instances.pop(hostname, None)
        self.host_meta.pop(hostname, None)
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
        # debug_logging lives in [vps_monitor] section
        val = load_ini("vps_monitor", "debug_logging")
        if val:
            self._ui_settings["debug_logging"] = val

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
            "v7_instances": self.v7_instances,
            "host_meta": self.host_meta,
            "streams": self.streams,
            "services": self.services,
            "bot_logs": self.bot_logs,
            "local_logs": local_logs,
            "timestamp": time.time(),
            "ui_settings": dict(self._ui_settings),
        }
