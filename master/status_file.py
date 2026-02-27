"""
Status File IPC for PBMaster.

The PBMaster daemon runs in a separate process. The Streamlit UI cannot
access its in-memory data directly. This module provides a JSON file-based
IPC mechanism:

    - Daemon side: `write_status()` serializes realtime data + connection
      status to `data/pbmaster_status.json` every loop iteration.
    - UI side: `read_status()` reads and deserializes the JSON file.

The status file contains:
    - connections: per-host SSH status (connected/disconnected, IP, errors)
    - system: per-host system metrics (CPU, RAM, Disk, Swap)
    - instances: per-host bot instance data
    - services: per-host service check results
    - streams: per-host stream alive status
    - timestamp: when the status was last written
"""

import json
import os
import time
from dataclasses import asdict
from pathlib import Path
from typing import Optional

from pbgui_purefunc import PBGDIR
from master.realtime_collector import SystemMetrics


STATUS_FILE = Path(f'{PBGDIR}/data/pbmaster_status.json')


def write_status(
    connections: dict,
    system_data: dict[str, "SystemMetrics"],
    instance_data: dict[str, list[dict]],
    stream_info: dict[str, dict],
    service_results: Optional[dict] = None,
):
    """
    Write current PBMaster state to status file (called from daemon loop).

    Uses atomic write (write to temp, rename) to avoid partial reads.
    """
    status = {
        "timestamp": time.time(),
        "connections": connections,
        "system": {},
        "instances": instance_data or {},
        "streams": stream_info or {},
        "services": service_results or {},
    }

    # Serialize SystemMetrics dataclasses
    for hostname, metrics in (system_data or {}).items():
        if metrics and hasattr(metrics, 'timestamp'):
            status["system"][hostname] = {
                "timestamp": metrics.timestamp,
                "cpu": metrics.cpu,
                "mem_total": metrics.mem_total,
                "mem_available": metrics.mem_available,
                "mem_percent": metrics.mem_percent,
                "mem_used": metrics.mem_used,
                "disk_total": metrics.disk_total,
                "disk_used": metrics.disk_used,
                "disk_free": metrics.disk_free,
                "disk_percent": metrics.disk_percent,
                "swap_total": metrics.swap_total,
                "swap_used": metrics.swap_used,
                "swap_free": metrics.swap_free,
                "swap_percent": metrics.swap_percent,
            }

    # Atomic write
    tmp_path = str(STATUS_FILE) + ".tmp"
    try:
        STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(tmp_path, 'w', encoding='utf-8') as f:
            json.dump(status, f, default=str)
        os.replace(tmp_path, STATUS_FILE)
    except Exception:
        # Best-effort; don't crash the daemon
        try:
            os.unlink(tmp_path)
        except Exception:
            pass


def read_status() -> Optional[dict]:
    """
    Read PBMaster status from file (called from Streamlit UI).

    Returns None if file doesn't exist or is stale (> 60s old).
    Returns parsed dict with system metrics converted back to SystemMetrics.
    """
    try:
        if not STATUS_FILE.exists():
            return None
        with open(STATUS_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Check staleness
        ts = data.get("timestamp", 0)
        if time.time() - ts > 60:
            return None

        # Convert system dicts back to SystemMetrics objects
        system = {}
        for hostname, metrics_dict in data.get("system", {}).items():
            system[hostname] = SystemMetrics(
                timestamp=metrics_dict.get("timestamp", 0),
                cpu=metrics_dict.get("cpu", 0),
                mem_total=metrics_dict.get("mem_total", 0),
                mem_available=metrics_dict.get("mem_available", 0),
                mem_percent=metrics_dict.get("mem_percent", 0),
                mem_used=metrics_dict.get("mem_used", 0),
                disk_total=metrics_dict.get("disk_total", 0),
                disk_used=metrics_dict.get("disk_used", 0),
                disk_free=metrics_dict.get("disk_free", 0),
                disk_percent=metrics_dict.get("disk_percent", 0),
                swap_total=metrics_dict.get("swap_total", 0),
                swap_used=metrics_dict.get("swap_used", 0),
                swap_free=metrics_dict.get("swap_free", 0),
                swap_percent=metrics_dict.get("swap_percent", 0),
            )
        data["system"] = system

        return data

    except (json.JSONDecodeError, OSError):
        return None


def status_age() -> float:
    """Return age of status file in seconds, or -1 if not found."""
    try:
        if not STATUS_FILE.exists():
            return -1
        with open(STATUS_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return time.time() - data.get("timestamp", 0)
    except Exception:
        return -1
