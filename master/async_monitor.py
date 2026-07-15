"""
Async VPS Monitor — system metrics, instances, services, alerts.

All coroutines run on the FastAPI event loop.  No threads, no paramiko.
Uses asyncssh via ``AsyncSSHPool`` for all SSH operations.
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import re
import socket
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from pbgui_release import read_local_pbgui_version
from typing import Any, Optional

import asyncssh

from MonitorConfig import MonitorConfig
from pbgui_purefunc import IniSnapshot, PBGDIR, load_ini, load_ini_snapshot, save_ini, update_ini
from logging_helpers import human_log as _log
from ini_watcher import IniWatcher
from master.async_pool import AsyncSSHPool, ConnectionStatus, remote_path_join, remote_shell_path
from master.async_store import VPSStore, SystemMetrics

SERVICE = "VPSMonitor"

# ── Constants ───────────────────────────────────────────────

LOOP_INTERVAL = 15          # seconds between main loop iterations
SERVICE_CHECK_EVERY = 4     # every N iterations (= 60s at 15s)
INSTANCE_COLLECT_INTERVAL = 30  # seconds
HOST_META_INTERVAL = 10     # seconds
PACKAGE_STATUS_INTERVAL = 3600  # seconds
METRICS_STREAM_STARTUP_GRACE_SECONDS = 30.0
METRICS_STREAM_STALE_SECONDS = 45.0
METRICS_STREAM_RECONNECT_AFTER_STALE_RESTARTS = 2
METRICS_STREAM_STALE_LOG_INTERVAL = 60.0
MONITOR_CACHE_VERSION = 2
STATE_SNAPSHOT_VERSION = 1
CPU_HISTORY_VERSION = 1
CPU_HISTORY_WINDOW_MINUTES = 24 * 60
CPU_HISTORY_STEP_SECONDS = 60
CPU_HISTORY_RESOLUTION_PCT = 0.5
CPU_HISTORY_MAX_PCT = 127.0
CPU_HISTORY_FLUSH_INTERVAL = 10.0
CPU_ALERT_SUSTAINED_SECONDS = 60.0
COUNT_HISTORY_VERSION = 1
COUNT_HISTORY_WINDOW_HOURS = 24 * 28
COUNT_HISTORY_STEP_SECONDS = 3600
BOT_MEMORY_HISTORY_RESOLUTION_MB = 2.0
BOT_MEMORY_HISTORY_MAX_MB = 32766.0
BOT_SWAP_HISTORY_RESOLUTION_MB = 2.0
BOT_SWAP_HISTORY_MAX_MB = 32766.0
HOST_HISTORY_SOURCES = {
    "cpu": "cpu_60s",
    "memory": "mem_percent",
    "disk": "disk_percent",
    "swap": "swap_percent",
}
BOT_HISTORY_SOURCES = {
    "cpu": "cpu_60s",
    "memory": "rss_mb",
    "swap": "swap_mb",
    "errors": "passivbot.log",
    "tracebacks": "passivbot_err.log",
    "pnl": "passivbot.log",
}

PNL_HISTORY_VERSION = 1
ALERT_STATE_VERSION = 1
ALERT_HISTORY_RETENTION_SECONDS = 7 * 24 * 60 * 60

ALERT_KIND_OFFLINE = "offline"
ALERT_KIND_SERVICE = "service"
ALERT_KIND_SYSTEM = "system"
ALERT_KIND_INSTANCE = "instance"

ALERT_ROUTE_GUI_DEFAULTS = {
    ALERT_KIND_OFFLINE: True,
    ALERT_KIND_SERVICE: True,
    ALERT_KIND_SYSTEM: True,
    ALERT_KIND_INSTANCE: True,
}

ALERT_ROUTE_TELEGRAM_DEFAULTS = {
    "ssh_lost": True,
    "ssh_recovered": True,
    "service_down": True,
    "service_restart_started": True,
    "service_recovered": True,
    "system_problem": True,
    "system_recovered": True,
    "instance_problem": True,
    "instance_recovered": True,
}

ALERT_ROUTE_GUI_KEYS = {
    ALERT_KIND_OFFLINE: "offline_gui",
    ALERT_KIND_SERVICE: "service_gui",
    ALERT_KIND_SYSTEM: "system_gui",
    ALERT_KIND_INSTANCE: "instance_gui",
}

ALERT_ROUTE_TELEGRAM_KEYS = {
    "ssh_lost": "ssh_lost_telegram",
    "ssh_recovered": "ssh_recovered_telegram",
    "service_down": "service_down_telegram",
    "service_restart_started": "service_restart_started_telegram",
    "service_recovered": "service_recovered_telegram",
    "system_problem": "system_problem_telegram",
    "system_recovered": "system_recovered_telegram",
    "instance_problem": "instance_problem_telegram",
    "instance_recovered": "instance_recovered_telegram",
}

MONITOR_DEFAULTS = {
    "mem_warning_server": 50.0,
    "mem_error_server": 25.0,
    "swap_warning_server": 150.0,
    "swap_error_server": 100.0,
    "disk_warning_server": 500.0,
    "disk_error_server": 250.0,
    "cpu_warning_server": 80.0,
    "cpu_error_server": 95.0,
    "mem_warning_v7": 250.0,
    "mem_error_v7": 500.0,
    "swap_warning_v7": 250.0,
    "swap_error_v7": 500.0,
    "cpu_warning_v7": 10.0,
    "cpu_error_v7": 15.0,
    "error_warning_v7": 100.0,
    "error_error_v7": 250.0,
    "traceback_warning_v7": 100.0,
    "traceback_error_v7": 250.0,
}


@dataclass(frozen=True)
class VPSMonitorConfigCandidate:
    """Fully validated runtime settings from one INI generation."""

    signature: Any
    enabled_hosts: frozenset[str]
    auto_restart: bool
    debug_logging: bool
    telegram_token: str
    telegram_chat_id: str
    alert_gui_routes: dict[str, bool]
    alert_telegram_routes: dict[str, bool]
    monitor_values: dict[str, float]
    ui_settings: dict[str, str]


def _read_ini_bool(section: str, key: str, default: bool) -> bool:
    raw = str(load_ini(section, key) or "").strip().lower()
    if not raw:
        return bool(default)
    return raw in {"1", "true", "yes", "on"}


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.parent.mkdir(parents=True, exist_ok=True)
    tmp.write_text(json.dumps(payload, indent=4), encoding="utf-8")
    tmp.replace(path)


def _alert_id(kind: str, host: str, name: str = "") -> str:
    base = f"{kind}:{host}"
    return f"{base}:{name}" if name else base


def _alert_history_id(alert: AlertRecord | dict[str, Any], episode: int | None = None) -> str:
    if isinstance(alert, AlertRecord):
        kind = alert.kind
        host = alert.host
        name = alert.name
        current_episode = int(alert.episode or 1)
    else:
        kind = str(alert.get("kind") or "")
        host = str(alert.get("host") or "")
        name = str(alert.get("name") or "")
        current_episode = int(alert.get("episode") or 1)
    base = _alert_id(kind, host, name)
    return f"{base}#episode:{max(1, int(episode or current_episode))}"


def _severity_rank(level: str) -> int:
    return {"critical": 3, "error": 2, "warning": 1}.get(str(level or "").lower(), 0)


def _bool_to_ini(value: bool) -> str:
    return "true" if value else "false"


@dataclass
class AlertRecord:
    id: str
    kind: str
    host: str
    name: str
    severity: str
    summary: str
    details: str
    was_restarted: bool = False
    triggered_thresholds: list[str] | None = None
    active: bool = True
    acknowledged: bool = False
    first_seen_ts: float = 0.0
    last_seen_ts: float = 0.0
    episode: int = 1

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "kind": self.kind,
            "host": self.host,
            "name": self.name,
            "severity": self.severity,
            "summary": self.summary,
            "details": self.details,
            "was_restarted": bool(self.was_restarted),
            "triggered_thresholds": list(self.triggered_thresholds or []),
            "active": bool(self.active),
            "acknowledged": bool(self.acknowledged),
            "first_seen_ts": float(self.first_seen_ts or 0.0),
            "last_seen_ts": float(self.last_seen_ts or 0.0),
            "episode": int(self.episode or 1),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "AlertRecord":
        return cls(
            id=str(payload.get("id") or ""),
            kind=str(payload.get("kind") or ""),
            host=str(payload.get("host") or ""),
            name=str(payload.get("name") or ""),
            severity=str(payload.get("severity") or "warning"),
            summary=str(payload.get("summary") or ""),
            details=str(payload.get("details") or ""),
            was_restarted=bool(payload.get("was_restarted", False)),
            triggered_thresholds=[str(item) for item in (payload.get("triggered_thresholds") or []) if str(item).strip()],
            active=bool(payload.get("active", True)),
            acknowledged=bool(payload.get("acknowledged", False)),
            first_seen_ts=float(payload.get("first_seen_ts") or 0.0),
            last_seen_ts=float(payload.get("last_seen_ts") or 0.0),
            episode=max(1, int(payload.get("episode") or 1)),
        )


def _utc_day_from_ts(ts_val: int) -> int:
    return int(ts_val // 86400)


def _system_threshold_labels(thresholds: list[str] | None) -> str:
    label_map = {
        "memory": "memory free",
        "swap": "swap free",
        "disk": "disk free",
        "cpu": "cpu",
    }
    items = [
        label_map.get(str(item).strip().lower(), str(item).strip().lower())
        for item in (thresholds or [])
        if str(item).strip()
    ]
    return ", ".join(items) if items else "system"


def _iso_day_label(day: int) -> str:
    if day <= 0:
        return "n/a"
    return datetime.fromtimestamp(day * 86400, timezone.utc).strftime("%Y-%m-%d")


def _parse_log_timestamp(line: str) -> int | None:
    match = re.match(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})Z', str(line or ''))
    if not match:
        return None
    try:
        return int(datetime.strptime(match.group(1), '%Y-%m-%dT%H:%M:%S').replace(tzinfo=timezone.utc).timestamp())
    except Exception:
        return None


def _extract_fill_summary(line: str) -> tuple[float, int] | None:
    text = str(line or '')
    if '[fill]' not in text:
        return None
    summary = FILL_SUMMARY_RE.search(text)
    if summary:
        return float(summary.group(2)), int(summary.group(1))
    pnl_match = FILL_PNL_RE.search(text)
    if pnl_match:
        return float(pnl_match.group(1)), 1
    return None


def _count_hourly_log_occurrences(lines: list[str], *, needle: str) -> dict[int, int]:
    buckets: dict[int, int] = {}
    for line in lines or []:
        if needle not in line:
            continue
        match = re.match(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})Z', str(line))
        if not match:
            continue
        try:
            ts_val = int(datetime.strptime(match.group(1), '%Y-%m-%dT%H:%M:%S').replace(tzinfo=timezone.utc).timestamp())
        except Exception:
            continue
        hour = ts_val // COUNT_HISTORY_STEP_SECONDS
        buckets[hour] = buckets.get(hour, 0) + 1
    return buckets


def _shell_quote(value: str) -> str:
    return "'" + str(value or '').replace("'", "'\"'\"'") + "'"


def _metrics_last_update(stream: dict[str, Any], metrics: dict[str, Any]) -> float:
    values: list[float] = []
    for raw in (stream.get("last_update"), metrics.get("timestamp")):
        try:
            value = float(raw or 0.0)
        except (TypeError, ValueError):
            value = 0.0
        if value > 0:
            values.append(value)
    return max(values) if values else 0.0


def _metrics_stale_age(stream: dict[str, Any], metrics: dict[str, Any], *, now: float | None = None) -> float | None:
    now = float(now or time.time())
    last_update = _metrics_last_update(stream, metrics)
    if last_update <= 0:
        return METRICS_STREAM_STALE_SECONDS + 1 if stream.get("stale") else None
    age = max(now - last_update, 0.0)
    if stream.get("stale") or age > METRICS_STREAM_STALE_SECONDS:
        return age
    return None


def collect_live_alerts(connections: dict[str, Any], system_state: dict[str, Any],
                        instances: dict[str, Any], services: dict[str, Any],
                        monitor_config, streams: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    alerts: list[dict[str, Any]] = []
    streams = streams or {}

    for hostname in sorted({*connections.keys(), *system_state.keys(), *instances.keys(), *services.keys()}):
        conn = connections.get(hostname) or {}
        status = str(conn.get("status") or "")
        if status and status != ConnectionStatus.CONNECTED.value:
            alerts.append({
                "id": _alert_id(ALERT_KIND_OFFLINE, hostname),
                "kind": ALERT_KIND_OFFLINE,
                "host": hostname,
                "name": "offline",
                "severity": "critical",
                "summary": f"SSH connection lost to {hostname}",
                "details": f"Host {hostname} is currently disconnected from the VPS monitor.",
            })
            continue

        host_services = services.get(hostname) or {}
        for svc_name, check in sorted(host_services.items()):
            if (check or {}).get("expected") is False:
                continue
            status_val = str((check or {}).get("status") or "")
            if status_val != ServiceStatus.STOPPED.value:
                continue
            was_restarted = bool((check or {}).get("was_restarted"))
            summary = f"{svc_name} is down on {hostname}"
            details = summary + (" Restart was initiated." if was_restarted else ".")
            alerts.append({
                "id": _alert_id(ALERT_KIND_SERVICE, hostname, svc_name),
                "kind": ALERT_KIND_SERVICE,
                "host": hostname,
                "name": svc_name,
                "severity": "error",
                "summary": summary,
                "details": details,
                "was_restarted": was_restarted,
            })

        metrics = system_state.get(hostname) or {}
        metrics_stale = _metrics_stale_age(streams.get(hostname) or {}, metrics) is not None
        mem_available_mb = float(metrics.get('mem_available') or 0) / 1024 / 1024
        swap_free_mb = float(metrics.get('swap_free') or 0) / 1024 / 1024
        disk_free_mb = float(metrics.get('disk_free') or 0) / 1024 / 1024
        cpu_live = float(metrics.get('cpu') or 0)
        cpu_60s = float(metrics.get('cpu_60s') or 0)
        cpu_60s_window = float(metrics.get('cpu_60s_window') or 0)
        cpu_threshold_duration = float(metrics.get('cpu_threshold_duration') or 0)
        cpu_confirmed = cpu_60s_window >= 60
        cpu_sustained = cpu_live >= monitor_config.cpu_error_server and cpu_threshold_duration >= CPU_ALERT_SUSTAINED_SECONDS
        cpu = cpu_60s if cpu_confirmed else cpu_live
        has_system_metrics = any(metrics.get(key) not in (None, 0, 0.0) for key in ('mem_total', 'disk_total', 'swap_total', 'timestamp'))
        if has_system_metrics and not metrics_stale and (
            mem_available_mb <= monitor_config.mem_error_server
            or swap_free_mb <= monitor_config.swap_error_server
            or disk_free_mb <= monitor_config.disk_error_server
            or cpu_sustained
        ):
            triggered_thresholds: list[str] = []
            triggered_parts: list[str] = []
            if mem_available_mb <= monitor_config.mem_error_server:
                triggered_thresholds.append("memory")
                triggered_parts.append(
                    f"memory free {int(mem_available_mb)}MB <= {int(monitor_config.mem_error_server)}MB"
                )
            if swap_free_mb <= monitor_config.swap_error_server:
                triggered_thresholds.append("swap")
                triggered_parts.append(
                    f"swap free {int(swap_free_mb)}MB <= {int(monitor_config.swap_error_server)}MB"
                )
            if disk_free_mb <= monitor_config.disk_error_server:
                triggered_thresholds.append("disk")
                triggered_parts.append(
                    f"disk free {int(disk_free_mb)}MB <= {int(monitor_config.disk_error_server)}MB"
                )
            if cpu_sustained:
                triggered_thresholds.append("cpu")
                triggered_parts.append(
                    f"CPU {cpu_live:.1f}% >= {monitor_config.cpu_error_server:.1f}% for {int(cpu_threshold_duration)}s"
                )
            current_text = (
                f"Current: mem free {int(mem_available_mb)}MB, swap free {int(swap_free_mb)}MB, "
                f"disk free {int(disk_free_mb)}MB, CPU {cpu_live:.1f}%"
            )
            if cpu_confirmed:
                current_text += f", CPU 60s avg {cpu_60s:.1f}%"
            details = f"Triggered: {'; '.join(triggered_parts)}\n{current_text}"
            threshold_label = _system_threshold_labels(triggered_thresholds)
            alerts.append({
                "id": _alert_id(ALERT_KIND_SYSTEM, hostname),
                "kind": ALERT_KIND_SYSTEM,
                "host": hostname,
                "name": "system",
                "severity": "error",
                "summary": (
                    f"System threshold exceeded on {hostname}: {threshold_label}"
                    if len(triggered_thresholds) == 1
                    else f"System thresholds exceeded on {hostname}: {threshold_label}"
                ),
                "details": details,
                "triggered_thresholds": triggered_thresholds,
            })

        for idx, monitor in enumerate(instances.get(hostname) or [], start=1):
            metrics_list = monitor.get('m') or []
            swap_value = metrics_list[9] / 1024 / 1024 if len(metrics_list) == 10 and metrics_list[9] else 0.0
            memory_mb = metrics_list[0] / 1024 / 1024 if metrics_list else 0.0
            cpu_live = float(monitor.get('c') or 0)
            cpu_60s = float(monitor.get('cpu_60s') or 0)
            cpu_60s_window = float(monitor.get('cpu_60s_window') or 0)
            cpu_confirmed = cpu_60s_window >= 60
            cpu_value = cpu_60s if cpu_confirmed else cpu_live
            errors_today = int(monitor.get('et') or 0)
            tracebacks_today = int(monitor.get('tt') or 0)
            if (
                memory_mb > monitor_config.mem_error_v7
                or swap_value > monitor_config.swap_error_v7
                or (not metrics_stale and cpu_confirmed and cpu_value > monitor_config.cpu_error_v7)
                or errors_today > monitor_config.error_error_v7
                or tracebacks_today > monitor_config.traceback_error_v7
            ):
                bot_name = str(monitor.get("u") or monitor.get("name") or f"unknown-{idx}")
                details = (
                    f"Memory {round(memory_mb, 1)}MB, swap {round(swap_value, 1)}MB, "
                    f"CPU {cpu_value:.1f}%, errors today {errors_today}, tracebacks today {tracebacks_today}"
                )
                alerts.append({
                    "id": _alert_id(ALERT_KIND_INSTANCE, hostname, bot_name),
                    "kind": ALERT_KIND_INSTANCE,
                    "host": hostname,
                    "name": bot_name,
                    "severity": "error",
                    "summary": f"Instance thresholds exceeded for {bot_name} on {hostname}",
                    "details": details,
                })
    return alerts


def _cpu_history_encode(value: Any, *, confirmed: bool) -> int:
    if not confirmed:
        return 0
    try:
        pct = float(value)
    except Exception:
        return 0
    pct = max(0.0, min(pct, CPU_HISTORY_MAX_PCT))
    return max(1, min(255, int(round(pct / CPU_HISTORY_RESOLUTION_PCT)) + 1))


def _cpu_history_decode(value: int) -> float | None:
    try:
        encoded = int(value)
    except Exception:
        return None
    if encoded <= 0:
        return None
    return round((encoded - 1) * CPU_HISTORY_RESOLUTION_PCT, 1)


def _bot_metric_history_encode(value: Any, *, confirmed: bool, resolution: float, max_value: float) -> int:
    if not confirmed:
        return 0
    try:
        numeric = float(value)
    except Exception:
        return 0
    numeric = max(0.0, min(numeric, max_value))
    return max(1, min(255, int(round(numeric / resolution)) + 1))


def _bot_metric_history_decode(value: int, *, resolution: float) -> float | None:
    try:
        encoded = int(value)
    except Exception:
        return None
    if encoded <= 0:
        return None
    return round((encoded - 1) * resolution, 1)


class CpuHistoryStore:
    """Compact 24h per-minute CPU history persisted as a binary ringbuffer."""

    def __init__(self, root_dir: Path, stem: str):
        self.root_dir = root_dir
        self.bin_path = root_dir / f"{stem}.bin"
        self.index_path = root_dir / f"{stem}_index.json"
        self._series: dict[str, bytearray] = {}
        self._meta: dict[str, dict[str, int]] = {}
        self._next_slot = 0
        self._loaded = False
        self._dirty = False
        self._last_flush_ts = 0.0

    def load(self) -> None:
        if self._loaded:
            return
        self.root_dir.mkdir(parents=True, exist_ok=True)
        raw_index: dict[str, Any] = {}
        raw_binary = b""
        try:
            if self.index_path.exists():
                loaded = json.loads(self.index_path.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    raw_index = loaded
        except Exception as exc:
            _log(SERVICE, f"[history] Failed to load {self.index_path.name}: {exc}", level="WARNING")
        try:
            if self.bin_path.exists():
                raw_binary = self.bin_path.read_bytes()
        except Exception as exc:
            _log(SERVICE, f"[history] Failed to load {self.bin_path.name}: {exc}", level="WARNING")

        series_meta = raw_index.get("series") or {}
        if not isinstance(series_meta, dict):
            series_meta = {}
        for key, meta in series_meta.items():
            if not isinstance(key, str) or not isinstance(meta, dict):
                continue
            slot = int(meta.get("slot") or 0)
            head = int(meta.get("head") or 0)
            last_minute = int(meta.get("last_minute") or 0)
            if slot < 0 or head < 0 or head >= CPU_HISTORY_WINDOW_MINUTES:
                continue
            start = slot * CPU_HISTORY_WINDOW_MINUTES
            end = start + CPU_HISTORY_WINDOW_MINUTES
            buf = bytearray(CPU_HISTORY_WINDOW_MINUTES)
            chunk = raw_binary[start:end]
            if chunk:
                buf[: min(len(chunk), CPU_HISTORY_WINDOW_MINUTES)] = chunk[:CPU_HISTORY_WINDOW_MINUTES]
            self._series[key] = buf
            self._meta[key] = {
                "slot": slot,
                "head": head,
                "last_minute": max(last_minute, 0),
            }
            self._next_slot = max(self._next_slot, slot + 1)
        self._loaded = True

    def _ensure_series(self, key: str) -> tuple[bytearray, dict[str, int]]:
        self.load()
        meta = self._meta.get(key)
        if meta is None:
            meta = {
                "slot": self._next_slot,
                "head": 0,
                "last_minute": 0,
            }
            self._next_slot += 1
            self._meta[key] = meta
            self._series[key] = bytearray(CPU_HISTORY_WINDOW_MINUTES)
            self._dirty = True
        return self._series[key], meta

    def record(self, key: str, *, minute: int, value: Any, confirmed: bool,
               same_minute_mode: str = "replace") -> None:
        key = str(key or "").strip()
        if not key:
            return
        if minute <= 0:
            minute = int(time.time() // CPU_HISTORY_STEP_SECONDS)
        buf, meta = self._ensure_series(key)
        encoded = _cpu_history_encode(value, confirmed=confirmed)
        changed = False
        meta_changed = False
        last_minute = int(meta.get("last_minute") or 0)

        if last_minute <= 0:
            meta["head"] = 0
            meta["last_minute"] = minute
            buf[:] = b"\x00" * CPU_HISTORY_WINDOW_MINUTES
            buf[0] = encoded
            meta_changed = True
            changed = True
        elif minute < last_minute:
            return
        elif minute == last_minute:
            head = int(meta.get("head") or 0)
            if encoded <= 0 < int(buf[head]):
                return
            next_value = encoded
            if same_minute_mode == "peak":
                next_value = max(int(buf[head]), encoded)
            if buf[head] != next_value:
                buf[head] = next_value
                changed = True
        else:
            delta = minute - last_minute
            if delta >= CPU_HISTORY_WINDOW_MINUTES:
                buf[:] = b"\x00" * CPU_HISTORY_WINDOW_MINUTES
                meta["head"] = 0
                meta["last_minute"] = minute
                buf[0] = encoded
                meta_changed = True
                changed = True
            else:
                head = int(meta.get("head") or 0)
                for _ in range(delta):
                    head = (head + 1) % CPU_HISTORY_WINDOW_MINUTES
                    if buf[head] != 0:
                        buf[head] = 0
                        changed = True
                meta["head"] = head
                meta["last_minute"] = minute
                meta_changed = True
                if buf[head] != encoded:
                    buf[head] = encoded
                    changed = True
        if changed or meta_changed:
            self._dirty = True

    def build_payload(self, key: str, *, hostname: str, bot_name: str = "",
                      metric: str = "cpu", source: str = "cpu_60s",
                      end_minute: int | None = None) -> dict[str, Any]:
        key = str(key or "").strip()
        self.load()
        if end_minute is None or end_minute <= 0:
            end_minute = int(time.time() // CPU_HISTORY_STEP_SECONDS)
        start_minute = end_minute - CPU_HISTORY_WINDOW_MINUTES + 1
        meta = self._meta.get(key)
        buf = self._series.get(key)
        points: list[float | None] = []
        last_minute = int((meta or {}).get("last_minute") or 0)
        head = int((meta or {}).get("head") or 0)
        for minute in range(start_minute, end_minute + 1):
            if not meta or buf is None or last_minute <= 0:
                points.append(None)
                continue
            distance = last_minute - minute
            if distance < 0 or distance >= CPU_HISTORY_WINDOW_MINUTES:
                points.append(None)
                continue
            slot = (head - distance) % CPU_HISTORY_WINDOW_MINUTES
            points.append(_cpu_history_decode(buf[slot]))
        return {
            "available": True,
            "scope": "bot" if bot_name else "host",
            "metric": metric,
            "hostname": hostname,
            "bot_name": bot_name,
            "source": source,
            "step_seconds": CPU_HISTORY_STEP_SECONDS,
            "window_minutes": CPU_HISTORY_WINDOW_MINUTES,
            "resolution_pct": CPU_HISTORY_RESOLUTION_PCT,
            "start_minute": start_minute,
            "end_minute": end_minute,
            "last_minute": last_minute,
            "series_exists": meta is not None,
            "points": points,
        }

    def maybe_flush(self, *, force: bool = False, now_ts: float | None = None) -> None:
        self.load()
        if not self._dirty:
            return
        now_ts = float(now_ts or time.time())
        if not force and (now_ts - self._last_flush_ts) < CPU_HISTORY_FLUSH_INTERVAL:
            return
        self._flush()
        self._last_flush_ts = now_ts
        self._dirty = False

    def _flush(self) -> None:
        self.root_dir.mkdir(parents=True, exist_ok=True)
        slot_count = max(self._next_slot, 0)
        payload = bytearray(slot_count * CPU_HISTORY_WINDOW_MINUTES)
        for key, meta in self._meta.items():
            slot = int(meta.get("slot") or 0)
            start = slot * CPU_HISTORY_WINDOW_MINUTES
            end = start + CPU_HISTORY_WINDOW_MINUTES
            buf = self._series.get(key) or bytearray(CPU_HISTORY_WINDOW_MINUTES)
            payload[start:end] = bytes(buf[:CPU_HISTORY_WINDOW_MINUTES])
        index_payload = {
            "version": CPU_HISTORY_VERSION,
            "window_minutes": CPU_HISTORY_WINDOW_MINUTES,
            "step_seconds": CPU_HISTORY_STEP_SECONDS,
            "series": {
                key: {
                    "slot": int(meta.get("slot") or 0),
                    "head": int(meta.get("head") or 0),
                    "last_minute": int(meta.get("last_minute") or 0),
                }
                for key, meta in sorted(self._meta.items())
            },
        }
        tmp_bin = self.bin_path.with_suffix(".bin.tmp")
        tmp_json = self.index_path.with_suffix(".json.tmp")
        tmp_bin.write_bytes(bytes(payload))
        tmp_bin.replace(self.bin_path)
        tmp_json.write_text(json.dumps(index_payload, indent=4), encoding="utf-8")
        tmp_json.replace(self.index_path)


class BotMetricHistoryStore(CpuHistoryStore):
    """Compact 24h per-minute bot metric history for MB-based metrics."""

    def __init__(self, root_dir: Path, stem: str, *, resolution: float, max_value: float):
        super().__init__(root_dir, stem)
        self._resolution = float(resolution)
        self._max_value = float(max_value)

    def record(self, key: str, *, minute: int, value: Any, confirmed: bool,
               same_minute_mode: str = "replace") -> None:
        key = str(key or "").strip()
        if not key:
            return
        if minute <= 0:
            minute = int(time.time() // CPU_HISTORY_STEP_SECONDS)
        buf, meta = self._ensure_series(key)
        encoded = _bot_metric_history_encode(
            value,
            confirmed=confirmed,
            resolution=self._resolution,
            max_value=self._max_value,
        )
        changed = False
        meta_changed = False
        last_minute = int(meta.get("last_minute") or 0)

        if last_minute <= 0:
            meta["head"] = 0
            meta["last_minute"] = minute
            buf[:] = b"\x00" * CPU_HISTORY_WINDOW_MINUTES
            buf[0] = encoded
            meta_changed = True
            changed = True
        elif minute < last_minute:
            return
        elif minute == last_minute:
            head = int(meta.get("head") or 0)
            next_value = encoded
            if same_minute_mode == "peak":
                next_value = max(int(buf[head]), encoded)
            if buf[head] != next_value:
                buf[head] = next_value
                changed = True
        else:
            delta = minute - last_minute
            if delta >= CPU_HISTORY_WINDOW_MINUTES:
                buf[:] = b"\x00" * CPU_HISTORY_WINDOW_MINUTES
                meta["head"] = 0
                meta["last_minute"] = minute
                buf[0] = encoded
                meta_changed = True
                changed = True
            else:
                head = int(meta.get("head") or 0)
                for _ in range(delta):
                    head = (head + 1) % CPU_HISTORY_WINDOW_MINUTES
                    if buf[head] != 0:
                        buf[head] = 0
                        changed = True
                meta["head"] = head
                meta["last_minute"] = minute
                meta_changed = True
                if buf[head] != encoded:
                    buf[head] = encoded
                    changed = True
        if changed or meta_changed:
            self._dirty = True

    def build_payload(self, key: str, *, hostname: str, bot_name: str = "",
                      metric: str = "memory", source: str = "rss_mb",
                      end_minute: int | None = None) -> dict[str, Any]:
        key = str(key or "").strip()
        self.load()
        if end_minute is None or end_minute <= 0:
            end_minute = int(time.time() // CPU_HISTORY_STEP_SECONDS)
        start_minute = end_minute - CPU_HISTORY_WINDOW_MINUTES + 1
        meta = self._meta.get(key)
        buf = self._series.get(key)
        points: list[float | None] = []
        last_minute = int((meta or {}).get("last_minute") or 0)
        head = int((meta or {}).get("head") or 0)
        for minute in range(start_minute, end_minute + 1):
            if not meta or buf is None or last_minute <= 0:
                points.append(None)
                continue
            distance = last_minute - minute
            if distance < 0 or distance >= CPU_HISTORY_WINDOW_MINUTES:
                points.append(None)
                continue
            slot = (head - distance) % CPU_HISTORY_WINDOW_MINUTES
            points.append(_bot_metric_history_decode(buf[slot], resolution=self._resolution))
        return {
            "available": True,
            "scope": "bot",
            "metric": metric,
            "hostname": hostname,
            "bot_name": bot_name,
            "source": source,
            "unit": "MB",
            "step_seconds": CPU_HISTORY_STEP_SECONDS,
            "window_minutes": CPU_HISTORY_WINDOW_MINUTES,
            "resolution_mb": self._resolution,
            "start_minute": start_minute,
            "end_minute": end_minute,
            "last_minute": last_minute,
            "series_exists": meta is not None,
            "points": points,
        }


class BotCountHistoryStore:
    """Persistent 4-week per-hour bot counter history with daily UTC aggregates."""

    def __init__(self, root_dir: Path, stem: str):
        self.root_dir = root_dir
        self.bin_path = root_dir / f"{stem}.bin"
        self.index_path = root_dir / f"{stem}_index.json"
        self._series: dict[str, bytearray] = {}
        self._meta: dict[str, dict[str, int]] = {}
        self._next_slot = 0
        self._loaded = False
        self._dirty = False
        self._last_flush_ts = 0.0

    def load(self) -> None:
        if self._loaded:
            return
        self.root_dir.mkdir(parents=True, exist_ok=True)
        raw_index: dict[str, Any] = {}
        raw_binary = b""
        try:
            if self.index_path.exists():
                loaded = json.loads(self.index_path.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    raw_index = loaded
        except Exception as exc:
            _log(SERVICE, f"[history] Failed to load {self.index_path.name}: {exc}", level="WARNING")
        try:
            if self.bin_path.exists():
                raw_binary = self.bin_path.read_bytes()
        except Exception as exc:
            _log(SERVICE, f"[history] Failed to load {self.bin_path.name}: {exc}", level="WARNING")

        series_meta = raw_index.get("series") or {}
        if not isinstance(series_meta, dict):
            series_meta = {}
        for key, meta in series_meta.items():
            if not isinstance(key, str) or not isinstance(meta, dict):
                continue
            slot = int(meta.get("slot") or 0)
            head = int(meta.get("head") or 0)
            last_hour = int(meta.get("last_hour") or 0)
            if slot < 0 or head < 0 or head >= COUNT_HISTORY_WINDOW_HOURS:
                continue
            start = slot * COUNT_HISTORY_WINDOW_HOURS
            end = start + COUNT_HISTORY_WINDOW_HOURS
            buf = bytearray(COUNT_HISTORY_WINDOW_HOURS)
            chunk = raw_binary[start:end]
            if chunk:
                buf[: min(len(chunk), COUNT_HISTORY_WINDOW_HOURS)] = chunk[:COUNT_HISTORY_WINDOW_HOURS]
            self._series[key] = buf
            self._meta[key] = {
                "slot": slot,
                "head": head,
                "last_hour": max(last_hour, 0),
            }
            self._next_slot = max(self._next_slot, slot + 1)
        self._loaded = True

    def _ensure_series(self, key: str) -> tuple[bytearray, dict[str, int]]:
        self.load()
        meta = self._meta.get(key)
        if meta is None:
            meta = {
                "slot": self._next_slot,
                "head": 0,
                "last_hour": 0,
            }
            self._next_slot += 1
            self._meta[key] = meta
            self._series[key] = bytearray(COUNT_HISTORY_WINDOW_HOURS)
            self._dirty = True
        return self._series[key], meta

    def set_count(self, key: str, *, hour: int, value: int) -> None:
        key = str(key or "").strip()
        if not key or hour <= 0:
            return
        buf, meta = self._ensure_series(key)
        value = max(0, min(255, int(value)))
        changed = False
        meta_changed = False
        last_hour = int(meta.get("last_hour") or 0)

        if last_hour <= 0:
            meta["head"] = 0
            meta["last_hour"] = hour
            buf[:] = b"\x00" * COUNT_HISTORY_WINDOW_HOURS
            buf[0] = value
            changed = True
            meta_changed = True
        elif hour < last_hour:
            distance = last_hour - hour
            if distance >= COUNT_HISTORY_WINDOW_HOURS:
                return
            slot = (int(meta.get("head") or 0) - distance) % COUNT_HISTORY_WINDOW_HOURS
            if buf[slot] != value:
                buf[slot] = value
                changed = True
        elif hour == last_hour:
            head = int(meta.get("head") or 0)
            if buf[head] != value:
                buf[head] = value
                changed = True
        else:
            delta = hour - last_hour
            if delta >= COUNT_HISTORY_WINDOW_HOURS:
                buf[:] = b"\x00" * COUNT_HISTORY_WINDOW_HOURS
                meta["head"] = 0
                meta["last_hour"] = hour
                buf[0] = value
                changed = True
                meta_changed = True
            else:
                head = int(meta.get("head") or 0)
                for _ in range(delta):
                    head = (head + 1) % COUNT_HISTORY_WINDOW_HOURS
                    if buf[head] != 0:
                        buf[head] = 0
                        changed = True
                meta["head"] = head
                meta["last_hour"] = hour
                meta_changed = True
                if buf[head] != value:
                    buf[head] = value
                    changed = True
        if changed or meta_changed:
            self._dirty = True

    def build_payload(self, key: str, *, hostname: str, bot_name: str = "",
                      metric: str = "errors", source: str = "passivbot.log",
                      end_hour: int | None = None) -> dict[str, Any]:
        key = str(key or "").strip()
        self.load()
        if end_hour is None or end_hour <= 0:
            end_hour = int(time.time() // COUNT_HISTORY_STEP_SECONDS)
        start_hour = end_hour - COUNT_HISTORY_WINDOW_HOURS + 1
        meta = self._meta.get(key)
        buf = self._series.get(key)
        points: list[int | None] = []
        last_hour = int((meta or {}).get("last_hour") or 0)
        head = int((meta or {}).get("head") or 0)
        daily_buckets: dict[int, int] = {}
        for hour in range(start_hour, end_hour + 1):
            value = None
            if meta and buf is not None and last_hour > 0:
                distance = last_hour - hour
                if 0 <= distance < COUNT_HISTORY_WINDOW_HOURS:
                    slot = (head - distance) % COUNT_HISTORY_WINDOW_HOURS
                    value = int(buf[slot])
            points.append(value)
            if value is not None:
                day = hour // 24
                daily_buckets[day] = daily_buckets.get(day, 0) + value
        daily_points = []
        start_day = start_hour // 24
        end_day = end_hour // 24
        for day in range(start_day, end_day + 1):
            daily_points.append(daily_buckets.get(day, 0))
        total_count = sum(value for value in points if isinstance(value, int))
        return {
            "available": True,
            "scope": "bot",
            "metric": metric,
            "hostname": hostname,
            "bot_name": bot_name,
            "source": source,
            "step_seconds": COUNT_HISTORY_STEP_SECONDS,
            "window_hours": COUNT_HISTORY_WINDOW_HOURS,
            "start_hour": start_hour,
            "end_hour": end_hour,
            "last_hour": last_hour,
            "series_exists": meta is not None,
            "timezone_basis": "UTC",
            "points": points,
            "total_count": total_count,
            "daily_points": daily_points,
            "daily_step_seconds": 86400,
            "daily_start_day": start_day,
            "daily_end_day": end_day,
        }

    def maybe_flush(self, *, force: bool = False, now_ts: float | None = None) -> None:
        self.load()
        if not self._dirty:
            return
        now_ts = float(now_ts or time.time())
        if not force and (now_ts - self._last_flush_ts) < CPU_HISTORY_FLUSH_INTERVAL:
            return
        self._flush()
        self._last_flush_ts = now_ts
        self._dirty = False

    def _flush(self) -> None:
        self.root_dir.mkdir(parents=True, exist_ok=True)
        slot_count = max(self._next_slot, 0)
        payload = bytearray(slot_count * COUNT_HISTORY_WINDOW_HOURS)
        for key, meta in self._meta.items():
            slot = int(meta.get("slot") or 0)
            start = slot * COUNT_HISTORY_WINDOW_HOURS
            end = start + COUNT_HISTORY_WINDOW_HOURS
            buf = self._series.get(key) or bytearray(COUNT_HISTORY_WINDOW_HOURS)
            payload[start:end] = bytes(buf[:COUNT_HISTORY_WINDOW_HOURS])
        index_payload = {
            "version": COUNT_HISTORY_VERSION,
            "window_hours": COUNT_HISTORY_WINDOW_HOURS,
            "step_seconds": COUNT_HISTORY_STEP_SECONDS,
            "series": {
                key: {
                    "slot": int(meta.get("slot") or 0),
                    "head": int(meta.get("head") or 0),
                    "last_hour": int(meta.get("last_hour") or 0),
                }
                for key, meta in sorted(self._meta.items())
            },
        }
        tmp_bin = self.bin_path.with_suffix(".bin.tmp")
        tmp_json = self.index_path.with_suffix(".json.tmp")
        tmp_bin.write_bytes(bytes(payload))
        tmp_bin.replace(self.bin_path)
        tmp_json.write_text(json.dumps(index_payload, indent=4), encoding="utf-8")
        tmp_json.replace(self.index_path)


class BotPnlHistoryStore:
    """Persistent bot PNL history keyed by bot name with UTC daily aggregates."""

    def __init__(self, root_dir: Path, stem: str):
        self.root_dir = root_dir
        self.data_path = root_dir / f"{stem}.json"
        self._loaded = False
        self._dirty = False
        self._last_flush_ts = 0.0
        self._series: dict[str, dict[str, Any]] = {}

    def load(self) -> None:
        if self._loaded:
            return
        self.root_dir.mkdir(parents=True, exist_ok=True)
        try:
            if self.data_path.exists():
                loaded = json.loads(self.data_path.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    for bot_name, payload in loaded.items():
                        if not isinstance(bot_name, str) or not isinstance(payload, dict):
                            continue
                        days = payload.get("days") or {}
                        if not isinstance(days, dict):
                            days = {}
                        normalized_days: dict[str, dict[str, Any]] = {}
                        for day, entry in days.items():
                            if not isinstance(entry, dict):
                                continue
                            normalized_days[str(day)] = {
                                "pnl": float(entry.get("pnl") or 0.0),
                                "fills": int(entry.get("fills") or 0),
                            }
                        self._series[bot_name] = {
                            "days": normalized_days,
                            "last_fill_ts": int(payload.get("last_fill_ts") or 0),
                        }
        except Exception as exc:
            _log(SERVICE, f"[history] Failed to load {self.data_path.name}: {exc}", level="WARNING")
        self._loaded = True

    def _ensure_bot(self, bot_name: str) -> dict[str, Any]:
        self.load()
        name = str(bot_name or '').strip()
        payload = self._series.get(name)
        if payload is None:
            payload = {"days": {}, "last_fill_ts": 0}
            self._series[name] = payload
            self._dirty = True
        return payload

    def add_day_values(self, bot_name: str, *, day: int, pnl: float, fills: int) -> bool:
        name = str(bot_name or '').strip()
        if not name or day <= 0:
            return False
        payload = self._ensure_bot(name)
        days = payload["days"]
        entry = days.get(str(day))
        if not isinstance(entry, dict):
            entry = {"pnl": 0.0, "fills": 0}
            days[str(day)] = entry
        entry["pnl"] = float(entry.get("pnl") or 0.0) + float(pnl or 0.0)
        entry["fills"] = int(entry.get("fills") or 0) + int(fills or 0)
        self._dirty = True
        return True

    def set_last_fill_ts(self, bot_name: str, ts_val: int) -> bool:
        name = str(bot_name or '').strip()
        if not name or ts_val <= 0:
            return False
        payload = self._ensure_bot(name)
        if ts_val <= int(payload.get("last_fill_ts") or 0):
            return False
        payload["last_fill_ts"] = int(ts_val)
        self._dirty = True
        return True

    def get_last_fill_ts(self, bot_name: str) -> int:
        self.load()
        payload = self._series.get(str(bot_name or '').strip()) or {}
        return int(payload.get("last_fill_ts") or 0)

    def get_total(self, bot_name: str) -> tuple[float, int]:
        self.load()
        payload = self._series.get(str(bot_name or '').strip()) or {}
        days = payload.get("days") or {}
        total_pnl = 0.0
        total_fills = 0
        for entry in days.values():
            if not isinstance(entry, dict):
                continue
            total_pnl += float(entry.get("pnl") or 0.0)
            total_fills += int(entry.get("fills") or 0)
        return total_pnl, total_fills

    def build_payload(self, bot_name: str, *, hostname: str, metric: str = "pnl", source: str = "passivbot.log") -> dict[str, Any]:
        name = str(bot_name or '').strip()
        self.load()
        payload = self._series.get(name) or {"days": {}, "last_fill_ts": 0}
        raw_days = payload.get("days") or {}
        day_keys: list[int] = []
        for key in raw_days.keys():
            try:
                day_keys.append(int(key))
            except Exception:
                continue
        day_keys.sort()
        points: list[float] = []
        cumulative_points: list[float] = []
        fills_points: list[int] = []
        total = 0.0
        total_fills = 0
        best_day_pnl = None
        worst_day_pnl = None
        for day in day_keys:
            entry = raw_days.get(str(day)) or {}
            day_pnl = float(entry.get("pnl") or 0.0)
            day_fills = int(entry.get("fills") or 0)
            total += day_pnl
            total_fills += day_fills
            points.append(day_pnl)
            cumulative_points.append(total)
            fills_points.append(day_fills)
            best_day_pnl = day_pnl if best_day_pnl is None else max(best_day_pnl, day_pnl)
            worst_day_pnl = day_pnl if worst_day_pnl is None else min(worst_day_pnl, day_pnl)
        return {
            "available": True,
            "scope": "bot",
            "metric": metric,
            "hostname": hostname,
            "bot_name": name,
            "source": source,
            "timezone_basis": "UTC",
            "series_exists": bool(day_keys),
            "days": day_keys,
            "start_day": day_keys[0] if day_keys else 0,
            "end_day": day_keys[-1] if day_keys else 0,
            "points": points,
            "cumulative_points": cumulative_points,
            "fills_points": fills_points,
            "last_fill_ts": int(payload.get("last_fill_ts") or 0),
            "total_pnl": total,
            "total_fills": total_fills,
            "best_day_pnl": best_day_pnl,
            "worst_day_pnl": worst_day_pnl,
        }

    def maybe_flush(self, *, force: bool = False, now_ts: float | None = None) -> None:
        self.load()
        if not self._dirty:
            return
        now_ts = float(now_ts or time.time())
        if not force and (now_ts - self._last_flush_ts) < CPU_HISTORY_FLUSH_INTERVAL:
            return
        tmp_path = self.data_path.with_suffix('.json.tmp')
        tmp_path.write_text(json.dumps(self._series, indent=4), encoding='utf-8')
        tmp_path.replace(self.data_path)
        self._last_flush_ts = now_ts
        self._dirty = False

# ── Remote scripts ──────────────────────────────────────────


def _monitor_agent_tail_command(remote_pbgui_dir: str) -> str:
    canonical = remote_shell_path(remote_path_join(remote_pbgui_dir, "data", "logs", "monitor-agent", "live_metrics.ndjson"))
    legacy = remote_shell_path(remote_path_join(remote_pbgui_dir, "data", "monitor_agent", "live_metrics.ndjson"))
    return f"canonical={canonical}; legacy={legacy}; while [ ! -f \"$canonical\" ] && [ ! -f \"$legacy\" ]; do sleep 1; done; if [ -f \"$canonical\" ]; then p=\"$canonical\"; else p=\"$legacy\"; fi; tail -n 1 -F \"$p\""


def _monitor_agent_cache_read_command(remote_pbgui_dir: str, filename: str) -> str:
    cache_path = remote_path_join(remote_pbgui_dir, "data", "monitor_agent", filename)
    return f"cat {remote_shell_path(cache_path)}"


def _host_meta_direct_command(remote_pbgui_dir: str) -> str:
    """Return a direct remote host-metadata probe command."""

    raw_dir = str(remote_pbgui_dir or "software/pbgui").strip().rstrip("/") or "software/pbgui"
    if raw_dir.startswith("/"):
        pbgdir_expr = json.dumps(raw_dir)
    else:
        pbgdir_expr = f"os.path.join(HOME, {json.dumps(raw_dir)})"
    command = HOST_META_SCRIPT.replace("os.path.join(HOME, '__PBGDIR__')", pbgdir_expr)
    return command.replace(
        "python3 -u - <<'PY'",
        "timeout --signal=TERM --kill-after=2s 20s python3 -u - <<'PY'",
        1,
    )

INSTANCE_COLLECT_SCRIPT = r'''python3 -u -c "
import hashlib, json, os, re, subprocess, sys, time
from datetime import datetime, timezone

try:
    import fcntl
except ImportError:
    fcntl = None

HOME = os.path.expanduser('~')

def _home_path(raw, default):
    value = str(raw or default or '').strip().rstrip('/')
    if not value:
        value = default
    if value.startswith('~/'):
        return os.path.join(HOME, value[2:])
    if os.path.isabs(value):
        return value
    return os.path.join(HOME, value)

PBGDIR = _home_path(os.environ.get('PBGUI_PBGDIR'), 'software/pbgui')
PB7DIR = _home_path(os.environ.get('PBGUI_PB7DIR'), 'software/pb7')
SERVICE = 'VPSMonitor'
LOG_PATH = os.path.join(PBGDIR, 'data', 'logs', 'VPSMonitor.log')
LOG_MAX_BYTES = 1024 * 1024
LOG_BACKUP_COUNT = 3
REDACTED = '[REDACTED]'
_PEM_RE = re.compile(r'-----BEGIN(?: [A-Z0-9]+)? PRIVATE KEY-----.*?-----END(?: [A-Z0-9]+)? PRIVATE KEY-----', re.I | re.S)
_HEADER_RE = re.compile(r'(?i)(\b(?:authorization|cookie|session(?:id)?|pbgui_session)\b[\x22\x27]?\s*[:=]\s*)(?:bearer\s+)?(?!\[REDACTED\])(?:\x22[^\x22]*\x22|\x27[^\x27]*\x27|[^\s,;\}\]]+)')
_BEARER_RE = re.compile(r'(?i)(\bbearer\s+)(?!\[REDACTED\])(?:\x22[^\x22]*\x22|\x27[^\x27]*\x27|[^\s,;\}\]]+)')
_SECRET_RE = re.compile(r'(?i)(\b(?:password|passwd|api[_-]?key|apikey|api[_-]?secret|secret|token|private[_-]?key|passphrase)\b[\x22\x27]?\s*[=:]\s*)(?!\[REDACTED\])(?:\x22[^\x22]*\x22|\x27[^\x27]*\x27|[^\s,;\}\]]+)')
_QUERY_RE = re.compile(r'(?i)([?&](?:password|passwd|api[_-]?key|apikey|api[_-]?secret|secret|token|session|cookie|authorization|bearer|private[_-]?key|passphrase)=)[^&#\s]+')

def _sanitize(value):
    try:
        text = str(value)[:16384]
        text = _PEM_RE.sub(REDACTED, text)
        text = _QUERY_RE.sub(lambda match: match.group(1) + REDACTED, text)
        text = _HEADER_RE.sub(lambda match: match.group(1) + REDACTED, text)
        text = _BEARER_RE.sub(lambda match: match.group(1) + REDACTED, text)
        return _SECRET_RE.sub(lambda match: match.group(1) + REDACTED, text)
    except Exception:
        return REDACTED

def _rotate_log():
    oldest = LOG_PATH + '.' + str(LOG_BACKUP_COUNT)
    try:
        if os.path.exists(oldest):
            os.remove(oldest)
        for index in range(LOG_BACKUP_COUNT - 1, 0, -1):
            source = LOG_PATH + '.' + str(index)
            if os.path.exists(source):
                os.replace(source, LOG_PATH + '.' + str(index + 1))
        if os.path.exists(LOG_PATH):
            os.replace(LOG_PATH, LOG_PATH + '.1')
    except FileNotFoundError:
        pass

def _log(service, msg, level='WARNING'):
    safe_service = _sanitize(service).replace('\n', ' ')
    safe_level = _sanitize(level).replace('\n', ' ')
    safe_msg = _sanitize(msg)
    lock_handle = None
    try:
        ts = datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
        log_dir = os.path.dirname(LOG_PATH)
        os.makedirs(log_dir, exist_ok=True)
        line = f'{ts} [{safe_service}] [{safe_level}] {safe_msg}\n'
        lock_handle = open(LOG_PATH + '.lock', 'a+', encoding='utf-8')
        if fcntl is not None:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        if os.path.exists(LOG_PATH) and os.path.getsize(LOG_PATH) + len(line.encode('utf-8')) > LOG_MAX_BYTES:
            _rotate_log()
        with open(LOG_PATH, 'a', encoding='utf-8') as handle:
            handle.write(line)
            handle.flush()
    except Exception:
        try:
            sys.stderr.write(f'[{safe_service}] [{safe_level}] {safe_msg}\n')
            sys.stderr.flush()
        except Exception:
            pass
    finally:
        if lock_handle is not None:
            try:
                if fcntl is not None:
                    fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
                lock_handle.close()
            except Exception:
                pass

TODAY_START = int(datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
YESTERDAY_START = TODAY_START - 86400
TODAY = datetime.fromtimestamp(TODAY_START, timezone.utc).strftime('%Y-%m-%d')
YESTERDAY = datetime.fromtimestamp(YESTERDAY_START, timezone.utc).strftime('%Y-%m-%d')

# PNL regex (matches PBRun patterns)
FILL_SUMMARY_RE = re.compile(r'\[fill\]\s+(\d+)\s+fills,\s+pnl=([+-]?(?:\d+\.?\d*|\d*\.\d+))\s+\w+')
FILL_PNL_RE = re.compile(r'\bpnl=([+-]?(?:\d+\.?\d*|\d*\.\d+))\b')
SYNC_EXCLUDE_FILES = {'approved_coins.json', 'config_run.json', 'ignored_coins.json', 'running_version.txt'}

# shared helpers (used by both counting and dump mode)

def _utc_ts(ts_str):
    return int(datetime.strptime(ts_str, '%Y-%m-%dT%H:%M:%S').replace(tzinfo=timezone.utc).timestamp())

def _parse_log_timestamp(line):
    mts = re.match(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})Z', str(line or ''))
    if not mts:
        return None
    try:
        return _utc_ts(mts.group(1))
    except Exception:
        return None

def _utc_day_from_ts(ts_val):
    return int(int(ts_val) // 86400)

def _extract_fill_summary(line):
    text = str(line or '')
    if '[fill]' not in text:
        return None
    m = FILL_SUMMARY_RE.search(text)
    if m:
        return float(m.group(2)), int(m.group(1))
    m = FILL_PNL_RE.search(text)
    if m:
        return float(m.group(1)), 1
    return None

def _read_json(path):
    try:
        with open(path, 'r', encoding='utf-8') as handle:
            return json.load(handle)
    except Exception:
        return None

def _read_text(path):
    try:
        with open(path, 'r', encoding='utf-8') as handle:
            return handle.read().strip()
    except Exception:
        return ''

def _load_cluster_context():
    root = os.path.join(PBGDIR, 'data', 'cluster')
    node_id = _read_text(os.path.join(root, 'node_id'))
    cluster_id = _read_text(os.path.join(root, 'cluster_id'))
    desired = _read_json(os.path.join(root, 'desired_state.json'))
    desired_loaded = isinstance(desired, dict)
    desired = desired if isinstance(desired, dict) else {}
    instances = desired.get('instances') if isinstance(desired.get('instances'), dict) else {}
    tombstones = desired.get('tombstones') if isinstance(desired.get('tombstones'), dict) else {}
    configured = bool(node_id and cluster_id)
    return {
        'configured': configured,
        'node_id': node_id,
        'cluster_id': cluster_id,
        'desired_loaded': desired_loaded,
        'desired_cluster_id': str(desired.get('cluster_id') or ''),
        'instances': instances,
        'tombstones': tombstones,
    }

def _config_meta(config_dir):
    cfg_path = os.path.join(config_dir, 'config.json')
    cfg = _read_json(cfg_path)
    pbgui = cfg.get('pbgui', {}) if isinstance(cfg, dict) else {}
    return {
        'version': pbgui.get('version', 0),
        'enabled_on': pbgui.get('enabled_on', 'disabled'),
        'dynamic_ignore': bool(pbgui.get('dynamic_ignore')),
    }

def _config_manifest_hash(config_dir):
    files = {}
    try:
        filenames = sorted(os.listdir(config_dir))
    except Exception:
        filenames = []
    for filename in filenames:
        if filename in SYNC_EXCLUDE_FILES or not filename.endswith('.json'):
            continue
        path = os.path.join(config_dir, filename)
        if not os.path.isfile(path):
            continue
        try:
            with open(path, 'rb') as handle:
                raw = handle.read()
        except Exception:
            continue
        files[filename] = {'sha256': hashlib.sha256(raw).hexdigest(), 'size': len(raw)}
    manifest = json.dumps({'schema_version': 1, 'files': files}, sort_keys=True, separators=(',', ':')).encode('utf-8')
    return 'sha256:' + hashlib.sha256(manifest).hexdigest()

def _cluster_gate_status(name, config_dir, version, cluster):
    if not cluster.get('configured'):
        return {'blocked': False, 'cluster_gate': 'not_configured', 'blocked_reason': ''}
    if not cluster.get('desired_loaded'):
        return {'blocked': True, 'cluster_gate': 'missing_desired_state', 'blocked_reason': 'Cluster desired_state.json is missing'}
    if str(cluster.get('desired_cluster_id') or '') != str(cluster.get('cluster_id') or ''):
        return {'blocked': True, 'cluster_gate': 'foreign_desired_state', 'blocked_reason': 'Cluster desired_state.json belongs to another cluster'}
    tombstones = cluster.get('tombstones') if isinstance(cluster.get('tombstones'), dict) else {}
    if name in tombstones:
        return {'blocked': True, 'cluster_gate': 'tombstoned', 'blocked_reason': 'Cluster desired state tombstoned this instance'}
    instances = cluster.get('instances') if isinstance(cluster.get('instances'), dict) else {}
    item = instances.get(name)
    if not isinstance(item, dict):
        return {'blocked': True, 'cluster_gate': 'missing_instance', 'blocked_reason': 'Instance is missing from Cluster desired state'}
    if item.get('conflicted') is True:
        return {'blocked': True, 'cluster_gate': 'conflicted', 'blocked_reason': 'Cluster desired state marks this instance as conflicted'}
    if str(item.get('desired_state') or '') != 'running':
        return {'blocked': True, 'cluster_gate': 'desired_stopped', 'blocked_reason': 'Cluster desired state is not running'}
    if str(item.get('assigned_host') or '') != str(cluster.get('node_id') or ''):
        return {'blocked': True, 'cluster_gate': 'wrong_host', 'blocked_reason': 'Cluster desired state assigns this instance to another node'}
    if not os.path.isdir(config_dir):
        return {'blocked': True, 'cluster_gate': 'missing_local_config', 'blocked_reason': 'Assigned config is not materialized locally'}
    expected_hash = str(item.get('config_manifest_hash') or '')
    try:
        if _config_manifest_hash(config_dir) != expected_hash:
            return {'blocked': True, 'cluster_gate': 'manifest_mismatch', 'blocked_reason': 'Local config manifest does not match Cluster desired state'}
    except Exception as exc:
        return {'blocked': True, 'cluster_gate': 'manifest_error', 'blocked_reason': 'Cluster config manifest check failed: ' + str(exc)}
    expected_version = str(item.get('version') or '')
    if str(version or '') != expected_version:
        return {'blocked': True, 'cluster_gate': 'version_mismatch', 'blocked_reason': 'Local config version does not match Cluster desired state'}
    return {'blocked': False, 'cluster_gate': 'allowed', 'blocked_reason': ''}

def _count_hourly_occurrences(lines, needle):
    buckets = {}
    for line in lines or []:
        if needle not in line:
            continue
        mts = re.match(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})Z', str(line))
        if not mts:
            continue
        try:
            hour = _utc_ts(mts.group(1)) // 3600
        except Exception:
            continue
        buckets[hour] = buckets.get(hour, 0) + 1
    return buckets

def _process_pb7_line(line, mode, bc=None, lines_out=None, last_day=None):
    if ' ERROR ' in line:
        if mode == 'count' and bc is not None:
            if last_day == 'today': bc['et'] += 1
        elif mode == 'dump' and lines_out is not None:
            lines_out.append(line.rstrip('\n'))
    if mode == 'count' and bc is not None:
        if '[fill]' not in line:
            return
        m = FILL_SUMMARY_RE.search(line)
        if m:
            c = int(m.group(1)); pnl = float(m.group(2))
            if last_day == 'today': bc['ct'] += c; bc['pt'] += pnl
        else:
            m = FILL_PNL_RE.search(line)
            if m:
                pnl = float(m.group(1))
                if last_day == 'today': bc['ct'] += 1; bc['pt'] += pnl

def _read_pb7_tail(fp, offset, today_start, yesterday_start, bc):
    # Incrementally read one pb7 log file from offset to EOF.
    last_day = None
    try:
        size = os.path.getsize(fp)
        if offset > size:
            offset = 0
        with open(fp, 'r') as f:
            f.seek(offset)
            for line in f:
                mts = re.match(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)', line)
                if mts:
                    ts_str = mts.group(1).rstrip('Z')
                    try:
                        ts_val = _utc_ts(ts_str)
                        if ts_val >= today_start: last_day = 'today'
                        elif ts_val >= yesterday_start: last_day = 'yesterday'
                        else: last_day = None
                    except: pass
                _process_pb7_line(line, 'count', bc=bc, last_day=last_day)
            return f.tell()
    except Exception:
        pass
    return offset

def _read_err_tail(fp, offset, today_start, yesterday_start, bc):
    # Incrementally read one stderr traceback file from offset to EOF.
    last_day = None
    try:
        size = os.path.getsize(fp)
        if offset > size:
            offset = 0
        with open(fp, 'r') as f:
            f.seek(offset)
            for line in f:
                mts = re.match(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)', line)
                if mts:
                    ts_str = mts.group(1).rstrip('Z')
                    try:
                        ts_val = _utc_ts(ts_str)
                        if ts_val >= today_start: last_day = 'today'
                        elif ts_val >= yesterday_start: last_day = 'yesterday'
                        else: last_day = None
                    except: pass
                if 'Traceback' in line:
                    if last_day == 'today': bc['tt'] += 1
            return f.tell()
    except Exception:
        pass
    return offset

def _file_start_sig(fp):
    # Return a small signature of the current file start to detect truncate+rewrite.
    try:
        with open(fp, 'r') as f:
            return f.readline().rstrip('\n')[:200]
    except Exception:
        pass
    return ''

def _read_pb7_file(fp, mode, today_start, yesterday_start, bc=None, lines_out=None,
                   target_start=None, target_end=None):
    # Read one pb7 log file. Returns earliest_ts seen or None.
    last_day = None
    earliest = None
    in_target = False
    try:
        with open(fp, 'r') as f:
            for line in f:
                mts = re.match(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)', line)
                if mts:
                    ts_str = mts.group(1).rstrip('Z')
                    try:
                        ts_val = _utc_ts(ts_str)
                        if earliest is None or ts_val < earliest:
                            earliest = ts_val
                        if mode == 'dump' and ts_val >= (target_end or ts_val + 1):
                            break
                        if ts_val >= today_start: last_day = 'today'
                        elif ts_val >= yesterday_start: last_day = 'yesterday'
                        else: last_day = None
                        if mode == 'dump':
                            in_target = (last_day is not None) if target_start is None else (
                                ts_val >= target_start and (target_end is None or ts_val < target_end)
                            )
                    except: pass
                if mode == 'dump' and not in_target:
                    continue
                _process_pb7_line(line, mode, bc=bc, lines_out=lines_out, last_day=last_day)
    except Exception as exc:
        _log(SERVICE, f'[monitor-helper] Failed to read PB7 log file {fp}: {exc}', level='WARNING')
    return earliest

def _read_err_file(fp, mode, today_start, yesterday_start, bc=None):
    # Read one err_log file and count tracebacks.
    last_day = None
    try:
        with open(fp, 'r') as f:
            for line in f:
                mts = re.match(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)', line)
                if mts:
                    ts_str = mts.group(1).rstrip('Z')
                    try:
                        ts_val = _utc_ts(ts_str)
                        if ts_val >= today_start: last_day = 'today'
                        elif ts_val >= yesterday_start: last_day = 'yesterday'
                        else: last_day = None
                    except: pass
                if 'Traceback' in line and last_day:
                    if mode == 'count' and bc is not None:
                        if last_day == 'today': bc['tt'] += 1
    except Exception as exc:
        _log(SERVICE, f'[monitor-helper] Failed to read error log file {fp}: {exc}', level='WARNING')

# cache from master
EXPECTED_CACHE_VERSION = int(os.environ.get('PBGUI_CACHE_VERSION', '0') or 0)
cache_raw = os.environ.get('PBGUI_CACHE', '{}')
host_cache = {}
try:
    host_cache = json.loads(cache_raw)
except Exception as exc:
    _log(SERVICE, f'[monitor-cache] Failed to parse PBGUI_CACHE: {exc}', level='WARNING')
if not isinstance(host_cache, dict):
    host_cache = {}
host_cache_version = int(host_cache.get('_version', 0) or 0) if isinstance(host_cache, dict) else 0
if host_cache_version != EXPECTED_CACHE_VERSION:
    host_cache = {}

# find running bots
running = {}
try:
    out = subprocess.check_output(['ps', 'aux'], text=True)
    for line in out.splitlines():
        if 'main.py' not in line or 'config_run.json' not in line:
            continue
        for part in line.split():
            if part.endswith('/config_run.json'):
                d = os.path.dirname(part)
                running[os.path.basename(d)] = d
                break
except Exception as exc:
    _log(SERVICE, f'[monitor-process] Failed to inspect running bot processes: {exc}', level='WARNING')

# ── dump mode: return matching log lines for bot-log popup ──
dump_mode = os.environ.get('PBGUI_DUMP')
if dump_mode:
    dump_bot = os.environ.get('PBGUI_DUMP_BOT', '')
    dump_kind = os.environ.get('PBGUI_DUMP_KIND', 'errors')
    dump_bucket = os.environ.get('PBGUI_DUMP_BUCKET', 'today')
    dump_lines = int(os.environ.get('PBGUI_DUMP_LINES', '5000'))

    # find cfg_dir for the requested bot
    cfg_dir = running.get(dump_bot)
    lines_out = []

    if cfg_dir:
        # file lists (same as counting loop)
        pb7_log = os.path.join(PB7DIR, 'logs', f'{dump_bot}.log')
        err_log = os.path.join(cfg_dir, 'passivbot_err.log')
        old_err = os.path.join(cfg_dir, 'passivbot_err.log.old')

        pb7_old_files = []
        try:
            import glob as _glob3
            log_real = os.path.realpath(pb7_log) if os.path.isfile(pb7_log) else ''
            for fp in sorted(
                _glob3.glob(os.path.join(PB7DIR, 'logs', '*' + dump_bot + '*.log')),
                key=os.path.getmtime, reverse=True
            ):
                if not os.path.isfile(fp): continue
                if os.path.islink(fp): continue
                if log_real and os.path.realpath(fp) == log_real: continue
                pb7_old_files.append(fp)
        except Exception as exc:
            _log(SERVICE, f'[monitor-dump] Failed to collect old PB7 logs for {dump_bot}: {exc}', level='WARNING')

        today_start = TODAY_START
        yesterday_start = YESTERDAY_START
        if dump_bucket == 'today':
            target_start = today_start
            target_end = today_start + 86400
        elif dump_bucket == 'yesterday':
            target_start = yesterday_start
            target_end = today_start
        else:
            target_start = yesterday_start
            target_end = today_start + 86400

        if dump_kind == 'tracebacks':
            # read passivbot_err.log and its .old (same files as counting)
            err_files = [
                os.path.join(cfg_dir, 'passivbot_err.log.old'),
                os.path.join(cfg_dir, 'passivbot_err.log'),
            ]
            for fp in err_files:
                if not os.path.isfile(fp):
                    continue
                # group lines by wrapper timestamp into entries
                entry_lines = []
                last_ts = None

                def flush_tb_entry():
                    if entry_lines and any('Traceback' in l for l in entry_lines):
                        lines_out.extend(entry_lines)
                        if len(lines_out) > 0 and lines_out[-1] != '-----':
                            lines_out.extend(['', '-----', ''])

                try:
                    with open(fp, 'r') as f:
                        for line in f:
                            line = line.rstrip('\n')
                            mts = re.match(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)', line)
                            ts_val = None
                            if mts:
                                try:
                                    ts_val = _utc_ts(mts.group(1).rstrip('Z'))
                                except: pass
                            if ts_val is not None and ts_val != last_ts:
                                flush_tb_entry()
                                entry_lines = []
                                last_ts = ts_val
                            if ts_val is not None and ts_val >= target_start and ts_val < target_end:
                                entry_lines.append(line)
                            elif entry_lines and 'Traceback' not in line:
                                entry_lines.append(line)
                    flush_tb_entry()
                except Exception as exc:
                    _log(SERVICE, f'[monitor-dump] Failed to read traceback log {fp}: {exc}', level='WARNING')
            # remove trailing separator
            while lines_out and lines_out[-1] == '-----':
                lines_out.pop()
                if lines_out and lines_out[-1] == '':
                    lines_out.pop()
        else:
            # errors: same file list + same read logic as counting
            for fp in pb7_old_files:
                if not os.path.isfile(fp):
                    continue
                earliest = _read_pb7_file(fp, 'dump', today_start, yesterday_start,
                                          lines_out=lines_out,
                                          target_start=target_start, target_end=target_end)
                if earliest is not None and earliest < yesterday_start:
                    break
            if os.path.isfile(pb7_log):
                _read_pb7_file(pb7_log, 'dump', today_start, yesterday_start,
                               lines_out=lines_out,
                               target_start=target_start, target_end=target_end)

        # trim to max lines
        if dump_lines > 0 and len(lines_out) > dump_lines:
            lines_out = lines_out[-dump_lines:]

    print(json.dumps({'lines': lines_out}))
    exit(0)

# ── count-history rebuild mode: return UTC hourly buckets for one bot ──
rebuild_mode = os.environ.get('PBGUI_REBUILD_COUNTS')
if rebuild_mode:
    rebuild_bot = os.environ.get('PBGUI_REBUILD_BOT', '')
    rebuild_from_hour = int(os.environ.get('PBGUI_REBUILD_FROM_HOUR', '0') or 0)
    rebuild_to_hour = int(os.environ.get('PBGUI_REBUILD_TO_HOUR', '0') or 0)
    cfg_dir = running.get(rebuild_bot)
    result = {'bot': rebuild_bot, 'from_hour': rebuild_from_hour, 'to_hour': rebuild_to_hour, 'errors': {}, 'tracebacks': {}}
    if cfg_dir and rebuild_to_hour > 0 and rebuild_from_hour > 0 and rebuild_from_hour <= rebuild_to_hour:
        pb7_log = os.path.join(PB7DIR, 'logs', f'{rebuild_bot}.log')
        err_log = os.path.join(cfg_dir, 'passivbot_err.log')
        old_err = os.path.join(cfg_dir, 'passivbot_err.log.old')

        pb7_old_files = []
        try:
            import glob as _glob4
            log_real = os.path.realpath(pb7_log) if os.path.isfile(pb7_log) else ''
            for fp in sorted(
                _glob4.glob(os.path.join(PB7DIR, 'logs', '*' + rebuild_bot + '*.log')),
                key=os.path.getmtime, reverse=True
            ):
                if not os.path.isfile(fp): continue
                if os.path.islink(fp): continue
                if log_real and os.path.realpath(fp) == log_real: continue
                pb7_old_files.append(fp)
        except Exception as exc:
            _log(SERVICE, f'[monitor-rebuild] Failed to collect old PB7 logs for {rebuild_bot}: {exc}', level='WARNING')

        files_by_metric = {
            'errors': list(pb7_old_files) + ([pb7_log] if os.path.isfile(pb7_log) else []),
            'tracebacks': [fp for fp in (old_err, err_log) if os.path.isfile(fp)],
        }
        needles = {'errors': ' ERROR ', 'tracebacks': 'Traceback'}
        for metric, files in files_by_metric.items():
            lines = []
            for fp in files:
                try:
                    with open(fp, 'r', encoding='utf-8', errors='ignore') as f:
                        lines.extend(f.read().splitlines())
                except Exception as exc:
                    _log(SERVICE, f'[monitor-rebuild] Failed to read {metric} log {fp}: {exc}', level='WARNING')
            buckets = _count_hourly_occurrences(lines, needles[metric])
            result[metric] = {
                str(hour): int(buckets.get(hour, 0))
                for hour in range(rebuild_from_hour, rebuild_to_hour + 1)
            }
    print(json.dumps(result))
    exit(0)

rebuild_pnl_mode = os.environ.get('PBGUI_REBUILD_PNL')
if rebuild_pnl_mode:
    rebuild_bot = os.environ.get('PBGUI_REBUILD_BOT', '')
    rebuild_since_ts = int(os.environ.get('PBGUI_REBUILD_PNL_SINCE_TS', '0') or 0)
    cfg_dir = running.get(rebuild_bot)
    result = {'bot': rebuild_bot, 'since_ts': rebuild_since_ts, 'days': {}, 'last_fill_ts': 0}
    if cfg_dir:
        pb7_log = os.path.join(PB7DIR, 'logs', f'{rebuild_bot}.log')
        pb7_old_files = []
        try:
            import glob as _glob5
            log_real = os.path.realpath(pb7_log) if os.path.isfile(pb7_log) else ''
            for fp in sorted(
                _glob5.glob(os.path.join(PB7DIR, 'logs', '*' + rebuild_bot + '*.log')),
                key=os.path.getmtime,
            ):
                if not os.path.isfile(fp):
                    continue
                if os.path.islink(fp):
                    continue
                if log_real and os.path.realpath(fp) == log_real:
                    continue
                pb7_old_files.append(fp)
        except Exception as exc:
            _log(SERVICE, f'[monitor-pnl] Failed to collect old PB7 logs for {rebuild_bot}: {exc}', level='WARNING')
        files = list(pb7_old_files) + ([pb7_log] if os.path.isfile(pb7_log) else [])
        days = {}
        last_fill_ts = 0
        for fp in files:
            try:
                with open(fp, 'r', encoding='utf-8', errors='ignore') as f:
                    for line in f:
                        fill = _extract_fill_summary(line)
                        if fill is None:
                            continue
                        ts_val = _parse_log_timestamp(line)
                        if ts_val is None or ts_val <= rebuild_since_ts:
                            continue
                        day = str(_utc_day_from_ts(ts_val))
                        entry = days.get(day)
                        if entry is None:
                            entry = {'pnl': 0.0, 'fills': 0}
                            days[day] = entry
                        entry['pnl'] += float(fill[0])
                        entry['fills'] += int(fill[1])
                        last_fill_ts = max(last_fill_ts, ts_val)
            except Exception as exc:
                _log(SERVICE, f'[monitor-pnl] Failed to read PB7 log {fp}: {exc}', level='WARNING')
        result['days'] = days
        result['last_fill_ts'] = last_fill_ts
    print(json.dumps(result))
    exit(0)

monitors = []
v7 = []
new_cache = {'_version': EXPECTED_CACHE_VERSION}

cluster_context = _load_cluster_context()

# collect bot log files for sidebar selector and history rebuild
bot_logs = {}
try:
    log_dir = os.path.join(PB7DIR, 'logs')
    if os.path.isdir(log_dir):
        running_names = sorted((name for name in running.keys() if name), key=len, reverse=True)
        for f in sorted(os.listdir(log_dir)):
            if not f.endswith('.log'): continue
            # extract bot name: either name.log or 20260508_..._name_config_run.json.log
            if os.path.islink(os.path.join(log_dir, f)): continue
            direct_name = f[:-4]
            matched_name = direct_name if direct_name in running else ''
            if not matched_name:
                for name in running_names:
                    if name in f:
                        matched_name = name
                        break
            if matched_name:
                bot_logs.setdefault(matched_name, {'errors': [], 'tracebacks': [], 'sidebar': []})['sidebar'].append(f'pb7/logs/{f}')
        for name in running_names:
            cfg_dir = running.get(name, '')
            if not cfg_dir:
                continue
            for err_name in ('passivbot_err.log', 'passivbot_err.log.old'):
                err_path = os.path.join(cfg_dir, err_name)
                if os.path.isfile(err_path):
                    bot_logs.setdefault(name, {'errors': [], 'tracebacks': [], 'sidebar': []})['sidebar'].append(f'data/run_v7/{name}/{err_name}')
except Exception as exc:
    _log(SERVICE, f'[monitor-sidebar] Failed to collect bot log sidebar files: {exc}', level='WARNING')

for name, cfg_dir in sorted(running.items()):
    # config version + enabled_on + dynamic_ignore
    version = 0; enabled_on = 'disabled'; dynamic_ignore = False
    cf = os.path.join(cfg_dir, 'config.json')
    if os.path.isfile(cf):
        try:
            pbgui = json.load(open(cf)).get('pbgui', {})
            version = pbgui.get('version', 0)
            enabled_on = pbgui.get('enabled_on', 'disabled')
            dynamic_ignore = bool(pbgui.get('dynamic_ignore'))
        except Exception as exc:
            _log(SERVICE, f'[monitor-config] Failed to read config for {name}: {exc}', level='WARNING')
    rv = 0
    rvf = os.path.join(cfg_dir, 'running_version.txt')
    if os.path.isfile(rvf):
        try: rv = int(open(rvf).read().strip())
        except Exception as exc:
            _log(SERVICE, f'[monitor-config] Failed to read running version for {name}: {exc}', level='WARNING')
    gate = _cluster_gate_status(name, cfg_dir, version, cluster_context)
    v7.append({
        'name': name,
        'running': True,
        'cv': version,
        'eo': enabled_on,
        'rv': rv,
        'di': dynamic_ignore,
        'blocked': bool(gate.get('blocked', False)),
        'blocked_reason': str(gate.get('blocked_reason', '') or ''),
        'cluster_gate': str(gate.get('cluster_gate', '') or ''),
    })

    # passivbot monitor dir (for start time)
    monitor_dir = None
    mroot = os.path.join(PB7DIR, 'monitor')
    if os.path.isdir(mroot):
        for ex in os.listdir(mroot):
            d = os.path.join(mroot, ex, name)
            if os.path.isdir(d) and os.path.isfile(os.path.join(d, 'state.latest.json')):
                monitor_dir = d; break

    # start time from state.latest.json (or default 0 if no monitor dir)
    start_ts = 0.0
    if monitor_dir:
        sf = os.path.join(monitor_dir, 'state.latest.json')
        if os.path.isfile(sf):
            try:
                meta = json.load(open(sf)).get('meta', {})
                start_ts = float(meta.get('bot_start_ts_ms', 0)) / 1000.0
            except Exception as exc:
                _log(SERVICE, f'[monitor-state] Failed to read start time for {name}: {exc}', level='WARNING')

    # per-bot cache
    bc = dict(host_cache.get(name, {}))
    bc.setdefault('today', TODAY)
    bc.setdefault('et', 0)
    bc.setdefault('tt', 0)
    bc.setdefault('ct', 0)
    bc.setdefault('pt', 0.0)
    bc.setdefault('log_off', 0)
    bc.setdefault('log_fp', '')
    bc.setdefault('log_sig', '')
    bc.setdefault('err_sig', '')

    # day change
    if bc['today'] != TODAY:
        bc['et'] = 0
        bc['tt'] = 0
        bc['ct'] = 0
        bc['pt'] = 0.0
        bc['today'] = TODAY

    # pb7 log (errors, PNL) — passivbot's own formatted output
    pb7_log = os.path.join(PB7DIR, 'logs', f'{name}.log')

    # collect old pb7 log files (non-symlink, newest-first by mtime)
    pb7_old_files = []
    try:
        import glob as _glob2
        log_real = os.path.realpath(pb7_log) if os.path.isfile(pb7_log) else ''
        for fp in sorted(
            _glob2.glob(os.path.join(PB7DIR, 'logs', '*' + name + '*.log')),
            key=os.path.getmtime, reverse=True
        ):
            if not os.path.isfile(fp):
                continue
            if os.path.islink(fp):
                continue
            if log_real and os.path.realpath(fp) == log_real:
                continue
            pb7_old_files.append(fp)
    except Exception as exc:
        _log(SERVICE, f'[monitor-logs] Failed to collect old PB7 logs for {name}: {exc}', level='WARNING')

    # stderr capture (traceback source, wrapper-timestamped)
    err_log = os.path.join(cfg_dir, 'passivbot_err.log')
    old_err = os.path.join(cfg_dir, 'passivbot_err.log.old')

    bot_entry = bot_logs.setdefault(name, {'errors': [], 'tracebacks': [], 'sidebar': []})
    bot_entry['errors'] = list(pb7_old_files)
    if os.path.isfile(pb7_log):
        bot_entry['errors'].append(pb7_log)
    bot_entry['tracebacks'] = [fp for fp in (old_err, err_log) if os.path.isfile(fp)]

    bc.setdefault('log_off', 0)
    bc.setdefault('err_off', 0)

    first_run = name not in host_cache
    today_start = TODAY_START
    yesterday_start = YESTERDAY_START

    if first_run:
        # errors/PNL: read old files until yesterday covered, then current log
        for fp in pb7_old_files:
            if not os.path.isfile(fp):
                continue
            earliest = _read_pb7_file(fp, 'count', today_start, yesterday_start, bc=bc)
            if earliest is not None and earliest < yesterday_start:
                break
        if os.path.isfile(pb7_log):
            _read_pb7_file(pb7_log, 'count', today_start, yesterday_start, bc=bc)
        bc['log_off'] = os.path.getsize(pb7_log) if os.path.isfile(pb7_log) else 0
        bc['log_fp'] = os.path.realpath(pb7_log) if os.path.isfile(pb7_log) else ''
        bc['log_sig'] = _file_start_sig(pb7_log) if os.path.isfile(pb7_log) else ''
        # tracebacks: read err_log and its .old
        for fp in (old_err, err_log):
            if os.path.isfile(fp):
                _read_err_file(fp, 'count', today_start, yesterday_start, bc=bc)
        bc['err_off'] = os.path.getsize(err_log) if os.path.isfile(err_log) else 0
        bc['err_sig'] = _file_start_sig(err_log) if os.path.isfile(err_log) else ''
    else:
        # incremental read: pb7 log
        if os.path.isfile(pb7_log):
            try:
                current_log_fp = os.path.realpath(pb7_log)
                current_log_sig = _file_start_sig(pb7_log)
                prev_log_fp = bc.get('log_fp', '')
                prev_log_sig = bc.get('log_sig', '')
                offset = bc['log_off']
                size = os.path.getsize(pb7_log)
                rotated = bool(offset and prev_log_sig and current_log_sig and prev_log_sig != current_log_sig)
                if prev_log_fp and current_log_fp and prev_log_fp != current_log_fp:
                    if os.path.isfile(prev_log_fp):
                        _read_pb7_tail(prev_log_fp, offset, today_start, yesterday_start, bc)
                    offset = 0
                elif offset > size or rotated:
                    offset = 0
                bc['log_off'] = _read_pb7_tail(pb7_log, offset, today_start, yesterday_start, bc)
                bc['log_fp'] = current_log_fp
                bc['log_sig'] = current_log_sig
            except Exception as exc:
                _log(SERVICE, f'[monitor-logs] Failed to update PB7 log counters for {name}: {exc}', level='WARNING')
        # incremental read: err_log
        if os.path.isfile(err_log):
            try:
                offset = bc['err_off']
                size = os.path.getsize(err_log)
                current_err_sig = _file_start_sig(err_log)
                prev_err_sig = bc.get('err_sig', '')
                rotated = bool(offset and prev_err_sig and current_err_sig and prev_err_sig != current_err_sig)
                if offset > size and os.path.isfile(old_err):
                    _read_err_tail(old_err, offset, today_start, yesterday_start, bc)
                    offset = 0
                elif rotated and os.path.isfile(old_err):
                    _read_err_tail(old_err, offset, today_start, yesterday_start, bc)
                    offset = 0
                bc['err_off'] = _read_err_tail(err_log, offset, today_start, yesterday_start, bc)
                bc['err_sig'] = current_err_sig
            except Exception as exc:
                _log(SERVICE, f'[monitor-logs] Failed to update error log counters for {name}: {exc}', level='WARNING')

    # build monitor dict
    monitors.append({
        'u': name, 'p': '7', 'v': version, 'st': start_ts,
        'm': [0]*10, 'c': 0.0,
        'i': '', 'it': 0, 'iy': 0, 'e': '', 't': '',
        'et': bc['et'],
        'tt': bc['tt'],
        'pt': bc['pt'],
        'ct': bc['ct'],
    })
    new_cache[name] = {
        'today': bc['today'],
        'et': bc['et'], 'tt': bc['tt'],
        'ct': bc['ct'], 'pt': bc['pt'],
        'log_off': bc['log_off'], 'err_off': bc['err_off'], 'log_fp': bc['log_fp'], 'log_sig': bc['log_sig'], 'err_sig': bc['err_sig'],
    }

candidate_names = set()
run_v7_root = os.path.join(PBGDIR, 'data', 'run_v7')
try:
    if os.path.isdir(run_v7_root):
        for item in os.listdir(run_v7_root):
            if os.path.isfile(os.path.join(run_v7_root, item, 'config.json')):
                candidate_names.add(item)
except Exception as exc:
    _log(SERVICE, f'[monitor-candidates] Failed to scan run_v7 candidates: {exc}', level='WARNING')
if cluster_context.get('configured'):
    node_id = str(cluster_context.get('node_id') or '')
    for name, item in (cluster_context.get('instances') or {}).items():
        if isinstance(item, dict) and str(item.get('assigned_host') or '') == node_id:
            candidate_names.add(str(name))

for name in sorted(candidate_names):
    if name in running:
        continue
    cfg_dir = os.path.join(run_v7_root, name)
    meta = _config_meta(cfg_dir)
    gate = _cluster_gate_status(name, cfg_dir, meta.get('version', 0), cluster_context)
    v7.append({
        'name': name,
        'running': False,
        'cv': meta.get('version', 0),
        'eo': meta.get('enabled_on', 'disabled'),
        'rv': 0,
        'di': bool(meta.get('dynamic_ignore', False)),
        'blocked': bool(gate.get('blocked', False)),
        'blocked_reason': str(gate.get('blocked_reason', '') or ''),
        'cluster_gate': str(gate.get('cluster_gate', '') or ''),
    })

print(json.dumps({'monitors': monitors, 'v7': v7, 'cache': new_cache,
    'bot_logs': bot_logs}))
"'''



HOST_META_SCRIPT = r'''python3 -u - <<'PY'
import configparser, hashlib, json, os, platform, re, subprocess, sys, time
from pathlib import Path

HOME = os.path.expanduser('~')
PBGDIR = os.path.join(HOME, '__PBGDIR__')
INI_PATH = os.path.join(PBGDIR, 'pbgui.ini')


def run(cmd, timeout=10):
    try:
        res = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=timeout,
        )
        if res.returncode == 0:
            return (res.stdout or '').strip()
    except Exception:
        pass
    return ''


def run_with_result(cmd, timeout=10, env=None):
    try:
        return subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout,
            env=env,
        )
    except Exception:
        return None


def read_pbgui_version(root):
    version_file = Path(root) / 'pbgui_purefunc.py'
    try:
        if version_file.exists():
            content = version_file.read_text(encoding='utf-8', errors='ignore')
            match = re.search(r'PBGUI_VERSION\s*=\s*["\']([^"\']+)["\']', content)
            if match:
                return match.group(1)
    except Exception:
        pass
    return 'N/A'


def read_pb7_version(pb7dir):
    if not pb7dir:
        return 'N/A'
    root = Path(pb7dir)
    version_file = root / 'src' / 'passivbot_version.py'
    try:
        if version_file.exists():
            content = version_file.read_text(encoding='utf-8', errors='ignore')
            match = re.search(r'__version__\s*=\s*[\"\']([^\"\']+)[\"\']', content)
            if match:
                return 'v' + match.group(1)
    except Exception:
        pass
    git_dir = root / '.git'
    if git_dir.exists():
        described = run(['git', '--git-dir', str(git_dir), 'describe', '--tags', '--always'], timeout=10)
        if described:
            return described if described.startswith('v') else 'v' + described
    readme = root / 'README.md'
    if readme.exists():
        try:
            for line in readme.read_text(encoding='utf-8', errors='ignore').splitlines()[:30]:
                match = re.search(r'v[0-9.]+', line)
                if match:
                    return match.group(0)
        except Exception:
            pass
    return 'N/A'


def read_pb7_config_schema(pb7dir):
    if not pb7dir:
        return 'N/A'
    schema_file = Path(pb7dir) / 'src' / 'config' / 'schema.py'
    try:
        if schema_file.exists():
            content = schema_file.read_text(encoding='utf-8', errors='ignore')
            match = re.search(r'CONFIG_SCHEMA_VERSION\s*=\s*["\']([^"\']+)["\']', content)
            if match:
                return match.group(1)
    except Exception:
        pass
    return 'N/A'


def git_value(git_dir, args, default=''):
    if not git_dir or not Path(git_dir).exists():
        return default
    value = run(['git', '--git-dir', git_dir] + list(args), timeout=10)
    return value or default


def python_version(exe):
    if not exe or not Path(exe).exists():
        return ''
    return run([exe, '-c', 'import sys; print(f\"{sys.version_info.major}.{sys.version_info.minor}\")'], timeout=5)


cfg = configparser.ConfigParser()
try:
    cfg.read(INI_PATH)
except Exception:
    pass

def config_value(section, option):
    value = str(cfg.get(section, option, fallback='') or '').strip()
    return value

def config_bool(section, option, default=False):
    raw = config_value(section, option)
    if raw == '':
        return bool(default)
    return raw.lower() in ('1', 'true', 'yes', 'on')

def systemd_user_env():
    env = os.environ.copy()
    env['XDG_RUNTIME_DIR'] = env.get('XDG_RUNTIME_DIR') or f"/run/user/{os.getuid()}"
    return env


def systemctl_user(args, timeout=5):
    return run_with_result(['systemctl', '--user'] + list(args), timeout=timeout, env=systemd_user_env())


def systemd_user_manager_ok(systemctl_exists):
    if not systemctl_exists:
        return False, 'systemctl missing'
    res = systemctl_user(['show-environment'], timeout=5)
    if res is not None and res.returncode == 0:
        return True, 'active'
    return False, 'not active'


def systemd_unit_status(unit, systemctl_exists):
    unit_dir = Path(HOME) / '.config' / 'systemd' / 'user'
    exists = (unit_dir / unit).exists()
    active = 'unknown'
    enabled = 'unknown'
    if systemctl_exists:
        active_res = systemctl_user(['is-active', unit], timeout=5)
        enabled_res = systemctl_user(['is-enabled', unit], timeout=5)
        if active_res is not None:
            active = (active_res.stdout or '').strip() or 'unknown'
        if enabled_res is not None:
            enabled = (enabled_res.stdout or '').strip() or 'unknown'
    return {
        'unit': unit,
        'exists': 'yes' if exists else 'no',
        'enabled': enabled,
        'active': active,
    }


def ignored_process_ids():
    ignored = {str(os.getpid())}
    pid = os.getpid()
    while True:
        try:
            stat = Path(f'/proc/{pid}/stat').read_text(encoding='utf-8', errors='replace')
            ppid = stat.rsplit(') ', 1)[1].split()[1]
        except Exception:
            break
        if not ppid or ppid == '0' or ppid in ignored:
            break
        ignored.add(ppid)
        try:
            pid = int(ppid)
        except ValueError:
            break
    return ignored


def collect_legacy_pbgui_processes(pbgui_dir):
    target_dir = os.path.realpath(pbgui_dir)
    target_prefix = target_dir + os.sep
    scripts = ('PBCluster.py', 'PBRun.py', 'PBCoinData.py', 'starter.py')
    unit_by_script = {
        'PBCluster.py': 'pbgui-pbcluster.service',
        'PBRun.py': 'pbgui-pbrun.service',
        'PBCoinData.py': 'pbgui-pbcoindata.service',
    }
    ignored = ignored_process_ids()
    out = []

    def matching_script(cmd):
        for script in scripts:
            if script in cmd:
                return script
        return None

    def is_systemd_managed(pid, script):
        unit = unit_by_script.get(script)
        if not unit:
            return False
        try:
            cgroup = Path(f'/proc/{pid}/cgroup').read_text(encoding='utf-8', errors='replace')
        except Exception:
            return False
        return unit in cgroup

    for entry in Path('/proc').iterdir():
        if not entry.name.isdigit() or entry.name in ignored:
            continue
        try:
            raw = (entry / 'cmdline').read_bytes()
        except Exception:
            continue
        cmd = raw.replace(b'\0', b' ').decode('utf-8', errors='replace').strip()
        try:
            cwd = os.path.realpath(os.readlink(entry / 'cwd'))
        except Exception:
            cwd = ''
        script = matching_script(cmd)
        if cmd and script and (target_prefix in cmd or cwd == target_dir) and not is_systemd_managed(entry.name, script):
            out.append(f'{entry.name} {cmd}')
    return out


def collect_legacy_cron_lines(pbgui_dir):
    legacy_start = str(Path(pbgui_dir) / 'start.sh')
    tokens = {legacy_start}
    home = str(Path(HOME).resolve())
    if legacy_start.startswith(home.rstrip('/') + '/'):
        rel = legacy_start[len(home.rstrip('/')) + 1:]
        tokens.update({f'~/{rel}', '$HOME/' + rel, '$' + '{HOME}/' + rel})
    res = run_with_result(['crontab', '-l'], timeout=5)
    if res is None or res.returncode != 0:
        return []
    return [line for line in (res.stdout or '').splitlines() if any(token in line for token in tokens) or line.strip() == '#Ansible: pbgui']


def build_systemd_migration_status(pbgui_dir, pbrun_configured, pbdata_configured, credential_active):
    pbgui_path = Path(pbgui_dir)
    python_bin = pbgui_path.parent / 'venv_pbgui' / 'bin' / 'python'
    systemctl_path = run(['which', 'systemctl'], timeout=5)
    systemctl_exists = bool(systemctl_path)
    user_manager_ok, user_manager_detail = systemd_user_manager_ok(systemctl_exists)
    all_unit_names = ['pbgui-pbcluster.service', 'pbgui-pbrun.service', 'pbgui-monitor-agent.service', 'pbgui-pbdata.service', 'pbgui-pbcoindata.service']
    required_unit_names = ['pbgui-pbcluster.service', 'pbgui-monitor-agent.service']
    if pbrun_configured:
        required_unit_names.append('pbgui-pbrun.service')
    if pbdata_configured:
        required_unit_names.append('pbgui-pbdata.service')
    units = [systemd_unit_status(unit, systemctl_exists) for unit in all_unit_names]
    by_name = {item['unit']: item for item in units}
    coindata_unit = by_name.get('pbgui-pbcoindata.service', {})
    if credential_active is True or (
        credential_active is None
        and (coindata_unit.get('enabled') == 'enabled' or coindata_unit.get('active') == 'active')
    ):
        required_unit_names.append('pbgui-pbcoindata.service')
    required_units = [by_name[unit] for unit in required_unit_names if unit in by_name]
    units_missing = [item for item in required_units if item.get('exists') != 'yes']
    units_not_enabled = [item for item in required_units if item.get('enabled') != 'enabled']
    units_inactive = [item for item in required_units if item.get('active') != 'active']
    units_ready = bool(required_units) and not units_missing and not units_not_enabled and not units_inactive
    legacy_processes = collect_legacy_pbgui_processes(str(pbgui_path)) if pbgui_path.exists() else []
    legacy_cron_lines = collect_legacy_cron_lines(str(pbgui_path))
    start_sh_exists = (pbgui_path / 'start.sh').exists()
    blockers = []
    if not pbgui_path.is_dir():
        blockers.append('PBGui directory missing')
    if not python_bin.exists():
        blockers.append('PBGui virtualenv Python missing')
    if not systemctl_exists:
        blockers.append('systemctl missing')
    migration_complete = bool(
        pbgui_path.is_dir()
        and python_bin.exists()
        and systemctl_exists
        and user_manager_ok
        and units_ready
        and not legacy_processes
        and not legacy_cron_lines
        and not start_sh_exists
    )
    state = 'complete' if migration_complete else ('blocked' if blockers else 'needed')
    return {
        'state': state,
        'available': True,
        'migration_complete': migration_complete,
        'migration_needed': not migration_complete,
        'units_ready': units_ready,
        'required_units': required_units,
        'units': units,
        'legacy_process_count': len(legacy_processes),
        'legacy_cron_count': len(legacy_cron_lines),
        'legacy_start_sh_exists': start_sh_exists,
        'systemd_user_manager': user_manager_ok,
        'systemd_user_manager_detail': user_manager_detail,
        'pbgui_dir_exists': pbgui_path.is_dir(),
        'python_exists': python_bin.exists(),
        'systemctl_exists': systemctl_exists,
        'checked_at': int(time.time()),
        'error': '',
        'blockers': blockers,
    }

def parsed_list_config(raw):
    text = str(raw or '').strip()
    if not text:
        return []
    try:
        import ast
        parsed = ast.literal_eval(text)
        if isinstance(parsed, (list, tuple, set)):
            return [str(item).strip() for item in parsed if str(item).strip()]
    except Exception:
        pass
    return [part.strip() for part in text.split(',') if part.strip()]

def pbrun_required_for_host(pbgui_dir, pbname):
    run_root = Path(pbgui_dir) / 'data' / 'run_v7'
    target = str(pbname or '').strip()
    if not target or not run_root.is_dir():
        return False
    for cfg_path in run_root.glob('*/config.json'):
        try:
            payload = json.loads(cfg_path.read_text(encoding='utf-8'))
        except Exception:
            continue
        pbgui = payload.get('pbgui') if isinstance(payload, dict) else None
        enabled_on = str((pbgui or {}).get('enabled_on') or '').strip()
        if enabled_on and enabled_on != 'disabled' and enabled_on == target:
            return True
    return False

def pbdata_required():
    if not cfg.has_section('pbdata'):
        return False
    fetch_users = parsed_list_config(cfg.get('pbdata', 'fetch_users', fallback=''))
    trades_users = parsed_list_config(cfg.get('pbdata', 'trades_users', fallback=''))
    return bool(fetch_users or trades_users)

def credential_metadata():
    unknown = {
        'credential_protocol_version': 2,
        'credential_active': None,
        'credential_reason': 'CMC credential pool status unavailable',
        'cmc_catalog_generation': None,
        'cmc_materialized_generation': None,
        'cmc_active_key_count': None,
        'cmc_provider_used': None,
        'cmc_provider_limit': None,
    }
    if os.environ.get('PBGUI_SKIP_CREDENTIAL_METADATA') == '1':
        return unknown
    try:
        if PBGDIR not in sys.path:
            sys.path.insert(0, PBGDIR)
        from cmc_pool import CmcPoolClient
        from credential_store import CredentialStore
        from master.cluster_state import default_cluster_root, local_cmc_credential_readiness, read_local_identity
        cluster_root = default_cluster_root(Path(PBGDIR))
        materialized = {
            'cluster_nodes': json.loads((cluster_root / 'cluster_nodes.json').read_text(encoding='utf-8')),
            'desired_state': json.loads((cluster_root / 'desired_state.json').read_text(encoding='utf-8')),
        }
        store = CredentialStore(Path(PBGDIR) / 'data' / 'credentials')
        records = store.list_cmc(active_only=False)
        status = CmcPoolClient(
            credential_store=store,
            state_root=store.root / 'cmc_pool',
            desired_state_provider=lambda: materialized,
        ).status()
    except Exception:
        return unknown
    active_count = max(int(status.get('active_credentials') or 0), 0)
    try:
        node_id = str(read_local_identity(cluster_root)['node_id'])
        unknown.update(local_cmc_credential_readiness(materialized, node_id, records))
        authorities = (((materialized.get('desired_state') or {}).get('cmc_pool') or {}).get('authorities') or {})
        authority_timestamps = [float(item.get('updated_at') or 0) for item in authorities.values() if isinstance(item, dict) and item.get('updated_at')]
        if authority_timestamps:
            unknown['cmc_authority_state_age_seconds'] = round(max(time.time() - max(authority_timestamps), 0.0), 1)
    except Exception:
        standalone = [record for record in records if isinstance(record, dict) and record.get('active') and not record.get('pending')]
        generation = max((int(record.get('generation') or 0) for record in standalone), default=0)
        unknown.update({
            'credential_active': bool(standalone),
            'credential_reason': 'Standalone local CMC credential pool active' if standalone else 'No active standalone CMC credentials',
            'cmc_catalog_generation': generation,
            'cmc_materialized_generation': generation,
            'cmc_active_key_count': len(standalone),
            'cluster_origin_metadata': False,
        })
    if unknown.get('credential_active') is True and active_count < 1:
        unknown['credential_active'] = False
        unknown['credential_reason'] = 'CMC pool has no exact eligible credential'
    provider_observations = []
    for key_status in status.get('keys') or []:
        if not isinstance(key_status, dict):
            continue
        try:
            timestamp = float(key_status.get('provider_usage_updated_at') or key_status.get('last_settled_at') or 0)
        except (TypeError, ValueError):
            timestamp = 0
        if timestamp > 0:
            provider_observations.append((timestamp, key_status))
    if provider_observations:
        provider_timestamp, provider_status = max(provider_observations, key=lambda item: item[0])
        unknown['cmc_provider_usage_age_seconds'] = round(max(time.time() - provider_timestamp, 0.0), 1)
        for source, target in (('provider_used', 'cmc_provider_used'), ('provider_limit', 'cmc_provider_limit')):
            try:
                unknown[target] = max(float(provider_status[source]), 0.0) if provider_status.get(source) is not None else None
            except (TypeError, ValueError):
                unknown[target] = None
    authority_reachable = status.get('authority_reachable')
    if isinstance(authority_reachable, bool):
        unknown['cmc_authority_reachable'] = authority_reachable
    return unknown

role = cfg.get('main', 'role', fallback='slave')
pbname = cfg.get('main', 'pbname', fallback=platform.node()).strip() or platform.node()
pb7dir = cfg.get('main', 'pb7dir', fallback='')
pb7venv = cfg.get('main', 'pb7venv', fallback='')
firewall_settings_present = cfg.has_section('firewall')
firewall_enabled = config_bool('firewall', 'enabled', False)
firewall_ssh_port = config_value('firewall', 'ssh_port') or '22'
firewall_ssh_ips = config_value('firewall', 'ssh_ips')
pbrun_configured = pbrun_required_for_host(PBGDIR, pbname)
pbdata_configured = pbdata_required()
credentials = credential_metadata()

result = {
    'role': role,
    'boot': 0,
    'api_md5': '',
    'reboot': os.path.exists('/var/run/reboot-required'),
    'pbgv': read_pbgui_version(PBGDIR),
    'pbgc': '',
    'pbgb': 'unknown',
    'pbgpy': 'N/A',
    'pb7v': read_pb7_version(pb7dir),
    'pb7_config_schema': read_pb7_config_schema(pb7dir),
    'pb7c': '',
    'pb7b': 'unknown',
    'pb7py': 'N/A',
    **credentials,
    'firewall_settings_present': firewall_settings_present,
    'firewall': firewall_enabled,
    'firewall_ssh_port': firewall_ssh_port,
    'firewall_ssh_ips': firewall_ssh_ips,
    'optional_services': {
        'PBRun': pbrun_configured,
        'PBData': pbdata_configured,
        'PBCoinData': credentials['credential_active'],
    },
}

try:
    with open('/proc/stat', encoding='utf-8') as f:
        for line in f:
            if line.startswith('btime '):
                result['boot'] = int(line.split()[1])
                break
except Exception:
    pass

api_keys = Path(pb7dir) / 'api-keys.json' if pb7dir else None
if api_keys and api_keys.exists():
    try:
        result['api_md5'] = hashlib.md5(api_keys.read_bytes()).hexdigest()
    except Exception:
        pass

pbgui_git = str(Path(PBGDIR) / '.git')
result['pbgc'] = git_value(pbgui_git, ['log', '-n', '1', '--pretty=format:%H'])
result['pbgb'] = git_value(pbgui_git, ['rev-parse', '--abbrev-ref', 'HEAD'], 'unknown')

pb7_git = str(Path(pb7dir) / '.git') if pb7dir else ''
result['pb7c'] = git_value(pb7_git, ['log', '-n', '1', '--pretty=format:%H'])
result['pb7b'] = git_value(pb7_git, ['rev-parse', '--abbrev-ref', 'HEAD'], 'unknown')

for candidate in (
    str(Path(PBGDIR) / '.venv' / 'bin' / 'python'),
    str(Path(PBGDIR).parent / 'venv_pbgui' / 'bin' / 'python'),
):
    version = python_version(candidate)
    if version:
        result['pbgpy'] = version
        break
if result['pbgpy'] == 'N/A':
    result['pbgpy'] = f'{sys.version_info.major}.{sys.version_info.minor}'

pb7_python = python_version(pb7venv)
if pb7_python:
    result['pb7py'] = pb7_python

available = []
logs_dir = os.path.join(PBGDIR, 'data', 'logs')
if os.path.isdir(logs_dir):
    for f in sorted(os.listdir(logs_dir)):
        full = os.path.join(logs_dir, f)
        if os.path.isfile(full) and (f.endswith('.log') or f.endswith('.log.old')):
            available.append('data/logs/' + f)
result['available_logs'] = available
result['systemd_migration'] = build_systemd_migration_status(PBGDIR, pbrun_configured, pbdata_configured, credentials['credential_active'])

print(json.dumps(result))
PY'''

PACKAGE_STATUS_SCRIPT = r'''python3 -u -c "
import json, os, re, subprocess

result = {
    'upgrades': 'N/A',
    'reboot': os.path.exists('/var/run/reboot-required'),
}
env = os.environ.copy()
env['LANG'] = 'C'
try:
    res = subprocess.run(
        ['apt-get', 'dist-upgrade', '-s'],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        timeout=60,
        env=env,
    )
    if res.returncode == 0:
        match = re.search(r'(\d+) upgraded', res.stdout or '')
        if match:
            result['upgrades'] = match.group(1)
except Exception:
    pass

print(json.dumps(result))
"'''


# ── Service definitions ─────────────────────────────────────

class ServiceStatus(Enum):
    RUNNING = "running"
    STOPPED = "stopped"
    UNKNOWN = "unknown"
    RESTARTING = "restarting"
    DISABLED = "disabled"


@dataclass
class ServiceInfo:
    name: str
    pid_file: str       # relative to PBGUI dir
    script_file: str    # Python script to run
    process_match: str  # grep string in cmdline


MONITORED_SERVICES = {
    "PBCluster": ServiceInfo("PBCluster", "data/pid/pbcluster.pid",
                             "PBCluster.py", "pbcluster.py"),
    "PBRun": ServiceInfo("PBRun", "data/pid/pbrun.pid",
                          "PBRun.py", "pbrun.py"),
    "PBData": ServiceInfo("PBData", "data/pid/pbdata.pid",
                           "PBData.py", "pbdata.py"),
    "PBCoinData": ServiceInfo("PBCoinData", "data/pid/pbcoindata.pid",
                               "PBCoinData.py", "pbcoindata.py"),
    "PBMonitorAgent": ServiceInfo("PBMonitorAgent", "data/pid/pbmonitoragent.pid",
                                  "monitor_agent.py", "monitor_agent.py"),
}

MONITORED_SERVICE_SYSTEMD_UNITS = {
    "PBCluster": "pbgui-pbcluster.service",
    "PBRun": "pbgui-pbrun.service",
    "PBData": "pbgui-pbdata.service",
    "PBCoinData": "pbgui-pbcoindata.service",
    "PBMonitorAgent": "pbgui-monitor-agent.service",
}

PBCLUSTER_DISABLED_REASON = "Cluster Sync is not enabled for this node"

OPTIONAL_SERVICE_REQUIREMENTS = {
    "PBRun": {
        "reason": "No local V7 run configs are enabled for this host",
    },
    "PBData": {
        "reason": "No PBData fetch_users or trades_users are configured",
    },
}


# ── Main orchestrator ───────────────────────────────────────

class VPSMonitor:
    """
    Async VPS monitoring orchestrator.

    Lifecycle:
        monitor = VPSMonitor()
        await monitor.start()   # launches all background tasks
        ...
        await monitor.stop()    # cancels everything, disconnects
    """

    def __init__(self):
        self.pool = AsyncSSHPool()
        self.store = VPSStore()
        self.loop: Optional[asyncio.AbstractEventLoop] = None

        # Config
        self._auto_restart: Optional[bool] = True
        self._enabled_hosts: Optional[set[str]] = set()

        # Telegram
        self._telegram_token = ""
        self._telegram_chat_id = ""

        # Alert state
        self._alert_state_path = Path(PBGDIR) / 'data' / 'state' / 'vps_monitor' / 'alerts.json'
        self._legacy_alert_state_path = Path(PBGDIR) / 'data' / 'vps_alert_state.json'
        self._alerts: dict[str, AlertRecord] = {}
        self._alert_gui_routes: dict[str, bool] = dict(ALERT_ROUTE_GUI_DEFAULTS)
        self._alert_telegram_routes: dict[str, bool] = dict(ALERT_ROUTE_TELEGRAM_DEFAULTS)
        self._alert_routes_loaded = False
        self._hl_expiry_last_warned: dict[str, str] = {}

        # Restart rate limiting
        self._restart_history: dict[str, dict[str, list[datetime]]] = {}
        self.max_restarts_per_hour = 3

        # Instance collection timing
        self._last_instance_collect: float = 0.0
        self._last_host_meta_collect: dict[str, float] = {}
        self._last_package_status_collect: dict[str, float] = {}
        self._host_meta_collecting: set[str] = set()

        # Monitor cache (persisted across restarts, per-host per-bot GZ state)
        self._cache_path = Path(PBGDIR) / 'data' / 'state' / 'vps_monitor' / 'cache.json'
        self._legacy_cache_path = Path(PBGDIR) / 'data' / 'monitor_cache.json'
        self._monitor_cache: dict[str, dict[str, dict]] = {}
        self._load_monitor_cache()
        history_dir = Path(PBGDIR) / 'data' / 'state' / 'vps_monitor' / 'history'
        self._host_metric_history = {
            'cpu': CpuHistoryStore(history_dir, 'hosts_cpu_24h'),
            'memory': CpuHistoryStore(history_dir, 'hosts_memory_24h'),
            'disk': CpuHistoryStore(history_dir, 'hosts_disk_24h'),
            'swap': CpuHistoryStore(history_dir, 'hosts_swap_24h'),
        }
        self._bot_cpu_history = CpuHistoryStore(history_dir, 'bots_cpu_24h')
        self._bot_metric_history = {
            'memory': BotMetricHistoryStore(
                history_dir,
                'bots_memory_24h',
                resolution=BOT_MEMORY_HISTORY_RESOLUTION_MB,
                max_value=BOT_MEMORY_HISTORY_MAX_MB,
            ),
            'swap': BotMetricHistoryStore(
                history_dir,
                'bots_swap_24h',
                resolution=BOT_SWAP_HISTORY_RESOLUTION_MB,
                max_value=BOT_SWAP_HISTORY_MAX_MB,
            ),
        }
        self._bot_count_history = {
            'errors': BotCountHistoryStore(history_dir, 'bots_errors_4w'),
            'tracebacks': BotCountHistoryStore(history_dir, 'bots_tracebacks_4w'),
        }
        self._bot_pnl_history = BotPnlHistoryStore(history_dir, 'bots_pnl_history')
        self._local_master_cpu_history: list[tuple[float, float, float]] = []
        self._local_master_metric_history: dict[str, list[tuple[float, float]]] = {
            'memory': [],
            'disk': [],
            'swap': [],
        }

        # Debug logging
        self._debug_logging: Optional[bool] = False

        # ini watcher (thread-based, fine alongside asyncio)
        self._ini_watcher = IniWatcher()
        self._config_changed = asyncio.Event()
        self._config_signature = None
        self._monitor_config = object.__new__(MonitorConfig)
        self._monitor_config.server = None
        self._monitor_config.servers = []
        self._monitor_config.logfiles = []
        for key, value in MONITOR_DEFAULTS.items():
            setattr(self._monitor_config, key, value)
        self._config_retry_count = 0
        self._config_retry_task: asyncio.Task | None = None

        # Background tasks
        self._tasks: list[asyncio.Task] = []
        self._stream_tasks: dict[str, asyncio.Task] = {}
        self._stream_generations: dict[str, int] = {}
        self._stream_started_at: dict[str, float] = {}
        self._stream_stale_counts: dict[str, int] = {}
        self._stream_stale_last_logged: dict[str, float] = {}
        self._running = False

    # ── Config ──────────────────────────────────────────────

    @staticmethod
    def _snapshot_value(snapshot: IniSnapshot, section: str, key: str, default: str) -> str:
        return snapshot.get(section, key).strip() if snapshot.has_option(section, key) else default

    @classmethod
    def _snapshot_bool(cls, snapshot: IniSnapshot, section: str, key: str, default: bool) -> bool:
        raw = cls._snapshot_value(snapshot, section, key, "").lower()
        if not raw:
            return default
        if raw in {"1", "true", "yes", "on"}:
            return True
        if raw in {"0", "false", "no", "off"}:
            return False
        raise ValueError(f"Invalid INI value [{section}] {key}")

    @classmethod
    def _build_config_candidate(cls, snapshot: IniSnapshot) -> VPSMonitorConfigCandidate:
        enabled_raw = cls._snapshot_value(snapshot, "vps_monitor", "enabled_hosts", "")
        enabled_hosts = frozenset(host.strip() for host in enabled_raw.split(",") if host.strip())
        monitor_values: dict[str, float] = {}
        for key, default in MONITOR_DEFAULTS.items():
            raw = cls._snapshot_value(snapshot, "monitor", key, str(default))
            try:
                value = float(raw)
            except ValueError as exc:
                raise ValueError(f"Invalid INI value [monitor] {key}") from exc
            if not math.isfinite(value):
                raise ValueError(f"Invalid INI value [monitor] {key}")
            monitor_values[key] = value

        alert_gui_routes = {
            kind: cls._snapshot_bool(snapshot, "vps_monitor_alerts", key, default)
            for kind, default in ALERT_ROUTE_GUI_DEFAULTS.items()
            for key in [ALERT_ROUTE_GUI_KEYS[kind]]
        }
        alert_telegram_routes = {
            event: cls._snapshot_bool(snapshot, "vps_monitor_alerts", key, default)
            for event, default in ALERT_ROUTE_TELEGRAM_DEFAULTS.items()
            for key in [ALERT_ROUTE_TELEGRAM_KEYS[event]]
        }
        debug_logging = cls._snapshot_bool(snapshot, "vps_monitor", "debug_logging", False)
        ui_settings = {"debug_logging": "true" if debug_logging else "false"}
        compact = cls._snapshot_value(snapshot, "vps_monitor_ui", "compact", "")
        if compact:
            ui_settings["compact"] = compact
        return VPSMonitorConfigCandidate(
            signature=snapshot.signature,
            enabled_hosts=enabled_hosts,
            auto_restart=cls._snapshot_bool(snapshot, "vps_monitor", "auto_restart", True),
            debug_logging=debug_logging,
            telegram_token=cls._snapshot_value(snapshot, "main", "telegram_token", ""),
            telegram_chat_id=cls._snapshot_value(snapshot, "main", "telegram_chat_id", ""),
            alert_gui_routes=alert_gui_routes,
            alert_telegram_routes=alert_telegram_routes,
            monitor_values=monitor_values,
            ui_settings=ui_settings,
        )

    def _commit_config_candidate(self, candidate: VPSMonitorConfigCandidate) -> None:
        monitor_config = object.__new__(MonitorConfig)
        monitor_config.server = None
        monitor_config.servers = []
        monitor_config.logfiles = []
        for key, value in candidate.monitor_values.items():
            setattr(monitor_config, key, value)
        self._enabled_hosts = set(candidate.enabled_hosts)
        self._auto_restart = candidate.auto_restart
        self._debug_logging = candidate.debug_logging
        self._telegram_token = candidate.telegram_token
        self._telegram_chat_id = candidate.telegram_chat_id
        self._alert_gui_routes = dict(candidate.alert_gui_routes)
        self._alert_telegram_routes = dict(candidate.alert_telegram_routes)
        self._alert_routes_loaded = True
        self._monitor_config = monitor_config
        self.store._ui_settings = dict(candidate.ui_settings)
        self.store.changed.set()
        self._config_signature = candidate.signature

    @staticmethod
    def _config_error_text(exc: Exception) -> str:
        message = str(exc)
        if message.startswith("Invalid INI value ["):
            return message
        return type(exc).__name__

    @property
    def auto_restart(self) -> bool:
        if self._auto_restart is None:
            val = load_ini("vps_monitor", "auto_restart")
            self._auto_restart = val.lower() == "true" if val else True
        return self._auto_restart

    @property
    def debug_logging(self) -> bool:
        if self._debug_logging is not None:
            return self._debug_logging
        val = load_ini("vps_monitor", "debug_logging")
        return val.lower() == "true" if val else False

    @debug_logging.setter
    def debug_logging(self, value: bool):
        self._debug_logging = bool(value)
        save_ini("vps_monitor", "debug_logging", "true" if value else "false")

    @property
    def enabled_hosts(self) -> set[str]:
        if self._enabled_hosts is None:
            val = load_ini("vps_monitor", "enabled_hosts")
            if val and val.strip():
                self._enabled_hosts = {
                    h.strip() for h in val.split(",") if h.strip()
                }
            else:
                self._enabled_hosts = set()
        return self._enabled_hosts

    @staticmethod
    def _configured_optional_value(value: Any) -> bool:
        normalized = str(value or "").strip()
        if not normalized:
            return False
        lowered = normalized.lower()
        if lowered in {"none", "null", "false", "<api_key>"}:
            return False
        return not (normalized.startswith("<") and normalized.endswith(">"))

    def _local_vps_config(self, hostname: str) -> dict[str, Any] | None:
        safe_host = str(hostname or "").strip()
        if not safe_host or "/" in safe_host or "\\" in safe_host:
            return None
        config_path = Path(PBGDIR) / "data" / "vpsmanager" / "hosts" / safe_host / f"{safe_host}.json"
        if not config_path.is_file():
            return None
        try:
            payload = json.loads(config_path.read_text(encoding="utf-8"))
        except Exception as exc:
            _log(SERVICE, f"[service] Could not read VPS config for {safe_host}: {exc}", level="WARNING")
            return None
        return payload if isinstance(payload, dict) else None

    def _local_optional_service_expected(self, hostname: str, service_name: str) -> bool | None:
        requirement = OPTIONAL_SERVICE_REQUIREMENTS.get(service_name)
        if not requirement:
            return True
        config_key = requirement.get("config_key")
        if not config_key:
            return None
        config = self._local_vps_config(hostname)
        if not config or config_key not in config:
            return None
        return self._configured_optional_value(config.get(config_key))

    def _optional_service_expected(self, hostname: str, service_name: str) -> bool | None:
        if service_name == "PBCluster":
            return self._pbcluster_expected(hostname)
        meta = self.store.host_meta.get(hostname, {}) if self.store else {}
        if service_name == "PBCoinData":
            active = meta.get("credential_active") if isinstance(meta, dict) else None
            return active if isinstance(active, bool) else None
        if service_name not in OPTIONAL_SERVICE_REQUIREMENTS:
            return True
        local_expected = self._local_optional_service_expected(hostname, service_name)
        remote_expected: bool | None = None
        optional_services = meta.get("optional_services") if isinstance(meta, dict) else None
        if isinstance(optional_services, dict) and service_name in optional_services:
            remote_expected = bool(optional_services.get(service_name))

        if local_expected is False or remote_expected is False:
            return False
        if local_expected is True:
            return True
        if remote_expected is not None:
            return remote_expected

        # Unknown capability should be treated as expected to avoid hiding real failures.
        return True

    def _pbcluster_expected(self, hostname: str) -> bool:
        safe_host = str(hostname or "").strip()
        if not safe_host:
            return False
        nodes_path = Path(PBGDIR) / "data" / "cluster" / "cluster_nodes.json"
        try:
            payload = json.loads(nodes_path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return False
        except Exception as exc:
            _log(SERVICE, f"[service] Could not read Cluster node membership: {exc}", level="WARNING")
            return False
        nodes = payload.get("nodes") if isinstance(payload, dict) else None
        if not isinstance(nodes, dict):
            return False
        for node in nodes.values():
            if not isinstance(node, dict):
                continue
            names = {
                str(node.get("hostname") or "").strip(),
                str(node.get("pbname") or "").strip(),
            }
            if safe_host not in names:
                continue
            if node.get("enabled") is False:
                return False
            return bool(node.get("sync_enabled"))
        return False

    def _disabled_service_check(self, service_name: str) -> dict[str, Any]:
        requirement = OPTIONAL_SERVICE_REQUIREMENTS.get(service_name) or {}
        return {
            "status": ServiceStatus.DISABLED.value,
            "pid": None,
            "error": None,
            "was_restarted": False,
            "expected": False,
            "reason": PBCLUSTER_DISABLED_REASON if service_name == "PBCluster" else str(requirement.get("reason") or "Service is not configured"),
        }

    def _auto_restart_allowed_for_host(self, hostname: str, service_name: str) -> bool:
        """Return whether this monitor may auto-restart a service on a host."""
        del service_name
        return str(hostname or "").strip() == self._local_master_hostname()

    @property
    def telegram_token(self):
        if not self._telegram_token:
            self._telegram_token = load_ini("main", "telegram_token") or ""
        return self._telegram_token

    @property
    def telegram_chat_id(self):
        if not self._telegram_chat_id:
            self._telegram_chat_id = load_ini("main", "telegram_chat_id") or ""
        return self._telegram_chat_id

    def _load_alert_routes(self, *, force: bool = False) -> None:
        if self._alert_routes_loaded and not force:
            return
        self._alert_gui_routes = {
            kind: _read_ini_bool("vps_monitor_alerts", key, default)
            for kind, default in ALERT_ROUTE_GUI_DEFAULTS.items()
            for key in [ALERT_ROUTE_GUI_KEYS[kind]]
        }
        self._alert_telegram_routes = {
            event: _read_ini_bool("vps_monitor_alerts", key, default)
            for event, default in ALERT_ROUTE_TELEGRAM_DEFAULTS.items()
            for key in [ALERT_ROUTE_TELEGRAM_KEYS[event]]
        }
        self._alert_routes_loaded = True

    def get_alert_settings(self) -> dict[str, Any]:
        self._load_alert_routes(force=True)
        return {
            "telegram_token": self.telegram_token,
            "telegram_chat_id": self.telegram_chat_id,
            **{ALERT_ROUTE_GUI_KEYS[k]: bool(v) for k, v in self._alert_gui_routes.items()},
            **{ALERT_ROUTE_TELEGRAM_KEYS[k]: bool(v) for k, v in self._alert_telegram_routes.items()},
        }

    def save_alert_settings(self, settings: dict[str, Any]) -> None:
        main_updates = {}
        if "telegram_token" in settings:
            main_updates["telegram_token"] = str(settings.get("telegram_token") or "").strip()
        if "telegram_chat_id" in settings:
            main_updates["telegram_chat_id"] = str(settings.get("telegram_chat_id") or "").strip()
        alert_updates = {
            ini_key: _bool_to_ini(bool(settings.get(ini_key)))
            for ini_key in (*ALERT_ROUTE_GUI_KEYS.values(), *ALERT_ROUTE_TELEGRAM_KEYS.values())
            if ini_key in settings
        }

        def mutate(parser) -> None:
            for section, values in (("main", main_updates), ("vps_monitor_alerts", alert_updates)):
                if values and not parser.has_section(section):
                    parser.add_section(section)
                for key, value in values.items():
                    parser.set(section, key, value)

        if main_updates or alert_updates:
            update_ini(mutate)
        if "telegram_token" in main_updates:
            self._telegram_token = main_updates["telegram_token"]
        if "telegram_chat_id" in main_updates:
            self._telegram_chat_id = main_updates["telegram_chat_id"]
        self._load_alert_routes(force=True)

    def _load_alert_state(self) -> None:
        self._alerts = {}
        self._hl_expiry_last_warned = {}
        try:
            state_path = self._alert_state_path
            if not state_path.exists() and self._legacy_alert_state_path.exists():
                state_path = self._legacy_alert_state_path
            if not state_path.exists():
                return
            payload = json.loads(state_path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                return
            alerts = payload.get("alerts") or {}
            if isinstance(alerts, dict):
                for alert_id, item in alerts.items():
                    if not isinstance(item, dict):
                        continue
                    alert = AlertRecord.from_dict(item)
                    alert.id = str(alert_id or alert.id)
                    if alert.id:
                        alert.active = False
                        self._alerts[alert.id] = alert
            warned = payload.get("hl_expiry_last_warned") or {}
            if isinstance(warned, dict):
                self._hl_expiry_last_warned = {str(k): str(v) for k, v in warned.items() if str(k).strip()}
        except Exception as e:
            _log(SERVICE, f"[alerts] failed to load alert state: {e}", level="WARNING")

    def _save_alert_state(self) -> None:
        payload = {
            "version": ALERT_STATE_VERSION,
            "alerts": {alert_id: alert.to_dict() for alert_id, alert in self._alerts.items()},
            "hl_expiry_last_warned": dict(self._hl_expiry_last_warned),
        }
        try:
            _write_json_atomic(self._alert_state_path, payload)
        except Exception as e:
            _log(SERVICE, f"[alerts] failed to save alert state: {e}", level="WARNING")

    def _prune_alert_history(self, now_ts: float | None = None) -> bool:
        cutoff = float(now_ts or time.time()) - ALERT_HISTORY_RETENTION_SECONDS
        removed = False
        for alert_id, alert in list(self._alerts.items()):
            if alert.active:
                continue
            resolved_ts = float(alert.last_seen_ts or 0.0)
            if resolved_ts <= 0.0 or resolved_ts >= cutoff:
                continue
            self._alerts.pop(alert_id, None)
            removed = True
        return removed

    def list_active_alerts(self, *, gui_only: bool = False) -> list[dict[str, Any]]:
        self._load_alert_routes()
        items = []
        for alert in self._alerts.values():
            if not alert.active:
                continue
            if gui_only and not self._alert_gui_routes.get(alert.kind, True):
                continue
            items.append(alert.to_dict())
        items.sort(key=lambda item: (
            item.get("acknowledged", False),
            -_severity_rank(str(item.get("severity") or "")),
            str(item.get("host") or ""),
            str(item.get("name") or ""),
        ))
        return items

    def list_alert_history(self, *, gui_only: bool = False, limit: int = 0) -> list[dict[str, Any]]:
        self._load_alert_routes()
        pruned = self._prune_alert_history()
        items = []
        for alert in self._alerts.values():
            if alert.active:
                continue
            if gui_only and not self._alert_gui_routes.get(alert.kind, True):
                continue
            payload = alert.to_dict()
            payload["id"] = _alert_history_id(alert)
            payload["resolved_ts"] = float(alert.last_seen_ts or 0.0)
            items.append(payload)
        items.sort(key=lambda item: item.get("resolved_ts") or 0.0, reverse=True)
        if pruned:
            self._save_alert_state()
        if limit > 0:
            return items[:limit]
        return items

    def get_alert_summary(self) -> dict[str, int]:
        items = self.list_active_alerts(gui_only=True)
        new_count = sum(1 for item in items if not item.get("acknowledged"))
        ack_count = sum(1 for item in items if item.get("acknowledged"))
        return {
            "new_count": new_count,
            "ack_count": ack_count,
            "total_active": len(items),
        }

    def acknowledge_alert(self, alert_id: str) -> bool:
        alert = self._alerts.get(str(alert_id or ""))
        if not alert or not alert.active:
            return False
        if alert.acknowledged:
            return True
        alert.acknowledged = True
        alert.last_seen_ts = time.time()
        self._save_alert_state()
        self.store.changed.set()
        return True

    def acknowledge_all_alerts(self) -> int:
        updated = 0
        for alert in self._alerts.values():
            if alert.active and self._alert_gui_routes.get(alert.kind, True) and not alert.acknowledged:
                alert.acknowledged = True
                alert.last_seen_ts = time.time()
                updated += 1
        if updated:
            self._save_alert_state()
            self.store.changed.set()
        return updated

    async def _send_alert_event(self, event_name: str, message: str) -> None:
        self._load_alert_routes()
        if not self._alert_telegram_routes.get(event_name, True):
            return
        await self._send_alert(message)

    async def _sync_live_alerts(self) -> None:
        self._load_alert_routes()
        conn_summary = self.pool.get_status_summary().get("connections") or {}
        monitor_config = getattr(self, "_monitor_config", None)
        if monitor_config is None:
            monitor_config = MonitorConfig()
        self._update_cpu_threshold_state(monitor_config)
        live_alerts = collect_live_alerts(
            conn_summary,
            {h: m.to_dict() for h, m in self.store.system.items()},
            self.store.instances,
            self.store.services,
            monitor_config,
            self.store.streams,
        )
        now = time.time()
        self._prune_alert_history(now)
        live_map = {str(item.get("id") or ""): item for item in live_alerts if str(item.get("id") or "")}
        changed = False

        for alert_id, payload in live_map.items():
            existing = self._alerts.get(alert_id)
            was_active = bool(existing and existing.active)
            if existing:
                existing.kind = str(payload.get("kind") or existing.kind)
                existing.host = str(payload.get("host") or existing.host)
                existing.name = str(payload.get("name") or existing.name)
                existing.severity = str(payload.get("severity") or existing.severity)
                existing.summary = str(payload.get("summary") or existing.summary)
                existing.details = str(payload.get("details") or existing.details)
                existing.was_restarted = bool(payload.get("was_restarted", existing.was_restarted))
                existing.triggered_thresholds = [
                    str(item) for item in (payload.get("triggered_thresholds") or existing.triggered_thresholds or []) if str(item).strip()
                ]
                existing.last_seen_ts = now
                existing.active = True
                if not was_active:
                    existing.episode += 1
                    existing.first_seen_ts = now
                    existing.acknowledged = False
                    changed = True
                else:
                    changed = True
            else:
                existing = AlertRecord(
                    id=alert_id,
                    kind=str(payload.get("kind") or ""),
                    host=str(payload.get("host") or ""),
                    name=str(payload.get("name") or ""),
                    severity=str(payload.get("severity") or "warning"),
                    summary=str(payload.get("summary") or ""),
                    details=str(payload.get("details") or ""),
                    was_restarted=bool(payload.get("was_restarted", False)),
                    triggered_thresholds=[str(item) for item in (payload.get("triggered_thresholds") or []) if str(item).strip()],
                    active=True,
                    acknowledged=False,
                    first_seen_ts=now,
                    last_seen_ts=now,
                    episode=1,
                )
                self._alerts[alert_id] = existing
                was_active = False
                changed = True

            if not was_active:
                await self._emit_problem_event(existing)

        for alert_id, alert in list(self._alerts.items()):
            if alert_id in live_map:
                continue
            if alert.active:
                history_key = _alert_history_id(alert)
                self._alerts[history_key] = AlertRecord.from_dict({
                    **alert.to_dict(),
                    "id": history_key,
                    "active": False,
                })
                alert.active = False
                alert.last_seen_ts = now
                changed = True
                suppress_recovery = (
                    alert.kind == ALERT_KIND_SERVICE
                    and alert.name == "PBCluster"
                    and not self._optional_service_expected(alert.host, alert.name)
                )
                if not suppress_recovery:
                    await self._emit_recovery_event(alert)

        if changed:
            self._save_alert_state()
            self.store.changed.set()

    def _update_cpu_threshold_state(self, monitor_config: MonitorConfig, now: float | None = None) -> None:
        """Track how long each host's live CPU has stayed above the alert threshold."""
        now = float(now or time.time())
        for metrics in self.store.system.values():
            ts = float(getattr(metrics, "timestamp", 0.0) or now)
            cpu = float(getattr(metrics, "cpu", 0.0) or 0.0)
            if cpu >= monitor_config.cpu_error_server:
                since = float(getattr(metrics, "cpu_threshold_since", 0.0) or 0.0)
                if since <= 0.0:
                    since = ts
                metrics.cpu_threshold_since = since
                metrics.cpu_threshold_duration = max(0.0, ts - since)
            else:
                metrics.cpu_threshold_since = 0.0
                metrics.cpu_threshold_duration = 0.0

    def _current_system_status_text(self, host: str) -> str:
        """Return current host metrics for recovery notifications."""
        metrics = self.store.system.get(host)
        if not metrics:
            return ""
        mem_available_mb = float(metrics.mem_available or 0) / 1024 / 1024
        swap_free_mb = float(metrics.swap_free or 0) / 1024 / 1024
        disk_free_mb = float(metrics.disk_free or 0) / 1024 / 1024
        text = (
            f"Current: mem free {int(mem_available_mb)}MB, swap free {int(swap_free_mb)}MB, "
            f"disk free {int(disk_free_mb)}MB, CPU {float(metrics.cpu or 0):.1f}%"
        )
        if float(metrics.cpu_60s_window or 0) >= 60.0:
            text += f", CPU 60s avg {float(metrics.cpu_60s or 0):.1f}%"
        return text

    async def _emit_problem_event(self, alert: AlertRecord) -> None:
        if alert.kind == ALERT_KIND_OFFLINE:
            await self._send_alert_event("ssh_lost", f"⚠️ *VPSMonitor*: SSH connection lost to *{alert.host}*")
            return
        if alert.kind == ALERT_KIND_SERVICE:
            restart_hint = " Restart initiated." if alert.was_restarted else ""
            await self._send_alert_event("service_down", f"❌ *VPSMonitor*: {alert.name} is down on *{alert.host}*{restart_hint}")
            if alert.was_restarted:
                await self._send_alert_event("service_restart_started", f"🔄 *VPSMonitor*: {alert.name} restart initiated on *{alert.host}*")
            return
        if alert.kind == ALERT_KIND_SYSTEM:
            await self._send_alert_event("system_problem", f"⚠️ *VPSMonitor*: {alert.summary}\n{alert.details}")
            return
        if alert.kind == ALERT_KIND_INSTANCE:
            await self._send_alert_event("instance_problem", f"⚠️ *VPSMonitor*: {alert.summary}\n{alert.details}")

    async def _emit_recovery_event(self, alert: AlertRecord) -> None:
        if alert.kind == ALERT_KIND_OFFLINE:
            await self._send_alert_event("ssh_recovered", f"✅ *VPSMonitor*: SSH reconnected to *{alert.host}*")
            return
        if alert.kind == ALERT_KIND_SERVICE:
            await self._send_alert_event("service_recovered", f"✅ *VPSMonitor*: {alert.name} is running on *{alert.host}*")
            return
        if alert.kind == ALERT_KIND_SYSTEM:
            threshold_label = _system_threshold_labels(alert.triggered_thresholds)
            headline = (
                f"✅ *VPSMonitor*: System recovered on *{alert.host}*: {threshold_label}"
                if len(alert.triggered_thresholds or []) == 1
                else f"✅ *VPSMonitor*: System recovered on *{alert.host}*: {threshold_label}"
            )
            current_text = self._current_system_status_text(alert.host)
            if not current_text:
                current_text = str(alert.details or "").split("\n", 1)[1] if "\n" in str(alert.details or "") else str(alert.details or "")
            recovered_from = str(alert.details or "").split("\n", 1)[0].replace("Triggered:", "Recovered from:", 1)
            await self._send_alert_event("system_recovered", f"{headline}\n{recovered_from}\n{current_text}")
            return
        if alert.kind == ALERT_KIND_INSTANCE:
            await self._send_alert_event("instance_recovered", f"✅ *VPSMonitor*: Instance recovered for *{alert.name}* on *{alert.host}*")

    async def check_hl_expiry(self) -> None:
        from api_key_state import get_user_state
        from User import Users

        warning_days_raw = load_ini("hl_expiry", "telegram_warning_days")
        warning_days = 7
        if warning_days_raw:
            try:
                warning_days = int(warning_days_raw)
            except ValueError:
                warning_days = 7
        if warning_days < 1 or not self.telegram_token or not self.telegram_chat_id:
            return

        today_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
        warnings: list[str] = []
        try:
            users = Users()
        except Exception as e:
            _log(SERVICE, f"[alerts] HL expiry check failed to load users: {e}", level="WARNING")
            return

        for user in users:
            if getattr(user, "exchange", "") != "hyperliquid":
                continue
            vu = get_user_state(user.name).get("hl_valid_until")
            if vu is None:
                continue
            try:
                expiry_dt = datetime.fromtimestamp(int(vu) / 1000, tz=timezone.utc)
                days = (expiry_dt - datetime.now(tz=timezone.utc)).days
            except Exception:
                continue
            if days > warning_days:
                continue
            if self._hl_expiry_last_warned.get(user.name) == today_str:
                continue
            self._hl_expiry_last_warned[user.name] = today_str
            if days < 0:
                warnings.append(f"⚠️ *{user.name}*: HL key EXPIRED ({-days}d ago)")
            elif days == 0:
                warnings.append(f"⚠️ *{user.name}*: HL key expires TODAY")
            else:
                warnings.append(f"⚠️ *{user.name}*: HL key expires in {days}d ({expiry_dt.strftime('%Y-%m-%d')})")

        if warnings:
            await self._send_alert("🔑 *HL API Key Expiry Warning*\n" + "\n".join(warnings))
            self._save_alert_state()

    # ── Lifecycle ───────────────────────────────────────────

    async def start(self):
        """Initialize and start all monitoring tasks."""
        if self._running:
            return
        self.loop = asyncio.get_running_loop()
        self._running = True
        _log(SERVICE, "Starting VPS monitor...")

        self.pool.load_vps_configs()
        self._ini_watcher.bind_asyncio(self.loop, self._config_changed)
        self._ini_watcher.start()
        try:
            initial_candidate = self._build_config_candidate(load_ini_snapshot(self._ini_watcher._ini_path))
            self._commit_config_candidate(initial_candidate)
        except Exception as exc:
            _log(SERVICE, f"Config reload rejected during startup: {self._config_error_text(exc)}", level="WARNING")
            self._schedule_config_retry()
        self._load_alert_state()
        for store in self._host_metric_history.values():
            store.load()
        self._bot_cpu_history.load()
        for store in self._bot_metric_history.values():
            store.load()
        for store in self._bot_count_history.values():
            store.load()
        self._bot_pnl_history.load()

        enabled = self.enabled_hosts
        if not enabled:
            _log(SERVICE, "No VPS hosts enabled for monitoring. "
                 "Enable hosts in Services → API Server → Settings.")
        else:
            # Remove non-enabled hosts from pool
            for h in list(self.pool.hostnames()):
                if h not in enabled:
                    self.pool.remove_host(h)

            results = await self.pool.connect_enabled(enabled)
            connected = sum(1 for v in results.values() if v)
            _log(SERVICE, f"Connected to {connected}/{len(results)} VPS servers")

            # Start metric streams for connected hosts
            for hostname, success in results.items():
                if success:
                    self._start_metrics_stream(hostname)
                    asyncio.create_task(
                        self.collect_host_meta_now(hostname, include_package_status=True),
                        name=f"host-meta-startup-{hostname}",
                    )

        # Launch main loop as background task
        self._tasks.append(asyncio.create_task(
            self._main_loop(), name="vps-main-loop"
        ))
        self._tasks.append(asyncio.create_task(
            self._hl_expiry_loop(), name="vps-hl-expiry-loop"
        ))

        _log(SERVICE, "VPS monitor started")

    async def stop(self):
        """Cancel all tasks and disconnect."""
        if not self._running:
            return
        self._running = False
        _log(SERVICE, "Stopping VPS monitor...")

        # Cancel stream tasks
        for task in self._stream_tasks.values():
            task.cancel()
        # Cancel main tasks
        for task in self._tasks:
            task.cancel()

        # Wait for cancellation
        all_tasks = list(self._tasks) + list(self._stream_tasks.values())
        if all_tasks:
            await asyncio.gather(*all_tasks, return_exceptions=True)

        self._tasks.clear()
        self._stream_tasks.clear()
        self._ini_watcher.stop()
        self._ini_watcher.unbind_asyncio()
        self._config_changed.clear()
        self._config_retry_task = None
        for store in self._host_metric_history.values():
            store.maybe_flush(force=True)
        self._bot_cpu_history.maybe_flush(force=True)
        for store in self._bot_metric_history.values():
            store.maybe_flush(force=True)
        for store in self._bot_count_history.values():
            store.maybe_flush(force=True)
        self._bot_pnl_history.maybe_flush(force=True)
        await self.pool.disconnect_all()
        self.loop = None
        _log(SERVICE, "VPS monitor stopped")

    # ── Main loop ───────────────────────────────────────────

    async def _main_loop(self):
        """Main monitoring loop — health checks, reconnects, services."""
        loop_count = 0
        while self._running:
            try:
                await self._loop_iteration(loop_count)
                loop_count += 1
            except asyncio.CancelledError:
                break
            except Exception as e:
                _log(SERVICE, f"Error in main loop: {e}", level="WARNING",
                     meta={'traceback': traceback.format_exc()})

            # Sleep but wake on ini change without occupying the default executor.
            try:
                await asyncio.wait_for(self._config_changed.wait(), timeout=LOOP_INTERVAL)
            except asyncio.TimeoutError:
                pass
            except asyncio.CancelledError:
                break

    async def _hl_expiry_loop(self):
        while self._running:
            try:
                await self.check_hl_expiry()
            except asyncio.CancelledError:
                return
            except Exception as e:
                _log(SERVICE, f"[alerts] HL expiry loop failed: {e}", level="WARNING", meta={'traceback': traceback.format_exc()})
            await asyncio.sleep(60)

    async def _loop_iteration(self, loop_count: int):
        """Single iteration of the main loop."""
        # Config changes
        if self._config_changed.is_set():
            await self._apply_config_changes()

        self._record_local_master_metric_history()

        enabled = self.enabled_hosts
        if not enabled:
            return

        # 1. Health check
        status = self.pool.health_check()
        enabled_status = {h: s for h, s in status.items() if h in enabled}

        # 2. Reconnect lost
        reconnected = await self.pool.reconnect_lost(enabled)
        newly_reconnected: list[str] = []
        for hostname, success in reconnected.items():
            if success:
                _log(SERVICE, f"Reconnected to {hostname}")
                self._start_metrics_stream(hostname)
                self._last_host_meta_collect.pop(hostname, None)
                self._last_package_status_collect.pop(hostname, None)
                asyncio.create_task(
                    self.collect_host_meta_now(hostname, include_package_status=True),
                    name=f"host-meta-reconnect-{hostname}",
                )

        # 3. Restart dead or stale metric streams
        await self._restart_dead_streams()

        # 4. Collect host metadata before slower optional snapshots.
        await self._collect_host_meta_all()

        # 4b. Collect instances (every ~30s)
        await self._collect_instances_all()

        # 5. Service monitoring (every N iterations)
        if loop_count % SERVICE_CHECK_EVERY == 0:
            connected = [
                h for h, s in enabled_status.items()
                if s == ConnectionStatus.CONNECTED
            ]
            if connected:
                results = await self._check_and_heal_services(connected)
                self.store.update_services(results)

        await self._sync_live_alerts()

    # ── Config reload ───────────────────────────────────────

    async def _apply_config_changes(self):
        """Validate one INI generation, then apply it and its host delta."""
        self._config_changed.clear()
        self._ini_watcher.changed.clear()
        try:
            snapshot = load_ini_snapshot(self._ini_watcher._ini_path)
            candidate = self._build_config_candidate(snapshot)
        except Exception as exc:
            error_text = self._config_error_text(exc)
            _log(SERVICE, f"Config reload rejected; keeping last known good settings: {error_text}", level="WARNING")
            self._schedule_config_retry()
            return

        prev_enabled = set(self._enabled_hosts or set())
        self._commit_config_candidate(candidate)
        self._config_retry_count = 0
        if self._config_retry_task is not None and not self._config_retry_task.done():
            self._config_retry_task.cancel()
        self._config_retry_task = None
        enabled = set(candidate.enabled_hosts)

        newly_disabled = prev_enabled - enabled
        newly_enabled = enabled - prev_enabled

        if newly_disabled:
            _log(SERVICE, f"Hosts disabled: {', '.join(sorted(newly_disabled))}")
            for h in newly_disabled:
                self._stop_metrics_stream(h)
                await self.pool.disconnect(h)
                self.pool.remove_host(h)
                self.store.remove_host(h)

        if newly_enabled:
            _log(SERVICE, f"Hosts newly enabled: "
                 f"{', '.join(sorted(newly_enabled))}")
            self.pool.load_vps_configs()
            for h in list(self.pool.hostnames()):
                if h not in enabled:
                    self.pool.remove_host(h)
            for h in newly_enabled:
                if h in self.pool.hostnames():
                    if await self.pool.connect(h):
                        self._start_metrics_stream(h)
                        self._create_owned_task(
                            self.collect_host_meta_now(h, include_package_status=True),
                            name=f"host-meta-enable-{h}",
                        )

    def _create_owned_task(self, coroutine, *, name: str) -> asyncio.Task:
        task = asyncio.create_task(coroutine, name=name)
        self._tasks.append(task)
        return task

    def _schedule_config_retry(self) -> None:
        if not self._running or self._config_retry_count >= 3:
            return
        if self._config_retry_task is not None and not self._config_retry_task.done():
            return
        self._config_retry_count += 1

        async def wake_after_backoff() -> None:
            await asyncio.sleep(min(2 ** (self._config_retry_count - 1), 4))
            if self._running:
                self._config_changed.set()

        self._config_retry_task = self._create_owned_task(
            wake_after_backoff(), name="vps-config-retry"
        )

    async def refresh_enabled_host(self, hostname: str) -> bool:
        """Reload config and reconnect one enabled host after credentials change."""

        host = str(hostname or "").strip()
        if not host:
            return False
        enabled = set(self.enabled_hosts)
        self.pool.load_vps_configs()
        for h in list(self.pool.hostnames()):
            if h not in enabled:
                self.pool.remove_host(h)
        if host not in enabled or host not in self.pool.hostnames():
            return False
        if await self.pool.connect(host):
            self._start_metrics_stream(host)
            self._last_host_meta_collect.pop(host, None)
            self._last_package_status_collect.pop(host, None)
            asyncio.create_task(
                self.collect_host_meta_now(host, include_package_status=True),
                name=f"host-meta-refresh-{host}",
            )
            _log(SERVICE, f"Refreshed monitor connection for {host}")
            return True
        return False

    # ── Metric streams ──────────────────────────────────────

    def _start_metrics_stream(self, hostname: str):
        """Launch an async task that reads system metrics from SSH."""
        self._stop_metrics_stream(hostname)
        generation = self._stream_generations.get(hostname, 0) + 1
        self._stream_generations[hostname] = generation
        started_at = time.time()
        self._stream_started_at[hostname] = started_at
        task = asyncio.create_task(
            self._metrics_stream(hostname, generation),
            name=f"metrics-{hostname}",
        )
        self._stream_tasks[hostname] = task

    def _stop_metrics_stream(self, hostname: str):
        """Cancel the metrics stream task for a host."""
        task = self._stream_tasks.pop(hostname, None)
        self._stream_generations[hostname] = self._stream_generations.get(hostname, 0) + 1
        if task and not task.done():
            task.cancel()

    def _stream_last_update(self, hostname: str) -> float:
        stream = self.store.streams.get(hostname) or {}
        metrics = self.store.system.get(hostname)
        metric_dict = metrics.to_dict() if metrics else {}
        return _metrics_last_update(stream, metric_dict)

    def _stream_stale_age(self, hostname: str, now: float) -> float | None:
        last_update = self._stream_last_update(hostname)
        if last_update > 0:
            age = max(now - last_update, 0.0)
            if age > METRICS_STREAM_STALE_SECONDS:
                return age
            self._stream_stale_counts.pop(hostname, None)
            return None
        started_at = self._stream_started_at.get(hostname, now)
        startup_age = max(now - started_at, 0.0)
        if startup_age > METRICS_STREAM_STARTUP_GRACE_SECONDS:
            return startup_age
        return None

    def _local_master_hostname(self) -> str:
        configured = str(load_ini("main", "pbname") or "").strip()
        return configured or socket.gethostname().strip() or "master"

    def _read_local_cpu_times(self) -> tuple[float, float] | None:
        try:
            line = Path("/proc/stat").read_text(encoding="utf-8").splitlines()[0]
            parts = line.split()
            if len(parts) < 5 or parts[0] != "cpu":
                return None
            values = [float(value) for value in parts[1:]]
            return values[3], sum(values)
        except Exception as exc:
            _log(SERVICE, f"local master CPU probe failed: {exc}", level="WARNING")
            return None

    def _local_master_cpu_stats(self, now: float) -> tuple[float, float, float, int] | None:
        current = self._read_local_cpu_times()
        if current is None:
            return None
        idle, total = current
        if total <= 0:
            return None

        history = self._local_master_cpu_history
        cutoff = now - (CPU_HISTORY_STEP_SECONDS + 2.0)
        history[:] = [sample for sample in history if sample[0] >= cutoff]
        previous = history[-1] if history else None
        history.append((now, idle, total))

        live_cpu = 0.0
        if previous is not None:
            total_delta = total - previous[2]
            if total_delta > 0:
                live_cpu = (1.0 - ((idle - previous[1]) / total_delta)) * 100.0

        base_sample = None
        for sample in history:
            if now - sample[0] >= CPU_HISTORY_STEP_SECONDS:
                base_sample = sample
            else:
                break

        if base_sample is None:
            window = round(now - history[0][0], 1) if history else 0.0
            return round(max(0.0, min(live_cpu, 100.0)), 1), 0.0, window, len(history)

        elapsed = now - base_sample[0]
        total_delta = total - base_sample[2]
        if elapsed <= 0 or total_delta <= 0:
            return round(max(0.0, min(live_cpu, 100.0)), 1), 0.0, round(max(elapsed, 0.0), 1), len(history)
        cpu_60s = (1.0 - ((idle - base_sample[1]) / total_delta)) * 100.0
        return (
            round(max(0.0, min(live_cpu, 100.0)), 1),
            round(max(0.0, min(cpu_60s, 100.0)), 1),
            round(elapsed, 1),
            len(history),
        )

    def _read_local_memory_metrics(self) -> tuple[int, int, float, int, int, int, int, float]:
        data: dict[str, int] = {}
        try:
            for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
                key, _, value = line.partition(":")
                if key in {"MemTotal", "MemAvailable", "SwapTotal", "SwapFree"}:
                    data[key] = int(value.split()[0]) * 1024
        except Exception as exc:
            _log(SERVICE, f"local master memory probe failed: {exc}", level="WARNING")
        mem_total = int(data.get("MemTotal") or 0)
        mem_available = int(data.get("MemAvailable") or 0)
        mem_used = max(mem_total - mem_available, 0)
        mem_percent = round(mem_used / mem_total * 100, 1) if mem_total else 0.0
        swap_total = int(data.get("SwapTotal") or 0)
        swap_free = int(data.get("SwapFree") or 0)
        swap_used = max(swap_total - swap_free, 0)
        swap_percent = round(swap_used / swap_total * 100, 1) if swap_total else 0.0
        return mem_total, mem_available, mem_percent, mem_used, swap_total, swap_used, swap_free, swap_percent

    def _read_local_disk_metrics(self) -> tuple[int, int, int, float]:
        try:
            stat = os.statvfs("/")
        except Exception as exc:
            _log(SERVICE, f"local master disk probe failed: {exc}", level="WARNING")
            return 0, 0, 0, 0.0
        total = stat.f_frsize * stat.f_blocks
        used = stat.f_frsize * (stat.f_blocks - stat.f_bfree)
        free = stat.f_frsize * stat.f_bavail
        percent = round(used / total * 100, 1) if total else 0.0
        return total, used, free, percent

    def _local_master_metric_peak(self, metric: str, value: float, now: float, *, enabled: bool = True) -> tuple[float, float]:
        history = self._local_master_metric_history.get(metric)
        if history is None:
            return 0.0, 0.0
        if not enabled:
            history.clear()
            return 0.0, 0.0
        history.append((now, max(0.0, float(value))))
        cutoff = now - (CPU_HISTORY_STEP_SECONDS + 2.0)
        history[:] = [sample for sample in history if sample[0] >= cutoff]
        if not history:
            return 0.0, 0.0
        peak = round(max(sample[1] for sample in history), 1)
        window = round(now - history[0][0], 1)
        return peak, window

    def _build_local_master_system_metrics(self) -> SystemMetrics | None:
        now = time.time()
        cpu_stats = self._local_master_cpu_stats(now)
        if cpu_stats is None:
            return None
        cpu, cpu_60s, cpu_60s_window, cpu_samples = cpu_stats
        (
            mem_total,
            mem_available,
            mem_percent,
            mem_used,
            swap_total,
            swap_used,
            swap_free,
            swap_percent,
        ) = self._read_local_memory_metrics()
        disk_total, disk_used, disk_free, disk_percent = self._read_local_disk_metrics()
        mem_peak, mem_window = self._local_master_metric_peak("memory", mem_percent, now, enabled=mem_total > 0)
        disk_peak, disk_window = self._local_master_metric_peak("disk", disk_percent, now, enabled=disk_total > 0)
        swap_peak, swap_window = self._local_master_metric_peak("swap", swap_percent, now, enabled=swap_total > 0)
        return SystemMetrics(
            timestamp=now,
            cpu=cpu,
            cpu_60s=cpu_60s,
            cpu_60s_window=cpu_60s_window,
            cpu_60s_samples=cpu_samples,
            mem_total=mem_total,
            mem_available=mem_available,
            mem_percent=mem_percent,
            mem_60s_peak=mem_peak,
            mem_60s_window=mem_window,
            mem_used=mem_used,
            disk_total=disk_total,
            disk_used=disk_used,
            disk_free=disk_free,
            disk_percent=disk_percent,
            disk_60s_peak=disk_peak,
            disk_60s_window=disk_window,
            swap_total=swap_total,
            swap_used=swap_used,
            swap_free=swap_free,
            swap_percent=swap_percent,
            swap_60s_peak=swap_peak,
            swap_60s_window=swap_window,
        )

    def _record_local_master_metric_history(self) -> None:
        hostname = self._local_master_hostname()
        if not hostname:
            return
        metrics = self._build_local_master_system_metrics()
        if metrics is None:
            return
        self.store.update_system(hostname, metrics)
        self._record_host_metric_history(hostname, metrics)
        for store in self._host_metric_history.values():
            store.maybe_flush(now_ts=metrics.timestamp)

    async def _restart_stale_metrics_stream(self, hostname: str, stale_age: float, now: float):
        count = self._stream_stale_counts.get(hostname, 0) + 1
        self._stream_stale_counts[hostname] = count
        stale_text = f"No metrics received for {int(stale_age)}s"
        self.store.update_stream_info(hostname, {
            "alive": False,
            "active": False,
            "stale": True,
            "stale_age": round(stale_age, 1),
            "stale_since": now,
            "error": stale_text,
        })
        last_logged = self._stream_stale_last_logged.get(hostname, 0.0)
        should_log = now - last_logged >= METRICS_STREAM_STALE_LOG_INTERVAL
        if should_log:
            self._stream_stale_last_logged[hostname] = now
        if count >= METRICS_STREAM_RECONNECT_AFTER_STALE_RESTARTS:
            if should_log:
                _log(SERVICE, f"[metrics] Stream stale for {hostname}: "
                     f"{stale_text}; restarting stream and keeping SSH connected", level="WARNING")
            self._start_metrics_stream(hostname)
            return
        if should_log:
            _log(SERVICE, f"[metrics] Stream stale for {hostname}: "
                 f"{stale_text}; restarting stream", level="WARNING")
        self._start_metrics_stream(hostname)

    async def _restart_dead_streams(self):
        """Restart metric streams that have ended or stopped updating."""
        connected = set(self.pool.connected_hosts())
        enabled = self.enabled_hosts
        now = time.time()
        for hostname in sorted(connected & enabled):
            task = self._stream_tasks.get(hostname)
            if task is None:
                _log(SERVICE, f"Starting missing metrics stream for {hostname}")
                self._start_metrics_stream(hostname)
                continue
            if task.done():
                _log(SERVICE, f"Restarting dead metrics stream for {hostname}")
                self._start_metrics_stream(hostname)
                continue
            stale_age = self._stream_stale_age(hostname, now)
            if stale_age is not None:
                await self._restart_stale_metrics_stream(hostname, stale_age, now)
        for hostname in list(self._stream_tasks):
            if hostname not in connected and self._stream_tasks[hostname].done():
                self._stream_tasks.pop(hostname, None)

    async def _metrics_stream(self, hostname: str, generation: int):
        """Read system metrics from SSH stdout (JSON per line, 1/s)."""
        proc = None
        cancelled = False
        stream_error: str | None = None
        try:
            pbgui_dir = self.pool.get_remote_pbgui_dir(hostname)
            proc = await self.pool.start_process(hostname, _monitor_agent_tail_command(pbgui_dir))
            if not proc:
                stream_error = "Cannot start monitor-agent cache stream"
                _log(SERVICE, f"[metrics] Cannot start stream for {hostname}",
                     level="WARNING")
                if self._stream_generations.get(hostname) == generation:
                    self.store.update_stream_info(hostname, {
                        "alive": False,
                        "active": False,
                        "stale": True,
                        "error": stream_error,
                    })
                return

            if self._stream_generations.get(hostname) == generation:
                self.store.update_stream_info(hostname, {
                    "alive": True,
                    "active": True,
                    "starting": True,
                    "stale": False,
                    "error": None,
                    "last_update": 0,
                    "started_at": self._stream_started_at.get(hostname, time.time()),
                })

            async for line in proc.stdout:
                if self._stream_generations.get(hostname) != generation:
                    break
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    metrics = SystemMetrics.from_json(data)
                    self.store.update_system(hostname, metrics)
                    self._update_monitor_agent_file_status(hostname, "live_metrics.ndjson", {
                        "state": "ok",
                        "error": None,
                        "age": round(max(time.time() - float(data.get("generated_at") or metrics.timestamp or time.time()), 0.0), 1),
                        "generated_at": float(data.get("generated_at") or metrics.timestamp or 0.0),
                        "checked_at": time.time(),
                    })
                    self._stream_stale_counts.pop(hostname, None)
                    self._record_host_metric_history(hostname, metrics)
                    bots = data.get("bots")
                    if bots:
                        self.store.update_instances_live(hostname, bots)
                        self._record_bot_cpu_history(hostname, bots, metrics.timestamp)
                        self._record_bot_metric_history(hostname, bots, metrics.timestamp)
                    for store in self._host_metric_history.values():
                        store.maybe_flush(now_ts=metrics.timestamp)
                    self._bot_cpu_history.maybe_flush(now_ts=metrics.timestamp)
                    for store in self._bot_metric_history.values():
                        store.maybe_flush(now_ts=metrics.timestamp)
                    for store in self._bot_count_history.values():
                        store.maybe_flush(now_ts=metrics.timestamp)
                    self._bot_pnl_history.maybe_flush(now_ts=metrics.timestamp)
                    self.store.update_stream_info(hostname, {
                        "alive": True,
                        "active": True,
                        "starting": False,
                        "stale": False,
                        "stale_age": 0,
                        "stale_since": 0,
                        "error": None,
                        "last_update": metrics.timestamp,
                    })
                except json.JSONDecodeError:
                    continue

        except asyncio.CancelledError:
            cancelled = True
        except Exception as e:
            stream_error = str(e)
            _log(SERVICE, f"[metrics] Stream error for {hostname}: {e}",
                 level="WARNING")
            if self._stream_generations.get(hostname) == generation:
                self.store.update_stream_info(hostname, {
                    "alive": False, "active": False, "error": stream_error,
                })
        finally:
            if proc is not None:
                try:
                    proc.close()
                    wait_closed = getattr(proc, "wait_closed", None)
                    if callable(wait_closed):
                        await asyncio.wait_for(wait_closed(), timeout=5)
                except Exception:
                    pass
            if self._stream_generations.get(hostname) == generation:
                self.store.update_stream_info(hostname, {
                    "alive": False,
                    "active": False,
                    "starting": False,
                    "error": None if cancelled else stream_error,
                })
            for store in self._host_metric_history.values():
                store.maybe_flush(force=True)
            self._bot_cpu_history.maybe_flush(force=True)
            for store in self._bot_metric_history.values():
                store.maybe_flush(force=True)
            for store in self._bot_count_history.values():
                store.maybe_flush(force=True)
            self._bot_pnl_history.maybe_flush(force=True)
            # A dead metrics subprocess does not necessarily mean SSH died.
            # Keep the connection alive so the loop can restart the stream
            # without generating a spurious offline/recovered alert pair.
            _log(SERVICE, f"[metrics] Stream ended for {hostname}")

    def _update_monitor_agent_file_status(self, hostname: str, filename: str, status: dict[str, Any]) -> None:
        """Merge one monitor-agent cache-file status into stream diagnostics."""

        current_stream = self.store.streams.get(hostname) or {}
        current_agent = current_stream.get("monitor_agent") if isinstance(current_stream, dict) else None
        if not isinstance(current_agent, dict):
            current_agent = {}
        files = dict(current_agent.get("files") or {})
        files[filename] = dict(status)
        rank = {"ok": 0, "unknown": 1, "stale": 2, "missing": 3, "error": 4}
        state = "ok"
        for item in files.values():
            item_state = str((item or {}).get("state") or "unknown")
            if rank.get(item_state, 1) > rank.get(state, 0):
                state = item_state
        errors = [str((item or {}).get("error") or "") for item in files.values() if (item or {}).get("error")]
        self.store.update_stream_info(hostname, {
            "monitor_agent": {
                "state": state,
                "error": errors[0] if errors else None,
                "files": files,
                "checked_at": time.time(),
            },
        })

    async def _read_monitor_agent_json(self, hostname: str, filename: str, *, stale_after: float,
                                       timeout: float = 10.0) -> dict[str, Any] | None:
        """Read one monitor-agent JSON cache file from a VPS and validate freshness."""

        pbgui_dir = self.pool.get_remote_pbgui_dir(hostname)
        result = await self.pool.run(
            hostname,
            _monitor_agent_cache_read_command(pbgui_dir, filename),
            timeout=timeout,
            check=False,
        )
        now = time.time()
        if not result or result.exit_status != 0 or not result.stdout:
            error = f"monitor-agent cache missing: {filename}"
            self._update_monitor_agent_file_status(hostname, filename, {"state": "missing", "error": error, "checked_at": now})
            return None
        try:
            payload = json.loads(result.stdout.strip())
        except json.JSONDecodeError:
            error = f"monitor-agent cache invalid JSON: {filename}"
            self._update_monitor_agent_file_status(hostname, filename, {"state": "error", "error": error, "checked_at": now})
            return None
        if not isinstance(payload, dict):
            error = f"monitor-agent cache invalid payload: {filename}"
            self._update_monitor_agent_file_status(hostname, filename, {"state": "error", "error": error, "checked_at": now})
            return None
        try:
            generated_at = float(payload.get("generated_at") or 0.0)
        except (TypeError, ValueError):
            generated_at = 0.0
        age = max(now - generated_at, 0.0) if generated_at > 0 else stale_after + 1.0
        if age > stale_after:
            error = f"monitor-agent cache stale: {filename} age={int(age)}s"
            self._update_monitor_agent_file_status(hostname, filename, {
                "state": "stale",
                "error": error,
                "age": round(age, 1),
                "generated_at": generated_at,
                "checked_at": now,
            })
            return None
        self._update_monitor_agent_file_status(hostname, filename, {
            "state": "ok",
            "error": None,
            "age": round(age, 1),
            "generated_at": generated_at,
            "checked_at": now,
        })
        return payload

    async def _collect_host_meta_direct(self, hostname: str, *, timeout: float = 25.0) -> dict[str, Any] | None:
        """Collect host metadata directly when the remote agent cache is unavailable."""

        pbgui_dir = self.pool.get_remote_pbgui_dir(hostname)
        result = await self.pool.run(
            hostname,
            _host_meta_direct_command(pbgui_dir),
            timeout=timeout,
            check=False,
        )
        if not result or result.exit_status != 0 or not result.stdout:
            error = f"direct host-meta probe failed for {hostname}"
            if result and (result.stderr or result.stdout):
                error = f"{error}: {(result.stderr or result.stdout).strip()[:200]}"
            _log(SERVICE, f"[host-meta] {error}", level="WARNING")
            return None
        try:
            payload = json.loads(result.stdout.strip())
        except json.JSONDecodeError as exc:
            _log(SERVICE, f"[host-meta] direct probe invalid JSON on {hostname}: {exc}", level="WARNING")
            return None
        if not isinstance(payload, dict):
            _log(SERVICE, f"[host-meta] direct probe invalid payload on {hostname}", level="WARNING")
            return None
        payload.pop("coinmarketcap" + "_api_key", None)
        payload["schema_version"] = payload.get("schema_version") or 1
        payload["generated_at"] = time.time()
        payload["source"] = "direct-ssh"
        return payload

    def _record_host_metric_history(self, hostname: str, metrics: SystemMetrics) -> None:
        minute = int((metrics.timestamp or time.time()) // CPU_HISTORY_STEP_SECONDS)
        self._host_metric_history['cpu'].record(
            hostname,
            minute=minute,
            value=metrics.cpu_60s,
            confirmed=float(metrics.cpu_60s_window or 0.0) >= 60.0,
        )
        self._host_metric_history['memory'].record(
            hostname,
            minute=minute,
            value=metrics.mem_percent,
            confirmed=metrics.mem_total > 0,
            same_minute_mode='peak',
        )
        self._host_metric_history['disk'].record(
            hostname,
            minute=minute,
            value=metrics.disk_percent,
            confirmed=metrics.disk_total > 0,
            same_minute_mode='peak',
        )
        self._host_metric_history['swap'].record(
            hostname,
            minute=minute,
            value=metrics.swap_percent,
            confirmed=metrics.swap_total > 0,
            same_minute_mode='peak',
        )

    def _record_bot_cpu_history(self, hostname: str, bots: list[dict], timestamp: float) -> None:
        minute = int((timestamp or time.time()) // CPU_HISTORY_STEP_SECONDS)
        for bot in bots or []:
            name = str(bot.get('name') or '').strip()
            if not name:
                continue
            self._bot_cpu_history.record(
                self._bot_history_key(hostname, name),
                minute=minute,
                value=bot.get('cpu_60s'),
                confirmed=float(bot.get('cpu_60s_window') or 0.0) >= 60.0,
            )

    def _record_bot_metric_history(self, hostname: str, bots: list[dict], timestamp: float) -> None:
        minute = int((timestamp or time.time()) // CPU_HISTORY_STEP_SECONDS)
        for bot in bots or []:
            name = str(bot.get('name') or '').strip()
            if not name:
                continue
            key = self._bot_history_key(hostname, name)
            self._bot_metric_history['memory'].record(
                key,
                minute=minute,
                value=bot.get('rss_mb'),
                confirmed=float(bot.get('rss_mb') or 0.0) > 0.0,
                same_minute_mode='peak',
            )
            self._bot_metric_history['swap'].record(
                key,
                minute=minute,
                value=bot.get('swap_mb'),
                confirmed=float(bot.get('swap_mb') or 0.0) > 0.0,
                same_minute_mode='peak',
            )

    def _instance_collect_env(self, hostname: str) -> str:
        """Return remote collector environment values for custom install paths."""
        pbgui_dir = self.pool.get_remote_pbgui_dir(hostname)
        entry = self.pool.get_connection(hostname)
        pb7_dir = str((entry.data or {}).get('pb7dir') or '') if entry else ''
        parts = [f"PBGUI_PBGDIR={_shell_quote(pbgui_dir)}"]
        if pb7_dir:
            parts.append(f"PBGUI_PB7DIR={_shell_quote(pb7_dir)}")
        return " ".join(parts)

    async def _rebuild_bot_count_history(self, hostname: str, bot_logs: dict[str, Any] | None) -> None:
        """Disabled: masters must not start remote rebuild collectors."""

        del hostname, bot_logs
        return

    async def _rebuild_bot_pnl_history(self, hostname: str, bot_logs: dict[str, Any] | None) -> None:
        """Disabled: masters must not start remote PNL rebuild collectors."""

        del hostname, bot_logs
        return

    def _bot_history_key(self, hostname: str, bot_name: str) -> str:
        return f"{hostname}:{bot_name}"

    def _bot_pnl_history_key(self, bot_name: str) -> str:
        return str(bot_name or '').strip()

    def _bot_count_total(self, hostname: str, bot_name: str, metric: str) -> int:
        hostname = str(hostname or '').strip()
        bot_name = str(bot_name or '').strip()
        metric = str(metric or '').strip().lower()
        store = self._bot_count_history.get(metric)
        if not hostname or not bot_name or store is None:
            return 0
        payload = store.build_payload(
            self._bot_history_key(hostname, bot_name),
            hostname=hostname,
            bot_name=bot_name,
            metric=metric,
            source=BOT_HISTORY_SOURCES.get(metric, ''),
        )
        return int(payload.get('total_count') or 0)

    def _bot_pnl_total(self, bot_name: str) -> tuple[float, int]:
        return self._bot_pnl_history.get_total(self._bot_pnl_history_key(bot_name))

    def get_host_cpu_history(self, hostname: str) -> dict[str, Any]:
        return self.get_host_metric_history(hostname, 'cpu')

    def get_host_metric_history(self, hostname: str, metric: str) -> dict[str, Any]:
        hostname = str(hostname or '').strip()
        metric = str(metric or 'cpu').strip().lower()
        source = HOST_HISTORY_SOURCES.get(metric, HOST_HISTORY_SOURCES['cpu'])
        store = self._host_metric_history.get(metric)
        if not hostname or store is None:
            return {
                'available': False,
                'scope': 'host',
                'metric': metric,
                'hostname': '',
                'bot_name': '',
                'source': source,
                'step_seconds': CPU_HISTORY_STEP_SECONDS,
                'window_minutes': CPU_HISTORY_WINDOW_MINUTES,
                'resolution_pct': CPU_HISTORY_RESOLUTION_PCT,
                'points': [],
            }
        return store.build_payload(
            hostname,
            hostname=hostname,
            metric=metric,
            source=source,
        )

    def get_bot_cpu_history(self, hostname: str, bot_name: str) -> dict[str, Any]:
        return self.get_bot_metric_history(hostname, bot_name, 'cpu')

    def get_bot_metric_history(self, hostname: str, bot_name: str, metric: str) -> dict[str, Any]:
        hostname = str(hostname or '').strip()
        bot_name = str(bot_name or '').strip()
        metric = str(metric or 'cpu').strip().lower()
        source = BOT_HISTORY_SOURCES.get(metric, BOT_HISTORY_SOURCES['cpu'])
        if not hostname or not bot_name:
            return {
                'available': False,
                'scope': 'bot',
                'metric': metric,
                'hostname': hostname,
                'bot_name': bot_name,
                'source': source,
                'step_seconds': CPU_HISTORY_STEP_SECONDS,
                'window_minutes': CPU_HISTORY_WINDOW_MINUTES,
                'points': [],
            }
        if metric == 'cpu':
            return self._bot_cpu_history.build_payload(
                self._bot_history_key(hostname, bot_name),
                hostname=hostname,
                bot_name=bot_name,
                metric='cpu',
                source='cpu_60s',
            )
        if metric in {'errors', 'tracebacks'}:
            store = self._bot_count_history.get(metric)
            if store is None:
                return {
                    'available': False,
                    'scope': 'bot',
                    'metric': metric,
                    'hostname': hostname,
                    'bot_name': bot_name,
                    'source': source,
                    'step_seconds': COUNT_HISTORY_STEP_SECONDS,
                    'window_hours': COUNT_HISTORY_WINDOW_HOURS,
                    'points': [],
                    'daily_points': [],
                    'timezone_basis': 'UTC',
                }
            return store.build_payload(
                self._bot_history_key(hostname, bot_name),
                hostname=hostname,
                bot_name=bot_name,
                metric=metric,
                source=source,
            )
        if metric == 'pnl':
            return self._bot_pnl_history.build_payload(
                self._bot_pnl_history_key(bot_name),
                hostname=hostname,
                metric='pnl',
                source=source,
            )
        store = self._bot_metric_history.get(metric)
        if store is None:
            return {
                'available': False,
                'scope': 'bot',
                'metric': metric,
                'hostname': hostname,
                'bot_name': bot_name,
                'source': source,
                'step_seconds': CPU_HISTORY_STEP_SECONDS,
                'window_minutes': CPU_HISTORY_WINDOW_MINUTES,
                'points': [],
            }
        return store.build_payload(
            self._bot_history_key(hostname, bot_name),
            hostname=hostname,
            bot_name=bot_name,
            metric=metric,
            source=source,
        )

    # ── Instance collection ─────────────────────────────────

    async def collect_instances_now(self, hostname: str):
        """Public: immediately collect instances from a single VPS.

        Unlike _collect_instances_all() this bypasses the interval gate
        so callers can trigger a refresh right after an activation signal.
        """
        entry = self.pool.get_connection(hostname)
        if not entry:
            _log(SERVICE, f"[instances] collect_instances_now: "
                 f"{hostname} not connected", level="WARNING")
            return
        try:
            await self._collect_instances(hostname)
            _log(SERVICE, f"[instances] Immediate collect for {hostname}",
                 level="DEBUG")
        except Exception as e:
            _log(SERVICE, f"[instances] Immediate collect error on "
                 f"{hostname}: {e}", level="WARNING")

    async def _collect_instances_all(self):
        """Collect bot instance data from all connected VPS."""
        now = time.time()
        if now - self._last_instance_collect < INSTANCE_COLLECT_INTERVAL:
            return
        self._last_instance_collect = now

        connected = self.pool.connected_hosts()
        targets = [
            h for h in connected
            if h in self._stream_tasks and not self._stream_tasks[h].done()
        ]
        if not targets:
            return

        results = await asyncio.gather(
            *(self._collect_instances(h) for h in targets),
            return_exceptions=True,
        )
        for hostname, result in zip(targets, results):
            if isinstance(result, Exception):
                _log(SERVICE, f"[instances] Error on {hostname}: {result}",
                     level="WARNING")

    async def _collect_instances(self, hostname: str):
        """Collect bot instances from the monitor-agent cache on a single VPS."""
        parsed = await self._read_monitor_agent_json(
            hostname,
            "instance_snapshot.json",
            stale_after=90.0,
            timeout=10,
        )
        if not parsed:
            return
        monitors = parsed.get('monitors', [])
        v7_list = parsed.get('v7', [])
        bot_logs = parsed.get('bot_logs', {})
        if isinstance(monitors, list) and isinstance(v7_list, list):
            enriched_monitors = []
            for monitor in monitors:
                item = dict(monitor) if isinstance(monitor, dict) else monitor
                if isinstance(item, dict):
                    bot_name = str(item.get('u') or '')
                    item['errors_4w'] = self._bot_count_total(hostname, bot_name, 'errors')
                    item['tracebacks_4w'] = self._bot_count_total(hostname, bot_name, 'tracebacks')
                    total_pnl, total_fills = self._bot_pnl_total(bot_name)
                    item['pnl_hist_total'] = total_pnl
                    item['pnls_hist_total'] = total_fills
                enriched_monitors.append(item)
            self.store.update_instances(hostname, enriched_monitors)
            self.store.update_v7_instances(hostname, v7_list)
            self.store.update_bot_logs(hostname, bot_logs if isinstance(bot_logs, dict) else {})
            self._cache_host_snapshot(hostname)
            if self.debug_logging:
                _log(SERVICE, f"[instances] Read {len(monitors)} monitors and {len(v7_list)} v7 instances from agent cache on {hostname}", level="DEBUG")

    def _load_monitor_cache(self) -> None:
        try:
            cache_path = self._cache_path
            if not cache_path.exists() and self._legacy_cache_path.exists():
                cache_path = self._legacy_cache_path
            if cache_path.exists():
                loaded = json.loads(cache_path.read_text())
                if not isinstance(loaded, dict):
                    self._monitor_cache = {}
                    return
                cleaned: dict[str, dict[str, dict]] = {}
                for hostname, host_cache in loaded.items():
                    if not isinstance(host_cache, dict):
                        continue
                    cache_version = int(host_cache.get('_version', 0) or 0)
                    if cache_version != MONITOR_CACHE_VERSION:
                        continue
                    cleaned[hostname] = host_cache
                self._monitor_cache = cleaned
                self._hydrate_monitor_cache()
        except Exception:
            self._monitor_cache = {}

    def _hydrate_monitor_cache(self) -> None:
        """Restore the last known monitor snapshot into the live store."""
        changed = False
        metric_fields = set(SystemMetrics.__dataclass_fields__)
        for hostname, host_cache in self._monitor_cache.items():
            if not isinstance(host_cache, dict):
                continue
            system = host_cache.get('system')
            if isinstance(system, dict):
                values = {key: system.get(key) for key in metric_fields if key in system}
                try:
                    self.store.system[hostname] = SystemMetrics(**values)
                    changed = True
                except Exception:
                    pass
            instances = host_cache.get('instances')
            if isinstance(instances, list):
                self.store.instances[hostname] = instances
                changed = True
            v7_instances = host_cache.get('v7_instances')
            if isinstance(v7_instances, list):
                self.store.v7_instances[hostname] = v7_instances
                changed = True
            host_meta = host_cache.get('host_meta')
            if isinstance(host_meta, dict):
                self.store.host_meta[hostname] = host_meta
                changed = True
            streams = host_cache.get('streams')
            if isinstance(streams, dict):
                cached_stream = dict(streams)
                cached_stream['cached'] = True
                self.store.streams[hostname] = cached_stream
                changed = True
            bot_logs = host_cache.get('bot_logs')
            if isinstance(bot_logs, dict):
                self.store.bot_logs[hostname] = bot_logs
                changed = True
        if changed:
            self.store.changed.set()

    def _cache_host_snapshot(self, hostname: str) -> None:
        """Persist the last known data needed for a fast Overview after restart."""
        host = str(hostname or '').strip()
        if not host:
            return
        payload = dict(self._monitor_cache.get(host) or {})
        payload['_version'] = MONITOR_CACHE_VERSION
        metrics = self.store.system.get(host)
        if metrics:
            payload['system'] = metrics.to_dict()
        if host in self.store.instances:
            payload['instances'] = self.store.instances.get(host) or []
        if host in self.store.v7_instances:
            payload['v7_instances'] = self.store.v7_instances.get(host) or []
        if host in self.store.host_meta:
            payload['host_meta'] = self.store.host_meta.get(host) or {}
        if host in self.store.streams:
            payload['streams'] = self.store.streams.get(host) or {}
        if host in self.store.bot_logs:
            payload['bot_logs'] = self.store.bot_logs.get(host) or {}
        self._monitor_cache[host] = payload
        self._save_monitor_cache()

    def _save_monitor_cache(self) -> None:
        try:
            tmp = self._cache_path.with_suffix('.json.tmp')
            tmp.parent.mkdir(parents=True, exist_ok=True)
            tmp.write_text(json.dumps(self._monitor_cache))
            tmp.replace(self._cache_path)
        except Exception:
            pass

    async def collect_host_meta_now(self, hostname: str,
                                    *, include_package_status: bool = False):
        """Public: immediately collect host metadata from a single VPS."""
        entry = self.pool.get_connection(hostname)
        if not entry:
            _log(SERVICE, f"[host-meta] collect_host_meta_now: "
                 f"{hostname} not connected", level="WARNING")
            return
        try:
            await self._collect_host_meta(hostname,
                                          include_package_status=include_package_status,
                                          force=True)
            _log(SERVICE, f"[host-meta] Immediate collect for {hostname}",
                 level="DEBUG")
        except Exception as e:
            _log(SERVICE, f"[host-meta] Immediate collect error on "
                 f"{hostname}: {e}", level="WARNING")

    async def _collect_host_meta_all(self):
        """Collect host metadata from all connected VPS via the shared SSH pool."""
        now = time.time()
        connected = self.pool.connected_hosts()
        targets = [
            h for h in connected
            if h in self._stream_tasks and not self._stream_tasks[h].done()
        ]
        if not targets:
            return

        scheduled: list[tuple[str, bool]] = []
        for hostname in targets:
            needs_host_meta = now - self._last_host_meta_collect.get(hostname, 0.0) >= HOST_META_INTERVAL
            needs_package_status = now - self._last_package_status_collect.get(hostname, 0.0) >= PACKAGE_STATUS_INTERVAL
            if needs_host_meta or needs_package_status:
                scheduled.append((hostname, needs_package_status))

        if not scheduled:
            return

        results = await asyncio.gather(
            *(
                self._collect_host_meta(hostname, include_package_status=include_package_status)
                for hostname, include_package_status in scheduled
            ),
            return_exceptions=True,
        )
        for (hostname, include_package_status), result in zip(scheduled, results):
            if isinstance(result, Exception):
                label = "Package status" if include_package_status else "host-meta"
                _log(SERVICE, f"[{label}] Error on {hostname}: {result}",
                     level="WARNING")

    async def _collect_host_meta(self, hostname: str,
                                 *, include_package_status: bool = False,
                                 force: bool = False):
        """Collect SSH-derived host metadata for a single VPS."""
        collecting = getattr(self, '_host_meta_collecting', None)
        if collecting is None:
            collecting = set()
            self._host_meta_collecting = collecting
        if hostname in collecting:
            return
        collecting.add(hostname)
        try:
            now = time.time()
            collect_host_meta = force or (
                now - self._last_host_meta_collect.get(hostname, 0.0) >= HOST_META_INTERVAL
            )
            collect_package_status = include_package_status and (
                force or now - self._last_package_status_collect.get(hostname, 0.0) >= PACKAGE_STATUS_INTERVAL
            )

            if not collect_host_meta and not collect_package_status:
                return

            if collect_host_meta:
                parsed = await self._read_monitor_agent_json(
                    hostname,
                    "host_meta.json",
                    stale_after=30.0,
                    timeout=10,
                )
                if not parsed:
                    parsed = await self._collect_host_meta_direct(hostname)
                if parsed:
                    self.store.update_host_meta(hostname, parsed)
                    self._last_host_meta_collect[hostname] = now
                    self._cache_host_snapshot(hostname)
                    if self.debug_logging:
                        _log(SERVICE, f"[host-meta] Read agent cache for {hostname}", level="DEBUG")

            if collect_package_status:
                package_data = await self._read_monitor_agent_json(
                    hostname,
                    "package_status.json",
                    stale_after=7200.0,
                    timeout=10,
                )
                if package_data:
                    current_meta = dict(self.store.host_meta.get(hostname, {}))
                    # Keep the last known package count when a slow probe falls back to N/A.
                    if package_data.get('upgrades') == 'N/A' and current_meta.get('upgrades') not in (None, '', 'N/A'):
                        package_data['upgrades'] = current_meta.get('upgrades')
                    self.store.update_host_meta(hostname, package_data)
                    self._last_package_status_collect[hostname] = now
                    self._cache_host_snapshot(hostname)
        finally:
            collecting.discard(hostname)

    # ── Service monitoring ──────────────────────────────────

    async def _check_systemd_service(self, hostname: str,
                                     service_name: str) -> dict | None:
        """Return remote systemd user-unit status when the unit is installed."""
        unit = MONITORED_SERVICE_SYSTEMD_UNITS.get(service_name)
        if not unit:
            return None
        unit_json = json.dumps(unit)
        result = await self.pool.run(
            hostname,
            "uid=\"$(id -u)\"; "
            "export XDG_RUNTIME_DIR=\"${XDG_RUNTIME_DIR:-/run/user/$uid}\"; "
            "command -v systemctl >/dev/null 2>&1 || exit 2; "
            f"unit={unit_json}; "
            "systemctl --user show \"$unit\" "
            "-p LoadState -p ActiveState -p SubState -p Result "
            "-p MainPID -p ExecMainPID -p ExecMainStatus -p FragmentPath "
            "--no-pager",
            timeout=10,
        )
        if result is None or result.exit_status != 0:
            return None

        props: dict[str, str] = {}
        for line in (result.stdout or "").splitlines():
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            props[key] = value

        load_state = props.get("LoadState", "")
        if not load_state or load_state == "not-found":
            return None

        def _int_prop(name: str) -> int:
            raw = props.get(name, "")
            return int(raw) if raw.isdigit() else 0

        active = props.get("ActiveState", "")
        sub = props.get("SubState", "")
        result_state = props.get("Result", "")
        main_pid = _int_prop("MainPID") or _int_prop("ExecMainPID")
        exec_status = props.get("ExecMainStatus", "")
        running = active == "active" and sub == "running" and main_pid > 0
        error = None
        if not running:
            error = (
                f"systemd {unit}: active={active or 'unknown'} "
                f"sub={sub or 'unknown'} result={result_state or 'unknown'} "
                f"status={exec_status or 'unknown'}"
            )
        return {
            "status": (ServiceStatus.RUNNING.value if running
                       else ServiceStatus.STOPPED.value),
            "pid": main_pid if running else None,
            "error": error,
            "was_restarted": False,
            "manager": "systemd",
            "unit": unit,
        }

    async def _check_service(self, hostname: str, svc: ServiceInfo
                             ) -> dict:
        """Check if a service is running on a VPS."""
        if not self._optional_service_expected(hostname, svc.name):
            return self._disabled_service_check(svc.name)

        systemd_status = await self._check_systemd_service(hostname, svc.name)
        if systemd_status is not None:
            return systemd_status
        if svc.name in {"PBCluster", "PBMonitorAgent"}:
            return {
                "status": ServiceStatus.STOPPED.value,
                "pid": None,
                "error": f"{svc.name} systemd user unit is missing or unavailable",
                "was_restarted": False,
                "manager": "systemd",
                "unit": MONITORED_SERVICE_SYSTEMD_UNITS.get(svc.name),
            }

        result = None
        for base_dir in self.pool.get_remote_pbgui_dirs(hostname):
            pid_path = f"{base_dir}/{svc.pid_file}"
            result = await self.pool.run(hostname, f'cat {pid_path}', timeout=10)
            pid_str = (result.stdout or "").strip() if result else ""
            if pid_str.isdigit():
                break
        if result is None:
            return {
                "status": ServiceStatus.UNKNOWN.value,
                "pid": None,
                "error": "SSH connection error",
                "was_restarted": False,
            }
        pid_str = (result.stdout or "").strip()
        if not pid_str.isdigit():
            return {
                "status": ServiceStatus.STOPPED.value,
                "pid": None,
                "error": "No PID file or invalid PID",
                "was_restarted": False,
            }
        pid = int(pid_str)

        # Step 2: Check if process is running
        check = await self.pool.run(
            hostname,
            f'ps -p {pid} -o cmd= 2>/dev/null | grep -qi '
            f'"{svc.process_match}" && echo "yes" || echo "no"',
            timeout=10,
        )
        if check is None:
            return {
                "status": ServiceStatus.UNKNOWN.value,
                "pid": pid,
                "error": "SSH error during process check",
                "was_restarted": False,
            }
        running = (check.stdout or "").strip() == "yes"
        return {
            "status": (ServiceStatus.RUNNING.value if running
                       else ServiceStatus.STOPPED.value),
            "pid": pid if running else None,
            "error": (None if running
                      else f"PID {pid} not running"),
            "was_restarted": False,
        }

    async def _restart_service(self, hostname: str,
                               service_name: str) -> bool:
        """Restart a service on a VPS (same logic as old ServiceMonitor)."""
        svc = MONITORED_SERVICES.get(service_name)
        if not svc:
            return False

        if not self._optional_service_expected(hostname, service_name):
            reason = (OPTIONAL_SERVICE_REQUIREMENTS.get(service_name) or {}).get("reason") or "service is not configured"
            _log(SERVICE, f"[service] Skip restart for {service_name} on {hostname}: {reason}")
            return False

        if not self._can_restart(hostname, service_name):
            _log(SERVICE, f"[service] Restart limit reached for "
                 f"{service_name} on {hostname}", level="WARNING")
            return False

        _log(SERVICE, f"[service] Restarting {service_name} on {hostname}")

        unit = MONITORED_SERVICE_SYSTEMD_UNITS.get(service_name)
        if unit:
            unit_json = json.dumps(unit)
            systemd_cmd = (
                "uid=\"$(id -u)\"; "
                "export XDG_RUNTIME_DIR=\"${XDG_RUNTIME_DIR:-/run/user/$uid}\"; "
                "command -v systemctl >/dev/null 2>&1 || exit 2; "
                f"unit={unit_json}; "
                "load_state=$(systemctl --user show \"$unit\" "
                "-p LoadState --value --no-pager 2>/dev/null || true); "
                "if [ -n \"$load_state\" ] && [ \"$load_state\" != not-found ]; then "
                "systemctl --user restart \"$unit\"; "
                "else exit 3; fi"
            )
            result = await self.pool.run(hostname, systemd_cmd, timeout=15)
            if result and result.exit_status == 0:
                self._record_restart(hostname, service_name)
                _log(SERVICE, f"[service] {service_name} systemd restart sent to "
                     f"{hostname}")
                return True
            if result and result.exit_status not in (2, 3):
                _log(SERVICE, f"[service] Failed to restart {service_name} on "
                     f"{hostname} through systemd", level="ERROR")
                return False
            if service_name in {"PBCluster", "PBMonitorAgent"}:
                _log(
                    SERVICE,
                    f"[service] {service_name} restart on {hostname} requires the systemd user unit; not using legacy starter.py fallback",
                    level="WARNING",
                )
                return False

        start_cmd = ""
        for base_dir in self.pool.get_remote_pbgui_dirs(hostname):
            parent_dir = base_dir.rsplit('/', 1)[0] if '/' in base_dir else ''
            venv_pbgui = (
                remote_path_join(parent_dir, 'venv_pbgui', 'bin', 'activate')
                if parent_dir else 'venv_pbgui/bin/activate'
            )
            base_shell = remote_shell_path(base_dir)
            venv_shell = remote_shell_path(venv_pbgui)
            dotvenv_shell = remote_shell_path(remote_path_join(base_dir, '.venv', 'bin', 'activate'))
            venv_check = await self.pool.run(
                hostname,
                f'test -d {base_shell} || exit 1; '
                f'test -f {venv_shell} && echo "venv_pbgui" '
                f'|| (test -f {dotvenv_shell} '
                f'&& echo "dotvenv" || echo "system")',
                timeout=5,
            )
            if not venv_check or venv_check.exit_status != 0:
                continue
            venv_type = (venv_check.stdout or "").strip() if venv_check else "system"
            if venv_type == "venv_pbgui":
                start_cmd = (
                    f"cd {base_shell} && "
                    f"source {venv_shell} && "
                    f"nohup python -u starter.py -r {service_name} "
                    f"> /dev/null 2>&1 &"
                )
            elif venv_type == "dotvenv":
                start_cmd = (
                    f"cd {base_shell} && "
                    f"source {dotvenv_shell} && "
                    f"nohup python -u starter.py -r {service_name} "
                    f"> /dev/null 2>&1 &"
                )
            else:
                start_cmd = (
                    f"cd {base_shell} && "
                    f"nohup python3 -u starter.py -r {service_name} "
                    f"> /dev/null 2>&1 &"
                )
            break
        if not start_cmd:
            return False

        result = await self.pool.run(hostname, start_cmd, timeout=15)
        if result and result.exit_status == 0:
            self._record_restart(hostname, service_name)
            _log(SERVICE, f"[service] {service_name} restart sent to "
                 f"{hostname}")
            return True
        _log(SERVICE, f"[service] Failed to restart {service_name} on "
             f"{hostname}", level="ERROR")
        return False

    async def _check_and_heal_services(self, hostnames: list[str]) -> dict:
        """Check + auto-heal all services on given hosts."""
        all_results: dict[str, dict] = {}
        for hostname in hostnames:
            host_svc: dict[str, dict] = {}
            payload = await self._read_monitor_agent_json(
                hostname,
                "service_status.json",
                stale_after=120.0,
                timeout=10,
            )
            services = payload.get("services") if isinstance(payload, dict) else None
            if not isinstance(services, dict):
                services = {}
            for svc_name in MONITORED_SERVICES.keys():
                if not self._optional_service_expected(hostname, svc_name):
                    check = self._disabled_service_check(svc_name)
                else:
                    raw_check = services.get(svc_name)
                    check = dict(raw_check) if isinstance(raw_check, dict) else {
                        "status": ServiceStatus.UNKNOWN.value,
                        "pid": None,
                        "error": "monitor-agent service status missing",
                        "was_restarted": False,
                    }

                status_val = check["status"]

                if (
                    status_val == ServiceStatus.STOPPED.value
                    and self.auto_restart
                    and self._auto_restart_allowed_for_host(hostname, svc_name)
                ):
                    _log(SERVICE, f"[service] {svc_name} down on {hostname}, "
                         "attempting restart")
                    restarted = await self._restart_service(hostname, svc_name)
                    check["was_restarted"] = restarted
                    if restarted:
                        check["status"] = ServiceStatus.RESTARTING.value

                host_svc[svc_name] = check
            all_results[hostname] = host_svc
        return all_results

    # ── Restart rate limiting ───────────────────────────────

    def _can_restart(self, hostname: str, service_name: str) -> bool:
        history = self._restart_history.get(hostname, {}).get(
            service_name, []
        )
        now = datetime.now()
        history = [ts for ts in history if (now - ts).total_seconds() < 3600]
        self._restart_history.setdefault(hostname, {})[service_name] = history
        return len(history) < self.max_restarts_per_hour

    def _record_restart(self, hostname: str, service_name: str):
        self._restart_history.setdefault(hostname, {}).setdefault(
            service_name, []
        ).append(datetime.now())


    async def _send_alert(self, message: str):
        """Send Telegram alert."""
        sender_host = socket.gethostname().strip() or "unknown-host"
        formatted_message = f"[{sender_host}]\n{message}"
        if not self.telegram_token or not self.telegram_chat_id:
            _log(SERVICE, f"[alert] No Telegram config: {formatted_message}",
                 level="WARNING")
            return
        try:
            from telegram import Bot
            bot = Bot(token=self.telegram_token)
            async with bot:
                await bot.send_message(
                    chat_id=self.telegram_chat_id,
                    text=formatted_message,
                    parse_mode='Markdown',
                )
            _log(SERVICE, f"[alert] Sent: {formatted_message}")
        except Exception as e:
            _log(SERVICE, f"[alert] Failed: {e}", level="ERROR")

    # ── Kill instance (called by WebSocket command) ─────────

    async def kill_instance(self, hostname: str, name: str,
                            pb_version: str = "") -> dict:
        """Kill a bot instance on a VPS."""
        name = str(name or "").strip()
        if (not name or len(name) > 255 or name in {".", ".."}
                or "/" in name or "\\" in name or "\x00" in name
                or any(ord(char) < 32 or ord(char) == 127 for char in name)):
            return {"success": False, "pid": ""}

        result = await self.pool.run(hostname, "ps -eo pid=,args=", timeout=15)
        killed_pid = ""
        config_marker = f"/data/run_v7/{name}/config_run.json"
        if result and result.exit_status == 0:
            for line in (result.stdout or "").splitlines():
                match = re.match(r"^\s*(\d+)\s+(.+)$", line)
                if not match:
                    continue
                pid, command = match.groups()
                if int(pid) > 1 and "main.py" in command and config_marker in command:
                    killed_pid = pid
                    break

        success = False
        if killed_pid:
            kill_result = await self.pool.run(
                hostname, f"kill -- {int(killed_pid)}", timeout=15
            )
            success = bool(kill_result and kill_result.exit_status == 0)
            if not success:
                killed_pid = ""

        _log(SERVICE,
             f"[cmd] Kill instance {name} on {hostname}: "
             f"{'OK pid=' + killed_pid if success else 'not found'}",
             level="INFO" if success else "WARNING")

        return {
            "success": success,
            "pid": killed_pid,
        }
