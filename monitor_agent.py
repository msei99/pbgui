#!/usr/bin/env python3
"""Local PBGui VPS monitor agent.

The agent performs local, high-frequency measurements once per VPS, writes
cache files that PBGui masters can read over SSH, and heals local PBGui
services that this host is responsible for. It does not control bots, write
cluster state, or perform remote actions.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import threading
import time
from collections import deque
from pathlib import Path
from typing import Any

from cmc_pool import CmcPoolClient
from credential_store import CredentialStore
from logging_helpers import human_log as _log
from master.cluster_state import (
    default_cluster_root,
    local_cmc_credential_readiness,
    read_local_identity,
)
import pbgui_purefunc


SERVICE = "PBMonitorAgent"
SCHEMA_VERSION = 1
LIVE_INTERVAL_SECONDS = 1.0
STATUS_INTERVAL_SECONDS = 5.0
INSTANCE_INTERVAL_SECONDS = 30.0
HOST_META_INTERVAL_SECONDS = 10.0
SERVICE_INTERVAL_SECONDS = 60.0
PACKAGE_INTERVAL_SECONDS = 3600.0
NDJSON_RETENTION_SECONDS = 300.0
HISTORY_SECONDS = 62.0
CPU_TICKS_PER_SECOND = os.sysconf(os.sysconf_names.get("SC_CLK_TCK", "SC_CLK_TCK")) or 100
MONITOR_CACHE_VERSION = 2
SERVICE_RESTART_COOLDOWN_SECONDS = 60.0
SERVICE_RESTART_MAX_PER_HOUR = 3


def _pbgui_dir() -> Path:
    """Return the PBGui checkout directory for this agent process."""

    return Path(os.environ.get("PBGUI_DIR") or Path(__file__).resolve().parent).resolve()


PBGDIR = _pbgui_dir()
DATA_DIR = PBGDIR / "data" / "monitor_agent"
_STATUS_LOCK = threading.Lock()
_SERVICE_RESTART_HISTORY: dict[str, list[float]] = {}

PBGUI_SERVICES = {
    "PBCluster": ("pbgui-pbcluster.service", "data/pid/pbcluster.pid", "pbcluster.py"),
    "PBRun": ("pbgui-pbrun.service", "data/pid/pbrun.pid", "pbrun.py"),
    "PBData": ("pbgui-pbdata.service", "data/pid/pbdata.pid", "pbdata.py"),
    "PBCoinData": ("pbgui-pbcoindata.service", "data/pid/pbcoindata.pid", "pbcoindata.py"),
    "PBMonitorAgent": ("pbgui-monitor-agent.service", "data/pid/pbmonitoragent.pid", "monitor_agent.py"),
}


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    """Write a JSON object atomically."""

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    os.replace(tmp, path)


def _read_json(path: Path, default: Any = None) -> Any:
    """Read a JSON file with a fallback value."""

    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _read_materialized_snapshot(cluster_root: Path) -> dict[str, Any]:
    """Read the persisted cluster snapshot without replaying the oplog."""

    return {
        "cluster_nodes": _read_json(cluster_root / "cluster_nodes.json", {}),
        "desired_state": _read_json(cluster_root / "desired_state.json", {}),
    }


def _read_ini():
    """Read the local PBGui config file."""

    return pbgui_purefunc.load_ini_snapshot(Path(PBGDIR) / "pbgui.ini").parser


def _local_credential_capability() -> dict[str, Any]:
    """Return secret-free readiness from the local CMC store and pool."""

    result: dict[str, Any] = {
        "credential_protocol_version": 2,
        "credential_active": None,
        "credential_reason": "CMC credential pool status unavailable",
        "cmc_catalog_generation": None,
        "cmc_materialized_generation": None,
        "cmc_active_key_count": None,
    }
    try:
        cluster_root = default_cluster_root(PBGDIR)
        materialized = _read_materialized_snapshot(cluster_root)
        store = CredentialStore(PBGDIR / "data" / "credentials")
        records = store.list_cmc(active_only=False)
        pool = CmcPoolClient(
            credential_store=store,
            state_root=store.root / "cmc_pool",
            desired_state_provider=lambda: materialized,
        )
        status = pool.status()
    except Exception as exc:
        _log(SERVICE, f"CMC credential capability unavailable: {exc.__class__.__name__}", level="WARNING")
        return result

    active_count = max(int(status.get("active_credentials") or 0), 0)
    readiness: dict[str, Any]
    try:
        try:
            node_id = str(read_local_identity(cluster_root)["node_id"])
        except Exception:
            nodes = ((materialized.get("cluster_nodes") or {}).get("nodes") or {})
            node_id = str(next(iter(nodes))) if len(nodes) == 1 else ""
        readiness = local_cmc_credential_readiness(materialized, node_id, records)
        authorities = (((materialized.get("desired_state") or {}).get("cmc_pool") or {}).get("authorities") or {})
        authority_timestamps = [
            float(item.get("updated_at") or 0)
            for item in authorities.values()
            if isinstance(item, dict) and item.get("updated_at")
        ]
        if authority_timestamps:
            result["cmc_authority_state_age_seconds"] = round(
                max(time.time() - max(authority_timestamps), 0.0),
                1,
            )
    except Exception:
        standalone = [
            record for record in records
            if isinstance(record, dict) and record.get("active") and not record.get("pending")
        ]
        generation = max((int(record.get("generation") or 0) for record in standalone), default=0)
        readiness = {
            "credential_protocol_version": 2,
            "credential_active": bool(standalone),
            "credential_reason": "CMC credential pool active" if standalone else "No active materialized CMC credentials",
            "cmc_catalog_generation": generation,
            "cmc_materialized_generation": generation,
            "cmc_active_key_count": len(standalone),
            "cluster_origin_metadata": False,
        }
    if readiness.get("credential_active") is True and active_count < 1:
        readiness["credential_active"] = False
        readiness["credential_reason"] = "CMC pool has no exact eligible credential"
    result.update(readiness)

    provider_observations: list[tuple[float, dict[str, Any]]] = []
    for key_status in status.get("keys") or []:
        if not isinstance(key_status, dict):
            continue
        try:
            timestamp = float(key_status.get("provider_usage_updated_at") or key_status.get("last_settled_at") or 0)
        except (TypeError, ValueError):
            timestamp = 0
        if timestamp > 0:
            provider_observations.append((timestamp, key_status))
    if provider_observations:
        provider_timestamp, provider_status = max(provider_observations, key=lambda item: item[0])
        result["cmc_provider_usage_age_seconds"] = round(
            max(time.time() - provider_timestamp, 0.0),
            1,
        )
        for source, target in (("provider_used", "cmc_provider_used"), ("provider_limit", "cmc_provider_limit")):
            try:
                result[target] = max(float(provider_status[source]), 0.0) if provider_status.get(source) is not None else None
            except (TypeError, ValueError):
                result[target] = None
    authority_reachable = status.get("authority_reachable")
    if isinstance(authority_reachable, bool):
        result["cmc_authority_reachable"] = authority_reachable
    return result


def _parsed_list_config(text: str) -> list[str]:
    """Parse a PBGui list setting stored as CSV or Python-style list."""

    raw = str(text or "").strip()
    if not raw:
        return []
    try:
        import ast

        parsed = ast.literal_eval(raw)
        if isinstance(parsed, (list, tuple, set)):
            return [str(item).strip() for item in parsed if str(item).strip()]
    except Exception:
        pass
    return [part.strip() for part in raw.split(",") if part.strip()]


def _local_pbname(cfg: configparser.ConfigParser | None = None) -> str:
    """Return the local PBGui node name."""

    cfg = cfg or _read_ini()
    return str(cfg.get("main", "pbname", fallback="") or socket.gethostname() or "").strip() or socket.gethostname()


def _pbrun_required_for_host(pbname: str) -> bool:
    """Return whether this host has local V7 run configs assigned."""

    target = str(pbname or "").strip()
    run_root = PBGDIR / "data" / "run_v7"
    if not target or not run_root.is_dir():
        return False
    for cfg_path in run_root.glob("*/config.json"):
        try:
            payload = json.loads(cfg_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        pbgui = payload.get("pbgui") if isinstance(payload, dict) else None
        enabled_on = str((pbgui or {}).get("enabled_on") or "").strip()
        if enabled_on and enabled_on != "disabled" and enabled_on == target:
            return True
    return False


def _pbdata_required(cfg: configparser.ConfigParser | None = None) -> bool:
    """Return whether PBData has local users to poll."""

    cfg = cfg or _read_ini()
    if not cfg.has_section("pbdata"):
        return False
    fetch_users = _parsed_list_config(cfg.get("pbdata", "fetch_users", fallback=""))
    trades_users = _parsed_list_config(cfg.get("pbdata", "trades_users", fallback=""))
    return bool(fetch_users or trades_users)


def _pbcluster_required_for_host(pbname: str) -> bool:
    """Return whether local Cluster Sync expects PBCluster on this host."""

    nodes = _read_json(PBGDIR / "data" / "cluster" / "cluster_nodes.json", {})
    node_map = nodes.get("nodes") if isinstance(nodes, dict) else None
    if not isinstance(node_map, dict):
        return False
    target = str(pbname or "").strip()
    for node in node_map.values():
        if not isinstance(node, dict):
            continue
        names = {str(node.get("hostname") or "").strip(), str(node.get("pbname") or "").strip()}
        if target not in names:
            continue
        if node.get("enabled") is False:
            return False
        return bool(node.get("sync_enabled"))
    return False


def _service_expected(service_name: str) -> bool | None:
    """Return whether this host should run one PBGui service."""

    if service_name == "PBCoinData":
        return _local_credential_capability()["credential_active"]
    cfg = _read_ini()
    pbname = _local_pbname(cfg)
    if service_name == "PBCluster":
        return _pbcluster_required_for_host(pbname)
    if service_name == "PBRun":
        return _pbrun_required_for_host(pbname)
    if service_name == "PBData":
        return _pbdata_required(cfg)
    if service_name == "PBMonitorAgent":
        return True
    return True


def _auto_heal_enabled() -> bool:
    """Return whether local service auto-healing is enabled."""

    cfg = _read_ini()
    raw = str(cfg.get("vps_monitor", "auto_restart", fallback="true") or "true").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _embedded_monitor_script(name: str) -> str:
    """Extract a legacy collector script constant without importing async_monitor."""

    source_path = PBGDIR / "master" / "async_monitor.py"
    source = source_path.read_text(encoding="utf-8")
    marker = f"{name} = r'''"
    start = source.index(marker) + len(marker)
    end = source.index("'''", start)
    return source[start:end]


def _run_shell_script(script: str, *, env: dict[str, str] | None = None, timeout: int = 30) -> dict[str, Any] | None:
    """Run one local collector script and parse its JSON stdout."""

    result = subprocess.run(
        script,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout,
        env=env,
    )
    if result.returncode != 0 or not result.stdout:
        stderr = (result.stderr or "").strip()[:400]
        raise RuntimeError(f"collector failed rc={result.returncode} stderr={stderr}")
    parsed = json.loads(result.stdout.strip())
    if not isinstance(parsed, dict):
        raise RuntimeError("collector returned non-object JSON")
    return parsed


def _pb7_dir() -> Path:
    """Return the configured PB7 directory or the default sibling checkout."""

    try:
        snapshot = pbgui_purefunc.load_ini_snapshot(Path(PBGDIR) / "pbgui.ini")
        raw = snapshot.get("main", "pb7dir") if snapshot.has_option("main", "pb7dir") else ""
        raw = str(raw or "").strip()
        if raw:
            value = Path(raw).expanduser()
            return value if value.is_absolute() else Path.home() / value
    except Exception:
        pass
    return PBGDIR.parent / "pb7"


def _script_env(extra: dict[str, str] | None = None) -> dict[str, str]:
    """Build the environment used by local collector scripts."""

    env = os.environ.copy()
    env["PBGUI_PBGDIR"] = str(PBGDIR)
    env["PBGUI_PB7DIR"] = str(_pb7_dir())
    if extra:
        env.update(extra)
    return env


def _write_loop_state(loop_state: dict[str, dict[str, Any]], name: str, interval: float, *, error: str = "") -> None:
    """Update one collector status entry."""

    with _STATUS_LOCK:
        previous = loop_state.get(name, {}) if isinstance(loop_state.get(name), dict) else {}
        loop_state[name] = {
            "interval": interval,
            "last_ok": previous.get("last_ok", 0) if error else time.time(),
            "last_error": str(error or ""),
        }


def _collector_status_snapshot(loop_state: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Return a thread-safe copy of collector loop state."""

    with _STATUS_LOCK:
        return {name: dict(payload) for name, payload in loop_state.items()}


def _run_instance_snapshot() -> None:
    """Write the current instance snapshot cache."""

    cache_path = DATA_DIR / "instance_cache.json"
    host_cache = _read_json(cache_path, {})
    if not isinstance(host_cache, dict):
        host_cache = {}
    host_cache["_version"] = MONITOR_CACHE_VERSION
    payload = _run_shell_script(
        _embedded_monitor_script("INSTANCE_COLLECT_SCRIPT"),
        env=_script_env({
            "PBGUI_CACHE_VERSION": str(MONITOR_CACHE_VERSION),
            "PBGUI_CACHE": json.dumps(host_cache, separators=(",", ":")),
        }),
        timeout=30,
    ) or {}
    now = time.time()
    payload["schema_version"] = SCHEMA_VERSION
    payload["generated_at"] = now
    payload["source"] = "monitor-agent"
    if isinstance(payload.get("cache"), dict):
        _atomic_write_json(cache_path, payload["cache"])
    _atomic_write_json(DATA_DIR / "instance_snapshot.json", payload)


def _run_host_meta() -> None:
    """Write the current host metadata cache."""

    script = _embedded_monitor_script("HOST_META_SCRIPT").replace("__PBGDIR__", str(PBGDIR))
    payload = _run_shell_script(
        script,
        env=_script_env({"PBGUI_SKIP_CREDENTIAL_METADATA": "1"}),
        timeout=20,
    ) or {}
    now = time.time()
    payload.pop("coinmarketcap" + "_api_key", None)
    payload.update(_local_credential_capability())
    payload["schema_version"] = SCHEMA_VERSION
    payload["generated_at"] = now
    payload["source"] = "monitor-agent"
    _atomic_write_json(DATA_DIR / "host_meta.json", payload)


def _run_package_status() -> None:
    """Write package status cache."""

    payload = _run_shell_script(_embedded_monitor_script("PACKAGE_STATUS_SCRIPT"), env=_script_env(), timeout=75) or {}
    payload["schema_version"] = SCHEMA_VERSION
    payload["generated_at"] = time.time()
    payload["source"] = "monitor-agent"
    _atomic_write_json(DATA_DIR / "package_status.json", payload)


def _systemd_user_env() -> dict[str, str]:
    env = os.environ.copy()
    env["XDG_RUNTIME_DIR"] = env.get("XDG_RUNTIME_DIR") or f"/run/user/{os.getuid()}"
    return env


def _disabled_service_status(service_name: str) -> dict[str, Any]:
    """Return a disabled service payload for unconfigured local services."""

    return {
        "status": "disabled",
        "pid": None,
        "error": None,
        "was_restarted": False,
        "expected": False,
        "reason": "Service is not configured on this host",
        "service": service_name,
    }


def _restart_allowed(service_name: str, now: float | None = None) -> tuple[bool, str]:
    """Return whether the monitor agent may restart a service now."""

    if service_name == "PBMonitorAgent":
        return False, "PBMonitorAgent is supervised by systemd"
    now = float(now or time.time())
    history = [ts for ts in _SERVICE_RESTART_HISTORY.get(service_name, []) if now - ts < 3600.0]
    _SERVICE_RESTART_HISTORY[service_name] = history
    if history and now - history[-1] < SERVICE_RESTART_COOLDOWN_SECONDS:
        return False, "restart cooldown active"
    if len(history) >= SERVICE_RESTART_MAX_PER_HOUR:
        return False, "restart rate limit reached"
    return True, ""


def _restart_systemd_service(service_name: str, unit: str) -> tuple[bool, str]:
    """Restart a local systemd user service."""

    allowed, reason = _restart_allowed(service_name)
    if not allowed:
        return False, reason
    try:
        result = subprocess.run(
            ["systemctl", "--user", "restart", unit],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=20,
            env=_systemd_user_env(),
        )
    except Exception as exc:
        return False, str(exc)
    if result.returncode != 0:
        stderr = (result.stderr or result.stdout or "").strip()
        return False, stderr or f"systemctl restart exited {result.returncode}"
    _SERVICE_RESTART_HISTORY.setdefault(service_name, []).append(time.time())
    _log(SERVICE, f"[service] Restarted local {service_name} via {unit}", level="WARNING")
    return True, ""


def _systemd_service_status(unit: str) -> dict[str, Any] | None:
    """Return systemd status for one user service unit."""

    try:
        result = subprocess.run(
            [
                "systemctl", "--user", "show", unit,
                "-p", "LoadState", "-p", "ActiveState", "-p", "SubState",
                "-p", "Result", "-p", "MainPID", "-p", "ExecMainPID",
                "-p", "ExecMainStatus", "-p", "FragmentPath", "--no-pager",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=10,
            env=_systemd_user_env(),
        )
    except Exception:
        return None
    if result.returncode != 0:
        return None
    props: dict[str, str] = {}
    for line in (result.stdout or "").splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            props[key] = value
    if props.get("LoadState") in {"", "not-found"}:
        return None

    def int_prop(name: str) -> int:
        raw = str(props.get(name) or "")
        return int(raw) if raw.isdigit() else 0

    active = props.get("ActiveState", "")
    sub = props.get("SubState", "")
    result_state = props.get("Result", "")
    main_pid = int_prop("MainPID") or int_prop("ExecMainPID")
    exec_status = props.get("ExecMainStatus", "")
    running = active == "active" and sub == "running" and main_pid > 0
    error = None if running else (
        f"systemd {unit}: active={active or 'unknown'} sub={sub or 'unknown'} "
        f"result={result_state or 'unknown'} status={exec_status or 'unknown'}"
    )
    return {
        "status": "running" if running else "stopped",
        "pid": main_pid if running else None,
        "error": error,
        "was_restarted": False,
        "manager": "systemd",
        "unit": unit,
    }


def _pid_file_service_status(pid_file: str, process_match: str) -> dict[str, Any]:
    """Return legacy PID-file status for a PBGui service."""

    try:
        pid_text = (PBGDIR / pid_file).read_text(encoding="utf-8").strip()
    except Exception:
        return {"status": "stopped", "pid": None, "error": "No PID file or invalid PID", "was_restarted": False}
    if not pid_text.isdigit():
        return {"status": "stopped", "pid": None, "error": "No PID file or invalid PID", "was_restarted": False}
    pid = int(pid_text)
    try:
        cmdline = Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\x00", b" ").decode("utf-8", errors="ignore").lower()
    except Exception:
        cmdline = ""
    running = bool(cmdline and process_match.lower() in cmdline)
    return {
        "status": "running" if running else "stopped",
        "pid": pid if running else None,
        "error": None if running else f"PID {pid} not running",
        "was_restarted": False,
    }


def _service_status(unit: str, pid_file: str, process_match: str) -> dict[str, Any]:
    """Return service status, accepting a live legacy process over inactive systemd."""

    systemd_status = _systemd_service_status(unit)
    legacy_status = _pid_file_service_status(pid_file, process_match)
    if legacy_status.get("status") == "running":
        if systemd_status is not None:
            legacy_status = dict(legacy_status)
            legacy_status["manager"] = "legacy"
            legacy_status["unit"] = unit
        return legacy_status
    if systemd_status is not None:
        return systemd_status
    return legacy_status


def _run_service_status() -> None:
    """Write PBGui service status cache."""

    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": time.time(),
        "source": "monitor-agent",
        "services": {},
    }
    heal_enabled = _auto_heal_enabled()
    for service_name, (unit, pid_file, process_match) in PBGUI_SERVICES.items():
        expected = _service_expected(service_name)
        if expected is False:
            payload["services"][service_name] = _disabled_service_status(service_name)
            continue
        status = _service_status(unit, pid_file, process_match)
        if status.get("status") != "running" and service_name in {"PBCluster", "PBMonitorAgent"} and status.get("manager") != "systemd":
            status = {
                "status": "stopped",
                "pid": None,
                "error": f"{service_name} systemd user unit is missing or unavailable",
                "was_restarted": False,
                "manager": "systemd",
                "unit": unit,
            }
        if status.get("status") == "stopped" and heal_enabled and expected is True and service_name != "PBMonitorAgent":
            restarted, restart_error = _restart_systemd_service(service_name, unit)
            status = dict(status)
            status["was_restarted"] = bool(restarted)
            status["restart_error"] = restart_error or None
            if restarted:
                status["status"] = "restarting"
                status["error"] = None
        status["expected"] = expected
        payload["services"][service_name] = status
    _atomic_write_json(DATA_DIR / "service_status.json", payload)


def _collector_loop(name: str, interval: float, callback, loop_state: dict[str, dict[str, Any]]) -> None:
    """Run one slow collector forever."""

    while True:
        started = time.time()
        try:
            callback()
            _write_loop_state(loop_state, name, interval)
        except Exception as exc:
            _log(SERVICE, f"{name} collector failed: {exc}", level="ERROR")
            _write_loop_state(loop_state, name, interval, error=str(exc))
        elapsed = time.time() - started
        time.sleep(max(interval - elapsed, 1.0))


def _read_cpu_times() -> tuple[int, int]:
    """Return idle and total CPU ticks from /proc/stat."""

    line = Path("/proc/stat").read_text(encoding="utf-8").splitlines()[0]
    parts = line.split()
    values = [int(value) for value in parts[1:]]
    return values[3], sum(values)


def _cpu_percent(previous: tuple[int, int], current: tuple[int, int]) -> float:
    """Return CPU percent between two /proc/stat samples."""

    idle_prev, total_prev = previous
    idle_now, total_now = current
    total_delta = total_now - total_prev
    if total_delta <= 0:
        return 0.0
    idle_delta = idle_now - idle_prev
    return round(max(0.0, min((1.0 - (idle_delta / total_delta)) * 100.0, 100.0)), 1)


def _memory_payload() -> tuple[list[Any], list[Any]]:
    """Return memory and swap payloads matching the legacy stream shape."""

    values: dict[str, int] = {}
    for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
        key, _, value = line.partition(":")
        if key in {"MemTotal", "MemAvailable", "SwapTotal", "SwapFree"}:
            values[key] = int(value.split()[0]) * 1024
    mem_total = values.get("MemTotal", 0)
    mem_available = values.get("MemAvailable", 0)
    mem_used = max(mem_total - mem_available, 0)
    mem_pct = round(mem_used / mem_total * 100, 1) if mem_total else 0.0
    swap_total = values.get("SwapTotal", 0)
    swap_free = values.get("SwapFree", 0)
    swap_used = max(swap_total - swap_free, 0)
    swap_pct = round(swap_used / swap_total * 100, 1) if swap_total else 0.0
    return [mem_total, mem_available, mem_pct, mem_used], [swap_total, swap_used, swap_free, swap_pct]


def _disk_payload() -> list[Any]:
    """Return disk payload matching the legacy stream shape."""

    stat = os.statvfs("/")
    total = stat.f_frsize * stat.f_blocks
    used = stat.f_frsize * (stat.f_blocks - stat.f_bfree)
    free = stat.f_frsize * stat.f_bavail
    pct = round(used / total * 100, 1) if total else 0.0
    return [total, used, free, pct]


def _peak(samples: deque[tuple[float, float]]) -> float:
    """Return the peak value from a bounded metric history."""

    if not samples:
        return 0.0
    return round(max(value for _ts, value in samples), 1)


def _window(samples: deque[tuple[float, Any]]) -> float:
    """Return the age covered by a metric history."""

    if not samples:
        return 0.0
    return round(max(time.time() - samples[0][0], 0.0), 1)


def _trim_history(samples: deque[tuple[float, Any]], now: float) -> None:
    """Trim one in-memory history to the configured 60 second window."""

    cutoff = now - HISTORY_SECONDS
    while samples and samples[0][0] < cutoff:
        samples.popleft()


def _bot_processes(previous: dict[int, tuple[int, float]], history: dict[int, deque[tuple[float, int]]], names: dict[int, str]) -> list[dict[str, Any]]:
    """Collect live bot CPU and memory values from /proc."""

    now = time.time()
    bots: list[dict[str, Any]] = []
    alive: set[int] = set()
    for proc_dir in Path("/proc").iterdir():
        if not proc_dir.name.isdigit():
            continue
        pid = int(proc_dir.name)
        try:
            raw_cmdline = (proc_dir / "cmdline").read_bytes().replace(b"\x00", b" ").decode("utf-8", errors="ignore")
        except Exception:
            continue
        if "main.py" not in raw_cmdline or "config_run.json" not in raw_cmdline:
            continue
        try:
            stat_parts = (proc_dir / "stat").read_text(encoding="utf-8").split()
            ticks = int(stat_parts[13]) + int(stat_parts[14])
        except Exception:
            continue
        alive.add(pid)
        previous_sample = previous.get(pid)
        cpu = 0.0
        if previous_sample:
            elapsed = now - previous_sample[1]
            if elapsed > 0:
                cpu = round((ticks - previous_sample[0]) / (elapsed * CPU_TICKS_PER_SECOND) * 100, 1)
        previous[pid] = (ticks, now)
        samples = history.setdefault(pid, deque())
        samples.append((now, ticks))
        _trim_history(samples, now)
        cpu_60s = 0.0
        cpu_60s_window = _window(samples)
        for sample_ts, sample_ticks in samples:
            if now - sample_ts >= 60.0:
                elapsed = now - sample_ts
                if elapsed > 0:
                    cpu_60s = round((ticks - sample_ticks) / (elapsed * CPU_TICKS_PER_SECOND) * 100, 1)
                    cpu_60s_window = round(elapsed, 1)
                break
        name = names.get(pid) or _bot_name_from_cmdline(raw_cmdline)
        if name:
            names[pid] = name
            rss_mb, swap_mb = _process_memory_mb(proc_dir)
            bots.append({"name": name, "cpu": cpu, "cpu_60s": cpu_60s, "cpu_60s_window": cpu_60s_window, "rss_mb": rss_mb, "swap_mb": swap_mb})
    for pid in list(previous):
        if pid not in alive:
            previous.pop(pid, None)
            history.pop(pid, None)
            names.pop(pid, None)
    return bots


def _bot_name_from_cmdline(cmdline: str) -> str:
    """Extract the bot name from a passivbot config path."""

    for part in cmdline.split():
        if part.endswith("/config_run.json"):
            return Path(part).parent.name
    return ""


def _process_memory_mb(proc_dir: Path) -> tuple[float, float]:
    """Return RSS and swap usage for a process in MiB."""

    rss_mb = 0.0
    swap_mb = 0.0
    try:
        for line in (proc_dir / "status").read_text(encoding="utf-8").splitlines():
            if line.startswith("VmRSS:"):
                rss_mb = round(int(line.split()[1]) / 1024, 1)
            elif line.startswith("VmSwap:"):
                swap_mb = round(int(line.split()[1]) / 1024, 1)
    except Exception:
        pass
    return rss_mb, swap_mb


def _append_live_sample(payload: dict[str, Any]) -> None:
    """Append a live metrics sample to NDJSON."""

    from logging_helpers import append_managed_transcript_line

    append_managed_transcript_line(
        _pbgui_dir() / "data" / "logs" / "monitor-agent" / "live_metrics.ndjson",
        json.dumps(payload, separators=(",", ":")),
        "monitor_agent_live",
    )


def _rotate_live_samples() -> None:
    """Apply configured managed rotation to the closed live metrics file."""

    from logging_helpers import rotate_managed_log_before_open

    path = _pbgui_dir() / "data" / "logs" / "monitor-agent" / "live_metrics.ndjson"
    try:
        rotate_managed_log_before_open(path, "monitor_agent_live")
    except Exception as exc:
        _log(SERVICE, f"Live metrics rotation failed: {exc}", level="WARNING")


def _collector_status(loop_state: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Build the agent health payload."""

    return {
        "schema_version": SCHEMA_VERSION,
        "hostname": socket.gethostname(),
        "agent_version": "1",
        "generated_at": time.time(),
        "loops": _collector_status_snapshot(loop_state),
    }


def run() -> None:
    """Run the monitor agent forever."""

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    loop_state = {
        "live_metrics": {"interval": LIVE_INTERVAL_SECONDS, "last_ok": 0, "last_error": ""},
        "instances": {"interval": INSTANCE_INTERVAL_SECONDS, "last_ok": 0, "last_error": ""},
        "host_meta": {"interval": HOST_META_INTERVAL_SECONDS, "last_ok": 0, "last_error": ""},
        "services": {"interval": SERVICE_INTERVAL_SECONDS, "last_ok": 0, "last_error": ""},
        "package_status": {"interval": PACKAGE_INTERVAL_SECONDS, "last_ok": 0, "last_error": ""},
    }
    cpu_history: deque[tuple[float, tuple[int, int]]] = deque()
    mem_history: deque[tuple[float, float]] = deque()
    disk_history: deque[tuple[float, float]] = deque()
    swap_history: deque[tuple[float, float]] = deque()
    bot_previous: dict[int, tuple[int, float]] = {}
    bot_history: dict[int, deque[tuple[float, int]]] = {}
    bot_names: dict[int, str] = {}
    previous_cpu = _read_cpu_times()
    last_status_write = 0.0
    last_rotate = 0.0
    _log(SERVICE, "Monitor agent started", level="INFO")
    for name, interval, callback in (
        ("instances", INSTANCE_INTERVAL_SECONDS, _run_instance_snapshot),
        ("host_meta", HOST_META_INTERVAL_SECONDS, _run_host_meta),
        ("services", SERVICE_INTERVAL_SECONDS, _run_service_status),
        ("package_status", PACKAGE_INTERVAL_SECONDS, _run_package_status),
    ):
        threading.Thread(
            target=_collector_loop,
            args=(name, interval, callback, loop_state),
            daemon=True,
            name=f"monitor-agent-{name}",
        ).start()
    while True:
        start = time.time()
        try:
            current_cpu = _read_cpu_times()
            cpu = _cpu_percent(previous_cpu, current_cpu)
            previous_cpu = current_cpu
            now = time.time()
            cpu_history.append((now, current_cpu))
            _trim_history(cpu_history, now)
            cpu_60s = 0.0
            cpu_60s_window = _window(cpu_history)
            for sample_ts, sample_cpu in cpu_history:
                if now - sample_ts >= 60.0:
                    cpu_60s = _cpu_percent(sample_cpu, current_cpu)
                    cpu_60s_window = round(now - sample_ts, 1)
                    break
            mem, swap = _memory_payload()
            disk = _disk_payload()
            mem_history.append((now, float(mem[2])))
            disk_history.append((now, float(disk[3])))
            if float(swap[0] or 0) > 0:
                swap_history.append((now, float(swap[3])))
            _trim_history(mem_history, now)
            _trim_history(disk_history, now)
            _trim_history(swap_history, now)
            payload = {
                "schema_version": SCHEMA_VERSION,
                "generated_at": now,
                "ts": now,
                "cpu": cpu,
                "cpu_60s": cpu_60s,
                "cpu_60s_window": cpu_60s_window,
                "cpu_60s_samples": len(cpu_history),
                "mem": mem,
                "disk": disk,
                "swap": swap,
                "mem_60s_peak": _peak(mem_history),
                "mem_60s_window": _window(mem_history),
                "disk_60s_peak": _peak(disk_history),
                "disk_60s_window": _window(disk_history),
                "swap_60s_peak": _peak(swap_history),
                "swap_60s_window": _window(swap_history),
                "bots": _bot_processes(bot_previous, bot_history, bot_names),
            }
            _atomic_write_json(DATA_DIR / "live_metrics.latest.json", payload)
            _append_live_sample(payload)
            _write_loop_state(loop_state, "live_metrics", LIVE_INTERVAL_SECONDS)
            if now - last_rotate >= 60.0:
                _rotate_live_samples()
                last_rotate = now
            if now - last_status_write >= STATUS_INTERVAL_SECONDS:
                _atomic_write_json(DATA_DIR / "collector_status.json", _collector_status(loop_state))
                last_status_write = now
        except Exception as exc:
            _log(SERVICE, f"Live metrics loop failed: {exc}", level="ERROR")
            _write_loop_state(loop_state, "live_metrics", LIVE_INTERVAL_SECONDS, error=str(exc))
            _atomic_write_json(DATA_DIR / "collector_status.json", _collector_status(loop_state))
        elapsed = time.time() - start
        time.sleep(max(LIVE_INTERVAL_SECONDS - elapsed, 0.05))


if __name__ == "__main__":
    from credential_process_registry import ProcessCapabilityHeartbeat

    with ProcessCapabilityHeartbeat(PBGDIR, SERVICE):
        run()
