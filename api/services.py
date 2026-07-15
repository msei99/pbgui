"""FastAPI router for Services management (start/stop/settings for all PBGui daemons)."""

from __future__ import annotations

import asyncio
import ast
from collections import Counter
import glob
import json
import importlib
import math
import os
import pwd
import re
import shlex
import signal
import socket
import subprocess
import sys
import time
import traceback
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, ConfigDict, Field

from api.auth import require_auth, SessionToken
from api.vps import get_monitor
from cluster_credential_publisher import ClusterCredentialPublisher, CredentialPublicationError
from cmc_leases import CmcLeaseAuthority
from cmc_pool import CmcPoolClient
from cmc_runtime import build_cmc_pool_client, read_cmc_cluster_snapshot
from credential_store import CredentialNotFoundError, CredentialStore, credential_mutation_lock
from credential_reconciler import reconcile_pending_credentials
from master.cluster_state import credential_lifecycle_status, default_cluster_root, read_local_identity, rebuild_materialized_state
from pbgui_purefunc import PBGDIR, load_ini, load_ini_snapshot, save_ini, save_ini_section, update_ini
from ini_settings import APPLY_GROUPS, apply_metadata, apply_metadata_for
from logging_helpers import human_log as _log
from operation_store import DurableOperationStore

SERVICE = "Services"

router = APIRouter()

_SERVICES = ["pbcluster", "pbrun", "pbdata", "pbcoindata", "monitor-agent", "api-server"]
_SYSTEMD_SERVICE_UNITS = {
    "pbcluster": "pbgui-pbcluster.service",
    "pbrun": "pbgui-pbrun.service",
    "pbdata": "pbgui-pbdata.service",
    "pbcoindata": "pbgui-pbcoindata.service",
    "monitor-agent": "pbgui-monitor-agent.service",
    "api-server": "pbgui-api.service",
}
_SYSTEMD_RUNNING_STATES = {"active", "activating", "reloading"}
_SYSTEMD_ENABLED_STATES = {"enabled", "enabled-runtime"}
_SERVICE_SCRIPT_NAMES = {
    "pbcluster": "PBCluster.py",
    "pbrun": "PBRun.py",
    "pbdata": "PBData.py",
    "pbcoindata": "PBCoinData.py",
    "monitor-agent": "monitor_agent.py",
    "api-server": "PBApiServer.py",
}
_SERVICE_PID_FILES = {
    "pbcluster": "pbcluster.pid",
    "pbrun": "pbrun.pid",
    "pbdata": "pbdata.pid",
    "pbcoindata": "pbcoindata.pid",
    "monitor-agent": "pbmonitoragent.pid",
    "api-server": "api_server.pid",
}
_MIGRATION_DEFAULT_SERVICES = ["api", "pbcluster", "pbrun", "pbdata", "pbcoindata", "monitor-agent"]
_MIGRATION_LEGACY_STOP_SERVICES = ["pbcluster", "pbrun", "pbdata", "pbcoindata"]
_fetch_summary_snapshot: Dict[str, Any] = {}
_poller_metrics_snapshot: Dict[str, Any] = {}
_TASK_WORKER_STOP_TIMEOUT_S = 35.0


def _get_service(name: str):
    """Instantiate and return the service object for the given name."""
    if name == "pbcluster":
        from PBCluster import PBCluster
        return PBCluster()
    if name == "pbrun":
        from PBRun import PBRun
        return PBRun()
    if name == "pbdata":
        from PBData import PBData
        obj = PBData.__new__(PBData)
        obj.piddir = Path(f'{PBGDIR}/data/pid')
        obj.pidfile = Path(f'{PBGDIR}/data/pid/pbdata.pid')
        obj.my_pid = None
        return obj
    if name == "pbcoindata":
        from PBCoinData import CoinData
        return CoinData()
    if name == "monitor-agent":
        raise RuntimeError("PBMonitorAgent requires the pbgui-monitor-agent.service systemd user unit.")
    if name == "api-server":
        # Lazy import to avoid circular import (PBApiServer.py imports api/services.py)
        mod = importlib.import_module("PBApiServer")
        return mod.PBApiServer()
    raise ValueError(f"Unknown service: {name}")


def _systemd_unit_path(unit: str) -> Path:
    """Return the per-user systemd unit path for a PBGui service."""
    return Path.home() / ".config" / "systemd" / "user" / unit


def _systemd_unit_for_service(name: str) -> str | None:
    """Return the systemd unit for a service when this install manages it."""
    unit = _SYSTEMD_SERVICE_UNITS.get(name)
    if not unit:
        return None
    return unit if _systemd_unit_path(unit).exists() else None


def _optional_service_blocker(name: str) -> str:
    return ""


def _systemd_user_env() -> dict[str, str]:
    """Build an environment that can talk to the current user's systemd manager."""
    env = os.environ.copy()
    env.setdefault("XDG_RUNTIME_DIR", f"/run/user/{os.getuid()}")
    return env


def _run_user_systemctl(args: list[str], *, timeout: int = 15) -> subprocess.CompletedProcess[str]:
    """Run systemctl against the current user's service manager."""
    return subprocess.run(
        ["systemctl", "--user", *args],
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=_systemd_user_env(),
    )


def _queue_api_systemd_restart(unit: str) -> tuple[bool, str]:
    """Queue an API restart from a transient unit outside the API service cgroup."""
    restart_unit = f"pbgui-api-restart-{os.getpid()}-{time.time_ns()}"
    restart_cmd = f"sleep 0.5\nsystemctl --user restart {shlex.quote(unit)}"
    try:
        proc = subprocess.run(
            ["systemd-run", "--user", f"--unit={restart_unit}", "--collect", "/bin/bash", "-lc", restart_cmd],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
            env=_systemd_user_env(),
        )
    except Exception as exc:
        return False, str(exc)
    output = ((proc.stderr or "") + (proc.stdout or "")).strip()
    if proc.returncode != 0:
        return False, output or str(proc.returncode)
    return True, output or restart_unit


def _systemd_action_error_message(
    *,
    action: str,
    unit: str,
    output: str,
    status: dict[str, Any] | None,
) -> str:
    """Build a user-facing systemd action error with final state context."""

    message = f"systemctl --user {action} {unit} failed"
    if output:
        message = f"{message}: {output}"
    if status:
        state = str(status.get("systemd_state") or "unknown")
        running = "running" if status.get("running") else "stopped"
        message = f"{message}\nCurrent state: {running} ({state})."
        if action == "stop" and status.get("running"):
            message = (
                f"{message}\nThe service may have been killed after TimeoutStopSec and "
                "restarted automatically by systemd."
            )
        elif action == "restart" and status.get("running"):
            message = f"{message}\nThe service is running, but systemd reported the restart action as failed."
    return message


def _systemd_service_status(name: str) -> dict[str, Any] | None:
    """Return systemd status for service, or None when no unit is installed."""
    unit = _systemd_unit_for_service(name)
    if not unit:
        return None
    proc = _run_user_systemctl(["is-active", unit], timeout=5)
    if proc.returncode not in {0, 3, 4}:
        return None
    state = (proc.stdout.strip().splitlines() or ["unknown"])[0]
    return {
        "running": state in _SYSTEMD_RUNNING_STATES,
        "manager": "systemd",
        "unit": unit,
        "systemd_state": state,
        **_systemd_enable_status(name, unit),
    }


def _systemd_enable_status(name: str, unit: str | None = None) -> dict[str, Any]:
    """Return user-systemd enablement fields for a service."""
    unit = unit or _systemd_unit_for_service(name)
    if not unit:
        return {"enabled": False, "can_enable": False}
    try:
        proc = _run_user_systemctl(["is-enabled", unit], timeout=5)
    except Exception as exc:
        return {
            "enabled": False,
            "can_enable": True,
            "systemd_enabled_state": "unknown",
            "systemd_enabled_error": str(exc),
        }
    state = (proc.stdout.strip().splitlines() or ["unknown"])[0]
    return {
        "enabled": proc.returncode == 0 and state in _SYSTEMD_ENABLED_STATES,
        "can_enable": True,
        "systemd_enabled_state": state,
    }


def _systemd_service_action(name: str, action: str) -> dict[str, Any] | None:
    """Run a lifecycle/autostart action through systemd when a unit is installed."""
    if action not in {"start", "stop", "restart", "enable", "disable"}:
        raise ValueError(f"Unsupported service action: {action}")
    unit = _systemd_unit_for_service(name)
    if not unit:
        return None
    args = [action, "--now", unit] if action in {"enable", "disable"} else [action, unit]
    try:
        proc = _run_user_systemctl(args, timeout=60)
    except subprocess.TimeoutExpired as exc:
        output = "\n".join(str(part).strip() for part in (exc.stderr, exc.stdout) if str(part or "").strip())
        status = _systemd_service_status(name)
        if status is not None:
            result = dict(status)
            result["action_failed"] = True
            result["error"] = _systemd_action_error_message(
                action=action,
                unit=unit,
                output=output or f"timed out after {int(exc.timeout or 0)}s",
                status=status,
            )
            return result
        raise RuntimeError(output or f"systemctl --user {' '.join(args)} timed out")
    if proc.returncode != 0:
        output = ((proc.stderr or "") + (proc.stdout or "")).strip()
        status = _systemd_service_status(name)
        if status is not None:
            result = dict(status)
            result["action_failed"] = True
            result["systemd_action_returncode"] = int(proc.returncode)
            result["error"] = _systemd_action_error_message(
                action=action,
                unit=unit,
                output=output,
                status=status,
            )
            return result
        raise RuntimeError(output or f"systemctl --user {' '.join(args)} failed")
    status = _systemd_service_status(name)
    if status is not None:
        return status
    running = _legacy_service_running(name) if action in {"enable", "disable"} else action != "stop"
    return {"running": running, "manager": "systemd", "unit": unit, **_systemd_enable_status(name, unit)}


def _service_pid_file(name: str) -> Path | None:
    pid_name = _SERVICE_PID_FILES.get(name)
    if not pid_name:
        return None
    return Path(PBGDIR) / "data" / "pid" / pid_name


def _read_service_pid(name: str) -> int | None:
    pid_file = _service_pid_file(name)
    if not pid_file or not pid_file.exists():
        return None
    try:
        raw = pid_file.read_text(encoding="utf-8").strip()
        return int(raw) if raw.isnumeric() else None
    except Exception:
        return None


def _pid_matches_service(pid: int, name: str) -> bool:
    script = _SERVICE_SCRIPT_NAMES.get(name, "").lower()
    if not pid or not script:
        return False
    try:
        import psutil  # type: ignore

        if not psutil.pid_exists(pid):
            return False
        cmdline = [str(part).lower() for part in psutil.Process(pid).cmdline()]
        return any(Path(part).name == script or part.endswith(script) for part in cmdline)
    except Exception:
        return False


def _legacy_service_running(name: str) -> bool:
    """Check legacy daemon status without instantiating service classes."""
    pid = _read_service_pid(name)
    if pid and _pid_matches_service(pid, name):
        return True
    return any(item.get("service") == name for item in _collect_pbgui_daemon_processes())


def _service_status(name: str) -> dict[str, Any]:
    """Return service status using systemd when available, otherwise legacy PID checks."""
    blocker = _optional_service_blocker(name)
    systemd_status = _systemd_service_status(name)
    if systemd_status is not None and systemd_status.get("running"):
        return systemd_status
    legacy_running = _legacy_service_running(name)
    if legacy_running:
        result = {"running": True, "manager": "legacy"}
        if systemd_status is not None:
            result["unit"] = systemd_status.get("unit")
            result["systemd_state"] = systemd_status.get("systemd_state")
            result.update(_systemd_enable_status(name, str(systemd_status.get("unit") or "") or None))
        else:
            result.update(_systemd_enable_status(name))
        return result
    if blocker:
        result = {
            "running": False,
            "manager": "disabled",
            "expected": False,
            "reason": blocker,
            "can_start": False,
            "enable_blocked_reason": blocker,
        }
        if systemd_status is not None:
            result["unit"] = systemd_status.get("unit")
            result["systemd_state"] = systemd_status.get("systemd_state")
            result.update(_systemd_enable_status(name, str(systemd_status.get("unit") or "") or None))
        else:
            result.update(_systemd_enable_status(name))
        return result
    if systemd_status is not None:
        return systemd_status
    return {"running": False, "manager": "legacy", **_systemd_enable_status(name)}


def _service_action(name: str, action: str) -> dict[str, Any]:
    """Start, stop, restart, enable, or disable a PBGui service."""
    if action in {"start", "restart", "enable"}:
        blocker = _optional_service_blocker(name)
        if blocker:
            result = _service_status(name)
            result["error"] = blocker
            return result
    systemd_status = _systemd_service_action(name, action)
    if systemd_status is not None:
        if systemd_status.get("error"):
            return systemd_status
        return _service_status(name) if action in {"enable", "disable"} else systemd_status

    if action in {"enable", "disable"}:
        raise RuntimeError(f"systemd unit is not installed for {name}")

    obj = _get_service(name)
    if action == "start":
        if not obj.is_running():
            obj.run()
    elif action == "stop":
        if obj.is_running():
            obj.stop()
    elif action == "restart":
        if obj.is_running():
            obj.stop()
            time.sleep(1.5)
        obj.run()
    else:
        raise ValueError(f"Unsupported service action: {action}")
    return _service_status(name)


def _current_username() -> str:
    """Return the Unix user running PBGui."""
    return pwd.getpwuid(os.getuid()).pw_name


def _detect_pbgui_python() -> str:
    """Return the Python executable that should run PBGui services."""
    pbgdir = Path(PBGDIR)
    candidates = [
        Path(sys.executable),
        pbgdir.parent / "venv_pbgui" / "bin" / "python",
        pbgdir.parent / "venv_pbgui312" / "bin" / "python",
        pbgdir.parent / "venv" / "bin" / "python",
    ]
    seen: set[Path] = set()
    for candidate in candidates:
        try:
            resolved = candidate.resolve()
        except Exception:
            resolved = candidate
        if resolved in seen:
            continue
        seen.add(resolved)
        if candidate.exists() and os.access(candidate, os.X_OK):
            return str(candidate)
    return sys.executable


def _legacy_crontab_line_matches(line: str) -> bool:
    """Return True for PBGui legacy autostart crontab entries."""
    stripped = line.strip()
    if not stripped or stripped.startswith("#") or not stripped.startswith("@reboot"):
        return False
    parts = stripped.split(None, 1)
    command = parts[1] if len(parts) > 1 else ""
    target_dir = Path(PBGDIR).resolve()
    target_start = target_dir / "start.sh"
    script_names = set(_SERVICE_SCRIPT_NAMES.values())
    candidate_paths: list[Path] = []

    def expand_cron_path(value: str) -> Path:
        text = value.strip().strip("'\"")
        home = str(Path.home())
        if text.startswith("~/"):
            text = f"{home}/{text[2:]}"
        elif text.startswith("$HOME/"):
            text = f"{home}/{text[6:]}"
        elif text.startswith("${HOME}/"):
            text = f"{home}/{text[8:]}"
        return Path(text).resolve(strict=False)

    for match in re.findall(r"(?:^|[\s;&|])((?:/|~/|\$HOME/|\$\{HOME\}/)[^\s'\";&|]+)", command):
        try:
            candidate_paths.append(expand_cron_path(match))
        except Exception:
            continue

    for path in candidate_paths:
        if path == target_start:
            return True
        if path.parent == target_dir and path.name in script_names:
            return True

    if any(path == target_dir for path in candidate_paths):
        return any(script in command for script in script_names)
    return False


def _read_legacy_crontab() -> dict[str, Any]:
    """Inspect current user's crontab for legacy PBGui autostarts."""
    try:
        proc = subprocess.run(["crontab", "-l"], check=False, capture_output=True, text=True, timeout=10)
    except FileNotFoundError:
        return {"checked": False, "entries": [], "warning": "crontab command is not installed."}
    except Exception as exc:
        return {"checked": False, "entries": [], "warning": f"Could not inspect crontab: {exc}"}

    if proc.returncode != 0:
        text = ((proc.stderr or "") + (proc.stdout or "")).strip().lower()
        if "no crontab" in text:
            return {"checked": True, "entries": [], "warning": ""}
        return {"checked": False, "entries": [], "warning": ((proc.stderr or proc.stdout or "").strip() or "Could not inspect crontab.")}

    lines = proc.stdout.splitlines()
    entries = [line for line in lines if _legacy_crontab_line_matches(line)]
    return {"checked": True, "entries": entries, "warning": "", "lines": lines}


def _remove_legacy_crontab_entries() -> dict[str, Any]:
    """Remove PBGui legacy autostart entries from current user's crontab."""
    status = _read_legacy_crontab()
    entries = list(status.get("entries") or [])
    if not status.get("checked") or not entries:
        status.pop("lines", None)
        status["removed"] = []
        return status

    lines = list(status.get("lines") or [])
    remaining = [line for line in lines if not _legacy_crontab_line_matches(line)]
    tmp_path = Path(PBGDIR) / "data" / "tmp" / f"crontab-migration-{int(time.time())}.tmp"
    tmp_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path.write_text("\n".join(remaining) + ("\n" if remaining else ""), encoding="utf-8")
    try:
        proc = subprocess.run(["crontab", str(tmp_path)], check=False, capture_output=True, text=True, timeout=10)
    finally:
        tmp_path.unlink(missing_ok=True)
    if proc.returncode != 0:
        return {
            "checked": True,
            "entries": entries,
            "removed": [],
            "warning": ((proc.stderr or proc.stdout or "").strip() or "Could not update crontab."),
        }
    return {"checked": True, "entries": entries, "removed": entries, "warning": ""}


def _collect_pbgui_daemon_processes() -> list[dict[str, Any]]:
    """Return PBGui daemon processes visible to this user."""
    try:
        import psutil  # type: ignore
    except Exception:
        return []

    processes: list[dict[str, Any]] = []
    current_pid = os.getpid()
    for proc in psutil.process_iter(["pid", "cmdline", "username"]):
        try:
            cmdline = [str(arg) for arg in (proc.info.get("cmdline") or [])]
        except Exception:
            continue
        if not cmdline:
            continue
        for service, script in _SERVICE_SCRIPT_NAMES.items():
            if any(Path(arg).name == script for arg in cmdline):
                pid = int(proc.info.get("pid") or 0)
                processes.append(
                    {
                        "pid": pid,
                        "service": service,
                        "script": script,
                        "current": pid == current_pid,
                        "cmdline": " ".join(cmdline),
                        "username": proc.info.get("username") or "",
                    }
                )
                break
    return processes


def _migration_systemd_units() -> list[dict[str, Any]]:
    """Return status rows for PBGui systemd user units."""
    rows: list[dict[str, Any]] = []
    for service, unit in _SYSTEMD_SERVICE_UNITS.items():
        unit_path = _systemd_unit_path(unit)
        row: dict[str, Any] = {
            "service": service,
            "unit": unit,
            "path": str(unit_path),
            "exists": unit_path.exists(),
            "enabled": False,
            "active": False,
            "state": "missing",
        }
        if unit_path.exists():
            row["state"] = "unknown"
            status = _systemd_service_status(service)
            if status is not None:
                row["active"] = bool(status.get("running"))
                row["state"] = status.get("systemd_state") or "unknown"
            enabled_proc = _run_user_systemctl(["is-enabled", unit], timeout=5)
            row["enabled"] = enabled_proc.returncode == 0 and enabled_proc.stdout.strip() == "enabled"
        rows.append(row)
    return rows


def _parsed_list_config(raw: Any) -> list[str]:
    """Parse list-like pbgui.ini values used by PBData."""
    text = str(raw or "").strip()
    if not text:
        return []
    try:
        parsed = ast.literal_eval(text)
    except Exception:
        parsed = None
    if isinstance(parsed, (list, tuple, set)):
        return [str(item).strip() for item in parsed if str(item).strip()]
    return [part.strip() for part in text.split(",") if part.strip()]


def _pbrun_required_for_host(pbgdir: Path) -> bool:
    """Return whether PBRun has local V7 configs assigned to this PBGui host."""
    pbname = str(load_ini("main", "pbname") or socket.gethostname() or "").strip()
    if not pbname:
        return False
    run_root = pbgdir / "data" / "run_v7"
    if not run_root.is_dir():
        return False
    for cfg_path in run_root.glob("*/config.json"):
        try:
            payload = json.loads(cfg_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        pbgui = payload.get("pbgui") if isinstance(payload, dict) else None
        enabled_on = str((pbgui or {}).get("enabled_on") or "").strip()
        if enabled_on and enabled_on != "disabled" and enabled_on == pbname:
            return True
    return False


def _pbdata_required() -> bool:
    """Return whether PBData has any configured users to process."""
    fetch_users = _parsed_list_config(load_ini("pbdata", "fetch_users") or "")
    trades_users = _parsed_list_config(load_ini("pbdata", "trades_users") or "")
    return bool(fetch_users or trades_users)


def _pbcoindata_required() -> bool:
    """Return strict local CMC readiness for PBCoinData expected state."""

    try:
        return bool(_cmc_pool_payload().get("ready"))
    except Exception:
        return False


def _migration_required_services(pbgdir: Path | None = None) -> set[str]:
    """Return local services that should be enabled by migration on this host."""
    root = Path(pbgdir or PBGDIR)
    required = {"api-server"}
    required.add("monitor-agent")
    if _pbcluster_required(root):
        required.add("pbcluster")
    if _pbrun_required_for_host(root):
        required.add("pbrun")
    if _pbdata_required():
        required.add("pbdata")
    if _pbcoindata_required():
        required.add("pbcoindata")
    return required


def _pbcluster_required(pbgdir: Path) -> bool:
    """Return whether PBCluster should run for this local Cluster Sync node."""
    pbname = str(load_ini("main", "pbname") or socket.gethostname() or "").strip()
    nodes_path = pbgdir / "data" / "cluster" / "cluster_nodes.json"
    try:
        payload = json.loads(nodes_path.read_text(encoding="utf-8"))
    except Exception:
        return False
    nodes = payload.get("nodes") if isinstance(payload, dict) else None
    if not isinstance(nodes, dict):
        return False
    for node in nodes.values():
        if not isinstance(node, dict):
            continue
        names = {str(node.get("hostname") or "").strip(), str(node.get("pbname") or "").strip()}
        if pbname in names and node.get("enabled") is not False:
            return bool(node.get("sync_enabled"))
    return False


def _migration_status_payload() -> dict[str, Any]:
    """Build the migration preflight payload for the Services UI."""
    pbgdir = Path(PBGDIR)
    legacy_start_sh = pbgdir / "start.sh"
    crontab = _read_legacy_crontab()
    processes = _collect_pbgui_daemon_processes()
    units = _migration_systemd_units()
    required_units = _migration_required_services(pbgdir)
    missing_default_units = [row for row in units if row["service"] in required_units and not row["exists"]]
    not_ready_default_units = [
        row for row in units
        if row["service"] in required_units and row["exists"] and (not row["enabled"] or not row["active"])
    ]
    legacy_entries = list(crontab.get("entries") or [])
    legacy_start_sh_exists = legacy_start_sh.exists()
    warnings = []
    if crontab.get("warning"):
        warnings.append(
            "Could not inspect/remove legacy crontab autostart. If you configured PBGui autostart manually, remove it yourself to avoid duplicate starts."
        )
    if not (pbgdir / "setup" / "setup_systemd.sh").exists():
        warnings.append("setup/setup_systemd.sh is missing; update PBGui before migrating to systemd.")
    return {
        "user": _current_username(),
        "uid": os.getuid(),
        "pbgui_dir": str(pbgdir),
        "pbgui_python": _detect_pbgui_python(),
        "pb7dir": str(load_ini("main", "pb7dir") or ""),
        "pb7venv": str(load_ini("main", "pb7venv") or ""),
        "systemd_unit_dir": str(Path.home() / ".config" / "systemd" / "user"),
        "systemd_units": units,
        "required_services": sorted(required_units),
        "missing_default_units": missing_default_units,
        "not_ready_default_units": not_ready_default_units,
        "legacy_crontab": {k: v for k, v in crontab.items() if k != "lines"},
        "legacy_start_sh": {"path": str(legacy_start_sh), "exists": legacy_start_sh_exists},
        "processes": processes,
        "migration_needed": bool(missing_default_units or not_ready_default_units or legacy_entries or legacy_start_sh_exists),
        "warnings": warnings,
    }


def _migration_enable_services(status: dict[str, Any]) -> list[str]:
    """Return the service set that migration would enable."""
    required = set(status.get("required_services") or [])
    services = []
    for service in _MIGRATION_DEFAULT_SERVICES:
        service_id = "api-server" if service == "api" else service
        if service_id in required:
            services.append(service)
    return services


def _migration_setup_command(status: dict[str, Any]) -> tuple[list[str], list[str], Path, str, Path, str]:
    """Build the exact setup_systemd.sh command used by migration."""
    user = _current_username()
    pbgdir = Path(PBGDIR)
    python_bin = _detect_pbgui_python()
    setup_script = pbgdir / "setup" / "setup_systemd.sh"
    enable_services = _migration_enable_services(status)
    cmd = [
        "bash",
        str(setup_script),
        "--user",
        user,
        "--pbgui-dir",
        str(pbgdir),
        "--python",
        python_bin,
        "--enable",
        ",".join(enable_services),
        "--no-start",
    ]
    return cmd, enable_services, setup_script, user, pbgdir, python_bin


def _try_enable_linger(user: str) -> dict[str, Any]:
    """Try to enable linger without prompting for a password."""
    commands = [["loginctl", "enable-linger", user]]
    if os.getuid() != 0:
        commands.append(["sudo", "-n", "loginctl", "enable-linger", user])
    last_output = ""
    for command in commands:
        try:
            proc = subprocess.run(command, check=False, capture_output=True, text=True, timeout=10)
        except FileNotFoundError:
            last_output = f"Command not found: {command[0]}"
            continue
        except Exception as exc:
            last_output = str(exc)
            continue
        if proc.returncode == 0:
            if os.getuid() != 0:
                uid = os.getuid()
                try:
                    subprocess.run(
                        ["sudo", "-n", "systemctl", "start", f"user@{uid}.service"],
                        check=False,
                        capture_output=True,
                        text=True,
                        timeout=10,
                    )
                except Exception:
                    pass
            return {"ok": True, "warning": ""}
        last_output = ((proc.stderr or "") + (proc.stdout or "")).strip()
    return {
        "ok": False,
        "warning": last_output or f"Could not enable linger for {user}; run: sudo loginctl enable-linger {user}",
    }


def _stop_legacy_services(logs: list[str]) -> None:
    """Stop non-API legacy daemons before systemd starts replacements."""
    for service in _MIGRATION_LEGACY_STOP_SERVICES:
        status = _systemd_service_status(service)
        if status is not None and status.get("running"):
            logs.append(f"Skipping {service}: already managed by active systemd unit.")
            continue
        try:
            obj = _get_service(service)
            if obj.is_running():
                obj.stop()
                logs.append(f"Stopped legacy {service} process.")
        except Exception as exc:
            logs.append(f"Warning: could not stop legacy {service}: {exc}")


def _schedule_api_systemd_handoff(logs: list[str]) -> str:
    """Restart the API through systemd after the HTTP response is sent."""
    import threading
    from logging_helpers import rotate_managed_log_before_open

    unit = _systemd_unit_for_service("api-server")
    if not unit:
        raise RuntimeError("pbgui-api.service was not installed.")
    status = _systemd_service_status("api-server")
    if status is not None and status.get("running"):
        handoff_log = Path(PBGDIR) / "data" / "logs" / "api-systemd-handoff.log"
        rotate_managed_log_before_open(handoff_log, "api_handoff")
        restart_unit = f"pbgui-api-restart-{os.getpid()}.service"
        restart_cmd = f"""unit={shlex.quote(unit)}
logfile={shlex.quote(str(handoff_log))}
{{
  printf '%s delayed restart for %s requested by migration\n' "$(date -Is)" "$unit"
  sleep 1
  for _ in $(seq 1 30); do
    state="$(systemctl --user is-active "$unit" 2>/dev/null || true)"
    if [ "$state" != "activating" ] && [ "$state" != "deactivating" ]; then
      break
    fi
    printf '%s waiting for %s to leave state %s\n' "$(date -Is)" "$unit" "$state"
    sleep 1
  done
  systemctl --user restart "$unit"
  rc=$?
  printf '%s restart command for %s exited rc=%s\n' "$(date -Is)" "$unit" "$rc"
  exit "$rc"
}} >> "$logfile" 2>&1"""
        proc = subprocess.run(
            ["systemd-run", "--user", f"--unit={restart_unit}", "--collect", "/bin/bash", "-lc", restart_cmd],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
            env=_systemd_user_env(),
        )
        if proc.returncode != 0:
            output = ((proc.stderr or "") + (proc.stdout or "")).strip()
            raise RuntimeError(output or f"Could not schedule delayed restart for {unit}.")
        return f"API restart scheduled through transient systemd unit {restart_unit}."

    current_pid = os.getpid()
    pidfile = Path(PBGDIR) / "data" / "pid" / "api_server.pid"
    handoff_log = Path(PBGDIR) / "data" / "logs" / "api-systemd-handoff.log"
    rotate_managed_log_before_open(handoff_log, "api_handoff")
    handoff_cmd = f"""old_pid={current_pid}
pidfile={shlex.quote(str(pidfile))}
logfile={shlex.quote(str(handoff_log))}
{{
  printf '%s API handoff waiting for old pid %s\n' "$(date -Is)" "$old_pid"
  for _ in $(seq 1 30); do
    if ! kill -0 "$old_pid" 2>/dev/null; then
      break
    fi
    sleep 1
  done
  if [ -f "$pidfile" ] && [ "$(cat "$pidfile" 2>/dev/null || true)" = "$old_pid" ]; then
    rm -f "$pidfile"
    printf '%s removed stale pidfile %s\n' "$(date -Is)" "$pidfile"
  fi
  printf '%s starting %s\n' "$(date -Is)" {shlex.quote(unit)}
  systemctl --user start {shlex.quote(unit)}
  printf '%s start command exited rc=%s\n' "$(date -Is)" "$?"
}} >> "$logfile" 2>&1"""

    env = _systemd_user_env()
    handoff_unit = f"pbgui-api-handoff-{current_pid}.service"
    proc = subprocess.run(
        ["systemd-run", "--user", f"--unit={handoff_unit}", "--collect", "/bin/bash", "-lc", handoff_cmd],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
        env=env,
    )
    if proc.returncode != 0:
        output = ((proc.stderr or "") + (proc.stdout or "")).strip()
        raise RuntimeError(output or "Could not schedule API systemd handoff.")

    def _stop_current_api() -> None:
        time.sleep(0.3)
        os.kill(os.getpid(), signal.SIGTERM)

    threading.Thread(target=_stop_current_api, daemon=True).start()
    logs.append(f"Scheduled current API process shutdown before systemd handoff unit {handoff_unit} starts pbgui-api.service.")
    return "API handoff scheduled through pbgui-api.service."


def _test_systemd_migration() -> dict[str, Any]:
    """Return a non-mutating dry-run plan for systemd migration."""
    status = _migration_status_payload()
    cmd, enable_services, setup_script, user, pbgdir, python_bin = _migration_setup_command(status)
    warnings: list[str] = list(status.get("warnings") or [])
    errors: list[str] = []
    logs: list[str] = [
        "DRY RUN: no files, crontab entries, services, or systemd units were changed.",
        f"PBGui user: {user}",
        f"PBGui directory: {pbgdir}",
        f"PBGui Python: {python_bin}",
        f"Systemd unit directory: {status.get('systemd_unit_dir') or ''}",
    ]

    if setup_script.exists():
        proc = subprocess.run(["bash", "-n", str(setup_script)], check=False, capture_output=True, text=True, timeout=20)
        if proc.returncode == 0:
            logs.append("Validated setup/setup_systemd.sh syntax with bash -n.")
        else:
            output = ((proc.stderr or "") + (proc.stdout or "")).strip()
            errors.append(output or "setup/setup_systemd.sh syntax check failed.")
    else:
        errors.append("setup/setup_systemd.sh is missing; update PBGui before migrating to systemd.")

    legacy_entries = list((status.get("legacy_crontab") or {}).get("entries") or [])
    if legacy_entries:
        logs.append(f"Would remove {len(legacy_entries)} legacy PBGui crontab autostart entrie(s) after systemd services verify successfully:")
        logs.extend(f"  - {entry}" for entry in legacy_entries)
    else:
        logs.append("Would not remove crontab entries because none were detected.")
    legacy_start_sh = status.get("legacy_start_sh") or {}
    if legacy_start_sh.get("exists"):
        logs.append(f"Would delete legacy start.sh after systemd services verify successfully: {legacy_start_sh.get('path') or ''}")
    else:
        logs.append("Would not delete legacy start.sh because it was not detected.")

    missing_units = [row.get("unit") or row.get("service") for row in status.get("missing_default_units") or []]
    if missing_units:
        logs.append("Would install missing systemd user unit(s): " + ", ".join(str(unit) for unit in missing_units))
    else:
        logs.append("Would refresh existing PBGui systemd user unit files.")
    not_ready_units = [row.get("unit") or row.get("service") for row in status.get("not_ready_default_units") or []]
    if not_ready_units:
        logs.append("Would enable/restart not-ready systemd user unit(s): " + ", ".join(str(unit) for unit in not_ready_units))

    logs.append("Would run setup command:")
    logs.append(f"  {shlex.join(cmd)}")
    logs.append("Would install and enable PBApiServer as pbgui-api.service because the setup command includes --enable api.")
    logs.append("Would not restart pbgui-api.service together with the daemon services; the current HTTP migration response must finish first.")

    legacy_processes = [
        proc for proc in status.get("processes") or []
        if proc.get("service") in _MIGRATION_LEGACY_STOP_SERVICES
    ]
    if legacy_processes:
        logs.append("Would stop legacy PBGui daemon process(es) before systemd restarts them:")
        for proc in legacy_processes:
            logs.append(f"  - {proc.get('service')} pid={proc.get('pid')} cmd={proc.get('cmdline')}")
    else:
        logs.append("Would not stop legacy daemon processes because none were detected.")

    restart_services = [service for service in enable_services if service != "api"]
    if restart_services:
        logs.append("Would restart systemd user service(s): " + ", ".join(f"pbgui-{service}.service" for service in restart_services))
    logs.append("Would hand off PBApiServer after returning the migration response by restarting pbgui-api.service through systemctl --user.")
    logs.append("Would terminate the current legacy PBApiServer process only after scheduling the systemd API restart, so systemd owns PBGui API afterwards.")
    logs.append("Would keep existing PBGui/PB7 data and configured paths unchanged.")

    return {
        "ok": not errors,
        "logs": logs,
        "warnings": warnings,
        "errors": errors,
        "status": status,
        "command": shlex.join(cmd),
    }


def _run_systemd_migration() -> dict[str, Any]:
    """Migrate the current master installation to systemd user services."""
    before = _migration_status_payload()
    logs: list[str] = []
    warnings: list[str] = list(before.get("warnings") or [])
    cmd, enable_services, setup_script, user, pbgdir, python_bin = _migration_setup_command(before)
    if not setup_script.exists():
        raise RuntimeError("setup/setup_systemd.sh is missing; update PBGui before migrating to systemd.")

    linger = _try_enable_linger(user)
    if linger.get("ok"):
        logs.append(f"Enabled linger for {user}.")
    elif linger.get("warning"):
        warnings.append(str(linger.get("warning")))

    proc = subprocess.run(cmd, check=False, capture_output=True, text=True, timeout=120, cwd=str(pbgdir))
    output = ((proc.stdout or "") + (proc.stderr or "")).strip()
    if output:
        logs.extend(output.splitlines())
    if proc.returncode != 0:
        raise RuntimeError(output or "setup_systemd.sh failed")

    _stop_legacy_services(logs)
    for service in enable_services:
        if service == "api":
            continue
        service_id = "api-server" if service == "api" else service
        action_result = _service_action(service_id, "restart")
        if action_result.get("error"):
            raise RuntimeError(str(action_result.get("error")))
        if not action_result.get("running"):
            raise RuntimeError(f"pbgui-{service}.service did not become active after restart.")
        logs.append(f"Restarted {service_id} with {action_result.get('manager', 'unknown')}.")

    if not _systemd_unit_for_service("api-server"):
        raise RuntimeError("pbgui-api.service was not installed.")

    crontab_result = _remove_legacy_crontab_entries()
    if crontab_result.get("removed"):
        logs.append(f"Removed {len(crontab_result.get('removed') or [])} legacy PBGui crontab autostart entrie(s).")
    if crontab_result.get("warning"):
        warnings.append(
            "Could not inspect/remove legacy crontab autostart. If you configured PBGui autostart manually, remove it yourself to avoid duplicate starts."
        )

    legacy_start_sh = pbgdir / "start.sh"
    if legacy_start_sh.exists():
        try:
            legacy_start_sh.unlink()
            logs.append(f"Deleted legacy start.sh: {legacy_start_sh}")
        except Exception as exc:
            warnings.append(f"Could not delete legacy start.sh {legacy_start_sh}: {exc}")

    api_message = _schedule_api_systemd_handoff(logs)
    logs.append(api_message)
    after = _migration_status_payload()
    return {
        "ok": True,
        "logs": logs,
        "warnings": warnings,
        "before": before,
        "after": after,
        "api_restart": True,
    }


def _task_active(task: Any) -> bool:
    return bool(task and not task.done())


def _worker_stat(label: str, value: Any) -> dict[str, str]:
    return {"label": str(label), "value": str(value)}


def _worker_item(
    *,
    worker_id: str,
    label: str,
    group: str,
    worker_type: str,
    running: bool,
    summary: str,
    description: str,
    note: str = "",
    stats: list[dict[str, str]] | None = None,
    log_file: str | None = None,
    monitor_path: str | None = None,
    available: bool = True,
    can_start: bool | None = None,
    can_stop: bool | None = None,
) -> dict[str, Any]:
    if can_start is None:
        can_start = available
    if can_stop is None:
        can_stop = available
    return {
        "id": worker_id,
        "label": label,
        "group": group,
        "type": worker_type,
        "running": bool(running),
        "summary": summary,
        "description": description,
        "note": note,
        "stats": stats or [],
        "log_file": log_file,
        "monitor_path": monitor_path,
        "available": bool(available),
        "can_start": bool(can_start),
        "can_stop": bool(can_stop),
    }


def _get_task_worker_item() -> dict[str, Any]:
    from task_queue import list_jobs, read_worker_pid, is_pid_running, clear_worker_pid

    jobs = list_jobs(states=["pending", "running", "done", "failed"], limit=0)
    counts = Counter(str(job.get("status") or "unknown").strip().lower() for job in jobs)
    pid = read_worker_pid()
    running = bool(pid and is_pid_running(int(pid)))
    if pid and not running:
        clear_worker_pid()
        pid = None

    pending = counts.get("pending", 0)
    active = counts.get("running", 0) + counts.get("cancelling", 0)
    done = counts.get("done", 0)
    failed = counts.get("failed", 0)
    summary = f"{pending} pending, {active} active"
    return _worker_item(
        worker_id="market-data-task",
        label="Market Data Queue",
        group="queue",
        worker_type="process worker",
        running=running,
        summary=summary,
        description="Processes queued Market Data and Heatmap jobs from the shared task queue.",
        note="Stop sends SIGTERM to the worker process. If pending jobs remain, the PBAPIServer watchdog may start it again.",
        stats=[
            _worker_stat("PID", pid or "-"),
            _worker_stat("Pending", pending),
            _worker_stat("Active", active),
            _worker_stat("Done", done),
            _worker_stat("Failed", failed),
        ],
        monitor_path="/app/jobs_monitor.html",
        log_file=None,
    )


async def _get_backtest_worker_item() -> dict[str, Any]:
    import api.backtest_v7 as bt7

    await bt7._store.refresh_from_disk()
    items = list(bt7._store.items.values())
    counts = Counter(str(item.get("status") or "unknown").strip().lower() for item in items)
    settings = bt7._read_ini_section()
    cpu = settings.get("cpu", "1")
    autostart = settings.get("autostart", "False").lower() == "true"
    running = _task_active(getattr(bt7._worker, "_task", None))
    summary = f"{counts.get('queued', 0)} queued, {counts.get('running', 0) + counts.get('backtesting', 0)} active"
    return _worker_item(
        worker_id="backtest-queue",
        label="Backtest Queue",
        group="queue",
        worker_type="scheduler task",
        running=running,
        summary=summary,
        description="Schedules queued PB7 backtests and launches detached PB7 subprocesses.",
        note="Stopping the worker pauses queue processing only. Already launched PB7 backtest subprocesses continue running.",
        stats=[
            _worker_stat("Queued", counts.get("queued", 0)),
            _worker_stat("Running", counts.get("running", 0)),
            _worker_stat("Backtesting", counts.get("backtesting", 0)),
            _worker_stat("Complete", counts.get("complete", 0)),
            _worker_stat("Error", counts.get("error", 0)),
            _worker_stat("Autostart", "On" if autostart else "Off"),
            _worker_stat("CPU limit", cpu),
        ],
        log_file="BacktestQueueAPI.log",
    )


async def _get_optimize_worker_item() -> dict[str, Any]:
    import api.optimize_v7 as opt7

    await opt7._store.refresh_from_disk()
    items = list(opt7._store.items.values())
    counts = Counter(str(item.get("status") or "unknown").strip().lower() for item in items)
    settings = opt7._read_ini_section()
    autostart = settings.get("autostart", "False").lower() == "true"
    cpu_override = settings.get("cpu_override", "True").lower() == "true"
    cpu = settings.get("cpu", "1")
    running = _task_active(getattr(opt7._worker, "_task", None))
    summary = f"{counts.get('queued', 0)} queued, {counts.get('running', 0) + counts.get('optimizing', 0)} active"
    return _worker_item(
        worker_id="optimize-queue",
        label="Optimize Queue",
        group="queue",
        worker_type="scheduler task",
        running=running,
        summary=summary,
        description="Schedules queued PB7 optimize jobs and launches detached PB7 optimizer subprocesses.",
        note="Stopping the worker pauses queue processing only. Already launched PB7 optimize subprocesses continue running.",
        stats=[
            _worker_stat("Queued", counts.get("queued", 0)),
            _worker_stat("Running", counts.get("running", 0)),
            _worker_stat("Optimizing", counts.get("optimizing", 0)),
            _worker_stat("Complete", counts.get("complete", 0)),
            _worker_stat("Error", counts.get("error", 0)),
            _worker_stat("Autostart", "On" if autostart else "Off"),
            _worker_stat("Autostart CPU", cpu),
            _worker_stat("CPU override", "On" if cpu_override else "Off"),
        ],
        log_file="OptimizeQueueAPI.log",
    )


def _get_archive_sync_worker_item() -> dict[str, Any]:
    import api.backtest_v7 as bt7

    running = _task_active(getattr(bt7._archive_sync_worker, "_task", None))
    return _worker_item(
        worker_id="archive-sync",
        label="Archive Sync",
        group="internal",
        worker_type="periodic task",
        running=running,
        summary="Auto-pulls configured backtest archives",
        description="Keeps configured backtest archives up to date in the background.",
        note="This worker is tied to the Backtest subsystem and is usually only needed for archive maintenance.",
        stats=[
            _worker_stat("Running", "Yes" if running else "No"),
            _worker_stat("Auto pull", bt7._read_auto_pull_interval()),
        ],
        log_file="ArchiveSync.log",
    )


def _get_hlcvs_cleanup_worker_item() -> dict[str, Any]:
    import api.backtest_v7 as bt7

    running = _task_active(getattr(bt7._hlcvs_cleanup_worker, "_task", None))
    targets = []
    try:
        targets = bt7._cleanup_cache_targets()
    except Exception:
        targets = []
    return _worker_item(
        worker_id="hlcvs-cleanup",
        label="HLCVS Cleanup",
        group="internal",
        worker_type="periodic task",
        running=running,
        summary=f"Maintains {len(targets)} cache target(s)",
        description="Periodically removes expired PB7 cache materialization data from configured cleanup targets.",
        stats=[
            _worker_stat("Running", "Yes" if running else "No"),
            _worker_stat("Targets", len(targets)),
        ],
        log_file="HLCVSCleanup.log",
    )


async def _collect_worker_groups() -> list[dict[str, Any]]:
    groups = [
        {
            "id": "queue",
            "label": "Queue Workers",
            "items": [
                _get_task_worker_item(),
                await _get_backtest_worker_item(),
                await _get_optimize_worker_item(),
            ],
        },
        {
            "id": "internal",
            "label": "Internal Helpers",
            "items": [
                _get_archive_sync_worker_item(),
                _get_hlcvs_cleanup_worker_item(),
            ],
        },
    ]
    return groups


async def _find_worker(worker_id: str) -> dict[str, Any] | None:
    groups = await _collect_worker_groups()
    for group in groups:
        for item in group.get("items", []):
            if item.get("id") == worker_id:
                return item
    return None


def _spawn_task_worker() -> None:
    from task_queue import clear_worker_pid

    clear_worker_pid()
    subprocess.Popen(
        [sys.executable, str(Path(__file__).resolve().parents[1] / "task_worker.py")],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        close_fds=True,
    )


async def _wait_for_task_worker_exit(pid: int, timeout_s: float = _TASK_WORKER_STOP_TIMEOUT_S) -> bool:
    """Wait until the detached market-data task worker process has exited."""

    from task_queue import is_pid_running

    deadline = time.monotonic() + max(0.1, float(timeout_s))
    while time.monotonic() < deadline:
        if not is_pid_running(int(pid)):
            return True
        await asyncio.sleep(0.5)
    return not is_pid_running(int(pid))


async def _start_worker(worker_id: str) -> None:
    if worker_id == "market-data-task":
        _spawn_task_worker()
        return
    if worker_id == "backtest-queue":
        import api.backtest_v7 as bt7
        bt7._worker.start()
        return
    if worker_id == "optimize-queue":
        import api.optimize_v7 as opt7
        opt7._worker.start()
        return
    if worker_id == "archive-sync":
        import api.backtest_v7 as bt7
        bt7._archive_sync_worker.start()
        return
    if worker_id == "hlcvs-cleanup":
        import api.backtest_v7 as bt7
        bt7._hlcvs_cleanup_worker.start()
        return
    raise HTTPException(status_code=404, detail=f"Unknown worker: {worker_id}")


async def _stop_worker(worker_id: str) -> None:
    if worker_id == "market-data-task":
        from task_queue import read_worker_pid, is_pid_running, clear_worker_pid

        pid = read_worker_pid()
        if pid and is_pid_running(int(pid)):
            os.kill(int(pid), signal.SIGTERM)
            exited = await _wait_for_task_worker_exit(int(pid))
            if not exited:
                raise HTTPException(status_code=409, detail=f"Market Data Queue worker PID {pid} did not stop within {int(_TASK_WORKER_STOP_TIMEOUT_S)}s")
        pid = read_worker_pid()
        if pid and not is_pid_running(int(pid)):
            clear_worker_pid()
        return
    if worker_id == "backtest-queue":
        import api.backtest_v7 as bt7
        await bt7._worker.stop()
        return
    if worker_id == "optimize-queue":
        import api.optimize_v7 as opt7
        await opt7._worker.stop()
        return
    if worker_id == "archive-sync":
        import api.backtest_v7 as bt7
        await bt7._archive_sync_worker.stop()
        return
    if worker_id == "hlcvs-cleanup":
        import api.backtest_v7 as bt7
        await bt7._hlcvs_cleanup_worker.stop()
        return
    raise HTTPException(status_code=404, detail=f"Unknown worker: {worker_id}")


# ── Status ───────────────────────────────────────────────────

@router.get("/status")
def get_status(session: SessionToken = Depends(require_auth)) -> Dict[str, Any]:
    """Return running status for all services."""
    try:
        reconcile_pending_credentials(PBGDIR)
    except Exception as exc:
        _log(
            SERVICE,
            f"Credential reconciliation remains pending: {type(exc).__name__}",
            level="WARNING",
        )
    result = {}
    for svc in _SERVICES:
        try:
            result[svc] = _service_status(svc)
        except Exception as e:
            _log(SERVICE, f"status check failed for {svc}: {e}", level="WARNING")
            result[svc] = {"running": False, "error": str(e)}
    return result


@router.get("/migration/status")
def get_migration_status(session: SessionToken = Depends(require_auth)) -> Dict[str, Any]:
    """Return preflight status for migrating this master to systemd services."""
    try:
        return _migration_status_payload()
    except Exception as e:
        _log(SERVICE, f"migration status failed: {e}", level="ERROR", meta={"operation": "migration_status", "traceback": traceback.format_exc()})
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/migration/test")
def test_migration(session: SessionToken = Depends(require_auth)) -> Dict[str, Any]:
    """Return a dry-run plan for migrating this master to systemd services."""
    try:
        return _test_systemd_migration()
    except Exception as e:
        _log(SERVICE, f"migration test failed: {e}", level="ERROR", meta={"operation": "migration_test", "traceback": traceback.format_exc()})
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/migration/run")
def run_migration(session: SessionToken = Depends(require_auth)) -> Dict[str, Any]:
    """Migrate the current master installation to systemd user services."""
    try:
        return _run_systemd_migration()
    except Exception as e:
        _log(SERVICE, f"migration run failed: {e}", level="ERROR", meta={"operation": "migration_run", "traceback": traceback.format_exc()})
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/workers/status")
async def get_workers_status(session: SessionToken = Depends(require_auth)) -> Dict[str, Any]:
    groups = await _collect_worker_groups()
    total = sum(len(group.get("items", [])) for group in groups)
    running = sum(
        1
        for group in groups
        for item in group.get("items", [])
        if item.get("running")
    )
    return {
        "updated_ts": int(time.time()),
        "counts": {"total": total, "running": running},
        "groups": groups,
    }


# ── Start / Stop ─────────────────────────────────────────────

@router.post("/{service}/start")
def start_service(service: str, session: SessionToken = Depends(require_auth)) -> Dict[str, Any]:
    if service not in _SERVICES:
        raise HTTPException(status_code=404, detail=f"Unknown service: {service}")
    try:
        return _service_action(service, "start")
    except Exception as e:
        _log(SERVICE, f"start {service} failed: {e}", level="ERROR", meta={"operation": "start_service", "service": service, "traceback": traceback.format_exc()})
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{service}/stop")
def stop_service(service: str, session: SessionToken = Depends(require_auth)) -> Dict[str, Any]:
    if service not in _SERVICES:
        raise HTTPException(status_code=404, detail=f"Unknown service: {service}")
    try:
        return _service_action(service, "stop")
    except Exception as e:
        _log(SERVICE, f"stop {service} failed: {e}", level="ERROR", meta={"operation": "stop_service", "service": service, "traceback": traceback.format_exc()})
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{service}/restart")
def restart_service(service: str, session: SessionToken = Depends(require_auth)) -> Dict[str, Any]:
    if service not in _SERVICES:
        raise HTTPException(status_code=404, detail=f"Unknown service: {service}")
    if service == "api-server":
        return restart_api_server(session=session)
    try:
        return _service_action(service, "restart")
    except Exception as e:
        _log(SERVICE, f"restart {service} failed: {e}", level="ERROR", meta={"operation": "restart_service", "service": service, "traceback": traceback.format_exc()})
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{service}/enable")
def enable_service(service: str, session: SessionToken = Depends(require_auth)) -> Dict[str, Any]:
    if service not in _SERVICES:
        raise HTTPException(status_code=404, detail=f"Unknown service: {service}")
    try:
        return _service_action(service, "enable")
    except Exception as e:
        _log(SERVICE, f"enable {service} failed: {e}", level="ERROR", meta={"operation": "enable_service", "service": service, "traceback": traceback.format_exc()})
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{service}/disable")
def disable_service(service: str, session: SessionToken = Depends(require_auth)) -> Dict[str, Any]:
    if service not in _SERVICES:
        raise HTTPException(status_code=404, detail=f"Unknown service: {service}")
    try:
        return _service_action(service, "disable")
    except Exception as e:
        _log(SERVICE, f"disable {service} failed: {e}", level="ERROR", meta={"operation": "disable_service", "service": service, "traceback": traceback.format_exc()})
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/workers/{worker_id}/{action}")
async def worker_action(worker_id: str, action: str, session: SessionToken = Depends(require_auth)) -> Dict[str, Any]:
    normalized_action = str(action or "").strip().lower()
    if normalized_action not in {"start", "stop", "restart"}:
        raise HTTPException(status_code=404, detail=f"Unknown action: {action}")

    try:
        if normalized_action == "start":
            await _start_worker(worker_id)
        elif normalized_action == "stop":
            await _stop_worker(worker_id)
        else:
            await _stop_worker(worker_id)
            await _start_worker(worker_id)
        await asyncio.sleep(0.1)
        item = await _find_worker(worker_id)
        if item is None:
            raise HTTPException(status_code=404, detail=f"Unknown worker: {worker_id}")
        return {"ok": True, "worker": item}
    except HTTPException:
        raise
    except Exception as e:
        _log(SERVICE, f"worker action failed ({worker_id}/{normalized_action}): {e}", level="ERROR", meta={"operation": "worker_action", "worker": worker_id, "action": normalized_action, "traceback": traceback.format_exc()})
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api-server/restart")
def restart_api_server(session: SessionToken = Depends(require_auth)) -> Dict[str, Any]:
    """Trigger in-process restart of the API server.

    Returns 200 immediately; the actual stop+restart happens 300 ms later in a
    daemon thread so the HTTP response has time to reach the browser before the
    process exits.
    """
    import os
    import signal
    import subprocess
    import sys
    import threading
    import time

    try:
        api_server = importlib.import_module("PBApiServer")
        restart_blocked, restart_block_reason = asyncio.run(api_server._restart_block_state())
        if restart_blocked:
            detail = restart_block_reason or "An API-owned mutable operation is still running."
            raise HTTPException(status_code=409, detail=f"Cannot restart API server: {detail}")

        systemd_unit = _systemd_unit_for_service("api-server")
        if systemd_unit:
            ok, output = _queue_api_systemd_restart(systemd_unit)
            if not ok:
                _log(SERVICE, f"[restart] systemd restart scheduling failed for {systemd_unit}: {output}", level="ERROR")
                raise RuntimeError(output or f"Could not schedule restart for {systemd_unit}.")
            _log(SERVICE, f"[restart] systemd restart scheduled for {systemd_unit}: {output}", level="WARNING")
            return {"ok": True, "message": "Restarting…"}

        pbgdir = Path(PBGDIR)
        venv_python: Optional[str] = None
        for candidate in [
            pbgdir.parent / "venv_pbgui" / "bin" / "python",
            pbgdir.parent / "venv_pbgui312" / "bin" / "python",
            pbgdir.parent / "venv" / "bin" / "python",
        ]:
            if candidate.exists():
                venv_python = str(candidate)
                break
        if not venv_python:
            venv_python = sys.executable

        pid_file = pbgdir / "data" / "pid" / "api_server.pid"

        def _do_restart() -> None:
            time.sleep(0.3)  # let HTTP response reach the browser first
            pid_file.unlink(missing_ok=True)
            env = os.environ.copy()
            env["PBGUI_RESTART_DELAY"] = "3"
            subprocess.Popen(
                [venv_python, str(pbgdir / "PBApiServer.py")],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                close_fds=True,
                cwd=str(pbgdir),
                env=env,
            )
            os.kill(os.getpid(), signal.SIGTERM)

        _log(SERVICE, "[restart] restart requested by user", level="WARNING")
        threading.Thread(target=_do_restart, daemon=True).start()
        return {"ok": True, "message": "Restarting\u2026"}
    except HTTPException:
        raise
    except Exception as e:
        _log(SERVICE, f"restart api-server failed: {e}", level="ERROR", meta={"operation": "restart_api_server", "service": "api-server", "traceback": traceback.format_exc()})
        raise HTTPException(status_code=500, detail=str(e))


# ── Monitor config ───────────────────────────────────────────

_MC_FIELDS = [
    'mem_warning_server', 'mem_error_server', 'swap_warning_server', 'swap_error_server',
    'disk_warning_server', 'disk_error_server', 'cpu_warning_server', 'cpu_error_server',
    'mem_warning_v7', 'mem_error_v7', 'swap_warning_v7', 'swap_error_v7',
    'cpu_warning_v7', 'cpu_error_v7', 'error_warning_v7', 'error_error_v7',
    'traceback_warning_v7', 'traceback_error_v7',
]


def _load_monitor_config_values() -> Dict[str, float]:
    from MonitorConfig import MonitorConfig

    mc = MonitorConfig()
    return {field: getattr(mc, field) for field in _MC_FIELDS}


def _save_monitor_config_values(values: Dict[str, float]) -> None:
    from MonitorConfig import MonitorConfig

    mc = MonitorConfig()
    validated = _validated_monitor_values(values)
    for field in _MC_FIELDS:
        if field in validated:
            setattr(mc, field, validated[field])
    mc.save_monitor_config()


def _validated_monitor_values(values: Dict[str, float]) -> Dict[str, float]:
    """Convert supplied monitor thresholds and reject non-finite values."""
    validated = {}
    for field in _MC_FIELDS:
        if field not in values:
            continue
        try:
            value = float(values[field])
        except (TypeError, ValueError, OverflowError) as exc:
            raise HTTPException(status_code=422, detail=f"Invalid monitor threshold: {field}") from exc
        if not math.isfinite(value):
            raise HTTPException(status_code=422, detail=f"Invalid monitor threshold: {field}")
        validated[field] = value
    return validated


@router.get("/settings/monitor-config")
def get_monitor_config(session: SessionToken = Depends(require_auth)) -> Dict[str, Any]:
    """Return all monitor threshold values."""
    return _load_monitor_config_values()


@router.post("/settings/monitor-config")
def save_monitor_config(
    body: Dict[str, float], session: SessionToken = Depends(require_auth)
) -> Dict[str, Any]:
    """Save monitor threshold values."""
    try:
        _save_monitor_config_values(body)
        return {"ok": True}
    except HTTPException:
        raise
    except Exception as e:
        _log(SERVICE, f"save monitor config: {e}", level="ERROR")
        raise HTTPException(status_code=500, detail=str(e))


# ── Settings: PBCoinData ─────────────────────────────────────

def _cmc_credential_store() -> CredentialStore:
    """Return the local owner-only CMC credential store."""
    return CredentialStore(Path(PBGDIR) / "data" / "credentials")


def _cmc_pool_client() -> CmcPoolClient:
    """Return a pool client bound to the same state used by PBCoinData."""
    store = _cmc_credential_store()
    return build_cmc_pool_client(PBGDIR, credential_store=store)


def _cmc_credential_publisher(store: CredentialStore) -> ClusterCredentialPublisher:
    """Return the Cluster Sync publisher for local credentials."""
    return ClusterCredentialPublisher(Path(PBGDIR) / "data" / "cluster", store)


def _cmc_operation_store(store: CredentialStore) -> DurableOperationStore:
    """Return the durable idempotency journal shared by credential APIs."""

    return DurableOperationStore(store.root)


def _begin_cmc_operation(
    store: CredentialStore,
    operation_id: str,
    action: str,
    target: str = "",
) -> tuple[DurableOperationStore, Dict[str, Any] | None]:
    """Start one mutation or return its exact durable completed response."""

    operations = _cmc_operation_store(store)
    record = operations.begin(operation_id, action, target)
    if record.get("status") == "complete" and isinstance(record.get("result"), dict):
        return operations, dict(record["result"])
    return operations, None


def _cmc_lease_authority() -> CmcLeaseAuthority:
    """Return the optional local CMC lease authority journal."""
    return CmcLeaseAuthority(Path(PBGDIR) / "data" / "credentials" / "cmc_pool" / "leases")


def _safe_cmc_key(record: Dict[str, Any]) -> Dict[str, Any]:
    """Whitelist public CMC key metadata and usage fields."""
    source = str(record.get("origin") or record.get("source") or "local")
    public_fields = (
        "id", "label", "active", "shared", "generation", "created_at", "updated_at",
        "status", "used_credits", "provider_remaining", "provider_limit", "provider_used",
        "provider_reset_at", "total_acquisitions", "total_failures", "cooldown_remaining",
        "exhausted_remaining", "last_outcome", "last_settled_at", "pending",
        "desired_state", "desired_generation", "desired_eligible", "quota_domain_id",
        "provider_plan", "minute_limit", "daily_limit", "monthly_limit", "authority_epoch",
    )
    payload = {field: record.get(field) for field in public_fields if field in record}
    payload["source"] = source
    payload["imported"] = source != "local"
    payload["materialized_generation"] = int(record.get("generation") or 0)
    if record.get("pending"):
        payload["local_state"] = "pending"
    elif not record.get("active", False):
        payload["local_state"] = "disabled"
    else:
        payload["local_state"] = str(record.get("status") or "active")
    timestamp = record.get("last_settled_at")
    try:
        payload["provider_stale_age_seconds"] = max(time.time() - float(timestamp), 0.0) if timestamp else None
    except (TypeError, ValueError):
        payload["provider_stale_age_seconds"] = None
    return payload


def _cmc_usage_payload() -> Dict[str, Any]:
    """Return CMC pool usage directly from the shared pool client."""
    status = _cmc_pool_client().status()
    return {
        "day": status.get("day"),
        "soft_credit_limit": status.get("soft_credit_limit"),
        "active_credentials": int(status.get("active_credentials") or 0),
        "keys": [_safe_cmc_key(item) for item in status.get("keys") or [] if isinstance(item, dict)],
    }


def _cmc_pool_payload() -> Dict[str, Any]:
    """Build a secret-free readiness and health summary for the local pool."""
    payload = _cmc_usage_payload()
    keys = payload["keys"]
    active_count = int(payload["active_credentials"])
    unhealthy = sum(
        1 for item in keys
        if item.get("active") and str(item.get("status") or "active") not in {"active", "ready"}
    )
    warnings = []
    for item in keys:
        label = str(item.get("label") or item.get("id") or "CMC key")
        try:
            day_used = float(item.get("used_credits") or 0)
            daily_limit = float(item.get("daily_limit") or payload.get("soft_credit_limit") or 0)
        except (TypeError, ValueError):
            day_used = daily_limit = 0
        if daily_limit > 0 and day_used >= daily_limit * 0.8:
            warnings.append(f"{label}: daily CMC usage is at or above 80%")
        try:
            provider_used = float(item.get("provider_used") or 0)
            monthly_limit = float(item.get("monthly_limit") or item.get("provider_limit") or 0)
        except (TypeError, ValueError):
            provider_used = monthly_limit = 0
        if monthly_limit > 0 and provider_used >= monthly_limit * 0.8:
            warnings.append(f"{label}: monthly CMC usage is at or above 80%")
    payload.update({
        "ready": active_count > 0,
        "health": "unconfigured" if active_count == 0 else "degraded" if unhealthy else "healthy",
        "total_credentials": len(keys),
        "unhealthy_credentials": unhealthy,
        "warnings": warnings,
    })
    snapshot = read_cmc_cluster_snapshot(PBGDIR)
    if isinstance(snapshot, dict):
        lifecycle = credential_lifecycle_status(snapshot)
        payload["credential_lifecycle"] = lifecycle
        pool = (snapshot.get("desired_state") or {}).get("cmc_pool") or {}
        payload["authorities"] = list((pool.get("authorities") or {}).values())
        nodes = (snapshot.get("cluster_nodes") or {}).get("nodes") or {}
        payload["eligible_authority_nodes"] = [
            {
                "node_id": str(node_id),
                "name": str(node.get("pbname") or node.get("hostname") or node_id),
            }
            for node_id, node in sorted(nodes.items())
            if isinstance(node, dict)
            and node.get("enabled", True) is not False
            and node.get("state_replica", True) is not False
            and str(node.get("role") or "") == "master"
            and int(node.get("credential_protocol_version") or 0) >= 2
            and node.get("credential_capable") is True
        ]
    return payload


def _cmc_leases_payload() -> Dict[str, Any]:
    """Transform the local authority journal into a compact secret-free status."""
    state = _cmc_lease_authority().status()
    requests = state.get("requests") if isinstance(state.get("requests"), dict) else {}
    lease_states = state.get("leases") if isinstance(state.get("leases"), dict) else {}
    leases: list[Dict[str, Any]] = []
    for request_id, request in sorted(requests.items()):
        lease = request.get("lease") if isinstance(request, dict) else None
        if not isinstance(lease, dict):
            continue
        lease_id = str(lease.get("lease_id") or "")
        lease_state = lease_states.get(lease_id) if isinstance(lease_states.get(lease_id), dict) else {}
        settlement = lease_state.get("settlement") if isinstance(lease_state.get("settlement"), dict) else {}
        leases.append({
            "lease_id": lease_id,
            "request_id": str(request_id),
            "credential_id": lease.get("credential_id"),
            "generation": lease.get("secret_generation"),
            "quota_domain_id": lease.get("quota_domain_id"),
            "authority_epoch": lease.get("authority_epoch"),
            "recipient": lease.get("recipient"),
            "credits": float(lease.get("credits_micros") or 0) / 1_000_000,
            "request_count": lease.get("request_count"),
            "granted_at": lease.get("granted_at"),
            "expires_at": lease.get("expires_at"),
            "terminal": bool(lease_state.get("terminal")),
            "outcome": settlement.get("outcome"),
            "actual_credits": float(settlement.get("actual_credits_micros") or 0) / 1_000_000,
            "status_code": settlement.get("status_code"),
            "settled_at": settlement.get("settled_at"),
        })
    terminal_count = sum(1 for item in leases if item["terminal"])
    snapshot = read_cmc_cluster_snapshot(PBGDIR) or {}
    desired = snapshot.get("desired_state") if isinstance(snapshot, dict) else {}
    pool = desired.get("cmc_pool") if isinstance(desired, dict) else {}
    authorities = pool.get("authorities") if isinstance(pool, dict) else {}
    nodes = ((snapshot.get("cluster_nodes") or {}).get("nodes") or {}) if isinstance(snapshot, dict) else {}
    try:
        local_node_id = str(read_local_identity(default_cluster_root(Path(PBGDIR))).get("node_id") or "")
    except Exception:
        local_node_id = ""
    monitor = get_monitor()
    connected_hosts = None
    if monitor and getattr(monitor, "pool", None) and hasattr(monitor.pool, "connected_hosts"):
        try:
            connected_hosts = set(monitor.pool.connected_hosts())
        except Exception:
            connected_hosts = None

    def credits(value: Any) -> float:
        return float(value or 0) / 1_000_000

    domains = []
    warnings = []
    state_domains = state.get("domains") if isinstance(state.get("domains"), dict) else {}
    authority_domains = authorities if isinstance(authorities, dict) else {}
    for domain_id in sorted(set(state_domains) | set(authority_domains)):
        domain = state_domains.get(domain_id) if isinstance(state_domains.get(domain_id), dict) else {}
        route = authorities.get(domain_id) if isinstance(authorities, dict) and isinstance(authorities.get(domain_id), dict) else {}
        authority_node_id = str(route.get("authority_node_id") or "")
        node = nodes.get(authority_node_id) if isinstance(nodes, dict) and isinstance(nodes.get(authority_node_id), dict) else {}
        authority_name = str(node.get("pbname") or node.get("hostname") or authority_node_id)
        if not authority_node_id:
            authority_reachable = None
        elif authority_node_id == local_node_id:
            authority_reachable = True
        elif authority_name and connected_hosts is not None:
            authority_reachable = authority_name in connected_hosts
        else:
            authority_reachable = None
        limits = domain.get("limits") if isinstance(domain.get("limits"), dict) else {}
        day_reserved = credits(domain.get("day_reserved_credits_micros"))
        day_used = credits(domain.get("day_used_credits_micros"))
        month_reserved = credits(domain.get("month_reserved_credits_micros"))
        month_used = credits(domain.get("month_used_credits_micros"))
        daily_limit = credits(limits.get("daily_credits_micros"))
        monthly_limit = credits(limits.get("monthly_credits_micros"))
        domain_warnings = []
        if daily_limit and day_reserved + day_used >= daily_limit * 0.8:
            domain_warnings.append("Daily CMC quota is at or above 80%")
        if monthly_limit and month_reserved + month_used >= monthly_limit * 0.8:
            domain_warnings.append("Monthly CMC quota is at or above 80%")
        warnings.extend(f"{domain_id}: {warning}" for warning in domain_warnings)
        provider_updated = domain.get("provider_usage_updated_at")
        try:
            provider_age = max(time.time() - float(provider_updated), 0.0) if provider_updated else None
        except (TypeError, ValueError):
            provider_age = None
        authority_updated = route.get("updated_at")
        try:
            authority_age = max(time.time() - float(authority_updated), 0.0) if authority_updated else None
        except (TypeError, ValueError):
            authority_age = None
        domains.append({
            "quota_domain_id": str(domain_id),
            "authority_node_id": authority_node_id or None,
            "authority_node": authority_name or None,
            "authority_epoch": route.get("authority_epoch", domain.get("authority_epoch")),
            "authority_reachable": authority_reachable,
            "authority_updated_at": authority_updated,
            "authority_state_age_seconds": authority_age,
            "day": domain.get("day"),
            "month": domain.get("month"),
            "day_reserved_credits": day_reserved,
            "day_used_credits": day_used,
            "month_reserved_credits": month_reserved,
            "month_used_credits": month_used,
            "uncertain_credits": credits(domain.get("uncertain_credits_micros")),
            "daily_limit": daily_limit,
            "monthly_limit": monthly_limit,
            "concurrent_leases": int(domain.get("concurrent_leases") or 0),
            "provider_remaining": credits(domain.get("provider_remaining_micros")) if domain.get("provider_remaining_micros") is not None else None,
            "provider_limit": credits(domain.get("provider_limit_micros")) if domain.get("provider_limit_micros") is not None else None,
            "provider_used": credits(domain.get("provider_used_micros")) if domain.get("provider_used_micros") is not None else None,
            "provider_reset_at": domain.get("provider_reset_at"),
            "provider_stale_age_seconds": provider_age,
            "warnings": domain_warnings,
        })

    key_usage = []
    for credential_id, key in sorted((state.get("keys") or {}).items()):
        if not isinstance(key, dict):
            continue
        key_usage.append({
            "credential_id": str(credential_id),
            "generation": key.get("generation"),
            "reserved_credits": credits(key.get("reserved_credits_micros")),
            "reserved_requests": int(key.get("reserved_requests") or 0),
            "used_credits": credits(key.get("used_credits_micros")),
            "used_requests": int(key.get("used_requests") or 0),
        })
    return {
        "authority": {
            "available": True,
            "key_count": len(state.get("keys") or {}),
            "request_count": len(requests),
            "lease_count": len(leases),
            "active_leases": len(leases) - terminal_count,
            "terminal_leases": terminal_count,
            "provider_event_count": len(state.get("provider_events") or {}),
        },
        "domains": domains,
        "key_usage": key_usage,
        "warnings": warnings,
        "leases": leases,
    }


def _raise_cmc_pool_error(
    operation: str,
    exc: Exception,
    *,
    operation_id: str | None = None,
) -> None:
    """Log a CMC pool failure and map it to a stable HTTP response."""
    if isinstance(exc, HTTPException):
        raise exc
    _log(
        SERVICE,
        f"CMC pool {operation} failed: {exc}",
        level="ERROR",
        meta={"operation": f"cmc_pool_{operation}", "traceback": traceback.format_exc()},
    )
    def detail(message: str) -> Any:
        return {"message": message, "operation_id": operation_id} if operation_id else message

    if isinstance(exc, CredentialNotFoundError):
        raise HTTPException(status_code=404, detail=detail(str(exc.args[0] if exc.args else exc))) from exc
    if isinstance(exc, CredentialPublicationError):
        raise HTTPException(status_code=409, detail=detail(str(exc))) from exc
    if isinstance(exc, (TypeError, ValueError)):
        raise HTTPException(status_code=422, detail=detail(str(exc))) from exc
    raise HTTPException(status_code=500, detail=detail("CMC pool operation failed")) from exc


class CmcPoolKeyCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    api_key: str
    label: str = ""
    active: bool = True
    imported: bool = False
    shared: bool = False
    operation_id: Optional[str] = Field(default=None, min_length=1, max_length=128)
    request_id: Optional[str] = Field(default=None, min_length=1, max_length=128)


class CmcPoolKeyPatch(BaseModel):
    model_config = ConfigDict(extra="forbid")

    label: Optional[str] = None
    active: Optional[bool] = None
    imported: Optional[bool] = None
    shared: Optional[bool] = None
    operation_id: Optional[str] = Field(default=None, min_length=1, max_length=128)
    request_id: Optional[str] = Field(default=None, min_length=1, max_length=128)


class CmcPoolKeyRotate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    api_key: str
    operation_id: Optional[str] = Field(default=None, min_length=1, max_length=128)
    request_id: Optional[str] = Field(default=None, min_length=1, max_length=128)


class CmcAuthorityTransfer(BaseModel):
    model_config = ConfigDict(extra="forbid")

    quota_domain_id: str = Field(min_length=1, max_length=256)
    authority_node_id: str = Field(min_length=1, max_length=128)
    expected_epoch: Optional[int] = Field(default=None, ge=0)
    operation_id: Optional[str] = Field(default=None, min_length=1, max_length=128)
    request_id: Optional[str] = Field(default=None, min_length=1, max_length=128)


def _resume_cmc_mutation(
    store: CredentialStore,
    record: Dict[str, Any],
    operation_id: str,
) -> tuple[Dict[str, Any], Dict[str, Any]]:
    """Resume a durable pending CMC publication without exposing it to the local pool."""

    credential_id = str(record["id"])
    reconciliation = reconcile_pending_credentials(
        PBGDIR,
        store=store,
        publisher=_cmc_credential_publisher(store),
    )
    item = next(
        (
            item for item in reconciliation.get("items") or []
            if item.get("kind") == "cmc"
            and item.get("credential_id") == credential_id
            and item.get("operation_id") == operation_id
        ),
        {"status": "active", "publication_status": "already_committed"},
    )
    return store.get_cmc(credential_id), {
        "status": item.get("publication_status") or item.get("status"),
        "activation_status": item.get("status"),
        "operation_id": operation_id,
    }


@router.get("/cmc-pool")
def get_cmc_pool(session: SessionToken = Depends(require_auth)) -> Dict[str, Any]:
    """Return local CMC pool readiness, metadata, health, and usage."""
    try:
        try:
            reconcile_pending_credentials(PBGDIR)
        except Exception as exc:
            _log(
                SERVICE,
                f"Credential reconciliation remains pending: {type(exc).__name__}",
                level="WARNING",
            )
        return _cmc_pool_payload()
    except Exception as exc:
        _raise_cmc_pool_error("status", exc)


@router.post("/cmc-pool/keys")
def create_cmc_pool_key(
    body: CmcPoolKeyCreate,
    session: SessionToken = Depends(require_auth),
) -> Dict[str, Any]:
    """Store and publish a new CMC credential without returning its secret."""
    operation_id = body.operation_id or body.request_id or uuid.uuid4().hex
    try:
        store = _cmc_credential_store()
        operations, completed = _begin_cmc_operation(store, operation_id, "cmc_create")
        if completed is not None:
            return completed
        with credential_mutation_lock(store.root):
            record = store.create_cmc(
                body.api_key,
                label=body.label,
                active=body.active,
                origin="imported" if body.imported else "local",
                shared=body.shared,
                pending=True,
                operation_id=operation_id,
            )
            record, publication = _resume_cmc_mutation(store, record, operation_id)
        response = {
            "ok": True,
            "operation_id": operation_id,
            "credential": _safe_cmc_key(record),
            "publication_status": publication.get("status"),
            "activation_status": publication.get("activation_status"),
        }
        operations.complete(operation_id, response)
        return response
    except Exception as exc:
        _raise_cmc_pool_error("create", exc, operation_id=operation_id)


@router.patch("/cmc-pool/keys/{key_id}")
def patch_cmc_pool_key(
    key_id: str,
    body: CmcPoolKeyPatch,
    session: SessionToken = Depends(require_auth),
) -> Dict[str, Any]:
    """Update non-secret CMC metadata or active state."""
    operation_id = body.operation_id or body.request_id or uuid.uuid4().hex
    try:
        store = _cmc_credential_store()
        operations, completed = _begin_cmc_operation(store, operation_id, "cmc_patch", key_id)
        if completed is not None:
            return completed
        with credential_mutation_lock(store.root):
            fields_set = body.model_fields_set
            changes: Dict[str, Any] = {}
            if "label" in fields_set:
                changes["label"] = body.label
            if "shared" in fields_set:
                changes["shared"] = body.shared
            if "imported" in fields_set:
                changes["origin"] = "imported" if body.imported else "local"
            publication_status = "unchanged"
            if "active" in fields_set:
                changes.update({
                    "active": body.active,
                    "pending": True,
                    "operation_id": operation_id,
                })
                record = store.update_cmc(key_id, **changes)
                record, publication = _resume_cmc_mutation(store, record, operation_id)
                publication_status = str(publication.get("status") or "updated")
            else:
                record = store.update_cmc(key_id, operation_id=operation_id, **changes)
        response = {
            "ok": True,
            "operation_id": operation_id,
            "credential": _safe_cmc_key(record),
            "publication_status": publication_status,
        }
        operations.complete(operation_id, response)
        return response
    except Exception as exc:
        _raise_cmc_pool_error("patch", exc, operation_id=operation_id)


@router.post("/cmc-pool/keys/{key_id}/rotate")
def rotate_cmc_pool_key(
    key_id: str,
    body: CmcPoolKeyRotate,
    session: SessionToken = Depends(require_auth),
) -> Dict[str, Any]:
    """Create and publish a new immutable generation for a CMC key."""
    operation_id = body.operation_id or body.request_id or uuid.uuid4().hex
    try:
        store = _cmc_credential_store()
        operations, completed = _begin_cmc_operation(store, operation_id, "cmc_rotate", key_id)
        if completed is not None:
            return completed
        with credential_mutation_lock(store.root):
            record = store.update_cmc(
                key_id,
                api_key=body.api_key,
                active=True,
                pending=True,
                operation_id=operation_id,
            )
            record, publication = _resume_cmc_mutation(store, record, operation_id)
        response = {
            "ok": True,
            "operation_id": operation_id,
            "credential": _safe_cmc_key(record),
            "publication_status": publication.get("status"),
            "activation_status": publication.get("activation_status"),
        }
        operations.complete(operation_id, response)
        return response
    except Exception as exc:
        _raise_cmc_pool_error("rotate", exc, operation_id=operation_id)


@router.post("/cmc-pool/keys/{key_id}/disable")
def disable_cmc_pool_key(
    key_id: str,
    session: SessionToken = Depends(require_auth),
    operation_id: Optional[str] = None,
    request_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Disable a local CMC key and publish the cluster state transition."""
    operation_id = operation_id or request_id or uuid.uuid4().hex
    try:
        store = _cmc_credential_store()
        operations, completed = _begin_cmc_operation(store, operation_id, "cmc_disable", key_id)
        if completed is not None:
            return completed
        with credential_mutation_lock(store.root):
            record = store.update_cmc(
                key_id,
                active=False,
                pending=True,
                operation_id=operation_id,
            )
            record, publication = _resume_cmc_mutation(store, record, operation_id)
        response = {
            "ok": True,
            "operation_id": operation_id,
            "credential": _safe_cmc_key(record),
            "publication_status": publication.get("status"),
        }
        operations.complete(operation_id, response)
        return response
    except Exception as exc:
        _raise_cmc_pool_error("disable", exc, operation_id=operation_id)


@router.delete("/cmc-pool/keys/{key_id}")
def delete_cmc_pool_key(
    key_id: str,
    session: SessionToken = Depends(require_auth),
    operation_id: Optional[str] = None,
    request_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Publish a cluster tombstone and soft-delete a local CMC credential."""
    operation_id = operation_id or request_id or uuid.uuid4().hex
    try:
        store = _cmc_credential_store()
        operations, completed = _begin_cmc_operation(store, operation_id, "cmc_delete", key_id)
        if completed is not None:
            return completed
        with credential_mutation_lock(store.root):
            publication = _cmc_credential_publisher(store).publish_tombstone(key_id, "cmc_api_key")
            operations.checkpoint(operation_id, "published", {"publication": publication})
            store.delete_cmc(key_id, operation_id=operation_id)
        response = {
            "ok": True,
            "operation_id": operation_id,
            "cluster_operation_id": str(publication.get("operation_id") or ""),
            "key_id": key_id,
            "publication_status": publication.get("status"),
        }
        operations.complete(operation_id, response)
        return response
    except Exception as exc:
        _raise_cmc_pool_error("delete", exc, operation_id=operation_id)


@router.get("/cmc-pool/usage")
def get_cmc_pool_usage(session: SessionToken = Depends(require_auth)) -> Dict[str, Any]:
    """Return secret-free local usage from CmcPoolClient status."""
    try:
        return _cmc_usage_payload()
    except Exception as exc:
        _raise_cmc_pool_error("usage", exc)


@router.get("/cmc-pool/leases")
def get_cmc_pool_leases(session: SessionToken = Depends(require_auth)) -> Dict[str, Any]:
    """Return a secret-free view of the optional local lease authority."""
    try:
        return _cmc_leases_payload()
    except Exception as exc:
        _raise_cmc_pool_error("leases", exc)


@router.get("/cmc-pool/operations/{operation_id}")
def get_cmc_pool_operation(
    operation_id: str,
    session: SessionToken = Depends(require_auth),
) -> Dict[str, Any]:
    """Return durable secret-free status for one CMC mutation request."""

    try:
        store = _cmc_credential_store()
        record = _cmc_operation_store(store).get(operation_id)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    if record is None:
        raise HTTPException(status_code=404, detail="CMC operation not found")
    return record


@router.post("/cmc-pool/authority/transfer")
def transfer_cmc_pool_authority(
    body: CmcAuthorityTransfer,
    session: SessionToken = Depends(require_auth),
) -> Dict[str, Any]:
    """Transfer a CMC quota domain immediately through signed epoch CAS."""

    operation_id = body.operation_id or body.request_id or uuid.uuid4().hex
    try:
        store = _cmc_credential_store()
        operations, completed = _begin_cmc_operation(
            store,
            operation_id,
            "cmc_authority_transfer",
            f"{body.quota_domain_id}:{body.authority_node_id}",
        )
        if completed is not None:
            return completed
        with credential_mutation_lock(store.root):
            transfer = _cmc_credential_publisher(store).set_cmc_authority(
                body.quota_domain_id,
                body.authority_node_id,
                expected_epoch=body.expected_epoch,
            )
        response = {
            "ok": True,
            "operation_id": operation_id,
            "cluster_operation_id": str(transfer.get("operation_id") or ""),
            "authority": transfer,
            "cmc_pool": _cmc_pool_payload(),
        }
        operations.complete(operation_id, response)
        return response
    except Exception as exc:
        _raise_cmc_pool_error("authority_transfer", exc, operation_id=operation_id)

@router.get("/settings/pbcoindata/key-status")
def get_pbcoindata_key_status(session: SessionToken = Depends(require_auth)) -> Dict[str, Any]:
    """Return pool status through the legacy secret-free compatibility route."""
    try:
        payload = _cmc_pool_payload()
        payload["ok"] = payload["ready"]
        return payload
    except Exception as exc:
        _raise_cmc_pool_error("legacy_status", exc)


@router.get("/settings/pbcoindata")
def get_pbcoindata_settings(session: SessionToken = Depends(require_auth)) -> Dict[str, Any]:
    try:
        from PBCoinData import CoinData
        obj = CoinData()
        return {
            "fetch_limit": obj.fetch_limit,
            "fetch_interval": obj.fetch_interval,
            "metadata_interval": obj.metadata_interval,
            "mapping_interval": obj.mapping_interval,
            "cmc_pool": _cmc_pool_payload(),
            "apply": apply_metadata("pbcoindata"),
        }
    except HTTPException:
        raise
    except Exception as exc:
        _log(SERVICE, f"load pbcoindata settings failed: {exc}", level="ERROR", meta={"operation": "load_pbcoindata_settings", "traceback": traceback.format_exc()})
        raise HTTPException(status_code=500, detail="Unable to load PBCoinData settings") from exc


class PBCoinDataSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    fetch_limit: int = Field(default=5000, ge=200, le=5000)
    fetch_interval: int = Field(default=24, ge=1, le=24)
    metadata_interval: int = Field(default=1, ge=1, le=7)
    mapping_interval: int = Field(default=24, ge=1, le=168)


@router.post("/settings/pbcoindata")
def save_pbcoindata_settings(
    body: PBCoinDataSettings, session: SessionToken = Depends(require_auth)
) -> Dict[str, Any]:
    try:
        from PBCoinData import CoinData
        obj = CoinData()
        obj.fetch_limit = body.fetch_limit
        obj.fetch_interval = body.fetch_interval
        obj.metadata_interval = body.metadata_interval
        obj.mapping_interval = body.mapping_interval
        obj.save_config()
        return {
            "ok": True,
            "cmc_pool": _cmc_pool_payload(),
            "apply": apply_metadata("pbcoindata"),
        }
    except HTTPException:
        raise
    except Exception as e:
        _log(SERVICE, f"save pbcoindata settings: {e}", level="ERROR", meta={"operation": "save_pbcoindata_settings", "traceback": traceback.format_exc()})
        raise HTTPException(status_code=500, detail="Unable to save PBCoinData settings") from e


# ── Settings: PBAPIServer ────────────────────────────────────

def _available_vps_hosts() -> List[str]:
    vps_dir = Path(f"{PBGDIR}/data/vpsmanager/hosts")
    hostnames: list[str] = []
    pattern = str(vps_dir / "*" / "*.json")
    for filepath in sorted(glob.glob(pattern)):
        try:
            with open(filepath, "r") as f:
                config = json.load(f)
            hostname = config.get("_hostname")
            if hostname:
                hostnames.append(hostname)
        except Exception:
            pass
    return sorted(set(hostnames))


@router.get("/settings/api-server")
def get_api_server_settings(session: SessionToken = Depends(require_auth)) -> Dict[str, Any]:
    mod = importlib.import_module("PBApiServer")
    obj = mod.PBApiServer()
    monitor = get_monitor()

    snapshot = load_ini_snapshot()

    def ini_value(section: str, key: str) -> str:
        return snapshot.get(section, key) if snapshot.has_option(section, key) else ""

    auto_restart_val = ini_value("vps_monitor", "auto_restart")
    auto_restart = auto_restart_val.lower() == "true" if auto_restart_val else True

    enabled_hosts_val = ini_value("vps_monitor", "enabled_hosts")
    enabled_hosts: list[str] = []
    if enabled_hosts_val and enabled_hosts_val.strip():
        enabled_hosts = [h.strip() for h in enabled_hosts_val.split(",") if h.strip()]

    return {
        "host": obj.host,
        "port": obj.port,
        "auto_restart": auto_restart,
        "enabled_hosts": enabled_hosts,
        "available_hosts": _available_vps_hosts(),
        "monitor_config": _load_monitor_config_values(),
        **(monitor.get_alert_settings() if monitor else {
            "telegram_token": ini_value("main", "telegram_token"),
            "telegram_chat_id": ini_value("main", "telegram_chat_id"),
            "offline_gui": True,
            "service_gui": True,
            "system_gui": True,
            "instance_gui": True,
            "ssh_lost_telegram": True,
            "ssh_recovered_telegram": True,
            "service_down_telegram": True,
            "service_restart_started_telegram": True,
            "service_recovered_telegram": True,
            "system_problem_telegram": True,
            "system_recovered_telegram": True,
            "instance_problem_telegram": True,
            "instance_recovered_telegram": True,
        }),
        "apply": apply_metadata("api_server_full"),
    }


class APIServerSettings(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8000
    auto_restart: bool = True
    enabled_hosts: List[str] = []
    monitor_config: Dict[str, float] = Field(default_factory=dict)
    telegram_token: str = ""
    telegram_chat_id: str = ""
    offline_gui: bool = True
    service_gui: bool = True
    system_gui: bool = True
    instance_gui: bool = True
    ssh_lost_telegram: bool = True
    ssh_recovered_telegram: bool = True
    service_down_telegram: bool = True
    service_restart_started_telegram: bool = True
    service_recovered_telegram: bool = True
    system_problem_telegram: bool = True
    system_recovered_telegram: bool = True
    instance_problem_telegram: bool = True
    instance_recovered_telegram: bool = True


@router.post("/settings/api-server")
def save_api_server_settings(
    body: APIServerSettings, session: SessionToken = Depends(require_auth)
) -> Dict[str, Any]:
    try:
        mod = importlib.import_module("PBApiServer")
        host = body.host.strip() if body.host else "0.0.0.0"
        port = max(1024, min(65535, body.port))
        current = mod.PBApiServer()
        host_changed = host != current.host
        port_changed = port != current.port
        bind_changed = host_changed or port_changed
        values = body.model_dump()
        monitor_values = _validated_monitor_values(body.monitor_config)

        def mutate_ini(parser) -> None:
            updates = {
                "api_server": {"host": host, "port": str(port)},
                "vps_monitor": {"auto_restart": str(body.auto_restart), "enabled_hosts": ",".join(sorted(body.enabled_hosts))},
                "monitor": {field: str(value) for field, value in monitor_values.items()},
                "main": {"telegram_token": body.telegram_token.strip(), "telegram_chat_id": body.telegram_chat_id.strip()},
                "vps_monitor_alerts": {key: "true" if bool(values[key]) else "false" for key in values if key.endswith("_gui") or key.endswith("_telegram")},
            }
            for section, section_values in updates.items():
                if not parser.has_section(section):
                    parser.add_section(section)
                for key, value in section_values.items():
                    parser.set(section, key, value)

        update_ini(mutate_ini)
        if bind_changed:
            mod.mark_runtime_restart_required("API host or port settings changed")
        apply_keys = list(APPLY_GROUPS["api_server_live"])
        if host_changed:
            apply_keys.append(("api_server", "host"))
        if port_changed:
            apply_keys.append(("api_server", "port"))
        return {"ok": True, "apply": apply_metadata_for(apply_keys)}
    except HTTPException:
        raise
    except Exception as e:
        _log(SERVICE, f"save api-server settings: {e}", level="ERROR")
        raise HTTPException(status_code=500, detail=str(e))


# ── Settings: PBData ─────────────────────────────────────────

def _read_ini_int(section: str, key: str, default: int, snapshot=None) -> int:
    try:
        v = snapshot.get(section, key) if snapshot and snapshot.has_option(section, key) else ""
        if snapshot is None:
            v = load_ini(section, key)
        s = str(v).strip() if v is not None else ""
        return int(float(s)) if s else default
    except Exception:
        return default


def _read_ini_float(section: str, key: str, default: float, snapshot=None) -> float:
    try:
        v = snapshot.get(section, key) if snapshot and snapshot.has_option(section, key) else ""
        if snapshot is None:
            v = load_ini(section, key)
        s = str(v).strip() if v is not None else ""
        return float(s) if s else default
    except Exception:
        return default


@router.get("/settings/pbdata")
def get_pbdata_settings(session: SessionToken = Depends(require_auth)) -> Dict[str, Any]:
    from Exchange import MAX_PRIVATE_WS_GLOBAL
    from User import Users
    import ast as _ast

    try:
        users = Users()
        all_users = users.list()
        valid = set(all_users)
    except Exception:
        all_users = []
        valid = set()

    snapshot = load_ini_snapshot()

    def ini_value(key: str) -> str:
        return snapshot.get("pbdata", key) if snapshot.has_option("pbdata", key) else ""

    # Read fetch_users and trades_users directly from ini (no PBData() instantiation)
    def _read_ini_list(key: str) -> list:
        try:
            raw = ini_value(key)
            if not raw or not str(raw).strip():
                return []
            users_list = _ast.literal_eval(str(raw).strip())
            if not isinstance(users_list, list):
                return []
            return [u for u in users_list if u in valid]
        except Exception:
            return []

    fetch_users = _read_ini_list('fetch_users')
    trades_users = _read_ini_list('trades_users')

    # per-exchange overrides: read JSON from ini, merge with defaults
    default_by_ex = {'hyperliquid': 3.0, 'bybit': 3.0}
    try:
        raw = ini_value('shared_rest_pause_by_exchange_json')
        overrides = json.loads(raw) if raw.strip() else {}
        if isinstance(overrides, dict):
            default_by_ex.update({str(k): float(v) for k, v in overrides.items() if v is not None})
    except Exception:
        pass

    return {
        "fetch_users": fetch_users,
        "trades_users": trades_users,
        "all_users": all_users,
        "log_level": ini_value("log_level") or "INFO",
        "ws_max": _read_ini_int("pbdata", "ws_max", MAX_PRIVATE_WS_GLOBAL, snapshot),
        "pollers_delay_seconds": _read_ini_int("pbdata", "pollers_delay_seconds", 60, snapshot),
        "poll_interval_combined_seconds": _read_ini_int("pbdata", "poll_interval_combined_seconds", 90, snapshot),
        "poll_interval_balance_seconds": _read_ini_int("pbdata", "poll_interval_balance_seconds", 300, snapshot),
        "poll_interval_positions_seconds": _read_ini_int("pbdata", "poll_interval_positions_seconds", 300, snapshot),
        "poll_interval_orders_seconds": _read_ini_int("pbdata", "poll_interval_orders_seconds", 60, snapshot),
        "poll_interval_history_seconds": _read_ini_int("pbdata", "poll_interval_history_seconds", 300, snapshot),
        "poll_interval_executions_seconds": _read_ini_int("pbdata", "poll_interval_executions_seconds", 1800, snapshot),
        "shared_rest_user_pause_seconds": _read_ini_float("pbdata", "shared_rest_user_pause_seconds", 0.75, snapshot),
        "shared_rest_pause_by_exchange": default_by_ex,
        "latest_1m_coin_pause_seconds": _read_ini_float("pbdata", "latest_1m_coin_pause_seconds", 2.0, snapshot),
        "apply": apply_metadata("pbdata"),
    }


class PBDataSettings(BaseModel):
    fetch_users: List[str] = []
    trades_users: List[str] = []
    log_level: str = "INFO"
    ws_max: int = 10
    pollers_delay_seconds: int = 60
    poll_interval_combined_seconds: int = 90
    poll_interval_balance_seconds: int = 300
    poll_interval_positions_seconds: int = 300
    poll_interval_orders_seconds: int = 60
    poll_interval_history_seconds: int = 300
    poll_interval_executions_seconds: int = 1800
    shared_rest_user_pause_seconds: float = 0.75
    shared_rest_pause_by_exchange: Dict[str, float] = {}
    latest_1m_coin_pause_seconds: float = 2.0


@router.post("/settings/pbdata")
def save_pbdata_settings(
    body: PBDataSettings, session: SessionToken = Depends(require_auth)
) -> Dict[str, Any]:
    try:
        # Only store exchanges that differ from the global pause (overrides only)
        global_pause = body.shared_rest_user_pause_seconds
        overrides = {
            ex: v for ex, v in body.shared_rest_pause_by_exchange.items()
            if abs(v - global_pause) > 1e-9
        }
        save_ini_section("pbdata", {
            "fetch_users": str(body.fetch_users),
            "trades_users": str(body.trades_users),
            "log_level": "" if body.log_level == "NONE" else body.log_level,
            "ws_max": str(body.ws_max),
            "pollers_delay_seconds": str(body.pollers_delay_seconds),
            "poll_interval_combined_seconds": str(body.poll_interval_combined_seconds),
            "poll_interval_balance_seconds": str(body.poll_interval_balance_seconds),
            "poll_interval_positions_seconds": str(body.poll_interval_positions_seconds),
            "poll_interval_orders_seconds": str(body.poll_interval_orders_seconds),
            "poll_interval_history_seconds": str(body.poll_interval_history_seconds),
            "poll_interval_executions_seconds": str(body.poll_interval_executions_seconds),
            "shared_rest_user_pause_seconds": str(body.shared_rest_user_pause_seconds),
            "latest_1m_coin_pause_seconds": str(body.latest_1m_coin_pause_seconds),
            "shared_rest_pause_by_exchange_json": json.dumps(overrides) if overrides else "{}",
        })
        return {"ok": True, "apply": apply_metadata("pbdata")}
    except HTTPException:
        raise
    except Exception as e:
        _log(SERVICE, f"save pbdata settings: {e}", level="ERROR")
        raise HTTPException(status_code=500, detail=str(e))


# ── Fetch summary (PBData) ───────────────────────────────────

@router.post("/internal/fetch-summary")
async def update_fetch_summary(request: Request) -> Dict[str, Any]:
    """Accept PBData fetch summary from localhost and keep it in memory."""
    client_host = request.client.host if request.client else ""
    if client_host not in ("127.0.0.1", "::1", "localhost"):
        raise HTTPException(status_code=403, detail="Internal endpoint")
    try:
        body = await request.json()
    except Exception:
        body = {}
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Invalid fetch summary payload")
    global _fetch_summary_snapshot
    _fetch_summary_snapshot = dict(body)
    return {"ok": True}


@router.get("/fetch-summary")
def get_fetch_summary(session: SessionToken = Depends(require_auth)) -> Dict[str, Any]:
    return dict(_fetch_summary_snapshot)


@router.get("/prices-snapshot")
def get_prices_snapshot(session: SessionToken = Depends(require_auth)) -> Dict[str, Any]:
    """Return latest price per (symbol, exchange) from the prices DB table, filtered to active symbols."""
    import sqlite3 as _sqlite3
    try:
        # Build user→exchange map from api-keys
        user_exchange: Dict[str, str] = {}
        try:
            from User import Users as _Users
            _u = _Users()
            _u.load()
            for _usr in _u:
                if _usr.name and _usr.exchange:
                    user_exchange[_usr.name] = _usr.exchange
        except Exception:
            pass

        # Load active symbol list from the in-memory fetch summary.
        # 1. symbol_list present → filter by (symbol, exchange) pairs
        # 2. symbols>0 but no symbol_list (old PBData) → top-N most-recently-updated
        # 3. fetch summary absent or symbols=0 → return empty
        active_symbols: Optional[List[str]] = None
        allowed_pairs: Optional[set] = None          # set of (symbol, exchange)
        top_n: Optional[int] = None
        fs = dict(_fetch_summary_snapshot)
        if fs:
            try:
                prices = fs.get("prices", {})
                total_active_count = sum(exd.get("symbols", 0) for exd in prices.values())
                sym_set: set = set()
                pair_set: set = set()
                has_symbol_list = False
                for exch_name, exch_data in prices.items():
                    if "symbol_list" in exch_data and exch_data["symbol_list"]:
                        has_symbol_list = True
                        for s in exch_data["symbol_list"]:
                            sym_set.add(s)
                            pair_set.add((s, exch_name))
                if has_symbol_list:
                    active_symbols = sorted(sym_set)
                    allowed_pairs = pair_set
                elif total_active_count == 0:
                    return {"rows": []}
                else:
                    top_n = total_active_count
            except Exception:
                pass
        else:
            return {"rows": []}

        db_path = Path(f"{PBGDIR}/data/pbgui.db")
        if not db_path.exists():
            return {"rows": []}
        with _sqlite3.connect(str(db_path), timeout=5) as conn:
            conn.row_factory = _sqlite3.Row
            cur = conn.cursor()
            if active_symbols:
                placeholders = ",".join("?" * len(active_symbols))
                cur.execute(
                    f"SELECT symbol, user, price, MAX(timestamp) AS ts FROM prices WHERE symbol IN ({placeholders}) GROUP BY symbol, user ORDER BY symbol, user",
                    active_symbols,
                )
            elif top_n:
                cur.execute(
                    "SELECT symbol, user, price, MAX(timestamp) AS ts FROM prices GROUP BY symbol, user ORDER BY ts DESC LIMIT ?",
                    (top_n,),
                )
            else:
                cur.execute(
                    "SELECT symbol, user, price, MAX(timestamp) AS ts FROM prices GROUP BY symbol, user ORDER BY symbol, user"
                )
            raw = [{"symbol": r["symbol"], "user": r["user"], "price": r["price"], "ts": r["ts"]} for r in cur.fetchall()]

        # Collapse to best price per (symbol, exchange) — keep MAX(ts)
        best: Dict[str, Dict] = {}
        for row in raw:
            exch = user_exchange.get(row["user"], "")
            key = row["symbol"] + "\x00" + exch
            if key not in best or row["ts"] > best[key]["ts"]:
                best[key] = {"symbol": row["symbol"], "exchange": exch, "price": row["price"], "ts": row["ts"]}

        # Filter to allowed (symbol, exchange) pairs if available
        if allowed_pairs:
            best = {k: v for k, v in best.items() if (v["symbol"], v["exchange"]) in allowed_pairs}

        rows = sorted(best.values(), key=lambda x: (x["symbol"], x["exchange"]))
        return {"rows": rows}
    except Exception as e:
        _log(SERVICE, f"prices-snapshot failed: {e}", level="WARNING")
        raise HTTPException(status_code=500, detail=str(e))


# ── Poller metrics (PBData) ──────────────────────────────────

@router.post("/internal/poller-metrics")
async def update_poller_metrics(request: Request) -> Dict[str, Any]:
    """Accept PBData poller metrics from localhost and keep them in memory."""
    client_host = request.client.host if request.client else ""
    if client_host not in ("127.0.0.1", "::1", "localhost"):
        raise HTTPException(status_code=403, detail="Internal endpoint")
    try:
        body = await request.json()
    except Exception:
        body = {}
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Invalid metrics payload")
    global _poller_metrics_snapshot
    _poller_metrics_snapshot = dict(body)
    return {"ok": True}

@router.get("/poller-metrics")
def get_poller_metrics(session: SessionToken = Depends(require_auth)) -> Dict[str, Any]:
    return dict(_poller_metrics_snapshot)


# ── Main page ────────────────────────────────────────────────

@router.get("/main_page", response_class=HTMLResponse)
def get_main_page(
    request: Request,
    session: SessionToken = Depends(require_auth),
) -> HTMLResponse:
    """Serve the standalone Services Monitor page with token injected server-side."""
    html_path = Path(__file__).parent.parent / "frontend" / "services_monitor.html"
    html = html_path.read_text(encoding="utf-8")

    scheme = request.url.scheme
    host = request.url.hostname or "127.0.0.1"
    port = request.url.port
    origin = f"{scheme}://{host}" + (f":{port}" if port else "")
    api_services_base = origin + "/api/services"
    ws_base = origin.replace("http://", "ws://").replace("https://", "wss://")

    html = html.replace('"%%TOKEN%%"', json.dumps(session.token))
    html = html.replace('"%%API_BASE%%"', json.dumps(api_services_base))
    html = html.replace('"%%WS_BASE%%"', json.dumps(ws_base))

    from pbgui_purefunc import PBGUI_VERSION
    from pbgui_purefunc import PBGUI_SERIAL
    html = html.replace('"%%VERSION%%"', json.dumps(PBGUI_VERSION))
    html = html.replace("%%VERSION%%", PBGUI_VERSION)
    html = html.replace('"%%SERIAL%%"', json.dumps(PBGUI_SERIAL))
    html = html.replace("%%SERIAL%%", PBGUI_SERIAL)

    nav_js = Path(__file__).parent.parent / "frontend" / "pbgui_nav.js"
    nav_hash = str(int(nav_js.stat().st_mtime)) if nav_js.exists() else PBGUI_VERSION
    html = html.replace("%%NAV_HASH%%", nav_hash)

    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})
